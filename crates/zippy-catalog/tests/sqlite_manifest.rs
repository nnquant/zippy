use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use rusqlite::Connection;
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{ManagedDataDirectory, OwnedDataDirectory};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandFormatVersion,
    CatalogCommandV1, CatalogOutcomeV1, CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector,
    CatalogWrite, CommandFingerprint, CommitCompactionCommand, CommitDataFilesCommand,
    CreateNamespaceCommand, CreateTableCommand, VersionedCatalogCommand, VersionedCatalogResponse,
    MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, DataFileLocation, DataFileProvenance,
    DataFileState, ExternalDataPath, ManagedDataPath, MetadataMap, NativeDataFileProvenance,
    PendingDataFile, SchemaIpcPayload, SnapshotOperation, SourceSegmentIdentity,
    SourceSegmentLineage, SourceSegmentLineages, TableDefinition,
};
use zippy_types::{
    ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
};

const NAMESPACE: u128 = 1;
const TABLE: u128 = 10;

fn data_directory(directory: &TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(
        OwnedDataDirectory::acquire_or_initialize(directory.path().join("data"))
            .expect("own data root"),
    )
}

fn catalog_path(data_directory: &Arc<OwnedDataDirectory>) -> std::path::PathBuf {
    data_directory
        .directory()
        .path(ManagedDataDirectory::Catalog)
        .join("catalog.db")
}

fn server_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "sqlite-manifest-integration-test".to_owned(),
    }
}

fn request_id(value: u128) -> RequestId {
    RequestId::from_u128(value).expect("test request id must be nonzero")
}

fn namespace_id(value: u128) -> NamespaceId {
    NamespaceId::from_u128(value).expect("test namespace id must be nonzero")
}

fn table_id(value: u128) -> TableId {
    TableId::from_u128(value).expect("test table id must be nonzero")
}

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("test file id must be nonzero")
}

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut payload = Vec::new();
    let mut writer = StreamWriter::try_new(&mut payload, schema).expect("create schema writer");
    writer.finish().expect("finish schema-only stream");
    drop(writer);
    SchemaIpcPayload::new(payload).expect("nonempty schema IPC payload")
}

fn table_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ],
        HashMap::from([
            ("source".to_owned(), "sqlite-manifest-test".to_owned()),
            ("version".to_owned(), "1".to_owned()),
        ]),
    )
}

fn append_definition() -> TableDefinition {
    TableDefinition::Append(AppendTableDefinition {
        event_time_column: Some(
            ColumnName::new("event_time").expect("valid event-time column name"),
        ),
    })
}

fn command(
    request: u128,
    expected_revision: Option<u64>,
    command: CatalogCommandV1,
) -> VersionedCatalogCommand {
    command_with_actor(request, expected_revision, server_actor(), command)
}

fn command_with_actor(
    request: u128,
    expected_revision: Option<u64>,
    actor: CatalogActor,
    command: CatalogCommandV1,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(request_id(request), expected_revision, actor, command)
        .expect("valid catalog command envelope")
        .into()
}

async fn create_append_table(catalog: &SqliteCatalog) {
    catalog
        .command(command(
            1,
            Some(0),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: namespace_id(NAMESPACE),
                name: CatalogName::new("market").expect("valid namespace name"),
            }),
        ))
        .await
        .expect("create namespace");
    catalog
        .command(command(
            2,
            Some(1),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: table_id(TABLE),
                namespace_id: namespace_id(NAMESPACE),
                name: CatalogName::new("ticks").expect("valid table name"),
                definition: append_definition(),
                schema_ipc: schema_ipc(&table_schema()),
                config: BTreeMap::new(),
            }),
        ))
        .await
        .expect("create append table");
}

fn offset_range(first: u64, last: u64) -> OffsetRange {
    OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
        .expect("ordered nonempty offset range")
}

fn source_lineage(
    segment_id: u64,
    generation: u64,
    writer_epoch: u64,
    first: u64,
    last: u64,
) -> SourceSegmentLineage {
    SourceSegmentLineage {
        source: SourceSegmentIdentity {
            segment_id,
            generation,
            writer_epoch,
        },
        offsets: offset_range(first, last),
    }
}

fn native_file_with_lineage(
    file: u128,
    table: u128,
    row_count: u64,
    first: u64,
    last: u64,
    lineages: Vec<SourceSegmentLineage>,
) -> PendingDataFile {
    let source_lineage =
        SourceSegmentLineages::new(lineages).expect("valid ordered source lineage");
    let provenance = NativeDataFileProvenance::new(offset_range(first, last), source_lineage)
        .expect("lineage must exactly cover native file offsets");
    PendingDataFile {
        file_id: file_id(file),
        table_id: table_id(table),
        schema_version: 1,
        location: DataFileLocation::Managed(
            ManagedDataPath::new(format!("tables/{table}/{file}.parquet"))
                .expect("valid managed data path"),
        ),
        file_size_bytes: row_count * 128,
        row_count: NonZeroU64::new(row_count).expect("test data files have rows"),
        provenance: DataFileProvenance::Native(provenance),
        partition: MetadataMap::new(),
        statistics: MetadataMap::new(),
        metadata: MetadataMap::new(),
        checksum: format!("sha256-v1:{file:064x}"),
        created_at_ns: 1_000 + u64::try_from(file).expect("small test file identity"),
    }
}

fn native_file(
    file: u128,
    table: u128,
    row_count: u64,
    first: u64,
    last: u64,
    segment_id: u64,
    writer_epoch: u64,
) -> PendingDataFile {
    native_file_with_lineage(
        file,
        table,
        row_count,
        first,
        last,
        vec![source_lineage(segment_id, 1, writer_epoch, first, last)],
    )
}

fn legacy_file(file: u128, table: u128) -> PendingDataFile {
    PendingDataFile {
        file_id: file_id(file),
        table_id: table_id(table),
        schema_version: 1,
        location: DataFileLocation::ExternalLegacy(
            ExternalDataPath::new(format!("/legacy/{table}/{file}.parquet"))
                .expect("valid external legacy path"),
        ),
        file_size_bytes: 128,
        row_count: NonZeroU64::new(1).expect("nonzero row count"),
        provenance: DataFileProvenance::Legacy {
            offsets: Some(offset_range(0, 0)),
            preserve_discovery_order: false,
        },
        partition: MetadataMap::new(),
        statistics: MetadataMap::new(),
        metadata: MetadataMap::new(),
        checksum: format!("sha256-v1:{file:064x}"),
        created_at_ns: 1_000 + u64::try_from(file).expect("small test file identity"),
    }
}

fn commit_data_files(
    request: u128,
    expected_revision: u64,
    expected_snapshot_version: u64,
    files: Vec<PendingDataFile>,
    committed_log_offset: OffsetWatermark,
) -> VersionedCatalogCommand {
    command(
        request,
        Some(expected_revision),
        CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
            table_id: table_id(TABLE),
            expected_snapshot_version,
            files,
            committed_log_offset,
        }),
    )
}

fn commit_compaction(
    request: u128,
    expected_revision: u64,
    expected_snapshot_version: u64,
    input_file_ids: Vec<FileId>,
    output_files: Vec<PendingDataFile>,
    committed_log_offset: OffsetWatermark,
) -> VersionedCatalogCommand {
    command(
        request,
        Some(expected_revision),
        CatalogCommandV1::CommitCompaction(CommitCompactionCommand {
            table_id: table_id(TABLE),
            expected_snapshot_version,
            input_file_ids,
            output_files,
            committed_log_offset,
        }),
    )
}

async fn persist_two_inputs(
    catalog: &SqliteCatalog,
) -> (PendingDataFile, PendingDataFile, VersionedCatalogResponse) {
    let first = native_file(10, TABLE, 2, 0, 1, 1, 0);
    let second = native_file(20, TABLE, 2, 2, 3, 2, 0);
    let response = catalog
        .command(commit_data_files(
            3,
            2,
            1,
            vec![first.clone(), second.clone()],
            OffsetWatermark::Known(LogOffset::new(3)),
        ))
        .await
        .expect("commit compaction input files");
    (first, second, response)
}

fn compacted_file(file: u128, row_count: u64) -> PendingDataFile {
    native_file_with_lineage(
        file,
        TABLE,
        row_count,
        0,
        3,
        vec![source_lineage(1, 1, 0, 0, 1), source_lineage(2, 1, 0, 2, 3)],
    )
}

async fn exact_snapshot(catalog: &SqliteCatalog, snapshot_version: u64) -> CatalogQuerySnapshot {
    catalog
        .read_snapshot_at(&[CatalogSnapshotSelector {
            table_id: table_id(TABLE),
            snapshot_version,
            schema_version: 1,
        }])
        .await
        .expect("read exact table snapshot")
}

fn snapshot_outcome(response: &VersionedCatalogResponse) -> &zippy_types::table::TableSnapshot {
    let VersionedCatalogResponse::V1(response) = response;
    let CatalogOutcomeV1::Snapshot(snapshot) = &response.outcome else {
        panic!("manifest command must return a snapshot outcome");
    };
    snapshot
}

fn assert_revision(connection: &Connection, expected: u64) {
    let revision: Vec<u8> = connection
        .query_row(
            "SELECT value FROM catalog_meta WHERE key = 'catalog_revision'",
            [],
            |row| row.get(0),
        )
        .expect("read raw catalog revision");
    assert_eq!(
        u64::from_be_bytes(revision.try_into().expect("eight-byte catalog revision")),
        expected
    );
}

fn assert_current_snapshot(connection: &Connection, expected: u64) {
    let snapshot_version: Vec<u8> = connection
        .query_row(
            "SELECT current_snapshot_version FROM tables WHERE table_id = ?1",
            [table_id(TABLE).into_bytes().as_slice()],
            |row| row.get(0),
        )
        .expect("read raw current snapshot version");
    assert_eq!(
        u64::from_be_bytes(
            snapshot_version
                .try_into()
                .expect("eight-byte current snapshot version"),
        ),
        expected
    );
}

fn assert_raw_count(connection: &Connection, table: &str, expected: i64) {
    let count: i64 = connection
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .expect("count raw catalog rows");
    assert_eq!(count, expected, "unexpected raw row count table=[{table}]");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commits_native_files_reads_exact_history_restarts_and_replays() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;

    let first_file = native_file(10, TABLE, 2, 0, 1, 1, 0);
    let second_file = native_file(20, TABLE, 2, 2, 3, 2, 0);
    let command = commit_data_files(
        3,
        2,
        1,
        vec![first_file.clone(), second_file.clone()],
        OffsetWatermark::Known(LogOffset::new(3)),
    );
    let first_response = catalog
        .command(command.clone())
        .await
        .expect("commit native data files");
    let VersionedCatalogResponse::V1(response) = &first_response;
    assert_eq!(response.catalog_revision, 3);
    let committed = snapshot_outcome(&first_response);
    assert_eq!(committed.table_id, table_id(TABLE));
    assert_eq!(committed.snapshot_version, 2);
    assert_eq!(committed.parent_snapshot_version, Some(1));
    assert_eq!(committed.operation, SnapshotOperation::Persist);
    assert_eq!(committed.commit_lsn, 3);
    assert_eq!(
        committed.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(3))
    );
    assert!(committed.created_at_ns > 0);
    assert_eq!(catalog.revision().await, Ok(3));

    let conflicting_reuse = commit_data_files(
        3,
        2,
        1,
        vec![native_file(11, TABLE, 4, 0, 3, 1, 0)],
        OffsetWatermark::Known(LogOffset::new(3)),
    );
    let error = catalog
        .command(conflicting_reuse)
        .await
        .expect_err("same request id with another fingerprint must conflict");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(error.request_id(), Some(request_id(3)));
    assert_eq!(catalog.revision().await, Ok(3));

    let version_one = exact_snapshot(&catalog, 1).await;
    assert!(version_one.tables()[0].files().is_empty());
    let version_two = exact_snapshot(&catalog, 2).await;
    let files = version_two.tables()[0].files().as_slice();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].file, first_file);
    assert_eq!(files[1].file, second_file);
    for descriptor in files {
        assert_eq!(descriptor.added_snapshot_version, 2);
        assert_eq!(descriptor.removed_snapshot_version, None);
        assert_eq!(descriptor.state, DataFileState::Committed);
        let DataFileProvenance::Native(provenance) = &descriptor.file.provenance else {
            panic!("persisted server file must retain typed native provenance");
        };
        assert_eq!(provenance.source_lineage().as_slice().len(), 1);
        assert_eq!(
            provenance.source_lineage().as_slice()[0]
                .source
                .writer_epoch,
            0
        );
    }
    catalog.close().expect("close sqlite catalog");

    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen sqlite catalog");
    let after_restart = exact_snapshot(&catalog, 2).await;
    assert_eq!(after_restart.tables()[0].files().as_slice(), files);
    let replay = catalog
        .command(command)
        .await
        .expect("replay persisted data-file commit");
    assert_eq!(replay, first_response);
    assert_eq!(catalog.revision().await, Ok(3));
    catalog.close().expect("close reopened catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_invalid_data_file_commits_without_advancing_revision() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;

    let raw = Connection::open(catalog_path(&data_directory)).expect("open schema test connection");
    raw.execute(
        "INSERT INTO table_schemas(\
             table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns\
         ) VALUES (?1, ?2, ?3, 'future-schema', 'full', ?4)",
        rusqlite::params![
            table_id(TABLE).into_bytes().as_slice(),
            2_u64.to_be_bytes().as_slice(),
            [1_u8].as_slice(),
            1_u64.to_be_bytes().as_slice(),
        ],
    )
    .expect("insert a physically present future schema");
    drop(raw);

    let mut future_schema_file = native_file(10, TABLE, 1, 0, 0, 1, 0);
    future_schema_file.schema_version = 2;
    let mut noncanonical_checksum = native_file(10, TABLE, 1, 0, 0, 1, 0);
    noncanonical_checksum.checksum = "sha256:AB".to_owned();

    let cases = vec![
        (
            "empty file list",
            commit_data_files(10, 2, 1, Vec::new(), OffsetWatermark::Empty),
            ErrorCode::InvalidArgument,
        ),
        (
            "unsorted file ids",
            commit_data_files(
                11,
                2,
                1,
                vec![
                    native_file(20, TABLE, 1, 1, 1, 2, 0),
                    native_file(10, TABLE, 1, 0, 0, 1, 0),
                ],
                OffsetWatermark::Known(LogOffset::new(1)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "cross-table file",
            commit_data_files(
                12,
                2,
                1,
                vec![native_file(10, TABLE + 1, 1, 0, 0, 1, 0)],
                OffsetWatermark::Known(LogOffset::new(0)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "legacy file",
            commit_data_files(
                13,
                2,
                1,
                vec![legacy_file(10, TABLE)],
                OffsetWatermark::Known(LogOffset::new(0)),
            ),
            ErrorCode::UnsupportedPlan,
        ),
        (
            "future writer epoch",
            commit_data_files(
                14,
                2,
                1,
                vec![native_file(10, TABLE, 1, 0, 0, 1, 1)],
                OffsetWatermark::Known(LogOffset::new(0)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "schema newer than table cursor",
            commit_data_files(
                16,
                2,
                1,
                vec![future_schema_file],
                OffsetWatermark::Known(LogOffset::new(0)),
            ),
            ErrorCode::SchemaMismatch,
        ),
        (
            "noncanonical checksum",
            commit_data_files(
                17,
                2,
                1,
                vec![noncanonical_checksum],
                OffsetWatermark::Known(LogOffset::new(0)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "non-server actor",
            command_with_actor(
                18,
                Some(2),
                CatalogActor {
                    kind: CatalogActorKind::User,
                    principal: "integration-test-user".to_owned(),
                },
                CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                    table_id: table_id(TABLE),
                    expected_snapshot_version: 1,
                    files: vec![native_file(10, TABLE, 1, 0, 0, 1, 0)],
                    committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
                }),
            ),
            ErrorCode::PermissionDenied,
        ),
        (
            "watermark gap",
            commit_data_files(
                15,
                2,
                1,
                vec![native_file(10, TABLE, 1, 1, 1, 1, 0)],
                OffsetWatermark::Known(LogOffset::new(1)),
            ),
            ErrorCode::Conflict,
        ),
    ];
    for (reason, command, expected_code) in cases {
        let error = catalog
            .command(command)
            .await
            .expect_err("invalid data-file commit must be rejected");
        assert_eq!(
            error.code(),
            expected_code,
            "unexpected error reason=[{reason}]"
        );
        assert_eq!(
            catalog.revision().await,
            Ok(2),
            "revision advanced reason=[{reason}]"
        );
    }
    assert!(exact_snapshot(&catalog, 1).await.tables()[0]
        .files()
        .is_empty());
    catalog.close().expect("close sqlite catalog");

    let raw = Connection::open(catalog_path(&data_directory)).expect("inspect raw catalog");
    assert_revision(&raw, 2);
    assert_current_snapshot(&raw, 1);
    assert_raw_count(&raw, "table_snapshots", 1);
    assert_raw_count(&raw, "data_files", 0);
    assert_raw_count(&raw, "data_file_sources", 0);
    assert_raw_count(&raw, "idempotent_requests", 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_manifest_before_queue_when_byte_budget_is_exhausted() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&data_directory).with_manifest_queue_capacity_bytes(1),
    )
    .expect("open sqlite catalog with a tiny manifest budget");
    create_append_table(&catalog).await;

    let error = catalog
        .command(commit_data_files(
            3,
            2,
            1,
            vec![native_file(10, TABLE, 1, 0, 0, 1, 0)],
            OffsetWatermark::Known(LogOffset::new(0)),
        ))
        .await
        .expect_err("manifest reservation must respect the queue byte budget");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert_eq!(error.request_id(), Some(request_id(3)));
    assert_eq!(catalog.revision().await, Ok(2));
    assert!(exact_snapshot(&catalog, 1).await.tables()[0]
        .files()
        .is_empty());
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounds_actor_principal_and_charges_it_to_the_manifest_queue() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&data_directory)
            .with_manifest_queue_capacity_bytes(MAX_CATALOG_ACTOR_PRINCIPAL_BYTES),
    )
    .expect("open sqlite catalog with a principal-sized manifest budget");
    create_append_table(&catalog).await;

    for (request, principal_bytes, expected_message) in [
        (
            3,
            MAX_CATALOG_ACTOR_PRINCIPAL_BYTES + 1,
            "actor principal exceeds byte limit",
        ),
        (
            4,
            MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
            "manifest command exceeds queue byte budget",
        ),
    ] {
        let actor = CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "p".repeat(principal_bytes),
        };
        let body = CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
            table_id: table_id(TABLE),
            expected_snapshot_version: 1,
            files: vec![native_file(10, TABLE, 1, 0, 0, 1, 0)],
            committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
        });
        let command = if principal_bytes > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
            VersionedCatalogCommand::V1(CatalogCommandEnvelopeV1 {
                format_version: CatalogCommandFormatVersion::v1(),
                request_id: request_id(request),
                expected_revision: Some(2),
                actor,
                command: body,
                canonical_fingerprint: CommandFingerprint::from_bytes([0; 32]),
            })
        } else {
            command_with_actor(request, Some(2), actor, body)
        };
        let error = catalog
            .command(command)
            .await
            .expect_err("principal admission must reject the command");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(!error.retryable());
        assert!(
            error.message().contains(expected_message),
            "unexpected principal admission error error=[{error}]"
        );
        assert_eq!(error.request_id(), Some(request_id(request)));
        assert_eq!(catalog.revision().await, Ok(2));
    }

    catalog
        .command(commit_data_files(
            5,
            2,
            1,
            vec![native_file(10, TABLE, 1, 0, 0, 1, 0)],
            OffsetWatermark::Known(LogOffset::new(0)),
        ))
        .await
        .expect("ordinary principal must fit the same queue budget");
    assert_eq!(catalog.revision().await, Ok(3));
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolls_back_data_file_snapshot_when_idempotency_insert_fails() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;

    let raw = Connection::open(catalog_path(&data_directory)).expect("open trigger connection");
    raw.execute_batch(
        "CREATE TRIGGER fail_commit_data_files_idempotency \
         BEFORE INSERT ON idempotent_requests \
         WHEN NEW.operation = 'commit_data_files' \
         BEGIN \
             SELECT RAISE(ABORT, 'injected commit_data_files failure'); \
         END",
    )
    .expect("install persist failure trigger");

    let error = catalog
        .command(commit_data_files(
            3,
            2,
            1,
            vec![native_file(10, TABLE, 1, 0, 0, 1, 0)],
            OffsetWatermark::Known(LogOffset::new(0)),
        ))
        .await
        .expect_err("trigger must abort data-file commit");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(catalog.revision().await, Ok(2));
    assert!(exact_snapshot(&catalog, 1).await.tables()[0]
        .files()
        .is_empty());

    assert_revision(&raw, 2);
    assert_current_snapshot(&raw, 1);
    assert_raw_count(&raw, "table_snapshots", 1);
    assert_raw_count(&raw, "data_files", 0);
    assert_raw_count(&raw, "data_file_sources", 0);
    assert_raw_count(&raw, "idempotent_requests", 2);
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compacts_two_inputs_preserves_history_and_replays_idempotently() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;
    let (first_input, second_input, _) = persist_two_inputs(&catalog).await;
    let output = compacted_file(30, 4);
    let command = commit_compaction(
        4,
        3,
        2,
        vec![file_id(10), file_id(20)],
        vec![output.clone()],
        OffsetWatermark::Known(LogOffset::new(3)),
    );

    let first_response = catalog
        .command(command.clone())
        .await
        .expect("commit compaction");
    let VersionedCatalogResponse::V1(response) = &first_response;
    assert_eq!(response.catalog_revision, 4);
    let committed = snapshot_outcome(&first_response);
    assert_eq!(committed.table_id, table_id(TABLE));
    assert_eq!(committed.snapshot_version, 3);
    assert_eq!(committed.parent_snapshot_version, Some(2));
    assert_eq!(committed.operation, SnapshotOperation::Compaction);
    assert_eq!(committed.commit_lsn, 4);
    assert_eq!(
        committed.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(3))
    );

    let version_two = exact_snapshot(&catalog, 2).await;
    let version_two_files = version_two.tables()[0].files().as_slice();
    assert_eq!(version_two_files.len(), 2);
    assert_eq!(version_two_files[0].file, first_input);
    assert_eq!(version_two_files[1].file, second_input);
    for input in version_two_files {
        assert_eq!(input.added_snapshot_version, 2);
        assert_eq!(input.removed_snapshot_version, Some(3));
        assert_eq!(input.state, DataFileState::Committed);
    }

    let version_three = exact_snapshot(&catalog, 3).await;
    let version_three_files = version_three.tables()[0].files().as_slice();
    assert_eq!(version_three_files.len(), 1);
    assert_eq!(version_three_files[0].file, output);
    assert_eq!(version_three_files[0].added_snapshot_version, 3);
    assert_eq!(version_three_files[0].removed_snapshot_version, None);
    assert_eq!(version_three_files[0].state, DataFileState::Committed);

    catalog.close().expect("close compacted catalog");
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen compacted catalog");
    let restarted_version_two = exact_snapshot(&catalog, 2).await;
    assert_eq!(
        restarted_version_two.tables()[0].files().as_slice(),
        version_two_files
    );
    let restarted_version_three = exact_snapshot(&catalog, 3).await;
    assert_eq!(
        restarted_version_three.tables()[0].files().as_slice(),
        version_three_files
    );

    let replay = catalog
        .command(command)
        .await
        .expect("replay compaction command");
    assert_eq!(replay, first_response);
    assert_eq!(catalog.revision().await, Ok(4));

    let stale_input = catalog
        .command(commit_compaction(
            5,
            4,
            3,
            vec![file_id(10), file_id(20)],
            vec![compacted_file(40, 4)],
            OffsetWatermark::Known(LogOffset::new(3)),
        ))
        .await
        .expect_err("removed compaction inputs must be stale");
    assert_eq!(stale_input.code(), ErrorCode::Conflict);
    assert_eq!(catalog.revision().await, Ok(4));
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_invalid_compactions_and_rolls_back_injected_failure() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;
    persist_two_inputs(&catalog).await;

    let raw = Connection::open(catalog_path(&data_directory)).expect("open schema test connection");
    raw.execute(
        "INSERT INTO table_schemas(\
             table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns\
         ) VALUES (?1, ?2, ?3, 'compaction-schema-v2', 'full', ?4)",
        rusqlite::params![
            table_id(TABLE).into_bytes().as_slice(),
            2_u64.to_be_bytes().as_slice(),
            [1_u8].as_slice(),
            2_u64.to_be_bytes().as_slice(),
        ],
    )
    .expect("insert second schema version");
    raw.execute(
        "UPDATE tables SET current_schema_version = ?1 WHERE table_id = ?2",
        rusqlite::params![
            2_u64.to_be_bytes().as_slice(),
            table_id(TABLE).into_bytes().as_slice(),
        ],
    )
    .expect("advance table schema cursor");
    drop(raw);

    let mismatched_lineage = native_file_with_lineage(
        33,
        TABLE,
        4,
        0,
        3,
        vec![source_lineage(1, 1, 0, 0, 1), source_lineage(3, 1, 0, 2, 3)],
    );
    let mut rewritten_schema = compacted_file(35, 4);
    rewritten_schema.schema_version = 2;
    let cases = vec![
        (
            "stale expected snapshot",
            commit_compaction(
                10,
                3,
                1,
                vec![file_id(10), file_id(20)],
                vec![compacted_file(30, 4)],
                OffsetWatermark::Known(LogOffset::new(3)),
            ),
            ErrorCode::Conflict,
        ),
        (
            "missing input",
            commit_compaction(
                11,
                3,
                2,
                vec![file_id(10), file_id(99)],
                vec![compacted_file(31, 4)],
                OffsetWatermark::Known(LogOffset::new(3)),
            ),
            ErrorCode::Conflict,
        ),
        (
            "changed watermark",
            commit_compaction(
                12,
                3,
                2,
                vec![file_id(10), file_id(20)],
                vec![compacted_file(32, 4)],
                OffsetWatermark::Known(LogOffset::new(4)),
            ),
            ErrorCode::Conflict,
        ),
        (
            "mismatched source lineage",
            commit_compaction(
                13,
                3,
                2,
                vec![file_id(10), file_id(20)],
                vec![mismatched_lineage],
                OffsetWatermark::Known(LogOffset::new(3)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "mismatched logical row count",
            commit_compaction(
                14,
                3,
                2,
                vec![file_id(10), file_id(20)],
                vec![compacted_file(34, 3)],
                OffsetWatermark::Known(LogOffset::new(3)),
            ),
            ErrorCode::InvalidArgument,
        ),
        (
            "schema rewrite",
            commit_compaction(
                15,
                3,
                2,
                vec![file_id(10), file_id(20)],
                vec![rewritten_schema],
                OffsetWatermark::Known(LogOffset::new(3)),
            ),
            ErrorCode::InvalidArgument,
        ),
    ];
    for (reason, command, expected_code) in cases {
        let error = catalog
            .command(command)
            .await
            .expect_err("invalid compaction must be rejected");
        assert_eq!(
            error.code(),
            expected_code,
            "unexpected error reason=[{reason}]"
        );
        assert_eq!(
            catalog.revision().await,
            Ok(3),
            "revision advanced reason=[{reason}]"
        );
    }

    let before_failure = exact_snapshot(&catalog, 2).await;
    for input in before_failure.tables()[0].files().as_slice() {
        assert_eq!(input.removed_snapshot_version, None);
    }
    let raw = Connection::open(catalog_path(&data_directory)).expect("open trigger connection");
    raw.execute_batch(
        "CREATE TRIGGER fail_commit_compaction_idempotency \
         BEFORE INSERT ON idempotent_requests \
         WHEN NEW.operation = 'commit_compaction' \
         BEGIN \
             SELECT RAISE(ABORT, 'injected commit_compaction failure'); \
         END",
    )
    .expect("install compaction failure trigger");
    let error = catalog
        .command(commit_compaction(
            20,
            3,
            2,
            vec![file_id(10), file_id(20)],
            vec![compacted_file(30, 4)],
            OffsetWatermark::Known(LogOffset::new(3)),
        ))
        .await
        .expect_err("trigger must abort compaction");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(catalog.revision().await, Ok(3));

    let after_failure = exact_snapshot(&catalog, 2).await;
    assert_eq!(after_failure.tables()[0].files().as_slice().len(), 2);
    for input in after_failure.tables()[0].files().as_slice() {
        assert_eq!(input.removed_snapshot_version, None);
    }
    assert_revision(&raw, 3);
    assert_current_snapshot(&raw, 2);
    assert_raw_count(&raw, "table_snapshots", 2);
    assert_raw_count(&raw, "data_files", 2);
    assert_raw_count(&raw, "data_file_sources", 2);
    assert_raw_count(&raw, "idempotent_requests", 3);
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_compaction_when_persisted_input_lineage_is_corrupt() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    create_append_table(&catalog).await;
    persist_two_inputs(&catalog).await;

    let raw = Connection::open(catalog_path(&data_directory)).expect("open corruption connection");
    raw.execute(
        "DELETE FROM data_file_sources WHERE file_id = ?1",
        [file_id(10).into_bytes().as_slice()],
    )
    .expect("remove persisted source lineage");

    let error = catalog
        .command(commit_compaction(
            4,
            3,
            2,
            vec![file_id(10), file_id(20)],
            vec![compacted_file(30, 4)],
            OffsetWatermark::Known(LogOffset::new(3)),
        ))
        .await
        .expect_err("corrupt persisted lineage must stop compaction");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(catalog.revision().await, Ok(3));
    assert_current_snapshot(&raw, 2);
    assert_raw_count(&raw, "data_files", 2);
    catalog.close().expect("close sqlite catalog");
}
