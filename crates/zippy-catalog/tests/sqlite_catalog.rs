use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig, SqliteCatalogSnapshotReadLimits};
use zippy_core::{DataDirectoryError, ManagedDataDirectory, OwnedDataDirectory};
use zippy_types::catalog::{
    AcquireProducerCommand, AcquireSnapshotLeaseCommand, CatalogActor, CatalogActorKind,
    CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogOutcomeV1, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, CreateTableCommand,
    ListNamespacesRequest, ListTablesRequest, NamespaceRef, ReleaseSnapshotLeaseCommand,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::storage::{CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseIssuer};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
    KeyValueTableDefinition, SchemaCompatibility, SchemaIpcPayload, SnapshotOperation,
    TableDefinition, TableRef, TableState,
};
use zippy_types::{
    ErrorCode, FileId, LeaseId, NamespaceId, OffsetWatermark, ProducerId, RequestId,
    SequenceWatermark, ServerErrorDetails, TableId,
};

type RawInitialSnapshotRow = (Vec<u8>, Vec<u8>, String, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

const MAX_SQLITE_CATALOG_NAME_BYTES: usize = 1024;

fn snapshot_memory_lease(
    issuer: &CatalogSnapshotMemoryLeaseIssuer,
    selector_count: usize,
) -> CatalogSnapshotMemoryLease {
    let reserved_bytes = SqliteCatalogSnapshotReadLimits::default()
        .conservative_owned_peak_bytes(selector_count)
        .expect("estimate integration snapshot memory");
    issuer
        .issue_with_owner(reserved_bytes, None, |_| Ok(Box::new(())))
        .expect("issue integration snapshot memory lease")
}

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

fn actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::User,
        principal: "integration-test".to_owned(),
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

fn producer_id(value: u128) -> ProducerId {
    ProducerId::from_u128(value).expect("test producer id must be nonzero")
}

fn acquire_producer(
    request: u128,
    table: u128,
    producer: u128,
    expected_writer_epoch: Option<u64>,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(request),
        None,
        actor(),
        CatalogCommandV1::AcquireProducer(AcquireProducerCommand {
            table_id: table_id(table),
            producer_id: producer_id(producer),
            expected_writer_epoch,
        }),
    )
    .expect("create producer acquisition command envelope")
    .into()
}

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("test file id must be nonzero")
}

fn lease_id(value: u128) -> LeaseId {
    LeaseId::from_u128(value).expect("test lease id must be nonzero")
}

fn snapshot_lease_command(request: u128, command: CatalogCommandV1) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(request_id(request), None, actor(), command)
        .expect("create snapshot lease command envelope")
        .into()
}

fn create_namespace(
    request: u128,
    namespace: u128,
    name: &str,
    expected_revision: Option<u64>,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(request),
        expected_revision,
        actor(),
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: namespace_id(namespace),
            name: CatalogName::new(name).expect("valid namespace name"),
        }),
    )
    .expect("create namespace command envelope")
    .into()
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
            Field::new("price", DataType::Float64, true),
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ],
        HashMap::from([
            ("source".to_owned(), "integration".to_owned()),
            ("version".to_owned(), "1".to_owned()),
        ]),
    )
}

fn create_table(
    request: u128,
    table: u128,
    namespace: u128,
    name: &str,
    expected_revision: Option<u64>,
    definition: TableDefinition,
    schema: SchemaIpcPayload,
) -> VersionedCatalogCommand {
    create_table_command(
        request,
        expected_revision,
        CreateTableCommand {
            table_id: table_id(table),
            namespace_id: namespace_id(namespace),
            name: CatalogName::new(name).expect("valid table name"),
            definition,
            schema_ipc: schema,
            config: BTreeMap::from([
                ("durability".to_owned(), "fsync".to_owned()),
                ("retention".to_owned(), "none".to_owned()),
            ]),
        },
    )
}

fn create_table_command(
    request: u128,
    expected_revision: Option<u64>,
    command: CreateTableCommand,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(request),
        expected_revision,
        actor(),
        CatalogCommandV1::CreateTable(command),
    )
    .expect("create table command envelope")
    .into()
}

fn append_definition() -> TableDefinition {
    TableDefinition::Append(AppendTableDefinition {
        event_time_column: Some(ColumnName::new("dt").expect("valid event-time column")),
    })
}

fn decode_u64_blob(value: Vec<u8>) -> u64 {
    u64::from_be_bytes(value.try_into().expect("eight-byte unsigned encoding"))
}

fn encode_optional_u64(value: Option<u64>) -> [u8; 9] {
    let mut encoded = [0_u8; 9];
    if let Some(value) = value {
        encoded[0] = 1;
        encoded[1..].copy_from_slice(&value.to_be_bytes());
    }
    encoded
}

fn empty_metadata_blob(kind: &str) -> Vec<u8> {
    const HASH_DOMAIN: &[u8] = b"zippy.catalog.typed-json.v1\0";
    const PAYLOAD: &[u8] = b"{}";

    let mut digest = Sha256::new();
    digest.update(HASH_DOMAIN);
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(PAYLOAD);
    let mut encoded = b"ZCJ\x01".to_vec();
    encoded.extend_from_slice(&digest.finalize());
    encoded.extend_from_slice(PAYLOAD);
    encoded
}

fn insert_native_data_file(connection: &Connection, table: TableId, file: FileId) {
    let table_id = table.into_bytes();
    let file_id = file.into_bytes();
    let schema_version = 1_u64.to_be_bytes();
    let file_size_bytes = 1_024_u64.to_be_bytes();
    let row_count = 10_u64.to_be_bytes();
    let first_offset = encode_optional_u64(Some(0));
    let last_offset = encode_optional_u64(Some(9));
    let absent = encode_optional_u64(None);
    let added_snapshot_version = 2_u64.to_be_bytes();
    let created_at_ns = 1_u64.to_be_bytes();
    let partition_blob = empty_metadata_blob("data_file_partition");
    let statistics_blob = empty_metadata_blob("data_file_statistics");
    let metadata_blob = empty_metadata_blob("data_file_metadata");
    connection
        .execute(
            "INSERT INTO data_files(\
                 file_id, table_id, schema_version, location_kind, file_path, \
                 file_size_bytes, row_count, provenance_kind, min_log_offset, \
                 max_log_offset, \
                 partition_blob, statistics_blob, metadata_blob, checksum, \
                 added_snapshot_version, removed_snapshot_version, state, created_at_ns\
             ) VALUES (\
                 ?1, ?2, ?3, 'managed', ?4, ?5, ?6, 'native', ?7, ?8, \
                 ?10, ?11, ?12, ?13, ?14, ?9, 'committed', ?15\
             )",
            rusqlite::params![
                file_id.as_slice(),
                table_id.as_slice(),
                schema_version.as_slice(),
                format!("tables/test/{}.parquet", file),
                file_size_bytes.as_slice(),
                row_count.as_slice(),
                first_offset.as_slice(),
                last_offset.as_slice(),
                absent.as_slice(),
                partition_blob,
                statistics_blob,
                metadata_blob,
                format!("sha256-v1:{:032x}", u128::from_be_bytes(file_id)),
                added_snapshot_version.as_slice(),
                created_at_ns.as_slice(),
            ],
        )
        .expect("insert raw native data file");
    connection
        .execute(
            "INSERT INTO data_file_sources(\
                 file_id, source_segment_id, source_generation, writer_epoch, \
                 min_log_offset, max_log_offset\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                file_id.as_slice(),
                1_u64.to_be_bytes().as_slice(),
                1_u64.to_be_bytes().as_slice(),
                1_u64.to_be_bytes().as_slice(),
                0_u64.to_be_bytes().as_slice(),
                9_u64.to_be_bytes().as_slice(),
            ],
        )
        .expect("insert raw native source lineage");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replays_create_namespace_exactly_across_restart_and_serves_reads() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let command = create_namespace(1, 1, "market", Some(0));
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    let first = catalog
        .command(command.clone())
        .await
        .expect("create namespace");
    let first_bytes = CatalogCodec::encode_response(&first).expect("encode first response");
    assert_eq!(catalog.revision().await, Ok(1));
    catalog.close().expect("close sqlite catalog");

    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&data_directory).with_busy_timeout(Duration::from_millis(25)),
    )
    .expect("reopen sqlite catalog");
    let replay = catalog
        .command(command.clone())
        .await
        .expect("replay command");
    assert_eq!(
        CatalogCodec::encode_response(&replay).expect("encode replay response"),
        first_bytes
    );
    assert_eq!(catalog.revision().await, Ok(1));

    let blocker = Connection::open(catalog_path(&data_directory)).expect("open lock holder");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold sqlite writer lock");
    let replay_while_write_locked = catalog
        .command(command)
        .await
        .expect("replay must not acquire a writer lock");
    assert_eq!(
        CatalogCodec::encode_response(&replay_while_write_locked)
            .expect("encode write-locked replay response"),
        first_bytes
    );
    assert_eq!(catalog.revision().await, Ok(1));
    blocker
        .execute_batch("ROLLBACK")
        .expect("release sqlite writer lock");

    let by_id = catalog
        .get_namespace(NamespaceRef::Id(namespace_id(1)))
        .await
        .expect("get namespace by id");
    let by_name = catalog
        .get_namespace(NamespaceRef::Name(
            CatalogName::new("market").expect("valid namespace name"),
        ))
        .await
        .expect("get namespace by name");
    assert_eq!(by_id, by_name);
    let page = catalog
        .list_namespaces(ListNamespacesRequest {
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list namespaces");
    assert_eq!(page.catalog_revision, 1);
    assert_eq!(page.namespaces, [by_id]);
    assert_eq!(page.next_after, None);

    let conflict = catalog
        .command(create_namespace(1, 2, "changed", None))
        .await
        .expect_err("request id reuse must conflict");
    assert_eq!(conflict.code(), ErrorCode::Conflict);
    assert_eq!(catalog.revision().await, Ok(1));
    catalog.close().expect("close reopened catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creates_table_atomically_and_replays_exactly_across_restart() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    let command = create_table(
        2,
        1,
        1,
        "ticks",
        Some(1),
        append_definition(),
        schema_ipc(&table_schema()),
    );
    let first = catalog
        .command(command.clone())
        .await
        .expect("create table");
    let first_bytes = CatalogCodec::encode_response(&first).expect("encode table response");
    let VersionedCatalogResponse::V1(response) = &first;
    let CatalogOutcomeV1::Table(created) = &response.outcome else {
        panic!("CreateTable must return a table descriptor");
    };
    assert_eq!(response.catalog_revision, 2);
    assert_eq!(created.table_id, table_id(1));
    assert_eq!(created.current_schema_version, 1);
    assert_eq!(created.current_snapshot_version, 1);
    assert_eq!(created.writer_epoch, 0);
    assert_eq!(created.state, TableState::Active);
    assert_eq!(created.created_at_ns, created.updated_at_ns);
    assert!(created.created_at_ns > 0);
    assert_eq!(catalog.revision().await, Ok(2));
    assert_eq!(
        catalog
            .get_table(TableRef::from(table_id(1)))
            .await
            .expect("get created table"),
        *created
    );
    let selector = CatalogSnapshotSelector {
        table_id: table_id(1),
        snapshot_version: 1,
        schema_version: 1,
    };
    let query_snapshot = catalog
        .read_snapshot_at(std::slice::from_ref(&selector))
        .await
        .expect("read exact initial table snapshot");
    assert_eq!(query_snapshot.catalog_revision(), 2);
    assert_eq!(query_snapshot.tables().len(), 1);
    let table_snapshot = &query_snapshot.tables()[0];
    assert_eq!(table_snapshot.selector(), &selector);
    assert_eq!(table_snapshot.table(), created);
    assert_eq!(table_snapshot.schema().table_id, table_id(1));
    assert_eq!(table_snapshot.schema().schema_version, 1);
    assert_eq!(
        table_snapshot.schema().compatibility,
        SchemaCompatibility::Initial
    );
    assert_eq!(table_snapshot.snapshot().table_id, table_id(1));
    assert_eq!(table_snapshot.snapshot().snapshot_version, 1);
    assert_eq!(table_snapshot.snapshot().parent_snapshot_version, None);
    assert_eq!(
        table_snapshot.snapshot().operation,
        SnapshotOperation::Create
    );
    assert_eq!(table_snapshot.snapshot().commit_lsn, 2);
    assert!(table_snapshot.files().is_empty());
    let empty_snapshot = catalog
        .read_snapshot_at(&[])
        .await
        .expect("read empty snapshot after table creation");
    assert!(empty_snapshot.is_empty());
    assert_eq!(empty_snapshot.catalog_revision(), 2);
    catalog.close().expect("close catalog");

    let raw = Connection::open(catalog_path(&data_directory)).expect("inspect raw catalog");
    for (table, expected) in [
        ("tables", 1_i64),
        ("table_schemas", 1),
        ("table_snapshots", 1),
        ("data_files", 0),
        ("idempotent_requests", 2),
    ] {
        let count: i64 = raw
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count catalog rows");
        assert_eq!(count, expected, "unexpected row count table=[{table}]");
    }
    let table_row: (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) = raw
        .query_row(
            "SELECT definition_blob, config_blob, created_at_ns, updated_at_ns \
             FROM tables WHERE table_id = ?1",
            [table_id(1).into_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read raw table row");
    assert!(table_row.0.starts_with(b"ZCJ\x01"));
    assert!(table_row.1.starts_with(b"ZCJ\x01"));
    assert_eq!(table_row.2, table_row.3);
    let schema_row: (Vec<u8>, String, String, Vec<u8>) = raw
        .query_row(
            "SELECT schema_version, schema_hash, compatibility, created_at_ns \
             FROM table_schemas WHERE table_id = ?1",
            [table_id(1).into_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read initial schema");
    assert_eq!(decode_u64_blob(schema_row.0), 1);
    assert_eq!(schema_row.1.len(), "sha256-v1:".len() + 64);
    assert!(schema_row.1.starts_with("sha256-v1:"));
    assert_eq!(schema_row.2, "initial");
    assert_eq!(schema_row.3, table_row.2);
    let snapshot_row: RawInitialSnapshotRow = raw
        .query_row(
            "SELECT snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                    committed_log_offset, created_at_ns, summary_blob \
             FROM table_snapshots WHERE table_id = ?1",
            [table_id(1).into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .expect("read initial snapshot");
    assert_eq!(decode_u64_blob(snapshot_row.0), 1);
    assert_eq!(snapshot_row.1, vec![0; 9]);
    assert_eq!(snapshot_row.2, "create");
    assert_eq!(decode_u64_blob(snapshot_row.3), 2);
    assert_eq!(snapshot_row.4, vec![0; 9]);
    assert_eq!(snapshot_row.5, table_row.2);
    assert!(snapshot_row.6.starts_with(b"ZCJ\x01"));
    drop(raw);

    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&data_directory).with_busy_timeout(Duration::from_millis(25)),
    )
    .expect("reopen catalog");
    let blocker = Connection::open(catalog_path(&data_directory)).expect("open lock holder");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold writer lock");
    let replay = catalog
        .command(command)
        .await
        .expect("replay table creation without writer lock");
    assert_eq!(
        CatalogCodec::encode_response(&replay).expect("encode replay"),
        first_bytes
    );
    blocker
        .execute_batch("ROLLBACK")
        .expect("release writer lock");
    let by_name = catalog
        .get_table(TableRef::Name {
            namespace: CatalogName::new("market").expect("valid namespace"),
            table: CatalogName::new("ticks").expect("valid table"),
        })
        .await
        .expect("get table by name");
    assert_eq!(by_name.table_id, table_id(1));
    assert_eq!(catalog.revision().await, Ok(2));
    catalog.close().expect("close reopened catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_acquisition_fences_stale_epochs_and_replays_without_rollback() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(101, 101, "producer_test", Some(0)))
        .await
        .expect("create producer namespace");
    catalog
        .command(create_table(
            102,
            101,
            101,
            "ticks",
            Some(1),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect("create producer table");

    let first_command = acquire_producer(103, 101, 501, Some(0));
    let first = catalog
        .command(first_command.clone())
        .await
        .expect("acquire first producer epoch");
    let VersionedCatalogResponse::V1(first) = &first;
    let CatalogOutcomeV1::Producer(first_producer) = &first.outcome else {
        panic!("AcquireProducer must return a producer descriptor");
    };
    assert_eq!(first.catalog_revision, 3);
    assert_eq!(first_producer.table_id, table_id(101));
    assert_eq!(first_producer.producer_id, producer_id(501));
    assert_eq!(first_producer.producer_epoch, 1);
    assert_eq!(first_producer.last_batch_sequence, SequenceWatermark::Empty);
    assert_eq!(
        first_producer.last_committed_log_offset,
        OffsetWatermark::Empty
    );

    let replay = catalog
        .command(first_command.clone())
        .await
        .expect("replay first producer acquisition");
    assert_eq!(
        CatalogCodec::encode_response(&replay).expect("encode producer replay"),
        CatalogCodec::encode_response(&VersionedCatalogResponse::V1(first.clone()))
            .expect("encode first producer response")
    );
    assert_eq!(catalog.revision().await, Ok(3));

    let stale = catalog
        .command(acquire_producer(104, 101, 501, Some(0)))
        .await
        .expect_err("stale producer epoch must be fenced");
    assert_eq!(stale.code(), ErrorCode::StaleProducerEpoch);
    assert_eq!(
        stale.details(),
        Some(&ServerErrorDetails::ProducerEpoch {
            expected: 1,
            actual: 0,
        })
    );
    assert_eq!(catalog.revision().await, Ok(3));

    let second = catalog
        .command(acquire_producer(105, 101, 501, Some(1)))
        .await
        .expect("acquire second producer epoch");
    let VersionedCatalogResponse::V1(second) = second;
    let CatalogOutcomeV1::Producer(second_producer) = second.outcome else {
        panic!("AcquireProducer must return a producer descriptor");
    };
    assert_eq!(second.catalog_revision, 4);
    assert_eq!(second_producer.producer_epoch, 2);
    assert_eq!(
        second_producer.last_batch_sequence,
        SequenceWatermark::Empty
    );
    assert_eq!(
        catalog
            .get_table(TableRef::Id {
                table_id: table_id(101),
            })
            .await
            .expect("read producer table")
            .writer_epoch,
        2
    );
    catalog.close().expect("close sqlite catalog");

    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen sqlite catalog");
    let historical_replay = catalog
        .command(first_command)
        .await
        .expect("replay historical producer acquisition");
    let VersionedCatalogResponse::V1(historical_replay) = historical_replay;
    let CatalogOutcomeV1::Producer(historical) = historical_replay.outcome else {
        panic!("historical AcquireProducer replay must remain typed");
    };
    assert_eq!(historical.producer_epoch, 1);
    assert_eq!(catalog.revision().await, Ok(4));
    assert_eq!(
        catalog
            .get_table(TableRef::Id {
                table_id: table_id(101),
            })
            .await
            .expect("read reopened producer table")
            .writer_epoch,
        2
    );
    catalog.close().expect("close reopened sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_empty_contracts_and_rejects_missing_or_unimplemented_paths() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");

    assert_eq!(catalog.resolve_tables(&[]).await, Ok(Vec::new()));
    let snapshot = catalog
        .read_snapshot_at(&[])
        .await
        .expect("read empty query snapshot");
    assert!(snapshot.is_empty());
    assert_eq!(snapshot.catalog_revision(), 0);
    let page = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list empty tables");
    assert_eq!(page.catalog_revision, 0);
    assert!(page.tables.is_empty());

    let table_ref = TableRef::from(table_id(1));
    assert_eq!(
        catalog
            .get_table(table_ref.clone())
            .await
            .expect_err("missing table must fail")
            .code(),
        ErrorCode::NotFound
    );
    let maximum_name = CatalogName::new("x".repeat(MAX_SQLITE_CATALOG_NAME_BYTES))
        .expect("maximum-length namespace name must be valid");
    let table_name = CatalogName::new("t").expect("valid table name");
    let oversized_refs = (0..1024)
        .map(|_| TableRef::Name {
            namespace: maximum_name.clone(),
            table: table_name.clone(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        catalog
            .resolve_tables(&oversized_refs)
            .await
            .expect_err("oversized table references must fail before enqueue")
            .code(),
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        catalog
            .resolve_tables(&[table_ref])
            .await
            .expect_err("missing table resolution must fail")
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        catalog
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 1,
                schema_version: 1,
            }])
            .await
            .expect_err("missing selected table must fail")
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        catalog
            .command(create_table(
                1,
                1,
                1,
                "ticks",
                Some(0),
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("missing namespace must reject table creation")
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(catalog.revision().await, Ok(0));
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_selector_validation_rejects_oversized_unsorted_and_duplicate_inputs() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");

    let oversized = (1..=4_097_u128)
        .map(|table| CatalogSnapshotSelector {
            table_id: table_id(table),
            snapshot_version: 1,
            schema_version: 1,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        catalog
            .read_snapshot_at(&oversized)
            .await
            .expect_err("oversized selector list must fail before enqueue")
            .code(),
        ErrorCode::ResourceExhausted
    );

    let unsorted = [
        CatalogSnapshotSelector {
            table_id: table_id(2),
            snapshot_version: 1,
            schema_version: 1,
        },
        CatalogSnapshotSelector {
            table_id: table_id(1),
            snapshot_version: 1,
            schema_version: 1,
        },
    ];
    assert_eq!(
        catalog
            .read_snapshot_at(&unsorted)
            .await
            .expect_err("unsorted selectors must fail before enqueue")
            .code(),
        ErrorCode::InvalidArgument
    );

    let duplicate = [
        CatalogSnapshotSelector {
            table_id: table_id(1),
            snapshot_version: 1,
            schema_version: 1,
        },
        CatalogSnapshotSelector {
            table_id: table_id(1),
            snapshot_version: 2,
            schema_version: 2,
        },
    ];
    assert_eq!(
        catalog
            .read_snapshot_at(&duplicate)
            .await
            .expect_err("duplicate selectors must fail before enqueue")
            .code(),
        ErrorCode::InvalidArgument
    );

    assert_eq!(catalog.revision().await, Ok(0));
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reads_require_exact_versions_and_reject_versions_ahead_of_current() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    catalog
        .command(create_table(
            2,
            1,
            1,
            "ticks",
            Some(1),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect("create table");

    for (selector, reason) in [
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 0,
                schema_version: 1,
            },
            "zero snapshot version",
        ),
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 1,
                schema_version: 0,
            },
            "zero schema version",
        ),
    ] {
        let error = catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .expect_err(reason);
        assert_eq!(
            error.code(),
            ErrorCode::InvalidArgument,
            "unexpected error reason=[{reason}]"
        );
    }

    let raw = Connection::open(catalog_path(&data_directory)).expect("open raw catalog");
    raw.pragma_update(None, "foreign_keys", false)
        .expect("disable raw foreign keys for exact-gap fixture");
    let version_two = 2_u64.to_be_bytes();
    raw.execute(
        "UPDATE tables \
         SET current_schema_version = ?1, current_snapshot_version = ?1 \
         WHERE table_id = ?2",
        rusqlite::params![version_two.as_slice(), table_id(1).into_bytes().as_slice()],
    )
    .expect("seed missing nonzero exact versions");
    drop(raw);

    for (selector, expected_code, reason) in [
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 2,
                schema_version: 1,
            },
            ErrorCode::NotFound,
            "missing exact snapshot version",
        ),
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 1,
                schema_version: 2,
            },
            ErrorCode::NotFound,
            "missing exact schema version",
        ),
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 3,
                schema_version: 1,
            },
            ErrorCode::Conflict,
            "snapshot version ahead of current",
        ),
        (
            CatalogSnapshotSelector {
                table_id: table_id(1),
                snapshot_version: 1,
                schema_version: 3,
            },
            ErrorCode::Conflict,
            "schema version ahead of current",
        ),
    ] {
        let error = catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .expect_err(reason);
        assert_eq!(
            error.code(),
            expected_code,
            "unexpected error reason=[{reason}]"
        );
    }

    assert_eq!(catalog.revision().await, Ok(2));
    catalog.close().expect("close sqlite catalog");
}

fn insert_raw_second_snapshot(raw: &Connection, table: TableId) -> CatalogSnapshotSelector {
    let table_bytes = table.into_bytes();
    let version_one = 1_u64.to_be_bytes();
    let version_two = 2_u64.to_be_bytes();
    let parent_one = encode_optional_u64(Some(1));
    let empty_offset = [0_u8; 9];
    let (created_at_ns, summary_blob): (Vec<u8>, Vec<u8>) = raw
        .query_row(
            "SELECT created_at_ns, summary_blob FROM table_snapshots \
             WHERE table_id = ?1 AND snapshot_version = ?2",
            rusqlite::params![table_bytes.as_slice(), version_one.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read initial snapshot seed values");
    raw.execute(
        "INSERT INTO table_snapshots(\
             table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
             committed_log_offset, created_at_ns, summary_blob\
         ) VALUES (?1, ?2, ?3, 'persist', ?2, ?4, ?5, ?6)",
        rusqlite::params![
            table_bytes.as_slice(),
            version_two.as_slice(),
            parent_one.as_slice(),
            empty_offset.as_slice(),
            created_at_ns,
            summary_blob,
        ],
    )
    .expect("insert raw second snapshot");
    raw.execute(
        "UPDATE tables SET current_snapshot_version = ?1 WHERE table_id = ?2",
        rusqlite::params![version_two.as_slice(), table_bytes.as_slice()],
    )
    .expect("advance raw table snapshot version");
    CatalogSnapshotSelector {
        table_id: table,
        snapshot_version: 2,
        schema_version: 1,
    }
}

async fn catalog_with_raw_second_snapshot(
    directory: &TempDir,
) -> (
    SqliteCatalog,
    CatalogSnapshotMemoryLeaseIssuer,
    Connection,
    CatalogSnapshotSelector,
) {
    let data_directory = data_directory(directory);
    let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
        SqliteCatalogConfig::new(&data_directory),
    )
    .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    catalog
        .command(create_table(
            2,
            1,
            1,
            "ticks",
            Some(1),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect("create table");

    let raw = Connection::open(catalog_path(&data_directory)).expect("open raw catalog");
    let selector = insert_raw_second_snapshot(&raw, table_id(1));
    (catalog, memory_lease_issuer, raw, selector)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_and_pinned_native_snapshots_share_file_id_order() {
    let directory = TempDir::new().expect("create temp directory");
    let (catalog, memory_lease_issuer, raw, selector) =
        catalog_with_raw_second_snapshot(&directory).await;
    let table = selector.table_id;
    insert_native_data_file(&raw, table, file_id(2));
    insert_native_data_file(&raw, table, file_id(1));
    insert_native_data_file(&raw, table, file_id(4));
    insert_native_data_file(&raw, table, file_id(3));

    let snapshot = catalog
        .read_snapshot_at(std::slice::from_ref(&selector))
        .await
        .expect("read raw native manifest");
    let files = snapshot.tables()[0].files().as_slice();
    assert_eq!(
        files.iter().map(|file| file.file_id()).collect::<Vec<_>>(),
        [file_id(1), file_id(2), file_id(3), file_id(4)]
    );

    let pinned = catalog
        .read_handle()
        .expect("create Catalog read handle")
        .read_pinned_snapshot_at(
            std::slice::from_ref(&selector),
            snapshot_memory_lease(&memory_lease_issuer, 1),
        )
        .await
        .expect("read pinned native manifest");
    let manifest = pinned.table(0).expect("pinned table manifest");
    assert_eq!(
        manifest
            .files()
            .map(|file| file.file_id())
            .collect::<Vec<_>>(),
        [file_id(1), file_id(2), file_id(3), file_id(4)]
    );
    assert_eq!(
        manifest.file(0).expect("first pinned file").file_id(),
        file_id(1)
    );
    assert_eq!(
        manifest.file(3).expect("last pinned file").file_id(),
        file_id(4)
    );
    drop((manifest, pinned));

    let mut corrupt_metadata = empty_metadata_blob("data_file_metadata");
    *corrupt_metadata
        .last_mut()
        .expect("metadata blob must contain a payload") ^= 1;
    raw.execute(
        "UPDATE data_files SET metadata_blob = ?1 WHERE file_id = ?2",
        rusqlite::params![corrupt_metadata, file_id(1).into_bytes().as_slice(),],
    )
    .expect("tamper raw file metadata blob");
    assert_eq!(
        catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .expect_err("metadata checksum corruption must fail the whole snapshot")
            .code(),
        ErrorCode::DataCorruption
    );

    drop(raw);
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_conflicts_and_injected_failure_leave_no_partial_table() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&data_directory).with_busy_timeout(Duration::from_millis(25)),
    )
    .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");

    let oversized_name = "x".repeat(MAX_SQLITE_CATALOG_NAME_BYTES + 1);
    assert_eq!(
        catalog
            .command(create_namespace(100, 100, &oversized_name, Some(1)))
            .await
            .expect_err("new oversized namespace must fail admission")
            .code(),
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        catalog
            .command(create_table(
                101,
                101,
                1,
                &oversized_name,
                Some(1),
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("new oversized table must fail admission")
            .code(),
        ErrorCode::ResourceExhausted
    );
    assert_eq!(catalog.revision().await, Ok(1));

    let raw = Connection::open(catalog_path(&data_directory)).expect("open raw catalog");
    raw.execute_batch("BEGIN IMMEDIATE")
        .expect("hold writer lock");
    let mut malformed = create_table(
        2,
        1,
        1,
        "bad_ipc",
        Some(1),
        append_definition(),
        schema_ipc(&table_schema()),
    );
    let VersionedCatalogCommand::V1(envelope) = &mut malformed;
    let CatalogCommandV1::CreateTable(command) = &mut envelope.command else {
        panic!("expected create table");
    };
    command.schema_ipc = SchemaIpcPayload::new(vec![1]).expect("nonempty malformed payload");
    assert_eq!(
        catalog
            .command(malformed)
            .await
            .expect_err("malformed IPC must fail before writer lock")
            .code(),
        ErrorCode::InvalidArgument
    );
    raw.execute_batch("ROLLBACK").expect("release writer lock");

    let missing_frame_column = TableDefinition::Frame(FrameTableDefinition {
        by: ColumnName::new("missing").expect("valid column name"),
    });
    assert_eq!(
        catalog
            .command(create_table(
                3,
                1,
                1,
                "bad_frame",
                Some(1),
                missing_frame_column,
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("missing frame column must fail")
            .code(),
        ErrorCode::SchemaMismatch
    );

    let keys =
        ColumnSet::new(vec![ColumnName::new("price").expect("valid key")]).expect("unique key set");
    let non_utf8_key = TableDefinition::KeyValue(
        KeyValueTableDefinition::new(keys).expect("nonempty key definition"),
    );
    assert_eq!(
        catalog
            .command(create_table(
                4,
                1,
                1,
                "bad_key",
                Some(1),
                non_utf8_key,
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("non-UTF8 key must fail")
            .code(),
        ErrorCode::SchemaMismatch
    );

    let duplicate_schema = Schema::new(vec![
        Field::new("dup", DataType::Utf8, false),
        Field::new("dup", DataType::Utf8, false),
    ]);
    assert_eq!(
        catalog
            .command(create_table(
                5,
                1,
                1,
                "bad_duplicate",
                Some(1),
                TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc(&duplicate_schema),
            ))
            .await
            .expect_err("duplicate schema fields must fail")
            .code(),
        ErrorCode::SchemaMismatch
    );
    let oversized_config = (0..4_097)
        .map(|index| (format!("key-{index}"), String::new()))
        .collect();
    assert_eq!(
        catalog
            .command(create_table_command(
                11,
                Some(1),
                CreateTableCommand {
                    table_id: table_id(11),
                    namespace_id: namespace_id(1),
                    name: CatalogName::new("oversized_config").expect("valid table name"),
                    definition: append_definition(),
                    schema_ipc: schema_ipc(&table_schema()),
                    config: oversized_config,
                },
            ))
            .await
            .expect_err("oversized table config must fail")
            .code(),
        ErrorCode::ResourceExhausted
    );
    assert_eq!(catalog.revision().await, Ok(1));

    catalog
        .command(create_table(
            6,
            1,
            1,
            "ticks",
            Some(1),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect("create valid table");
    assert_eq!(
        catalog
            .command(create_table(
                7,
                1,
                1,
                "different_name",
                None,
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("duplicate table id must fail")
            .code(),
        ErrorCode::AlreadyExists
    );
    assert_eq!(
        catalog
            .command(create_table(
                8,
                2,
                1,
                "ticks",
                None,
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("duplicate table name must fail")
            .code(),
        ErrorCode::AlreadyExists
    );
    assert_eq!(
        catalog
            .command(create_table(
                9,
                2,
                9,
                "missing_namespace",
                None,
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect_err("missing namespace must fail")
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(catalog.revision().await, Ok(2));

    raw.execute_batch(
        "CREATE TRIGGER inject_snapshot_failure
         BEFORE INSERT ON table_snapshots
         BEGIN
             SELECT RAISE(ABORT, 'injected snapshot failure');
         END;",
    )
    .expect("install failure trigger");
    let error = catalog
        .command(create_table(
            10,
            2,
            1,
            "rolled_back",
            Some(2),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect_err("injected snapshot failure must abort transaction");
    assert_eq!(error.code(), ErrorCode::Internal);
    raw.execute_batch("DROP TRIGGER inject_snapshot_failure")
        .expect("remove failure trigger");
    assert_eq!(catalog.revision().await, Ok(2));
    assert_eq!(
        catalog
            .get_table(TableRef::from(table_id(2)))
            .await
            .expect_err("rolled-back table must be absent")
            .code(),
        ErrorCode::NotFound
    );
    for table in ["tables", "table_schemas", "table_snapshots"] {
        let count: i64 = raw
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count rows after rollback");
        assert_eq!(count, 1, "partial domain row survived table=[{table}]");
    }
    drop(raw);
    catalog.close().expect("close catalog");
    SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen catalog after removed trigger")
        .close()
        .expect("close reopened catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_listing_and_resolution_are_bounded_stable_and_revision_consistent() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "one", Some(0)))
        .await
        .expect("create first namespace");
    catalog
        .command(create_namespace(2, 2, "two", None))
        .await
        .expect("create second namespace");
    for (request, table, namespace, name) in
        [(3, 3, 1, "three"), (4, 1, 1, "one"), (5, 2, 2, "two")]
    {
        catalog
            .command(create_table(
                request,
                table,
                namespace,
                name,
                None,
                append_definition(),
                schema_ipc(&table_schema()),
            ))
            .await
            .expect("create listed table");
    }

    let first = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(2).expect("nonzero page size"),
        })
        .await
        .expect("list first table page");
    assert_eq!(first.catalog_revision, 5);
    assert_eq!(
        first
            .tables
            .iter()
            .map(|table| table.table_id)
            .collect::<Vec<_>>(),
        [table_id(1), table_id(2)]
    );
    assert_eq!(first.next_after, Some(table_id(2)));
    let second = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: first.next_after,
            limit: NonZeroU32::new(2).expect("nonzero page size"),
        })
        .await
        .expect("list second table page");
    assert_eq!(second.catalog_revision, 5);
    assert_eq!(second.tables.len(), 1);
    assert_eq!(second.tables[0].table_id, table_id(3));
    assert_eq!(second.next_after, None);

    let namespace_page = catalog
        .list_tables(ListTablesRequest {
            namespace_id: Some(namespace_id(1)),
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list namespace tables");
    assert_eq!(
        namespace_page
            .tables
            .iter()
            .map(|table| table.table_id)
            .collect::<Vec<_>>(),
        [table_id(1), table_id(3)]
    );
    let unknown_namespace = catalog
        .list_tables(ListTablesRequest {
            namespace_id: Some(namespace_id(9)),
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("unknown namespace listing is empty");
    assert!(unknown_namespace.tables.is_empty());
    assert_eq!(unknown_namespace.catalog_revision, 5);

    let references = [
        TableRef::from(table_id(3)),
        TableRef::Name {
            namespace: CatalogName::new("one").expect("valid namespace"),
            table: CatalogName::new("one").expect("valid table"),
        },
        TableRef::from(table_id(3)),
    ];
    let resolved = catalog
        .resolve_tables(&references)
        .await
        .expect("resolve table references");
    assert_eq!(
        resolved
            .iter()
            .map(|table| table.table_id)
            .collect::<Vec<_>>(),
        [table_id(3), table_id(1), table_id(3)]
    );
    assert_eq!(
        catalog
            .resolve_tables(&[TableRef::from(table_id(9))])
            .await
            .expect_err("resolution must fail atomically when one table is missing")
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        catalog
            .list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(4_097).expect("nonzero oversized page"),
            })
            .await
            .expect_err("oversized table page must fail")
            .code(),
        ErrorCode::InvalidArgument
    );
    let oversized_resolution = vec![TableRef::from(table_id(1)); 4_097];
    assert_eq!(
        catalog
            .resolve_tables(&oversized_resolution)
            .await
            .expect_err("oversized table resolution count must fail before enqueue")
            .code(),
        ErrorCode::ResourceExhausted
    );
    catalog.close().expect("close catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_read_byte_budget_pages_lists_and_rejects_oversized_resolution() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    let config = BTreeMap::from([("payload".to_owned(), "x".repeat(900 * 1024))]);
    for index in 1..=5_u128 {
        catalog
            .command(create_table_command(
                index + 1,
                None,
                CreateTableCommand {
                    table_id: table_id(index),
                    namespace_id: namespace_id(1),
                    name: CatalogName::new(format!("table-{index}")).expect("valid table name"),
                    definition: append_definition(),
                    schema_ipc: schema_ipc(&table_schema()),
                    config: config.clone(),
                },
            ))
            .await
            .expect("create table with large bounded config");
    }

    let first = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list byte-bounded first page");
    assert_eq!(first.tables.len(), 4);
    assert_eq!(first.next_after, Some(table_id(4)));
    let second = catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: first.next_after,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list byte-bounded second page");
    assert_eq!(second.tables.len(), 1);
    assert_eq!(second.tables[0].table_id, table_id(5));
    assert_eq!(second.next_after, None);

    let references = (1..=5_u128)
        .map(|index| TableRef::from(table_id(index)))
        .collect::<Vec<_>>();
    assert_eq!(
        catalog
            .resolve_tables(&references)
            .await
            .expect_err("oversized resolution response must fail")
            .code(),
        ErrorCode::ResourceExhausted
    );
    catalog.close().expect("close catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_snapshot_lease_acquire_release_and_replay_are_atomic() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    catalog
        .command(create_table(
            2,
            1,
            1,
            "ticks",
            Some(1),
            append_definition(),
            schema_ipc(&table_schema()),
        ))
        .await
        .expect("create table");

    let acquire = snapshot_lease_command(
        3,
        CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
            lease_id: lease_id(1),
            table_id: table_id(1),
            snapshot_version: 1,
            owner: "backup-job-1".to_string(),
            expires_at_ns: u64::MAX,
        }),
    );
    let acquired = catalog
        .command(acquire.clone())
        .await
        .expect("acquire durable snapshot lease");
    let VersionedCatalogResponse::V1(acquired_response) = &acquired;
    assert_eq!(acquired_response.catalog_revision, 3);
    assert!(matches!(
        &acquired_response.outcome,
        CatalogOutcomeV1::SnapshotLease(lease)
            if lease.lease_id == lease_id(1)
                && lease.table_id == table_id(1)
                && lease.snapshot_version == 1
                && lease.owner == "backup-job-1"
    ));
    assert_eq!(
        catalog
            .command(acquire)
            .await
            .expect("replay lease acquire"),
        acquired
    );

    let conflicting_replay = snapshot_lease_command(
        3,
        CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
            lease_id: lease_id(1),
            table_id: table_id(1),
            snapshot_version: 1,
            owner: "backup-job-1".to_string(),
            expires_at_ns: u64::MAX - 1,
        }),
    );
    assert_eq!(
        catalog
            .command(conflicting_replay)
            .await
            .expect_err("same request id with another lease fingerprint must conflict")
            .code(),
        ErrorCode::Conflict
    );

    let wrong_owner_release = snapshot_lease_command(
        4,
        CatalogCommandV1::ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand {
            lease_id: lease_id(1),
            owner: "another-owner".to_string(),
        }),
    );
    assert_eq!(
        catalog
            .command(wrong_owner_release)
            .await
            .expect_err("another owner must not release the lease")
            .code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(catalog.revision().await.expect("read revision"), 3);

    let release = snapshot_lease_command(
        5,
        CatalogCommandV1::ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand {
            lease_id: lease_id(1),
            owner: "backup-job-1".to_string(),
        }),
    );
    let released = catalog
        .command(release.clone())
        .await
        .expect("release durable snapshot lease");
    let VersionedCatalogResponse::V1(released_response) = &released;
    assert_eq!(released_response.catalog_revision, 4);
    assert!(matches!(
        released_response.outcome,
        CatalogOutcomeV1::SnapshotLeaseReleased { lease_id: released }
            if released == lease_id(1)
    ));
    assert_eq!(
        catalog
            .command(release)
            .await
            .expect("replay lease release"),
        released
    );

    let expired = snapshot_lease_command(
        6,
        CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
            lease_id: lease_id(2),
            table_id: table_id(1),
            snapshot_version: 1,
            owner: "expired-backup".to_string(),
            expires_at_ns: 1,
        }),
    );
    assert_eq!(
        catalog
            .command(expired)
            .await
            .expect_err("already expired lease must be rejected")
            .code(),
        ErrorCode::InvalidArgument
    );
    let missing_snapshot = snapshot_lease_command(
        7,
        CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
            lease_id: lease_id(3),
            table_id: table_id(1),
            snapshot_version: 99,
            owner: "missing-snapshot".to_string(),
            expires_at_ns: u64::MAX,
        }),
    );
    assert_eq!(
        catalog
            .command(missing_snapshot)
            .await
            .expect_err("lease target snapshot must exist")
            .code(),
        ErrorCode::NotFound
    );

    let raw = Connection::open(catalog_path(&data_directory)).expect("open raw catalog");
    let remaining: i64 = raw
        .query_row("SELECT COUNT(*) FROM snapshot_leases", [], |row| row.get(0))
        .expect("count durable snapshot leases");
    assert_eq!(remaining, 0);
    drop(raw);
    catalog.close().expect("close catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_catalog_runtime_is_single_owner_and_reopen_preserves_command_replay() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let first_catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open first sqlite catalog");
    let second_error = match SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)) {
        Ok(catalog) => {
            catalog.close().expect("close unexpected second catalog");
            panic!("second typed Catalog actor unexpectedly acquired the same data root");
        }
        Err(error) => error,
    };
    assert_eq!(second_error.code(), ErrorCode::Conflict);
    assert!(second_error.message().contains("already active"));

    first_catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    let create = create_table(
        2,
        1,
        1,
        "one",
        Some(1),
        append_definition(),
        schema_ipc(&table_schema()),
    );
    let committed = first_catalog
        .command(create.clone())
        .await
        .expect("commit table through the sole Catalog actor");
    assert_eq!(first_catalog.revision().await, Ok(2));
    first_catalog.close().expect("close first catalog");

    let second_catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen typed Catalog after ownership release");
    let replay = second_catalog
        .command(create)
        .await
        .expect("replay committed command after Catalog restart");
    assert_eq!(
        CatalogCodec::encode_response(&replay).expect("encode replay response"),
        CatalogCodec::encode_response(&committed).expect("encode committed response")
    );
    assert_eq!(second_catalog.revision().await, Ok(2));
    let page = second_catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list committed table");
    assert_eq!(page.tables.len(), 1);
    second_catalog.close().expect("close second catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_an_unpolled_command_future_never_enqueues_or_commits() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");

    let unpolled = catalog.command(create_namespace(1, 1, "market", Some(0)));
    drop(unpolled);

    assert_eq!(catalog.revision().await, Ok(0));
    let error = catalog
        .get_namespace(NamespaceRef::Id(namespace_id(1)))
        .await
        .expect_err("unpolled command must not create a namespace");
    assert_eq!(error.code(), ErrorCode::NotFound);
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_polled_command_future_does_not_cancel_its_commit() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");
    let blocker = Connection::open(catalog_path(&data_directory)).expect("open lock holder");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold sqlite writer lock");

    let mut commit_unknown = catalog.command(create_namespace(1, 1, "market", Some(0)));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        commit_unknown.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(commit_unknown);
    blocker
        .execute_batch("ROLLBACK")
        .expect("release sqlite writer lock");

    catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("replay commit-unknown command before requiring read-after-write");
    assert_eq!(catalog.revision().await, Ok(1));
    assert!(catalog
        .get_namespace(NamespaceRef::Id(namespace_id(1)))
        .await
        .is_ok());
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_validation_precedes_enqueue_and_reads_do_not_share_writer_queue() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog =
        SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory).with_command_capacity(1))
            .expect("open sqlite catalog");
    let blocker = Connection::open(catalog_path(&data_directory)).expect("open lock holder");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold sqlite writer lock");

    let mut blocked_write = catalog.command(create_namespace(1, 1, "market", Some(0)));
    {
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            blocked_write.as_mut().poll(&mut context),
            Poll::Pending
        ));
    }
    let selectors = [CatalogSnapshotSelector {
        table_id: table_id(1),
        snapshot_version: 1,
        schema_version: 1,
    }];
    let independent_read = catalog
        .read_snapshot_at(&selectors)
        .await
        .expect_err("missing snapshot read must execute independently of the blocked writer");
    assert_eq!(independent_read.code(), ErrorCode::NotFound);
    let unsorted = [
        CatalogSnapshotSelector {
            table_id: table_id(2),
            snapshot_version: 1,
            schema_version: 1,
        },
        CatalogSnapshotSelector {
            table_id: table_id(1),
            snapshot_version: 1,
            schema_version: 1,
        },
    ];
    assert_eq!(
        catalog
            .read_snapshot_at(&unsorted)
            .await
            .expect_err("selector order must be validated before enqueue")
            .code(),
        ErrorCode::InvalidArgument
    );
    let oversized = (1..=4_097_u128)
        .map(|table| CatalogSnapshotSelector {
            table_id: table_id(table),
            snapshot_version: 1,
            schema_version: 1,
        })
        .collect::<Vec<_>>();
    let oversized_error = catalog
        .read_snapshot_at(&oversized)
        .await
        .expect_err("selector count must be validated before enqueue");
    assert_eq!(oversized_error.code(), ErrorCode::ResourceExhausted);
    assert!(oversized_error
        .message()
        .starts_with("catalog snapshot selector count exceeds maximum"));
    blocker
        .execute_batch("ROLLBACK")
        .expect("release sqlite writer lock");
    blocked_write
        .await
        .expect("write ahead of cancelled read must commit");
    assert_eq!(catalog.revision().await, Ok(1));
    catalog.close().expect("close sqlite catalog");
}

#[test]
fn writable_config_retains_data_directory_ownership() {
    let directory = TempDir::new().expect("create temp directory");
    let ownership = data_directory(&directory);
    let read_view = ownership.directory().clone();
    let config = SqliteCatalogConfig::new(&ownership);
    drop(ownership);

    assert!(matches!(
        read_view.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));

    drop(config);
    read_view
        .acquire_process_ownership()
        .expect("dropping the final writable config must release ownership");
}

#[test]
fn runtime_snapshot_pin_status_reports_limits_watermarks_and_rejections() {
    let directory = TempDir::new().expect("create temp directory");
    let ownership = data_directory(&directory);
    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&ownership).with_runtime_snapshot_pin_limits(2, 1),
    )
    .expect("open sqlite catalog");

    let initial = catalog.runtime_snapshot_pin_status();
    assert!(initial.admission_open);
    assert_eq!(initial.active_pins, 0);
    assert_eq!(initial.active_tables, 0);
    assert_eq!(initial.max_active_pins, 2);
    assert_eq!(initial.max_active_tables, 1);
    assert_eq!(initial.pin_budget_exhaustions_total, 0);
    assert_eq!(initial.table_budget_exhaustions_total, 0);

    let first = catalog
        .pin_runtime_snapshot(table_id(1), 1)
        .expect("pin first snapshot");
    let second = catalog
        .pin_runtime_snapshot(table_id(1), 2)
        .expect("pin second snapshot");
    assert_eq!(
        catalog
            .pin_runtime_snapshot(table_id(1), 3)
            .expect_err("pin budget must reject excess admission")
            .code(),
        ErrorCode::ResourceExhausted
    );
    assert_eq!(
        catalog
            .pin_runtime_snapshot(table_id(2), 1)
            .expect_err("table budget must reject excess admission")
            .code(),
        ErrorCode::ResourceExhausted
    );

    let exhausted = catalog.runtime_snapshot_pin_status();
    assert_eq!(exhausted.active_pins, 2);
    assert_eq!(exhausted.active_tables, 1);
    assert_eq!(exhausted.pin_budget_exhaustions_total, 1);
    assert_eq!(exhausted.table_budget_exhaustions_total, 1);
    let encoded = serde_json::to_value(exhausted).expect("serialize runtime pin status");
    assert_eq!(encoded["max_active_pins"], 2);
    assert_eq!(encoded["table_budget_exhaustions_total"], 1);
    let decoded: zippy_catalog::SqliteCatalogRuntimeSnapshotPinStatus =
        serde_json::from_value(encoded).expect("deserialize runtime pin status");
    assert_eq!(decoded, exhausted);

    drop((first, second));
    let released = catalog.runtime_snapshot_pin_status();
    assert_eq!(released.active_pins, 0);
    assert_eq!(released.active_tables, 0);
    assert_eq!(released.pin_budget_exhaustions_total, 1);
    assert_eq!(released.table_budget_exhaustions_total, 1);
    catalog.close().expect("close sqlite catalog");
}

#[test]
fn read_handle_reports_closed_pin_registry_until_the_last_guard_drops() {
    let directory = TempDir::new().expect("create temp directory");
    let ownership = data_directory(&directory);
    let catalog = SqliteCatalog::open(
        SqliteCatalogConfig::new(&ownership).with_runtime_snapshot_pin_limits(2, 2),
    )
    .expect("open sqlite catalog");
    let read_handle = catalog.read_handle().expect("create read handle");
    assert!(
        read_handle
            .runtime_snapshot_pin_status()
            .expect("read open registry status")
            .admission_open
    );
    let retained = catalog
        .pin_runtime_snapshot(table_id(1), 1)
        .expect("retain one snapshot pin");

    catalog.close().expect("close sqlite catalog");
    let closed = read_handle
        .runtime_snapshot_pin_status()
        .expect("retained guard keeps closed registry observable");
    assert!(!closed.admission_open);
    assert_eq!(closed.active_pins, 1);
    assert_eq!(closed.active_tables, 1);

    drop(retained);
    let unavailable = read_handle
        .runtime_snapshot_pin_status()
        .expect_err("dropped registry must be terminally unavailable");
    assert_eq!(unavailable.code(), ErrorCode::Unavailable);
    assert!(unavailable
        .message()
        .contains("runtime snapshot pin registry is no longer available"));
}

#[test]
fn catalog_workers_hold_ownership_until_they_are_closed() {
    let directory = TempDir::new().expect("create temp directory");
    let ownership = data_directory(&directory);
    let read_view = ownership.directory().clone();
    let catalog =
        SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).expect("open sqlite catalog");
    drop(ownership);

    assert!(matches!(
        read_view.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));

    catalog.close().expect("close sqlite catalog workers");
    read_view
        .acquire_process_ownership()
        .expect("worker shutdown must release its final ownership capability");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_snapshot_forks_keep_gc_pins_and_independent_query_accounting() {
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zippy_types::query::QueryInstanceId;
    use zippy_types::QueryId;

    struct LeaseOwner(Arc<AtomicUsize>);
    impl Drop for LeaseOwner {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let temp = TempDir::new().unwrap();
    let (catalog, issuer, raw, selector) = catalog_with_raw_second_snapshot(&temp).await;
    insert_native_data_file(&raw, selector.table_id, file_id(2));
    let live_leases = Arc::new(AtomicUsize::new(0));
    let issue = |issuer: &CatalogSnapshotMemoryLeaseIssuer, query, bytes| {
        issuer
            .issue_with_owner(bytes, query, |_| {
                live_leases.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(LeaseOwner(Arc::clone(&live_leases))))
            })
            .unwrap()
    };
    let bytes = SqliteCatalogSnapshotReadLimits::default()
        .conservative_owned_peak_bytes(1)
        .unwrap();
    let instance =
        |id| QueryInstanceId::new(QueryId::from_u128(id).unwrap(), NonZeroU64::new(1).unwrap());
    let pinned = catalog
        .read_handle()
        .unwrap()
        .read_pinned_snapshot_at(std::slice::from_ref(&selector), issue(&issuer, None, bytes))
        .await
        .unwrap();
    assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 1);
    let (foreign, _) = CatalogSnapshotMemoryLeaseIssuer::new_authority().unwrap();
    for (indices, lease) in [
        (vec![0], issue(&foreign, Some(instance(11)), bytes)),
        (vec![0], issue(&issuer, None, bytes)),
        (
            vec![0],
            issue(&issuer, Some(instance(11)), NonZeroU64::new(1).unwrap()),
        ),
        (vec![], issue(&issuer, Some(instance(11)), bytes)),
        (vec![0, 0], issue(&issuer, Some(instance(11)), bytes)),
        (vec![1], issue(&issuer, Some(instance(11)), bytes)),
    ] {
        assert_eq!(
            pinned.fork_for_query(&indices, lease).err().unwrap().code(),
            ErrorCode::InvalidArgument
        );
    }
    assert_eq!(live_leases.load(Ordering::SeqCst), 1);
    let fork = pinned
        .fork_for_query(&[0], issue(&issuer, Some(instance(11)), bytes))
        .unwrap();
    assert_eq!(
        fork.memory_lease_descriptor().query_instance_id(),
        Some(instance(11))
    );
    assert_eq!(fork.descriptor(), pinned.descriptor());
    let file = fork.table(0).unwrap().file(0).unwrap();
    drop((pinned, fork));
    assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 1);
    assert_eq!(live_leases.load(Ordering::SeqCst), 2);
    assert_eq!(file.file_id(), file_id(2));
    drop(file);
    assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 0);
    assert_eq!(live_leases.load(Ordering::SeqCst), 0);
    drop(raw);
    catalog.close().unwrap();
}
