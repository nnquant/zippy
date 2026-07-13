use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{DataDirectory, ManagedDataDirectory};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogOutcomeV1, CatalogRead, CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand,
    CreateTableCommand, ListNamespacesRequest, ListTablesRequest, NamespaceRef,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
    KeyValueTableDefinition, SchemaCompatibility, SchemaIpcPayload, SnapshotOperation,
    TableDefinition, TableRef, TableState,
};
use zippy_types::{ErrorCode, FileId, NamespaceId, RequestId, TableId};

type RawInitialSnapshotRow = (Vec<u8>, Vec<u8>, String, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

const MAX_SQLITE_CATALOG_NAME_BYTES: usize = 1024;

fn data_directory(directory: &TempDir) -> DataDirectory {
    DataDirectory::initialize(directory.path().join("data")).expect("initialize data root")
}

fn catalog_path(data_directory: &DataDirectory) -> std::path::PathBuf {
    data_directory
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

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("test file id must be nonzero")
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

fn insert_legacy_data_file(
    connection: &Connection,
    table: TableId,
    file: FileId,
    preserve_discovery_order: bool,
) {
    let table_id = table.into_bytes();
    let file_id = file.into_bytes();
    let schema_version = 1_u64.to_be_bytes();
    let file_size_bytes = 1_024_u64.to_be_bytes();
    let row_count = 10_u64.to_be_bytes();
    let absent = encode_optional_u64(None);
    let legacy_order = encode_optional_u64(preserve_discovery_order.then_some(0));
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
                 max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
                 partition_blob, statistics_blob, metadata_blob, checksum, \
                 added_snapshot_version, removed_snapshot_version, state, created_at_ns\
             ) VALUES (\
                 ?1, ?2, ?3, 'external_legacy', ?4, ?5, ?6, 'legacy', ?7, ?7, \
                 ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?7, 'committed', ?15\
             )",
            rusqlite::params![
                file_id.as_slice(),
                table_id.as_slice(),
                schema_version.as_slice(),
                format!("/tmp/zippy-catalog-file-{}.parquet", file),
                file_size_bytes.as_slice(),
                row_count.as_slice(),
                absent.as_slice(),
                i64::from(preserve_discovery_order),
                legacy_order.as_slice(),
                partition_blob,
                statistics_blob,
                metadata_blob,
                format!("sha256-v1:{:032x}", u128::from_be_bytes(file_id)),
                added_snapshot_version.as_slice(),
                created_at_ns.as_slice(),
            ],
        )
        .expect("insert raw legacy data file");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_manifest_sorts_modern_legacy_and_rejects_preserved_or_corrupt_metadata() {
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

    let raw = Connection::open(catalog_path(&data_directory)).expect("open raw catalog");
    let table = table_id(1);
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
    insert_legacy_data_file(&raw, table, file_id(2), false);
    insert_legacy_data_file(&raw, table, file_id(1), false);

    let selector = CatalogSnapshotSelector {
        table_id: table,
        snapshot_version: 2,
        schema_version: 1,
    };
    let snapshot = catalog
        .read_snapshot_at(std::slice::from_ref(&selector))
        .await
        .expect("read modern legacy manifest");
    let files = snapshot.tables()[0].files().as_slice();
    assert_eq!(
        files.iter().map(|file| file.file_id()).collect::<Vec<_>>(),
        [file_id(1), file_id(2)]
    );

    insert_legacy_data_file(&raw, table, file_id(3), true);
    assert_eq!(
        catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .expect_err("preserved legacy order must reject the whole V1 snapshot")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    raw.execute(
        "DELETE FROM data_files WHERE file_id = ?1",
        [file_id(3).into_bytes().as_slice()],
    )
    .expect("remove preserved-order test file");

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
    let malformed = create_table(
        2,
        1,
        1,
        "bad_ipc",
        Some(1),
        append_definition(),
        SchemaIpcPayload::new(vec![1]).expect("nonempty malformed payload"),
    );
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
async fn independent_catalog_actors_revision_cas_allow_exactly_one_commit_and_replay() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let first_catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open first sqlite catalog");
    let second_catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open second sqlite catalog");
    first_catalog
        .command(create_namespace(1, 1, "market", Some(0)))
        .await
        .expect("create namespace");
    let first = create_table(
        2,
        1,
        1,
        "one",
        Some(1),
        append_definition(),
        schema_ipc(&table_schema()),
    );
    let second = create_table(
        3,
        2,
        1,
        "two",
        Some(1),
        append_definition(),
        schema_ipc(&table_schema()),
    );
    let (first_result, second_result) = tokio::join!(
        first_catalog.command(first.clone()),
        second_catalog.command(second.clone())
    );
    let winning_response = first_result
        .as_ref()
        .ok()
        .or_else(|| second_result.as_ref().ok())
        .expect("one independent actor must commit")
        .clone();
    let results = [&first_result, &second_result];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let conflicts = results
        .iter()
        .filter_map(|result| result.as_ref().err())
        .collect::<Vec<_>>();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].code(), ErrorCode::Conflict);
    assert_eq!(first_catalog.revision().await, Ok(2));
    assert_eq!(second_catalog.revision().await, Ok(2));
    let replay = if first_result.is_ok() {
        second_catalog.command(first).await
    } else {
        first_catalog.command(second).await
    }
    .expect("replay winning command through the other actor");
    assert_eq!(
        CatalogCodec::encode_response(&replay).expect("encode replay response"),
        CatalogCodec::encode_response(&winning_response).expect("encode winning response")
    );
    let page = first_catalog
        .list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).expect("nonzero page size"),
        })
        .await
        .expect("list committed table");
    assert_eq!(page.tables.len(), 1);
    first_catalog.close().expect("close first catalog");
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

    assert_eq!(catalog.revision().await, Ok(1));
    assert!(catalog
        .get_namespace(NamespaceRef::Id(namespace_id(1)))
        .await
        .is_ok());
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_validation_precedes_enqueue_and_cancelled_read_is_skippable() {
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
    let cancelled_read = loop {
        let mut read = catalog.read_snapshot_at(&selectors);
        let poll = {
            let mut context = Context::from_waker(Waker::noop());
            read.as_mut().poll(&mut context)
        };
        match poll {
            Poll::Pending => break read,
            Poll::Ready(Err(error)) if error.code() == ErrorCode::ResourceExhausted => {
                tokio::task::yield_now().await;
            }
            result => panic!("unexpected queued read poll result result=[{result:?}]"),
        }
    };

    let queue_full = catalog
        .read_snapshot_at(&selectors)
        .await
        .expect_err("valid read must observe the saturated actor queue");
    assert_eq!(queue_full.code(), ErrorCode::ResourceExhausted);
    assert_eq!(
        queue_full.message(),
        "sqlite catalog actor command queue is full"
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
    drop(cancelled_read);

    blocker
        .execute_batch("ROLLBACK")
        .expect("release sqlite writer lock");
    blocked_write
        .await
        .expect("write ahead of cancelled read must commit");
    assert_eq!(catalog.revision().await, Ok(1));
    catalog.close().expect("close sqlite catalog");
}
