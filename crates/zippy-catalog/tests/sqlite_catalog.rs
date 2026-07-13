use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use rusqlite::Connection;
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
    KeyValueTableDefinition, SchemaIpcPayload, TableDefinition, TableRef, TableState,
};
use zippy_types::{ErrorCode, NamespaceId, RequestId, TableId};

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
            .expect_err("nonempty snapshot read is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
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
