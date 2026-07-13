use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use rusqlite::Connection;
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{DataDirectory, ManagedDataDirectory};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogRead, CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, CreateTableCommand,
    ListNamespacesRequest, ListTablesRequest, NamespaceRef,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, SchemaIpcPayload, TableDefinition, TableRef,
};
use zippy_types::{ErrorCode, NamespaceId, RequestId, TableId};

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

fn create_namespace(
    request_id: RequestId,
    namespace_id: NamespaceId,
    name: &str,
) -> zippy_types::catalog::VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id,
        Some(0),
        actor(),
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id,
            name: CatalogName::new(name).expect("valid namespace name"),
        }),
    )
    .expect("create command envelope")
    .into()
}

fn create_table() -> zippy_types::catalog::VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(9),
        None,
        actor(),
        CatalogCommandV1::CreateTable(CreateTableCommand {
            table_id: TableId::from_u128(1).expect("test table id must be nonzero"),
            namespace_id: namespace_id(1),
            name: CatalogName::new("ticks").expect("valid table name"),
            definition: TableDefinition::Append(AppendTableDefinition::default()),
            schema_ipc: SchemaIpcPayload::new(vec![1]).expect("nonempty schema payload"),
            config: BTreeMap::new(),
        }),
    )
    .expect("create command envelope")
    .into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replays_create_namespace_exactly_across_restart_and_serves_reads() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let command = create_namespace(request_id(1), namespace_id(1), "market");
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
        .command(create_namespace(request_id(1), namespace_id(2), "changed"))
        .await
        .expect_err("request id reuse must conflict");
    assert_eq!(conflict.code(), ErrorCode::Conflict);
    assert_eq!(catalog.revision().await, Ok(1));
    catalog.close().expect("close reopened catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_empty_contracts_and_rejects_unimplemented_table_paths() {
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

    let table_id = TableId::from_u128(1).expect("test table id must be nonzero");
    let table_ref = TableRef::Id { table_id };
    assert_eq!(
        catalog
            .get_table(table_ref.clone())
            .await
            .expect_err("table get is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    assert_eq!(
        catalog
            .list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(10).expect("nonzero page size"),
            })
            .await
            .expect_err("table listing is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    assert_eq!(
        catalog
            .resolve_tables(&[table_ref])
            .await
            .expect_err("nonempty table resolution is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    assert_eq!(
        catalog
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id,
                snapshot_version: 0,
                schema_version: 0,
            }])
            .await
            .expect_err("nonempty snapshot read is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    assert_eq!(
        catalog
            .command(create_table())
            .await
            .expect_err("table creation is not implemented")
            .code(),
        ErrorCode::UnsupportedPlan
    );
    catalog.close().expect("close sqlite catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_an_unpolled_command_future_never_enqueues_or_commits() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
        .expect("open sqlite catalog");

    let unpolled = catalog.command(create_namespace(request_id(1), namespace_id(1), "market"));
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

    let mut commit_unknown =
        catalog.command(create_namespace(request_id(1), namespace_id(1), "market"));
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
