//! Durable replay boundaries use Catalog CAS, schema identity and persisted native watermarks.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use rusqlite::Connection;
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{ManagedDataDirectory, OwnedDataDirectory};
use zippy_types::catalog::{
    AlterTableSchemaCommand, CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1,
    CatalogCommandV1, CatalogConsumerRead, CatalogOutcomeV1, CatalogRead, CatalogWrite,
    CommitConsumerOffsetCommand, CommitDataFilesCommand, CreateNamespaceCommand,
    CreateTableCommand, DropTableCommand, ReadConsumerOffsetRequest, VersionedCatalogCommand,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, DataFileLocation,
    DataFileProvenance, FrameTableDefinition, KeyValueTableDefinition, ManagedDataPath,
    MetadataScalar, NativeDataFileProvenance, PendingDataFile, SchemaIpcPayload,
    SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableDefinition, TableKind,
};
use zippy_types::{
    CommittedOffset, ConsumerGroupId, ConsumerOffsetAuthority, DurableConsumerId, ErrorCode,
    FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
};

fn table(value: u128) -> TableId {
    TableId::from_u128(value).unwrap()
}
fn consumer(value: u128) -> DurableConsumerId {
    DurableConsumerId::from_u128(value).unwrap()
}
fn position(value: u64) -> CommittedOffset {
    CommittedOffset::Known(LogOffset::new(value))
}
fn individual(value: u128) -> ConsumerOffsetAuthority {
    ConsumerOffsetAuthority::Consumer(consumer(value))
}
fn request(
    table_id: u128,
    consumer_id: u128,
    authority: ConsumerOffsetAuthority,
) -> ReadConsumerOffsetRequest {
    ReadConsumerOffsetRequest {
        table_id: table(table_id),
        consumer_id: consumer(consumer_id),
        offset_authority: authority,
    }
}
fn command(id: u128, command: CatalogCommandV1) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        RequestId::from_u128(id).unwrap(),
        None,
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "consumer-contract-test".to_owned(),
        },
        command,
    )
    .unwrap()
    .into()
}
fn ack(
    id: u128,
    request: ReadConsumerOffsetRequest,
    old: CommittedOffset,
    new: CommittedOffset,
) -> VersionedCatalogCommand {
    command(
        id,
        CatalogCommandV1::CommitConsumerOffset(CommitConsumerOffsetCommand {
            consumer_id: request.consumer_id,
            table_id: request.table_id,
            offset_authority: request.offset_authority,
            expected_schema_version: 1,
            expected_committed_log_offset: old,
            committed_log_offset: new,
        }),
    )
}
fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
    writer.finish().unwrap();
    drop(writer);
    SchemaIpcPayload::new(bytes).unwrap()
}
fn schema() -> Schema {
    Schema::new(vec![Field::new("key", DataType::Utf8, false)])
}
fn root(directory: &TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(OwnedDataDirectory::acquire_or_initialize(directory.path().join("data")).unwrap())
}
fn raw(root: &Arc<OwnedDataDirectory>) -> Connection {
    Connection::open(
        root.directory()
            .path(ManagedDataDirectory::Catalog)
            .join("catalog.db"),
    )
    .unwrap()
}
async fn setup(catalog: &SqliteCatalog) {
    catalog
        .command(command(
            1,
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: NamespaceId::from_u128(1).unwrap(),
                name: CatalogName::new("market").unwrap(),
            }),
        ))
        .await
        .unwrap();
    for (table_id, kind) in [
        (10, TableKind::Append),
        (11, TableKind::Frame),
        (12, TableKind::KeyValue),
    ] {
        let definition = match kind {
            TableKind::Append => TableDefinition::Append(AppendTableDefinition::default()),
            TableKind::Frame => TableDefinition::Frame(FrameTableDefinition {
                by: ColumnName::new("key").unwrap(),
            }),
            TableKind::KeyValue => TableDefinition::KeyValue(
                KeyValueTableDefinition::new(
                    ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
                )
                .unwrap(),
            ),
        };
        let physical = if kind == TableKind::KeyValue {
            zippy_types::key_value::log_schema(&schema(), &["key".to_owned()]).unwrap()
        } else {
            Arc::new(schema())
        };
        catalog
            .command(command(
                table_id,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table(table_id),
                    namespace_id: NamespaceId::from_u128(1).unwrap(),
                    name: CatalogName::new(format!("table_{table_id}")).unwrap(),
                    definition,
                    schema_ipc: schema_ipc(&physical),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .read_consumer_offset(request(table_id, table_id, individual(table_id)))
                .await
                .unwrap()
                .committed_log_offset,
            CommittedOffset::Empty
        );
        assert_eq!(
            catalog
                .command(ack(
                    100 + table_id,
                    request(table_id, table_id, individual(table_id)),
                    CommittedOffset::Empty,
                    position(0)
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        let offsets = OffsetRange::new(LogOffset::new(0), LogOffset::new(9)).unwrap();
        let mut metadata = BTreeMap::new();
        if kind == TableKind::KeyValue {
            metadata.insert(
                zippy_types::key_value::STATE_VERSION_METADATA.to_owned(),
                MetadataScalar::UInt64(1),
            );
        }
        let file = PendingDataFile {
            file_id: FileId::from_u128(table_id).unwrap(),
            table_id: table(table_id),
            schema_version: 1,
            location: DataFileLocation::Managed(
                ManagedDataPath::new(format!("tables/{table_id}/part.parquet")).unwrap(),
            ),
            file_size_bytes: 1280,
            row_count: NonZeroU64::new(10).unwrap(),
            provenance: DataFileProvenance::Native(
                NativeDataFileProvenance::new(
                    offsets,
                    SourceSegmentLineages::new(vec![SourceSegmentLineage {
                        source: SourceSegmentIdentity {
                            segment_id: 1,
                            generation: 1,
                            writer_epoch: 0,
                        },
                        offsets,
                    }])
                    .unwrap(),
                )
                .unwrap(),
            ),
            partition: BTreeMap::new(),
            statistics: BTreeMap::new(),
            metadata,
            checksum: format!("sha256-v1:{table_id:064x}"),
            created_at_ns: 1,
        };
        catalog
            .command(command(
                20 + table_id,
                CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                    table_id: table(table_id),
                    expected_snapshot_version: 1,
                    files: vec![file],
                    committed_log_offset: OffsetWatermark::Known(LogOffset::new(9)),
                }),
            ))
            .await
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn individual_progress_replays_across_restart_and_does_not_rebind_after_drop() {
    let directory = TempDir::new().unwrap();
    let root = root(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&root)).unwrap();
    setup(&catalog).await;
    assert_eq!(
        raw(&root)
            .query_row("SELECT COUNT(*) FROM durable_consumers", [], |row| row
                .get::<_, u64>(0))
            .unwrap(),
        0,
    );
    let mut originals = Vec::new();
    for table_id in [10, 11, 12] {
        let read = request(table_id, table_id, individual(table_id));
        catalog
            .command(ack(
                50 + table_id,
                read,
                CommittedOffset::Empty,
                CommittedOffset::Empty,
            ))
            .await
            .unwrap();
        assert_eq!(
            catalog
                .read_consumer_offset(read)
                .await
                .unwrap()
                .committed_log_offset,
            CommittedOffset::Empty
        );
        let first = ack(100 + table_id, read, CommittedOffset::Empty, position(0));
        let response = catalog.command(first.clone()).await.unwrap();
        let zippy_types::catalog::VersionedCatalogResponse::V1(detail) = &response;
        assert!(matches!(detail.outcome,
                CatalogOutcomeV1::ConsumerOffset {
                    committed_log_offset: CommittedOffset::Known(offset), ..
                } if offset.get() == 0));
        catalog
            .command(ack(200 + table_id, read, position(0), position(9)))
            .await
            .unwrap();
        let current = catalog
            .read_handle()
            .unwrap()
            .read_consumer_offset(read)
            .await
            .unwrap();
        assert_eq!(current.committed_log_offset, position(9));
        assert_eq!(current.persisted_log_offset, position(9));
        let revision = catalog.revision().await.unwrap();
        assert_eq!(catalog.command(first.clone()).await.unwrap(), response);
        assert_eq!(catalog.revision().await.unwrap(), revision);
        originals.push((first, response));
    }
    catalog.close().unwrap();
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&root)).unwrap();
    for (first, response) in &originals {
        assert_eq!(&catalog.command(first.clone()).await.unwrap(), response);
    }
    assert_eq!(
        catalog
            .read_consumer_offset(request(11, 10, individual(10)))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    catalog
        .command(command(
            500,
            CatalogCommandV1::DropTable(DropTableCommand {
                table_id: table(10),
                expected_snapshot_version: 2,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        catalog.command(originals[0].0.clone()).await.unwrap(),
        originals[0].1
    );
    assert_eq!(
        catalog
            .read_consumer_offset(request(10, 10, individual(10)))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        catalog
            .command(ack(
                501,
                request(10, 10, individual(10)),
                position(9),
                position(9)
            ))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    catalog.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_cas_has_one_winner_and_sql_failure_rolls_back_identity_offset_and_receipt() {
    let directory = TempDir::new().unwrap();
    let root = root(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&root)).unwrap();
    setup(&catalog).await;
    let group = ConsumerOffsetAuthority::Group(ConsumerGroupId::from_u128(90).unwrap());
    let first = request(10, 80, group);
    let second = request(10, 81, group);
    let before = catalog.revision().await.unwrap();
    let (a, b) = tokio::join!(
        catalog.command(ack(400, first, CommittedOffset::Empty, position(4))),
        catalog.command(ack(401, second, CommittedOffset::Empty, position(4))),
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        a.err().or_else(|| b.err()).unwrap().code(),
        ErrorCode::Conflict
    );
    assert_eq!(catalog.revision().await.unwrap(), before + 1);
    assert_eq!(
        catalog
            .read_consumer_offset(first)
            .await
            .unwrap()
            .committed_log_offset,
        position(4)
    );
    assert_eq!(
        catalog
            .read_consumer_offset(second)
            .await
            .unwrap()
            .committed_log_offset,
        position(4)
    );
    catalog
        .command(ack(402, second, position(4), position(9)))
        .await
        .unwrap();
    let raw = raw(&root);
    raw.execute_batch(
        "CREATE TRIGGER reject_consumer_offset BEFORE INSERT ON durable_consumer_offsets \
         BEGIN SELECT RAISE(ABORT, 'injected offset failure'); END;",
    )
    .unwrap();
    let failed = ack(
        403,
        request(11, 999, individual(999)),
        CommittedOffset::Empty,
        position(0),
    );
    let before = catalog.revision().await.unwrap();
    assert!(catalog.command(failed.clone()).await.is_err());
    assert_eq!(catalog.revision().await.unwrap(), before);
    assert_eq!(
        raw.query_row(
            "SELECT COUNT(*) FROM durable_consumers WHERE consumer_id = ?1",
            [consumer(999).into_bytes().as_slice()],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    raw.execute_batch("DROP TRIGGER reject_consumer_offset")
        .unwrap();
    catalog.command(failed).await.unwrap();
    assert_eq!(
        catalog
            .read_consumer_offset(request(11, 999, individual(999)))
            .await
            .unwrap()
            .committed_log_offset,
        position(0)
    );
    catalog.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commits_reject_unauthorized_stale_regressing_and_undurable_acknowledgements() {
    let directory = TempDir::new().unwrap();
    let root = root(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&root)).unwrap();
    setup(&catalog).await;
    let read = request(10, 1000, individual(1000));
    for (schema_version, expected) in [
        (0, ErrorCode::InvalidArgument),
        (2, ErrorCode::SchemaMismatch),
    ] {
        let VersionedCatalogCommand::V1(envelope) =
            ack(600, read, CommittedOffset::Empty, position(0));
        let CatalogCommandV1::CommitConsumerOffset(mut commit) = envelope.command else {
            unreachable!()
        };
        commit.expected_schema_version = schema_version;
        assert_eq!(
            catalog
                .command(command(600, CatalogCommandV1::CommitConsumerOffset(commit)))
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    for (old, new, expected) in [
        (
            CommittedOffset::Empty,
            position(10),
            ErrorCode::InvalidArgument,
        ),
        (position(7), position(4), ErrorCode::InvalidArgument),
        (position(4), position(5), ErrorCode::Conflict),
    ] {
        assert_eq!(
            catalog
                .command(ack(600, read, old, new))
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    let VersionedCatalogCommand::V1(mut denied) =
        ack(600, read, CommittedOffset::Empty, position(4));
    denied.actor.kind = CatalogActorKind::User;
    let denied =
        CatalogCommandEnvelopeV1::new(denied.request_id, None, denied.actor, denied.command)
            .unwrap();
    assert_eq!(
        catalog.command(denied.into()).await.unwrap_err().code(),
        ErrorCode::PermissionDenied
    );
    assert_eq!(
        catalog
            .read_consumer_offset(request(10, 1000, individual(1001)))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::InvalidArgument
    );
    catalog
        .command(ack(600, read, CommittedOffset::Empty, position(4)))
        .await
        .unwrap();
    let new_schema = Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("note", DataType::Utf8, true),
    ]);
    catalog
        .command(command(
            601,
            CatalogCommandV1::AlterTableSchema(AlterTableSchemaCommand {
                table_id: table(10),
                expected_schema_version: 1,
                schema_ipc: schema_ipc(&new_schema),
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        catalog
            .command(ack(602, read, position(4), position(5)))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::SchemaMismatch
    );
    let current = catalog.read_consumer_offset(read).await.unwrap();
    assert_eq!(current.schema_version, 2);
    assert_eq!(current.committed_log_offset, position(4));
    let VersionedCatalogCommand::V1(envelope) = ack(602, read, position(4), position(5));
    let CatalogCommandV1::CommitConsumerOffset(mut commit) = envelope.command else {
        unreachable!()
    };
    commit.expected_schema_version = 2;
    catalog
        .command(command(602, CatalogCommandV1::CommitConsumerOffset(commit)))
        .await
        .unwrap();
    catalog.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_offsets_are_bounded_and_never_returned_as_empty_replay() {
    let directory = TempDir::new().unwrap();
    let root = root(&directory);
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&root)).unwrap();
    setup(&catalog).await;
    let read = request(10, 1000, individual(1000));
    catalog
        .command(ack(700, read, CommittedOffset::Empty, position(4)))
        .await
        .unwrap();
    let raw = raw(&root);
    raw.execute_batch("PRAGMA ignore_check_constraints = ON; PRAGMA foreign_keys = OFF;")
        .unwrap();
    for corrupt in [
        vec![2; 9],
        vec![0; 2 * 1024 * 1024],
        [vec![1], 10_u64.to_be_bytes().to_vec()].concat(),
    ] {
        raw.execute(
            "UPDATE durable_consumer_offsets SET committed_log_offset = ?1",
            [&corrupt],
        )
        .unwrap();
        assert_eq!(
            catalog.read_consumer_offset(read).await.unwrap_err().code(),
            ErrorCode::DataCorruption
        );
        assert_eq!(
            catalog
                .command(ack(701, read, position(4), position(5)))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::DataCorruption
        );
    }
    raw.execute(
        "UPDATE durable_consumer_offsets SET committed_log_offset = ?1",
        [[vec![1], 4_u64.to_be_bytes().to_vec()].concat()],
    )
    .unwrap();
    raw.execute_batch("DELETE FROM durable_consumers").unwrap();
    assert_eq!(
        catalog.read_consumer_offset(read).await.unwrap_err().code(),
        ErrorCode::DataCorruption
    );
    catalog.close().unwrap();
}
