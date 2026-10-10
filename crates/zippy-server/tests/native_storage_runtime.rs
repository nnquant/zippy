#[path = "../../zippy-storage/tests/support/fake_persistence_catalog.rs"]
mod fake_persistence_catalog;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::{DataDirectoryError, OwnedDataDirectory};
use zippy_ingest::{TableActorRegistryConfig, TablePersistenceStatus};
use zippy_server::{
    NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    NativeStorageStartOutcome,
};
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, ParquetCandidateSpec,
    ParquetCandidateWriter, PersistCandidateInput, PersistController, PersistControllerConfig,
    PersistRequest, StartupCatalogFileVerifier, StartupCatalogFileVerifierConfig,
    StartupOrphanReconciler,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
    CatalogWrite, CommitDataFilesCommand, CreateNamespaceCommand, CreateTableCommand,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance, SchemaIpcPayload,
    SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableDefinition,
};
use zippy_types::{
    BoxFuture, ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId,
    ServerError, TableId,
};

const NAMESPACE_ID: u128 = 1;
const TABLE_ID: u128 = 10;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_schema_evolution_preserves_history_wal_and_queries_for_all_kinds() {
    use arrow::array::{Array, StringArray};
    use futures::StreamExt;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::catalog::AlterTableSchemaCommand;
    use zippy_types::ingest::{
        DurableWriteTarget, IncomingWriteRequest, IngestService, KeyValueMutation,
        MutationFingerprintComputer, OpenWriterRequest, TableMutation,
    };
    use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
    use zippy_types::table::{
        ColumnName, ColumnSet, FrameTableDefinition, KeyValueTableDefinition, TableKind, TableRef,
    };
    use zippy_types::{ConnectionId, ProducerId, QueryId, ResourceLimit, SessionId};

    for kind in [TableKind::Append, TableKind::Frame, TableKind::KeyValue] {
        let temp = tempfile::tempdir().unwrap();
        let ownership =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
        let old = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let new = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("comment", DataType::Utf8, true),
        ]));
        let physical = if kind == TableKind::KeyValue {
            zippy_types::key_value::log_schema(&new, &["key".to_owned()]).unwrap()
        } else {
            new.clone()
        };
        let described = Arc::new(
            new.as_ref().clone().with_metadata(
                [(
                    "description".to_owned(),
                    "recovered schema transition".to_owned(),
                )]
                .into_iter()
                .collect(),
            ),
        );
        let described_physical = if kind == TableKind::KeyValue {
            zippy_types::key_value::log_schema(&described, &["key".to_owned()]).unwrap()
        } else {
            described.clone()
        };
        let alter = AlterTableSchemaCommand {
            table_id: table_id(),
            expected_schema_version: 1,
            schema_ipc: schema_ipc(&new),
        };
        let key_value = (kind == TableKind::KeyValue).then(|| {
            KeyValueTableDefinition::new(
                ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
            )
            .unwrap()
        });
        let mut response = None;
        let write_request = |version, sequence, key: &str| {
            let mut columns: Vec<arrow::array::ArrayRef> = vec![
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Int64Array::from(vec![42])),
            ];
            if version == 2 {
                columns.push(Arc::new(StringArray::from(vec!["added"])));
            }
            let batch = RecordBatch::try_new(
                if version == 1 {
                    old.clone()
                } else {
                    new.clone()
                },
                columns,
            )
            .unwrap();
            ArrowIpcMutationFingerprintComputer
                .prepare(IncomingWriteRequest {
                    request_id: request_id(1000 + u128::from(version) * 10 + u128::from(sequence)),
                    schema_version: version,
                    batch_sequence: sequence,
                    mutation: match kind {
                        TableKind::Append => TableMutation::Append(batch),
                        TableKind::Frame => TableMutation::Frame(batch),
                        TableKind::KeyValue => {
                            TableMutation::KeyValue(KeyValueMutation::Upsert(batch))
                        }
                    },
                })
                .unwrap()
        };
        for restart in 0..3 {
            let mut runtime =
                NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
            let ingest = runtime
                .start_native_ingest_with_config(
                    temp.path().join("wal"),
                    NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                    NativeTableMaterializerConfig::new(),
                )
                .unwrap();
            let admin = runtime.table_admin().unwrap();
            if restart == 0 {
                admin
                    .create_namespace(
                        request_id(1),
                        CreateNamespaceCommand {
                            namespace_id: namespace_id(),
                            name: CatalogName::new("market").unwrap(),
                        },
                    )
                    .await
                    .unwrap();
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
                let unsupported = Arc::new(Schema::new(vec![
                    old.field(0).clone(),
                    old.field(1).clone(),
                    Field::new("flag", DataType::Boolean, true),
                ]));
                assert_eq!(
                    admin
                        .create_table(
                            request_id(2),
                            CreateTableCommand {
                                table_id: table_id(),
                                namespace_id: namespace_id(),
                                name: CatalogName::new("events").unwrap(),
                                definition: definition.clone(),
                                schema_ipc: schema_ipc(&unsupported),
                                config: BTreeMap::new(),
                            }
                        )
                        .await
                        .unwrap_err()
                        .code(),
                    ErrorCode::SchemaMismatch
                );
                assert_eq!(
                    runtime
                        .catalog_read_handle()
                        .get_table(TableRef::Id {
                            table_id: table_id(),
                        })
                        .await
                        .unwrap_err()
                        .code(),
                    ErrorCode::NotFound
                );
                admin
                    .create_table(
                        request_id(2),
                        CreateTableCommand {
                            table_id: table_id(),
                            namespace_id: namespace_id(),
                            name: CatalogName::new("events").unwrap(),
                            definition,
                            schema_ipc: schema_ipc(&old),
                            config: BTreeMap::new(),
                        },
                    )
                    .await
                    .unwrap();
                let mut writer = ingest
                    .open_writer(OpenWriterRequest {
                        request_id: request_id(3),
                        table_id: table_id(),
                        producer_id: ProducerId::from_u128(1).unwrap(),
                        connection_id: ConnectionId::from_u128(1).unwrap(),
                        expected_writer_epoch: Some(0),
                    })
                    .await
                    .unwrap();
                tokio::task::block_in_place(|| writer.write_memory(write_request(1, 0, "a")))
                    .unwrap();
                let fixed_engine = runtime.query_engine();
                let fixed_session = SessionId::from_u128(9800).unwrap();
                let fixed_owner = fixed_engine.owner(fixed_session);
                let fixed_table = TableRef::Id {
                    table_id: table_id(),
                };
                let mut fixed_request = native_fixed_test_request(
                    9800,
                    fixed_session,
                    vec![fixed_table.clone()],
                    "SELECT key, price FROM frozen",
                );
                fixed_request.deadline_ns = Some(fixed_test_expiry());
                let fixed = fixed_engine
                    .capture_fixed_snapshot(
                        fixed_owner.as_ref(),
                        fixed_capture_request(fixed_request),
                    )
                    .await
                    .unwrap();
                let mut live = fixed_engine
                    .subscribe_log(
                        fixed_owner.as_ref(),
                        zippy_server::NativeLogRequest {
                            query_id: QueryId::from_u128(9899).unwrap(),
                            table: fixed_table.clone(),
                            start: Some(LogOffset::new(0)),
                            deadline_ns: None,
                            memory_limit_bytes: ResourceLimit::ServerDefault,
                            batch_rows: None,
                        },
                    )
                    .await
                    .unwrap()
                    .into_stream();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), live.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap()
                        .offsets,
                    OffsetRange::single(LogOffset::new(0))
                );
                let actor = runtime.table_actor(table_id()).unwrap();
                let retained = actor.snapshot().await.unwrap();
                let (window, owner) = prepare_event_window(&runtime, 900, Some(0)).await;
                let mut wrong = alter.clone();
                wrong.expected_schema_version = 9;
                assert_eq!(
                    admin
                        .alter_table_schema(request_id(4), wrong, key_value.clone())
                        .await
                        .unwrap_err()
                        .code(),
                    ErrorCode::Conflict
                );
                actor.snapshot().await.unwrap();
                assert_eq!(
                    admin
                        .alter_table_schema(
                            request_id(4),
                            AlterTableSchemaCommand {
                                table_id: table_id(),
                                expected_schema_version: 1,
                                schema_ipc: schema_ipc(&unsupported),
                            },
                            key_value.clone(),
                        )
                        .await
                        .unwrap_err()
                        .code(),
                    ErrorCode::SchemaMismatch
                );
                assert_eq!(
                    runtime
                        .catalog_read_handle()
                        .get_table(TableRef::Id {
                            table_id: table_id(),
                        })
                        .await
                        .unwrap()
                        .current_schema_version,
                    1
                );
                response = Some(
                    admin
                        .alter_table_schema(request_id(5), alter.clone(), key_value.clone())
                        .await
                        .unwrap(),
                );
                // Continuous readers must observe actor retirement even after a live-only read.
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(5), live.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .err()
                        .unwrap()
                        .code(),
                    ErrorCode::SchemaMismatch
                );
                assert!(live.next().await.is_none());
                drop(live);
                assert_eq!(
                    tokio::task::block_in_place(
                        || writer.write_memory(write_request(1, 1, "rejected"))
                    )
                    .unwrap_err()
                    .code(),
                    ErrorCode::SchemaMismatch
                );
                assert_eq!(
                    actor.snapshot().await.unwrap_err().code(),
                    ErrorCode::SchemaMismatch
                );
                assert_eq!(
                    retained.live_segments[0]
                        .span()
                        .as_record_batch()
                        .unwrap()
                        .schema(),
                    if kind == TableKind::KeyValue {
                        zippy_types::key_value::log_schema(&old, &["key".to_owned()]).unwrap()
                    } else {
                        old.clone()
                    }
                );
                let mut stream = window.into_stream();
                assert_eq!(
                    stream
                        .next()
                        .await
                        .unwrap()
                        .unwrap()
                        .batch
                        .record_batch()
                        .num_rows(),
                    1
                );
                assert!(stream.next().await.is_none());
                drop(stream);
                drop(owner);
                drop(retained);
                drop(actor);
                drop(writer);
                let mut writer = ingest
                    .open_writer(OpenWriterRequest {
                        request_id: request_id(6),
                        table_id: table_id(),
                        producer_id: ProducerId::from_u128(2).unwrap(),
                        connection_id: ConnectionId::from_u128(2).unwrap(),
                        expected_writer_epoch: Some(2),
                    })
                    .await
                    .unwrap();
                let ack = tokio::task::block_in_place(|| {
                    writer.submit_durable(write_request(2, 0, "b"), DurableWriteTarget::Wal)
                })
                .unwrap();
                ack.wait().await.unwrap();
                if kind == TableKind::KeyValue {
                    // The latest value for a persisted key must win across the live/file
                    // boundary; independently sorted children cannot satisfy this window.
                    let ack = tokio::task::block_in_place(|| {
                        writer.submit_durable(write_request(2, 1, "a"), DurableWriteTarget::Wal)
                    })
                    .unwrap();
                    ack.wait().await.unwrap();
                }
                let fixed_query = fixed_engine
                    .prepare_fixed(
                        &fixed,
                        fixed_owner.as_ref(),
                        native_fixed_test_request(
                            9801,
                            fixed_session,
                            vec![fixed_table.clone()],
                            "SELECT key, price FROM frozen",
                        ),
                        vec![
                            zippy_server::NativeQueryBinding::try_new("frozen", fixed_table)
                                .unwrap(),
                        ],
                    )
                    .await
                    .unwrap();
                assert_eq!(fixed_query.schema(), old);
                fixed.close().unwrap();
                let mut fixed_stream = fixed_query.into_stream().unwrap();
                let batch = std::future::poll_fn(|cx| fixed_stream.as_mut().poll_next(cx))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(batch.record_batch().num_rows(), 1);
                assert_eq!(
                    batch
                        .record_batch()
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    "a"
                );
                assert!(
                    std::future::poll_fn(|cx| fixed_stream.as_mut().poll_next(cx))
                        .await
                        .is_none()
                );
                drop((batch, fixed_stream, fixed_owner, fixed_engine));
            }
            assert_eq!(
                admin
                    .alter_table_schema(request_id(5), alter.clone(), key_value.clone())
                    .await
                    .unwrap(),
                *response.as_ref().unwrap()
            );
            assert_eq!(
                runtime
                    .catalog_read_handle()
                    .get_table(TableRef::Id {
                        table_id: table_id()
                    })
                    .await
                    .unwrap()
                    .current_schema_version,
                if restart == 2 { 3 } else { 2 }
            );
            let (window, owner) = prepare_event_window(&runtime, 901, Some(0)).await;
            assert_eq!(
                window.schema(),
                if restart == 2 {
                    described_physical.clone()
                } else {
                    physical.clone()
                }
            );
            let mut stream = window.into_stream();
            let mut comments = Vec::new();
            let mut offsets = Vec::new();
            while let Some(event) = stream.next().await {
                let event = event.unwrap();
                let batch = event.batch.record_batch();
                let column = batch
                    .column_by_name("comment")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    comments.push(if column.is_null(row) {
                        None
                    } else {
                        Some(column.value(row).to_owned())
                    });
                }
                offsets.extend(event.offsets.first().get()..=event.offsets.last().get());
            }
            let mut expected_comments = vec![None, Some("added".to_owned())];
            if kind == TableKind::KeyValue {
                expected_comments.push(Some("added".to_owned()));
            }
            assert_eq!(comments, expected_comments);
            assert_eq!(offsets, (0..comments.len() as u64).collect::<Vec<_>>());
            drop(stream);
            drop(owner);
            let engine = runtime.query_engine();
            let session = SessionId::from_u128(902).unwrap();
            let owner = engine.owner(session);
            let table = TableRef::Id {
                table_id: table_id(),
            };
            let prepared = engine
                .prepare_bound(
                    owner.as_ref(),
                    QueryRequest {
                        query_id: QueryId::from_u128(903).unwrap(),
                        session_id: session,
                        input: QueryInput::Sql(
                            "SELECT key, comment FROM events ORDER BY key".to_owned(),
                        ),
                        tables: vec![table.clone()],
                        priority: QueryPriority::Normal,
                        deadline_ns: None,
                        memory_limit_bytes: ResourceLimit::ServerDefault,
                        spill_limit_bytes: ResourceLimit::Disabled,
                        result_batch_rows: None,
                    },
                    vec![zippy_server::NativeQueryBinding::try_new("events", table).unwrap()],
                )
                .await
                .unwrap();
            let mut stream = prepared.into_stream().unwrap();
            let mut values = Vec::new();
            while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                let batch = batch.unwrap();
                let batch = batch.record_batch();
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let comments = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    values.push((
                        keys.value(row).to_owned(),
                        if comments.is_null(row) {
                            None
                        } else {
                            Some(comments.value(row).to_owned())
                        },
                    ));
                }
            }
            assert!(values.contains(&("b".to_owned(), Some("added".to_owned()))));
            if kind == TableKind::Append {
                assert!(values.contains(&("a".to_owned(), None)));
            } else if kind == TableKind::KeyValue {
                assert_eq!(
                    values,
                    vec![
                        ("a".to_owned(), Some("added".to_owned())),
                        ("b".to_owned(), Some("added".to_owned())),
                    ]
                );
            }
            drop(stream);
            drop(owner);
            drop(engine);
            drop(admin);
            if restart == 1 {
                ingest.checkpoint().await.unwrap();
                // No producer has reconnected in this process. The manifest now covers
                // retained v2 WAL, and registration under v3 must not replay those records.
                runtime
                    .table_admin()
                    .unwrap()
                    .alter_table_schema(
                        request_id(7),
                        AlterTableSchemaCommand {
                            table_id: table_id(),
                            expected_schema_version: 2,
                            schema_ipc: schema_ipc(&described),
                        },
                        key_value.clone(),
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(
                runtime.shutdown(Duration::from_secs(5)),
                NativeStorageShutdownOutcome::Complete
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_drop_retires_all_table_kinds_and_reuses_runtime_capacity() {
    use arrow::array::StringArray;
    use futures::StreamExt;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::catalog::DropTableCommand;
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, KeyValueMutation, MutationFingerprintComputer,
        OpenWriterRequest, TableMutation,
    };
    use zippy_types::table::{
        ColumnName, ColumnSet, FrameTableDefinition, KeyValueTableDefinition, TableKind, TableRef,
    };
    use zippy_types::{ConnectionId, ProducerId, QueryId, ResourceLimit, SessionId};

    for kind in [TableKind::Append, TableKind::Frame, TableKind::KeyValue] {
        let temp = tempfile::tempdir().unwrap();
        let ownership =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
        let config = NativeStorageRuntimeConfig::new(&ownership)
            .with_catalog_file_verification_config(
                StartupCatalogFileVerifierConfig::new()
                    .with_max_tables(1)
                    .with_max_runtime_metadata_bytes(131072),
            );
        let mut runtime = NativeStorageRuntime::start(config).unwrap();
        let ingest = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new(),
            )
            .unwrap();
        let admin = runtime.table_admin().unwrap();
        admin
            .create_namespace(
                request_id(1),
                CreateNamespaceCommand {
                    namespace_id: namespace_id(),
                    name: CatalogName::new("market").unwrap(),
                },
            )
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
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
        let table = CreateTableCommand {
            table_id: table_id(),
            namespace_id: namespace_id(),
            name: CatalogName::new("events").unwrap(),
            definition,
            schema_ipc: schema_ipc(&schema),
            config: BTreeMap::new(),
        };
        let created = admin
            .create_table(request_id(2), table.clone())
            .await
            .unwrap();
        let mut writer = ingest
            .open_writer(OpenWriterRequest {
                request_id: request_id(3),
                table_id: table_id(),
                producer_id: ProducerId::from_u128(1).unwrap(),
                connection_id: ConnectionId::from_u128(1).unwrap(),
                expected_writer_epoch: Some(0),
            })
            .await
            .unwrap();
        let make_write = |sequence: u64| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![42])),
                ],
            )
            .unwrap();
            ArrowIpcMutationFingerprintComputer
                .prepare(IncomingWriteRequest {
                    request_id: request_id(100 + sequence as u128),
                    schema_version: 1,
                    batch_sequence: sequence,
                    mutation: match kind {
                        TableKind::Append => TableMutation::Append(batch),
                        TableKind::Frame => TableMutation::Frame(batch),
                        TableKind::KeyValue => {
                            TableMutation::KeyValue(KeyValueMutation::Upsert(batch))
                        }
                    },
                })
                .unwrap()
        };
        tokio::task::block_in_place(|| writer.write_memory(make_write(0))).unwrap();
        let actor = runtime.table_actor(table_id()).unwrap();
        let pinned = actor.snapshot().await.unwrap();
        ingest.checkpoint().await.unwrap();
        let engine = runtime.query_engine();
        let owner = engine.owner(SessionId::from_u128(900).unwrap());
        let fixed_engine = runtime.query_engine();
        let fixed_session = SessionId::from_u128(9850).unwrap();
        let fixed_owner = fixed_engine.owner(fixed_session);
        let fixed_table = TableRef::Name {
            namespace: CatalogName::new("market").unwrap(),
            table: CatalogName::new("events").unwrap(),
        };
        let mut capture = native_fixed_test_request(
            9850,
            fixed_session,
            vec![fixed_table.clone()],
            "SELECT price FROM frozen",
        );
        capture.deadline_ns = Some(fixed_test_expiry());
        let fixed = fixed_engine
            .capture_fixed_snapshot(fixed_owner.as_ref(), fixed_capture_request(capture))
            .await
            .unwrap();

        let mut log = engine
            .subscribe_log(
                owner.as_ref(),
                zippy_server::NativeLogRequest {
                    query_id: QueryId::from_u128(9001).unwrap(),
                    table: TableRef::Id {
                        table_id: table_id(),
                    },
                    start: None,
                    deadline_ns: None,
                    memory_limit_bytes: ResourceLimit::ServerDefault,
                    batch_rows: None,
                },
            )
            .await
            .unwrap()
            .into_stream();
        let mut replay = engine
            .subscribe_log(
                owner.as_ref(),
                zippy_server::NativeLogRequest {
                    query_id: QueryId::from_u128(9002).unwrap(),
                    table: TableRef::Id {
                        table_id: table_id(),
                    },
                    start: Some(LogOffset::new(0)),
                    deadline_ns: None,
                    memory_limit_bytes: ResourceLimit::ServerDefault,
                    batch_rows: None,
                },
            )
            .await
            .unwrap()
            .into_stream();
        let current = runtime
            .catalog_read_handle()
            .get_table(TableRef::Id {
                table_id: table_id(),
            })
            .await
            .unwrap();
        let wrong = admin
            .drop_table(
                request_id(4),
                DropTableCommand {
                    table_id: table_id(),
                    expected_snapshot_version: current.current_snapshot_version + 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(wrong.code(), ErrorCode::Conflict);
        // Failed Catalog CAS rolls both actor and writer admission back.
        actor.snapshot().await.unwrap();
        tokio::task::block_in_place(|| writer.write_memory(make_write(1))).unwrap();
        let drop_command = DropTableCommand {
            table_id: table_id(),
            expected_snapshot_version: current.current_snapshot_version,
        };
        let dropped = admin
            .drop_table(request_id(5), drop_command.clone())
            .await
            .unwrap();
        assert_eq!(
            admin
                .drop_table(request_id(5), drop_command.clone())
                .await
                .unwrap(),
            dropped
        );
        assert_eq!(
            tokio::task::block_in_place(|| writer.write_memory(make_write(2)))
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::NotFound
        );
        assert_eq!(runtime.native_ingest_status().unwrap().table_count, 0);
        assert!(runtime.is_healthy());
        let event = tokio::time::timeout(Duration::from_secs(2), log.next())
            .await
            .unwrap();
        assert!(matches!(event, Some(Err(error)) if error.code() == ErrorCode::NotFound));
        assert_eq!(
            pinned.live_segments[0]
                .span()
                .as_record_batch()
                .unwrap()
                .num_rows(),
            1
        );
        // Historical create retries cannot restore the dropped table or consume capacity.
        assert_eq!(
            admin
                .create_table(request_id(2), table.clone())
                .await
                .unwrap(),
            created
        );
        assert!(!ingest.contains_table(table_id()).unwrap());
        assert!(
            matches!(tokio::time::timeout(Duration::from_secs(2), replay.next()).await.unwrap(),
            Some(Err(error)) if error.code() == ErrorCode::NotFound)
        );
        drop(replay);
        drop(log);
        drop(owner);
        drop(engine);
        drop(pinned);
        drop(actor);
        drop(writer);
        // Repeated create/drop exceeds the byte budget without correct per-table refunds.
        for index in 0..64_u128 {
            let mut next = table.clone();
            next.table_id = TableId::from_u128(1000 + index).unwrap();
            // The tombstone retains its ID and history while releasing this exact name.
            next.name = table.name.clone();
            admin
                .create_table(request_id(1000 + index * 2), next.clone())
                .await
                .unwrap();
            assert_eq!(
                admin
                    .create_table(request_id(2), table.clone())
                    .await
                    .unwrap(),
                created,
                "old create retries must succeed when the same-name replacement fills capacity",
            );
            assert!(!ingest.contains_table(table_id()).unwrap());
            assert!(ingest.contains_table(next.table_id).unwrap());
            if index == 0 || index == 63 {
                let prepared = fixed_engine
                    .prepare_fixed(
                        &fixed,
                        fixed_owner.as_ref(),
                        native_fixed_test_request(
                            9860 + index,
                            fixed_session,
                            vec![fixed_table.clone()],
                            "SELECT price FROM frozen",
                        ),
                        vec![zippy_server::NativeQueryBinding::try_new(
                            "frozen",
                            fixed_table.clone(),
                        )
                        .unwrap()],
                    )
                    .await
                    .unwrap();
                assert_eq!(collect_fixed_prices(prepared).await, [42]);
                let replacement = TableRef::Id {
                    table_id: next.table_id,
                };
                let error = fixed_engine
                    .prepare_fixed(
                        &fixed,
                        fixed_owner.as_ref(),
                        native_fixed_test_request(
                            9960 + index,
                            fixed_session,
                            vec![replacement.clone()],
                            "SELECT price FROM frozen",
                        ),
                        vec![
                            zippy_server::NativeQueryBinding::try_new("frozen", replacement)
                                .unwrap(),
                        ],
                    )
                    .await
                    .err()
                    .unwrap();
                assert_eq!(error.code(), ErrorCode::InvalidArgument);
            }

            admin
                .drop_table(
                    request_id(1001 + index * 2),
                    DropTableCommand {
                        table_id: next.table_id,
                        expected_snapshot_version: 1,
                    },
                )
                .await
                .unwrap();
        }
        fixed.close().unwrap();
        drop((fixed_engine, fixed_owner));
        drop(admin);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
        drop(runtime);
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        runtime
            .start_native_ingest(temp.path().join("wal"))
            .unwrap();
        assert_eq!(runtime.native_ingest_status().unwrap().table_count, 0);
        let admin = runtime.table_admin().unwrap();
        assert_eq!(
            admin.drop_table(request_id(5), drop_command).await.unwrap(),
            dropped
        );
        assert_eq!(
            admin.create_table(request_id(2), table).await.unwrap(),
            created
        );
        assert_eq!(runtime.native_ingest_status().unwrap().table_count, 0);
        drop(admin);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_checkpoints_recycle_tiny_budgets_and_recover_exact_receipts() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, MutationFingerprintComputer, OpenWriterRequest,
        TableMutation,
    };
    use zippy_types::{ConnectionId, ProducerId};

    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let wal_root = temp.path().join("wal");
    let ingest_config = NativeIngestConfig::new()
        .with_max_receipts_per_epoch(1)
        .with_checkpoint_interval(Duration::from_secs(60));
    let materializer_config = NativeTableMaterializerConfig::new()
        .with_row_capacity(2)
        .with_max_live_segments(3)
        .with_max_buffered_bytes(4096);
    let make_request = |sequence: u64, price: i64| {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(1000 + u128::from(sequence)),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Append(
                    RecordBatch::try_new(
                        test_schema(),
                        vec![Arc::new(Int64Array::from(vec![price, price + 1]))],
                    )
                    .unwrap(),
                ),
            })
            .unwrap()
    };
    let open = OpenWriterRequest {
        request_id: request_id(100),
        table_id: table_id(),
        producer_id: ProducerId::from_u128(1).unwrap(),
        connection_id: ConnectionId::from_u128(1).unwrap(),
        expected_writer_epoch: Some(0),
    };
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let handle = runtime
        .start_native_ingest_with_config(&wal_root, ingest_config.clone(), materializer_config)
        .unwrap();
    let admin = runtime.table_admin().unwrap();
    let namespace = CreateNamespaceCommand {
        namespace_id: namespace_id(),
        name: CatalogName::new("market").unwrap(),
    };
    admin
        .create_namespace(request_id(1), namespace.clone())
        .await
        .unwrap();
    admin
        .create_namespace(request_id(1), namespace)
        .await
        .unwrap();
    let table = CreateTableCommand {
        table_id: table_id(),
        namespace_id: namespace_id(),
        name: CatalogName::new("ticks").unwrap(),
        definition: TableDefinition::Append(AppendTableDefinition::default()),
        schema_ipc: schema_ipc(test_schema().as_ref()),
        config: BTreeMap::new(),
    };
    let created = admin
        .create_table(request_id(2), table.clone())
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_table(request_id(2), table.clone())
            .await
            .unwrap(),
        created
    );
    let mut conflicting = table.clone();
    conflicting.name = CatalogName::new("different").unwrap();
    assert_eq!(
        admin
            .create_table(request_id(2), conflicting)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(runtime.native_ingest_status().unwrap().table_count, 1);
    let mut writer = handle.open_writer(open.clone()).await.unwrap();
    let write = |writer: &mut zippy_types::ingest::OpaqueIngestWriter, request| {
        tokio::task::block_in_place(|| writer.write_memory(request))
    };
    let first = write(&mut writer, make_request(0, 10)).unwrap();
    let pinned = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    handle.checkpoint().await.unwrap();
    let old = zippy_core::SegmentTableView::from_row_span(pinned.live_segments[0].span().clone())
        .to_record_batch()
        .unwrap();
    assert_eq!(
        old.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    drop(old);
    drop(pinned);
    for sequence in 0..20 {
        if sequence > 0 {
            write(&mut writer, make_request(sequence, 10)).unwrap();
        }
        handle.checkpoint().await.unwrap();
    }
    assert_eq!(
        write(&mut writer, make_request(0, 10)).unwrap().offsets(),
        first.offsets()
    );
    assert_eq!(
        write(&mut writer, make_request(0, 999)).unwrap_err().code(),
        ErrorCode::Conflict
    );
    handle.checkpoint().await.unwrap();
    let snapshot = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    assert_eq!(
        snapshot.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(39))
    );
    assert!(snapshot.live_segments.is_empty());
    let wal_directory = wal_root.join(table_id().to_string());
    let wal_files = || {
        fs::read_dir(&wal_directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|value| value == "wal"))
            .count()
    };
    assert_eq!(wal_files(), 1, "checkpoint retains only the new WAL tail");
    assert_eq!(
        runtime.native_ingest_status().unwrap().checkpoints_total,
        20
    );
    let tail = write(&mut writer, make_request(20, 20)).unwrap();
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    assert_eq!(
        admin
            .create_table(request_id(2), table.clone())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unavailable
    );
    drop(admin);
    drop(runtime);
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let handle = runtime
        .start_native_ingest_with_config(&wal_root, ingest_config, materializer_config)
        .unwrap();
    assert_eq!(
        runtime
            .table_admin()
            .unwrap()
            .create_table(request_id(2), table)
            .await
            .unwrap(),
        created
    );
    let mut resumed = open;
    resumed.connection_id = ConnectionId::from_u128(2).unwrap();
    // Replay the original acquisition identity to resume its epoch after restart.
    let mut writer = handle.open_writer(resumed).await.unwrap();
    assert_eq!(
        write(&mut writer, make_request(0, 10)).unwrap().offsets(),
        first.offsets()
    );
    assert_eq!(
        write(&mut writer, make_request(20, 20)).unwrap().offsets(),
        tail.offsets()
    );
    handle.checkpoint().await.unwrap();
    let empty = ArrowIpcMutationFingerprintComputer
        .prepare(IncomingWriteRequest {
            request_id: request_id(1021),
            schema_version: 1,
            batch_sequence: 21,
            mutation: TableMutation::Append(RecordBatch::new_empty(test_schema())),
        })
        .unwrap();
    let receipt = write(&mut writer, empty.clone()).unwrap();
    assert_eq!(receipt.offsets(), None);
    handle.checkpoint().await.unwrap();
    assert_eq!(
        write(&mut writer, empty).unwrap().committed_log_offset(),
        tail.committed_log_offset()
    );
    assert_eq!(
        write(&mut writer, make_request(22, 22))
            .unwrap()
            .offsets()
            .unwrap()
            .first(),
        LogOffset::new(42)
    );
    handle.checkpoint().await.unwrap();
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

struct TerminalCatalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_checkpoint_worker_persists_without_an_explicit_flush() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, MutationFingerprintComputer, OpenWriterRequest,
        TableMutation,
    };
    use zippy_types::{ConnectionId, ProducerId};
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
    catalog.close().unwrap();
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let handle = runtime
        .start_native_ingest_with_config(
            temp.path().join("wal"),
            NativeIngestConfig::new().with_checkpoint_interval(Duration::from_millis(1)),
            NativeTableMaterializerConfig::new().with_row_capacity(2),
        )
        .unwrap();
    let mut writer = handle
        .open_writer(OpenWriterRequest {
            request_id: request_id(100),
            table_id: table_id(),
            producer_id: ProducerId::from_u128(1).unwrap(),
            connection_id: ConnectionId::from_u128(1).unwrap(),
            expected_writer_epoch: Some(0),
        })
        .await
        .unwrap();
    let request = ArrowIpcMutationFingerprintComputer
        .prepare(IncomingWriteRequest {
            request_id: request_id(1000),
            schema_version: 1,
            batch_sequence: 0,
            mutation: TableMutation::Append(
                RecordBatch::try_new(
                    test_schema(),
                    vec![Arc::new(Int64Array::from(vec![10, 11]))],
                )
                .unwrap(),
            ),
        })
        .unwrap();
    tokio::task::block_in_place(|| writer.write_memory(request)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.native_ingest_status().unwrap().checkpoints_total == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    // Closing public admission must retain the actor/persistence dependencies
    // until every accepted checkpoint operation has drained.
    runtime.begin_shutdown();
    assert!(!runtime.native_ingest_status().unwrap().accepting);
    assert_eq!(
        runtime
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(1))
    );
    drop(writer);
    let outcome = runtime.shutdown(Duration::from_secs(5));
    assert!(outcome.is_complete(), "shutdown outcome: {outcome:?}");
}

/// Opt-in real persistence benchmark; deliberately excluded from deterministic CI timing gates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "release-mode native ingest persistence benchmark"]
async fn native_checkpoint_throughput_benchmark() {
    use arrow::array::{ArrayRef, StringArray};
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, MutationFingerprintComputer, OpenWriterRequest,
        TableMutation,
    };
    use zippy_types::{ConnectionId, ProducerId};
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    let mut fields = vec![Field::new("instrument_id", DataType::Utf8, false)];
    for side in ["bid", "ask"] {
        for level in 1..=10 {
            for metric in ["price_scaled", "volume_shares"] {
                fields.push(Field::new(
                    format!("{side}_{metric}_{level:02}"),
                    DataType::Int64,
                    false,
                ));
            }
        }
    }
    let schema = Arc::new(Schema::new(fields));
    create_test_table_with_schema(&catalog, &schema).await;
    catalog.close().unwrap();
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let handle = runtime
        .start_native_ingest_with_config(
            temp.path().join("wal"),
            NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
            NativeTableMaterializerConfig::new().with_row_capacity(8192),
        )
        .unwrap();
    let mut writer = handle
        .open_writer(OpenWriterRequest {
            request_id: request_id(100),
            table_id: table_id(),
            producer_id: ProducerId::from_u128(1).unwrap(),
            connection_id: ConnectionId::from_u128(1).unwrap(),
            expected_writer_epoch: Some(0),
        })
        .await
        .unwrap();
    const ROWS: usize = 1024;
    const BATCHES: u64 = 32;
    let rounds: u64 = std::env::var("ZIPPY_CHECKPOINT_BENCH_ROUNDS")
        .map(|value| value.parse().expect("positive benchmark round count"))
        .unwrap_or(30);
    assert!(rounds > 0);
    let batches = (0..BATCHES)
        .map(|index| {
            let symbols = (0..ROWS)
                .map(|row| format!("S{:05}", (index as usize * ROWS + row) % 10000))
                .collect::<Vec<_>>();
            let mut arrays = vec![Arc::new(StringArray::from(symbols)) as ArrayRef];
            for column in 0..40 {
                arrays
                    .push(Arc::new(Int64Array::from_iter_values((0..ROWS).map(|row| {
                        10000 + (row as i64 * 17 + column * 7 + index as i64) % 1024
                    }))) as ArrayRef);
            }
            RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap()
        })
        .collect::<Vec<_>>();
    let mut latencies = Vec::new();
    let start = Instant::now();
    for round in 0..rounds {
        tokio::task::block_in_place(|| {
            for index in 0..BATCHES {
                let sequence = round * BATCHES + index;
                let started = Instant::now();
                let request = ArrowIpcMutationFingerprintComputer
                    .prepare(IncomingWriteRequest {
                        request_id: request_id(1000 + u128::from(sequence)),
                        schema_version: 1,
                        batch_sequence: sequence,
                        mutation: TableMutation::Append(batches[index as usize].clone()),
                    })
                    .unwrap();
                writer.write_memory(request).unwrap();
                latencies.push(started.elapsed().as_micros());
            }
        });
        handle.checkpoint().await.unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let rows = rounds * BATCHES * ROWS as u64;
    assert_eq!(
        runtime
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(rows - 1))
    );
    println!(
        "{}",
        serde_json::json!({"case": "native_append_manifest_checkpoint",
        "rows": rows, "schema": "instrument_id:utf8 + 40 int64 depth fields", "symbols": 10000,
        "elapsed_seconds": elapsed,
        "rows_per_second": rows as f64 / elapsed, "batch_rows": ROWS,
        "admission_p99_us": latencies[latencies.len() * 99 / 100],
        "peak_rss": fs::read_to_string("/proc/self/status").ok().and_then(|status|
            status.lines().find(|line| line.starts_with("VmHWM:")).map(str::to_owned)),
        "checkpoints": runtime.native_ingest_status().unwrap().checkpoints_total})
    );
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

impl CatalogWrite for TerminalCatalog {
    fn command(
        &self,
        _command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, zippy_types::Result<VersionedCatalogResponse>> {
        Box::pin(async {
            Err(
                ServerError::new(ErrorCode::DataCorruption, "injected fatal Catalog failure")
                    .with_retryable(false),
            )
        })
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::from_u128(NAMESPACE_ID).expect("nonzero test namespace ID")
}

fn table_id() -> TableId {
    TableId::from_u128(TABLE_ID).expect("nonzero test table ID")
}

fn request_id(value: u128) -> RequestId {
    RequestId::from_u128(value).expect("nonzero test request ID")
}

fn server_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "native-storage-runtime-test".to_owned(),
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]))
}

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
    writer.finish().unwrap();
    drop(writer);
    SchemaIpcPayload::new(bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn online_creation_enforces_startup_budgets_before_catalog_commit() {
    for (table_limit, byte_limit, accepted) in [(1, 1_048_576, 1), (10, 1, 0)] {
        let temp = tempfile::tempdir().unwrap();
        let ownership =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
        let mut runtime = NativeStorageRuntime::start(
            NativeStorageRuntimeConfig::new(&ownership).with_catalog_file_verification_config(
                StartupCatalogFileVerifierConfig::new()
                    .with_max_tables(table_limit)
                    .with_max_runtime_metadata_bytes(byte_limit),
            ),
        )
        .unwrap();
        runtime
            .start_native_ingest(temp.path().join("wal"))
            .unwrap();
        let admin = runtime.table_admin().unwrap();
        admin
            .create_namespace(
                request_id(1),
                CreateNamespaceCommand {
                    namespace_id: namespace_id(),
                    name: CatalogName::new("market").unwrap(),
                },
            )
            .await
            .unwrap();
        for index in 0..=accepted {
            let id = TableId::from_u128(100 + index).unwrap();
            let result = admin
                .create_table(
                    request_id(10 + index),
                    CreateTableCommand {
                        table_id: id,
                        namespace_id: namespace_id(),
                        name: CatalogName::new(format!("table_{index}")).unwrap(),
                        definition: TableDefinition::Append(AppendTableDefinition::default()),
                        schema_ipc: schema_ipc(test_schema().as_ref()),
                        config: BTreeMap::new(),
                    },
                )
                .await;
            if index < accepted {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().code(), ErrorCode::ResourceExhausted);
                assert_eq!(
                    runtime
                        .catalog_read_handle()
                        .get_table(zippy_types::table::TableRef::Id { table_id: id },)
                        .await
                        .unwrap_err()
                        .code(),
                    ErrorCode::NotFound
                );
            }
        }
        assert_eq!(
            runtime.native_ingest_status().unwrap().table_count as u128,
            accepted
        );
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_receipts_cover_real_manifests_and_survive_restart_exactly() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        Durability, DurableWriteTarget, IncomingWriteRequest, IngestService,
        MutationFingerprintComputer, OpenWriterRequest, TableMutation,
    };
    use zippy_types::{ConnectionId, ProducerId};
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
    catalog.close().unwrap();
    let start = || {
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let handle = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new()
                    .with_max_receipts_per_epoch(1)
                    .with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new().with_row_capacity(2),
            )
            .unwrap();
        (runtime, handle)
    };
    let (mut runtime, handle) = start();
    let open = OpenWriterRequest {
        request_id: request_id(100),
        table_id: table_id(),
        producer_id: ProducerId::from_u128(1).unwrap(),
        connection_id: ConnectionId::from_u128(1).unwrap(),
        expected_writer_epoch: Some(0),
    };
    let mut writer = handle.open_writer(open.clone()).await.unwrap();
    let request = |sequence: u64, values: Vec<i64>| {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(200 + sequence as u128),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Append(
                    RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(values))])
                        .unwrap(),
                ),
            })
            .unwrap()
    };
    let mut receipts = Vec::new();
    for (sequence, values) in [vec![], vec![10, 11], vec![20]].into_iter().enumerate() {
        let ack = tokio::task::block_in_place(|| {
            writer.submit_durable(
                request(sequence as u64, values),
                DurableWriteTarget::Persisted,
            )
        })
        .unwrap();
        let receipt = ack.wait().await.unwrap();
        assert_eq!(receipt.durability_reached(), Durability::Persisted);
        let snapshot = runtime
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        assert_eq!(
            receipt.persisted_snapshot_version(),
            Some(snapshot.persisted_snapshot_version)
        );
        assert_eq!(
            snapshot.committed_log_offset,
            receipt.committed_log_offset()
        );
        assert!(snapshot.live_segments.is_empty());
        receipts.push(receipt);
    }
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.persisted_snapshot_version().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);
    let (mut runtime, handle) = start();
    let mut resumed = open;
    resumed.connection_id = ConnectionId::from_u128(2).unwrap();
    let mut writer = handle.open_writer(resumed).await.unwrap();
    for (sequence, values) in [vec![], vec![10, 11], vec![20]].into_iter().enumerate() {
        let ack = tokio::task::block_in_place(|| {
            writer.submit_durable(
                request(sequence as u64, values),
                DurableWriteTarget::Persisted,
            )
        })
        .unwrap();
        assert_eq!(ack.wait().await.unwrap(), receipts[sequence]);
    }
    assert_eq!(
        runtime
            .catalog_file_verification_report()
            .managed_file_count,
        2
    );
    assert_eq!(
        runtime
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(2))
    );
    let window = prepare_event_window(&runtime, 7001, Some(1)).await;
    assert_eq!(window.0.next_offset(), Some(LogOffset::new(3)));
    assert_eq!(
        consume_event_window(&runtime, window).await,
        vec![(1, None, Some(11), None), (2, None, Some(20), None),]
    );
    let window = prepare_event_window(&runtime, 7002, None).await;
    assert_eq!(window.0.next_offset(), Some(LogOffset::new(3)));
    assert!(consume_event_window(&runtime, window).await.is_empty());
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

fn catalog_command(
    request: u128,
    expected_revision: Option<u64>,
    command: CatalogCommandV1,
) -> zippy_types::catalog::VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(request),
        expected_revision,
        server_actor(),
        command,
    )
    .unwrap()
    .into()
}

async fn create_test_table(catalog: &SqliteCatalog) {
    create_test_table_with_schema(catalog, &test_schema()).await;
}

async fn create_test_table_with_schema(catalog: &SqliteCatalog, schema: &SchemaRef) {
    catalog
        .command(catalog_command(
            1,
            Some(0),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: namespace_id(),
                name: CatalogName::new("market").unwrap(),
            }),
        ))
        .await
        .unwrap();
    catalog
        .command(catalog_command(
            2,
            Some(1),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: table_id(),
                namespace_id: namespace_id(),
                name: CatalogName::new("ticks").unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: schema_ipc(schema.as_ref()),
                config: BTreeMap::new(),
            }),
        ))
        .await
        .unwrap();
}

fn write_test_candidate(
    ownership: &Arc<OwnedDataDirectory>,
    file: u128,
    offset: u64,
    price: i64,
) -> zippy_storage::DurableParquetCandidate {
    let offsets = OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset)).unwrap();
    let provenance = NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: offset + 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .unwrap(),
    )
    .unwrap();
    let schema = test_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![price]))],
    )
    .unwrap();
    ParquetCandidateWriter::new(Arc::clone(ownership))
        .write_candidate(
            ParquetCandidateSpec {
                file_id: FileId::from_u128(file).unwrap(),
                table_id: table_id(),
                schema_version: 1,
                schema,
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: 1_752_492_600_000_000_000 + file as u64,
            },
            vec![batch],
        )
        .unwrap()
}

fn test_persist_request(
    request: u128,
    file: u128,
    offset: u64,
    price: i64,
    expected_snapshot_version: u64,
) -> PersistRequest {
    PersistRequest::new(
        request_id(request),
        table_id(),
        expected_snapshot_version,
        0,
        vec![test_persist_input(file, offset, price)],
        OffsetWatermark::Known(LogOffset::new(offset)),
    )
    .unwrap()
}

fn test_persist_input(file: u128, offset: u64, price: i64) -> PersistCandidateInput {
    let offsets = OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset)).unwrap();
    let provenance = NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: offset + 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .unwrap(),
    )
    .unwrap();
    let schema = test_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![price]))],
    )
    .unwrap();
    PersistCandidateInput::new(
        ParquetCandidateSpec {
            file_id: FileId::from_u128(file).unwrap(),
            table_id: table_id(),
            schema_version: 1,
            schema,
            provenance,
            partition: MetadataMap::new(),
            metadata: MetadataMap::new(),
            created_at_ns: 1_752_492_600_000_000_000 + file as u64,
        },
        vec![batch],
    )
    .unwrap()
}

#[tokio::test]
async fn supervisor_starts_inside_an_existing_tokio_runtime_and_closes_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let directory = ownership.directory().clone();

    let mut runtime = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("native storage should become ready");

    assert_eq!(runtime.reconciliation_report().catalog_revision, 0);
    assert!(runtime.reconciliation_report().has_no_observed_backlog());
    assert_eq!(
        runtime.catalog_file_verification_report().catalog_revision,
        0
    );
    assert_eq!(runtime.catalog_file_verification_report().table_count, 0);
    assert_eq!(runtime.table_runtime_count(), 0);
    assert_eq!(runtime.table_runtime_catalog_revision(), 0);
    assert_eq!(
        runtime.table_actor(table_id()).unwrap_err().code(),
        ErrorCode::NotFound
    );
    assert_eq!(
        runtime
            .catalog_file_verification_report()
            .managed_file_count,
        0
    );
    assert!(root.join("catalog/catalog.db").is_file());
    assert!(matches!(
        directory.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));
    let read_handle = runtime.catalog_read_handle();
    assert_eq!(read_handle.revision().await.unwrap(), 0);

    assert!(runtime.shutdown(Duration::from_secs(2)).is_complete());
    let closed_error = read_handle
        .revision()
        .await
        .expect_err("retained read handle must fail after Catalog shutdown");
    assert_eq!(closed_error.code(), ErrorCode::Unavailable);
    assert!(!closed_error.retryable());
    drop(runtime);
    assert!(matches!(
        directory.clone().acquire_process_ownership(),
        Err(DataDirectoryError::OwnershipHeld { .. })
    ));

    drop(ownership);
    directory
        .acquire_process_ownership()
        .expect("the caller releases ownership after native storage is closed");
}

#[tokio::test]
async fn actor_persist_is_recovered_as_the_same_snapshot_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    Arc::try_unwrap(catalog)
        .ok()
        .expect("table setup released its Catalog references")
        .close()
        .unwrap();

    let mut runtime = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("native storage should recover the created table");
    let actor = runtime.table_actor(table_id()).unwrap();
    let before = actor.snapshot().await.unwrap();
    assert_eq!(before.persisted_snapshot_version, 1);
    assert_eq!(before.committed_log_offset, OffsetWatermark::Empty);
    assert_eq!(before.persistence_status, TablePersistenceStatus::Idle);

    let receipt = actor
        .persist(test_persist_request(100, 100, 0, 42, 1))
        .await
        .expect("actor should validate and commit the native candidate");
    assert_eq!(receipt.snapshot.snapshot_version, 2);
    assert_eq!(
        receipt.snapshot.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    let committed = actor.snapshot().await.unwrap();
    assert_eq!(committed.persisted_snapshot_version, 2);
    assert_eq!(
        committed.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    assert_eq!(committed.persistence_status, TablePersistenceStatus::Idle);
    assert!(runtime.shutdown(Duration::from_secs(2)).is_complete());

    let mut restarted = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("native storage should recover the committed snapshot and candidate");
    let restarted_actor = restarted.table_actor(table_id()).unwrap();
    let recovered = restarted_actor.snapshot().await.unwrap();
    assert_eq!(recovered.persisted_snapshot_version, 2);
    assert_eq!(
        recovered.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    assert_eq!(recovered.persistence_status, TablePersistenceStatus::Idle);

    let replayed = restarted_actor
        .persist(test_persist_request(100, 100, 0, 42, 1))
        .await
        .expect("the Catalog must replay the original durable receipt after restart");
    assert_eq!(replayed.request_id, receipt.request_id);
    assert_eq!(replayed.catalog_revision, receipt.catalog_revision);
    assert_eq!(replayed.snapshot, receipt.snapshot);
    assert_eq!(
        restarted_actor
            .snapshot()
            .await
            .unwrap()
            .persisted_snapshot_version,
        2
    );

    let conflicting_replay = restarted_actor
        .persist(test_persist_request(100, 101, 0, 43, 1))
        .await
        .expect_err("one request id cannot name a different durable manifest");
    assert_eq!(conflicting_replay.code(), ErrorCode::Conflict);
    assert!(restarted.is_healthy());
    assert!(!root
        .join(format!(
            "parquet/{}/{}.parquet",
            table_id(),
            FileId::from_u128(101).unwrap()
        ))
        .exists());

    let advanced = restarted_actor
        .persist(test_persist_request(101, 102, 1, 44, 2))
        .await
        .expect("a rejected replay must not poison later native writes");
    assert_eq!(advanced.snapshot.snapshot_version, 3);

    let current_parent_conflict = restarted_actor
        .persist(test_persist_request(100, 103, 2, 45, 3))
        .await
        .expect_err("a reused request id cannot name a new current-parent manifest");
    assert_eq!(current_parent_conflict.code(), ErrorCode::Conflict);
    assert!(restarted.is_healthy());
    assert!(!root
        .join(format!(
            "parquet/{}/{}.parquet",
            table_id(),
            FileId::from_u128(103).unwrap()
        ))
        .exists());

    let continued = restarted_actor
        .persist(test_persist_request(102, 104, 2, 46, 3))
        .await
        .expect("a definitive current-parent rejection must remain request-local");
    assert_eq!(continued.snapshot.snapshot_version, 4);
    assert_eq!(
        fs::read_dir(root.join("wal/catalog-commit-intents"))
            .unwrap()
            .count(),
        0
    );
    assert!(restarted.shutdown(Duration::from_secs(2)).is_complete());

    let mut verified = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("a rejected replay must leave restart recovery clean");
    assert_eq!(
        verified
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap()
            .persisted_snapshot_version,
        4
    );
    assert!(verified.shutdown(Duration::from_secs(2)).is_complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_persist_failure_revokes_health_and_fails_ordered_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    Arc::try_unwrap(catalog)
        .ok()
        .expect("table setup released its Catalog references")
        .close()
        .unwrap();

    let mut runtime = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("native storage should become ready before fault injection");
    assert!(runtime.is_healthy());
    let actor = runtime.table_actor(table_id()).unwrap();
    write_test_candidate(&ownership, 200, 0, 1);
    let error = actor
        .persist(test_persist_request(200, 200, 0, 2, 1))
        .await
        .expect_err("conflicting durable candidate content must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);

    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.is_healthy() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("persist controller failure must revoke native readiness");
    assert_eq!(
        actor.snapshot().await.unwrap_err().code(),
        ErrorCode::Unavailable
    );

    let outcome = runtime.shutdown(Duration::from_secs(2));
    let failure = outcome
        .error()
        .expect("terminal persist failure must survive ordered cleanup");
    assert_eq!(failure.code(), ErrorCode::DataCorruption);
    assert!(matches!(outcome, NativeStorageShutdownOutcome::Failed(_)));
}

#[test]
fn shutdown_timeout_keeps_the_runtime_handle_and_can_be_observed_again() {
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("database")).unwrap());
    let mut runtime = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("native storage should become ready");

    let first = runtime.shutdown_until(Instant::now());
    assert!(matches!(
        first,
        NativeStorageShutdownOutcome::Complete | NativeStorageShutdownOutcome::TimedOut
    ));
    let final_outcome = runtime.shutdown(Duration::from_secs(2));
    assert!(final_outcome.is_complete());
}

#[test]
fn invalid_server_actor_fails_before_catalog_startup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let config = NativeStorageRuntimeConfig::new(&ownership).with_actor(CatalogActor {
        kind: CatalogActorKind::User,
        principal: "not-the-server".to_owned(),
    });

    match NativeStorageRuntime::start_with_timeout(config, Duration::from_secs(1)) {
        NativeStorageStartOutcome::Failed(error) => {
            assert_eq!(error.code(), ErrorCode::InvalidArgument);
            assert!(!root.join("catalog/catalog.db").exists());
        }
        NativeStorageStartOutcome::Ready(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("invalid Catalog actor unexpectedly started native storage");
        }
        NativeStorageStartOutcome::TimedOut(startup) => {
            startup.cancel();
            panic!("invalid Catalog actor unexpectedly reached supervisor startup");
        }
    }
}

#[test]
fn invalid_table_actor_budget_fails_before_catalog_startup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let config = NativeStorageRuntimeConfig::new(&ownership).with_table_actor_config(
        TableActorRegistryConfig::new().with_per_table_mailbox_capacity(0),
    );

    match NativeStorageRuntime::start_with_timeout(config, Duration::from_secs(1)) {
        NativeStorageStartOutcome::Failed(error) => {
            assert_eq!(error.code(), ErrorCode::InvalidArgument);
            assert!(!root.join("catalog/catalog.db").exists());
        }
        NativeStorageStartOutcome::Ready(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("invalid table actor budget unexpectedly started native storage");
        }
        NativeStorageStartOutcome::TimedOut(startup) => {
            startup.cancel();
            panic!("invalid table actor budget unexpectedly reached supervisor startup");
        }
    }
}

#[test]
fn invalid_persist_controller_budget_fails_before_catalog_startup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let config = NativeStorageRuntimeConfig::new(&ownership).with_persist_controller_config(
        PersistControllerConfig::new().with_max_parallel_writers(0),
    );

    match NativeStorageRuntime::start_with_timeout(config, Duration::from_secs(1)) {
        NativeStorageStartOutcome::Failed(error) => {
            assert_eq!(error.code(), ErrorCode::InvalidArgument);
            assert!(!root.join("catalog/catalog.db").exists());
        }
        NativeStorageStartOutcome::Ready(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("invalid persist controller budget unexpectedly started native storage");
        }
        NativeStorageStartOutcome::TimedOut(startup) => {
            startup.cancel();
            panic!("invalid persist controller budget unexpectedly reached supervisor startup");
        }
    }
}

#[tokio::test]
async fn decoded_schema_charge_uses_the_same_startup_metadata_budget() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    let catalog_read: Arc<dyn CatalogRead> = catalog.clone();
    let verifier = StartupCatalogFileVerifier::new(Arc::clone(&ownership), catalog_read);
    let descriptor_only_charge = verifier
        .verify_with_bootstrap()
        .await
        .unwrap()
        .runtime_metadata_bytes;
    drop(verifier);
    Arc::try_unwrap(catalog)
        .ok()
        .expect("startup verifier released its Catalog reference")
        .close()
        .unwrap();

    let config = NativeStorageRuntimeConfig::new(&ownership).with_catalog_file_verification_config(
        StartupCatalogFileVerifierConfig::new()
            .with_max_runtime_metadata_bytes(descriptor_only_charge),
    );
    let error = match NativeStorageRuntime::start(config) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("decoded schema unexpectedly bypassed the startup metadata budget");
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert_eq!(error.table_id(), Some(table_id()));
    assert!(error
        .message()
        .contains("decoded schemas exceed byte budget"));
}

#[test]
fn corrupt_catalog_fails_closed_after_supervisor_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    fs::write(root.join("catalog/catalog.db"), b"not a sqlite database").unwrap();

    let error = match NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("corrupt Catalog unexpectedly became ready");
        }
        Err(error) => error,
    };

    assert!(matches!(
        error.code(),
        ErrorCode::DataCorruption | ErrorCode::Unavailable | ErrorCode::Internal
    ));
    assert!(error.to_string().to_ascii_lowercase().contains("catalog"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_replays_retained_intent_before_publishing_final_reconciliation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        fake_persistence_catalog::with_existing_sqlite(
            Arc::new(TerminalCatalog) as Arc<dyn CatalogWrite>,
            Arc::clone(&catalog),
        ),
        server_actor(),
    )
    .await
    .unwrap();
    let controller = PersistController::start(
        PersistControllerConfig::new(),
        ParquetCandidateWriter::new(Arc::clone(&ownership)),
        coordinator,
    )
    .await
    .unwrap();
    let error = controller
        .handle()
        .submit(test_persist_request(10, 20, 0, 100, 1))
        .unwrap()
        .wait()
        .await
        .expect_err("fatal Catalog failure must retain the durable intent");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(controller.drain().await.is_err());
    assert_eq!(
        fs::read_dir(root.join("wal/catalog-commit-intents"))
            .unwrap()
            .count(),
        1
    );

    let catalog = Arc::try_unwrap(catalog)
        .ok()
        .expect("controller released its Catalog reference");
    catalog.close().unwrap();

    let mut runtime = NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership))
        .expect("startup must replay the retained intent");
    let report = runtime.reconciliation_report();
    assert_eq!(report.catalog_revision, 3);
    assert_eq!(report.files.len(), 1);
    assert!(report.has_no_observed_backlog());
    assert_eq!(runtime.table_runtime_count(), 1);
    assert_eq!(runtime.table_runtime_catalog_revision(), 3);
    let actor = runtime.table_actor(table_id()).unwrap();
    let actor_snapshot = actor.snapshot().await.unwrap();
    assert_eq!(actor_snapshot.schema_version, 1);
    assert_eq!(actor_snapshot.persisted_snapshot_version, 2);
    assert_eq!(
        actor_snapshot.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    assert_eq!(
        fs::read_dir(root.join("wal/catalog-commit-intents"))
            .unwrap()
            .count(),
        0
    );

    runtime.begin_shutdown();
    assert_eq!(
        actor.snapshot().await.unwrap_err().code(),
        ErrorCode::Unavailable
    );
    assert!(runtime.shutdown(Duration::from_secs(2)).is_complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_replay_gate_rejects_partially_represented_multi_file_intent() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        fake_persistence_catalog::with_existing_sqlite(
            Arc::new(TerminalCatalog) as Arc<dyn CatalogWrite>,
            Arc::clone(&catalog),
        ),
        server_actor(),
    )
    .await
    .unwrap();
    let controller = PersistController::start(
        PersistControllerConfig::new(),
        ParquetCandidateWriter::new(Arc::clone(&ownership)),
        coordinator,
    )
    .await
    .unwrap();
    let request = PersistRequest::new(
        request_id(10),
        table_id(),
        1,
        0,
        vec![
            test_persist_input(20, 0, 100),
            test_persist_input(21, 1, 101),
        ],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .unwrap();
    assert_eq!(
        controller
            .handle()
            .submit(request)
            .unwrap()
            .wait()
            .await
            .unwrap_err()
            .code(),
        ErrorCode::DataCorruption
    );
    assert!(controller.drain().await.is_err());

    let report = StartupOrphanReconciler::new(
        Arc::clone(&ownership),
        Arc::clone(&catalog) as Arc<dyn zippy_types::catalog::CatalogFileRead>,
    )
    .reconcile()
    .await
    .unwrap();
    let first_pending = report
        .files
        .iter()
        .find_map(|file| {
            (file
                .verified_candidate
                .as_ref()
                .map(|candidate| candidate.file_id)
                == Some(FileId::from_u128(20).unwrap()))
            .then(|| file.verified_candidate.clone().unwrap())
        })
        .expect("retained first candidate is physically verified");

    catalog
        .command(catalog_command(
            20,
            Some(2),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(),
                expected_snapshot_version: 1,
                files: vec![first_pending],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
            }),
        ))
        .await
        .expect("commit only one member outside the retained atomic intent");
    let catalog = Arc::try_unwrap(catalog)
        .ok()
        .expect("controller released its Catalog reference");
    catalog.close().unwrap();

    let error = match NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("partially represented multi-file intent unexpectedly became ready");
        }
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(error.message().contains("partially represented"));
    assert_eq!(
        fs::read_dir(root.join("wal/catalog-commit-intents"))
            .unwrap()
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_visible_missing_parquet_prevents_native_storage_readiness() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("database");
    let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(&root).unwrap());
    let candidate_path = root.join(format!(
        "parquet/{}/{}.parquet",
        table_id(),
        FileId::from_u128(20).unwrap()
    ));

    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        Arc::clone(&catalog) as Arc<dyn zippy_types::catalog::CatalogPersistence>,
        server_actor(),
    )
    .await
    .unwrap();
    let controller = PersistController::start(
        PersistControllerConfig::new(),
        ParquetCandidateWriter::new(Arc::clone(&ownership)),
        coordinator,
    )
    .await
    .unwrap();
    controller
        .handle()
        .submit(test_persist_request(10, 20, 0, 100, 1))
        .unwrap()
        .wait()
        .await
        .unwrap();
    controller.drain().await.unwrap();
    let catalog = Arc::try_unwrap(catalog)
        .ok()
        .expect("coordinator released its Catalog reference");
    catalog.close().unwrap();
    fs::remove_file(candidate_path).unwrap();

    let error = match NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)) {
        Ok(mut runtime) => {
            let _ = runtime.shutdown(Duration::from_secs(1));
            panic!("missing Catalog-visible Parquet unexpectedly became ready");
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(
        error
            .message()
            .contains("catalog-visible candidate file is missing or unsafe"),
        "unexpected startup error: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_frames_keep_boundaries_through_wal_recovery_and_manifest_checkpoint() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        DurableWriteTarget, IncomingWriteRequest, IngestService, MutationFingerprintComputer,
        OpenWriterRequest, TableMutation,
    };
    use zippy_types::table::{ColumnName, FrameTableDefinition};
    use zippy_types::{ConnectionId, ProducerId};

    let frame_schema = || {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            true,
        )]))
    };
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let start = || {
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let ingest = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new()
                    .with_row_capacity(3)
                    .with_max_live_segments(2),
            )
            .unwrap();
        (runtime, ingest)
    };
    let (mut runtime, ingest) = start();
    let admin = runtime.table_admin().unwrap();
    admin
        .create_namespace(
            request_id(1),
            CreateNamespaceCommand {
                namespace_id: namespace_id(),
                name: CatalogName::new("market").unwrap(),
            },
        )
        .await
        .unwrap();
    admin
        .create_table(
            request_id(2),
            CreateTableCommand {
                table_id: table_id(),
                namespace_id: namespace_id(),
                name: CatalogName::new("frames").unwrap(),
                definition: TableDefinition::Frame(FrameTableDefinition {
                    by: ColumnName::new("price").unwrap(),
                }),
                schema_ipc: schema_ipc(frame_schema().as_ref()),
                config: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    let open = OpenWriterRequest {
        request_id: request_id(10),
        table_id: table_id(),
        producer_id: ProducerId::from_u128(1).unwrap(),
        connection_id: ConnectionId::from_u128(1).unwrap(),
        expected_writer_epoch: Some(0),
    };
    let frame = |sequence: u64, values: Vec<Option<i64>>| {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(100 + u128::from(sequence)),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Frame(
                    RecordBatch::try_new(frame_schema(), vec![Arc::new(Int64Array::from(values))])
                        .unwrap(),
                ),
            })
            .unwrap()
    };
    let mut writer = ingest.open_writer(open.clone()).await.unwrap();
    let too_large = tokio::task::block_in_place(|| writer.write_memory(frame(0, vec![Some(9); 4])))
        .unwrap_err();
    assert_eq!(too_large.code(), ErrorCode::ResourceExhausted);
    assert!(!too_large.retryable());
    assert_eq!(
        runtime.native_ingest_status().unwrap().written_rows_total,
        0
    );
    for (sequence, values) in [vec![Some(1), None], vec![Some(3)]].into_iter().enumerate() {
        let ack = tokio::task::block_in_place(|| {
            writer.submit_durable(frame(sequence as u64, values), DurableWriteTarget::Wal)
        })
        .unwrap();
        ack.wait().await.unwrap();
    }
    let window = prepare_event_window(&runtime, 7101, Some(0)).await;
    assert_eq!(
        consume_event_window(&runtime, window).await,
        vec![
            (0, Some((0, 1)), Some(1), None),
            (1, Some((0, 1)), None, None),
            (2, Some((2, 2)), Some(3), None),
        ]
    );
    let full =
        tokio::task::block_in_place(|| writer.write_memory(frame(2, vec![Some(4)]))).unwrap_err();
    assert_eq!(full.code(), ErrorCode::ResourceExhausted);
    assert!(full.retryable());
    drop(writer);
    drop(admin);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);

    let (mut runtime, ingest) = start();
    let snapshot = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.live_segments.len(), 2);
    let mut recovered = Vec::new();
    for segment in snapshot.live_segments.iter() {
        assert_eq!(
            segment.role(),
            zippy_ingest::TableActorLiveSegmentRole::RetainedSealed
        );
        let batch = zippy_core::SegmentTableView::from_row_span(segment.span().clone())
            .to_record_batch()
            .unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        recovered.push(values.iter().collect::<Vec<_>>());
    }
    assert_eq!(recovered, [vec![Some(1), None], vec![Some(3)]]);
    let partial_offsets = OffsetRange::single(LogOffset::new(0));
    let partial = PersistCandidateInput::new(
        ParquetCandidateSpec {
            file_id: FileId::from_u128(9002).unwrap(),
            table_id: table_id(),
            schema_version: 1,
            schema: frame_schema(),
            provenance: NativeDataFileProvenance::new(
                partial_offsets,
                SourceSegmentLineages::new(vec![SourceSegmentLineage {
                    source: snapshot.live_segments[0].identity(),
                    offsets: partial_offsets,
                }])
                .unwrap(),
            )
            .unwrap(),
            partition: MetadataMap::new(),
            metadata: MetadataMap::new(),
            created_at_ns: 1,
        },
        vec![
            RecordBatch::try_new(frame_schema(), vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap(),
        ],
    )
    .unwrap();
    let partial_request = PersistRequest::new(
        request_id(9001),
        table_id(),
        snapshot.persisted_snapshot_version,
        snapshot.writer_epoch,
        vec![partial],
        OffsetWatermark::Known(LogOffset::new(0)),
    )
    .unwrap();
    let error = runtime
        .table_actor(table_id())
        .unwrap()
        .persist(partial_request)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(error
        .to_string()
        .contains("complete published frame boundary"));

    let mut resumed = open.clone();
    resumed.connection_id = ConnectionId::from_u128(2).unwrap();
    let mut writer = ingest.open_writer(resumed).await.unwrap();
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(frame(0, vec![Some(1), None]), DurableWriteTarget::Persisted)
    })
    .unwrap();
    let persisted = ack.wait().await.unwrap();
    assert!(persisted.persisted_snapshot_version().is_some());
    // Snapshot holders retain complete old frames while the manifest replaces the live prefix.
    assert_eq!(snapshot.live_segments[0].span().row_count(), 2);
    drop(snapshot);
    ingest.checkpoint().await.unwrap();
    let empty = tokio::task::block_in_place(|| writer.write_memory(frame(2, vec![]))).unwrap();
    assert_eq!(empty.row_count(), 0);
    assert_eq!(
        empty.committed_log_offset(),
        OffsetWatermark::Known(LogOffset::new(2))
    );
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(frame(3, vec![Some(4)]), DurableWriteTarget::Persisted)
    })
    .unwrap();
    ack.wait().await.unwrap();
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);

    let (mut runtime, ingest) = start();
    assert_eq!(
        runtime
            .catalog_file_verification_report()
            .managed_file_count,
        2
    );
    let snapshot = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    assert!(snapshot.live_segments.is_empty());
    assert_eq!(
        snapshot.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(3))
    );
    drop(snapshot);
    let mut resumed = open;
    resumed.connection_id = ConnectionId::from_u128(3).unwrap();
    let mut writer = ingest.open_writer(resumed).await.unwrap();
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(frame(0, vec![Some(1), None]), DurableWriteTarget::Persisted)
    })
    .unwrap();
    assert_eq!(ack.wait().await.unwrap(), persisted);
    let window = prepare_event_window(&runtime, 7102, Some(0)).await;
    assert_eq!(window.0.next_offset(), Some(LogOffset::new(4)));
    assert_eq!(
        consume_event_window(&runtime, window).await,
        vec![
            (0, Some((0, 1)), Some(1), None),
            (1, Some((0, 1)), None, None),
            (2, Some((2, 2)), Some(3), None),
            (3, Some((3, 3)), Some(4), None),
        ]
    );
    let query = runtime.query_engine();
    let owner = query.owner(zippy_types::SessionId::from_u128(700).unwrap());
    let error = query
        .prepare_log_window(
            owner.as_ref(),
            zippy_server::NativeLogRequest {
                query_id: zippy_types::QueryId::from_u128(7103).unwrap(),
                table: zippy_types::table::TableRef::Id {
                    table_id: table_id(),
                },
                start: Some(LogOffset::new(1)),
                deadline_ns: None,
                memory_limit_bytes: zippy_types::ResourceLimit::ServerDefault,
                batch_rows: None,
            },
        )
        .await
        .err()
        .expect("partial frame cursor must fail");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    drop(owner);
    drop(query);
    // Resume the same producer epoch after a persisted prefix. Physical segment numbering
    // must not reuse a prior frame's durable lineage identity in the new process.
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(frame(4, vec![Some(5)]), DurableWriteTarget::Wal)
    })
    .unwrap();
    ack.wait().await.unwrap();
    let expected = vec![
        (0, Some((0, 1)), Some(1), None),
        (1, Some((0, 1)), None, None),
        (2, Some((2, 2)), Some(3), None),
        (3, Some((3, 3)), Some(4), None),
        (4, Some((4, 4)), Some(5), None),
    ];
    let window = prepare_event_window(&runtime, 7104, Some(0)).await;
    assert_eq!(consume_event_window(&runtime, window).await, expected);
    ingest.checkpoint().await.unwrap();
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);
    let (mut runtime, _) = start();
    let window = prepare_event_window(&runtime, 7105, Some(0)).await;
    assert_eq!(consume_event_window(&runtime, window).await, expected);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_key_value_events_recover_versions_and_exact_persisted_receipts() {
    use arrow::array::StringArray;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        DurableWriteTarget, IncomingWriteRequest, IngestService, KeyValueMutation,
        MutationFingerprintComputer, OpenWriterRequest, TableMutation,
    };
    use zippy_types::table::{ColumnName, ColumnSet, KeyValueTableDefinition};
    use zippy_types::{ConnectionId, ProducerId};

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let start = || {
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let handle = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new()
                    .with_row_capacity(2)
                    .with_max_live_segments(8),
            )
            .unwrap();
        (runtime, handle)
    };
    let (mut runtime, ingest) = start();
    let admin = runtime.table_admin().unwrap();
    admin
        .create_namespace(
            request_id(1),
            CreateNamespaceCommand {
                namespace_id: namespace_id(),
                name: CatalogName::new("market").unwrap(),
            },
        )
        .await
        .unwrap();
    let create = CreateTableCommand {
        table_id: table_id(),
        namespace_id: namespace_id(),
        name: CatalogName::new("quotes").unwrap(),
        definition: TableDefinition::KeyValue(
            KeyValueTableDefinition::new(
                ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
            )
            .unwrap(),
        ),
        schema_ipc: schema_ipc(&schema),
        config: BTreeMap::new(),
    };
    let created = admin
        .create_table(request_id(2), create.clone())
        .await
        .unwrap();
    assert_eq!(
        admin.create_table(request_id(2), create).await.unwrap(),
        created
    );
    drop(admin);
    let open = |connection| OpenWriterRequest {
        request_id: request_id(10),
        table_id: table_id(),
        producer_id: ProducerId::from_u128(1).unwrap(),
        connection_id: ConnectionId::from_u128(connection).unwrap(),
        expected_writer_epoch: Some(0),
    };
    let upsert = |keys: Vec<&str>, values: Vec<i64>| {
        KeyValueMutation::Upsert(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(keys)),
                    Arc::new(Int64Array::from(values)),
                ],
            )
            .unwrap(),
        )
    };
    let delete = |keys: Vec<&str>| {
        KeyValueMutation::Delete(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![schema.field(0).clone()])),
                vec![Arc::new(StringArray::from(keys))],
            )
            .unwrap(),
        )
    };
    let request = |sequence, mutation| {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(100 + u128::from(sequence)),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::KeyValue(mutation),
            })
            .unwrap()
    };
    let query = runtime.query_engine();
    let session_id = zippy_types::SessionId::from_u128(21).unwrap();
    let owner = query.owner(session_id);
    let mut before_delete = None;
    let mut writer = ingest.open_writer(open(1)).await.unwrap();
    for (sequence, mutation) in [
        upsert(vec!["a", "b", "a"], vec![1, 2, 3]),
        delete(vec!["b"]),
    ]
    .into_iter()
    .enumerate()
    {
        let ack = tokio::task::block_in_place(|| {
            writer.submit_durable(request(sequence as u64, mutation), DurableWriteTarget::Wal)
        })
        .unwrap();
        ack.wait().await.unwrap();
        if sequence == 0 {
            use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
            use zippy_types::table::TableRef;
            let reference = TableRef::Id {
                table_id: table_id(),
            };
            before_delete = Some(
                query
                    .prepare_key_value_snapshot(
                        owner.as_ref(),
                        QueryRequest {
                            query_id: zippy_types::QueryId::from_u128(20).unwrap(),
                            session_id,
                            input: QueryInput::Sql(
                                zippy_protocol::query::KEY_VALUE_SNAPSHOT_SQL.to_owned(),
                            ),
                            tables: vec![reference.clone()],
                            priority: QueryPriority::Interactive,
                            deadline_ns: None,
                            memory_limit_bytes: zippy_types::ResourceLimit::ServerDefault,
                            spill_limit_bytes: zippy_types::ResourceLimit::Disabled,
                            result_batch_rows: None,
                        },
                        vec![zippy_server::NativeQueryBinding::try_new(
                            zippy_protocol::query::KEY_VALUE_SNAPSHOT_BINDING,
                            reference,
                        )
                        .unwrap()],
                    )
                    .await
                    .unwrap(),
            );
        }
    }
    let prepared = before_delete.unwrap();
    assert_eq!(prepared.schema().metadata()["zippy_kv_seq"], "3");
    assert_eq!(prepared.schema().metadata()["zippy_kv_version"], "1");
    // Delete is already visible to new queries. This earlier ticket must still return b=2.
    let instance = prepared.instance_id();
    let mut retained = Vec::new();
    let mut stream = prepared.into_stream().unwrap();
    let mut rows = BTreeMap::new();
    while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        let result = batch.unwrap();
        let batch = result.record_batch();
        assert_eq!(batch.schema().metadata()["zippy_kv_seq"], "3");
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert(keys.value(row).to_owned(), prices.value(row));
        }
        retained.push(result);
    }
    assert_eq!(
        rows,
        BTreeMap::from([("a".to_owned(), 3), ("b".to_owned(), 2)])
    );
    drop(stream);
    use zippy_types::query::QueryEngine;
    assert!(
        query
            .status(owner.as_ref(), instance)
            .await
            .unwrap()
            .metrics["query.active_memory_bytes"]
            > 0
    );
    drop(retained);
    assert_eq!(
        query
            .status(owner.as_ref(), instance)
            .await
            .unwrap()
            .metrics["query.active_memory_bytes"],
        0
    );
    drop(owner);
    drop(query);
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);

    let (mut runtime, ingest) = start();
    let snapshot = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    let mut versions = Vec::new();
    let mut sequences = Vec::new();
    for segment in snapshot.live_segments.iter() {
        let events = zippy_core::SegmentTableView::from_row_span(segment.span().clone())
            .to_record_batch()
            .unwrap();
        versions.extend_from_slice(
            events
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
        sequences.extend_from_slice(
            events
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(versions, [1, 1, 1, 2]);
    assert_eq!(sequences, [1, 2, 3, 4]);
    let first_segment = &snapshot.live_segments[0];
    let partial_batch = zippy_core::SegmentTableView::from_row_span(first_segment.span().clone())
        .to_record_batch()
        .unwrap()
        .slice(0, 1);
    let offsets = OffsetRange::single(LogOffset::new(0));
    let candidate = PersistCandidateInput::new(
        ParquetCandidateSpec {
            file_id: FileId::from_u128(9001).unwrap(),
            table_id: table_id(),
            schema_version: 1,
            schema: partial_batch.schema(),
            provenance: NativeDataFileProvenance::new(
                offsets,
                SourceSegmentLineages::new(vec![SourceSegmentLineage {
                    source: first_segment.identity(),
                    offsets,
                }])
                .unwrap(),
            )
            .unwrap(),
            partition: MetadataMap::new(),
            metadata: MetadataMap::from([(
                zippy_types::key_value::STATE_VERSION_METADATA.to_owned(),
                zippy_types::table::MetadataScalar::UInt64(1),
            )]),
            created_at_ns: 1,
        },
        vec![partial_batch],
    )
    .unwrap();
    let partial = PersistRequest::new(
        request_id(9001),
        table_id(),
        snapshot.persisted_snapshot_version,
        snapshot.writer_epoch,
        vec![candidate],
        OffsetWatermark::Known(LogOffset::new(0)),
    )
    .unwrap();
    let error = runtime
        .table_actor(table_id())
        .unwrap()
        .persist(partial)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(error.to_string().contains("complete mutation boundary"));
    drop(snapshot);
    let mut writer = ingest.open_writer(open(2)).await.unwrap();
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(request(1, delete(vec!["b"])), DurableWriteTarget::Persisted)
    })
    .unwrap();
    let persisted = ack.wait().await.unwrap();
    assert!(persisted.persisted_snapshot_version().is_some());
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    drop(runtime);

    let (mut runtime, ingest) = start();
    assert_eq!(
        runtime
            .catalog_file_verification_report()
            .managed_file_count,
        1
    );
    let mut writer = ingest.open_writer(open(3)).await.unwrap();
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(request(1, delete(vec!["b"])), DurableWriteTarget::Persisted)
    })
    .unwrap();
    assert_eq!(ack.wait().await.unwrap(), persisted);
    tokio::task::block_in_place(|| {
        writer.write_memory(request(2, upsert(vec!["c", "a"], vec![4, 5])))
    })
    .unwrap();
    let snapshot = runtime
        .table_actor(table_id())
        .unwrap()
        .snapshot()
        .await
        .unwrap();
    let events =
        zippy_core::SegmentTableView::from_row_span(snapshot.live_segments[0].span().clone())
            .to_record_batch()
            .unwrap();
    assert_eq!(
        events
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3, 3]
    );
    assert_eq!(
        events
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[5, 6]
    );
    drop(snapshot);
    let window = prepare_event_window(&runtime, 7201, Some(0)).await;
    assert_eq!(window.0.next_offset(), Some(LogOffset::new(6)));
    // The old window pins both persisted and live events while checkpoint replaces its tail.
    ingest.checkpoint().await.unwrap();
    let events = consume_event_window(&runtime, window).await;
    assert_eq!(
        events.iter().map(|row| row.0).collect::<Vec<_>>(),
        [0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        events
            .iter()
            .map(|row| row.3.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["upsert", "upsert", "upsert", "delete", "upsert", "upsert"]
    );
    assert_eq!(
        events.iter().take(3).map(|row| row.2).collect::<Vec<_>>(),
        [Some(1), Some(2), Some(3)]
    );
    let window = prepare_event_window(&runtime, 7202, Some(3)).await;
    assert_eq!(consume_event_window(&runtime, window).await, events[3..]);
    drop(writer);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

type EventRow = (u64, Option<(u64, u64)>, Option<i64>, Option<String>);

async fn prepare_event_window(
    runtime: &NativeStorageRuntime,
    query_number: u128,
    start: Option<u64>,
) -> (
    zippy_server::NativeLogWindow,
    zippy_types::query::OpaqueQueryOwnerCapability,
) {
    let engine = runtime.query_engine();
    let owner = engine.owner(zippy_types::SessionId::from_u128(700).unwrap());
    let window = engine
        .prepare_log_window(
            owner.as_ref(),
            zippy_server::NativeLogRequest {
                query_id: zippy_types::QueryId::from_u128(query_number).unwrap(),
                table: zippy_types::table::TableRef::Id {
                    table_id: table_id(),
                },
                start: start.map(LogOffset::new),
                deadline_ns: None,
                memory_limit_bytes: zippy_types::ResourceLimit::ServerDefault,
                batch_rows: std::num::NonZeroU32::new(1),
            },
        )
        .await
        .unwrap();
    (window, owner)
}

async fn consume_event_window(
    runtime: &NativeStorageRuntime,
    (window, owner): (
        zippy_server::NativeLogWindow,
        zippy_types::query::OpaqueQueryOwnerCapability,
    ),
) -> Vec<EventRow> {
    use arrow::array::StringArray;
    use futures::StreamExt;
    use zippy_types::query::QueryEngine;
    let engine = runtime.query_engine();
    let instance = window.instance_id();
    let mut stream = window.into_stream();
    let mut rows = Vec::new();
    let mut retained = Vec::new();
    while let Some(result) = stream.next().await {
        let result = result.unwrap();
        let batch = result.batch.record_batch();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(result.offsets.first(), result.offsets.last());
        let offset = result.offsets.first().get();
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .next()
            .unwrap();
        let operation = batch
            .column_by_name(zippy_types::key_value::OPERATION)
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_owned()
            });
        if operation.is_some() {
            assert_eq!(
                batch
                    .column_by_name(zippy_types::key_value::SEQUENCE)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0) as u64,
                offset + 1
            );
        }
        rows.push((
            offset,
            result
                .frame
                .map(|frame| (frame.first().get(), frame.last().get())),
            price,
            operation,
        ));
        retained.push(result);
    }
    drop(stream);
    if !retained.is_empty() {
        assert!(
            engine
                .status(owner.as_ref(), instance)
                .await
                .unwrap()
                .metrics["query.active_memory_bytes"]
                > 0
        );
    }
    drop(retained);
    assert_eq!(
        engine
            .status(owner.as_ref(), instance)
            .await
            .unwrap()
            .metrics["query.active_memory_bytes"],
        0
    );
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_log_windows_preserve_writer_handoffs_and_cancel_without_leaking_results() {
    use futures::StreamExt;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, MutationFingerprintComputer, OpenWriterRequest,
        TableMutation,
    };
    use zippy_types::query::{QueryEngine, QueryInput, QueryPriority, QueryRequest, QueryState};
    use zippy_types::table::TableRef;
    use zippy_types::{ConnectionId, ProducerId, QueryId, ResourceLimit, SessionId};
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
    catalog.close().unwrap();
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let ingest = runtime
        .start_native_ingest_with_config(
            temp.path().join("wal"),
            NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
            NativeTableMaterializerConfig::new().with_row_capacity(2),
        )
        .unwrap();
    let live_engine = runtime.query_engine();
    let live_owner = live_engine.owner(SessionId::from_u128(8800).unwrap());
    let mut live = None;
    let mut live_id = None;
    for epoch in 1..=2 {
        let mut writer = ingest
            .open_writer(OpenWriterRequest {
                request_id: request_id(8100 + epoch),
                table_id: table_id(),
                producer_id: ProducerId::from_u128(epoch).unwrap(),
                connection_id: ConnectionId::from_u128(epoch).unwrap(),
                expected_writer_epoch: Some(epoch as u64 - 1),
            })
            .await
            .unwrap();
        let request = ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(8200 + epoch),
                schema_version: 1,
                batch_sequence: 0,
                mutation: TableMutation::Append(
                    RecordBatch::try_new(
                        test_schema(),
                        vec![Arc::new(Int64Array::from(vec![10 * epoch as i64]))],
                    )
                    .unwrap(),
                ),
            })
            .unwrap();
        tokio::task::block_in_place(|| writer.write_memory(request)).unwrap();
        if epoch == 1 {
            let subscription = live_engine
                .subscribe_log(
                    live_owner.as_ref(),
                    zippy_server::NativeLogRequest {
                        query_id: QueryId::from_u128(8800).unwrap(),
                        table: TableRef::Id {
                            table_id: table_id(),
                        },
                        start: Some(LogOffset::new(0)),
                        deadline_ns: None,
                        memory_limit_bytes: ResourceLimit::ServerDefault,
                        batch_rows: std::num::NonZeroU32::new(1),
                    },
                )
                .await
                .unwrap();
            live_id = Some(subscription.instance_id());
            live = Some(subscription.into_stream());
        }
        let event = tokio::time::timeout(Duration::from_secs(5), live.as_mut().unwrap().next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            event.offsets,
            OffsetRange::single(LogOffset::new(epoch as u64 - 1))
        );
        assert_eq!(
            event
                .batch
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10 * epoch as i64
        );
        assert_eq!(
            live_engine
                .status(live_owner.as_ref(), live_id.unwrap())
                .await
                .unwrap()
                .state,
            QueryState::Running
        );
    }
    drop(live);
    assert_eq!(
        live_engine
            .status(live_owner.as_ref(), live_id.unwrap())
            .await
            .unwrap()
            .metrics["query.active_memory_bytes"],
        0
    );
    drop((live_owner, live_engine));
    let window = prepare_event_window(&runtime, 8001, Some(0)).await;
    assert_eq!(
        consume_event_window(&runtime, window).await,
        vec![(0, None, Some(10), None), (1, None, Some(20), None)]
    );
    // Ordinary analytical scans must honor the same historical source epoch contract.
    let engine = runtime.query_engine();
    let session = SessionId::from_u128(800).unwrap();
    let owner = engine.owner(session);
    let table = TableRef::Id {
        table_id: table_id(),
    };
    let query = engine
        .prepare_bound(
            owner.as_ref(),
            QueryRequest {
                query_id: QueryId::from_u128(8002).unwrap(),
                session_id: session,
                input: QueryInput::Sql("SELECT price FROM events ORDER BY price".to_owned()),
                tables: vec![table.clone()],
                priority: QueryPriority::Normal,
                deadline_ns: None,
                memory_limit_bytes: ResourceLimit::ServerDefault,
                spill_limit_bytes: ResourceLimit::Disabled,
                result_batch_rows: None,
            },
            vec![zippy_server::NativeQueryBinding::try_new("events", table).unwrap()],
        )
        .await
        .unwrap();
    let mut stream = query.into_stream().unwrap();
    let mut prices = Vec::new();
    while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        prices.extend_from_slice(
            batch
                .unwrap()
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(prices, [10, 20]);
    drop(stream);

    let (window, owner) = prepare_event_window(&runtime, 8003, Some(0)).await;
    let instance = window.instance_id();
    let mut stream = window.into_stream();
    let retained = stream.next().await.unwrap().unwrap();
    engine.cancel(owner.as_ref(), instance).await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().err().unwrap().code(),
        ErrorCode::Cancelled
    );
    assert!(stream.next().await.is_none());
    assert!(
        engine
            .status(owner.as_ref(), instance)
            .await
            .unwrap()
            .metrics["query.active_memory_bytes"]
            > 0
    );
    drop(retained);
    drop(stream);
    let status = engine.status(owner.as_ref(), instance).await.unwrap();
    assert_eq!(status.state, QueryState::Cancelled);
    assert_eq!(status.metrics["query.active_memory_bytes"], 0);
    let (window, owner) = prepare_event_window(&runtime, 8004, Some(0)).await;
    let instance = window.instance_id();
    drop(window);
    let status = engine.status(owner.as_ref(), instance).await.unwrap();
    assert_eq!(status.state, QueryState::Cancelled);
    assert_eq!(status.metrics["query.active_memory_bytes"], 0);
    drop(owner);
    drop(engine);
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_live_logs_follow_all_table_kinds_across_checkpoints_with_one_identity() {
    use arrow::array::StringArray;
    use futures::StreamExt;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, KeyValueMutation, MutationFingerprintComputer,
        OpenWriterRequest, TableMutation,
    };
    use zippy_types::query::{QueryEngine, QueryState};
    use zippy_types::table::{
        ColumnName, ColumnSet, FrameTableDefinition, KeyValueTableDefinition, TableKind, TableRef,
    };
    use zippy_types::{ConnectionId, ProducerId, QueryId, ResourceLimit, SessionId};

    for kind in [TableKind::Append, TableKind::Frame, TableKind::KeyValue] {
        let temp = tempfile::tempdir().unwrap();
        let ownership =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let ingest = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new()
                    .with_row_capacity(3)
                    .with_max_live_segments(32),
            )
            .unwrap();
        let admin = runtime.table_admin().unwrap();
        admin
            .create_namespace(
                request_id(1),
                CreateNamespaceCommand {
                    namespace_id: namespace_id(),
                    name: CatalogName::new("market").unwrap(),
                },
            )
            .await
            .unwrap();
        let schema = if kind == TableKind::KeyValue {
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("price", DataType::Int64, false),
            ]))
        } else {
            test_schema()
        };
        let definition = match kind {
            TableKind::Append => TableDefinition::Append(AppendTableDefinition::default()),
            TableKind::Frame => TableDefinition::Frame(FrameTableDefinition {
                by: ColumnName::new("price").unwrap(),
            }),
            TableKind::KeyValue => TableDefinition::KeyValue(
                KeyValueTableDefinition::new(
                    ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
                )
                .unwrap(),
            ),
        };
        admin
            .create_table(
                request_id(2),
                CreateTableCommand {
                    table_id: table_id(),
                    namespace_id: namespace_id(),
                    name: CatalogName::new("events").unwrap(),
                    definition,
                    schema_ipc: schema_ipc(&schema),
                    config: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        drop(admin);
        let mut writer = ingest
            .open_writer(OpenWriterRequest {
                request_id: request_id(3),
                table_id: table_id(),
                producer_id: ProducerId::from_u128(1).unwrap(),
                connection_id: ConnectionId::from_u128(1).unwrap(),
                expected_writer_epoch: Some(0),
            })
            .await
            .unwrap();
        let write = |writer: &mut zippy_types::ingest::OpaqueIngestWriter, sequence: u64| {
            let prices = Arc::new(Int64Array::from(vec![
                2 * sequence as i64,
                2 * sequence as i64 + 1,
            ]));
            let batch = if kind == TableKind::KeyValue {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec!["same", "same"])), prices],
                )
                .unwrap()
            } else {
                RecordBatch::try_new(schema.clone(), vec![prices]).unwrap()
            };
            let mutation = match kind {
                TableKind::Append => TableMutation::Append(batch),
                TableKind::Frame => TableMutation::Frame(batch),
                TableKind::KeyValue => TableMutation::KeyValue(KeyValueMutation::Upsert(batch)),
            };
            let request = ArrowIpcMutationFingerprintComputer
                .prepare(IncomingWriteRequest {
                    request_id: request_id(100 + sequence as u128),
                    schema_version: 1,
                    batch_sequence: sequence,
                    mutation,
                })
                .unwrap();
            tokio::task::block_in_place(|| writer.write_memory(request)).unwrap();
        };
        write(&mut writer, 0);
        let engine = runtime.query_engine();
        let owner = engine.owner(SessionId::from_u128(900).unwrap());
        let request = |id, start: Option<u64>| zippy_server::NativeLogRequest {
            query_id: QueryId::from_u128(id).unwrap(),
            table: TableRef::Id {
                table_id: table_id(),
            },
            start: start.map(LogOffset::new),
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            batch_rows: std::num::NonZeroU32::new(1),
        };
        let replay = engine
            .subscribe_log(owner.as_ref(), request(9001, Some(0)))
            .await
            .unwrap();
        let replay_id = replay.instance_id();
        assert_eq!(replay.kind(), kind);
        assert_eq!(
            replay.watermark(),
            OffsetWatermark::Known(LogOffset::new(1))
        );
        let latest = engine
            .subscribe_log(owner.as_ref(), request(9002, None))
            .await
            .unwrap();
        let latest_id = latest.instance_id();
        let slow = engine
            .subscribe_log(owner.as_ref(), request(9006, None))
            .await
            .unwrap();
        let slow_id = slow.instance_id();
        let mut replay = replay.into_stream();
        let mut latest = latest.into_stream();
        let mut slow = slow.into_stream();
        let assert_event = |event: &zippy_server::NativeLogBatch, offset: u64| {
            assert_eq!(event.offsets, OffsetRange::single(LogOffset::new(offset)));
            assert_eq!(
                event
                    .batch
                    .record_batch()
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                offset as i64
            );
            if kind == TableKind::Frame {
                assert_eq!(
                    event.frame,
                    Some(
                        OffsetRange::new(
                            LogOffset::new(offset / 2 * 2),
                            LogOffset::new(offset / 2 * 2 + 1),
                        )
                        .unwrap()
                    )
                );
            }
            if kind == TableKind::KeyValue {
                assert_eq!(
                    event
                        .batch
                        .record_batch()
                        .column_by_name(zippy_types::key_value::SEQUENCE)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    offset as i64 + 1
                );
            }
        };
        // The network transport reads the same native log with one stable identity.
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::{FlightData, FlightDescriptor};
        use futures::SinkExt;
        use prost::Message;
        use zippy_protocol::wire::{self, log_metadata};
        let frontend = zippy_server::NativeFlightQueryFrontend::new(
            engine.clone(),
            runtime.catalog_read_handle(),
        );
        let service = zippy_protocol::server::ZippyFlightService::try_new(
            zippy_protocol::server::FlightServiceConfig::try_new(
                "native-log-test",
                64 * 1024,
                16,
                256 * 1024,
            )
            .unwrap()
            .with_local_read_sessions(Duration::from_secs(60), 128)
            .unwrap(),
            Arc::new(frontend),
        )
        .unwrap();
        let mut flight = zippy_protocol::runtime::FlightServerRuntime::start(
            "127.0.0.1:0".parse().unwrap(),
            service,
            Duration::from_secs(5),
        )
        .unwrap();
        let mut client = FlightServiceClient::connect(format!("http://{}", flight.endpoint()))
            .await
            .unwrap();
        let payload = client
            .handshake(futures::stream::iter(vec![
                arrow_flight::HandshakeRequest {
                    protocol_version: 0,
                    payload: wire::SessionOpenRequest {
                        protocol_version: 1,
                    }
                    .encode_to_vec()
                    .into(),
                },
            ]))
            .await
            .unwrap()
            .into_inner()
            .message()
            .await
            .unwrap()
            .unwrap();
        let opened = wire::SessionOpenResponse::decode(payload.payload).unwrap();
        let session_token = format!("Bearer {}", opened.bearer_token);
        let (mut control, requests) = futures::channel::mpsc::channel(2);
        let command = wire::LogCommand {
            consumer: None,
            protocol_version: 1,
            query_id: QueryId::from_u128(9004)
                .unwrap()
                .into_bytes()
                .to_vec()
                .into(),
            session_id: opened.session_id.clone(),
            table: Some(wire::TableBinding {
                sql_name: String::new(),
                table: Some(wire::table_binding::Table::TableId(
                    table_id().into_bytes().to_vec().into(),
                )),
            }),
            start: Some(wire::log_command::Start::Offset(0)),
            deadline_ns: None,
            memory_limit: Some(wire::ResourceLimit {
                kind: wire::resource_limit::Kind::ServerDefault as i32,
                bytes: 0,
            }),
            batch_rows: Some(1),
        };
        for wrong_session in [None, Some(SessionId::from_u128(901).unwrap())] {
            let mut rejected = tonic::Request::new(futures::stream::iter(vec![FlightData {
                flight_descriptor: Some(FlightDescriptor::new_cmd(command.encode_to_vec())),
                ..FlightData::default()
            }]));
            if let Some(id) = wrong_session {
                rejected
                    .metadata_mut()
                    .insert("x-zippy-session-id", id.to_string().parse().unwrap());
            }
            let error = client.do_exchange(rejected).await.unwrap_err();
            assert_eq!(
                zippy_protocol::error::status_to_server_error(&error).code(),
                ErrorCode::Unauthenticated
            );
        }
        control
            .send(FlightData {
                flight_descriptor: Some(FlightDescriptor::new_cmd(command.encode_to_vec())),
                ..FlightData::default()
            })
            .await
            .unwrap();
        let mut rpc = tonic::Request::new(requests);
        rpc.metadata_mut().insert(
            zippy_protocol::AUTHORIZATION_METADATA_KEY,
            session_token.parse().unwrap(),
        );
        let mut remote = client.do_exchange(rpc).await.unwrap().into_inner();
        let opening = remote.message().await.unwrap().unwrap();
        assert!(opening.data_header.is_empty());
        let schema_message = remote.message().await.unwrap().unwrap();
        let remote_schema = Arc::new(Schema::try_from(&schema_message).unwrap());
        let Some(log_metadata::Payload::Opened(opened)) =
            wire::LogMetadata::decode(opening.app_metadata)
                .unwrap()
                .payload
        else {
            panic!("log opening metadata is missing")
        };
        assert_eq!(opened.table_id.as_ref(), &table_id().into_bytes());
        assert_eq!(opened.watermark.unwrap().offset, 1);
        let held = replay.next().await.unwrap().unwrap();
        assert_eq!(held.offsets, OffsetRange::single(LogOffset::new(0)));
        assert_eq!(
            replay.next().await.unwrap().unwrap().offsets,
            OffsetRange::single(LogOffset::new(1))
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), latest.next())
                .await
                .is_err()
        );
        for sequence in 1..=8 {
            write(&mut writer, sequence);
            if sequence % 2 == 0 {
                ingest.checkpoint().await.unwrap();
            }
            for stream in [&mut replay, &mut latest] {
                for offset in 2 * sequence..2 * sequence + 2 {
                    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_event(&event, offset);
                }
            }
            for instance in [replay_id, latest_id] {
                let status = engine.status(owner.as_ref(), instance).await.unwrap();
                assert_eq!(status.state, QueryState::Running);
            }
            if sequence == 1 {
                // First enter a live-only refresh, then stop polling through four checkpoints.
                for offset in 2..4 {
                    let event = tokio::time::timeout(Duration::from_secs(5), slow.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_event(&event, offset);
                }
            }
        }
        for offset in 0..18 {
            let data = tokio::time::timeout(Duration::from_secs(5), remote.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let batch = arrow_flight::utils::flight_data_to_arrow_batch(
                &data,
                remote_schema.clone(),
                &std::collections::HashMap::new(),
            )
            .unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(
                batch
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                offset as i64
            );
            let Some(log_metadata::Payload::Progress(progress)) =
                wire::LogMetadata::decode(data.app_metadata)
                    .unwrap()
                    .payload
            else {
                panic!("log progress metadata is missing")
            };
            assert_eq!(
                (progress.first_offset, progress.last_offset),
                (offset, offset)
            );
            if kind == TableKind::Frame {
                assert_eq!(progress.frame_first_offset, Some(offset / 2 * 2));
                assert_eq!(progress.frame_last_offset, Some(offset / 2 * 2 + 1));
            }
        }
        control
            .send(FlightData {
                app_metadata: wire::LogControl {
                    protocol_version: 1,
                    control: Some(wire::log_control::Control::Cancel(true)),
                }
                .encode_to_vec()
                .into(),
                ..FlightData::default()
            })
            .await
            .unwrap();
        let cancelled = tokio::time::timeout(Duration::from_secs(5), remote.message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            zippy_protocol::error::status_to_server_error(&cancelled).code(),
            ErrorCode::Cancelled
        );
        drop(remote);
        drop(control);
        // A half-closed request stream still follows writes until its explicit deadline.
        let mut half_closed = command.clone();
        half_closed.query_id = QueryId::from_u128(9005)
            .unwrap()
            .into_bytes()
            .to_vec()
            .into();
        half_closed.start = Some(wire::log_command::Start::Latest(true));
        half_closed.deadline_ns = Some(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                + Duration::from_secs(1))
            .as_nanos() as u64,
        );
        let mut rpc = tonic::Request::new(futures::stream::iter(vec![FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(half_closed.encode_to_vec())),
            ..FlightData::default()
        }]));
        rpc.metadata_mut().insert(
            zippy_protocol::AUTHORIZATION_METADATA_KEY,
            session_token.parse().unwrap(),
        );
        let mut remote = client.do_exchange(rpc).await.unwrap().into_inner();
        assert!(remote.message().await.unwrap().is_some());
        assert!(remote.message().await.unwrap().is_some());
        let timeout = tokio::time::timeout(Duration::from_secs(5), remote.message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            zippy_protocol::error::status_to_server_error(&timeout).code(),
            ErrorCode::DeadlineExceeded
        );
        drop(remote);
        drop(client);
        flight.shutdown(Duration::from_secs(5)).unwrap();
        let mut expires = request(9003, None);
        expires.deadline_ns = Some(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                + Duration::from_secs(1))
            .as_nanos() as u64,
        );
        let timed = engine.subscribe_log(owner.as_ref(), expires).await.unwrap();
        let timed_id = timed.instance_id();
        let mut timed = timed.into_stream();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), timed.next())
                .await
                .unwrap()
                .unwrap()
                .err()
                .unwrap()
                .code(),
            ErrorCode::DeadlineExceeded
        );
        drop(timed);
        let status = engine.status(owner.as_ref(), timed_id).await.unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(status.metrics["query.active_memory_bytes"], 0);
        // No new row appears merely because the durable boundary advances.
        ingest.checkpoint().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), replay.next())
                .await
                .is_err()
        );
        engine.cancel(owner.as_ref(), replay_id).await.unwrap();
        assert_eq!(
            replay.next().await.unwrap().err().unwrap().code(),
            ErrorCode::Cancelled
        );
        drop(replay);
        assert!(
            engine
                .status(owner.as_ref(), replay_id)
                .await
                .unwrap()
                .metrics["query.active_memory_bytes"]
                > 0
        );
        drop(held);
        assert_eq!(
            engine
                .status(owner.as_ref(), replay_id)
                .await
                .unwrap()
                .metrics["query.active_memory_bytes"],
            0
        );
        let snapshot = runtime
            .table_actor(table_id())
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        assert_eq!(
            snapshot.committed_log_offset,
            OffsetWatermark::Known(LogOffset::new(17))
        );
        drop(snapshot);
        write(&mut writer, 9);
        // Cursor 4 is now persisted: the same reader must fall back to a complete window,
        // consume the durable prefix, then deliver the not-yet-checkpointed live suffix.
        for offset in 4..20 {
            let event = tokio::time::timeout(Duration::from_secs(5), slow.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_event(&event, offset);
        }
        write(&mut writer, 10);
        // Having caught up, cursor 20 returns to the live-only path without a new identity.
        for offset in 20..22 {
            let event = tokio::time::timeout(Duration::from_secs(5), slow.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_event(&event, offset);
        }
        assert_eq!(
            engine.status(owner.as_ref(), slow_id).await.unwrap().state,
            QueryState::Running
        );
        engine.cancel(owner.as_ref(), slow_id).await.unwrap();
        assert_eq!(
            slow.next().await.unwrap().err().unwrap().code(),
            ErrorCode::Cancelled
        );
        drop(slow);
        assert_eq!(
            engine
                .status(owner.as_ref(), slow_id)
                .await
                .unwrap()
                .metrics["query.active_memory_bytes"],
            0
        );
        drop(writer);
        runtime.begin_shutdown();
        assert!(tokio::time::timeout(Duration::from_secs(5), latest.next())
            .await
            .unwrap()
            .unwrap()
            .is_err());
        drop(latest);
        drop(owner);
        drop(engine);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_consumers_replay_atomic_units_and_ack_memory_writes_before_restart() {
    use arrow::array::StringArray;
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::catalog::{CatalogConsumerRead, ReadConsumerOffsetRequest};
    use zippy_types::ingest::{
        IncomingWriteRequest, IngestService, KeyValueMutation, MutationFingerprintComputer,
        OpenWriterRequest, TableMutation,
    };
    use zippy_types::query::QueryEngine;
    use zippy_types::table::{
        ColumnName, ColumnSet, FrameTableDefinition, KeyValueTableDefinition, TableKind, TableRef,
    };
    use zippy_types::{
        CommittedOffset, ConnectionId, ConsumerGroupId, ConsumerOffsetAuthority, DurableConsumerId,
        ProducerId, QueryId, ResourceLimit, SessionId,
    };
    for kind in [TableKind::Append, TableKind::Frame, TableKind::KeyValue] {
        let temp = tempfile::tempdir().unwrap();
        let ownership =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        let ingest = runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                NativeTableMaterializerConfig::new().with_row_capacity(8),
            )
            .unwrap();
        let admin = runtime.table_admin().unwrap();
        admin
            .create_namespace(
                request_id(1),
                CreateNamespaceCommand {
                    namespace_id: namespace_id(),
                    name: CatalogName::new("market").unwrap(),
                },
            )
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
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
        admin
            .create_table(
                request_id(2),
                CreateTableCommand {
                    table_id: table_id(),
                    namespace_id: namespace_id(),
                    name: CatalogName::new("events").unwrap(),
                    definition,
                    schema_ipc: schema_ipc(&schema),
                    config: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        let mut writer = ingest
            .open_writer(OpenWriterRequest {
                request_id: request_id(3),
                table_id: table_id(),
                producer_id: ProducerId::from_u128(1).unwrap(),
                connection_id: ConnectionId::from_u128(1).unwrap(),
                expected_writer_epoch: Some(0),
            })
            .await
            .unwrap();
        for sequence in 0..2 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["a", "b"])),
                    Arc::new(Int64Array::from(vec![sequence as i64, sequence as i64 + 1])),
                ],
            )
            .unwrap();
            let mutation = match kind {
                TableKind::Append => TableMutation::Append(batch),
                TableKind::Frame => TableMutation::Frame(batch),
                TableKind::KeyValue => TableMutation::KeyValue(KeyValueMutation::Upsert(batch)),
            };
            let prepared = ArrowIpcMutationFingerprintComputer
                .prepare(IncomingWriteRequest {
                    request_id: request_id(10 + sequence as u128),
                    schema_version: 1,
                    batch_sequence: sequence,
                    mutation,
                })
                .unwrap();
            tokio::task::block_in_place(|| writer.write_memory(prepared)).unwrap();
        }
        let consumer_id = DurableConsumerId::from_u128(1).unwrap();
        let consumer = ReadConsumerOffsetRequest {
            consumer_id,
            table_id: table_id(),
            offset_authority: ConsumerOffsetAuthority::Group(
                ConsumerGroupId::from_u128(1).unwrap(),
            ),
        };
        let request = |id| zippy_server::NativeLogRequest {
            query_id: QueryId::from_u128(id).unwrap(),
            table: TableRef::Id {
                table_id: table_id(),
            },
            start: None,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            batch_rows: std::num::NonZeroU32::new(1),
        };
        let service = runtime.consumer_logs().unwrap();
        let engine = runtime.query_engine();
        let owner = engine.owner(SessionId::from_u128(1).unwrap());
        let mut log = service
            .open(
                owner.as_ref(),
                ConnectionId::from_u128(2).unwrap(),
                consumer,
                request(101),
            )
            .await
            .unwrap();
        assert_eq!(
            runtime
                .catalog_read_handle()
                .read_consumer_offset(consumer)
                .await
                .unwrap()
                .persisted_log_offset,
            CommittedOffset::Empty
        );
        let blocked = service
            .open(
                owner.as_ref(),
                ConnectionId::from_u128(3).unwrap(),
                ReadConsumerOffsetRequest {
                    consumer_id: DurableConsumerId::from_u128(2).unwrap(),
                    ..consumer
                },
                request(102),
            )
            .await;
        assert!(matches!(blocked, Err(error) if error.code() == ErrorCode::Conflict));
        let first = log.next_delivery().await.unwrap();
        let first_end = if kind == TableKind::Append { 0 } else { 1 };
        assert_eq!(
            first.acknowledgement.processed_through,
            CommittedOffset::Known(LogOffset::new(first_end))
        );
        assert_eq!(first.fragments.len(), first_end as usize + 1);
        assert!(first
            .fragments
            .iter()
            .all(|fragment| fragment.batch.has_owner()));
        assert!(
            matches!(log.next_delivery().await, Err(error) if error.code() == ErrorCode::InvalidArgument)
        );
        let mut wrong = first.acknowledgement;
        wrong.delivery_sequence += 1;
        assert_eq!(
            log.acknowledge(request_id(201), wrong)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        wrong = first.acknowledgement;
        wrong.processed_through = CommittedOffset::Empty;
        assert_eq!(
            log.acknowledge(request_id(201), wrong)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        if kind != TableKind::Append {
            wrong.processed_through = CommittedOffset::Known(LogOffset::new(0));
            assert_eq!(
                log.acknowledge(request_id(201), wrong)
                    .await
                    .unwrap_err()
                    .code(),
                ErrorCode::InvalidArgument
            );
        }
        wrong = first.acknowledgement;
        wrong.processed_through = CommittedOffset::Known(LogOffset::new(3));
        assert_eq!(
            log.acknowledge(request_id(201), wrong)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        // Drop without ACK: no durable advancement, and the complete unit reappears.
        drop(log);
        drop(first);
        let mut log = service
            .open(
                owner.as_ref(),
                ConnectionId::from_u128(4).unwrap(),
                consumer,
                request(103),
            )
            .await
            .unwrap();
        let first = log.next_delivery().await.unwrap();
        assert_eq!(first.fragments[0].offsets.first(), LogOffset::new(0));
        log.acknowledge(request_id(202), first.acknowledgement)
            .await
            .unwrap();
        log.acknowledge(request_id(203), first.acknowledgement)
            .await
            .unwrap();
        let durable = runtime
            .catalog_read_handle()
            .read_consumer_offset(consumer)
            .await
            .unwrap();
        assert_eq!(
            durable.committed_log_offset,
            first.acknowledgement.processed_through
        );
        assert_eq!(
            durable.persisted_log_offset,
            CommittedOffset::Known(LogOffset::new(3))
        );
        if kind == TableKind::KeyValue {
            // Catalog validates persistence, not the delivery boundary. A corrupted/trusted
            // internal partial offset must never turn into a partial mutation on replay.
            use zippy_types::catalog::CommitConsumerOffsetCommand;
            let invalid_consumer = DurableConsumerId::from_u128(3).unwrap();
            let invalid_request = ReadConsumerOffsetRequest {
                consumer_id: invalid_consumer,
                table_id: table_id(),
                offset_authority: ConsumerOffsetAuthority::Consumer(invalid_consumer),
            };
            runtime
                .catalog_write_handle()
                .command(
                    CatalogCommandEnvelopeV1::new(
                        request_id(501),
                        None,
                        server_actor(),
                        CatalogCommandV1::CommitConsumerOffset(CommitConsumerOffsetCommand {
                            consumer_id: invalid_consumer,
                            table_id: table_id(),
                            offset_authority: invalid_request.offset_authority,
                            expected_schema_version: 1,
                            expected_committed_log_offset: CommittedOffset::Empty,
                            committed_log_offset: CommittedOffset::Known(LogOffset::new(0)),
                        }),
                    )
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
            let mut invalid_log = service
                .open(
                    owner.as_ref(),
                    ConnectionId::from_u128(8).unwrap(),
                    invalid_request,
                    request(108),
                )
                .await
                .unwrap();
            assert!(matches!(invalid_log.next_delivery().await,
                Err(error) if error.code() == ErrorCode::DataCorruption));
        }
        let second = log.next_delivery().await.unwrap();
        assert_eq!(
            second.fragments[0].offsets.first(),
            LogOffset::new(first_end + 1)
        );
        // Explicit query cancellation prevents a new ACK from advancing the durable offset.
        engine
            .cancel(owner.as_ref(), log.instance_id())
            .await
            .unwrap();
        assert_eq!(
            log.acknowledge(request_id(204), second.acknowledgement)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Cancelled
        );
        drop(second);
        drop(first);
        drop(log);
        drop(writer);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
        drop(runtime);
        drop(service);
        drop(admin);
        drop(ingest);
        let mut runtime =
            NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
        runtime
            .start_native_ingest_with_config(
                temp.path().join("wal"),
                NativeIngestConfig::new(),
                NativeTableMaterializerConfig::new().with_row_capacity(8),
            )
            .unwrap();
        let service = runtime.consumer_logs().unwrap();
        let engine = runtime.query_engine();
        let owner = engine.owner(SessionId::from_u128(2).unwrap());
        let mut log = service
            .open(
                owner.as_ref(),
                ConnectionId::from_u128(5).unwrap(),
                consumer,
                request(105),
            )
            .await
            .unwrap();
        let replayed = log.next_delivery().await.unwrap();
        assert_eq!(
            replayed.fragments[0].offsets.first(),
            LogOffset::new(first_end + 1)
        );
        let instance = log.instance_id();
        let ingest = runtime.native_ingest_handle().unwrap();
        assert!(matches!(ingest.checkpoint_table(table_id(), 2).await,
            Err(error) if error.code() == ErrorCode::SchemaMismatch));
        let gate = ingest.checkpoint_table(table_id(), 1).await.unwrap();
        assert!(matches!(ingest.begin_table_transition(table_id()),
            Err(error) if error.code() == ErrorCode::ResourceExhausted));
        // Keep the table gate closed until the ACK is demonstrably admitted, then cancel
        // only its waiter. The detached commit must retain both admission and membership.
        let mut acknowledge = Box::pin(log.acknowledge(request_id(205), replayed.acknowledgement));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    result = &mut acknowledge => panic!("gate admitted premature ACK: {result:?}"),
                    () = tokio::task::yield_now() => {}
                }
                if runtime.native_ingest_status().unwrap().active_operations >= 2 {
                    break;
                }
            }
        })
        .await
        .unwrap();
        drop(acknowledge);
        assert_eq!(
            log.acknowledge(request_id(206), replayed.acknowledgement)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        drop(log);
        assert!(
            matches!(service.open(owner.as_ref(), ConnectionId::from_u128(6).unwrap(),
            consumer, request(106)).await, Err(error) if error.code() == ErrorCode::Conflict)
        );
        assert!(
            engine
                .status(owner.as_ref(), instance)
                .await
                .unwrap()
                .metrics["query.result_memory_bytes"]
                > 0
        );
        drop(gate);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtime
                    .catalog_read_handle()
                    .read_consumer_offset(consumer)
                    .await
                    .unwrap()
                    .committed_log_offset
                    == replayed.acknowledgement.processed_through
                    && runtime.native_ingest_status().unwrap().active_operations == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(replayed);
        assert_eq!(
            engine
                .status(owner.as_ref(), instance)
                .await
                .unwrap()
                .metrics["query.result_memory_bytes"],
            0
        );
        let reconnected = service
            .open(
                owner.as_ref(),
                ConnectionId::from_u128(7).unwrap(),
                consumer,
                request(107),
            )
            .await
            .unwrap();
        drop(reconnected);
        let draining_checkpoint = ingest.checkpoint_table(table_id(), 1).await.unwrap();
        runtime.begin_shutdown();
        assert!(!runtime.shutdown(Duration::from_millis(20)).is_complete());
        drop(draining_checkpoint);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fixed_native_snapshot_keeps_live_boundary_after_checkpoint_and_release() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeIngestConfig, NativeTableMaterializerConfig,
    };
    use zippy_types::ingest::{
        DurableWriteTarget, IncomingWriteRequest, IngestService, MutationFingerprintComputer,
        OpenWriterRequest, TableMutation,
    };
    use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
    use zippy_types::table::TableRef;
    use zippy_types::{ConnectionId, ProducerId, QueryId, ResourceLimit, SessionId};
    let temp = tempfile::tempdir().unwrap();
    let ownership =
        Arc::new(OwnedDataDirectory::acquire_or_initialize(temp.path().join("db")).unwrap());
    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
    catalog.close().unwrap();
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let ingest = runtime
        .start_native_ingest_with_config(
            temp.path().join("wal"),
            NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
            NativeTableMaterializerConfig::new().with_row_capacity(8),
        )
        .unwrap();

    let extra_id = TableId::from_u128(TABLE_ID + 1).unwrap();
    runtime
        .table_admin()
        .unwrap()
        .create_table(
            request_id(9599),
            CreateTableCommand {
                table_id: extra_id,
                namespace_id: namespace_id(),
                name: CatalogName::new("extra").unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: schema_ipc(&test_schema()),
                config: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    let mut writer = ingest
        .open_writer(OpenWriterRequest {
            request_id: request_id(9600),
            table_id: table_id(),
            producer_id: ProducerId::from_u128(9600).unwrap(),
            connection_id: ConnectionId::from_u128(9600).unwrap(),
            expected_writer_epoch: Some(0),
        })
        .await
        .unwrap();
    let write = |sequence, value| {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(9601 + sequence as u128),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Append(
                    RecordBatch::try_new(
                        test_schema(),
                        vec![Arc::new(Int64Array::from(vec![value]))],
                    )
                    .unwrap(),
                ),
            })
            .unwrap()
    };
    tokio::task::block_in_place(|| writer.write_memory(write(0, 42))).unwrap();
    let engine = runtime.query_engine();
    let session = SessionId::from_u128(9600).unwrap();
    let owner = engine.owner(session);
    let table = TableRef::Id {
        table_id: table_id(),
    };
    let request = |id, session_id, tables| QueryRequest {
        query_id: QueryId::from_u128(id).unwrap(),
        session_id,
        input: QueryInput::Sql("SELECT price FROM frozen ORDER BY price".to_owned()),
        tables,
        priority: QueryPriority::Normal,
        deadline_ns: None,
        memory_limit_bytes: ResourceLimit::ServerDefault,
        spill_limit_bytes: ResourceLimit::Disabled,
        result_batch_rows: None,
    };
    let mut capture = request(
        9610,
        session,
        vec![TableRef::Id { table_id: extra_id }, table.clone()],
    );
    capture.deadline_ns = Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
            + 60_000_000_000,
    );
    let fixed = engine
        .capture_fixed_snapshot(owner.as_ref(), fixed_capture_request(capture))
        .await
        .unwrap();
    let ack = tokio::task::block_in_place(|| {
        writer.submit_durable(write(1, 44), DurableWriteTarget::Persisted)
    })
    .unwrap();
    ack.wait().await.unwrap();
    let bindings =
        || vec![zippy_server::NativeQueryBinding::try_new("frozen", table.clone()).unwrap()];
    let foreign_session = SessionId::from_u128(9700).unwrap();
    let foreign = engine.owner(foreign_session);
    let error = engine
        .prepare_fixed(
            &fixed,
            foreign.as_ref(),
            request(9611, foreign_session, vec![table.clone()]),
            bindings(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), ErrorCode::PermissionDenied);
    for id in [9612, 9613] {
        let prepared = engine
            .prepare_fixed(
                &fixed,
                owner.as_ref(),
                request(id, session, vec![table.clone()]),
                bindings(),
            )
            .await
            .unwrap();
        assert_eq!(collect_fixed_prices(prepared).await, [42]);
    }
    let prepared = engine
        .prepare_fixed(
            &fixed,
            owner.as_ref(),
            request(9614, session, vec![table.clone()]),
            bindings(),
        )
        .await
        .unwrap();
    fixed.close().unwrap();
    assert_eq!(
        runtime.runtime_snapshot_pin_status().unwrap().active_pins,
        2
    );
    assert_eq!(collect_fixed_prices(prepared).await, [42]);
    assert_eq!(
        runtime.runtime_snapshot_pin_status().unwrap().active_pins,
        0
    );
    let current = engine
        .prepare_bound(
            owner.as_ref(),
            request(9615, session, vec![table.clone()]),
            bindings(),
        )
        .await
        .unwrap();
    assert_eq!(collect_fixed_prices(current).await, [42, 44]);

    let mut capture = request(9620, session, vec![table.clone()]);
    capture.deadline_ns = Some(fixed_test_expiry());
    let cancelled = engine
        .capture_fixed_snapshot(owner.as_ref(), fixed_capture_request(capture))
        .await
        .unwrap();
    engine
        .cancel_administrator(cancelled.instance_id())
        .unwrap();
    assert!(cancelled.is_expired());
    let error = engine
        .prepare_fixed(
            &cancelled,
            owner.as_ref(),
            request(9621, session, vec![table.clone()]),
            bindings(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), ErrorCode::Cancelled);
    drop(cancelled);
    assert_eq!(
        runtime.runtime_snapshot_pin_status().unwrap().active_pins,
        0
    );

    let expired = zippy_server::NativeFixedSnapshotRequest {
        query_id: QueryId::from_u128(9622).unwrap(),
        tables: vec![table.clone()],
        expires_at_ns: 1,
        memory_limit_bytes: ResourceLimit::ServerDefault,
    };
    let error = engine
        .capture_fixed_snapshot(owner.as_ref(), expired)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
    assert_eq!(
        runtime.runtime_snapshot_pin_status().unwrap().active_pins,
        0
    );
    drop((writer, owner, foreign, engine));
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

async fn collect_fixed_prices(prepared: zippy_server::NativePreparedQuery) -> Vec<i64> {
    let mut stream = prepared.into_stream().unwrap();
    let mut values = Vec::new();
    while let Some(batch) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        values.extend_from_slice(
            batch
                .unwrap()
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
        );
    }
    values
}

fn fixed_test_expiry() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        + 60_000_000_000
}

fn native_fixed_test_request(
    id: u128,
    session_id: zippy_types::SessionId,
    tables: Vec<zippy_types::table::TableRef>,
    sql: &str,
) -> zippy_types::query::QueryRequest {
    use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
    QueryRequest {
        query_id: zippy_types::QueryId::from_u128(id).unwrap(),
        session_id,
        input: QueryInput::Sql(sql.to_owned()),
        tables,
        priority: QueryPriority::Normal,
        deadline_ns: None,
        memory_limit_bytes: zippy_types::ResourceLimit::ServerDefault,
        spill_limit_bytes: zippy_types::ResourceLimit::Disabled,
        result_batch_rows: None,
    }
}

fn fixed_capture_request(
    request: zippy_types::query::QueryRequest,
) -> zippy_server::NativeFixedSnapshotRequest {
    zippy_server::NativeFixedSnapshotRequest {
        query_id: request.query_id,
        tables: request.tables,
        expires_at_ns: request.deadline_ns.unwrap(),
        memory_limit_bytes: request.memory_limit_bytes,
    }
}
