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
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, CommitDataFilesIntent,
    ParquetCandidateSpec, ParquetCandidateWriter, PersistCandidateInput, PersistControllerConfig,
    PersistRequest, StartupCatalogFileVerifier, StartupCatalogFileVerifierConfig,
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
async fn native_checkpoints_recycle_tiny_budgets_and_recover_exact_receipts() {
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeAppendMaterializerConfig, NativeIngestConfig,
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
    let wal_root = temp.path().join("wal");
    let ingest_config = NativeIngestConfig::new()
        .with_max_receipts_per_epoch(1)
        .with_checkpoint_interval(Duration::from_secs(60));
    let materializer_config = NativeAppendMaterializerConfig::new()
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
    drop(runtime);
    let mut runtime =
        NativeStorageRuntime::start(NativeStorageRuntimeConfig::new(&ownership)).unwrap();
    let handle = runtime
        .start_native_ingest_with_config(&wal_root, ingest_config, materializer_config)
        .unwrap();
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
        ArrowIpcMutationFingerprintComputer, NativeAppendMaterializerConfig, NativeIngestConfig,
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
            NativeAppendMaterializerConfig::new().with_row_capacity(2),
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
    assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
}

/// Opt-in real persistence benchmark; deliberately excluded from deterministic CI timing gates.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "release-mode native ingest persistence benchmark"]
async fn native_checkpoint_throughput_benchmark() {
    use arrow::array::{ArrayRef, StringArray};
    use zippy_ingest::{
        ArrowIpcMutationFingerprintComputer, NativeAppendMaterializerConfig, NativeIngestConfig,
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
            NativeAppendMaterializerConfig::new().with_row_capacity(8192),
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
    let candidate = PersistCandidateInput::new(
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
    .unwrap();
    PersistRequest::new(
        request_id(request),
        table_id(),
        expected_snapshot_version,
        vec![candidate],
        OffsetWatermark::Known(LogOffset::new(offset)),
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
    let candidate = write_test_candidate(&ownership, 20, 0, 100);

    let terminal_catalog = Arc::new(TerminalCatalog);
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        terminal_catalog as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .unwrap();
    let intent = CommitDataFilesIntent::new(
        request_id(10),
        table_id(),
        1,
        vec![candidate],
        OffsetWatermark::Known(LogOffset::new(0)),
    )
    .unwrap();

    let error = coordinator
        .commit(intent)
        .await
        .expect_err("fatal Catalog failure must retain the durable intent");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(coordinator.drain().await.is_err());
    assert_eq!(
        fs::read_dir(root.join("wal/catalog-commit-intents"))
            .unwrap()
            .count(),
        1
    );

    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
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
    let first = write_test_candidate(&ownership, 20, 0, 100);
    let first_pending = first.pending().clone();
    let second = write_test_candidate(&ownership, 21, 1, 101);

    let terminal_catalog = Arc::new(TerminalCatalog);
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        terminal_catalog as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .unwrap();
    let intent = CommitDataFilesIntent::new(
        request_id(10),
        table_id(),
        1,
        vec![first, second],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .unwrap();
    assert_eq!(
        coordinator.commit(intent).await.unwrap_err().code(),
        ErrorCode::DataCorruption
    );
    assert!(coordinator.drain().await.is_err());

    let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
    create_test_table(&catalog).await;
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
    let candidate = write_test_candidate(&ownership, 20, 0, 100);
    let candidate_path = candidate.path().to_path_buf();

    let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap());
    create_test_table(catalog.as_ref()).await;
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&ownership),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .unwrap();
    coordinator
        .commit(
            CommitDataFilesIntent::new(
                request_id(10),
                table_id(),
                1,
                vec![candidate],
                OffsetWatermark::Known(LogOffset::new(0)),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    coordinator.drain().await.unwrap();
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
