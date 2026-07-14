use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tokio::sync::Semaphore;
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, ParquetCandidateSpec,
    ParquetCandidateWriter, PersistCandidateInput, PersistController, PersistControllerConfig,
    PersistRequest,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandV1, CatalogOutcomeV1, CatalogResponseV1,
    CatalogWrite, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    MetadataMap, MetadataScalar, NativeDataFileProvenance, SnapshotOperation,
    SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableSnapshot,
};
use zippy_types::{
    BoxFuture, ErrorCode, FileId, LogOffset, OffsetRange, OffsetWatermark, RequestId, Result,
    TableId,
};

const TABLE: u128 = 10;

fn table_id(value: u128) -> TableId {
    TableId::from_u128(value).expect("test table id must be nonzero")
}

fn file_id(value: u128) -> FileId {
    FileId::from_u128(value).expect("test file id must be nonzero")
}

fn request_id(value: u128) -> RequestId {
    RequestId::from_u128(value).expect("test request id must be nonzero")
}

fn server_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "zippy-storage-persist-controller-test".to_owned(),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]))
}

fn provenance(first: u64, row_count: usize) -> NativeDataFileProvenance {
    let last = first + u64::try_from(row_count).expect("small test row count") - 1;
    let offsets = OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
        .expect("ordered test offsets");
    NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: first + 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .expect("valid source lineage"),
    )
    .expect("lineage covers candidate offsets")
}

fn candidate_input(file: u128, first: u64, values: Vec<i64>) -> PersistCandidateInput {
    candidate_input_for_table(TABLE, file, first, values)
}

fn candidate_input_for_table(
    table: u128,
    file: u128,
    first: u64,
    values: Vec<i64>,
) -> PersistCandidateInput {
    let (spec, batches) = candidate_parts(table, file, first, values);
    PersistCandidateInput::new(spec, batches).expect("valid persist candidate input")
}

fn candidate_parts(
    table: u128,
    file: u128,
    first: u64,
    values: Vec<i64>,
) -> (ParquetCandidateSpec, Vec<RecordBatch>) {
    let schema = schema();
    let row_count = values.len();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
        .expect("valid test batch");
    (
        ParquetCandidateSpec {
            file_id: file_id(file),
            table_id: table_id(table),
            schema_version: 1,
            schema,
            provenance: provenance(first, row_count),
            partition: MetadataMap::new(),
            metadata: MetadataMap::new(),
            created_at_ns: 1_752_492_600_000_000_000 + file as u64,
        },
        vec![batch],
    )
}

fn persist_request(
    request: u128,
    file: u128,
    first: u64,
    values: Vec<i64>,
    expected_snapshot_version: u64,
) -> PersistRequest {
    persist_request_for_table(
        TABLE,
        request,
        file,
        first,
        values,
        expected_snapshot_version,
    )
}

fn persist_request_for_table(
    table: u128,
    request: u128,
    file: u128,
    first: u64,
    values: Vec<i64>,
    expected_snapshot_version: u64,
) -> PersistRequest {
    let last = first + u64::try_from(values.len()).expect("small test row count") - 1;
    PersistRequest::new(
        request_id(request),
        table_id(table),
        expected_snapshot_version,
        vec![candidate_input_for_table(table, file, first, values)],
        OffsetWatermark::Known(LogOffset::new(last)),
    )
    .expect("valid persist request")
}

fn owned_directory(root: &tempfile::TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(
        OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
            .expect("own test data root"),
    )
}

fn candidate_path(root: &tempfile::TempDir, file: u128) -> std::path::PathBuf {
    candidate_path_for_table(root, TABLE, file)
}

fn candidate_path_for_table(
    root: &tempfile::TempDir,
    table: u128,
    file: u128,
) -> std::path::PathBuf {
    root.path().join(format!(
        "data/parquet/{}/{}.parquet",
        table_id(table),
        file_id(file)
    ))
}

struct FakeCatalog {
    blocked: bool,
    calls: AtomicUsize,
    started: Semaphore,
    release: Semaphore,
}

impl FakeCatalog {
    fn success() -> Self {
        Self {
            blocked: false,
            calls: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    fn blocked() -> Self {
        Self {
            blocked: true,
            calls: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }

    async fn wait_started(&self) {
        self.started
            .acquire()
            .await
            .expect("wait for fake catalog call")
            .forget();
    }
}

impl CatalogWrite for FakeCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.started.add_permits(1);
            if self.blocked {
                self.release
                    .acquire()
                    .await
                    .expect("fake catalog release")
                    .forget();
            }
            Ok(success_response(&command))
        })
    }
}

fn success_response(command: &VersionedCatalogCommand) -> VersionedCatalogResponse {
    let VersionedCatalogCommand::V1(envelope) = command;
    let CatalogCommandV1::CommitDataFiles(commit) = &envelope.command else {
        panic!("fake catalog accepts only CommitDataFiles")
    };
    let catalog_revision = commit.expected_snapshot_version + 10;
    CatalogResponseV1 {
        request_id: envelope.request_id,
        catalog_revision,
        outcome: CatalogOutcomeV1::Snapshot(TableSnapshot {
            table_id: commit.table_id,
            snapshot_version: commit.expected_snapshot_version + 1,
            parent_snapshot_version: Some(commit.expected_snapshot_version),
            operation: SnapshotOperation::Persist,
            commit_lsn: catalog_revision,
            committed_log_offset: commit.committed_log_offset,
            created_at_ns: 1_752_492_600_000_000_000,
            summary: MetadataMap::new(),
        }),
    }
    .into()
}

async fn start_controller(
    owned: Arc<OwnedDataDirectory>,
    catalog: Arc<FakeCatalog>,
    config: PersistControllerConfig,
) -> PersistController {
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        catalog as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start Catalog commit coordinator");
    PersistController::start(config, ParquetCandidateWriter::new(owned), coordinator)
        .await
        .expect("start persist controller")
}

#[test]
fn config_and_request_validation_rejects_work_before_startup() {
    let error = PersistControllerConfig::new()
        .with_max_outstanding_jobs(0)
        .validate()
        .expect_err("zero jobs must fail validation");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    let error = PersistControllerConfig::new()
        .with_max_inflight_input_bytes(65_535)
        .validate()
        .expect_err("unaligned input budget must fail validation");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    let error = PersistControllerConfig::new()
        .with_max_outstanding_jobs(1)
        .with_max_parallel_writers(2)
        .validate()
        .expect_err("writers cannot exceed outstanding jobs");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);

    let wrong_table = PersistRequest::new(
        request_id(1),
        table_id(TABLE),
        1,
        vec![candidate_input_for_table(TABLE + 1, 20, 0, vec![1])],
        OffsetWatermark::Known(LogOffset::new(0)),
    )
    .expect_err("candidate table mismatch must fail");
    assert_eq!(wrong_table.code(), ErrorCode::InvalidArgument);

    let gap = PersistRequest::new(
        request_id(2),
        table_id(TABLE),
        1,
        vec![
            candidate_input(20, 0, vec![1]),
            candidate_input(21, 2, vec![2]),
        ],
        OffsetWatermark::Known(LogOffset::new(2)),
    )
    .expect_err("cross-candidate offset gap must fail");
    assert_eq!(gap.code(), ErrorCode::InvalidArgument);

    let watermark = PersistRequest::new(
        request_id(3),
        table_id(TABLE),
        1,
        vec![candidate_input(22, 0, vec![1])],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .expect_err("watermark beyond coverage must fail");
    assert_eq!(watermark.code(), ErrorCode::InvalidArgument);
}

#[test]
fn oversized_footer_is_rejected_while_preparing_persist_input() {
    let (mut spec, batches) = candidate_parts(TABLE, 23, 0, vec![1]);
    spec.metadata.insert(
        "oversized".to_owned(),
        MetadataScalar::String("x".repeat(1024 * 1024)),
    );

    let error = PersistCandidateInput::new(spec, batches)
        .expect_err("footer above one MiB must fail before controller admission");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test]
async fn metadata_only_payload_cannot_bypass_input_budget() {
    let root = tempfile::TempDir::new().expect("create test root");
    let catalog = Arc::new(FakeCatalog::success());
    let controller = start_controller(
        owned_directory(&root),
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(1)
            .with_max_inflight_input_bytes(64 * 1024)
            .with_max_parallel_writers(1),
    )
    .await;
    let (mut spec, batches) = candidate_parts(TABLE, 24, 0, vec![1]);
    spec.metadata.insert(
        "large_metadata".to_owned(),
        MetadataScalar::String("x".repeat(96 * 1024)),
    );
    let candidate =
        PersistCandidateInput::new(spec, batches).expect("sub-limit footer is valid input");
    let request = PersistRequest::new(
        request_id(4),
        table_id(TABLE),
        1,
        vec![candidate],
        OffsetWatermark::Known(LogOffset::new(0)),
    )
    .expect("metadata-heavy request is structurally valid");
    assert!(request.retained_input_bytes() > 64 * 1024);

    let error = controller
        .handle()
        .submit(request)
        .err()
        .expect("metadata-heavy request must exceed the retained input budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert!(!candidate_path(&root, 24).exists());
    assert_eq!(catalog.calls(), 0);
    controller.drain().await.expect("drain idle controller");
}

#[tokio::test]
async fn admission_distinguishes_oversized_and_temporarily_exhausted_input() {
    let root = tempfile::TempDir::new().expect("create test root");
    let catalog = Arc::new(FakeCatalog::success());
    let controller = start_controller(
        owned_directory(&root),
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(64 * 1024)
            .with_max_parallel_writers(1),
    )
    .await;
    let handle = controller.handle();

    let oversized = handle
        .submit(persist_request(1, 20, 0, vec![1; 10_000], 1))
        .err()
        .expect("single oversized request must fail admission");
    assert_eq!(oversized.code(), ErrorCode::ResourceExhausted);
    assert!(!oversized.retryable());
    assert!(!candidate_path(&root, 20).exists());

    let first = handle
        .submit(persist_request(2, 21, 0, vec![1], 1))
        .expect("first request fits one accounting unit");
    let exhausted = handle
        .submit(persist_request_for_table(TABLE + 1, 3, 22, 0, vec![2], 1))
        .err()
        .expect("second request must see temporary input pressure");
    assert_eq!(exhausted.code(), ErrorCode::ResourceExhausted);
    assert!(exhausted.retryable());

    first.wait().await.expect("first persist succeeds");
    assert_eq!(catalog.calls(), 1);
    controller.drain().await.expect("drain controller");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_parallelism_is_bounded_and_configurable() {
    let serial_root = tempfile::TempDir::new().expect("create serial test root");
    let serial_catalog = Arc::new(FakeCatalog::blocked());
    let serial = start_controller(
        owned_directory(&serial_root),
        Arc::clone(&serial_catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(128 * 1024)
            .with_max_parallel_writers(1),
    )
    .await;
    let serial_handle = serial.handle();
    let serial_first = serial_handle
        .submit(persist_request(10, 30, 0, vec![1], 1))
        .expect("admit first serial request");
    let serial_second = serial_handle
        .submit(persist_request_for_table(TABLE + 1, 11, 31, 0, vec![2], 1))
        .expect("admit second serial request");
    serial_catalog.wait_started().await;
    assert!(candidate_path(&serial_root, 30).exists());
    assert!(!candidate_path_for_table(&serial_root, TABLE + 1, 31).exists());
    serial_catalog.release.add_permits(2);
    serial_first.wait().await.expect("first serial persist");
    serial_second.wait().await.expect("second serial persist");
    serial.drain().await.expect("drain serial controller");

    let parallel_root = tempfile::TempDir::new().expect("create parallel test root");
    let parallel_catalog = Arc::new(FakeCatalog::blocked());
    let parallel = start_controller(
        owned_directory(&parallel_root),
        Arc::clone(&parallel_catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(128 * 1024)
            .with_max_parallel_writers(2),
    )
    .await;
    let parallel_handle = parallel.handle();
    let parallel_first = parallel_handle
        .submit(persist_request(20, 40, 0, vec![1], 1))
        .expect("admit first parallel request");
    let parallel_second = parallel_handle
        .submit(persist_request_for_table(TABLE + 1, 21, 41, 0, vec![2], 1))
        .expect("admit second parallel request");
    parallel_catalog.wait_started().await;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !candidate_path_for_table(&parallel_root, TABLE + 1, 41).exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second parallel writer must publish while first commit is blocked");
    parallel_catalog.release.add_permits(2);
    parallel_first.wait().await.expect("first parallel persist");
    parallel_second
        .wait()
        .await
        .expect("second parallel persist");
    parallel.drain().await.expect("drain parallel controller");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_table_admission_is_single_flight_until_commit_completes() {
    let root = tempfile::TempDir::new().expect("create test root");
    let catalog = Arc::new(FakeCatalog::blocked());
    let controller = start_controller(
        owned_directory(&root),
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(128 * 1024)
            .with_max_parallel_writers(2),
    )
    .await;
    let handle = controller.handle();
    let first = handle
        .submit(persist_request(25, 45, 0, vec![1], 1))
        .expect("admit first table persist");
    catalog.wait_started().await;

    let busy = handle
        .submit(persist_request(26, 46, 1, vec![2], 2))
        .err()
        .expect("second same-table persist must not reach a writer");
    assert_eq!(busy.code(), ErrorCode::ResourceExhausted);
    assert!(busy.retryable());
    assert!(!candidate_path(&root, 46).exists());

    catalog.release.add_permits(1);
    first.wait().await.expect("first table persist");
    let second = handle
        .submit(persist_request(26, 46, 1, vec![2], 2))
        .expect("table permit releases after the first commit");
    catalog.wait_started().await;
    catalog.release.add_permits(1);
    second.wait().await.expect("second table persist");
    controller.drain().await.expect("drain controller");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_ticket_does_not_cancel_and_drain_closes_admission() {
    let root = tempfile::TempDir::new().expect("create test root");
    let catalog = Arc::new(FakeCatalog::blocked());
    let controller = start_controller(
        owned_directory(&root),
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(1)
            .with_max_inflight_input_bytes(64 * 1024)
            .with_max_parallel_writers(1),
    )
    .await;
    let handle = controller.handle();
    let ticket = handle
        .submit(persist_request(30, 50, 0, vec![1], 1))
        .expect("admit persist before dropping waiter");
    catalog.wait_started().await;
    drop(ticket);

    let drain = tokio::spawn(controller.drain());
    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let error = handle
                .submit(persist_request(31, 51, 1, vec![2], 2))
                .err()
                .expect("active request keeps new work from admission");
            if error.code() == ErrorCode::Unavailable {
                break error;
            }
            assert_eq!(error.code(), ErrorCode::ResourceExhausted);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drain task must close admission");
    assert_eq!(closed.code(), ErrorCode::Unavailable);
    assert!(!drain.is_finished());

    catalog.release.add_permits(1);
    drain
        .await
        .expect("join drain task")
        .expect("drain accepted persist");
    assert_eq!(catalog.calls(), 1);
    assert!(candidate_path(&root, 50).exists());
}

#[tokio::test]
async fn first_writer_error_closes_admission_but_drains_already_accepted_jobs() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let (spec, batches) = candidate_parts(TABLE, 60, 0, vec![1]);
    ParquetCandidateWriter::new(Arc::clone(&owned))
        .write_candidate(spec, batches)
        .expect("write conflicting preexisting candidate");

    let catalog = Arc::new(FakeCatalog::success());
    let controller = start_controller(
        owned,
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(128 * 1024)
            .with_max_parallel_writers(1),
    )
    .await;
    let handle = controller.handle();
    let failed = handle
        .submit(persist_request(40, 60, 0, vec![2], 1))
        .expect("admit conflicting request");
    let accepted = handle
        .submit(persist_request_for_table(TABLE + 1, 41, 61, 0, vec![3], 1))
        .expect("admit queued request before writer failure");

    let error = failed.wait().await.expect_err("conflicting file must fail");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    let closed = handle
        .submit(persist_request(42, 62, 2, vec![4], 3))
        .err()
        .expect("writer error must close global admission");
    assert_eq!(closed.code(), ErrorCode::Unavailable);
    accepted
        .wait()
        .await
        .expect("already accepted job must still commit");
    assert_eq!(catalog.calls(), 1);
    let drain_error = controller
        .drain()
        .await
        .expect_err("drain must return the first writer error");
    assert_eq!(drain_error.code(), ErrorCode::DataCorruption);
}
