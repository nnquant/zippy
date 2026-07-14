use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::Semaphore;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, CommitDataFilesIntent,
    ParquetCandidateSpec, ParquetCandidateWriter,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogOutcomeV1,
    CatalogRead, CatalogResponseV1, CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand,
    CreateTableCommand, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance, SchemaIpcPayload,
    SnapshotOperation, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
    TableDefinition, TableSnapshot,
};
use zippy_types::{
    BoxFuture, ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId,
    Result, ServerError, TableId,
};

const NAMESPACE: u128 = 1;
const TABLE: u128 = 10;

fn namespace_id(value: u128) -> NamespaceId {
    NamespaceId::from_u128(value).expect("test namespace id must be nonzero")
}

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
        principal: "zippy-storage-catalog-commit-test".to_owned(),
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]))
}

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).expect("create schema writer");
    writer.finish().expect("finish schema writer");
    drop(writer);
    SchemaIpcPayload::new(bytes).expect("nonempty schema IPC")
}

fn batch(schema: SchemaRef, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
        .expect("valid test batch")
}

fn provenance(first: u64, last: u64) -> NativeDataFileProvenance {
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

fn write_candidate(
    writer: &ParquetCandidateWriter,
    file: u128,
    first: u64,
    values: Vec<i64>,
) -> zippy_storage::DurableParquetCandidate {
    let last = first + u64::try_from(values.len()).expect("small test row count") - 1;
    let schema = schema();
    writer
        .write_candidate(
            ParquetCandidateSpec {
                file_id: file_id(file),
                table_id: table_id(TABLE),
                schema_version: 1,
                schema: Arc::clone(&schema),
                provenance: provenance(first, last),
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: 1_752_492_600_000_000_000 + file as u64,
            },
            vec![batch(schema, values)],
        )
        .expect("write durable candidate")
}

fn commit_intent(
    request: u128,
    expected_snapshot_version: u64,
    committed_offset: u64,
    candidates: Vec<zippy_storage::DurableParquetCandidate>,
) -> CommitDataFilesIntent {
    CommitDataFilesIntent::new(
        request_id(request),
        table_id(TABLE),
        expected_snapshot_version,
        candidates,
        OffsetWatermark::Known(LogOffset::new(committed_offset)),
    )
    .expect("valid commit intent")
}

fn owned_directory(root: &tempfile::TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(
        OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
            .expect("own test data root"),
    )
}

fn intent_files(root: &tempfile::TempDir) -> Vec<std::path::PathBuf> {
    let directory = root.path().join("data/wal/catalog-commit-intents");
    let Ok(entries) = fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.expect("read intent entry").path())
        .collect()
}

fn catalog_command(
    request: u128,
    expected_revision: Option<u64>,
    command: CatalogCommandV1,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(
        request_id(request),
        expected_revision,
        server_actor(),
        command,
    )
    .expect("valid catalog command")
    .into()
}

async fn create_append_table(catalog: &SqliteCatalog, schema: &Schema) {
    catalog
        .command(catalog_command(
            1,
            Some(0),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: namespace_id(NAMESPACE),
                name: CatalogName::new("market").expect("valid namespace"),
            }),
        ))
        .await
        .expect("create namespace");
    catalog
        .command(catalog_command(
            2,
            Some(1),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: table_id(TABLE),
                namespace_id: namespace_id(NAMESPACE),
                name: CatalogName::new("ticks").expect("valid table"),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: schema_ipc(schema),
                config: Default::default(),
            }),
        ))
        .await
        .expect("create append table");
}

#[derive(Debug, Clone, Copy)]
enum FakeMode {
    Success,
    UnavailableOnce,
    BlockSuccess,
    TerminalLimit,
    MismatchedResponse,
}

struct FakeCatalog {
    mode: FakeMode,
    calls: AtomicUsize,
    commands: Mutex<Vec<VersionedCatalogCommand>>,
    started: Semaphore,
    release: Semaphore,
}

impl FakeCatalog {
    fn new(mode: FakeMode) -> Self {
        Self {
            mode,
            calls: AtomicUsize::new(0),
            commands: Mutex::new(Vec::new()),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Acquire)
    }

    fn commands(&self) -> Vec<VersionedCatalogCommand> {
        self.commands.lock().expect("lock fake commands").clone()
    }
}

impl CatalogWrite for FakeCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            self.commands
                .lock()
                .expect("lock fake commands")
                .push(command.clone());
            self.started.add_permits(1);
            match self.mode {
                FakeMode::UnavailableOnce if call == 0 => Err(ServerError::new(
                    ErrorCode::Unavailable,
                    "lost fake response",
                )
                .with_request_id(command_request_id(&command))),
                FakeMode::BlockSuccess => {
                    self.release
                        .acquire()
                        .await
                        .expect("fake release semaphore")
                        .forget();
                    Ok(success_response(&command, false))
                }
                FakeMode::TerminalLimit => Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "fake command permanently exceeds its limit",
                )
                .with_request_id(command_request_id(&command))
                .with_retryable(false)),
                FakeMode::MismatchedResponse => Ok(success_response(&command, true)),
                FakeMode::Success | FakeMode::UnavailableOnce => {
                    Ok(success_response(&command, false))
                }
            }
        })
    }
}

fn command_request_id(command: &VersionedCatalogCommand) -> RequestId {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope.request_id
}

fn success_response(
    command: &VersionedCatalogCommand,
    mismatched_table: bool,
) -> VersionedCatalogResponse {
    let VersionedCatalogCommand::V1(envelope) = command;
    let CatalogCommandV1::CommitDataFiles(commit) = &envelope.command else {
        panic!("fake catalog accepts only CommitDataFiles")
    };
    let catalog_revision = 3;
    CatalogResponseV1 {
        request_id: envelope.request_id,
        catalog_revision,
        outcome: CatalogOutcomeV1::Snapshot(TableSnapshot {
            table_id: if mismatched_table {
                table_id(TABLE + 1)
            } else {
                commit.table_id
            },
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_catalog_commit_publishes_one_exact_snapshot() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let schema = schema();
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog, &schema).await;
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100, 101]);
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start commit coordinator");

    let receipt = coordinator
        .commit(commit_intent(10, 1, 1, vec![candidate]))
        .await
        .expect("commit candidate snapshot");

    assert_eq!(receipt.request_id, request_id(10));
    assert_eq!(receipt.catalog_revision, 3);
    assert_eq!(receipt.snapshot.snapshot_version, 2);
    assert!(!receipt.intent_cleanup_pending);
    assert!(intent_files(&root).is_empty());
    let snapshot = catalog
        .read_snapshot_at(&[CatalogSnapshotSelector {
            table_id: table_id(TABLE),
            snapshot_version: 2,
            schema_version: 1,
        }])
        .await
        .expect("read committed snapshot");
    assert_eq!(snapshot.tables()[0].files().as_slice().len(), 1);

    coordinator.drain().await.expect("drain coordinator");
    Arc::try_unwrap(catalog)
        .ok()
        .expect("coordinator released catalog")
        .close()
        .expect("close catalog");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_unknown_replays_the_identical_envelope() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100]);
    let catalog = Arc::new(FakeCatalog::new(FakeMode::UnavailableOnce));
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new()
            .with_retry_initial_delay(std::time::Duration::from_millis(1))
            .with_retry_max_delay(std::time::Duration::from_millis(2)),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start commit coordinator");

    coordinator
        .commit(commit_intent(10, 1, 0, vec![candidate]))
        .await
        .expect("replay commit-unknown intent");

    assert_eq!(catalog.calls(), 2);
    let commands = catalog.commands();
    assert_eq!(commands[0], commands[1]);
    assert!(intent_files(&root).is_empty());
    coordinator.drain().await.expect("drain coordinator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_waiter_does_not_cancel_an_accepted_commit() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100]);
    let catalog = Arc::new(FakeCatalog::new(FakeMode::BlockSuccess));
    let coordinator = Arc::new(
        CatalogCommitCoordinator::start(
            CatalogCommitCoordinatorConfig::new(),
            Arc::clone(&owned),
            Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
            server_actor(),
        )
        .await
        .expect("start commit coordinator"),
    );
    let task_coordinator = Arc::clone(&coordinator);
    let waiter = tokio::spawn(async move {
        task_coordinator
            .commit(commit_intent(10, 1, 0, vec![candidate]))
            .await
    });
    catalog
        .started
        .acquire()
        .await
        .expect("wait for fake catalog call")
        .forget();

    waiter.abort();
    assert!(waiter
        .await
        .expect_err("waiter must be cancelled")
        .is_cancelled());
    catalog.release.add_permits(1);
    let coordinator = Arc::try_unwrap(coordinator)
        .ok()
        .expect("cancelled waiter released coordinator");
    coordinator.drain().await.expect("drain accepted commit");

    assert_eq!(catalog.calls(), 1);
    assert!(intent_files(&root).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_failure_retains_intent_and_next_startup_recovers_it() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100]);
    let failed_catalog = Arc::new(FakeCatalog::new(FakeMode::TerminalLimit));
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&failed_catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start first coordinator");

    let error = coordinator
        .commit(commit_intent(10, 1, 0, vec![candidate]))
        .await
        .expect_err("terminal catalog failure must surface");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert_eq!(failed_catalog.calls(), 1);
    assert_eq!(intent_files(&root).len(), 1);
    assert!(coordinator.drain().await.is_err());

    let recovered_catalog = Arc::new(FakeCatalog::new(FakeMode::Success));
    let recovered = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&recovered_catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("recover retained commit intent");

    assert_eq!(recovered_catalog.calls(), 1);
    assert_eq!(
        command_request_id(&recovered_catalog.commands()[0]),
        request_id(10)
    );
    assert!(intent_files(&root).is_empty());
    recovered
        .drain()
        .await
        .expect("drain recovered coordinator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tampered_candidate_and_mismatched_response_fail_closed() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100]);
    fs::write(candidate.path(), b"tampered").expect("tamper candidate");
    let catalog = Arc::new(FakeCatalog::new(FakeMode::Success));
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start commit coordinator");

    let error = coordinator
        .commit(commit_intent(10, 1, 0, vec![candidate]))
        .await
        .expect_err("tampered candidate must fail before Catalog");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(catalog.calls(), 0);
    assert_eq!(intent_files(&root).len(), 1);
    assert!(coordinator.drain().await.is_err());

    let second_root = tempfile::TempDir::new().expect("create second test root");
    let second_owned = owned_directory(&second_root);
    let second_writer = ParquetCandidateWriter::new(Arc::clone(&second_owned));
    let second_candidate = write_candidate(&second_writer, 21, 0, vec![100]);
    let mismatch = Arc::new(FakeCatalog::new(FakeMode::MismatchedResponse));
    let second = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&second_owned),
        Arc::clone(&mismatch) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start second coordinator");
    let error = second
        .commit(commit_intent(11, 1, 0, vec![second_candidate]))
        .await
        .expect_err("mismatched response must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(intent_files(&second_root).len(), 1);
    assert!(second.drain().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpolled_and_oversized_requests_never_reach_the_worker() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let first = write_candidate(&writer, 20, 0, vec![100]);
    let second = write_candidate(&writer, 21, 1, vec![101]);
    let catalog = Arc::new(FakeCatalog::new(FakeMode::Success));
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new().with_max_outstanding_command_bytes(1),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start commit coordinator");

    let unpolled = coordinator.commit(commit_intent(10, 1, 0, vec![first]));
    drop(unpolled);
    let error = coordinator
        .commit(commit_intent(11, 1, 1, vec![second]))
        .await
        .expect_err("single command cannot fit one-byte budget");

    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert_eq!(catalog.calls(), 0);
    assert!(intent_files(&root).is_empty());
    coordinator.drain().await.expect("drain empty coordinator");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_first_commit_prevents_the_second_from_overtaking() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let first = write_candidate(&writer, 20, 0, vec![100]);
    let second = write_candidate(&writer, 21, 1, vec![101]);
    let catalog = Arc::new(FakeCatalog::new(FakeMode::BlockSuccess));
    let coordinator = Arc::new(
        CatalogCommitCoordinator::start(
            CatalogCommitCoordinatorConfig::new().with_max_outstanding_commits(2),
            Arc::clone(&owned),
            Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
            server_actor(),
        )
        .await
        .expect("start commit coordinator"),
    );
    let first_coordinator = Arc::clone(&coordinator);
    let first_waiter = tokio::spawn(async move {
        first_coordinator
            .commit(commit_intent(10, 1, 0, vec![first]))
            .await
    });
    catalog
        .started
        .acquire()
        .await
        .expect("wait for first catalog call")
        .forget();
    let second_coordinator = Arc::clone(&coordinator);
    let second_waiter = tokio::spawn(async move {
        second_coordinator
            .commit(commit_intent(11, 2, 1, vec![second]))
            .await
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(catalog.calls(), 1);
    catalog.release.add_permits(1);
    catalog
        .started
        .acquire()
        .await
        .expect("wait for second catalog call")
        .forget();
    assert_eq!(catalog.calls(), 2);
    assert_eq!(command_request_id(&catalog.commands()[0]), request_id(10));
    assert_eq!(command_request_id(&catalog.commands()[1]), request_id(11));
    catalog.release.add_permits(1);
    first_waiter
        .await
        .expect("join first waiter")
        .expect("first commit succeeds");
    second_waiter
        .await
        .expect("join second waiter")
        .expect("second commit succeeds");

    let coordinator = Arc::try_unwrap(coordinator)
        .ok()
        .expect("waiters released coordinator");
    coordinator.drain().await.expect("drain coordinator");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_cleans_intent_temporary_files_and_rejects_symlink_directory() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let intent_directory = root.path().join("data/wal/catalog-commit-intents");
    fs::create_dir(&intent_directory).expect("create intent directory");
    fs::set_permissions(&intent_directory, fs::Permissions::from_mode(0o700))
        .expect("secure intent directory");
    let temporary = intent_directory.join(format!(
        ".00000000000000000001-{}.json.tmp-crash",
        request_id(10)
    ));
    fs::write(&temporary, b"incomplete").expect("write temporary intent");
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
        .expect("secure temporary intent");
    let catalog = Arc::new(FakeCatalog::new(FakeMode::Success));

    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("clean stale intent temporary file");
    assert!(!temporary.exists());
    coordinator.drain().await.expect("drain coordinator");

    fs::remove_dir(&intent_directory).expect("remove empty intent directory");
    let external = root.path().join("external-intents");
    fs::create_dir(&external).expect("create external intent directory");
    symlink(&external, &intent_directory).expect("replace intent directory with symlink");
    let error = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        Arc::clone(&catalog) as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .err()
    .expect("symlink intent directory must fail startup");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}
