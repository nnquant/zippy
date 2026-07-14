use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::Semaphore;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, ParquetCandidateSpec,
    ParquetCandidateWriter, PersistCandidateInput, PersistCompletionFailure, PersistController,
    PersistControllerConfig, PersistMode, PersistRequest,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
    CatalogCommandV1, CatalogDataFileProbeMatch, CatalogDataFileProbeResult,
    CatalogDataFileReferenceSnapshot, CatalogFileRead, CatalogIdempotencyRead, CatalogOutcomeV1,
    CatalogPersistence, CatalogPublicationControl, CatalogPublicationExclusion,
    CatalogPublicationPermit, CatalogQuerySnapshot, CatalogRead, CatalogResponseV1,
    CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, CreateTableCommand,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    ManagedDataFileProbe, NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, MetadataMap, MetadataScalar, NamespaceDescriptor,
    NativeDataFileProvenance, SchemaIpcPayload, SnapshotOperation, SourceSegmentIdentity,
    SourceSegmentLineage, SourceSegmentLineages, TableDefinition, TableDescriptor, TableRef,
    TableSnapshot,
};
use zippy_types::{
    BoxFuture, ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId,
    Result, ServerError, TableId,
};

const NAMESPACE: u128 = 1;
const TABLE: u128 = 10;
const FAKE_PUBLICATION_GATE_CAPACITY: u32 = 8;

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

fn schema_ipc(schema: &Schema) -> SchemaIpcPayload {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, schema).expect("create schema writer");
    writer.finish().expect("finish schema writer");
    drop(writer);
    SchemaIpcPayload::new(bytes).expect("nonempty schema IPC")
}

fn catalog_command(request: u128, command: CatalogCommandV1) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(request_id(request), None, server_actor(), command)
        .expect("valid catalog command")
        .into()
}

async fn create_append_table(catalog: &SqliteCatalog) {
    catalog
        .command(catalog_command(
            1,
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
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: table_id(TABLE),
                namespace_id: namespace_id(NAMESPACE),
                name: CatalogName::new("ticks").expect("valid table"),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: schema_ipc(schema().as_ref()),
                config: Default::default(),
            }),
        ))
        .await
        .expect("create append table");
}

fn provenance(first: u64, row_count: usize) -> NativeDataFileProvenance {
    provenance_with_epoch(first, row_count, 0)
}

fn provenance_with_epoch(
    first: u64,
    row_count: usize,
    writer_epoch: u64,
) -> NativeDataFileProvenance {
    let last = first + u64::try_from(row_count).expect("small test row count") - 1;
    let offsets = OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
        .expect("ordered test offsets");
    NativeDataFileProvenance::new(
        offsets,
        SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: first + 1,
                generation: 1,
                writer_epoch,
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

fn intent_files(root: &tempfile::TempDir) -> Vec<std::path::PathBuf> {
    let directory = root.path().join("data/wal/catalog-commit-intents");
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .map(|entry| entry.expect("read intent entry").path())
        .collect()
}

fn candidate_temporary_count(root: &tempfile::TempDir, table: u128) -> usize {
    let directory = root
        .path()
        .join(format!("data/parquet/{}", table_id(table)));
    let Ok(entries) = std::fs::read_dir(directory) else {
        return 0;
    };
    entries
        .filter_map(std::result::Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with('.') && name.contains(".parquet.tmp-"))
        })
        .count()
}

struct FakeCatalog {
    blocked: bool,
    calls: AtomicUsize,
    started: Semaphore,
    release: Semaphore,
    publication_gate: Arc<Semaphore>,
}

struct FakePublicationPermit {
    owner_id: usize,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl FakeCatalog {
    fn success() -> Self {
        Self {
            blocked: false,
            calls: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            publication_gate: Arc::new(Semaphore::new(FAKE_PUBLICATION_GATE_CAPACITY as usize)),
        }
    }

    fn blocked() -> Self {
        Self {
            blocked: true,
            calls: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            publication_gate: Arc::new(Semaphore::new(FAKE_PUBLICATION_GATE_CAPACITY as usize)),
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

    fn publication_owner_id(&self) -> usize {
        Arc::as_ptr(&self.publication_gate) as usize
    }

    async fn execute(&self, command: VersionedCatalogCommand) -> Result<VersionedCatalogResponse> {
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
    }
}

impl CatalogWrite for FakeCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let _permit = Arc::clone(&self.publication_gate)
                .acquire_owned()
                .await
                .expect("fake publication gate remains open");
            self.execute(command).await
        })
    }
}

impl CatalogPublicationControl for FakeCatalog {
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>> {
        Box::pin(async move {
            let permit = Arc::clone(&self.publication_gate)
                .acquire_owned()
                .await
                .expect("fake publication gate remains open");
            Ok(CatalogPublicationPermit::from_opaque(
                FakePublicationPermit {
                    owner_id: self.publication_owner_id(),
                    _permit: permit,
                },
            ))
        })
    }

    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let permit = permit
                .try_into_opaque::<FakePublicationPermit>()
                .map_err(|_| {
                    ServerError::new(
                        ErrorCode::InvalidArgument,
                        "fake publication permit has an invalid capability type",
                    )
                    .with_retryable(false)
                })?;
            if permit.owner_id != self.publication_owner_id() {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "fake publication permit belongs to another Catalog",
                )
                .with_retryable(false));
            }
            let _permit = permit;
            self.execute(command).await
        })
    }

    fn acquire_publication_exclusion(&self) -> BoxFuture<'_, Result<CatalogPublicationExclusion>> {
        Box::pin(async move {
            let permit = Arc::clone(&self.publication_gate)
                .acquire_many_owned(FAKE_PUBLICATION_GATE_CAPACITY)
                .await
                .expect("fake publication gate remains open");
            Ok(CatalogPublicationExclusion::from_opaque(permit))
        })
    }
}

impl CatalogRead for FakeCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move { Ok(self.calls() as u64) })
    }

    fn get_namespace(&self, _: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }

    fn list_namespaces(
        &self,
        _: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }

    fn get_table(&self, _: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }

    fn list_tables(&self, _: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }

    fn resolve_tables<'a>(
        &'a self,
        _: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        _: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async { Err(fake_read_unsupported()) })
    }
}

impl CatalogFileRead for FakeCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            Ok(CatalogDataFileReferenceSnapshot {
                catalog_revision: self.calls() as u64,
                results: probes
                    .iter()
                    .cloned()
                    .map(|probe| CatalogDataFileProbeResult {
                        probe,
                        matched: CatalogDataFileProbeMatch::Unreferenced,
                    })
                    .collect(),
            })
        })
    }
}

impl CatalogIdempotencyRead for FakeCatalog {
    fn probe_command_replay<'a>(
        &'a self,
        _: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
        Box::pin(async { Ok(CatalogCommandReplayProbe::Missing) })
    }
}

fn fake_read_unsupported() -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        "fake Catalog does not implement discovery reads",
    )
    .with_retryable(false)
}

struct RejectOnceCatalog {
    inner: Arc<SqliteCatalog>,
    reject_next_command: AtomicBool,
    replacement_path: Option<std::path::PathBuf>,
    replaced: AtomicBool,
}

impl RejectOnceCatalog {
    fn new(inner: Arc<SqliteCatalog>, replacement_path: Option<std::path::PathBuf>) -> Self {
        Self {
            inner,
            reject_next_command: AtomicBool::new(true),
            replacement_path,
            replaced: AtomicBool::new(false),
        }
    }
}

impl CatalogRead for RejectOnceCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        self.inner.revision()
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        self.inner.get_namespace(namespace)
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        self.inner.list_namespaces(request)
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        self.inner.get_table(table)
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        self.inner.list_tables(request)
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        self.inner.resolve_tables(tables)
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        self.inner.read_snapshot_at(selectors)
    }
}

impl CatalogFileRead for RejectOnceCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            if !self.replaced.swap(true, Ordering::AcqRel) {
                let concurrent_mutation = catalog_command(
                    9_999,
                    CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                        namespace_id: namespace_id(9_999),
                        name: CatalogName::new("cleanup-race")
                            .expect("valid cleanup race namespace"),
                    }),
                );
                assert!(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(50),
                        self.inner.command(concurrent_mutation),
                    )
                    .await
                    .is_err(),
                    "candidate cleanup must exclude concurrent Catalog mutation"
                );
                if let Some(path) = &self.replacement_path {
                    std::fs::write(path, b"replacement bytes")
                        .expect("replace published candidate before cleanup verification");
                }
            }
            self.inner.inspect_data_file_references(probes).await
        })
    }
}

impl CatalogIdempotencyRead for RejectOnceCatalog {
    fn probe_command_replay<'a>(
        &'a self,
        command: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
        self.inner.probe_command_replay(command)
    }
}

impl CatalogWrite for RejectOnceCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            if self.reject_next_command.swap(false, Ordering::AcqRel) {
                let VersionedCatalogCommand::V1(envelope) = &command;
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "injected definitive Catalog rejection after publication",
                )
                .with_request_id(envelope.request_id)
                .with_retryable(false));
            }
            self.inner.command(command).await
        })
    }
}

impl CatalogPublicationControl for RejectOnceCatalog {
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>> {
        self.inner.acquire_publication_permit()
    }

    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            if self.reject_next_command.swap(false, Ordering::AcqRel) {
                let VersionedCatalogCommand::V1(envelope) = &command;
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "injected definitive Catalog rejection after publication",
                )
                .with_request_id(envelope.request_id)
                .with_retryable(false));
            }
            self.inner
                .command_with_publication_permit(permit, command)
                .await
        })
    }

    fn acquire_publication_exclusion(&self) -> BoxFuture<'_, Result<CatalogPublicationExclusion>> {
        self.inner.acquire_publication_exclusion()
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
    let coordinator = CatalogCommitCoordinator::start_with_persistence(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        catalog as Arc<dyn CatalogPersistence>,
        server_actor(),
    )
    .await
    .expect("start Catalog commit coordinator");
    PersistController::start(config, ParquetCandidateWriter::new(owned), coordinator)
        .await
        .expect("start persist controller")
}

async fn start_persistent_controller(
    owned: Arc<OwnedDataDirectory>,
    catalog: Arc<SqliteCatalog>,
    config: PersistControllerConfig,
) -> PersistController {
    start_controller_with_persistence(
        owned,
        catalog as Arc<dyn CatalogPersistence>,
        config,
        CatalogCommitCoordinatorConfig::new(),
    )
    .await
}

async fn start_controller_with_persistence(
    owned: Arc<OwnedDataDirectory>,
    catalog: Arc<dyn CatalogPersistence>,
    config: PersistControllerConfig,
    coordinator_config: CatalogCommitCoordinatorConfig,
) -> PersistController {
    let coordinator = CatalogCommitCoordinator::start_with_persistence(
        coordinator_config,
        Arc::clone(&owned),
        catalog,
        server_actor(),
    )
    .await
    .expect("start persistent Catalog commit coordinator");
    PersistController::start(config, ParquetCandidateWriter::new(owned), coordinator)
        .await
        .expect("start persistent controller")
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

    let (mut second_schema_spec, second_schema_batches) = candidate_parts(TABLE, 23, 1, vec![2]);
    second_schema_spec.schema_version = 2;
    let mixed_schema = PersistRequest::new(
        request_id(4),
        table_id(TABLE),
        1,
        vec![
            candidate_input(22, 0, vec![1]),
            PersistCandidateInput::new(second_schema_spec, second_schema_batches).unwrap(),
        ],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .expect_err("one persist request cannot mix schema versions");
    assert_eq!(mixed_schema.code(), ErrorCode::SchemaMismatch);

    let (mut second_epoch_spec, second_epoch_batches) = candidate_parts(TABLE, 24, 1, vec![2]);
    second_epoch_spec.provenance = provenance_with_epoch(1, 1, 1);
    let mixed_epoch = PersistRequest::new(
        request_id(5),
        table_id(TABLE),
        1,
        vec![
            candidate_input(23, 0, vec![1]),
            PersistCandidateInput::new(second_epoch_spec, second_epoch_batches).unwrap(),
        ],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .expect_err("one persist request cannot mix writer epochs");
    assert_eq!(mixed_epoch.code(), ErrorCode::InvalidArgument);

    let (mut second_arrow_spec, _) = candidate_parts(TABLE, 25, 1, vec![2]);
    let second_arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "volume",
        DataType::Int64,
        false,
    )]));
    second_arrow_spec.schema = Arc::clone(&second_arrow_schema);
    let second_arrow_batches = vec![RecordBatch::try_new(
        second_arrow_schema,
        vec![Arc::new(Int64Array::from(vec![2]))],
    )
    .unwrap()];
    let mixed_arrow_schema = PersistRequest::new(
        request_id(6),
        table_id(TABLE),
        1,
        vec![
            candidate_input(24, 0, vec![1]),
            PersistCandidateInput::new(second_arrow_spec, second_arrow_batches).unwrap(),
        ],
        OffsetWatermark::Known(LogOffset::new(1)),
    )
    .expect_err("one persist request cannot mix Arrow schemas under the same version");
    assert_eq!(mixed_arrow_schema.code(), ErrorCode::SchemaMismatch);
}

#[tokio::test]
async fn controller_rejects_a_coordinator_without_publication_control() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(FakeCatalog::success());
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::new(),
        Arc::clone(&owned),
        catalog as Arc<dyn CatalogWrite>,
        server_actor(),
    )
    .await
    .expect("start compatibility coordinator");

    let result = PersistController::start(
        PersistControllerConfig::new(),
        ParquetCandidateWriter::new(owned),
        coordinator,
    )
    .await;
    let error = result
        .err()
        .expect("native controller must require publication control");

    assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
    assert!(!error.retryable());
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
        .submit_classified(persist_request(1, 20, 0, vec![1; 10_000], 1))
        .err()
        .expect("single oversized request must fail admission");
    let PersistCompletionFailure::Rejected(oversized) = oversized else {
        panic!("oversized input is a request-local admission rejection")
    };
    assert_eq!(oversized.code(), ErrorCode::ResourceExhausted);
    assert!(!oversized.retryable());
    assert!(!candidate_path(&root, 20).exists());

    let first = handle
        .submit(persist_request(2, 21, 0, vec![1], 1))
        .expect("first request fits one accounting unit");
    let exhausted = handle
        .submit_classified(persist_request_for_table(TABLE + 1, 3, 22, 0, vec![2], 1))
        .err()
        .expect("second request must see temporary input pressure");
    let PersistCompletionFailure::Rejected(exhausted) = exhausted else {
        panic!("temporary input pressure is a request-local rejection")
    };
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
    assert_eq!(candidate_temporary_count(&serial_root, TABLE + 1), 0);
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
        while candidate_temporary_count(&parallel_root, TABLE)
            + candidate_temporary_count(&parallel_root, TABLE + 1)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second parallel writer must finish a staged temp while commit is blocked");
    assert_ne!(
        candidate_path(&parallel_root, 40).exists(),
        candidate_path_for_table(&parallel_root, TABLE + 1, 41).exists(),
        "only the globally serialized commit owner may publish"
    );
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
                .submit_classified(persist_request(31, 51, 1, vec![2], 2))
                .err()
                .expect("active request keeps new work from admission");
            match error {
                PersistCompletionFailure::Fatal(error)
                    if error.code() == ErrorCode::Unavailable =>
                {
                    break error;
                }
                PersistCompletionFailure::Rejected(error) => {
                    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
                }
                PersistCompletionFailure::Fatal(error) => {
                    panic!("unexpected fatal admission error: {error}");
                }
            }
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
    let mut failure_monitor = controller.failure_monitor();
    let handle = controller.handle();
    let failed = handle
        .submit(persist_request(40, 60, 0, vec![2], 1))
        .expect("admit conflicting request");
    let accepted = handle
        .submit(persist_request_for_table(TABLE + 1, 41, 61, 0, vec![3], 1))
        .expect("admit queued request before writer failure");

    let error = failed.wait().await.expect_err("conflicting file must fail");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        failure_monitor.wait_for_failure(),
    )
    .await
    .expect("controller failure monitor must resolve before shutdown");
    assert_eq!(observed.code(), ErrorCode::DataCorruption);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_probe_resolves_before_any_candidate_publication() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog).await;
    let controller = start_persistent_controller(
        owned,
        Arc::clone(&catalog),
        PersistControllerConfig::new()
            .with_max_outstanding_jobs(2)
            .with_max_inflight_input_bytes(128 * 1024)
            .with_max_parallel_writers(2),
    )
    .await;
    let handle = controller.handle();

    let first = handle
        .submit_with_mode(
            persist_request(10, 30, 0, vec![100], 1),
            PersistMode::Advance,
        )
        .expect("admit current snapshot advance")
        .wait()
        .await
        .expect("commit current snapshot advance");
    assert_eq!(first.snapshot.snapshot_version, 2);
    assert!(candidate_path(&root, 30).exists());

    let replay = handle
        .submit_with_mode(
            persist_request(10, 30, 0, vec![100], 1),
            PersistMode::HistoricalReplay,
        )
        .expect("admit exact historical replay")
        .wait()
        .await
        .expect("resolve exact historical replay");
    assert_eq!(replay, first);

    let missing = handle
        .submit_with_mode(
            persist_request(11, 31, 0, vec![101], 1),
            PersistMode::HistoricalReplay,
        )
        .expect("admit missing historical replay")
        .wait()
        .await
        .expect_err("missing historical replay must not execute a new commit");
    assert_eq!(missing.code(), ErrorCode::Conflict);
    assert!(!candidate_path(&root, 31).exists());

    let conflicting = handle
        .submit_with_mode(
            persist_request(10, 32, 0, vec![102], 1),
            PersistMode::HistoricalReplay,
        )
        .expect("admit conflicting historical replay")
        .wait()
        .await
        .expect_err("request id reuse with another envelope must conflict");
    assert_eq!(conflicting.code(), ErrorCode::Conflict);
    assert!(!candidate_path(&root, 32).exists());

    let continued = handle
        .submit_with_mode(
            persist_request(12, 33, 1, vec![103], 2),
            PersistMode::Advance,
        )
        .expect("controller remains available after request-local replay rejection")
        .wait()
        .await
        .expect("later current snapshot advance succeeds");
    assert_eq!(continued.snapshot.snapshot_version, 3);
    assert!(candidate_path(&root, 33).exists());

    controller.drain().await.expect("drain replay controller");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_post_publication_rejection_cleans_owned_candidate_and_continues() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let sqlite = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&sqlite).await;
    let catalog = Arc::new(RejectOnceCatalog::new(Arc::clone(&sqlite), None));
    let controller = start_controller_with_persistence(
        owned,
        catalog as Arc<dyn CatalogPersistence>,
        PersistControllerConfig::new(),
        CatalogCommitCoordinatorConfig::new(),
    )
    .await;
    let handle = controller.handle();

    let rejected = handle
        .submit(persist_request(20, 40, 0, vec![100], 1))
        .expect("admit request rejected after publication")
        .wait()
        .await
        .expect_err("injected Catalog conflict must reject the request");
    assert_eq!(rejected.code(), ErrorCode::Conflict);
    assert!(!candidate_path(&root, 40).exists());
    assert!(intent_files(&root).is_empty());

    let continued = handle
        .submit(persist_request(21, 41, 0, vec![101], 1))
        .expect("controller remains available after safe cleanup")
        .wait()
        .await
        .expect("later request commits");
    assert_eq!(continued.snapshot.snapshot_version, 2);
    assert!(candidate_path(&root, 41).exists());
    controller.drain().await.expect("drain cleanup controller");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_candidate_replacement_fails_cleanup_closed() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let sqlite = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&sqlite).await;
    let candidate_path = candidate_path(&root, 40);
    let catalog = Arc::new(RejectOnceCatalog::new(sqlite, Some(candidate_path.clone())));
    let controller = start_controller_with_persistence(
        owned,
        catalog as Arc<dyn CatalogPersistence>,
        PersistControllerConfig::new(),
        CatalogCommitCoordinatorConfig::new(),
    )
    .await;
    let mut failure_monitor = controller.failure_monitor();
    let handle = controller.handle();

    let error = handle
        .submit(persist_request(20, 40, 0, vec![100], 1))
        .expect("admit request with replaced cleanup candidate")
        .wait()
        .await
        .expect_err("cleanup must reject a replaced physical file");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(candidate_path.exists());
    assert!(intent_files(&root).is_empty());
    let observed = failure_monitor.wait_for_failure().await;
    assert_eq!(observed.code(), ErrorCode::DataCorruption);
    let closed = handle
        .submit(persist_request(21, 41, 0, vec![101], 1))
        .err()
        .expect("cleanup corruption closes controller admission");
    assert_eq!(closed.code(), ErrorCode::Unavailable);
    let drained = controller
        .drain()
        .await
        .expect_err("drain returns cleanup corruption");
    assert_eq!(drained.code(), ErrorCode::DataCorruption);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_replay_bypasses_a_lowered_commit_budget() {
    let root = tempfile::TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog).await;
    let first_controller = start_persistent_controller(
        Arc::clone(&owned),
        Arc::clone(&catalog),
        PersistControllerConfig::new(),
    )
    .await;
    let first = first_controller
        .handle()
        .submit(persist_request(20, 40, 0, vec![100], 1))
        .expect("admit first persist")
        .wait()
        .await
        .expect("commit first persist");
    first_controller
        .drain()
        .await
        .expect("drain first controller");

    let replay_controller = start_controller_with_persistence(
        owned,
        catalog as Arc<dyn CatalogPersistence>,
        PersistControllerConfig::new(),
        CatalogCommitCoordinatorConfig::new().with_max_outstanding_command_bytes(1),
    )
    .await;
    let replay = replay_controller
        .handle()
        .submit_with_mode(
            persist_request(20, 40, 0, vec![100], 1),
            PersistMode::HistoricalReplay,
        )
        .expect("admit exact replay under lower commit budget")
        .wait()
        .await
        .expect("exact replay returns before new-commit budget validation");
    assert_eq!(replay, first);
    replay_controller
        .drain()
        .await
        .expect("drain replay controller");
}
