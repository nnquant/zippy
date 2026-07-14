use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, CommitDataFilesIntent,
    ParquetCandidateSpec, ParquetCandidateWriter, ReconciledFileDisposition,
    ReconciliationIssueKind, StartupOrphanReconciler, StartupOrphanReconcilerConfig,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogDataFileReferenceSnapshot, CatalogFileRead, CatalogQuerySnapshot, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, CommitDataFilesCommand, CreateNamespaceCommand,
    CreateTableCommand, ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest,
    ListTablesResponse, ManagedDataFileProbe, NamespaceRef, VersionedCatalogCommand,
    VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ManagedDataPath, MetadataMap, NamespaceDescriptor,
    NativeDataFileProvenance, SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage,
    SourceSegmentLineages, TableDefinition, TableDescriptor, TableRef,
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

fn owned_directory(root: &TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(
        OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
            .expect("own test data root"),
    )
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
    let last = first + u64::try_from(values.len()).expect("small row count") - 1;
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

fn server_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "orphan-reconciliation-test".to_owned(),
    }
}

fn command(
    request: u128,
    expected_revision: Option<u64>,
    body: CatalogCommandV1,
) -> VersionedCatalogCommand {
    CatalogCommandEnvelopeV1::new(request_id(request), expected_revision, server_actor(), body)
        .expect("valid Catalog command")
        .into()
}

async fn create_append_table(catalog: &SqliteCatalog, schema: &Schema) {
    catalog
        .command(command(
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
        .command(command(
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

struct TerminalCatalog;

impl CatalogWrite for TerminalCatalog {
    fn command(
        &self,
        _command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async {
            Err(ServerError::new(
                ErrorCode::DataCorruption,
                "terminal catalog failure for reconciliation test",
            )
            .with_retryable(false))
        })
    }
}

struct RevisionDriftCatalog {
    inner: Arc<SqliteCatalog>,
    revision_calls: AtomicUsize,
}

struct ShortProbeCatalog {
    inner: Arc<SqliteCatalog>,
}

impl CatalogRead for RevisionDriftCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            let revision = self.inner.revision().await?;
            let call = self.revision_calls.fetch_add(1, Ordering::AcqRel);
            Ok(revision + u64::from(call > 0))
        })
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

impl CatalogFileRead for RevisionDriftCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        self.inner.inspect_data_file_references(probes)
    }
}

impl CatalogRead for ShortProbeCatalog {
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

impl CatalogFileRead for ShortProbeCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            let mut response = self.inner.inspect_data_file_references(probes).await?;
            response.results.pop();
            Ok(response)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_verified_orphan_then_exact_catalog_reference_without_deleting() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let schema = schema();
    create_append_table(&catalog, &schema).await;
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidate = write_candidate(&writer, 20, 0, vec![100, 101]);
    let candidate_path = candidate.path().to_path_buf();

    let report = StartupOrphanReconciler::new(Arc::clone(&owned), catalog.clone())
        .reconcile()
        .await
        .expect("report unreferenced candidate");
    assert_eq!(report.catalog_revision, 2);
    assert_eq!(report.orphan_count(), 1);
    assert_eq!(
        report.files[0].disposition,
        ReconciledFileDisposition::Orphan
    );
    assert_eq!(
        report.files[0]
            .verified_candidate
            .as_ref()
            .map(|pending| pending.file_id),
        Some(file_id(20))
    );
    assert!(
        candidate_path.exists(),
        "read-only scan must retain the orphan"
    );

    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::default(),
        Arc::clone(&owned),
        catalog.clone(),
        server_actor(),
    )
    .await
    .expect("start commit coordinator");
    coordinator
        .commit(
            CommitDataFilesIntent::new(
                request_id(3),
                table_id(TABLE),
                1,
                vec![candidate],
                OffsetWatermark::Known(LogOffset::new(1)),
            )
            .expect("valid commit intent"),
        )
        .await
        .expect("commit candidate");
    coordinator.drain().await.expect("drain coordinator");

    let report = StartupOrphanReconciler::new(Arc::clone(&owned), catalog.clone())
        .reconcile()
        .await
        .expect("report committed candidate");
    assert_eq!(report.catalog_revision, 3);
    assert_eq!(report.orphan_count(), 0);
    assert!(report.has_no_observed_backlog());
    assert!(matches!(
        report.files[0].disposition,
        ReconciledFileDisposition::CatalogReferenced {
            state: zippy_types::table::DataFileState::Committed,
            removed_snapshot_version: None,
            durable_intent_request_id: None,
        }
    ));
    assert!(candidate_path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_terminal_intent_protects_uncommitted_candidate() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog, schema().as_ref()).await;
    let candidate = write_candidate(
        &ParquetCandidateWriter::new(Arc::clone(&owned)),
        20,
        0,
        vec![1, 2],
    );
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::default(),
        Arc::clone(&owned),
        Arc::new(TerminalCatalog),
        server_actor(),
    )
    .await
    .expect("start terminal coordinator");
    let error = coordinator
        .commit(
            CommitDataFilesIntent::new(
                request_id(3),
                table_id(TABLE),
                1,
                vec![candidate],
                OffsetWatermark::Known(LogOffset::new(1)),
            )
            .expect("valid commit intent"),
        )
        .await
        .expect_err("terminal Catalog error must retain intent");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(coordinator.drain().await.is_err());

    let report = StartupOrphanReconciler::new(Arc::clone(&owned), catalog.clone())
        .reconcile()
        .await
        .expect("retained intent must be visible to reconciliation");
    assert_eq!(report.orphan_count(), 0);
    assert!(matches!(
        report.files[0].disposition,
        ReconciledFileDisposition::PendingIntent { request_id: observed }
            if observed == request_id(3)
    ));
    assert!(report.files[0].verified_candidate.is_some());
    assert!(!report.has_no_observed_backlog());

    let reconciler = StartupOrphanReconciler::with_config(
        StartupOrphanReconcilerConfig::new().with_max_intent_bytes(1),
        owned,
        catalog,
    )
    .expect("valid one-byte intent budget");
    let error = reconciler
        .reconcile()
        .await
        .expect_err("aggregate intent bytes must be bounded before decode");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_multi_file_intent_is_reported_as_one_atomic_pending_group() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog, schema().as_ref()).await;
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let candidates = vec![
        write_candidate(&writer, 20, 0, vec![1, 2]),
        write_candidate(&writer, 21, 2, vec![3, 4]),
    ];
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::default(),
        Arc::clone(&owned),
        Arc::new(TerminalCatalog),
        server_actor(),
    )
    .await
    .expect("start terminal coordinator");
    coordinator
        .commit(
            CommitDataFilesIntent::new(
                request_id(3),
                table_id(TABLE),
                1,
                candidates,
                OffsetWatermark::Known(LogOffset::new(3)),
            )
            .expect("valid multi-file commit intent"),
        )
        .await
        .expect_err("terminal Catalog error must retain the group");
    assert!(coordinator.drain().await.is_err());

    let report = StartupOrphanReconciler::new(Arc::clone(&owned), catalog.clone())
        .reconcile()
        .await
        .expect("report atomic retained intent");
    assert_eq!(report.files.len(), 2);
    assert!(report.files.iter().all(|file| matches!(
        file.disposition,
        ReconciledFileDisposition::PendingIntent { request_id: observed }
            if observed == request_id(3)
    )));

    let reconciler = StartupOrphanReconciler::with_config(
        StartupOrphanReconcilerConfig::new().with_max_pending_intent_files(1),
        owned,
        catalog,
    )
    .expect("valid one-file intent budget");
    let error = reconciler
        .reconcile()
        .await
        .expect_err("multi-file intent must respect aggregate file budget");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partially_committed_multi_file_intent_fails_closed() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    create_append_table(&catalog, schema().as_ref()).await;
    let writer = ParquetCandidateWriter::new(Arc::clone(&owned));
    let first = write_candidate(&writer, 20, 0, vec![1, 2]);
    let first_pending = first.pending().clone();
    let second = write_candidate(&writer, 21, 2, vec![3, 4]);
    let coordinator = CatalogCommitCoordinator::start(
        CatalogCommitCoordinatorConfig::default(),
        Arc::clone(&owned),
        Arc::new(TerminalCatalog),
        server_actor(),
    )
    .await
    .expect("start terminal coordinator");
    coordinator
        .commit(
            CommitDataFilesIntent::new(
                request_id(3),
                table_id(TABLE),
                1,
                vec![first, second],
                OffsetWatermark::Known(LogOffset::new(3)),
            )
            .expect("valid multi-file commit intent"),
        )
        .await
        .expect_err("terminal Catalog error must retain the group");
    assert!(coordinator.drain().await.is_err());

    catalog
        .command(command(
            4,
            None,
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(TABLE),
                expected_snapshot_version: 1,
                files: vec![first_pending],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(1)),
            }),
        ))
        .await
        .expect("inject partial Catalog state");

    let error = StartupOrphanReconciler::new(owned, catalog)
        .reconcile()
        .await
        .expect_err("mixed atomic group must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(!error.retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_byte_budget_fails_before_unbounded_candidate_reads() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let candidate = write_candidate(
        &ParquetCandidateWriter::new(Arc::clone(&owned)),
        20,
        0,
        vec![1, 2],
    );
    let candidate_path = candidate.path().to_path_buf();
    let budget = candidate.pending().file_size_bytes - 1;
    let reconciler = StartupOrphanReconciler::with_config(
        StartupOrphanReconcilerConfig::new().with_max_verification_bytes(budget),
        Arc::clone(&owned),
        catalog.clone(),
    )
    .expect("valid nonzero verification budget");
    let error = reconciler
        .reconcile()
        .await
        .expect_err("candidate larger than verification budget must be rejected");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert!(candidate_path.exists());

    let reconciler = StartupOrphanReconciler::with_config(
        StartupOrphanReconcilerConfig::new().with_max_retained_metadata_bytes(1),
        owned,
        catalog,
    )
    .expect("valid one-byte retained metadata budget");
    let error = reconciler
        .reconcile()
        .await
        .expect_err("orphan evidence must respect the retained metadata budget");
    assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    assert!(!error.retryable());
    assert!(candidate_path.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intent_temporary_file_is_reported_without_read_only_cleanup() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let target = ManagedDataPath::new(format!(
        "wal/catalog-commit-intents/{:020}-{}.json",
        1,
        request_id(3)
    ))
    .expect("valid intent path");
    let mut transaction = owned
        .begin_durable_file(&target)
        .expect("create unpublished intent temp");
    transaction
        .write_all(b"partial")
        .expect("write partial intent");
    let temporary_path = transaction.temporary_path().to_path_buf();

    let report = StartupOrphanReconciler::new(owned, catalog)
        .reconcile()
        .await
        .expect("report intent temporary file");
    assert_eq!(report.temporary_files.len(), 1);
    assert!(temporary_path.exists());
    assert!(!report.has_no_observed_backlog());
    drop(transaction);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_temporary_unexpected_and_invalid_unreferenced_entries_without_removal() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let table = table_id(TABLE);
    let temporary_target = ManagedDataPath::new(format!("parquet/{table}/{}.parquet", file_id(20)))
        .expect("valid temporary target");
    let mut temporary = owned
        .begin_durable_file(&temporary_target)
        .expect("create unpublished candidate temp");
    temporary
        .write_all(b"unpublished")
        .expect("write unpublished bytes");
    let temporary_path = temporary.temporary_path().to_path_buf();

    let unexpected_path = ManagedDataPath::new(format!("parquet/{table}/notes.txt"))
        .expect("valid unexpected managed path");
    let mut unexpected = owned
        .begin_durable_file(&unexpected_path)
        .expect("create unexpected file");
    unexpected
        .write_all(b"notes")
        .expect("write unexpected file");
    let unexpected = unexpected
        .publish_noclobber()
        .expect("publish unexpected file");

    let invalid_path = ManagedDataPath::new(format!("parquet/{table}/{}.parquet", file_id(30)))
        .expect("valid invalid candidate path");
    let mut invalid = owned
        .begin_durable_file(&invalid_path)
        .expect("create invalid candidate");
    invalid
        .write_all(b"not a parquet candidate")
        .expect("write invalid candidate");
    let invalid = invalid
        .publish_noclobber()
        .expect("publish invalid candidate");

    let report = StartupOrphanReconciler::new(Arc::clone(&owned), catalog)
        .reconcile()
        .await
        .expect("report non-authoritative anomalies");
    assert_eq!(report.files.len(), 0);
    assert_eq!(report.temporary_files.len(), 1);
    assert_eq!(report.issues.len(), 2);
    assert!(report
        .issues
        .iter()
        .any(|issue| issue.kind == ReconciliationIssueKind::UnexpectedEntry));
    assert!(report
        .issues
        .iter()
        .any(|issue| { issue.kind == ReconciliationIssueKind::InvalidUnreferencedCandidate }));
    assert!(temporary_path.exists());
    assert!(unexpected.path().exists());
    assert!(invalid.path().exists());
    assert!(!report.has_no_observed_backlog());
    drop(temporary);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_scan_budgets_fail_before_filesystem_or_catalog_work() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let error = match StartupOrphanReconciler::with_config(
        StartupOrphanReconcilerConfig::new().with_probe_batch_size(0),
        owned,
        catalog,
    ) {
        Ok(_) => panic!("zero probe batch must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
    assert!(!error.retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_revision_drift_rejects_the_entire_report_as_retryable() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let drifting = Arc::new(RevisionDriftCatalog {
        inner: catalog,
        revision_calls: AtomicUsize::new(0),
    });
    let error = StartupOrphanReconciler::new(owned, drifting)
        .reconcile()
        .await
        .expect_err("revision drift must discard the full report");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(error.retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_revision_short_probe_response_is_terminal_corruption() {
    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    write_candidate(
        &ParquetCandidateWriter::new(Arc::clone(&owned)),
        20,
        0,
        vec![1, 2],
    );
    let malformed = Arc::new(ShortProbeCatalog { inner: catalog });
    let error = StartupOrphanReconciler::new(owned, malformed)
        .reconcile()
        .await
        .expect_err("non-dense response at the same revision must be corruption");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(!error.retryable());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn symbolic_link_in_managed_parquet_tree_fails_closed() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().expect("create test root");
    let owned = owned_directory(&root);
    let catalog = Arc::new(
        SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open sqlite catalog"),
    );
    let parquet = root.path().join("data/parquet");
    std::fs::create_dir_all(&parquet).expect("create Parquet directory");
    std::fs::set_permissions(&parquet, std::fs::Permissions::from_mode(0o700))
        .expect("secure Parquet directory");
    symlink(root.path(), parquet.join(table_id(TABLE).to_string()))
        .expect("create malicious table symlink");

    let error = StartupOrphanReconciler::new(owned, catalog)
        .reconcile()
        .await
        .expect_err("symlink must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
