//! Deterministic checkpoint/publication interleavings using real persistence and table actors.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;
use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, ParquetCandidateWriter,
    PersistController, PersistControllerConfig,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
    CatalogCommandV1, CatalogDataFileReferenceSnapshot, CatalogFileRead, CatalogIdempotencyRead,
    CatalogPersistence, CatalogPublicationControl, CatalogPublicationExclusion,
    CatalogPublicationPermit, CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector,
    CatalogWrite, CreateNamespaceCommand, CreateTableCommand, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse, ManagedDataFileProbe,
    NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::ingest::{Durability, KeyValueMutation, TableMutation, WriteReceipt};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
    KeyValueTableDefinition, NamespaceDescriptor, SchemaIpcPayload, TableDefinition,
    TableDescriptor, TableKind, TableRef,
};
use zippy_types::{
    BoxFuture, ErrorCode, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, ProducerId,
    RequestId, Result, TableId,
};

use crate::{
    IngestWalIdentity, NativeIngestApplyContext, NativeIngestSink, NativeIngestSinkError,
    NativeIngestTableBootstrap, TableActorRegistry, TableActorRegistryConfig,
    TableRuntimeBootstrap, TableRuntimeRegistry,
};

use super::{build_publication, NativeTableMaterializerConfig, NativeTableMaterializerSink};

/// Stops exactly after the actor acknowledges the new persisted watermark.
pub(super) struct CheckpointPause {
    reached: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}

impl CheckpointPause {
    pub(super) fn wait(self) {
        self.reached
            .send(())
            .expect("announce persisted checkpoint");
        self.resume.recv().expect("resume checkpoint reclamation");
    }
}

/// Suspends the real persistence pipeline immediately before its Catalog commit.
struct CatalogCommitPause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

/// Keeps all storage real while making one slow persistence boundary deterministic.
struct PausableCatalog {
    inner: Arc<SqliteCatalog>,
    pause: Arc<Mutex<Option<CatalogCommitPause>>>,
}

impl CatalogRead for PausableCatalog {
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

impl CatalogFileRead for PausableCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        self.inner.inspect_data_file_references(probes)
    }
}

impl CatalogIdempotencyRead for PausableCatalog {
    fn probe_command_replay<'a>(
        &'a self,
        command: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
        self.inner.probe_command_replay(command)
    }
}

impl CatalogWrite for PausableCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        self.inner.command(command)
    }
}

impl CatalogPublicationControl for PausableCatalog {
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>> {
        self.inner.acquire_publication_permit()
    }

    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let pause = self.pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause
                    .reached
                    .send(())
                    .expect("announce paused Catalog commit");
                pause.resume.await.expect("resume paused Catalog commit");
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

pub(super) struct Fixture {
    pub(super) sink: Arc<NativeTableMaterializerSink>,
    pub(super) actors: TableActorRegistry,
    pub(super) persistence: PersistController,
    persist_pause: Arc<Mutex<Option<CatalogCommitPause>>>,
    pub(super) _catalog: Arc<SqliteCatalog>,
    pub(super) _root: tempfile::TempDir,
}

pub(super) fn table_id() -> TableId {
    TableId::from_u128(10).unwrap()
}

fn logical_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Int64, false),
    ]))
}

fn server_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Server,
        principal: "materializer-checkpoint-test".to_owned(),
    }
}

impl Fixture {
    pub(super) async fn start(kind: TableKind) -> Self {
        let root = tempfile::TempDir::new().unwrap();
        let owned =
            Arc::new(OwnedDataDirectory::acquire_or_initialize(root.path().join("data")).unwrap());
        let catalog = Arc::new(SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).unwrap());
        let definition = match kind {
            TableKind::Append => TableDefinition::Append(AppendTableDefinition::default()),
            TableKind::Frame => TableDefinition::Frame(FrameTableDefinition {
                by: ColumnName::new("price").unwrap(),
            }),
            TableKind::KeyValue => TableDefinition::KeyValue(
                KeyValueTableDefinition::new(
                    ColumnSet::new(vec![ColumnName::new("symbol").unwrap()]).unwrap(),
                )
                .unwrap(),
            ),
        };
        let physical_schema = if kind == TableKind::KeyValue {
            zippy_types::key_value::log_schema(logical_schema().as_ref(), &["symbol".to_owned()])
                .unwrap()
        } else {
            logical_schema()
        };
        let mut schema_bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut schema_bytes, &physical_schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let namespace_id = NamespaceId::from_u128(1).unwrap();
        let commands = [
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id,
                name: CatalogName::new("checkpoint_tests").unwrap(),
            }),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: table_id(),
                namespace_id,
                name: CatalogName::new("prices").unwrap(),
                definition,
                schema_ipc: SchemaIpcPayload::new(schema_bytes).unwrap(),
                config: Default::default(),
            }),
        ];
        for (index, command) in commands.into_iter().enumerate() {
            catalog
                .command(
                    CatalogCommandEnvelopeV1::new(
                        RequestId::from_u128(index as u128 + 1).unwrap(),
                        None,
                        server_actor(),
                        command,
                    )
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
        }
        let snapshot = catalog
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                schema_version: 1,
                snapshot_version: 1,
            }])
            .await
            .unwrap();
        let current = &snapshot.tables()[0];
        let bootstrap = NativeIngestTableBootstrap::from_catalog_snapshot(
            current.table(),
            Arc::clone(&physical_schema),
            current.snapshot(),
        )
        .unwrap();
        let runtime = TableRuntimeBootstrap::new(
            current.table().clone(),
            current.schema().clone(),
            physical_schema,
            current.snapshot().clone(),
        )
        .unwrap();
        let persist_pause = Arc::new(Mutex::new(None));
        let persistence_catalog = Arc::new(PausableCatalog {
            inner: Arc::clone(&catalog),
            pause: Arc::clone(&persist_pause),
        });
        let coordinator = CatalogCommitCoordinator::start(
            CatalogCommitCoordinatorConfig::new(),
            Arc::clone(&owned),
            persistence_catalog as Arc<dyn CatalogPersistence>,
            server_actor(),
        )
        .await
        .unwrap();
        let persistence = PersistController::start(
            PersistControllerConfig::new(),
            ParquetCandidateWriter::new(owned),
            coordinator,
        )
        .await
        .unwrap();
        let actors = TableActorRegistry::start(
            TableActorRegistryConfig::new(),
            TableRuntimeRegistry::from_bootstraps(snapshot.catalog_revision(), vec![runtime])
                .unwrap(),
            persistence.handle(),
        )
        .unwrap();
        let sink = Arc::new(
            NativeTableMaterializerSink::start(
                NativeTableMaterializerConfig::new().with_row_capacity(8),
                &[bootstrap],
                &actors.handle(),
            )
            .unwrap(),
        );
        Self {
            sink,
            actors,
            persistence,
            persist_pause,
            _catalog: catalog,
            _root: root,
        }
    }

    pub(super) async fn drain(self) {
        self.actors.drain().await.unwrap();
        self.persistence.drain().await.unwrap();
    }
}

pub(super) fn apply_one(
    sink: &NativeTableMaterializerSink,
    kind: TableKind,
    offset: u64,
) -> std::result::Result<(), NativeIngestSinkError> {
    let batch = RecordBatch::try_new(
        logical_schema(),
        vec![
            Arc::new(StringArray::from(vec![format!("symbol_{offset}")])),
            Arc::new(Int64Array::from(vec![offset as i64])),
        ],
    )
    .unwrap();
    let mutation = match kind {
        TableKind::Append => TableMutation::Append(batch),
        TableKind::Frame => TableMutation::Frame(batch),
        TableKind::KeyValue => TableMutation::KeyValue(KeyValueMutation::Upsert(batch)),
    };
    sink.apply(
        NativeIngestApplyContext {
            identity: IngestWalIdentity {
                table_id: table_id(),
                producer_id: ProducerId::from_u128(1).unwrap(),
                producer_epoch: 0,
            },
            schema_version: 1,
            receipt: WriteReceipt::new(
                RequestId::from_u128(u128::from(offset) + 100).unwrap(),
                offset,
                1,
                Some(OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset)).unwrap()),
                offset
                    .checked_sub(1)
                    .map_or(OffsetWatermark::Empty, |previous| {
                        OffsetWatermark::Known(LogOffset::new(previous))
                    }),
                OffsetWatermark::Known(LogOffset::new(offset)),
                Durability::Memory,
                None,
            )
            .unwrap(),
            replay: false,
        },
        &mutation,
    )
}

async fn checkpoint_interleaving(kind: TableKind) {
    let fixture = Fixture::start(kind).await;
    let sink = Arc::clone(&fixture.sink);
    tokio::task::spawn_blocking(move || apply_one(&sink, kind, 0))
        .await
        .unwrap()
        .unwrap();
    let table = fixture.sink.table(table_id()).unwrap();
    let (reached, persisted) = mpsc::channel();
    let (resume, resume_receiver) = mpsc::channel();
    *table.checkpoint_pause.lock().unwrap() = Some(CheckpointPause {
        reached,
        resume: resume_receiver,
    });
    let sink = Arc::clone(&fixture.sink);
    let checkpoint = tokio::task::spawn_blocking(move || {
        sink.prepare_checkpoint(table_id(), OffsetWatermark::Known(LogOffset::new(0)))
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let checkpoint = tokio::spawn(checkpoint);
    tokio::task::spawn_blocking(move || persisted.recv().unwrap())
        .await
        .unwrap();
    let advanced = table.actor.snapshot().await.unwrap();
    assert_eq!(
        advanced.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );

    // The old implementation releases the state lock before persistence. Force the entire
    // competing write into that window. A corrected implementation keeps this lock, so the
    // competing write must instead wait for reclamation before it can build its publication.
    let checkpoint_owns_state = match table.state.try_lock() {
        Ok(state) => {
            drop(state);
            false
        }
        Err(std::sync::TryLockError::WouldBlock) => true,
        Err(error) => panic!("checkpoint state lock failed: {error}"),
    };
    let (started, start_receiver) = mpsc::channel();
    let sink = Arc::clone(&fixture.sink);
    let write = tokio::task::spawn_blocking(move || {
        started.send(()).unwrap();
        apply_one(&sink, kind, 1)
    });
    tokio::task::spawn_blocking(move || start_receiver.recv().unwrap())
        .await
        .unwrap();
    let write_result = if checkpoint_owns_state {
        resume.send(()).unwrap();
        write.await.unwrap()
    } else {
        let result = write.await.unwrap();
        resume.send(()).unwrap();
        result
    };
    let checkpoint_result = checkpoint.await.unwrap();
    if let Err(
        NativeIngestSinkError::Rejected(error) | NativeIngestSinkError::Indeterminate(error),
    ) = write_result
    {
        panic!("interleaved write must succeed: {error}; checkpoint result: {checkpoint_result:?}");
    }
    checkpoint_result.unwrap();
    let snapshot = table.actor.snapshot().await.unwrap();
    assert_eq!(
        snapshot.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    assert_eq!(snapshot.live_segments.len(), 1);
    assert_eq!(
        snapshot.live_segments[0].offsets().first(),
        LogOffset::new(1)
    );
    assert_eq!(
        snapshot.live_segments[0].offsets().last(),
        LogOffset::new(1)
    );
    {
        let state = table.state.lock().unwrap();
        assert_eq!(state.persisted_log_offset, snapshot.committed_log_offset);
        assert_eq!(
            state.last_applied_log_offset,
            OffsetWatermark::Known(LogOffset::new(1))
        );
        assert_eq!(state.retained_start, 1);
        assert_eq!(state.live_rows, 1);
        build_publication(&table, &state, state.live_rows)
            .expect("physical and logical rows agree");
    }
    fixture.drain().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_publication_interleaving_append() {
    checkpoint_interleaving(TableKind::Append).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_publication_interleaving_key_value() {
    checkpoint_interleaving(TableKind::KeyValue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_publication_interleaving_frame() {
    checkpoint_interleaving(TableKind::Frame).await;
}

async fn writes_progress_during_slow_checkpoint(kind: TableKind) {
    let fixture = Fixture::start(kind).await;
    let sink = Arc::clone(&fixture.sink);
    tokio::task::spawn_blocking(move || apply_one(&sink, kind, 0))
        .await
        .unwrap()
        .unwrap();
    let table = fixture.sink.table(table_id()).unwrap();
    let (reached, paused) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *fixture.persist_pause.lock().unwrap() = Some(CatalogCommitPause {
        reached,
        resume: resumed,
    });
    let sink = Arc::clone(&fixture.sink);
    let checkpoint = tokio::task::spawn_blocking(move || {
        sink.prepare_checkpoint(table_id(), OffsetWatermark::Known(LogOffset::new(0)))
    })
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let checkpoint = tokio::spawn(checkpoint);
    tokio::time::timeout(Duration::from_secs(10), paused)
        .await
        .expect("checkpoint reaches its paused Catalog commit")
        .unwrap();

    let before = table.actor.snapshot().await.unwrap();
    assert_eq!(before.committed_log_offset, OffsetWatermark::Empty);
    let (started, start_receiver) = oneshot::channel();
    let sink = Arc::clone(&fixture.sink);
    let mut write = tokio::task::spawn_blocking(move || {
        started.send(()).unwrap();
        apply_one(&sink, kind, 1)
    });
    start_receiver.await.unwrap();
    // The channel proves persistence is paused before the competing write starts. This
    // timeout is only a deadlock/failure bound, not a delay used to manufacture an ordering.
    let completion = tokio::time::timeout(Duration::from_secs(2), &mut write).await;
    let write_finished_while_paused = completion.is_ok();
    let during = if write_finished_while_paused {
        Some(table.actor.snapshot().await.unwrap())
    } else {
        None
    };

    // Always unblock and drain the real pipeline before failing the old implementation.
    resume.send(()).unwrap();
    let write_result = match completion {
        Ok(result) => result.unwrap(),
        Err(_) => write.await.unwrap(),
    };
    let checkpoint_result = checkpoint.await.unwrap();
    let after = table.actor.snapshot().await.unwrap();
    fixture.drain().await;

    assert!(
        write_finished_while_paused,
        "same-table write must complete while checkpoint Catalog persistence is paused kind=[{kind:?}]"
    );
    write_result.expect("write completes successfully during slow persistence");
    checkpoint_result.unwrap();
    let during = during.unwrap();
    assert_eq!(during.committed_log_offset, OffsetWatermark::Empty);
    assert_eq!(
        during.max_log_offset,
        OffsetWatermark::Known(LogOffset::new(1))
    );
    let during_offsets = during
        .live_segments
        .iter()
        .flat_map(|segment| segment.offsets().first().get()..=segment.offsets().last().get())
        .collect::<Vec<_>>();
    assert_eq!(
        during_offsets,
        vec![0, 1],
        "paused persistence leaves a continuous live prefix"
    );
    assert_eq!(
        after.committed_log_offset,
        OffsetWatermark::Known(LogOffset::new(0))
    );
    assert_eq!(after.live_segments.len(), 1);
    assert_eq!(after.live_segments[0].offsets().first(), LogOffset::new(1));
    assert_eq!(after.live_segments[0].offsets().last(), LogOffset::new(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_slow_persistence_allows_append_writes() {
    writes_progress_during_slow_checkpoint(TableKind::Append).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_slow_persistence_allows_frame_writes() {
    writes_progress_during_slow_checkpoint(TableKind::Frame).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_slow_persistence_allows_key_value_writes() {
    writes_progress_during_slow_checkpoint(TableKind::KeyValue).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publication_rejection_preserves_snapshot_but_leaves_unpublished_physical_rows() {
    let mut fixture = Fixture::start(TableKind::Append).await;
    let sink = Arc::clone(&fixture.sink);
    tokio::task::spawn_blocking(move || apply_one(&sink, TableKind::Append, 0))
        .await
        .unwrap()
        .unwrap();
    let table = fixture.sink.table(table_id()).unwrap();
    let published = table.actor.snapshot().await.unwrap();
    fixture.actors.close_admission();
    let sink = Arc::clone(&fixture.sink);
    let result = tokio::task::spawn_blocking(move || apply_one(&sink, TableKind::Append, 1))
        .await
        .unwrap();
    let Err(NativeIngestSinkError::Indeterminate(error)) = result else {
        panic!("publication rejection after physical append must remain indeterminate");
    };
    assert_eq!(error.code(), ErrorCode::Unavailable);
    {
        let state = table.state.lock().unwrap();
        let physical_rows = state
            .epochs
            .iter()
            .map(|epoch| {
                epoch
                    .materializer
                    .live_segments()
                    .unwrap()
                    .iter()
                    .map(|segment| segment.row_count())
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert_eq!(physical_rows, 2);
        assert_eq!(state.live_rows, 1);
        assert_eq!(
            state.last_applied_log_offset,
            OffsetWatermark::Known(LogOffset::new(0))
        );
        let mismatch = build_publication(&table, &state, state.live_rows).unwrap_err();
        assert_eq!(mismatch.code(), ErrorCode::Internal);
        assert_eq!(
            mismatch.message(),
            "native table materialized row count mismatch expected=[1] actual=[2]",
        );
    }
    assert_eq!(published.live_segments.len(), 1);
    assert_eq!(
        published.live_segments[0].offsets().first(),
        LogOffset::new(0)
    );
    assert_eq!(
        published.live_segments[0].offsets().last(),
        LogOffset::new(0)
    );
    assert_eq!(published.live_segments[0].span().row_count(), 1);
    fixture.drain().await;
}

#[test]
fn checkpoint_persistence_progresses_with_one_blocking_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(15),
            writes_progress_during_slow_checkpoint(TableKind::Append),
        )
        .await
    });
    // A regression may leave a nested spawn_blocking worker waiting forever. A bounded
    // shutdown preserves a deterministic failure instead of hanging the complete test run.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result.expect("checkpoint must progress without nesting a blocked pool worker");
}
