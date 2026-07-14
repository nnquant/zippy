//! Asynchronous Catalog traits backed by isolated SQLite writer and read workers.

use std::collections::VecDeque;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, TrySendError};
use rusqlite::Connection;
use tokio::sync::{oneshot, Semaphore};
use zippy_types::catalog::{
    CatalogCommandReplayProbe, CatalogDataFileReferenceSnapshot, CatalogFileRead,
    CatalogGarbageCollection, CatalogGarbageCollectionGuard, CatalogIdempotencyRead,
    CatalogPublicationControl, CatalogPublicationExclusion, CatalogPublicationPermit,
    CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector, CatalogWrite,
    ClaimGarbageCollectionFilesRequest, ClaimedGarbageCollectionFiles,
    FinalizeGarbageCollectionFilesRequest, FinalizedGarbageCollectionFiles,
    LegacyCatalogImportEnvelopeV2, LegacyCatalogImportReplayProbeV2, LegacyCatalogImportResponseV2,
    LegacyCatalogReconcileEnvelopeV3, LegacyCatalogReconcileResponseV3,
    LegacyFileViewSnapshotMapping, ListNamespacesRequest, ListNamespacesResponse,
    ListTablesRequest, ListTablesResponse, ManagedDataFileProbe, NamespaceRef,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, ErrorCode, RequestId, Result, ServerError};

use crate::SqliteCatalogConfig;
use crate::{
    domain, file_domain, legacy_import_domain, legacy_reconcile_domain, manifest_domain,
    snapshot_domain, table_domain,
};

const SERVICE_ACTOR_THREAD_NAME: &str = "zippy-catalog-service";
const MANIFEST_PREPARATION_THREAD_NAME: &str = "zippy-catalog-manifest-prepare";
const CONTROL_READ_THREAD_NAME: &str = "zippy-catalog-control-read";
const SNAPSHOT_READ_THREAD_NAME: &str = "zippy-catalog-snapshot-read";
const REPLAY_PROBE_CAPACITY: usize = 4;
const LEGACY_IMPORT_AVAILABLE: u8 = 0;
const LEGACY_IMPORT_ACTIVE: u8 = 1;
const LEGACY_IMPORT_SEALED: u8 = 2;

/// A typed SQLite Catalog with thread-confined writer and read-only connections.
///
/// Catalog trait futures submit through a bounded nonblocking queue and await a
/// Tokio oneshot response. SQLite work therefore never blocks an async executor
/// thread. Every trait future is cold: constructing and dropping it before its
/// first poll neither occupies the actor queue nor executes any operation. Once
/// a command future has been polled and successfully enqueued, cancelling it
/// abandons only the response; the actor still executes the command. Callers
/// must treat that case as commit-unknown and replay the same request ID.
/// Cancelled reads may be skipped before execution.
///
/// Mutations retain their actor admission order. While a manifest mutation is
/// being prepared outside SQLite, a later read may complete first and observe
/// the preceding committed revision. A caller requiring read-after-write must
/// await the mutation response before issuing the read. `open`, `close`, and
/// `Drop` remain synchronous in this migration slice and must not be wired into
/// the server runtime until shutdown receives an absolute deadline.
pub struct SqliteCatalog {
    sender: Option<Sender<ServiceMessage>>,
    writer_endpoint: Arc<CatalogWriterEndpoint>,
    actor: Option<JoinHandle<()>>,
    read_pool: Option<CatalogReadPool>,
    command_budget: Arc<CommandBudget>,
    manifest_queue_budget: Arc<ManifestQueueBudget>,
    replay_probe_budget: Arc<ReplayProbeBudget>,
    publication_gate: Arc<Semaphore>,
    publication_gate_capacity: u32,
    runtime_snapshot_pins: Arc<crate::runtime_pins::RuntimeSnapshotPinRegistry>,
    ownership: Arc<zippy_core::OwnedDataDirectory>,
    legacy_import_state: AtomicU8,
}

/// Exclusive startup capability for verified V2 legacy table imports.
///
/// The handle is bound to one running Catalog instance, cannot be cloned, and permanently seals
/// that instance's import window when dropped. Ordinary Catalog mutation admission also seals an
/// unused window before executing, so V2 imports cannot be opened after normal service writes have
/// started. The handle is intentionally not an RPC or serializable authorization token.
pub struct SqliteCatalogLegacyImportSession<'catalog> {
    catalog: &'catalog SqliteCatalog,
    active: bool,
}

impl fmt::Debug for SqliteCatalogLegacyImportSession<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteCatalogLegacyImportSession")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

/// Cloneable read-only view over one running [`SqliteCatalog`].
///
/// The handle owns only bounded read-lane senders and shared admission state;
/// it never owns or borrows the parent Catalog. Retaining a handle therefore
/// cannot keep a Catalog owner alive or prevent `Arc::try_unwrap` during
/// shutdown. Once the owner closes read admission or a read worker stops, all
/// subsequent calls fail with a terminal `Unavailable` error.
#[derive(Clone)]
pub struct SqliteCatalogReadHandle {
    endpoint: Arc<CatalogReadEndpoint>,
    runtime_snapshot_pins: Weak<crate::runtime_pins::RuntimeSnapshotPinRegistry>,
    ownership: Weak<zippy_core::OwnedDataDirectory>,
}

/// Cloneable lifecycle-bound submitter for trusted online legacy file-view reconciliation.
///
/// The handle does not own the Catalog actor. Admission closes before Catalog shutdown, and calls
/// made after owner teardown fail terminally instead of reopening SQLite or extending authority.
#[derive(Clone)]
pub struct SqliteCatalogLegacyReconcileHandle {
    endpoint: Weak<CatalogWriterEndpoint>,
    command_budget: Arc<CommandBudget>,
    manifest_queue_budget: Arc<ManifestQueueBudget>,
    publication_gate: Arc<Semaphore>,
    publication_owner_id: usize,
}

impl SqliteCatalog {
    /// Opens or migrates the Catalog and starts its connection-owning actor.
    ///
    /// Startup is synchronous and returns only after the existing foundation
    /// preflight, durable SQLite configuration, migrations, and integrity check
    /// have completed on the actor thread.
    pub fn open(config: SqliteCatalogConfig) -> Result<Self> {
        Self::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(DomainCatalogReadBackend),
        )
    }

    #[cfg(test)]
    fn open_with_manifest_backend(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
    ) -> Result<Self> {
        Self::open_with_backends(config, manifest_backend, Arc::new(DomainCatalogReadBackend))
    }

    fn open_with_backends(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
        read_backend: Arc<dyn CatalogReadBackend>,
    ) -> Result<Self> {
        crate::validate_config(&config)?;
        let ownership = Arc::clone(config.ownership());
        let runtime_ownership = config
            .ownership
            .acquire_catalog_runtime_ownership()
            .map_err(|error| match error {
                zippy_core::DataDirectoryError::CatalogRuntimeOwnershipHeld { .. } => {
                    ServerError::new(ErrorCode::Conflict, error.to_string()).with_retryable(false)
                }
                _ => ServerError::new(ErrorCode::Internal, error.to_string()).with_retryable(false),
            })?;
        let (sender, receiver) = crossbeam_channel::bounded(config.command_capacity);
        let writer_endpoint = Arc::new(CatalogWriterEndpoint {
            accepting: AtomicBool::new(true),
            sender: sender.clone(),
        });
        let (startup_sender, startup_receiver) = crossbeam_channel::bounded(1);
        let command_budget = Arc::new(CommandBudget::new(config.command_capacity));
        let manifest_queue_budget = Arc::new(ManifestQueueBudget::new(
            config.manifest_queue_capacity_bytes,
        ));
        let replay_probe_budget = Arc::new(ReplayProbeBudget::new(REPLAY_PROBE_CAPACITY));
        let publication_gate_capacity =
            u32::try_from(config.command_capacity).expect("validated Catalog command capacity");
        let publication_gate = Arc::new(Semaphore::new(config.command_capacity));
        let runtime_snapshot_pins = crate::runtime_pins::RuntimeSnapshotPinRegistry::new(
            runtime_ownership,
            config.runtime_snapshot_pin_max_active_pins,
            config.runtime_snapshot_pin_max_active_tables,
        );
        let actor_config = config.clone();
        let actor = thread::Builder::new()
            .name(SERVICE_ACTOR_THREAD_NAME.to_owned())
            .spawn(move || {
                run_actor(actor_config, receiver, startup_sender, manifest_backend);
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start sqlite catalog actor thread error=[{error}]"),
                )
            })?;
        match startup_receiver.recv() {
            Ok(Ok(())) => {
                let mut catalog = Self {
                    read_pool: None,
                    sender: Some(sender),
                    writer_endpoint,
                    actor: Some(actor),
                    command_budget,
                    manifest_queue_budget,
                    replay_probe_budget,
                    publication_gate,
                    publication_gate_capacity,
                    runtime_snapshot_pins,
                    ownership,
                    legacy_import_state: AtomicU8::new(LEGACY_IMPORT_AVAILABLE),
                };
                match CatalogReadPool::start(&config, read_backend) {
                    Ok(read_pool) => {
                        catalog.read_pool = Some(read_pool);
                        Ok(catalog)
                    }
                    Err(error) => {
                        if let Err(cleanup_error) = catalog.shutdown() {
                            tracing::error!(
                                error = %cleanup_error,
                                "sqlite catalog writer cleanup failed after read pool startup error"
                            );
                        }
                        Err(error)
                    }
                }
            }
            Ok(Err(error)) => {
                join_startup_actor(actor)?;
                Err(error)
            }
            Err(_) => {
                join_startup_actor(actor)?;
                Err(actor_stopped())
            }
        }
    }

    /// Gracefully closes SQLite and joins the actor thread synchronously.
    ///
    /// This method is reliable but not deadline-bounded. It is intentionally a
    /// pre-server lifecycle API in the current migration slice.
    pub fn close(mut self) -> Result<()> {
        self.shutdown()
    }

    /// Creates a lifecycle-safe read-only handle for query and file snapshots.
    ///
    /// The returned handle does not extend this Catalog's owner lifetime. It
    /// remains valid only while the Catalog read workers are running; reads
    /// attempted after shutdown fail terminally instead of reopening SQLite or
    /// silently falling back to another metadata authority.
    pub fn read_handle(&self) -> Result<SqliteCatalogReadHandle> {
        let read_pool = self.read_pool()?;
        read_pool.check_runtime_health()?;
        Ok(SqliteCatalogReadHandle {
            endpoint: Arc::clone(&read_pool.endpoint),
            runtime_snapshot_pins: Arc::downgrade(&self.runtime_snapshot_pins),
            ownership: Arc::downgrade(&self.ownership),
        })
    }

    /// Creates a lifecycle-bound online legacy reconcile submitter.
    ///
    /// Creating this handle is an ordinary mutation boundary and permanently seals any unused V2
    /// startup import window. It therefore cannot race an active startup import session.
    pub fn legacy_reconcile_handle(&self) -> Result<SqliteCatalogLegacyReconcileHandle> {
        self.check_runtime_health()?;
        self.seal_legacy_imports_for_ordinary_mutation()?;
        Ok(SqliteCatalogLegacyReconcileHandle {
            endpoint: Arc::downgrade(&self.writer_endpoint),
            command_budget: Arc::clone(&self.command_budget),
            manifest_queue_budget: Arc::clone(&self.manifest_queue_budget),
            publication_gate: Arc::clone(&self.publication_gate),
            publication_owner_id: self.publication_owner_id(),
        })
    }

    /// Resolves one exact per-table legacy revision to its immutable native snapshot mapping.
    pub fn resolve_legacy_file_view_mapping(
        &self,
        table_id: zippy_types::TableId,
        legacy_files_revision: u64,
    ) -> BoxFuture<'_, Result<LegacyFileViewSnapshotMapping>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|response| ControlReadMessage::ResolveLegacyMapping {
                    table_id,
                    legacy_files_revision,
                    response,
                })
            }))
            .await
        })
    }

    /// Atomically resolves the explicit mapping joined to an active table's current cursor.
    pub fn resolve_current_legacy_file_view_mapping(
        &self,
        table_id: zippy_types::TableId,
    ) -> BoxFuture<'_, Result<LegacyFileViewSnapshotMapping>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|response| ControlReadMessage::ResolveCurrentLegacyMapping {
                    table_id,
                    response,
                })
            }))
            .await
        })
    }

    /// Acquires the only V2 legacy-import capability for this Catalog instance.
    ///
    /// Startup orchestration must acquire this handle before submitting ordinary non-migration
    /// writes, retain it through every verified table import, and then explicitly seal it before
    /// publishing server readiness. Dropping the handle has the same fail-closed sealing effect.
    pub fn begin_legacy_import_session(&self) -> Result<SqliteCatalogLegacyImportSession<'_>> {
        self.check_runtime_health()?;
        self.legacy_import_state
            .compare_exchange(
                LEGACY_IMPORT_AVAILABLE,
                LEGACY_IMPORT_ACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|state| {
                let message = if state == LEGACY_IMPORT_ACTIVE {
                    "legacy import session is already active"
                } else {
                    "legacy import window is permanently sealed for this Catalog instance"
                };
                ServerError::new(ErrorCode::Conflict, message).with_retryable(false)
            })?;
        Ok(SqliteCatalogLegacyImportSession {
            catalog: self,
            active: true,
        })
    }

    /// Pins one exact in-process table snapshot against safe garbage collection.
    ///
    /// Pin admission fails while a claim for the same table owns its exclusion. The returned
    /// guard must remain alive through all physical reads that use the selected snapshot.
    pub fn pin_runtime_snapshot(
        &self,
        table_id: zippy_types::TableId,
        snapshot_version: u64,
    ) -> Result<crate::SqliteCatalogRuntimeSnapshotPin> {
        self.check_runtime_health()?;
        self.runtime_snapshot_pins.pin(table_id, snapshot_version)
    }

    /// Returns process-local query snapshot pin gauges, limits, and cumulative rejections.
    ///
    /// The snapshot is lock-free and does not enqueue Catalog or SQLite work.
    #[must_use]
    pub fn runtime_snapshot_pin_status(&self) -> crate::SqliteCatalogRuntimeSnapshotPinStatus {
        self.runtime_snapshot_pins.status()
    }

    /// Excludes new runtime pins for one table and captures its current minimum pin.
    ///
    /// The returned opaque capability is bound to this Catalog and table. It must be moved into
    /// [`CatalogGarbageCollection::claim_garbage_collection_files`]; forged, cross-Catalog, or
    /// cross-table capabilities are rejected before writer admission.
    pub fn acquire_garbage_collection_guard(
        &self,
        table_id: zippy_types::TableId,
    ) -> Result<CatalogGarbageCollectionGuard> {
        self.check_runtime_health()?;
        self.runtime_snapshot_pins
            .exclude_garbage_collection(table_id)
            .map(CatalogGarbageCollectionGuard::from_opaque)
    }

    fn writer_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ServiceMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or_else(actor_stopped)?;
        let (response_sender, response_receiver) = oneshot::channel();
        match sender.try_send(build(response_sender)) {
            Ok(()) => Ok(response_receiver),
            Err(TrySendError::Full(_)) => Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "sqlite catalog actor command queue is full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(actor_stopped()),
        }
    }

    fn read_pool(&self) -> Result<&CatalogReadPool> {
        self.read_pool.as_ref().ok_or_else(read_pool_stopped)
    }

    /// Checks that every thread owned by this Catalog instance is still running.
    ///
    /// This is a local liveness check and does not enqueue SQLite work. A
    /// stopped writer or read worker is terminal for this instance; callers
    /// must close admission and reopen the Catalog instead of retrying forever.
    pub fn check_runtime_health(&self) -> Result<()> {
        if self.sender.is_none()
            || self
                .actor
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        {
            return Err(actor_stopped());
        }
        self.read_pool()
            .and_then(CatalogReadPool::check_runtime_health)
    }

    fn publication_owner_id(&self) -> usize {
        Arc::as_ptr(&self.publication_gate) as usize
    }

    async fn acquire_shared_publication_permit(&self) -> Result<CatalogPublicationPermit> {
        let command_permit = self.command_budget.reserve()?;
        let publication_permit = Arc::clone(&self.publication_gate)
            .acquire_owned()
            .await
            .map_err(|_| publication_gate_stopped())?;
        Ok(CatalogPublicationPermit::from_opaque(
            SqlitePublicationPermit {
                owner_id: self.publication_owner_id(),
                _command_permit: command_permit,
                _publication_permit: publication_permit,
            },
        ))
    }

    async fn submit_command_with_publication_permit(
        &self,
        command: VersionedCatalogCommand,
        publication_permit: CatalogPublicationPermit,
    ) -> Result<VersionedCatalogResponse> {
        let VersionedCatalogCommand::V1(envelope) = &command;
        let request_id = envelope.request_id;
        self.seal_legacy_imports_for_command(envelope.actor.kind, request_id)?;
        let publication_permit = publication_permit
            .try_into_opaque::<SqlitePublicationPermit>()
            .map_err(|_| {
                ServerError::new(
                    ErrorCode::InvalidArgument,
                    "catalog publication permit has an invalid capability type",
                )
                .with_request_id(request_id)
                .with_retryable(false)
            })?;
        if publication_permit.owner_id != self.publication_owner_id() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "catalog publication permit belongs to another Catalog instance",
            )
            .with_request_id(request_id)
            .with_retryable(false));
        }
        let queued_bytes = manifest_domain::manifest_command_preflight_bytes(&command).await?;
        let manifest_permit = self
            .manifest_queue_budget
            .reserve(queued_bytes)
            .map_err(|error| error.with_request_id(request_id))?;
        await_response(self.writer_request(|response| {
            ServiceMessage::Command(CommandRequest {
                command,
                state: CommandRequestState {
                    request_id,
                    response,
                    _manifest_permit: manifest_permit,
                    _publication_permit: publication_permit,
                },
            })
        }))
        .await
    }

    fn seal_legacy_imports_for_command(
        &self,
        actor_kind: zippy_types::catalog::CatalogActorKind,
        request_id: RequestId,
    ) -> Result<()> {
        if actor_kind == zippy_types::catalog::CatalogActorKind::Migration {
            return Ok(());
        }
        self.seal_legacy_imports_for_ordinary_mutation()
            .map_err(|error| error.with_request_id(request_id))
    }

    fn seal_legacy_imports_for_ordinary_mutation(&self) -> Result<()> {
        loop {
            match self.legacy_import_state.load(Ordering::Acquire) {
                LEGACY_IMPORT_AVAILABLE => {
                    if self
                        .legacy_import_state
                        .compare_exchange(
                            LEGACY_IMPORT_AVAILABLE,
                            LEGACY_IMPORT_SEALED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                LEGACY_IMPORT_ACTIVE => {
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        "ordinary Catalog writes are closed while legacy import is active",
                    )
                    .with_retryable(false));
                }
                LEGACY_IMPORT_SEALED => return Ok(()),
                _ => {
                    return Err(ServerError::new(
                        ErrorCode::Internal,
                        "legacy import lifecycle entered an invalid state",
                    )
                    .with_retryable(false));
                }
            }
        }
    }

    fn submit_legacy_import(
        &self,
        command: LegacyCatalogImportEnvelopeV2,
    ) -> BoxFuture<'_, Result<LegacyCatalogImportResponseV2>> {
        Box::pin(async move {
            let request_id = command.request_id;
            let publication_permit = self
                .acquire_shared_publication_permit()
                .await
                .map_err(|error| error.with_request_id(request_id))?;
            let publication_permit = publication_permit
                .try_into_opaque::<SqlitePublicationPermit>()
                .map_err(|_| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "sqlite Catalog created an invalid publication capability",
                    )
                    .with_request_id(request_id)
                    .with_retryable(false)
                })?;
            let queued_bytes =
                legacy_import_domain::legacy_import_preflight_bytes(&command).await?;
            let manifest_permit = self
                .manifest_queue_budget
                .reserve(queued_bytes)
                .map_err(|error| error.with_request_id(request_id))?;
            await_response(self.writer_request(|response| {
                ServiceMessage::LegacyImport(LegacyImportRequest {
                    command,
                    state: LegacyImportRequestState {
                        request_id,
                        response,
                        _manifest_permit: manifest_permit,
                        _publication_permit: publication_permit,
                    },
                })
            }))
            .await
        })
    }

    fn shutdown(&mut self) -> Result<()> {
        self.writer_endpoint.close_admission();
        self.runtime_snapshot_pins.close();
        let read_result = match self.read_pool.take() {
            Some(mut read_pool) => read_pool.shutdown(),
            None => Ok(()),
        };
        let writer_result = self.shutdown_writer();
        read_result.and(writer_result)
    }

    fn shutdown_writer(&mut self) -> Result<()> {
        let Some(sender) = self.sender.take() else {
            return match self.actor.take() {
                Some(actor) => actor.join().map_err(|_| {
                    ServerError::new(ErrorCode::Internal, "sqlite catalog actor thread panicked")
                }),
                None => Ok(()),
            };
        };
        let (response_sender, response_receiver) = crossbeam_channel::bounded(1);
        let send_result = sender
            .send(ServiceMessage::Shutdown(response_sender))
            .map_err(|_| actor_stopped());
        drop(sender);
        let actor_result = match send_result {
            Ok(()) => match response_receiver.recv() {
                Ok(result) => result,
                Err(_) => Err(actor_stopped()),
            },
            Err(error) => Err(error),
        };
        let join_result = match self.actor.take() {
            Some(actor) => actor.join().map_err(|_| {
                ServerError::new(ErrorCode::Internal, "sqlite catalog actor thread panicked")
            }),
            None => Ok(()),
        };
        actor_result.and(join_result)
    }
}

impl CatalogRead for SqliteCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            await_response(
                self.read_pool()
                    .and_then(|pool| pool.control_request(ControlReadMessage::Revision)),
            )
            .await
        })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::GetNamespace(namespace, sender))
            }))
            .await
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::ListNamespaces(request, sender))
            }))
            .await
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::GetTable(table, sender))
            }))
            .await
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::ListTables(request, sender))
            }))
            .await
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            table_domain::validate_table_resolution_request(tables)?;
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| {
                    ControlReadMessage::ResolveTables(tables.to_vec(), sender)
                })
            }))
            .await
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            snapshot_domain::validate_snapshot_selectors(selectors)?;
            let selectors = selectors.to_vec();
            await_response(self.read_pool().and_then(|pool| {
                pool.snapshot_request(|response| {
                    SnapshotReadMessage::Read(SnapshotReadRequest {
                        selectors,
                        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                        response,
                    })
                })
            }))
            .await
        })
    }
}

impl CatalogFileRead for SqliteCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            file_domain::validate_file_probes(probes)?;
            let probes = probes.to_vec();
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::InspectFiles(probes, sender))
            }))
            .await
        })
    }
}

impl SqliteCatalogReadHandle {
    /// Returns query snapshot pin gauges, limits, and cumulative rejections.
    ///
    /// A Catalog that has begun shutdown but still has retained pin guards returns a status with
    /// `admission_open=false`. Once the registry and its final guard have been dropped, this method
    /// returns a terminal `Unavailable` error.
    pub fn runtime_snapshot_pin_status(
        &self,
    ) -> Result<crate::SqliteCatalogRuntimeSnapshotPinStatus> {
        self.runtime_snapshot_pin_registry()
            .map(|registry| registry.status())
    }

    /// Reads exact metadata only after pinning every selected table against physical GC.
    ///
    /// Pin admission precedes the Catalog read. If any table is currently excluded by a GC claim,
    /// or if the read fails, all pins already acquired by this call are released before the error
    /// is returned. The successful result must remain alive through physical file execution.
    pub async fn read_pinned_snapshot_at(
        &self,
        selectors: &[CatalogSnapshotSelector],
    ) -> Result<crate::SqliteCatalogPinnedQuerySnapshot> {
        self.read_pinned_snapshot_at_with_limits(
            selectors,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
        )
        .await
    }

    /// Reads and pins an exact snapshot under stricter caller-selected SQLite copy limits.
    pub async fn read_pinned_snapshot_at_with_limits(
        &self,
        selectors: &[CatalogSnapshotSelector],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
    ) -> Result<crate::SqliteCatalogPinnedQuerySnapshot> {
        snapshot_domain::validate_snapshot_selectors(selectors)?;
        let limits = limits.validate()?;
        let ownership = self.ownership.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "sqlite catalog data-directory ownership is no longer available",
            )
            .with_retryable(false)
        })?;
        let runtime_snapshot_pins = self.runtime_snapshot_pin_registry()?;
        let mut pins = Vec::with_capacity(selectors.len());
        for selector in selectors {
            pins.push(runtime_snapshot_pins.pin(selector.table_id, selector.snapshot_version)?);
        }
        let snapshot = self.read_snapshot_at_with_limits(selectors, limits).await?;
        Ok(crate::SqliteCatalogPinnedQuerySnapshot::new(
            snapshot, pins, ownership,
        ))
    }

    fn runtime_snapshot_pin_registry(
        &self,
    ) -> Result<Arc<crate::runtime_pins::RuntimeSnapshotPinRegistry>> {
        self.runtime_snapshot_pins.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "sqlite catalog runtime snapshot pin registry is no longer available",
            )
            .with_retryable(false)
        })
    }

    /// Reads exact metadata with a per-request bound below the Catalog hard limits.
    ///
    /// This method does not protect physical files after the SQLite transaction ends. It is for
    /// metadata-only inspection such as asynchronous authority comparison; physical query plans
    /// must use [`Self::read_pinned_snapshot_at_with_limits`].
    pub async fn read_snapshot_at_with_limits(
        &self,
        selectors: &[CatalogSnapshotSelector],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
    ) -> Result<CatalogQuerySnapshot> {
        snapshot_domain::validate_snapshot_selectors(selectors)?;
        let limits = limits.validate()?;
        let selectors = selectors.to_vec();
        await_response(self.endpoint.snapshot_request(|response| {
            SnapshotReadMessage::Read(SnapshotReadRequest {
                selectors,
                limits,
                response,
            })
        }))
        .await
    }

    /// Resolves one exact per-table legacy revision to its immutable native snapshot mapping.
    pub async fn resolve_legacy_file_view_mapping(
        &self,
        table_id: zippy_types::TableId,
        legacy_files_revision: u64,
    ) -> Result<LegacyFileViewSnapshotMapping> {
        await_response(self.endpoint.control_request(|response| {
            ControlReadMessage::ResolveLegacyMapping {
                table_id,
                legacy_files_revision,
                response,
            }
        }))
        .await
    }

    /// Atomically resolves the explicit mapping joined to an active table's current cursor.
    ///
    /// A table with no mapping for its current snapshot returns `NotFound`; a present mapping with
    /// malformed identity or fixed-width fields returns `DataCorruption`.
    pub async fn resolve_current_legacy_file_view_mapping(
        &self,
        table_id: zippy_types::TableId,
    ) -> Result<LegacyFileViewSnapshotMapping> {
        await_response(self.endpoint.control_request(|response| {
            ControlReadMessage::ResolveCurrentLegacyMapping { table_id, response }
        }))
        .await
    }
}

impl CatalogRead for SqliteCatalogReadHandle {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            await_response(self.endpoint.control_request(ControlReadMessage::Revision)).await
        })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            await_response(
                self.endpoint
                    .control_request(|sender| ControlReadMessage::GetNamespace(namespace, sender)),
            )
            .await
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            await_response(
                self.endpoint
                    .control_request(|sender| ControlReadMessage::ListNamespaces(request, sender)),
            )
            .await
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            await_response(
                self.endpoint
                    .control_request(|sender| ControlReadMessage::GetTable(table, sender)),
            )
            .await
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            await_response(
                self.endpoint
                    .control_request(|sender| ControlReadMessage::ListTables(request, sender)),
            )
            .await
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            table_domain::validate_table_resolution_request(tables)?;
            await_response(self.endpoint.control_request(|sender| {
                ControlReadMessage::ResolveTables(tables.to_vec(), sender)
            }))
            .await
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            self.read_snapshot_at_with_limits(
                selectors,
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
            )
            .await
        })
    }
}

impl CatalogFileRead for SqliteCatalogReadHandle {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            file_domain::validate_file_probes(probes)?;
            let probes = probes.to_vec();
            await_response(
                self.endpoint
                    .control_request(|sender| ControlReadMessage::InspectFiles(probes, sender)),
            )
            .await
        })
    }
}

impl CatalogIdempotencyRead for SqliteCatalog {
    fn probe_command_replay<'a>(
        &'a self,
        command: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
        Box::pin(async move {
            let VersionedCatalogCommand::V1(envelope) = command;
            let request_id = envelope.request_id;
            let probe_permit = self
                .replay_probe_budget
                .reserve()
                .map_err(|error| error.with_request_id(request_id))?;
            let (key, probe_permit) = verify_replay_probe_command(
                request_id,
                command.clone(),
                probe_permit,
                domain::verify_command_replay_key,
            )
            .await?;
            let result = await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|response| {
                    ControlReadMessage::ProbeCommandReplay(CommandReplayProbeRequest {
                        key,
                        response,
                        probe_permit,
                    })
                })
            }))
            .await;
            result.map_err(|error| error.with_request_id(request_id))
        })
    }
}

impl CatalogWrite for SqliteCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let VersionedCatalogCommand::V1(envelope) = &command;
            let request_id = envelope.request_id;
            let permit = self
                .acquire_shared_publication_permit()
                .await
                .map_err(|error| error.with_request_id(request_id))?;
            self.submit_command_with_publication_permit(command, permit)
                .await
        })
    }
}

impl SqliteCatalogLegacyImportSession<'_> {
    /// Probes one V2 request ID through the bounded control-read lane.
    ///
    /// This lookup does not reserve publication or manifest-preparation capacity and does not
    /// validate a new import command. The returned response has only passed intrinsic permanent
    /// record checks; startup orchestration must compare it with its current migration plan.
    pub fn probe_legacy_import_v2(
        &self,
        request_id: RequestId,
    ) -> BoxFuture<'_, Result<LegacyCatalogImportReplayProbeV2>> {
        if !self.active
            || self.catalog.legacy_import_state.load(Ordering::Acquire) != LEGACY_IMPORT_ACTIVE
        {
            return Box::pin(async {
                Err(ServerError::new(
                    ErrorCode::Unavailable,
                    "legacy import session is no longer active",
                )
                .with_retryable(false))
            });
        }
        Box::pin(async move {
            let probe_permit = self
                .catalog
                .replay_probe_budget
                .reserve()
                .map_err(|error| error.with_request_id(request_id))?;
            let result = await_response(self.catalog.read_pool().and_then(|pool| {
                pool.control_request(|response| {
                    ControlReadMessage::ProbeLegacyImport(LegacyImportReplayProbeRequest {
                        request_id,
                        response,
                        probe_permit,
                    })
                })
            }))
            .await;
            result.map_err(|error| error.with_request_id(request_id))
        })
    }

    /// Imports one fully verified legacy table or replays its exact permanent response.
    pub fn import_legacy_table_v2(
        &self,
        command: LegacyCatalogImportEnvelopeV2,
    ) -> BoxFuture<'_, Result<LegacyCatalogImportResponseV2>> {
        if !self.active
            || self.catalog.legacy_import_state.load(Ordering::Acquire) != LEGACY_IMPORT_ACTIVE
        {
            return Box::pin(async {
                Err(ServerError::new(
                    ErrorCode::Unavailable,
                    "legacy import session is no longer active",
                )
                .with_retryable(false))
            });
        }
        self.catalog.submit_legacy_import(command)
    }

    /// Permanently closes the V2 import window before server admission becomes visible.
    pub fn seal(mut self) {
        self.active = false;
        self.catalog
            .legacy_import_state
            .store(LEGACY_IMPORT_SEALED, Ordering::Release);
    }
}

impl SqliteCatalogLegacyReconcileHandle {
    /// Atomically reconciles one existing migrated table to a verified full legacy file view.
    ///
    /// Exact request replays return the durable mapping without advancing Catalog revision. Queue,
    /// byte, and publication admission are bounded by the owning Catalog configuration.
    pub fn reconcile_legacy_table_v3(
        &self,
        command: LegacyCatalogReconcileEnvelopeV3,
    ) -> BoxFuture<'_, Result<LegacyCatalogReconcileResponseV3>> {
        Box::pin(async move {
            let request_id = command.request_id;
            let endpoint = self.endpoint.upgrade().ok_or_else(actor_stopped)?;
            endpoint.check_admission()?;
            let command_permit = self
                .command_budget
                .reserve()
                .map_err(|error| error.with_request_id(request_id))?;
            let publication_permit = Arc::clone(&self.publication_gate)
                .acquire_owned()
                .await
                .map_err(|_| publication_gate_stopped())?;
            let publication_permit = SqlitePublicationPermit {
                owner_id: self.publication_owner_id,
                _command_permit: command_permit,
                _publication_permit: publication_permit,
            };
            let queued_bytes =
                legacy_reconcile_domain::legacy_reconcile_preflight_bytes(&command).await?;
            let manifest_permit = self
                .manifest_queue_budget
                .reserve(queued_bytes)
                .map_err(|error| error.with_request_id(request_id))?;
            await_response(endpoint.writer_request(|response| {
                ServiceMessage::LegacyReconcile(LegacyReconcileRequest {
                    command,
                    state: LegacyReconcileRequestState {
                        request_id,
                        response,
                        _manifest_permit: manifest_permit,
                        _publication_permit: publication_permit,
                    },
                })
            }))
            .await
        })
    }
}

impl Drop for SqliteCatalogLegacyImportSession<'_> {
    fn drop(&mut self) {
        if self.active {
            self.catalog
                .legacy_import_state
                .store(LEGACY_IMPORT_SEALED, Ordering::Release);
            self.active = false;
        }
    }
}

impl CatalogPublicationControl for SqliteCatalog {
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>> {
        Box::pin(self.acquire_shared_publication_permit())
    }

    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(self.submit_command_with_publication_permit(command, permit))
    }

    fn acquire_publication_exclusion(&self) -> BoxFuture<'_, Result<CatalogPublicationExclusion>> {
        Box::pin(async move {
            let permit = Arc::clone(&self.publication_gate)
                .acquire_many_owned(self.publication_gate_capacity)
                .await
                .map_err(|_| publication_gate_stopped())?;
            Ok(CatalogPublicationExclusion::from_opaque(permit))
        })
    }
}

impl CatalogGarbageCollection for SqliteCatalog {
    fn claim_garbage_collection_files(
        &self,
        guard: CatalogGarbageCollectionGuard,
        request: ClaimGarbageCollectionFilesRequest,
    ) -> BoxFuture<'_, Result<ClaimedGarbageCollectionFiles>> {
        Box::pin(async move {
            self.seal_legacy_imports_for_ordinary_mutation()?;
            let guard = guard
                .try_into_opaque::<crate::runtime_pins::SqliteGarbageCollectionGuard>()
                .map_err(|_| {
                    ServerError::new(
                        ErrorCode::InvalidArgument,
                        "garbage collection guard has an invalid capability type",
                    )
                    .with_table_id(request.table_id)
                    .with_retryable(false)
                })?;
            if guard.owner_id() != self.runtime_snapshot_pins.owner_id() {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "garbage collection guard belongs to another Catalog instance",
                )
                .with_table_id(request.table_id)
                .with_retryable(false));
            }
            if guard.table_id() != request.table_id {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "garbage collection guard belongs to another table",
                )
                .with_table_id(request.table_id)
                .with_retryable(false));
            }
            crate::gc_domain::validate_claim_request(
                &request,
                guard.runtime_min_pinned_snapshot(),
            )?;
            let command_permit = self.command_budget.reserve()?;
            await_response(self.writer_request(|response| {
                ServiceMessage::ClaimGarbageCollection(GarbageCollectionClaimRequest {
                    guard,
                    request,
                    response,
                    _command_permit: command_permit,
                })
            }))
            .await
        })
    }

    fn finalize_garbage_collection_files(
        &self,
        request: FinalizeGarbageCollectionFilesRequest,
    ) -> BoxFuture<'_, Result<FinalizedGarbageCollectionFiles>> {
        Box::pin(async move {
            self.seal_legacy_imports_for_ordinary_mutation()?;
            crate::gc_domain::validate_finalize_request(&request)?;
            let command_permit = self.command_budget.reserve()?;
            await_response(self.writer_request(|response| {
                ServiceMessage::FinalizeGarbageCollection(GarbageCollectionFinalizeRequest {
                    request,
                    response,
                    _command_permit: command_permit,
                })
            }))
            .await
        })
    }
}

struct CommandBudget {
    used: AtomicUsize,
    maximum: usize,
}

impl CommandBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            maximum,
        }
    }

    fn reserve(self: &Arc<Self>) -> Result<CommandPermit> {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if current >= self.maximum {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "sqlite catalog outstanding command budget exhausted used=[{current}] maximum=[{}]",
                        self.maximum
                    ),
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(CommandPermit {
                        budget: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct CommandPermit {
    budget: Arc<CommandBudget>,
}

struct SqlitePublicationPermit {
    owner_id: usize,
    _command_permit: CommandPermit,
    _publication_permit: tokio::sync::OwnedSemaphorePermit,
}

struct ReplayProbeBudget {
    used: AtomicUsize,
    maximum: usize,
}

impl ReplayProbeBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            maximum,
        }
    }

    fn reserve(self: &Arc<Self>) -> Result<ReplayProbePermit> {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if current >= self.maximum {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "sqlite catalog replay probe budget exhausted used=[{current}] maximum=[{}]",
                        self.maximum
                    ),
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ReplayProbePermit {
                        budget: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct ReplayProbePermit {
    budget: Arc<ReplayProbeBudget>,
}

impl Drop for ReplayProbePermit {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

impl Drop for CommandPermit {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

struct ManifestQueueBudget {
    used: AtomicUsize,
    maximum: usize,
}

impl ManifestQueueBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            maximum,
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Option<ManifestQueuePermit>> {
        if bytes == 0 {
            return Ok(None);
        }
        if bytes > self.maximum {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "sqlite catalog manifest command exceeds queue byte budget requested=[{bytes}] maximum=[{}]",
                    self.maximum
                ),
            )
            .with_retryable(false));
        }
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "sqlite catalog manifest queue byte reservation overflow",
                )
                .with_retryable(false)
            })?;
            if next > self.maximum {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "sqlite catalog manifest queue byte budget exhausted used=[{current}] requested=[{bytes}] maximum=[{}]",
                        self.maximum
                    ),
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Some(ManifestQueuePermit {
                        budget: Arc::clone(self),
                        bytes,
                    }));
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct ManifestQueuePermit {
    budget: Arc<ManifestQueueBudget>,
    bytes: usize,
}

impl Drop for ManifestQueuePermit {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}

trait CatalogReadBackend: Send + Sync + 'static {
    fn revision(&self, connection: &Connection) -> Result<u64> {
        domain::revision(connection)
    }

    fn get_namespace(
        &self,
        connection: &Connection,
        namespace: NamespaceRef,
    ) -> Result<NamespaceDescriptor> {
        domain::get_namespace(connection, namespace)
    }

    fn list_namespaces(
        &self,
        connection: &mut Connection,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        domain::list_namespaces(connection, request)
    }

    fn get_table(&self, connection: &Connection, table: TableRef) -> Result<TableDescriptor> {
        table_domain::get_table(connection, table)
    }

    fn list_tables(
        &self,
        connection: &mut Connection,
        request: ListTablesRequest,
    ) -> Result<ListTablesResponse> {
        table_domain::list_tables(connection, request)
    }

    fn resolve_tables(
        &self,
        connection: &mut Connection,
        tables: &[TableRef],
    ) -> Result<Vec<TableDescriptor>> {
        table_domain::resolve_tables(connection, tables)
    }

    fn read_snapshot(
        &self,
        connection: &mut Connection,
        selectors: &[CatalogSnapshotSelector],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
    ) -> Result<CatalogQuerySnapshot> {
        snapshot_domain::read_snapshot_at_with_limits(connection, selectors, limits)
    }

    fn inspect_data_file_references(
        &self,
        connection: &mut Connection,
        probes: &[ManagedDataFileProbe],
    ) -> Result<CatalogDataFileReferenceSnapshot> {
        file_domain::inspect_data_file_references(connection, probes)
    }

    fn probe_command_replay(
        &self,
        connection: &Connection,
        key: &domain::VerifiedCommandReplayKey,
    ) -> Result<CatalogCommandReplayProbe> {
        domain::probe_command_replay(connection, key)
    }

    fn probe_legacy_import_v2(
        &self,
        connection: &mut Connection,
        request_id: RequestId,
    ) -> Result<LegacyCatalogImportReplayProbeV2> {
        legacy_import_domain::probe_legacy_catalog_import_v2(connection, request_id)
    }

    fn resolve_legacy_file_view_mapping(
        &self,
        connection: &Connection,
        table_id: zippy_types::TableId,
        legacy_files_revision: u64,
    ) -> Result<LegacyFileViewSnapshotMapping> {
        legacy_reconcile_domain::read_legacy_file_view_mapping(
            connection,
            table_id,
            legacy_files_revision,
        )?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::NotFound,
                "exact legacy file-view revision mapping was not found",
            )
            .with_table_id(table_id)
            .with_retryable(false)
        })
    }

    fn resolve_current_legacy_file_view_mapping(
        &self,
        connection: &Connection,
        table_id: zippy_types::TableId,
    ) -> Result<LegacyFileViewSnapshotMapping> {
        legacy_reconcile_domain::read_current_legacy_file_view_mapping(connection, table_id)
    }
}

struct DomainCatalogReadBackend;

impl CatalogReadBackend for DomainCatalogReadBackend {}

enum ControlReadMessage {
    Revision(oneshot::Sender<Result<u64>>),
    GetNamespace(NamespaceRef, oneshot::Sender<Result<NamespaceDescriptor>>),
    ListNamespaces(
        ListNamespacesRequest,
        oneshot::Sender<Result<ListNamespacesResponse>>,
    ),
    GetTable(TableRef, oneshot::Sender<Result<TableDescriptor>>),
    ListTables(
        ListTablesRequest,
        oneshot::Sender<Result<ListTablesResponse>>,
    ),
    ResolveTables(Vec<TableRef>, oneshot::Sender<Result<Vec<TableDescriptor>>>),
    InspectFiles(
        Vec<ManagedDataFileProbe>,
        oneshot::Sender<Result<CatalogDataFileReferenceSnapshot>>,
    ),
    ProbeCommandReplay(CommandReplayProbeRequest),
    ProbeLegacyImport(LegacyImportReplayProbeRequest),
    ResolveLegacyMapping {
        table_id: zippy_types::TableId,
        legacy_files_revision: u64,
        response: oneshot::Sender<Result<LegacyFileViewSnapshotMapping>>,
    },
    ResolveCurrentLegacyMapping {
        table_id: zippy_types::TableId,
        response: oneshot::Sender<Result<LegacyFileViewSnapshotMapping>>,
    },
    Shutdown,
}

struct CommandReplayProbeRequest {
    key: domain::VerifiedCommandReplayKey,
    response: oneshot::Sender<Result<CatalogCommandReplayProbe>>,
    probe_permit: ReplayProbePermit,
}

struct LegacyImportReplayProbeRequest {
    request_id: RequestId,
    response: oneshot::Sender<Result<LegacyCatalogImportReplayProbeV2>>,
    probe_permit: ReplayProbePermit,
}

enum SnapshotReadMessage {
    Read(SnapshotReadRequest),
    Shutdown,
}

struct SnapshotReadRequest {
    selectors: Vec<CatalogSnapshotSelector>,
    limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
    response: oneshot::Sender<Result<CatalogQuerySnapshot>>,
}

struct CatalogReadEndpoint {
    accepting: AtomicBool,
    control_sender: Sender<ControlReadMessage>,
    snapshot_sender: Sender<SnapshotReadMessage>,
}

struct CatalogWriterEndpoint {
    accepting: AtomicBool,
    sender: Sender<ServiceMessage>,
}

impl CatalogWriterEndpoint {
    fn check_admission(&self) -> Result<()> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(actor_stopped())
        }
    }

    fn writer_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ServiceMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        self.check_admission()?;
        let (response_sender, response_receiver) = oneshot::channel();
        match self.sender.try_send(build(response_sender)) {
            Ok(()) => Ok(response_receiver),
            Err(TrySendError::Full(_)) => Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "sqlite catalog actor command queue is full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(actor_stopped()),
        }
    }

    fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }
}

impl CatalogReadEndpoint {
    fn control_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ControlReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(read_pool_stopped());
        }
        submit_read_request(
            Some(&self.control_sender),
            build,
            "sqlite catalog control read queue is full",
        )
    }

    fn snapshot_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> SnapshotReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(read_pool_stopped());
        }
        submit_read_request(
            Some(&self.snapshot_sender),
            build,
            "sqlite catalog snapshot read queue is full",
        )
    }

    fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }
}

struct CatalogReadPool {
    endpoint: Arc<CatalogReadEndpoint>,
    control_actor: Option<JoinHandle<Result<()>>>,
    snapshot_actor: Option<JoinHandle<Result<()>>>,
}

impl CatalogReadPool {
    fn start(config: &SqliteCatalogConfig, backend: Arc<dyn CatalogReadBackend>) -> Result<Self> {
        let (control_sender, control_receiver) =
            crossbeam_channel::bounded(config.control_read_capacity);
        let control_actor = start_read_worker(
            CONTROL_READ_THREAD_NAME,
            config.clone(),
            control_receiver,
            Arc::clone(&backend),
            control_read_loop,
        )?;

        let (snapshot_sender, snapshot_receiver) =
            crossbeam_channel::bounded(config.snapshot_read_capacity);
        let snapshot_actor = match start_read_worker(
            SNAPSHOT_READ_THREAD_NAME,
            config.clone(),
            snapshot_receiver,
            backend,
            snapshot_read_loop,
        ) {
            Ok(actor) => actor,
            Err(error) => {
                drop(control_sender);
                if let Err(cleanup_error) = join_read_worker(control_actor, "control read") {
                    tracing::error!(
                        error = %cleanup_error,
                        "sqlite catalog control read cleanup failed after snapshot startup error"
                    );
                }
                return Err(error);
            }
        };

        Ok(Self {
            endpoint: Arc::new(CatalogReadEndpoint {
                accepting: AtomicBool::new(true),
                control_sender,
                snapshot_sender,
            }),
            control_actor: Some(control_actor),
            snapshot_actor: Some(snapshot_actor),
        })
    }

    fn control_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ControlReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        self.endpoint.control_request(build)
    }

    fn snapshot_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> SnapshotReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        self.endpoint.snapshot_request(build)
    }

    fn check_runtime_health(&self) -> Result<()> {
        if !self.endpoint.accepting.load(Ordering::Acquire)
            || self
                .control_actor
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        {
            return Err(read_worker_stopped("control"));
        }
        if self
            .snapshot_actor
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
        {
            return Err(read_worker_stopped("snapshot"));
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<()> {
        self.endpoint.close_admission();
        let _ = self
            .endpoint
            .control_sender
            .send(ControlReadMessage::Shutdown);
        let _ = self
            .endpoint
            .snapshot_sender
            .send(SnapshotReadMessage::Shutdown);
        let control_result = match self.control_actor.take() {
            Some(actor) => join_read_worker(actor, "control read"),
            None => Ok(()),
        };
        let snapshot_result = match self.snapshot_actor.take() {
            Some(actor) => join_read_worker(actor, "snapshot read"),
            None => Ok(()),
        };
        control_result.and(snapshot_result)
    }
}

fn submit_read_request<M, T>(
    sender: Option<&Sender<M>>,
    build: impl FnOnce(oneshot::Sender<Result<T>>) -> M,
    full_message: &'static str,
) -> Result<oneshot::Receiver<Result<T>>>
where
    M: Send + 'static,
    T: Send + 'static,
{
    let sender = sender.ok_or_else(read_pool_stopped)?;
    let (response_sender, response_receiver) = oneshot::channel();
    match sender.try_send(build(response_sender)) {
        Ok(()) => Ok(response_receiver),
        Err(TrySendError::Full(_)) => {
            Err(ServerError::new(ErrorCode::ResourceExhausted, full_message))
        }
        Err(TrySendError::Disconnected(_)) => Err(read_pool_stopped()),
    }
}

fn start_read_worker<M>(
    thread_name: &'static str,
    config: SqliteCatalogConfig,
    receiver: Receiver<M>,
    backend: Arc<dyn CatalogReadBackend>,
    run: fn(Connection, Receiver<M>, Arc<dyn CatalogReadBackend>) -> Result<()>,
) -> Result<JoinHandle<Result<()>>>
where
    M: Send + 'static,
{
    let (startup_sender, startup_receiver) = crossbeam_channel::bounded(1);
    let actor = thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            let connection = match crate::open_read_connection(&config) {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = startup_sender.send(Err(error.clone()));
                    return Err(error);
                }
            };
            if startup_sender.send(Ok(())).is_err() {
                return connection.close().map_err(|(_, error)| {
                    crate::map_sqlite_error("close unclaimed catalog read connection", error)
                });
            }
            run(connection, receiver, backend)
        })
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("start sqlite catalog read worker thread=[{thread_name}] error=[{error}]"),
            )
        })?;
    match startup_receiver.recv() {
        Ok(Ok(())) => Ok(actor),
        Ok(Err(error)) => {
            let _ = join_read_worker(actor, thread_name);
            Err(error)
        }
        Err(_) => match join_read_worker(actor, thread_name) {
            Ok(()) => Err(read_pool_stopped()),
            Err(error) => Err(error),
        },
    }
}

fn control_read_loop(
    mut connection: Connection,
    receiver: Receiver<ControlReadMessage>,
    backend: Arc<dyn CatalogReadBackend>,
) -> Result<()> {
    while let Ok(message) = receiver.recv() {
        match message {
            ControlReadMessage::Revision(response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.revision(&connection));
                }
            }
            ControlReadMessage::GetNamespace(namespace, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.get_namespace(&connection, namespace));
                }
            }
            ControlReadMessage::ListNamespaces(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.list_namespaces(&mut connection, request));
                }
            }
            ControlReadMessage::GetTable(table, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.get_table(&connection, table));
                }
            }
            ControlReadMessage::ListTables(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.list_tables(&mut connection, request));
                }
            }
            ControlReadMessage::ResolveTables(tables, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.resolve_tables(&mut connection, &tables));
                }
            }
            ControlReadMessage::InspectFiles(probes, response) => {
                if !response.is_closed() {
                    let _ = response
                        .send(backend.inspect_data_file_references(&mut connection, &probes));
                }
            }
            ControlReadMessage::ProbeCommandReplay(request) => {
                let CommandReplayProbeRequest {
                    key,
                    response,
                    probe_permit,
                } = request;
                if response.is_closed() {
                    drop(probe_permit);
                    continue;
                }
                let result = backend.probe_command_replay(&connection, &key);
                drop(probe_permit);
                let _ = response.send(result);
            }
            ControlReadMessage::ProbeLegacyImport(request) => {
                let LegacyImportReplayProbeRequest {
                    request_id,
                    response,
                    probe_permit,
                } = request;
                if response.is_closed() {
                    drop(probe_permit);
                    continue;
                }
                let result = backend.probe_legacy_import_v2(&mut connection, request_id);
                drop(probe_permit);
                let _ = response.send(result);
            }
            ControlReadMessage::ResolveLegacyMapping {
                table_id,
                legacy_files_revision,
                response,
            } => {
                if !response.is_closed() {
                    let _ = response.send(backend.resolve_legacy_file_view_mapping(
                        &connection,
                        table_id,
                        legacy_files_revision,
                    ));
                }
            }
            ControlReadMessage::ResolveCurrentLegacyMapping { table_id, response } => {
                if !response.is_closed() {
                    let _ = response.send(
                        backend.resolve_current_legacy_file_view_mapping(&connection, table_id),
                    );
                }
            }
            ControlReadMessage::Shutdown => break,
        }
    }
    connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close catalog control read", error))
}

fn snapshot_read_loop(
    mut connection: Connection,
    receiver: Receiver<SnapshotReadMessage>,
    backend: Arc<dyn CatalogReadBackend>,
) -> Result<()> {
    while let Ok(message) = receiver.recv() {
        match message {
            SnapshotReadMessage::Read(request) => {
                let SnapshotReadRequest {
                    selectors,
                    limits,
                    response,
                } = request;
                if !response.is_closed() {
                    let _ =
                        response.send(backend.read_snapshot(&mut connection, &selectors, limits));
                }
            }
            SnapshotReadMessage::Shutdown => break,
        }
    }
    connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close catalog snapshot read", error))
}

fn join_read_worker(actor: JoinHandle<Result<()>>, role: &str) -> Result<()> {
    actor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            format!("sqlite catalog {role} worker thread panicked"),
        )
    })?
}

enum ServiceMessage {
    Command(CommandRequest),
    LegacyImport(LegacyImportRequest),
    LegacyReconcile(LegacyReconcileRequest),
    ClaimGarbageCollection(GarbageCollectionClaimRequest),
    FinalizeGarbageCollection(GarbageCollectionFinalizeRequest),
    Shutdown(Sender<Result<()>>),
}

enum WriterRequest {
    Command(CommandRequest),
    LegacyImport(LegacyImportRequest),
    LegacyReconcile(LegacyReconcileRequest),
    ClaimGarbageCollection(GarbageCollectionClaimRequest),
    FinalizeGarbageCollection(GarbageCollectionFinalizeRequest),
}

struct CommandRequest {
    command: VersionedCatalogCommand,
    state: CommandRequestState,
}

struct LegacyImportRequest {
    command: LegacyCatalogImportEnvelopeV2,
    state: LegacyImportRequestState,
}

struct LegacyImportRequestState {
    request_id: RequestId,
    response: oneshot::Sender<Result<LegacyCatalogImportResponseV2>>,
    _manifest_permit: Option<ManifestQueuePermit>,
    _publication_permit: SqlitePublicationPermit,
}

struct LegacyReconcileRequest {
    command: LegacyCatalogReconcileEnvelopeV3,
    state: LegacyReconcileRequestState,
}

struct LegacyReconcileRequestState {
    request_id: RequestId,
    response: oneshot::Sender<Result<LegacyCatalogReconcileResponseV3>>,
    _manifest_permit: Option<ManifestQueuePermit>,
    _publication_permit: SqlitePublicationPermit,
}

impl LegacyReconcileRequestState {
    fn send(self, result: Result<LegacyCatalogReconcileResponseV3>) {
        let _ = self.response.send(result);
    }
}

impl LegacyImportRequestState {
    fn send(self, result: Result<LegacyCatalogImportResponseV2>) {
        let _ = self.response.send(result);
    }
}

enum PreparationRequestState {
    Command(CommandRequestState),
    LegacyImport(LegacyImportRequestState),
    LegacyReconcile(LegacyReconcileRequestState),
}

impl PreparationRequestState {
    fn request_id(&self) -> RequestId {
        match self {
            Self::Command(state) => state.request_id,
            Self::LegacyImport(state) => state.request_id,
            Self::LegacyReconcile(state) => state.request_id,
        }
    }

    fn send_error(self, error: ServerError) {
        match self {
            Self::Command(state) => state.send(Err(error)),
            Self::LegacyImport(state) => state.send(Err(error)),
            Self::LegacyReconcile(state) => state.send(Err(error)),
        }
    }
}

struct CommandRequestState {
    request_id: RequestId,
    response: oneshot::Sender<Result<VersionedCatalogResponse>>,
    _manifest_permit: Option<ManifestQueuePermit>,
    _publication_permit: SqlitePublicationPermit,
}

impl CommandRequestState {
    fn send(self, result: Result<VersionedCatalogResponse>) {
        let _ = self.response.send(result);
    }
}

struct GarbageCollectionClaimRequest {
    guard: crate::runtime_pins::SqliteGarbageCollectionGuard,
    request: ClaimGarbageCollectionFilesRequest,
    response: oneshot::Sender<Result<ClaimedGarbageCollectionFiles>>,
    _command_permit: CommandPermit,
}

impl GarbageCollectionClaimRequest {
    fn execute(self, connection: &mut Connection) {
        let Self {
            guard,
            request,
            response,
            _command_permit,
        } = self;
        let runtime_min_pinned_snapshot = guard.runtime_min_pinned_snapshot();
        let committed = crate::gc_domain::commit_garbage_collection_claim(
            connection,
            request,
            runtime_min_pinned_snapshot,
        );
        // Claim commit is the end of the pin-admission exclusion. Descriptor decoding is
        // read-only and intentionally runs after releasing the table-local guard.
        drop(guard);
        let result = committed.and_then(|claim| {
            crate::gc_domain::materialize_garbage_collection_claim(connection, claim)
        });
        let _ = response.send(result);
    }
}

struct GarbageCollectionFinalizeRequest {
    request: FinalizeGarbageCollectionFilesRequest,
    response: oneshot::Sender<Result<FinalizedGarbageCollectionFiles>>,
    _command_permit: CommandPermit,
}

impl GarbageCollectionFinalizeRequest {
    fn execute(self, connection: &mut Connection) {
        let result = crate::gc_domain::finalize_garbage_collection_files(connection, self.request);
        let _ = self.response.send(result);
    }
}

trait ManifestPreparationBackend: Send + Sync + 'static {
    fn verify(&self, command: VersionedCatalogCommand) -> Result<domain::VerifiedManifestCommand>;

    fn prepare(
        &self,
        preparation: domain::ManifestCommandPreparation,
    ) -> domain::ManifestPreparationResult;
}

struct DomainManifestPreparationBackend;

impl ManifestPreparationBackend for DomainManifestPreparationBackend {
    fn verify(&self, command: VersionedCatalogCommand) -> Result<domain::VerifiedManifestCommand> {
        domain::verify_manifest_command(command)
    }

    fn prepare(
        &self,
        preparation: domain::ManifestCommandPreparation,
    ) -> domain::ManifestPreparationResult {
        domain::prepare_manifest_command(preparation)
    }
}

enum ManifestWorkerJob {
    Verify(VersionedCatalogCommand),
    Prepare(domain::ManifestCommandPreparation),
    PrepareLegacyImport(LegacyCatalogImportEnvelopeV2),
    PrepareLegacyReconcile(Box<LegacyCatalogReconcileEnvelopeV3>),
}

impl ManifestWorkerJob {
    fn request_id(&self) -> RequestId {
        match self {
            Self::Verify(command) => command_request_id(command),
            Self::Prepare(preparation) => preparation.request_id(),
            Self::PrepareLegacyImport(command) => command.request_id,
            Self::PrepareLegacyReconcile(command) => command.request_id,
        }
    }
}

enum ManifestWorkerCompletion {
    Verified(Result<domain::VerifiedManifestCommand>),
    Prepared(domain::ManifestPreparationResult),
    LegacyImportPrepared(Result<legacy_import_domain::PreparedLegacyCatalogImportV2>),
    LegacyReconcilePrepared(Box<Result<legacy_reconcile_domain::PreparedLegacyCatalogReconcileV3>>),
}

enum ManifestWorkerSubmitError {
    Full(ManifestWorkerJob),
    Stopped(ManifestWorkerJob),
}

struct ManifestPreparationWorker {
    sender: Option<Sender<ManifestWorkerJob>>,
    actor: Option<JoinHandle<()>>,
}

impl ManifestPreparationWorker {
    fn start(
        backend: Arc<dyn ManifestPreparationBackend>,
    ) -> Result<(Self, Receiver<ManifestWorkerCompletion>)> {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (completion_sender, completion_receiver) = crossbeam_channel::bounded(1);
        let actor = thread::Builder::new()
            .name(MANIFEST_PREPARATION_THREAD_NAME.to_owned())
            .spawn(move || {
                manifest_worker_loop(receiver, completion_sender, backend);
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start sqlite catalog manifest preparation worker error=[{error}]"),
                )
            })?;
        Ok((
            Self {
                sender: Some(sender),
                actor: Some(actor),
            },
            completion_receiver,
        ))
    }

    fn submit(&self, job: ManifestWorkerJob) -> std::result::Result<(), ManifestWorkerSubmitError> {
        let Some(sender) = &self.sender else {
            return Err(ManifestWorkerSubmitError::Stopped(job));
        };
        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(job)) => Err(ManifestWorkerSubmitError::Full(job)),
            Err(TrySendError::Disconnected(job)) => Err(ManifestWorkerSubmitError::Stopped(job)),
        }
    }

    fn shutdown(&mut self) -> Result<()> {
        self.sender.take();
        match self.actor.take() {
            Some(actor) => actor.join().map_err(|_| {
                ServerError::new(
                    ErrorCode::Internal,
                    "sqlite catalog manifest preparation worker panicked",
                )
            }),
            None => Ok(()),
        }
    }
}

fn manifest_worker_loop(
    receiver: Receiver<ManifestWorkerJob>,
    completion_sender: Sender<ManifestWorkerCompletion>,
    backend: Arc<dyn ManifestPreparationBackend>,
) {
    while let Ok(job) = receiver.recv() {
        let request_id = job.request_id();
        let completion = match job {
            ManifestWorkerJob::Verify(command) => {
                let result = catch_unwind(AssertUnwindSafe(|| backend.verify(command)))
                    .unwrap_or_else(|_| Err(manifest_worker_panicked(request_id, "verify")));
                ManifestWorkerCompletion::Verified(result)
            }
            ManifestWorkerJob::Prepare(preparation) => {
                let panic_result = domain::ManifestPreparationResult::failed(
                    &preparation,
                    manifest_worker_panicked(request_id, "prepare"),
                );
                let result = catch_unwind(AssertUnwindSafe(|| backend.prepare(preparation)))
                    .unwrap_or(panic_result);
                ManifestWorkerCompletion::Prepared(result)
            }
            ManifestWorkerJob::PrepareLegacyImport(command) => {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    legacy_import_domain::prepare_legacy_catalog_import_v2(command)
                }))
                .unwrap_or_else(|_| Err(manifest_worker_panicked(request_id, "legacy_import")));
                ManifestWorkerCompletion::LegacyImportPrepared(result)
            }
            ManifestWorkerJob::PrepareLegacyReconcile(command) => {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    legacy_reconcile_domain::prepare_legacy_catalog_reconcile_v3(*command)
                }))
                .unwrap_or_else(|_| Err(manifest_worker_panicked(request_id, "legacy_reconcile")));
                ManifestWorkerCompletion::LegacyReconcilePrepared(Box::new(result))
            }
        };
        if completion_sender.send(completion).is_err() {
            break;
        }
    }
}

fn manifest_worker_panicked(request_id: RequestId, stage: &str) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("sqlite catalog manifest preparation worker panicked stage=[{stage}]"),
    )
    .with_request_id(request_id)
    .with_retryable(false)
}

fn command_request_id(command: &VersionedCatalogCommand) -> RequestId {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope.request_id
}

fn run_actor(
    config: SqliteCatalogConfig,
    receiver: Receiver<ServiceMessage>,
    startup_sender: Sender<Result<()>>,
    manifest_backend: Arc<dyn ManifestPreparationBackend>,
) {
    let (connection, _) = match crate::open_on_actor(&config) {
        Ok(opened) => opened,
        Err(error) => {
            let _ = startup_sender.send(Err(error));
            return;
        }
    };
    let (mut manifest_worker, manifest_completions) =
        match ManifestPreparationWorker::start(manifest_backend) {
            Ok(started) => started,
            Err(error) => {
                let _ = connection.close();
                let _ = startup_sender.send(Err(error));
                return;
            }
        };
    if startup_sender.send(Ok(())).is_err() {
        let _ = manifest_worker.shutdown();
        let _ = connection.close();
        return;
    }
    actor_loop(
        connection,
        receiver,
        &mut manifest_worker,
        manifest_completions,
    );
}

fn actor_loop(
    mut connection: Connection,
    receiver: Receiver<ServiceMessage>,
    manifest_worker: &mut ManifestPreparationWorker,
    manifest_completions: Receiver<ManifestWorkerCompletion>,
) {
    let mut active_manifest = None;
    let mut pending_requests = VecDeque::new();
    let mut draining = false;
    let mut shutdown_response = None;

    loop {
        if active_manifest.is_none() {
            if let Some(request) = pending_requests.pop_front() {
                start_writer_request(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    request,
                );
                continue;
            }
            if draining {
                break;
            }
        }

        if active_manifest.is_some() {
            if let Ok(completion) = manifest_completions.try_recv() {
                handle_manifest_completion(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    completion,
                );
                continue;
            }
        }

        if draining {
            match manifest_completions.recv() {
                Ok(completion) => handle_manifest_completion(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    completion,
                ),
                Err(_) => fail_active_manifest_worker(
                    &mut active_manifest,
                    "sqlite catalog manifest completion channel stopped during shutdown",
                ),
            }
            continue;
        }

        if active_manifest.is_some() {
            crossbeam_channel::select! {
                recv(manifest_completions) -> completion => {
                    match completion {
                        Ok(completion) => handle_manifest_completion(
                            &mut connection,
                            manifest_worker,
                            &mut active_manifest,
                            completion,
                        ),
                        Err(_) => fail_active_manifest_worker(
                            &mut active_manifest,
                            "sqlite catalog manifest completion channel stopped",
                        ),
                    }
                }
                recv(receiver) -> message => {
                    match message {
                        Ok(message) => handle_service_message(
                            &mut connection,
                            manifest_worker,
                            &mut active_manifest,
                            &mut pending_requests,
                            &mut draining,
                            &mut shutdown_response,
                            message,
                        ),
                        Err(_) => draining = true,
                    }
                }
            }
        } else {
            match receiver.recv() {
                Ok(message) => handle_service_message(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    &mut pending_requests,
                    &mut draining,
                    &mut shutdown_response,
                    message,
                ),
                Err(_) => draining = true,
            }
        }
    }

    let worker_result = manifest_worker.shutdown();
    let close_result = connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close sqlite catalog database", error));
    let shutdown_result = worker_result.and(close_result);
    if let Some(response) = shutdown_response {
        let _ = response.send(shutdown_result);
    }
}

fn handle_service_message(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    pending_requests: &mut VecDeque<WriterRequest>,
    draining: &mut bool,
    shutdown_response: &mut Option<Sender<Result<()>>>,
    message: ServiceMessage,
) {
    match message {
        ServiceMessage::Command(command) => admit_writer_request(
            connection,
            manifest_worker,
            active_manifest,
            pending_requests,
            WriterRequest::Command(command),
        ),
        ServiceMessage::LegacyImport(request) => admit_writer_request(
            connection,
            manifest_worker,
            active_manifest,
            pending_requests,
            WriterRequest::LegacyImport(request),
        ),
        ServiceMessage::LegacyReconcile(request) => admit_writer_request(
            connection,
            manifest_worker,
            active_manifest,
            pending_requests,
            WriterRequest::LegacyReconcile(request),
        ),
        ServiceMessage::ClaimGarbageCollection(request) => admit_writer_request(
            connection,
            manifest_worker,
            active_manifest,
            pending_requests,
            WriterRequest::ClaimGarbageCollection(request),
        ),
        ServiceMessage::FinalizeGarbageCollection(request) => admit_writer_request(
            connection,
            manifest_worker,
            active_manifest,
            pending_requests,
            WriterRequest::FinalizeGarbageCollection(request),
        ),
        ServiceMessage::Shutdown(response) => {
            *draining = true;
            *shutdown_response = Some(response);
        }
    }
}

fn admit_writer_request(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    pending_requests: &mut VecDeque<WriterRequest>,
    request: WriterRequest,
) {
    if active_manifest.is_some() || !pending_requests.is_empty() {
        pending_requests.push_back(request);
    } else {
        start_writer_request(connection, manifest_worker, active_manifest, request);
    }
}

fn start_writer_request(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    request: WriterRequest,
) {
    match request {
        WriterRequest::Command(command) => {
            start_command(connection, manifest_worker, active_manifest, command);
        }
        WriterRequest::LegacyImport(request) => {
            let LegacyImportRequest { command, state } = request;
            submit_manifest_job(
                manifest_worker,
                active_manifest,
                ManifestWorkerJob::PrepareLegacyImport(command),
                PreparationRequestState::LegacyImport(state),
            );
        }
        WriterRequest::LegacyReconcile(request) => {
            let LegacyReconcileRequest { command, state } = request;
            submit_manifest_job(
                manifest_worker,
                active_manifest,
                ManifestWorkerJob::PrepareLegacyReconcile(Box::new(command)),
                PreparationRequestState::LegacyReconcile(state),
            );
        }
        WriterRequest::ClaimGarbageCollection(request) => request.execute(connection),
        WriterRequest::FinalizeGarbageCollection(request) => request.execute(connection),
    }
}

fn start_command(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    request: CommandRequest,
) {
    let CommandRequest { command, state } = request;
    if domain::is_manifest_command(&command) {
        submit_manifest_job(
            manifest_worker,
            active_manifest,
            ManifestWorkerJob::Verify(command),
            PreparationRequestState::Command(state),
        );
    } else {
        state.send(domain::execute_command(connection, command));
    }
}

fn handle_manifest_completion(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    completion: ManifestWorkerCompletion,
) {
    let Some(state) = active_manifest.take() else {
        tracing::error!("sqlite catalog received manifest completion without active command");
        return;
    };
    match completion {
        ManifestWorkerCompletion::Verified(Err(error)) => state.send_error(error),
        ManifestWorkerCompletion::Verified(Ok(verified)) => {
            let PreparationRequestState::Command(state) = state else {
                state.send_error(ServerError::new(
                    ErrorCode::Internal,
                    "sqlite Catalog preparation state mismatched verified manifest",
                ));
                return;
            };
            match domain::begin_verified_manifest_command(connection, verified) {
                Ok(domain::ManifestCommandStart::Replay(response)) => state.send(Ok(response)),
                Ok(domain::ManifestCommandStart::Prepare(preparation)) => submit_manifest_job(
                    manifest_worker,
                    active_manifest,
                    ManifestWorkerJob::Prepare(preparation),
                    PreparationRequestState::Command(state),
                ),
                Err(error) => state.send(Err(error)),
            }
        }
        ManifestWorkerCompletion::Prepared(result) => {
            let PreparationRequestState::Command(state) = state else {
                state.send_error(ServerError::new(
                    ErrorCode::Internal,
                    "sqlite Catalog preparation state mismatched prepared manifest",
                ));
                return;
            };
            state.send(domain::finalize_manifest_preparation(connection, result));
        }
        ManifestWorkerCompletion::LegacyImportPrepared(result) => {
            let PreparationRequestState::LegacyImport(state) = state else {
                state.send_error(ServerError::new(
                    ErrorCode::Internal,
                    "sqlite Catalog preparation state mismatched legacy import",
                ));
                return;
            };
            state.send(result.and_then(|prepared| {
                legacy_import_domain::commit_legacy_catalog_import_v2(connection, prepared)
            }));
        }
        ManifestWorkerCompletion::LegacyReconcilePrepared(result) => {
            let PreparationRequestState::LegacyReconcile(state) = state else {
                state.send_error(ServerError::new(
                    ErrorCode::Internal,
                    "sqlite Catalog preparation state mismatched legacy reconcile",
                ));
                return;
            };
            state.send((*result).and_then(|prepared| {
                legacy_reconcile_domain::commit_legacy_catalog_reconcile_v3(connection, prepared)
            }));
        }
    }
}

fn submit_manifest_job(
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<PreparationRequestState>,
    job: ManifestWorkerJob,
    state: PreparationRequestState,
) {
    match manifest_worker.submit(job) {
        Ok(()) => *active_manifest = Some(state),
        Err(ManifestWorkerSubmitError::Full(job)) => {
            let request_id = job.request_id();
            state.send_error(
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "sqlite catalog manifest preparation queue is full",
                )
                .with_request_id(request_id),
            );
        }
        Err(ManifestWorkerSubmitError::Stopped(job)) => {
            let request_id = job.request_id();
            state.send_error(
                ServerError::new(
                    ErrorCode::Unavailable,
                    "sqlite catalog manifest preparation worker is not running",
                )
                .with_request_id(request_id)
                .with_retryable(false),
            );
        }
    }
}

fn fail_active_manifest_worker(
    active_manifest: &mut Option<PreparationRequestState>,
    message: &'static str,
) {
    if let Some(state) = active_manifest.take() {
        let request_id = state.request_id();
        state.send_error(
            ServerError::new(ErrorCode::Unavailable, message)
                .with_request_id(request_id)
                .with_retryable(false),
        );
    }
}

async fn await_response<T>(response: Result<oneshot::Receiver<Result<T>>>) -> Result<T> {
    response?.await.map_err(|_| actor_stopped())?
}

fn actor_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "sqlite catalog actor is not running",
    )
    .with_retryable(false)
}

fn publication_gate_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "sqlite catalog publication gate is not running",
    )
    .with_retryable(false)
}

fn replay_probe_verification_stopped(
    request_id: RequestId,
    error: tokio::task::JoinError,
) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("sqlite catalog replay probe verification task stopped error=[{error}]"),
    )
    .with_request_id(request_id)
    .with_retryable(false)
}

async fn verify_replay_probe_command<F>(
    request_id: RequestId,
    command: VersionedCatalogCommand,
    probe_permit: ReplayProbePermit,
    verify: F,
) -> Result<(domain::VerifiedCommandReplayKey, ReplayProbePermit)>
where
    F: FnOnce(&VersionedCatalogCommand) -> Result<domain::VerifiedCommandReplayKey>
        + Send
        + 'static,
{
    let (key, probe_permit) = tokio::task::spawn_blocking(move || (verify(&command), probe_permit))
        .await
        .map_err(|error| replay_probe_verification_stopped(request_id, error))?;
    Ok((key?, probe_permit))
}

fn read_pool_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "sqlite catalog read pool is not running",
    )
    .with_retryable(false)
}

fn read_worker_stopped(role: &str) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!("sqlite catalog {role} read worker is not running"),
    )
    .with_retryable(false)
}

fn join_startup_actor(actor: JoinHandle<()>) -> Result<()> {
    actor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "sqlite catalog actor thread panicked during startup",
        )
    })
}

impl Drop for SqliteCatalog {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            tracing::error!(error = %error, "sqlite catalog shutdown failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::io::Read;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use zippy_core::OwnedDataDirectory;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
        CatalogCommandV1, CatalogDataFileProbeMatch, CatalogFileRead, CatalogGarbageCollection,
        CatalogGarbageCollectionGuard, CatalogIdempotencyRead, CatalogPublicationControl,
        CatalogPublicationPermit, CatalogRead, CatalogSnapshotSelector, CatalogTableSnapshot,
        CatalogWrite, ClaimGarbageCollectionFilesRequest, CommandFingerprint,
        CommitCompactionCommand, CommitDataFilesCommand, CreateNamespaceCommand,
        CreateTableCommand, LegacyCatalogImportCommandV2, LegacyCatalogImportEnvelopeV2,
        LegacyCatalogImportFileV2, LegacyCatalogImportReplayProbeV2,
        LegacyCatalogReconcileCommandV3, LegacyCatalogReconcileEnvelopeV3,
        LegacyFileViewContinuity, ManagedDataFileProbe, VersionedCatalogCommand,
        VersionedCatalogResponse, LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY,
        LEGACY_REGISTRY_INDEX_METADATA_KEY, LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, DataFileDescriptor, DataFileLocation,
        DataFileProvenance, DataFileState, DataFiles, ExternalDataPath, ManagedDataPath,
        MetadataMap, MetadataScalar, NativeDataFileProvenance, PendingDataFile,
        SchemaCompatibility, SchemaDescriptor, SchemaIpcPayload, SourceSegmentIdentity,
        SourceSegmentLineage, SourceSegmentLineages, TableDefinition,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
    };

    use super::{
        domain, snapshot_domain, verify_replay_probe_command, CatalogReadBackend,
        DomainManifestPreparationBackend, ManifestPreparationBackend, ManifestQueueBudget,
        ReplayProbeBudget, SqliteCatalog, SqliteCatalogConfig, REPLAY_PROBE_CAPACITY,
    };

    const NAMESPACE_ID: u128 = 1;
    const TABLE_ID: u128 = 2;

    struct BlockingManifestBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    }

    struct PanickingManifestBackend {
        panic_prepare_once: AtomicBool,
    }

    struct CountingManifestBackend {
        verify_calls: Arc<AtomicUsize>,
        prepare_calls: Arc<AtomicUsize>,
    }

    struct PanickingControlReadBackend;

    struct CountingReplayReadBackend {
        probe_calls: Arc<AtomicUsize>,
    }

    struct BlockingLegacyReplayReadBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
    }

    struct BlockingSnapshotBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
        calls: Arc<AtomicUsize>,
    }

    impl ManifestPreparationBackend for BlockingManifestBackend {
        fn verify(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::Result<domain::VerifiedManifestCommand> {
            domain::verify_manifest_command(command)
        }

        fn prepare(
            &self,
            preparation: domain::ManifestCommandPreparation,
        ) -> domain::ManifestPreparationResult {
            self.started
                .send(())
                .expect("test preparation start observer must remain connected");
            self.release
                .recv()
                .expect("test preparation release must be signalled");
            domain::prepare_manifest_command(preparation)
        }
    }

    impl ManifestPreparationBackend for PanickingManifestBackend {
        fn verify(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::Result<domain::VerifiedManifestCommand> {
            domain::verify_manifest_command(command)
        }

        fn prepare(
            &self,
            preparation: domain::ManifestCommandPreparation,
        ) -> domain::ManifestPreparationResult {
            if self.panic_prepare_once.swap(false, Ordering::AcqRel) {
                panic!("injected manifest preparation panic");
            }
            domain::prepare_manifest_command(preparation)
        }
    }

    impl ManifestPreparationBackend for CountingManifestBackend {
        fn verify(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::Result<domain::VerifiedManifestCommand> {
            self.verify_calls.fetch_add(1, Ordering::AcqRel);
            domain::verify_manifest_command(command)
        }

        fn prepare(
            &self,
            preparation: domain::ManifestCommandPreparation,
        ) -> domain::ManifestPreparationResult {
            self.prepare_calls.fetch_add(1, Ordering::AcqRel);
            domain::prepare_manifest_command(preparation)
        }
    }

    impl CatalogReadBackend for CountingReplayReadBackend {
        fn probe_command_replay(
            &self,
            connection: &rusqlite::Connection,
            key: &domain::VerifiedCommandReplayKey,
        ) -> zippy_types::Result<CatalogCommandReplayProbe> {
            self.probe_calls.fetch_add(1, Ordering::AcqRel);
            domain::probe_command_replay(connection, key)
        }
    }

    impl CatalogReadBackend for BlockingLegacyReplayReadBackend {
        fn probe_legacy_import_v2(
            &self,
            connection: &mut rusqlite::Connection,
            request_id: RequestId,
        ) -> zippy_types::Result<LegacyCatalogImportReplayProbeV2> {
            if self.block_once.swap(false, Ordering::AcqRel) {
                self.started
                    .send(())
                    .expect("observe blocking legacy replay probe");
                self.release
                    .recv()
                    .expect("release blocking legacy replay probe");
            }
            crate::legacy_import_domain::probe_legacy_catalog_import_v2(connection, request_id)
        }
    }

    impl CatalogReadBackend for BlockingSnapshotBackend {
        fn read_snapshot(
            &self,
            connection: &mut rusqlite::Connection,
            selectors: &[CatalogSnapshotSelector],
            limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        ) -> zippy_types::Result<zippy_types::catalog::CatalogQuerySnapshot> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.block_once.swap(false, Ordering::AcqRel) {
                self.started
                    .send(())
                    .expect("test snapshot start observer must remain connected");
                self.release
                    .recv()
                    .expect("test snapshot release must be signalled");
            }
            snapshot_domain::read_snapshot_at_with_limits(connection, selectors, limits)
        }
    }

    impl CatalogReadBackend for PanickingControlReadBackend {
        fn revision(&self, _: &rusqlite::Connection) -> zippy_types::Result<u64> {
            panic!("injected control read worker panic");
        }
    }

    fn data_directory(directory: &TempDir) -> Arc<OwnedDataDirectory> {
        Arc::new(
            OwnedDataDirectory::acquire_or_initialize(directory.path().join("data"))
                .expect("own data root"),
        )
    }

    #[cfg(unix)]
    fn write_managed_file_fixture(
        data_directory: &OwnedDataDirectory,
        file: u128,
        contents: &[u8],
    ) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let table_directory = data_directory.directory().root().join("tables/ticks");
        std::fs::create_dir_all(&table_directory).expect("create table data directory");
        std::fs::set_permissions(
            table_directory.parent().expect("tables parent"),
            std::fs::Permissions::from_mode(0o700),
        )
        .expect("secure tables directory");
        std::fs::set_permissions(&table_directory, std::fs::Permissions::from_mode(0o700))
            .expect("secure table directory");
        let physical_file = table_directory.join(format!("{file}.parquet"));
        std::fs::write(&physical_file, contents).expect("write physical file fixture");
        std::fs::set_permissions(&physical_file, std::fs::Permissions::from_mode(0o600))
            .expect("secure physical file fixture");
        physical_file
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("nonzero request id")
    }

    fn table_id() -> TableId {
        TableId::from_u128(TABLE_ID).expect("nonzero table id")
    }

    fn command(
        request: u128,
        expected_revision: Option<u64>,
        command: CatalogCommandV1,
    ) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id(request),
            expected_revision,
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "catalog-service-test".to_owned(),
            },
            command,
        )
        .expect("valid command envelope")
        .into()
    }

    fn migration_command(
        request: u128,
        expected_revision: Option<u64>,
        command: CatalogCommandV1,
    ) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id(request),
            expected_revision,
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "catalog-service-migration-test".to_owned(),
            },
            command,
        )
        .expect("valid migration command envelope")
        .into()
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).expect("create IPC writer");
        writer.finish().expect("finish schema-only IPC stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("nonempty schema IPC payload")
    }

    async fn create_append_table(catalog: &SqliteCatalog) {
        catalog
            .command(command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create namespace");
        catalog
            .command(command(
                2,
                Some(1),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(),
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("ticks").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition {
                        event_time_column: None,
                    }),
                    schema_ipc: schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create append table");
    }

    #[cfg(unix)]
    fn external_legacy_import_command(
        path: &std::path::Path,
        contents: &[u8],
    ) -> LegacyCatalogImportEnvelopeV2 {
        let schema_ipc = schema_ipc();
        let canonical_schema_hash = crate::canonical_schema_hash_from_ipc(&schema_ipc)
            .expect("hash external legacy Schema");
        let checksum = format!("sha256-v1:{:x}", Sha256::digest(contents));
        let pending_file = PendingDataFile {
            file_id: FileId::from_u128(10).expect("nonzero file id"),
            table_id: table_id(),
            schema_version: 1,
            location: DataFileLocation::ExternalLegacy(
                ExternalDataPath::new(path.to_str().expect("UTF-8 external legacy fixture path"))
                    .expect("valid external legacy fixture path"),
            ),
            file_size_bytes: u64::try_from(contents.len())
                .expect("external legacy fixture size fits u64"),
            row_count: NonZeroU64::new(1).expect("nonzero row count"),
            provenance: DataFileProvenance::Legacy {
                offsets: None,
                preserve_discovery_order: true,
            },
            partition: MetadataMap::new(),
            statistics: MetadataMap::new(),
            metadata: MetadataMap::from([(
                LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY.to_owned(),
                MetadataScalar::String(canonical_schema_hash.clone()),
            )]),
            checksum,
            created_at_ns: 1,
        };
        LegacyCatalogImportEnvelopeV2::new(
            request_id(2),
            Some(1),
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "catalog-service-external-legacy-test".to_owned(),
            },
            format!("sha256-v1:{}", "7".repeat(64)),
            1,
            LegacyCatalogImportCommandV2 {
                table_id: table_id(),
                namespace_id: NamespaceId::from_u128(NAMESPACE_ID).expect("nonzero namespace id"),
                name: CatalogName::new("ticks").expect("valid table name"),
                definition: TableDefinition::Append(AppendTableDefinition {
                    event_time_column: None,
                }),
                schema_ipc,
                canonical_schema_hash,
                config: BTreeMap::from([
                    (
                        "zippy.migration.legacy_stream_name".to_owned(),
                        "ticks".to_owned(),
                    ),
                    (
                        "zippy.migration.legacy_persisted_files_revision_base".to_owned(),
                        "1".to_owned(),
                    ),
                    (
                        "zippy.migration.native_snapshot_version_base".to_owned(),
                        "2".to_owned(),
                    ),
                    (
                        "zippy.migration.source_snapshot_hash".to_owned(),
                        format!("sha256-v1:{}", "7".repeat(64)),
                    ),
                    ("zippy.migration.version".to_owned(), "2".to_owned()),
                ]),
                writer_epoch: 1,
                files: vec![LegacyCatalogImportFileV2 {
                    file: pending_file,
                    discovery_order: 0,
                }],
                legacy_files_revision: 1,
            },
        )
        .expect("build external legacy import")
    }

    #[cfg(unix)]
    fn legacy_reconcile_command(
        request: u128,
        schema_hash: &str,
        expected_writer_epoch: u64,
        writer_epoch: u64,
        expected_legacy_files_revision: u64,
        legacy_files_revision: u64,
        files: Vec<LegacyCatalogImportFileV2>,
    ) -> LegacyCatalogReconcileEnvelopeV3 {
        LegacyCatalogReconcileEnvelopeV3::new(
            request_id(request),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "catalog-service-legacy-reconcile-test".to_owned(),
            },
            format!("sha256-v1:{}", "8".repeat(64)),
            legacy_files_revision,
            LegacyCatalogReconcileCommandV3 {
                table_id: table_id(),
                expected_schema_version: 1,
                expected_schema_hash: schema_hash.to_owned(),
                expected_writer_epoch,
                writer_epoch,
                expected_legacy_files_revision,
                legacy_files_revision,
                files,
            },
        )
        .expect("build legacy reconcile command")
    }

    #[cfg(unix)]
    async fn import_external_legacy_file(
        catalog: &SqliteCatalog,
        path: &std::path::Path,
        contents: &[u8],
    ) -> zippy_types::catalog::CatalogQuerySnapshot {
        std::fs::write(path, contents).expect("write external legacy fixture");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire external legacy import session");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create external legacy namespace");

        let command = external_legacy_import_command(path, contents);
        let schema_ipc = command.command.schema_ipc.clone();
        let canonical_schema_hash = command.command.canonical_schema_hash.clone();
        let pending_file = command.command.files[0].file.clone();
        let response = session
            .import_legacy_table_v2(command)
            .await
            .expect("import external legacy file");
        session.seal();
        let selector = CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 2,
            schema_version: 1,
        };
        let schema = SchemaDescriptor {
            table_id: table_id(),
            schema_version: 1,
            schema_ipc,
            schema_hash: canonical_schema_hash,
            compatibility: SchemaCompatibility::Initial,
            created_at_ns: response.table.created_at_ns,
        };
        let files = DataFiles::new(vec![DataFileDescriptor {
            file: pending_file,
            legacy_discovery_order: Some(0),
            added_snapshot_version: 2,
            removed_snapshot_version: None,
            state: DataFileState::Committed,
        }])
        .expect("build external legacy pinned file set");
        let table =
            CatalogTableSnapshot::new(selector, response.table, schema, response.snapshot, files)
                .expect("build external legacy pinned table");
        zippy_types::catalog::CatalogQuerySnapshot::new(response.catalog_revision, vec![table])
            .expect("build external legacy pinned query snapshot")
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_replay_probe_reports_transactional_revision_and_committed_response() {
        let directory = TempDir::new().expect("create replay probe directory");
        let owned = data_directory(&directory);
        let physical_file = directory.path().join("legacy-probe.parquet");
        let contents = b"legacy-replay-probe-fixture";
        std::fs::write(&physical_file, contents).expect("write external legacy fixture");
        let import = external_legacy_import_command(&physical_file, contents);
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned))
            .expect("open replay probe Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire replay probe session");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create import namespace");

        assert_eq!(
            session
                .probe_legacy_import_v2(request_id(2))
                .await
                .expect("probe missing legacy import"),
            LegacyCatalogImportReplayProbeV2::Missing {
                observed_catalog_revision: 1,
            }
        );
        let committed = session
            .import_legacy_table_v2(import)
            .await
            .expect("commit legacy import");
        catalog
            .command(migration_command(
                3,
                Some(2),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(3).expect("nonzero namespace id"),
                    name: CatalogName::new("audit").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("advance Catalog after legacy import");

        assert_eq!(
            session
                .probe_legacy_import_v2(request_id(2))
                .await
                .expect("probe committed legacy import"),
            LegacyCatalogImportReplayProbeV2::Committed {
                observed_catalog_revision: 3,
                response: committed,
            }
        );
        session.seal();
        catalog.close().expect("close replay probe Catalog");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_reconcile_commits_full_views_replays_and_preserves_historical_order() {
        let directory = TempDir::new().expect("create reconcile directory");
        let owned = data_directory(&directory);
        let first_path = directory.path().join("legacy-first.parquet");
        let removed_path = directory.path().join("legacy-removed.parquet");
        let new_path = directory.path().join("legacy-new.parquet");
        let contents = b"legacy-reconcile-fixture";
        for path in [&first_path, &removed_path, &new_path] {
            std::fs::write(path, contents).expect("write reconcile fixture");
        }
        let base = external_legacy_import_command(&first_path, contents);
        let mut import_body = base.command.clone();
        import_body.files[0].file.metadata.insert(
            LEGACY_REGISTRY_INDEX_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(0),
        );
        import_body.files[0].file.metadata.insert(
            LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(0),
        );
        let mut removed = import_body.files[0].clone();
        removed.file.file_id = FileId::from_u128(12).expect("nonzero removed file id");
        removed.file.location = DataFileLocation::ExternalLegacy(
            ExternalDataPath::new(removed_path.to_str().expect("UTF-8 removed path"))
                .expect("valid removed path"),
        );
        removed.discovery_order = 1;
        removed.file.metadata.insert(
            LEGACY_REGISTRY_INDEX_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(1),
        );
        removed.file.metadata.insert(
            LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(1),
        );
        import_body.files.push(removed.clone());
        let import = LegacyCatalogImportEnvelopeV2::new(
            base.request_id,
            base.expected_revision,
            base.actor.clone(),
            base.source_snapshot_hash.clone(),
            base.source_catalog_revision,
            import_body.clone(),
        )
        .expect("build two-file import");
        let schema_hash = import_body.canonical_schema_hash.clone();

        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open reconcile Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire reconcile import session");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create reconcile namespace");
        session
            .import_legacy_table_v2(import)
            .await
            .expect("import initial reconcile view");
        session.seal();

        let handle = catalog
            .legacy_reconcile_handle()
            .expect("create online reconcile handle");
        let mut new_file = import_body.files[0].clone();
        new_file.file.file_id = FileId::from_u128(11).expect("nonzero new file id");
        new_file.file.location = DataFileLocation::ExternalLegacy(
            ExternalDataPath::new(new_path.to_str().expect("UTF-8 new path"))
                .expect("valid new path"),
        );
        new_file.discovery_order = 0;
        let mut retained = import_body.files[0].clone();
        retained.discovery_order = 1;
        retained.file.metadata.insert(
            LEGACY_REGISTRY_INDEX_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(9),
        );
        retained.file.metadata.insert(
            LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(1),
        );
        let target = vec![new_file.clone(), retained.clone()];
        let reconcile = legacy_reconcile_command(3, &schema_hash, 1, 2, 1, 3, target.clone());

        for colliding_request in [1, 2] {
            let error = handle
                .reconcile_legacy_table_v3(legacy_reconcile_command(
                    colliding_request,
                    &schema_hash,
                    1,
                    2,
                    1,
                    3,
                    target.clone(),
                ))
                .await
                .expect_err("V1 and V2 request ids must conflict with V3");
            assert_eq!(error.code(), ErrorCode::Conflict);
        }

        let response = handle
            .reconcile_legacy_table_v3(reconcile.clone())
            .await
            .expect("commit gap reconcile");
        assert_eq!(response.catalog_revision, 3);
        assert_eq!(response.native_snapshot_version, 3);
        assert_eq!(response.continuity, LegacyFileViewContinuity::Gap);
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(reconcile.clone())
                .await
                .expect("replay exact reconcile"),
            response
        );
        assert_eq!(catalog.revision().await.expect("read replay revision"), 3);

        let base_mapping = catalog
            .resolve_legacy_file_view_mapping(table_id(), 1)
            .await
            .expect("resolve imported base mapping");
        assert_eq!(base_mapping.native_snapshot_version, 2);
        assert_eq!(base_mapping.continuity, LegacyFileViewContinuity::Base);
        let mapping = catalog
            .resolve_legacy_file_view_mapping(table_id(), 3)
            .await
            .expect("resolve exact reconcile mapping");
        assert_eq!(mapping.native_snapshot_version, 3);
        assert_eq!(mapping.request_id, Some(request_id(3)));

        let snapshot = |version| CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: version,
            schema_version: 1,
        };
        let old = catalog
            .read_snapshot_at(&[snapshot(2)])
            .await
            .expect("read historical import snapshot");
        let old_files = old.tables()[0].files().as_slice();
        assert_eq!(old_files.len(), 2);
        assert_eq!(
            old_files
                .iter()
                .find(|file| file.file_id() == FileId::from_u128(10).unwrap())
                .and_then(DataFileDescriptor::legacy_discovery_order),
            Some(0)
        );
        assert_eq!(
            old_files
                .iter()
                .find(|file| file.file_id() == FileId::from_u128(12).unwrap())
                .and_then(DataFileDescriptor::legacy_discovery_order),
            Some(1)
        );
        let current = catalog
            .read_snapshot_at(&[snapshot(3)])
            .await
            .expect("read reconciled snapshot");
        let current_files = current.tables()[0].files().as_slice();
        assert_eq!(current_files.len(), 2);
        assert_eq!(
            current_files
                .iter()
                .find(|file| file.file_id() == FileId::from_u128(11).unwrap())
                .and_then(DataFileDescriptor::legacy_discovery_order),
            Some(0)
        );
        assert_eq!(
            current_files
                .iter()
                .find(|file| file.file_id() == FileId::from_u128(10).unwrap())
                .and_then(DataFileDescriptor::legacy_discovery_order),
            Some(1)
        );
        assert!(current_files
            .iter()
            .all(|file| file.file_id() != FileId::from_u128(12).unwrap()));

        let mut metadata_drift = target.clone();
        metadata_drift[1].file.metadata.insert(
            "source.dataset_version".to_owned(),
            MetadataScalar::UInt64(2),
        );
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(legacy_reconcile_command(
                    8,
                    &schema_hash,
                    2,
                    2,
                    3,
                    4,
                    metadata_drift,
                ))
                .await
                .expect_err("ordinary retained metadata changes must conflict")
                .code(),
            ErrorCode::Conflict
        );
        assert_eq!(
            catalog
                .revision()
                .await
                .expect("read revision after conflict"),
            3
        );

        let different_payload =
            legacy_reconcile_command(3, &schema_hash, 2, 2, 3, 4, target.clone());
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(different_payload)
                .await
                .expect_err("same request with another payload must conflict")
                .code(),
            ErrorCode::Conflict
        );
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(legacy_reconcile_command(
                    4,
                    &schema_hash,
                    2,
                    2,
                    1,
                    3,
                    target.clone(),
                ))
                .await
                .expect_err("same target revision with another request must conflict")
                .code(),
            ErrorCode::Conflict
        );
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(legacy_reconcile_command(
                    5,
                    &schema_hash,
                    2,
                    2,
                    1,
                    4,
                    target.clone(),
                ))
                .await
                .expect_err("stale per-table CAS must conflict")
                .code(),
            ErrorCode::Conflict
        );

        let empty = handle
            .reconcile_legacy_table_v3(legacy_reconcile_command(
                6,
                &schema_hash,
                2,
                2,
                3,
                4,
                Vec::new(),
            ))
            .await
            .expect("commit empty authoritative target");
        assert_eq!(empty.continuity, LegacyFileViewContinuity::Contiguous);
        let empty_snapshot = catalog
            .read_snapshot_at(&[snapshot(4)])
            .await
            .expect("read empty reconcile snapshot");
        assert!(empty_snapshot.tables()[0].files().is_empty());

        let mut resurrected = removed;
        resurrected.discovery_order = 0;
        assert_eq!(
            handle
                .reconcile_legacy_table_v3(legacy_reconcile_command(
                    7,
                    &schema_hash,
                    2,
                    2,
                    4,
                    5,
                    vec![resurrected],
                ))
                .await
                .expect_err("removed legacy identity resurrection must fail")
                .code(),
            ErrorCode::Conflict
        );
        let v1_collision = catalog
            .command(command(
                3,
                Some(4),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(30).unwrap(),
                    name: CatalogName::new("collision").unwrap(),
                }),
            ))
            .await
            .expect_err("V1 must reject an existing V3 request id");
        assert_eq!(v1_collision.code(), ErrorCode::Conflict);
        drop(handle);
        catalog.close().expect("close reconciled Catalog");

        let reopened = SqliteCatalog::open(SqliteCatalogConfig::new(&owned))
            .expect("reopen reconciled Catalog");
        let replay_session = reopened
            .begin_legacy_import_session()
            .expect("open replay session after restart");
        let v2_collision = replay_session
            .probe_legacy_import_v2(request_id(3))
            .await
            .expect_err("V2 must reject an existing V3 request id");
        assert_eq!(v2_collision.code(), ErrorCode::Conflict);
        replay_session.seal();
        reopened.close().expect("close reopened reconciled Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_reconcile_handle_fails_terminally_after_catalog_close() {
        let directory = TempDir::new().expect("create closed handle directory");
        let owned = data_directory(&directory);
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned))
            .expect("open closed handle Catalog");
        let handle = catalog
            .legacy_reconcile_handle()
            .expect("create lifecycle-bound reconcile handle");
        let read_handle = catalog
            .read_handle()
            .expect("create lifecycle-bound read handle");
        catalog.close().expect("close handle owner");
        let command = legacy_reconcile_command(
            1,
            &format!("sha256-v1:{}", "9".repeat(64)),
            0,
            0,
            0,
            1,
            Vec::new(),
        );
        let error = handle
            .reconcile_legacy_table_v3(command)
            .await
            .expect_err("closed reconcile handle must fail");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(!error.retryable());
        let read_error = read_handle
            .resolve_current_legacy_file_view_mapping(table_id())
            .await
            .expect_err("closed read handle must reject current mapping lookup");
        assert_eq!(read_error.code(), ErrorCode::Unavailable);
        assert!(!read_error.retryable());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_reconcile_bootstraps_a_strict_v1_empty_table_base_mapping() {
        let directory = TempDir::new().expect("create V1 bootstrap directory");
        let owned = data_directory(&directory);
        let physical_file = directory.path().join("v1-bootstrap.parquet");
        let contents = b"v1-bootstrap-file";
        std::fs::write(&physical_file, contents).expect("write V1 bootstrap file");
        let schema_ipc = schema_ipc();
        let schema_hash =
            crate::canonical_schema_hash_from_ipc(&schema_ipc).expect("hash V1 bootstrap Schema");
        let source_hash = format!("sha256-v1:{}", "7".repeat(64));
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned))
            .expect("open V1 bootstrap Catalog");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID).unwrap(),
                    name: CatalogName::new("market").unwrap(),
                }),
            ))
            .await
            .expect("create V1 bootstrap namespace");
        catalog
            .command(migration_command(
                2,
                Some(1),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(),
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID).unwrap(),
                    name: CatalogName::new("ticks").unwrap(),
                    definition: TableDefinition::Append(AppendTableDefinition {
                        event_time_column: None,
                    }),
                    schema_ipc,
                    config: BTreeMap::from([
                        (
                            "zippy.migration.legacy_stream_name".to_owned(),
                            "ticks".to_owned(),
                        ),
                        (
                            "zippy.migration.legacy_persisted_files_revision_base".to_owned(),
                            "0".to_owned(),
                        ),
                        (
                            "zippy.migration.native_snapshot_version_base".to_owned(),
                            "1".to_owned(),
                        ),
                        (
                            "zippy.migration.source_snapshot_hash".to_owned(),
                            source_hash,
                        ),
                        ("zippy.migration.version".to_owned(), "1".to_owned()),
                    ]),
                }),
            ))
            .await
            .expect("create V1 empty migrated table");
        let missing = catalog
            .resolve_current_legacy_file_view_mapping(table_id())
            .await
            .expect_err("V1 empty table has no physical base mapping before bootstrap");
        assert_eq!(missing.code(), ErrorCode::NotFound);
        let target_file = external_legacy_import_command(&physical_file, contents)
            .command
            .files;
        let handle = catalog
            .legacy_reconcile_handle()
            .expect("create V1 bootstrap reconcile handle");
        let response = handle
            .reconcile_legacy_table_v3(legacy_reconcile_command(
                3,
                &schema_hash,
                0,
                1,
                0,
                2,
                target_file,
            ))
            .await
            .expect("reconcile V1 empty base");
        assert_eq!(response.native_snapshot_version, 2);
        assert_eq!(response.continuity, LegacyFileViewContinuity::Gap);
        let base = catalog
            .resolve_legacy_file_view_mapping(table_id(), 0)
            .await
            .expect("resolve bootstrapped V1 base");
        assert_eq!(base.native_snapshot_version, 1);
        assert_eq!(base.request_id, None);
        assert_eq!(base.continuity, LegacyFileViewContinuity::Base);
        let current = catalog
            .resolve_current_legacy_file_view_mapping(table_id())
            .await
            .expect("resolve current mapping with one joined read");
        assert_eq!(current.legacy_files_revision, 2);
        assert_eq!(current.native_snapshot_version, 2);

        let database_path = owned.directory().root().join("catalog/catalog.db");
        let raw = rusqlite::Connection::open(&database_path).expect("open mapping tamper fixture");
        raw.execute_batch("PRAGMA ignore_check_constraints = ON")
            .expect("allow focused mapping corruption fixture");
        raw.execute(
            "UPDATE legacy_file_view_mappings SET continuity = 'invalid' \
              WHERE table_id = ?1 AND legacy_files_revision = ?2",
            rusqlite::params![
                table_id().into_bytes().as_slice(),
                2_u64.to_be_bytes().as_slice(),
            ],
        )
        .expect("corrupt current mapping continuity");
        drop(raw);
        let corrupt = catalog
            .resolve_current_legacy_file_view_mapping(table_id())
            .await
            .expect_err("malformed current mapping must fail closed");
        assert_eq!(corrupt.code(), ErrorCode::DataCorruption);
        let raw = rusqlite::Connection::open(database_path).expect("open mapping repair fixture");
        raw.execute(
            "UPDATE legacy_file_view_mappings SET continuity = 'gap' \
              WHERE table_id = ?1 AND legacy_files_revision = ?2",
            rusqlite::params![
                table_id().into_bytes().as_slice(),
                2_u64.to_be_bytes().as_slice(),
            ],
        )
        .expect("repair current mapping continuity");
        catalog.close().expect("close V1 bootstrap Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_replay_probe_rejects_v1_request_id_conflicts() {
        let directory = TempDir::new().expect("create conflict directory");
        let owned = data_directory(&directory);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open conflict Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire conflict probe session");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("commit V1 request-id fixture");

        let error = session
            .probe_legacy_import_v2(request_id(1))
            .await
            .expect_err("V1 request ID must conflict with a V2 replay probe");
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(error.request_id(), Some(request_id(1)));
        assert!(!error.retryable());
        session.seal();
        catalog.close().expect("close conflict Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_replay_probe_uses_only_fixed_control_read_admission() {
        let directory = TempDir::new().expect("create bounded probe directory");
        let owned = data_directory(&directory);
        let catalog = SqliteCatalog::open(
            SqliteCatalogConfig::new(&owned)
                .with_command_capacity(1)
                .with_manifest_queue_capacity_bytes(1),
        )
        .expect("open bounded probe Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire bounded probe session");
        let publication_permit = catalog
            .acquire_publication_permit()
            .await
            .expect("occupy only publication permit");

        assert_eq!(catalog.command_budget.used.load(Ordering::Acquire), 1);
        assert_eq!(
            catalog.manifest_queue_budget.used.load(Ordering::Acquire),
            0
        );
        assert_eq!(
            session
                .probe_legacy_import_v2(request_id(1))
                .await
                .expect("publication exhaustion must not block replay probe"),
            LegacyCatalogImportReplayProbeV2::Missing {
                observed_catalog_revision: 0,
            }
        );
        assert_eq!(catalog.command_budget.used.load(Ordering::Acquire), 1);
        assert_eq!(
            catalog.manifest_queue_budget.used.load(Ordering::Acquire),
            0
        );
        assert_eq!(catalog.replay_probe_budget.used.load(Ordering::Acquire), 0);

        let replay_permits = (0..REPLAY_PROBE_CAPACITY)
            .map(|_| {
                catalog
                    .replay_probe_budget
                    .reserve()
                    .expect("reserve fixed replay slot")
            })
            .collect::<Vec<_>>();
        let exhausted = session
            .probe_legacy_import_v2(request_id(1))
            .await
            .expect_err("fixed replay capacity must reject excess work");
        assert_eq!(exhausted.code(), ErrorCode::ResourceExhausted);
        assert_eq!(exhausted.request_id(), Some(request_id(1)));
        drop(replay_permits);
        drop(publication_permit);
        session.seal();
        catalog.close().expect("close bounded probe Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_legacy_replay_probe_retains_admission_until_read_exits() {
        let directory = TempDir::new().expect("create cancelled probe directory");
        let owned = data_directory(&directory);
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        let catalog = SqliteCatalog::open_with_backends(
            SqliteCatalogConfig::new(&owned),
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(BlockingLegacyReplayReadBackend {
                started: started_sender,
                release: release_receiver,
                block_once: AtomicBool::new(true),
            }),
        )
        .expect("open Catalog with blocking legacy probe backend");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire cancelled probe session");
        let mut probe = session.probe_legacy_import_v2(request_id(1));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(probe.as_mut().poll(&mut context), Poll::Pending));
        started_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("legacy replay probe must enter the read backend");
        assert_eq!(catalog.replay_probe_budget.used.load(Ordering::Acquire), 1);

        drop(probe);
        assert_eq!(catalog.replay_probe_budget.used.load(Ordering::Acquire), 1);
        release_sender
            .send(())
            .expect("release cancelled legacy replay probe");
        tokio::time::timeout(Duration::from_secs(2), async {
            while catalog.replay_probe_budget.used.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancelled legacy replay probe must release fixed admission");
        session.seal();
        catalog
            .close()
            .expect("close Catalog with blocking legacy probe backend");
    }

    #[cfg(unix)]
    fn overwrite_legacy_import_response(
        database_path: &std::path::Path,
        response: &serde_json::Value,
    ) {
        let encoded = serde_json::to_vec(response).expect("encode tampered legacy response");
        overwrite_legacy_import_response_blob(database_path, encoded);
    }

    #[cfg(unix)]
    fn overwrite_legacy_import_response_blob(database_path: &std::path::Path, encoded: Vec<u8>) {
        let connection = rusqlite::Connection::open(database_path).expect("open Catalog database");
        connection
            .execute(
                "UPDATE legacy_import_requests SET response_blob = ?1 WHERE request_id = ?2",
                rusqlite::params![encoded, request_id(2).into_bytes().as_slice()],
            )
            .expect("tamper retained legacy response");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn legacy_import_replay_rejects_semantically_tampered_responses() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let physical_file = directory.path().join("legacy.parquet");
        let contents = b"legacy-replay-revision-fixture";
        std::fs::write(&physical_file, contents).expect("write external legacy fixture");
        let import_command = external_legacy_import_command(&physical_file, contents);

        let catalog = SqliteCatalog::open(config.clone()).expect("open Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire legacy import session");
        catalog
            .command(migration_command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create external legacy namespace");
        session
            .import_legacy_table_v2(import_command.clone())
            .await
            .expect("commit legacy import");
        session.seal();
        catalog.close().expect("close Catalog before tampering");

        let database_path = data_directory.directory().root().join("catalog/catalog.db");
        let connection = rusqlite::Connection::open(&database_path).expect("open Catalog database");
        let response_blob: Vec<u8> = connection
            .query_row(
                "SELECT response_blob FROM legacy_import_requests WHERE request_id = ?1",
                [request_id(2).into_bytes().as_slice()],
                |row| row.get(0),
            )
            .expect("read retained legacy response");
        let response: serde_json::Value =
            serde_json::from_slice(&response_blob).expect("decode retained legacy response");
        drop(connection);

        let mut wrong_created_at = response.clone();
        wrong_created_at["response"]["table"]["created_at_ns"] = serde_json::json!(1);
        overwrite_legacy_import_response(&database_path, &wrong_created_at);
        let reopened = SqliteCatalog::open(config.clone()).expect("reopen Catalog");
        let replay_session = reopened
            .begin_legacy_import_session()
            .expect("acquire replay import session");
        let error = replay_session
            .probe_legacy_import_v2(request_id(2))
            .await
            .expect_err("probe must reject wrong replay creation time");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        let error = replay_session
            .import_legacy_table_v2(import_command.clone())
            .await
            .expect_err("wrong replay creation time must fail closed");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        replay_session.seal();
        reopened.close().expect("close reopened Catalog");

        let mut invalid_summary = response.clone();
        invalid_summary["response"]["snapshot"]["summary"]["added_files"]["value"] =
            serde_json::json!(4097);
        overwrite_legacy_import_response(&database_path, &invalid_summary);
        let reopened = SqliteCatalog::open(config.clone()).expect("reopen summary Catalog");
        let replay_session = reopened
            .begin_legacy_import_session()
            .expect("acquire summary replay session");
        let error = replay_session
            .probe_legacy_import_v2(request_id(2))
            .await
            .expect_err("probe must reject impossible import file count");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        replay_session.seal();
        reopened.close().expect("close summary replay Catalog");

        overwrite_legacy_import_response_blob(&database_path, vec![0_u8; 4 * 1024 * 1024 + 1]);
        let reopened = SqliteCatalog::open(config.clone()).expect("reopen oversized Catalog");
        let replay_session = reopened
            .begin_legacy_import_session()
            .expect("acquire oversized replay session");
        let error = replay_session
            .probe_legacy_import_v2(request_id(2))
            .await
            .expect_err("probe must reject oversized response before decoding it");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        assert!(error.to_string().contains("exceeds byte limit"));
        replay_session.seal();
        reopened.close().expect("close oversized replay Catalog");

        let mut wrong_revision = response;
        wrong_revision["response"]["catalog_revision"] = serde_json::json!(3);
        wrong_revision["response"]["snapshot"]["commit_lsn"] = serde_json::json!(3);
        overwrite_legacy_import_response(&database_path, &wrong_revision);
        let reopened = SqliteCatalog::open(config).expect("reopen Catalog for revision check");
        let replay_session = reopened
            .begin_legacy_import_session()
            .expect("acquire revision replay session");
        let error = replay_session
            .probe_legacy_import_v2(request_id(2))
            .await
            .expect_err("probe must reject replay revisions newer than its transaction");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        let error = replay_session
            .import_legacy_table_v2(import_command)
            .await
            .expect_err("wrong replay revision must fail closed");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(2)));
        replay_session.seal();
        reopened.close().expect("close revision replay Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn legacy_import_session_is_exclusive_and_sealed_by_ordinary_writes() {
        let directory = TempDir::new().expect("create session test directory");
        let owned = data_directory(&directory);
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&owned)).expect("open Catalog");
        let session = catalog
            .begin_legacy_import_session()
            .expect("acquire first import session");
        let duplicate = catalog
            .begin_legacy_import_session()
            .expect_err("second active import session must be rejected");
        assert_eq!(duplicate.code(), ErrorCode::Conflict);

        let ordinary_write = catalog
            .command(command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect_err("ordinary writes must not cross an active import session");
        assert_eq!(ordinary_write.code(), ErrorCode::Conflict);
        drop(session);
        let sealed = catalog
            .begin_legacy_import_session()
            .expect_err("dropping a session must seal the Catalog instance");
        assert_eq!(sealed.code(), ErrorCode::Conflict);
        catalog.close().expect("close first Catalog");

        let second_directory = TempDir::new().expect("create ordinary-write test directory");
        let second_owned = data_directory(&second_directory);
        let second = SqliteCatalog::open(SqliteCatalogConfig::new(&second_owned))
            .expect("open second Catalog");
        second
            .command(command(
                2,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("commit ordinary Catalog write");
        let sealed = second
            .begin_legacy_import_session()
            .expect_err("ordinary mutation must seal an unused import window");
        assert_eq!(sealed.code(), ErrorCode::Conflict);
        second.close().expect("close second Catalog");
    }

    fn pending_file(file: u128, offset: u64) -> PendingDataFile {
        let offsets = OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset))
            .expect("valid one-row offset range");
        let lineage = SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: offset + 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .expect("valid source lineage");
        PendingDataFile {
            file_id: FileId::from_u128(file).expect("nonzero file id"),
            table_id: table_id(),
            schema_version: 1,
            location: DataFileLocation::Managed(
                ManagedDataPath::new(format!("tables/ticks/{file}.parquet"))
                    .expect("valid managed path"),
            ),
            file_size_bytes: 128,
            row_count: NonZeroU64::new(1).expect("nonzero row count"),
            provenance: DataFileProvenance::Native(
                NativeDataFileProvenance::new(offsets, lineage).expect("valid native provenance"),
            ),
            partition: MetadataMap::new(),
            statistics: MetadataMap::new(),
            metadata: MetadataMap::new(),
            checksum: format!("sha256-v1:{file:064x}"),
            created_at_ns: 1,
        }
    }

    fn commit_data_file(
        request: u128,
        file: u128,
        expected_revision: u64,
        expected_snapshot_version: u64,
        offset: u64,
    ) -> VersionedCatalogCommand {
        command(
            request,
            Some(expected_revision),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(),
                expected_snapshot_version,
                files: vec![pending_file(file, offset)],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(offset)),
            }),
        )
    }

    fn compact_data_file(
        request: u128,
        expected_revision: u64,
        expected_snapshot_version: u64,
        input_file: u128,
        output_file: u128,
        offset: u64,
    ) -> VersionedCatalogCommand {
        command(
            request,
            Some(expected_revision),
            CatalogCommandV1::CommitCompaction(CommitCompactionCommand {
                table_id: table_id(),
                expected_snapshot_version,
                input_file_ids: vec![FileId::from_u128(input_file).expect("nonzero input file id")],
                output_files: vec![pending_file(output_file, offset)],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(offset)),
            }),
        )
    }

    fn commit_data_file_with_metadata(request: u128, entries: usize) -> VersionedCatalogCommand {
        let mut file = pending_file(10, 0);
        for index in 0..entries {
            file.metadata.insert(
                format!("field_{index:04}"),
                MetadataScalar::String(format!("value_{index:04}")),
            );
        }
        command(
            request,
            Some(2),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(),
                expected_snapshot_version: 1,
                files: vec![file],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
            }),
        )
    }

    fn blocking_backend() -> (
        Arc<dyn ManifestPreparationBackend>,
        crossbeam_channel::Receiver<()>,
        crossbeam_channel::Sender<()>,
    ) {
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        (
            Arc::new(BlockingManifestBackend {
                started: started_sender,
                release: release_receiver,
            }),
            started_receiver,
            release_sender,
        )
    }

    fn blocking_snapshot_backend() -> (
        Arc<dyn CatalogReadBackend>,
        crossbeam_channel::Receiver<()>,
        crossbeam_channel::Sender<()>,
        Arc<AtomicUsize>,
    ) {
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(BlockingSnapshotBackend {
                started: started_sender,
                release: release_receiver,
                block_once: AtomicBool::new(true),
                calls: Arc::clone(&calls),
            }),
            started_receiver,
            release_sender,
            calls,
        )
    }

    #[test]
    fn manifest_queue_budget_releases_completed_reservations() {
        let budget = Arc::new(ManifestQueueBudget::new(8));
        assert!(budget.reserve(0).expect("zero-byte reservation").is_none());

        let permit = budget
            .reserve(8)
            .expect("reserve complete byte budget")
            .expect("nonzero reservation returns a permit");
        let error = budget
            .reserve(1)
            .err()
            .expect("occupied byte budget must reject another reservation");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(error.retryable());

        let oversized = budget
            .reserve(9)
            .err()
            .expect("single oversized command must be rejected permanently");
        assert_eq!(oversized.code(), ErrorCode::ResourceExhausted);
        assert!(!oversized.retryable());

        drop(permit);
        assert!(budget
            .reserve(8)
            .expect("released byte budget can be reserved again")
            .is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_handle_does_not_retain_owner_and_fails_terminally_after_close() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog = Arc::new(
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog"),
        );
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit file fixture");

        let handle = catalog.read_handle().expect("create read handle");
        assert_eq!(handle.revision().await, Ok(3));
        let snapshot = handle
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 2,
                schema_version: 1,
            }])
            .await
            .expect("read exact table snapshot through handle");
        assert_eq!(snapshot.catalog_revision(), 3);
        assert_eq!(snapshot.tables()[0].files().as_slice().len(), 1);

        let file_snapshot = handle
            .inspect_data_file_references(&[ManagedDataFileProbe {
                file_id: FileId::from_u128(10).expect("nonzero file id"),
                table_id: table_id(),
                path: ManagedDataPath::new("tables/ticks/10.parquet").expect("valid managed path"),
            }])
            .await
            .expect("inspect file identity through handle");
        assert_eq!(file_snapshot.catalog_revision, 3);
        assert!(matches!(
            file_snapshot.results[0].matched,
            CatalogDataFileProbeMatch::Exact { .. }
        ));

        let weak_owner = Arc::downgrade(&catalog);
        let catalog = Arc::try_unwrap(catalog)
            .unwrap_or_else(|_| panic!("read handle must not retain Catalog owner"));
        assert!(weak_owner.upgrade().is_none());
        let cold_revision = handle.revision();
        catalog.close().expect("close Catalog with retained handle");

        let error = cold_revision
            .await
            .expect_err("unpolled handle read must observe Catalog shutdown");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(!error.retryable());

        let snapshot_error = handle
            .read_snapshot_at(&[])
            .await
            .expect_err("closed Catalog handle must reject snapshot reads");
        assert_eq!(snapshot_error.code(), ErrorCode::Unavailable);
        assert!(!snapshot_error.retryable());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn pinned_file_tasks_and_readers_protect_physical_files_until_final_drop() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let contents: Vec<u8> = (0..128).collect();
        write_managed_file_fixture(&data_directory, 10, &contents);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit historical file");
        catalog
            .command(compact_data_file(4, 3, 2, 10, 11, 0))
            .await
            .expect("replace historical file");

        let handle = catalog.read_handle().expect("create read handle");
        let pinned = handle
            .read_pinned_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 2,
                schema_version: 1,
            }])
            .await
            .expect("read and pin historical snapshot");
        assert_eq!(pinned.catalog_revision(), 4);
        let table = pinned.table(0).expect("pinned table manifest");
        assert_eq!(table.file_count(), 1);
        let file = table.file(0).expect("pinned data-file task");
        assert_eq!(file.file_id(), FileId::from_u128(10).unwrap());
        assert!(!format!("{file:?}").contains("tables/ticks"));
        let worker_file = file.clone();
        drop(file);
        drop(table);
        drop(pinned);

        let protected = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .expect("exclude new pins"),
                ClaimGarbageCollectionFilesRequest {
                    table_id: table_id(),
                    retry_limit: 1,
                    new_claim_limit: 1,
                },
            )
            .await
            .expect("claim while cloned file task retains pin owner");
        assert!(protected.files.is_empty());

        let second_worker_file = worker_file.clone();
        let mut reader = worker_file.open_reader().expect("open controlled reader");
        let mut second_reader = second_worker_file
            .open_reader()
            .expect("open independent controlled reader");
        let mut first_byte = [u8::MAX];
        let mut second_first_byte = [u8::MAX];
        reader
            .read_exact(&mut first_byte)
            .expect("read first worker cursor");
        second_reader
            .read_exact(&mut second_first_byte)
            .expect("read independent worker cursor");
        assert_eq!(first_byte, [0]);
        assert_eq!(second_first_byte, [0]);
        drop(worker_file);
        drop(second_worker_file);
        drop(reader);

        let reader_protected = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .expect("exclude pins retained by independent reader"),
                ClaimGarbageCollectionFilesRequest {
                    table_id: table_id(),
                    retry_limit: 1,
                    new_claim_limit: 1,
                },
            )
            .await
            .expect("claim while independent reader retains pin owner");
        assert!(reader_protected.files.is_empty());

        drop(second_reader);
        let claimed = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .expect("exclude pins after query completion"),
                ClaimGarbageCollectionFilesRequest {
                    table_id: table_id(),
                    retry_limit: 1,
                    new_claim_limit: 1,
                },
            )
            .await
            .expect("claim after final pinned reader drops");
        assert_eq!(claimed.files.len(), 1);
        assert_eq!(claimed.files[0].file_id(), FileId::from_u128(10).unwrap());
        catalog.close().expect("close catalog");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn pinned_external_legacy_reader_starts_at_offset_zero_after_checksum() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let physical_file = directory.path().join("legacy-events.parquet");
        let contents = (0_u8..=127).collect::<Vec<_>>();
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        let snapshot = import_external_legacy_file(&catalog, &physical_file, &contents).await;
        let pin = catalog
            .pin_runtime_snapshot(table_id(), 2)
            .expect("pin external legacy snapshot");
        let pinned = crate::SqliteCatalogPinnedQuerySnapshot::new(
            snapshot,
            vec![pin],
            Arc::clone(&data_directory),
        );
        let file = pinned
            .table(0)
            .expect("pinned external legacy table")
            .file(0)
            .expect("pinned external legacy file");
        let mut reader = file
            .open_reader()
            .expect("open checksum-validated external legacy reader");
        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .expect("read external legacy file from its first byte");
        assert_eq!(actual, contents);

        drop((reader, file, pinned));
        catalog.close().expect("close catalog");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn pinned_external_legacy_reader_rejects_same_size_content_replacement() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let physical_file = directory.path().join("legacy-events.parquet");
        let contents = [0x5a_u8; 128];
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        let snapshot = import_external_legacy_file(&catalog, &physical_file, &contents).await;
        let pin = catalog
            .pin_runtime_snapshot(table_id(), 2)
            .expect("pin external legacy snapshot");
        let pinned = crate::SqliteCatalogPinnedQuerySnapshot::new(
            snapshot,
            vec![pin],
            Arc::clone(&data_directory),
        );
        let file = pinned
            .table(0)
            .expect("pinned external legacy table")
            .file(0)
            .expect("pinned external legacy file");

        std::fs::write(&physical_file, [0xa5_u8; 128])
            .expect("replace external legacy contents without changing length");
        let error = file
            .open_reader()
            .expect_err("same-size external legacy replacement must fail closed");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.message().contains(
            physical_file
                .to_str()
                .expect("UTF-8 external legacy fixture path")
        ));

        drop((file, pinned));
        catalog.close().expect("close catalog");
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn pinned_reader_rejects_unsafe_or_changed_managed_files_without_path_disclosure() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let physical_file = write_managed_file_fixture(&data_directory, 10, &[0x5a; 127]);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit physical file metadata");
        let pinned = catalog
            .read_handle()
            .expect("create read handle")
            .read_pinned_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 2,
                schema_version: 1,
            }])
            .await
            .expect("read pinned snapshot");
        let table = pinned.table(0).expect("pinned table manifest");
        let file = table.file(0).expect("pinned data-file task");

        let wrong_size = file
            .open_reader()
            .expect_err("changed physical length must fail closed");
        assert_eq!(wrong_size.code(), ErrorCode::DataCorruption);
        assert!(!wrong_size.message().contains("tables/ticks"));
        assert!(!wrong_size
            .message()
            .contains(data_directory.directory().root().to_string_lossy().as_ref()));

        std::fs::write(&physical_file, [0x5a; 128]).expect("restore expected length");
        std::fs::set_permissions(&physical_file, std::fs::Permissions::from_mode(0o600))
            .expect("restore secure permissions");
        let hard_link = physical_file.with_extension("hardlink");
        std::fs::hard_link(&physical_file, &hard_link).expect("create unsafe hard link");
        let linked = file
            .open_reader()
            .expect_err("multiply linked managed file must fail closed");
        assert_eq!(linked.code(), ErrorCode::DataCorruption);
        std::fs::remove_file(hard_link).expect("remove hard-link fixture");

        file.open_reader()
            .expect("restored secure physical file must open");
        std::fs::remove_file(&physical_file).expect("remove physical file fixture");
        symlink("/etc/passwd", &physical_file).expect("create unsafe symlink fixture");
        let symlinked = file
            .open_reader()
            .expect_err("symbolic-link replacement must fail closed");
        assert_eq!(symlinked.code(), ErrorCode::DataCorruption);
        assert!(!symlinked.message().contains("/etc/passwd"));

        drop(file);
        drop(table);
        drop(pinned);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn active_pinned_snapshot_retains_catalog_epoch_across_owner_close() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let catalog = SqliteCatalog::open(config.clone()).expect("open catalog");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let pinned = handle
            .read_pinned_snapshot_at(&selectors)
            .await
            .expect("pin initial snapshot");

        catalog
            .close()
            .expect("close Catalog owner with active pin");
        let conflict = match SqliteCatalog::open(config.clone()) {
            Ok(reopened) => {
                reopened.close().expect("close unexpected reopened Catalog");
                panic!("active pin unexpectedly released Catalog runtime epoch");
            }
            Err(error) => error,
        };
        assert_eq!(conflict.code(), ErrorCode::Conflict);
        let stale_handle = handle
            .read_pinned_snapshot_at(&selectors)
            .await
            .expect_err("closed epoch rejects new pin admission");
        assert_eq!(stale_handle.code(), ErrorCode::Unavailable);

        drop(pinned);
        let reopened = SqliteCatalog::open(config).expect("reopen after final active pin drops");
        assert_eq!(reopened.revision().await, Ok(2));
        reopened.close().expect("close reopened Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_handle_applies_per_request_snapshot_limits_inside_sqlite_lane() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit first file");
        catalog
            .command(commit_data_file(4, 11, 3, 2, 1))
            .await
            .expect("commit second file");
        let handle = catalog.read_handle().expect("create read handle");
        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 3,
            schema_version: 1,
        }];

        let file_limit = handle
            .read_snapshot_at_with_limits(
                &selectors,
                snapshot_domain::SqliteCatalogSnapshotReadLimits {
                    max_visible_files: 1,
                    ..snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
                },
            )
            .await
            .expect_err("SQLite lane must stop before returning a second file");
        assert_eq!(file_limit.code(), ErrorCode::ResourceExhausted);

        let byte_limit = handle
            .read_snapshot_at_with_limits(
                &selectors,
                snapshot_domain::SqliteCatalogSnapshotReadLimits {
                    max_raw_response_bytes: 1,
                    ..snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
                },
            )
            .await
            .expect_err("SQLite lane must enforce raw response bytes while decoding");
        assert_eq!(byte_limit.code(), ErrorCode::ResourceExhausted);
        catalog.close().expect("close Catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_probe_is_bounded_read_only_and_skips_manifest_preparation() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let verify_calls = Arc::new(AtomicUsize::new(0));
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let catalog = SqliteCatalog::open_with_manifest_backend(
            SqliteCatalogConfig::new(&data_directory),
            Arc::new(CountingManifestBackend {
                verify_calls: Arc::clone(&verify_calls),
                prepare_calls: Arc::clone(&prepare_calls),
            }),
        )
        .expect("open catalog with counting manifest backend");
        create_append_table(&catalog).await;

        let command = commit_data_file(3, 10, 2, 1, 0);
        assert_eq!(
            catalog
                .probe_command_replay(&command)
                .await
                .expect("probe missing command"),
            CatalogCommandReplayProbe::Missing
        );
        assert_eq!(catalog.revision().await, Ok(2));
        assert_eq!(verify_calls.load(Ordering::Acquire), 0);
        assert_eq!(prepare_calls.load(Ordering::Acquire), 0);

        let committed = catalog
            .command(command.clone())
            .await
            .expect("commit manifest once");
        assert_eq!(verify_calls.load(Ordering::Acquire), 1);
        assert_eq!(prepare_calls.load(Ordering::Acquire), 1);

        assert_eq!(
            catalog
                .probe_command_replay(&command)
                .await
                .expect("probe exact committed command"),
            CatalogCommandReplayProbe::Exact {
                response: committed
            }
        );
        let conflict = catalog
            .probe_command_replay(&commit_data_file(3, 11, 3, 2, 1))
            .await
            .expect_err("same request id with another fingerprint must conflict");
        assert_eq!(conflict.code(), ErrorCode::Conflict);
        assert_eq!(conflict.request_id(), Some(request_id(3)));
        assert_eq!(catalog.revision().await, Ok(3));
        assert_eq!(verify_calls.load(Ordering::Acquire), 1);
        assert_eq!(prepare_calls.load(Ordering::Acquire), 1);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_probe_ignores_manifest_write_budget_for_missing_and_exact_results() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let writer = SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
            .expect("open catalog writer");
        create_append_table(&writer).await;
        let committed_command = commit_data_file(3, 10, 2, 1, 0);
        let committed = writer
            .command(committed_command.clone())
            .await
            .expect("commit replay fixture");
        writer.close().expect("close catalog writer");

        let verify_calls = Arc::new(AtomicUsize::new(0));
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let catalog = SqliteCatalog::open_with_manifest_backend(
            SqliteCatalogConfig::new(&data_directory).with_manifest_queue_capacity_bytes(1),
            Arc::new(CountingManifestBackend {
                verify_calls: Arc::clone(&verify_calls),
                prepare_calls: Arc::clone(&prepare_calls),
            }),
        )
        .expect("open catalog with one-byte manifest budget");

        assert_eq!(
            catalog
                .probe_command_replay(&commit_data_file(4, 11, 3, 2, 1))
                .await
                .expect("low manifest budget must still probe a missing command"),
            CatalogCommandReplayProbe::Missing
        );
        assert_eq!(
            catalog
                .probe_command_replay(&committed_command)
                .await
                .expect("low manifest budget must replay a retained command"),
            CatalogCommandReplayProbe::Exact {
                response: committed
            }
        );
        assert_eq!(catalog.revision().await, Ok(3));
        assert_eq!(verify_calls.load(Ordering::Acquire), 0);
        assert_eq!(prepare_calls.load(Ordering::Acquire), 0);
        catalog.close().expect("close bounded catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_probe_releases_fixed_admission_before_observing_its_response() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog = SqliteCatalog::open(
            SqliteCatalogConfig::new(&data_directory).with_control_read_capacity(1),
        )
        .expect("open single-probe catalog");
        let command = commit_data_file(3, 10, 2, 1, 0);

        for _ in 0..32 {
            assert_eq!(
                catalog
                    .probe_command_replay(&command)
                    .await
                    .expect("immediate replay probe admission must remain available"),
                CatalogCommandReplayProbe::Missing
            );
            assert_eq!(catalog.replay_probe_budget.used.load(Ordering::Acquire), 0);
        }

        catalog.close().expect("close single-probe catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_probe_rejects_bad_fingerprint_before_control_read_worker() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let probe_calls = Arc::new(AtomicUsize::new(0));
        let catalog = SqliteCatalog::open_with_backends(
            SqliteCatalogConfig::new(&data_directory),
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(CountingReplayReadBackend {
                probe_calls: Arc::clone(&probe_calls),
            }),
        )
        .expect("open catalog with counting replay backend");
        let mut command = commit_data_file(3, 10, 2, 1, 0);
        let VersionedCatalogCommand::V1(envelope) = &mut command;
        envelope.canonical_fingerprint = CommandFingerprint::from_bytes([0_u8; 32]);

        let error = catalog
            .probe_command_replay(&command)
            .await
            .expect_err("bad fingerprint must fail before SQLite read admission");

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert_eq!(error.request_id(), Some(request_id(3)));
        assert_eq!(probe_calls.load(Ordering::Acquire), 0);
        assert_eq!(catalog.replay_probe_budget.used.load(Ordering::Acquire), 0);
        catalog
            .close()
            .expect("close catalog with counting replay backend");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_replay_verification_retains_admission_until_blocking_work_exits() {
        let budget = Arc::new(ReplayProbeBudget::new(1));
        let permit = budget.reserve().expect("reserve only replay probe slot");
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        let verification = tokio::spawn(verify_replay_probe_command(
            request_id(3),
            commit_data_file(3, 10, 2, 1, 0),
            permit,
            move |command| {
                started_sender
                    .send(())
                    .expect("observe blocking replay verification");
                release_receiver
                    .recv()
                    .expect("release blocking replay verification");
                domain::verify_command_replay_key(command)
            },
        ));
        tokio::task::yield_now().await;
        started_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("blocking replay verification must start");

        verification.abort();
        let join_error = match verification.await {
            Ok(_) => panic!("aborted replay verification unexpectedly completed"),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());
        assert_eq!(budget.used.load(Ordering::Acquire), 1);

        release_sender
            .send(())
            .expect("finish cancelled blocking verification");
        tokio::time::timeout(Duration::from_secs(2), async {
            while budget.used.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("cancelled verification must eventually release admission");
        drop(
            budget
                .reserve()
                .expect("released replay probe slot must be reusable"),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_verification_panic_is_terminal_and_releases_admission() {
        let budget = Arc::new(ReplayProbeBudget::new(1));
        let permit = budget.reserve().expect("reserve replay probe slot");

        let result = verify_replay_probe_command(
            request_id(3),
            commit_data_file(3, 10, 2, 1, 0),
            permit,
            |_| panic!("injected replay verification panic"),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("replay verification panic unexpectedly succeeded"),
            Err(error) => error,
        };

        assert_eq!(error.code(), ErrorCode::Internal);
        assert!(!error.retryable());
        assert_eq!(error.request_id(), Some(request_id(3)));
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_manifest_preflight_yields_and_releases_cancelled_command_budget() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory).with_command_capacity(1))
                .expect("open single-command catalog");
        create_append_table(&catalog).await;

        let command = commit_data_file_with_metadata(3, 512);
        let mut scanning = catalog.command(command.clone());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            scanning.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(catalog.revision().await, Ok(2));

        let occupied = catalog
            .command(command.clone())
            .await
            .expect_err("preflight must hold the global command permit until cancellation");
        assert_eq!(occupied.code(), ErrorCode::ResourceExhausted);
        assert!(occupied.retryable());
        drop(scanning);

        let VersionedCatalogResponse::V1(response) = catalog
            .command(command)
            .await
            .expect("cancelled preflight must release its command permit");
        assert_eq!(response.catalog_revision, 3);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_snapshot_does_not_block_control_reads_or_writer_commit() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (read_backend, started, release, _) = blocking_snapshot_backend();
        let catalog = SqliteCatalog::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            read_backend,
        )
        .expect("open catalog with blocking snapshot backend");
        create_append_table(&catalog).await;

        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let mut snapshot = catalog.read_snapshot_at(&selectors);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            snapshot.as_mut().poll(&mut context),
            Poll::Pending
        ));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot must reach the dedicated read worker");

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), catalog.revision())
                .await
                .expect("control read must not wait for snapshot"),
            Ok(2)
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            catalog.command(commit_data_file(3, 10, 2, 1, 0)),
        )
        .await
        .expect("writer commit must not wait for snapshot")
        .expect("commit one manifest while snapshot is blocked");
        assert_eq!(catalog.revision().await, Ok(3));

        release.send(()).expect("release snapshot read");
        let snapshot = snapshot.await.expect("complete exact snapshot read");
        assert_eq!(snapshot.catalog_revision(), 3);
        assert!(snapshot.tables()[0].files().as_slice().is_empty());
        catalog
            .close()
            .expect("close catalog read lanes and writer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_queued_snapshot_is_skipped_and_releases_queue_capacity() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory).with_snapshot_read_capacity(1);
        let (read_backend, started, release, calls) = blocking_snapshot_backend();
        let catalog = SqliteCatalog::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            read_backend,
        )
        .expect("open catalog with one queued snapshot slot");
        create_append_table(&catalog).await;

        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let mut active = catalog.read_snapshot_at(&selectors);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(active.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("first snapshot must occupy its worker");

        let mut cancelled = catalog.read_snapshot_at(&selectors);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        let full = catalog
            .read_snapshot_at(&selectors)
            .await
            .expect_err("active and queued snapshots must saturate the lane");
        assert_eq!(full.code(), ErrorCode::ResourceExhausted);
        assert_eq!(full.message(), "sqlite catalog snapshot read queue is full");
        drop(cancelled);

        release.send(()).expect("release active snapshot");
        active.await.expect("finish active snapshot");
        let final_snapshot = loop {
            match catalog.read_snapshot_at(&selectors).await {
                Err(error) if error.code() == ErrorCode::ResourceExhausted => {
                    tokio::task::yield_now().await;
                }
                result => break result,
            }
        };
        final_snapshot.expect("snapshot lane must recover after skipping cancellation");
        assert_eq!(calls.load(Ordering::Acquire), 2);
        catalog.close().expect("close catalog after cancellation");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_manifest_preparation_keeps_reads_live_and_shutdown_drains_cancelled_commit() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let catalog = Arc::new(
            SqliteCatalog::open_with_manifest_backend(config.clone(), backend)
                .expect("open catalog with blocking manifest backend"),
        );
        create_append_table(&catalog).await;

        let task_catalog = Arc::clone(&catalog);
        let mut commit =
            Box::pin(async move { task_catalog.command(commit_data_file(3, 10, 2, 1, 0)).await });
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("manifest must reach semantic preparation");

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), catalog.revision())
                .await
                .expect("revision read must not wait for manifest preparation"),
            Ok(2)
        );
        drop(commit);

        let catalog = match Arc::try_unwrap(catalog) {
            Ok(catalog) => catalog,
            Err(_) => panic!("cancelled manifest future must release its catalog Arc"),
        };
        let (closed_sender, closed_receiver) = crossbeam_channel::bounded(1);
        let close_thread = std::thread::spawn(move || {
            let _ = closed_sender.send(catalog.close());
        });
        assert!(matches!(
            closed_receiver.recv_timeout(Duration::from_millis(100)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        ));

        release.send(()).expect("release manifest preparation");
        closed_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("catalog close must finish after preparation release")
            .expect("catalog close must drain the accepted manifest");
        close_thread.join().expect("join catalog close thread");

        let reopened = SqliteCatalog::open(config).expect("reopen catalog after drained close");
        assert_eq!(reopened.revision().await, Ok(3));
        let snapshot = reopened
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 2,
                schema_version: 1,
            }])
            .await
            .expect("read committed manifest after restart");
        assert_eq!(snapshot.tables()[0].files().as_slice().len(), 1);
        reopened.close().expect("close reopened catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_admitted_gc_claim_executes_in_writer_fifo_order() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let setup = SqliteCatalog::open(config.clone()).expect("open setup catalog");
        create_append_table(&setup).await;
        setup
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit garbage collection input");
        setup
            .command(compact_data_file(4, 3, 2, 10, 11, 0))
            .await
            .expect("remove garbage collection input");
        setup.close().expect("close setup catalog");

        let (backend, started, release) = blocking_backend();
        let catalog = SqliteCatalog::open_with_manifest_backend(config, backend)
            .expect("reopen with blocking manifest backend");
        let mut blocker = Box::pin(catalog.command(commit_data_file(5, 12, 4, 3, 1)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(blocker.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("manifest command must block preparation");

        let guard = catalog
            .acquire_garbage_collection_guard(table_id())
            .expect("acquire genuine table-bound garbage collection guard");
        let mut claim = Box::pin(catalog.claim_garbage_collection_files(
            guard,
            ClaimGarbageCollectionFilesRequest {
                table_id: table_id(),
                retry_limit: 1,
                new_claim_limit: 1,
            },
        ));
        assert!(matches!(claim.as_mut().poll(&mut context), Poll::Pending));
        drop(claim);
        let excluded = catalog
            .pin_runtime_snapshot(table_id(), 2)
            .expect_err("cancelled waiter must retain query-pin exclusion until transaction");
        assert_eq!(excluded.code(), ErrorCode::Conflict);

        release.send(()).expect("release manifest preparation");
        blocker.await.expect("finish preceding manifest command");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if catalog.revision().await == Ok(6)
                    && catalog.command_budget.used.load(Ordering::Acquire) == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled claim must execute after preceding writer request");
        let pin = catalog
            .pin_runtime_snapshot(table_id(), 2)
            .expect("query-pin exclusion must release after claim transaction");
        drop(pin);
        assert_eq!(catalog.command_budget.used.load(Ordering::Acquire), 0);

        let state = catalog
            .inspect_data_file_references(&[ManagedDataFileProbe {
                file_id: FileId::from_u128(10).expect("nonzero file id"),
                table_id: table_id(),
                path: ManagedDataPath::new("tables/ticks/10.parquet").expect("valid managed path"),
            }])
            .await
            .expect("inspect cancelled claim result");
        let CatalogDataFileProbeMatch::Exact { reference } = &state.results[0].matched else {
            panic!("claimed file identity must remain exact");
        };
        assert_eq!(reference.state, DataFileState::Deleting);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_claim_rejects_forged_cross_table_and_cross_catalog_guards() {
        fn request() -> ClaimGarbageCollectionFilesRequest {
            ClaimGarbageCollectionFilesRequest {
                table_id: table_id(),
                retry_limit: 1,
                new_claim_limit: 1,
            }
        }

        let first_directory = TempDir::new().expect("create first temp directory");
        let first_data_directory = data_directory(&first_directory);
        let first = SqliteCatalog::open(SqliteCatalogConfig::new(&first_data_directory))
            .expect("open first Catalog");

        let forged = CatalogGarbageCollectionGuard::from_opaque(());
        let forged_error = first
            .claim_garbage_collection_files(forged, request())
            .await
            .expect_err("arbitrary opaque value must not authorize garbage collection");
        assert_eq!(forged_error.code(), ErrorCode::InvalidArgument);

        let other_table = TableId::from_u128(999).expect("nonzero table id");
        let cross_table = first
            .acquire_garbage_collection_guard(other_table)
            .expect("acquire guard for another table");
        let cross_table_error = first
            .claim_garbage_collection_files(cross_table, request())
            .await
            .expect_err("table-bound guard must not authorize another table");
        assert_eq!(cross_table_error.code(), ErrorCode::InvalidArgument);

        let second_directory = TempDir::new().expect("create second temp directory");
        let second_data_directory = data_directory(&second_directory);
        let second = SqliteCatalog::open(SqliteCatalogConfig::new(&second_data_directory))
            .expect("open second Catalog");
        let cross_catalog = second
            .acquire_garbage_collection_guard(table_id())
            .expect("acquire guard from another Catalog");
        let cross_catalog_error = first
            .claim_garbage_collection_files(cross_catalog, request())
            .await
            .expect_err("Catalog-bound guard must not authorize another Catalog");
        assert_eq!(cross_catalog_error.code(), ErrorCode::InvalidArgument);

        second.close().expect("close second Catalog");
        first.close().expect("close first Catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_exclusion_waits_for_cancelled_admitted_mutation() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (backend, started, release) = blocking_backend();
        let catalog = Arc::new(
            SqliteCatalog::open_with_manifest_backend(
                SqliteCatalogConfig::new(&data_directory),
                backend,
            )
            .expect("open catalog with blocking manifest backend"),
        );
        create_append_table(&catalog).await;

        let task_catalog = Arc::clone(&catalog);
        let command =
            tokio::spawn(
                async move { task_catalog.command(commit_data_file(3, 10, 2, 1, 0)).await },
            );
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("manifest must reach semantic preparation");
        command.abort();
        assert!(command
            .await
            .expect_err("command waiter must cancel")
            .is_cancelled());

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                catalog.acquire_publication_exclusion(),
            )
            .await
            .is_err(),
            "admitted mutation must retain its shared publication permit"
        );

        release.send(()).expect("release manifest preparation");
        let exclusion = tokio::time::timeout(
            Duration::from_secs(2),
            catalog.acquire_publication_exclusion(),
        )
        .await
        .expect("exclusive cleanup admission must unblock")
        .expect("acquire exclusive cleanup authority");
        assert_eq!(catalog.revision().await, Ok(3));
        drop(exclusion);

        let catalog = Arc::try_unwrap(catalog)
            .unwrap_or_else(|_| panic!("publication guard test must release Catalog Arc"));
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forged_publication_permit_cannot_mutate_under_exclusion() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory)).expect("open catalog");
        create_append_table(&catalog).await;

        let exclusion = catalog
            .acquire_publication_exclusion()
            .await
            .expect("acquire exclusive cleanup authority");
        let forged = CatalogPublicationPermit::from_opaque(());
        let error = tokio::time::timeout(
            Duration::from_millis(100),
            catalog.command_with_publication_permit(forged, commit_data_file(3, 10, 2, 1, 0)),
        )
        .await
        .expect("forged capability must be rejected without waiting for exclusion")
        .expect_err("forged capability must not reach the Catalog actor");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!error.retryable());
        assert_eq!(catalog.revision().await, Ok(2));

        drop(exclusion);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_handle_and_runtime_health_report_a_stopped_worker_as_terminal() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog = SqliteCatalog::open_with_backends(
            SqliteCatalogConfig::new(&data_directory),
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(PanickingControlReadBackend),
        )
        .expect("open catalog with panicking control read backend");
        let handle = catalog.read_handle().expect("create read handle");

        let request_error = handle
            .revision()
            .await
            .expect_err("panicked read worker must drop its response");
        assert_eq!(request_error.code(), ErrorCode::Unavailable);
        assert!(!request_error.retryable());

        let health_error = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match catalog.check_runtime_health() {
                    Ok(()) => tokio::task::yield_now().await,
                    Err(error) => break error,
                }
            }
        })
        .await
        .expect("runtime health must observe the stopped worker");
        assert_eq!(health_error.code(), ErrorCode::Unavailable);
        assert!(!health_error.retryable());
        assert!(health_error.message().contains("control read worker"));

        let subsequent_error = handle
            .revision()
            .await
            .expect_err("stopped worker must reject subsequent handle reads");
        assert_eq!(subsequent_error.code(), ErrorCode::Unavailable);
        assert!(!subsequent_error.retryable());

        let close_error = catalog
            .close()
            .expect_err("joining the panicked read worker must fail");
        assert_eq!(close_error.code(), ErrorCode::Internal);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_manifest_preserves_pending_mutation_fifo() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let catalog = SqliteCatalog::open_with_manifest_backend(config, backend)
            .expect("open catalog with blocking manifest backend");
        create_append_table(&catalog).await;

        let mut first = Box::pin(catalog.command(commit_data_file(3, 10, 2, 1, 0)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("first manifest must reach semantic preparation");

        let mut second = Box::pin(catalog.command(commit_data_file(4, 11, 3, 2, 1)));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(catalog.revision().await, Ok(2));

        release
            .send(())
            .expect("release first manifest preparation");
        let VersionedCatalogResponse::V1(first_response) = first
            .await
            .expect("first manifest must commit before the pending manifest");
        assert_eq!(first_response.catalog_revision, 3);
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("second manifest must start after the first terminal result");
        assert_eq!(catalog.revision().await, Ok(3));

        release
            .send(())
            .expect("release second manifest preparation");
        let VersionedCatalogResponse::V1(second_response) = second
            .await
            .expect("second manifest must commit from the updated parent state");
        assert_eq!(second_response.catalog_revision, 4);
        assert_eq!(catalog.revision().await, Ok(4));
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preparation_panic_is_terminal_and_the_same_request_can_retry() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog = SqliteCatalog::open_with_manifest_backend(
            SqliteCatalogConfig::new(&data_directory),
            Arc::new(PanickingManifestBackend {
                panic_prepare_once: AtomicBool::new(true),
            }),
        )
        .expect("open catalog with panicking manifest backend");
        create_append_table(&catalog).await;

        let command = commit_data_file(3, 10, 2, 1, 0);
        let error = catalog
            .command(command.clone())
            .await
            .expect_err("injected preparation panic must be terminal");
        assert_eq!(error.code(), ErrorCode::Internal);
        assert_eq!(error.request_id(), Some(request_id(3)));
        assert_eq!(catalog.revision().await, Ok(2));

        let VersionedCatalogResponse::V1(response) = catalog
            .command(command)
            .await
            .expect("worker must remain live and the released request can retry");
        assert_eq!(response.catalog_revision, 3);
        catalog.close().expect("close catalog after panic recovery");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_runtime_ownership_survives_preparation_and_reopen_replays_exactly() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let first = Arc::new(
            SqliteCatalog::open_with_manifest_backend(config.clone(), backend)
                .expect("open first catalog actor"),
        );
        create_append_table(&first).await;

        let first_catalog = Arc::clone(&first);
        let persisted = commit_data_file(3, 10, 2, 1, 0);
        let first_persisted = persisted.clone();
        let first_command =
            tokio::spawn(async move { first_catalog.command(first_persisted).await });
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("command must reach manifest preparation");

        let ownership_error = match SqliteCatalog::open_with_manifest_backend(
            config.clone(),
            Arc::new(DomainManifestPreparationBackend),
        ) {
            Ok(catalog) => {
                catalog.close().expect("close unexpected second actor");
                panic!("second typed Catalog actor unexpectedly acquired ownership");
            }
            Err(error) => error,
        };
        assert_eq!(ownership_error.code(), ErrorCode::Conflict);

        release.send(()).expect("release manifest preparation");
        let committed = first_command
            .await
            .expect("join first catalog command")
            .expect("first actor commits after preparation");
        assert_eq!(first.revision().await, Ok(3));

        let first = match Arc::try_unwrap(first) {
            Ok(catalog) => catalog,
            Err(_) => panic!("manifest task must release the first catalog Arc"),
        };
        first.close().expect("close first catalog actor");

        let reopened = SqliteCatalog::open(config).expect("reopen Catalog after owner release");
        assert_eq!(
            reopened
                .command(persisted)
                .await
                .expect("replay exact persisted command"),
            committed
        );
        reopened.close().expect("close reopened Catalog actor");
    }
}
