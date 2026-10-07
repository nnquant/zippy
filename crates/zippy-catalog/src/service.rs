//! Asynchronous Catalog traits backed by isolated SQLite writer and read workers.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::num::NonZeroU64;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, TrySendError};
use rusqlite::{Connection, TransactionBehavior};
use tokio::sync::{oneshot, Semaphore};
use zippy_types::catalog::{
    CatalogCommandReplayProbe, CatalogConsumerRead, CatalogDataFileReferenceSnapshot,
    CatalogFileRead, CatalogGarbageCollection, CatalogGarbageCollectionGuard,
    CatalogIdempotencyRead, CatalogPublicationControl, CatalogPublicationExclusion,
    CatalogPublicationPermit, CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector,
    CatalogWrite, ClaimGarbageCollectionFilesRequest, ClaimedGarbageCollectionFiles,
    ConsumerOffsetSnapshot, FinalizeGarbageCollectionFilesRequest, FinalizedGarbageCollectionFiles,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    ManagedDataFileProbe, NamespaceRef, ReadConsumerOffsetRequest, VersionedCatalogCommand,
    VersionedCatalogResponse,
};
use zippy_types::storage::{
    CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseIssuer,
    CatalogSnapshotMemoryLeaseVerifier,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, ErrorCode, QueryId, RequestId, Result, ServerError};

use crate::SqliteCatalogConfig;
use crate::{domain, file_domain, manifest_domain, snapshot_domain, table_domain};

const SERVICE_ACTOR_THREAD_NAME: &str = "zippy-catalog-service";
const MANIFEST_PREPARATION_THREAD_NAME: &str = "zippy-catalog-manifest-prepare";
const CONTROL_READ_THREAD_NAME: &str = "zippy-catalog-control-read";
const SNAPSHOT_READ_THREAD_NAME: &str = "zippy-catalog-snapshot-read";
const REPLAY_PROBE_CAPACITY: usize = 4;

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
/// `Drop` remain synchronous. The owning server supervisor coordinates its deadline
/// and retains directory ownership until these workers have joined.
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
    snapshot_memory_lease_verifier: CatalogSnapshotMemoryLeaseVerifier,
    ownership: Arc<zippy_core::OwnedDataDirectory>,
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
    replay_probe_budget: Arc<ReplayProbeBudget>,
    runtime_snapshot_pins: Weak<crate::runtime_pins::RuntimeSnapshotPinRegistry>,
    snapshot_memory_lease_verifier: CatalogSnapshotMemoryLeaseVerifier,
    ownership: Weak<zippy_core::OwnedDataDirectory>,
}

/// Cloneable lifecycle-bound command submitter for server-owned native services.
///
/// The handle retains bounded admission budgets but only a weak writer endpoint. Keeping a clone
/// therefore cannot extend the Catalog owner lifetime or prevent deterministic shutdown.
#[derive(Clone)]
pub struct SqliteCatalogWriteHandle {
    endpoint: Weak<CatalogWriterEndpoint>,
    command_budget: Arc<CommandBudget>,
    manifest_queue_budget: Arc<ManifestQueueBudget>,
    publication_gate: Arc<Semaphore>,
    publication_owner_id: usize,
}

/// Owner-issued control capability that irreversibly closes runtime snapshot-pin admission.
///
/// The capability holds only a weak reference, so retaining it cannot extend Catalog or data-root
/// ownership. Shutdown orchestration closes this gate before sampling active pins, which prevents a
/// read handle from inserting a new pin after the zero-pin drain decision.
pub struct SqliteCatalogRuntimeSnapshotPinAdmissionControl {
    runtime_snapshot_pins: Weak<crate::runtime_pins::RuntimeSnapshotPinRegistry>,
}

impl SqliteCatalogRuntimeSnapshotPinAdmissionControl {
    /// Irreversibly closes new query-pin and garbage-collection exclusion admission.
    pub fn close_admission(&self) {
        if let Some(runtime_snapshot_pins) = self.runtime_snapshot_pins.upgrade() {
            runtime_snapshot_pins.close();
        }
    }
}

impl fmt::Debug for SqliteCatalogRuntimeSnapshotPinAdmissionControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteCatalogRuntimeSnapshotPinAdmissionControl")
            .finish_non_exhaustive()
    }
}

impl SqliteCatalog {
    /// Opens the current Catalog layout or initializes an empty database and starts its actor.
    ///
    /// Startup is synchronous and returns only after the existing foundation
    /// preflight, durable SQLite configuration, initialization, and integrity check
    /// have completed on the actor thread.
    pub fn open(config: SqliteCatalogConfig) -> Result<Self> {
        Self::open_with_backends_and_snapshot_memory_lease_issuer(
            config,
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(DomainCatalogReadBackend),
        )
        .map(|(catalog, _issuer)| catalog)
    }

    /// Opens the Catalog and returns its exclusive native-query memory-lease issuer.
    ///
    /// The paired verifier remains private to this Catalog instance and is copied into each read
    /// handle. Callers must retain the issuer in the native snapshot coordinator; a lease issued by
    /// any other Catalog or independently created authority is rejected before worker admission or
    /// runtime pin acquisition. [`Self::open`] deliberately discards this capability.
    pub fn open_with_snapshot_memory_lease_issuer(
        config: SqliteCatalogConfig,
    ) -> Result<(Self, CatalogSnapshotMemoryLeaseIssuer)> {
        Self::open_with_backends_and_snapshot_memory_lease_issuer(
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

    #[cfg(test)]
    fn open_with_backends(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
        read_backend: Arc<dyn CatalogReadBackend>,
    ) -> Result<Self> {
        Self::open_with_backends_and_snapshot_memory_lease_issuer(
            config,
            manifest_backend,
            read_backend,
        )
        .map(|(catalog, _issuer)| catalog)
    }

    fn open_with_backends_and_snapshot_memory_lease_issuer(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
        read_backend: Arc<dyn CatalogReadBackend>,
    ) -> Result<(Self, CatalogSnapshotMemoryLeaseIssuer)> {
        crate::validate_config(&config)?;
        let (snapshot_memory_lease_issuer, snapshot_memory_lease_verifier) =
            CatalogSnapshotMemoryLeaseIssuer::new_authority()?;
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
                    snapshot_memory_lease_verifier,
                    ownership,
                };
                match CatalogReadPool::start(&config, read_backend) {
                    Ok(read_pool) => {
                        catalog.read_pool = Some(read_pool);
                        Ok((catalog, snapshot_memory_lease_issuer))
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
    /// joined-worker API; the owning supervisor coordinates bounded shutdown.
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
            replay_probe_budget: Arc::clone(&self.replay_probe_budget),
            endpoint: Arc::clone(&read_pool.endpoint),
            runtime_snapshot_pins: Arc::downgrade(&self.runtime_snapshot_pins),
            snapshot_memory_lease_verifier: self.snapshot_memory_lease_verifier.clone(),
            ownership: Arc::downgrade(&self.ownership),
        })
    }

    /// Creates a lifecycle-bound submitter for ordinary server-owned Catalog commands.
    pub fn write_handle(&self) -> Result<SqliteCatalogWriteHandle> {
        self.check_runtime_health()?;
        Ok(SqliteCatalogWriteHandle {
            endpoint: Arc::downgrade(&self.writer_endpoint),
            command_budget: Arc::clone(&self.command_budget),
            manifest_queue_budget: Arc::clone(&self.manifest_queue_budget),
            publication_gate: Arc::clone(&self.publication_gate),
            publication_owner_id: self.publication_owner_id(),
        })
    }

    /// Issues the lifecycle control used to close runtime pin admission before Catalog shutdown.
    #[must_use]
    pub fn runtime_snapshot_pin_admission_control(
        &self,
    ) -> SqliteCatalogRuntimeSnapshotPinAdmissionControl {
        SqliteCatalogRuntimeSnapshotPinAdmissionControl {
            runtime_snapshot_pins: Arc::downgrade(&self.runtime_snapshot_pins),
        }
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

    /// Returns a bounded snapshot of active query-attributed runtime pins.
    #[must_use]
    pub fn runtime_query_snapshot_pins(&self) -> Vec<crate::SqliteCatalogRuntimeSnapshotPinRecord> {
        self.runtime_snapshot_pins.query_pin_records()
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
                    ControlReadMessage::ResolveTables(ResolveTablesRequest {
                        tables: tables.to_vec(),
                        memory_lease: None,
                        response: sender,
                    })
                })
            }))
            .await
            .map(|response| response.tables)
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
                        memory_lease: None,
                        response,
                    })
                })
            }))
            .await
            .map(|response| response.snapshot)
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
    /// Reads exact schemas in one bounded Catalog transaction without loading file manifests.
    pub async fn read_schemas_at_with_limits(
        &self,
        selectors: &[snapshot_domain::SqliteCatalogSchemaSelector],
        limits: snapshot_domain::SqliteCatalogSchemaReadLimits,
    ) -> Result<snapshot_domain::SqliteCatalogSchemaSnapshot> {
        snapshot_domain::validate_schema_selectors(selectors)?;
        let limits = limits.validate()?;
        let selectors = selectors.to_vec();
        await_response(
            self.endpoint
                .control_request(|response| ControlReadMessage::ReadSchemas {
                    selectors,
                    limits,
                    response,
                }),
        )
        .await
    }

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

    /// Returns active query-attributed runtime pins for diagnostics and shutdown barriers.
    pub fn runtime_query_snapshot_pins(
        &self,
    ) -> Result<Vec<crate::SqliteCatalogRuntimeSnapshotPinRecord>> {
        self.runtime_snapshot_pin_registry()
            .map(|registry| registry.query_pin_records())
    }

    /// Resolves table references while retaining one query-attributed memory reservation.
    ///
    /// Validation and reservation checks happen before the request is cloned or admitted to the
    /// control-read queue. Once admitted, the reservation follows the queued request, synchronous
    /// SQLite worker, and response. Cancellation cannot release it while worker work remains live.
    ///
    /// :param query_id: Caller-assigned query identity that must match the lease attribution.
    /// :param tables: Ordered table references to resolve atomically.
    /// :param memory_lease: Exclusive reservation covering Catalog's conservative resolution peak.
    /// :returns: Resolved descriptors in input order and the same exclusive reservation.
    /// :raises ServerError: If validation, admission, resolution, or attribution fails.
    pub async fn resolve_tables_with_memory_lease(
        &self,
        query_id: QueryId,
        tables: &[TableRef],
        memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<(Vec<TableDescriptor>, CatalogSnapshotMemoryLease)> {
        table_domain::validate_table_resolution_request(tables)?;
        let required = table_domain::conservative_table_resolution_owned_peak_bytes(tables.len())?;
        validate_catalog_memory_lease(
            &memory_lease,
            &self.snapshot_memory_lease_verifier,
            required,
            Some(query_id),
            "table resolution",
        )?;
        let tables = tables.to_vec();
        let response = await_response(self.endpoint.control_request(|response| {
            ControlReadMessage::ResolveTables(ResolveTablesRequest {
                tables,
                memory_lease: Some(memory_lease),
                response,
            })
        }))
        .await?;
        let memory_lease = response.memory_lease.ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "catalog control worker lost an owned memory reservation",
            )
            .with_query_id(query_id)
            .with_retryable(false)
        })?;
        Ok((response.tables, memory_lease))
    }

    /// Atomically admits query-attributed pins before reading exact Catalog metadata.
    ///
    /// Requests must be nonempty, in strict table-ID order, and use unique lease identities. If
    /// any pin or the Catalog read fails, every pin acquired by this call is released before
    /// returning.
    pub async fn read_pinned_query_snapshot_at(
        &self,
        query_id: QueryId,
        requests: &[crate::SqliteCatalogQuerySnapshotPinRequest],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<crate::SqliteCatalogPinnedQuerySnapshot> {
        if requests.is_empty() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "query-attributed pinned snapshot requires at least one table",
            )
            .with_query_id(query_id)
            .with_retryable(false));
        }
        let limits = limits.validate()?;
        let required = limits.conservative_owned_peak_bytes(requests.len())?;
        validate_catalog_memory_lease(
            &memory_lease,
            &self.snapshot_memory_lease_verifier,
            required,
            Some(query_id),
            "query snapshot",
        )?;
        let selectors = requests
            .iter()
            .map(|request| request.selector.clone())
            .collect::<Vec<_>>();
        snapshot_domain::validate_snapshot_selectors(&selectors)?;
        let mut lease_ids = HashSet::with_capacity(requests.len());
        if let Some(duplicate) = requests
            .iter()
            .find(|request| !lease_ids.insert(request.lease_id))
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "query snapshot pin requests contain a duplicate lease identity",
            )
            .with_query_id(query_id)
            .with_table_id(duplicate.selector.table_id)
            .with_retryable(false));
        }
        let acquired_at_ns = unix_time_ns()?;
        let ownership = self.ownership.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "sqlite catalog data-directory ownership is no longer available",
            )
            .with_retryable(false)
        })?;
        let runtime_snapshot_pins = self.runtime_snapshot_pin_registry()?;
        let mut pins = Vec::with_capacity(requests.len());
        for request in requests {
            pins.push(runtime_snapshot_pins.pin_query(
                query_id,
                request.lease_id,
                request.selector.table_id,
                request.selector.snapshot_version,
                acquired_at_ns,
            )?);
        }
        let response = self
            .read_snapshot_at_with_limits_and_memory_lease(&selectors, limits, Some(memory_lease))
            .await?;
        let memory_lease = response.memory_lease.ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "catalog snapshot worker lost an owned memory reservation",
            )
            .with_query_id(query_id)
            .with_retryable(false)
        })?;
        crate::SqliteCatalogPinnedQuerySnapshot::try_new(
            response.snapshot,
            pins,
            ownership,
            memory_lease,
        )
    }

    /// Reads exact metadata only after pinning every selected table against physical GC.
    ///
    /// Selectors must be nonempty. Pin admission precedes the Catalog read. If any table is
    /// currently excluded by a GC claim, or if the read fails, all pins already acquired by this
    /// call are released before the error is returned. The successful result must remain alive
    /// through physical file execution.
    pub async fn read_pinned_snapshot_at(
        &self,
        selectors: &[CatalogSnapshotSelector],
        memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<crate::SqliteCatalogPinnedQuerySnapshot> {
        self.read_pinned_snapshot_at_with_limits(
            selectors,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
            memory_lease,
        )
        .await
    }

    /// Reads and pins an exact snapshot under stricter caller-selected SQLite copy limits.
    pub async fn read_pinned_snapshot_at_with_limits(
        &self,
        selectors: &[CatalogSnapshotSelector],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<crate::SqliteCatalogPinnedQuerySnapshot> {
        if selectors.is_empty() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "pinned snapshot requires at least one table",
            )
            .with_retryable(false));
        }
        snapshot_domain::validate_snapshot_selectors(selectors)?;
        let limits = limits.validate()?;
        let required = limits.conservative_owned_peak_bytes(selectors.len())?;
        validate_catalog_memory_lease(
            &memory_lease,
            &self.snapshot_memory_lease_verifier,
            required,
            None,
            "snapshot",
        )?;
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
        let response = self
            .read_snapshot_at_with_limits_and_memory_lease(selectors, limits, Some(memory_lease))
            .await?;
        let memory_lease = response.memory_lease.ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "catalog snapshot worker lost an owned memory reservation",
            )
            .with_retryable(false)
        })?;
        crate::SqliteCatalogPinnedQuerySnapshot::try_new(
            response.snapshot,
            pins,
            ownership,
            memory_lease,
        )
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
        self.read_snapshot_at_with_limits_and_memory_lease(selectors, limits, None)
            .await
            .map(|response| response.snapshot)
    }

    async fn read_snapshot_at_with_limits_and_memory_lease(
        &self,
        selectors: &[CatalogSnapshotSelector],
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        memory_lease: Option<CatalogSnapshotMemoryLease>,
    ) -> Result<SnapshotReadResponse> {
        snapshot_domain::validate_snapshot_selectors(selectors)?;
        let limits = limits.validate()?;
        let selectors = selectors.to_vec();
        await_response(self.endpoint.snapshot_request(|response| {
            SnapshotReadMessage::Read(SnapshotReadRequest {
                selectors,
                limits,
                memory_lease,
                response,
            })
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
                ControlReadMessage::ResolveTables(ResolveTablesRequest {
                    tables: tables.to_vec(),
                    memory_lease: None,
                    response: sender,
                })
            }))
            .await
            .map(|response| response.tables)
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

impl CatalogConsumerRead for SqliteCatalog {
    fn read_consumer_offset(
        &self,
        request: ReadConsumerOffsetRequest,
    ) -> BoxFuture<'_, Result<ConsumerOffsetSnapshot>> {
        Box::pin(async move { self.read_handle()?.read_consumer_offset(request).await })
    }
}

impl CatalogConsumerRead for SqliteCatalogReadHandle {
    fn read_consumer_offset(
        &self,
        request: ReadConsumerOffsetRequest,
    ) -> BoxFuture<'_, Result<ConsumerOffsetSnapshot>> {
        Box::pin(async move {
            await_response(
                self.endpoint.control_request(|sender| {
                    ControlReadMessage::ReadConsumerOffset(request, sender)
                }),
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
        Box::pin(async move { self.read_handle()?.probe_command_replay(command).await })
    }
}

impl CatalogIdempotencyRead for SqliteCatalogReadHandle {
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
            let result = await_response(self.endpoint.control_request(|response| {
                ControlReadMessage::ProbeCommandReplay(CommandReplayProbeRequest {
                    key,
                    response,
                    probe_permit,
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

impl CatalogWrite for SqliteCatalogWriteHandle {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let VersionedCatalogCommand::V1(envelope) = &command;
            let request_id = envelope.request_id;
            let endpoint = self.endpoint.upgrade().ok_or_else(actor_stopped)?;
            endpoint.check_admission()?;
            let command_permit = self
                .command_budget
                .reserve()
                .map_err(|error| error.with_request_id(request_id))?;
            let publication_permit = Arc::clone(&self.publication_gate)
                .acquire_owned()
                .await
                .map_err(|_| publication_gate_stopped().with_request_id(request_id))?;
            let queued_bytes = manifest_domain::manifest_command_preflight_bytes(&command).await?;
            let manifest_permit = self
                .manifest_queue_budget
                .reserve(queued_bytes)
                .map_err(|error| error.with_request_id(request_id))?;
            let publication_permit = SqlitePublicationPermit {
                owner_id: self.publication_owner_id,
                _command_permit: command_permit,
                _publication_permit: publication_permit,
            };
            await_response(endpoint.writer_request(|response| {
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
        })
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
    fn read_consumer_offset(
        &self,
        connection: &mut Connection,
        request: ReadConsumerOffsetRequest,
    ) -> Result<ConsumerOffsetSnapshot> {
        deferred_control_read(connection, "read durable consumer snapshot", |connection| {
            crate::consumer_domain::read_consumer_offset(connection, request)
        })
    }

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

    fn read_schemas(
        &self,
        connection: &mut Connection,
        selectors: &[snapshot_domain::SqliteCatalogSchemaSelector],
        limits: snapshot_domain::SqliteCatalogSchemaReadLimits,
    ) -> Result<snapshot_domain::SqliteCatalogSchemaSnapshot> {
        snapshot_domain::read_schemas_at_with_limits(connection, selectors, limits)
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
}

fn deferred_control_read<T>(
    connection: &mut Connection,
    operation: &'static str,
    read: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error(operation, error))?;
    let result = read(&transaction)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error(operation, error))?;
    Ok(result)
}

struct DomainCatalogReadBackend;

impl CatalogReadBackend for DomainCatalogReadBackend {}

enum ControlReadMessage {
    ReadConsumerOffset(
        ReadConsumerOffsetRequest,
        oneshot::Sender<Result<ConsumerOffsetSnapshot>>,
    ),
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
    ReadSchemas {
        selectors: Vec<snapshot_domain::SqliteCatalogSchemaSelector>,
        limits: snapshot_domain::SqliteCatalogSchemaReadLimits,
        response: oneshot::Sender<Result<snapshot_domain::SqliteCatalogSchemaSnapshot>>,
    },
    ResolveTables(ResolveTablesRequest),
    InspectFiles(
        Vec<ManagedDataFileProbe>,
        oneshot::Sender<Result<CatalogDataFileReferenceSnapshot>>,
    ),
    ProbeCommandReplay(CommandReplayProbeRequest),
    Shutdown,
}

struct CommandReplayProbeRequest {
    key: domain::VerifiedCommandReplayKey,
    response: oneshot::Sender<Result<CatalogCommandReplayProbe>>,
    probe_permit: ReplayProbePermit,
}

struct ResolveTablesRequest {
    tables: Vec<TableRef>,
    memory_lease: Option<CatalogSnapshotMemoryLease>,
    response: oneshot::Sender<Result<ResolveTablesResponse>>,
}

struct ResolveTablesResponse {
    tables: Vec<TableDescriptor>,
    memory_lease: Option<CatalogSnapshotMemoryLease>,
}

enum SnapshotReadMessage {
    Read(SnapshotReadRequest),
    Shutdown,
}

struct SnapshotReadRequest {
    selectors: Vec<CatalogSnapshotSelector>,
    limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
    memory_lease: Option<CatalogSnapshotMemoryLease>,
    response: oneshot::Sender<Result<SnapshotReadResponse>>,
}

struct SnapshotReadResponse {
    snapshot: CatalogQuerySnapshot,
    memory_lease: Option<CatalogSnapshotMemoryLease>,
}

struct LeasedReadCompletion<T> {
    memory_lease: Option<CatalogSnapshotMemoryLease>,
    response: Option<oneshot::Sender<Result<T>>>,
}

impl<T> LeasedReadCompletion<T> {
    fn new(
        memory_lease: Option<CatalogSnapshotMemoryLease>,
        response: oneshot::Sender<Result<T>>,
    ) -> Self {
        Self {
            memory_lease,
            response: Some(response),
        }
    }

    fn response_is_closed(&self) -> bool {
        self.response
            .as_ref()
            .is_none_or(oneshot::Sender::is_closed)
    }

    fn send_success(mut self, build: impl FnOnce(Option<CatalogSnapshotMemoryLease>) -> T) {
        let value = build(self.memory_lease.take());
        if let Some(response) = self.response.take() {
            let _ = response.send(Ok(value));
        }
    }

    fn send_error(mut self, error: ServerError) {
        drop(self.memory_lease.take());
        if let Some(response) = self.response.take() {
            let _ = response.send(Err(error));
        }
    }
}

impl<T> Drop for LeasedReadCompletion<T> {
    fn drop(&mut self) {
        // A closed response wakes the async caller, so release the budget before exposing it.
        drop(self.memory_lease.take());
        drop(self.response.take());
    }
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
            ControlReadMessage::ReadConsumerOffset(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.read_consumer_offset(&mut connection, request));
                }
            }
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
            ControlReadMessage::ReadSchemas {
                selectors,
                limits,
                response,
            } => {
                if !response.is_closed() {
                    let _ =
                        response.send(backend.read_schemas(&mut connection, &selectors, limits));
                }
            }
            ControlReadMessage::ResolveTables(request) => {
                let ResolveTablesRequest {
                    tables,
                    memory_lease,
                    response,
                } = request;
                let completion = LeasedReadCompletion::new(memory_lease, response);
                if completion.response_is_closed() {
                    continue;
                }
                match backend.resolve_tables(&mut connection, &tables) {
                    Ok(tables) => {
                        completion.send_success(|memory_lease| ResolveTablesResponse {
                            tables,
                            memory_lease,
                        });
                    }
                    Err(error) => {
                        completion.send_error(error);
                    }
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
                    memory_lease,
                    response,
                } = request;
                let completion = LeasedReadCompletion::new(memory_lease, response);
                if completion.response_is_closed() {
                    continue;
                }
                match backend.read_snapshot(&mut connection, &selectors, limits) {
                    Ok(snapshot) => {
                        completion.send_success(|memory_lease| SnapshotReadResponse {
                            snapshot,
                            memory_lease,
                        });
                    }
                    Err(error) => {
                        completion.send_error(error);
                    }
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
    ClaimGarbageCollection(GarbageCollectionClaimRequest),
    FinalizeGarbageCollection(GarbageCollectionFinalizeRequest),
    Shutdown(Sender<Result<()>>),
}

enum WriterRequest {
    Command(CommandRequest),
    ClaimGarbageCollection(GarbageCollectionClaimRequest),
    FinalizeGarbageCollection(GarbageCollectionFinalizeRequest),
}

struct CommandRequest {
    command: VersionedCatalogCommand,
    state: CommandRequestState,
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
}

impl ManifestWorkerJob {
    fn request_id(&self) -> RequestId {
        match self {
            Self::Verify(command) => command_request_id(command),
            Self::Prepare(preparation) => preparation.request_id(),
        }
    }
}

enum ManifestWorkerCompletion {
    Verified(Result<domain::VerifiedManifestCommand>),
    Prepared(domain::ManifestPreparationResult),
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
    active_manifest: &mut Option<CommandRequestState>,
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
    active_manifest: &mut Option<CommandRequestState>,
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
    active_manifest: &mut Option<CommandRequestState>,
    request: WriterRequest,
) {
    match request {
        WriterRequest::Command(command) => {
            start_command(connection, manifest_worker, active_manifest, command);
        }
        WriterRequest::ClaimGarbageCollection(request) => request.execute(connection),
        WriterRequest::FinalizeGarbageCollection(request) => request.execute(connection),
    }
}

fn start_command(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    request: CommandRequest,
) {
    let CommandRequest { command, state } = request;
    if domain::is_manifest_command(&command) {
        submit_manifest_job(
            manifest_worker,
            active_manifest,
            ManifestWorkerJob::Verify(command),
            state,
        );
    } else {
        state.send(domain::execute_command(connection, command));
    }
}

fn handle_manifest_completion(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    completion: ManifestWorkerCompletion,
) {
    let Some(state) = active_manifest.take() else {
        tracing::error!("sqlite catalog received manifest completion without active command");
        return;
    };
    match completion {
        ManifestWorkerCompletion::Verified(Err(error)) => state.send(Err(error)),
        ManifestWorkerCompletion::Verified(Ok(verified)) => {
            match domain::begin_verified_manifest_command(connection, verified) {
                Ok(domain::ManifestCommandStart::Replay(response)) => state.send(Ok(response)),
                Ok(domain::ManifestCommandStart::Prepare(preparation)) => submit_manifest_job(
                    manifest_worker,
                    active_manifest,
                    ManifestWorkerJob::Prepare(preparation),
                    state,
                ),
                Err(error) => state.send(Err(error)),
            }
        }
        ManifestWorkerCompletion::Prepared(result) => {
            state.send(domain::finalize_manifest_preparation(connection, result));
        }
    }
}

fn submit_manifest_job(
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    job: ManifestWorkerJob,
    state: CommandRequestState,
) {
    match manifest_worker.submit(job) {
        Ok(()) => *active_manifest = Some(state),
        Err(ManifestWorkerSubmitError::Full(job)) => {
            let request_id = job.request_id();
            state.send(Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "sqlite catalog manifest preparation queue is full",
            )
            .with_request_id(request_id)));
        }
        Err(ManifestWorkerSubmitError::Stopped(job)) => {
            let request_id = job.request_id();
            state.send(Err(ServerError::new(
                ErrorCode::Unavailable,
                "sqlite catalog manifest preparation worker is not running",
            )
            .with_request_id(request_id)
            .with_retryable(false)));
        }
    }
}

fn fail_active_manifest_worker(
    active_manifest: &mut Option<CommandRequestState>,
    message: &'static str,
) {
    if let Some(state) = active_manifest.take() {
        let request_id = state.request_id;
        state.send(Err(ServerError::new(ErrorCode::Unavailable, message)
            .with_request_id(request_id)
            .with_retryable(false)));
    }
}

fn validate_catalog_memory_lease(
    memory_lease: &CatalogSnapshotMemoryLease,
    verifier: &CatalogSnapshotMemoryLeaseVerifier,
    required_bytes: NonZeroU64,
    expected_query_id: Option<QueryId>,
    operation: &'static str,
) -> Result<()> {
    if !verifier.accepts(memory_lease) {
        let error = ServerError::new(
            ErrorCode::InvalidArgument,
            format!("catalog memory lease has a foreign authority operation=[{operation}]"),
        )
        .with_retryable(false);
        return Err(match expected_query_id {
            Some(query_id) => error.with_query_id(query_id),
            None => error,
        });
    }
    let actual_instance = memory_lease.query_instance_id();
    match (expected_query_id, actual_instance) {
        (Some(expected), Some(actual)) if actual.query_id() == expected => {}
        (Some(expected), _) => {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "catalog memory lease query attribution does not match operation=[{operation}]"
                ),
            )
            .with_query_id(expected)
            .with_retryable(false));
        }
        (None, None) => {}
        (None, Some(_)) => {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!("catalog memory lease must be unattributed operation=[{operation}]"),
            )
            .with_retryable(false));
        }
    }
    let reserved_bytes = memory_lease.reserved_bytes();
    if reserved_bytes < required_bytes {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                concat!(
                    "catalog memory lease is smaller than conservative peak ",
                    "operation=[{}] reserved_bytes=[{}] required_bytes=[{}]"
                ),
                operation,
                reserved_bytes.get(),
                required_bytes.get()
            ),
        )
        .with_retryable(false));
    }
    Ok(())
}

async fn await_response<T>(response: Result<oneshot::Receiver<Result<T>>>) -> Result<T> {
    response?.await.map_err(|_| actor_stopped())?
}

fn unix_time_ns() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("system clock is before Unix epoch error=[{error}]"),
            )
            .with_retryable(false)
        })?;
    u64::try_from(duration.as_nanos()).map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "system clock Unix nanoseconds exceed u64",
        )
        .with_retryable(false)
    })
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
    use tempfile::TempDir;
    use zippy_core::OwnedDataDirectory;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
        CatalogCommandV1, CatalogDataFileProbeMatch, CatalogFileRead, CatalogGarbageCollection,
        CatalogGarbageCollectionGuard, CatalogIdempotencyRead, CatalogPublicationControl,
        CatalogPublicationPermit, CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector,
        CatalogTableSnapshot, CatalogWrite, ClaimGarbageCollectionFilesRequest, CommandFingerprint,
        CommitCompactionCommand, CommitDataFilesCommand, CreateNamespaceCommand,
        CreateTableCommand, ManagedDataFileProbe, VersionedCatalogCommand,
        VersionedCatalogResponse,
    };
    use zippy_types::query::QueryInstanceId;
    use zippy_types::storage::{CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseIssuer};
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, DataFileLocation, DataFileProvenance, DataFileState,
        ManagedDataPath, MetadataMap, MetadataScalar, NativeDataFileProvenance, PendingDataFile,
        SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition, TableRef,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
    };

    use super::{
        deferred_control_read, domain, snapshot_domain, table_domain, verify_replay_probe_command,
        CatalogReadBackend, DomainManifestPreparationBackend, ManifestPreparationBackend,
        ManifestQueueBudget, ReplayProbeBudget, SqliteCatalog, SqliteCatalogConfig,
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

    struct PanickingResolutionReadBackend;

    struct PanickingSnapshotReadBackend;

    struct CountingResolutionAndSnapshotBackend {
        resolution_calls: Arc<AtomicUsize>,
        snapshot_calls: Arc<AtomicUsize>,
    }

    struct CountingReplayReadBackend {
        probe_calls: Arc<AtomicUsize>,
    }

    struct BlockingSnapshotBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
        calls: Arc<AtomicUsize>,
    }

    struct BlockingResolutionBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
        calls: Arc<AtomicUsize>,
    }

    struct DropCountingSnapshotMemoryLease {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropCountingSnapshotMemoryLease {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn snapshot_memory_lease(
        issuer: &CatalogSnapshotMemoryLeaseIssuer,
        drops: &Arc<AtomicUsize>,
        reserved_bytes: NonZeroU64,
        query_instance_id: Option<QueryInstanceId>,
    ) -> CatalogSnapshotMemoryLease {
        let drops = Arc::clone(drops);
        issuer
            .issue_with_owner(reserved_bytes, query_instance_id, move |_| {
                Ok(Box::new(DropCountingSnapshotMemoryLease { drops }))
            })
            .expect("issue test Catalog snapshot memory lease")
    }

    fn unattributed_snapshot_memory_lease(
        issuer: &CatalogSnapshotMemoryLeaseIssuer,
        selector_count: usize,
    ) -> CatalogSnapshotMemoryLease {
        let drops = Arc::new(AtomicUsize::new(0));
        let required = snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
            .conservative_owned_peak_bytes(selector_count)
            .expect("estimate default test snapshot memory");
        snapshot_memory_lease(issuer, &drops, required, None)
    }

    fn attributed_snapshot_memory_lease(
        issuer: &CatalogSnapshotMemoryLeaseIssuer,
        drops: &Arc<AtomicUsize>,
        query_id: zippy_types::QueryId,
        limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        selector_count: usize,
    ) -> CatalogSnapshotMemoryLease {
        let required = limits
            .conservative_owned_peak_bytes(selector_count)
            .expect("estimate attributed test snapshot memory");
        snapshot_memory_lease(
            issuer,
            drops,
            required,
            Some(QueryInstanceId::new(
                query_id,
                NonZeroU64::new(1).expect("nonzero test query generation"),
            )),
        )
    }

    fn attributed_resolution_memory_lease(
        issuer: &CatalogSnapshotMemoryLeaseIssuer,
        drops: &Arc<AtomicUsize>,
        query_id: zippy_types::QueryId,
        reference_count: usize,
    ) -> CatalogSnapshotMemoryLease {
        snapshot_memory_lease(
            issuer,
            drops,
            table_domain::conservative_table_resolution_owned_peak_bytes(reference_count)
                .expect("estimate attributed test resolution memory"),
            Some(QueryInstanceId::new(
                query_id,
                NonZeroU64::new(1).expect("nonzero test query generation"),
            )),
        )
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

    impl CatalogReadBackend for BlockingResolutionBackend {
        fn resolve_tables(
            &self,
            connection: &mut rusqlite::Connection,
            tables: &[zippy_types::table::TableRef],
        ) -> zippy_types::Result<Vec<zippy_types::table::TableDescriptor>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.block_once.swap(false, Ordering::AcqRel) {
                self.started
                    .send(())
                    .expect("test resolution start observer must remain connected");
                self.release
                    .recv()
                    .expect("test resolution release must be signalled");
            }
            table_domain::resolve_tables(connection, tables)
        }
    }

    impl CatalogReadBackend for CountingResolutionAndSnapshotBackend {
        fn resolve_tables(
            &self,
            connection: &mut rusqlite::Connection,
            tables: &[zippy_types::table::TableRef],
        ) -> zippy_types::Result<Vec<zippy_types::table::TableDescriptor>> {
            self.resolution_calls.fetch_add(1, Ordering::AcqRel);
            table_domain::resolve_tables(connection, tables)
        }

        fn read_snapshot(
            &self,
            connection: &mut rusqlite::Connection,
            selectors: &[CatalogSnapshotSelector],
            limits: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        ) -> zippy_types::Result<zippy_types::catalog::CatalogQuerySnapshot> {
            self.snapshot_calls.fetch_add(1, Ordering::AcqRel);
            snapshot_domain::read_snapshot_at_with_limits(connection, selectors, limits)
        }
    }

    impl CatalogReadBackend for PanickingControlReadBackend {
        fn revision(&self, _: &rusqlite::Connection) -> zippy_types::Result<u64> {
            panic!("injected control read worker panic");
        }
    }

    impl CatalogReadBackend for PanickingResolutionReadBackend {
        fn resolve_tables(
            &self,
            _: &mut rusqlite::Connection,
            _: &[zippy_types::table::TableRef],
        ) -> zippy_types::Result<Vec<zippy_types::table::TableDescriptor>> {
            panic!("injected table resolution worker panic");
        }
    }

    impl CatalogReadBackend for PanickingSnapshotReadBackend {
        fn read_snapshot(
            &self,
            _: &mut rusqlite::Connection,
            _: &[CatalogSnapshotSelector],
            _: snapshot_domain::SqliteCatalogSnapshotReadLimits,
        ) -> zippy_types::Result<zippy_types::catalog::CatalogQuerySnapshot> {
            panic!("injected snapshot read worker panic");
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

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).expect("create IPC writer");
        writer.finish().expect("finish schema-only IPC stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("nonempty schema IPC payload")
    }

    fn wider_schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("volume", DataType::Int64, false),
        ]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).expect("create IPC writer");
        writer.finish().expect("finish schema-only IPC stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("nonempty wider schema IPC payload")
    }

    fn canonical_hash(digit: char) -> String {
        format!("sha256-v1:{}", digit.to_string().repeat(64))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_drop_is_atomic_replayable_and_preserves_pins_and_leases() {
        use zippy_types::catalog::{
            AcquireProducerCommand, AcquireSnapshotLeaseCommand, DropTableCommand,
            ReleaseSnapshotLeaseCommand,
        };
        use zippy_types::table::{SnapshotOperation, TableState};
        use zippy_types::{LeaseId, ProducerId};

        let directory = TempDir::new().unwrap();
        let data_directory = data_directory(&directory);
        write_managed_file_fixture(&data_directory, 10, &[1; 128]);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (catalog, issuer) =
            SqliteCatalog::open_with_snapshot_memory_lease_issuer(config.clone()).unwrap();
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .unwrap();
        let drop_command = |request, snapshot, revision| {
            command(
                request,
                revision,
                CatalogCommandV1::DropTable(DropTableCommand {
                    table_id: table_id(),
                    expected_snapshot_version: snapshot,
                }),
            )
        };
        let failed = catalog
            .command(drop_command(4, 1, Some(3)))
            .await
            .unwrap_err();
        assert_eq!(failed.code(), ErrorCode::Conflict);
        assert_eq!(
            catalog
                .get_table(TableRef::Id {
                    table_id: table_id()
                })
                .await
                .unwrap()
                .current_snapshot_version,
            2
        );
        let selector = CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 2,
            schema_version: 1,
        };
        let handle = catalog.read_handle().unwrap();
        let pinned = handle
            .read_pinned_snapshot_at(
                std::slice::from_ref(&selector),
                unattributed_snapshot_memory_lease(&issuer, 1),
            )
            .await
            .unwrap();
        let lease_id = LeaseId::from_u128(99).unwrap();
        catalog
            .command(command(
                5,
                Some(3),
                CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
                    lease_id,
                    table_id: table_id(),
                    snapshot_version: 2,
                    owner: "drop-test".to_owned(),
                    expires_at_ns: crate::domain::unix_time_ns().unwrap() + 60_000_000_000,
                }),
            ))
            .await
            .unwrap();
        let command = drop_command(6, 2, Some(4));
        let response = catalog.command(command.clone()).await.unwrap();
        assert_eq!(catalog.command(command.clone()).await.unwrap(), response);
        assert_eq!(
            catalog
                .get_table(TableRef::Id {
                    table_id: table_id()
                })
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            catalog
                .command(drop_command(7, 2, None))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        let historical = catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .unwrap();
        assert_eq!(historical.tables()[0].files().len(), 1);
        assert_eq!(historical.tables()[0].table().state, TableState::Dropped);
        assert_eq!(historical.tables()[0].table().writer_epoch, 1);
        let empty = catalog
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 3,
                schema_version: 1,
            }])
            .await
            .unwrap();
        assert!(empty.tables()[0].files().is_empty());
        assert_eq!(
            empty.tables()[0].snapshot().operation,
            SnapshotOperation::Retention
        );
        assert_eq!(
            empty.tables()[0].snapshot().committed_log_offset,
            OffsetWatermark::Known(LogOffset::new(0))
        );
        assert_eq!(
            catalog
                .command(commit_data_file(8, 11, 5, 3, 1))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        assert_eq!(
            catalog
                .command(self::command(
                    9,
                    None,
                    CatalogCommandV1::AcquireProducer(AcquireProducerCommand {
                        table_id: table_id(),
                        producer_id: ProducerId::from_u128(1).unwrap(),
                        expected_writer_epoch: Some(0)
                    },)
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        let replacement_id = TableId::from_u128(202).unwrap();
        let replacement = CreateTableCommand {
            table_id: replacement_id,
            namespace_id: NamespaceId::from_u128(NAMESPACE_ID).unwrap(),
            name: CatalogName::new("ticks").unwrap(),
            definition: TableDefinition::Append(AppendTableDefinition {
                event_time_column: None,
            }),
            schema_ipc: schema_ipc(),
            config: BTreeMap::new(),
        };
        catalog
            .command(self::command(
                20,
                None,
                CatalogCommandV1::CreateTable(replacement.clone()),
            ))
            .await
            .unwrap();
        let name = TableRef::Name {
            namespace: CatalogName::new("market").unwrap(),
            table: CatalogName::new("ticks").unwrap(),
        };
        assert_eq!(
            catalog.get_table(name.clone()).await.unwrap().table_id,
            replacement_id
        );
        let mut conflicting = replacement.clone();
        conflicting.table_id = TableId::from_u128(203).unwrap();
        assert_eq!(
            catalog
                .command(self::command(
                    21,
                    None,
                    CatalogCommandV1::CreateTable(conflicting)
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::AlreadyExists
        );
        let mut reused = replacement;
        reused.table_id = table_id();
        reused.name = CatalogName::new("different_name").unwrap();
        assert_eq!(
            catalog
                .command(self::command(
                    22,
                    None,
                    CatalogCommandV1::CreateTable(reused)
                ))
                .await
                .unwrap_err()
                .code(),
            ErrorCode::AlreadyExists
        );
        let old_history = catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .unwrap();
        assert_eq!(old_history.tables()[0].table().table_id, table_id());
        assert_eq!(old_history.tables()[0].table().name.as_str(), "ticks");
        assert_eq!(old_history.tables()[0].files().len(), 1);
        assert_eq!(catalog.command(command.clone()).await.unwrap(), response);
        assert_eq!(
            catalog.get_table(name.clone()).await.unwrap().table_id,
            replacement_id
        );
        let claim_request = ClaimGarbageCollectionFilesRequest {
            table_id: table_id(),
            retry_limit: 1,
            new_claim_limit: 1,
        };
        let protected = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .unwrap(),
                claim_request.clone(),
            )
            .await
            .unwrap();
        assert!(protected.files.is_empty());
        // Releasing the durable lease still cannot collect a live query's files.
        catalog
            .command(self::command(
                10,
                None,
                CatalogCommandV1::ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand {
                    lease_id,
                    owner: "drop-test".to_owned(),
                }),
            ))
            .await
            .unwrap();
        let protected = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .unwrap(),
                claim_request.clone(),
            )
            .await
            .unwrap();
        assert!(protected.files.is_empty());
        drop(pinned);
        let claim = catalog
            .claim_garbage_collection_files(
                catalog
                    .acquire_garbage_collection_guard(table_id())
                    .unwrap(),
                claim_request,
            )
            .await
            .unwrap();
        assert_eq!(claim.files.len(), 1);
        assert_eq!(claim.files[0].file_id(), FileId::from_u128(10).unwrap());
        drop(handle);
        catalog.close().unwrap();
        let reopened = SqliteCatalog::open(config).unwrap();
        assert_eq!(reopened.command(command).await.unwrap(), response);
        assert_eq!(
            reopened
                .get_table(TableRef::Id {
                    table_id: table_id()
                })
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            reopened.get_table(name).await.unwrap().table_id,
            replacement_id
        );
        reopened.close().unwrap();
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

    fn blocking_resolution_backend() -> (
        Arc<dyn CatalogReadBackend>,
        crossbeam_channel::Receiver<()>,
        crossbeam_channel::Sender<()>,
        Arc<AtomicUsize>,
    ) {
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(BlockingResolutionBackend {
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

        let original = commit_data_file(3, 10, 2, 1, 0);
        assert!(matches!(
            handle.probe_command_replay(&original).await.unwrap(),
            CatalogCommandReplayProbe::Exact { .. }
        ));
        let weak_owner = Arc::downgrade(&catalog);
        let catalog = Arc::try_unwrap(catalog)
            .unwrap_or_else(|_| panic!("read handle must not retain Catalog owner"));
        assert!(weak_owner.upgrade().is_none());
        let cold_revision = handle.revision();
        catalog.close().expect("close Catalog with retained handle");
        let replay_error = handle.probe_command_replay(&original).await.unwrap_err();
        assert_eq!(replay_error.code(), ErrorCode::Unavailable);
        assert!(!replay_error.retryable());

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
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
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
            .read_pinned_snapshot_at(
                &[CatalogSnapshotSelector {
                    table_id: table_id(),
                    snapshot_version: 2,
                    schema_version: 1,
                }],
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 1),
            )
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
        let mut first_range = [u8::MAX; 4];
        let mut second_range = [u8::MAX; 3];
        reader
            .read_exact_at(10, &mut first_range)
            .expect("read first positioned range");
        second_reader
            .read_exact_at(64, &mut second_range)
            .expect("read second positioned range");
        assert_eq!(first_range, [10, 11, 12, 13]);
        assert_eq!(second_range, [64, 65, 66]);
        let range_error = reader
            .read_exact_at(127, &mut [0_u8; 2])
            .expect_err("positioned range beyond verified length must fail");
        assert_eq!(range_error.kind(), std::io::ErrorKind::UnexpectedEof);

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

    #[tokio::test(flavor = "current_thread")]
    async fn attributed_multi_table_read_failure_rolls_back_all_runtime_pins() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(41).expect("nonzero query id");
        let empty_attributed = handle
            .read_pinned_query_snapshot_at(
                query_id,
                &[],
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                attributed_snapshot_memory_lease(
                    &memory_lease_issuer,
                    &Arc::new(AtomicUsize::new(0)),
                    query_id,
                    snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                    1,
                ),
            )
            .await
            .expect_err("empty attributed pin must not retain uncounted ownership");
        assert_eq!(empty_attributed.code(), ErrorCode::InvalidArgument);
        let empty_generic = handle
            .read_pinned_snapshot_at(
                &[],
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 1),
            )
            .await
            .expect_err("empty generic pin must not retain uncounted ownership");
        assert_eq!(empty_generic.code(), ErrorCode::InvalidArgument);
        let requests = vec![
            crate::SqliteCatalogQuerySnapshotPinRequest {
                selector: CatalogSnapshotSelector {
                    table_id: table_id(),
                    snapshot_version: 1,
                    schema_version: 1,
                },
                lease_id: zippy_types::LeaseId::from_u128(42).expect("nonzero lease id"),
            },
            crate::SqliteCatalogQuerySnapshotPinRequest {
                selector: CatalogSnapshotSelector {
                    table_id: TableId::from_u128(3).expect("nonzero missing table id"),
                    snapshot_version: 1,
                    schema_version: 1,
                },
                lease_id: zippy_types::LeaseId::from_u128(43).expect("nonzero lease id"),
            },
        ];
        let error = handle
            .read_pinned_query_snapshot_at(
                query_id,
                &requests,
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                attributed_snapshot_memory_lease(
                    &memory_lease_issuer,
                    &Arc::new(AtomicUsize::new(0)),
                    query_id,
                    snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                    requests.len(),
                ),
            )
            .await
            .expect_err("missing second table must fail the exact read");
        assert_eq!(error.code(), ErrorCode::NotFound);
        let status = handle
            .runtime_snapshot_pin_status()
            .expect("read pin status after rollback");
        assert_eq!(status.active_pins, 0);
        assert_eq!(status.active_query_pins, 0);
        assert!(handle
            .runtime_query_snapshot_pins()
            .expect("read query pin audit records")
            .is_empty());
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn attributed_snapshot_memory_lease_follows_success_and_read_error_ownership() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(44).expect("nonzero query id");
        let request = crate::SqliteCatalogQuerySnapshotPinRequest {
            selector: CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 1,
                schema_version: 1,
            },
            lease_id: zippy_types::LeaseId::from_u128(45).expect("nonzero lease id"),
        };

        let success_drops = Arc::new(AtomicUsize::new(0));
        let pinned = handle
            .read_pinned_query_snapshot_at(
                query_id,
                std::slice::from_ref(&request),
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                attributed_snapshot_memory_lease(
                    &memory_lease_issuer,
                    &success_drops,
                    query_id,
                    snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                    1,
                ),
            )
            .await
            .expect("build attributed pinned snapshot");
        let memory_descriptor = pinned.memory_lease_descriptor();
        assert_eq!(
            memory_descriptor
                .query_instance_id()
                .expect("query-attributed pinned reservation")
                .query_id(),
            query_id
        );
        assert_eq!(
            memory_descriptor.reserved_bytes(),
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
                .conservative_owned_peak_bytes(1)
                .expect("estimate pinned reservation")
        );
        assert_eq!(success_drops.load(Ordering::Acquire), 0);
        let manifest = pinned.table(0).expect("retain one pinned table owner");
        drop(pinned);
        assert_eq!(success_drops.load(Ordering::Acquire), 0);
        drop(manifest);
        assert_eq!(success_drops.load(Ordering::Acquire), 1);

        let error_drops = Arc::new(AtomicUsize::new(0));
        let error_request = crate::SqliteCatalogQuerySnapshotPinRequest {
            lease_id: zippy_types::LeaseId::from_u128(46).expect("nonzero lease id"),
            ..request
        };
        let error_limits = snapshot_domain::SqliteCatalogSnapshotReadLimits {
            max_raw_response_bytes: 1,
            ..snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
        };
        let error = handle
            .read_pinned_query_snapshot_at(
                query_id,
                std::slice::from_ref(&error_request),
                error_limits,
                attributed_snapshot_memory_lease(
                    &memory_lease_issuer,
                    &error_drops,
                    query_id,
                    error_limits,
                    1,
                ),
            )
            .await
            .expect_err("snapshot read error must release its memory reservation");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(error_drops.load(Ordering::Acquire), 1);
        assert_eq!(
            handle
                .runtime_snapshot_pin_status()
                .expect("read pin status after memory-budget failure")
                .active_pins,
            0
        );
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pinned_schema_charges_follow_deterministic_table_ordinals() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        let second_table_id = TableId::from_u128(3).expect("nonzero second table id");
        catalog
            .command(command(
                3,
                Some(2),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: second_table_id,
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("quotes").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition {
                        event_time_column: None,
                    }),
                    schema_ipc: wider_schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create second append table");
        let selectors = [
            CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 1,
                schema_version: 1,
            },
            CatalogSnapshotSelector {
                table_id: second_table_id,
                snapshot_version: 1,
                schema_version: 1,
            },
        ];
        let expected = catalog
            .read_snapshot_at(&selectors)
            .await
            .expect("read expected exact schemas")
            .tables()
            .iter()
            .map(|table| {
                crate::decode_persisted_table_schema(table.table(), table.schema())
                    .expect("decode expected persisted schema")
                    .retained_charge_bytes()
            })
            .collect::<Vec<_>>();
        assert_ne!(expected[0], expected[1]);

        let pinned = catalog
            .read_handle()
            .expect("create read handle")
            .read_pinned_snapshot_at(
                &selectors,
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 2),
            )
            .await
            .expect("pin exact two-table snapshot");
        let manifests = pinned.tables().collect::<Vec<_>>();
        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].selector().table_id, table_id());
        assert_eq!(manifests[1].selector().table_id, second_table_id);
        assert_eq!(
            manifests
                .iter()
                .map(|table| table.retained_schema_charge_bytes().get())
                .collect::<Vec<_>>(),
            expected
        );

        drop((manifests, pinned));
        assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 0);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pinned_schema_sidecar_failure_rolls_back_runtime_pins() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        let selector = CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        };
        let snapshot = catalog
            .read_snapshot_at(std::slice::from_ref(&selector))
            .await
            .expect("read valid exact snapshot");
        let valid_table = &snapshot.tables()[0];
        let mut corrupt_schema = valid_table.schema().clone();
        corrupt_schema.schema_hash = canonical_hash('f');
        let corrupt_table = CatalogTableSnapshot::new(
            selector,
            valid_table.table().clone(),
            corrupt_schema,
            valid_table.snapshot().clone(),
            valid_table.files().clone(),
        )
        .expect("aggregate permits an independently corrupted schema payload");
        let corrupt_snapshot =
            CatalogQuerySnapshot::new(snapshot.catalog_revision(), vec![corrupt_table])
                .expect("build corrupt exact snapshot fixture");
        let pin = catalog
            .pin_runtime_snapshot(table_id(), 1)
            .expect("pin runtime snapshot before sidecar validation");
        assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 1);
        let memory_lease_drops = Arc::new(AtomicUsize::new(0));

        let error = crate::SqliteCatalogPinnedQuerySnapshot::try_new(
            corrupt_snapshot,
            vec![pin],
            Arc::clone(&data_directory),
            snapshot_memory_lease(
                &memory_lease_issuer,
                &memory_lease_drops,
                NonZeroU64::new(1).expect("nonzero direct test reservation"),
                None,
            ),
        )
        .expect_err("corrupt persisted schema must reject pinned owner construction");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 0);
        assert_eq!(memory_lease_drops.load(Ordering::Acquire), 1);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pinned_snapshot_rebind_requires_unique_matching_admission() {
        let directory = TempDir::new().unwrap();
        let data_directory = data_directory(&directory);
        let (catalog, issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .unwrap();
        create_append_table(&catalog).await;
        let mut pinned = catalog
            .read_handle()
            .unwrap()
            .read_pinned_snapshot_at(
                &[CatalogSnapshotSelector {
                    table_id: table_id(),
                    snapshot_version: 1,
                    schema_version: 1,
                }],
                unattributed_snapshot_memory_lease(&issuer, 1),
            )
            .await
            .unwrap();
        let original = pinned.memory_lease_descriptor();
        let required = pinned.retained_memory_bytes();
        assert!(required.get() > 64 * 1024);
        assert!(required < original.reserved_bytes());
        let drops = Arc::new(AtomicUsize::new(0));
        let (foreign, _) = CatalogSnapshotMemoryLeaseIssuer::new_authority().unwrap();
        assert_eq!(
            pinned
                .rebind_memory_lease(snapshot_memory_lease(&foreign, &drops, required, None))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            pinned
                .rebind_memory_lease(snapshot_memory_lease(
                    &issuer,
                    &drops,
                    NonZeroU64::new(1).unwrap(),
                    None
                ))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        let query = QueryInstanceId::new(
            zippy_types::QueryId::from_u128(90).unwrap(),
            NonZeroU64::new(1).unwrap(),
        );
        assert_eq!(
            pinned
                .rebind_memory_lease(snapshot_memory_lease(
                    &issuer,
                    &drops,
                    required,
                    Some(query)
                ))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        let table = pinned.table(0).unwrap();
        assert_eq!(
            pinned
                .rebind_memory_lease(snapshot_memory_lease(&issuer, &drops, required, None))
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        assert_eq!(pinned.memory_lease_descriptor(), original);
        drop(table);
        pinned
            .rebind_memory_lease(snapshot_memory_lease(&issuer, &drops, required, None))
            .unwrap();
        assert_eq!(pinned.memory_lease_descriptor().reserved_bytes(), required);
        assert_ne!(
            pinned.memory_lease_descriptor().reservation_id(),
            original.reservation_id()
        );
        assert_eq!(drops.load(Ordering::Acquire), 4);
        drop(pinned);
        assert_eq!(drops.load(Ordering::Acquire), 5);
        assert_eq!(catalog.runtime_snapshot_pin_status().active_pins, 0);
        catalog.close().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn pinned_reader_rejects_unsafe_or_changed_managed_files_without_path_disclosure() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let physical_file = write_managed_file_fixture(&data_directory, 10, &[0x5a; 127]);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        catalog
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("commit physical file metadata");
        let pinned = catalog
            .read_handle()
            .expect("create read handle")
            .read_pinned_snapshot_at(
                &[CatalogSnapshotSelector {
                    table_id: table_id(),
                    snapshot_version: 2,
                    schema_version: 1,
                }],
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 1),
            )
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
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_snapshot_memory_lease_issuer(config.clone())
                .expect("open catalog");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let pinned = handle
            .read_pinned_snapshot_at(
                &selectors,
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 1),
            )
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
            .read_pinned_snapshot_at(
                &selectors,
                unattributed_snapshot_memory_lease(&memory_lease_issuer, 1),
            )
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
    async fn leased_table_resolution_returns_ownership_and_releases_it_on_read_error() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) = SqliteCatalog::open_with_snapshot_memory_lease_issuer(
            SqliteCatalogConfig::new(&data_directory),
        )
        .expect("open catalog");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(81).expect("nonzero query id");
        let references = [TableRef::from(table_id())];

        let success_drops = Arc::new(AtomicUsize::new(0));
        let (tables, memory_lease) = handle
            .resolve_tables_with_memory_lease(
                query_id,
                &references,
                attributed_resolution_memory_lease(
                    &memory_lease_issuer,
                    &success_drops,
                    query_id,
                    references.len(),
                ),
            )
            .await
            .expect("resolve one table with retained admission");
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_id, table_id());
        assert_eq!(
            memory_lease
                .query_instance_id()
                .expect("query-attributed resolution lease")
                .query_id(),
            query_id
        );
        assert_eq!(success_drops.load(Ordering::Acquire), 0);
        drop(tables);
        assert_eq!(success_drops.load(Ordering::Acquire), 0);
        drop(memory_lease);
        assert_eq!(success_drops.load(Ordering::Acquire), 1);

        let error_drops = Arc::new(AtomicUsize::new(0));
        let missing = [TableRef::from(
            TableId::from_u128(999).expect("nonzero missing table id"),
        )];
        let error = handle
            .resolve_tables_with_memory_lease(
                query_id,
                &missing,
                attributed_resolution_memory_lease(
                    &memory_lease_issuer,
                    &error_drops,
                    query_id,
                    missing.len(),
                ),
            )
            .await
            .expect_err("resolution error must release worker-owned admission");
        assert_eq!(error.code(), ErrorCode::NotFound);
        assert_eq!(error_drops.load(Ordering::Acquire), 1);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn memory_lease_validation_precedes_resolution_worker_and_runtime_pins() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let resolution_calls = Arc::new(AtomicUsize::new(0));
        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                SqliteCatalogConfig::new(&data_directory),
                Arc::new(DomainManifestPreparationBackend),
                Arc::new(CountingResolutionAndSnapshotBackend {
                    resolution_calls: Arc::clone(&resolution_calls),
                    snapshot_calls: Arc::clone(&snapshot_calls),
                }),
            )
            .expect("open catalog with counting read backend");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(82).expect("nonzero query id");
        let references = [TableRef::from(table_id())];

        let (foreign_issuer, _foreign_verifier) = CatalogSnapshotMemoryLeaseIssuer::new_authority()
            .expect("create foreign memory lease authority");
        let foreign_resolution_drops = Arc::new(AtomicUsize::new(0));
        let foreign_resolution = snapshot_memory_lease(
            &foreign_issuer,
            &foreign_resolution_drops,
            table_domain::conservative_table_resolution_owned_peak_bytes(references.len())
                .expect("estimate foreign resolution reservation"),
            Some(QueryInstanceId::new(
                query_id,
                NonZeroU64::new(1).expect("nonzero query generation"),
            )),
        );
        let error = handle
            .resolve_tables_with_memory_lease(query_id, &references, foreign_resolution)
            .await
            .expect_err("foreign issuer must fail before resolution admission");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("foreign authority"));
        assert_eq!(foreign_resolution_drops.load(Ordering::Acquire), 1);
        assert_eq!(resolution_calls.load(Ordering::Acquire), 0);

        let insufficient_resolution_drops = Arc::new(AtomicUsize::new(0));
        let insufficient_resolution = snapshot_memory_lease(
            &memory_lease_issuer,
            &insufficient_resolution_drops,
            NonZeroU64::new(1).expect("nonzero insufficient reservation"),
            Some(QueryInstanceId::new(
                query_id,
                NonZeroU64::new(1).expect("nonzero query generation"),
            )),
        );
        let error = handle
            .resolve_tables_with_memory_lease(query_id, &references, insufficient_resolution)
            .await
            .expect_err("insufficient resolution lease must fail before enqueue");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(insufficient_resolution_drops.load(Ordering::Acquire), 1);
        assert_eq!(resolution_calls.load(Ordering::Acquire), 0);

        let wrong_query_drops = Arc::new(AtomicUsize::new(0));
        let wrong_query_id = zippy_types::QueryId::from_u128(83).expect("nonzero wrong query id");
        let error = handle
            .resolve_tables_with_memory_lease(
                query_id,
                &references,
                attributed_resolution_memory_lease(
                    &memory_lease_issuer,
                    &wrong_query_drops,
                    wrong_query_id,
                    references.len(),
                ),
            )
            .await
            .expect_err("cross-query resolution lease must fail before enqueue");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert_eq!(wrong_query_drops.load(Ordering::Acquire), 1);
        assert_eq!(resolution_calls.load(Ordering::Acquire), 0);

        let selector = CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        };
        let foreign_snapshot_drops = Arc::new(AtomicUsize::new(0));
        let foreign_snapshot = snapshot_memory_lease(
            &foreign_issuer,
            &foreign_snapshot_drops,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
                .conservative_owned_peak_bytes(1)
                .expect("estimate foreign snapshot reservation"),
            None,
        );
        let error = handle
            .read_pinned_snapshot_at(std::slice::from_ref(&selector), foreign_snapshot)
            .await
            .expect_err("foreign issuer must fail before runtime pin admission");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("foreign authority"));
        assert_eq!(foreign_snapshot_drops.load(Ordering::Acquire), 1);
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 0);
        assert_eq!(
            handle
                .runtime_snapshot_pin_status()
                .expect("read pin status after foreign lease")
                .active_pins,
            0
        );

        let foreign_query_drops = Arc::new(AtomicUsize::new(0));
        let foreign_query_request = crate::SqliteCatalogQuerySnapshotPinRequest {
            selector: selector.clone(),
            lease_id: zippy_types::LeaseId::from_u128(84).expect("nonzero lease id"),
        };
        let foreign_query_lease = snapshot_memory_lease(
            &foreign_issuer,
            &foreign_query_drops,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default()
                .conservative_owned_peak_bytes(1)
                .expect("estimate foreign query snapshot reservation"),
            Some(QueryInstanceId::new(
                query_id,
                NonZeroU64::new(1).expect("nonzero query generation"),
            )),
        );
        let error = handle
            .read_pinned_query_snapshot_at(
                query_id,
                std::slice::from_ref(&foreign_query_request),
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                foreign_query_lease,
            )
            .await
            .expect_err("foreign issuer must fail before query pin admission");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("foreign authority"));
        assert_eq!(foreign_query_drops.load(Ordering::Acquire), 1);
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 0);
        assert_eq!(
            handle
                .runtime_snapshot_pin_status()
                .expect("read query pin status after foreign lease")
                .active_query_pins,
            0
        );

        let insufficient_snapshot_drops = Arc::new(AtomicUsize::new(0));
        let error = handle
            .read_pinned_snapshot_at(
                std::slice::from_ref(&selector),
                snapshot_memory_lease(
                    &memory_lease_issuer,
                    &insufficient_snapshot_drops,
                    NonZeroU64::new(1).expect("nonzero insufficient reservation"),
                    None,
                ),
            )
            .await
            .expect_err("insufficient snapshot lease must fail before pin admission");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(insufficient_snapshot_drops.load(Ordering::Acquire), 1);
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 0);
        assert_eq!(
            handle
                .runtime_snapshot_pin_status()
                .expect("read pin status after rejected lease")
                .active_pins,
            0
        );

        let query_pin_drops = Arc::new(AtomicUsize::new(0));
        let request = crate::SqliteCatalogQuerySnapshotPinRequest {
            selector,
            lease_id: zippy_types::LeaseId::from_u128(84).expect("nonzero lease id"),
        };
        let error = handle
            .read_pinned_query_snapshot_at(
                query_id,
                std::slice::from_ref(&request),
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                snapshot_memory_lease(
                    &memory_lease_issuer,
                    &query_pin_drops,
                    NonZeroU64::new(1).expect("nonzero insufficient reservation"),
                    Some(QueryInstanceId::new(
                        query_id,
                        NonZeroU64::new(1).expect("nonzero query generation"),
                    )),
                ),
            )
            .await
            .expect_err("insufficient query snapshot lease must fail before pin admission");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(query_pin_drops.load(Ordering::Acquire), 1);
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 0);
        assert_eq!(
            handle
                .runtime_snapshot_pin_status()
                .expect("read query pin status after rejected lease")
                .active_query_pins,
            0
        );
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_queued_resolution_retains_lease_until_control_worker_dequeues_it() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (read_backend, started, release, calls) = blocking_resolution_backend();
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                SqliteCatalogConfig::new(&data_directory).with_control_read_capacity(1),
                Arc::new(DomainManifestPreparationBackend),
                read_backend,
            )
            .expect("open catalog with one queued control slot");
        create_append_table(&catalog).await;
        let references = [TableRef::from(table_id())];
        let mut active = catalog.resolve_tables(&references);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(active.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("first resolution must occupy its worker");

        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(85).expect("nonzero query id");
        let lease_drops = Arc::new(AtomicUsize::new(0));
        let mut queued = Box::pin(handle.resolve_tables_with_memory_lease(
            query_id,
            &references,
            attributed_resolution_memory_lease(
                &memory_lease_issuer,
                &lease_drops,
                query_id,
                references.len(),
            ),
        ));
        assert!(matches!(queued.as_mut().poll(&mut context), Poll::Pending));
        let full = catalog
            .revision()
            .await
            .expect_err("active and queued resolutions must saturate the control lane");
        assert_eq!(full.code(), ErrorCode::ResourceExhausted);
        drop(queued);
        assert_eq!(lease_drops.load(Ordering::Acquire), 0);

        release.send(()).expect("release active resolution");
        active.await.expect("finish active resolution");
        tokio::time::timeout(Duration::from_secs(2), async {
            while lease_drops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled queued resolution must eventually release its lease");
        assert_eq!(lease_drops.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_inflight_resolution_retains_lease_until_sqlite_work_exits() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (read_backend, started, release, calls) = blocking_resolution_backend();
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                SqliteCatalogConfig::new(&data_directory),
                Arc::new(DomainManifestPreparationBackend),
                read_backend,
            )
            .expect("open catalog with blocking resolution backend");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(86).expect("nonzero query id");
        let references = [TableRef::from(table_id())];
        let lease_drops = Arc::new(AtomicUsize::new(0));
        let mut resolution = Box::pin(handle.resolve_tables_with_memory_lease(
            query_id,
            &references,
            attributed_resolution_memory_lease(
                &memory_lease_issuer,
                &lease_drops,
                query_id,
                references.len(),
            ),
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            resolution.as_mut().poll(&mut context),
            Poll::Pending
        ));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("leased resolution must enter SQLite work");

        drop(resolution);
        assert_eq!(lease_drops.load(Ordering::Acquire), 0);
        release.send(()).expect("release in-flight resolution");
        tokio::time::timeout(Duration::from_secs(2), async {
            while lease_drops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled in-flight resolution must release after SQLite exits");
        assert_eq!(lease_drops.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicked_resolution_worker_drops_its_inflight_memory_lease() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                SqliteCatalogConfig::new(&data_directory),
                Arc::new(DomainManifestPreparationBackend),
                Arc::new(PanickingResolutionReadBackend),
            )
            .expect("open catalog with panicking resolution backend");
        let handle = catalog.read_handle().expect("create read handle");
        let query_id = zippy_types::QueryId::from_u128(87).expect("nonzero query id");
        let references = [TableRef::from(table_id())];
        let lease_drops = Arc::new(AtomicUsize::new(0));

        let error = match handle
            .resolve_tables_with_memory_lease(
                query_id,
                &references,
                attributed_resolution_memory_lease(
                    &memory_lease_issuer,
                    &lease_drops,
                    query_id,
                    references.len(),
                ),
            )
            .await
        {
            Ok(_) => panic!("panicked worker unexpectedly returned a leased resolution"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(lease_drops.load(Ordering::Acquire), 1);

        let close_error = catalog
            .close()
            .expect_err("joining the panicked control worker must fail");
        assert_eq!(close_error.code(), ErrorCode::Internal);
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
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
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

        let handle = catalog.read_handle().expect("create read handle");
        let queued_lease_drops = Arc::new(AtomicUsize::new(0));
        let mut cancelled = Box::pin(handle.read_snapshot_at_with_limits_and_memory_lease(
            &selectors,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
            Some(snapshot_memory_lease(
                &memory_lease_issuer,
                &queued_lease_drops,
                NonZeroU64::new(1).expect("nonzero queued test reservation"),
                None,
            )),
        ));
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
        assert_eq!(queued_lease_drops.load(Ordering::Acquire), 0);

        release.send(()).expect("release active snapshot");
        active.await.expect("finish active snapshot");
        tokio::time::timeout(Duration::from_secs(2), async {
            while queued_lease_drops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled queued request must release its memory lease");
        assert_eq!(queued_lease_drops.load(Ordering::Acquire), 1);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_inflight_snapshot_retains_memory_lease_until_sqlite_read_exits() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (read_backend, started, release, calls) = blocking_snapshot_backend();
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                config,
                Arc::new(DomainManifestPreparationBackend),
                read_backend,
            )
            .expect("open catalog with blocking snapshot backend");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let lease_drops = Arc::new(AtomicUsize::new(0));
        let mut snapshot = Box::pin(handle.read_snapshot_at_with_limits_and_memory_lease(
            &selectors,
            snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
            Some(snapshot_memory_lease(
                &memory_lease_issuer,
                &lease_drops,
                NonZeroU64::new(1).expect("nonzero in-flight test reservation"),
                None,
            )),
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            snapshot.as_mut().poll(&mut context),
            Poll::Pending
        ));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("leased snapshot must enter its SQLite read");

        drop(snapshot);
        assert_eq!(lease_drops.load(Ordering::Acquire), 0);
        release.send(()).expect("release in-flight snapshot read");
        tokio::time::timeout(Duration::from_secs(2), async {
            while lease_drops.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed response must drop the returned memory lease");
        assert_eq!(lease_drops.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::Acquire), 1);
        catalog
            .close()
            .expect("close catalog after in-flight cancellation");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicked_snapshot_worker_drops_its_inflight_memory_lease() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let (catalog, memory_lease_issuer) =
            SqliteCatalog::open_with_backends_and_snapshot_memory_lease_issuer(
                SqliteCatalogConfig::new(&data_directory),
                Arc::new(DomainManifestPreparationBackend),
                Arc::new(PanickingSnapshotReadBackend),
            )
            .expect("open catalog with panicking snapshot read backend");
        create_append_table(&catalog).await;
        let handle = catalog.read_handle().expect("create read handle");
        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let lease_drops = Arc::new(AtomicUsize::new(0));

        let error = match handle
            .read_snapshot_at_with_limits_and_memory_lease(
                &selectors,
                snapshot_domain::SqliteCatalogSnapshotReadLimits::default(),
                Some(snapshot_memory_lease(
                    &memory_lease_issuer,
                    &lease_drops,
                    NonZeroU64::new(1).expect("nonzero panic test reservation"),
                    None,
                )),
            )
            .await
        {
            Ok(_) => panic!("panicked worker unexpectedly returned a leased snapshot"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(lease_drops.load(Ordering::Acquire), 1);

        let close_error = catalog
            .close()
            .expect_err("joining the panicked snapshot worker must fail");
        assert_eq!(close_error.code(), ErrorCode::Internal);
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

    #[test]
    fn deferred_control_read_keeps_multi_query_state_on_one_wal_snapshot() {
        let directory = TempDir::new().expect("create temp directory");
        let database_path = directory.path().join("deferred-read.db");
        let setup = rusqlite::Connection::open(&database_path).expect("open SQLite fixture");
        setup
            .execute_batch(
                "PRAGMA journal_mode = WAL; \
                 CREATE TABLE projection(value INTEGER NOT NULL); \
                 CREATE TABLE source(value INTEGER NOT NULL); \
                 INSERT INTO projection VALUES (1); \
                 INSERT INTO source VALUES (1);",
            )
            .expect("initialize deferred-read fixture");
        drop(setup);

        let mut reader = rusqlite::Connection::open(&database_path).expect("open reader");
        let writer = rusqlite::Connection::open(&database_path).expect("open writer");
        let (read_started_sender, read_started_receiver) = crossbeam_channel::bounded(1);
        let (write_done_sender, write_done_receiver) = crossbeam_channel::bounded(1);
        let writer_thread = std::thread::spawn(move || {
            read_started_receiver
                .recv()
                .expect("reader must establish its snapshot");
            writer
                .execute_batch(
                    "BEGIN IMMEDIATE; \
                     UPDATE projection SET value = 2; \
                     UPDATE source SET value = 2; \
                     COMMIT;",
                )
                .expect("commit concurrent fixture update");
            write_done_sender.send(()).expect("signal committed update");
        });

        let observed =
            deferred_control_read(&mut reader, "test deferred control read", |transaction| {
                let projection = transaction
                    .query_row("SELECT value FROM projection", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(|error| crate::map_sqlite_error("read projection fixture", error))?;
                read_started_sender
                    .send(())
                    .expect("signal established read snapshot");
                write_done_receiver
                    .recv_timeout(Duration::from_secs(2))
                    .expect("writer must commit while read transaction remains open");
                let source = transaction
                    .query_row("SELECT value FROM source", [], |row| row.get::<_, i64>(0))
                    .map_err(|error| crate::map_sqlite_error("read source fixture", error))?;
                Ok((projection, source))
            })
            .expect("complete deferred control read");
        writer_thread.join().expect("join writer fixture thread");
        assert_eq!(observed, (1, 1));
        assert_eq!(
            reader
                .query_row("SELECT value FROM projection", [], |row| row
                    .get::<_, i64>(0))
                .expect("read post-transaction projection"),
            2
        );
        assert_eq!(
            reader
                .query_row("SELECT value FROM source", [], |row| row.get::<_, i64>(0))
                .expect("read post-transaction source"),
            2
        );
    }
}
