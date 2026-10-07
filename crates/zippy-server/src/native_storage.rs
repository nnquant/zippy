//! Server-owned lifecycle for the SQLite Catalog and native manifest storage.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use zippy_catalog::{
    decode_persisted_table_schema, SqliteCatalog, SqliteCatalogConfig, SqliteCatalogReadHandle,
    SqliteCatalogRuntimeSnapshotPinAdmissionControl, SqliteCatalogRuntimeSnapshotPinRecord,
    SqliteCatalogRuntimeSnapshotPinStatus, SqliteCatalogWriteHandle,
};
use zippy_core::OwnedDataDirectory;
use zippy_ingest::{
    NativeIngestConfig, NativeIngestHandle, NativeIngestRuntime, NativeIngestShutdownOutcome,
    NativeIngestStatus, NativeIngestTableBootstrap, NativeTableMaterializerConfig,
    NativeTableMaterializerSink, TableActorAdmissionControl, TableActorHandle, TableActorRegistrar,
    TableActorRegistry, TableActorRegistryConfig, TableActorRegistryHandle, TableRuntimeBootstrap,
    TableRuntimeRegistry,
};
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, ParquetCandidateWriter,
    PersistController, PersistControllerConfig, StartupCatalogBootstrap,
    StartupCatalogFileVerificationReport, StartupCatalogFileVerifier,
    StartupCatalogFileVerifierConfig, StartupOrphanReconciler, StartupOrphanReconcilerConfig,
    StartupOrphanReconciliationReport,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogPersistence, CatalogRead, CatalogWrite,
};
use zippy_types::{ErrorCode, Result, ServerError, TableId};

const SUPERVISOR_THREAD_NAME: &str = "zippy-native-storage";
const REAPER_THREAD_NAME: &str = "zippy-native-storage-reaper";
const NATIVE_INGEST_START_THREAD_NAME: &str = "zippy-native-ingest-start";
const DEFAULT_SERVER_ACTOR_PRINCIPAL: &str = "zippy-server";
const STARTUP_CANCELLED_MESSAGE: &str = "native storage startup was cancelled before readiness";
const CATALOG_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Configuration owned by one native storage supervisor.
#[derive(Debug, Clone)]
pub struct NativeStorageRuntimeConfig {
    data_directory: Arc<OwnedDataDirectory>,
    catalog: SqliteCatalogConfig,
    coordinator: CatalogCommitCoordinatorConfig,
    persist_controller: PersistControllerConfig,
    reconciliation: StartupOrphanReconcilerConfig,
    catalog_file_verification: StartupCatalogFileVerifierConfig,
    table_actors: TableActorRegistryConfig,
    query_resources: crate::query_service::QueryServiceResourceConfig,
    query_registry: crate::query_service::QueryServiceRegistryConfig,
    query_engine: crate::native_query::NativeQueryEngineConfig,
    query_canary: Option<crate::native_query_canary::NativeQueryCanaryRuntimeConfig>,
    actor: CatalogActor,
}

impl NativeStorageRuntimeConfig {
    /// Creates a conservative runtime configuration under the process-owned data root.
    #[must_use]
    pub fn new(data_directory: &Arc<OwnedDataDirectory>) -> Self {
        let query_resources = crate::query_service::QueryServiceResourceConfig::default();
        Self {
            data_directory: Arc::clone(data_directory),
            catalog: SqliteCatalogConfig::new(data_directory),
            coordinator: CatalogCommitCoordinatorConfig::new(),
            persist_controller: PersistControllerConfig::new(),
            reconciliation: StartupOrphanReconcilerConfig::new(),
            catalog_file_verification: StartupCatalogFileVerifierConfig::new(),
            table_actors: TableActorRegistryConfig::new(),
            query_resources,
            query_registry: crate::query_service::QueryServiceRegistryConfig::default(),
            query_engine: crate::native_query::NativeQueryEngineConfig::try_new(
                usize::try_from(query_resources.max_cpu_slots)
                    .expect("default query CPU slots fit usize"),
                query_resources.max_active_tasks,
                query_resources.max_cpu_slots,
                query_resources.max_memory_bytes,
                query_resources.max_memory_bytes.saturating_div(4).max(1),
            )
            .expect("default native query engine config is valid"),
            query_canary: None,
            actor: CatalogActor {
                kind: CatalogActorKind::Server,
                principal: DEFAULT_SERVER_ACTOR_PRINCIPAL.to_owned(),
            },
        }
    }

    /// Replaces the SQLite configuration while preserving the same ownership capability.
    pub fn with_catalog_config(mut self, catalog: SqliteCatalogConfig) -> Result<Self> {
        if !Arc::ptr_eq(&self.data_directory, catalog.ownership()) {
            return Err(terminal_invalid(
                "native storage Catalog config must retain the same data-root ownership",
            ));
        }
        self.catalog = catalog;
        Ok(self)
    }

    /// Replaces the bounded durable commit recovery policy.
    #[must_use]
    pub fn with_coordinator_config(mut self, coordinator: CatalogCommitCoordinatorConfig) -> Self {
        self.coordinator = coordinator;
        self
    }

    /// Replaces the bounded Parquet persist execution policy.
    #[must_use]
    pub fn with_persist_controller_config(
        mut self,
        persist_controller: PersistControllerConfig,
    ) -> Self {
        self.persist_controller = persist_controller;
        self
    }

    /// Replaces the bounded startup reconciliation policy.
    #[must_use]
    pub fn with_reconciliation_config(
        mut self,
        reconciliation: StartupOrphanReconcilerConfig,
    ) -> Self {
        self.reconciliation = reconciliation;
        self
    }

    /// Replaces the bounded Catalog-to-physical startup verification policy.
    #[must_use]
    pub fn with_catalog_file_verification_config(
        mut self,
        verification: StartupCatalogFileVerifierConfig,
    ) -> Self {
        self.catalog_file_verification = verification;
        self
    }

    /// Replaces the bounded per-table actor admission policy.
    #[must_use]
    pub fn with_table_actor_config(mut self, table_actors: TableActorRegistryConfig) -> Self {
        self.table_actors = table_actors;
        self
    }

    pub(crate) fn with_query_resource_config(
        mut self,
        query_resources: crate::query_service::QueryServiceResourceConfig,
    ) -> Self {
        self.query_resources = query_resources;
        self
    }

    pub(crate) fn with_query_registry_config(
        mut self,
        query_registry: crate::query_service::QueryServiceRegistryConfig,
    ) -> Self {
        self.query_registry = query_registry;
        self
    }

    pub(crate) fn with_query_engine_config(
        mut self,
        query_engine: crate::native_query::NativeQueryEngineConfig,
    ) -> Self {
        self.query_engine = query_engine;
        self
    }

    pub(crate) fn with_query_canary_config(
        mut self,
        query_canary: Option<crate::native_query_canary::NativeQueryCanaryRuntimeConfig>,
    ) -> Self {
        self.query_canary = query_canary;
        self
    }

    /// Replaces the audited server principal used for recovered Catalog commits.
    #[must_use]
    pub fn with_actor(mut self, actor: CatalogActor) -> Self {
        self.actor = actor;
        self
    }

    fn validate(&self) -> Result<()> {
        if !Arc::ptr_eq(&self.data_directory, self.catalog.ownership()) {
            return Err(terminal_invalid(
                "native storage Catalog ownership no longer matches the runtime data root",
            ));
        }
        if self.actor.kind != CatalogActorKind::Server {
            return Err(terminal_invalid(
                "native storage commit recovery requires a server Catalog actor",
            ));
        }
        self.persist_controller.validate()?;
        self.table_actors.validate()?;
        self.query_resources.validate()?;
        self.query_registry.validate(self.query_resources)?;
        Ok(())
    }
}

/// Result of waiting for one native storage startup attempt.
pub enum NativeStorageStartOutcome {
    /// Catalog recovery and bidirectional current-snapshot verification completed in time.
    Ready(NativeStorageRuntime),
    /// The caller deadline elapsed; the returned handle still owns the running attempt.
    TimedOut(NativeStorageStartup),
    /// Startup failed after all initialized storage components were cleaned up.
    Failed(ServerError),
}

/// An in-progress native storage startup that retains cancellation and completion state.
pub struct NativeStorageStartup {
    shutdown: Option<watch::Sender<bool>>,
    startup: mpsc::Receiver<Result<SupervisorReady>>,
    completion: Option<mpsc::Receiver<Result<()>>>,
    supervisor: Option<JoinHandle<()>>,
    terminal_failure: Arc<Mutex<Option<ServerError>>>,
}

impl NativeStorageStartup {
    /// Waits reliably until startup either becomes ready or fails.
    pub fn wait(self) -> Result<NativeStorageRuntime> {
        let event = self.startup.recv().map_err(|_| supervisor_stopped());
        self.finish_startup(event)
    }

    /// Waits until the absolute monotonic deadline without abandoning supervisor ownership.
    #[must_use]
    pub fn wait_until(self, deadline: Instant) -> NativeStorageStartOutcome {
        let wait = deadline.saturating_duration_since(Instant::now());
        match self.startup.recv_timeout(wait) {
            Ok(Ok(ready)) => NativeStorageStartOutcome::Ready(self.into_runtime(ready)),
            Ok(Err(error)) => NativeStorageStartOutcome::Failed(self.finish_failed_startup(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => NativeStorageStartOutcome::TimedOut(self),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                NativeStorageStartOutcome::Failed(self.finish_failed_startup(supervisor_stopped()))
            }
        }
    }

    /// Requests cancellation without claiming that synchronous Catalog cleanup has finished.
    pub fn cancel(&self) {
        signal_shutdown(self.shutdown.as_ref());
    }

    /// Cancels startup and waits until the supervisor has released every owned component.
    pub fn cancel_and_wait(mut self) -> Result<()> {
        self.cancel();
        match self.wait_for_completion_and_join() {
            Err(error) if is_expected_startup_cancellation(&error) => Ok(()),
            result => result,
        }
    }

    /// Cancels startup and transfers reliable cleanup to a named background reaper.
    pub fn cancel_in_background(self) -> Result<()> {
        self.cancel();
        let (handoff, receiver) = mpsc::sync_channel::<NativeStorageStartup>(1);
        let reaper = match thread::Builder::new()
            .name(REAPER_THREAD_NAME.to_owned())
            .spawn(move || {
                let Ok(startup) = receiver.recv() else {
                    return;
                };
                if let Err(error) = startup.cancel_and_wait() {
                    tracing::error!(
                        component = "native_storage",
                        event = "native_storage_startup_reap_failed",
                        error = %error,
                        "native storage startup cleanup failed in the background reaper"
                    );
                }
            }) {
            Ok(reaper) => reaper,
            Err(error) => {
                let spawn_error = ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start native storage cleanup reaper error=[{error}]"),
                );
                tracing::error!(
                    component = "native_storage",
                    event = "native_storage_startup_cleanup_detached",
                    error = %spawn_error,
                    "native storage supervisor retains cleanup ownership without a reaper"
                );
                drop(self);
                return Err(spawn_error);
            }
        };
        match handoff.send(self) {
            Ok(()) => {
                drop(reaper);
                Ok(())
            }
            Err(error) => {
                let handoff_error = ServerError::new(
                    ErrorCode::Unavailable,
                    "native storage cleanup reaper stopped before accepting ownership",
                );
                tracing::error!(
                    component = "native_storage",
                    event = "native_storage_startup_cleanup_detached",
                    error = %handoff_error,
                    "native storage supervisor retains cleanup ownership after reaper handoff failure"
                );
                drop(error.0);
                let join_result = reaper.join().map_err(|_| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "native storage cleanup reaper panicked before ownership handoff",
                    )
                });
                combine_results(Err(handoff_error), join_result)
            }
        }
    }

    fn finish_startup(
        mut self,
        event: Result<Result<SupervisorReady>>,
    ) -> Result<NativeStorageRuntime> {
        match event {
            Ok(Ok(ready)) => Ok(self.take_runtime(ready)),
            Ok(Err(error)) | Err(error) => Err(self.finish_failed_startup(error)),
        }
    }

    fn finish_failed_startup(mut self, announced_error: ServerError) -> ServerError {
        let completion_result = self.wait_for_completion();
        let join_result = join_supervisor(&mut self.supervisor);
        let surfaced = match completion_result {
            Err(completion_error) if completion_error != announced_error => {
                combine_errors(announced_error, completion_error)
            }
            Ok(()) | Err(_) => announced_error,
        };
        match join_result {
            Ok(()) => surfaced,
            Err(join_error) => combine_errors(surfaced, join_error),
        }
    }

    fn wait_for_completion_and_join(&mut self) -> Result<()> {
        let completion_result = self.wait_for_completion();
        let join_result = join_supervisor(&mut self.supervisor);
        combine_results(completion_result, join_result)
    }

    fn wait_for_completion(&mut self) -> Result<()> {
        match self.completion.take() {
            Some(completion) => completion
                .recv()
                .unwrap_or_else(|_| Err(supervisor_stopped())),
            None => Err(supervisor_stopped()),
        }
    }

    fn into_runtime(mut self, ready: SupervisorReady) -> NativeStorageRuntime {
        self.take_runtime(ready)
    }

    fn take_runtime(&mut self, ready: SupervisorReady) -> NativeStorageRuntime {
        let query_snapshot_coordinator = crate::query_snapshot::NativeQuerySnapshotCoordinator::new(
            ready.catalog_read_handle.clone(),
            ready.table_actors.clone(),
        );
        let query_canary = ready.query_canary.map(|config| {
            crate::native_query_canary::NativeQueryCanaryService::new(
                config,
                query_snapshot_coordinator.clone(),
                ready.query_resources.clone(),
                ready.query_process_memory.clone(),
            )
        });
        let query_engine = crate::native_query::NativeQueryEngine::new(
            ready.query_engine,
            query_snapshot_coordinator.clone(),
            ready.query_resources.clone(),
            ready.query_process_memory.clone(),
        );
        NativeStorageRuntime {
            report: ready.reconciliation,
            catalog_file_verification: ready.catalog_file_verification,
            catalog_read_handle: ready.catalog_read_handle,
            catalog_write_handle: ready.catalog_write_handle,
            runtime_snapshot_pin_admission: ready.runtime_snapshot_pin_admission,
            table_actors: ready.table_actors,
            table_registrar: ready.table_registrar,
            table_admin_limits: ready.table_admin_limits,
            native_ingest_bootstraps: ready.native_ingest_bootstraps,
            table_actor_admission: ready.table_actor_admission,
            query_snapshot_coordinator,
            query_resources: ready.query_resources,
            query_process_memory: ready.query_process_memory,
            query_engine,
            query_canary,
            native_ingest: None,
            table_admin: None,
            consumer_logs: None,
            healthy: ready.healthy,
            terminal_failure: Arc::clone(&self.terminal_failure),
            shutdown: self.shutdown.take(),
            completion: self.completion.take(),
            supervisor: self.supervisor.take(),
            shutdown_requested: false,
            final_shutdown: None,
            storage_shutdown_signalled: false,
        }
    }
}

impl Drop for NativeStorageStartup {
    fn drop(&mut self) {
        signal_shutdown(self.shutdown.as_ref());
    }
}

/// Final or still-running state of a native storage shutdown request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeStorageShutdownOutcome {
    /// Coordinator drain, Catalog close, and supervisor completion all succeeded.
    Complete,
    /// The caller deadline elapsed while the supervisor retained ownership and kept cleaning up.
    TimedOut,
    /// The supervisor completed cleanup but reported an error.
    Failed(ServerError),
}

impl NativeStorageShutdownOutcome {
    /// Returns whether native storage released every owned component cleanly.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Returns the terminal cleanup error, when shutdown finished unsuccessfully.
    #[must_use]
    pub const fn error(&self) -> Option<&ServerError> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Complete | Self::TimedOut => None,
        }
    }
}

/// Dedicated supervisor for SQLite Catalog recovery and native manifest ownership.
pub struct NativeStorageRuntime {
    report: StartupOrphanReconciliationReport,
    catalog_file_verification: StartupCatalogFileVerificationReport,
    catalog_read_handle: SqliteCatalogReadHandle,
    catalog_write_handle: SqliteCatalogWriteHandle,
    runtime_snapshot_pin_admission: SqliteCatalogRuntimeSnapshotPinAdmissionControl,
    table_actors: TableActorRegistryHandle,
    table_registrar: TableActorRegistrar,
    table_admin_limits: crate::table_admin::NativeTableAdminLimits,
    native_ingest_bootstraps: Arc<[NativeIngestTableBootstrap]>,
    table_actor_admission: TableActorAdmissionControl,
    query_snapshot_coordinator: crate::query_snapshot::NativeQuerySnapshotCoordinator,
    query_resources: crate::query_service::QueryResourceCoordinator,
    query_process_memory: zippy_query::QueryProcessMemoryPool,
    query_engine: crate::native_query::NativeQueryEngine,
    query_canary: Option<crate::native_query_canary::NativeQueryCanaryService>,
    native_ingest: Option<NativeIngestRuntime>,
    table_admin: Option<crate::NativeTableAdmin>,
    consumer_logs: Option<crate::NativeConsumerLogService>,
    healthy: Arc<AtomicBool>,
    terminal_failure: Arc<Mutex<Option<ServerError>>>,
    shutdown: Option<watch::Sender<bool>>,
    completion: Option<mpsc::Receiver<Result<()>>>,
    supervisor: Option<JoinHandle<()>>,
    shutdown_requested: bool,
    final_shutdown: Option<NativeStorageShutdownOutcome>,
    storage_shutdown_signalled: bool,
}

impl NativeStorageRuntime {
    /// Starts native storage reliably on a dedicated OS thread.
    pub fn start(config: NativeStorageRuntimeConfig) -> Result<Self> {
        spawn_supervisor(config)?.wait()
    }

    /// Starts native storage and waits only until the supplied absolute deadline.
    #[must_use]
    pub fn start_until(
        config: NativeStorageRuntimeConfig,
        deadline: Instant,
    ) -> NativeStorageStartOutcome {
        match spawn_supervisor(config) {
            Ok(startup) => startup.wait_until(deadline),
            Err(error) => NativeStorageStartOutcome::Failed(error),
        }
    }

    /// Starts native storage and waits for at most the supplied positive duration.
    #[must_use]
    pub fn start_with_timeout(
        config: NativeStorageRuntimeConfig,
        timeout: Duration,
    ) -> NativeStorageStartOutcome {
        if timeout.is_zero() {
            return NativeStorageStartOutcome::Failed(terminal_invalid(
                "native storage startup timeout must be positive",
            ));
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return NativeStorageStartOutcome::Failed(terminal_invalid(
                "native storage startup timeout exceeds monotonic clock range",
            ));
        };
        Self::start_until(config, deadline)
    }

    /// Returns the final revision-stable reconciliation report used to open readiness.
    #[must_use]
    pub const fn reconciliation_report(&self) -> &StartupOrphanReconciliationReport {
        &self.report
    }

    /// Returns the revision-stable Catalog-to-physical verification evidence.
    #[must_use]
    pub const fn catalog_file_verification_report(&self) -> &StartupCatalogFileVerificationReport {
        &self.catalog_file_verification
    }

    /// Returns a lifecycle-safe clone of the running SQLite Catalog read endpoint.
    #[must_use]
    pub fn catalog_read_handle(&self) -> SqliteCatalogReadHandle {
        self.catalog_read_handle.clone()
    }

    /// Returns a lifecycle-bound Catalog command endpoint for native ingest fencing.
    #[must_use]
    pub fn catalog_write_handle(&self) -> SqliteCatalogWriteHandle {
        self.catalog_write_handle.clone()
    }

    /// Returns process-local query snapshot pin gauges, limits, and rejection counters.
    pub fn runtime_snapshot_pin_status(&self) -> Result<SqliteCatalogRuntimeSnapshotPinStatus> {
        self.catalog_read_handle.runtime_snapshot_pin_status()
    }

    /// Returns active query-attributed snapshot pins for diagnostics.
    pub fn runtime_query_snapshot_pins(
        &self,
    ) -> Result<Vec<SqliteCatalogRuntimeSnapshotPinRecord>> {
        self.catalog_read_handle.runtime_query_snapshot_pins()
    }

    #[allow(dead_code)]
    pub(crate) fn query_snapshot_coordinator(
        &self,
    ) -> crate::query_snapshot::NativeQuerySnapshotCoordinator {
        self.query_snapshot_coordinator.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn query_resource_coordinator(
        &self,
    ) -> crate::query_service::QueryResourceCoordinator {
        self.query_resources.clone()
    }

    /// Returns process-wide native operator-memory allocation counters.
    #[must_use]
    pub fn query_process_memory_status(&self) -> zippy_query::QueryProcessMemoryStatus {
        self.query_process_memory.status()
    }

    /// Returns the server-owned native SQL query engine.
    #[must_use]
    pub fn query_engine(&self) -> crate::native_query::NativeQueryEngine {
        self.query_engine.clone()
    }

    /// Returns the internal persisted-only canary when it was explicitly configured.
    #[must_use]
    pub fn query_canary(&self) -> Option<crate::native_query_canary::NativeQueryCanaryService> {
        self.query_canary.clone()
    }

    pub(crate) fn close_query_canary_admission(&self) {
        if let Some(canary) = &self.query_canary {
            canary.close_admission();
        }
    }

    /// Returns the number of active native table runtimes recovered before readiness.
    #[must_use]
    pub fn table_runtime_count(&self) -> usize {
        self.table_actors.len()
    }

    /// Returns the Catalog revision shared by every recovered native table runtime.
    #[must_use]
    pub const fn table_runtime_catalog_revision(&self) -> u64 {
        self.table_actors.catalog_revision()
    }

    /// Resolves the server-owned serial actor for one active native table.
    pub fn table_actor(&self, table_id: TableId) -> Result<TableActorHandle> {
        self.table_actors.table(table_id)
    }

    /// Returns the cloneable actor registry used by native ingest publication.
    #[must_use]
    pub fn table_actor_registry_handle(&self) -> TableActorRegistryHandle {
        self.table_actors.clone()
    }

    /// Returns the revision-consistent startup inputs required by WAL recovery.
    #[must_use]
    pub fn native_ingest_bootstraps(&self) -> Arc<[NativeIngestTableBootstrap]> {
        Arc::clone(&self.native_ingest_bootstraps)
    }

    /// Starts WAL recovery and native append publication after table actors are ready.
    pub fn start_native_ingest(
        &mut self,
        wal_root: impl Into<PathBuf>,
    ) -> Result<NativeIngestHandle> {
        self.start_native_ingest_with_config(
            wal_root,
            NativeIngestConfig::new(),
            NativeTableMaterializerConfig::new(),
        )
    }

    /// Starts native ingestion with explicit receipt, WAL, and materialization budgets.
    pub fn start_native_ingest_with_config(
        &mut self,
        wal_root: impl Into<PathBuf>,
        ingest_config: NativeIngestConfig,
        materializer_config: NativeTableMaterializerConfig,
    ) -> Result<NativeIngestHandle> {
        if let Some(runtime) = &self.native_ingest {
            return Ok(runtime.handle());
        }
        if self.shutdown_requested || self.final_shutdown.is_some() || !self.is_healthy() {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "native storage is not available for ingest startup",
            )
            .with_retryable(false));
        }
        let wal_root = wal_root.into();
        let bootstraps = self.native_ingest_bootstraps.to_vec();
        let sink = Arc::new(NativeTableMaterializerSink::start(
            materializer_config,
            &bootstraps,
            &self.table_actors,
        )?);
        let catalog: Arc<dyn CatalogWrite> = Arc::new(self.catalog_write_handle.clone());
        let ingest_sink = Arc::clone(&sink);
        let runtime = thread::Builder::new()
            .name(NATIVE_INGEST_START_THREAD_NAME.to_owned())
            .spawn(move || {
                NativeIngestRuntime::start(
                    ingest_config,
                    wal_root,
                    catalog,
                    bootstraps,
                    ingest_sink,
                )
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("failed to start native ingest recovery thread error=[{error}]"),
                )
                .with_retryable(false)
            })?
            .join()
            .map_err(|_| {
                ServerError::new(
                    ErrorCode::Internal,
                    "native ingest recovery thread panicked before startup completed",
                )
                .with_retryable(false)
            })??;
        let handle = runtime.handle();
        self.table_admin = Some(crate::NativeTableAdmin::new(
            self.catalog_read_handle.clone(),
            self.catalog_write_handle.clone(),
            self.table_actors.clone(),
            self.table_registrar.clone(),
            sink,
            handle.clone(),
            crate::table_admin::NativeTableAdminLimits {
                max_tables: self.table_admin_limits.max_tables,
                max_metadata_bytes: self.table_admin_limits.max_metadata_bytes,
                initial_metadata_bytes: self.table_admin_limits.initial_metadata_bytes,
                initial_table_bytes: std::mem::take(
                    &mut self.table_admin_limits.initial_table_bytes,
                ),
            },
        ));
        self.consumer_logs = Some(crate::NativeConsumerLogService::new(
            self.query_engine.clone(),
            self.catalog_read_handle.clone(),
            self.catalog_write_handle.clone(),
            handle.clone(),
        ));
        self.native_ingest = Some(runtime);
        Ok(handle)
    }

    /// Returns the online table-management capability after native ingest startup.
    #[must_use]
    pub fn table_admin(&self) -> Option<crate::NativeTableAdmin> {
        self.table_admin.clone()
    }

    /// Returns the shared native durable consumer service after ingest startup.
    #[must_use]
    pub fn consumer_logs(&self) -> Option<crate::NativeConsumerLogService> {
        self.consumer_logs.clone()
    }

    /// Returns the producer-fenced ingest endpoint when configured.
    #[must_use]
    pub fn native_ingest_handle(&self) -> Option<NativeIngestHandle> {
        self.native_ingest.as_ref().map(NativeIngestRuntime::handle)
    }

    /// Returns native ingest lifecycle and hot-path counters when configured.
    #[must_use]
    pub fn native_ingest_status(&self) -> Option<NativeIngestStatus> {
        self.native_ingest.as_ref().map(NativeIngestRuntime::status)
    }

    /// Returns whether the supervisor still owns a ready native storage stack.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.terminal_failure().is_none()
            && self.healthy.load(Ordering::Acquire)
            && !self.shutdown_requested
            && self.final_shutdown.is_none()
            && self.query_resources.status().sweeper_running
            && self
                .supervisor
                .as_ref()
                .is_some_and(|supervisor| !supervisor.is_finished())
            && self
                .native_ingest
                .as_ref()
                .is_none_or(|ingest| ingest.status().accepting)
    }

    /// Returns the first terminal supervisor error observed after readiness.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<ServerError> {
        terminal_failure(&self.terminal_failure)
    }

    /// Irreversibly closes native work admission and requests ordered background cleanup.
    pub fn begin_shutdown(&mut self) {
        self.healthy.store(false, Ordering::Release);
        self.close_query_canary_admission();
        if let Some(ingest) = &self.native_ingest {
            ingest.close_admission();
        }
        if !self.shutdown_requested {
            self.shutdown_requested = true;
            self.query_snapshot_coordinator.close_admission();
            self.runtime_snapshot_pin_admission.close_admission();
        }
        self.query_resources.begin_shutdown();
        self.try_signal_storage_shutdown();
    }

    /// Waits for ordered cleanup until an absolute monotonic deadline.
    pub fn shutdown_until(&mut self, deadline: Instant) -> NativeStorageShutdownOutcome {
        if let Some(outcome) = &self.final_shutdown {
            return outcome.clone();
        }
        self.begin_shutdown();
        if let Some(ingest) = self.native_ingest.as_mut() {
            match ingest.shutdown_until(deadline) {
                NativeIngestShutdownOutcome::Complete => {
                    self.native_ingest.take();
                }
                NativeIngestShutdownOutcome::TimedOut => {
                    return NativeStorageShutdownOutcome::TimedOut;
                }
                NativeIngestShutdownOutcome::Failed(error) => {
                    return NativeStorageShutdownOutcome::Failed(error);
                }
            }
        }
        while !self.try_signal_storage_shutdown() {
            let now = Instant::now();
            if now >= deadline {
                return NativeStorageShutdownOutcome::TimedOut;
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(2)),
            );
        }
        let Some(completion) = self.completion.as_ref() else {
            let outcome = NativeStorageShutdownOutcome::Failed(supervisor_stopped());
            self.final_shutdown = Some(outcome.clone());
            return outcome;
        };
        let wait = deadline.saturating_duration_since(Instant::now());
        let mut outcome = match completion.recv_timeout(wait) {
            Ok(Ok(())) => NativeStorageShutdownOutcome::Complete,
            Ok(Err(error)) => NativeStorageShutdownOutcome::Failed(error),
            Err(mpsc::RecvTimeoutError::Timeout) => NativeStorageShutdownOutcome::TimedOut,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                NativeStorageShutdownOutcome::Failed(supervisor_stopped())
            }
        };
        if !matches!(outcome, NativeStorageShutdownOutcome::TimedOut) {
            self.completion.take();
            if let Err(join_error) = join_supervisor(&mut self.supervisor) {
                outcome = match outcome {
                    NativeStorageShutdownOutcome::Complete => {
                        NativeStorageShutdownOutcome::Failed(join_error)
                    }
                    NativeStorageShutdownOutcome::Failed(error) => {
                        NativeStorageShutdownOutcome::Failed(combine_errors(error, join_error))
                    }
                    NativeStorageShutdownOutcome::TimedOut => unreachable!(
                        "timed-out native storage shutdown does not join its supervisor"
                    ),
                };
            }
            self.final_shutdown = Some(outcome.clone());
        }
        outcome
    }

    fn try_signal_storage_shutdown(&mut self) -> bool {
        if self.storage_shutdown_signalled {
            return true;
        }
        // Accepted ingest/checkpoint work still needs actors and persistence. The
        // ingest runtime is removed only after its complete, checked drain.
        if self.native_ingest.is_some() {
            return false;
        }
        self.table_actor_admission.close();
        let queries_drained = self.query_resources.status().lifecycle
            == crate::query_service::QueryServiceLifecycle::Stopped;
        let snapshot_planning_drained = self.query_snapshot_coordinator.planning_drained();
        let pins_drained = match self.catalog_read_handle.runtime_snapshot_pin_status() {
            Ok(status) => status.active_pins == 0,
            Err(_) => self
                .supervisor
                .as_ref()
                .is_some_and(JoinHandle::is_finished),
        };
        if !queries_drained || !snapshot_planning_drained || !pins_drained {
            return false;
        }
        signal_shutdown(self.shutdown.as_ref());
        self.storage_shutdown_signalled = true;
        true
    }

    /// Waits for ordered cleanup for at most the supplied positive duration.
    pub fn shutdown(&mut self, timeout: Duration) -> NativeStorageShutdownOutcome {
        if timeout.is_zero() {
            return NativeStorageShutdownOutcome::Failed(terminal_invalid(
                "native storage shutdown timeout must be positive",
            ));
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return NativeStorageShutdownOutcome::Failed(terminal_invalid(
                "native storage shutdown timeout exceeds monotonic clock range",
            ));
        };
        self.shutdown_until(deadline)
    }
}

impl Drop for NativeStorageRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
        if self.native_ingest.is_some() {
            // Drop is best-effort cleanup, not a successful graceful-shutdown
            // receipt. Do not strand the supervisor when the owner omits drain
            // or abandons a timed-out/failed shutdown.
            self.table_actor_admission.close();
            signal_shutdown(self.shutdown.as_ref());
        }
        if self
            .supervisor
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
        {
            tracing::warn!(
                component = "native_storage",
                event = "native_storage_cleanup_detached",
                "native storage cleanup continues after runtime handle drop"
            );
        }
    }
}

struct SupervisorReady {
    reconciliation: StartupOrphanReconciliationReport,
    catalog_file_verification: StartupCatalogFileVerificationReport,
    catalog_read_handle: SqliteCatalogReadHandle,
    catalog_write_handle: SqliteCatalogWriteHandle,
    runtime_snapshot_pin_admission: SqliteCatalogRuntimeSnapshotPinAdmissionControl,
    table_actors: TableActorRegistryHandle,
    table_registrar: TableActorRegistrar,
    table_admin_limits: crate::table_admin::NativeTableAdminLimits,
    native_ingest_bootstraps: Arc<[NativeIngestTableBootstrap]>,
    table_actor_admission: TableActorAdmissionControl,
    healthy: Arc<AtomicBool>,
    query_resources: crate::query_service::QueryResourceCoordinator,
    query_process_memory: zippy_query::QueryProcessMemoryPool,
    query_engine: crate::native_query::NativeQueryEngineConfig,
    query_canary: Option<crate::native_query_canary::NativeQueryCanaryRuntimeConfig>,
}

fn spawn_supervisor(config: NativeStorageRuntimeConfig) -> Result<NativeStorageStartup> {
    config.validate()?;
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
    let (completion_sender, completion_receiver) = mpsc::sync_channel(1);
    let terminal_failure = Arc::new(Mutex::new(None));
    let supervisor_terminal_failure = Arc::clone(&terminal_failure);
    let supervisor = thread::Builder::new()
        .name(SUPERVISOR_THREAD_NAME.to_owned())
        .spawn(move || {
            run_supervisor(
                config,
                shutdown_receiver,
                startup_sender,
                completion_sender,
                supervisor_terminal_failure,
            );
        })
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("start native storage supervisor error=[{error}]"),
            )
        })?;
    Ok(NativeStorageStartup {
        shutdown: Some(shutdown_sender),
        startup: startup_receiver,
        completion: Some(completion_receiver),
        supervisor: Some(supervisor),
        terminal_failure,
    })
}

fn run_supervisor(
    config: NativeStorageRuntimeConfig,
    shutdown: watch::Receiver<bool>,
    startup: mpsc::SyncSender<Result<SupervisorReady>>,
    completion: mpsc::SyncSender<Result<()>>,
    terminal_failure: Arc<Mutex<Option<ServerError>>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = ServerError::new(
                ErrorCode::Unavailable,
                format!("build native storage Tokio runtime error=[{error}]"),
            );
            latch_terminal_failure(&terminal_failure, &error);
            let _ = startup.send(Err(error.clone()));
            let _ = completion.send(Err(error));
            return;
        }
    };
    let (ready_sent, result) = runtime.block_on(supervise_storage(
        config,
        shutdown,
        &startup,
        &terminal_failure,
    ));
    drop(runtime);
    if let Err(error) = &result {
        latch_terminal_failure(&terminal_failure, error);
    }
    if !ready_sent {
        let startup_error = result
            .as_ref()
            .err()
            .cloned()
            .unwrap_or_else(supervisor_stopped);
        let _ = startup.send(Err(startup_error));
    }
    let _ = completion.send(result);
}

async fn supervise_storage(
    config: NativeStorageRuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
    startup: &mpsc::SyncSender<Result<SupervisorReady>>,
    terminal_failure: &Arc<Mutex<Option<ServerError>>>,
) -> (bool, Result<()>) {
    let (catalog, snapshot_memory_lease_issuer) =
        match SqliteCatalog::open_with_snapshot_memory_lease_issuer(config.catalog.clone()) {
            Ok((catalog, issuer)) => (Arc::new(catalog), issuer),
            Err(error) => {
                return (false, Err(error));
            }
        };

    if shutdown_requested(&shutdown) {
        return (
            false,
            fail_storage(startup_cancelled(), None, None, catalog).await,
        );
    }

    let pre_replay = match reconcile_once(
        &config,
        Arc::clone(&catalog),
        &mut shutdown,
        "pre-replay",
        false,
    )
    .await
    {
        Ok(report) => report,
        Err(error) => return (false, fail_storage(error, None, None, catalog).await),
    };
    tracing::info!(
        component = "native_storage",
        event = "native_storage_pre_replay_reconciled",
        catalog_revision = pre_replay.catalog_revision,
        files = pre_replay.files.len(),
        orphans = pre_replay.orphan_count(),
        temporary_files = pre_replay.temporary_files.len(),
        issues = pre_replay.issues.len(),
        "native storage pre-replay reconciliation completed"
    );

    let catalog_persistence: Arc<dyn CatalogPersistence> = catalog.clone();
    let coordinator = match wait_for_startup_stage(
        &mut shutdown,
        CatalogCommitCoordinator::start(
            config.coordinator.clone(),
            Arc::clone(&config.data_directory),
            catalog_persistence,
            config.actor.clone(),
        ),
    )
    .await
    {
        Ok(coordinator) => coordinator,
        Err(error) => return (false, fail_storage(error, None, None, catalog).await),
    };
    let persist_controller = match PersistController::start(
        config.persist_controller.clone(),
        ParquetCandidateWriter::new(Arc::clone(&config.data_directory)),
        coordinator,
    )
    .await
    {
        Ok(controller) => controller,
        Err(error) => return (false, fail_storage(error, None, None, catalog).await),
    };

    let post_replay = match reconcile_once(
        &config,
        Arc::clone(&catalog),
        &mut shutdown,
        "post-replay",
        true,
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            return (
                false,
                fail_storage(error, None, Some(persist_controller), catalog).await,
            );
        }
    };
    tracing::info!(
        component = "native_storage",
        event = "native_storage_post_replay_reconciled",
        catalog_revision = post_replay.catalog_revision,
        files = post_replay.files.len(),
        orphans = post_replay.orphan_count(),
        temporary_files = post_replay.temporary_files.len(),
        issues = post_replay.issues.len(),
        "native storage post-replay reconciliation completed"
    );

    let catalog_bootstrap =
        match verify_catalog_files_once(&config, Arc::clone(&catalog), &mut shutdown).await {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                return (
                    false,
                    fail_storage(error, None, Some(persist_controller), catalog).await,
                );
            }
        };
    let catalog_file_verification = catalog_bootstrap.verification.clone();
    if catalog_file_verification.catalog_revision != post_replay.catalog_revision {
        let error = ServerError::new(
            ErrorCode::Conflict,
            format!(
                concat!(
                    "catalog revision changed between startup reconciliation and physical ",
                    "verification reconciliation_revision=[{}] verification_revision=[{}]"
                ),
                post_replay.catalog_revision, catalog_file_verification.catalog_revision
            ),
        )
        .with_retryable(true);
        return (
            false,
            fail_storage(error, None, Some(persist_controller), catalog).await,
        );
    }

    let (table_runtimes, runtime_metadata_bytes, table_metadata_bytes) =
        match build_table_runtime_registry(
            catalog_bootstrap,
            config
                .catalog_file_verification
                .max_runtime_metadata_bytes(),
        ) {
            Ok(registry) => registry,
            Err(error) => {
                return (
                    false,
                    fail_storage(error, None, Some(persist_controller), catalog).await,
                );
            }
        };
    let native_ingest_bootstraps = match table_runtimes
        .iter()
        .map(|runtime| {
            NativeIngestTableBootstrap::from_catalog_snapshot(
                runtime.table(),
                Arc::clone(runtime.schema()),
                runtime.snapshot(),
            )
        })
        .collect::<Result<Vec<_>>>()
    {
        Ok(bootstraps) => Arc::<[NativeIngestTableBootstrap]>::from(bootstraps),
        Err(error) => {
            return (
                false,
                fail_storage(error, None, Some(persist_controller), catalog).await,
            );
        }
    };
    let mut table_actors = match TableActorRegistry::start(
        config.table_actors.clone(),
        table_runtimes,
        persist_controller.handle(),
    ) {
        Ok(registry) => registry,
        Err(error) => {
            return (
                false,
                fail_storage(error, None, Some(persist_controller), catalog).await,
            );
        }
    };
    if shutdown_requested(&shutdown) {
        return (
            false,
            fail_storage(
                startup_cancelled(),
                Some(table_actors),
                Some(persist_controller),
                catalog,
            )
            .await,
        );
    }
    let table_actor_handle = table_actors.handle();
    let table_actor_admission = table_actors.admission_control();
    let final_revision = match wait_for_startup_stage(&mut shutdown, catalog.revision()).await {
        Ok(revision) => revision,
        Err(error) => {
            return (
                false,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            );
        }
    };
    if final_revision != table_actor_handle.catalog_revision() {
        let error = ServerError::new(
            ErrorCode::Conflict,
            format!(
                concat!(
                    "catalog revision changed while constructing table actors ",
                    "expected=[{}] actual=[{}]"
                ),
                table_actor_handle.catalog_revision(),
                final_revision
            ),
        )
        .with_retryable(true);
        return (
            false,
            fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
        );
    }
    let query_process_memory = match zippy_query::QueryProcessMemoryPool::try_new(
        NonZeroU64::new(config.query_resources.max_memory_bytes)
            .expect("validated query process memory is nonzero"),
    ) {
        Ok(memory) => memory,
        Err(error) => {
            return (
                false,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            );
        }
    };
    let query_resources = match crate::query_service::QueryResourceCoordinator::with_registry_config_and_snapshot_memory_lease_issuer(
        config.query_resources,
        config.query_registry,
        snapshot_memory_lease_issuer,
    ) {
        Ok(query_resources) => query_resources,
        Err(error) => {
            return (
                false,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            );
        }
    };
    tracing::info!(
        component = "native_storage",
        event = "native_storage_ready",
        catalog_revision = table_actor_handle.catalog_revision(),
        tables = table_actor_handle.len(),
        files = catalog_file_verification.file_count,
        managed_files = catalog_file_verification.managed_file_count,
        verified_bytes = catalog_file_verification.verified_bytes,
        "native storage recovery and bidirectional verification completed"
    );

    let healthy = Arc::new(AtomicBool::new(true));
    let mut persist_failure = persist_controller.failure_monitor();
    let mut table_actor_failure = table_actors.failure_monitor();
    let catalog_read_handle = match catalog.read_handle() {
        Ok(handle) => handle,
        Err(error) => {
            return (
                false,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            );
        }
    };
    let catalog_write_handle = match catalog.write_handle() {
        Ok(handle) => handle,
        Err(error) => {
            return (
                false,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            );
        }
    };
    let runtime_snapshot_pin_admission = catalog.runtime_snapshot_pin_admission_control();

    if startup
        .send(Ok(SupervisorReady {
            reconciliation: post_replay,
            catalog_file_verification,
            catalog_read_handle,
            catalog_write_handle,
            runtime_snapshot_pin_admission,
            table_actors: table_actor_handle,
            table_registrar: table_actors.registrar(),
            table_admin_limits: crate::table_admin::NativeTableAdminLimits {
                max_tables: config.catalog_file_verification.max_tables(),
                max_metadata_bytes: config
                    .catalog_file_verification
                    .max_runtime_metadata_bytes(),
                initial_metadata_bytes: runtime_metadata_bytes,
                initial_table_bytes: table_metadata_bytes,
            },
            native_ingest_bootstraps,
            table_actor_admission,
            healthy: Arc::clone(&healthy),
            query_resources,
            query_process_memory,
            query_engine: config.query_engine,
            query_canary: config.query_canary,
        }))
        .is_err()
    {
        healthy.store(false, Ordering::Release);
        return (
            false,
            close_storage(Some(table_actors), Some(persist_controller), catalog).await,
        );
    }

    let failure = tokio::select! {
        biased;
        error = persist_failure.wait_for_failure() => Some(error),
        error = table_actor_failure.wait_for_failure() => Some(error),
        error = wait_for_catalog_failure(Arc::clone(&catalog)) => Some(error),
        _ = wait_for_shutdown(&mut shutdown) => None,
    };
    if let Some(error) = &failure {
        latch_terminal_failure(terminal_failure, error);
    }
    healthy.store(false, Ordering::Release);
    match failure {
        Some(error) => {
            table_actors.close_admission();
            (
                true,
                fail_storage(error, Some(table_actors), Some(persist_controller), catalog).await,
            )
        }
        None => (
            true,
            close_storage(Some(table_actors), Some(persist_controller), catalog).await,
        ),
    }
}

fn latch_terminal_failure(terminal_failure: &Arc<Mutex<Option<ServerError>>>, error: &ServerError) {
    let mut terminal = terminal_failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if terminal.is_none() {
        *terminal = Some(error.clone());
    }
}

fn terminal_failure(terminal_failure: &Arc<Mutex<Option<ServerError>>>) -> Option<ServerError> {
    terminal_failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

async fn reconcile_once(
    config: &NativeStorageRuntimeConfig,
    catalog: Arc<SqliteCatalog>,
    shutdown: &mut watch::Receiver<bool>,
    stage: &'static str,
    remove_orphans: bool,
) -> Result<StartupOrphanReconciliationReport> {
    let reconciler = StartupOrphanReconciler::with_config_and_cleanup_control(
        config.reconciliation.clone(),
        Arc::clone(&config.data_directory),
        catalog,
    )?;
    let result = if remove_orphans {
        wait_for_startup_stage(
            shutdown,
            reconciler.reconcile_and_remove_orphans_before_admission(),
        )
        .await
    } else {
        wait_for_startup_stage(shutdown, reconciler.reconcile()).await
    };
    result.map_err(|error| with_stage(stage, error))
}

async fn verify_catalog_files_once(
    config: &NativeStorageRuntimeConfig,
    catalog: Arc<SqliteCatalog>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<StartupCatalogBootstrap> {
    let catalog_read: Arc<dyn CatalogRead> = catalog;
    let verifier = StartupCatalogFileVerifier::with_config(
        config.catalog_file_verification.clone(),
        Arc::clone(&config.data_directory),
        catalog_read,
    )?;
    wait_for_startup_stage(shutdown, verifier.verify_with_bootstrap())
        .await
        .map_err(|error| with_stage("catalog-to-physical", error))
}

fn build_table_runtime_registry(
    bootstrap: StartupCatalogBootstrap,
    max_runtime_metadata_bytes: u64,
) -> Result<(
    TableRuntimeRegistry,
    u64,
    BTreeMap<zippy_types::TableId, u64>,
)> {
    let catalog_revision = bootstrap.verification.catalog_revision;
    let mut tables = Vec::with_capacity(bootstrap.active_tables.len());
    let mut retained_bytes = bootstrap.runtime_metadata_bytes;
    let mut table_bytes = BTreeMap::new();
    for descriptor in bootstrap.active_tables {
        let table_id = descriptor.table.table_id;
        let decoded = decode_persisted_table_schema(&descriptor.table, &descriptor.schema)?;
        let next_retained_bytes = retained_bytes
            .checked_add(decoded.retained_charge_bytes())
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "startup runtime retained-memory byte charge overflow",
                )
                .with_table_id(table_id)
                .with_retryable(false)
            })?;
        if next_retained_bytes > max_runtime_metadata_bytes {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    concat!(
                        "startup runtime metadata and decoded schemas exceed byte budget ",
                        "actual=[{}] maximum=[{}]"
                    ),
                    next_retained_bytes, max_runtime_metadata_bytes
                ),
            )
            .with_table_id(table_id)
            .with_retryable(false));
        }
        table_bytes.insert(
            table_id,
            descriptor
                .retained_metadata_bytes
                .checked_add(decoded.retained_charge_bytes())
                .ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "table metadata charge overflow",
                    )
                })?,
        );
        retained_bytes = next_retained_bytes;
        tables.push(TableRuntimeBootstrap::new(
            descriptor.table,
            descriptor.schema,
            decoded.into_schema(),
            descriptor.snapshot,
        )?);
    }
    Ok((
        TableRuntimeRegistry::from_bootstraps(catalog_revision, tables)?,
        retained_bytes,
        table_bytes,
    ))
}

async fn wait_for_startup_stage<F, T>(shutdown: &mut watch::Receiver<bool>, future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    if shutdown_requested(shutdown) {
        return Err(startup_cancelled());
    }
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let _ = changed;
            Err(startup_cancelled())
        }
        result = future => result,
    }
}

async fn fail_storage(
    error: ServerError,
    table_actors: Option<TableActorRegistry>,
    persist_controller: Option<PersistController>,
    catalog: Arc<SqliteCatalog>,
) -> Result<()> {
    let cleanup = close_storage(table_actors, persist_controller, catalog).await;
    let surfaced = match cleanup {
        Ok(()) => error,
        Err(cleanup_error) => combine_errors(error, cleanup_error),
    };
    Err(surfaced)
}

async fn close_storage(
    table_actors: Option<TableActorRegistry>,
    persist_controller: Option<PersistController>,
    catalog: Arc<SqliteCatalog>,
) -> Result<()> {
    let actor_result = match table_actors {
        Some(table_actors) => table_actors.drain().await,
        None => Ok(()),
    };
    let persist_result = match persist_controller {
        Some(persist_controller) => persist_controller.drain().await,
        None => Ok(()),
    };
    let catalog_result = match Arc::try_unwrap(catalog) {
        Ok(catalog) => catalog.close(),
        Err(_) => Err(ServerError::new(
            ErrorCode::Internal,
            "native storage retained an unexpected SQLite catalog reference during shutdown",
        )),
    };
    let mut error = None;
    for result in [actor_result, persist_result, catalog_result] {
        if let Err(next) = result {
            error = Some(match error {
                Some(previous) => combine_errors(previous, next),
                None => next,
            });
        }
    }
    match error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if shutdown_requested(shutdown) {
        return;
    }
    let _ = shutdown.changed().await;
}

async fn wait_for_catalog_failure(catalog: Arc<SqliteCatalog>) -> ServerError {
    let mut ticker = tokio::time::interval(CATALOG_HEALTH_POLL_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if let Err(error) = catalog.check_runtime_health() {
            let message = format!("native storage Catalog runtime failed error=[{error}]");
            return error.with_message(message).with_retryable(false);
        }
    }
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn signal_shutdown(shutdown: Option<&watch::Sender<bool>>) {
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(true);
    }
}

fn with_stage(stage: &str, error: ServerError) -> ServerError {
    let message = format!("native storage startup stage failed stage=[{stage}] error=[{error}]");
    error.with_message(message)
}

fn combine_errors(primary: ServerError, cleanup: ServerError) -> ServerError {
    let message = format!("{primary}; native storage cleanup failed error=[{cleanup}]");
    primary.with_message(message)
}

fn combine_results(primary: Result<()>, secondary: Result<()>) -> Result<()> {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(secondary)) => Err(combine_errors(primary, secondary)),
    }
}

fn join_supervisor(supervisor: &mut Option<JoinHandle<()>>) -> Result<()> {
    let Some(supervisor) = supervisor.take() else {
        return Ok(());
    };
    supervisor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "native storage supervisor panicked before releasing ownership",
        )
    })
}

fn is_expected_startup_cancellation(error: &ServerError) -> bool {
    error.code() == ErrorCode::DeadlineExceeded
        && error.message().contains(STARTUP_CANCELLED_MESSAGE)
        && !error.message().contains("native storage cleanup failed")
}

fn startup_cancelled() -> ServerError {
    ServerError::new(ErrorCode::DeadlineExceeded, STARTUP_CANCELLED_MESSAGE)
}

fn supervisor_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "native storage supervisor stopped without reporting completion",
    )
}

fn terminal_invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn cancel_and_wait_does_not_return_before_the_supervisor_finishes() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let (_startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let (completion_sender, completion_receiver) = mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let thread_finished = Arc::clone(&finished);
        let supervisor = thread::spawn(move || {
            while !*shutdown_receiver.borrow() {
                thread::sleep(Duration::from_millis(1));
            }
            thread::sleep(Duration::from_millis(20));
            thread_finished.store(true, Ordering::Release);
            completion_sender
                .send(Err(startup_cancelled()))
                .expect("startup handle retains completion receiver");
        });
        let startup = NativeStorageStartup {
            shutdown: Some(shutdown_sender),
            startup: startup_receiver,
            completion: Some(completion_receiver),
            supervisor: Some(supervisor),
            terminal_failure: Arc::new(Mutex::new(None)),
        };

        startup
            .cancel_and_wait()
            .expect("expected startup cancellation is successful cleanup");

        assert!(finished.load(Ordering::Acquire));
    }

    #[test]
    fn identical_startup_and_completion_error_is_not_labeled_as_cleanup_failure() {
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let (completion_sender, completion_receiver) = mpsc::sync_channel(1);
        let failure = ServerError::new(
            ErrorCode::DataCorruption,
            "injected native storage startup failure",
        )
        .with_retryable(false);
        startup_sender
            .send(Err(failure.clone()))
            .expect("startup receiver remains owned");
        completion_sender
            .send(Err(failure.clone()))
            .expect("completion receiver remains owned");
        let startup = NativeStorageStartup {
            shutdown: None,
            startup: startup_receiver,
            completion: Some(completion_receiver),
            supervisor: Some(thread::spawn(|| {})),
            terminal_failure: Arc::new(Mutex::new(None)),
        };

        let surfaced = startup
            .wait()
            .err()
            .expect("injected startup failure must be returned");
        assert_eq!(surfaced, failure);
        assert!(!surfaced.message().contains("cleanup failed"));
    }

    #[test]
    fn cancel_in_background_hands_supervisor_ownership_to_the_reaper() {
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);
        let (_startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let (completion_sender, completion_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let thread_finished = Arc::clone(&finished);
        let supervisor = thread::spawn(move || {
            while !*shutdown_receiver.borrow() {
                thread::sleep(Duration::from_millis(1));
            }
            release_receiver
                .recv()
                .expect("test controls delayed supervisor cleanup");
            thread_finished.store(true, Ordering::Release);
            completion_sender
                .send(Err(startup_cancelled()))
                .expect("background reaper retains completion receiver");
            finished_sender
                .send(())
                .expect("test retains finished receiver");
        });
        let startup = NativeStorageStartup {
            shutdown: Some(shutdown_sender),
            startup: startup_receiver,
            completion: Some(completion_receiver),
            supervisor: Some(supervisor),
            terminal_failure: Arc::new(Mutex::new(None)),
        };

        startup
            .cancel_in_background()
            .expect("startup ownership is handed to a background reaper");
        assert!(!finished.load(Ordering::Acquire));

        release_sender
            .send(())
            .expect("release delayed supervisor cleanup");
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("background reaper allows the supervisor to finish");
        assert!(finished.load(Ordering::Acquire));
    }
}
