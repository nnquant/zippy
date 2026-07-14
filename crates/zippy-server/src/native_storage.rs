//! Server-owned lifecycle for the SQLite Catalog and native manifest storage.

use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use zippy_catalog::{decode_persisted_table_schema, SqliteCatalog, SqliteCatalogConfig};
use zippy_core::OwnedDataDirectory;
use zippy_ingest::{
    TableActorAdmissionControl, TableActorHandle, TableActorRegistry, TableActorRegistryConfig,
    TableActorRegistryHandle, TableRuntimeBootstrap, TableRuntimeRegistry,
};
use zippy_storage::{
    CatalogCommitCoordinator, CatalogCommitCoordinatorConfig, StartupCatalogBootstrap,
    StartupCatalogFileVerificationReport, StartupCatalogFileVerifier,
    StartupCatalogFileVerifierConfig, StartupOrphanReconciler, StartupOrphanReconcilerConfig,
    StartupOrphanReconciliationReport,
};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogFileRead, CatalogRead, CatalogWrite,
};
use zippy_types::{ErrorCode, Result, ServerError, TableId};

const SUPERVISOR_THREAD_NAME: &str = "zippy-native-storage";
const REAPER_THREAD_NAME: &str = "zippy-native-storage-reaper";
const DEFAULT_SERVER_ACTOR_PRINCIPAL: &str = "zippy-server";
const STARTUP_CANCELLED_MESSAGE: &str = "native storage startup was cancelled before readiness";

/// Configuration owned by one native storage supervisor.
#[derive(Debug, Clone)]
pub struct NativeStorageRuntimeConfig {
    data_directory: Arc<OwnedDataDirectory>,
    catalog: SqliteCatalogConfig,
    coordinator: CatalogCommitCoordinatorConfig,
    reconciliation: StartupOrphanReconcilerConfig,
    catalog_file_verification: StartupCatalogFileVerifierConfig,
    table_actors: TableActorRegistryConfig,
    actor: CatalogActor,
}

impl NativeStorageRuntimeConfig {
    /// Creates a conservative runtime configuration under the process-owned data root.
    #[must_use]
    pub fn new(data_directory: &Arc<OwnedDataDirectory>) -> Self {
        Self {
            data_directory: Arc::clone(data_directory),
            catalog: SqliteCatalogConfig::new(data_directory),
            coordinator: CatalogCommitCoordinatorConfig::new(),
            reconciliation: StartupOrphanReconcilerConfig::new(),
            catalog_file_verification: StartupCatalogFileVerifierConfig::new(),
            table_actors: TableActorRegistryConfig::new(),
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
        self.table_actors.validate()?;
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
        match self.wait_for_completion_and_join() {
            Ok(()) => announced_error,
            Err(completion_error) => completion_error,
        }
    }

    fn wait_for_completion_and_join(&mut self) -> Result<()> {
        let completion_result = match self.completion.take() {
            Some(completion) => completion
                .recv()
                .unwrap_or_else(|_| Err(supervisor_stopped())),
            None => Err(supervisor_stopped()),
        };
        let join_result = join_supervisor(&mut self.supervisor);
        combine_results(completion_result, join_result)
    }

    fn into_runtime(mut self, ready: SupervisorReady) -> NativeStorageRuntime {
        self.take_runtime(ready)
    }

    fn take_runtime(&mut self, ready: SupervisorReady) -> NativeStorageRuntime {
        NativeStorageRuntime {
            report: ready.reconciliation,
            catalog_file_verification: ready.catalog_file_verification,
            table_actors: ready.table_actors,
            table_actor_admission: ready.table_actor_admission,
            shutdown: self.shutdown.take(),
            completion: self.completion.take(),
            supervisor: self.supervisor.take(),
            shutdown_requested: false,
            final_shutdown: None,
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
    table_actors: TableActorRegistryHandle,
    table_actor_admission: TableActorAdmissionControl,
    shutdown: Option<watch::Sender<bool>>,
    completion: Option<mpsc::Receiver<Result<()>>>,
    supervisor: Option<JoinHandle<()>>,
    shutdown_requested: bool,
    final_shutdown: Option<NativeStorageShutdownOutcome>,
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

    /// Irreversibly closes native work admission and requests ordered background cleanup.
    pub fn begin_shutdown(&mut self) {
        if self.shutdown_requested {
            return;
        }
        self.shutdown_requested = true;
        self.table_actor_admission.close();
        signal_shutdown(self.shutdown.as_ref());
    }

    /// Waits for ordered cleanup until an absolute monotonic deadline.
    pub fn shutdown_until(&mut self, deadline: Instant) -> NativeStorageShutdownOutcome {
        if let Some(outcome) = &self.final_shutdown {
            return outcome.clone();
        }
        self.begin_shutdown();
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
    table_actors: TableActorRegistryHandle,
    table_actor_admission: TableActorAdmissionControl,
}

fn spawn_supervisor(config: NativeStorageRuntimeConfig) -> Result<NativeStorageStartup> {
    config.validate()?;
    let (shutdown_sender, shutdown_receiver) = watch::channel(false);
    let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
    let (completion_sender, completion_receiver) = mpsc::sync_channel(1);
    let supervisor = thread::Builder::new()
        .name(SUPERVISOR_THREAD_NAME.to_owned())
        .spawn(move || {
            run_supervisor(config, shutdown_receiver, startup_sender, completion_sender);
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
    })
}

fn run_supervisor(
    config: NativeStorageRuntimeConfig,
    shutdown: watch::Receiver<bool>,
    startup: mpsc::SyncSender<Result<SupervisorReady>>,
    completion: mpsc::SyncSender<Result<()>>,
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
            let _ = startup.send(Err(error.clone()));
            let _ = completion.send(Err(error));
            return;
        }
    };
    let (ready_sent, result) = runtime.block_on(supervise_storage(config, shutdown, &startup));
    drop(runtime);
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
) -> (bool, Result<()>) {
    let catalog = match SqliteCatalog::open(config.catalog.clone()) {
        Ok(catalog) => Arc::new(catalog),
        Err(error) => {
            return (false, Err(error));
        }
    };

    if shutdown_requested(&shutdown) {
        return (
            false,
            fail_startup(startup_cancelled(), None, None, catalog).await,
        );
    }

    let pre_replay =
        match reconcile_once(&config, Arc::clone(&catalog), &mut shutdown, "pre-replay").await {
            Ok(report) => report,
            Err(error) => return (false, fail_startup(error, None, None, catalog).await),
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

    let catalog_write: Arc<dyn CatalogWrite> = catalog.clone();
    let coordinator = match wait_for_startup_stage(
        &mut shutdown,
        CatalogCommitCoordinator::start(
            config.coordinator.clone(),
            Arc::clone(&config.data_directory),
            catalog_write,
            config.actor.clone(),
        ),
    )
    .await
    {
        Ok(coordinator) => coordinator,
        Err(error) => return (false, fail_startup(error, None, None, catalog).await),
    };

    let post_replay =
        match reconcile_once(&config, Arc::clone(&catalog), &mut shutdown, "post-replay").await {
            Ok(report) => report,
            Err(error) => {
                return (
                    false,
                    fail_startup(error, None, Some(coordinator), catalog).await,
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
                    fail_startup(error, None, Some(coordinator), catalog).await,
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
            fail_startup(error, None, Some(coordinator), catalog).await,
        );
    }

    let table_runtimes = match build_table_runtime_registry(
        catalog_bootstrap,
        config
            .catalog_file_verification
            .max_runtime_metadata_bytes(),
    ) {
        Ok(registry) => registry,
        Err(error) => {
            return (
                false,
                fail_startup(error, None, Some(coordinator), catalog).await,
            );
        }
    };
    let table_actors = match TableActorRegistry::start(config.table_actors.clone(), table_runtimes)
    {
        Ok(registry) => registry,
        Err(error) => {
            return (
                false,
                fail_startup(error, None, Some(coordinator), catalog).await,
            );
        }
    };
    if shutdown_requested(&shutdown) {
        return (
            false,
            fail_startup(
                startup_cancelled(),
                Some(table_actors),
                Some(coordinator),
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
                fail_startup(error, Some(table_actors), Some(coordinator), catalog).await,
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
            fail_startup(error, Some(table_actors), Some(coordinator), catalog).await,
        );
    }
    tracing::info!(
        component = "native_storage",
        event = "native_storage_ready",
        catalog_revision = table_actor_handle.catalog_revision(),
        tables = table_actor_handle.len(),
        managed_files = catalog_file_verification.managed_file_count,
        verified_bytes = catalog_file_verification.verified_bytes,
        "native storage recovery and bidirectional verification completed"
    );

    if startup
        .send(Ok(SupervisorReady {
            reconciliation: post_replay,
            catalog_file_verification,
            table_actors: table_actor_handle,
            table_actor_admission,
        }))
        .is_err()
    {
        return (
            false,
            close_storage(Some(table_actors), Some(coordinator), catalog).await,
        );
    }

    wait_for_shutdown(&mut shutdown).await;
    (
        true,
        close_storage(Some(table_actors), Some(coordinator), catalog).await,
    )
}

async fn reconcile_once(
    config: &NativeStorageRuntimeConfig,
    catalog: Arc<SqliteCatalog>,
    shutdown: &mut watch::Receiver<bool>,
    stage: &'static str,
) -> Result<StartupOrphanReconciliationReport> {
    let catalog_read: Arc<dyn CatalogFileRead> = catalog;
    let reconciler = StartupOrphanReconciler::with_config(
        config.reconciliation.clone(),
        Arc::clone(&config.data_directory),
        catalog_read,
    )?;
    wait_for_startup_stage(shutdown, reconciler.reconcile())
        .await
        .map_err(|error| with_stage(stage, error))
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
) -> Result<TableRuntimeRegistry> {
    let catalog_revision = bootstrap.verification.catalog_revision;
    let mut tables = Vec::with_capacity(bootstrap.active_tables.len());
    let mut retained_bytes = bootstrap.runtime_metadata_bytes;
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
        retained_bytes = next_retained_bytes;
        tables.push(TableRuntimeBootstrap::new(
            descriptor.table,
            descriptor.schema,
            decoded.into_schema(),
            descriptor.snapshot,
        )?);
    }
    TableRuntimeRegistry::from_bootstraps(catalog_revision, tables)
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

async fn fail_startup(
    error: ServerError,
    table_actors: Option<TableActorRegistry>,
    coordinator: Option<CatalogCommitCoordinator>,
    catalog: Arc<SqliteCatalog>,
) -> Result<()> {
    let cleanup = close_storage(table_actors, coordinator, catalog).await;
    let surfaced = match cleanup {
        Ok(()) => error,
        Err(cleanup_error) => combine_errors(error, cleanup_error),
    };
    Err(surfaced)
}

async fn close_storage(
    table_actors: Option<TableActorRegistry>,
    coordinator: Option<CatalogCommitCoordinator>,
    catalog: Arc<SqliteCatalog>,
) -> Result<()> {
    let actor_result = match table_actors {
        Some(table_actors) => table_actors.drain().await,
        None => Ok(()),
    };
    let coordinator_result = match coordinator {
        Some(coordinator) => coordinator.drain().await,
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
    for result in [actor_result, coordinator_result, catalog_result] {
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

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn signal_shutdown(shutdown: Option<&watch::Sender<bool>>) {
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(true);
    }
}

fn with_stage(stage: &str, error: ServerError) -> ServerError {
    ServerError::new(
        error.code(),
        format!("native storage startup stage failed stage=[{stage}] error=[{error}]"),
    )
}

fn combine_errors(primary: ServerError, cleanup: ServerError) -> ServerError {
    ServerError::new(
        primary.code(),
        format!("{primary}; native storage cleanup failed error=[{cleanup}]"),
    )
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
        };

        startup
            .cancel_and_wait()
            .expect("expected startup cancellation is successful cleanup");

        assert!(finished.load(Ordering::Acquire));
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
