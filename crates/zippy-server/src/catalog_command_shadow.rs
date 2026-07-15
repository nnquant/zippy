//! Bounded legacy-to-SQLite Catalog command shadowing.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zippy_catalog::{
    SqliteCatalogLegacyReconcileHandle, SqliteCatalogReadHandle, SqliteCatalogSnapshotReadLimits,
};
use zippy_master::legacy_catalog::{
    LegacyCatalogActivePublicTableV4, LegacyCatalogAdapter, LegacyCatalogProjectionSnapshotV4,
};
use zippy_master::legacy_catalog_commit::{
    LegacyCatalogCommitEvent, LegacyCatalogCommitMutationKind, LegacyCatalogCommitPublisher,
    LegacyCatalogCommitPublisherMetrics, LegacyCatalogCommitPublisherMetricsSnapshot,
};
use zippy_master::server::MasterServer;
use zippy_types::catalog::{
    compute_legacy_catalog_projection_digest_v4, compute_legacy_catalog_source_metadata_hash_v4,
    CatalogActor, CatalogActorKind, CatalogRead, CatalogSnapshotSelector, CommandFingerprint,
    LegacyCatalogAdoptExistingTableCommandV4, LegacyCatalogAdvanceCheckpointCommandV4,
    LegacyCatalogBootstrapTableCommandV4, LegacyCatalogCheckpointModeV4,
    LegacyCatalogDropTableCommandV4, LegacyCatalogProjectionCheckpointV4,
    LegacyCatalogProjectionCommandV4, LegacyCatalogProjectionCompletenessV4,
    LegacyCatalogProjectionDigestTableV4, LegacyCatalogProjectionEnvelopeV4,
    LegacyCatalogProjectionOutcomeV4, LegacyCatalogProjectionPhaseV4,
    LegacyCatalogProjectionResponseV4, LegacyCatalogProjectionTableStateV4,
    LegacyCatalogReconcileCommandV3, LegacyCatalogReconcileEnvelopeV3,
    LegacyCatalogSyncTableMetadataCommandV4, LegacyCatalogTableProjectionV4,
    LegacyFileViewSnapshotMapping, ListTablesRequest,
};
use zippy_types::table::{TableDescriptor, TableRef, TableState};
use zippy_types::{ErrorCode, RequestId, ServerError, TableId};

use crate::legacy_catalog_migration::{
    verify_legacy_catalog_projection_table_view, VerifiedLegacyCatalogFileCacheEntry,
    VerifiedLegacyCatalogTableView, LEGACY_CATALOG_METADATA_MIGRATION_VERSION,
    LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION, LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE,
    LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE, LEGACY_MIGRATION_VERSION_CONFIG_KEY,
    LEGACY_REVISION_BASE_CONFIG_KEY, LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY,
    LEGACY_STREAM_NAME_CONFIG_KEY, NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
};

const WORKER_THREAD_NAME: &str = "zippy-legacy-catalog-shadow";
const RECONCILE_ACTOR_PRINCIPAL: &str = "zippy-server/legacy-catalog-shadow-v3";
const REQUEST_ID_DOMAIN: &[u8] = b"zippy.legacy_catalog.shadow.reconcile.request.v3\0";
const PROJECTION_ACTOR_PRINCIPAL: &str = "zippy-server/legacy-catalog-shadow-v4";
const PROJECTION_REQUEST_ID_DOMAIN: &[u8] = b"zippy.legacy_catalog.shadow.projection.request.v4\0";
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RECOVERABLE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const TABLE_LIST_PAGE_SIZE: u32 = 256;
const MIGRATED_SCHEMA_VERSION: u64 = 1;

/// Fail-closed eligibility state of the online legacy Catalog shadow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogShadowState {
    /// The worker is consuming events or reconciling an authoritative full snapshot.
    CatchingUp,
    /// A stable full snapshot and exact native diff proved current equivalence.
    EligibleCandidate,
    /// A retryable gap, timeout, physical verification, or Catalog error blocks eligibility.
    BlockedRecoverable,
    /// The full projection diff found an identity or generation state V4 cannot represent.
    BlockedUnsupported,
    /// The publisher was sealed and the worker thread drained and exited.
    Closed,
}

/// Bounded publisher counters retained with one shadow status snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogShadowPublisherStatus {
    /// Commit notifications accepted by the bounded receiver.
    pub published_total: u64,
    /// Worker-local notifications rejected because the queue was full.
    pub queue_full_total: u64,
    /// Notifications rejected because the receiver was unavailable.
    pub disconnected_total: u64,
    /// Legacy mutation boundaries that could not emit a durable event.
    pub unpublished_mutation_total: u64,
    /// Worker-local queue-continuity loss epoch; durable status uses the resolved epoch.
    pub continuity_loss_epoch: u64,
    /// Revision captured when observation began.
    pub baseline_revision: u64,
    /// Latest legacy Catalog revision observed at mutation admission.
    pub latest_observed_revision: u64,
    /// Final revision captured by publisher seal.
    pub final_revision: u64,
    /// Whether event publication has been sealed.
    pub sealed: bool,
}

impl From<LegacyCatalogCommitPublisherMetricsSnapshot> for LegacyCatalogShadowPublisherStatus {
    fn from(metrics: LegacyCatalogCommitPublisherMetricsSnapshot) -> Self {
        Self {
            published_total: metrics.published_total,
            queue_full_total: metrics.queue_full_total,
            disconnected_total: metrics.disconnected_total,
            unpublished_mutation_total: metrics.unpublished_mutation_total,
            continuity_loss_epoch: metrics.continuity_loss_epoch,
            baseline_revision: metrics.baseline_revision,
            latest_observed_revision: metrics.latest_observed_revision,
            final_revision: metrics.final_revision,
            sealed: metrics.sealed,
        }
    }
}

/// Bounded durable V4 checkpoint evidence associated with one eligible shadow state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogShadowCheckpointStatus {
    /// Monotonic checkpoint compare-and-swap generation.
    pub checkpoint_generation: u64,
    /// Projection-worker fencing epoch.
    pub shadow_epoch: u64,
    /// Durable authority phase; this worker never promotes into read cutover readiness.
    pub phase: LegacyCatalogProjectionPhaseV4,
    /// Source Catalog revision durably acknowledged by the checkpoint.
    pub source_catalog_revision: u64,
    /// Publisher continuity-loss epoch resolved by the full reconcile.
    pub resolved_continuity_loss_epoch: u64,
    /// Complete active projection digest recomputed and persisted by Catalog.
    pub projection_digest: String,
}

impl From<&LegacyCatalogProjectionCheckpointV4> for LegacyCatalogShadowCheckpointStatus {
    fn from(checkpoint: &LegacyCatalogProjectionCheckpointV4) -> Self {
        Self {
            checkpoint_generation: checkpoint.checkpoint_generation,
            shadow_epoch: checkpoint.shadow_epoch,
            phase: checkpoint.phase,
            source_catalog_revision: checkpoint.source_catalog_revision,
            resolved_continuity_loss_epoch: checkpoint.resolved_continuity_loss_epoch,
            projection_digest: checkpoint.projection_digest.clone(),
        }
    }
}

/// Observable state and evidence counters for the online Catalog shadow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogShadowStatus {
    /// Current fail-closed state.
    pub state: LegacyCatalogShadowState,
    /// Last legacy Catalog revision consumed from the event stream or covered by full reconcile.
    pub consumed_catalog_revision: u64,
    /// Last stable legacy Catalog revision proven by a full exact reconcile.
    pub reconciled_catalog_revision: u64,
    /// Publisher loss epoch covered by the last successful full reconcile.
    pub recovered_continuity_loss_epoch: u64,
    /// Worker-local publisher loss epoch covered by the last successful full reconcile.
    pub recovered_worker_local_continuity_loss_epoch: u64,
    /// Full reconcile attempts, including failed attempts.
    pub full_reconciles_total: u64,
    /// Per-table exact manifest comparisons that succeeded.
    pub exact_diffs_total: u64,
    /// V3 commands committed or idempotently replayed.
    pub v3_reconciles_total: u64,
    /// V4 BootstrapTable responses committed or idempotently replayed.
    pub v4_bootstrap_total: u64,
    /// V4 AdoptExistingTable responses committed or idempotently replayed.
    pub v4_adopt_total: u64,
    /// V4 SyncTableMetadata responses committed or idempotently replayed.
    pub v4_metadata_sync_total: u64,
    /// V4 DropTable responses committed or idempotently replayed.
    pub v4_drop_total: u64,
    /// V4 AdvanceCheckpoint responses committed or idempotently replayed.
    pub v4_checkpoint_total: u64,
    /// Event revision gaps observed by the consumer.
    pub revision_gaps_total: u64,
    /// Operation deadlines that elapsed.
    pub timeouts_total: u64,
    /// Catalog read or write failures.
    pub catalog_errors_total: u64,
    /// Full projection diffs that found an unrepresentable state.
    pub unsupported_mutations_total: u64,
    /// Short, path-free failure category for operations diagnostics.
    pub last_error: Option<String>,
    /// Current bounded publisher evidence.
    pub publisher: LegacyCatalogShadowPublisherStatus,
    /// Last checkpoint read from Catalog; eligibility requires this evidence to be revalidated.
    pub checkpoint: Option<LegacyCatalogShadowCheckpointStatus>,
}

impl LegacyCatalogShadowStatus {
    fn initial(metrics: LegacyCatalogCommitPublisherMetricsSnapshot) -> Self {
        Self {
            state: LegacyCatalogShadowState::CatchingUp,
            consumed_catalog_revision: metrics.baseline_revision,
            reconciled_catalog_revision: 0,
            recovered_continuity_loss_epoch: 0,
            recovered_worker_local_continuity_loss_epoch: 0,
            full_reconciles_total: 0,
            exact_diffs_total: 0,
            v3_reconciles_total: 0,
            v4_bootstrap_total: 0,
            v4_adopt_total: 0,
            v4_metadata_sync_total: 0,
            v4_drop_total: 0,
            v4_checkpoint_total: 0,
            revision_gaps_total: 0,
            timeouts_total: 0,
            catalog_errors_total: 0,
            unsupported_mutations_total: 0,
            last_error: None,
            publisher: metrics.into(),
            checkpoint: None,
        }
    }
}

/// Final state of a bounded shadow-worker drain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyCatalogShadowShutdownOutcome {
    /// Publisher drain, final reconcile, and thread join completed.
    Complete,
    /// The server shutdown deadline elapsed while the worker retained ownership.
    TimedOut,
    /// The worker thread or its local runtime failed structurally.
    Failed(ServerError),
}

impl LegacyCatalogShadowShutdownOutcome {
    /// Returns whether the worker released all of its resources.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Returns a structural shutdown error, when present.
    #[must_use]
    pub const fn error(&self) -> Option<&ServerError> {
        match self {
            Self::Failed(error) => Some(error),
            Self::Complete | Self::TimedOut => None,
        }
    }
}

pub(crate) struct LegacyCatalogShadowWorkerConfig {
    pub(crate) queue_capacity: usize,
    pub(crate) max_files_per_table: usize,
    pub(crate) max_metadata_bytes_per_table: usize,
    pub(crate) operation_timeout: Duration,
    pub(crate) trusted_fallback_root: Option<PathBuf>,
}

pub(crate) struct LegacyCatalogShadowWorker {
    status: Arc<Mutex<LegacyCatalogShadowStatus>>,
    metrics: LegacyCatalogCommitPublisherMetrics,
    initial_attempt: Option<Receiver<Result<(), ServerError>>>,
    completion: Option<Receiver<Result<(), ServerError>>>,
    thread: Option<JoinHandle<()>>,
    final_shutdown: Option<LegacyCatalogShadowShutdownOutcome>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyCatalogShadowTestFault {
    None,
    InitialRecoverableOnce,
    ExitBeforeInitialAttempt,
    DelayBeforeInitialAttempt(Duration),
    DisconnectBeforeFinalSeal,
}

impl LegacyCatalogShadowWorker {
    #[cfg(not(test))]
    pub(crate) fn install_and_start(
        master: &MasterServer,
        catalog_read: SqliteCatalogReadHandle,
        catalog_reconcile: SqliteCatalogLegacyReconcileHandle,
        config: LegacyCatalogShadowWorkerConfig,
    ) -> Result<Self, ServerError> {
        Self::install_and_start_inner(
            master,
            catalog_read,
            catalog_reconcile,
            config,
            #[cfg(test)]
            LegacyCatalogShadowTestFault::None,
        )
    }

    #[cfg(test)]
    pub(crate) fn install_and_start_with_test_fault(
        master: &MasterServer,
        catalog_read: SqliteCatalogReadHandle,
        catalog_reconcile: SqliteCatalogLegacyReconcileHandle,
        config: LegacyCatalogShadowWorkerConfig,
        test_fault: LegacyCatalogShadowTestFault,
    ) -> Result<Self, ServerError> {
        Self::install_and_start_inner(master, catalog_read, catalog_reconcile, config, test_fault)
    }

    fn install_and_start_inner(
        master: &MasterServer,
        catalog_read: SqliteCatalogReadHandle,
        catalog_reconcile: SqliteCatalogLegacyReconcileHandle,
        config: LegacyCatalogShadowWorkerConfig,
        #[cfg(test)] test_fault: LegacyCatalogShadowTestFault,
    ) -> Result<Self, ServerError> {
        let capacity = NonZeroUsize::new(config.queue_capacity).ok_or_else(|| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                "legacy Catalog shadow queue capacity must be positive",
            )
            .with_retryable(false)
        })?;
        if config.operation_timeout.is_zero() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "legacy Catalog shadow operation timeout must be positive",
            )
            .with_retryable(false));
        }
        let read_limits = SqliteCatalogSnapshotReadLimits {
            max_visible_files: config.max_files_per_table,
            max_raw_response_bytes: config.max_metadata_bytes_per_table,
            ..SqliteCatalogSnapshotReadLimits::default()
        }
        .validate()?;
        let (publisher, receiver) = LegacyCatalogCommitPublisher::bounded(capacity);
        let metrics = publisher.metrics();
        // Thread creation can fail, while the Master publisher slot is irreversible. Start the
        // thread first but hold reconciliation until installation has seeded its exact baseline.
        let (activation_tx, activation_rx) = mpsc::sync_channel(1);
        let (initial_attempt_tx, initial_attempt_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(WORKER_THREAD_NAME.to_owned())
            .spawn(move || {
                let result = match activation_rx.recv() {
                    Ok(engine) => run_worker(
                        receiver,
                        engine,
                        initial_attempt_tx,
                        #[cfg(test)]
                        test_fault,
                    ),
                    Err(_) => Err(worker_stopped()),
                };
                let _ = completion_tx.send(result);
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start legacy Catalog shadow worker error=[{error}]"),
                )
                .with_retryable(false)
            })?;
        if let Err(error) = master.install_legacy_catalog_commit_publisher(publisher) {
            drop(activation_tx);
            let _ = thread.join();
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!("install legacy Catalog shadow publisher error=[{error}]"),
            )
            .with_retryable(false));
        }
        let source = master.legacy_catalog_adapter();
        let status = Arc::new(Mutex::new(LegacyCatalogShadowStatus::initial(
            metrics.snapshot(),
        )));
        let engine = ShadowEngine {
            source,
            catalog_read,
            catalog_reconcile,
            metrics: metrics.clone(),
            status: Arc::clone(&status),
            trusted_fallback_root: config.trusted_fallback_root,
            operation_timeout: config.operation_timeout,
            read_limits,
            max_files_per_table: config.max_files_per_table,
            max_metadata_bytes_per_table: config.max_metadata_bytes_per_table,
            verified: BTreeMap::new(),
            loss_epoch_base: None,
            pending_explicit_drops: BTreeSet::new(),
            generation_conflict_latched: false,
        };
        if activation_tx.send(engine).is_err() {
            let _ = thread.join();
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy Catalog shadow worker stopped before activation",
            ));
        }
        Ok(Self {
            status,
            metrics,
            initial_attempt: Some(initial_attempt_rx),
            completion: Some(completion_rx),
            thread: Some(thread),
            final_shutdown: None,
        })
    }

    /// Waits for the first semantic reconcile attempt before listeners may accept mutations.
    ///
    /// Physical Parquet verification currently uses non-cancellable blocking work. Consequently,
    /// this barrier deliberately owns the worker until that first attempt ends; the server startup
    /// deadline is checked immediately afterwards and is a soft limit across this stage.
    pub(crate) fn wait_for_initial_attempt(&mut self) -> Result<(), ServerError> {
        let Some(initial_attempt) = self.initial_attempt.take() else {
            return Err(worker_stopped());
        };
        match initial_attempt.recv() {
            Ok(result) => result,
            Err(_) => Err(worker_stopped()),
        }
    }

    pub(crate) fn status(&self) -> LegacyCatalogShadowStatus {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let publisher = self.metrics.snapshot();
        if status.state == LegacyCatalogShadowState::EligibleCandidate
            && (publisher.latest_observed_revision > status.reconciled_catalog_revision
                || publisher.continuity_loss_epoch
                    > status.recovered_worker_local_continuity_loss_epoch)
        {
            status.state = if publisher.continuity_loss_epoch
                > status.recovered_worker_local_continuity_loss_epoch
            {
                LegacyCatalogShadowState::BlockedRecoverable
            } else {
                LegacyCatalogShadowState::CatchingUp
            };
        }
        status.publisher = publisher.into();
        status
    }

    pub(crate) fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> LegacyCatalogShadowShutdownOutcome {
        self.shutdown_with_wait(Some(deadline))
    }

    /// Drains and joins without a deadline during startup rollback.
    ///
    /// Startup rollback must not detach a verifier that outlived the soft startup deadline.
    pub(crate) fn shutdown_and_join(&mut self) -> LegacyCatalogShadowShutdownOutcome {
        self.shutdown_with_wait(None)
    }

    fn shutdown_with_wait(
        &mut self,
        deadline: Option<Instant>,
    ) -> LegacyCatalogShadowShutdownOutcome {
        if let Some(outcome) = &self.final_shutdown {
            return outcome.clone();
        }
        let Some(completion) = self.completion.as_ref() else {
            let outcome = LegacyCatalogShadowShutdownOutcome::Failed(worker_stopped());
            self.final_shutdown = Some(outcome.clone());
            return outcome;
        };
        let received = match deadline {
            Some(deadline) => completion
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => None,
                    mpsc::RecvTimeoutError::Disconnected => Some(worker_stopped()),
                }),
            None => completion.recv().map_err(|_| Some(worker_stopped())),
        };
        let mut outcome = match received {
            Ok(Ok(())) => LegacyCatalogShadowShutdownOutcome::Complete,
            Ok(Err(error)) => LegacyCatalogShadowShutdownOutcome::Failed(error),
            Err(None) => LegacyCatalogShadowShutdownOutcome::TimedOut,
            Err(Some(error)) => LegacyCatalogShadowShutdownOutcome::Failed(error),
        };
        if !matches!(outcome, LegacyCatalogShadowShutdownOutcome::TimedOut) {
            self.completion.take();
            if self
                .thread
                .take()
                .is_some_and(|thread| thread.join().is_err())
            {
                outcome = LegacyCatalogShadowShutdownOutcome::Failed(ServerError::new(
                    ErrorCode::Internal,
                    "legacy Catalog shadow worker panicked",
                ));
            }
            self.final_shutdown = Some(outcome.clone());
        }
        outcome
    }
}

impl Drop for LegacyCatalogShadowWorker {
    fn drop(&mut self) {
        if self
            .thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
        {
            tracing::warn!(
                component = "legacy_catalog_shadow",
                event = "legacy_catalog_shadow_worker_detached",
                "legacy Catalog shadow worker still owns its drain after handle drop"
            );
        }
    }
}

struct ShadowEngine {
    source: LegacyCatalogAdapter,
    catalog_read: SqliteCatalogReadHandle,
    catalog_reconcile: SqliteCatalogLegacyReconcileHandle,
    metrics: LegacyCatalogCommitPublisherMetrics,
    status: Arc<Mutex<LegacyCatalogShadowStatus>>,
    trusted_fallback_root: Option<PathBuf>,
    operation_timeout: Duration,
    read_limits: SqliteCatalogSnapshotReadLimits,
    max_files_per_table: usize,
    max_metadata_bytes_per_table: usize,
    verified: BTreeMap<TableId, BTreeMap<String, VerifiedLegacyCatalogFileCacheEntry>>,
    loss_epoch_base: Option<u64>,
    pending_explicit_drops: BTreeSet<TableId>,
    generation_conflict_latched: bool,
}

#[derive(Debug)]
enum ShadowFailure {
    Recoverable(&'static str),
    Unsupported(&'static str),
    Timeout(&'static str),
    Catalog(&'static str, ServerError),
    PermanentCatalog(&'static str, ServerError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FullReconcileEvidence {
    revision: u64,
    loss_epoch: u64,
    worker_local_loss_epoch: u64,
    checkpoint: LegacyCatalogShadowCheckpointStatus,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionTableObservation<'a> {
    table_id: TableId,
    source: Option<&'a LegacyCatalogActivePublicTableV4>,
    native: Option<&'a TableDescriptor>,
    projection: Option<&'a LegacyCatalogTableProjectionV4>,
    durable_drop_tombstone: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedShadowCommand {
    ReconcileV3,
    BootstrapV4,
    AdoptV4,
    SyncMetadataV4,
    DropV4,
    CheckpointV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionPlanningState {
    Absent,
    SeededActive,
    CompleteActive,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TablePlanningState {
    source_present: bool,
    native_present: bool,
    projection: ProjectionPlanningState,
    file_order: Option<Ordering>,
    writer_order: Option<Ordering>,
    metadata_changed: bool,
    strict_unprojected_v1: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannedTableTransition {
    None,
    Bootstrap,
    Adopt,
    AdoptThenSync,
    AdvanceFilesThenAdopt,
    Sync,
    AdvanceFiles,
    SyncThenAdvanceFiles,
    DropComplete,
    DropSeeded,
    DropUnprojectedV1,
}

fn plan_table_transition(
    state: TablePlanningState,
) -> Result<PlannedTableTransition, &'static str> {
    use PlannedTableTransition as Transition;
    use ProjectionPlanningState as Projection;

    match (state.source_present, state.native_present, state.projection) {
        (true, false, Projection::Absent) => Ok(Transition::Bootstrap),
        (true, false, Projection::Dropped) => {
            Err("legacy_table_recreate_requires_projection_generation_v5")
        }
        (true, false, _) => Err("legacy_projection_exists_without_active_native_table"),
        (true, true, Projection::Dropped) => {
            // V4 has no source generation, so a deterministic TableId cannot distinguish recreate.
            Err("legacy_table_recreate_requires_projection_generation_v5")
        }
        (true, true, Projection::Absent) => {
            if !state.strict_unprojected_v1 {
                return Err("unprojected_native_table_is_not_strict_empty_v1");
            }
            match (state.file_order, state.writer_order) {
                (Some(Ordering::Less), Some(Ordering::Less | Ordering::Equal)) => {
                    Ok(Transition::AdvanceFilesThenAdopt)
                }
                (Some(Ordering::Equal), Some(Ordering::Less)) => Ok(Transition::AdoptThenSync),
                (Some(Ordering::Equal), Some(Ordering::Equal)) => Ok(Transition::Adopt),
                (Some(Ordering::Greater), _) => Err("native_legacy_revision_is_ahead_of_primary"),
                (_, Some(Ordering::Greater)) => Err("legacy_writer_epoch_is_behind_native_table"),
                _ => Err("unprojected_native_table_cursor_is_incomplete"),
            }
        }
        (true, true, Projection::SeededActive) => match (state.file_order, state.writer_order) {
            (Some(Ordering::Less), Some(Ordering::Less | Ordering::Equal)) => {
                Ok(Transition::AdvanceFilesThenAdopt)
            }
            (Some(Ordering::Equal), Some(Ordering::Less)) => Ok(Transition::AdoptThenSync),
            (Some(Ordering::Equal), Some(Ordering::Equal)) => Ok(Transition::Adopt),
            (Some(Ordering::Greater), _) => Err("native_legacy_revision_is_ahead_of_primary"),
            (_, Some(Ordering::Greater)) => Err("legacy_writer_epoch_is_behind_native_table"),
            _ => Err("seeded_projection_cursor_is_incomplete"),
        },
        (true, true, Projection::CompleteActive) => {
            if state.file_order == Some(Ordering::Greater) {
                return Err("native_legacy_revision_is_ahead_of_primary");
            }
            if state.writer_order == Some(Ordering::Greater) {
                return Err("legacy_writer_epoch_is_behind_native_table");
            }
            match (state.file_order, state.metadata_changed) {
                (Some(Ordering::Less), true) => Ok(Transition::SyncThenAdvanceFiles),
                (Some(Ordering::Less), false) => Ok(Transition::AdvanceFiles),
                (Some(Ordering::Equal), true) => Ok(Transition::Sync),
                (Some(Ordering::Equal), false) => Ok(Transition::None),
                _ => Err("complete_projection_cursor_is_incomplete"),
            }
        }
        (false, false, Projection::Absent | Projection::Dropped) => Ok(Transition::None),
        (false, false, _) => Err("active_projection_exists_without_native_table"),
        (false, true, Projection::CompleteActive) => Ok(Transition::DropComplete),
        (false, true, Projection::SeededActive) => Ok(Transition::DropSeeded),
        (false, true, Projection::Absent) if state.strict_unprojected_v1 => {
            Ok(Transition::DropUnprojectedV1)
        }
        (false, true, Projection::Absent) => Err("unprojected_native_drop_is_not_strict_empty_v1"),
        (false, true, Projection::Dropped) => Err("dropped_projection_has_active_native_table"),
    }
}

impl ShadowFailure {
    fn final_server_error(&self) -> ServerError {
        match self {
            Self::Recoverable(reason) => ServerError::new(
                ErrorCode::Unavailable,
                format!("final legacy Catalog shadow reconcile failed reason=[{reason}]"),
            )
            .with_retryable(true),
            Self::Unsupported(reason) => ServerError::new(
                ErrorCode::Conflict,
                format!("final legacy Catalog shadow reconcile unsupported reason=[{reason}]"),
            )
            .with_retryable(false),
            Self::Timeout(operation) => ServerError::new(
                ErrorCode::DeadlineExceeded,
                format!("final legacy Catalog shadow reconcile timed out operation=[{operation}]"),
            )
            .with_retryable(true),
            Self::Catalog(operation, error) => error.clone().with_message(format!(
                "final legacy Catalog shadow reconcile failed operation=[{operation}] code=[{}]",
                error.code()
            )),
            Self::PermanentCatalog(operation, error) => error.clone().with_message(format!(
                concat!(
                    "final legacy Catalog shadow reconcile permanently failed ",
                    "operation=[{}] code=[{}]"
                ),
                operation,
                error.code()
            )),
        }
    }
}

impl ShadowEngine {
    fn apply_events(&mut self, events: &[LegacyCatalogCommitEvent]) -> bool {
        let publisher = self.metrics.snapshot();
        let effective_loss_epoch = match self.effective_loss_epoch(publisher.continuity_loss_epoch)
        {
            Ok(loss_epoch) => loss_epoch,
            Err(failure) => {
                self.record_failure(&failure);
                return true;
            }
        };
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let was_candidate = status.state == LegacyCatalogShadowState::EligibleCandidate;
        let mut requires_reconcile = !was_candidate;
        let mut generation_conflict = false;
        for event in events {
            generation_conflict |= observe_generation_mutation(
                &mut self.pending_explicit_drops,
                event.mutation_kind(),
                event.stream().map(|stream| stream.table_id()),
            );
            if event.committed_revision() <= status.consumed_catalog_revision {
                continue;
            }
            let contiguous = event.previous_revision() == status.consumed_catalog_revision
                && event.committed_revision()
                    == event.previous_revision().checked_add(1).unwrap_or(0);
            if !contiguous {
                status.revision_gaps_total = status.revision_gaps_total.saturating_add(1);
                status.state = LegacyCatalogShadowState::BlockedRecoverable;
                status.last_error = Some("legacy_event_revision_gap".to_owned());
            }
            status.consumed_catalog_revision = event.committed_revision();
            // Notifications are bounded wakeups and continuity evidence only. Every source
            // revision, including a watermark-only revision, must reach the durable V4 checkpoint.
            requires_reconcile = true;
        }
        if effective_loss_epoch > status.recovered_continuity_loss_epoch {
            status.state = LegacyCatalogShadowState::BlockedRecoverable;
            status.last_error = Some("legacy_event_queue_continuity_lost".to_owned());
            requires_reconcile = true;
        }
        if generation_conflict || self.generation_conflict_latched {
            let first_observation = !self.generation_conflict_latched;
            self.generation_conflict_latched = true;
            status.state = LegacyCatalogShadowState::BlockedUnsupported;
            status.last_error =
                Some("legacy_drop_recreate_requires_projection_generation_v5".to_owned());
            if first_observation {
                status.unsupported_mutations_total =
                    status.unsupported_mutations_total.saturating_add(1);
            }
            status.publisher = publisher.into();
            return false;
        }
        if !requires_reconcile {
            status.reconciled_catalog_revision = status.consumed_catalog_revision;
            status.state = LegacyCatalogShadowState::EligibleCandidate;
            status.last_error = None;
        }
        status.publisher = publisher.into();
        requires_reconcile
    }

    async fn full_reconcile(&mut self) -> Result<FullReconcileEvidence, ShadowFailure> {
        {
            let mut status = self.lock_status();
            status.full_reconciles_total = status.full_reconciles_total.saturating_add(1);
            status.state = LegacyCatalogShadowState::CatchingUp;
        }
        if self.generation_conflict_latched {
            return Err(ShadowFailure::Unsupported(
                "legacy_drop_recreate_requires_projection_generation_v5",
            ));
        }
        let evidence = self.try_full_reconcile().await?;
        {
            let mut status = self.lock_status();
            status.state = LegacyCatalogShadowState::EligibleCandidate;
            status.consumed_catalog_revision =
                status.consumed_catalog_revision.max(evidence.revision);
            status.reconciled_catalog_revision = evidence.revision;
            status.recovered_continuity_loss_epoch = evidence.loss_epoch;
            status.recovered_worker_local_continuity_loss_epoch = evidence.worker_local_loss_epoch;
            status.checkpoint = Some(evidence.checkpoint.clone());
            status.last_error = None;
        }
        Ok(evidence)
    }

    async fn reconcile_and_record_failure(&mut self) {
        if let Err(failure) = self.full_reconcile().await {
            self.record_failure(&failure);
        }
    }

    async fn try_full_reconcile(&mut self) -> Result<FullReconcileEvidence, ShadowFailure> {
        let first = self.capture_snapshot()?;
        let initial_checkpoint = self.read_checkpoint().await?;
        self.establish_loss_epoch_base(
            &initial_checkpoint,
            self.metrics.snapshot().continuity_loss_epoch,
        )?;
        self.lock_status().checkpoint = Some((&initial_checkpoint).into());
        if initial_checkpoint.phase == LegacyCatalogProjectionPhaseV4::NativeAuthoritative {
            return Err(ShadowFailure::Unsupported(
                "native_authoritative_checkpoint_forbids_legacy_shadow",
            ));
        }
        if first.catalog_revision() < initial_checkpoint.source_catalog_revision {
            return Err(ShadowFailure::Unsupported(
                "legacy_source_revision_is_behind_projection_checkpoint",
            ));
        }
        if initial_checkpoint.checkpoint_generation > 0
            && first.catalog_revision() == initial_checkpoint.source_catalog_revision
            && first.global_projection_digest() != initial_checkpoint.source_projection_hash
        {
            return Err(ShadowFailure::Unsupported(
                "legacy_source_digest_changed_without_revision_advance",
            ));
        }
        let native = self.list_migrated_tables().await?;
        // This trusted full-state read is deliberately unbounded at the protocol layer in V4.
        let projections = self.list_projections().await?;
        let source = first
            .tables()
            .iter()
            .map(|table| (table.descriptor().table_id, table))
            .collect::<BTreeMap<_, _>>();
        let dropped = first
            .dropped_tables()
            .iter()
            .map(|table| table.table_id())
            .collect::<BTreeSet<_>>();
        let mut table_ids = source.keys().copied().collect::<BTreeSet<_>>();
        table_ids.extend(native.keys().copied());
        table_ids.extend(projections.keys().copied());
        table_ids.extend(dropped.iter().copied());
        for table_id in table_ids {
            let exact_diffs = self
                .reconcile_projection_table(
                    &first,
                    &initial_checkpoint,
                    ProjectionTableObservation {
                        table_id,
                        source: source.get(&table_id).copied(),
                        native: native.get(&table_id),
                        projection: projections.get(&table_id),
                        durable_drop_tombstone: dropped.contains(&table_id),
                    },
                )
                .await?;
            if exact_diffs > 0 {
                let mut status = self.lock_status();
                status.exact_diffs_total = status.exact_diffs_total.saturating_add(exact_diffs);
            }
        }
        let second = self.capture_snapshot()?;
        if !same_projection_snapshot(&first, &second) {
            return Err(ShadowFailure::Recoverable(
                "legacy_snapshot_changed_during_full_reconcile",
            ));
        }
        let publisher_before_checkpoint = self.metrics.snapshot();
        let effective_loss_epoch =
            self.effective_loss_epoch(publisher_before_checkpoint.continuity_loss_epoch)?;
        if publisher_before_checkpoint.latest_observed_revision != second.catalog_revision() {
            return Err(ShadowFailure::Recoverable(
                "legacy_publisher_moved_before_projection_checkpoint",
            ));
        }
        let checkpoint_before_advance = self.read_checkpoint().await?;
        self.lock_status().checkpoint = Some((&checkpoint_before_advance).into());
        let checkpoint = self
            .advance_checkpoint_if_needed(&second, &checkpoint_before_advance, effective_loss_epoch)
            .await?;
        self.lock_status().checkpoint = Some((&checkpoint).into());
        let verified_checkpoint = self.verify_durable_projection(&second, &checkpoint).await?;
        let third = self.capture_snapshot()?;
        let publisher = self.metrics.snapshot();
        let final_effective_loss_epoch =
            self.effective_loss_epoch(publisher.continuity_loss_epoch)?;
        if !same_projection_snapshot(&second, &third)
            || publisher.latest_observed_revision != third.catalog_revision()
            || final_effective_loss_epoch != verified_checkpoint.resolved_continuity_loss_epoch
            || (publisher.sealed && publisher.final_revision != third.catalog_revision())
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_snapshot_or_publisher_moved_after_projection_checkpoint",
            ));
        }
        Ok(FullReconcileEvidence {
            revision: third.catalog_revision(),
            loss_epoch: final_effective_loss_epoch,
            worker_local_loss_epoch: publisher.continuity_loss_epoch,
            checkpoint: (&verified_checkpoint).into(),
        })
    }

    fn capture_snapshot(&self) -> Result<LegacyCatalogProjectionSnapshotV4, ShadowFailure> {
        self.source
            .capture_projection_snapshot_v4()
            .map_err(|_| ShadowFailure::Recoverable("legacy_projection_snapshot_capture_failed"))
    }

    async fn read_checkpoint(&self) -> Result<LegacyCatalogProjectionCheckpointV4, ShadowFailure> {
        self.catalog_timeout(
            "catalog_read_projection_checkpoint_v4",
            self.catalog_read.read_legacy_projection_checkpoint_v4(),
        )
        .await
    }

    async fn list_projections(
        &self,
    ) -> Result<BTreeMap<TableId, LegacyCatalogTableProjectionV4>, ShadowFailure> {
        let projections = self
            .catalog_timeout(
                "catalog_list_table_projections_v4",
                self.catalog_read.list_legacy_table_projections_v4(),
            )
            .await?;
        Ok(projections
            .into_iter()
            .map(|projection| (projection.table_id, projection))
            .collect())
    }

    async fn list_migrated_tables(
        &self,
    ) -> Result<BTreeMap<TableId, TableDescriptor>, ShadowFailure> {
        let mut tables = BTreeMap::new();
        let mut after = None;
        let limit = NonZeroU32::new(TABLE_LIST_PAGE_SIZE).expect("page size is nonzero");
        loop {
            let request = ListTablesRequest {
                namespace_id: None,
                after,
                limit,
            };
            let page = self
                .catalog_timeout(
                    "catalog_list_tables",
                    self.catalog_read.list_tables(request),
                )
                .await?;
            for table in page.tables {
                if let Some(stream_name) = table.config.get(LEGACY_STREAM_NAME_CONFIG_KEY) {
                    if stream_name != table.name.as_str() || table.state != TableState::Active {
                        return Err(ShadowFailure::Unsupported(
                            "native_legacy_table_identity_or_state_changed",
                        ));
                    }
                    tables.insert(table.table_id, table);
                }
            }
            match page.next_after {
                Some(next) => after = Some(next),
                None => break,
            }
        }
        Ok(tables)
    }

    async fn reconcile_projection_table(
        &mut self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        checkpoint: &LegacyCatalogProjectionCheckpointV4,
        observation: ProjectionTableObservation<'_>,
    ) -> Result<u64, ShadowFailure> {
        let ProjectionTableObservation {
            table_id,
            source,
            native,
            projection,
            durable_drop_tombstone,
        } = observation;
        let explicit_drop_pending =
            durable_drop_tombstone || self.pending_explicit_drops.contains(&table_id);
        if explicit_drop_pending && source.is_some() {
            self.generation_conflict_latched = true;
            return Err(ShadowFailure::Unsupported(
                "legacy_drop_recreate_requires_projection_generation_v5",
            ));
        }
        if source.is_none()
            && native.is_some()
            && !explicit_drop_pending
            && !projection
                .is_some_and(|value| value.state == LegacyCatalogProjectionTableStateV4::Dropped)
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_table_source_absent_without_explicit_drop",
            ));
        }
        if explicit_drop_pending
            && source.is_none()
            && native.is_none()
            && projection
                .is_some_and(|value| value.state == LegacyCatalogProjectionTableStateV4::Dropped)
        {
            self.pending_explicit_drops.remove(&table_id);
        }

        let strict_v1_revision = native.and_then(strict_unprojected_v1_revision);
        let current_legacy_revision = projection
            .map(|value| value.legacy_files_revision)
            .or(strict_v1_revision);
        let target_source_hash = source
            .map(|table| {
                compute_legacy_catalog_source_metadata_hash_v4(table.sources()).map_err(|_| {
                    ShadowFailure::Unsupported("legacy_source_metadata_hash_is_invalid")
                })
            })
            .transpose()?;
        let projection_state = match projection {
            None => ProjectionPlanningState::Absent,
            Some(value) if value.state == LegacyCatalogProjectionTableStateV4::Dropped => {
                ProjectionPlanningState::Dropped
            }
            Some(value) if value.completeness == LegacyCatalogProjectionCompletenessV4::Seeded => {
                ProjectionPlanningState::SeededActive
            }
            Some(_) => ProjectionPlanningState::CompleteActive,
        };
        let state = TablePlanningState {
            source_present: source.is_some(),
            native_present: native.is_some(),
            projection: projection_state,
            file_order: source.and_then(|table| {
                current_legacy_revision
                    .map(|revision| revision.cmp(&table.persisted_files_revision()))
            }),
            writer_order: source.and_then(|table| {
                native.map(|current| current.writer_epoch.cmp(&table.descriptor().writer_epoch))
            }),
            metadata_changed: match (source, projection, target_source_hash.as_deref()) {
                (Some(source), Some(current), Some(target_hash)) => {
                    current.writer_epoch != source.descriptor().writer_epoch
                        || current.source_metadata_hash.as_deref() != Some(target_hash)
                        || current.sources != source.sources()
                }
                _ => false,
            },
            strict_unprojected_v1: strict_v1_revision.is_some(),
        };
        let transition = plan_table_transition(state).map_err(ShadowFailure::Unsupported)?;

        if let (Some(source), Some(native)) = (source, native) {
            validate_source_and_native_identity(source, native, projection)?;
        }
        let verified = match source {
            Some(source) => Some(self.verify_projection_table(source).await?),
            None => None,
        };

        match transition {
            PlannedTableTransition::None => {}
            PlannedTableTransition::Bootstrap => {
                let source = source.expect("bootstrap requires source state");
                let verified = verified.as_ref().expect("source state was verified");
                let command = LegacyCatalogProjectionCommandV4::BootstrapTable(
                    LegacyCatalogBootstrapTableCommandV4 {
                        table_id,
                        namespace_id: source.descriptor().namespace_id,
                        name: source.descriptor().name.clone(),
                        definition: source.descriptor().definition.clone(),
                        schema_ipc: source.schema_ipc().clone(),
                        canonical_schema_hash: source.canonical_schema_hash().to_owned(),
                        config: bootstrap_config(snapshot, source)?,
                        writer_epoch: source.descriptor().writer_epoch,
                        sources: source.sources().to_vec(),
                        files: verified.files.clone(),
                        legacy_files_revision: source.persisted_files_revision(),
                    },
                );
                self.submit_v4(snapshot, checkpoint.shadow_epoch, command, true)
                    .await?;
                self.record_observed_command(ObservedShadowCommand::BootstrapV4);
            }
            PlannedTableTransition::Adopt => {
                self.adopt_current(snapshot, checkpoint.shadow_epoch, source, verified.as_ref())
                    .await?;
                self.record_observed_command(ObservedShadowCommand::AdoptV4);
            }
            PlannedTableTransition::AdoptThenSync => {
                self.adopt_current(snapshot, checkpoint.shadow_epoch, source, verified.as_ref())
                    .await?;
                self.record_observed_command(ObservedShadowCommand::AdoptV4);
                self.sync_current(snapshot, checkpoint.shadow_epoch, source)
                    .await?;
                self.record_observed_command(ObservedShadowCommand::SyncMetadataV4);
            }
            PlannedTableTransition::AdvanceFilesThenAdopt => {
                self.advance_files_current(
                    snapshot,
                    source,
                    verified.as_ref(),
                    current_legacy_revision,
                )
                .await?;
                self.record_observed_command(ObservedShadowCommand::ReconcileV3);
                self.adopt_current(snapshot, checkpoint.shadow_epoch, source, verified.as_ref())
                    .await?;
                self.record_observed_command(ObservedShadowCommand::AdoptV4);
            }
            PlannedTableTransition::Sync => {
                self.sync_current(snapshot, checkpoint.shadow_epoch, source)
                    .await?;
                self.record_observed_command(ObservedShadowCommand::SyncMetadataV4);
            }
            PlannedTableTransition::AdvanceFiles => {
                self.advance_files_current(
                    snapshot,
                    source,
                    verified.as_ref(),
                    current_legacy_revision,
                )
                .await?;
                self.record_observed_command(ObservedShadowCommand::ReconcileV3);
            }
            PlannedTableTransition::SyncThenAdvanceFiles => {
                self.sync_current(snapshot, checkpoint.shadow_epoch, source)
                    .await?;
                self.record_observed_command(ObservedShadowCommand::SyncMetadataV4);
                self.advance_files_current(
                    snapshot,
                    source,
                    verified.as_ref(),
                    current_legacy_revision,
                )
                .await?;
                self.record_observed_command(ObservedShadowCommand::ReconcileV3);
            }
            PlannedTableTransition::DropComplete
            | PlannedTableTransition::DropSeeded
            | PlannedTableTransition::DropUnprojectedV1 => {
                let native = native.expect("drop requires an active native table");
                let (
                    projection_revision,
                    schema_version,
                    schema_hash,
                    snapshot_version,
                    legacy_files_revision,
                    writer_epoch,
                    source_hash,
                ) = match projection {
                    Some(current) => (
                        current.projection_revision,
                        current.schema_version,
                        current.schema_hash.clone(),
                        current.native_snapshot_version,
                        current.legacy_files_revision,
                        current.writer_epoch,
                        current.source_metadata_hash.clone(),
                    ),
                    None => (
                        0,
                        native.current_schema_version,
                        self.read_native_schema_hash(native).await?,
                        native.current_snapshot_version,
                        strict_v1_revision.expect("unprojected drop requires V1 base"),
                        native.writer_epoch,
                        None,
                    ),
                };
                let command =
                    LegacyCatalogProjectionCommandV4::DropTable(LegacyCatalogDropTableCommandV4 {
                        table_id,
                        expected_projection_revision: projection_revision,
                        expected_schema_version: schema_version,
                        expected_schema_hash: schema_hash,
                        expected_snapshot_version: snapshot_version,
                        expected_legacy_files_revision: legacy_files_revision,
                        expected_writer_epoch: writer_epoch,
                        expected_source_metadata_hash: source_hash,
                    });
                self.submit_v4(snapshot, checkpoint.shadow_epoch, command, false)
                    .await?;
                self.record_observed_command(ObservedShadowCommand::DropV4);
                self.pending_explicit_drops.remove(&table_id);
                return Ok(0);
            }
        }

        let Some(source) = source else {
            return Ok(0);
        };
        let verified = verified.as_ref().expect("source state was verified");
        let current_native = self.read_active_table(table_id).await?;
        let current_projection =
            self.read_projection(table_id)
                .await?
                .ok_or(ShadowFailure::Unsupported(
                    "legacy_projection_missing_after_reconcile",
                ))?;
        validate_source_and_native_identity(source, &current_native, Some(&current_projection))?;
        if current_projection.completeness != LegacyCatalogProjectionCompletenessV4::Complete
            || current_projection.state != LegacyCatalogProjectionTableStateV4::Active
            || current_projection.legacy_files_revision != source.persisted_files_revision()
            || current_projection.writer_epoch != source.descriptor().writer_epoch
            || current_projection.sources != source.sources()
            || current_projection.source_metadata_hash.as_deref() != target_source_hash.as_deref()
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_projection_did_not_converge_to_source",
            ));
        }
        let mapping = self.read_current_mapping(table_id).await?;
        self.exact_diff(source, &mapping, verified).await?;
        Ok(1)
    }

    async fn verify_projection_table(
        &mut self,
        source: &LegacyCatalogActivePublicTableV4,
    ) -> Result<VerifiedLegacyCatalogTableView, ShadowFailure> {
        self.validate_metadata_budget(source)?;
        let table_id = source.descriptor().table_id;
        let retained = self.verified.remove(&table_id);
        // The verifier owns non-cancellable blocking checksum work. Keep ownership until it
        // finishes so retries cannot accumulate detached scans of the same immutable files.
        let verification = verify_legacy_catalog_projection_table_view(
            source,
            self.trusted_fallback_root.as_deref(),
            retained.as_ref(),
        )
        .await;
        match verification {
            Ok(verified) => {
                self.verified.insert(table_id, verified.cache.clone());
                Ok(verified)
            }
            Err(error) => {
                if let Some(retained) = retained {
                    self.verified.insert(table_id, retained);
                }
                if error.retryable() {
                    Err(ShadowFailure::Recoverable(
                        "legacy_physical_verification_failed",
                    ))
                } else {
                    Err(ShadowFailure::PermanentCatalog(
                        "legacy_physical_verification",
                        error,
                    ))
                }
            }
        }
    }

    async fn adopt_current(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        shadow_epoch: u64,
        source: Option<&LegacyCatalogActivePublicTableV4>,
        verified: Option<&VerifiedLegacyCatalogTableView>,
    ) -> Result<(), ShadowFailure> {
        let source = source.expect("adopt requires source state");
        let verified = verified.expect("adopt requires physical verification");
        let native = self.read_active_table(source.descriptor().table_id).await?;
        let projection = self.read_projection(native.table_id).await?;
        let legacy_revision = projection
            .as_ref()
            .map(|value| value.legacy_files_revision)
            .or_else(|| strict_unprojected_v1_revision(&native))
            .ok_or(ShadowFailure::Unsupported(
                "legacy_adopt_cursor_is_not_seeded_or_empty_v1",
            ))?;
        let command = LegacyCatalogProjectionCommandV4::AdoptExistingTable(
            LegacyCatalogAdoptExistingTableCommandV4 {
                table_id: native.table_id,
                namespace_id: native.namespace_id,
                name: native.name.clone(),
                definition: native.definition.clone(),
                expected_schema_version: native.current_schema_version,
                expected_schema_hash: verified.canonical_schema_hash.clone(),
                config: native.config.clone(),
                expected_snapshot_version: native.current_snapshot_version,
                expected_legacy_files_revision: legacy_revision,
                expected_writer_epoch: native.writer_epoch,
                sources: source.sources().to_vec(),
                files: verified.files.clone(),
            },
        );
        self.submit_v4(snapshot, shadow_epoch, command, false)
            .await?;
        Ok(())
    }

    async fn sync_current(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        shadow_epoch: u64,
        source: Option<&LegacyCatalogActivePublicTableV4>,
    ) -> Result<(), ShadowFailure> {
        let source = source.expect("sync requires source state");
        let native = self.read_active_table(source.descriptor().table_id).await?;
        let projection =
            self.read_projection(native.table_id)
                .await?
                .ok_or(ShadowFailure::Unsupported(
                    "legacy_sync_projection_is_missing",
                ))?;
        let source_hash =
            projection
                .source_metadata_hash
                .clone()
                .ok_or(ShadowFailure::Unsupported(
                    "legacy_sync_projection_is_not_complete",
                ))?;
        let command = LegacyCatalogProjectionCommandV4::SyncTableMetadata(
            LegacyCatalogSyncTableMetadataCommandV4 {
                table_id: native.table_id,
                expected_projection_revision: projection.projection_revision,
                expected_schema_version: projection.schema_version,
                expected_schema_hash: projection.schema_hash,
                expected_snapshot_version: projection.native_snapshot_version,
                expected_legacy_files_revision: projection.legacy_files_revision,
                expected_writer_epoch: projection.writer_epoch,
                expected_source_metadata_hash: source_hash,
                writer_epoch: source.descriptor().writer_epoch,
                sources: source.sources().to_vec(),
            },
        );
        self.submit_v4(snapshot, shadow_epoch, command, false)
            .await?;
        Ok(())
    }

    async fn advance_files_current(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        source: Option<&LegacyCatalogActivePublicTableV4>,
        verified: Option<&VerifiedLegacyCatalogTableView>,
        expected_legacy_revision: Option<u64>,
    ) -> Result<(), ShadowFailure> {
        let source = source.expect("V3 reconcile requires source state");
        let verified = verified.expect("V3 reconcile requires physical verification");
        let native = self.read_active_table(source.descriptor().table_id).await?;
        let expected_legacy_revision = expected_legacy_revision
            .or_else(|| strict_unprojected_v1_revision(&native))
            .ok_or(ShadowFailure::Unsupported(
                "legacy_reconcile_current_file_revision_is_missing",
            ))?;
        let command = LegacyCatalogReconcileCommandV3 {
            table_id: native.table_id,
            expected_schema_version: native.current_schema_version,
            expected_schema_hash: verified.canonical_schema_hash.clone(),
            expected_writer_epoch: native.writer_epoch,
            writer_epoch: source.descriptor().writer_epoch,
            expected_legacy_files_revision: expected_legacy_revision,
            legacy_files_revision: source.persisted_files_revision(),
            files: verified.files.clone(),
        };
        let envelope = build_v3_envelope(snapshot, command)?;
        let request_id = envelope.request_id;
        let first = tokio::time::timeout(
            self.operation_timeout,
            self.catalog_reconcile
                .reconcile_legacy_table_v3(envelope.clone()),
        )
        .await;
        let response = match first {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                return Err(ShadowFailure::Catalog("catalog_reconcile_v3", error));
            }
            Err(_) => match tokio::time::timeout(
                self.operation_timeout,
                self.catalog_reconcile.reconcile_legacy_table_v3(envelope),
            )
            .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    return Err(ShadowFailure::Catalog("catalog_reconcile_v3_replay", error));
                }
                Err(_) => return Err(ShadowFailure::Timeout("catalog_reconcile_v3_replay")),
            },
        };
        if response.request_id != request_id
            || response.table_id != native.table_id
            || response.previous_legacy_files_revision != expected_legacy_revision
            || response.legacy_files_revision != source.persisted_files_revision()
        {
            return Err(ShadowFailure::Catalog(
                "catalog_reconcile_response_inconsistent",
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "legacy reconcile response does not match its request",
                )
                .with_request_id(request_id)
                .with_table_id(native.table_id),
            ));
        }
        let current = self.read_current_mapping(native.table_id).await?;
        if current.legacy_files_revision != source.persisted_files_revision() {
            return Err(ShadowFailure::Recoverable(
                "legacy_reconcile_mapping_did_not_advance",
            ));
        }
        Ok(())
    }

    async fn submit_v4(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        shadow_epoch: u64,
        command: LegacyCatalogProjectionCommandV4,
        creation: bool,
    ) -> Result<LegacyCatalogProjectionResponseV4, ShadowFailure> {
        let expected_outcome = ProjectionOutcomeExpectation::from_command(&command);
        let envelope = build_v4_envelope(snapshot, shadow_epoch, command)?;
        let request_id = envelope.request_id;
        let result = tokio::time::timeout(
            self.operation_timeout,
            self.catalog_reconcile
                .apply_legacy_catalog_projection_v4(envelope.clone()),
        )
        .await;
        let result = match result {
            Err(_) => tokio::time::timeout(
                self.operation_timeout,
                self.catalog_reconcile
                    .apply_legacy_catalog_projection_v4(envelope),
            )
            .await
            .map_err(|_| ShadowFailure::Timeout("catalog_projection_v4_replay"))?,
            Ok(result) => result,
        };
        let response = match result {
            Ok(response) => response,
            Err(error) if creation && error.code() == ErrorCode::AlreadyExists => {
                return Err(ShadowFailure::Unsupported(
                    "legacy_table_identity_is_occupied_or_recreated",
                ));
            }
            Err(error) => return Err(ShadowFailure::Catalog("catalog_projection_v4", error)),
        };
        if response.request_id != request_id || !expected_outcome.matches(&response.outcome) {
            return Err(ShadowFailure::Catalog(
                "catalog_projection_response_inconsistent",
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "legacy V4 projection response does not match its request",
                )
                .with_request_id(request_id),
            ));
        }
        Ok(response)
    }

    async fn read_active_table(&self, table_id: TableId) -> Result<TableDescriptor, ShadowFailure> {
        self.catalog_timeout(
            "catalog_get_legacy_table",
            self.catalog_read.get_table(TableRef::Id { table_id }),
        )
        .await
    }

    async fn read_projection(
        &self,
        table_id: TableId,
    ) -> Result<Option<LegacyCatalogTableProjectionV4>, ShadowFailure> {
        self.catalog_timeout(
            "catalog_get_projection_v4",
            self.catalog_read.read_legacy_table_projection_v4(table_id),
        )
        .await
    }

    async fn read_current_mapping(
        &self,
        table_id: TableId,
    ) -> Result<LegacyFileViewSnapshotMapping, ShadowFailure> {
        self.catalog_timeout(
            "catalog_resolve_current_mapping",
            self.catalog_read
                .resolve_current_legacy_file_view_mapping(table_id),
        )
        .await
    }

    async fn read_native_schema_hash(
        &self,
        native: &TableDescriptor,
    ) -> Result<String, ShadowFailure> {
        let selector = CatalogSnapshotSelector {
            table_id: native.table_id,
            snapshot_version: native.current_snapshot_version,
            schema_version: native.current_schema_version,
        };
        let snapshot = self
            .catalog_timeout(
                "catalog_read_legacy_schema",
                self.catalog_read.read_snapshot_at_with_limits(
                    std::slice::from_ref(&selector),
                    self.read_limits,
                ),
            )
            .await?;
        snapshot
            .tables()
            .first()
            .filter(|table| snapshot.tables().len() == 1 && table.selector() == &selector)
            .map(|table| table.schema().schema_hash.clone())
            .ok_or(ShadowFailure::Unsupported(
                "legacy_native_schema_snapshot_is_missing",
            ))
    }

    async fn exact_diff(
        &self,
        source: &LegacyCatalogActivePublicTableV4,
        mapping: &LegacyFileViewSnapshotMapping,
        verified: &VerifiedLegacyCatalogTableView,
    ) -> Result<(), ShadowFailure> {
        let selector = CatalogSnapshotSelector {
            table_id: source.descriptor().table_id,
            snapshot_version: mapping.native_snapshot_version,
            schema_version: MIGRATED_SCHEMA_VERSION,
        };
        let snapshot = self
            .catalog_timeout(
                "catalog_read_exact_shadow_snapshot",
                self.catalog_read.read_snapshot_at_with_limits(
                    std::slice::from_ref(&selector),
                    self.read_limits,
                ),
            )
            .await?;
        let Some(table) = snapshot.tables().first() else {
            return Err(ShadowFailure::Catalog(
                "catalog_exact_snapshot_missing",
                ServerError::new(ErrorCode::DataCorruption, "exact native snapshot is empty"),
            ));
        };
        if snapshot.tables().len() != 1
            || table.selector() != &selector
            || table.schema().schema_hash != verified.canonical_schema_hash
            || table.table().writer_epoch != source.descriptor().writer_epoch
            || table.table().current_snapshot_version != mapping.native_snapshot_version
            || mapping.legacy_files_revision != source.persisted_files_revision()
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_native_table_or_schema_diff",
            ));
        }
        let mut actual = table.files().as_slice().iter().collect::<Vec<_>>();
        actual.sort_unstable_by_key(|file| (file.legacy_discovery_order(), file.file_id()));
        let exact = actual.len() == verified.files.len()
            && actual
                .iter()
                .zip(&verified.files)
                .all(|(actual, expected)| {
                    actual.legacy_discovery_order() == Some(expected.discovery_order)
                        && actual.file == expected.file
                });
        if !exact {
            return Err(ShadowFailure::Recoverable(
                "legacy_native_exact_manifest_diff",
            ));
        }
        Ok(())
    }

    fn validate_metadata_budget(
        &self,
        table: &LegacyCatalogActivePublicTableV4,
    ) -> Result<(), ShadowFailure> {
        if table.migration_files().len() > self.max_files_per_table {
            return Err(ShadowFailure::Recoverable(
                "legacy_manifest_file_limit_exceeded",
            ));
        }
        let mut bytes = 0_usize;
        for file in table.migration_files() {
            let encoded = serde_json::to_vec(file.canonical_value()).map_err(|_| {
                ShadowFailure::Recoverable("legacy_manifest_metadata_encoding_failed")
            })?;
            bytes = bytes
                .checked_add(encoded.len())
                .ok_or(ShadowFailure::Recoverable(
                    "legacy_manifest_metadata_size_overflow",
                ))?;
            if bytes > self.max_metadata_bytes_per_table {
                return Err(ShadowFailure::Recoverable(
                    "legacy_manifest_metadata_limit_exceeded",
                ));
            }
        }
        Ok(())
    }

    async fn advance_checkpoint_if_needed(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        current: &LegacyCatalogProjectionCheckpointV4,
        effective_loss_epoch: u64,
    ) -> Result<LegacyCatalogProjectionCheckpointV4, ShadowFailure> {
        let target_phase = match current.phase {
            LegacyCatalogProjectionPhaseV4::NativeAuthoritative => {
                return Err(ShadowFailure::Unsupported(
                    "native_authoritative_checkpoint_forbids_legacy_shadow",
                ));
            }
            LegacyCatalogProjectionPhaseV4::ReadCutoverReady => {
                LegacyCatalogProjectionPhaseV4::ReadCutoverReady
            }
            LegacyCatalogProjectionPhaseV4::FoundationOnly
            | LegacyCatalogProjectionPhaseV4::FileShadow
            | LegacyCatalogProjectionPhaseV4::FullShadow => {
                LegacyCatalogProjectionPhaseV4::FullShadow
            }
        };
        if current.source_catalog_revision == snapshot.catalog_revision()
            && current.source_projection_hash == snapshot.global_projection_digest()
            && current.projection_digest == snapshot.global_projection_digest()
            && current.phase == target_phase
            && current.mode == LegacyCatalogCheckpointModeV4::FullReconcile
            && current.resolved_continuity_loss_epoch == effective_loss_epoch
        {
            return Ok(current.clone());
        }
        let target_shadow_epoch =
            current
                .shadow_epoch
                .checked_add(1)
                .ok_or(ShadowFailure::Unsupported(
                    "legacy_projection_shadow_epoch_overflow",
                ))?;
        let command = LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
            LegacyCatalogAdvanceCheckpointCommandV4 {
                expected_checkpoint_generation: current.checkpoint_generation,
                expected_source_catalog_revision: current.source_catalog_revision,
                expected_source_projection_hash: current.source_projection_hash.clone(),
                expected_phase: current.phase,
                shadow_epoch: target_shadow_epoch,
                phase: target_phase,
                mode: LegacyCatalogCheckpointModeV4::FullReconcile,
                target_projection_digest: snapshot.global_projection_digest().to_owned(),
                observed_continuity_loss_epoch: effective_loss_epoch,
            },
        );
        let response = self
            .submit_v4(snapshot, current.shadow_epoch, command, false)
            .await?;
        let LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(committed) = response.outcome
        else {
            return Err(ShadowFailure::Unsupported(
                "legacy_projection_checkpoint_response_kind_mismatch",
            ));
        };
        self.record_observed_command(ObservedShadowCommand::CheckpointV4);
        let reread = self.read_checkpoint().await?;
        if reread != committed {
            return Err(ShadowFailure::Recoverable(
                "legacy_projection_checkpoint_changed_after_commit",
            ));
        }
        Ok(reread)
    }

    async fn verify_durable_projection(
        &self,
        snapshot: &LegacyCatalogProjectionSnapshotV4,
        expected_checkpoint: &LegacyCatalogProjectionCheckpointV4,
    ) -> Result<LegacyCatalogProjectionCheckpointV4, ShadowFailure> {
        let checkpoint = self.read_checkpoint().await?;
        if &checkpoint != expected_checkpoint
            || checkpoint.source_catalog_revision != snapshot.catalog_revision()
            || checkpoint.source_projection_hash != snapshot.global_projection_digest()
            || checkpoint.projection_digest != snapshot.global_projection_digest()
            || checkpoint.mode != LegacyCatalogCheckpointModeV4::FullReconcile
            || !matches!(
                checkpoint.phase,
                LegacyCatalogProjectionPhaseV4::FullShadow
                    | LegacyCatalogProjectionPhaseV4::ReadCutoverReady
            )
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_projection_checkpoint_evidence_mismatch",
            ));
        }

        let native = self.list_migrated_tables().await?;
        let projections = self.list_projections().await?;
        let source_ids = snapshot
            .tables()
            .iter()
            .map(|table| table.descriptor().table_id)
            .collect::<BTreeSet<_>>();
        let native_ids = native.keys().copied().collect::<BTreeSet<_>>();
        let active_projection_ids = projections
            .values()
            .filter(|projection| projection.state == LegacyCatalogProjectionTableStateV4::Active)
            .map(|projection| projection.table_id)
            .collect::<BTreeSet<_>>();
        if source_ids != native_ids || source_ids != active_projection_ids {
            return Err(ShadowFailure::Unsupported(
                "legacy_projection_active_table_sets_diverged",
            ));
        }

        let mut digest_inputs = Vec::with_capacity(snapshot.tables().len());
        for source in snapshot.tables() {
            let table_id = source.descriptor().table_id;
            let native = native.get(&table_id).ok_or(ShadowFailure::Unsupported(
                "legacy_projection_native_table_missing_after_checkpoint",
            ))?;
            let projection = projections
                .get(&table_id)
                .ok_or(ShadowFailure::Unsupported(
                    "legacy_projection_row_missing_after_checkpoint",
                ))?;
            validate_source_and_native_identity(source, native, Some(projection))?;
            let expected_source_hash =
                compute_legacy_catalog_source_metadata_hash_v4(source.sources()).map_err(|_| {
                    ShadowFailure::Unsupported("legacy_source_metadata_hash_is_invalid")
                })?;
            if projection.completeness != LegacyCatalogProjectionCompletenessV4::Complete
                || projection.schema_hash != source.canonical_schema_hash()
                || projection.legacy_files_revision != source.persisted_files_revision()
                || projection.writer_epoch != source.descriptor().writer_epoch
                || projection.sources != source.sources()
                || projection.source_metadata_hash.as_deref() != Some(expected_source_hash.as_str())
            {
                return Err(ShadowFailure::Recoverable(
                    "legacy_projection_table_evidence_mismatch",
                ));
            }
            digest_inputs.push(LegacyCatalogProjectionDigestTableV4 {
                table_id,
                namespace_id: native.namespace_id,
                name: native.name.clone(),
                definition: native.definition.clone(),
                schema_version: projection.schema_version,
                schema_hash: projection.schema_hash.clone(),
                config: source_owned_config(&native.config),
                legacy_files_revision: projection.legacy_files_revision,
                writer_epoch: projection.writer_epoch,
                state: LegacyCatalogProjectionTableStateV4::Active,
                sources: projection.sources.clone(),
            });
        }
        let digest = compute_legacy_catalog_projection_digest_v4(&digest_inputs).map_err(|_| {
            ShadowFailure::Unsupported("legacy_native_projection_digest_is_invalid")
        })?;
        if digest != snapshot.global_projection_digest() || digest != checkpoint.projection_digest {
            return Err(ShadowFailure::Recoverable(
                "legacy_native_projection_digest_mismatch",
            ));
        }
        Ok(checkpoint)
    }

    async fn catalog_timeout<T>(
        &self,
        operation: &'static str,
        future: impl std::future::Future<Output = Result<T, ServerError>>,
    ) -> Result<T, ShadowFailure> {
        match tokio::time::timeout(self.operation_timeout, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(ShadowFailure::Catalog(operation, error)),
            Err(_) => Err(ShadowFailure::Timeout(operation)),
        }
    }

    fn establish_loss_epoch_base(
        &mut self,
        checkpoint: &LegacyCatalogProjectionCheckpointV4,
        worker_local_loss_epoch: u64,
    ) -> Result<(), ShadowFailure> {
        match self.loss_epoch_base {
            Some(base) => {
                let maximum =
                    base.checked_add(worker_local_loss_epoch)
                        .ok_or(ShadowFailure::Unsupported(
                            "projection_checkpoint_loss_epoch_overflow",
                        ))?;
                if (base..=maximum).contains(&checkpoint.resolved_continuity_loss_epoch) {
                    Ok(())
                } else {
                    Err(ShadowFailure::Unsupported(
                        "projection_checkpoint_loss_epoch_changed_outside_worker",
                    ))
                }
            }
            None => {
                self.loss_epoch_base = Some(checkpoint.resolved_continuity_loss_epoch);
                Ok(())
            }
        }
    }

    fn effective_loss_epoch(&self, worker_local_loss_epoch: u64) -> Result<u64, ShadowFailure> {
        self.loss_epoch_base
            .ok_or(ShadowFailure::Recoverable(
                "projection_checkpoint_loss_epoch_base_uninitialized",
            ))?
            .checked_add(worker_local_loss_epoch)
            .ok_or(ShadowFailure::Unsupported(
                "projection_checkpoint_loss_epoch_overflow",
            ))
    }

    fn record_observed_command(&self, command: ObservedShadowCommand) {
        let mut status = self.lock_status();
        match command {
            ObservedShadowCommand::ReconcileV3 => {
                status.v3_reconciles_total = status.v3_reconciles_total.saturating_add(1);
            }
            ObservedShadowCommand::BootstrapV4 => {
                status.v4_bootstrap_total = status.v4_bootstrap_total.saturating_add(1);
            }
            ObservedShadowCommand::AdoptV4 => {
                status.v4_adopt_total = status.v4_adopt_total.saturating_add(1);
            }
            ObservedShadowCommand::SyncMetadataV4 => {
                status.v4_metadata_sync_total = status.v4_metadata_sync_total.saturating_add(1);
            }
            ObservedShadowCommand::DropV4 => {
                status.v4_drop_total = status.v4_drop_total.saturating_add(1);
            }
            ObservedShadowCommand::CheckpointV4 => {
                status.v4_checkpoint_total = status.v4_checkpoint_total.saturating_add(1);
            }
        }
    }

    fn record_failure(&mut self, failure: &ShadowFailure) {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match failure {
            ShadowFailure::Recoverable(reason) => {
                status.state = LegacyCatalogShadowState::BlockedRecoverable;
                status.last_error = Some((*reason).to_owned());
            }
            ShadowFailure::Unsupported(reason) => {
                status.state = LegacyCatalogShadowState::BlockedUnsupported;
                status.unsupported_mutations_total =
                    status.unsupported_mutations_total.saturating_add(1);
                status.last_error = Some((*reason).to_owned());
            }
            ShadowFailure::Timeout(operation) => {
                status.state = LegacyCatalogShadowState::BlockedRecoverable;
                status.timeouts_total = status.timeouts_total.saturating_add(1);
                status.last_error = Some(format!("{operation}_timeout"));
            }
            ShadowFailure::Catalog(operation, error) => {
                status.state = LegacyCatalogShadowState::BlockedRecoverable;
                status.catalog_errors_total = status.catalog_errors_total.saturating_add(1);
                status.last_error = Some(format!("{operation}_code_{}", error.code()));
            }
            ShadowFailure::PermanentCatalog(operation, error) => {
                status.state = LegacyCatalogShadowState::BlockedUnsupported;
                status.catalog_errors_total = status.catalog_errors_total.saturating_add(1);
                status.unsupported_mutations_total =
                    status.unsupported_mutations_total.saturating_add(1);
                status.last_error = Some(format!("{operation}_code_{}", error.code()));
            }
        }
    }

    fn lock_status(&self) -> std::sync::MutexGuard<'_, LegacyCatalogShadowStatus> {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn run_worker(
    receiver: Receiver<LegacyCatalogCommitEvent>,
    mut engine: ShadowEngine,
    initial_attempt: mpsc::SyncSender<Result<(), ServerError>>,
    #[cfg(test)] test_fault: LegacyCatalogShadowTestFault,
) -> Result<(), ServerError> {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = ServerError::new(
                ErrorCode::Unavailable,
                format!("build legacy Catalog shadow runtime error=[{error}]"),
            );
            let _ = initial_attempt.send(Err(error.clone()));
            return Err(error);
        }
    };
    #[cfg(test)]
    match test_fault {
        LegacyCatalogShadowTestFault::ExitBeforeInitialAttempt => {
            let error = ServerError::new(
                ErrorCode::Unavailable,
                "legacy Catalog shadow worker exited before its initial reconcile attempt",
            );
            let _ = initial_attempt.send(Err(error.clone()));
            return Err(error);
        }
        LegacyCatalogShadowTestFault::DelayBeforeInitialAttempt(delay) => thread::sleep(delay),
        LegacyCatalogShadowTestFault::None
        | LegacyCatalogShadowTestFault::InitialRecoverableOnce
        | LegacyCatalogShadowTestFault::DisconnectBeforeFinalSeal => {}
    }
    #[cfg(test)]
    if test_fault == LegacyCatalogShadowTestFault::InitialRecoverableOnce {
        {
            let mut status = engine.lock_status();
            status.full_reconciles_total = status.full_reconciles_total.saturating_add(1);
            status.state = LegacyCatalogShadowState::CatchingUp;
        }
        engine.record_failure(&ShadowFailure::Recoverable(
            "injected_initial_reconcile_failure",
        ));
    } else {
        runtime.block_on(engine.reconcile_and_record_failure());
    }
    #[cfg(not(test))]
    runtime.block_on(engine.reconcile_and_record_failure());
    initial_attempt.send(Ok(())).map_err(|_| {
        ServerError::new(
            ErrorCode::Unavailable,
            "legacy Catalog shadow initial reconcile observer disconnected",
        )
    })?;
    #[cfg(test)]
    if test_fault == LegacyCatalogShadowTestFault::DisconnectBeforeFinalSeal {
        return finish_worker(&runtime, &mut engine);
    }
    let mut last_retry = Instant::now();
    loop {
        match receiver.recv_timeout(IDLE_POLL_INTERVAL) {
            Ok(first) => {
                let mut events = vec![first];
                let disconnected = drain_events(&receiver, &mut events);
                if engine.apply_events(&events) {
                    runtime.block_on(engine.reconcile_and_record_failure());
                }
                last_retry = Instant::now();
                if disconnected {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let snapshot = engine.metrics.snapshot();
                let current = engine
                    .status
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let unresolved_loss = continuity_loss_requires_reconcile(
                    engine.loss_epoch_base,
                    snapshot.continuity_loss_epoch,
                    current.recovered_continuity_loss_epoch,
                );
                let retryable = current.state == LegacyCatalogShadowState::BlockedRecoverable
                    || unresolved_loss
                    || snapshot.latest_observed_revision > current.consumed_catalog_revision;
                if retryable && last_retry.elapsed() >= RECOVERABLE_RETRY_INTERVAL {
                    runtime.block_on(engine.reconcile_and_record_failure());
                    last_retry = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    finish_worker(&runtime, &mut engine)
}

fn continuity_loss_requires_reconcile(
    durable_epoch_base: Option<u64>,
    worker_local_loss_epoch: u64,
    recovered_loss_epoch: u64,
) -> bool {
    durable_epoch_base
        .and_then(|base| base.checked_add(worker_local_loss_epoch))
        .is_none_or(|effective| effective > recovered_loss_epoch)
}

fn finish_worker(
    runtime: &tokio::runtime::Runtime,
    engine: &mut ShadowEngine,
) -> Result<(), ServerError> {
    let publisher = engine.metrics.snapshot();
    if !publisher.sealed {
        let failure =
            ShadowFailure::Recoverable("legacy_catalog_shadow_publisher_unsealed_at_worker_exit");
        engine.record_failure(&failure);
        engine.lock_status().publisher = publisher.into();
        return Err(failure.final_server_error());
    }
    match runtime.block_on(engine.full_reconcile()) {
        Ok(_) => {
            let mut final_status = engine.lock_status();
            final_status.publisher = engine.metrics.snapshot().into();
            final_status.state = LegacyCatalogShadowState::Closed;
            Ok(())
        }
        Err(failure) => {
            engine.record_failure(&failure);
            engine.lock_status().publisher = engine.metrics.snapshot().into();
            Err(failure.final_server_error())
        }
    }
}

fn drain_events(
    receiver: &Receiver<LegacyCatalogCommitEvent>,
    events: &mut Vec<LegacyCatalogCommitEvent>,
) -> bool {
    loop {
        match receiver.try_recv() {
            Ok(event) => events.push(event),
            Err(TryRecvError::Empty) => return false,
            Err(TryRecvError::Disconnected) => return true,
        }
    }
}

fn observe_generation_mutation(
    pending_explicit_drops: &mut BTreeSet<TableId>,
    mutation_kind: LegacyCatalogCommitMutationKind,
    table_id: Option<TableId>,
) -> bool {
    let Some(table_id) = table_id else {
        return false;
    };
    match mutation_kind {
        LegacyCatalogCommitMutationKind::DropTable => {
            pending_explicit_drops.insert(table_id);
            false
        }
        LegacyCatalogCommitMutationKind::RegisterStream => {
            pending_explicit_drops.contains(&table_id)
        }
        LegacyCatalogCommitMutationKind::RegisterSource
        | LegacyCatalogCommitMutationKind::UnregisterSource
        | LegacyCatalogCommitMutationKind::PublishPersistedFile
        | LegacyCatalogCommitMutationKind::ReplacePersistedFiles
        | LegacyCatalogCommitMutationKind::Compaction
        | LegacyCatalogCommitMutationKind::PersistEventWatermark => false,
    }
}

fn same_projection_snapshot(
    left: &LegacyCatalogProjectionSnapshotV4,
    right: &LegacyCatalogProjectionSnapshotV4,
) -> bool {
    left.catalog_revision() == right.catalog_revision()
        && left.global_projection_digest() == right.global_projection_digest()
        && left.namespace() == right.namespace()
        && left.dropped_tables() == right.dropped_tables()
        && left.tables().len() == right.tables().len()
        && left
            .tables()
            .iter()
            .zip(right.tables())
            .all(|(left, right)| {
                left.descriptor() == right.descriptor()
                    && left.legacy_schema() == right.legacy_schema()
                    && left.legacy_schema_hash() == right.legacy_schema_hash()
                    && left.canonical_schema_hash() == right.canonical_schema_hash()
                    && left.schema_has_custom_metadata() == right.schema_has_custom_metadata()
                    && left.persisted_files_revision() == right.persisted_files_revision()
                    && left.migration_files() == right.migration_files()
                    && left.sources() == right.sources()
            })
}

fn source_owned_config(config: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    config
        .iter()
        .filter(|(key, _)| !key.starts_with("zippy.migration."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn bootstrap_config(
    snapshot: &LegacyCatalogProjectionSnapshotV4,
    source: &LegacyCatalogActivePublicTableV4,
) -> Result<BTreeMap<String, String>, ShadowFailure> {
    let mut config = source.descriptor().config.clone();
    let has_files = !source.migration_files().is_empty();
    let native_snapshot_base = if has_files {
        LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE
    } else {
        LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE
    };
    let migration_version = if has_files {
        LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION
    } else {
        LEGACY_CATALOG_METADATA_MIGRATION_VERSION
    };
    for (key, value) in [
        (
            LEGACY_STREAM_NAME_CONFIG_KEY,
            source.descriptor().name.as_str().to_owned(),
        ),
        (
            LEGACY_REVISION_BASE_CONFIG_KEY,
            source.persisted_files_revision().to_string(),
        ),
        (
            NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
            native_snapshot_base.to_string(),
        ),
        (
            LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY,
            snapshot.global_projection_digest().to_owned(),
        ),
        (
            LEGACY_MIGRATION_VERSION_CONFIG_KEY,
            migration_version.to_string(),
        ),
    ] {
        if config.insert(key.to_owned(), value).is_some() {
            return Err(ShadowFailure::Unsupported(
                "legacy_source_config_uses_reserved_migration_key",
            ));
        }
    }
    Ok(config)
}

fn strict_unprojected_v1_revision(table: &TableDescriptor) -> Option<u64> {
    if table.state != TableState::Active
        || table.current_schema_version != MIGRATED_SCHEMA_VERSION
        || table.current_snapshot_version != LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE
        || table
            .config
            .get(LEGACY_STREAM_NAME_CONFIG_KEY)
            .map(String::as_str)
            != Some(table.name.as_str())
        || table
            .config
            .get(NATIVE_SNAPSHOT_BASE_CONFIG_KEY)
            .map(String::as_str)
            != Some("1")
        || table
            .config
            .get(LEGACY_MIGRATION_VERSION_CONFIG_KEY)
            .map(String::as_str)
            != Some("1")
        || !table
            .config
            .get(LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY)
            .is_some_and(|value| is_canonical_sha256(value))
    {
        return None;
    }
    table
        .config
        .get(LEGACY_REVISION_BASE_CONFIG_KEY)
        .and_then(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|parsed| parsed.to_string() == *value)
        })
}

fn is_canonical_sha256(value: &str) -> bool {
    value.strip_prefix("sha256-v1:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn validate_source_and_native_identity(
    source: &LegacyCatalogActivePublicTableV4,
    native: &TableDescriptor,
    projection: Option<&LegacyCatalogTableProjectionV4>,
) -> Result<(), ShadowFailure> {
    let source_descriptor = source.descriptor();
    if source_descriptor.state != TableState::Active
        || source_descriptor.current_schema_version != MIGRATED_SCHEMA_VERSION
        || source_descriptor.current_snapshot_version != source.persisted_files_revision()
        || native.state != TableState::Active
        || native.table_id != source_descriptor.table_id
        || native.namespace_id != source_descriptor.namespace_id
        || native.name != source_descriptor.name
        || native.definition != source_descriptor.definition
        || native.current_schema_version != MIGRATED_SCHEMA_VERSION
        || source_owned_config(&native.config) != source_descriptor.config
    {
        return Err(ShadowFailure::Unsupported(
            "legacy_source_and_native_table_identity_diverged",
        ));
    }
    if let Some(projection) = projection {
        if projection.table_id != native.table_id
            || projection.schema_version != native.current_schema_version
            || projection.schema_hash != source.canonical_schema_hash()
            || projection.native_snapshot_version != native.current_snapshot_version
            || projection.writer_epoch != native.writer_epoch
            || (projection.state == LegacyCatalogProjectionTableStateV4::Dropped
                && native.state == TableState::Active)
        {
            return Err(ShadowFailure::Unsupported(
                "legacy_projection_cursor_or_identity_diverged",
            ));
        }
    }
    Ok(())
}

fn build_v3_envelope(
    snapshot: &LegacyCatalogProjectionSnapshotV4,
    command: LegacyCatalogReconcileCommandV3,
) -> Result<LegacyCatalogReconcileEnvelopeV3, ShadowFailure> {
    let actor = CatalogActor {
        kind: CatalogActorKind::Server,
        principal: RECONCILE_ACTOR_PRINCIPAL.to_owned(),
    };
    let placeholder = placeholder_request_id();
    let probe = LegacyCatalogReconcileEnvelopeV3::new(
        placeholder,
        actor.clone(),
        snapshot.global_projection_digest().to_owned(),
        snapshot.catalog_revision(),
        command.clone(),
    )
    .map_err(|_| ShadowFailure::Unsupported("legacy_v3_command_fingerprint_failed"))?;
    let request_id = request_id_from_fingerprint(REQUEST_ID_DOMAIN, probe.canonical_fingerprint);
    LegacyCatalogReconcileEnvelopeV3::new(
        request_id,
        actor,
        snapshot.global_projection_digest().to_owned(),
        snapshot.catalog_revision(),
        command,
    )
    .map_err(|_| ShadowFailure::Unsupported("legacy_v3_command_fingerprint_failed"))
}

fn build_v4_envelope(
    snapshot: &LegacyCatalogProjectionSnapshotV4,
    shadow_epoch: u64,
    command: LegacyCatalogProjectionCommandV4,
) -> Result<LegacyCatalogProjectionEnvelopeV4, ShadowFailure> {
    let kind = match command {
        LegacyCatalogProjectionCommandV4::BootstrapTable(_)
        | LegacyCatalogProjectionCommandV4::AdoptExistingTable(_) => CatalogActorKind::Migration,
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(_)
        | LegacyCatalogProjectionCommandV4::DropTable(_)
        | LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(_) => CatalogActorKind::Server,
    };
    let actor = CatalogActor {
        kind,
        principal: PROJECTION_ACTOR_PRINCIPAL.to_owned(),
    };
    let placeholder = placeholder_request_id();
    let probe = LegacyCatalogProjectionEnvelopeV4::new(
        placeholder,
        actor.clone(),
        shadow_epoch,
        snapshot.global_projection_digest().to_owned(),
        snapshot.catalog_revision(),
        command.clone(),
    )
    .map_err(|_| ShadowFailure::Unsupported("legacy_v4_command_fingerprint_failed"))?;
    let request_id =
        request_id_from_fingerprint(PROJECTION_REQUEST_ID_DOMAIN, probe.canonical_fingerprint);
    LegacyCatalogProjectionEnvelopeV4::new(
        request_id,
        actor,
        shadow_epoch,
        snapshot.global_projection_digest().to_owned(),
        snapshot.catalog_revision(),
        command,
    )
    .map_err(|_| ShadowFailure::Unsupported("legacy_v4_command_fingerprint_failed"))
}

fn placeholder_request_id() -> RequestId {
    RequestId::from_u128(1).expect("one is a nonzero request id")
}

fn request_id_from_fingerprint(domain: &[u8], fingerprint: CommandFingerprint) -> RequestId {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(fingerprint.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    RequestId::from_bytes(bytes).expect("nonzero deterministic request id")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionOutcomeExpectation {
    Bootstrap(TableId),
    Adopt(TableId),
    Sync(TableId),
    Drop(TableId),
    Checkpoint,
}

impl ProjectionOutcomeExpectation {
    fn from_command(command: &LegacyCatalogProjectionCommandV4) -> Self {
        match command {
            LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
                Self::Bootstrap(command.table_id)
            }
            LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
                Self::Adopt(command.table_id)
            }
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
                Self::Sync(command.table_id)
            }
            LegacyCatalogProjectionCommandV4::DropTable(command) => Self::Drop(command.table_id),
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(_) => Self::Checkpoint,
        }
    }

    fn matches(self, outcome: &LegacyCatalogProjectionOutcomeV4) -> bool {
        match (self, outcome) {
            (
                Self::Bootstrap(expected),
                LegacyCatalogProjectionOutcomeV4::BootstrapTable(actual),
            )
            | (
                Self::Adopt(expected),
                LegacyCatalogProjectionOutcomeV4::AdoptExistingTable(actual),
            )
            | (Self::Sync(expected), LegacyCatalogProjectionOutcomeV4::SyncTableMetadata(actual))
            | (Self::Drop(expected), LegacyCatalogProjectionOutcomeV4::DropTable(actual)) => {
                actual.table_id == expected
            }
            (Self::Checkpoint, LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(_)) => true,
            _ => false,
        }
    }
}

fn worker_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "legacy Catalog shadow worker stopped without a completion result",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        continuity_loss_requires_reconcile, observe_generation_mutation, plan_table_transition,
        request_id_from_fingerprint, LegacyCatalogShadowState, PlannedTableTransition,
        ProjectionPlanningState, TablePlanningState,
    };
    use std::cmp::Ordering;
    use std::collections::BTreeSet;
    use zippy_master::legacy_catalog_commit::LegacyCatalogCommitMutationKind;
    use zippy_types::catalog::CommandFingerprint;
    use zippy_types::TableId;

    #[test]
    fn planner_covers_bootstrap_adopt_sync_advance_and_drop() {
        let state = |source_present,
                     native_present,
                     projection,
                     file_order,
                     writer_order,
                     metadata_changed,
                     strict_unprojected_v1| TablePlanningState {
            source_present,
            native_present,
            projection,
            file_order,
            writer_order,
            metadata_changed,
            strict_unprojected_v1,
        };
        assert_eq!(
            plan_table_transition(state(
                true,
                false,
                ProjectionPlanningState::Absent,
                None,
                None,
                false,
                false,
            )),
            Ok(PlannedTableTransition::Bootstrap)
        );
        assert_eq!(
            plan_table_transition(state(
                true,
                true,
                ProjectionPlanningState::SeededActive,
                Some(Ordering::Less),
                Some(Ordering::Equal),
                false,
                false,
            )),
            Ok(PlannedTableTransition::AdvanceFilesThenAdopt)
        );
        assert_eq!(
            plan_table_transition(state(
                true,
                true,
                ProjectionPlanningState::SeededActive,
                Some(Ordering::Equal),
                Some(Ordering::Less),
                true,
                false,
            )),
            Ok(PlannedTableTransition::AdoptThenSync)
        );
        assert_eq!(
            plan_table_transition(state(
                true,
                true,
                ProjectionPlanningState::CompleteActive,
                Some(Ordering::Less),
                Some(Ordering::Equal),
                true,
                false,
            )),
            Ok(PlannedTableTransition::SyncThenAdvanceFiles)
        );
        assert_eq!(
            plan_table_transition(state(
                false,
                true,
                ProjectionPlanningState::CompleteActive,
                None,
                None,
                false,
                false,
            )),
            Ok(PlannedTableTransition::DropComplete)
        );
        assert_eq!(
            plan_table_transition(state(
                false,
                true,
                ProjectionPlanningState::Absent,
                None,
                None,
                false,
                true,
            )),
            Ok(PlannedTableTransition::DropUnprojectedV1)
        );
    }

    #[test]
    fn planner_rejects_dropped_generation_recreation() {
        let error = plan_table_transition(TablePlanningState {
            source_present: true,
            native_present: false,
            projection: ProjectionPlanningState::Dropped,
            file_order: None,
            writer_order: None,
            metadata_changed: false,
            strict_unprojected_v1: false,
        })
        .expect_err("V4 cannot distinguish a same-name legacy generation recreation");
        assert_eq!(
            error,
            "legacy_table_recreate_requires_projection_generation_v5"
        );
    }

    #[test]
    fn explicit_drop_intent_survives_event_batches_until_reconcile() {
        let table_id = TableId::from_u128(42).expect("table id is nonzero");
        let mut pending = BTreeSet::new();
        assert!(!observe_generation_mutation(
            &mut pending,
            LegacyCatalogCommitMutationKind::DropTable,
            Some(table_id),
        ));
        assert!(pending.contains(&table_id));
        assert!(observe_generation_mutation(
            &mut pending,
            LegacyCatalogCommitMutationKind::RegisterStream,
            Some(table_id),
        ));
    }

    #[test]
    fn request_identity_is_stable_and_binds_the_full_fingerprint() {
        let first_fingerprint = CommandFingerprint::from_bytes([7; 32]);
        let second_fingerprint = CommandFingerprint::from_bytes([8; 32]);
        let first = request_id_from_fingerprint(b"test-domain", first_fingerprint);
        assert_eq!(
            first,
            request_id_from_fingerprint(b"test-domain", first_fingerprint)
        );
        assert_ne!(
            first,
            request_id_from_fingerprint(b"test-domain", second_fingerprint)
        );
        assert_ne!(
            first,
            request_id_from_fingerprint(b"other-domain", first_fingerprint)
        );
    }

    #[test]
    fn worker_local_loss_epoch_is_compared_in_the_durable_epoch_domain() {
        assert!(!continuity_loss_requires_reconcile(Some(5), 0, 5));
        assert!(continuity_loss_requires_reconcile(Some(5), 1, 5));
        assert!(!continuity_loss_requires_reconcile(Some(5), 1, 6));
        assert!(continuity_loss_requires_reconcile(None, 0, 0));
        assert!(continuity_loss_requires_reconcile(
            Some(u64::MAX),
            1,
            u64::MAX
        ));
    }

    #[test]
    fn closed_is_distinct_from_candidate_eligibility() {
        assert_ne!(
            LegacyCatalogShadowState::Closed,
            LegacyCatalogShadowState::EligibleCandidate
        );
    }
}
