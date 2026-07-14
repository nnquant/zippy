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
    LegacyCatalogAdapter, LegacyCatalogMigrationSnapshot, LegacyCatalogMigrationTable,
};
use zippy_master::legacy_catalog_commit::{
    LegacyCatalogCommitEvent, LegacyCatalogCommitMutationKind, LegacyCatalogCommitPublisher,
    LegacyCatalogCommitPublisherMetrics, LegacyCatalogCommitPublisherMetricsSnapshot,
};
use zippy_master::server::MasterServer;
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogRead, CatalogSnapshotSelector,
    LegacyCatalogReconcileCommandV3, LegacyCatalogReconcileEnvelopeV3,
    LegacyFileViewSnapshotMapping, ListTablesRequest,
};
use zippy_types::table::{TableDescriptor, TableState};
use zippy_types::{ErrorCode, RequestId, ServerError, TableId};

use crate::legacy_catalog_migration::{
    verify_legacy_catalog_table_view, VerifiedLegacyCatalogFileCacheEntry,
    VerifiedLegacyCatalogTableView, LEGACY_STREAM_NAME_CONFIG_KEY,
};

const WORKER_THREAD_NAME: &str = "zippy-legacy-catalog-shadow";
const RECONCILE_ACTOR_PRINCIPAL: &str = "zippy-server/legacy-catalog-shadow-v3";
const REQUEST_ID_DOMAIN: &[u8] = b"zippy.legacy_catalog.shadow.reconcile.request.v3\0";
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
    /// The legacy mutation requires an online primitive that V3 does not provide.
    BlockedUnsupported,
    /// The publisher was sealed and the worker thread drained and exited.
    Closed,
}

/// Bounded publisher counters retained with one shadow status snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogShadowPublisherStatus {
    /// Commit notifications accepted by the bounded receiver.
    pub published_total: u64,
    /// Notifications rejected because the queue was full.
    pub queue_full_total: u64,
    /// Notifications rejected because the receiver was unavailable.
    pub disconnected_total: u64,
    /// Legacy mutation boundaries that could not emit a durable event.
    pub unpublished_mutation_total: u64,
    /// Monotonic queue-continuity loss epoch.
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
    /// Full reconcile attempts, including failed attempts.
    pub full_reconciles_total: u64,
    /// Per-table exact manifest comparisons that succeeded.
    pub exact_diffs_total: u64,
    /// V3 commands committed or idempotently replayed.
    pub v3_reconciles_total: u64,
    /// Event revision gaps observed by the consumer.
    pub revision_gaps_total: u64,
    /// Operation deadlines that elapsed.
    pub timeouts_total: u64,
    /// Catalog read or write failures.
    pub catalog_errors_total: u64,
    /// Unsupported legacy mutations observed.
    pub unsupported_mutations_total: u64,
    /// Short, path-free failure category for operations diagnostics.
    pub last_error: Option<String>,
    /// Current bounded publisher evidence.
    pub publisher: LegacyCatalogShadowPublisherStatus,
}

impl LegacyCatalogShadowStatus {
    fn initial(metrics: LegacyCatalogCommitPublisherMetricsSnapshot) -> Self {
        Self {
            state: LegacyCatalogShadowState::CatchingUp,
            consumed_catalog_revision: metrics.baseline_revision,
            reconciled_catalog_revision: 0,
            recovered_continuity_loss_epoch: 0,
            full_reconciles_total: 0,
            exact_diffs_total: 0,
            v3_reconciles_total: 0,
            revision_gaps_total: 0,
            timeouts_total: 0,
            catalog_errors_total: 0,
            unsupported_mutations_total: 0,
            last_error: None,
            publisher: metrics.into(),
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
    completion: Option<Receiver<Result<(), ServerError>>>,
    thread: Option<JoinHandle<()>>,
    final_shutdown: Option<LegacyCatalogShadowShutdownOutcome>,
}

impl LegacyCatalogShadowWorker {
    pub(crate) fn install_and_start(
        master: &MasterServer,
        catalog_read: SqliteCatalogReadHandle,
        catalog_reconcile: SqliteCatalogLegacyReconcileHandle,
        config: LegacyCatalogShadowWorkerConfig,
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
        let (completion_tx, completion_rx) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(WORKER_THREAD_NAME.to_owned())
            .spawn(move || {
                let result = match activation_rx.recv() {
                    Ok(engine) => run_worker(receiver, engine),
                    Err(_) => Ok(()),
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
            unsupported_latched: false,
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
            completion: Some(completion_rx),
            thread: Some(thread),
            final_shutdown: None,
        })
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
                || publisher.continuity_loss_epoch > status.recovered_continuity_loss_epoch)
        {
            status.state =
                if publisher.continuity_loss_epoch > status.recovered_continuity_loss_epoch {
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
        if let Some(outcome) = &self.final_shutdown {
            return outcome.clone();
        }
        let Some(completion) = self.completion.as_ref() else {
            let outcome = LegacyCatalogShadowShutdownOutcome::Failed(worker_stopped());
            self.final_shutdown = Some(outcome.clone());
            return outcome;
        };
        let wait = deadline.saturating_duration_since(Instant::now());
        let mut outcome = match completion.recv_timeout(wait) {
            Ok(Ok(())) => LegacyCatalogShadowShutdownOutcome::Complete,
            Ok(Err(error)) => LegacyCatalogShadowShutdownOutcome::Failed(error),
            Err(mpsc::RecvTimeoutError::Timeout) => LegacyCatalogShadowShutdownOutcome::TimedOut,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                LegacyCatalogShadowShutdownOutcome::Failed(worker_stopped())
            }
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
    unsupported_latched: bool,
}

#[derive(Debug)]
enum ShadowFailure {
    Recoverable(&'static str),
    Unsupported(&'static str),
    Timeout(&'static str),
    Catalog(&'static str, ServerError),
}

impl ShadowEngine {
    fn apply_events(&mut self, events: &[LegacyCatalogCommitEvent]) -> bool {
        let publisher = self.metrics.snapshot();
        let mut latch_unsupported = false;
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let was_candidate = status.state == LegacyCatalogShadowState::EligibleCandidate;
        let mut requires_reconcile = !was_candidate;
        for event in events {
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
                requires_reconcile = true;
            }
            status.consumed_catalog_revision = event.committed_revision();
            if is_unsupported(event.mutation_kind()) {
                status.unsupported_mutations_total =
                    status.unsupported_mutations_total.saturating_add(1);
                status.state = LegacyCatalogShadowState::BlockedUnsupported;
                status.last_error = Some(unsupported_reason(event.mutation_kind()).to_owned());
                latch_unsupported = true;
            } else if event.mutation_kind()
                != LegacyCatalogCommitMutationKind::PersistEventWatermark
            {
                requires_reconcile = true;
            }
        }
        if publisher.continuity_loss_epoch > status.recovered_continuity_loss_epoch {
            status.state = LegacyCatalogShadowState::BlockedRecoverable;
            status.last_error = Some("legacy_event_queue_continuity_lost".to_owned());
            requires_reconcile = true;
        }
        if !requires_reconcile && !latch_unsupported {
            status.reconciled_catalog_revision = status.consumed_catalog_revision;
            status.state = LegacyCatalogShadowState::EligibleCandidate;
            status.last_error = None;
        }
        status.publisher = publisher.into();
        drop(status);
        if latch_unsupported {
            self.unsupported_latched = true;
        }
        requires_reconcile
    }

    async fn full_reconcile(&mut self) {
        {
            let mut status = self.lock_status();
            status.full_reconciles_total = status.full_reconciles_total.saturating_add(1);
            if !self.unsupported_latched {
                status.state = LegacyCatalogShadowState::CatchingUp;
            }
        }
        if self.unsupported_latched {
            return;
        }
        match self.try_full_reconcile().await {
            Ok((revision, loss_epoch, exact_diffs, v3_reconciles)) => {
                let mut status = self.lock_status();
                status.state = LegacyCatalogShadowState::EligibleCandidate;
                status.consumed_catalog_revision = status.consumed_catalog_revision.max(revision);
                status.reconciled_catalog_revision = revision;
                status.recovered_continuity_loss_epoch = loss_epoch;
                status.exact_diffs_total = status.exact_diffs_total.saturating_add(exact_diffs);
                status.v3_reconciles_total =
                    status.v3_reconciles_total.saturating_add(v3_reconciles);
                status.last_error = None;
            }
            Err(failure) => self.record_failure(failure),
        }
    }

    async fn try_full_reconcile(&mut self) -> Result<(u64, u64, u64, u64), ShadowFailure> {
        let first = self.capture_snapshot()?;
        let native = self.list_migrated_tables().await?;
        let source_ids = first
            .tables()
            .iter()
            .map(|table| table.descriptor().table_id)
            .collect::<BTreeSet<_>>();
        let native_ids = native.keys().copied().collect::<BTreeSet<_>>();
        if source_ids != native_ids {
            return Err(ShadowFailure::Unsupported(
                "legacy_table_set_change_requires_migration_primitive",
            ));
        }
        let mut exact_diffs = 0_u64;
        let mut v3_reconciles = 0_u64;
        for table in first.tables() {
            let native_table =
                native
                    .get(&table.descriptor().table_id)
                    .ok_or(ShadowFailure::Unsupported(
                        "legacy_table_missing_from_native_catalog",
                    ))?;
            let reconciled = self.reconcile_table(&first, table, native_table).await?;
            exact_diffs = exact_diffs.saturating_add(1);
            v3_reconciles = v3_reconciles.saturating_add(u64::from(reconciled));
        }
        let second = self.capture_snapshot()?;
        let publisher = self.metrics.snapshot();
        if first.source_catalog_revision() != second.source_catalog_revision()
            || first.source_snapshot_hash() != second.source_snapshot_hash()
            || publisher.latest_observed_revision != second.source_catalog_revision()
            || (publisher.sealed && publisher.final_revision != second.source_catalog_revision())
        {
            return Err(ShadowFailure::Recoverable(
                "legacy_snapshot_changed_during_full_reconcile",
            ));
        }
        Ok((
            second.source_catalog_revision(),
            publisher.continuity_loss_epoch,
            exact_diffs,
            v3_reconciles,
        ))
    }

    fn capture_snapshot(&self) -> Result<LegacyCatalogMigrationSnapshot, ShadowFailure> {
        self.source
            .capture_migration_snapshot()
            .map_err(|_| ShadowFailure::Recoverable("legacy_snapshot_capture_failed"))
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

    async fn reconcile_table(
        &mut self,
        snapshot: &LegacyCatalogMigrationSnapshot,
        source: &LegacyCatalogMigrationTable,
        native: &TableDescriptor,
    ) -> Result<bool, ShadowFailure> {
        let table_id = source.descriptor().table_id;
        if native.table_id != table_id
            || native.name != source.descriptor().name
            || native.current_schema_version != MIGRATED_SCHEMA_VERSION
        {
            return Err(ShadowFailure::Unsupported(
                "legacy_table_identity_or_schema_changed",
            ));
        }
        self.validate_metadata_budget(source)?;
        let retained = self.verified.remove(&table_id);
        let verification = tokio::time::timeout(
            self.operation_timeout,
            verify_legacy_catalog_table_view(
                snapshot,
                source,
                self.trusted_fallback_root.as_deref(),
                retained.as_ref(),
            ),
        )
        .await;
        let verified = match verification {
            Ok(Ok(verified)) => verified,
            Ok(Err(_)) => {
                if let Some(retained) = retained {
                    self.verified.insert(table_id, retained);
                }
                return Err(ShadowFailure::Recoverable(
                    "legacy_physical_verification_failed",
                ));
            }
            Err(_) => {
                if let Some(retained) = retained {
                    self.verified.insert(table_id, retained);
                }
                return Err(ShadowFailure::Timeout(
                    "legacy_physical_verification_timeout",
                ));
            }
        };
        self.verified.insert(table_id, verified.cache.clone());
        let current = self
            .catalog_timeout(
                "catalog_resolve_current_mapping",
                self.catalog_read
                    .resolve_current_legacy_file_view_mapping(table_id),
            )
            .await?;
        if current.table_id != table_id
            || current.native_snapshot_version != native.current_snapshot_version
        {
            return Err(ShadowFailure::Catalog(
                "catalog_current_mapping_inconsistent",
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "active native legacy mapping does not match the table cursor",
                )
                .with_table_id(table_id),
            ));
        }
        let target_revision = source.persisted_files_revision();
        let revision_order = current.legacy_files_revision.cmp(&target_revision);
        let reconciled = revision_order == Ordering::Less;
        let mapping = match revision_order {
            Ordering::Equal => {
                if native.writer_epoch != source.descriptor().writer_epoch {
                    return Err(ShadowFailure::Unsupported(
                        "pure_writer_epoch_change_requires_migration_primitive",
                    ));
                }
                current
            }
            Ordering::Less => {
                self.submit_v3(snapshot, source, native, &current, &verified)
                    .await?
            }
            Ordering::Greater => {
                return Err(ShadowFailure::Recoverable(
                    "native_legacy_revision_is_ahead_of_primary",
                ));
            }
        };
        self.exact_diff(source, &mapping, &verified).await?;
        Ok(reconciled)
    }

    async fn submit_v3(
        &self,
        snapshot: &LegacyCatalogMigrationSnapshot,
        source: &LegacyCatalogMigrationTable,
        native: &TableDescriptor,
        current: &LegacyFileViewSnapshotMapping,
        verified: &VerifiedLegacyCatalogTableView,
    ) -> Result<LegacyFileViewSnapshotMapping, ShadowFailure> {
        let table_id = source.descriptor().table_id;
        let request_id = reconcile_request_id(
            snapshot.source_snapshot_hash(),
            table_id,
            current.legacy_files_revision,
            source.persisted_files_revision(),
        );
        let command = LegacyCatalogReconcileCommandV3 {
            table_id,
            expected_schema_version: native.current_schema_version,
            expected_schema_hash: verified.canonical_schema_hash.clone(),
            expected_writer_epoch: native.writer_epoch,
            writer_epoch: source.descriptor().writer_epoch,
            expected_legacy_files_revision: current.legacy_files_revision,
            legacy_files_revision: source.persisted_files_revision(),
            files: verified.files.clone(),
        };
        let envelope = LegacyCatalogReconcileEnvelopeV3::new(
            request_id,
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: RECONCILE_ACTOR_PRINCIPAL.to_owned(),
            },
            snapshot.source_snapshot_hash().to_owned(),
            snapshot.source_catalog_revision(),
            command,
        )
        .map_err(|_| ShadowFailure::Recoverable("legacy_reconcile_fingerprint_failed"))?;
        let response = self
            .catalog_timeout(
                "catalog_reconcile_v3",
                self.catalog_reconcile.reconcile_legacy_table_v3(envelope),
            )
            .await?;
        if response.table_id != table_id
            || response.previous_legacy_files_revision != current.legacy_files_revision
            || response.legacy_files_revision != source.persisted_files_revision()
        {
            return Err(ShadowFailure::Catalog(
                "catalog_reconcile_response_inconsistent",
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "legacy reconcile response does not match its request",
                )
                .with_table_id(table_id),
            ));
        }
        self.catalog_timeout(
            "catalog_resolve_reconciled_mapping",
            self.catalog_read
                .resolve_legacy_file_view_mapping(table_id, source.persisted_files_revision()),
        )
        .await
    }

    async fn exact_diff(
        &self,
        source: &LegacyCatalogMigrationTable,
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
        table: &LegacyCatalogMigrationTable,
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

    fn record_failure(&mut self, failure: ShadowFailure) {
        let mut latch_unsupported = false;
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match failure {
            ShadowFailure::Recoverable(reason) => {
                status.state = LegacyCatalogShadowState::BlockedRecoverable;
                status.last_error = Some(reason.to_owned());
            }
            ShadowFailure::Unsupported(reason) => {
                status.state = LegacyCatalogShadowState::BlockedUnsupported;
                status.unsupported_mutations_total =
                    status.unsupported_mutations_total.saturating_add(1);
                status.last_error = Some(reason.to_owned());
                latch_unsupported = true;
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
        }
        drop(status);
        if latch_unsupported {
            self.unsupported_latched = true;
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
) -> Result<(), ServerError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("build legacy Catalog shadow runtime error=[{error}]"),
            )
        })?;
    runtime.block_on(engine.full_reconcile());
    let mut last_retry = Instant::now();
    loop {
        match receiver.recv_timeout(IDLE_POLL_INTERVAL) {
            Ok(first) => {
                let mut events = vec![first];
                let disconnected = drain_events(&receiver, &mut events);
                if engine.apply_events(&events) {
                    runtime.block_on(engine.full_reconcile());
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
                let retryable = current.state == LegacyCatalogShadowState::BlockedRecoverable
                    || snapshot.continuity_loss_epoch > current.recovered_continuity_loss_epoch
                    || snapshot.latest_observed_revision > current.consumed_catalog_revision;
                if retryable && last_retry.elapsed() >= RECOVERABLE_RETRY_INTERVAL {
                    runtime.block_on(engine.full_reconcile());
                    last_retry = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    runtime.block_on(engine.full_reconcile());
    let mut final_status = engine
        .status
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    final_status.publisher = engine.metrics.snapshot().into();
    final_status.state = LegacyCatalogShadowState::Closed;
    Ok(())
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

fn is_unsupported(kind: LegacyCatalogCommitMutationKind) -> bool {
    matches!(
        kind,
        LegacyCatalogCommitMutationKind::RegisterStream
            | LegacyCatalogCommitMutationKind::RegisterSource
            | LegacyCatalogCommitMutationKind::UnregisterSource
            | LegacyCatalogCommitMutationKind::DropTable
    )
}

fn unsupported_reason(kind: LegacyCatalogCommitMutationKind) -> &'static str {
    match kind {
        LegacyCatalogCommitMutationKind::RegisterStream => {
            "new_legacy_table_requires_migration_primitive"
        }
        LegacyCatalogCommitMutationKind::RegisterSource
        | LegacyCatalogCommitMutationKind::UnregisterSource => {
            "legacy_writer_or_source_change_requires_migration_primitive"
        }
        LegacyCatalogCommitMutationKind::DropTable => {
            "legacy_table_drop_requires_migration_primitive"
        }
        LegacyCatalogCommitMutationKind::PublishPersistedFile
        | LegacyCatalogCommitMutationKind::ReplacePersistedFiles
        | LegacyCatalogCommitMutationKind::Compaction
        | LegacyCatalogCommitMutationKind::PersistEventWatermark => "supported",
    }
}

fn reconcile_request_id(
    source_snapshot_hash: &str,
    table_id: TableId,
    expected_revision: u64,
    target_revision: u64,
) -> RequestId {
    let mut hasher = Sha256::new();
    hasher.update(REQUEST_ID_DOMAIN);
    hasher.update(source_snapshot_hash.as_bytes());
    hasher.update(table_id.into_bytes());
    hasher.update(expected_revision.to_be_bytes());
    hasher.update(target_revision.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    RequestId::from_bytes(bytes).expect("nonzero deterministic request id")
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
        is_unsupported, reconcile_request_id, unsupported_reason, LegacyCatalogShadowState,
    };
    use zippy_master::legacy_catalog_commit::LegacyCatalogCommitMutationKind;
    use zippy_types::TableId;

    #[test]
    fn only_v3_unrepresentable_mutations_are_unsupported() {
        for kind in [
            LegacyCatalogCommitMutationKind::RegisterStream,
            LegacyCatalogCommitMutationKind::RegisterSource,
            LegacyCatalogCommitMutationKind::UnregisterSource,
            LegacyCatalogCommitMutationKind::DropTable,
        ] {
            assert!(is_unsupported(kind));
            assert_ne!(unsupported_reason(kind), "supported");
        }
        for kind in [
            LegacyCatalogCommitMutationKind::PublishPersistedFile,
            LegacyCatalogCommitMutationKind::ReplacePersistedFiles,
            LegacyCatalogCommitMutationKind::Compaction,
            LegacyCatalogCommitMutationKind::PersistEventWatermark,
        ] {
            assert!(!is_unsupported(kind));
        }
    }

    #[test]
    fn reconcile_request_identity_is_stable_and_revision_scoped() {
        let table_id = TableId::from_u128(7).unwrap();
        let first = reconcile_request_id("sha256-v1:abc", table_id, 4, 5);
        assert_eq!(first, reconcile_request_id("sha256-v1:abc", table_id, 4, 5));
        assert_ne!(first, reconcile_request_id("sha256-v1:abc", table_id, 4, 6));
    }

    #[test]
    fn closed_is_distinct_from_candidate_eligibility() {
        assert_ne!(
            LegacyCatalogShadowState::Closed,
            LegacyCatalogShadowState::EligibleCandidate
        );
    }
}
