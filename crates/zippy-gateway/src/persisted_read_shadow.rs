use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::io::{self, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Serialize;
use sha2::{Digest, Sha256};
use zippy_core::{Result, StreamInfo, ZippyError};

use crate::{PersistedReadSnapshot, PersistedReadSnapshotSource};

/// Default number of metadata comparisons waiting behind the shadow worker.
pub const DEFAULT_PERSISTED_READ_SHADOW_QUEUE_CAPACITY: usize = 64;
/// Default byte budget shared by queued and active legacy metadata comparisons.
pub const DEFAULT_PERSISTED_READ_SHADOW_METADATA_BUDGET_BYTES: usize = 64 * 1024 * 1024;
/// Default file-count limit for one metadata comparison.
pub const DEFAULT_PERSISTED_READ_SHADOW_MAX_FILES: usize = 4096;
/// Default encoded metadata limit for one comparison.
pub const DEFAULT_PERSISTED_READ_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON: usize = 8 * 1024 * 1024;
/// Default deadline for one native metadata comparison.
pub const DEFAULT_PERSISTED_READ_SHADOW_COMPARISON_TIMEOUT_MS: u64 = 5000;
/// Default number of completed metadata fingerprints retained for deduplication.
pub const DEFAULT_PERSISTED_READ_SHADOW_DEDUP_CAPACITY: usize = 1024;

const MAX_SHADOW_STATUS_ERROR_BYTES: usize = 512;
const SHADOW_WORKER_DROP_JOIN_TIMEOUT: Duration = Duration::from_millis(100);

/// Bounds best-effort persisted-read shadow comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct PersistedReadShadowConfig {
    /// Maximum number of comparisons waiting in the worker channel.
    pub queue_capacity: usize,
    /// Maximum encoded legacy metadata bytes queued or being compared.
    pub metadata_budget_bytes: usize,
    /// Maximum number of persisted-file descriptors accepted from one primary snapshot.
    pub max_files_per_comparison: usize,
    /// Maximum encoded metadata bytes accepted from one primary or shadow snapshot.
    pub max_metadata_bytes_per_comparison: usize,
    /// Deadline applied to one asynchronous shadow authority lookup.
    pub comparison_timeout_ms: u64,
    /// Maximum completed fingerprints retained to suppress duplicate comparisons.
    pub dedup_capacity: usize,
}

impl Default for PersistedReadShadowConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_PERSISTED_READ_SHADOW_QUEUE_CAPACITY,
            metadata_budget_bytes: DEFAULT_PERSISTED_READ_SHADOW_METADATA_BUDGET_BYTES,
            max_files_per_comparison: DEFAULT_PERSISTED_READ_SHADOW_MAX_FILES,
            max_metadata_bytes_per_comparison:
                DEFAULT_PERSISTED_READ_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON,
            comparison_timeout_ms: DEFAULT_PERSISTED_READ_SHADOW_COMPARISON_TIMEOUT_MS,
            dedup_capacity: DEFAULT_PERSISTED_READ_SHADOW_DEDUP_CAPACITY,
        }
    }
}

impl PersistedReadShadowConfig {
    fn validate(self) -> Result<Self> {
        for (name, value) in [
            ("queue_capacity", self.queue_capacity),
            ("metadata_budget_bytes", self.metadata_budget_bytes),
            ("max_files_per_comparison", self.max_files_per_comparison),
            (
                "max_metadata_bytes_per_comparison",
                self.max_metadata_bytes_per_comparison,
            ),
            ("dedup_capacity", self.dedup_capacity),
        ] {
            if value == 0 {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("persisted read shadow {} must be positive", name),
                });
            }
        }
        if self.comparison_timeout_ms == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "persisted read shadow comparison_timeout_ms must be positive".to_string(),
            });
        }
        if self.max_metadata_bytes_per_comparison > self.metadata_budget_bytes {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "persisted read shadow per-comparison metadata limit exceeds total budget \
                     per_comparison=[{}] total=[{}]",
                    self.max_metadata_bytes_per_comparison, self.metadata_budget_bytes
                ),
            });
        }
        Ok(self)
    }
}

/// Cumulative best-effort shadow comparison metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PersistedReadShadowMetrics {
    /// Primary snapshots returned successfully to callers.
    pub primary_snapshots_total: u64,
    /// Primary snapshot errors returned unchanged to callers.
    pub primary_errors_total: u64,
    /// Metadata comparisons accepted by the background worker.
    pub comparisons_enqueued_total: u64,
    /// Repeated comparisons suppressed by an in-flight or completed fingerprint.
    pub comparisons_deduplicated_total: u64,
    /// Comparisons rejected because the bounded channel was full.
    pub queue_full_total: u64,
    /// Comparisons rejected because their metadata exceeded the shared byte budget.
    pub metadata_budget_exhausted_total: u64,
    /// Comparisons rejected because one primary snapshot contained too many files.
    pub comparison_file_limit_exhausted_total: u64,
    /// Primary or shadow snapshots rejected by the per-comparison metadata byte limit.
    pub comparison_metadata_limit_exhausted_total: u64,
    /// Comparisons rejected after the worker channel became unavailable.
    pub worker_unavailable_total: u64,
    /// Legacy or shadow metadata values that could not be encoded for comparison.
    pub metadata_encode_errors_total: u64,
    /// Legacy or shadow metadata values missing a comparable canonical file path.
    pub metadata_normalization_errors_total: u64,
    /// Comparisons that began executing on the worker.
    pub comparisons_started_total: u64,
    /// Comparisons whose stream identity and file metadata matched exactly.
    pub matches_total: u64,
    /// Comparisons whose stream identity or file metadata differed.
    pub mismatches_total: u64,
    /// Errors returned by the injected shadow authority.
    pub shadow_errors_total: u64,
    /// Panics contained while invoking the injected shadow authority.
    pub shadow_panics_total: u64,
    /// Shadow lookups cancelled during source shutdown.
    pub shadow_cancellations_total: u64,
    /// Shadow lookups that exceeded the configured comparison deadline.
    pub shadow_timeouts_total: u64,
    /// Highest observed number of jobs waiting in the channel.
    pub max_queue_depth: usize,
    /// Highest observed metadata bytes reserved by queued and active jobs.
    pub max_outstanding_metadata_bytes: usize,
}

/// Compact description of the latest persisted-read metadata mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistedReadShadowMismatch {
    /// Highest-level semantic category of this mismatch.
    pub kind: PersistedReadShadowMismatchKind,
    /// Stream requested by the primary Gateway read.
    pub stream_name: String,
    /// Exact legacy persisted-file revision requested from the shadow authority.
    pub persisted_files_revision: u64,
    /// Stream identity returned by the legacy authority.
    pub primary_stream_name: String,
    /// Stream identity returned by the shadow authority.
    pub shadow_stream_name: String,
    /// Number of file descriptors returned by the legacy authority.
    pub primary_file_count: usize,
    /// Number of file descriptors returned by the shadow authority.
    pub shadow_file_count: usize,
    /// SHA-256 fingerprint of the legacy file metadata.
    pub primary_metadata_sha256: String,
    /// SHA-256 fingerprint of the shadow file metadata.
    pub shadow_metadata_sha256: String,
}

/// Stable semantic categories used by persisted-read shadow mismatch metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedReadShadowMismatchKind {
    /// Primary and shadow authorities resolved different stream identities.
    StreamName,
    /// Primary and shadow authorities resolved different schema hashes.
    SchemaHash,
    /// Primary and shadow snapshots contained different file counts.
    FileCount,
    /// Canonical files matched as a set but appeared in a different deterministic order.
    FileOrder,
    /// At least one canonical file path, row count, lineage, range, partition, or stat differed.
    FileMetadata,
}

/// Native schema and persisted-file metadata resolved for one exact shadow revision.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedReadShadowSnapshot {
    schema_hash: String,
    snapshot: PersistedReadSnapshot,
}

impl PersistedReadShadowSnapshot {
    /// Create one exact native schema and persisted-file metadata view.
    #[must_use]
    pub fn new(schema_hash: impl Into<String>, snapshot: PersistedReadSnapshot) -> Self {
        Self {
            schema_hash: schema_hash.into(),
            snapshot,
        }
    }

    /// Return the native schema hash bound to this file view.
    #[must_use]
    pub fn schema_hash(&self) -> &str {
        &self.schema_hash
    }

    /// Return the native persisted-read snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &PersistedReadSnapshot {
        &self.snapshot
    }
}

/// Current bounded-queue state plus cumulative shadow metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistedReadShadowStatus {
    /// Active bounds used by this shadow source.
    pub config: PersistedReadShadowConfig,
    /// Whether the dedicated comparison worker remains available.
    pub worker_running: bool,
    /// Jobs currently waiting in the bounded channel.
    pub queue_depth: usize,
    /// Comparisons currently executing; this implementation uses one worker.
    pub comparisons_active: usize,
    /// Metadata bytes reserved by queued and active comparisons.
    pub outstanding_metadata_bytes: usize,
    /// Distinct in-flight fingerprints currently suppressing duplicates.
    pub in_flight_dedup_entries: usize,
    /// Completed fingerprints retained in the bounded deduplication window.
    pub completed_dedup_entries: usize,
    /// Cumulative comparison metrics.
    pub metrics: PersistedReadShadowMetrics,
    /// Latest bounded worker or encoding error, if any.
    pub last_error: Option<String>,
    /// Latest compact mismatch, if any.
    pub last_mismatch: Option<PersistedReadShadowMismatch>,
}

/// Resolves one exact native persisted-file view for background comparison.
///
/// Implementations must resolve `persisted_files_revision` exactly. The method is invoked only on
/// the dedicated shadow worker, never on a Gateway query thread. The returned future must not
/// block inside `poll`; shutdown and deadline enforcement rely on cancelling it by dropping it.
pub trait PersistedReadShadowSource: Send + Sync + 'static {
    /// Resolve native metadata for one exact legacy persisted-file revision.
    fn snapshot<'a>(
        &'a self,
        stream_name: &str,
        persisted_files_revision: u64,
    ) -> Pin<Box<dyn Future<Output = Result<PersistedReadShadowSnapshot>> + Send + 'a>>;
}

/// Returns legacy persisted-read snapshots while comparing native metadata asynchronously.
///
/// Only JSON file descriptors are copied into the bounded queue. The wrapper never opens a data
/// file and never waits for the shadow authority, so a full queue, exhausted byte budget, shadow
/// error, or contained panic cannot change the primary query result.
pub struct ShadowPersistedReadSnapshotSource {
    primary: Arc<dyn PersistedReadSnapshotSource>,
    sender: Mutex<Option<mpsc::SyncSender<PersistedReadShadowJob>>>,
    shared: Arc<PersistedReadShadowShared>,
    shutdown: tokio::sync::watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl ShadowPersistedReadSnapshotSource {
    /// Create a bounded, single-worker persisted-read shadow source.
    pub fn new(
        primary: Arc<dyn PersistedReadSnapshotSource>,
        shadow: Arc<dyn PersistedReadShadowSource>,
        config: PersistedReadShadowConfig,
    ) -> Result<Self> {
        let config = config.validate()?;
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let (shutdown, shutdown_receiver) = tokio::sync::watch::channel(false);
        let shared = Arc::new(PersistedReadShadowShared {
            config,
            state: Mutex::new(PersistedReadShadowState {
                worker_running: true,
                ..PersistedReadShadowState::default()
            }),
            idle: Condvar::new(),
        });
        let worker_shared = Arc::clone(&shared);
        let worker = thread::Builder::new()
            .name("zippy-persisted-read-shadow".to_string())
            .spawn(move || run_shadow_worker(receiver, shadow, worker_shared, shutdown_receiver))
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to spawn persisted read shadow worker error=[{}]",
                    error
                ),
            })?;
        Ok(Self {
            primary,
            sender: Mutex::new(Some(sender)),
            shared,
            shutdown,
            worker: Mutex::new(Some(worker)),
        })
    }

    /// Return current queue, deduplication, and comparison status.
    #[must_use]
    pub fn status(&self) -> PersistedReadShadowStatus {
        let state = self.shared.state.lock().unwrap();
        PersistedReadShadowStatus {
            config: self.shared.config,
            worker_running: state.worker_running
                && self
                    .worker
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|worker| !worker.is_finished()),
            queue_depth: state.queue_depth,
            comparisons_active: state.comparisons_active,
            outstanding_metadata_bytes: state.outstanding_metadata_bytes,
            in_flight_dedup_entries: state.in_flight_keys.len(),
            completed_dedup_entries: state.completed_keys.len(),
            metrics: state.metrics.clone(),
            last_error: state.last_error.clone(),
            last_mismatch: state.last_mismatch.clone(),
        }
    }

    /// Wait up to `timeout` for all accepted comparisons to finish.
    ///
    /// This is intended for deterministic tests and bounded operational draining. It never affects
    /// primary query admission.
    #[must_use]
    pub fn wait_for_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.shared.state.lock().unwrap();
        loop {
            if state.queue_depth == 0 && state.comparisons_active == 0 {
                return true;
            }
            let Some(deadline) = deadline else {
                return false;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, wait) = self.shared.idle.wait_timeout(state, remaining).unwrap();
            state = next;
            if wait.timed_out() && (state.queue_depth != 0 || state.comparisons_active != 0) {
                return false;
            }
        }
    }

    fn enqueue_comparison(&self, legacy_stream: &StreamInfo, primary: &PersistedReadSnapshot) {
        if primary.persisted_files().len() > self.shared.config.max_files_per_comparison {
            self.shared.record_file_limit_exhausted();
            return;
        }
        let primary_metadata = match encode_bounded_metadata(
            primary.persisted_files(),
            self.shared.config.max_metadata_bytes_per_comparison,
        ) {
            Ok(metadata) => metadata,
            Err(MetadataEncodingError::LimitExceeded) => {
                self.shared.record_comparison_metadata_limit_exhausted();
                return;
            }
            Err(MetadataEncodingError::Serialize(error)) => {
                self.shared.record_encode_error(format!(
                    "failed to encode primary persisted metadata stream_name=[{}] error=[{}]",
                    legacy_stream.stream_name, error
                ));
                return;
            }
        };
        let reserved_metadata_bytes = legacy_stream
            .stream_name
            .len()
            .saturating_add(legacy_stream.schema_hash.len())
            .saturating_add(primary.stream_name().len())
            .saturating_add(primary_metadata.len());
        let key = PersistedReadShadowKey::new(
            &legacy_stream.stream_name,
            legacy_stream.persisted_files_revision,
            &legacy_stream.schema_hash,
            primary.stream_name(),
            &primary_metadata,
        );
        let job = PersistedReadShadowJob {
            key,
            stream_name: legacy_stream.stream_name.clone(),
            persisted_files_revision: legacy_stream.persisted_files_revision,
            primary_schema_hash: legacy_stream.schema_hash.clone(),
            primary_stream_name: primary.stream_name().to_string(),
            primary_file_count: primary.persisted_files().len(),
            primary_metadata,
            reserved_metadata_bytes,
        };
        let sender = self.sender.lock().unwrap().as_ref().cloned();
        if let Some(sender) = sender {
            self.shared.try_enqueue(&sender, job);
        } else {
            self.shared.record_worker_unavailable();
        }
    }
}

impl Drop for ShadowPersistedReadSnapshotSource {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.sender.get_mut().unwrap().take();
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            let deadline = Instant::now()
                .checked_add(SHADOW_WORKER_DROP_JOIN_TIMEOUT)
                .unwrap_or_else(Instant::now);
            while !worker.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            if worker.is_finished() {
                if worker.join().is_err() {
                    tracing::warn!(
                        event = "persisted_read_shadow_join_failed",
                        "persisted read shadow worker panicked during shutdown"
                    );
                }
            } else if let Err(error) = thread::Builder::new()
                .name("zippy-persisted-read-shadow-reaper".to_string())
                .spawn(move || {
                    if worker.join().is_err() {
                        tracing::warn!(
                            event = "persisted_read_shadow_join_failed",
                            "persisted read shadow worker panicked during deferred shutdown"
                        );
                    }
                })
            {
                tracing::warn!(
                    event = "persisted_read_shadow_reaper_start_failed",
                    error = %error,
                    "failed to start persisted read shadow shutdown reaper"
                );
            }
        }
    }
}

impl PersistedReadSnapshotSource for ShadowPersistedReadSnapshotSource {
    fn snapshot(&self, legacy_stream: &StreamInfo) -> Result<PersistedReadSnapshot> {
        let primary = match self.primary.snapshot(legacy_stream) {
            Ok(primary) => primary,
            Err(error) => {
                self.shared
                    .state
                    .lock()
                    .unwrap()
                    .metrics
                    .primary_errors_total += 1;
                return Err(error);
            }
        };
        self.shared
            .state
            .lock()
            .unwrap()
            .metrics
            .primary_snapshots_total += 1;
        self.enqueue_comparison(legacy_stream, &primary);
        Ok(primary)
    }

    fn shadow_status(&self) -> Option<PersistedReadShadowStatus> {
        Some(self.status())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PersistedReadShadowKey([u8; 32]);

impl PersistedReadShadowKey {
    fn new(
        requested_stream_name: &str,
        persisted_files_revision: u64,
        primary_schema_hash: &str,
        primary_stream_name: &str,
        primary_metadata: &[u8],
    ) -> Self {
        let mut hasher = Sha256::new();
        update_length_prefixed(&mut hasher, requested_stream_name.as_bytes());
        hasher.update(persisted_files_revision.to_le_bytes());
        update_length_prefixed(&mut hasher, primary_schema_hash.as_bytes());
        update_length_prefixed(&mut hasher, primary_stream_name.as_bytes());
        update_length_prefixed(&mut hasher, primary_metadata);
        Self(hasher.finalize().into())
    }
}

enum MetadataEncodingError {
    LimitExceeded,
    Serialize(String),
}

struct BoundedMetadataBuffer {
    bytes: Vec<u8>,
    limit: usize,
    limit_exceeded: bool,
}

impl BoundedMetadataBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(64 * 1024)),
            limit,
            limit_exceeded: false,
        }
    }
}

impl Write for BoundedMetadataBuffer {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        if value.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "persisted read shadow metadata limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_bounded_metadata(
    persisted_files: &[serde_json::Value],
    limit: usize,
) -> std::result::Result<Vec<u8>, MetadataEncodingError> {
    let mut writer = BoundedMetadataBuffer::new(limit);
    match serde_json::to_writer(&mut writer, persisted_files) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.limit_exceeded => Err(MetadataEncodingError::LimitExceeded),
        Err(error) => Err(MetadataEncodingError::Serialize(error.to_string())),
    }
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(value.len().to_le_bytes());
    hasher.update(value);
}

struct PersistedReadShadowJob {
    key: PersistedReadShadowKey,
    stream_name: String,
    persisted_files_revision: u64,
    primary_schema_hash: String,
    primary_stream_name: String,
    primary_file_count: usize,
    primary_metadata: Vec<u8>,
    reserved_metadata_bytes: usize,
}

struct PersistedReadShadowShared {
    config: PersistedReadShadowConfig,
    state: Mutex<PersistedReadShadowState>,
    idle: Condvar,
}

#[derive(Default)]
struct PersistedReadShadowState {
    worker_running: bool,
    queue_depth: usize,
    comparisons_active: usize,
    outstanding_metadata_bytes: usize,
    in_flight_keys: HashSet<PersistedReadShadowKey>,
    completed_keys: HashSet<PersistedReadShadowKey>,
    completed_order: VecDeque<PersistedReadShadowKey>,
    metrics: PersistedReadShadowMetrics,
    last_error: Option<String>,
    last_mismatch: Option<PersistedReadShadowMismatch>,
}

impl PersistedReadShadowShared {
    fn try_enqueue(
        &self,
        sender: &mpsc::SyncSender<PersistedReadShadowJob>,
        job: PersistedReadShadowJob,
    ) {
        let mut state = self.state.lock().unwrap();
        if state.in_flight_keys.contains(&job.key) || state.completed_keys.contains(&job.key) {
            state.metrics.comparisons_deduplicated_total += 1;
            return;
        }
        let requested = job.reserved_metadata_bytes;
        if requested > self.config.metadata_budget_bytes
            || state.outstanding_metadata_bytes
                > self.config.metadata_budget_bytes.saturating_sub(requested)
        {
            state.metrics.metadata_budget_exhausted_total += 1;
            return;
        }

        state.in_flight_keys.insert(job.key);
        state.queue_depth += 1;
        state.outstanding_metadata_bytes += requested;
        match sender.try_send(job) {
            Ok(()) => {
                state.metrics.comparisons_enqueued_total += 1;
                state.metrics.max_queue_depth =
                    state.metrics.max_queue_depth.max(state.queue_depth);
                state.metrics.max_outstanding_metadata_bytes = state
                    .metrics
                    .max_outstanding_metadata_bytes
                    .max(state.outstanding_metadata_bytes);
            }
            Err(mpsc::TrySendError::Full(job)) => {
                rollback_reservation(&mut state, &job);
                state.metrics.queue_full_total += 1;
            }
            Err(mpsc::TrySendError::Disconnected(job)) => {
                rollback_reservation(&mut state, &job);
                state.worker_running = false;
                state.metrics.worker_unavailable_total += 1;
                self.idle.notify_all();
            }
        }
    }

    fn record_encode_error(&self, error: String) {
        let mut state = self.state.lock().unwrap();
        state.metrics.metadata_encode_errors_total += 1;
        state.last_error = Some(bounded_status_error(error));
    }

    fn record_file_limit_exhausted(&self) {
        self.state
            .lock()
            .unwrap()
            .metrics
            .comparison_file_limit_exhausted_total += 1;
    }

    fn record_comparison_metadata_limit_exhausted(&self) {
        self.state
            .lock()
            .unwrap()
            .metrics
            .comparison_metadata_limit_exhausted_total += 1;
    }

    fn record_worker_unavailable(&self) {
        let mut state = self.state.lock().unwrap();
        state.worker_running = false;
        state.metrics.worker_unavailable_total += 1;
    }
}

fn rollback_reservation(state: &mut PersistedReadShadowState, job: &PersistedReadShadowJob) {
    state.in_flight_keys.remove(&job.key);
    state.queue_depth = state.queue_depth.saturating_sub(1);
    state.outstanding_metadata_bytes = state
        .outstanding_metadata_bytes
        .saturating_sub(job.reserved_metadata_bytes);
}

fn run_shadow_worker(
    receiver: mpsc::Receiver<PersistedReadShadowJob>,
    shadow: Arc<dyn PersistedReadShadowSource>,
    shared: Arc<PersistedReadShadowShared>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let _running = PersistedReadShadowWorkerGuard {
        shared: Arc::clone(&shared),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let mut state = shared.state.lock().unwrap();
            state.metrics.worker_unavailable_total += 1;
            state.last_error = Some(format!(
                "failed to create persisted read shadow runtime error=[{}]",
                error
            ));
            return;
        }
    };
    while let Ok(job) = receiver.recv() {
        if *shutdown.borrow() {
            break;
        }
        {
            let mut state = shared.state.lock().unwrap();
            state.queue_depth = state.queue_depth.saturating_sub(1);
            state.comparisons_active += 1;
            state.metrics.comparisons_started_total += 1;
        }
        let outcome = compare_shadow_snapshot(
            &job,
            shadow.as_ref(),
            shared.config,
            &runtime,
            &mut shutdown,
        );
        let cancelled = matches!(outcome, PersistedReadShadowOutcome::Cancelled);
        finish_shadow_job(&shared, job, outcome);
        if cancelled {
            break;
        }
    }
}

struct PersistedReadShadowWorkerGuard {
    shared: Arc<PersistedReadShadowShared>,
}

impl Drop for PersistedReadShadowWorkerGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        state.worker_running = false;
        self.shared.idle.notify_all();
    }
}

enum PersistedReadShadowCall {
    Completed(Result<PersistedReadShadowSnapshot>),
    Cancelled,
    TimedOut,
}

enum PersistedReadShadowOutcome {
    Match,
    Mismatch(PersistedReadShadowMismatch),
    ShadowError(String),
    EncodeError(String),
    NormalizeError(String),
    FileLimitExceeded,
    MetadataLimitExceeded,
    Cancelled,
    TimedOut,
    ShadowPanic,
}

#[derive(Debug, Serialize)]
struct CanonicalPersistedReadSnapshot {
    stream_name: String,
    schema_hash: String,
    files: Vec<CanonicalPersistedFile>,
}

#[derive(Debug, Serialize)]
struct CanonicalPersistedFile {
    file_path: String,
    row_count: Option<u64>,
    source_segments: Vec<CanonicalSourceSegment>,
    logical_offsets: Option<CanonicalOffsetRange>,
    partition: Option<serde_json::Value>,
    statistics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct CanonicalSourceSegment {
    segment_id: Option<u64>,
    generation: Option<u64>,
    writer_epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
struct CanonicalOffsetRange {
    first: Option<u64>,
    last: Option<u64>,
}

fn compare_shadow_snapshot(
    job: &PersistedReadShadowJob,
    shadow: &dyn PersistedReadShadowSource,
    config: PersistedReadShadowConfig,
    runtime: &tokio::runtime::Runtime,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> PersistedReadShadowOutcome {
    let primary_files =
        match serde_json::from_slice::<Vec<serde_json::Value>>(&job.primary_metadata) {
            Ok(files) => files,
            Err(error) => {
                return PersistedReadShadowOutcome::EncodeError(format!(
                    "failed to decode queued primary metadata stream_name=[{}] error=[{}]",
                    job.stream_name, error
                ));
            }
        };
    let primary = match canonicalize_persisted_read_snapshot(
        &job.primary_stream_name,
        &job.primary_schema_hash,
        &primary_files,
    ) {
        Ok(primary) => primary,
        Err(error) => return PersistedReadShadowOutcome::NormalizeError(error),
    };
    if *shutdown.borrow() {
        return PersistedReadShadowOutcome::Cancelled;
    }
    let native = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    PersistedReadShadowCall::Cancelled
                }
                result = tokio::time::timeout(
                    Duration::from_millis(config.comparison_timeout_ms),
                    shadow.snapshot(&job.stream_name, job.persisted_files_revision),
                ) => match result {
                    Ok(result) => PersistedReadShadowCall::Completed(result),
                    Err(_) => PersistedReadShadowCall::TimedOut,
                }
            }
        })
    }));
    let native = match native {
        Ok(PersistedReadShadowCall::Completed(Ok(native))) => native,
        Ok(PersistedReadShadowCall::Completed(Err(error))) => {
            return PersistedReadShadowOutcome::ShadowError(error.to_string());
        }
        Ok(PersistedReadShadowCall::Cancelled) => {
            return PersistedReadShadowOutcome::Cancelled;
        }
        Ok(PersistedReadShadowCall::TimedOut) => {
            return PersistedReadShadowOutcome::TimedOut;
        }
        Err(_) => return PersistedReadShadowOutcome::ShadowPanic,
    };
    if native.snapshot().persisted_files().len() > config.max_files_per_comparison {
        return PersistedReadShadowOutcome::FileLimitExceeded;
    }
    let _native_metadata = match encode_bounded_metadata(
        native.snapshot().persisted_files(),
        config.max_metadata_bytes_per_comparison,
    ) {
        Ok(metadata) => metadata,
        Err(MetadataEncodingError::LimitExceeded) => {
            return PersistedReadShadowOutcome::MetadataLimitExceeded;
        }
        Err(MetadataEncodingError::Serialize(error)) => {
            return PersistedReadShadowOutcome::EncodeError(format!(
                "failed to encode shadow persisted metadata stream_name=[{}] error=[{}]",
                job.stream_name, error
            ));
        }
    };
    let native = match canonicalize_persisted_read_snapshot(
        native.snapshot().stream_name(),
        native.schema_hash(),
        native.snapshot().persisted_files(),
    ) {
        Ok(native) => native,
        Err(error) => return PersistedReadShadowOutcome::NormalizeError(error),
    };
    let Some(kind) = persisted_read_mismatch_kind(&primary, &native) else {
        return PersistedReadShadowOutcome::Match;
    };
    PersistedReadShadowOutcome::Mismatch(PersistedReadShadowMismatch {
        kind,
        stream_name: job.stream_name.clone(),
        persisted_files_revision: job.persisted_files_revision,
        primary_stream_name: job.primary_stream_name.clone(),
        shadow_stream_name: native.stream_name.clone(),
        primary_file_count: job.primary_file_count,
        shadow_file_count: native.files.len(),
        primary_metadata_sha256: canonical_snapshot_sha256(&primary),
        shadow_metadata_sha256: canonical_snapshot_sha256(&native),
    })
}

fn canonicalize_persisted_read_snapshot(
    stream_name: &str,
    schema_hash: &str,
    persisted_files: &[serde_json::Value],
) -> std::result::Result<CanonicalPersistedReadSnapshot, String> {
    let files = persisted_files
        .iter()
        .enumerate()
        .map(|(index, descriptor)| {
            canonicalize_persisted_file(descriptor).map_err(|error| {
                format!(
                    "failed to normalize persisted metadata stream_name=[{}] file_index=[{}] \
                     error=[{}]",
                    stream_name, index, error
                )
            })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(CanonicalPersistedReadSnapshot {
        stream_name: stream_name.to_string(),
        schema_hash: schema_hash.to_string(),
        files,
    })
}

fn canonicalize_persisted_file(
    descriptor: &serde_json::Value,
) -> std::result::Result<CanonicalPersistedFile, String> {
    let file = descriptor
        .get("file")
        .filter(|value| value.is_object())
        .unwrap_or(descriptor);
    let file_path = canonical_descriptor_path(descriptor, file)?;
    let row_count = descriptor_field(descriptor, file, "row_count").and_then(json_u64);
    let source_segments = canonical_source_segments(descriptor, file);
    let logical_offsets = canonical_logical_offsets(descriptor, file);
    let partition = descriptor_field(descriptor, file, "partition")
        .or_else(|| descriptor_field(descriptor, file, "partition_spec"))
        .or_else(|| descriptor_field(descriptor, file, "partition_path"))
        .map(normalize_metadata_evidence);
    let statistics = descriptor_field(descriptor, file, "stats")
        .or_else(|| descriptor_field(descriptor, file, "statistics"))
        .map(normalize_metadata_evidence);
    Ok(CanonicalPersistedFile {
        file_path,
        row_count,
        source_segments,
        logical_offsets,
        partition,
        statistics,
    })
}

fn descriptor_field<'a>(
    descriptor: &'a serde_json::Value,
    file: &'a serde_json::Value,
    name: &str,
) -> Option<&'a serde_json::Value> {
    file.get(name).or_else(|| descriptor.get(name))
}

fn canonical_descriptor_path(
    descriptor: &serde_json::Value,
    file: &serde_json::Value,
) -> std::result::Result<String, String> {
    let location = descriptor_field(descriptor, file, "location");
    let raw_path = descriptor_field(descriptor, file, "file_path")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            location
                .and_then(|value| value.get("path"))
                .and_then(serde_json::Value::as_str)
        })
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "persisted file descriptor is missing file_path/location.path".to_string()
        })?;
    let normalized_path = normalize_lexical_path(raw_path)?;
    let managed_location = location
        .and_then(|value| value.get("type"))
        .and_then(serde_json::Value::as_str)
        == Some("managed");
    let root = descriptor_field(descriptor, file, "persist_data_root")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty());
    if let Some(root) = root {
        let root = normalize_lexical_path(root)?;
        if let Some(relative) = strip_normalized_root(&normalized_path, &root) {
            if relative.is_empty() {
                return Err("persisted file path resolves to the data root".to_string());
            }
            return Ok(relative.to_string());
        }
        if !normalized_path.starts_with('/') {
            return Ok(normalized_path);
        }
    }
    if managed_location && normalized_path.starts_with('/') {
        return Err("managed persisted path must remain relative".to_string());
    }
    Ok(normalized_path)
}

fn normalize_lexical_path(value: &str) -> std::result::Result<String, String> {
    let normalized_separators = value.replace('\\', "/");
    let absolute = normalized_separators.starts_with('/');
    let mut parts = Vec::new();
    for part in normalized_separators.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    return Err(format!("persisted path escapes its root path=[{}]", value));
                }
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        return Err(format!(
            "persisted path is empty after normalization path=[{}]",
            value
        ));
    }
    let joined = parts.join("/");
    if absolute {
        Ok(format!("/{joined}"))
    } else {
        Ok(joined)
    }
}

fn strip_normalized_root<'a>(path: &'a str, root: &str) -> Option<&'a str> {
    if path == root {
        return Some("");
    }
    path.strip_prefix(root)?.strip_prefix('/')
}

fn canonical_source_segments(
    descriptor: &serde_json::Value,
    file: &serde_json::Value,
) -> Vec<CanonicalSourceSegment> {
    let mut segments = descriptor_field(descriptor, file, "source_segments")
        .and_then(serde_json::Value::as_array)
        .map(|segments| {
            segments
                .iter()
                .filter_map(canonical_source_segment)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if segments.is_empty() {
        let provenance = descriptor_field(descriptor, file, "provenance");
        let lineage = provenance
            .and_then(|value| value.get("source_lineage"))
            .and_then(serde_json::Value::as_array);
        if let Some(lineage) = lineage {
            segments.extend(
                lineage.iter().filter_map(|item| {
                    canonical_source_segment(item.get("source").unwrap_or(item))
                }),
            );
        }
    }
    if segments.is_empty() {
        if let Some(segment) =
            canonical_source_segment(file).or_else(|| canonical_source_segment(descriptor))
        {
            segments.push(segment);
        }
    }
    segments.sort_unstable();
    segments.dedup();
    segments
}

fn canonical_source_segment(value: &serde_json::Value) -> Option<CanonicalSourceSegment> {
    let segment_id = value
        .get("source_segment_id")
        .or_else(|| value.get("segment_id"))
        .and_then(json_u64);
    let generation = value
        .get("source_generation")
        .or_else(|| value.get("generation"))
        .and_then(json_u64);
    let writer_epoch = value.get("writer_epoch").and_then(json_u64);
    if segment_id.is_none() && generation.is_none() && writer_epoch.is_none() {
        return None;
    }
    Some(CanonicalSourceSegment {
        segment_id,
        generation,
        writer_epoch,
    })
}

fn canonical_logical_offsets(
    descriptor: &serde_json::Value,
    file: &serde_json::Value,
) -> Option<CanonicalOffsetRange> {
    let provenance = descriptor_field(descriptor, file, "provenance");
    let nested_offsets = provenance.and_then(|value| value.get("offsets"));
    if let Some(offsets) = nested_offsets {
        return canonical_offset_range(offsets);
    }
    let first = [
        "first_log_offset",
        "logical_offset_start",
        "min_log_offset",
        "first_offset",
    ]
    .into_iter()
    .find_map(|name| descriptor_field(descriptor, file, name).and_then(json_u64));
    let last = [
        "last_log_offset",
        "logical_offset_end",
        "max_log_offset",
        "last_offset",
    ]
    .into_iter()
    .find_map(|name| descriptor_field(descriptor, file, name).and_then(json_u64));
    if first.is_none() && last.is_none() {
        None
    } else {
        Some(CanonicalOffsetRange { first, last })
    }
}

fn canonical_offset_range(value: &serde_json::Value) -> Option<CanonicalOffsetRange> {
    let first = value
        .get("first")
        .or_else(|| value.get("start"))
        .and_then(json_u64);
    let last = value
        .get("last")
        .or_else(|| value.get("end"))
        .and_then(json_u64);
    if first.is_none() && last.is_none() {
        None
    } else {
        Some(CanonicalOffsetRange { first, last })
    }
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn normalize_metadata_evidence(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(normalize_metadata_evidence).collect())
        }
        serde_json::Value::Object(object) => {
            let scalar_type = object.get("type").and_then(serde_json::Value::as_str);
            if matches!(
                scalar_type,
                Some("null" | "boolean" | "int64" | "uint64" | "float64" | "string" | "bytes")
            ) {
                return object
                    .get("value")
                    .map(normalize_metadata_evidence)
                    .unwrap_or(serde_json::Value::Null);
            }
            serde_json::Value::Object(
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), normalize_metadata_evidence(value)))
                    .collect(),
            )
        }
        value => value.clone(),
    }
}

fn persisted_read_mismatch_kind(
    primary: &CanonicalPersistedReadSnapshot,
    shadow: &CanonicalPersistedReadSnapshot,
) -> Option<PersistedReadShadowMismatchKind> {
    if primary.stream_name != shadow.stream_name {
        return Some(PersistedReadShadowMismatchKind::StreamName);
    }
    if primary.schema_hash != shadow.schema_hash {
        return Some(PersistedReadShadowMismatchKind::SchemaHash);
    }
    if primary.files.len() != shadow.files.len() {
        return Some(PersistedReadShadowMismatchKind::FileCount);
    }
    if primary
        .files
        .iter()
        .zip(&shadow.files)
        .all(|(primary, shadow)| canonical_files_match(primary, shadow))
    {
        return None;
    }
    let mut primary_order = primary.files.iter().collect::<Vec<_>>();
    let mut shadow_order = shadow.files.iter().collect::<Vec<_>>();
    primary_order.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    shadow_order.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    if primary_order
        .iter()
        .zip(&shadow_order)
        .all(|(primary, shadow)| canonical_files_match(primary, shadow))
    {
        Some(PersistedReadShadowMismatchKind::FileOrder)
    } else {
        Some(PersistedReadShadowMismatchKind::FileMetadata)
    }
}

fn canonical_files_match(
    primary: &CanonicalPersistedFile,
    shadow: &CanonicalPersistedFile,
) -> bool {
    primary.file_path == shadow.file_path
        && optional_evidence_matches(primary.row_count.as_ref(), shadow.row_count.as_ref())
        && (primary.source_segments.is_empty()
            || shadow.source_segments.is_empty()
            || primary.source_segments == shadow.source_segments)
        && optional_evidence_matches(
            primary.logical_offsets.as_ref(),
            shadow.logical_offsets.as_ref(),
        )
        && optional_json_evidence_matches(primary.partition.as_ref(), shadow.partition.as_ref())
        && optional_json_evidence_matches(primary.statistics.as_ref(), shadow.statistics.as_ref())
}

fn optional_evidence_matches<T: PartialEq>(primary: Option<&T>, shadow: Option<&T>) -> bool {
    match (primary, shadow) {
        (Some(primary), Some(shadow)) => primary == shadow,
        _ => true,
    }
}

fn optional_json_evidence_matches(
    primary: Option<&serde_json::Value>,
    shadow: Option<&serde_json::Value>,
) -> bool {
    match (primary, shadow) {
        (Some(primary), Some(shadow)) => json_evidence_matches(primary, shadow),
        _ => true,
    }
}

fn json_evidence_matches(primary: &serde_json::Value, shadow: &serde_json::Value) -> bool {
    match (primary, shadow) {
        (serde_json::Value::Object(primary), serde_json::Value::Object(shadow)) => primary
            .iter()
            .filter_map(|(key, primary)| shadow.get(key).map(|shadow| (primary, shadow)))
            .all(|(primary, shadow)| json_evidence_matches(primary, shadow)),
        _ => primary == shadow,
    }
}

fn canonical_snapshot_sha256(snapshot: &CanonicalPersistedReadSnapshot) -> String {
    serde_json::to_vec(snapshot)
        .map(|metadata| sha256_hex(&metadata))
        .unwrap_or_else(|_| sha256_hex(&[]))
}

fn finish_shadow_job(
    shared: &PersistedReadShadowShared,
    job: PersistedReadShadowJob,
    outcome: PersistedReadShadowOutcome,
) {
    let mut state = shared.state.lock().unwrap();
    state.comparisons_active = state.comparisons_active.saturating_sub(1);
    state.outstanding_metadata_bytes = state
        .outstanding_metadata_bytes
        .saturating_sub(job.reserved_metadata_bytes);
    state.in_flight_keys.remove(&job.key);
    let retain_dedup_key = match outcome {
        PersistedReadShadowOutcome::Match => {
            state.metrics.matches_total += 1;
            true
        }
        PersistedReadShadowOutcome::Mismatch(mismatch) => {
            state.metrics.mismatches_total += 1;
            state.last_mismatch = Some(mismatch);
            true
        }
        PersistedReadShadowOutcome::ShadowError(error) => {
            state.metrics.shadow_errors_total += 1;
            state.last_error = Some(bounded_status_error(error));
            false
        }
        PersistedReadShadowOutcome::EncodeError(error) => {
            state.metrics.metadata_encode_errors_total += 1;
            state.last_error = Some(bounded_status_error(error));
            false
        }
        PersistedReadShadowOutcome::NormalizeError(error) => {
            state.metrics.metadata_normalization_errors_total += 1;
            state.last_error = Some(bounded_status_error(error));
            false
        }
        PersistedReadShadowOutcome::FileLimitExceeded => {
            state.metrics.comparison_file_limit_exhausted_total += 1;
            state.last_error = Some(format!(
                "persisted read shadow file count exceeds per-comparison limit maximum=[{}]",
                shared.config.max_files_per_comparison
            ));
            false
        }
        PersistedReadShadowOutcome::MetadataLimitExceeded => {
            state.metrics.comparison_metadata_limit_exhausted_total += 1;
            state.last_error = Some(format!(
                "persisted read shadow metadata exceeds per-comparison limit maximum=[{}]",
                shared.config.max_metadata_bytes_per_comparison
            ));
            false
        }
        PersistedReadShadowOutcome::Cancelled => {
            state.metrics.shadow_cancellations_total += 1;
            false
        }
        PersistedReadShadowOutcome::TimedOut => {
            state.metrics.shadow_timeouts_total += 1;
            state.last_error = Some(format!(
                "persisted read shadow comparison timed out timeout_ms=[{}]",
                shared.config.comparison_timeout_ms
            ));
            false
        }
        PersistedReadShadowOutcome::ShadowPanic => {
            state.metrics.shadow_panics_total += 1;
            state.last_error = Some("persisted read shadow source panicked".to_string());
            false
        }
    };
    if retain_dedup_key && state.completed_keys.insert(job.key) {
        state.completed_order.push_back(job.key);
        while state.completed_order.len() > shared.config.dedup_capacity {
            if let Some(expired) = state.completed_order.pop_front() {
                state.completed_keys.remove(&expired);
            }
        }
    }
    shared.idle.notify_all();
}

fn bounded_status_error(mut error: String) -> String {
    if error.len() <= MAX_SHADOW_STATUS_ERROR_BYTES {
        return error;
    }
    let mut truncate_at = MAX_SHADOW_STATUS_ERROR_BYTES;
    while !error.is_char_boundary(truncate_at) {
        truncate_at -= 1;
    }
    error.truncate(truncate_at);
    error
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{self, Sender};

    use serde_json::{json, Value};

    use super::*;

    struct FixedPrimarySource {
        calls: AtomicUsize,
        files_by_stream: Mutex<VecDeque<Vec<Value>>>,
    }

    impl FixedPrimarySource {
        fn new(files_by_stream: impl IntoIterator<Item = Vec<Value>>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                files_by_stream: Mutex::new(files_by_stream.into_iter().collect()),
            }
        }
    }

    impl PersistedReadSnapshotSource for FixedPrimarySource {
        fn snapshot(&self, legacy_stream: &StreamInfo) -> Result<PersistedReadSnapshot> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let files = self
                .files_by_stream
                .lock()
                .unwrap()
                .pop_front()
                .expect("test primary response must exist");
            Ok(PersistedReadSnapshot::new(
                legacy_stream.stream_name.clone(),
                files,
            ))
        }
    }

    struct BlockingShadowSource {
        started: Sender<(String, u64)>,
        release: Arc<tokio::sync::Semaphore>,
        responses: Mutex<VecDeque<Result<PersistedReadShadowSnapshot>>>,
    }

    impl PersistedReadShadowSource for BlockingShadowSource {
        fn snapshot<'a>(
            &'a self,
            stream_name: &str,
            persisted_files_revision: u64,
        ) -> Pin<Box<dyn Future<Output = Result<PersistedReadShadowSnapshot>> + Send + 'a>>
        {
            self.started
                .send((stream_name.to_string(), persisted_files_revision))
                .unwrap();
            let release = Arc::clone(&self.release);
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("test shadow response must exist");
            Box::pin(async move {
                let permit = release.acquire_owned().await.unwrap();
                permit.forget();
                response
            })
        }
    }

    struct ImmediateShadowSource {
        calls: AtomicUsize,
        responses: Mutex<VecDeque<Result<PersistedReadShadowSnapshot>>>,
    }

    struct BlockingPollShadowSource {
        started: Sender<()>,
        delay: Duration,
        response: PersistedReadShadowSnapshot,
    }

    impl PersistedReadShadowSource for BlockingPollShadowSource {
        fn snapshot<'a>(
            &'a self,
            _stream_name: &str,
            _persisted_files_revision: u64,
        ) -> Pin<Box<dyn Future<Output = Result<PersistedReadShadowSnapshot>> + Send + 'a>>
        {
            let started = self.started.clone();
            let delay = self.delay;
            let response = self.response.clone();
            Box::pin(async move {
                started.send(()).unwrap();
                std::thread::sleep(delay);
                Ok(response)
            })
        }
    }

    impl ImmediateShadowSource {
        fn new(responses: impl IntoIterator<Item = Result<PersistedReadShadowSnapshot>>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }
    }

    impl PersistedReadShadowSource for ImmediateShadowSource {
        fn snapshot<'a>(
            &'a self,
            _stream_name: &str,
            _persisted_files_revision: u64,
        ) -> Pin<Box<dyn Future<Output = Result<PersistedReadShadowSnapshot>> + Send + 'a>>
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("test shadow response must exist");
            Box::pin(async move { response })
        }
    }

    fn stream_info(stream_name: &str, persisted_files_revision: u64) -> StreamInfo {
        StreamInfo {
            stream_name: stream_name.to_string(),
            schema: json!({"fields": []}),
            schema_hash: "schema".to_string(),
            data_path: String::new(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: None,
            sealed_segments: Vec::new(),
            persisted_files: Vec::new(),
            persisted_files_revision,
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 0,
            frame_size: 0,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 0,
            reader_count: 0,
            status: "active".to_string(),
        }
    }

    fn one_file(path: &str) -> Vec<Value> {
        vec![json!({"file_path": path, "row_count": 10})]
    }

    fn shadow_snapshot(stream_name: &str, files: Vec<Value>) -> PersistedReadShadowSnapshot {
        PersistedReadShadowSnapshot::new("schema", PersistedReadSnapshot::new(stream_name, files))
    }

    fn tiny_config() -> PersistedReadShadowConfig {
        PersistedReadShadowConfig {
            queue_capacity: 1,
            metadata_budget_bytes: 4096,
            max_files_per_comparison: 8,
            max_metadata_bytes_per_comparison: 2048,
            comparison_timeout_ms: 1000,
            dedup_capacity: 4,
        }
    }

    #[test]
    fn primary_snapshot_returns_while_shadow_comparison_is_blocked() {
        let files = one_file("legacy.parquet");
        let primary = Arc::new(FixedPrimarySource::new([files.clone()]));
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let shadow = Arc::new(BlockingShadowSource {
            started: started_tx,
            release: Arc::clone(&release),
            responses: Mutex::new(
                [Ok(shadow_snapshot("ticks", files.clone()))]
                    .into_iter()
                    .collect(),
            ),
        });
        let source =
            ShadowPersistedReadSnapshotSource::new(primary.clone(), shadow, tiny_config()).unwrap();

        let snapshot = source.snapshot(&stream_info("ticks", 7)).unwrap();

        assert_eq!(snapshot, PersistedReadSnapshot::new("ticks", files));
        assert_eq!(primary.calls.load(Ordering::Relaxed), 1);
        assert_eq!(started_rx.recv().unwrap(), ("ticks".to_string(), 7));
        assert_eq!(source.status().comparisons_active, 1);
        release.add_permits(1);
        assert!(source.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(source.status().metrics.matches_total, 1);
    }

    #[test]
    fn completed_and_in_flight_metadata_are_deduplicated() {
        let files = one_file("same.parquet");
        let primary = Arc::new(FixedPrimarySource::new([files.clone(), files.clone()]));
        let shadow = Arc::new(ImmediateShadowSource::new([Ok(shadow_snapshot(
            "ticks",
            files.clone(),
        ))]));
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow.clone(), tiny_config()).unwrap();

        assert_eq!(
            source.snapshot(&stream_info("ticks", 3)).unwrap(),
            PersistedReadSnapshot::new("ticks", files.clone())
        );
        assert!(source.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(
            source.snapshot(&stream_info("ticks", 3)).unwrap(),
            PersistedReadSnapshot::new("ticks", files)
        );

        let status = source.status();
        assert_eq!(shadow.calls.load(Ordering::Relaxed), 1);
        assert_eq!(status.metrics.comparisons_enqueued_total, 1);
        assert_eq!(status.metrics.comparisons_deduplicated_total, 1);
        assert_eq!(status.completed_dedup_entries, 1);
    }

    #[test]
    fn full_queue_drops_shadow_jobs_without_changing_primary_results() {
        let first_files = one_file("first.parquet");
        let second_files = one_file("second.parquet");
        let third_files = one_file("third.parquet");
        let primary = Arc::new(FixedPrimarySource::new([
            first_files.clone(),
            second_files.clone(),
            third_files.clone(),
        ]));
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let shadow = Arc::new(BlockingShadowSource {
            started: started_tx,
            release: Arc::clone(&release),
            responses: Mutex::new(
                [
                    Ok(shadow_snapshot("first", first_files.clone())),
                    Ok(shadow_snapshot("second", second_files.clone())),
                ]
                .into_iter()
                .collect(),
            ),
        });
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow, tiny_config()).unwrap();

        let first = source.snapshot(&stream_info("first", 1)).unwrap();
        assert_eq!(started_rx.recv().unwrap(), ("first".to_string(), 1));
        let second = source.snapshot(&stream_info("second", 2)).unwrap();
        let third = source.snapshot(&stream_info("third", 3)).unwrap();

        assert_eq!(first.persisted_files(), first_files.as_slice());
        assert_eq!(second.persisted_files(), second_files.as_slice());
        assert_eq!(third.persisted_files(), third_files.as_slice());
        assert_eq!(source.status().metrics.queue_full_total, 1);
        release.add_permits(1);
        assert_eq!(started_rx.recv().unwrap(), ("second".to_string(), 2));
        release.add_permits(1);
        assert!(source.wait_for_idle(Duration::from_secs(1)));
        assert_eq!(source.status().metrics.matches_total, 2);
    }

    #[test]
    fn per_comparison_byte_limit_drops_shadow_job_without_changing_primary_result() {
        let files = one_file("larger-than-one-byte.parquet");
        let primary = Arc::new(FixedPrimarySource::new([files.clone()]));
        let shadow = Arc::new(ImmediateShadowSource::new(Vec::<
            Result<PersistedReadShadowSnapshot>,
        >::new()));
        let source = ShadowPersistedReadSnapshotSource::new(
            primary,
            shadow.clone(),
            PersistedReadShadowConfig {
                queue_capacity: 1,
                metadata_budget_bytes: 1,
                max_files_per_comparison: 1,
                max_metadata_bytes_per_comparison: 1,
                comparison_timeout_ms: 1000,
                dedup_capacity: 1,
            },
        )
        .unwrap();

        let snapshot = source.snapshot(&stream_info("ticks", 1)).unwrap();

        assert_eq!(snapshot.persisted_files(), files.as_slice());
        assert_eq!(shadow.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            source
                .status()
                .metrics
                .comparison_metadata_limit_exhausted_total,
            1
        );
    }

    #[test]
    fn per_comparison_file_limit_drops_shadow_job_without_calling_native() {
        let files = [one_file("first.parquet"), one_file("second.parquet")].concat();
        let primary = Arc::new(FixedPrimarySource::new([files.clone()]));
        let shadow = Arc::new(ImmediateShadowSource::new(Vec::<
            Result<PersistedReadShadowSnapshot>,
        >::new()));
        let source = ShadowPersistedReadSnapshotSource::new(
            primary,
            shadow.clone(),
            PersistedReadShadowConfig {
                max_files_per_comparison: 1,
                ..tiny_config()
            },
        )
        .unwrap();

        let snapshot = source.snapshot(&stream_info("ticks", 1)).unwrap();

        assert_eq!(snapshot.persisted_files(), files.as_slice());
        assert_eq!(shadow.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            source
                .status()
                .metrics
                .comparison_file_limit_exhausted_total,
            1
        );
    }

    #[test]
    fn outstanding_metadata_budget_rejects_second_job_while_first_is_active() {
        let files = one_file("budget.parquet");
        let encoded_bytes = serde_json::to_vec(&files).unwrap().len();
        let reserved_bytes = encoded_bytes + "first".len() * 2 + "schema".len();
        let primary = Arc::new(FixedPrimarySource::new([files.clone(), files.clone()]));
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let shadow = Arc::new(BlockingShadowSource {
            started: started_tx,
            release: Arc::clone(&release),
            responses: Mutex::new(
                [Ok(shadow_snapshot("first", files.clone()))]
                    .into_iter()
                    .collect(),
            ),
        });
        let source = ShadowPersistedReadSnapshotSource::new(
            primary,
            shadow,
            PersistedReadShadowConfig {
                queue_capacity: 1,
                metadata_budget_bytes: reserved_bytes,
                max_files_per_comparison: 1,
                max_metadata_bytes_per_comparison: encoded_bytes,
                comparison_timeout_ms: 1000,
                dedup_capacity: 2,
            },
        )
        .unwrap();

        source.snapshot(&stream_info("first", 1)).unwrap();
        assert_eq!(started_rx.recv().unwrap(), ("first".to_string(), 1));
        let second = source.snapshot(&stream_info("other", 1)).unwrap();

        assert_eq!(second.persisted_files(), files.as_slice());
        assert_eq!(source.status().metrics.metadata_budget_exhausted_total, 1);
        release.add_permits(1);
        assert!(source.wait_for_idle(Duration::from_secs(1)));
    }

    #[test]
    fn canonical_comparison_ignores_volatile_fields_and_normalizes_native_shape() {
        let primary_files = vec![json!({
            "persist_file_id": "legacy-id",
            "file_path": "/data/./part.parquet",
            "persist_data_root": "/data",
            "row_count": 10,
            "source_segment_id": 4,
            "source_generation": 2,
            "writer_epoch": 8,
            "logical_offset_start": 11,
            "logical_offset_end": 20,
            "partition": {"instrument_id": "IF2606"},
            "stats": {"dt": {"min": 1, "max": 2}},
            "created_at": 100,
            "checksum": "legacy-checksum"
        })];
        let native_files = vec![json!({
            "file_id": "native-id",
            "location": {"type": "managed", "path": "part.parquet"},
            "row_count": 10,
            "provenance": {
                "type": "native",
                "offsets": {"first": 11, "last": 20},
                "source_lineage": [{
                    "source": {"segment_id": 4, "generation": 2, "writer_epoch": 8},
                    "offsets": {"first": 11, "last": 20}
                }]
            },
            "partition": {
                "instrument_id": {"type": "string", "value": "IF2606"}
            },
            "statistics": {
                "dt": {
                    "min": {"type": "int64", "value": 1},
                    "max": {"type": "int64", "value": 2}
                },
                "zippy.parquet.row_group_count": {"type": "uint64", "value": 1}
            },
            "file_size_bytes": 999,
            "checksum": "sha256-v1:native",
            "created_at_ns": 200
        })];
        let primary = Arc::new(FixedPrimarySource::new([primary_files]));
        let shadow = Arc::new(ImmediateShadowSource::new([Ok(shadow_snapshot(
            "ticks",
            native_files,
        ))]));
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow, tiny_config()).unwrap();

        source.snapshot(&stream_info("ticks", 9)).unwrap();
        assert!(source.wait_for_idle(Duration::from_secs(1)));

        let status = source.status();
        assert_eq!(status.metrics.matches_total, 1);
        assert_eq!(status.metrics.mismatches_total, 0);
    }

    #[test]
    fn deterministic_file_order_mismatch_is_classified_separately() {
        let first = one_file("first.parquet");
        let second = one_file("second.parquet");
        let primary_files = [first.clone(), second.clone()].concat();
        let shadow_files = [second, first].concat();
        let primary = Arc::new(FixedPrimarySource::new([primary_files]));
        let shadow = Arc::new(ImmediateShadowSource::new([Ok(shadow_snapshot(
            "ticks",
            shadow_files,
        ))]));
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow, tiny_config()).unwrap();

        source.snapshot(&stream_info("ticks", 5)).unwrap();
        assert!(source.wait_for_idle(Duration::from_secs(1)));

        assert_eq!(
            source.status().last_mismatch.unwrap().kind,
            PersistedReadShadowMismatchKind::FileOrder
        );
    }

    #[test]
    fn drop_cancels_active_lookup_joins_worker_and_releases_shadow_source() {
        let files = one_file("blocked.parquet");
        let primary = Arc::new(FixedPrimarySource::new([files.clone()]));
        let (started_tx, started_rx) = mpsc::channel();
        let shadow = Arc::new(BlockingShadowSource {
            started: started_tx,
            release: Arc::new(tokio::sync::Semaphore::new(0)),
            responses: Mutex::new([Ok(shadow_snapshot("ticks", files))].into_iter().collect()),
        });
        let weak_shadow = Arc::downgrade(&shadow);
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow.clone(), tiny_config()).unwrap();
        drop(shadow);
        source.snapshot(&stream_info("ticks", 1)).unwrap();
        assert_eq!(started_rx.recv().unwrap(), ("ticks".to_string(), 1));

        drop(source);

        assert!(weak_shadow.upgrade().is_none());
    }

    #[test]
    fn drop_defers_an_uncooperative_shadow_poll_after_bounded_join_wait() {
        let files = one_file("blocked-poll.parquet");
        let primary = Arc::new(FixedPrimarySource::new([files.clone()]));
        let (started_tx, started_rx) = mpsc::channel();
        let shadow = Arc::new(BlockingPollShadowSource {
            started: started_tx,
            delay: Duration::from_millis(300),
            response: shadow_snapshot("ticks", files),
        });
        let weak_shadow = Arc::downgrade(&shadow);
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow.clone(), tiny_config()).unwrap();
        drop(shadow);
        source.snapshot(&stream_info("ticks", 1)).unwrap();
        started_rx.recv().expect("shadow poll started");

        let started = Instant::now();
        drop(source);
        assert!(started.elapsed() < Duration::from_millis(250));

        let deadline = Instant::now() + Duration::from_secs(1);
        while weak_shadow.upgrade().is_some() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(weak_shadow.upgrade().is_none());
    }

    #[test]
    fn shadow_error_and_mismatch_remain_diagnostic_only() {
        let first_files = one_file("legacy-first.parquet");
        let second_files = one_file("legacy-second.parquet");
        let primary = Arc::new(FixedPrimarySource::new([
            first_files.clone(),
            second_files.clone(),
        ]));
        let shadow = Arc::new(ImmediateShadowSource::new([
            Err(ZippyError::DataCorruption {
                reason: "native catalog unavailable".to_string(),
            }),
            Ok(shadow_snapshot("second", one_file("native-second.parquet"))),
        ]));
        let source =
            ShadowPersistedReadSnapshotSource::new(primary, shadow, tiny_config()).unwrap();

        let first = source.snapshot(&stream_info("first", 1)).unwrap();
        assert!(source.wait_for_idle(Duration::from_secs(1)));
        let second = source.snapshot(&stream_info("second", 2)).unwrap();
        assert!(source.wait_for_idle(Duration::from_secs(1)));

        assert_eq!(first.persisted_files(), first_files.as_slice());
        assert_eq!(second.persisted_files(), second_files.as_slice());
        let status = source.status();
        assert_eq!(status.metrics.shadow_errors_total, 1);
        assert_eq!(status.metrics.mismatches_total, 1);
        assert!(status
            .last_error
            .as_deref()
            .unwrap()
            .contains("native catalog unavailable"));
        let mismatch = status.last_mismatch.unwrap();
        assert_eq!(mismatch.kind, PersistedReadShadowMismatchKind::FileMetadata);
        assert_eq!(mismatch.stream_name, "second");
        assert_eq!(mismatch.persisted_files_revision, 2);
        assert_eq!(mismatch.primary_file_count, 1);
        assert_eq!(mismatch.shadow_file_count, 1);
    }

    #[test]
    fn invalid_shadow_bounds_are_rejected_before_worker_spawn() {
        let primary = Arc::new(FixedPrimarySource::new(Vec::<Vec<Value>>::new()));
        let shadow = Arc::new(ImmediateShadowSource::new(Vec::<
            Result<PersistedReadShadowSnapshot>,
        >::new()));

        let error = ShadowPersistedReadSnapshotSource::new(
            primary,
            shadow,
            PersistedReadShadowConfig {
                queue_capacity: 0,
                ..PersistedReadShadowConfig::default()
            },
        )
        .err()
        .expect("zero queue capacity must fail");

        assert!(matches!(error, ZippyError::InvalidConfig { .. }));
    }
}
