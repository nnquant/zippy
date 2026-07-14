//! Bounded observation of committed legacy Catalog mutations.
//!
//! Events are fixed-capacity values and deliberately omit schemas and file manifests. Consumers
//! must treat any queue loss as a signal to run a full Catalog reconciliation.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use zippy_types::TableId;

use crate::legacy_catalog::legacy_table_id;

/// Maximum UTF-8 bytes retained for the diagnostic legacy stream name.
pub const LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY: usize = 96;

/// Origin of one committed legacy Catalog mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum LegacyCatalogCommitMutationKind {
    /// A previously absent legacy stream was registered.
    RegisterStream = 1,
    /// Source identity or writer-epoch metadata changed.
    RegisterSource = 2,
    /// A source and its table-definition metadata were removed.
    UnregisterSource = 3,
    /// One persisted-file descriptor was published.
    PublishPersistedFile = 4,
    /// The complete persisted-file descriptor set was replaced.
    ReplacePersistedFiles = 5,
    /// A table and its associated legacy records were removed.
    DropTable = 6,
    /// Background compaction replaced persisted-file descriptors.
    Compaction = 7,
    /// Persist-event history advanced without changing the native table manifest.
    PersistEventWatermark = 8,
}

/// Reason a commit publisher could not be installed on a Master server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyCatalogCommitPublisherInstallError {
    /// A publisher was already installed.
    AlreadyInstalled,
    /// A listener has started, so changing observation cannot preserve the complete sequence.
    ListenerStarted,
}

impl fmt::Display for LegacyCatalogCommitPublisherInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyInstalled => {
                formatter.write_str("legacy Catalog commit publisher is already installed")
            }
            Self::ListenerStarted => formatter.write_str(
                "legacy Catalog commit publisher must be installed before listener startup",
            ),
        }
    }
}

impl std::error::Error for LegacyCatalogCommitPublisherInstallError {}

/// Reason Catalog mutation admission could not be drained before publisher seal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyCatalogCommitPublisherSealError {
    /// The supplied duration cannot be represented by the monotonic clock.
    DeadlineOverflow,
    /// Previously admitted mutations did not finish before the deadline.
    InFlightTimeout {
        /// Number of mutations still inside the admission boundary.
        in_flight: usize,
    },
}

impl fmt::Display for LegacyCatalogCommitPublisherSealError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineOverflow => {
                formatter.write_str("legacy Catalog commit seal deadline overflow")
            }
            Self::InFlightTimeout { in_flight } => write!(
                formatter,
                "legacy Catalog commit seal timed out in_flight=[{in_flight}]"
            ),
        }
    }
}

impl std::error::Error for LegacyCatalogCommitPublisherSealError {}

#[derive(Debug)]
pub(crate) struct LegacyCatalogMutationAdmission {
    open: AtomicBool,
    in_flight: AtomicUsize,
}

impl LegacyCatalogMutationAdmission {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            open: AtomicBool::new(true),
            in_flight: AtomicUsize::new(0),
        })
    }

    pub(crate) fn try_enter(self: &Arc<Self>) -> Option<LegacyCatalogMutationPermit> {
        if !self.open.load(Ordering::Acquire) {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if !self.open.load(Ordering::Acquire) {
            self.in_flight.fetch_sub(1, Ordering::Release);
            return None;
        }
        Some(LegacyCatalogMutationPermit {
            admission: Arc::clone(self),
        })
    }

    pub(crate) fn close_and_wait(
        &self,
        timeout: Duration,
    ) -> Result<(), LegacyCatalogCommitPublisherSealError> {
        self.open.store(false, Ordering::Release);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(LegacyCatalogCommitPublisherSealError::DeadlineOverflow)?;
        loop {
            let in_flight = self.in_flight.load(Ordering::Acquire);
            if in_flight == 0 {
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(LegacyCatalogCommitPublisherSealError::InFlightTimeout { in_flight });
            }
            thread::sleep(remaining.min(Duration::from_millis(1)));
        }
    }
}

pub(crate) struct LegacyCatalogMutationPermit {
    admission: Arc<LegacyCatalogMutationAdmission>,
}

impl Drop for LegacyCatalogMutationPermit {
    fn drop(&mut self) {
        self.admission.in_flight.fetch_sub(1, Ordering::Release);
    }
}

/// Fixed-capacity identity for a legacy stream represented as a Catalog table.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct LegacyCatalogCommitStreamIdentity {
    table_id: TableId,
    stream_name: [u8; LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY],
    stream_name_len: u8,
    stream_name_truncated: bool,
}

impl LegacyCatalogCommitStreamIdentity {
    fn new(stream_name: &str) -> Self {
        let mut retained_len = stream_name
            .len()
            .min(LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY);
        while !stream_name.is_char_boundary(retained_len) {
            retained_len -= 1;
        }
        let mut retained_name = [0_u8; LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY];
        retained_name[..retained_len].copy_from_slice(&stream_name.as_bytes()[..retained_len]);
        Self {
            table_id: legacy_table_id(stream_name),
            stream_name: retained_name,
            stream_name_len: retained_len as u8,
            stream_name_truncated: retained_len != stream_name.len(),
        }
    }

    /// Returns the stable legacy table identity derived from the complete stream name.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the retained UTF-8 stream name prefix for diagnostics.
    #[must_use]
    pub fn stream_name(&self) -> &str {
        std::str::from_utf8(&self.stream_name[..usize::from(self.stream_name_len)])
            .expect("legacy Catalog commit stream name is copied from valid UTF-8")
    }

    /// Returns whether the diagnostic stream name was truncated.
    #[must_use]
    pub const fn stream_name_truncated(&self) -> bool {
        self.stream_name_truncated
    }
}

impl fmt::Debug for LegacyCatalogCommitStreamIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LegacyCatalogCommitStreamIdentity")
            .field("table_id", &self.table_id)
            .field("stream_name", &self.stream_name())
            .field("stream_name_truncated", &self.stream_name_truncated)
            .finish()
    }
}

/// Fixed-capacity, fully owned notification for one committed legacy Catalog mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyCatalogCommitEvent {
    previous_revision: u64,
    committed_revision: u64,
    mutation_kind: LegacyCatalogCommitMutationKind,
    stream: Option<LegacyCatalogCommitStreamIdentity>,
}

impl LegacyCatalogCommitEvent {
    pub(crate) fn for_stream(
        previous_revision: u64,
        committed_revision: u64,
        mutation_kind: LegacyCatalogCommitMutationKind,
        stream_name: &str,
    ) -> Self {
        Self {
            previous_revision,
            committed_revision,
            mutation_kind,
            stream: Some(LegacyCatalogCommitStreamIdentity::new(stream_name)),
        }
    }

    /// Returns the revision immediately preceding this commit.
    #[must_use]
    pub const fn previous_revision(&self) -> u64 {
        self.previous_revision
    }

    /// Returns the committed legacy Catalog revision.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.committed_revision
    }

    /// Returns the committed legacy Catalog revision.
    #[must_use]
    pub const fn committed_revision(&self) -> u64 {
        self.committed_revision
    }

    /// Returns the mutation origin.
    #[must_use]
    pub const fn mutation_kind(&self) -> LegacyCatalogCommitMutationKind {
        self.mutation_kind
    }

    /// Returns the affected stream identity when the mutation is table-scoped.
    #[must_use]
    pub const fn stream(&self) -> Option<&LegacyCatalogCommitStreamIdentity> {
        self.stream.as_ref()
    }
}

/// Current counters and continuity state for a legacy Catalog commit publisher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LegacyCatalogCommitPublisherMetricsSnapshot {
    /// Number of publication attempts.
    pub attempts_total: u64,
    /// Number of events accepted by the bounded queue.
    pub published_total: u64,
    /// Number of events discarded because the queue was full.
    pub queue_full_total: u64,
    /// Number of events discarded because the receiver was disconnected.
    pub disconnected_total: u64,
    /// Number of Catalog revision changes that had no durable commit-event boundary.
    pub unpublished_mutation_total: u64,
    /// Number of attempts that collided with publisher seal/close activity.
    pub publisher_busy_total: u64,
    /// Revision atomically captured when the publisher was installed.
    pub baseline_revision: u64,
    /// Last revision accepted by the queue, or zero when none was accepted.
    pub last_published_revision: u64,
    /// Latest committed revision observed by the publisher, including events lost at admission.
    pub latest_committed_revision: u64,
    /// Latest Catalog revision seen, including non-durable legacy mutation edges.
    pub latest_observed_revision: u64,
    /// Last revision that could not be published, or zero when continuity is intact.
    pub last_lost_revision: u64,
    /// Monotonic count of queue-full or disconnected losses.
    pub continuity_loss_epoch: u64,
    /// Final Catalog revision captured when the publisher was sealed.
    pub final_revision: u64,
    /// Whether the sender has been closed and the receiver can drain to disconnection.
    pub sealed: bool,
    /// Sticky loss flag. Once true, only a full reconciliation can restore trust.
    pub continuity_lost: bool,
}

#[derive(Debug, Default)]
struct LegacyCatalogCommitPublisherState {
    attempts_total: AtomicU64,
    published_total: AtomicU64,
    queue_full_total: AtomicU64,
    disconnected_total: AtomicU64,
    unpublished_mutation_total: AtomicU64,
    publisher_busy_total: AtomicU64,
    baseline_revision: AtomicU64,
    last_published_revision: AtomicU64,
    latest_committed_revision: AtomicU64,
    latest_observed_revision: AtomicU64,
    last_lost_revision: AtomicU64,
    continuity_loss_epoch: AtomicU64,
    continuity_lost: AtomicBool,
    final_revision: AtomicU64,
    sealed: AtomicBool,
}

/// Cloneable metrics handle for a legacy Catalog commit publisher.
#[derive(Clone, Debug)]
pub struct LegacyCatalogCommitPublisherMetrics {
    state: Arc<LegacyCatalogCommitPublisherState>,
}

impl LegacyCatalogCommitPublisherMetrics {
    /// Captures the current counters without blocking the primary Master path.
    #[must_use]
    pub fn snapshot(&self) -> LegacyCatalogCommitPublisherMetricsSnapshot {
        LegacyCatalogCommitPublisherMetricsSnapshot {
            attempts_total: self.state.attempts_total.load(Ordering::Relaxed),
            published_total: self.state.published_total.load(Ordering::Relaxed),
            queue_full_total: self.state.queue_full_total.load(Ordering::Relaxed),
            disconnected_total: self.state.disconnected_total.load(Ordering::Relaxed),
            unpublished_mutation_total: self
                .state
                .unpublished_mutation_total
                .load(Ordering::Relaxed),
            publisher_busy_total: self.state.publisher_busy_total.load(Ordering::Relaxed),
            baseline_revision: self.state.baseline_revision.load(Ordering::Acquire),
            last_published_revision: self.state.last_published_revision.load(Ordering::Relaxed),
            latest_committed_revision: self.state.latest_committed_revision.load(Ordering::Acquire),
            latest_observed_revision: self.state.latest_observed_revision.load(Ordering::Acquire),
            last_lost_revision: self.state.last_lost_revision.load(Ordering::Relaxed),
            continuity_loss_epoch: self.state.continuity_loss_epoch.load(Ordering::Relaxed),
            final_revision: self.state.final_revision.load(Ordering::Acquire),
            sealed: self.state.sealed.load(Ordering::Acquire),
            continuity_lost: self.state.continuity_lost.load(Ordering::Acquire),
        }
    }
}

/// Non-blocking producer for a bounded legacy Catalog commit queue.
#[derive(Clone, Debug)]
pub struct LegacyCatalogCommitPublisher {
    sender: Arc<Mutex<Option<SyncSender<LegacyCatalogCommitEvent>>>>,
    state: Arc<LegacyCatalogCommitPublisherState>,
}

impl LegacyCatalogCommitPublisher {
    /// Creates a bounded queue and its single consumer.
    #[must_use]
    pub fn bounded(capacity: NonZeroUsize) -> (Self, Receiver<LegacyCatalogCommitEvent>) {
        let (sender, receiver) = mpsc::sync_channel(capacity.get());
        (
            Self {
                sender: Arc::new(Mutex::new(Some(sender))),
                state: Arc::new(LegacyCatalogCommitPublisherState::default()),
            },
            receiver,
        )
    }

    /// Returns a cloneable metrics handle that remains valid after publisher installation.
    #[must_use]
    pub fn metrics(&self) -> LegacyCatalogCommitPublisherMetrics {
        LegacyCatalogCommitPublisherMetrics {
            state: Arc::clone(&self.state),
        }
    }

    pub(crate) fn try_publish(&self, event: LegacyCatalogCommitEvent) {
        self.state.attempts_total.fetch_add(1, Ordering::Relaxed);
        self.state
            .latest_committed_revision
            .store(event.catalog_revision(), Ordering::Release);
        self.state
            .latest_observed_revision
            .store(event.catalog_revision(), Ordering::Release);
        let sender = match self.sender.try_lock() {
            Ok(sender) => sender,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.state
                    .publisher_busy_total
                    .fetch_add(1, Ordering::Relaxed);
                self.record_continuity_loss(event.catalog_revision());
                return;
            }
        };
        let Some(sender) = sender.as_ref() else {
            self.state
                .disconnected_total
                .fetch_add(1, Ordering::Relaxed);
            self.record_continuity_loss(event.catalog_revision());
            return;
        };
        match sender.try_send(event) {
            Ok(()) => {
                self.state.published_total.fetch_add(1, Ordering::Relaxed);
                self.state
                    .last_published_revision
                    .store(event.catalog_revision(), Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) => {
                self.state.queue_full_total.fetch_add(1, Ordering::Relaxed);
                self.record_continuity_loss(event.catalog_revision());
            }
            Err(TrySendError::Disconnected(_)) => {
                self.state
                    .disconnected_total
                    .fetch_add(1, Ordering::Relaxed);
                self.record_continuity_loss(event.catalog_revision());
            }
        }
    }

    pub(crate) fn record_unpublished_mutation(&self, catalog_revision: u64) {
        self.state
            .unpublished_mutation_total
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .latest_observed_revision
            .store(catalog_revision, Ordering::Release);
        self.record_continuity_loss(catalog_revision);
    }

    pub(crate) fn seed_baseline_revision(&self, catalog_revision: u64) {
        self.state
            .baseline_revision
            .store(catalog_revision, Ordering::Release);
        self.state
            .latest_committed_revision
            .store(catalog_revision, Ordering::Release);
        self.state
            .latest_observed_revision
            .store(catalog_revision, Ordering::Release);
    }

    pub(crate) fn seal(&self, final_revision: u64) {
        self.state
            .final_revision
            .store(final_revision, Ordering::Release);
        self.state.sealed.store(true, Ordering::Release);
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn record_continuity_loss(&self, catalog_revision: u64) {
        self.state
            .last_lost_revision
            .store(catalog_revision, Ordering::Relaxed);
        self.state
            .continuity_loss_epoch
            .fetch_add(1, Ordering::Relaxed);
        self.state.continuity_lost.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use super::{
        LegacyCatalogCommitEvent, LegacyCatalogCommitMutationKind, LegacyCatalogCommitPublisher,
        LegacyCatalogCommitPublisherSealError, LegacyCatalogMutationAdmission,
        LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY,
    };

    #[test]
    fn event_is_fixed_capacity_and_preserves_full_table_identity() {
        let name = format!(
            "{}中",
            "x".repeat(LEGACY_CATALOG_COMMIT_STREAM_NAME_CAPACITY - 1)
        );
        let event = LegacyCatalogCommitEvent::for_stream(
            6,
            7,
            LegacyCatalogCommitMutationKind::RegisterStream,
            &name,
        );
        let stream = event.stream().unwrap();

        assert!(stream.stream_name_truncated());
        assert!(stream
            .stream_name()
            .is_char_boundary(stream.stream_name().len()));
        assert!(!mem::needs_drop::<LegacyCatalogCommitEvent>());
        assert!(mem::size_of::<LegacyCatalogCommitEvent>() <= 160);
    }

    #[test]
    fn full_queue_latches_continuity_loss_without_blocking() {
        let (publisher, receiver) =
            LegacyCatalogCommitPublisher::bounded(NonZeroUsize::new(1).unwrap());
        publisher.try_publish(LegacyCatalogCommitEvent::for_stream(
            0,
            1,
            LegacyCatalogCommitMutationKind::RegisterStream,
            "events",
        ));
        publisher.try_publish(LegacyCatalogCommitEvent::for_stream(
            1,
            2,
            LegacyCatalogCommitMutationKind::RegisterSource,
            "events",
        ));

        assert_eq!(receiver.try_recv().unwrap().catalog_revision(), 1);
        let metrics = publisher.metrics().snapshot();
        assert_eq!(metrics.attempts_total, 2);
        assert_eq!(metrics.published_total, 1);
        assert_eq!(metrics.queue_full_total, 1);
        assert_eq!(metrics.latest_committed_revision, 2);
        assert_eq!(metrics.last_lost_revision, 2);
        assert_eq!(metrics.continuity_loss_epoch, 1);
        assert!(metrics.continuity_lost);
    }

    #[test]
    fn closing_mutation_admission_drains_existing_permits_and_rejects_new_work() {
        let admission = LegacyCatalogMutationAdmission::new();
        let permit = admission.try_enter().unwrap();

        assert_eq!(
            admission.close_and_wait(Duration::ZERO),
            Err(LegacyCatalogCommitPublisherSealError::InFlightTimeout { in_flight: 1 })
        );
        assert!(admission.try_enter().is_none());

        drop(permit);
        admission.close_and_wait(Duration::ZERO).unwrap();
    }
}
