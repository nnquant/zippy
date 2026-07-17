//! Bounded per-table serialization and lifecycle ownership.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use zippy_segment_store::{ActiveSegmentDescriptor, RowSpanView};
use zippy_storage::{
    CommitDataFilesReceipt, PersistCompletionFailure, PersistCompletionResult,
    PersistControllerHandle, PersistMode, PersistRequest, PersistTicket,
};
use zippy_types::table::{SnapshotOperation, SourceSegmentIdentity, TableKind, TableSnapshot};
use zippy_types::{
    ErrorCode, LogOffset, OffsetRange, OffsetWatermark, RequestId, Result, ServerError,
    ServerErrorDetails, TableId,
};

use crate::{TableRuntime, TableRuntimeRegistry};

const DEFAULT_PER_TABLE_MAILBOX_CAPACITY: usize = 64;
const DEFAULT_MAX_OUTSTANDING_COMMANDS: usize = 4_096;
const MAX_PER_TABLE_MAILBOX_CAPACITY: usize = 65_536;
const MAX_OUTSTANDING_COMMANDS: usize = 1_048_576;
const MAX_LIVE_SEGMENTS_PER_TABLE: usize = 1_024;

/// Bounded admission policy shared by all table actors in one server runtime.
#[derive(Debug, Clone)]
pub struct TableActorRegistryConfig {
    per_table_mailbox_capacity: usize,
    max_outstanding_commands: usize,
}

impl Default for TableActorRegistryConfig {
    fn default() -> Self {
        Self {
            per_table_mailbox_capacity: DEFAULT_PER_TABLE_MAILBOX_CAPACITY,
            max_outstanding_commands: DEFAULT_MAX_OUTSTANDING_COMMANDS,
        }
    }
}

impl TableActorRegistryConfig {
    /// Creates the default 64-command per-table and 4,096-command global policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum commands queued for any one table.
    #[must_use]
    pub const fn with_per_table_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.per_table_mailbox_capacity = capacity;
        self
    }

    /// Sets the maximum commands admitted across every table actor.
    #[must_use]
    pub const fn with_max_outstanding_commands(mut self, maximum: usize) -> Self {
        self.max_outstanding_commands = maximum;
        self
    }

    /// Validates all allocation bounds without starting a runtime or actor.
    pub fn validate(&self) -> Result<()> {
        validate_config(self)
    }
}

/// A compact observation taken in one table actor turn.
#[derive(Debug, Clone)]
pub struct TableActorSnapshot {
    /// The observed table identity.
    pub table_id: TableId,
    /// The current schema version bound to future native writes.
    pub schema_version: u64,
    /// The latest persisted snapshot observed by the actor.
    pub persisted_snapshot_version: u64,
    /// The current producer fencing epoch.
    pub writer_epoch: u64,
    /// The durable logical offset represented by the persisted snapshot.
    pub committed_log_offset: OffsetWatermark,
    /// Monotonic process-local command sequence assigned by this actor.
    pub actor_command_sequence: u64,
    /// The actor-owned persistence state observed in the same serialized turn.
    pub persistence_status: TablePersistenceStatus,
    /// Snapshot-pinned live segments after removing the current persisted prefix.
    pub live_segments: Arc<[TableActorLiveSegmentSnapshot]>,
    /// Greatest logical offset visible across the persisted and live snapshot sources.
    pub max_log_offset: OffsetWatermark,
}

/// Role of one shared-memory segment in an actor-owned live publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableActorLiveSegmentRole {
    /// A sealed segment still retained because persistence has not covered its full range.
    RetainedSealed,
    /// The current append-only segment whose committed prefix may continue growing.
    Active,
}

/// One immutable, capability-bearing live segment inside a table actor snapshot.
///
/// Query planning must project only its scalar identity and range fields. The underlying
/// [`RowSpanView`] is a server-local lease and must remain behind the execution authority.
#[derive(Debug, Clone)]
pub struct TableActorLiveSegmentSnapshot {
    role: TableActorLiveSegmentRole,
    identity: SourceSegmentIdentity,
    schema_version: u64,
    offsets: OffsetRange,
    span: RowSpanView,
}

impl TableActorLiveSegmentSnapshot {
    /// Returns whether this is retained sealed data or the current active prefix.
    #[must_use]
    pub const fn role(&self) -> TableActorLiveSegmentRole {
        self.role
    }

    /// Returns the exact producer segment identity.
    #[must_use]
    pub const fn identity(&self) -> SourceSegmentIdentity {
        self.identity
    }

    /// Returns the schema version encoded by the shared-memory segment.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Returns the contiguous logical offsets represented by the fixed row span.
    #[must_use]
    pub const fn offsets(&self) -> OffsetRange {
        self.offsets
    }

    /// Borrows the server-local snapshot capability.
    #[must_use]
    pub const fn span(&self) -> &RowSpanView {
        &self.span
    }
}

/// One shared-memory segment supplied to the actor by the native append writer.
#[derive(Debug)]
pub struct TableActorLiveSegmentPublication {
    role: TableActorLiveSegmentRole,
    identity: SourceSegmentIdentity,
    schema_version: u64,
    offsets: OffsetRange,
    descriptor: ActiveSegmentDescriptor,
    start_row: usize,
    end_row: usize,
}

impl TableActorLiveSegmentPublication {
    /// Creates one publication item whose full identity is revalidated in actor order.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        role: TableActorLiveSegmentRole,
        identity: SourceSegmentIdentity,
        schema_version: u64,
        offsets: OffsetRange,
        descriptor: ActiveSegmentDescriptor,
        start_row: usize,
        end_row: usize,
    ) -> Self {
        Self {
            role,
            identity,
            schema_version,
            offsets,
            descriptor,
            start_row,
            end_row,
        }
    }
}

/// Atomic replacement of every not-yet-persisted live segment for one append table.
#[derive(Debug)]
pub struct TableActorLiveSegmentsPublication {
    table_id: TableId,
    segments: Vec<TableActorLiveSegmentPublication>,
}

impl TableActorLiveSegmentsPublication {
    /// Creates a full actor publication. An empty vector explicitly clears live state.
    #[must_use]
    pub fn new(table_id: TableId, segments: Vec<TableActorLiveSegmentPublication>) -> Self {
        Self { table_id, segments }
    }

    /// Returns the target table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }
}

/// Observable state of one table's serialized native persistence pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TablePersistenceStatus {
    /// This registry was started through the read-only compatibility entrypoint.
    Disabled,
    /// No persist operation is currently active and the actor can accept one.
    Idle,
    /// One accepted operation is writing files or awaiting its Catalog receipt.
    InFlight {
        /// The idempotency identity of the active persist operation.
        request_id: RequestId,
    },
    /// A submitted operation failed or returned an invalid durable receipt.
    Failed {
        /// The idempotency identity of the operation that failed.
        request_id: RequestId,
        /// The stable error code retained by the actor.
        code: ErrorCode,
    },
}

/// One successfully admitted snapshot request awaiting its actor turn.
///
/// Dropping this value abandons only the response. The admitted actor command
/// still executes and releases its global budget permit.
#[derive(Debug)]
pub struct TableActorSnapshotRequest {
    table_id: TableId,
    response: oneshot::Receiver<Result<TableActorSnapshot>>,
}

impl TableActorSnapshotRequest {
    /// Waits for the serialized actor observation.
    pub async fn wait(self) -> Result<TableActorSnapshot> {
        self.response.await.unwrap_or_else(|_| {
            Err(ServerError::new(
                ErrorCode::Internal,
                "table actor stopped before returning an accepted snapshot request",
            )
            .with_table_id(self.table_id)
            .with_retryable(false))
        })
    }
}

/// One accepted live-segment replacement awaiting its serialized actor turn.
#[derive(Debug)]
pub struct TableActorLiveSegmentsPublishRequest {
    table_id: TableId,
    response: oneshot::Receiver<Result<()>>,
}

impl TableActorLiveSegmentsPublishRequest {
    /// Waits until the actor has attached, validated, and atomically installed the publication.
    pub async fn wait(self) -> Result<()> {
        self.response.await.unwrap_or_else(|_| {
            Err(ServerError::new(
                ErrorCode::Internal,
                "table actor stopped before returning an accepted live segment publication",
            )
            .with_table_id(self.table_id)
            .with_retryable(false))
        })
    }
}

/// One admitted persist request whose completion remains owned by its table actor.
///
/// Dropping this waiter abandons only the response. The actor still waits for
/// the controller ticket, validates its durable receipt, and updates its local
/// snapshot state before admitting another persist operation.
#[derive(Debug)]
pub struct TableActorPersistRequest {
    request_id: RequestId,
    table_id: TableId,
    response: oneshot::Receiver<Result<CommitDataFilesReceipt>>,
}

impl TableActorPersistRequest {
    /// Returns the idempotency identity of the admitted operation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Waits for the actor-validated durable Catalog receipt.
    pub async fn wait(self) -> Result<CommitDataFilesReceipt> {
        self.response.await.unwrap_or_else(|_| {
            Err(ServerError::new(
                ErrorCode::Internal,
                "table actor stopped before returning an accepted persist request",
            )
            .with_request_id(self.request_id)
            .with_table_id(self.table_id)
            .with_retryable(false))
        })
    }
}

/// Cloneable admission handle for one server-owned table actor.
#[derive(Clone, Debug)]
pub struct TableActorHandle {
    table_id: TableId,
    sender: mpsc::Sender<TableActorCommand>,
    accepting: Arc<AtomicBool>,
    global_budget: Arc<Semaphore>,
    persist_budget: Arc<Semaphore>,
    persistence_enabled: bool,
}

impl TableActorHandle {
    /// Returns the immutable table identity served by this actor.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Attempts immediate bounded admission without waiting for queue capacity.
    pub fn try_snapshot(&self) -> Result<TableActorSnapshotRequest> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(actor_closed(self.table_id));
        }
        let permit = Arc::clone(&self.global_budget)
            .try_acquire_owned()
            .map_err(|_| actor_global_budget_exhausted(self.table_id))?;
        let (response_sender, response) = oneshot::channel();
        let command = TableActorCommand::Snapshot {
            response: response_sender,
            _permit: permit,
        };
        match self.sender.try_send(command) {
            Ok(()) => Ok(TableActorSnapshotRequest {
                table_id: self.table_id,
                response,
            }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(actor_mailbox_full(self.table_id)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(actor_closed(self.table_id)),
        }
    }

    /// Admits and waits for one serialized actor observation.
    pub async fn snapshot(&self) -> Result<TableActorSnapshot> {
        self.try_snapshot()?.wait().await
    }

    /// Attempts to atomically replace this append table's complete live segment set.
    ///
    /// The actor reopens every descriptor and pins its payload version before publishing any
    /// segment. Malformed or partially attachable replacements leave the previous set unchanged.
    pub fn try_publish_live_segments(
        &self,
        publication: TableActorLiveSegmentsPublication,
    ) -> Result<TableActorLiveSegmentsPublishRequest> {
        if publication.table_id() != self.table_id {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "live segment publication targets another table actor",
            )
            .with_table_id(self.table_id)
            .with_retryable(false));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(actor_closed(self.table_id));
        }
        let permit = Arc::clone(&self.global_budget)
            .try_acquire_owned()
            .map_err(|_| actor_global_budget_exhausted(self.table_id))?;
        let (response_sender, response) = oneshot::channel();
        let command = TableActorCommand::PublishLiveSegments {
            publication,
            response: response_sender,
            _permit: permit,
        };
        match self.sender.try_send(command) {
            Ok(()) => Ok(TableActorLiveSegmentsPublishRequest {
                table_id: self.table_id,
                response,
            }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(actor_mailbox_full(self.table_id)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(actor_closed(self.table_id)),
        }
    }

    /// Publishes and waits for one actor-validated live segment replacement.
    pub async fn publish_live_segments(
        &self,
        publication: TableActorLiveSegmentsPublication,
    ) -> Result<()> {
        self.try_publish_live_segments(publication)?.wait().await
    }

    /// Attempts immediate admission of one atomic native persist operation.
    ///
    /// Admission synchronously reserves both this table's single-flight slot
    /// and the registry-wide command budget before touching the actor mailbox.
    /// A second operation for the same table therefore returns retryable
    /// [`ErrorCode::ResourceExhausted`] before any candidate file can be written.
    pub fn try_persist(&self, request: PersistRequest) -> Result<TableActorPersistRequest> {
        let request_id = request.request_id();
        if request.table_id() != self.table_id {
            return Err(persist_table_mismatch(
                self.table_id,
                request.table_id(),
                request_id,
            ));
        }
        if !self.persistence_enabled {
            return Err(persistence_disabled(self.table_id, request_id));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(actor_closed(self.table_id).with_request_id(request_id));
        }
        let persist_permit = Arc::clone(&self.persist_budget)
            .try_acquire_owned()
            .map_err(|_| table_persist_inflight(self.table_id, request_id))?;
        let global_permit = Arc::clone(&self.global_budget)
            .try_acquire_owned()
            .map_err(|_| {
                actor_global_budget_exhausted(self.table_id).with_request_id(request_id)
            })?;
        let (response_sender, response) = oneshot::channel();
        let command = TableActorCommand::Persist {
            request,
            response: response_sender,
            _persist_permit: persist_permit,
            _global_permit: global_permit,
        };
        match self.sender.try_send(command) {
            Ok(()) => Ok(TableActorPersistRequest {
                request_id,
                table_id: self.table_id,
                response,
            }),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(actor_mailbox_full(self.table_id).with_request_id(request_id))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(actor_closed(self.table_id).with_request_id(request_id))
            }
        }
    }

    /// Admits and waits for one actor-validated native persist operation.
    pub async fn persist(&self, request: PersistRequest) -> Result<CommitDataFilesReceipt> {
        self.try_persist(request)?.wait().await
    }
}

/// Cloneable lookup surface published after every table actor has started.
#[derive(Clone)]
pub struct TableActorRegistryHandle {
    catalog_revision: u64,
    tables: Arc<BTreeMap<TableId, TableActorHandle>>,
    accepting: Arc<AtomicBool>,
}

/// One observer for the first abnormal table actor termination in a registry.
///
/// Normal shutdown does not publish a failure. A returned error means global
/// actor admission has already been closed and the owning runtime must begin
/// ordered shutdown before advertising native ingest as healthy again.
pub struct TableActorFailureMonitor {
    receiver: watch::Receiver<Option<ServerError>>,
}

impl TableActorFailureMonitor {
    /// Waits until an actor returns an error, panics, or terminates unexpectedly.
    pub async fn wait_for_failure(&mut self) -> ServerError {
        loop {
            if let Some(error) = self.receiver.borrow().clone() {
                return error;
            }
            if self.receiver.changed().await.is_err() {
                return ServerError::new(
                    ErrorCode::Internal,
                    "table actor registry stopped without publishing a terminal outcome",
                )
                .with_retryable(false);
            }
        }
    }
}

/// Non-cloneable composition-root control for immediately closing actor admission.
///
/// This value is deliberately not cloneable and is not carried by normal table
/// handles. The server lifecycle owner uses it to make `begin_shutdown`
/// synchronous from the perspective of new command admission.
pub struct TableActorAdmissionControl {
    accepting: Arc<AtomicBool>,
}

impl TableActorAdmissionControl {
    /// Irreversibly rejects all future commands across the actor registry.
    pub fn close(&self) {
        self.accepting.store(false, Ordering::Release);
    }
}

impl TableActorRegistryHandle {
    /// Returns the Catalog revision shared by all actor bootstrap states.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the number of table actors published atomically at startup.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns whether no active table actor was recovered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Returns whether this registry still admits actor commands.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Resolves a table actor by immutable identity.
    pub fn table(&self, table_id: TableId) -> Result<TableActorHandle> {
        self.tables.get(&table_id).cloned().ok_or_else(|| {
            ServerError::new(ErrorCode::NotFound, "native table actor was not found")
                .with_table_id(table_id)
                .with_retryable(false)
        })
    }
}

/// Exclusive lifecycle owner for all table actors in one native storage runtime.
///
/// The owner is intentionally not cloneable. [`TableActorRegistryHandle`] may
/// be shared with service adapters, while only this value can close admission
/// and join every actor during ordered server shutdown.
pub struct TableActorRegistry {
    handle: TableActorRegistryHandle,
    shutdown: watch::Sender<bool>,
    actors: Vec<(TableId, JoinHandle<Result<()>>)>,
    failure: watch::Receiver<Option<ServerError>>,
    _failure_sender: watch::Sender<Option<ServerError>>,
    shutdown_started: bool,
}

impl TableActorRegistry {
    /// Starts one bounded read-only actor for every bootstrapped active table.
    ///
    /// This compatibility entrypoint preserves startup introspection without
    /// granting actors access to native persistence. Production servers should
    /// use [`Self::start_with_persistence`].
    pub fn start(config: TableActorRegistryConfig, runtimes: TableRuntimeRegistry) -> Result<Self> {
        Self::start_internal(config, runtimes, None)
    }

    /// Starts one persistence-capable serial actor for every active table.
    ///
    /// The controller handle is retained only behind the actors. Normal table
    /// handles can submit bounded requests but cannot close or drain the shared
    /// persistence controller.
    pub fn start_with_persistence(
        config: TableActorRegistryConfig,
        runtimes: TableRuntimeRegistry,
        persistence: PersistControllerHandle,
    ) -> Result<Self> {
        Self::start_internal(config, runtimes, Some(Arc::new(persistence)))
    }

    fn start_internal(
        config: TableActorRegistryConfig,
        runtimes: TableRuntimeRegistry,
        persistence: Option<Arc<dyn PersistSubmitter>>,
    ) -> Result<Self> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("start table actors outside a Tokio runtime error=[{error}]"),
            )
            .with_retryable(false)
        })?;
        let accepting = Arc::new(AtomicBool::new(true));
        let global_budget = Arc::new(Semaphore::new(config.max_outstanding_commands));
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (failure, failure_receiver) = watch::channel(None);
        let catalog_revision = runtimes.catalog_revision();
        let mut tables = BTreeMap::new();
        let mut actors = Vec::with_capacity(runtimes.len());
        let persistence_enabled = persistence.is_some();

        for runtime in runtimes.iter() {
            let table_id = runtime.table_id();
            let (sender, receiver) = mpsc::channel(config.per_table_mailbox_capacity);
            let actor = tokio::spawn(run_supervised_table_actor(
                Arc::clone(runtime),
                receiver,
                shutdown_receiver.clone(),
                persistence.as_ref().map(Arc::clone),
                Arc::clone(&accepting),
                failure.clone(),
            ));
            let handle = TableActorHandle {
                table_id,
                sender,
                accepting: Arc::clone(&accepting),
                global_budget: Arc::clone(&global_budget),
                persist_budget: Arc::new(Semaphore::new(1)),
                persistence_enabled,
            };
            if tables.insert(table_id, handle).is_some() {
                accepting.store(false, Ordering::Release);
                let _ = shutdown.send(true);
                return Err(corruption(format!(
                    "table actor registry contains duplicate table id table_id=[{table_id}]"
                ))
                .with_table_id(table_id));
            }
            actors.push((table_id, actor));
        }
        drop(shutdown_receiver);
        Ok(Self {
            handle: TableActorRegistryHandle {
                catalog_revision,
                tables: Arc::new(tables),
                accepting,
            },
            shutdown,
            actors,
            failure: failure_receiver,
            _failure_sender: failure,
            shutdown_started: false,
        })
    }

    /// Returns the cloneable, read-only admission and lookup surface.
    #[must_use]
    pub fn handle(&self) -> TableActorRegistryHandle {
        self.handle.clone()
    }

    /// Transfers a non-cloneable admission control to the server lifecycle owner.
    #[must_use]
    pub fn admission_control(&self) -> TableActorAdmissionControl {
        TableActorAdmissionControl {
            accepting: Arc::clone(&self.handle.accepting),
        }
    }

    /// Returns an independent observer for the registry's first actor failure.
    #[must_use]
    pub fn failure_monitor(&self) -> TableActorFailureMonitor {
        TableActorFailureMonitor {
            receiver: self.failure.clone(),
        }
    }

    /// Irreversibly closes new command admission for every table actor.
    pub fn close_admission(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.shutdown_started = true;
        self.handle.accepting.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    /// Closes admission, drains accepted commands, and joins every actor.
    pub async fn drain(mut self) -> Result<()> {
        self.close_admission();
        let mut first_error = None;
        for (table_id, actor) in self.actors.drain(..) {
            let outcome = actor.await.map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("join table actor task failed error=[{error}]"),
                )
                .with_table_id(table_id)
                .with_retryable(false)
            });
            let outcome = outcome.and_then(|result| result);
            if first_error.is_none() {
                first_error = outcome.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for TableActorRegistry {
    fn drop(&mut self) {
        self.close_admission();
    }
}

enum TableActorCommand {
    Snapshot {
        response: oneshot::Sender<Result<TableActorSnapshot>>,
        _permit: OwnedSemaphorePermit,
    },
    Persist {
        request: PersistRequest,
        response: oneshot::Sender<Result<CommitDataFilesReceipt>>,
        _persist_permit: OwnedSemaphorePermit,
        _global_permit: OwnedSemaphorePermit,
    },
    PublishLiveSegments {
        publication: TableActorLiveSegmentsPublication,
        response: oneshot::Sender<Result<()>>,
        _permit: OwnedSemaphorePermit,
    },
    #[cfg(test)]
    Panic,
}

type PersistCompletion = Pin<Box<dyn Future<Output = PersistCompletionResult> + Send + 'static>>;
type PersistSubmissionResult = std::result::Result<PersistCompletion, PersistCompletionFailure>;

trait PersistSubmitter: Send + Sync {
    fn submit(&self, request: PersistRequest, mode: PersistMode) -> PersistSubmissionResult;
}

impl PersistSubmitter for PersistControllerHandle {
    fn submit(&self, request: PersistRequest, mode: PersistMode) -> PersistSubmissionResult {
        let ticket = PersistControllerHandle::submit_classified_with_mode(self, request, mode)?;
        Ok(persist_ticket_completion(ticket))
    }
}

fn persist_ticket_completion(ticket: PersistTicket) -> PersistCompletion {
    Box::pin(ticket.wait_classified())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PersistDisposition {
    Advance,
    Replay,
}

impl PersistDisposition {
    const fn persist_mode(self) -> PersistMode {
        match self {
            Self::Advance => PersistMode::Advance,
            Self::Replay => PersistMode::HistoricalReplay,
        }
    }
}

struct InFlightPersist {
    request_id: RequestId,
    table_id: TableId,
    expected_snapshot_version: u64,
    committed_log_offset: OffsetWatermark,
    disposition: PersistDisposition,
    response: oneshot::Sender<Result<CommitDataFilesReceipt>>,
    _persist_permit: OwnedSemaphorePermit,
    _global_permit: OwnedSemaphorePermit,
}

struct PersistFailure {
    request_id: RequestId,
    error: ServerError,
}

enum ActorPersistenceState {
    Idle,
    InFlight(InFlightPersist),
    Failed(PersistFailure),
}

struct TableActorState {
    table_id: TableId,
    table_kind: TableKind,
    schema_version: u64,
    schema: SchemaRef,
    writer_epoch: u64,
    current_snapshot: TableSnapshot,
    persistence_enabled: bool,
    persistence: ActorPersistenceState,
    command_sequence: u64,
    live_segments: Arc<[TableActorLiveSegmentSnapshot]>,
}

impl TableActorState {
    fn new(runtime: &TableRuntime, persistence_enabled: bool) -> Self {
        Self {
            table_id: runtime.table_id(),
            table_kind: runtime.table().kind(),
            schema_version: runtime.schema_descriptor().schema_version,
            schema: Arc::clone(runtime.schema()),
            writer_epoch: runtime.writer_epoch(),
            current_snapshot: runtime.snapshot().clone(),
            persistence_enabled,
            persistence: ActorPersistenceState::Idle,
            command_sequence: 0,
            live_segments: Arc::from([]),
        }
    }

    fn snapshot(&mut self) -> Result<TableActorSnapshot> {
        self.command_sequence = self.command_sequence.checked_add(1).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "table actor command sequence exhausted u64",
            )
            .with_table_id(self.table_id)
            .with_retryable(false)
        })?;
        let (live_segments, max_log_offset) = self.visible_live_segments()?;
        Ok(TableActorSnapshot {
            table_id: self.table_id,
            schema_version: self.schema_version,
            persisted_snapshot_version: self.current_snapshot.snapshot_version,
            writer_epoch: self.writer_epoch,
            committed_log_offset: self.current_snapshot.committed_log_offset,
            actor_command_sequence: self.command_sequence,
            persistence_status: self.persistence_status(),
            live_segments,
            max_log_offset,
        })
    }

    fn visible_live_segments(
        &self,
    ) -> Result<(Arc<[TableActorLiveSegmentSnapshot]>, OffsetWatermark)> {
        let persisted = self.current_snapshot.committed_log_offset;
        if self.live_segments.is_empty() {
            return Ok((Arc::from([]), persisted));
        }
        let persisted_offset = match persisted {
            OffsetWatermark::Empty => None,
            OffsetWatermark::Known(offset) => Some(offset),
            OffsetWatermark::LegacyUnknown => {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "live segments cannot follow a legacy-unknown persisted watermark",
                )
                .with_table_id(self.table_id)
                .with_retryable(false));
            }
        };

        let mut visible = Vec::with_capacity(self.live_segments.len());
        for segment in self.live_segments.iter() {
            let Some(persisted_offset) = persisted_offset else {
                visible.push(segment.clone());
                continue;
            };
            if segment.offsets.last().get() <= persisted_offset.get() {
                continue;
            }
            if segment.offsets.first().get() > persisted_offset.get() {
                visible.push(segment.clone());
                continue;
            }

            let first_visible = persisted_offset.checked_next().ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "persisted watermark exhausted before a live segment suffix",
                )
                .with_table_id(self.table_id)
                .with_retryable(false)
            })?;
            let skipped_rows_u64 = first_visible
                .get()
                .checked_sub(segment.offsets.first().get())
                .ok_or_else(|| live_corruption(self.table_id, "live segment trim underflow"))?;
            let skipped_rows = usize::try_from(skipped_rows_u64).map_err(|_| {
                live_corruption(self.table_id, "live segment trim exceeds platform usize")
            })?;
            let span = segment
                .span
                .slice_rows(skipped_rows, segment.span.row_count())
                .map_err(|error| {
                    live_corruption(
                        self.table_id,
                        format!("live segment trim is invalid error=[{error}]"),
                    )
                })?;
            let offsets =
                OffsetRange::new(first_visible, segment.offsets.last()).map_err(|error| {
                    live_corruption(
                        self.table_id,
                        format!("trimmed live offsets are invalid error=[{error}]"),
                    )
                })?;
            visible.push(TableActorLiveSegmentSnapshot {
                role: segment.role,
                identity: segment.identity,
                schema_version: segment.schema_version,
                offsets,
                span,
            });
        }
        let max_log_offset = visible
            .last()
            .map(|segment| OffsetWatermark::Known(segment.offsets.last()))
            .unwrap_or(persisted);
        Ok((visible.into(), max_log_offset))
    }

    fn install_live_segments(
        &mut self,
        publication: TableActorLiveSegmentsPublication,
    ) -> Result<()> {
        let TableActorLiveSegmentsPublication { table_id, segments } = publication;
        if table_id != self.table_id {
            return Err(live_publication_error(
                self.table_id,
                ErrorCode::InvalidArgument,
                "live segment publication table identity changed before actor validation",
            ));
        }
        if self.table_kind != TableKind::Append {
            return Err(live_publication_error(
                self.table_id,
                ErrorCode::UnsupportedPlan,
                "live query snapshots currently support append tables only",
            ));
        }
        if segments.len() > MAX_LIVE_SEGMENTS_PER_TABLE {
            return Err(live_publication_error(
                self.table_id,
                ErrorCode::ResourceExhausted,
                format!(
                    "live segment publication exceeds per-table limit actual=[{}] limit=[{}]",
                    segments.len(),
                    MAX_LIVE_SEGMENTS_PER_TABLE
                ),
            ));
        }
        if segments.is_empty() {
            self.live_segments = Arc::from([]);
            return Ok(());
        }

        let mut expected_offset = match self.current_snapshot.committed_log_offset {
            OffsetWatermark::Empty => LogOffset::new(0),
            OffsetWatermark::Known(offset) => offset.checked_next().ok_or_else(|| {
                live_publication_error(
                    self.table_id,
                    ErrorCode::ResourceExhausted,
                    "persisted watermark exhausted before live segment publication",
                )
            })?,
            OffsetWatermark::LegacyUnknown => {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::DataCorruption,
                    "cannot publish live segments after a legacy-unknown watermark",
                ));
            }
        };
        let segment_count = segments.len();
        let mut identities = BTreeSet::new();
        let mut installed = Vec::with_capacity(segment_count);

        for (index, segment) in segments.into_iter().enumerate() {
            if segment.role == TableActorLiveSegmentRole::Active && index + 1 != segment_count {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    "active segment must be the final live publication item",
                ));
            }
            if !identities.insert(segment.identity) {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    "live segment publication contains duplicate source identity",
                ));
            }
            if segment.schema_version != self.schema_version {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::SchemaMismatch,
                    format!(
                        "live segment schema version mismatch expected=[{}] actual=[{}]",
                        self.schema_version, segment.schema_version
                    ),
                ));
            }
            if segment.identity.writer_epoch != self.writer_epoch
                || segment.descriptor.writer_epoch() != self.writer_epoch
            {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::StaleProducerEpoch,
                    format!(
                        "live segment writer epoch mismatch expected=[{}] identity=[{}] descriptor=[{}]",
                        self.writer_epoch,
                        segment.identity.writer_epoch,
                        segment.descriptor.writer_epoch()
                    ),
                ));
            }
            if segment.identity.segment_id != segment.descriptor.segment_id()
                || segment.identity.generation != segment.descriptor.generation()
            {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    "live segment source identity differs from its shared-memory descriptor",
                ));
            }
            if segment.start_row >= segment.end_row {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    "live segment publication must contain at least one row",
                ));
            }
            let row_count = segment.end_row - segment.start_row;
            if row_count as u128 != segment.offsets.len() {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    "live segment row count differs from its logical offset range",
                ));
            }
            if segment.offsets.first() != expected_offset {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::SequenceGap,
                    format!(
                        "live segment offsets are not contiguous expected=[{}] actual=[{}]",
                        expected_offset,
                        segment.offsets.first()
                    ),
                ));
            }

            let span = RowSpanView::snapshot_from_active_descriptor(
                segment.descriptor,
                segment.start_row,
                segment.end_row,
            )
            .map_err(|error| {
                live_publication_error(
                    self.table_id,
                    ErrorCode::InvalidArgument,
                    format!("failed to pin live segment snapshot error=[{error}]"),
                )
            })?;
            if span.schema_ref().as_ref() != self.schema.as_ref() {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::SchemaMismatch,
                    "live segment Arrow schema differs from the actor schema",
                ));
            }
            let sealed = span.active_segment_is_sealed().map_err(|error| {
                live_publication_error(
                    self.table_id,
                    ErrorCode::DataCorruption,
                    format!("failed to read live segment seal state error=[{error}]"),
                )
            })?;
            let expected_sealed = segment.role == TableActorLiveSegmentRole::RetainedSealed;
            if sealed != Some(expected_sealed) {
                return Err(live_publication_error(
                    self.table_id,
                    ErrorCode::Conflict,
                    "live segment seal state differs from its publication role",
                ));
            }

            if index + 1 != segment_count {
                expected_offset = segment.offsets.last().checked_next().ok_or_else(|| {
                    live_publication_error(
                        self.table_id,
                        ErrorCode::ResourceExhausted,
                        "live segment offset domain exhausted before final segment",
                    )
                })?;
            }
            installed.push(TableActorLiveSegmentSnapshot {
                role: segment.role,
                identity: segment.identity,
                schema_version: segment.schema_version,
                offsets: segment.offsets,
                span,
            });
        }

        self.live_segments = installed.into();
        Ok(())
    }

    fn advance_command(&mut self) -> Result<()> {
        self.command_sequence = self.command_sequence.checked_add(1).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "table actor command sequence exhausted u64",
            )
            .with_table_id(self.table_id)
            .with_retryable(false)
        })?;
        Ok(())
    }

    fn persistence_status(&self) -> TablePersistenceStatus {
        match &self.persistence {
            ActorPersistenceState::Idle if !self.persistence_enabled => {
                TablePersistenceStatus::Disabled
            }
            ActorPersistenceState::Idle => TablePersistenceStatus::Idle,
            ActorPersistenceState::InFlight(active) => TablePersistenceStatus::InFlight {
                request_id: active.request_id,
            },
            ActorPersistenceState::Failed(failure) => TablePersistenceStatus::Failed {
                request_id: failure.request_id,
                code: failure.error.code(),
            },
        }
    }

    fn terminal_persistence_error(&self) -> Option<ServerError> {
        match &self.persistence {
            ActorPersistenceState::Failed(failure) => Some(failure.error.clone()),
            ActorPersistenceState::Idle | ActorPersistenceState::InFlight(_) => None,
        }
    }
}

fn live_publication_error(
    table_id: TableId,
    code: ErrorCode,
    message: impl Into<String>,
) -> ServerError {
    ServerError::new(code, message)
        .with_table_id(table_id)
        .with_retryable(false)
}

fn live_corruption(table_id: TableId, message: impl Into<String>) -> ServerError {
    live_publication_error(table_id, ErrorCode::DataCorruption, message)
}

struct TableActorTaskGuard {
    table_id: TableId,
    accepting: Arc<AtomicBool>,
    failure: watch::Sender<Option<ServerError>>,
    completed: bool,
}

impl TableActorTaskGuard {
    fn new(
        table_id: TableId,
        accepting: Arc<AtomicBool>,
        failure: watch::Sender<Option<ServerError>>,
    ) -> Self {
        Self {
            table_id,
            accepting,
            failure,
            completed: false,
        }
    }

    fn complete(&mut self, outcome: &Result<()>) {
        self.completed = true;
        if let Err(error) = outcome {
            publish_actor_failure(error.clone(), &self.accepting, &self.failure);
        }
    }
}

impl Drop for TableActorTaskGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        publish_actor_failure(
            ServerError::new(
                ErrorCode::Internal,
                "table actor task terminated without returning an outcome",
            )
            .with_table_id(self.table_id)
            .with_retryable(false),
            &self.accepting,
            &self.failure,
        );
    }
}

fn publish_actor_failure(
    error: ServerError,
    accepting: &Arc<AtomicBool>,
    failure: &watch::Sender<Option<ServerError>>,
) {
    accepting.store(false, Ordering::Release);
    let _ = failure.send_if_modified(|published| {
        if published.is_some() {
            return false;
        }
        *published = Some(error.clone());
        true
    });
}

async fn run_supervised_table_actor(
    runtime: Arc<TableRuntime>,
    receiver: mpsc::Receiver<TableActorCommand>,
    shutdown: watch::Receiver<bool>,
    persistence: Option<Arc<dyn PersistSubmitter>>,
    accepting: Arc<AtomicBool>,
    failure: watch::Sender<Option<ServerError>>,
) -> Result<()> {
    let mut guard =
        TableActorTaskGuard::new(runtime.table_id(), Arc::clone(&accepting), failure.clone());
    let outcome =
        run_table_actor(runtime, receiver, shutdown, persistence, accepting, failure).await;
    guard.complete(&outcome);
    outcome
}

async fn run_table_actor(
    runtime: Arc<TableRuntime>,
    mut receiver: mpsc::Receiver<TableActorCommand>,
    mut shutdown: watch::Receiver<bool>,
    persistence: Option<Arc<dyn PersistSubmitter>>,
    accepting: Arc<AtomicBool>,
    failure: watch::Sender<Option<ServerError>>,
) -> Result<()> {
    let mut state = TableActorState::new(&runtime, persistence.is_some());
    let mut completion: Option<PersistCompletion> = None;
    let mut shutdown_observed = false;
    let mut mailbox_closed = false;

    loop {
        if mailbox_closed && completion.is_none() {
            return match &state.persistence {
                ActorPersistenceState::Failed(failure) => Err(failure.error.clone()),
                ActorPersistenceState::Idle => Ok(()),
                ActorPersistenceState::InFlight(_) => {
                    unreachable!("mailbox cannot finish while a persist completion is active")
                }
            };
        }

        if let Some(active_completion) = completion.as_mut() {
            tokio::select! {
                biased;
                outcome = active_completion.as_mut() => {
                    completion = None;
                    complete_persist(&mut state, outcome, &accepting, &failure)?;
                }
                changed = shutdown.changed(), if !shutdown_observed => {
                    let _ = changed;
                    shutdown_observed = true;
                    receiver.close();
                }
                command = receiver.recv(), if !mailbox_closed => {
                    match command {
                        Some(command) => {
                            let unexpected = process_command(
                                &mut state,
                                persistence.as_deref(),
                                command,
                                &accepting,
                                &failure,
                            );
                            debug_assert!(unexpected.is_none());
                            if let Some(error) = state.terminal_persistence_error() {
                                return Err(error);
                            }
                        }
                        None => mailbox_closed = true,
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                changed = shutdown.changed(), if !shutdown_observed => {
                    let _ = changed;
                    shutdown_observed = true;
                    receiver.close();
                }
                command = receiver.recv(), if !mailbox_closed => {
                    match command {
                        Some(command) => {
                            completion = process_command(
                                &mut state,
                                persistence.as_deref(),
                                command,
                                &accepting,
                                &failure,
                            );
                            if let Some(error) = state.terminal_persistence_error() {
                                return Err(error);
                            }
                        }
                        None => mailbox_closed = true,
                    }
                }
            }
        }
    }
}

fn process_command(
    state: &mut TableActorState,
    persistence: Option<&dyn PersistSubmitter>,
    command: TableActorCommand,
    accepting: &Arc<AtomicBool>,
    failure: &watch::Sender<Option<ServerError>>,
) -> Option<PersistCompletion> {
    match command {
        TableActorCommand::Snapshot { response, _permit } => {
            let result = state.snapshot();
            drop(_permit);
            let _ = response.send(result);
            None
        }
        TableActorCommand::Persist {
            request,
            response,
            _persist_permit,
            _global_permit,
        } => {
            let request_id = request.request_id();
            let expected_snapshot_version = request.expected_snapshot_version();
            let committed_log_offset = request.committed_log_offset();
            let validated = state
                .advance_command()
                .map_err(PersistCompletionFailure::Fatal)
                .and_then(|()| {
                    validate_persist_request(state, &request)
                        .map_err(PersistCompletionFailure::Rejected)
                });
            let result = match validated {
                Ok(disposition) => match persistence {
                    Some(submitter) => submitter
                        .submit(request, disposition.persist_mode())
                        .map(|completion| (completion, disposition)),
                    None => Err(PersistCompletionFailure::Rejected(persistence_disabled(
                        state.table_id,
                        request_id,
                    ))),
                },
                Err(failure) => Err(failure),
            };
            match result {
                Ok((completion, disposition)) => {
                    let active = InFlightPersist {
                        request_id,
                        table_id: state.table_id,
                        expected_snapshot_version,
                        committed_log_offset,
                        disposition,
                        response,
                        _persist_permit,
                        _global_permit,
                    };
                    state.persistence = ActorPersistenceState::InFlight(active);
                    Some(completion)
                }
                Err(PersistCompletionFailure::Rejected(error)) => {
                    drop(_persist_permit);
                    drop(_global_permit);
                    let _ = response.send(Err(error));
                    None
                }
                Err(PersistCompletionFailure::Fatal(error)) => {
                    state.persistence = ActorPersistenceState::Failed(PersistFailure {
                        request_id,
                        error: error.clone(),
                    });
                    publish_actor_failure(error.clone(), accepting, failure);
                    drop(_persist_permit);
                    drop(_global_permit);
                    let _ = response.send(Err(error));
                    None
                }
            }
        }
        TableActorCommand::PublishLiveSegments {
            publication,
            response,
            _permit,
        } => {
            let result = state
                .advance_command()
                .and_then(|()| state.install_live_segments(publication));
            drop(_permit);
            let _ = response.send(result);
            None
        }
        #[cfg(test)]
        TableActorCommand::Panic => panic!("injected table actor panic"),
    }
}

fn validate_persist_request(
    state: &TableActorState,
    request: &PersistRequest,
) -> Result<PersistDisposition> {
    let request_id = request.request_id();
    if !state.persistence_enabled {
        return Err(persistence_disabled(state.table_id, request_id));
    }
    match &state.persistence {
        ActorPersistenceState::Idle => {}
        ActorPersistenceState::InFlight(active) => {
            return Err(table_persist_inflight(state.table_id, request_id)
                .with_retryable(true)
                .with_request_id(request_id)
                .with_table_id(active.table_id));
        }
        ActorPersistenceState::Failed(failure) => {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                format!(
                    concat!(
                        "native table persistence is failed failed_request_id=[{}] ",
                        "failed_code=[{}]"
                    ),
                    failure.request_id,
                    failure.error.code()
                ),
            )
            .with_request_id(request_id)
            .with_table_id(state.table_id)
            .with_retryable(false));
        }
    }
    if request.table_id() != state.table_id {
        return Err(persist_table_mismatch(
            state.table_id,
            request.table_id(),
            request_id,
        ));
    }
    if request.expected_snapshot_version() > state.current_snapshot.snapshot_version {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "persist snapshot is ahead of the actor expected=[{}] actual=[{}]",
                request.expected_snapshot_version(),
                state.current_snapshot.snapshot_version
            ),
        )
        .with_request_id(request_id)
        .with_table_id(state.table_id)
        .with_retryable(false)
        .with_details(ServerErrorDetails::RevisionConflict {
            expected: request.expected_snapshot_version(),
            actual: state.current_snapshot.snapshot_version,
        })
        .expect("revision details match the conflict error code"));
    }
    if request.expected_snapshot_version() < state.current_snapshot.snapshot_version {
        return Ok(PersistDisposition::Replay);
    }
    state
        .current_snapshot
        .snapshot_version
        .checked_add(1)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "persist snapshot version domain is exhausted",
            )
            .with_request_id(request_id)
            .with_table_id(state.table_id)
            .with_retryable(false)
        })?;
    if request.schema_version() != state.schema_version {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "persist schema version mismatch expected=[{}] actual=[{}]",
                state.schema_version,
                request.schema_version()
            ),
        )
        .with_request_id(request_id)
        .with_table_id(state.table_id)
        .with_retryable(false)
        .with_details(ServerErrorDetails::SchemaVersion {
            expected: state.schema_version,
            actual: request.schema_version(),
        })
        .expect("schema details match the schema-mismatch error code"));
    }
    if request.schema().as_ref() != state.schema.as_ref() {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            "persist Arrow schema does not match the actor-owned current schema",
        )
        .with_request_id(request_id)
        .with_table_id(state.table_id)
        .with_retryable(false));
    }
    if request.writer_epoch() != state.writer_epoch {
        return Err(ServerError::new(
            ErrorCode::StaleProducerEpoch,
            format!(
                "persist writer epoch mismatch expected=[{}] actual=[{}]",
                state.writer_epoch,
                request.writer_epoch()
            ),
        )
        .with_request_id(request_id)
        .with_table_id(state.table_id)
        .with_retryable(false)
        .with_details(ServerErrorDetails::ProducerEpoch {
            expected: state.writer_epoch,
            actual: request.writer_epoch(),
        })
        .expect("producer details match the stale-epoch error code"));
    }
    let expected_first = match state.current_snapshot.committed_log_offset {
        OffsetWatermark::Empty => LogOffset::new(0),
        OffsetWatermark::Known(previous) => previous.checked_next().ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "persist log offset domain is exhausted",
            )
            .with_request_id(request_id)
            .with_table_id(state.table_id)
            .with_retryable(false)
        })?,
        OffsetWatermark::LegacyUnknown => {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "cannot append to a legacy snapshot with unknown log watermark",
            )
            .with_request_id(request_id)
            .with_table_id(state.table_id)
            .with_retryable(false));
        }
    };
    if request.first_log_offset() != expected_first {
        return Err(ServerError::new(
            ErrorCode::SequenceGap,
            format!(
                "persist log offset is not contiguous expected=[{}] actual=[{}]",
                expected_first,
                request.first_log_offset()
            ),
        )
        .with_request_id(request_id)
        .with_table_id(state.table_id)
        .with_retryable(false)
        .with_details(ServerErrorDetails::SequenceGap {
            expected: expected_first.get(),
            actual: request.first_log_offset().get(),
        })
        .expect("sequence details match the sequence-gap error code"));
    }
    Ok(PersistDisposition::Advance)
}

fn complete_persist(
    state: &mut TableActorState,
    outcome: PersistCompletionResult,
    accepting: &Arc<AtomicBool>,
    failure: &watch::Sender<Option<ServerError>>,
) -> Result<()> {
    let previous = std::mem::replace(&mut state.persistence, ActorPersistenceState::Idle);
    let active = match previous {
        ActorPersistenceState::InFlight(active) => active,
        ActorPersistenceState::Idle | ActorPersistenceState::Failed(_) => {
            unreachable!("persist completion requires an in-flight actor operation")
        }
    };
    let classified = match outcome {
        Ok(receipt) => validate_persist_receipt(state, &active, &receipt)
            .map(|()| receipt)
            .map_err(PersistCompletionFailure::Fatal),
        Err(PersistCompletionFailure::Rejected(error)) => Err(PersistCompletionFailure::Rejected(
            error
                .with_request_id(active.request_id)
                .with_table_id(state.table_id),
        )),
        Err(PersistCompletionFailure::Fatal(error)) => Err(PersistCompletionFailure::Fatal(
            error
                .with_request_id(active.request_id)
                .with_table_id(state.table_id),
        )),
    };
    let disposition = active.disposition;
    let InFlightPersist {
        request_id,
        response,
        _persist_permit: persist_permit,
        _global_permit: global_permit,
        ..
    } = active;
    match classified {
        Ok(receipt) => {
            if disposition == PersistDisposition::Advance {
                state.current_snapshot = receipt.snapshot.clone();
            }
            drop(persist_permit);
            drop(global_permit);
            let _ = response.send(Ok(receipt));
            Ok(())
        }
        Err(PersistCompletionFailure::Rejected(error)) => {
            drop(persist_permit);
            drop(global_permit);
            let _ = response.send(Err(error));
            Ok(())
        }
        Err(PersistCompletionFailure::Fatal(error)) => {
            let response_error = error.clone();
            state.persistence = ActorPersistenceState::Failed(PersistFailure {
                request_id,
                error: error.clone(),
            });
            publish_actor_failure(error.clone(), accepting, failure);
            drop(persist_permit);
            drop(global_permit);
            let _ = response.send(Err(response_error));
            Err(error)
        }
    }
}

fn validate_persist_receipt(
    state: &TableActorState,
    active: &InFlightPersist,
    receipt: &CommitDataFilesReceipt,
) -> Result<()> {
    let invalid = |message: String| {
        ServerError::new(ErrorCode::DataCorruption, message)
            .with_request_id(active.request_id)
            .with_table_id(state.table_id)
            .with_retryable(false)
    };
    if receipt.request_id != active.request_id {
        return Err(invalid(format!(
            "persist receipt request mismatch expected=[{}] actual=[{}]",
            active.request_id, receipt.request_id
        )));
    }
    if active.table_id != state.table_id || receipt.snapshot.table_id != state.table_id {
        return Err(invalid(format!(
            concat!(
                "persist receipt table mismatch actor=[{}] active=[{}] ",
                "receipt=[{}]"
            ),
            state.table_id, active.table_id, receipt.snapshot.table_id
        )));
    }
    if receipt.snapshot.parent_snapshot_version != Some(active.expected_snapshot_version) {
        return Err(invalid(format!(
            "persist receipt parent mismatch expected=[{}] actual=[{:?}]",
            active.expected_snapshot_version, receipt.snapshot.parent_snapshot_version
        )));
    }
    let expected_version = active
        .expected_snapshot_version
        .checked_add(1)
        .ok_or_else(|| invalid("persist snapshot version domain is exhausted".to_owned()))?;
    if receipt.snapshot.snapshot_version != expected_version {
        return Err(invalid(format!(
            "persist receipt version mismatch expected=[{}] actual=[{}]",
            expected_version, receipt.snapshot.snapshot_version
        )));
    }
    if receipt.snapshot.operation != SnapshotOperation::Persist {
        return Err(invalid(format!(
            "persist receipt operation mismatch actual=[{:?}]",
            receipt.snapshot.operation
        )));
    }
    if receipt.catalog_revision == 0 || receipt.snapshot.commit_lsn != receipt.catalog_revision {
        return Err(invalid(format!(
            "persist receipt commit lsn is invalid snapshot=[{}] catalog=[{}]",
            receipt.snapshot.commit_lsn, receipt.catalog_revision
        )));
    }
    match active.disposition {
        PersistDisposition::Advance => {
            if state.current_snapshot.snapshot_version != active.expected_snapshot_version {
                return Err(invalid(format!(
                    "persist actor parent changed expected=[{}] actual=[{}]",
                    active.expected_snapshot_version, state.current_snapshot.snapshot_version
                )));
            }
            if receipt.snapshot.commit_lsn <= state.current_snapshot.commit_lsn {
                return Err(invalid(format!(
                    "persist receipt commit lsn did not advance parent=[{}] actual=[{}]",
                    state.current_snapshot.commit_lsn, receipt.snapshot.commit_lsn
                )));
            }
        }
        PersistDisposition::Replay => {
            if receipt.snapshot.snapshot_version > state.current_snapshot.snapshot_version {
                return Err(invalid(format!(
                    "persist replay receipt is newer than actor snapshot receipt=[{}] actor=[{}]",
                    receipt.snapshot.snapshot_version, state.current_snapshot.snapshot_version
                )));
            }
            if receipt.snapshot.commit_lsn > state.current_snapshot.commit_lsn {
                return Err(invalid(format!(
                    "persist replay receipt lsn is newer than actor snapshot receipt=[{}] actor=[{}]",
                    receipt.snapshot.commit_lsn, state.current_snapshot.commit_lsn
                )));
            }
        }
    }
    if receipt.snapshot.committed_log_offset != active.committed_log_offset {
        return Err(invalid(format!(
            "persist receipt watermark mismatch expected=[{:?}] actual=[{:?}]",
            active.committed_log_offset, receipt.snapshot.committed_log_offset
        )));
    }
    Ok(())
}

fn validate_config(config: &TableActorRegistryConfig) -> Result<()> {
    if config.per_table_mailbox_capacity == 0
        || config.per_table_mailbox_capacity > MAX_PER_TABLE_MAILBOX_CAPACITY
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                concat!(
                    "table actor per-table mailbox capacity is outside supported range ",
                    "actual=[{}] maximum=[{}]"
                ),
                config.per_table_mailbox_capacity, MAX_PER_TABLE_MAILBOX_CAPACITY
            ),
        )
        .with_retryable(false));
    }
    if config.max_outstanding_commands == 0
        || config.max_outstanding_commands > MAX_OUTSTANDING_COMMANDS
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                concat!(
                    "table actor global command capacity is outside supported range ",
                    "actual=[{}] maximum=[{}]"
                ),
                config.max_outstanding_commands, MAX_OUTSTANDING_COMMANDS
            ),
        )
        .with_retryable(false));
    }
    Ok(())
}

fn actor_closed(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "native table actor is not accepting commands",
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

fn actor_mailbox_full(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native table actor mailbox is full",
    )
    .with_table_id(table_id)
    .with_retryable(true)
}

fn actor_global_budget_exhausted(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native table actor global command budget is exhausted",
    )
    .with_table_id(table_id)
    .with_retryable(true)
}

fn table_persist_inflight(table_id: TableId, request_id: RequestId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native table already has one admitted persist operation",
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(true)
}

fn persistence_disabled(table_id: TableId, request_id: RequestId) -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        "native table actor persistence is disabled for this runtime",
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(false)
}

fn persist_table_mismatch(
    expected: TableId,
    actual: TableId,
    request_id: RequestId,
) -> ServerError {
    ServerError::new(
        ErrorCode::InvalidArgument,
        format!("persist request table mismatch expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_table_id(expected)
    .with_retryable(false)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use tokio::sync::{mpsc, oneshot};
    use zippy_segment_store::{
        compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, SegmentStore,
        SegmentStoreConfig,
    };
    use zippy_storage::{
        CommitDataFilesReceipt, ParquetCandidateSpec, PersistCandidateInput,
        PersistCompletionFailure, PersistCompletionResult, PersistMode, PersistRequest,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance,
        SchemaCompatibility, SchemaDescriptor, SchemaIpcPayload, SnapshotOperation,
        SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableDefinition,
        TableDescriptor, TableSnapshot, TableState,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId,
        ServerError, TableId,
    };

    use crate::{TableRuntimeBootstrap, TableRuntimeRegistry};

    use super::{
        validate_persist_request, PersistSubmissionResult, PersistSubmitter, TableActorCommand,
        TableActorLiveSegmentPublication, TableActorLiveSegmentRole,
        TableActorLiveSegmentsPublication, TableActorRegistry, TableActorRegistryConfig,
        TableActorState, TablePersistenceStatus,
    };

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).unwrap()
    }

    fn persist_request(
        request: u128,
        table: u128,
        expected_snapshot_version: u64,
        schema_version: u64,
        writer_epoch: u64,
        first: u64,
        values: Vec<i64>,
    ) -> PersistRequest {
        persist_request_with_schema(
            request,
            table,
            expected_snapshot_version,
            schema_version,
            writer_epoch,
            first,
            values,
            schema(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_request_with_schema(
        request: u128,
        table: u128,
        expected_snapshot_version: u64,
        schema_version: u64,
        writer_epoch: u64,
        first: u64,
        values: Vec<i64>,
        schema: SchemaRef,
    ) -> PersistRequest {
        let table_id = table_id(table);
        let request_id = request_id(request);
        let row_count = u64::try_from(values.len()).unwrap();
        let last = first.checked_add(row_count - 1).unwrap();
        let offsets = OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).unwrap();
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(vec![SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: request as u64,
                    generation: 1,
                    writer_epoch,
                },
                offsets,
            }])
            .unwrap(),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id: FileId::from_u128(request + 1_000).unwrap(),
                table_id,
                schema_version,
                schema,
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: request as u64,
            },
            vec![batch],
        )
        .unwrap();
        PersistRequest::new(
            request_id,
            table_id,
            expected_snapshot_version,
            vec![candidate],
            OffsetWatermark::Known(LogOffset::new(last)),
        )
        .unwrap()
    }

    fn successful_receipt(
        request: u128,
        table: u128,
        parent_snapshot_version: u64,
        watermark: OffsetWatermark,
    ) -> CommitDataFilesReceipt {
        let catalog_revision = parent_snapshot_version + 10;
        CommitDataFilesReceipt {
            request_id: request_id(request),
            catalog_revision,
            snapshot: TableSnapshot {
                table_id: table_id(table),
                snapshot_version: parent_snapshot_version + 1,
                parent_snapshot_version: Some(parent_snapshot_version),
                operation: SnapshotOperation::Persist,
                commit_lsn: catalog_revision,
                committed_log_offset: watermark,
                created_at_ns: 2,
                summary: MetadataMap::new(),
            },
            intent_cleanup_pending: false,
        }
    }

    struct FakeSubmission {
        request_id: RequestId,
        mode: PersistMode,
        response: oneshot::Sender<PersistCompletionResult>,
    }

    struct FakePersistSubmitter {
        submissions: mpsc::UnboundedSender<FakeSubmission>,
    }

    impl PersistSubmitter for FakePersistSubmitter {
        fn submit(&self, request: PersistRequest, mode: PersistMode) -> PersistSubmissionResult {
            let request_id = request.request_id();
            let (response, completion) = oneshot::channel();
            self.submissions
                .send(FakeSubmission {
                    request_id,
                    mode,
                    response,
                })
                .map_err(|_| {
                    PersistCompletionFailure::Fatal(
                        ServerError::new(ErrorCode::Internal, "fake persist receiver was dropped")
                            .with_request_id(request_id)
                            .with_table_id(request.table_id())
                            .with_retryable(false),
                    )
                })?;
            Ok(Box::pin(async move {
                completion.await.unwrap_or_else(|_| {
                    Err(PersistCompletionFailure::Fatal(
                        ServerError::new(
                            ErrorCode::Internal,
                            "fake persist completion sender was dropped",
                        )
                        .with_request_id(request_id)
                        .with_retryable(false),
                    ))
                })
            }))
        }
    }

    fn fake_submitter() -> (
        Arc<dyn PersistSubmitter>,
        mpsc::UnboundedReceiver<FakeSubmission>,
    ) {
        let (submissions, receiver) = mpsc::unbounded_channel();
        (Arc::new(FakePersistSubmitter { submissions }), receiver)
    }

    struct ImmediateErrorSubmitter {
        code: ErrorCode,
        fatal: bool,
    }

    impl PersistSubmitter for ImmediateErrorSubmitter {
        fn submit(&self, request: PersistRequest, _mode: PersistMode) -> PersistSubmissionResult {
            let error = ServerError::new(self.code, "fake persist submitter rejected the request")
                .with_request_id(request.request_id())
                .with_table_id(request.table_id());
            if self.fatal {
                Err(PersistCompletionFailure::Fatal(error))
            } else {
                Err(PersistCompletionFailure::Rejected(error))
            }
        }
    }

    fn immediate_error_submitter(code: ErrorCode, fatal: bool) -> Arc<dyn PersistSubmitter> {
        Arc::new(ImmediateErrorSubmitter { code, fatal })
    }

    fn runtime_registry(table_values: &[u128]) -> TableRuntimeRegistry {
        let bootstraps = table_values
            .iter()
            .map(|value| {
                let table_id = table_id(*value);
                TableRuntimeBootstrap::new(
                    TableDescriptor {
                        table_id,
                        namespace_id: NamespaceId::from_u128(1).unwrap(),
                        name: CatalogName::new(format!("table_{value}")).unwrap(),
                        definition: TableDefinition::Append(AppendTableDefinition::default()),
                        current_schema_version: 1,
                        current_snapshot_version: 1,
                        writer_epoch: 7,
                        state: TableState::Active,
                        config: BTreeMap::new(),
                        created_at_ns: 1,
                        updated_at_ns: 1,
                    },
                    SchemaDescriptor {
                        table_id,
                        schema_version: 1,
                        schema_ipc: SchemaIpcPayload::new(vec![1]).unwrap(),
                        schema_hash: "sha256-v1:test".to_owned(),
                        compatibility: SchemaCompatibility::Initial,
                        created_at_ns: 1,
                    },
                    schema(),
                    TableSnapshot {
                        table_id,
                        snapshot_version: 1,
                        parent_snapshot_version: None,
                        operation: SnapshotOperation::Create,
                        commit_lsn: 1,
                        committed_log_offset: OffsetWatermark::Empty,
                        created_at_ns: 1,
                        summary: BTreeMap::new(),
                    },
                )
                .unwrap()
            })
            .collect();
        TableRuntimeRegistry::from_bootstraps(9, bootstraps).unwrap()
    }

    #[tokio::test]
    async fn actor_serializes_observations_and_drains_cleanly() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();

        let first = actor.snapshot().await.unwrap();
        let second = actor.snapshot().await.unwrap();

        assert_eq!(first.actor_command_sequence, 1);
        assert_eq!(second.actor_command_sequence, 2);
        assert_eq!(second.writer_epoch, 7);
        assert_eq!(second.committed_log_offset, OffsetWatermark::Empty);
        assert_eq!(second.persistence_status, TablePersistenceStatus::Disabled);
        assert_eq!(
            actor
                .try_persist(persist_request(1, 10, 1, 1, 7, 0, vec![1]))
                .unwrap_err()
                .code(),
            ErrorCode::UnsupportedPlan
        );
        registry.drain().await.unwrap();
        assert!(!handle.is_accepting());
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
    }

    #[tokio::test]
    async fn actor_publishes_fixed_active_prefix_and_rejects_rewrite() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let segment_schema =
            compile_schema(&[ColumnSpec::new("price", ColumnType::Int64)]).unwrap();
        let layout = LayoutPlan::for_schema(&segment_schema, 4).unwrap();
        let mut writer =
            ActiveSegmentWriter::new_for_runtime_with_epoch(segment_schema, layout, 11, 2, 7, 1)
                .unwrap();
        for value in [101_i64, 102_i64] {
            writer.begin_row().unwrap();
            writer.write_i64("price", value).unwrap();
            writer.commit_row().unwrap();
        }
        let identity = SourceSegmentIdentity {
            segment_id: 11,
            generation: 2,
            writer_epoch: 7,
        };
        let offsets = OffsetRange::new(LogOffset::new(0), LogOffset::new(1)).unwrap();
        actor
            .publish_live_segments(TableActorLiveSegmentsPublication::new(
                table_id(10),
                vec![TableActorLiveSegmentPublication::new(
                    TableActorLiveSegmentRole::Active,
                    identity,
                    1,
                    offsets,
                    writer.active_descriptor(),
                    0,
                    2,
                )],
            ))
            .await
            .unwrap();

        let pinned = actor.snapshot().await.unwrap();
        assert_eq!(pinned.live_segments.len(), 1);
        assert_eq!(pinned.live_segments[0].offsets(), offsets);
        assert_eq!(pinned.live_segments[0].span().row_count(), 2);
        assert_eq!(
            pinned.max_log_offset,
            OffsetWatermark::Known(LogOffset::new(1))
        );

        writer.begin_row().unwrap();
        writer.write_i64("price", 103).unwrap();
        writer.commit_row().unwrap();
        assert_eq!(
            pinned.live_segments[0]
                .span()
                .as_record_batch()
                .unwrap()
                .num_rows(),
            2
        );

        writer.begin_payload_mutation_for_test().unwrap();
        writer.finish_payload_mutation_for_test().unwrap();
        let error = pinned.live_segments[0]
            .span()
            .as_record_batch()
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("active payload version differs from pinned snapshot"));
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn actor_publishes_retained_sealed_then_active_segments_in_offset_order() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let segment_schema =
            compile_schema(&[ColumnSpec::new("price", ColumnType::Int64)]).unwrap();
        let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
        let handle = store
            .open_partition_with_schema_and_writer_epoch("prices", "all", segment_schema, 7)
            .unwrap();
        let writer = handle.writer();

        writer.write_row(|row| row.write_i64("price", 101)).unwrap();
        let retained_descriptor = handle.active_descriptor_for_test();
        writer.rollover_without_persistence().unwrap();
        writer.write_row(|row| row.write_i64("price", 102)).unwrap();
        let active_descriptor = handle.active_descriptor_for_test();

        actor
            .publish_live_segments(TableActorLiveSegmentsPublication::new(
                table_id(10),
                vec![
                    TableActorLiveSegmentPublication::new(
                        TableActorLiveSegmentRole::RetainedSealed,
                        SourceSegmentIdentity {
                            segment_id: retained_descriptor.segment_id(),
                            generation: retained_descriptor.generation(),
                            writer_epoch: retained_descriptor.writer_epoch(),
                        },
                        1,
                        OffsetRange::new(LogOffset::new(0), LogOffset::new(0)).unwrap(),
                        retained_descriptor,
                        0,
                        1,
                    ),
                    TableActorLiveSegmentPublication::new(
                        TableActorLiveSegmentRole::Active,
                        SourceSegmentIdentity {
                            segment_id: active_descriptor.segment_id(),
                            generation: active_descriptor.generation(),
                            writer_epoch: active_descriptor.writer_epoch(),
                        },
                        1,
                        OffsetRange::new(LogOffset::new(1), LogOffset::new(1)).unwrap(),
                        active_descriptor,
                        0,
                        1,
                    ),
                ],
            ))
            .await
            .unwrap();

        let snapshot = actor.snapshot().await.unwrap();
        assert_eq!(snapshot.live_segments.len(), 2);
        assert_eq!(
            snapshot.live_segments[0].role(),
            TableActorLiveSegmentRole::RetainedSealed
        );
        assert_eq!(
            snapshot.live_segments[1].role(),
            TableActorLiveSegmentRole::Active
        );
        let values = snapshot
            .live_segments
            .iter()
            .map(|segment| {
                let batch = segment.span().as_record_batch().unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![101, 102]);
        assert_eq!(
            snapshot.max_log_offset,
            OffsetWatermark::Known(LogOffset::new(1))
        );

        registry.drain().await.unwrap();
    }

    #[test]
    fn actor_snapshot_removes_persisted_prefix_without_copying_live_rows() {
        let runtimes = runtime_registry(&[10]);
        let runtime = runtimes.get(table_id(10)).unwrap();
        let mut state = TableActorState::new(&runtime, true);
        let segment_schema =
            compile_schema(&[ColumnSpec::new("price", ColumnType::Int64)]).unwrap();
        let layout = LayoutPlan::for_schema(&segment_schema, 4).unwrap();
        let mut writer =
            ActiveSegmentWriter::new_for_runtime_with_epoch(segment_schema, layout, 11, 2, 7, 1)
                .unwrap();
        for value in [101_i64, 102_i64, 103_i64] {
            writer.begin_row().unwrap();
            writer.write_i64("price", value).unwrap();
            writer.commit_row().unwrap();
        }
        state
            .install_live_segments(TableActorLiveSegmentsPublication::new(
                table_id(10),
                vec![TableActorLiveSegmentPublication::new(
                    TableActorLiveSegmentRole::Active,
                    SourceSegmentIdentity {
                        segment_id: 11,
                        generation: 2,
                        writer_epoch: 7,
                    },
                    1,
                    OffsetRange::new(LogOffset::new(0), LogOffset::new(2)).unwrap(),
                    writer.active_descriptor(),
                    0,
                    3,
                )],
            ))
            .unwrap();
        state.current_snapshot.committed_log_offset = OffsetWatermark::Known(LogOffset::new(0));

        let snapshot = state.snapshot().unwrap();
        let live = &snapshot.live_segments[0];
        assert_eq!(live.offsets().first(), LogOffset::new(1));
        assert_eq!(live.offsets().last(), LogOffset::new(2));
        assert_eq!(live.span().start_row(), 1);
        assert_eq!(live.span().row_count(), 2);
        assert_eq!(live.span().pinned_payload_version(), Some(0));
    }

    #[test]
    fn actor_rechecks_every_persist_state_precondition() {
        let runtimes = runtime_registry(&[10]);
        let runtime = runtimes.get(table_id(10)).unwrap();
        let mut state = TableActorState::new(&runtime, true);

        let wrong_table = persist_request(1, 20, 1, 1, 7, 0, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &wrong_table)
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        let future_snapshot = persist_request(2, 10, 2, 1, 7, 0, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &future_snapshot)
                .unwrap_err()
                .code(),
            ErrorCode::Conflict
        );
        let wrong_schema = persist_request(3, 10, 1, 2, 7, 0, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &wrong_schema)
                .unwrap_err()
                .code(),
            ErrorCode::SchemaMismatch
        );
        let wrong_arrow_schema = persist_request_with_schema(
            31,
            10,
            1,
            1,
            7,
            0,
            vec![1],
            Arc::new(Schema::new(vec![Field::new(
                "volume",
                DataType::Int64,
                false,
            )])),
        );
        assert_eq!(
            validate_persist_request(&state, &wrong_arrow_schema)
                .unwrap_err()
                .code(),
            ErrorCode::SchemaMismatch
        );
        let stale_epoch = persist_request(4, 10, 1, 1, 6, 0, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &stale_epoch)
                .unwrap_err()
                .code(),
            ErrorCode::StaleProducerEpoch
        );
        let offset_gap = persist_request(5, 10, 1, 1, 7, 1, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &offset_gap)
                .unwrap_err()
                .code(),
            ErrorCode::SequenceGap
        );
        state.current_snapshot.committed_log_offset = OffsetWatermark::LegacyUnknown;
        let legacy_unknown = persist_request(6, 10, 1, 1, 7, 0, vec![1]);
        assert_eq!(
            validate_persist_request(&state, &legacy_unknown)
                .unwrap_err()
                .code(),
            ErrorCode::DataCorruption
        );
    }

    #[tokio::test]
    async fn persist_is_single_flight_and_snapshot_observes_actor_owned_state() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            Some(submitter),
        )
        .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();

        assert_eq!(
            actor.snapshot().await.unwrap().persistence_status,
            TablePersistenceStatus::Idle
        );
        let wrong_table = actor
            .try_persist(persist_request(9, 20, 1, 1, 7, 0, vec![1]))
            .unwrap_err();
        assert_eq!(wrong_table.code(), ErrorCode::InvalidArgument);

        let first = actor
            .try_persist(persist_request(10, 10, 1, 1, 7, 0, vec![1, 2]))
            .unwrap();
        let first_submission = submissions.recv().await.unwrap();
        assert_eq!(first_submission.request_id, request_id(10));
        assert_eq!(first_submission.mode, PersistMode::Advance);
        let duplicate = actor
            .try_persist(persist_request(11, 10, 1, 1, 7, 0, vec![3]))
            .unwrap_err();
        assert_eq!(duplicate.code(), ErrorCode::ResourceExhausted);
        assert!(duplicate.retryable());

        let during = tokio::time::timeout(Duration::from_millis(100), actor.snapshot())
            .await
            .expect("snapshot must not wait for persistence")
            .unwrap();
        assert_eq!(during.persisted_snapshot_version, 1);
        assert_eq!(during.committed_log_offset, OffsetWatermark::Empty);
        assert_eq!(
            during.persistence_status,
            TablePersistenceStatus::InFlight {
                request_id: request_id(10)
            }
        );

        first_submission
            .response
            .send(Ok(successful_receipt(
                10,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(1)),
            )))
            .unwrap();
        let first_receipt = first.wait().await.unwrap();
        assert_eq!(first_receipt.snapshot.snapshot_version, 2);

        let second = actor
            .try_persist(persist_request(12, 10, 2, 1, 7, 2, vec![3]))
            .expect("completion releases the single-flight permit before waking its waiter");
        let second_submission = submissions.recv().await.unwrap();
        second_submission
            .response
            .send(Ok(successful_receipt(
                12,
                10,
                2,
                OffsetWatermark::Known(LogOffset::new(2)),
            )))
            .unwrap();
        second.wait().await.unwrap();

        let after = actor.snapshot().await.unwrap();
        assert_eq!(after.persisted_snapshot_version, 3);
        assert_eq!(
            after.committed_log_offset,
            OffsetWatermark::Known(LogOffset::new(2))
        );
        assert_eq!(after.persistence_status, TablePersistenceStatus::Idle);

        let future_snapshot = actor
            .persist(persist_request(13, 10, 4, 1, 7, 3, vec![4]))
            .await
            .unwrap_err();
        assert_eq!(future_snapshot.code(), ErrorCode::Conflict);
        assert!(submissions.try_recv().is_err());
        let corrected = actor
            .try_persist(persist_request(14, 10, 3, 1, 7, 3, vec![4]))
            .expect("rejected request releases permits before waking its waiter");
        let corrected_submission = submissions.recv().await.unwrap();
        corrected_submission
            .response
            .send(Ok(successful_receipt(
                14,
                10,
                3,
                OffsetWatermark::Known(LogOffset::new(3)),
            )))
            .unwrap();
        corrected.wait().await.unwrap();
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_waiter_and_historical_replay_never_roll_back_current_snapshot() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            Some(submitter),
        )
        .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();

        let abandoned = actor
            .try_persist(persist_request(60, 10, 1, 1, 7, 0, vec![1]))
            .unwrap();
        let first_submission = submissions.recv().await.unwrap();
        drop(abandoned);
        first_submission
            .response
            .send(Ok(successful_receipt(
                60,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        assert_eq!(
            actor.snapshot().await.unwrap().persisted_snapshot_version,
            2
        );

        let second = actor
            .try_persist(persist_request(61, 10, 2, 1, 7, 1, vec![2]))
            .unwrap();
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Ok(successful_receipt(
                61,
                10,
                2,
                OffsetWatermark::Known(LogOffset::new(1)),
            )))
            .unwrap();
        second.wait().await.unwrap();

        let replay = actor
            .try_persist(persist_request(60, 10, 1, 1, 7, 0, vec![1]))
            .expect("a stale parent is a Catalog idempotency candidate");
        let replay_submission = submissions.recv().await.unwrap();
        assert_eq!(replay_submission.request_id, request_id(60));
        assert_eq!(replay_submission.mode, PersistMode::HistoricalReplay);
        replay_submission
            .response
            .send(Ok(successful_receipt(
                60,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        let replayed = replay.wait().await.unwrap();
        assert_eq!(replayed.snapshot.snapshot_version, 2);

        let rejected = actor
            .try_persist(persist_request(62, 10, 1, 1, 7, 0, vec![3]))
            .unwrap();
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Err(PersistCompletionFailure::Rejected(ServerError::new(
                ErrorCode::Conflict,
                "request id does not match the stored Catalog fingerprint",
            ))))
            .unwrap();
        assert_eq!(
            rejected.wait().await.unwrap_err().code(),
            ErrorCode::Conflict
        );

        let current = actor.snapshot().await.unwrap();
        assert_eq!(current.persisted_snapshot_version, 3);
        assert_eq!(
            current.committed_log_offset,
            OffsetWatermark::Known(LogOffset::new(1))
        );
        assert_eq!(current.persistence_status, TablePersistenceStatus::Idle);
        assert!(handle.is_accepting());
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn definitive_rejection_with_current_parent_keeps_actor_available() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            Some(submitter),
        )
        .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();

        let rejected = actor
            .try_persist(persist_request(70, 10, 1, 1, 7, 0, vec![1]))
            .unwrap();
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Err(PersistCompletionFailure::Rejected(ServerError::new(
                ErrorCode::Conflict,
                "current-parent request id conflicts with its Catalog fingerprint",
            ))))
            .unwrap();
        assert_eq!(
            rejected.wait().await.unwrap_err().code(),
            ErrorCode::Conflict
        );
        assert!(handle.is_accepting());
        let unchanged = actor.snapshot().await.unwrap();
        assert_eq!(unchanged.persisted_snapshot_version, 1);
        assert_eq!(unchanged.persistence_status, TablePersistenceStatus::Idle);

        let valid = actor
            .try_persist(persist_request(71, 10, 1, 1, 7, 0, vec![2]))
            .unwrap();
        submissions
            .recv()
            .await
            .unwrap()
            .response
            .send(Ok(successful_receipt(
                71,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        valid.wait().await.unwrap();
        assert_eq!(
            actor.snapshot().await.unwrap().persisted_snapshot_version,
            2
        );
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_receipt_fails_table_and_is_reported_during_drain() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10, 20]),
            Some(submitter),
        )
        .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();
        let unaffected_actor = handle.table(table_id(20)).unwrap();
        let mut failure_monitor = registry.failure_monitor();
        let accepted = actor
            .try_persist(persist_request(20, 10, 1, 1, 7, 0, vec![1]))
            .unwrap();
        let submission = submissions.recv().await.unwrap();
        let invalid = successful_receipt(21, 10, 1, OffsetWatermark::Known(LogOffset::new(0)));
        submission.response.send(Ok(invalid)).unwrap();

        let failure = accepted.wait().await.unwrap_err();
        assert_eq!(failure.code(), ErrorCode::DataCorruption);
        assert!(!handle.is_accepting());
        let observed =
            tokio::time::timeout(Duration::from_secs(1), failure_monitor.wait_for_failure())
                .await
                .expect("invalid receipt must revoke global actor health immediately");
        assert_eq!(observed.code(), ErrorCode::DataCorruption);
        assert_eq!(observed.request_id(), Some(request_id(20)));
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
        assert_eq!(
            unaffected_actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
        assert!(submissions.try_recv().is_err());
        let drain_error = registry.drain().await.unwrap_err();
        assert_eq!(drain_error.code(), ErrorCode::DataCorruption);
        assert_eq!(drain_error.request_id(), Some(request_id(20)));
    }

    #[tokio::test]
    async fn unavailable_submit_failure_is_retained_but_backpressure_keeps_actor_idle() {
        let unavailable_registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            Some(immediate_error_submitter(ErrorCode::Unavailable, true)),
        )
        .unwrap();
        let unavailable_handle = unavailable_registry.handle();
        let unavailable_actor = unavailable_handle.table(table_id(10)).unwrap();
        let mut unavailable_failure = unavailable_registry.failure_monitor();

        let unavailable = unavailable_actor
            .persist(persist_request(40, 10, 1, 1, 7, 0, vec![1]))
            .await
            .unwrap_err();
        assert_eq!(unavailable.code(), ErrorCode::Unavailable);
        assert!(!unavailable_handle.is_accepting());
        let observed = tokio::time::timeout(
            Duration::from_secs(1),
            unavailable_failure.wait_for_failure(),
        )
        .await
        .expect("controller unavailability must stop the actor");
        assert_eq!(observed.code(), ErrorCode::Unavailable);
        assert_eq!(
            unavailable_actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
        let drain_error = unavailable_registry.drain().await.unwrap_err();
        assert_eq!(drain_error.code(), ErrorCode::Unavailable);
        assert_eq!(drain_error.request_id(), Some(request_id(40)));

        let backpressure_registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[20]),
            Some(immediate_error_submitter(
                ErrorCode::ResourceExhausted,
                false,
            )),
        )
        .unwrap();
        let backpressure_handle = backpressure_registry.handle();
        let backpressure_actor = backpressure_handle.table(table_id(20)).unwrap();

        let backpressure = backpressure_actor
            .persist(persist_request(50, 20, 1, 1, 7, 0, vec![1]))
            .await
            .unwrap_err();
        assert_eq!(backpressure.code(), ErrorCode::ResourceExhausted);
        assert!(backpressure.retryable());
        assert_eq!(
            backpressure_actor
                .snapshot()
                .await
                .unwrap()
                .persistence_status,
            TablePersistenceStatus::Idle
        );
        assert!(backpressure_handle.is_accepting());
        backpressure_registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_inflight_persist_after_external_waiter_is_dropped() {
        let (submitter, mut submissions) = fake_submitter();
        let registry = TableActorRegistry::start_internal(
            TableActorRegistryConfig::new(),
            runtime_registry(&[10]),
            Some(submitter),
        )
        .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        let waiter = actor
            .try_persist(persist_request(30, 10, 1, 1, 7, 0, vec![1]))
            .unwrap();
        let submission = submissions.recv().await.unwrap();
        drop(waiter);

        let mut drain = tokio::spawn(registry.drain());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut drain)
                .await
                .is_err(),
            "actor drain must retain and wait for the controller ticket"
        );
        submission
            .response
            .send(Ok(successful_receipt(
                30,
                10,
                1,
                OffsetWatermark::Known(LogOffset::new(0)),
            )))
            .unwrap();
        drain.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_table_and_global_admission_are_strictly_bounded() {
        let registry = TableActorRegistry::start(
            TableActorRegistryConfig::new()
                .with_per_table_mailbox_capacity(1)
                .with_max_outstanding_commands(2),
            runtime_registry(&[10, 20]),
        )
        .unwrap();
        let handle = registry.handle();
        let first_actor = handle.table(table_id(10)).unwrap();
        let second_actor = handle.table(table_id(20)).unwrap();

        let first = first_actor.try_snapshot().unwrap();
        let table_full = first_actor.try_snapshot().unwrap_err();
        assert_eq!(table_full.code(), ErrorCode::ResourceExhausted);
        assert!(table_full.retryable());
        let second = second_actor.try_snapshot().unwrap();
        let global_full = second_actor.try_snapshot().unwrap_err();
        assert_eq!(global_full.code(), ErrorCode::ResourceExhausted);
        assert!(global_full.retryable());

        assert_eq!(first.wait().await.unwrap().actor_command_sequence, 1);
        assert_eq!(second.wait().await.unwrap().actor_command_sequence, 1);
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_requests_accepted_before_admission_closed() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();
        let accepted = actor.try_snapshot().unwrap();

        let drain = tokio::spawn(registry.drain());
        let snapshot = accepted.wait().await.unwrap();

        assert_eq!(snapshot.actor_command_sequence, 1);
        drain.await.unwrap().unwrap();
        assert!(!handle.is_accepting());
    }

    #[tokio::test]
    async fn actor_panic_closes_global_admission_and_is_reported_immediately() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10, 20]))
                .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();
        let unaffected_actor = handle.table(table_id(20)).unwrap();
        let mut failure_monitor = registry.failure_monitor();
        assert!(actor.sender.try_send(TableActorCommand::Panic).is_ok());

        let observed =
            tokio::time::timeout(Duration::from_secs(1), failure_monitor.wait_for_failure())
                .await
                .expect("actor panic must be published without waiting for registry drain");
        assert_eq!(observed.code(), ErrorCode::Internal);
        assert_eq!(observed.table_id(), Some(table_id(10)));
        assert!(!observed.retryable());
        assert!(!handle.is_accepting());

        let request_error = actor.snapshot().await.unwrap_err();
        assert_eq!(request_error.code(), ErrorCode::Unavailable);
        let global_error = unaffected_actor.snapshot().await.unwrap_err();
        assert_eq!(global_error.code(), ErrorCode::Unavailable);
        let drain_error = registry.drain().await.unwrap_err();
        assert_eq!(drain_error.code(), ErrorCode::Internal);
        assert_eq!(drain_error.table_id(), Some(table_id(10)));
    }

    #[test]
    fn invalid_actor_budget_fails_before_spawning_tasks() {
        let error = TableActorRegistry::start(
            TableActorRegistryConfig::new().with_per_table_mailbox_capacity(0),
            runtime_registry(&[10]),
        )
        .err()
        .expect("invalid config must not return an actor registry");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn actor_registry_requires_an_explicit_tokio_runtime() {
        let error =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .err()
                .expect("starting actors without a runtime must fail structurally");
        assert_eq!(error.code(), ErrorCode::Unavailable);
    }
}
