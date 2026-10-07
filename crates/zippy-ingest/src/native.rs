//! Producer-fenced native ingest authority with stable offsets and WAL/manifest acknowledgements.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use zippy_types::catalog::{
    AcquireProducerCommand, CatalogActor, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogOutcomeV1, CatalogWrite, VersionedCatalogResponse,
};
use zippy_types::ingest::{
    Durability, DurableWriteAck, DurableWriteTarget, IngestService, IngestWriter,
    MutationFingerprint, OpaqueDurableWriteAck, OpaqueIngestWriter, OpenWriterRequest,
    TableMutation, WriteReceipt, WriteRequest, WriterInfo,
};
use zippy_types::table::{TableDefinition, TableKind};
use zippy_types::{
    BoxFuture, ConnectionId, ErrorCode, LogOffset, OffsetRange, OffsetWatermark, ProducerId,
    RequestId, Result, SequenceWatermark, ServerError, ServerErrorDetails, TableId,
};

use crate::checkpoint::{ReceiptArchive, WalCheckpoint};
use crate::{
    ArrowIpcMutationFingerprintComputer, IngestWalConfig, IngestWalIdentity, IngestWalWriter,
    RecoveredWalRecord, TableRuntimeRegistry, WalCommitReceipt,
};

/// Default maximum idempotent receipts retained for one active producer epoch.
pub const DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH: usize = 1_000_000;
/// Hard ceiling for configured per-epoch idempotent receipts.
pub const MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH: usize = 100_000_000;
/// Default maximum durable acknowledgements waiting for completion per table.
pub const DEFAULT_NATIVE_INGEST_MAX_PENDING_DURABLE_ACKS: usize = 1_024;
/// Hard ceiling for configured pending durable acknowledgements per table.
pub const MAX_NATIVE_INGEST_PENDING_DURABLE_ACKS: usize = 1_000_000;
/// Default shared budget for foreground operations and background durable acknowledgements.
pub const DEFAULT_NATIVE_INGEST_MAX_ACTIVE_OPERATIONS: usize = 4_096;
/// Hard ceiling for configured native ingest operations.
pub const MAX_NATIVE_INGEST_ACTIVE_OPERATIONS: usize = 10_000_000;

/// Bounded configuration for one native ingest authority.
#[derive(Debug, Clone)]
pub struct NativeIngestConfig {
    wal: IngestWalConfig,
    max_receipts_per_epoch: usize,
    max_pending_durable_acks: usize,
    max_active_operations: usize,
    catalog_actor: CatalogActor,
    checkpoint_interval: Duration,
}

impl Default for NativeIngestConfig {
    fn default() -> Self {
        Self {
            wal: IngestWalConfig::new(),
            max_receipts_per_epoch: DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH,
            max_pending_durable_acks: DEFAULT_NATIVE_INGEST_MAX_PENDING_DURABLE_ACKS,
            max_active_operations: DEFAULT_NATIVE_INGEST_MAX_ACTIVE_OPERATIONS,
            checkpoint_interval: Duration::from_secs(1),
            catalog_actor: CatalogActor {
                kind: zippy_types::catalog::CatalogActorKind::Server,
                principal: "zippy-server-ingest".to_owned(),
            },
        }
    }
}

impl NativeIngestConfig {
    /// Sets the background persistence cadence; backlog limits still apply between ticks.
    #[must_use]
    pub const fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }
    /// Creates conservative production defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the bounded WAL record and recovery policy.
    #[must_use]
    pub const fn with_wal_config(mut self, wal: IngestWalConfig) -> Self {
        self.wal = wal;
        self
    }

    /// Replaces the hot receipt budget; archived receipts remain exact within the active epoch.
    #[must_use]
    pub const fn with_max_receipts_per_epoch(mut self, maximum: usize) -> Self {
        self.max_receipts_per_epoch = maximum;
        self
    }

    /// Replaces the per-table budget for durable acknowledgements waiting on WAL or manifest commit.
    #[must_use]
    pub const fn with_max_pending_durable_acks(mut self, maximum: usize) -> Self {
        self.max_pending_durable_acks = maximum;
        self
    }

    /// Replaces the shared budget for foreground calls and background durable acknowledgements.
    #[must_use]
    pub const fn with_max_active_operations(mut self, maximum: usize) -> Self {
        self.max_active_operations = maximum;
        self
    }

    /// Replaces the audited Catalog actor used by producer acquisition commands.
    #[must_use]
    pub fn with_catalog_actor(mut self, actor: CatalogActor) -> Self {
        self.catalog_actor = actor;
        self
    }

    /// Validates all static allocation and recovery limits.
    pub fn validate(&self) -> Result<()> {
        self.wal.validate()?;
        if self.checkpoint_interval.is_zero() || self.checkpoint_interval > Duration::from_secs(60)
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "checkpoint interval must be positive and at most 60 seconds",
            )
            .with_retryable(false));
        }
        if self.max_receipts_per_epoch == 0
            || self.max_receipts_per_epoch > MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!("native ingest receipt limit must be between 1 and {} actual=[{}]"),
                    MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH, self.max_receipts_per_epoch
                ),
            )
            .with_retryable(false));
        }
        if self.max_pending_durable_acks == 0
            || self.max_pending_durable_acks > MAX_NATIVE_INGEST_PENDING_DURABLE_ACKS
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!(
                        "native ingest pending durable acknowledgement limit must be between 1 ",
                        "and {} actual=[{}]"
                    ),
                    MAX_NATIVE_INGEST_PENDING_DURABLE_ACKS, self.max_pending_durable_acks
                ),
            )
            .with_retryable(false));
        }
        if self.max_active_operations < 2
            || self.max_active_operations > MAX_NATIVE_INGEST_ACTIVE_OPERATIONS
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!(
                        "native ingest active operation limit must be between 2 and {} ",
                        "actual=[{}]"
                    ),
                    MAX_NATIVE_INGEST_ACTIVE_OPERATIONS, self.max_active_operations
                ),
            )
            .with_retryable(false));
        }
        Ok(())
    }
}

/// Immutable current-table metadata required to recover and admit native writes.
#[derive(Debug, Clone)]
pub struct NativeIngestTableBootstrap {
    table_id: TableId,
    table_definition: TableDefinition,
    schema_version: u64,
    schema: SchemaRef,
    writer_epoch: u64,
    persisted_log_offset: OffsetWatermark,
    persisted_state_version: u64,
}

impl NativeIngestTableBootstrap {
    /// Creates one validated table bootstrap from a revision-consistent Catalog snapshot.
    pub fn new(
        table_id: TableId,
        table_definition: TableDefinition,
        schema_version: u64,
        schema: SchemaRef,
        writer_epoch: u64,
        persisted_log_offset: OffsetWatermark,
    ) -> Result<Self> {
        if schema_version == 0 {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "native ingest table schema version must be nonzero",
            )
            .with_table_id(table_id)
            .with_retryable(false));
        }
        Ok(Self {
            table_id,
            table_definition,
            schema_version,
            schema,
            writer_epoch,
            persisted_log_offset,
            persisted_state_version: 0,
        })
    }

    /// Derives the logical write schema and checkpoint cursor from a native Catalog snapshot.
    pub fn from_catalog_snapshot(
        table: &zippy_types::table::TableDescriptor,
        physical: SchemaRef,
        snapshot: &zippy_types::table::TableSnapshot,
    ) -> Result<Self> {
        let logical = if let TableDefinition::KeyValue(definition) = &table.definition {
            let decoded = zippy_types::key_value::decode_log_schema(physical.as_ref())?
                .ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::DataCorruption,
                        "native key-value Catalog schema has no event envelope",
                    )
                })?;
            if decoded.keys.iter().map(String::as_str).ne(definition
                .keys()
                .as_slice()
                .iter()
                .map(|key| key.as_str()))
            {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "native key-value Catalog keys differ from the event schema",
                ));
            }
            decoded.logical
        } else {
            physical
        };
        let mut bootstrap = Self::new(
            table.table_id,
            table.definition.clone(),
            table.current_schema_version,
            logical,
            table.writer_epoch,
            snapshot.committed_log_offset,
        )?;
        if matches!(table.definition, TableDefinition::KeyValue(_)) {
            bootstrap.persisted_state_version = match snapshot
                .summary
                .get(zippy_types::key_value::STATE_VERSION_METADATA)
            {
                Some(zippy_types::table::MetadataScalar::UInt64(version))
                    if *version > 0 && *version <= i64::MAX as u64 =>
                {
                    *version
                }
                None if snapshot.committed_log_offset == OffsetWatermark::Empty => 0,
                _ => {
                    return Err(ServerError::new(
                        ErrorCode::DataCorruption,
                        "native key-value checkpoint has an invalid mutation version",
                    ))
                }
            };
        }
        Ok(bootstrap)
    }

    /// Returns the last completely checkpointed key-value mutation version.
    pub const fn persisted_state_version(&self) -> u64 {
        self.persisted_state_version
    }

    /// Returns the immutable table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Borrows the table-kind-specific durable definition.
    #[must_use]
    pub const fn table_definition(&self) -> &TableDefinition {
        &self.table_definition
    }

    /// Returns the schema version accepted by this startup generation.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Borrows the decoded Arrow schema used by native writers.
    #[must_use]
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the latest Catalog writer epoch recovered at startup.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the manifest-covered logical offset recovered at startup.
    #[must_use]
    pub const fn persisted_log_offset(&self) -> OffsetWatermark {
        self.persisted_log_offset
    }
}

/// Context supplied exactly once when a newly accepted or recovered mutation becomes visible.
#[derive(Debug, Clone)]
pub struct NativeIngestApplyContext {
    /// The table and producer epoch that own the mutation.
    pub identity: IngestWalIdentity,
    /// The exact schema version verified before application.
    pub schema_version: u64,
    /// Stable offsets and acknowledgement identity assigned by the authority.
    pub receipt: WriteReceipt,
    /// Whether this application occurs during startup WAL replay.
    pub replay: bool,
}

/// Classification of a native materializer failure relative to visible table state.
#[derive(Debug, Clone)]
pub enum NativeIngestSinkError {
    /// Validation failed before any mutation became visible and the WAL append may be rolled back.
    Rejected(ServerError),
    /// The materializer may have changed visibility and the table authority must stop.
    Indeterminate(ServerError),
}

impl NativeIngestSinkError {
    fn into_error(self) -> ServerError {
        match self {
            Self::Rejected(error) | Self::Indeterminate(error) => error,
        }
    }
}

impl From<ServerError> for NativeIngestSinkError {
    fn from(error: ServerError) -> Self {
        Self::Rejected(error)
    }
}

/// Durable manifest boundary observed through the table actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePersistedState {
    /// Catalog snapshot whose publication completed before this observation.
    pub snapshot_version: u64,
    /// Complete logical prefix covered by that snapshot.
    pub committed_log_offset: OffsetWatermark,
}

/// Server-owned mutation materializer behind the producer and WAL authority.
pub trait NativeIngestSink: Send + Sync {
    /// Reports whether this sink can prepare manifest-backed recovery checkpoints.
    fn supports_checkpoint(&self) -> bool {
        false
    }

    /// Freezes the accepted prefix while admission is serialized, then persists it asynchronously.
    ///
    /// :param watermark: Complete accepted mutation boundary, already covered by WAL fsync.
    /// :returns: A job completing only after the manifest durably covers the prefix.
    fn prepare_checkpoint(
        &self,
        _table_id: TableId,
        _watermark: OffsetWatermark,
    ) -> Result<Option<BoxFuture<'static, Result<OffsetWatermark>>>> {
        Ok(None)
    }

    /// Returns the maximum rows accepted atomically in one frame, if supported.
    fn frame_row_capacity(&self) -> Option<usize> {
        None
    }

    /// Observes the manifest boundary after a requested checkpoint completes.
    fn persisted_state(&self, _table_id: TableId) -> BoxFuture<'_, Result<NativePersistedState>> {
        Box::pin(async {
            Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native sink does not expose a durable manifest boundary",
            ))
        })
    }

    /// Installs one Catalog-committed writer epoch before the first mutation uses it.
    ///
    /// Implementations must be idempotent because retrying the same producer acquisition replays
    /// the Catalog response and invokes this hook again.
    fn fence_writer(&self, _table_id: TableId, _writer_epoch: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Applies one non-duplicate mutation to process-local table state.
    ///
    /// The implementation must make each call atomic at its visible table boundary. A normal error
    /// means no part of the mutation became visible and allows the authority to roll back its
    /// uncommitted WAL append. A panic is treated as an unknown outcome and terminally closes the
    /// table authority.
    fn apply(
        &self,
        context: NativeIngestApplyContext,
        mutation: &TableMutation,
    ) -> std::result::Result<(), NativeIngestSinkError>;
}

/// Point-in-time lifecycle and hot-path counters for native ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeIngestStatus {
    /// Whether new opens and writes are admitted.
    pub accepting: bool,
    /// Operations and durable acknowledgements that have not returned.
    pub active_operations: usize,
    /// Number of recovered table authorities.
    pub table_count: usize,
    /// Newly applied batches, excluding exact retries.
    pub write_batches_total: u64,
    /// Newly applied logical rows.
    pub written_rows_total: u64,
    /// Exact sequence retries served without reapplication.
    pub duplicate_writes_total: u64,
    /// Successful WAL fsync group boundaries.
    pub wal_commits_total: u64,
    /// Catalog-backed producer acquisitions that installed a writer epoch.
    pub writer_acquisitions_total: u64,
    /// Writer opens that reused the active producer, connection, and epoch.
    pub writer_resumes_total: u64,
    /// Rejected opens or writes.
    pub rejected_operations_total: u64,
    /// Bytes removed from incomplete WAL tails during startup.
    pub recovered_truncated_tail_bytes: u64,
    /// Successfully committed recovery checkpoints (not idle reclamation passes).
    pub checkpoints_total: u64,
    /// Maintenance attempts deferred by an explicit retryable persistence admission limit.
    pub checkpoint_backpressure_total: u64,
}

/// Final state of one bounded native ingest shutdown attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeIngestShutdownOutcome {
    /// Every admitted operation drained and every pending WAL record was committed.
    Complete,
    /// The supplied absolute deadline elapsed while admitted work remained.
    TimedOut,
    /// Draining completed but a table WAL could not be committed.
    Failed(ServerError),
}

impl NativeIngestShutdownOutcome {
    /// Returns whether the authority closed without data-path errors.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Default)]
struct NativeIngestMetrics {
    write_batches_total: u64,
    written_rows_total: u64,
    duplicate_writes_total: u64,
    wal_commits_total: u64,
    writer_acquisitions_total: u64,
    writer_resumes_total: u64,
    rejected_operations_total: u64,
    recovered_truncated_tail_bytes: u64,
    checkpoints_total: u64,
    checkpoint_backpressure_total: u64,
}

struct NativeIngestAdmission {
    accepting: bool,
    active_operations: usize,
}

struct NativeIngestInner {
    config: NativeIngestConfig,
    wal_root: PathBuf,
    catalog: Arc<dyn CatalogWrite>,
    sink: Arc<dyn NativeIngestSink>,
    tables: std::sync::RwLock<BTreeMap<TableId, Arc<TableAuthority>>>,
    registration: Mutex<()>,
    admission: Mutex<NativeIngestAdmission>,
    drained: Condvar,
    metrics: Mutex<NativeIngestMetrics>,
}

struct TableAuthority {
    table_id: TableId,
    table_definition: TableDefinition,
    schema_version: u64,
    schema: SchemaRef,
    open_gate: Arc<tokio::sync::Mutex<()>>,
    state: Mutex<TableAuthorityState>,
    archive: Mutex<ReceiptArchive>,
    retirement: Arc<OnceLock<zippy_types::table::TableRetirementReason>>,
}

struct TableAuthorityState {
    current_epoch: u64,
    current_producer: Option<ProducerId>,
    active_connection: Option<ConnectionId>,
    transitioning: bool,
    committed_log_offset: OffsetWatermark,
    last_accepted_sequence: SequenceWatermark,
    last_durable_sequence: SequenceWatermark,
    last_archived_sequence: SequenceWatermark,
    last_checkpoint_sequence: SequenceWatermark,
    pending_checkpoint: Option<WalCheckpoint>,
    receipts: BTreeMap<u64, CachedWrite>,
    request_sequences: HashMap<RequestId, u64>,
    wal: Option<Arc<Mutex<IngestWalWriter>>>,
    pending_durable_acks: usize,
    terminal_error: Option<ServerError>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CachedWrite {
    pub request_id: RequestId,
    pub fingerprint: MutationFingerprint,
    pub receipt: WriteReceipt,
}

struct NativeIngestOperationPermit {
    inner: Arc<NativeIngestInner>,
}

impl Drop for NativeIngestOperationPermit {
    fn drop(&mut self) {
        let mut admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.active_operations = admission.active_operations.saturating_sub(1);
        if admission.active_operations == 0 {
            self.inner.drained.notify_all();
        }
    }
}

/// Cloneable service surface that cannot extend the authority owner's lifetime.
#[derive(Clone)]
pub struct NativeIngestHandle {
    inner: Weak<NativeIngestInner>,
}

/// Admitted table-management operation owned until installation or failure completes.
///
/// Acquire before committing Catalog metadata so shutdown drains the entire operation.
pub struct NativeTableRegistration {
    inner: Arc<NativeIngestInner>,
    operation: NativeIngestOperationPermit,
}

/// A persisted table prefix protected against schema changes until its consumer ACK commits.
///
/// Retain this guard through the Catalog command. Shutdown drains its lifecycle admission,
/// and table transitions cannot replace the schema while its gate is held. Ordinary writes
/// may continue after the captured prefix.
pub struct NativeTableCheckpointGuard {
    _gate: tokio::sync::OwnedMutexGuard<()>,
    _operation: NativeIngestOperationPermit,
}

impl NativeTableRegistration {
    /// Installs one table while retaining the original lifecycle admission.
    pub async fn register(self, bootstrap: NativeIngestTableBootstrap) -> Result<()> {
        let inner = self.inner;
        tokio::task::spawn_blocking(move || {
            let _operation = self.operation;
            let _registration = inner.registration.lock().unwrap();
            let table_id = bootstrap.table_id;
            if inner.tables.read().unwrap().contains_key(&table_id) {
                return Err(ServerError::new(
                    ErrorCode::AlreadyExists,
                    "native ingest table already exists",
                )
                .with_table_id(table_id));
            }
            let (archive, checkpoint, recovery) =
                open_table_recovery(&inner.wal_root, inner.config.wal, &bootstrap)?;
            let authority = recover_table_authority(
                &inner.config,
                bootstrap,
                recovery.records,
                inner.sink.as_ref(),
                archive,
                checkpoint.as_ref(),
            )?;
            inner
                .tables
                .write()
                .unwrap()
                .insert(table_id, Arc::new(authority));
            Ok(())
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("online table recovery task failed error=[{error}]"),
            )
        })?
    }
}

/// Exclusive, reversible write fencing for a server-owned table transition.
///
/// Holds lifecycle admission so shutdown waits for the complete Catalog/runtime mutation.
/// Server must retain this guard in a cancellation-independent task until Catalog commit is
/// known. Dropping before commit restores admission; call `retire` after a durable tombstone or schema change.
pub struct NativeTableTransitionGuard {
    inner: Arc<NativeIngestInner>,
    table: Arc<TableAuthority>,
    _operation: NativeIngestOperationPermit,
    _gate: tokio::sync::OwnedMutexGuard<()>,
    committed: bool,
}

impl NativeTableTransitionGuard {
    /// Persists every admitted mutation while this guard excludes new writers and checkpoints.
    ///
    /// Use in a cancellation-independent management task before committing a schema change.
    /// Failure leaves the previous schema active when the guard is dropped.
    pub async fn checkpoint(&self) -> Result<()> {
        if !self.inner.sink.supports_checkpoint() {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "table schema changes require durable checkpoint support",
            )
            .with_retryable(false));
        }
        run_checkpoint(Arc::clone(&self.inner), Arc::clone(&self.table), true).await?;
        let persisted = self.inner.sink.persisted_state(self.table.table_id).await?;
        let state = lock_table_state(&self.table);
        if !watermark_covers(persisted.committed_log_offset, state.committed_log_offset) {
            return Err(internal_table_error(
                self.table.table_id,
                "table transition checkpoint did not cover admitted mutations",
            ));
        }
        Ok(())
    }

    /// Permanently rejects retained writers and removes this table from live ingest lookup.
    ///
    /// Call only after the corresponding Catalog change committed. WAL files stay untouched:
    /// their physical reclamation is a separate operation, never an implicit directory erase.
    pub fn retire(mut self, reason: zippy_types::table::TableRetirementReason) {
        {
            let mut state = lock_table_state(&self.table);
            self.table.retirement.get_or_init(|| reason);
            state.active_connection = None;
            state.wal = None;
        }
        self.inner
            .tables
            .write()
            .unwrap()
            .remove(&self.table.table_id);
        self.committed = true;
    }
}

impl Drop for NativeTableTransitionGuard {
    fn drop(&mut self) {
        if !self.committed {
            lock_table_state(&self.table).transitioning = false;
        }
    }
}

impl NativeIngestHandle {
    /// Persists one table and holds its schema stable through a subsequent Catalog command.
    ///
    /// The admitted checkpoint survives cancellation. Dropping the returned guard releases
    /// both lifecycle admission and the table transition gate; no other table is checkpointed.
    pub async fn checkpoint_table(
        &self,
        table_id: TableId,
        expected_schema_version: u64,
    ) -> Result<NativeTableCheckpointGuard> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let operation = inner.admit()?;
        let table = inner.table(table_id)?;
        tokio::spawn(async move {
            let gate = Arc::clone(&table.open_gate).lock_owned().await;
            {
                let state = lock_table_state(&table);
                require_table_healthy(&table, &state)?;
                if table.schema_version != expected_schema_version {
                    return Err(ServerError::new(
                        ErrorCode::SchemaMismatch,
                        "consumer checkpoint schema differs from the active table",
                    )
                    .with_table_id(table_id));
                }
            }
            if !inner.sink.supports_checkpoint() {
                return Err(ServerError::new(
                    ErrorCode::UnsupportedPlan,
                    "consumer acknowledgement requires durable checkpoint support",
                )
                .with_table_id(table_id));
            }
            if let Err(error) = run_checkpoint(Arc::clone(&inner), Arc::clone(&table), false).await
            {
                record_checkpoint_error(&inner, &table, error.clone());
                return Err(error);
            }
            Ok(NativeTableCheckpointGuard {
                _operation: operation,
                _gate: gate,
            })
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("consumer checkpoint task failed error=[{error}]"),
            )
        })?
    }

    /// Returns whether the live authority has installed one immutable table identity.
    pub fn contains_table(&self, table_id: TableId) -> Result<bool> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let contains = inner.tables.read().unwrap().contains_key(&table_id);
        Ok(contains)
    }

    /// Fences one idle table without waiting behind a producer transition or checkpoint.
    ///
    /// Pending durable acknowledgements must complete first; busy rejection is retryable and
    /// leaves the table unchanged. Accepted memory writes are serialized by the table lock.
    pub fn begin_table_transition(&self, table_id: TableId) -> Result<NativeTableTransitionGuard> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let operation = inner.admit()?;
        let table = inner.table(table_id)?;
        let gate = Arc::clone(&table.open_gate).try_lock_owned().map_err(|_| {
            backpressure("table transition is waiting for producer transition or checkpoint")
                .with_table_id(table_id)
        })?;
        {
            let mut state = lock_table_state(&table);
            require_table_healthy(&table, &state)?;
            if state.transitioning || state.pending_durable_acks != 0 {
                return Err(backpressure(
                    "table transition is waiting for admitted durable writes",
                )
                .with_table_id(table_id));
            }
            state.transitioning = true;
        }
        Ok(NativeTableTransitionGuard {
            inner,
            table,
            _operation: operation,
            _gate: gate,
            committed: false,
        })
    }

    /// Reports whether this authority can commit manifest-backed acknowledgements.
    #[must_use]
    pub fn supports_persisted(&self) -> bool {
        self.inner
            .upgrade()
            .is_some_and(|inner| inner.sink.supports_checkpoint())
    }

    /// Reports the installed frame sink's atomic row bound.
    pub fn frame_row_capacity(&self) -> Option<usize> {
        self.inner
            .upgrade()
            .and_then(|inner| inner.sink.frame_row_capacity())
    }

    /// Reserves lifecycle admission before a durable table-management command.
    ///
    /// Shutdown waits for this guard, including Catalog work preceding registration.
    pub fn begin_table_registration(&self) -> Result<NativeTableRegistration> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let operation = inner.admit()?;
        Ok(NativeTableRegistration { inner, operation })
    }

    /// Recovers and installs one Catalog-backed table after its actor and sink exist.
    pub async fn register_table(&self, bootstrap: NativeIngestTableBootstrap) -> Result<()> {
        self.begin_table_registration()?.register(bootstrap).await
    }

    /// Persists and checkpoints each accepted table prefix, including receipt and WAL retirement.
    ///
    /// Concurrent writes may continue after the captured prefix. Cancellation does not cancel
    /// an admitted checkpoint; shutdown waits for its operation permit.
    pub async fn checkpoint(&self) -> Result<()> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let operation = inner.admit()?;
        tokio::spawn(async move {
            let _operation = operation;
            for table in &inner.table_authorities() {
                let _gate = table.open_gate.lock().await;
                if let Err(error) =
                    run_checkpoint(Arc::clone(&inner), Arc::clone(table), false).await
                {
                    record_checkpoint_error(&inner, table, error.clone());
                    return Err(error);
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("checkpoint task failed error=[{error}]"),
            )
            .with_retryable(false)
        })?
    }
}

/// Exclusive lifecycle owner for producer fencing, WAL recovery, and table authorities.
pub struct NativeIngestRuntime {
    inner: Arc<NativeIngestInner>,
    shutdown: Option<NativeIngestShutdownOutcome>,
    maintenance_stop: Option<std::sync::mpsc::SyncSender<()>>,
    maintenance_thread: Option<std::thread::JoinHandle<()>>,
}

impl NativeIngestRuntime {
    /// Recovers the configured table WALs and publishes admission only after every replay succeeds.
    pub fn start(
        config: NativeIngestConfig,
        wal_root: impl Into<PathBuf>,
        catalog: Arc<dyn CatalogWrite>,
        tables: Vec<NativeIngestTableBootstrap>,
        sink: Arc<dyn NativeIngestSink>,
    ) -> Result<Self> {
        config.validate()?;
        let wal_root = wal_root.into();
        for adjacent in tables.windows(2) {
            if adjacent[0].table_id >= adjacent[1].table_id {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "native ingest bootstraps are not strictly ordered by table id",
                )
                .with_table_id(adjacent[1].table_id)
                .with_retryable(false));
            }
        }
        let mut authorities = BTreeMap::new();
        let mut truncated_tail_bytes = 0_u64;
        for bootstrap in tables {
            let (archive, checkpoint, recovery) =
                open_table_recovery(&wal_root, config.wal, &bootstrap)?;
            if let Some(checkpoint) = &checkpoint {
                crate::wal::reclaim_checkpointed_wal(&wal_root, checkpoint)?;
            }
            truncated_tail_bytes = truncated_tail_bytes
                .checked_add(recovery.truncated_tail_bytes)
                .ok_or_else(|| {
                    resource_exhausted("native ingest recovered byte counter overflow")
                })?;
            let authority = recover_table_authority(
                &config,
                bootstrap,
                recovery.records,
                sink.as_ref(),
                archive,
                checkpoint.as_ref(),
            )?;
            if authorities
                .insert(authority.table_id, Arc::new(authority))
                .is_some()
            {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "native ingest contains a duplicate table authority",
                )
                .with_retryable(false));
            }
        }
        let mut runtime = Self {
            inner: Arc::new(NativeIngestInner {
                config,
                wal_root,
                catalog,
                sink,
                tables: std::sync::RwLock::new(authorities),
                registration: Mutex::new(()),
                admission: Mutex::new(NativeIngestAdmission {
                    accepting: true,
                    active_operations: 0,
                }),
                drained: Condvar::new(),
                metrics: Mutex::new(NativeIngestMetrics {
                    recovered_truncated_tail_bytes: truncated_tail_bytes,
                    ..NativeIngestMetrics::default()
                }),
            }),
            shutdown: None,
            maintenance_stop: None,
            maintenance_thread: None,
        };
        if runtime.inner.sink.supports_checkpoint() {
            let interval = runtime.inner.config.checkpoint_interval;
            let (stop, receiver) = std::sync::mpsc::sync_channel(1);
            let inner = Arc::downgrade(&runtime.inner);
            let worker = std::thread::Builder::new()
                .name("native-ingest-checkpoint".to_string())
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build();
                    let runtime = match runtime {
                        Ok(runtime) => runtime,
                        Err(error) => {
                            if let Some(inner) = inner.upgrade() {
                                for table in &inner.table_authorities() {
                                    lock_table_state(table).terminal_error =
                                        Some(internal_table_error(
                                            table.table_id,
                                            format!("start checkpoint executor error=[{error}]"),
                                        ));
                                }
                            }
                            return;
                        }
                    };
                    while matches!(
                        receiver.recv_timeout(interval),
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                    ) {
                        let Some(inner) = inner.upgrade() else {
                            break;
                        };
                        runtime.block_on(async {
                            let mut jobs = tokio::task::JoinSet::new();
                            for table in &inner.table_authorities() {
                                let inner = Arc::clone(&inner);
                                let table = Arc::clone(table);
                                jobs.spawn(async move { checkpoint_table(inner, table).await });
                            }
                            while let Some(result) = jobs.join_next().await {
                                if let Err(error) = result {
                                    for table in &inner.table_authorities() {
                                        lock_table_state(table).terminal_error =
                                            Some(internal_table_error(
                                                table.table_id,
                                                format!("checkpoint task panicked error=[{error}]"),
                                            ));
                                    }
                                }
                            }
                        });
                    }
                })
                .map_err(|error| {
                    ServerError::new(
                        ErrorCode::Unavailable,
                        format!("start checkpoint worker error=[{error}]"),
                    )
                    .with_retryable(false)
                })?;
            runtime.maintenance_stop = Some(stop);
            runtime.maintenance_thread = Some(worker);
        }
        Ok(runtime)
    }

    /// Builds table bootstraps from the same revision-consistent registry used by table actors.
    pub fn start_from_registry(
        config: NativeIngestConfig,
        wal_root: impl Into<PathBuf>,
        catalog: Arc<dyn CatalogWrite>,
        tables: &TableRuntimeRegistry,
        sink: Arc<dyn NativeIngestSink>,
    ) -> Result<Self> {
        let bootstraps = tables
            .iter()
            .map(|runtime| {
                NativeIngestTableBootstrap::from_catalog_snapshot(
                    runtime.table(),
                    Arc::clone(runtime.schema()),
                    runtime.snapshot(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::start(config, wal_root, catalog, bootstraps, sink)
    }

    /// Returns the lifecycle-safe producer service used by protocol frontends.
    #[must_use]
    pub fn handle(&self) -> NativeIngestHandle {
        NativeIngestHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Returns current lifecycle, recovery, and write counters.
    #[must_use]
    pub fn status(&self) -> NativeIngestStatus {
        self.inner.status()
    }

    /// Irreversibly rejects new writer opens and writes without waiting.
    pub fn close_admission(&self) {
        self.inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepting = false;
        if let Some(stop) = &self.maintenance_stop {
            let _ = stop.try_send(());
        }
    }

    /// Drains admitted work and commits every complete in-process WAL tail by one deadline.
    pub fn shutdown_until(&mut self, deadline: Instant) -> NativeIngestShutdownOutcome {
        if let Some(outcome) = &self.shutdown {
            return outcome.clone();
        }
        self.close_admission();
        if !self.inner.wait_for_operations(deadline) {
            return NativeIngestShutdownOutcome::TimedOut;
        }
        if let Some(worker) = &self.maintenance_thread {
            while !worker.is_finished() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return NativeIngestShutdownOutcome::TimedOut;
                }
                std::thread::sleep(remaining.min(Duration::from_millis(1)));
            }
        }
        if let Some(worker) = self.maintenance_thread.take() {
            if worker.join().is_err() {
                let outcome = NativeIngestShutdownOutcome::Failed(
                    ServerError::new(
                        ErrorCode::Internal,
                        "native checkpoint worker panicked during shutdown",
                    )
                    .with_retryable(false),
                );
                self.shutdown = Some(outcome.clone());
                return outcome;
            }
        }
        for table in &self.inner.table_authorities() {
            let transition = {
                let mut state = lock_table_state(table);
                state.transitioning = true;
                state.active_connection = None;
                state
                    .terminal_error
                    .clone()
                    .map_or_else(|| Ok(state.wal.clone()), Err)
            };
            let wal = match transition {
                Ok(wal) => wal,
                Err(error) => {
                    let outcome = NativeIngestShutdownOutcome::Failed(error);
                    self.shutdown = Some(outcome.clone());
                    return outcome;
                }
            };
            let Some(wal) = wal else {
                continue;
            };
            let commit = wal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .commit();
            match commit {
                Ok(commit) => {
                    promote_wal_commit(table, commit);
                    record_wal_commit(self.inner.as_ref(), &commit);
                }
                Err(error) => {
                    let outcome = NativeIngestShutdownOutcome::Failed(error);
                    self.shutdown = Some(outcome.clone());
                    return outcome;
                }
            }
        }
        let outcome = NativeIngestShutdownOutcome::Complete;
        self.shutdown = Some(outcome.clone());
        outcome
    }
}

impl Drop for NativeIngestRuntime {
    fn drop(&mut self) {
        self.close_admission();
    }
}

impl NativeIngestInner {
    fn admit(self: &Arc<Self>) -> Result<NativeIngestOperationPermit> {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !admission.accepting {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "native ingest admission is closed",
            )
            .with_retryable(true));
        }
        if admission.active_operations >= self.config.max_active_operations {
            return Err(backpressure(format!(
                "native ingest active operation budget exhausted maximum=[{}]",
                self.config.max_active_operations
            )));
        }
        admission.active_operations = admission
            .active_operations
            .checked_add(1)
            .ok_or_else(|| resource_exhausted("native ingest active operation count overflow"))?;
        drop(admission);
        Ok(NativeIngestOperationPermit {
            inner: Arc::clone(self),
        })
    }

    fn table_authorities(&self) -> Vec<Arc<TableAuthority>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    fn table(&self, table_id: TableId) -> Result<Arc<TableAuthority>> {
        self.tables
            .read()
            .unwrap()
            .get(&table_id)
            .cloned()
            .ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "native ingest table was not found")
                    .with_table_id(table_id)
                    .with_retryable(false)
            })
    }

    fn status(&self) -> NativeIngestStatus {
        let admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let accepting = admission.accepting;
        let active_operations = admission.active_operations;
        drop(admission);
        let metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        NativeIngestStatus {
            accepting,
            active_operations,
            table_count: self.tables.read().unwrap().len(),
            write_batches_total: metrics.write_batches_total,
            written_rows_total: metrics.written_rows_total,
            duplicate_writes_total: metrics.duplicate_writes_total,
            wal_commits_total: metrics.wal_commits_total,
            writer_acquisitions_total: metrics.writer_acquisitions_total,
            writer_resumes_total: metrics.writer_resumes_total,
            rejected_operations_total: metrics.rejected_operations_total,
            recovered_truncated_tail_bytes: metrics.recovered_truncated_tail_bytes,
            checkpoints_total: metrics.checkpoints_total,
            checkpoint_backpressure_total: metrics.checkpoint_backpressure_total,
        }
    }

    fn wait_for_operations(&self, deadline: Instant) -> bool {
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while admission.active_operations != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timeout) = self
                .drained
                .wait_timeout(admission, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            admission = next;
            if timeout.timed_out() && admission.active_operations != 0 {
                return false;
            }
        }
        true
    }

    fn reject(&self) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.rejected_operations_total = metrics.rejected_operations_total.saturating_add(1);
    }
}

impl IngestService for NativeIngestHandle {
    fn open_writer(&self, request: OpenWriterRequest) -> BoxFuture<'_, Result<OpaqueIngestWriter>> {
        let inner = self.inner.upgrade();
        Box::pin(async move {
            let inner = inner.ok_or_else(authority_stopped)?;
            let task_inner = Arc::clone(&inner);
            let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("open native producer outside a Tokio runtime error=[{error}]"),
                )
                .with_retryable(false)
            })?;
            let operation = match inner.admit() {
                Ok(operation) => operation,
                Err(error) => {
                    inner.reject();
                    return Err(error
                        .with_request_id(request.request_id)
                        .with_table_id(request.table_id));
                }
            };
            runtime
                .spawn(async move {
                    let result = open_writer(Arc::clone(&task_inner), request, operation).await;
                    if result.is_err() {
                        task_inner.reject();
                    }
                    result
                })
                .await
                .map_err(|error| {
                    ServerError::new(
                        ErrorCode::Internal,
                        format!("join native producer acquisition error=[{error}]"),
                    )
                    .with_retryable(false)
                })?
        })
    }
}

async fn open_writer(
    inner: Arc<NativeIngestInner>,
    request: OpenWriterRequest,
    _operation: NativeIngestOperationPermit,
) -> Result<OpaqueIngestWriter> {
    let table = inner.table(request.table_id)?;
    let _open = table.open_gate.lock().await;

    let resumed_info = {
        let state = lock_table_state(&table);
        require_table_healthy(&table, &state)?;
        if state.transitioning {
            return Err(table_unavailable(
                table.table_id,
                "native ingest producer transition is already active",
            ));
        }
        if state.current_producer == Some(request.producer_id)
            && state.active_connection == Some(request.connection_id)
        {
            if let Some(expected) = request.expected_writer_epoch {
                if expected != state.current_epoch {
                    return Err(stale_epoch(
                        table.table_id,
                        state.current_epoch,
                        expected,
                        request.request_id,
                    ));
                }
            }
            Some(WriterInfo {
                table_id: table.table_id,
                producer_id: request.producer_id,
                connection_id: request.connection_id,
                producer_epoch: state.current_epoch,
                schema_version: table.schema_version,
                last_durable_batch_sequence: state.last_durable_sequence,
                last_durable_log_offset: state.committed_log_offset,
            })
        } else {
            None
        }
    };
    if let Some(info) = resumed_info {
        let mut metrics = inner
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.writer_resumes_total = metrics.writer_resumes_total.saturating_add(1);
        drop(metrics);
        return Ok(Box::new(NativeIngestWriter {
            info,
            inner: Arc::downgrade(&inner),
            table: Arc::downgrade(&table),
            retirement: Arc::clone(&table.retirement),
        }));
    }

    let prior_connection = {
        let mut state = lock_table_state(&table);
        require_table_healthy(&table, &state)?;
        if state.transitioning {
            return Err(table_unavailable(
                table.table_id,
                "native ingest producer transition is already active",
            ));
        }
        if state.pending_durable_acks != 0 {
            return Err(resource_exhausted(format!(
                "native ingest producer transition has pending durable acknowledgements count=[{}]",
                state.pending_durable_acks
            ))
            .with_table_id(table.table_id));
        }
        state.transitioning = true;
        state.active_connection.take()
    };

    if let Err(error) = flush_transition_wal(&inner, &table).await {
        restore_transition(&table, prior_connection);
        return Err(error);
    }

    let recovered_producer = {
        let state = lock_table_state(&table);
        (prior_connection.is_none()
            && state.current_producer == Some(request.producer_id)
            && request.expected_writer_epoch == Some(state.current_epoch))
        .then_some(zippy_types::catalog::ProducerDescriptor {
            producer_id: request.producer_id,
            table_id: table.table_id,
            producer_epoch: state.current_epoch,
            last_batch_sequence: state.last_durable_sequence,
            last_committed_log_offset: state.committed_log_offset,
        })
    };
    // Recovery must install the actor fence and reopen a WAL segment, just like an exact
    // acquisition replay, while keeping the durable epoch and receipt sequence unchanged.
    let recovered_resume = recovered_producer.is_some();
    let producer = if let Some(producer) = recovered_producer {
        producer
    } else {
        let command = CatalogCommandEnvelopeV1::new(
            request.request_id,
            None,
            inner.config.catalog_actor.clone(),
            CatalogCommandV1::AcquireProducer(AcquireProducerCommand {
                table_id: request.table_id,
                producer_id: request.producer_id,
                expected_writer_epoch: request.expected_writer_epoch,
            }),
        );
        let command = match command {
            Ok(command) => command,
            Err(error) => {
                restore_transition(&table, prior_connection);
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    format!("build producer acquisition command error=[{error}]"),
                )
                .with_request_id(request.request_id)
                .with_table_id(request.table_id)
                .with_retryable(false));
            }
        };
        let response = match inner.catalog.command(command.into()).await {
            Ok(response) => response,
            Err(error) => {
                restore_transition(&table, prior_connection);
                return Err(error);
            }
        };
        let VersionedCatalogResponse::V1(response) = response;
        if response.request_id != request.request_id {
            restore_transition(&table, prior_connection);
            return Err(internal_table_error(
                table.table_id,
                "Catalog producer response request identity mismatch",
            ));
        }
        let CatalogOutcomeV1::Producer(producer) = response.outcome else {
            restore_transition(&table, prior_connection);
            return Err(internal_table_error(
                table.table_id,
                "Catalog producer acquisition returned another outcome",
            ));
        };
        if producer.table_id != table.table_id || producer.producer_id != request.producer_id {
            restore_transition(&table, prior_connection);
            return Err(internal_table_error(
                table.table_id,
                "Catalog producer descriptor identity mismatch",
            ));
        }

        producer
    };

    let sink = Arc::clone(&inner.sink);
    let table_id = table.table_id;
    let producer_epoch = producer.producer_epoch;
    let fence_result =
        tokio::spawn(async move { sink.fence_writer(table_id, producer_epoch).await })
            .await
            .unwrap_or_else(|_| {
                Err(internal_table_error(
                    table_id,
                    "panic while installing native ingest writer epoch in the materializer",
                ))
            });
    if let Err(error) = fence_result {
        let mut state = lock_table_state(&table);
        if producer.producer_epoch > state.current_epoch {
            install_new_epoch(&mut state, producer.producer_epoch, producer.producer_id);
        }
        state.transitioning = false;
        state.active_connection = None;
        return Err(error
            .with_request_id(request.request_id)
            .with_table_id(table.table_id));
    }

    let needs_wal = {
        let state = lock_table_state(&table);
        if producer.producer_epoch < state.current_epoch {
            drop(state);
            restore_transition(&table, prior_connection);
            return Err(stale_epoch(
                table.table_id,
                state_epoch(&table),
                producer.producer_epoch,
                request.request_id,
            ));
        }
        if producer.producer_epoch == state.current_epoch {
            if state
                .current_producer
                .is_some_and(|current| current != producer.producer_id)
            {
                drop(state);
                restore_transition(&table, prior_connection);
                return Err(internal_table_error(
                    table.table_id,
                    "Catalog reused one producer epoch for another producer identity",
                ));
            }
            state.wal.is_none()
        } else {
            if producer.last_batch_sequence != SequenceWatermark::Empty {
                drop(state);
                restore_transition(&table, prior_connection);
                return Err(internal_table_error(
                    table.table_id,
                    "new Catalog producer epoch did not reset its sequence watermark",
                ));
            }
            true
        }
    };
    let identity = IngestWalIdentity {
        table_id: table.table_id,
        producer_id: request.producer_id,
        producer_epoch: producer.producer_epoch,
    };
    let opened_wal = if needs_wal {
        match IngestWalWriter::create_next(&inner.wal_root, identity, inner.config.wal) {
            Ok(wal) => Some(Arc::new(Mutex::new(wal))),
            Err(error) => {
                let mut state = lock_table_state(&table);
                if producer.producer_epoch > state.current_epoch {
                    install_new_epoch(&mut state, producer.producer_epoch, producer.producer_id);
                }
                state.transitioning = false;
                state.active_connection = None;
                return Err(error.with_request_id(request.request_id));
            }
        }
    } else {
        None
    };

    let info = {
        let mut state = lock_table_state(&table);
        if producer.producer_epoch < state.current_epoch {
            state.transitioning = false;
            state.active_connection = prior_connection;
            return Err(stale_epoch(
                table.table_id,
                state.current_epoch,
                producer.producer_epoch,
                request.request_id,
            ));
        }
        if producer.producer_epoch > state.current_epoch {
            install_new_epoch(&mut state, producer.producer_epoch, producer.producer_id);
        } else {
            state.current_producer = Some(producer.producer_id);
        }
        if let Some(wal) = opened_wal {
            state.wal = Some(wal);
        }
        state.active_connection = Some(request.connection_id);
        state.transitioning = false;
        WriterInfo {
            table_id: table.table_id,
            producer_id: request.producer_id,
            connection_id: request.connection_id,
            producer_epoch: producer.producer_epoch,
            schema_version: table.schema_version,
            last_durable_batch_sequence: state.last_durable_sequence,
            last_durable_log_offset: state.committed_log_offset,
        }
    };

    let mut metrics = inner
        .metrics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if recovered_resume {
        metrics.writer_resumes_total = metrics.writer_resumes_total.saturating_add(1);
    } else {
        metrics.writer_acquisitions_total = metrics.writer_acquisitions_total.saturating_add(1);
    }
    drop(metrics);

    Ok(Box::new(NativeIngestWriter {
        info,
        inner: Arc::downgrade(&inner),
        table: Arc::downgrade(&table),
        retirement: Arc::clone(&table.retirement),
    }))
}

fn install_new_epoch(state: &mut TableAuthorityState, epoch: u64, producer_id: ProducerId) {
    state.current_epoch = epoch;
    state.current_producer = Some(producer_id);
    state.active_connection = None;
    state.last_accepted_sequence = SequenceWatermark::Empty;
    state.last_durable_sequence = SequenceWatermark::Empty;
    state.last_archived_sequence = SequenceWatermark::Empty;
    state.last_checkpoint_sequence = SequenceWatermark::Empty;
    state.pending_checkpoint = None;
    state.receipts.clear();
    state.request_sequences.clear();
    state.wal = None;
}

async fn flush_transition_wal(
    inner: &Arc<NativeIngestInner>,
    table: &Arc<TableAuthority>,
) -> Result<()> {
    let wal = lock_table_state(table).wal.clone();
    let Some(wal) = wal else {
        return Ok(());
    };
    let commit = tokio::task::spawn_blocking(move || {
        wal.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .commit()
    })
    .await
    .map_err(|error| {
        internal_table_error(
            table.table_id,
            format!("join producer transition WAL commit error=[{error}]"),
        )
    })??;
    promote_wal_commit(table, commit);
    record_wal_commit(inner.as_ref(), &commit);
    Ok(())
}

fn restore_transition(table: &TableAuthority, prior_connection: Option<ConnectionId>) {
    let mut state = lock_table_state(table);
    state.transitioning = false;
    state.active_connection = prior_connection;
}

/// One producer-epoch writer bound to a boot-scoped connection lease.
struct NativeIngestWriter {
    info: WriterInfo,
    inner: Weak<NativeIngestInner>,
    table: Weak<TableAuthority>,
    // Preserve a tiny terminal reason without retaining WAL, receipts, or schema allocations.
    retirement: Arc<OnceLock<zippy_types::table::TableRetirementReason>>,
}

impl IngestWriter for NativeIngestWriter {
    fn info(&self) -> &WriterInfo {
        &self.info
    }

    fn write_memory(&mut self, request: WriteRequest) -> Result<WriteReceipt> {
        let (inner, table) = self.authority()?;
        let _operation = inner.admit()?;
        let result = admit_write(&inner, &table, &self.info, request, false);
        if result.is_err() {
            inner.reject();
        }
        result.map(|admission| admission.receipt)
    }

    fn submit_durable(
        &mut self,
        request: WriteRequest,
        target: DurableWriteTarget,
    ) -> Result<OpaqueDurableWriteAck> {
        tokio::runtime::Handle::try_current().map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("submit durable ingest outside a Tokio runtime error=[{error}]"),
            )
            .with_request_id(request.request_id())
            .with_table_id(self.info.table_id)
            .with_retryable(false)
        })?;
        let (inner, table) = self.authority()?;
        if target == DurableWriteTarget::Persisted && !inner.sink.supports_checkpoint() {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native ingest persisted durability requires a manifest checkpoint sink",
            )
            .with_request_id(request.request_id())
            .with_table_id(table.table_id)
            .with_retryable(false));
        }
        let _operation = inner.admit()?;
        let task_operation = match inner.admit() {
            Ok(operation) => operation,
            Err(error) => {
                inner.reject();
                return Err(error
                    .with_request_id(request.request_id())
                    .with_table_id(table.table_id));
            }
        };
        let request_id = request.request_id();
        if let Err(error) =
            reserve_pending_durable_ack(&table, request_id, inner.config.max_pending_durable_acks)
        {
            inner.reject();
            return Err(error);
        }
        let admission = match admit_write(&inner, &table, &self.info, request, true) {
            Ok(admission) => admission,
            Err(error) => {
                release_pending_durable_ack(&table);
                inner.reject();
                return Err(error);
            }
        };
        if admission
            .receipt
            .durability_reached()
            .satisfies(target.into())
        {
            release_pending_durable_ack(&table);
            drop(task_operation);
            return Ok(Box::new(ReadyDurableWriteAck {
                receipt: admission.receipt,
            }));
        }
        let pending = PendingDurableAck {
            table: Arc::clone(&table),
        };
        let task = tokio::spawn(async move {
            let _operation = task_operation;
            let _pending = pending;
            let receipt = if admission
                .receipt
                .durability_reached()
                .satisfies(Durability::Wal)
            {
                admission.receipt
            } else {
                let wal = admission.wal.ok_or_else(|| {
                    internal_table_error(
                        table.table_id,
                        "accepted durable write has no active WAL writer",
                    )
                })?;
                let task_inner = Arc::clone(&inner);
                let task_table = Arc::clone(&table);
                tokio::task::spawn_blocking(move || {
                    let commit = catch_unwind(AssertUnwindSafe(|| {
                        wal.lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .commit()
                    }))
                    .unwrap_or_else(|_| {
                        Err(internal_table_error(
                            task_table.table_id,
                            "panic while committing native ingest WAL",
                        ))
                    });
                    complete_durable_commit(&task_inner, &task_table, request_id, commit)
                })
                .await
                .map_err(|error| {
                    internal_table_error(
                        table.table_id,
                        format!("WAL acknowledgement task failed error=[{error}]"),
                    )
                })??
            };
            if target == DurableWriteTarget::Wal {
                return Ok(receipt);
            }
            let _gate = table.open_gate.lock().await;
            let result =
                persist_write_receipt(Arc::clone(&inner), Arc::clone(&table), receipt).await;
            if let Err(error) = &result {
                record_checkpoint_error(&inner, &table, error.clone());
            }
            result.map_err(|error| {
                error
                    .with_request_id(request_id)
                    .with_table_id(table.table_id)
            })
        });
        Ok(Box::new(NativeDurableWriteAck { request_id, task }))
    }
}

struct PendingDurableAck {
    table: Arc<TableAuthority>,
}

impl Drop for PendingDurableAck {
    fn drop(&mut self) {
        release_pending_durable_ack(&self.table);
    }
}

async fn persist_write_receipt(
    inner: Arc<NativeIngestInner>,
    table: Arc<TableAuthority>,
    receipt: WriteReceipt,
) -> Result<WriteReceipt> {
    run_checkpoint(Arc::clone(&inner), Arc::clone(&table), false).await?;
    let persisted = inner.sink.persisted_state(table.table_id).await?;
    if persisted.snapshot_version == 0
        || !watermark_covers(
            persisted.committed_log_offset,
            receipt.committed_log_offset(),
        )
    {
        return Err(internal_table_error(
            table.table_id,
            "persisted acknowledgement is not covered by a committed manifest",
        ));
    }
    let promoted = promote_receipt(
        &receipt,
        Durability::Persisted,
        Some(persisted.snapshot_version),
    );
    tokio::task::spawn_blocking(move || {
        let mut state = lock_table_state(&table);
        require_table_healthy(&table, &state)?;
        let receipt = table
            .archive
            .lock()
            .unwrap()
            .promote_persisted(state.current_epoch, &promoted)?;
        if let Some(cached) = state.receipts.get_mut(&receipt.batch_sequence()) {
            cached.receipt = receipt.clone();
        }
        Ok(receipt)
    })
    .await
    .map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("persisted receipt archive task failed error=[{error}]"),
        )
    })?
}

fn reserve_pending_durable_ack(
    table: &TableAuthority,
    request_id: RequestId,
    maximum: usize,
) -> Result<()> {
    let mut state = lock_table_state(table);
    require_table_healthy(table, &state)?;
    if state.transitioning {
        return Err(backpressure("native ingest table transition is active")
            .with_request_id(request_id)
            .with_table_id(table.table_id));
    }
    if state.pending_durable_acks >= maximum {
        return Err(backpressure(format!(
            "native ingest pending durable acknowledgement budget exhausted maximum=[{maximum}]"
        ))
        .with_request_id(request_id)
        .with_table_id(table.table_id));
    }
    // Bounded by `maximum` above, so the increment cannot overflow.
    state.pending_durable_acks += 1;
    Ok(())
}

fn release_pending_durable_ack(table: &TableAuthority) {
    let mut state = lock_table_state(table);
    state.pending_durable_acks = state.pending_durable_acks.saturating_sub(1);
}

impl NativeIngestWriter {
    fn authority(&self) -> Result<(Arc<NativeIngestInner>, Arc<TableAuthority>)> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let table = self.table.upgrade().ok_or_else(|| {
            self.retirement
                .get()
                .map(|reason| reason.error(self.info.table_id))
                .unwrap_or_else(authority_stopped)
        })?;
        Ok((inner, table))
    }
}

struct WriteAdmission {
    receipt: WriteReceipt,
    wal: Option<Arc<Mutex<IngestWalWriter>>>,
}

fn admit_write(
    inner: &Arc<NativeIngestInner>,
    table: &Arc<TableAuthority>,
    writer: &WriterInfo,
    request: WriteRequest,
    durable: bool,
) -> Result<WriteAdmission> {
    let mut state = lock_table_state(table);
    require_table_healthy(table, &state)?;
    validate_writer_lease(table, &state, writer, request.request_id())?;
    validate_mutation(table, &request)?;
    let expected_sequence = next_sequence(table.table_id, state.last_accepted_sequence)?;
    if request.batch_sequence() < expected_sequence {
        let cached = lookup_receipt(table, &state, request.batch_sequence())?.ok_or_else(|| {
            ServerError::new(
                ErrorCode::Conflict,
                "producer sequence is older than the retained idempotency window",
            )
            .with_request_id(request.request_id())
            .with_table_id(table.table_id)
            .with_retryable(false)
        })?;
        if cached.request_id != request.request_id()
            || cached.fingerprint != request.mutation_fingerprint()
        {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "producer sequence was retried with another request or mutation fingerprint",
            )
            .with_request_id(request.request_id())
            .with_table_id(table.table_id)
            .with_retryable(false));
        }
        let receipt = cached.receipt.clone();
        let wal = if durable && !receipt.durability_reached().satisfies(Durability::Wal) {
            state.wal.clone()
        } else {
            None
        };
        drop(state);
        let mut metrics = inner
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.duplicate_writes_total = metrics.duplicate_writes_total.saturating_add(1);
        return Ok(WriteAdmission { receipt, wal });
    }
    if request.batch_sequence() > expected_sequence {
        return Err(sequence_gap(
            table.table_id,
            expected_sequence,
            request.batch_sequence(),
            request.request_id(),
        ));
    }
    if state.receipts.len() >= inner.config.max_receipts_per_epoch {
        return Err(resource_exhausted(format!(
            "native ingest idempotency receipt limit exhausted maximum=[{}]",
            inner.config.max_receipts_per_epoch
        ))
        .with_request_id(request.request_id())
        .with_table_id(table.table_id));
    }
    if let Some(sequence) = lookup_request_sequence(table, &state, request.request_id())? {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "ingest request identity is already bound to another sequence sequence=[{sequence}]"
            ),
        )
        .with_request_id(request.request_id())
        .with_table_id(table.table_id)
        .with_retryable(false));
    }

    let receipt = allocate_receipt(&request, state.committed_log_offset, Durability::Memory)
        .map_err(|error| error.with_table_id(table.table_id))?;
    let wal = state.wal.clone().ok_or_else(|| {
        internal_table_error(table.table_id, "active producer has no WAL segment")
            .with_request_id(request.request_id())
    })?;
    let previous_batch_sequence = match state.last_accepted_sequence {
        SequenceWatermark::Empty => None,
        SequenceWatermark::Known(sequence) => Some(sequence),
    };
    let previous_log_offset = state.committed_log_offset;
    let mut wal_writer = wal
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let position = match wal_writer.append(&request, &receipt) {
        Ok(position) => position,
        Err(error) => {
            state.terminal_error = Some(error.clone());
            return Err(error.with_request_id(request.request_id()));
        }
    };
    let context = NativeIngestApplyContext {
        identity: IngestWalIdentity {
            table_id: table.table_id,
            producer_id: writer.producer_id,
            producer_epoch: writer.producer_epoch,
        },
        schema_version: request.schema_version(),
        receipt: receipt.clone(),
        replay: false,
    };
    match catch_unwind(AssertUnwindSafe(|| {
        inner.sink.apply(context, request.mutation())
    })) {
        Ok(Ok(())) => {}
        Ok(Err(NativeIngestSinkError::Rejected(error))) => {
            if let Err(rollback_error) = wal_writer.rollback_uncommitted_append(
                position,
                previous_batch_sequence,
                previous_log_offset,
            ) {
                let rollback_error = ServerError::new(
                    ErrorCode::Internal,
                    format!(
                        "rollback rejected native ingest mutation failed error=[{rollback_error}]"
                    ),
                )
                .with_request_id(request.request_id())
                .with_table_id(table.table_id)
                .with_retryable(false);
                state.terminal_error = Some(rollback_error.clone());
                return Err(rollback_error);
            }
            return Err(error
                .with_request_id(request.request_id())
                .with_table_id(table.table_id));
        }
        Ok(Err(NativeIngestSinkError::Indeterminate(error))) => {
            let error = error
                .with_request_id(request.request_id())
                .with_table_id(table.table_id)
                .with_retryable(false);
            state.terminal_error = Some(error.clone());
            return Err(error);
        }
        Err(_) => {
            let _ = wal_writer.rollback_uncommitted_append(
                position,
                previous_batch_sequence,
                previous_log_offset,
            );
            let error = internal_table_error(
                table.table_id,
                "native ingest sink panicked with an unknown visibility outcome",
            )
            .with_request_id(request.request_id());
            state.terminal_error = Some(error.clone());
            return Err(error);
        }
    }
    drop(wal_writer);
    state.last_accepted_sequence = SequenceWatermark::Known(request.batch_sequence());
    state.committed_log_offset = receipt.committed_log_offset();
    state
        .request_sequences
        .insert(request.request_id(), request.batch_sequence());
    state.receipts.insert(
        request.batch_sequence(),
        CachedWrite {
            request_id: request.request_id(),
            fingerprint: request.mutation_fingerprint(),
            receipt: receipt.clone(),
        },
    );
    drop(state);

    let mut metrics = inner
        .metrics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    metrics.write_batches_total = metrics.write_batches_total.saturating_add(1);
    metrics.written_rows_total = metrics
        .written_rows_total
        .saturating_add(request.mutation().row_count());
    Ok(WriteAdmission {
        receipt,
        wal: durable.then_some(wal),
    })
}

fn complete_durable_commit(
    inner: &Arc<NativeIngestInner>,
    table: &Arc<TableAuthority>,
    request_id: RequestId,
    commit: Result<WalCommitReceipt>,
) -> Result<WriteReceipt> {
    let mut state = lock_table_state(table);
    let commit = match commit {
        Ok(commit) => commit,
        Err(error) => {
            let error = error
                .with_request_id(request_id)
                .with_table_id(table.table_id);
            state.terminal_error = Some(error.clone());
            return Err(error);
        }
    };
    promote_wal_commit_locked(&mut state, commit);
    let sequence = lookup_request_sequence(table, &state, request_id)?.ok_or_else(|| {
        internal_table_error(
            table.table_id,
            "durable acknowledgement request disappeared from idempotency state",
        )
        .with_request_id(request_id)
    })?;
    let receipt = lookup_receipt(table, &state, sequence)?
        .map(|cached| cached.receipt)
        .ok_or_else(|| {
            internal_table_error(
                table.table_id,
                "durable acknowledgement sequence disappeared from idempotency state",
            )
            .with_request_id(request_id)
        })?;
    if !receipt.durability_reached().satisfies(Durability::Wal) {
        let error = internal_table_error(
            table.table_id,
            "WAL commit did not cover the requested producer sequence",
        )
        .with_request_id(request_id);
        state.terminal_error = Some(error.clone());
        return Err(error);
    }
    drop(state);
    record_wal_commit(inner.as_ref(), &commit);
    Ok(receipt)
}

fn record_wal_commit(inner: &NativeIngestInner, commit: &WalCommitReceipt) {
    if commit.committed_records == 0 {
        return;
    }
    let mut metrics = inner
        .metrics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    metrics.wal_commits_total = metrics.wal_commits_total.saturating_add(1);
}

fn promote_wal_commit(table: &TableAuthority, commit: WalCommitReceipt) {
    let mut state = lock_table_state(table);
    promote_wal_commit_locked(&mut state, commit);
}

fn promote_wal_commit_locked(state: &mut TableAuthorityState, commit: WalCommitReceipt) {
    let SequenceWatermark::Known(last_batch_sequence) = commit.last_batch_sequence else {
        return;
    };
    let start = match state.last_durable_sequence {
        SequenceWatermark::Known(current) if current >= last_batch_sequence => return,
        SequenceWatermark::Known(current) => Excluded(current),
        SequenceWatermark::Empty => Unbounded,
    };
    state.last_durable_sequence = SequenceWatermark::Known(last_batch_sequence);
    // Each receipt is promoted once per epoch, including when group commits
    // complete out of order. Never rescan the already durable prefix.
    for cached in state
        .receipts
        .range_mut((start, Included(last_batch_sequence)))
    {
        let receipt = &cached.1.receipt;
        if receipt.durability_reached() == Durability::Memory {
            cached.1.receipt = promote_receipt(receipt, Durability::Wal, None);
        }
    }
}

struct NativeDurableWriteAck {
    request_id: RequestId,
    task: tokio::task::JoinHandle<Result<WriteReceipt>>,
}

impl DurableWriteAck for NativeDurableWriteAck {
    fn request_id(&self) -> RequestId {
        self.request_id
    }

    fn wait(self: Box<Self>) -> BoxFuture<'static, Result<WriteReceipt>> {
        Box::pin(async move {
            self.task.await.map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("join native durable acknowledgement error=[{error}]"),
                )
                .with_request_id(self.request_id)
                .with_retryable(false)
            })?
        })
    }
}

struct ReadyDurableWriteAck {
    receipt: WriteReceipt,
}

impl DurableWriteAck for ReadyDurableWriteAck {
    fn request_id(&self) -> RequestId {
        self.receipt.request_id()
    }

    fn wait(self: Box<Self>) -> BoxFuture<'static, Result<WriteReceipt>> {
        Box::pin(async move { Ok(self.receipt) })
    }
}

/// Opens a table's receipt archive, validates its checkpoint, and recovers the WAL suffix.
fn open_table_recovery(
    wal_root: &std::path::Path,
    wal: IngestWalConfig,
    bootstrap: &NativeIngestTableBootstrap,
) -> Result<(
    ReceiptArchive,
    Option<WalCheckpoint>,
    crate::IngestWalRecovery,
)> {
    let archive = ReceiptArchive::open(&crate::wal::table_wal_directory(
        wal_root,
        bootstrap.table_id,
    ))?;
    let checkpoint = archive.checkpoint()?;
    validate_checkpoint(bootstrap, checkpoint.as_ref())?;
    let recovery = crate::wal::recover_table_wal_after(
        wal_root,
        bootstrap.table_id,
        wal,
        &ArrowIpcMutationFingerprintComputer,
        checkpoint.as_ref(),
    )?;
    Ok((archive, checkpoint, recovery))
}

/// An earlier schema checkpoint is valid only behind both the Catalog watermark and a newer
/// writer epoch. Uncovered WAL records still undergo exact current-schema validation.
fn validate_checkpoint(
    bootstrap: &NativeIngestTableBootstrap,
    checkpoint: Option<&WalCheckpoint>,
) -> Result<()> {
    if let Some(checkpoint) = checkpoint {
        if checkpoint.table_id != bootstrap.table_id
            || checkpoint.schema_version == 0
            || checkpoint.schema_version > bootstrap.schema_version
            || checkpoint.producer_epoch > bootstrap.writer_epoch
            || (checkpoint.schema_version < bootstrap.schema_version
                && checkpoint.producer_epoch >= bootstrap.writer_epoch)
            || !watermark_covers(bootstrap.persisted_log_offset, checkpoint.log_offset)
        {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "ingest checkpoint is not covered by the authoritative catalog",
            )
            .with_table_id(bootstrap.table_id)
            .with_retryable(false));
        }
    }
    Ok(())
}

fn recover_table_authority(
    config: &NativeIngestConfig,
    bootstrap: NativeIngestTableBootstrap,
    records: Vec<RecoveredWalRecord>,
    sink: &dyn NativeIngestSink,
    archive: ReceiptArchive,
    checkpoint: Option<&WalCheckpoint>,
) -> Result<TableAuthority> {
    let mut visible_watermark = bootstrap.persisted_log_offset;
    let current_checkpoint =
        checkpoint.filter(|value| value.producer_epoch == bootstrap.writer_epoch);
    let mut current_producer = current_checkpoint.map(|value| value.producer_id);
    let mut current_receipts = BTreeMap::new();
    let mut current_request_sequences = HashMap::new();
    let mut current_last_sequence = current_checkpoint.map_or(SequenceWatermark::Empty, |value| {
        SequenceWatermark::Known(value.last_sequence)
    });

    for record in records {
        if record.identity.producer_epoch > bootstrap.writer_epoch {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    concat!(
                        "ingest WAL producer epoch exceeds Catalog epoch wal=[{}] catalog=[{}]"
                    ),
                    record.identity.producer_epoch, bootstrap.writer_epoch
                ),
            )
            .with_table_id(bootstrap.table_id)
            .with_retryable(false));
        }
        let replay = record_should_replay(bootstrap.table_id, visible_watermark, &record.receipt)?;
        // A schema transition may checkpoint a recovered prefix without opening a writer.
        // Its verified WAL files remain until a later writer owns their reclamation. These
        // records are historical only when Catalog covers the complete mutation and has
        // fenced its epoch. Never reinterpret or replay an older schema as the current one.
        if record.request.schema_version() < bootstrap.schema_version
            && record.identity.producer_epoch < bootstrap.writer_epoch
            && watermark_covers(
                bootstrap.persisted_log_offset,
                record.receipt.committed_log_offset(),
            )
            && !replay
        {
            validate_kind(
                bootstrap.table_id,
                bootstrap.table_definition.kind(),
                record.request.mutation(),
            )?;
            continue;
        }
        validate_recovered_mutation(&bootstrap, &record)?;
        if replay {
            sink.apply(
                NativeIngestApplyContext {
                    identity: record.identity,
                    schema_version: record.request.schema_version(),
                    receipt: record.receipt.clone(),
                    replay: true,
                },
                record.request.mutation(),
            )
            .map_err(NativeIngestSinkError::into_error)?;
            visible_watermark = record.receipt.committed_log_offset();
        }
        if record.identity.producer_epoch == bootstrap.writer_epoch {
            if current_producer.is_some_and(|producer| producer != record.identity.producer_id) {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "one recovered producer epoch contains multiple producer identities",
                )
                .with_table_id(bootstrap.table_id)
                .with_retryable(false));
            }
            if current_receipts.len() >= config.max_receipts_per_epoch {
                return Err(resource_exhausted(format!(
                    "recovered active producer receipts exceed limit maximum=[{}]",
                    config.max_receipts_per_epoch
                ))
                .with_table_id(bootstrap.table_id));
            }
            if current_request_sequences
                .insert(record.request.request_id(), record.request.batch_sequence())
                .is_some()
            {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "recovered active producer reused a request identity",
                )
                .with_table_id(bootstrap.table_id)
                .with_retryable(false));
            }
            current_producer = Some(record.identity.producer_id);
            current_last_sequence = SequenceWatermark::Known(record.request.batch_sequence());
            current_receipts.insert(
                record.request.batch_sequence(),
                CachedWrite {
                    request_id: record.request.request_id(),
                    fingerprint: record.request.mutation_fingerprint(),
                    receipt: record.receipt,
                },
            );
        }
    }

    Ok(TableAuthority {
        table_id: bootstrap.table_id,
        table_definition: bootstrap.table_definition,
        schema_version: bootstrap.schema_version,
        schema: bootstrap.schema,
        open_gate: Arc::new(tokio::sync::Mutex::new(())),
        archive: Mutex::new(archive),
        retirement: Arc::new(OnceLock::new()),
        state: Mutex::new(TableAuthorityState {
            current_epoch: bootstrap.writer_epoch,
            current_producer,
            active_connection: None,
            transitioning: false,
            committed_log_offset: visible_watermark,
            last_accepted_sequence: current_last_sequence,
            last_durable_sequence: current_last_sequence,
            last_archived_sequence: current_checkpoint.map_or(SequenceWatermark::Empty, |value| {
                SequenceWatermark::Known(value.last_sequence)
            }),
            last_checkpoint_sequence: current_checkpoint
                .map_or(SequenceWatermark::Empty, |value| {
                    SequenceWatermark::Known(value.last_sequence)
                }),
            receipts: current_receipts,
            pending_checkpoint: None,
            request_sequences: current_request_sequences,
            wal: None,
            pending_durable_acks: 0,
            terminal_error: None,
        }),
    })
}

async fn checkpoint_table(inner: Arc<NativeIngestInner>, table: Arc<TableAuthority>) {
    let Ok(_operation) = inner.admit() else {
        return;
    };
    let _open_gate = table.open_gate.lock().await;
    let result = run_checkpoint(Arc::clone(&inner), Arc::clone(&table), false).await;
    if let Err(error) = result {
        record_checkpoint_error(&inner, &table, error);
    }
}

fn record_checkpoint_error(inner: &NativeIngestInner, table: &TableAuthority, error: ServerError) {
    if error.code() == ErrorCode::ResourceExhausted && error.retryable() {
        let mut metrics = inner.metrics.lock().unwrap();
        metrics.checkpoint_backpressure_total =
            metrics.checkpoint_backpressure_total.saturating_add(1);
    } else {
        lock_table_state(table).terminal_error = Some(error);
    }
}

async fn run_checkpoint(
    inner: Arc<NativeIngestInner>,
    table: Arc<TableAuthority>,
    transition_owner: bool,
) -> Result<()> {
    let prepare_inner = Arc::clone(&inner);
    let prepare_table = Arc::clone(&table);
    let prepared = tokio::task::spawn_blocking(move || {
        let mut state = lock_table_state(&prepare_table);
        if prepare_table.retirement.get().is_some() {
            return Ok(None);
        }
        require_table_healthy(&prepare_table, &state)?;
        if state.transitioning && !transition_owner {
            return Ok(None);
        }
        let expected_watermark = state.committed_log_offset;
        let (Some(wal), SequenceWatermark::Known(sequence)) =
            (state.wal.clone(), state.last_accepted_sequence)
        else {
            // Recovery or writer handoff has already fsynced the prior WAL prefix. Readers
            // may acknowledge it before the current producer writes its first batch, so
            // persistence must not depend on current-epoch sequence or WAL ownership. Keep
            // prior files until a later writer checkpoint can archive and reclaim them.
            let job = prepare_inner
                .sink
                .prepare_checkpoint(prepare_table.table_id, expected_watermark)?;
            return Ok(job.map(|job| (job, None, None, expected_watermark)));
        };
        let commit = wal.lock().unwrap().commit()?;
        promote_wal_commit_locked(&mut state, commit);
        record_wal_commit(&prepare_inner, &commit);
        let job = prepare_inner
            .sink
            .prepare_checkpoint(prepare_table.table_id, state.committed_log_offset)?;
        let Some(job) = job else {
            return Ok(None);
        };
        let checkpoint = if state.last_checkpoint_sequence == SequenceWatermark::Known(sequence) {
            None
        } else if state
            .pending_checkpoint
            .as_ref()
            .is_some_and(|value| value.last_sequence == sequence)
        {
            state.pending_checkpoint.clone()
        } else {
            let checkpoint = WalCheckpoint {
                table_id: prepare_table.table_id,
                schema_version: prepare_table.schema_version,
                producer_epoch: state.current_epoch,
                producer_id: state.current_producer.ok_or_else(|| {
                    internal_table_error(prepare_table.table_id, "checkpoint producer is missing")
                })?,
                last_sequence: sequence,
                log_offset: state.committed_log_offset,
                through_file: wal.lock().unwrap().rotate(&prepare_inner.wal_root)?,
            };
            state.pending_checkpoint = Some(checkpoint.clone());
            Some(checkpoint)
        };
        let start = match state.last_archived_sequence {
            SequenceWatermark::Empty => Unbounded,
            SequenceWatermark::Known(value) => Excluded(value),
        };
        let receipts = if state.last_archived_sequence == SequenceWatermark::Known(sequence) {
            Vec::new()
        } else {
            state
                .receipts
                .range((start, Included(sequence)))
                .map(|(_, cached)| cached.clone())
                .collect()
        };
        Ok::<_, ServerError>(Some((
            job,
            checkpoint,
            Some((receipts, state.current_epoch, sequence)),
            expected_watermark,
        )))
    })
    .await
    .map_err(|error| {
        internal_table_error(
            table.table_id,
            format!("prepare checkpoint task error=[{error}]"),
        )
    })??;
    let Some((job, checkpoint, archive, expected_watermark)) = prepared else {
        return Ok(());
    };
    if let Some((receipts, epoch, sequence)) =
        archive.filter(|(receipts, _, _)| !receipts.is_empty())
    {
        let archive_table = Arc::clone(&table);
        tokio::task::spawn_blocking(move || {
            archive_table
                .archive
                .lock()
                .unwrap()
                .archive(epoch, &receipts)
        })
        .await
        .map_err(|error| {
            internal_table_error(
                table.table_id,
                format!("archive checkpoint task error=[{error}]"),
            )
        })??;
        let mut state = lock_table_state(&table);
        state.last_archived_sequence = SequenceWatermark::Known(sequence);
        let keep = (inner.config.max_receipts_per_epoch / 2).min(4096);
        while state.receipts.len() > keep
            && state
                .receipts
                .first_key_value()
                .is_some_and(|(key, _)| *key <= sequence)
        {
            let (_, cached) = state.receipts.pop_first().expect("first receipt exists");
            state.request_sequences.remove(&cached.request_id);
        }
    }
    let persisted = job.await?;
    if !watermark_covers(persisted, expected_watermark) {
        return Err(internal_table_error(
            table.table_id,
            "checkpoint job did not persist its complete prefix",
        ));
    }
    if let Some(checkpoint) = checkpoint {
        if !watermark_covers(persisted, checkpoint.log_offset) {
            return Err(internal_table_error(
                table.table_id,
                "checkpoint job did not persist its complete prefix",
            ));
        }
        let checkpoint_table = Arc::clone(&table);
        let wal_root = inner.wal_root.clone();
        let sequence = checkpoint.last_sequence;
        tokio::task::spawn_blocking(move || {
            checkpoint_table
                .archive
                .lock()
                .unwrap()
                .commit_checkpoint(&checkpoint)?;
            crate::wal::reclaim_checkpointed_wal(&wal_root, &checkpoint)
        })
        .await
        .map_err(|error| {
            internal_table_error(
                table.table_id,
                format!("commit checkpoint task error=[{error}]"),
            )
        })??;
        let mut state = lock_table_state(&table);
        state.last_checkpoint_sequence = SequenceWatermark::Known(sequence);
        state.pending_checkpoint = None;
        drop(state);
        let mut metrics = inner.metrics.lock().unwrap();
        metrics.checkpoints_total = metrics.checkpoints_total.saturating_add(1);
    }
    Ok(())
}

fn watermark_covers(cover: OffsetWatermark, target: OffsetWatermark) -> bool {
    match (cover, target) {
        (OffsetWatermark::Empty | OffsetWatermark::Known(_), OffsetWatermark::Empty) => true,
        (OffsetWatermark::Known(cover), OffsetWatermark::Known(target)) => cover >= target,
        _ => false,
    }
}

fn lookup_receipt(
    table: &TableAuthority,
    state: &TableAuthorityState,
    sequence: u64,
) -> Result<Option<CachedWrite>> {
    if let Some(cached) = state.receipts.get(&sequence) {
        return Ok(Some(cached.clone()));
    }
    table
        .archive
        .lock()
        .unwrap()
        .lookup(state.current_epoch, sequence)
}

fn lookup_request_sequence(
    table: &TableAuthority,
    state: &TableAuthorityState,
    request: RequestId,
) -> Result<Option<u64>> {
    if let Some(sequence) = state.request_sequences.get(&request) {
        return Ok(Some(*sequence));
    }
    // Receipt eviction removes only a contiguous prefix. While sequence zero is still
    // hot, the in-memory request index covers the entire current epoch.
    if state.last_accepted_sequence == SequenceWatermark::Empty
        || state
            .receipts
            .first_key_value()
            .is_some_and(|(sequence, _)| *sequence == 0)
    {
        return Ok(None);
    }
    table
        .archive
        .lock()
        .unwrap()
        .request_sequence(state.current_epoch, request)
}

fn record_should_replay(
    table_id: TableId,
    visible: OffsetWatermark,
    receipt: &WriteReceipt,
) -> Result<bool> {
    let Some(offsets) = receipt.offsets() else {
        return Ok(false);
    };
    match visible {
        OffsetWatermark::Empty => {
            if receipt.previous_log_offset() != OffsetWatermark::Empty {
                return Err(recovery_gap(table_id));
            }
            Ok(true)
        }
        OffsetWatermark::Known(current) if offsets.last().get() <= current.get() => Ok(false),
        OffsetWatermark::Known(current) if offsets.first().get() <= current.get() => {
            Err(ServerError::new(
                ErrorCode::DataCorruption,
                "persisted watermark splits one atomic WAL mutation",
            )
            .with_table_id(table_id)
            .with_retryable(false))
        }
        OffsetWatermark::Known(_) => {
            if receipt.previous_log_offset() != visible {
                return Err(recovery_gap(table_id));
            }
            Ok(true)
        }
    }
}

fn validate_recovered_mutation(
    bootstrap: &NativeIngestTableBootstrap,
    record: &RecoveredWalRecord,
) -> Result<()> {
    if record.request.schema_version() != bootstrap.schema_version {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "recovered WAL schema version mismatch expected=[{}] actual=[{}]",
                bootstrap.schema_version,
                record.request.schema_version()
            ),
        )
        .with_table_id(bootstrap.table_id)
        .with_retryable(false));
    }
    validate_kind(
        bootstrap.table_id,
        bootstrap.table_definition.kind(),
        record.request.mutation(),
    )?;
    if !mutation_schema_matches(
        &bootstrap.table_definition,
        bootstrap.schema.as_ref(),
        record.request.mutation(),
    ) {
        return Err(schema_mismatch(
            bootstrap.table_id,
            bootstrap.schema_version,
            record.request.schema_version(),
            record.request.request_id(),
        ));
    }
    Ok(())
}

fn validate_mutation(table: &TableAuthority, request: &WriteRequest) -> Result<()> {
    if request.schema_version() != table.schema_version {
        return Err(schema_mismatch(
            table.table_id,
            table.schema_version,
            request.schema_version(),
            request.request_id(),
        ));
    }
    validate_kind(
        table.table_id,
        table.table_definition.kind(),
        request.mutation(),
    )?;
    if !mutation_schema_matches(
        &table.table_definition,
        table.schema.as_ref(),
        request.mutation(),
    ) {
        return Err(schema_mismatch(
            table.table_id,
            table.schema_version,
            request.schema_version(),
            request.request_id(),
        ));
    }
    Ok(())
}

fn mutation_schema_matches(
    definition: &TableDefinition,
    table_schema: &arrow::datatypes::Schema,
    mutation: &TableMutation,
) -> bool {
    let mutation_schema = mutation_schema(mutation);
    let zippy_types::ingest::TableMutation::KeyValue(
        zippy_types::ingest::KeyValueMutation::Delete(_),
    ) = mutation
    else {
        return mutation_schema.as_ref() == table_schema;
    };
    if mutation_schema.as_ref() == table_schema {
        return true;
    }
    let TableDefinition::KeyValue(key_value) = definition else {
        return false;
    };
    if mutation_schema.fields().len() != key_value.keys().as_slice().len()
        || mutation_schema.metadata() != table_schema.metadata()
    {
        return false;
    }
    key_value
        .keys()
        .as_slice()
        .iter()
        .zip(mutation_schema.fields())
        .all(|(key, actual)| {
            table_schema
                .field_with_name(key.as_str())
                .is_ok_and(|expected| actual.as_ref() == expected)
        })
}

fn validate_kind(table_id: TableId, kind: TableKind, mutation: &TableMutation) -> Result<()> {
    let matches = matches!(
        (kind, mutation),
        (TableKind::Append, TableMutation::Append(_))
            | (TableKind::Frame, TableMutation::Frame(_))
            | (TableKind::KeyValue, TableMutation::KeyValue(_))
    );
    if matches {
        Ok(())
    } else {
        Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "ingest mutation does not match table kind table_kind=[{}]",
                kind.as_str()
            ),
        )
        .with_table_id(table_id)
        .with_retryable(false))
    }
}

fn mutation_schema(mutation: &TableMutation) -> SchemaRef {
    match mutation {
        TableMutation::Append(batch) | TableMutation::Frame(batch) => batch.schema(),
        TableMutation::KeyValue(zippy_types::ingest::KeyValueMutation::Upsert(batch))
        | TableMutation::KeyValue(zippy_types::ingest::KeyValueMutation::Delete(batch)) => {
            batch.schema()
        }
    }
}

fn validate_writer_lease(
    table: &TableAuthority,
    state: &TableAuthorityState,
    writer: &WriterInfo,
    request_id: RequestId,
) -> Result<()> {
    if state.transitioning {
        return Err(table_unavailable(
            table.table_id,
            "native ingest producer transition is active",
        )
        .with_request_id(request_id));
    }
    if writer.producer_epoch != state.current_epoch {
        return Err(stale_epoch(
            table.table_id,
            state.current_epoch,
            writer.producer_epoch,
            request_id,
        ));
    }
    if state.current_producer != Some(writer.producer_id)
        || state.active_connection != Some(writer.connection_id)
    {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "native ingest writer connection lease is no longer active",
        )
        .with_request_id(request_id)
        .with_table_id(table.table_id)
        .with_retryable(false));
    }
    Ok(())
}

fn require_table_healthy(table: &TableAuthority, state: &TableAuthorityState) -> Result<()> {
    if let Some(reason) = table.retirement.get() {
        return Err(reason.error(table.table_id));
    }
    match &state.terminal_error {
        Some(error) => Err(ServerError::new(
            ErrorCode::Unavailable,
            format!("native ingest table authority failed error=[{error}]"),
        )
        .with_table_id(table.table_id)
        .with_retryable(false)),
        None => Ok(()),
    }
}

fn allocate_receipt(
    request: &WriteRequest,
    previous: OffsetWatermark,
    durability: Durability,
) -> Result<WriteReceipt> {
    let rows = request.mutation().row_count();
    let offsets = if rows == 0 {
        None
    } else {
        let first = match previous {
            OffsetWatermark::Empty => LogOffset::new(0),
            OffsetWatermark::Known(offset) => offset.checked_next().ok_or_else(|| {
                resource_exhausted("native ingest logical offset domain is exhausted")
            })?,
        };
        let last = first
            .get()
            .checked_add(rows - 1)
            .map(LogOffset::new)
            .ok_or_else(|| resource_exhausted("native ingest logical offset range is exhausted"))?;
        Some(OffsetRange::new(first, last).expect("checked native offset range is ordered"))
    };
    let committed = offsets.map_or(previous, |range| OffsetWatermark::Known(range.last()));
    WriteReceipt::new(
        request.request_id(),
        request.batch_sequence(),
        rows,
        offsets,
        previous,
        committed,
        durability,
        None,
    )
    .map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("construct native ingest receipt error=[{error}]"),
        )
        .with_request_id(request.request_id())
        .with_retryable(false)
    })
}

pub(crate) fn promote_receipt(
    receipt: &WriteReceipt,
    durability: Durability,
    snapshot_version: Option<u64>,
) -> WriteReceipt {
    WriteReceipt::new(
        receipt.request_id(),
        receipt.batch_sequence(),
        receipt.row_count(),
        receipt.offsets(),
        receipt.previous_log_offset(),
        receipt.committed_log_offset(),
        durability,
        snapshot_version,
    )
    .expect("promoting a validated receipt preserves its invariants")
}

fn next_sequence(table_id: TableId, watermark: SequenceWatermark) -> Result<u64> {
    match watermark {
        SequenceWatermark::Empty => Ok(0),
        SequenceWatermark::Known(sequence) => sequence.checked_add(1).ok_or_else(|| {
            resource_exhausted("native ingest producer sequence domain is exhausted")
                .with_table_id(table_id)
        }),
    }
}

fn lock_table_state(table: &TableAuthority) -> std::sync::MutexGuard<'_, TableAuthorityState> {
    table
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn state_epoch(table: &TableAuthority) -> u64 {
    lock_table_state(table).current_epoch
}

fn stale_epoch(
    table_id: TableId,
    expected: u64,
    actual: u64,
    request_id: RequestId,
) -> ServerError {
    ServerError::new(
        ErrorCode::StaleProducerEpoch,
        format!("stale producer epoch expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::ProducerEpoch { expected, actual })
    .expect("producer epoch details match stale producer error code")
}

fn sequence_gap(
    table_id: TableId,
    expected: u64,
    actual: u64,
    request_id: RequestId,
) -> ServerError {
    ServerError::new(
        ErrorCode::SequenceGap,
        format!("producer sequence gap expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::SequenceGap { expected, actual })
    .expect("sequence details match sequence gap error code")
}

fn schema_mismatch(
    table_id: TableId,
    expected: u64,
    actual: u64,
    request_id: RequestId,
) -> ServerError {
    ServerError::new(
        ErrorCode::SchemaMismatch,
        format!("ingest schema mismatch expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::SchemaVersion { expected, actual })
    .expect("schema details match schema mismatch error code")
}

fn recovery_gap(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        "ingest WAL replay does not continue from the persisted watermark",
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

fn table_unavailable(table_id: TableId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, message)
        .with_table_id(table_id)
        .with_retryable(true)
}

fn authority_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "native ingest authority is no longer running",
    )
    .with_retryable(false)
}

fn resource_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn backpressure(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(true)
}

fn internal_table_error(table_id: TableId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message)
        .with_table_id(table_id)
        .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use serde::Serialize;
    use tempfile::TempDir;
    use zippy_types::catalog::{
        CatalogCommandV1, CatalogResponseV1, CatalogWrite, ProducerDescriptor,
        VersionedCatalogCommand, VersionedCatalogResponse,
    };
    use zippy_types::ingest::{
        Durability, DurableWriteTarget, IncomingWriteRequest, IngestService, KeyValueMutation,
        MutationFingerprintComputer, OpenWriterRequest, TableMutation,
    };
    use zippy_types::table::{
        AppendTableDefinition, ColumnName, ColumnSet, KeyValueTableDefinition, TableDefinition,
    };
    use zippy_types::{
        BoxFuture, ConnectionId, ErrorCode, OffsetWatermark, ProducerId, RequestId, Result,
        SequenceWatermark, ServerError, ServerErrorDetails, TableId,
    };

    use super::{
        ArrowIpcMutationFingerprintComputer, NativeIngestApplyContext, NativeIngestConfig,
        NativeIngestRuntime, NativeIngestShutdownOutcome, NativeIngestSink, NativeIngestSinkError,
        NativeIngestTableBootstrap,
    };

    #[derive(Default)]
    struct FakeCatalogState {
        epoch: u64,
        revision: u64,
        replays: HashMap<RequestId, (VersionedCatalogCommand, VersionedCatalogResponse)>,
    }

    struct FakeCatalogGate {
        block_next: AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl FakeCatalogGate {
        fn new() -> Self {
            Self {
                block_next: AtomicBool::new(true),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            }
        }
    }

    #[derive(Default)]
    struct FakeCatalog {
        state: Arc<Mutex<FakeCatalogState>>,
        gate: Option<Arc<FakeCatalogGate>>,
    }

    impl FakeCatalog {
        fn with_gate(gate: Arc<FakeCatalogGate>) -> Self {
            Self {
                state: Arc::default(),
                gate: Some(gate),
            }
        }

        fn epoch(&self) -> u64 {
            self.state.lock().unwrap().epoch
        }
    }

    impl CatalogWrite for FakeCatalog {
        fn command(
            &self,
            command: VersionedCatalogCommand,
        ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
            let state = Arc::clone(&self.state);
            let gate = self.gate.clone();
            Box::pin(async move {
                if let Some(gate) = gate {
                    if gate.block_next.swap(false, Ordering::AcqRel) {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                }
                let VersionedCatalogCommand::V1(envelope) = &command;
                let mut state = state.lock().unwrap();
                if let Some((stored, response)) = state.replays.get(&envelope.request_id) {
                    if stored == &command {
                        return Ok(response.clone());
                    }
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        "fake Catalog request id conflict",
                    )
                    .with_request_id(envelope.request_id)
                    .with_retryable(false));
                }
                let CatalogCommandV1::AcquireProducer(acquire) = &envelope.command else {
                    return Err(ServerError::new(
                        ErrorCode::UnsupportedPlan,
                        "fake Catalog supports producer acquisition only",
                    )
                    .with_retryable(false));
                };
                if let Some(expected) = acquire.expected_writer_epoch {
                    if expected != state.epoch {
                        return Err(ServerError::new(
                            ErrorCode::StaleProducerEpoch,
                            "fake Catalog producer epoch mismatch",
                        )
                        .with_request_id(envelope.request_id)
                        .with_table_id(acquire.table_id)
                        .with_retryable(false)
                        .with_details(ServerErrorDetails::ProducerEpoch {
                            expected: state.epoch,
                            actual: expected,
                        })
                        .unwrap());
                    }
                }
                state.epoch += 1;
                state.revision += 1;
                let response = VersionedCatalogResponse::V1(CatalogResponseV1 {
                    request_id: envelope.request_id,
                    catalog_revision: state.revision,
                    outcome: zippy_types::catalog::CatalogOutcomeV1::Producer(ProducerDescriptor {
                        producer_id: acquire.producer_id,
                        table_id: acquire.table_id,
                        producer_epoch: state.epoch,
                        last_batch_sequence: SequenceWatermark::Empty,
                        last_committed_log_offset: OffsetWatermark::Empty,
                    }),
                });
                state
                    .replays
                    .insert(envelope.request_id, (command, response.clone()));
                Ok(response)
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct AppliedMutation {
        request_id: RequestId,
        replay: bool,
        row_count: u64,
        committed_log_offset: OffsetWatermark,
    }

    #[derive(Default)]
    struct RecordingSink {
        applied: Mutex<Vec<AppliedMutation>>,
        next_error: Mutex<Option<ServerError>>,
    }

    impl RecordingSink {
        fn applied(&self) -> Vec<AppliedMutation> {
            self.applied.lock().unwrap().clone()
        }

        fn fail_once(&self, error: ServerError) {
            *self.next_error.lock().unwrap() = Some(error);
        }
    }

    impl NativeIngestSink for RecordingSink {
        fn apply(
            &self,
            context: NativeIngestApplyContext,
            mutation: &TableMutation,
        ) -> std::result::Result<(), NativeIngestSinkError> {
            if let Some(error) = self.next_error.lock().unwrap().take() {
                return Err(error.into());
            }
            self.applied.lock().unwrap().push(AppliedMutation {
                request_id: context.receipt.request_id(),
                replay: context.replay,
                row_count: mutation.row_count(),
                committed_log_offset: context.receipt.committed_log_offset(),
            });
            Ok(())
        }
    }

    struct ControlledCheckpointSink {
        records: RecordingSink,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        valid_manifest: bool,
    }

    impl NativeIngestSink for ControlledCheckpointSink {
        fn supports_checkpoint(&self) -> bool {
            true
        }

        fn prepare_checkpoint(
            &self,
            _: TableId,
            watermark: OffsetWatermark,
        ) -> Result<Option<BoxFuture<'static, Result<OffsetWatermark>>>> {
            let entered = Arc::clone(&self.entered);
            let release = Arc::clone(&self.release);
            Ok(Some(Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(watermark)
            })))
        }

        fn persisted_state(
            &self,
            _: TableId,
        ) -> BoxFuture<'_, Result<super::NativePersistedState>> {
            Box::pin(async move {
                Ok(super::NativePersistedState {
                    snapshot_version: 2,
                    committed_log_offset: if self.valid_manifest {
                        self.records.applied().last().unwrap().committed_log_offset
                    } else {
                        OffsetWatermark::Empty
                    },
                })
            })
        }

        fn apply(
            &self,
            context: NativeIngestApplyContext,
            mutation: &TableMutation,
        ) -> std::result::Result<(), NativeIngestSinkError> {
            self.records.apply(context, mutation)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consumer_checkpoint_persists_prior_epoch_before_new_writer_first_batch() {
        let directory = TempDir::new().unwrap();
        let sink = Arc::new(ControlledCheckpointSink {
            records: RecordingSink::default(),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            valid_manifest: true,
        });
        let mut runtime = NativeIngestRuntime::start(
            NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
            directory.path().join("wal"),
            Arc::new(FakeCatalog::default()),
            vec![bootstrap(0)],
            sink.clone(),
        )
        .unwrap();
        let handle = runtime.handle();
        let mut first = handle
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        first.write_memory(write_request(100, 0, &[1, 2])).unwrap();
        drop(first);
        let second = handle
            .open_writer(open_request(11, 21, Some(1)))
            .await
            .unwrap();
        assert_eq!(second.info().producer_epoch, 2);
        assert_eq!(
            second.info().last_durable_batch_sequence,
            SequenceWatermark::Empty
        );
        let checkpoint = tokio::spawn(async move { handle.checkpoint_table(table_id(), 1).await });
        tokio::time::timeout(Duration::from_secs(2), sink.entered.notified())
            .await
            .unwrap();
        assert!(
            !checkpoint.is_finished(),
            "ACK must await prior-prefix persistence"
        );
        sink.release.notify_one();
        let guard = checkpoint.await.unwrap().unwrap();
        drop(guard);
        drop(second);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_ack_survives_cancellation_and_checks_manifest_coverage() {
        for valid_manifest in [true, false] {
            let directory = TempDir::new().unwrap();
            let sink = Arc::new(ControlledCheckpointSink {
                records: RecordingSink::default(),
                entered: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Notify::new()),
                valid_manifest,
            });
            let mut runtime = NativeIngestRuntime::start(
                NativeIngestConfig::new().with_checkpoint_interval(Duration::from_secs(60)),
                directory.path().join("wal"),
                Arc::new(FakeCatalog::default()),
                vec![bootstrap(0)],
                sink.clone(),
            )
            .unwrap();
            let handle = runtime.handle();
            let mut writer = handle
                .open_writer(open_request(10, 20, Some(0)))
                .await
                .unwrap();
            let acknowledgement = writer
                .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Persisted)
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), sink.entered.notified())
                .await
                .unwrap();
            assert_eq!(runtime.status().active_operations, 1);
            assert_eq!(
                runtime.shutdown_until(Instant::now()),
                NativeIngestShutdownOutcome::TimedOut
            );
            if valid_manifest {
                drop(acknowledgement);
                sink.release.notify_one();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while runtime.status().active_operations != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let table = runtime.inner.table(table_id()).unwrap();
                let cached = table.archive.lock().unwrap().lookup(1, 0).unwrap().unwrap();
                assert_eq!(cached.receipt.durability_reached(), Durability::Persisted);
                assert_eq!(cached.receipt.persisted_snapshot_version(), Some(2));
                assert_eq!(
                    runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
                    NativeIngestShutdownOutcome::Complete
                );
            } else {
                sink.release.notify_one();
                assert_eq!(
                    acknowledgement.wait().await.unwrap_err().code(),
                    ErrorCode::Internal
                );
                let table = runtime.inner.table(table_id()).unwrap();
                let cached = table.archive.lock().unwrap().lookup(1, 0).unwrap().unwrap();
                assert_eq!(cached.receipt.durability_reached(), Durability::Wal);
                assert!(cached.receipt.persisted_snapshot_version().is_none());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_unsupported_sink_rejects_before_applying_a_write() {
        let directory = TempDir::new().unwrap();
        let sink = Arc::new(RecordingSink::default());
        let runtime = start_runtime(
            &directory,
            Arc::new(FakeCatalog::default()),
            Arc::clone(&sink),
            0,
        );
        let mut writer = runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        assert!(matches!(writer.submit_durable(write_request(100, 0, &[1]),
            DurableWriteTarget::Persisted), Err(error) if error.code() == ErrorCode::UnsupportedPlan));
        assert!(sink.applied().is_empty());
        assert_eq!(runtime.status().written_rows_total, 0);
    }

    fn table_id() -> TableId {
        TableId::from_u128(1).unwrap()
    }

    fn producer_id() -> ProducerId {
        ProducerId::from_u128(2).unwrap()
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).unwrap()
    }

    fn connection_id(value: u128) -> ConnectionId {
        ConnectionId::from_u128(value).unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    #[test]
    fn schema_checkpoint_requires_covered_watermark_and_strictly_newer_epoch() {
        use crate::checkpoint::WalCheckpoint;
        use zippy_types::LogOffset;

        let bootstrap = NativeIngestTableBootstrap::new(
            table_id(),
            TableDefinition::Append(AppendTableDefinition::default()),
            2,
            schema(),
            3,
            OffsetWatermark::Known(LogOffset::new(9)),
        )
        .unwrap();
        let checkpoint = WalCheckpoint {
            table_id: table_id(),
            schema_version: 1,
            producer_epoch: 2,
            producer_id: producer_id(),
            last_sequence: 1,
            log_offset: OffsetWatermark::Known(LogOffset::new(9)),
            through_file: "00000000000000000002-00000000000000000000.wal".to_owned(),
        };
        super::validate_checkpoint(&bootstrap, Some(&checkpoint)).unwrap();
        for (version, epoch, offset) in [(0, 2, 9), (3, 2, 9), (1, 3, 9), (1, 2, 10)] {
            let mut invalid = checkpoint.clone();
            invalid.schema_version = version;
            invalid.producer_epoch = epoch;
            invalid.log_offset = OffsetWatermark::Known(LogOffset::new(offset));
            assert_eq!(
                super::validate_checkpoint(&bootstrap, Some(&invalid))
                    .unwrap_err()
                    .code(),
                ErrorCode::DataCorruption
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_schema_wal_requires_full_manifest_coverage_and_a_fenced_epoch() {
        use zippy_types::LogOffset;

        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let mut original = start_runtime(
            &directory,
            catalog.clone(),
            Arc::new(RecordingSink::default()),
            0,
        );
        let mut writer = original
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        writer
            .submit_durable(write_request(100, 0, &[1, 2]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        drop(writer);
        assert_eq!(
            original.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
        drop(original);

        for (epoch, persisted, expected_error) in [
            (1, Some(1), Some(ErrorCode::SchemaMismatch)),
            (2, None, Some(ErrorCode::SchemaMismatch)),
            (2, Some(0), Some(ErrorCode::DataCorruption)),
            (0, Some(1), Some(ErrorCode::DataCorruption)),
            (2, Some(1), None),
        ] {
            let sink = Arc::new(RecordingSink::default());
            let next = NativeIngestTableBootstrap::new(
                table_id(),
                TableDefinition::Append(AppendTableDefinition::default()),
                2,
                schema(),
                epoch,
                persisted.map_or(OffsetWatermark::Empty, |offset| {
                    OffsetWatermark::Known(LogOffset::new(offset))
                }),
            )
            .unwrap();
            let result = NativeIngestRuntime::start(
                NativeIngestConfig::new(),
                directory.path().join("wal"),
                catalog.clone(),
                vec![next],
                sink.clone(),
            );
            match (result, expected_error) {
                (Err(error), Some(expected)) => assert_eq!(error.code(), expected),
                (Ok(mut runtime), None) => {
                    assert!(
                        sink.applied().is_empty(),
                        "covered old WAL must not be reapplied"
                    );
                    assert_eq!(
                        runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
                        NativeIngestShutdownOutcome::Complete
                    );
                }
                (Err(error), None) => panic!("covered historical WAL was rejected: {error}"),
                (Ok(_), Some(expected)) => panic!("invalid schema recovery accepted: {expected:?}"),
            }
        }
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn write_request(
        request: u128,
        sequence: u64,
        values: &[i64],
    ) -> zippy_types::ingest::WriteRequest {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(request),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Append(batch(values)),
            })
            .unwrap()
    }

    fn open_request(
        request: u128,
        connection: u128,
        expected_writer_epoch: Option<u64>,
    ) -> OpenWriterRequest {
        OpenWriterRequest {
            request_id: request_id(request),
            table_id: table_id(),
            producer_id: producer_id(),
            connection_id: connection_id(connection),
            expected_writer_epoch,
        }
    }

    fn bootstrap(writer_epoch: u64) -> NativeIngestTableBootstrap {
        NativeIngestTableBootstrap::new(
            table_id(),
            TableDefinition::Append(AppendTableDefinition::default()),
            1,
            schema(),
            writer_epoch,
            OffsetWatermark::Empty,
        )
        .unwrap()
    }

    #[test]
    fn key_value_delete_accepts_only_full_rows_or_ordered_key_tombstones() {
        let definition = TableDefinition::KeyValue(
            KeyValueTableDefinition::new(
                ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
            )
            .unwrap(),
        );
        let table_schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]);
        let key_only = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"])) as ArrayRef],
        )
        .unwrap();
        assert!(super::mutation_schema_matches(
            &definition,
            &table_schema,
            &TableMutation::KeyValue(KeyValueMutation::Delete(key_only.clone()))
        ));

        let full_row = RecordBatch::try_new(
            Arc::new(table_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
            ],
        )
        .unwrap();
        assert!(super::mutation_schema_matches(
            &definition,
            &table_schema,
            &TableMutation::KeyValue(KeyValueMutation::Delete(full_row))
        ));
        assert!(!super::mutation_schema_matches(
            &definition,
            &table_schema,
            &TableMutation::KeyValue(KeyValueMutation::Upsert(key_only.clone()))
        ));

        let wrong_key = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();
        assert!(!super::mutation_schema_matches(
            &definition,
            &table_schema,
            &TableMutation::KeyValue(KeyValueMutation::Delete(wrong_key))
        ));
    }

    fn start_runtime(
        directory: &TempDir,
        catalog: Arc<FakeCatalog>,
        sink: Arc<RecordingSink>,
        writer_epoch: u64,
    ) -> NativeIngestRuntime {
        start_runtime_with_config(
            directory,
            catalog,
            sink,
            writer_epoch,
            NativeIngestConfig::new(),
        )
    }

    fn start_runtime_with_config(
        directory: &TempDir,
        catalog: Arc<FakeCatalog>,
        sink: Arc<RecordingSink>,
        writer_epoch: u64,
        config: NativeIngestConfig,
    ) -> NativeIngestRuntime {
        let catalog: Arc<dyn CatalogWrite> = catalog;
        let sink: Arc<dyn NativeIngestSink> = sink;
        NativeIngestRuntime::start(
            config,
            directory.path().join("wal"),
            catalog,
            vec![bootstrap(writer_epoch)],
            sink,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_online_registration_survives_closed_admission_and_delays_shutdown() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = NativeIngestRuntime::start(
            NativeIngestConfig::new(),
            directory.path().join("wal"),
            catalog,
            Vec::new(),
            sink,
        )
        .unwrap();
        let handle = runtime.handle();
        let registration = handle.begin_table_registration().unwrap();
        assert_eq!(runtime.status().table_count, 0);
        assert_eq!(runtime.status().active_operations, 1);
        assert_eq!(
            runtime.shutdown_until(Instant::now()),
            NativeIngestShutdownOutcome::TimedOut
        );
        assert!(
            matches!(handle.begin_table_registration(), Err(error) if error.code() == ErrorCode::Unavailable)
        );
        registration.register(bootstrap(0)).await.unwrap();
        assert_eq!(runtime.status().table_count, 1);
        assert_eq!(runtime.status().active_operations, 0);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn table_drop_fence_rolls_back_or_permanently_retires_existing_writers() {
        let directory = TempDir::new().unwrap();
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(
            &directory,
            Arc::new(FakeCatalog::default()),
            Arc::clone(&sink),
            0,
        );
        let handle = runtime.handle();
        let mut writer = handle
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        writer.write_memory(write_request(100, 0, &[1])).unwrap();
        let guard = handle.begin_table_transition(table_id()).unwrap();
        assert!(
            matches!(handle.begin_table_transition(table_id()), Err(error) if error.retryable())
        );
        assert!(writer
            .write_memory(write_request(101, 1, &[2]))
            .unwrap_err()
            .retryable());
        assert!(
            matches!(writer.submit_durable(write_request(102, 1, &[2]), DurableWriteTarget::Wal),
            Err(error) if error.retryable())
        );
        assert_eq!(sink.applied().len(), 1);
        drop(guard);
        writer.write_memory(write_request(101, 1, &[2])).unwrap();
        assert_eq!(sink.applied().len(), 2);
        let guard = handle.begin_table_transition(table_id()).unwrap();
        guard.retire(zippy_types::table::TableRetirementReason::Dropped);
        assert!(!handle.contains_table(table_id()).unwrap());
        // The runtime is alive, but this immutable table identity is gone.
        assert_eq!(
            writer
                .write_memory(write_request(103, 2, &[3]))
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(sink.applied().len(), 2);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_table_drop_delays_shutdown_until_catalog_outcome_is_resolved() {
        let directory = TempDir::new().unwrap();
        let mut runtime = start_runtime(
            &directory,
            Arc::new(FakeCatalog::default()),
            Arc::new(RecordingSink::default()),
            0,
        );
        let guard = runtime.handle().begin_table_transition(table_id()).unwrap();
        assert_eq!(
            runtime.shutdown_until(Instant::now()),
            NativeIngestShutdownOutcome::TimedOut
        );
        guard.retire(zippy_types::table::TableRetirementReason::Dropped);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[test]
    fn pending_durable_ack_budget_rejects_before_applying_the_mutation() {
        assert_durable_budget_rejects_before_apply(
            NativeIngestConfig::new().with_max_pending_durable_acks(1),
        );
    }

    #[test]
    fn shared_operation_budget_rejects_before_applying_the_mutation() {
        assert_durable_budget_rejects_before_apply(
            NativeIngestConfig::new()
                .with_max_pending_durable_acks(8)
                .with_max_active_operations(2),
        );
    }

    fn assert_durable_budget_rejects_before_apply(config: NativeIngestConfig) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let release = Arc::new((Mutex::new(false), Condvar::new()));
            let blocker_release = Arc::clone(&release);
            let (started_sender, started_receiver) = std::sync::mpsc::sync_channel(1);
            let blocker = tokio::task::spawn_blocking(move || {
                started_sender.send(()).unwrap();
                let (lock, wake) = blocker_release.as_ref();
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            });
            started_receiver.recv().unwrap();

            let directory = TempDir::new().unwrap();
            let catalog = Arc::new(FakeCatalog::default());
            let sink = Arc::new(RecordingSink::default());
            let mut ingest =
                start_runtime_with_config(&directory, catalog, Arc::clone(&sink), 0, config);
            let mut writer = ingest
                .handle()
                .open_writer(open_request(10, 20, Some(0)))
                .await
                .unwrap();
            let first_ack = writer
                .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
                .unwrap();
            let exhausted =
                match writer.submit_durable(write_request(101, 1, &[2]), DurableWriteTarget::Wal) {
                    Ok(_) => panic!("durable acknowledgement budget must reject"),
                    Err(error) => error,
                };
            assert_eq!(exhausted.code(), ErrorCode::ResourceExhausted);
            assert!(exhausted.retryable());
            assert_eq!(sink.applied().len(), 1);
            assert_eq!(ingest.status().write_batches_total, 1);
            assert_eq!(ingest.status().rejected_operations_total, 1);
            assert!(matches!(ingest.handle().begin_table_transition(table_id()),
                Err(error) if error.code() == ErrorCode::ResourceExhausted && error.retryable()));

            let (lock, wake) = release.as_ref();
            *lock.lock().unwrap() = true;
            wake.notify_one();
            blocker.await.unwrap();
            first_ack.wait().await.unwrap();
            drop(ingest.handle().begin_table_transition(table_id()).unwrap());

            writer
                .submit_durable(write_request(101, 1, &[2]), DurableWriteTarget::Wal)
                .unwrap()
                .wait()
                .await
                .unwrap();
            assert_eq!(sink.applied().len(), 2);
            assert_eq!(
                ingest.shutdown_until(Instant::now() + Duration::from_secs(2)),
                NativeIngestShutdownOutcome::Complete
            );
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atomic_sink_rejection_rolls_back_wal_and_allows_exact_retry() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        sink.fail_once(
            ServerError::new(ErrorCode::Unavailable, "injected atomic sink rejection")
                .with_retryable(true),
        );
        let mut runtime = start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&sink), 0);
        let mut writer = runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        let request = write_request(100, 0, &[1, 2]);
        let rejected = writer.write_memory(request.clone()).unwrap_err();
        assert_eq!(rejected.code(), ErrorCode::Unavailable);
        assert!(rejected.retryable());
        assert!(sink.applied().is_empty());
        assert_eq!(runtime.status().write_batches_total, 0);

        let receipt = writer
            .submit_durable(request, DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(receipt.offsets().unwrap().first().get(), 0);
        assert_eq!(sink.applied().len(), 1);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
        drop(writer);
        drop(runtime);

        let recovered_sink = Arc::new(RecordingSink::default());
        let mut recovered = start_runtime(&directory, catalog, Arc::clone(&recovered_sink), 1);
        assert_eq!(recovered_sink.applied().len(), 1);
        assert_eq!(recovered_sink.applied()[0].request_id, request_id(100));
        assert_eq!(
            recovered.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_record_commit_receipt_promotes_an_already_durable_group_out_of_order() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, catalog, sink, 0);
        let mut writer = runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        writer.write_memory(write_request(100, 0, &[1])).unwrap();
        writer.write_memory(write_request(101, 1, &[2])).unwrap();

        let table = runtime.inner.table(table_id()).unwrap();
        let wal = super::lock_table_state(&table).wal.clone().unwrap();
        let covered = wal.lock().unwrap().commit().unwrap();
        let already_durable = wal.lock().unwrap().commit().unwrap();
        assert_eq!(covered.committed_records, 2);
        assert_eq!(already_durable.committed_records, 0);
        writer.write_memory(write_request(102, 2, &[3])).unwrap();

        let mut state = super::lock_table_state(&table);
        super::promote_wal_commit_locked(&mut state, already_durable);
        assert_eq!(state.last_durable_sequence, SequenceWatermark::Known(1));
        assert!(state.receipts.range(..=1).all(|(_, cached)| cached
            .receipt
            .durability_reached()
            .satisfies(Durability::Wal)));
        assert_eq!(
            state.receipts[&2].receipt.durability_reached(),
            Durability::Memory
        );
        super::promote_wal_commit_locked(&mut state, covered);
        drop(state);

        let next = wal.lock().unwrap().commit().unwrap();
        let mut state = super::lock_table_state(&table);
        super::promote_wal_commit_locked(&mut state, next);
        assert_eq!(state.last_durable_sequence, SequenceWatermark::Known(2));
        assert!(state.receipts.values().all(|cached| cached
            .receipt
            .durability_reached()
            .satisfies(Durability::Wal)));
        drop(state);

        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_open_does_not_cancel_the_server_owned_producer_transition() {
        let directory = TempDir::new().unwrap();
        let gate = Arc::new(FakeCatalogGate::new());
        let catalog = Arc::new(FakeCatalog::with_gate(Arc::clone(&gate)));
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&sink), 0);

        let handle = runtime.handle();
        let entered = gate.entered.notified();
        let caller =
            tokio::spawn(async move { handle.open_writer(open_request(10, 20, Some(0))).await });
        entered.await;
        caller.abort();
        assert!(matches!(caller.await, Err(error) if error.is_cancelled()));
        gate.release.notify_one();

        tokio::time::timeout(Duration::from_secs(2), async {
            while catalog.epoch() != 1 || runtime.status().active_operations != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let mut writer = runtime
            .handle()
            .open_writer(open_request(10, 21, Some(0)))
            .await
            .unwrap();
        assert_eq!(writer.info().producer_epoch, 1);
        writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(sink.applied().len(), 1);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[derive(Debug, Serialize)]
    struct NativeIngestPerformanceReport {
        format_version: u32,
        zippy_version: &'static str,
        warmup_samples: usize,
        measured_samples: usize,
        scenarios: Vec<NativeIngestPerformanceScenario>,
    }

    #[derive(Debug, Serialize)]
    struct NativeIngestPerformanceScenario {
        durability: &'static str,
        rows: usize,
        logical_bytes: usize,
        p50_us: f64,
        p99_us: f64,
        p999_us: f64,
        max_us: f64,
        p50_rows_per_second: f64,
        wal_commits_total: u64,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "manual release benchmark; run with --ignored --test-threads=1"]
    async fn native_ingest_memory_and_wal_performance_baseline() {
        let warmup_samples =
            performance_sample_count("ZIPPY_NATIVE_INGEST_PERF_WARMUP_SAMPLES", 10);
        let measured_samples =
            performance_sample_count("ZIPPY_NATIVE_INGEST_PERF_MEASURED_SAMPLES", 100);
        let mut scenarios = Vec::new();
        for rows in [1, 1_024, 65_536] {
            scenarios
                .push(measure_native_ingest(rows, false, warmup_samples, measured_samples).await);
            scenarios
                .push(measure_native_ingest(rows, true, warmup_samples, measured_samples).await);
        }
        let report = NativeIngestPerformanceReport {
            format_version: 1,
            zippy_version: env!("CARGO_PKG_VERSION"),
            warmup_samples,
            measured_samples,
            scenarios,
        };
        let encoded = serde_json::to_string_pretty(&report).unwrap();
        println!("{encoded}");
        if let Some(path) = std::env::var_os("ZIPPY_NATIVE_INGEST_PERF_REPORT_JSON") {
            let path = PathBuf::from(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, encoded).unwrap();
        }
    }

    async fn measure_native_ingest(
        rows: usize,
        durable: bool,
        warmup_samples: usize,
        measured_samples: usize,
    ) -> NativeIngestPerformanceScenario {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, catalog, sink, 0);
        let mut writer = runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        let mutation = TableMutation::Append(batch(&vec![42_i64; rows]));
        let total_samples = warmup_samples.checked_add(measured_samples).unwrap();
        let mut elapsed_ns = Vec::with_capacity(measured_samples);
        for sequence in 0..total_samples {
            let sequence = u64::try_from(sequence).unwrap();
            let incoming = IncomingWriteRequest {
                request_id: request_id(1_000 + u128::from(sequence)),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: mutation.clone(),
            };
            let started = Instant::now();
            let request = ArrowIpcMutationFingerprintComputer
                .prepare(incoming)
                .unwrap();
            if durable {
                writer
                    .submit_durable(request, DurableWriteTarget::Wal)
                    .unwrap()
                    .wait()
                    .await
                    .unwrap();
            } else {
                writer.write_memory(request).unwrap();
            }
            if usize::try_from(sequence).unwrap() >= warmup_samples {
                elapsed_ns.push(u64::try_from(started.elapsed().as_nanos()).unwrap());
            }
        }
        let status_before_shutdown = runtime.status();
        assert_eq!(
            status_before_shutdown.write_batches_total as usize,
            total_samples
        );
        assert_eq!(
            status_before_shutdown.written_rows_total as usize,
            rows * total_samples
        );
        assert_eq!(
            status_before_shutdown.wal_commits_total,
            if durable { total_samples as u64 } else { 0 }
        );
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(30)),
            NativeIngestShutdownOutcome::Complete
        );
        let wal_commits_total = runtime.status().wal_commits_total;
        assert_eq!(
            wal_commits_total,
            if durable { total_samples as u64 } else { 1 }
        );

        elapsed_ns.sort_unstable();
        let p50_us = percentile_us(&elapsed_ns, 50, 100);
        NativeIngestPerformanceScenario {
            durability: if durable { "wal" } else { "memory" },
            rows,
            logical_bytes: rows * std::mem::size_of::<i64>(),
            p50_us,
            p99_us: percentile_us(&elapsed_ns, 99, 100),
            p999_us: percentile_us(&elapsed_ns, 999, 1_000),
            max_us: elapsed_ns.last().copied().unwrap() as f64 / 1_000.0,
            p50_rows_per_second: rows as f64 * 1_000_000.0 / p50_us,
            wal_commits_total,
        }
    }

    fn percentile_us(values: &[u64], numerator: usize, denominator: usize) -> f64 {
        assert!(!values.is_empty());
        let index = (values.len() - 1).checked_mul(numerator).unwrap() / denominator;
        values[index] as f64 / 1_000.0
    }

    fn performance_sample_count(name: &str, default: usize) -> usize {
        match std::env::var(name) {
            Ok(value) => {
                let parsed = value.parse::<usize>().unwrap();
                assert!((1..=10_000).contains(&parsed));
                parsed
            }
            Err(std::env::VarError::NotPresent) => default,
            Err(error) => panic!("read performance sample count error=[{error}]"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_retry_is_idempotent_and_sequence_conflicts_fail_closed() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&sink), 0);
        let handle = runtime.handle();
        let mut writer = handle
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        assert_eq!(writer.info().producer_epoch, 1);
        assert_eq!(
            writer.info().last_durable_batch_sequence,
            SequenceWatermark::Empty
        );

        let first = write_request(100, 0, &[10, 11]);
        let first_receipt = writer
            .submit_durable(first.clone(), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(first_receipt.durability_reached(), Durability::Wal);
        assert_eq!(first_receipt.offsets().unwrap().first().get(), 0);
        assert_eq!(first_receipt.offsets().unwrap().last().get(), 1);

        let retry = writer
            .submit_durable(first, DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(retry, first_receipt);
        assert_eq!(sink.applied().len(), 1);

        let conflict = writer
            .write_memory(write_request(100, 0, &[99]))
            .unwrap_err();
        assert_eq!(conflict.code(), ErrorCode::Conflict);
        let gap = writer
            .write_memory(write_request(102, 2, &[12]))
            .unwrap_err();
        assert_eq!(gap.code(), ErrorCode::SequenceGap);
        assert_eq!(
            gap.details(),
            Some(&ServerErrorDetails::SequenceGap {
                expected: 1,
                actual: 2,
            })
        );

        let second = write_request(101, 1, &[12]);
        let memory = writer.write_memory(second.clone()).unwrap();
        assert_eq!(memory.durability_reached(), Durability::Memory);
        assert_eq!(memory.offsets().unwrap().first().get(), 2);
        assert_eq!(writer.write_memory(second).unwrap(), memory);
        assert_eq!(sink.applied().len(), 2);
        assert_eq!(runtime.status().duplicate_writes_total, 2);
        assert_eq!(runtime.status().wal_commits_total, 1);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
        assert_eq!(runtime.status().wal_commits_total, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn producer_reacquire_fences_old_connections_without_resetting_offsets() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&sink), 0);
        let handle = runtime.handle();
        let mut first = handle
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        first
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();

        let mut second = handle
            .open_writer(open_request(11, 21, Some(1)))
            .await
            .unwrap();
        assert_eq!(second.info().producer_epoch, 2);
        let stale = first.write_memory(write_request(101, 1, &[2])).unwrap_err();
        assert_eq!(stale.code(), ErrorCode::StaleProducerEpoch);
        let second_receipt = second
            .submit_durable(write_request(200, 0, &[2]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(second_receipt.offsets().unwrap().first().get(), 1);

        let mut reconnected = handle
            .open_writer(open_request(11, 22, Some(1)))
            .await
            .unwrap();
        assert_eq!(reconnected.info().producer_epoch, 2);
        assert_eq!(
            reconnected.info().last_durable_batch_sequence,
            SequenceWatermark::Known(0)
        );
        let disconnected = second
            .write_memory(write_request(201, 1, &[3]))
            .unwrap_err();
        assert_eq!(disconnected.code(), ErrorCode::Conflict);
        let retry = reconnected
            .submit_durable(write_request(200, 0, &[2]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(retry, second_receipt);
        assert_eq!(sink.applied().len(), 2);
        assert_eq!(catalog.epoch(), 2);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_logical_connection_resumes_without_reacquiring_producer_epoch() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let sink = Arc::new(RecordingSink::default());
        let mut runtime = start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&sink), 0);
        let handle = runtime.handle();
        let mut first = handle
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        first
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();

        let mut resumed = handle
            .open_writer(open_request(11, 20, Some(1)))
            .await
            .unwrap();
        assert_eq!(resumed.info().producer_epoch, 1);
        assert_eq!(
            resumed.info().last_durable_batch_sequence,
            SequenceWatermark::Known(0)
        );
        resumed.write_memory(write_request(101, 1, &[2])).unwrap();

        assert_eq!(catalog.epoch(), 1);
        assert_eq!(sink.applied().len(), 2);
        let status = runtime.status();
        assert_eq!(status.writer_acquisitions_total, 1);
        assert_eq!(status.writer_resumes_total, 1);

        let stale = handle
            .open_writer(open_request(12, 20, Some(0)))
            .await
            .err()
            .expect("stale resume must fail");
        assert_eq!(stale.code(), ErrorCode::StaleProducerEpoch);
        assert_eq!(catalog.epoch(), 1);
        assert_eq!(runtime.status().writer_resumes_total, 1);
        assert_eq!(
            runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_replays_only_committed_wal_and_resumes_exact_acquire() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let first_sink = Arc::new(RecordingSink::default());
        let mut first_runtime =
            start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&first_sink), 0);
        let mut first_writer = first_runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        let first_receipt = first_writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            first_runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
        drop(first_writer);
        drop(first_runtime);

        let recovered_sink = Arc::new(RecordingSink::default());
        let mut recovered = start_runtime(
            &directory,
            Arc::clone(&catalog),
            Arc::clone(&recovered_sink),
            1,
        );
        assert_eq!(recovered_sink.applied().len(), 1);
        assert!(recovered_sink.applied()[0].replay);
        let mut writer = recovered
            .handle()
            .open_writer(open_request(10, 21, Some(0)))
            .await
            .unwrap();
        assert_eq!(writer.info().producer_epoch, 1);
        assert_eq!(
            writer.info().last_durable_batch_sequence,
            SequenceWatermark::Known(0)
        );
        let retry = writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(retry, first_receipt);
        assert_eq!(recovered_sink.applied().len(), 1);
        let next = writer
            .submit_durable(write_request(101, 1, &[2]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(next.offsets().unwrap().first().get(), 1);
        assert_eq!(recovered_sink.applied().len(), 2);
        assert_eq!(
            recovered.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_resumes_current_epoch_without_reacquiring_or_resetting_sequence() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let first_sink = Arc::new(RecordingSink::default());
        let mut first_runtime =
            start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&first_sink), 0);
        let mut first_writer = first_runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        let first_receipt = first_writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(
            first_runtime.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
        drop(first_writer);
        drop(first_runtime);

        let recovered_sink = Arc::new(RecordingSink::default());
        let mut recovered = start_runtime(
            &directory,
            Arc::clone(&catalog),
            Arc::clone(&recovered_sink),
            1,
        );
        assert_eq!(recovered_sink.applied().len(), 1);
        assert!(recovered_sink.applied()[0].replay);
        let mut writer = recovered
            .handle()
            .open_writer(open_request(99, 21, Some(1)))
            .await
            .unwrap();
        assert_eq!(writer.info().producer_epoch, 1);
        assert_eq!(recovered.status().writer_resumes_total, 1);
        assert_eq!(recovered.status().writer_acquisitions_total, 0);
        assert_eq!(
            writer.info().last_durable_batch_sequence,
            SequenceWatermark::Known(0)
        );
        let retry = writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(retry, first_receipt);
        assert_eq!(recovered_sink.applied().len(), 1);
        let next = writer
            .submit_durable(write_request(101, 1, &[2]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(next.offsets().unwrap().first().get(), 1);
        assert_eq!(recovered_sink.applied().len(), 2);
        let replacement = recovered
            .handle()
            .open_writer(open_request(200, 22, Some(1)))
            .await
            .unwrap();
        assert_eq!(replacement.info().producer_epoch, 2);
        let stale = writer
            .write_memory(write_request(102, 2, &[3]))
            .unwrap_err();
        assert_eq!(stale.code(), ErrorCode::StaleProducerEpoch);

        assert_eq!(
            recovered.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crash_discards_uncommitted_memory_tail_before_exact_retry() {
        let directory = TempDir::new().unwrap();
        let catalog = Arc::new(FakeCatalog::default());
        let first_sink = Arc::new(RecordingSink::default());
        let first_runtime =
            start_runtime(&directory, Arc::clone(&catalog), Arc::clone(&first_sink), 0);
        let mut writer = first_runtime
            .handle()
            .open_writer(open_request(10, 20, Some(0)))
            .await
            .unwrap();
        writer.write_memory(write_request(100, 0, &[1])).unwrap();
        drop(writer);
        drop(first_runtime);

        let recovered_sink = Arc::new(RecordingSink::default());
        let mut recovered = start_runtime(
            &directory,
            Arc::clone(&catalog),
            Arc::clone(&recovered_sink),
            1,
        );
        assert!(recovered_sink.applied().is_empty());
        assert!(recovered.status().recovered_truncated_tail_bytes > 0);
        let mut writer = recovered
            .handle()
            .open_writer(open_request(10, 21, Some(0)))
            .await
            .unwrap();
        let receipt = writer
            .submit_durable(write_request(100, 0, &[1]), DurableWriteTarget::Wal)
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(receipt.offsets().unwrap().first().get(), 0);
        assert_eq!(recovered_sink.applied().len(), 1);
        assert_eq!(
            recovered.shutdown_until(Instant::now() + Duration::from_secs(2)),
            NativeIngestShutdownOutcome::Complete
        );
    }
}
