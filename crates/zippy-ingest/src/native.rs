//! Producer-fenced native ingest authority with stable offsets and durable WAL acknowledgements.

use std::collections::{BTreeMap, HashMap};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Instant;

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

use crate::{
    recover_table_wal, ArrowIpcMutationFingerprintComputer, IngestWalConfig, IngestWalIdentity,
    IngestWalWriter, RecoveredWalRecord, TableRuntimeRegistry, WalCommitReceipt,
};

/// Default maximum idempotent receipts retained for one active producer epoch.
pub const DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH: usize = 1_000_000;
/// Hard ceiling for configured per-epoch idempotent receipts.
pub const MAX_NATIVE_INGEST_RECEIPTS_PER_EPOCH: usize = 100_000_000;
/// Default maximum WAL acknowledgements waiting for completion per table.
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
}

impl Default for NativeIngestConfig {
    fn default() -> Self {
        Self {
            wal: IngestWalConfig::new(),
            max_receipts_per_epoch: DEFAULT_NATIVE_INGEST_MAX_RECEIPTS_PER_EPOCH,
            max_pending_durable_acks: DEFAULT_NATIVE_INGEST_MAX_PENDING_DURABLE_ACKS,
            max_active_operations: DEFAULT_NATIVE_INGEST_MAX_ACTIVE_OPERATIONS,
            catalog_actor: CatalogActor {
                kind: zippy_types::catalog::CatalogActorKind::Server,
                principal: "zippy-server-ingest".to_owned(),
            },
        }
    }
}

impl NativeIngestConfig {
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

    /// Replaces the maximum exact receipts retained for an active epoch.
    #[must_use]
    pub const fn with_max_receipts_per_epoch(mut self, maximum: usize) -> Self {
        self.max_receipts_per_epoch = maximum;
        self
    }

    /// Replaces the per-table budget for durable acknowledgements waiting on WAL commit.
    #[must_use]
    pub const fn with_max_pending_durable_acks(mut self, maximum: usize) -> Self {
        self.max_pending_durable_acks = maximum;
        self
    }

    /// Replaces the shared budget for foreground calls and background WAL acknowledgements.
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
        if persisted_log_offset == OffsetWatermark::LegacyUnknown {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native ingest cannot recover a legacy-unknown logical boundary",
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
        })
    }

    /// Returns the immutable table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
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

/// Server-owned mutation materializer behind the producer and WAL authority.
pub trait NativeIngestSink: Send + Sync {
    /// Applies one non-duplicate mutation to process-local table state.
    ///
    /// The implementation must make each call atomic at its visible table boundary. A normal error
    /// means no part of the mutation became visible and allows the authority to roll back its
    /// uncommitted WAL append. A panic is treated as an unknown outcome and terminally closes the
    /// table authority.
    fn apply(&self, context: NativeIngestApplyContext, mutation: &TableMutation) -> Result<()>;
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
    /// Rejected opens or writes.
    pub rejected_operations_total: u64,
    /// Bytes removed from incomplete WAL tails during startup.
    pub recovered_truncated_tail_bytes: u64,
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
    rejected_operations_total: u64,
    recovered_truncated_tail_bytes: u64,
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
    tables: BTreeMap<TableId, Arc<TableAuthority>>,
    admission: Mutex<NativeIngestAdmission>,
    drained: Condvar,
    metrics: Mutex<NativeIngestMetrics>,
}

struct TableAuthority {
    table_id: TableId,
    table_definition: TableDefinition,
    schema_version: u64,
    schema: SchemaRef,
    open_gate: tokio::sync::Mutex<()>,
    state: Mutex<TableAuthorityState>,
}

struct TableAuthorityState {
    current_epoch: u64,
    current_producer: Option<ProducerId>,
    active_connection: Option<ConnectionId>,
    transitioning: bool,
    committed_log_offset: OffsetWatermark,
    last_accepted_sequence: SequenceWatermark,
    last_durable_sequence: SequenceWatermark,
    receipts: BTreeMap<u64, CachedWrite>,
    request_sequences: HashMap<RequestId, u64>,
    wal: Option<Arc<Mutex<IngestWalWriter>>>,
    pending_durable_acks: usize,
    terminal_error: Option<ServerError>,
}

#[derive(Clone)]
struct CachedWrite {
    request_id: RequestId,
    fingerprint: MutationFingerprint,
    receipt: WriteReceipt,
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

/// Exclusive lifecycle owner for producer fencing, WAL recovery, and table authorities.
pub struct NativeIngestRuntime {
    inner: Arc<NativeIngestInner>,
    shutdown: Option<NativeIngestShutdownOutcome>,
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
            let recovery = recover_table_wal(
                &wal_root,
                bootstrap.table_id,
                config.wal,
                &ArrowIpcMutationFingerprintComputer,
            )?;
            truncated_tail_bytes = truncated_tail_bytes
                .checked_add(recovery.truncated_tail_bytes)
                .ok_or_else(|| {
                    resource_exhausted("native ingest recovered byte counter overflow")
                })?;
            let authority =
                recover_table_authority(&config, bootstrap, recovery.records, sink.as_ref())?;
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
        Ok(Self {
            inner: Arc::new(NativeIngestInner {
                config,
                wal_root,
                catalog,
                sink,
                tables: authorities,
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
        })
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
                NativeIngestTableBootstrap::new(
                    runtime.table_id(),
                    runtime.table().definition.clone(),
                    runtime.schema_descriptor().schema_version,
                    Arc::clone(runtime.schema()),
                    runtime.writer_epoch(),
                    runtime.committed_log_offset(),
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
        for table in self.inner.tables.values() {
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

    fn table(&self, table_id: TableId) -> Result<Arc<TableAuthority>> {
        self.tables.get(&table_id).cloned().ok_or_else(|| {
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
            table_count: self.tables.len(),
            write_batches_total: metrics.write_batches_total,
            written_rows_total: metrics.written_rows_total,
            duplicate_writes_total: metrics.duplicate_writes_total,
            wal_commits_total: metrics.wal_commits_total,
            rejected_operations_total: metrics.rejected_operations_total,
            recovered_truncated_tail_bytes: metrics.recovered_truncated_tail_bytes,
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

    Ok(Box::new(NativeIngestWriter {
        info,
        inner: Arc::downgrade(&inner),
        table: Arc::downgrade(&table),
    }))
}

fn install_new_epoch(state: &mut TableAuthorityState, epoch: u64, producer_id: ProducerId) {
    state.current_epoch = epoch;
    state.current_producer = Some(producer_id);
    state.active_connection = None;
    state.last_accepted_sequence = SequenceWatermark::Empty;
    state.last_durable_sequence = SequenceWatermark::Empty;
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
        if target == DurableWriteTarget::Persisted {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native ingest persisted durability awaits manifest checkpoint integration",
            )
            .with_request_id(request.request_id())
            .with_table_id(self.info.table_id)
            .with_retryable(false));
        }
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
            .satisfies(Durability::Wal)
        {
            release_pending_durable_ack(&table);
            drop(task_operation);
            return Ok(Box::new(ReadyDurableWriteAck {
                receipt: admission.receipt,
            }));
        }
        let wal = match admission.wal {
            Some(wal) => wal,
            None => {
                release_pending_durable_ack(&table);
                return Err(internal_table_error(
                    table.table_id,
                    "accepted durable write has no active WAL writer",
                ));
            }
        };
        let task_inner = Arc::clone(&inner);
        let task_table = Arc::clone(&table);
        let task = tokio::task::spawn_blocking(move || {
            let _operation = task_operation;
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
        });
        Ok(Box::new(NativeDurableWriteAck { request_id, task }))
    }
}

fn reserve_pending_durable_ack(
    table: &TableAuthority,
    request_id: RequestId,
    maximum: usize,
) -> Result<()> {
    let mut state = lock_table_state(table);
    if state.pending_durable_acks >= maximum {
        return Err(backpressure(format!(
            "native ingest pending durable acknowledgement budget exhausted maximum=[{maximum}]"
        ))
        .with_request_id(request_id)
        .with_table_id(table.table_id));
    }
    state.pending_durable_acks = state
        .pending_durable_acks
        .checked_add(1)
        .ok_or_else(|| resource_exhausted("durable acknowledgement count overflow"))?;
    Ok(())
}

fn release_pending_durable_ack(table: &TableAuthority) {
    let mut state = lock_table_state(table);
    state.pending_durable_acks = state.pending_durable_acks.saturating_sub(1);
}

impl NativeIngestWriter {
    fn authority(&self) -> Result<(Arc<NativeIngestInner>, Arc<TableAuthority>)> {
        let inner = self.inner.upgrade().ok_or_else(authority_stopped)?;
        let table = self.table.upgrade().ok_or_else(authority_stopped)?;
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
        let cached = state
            .receipts
            .get(&request.batch_sequence())
            .ok_or_else(|| {
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
    if let Some(sequence) = state.request_sequences.get(&request.request_id()) {
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
        Ok(Err(error)) => {
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
    state.pending_durable_acks = state.pending_durable_acks.saturating_sub(1);
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
    let sequence = state
        .request_sequences
        .get(&request_id)
        .copied()
        .ok_or_else(|| {
            internal_table_error(
                table.table_id,
                "durable acknowledgement request disappeared from idempotency state",
            )
            .with_request_id(request_id)
        })?;
    let receipt = state
        .receipts
        .get(&sequence)
        .map(|cached| cached.receipt.clone())
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
    if matches!(
        state.last_durable_sequence,
        SequenceWatermark::Known(current) if current > last_batch_sequence
    ) {
        return;
    }
    state.last_durable_sequence = SequenceWatermark::Known(last_batch_sequence);
    for cached in state.receipts.range_mut(..=last_batch_sequence) {
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

fn recover_table_authority(
    config: &NativeIngestConfig,
    bootstrap: NativeIngestTableBootstrap,
    records: Vec<RecoveredWalRecord>,
    sink: &dyn NativeIngestSink,
) -> Result<TableAuthority> {
    let mut visible_watermark = bootstrap.persisted_log_offset;
    let mut current_producer = None;
    let mut current_receipts = BTreeMap::new();
    let mut current_request_sequences = HashMap::new();
    let mut current_last_sequence = SequenceWatermark::Empty;

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
        validate_recovered_mutation(&bootstrap, &record)?;
        if record_should_replay(bootstrap.table_id, visible_watermark, &record.receipt)? {
            sink.apply(
                NativeIngestApplyContext {
                    identity: record.identity,
                    schema_version: record.request.schema_version(),
                    receipt: record.receipt.clone(),
                    replay: true,
                },
                record.request.mutation(),
            )?;
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
        open_gate: tokio::sync::Mutex::new(()),
        state: Mutex::new(TableAuthorityState {
            current_epoch: bootstrap.writer_epoch,
            current_producer,
            active_connection: None,
            transitioning: false,
            committed_log_offset: visible_watermark,
            last_accepted_sequence: current_last_sequence,
            last_durable_sequence: current_last_sequence,
            receipts: current_receipts,
            request_sequences: current_request_sequences,
            wal: None,
            pending_durable_acks: 0,
            terminal_error: None,
        }),
    })
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
        OffsetWatermark::LegacyUnknown => Err(ServerError::new(
            ErrorCode::DataCorruption,
            "native WAL replay encountered a legacy-unknown visible watermark",
        )
        .with_table_id(table_id)
        .with_retryable(false)),
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
        || (!mutation_schema.metadata().is_empty()
            && mutation_schema.metadata() != table_schema.metadata())
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
            OffsetWatermark::LegacyUnknown => {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "native ingest cannot allocate after a legacy-unknown watermark",
                )
                .with_retryable(false));
            }
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

fn promote_receipt(
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
        NativeIngestRuntime, NativeIngestShutdownOutcome, NativeIngestSink,
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
        fn apply(&self, context: NativeIngestApplyContext, mutation: &TableMutation) -> Result<()> {
            if let Some(error) = self.next_error.lock().unwrap().take() {
                return Err(error);
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

            let (lock, wake) = release.as_ref();
            *lock.lock().unwrap() = true;
            wake.notify_one();
            blocker.await.unwrap();
            first_ack.wait().await.unwrap();

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

        let table = runtime.inner.tables.get(&table_id()).unwrap();
        let wal = super::lock_table_state(table).wal.clone().unwrap();
        let covered = wal.lock().unwrap().commit().unwrap();
        let already_durable = wal.lock().unwrap().commit().unwrap();
        assert_eq!(covered.committed_records, 2);
        assert_eq!(already_durable.committed_records, 0);

        let mut state = super::lock_table_state(table);
        super::promote_wal_commit_locked(&mut state, already_durable);
        assert_eq!(state.last_durable_sequence, SequenceWatermark::Known(1));
        assert!(state.receipts.values().all(|cached| cached
            .receipt
            .durability_reached()
            .satisfies(Durability::Wal)));
        super::promote_wal_commit_locked(&mut state, covered);
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
