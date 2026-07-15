//! Query lifecycle adapter over the bounded legacy gateway collect producer.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{oneshot, watch};
use zippy_core::ZippyError;
use zippy_gateway::legacy_query::{LegacyCollectCursor, LegacyCollectFacade, LegacyCollectRequest};
use zippy_types::catalog::CatalogRead;
use zippy_types::query::{
    LogicalPlanEnvelope, OpaqueQueryExecutionToken, OpaqueQueryOwnerCapability, QueryEngine,
    QueryExecutionToken, QueryInput, QueryInstanceId, QueryOwnerCapability, QueryPlanInfo,
    QueryRequest, QueryState, QueryStatus, RecordBatchStream, SendableRecordBatchStream,
};
use zippy_types::{
    BoxFuture, ErrorCode, QueryId, ResourceLimit, Result, ServerError, ServerErrorDetails,
    SessionId,
};

/// Dedicated JSON compatibility format for the legacy Gateway collect plan.
///
/// This value is intentionally distinct from native logical-plan protobuf
/// version 1. It prevents a migration-only JSON payload from being decoded as
/// the future DataFusion plan format.
pub const LEGACY_GATEWAY_PLAN_FORMAT_VERSION: u32 = 0x4c47_0001;

const MAX_LEGACY_PLAN_BYTES: usize = 1024 * 1024;
const DEFAULT_PLAN_TTL: Duration = Duration::from_secs(30);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_TERMINAL_RETENTION: Duration = Duration::from_secs(300);
const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 64;
const DEFAULT_MAX_BLOCKING_QUERY_WORKERS: usize = 72;
const DEFAULT_MAX_REGISTERED_QUERIES: usize = 4096;
const DEFAULT_MAX_QUERY_TOMBSTONES: usize = 65_536;
const ADMISSION_INITIALIZING: u8 = 0;
const ADMISSION_ACTIVE: u8 = 1;
const ADMISSION_SHUTTING_DOWN: u8 = 2;

static NEXT_ENGINE_NONCE: AtomicU64 = AtomicU64::new(1);

/// A strictly versioned compatibility plan understood by the legacy adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyGatewayPlan {
    source: String,
    operations: Vec<Value>,
}

impl LegacyGatewayPlan {
    /// Creates a plan for one legacy stream or published table.
    pub fn new(source: impl Into<String>, operations: Vec<Value>) -> Result<Self> {
        let source = source.into();
        if source.trim().is_empty() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "legacy gateway plan source must not be empty",
            ));
        }
        Ok(Self { source, operations })
    }

    /// Returns the legacy source name.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Borrows the ordered legacy collect operations.
    #[must_use]
    pub fn operations(&self) -> &[Value] {
        &self.operations
    }

    /// Encodes this compatibility plan into a distinct logical-plan envelope.
    pub fn encode(&self) -> Result<LogicalPlanEnvelope> {
        let payload = serde_json::to_vec(self).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("failed to encode legacy gateway plan error=[{error}]"),
            )
        })?;
        if payload.len() > MAX_LEGACY_PLAN_BYTES {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "legacy gateway plan exceeds byte limit bytes=[{}] limit=[{}]",
                    payload.len(),
                    MAX_LEGACY_PLAN_BYTES
                ),
            ));
        }
        LogicalPlanEnvelope::new(LEGACY_GATEWAY_PLAN_FORMAT_VERSION, payload).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("failed to wrap legacy gateway plan error=[{error}]"),
            )
        })
    }

    fn decode(envelope: &LogicalPlanEnvelope) -> Result<Self> {
        if envelope.format_version() != LEGACY_GATEWAY_PLAN_FORMAT_VERSION {
            return Err(unsupported("legacy_gateway.plan_format"));
        }
        if envelope.payload().len() > MAX_LEGACY_PLAN_BYTES {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "legacy gateway plan exceeds byte limit bytes=[{}] limit=[{}]",
                    envelope.payload().len(),
                    MAX_LEGACY_PLAN_BYTES
                ),
            ));
        }
        let plan: Self = serde_json::from_slice(envelope.payload()).map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("invalid legacy gateway plan payload error=[{error}]"),
            )
        })?;
        Self::new(plan.source, plan.operations)
    }
}

/// Lifecycle and retention configuration for the legacy query adapter.
#[derive(Debug, Clone, Copy)]
pub struct LegacyQueryEngineConfig {
    /// Maximum lifetime of an unexecuted plan.
    pub plan_ttl: Duration,
    /// Interval between plan-expiry and status-retention sweeps.
    pub sweep_interval: Duration,
    /// Duration terminal status remains queryable.
    pub terminal_retention: Duration,
    /// Maximum number of query driver threads that may run concurrently.
    pub max_concurrent_queries: usize,
    /// Hard limit for blocking opener and driver workers, including cancelled workers whose
    /// socket or file I/O has not returned yet.
    ///
    /// Exhaustion fails execution immediately with `ResourceExhausted`; cancellation does not
    /// claim to interrupt an already-blocked worker. The default leaves eight bounded worker
    /// slots beyond logical query admission so replacement work can proceed after cancellation.
    pub max_blocking_query_workers: usize,
    /// Maximum number of planned, running, and retained terminal queries.
    pub max_registered_queries: usize,
    /// Maximum number of early-eviction tombstones retained for query identity safety.
    ///
    /// Once exhausted, capacity eviction fails closed with `ResourceExhausted` until tombstones
    /// reach their original terminal-retention deadline.
    pub max_query_tombstones: usize,
}

impl Default for LegacyQueryEngineConfig {
    fn default() -> Self {
        Self {
            plan_ttl: DEFAULT_PLAN_TTL,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            terminal_retention: DEFAULT_TERMINAL_RETENTION,
            max_concurrent_queries: DEFAULT_MAX_CONCURRENT_QUERIES,
            max_blocking_query_workers: DEFAULT_MAX_BLOCKING_QUERY_WORKERS,
            max_registered_queries: DEFAULT_MAX_REGISTERED_QUERIES,
            max_query_tombstones: DEFAULT_MAX_QUERY_TOMBSTONES,
        }
    }
}

/// Adapts bounded legacy Gateway collect execution to the stable query contract.
#[derive(Clone)]
pub struct LegacyQueryEngineAdapter {
    inner: Arc<EngineInner>,
}

impl LegacyQueryEngineAdapter {
    /// Creates an adapter using conservative lifecycle defaults.
    pub fn new(facade: LegacyCollectFacade, catalog: Arc<dyn CatalogRead>) -> Result<Self> {
        Self::build(
            facade,
            catalog,
            LegacyQueryEngineConfig::default(),
            ADMISSION_ACTIVE,
        )
    }

    /// Creates an adapter with explicit lifecycle timing.
    pub fn with_config(
        facade: LegacyCollectFacade,
        catalog: Arc<dyn CatalogRead>,
        config: LegacyQueryEngineConfig,
    ) -> Result<Self> {
        Self::build(facade, catalog, config, ADMISSION_ACTIVE)
    }

    pub(crate) fn new_inactive(
        facade: LegacyCollectFacade,
        catalog: Arc<dyn CatalogRead>,
    ) -> Result<Self> {
        Self::build(
            facade,
            catalog,
            LegacyQueryEngineConfig::default(),
            ADMISSION_INITIALIZING,
        )
    }

    fn build(
        facade: LegacyCollectFacade,
        catalog: Arc<dyn CatalogRead>,
        config: LegacyQueryEngineConfig,
        admission_state: u8,
    ) -> Result<Self> {
        if config.plan_ttl.is_zero()
            || config.sweep_interval.is_zero()
            || config.terminal_retention.is_zero()
            || config.max_concurrent_queries == 0
            || config.max_blocking_query_workers == 0
            || config.max_registered_queries == 0
            || config.max_query_tombstones == 0
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "legacy query lifecycle durations and capacities must be positive",
            ));
        }
        if config.max_blocking_query_workers < config.max_concurrent_queries {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!(
                        "legacy blocking worker limit must cover logical concurrency ",
                        "workers=[{}] queries=[{}]"
                    ),
                    config.max_blocking_query_workers, config.max_concurrent_queries
                ),
            ));
        }
        let inner = Arc::new(EngineInner {
            nonce: next_engine_nonce(),
            facade,
            catalog,
            queries: Mutex::new(QueryRegistry::default()),
            plan_ttl_ns: duration_ns(config.plan_ttl),
            terminal_retention_ns: duration_ns(config.terminal_retention),
            next_query_generation: AtomicU64::new(1),
            next_owner_nonce: AtomicU64::new(1),
            active_queries: AtomicUsize::new(0),
            active_blocking_workers: AtomicUsize::new(0),
            max_concurrent_queries: config.max_concurrent_queries,
            max_blocking_query_workers: config.max_blocking_query_workers,
            max_registered_queries: config.max_registered_queries,
            max_query_tombstones: config.max_query_tombstones,
            admission_state: AtomicU8::new(admission_state),
        });
        spawn_sweeper(&inner, config.sweep_interval)?;
        Ok(Self { inner })
    }

    /// Issues an opaque owner capability for one client session.
    #[must_use]
    pub fn owner(&self, session_id: SessionId) -> OpaqueQueryOwnerCapability {
        Box::new(LegacyQueryOwner {
            session_id,
            engine_nonce: self.inner.nonce,
            owner_nonce: self.inner.next_owner_nonce(),
        })
    }

    /// Allows new plans after the surrounding runtime publishes readiness.
    pub fn activate(&self) -> Result<()> {
        match self.inner.admission_state.compare_exchange(
            ADMISSION_INITIALIZING,
            ADMISSION_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(ADMISSION_ACTIVE) => Ok(()),
            Err(_) => Err(ServerError::new(
                ErrorCode::Conflict,
                "legacy query engine cannot reactivate after shutdown begins",
            )),
        }
    }

    /// Rejects new plans and cancels all registered nonterminal queries.
    pub fn begin_shutdown(&self) {
        self.inner
            .admission_state
            .store(ADMISSION_SHUTTING_DOWN, Ordering::Release);
        self.inner
            .cancel_all("legacy query engine is shutting down");
    }

    /// Returns whether new query plans are currently admitted.
    #[must_use]
    pub fn is_accepting_work(&self) -> bool {
        self.inner.admission_state.load(Ordering::Acquire) == ADMISSION_ACTIVE
    }

    fn validate_owner<'a>(
        &self,
        owner: &'a dyn QueryOwnerCapability,
    ) -> Result<&'a LegacyQueryOwner> {
        let owner = owner
            .as_any()
            .downcast_ref::<LegacyQueryOwner>()
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::PermissionDenied,
                    "query owner capability was not issued by the legacy adapter",
                )
            })?;
        if owner.engine_nonce != self.inner.nonce {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "query owner capability belongs to another engine instance",
            ));
        }
        Ok(owner)
    }
}

impl QueryEngine for LegacyQueryEngineAdapter {
    fn plan<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
    ) -> BoxFuture<'a, Result<OpaqueQueryExecutionToken>> {
        Box::pin(async move {
            let submitted_at_ns = now_ns();
            let owner = self.validate_owner(owner)?;
            if self.inner.admission_state.load(Ordering::Acquire) != ADMISSION_ACTIVE {
                return Err(query_unavailable(request.query_id));
            }
            if owner.session_id != request.session_id {
                return Err(ServerError::new(
                    ErrorCode::PermissionDenied,
                    "query request session does not match owner capability",
                )
                .with_query_id(request.query_id));
            }
            validate_request_capabilities(&request)
                .map_err(|error| error.with_query_id(request.query_id))?;
            let plan = match &request.input {
                QueryInput::Sql(_) => {
                    return Err(unsupported("legacy_gateway.sql").with_query_id(request.query_id));
                }
                QueryInput::LogicalPlan(envelope) => LegacyGatewayPlan::decode(envelope)
                    .map_err(|error| error.with_query_id(request.query_id))?,
            };
            let batch_rows = request
                .result_batch_rows
                .map_or(65_536, |rows| rows.get() as usize);
            let collect_request = LegacyCollectRequest::new(
                plan.source().to_string(),
                plan.operations().to_vec(),
                batch_rows,
            )
            .map_err(|error| map_gateway_error(error, request.query_id))?;
            self.inner
                .facade
                .validate(&collect_request)
                .map_err(|error| map_gateway_error(error, request.query_id))?;

            if request
                .deadline_ns
                .is_some_and(|deadline| deadline <= submitted_at_ns)
            {
                return Err(ServerError::new(
                    ErrorCode::DeadlineExceeded,
                    "query deadline elapsed before planning",
                )
                .with_query_id(request.query_id));
            }
            let catalog_revision = self.inner.catalog.revision().await?;
            let planned_at_ns = now_ns();
            if request
                .deadline_ns
                .is_some_and(|deadline| deadline <= planned_at_ns)
            {
                return Err(ServerError::new(
                    ErrorCode::DeadlineExceeded,
                    "query deadline elapsed during planning",
                )
                .with_query_id(request.query_id));
            }
            let expires_at_ns = request.deadline_ns.map_or_else(
                || planned_at_ns.saturating_add(self.inner.plan_ttl_ns),
                |deadline| deadline.min(planned_at_ns.saturating_add(self.inner.plan_ttl_ns)),
            );
            let info = QueryPlanInfo {
                query_id: request.query_id,
                catalog_revision,
                snapshots: Vec::new(),
                planned_at_ns,
                expires_at_ns,
            };
            let generation = self.inner.next_query_generation();
            let instance_id = query_instance_id(request.query_id, generation);
            let control = Arc::new(QueryControl::new(
                &request,
                &info,
                submitted_at_ns,
                Arc::downgrade(&self.inner),
                generation,
                owner.owner_nonce,
            ));
            {
                let mut queries = self.inner.queries.lock().unwrap();
                if self.inner.admission_state.load(Ordering::Acquire) != ADMISSION_ACTIVE {
                    return Err(query_unavailable(request.query_id));
                }
                if queries.is_query_id_reserved(request.query_id, planned_at_ns) {
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        "query identifier is already registered or retained after eviction",
                    )
                    .with_query_id(request.query_id));
                }
                self.inner.prepare_registry_for_registration(
                    &mut queries,
                    planned_at_ns,
                    request.query_id,
                )?;
                queries.insert(request.query_id, Arc::clone(&control));
            }
            Ok(Box::new(LegacyExecutionToken {
                info,
                owner_session_id: request.session_id,
                owner_nonce: owner.owner_nonce,
                engine_nonce: self.inner.nonce,
                instance_id,
                plan,
                batch_rows,
                control,
                armed: true,
            }) as OpaqueQueryExecutionToken)
        })
    }

    fn execute(
        &self,
        token: OpaqueQueryExecutionToken,
    ) -> BoxFuture<'_, Result<SendableRecordBatchStream>> {
        let transferred = token.into_any();
        let transferred = match transferred.downcast::<LegacyTransferredToken>() {
            Ok(token) => *token,
            Err(_) => {
                return Box::pin(async {
                    Err(ServerError::new(
                        ErrorCode::InvalidArgument,
                        "query execution token was not issued by the legacy adapter",
                    ))
                });
            }
        };
        let inner = Arc::clone(&self.inner);
        let mut execution_guard =
            CancelOnDrop::new(Arc::clone(&transferred.control), transferred.info.query_id);
        Box::pin(async move {
            if transferred.engine_nonce != inner.nonce {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "query execution token belongs to another engine instance",
                )
                .with_query_id(transferred.info.query_id));
            }
            let control = inner.control_instance(transferred.instance_id)?;
            if !Arc::ptr_eq(&control, &transferred.control) {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "query execution token refers to a retired query instance",
                )
                .with_query_id(transferred.info.query_id));
            }
            control.authorize(
                transferred.owner_session_id,
                transferred.owner_nonce,
                transferred.info.query_id,
            )?;
            let admission_permit = match inner.acquire_query_permit(transferred.info.query_id) {
                Ok(permit) => permit,
                Err(error) => {
                    control.fail(error.clone());
                    return Err(control.terminal_error().unwrap_or(error));
                }
            };
            control.start_with_permit(now_ns(), admission_permit)?;
            let request = LegacyCollectRequest::new(
                transferred.plan.source().to_string(),
                transferred.plan.operations().to_vec(),
                transferred.batch_rows,
            )
            .map_err(|error| map_gateway_error(error, transferred.info.query_id));
            let request = match request {
                Ok(request) => request,
                Err(error) => {
                    control.fail(error.clone());
                    return Err(control.terminal_error().unwrap_or(error));
                }
            };
            control.expire_deadline_if_due(
                transferred.info.query_id,
                now_ns(),
                "query deadline elapsed before opening legacy producer",
            )?;
            let worker_permit =
                match inner.acquire_blocking_worker_permit(transferred.info.query_id) {
                    Ok(permit) => permit,
                    Err(error) => {
                        control.fail(error.clone());
                        return Err(control.terminal_error().unwrap_or(error));
                    }
                };

            let (sender, receiver) = oneshot::channel();
            let facade = inner.facade.clone();
            let cancelled = Arc::clone(&control.cancelled);
            let opener_timeout = control.remaining_deadline(now_ns());
            let mut terminal_receiver = control.subscribe_terminal();
            thread::Builder::new()
                .name("zippy-legacy-query-open".to_string())
                .spawn(move || {
                    let result = match opener_timeout {
                        Some(timeout) => {
                            facade.open_cancellable_with_timeout(request, cancelled, timeout)
                        }
                        None => facade.open_cancellable(request, cancelled),
                    };
                    let _ = sender.send((result, worker_permit));
                })
                .map_err(|error| {
                    let server_error = ServerError::new(
                        ErrorCode::Unavailable,
                        format!("failed to spawn legacy query opener error=[{error}]"),
                    )
                    .with_query_id(transferred.info.query_id);
                    control.fail(server_error.clone());
                    control.terminal_error().unwrap_or(server_error)
                })?;
            let deadline_ns = control.deadline_ns;
            let opener_result = tokio::select! {
                biased;
                error = wait_for_terminal(&mut terminal_receiver) => {
                    return Err(error);
                }
                () = wait_for_deadline(deadline_ns) => {
                    control.timeout(
                        transferred.info.query_id,
                        "query deadline elapsed while opening legacy producer",
                    );
                    return Err(control.terminal_error().unwrap_or_else(|| {
                        deadline_error(
                            transferred.info.query_id,
                            "query deadline elapsed while opening legacy producer",
                        )
                    }));
                }
                result = receiver => result,
            };
            let (cursor, worker_permit) = opener_result.map_err(|_| {
                let error = ServerError::new(
                    ErrorCode::Unavailable,
                    "legacy query opener stopped before returning a cursor",
                )
                .with_query_id(transferred.info.query_id);
                control.fail(error.clone());
                control.terminal_error().unwrap_or(error)
            })?;
            control.expire_deadline_if_due(
                transferred.info.query_id,
                now_ns(),
                "query deadline elapsed while opening legacy producer",
            )?;
            let cursor = cursor.map_err(|error| {
                let error =
                    map_gateway_open_error(error, transferred.info.query_id, &control, now_ns());
                control.finish_gateway_error(error.clone());
                control.terminal_error().unwrap_or(error)
            })?;
            if control.cancelled.load(Ordering::Acquire) {
                control.cancel(
                    transferred.info.query_id,
                    "query cancelled while opening result cursor",
                );
                return Err(control
                    .terminal_error()
                    .unwrap_or_else(|| cancelled_error(transferred.info.query_id)));
            }
            let schema = cursor.schema();
            let shared = Arc::new(StreamDelivery::default());
            control.attach_delivery(&shared)?;
            control.expire_deadline_if_due(
                transferred.info.query_id,
                now_ns(),
                "query deadline elapsed before legacy producer execution",
            )?;
            spawn_driver(
                cursor,
                Arc::clone(&shared),
                Arc::clone(&control),
                transferred.info.query_id,
                worker_permit,
            )?;
            execution_guard.disarm();
            Ok(Box::pin(LegacyRecordBatchStream {
                query_id: transferred.info.query_id,
                schema,
                shared,
                control,
                terminal_observed: false,
            }) as SendableRecordBatchStream)
        })
    }

    fn cancel<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let owner = self.validate_owner(owner)?;
            let query_id = instance_id.query_id();
            let control = self.inner.control_instance(instance_id)?;
            control.authorize(owner.session_id, owner.owner_nonce, query_id)?;
            control.cancel(query_id, "query cancelled by owner");
            Ok(())
        })
    }

    fn status<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<QueryStatus>> {
        Box::pin(async move {
            let owner = self.validate_owner(owner)?;
            let query_id = instance_id.query_id();
            let control = self.inner.control_instance(instance_id)?;
            control.authorize(owner.session_id, owner.owner_nonce, query_id)?;
            let status = control.status.lock().unwrap().clone();
            Ok(status)
        })
    }
}

type TerminalOrderKey = Reverse<(u64, QueryId, u64)>;
type TombstoneOrderKey = Reverse<(u64, QueryId, u64)>;

#[derive(Clone, Copy)]
struct QueryTombstone {
    expires_at_ns: u64,
    generation: u64,
}

enum CapacityEviction {
    Evicted,
    NoTerminal,
    TombstoneCapacityExhausted,
}

#[derive(Default)]
struct QueryRegistry {
    by_id: HashMap<QueryId, Arc<QueryControl>>,
    terminal_order: BinaryHeap<TerminalOrderKey>,
    tombstones: HashMap<QueryId, QueryTombstone>,
    tombstone_order: BinaryHeap<TombstoneOrderKey>,
}

impl QueryRegistry {
    fn contains_key(&self, query_id: &QueryId) -> bool {
        self.by_id.contains_key(query_id)
    }

    fn get(&self, query_id: &QueryId) -> Option<&Arc<QueryControl>> {
        self.by_id.get(query_id)
    }

    fn insert(&mut self, query_id: QueryId, control: Arc<QueryControl>) {
        debug_assert!(!self.tombstones.contains_key(&query_id));
        let previous = self.by_id.insert(query_id, control);
        debug_assert!(previous.is_none());
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    fn controls(&self) -> Vec<(QueryId, Arc<QueryControl>)> {
        self.by_id
            .iter()
            .map(|(query_id, control)| (*query_id, Arc::clone(control)))
            .collect()
    }

    fn is_query_id_reserved(&mut self, query_id: QueryId, now: u64) -> bool {
        self.remove_expired_tombstones(now);
        self.contains_key(&query_id) || self.tombstones.contains_key(&query_id)
    }

    fn record_terminal(&mut self, query_id: QueryId, finished_at_ns: u64, generation: u64) {
        if self
            .by_id
            .get(&query_id)
            .is_some_and(|control| control.generation == generation)
        {
            self.terminal_order
                .push(Reverse((finished_at_ns, query_id, generation)));
        }
    }

    fn remove_expired_terminal(&mut self, now: u64, retention_ns: u64) {
        loop {
            self.discard_stale_terminal_entries();
            let Some(Reverse((finished_at_ns, query_id, generation))) =
                self.terminal_order.peek().copied()
            else {
                return;
            };
            if now.saturating_sub(finished_at_ns) < retention_ns {
                return;
            }
            self.terminal_order.pop();
            if self.is_current_generation(query_id, generation) {
                self.by_id.remove(&query_id);
            }
        }
    }

    fn evict_oldest_terminal(
        &mut self,
        now: u64,
        retention_ns: u64,
        max_tombstones: usize,
    ) -> CapacityEviction {
        self.discard_stale_terminal_entries();
        let Some(Reverse((finished_at_ns, query_id, generation))) =
            self.terminal_order.peek().copied()
        else {
            return CapacityEviction::NoTerminal;
        };
        self.remove_expired_tombstones(now);
        if self.tombstones.len() >= max_tombstones {
            return CapacityEviction::TombstoneCapacityExhausted;
        }
        self.terminal_order.pop();
        if self.is_current_generation(query_id, generation) {
            self.by_id.remove(&query_id);
            let expires_at_ns = finished_at_ns.saturating_add(retention_ns);
            if expires_at_ns > now {
                let previous = self.tombstones.insert(
                    query_id,
                    QueryTombstone {
                        expires_at_ns,
                        generation,
                    },
                );
                debug_assert!(previous.is_none());
                self.tombstone_order
                    .push(Reverse((expires_at_ns, query_id, generation)));
            }
            return CapacityEviction::Evicted;
        }
        CapacityEviction::NoTerminal
    }

    fn discard_stale_terminal_entries(&mut self) {
        while let Some(Reverse((_, query_id, generation))) = self.terminal_order.peek().copied() {
            if self.is_current_generation(query_id, generation) {
                return;
            }
            self.terminal_order.pop();
        }
    }

    fn is_current_generation(&self, query_id: QueryId, generation: u64) -> bool {
        self.by_id
            .get(&query_id)
            .is_some_and(|control| control.generation == generation)
    }

    fn remove_expired_tombstones(&mut self, now: u64) {
        loop {
            self.discard_stale_tombstone_entries();
            let Some(Reverse((expires_at_ns, query_id, generation))) =
                self.tombstone_order.peek().copied()
            else {
                return;
            };
            if expires_at_ns > now {
                return;
            }
            self.tombstone_order.pop();
            if self.is_current_tombstone(query_id, expires_at_ns, generation) {
                self.tombstones.remove(&query_id);
            }
        }
    }

    fn discard_stale_tombstone_entries(&mut self) {
        while let Some(Reverse((expires_at_ns, query_id, generation))) =
            self.tombstone_order.peek().copied()
        {
            if self.is_current_tombstone(query_id, expires_at_ns, generation) {
                return;
            }
            self.tombstone_order.pop();
        }
    }

    fn is_current_tombstone(&self, query_id: QueryId, expires_at_ns: u64, generation: u64) -> bool {
        self.tombstones.get(&query_id).is_some_and(|tombstone| {
            tombstone.generation == generation && tombstone.expires_at_ns == expires_at_ns
        })
    }
}

struct EngineInner {
    nonce: u128,
    facade: LegacyCollectFacade,
    catalog: Arc<dyn CatalogRead>,
    queries: Mutex<QueryRegistry>,
    plan_ttl_ns: u64,
    terminal_retention_ns: u64,
    next_query_generation: AtomicU64,
    next_owner_nonce: AtomicU64,
    active_queries: AtomicUsize,
    active_blocking_workers: AtomicUsize,
    max_concurrent_queries: usize,
    max_blocking_query_workers: usize,
    max_registered_queries: usize,
    max_query_tombstones: usize,
    admission_state: AtomicU8,
}

impl EngineInner {
    fn next_query_generation(&self) -> u64 {
        self.next_query_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .expect("legacy query generation space exhausted")
    }

    fn next_owner_nonce(&self) -> u64 {
        self.next_owner_nonce
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |nonce| {
                nonce.checked_add(1)
            })
            .expect("legacy query owner nonce space exhausted")
    }

    fn control(&self, query_id: QueryId) -> Result<Arc<QueryControl>> {
        self.queries
            .lock()
            .unwrap()
            .get(&query_id)
            .cloned()
            .ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "query is not registered")
                    .with_query_id(query_id)
            })
    }

    fn control_instance(&self, instance_id: QueryInstanceId) -> Result<Arc<QueryControl>> {
        let query_id = instance_id.query_id();
        self.control(query_id).and_then(|control| {
            if control.generation == instance_id.generation().get() {
                Ok(control)
            } else {
                Err(ServerError::new(
                    ErrorCode::NotFound,
                    "query instance is no longer registered",
                )
                .with_query_id(query_id)
                .with_retryable(false))
            }
        })
    }

    fn acquire_query_permit(self: &Arc<Self>, query_id: QueryId) -> Result<QueryPermit> {
        let mut active = self.active_queries.load(Ordering::Acquire);
        loop {
            if active >= self.max_concurrent_queries {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "legacy query concurrency is exhausted active=[{}] limit=[{}]",
                        active, self.max_concurrent_queries
                    ),
                )
                .with_query_id(query_id));
            }
            match self.active_queries.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(QueryPermit {
                        inner: Arc::downgrade(self),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }

    fn prepare_registry_for_registration(
        &self,
        queries: &mut QueryRegistry,
        now: u64,
        query_id: QueryId,
    ) -> Result<()> {
        if queries.len() < self.max_registered_queries {
            return Ok(());
        }
        queries.remove_expired_terminal(now, self.terminal_retention_ns);
        if queries.len() < self.max_registered_queries {
            return Ok(());
        }
        match queries.evict_oldest_terminal(
            now,
            self.terminal_retention_ns,
            self.max_query_tombstones,
        ) {
            CapacityEviction::Evicted => {}
            CapacityEviction::NoTerminal => {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "legacy query registry is full with active work limit=[{}]",
                        self.max_registered_queries
                    ),
                )
                .with_query_id(query_id));
            }
            CapacityEviction::TombstoneCapacityExhausted => {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        concat!(
                            "legacy query tombstone capacity is exhausted limit=[{}]; ",
                            "identity reuse remains blocked until retention expires"
                        ),
                        self.max_query_tombstones
                    ),
                )
                .with_query_id(query_id));
            }
        }
        Ok(())
    }

    fn acquire_blocking_worker_permit(
        self: &Arc<Self>,
        query_id: QueryId,
    ) -> Result<BlockingWorkerPermit> {
        let mut active = self.active_blocking_workers.load(Ordering::Acquire);
        loop {
            if active >= self.max_blocking_query_workers {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        concat!(
                            "legacy blocking query workers are exhausted active=[{}] ",
                            "limit=[{}]; cancelled workers retain physical slots until I/O returns"
                        ),
                        active, self.max_blocking_query_workers
                    ),
                )
                .with_query_id(query_id));
            }
            match self.active_blocking_workers.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BlockingWorkerPermit {
                        inner: Arc::downgrade(self),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }

    fn cancel_all(&self, reason: &'static str) {
        let controls = self.queries.lock().unwrap().controls();
        for (query_id, control) in controls {
            control.cancel(query_id, reason);
        }
    }

    fn sweep(&self, now: u64) {
        let controls = self.queries.lock().unwrap().controls();
        for (query_id, control) in controls {
            let state = control.status.lock().unwrap().state;
            if state.is_terminal() {
                continue;
            }
            if state == QueryState::Running {
                if control.deadline_ns.is_some_and(|deadline| deadline <= now) {
                    control.timeout(query_id, "running legacy query exceeded its deadline");
                }
            } else if control.plan_expires_at_ns <= now {
                control.timeout(query_id, "unexecuted legacy query plan expired");
            }
        }
        self.queries
            .lock()
            .unwrap()
            .remove_expired_terminal(now, self.terminal_retention_ns);
        self.queries.lock().unwrap().remove_expired_tombstones(now);
    }
}

impl Drop for EngineInner {
    fn drop(&mut self) {
        self.admission_state
            .store(ADMISSION_SHUTTING_DOWN, Ordering::Release);
        if let Ok(registry) = self.queries.get_mut() {
            for (query_id, control) in registry.controls() {
                control.cancel(query_id, "legacy query engine stopped");
            }
        }
    }
}

struct QueryPermit {
    inner: Weak<EngineInner>,
}

impl Drop for QueryPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.active_queries.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

struct BlockingWorkerPermit {
    inner: Weak<EngineInner>,
}

impl Drop for BlockingWorkerPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.active_blocking_workers.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

struct LegacyQueryOwner {
    session_id: SessionId,
    engine_nonce: u128,
    owner_nonce: u64,
}

impl QueryOwnerCapability for LegacyQueryOwner {
    fn session_id(&self) -> SessionId {
        self.session_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct LegacyExecutionToken {
    info: QueryPlanInfo,
    owner_session_id: SessionId,
    owner_nonce: u64,
    engine_nonce: u128,
    instance_id: QueryInstanceId,
    plan: LegacyGatewayPlan,
    batch_rows: usize,
    control: Arc<QueryControl>,
    armed: bool,
}

impl QueryExecutionToken for LegacyExecutionToken {
    fn info(&self) -> &QueryPlanInfo {
        &self.info
    }

    fn owner_session_id(&self) -> SessionId {
        self.owner_session_id
    }

    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    fn into_any(mut self: Box<Self>) -> Box<dyn Any + Send> {
        let transferred = LegacyTransferredToken {
            info: self.info.clone(),
            owner_session_id: self.owner_session_id,
            owner_nonce: self.owner_nonce,
            engine_nonce: self.engine_nonce,
            instance_id: self.instance_id,
            plan: self.plan.clone(),
            batch_rows: self.batch_rows,
            control: Arc::clone(&self.control),
        };
        self.armed = false;
        Box::new(transferred)
    }
}

impl Drop for LegacyExecutionToken {
    fn drop(&mut self) {
        if self.armed {
            self.control
                .cancel(self.info.query_id, "unexecuted query token dropped");
        }
    }
}

struct LegacyTransferredToken {
    info: QueryPlanInfo,
    owner_session_id: SessionId,
    owner_nonce: u64,
    engine_nonce: u128,
    instance_id: QueryInstanceId,
    plan: LegacyGatewayPlan,
    batch_rows: usize,
    control: Arc<QueryControl>,
}

struct QueryControl {
    engine: Weak<EngineInner>,
    generation: u64,
    owner_nonce: u64,
    status: Mutex<QueryStatus>,
    plan_expires_at_ns: u64,
    deadline_ns: Option<u64>,
    cancelled: Arc<AtomicBool>,
    admission_permit: Mutex<Option<QueryPermit>>,
    terminal_sender: watch::Sender<Option<ServerError>>,
    delivery: Mutex<Option<Weak<StreamDelivery>>>,
}

impl QueryControl {
    fn new(
        request: &QueryRequest,
        info: &QueryPlanInfo,
        submitted_at_ns: u64,
        engine: Weak<EngineInner>,
        generation: u64,
        owner_nonce: u64,
    ) -> Self {
        let (terminal_sender, _terminal_receiver) = watch::channel(None);
        Self {
            engine,
            generation,
            owner_nonce,
            status: Mutex::new(QueryStatus {
                query_id: request.query_id,
                generation: NonZeroU64::new(generation)
                    .expect("legacy query generation is nonzero"),
                session_id: request.session_id,
                state: QueryState::Planned,
                submitted_at_ns,
                started_at_ns: None,
                finished_at_ns: None,
                catalog_revision: Some(info.catalog_revision),
                snapshots: Vec::new(),
                metrics: BTreeMap::from([("legacy_adapter".to_string(), 1)]),
                error: None,
            }),
            plan_expires_at_ns: info.expires_at_ns,
            deadline_ns: request.deadline_ns,
            cancelled: Arc::new(AtomicBool::new(false)),
            admission_permit: Mutex::new(None),
            terminal_sender,
            delivery: Mutex::new(None),
        }
    }

    fn authorize(&self, session_id: SessionId, owner_nonce: u64, query_id: QueryId) -> Result<()> {
        if self.status.lock().unwrap().session_id != session_id || self.owner_nonce != owner_nonce {
            return Err(ServerError::new(
                ErrorCode::PermissionDenied,
                "query owner capability does not own the query instance",
            )
            .with_query_id(query_id));
        }
        Ok(())
    }

    fn start_with_permit(&self, now: u64, permit: QueryPermit) -> Result<()> {
        let query_id = {
            let mut status = self.status.lock().unwrap();
            if status.state.is_terminal() {
                return Err(status.error.clone().unwrap_or_else(|| {
                    ServerError::new(
                        ErrorCode::Conflict,
                        format!("query is not executable state=[{:?}]", status.state),
                    )
                    .with_query_id(status.query_id)
                }));
            }
            if status.state != QueryState::Planned {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    format!("query is not executable state=[{:?}]", status.state),
                )
                .with_query_id(status.query_id));
            }
            if self.plan_expires_at_ns > now {
                let mut admission_permit = self.admission_permit.lock().unwrap();
                debug_assert!(admission_permit.is_none());
                *admission_permit = Some(permit);
                status.state = QueryState::Running;
                status.started_at_ns = Some(now);
                return Ok(());
            }
            status.query_id
        };

        self.timeout(query_id, "legacy query execution token expired");
        Err(self.terminal_error().unwrap_or_else(|| {
            ServerError::new(
                ErrorCode::DeadlineExceeded,
                "legacy query execution token expired",
            )
            .with_query_id(query_id)
        }))
    }

    fn expire_deadline_if_due(
        &self,
        query_id: QueryId,
        now: u64,
        reason: &'static str,
    ) -> Result<()> {
        if self.deadline_ns.is_none_or(|deadline| deadline > now) {
            return Ok(());
        }
        self.timeout(query_id, reason);
        Err(self
            .terminal_error()
            .unwrap_or_else(|| deadline_error(query_id, reason)))
    }

    fn subscribe_terminal(&self) -> watch::Receiver<Option<ServerError>> {
        self.terminal_sender.subscribe()
    }

    fn attach_delivery(&self, delivery: &Arc<StreamDelivery>) -> Result<()> {
        *self.delivery.lock().unwrap() = Some(Arc::downgrade(delivery));
        let terminal = {
            let status = self.status.lock().unwrap();
            status
                .state
                .is_terminal()
                .then(|| (status.state, status.error.clone(), status.query_id))
        };
        let Some((state, error, query_id)) = terminal else {
            return Ok(());
        };
        if let Some(error) = error {
            delivery.cancel(error.clone());
            return Err(error);
        }

        delivery.close();
        Err(ServerError::new(
            ErrorCode::Conflict,
            format!("query completed before result delivery attached state=[{state:?}]"),
        )
        .with_query_id(query_id))
    }

    fn cancel(&self, query_id: QueryId, reason: &'static str) -> bool {
        let error = ServerError::new(ErrorCode::Cancelled, reason).with_query_id(query_id);
        self.cancel_with_error(error)
    }

    fn cancel_with_error(&self, error: ServerError) -> bool {
        debug_assert_eq!(error.code(), ErrorCode::Cancelled);
        self.finish(QueryState::Cancelled, Some(error))
    }

    fn timeout(&self, query_id: QueryId, reason: &'static str) -> bool {
        let error = ServerError::new(ErrorCode::DeadlineExceeded, reason).with_query_id(query_id);
        self.timeout_with_error(error)
    }

    fn timeout_with_error(&self, error: ServerError) -> bool {
        debug_assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        self.finish(QueryState::TimedOut, Some(error))
    }

    fn fail(&self, error: ServerError) -> bool {
        self.finish(QueryState::Failed, Some(error))
    }

    fn finish_gateway_error(&self, error: ServerError) -> bool {
        match error.code() {
            ErrorCode::Cancelled => self.cancel_with_error(error),
            ErrorCode::DeadlineExceeded => self.timeout_with_error(error),
            _ => self.fail(error),
        }
    }

    fn finish(&self, state: QueryState, error: Option<ServerError>) -> bool {
        debug_assert!(state.is_terminal());
        debug_assert_eq!(error.is_none(), state == QueryState::Finished);
        let (query_id, finished_at_ns) = {
            let mut status = self.status.lock().unwrap();
            if status.state.is_terminal() {
                return false;
            }
            if !status.state.can_transition_to(state) {
                debug_assert!(
                    false,
                    "invalid query transition current={:?} next={state:?}",
                    status.state
                );
                return false;
            }
            let finished_at_ns = now_ns();
            status.state = state;
            status.finished_at_ns = Some(finished_at_ns);
            status.error = error.clone();
            (status.query_id, finished_at_ns)
        };
        if let Some(engine) = self.engine.upgrade() {
            engine.queries.lock().unwrap().record_terminal(
                query_id,
                finished_at_ns,
                self.generation,
            );
        }
        drop(self.admission_permit.lock().unwrap().take());
        if state != QueryState::Finished {
            self.cancelled.store(true, Ordering::Release);
            if let Some(error) = error.clone() {
                self.notify_delivery(error.clone());
                self.terminal_sender.send_replace(Some(error));
            }
        }
        true
    }

    fn terminal_error(&self) -> Option<ServerError> {
        let status = self.status.lock().unwrap();
        status
            .state
            .is_terminal()
            .then(|| status.error.clone())
            .flatten()
    }

    fn remaining_deadline(&self, now: u64) -> Option<Duration> {
        self.deadline_ns
            .map(|deadline| Duration::from_nanos(deadline.saturating_sub(now).max(1)))
    }

    fn query_deadline_is_due(&self, now: u64) -> bool {
        self.deadline_ns
            .is_some_and(|deadline_ns| deadline_ns <= now)
    }

    fn notify_delivery(&self, error: ServerError) {
        let delivery = self
            .delivery
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(delivery) = delivery {
            delivery.cancel(error);
        }
    }
}

#[derive(Default)]
struct StreamDelivery {
    state: Mutex<DeliveryState>,
    changed: Condvar,
}

#[derive(Default)]
struct DeliveryState {
    item: Option<DeliveryItem>,
    waker: Option<Waker>,
    cancelled: bool,
    terminal_error: Option<ServerError>,
}

enum DeliveryItem {
    Batch(RecordBatch),
    Eof,
}

impl StreamDelivery {
    fn send(&self, item: DeliveryItem) -> bool {
        let waker = {
            let mut state = self.state.lock().unwrap();
            while state.item.is_some() && !state.cancelled {
                state = self.changed.wait(state).unwrap();
            }
            if state.cancelled {
                return false;
            }
            state.item = Some(item);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    fn cancel(&self, error: ServerError) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
            state.terminal_error.get_or_insert(error);
            self.changed.notify_all();
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn close(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
            self.changed.notify_all();
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct LegacyRecordBatchStream {
    query_id: QueryId,
    schema: SchemaRef,
    shared: Arc<StreamDelivery>,
    control: Arc<QueryControl>,
    terminal_observed: bool,
}

impl RecordBatchStream for LegacyRecordBatchStream {
    fn instance_id(&self) -> QueryInstanceId {
        query_instance_id(self.query_id, self.control.generation)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.terminal_observed {
            return Poll::Ready(None);
        }
        let polled = {
            let mut state = self.shared.state.lock().unwrap();
            if state.cancelled {
                Poll::Ready(Some(Err(state
                    .terminal_error
                    .clone()
                    .unwrap_or_else(|| cancelled_error(self.query_id)))))
            } else {
                match state.item.take() {
                    Some(DeliveryItem::Batch(batch)) => {
                        self.shared.changed.notify_one();
                        Poll::Ready(Some(Ok(batch)))
                    }
                    Some(DeliveryItem::Eof) => {
                        self.shared.changed.notify_one();
                        Poll::Ready(None)
                    }
                    None => {
                        if state
                            .waker
                            .as_ref()
                            .is_none_or(|waker| !waker.will_wake(context.waker()))
                        {
                            state.waker = Some(context.waker().clone());
                        }
                        Poll::Pending
                    }
                }
            }
        };
        if matches!(&polled, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.terminal_observed = true;
        }
        polled
    }
}

impl Drop for LegacyRecordBatchStream {
    fn drop(&mut self) {
        if !self.terminal_observed {
            self.control
                .cancel(self.query_id, "query result stream dropped");
            self.shared.close();
        }
    }
}

struct CancelOnDrop {
    control: Arc<QueryControl>,
    query_id: QueryId,
    armed: bool,
}

impl CancelOnDrop {
    fn new(control: Arc<QueryControl>, query_id: QueryId) -> Self {
        Self {
            control,
            query_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.control
                .cancel(self.query_id, "query execution future dropped");
        }
    }
}

fn spawn_driver(
    mut cursor: LegacyCollectCursor,
    shared: Arc<StreamDelivery>,
    control: Arc<QueryControl>,
    query_id: QueryId,
    permit: BlockingWorkerPermit,
) -> Result<()> {
    let failure_control = Arc::clone(&control);
    thread::Builder::new()
        .name("zippy-legacy-query-driver".to_string())
        .spawn(move || {
            let _permit = permit;
            loop {
                if control.cancelled.load(Ordering::Acquire) {
                    control.cancel(query_id, "legacy query cursor was cancelled");
                    if let Some(error) = control.terminal_error() {
                        shared.cancel(error);
                    } else {
                        shared.close();
                    }
                    return;
                }
                match cursor.next_batch() {
                    Ok(Some(batch)) => {
                        if !shared.send(DeliveryItem::Batch(batch)) {
                            return;
                        }
                    }
                    Ok(None) => {
                        if control.finish(QueryState::Finished, None) {
                            let _ = shared.send(DeliveryItem::Eof);
                        } else if let Some(error) = control.terminal_error() {
                            shared.cancel(error);
                        } else {
                            shared.close();
                        }
                        return;
                    }
                    Err(error) => {
                        let error = map_gateway_error(error, query_id);
                        control.finish_gateway_error(error);
                        return;
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(|error| {
            let error = ServerError::new(
                ErrorCode::Unavailable,
                format!("failed to spawn legacy query driver error=[{error}]"),
            )
            .with_query_id(query_id);
            failure_control.fail(error.clone());
            failure_control.terminal_error().unwrap_or(error)
        })
}

fn spawn_sweeper(inner: &Arc<EngineInner>, interval: Duration) -> Result<()> {
    let inner = Arc::downgrade(inner);
    thread::Builder::new()
        .name("zippy-legacy-query-sweeper".to_string())
        .spawn(move || loop {
            thread::sleep(interval);
            let Some(inner) = inner.upgrade() else {
                return;
            };
            inner.sweep(now_ns());
        })
        .map(|_| ())
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("failed to spawn legacy query sweeper error=[{error}]"),
            )
        })
}

fn validate_request_capabilities(request: &QueryRequest) -> Result<()> {
    if !request.tables.is_empty() {
        return Err(unsupported("legacy_gateway.snapshot_table_binding"));
    }
    if request.memory_limit_bytes != ResourceLimit::ServerDefault {
        return Err(unsupported("legacy_gateway.per_query_memory_limit"));
    }
    if matches!(request.spill_limit_bytes, ResourceLimit::Bytes(_)) {
        return Err(unsupported("legacy_gateway.spill_limit"));
    }
    Ok(())
}

fn unsupported(operator: &str) -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("legacy query adapter does not support operator=[{operator}]"),
    )
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operator.to_string(),
    })
    .expect("unsupported detail always matches unsupported code")
}

fn cancelled_error(query_id: QueryId) -> ServerError {
    ServerError::new(ErrorCode::Cancelled, "legacy query was cancelled").with_query_id(query_id)
}

fn deadline_error(query_id: QueryId, reason: &'static str) -> ServerError {
    ServerError::new(ErrorCode::DeadlineExceeded, reason).with_query_id(query_id)
}

async fn wait_for_terminal(receiver: &mut watch::Receiver<Option<ServerError>>) -> ServerError {
    loop {
        if let Some(error) = receiver.borrow().clone() {
            return error;
        }
        receiver
            .changed()
            .await
            .expect("query control holds the terminal notification sender");
    }
}

async fn wait_for_deadline(deadline_ns: Option<u64>) {
    let Some(deadline_ns) = deadline_ns else {
        std::future::pending::<()>().await;
        return;
    };
    tokio::time::sleep(Duration::from_nanos(deadline_ns.saturating_sub(now_ns()))).await;
}

fn query_unavailable(query_id: QueryId) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "legacy query engine is not accepting new work",
    )
    .with_query_id(query_id)
}

fn map_gateway_error(error: ZippyError, query_id: QueryId) -> ServerError {
    let message = error.to_string();
    match error {
        ZippyError::InvalidConfig { .. } => ServerError::new(
            ErrorCode::UnsupportedPlan,
            format!("legacy gateway collect rejected request error=[{message}]"),
        )
        .with_query_id(query_id)
        .with_details(ServerErrorDetails::UnsupportedPlan {
            operator: "legacy_gateway.collect".to_string(),
        })
        .expect("unsupported detail always matches unsupported code"),
        ZippyError::ResourceExhausted { .. } => {
            ServerError::new(ErrorCode::ResourceExhausted, message).with_query_id(query_id)
        }
        ZippyError::NotFound { .. } => {
            ServerError::new(ErrorCode::NotFound, message).with_query_id(query_id)
        }
        ZippyError::Conflict { .. } => {
            ServerError::new(ErrorCode::Conflict, message).with_query_id(query_id)
        }
        ZippyError::DataCorruption { .. } => {
            ServerError::new(ErrorCode::DataCorruption, message).with_query_id(query_id)
        }
        ZippyError::SchemaMismatch { .. } => {
            ServerError::new(ErrorCode::SchemaMismatch, message).with_query_id(query_id)
        }
        ZippyError::Cancelled { .. } => {
            ServerError::new(ErrorCode::Cancelled, message).with_query_id(query_id)
        }
        ZippyError::DeadlineExceeded { .. } => {
            ServerError::new(ErrorCode::DeadlineExceeded, message).with_query_id(query_id)
        }
        ZippyError::InvalidState { status: "stopped" } => {
            ServerError::new(ErrorCode::Unavailable, message).with_query_id(query_id)
        }
        ZippyError::InvalidState { .. } => {
            ServerError::new(ErrorCode::Conflict, message).with_query_id(query_id)
        }
        ZippyError::Io { .. }
        | ZippyError::ChannelSend
        | ZippyError::ChannelReceive
        | ZippyError::QueueClosed
        | ZippyError::MasterShutdownRequested { .. } => {
            ServerError::new(ErrorCode::Unavailable, message).with_query_id(query_id)
        }
        ZippyError::LateData { .. } => {
            ServerError::new(ErrorCode::Internal, message).with_query_id(query_id)
        }
    }
}

fn map_gateway_open_error(
    error: ZippyError,
    query_id: QueryId,
    control: &QueryControl,
    now: u64,
) -> ServerError {
    match error {
        ZippyError::DeadlineExceeded { reason } if !control.query_deadline_is_due(now) => {
            ServerError::new(
                ErrorCode::Unavailable,
                format!(
                    concat!(
                        "legacy gateway operational timeout before query deadline ",
                        "reason=[{}]"
                    ),
                    reason
                ),
            )
            .with_query_id(query_id)
        }
        error => map_gateway_error(error, query_id),
    }
}

fn query_instance_id(query_id: QueryId, generation: u64) -> QueryInstanceId {
    QueryInstanceId::new(
        query_id,
        NonZeroU64::new(generation).expect("query generation is nonzero"),
    )
}

fn next_engine_nonce() -> u128 {
    let sequence = NEXT_ENGINE_NONCE.fetch_add(1, Ordering::Relaxed).max(1);
    (u128::from(now_ns()) << 64) | u128::from(sequence)
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::io::{BufRead, BufReader, Write};
    use std::num::NonZeroU32;
    use std::sync::{mpsc, Arc};
    use std::task::{Wake, Waker};
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use zippy_core::{
        ControlEndpoint, ControlRequest, ControlResponse, GetStreamResponse, StreamInfo,
    };
    use zippy_gateway::{GatewayServer, GatewayServerConfig};
    use zippy_master::server::MasterServer;
    use zippy_types::query::{
        OpaqueQueryExecutionToken, QueryEngine, QueryInput, QueryInstanceId, QueryPriority,
        QueryRequest, QueryState,
    };
    use zippy_types::{ErrorCode, QueryId, ResourceLimit, SessionId};

    use super::{
        LegacyGatewayPlan, LegacyQueryEngineAdapter, LegacyQueryEngineConfig,
        LEGACY_GATEWAY_PLAN_FORMAT_VERSION,
    };

    fn test_engine(config: LegacyQueryEngineConfig) -> (LegacyQueryEngineAdapter, GatewayServer) {
        let master = MasterServer::default();
        let catalog = Arc::new(master.legacy_catalog_adapter());
        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();
        let engine =
            LegacyQueryEngineAdapter::with_config(gateway.legacy_collect_facade(), catalog, config)
                .unwrap();
        (engine, gateway)
    }

    fn query_request(query_id: u128, session_id: SessionId) -> QueryRequest {
        QueryRequest {
            query_id: QueryId::from_u128(query_id).unwrap(),
            session_id,
            input: QueryInput::LogicalPlan(
                LegacyGatewayPlan::new("ticks", Vec::new())
                    .unwrap()
                    .encode()
                    .unwrap(),
            ),
            tables: Vec::new(),
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: NonZeroU32::new(128),
        }
    }

    fn token_instance(token: &OpaqueQueryExecutionToken) -> QueryInstanceId {
        token.instance_id()
    }

    fn persisted_stream(stream_name: &str, file_path: &std::path::Path) -> StreamInfo {
        StreamInfo {
            stream_name: stream_name.to_string(),
            schema: json!({
                "fields": [{
                    "name": "seq",
                    "segment_type": "int64",
                    "nullable": false
                }],
                "metadata": {}
            }),
            schema_hash: "legacy-query-test-schema".to_string(),
            data_path: "segment".to_string(),
            descriptor_generation: 1,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: None,
            sealed_segments: Vec::new(),
            persisted_files: vec![json!({
                "file_path": file_path.to_string_lossy(),
                "row_count": 5,
                "source_segment_id": 1,
                "source_generation": 0
            })],
            persisted_files_revision: 1,
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 1,
            reader_count: 0,
            status: "stale".to_string(),
        }
    }

    fn spawn_master_response(
        stream_info: StreamInfo,
    ) -> (ControlEndpoint, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = ControlEndpoint::Tcp(listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
            assert!(matches!(request, ControlRequest::GetStream(_)));
            let response = ControlResponse::StreamFetched(GetStreamResponse {
                stream: stream_info,
            });
            stream
                .write_all(serde_json::to_string(&response).unwrap().as_bytes())
                .unwrap();
            stream.write_all(b"\n").unwrap();
        });
        (endpoint, server)
    }

    fn test_engine_with_endpoint(
        endpoint: ControlEndpoint,
    ) -> (LegacyQueryEngineAdapter, GatewayServer) {
        test_engine_with_endpoint_config(endpoint, None, LegacyQueryEngineConfig::default())
    }

    fn test_engine_with_endpoint_config(
        endpoint: ControlEndpoint,
        write_timeout_ms: Option<u64>,
        config: LegacyQueryEngineConfig,
    ) -> (LegacyQueryEngineAdapter, GatewayServer) {
        let master = MasterServer::default();
        let catalog = Arc::new(master.legacy_catalog_adapter());
        let gateway = GatewayServer::new_with_query_limits(
            GatewayServerConfig {
                endpoint: "127.0.0.1:0".to_string(),
                master_endpoint: endpoint,
                token: None,
                max_write_rows: None,
                max_connections: None,
                max_subscribers: None,
                max_blocking_requests: None,
                write_timeout_ms,
            },
            2,
            2,
            64 * 1024,
            32 * 1024,
        )
        .unwrap();
        let engine =
            LegacyQueryEngineAdapter::with_config(gateway.legacy_collect_facade(), catalog, config)
                .unwrap();
        (engine, gateway)
    }

    fn spawn_stalled_master_response() -> (
        ControlEndpoint,
        mpsc::Receiver<()>,
        mpsc::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = ControlEndpoint::Tcp(listener.local_addr().unwrap());
        let (accepted_sender, accepted_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
            assert!(matches!(request, ControlRequest::GetStream(_)));
            accepted_sender.send(()).unwrap();
            let _ = release_receiver.recv_timeout(Duration::from_secs(2));
            drop(stream);
        });
        (endpoint, accepted_receiver, release_sender, server)
    }

    #[test]
    fn compatibility_plan_uses_a_distinct_format_version() {
        let plan = LegacyGatewayPlan::new("ticks", Vec::new()).unwrap();
        let envelope = plan.encode().unwrap();

        assert_eq!(
            envelope.format_version(),
            LEGACY_GATEWAY_PLAN_FORMAT_VERSION
        );
        assert_ne!(
            envelope.format_version(),
            zippy_types::query::CURRENT_LOGICAL_PLAN_FORMAT_VERSION
        );
        assert_eq!(LegacyGatewayPlan::decode(&envelope).unwrap(), plan);
    }

    #[test]
    fn compatibility_plan_rejects_blank_source() {
        let error = LegacyGatewayPlan::new("  ", Vec::new()).unwrap_err();

        assert_eq!(error.code(), zippy_types::ErrorCode::InvalidArgument);
    }

    #[test]
    fn gateway_runtime_failures_map_without_message_parsing() {
        let query_id = QueryId::from_u128(19).unwrap();
        let cases = [
            (
                zippy_core::ZippyError::ResourceExhausted {
                    reason: "budget".to_string(),
                },
                ErrorCode::ResourceExhausted,
            ),
            (
                zippy_core::ZippyError::NotFound {
                    reason: "file".to_string(),
                },
                ErrorCode::NotFound,
            ),
            (
                zippy_core::ZippyError::DataCorruption {
                    reason: "parquet".to_string(),
                },
                ErrorCode::DataCorruption,
            ),
            (
                zippy_core::ZippyError::Cancelled {
                    reason: "owner".to_string(),
                },
                ErrorCode::Cancelled,
            ),
            (
                zippy_core::ZippyError::DeadlineExceeded {
                    reason: "deadline".to_string(),
                },
                ErrorCode::DeadlineExceeded,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(super::map_gateway_error(error, query_id).code(), expected);
        }
    }

    #[tokio::test]
    async fn owner_capability_is_bound_to_one_engine_instance() {
        let (first, _first_gateway) = test_engine(LegacyQueryEngineConfig::default());
        let (second, _second_gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(1).unwrap();
        let owner = first.owner(session_id);

        let error = second
            .plan(owner.as_ref(), query_request(1, session_id))
            .await
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::PermissionDenied);
    }

    #[tokio::test]
    async fn owner_capability_nonce_isolates_query_instances_in_one_session() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(6).unwrap();
        let first_owner = engine.owner(session_id);
        let second_owner = engine.owner(session_id);
        let request = query_request(6, session_id);
        let token = engine.plan(first_owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        let error = engine
            .status(second_owner.as_ref(), instance_id)
            .await
            .unwrap_err();

        assert_eq!(error.code(), ErrorCode::PermissionDenied);
        let status = engine
            .status(first_owner.as_ref(), instance_id)
            .await
            .unwrap();
        assert_eq!(status.instance_id(), instance_id);
        assert_eq!(status.state, QueryState::Planned);
        drop(token);
    }

    #[tokio::test]
    async fn early_eviction_tombstone_blocks_query_id_reuse() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            max_registered_queries: 1,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(7).unwrap();
        let owner = engine.owner(session_id);
        let evicted = engine
            .plan(owner.as_ref(), query_request(7, session_id))
            .await
            .unwrap();
        let evicted_instance = token_instance(&evicted);
        drop(evicted);
        let replacement = engine
            .plan(owner.as_ref(), query_request(8, session_id))
            .await
            .unwrap();

        let status_error = engine
            .status(owner.as_ref(), evicted_instance)
            .await
            .unwrap_err();
        let reuse_error = engine
            .plan(owner.as_ref(), query_request(7, session_id))
            .await
            .err()
            .unwrap();

        assert_eq!(status_error.code(), ErrorCode::NotFound);
        assert_eq!(reuse_error.code(), ErrorCode::Conflict);
        assert_eq!(engine.inner.queries.lock().unwrap().tombstones.len(), 1);
        drop(replacement);
    }

    #[tokio::test]
    async fn tombstone_capacity_exhaustion_fails_closed() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            max_registered_queries: 1,
            max_query_tombstones: 1,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(9).unwrap();
        let owner = engine.owner(session_id);
        let first = engine
            .plan(owner.as_ref(), query_request(9, session_id))
            .await
            .unwrap();
        drop(first);
        let retained = engine
            .plan(owner.as_ref(), query_request(10, session_id))
            .await
            .unwrap();
        let retained_instance = token_instance(&retained);
        drop(retained);

        let error = engine
            .plan(owner.as_ref(), query_request(11, session_id))
            .await
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(
            engine
                .status(owner.as_ref(), retained_instance)
                .await
                .unwrap()
                .state,
            QueryState::Cancelled
        );
        assert_eq!(engine.inner.queries.lock().unwrap().tombstones.len(), 1);
    }

    #[tokio::test]
    async fn dropping_unexecuted_token_cancels_query_synchronously() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(2).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(2, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        drop(token);

        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::Cancelled);
        assert_eq!(status.error.unwrap().code(), ErrorCode::Cancelled);
    }

    #[tokio::test]
    async fn dropping_unpolled_execution_future_cancels_query() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(20).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(20, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        let execution = engine.execute(token);
        drop(execution);

        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::Cancelled);
    }

    #[tokio::test]
    async fn terminal_query_flood_does_not_block_new_plans() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            sweep_interval: Duration::from_secs(1),
            max_registered_queries: 2,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(21).unwrap();
        let owner = engine.owner(session_id);
        let first_id = QueryId::from_u128(21).unwrap();
        let second_id = QueryId::from_u128(22).unwrap();

        let first = engine
            .plan(owner.as_ref(), query_request(21, session_id))
            .await
            .unwrap();
        drop(first);
        let second = engine
            .plan(owner.as_ref(), query_request(22, session_id))
            .await
            .unwrap();
        drop(second);

        let replacement = engine
            .plan(owner.as_ref(), query_request(23, session_id))
            .await
            .unwrap();

        assert_eq!(engine.inner.queries.lock().unwrap().len(), 2);
        let evicted = [first_id, second_id]
            .into_iter()
            .filter(|query_id| {
                engine
                    .inner
                    .control(*query_id)
                    .is_err_and(|error| error.code() == ErrorCode::NotFound)
            })
            .count();
        assert_eq!(evicted, 1);
        drop(replacement);
    }

    #[tokio::test]
    async fn active_only_registry_capacity_still_rejects_new_plans() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            max_registered_queries: 2,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(24).unwrap();
        let owner = engine.owner(session_id);
        let first = engine
            .plan(owner.as_ref(), query_request(24, session_id))
            .await
            .unwrap();
        let second = engine
            .plan(owner.as_ref(), query_request(25, session_id))
            .await
            .unwrap();

        let error = engine
            .plan(owner.as_ref(), query_request(26, session_id))
            .await
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(engine.inner.queries.lock().unwrap().len(), 2);
        drop(first);
        drop(second);
    }

    #[tokio::test]
    async fn terminal_eviction_uses_query_id_as_a_stable_tie_break() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            sweep_interval: Duration::from_secs(1),
            max_registered_queries: 2,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(27).unwrap();
        let owner = engine.owner(session_id);
        let first_id = QueryId::from_u128(27).unwrap();
        let second_id = QueryId::from_u128(28).unwrap();
        let first = engine
            .plan(owner.as_ref(), query_request(27, session_id))
            .await
            .unwrap();
        let first_instance = token_instance(&first);
        let first_control = engine.inner.control(first_id).unwrap();
        drop(first);
        let second = engine
            .plan(owner.as_ref(), query_request(28, session_id))
            .await
            .unwrap();
        let second_instance = token_instance(&second);
        let second_control = engine.inner.control(second_id).unwrap();
        drop(second);
        let shared_finished_at = super::now_ns();
        first_control.status.lock().unwrap().finished_at_ns = Some(shared_finished_at);
        second_control.status.lock().unwrap().finished_at_ns = Some(shared_finished_at);
        {
            let mut registry = engine.inner.queries.lock().unwrap();
            registry.terminal_order.clear();
            registry.record_terminal(first_id, shared_finished_at, first_control.generation);
            registry.record_terminal(second_id, shared_finished_at, second_control.generation);
        }

        let replacement = engine
            .plan(owner.as_ref(), query_request(29, session_id))
            .await
            .unwrap();

        let evicted_error = engine
            .status(owner.as_ref(), first_instance)
            .await
            .unwrap_err();
        assert_eq!(evicted_error.code(), ErrorCode::NotFound);
        assert_eq!(
            engine
                .status(owner.as_ref(), second_instance)
                .await
                .unwrap()
                .state,
            QueryState::Cancelled
        );
        drop(replacement);
    }

    #[tokio::test]
    async fn sweeper_prunes_expired_terminal_before_duplicate_reuse() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            sweep_interval: Duration::from_secs(1),
            terminal_retention: Duration::from_nanos(1),
            max_registered_queries: 1,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(39).unwrap();
        let owner = engine.owner(session_id);
        let query_id = QueryId::from_u128(39).unwrap();
        let old = engine
            .plan(owner.as_ref(), query_request(39, session_id))
            .await
            .unwrap();
        let old_control = engine.inner.control(query_id).unwrap();
        drop(old);
        engine.inner.sweep(super::now_ns().saturating_add(1));

        let replacement = engine
            .plan(owner.as_ref(), query_request(39, session_id))
            .await
            .unwrap();
        let replacement_control = engine.inner.control(query_id).unwrap();

        assert!(!Arc::ptr_eq(&old_control, &replacement_control));
        engine
            .inner
            .queries
            .lock()
            .unwrap()
            .terminal_order
            .push(std::cmp::Reverse((0, query_id, old_control.generation)));
        let error = engine
            .plan(owner.as_ref(), query_request(40, session_id))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(Arc::ptr_eq(
            &replacement_control,
            &engine.inner.control(query_id).unwrap()
        ));
        drop(replacement);
    }

    #[tokio::test]
    async fn terminal_registry_churn_keeps_heap_and_map_bounded() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            sweep_interval: Duration::from_secs(1),
            max_registered_queries: 4,
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(41).unwrap();
        let owner = engine.owner(session_id);

        for query_id in 41_u128..141 {
            let token = engine
                .plan(owner.as_ref(), query_request(query_id, session_id))
                .await
                .unwrap();
            drop(token);
        }

        let registry = engine.inner.queries.lock().unwrap();
        assert_eq!(registry.len(), 4);
        assert_eq!(registry.terminal_order.len(), 4);
        assert!(!registry.contains_key(&QueryId::from_u128(41).unwrap()));
        assert!(registry.contains_key(&QueryId::from_u128(140).unwrap()));
    }

    #[test]
    fn concurrent_query_permits_are_strictly_bounded() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            max_concurrent_queries: 1,
            ..LegacyQueryEngineConfig::default()
        });
        let first = engine
            .inner
            .acquire_query_permit(QueryId::from_u128(30).unwrap())
            .unwrap();

        let error = engine
            .inner
            .acquire_query_permit(QueryId::from_u128(31).unwrap())
            .err()
            .unwrap();
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);

        drop(first);
        let second = engine
            .inner
            .acquire_query_permit(QueryId::from_u128(31).unwrap())
            .unwrap();
        drop(second);
    }

    #[tokio::test]
    async fn logical_admission_failure_sets_matching_failed_status() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            max_concurrent_queries: 1,
            ..LegacyQueryEngineConfig::default()
        });
        let occupied = engine
            .inner
            .acquire_query_permit(QueryId::from_u128(42).unwrap())
            .unwrap();
        let session_id = SessionId::from_u128(42).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(42, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        let error = engine.execute(token).await.err().unwrap();
        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();

        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(status.state, QueryState::Failed);
        assert_eq!(status.error.as_ref(), Some(&error));
        drop(occupied);
    }

    #[test]
    fn delivery_attach_after_cancellation_observes_first_terminal_error() {
        let session_id = SessionId::from_u128(32).unwrap();
        let request = query_request(32, session_id);
        let now = super::now_ns();
        let info = zippy_types::query::QueryPlanInfo {
            query_id: request.query_id,
            catalog_revision: 1,
            snapshots: Vec::new(),
            planned_at_ns: now,
            expires_at_ns: now.saturating_add(1_000_000_000),
        };
        let control = super::QueryControl::new(&request, &info, now, std::sync::Weak::new(), 1, 1);

        assert!(control.cancel(request.query_id, "first cancellation wins"));
        let status_error = control.status.lock().unwrap().error.clone().unwrap();
        let delivery = Arc::new(super::StreamDelivery::default());
        let attach_error = control.attach_delivery(&delivery).unwrap_err();

        assert_eq!(attach_error, status_error);
        let delivery_error = delivery
            .state
            .lock()
            .unwrap()
            .terminal_error
            .clone()
            .unwrap();
        assert_eq!(delivery_error, status_error);
        assert!(!control.timeout(request.query_id, "later timeout loses"));
        assert_eq!(
            control.status.lock().unwrap().error.as_ref(),
            Some(&status_error)
        );
    }

    #[test]
    fn structured_gateway_deadline_sets_timed_out_status_and_wins_terminal_race() {
        let session_id = SessionId::from_u128(43).unwrap();
        let request = query_request(43, session_id);
        let query_id = request.query_id;
        let now = super::now_ns();
        let info = zippy_types::query::QueryPlanInfo {
            query_id,
            catalog_revision: 1,
            snapshots: Vec::new(),
            planned_at_ns: now,
            expires_at_ns: now.saturating_add(1_000_000_000),
        };
        let control = super::QueryControl::new(&request, &info, now, std::sync::Weak::new(), 1, 1);
        let error = super::map_gateway_error(
            zippy_core::ZippyError::DeadlineExceeded {
                reason: "control deadline".to_string(),
            },
            query_id,
        );

        assert!(control.finish_gateway_error(error.clone()));
        assert!(!control.cancel(query_id, "later cancellation loses"));

        let status = control.status.lock().unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(status.error.as_ref(), Some(&error));
    }

    #[test]
    fn delivery_waker_runs_without_holding_delivery_lock() {
        struct LockingWake {
            delivery: Arc<super::StreamDelivery>,
        }

        impl Wake for LockingWake {
            fn wake(self: Arc<Self>) {
                drop(self.delivery.state.lock().unwrap());
            }
        }

        let query_id = QueryId::from_u128(33).unwrap();
        let delivery = Arc::new(super::StreamDelivery::default());
        delivery.state.lock().unwrap().waker = Some(Waker::from(Arc::new(LockingWake {
            delivery: Arc::clone(&delivery),
        })));
        let (completed_sender, completed_receiver) = mpsc::channel();
        let worker_delivery = Arc::clone(&delivery);
        let worker = std::thread::spawn(move || {
            worker_delivery.cancel(super::cancelled_error(query_id));
            completed_sender.send(()).unwrap();
        });

        completed_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("waker must not run while the delivery lock is held");
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn cancellation_releases_admission_but_retains_stalled_worker_slot() {
        let (endpoint, accepted, release, server) = spawn_stalled_master_response();
        let (engine, _gateway) = test_engine_with_endpoint_config(
            endpoint,
            Some(3_000),
            LegacyQueryEngineConfig {
                max_concurrent_queries: 1,
                max_blocking_query_workers: 2,
                ..LegacyQueryEngineConfig::default()
            },
        );
        let session_id = SessionId::from_u128(34).unwrap();
        let owner = engine.owner(session_id);
        let mut request = query_request(34, session_id);
        request.deadline_ns = Some(super::now_ns().saturating_add(500_000_000));
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let mut execution = engine.execute(token);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut execution)
                .await
                .is_err()
        );
        accepted.recv_timeout(Duration::from_millis(500)).unwrap();
        engine.cancel(owner.as_ref(), instance_id).await.unwrap();

        let error = tokio::time::timeout(Duration::from_millis(100), execution)
            .await
            .expect("cancellation must wake the execution future")
            .err()
            .unwrap();
        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert_eq!(
            engine
                .status(owner.as_ref(), instance_id)
                .await
                .unwrap()
                .state,
            QueryState::Cancelled
        );
        assert_eq!(
            engine
                .inner
                .active_queries
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );

        let replacement_admission = engine
            .inner
            .acquire_query_permit(QueryId::from_u128(35).unwrap())
            .unwrap();
        let replacement_worker = engine
            .inner
            .acquire_blocking_worker_permit(QueryId::from_u128(35).unwrap())
            .unwrap();
        assert_eq!(
            engine
                .inner
                .active_blocking_workers
                .load(std::sync::atomic::Ordering::Acquire),
            2
        );
        let worker_error = engine
            .inner
            .acquire_blocking_worker_permit(QueryId::from_u128(38).unwrap())
            .err()
            .unwrap();
        assert_eq!(worker_error.code(), ErrorCode::ResourceExhausted);
        drop(replacement_worker);
        drop(replacement_admission);

        assert_eq!(
            engine
                .inner
                .active_blocking_workers
                .load(std::sync::atomic::Ordering::Acquire),
            1
        );
        let _ = release.send(());
        server.join().unwrap();
        let opener_released = tokio::time::timeout(Duration::from_millis(500), async {
            while engine
                .inner
                .active_blocking_workers
                .load(std::sync::atomic::Ordering::Acquire)
                != 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        assert!(
            opener_released,
            "stalled opener did not release its worker slot"
        );

        let worker_permit = engine
            .inner
            .acquire_blocking_worker_permit(QueryId::from_u128(35).unwrap())
            .unwrap();
        drop(worker_permit);
    }

    #[tokio::test]
    async fn explicit_deadline_times_out_a_stalled_opener() {
        let (endpoint, accepted, release, server) = spawn_stalled_master_response();
        let (engine, _gateway) = test_engine_with_endpoint_config(
            endpoint,
            Some(3_000),
            LegacyQueryEngineConfig {
                sweep_interval: Duration::from_secs(1),
                max_concurrent_queries: 1,
                max_blocking_query_workers: 1,
                ..LegacyQueryEngineConfig::default()
            },
        );
        let session_id = SessionId::from_u128(36).unwrap();
        let owner = engine.owner(session_id);
        let mut request = query_request(36, session_id);
        request.deadline_ns = Some(super::now_ns().saturating_add(40_000_000));
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let mut execution = engine.execute(token);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut execution)
                .await
                .is_err()
        );
        accepted.recv_timeout(Duration::from_millis(500)).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), execution)
            .await
            .expect("explicit deadline must wake the execution future")
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(status.error.unwrap().code(), ErrorCode::DeadlineExceeded);
        assert_eq!(
            engine
                .inner
                .active_queries
                .load(std::sync::atomic::Ordering::Acquire),
            0
        );

        let _ = release.send(());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn gateway_operational_timeout_before_query_deadline_sets_failed() {
        let (endpoint, accepted, release, server) = spawn_stalled_master_response();
        let (engine, _gateway) = test_engine_with_endpoint_config(
            endpoint,
            Some(20),
            LegacyQueryEngineConfig {
                sweep_interval: Duration::from_secs(1),
                ..LegacyQueryEngineConfig::default()
            },
        );
        let session_id = SessionId::from_u128(37).unwrap();
        let owner = engine.owner(session_id);
        let mut request = query_request(37, session_id);
        request.deadline_ns = Some(super::now_ns().saturating_add(500_000_000));
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let mut execution = engine.execute(token);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut execution)
                .await
                .is_err()
        );
        accepted.recv_timeout(Duration::from_millis(500)).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), execution)
            .await
            .expect("gateway socket timeout must remain bounded")
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(error
            .message()
            .contains("legacy gateway operational timeout"));
        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::Failed);
        assert_eq!(status.error.as_ref(), Some(&error));

        let _ = release.send(());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn gateway_operational_timeout_without_query_deadline_sets_failed() {
        let (endpoint, accepted, release, server) = spawn_stalled_master_response();
        let (engine, _gateway) = test_engine_with_endpoint_config(
            endpoint,
            Some(20),
            LegacyQueryEngineConfig {
                sweep_interval: Duration::from_secs(1),
                ..LegacyQueryEngineConfig::default()
            },
        );
        let session_id = SessionId::from_u128(44).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(44, session_id);
        let query_id = request.query_id;
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let mut execution = engine.execute(token);

        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut execution)
                .await
                .is_err()
        );
        accepted.recv_timeout(Duration::from_millis(500)).unwrap();
        let error = tokio::time::timeout(Duration::from_millis(500), execution)
            .await
            .expect("gateway operational timeout must remain bounded")
            .err()
            .unwrap();

        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(error.query_id(), Some(query_id));
        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::Failed);
        assert_eq!(status.error.as_ref(), Some(&error));

        let _ = release.send(());
        server.join().unwrap();
    }

    #[tokio::test]
    async fn sweeper_times_out_an_unexecuted_plan() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            plan_ttl: Duration::from_millis(10),
            sweep_interval: Duration::from_millis(2),
            terminal_retention: Duration::from_secs(1),
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(3).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(3, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        tokio::time::sleep(Duration::from_millis(30)).await;

        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(status.error.unwrap().code(), ErrorCode::DeadlineExceeded);
        drop(token);
    }

    #[tokio::test]
    async fn sweeper_enforces_deadline_after_query_starts() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            sweep_interval: Duration::from_millis(2),
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(40).unwrap();
        let owner = engine.owner(session_id);
        let mut request = query_request(40, session_id);
        request.deadline_ns = Some(super::now_ns().saturating_add(10_000_000));
        let query_id = request.query_id;
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let control = engine.inner.control(query_id).unwrap();
        let delivery = Arc::new(super::StreamDelivery::default());
        control.attach_delivery(&delivery).unwrap();
        let permit = engine.inner.acquire_query_permit(query_id).unwrap();
        control.start_with_permit(super::now_ns(), permit).unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        let status = engine.status(owner.as_ref(), instance_id).await.unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(status.error.unwrap().code(), ErrorCode::DeadlineExceeded);
        assert_eq!(
            delivery
                .state
                .lock()
                .unwrap()
                .terminal_error
                .as_ref()
                .unwrap()
                .code(),
            ErrorCode::DeadlineExceeded
        );
        drop(token);
    }

    #[tokio::test]
    async fn retired_token_cannot_affect_reused_query_id() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig {
            plan_ttl: Duration::from_millis(5),
            sweep_interval: Duration::from_millis(2),
            terminal_retention: Duration::from_millis(5),
            ..LegacyQueryEngineConfig::default()
        });
        let session_id = SessionId::from_u128(50).unwrap();
        let owner = engine.owner(session_id);
        let old_token = engine
            .plan(owner.as_ref(), query_request(50, session_id))
            .await
            .unwrap();
        let old_instance = token_instance(&old_token);

        tokio::time::sleep(Duration::from_millis(30)).await;

        let new_token = engine
            .plan(owner.as_ref(), query_request(50, session_id))
            .await
            .unwrap();
        let new_instance = token_instance(&new_token);
        let error = engine.execute(old_token).await.err().unwrap();
        assert_eq!(error.code(), ErrorCode::NotFound);
        assert_eq!(
            engine
                .status(owner.as_ref(), old_instance)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            engine
                .cancel(owner.as_ref(), old_instance)
                .await
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            engine
                .status(owner.as_ref(), new_instance)
                .await
                .unwrap()
                .state,
            QueryState::Planned
        );
        drop(new_token);
    }

    #[tokio::test]
    async fn sql_is_rejected_without_registering_a_query() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(4).unwrap();
        let owner = engine.owner(session_id);
        let mut request = query_request(4, session_id);
        request.input = QueryInput::Sql("select * from ticks".to_string());
        let query_id = request.query_id;

        let error = engine.plan(owner.as_ref(), request).await.err().unwrap();

        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        let status_error = engine
            .status(owner.as_ref(), super::query_instance_id(query_id, 1))
            .await
            .unwrap_err();
        assert_eq!(status_error.code(), ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn shutdown_gate_rejects_new_plans_and_cancels_registered_work() {
        let (engine, _gateway) = test_engine(LegacyQueryEngineConfig::default());
        let session_id = SessionId::from_u128(60).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(60, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);

        engine.begin_shutdown();

        assert_eq!(
            engine
                .status(owner.as_ref(), instance_id)
                .await
                .unwrap()
                .state,
            QueryState::Cancelled
        );
        let error = engine
            .plan(owner.as_ref(), query_request(61, session_id))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert_eq!(engine.activate().unwrap_err().code(), ErrorCode::Conflict);
        drop(token);
    }

    #[tokio::test]
    async fn execution_streams_bounded_batches_without_materializing_the_table() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("legacy-query.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])) as ArrayRef],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&file_path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let (endpoint, server) = spawn_master_response(persisted_stream("ticks", &file_path));
        let (engine, _gateway) = test_engine_with_endpoint(endpoint);
        let session_id = SessionId::from_u128(5).unwrap();
        let owner = engine.owner(session_id);
        let request = query_request(5, session_id);
        let token = engine.plan(owner.as_ref(), request).await.unwrap();
        let instance_id = token_instance(&token);
        let mut stream = engine.execute(token).await.unwrap();
        assert_eq!(stream.instance_id(), instance_id);
        assert_eq!(stream.query_id(), instance_id.query_id());
        let mut values = Vec::new();

        while let Some(batch) = poll_fn(|context| stream.as_mut().poll_next(context))
            .await
            .transpose()
            .unwrap()
        {
            assert!(batch.num_rows() <= 128);
            values.extend_from_slice(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
            );
        }

        assert_eq!(values, vec![1_i64, 2, 3, 4, 5]);
        assert_eq!(
            engine
                .status(owner.as_ref(), instance_id)
                .await
                .unwrap()
                .state,
            QueryState::Finished
        );
        server.join().unwrap();
    }
}
