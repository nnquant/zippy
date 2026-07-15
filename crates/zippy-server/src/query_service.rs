//! Native query resource admission, lifecycle retention, and cancellation coordination.
//!
//! Stable identities, owner capabilities, one-time execution tokens, and snapshot guards remain
//! defined by `zippy-types`. This module only coordinates their process-local runtime ownership.
//! It is intentionally not wired to Gateway until a native physical planner exists.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use zippy_types::query::{
    OpaqueQueryExecutionToken, OpaqueQueryOwnerCapability, QueryExecutionToken, QueryInstanceId,
    QueryOwnerCapability, QueryPlanInfo, QueryState,
};
use zippy_types::storage::OpaqueQueryReadSnapshot;
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId};

static NEXT_COORDINATOR_NONCE: AtomicU64 = AtomicU64::new(1);

const DEFAULT_TERMINAL_RETENTION: Duration = Duration::from_secs(300);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_MAX_REGISTERED_QUERIES: usize = 4096;
const DEFAULT_MAX_QUERY_TOMBSTONES: usize = 65_536;
const QUERY_SWEEPER_THREAD_NAME: &str = "zippy-query-sweeper";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryServiceResourceConfig {
    pub max_active_queries: usize,
    pub max_active_tasks: usize,
    pub max_cpu_slots: u32,
    pub max_memory_bytes: u64,
}

impl Default for QueryServiceResourceConfig {
    fn default() -> Self {
        Self {
            max_active_queries: 64,
            max_active_tasks: 256,
            max_cpu_slots: 4,
            max_memory_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl QueryServiceResourceConfig {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_active_queries == 0
            || self.max_active_tasks == 0
            || self.max_cpu_slots == 0
            || self.max_memory_bytes == 0
        {
            return Err(invalid("query service resource limits must be positive"));
        }
        Ok(self)
    }
}

/// Bounded lifecycle retention for the process-local query registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryServiceRegistryConfig {
    pub sweep_interval: Duration,
    pub terminal_retention: Duration,
    pub max_registered_queries: usize,
    pub max_query_tombstones: usize,
}

impl Default for QueryServiceRegistryConfig {
    fn default() -> Self {
        Self {
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            terminal_retention: DEFAULT_TERMINAL_RETENTION,
            max_registered_queries: DEFAULT_MAX_REGISTERED_QUERIES,
            max_query_tombstones: DEFAULT_MAX_QUERY_TOMBSTONES,
        }
    }
}

impl QueryServiceRegistryConfig {
    pub(crate) fn validate(self, resources: QueryServiceResourceConfig) -> Result<Self> {
        if self.sweep_interval.is_zero()
            || self.terminal_retention.is_zero()
            || self.max_registered_queries == 0
            || self.max_query_tombstones == 0
        {
            return Err(invalid(
                "query service registry retention and capacities must be positive",
            ));
        }
        if self.max_registered_queries < resources.max_active_queries {
            return Err(invalid(
                "query registry capacity must cover active logical query admission",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryResourceBudget {
    pub max_active_tasks: usize,
    pub max_cpu_slots: u32,
    pub max_memory_bytes: u64,
}

impl QueryResourceBudget {
    fn validate(self, global: QueryServiceResourceConfig) -> Result<Self> {
        if self.max_active_tasks == 0 || self.max_cpu_slots == 0 || self.max_memory_bytes == 0 {
            return Err(invalid("per-query resource limits must be positive"));
        }
        if self.max_active_tasks > global.max_active_tasks
            || self.max_cpu_slots > global.max_cpu_slots
            || self.max_memory_bytes > global.max_memory_bytes
        {
            return Err(invalid(
                "per-query resource limits must not exceed global limits",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryTaskCharge {
    pub cpu_slots: NonZeroU32,
    pub memory_bytes: NonZeroU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryServiceLifecycle {
    Accepting,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryServiceStatus {
    pub lifecycle: QueryServiceLifecycle,
    pub registered_queries: usize,
    pub active_logical_queries: usize,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_memory_bytes: u64,
    pub active_snapshot_owners: usize,
    pub query_tombstones: usize,
    pub scheduled_deadlines: usize,
    pub sweeper_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRuntimeStatus {
    pub query_id: QueryId,
    pub generation: u64,
    pub owner_session_id: SessionId,
    pub state: QueryState,
    pub logical_admitted: bool,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_memory_bytes: u64,
    pub cancellation_requested: bool,
    pub finished_at_ns: Option<u64>,
    pub error: Option<ServerError>,
}

impl QueryRuntimeStatus {
    pub(crate) fn instance_id(&self) -> QueryInstanceId {
        QueryInstanceId::new(
            self.query_id,
            NonZeroU64::new(self.generation).expect("QueryService query generation is nonzero"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryTerminalSignal {
    pub query_id: QueryId,
    pub generation: u64,
    pub state: QueryState,
    pub error: Option<ServerError>,
}

pub(crate) struct QueryCancellation {
    query_id: QueryId,
    cancelled: Arc<AtomicBool>,
    receiver: watch::Receiver<Option<QueryTerminalSignal>>,
}

impl QueryCancellation {
    pub(crate) fn terminal_signal(&self) -> Option<QueryTerminalSignal> {
        self.receiver.borrow().clone()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_terminal(&mut self) -> Result<QueryTerminalSignal> {
        loop {
            if let Some(signal) = self.terminal_signal() {
                return Ok(signal);
            }
            if self.receiver.changed().await.is_err() {
                return Err(ServerError::new(
                    ErrorCode::Unavailable,
                    "query terminal notification channel closed",
                )
                .with_query_id(self.query_id)
                .with_retryable(false));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct QueryResourceCoordinator {
    inner: Arc<CoordinatorInner>,
    sweeper: Arc<QueryServiceSweeper>,
}

impl QueryResourceCoordinator {
    pub(crate) fn new(config: QueryServiceResourceConfig) -> Result<Self> {
        Self::with_registry_config(config, QueryServiceRegistryConfig::default())
    }

    pub(crate) fn with_registry_config(
        config: QueryServiceResourceConfig,
        registry_config: QueryServiceRegistryConfig,
    ) -> Result<Self> {
        let config = config.validate()?;
        let registry_config = registry_config.validate(config)?;
        let inner = Arc::new(CoordinatorInner {
            nonce: next_nonce(),
            config,
            terminal_retention_ns: duration_ns(registry_config.terminal_retention),
            max_registered_queries: registry_config.max_registered_queries,
            max_query_tombstones: registry_config.max_query_tombstones,
            next_query_generation: AtomicU64::new(1),
            next_owner_nonce: AtomicU64::new(1),
            state: Mutex::new(CoordinatorState::default()),
        });
        let sweeper = Arc::new(QueryServiceSweeper::start(
            &inner,
            registry_config.sweep_interval,
        )?);
        Ok(Self { inner, sweeper })
    }

    pub(crate) fn owner(&self, session_id: SessionId) -> OpaqueQueryOwnerCapability {
        Box::new(QueryServiceOwner {
            session_id,
            coordinator_nonce: self.inner.nonce,
            owner_nonce: self.inner.next_owner_nonce(),
        })
    }

    pub(crate) fn admin_handle(&self) -> QueryAdminHandle {
        QueryAdminHandle {
            coordinator: self.clone(),
        }
    }

    pub(crate) fn register_planned(
        &self,
        owner: &dyn QueryOwnerCapability,
        info: QueryPlanInfo,
        execution_deadline_ns: Option<u64>,
        budget: QueryResourceBudget,
        snapshot: OpaqueQueryReadSnapshot,
        now_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        let owner = self.validate_owner(owner)?;
        let query_id = info.query_id;
        let budget = budget.validate(self.inner.config)?;
        validate_plan_snapshot_binding(&info, snapshot.descriptor())?;
        if info.expires_at_ns <= now_ns
            || execution_deadline_ns.is_some_and(|deadline| deadline <= now_ns)
        {
            return Err(deadline(query_id, "query expired before plan registration"));
        }

        let generation = self.inner.next_query_generation();
        let (terminal_sender, _) = watch::channel(None);
        let control = Arc::new(QueryControl {
            owner: Arc::downgrade(&self.inner),
            query_id,
            generation,
            owner_session_id: owner.session_id,
            owner_nonce: owner.owner_nonce,
            budget,
            plan_expires_at_ns: info.expires_at_ns,
            execution_deadline_ns,
            cancelled: Arc::new(AtomicBool::new(false)),
            terminal_sender,
            state: Mutex::new(QueryControlState::default()),
        });
        let ownership;
        {
            let mut state = self.inner.state.lock().unwrap();
            state
                .registry
                .remove_expired_terminal(now_ns, self.inner.terminal_retention_ns);
            state.registry.remove_expired_tombstones(now_ns);
            if state.lifecycle != QueryServiceLifecycle::Accepting {
                return Err(unavailable(query_id));
            }
            if state.registry.is_query_id_reserved(query_id) {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "query identifier is already registered",
                )
                .with_query_id(query_id));
            }
            if state.active_logical_queries >= self.inner.config.max_active_queries {
                return Err(exhausted(
                    query_id,
                    "global logical query admission is exhausted",
                ));
            }
            if state.active_snapshot_owners >= self.inner.config.max_active_queries {
                return Err(exhausted(
                    query_id,
                    "global query snapshot owner admission is exhausted",
                ));
            }
            self.inner
                .prepare_registry_for_registration(&mut state.registry, now_ns, query_id)?;
            ownership = Arc::new(SharedSnapshotOwnership {
                owner: Arc::downgrade(&self.inner),
                snapshot: Mutex::new(Some(snapshot)),
            });
            state.active_snapshot_owners = state
                .active_snapshot_owners
                .checked_add(1)
                .ok_or_else(|| exhausted(query_id, "snapshot owner accounting is exhausted"))?;
            state.active_logical_queries += 1;
            state.registry.insert(
                query_id,
                RegistryEntry {
                    control: Arc::clone(&control),
                    snapshot_root: Some(Arc::clone(&ownership)),
                },
            );
        }
        let token: OpaqueQueryExecutionToken = Box::new(QueryServicePlanToken {
            coordinator_nonce: self.inner.nonce,
            info,
            owner_session_id: owner.session_id,
            control,
            ownership: Some(ownership),
            armed: true,
        });
        Ok(token)
    }

    pub(crate) fn begin_execution(
        &self,
        token: OpaqueQueryExecutionToken,
        now_ns: u64,
    ) -> Result<QueryExecutionScope> {
        let transferred = token.into_any();
        let mut transferred = transferred
            .downcast::<TransferredPlanToken>()
            .map_err(|_| invalid("query token was not issued by this QueryService"))?;
        let query_id = transferred.info.query_id;
        if transferred.coordinator_nonce != self.inner.nonce {
            return Err(
                invalid("query token belongs to another QueryService instance")
                    .with_query_id(query_id),
            );
        }
        self.inner.begin_execution(&transferred.control, now_ns)?;
        let ownership = transferred
            .ownership
            .take()
            .expect("armed transferred token retains snapshot ownership");
        transferred.armed = false;
        Ok(QueryExecutionScope {
            control: Arc::clone(&transferred.control),
            ownership: Some(ownership),
            armed: true,
        })
    }

    pub(crate) fn cancel_owned(
        &self,
        owner: &dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> Result<()> {
        let owner = self.validate_owner(owner)?;
        let control = self.inner.control_instance(instance_id)?;
        control.authorize(owner, instance_id.query_id())?;
        self.inner.terminate_control(
            &control,
            QueryState::Cancelled,
            ServerError::new(ErrorCode::Cancelled, "query cancelled by owner")
                .with_query_id(instance_id.query_id()),
        );
        Ok(())
    }

    pub(crate) fn sweep_expired(&self, now_ns: u64) -> usize {
        self.inner.sweep_expired(now_ns)
    }

    pub(crate) fn begin_shutdown(&self) -> QueryServiceStatus {
        self.inner.begin_shutdown();
        self.sweeper.stop_and_join();
        self.status()
    }

    pub(crate) fn status(&self) -> QueryServiceStatus {
        let mut status = self.inner.status();
        status.sweeper_running = self.sweeper.is_running();
        status
    }

    pub(crate) fn status_owned(
        &self,
        owner: &dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> Result<QueryRuntimeStatus> {
        let owner = self.validate_owner(owner)?;
        let control = self.inner.control_instance(instance_id)?;
        control.authorize(owner, instance_id.query_id())?;
        self.inner.query_status(&control)
    }

    #[cfg(test)]
    pub(crate) fn register_planned_for_session(
        &self,
        info: QueryPlanInfo,
        owner_session_id: SessionId,
        execution_deadline_ns: Option<u64>,
        budget: QueryResourceBudget,
        snapshot: OpaqueQueryReadSnapshot,
        now_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        let owner = self.owner(owner_session_id);
        self.register_planned(
            owner.as_ref(),
            info,
            execution_deadline_ns,
            budget,
            snapshot,
            now_ns,
        )
    }

    #[cfg(test)]
    pub(crate) fn cancel_current_for_test(&self, query_id: QueryId) -> Result<()> {
        let instance_id = self.inner.current_instance_for_test(query_id)?;
        self.admin_handle().cancel(instance_id)
    }

    #[cfg(test)]
    pub(crate) fn query_status_current_for_test(
        &self,
        query_id: QueryId,
    ) -> Result<QueryRuntimeStatus> {
        let instance_id = self.inner.current_instance_for_test(query_id)?;
        self.admin_handle().status(instance_id)
    }

    fn validate_owner<'a>(
        &self,
        owner: &'a dyn QueryOwnerCapability,
    ) -> Result<&'a QueryServiceOwner> {
        let owner = owner
            .as_any()
            .downcast_ref::<QueryServiceOwner>()
            .ok_or_else(|| permission_denied("query owner capability has a foreign type", None))?;
        if owner.coordinator_nonce != self.inner.nonce {
            return Err(permission_denied(
                "query owner capability belongs to another QueryService",
                None,
            ));
        }
        Ok(owner)
    }
}

#[derive(Clone)]
pub(crate) struct QueryAdminHandle {
    coordinator: QueryResourceCoordinator,
}

impl QueryAdminHandle {
    pub(crate) fn cancel(&self, instance_id: QueryInstanceId) -> Result<()> {
        let control = self.coordinator.inner.control_instance(instance_id)?;
        self.coordinator.inner.terminate_control(
            &control,
            QueryState::Cancelled,
            ServerError::new(ErrorCode::Cancelled, "query cancelled by administrator")
                .with_query_id(instance_id.query_id()),
        );
        Ok(())
    }

    pub(crate) fn status(&self, instance_id: QueryInstanceId) -> Result<QueryRuntimeStatus> {
        let control = self.coordinator.inner.control_instance(instance_id)?;
        self.coordinator.inner.query_status(&control)
    }
}

struct QueryServiceSweeper {
    signal: Arc<QuerySweeperSignal>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct QueryServiceOwner {
    session_id: SessionId,
    coordinator_nonce: u64,
    owner_nonce: u64,
}

impl QueryOwnerCapability for QueryServiceOwner {
    fn session_id(&self) -> SessionId {
        self.session_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct QuerySweeperSignal {
    stopping: Mutex<bool>,
    changed: Condvar,
}

impl QueryServiceSweeper {
    fn start(inner: &Arc<CoordinatorInner>, interval: Duration) -> Result<Self> {
        let signal = Arc::new(QuerySweeperSignal {
            stopping: Mutex::new(false),
            changed: Condvar::new(),
        });
        let worker_signal = Arc::clone(&signal);
        let weak_inner = Arc::downgrade(inner);
        let worker = thread::Builder::new()
            .name(QUERY_SWEEPER_THREAD_NAME.to_owned())
            .spawn(move || run_query_sweeper(weak_inner, worker_signal, interval))
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("failed to spawn query sweeper error=[{error}]"),
                )
                .with_retryable(false)
            })?;
        Ok(Self {
            signal,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn is_running(&self) -> bool {
        self.worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
    }

    fn stop_and_join(&self) {
        {
            let mut stopping = self
                .signal
                .stopping
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            *stopping = true;
            self.signal.changed.notify_all();
        }
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for QueryServiceSweeper {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

fn run_query_sweeper(
    inner: Weak<CoordinatorInner>,
    signal: Arc<QuerySweeperSignal>,
    interval: Duration,
) {
    loop {
        let stopping = signal
            .stopping
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (stopping, _) = signal
            .changed
            .wait_timeout_while(stopping, interval, |stopping| !*stopping)
            .unwrap_or_else(|error| error.into_inner());
        if *stopping {
            return;
        }
        drop(stopping);
        let Some(inner) = inner.upgrade() else {
            return;
        };
        inner.sweep_expired(now_ns());
    }
}

struct CoordinatorInner {
    nonce: u64,
    config: QueryServiceResourceConfig,
    terminal_retention_ns: u64,
    max_registered_queries: usize,
    max_query_tombstones: usize,
    next_query_generation: AtomicU64,
    next_owner_nonce: AtomicU64,
    state: Mutex<CoordinatorState>,
}

struct CoordinatorState {
    lifecycle: QueryServiceLifecycle,
    registry: QueryRegistry,
    active_logical_queries: usize,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_memory_bytes: u64,
    active_snapshot_owners: usize,
}

struct RegistryEntry {
    control: Arc<QueryControl>,
    snapshot_root: Option<Arc<SharedSnapshotOwnership>>,
}

type TerminalOrderKey = Reverse<(u64, QueryId, u64)>;
type TombstoneOrderKey = Reverse<(u64, QueryId, u64)>;
type DeadlineOrderKey = (u64, QueryId, u64, QueryDeadlineKind);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QueryDeadlineKind {
    PlanExpiry,
    ExecutionDeadline,
}

#[derive(Clone, Copy)]
struct QueryTombstone {
    expires_at_ns: u64,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapacityEviction {
    Evicted,
    NoTerminal,
    TombstoneCapacityExhausted,
}

#[derive(Default)]
struct QueryRegistry {
    by_id: HashMap<QueryId, RegistryEntry>,
    deadline_order: BTreeSet<DeadlineOrderKey>,
    drained_terminal_order: BinaryHeap<TerminalOrderKey>,
    tombstones: HashMap<QueryId, QueryTombstone>,
    tombstone_order: BinaryHeap<TombstoneOrderKey>,
}

impl QueryRegistry {
    fn get(&self, query_id: &QueryId) -> Option<&RegistryEntry> {
        self.by_id.get(query_id)
    }

    fn insert(&mut self, query_id: QueryId, entry: RegistryEntry) {
        debug_assert!(!self.tombstones.contains_key(&query_id));
        let control = &entry.control;
        let inserted_plan_deadline = self.deadline_order.insert((
            control.plan_expires_at_ns,
            query_id,
            control.generation,
            QueryDeadlineKind::PlanExpiry,
        ));
        debug_assert!(inserted_plan_deadline);
        if let Some(deadline_ns) = control.execution_deadline_ns {
            let inserted_execution_deadline = self.deadline_order.insert((
                deadline_ns,
                query_id,
                control.generation,
                QueryDeadlineKind::ExecutionDeadline,
            ));
            debug_assert!(inserted_execution_deadline);
        }
        let previous = self.by_id.insert(query_id, entry);
        debug_assert!(previous.is_none());
    }

    fn len(&self) -> usize {
        self.by_id.len()
    }

    fn controls(&self) -> Vec<Arc<QueryControl>> {
        self.by_id
            .values()
            .map(|entry| Arc::clone(&entry.control))
            .collect()
    }

    fn is_query_id_reserved(&self, query_id: QueryId) -> bool {
        self.by_id.contains_key(&query_id) || self.tombstones.contains_key(&query_id)
    }

    fn take_snapshot_root(
        &mut self,
        query_id: QueryId,
        generation: u64,
    ) -> Option<Arc<SharedSnapshotOwnership>> {
        self.by_id
            .get_mut(&query_id)
            .filter(|entry| entry.control.generation == generation)
            .and_then(|entry| entry.snapshot_root.take())
    }

    fn remove_plan_deadline(&mut self, control: &QueryControl) {
        self.deadline_order.remove(&(
            control.plan_expires_at_ns,
            control.query_id,
            control.generation,
            QueryDeadlineKind::PlanExpiry,
        ));
    }

    fn remove_deadlines(&mut self, control: &QueryControl) {
        self.remove_plan_deadline(control);
        if let Some(deadline_ns) = control.execution_deadline_ns {
            self.deadline_order.remove(&(
                deadline_ns,
                control.query_id,
                control.generation,
                QueryDeadlineKind::ExecutionDeadline,
            ));
        }
    }

    fn pop_due_deadline(&mut self, now_ns: u64) -> Option<DeadlineOrderKey> {
        let due = self.deadline_order.first().copied()?;
        if due.0 > now_ns {
            return None;
        }
        self.deadline_order.pop_first()
    }

    fn control_for_generation(
        &self,
        query_id: QueryId,
        generation: u64,
    ) -> Option<Arc<QueryControl>> {
        self.by_id
            .get(&query_id)
            .filter(|entry| entry.control.generation == generation)
            .map(|entry| Arc::clone(&entry.control))
    }

    fn record_drained_terminal(
        &mut self,
        query_id: QueryId,
        finished_at_ns: u64,
        generation: u64,
    ) -> bool {
        if self.is_current_generation(query_id, generation) {
            self.drained_terminal_order
                .push(Reverse((finished_at_ns, query_id, generation)));
            true
        } else {
            false
        }
    }

    fn remove_expired_terminal(&mut self, now_ns: u64, retention_ns: u64) {
        loop {
            self.discard_stale_terminal_entries();
            let Some(Reverse((finished_at_ns, query_id, generation))) =
                self.drained_terminal_order.peek().copied()
            else {
                return;
            };
            if now_ns.saturating_sub(finished_at_ns) < retention_ns {
                return;
            }
            if !self.is_current_drained_generation(query_id, generation) {
                return;
            }
            self.drained_terminal_order.pop();
            self.by_id.remove(&query_id);
        }
    }

    fn evict_oldest_terminal(
        &mut self,
        now_ns: u64,
        retention_ns: u64,
        max_tombstones: usize,
    ) -> CapacityEviction {
        self.discard_stale_terminal_entries();
        let Some(Reverse((finished_at_ns, query_id, generation))) =
            self.drained_terminal_order.peek().copied()
        else {
            return CapacityEviction::NoTerminal;
        };
        if !self.is_current_drained_generation(query_id, generation) {
            return CapacityEviction::NoTerminal;
        }
        self.remove_expired_tombstones(now_ns);
        if self.tombstones.len() >= max_tombstones {
            return CapacityEviction::TombstoneCapacityExhausted;
        }
        self.drained_terminal_order.pop();
        self.by_id.remove(&query_id);
        let expires_at_ns = finished_at_ns.saturating_add(retention_ns);
        if expires_at_ns > now_ns {
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
        CapacityEviction::Evicted
    }

    fn discard_stale_terminal_entries(&mut self) {
        while let Some(Reverse((_, query_id, generation))) =
            self.drained_terminal_order.peek().copied()
        {
            if self.is_current_generation(query_id, generation) {
                return;
            }
            self.drained_terminal_order.pop();
        }
    }

    fn is_current_generation(&self, query_id: QueryId, generation: u64) -> bool {
        self.by_id
            .get(&query_id)
            .is_some_and(|entry| entry.control.generation == generation)
    }

    fn is_current_drained_generation(&self, query_id: QueryId, generation: u64) -> bool {
        self.by_id.get(&query_id).is_some_and(|entry| {
            entry.control.generation == generation && entry.snapshot_root.is_none()
        })
    }

    fn remove_expired_tombstones(&mut self, now_ns: u64) {
        loop {
            self.discard_stale_tombstone_entries();
            let Some(Reverse((expires_at_ns, query_id, generation))) =
                self.tombstone_order.peek().copied()
            else {
                return;
            };
            if expires_at_ns > now_ns {
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

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            lifecycle: QueryServiceLifecycle::Accepting,
            registry: QueryRegistry::default(),
            active_logical_queries: 0,
            active_tasks: 0,
            active_cpu_slots: 0,
            active_memory_bytes: 0,
            active_snapshot_owners: 0,
        }
    }
}

impl CoordinatorState {
    fn take_snapshot_root(
        &mut self,
        query_id: QueryId,
        generation: u64,
    ) -> Option<Arc<SharedSnapshotOwnership>> {
        self.registry.take_snapshot_root(query_id, generation)
    }

    fn refresh_shutdown(&mut self) {
        if self.lifecycle == QueryServiceLifecycle::Draining
            && self.active_logical_queries == 0
            && self.active_tasks == 0
            && self.active_snapshot_owners == 0
        {
            self.lifecycle = QueryServiceLifecycle::Stopped;
        }
    }

    fn status(&self) -> QueryServiceStatus {
        QueryServiceStatus {
            lifecycle: self.lifecycle,
            registered_queries: self.registry.len(),
            active_logical_queries: self.active_logical_queries,
            active_tasks: self.active_tasks,
            active_cpu_slots: self.active_cpu_slots,
            active_memory_bytes: self.active_memory_bytes,
            active_snapshot_owners: self.active_snapshot_owners,
            query_tombstones: self.registry.tombstones.len(),
            scheduled_deadlines: self.registry.deadline_order.len(),
            sweeper_running: false,
        }
    }
}

impl CoordinatorInner {
    fn next_query_generation(&self) -> u64 {
        self.next_query_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .expect("QueryService query generation space exhausted")
    }

    fn next_owner_nonce(&self) -> u64 {
        self.next_owner_nonce
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |nonce| {
                nonce.checked_add(1)
            })
            .expect("QueryService owner nonce space exhausted")
    }

    fn control_instance(&self, instance_id: QueryInstanceId) -> Result<Arc<QueryControl>> {
        self.state
            .lock()
            .unwrap()
            .registry
            .control_for_generation(instance_id.query_id(), instance_id.generation().get())
            .ok_or_else(|| not_found(instance_id.query_id()))
    }

    #[cfg(test)]
    fn current_instance_for_test(&self, query_id: QueryId) -> Result<QueryInstanceId> {
        let control = self
            .state
            .lock()
            .unwrap()
            .registry
            .get(&query_id)
            .map(|entry| Arc::clone(&entry.control))
            .ok_or_else(|| not_found(query_id))?;
        Ok(QueryInstanceId::new(
            query_id,
            NonZeroU64::new(control.generation).expect("QueryService query generation is nonzero"),
        ))
    }

    fn prepare_registry_for_registration(
        &self,
        registry: &mut QueryRegistry,
        now_ns: u64,
        query_id: QueryId,
    ) -> Result<()> {
        if registry.len() < self.max_registered_queries {
            return Ok(());
        }
        registry.remove_expired_terminal(now_ns, self.terminal_retention_ns);
        if registry.len() < self.max_registered_queries {
            return Ok(());
        }
        match registry.evict_oldest_terminal(
            now_ns,
            self.terminal_retention_ns,
            self.max_query_tombstones,
        ) {
            CapacityEviction::Evicted => Ok(()),
            CapacityEviction::NoTerminal => Err(exhausted(
                query_id,
                format!(
                    "query registry is full with non-drained work limit=[{}]",
                    self.max_registered_queries
                ),
            )),
            CapacityEviction::TombstoneCapacityExhausted => Err(exhausted(
                query_id,
                format!(
                    concat!(
                        "query tombstone capacity is exhausted limit=[{}]; ",
                        "identity reuse remains blocked until retention expires"
                    ),
                    self.max_query_tombstones
                ),
            )),
        }
    }

    fn begin_execution(&self, control: &Arc<QueryControl>, now_ns: u64) -> Result<()> {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        if query.state.is_terminal() {
            return Err(query.error.clone().unwrap_or_else(|| {
                ServerError::new(ErrorCode::Conflict, "query is already terminal")
                    .with_query_id(control.query_id)
            }));
        }
        if global.lifecycle != QueryServiceLifecycle::Accepting {
            return Err(unavailable(control.query_id));
        }
        if control.plan_expires_at_ns <= now_ns
            || control
                .execution_deadline_ns
                .is_some_and(|deadline| deadline <= now_ns)
        {
            let error = deadline(control.query_id, "query plan expired before execution");
            let signal = Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::TimedOut,
                Some(error.clone()),
                now_ns,
            );
            let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
            drop(query);
            drop(global);
            control.publish_terminal(signal);
            release_snapshot_root(snapshot_root);
            return Err(error);
        }
        if query.state != QueryState::Planned {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!("query is not planned state=[{:?}]", query.state),
            )
            .with_query_id(control.query_id));
        }
        query.state = QueryState::Running;
        global.registry.remove_plan_deadline(control);
        Ok(())
    }

    fn admit_task(&self, control: &Arc<QueryControl>, charge: QueryTaskCharge) -> Result<()> {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        if query.state.is_terminal() {
            return Err(query.error.clone().unwrap_or_else(|| {
                ServerError::new(ErrorCode::Conflict, "query is already terminal")
                    .with_query_id(control.query_id)
            }));
        }
        if query.state != QueryState::Running {
            return Err(
                ServerError::new(ErrorCode::Conflict, "query is not running")
                    .with_query_id(control.query_id),
            );
        }
        if global.lifecycle != QueryServiceLifecycle::Accepting {
            return Err(unavailable(control.query_id));
        }

        let cpu_slots = charge.cpu_slots.get();
        let memory_bytes = charge.memory_bytes.get();
        let query_cpu_after = query.active_cpu_slots.checked_add(cpu_slots);
        let query_memory_after = query.active_memory_bytes.checked_add(memory_bytes);
        if query.active_tasks >= control.budget.max_active_tasks
            || query_cpu_after.is_none_or(|value| value > control.budget.max_cpu_slots)
            || query_memory_after.is_none_or(|value| value > control.budget.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "per-query task resources are exhausted",
            ));
        }
        let global_cpu_after = global.active_cpu_slots.checked_add(cpu_slots);
        let global_memory_after = global.active_memory_bytes.checked_add(memory_bytes);
        if global.active_tasks >= self.config.max_active_tasks
            || global_cpu_after.is_none_or(|value| value > self.config.max_cpu_slots)
            || global_memory_after.is_none_or(|value| value > self.config.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "global task resources are exhausted",
            ));
        }

        query.active_tasks += 1;
        query.active_cpu_slots = query_cpu_after.expect("validated per-query CPU addition");
        query.active_memory_bytes =
            query_memory_after.expect("validated per-query memory addition");
        global.active_tasks += 1;
        global.active_cpu_slots = global_cpu_after.expect("validated global CPU addition");
        global.active_memory_bytes = global_memory_after.expect("validated global memory addition");
        Ok(())
    }

    fn release_task(&self, control: &Arc<QueryControl>, charge: QueryTaskCharge) {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        let cpu_slots = charge.cpu_slots.get();
        let memory_bytes = charge.memory_bytes.get();
        debug_assert!(query.active_tasks > 0 && global.active_tasks > 0);
        query.active_tasks -= 1;
        query.active_cpu_slots -= cpu_slots;
        query.active_memory_bytes -= memory_bytes;
        global.active_tasks -= 1;
        global.active_cpu_slots -= cpu_slots;
        global.active_memory_bytes -= memory_bytes;
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        global.refresh_shutdown();
        drop(query);
        drop(global);
        release_snapshot_root(snapshot_root);
    }

    fn release_snapshot_owner(&self) {
        let mut global = self.state.lock().unwrap();
        debug_assert!(global.active_snapshot_owners > 0);
        global.active_snapshot_owners -= 1;
        global.refresh_shutdown();
    }

    fn finish(&self, control: &Arc<QueryControl>) -> Result<()> {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        if query.active_tasks != 0 {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "query cannot finish while physical tasks remain active",
            )
            .with_query_id(control.query_id));
        }
        if query.state.is_terminal() {
            return Err(query.error.clone().unwrap_or_else(|| {
                ServerError::new(ErrorCode::Conflict, "query is already terminal")
                    .with_query_id(control.query_id)
            }));
        }
        if query.state != QueryState::Running {
            return Err(
                ServerError::new(ErrorCode::Conflict, "query is not running")
                    .with_query_id(control.query_id),
            );
        }
        let signal = Self::terminate_locked(
            &mut global,
            control,
            &mut query,
            QueryState::Finished,
            None,
            now_ns(),
        );
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        drop(query);
        drop(global);
        control.publish_terminal(signal);
        release_snapshot_root(snapshot_root);
        Ok(())
    }

    fn terminate_control(
        &self,
        control: &Arc<QueryControl>,
        terminal: QueryState,
        error: ServerError,
    ) {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        let signal = Self::terminate_locked(
            &mut global,
            control,
            &mut query,
            terminal,
            Some(error),
            now_ns(),
        );
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        drop(query);
        drop(global);
        control.publish_terminal(signal);
        release_snapshot_root(snapshot_root);
    }

    fn terminate_locked(
        global: &mut CoordinatorState,
        control: &QueryControl,
        query: &mut QueryControlState,
        terminal: QueryState,
        error: Option<ServerError>,
        finished_at_ns: u64,
    ) -> Option<QueryTerminalSignal> {
        if query.state.is_terminal() {
            return None;
        }
        debug_assert!(terminal.is_terminal());
        debug_assert!(query.state.can_transition_to(terminal));
        debug_assert_eq!(terminal == QueryState::Finished, error.is_none());
        let signal = QueryTerminalSignal {
            query_id: control.query_id,
            generation: control.generation,
            state: terminal,
            error: error.clone(),
        };
        query.state = terminal;
        query.error = error;
        query.finished_at_ns = Some(finished_at_ns);
        global.registry.remove_deadlines(control);
        if query.logical_admitted {
            query.logical_admitted = false;
            debug_assert!(global.active_logical_queries > 0);
            global.active_logical_queries -= 1;
        }
        if terminal != QueryState::Finished {
            control.cancelled.store(true, Ordering::Release);
        }
        global.refresh_shutdown();
        Some(signal)
    }

    fn retire_terminal_if_drained(
        global: &mut CoordinatorState,
        control: &QueryControl,
        query: &mut QueryControlState,
    ) -> Option<Arc<SharedSnapshotOwnership>> {
        if !query.state.is_terminal() || query.active_tasks != 0 {
            return None;
        }
        let snapshot_root = global.take_snapshot_root(control.query_id, control.generation);
        if !query.retention_scheduled {
            let finished_at_ns = query
                .finished_at_ns
                .expect("terminal query records its completion time");
            query.retention_scheduled = global.registry.record_drained_terminal(
                control.query_id,
                finished_at_ns,
                control.generation,
            );
        }
        snapshot_root
    }

    fn sweep_expired(&self, now_ns: u64) -> usize {
        let mut global = self.state.lock().unwrap();
        let mut expired = 0;
        let mut snapshot_roots = Vec::new();
        let mut terminal_notifications = Vec::new();
        while let Some((_, query_id, generation, kind)) = global.registry.pop_due_deadline(now_ns) {
            let Some(control) = global.registry.control_for_generation(query_id, generation) else {
                continue;
            };
            let mut query = control.state.lock().unwrap();
            let due = match kind {
                QueryDeadlineKind::PlanExpiry => query.state == QueryState::Planned,
                QueryDeadlineKind::ExecutionDeadline => {
                    matches!(query.state, QueryState::Planned | QueryState::Running)
                }
            };
            if due {
                let error = deadline(control.query_id, "query exceeded its runtime deadline");
                let terminated = Self::terminate_locked(
                    &mut global,
                    &control,
                    &mut query,
                    QueryState::TimedOut,
                    Some(error),
                    now_ns,
                );
                expired += usize::from(terminated.is_some());
                if let Some(signal) = terminated {
                    terminal_notifications.push((Arc::clone(&control), signal));
                }
                if let Some(root) =
                    Self::retire_terminal_if_drained(&mut global, &control, &mut query)
                {
                    snapshot_roots.push(root);
                }
            }
        }
        global
            .registry
            .remove_expired_terminal(now_ns, self.terminal_retention_ns);
        global.registry.remove_expired_tombstones(now_ns);
        drop(global);
        for (control, signal) in terminal_notifications {
            control.publish_terminal(Some(signal));
        }
        for root in snapshot_roots {
            root.release();
        }
        expired
    }

    fn begin_shutdown(&self) {
        let mut global = self.state.lock().unwrap();
        if global.lifecycle == QueryServiceLifecycle::Accepting {
            global.lifecycle = QueryServiceLifecycle::Draining;
        }
        let controls = global.registry.controls();
        let mut snapshot_roots = Vec::new();
        let mut terminal_notifications = Vec::new();
        let finished_at_ns = now_ns();
        for control in controls {
            let mut query = control.state.lock().unwrap();
            let error = ServerError::new(
                ErrorCode::Cancelled,
                "query cancelled during QueryService shutdown",
            )
            .with_query_id(control.query_id);
            let signal = Self::terminate_locked(
                &mut global,
                &control,
                &mut query,
                QueryState::Cancelled,
                Some(error),
                finished_at_ns,
            );
            if let Some(signal) = signal {
                terminal_notifications.push((Arc::clone(&control), signal));
            }
            if let Some(root) = Self::retire_terminal_if_drained(&mut global, &control, &mut query)
            {
                snapshot_roots.push(root);
            }
        }
        global.refresh_shutdown();
        drop(global);
        for (control, signal) in terminal_notifications {
            control.publish_terminal(Some(signal));
        }
        for root in snapshot_roots {
            root.release();
        }
    }

    fn status(&self) -> QueryServiceStatus {
        self.state.lock().unwrap().status()
    }

    fn query_status(&self, control: &Arc<QueryControl>) -> Result<QueryRuntimeStatus> {
        let query = control.state.lock().unwrap();
        Ok(QueryRuntimeStatus {
            query_id: control.query_id,
            generation: control.generation,
            owner_session_id: control.owner_session_id,
            state: query.state,
            logical_admitted: query.logical_admitted,
            active_tasks: query.active_tasks,
            active_cpu_slots: query.active_cpu_slots,
            active_memory_bytes: query.active_memory_bytes,
            cancellation_requested: control.cancelled.load(Ordering::Acquire),
            finished_at_ns: query.finished_at_ns,
            error: query.error.clone(),
        })
    }
}

impl Drop for CoordinatorInner {
    fn drop(&mut self) {
        let mut terminal_notifications = Vec::new();
        let mut snapshot_roots = Vec::new();
        {
            let global = self
                .state
                .get_mut()
                .unwrap_or_else(|error| error.into_inner());
            let finished_at_ns = now_ns();
            global.lifecycle = QueryServiceLifecycle::Draining;
            for control in global.registry.controls() {
                let mut query = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let error =
                    ServerError::new(ErrorCode::Cancelled, "QueryService coordinator was dropped")
                        .with_query_id(control.query_id);
                if let Some(signal) = Self::terminate_locked(
                    global,
                    &control,
                    &mut query,
                    QueryState::Cancelled,
                    Some(error),
                    finished_at_ns,
                ) {
                    terminal_notifications.push((Arc::clone(&control), signal));
                }
                if let Some(root) = Self::retire_terminal_if_drained(global, &control, &mut query) {
                    snapshot_roots.push(root);
                }
            }
            global.lifecycle = QueryServiceLifecycle::Stopped;
        }
        for (control, signal) in terminal_notifications {
            control.publish_terminal(Some(signal));
        }
        for root in snapshot_roots {
            root.release();
        }
    }
}

struct QueryControl {
    owner: Weak<CoordinatorInner>,
    query_id: QueryId,
    generation: u64,
    owner_session_id: SessionId,
    owner_nonce: u64,
    budget: QueryResourceBudget,
    plan_expires_at_ns: u64,
    execution_deadline_ns: Option<u64>,
    cancelled: Arc<AtomicBool>,
    terminal_sender: watch::Sender<Option<QueryTerminalSignal>>,
    state: Mutex<QueryControlState>,
}

struct QueryControlState {
    state: QueryState,
    logical_admitted: bool,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_memory_bytes: u64,
    finished_at_ns: Option<u64>,
    retention_scheduled: bool,
    error: Option<ServerError>,
}

impl Default for QueryControlState {
    fn default() -> Self {
        Self {
            state: QueryState::Planned,
            logical_admitted: true,
            active_tasks: 0,
            active_cpu_slots: 0,
            active_memory_bytes: 0,
            finished_at_ns: None,
            retention_scheduled: false,
            error: None,
        }
    }
}

impl QueryControl {
    fn authorize(&self, owner: &QueryServiceOwner, query_id: QueryId) -> Result<()> {
        if self.owner_session_id != owner.session_id || self.owner_nonce != owner.owner_nonce {
            return Err(permission_denied(
                "query owner capability does not own the query instance",
                Some(query_id),
            ));
        }
        Ok(())
    }

    fn cancel(self: &Arc<Self>, reason: &'static str) {
        if let Some(owner) = self.owner.upgrade() {
            owner.terminate_control(
                self,
                QueryState::Cancelled,
                ServerError::new(ErrorCode::Cancelled, reason).with_query_id(self.query_id),
            );
        } else {
            self.cancelled.store(true, Ordering::Release);
        }
    }

    fn cancellation(&self) -> QueryCancellation {
        QueryCancellation {
            query_id: self.query_id,
            cancelled: Arc::clone(&self.cancelled),
            receiver: self.terminal_sender.subscribe(),
        }
    }

    fn publish_terminal(&self, signal: Option<QueryTerminalSignal>) {
        if let Some(signal) = signal {
            let previous = self.terminal_sender.send_replace(Some(signal));
            debug_assert!(
                previous.is_none(),
                "terminal notification is published once"
            );
        }
    }
}

struct SharedSnapshotOwnership {
    owner: Weak<CoordinatorInner>,
    snapshot: Mutex<Option<OpaqueQueryReadSnapshot>>,
}

impl SharedSnapshotOwnership {
    fn release(&self) {
        let snapshot = self
            .snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if snapshot.is_none() {
            return;
        }
        drop(snapshot);
        if let Some(owner) = self.owner.upgrade() {
            owner.release_snapshot_owner();
        }
    }
}

impl Drop for SharedSnapshotOwnership {
    fn drop(&mut self) {
        let snapshot = self
            .snapshot
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if snapshot.is_some() {
            drop(snapshot);
            if let Some(owner) = self.owner.upgrade() {
                owner.release_snapshot_owner();
            }
        }
    }
}

fn release_snapshot_root(root: Option<Arc<SharedSnapshotOwnership>>) {
    if let Some(root) = root {
        root.release();
    }
}

struct QueryServicePlanToken {
    coordinator_nonce: u64,
    info: QueryPlanInfo,
    owner_session_id: SessionId,
    control: Arc<QueryControl>,
    ownership: Option<Arc<SharedSnapshotOwnership>>,
    armed: bool,
}

impl QueryExecutionToken for QueryServicePlanToken {
    fn info(&self) -> &QueryPlanInfo {
        &self.info
    }

    fn owner_session_id(&self) -> SessionId {
        self.owner_session_id
    }

    fn instance_id(&self) -> QueryInstanceId {
        QueryInstanceId::new(
            self.info.query_id,
            NonZeroU64::new(self.control.generation)
                .expect("QueryService query generation is nonzero"),
        )
    }

    fn into_any(mut self: Box<Self>) -> Box<dyn Any + Send> {
        let transferred = TransferredPlanToken {
            coordinator_nonce: self.coordinator_nonce,
            info: self.info.clone(),
            control: Arc::clone(&self.control),
            ownership: self.ownership.take(),
            armed: true,
        };
        self.armed = false;
        Box::new(transferred)
    }
}

impl Drop for QueryServicePlanToken {
    fn drop(&mut self) {
        if self.armed {
            self.control
                .cancel("unexecuted QueryService plan token dropped");
        }
    }
}

struct TransferredPlanToken {
    coordinator_nonce: u64,
    info: QueryPlanInfo,
    control: Arc<QueryControl>,
    ownership: Option<Arc<SharedSnapshotOwnership>>,
    armed: bool,
}

impl Drop for TransferredPlanToken {
    fn drop(&mut self) {
        if self.armed {
            self.control
                .cancel("transferred QueryService plan token dropped");
        }
    }
}

pub(crate) struct QueryExecutionScope {
    control: Arc<QueryControl>,
    ownership: Option<Arc<SharedSnapshotOwnership>>,
    armed: bool,
}

impl QueryExecutionScope {
    pub(crate) fn query_id(&self) -> QueryId {
        self.control.query_id
    }

    /// Borrows the concrete snapshot capability while this execution scope owns it.
    ///
    /// The callback boundary prevents a reference from escaping the ownership lock. A physical
    /// planner may clone only lease-bound task capabilities from the concrete guard.
    fn with_snapshot<T, R>(&self, inspect: impl FnOnce(&T) -> Result<R>) -> Result<R>
    where
        T: Any + Send,
    {
        let ownership = self
            .ownership
            .as_ref()
            .ok_or_else(|| unavailable(self.control.query_id))?;
        let snapshot = ownership
            .snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let snapshot = snapshot.as_ref().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "QueryService execution scope lost snapshot ownership",
            )
            .with_query_id(self.control.query_id)
            .with_retryable(false)
        })?;
        let snapshot = snapshot.as_any().downcast_ref::<T>().ok_or_else(|| {
            invalid("query snapshot was not issued by the expected storage engine")
                .with_query_id(self.control.query_id)
        })?;
        inspect(snapshot)
    }

    /// Admits resources before allowing a lease-bound physical task to escape the snapshot guard.
    pub(crate) fn admit_snapshot_task<T, R>(
        &self,
        charge: QueryTaskCharge,
        build: impl FnOnce(&T) -> Result<R>,
    ) -> Result<QueryTaskPermit<R>>
    where
        T: Any + Send,
        R: Send + 'static,
    {
        let owner = self.control.owner.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "QueryService coordinator is unavailable",
            )
            .with_query_id(self.control.query_id)
        })?;
        owner.admit_task(&self.control, charge)?;
        let resource = match self.with_snapshot(build) {
            Ok(resource) => resource,
            Err(error) => {
                owner.release_task(&self.control, charge);
                return Err(error);
            }
        };
        Ok(QueryTaskPermit {
            owner: Arc::downgrade(&owner),
            control: Arc::clone(&self.control),
            ownership: self.ownership.as_ref().map(Arc::clone),
            charge,
            resource: Some(resource),
        })
    }

    fn admit_task<R: Send + 'static>(
        &self,
        charge: QueryTaskCharge,
        resource: R,
    ) -> Result<QueryTaskPermit<R>> {
        let owner = self.control.owner.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "QueryService coordinator is unavailable",
            )
            .with_query_id(self.control.query_id)
        })?;
        owner.admit_task(&self.control, charge)?;
        Ok(QueryTaskPermit {
            owner: Arc::downgrade(&owner),
            control: Arc::clone(&self.control),
            ownership: self.ownership.as_ref().map(Arc::clone),
            charge,
            resource: Some(resource),
        })
    }

    pub(crate) fn finish(mut self) -> Result<()> {
        let owner = self.control.owner.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "QueryService coordinator is unavailable",
            )
            .with_query_id(self.control.query_id)
        })?;
        owner.finish(&self.control)?;
        self.armed = false;
        drop(self.ownership.take());
        Ok(())
    }
}

impl Drop for QueryExecutionScope {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel("QueryService execution scope dropped");
        }
    }
}

pub(crate) struct QueryTaskPermit<R: Send + 'static> {
    owner: Weak<CoordinatorInner>,
    control: Arc<QueryControl>,
    ownership: Option<Arc<SharedSnapshotOwnership>>,
    charge: QueryTaskCharge,
    resource: Option<R>,
}

impl<R: Send + 'static> QueryTaskPermit<R> {
    #[cfg(test)]
    pub(crate) fn resource(&self) -> &R {
        self.resource
            .as_ref()
            .expect("live task permit retains its physical resource")
    }

    #[cfg(test)]
    pub(crate) fn resource_mut(&mut self) -> &mut R {
        self.resource
            .as_mut()
            .expect("live task permit retains its physical resource")
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.control.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.control.cancellation()
    }
}

impl<R: Send + 'static> Drop for QueryTaskPermit<R> {
    fn drop(&mut self) {
        drop(self.resource.take());
        drop(self.ownership.take());
        if let Some(owner) = self.owner.upgrade() {
            owner.release_task(&self.control, self.charge);
        }
    }
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

fn next_nonce() -> u64 {
    NEXT_COORDINATOR_NONCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |nonce| {
            nonce.checked_add(1)
        })
        .expect("QueryService coordinator nonce space exhausted")
}

fn validate_plan_snapshot_binding(
    info: &QueryPlanInfo,
    snapshot: &zippy_types::storage::QueryReadSnapshotDescriptor,
) -> Result<()> {
    if snapshot.query_id != info.query_id {
        return Err(
            invalid("query snapshot owner belongs to another query").with_query_id(info.query_id)
        );
    }
    if snapshot.catalog_revision != info.catalog_revision {
        return Err(
            invalid("query plan catalog revision does not match snapshot owner")
                .with_query_id(info.query_id),
        );
    }
    if snapshot.tables.len() != info.snapshots.len()
        || snapshot
            .tables
            .iter()
            .zip(&info.snapshots)
            .any(|(actual, planned)| {
                actual.table_id != planned.table_id
                    || actual.persisted_snapshot_version != planned.snapshot_version
                    || actual.schema_version != planned.schema_version
            })
    {
        return Err(
            invalid("query plan table versions do not match snapshot owner")
                .with_query_id(info.query_id),
        );
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unavailable(query_id: QueryId) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, "QueryService admission is closed")
        .with_query_id(query_id)
        .with_retryable(false)
}

fn exhausted(query_id: QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
        .with_query_id(query_id)
        .with_retryable(true)
}

fn deadline(query_id: QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DeadlineExceeded, message)
        .with_query_id(query_id)
        .with_retryable(false)
}

fn not_found(query_id: QueryId) -> ServerError {
    ServerError::new(ErrorCode::NotFound, "query is not registered")
        .with_query_id(query_id)
        .with_retryable(false)
}

fn permission_denied(message: impl Into<String>, query_id: Option<QueryId>) -> ServerError {
    let error = ServerError::new(ErrorCode::PermissionDenied, message).with_retryable(false);
    match query_id {
        Some(query_id) => error.with_query_id(query_id),
        None => error,
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use zippy_types::query::QueryOwnerCapability;
    use zippy_types::storage::{QueryReadSnapshotDescriptor, QueryReadSnapshotGuard};
    use zippy_types::{BoxFuture, ErrorCode, QueryId, Result, SessionId};

    use super::{
        QueryResourceBudget, QueryResourceCoordinator, QueryServiceLifecycle,
        QueryServiceRegistryConfig, QueryServiceResourceConfig, QueryState, QueryTaskCharge,
    };

    struct TestSnapshot {
        descriptor: QueryReadSnapshotDescriptor,
        drops: Arc<AtomicUsize>,
    }

    impl QueryReadSnapshotGuard for TestSnapshot {
        fn descriptor(&self) -> &QueryReadSnapshotDescriptor {
            &self.descriptor
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn release(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                drop(self);
                Ok(())
            })
        }

        fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }

    impl Drop for TestSnapshot {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct ForeignOwner(SessionId);

    impl QueryOwnerCapability for ForeignOwner {
        fn session_id(&self) -> SessionId {
            self.0
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn query_id(value: u128) -> QueryId {
        QueryId::from_u128(value).unwrap()
    }

    fn session_id(value: u128) -> SessionId {
        SessionId::from_u128(value).unwrap()
    }

    fn config() -> QueryServiceResourceConfig {
        QueryServiceResourceConfig {
            max_active_queries: 1,
            max_active_tasks: 2,
            max_cpu_slots: 4,
            max_memory_bytes: 100,
        }
    }

    fn budget() -> QueryResourceBudget {
        QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 2,
            max_memory_bytes: 80,
        }
    }

    fn registry_config(
        max_registered_queries: usize,
        max_query_tombstones: usize,
        terminal_retention: Duration,
    ) -> QueryServiceRegistryConfig {
        QueryServiceRegistryConfig {
            sweep_interval: Duration::from_secs(3600),
            terminal_retention,
            max_registered_queries,
            max_query_tombstones,
        }
    }

    fn coordinator(resources: QueryServiceResourceConfig) -> QueryResourceCoordinator {
        QueryResourceCoordinator::with_registry_config(
            resources,
            QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                ..QueryServiceRegistryConfig::default()
            },
        )
        .unwrap()
    }

    fn info(query_id: QueryId, expires_at_ns: u64) -> zippy_types::query::QueryPlanInfo {
        zippy_types::query::QueryPlanInfo {
            query_id,
            catalog_revision: 1,
            snapshots: Vec::new(),
            planned_at_ns: 10,
            expires_at_ns,
        }
    }

    fn snapshot(query_id: QueryId, drops: Arc<AtomicUsize>) -> Box<TestSnapshot> {
        snapshot_at_revision(query_id, 1, drops)
    }

    fn snapshot_at_revision(
        query_id: QueryId,
        catalog_revision: u64,
        drops: Arc<AtomicUsize>,
    ) -> Box<TestSnapshot> {
        Box::new(TestSnapshot {
            descriptor: QueryReadSnapshotDescriptor {
                query_id,
                catalog_revision,
                tables: Vec::new(),
            },
            drops,
        })
    }

    fn charge(cpu_slots: u32, memory_bytes: u64) -> QueryTaskCharge {
        QueryTaskCharge {
            cpu_slots: NonZeroU32::new(cpu_slots).unwrap(),
            memory_bytes: NonZeroU64::new(memory_bytes).unwrap(),
        }
    }

    #[test]
    fn owner_and_admin_capabilities_require_the_exact_query_instance() {
        let foreign_coordinator = coordinator(config());
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let same_session_other_owner = coordinator.owner(session_id(1));
        let other_service_owner = foreign_coordinator.owner(session_id(1));
        let foreign_type = ForeignOwner(session_id(1));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
                owner.as_ref(),
                info(id, 100),
                None,
                budget(),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let instance_id = token.instance_id();

        for denied_owner in [
            same_session_other_owner.as_ref(),
            other_service_owner.as_ref(),
            &foreign_type as &dyn QueryOwnerCapability,
        ] {
            assert_eq!(
                coordinator
                    .status_owned(denied_owner, instance_id)
                    .unwrap_err()
                    .code(),
                ErrorCode::PermissionDenied
            );
        }

        let owner_status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(owner_status.instance_id(), instance_id);
        assert_eq!(owner_status.owner_session_id, session_id(1));
        assert_eq!(
            coordinator.admin_handle().status(instance_id).unwrap(),
            owner_status
        );
        coordinator
            .cancel_owned(owner.as_ref(), instance_id)
            .unwrap();
        coordinator
            .cancel_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), instance_id)
                .unwrap()
                .state,
            QueryState::Cancelled
        );
        drop(token);
    }

    #[test]
    fn stale_owner_and_admin_instance_cannot_cancel_reused_query_id() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 8, Duration::from_nanos(1)),
        )
        .unwrap();
        let owner = coordinator.owner(session_id(1));
        let admin = coordinator.admin_handle();
        let id = query_id(1);
        let now = super::now_ns();
        let expires_at_ns = now.saturating_add(60_000_000_000);
        let old_drops = Arc::new(AtomicUsize::new(0));
        let old_token = coordinator
            .register_planned(
                owner.as_ref(),
                info(id, expires_at_ns),
                None,
                budget(),
                snapshot(id, Arc::clone(&old_drops)),
                now,
            )
            .unwrap();
        let old_instance = old_token.instance_id();
        coordinator
            .cancel_owned(owner.as_ref(), old_instance)
            .unwrap();
        assert_eq!(old_drops.load(Ordering::Relaxed), 1);
        let reuse_at = super::now_ns().saturating_add(2);
        assert_eq!(coordinator.sweep_expired(reuse_at), 0);

        let new_drops = Arc::new(AtomicUsize::new(0));
        let new_token = coordinator
            .register_planned(
                owner.as_ref(),
                info(id, expires_at_ns),
                None,
                budget(),
                snapshot(id, Arc::clone(&new_drops)),
                reuse_at,
            )
            .unwrap();
        let new_instance = new_token.instance_id();
        assert_ne!(old_instance, new_instance);

        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), old_instance)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            coordinator
                .cancel_owned(owner.as_ref(), old_instance)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            admin.cancel(old_instance).unwrap_err().code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            admin.status(old_instance).unwrap_err().code(),
            ErrorCode::NotFound
        );
        assert_eq!(
            admin.status(new_instance).unwrap().state,
            QueryState::Planned
        );
        assert_eq!(new_drops.load(Ordering::Relaxed), 0);
        assert_eq!(coordinator.status().active_logical_queries, 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        drop(old_token);
        assert_eq!(
            admin.status(new_instance).unwrap().state,
            QueryState::Planned
        );
        drop(new_token);
        assert_eq!(new_drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn registry_retains_revocable_snapshot_root_and_plan_drop_releases_once() {
        let coordinator = coordinator(config());
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&drops)),
                10,
            )
            .unwrap();
        {
            let state = coordinator.inner.state.lock().unwrap();
            let entry = state.registry.get(&id).unwrap();
            assert_eq!(Arc::strong_count(&entry.control), 2);
            assert!(entry.snapshot_root.is_some());
        }
        drop(token);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_logical_queries, 0);
        assert_eq!(coordinator.status().registered_queries, 1);
        assert_eq!(
            coordinator.query_status_current_for_test(id).unwrap().state,
            zippy_types::query::QueryState::Cancelled
        );
        assert_eq!(coordinator.sweep_expired(u64::MAX), 0);
        assert_eq!(coordinator.status().registered_queries, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn rejected_registration_does_not_decrement_or_increment_admitted_ownership() {
        let coordinator = coordinator(config());
        let admitted_drops = Arc::new(AtomicUsize::new(0));
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&admitted_drops)),
                10,
            )
            .unwrap();

        let duplicate = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&rejected_drops)),
                10,
            )
            .err()
            .expect("duplicate query must be rejected");
        assert_eq!(duplicate.code(), ErrorCode::Conflict);
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);
        let status = coordinator.status();
        assert_eq!(status.active_logical_queries, 1);
        assert_eq!(status.active_snapshot_owners, 1);

        let mismatched_drops = Arc::new(AtomicUsize::new(0));
        let mismatch = coordinator
            .register_planned_for_session(
                info(query_id(2), 100),
                session_id(1),
                None,
                budget(),
                snapshot_at_revision(query_id(2), 2, Arc::clone(&mismatched_drops)),
                10,
            )
            .err()
            .expect("catalog revision mismatch must be rejected");
        assert_eq!(mismatch.code(), ErrorCode::InvalidArgument);
        assert_eq!(mismatched_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_snapshot_owners, 1);

        let capacity_drops = Arc::new(AtomicUsize::new(0));
        let capacity = coordinator
            .register_planned_for_session(
                info(query_id(2), 100),
                session_id(1),
                None,
                budget(),
                snapshot(query_id(2), Arc::clone(&capacity_drops)),
                10,
            )
            .err()
            .expect("logical query capacity must reject excess admission");
        assert_eq!(capacity.code(), ErrorCode::ResourceExhausted);
        assert_eq!(capacity_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_snapshot_owners, 1);

        drop(token);
        assert_eq!(admitted_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
    }

    #[test]
    fn task_admission_enforces_per_query_and_global_cpu_memory_concurrency() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes: 100,
        });
        let full_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 2,
            max_memory_bytes: 100,
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let first_id = query_id(1);
        let second_id = query_id(2);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, 100),
                session_id(1),
                None,
                full_budget,
                snapshot(first_id, Arc::clone(&drops)),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned_for_session(
                info(second_id, 100),
                session_id(1),
                None,
                full_budget,
                snapshot(second_id, Arc::clone(&drops)),
                10,
            )
            .unwrap();
        let first = coordinator.begin_execution(first, 11).unwrap();
        let second = coordinator.begin_execution(second, 11).unwrap();
        let mut first_task = first
            .admit_task(charge(1, 80), String::from("physical-reader"))
            .unwrap();
        first_task.resource_mut().push_str("-active");
        assert_eq!(first_task.resource(), "physical-reader-active");

        let per_query = first
            .admit_task(charge(1, 1), ())
            .err()
            .expect("per-query task concurrency must be bounded");
        assert_eq!(per_query.code(), ErrorCode::ResourceExhausted);
        let global_memory = second
            .admit_task(charge(1, 30), ())
            .err()
            .expect("global memory must remain reserved");
        assert_eq!(global_memory.code(), ErrorCode::ResourceExhausted);
        let global_cpu = second
            .admit_task(charge(2, 20), ())
            .err()
            .expect("global CPU slots must remain reserved");
        assert_eq!(global_cpu.code(), ErrorCode::ResourceExhausted);
        let second_task = second.admit_task(charge(1, 20), ()).unwrap();
        let status = coordinator.status();
        assert_eq!(status.active_tasks, 2);
        assert_eq!(status.active_cpu_slots, 2);
        assert_eq!(status.active_memory_bytes, 100);

        drop((first_task, second_task));
        drop((first, second));
        assert_eq!(coordinator.status().active_tasks, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn cancellation_releases_logical_slot_but_retains_physical_resources() {
        let coordinator = coordinator(config());
        let snapshot_drops = Arc::new(AtomicUsize::new(0));
        let resource_drops = Arc::new(AtomicUsize::new(0));
        let replacement_drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&snapshot_drops)),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let task = execution
            .admit_task(charge(2, 80), DropProbe(Arc::clone(&resource_drops)))
            .unwrap();

        coordinator.cancel_current_for_test(id).unwrap();
        let status = coordinator.status();
        assert_eq!(status.active_logical_queries, 0);
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_cpu_slots, 2);
        assert_eq!(status.active_memory_bytes, 80);
        assert!(task.is_cancelled());
        let replacement_id = query_id(2);
        let blocked_replacement = coordinator
            .register_planned_for_session(
                info(replacement_id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(replacement_id, Arc::clone(&replacement_drops)),
                10,
            )
            .err()
            .expect("retained snapshot owner must bound replacement admission");
        assert_eq!(blocked_replacement.code(), ErrorCode::ResourceExhausted);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 1);
        drop(execution);
        assert_eq!(snapshot_drops.load(Ordering::Relaxed), 0);
        assert_eq!(resource_drops.load(Ordering::Relaxed), 0);

        drop(task);
        assert_eq!(snapshot_drops.load(Ordering::Relaxed), 1);
        assert_eq!(resource_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_tasks, 0);
        let replacement = coordinator
            .register_planned_for_session(
                info(replacement_id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(replacement_id, Arc::clone(&replacement_drops)),
                10,
            )
            .expect("released snapshot owner restores replacement admission");
        drop(replacement);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn ttl_revokes_unexecuted_snapshot_even_when_plan_token_is_retained() {
        let coordinator = coordinator(config());
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 20),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&drops)),
                10,
            )
            .unwrap();

        assert_eq!(coordinator.sweep_expired(20), 1);
        assert_eq!(
            coordinator.query_status_current_for_test(id).unwrap().state,
            zippy_types::query::QueryState::TimedOut
        );
        assert_eq!(coordinator.status().active_logical_queries, 0);
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        drop(token);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.sweep_expired(30), 0);
    }

    #[test]
    fn deadline_index_processes_only_due_running_generation() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            max_active_tasks: 2,
            max_cpu_slots: 4,
            max_memory_bytes: 200,
        });
        let first_id = query_id(1);
        let second_id = query_id(2);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, 100),
                session_id(1),
                Some(20),
                budget(),
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned_for_session(
                info(second_id, 100),
                session_id(1),
                Some(30),
                budget(),
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let first = coordinator.begin_execution(first, 11).unwrap();
        let second = coordinator.begin_execution(second, 11).unwrap();
        assert_eq!(coordinator.status().scheduled_deadlines, 2);

        assert_eq!(coordinator.sweep_expired(19), 0);
        assert_eq!(coordinator.sweep_expired(20), 1);
        assert_eq!(
            coordinator
                .query_status_current_for_test(first_id)
                .unwrap()
                .state,
            QueryState::TimedOut
        );
        assert_eq!(
            coordinator
                .query_status_current_for_test(second_id)
                .unwrap()
                .state,
            QueryState::Running
        );
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        assert_eq!(coordinator.sweep_expired(30), 1);
        assert_eq!(
            coordinator
                .query_status_current_for_test(second_id)
                .unwrap()
                .state,
            QueryState::TimedOut
        );
        assert_eq!(coordinator.status().scheduled_deadlines, 0);
        drop((first, second));
    }

    #[test]
    fn managed_sweeper_times_out_plan_and_joins_during_shutdown() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            QueryServiceRegistryConfig {
                sweep_interval: Duration::from_millis(5),
                terminal_retention: Duration::from_secs(30),
                max_registered_queries: 1,
                max_query_tombstones: 8,
            },
        )
        .unwrap();
        let now = super::now_ns();
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
        let token = coordinator
            .register_planned_for_session(
                info(id, now.saturating_add(20_000_000)),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&drops)),
                now,
            )
            .unwrap();
        assert!(coordinator.status().sweeper_running);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        let wait_deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if coordinator.query_status_current_for_test(id).unwrap().state == QueryState::TimedOut
            {
                break;
            }
            assert!(
                Instant::now() < wait_deadline,
                "managed sweeper did not expire plan"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 0);

        let stopped = coordinator.begin_shutdown();
        assert_eq!(stopped.lifecycle, QueryServiceLifecycle::Stopped);
        assert!(!stopped.sweeper_running);
        drop(token);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancellation_actively_wakes_waiting_physical_task() {
        let coordinator = coordinator(config());
        let now = super::now_ns();
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, now.saturating_add(60_000_000_000)),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                now,
            )
            .unwrap();
        let execution = coordinator
            .begin_execution(token, now.saturating_add(1))
            .unwrap();
        let task = execution.admit_task(charge(1, 1), ()).unwrap();
        let mut cancellation = task.cancellation();
        assert!(!cancellation.is_cancelled());
        let waiter = tokio::spawn(async move { cancellation.wait_terminal().await });
        tokio::task::yield_now().await;

        coordinator.cancel_current_for_test(id).unwrap();
        let signal = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("terminal notification must wake without polling")
            .expect("cancellation waiter task must not panic")
            .expect("terminal channel must remain available");
        assert_eq!(signal.query_id, id);
        assert_eq!(signal.state, QueryState::Cancelled);
        assert_eq!(signal.error.unwrap().code(), ErrorCode::Cancelled);
        assert!(task.is_cancelled());
        assert!(task.cancellation().is_cancelled());

        coordinator.cancel_current_for_test(id).unwrap();
        assert_eq!(
            coordinator.query_status_current_for_test(id).unwrap().state,
            QueryState::Cancelled
        );
        drop((execution, task));
        assert_eq!(
            coordinator.begin_shutdown().lifecycle,
            QueryServiceLifecycle::Stopped
        );
    }

    #[test]
    fn capacity_eviction_retains_query_id_tombstone_until_original_deadline() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 8, Duration::from_secs(1)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(10_000_000_000);
        let first_id = query_id(1);
        let second_id = query_id(2);

        let first = coordinator
            .register_planned_for_session(
                info(first_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                now,
            )
            .unwrap();
        drop(first);
        let second = coordinator
            .register_planned_for_session(
                info(second_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                super::now_ns(),
            )
            .unwrap();
        assert_eq!(coordinator.status().registered_queries, 1);
        assert_eq!(coordinator.status().query_tombstones, 1);

        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let reserved = coordinator
            .register_planned_for_session(
                info(first_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(first_id, Arc::clone(&rejected_drops)),
                super::now_ns(),
            )
            .err()
            .expect("early eviction must reserve the original query identifier");
        assert_eq!(reserved.code(), ErrorCode::Conflict);
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);

        drop(second);
        let after_retention = now.saturating_add(2_000_000_000);
        assert_eq!(coordinator.sweep_expired(after_retention), 0);
        assert_eq!(coordinator.status().registered_queries, 0);
        assert_eq!(coordinator.status().query_tombstones, 0);
        let reused = coordinator
            .register_planned_for_session(
                info(first_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                after_retention,
            )
            .expect("query identifier becomes reusable after its full retention window");
        drop(reused);
    }

    #[test]
    fn tombstone_capacity_exhaustion_fails_closed() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 1, Duration::from_secs(30)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(60_000_000_000);

        let first_id = query_id(1);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                now,
            )
            .unwrap();
        drop(first);

        let second_id = query_id(2);
        let second = coordinator
            .register_planned_for_session(
                info(second_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                super::now_ns(),
            )
            .unwrap();
        drop(second);

        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let third_id = query_id(3);
        let exhausted = coordinator
            .register_planned_for_session(
                info(third_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(third_id, Arc::clone(&rejected_drops)),
                super::now_ns(),
            )
            .err()
            .expect("unsafe identity eviction must fail when tombstones are full");
        assert_eq!(exhausted.code(), ErrorCode::ResourceExhausted);
        assert!(exhausted.message().contains("tombstone capacity"));
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().registered_queries, 1);
        assert_eq!(coordinator.status().query_tombstones, 1);
        assert_eq!(
            coordinator
                .query_status_current_for_test(second_id)
                .unwrap()
                .state,
            zippy_types::query::QueryState::Cancelled
        );
    }

    #[test]
    fn terminal_churn_keeps_registry_and_order_heaps_bounded() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(2, 16, Duration::from_secs(30)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(60_000_000_000);

        for value in 1..=10 {
            let id = query_id(value);
            let token = coordinator
                .register_planned_for_session(
                    info(id, expires_at),
                    session_id(1),
                    None,
                    budget(),
                    snapshot(id, Arc::new(AtomicUsize::new(0))),
                    super::now_ns(),
                )
                .unwrap();
            drop(token);
        }

        let state = coordinator.inner.state.lock().unwrap();
        assert_eq!(state.registry.by_id.len(), 2);
        assert_eq!(state.registry.deadline_order.len(), 0);
        assert_eq!(state.registry.drained_terminal_order.len(), 2);
        assert_eq!(state.registry.tombstones.len(), 8);
        assert_eq!(state.registry.tombstone_order.len(), 8);
    }

    #[test]
    fn non_drained_terminal_query_is_not_capacity_evicted() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 8, Duration::from_secs(30)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(60_000_000_000);
        let first_id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(first_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                now,
            )
            .unwrap();
        let execution = coordinator
            .begin_execution(token, now.saturating_add(1))
            .unwrap();
        let task = execution.admit_task(charge(1, 1), ()).unwrap();
        coordinator.cancel_current_for_test(first_id).unwrap();
        drop(execution);
        assert_eq!(coordinator.status().registered_queries, 1);
        assert_eq!(coordinator.status().query_tombstones, 0);

        let blocked_id = query_id(2);
        let blocked = coordinator
            .register_planned_for_session(
                info(blocked_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(blocked_id, Arc::new(AtomicUsize::new(0))),
                super::now_ns(),
            )
            .err()
            .expect("a non-drained physical task must keep replacement admission bounded");
        assert_eq!(blocked.code(), ErrorCode::ResourceExhausted);
        assert_eq!(coordinator.status().query_tombstones, 0);

        drop(task);
        let replacement = coordinator
            .register_planned_for_session(
                info(blocked_id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(blocked_id, Arc::new(AtomicUsize::new(0))),
                super::now_ns(),
            )
            .expect("drained terminal work becomes safely capacity-evictable");
        assert_eq!(coordinator.status().registered_queries, 1);
        assert_eq!(coordinator.status().query_tombstones, 1);
        drop(replacement);
    }

    #[test]
    fn old_plan_token_cannot_release_reused_query_generation() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 8, Duration::from_secs(1)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(10_000_000_000);
        let id = query_id(1);
        let old_drops = Arc::new(AtomicUsize::new(0));
        let old_token = coordinator
            .register_planned_for_session(
                info(id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&old_drops)),
                now,
            )
            .unwrap();
        let old_generation = coordinator
            .query_status_current_for_test(id)
            .unwrap()
            .generation;
        coordinator.cancel_current_for_test(id).unwrap();
        assert_eq!(old_drops.load(Ordering::Relaxed), 1);

        let reuse_at = now.saturating_add(2_000_000_000);
        assert_eq!(coordinator.status().registered_queries, 1);
        let new_drops = Arc::new(AtomicUsize::new(0));
        let new_token = coordinator
            .register_planned_for_session(
                info(id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&new_drops)),
                reuse_at,
            )
            .unwrap();
        let new_status = coordinator.query_status_current_for_test(id).unwrap();
        assert!(new_status.generation > old_generation);
        assert_eq!(new_status.state, zippy_types::query::QueryState::Planned);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        drop(old_token);
        let unaffected = coordinator.query_status_current_for_test(id).unwrap();
        assert_eq!(unaffected.generation, new_status.generation);
        assert_eq!(unaffected.state, zippy_types::query::QueryState::Planned);
        assert_eq!(new_drops.load(Ordering::Relaxed), 0);
        assert_eq!(coordinator.status().active_logical_queries, 1);
        assert_eq!(coordinator.status().active_snapshot_owners, 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        drop(new_token);
        assert_eq!(new_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_logical_queries, 0);
        assert_eq!(coordinator.status().scheduled_deadlines, 0);
    }

    #[test]
    fn old_execution_scope_cannot_cancel_reused_query_generation() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 8, Duration::from_secs(1)),
        )
        .unwrap();
        let now = super::now_ns();
        let expires_at = now.saturating_add(10_000_000_000);
        let id = query_id(1);
        let old_drops = Arc::new(AtomicUsize::new(0));
        let old_token = coordinator
            .register_planned_for_session(
                info(id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&old_drops)),
                now,
            )
            .unwrap();
        let old_execution = coordinator
            .begin_execution(old_token, now.saturating_add(1))
            .unwrap();
        let old_generation = coordinator
            .query_status_current_for_test(id)
            .unwrap()
            .generation;
        coordinator.cancel_current_for_test(id).unwrap();
        assert_eq!(old_drops.load(Ordering::Relaxed), 1);

        let reuse_at = now.saturating_add(2_000_000_000);
        let new_drops = Arc::new(AtomicUsize::new(0));
        let new_token = coordinator
            .register_planned_for_session(
                info(id, expires_at),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&new_drops)),
                reuse_at,
            )
            .unwrap();
        let new_generation = coordinator
            .query_status_current_for_test(id)
            .unwrap()
            .generation;
        assert!(new_generation > old_generation);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        drop(old_execution);
        let unaffected = coordinator.query_status_current_for_test(id).unwrap();
        assert_eq!(unaffected.generation, new_generation);
        assert_eq!(unaffected.state, zippy_types::query::QueryState::Planned);
        assert_eq!(new_drops.load(Ordering::Relaxed), 0);
        assert_eq!(coordinator.status().active_logical_queries, 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        drop(new_token);
        assert_eq!(new_drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn shutdown_drains_only_after_tasks_and_snapshot_owners_exit() {
        let coordinator = coordinator(config());
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&drops)),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let task = execution.admit_task(charge(1, 1), ()).unwrap();

        let draining = coordinator.begin_shutdown();
        assert_eq!(draining.lifecycle, QueryServiceLifecycle::Draining);
        assert_eq!(draining.active_logical_queries, 0);
        assert_eq!(draining.active_tasks, 1);
        drop(execution);
        assert_eq!(
            coordinator.status().lifecycle,
            QueryServiceLifecycle::Draining
        );
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(task);
        assert_eq!(
            coordinator.status().lifecycle,
            QueryServiceLifecycle::Stopped
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(
            coordinator.begin_shutdown().lifecycle,
            QueryServiceLifecycle::Stopped
        );
    }

    #[test]
    fn shutdown_revokes_planned_snapshot_without_waiting_for_token_drop() {
        let coordinator = coordinator(config());
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&drops)),
                10,
            )
            .unwrap();

        let draining = coordinator.begin_shutdown();
        assert_eq!(draining.lifecycle, QueryServiceLifecycle::Stopped);
        assert_eq!(draining.active_logical_queries, 0);
        assert_eq!(draining.active_snapshot_owners, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(
            coordinator.begin_shutdown().lifecycle,
            QueryServiceLifecycle::Stopped
        );
        drop(token);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(
            coordinator.status().lifecycle,
            QueryServiceLifecycle::Stopped
        );
    }
}
