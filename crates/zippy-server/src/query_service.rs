//! Q0 query resource admission and cancellation coordination.
//!
//! Stable identities, owner capabilities, one-time execution tokens, and snapshot guards remain
//! defined by `zippy-types`. This module only coordinates their process-local runtime ownership.
//! It is intentionally not wired to Gateway until a native physical planner exists.
//! Q0 also does not retain terminal tombstones after every strong query owner exits; bounded
//! status retention and query-ID reuse windows belong to the production registry integration.

use std::any::Any;
use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use zippy_types::query::{
    OpaqueQueryExecutionToken, QueryExecutionToken, QueryPlanInfo, QueryState,
};
use zippy_types::storage::OpaqueQueryReadSnapshot;
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId};

static NEXT_COORDINATOR_NONCE: AtomicU64 = AtomicU64::new(1);

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRuntimeStatus {
    pub query_id: QueryId,
    pub state: QueryState,
    pub logical_admitted: bool,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_memory_bytes: u64,
    pub cancellation_requested: bool,
    pub error: Option<ServerError>,
}

#[derive(Clone)]
pub(crate) struct QueryResourceCoordinator {
    inner: Arc<CoordinatorInner>,
}

impl QueryResourceCoordinator {
    pub(crate) fn new(config: QueryServiceResourceConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                nonce: next_nonce(),
                config: config.validate()?,
                state: Mutex::new(CoordinatorState::default()),
            }),
        })
    }

    pub(crate) fn register_planned(
        &self,
        info: QueryPlanInfo,
        owner_session_id: SessionId,
        execution_deadline_ns: Option<u64>,
        budget: QueryResourceBudget,
        snapshot: OpaqueQueryReadSnapshot,
        now_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        let query_id = info.query_id;
        let budget = budget.validate(self.inner.config)?;
        validate_plan_snapshot_binding(&info, snapshot.descriptor())?;
        if info.expires_at_ns <= now_ns
            || execution_deadline_ns.is_some_and(|deadline| deadline <= now_ns)
        {
            return Err(deadline(query_id, "query expired before plan registration"));
        }

        let control = Arc::new(QueryControl {
            owner: Arc::downgrade(&self.inner),
            query_id,
            budget,
            plan_expires_at_ns: info.expires_at_ns,
            execution_deadline_ns,
            cancelled: AtomicBool::new(false),
            state: Mutex::new(QueryControlState::default()),
        });
        let ownership;
        {
            let mut state = self.inner.state.lock().unwrap();
            state.prune_registry();
            if state.lifecycle != QueryServiceLifecycle::Accepting {
                return Err(unavailable(query_id));
            }
            if state
                .registry
                .get(&query_id)
                .is_some_and(RegistryEntry::is_live)
            {
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
                    control: Arc::downgrade(&control),
                    snapshot_root: Some(Arc::clone(&ownership)),
                },
            );
        }
        Ok(Box::new(QueryServicePlanToken {
            coordinator_nonce: self.inner.nonce,
            info,
            owner_session_id,
            control,
            ownership: Some(ownership),
            armed: true,
        }))
    }

    pub(crate) fn begin_execution(
        &self,
        token: OpaqueQueryExecutionToken,
        now_ns: u64,
    ) -> Result<QueryExecutionScope> {
        let transferred = token.into_any();
        let mut transferred = transferred
            .downcast::<TransferredPlanToken>()
            .map_err(|_| invalid("query token was not issued by QueryService Q0"))?;
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

    pub(crate) fn cancel(&self, query_id: QueryId) -> Result<()> {
        self.inner.terminate_by_id(
            query_id,
            QueryState::Cancelled,
            ServerError::new(ErrorCode::Cancelled, "query cancelled by coordinator")
                .with_query_id(query_id),
        )
    }

    pub(crate) fn sweep_expired(&self, now_ns: u64) -> usize {
        self.inner.sweep_expired(now_ns)
    }

    pub(crate) fn begin_shutdown(&self) -> QueryServiceStatus {
        self.inner.begin_shutdown()
    }

    pub(crate) fn status(&self) -> QueryServiceStatus {
        self.inner.status()
    }

    pub(crate) fn query_status(&self, query_id: QueryId) -> Result<QueryRuntimeStatus> {
        self.inner.query_status(query_id)
    }
}

struct CoordinatorInner {
    nonce: u64,
    config: QueryServiceResourceConfig,
    state: Mutex<CoordinatorState>,
}

struct CoordinatorState {
    lifecycle: QueryServiceLifecycle,
    registry: HashMap<QueryId, RegistryEntry>,
    active_logical_queries: usize,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_memory_bytes: u64,
    active_snapshot_owners: usize,
}

struct RegistryEntry {
    control: Weak<QueryControl>,
    snapshot_root: Option<Arc<SharedSnapshotOwnership>>,
}

impl RegistryEntry {
    fn is_live(&self) -> bool {
        self.snapshot_root.is_some() || self.control.strong_count() > 0
    }
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            lifecycle: QueryServiceLifecycle::Accepting,
            registry: HashMap::new(),
            active_logical_queries: 0,
            active_tasks: 0,
            active_cpu_slots: 0,
            active_memory_bytes: 0,
            active_snapshot_owners: 0,
        }
    }
}

impl CoordinatorState {
    fn prune_registry(&mut self) {
        self.registry.retain(|_, entry| entry.is_live());
    }

    fn take_snapshot_root(&mut self, query_id: QueryId) -> Option<Arc<SharedSnapshotOwnership>> {
        self.registry
            .get_mut(&query_id)
            .and_then(|entry| entry.snapshot_root.take())
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

    fn status(&mut self) -> QueryServiceStatus {
        self.prune_registry();
        QueryServiceStatus {
            lifecycle: self.lifecycle,
            registered_queries: self.registry.len(),
            active_logical_queries: self.active_logical_queries,
            active_tasks: self.active_tasks,
            active_cpu_slots: self.active_cpu_slots,
            active_memory_bytes: self.active_memory_bytes,
            active_snapshot_owners: self.active_snapshot_owners,
        }
    }
}

impl CoordinatorInner {
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
            Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::TimedOut,
                Some(error.clone()),
            );
            let snapshot_root = global.take_snapshot_root(control.query_id);
            drop(query);
            drop(global);
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
        let snapshot_root = (query.state.is_terminal() && query.active_tasks == 0)
            .then(|| global.take_snapshot_root(control.query_id))
            .flatten();
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
        Self::terminate_locked(&mut global, control, &mut query, QueryState::Finished, None);
        let snapshot_root = global.take_snapshot_root(control.query_id);
        drop(query);
        drop(global);
        release_snapshot_root(snapshot_root);
        Ok(())
    }

    fn terminate_by_id(
        &self,
        query_id: QueryId,
        terminal: QueryState,
        error: ServerError,
    ) -> Result<()> {
        let mut global = self.state.lock().unwrap();
        let control = global
            .registry
            .get(&query_id)
            .and_then(|entry| entry.control.upgrade())
            .ok_or_else(|| not_found(query_id))?;
        let mut query = control.state.lock().unwrap();
        Self::terminate_locked(&mut global, &control, &mut query, terminal, Some(error));
        let snapshot_root = (query.active_tasks == 0)
            .then(|| global.take_snapshot_root(query_id))
            .flatten();
        drop(query);
        drop(global);
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
        Self::terminate_locked(&mut global, control, &mut query, terminal, Some(error));
        let snapshot_root = (query.active_tasks == 0)
            .then(|| global.take_snapshot_root(control.query_id))
            .flatten();
        drop(query);
        drop(global);
        release_snapshot_root(snapshot_root);
    }

    fn terminate_locked(
        global: &mut CoordinatorState,
        control: &QueryControl,
        query: &mut QueryControlState,
        terminal: QueryState,
        error: Option<ServerError>,
    ) -> bool {
        if query.state.is_terminal() {
            return false;
        }
        debug_assert!(terminal.is_terminal());
        debug_assert!(query.state.can_transition_to(terminal));
        debug_assert_eq!(terminal == QueryState::Finished, error.is_none());
        query.state = terminal;
        query.error = error;
        if query.logical_admitted {
            query.logical_admitted = false;
            debug_assert!(global.active_logical_queries > 0);
            global.active_logical_queries -= 1;
        }
        if terminal != QueryState::Finished {
            control.cancelled.store(true, Ordering::Release);
        }
        global.refresh_shutdown();
        true
    }

    fn sweep_expired(&self, now_ns: u64) -> usize {
        let mut global = self.state.lock().unwrap();
        global.prune_registry();
        let controls = global
            .registry
            .values()
            .filter_map(|entry| entry.control.upgrade())
            .collect::<Vec<_>>();
        let mut expired = 0;
        let mut snapshot_roots = Vec::new();
        for control in controls {
            let mut query = control.state.lock().unwrap();
            let due = match query.state {
                QueryState::Planned => {
                    control.plan_expires_at_ns <= now_ns
                        || control
                            .execution_deadline_ns
                            .is_some_and(|deadline| deadline <= now_ns)
                }
                QueryState::Running => control
                    .execution_deadline_ns
                    .is_some_and(|deadline| deadline <= now_ns),
                _ => false,
            };
            if due {
                let error = deadline(control.query_id, "query exceeded its runtime deadline");
                let terminated = Self::terminate_locked(
                    &mut global,
                    &control,
                    &mut query,
                    QueryState::TimedOut,
                    Some(error),
                );
                expired += usize::from(terminated);
                if terminated && query.active_tasks == 0 {
                    if let Some(root) = global.take_snapshot_root(control.query_id) {
                        snapshot_roots.push(root);
                    }
                }
            }
        }
        drop(global);
        for root in snapshot_roots {
            root.release();
        }
        expired
    }

    fn begin_shutdown(&self) -> QueryServiceStatus {
        let mut global = self.state.lock().unwrap();
        if global.lifecycle == QueryServiceLifecycle::Accepting {
            global.lifecycle = QueryServiceLifecycle::Draining;
        }
        let controls = global
            .registry
            .values()
            .filter_map(|entry| entry.control.upgrade())
            .collect::<Vec<_>>();
        let mut snapshot_roots = Vec::new();
        for control in controls {
            let mut query = control.state.lock().unwrap();
            let error = ServerError::new(
                ErrorCode::Cancelled,
                "query cancelled during QueryService shutdown",
            )
            .with_query_id(control.query_id);
            Self::terminate_locked(
                &mut global,
                &control,
                &mut query,
                QueryState::Cancelled,
                Some(error),
            );
            if query.active_tasks == 0 {
                if let Some(root) = global.take_snapshot_root(control.query_id) {
                    snapshot_roots.push(root);
                }
            }
        }
        global.refresh_shutdown();
        drop(global);
        for root in snapshot_roots {
            root.release();
        }
        self.status()
    }

    fn status(&self) -> QueryServiceStatus {
        self.state.lock().unwrap().status()
    }

    fn query_status(&self, query_id: QueryId) -> Result<QueryRuntimeStatus> {
        let control = self
            .state
            .lock()
            .unwrap()
            .registry
            .get(&query_id)
            .and_then(|entry| entry.control.upgrade())
            .ok_or_else(|| not_found(query_id))?;
        let query = control.state.lock().unwrap();
        Ok(QueryRuntimeStatus {
            query_id,
            state: query.state,
            logical_admitted: query.logical_admitted,
            active_tasks: query.active_tasks,
            active_cpu_slots: query.active_cpu_slots,
            active_memory_bytes: query.active_memory_bytes,
            cancellation_requested: control.cancelled.load(Ordering::Acquire),
            error: query.error.clone(),
        })
    }
}

impl Drop for CoordinatorInner {
    fn drop(&mut self) {
        if let Ok(global) = self.state.get_mut() {
            global.lifecycle = QueryServiceLifecycle::Stopped;
            for control in global
                .registry
                .values()
                .filter_map(|entry| entry.control.upgrade())
            {
                control.cancelled.store(true, Ordering::Release);
                if let Ok(mut query) = control.state.lock() {
                    if !query.state.is_terminal() {
                        query.state = QueryState::Cancelled;
                        query.logical_admitted = false;
                        query.error = Some(
                            ServerError::new(
                                ErrorCode::Cancelled,
                                "QueryService coordinator was dropped",
                            )
                            .with_query_id(control.query_id),
                        );
                    }
                }
            }
        }
    }
}

struct QueryControl {
    owner: Weak<CoordinatorInner>,
    query_id: QueryId,
    budget: QueryResourceBudget,
    plan_expires_at_ns: u64,
    execution_deadline_ns: Option<u64>,
    cancelled: AtomicBool,
    state: Mutex<QueryControlState>,
}

struct QueryControlState {
    state: QueryState,
    logical_admitted: bool,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_memory_bytes: u64,
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
            error: None,
        }
    }
}

impl QueryControl {
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use zippy_types::storage::{QueryReadSnapshotDescriptor, QueryReadSnapshotGuard};
    use zippy_types::{BoxFuture, ErrorCode, QueryId, Result, SessionId};

    use super::{
        QueryResourceBudget, QueryResourceCoordinator, QueryServiceLifecycle,
        QueryServiceResourceConfig, QueryTaskCharge,
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
    fn registry_retains_revocable_snapshot_root_and_plan_drop_releases_once() {
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
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
            assert_eq!(state.registry[&id].control.strong_count(), 1);
            assert!(state.registry[&id].snapshot_root.is_some());
        }
        drop(token);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_logical_queries, 0);
        assert_eq!(coordinator.status().registered_queries, 0);
        assert_eq!(coordinator.sweep_expired(200), 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn rejected_registration_does_not_decrement_or_increment_admitted_ownership() {
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let admitted_drops = Arc::new(AtomicUsize::new(0));
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::clone(&admitted_drops)),
                10,
            )
            .unwrap();

        let duplicate = coordinator
            .register_planned(
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
            .register_planned(
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
            .register_planned(
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
        let coordinator = QueryResourceCoordinator::new(QueryServiceResourceConfig {
            max_active_queries: 2,
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes: 100,
        })
        .unwrap();
        let full_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 2,
            max_memory_bytes: 100,
        };
        let drops = Arc::new(AtomicUsize::new(0));
        let first_id = query_id(1);
        let second_id = query_id(2);
        let first = coordinator
            .register_planned(
                info(first_id, 100),
                session_id(1),
                None,
                full_budget,
                snapshot(first_id, Arc::clone(&drops)),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned(
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
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let snapshot_drops = Arc::new(AtomicUsize::new(0));
        let resource_drops = Arc::new(AtomicUsize::new(0));
        let replacement_drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
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

        coordinator.cancel(id).unwrap();
        let status = coordinator.status();
        assert_eq!(status.active_logical_queries, 0);
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_cpu_slots, 2);
        assert_eq!(status.active_memory_bytes, 80);
        assert!(task.is_cancelled());
        let replacement_id = query_id(2);
        let blocked_replacement = coordinator
            .register_planned(
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
            .register_planned(
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
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
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
            coordinator.query_status(id).unwrap().state,
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
    fn shutdown_drains_only_after_tasks_and_snapshot_owners_exit() {
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
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
        let coordinator = QueryResourceCoordinator::new(config()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned(
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
