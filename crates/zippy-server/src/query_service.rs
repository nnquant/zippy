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
use zippy_core::{
    DEFAULT_MAX_CPU_CORES, DEFAULT_QUERY_MAX_ACTIVE_TASKS, DEFAULT_QUERY_MAX_CONCURRENT_QUERIES,
    DEFAULT_QUERY_MAX_MEMORY_BYTES, DEFAULT_QUERY_PLAN_TTL_MS, MAX_QUERY_CONCURRENT_QUERIES,
};
use zippy_types::query::{
    OpaqueQueryExecutionToken, OpaqueQueryOwnerCapability, QueryExecutionToken, QueryInstanceId,
    QueryOwnerCapability, QueryPlanInfo, QueryState, QueryTableVersion,
};
use zippy_types::storage::OpaqueQueryReadSnapshot;
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId};

static NEXT_COORDINATOR_NONCE: AtomicU64 = AtomicU64::new(1);

const DEFAULT_TERMINAL_RETENTION: Duration = Duration::from_secs(300);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_MAX_PLANNING_DURATION: Duration = Duration::from_secs(30);
const DEFAULT_PLAN_TTL: Duration = Duration::from_millis(DEFAULT_QUERY_PLAN_TTL_MS);
const DEFAULT_MAX_REGISTERED_QUERIES: usize = MAX_QUERY_CONCURRENT_QUERIES;
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
            max_active_queries: DEFAULT_QUERY_MAX_CONCURRENT_QUERIES,
            max_active_tasks: DEFAULT_QUERY_MAX_ACTIVE_TASKS,
            max_cpu_slots: u32::try_from(DEFAULT_MAX_CPU_CORES)
                .expect("default runtime CPU budget must fit u32"),
            max_memory_bytes: DEFAULT_QUERY_MAX_MEMORY_BYTES,
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
    pub max_planning_duration: Duration,
    pub plan_ttl: Duration,
    pub max_registered_queries: usize,
    pub max_query_tombstones: usize,
}

impl Default for QueryServiceRegistryConfig {
    fn default() -> Self {
        Self {
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            terminal_retention: DEFAULT_TERMINAL_RETENTION,
            max_planning_duration: DEFAULT_MAX_PLANNING_DURATION,
            plan_ttl: DEFAULT_PLAN_TTL,
            max_registered_queries: DEFAULT_MAX_REGISTERED_QUERIES,
            max_query_tombstones: DEFAULT_MAX_QUERY_TOMBSTONES,
        }
    }
}

impl QueryServiceRegistryConfig {
    pub(crate) fn validate(self, resources: QueryServiceResourceConfig) -> Result<Self> {
        if self.sweep_interval.is_zero()
            || self.terminal_retention.is_zero()
            || self.max_planning_duration.is_zero()
            || self.plan_ttl.is_zero()
            || self.max_registered_queries == 0
            || self.max_query_tombstones == 0
        {
            return Err(invalid(
                "query service registry durations and capacities must be positive",
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

/// Immutable request metadata retained before snapshot resolution starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuerySubmissionMetadata {
    /// Caller-assigned identity; QueryService adds a process-local generation at admission.
    pub query_id: QueryId,
    /// Absolute Unix-nanosecond deadline for planning, execution, and result delivery.
    pub deadline_ns: Option<u64>,
}

/// Capability-free native plan metadata before QueryService assigns lifecycle timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryPlanDraft {
    query_id: QueryId,
    catalog_revision: u64,
    snapshots: Vec<QueryTableVersion>,
}

impl QueryPlanDraft {
    pub(crate) fn new(
        query_id: QueryId,
        catalog_revision: u64,
        snapshots: Vec<QueryTableVersion>,
    ) -> Self {
        Self {
            query_id,
            catalog_revision,
            snapshots,
        }
    }

    pub(crate) fn snapshots(&self) -> &[QueryTableVersion] {
        &self.snapshots
    }

    fn into_plan_info(self, planned_at_ns: u64, expires_at_ns: u64) -> QueryPlanInfo {
        QueryPlanInfo {
            query_id: self.query_id,
            catalog_revision: self.catalog_revision,
            snapshots: self.snapshots,
            planned_at_ns,
            expires_at_ns,
        }
    }
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
pub(crate) struct QueryResultCharge {
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
    pub tracked_sessions: usize,
    pub active_logical_queries: usize,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_task_memory_bytes: u64,
    pub active_result_permits: usize,
    pub active_result_memory_bytes: u64,
    pub active_memory_bytes: u64,
    pub active_snapshot_owners: usize,
    pub query_tombstones: usize,
    pub scheduled_deadlines: usize,
    pub sweeper_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryPlanStatus {
    pub planned_at_ns: u64,
    pub expires_at_ns: u64,
    pub catalog_revision: u64,
    /// An empty vector is a legitimately planned table-free query.
    pub snapshots: Vec<QueryTableVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRuntimeStatus {
    pub query_id: QueryId,
    pub generation: u64,
    pub owner_session_id: SessionId,
    pub state: QueryState,
    pub submitted_at_ns: u64,
    /// `None` means snapshot planning has not completed; a present empty snapshot list is valid.
    pub plan: Option<QueryPlanStatus>,
    pub started_at_ns: Option<u64>,
    pub logical_admitted: bool,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_task_memory_bytes: u64,
    pub active_result_permits: usize,
    pub active_result_memory_bytes: u64,
    pub active_memory_bytes: u64,
    pub cancellation_requested: bool,
    pub finished_at_ns: Option<u64>,
    pub error: Option<ServerError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuerySessionResourceStatus {
    pub session_id: SessionId,
    pub active_logical_queries: usize,
    pub active_tasks: usize,
    pub active_cpu_slots: u32,
    pub active_task_memory_bytes: u64,
    pub active_result_permits: usize,
    pub active_result_memory_bytes: u64,
    pub active_memory_bytes: u64,
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
            max_planning_duration_ns: duration_ns(registry_config.max_planning_duration),
            plan_ttl_ns: duration_ns(registry_config.plan_ttl),
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

    /// Admits a query before snapshot resolution or physical planning starts.
    pub(crate) fn submit(
        &self,
        owner: &dyn QueryOwnerCapability,
        metadata: QuerySubmissionMetadata,
        budget: QueryResourceBudget,
    ) -> Result<QueryPlanningToken> {
        self.submit_at(owner, metadata, budget, now_ns())
    }

    fn submit_at(
        &self,
        owner: &dyn QueryOwnerCapability,
        metadata: QuerySubmissionMetadata,
        budget: QueryResourceBudget,
        submitted_at_ns: u64,
    ) -> Result<QueryPlanningToken> {
        let owner = self.validate_owner(owner)?;
        let query_id = metadata.query_id;
        let budget = budget.validate(self.inner.config)?;
        if metadata
            .deadline_ns
            .is_some_and(|deadline| deadline <= submitted_at_ns)
        {
            return Err(deadline(query_id, "query expired before submission"));
        }
        let server_planning_deadline_ns = submitted_at_ns
            .checked_add(self.inner.max_planning_duration_ns)
            .ok_or_else(|| deadline(query_id, "query planning deadline exceeds Unix time range"))?;
        let planning_deadline_ns = metadata
            .deadline_ns
            .map_or(server_planning_deadline_ns, |request_deadline_ns| {
                request_deadline_ns.min(server_planning_deadline_ns)
            });

        let generation = self.inner.next_query_generation();
        let (terminal_sender, _) = watch::channel(None);
        let control = Arc::new(QueryControl {
            owner: Arc::downgrade(&self.inner),
            query_id,
            generation,
            owner_session_id: owner.session_id,
            owner_nonce: owner.owner_nonce,
            budget,
            submitted_at_ns,
            request_deadline_ns: metadata.deadline_ns,
            planning_deadline_ns,
            cancelled: Arc::new(AtomicBool::new(false)),
            terminal_sender,
            state: Mutex::new(QueryControlState::default()),
        });
        {
            let mut state = self.inner.state.lock().unwrap();
            state
                .registry
                .remove_expired_terminal(submitted_at_ns, self.inner.terminal_retention_ns);
            state.registry.remove_expired_tombstones(submitted_at_ns);
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
            self.inner.prepare_registry_for_registration(
                &mut state.registry,
                submitted_at_ns,
                query_id,
            )?;
            state.active_logical_queries += 1;
            state
                .sessions
                .entry(owner.session_id)
                .or_default()
                .active_logical_queries += 1;
            state.registry.insert_planning(
                query_id,
                RegistryEntry {
                    control: Arc::clone(&control),
                    snapshot_root: None,
                },
            );
        }
        Ok(QueryPlanningToken {
            coordinator_nonce: self.inner.nonce,
            control,
            armed: true,
        })
    }

    /// Atomically binds the completed plan and snapshot root to an admitted planning instance.
    pub(crate) fn complete_planning(
        &self,
        token: QueryPlanningToken,
        draft: QueryPlanDraft,
        snapshot: OpaqueQueryReadSnapshot,
    ) -> Result<OpaqueQueryExecutionToken> {
        self.complete_planning_at(token, draft, snapshot, now_ns())
    }

    fn complete_planning_at(
        &self,
        mut token: QueryPlanningToken,
        draft: QueryPlanDraft,
        snapshot: OpaqueQueryReadSnapshot,
        completed_at_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        let query_id = token.control.query_id;
        if token.coordinator_nonce != self.inner.nonce {
            return Err(
                invalid("planning token belongs to another QueryService instance")
                    .with_query_id(query_id),
            );
        }
        if draft.query_id != query_id {
            let error = invalid("completed plan belongs to another query").with_query_id(query_id);
            return self.reject_planning(&mut token, error);
        }
        let server_plan_expiry_ns = completed_at_ns.saturating_add(self.inner.plan_ttl_ns);
        let expires_at_ns = token
            .control
            .request_deadline_ns
            .map_or(server_plan_expiry_ns, |deadline_ns| {
                deadline_ns.min(server_plan_expiry_ns)
            });
        if let Err(error) = validate_plan_snapshot_binding(&draft, snapshot.descriptor()) {
            return self.reject_planning(&mut token, error);
        }
        let info = draft.into_plan_info(completed_at_ns, expires_at_ns);

        match self
            .inner
            .complete_planning(&token.control, info.clone(), snapshot, completed_at_ns)
        {
            Ok(ownership) => {
                token.armed = false;
                let execution: OpaqueQueryExecutionToken = Box::new(QueryServicePlanToken {
                    coordinator_nonce: self.inner.nonce,
                    info,
                    owner_session_id: token.control.owner_session_id,
                    control: Arc::clone(&token.control),
                    ownership: Some(ownership),
                    armed: true,
                });
                Ok(execution)
            }
            Err(error) => {
                token.armed = false;
                Err(error)
            }
        }
    }

    /// Completes planning unsuccessfully without allowing token drop to relabel it as cancellation.
    pub(crate) fn fail_planning(
        &self,
        mut token: QueryPlanningToken,
        error: ServerError,
    ) -> Result<()> {
        let query_id = token.control.query_id;
        if token.coordinator_nonce != self.inner.nonce {
            return Err(
                invalid("planning token belongs to another QueryService instance")
                    .with_query_id(query_id),
            );
        }
        let result = self
            .inner
            .fail_planning(&token.control, error.with_query_id(query_id));
        token.armed = false;
        result
    }

    fn reject_planning(
        &self,
        token: &mut QueryPlanningToken,
        error: ServerError,
    ) -> Result<OpaqueQueryExecutionToken> {
        let result = self.inner.fail_planning(&token.control, error.clone());
        token.armed = false;
        match result {
            Ok(()) => Err(error),
            Err(terminal) => Err(terminal),
        }
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

    pub(crate) fn session_status_owned(
        &self,
        owner: &dyn QueryOwnerCapability,
    ) -> Result<QuerySessionResourceStatus> {
        let owner = self.validate_owner(owner)?;
        Ok(self.inner.session_status(owner.session_id))
    }

    #[cfg(test)]
    pub(crate) fn submit_at_for_test(
        &self,
        owner: &dyn QueryOwnerCapability,
        metadata: QuerySubmissionMetadata,
        budget: QueryResourceBudget,
        submitted_at_ns: u64,
    ) -> Result<QueryPlanningToken> {
        self.submit_at(owner, metadata, budget, submitted_at_ns)
    }

    #[cfg(test)]
    pub(crate) fn complete_planning_at_for_test(
        &self,
        token: QueryPlanningToken,
        draft: QueryPlanDraft,
        snapshot: OpaqueQueryReadSnapshot,
        completed_at_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        self.complete_planning_at(token, draft, snapshot, completed_at_ns)
    }

    #[cfg(test)]
    pub(crate) fn register_planned(
        &self,
        owner: &dyn QueryOwnerCapability,
        info: QueryPlanInfo,
        execution_deadline_ns: Option<u64>,
        budget: QueryResourceBudget,
        snapshot: OpaqueQueryReadSnapshot,
        now_ns: u64,
    ) -> Result<OpaqueQueryExecutionToken> {
        let draft =
            QueryPlanDraft::new(info.query_id, info.catalog_revision, info.snapshots.clone());
        validate_plan_snapshot_binding(&draft, snapshot.descriptor())?;
        if execution_deadline_ns.is_some_and(|deadline| deadline <= now_ns) {
            return Err(deadline(
                info.query_id,
                "query expired before test plan registration",
            ));
        }
        let planning = self.submit_at_for_test(
            owner,
            QuerySubmissionMetadata {
                query_id: info.query_id,
                deadline_ns: execution_deadline_ns,
            },
            budget,
            now_ns,
        )?;
        self.complete_planning_at_for_test(planning, draft, snapshot, now_ns)
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

    pub(crate) fn session_status(&self, session_id: SessionId) -> QuerySessionResourceStatus {
        self.coordinator.inner.session_status(session_id)
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
    max_planning_duration_ns: u64,
    plan_ttl_ns: u64,
    next_query_generation: AtomicU64,
    next_owner_nonce: AtomicU64,
    state: Mutex<CoordinatorState>,
}

struct CoordinatorState {
    lifecycle: QueryServiceLifecycle,
    registry: QueryRegistry,
    sessions: HashMap<SessionId, SessionResourceUsage>,
    active_logical_queries: usize,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_task_memory_bytes: u64,
    active_result_permits: usize,
    active_result_memory_bytes: u64,
    active_snapshot_owners: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SessionResourceUsage {
    active_logical_queries: usize,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_task_memory_bytes: u64,
    active_result_permits: usize,
    active_result_memory_bytes: u64,
}

impl SessionResourceUsage {
    fn is_idle(self) -> bool {
        self.active_logical_queries == 0
            && self.active_tasks == 0
            && self.active_cpu_slots == 0
            && self.active_task_memory_bytes == 0
            && self.active_result_permits == 0
            && self.active_result_memory_bytes == 0
    }

    fn status(self, session_id: SessionId) -> QuerySessionResourceStatus {
        QuerySessionResourceStatus {
            session_id,
            active_logical_queries: self.active_logical_queries,
            active_tasks: self.active_tasks,
            active_cpu_slots: self.active_cpu_slots,
            active_task_memory_bytes: self.active_task_memory_bytes,
            active_result_permits: self.active_result_permits,
            active_result_memory_bytes: self.active_result_memory_bytes,
            active_memory_bytes: self
                .active_task_memory_bytes
                .checked_add(self.active_result_memory_bytes)
                .expect("session query memory accounting remains within global budget"),
        }
    }
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
    PlanningExpiry,
    PlanExpiry,
    RequestDeadline,
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

    fn insert_planning(&mut self, query_id: QueryId, entry: RegistryEntry) {
        debug_assert!(!self.tombstones.contains_key(&query_id));
        let control = &entry.control;
        let inserted_planning_deadline = self.deadline_order.insert((
            control.planning_deadline_ns,
            query_id,
            control.generation,
            QueryDeadlineKind::PlanningExpiry,
        ));
        debug_assert!(inserted_planning_deadline);
        let previous = self.by_id.insert(query_id, entry);
        debug_assert!(previous.is_none());
    }

    fn install_planned(
        &mut self,
        control: &QueryControl,
        ownership: Arc<SharedSnapshotOwnership>,
        expires_at_ns: u64,
    ) {
        let entry = self
            .by_id
            .get_mut(&control.query_id)
            .filter(|entry| entry.control.generation == control.generation)
            .expect("planning control remains registered at completion");
        debug_assert!(entry.snapshot_root.is_none());
        entry.snapshot_root = Some(ownership);
        let removed_planning_deadline = self.deadline_order.remove(&(
            control.planning_deadline_ns,
            control.query_id,
            control.generation,
            QueryDeadlineKind::PlanningExpiry,
        ));
        debug_assert!(removed_planning_deadline);
        if let Some(deadline_ns) = control.request_deadline_ns {
            let inserted_request_deadline = self.deadline_order.insert((
                deadline_ns,
                control.query_id,
                control.generation,
                QueryDeadlineKind::RequestDeadline,
            ));
            debug_assert!(inserted_request_deadline);
        }
        if control.request_deadline_ns != Some(expires_at_ns) {
            let inserted_plan_deadline = self.deadline_order.insert((
                expires_at_ns,
                control.query_id,
                control.generation,
                QueryDeadlineKind::PlanExpiry,
            ));
            debug_assert!(inserted_plan_deadline);
        }
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

    fn remove_plan_deadline(&mut self, control: &QueryControl, expires_at_ns: u64) {
        self.deadline_order.remove(&(
            expires_at_ns,
            control.query_id,
            control.generation,
            QueryDeadlineKind::PlanExpiry,
        ));
    }

    fn remove_deadlines(&mut self, control: &QueryControl, plan: Option<&QueryPlanInfo>) {
        self.deadline_order.remove(&(
            control.planning_deadline_ns,
            control.query_id,
            control.generation,
            QueryDeadlineKind::PlanningExpiry,
        ));
        if let Some(plan) = plan {
            self.remove_plan_deadline(control, plan.expires_at_ns);
        }
        if let Some(deadline_ns) = control.request_deadline_ns {
            self.deadline_order.remove(&(
                deadline_ns,
                control.query_id,
                control.generation,
                QueryDeadlineKind::RequestDeadline,
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
            sessions: HashMap::new(),
            active_logical_queries: 0,
            active_tasks: 0,
            active_cpu_slots: 0,
            active_task_memory_bytes: 0,
            active_result_permits: 0,
            active_result_memory_bytes: 0,
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
            && self.active_result_permits == 0
            && self.active_snapshot_owners == 0
        {
            self.lifecycle = QueryServiceLifecycle::Stopped;
        }
    }

    fn session_status(&self, session_id: SessionId) -> QuerySessionResourceStatus {
        self.sessions
            .get(&session_id)
            .copied()
            .unwrap_or_default()
            .status(session_id)
    }

    fn remove_idle_session(&mut self, session_id: SessionId) {
        if self
            .sessions
            .get(&session_id)
            .is_some_and(|usage| usage.is_idle())
        {
            self.sessions.remove(&session_id);
        }
    }

    fn status(&self) -> QueryServiceStatus {
        let active_memory_bytes = self
            .active_task_memory_bytes
            .checked_add(self.active_result_memory_bytes)
            .expect("global query memory accounting remains within configured budget");
        QueryServiceStatus {
            lifecycle: self.lifecycle,
            registered_queries: self.registry.len(),
            tracked_sessions: self.sessions.len(),
            active_logical_queries: self.active_logical_queries,
            active_tasks: self.active_tasks,
            active_cpu_slots: self.active_cpu_slots,
            active_task_memory_bytes: self.active_task_memory_bytes,
            active_result_permits: self.active_result_permits,
            active_result_memory_bytes: self.active_result_memory_bytes,
            active_memory_bytes,
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

    fn complete_planning(
        &self,
        control: &Arc<QueryControl>,
        info: QueryPlanInfo,
        snapshot: OpaqueQueryReadSnapshot,
        completed_at_ns: u64,
    ) -> Result<Arc<SharedSnapshotOwnership>> {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        if query.state.is_terminal() {
            return Err(query.error.clone().unwrap_or_else(|| {
                ServerError::new(ErrorCode::Conflict, "query is already terminal")
                    .with_query_id(control.query_id)
            }));
        }
        if query.state != QueryState::Planning {
            let error = ServerError::new(
                ErrorCode::Conflict,
                format!(
                    "query planning is already complete state=[{:?}]",
                    query.state
                ),
            )
            .with_query_id(control.query_id)
            .with_retryable(false);
            let signal = Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::Failed,
                Some(error.clone()),
                completed_at_ns,
            );
            let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
            drop(query);
            drop(global);
            control.publish_terminal(signal);
            release_snapshot_root(snapshot_root);
            return Err(error);
        }

        if control.planning_deadline_ns <= completed_at_ns {
            let error = deadline(control.query_id, "query expired while planning");
            let signal = Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::TimedOut,
                Some(error.clone()),
                completed_at_ns,
            );
            let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
            drop(query);
            drop(global);
            control.publish_terminal(signal);
            release_snapshot_root(snapshot_root);
            return Err(error);
        }

        if global.lifecycle != QueryServiceLifecycle::Accepting {
            let error = ServerError::new(
                ErrorCode::Cancelled,
                "query cancelled while QueryService admission is closed",
            )
            .with_query_id(control.query_id)
            .with_retryable(false);
            let signal = Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::Cancelled,
                Some(error.clone()),
                completed_at_ns,
            );
            let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
            drop(query);
            drop(global);
            control.publish_terminal(signal);
            release_snapshot_root(snapshot_root);
            return Err(error);
        }

        if global.active_snapshot_owners >= self.config.max_active_queries {
            let error = exhausted(
                control.query_id,
                "global query snapshot owner admission is exhausted",
            );
            let signal = Self::terminate_locked(
                &mut global,
                control,
                &mut query,
                QueryState::Failed,
                Some(error.clone()),
                completed_at_ns,
            );
            let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
            drop(query);
            drop(global);
            control.publish_terminal(signal);
            release_snapshot_root(snapshot_root);
            return Err(error);
        }

        let ownership = Arc::new(SharedSnapshotOwnership {
            owner: control.owner.clone(),
            snapshot: Mutex::new(Some(snapshot)),
        });
        global.active_snapshot_owners += 1;
        global
            .registry
            .install_planned(control, Arc::clone(&ownership), info.expires_at_ns);
        query.plan = Some(info);
        query.state = QueryState::Planned;
        Ok(ownership)
    }

    fn fail_planning(&self, control: &Arc<QueryControl>, error: ServerError) -> Result<()> {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        if query.state.is_terminal() {
            return Err(query.error.clone().unwrap_or_else(|| {
                ServerError::new(ErrorCode::Conflict, "query is already terminal")
                    .with_query_id(control.query_id)
            }));
        }
        if query.state != QueryState::Planning {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!("query is not planning state=[{:?}]", query.state),
            )
            .with_query_id(control.query_id)
            .with_retryable(false));
        }
        let signal = Self::terminate_locked(
            &mut global,
            control,
            &mut query,
            QueryState::Failed,
            Some(error),
            now_ns(),
        );
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        drop(query);
        drop(global);
        control.publish_terminal(signal);
        release_snapshot_root(snapshot_root);
        Ok(())
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
        let plan_expires_at_ns = query
            .plan
            .as_ref()
            .map(|plan| plan.expires_at_ns)
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Internal,
                    "planned query is missing plan metadata",
                )
                .with_query_id(control.query_id)
                .with_retryable(false)
            })?;
        if plan_expires_at_ns <= now_ns
            || control
                .request_deadline_ns
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
        query.started_at_ns = Some(now_ns);
        global
            .registry
            .remove_plan_deadline(control, plan_expires_at_ns);
        Ok(())
    }

    fn reserve_task(
        self: &Arc<Self>,
        control: &Arc<QueryControl>,
        charge: QueryTaskCharge,
    ) -> Result<QueryTaskReservation> {
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
        let query_task_memory_after = query.active_task_memory_bytes.checked_add(memory_bytes);
        let query_memory_after = query_task_memory_after
            .and_then(|task_memory| task_memory.checked_add(query.active_result_memory_bytes));
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
        let global_task_memory_after = global.active_task_memory_bytes.checked_add(memory_bytes);
        let global_memory_after = global_task_memory_after
            .and_then(|task_memory| task_memory.checked_add(global.active_result_memory_bytes));
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
        query.active_task_memory_bytes =
            query_task_memory_after.expect("validated per-query task memory addition");
        global.active_tasks += 1;
        global.active_cpu_slots = global_cpu_after.expect("validated global CPU addition");
        global.active_task_memory_bytes =
            global_task_memory_after.expect("validated global task memory addition");
        let session = global
            .sessions
            .get_mut(&control.owner_session_id)
            .expect("admitted query retains its session resource ledger");
        session.active_tasks += 1;
        session.active_cpu_slots = session
            .active_cpu_slots
            .checked_add(cpu_slots)
            .expect("session CPU accounting remains within global budget");
        session.active_task_memory_bytes = session
            .active_task_memory_bytes
            .checked_add(memory_bytes)
            .expect("session task memory accounting remains within global budget");
        Ok(QueryTaskReservation {
            owner: Arc::downgrade(self),
            control: Arc::clone(control),
            charge: Some(charge),
        })
    }

    fn release_task(&self, control: &Arc<QueryControl>, charge: QueryTaskCharge) {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        let cpu_slots = charge.cpu_slots.get();
        let memory_bytes = charge.memory_bytes.get();
        debug_assert!(query.active_tasks > 0 && global.active_tasks > 0);
        query.active_tasks -= 1;
        query.active_cpu_slots -= cpu_slots;
        query.active_task_memory_bytes -= memory_bytes;
        global.active_tasks -= 1;
        global.active_cpu_slots -= cpu_slots;
        global.active_task_memory_bytes -= memory_bytes;
        let session = global
            .sessions
            .get_mut(&control.owner_session_id)
            .expect("active task retains its session resource ledger");
        debug_assert!(session.active_tasks > 0);
        session.active_tasks -= 1;
        session.active_cpu_slots -= cpu_slots;
        session.active_task_memory_bytes -= memory_bytes;
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        global.remove_idle_session(control.owner_session_id);
        global.refresh_shutdown();
        drop(query);
        drop(global);
        release_snapshot_root(snapshot_root);
    }

    fn reserve_result(
        self: &Arc<Self>,
        control: &Arc<QueryControl>,
        charge: QueryResultCharge,
    ) -> Result<QueryResultReservation> {
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

        let memory_bytes = charge.memory_bytes.get();
        let query_result_memory_after = query.active_result_memory_bytes.checked_add(memory_bytes);
        let query_memory_after = query_result_memory_after
            .and_then(|result_memory| result_memory.checked_add(query.active_task_memory_bytes));
        let query_permits_after = query.active_result_permits.checked_add(1);
        if query.active_result_permits >= control.budget.max_active_tasks
            || query_permits_after.is_none()
            || query_memory_after.is_none_or(|value| value > control.budget.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "per-query result resources are exhausted",
            ));
        }
        let global_result_memory_after =
            global.active_result_memory_bytes.checked_add(memory_bytes);
        let global_memory_after = global_result_memory_after
            .and_then(|result_memory| result_memory.checked_add(global.active_task_memory_bytes));
        let global_permits_after = global.active_result_permits.checked_add(1);
        if global.active_result_permits >= self.config.max_active_tasks
            || global_permits_after.is_none()
            || global_memory_after.is_none_or(|value| value > self.config.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "global result resources are exhausted",
            ));
        }

        query.active_result_permits = query_permits_after.expect("validated result permit count");
        query.active_result_memory_bytes =
            query_result_memory_after.expect("validated per-query result memory addition");
        global.active_result_permits =
            global_permits_after.expect("validated global result permit count");
        global.active_result_memory_bytes =
            global_result_memory_after.expect("validated global result memory addition");
        let session = global
            .sessions
            .get_mut(&control.owner_session_id)
            .expect("admitted query retains its session resource ledger");
        session.active_result_permits += 1;
        session.active_result_memory_bytes = session
            .active_result_memory_bytes
            .checked_add(memory_bytes)
            .expect("session result memory accounting remains within global budget");
        Ok(QueryResultReservation {
            owner: Arc::downgrade(self),
            control: Arc::clone(control),
            charge: Some(charge),
        })
    }

    fn transfer_task_to_result(
        self: &Arc<Self>,
        control: &Arc<QueryControl>,
        task_charge: QueryTaskCharge,
        result_charge: QueryResultCharge,
    ) -> Result<QueryResultReservation> {
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

        let cpu_slots = task_charge.cpu_slots.get();
        let task_memory_bytes = task_charge.memory_bytes.get();
        let result_memory_bytes = result_charge.memory_bytes.get();
        debug_assert!(query.active_tasks > 0 && global.active_tasks > 0);
        let query_task_memory_after = query
            .active_task_memory_bytes
            .checked_sub(task_memory_bytes)
            .expect("task reservation owns its per-query memory charge");
        let query_result_memory_after = query
            .active_result_memory_bytes
            .checked_add(result_memory_bytes);
        let query_memory_after = query_result_memory_after
            .and_then(|result_memory| result_memory.checked_add(query_task_memory_after));
        let query_permits_after = query.active_result_permits.checked_add(1);
        if query.active_result_permits >= control.budget.max_active_tasks
            || query_permits_after.is_none()
            || query_memory_after.is_none_or(|value| value > control.budget.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "per-query task-to-result transfer is exhausted",
            ));
        }

        let global_task_memory_after = global
            .active_task_memory_bytes
            .checked_sub(task_memory_bytes)
            .expect("task reservation owns its global memory charge");
        let global_result_memory_after = global
            .active_result_memory_bytes
            .checked_add(result_memory_bytes);
        let global_memory_after = global_result_memory_after
            .and_then(|result_memory| result_memory.checked_add(global_task_memory_after));
        let global_permits_after = global.active_result_permits.checked_add(1);
        if global.active_result_permits >= self.config.max_active_tasks
            || global_permits_after.is_none()
            || global_memory_after.is_none_or(|value| value > self.config.max_memory_bytes)
        {
            return Err(exhausted(
                control.query_id,
                "global task-to-result transfer is exhausted",
            ));
        }

        query.active_tasks -= 1;
        query.active_cpu_slots -= cpu_slots;
        query.active_task_memory_bytes = query_task_memory_after;
        query.active_result_permits = query_permits_after.expect("validated result permit count");
        query.active_result_memory_bytes =
            query_result_memory_after.expect("validated per-query result memory addition");
        global.active_tasks -= 1;
        global.active_cpu_slots -= cpu_slots;
        global.active_task_memory_bytes = global_task_memory_after;
        global.active_result_permits =
            global_permits_after.expect("validated global result permit count");
        global.active_result_memory_bytes =
            global_result_memory_after.expect("validated global result memory addition");
        let session = global
            .sessions
            .get_mut(&control.owner_session_id)
            .expect("active task retains its session resource ledger");
        debug_assert!(session.active_tasks > 0);
        session.active_tasks -= 1;
        session.active_cpu_slots -= cpu_slots;
        session.active_task_memory_bytes -= task_memory_bytes;
        session.active_result_permits += 1;
        session.active_result_memory_bytes = session
            .active_result_memory_bytes
            .checked_add(result_memory_bytes)
            .expect("session result memory accounting remains within global budget");
        Ok(QueryResultReservation {
            owner: Arc::downgrade(self),
            control: Arc::clone(control),
            charge: Some(result_charge),
        })
    }

    fn release_result(&self, control: &Arc<QueryControl>, charge: QueryResultCharge) {
        let mut global = self.state.lock().unwrap();
        let mut query = control.state.lock().unwrap();
        let memory_bytes = charge.memory_bytes.get();
        debug_assert!(query.active_result_permits > 0 && global.active_result_permits > 0);
        query.active_result_permits -= 1;
        query.active_result_memory_bytes -= memory_bytes;
        global.active_result_permits -= 1;
        global.active_result_memory_bytes -= memory_bytes;
        let session = global
            .sessions
            .get_mut(&control.owner_session_id)
            .expect("active result retains its session resource ledger");
        debug_assert!(session.active_result_permits > 0);
        session.active_result_permits -= 1;
        session.active_result_memory_bytes -= memory_bytes;
        let snapshot_root = Self::retire_terminal_if_drained(&mut global, control, &mut query);
        global.remove_idle_session(control.owner_session_id);
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
        if query.active_tasks != 0 || query.active_result_permits != 0 {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "query cannot finish while physical tasks or result delivery remain active",
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

    fn fail_execution(&self, control: &Arc<QueryControl>, error: ServerError) -> Result<()> {
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
                    .with_query_id(control.query_id)
                    .with_retryable(false),
            );
        }
        let signal = Self::terminate_locked(
            &mut global,
            control,
            &mut query,
            QueryState::Failed,
            Some(error.with_query_id(control.query_id)),
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
        global
            .registry
            .remove_deadlines(control, query.plan.as_ref());
        if query.logical_admitted {
            query.logical_admitted = false;
            debug_assert!(global.active_logical_queries > 0);
            global.active_logical_queries -= 1;
            let session = global
                .sessions
                .get_mut(&control.owner_session_id)
                .expect("logically admitted query retains its session resource ledger");
            debug_assert!(session.active_logical_queries > 0);
            session.active_logical_queries -= 1;
            global.remove_idle_session(control.owner_session_id);
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
        if !query.state.is_terminal() {
            return None;
        }
        let snapshot_root = if query.active_tasks == 0 {
            global.take_snapshot_root(control.query_id, control.generation)
        } else {
            None
        };
        if query.active_tasks == 0 && query.active_result_permits == 0 && !query.retention_scheduled
        {
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
                QueryDeadlineKind::PlanningExpiry => query.state == QueryState::Planning,
                QueryDeadlineKind::PlanExpiry => query.state == QueryState::Planned,
                QueryDeadlineKind::RequestDeadline => {
                    matches!(query.state, QueryState::Planned | QueryState::Running)
                }
            };
            if due {
                let message = match kind {
                    QueryDeadlineKind::PlanningExpiry => {
                        "query exceeded its effective planning deadline"
                    }
                    QueryDeadlineKind::PlanExpiry => "unexecuted query plan exceeded its TTL",
                    QueryDeadlineKind::RequestDeadline => "query exceeded its request deadline",
                };
                let error = deadline(control.query_id, message);
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

    fn session_status(&self, session_id: SessionId) -> QuerySessionResourceStatus {
        self.state.lock().unwrap().session_status(session_id)
    }

    fn query_status(&self, control: &Arc<QueryControl>) -> Result<QueryRuntimeStatus> {
        let query = control.state.lock().unwrap();
        Ok(QueryRuntimeStatus {
            query_id: control.query_id,
            generation: control.generation,
            owner_session_id: control.owner_session_id,
            state: query.state,
            submitted_at_ns: control.submitted_at_ns,
            plan: query.plan.as_ref().map(|plan| QueryPlanStatus {
                planned_at_ns: plan.planned_at_ns,
                expires_at_ns: plan.expires_at_ns,
                catalog_revision: plan.catalog_revision,
                snapshots: plan.snapshots.clone(),
            }),
            started_at_ns: query.started_at_ns,
            logical_admitted: query.logical_admitted,
            active_tasks: query.active_tasks,
            active_cpu_slots: query.active_cpu_slots,
            active_task_memory_bytes: query.active_task_memory_bytes,
            active_result_permits: query.active_result_permits,
            active_result_memory_bytes: query.active_result_memory_bytes,
            active_memory_bytes: query
                .active_task_memory_bytes
                .checked_add(query.active_result_memory_bytes)
                .expect("query memory accounting remains within configured budget"),
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
    submitted_at_ns: u64,
    request_deadline_ns: Option<u64>,
    planning_deadline_ns: u64,
    cancelled: Arc<AtomicBool>,
    terminal_sender: watch::Sender<Option<QueryTerminalSignal>>,
    state: Mutex<QueryControlState>,
}

struct QueryControlState {
    state: QueryState,
    plan: Option<QueryPlanInfo>,
    started_at_ns: Option<u64>,
    logical_admitted: bool,
    active_tasks: usize,
    active_cpu_slots: u32,
    active_task_memory_bytes: u64,
    active_result_permits: usize,
    active_result_memory_bytes: u64,
    finished_at_ns: Option<u64>,
    retention_scheduled: bool,
    error: Option<ServerError>,
}

impl Default for QueryControlState {
    fn default() -> Self {
        Self {
            state: QueryState::Planning,
            plan: None,
            started_at_ns: None,
            logical_admitted: true,
            active_tasks: 0,
            active_cpu_slots: 0,
            active_task_memory_bytes: 0,
            active_result_permits: 0,
            active_result_memory_bytes: 0,
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

struct SnapshotOwnerReleaseGuard {
    owner: Weak<CoordinatorInner>,
}

impl Drop for SnapshotOwnerReleaseGuard {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.release_snapshot_owner();
        }
    }
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
        let _release = SnapshotOwnerReleaseGuard {
            owner: self.owner.clone(),
        };
        drop(snapshot);
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
            let _release = SnapshotOwnerReleaseGuard {
                owner: self.owner.clone(),
            };
            drop(snapshot);
        }
    }
}

fn release_snapshot_root(root: Option<Arc<SharedSnapshotOwnership>>) {
    if let Some(root) = root {
        root.release();
    }
}

/// One-time, engine-issued ownership of an admitted query while it is planning.
///
/// Private fields make the token origin-authenticated, and the absence of `Clone` makes exactly
/// one caller responsible for completing, failing, or cancelling this query incarnation.
pub(crate) struct QueryPlanningToken {
    coordinator_nonce: u64,
    control: Arc<QueryControl>,
    armed: bool,
}

impl QueryPlanningToken {
    pub(crate) fn instance_id(&self) -> QueryInstanceId {
        QueryInstanceId::new(
            self.control.query_id,
            NonZeroU64::new(self.control.generation)
                .expect("QueryService query generation is nonzero"),
        )
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.control.cancellation()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.control.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn terminal_error(&self) -> Option<ServerError> {
        self.control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .error
            .clone()
    }
}

impl Drop for QueryPlanningToken {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel("QueryService planning token dropped");
        }
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

    pub(crate) fn instance_id(&self) -> QueryInstanceId {
        QueryInstanceId::new(
            self.control.query_id,
            NonZeroU64::new(self.control.generation)
                .expect("QueryService query generation is nonzero"),
        )
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.control.cancellation()
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
        let reservation = owner.reserve_task(&self.control, charge)?;
        let resource = self.with_snapshot(build)?;
        Ok(QueryTaskPermit {
            ownership: self.ownership.as_ref().map(Arc::clone),
            resource: Some(resource),
            reservation: Some(reservation),
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
        let reservation = owner.reserve_task(&self.control, charge)?;
        Ok(QueryTaskPermit {
            ownership: self.ownership.as_ref().map(Arc::clone),
            resource: Some(resource),
            reservation: Some(reservation),
        })
    }

    pub(crate) fn admit_result<R: Send + 'static>(
        &self,
        charge: QueryResultCharge,
        result: R,
    ) -> Result<QueryResultPermit<R>> {
        let owner = self.control.owner.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "QueryService coordinator is unavailable",
            )
            .with_query_id(self.control.query_id)
        })?;
        let reservation = owner.reserve_result(&self.control, charge)?;
        Ok(QueryResultPermit {
            result: Some(result),
            reservation: Some(reservation),
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

    /// Records a physical execution error without allowing scope drop to relabel it as cancellation.
    pub(crate) fn fail(mut self, error: ServerError) -> Result<()> {
        let owner = self.control.owner.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Unavailable,
                "QueryService coordinator is unavailable",
            )
            .with_query_id(self.control.query_id)
        })?;
        owner.fail_execution(&self.control, error)?;
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

struct QueryTaskReservation {
    owner: Weak<CoordinatorInner>,
    control: Arc<QueryControl>,
    charge: Option<QueryTaskCharge>,
}

impl QueryTaskReservation {
    fn try_into_result(&mut self, charge: QueryResultCharge) -> Result<QueryResultReservation> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| unavailable(self.control.query_id))?;
        let task_charge = self
            .charge
            .expect("live task reservation retains its resource charge");
        let result = owner.transfer_task_to_result(&self.control, task_charge, charge)?;
        self.charge = None;
        Ok(result)
    }
}

impl Drop for QueryTaskReservation {
    fn drop(&mut self) {
        let Some(charge) = self.charge.take() else {
            return;
        };
        if let Some(owner) = self.owner.upgrade() {
            owner.release_task(&self.control, charge);
        }
    }
}

struct QueryResultReservation {
    owner: Weak<CoordinatorInner>,
    control: Arc<QueryControl>,
    charge: Option<QueryResultCharge>,
}

impl Drop for QueryResultReservation {
    fn drop(&mut self) {
        let Some(charge) = self.charge.take() else {
            return;
        };
        if let Some(owner) = self.owner.upgrade() {
            owner.release_result(&self.control, charge);
        }
    }
}

pub(crate) struct QueryTaskPermit<R: Send + 'static> {
    resource: Option<R>,
    ownership: Option<Arc<SharedSnapshotOwnership>>,
    reservation: Option<QueryTaskReservation>,
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
        self.reservation
            .as_ref()
            .expect("live task permit retains its resource reservation")
            .control
            .cancelled
            .load(Ordering::Acquire)
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.reservation
            .as_ref()
            .expect("live task permit retains its resource reservation")
            .control
            .cancellation()
    }

    /// Converts a task-owned buffer and atomically reclassifies its charge for result delivery.
    ///
    /// The task charge must cover memory materialized by `convert` until the reclassification
    /// commits. A rejected transfer drops the converted result before releasing that task charge.
    pub(crate) fn into_result<T: Send + 'static>(
        mut self,
        charge: QueryResultCharge,
        convert: impl FnOnce(R) -> Result<T>,
    ) -> Result<QueryResultPermit<T>> {
        let resource = self
            .resource
            .take()
            .expect("live task permit retains its physical resource");
        let result = convert(resource)?;
        let reservation = match self
            .reservation
            .as_mut()
            .expect("live task permit retains its resource reservation")
            .try_into_result(charge)
        {
            Ok(reservation) => reservation,
            Err(error) => {
                drop(result);
                return Err(error);
            }
        };
        drop(self.ownership.take());
        Ok(QueryResultPermit {
            result: Some(result),
            reservation: Some(reservation),
        })
    }
}

pub(crate) struct QueryResultPermit<R: Send + 'static> {
    result: Option<R>,
    reservation: Option<QueryResultReservation>,
}

impl<R: Send + 'static> QueryResultPermit<R> {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.reservation
            .as_ref()
            .expect("live result permit retains its resource reservation")
            .control
            .cancelled
            .load(Ordering::Acquire)
    }

    pub(crate) fn cancellation(&self) -> QueryCancellation {
        self.reservation
            .as_ref()
            .expect("live result permit retains its resource reservation")
            .control
            .cancellation()
    }
}

impl<R: Send + 'static> AsRef<R> for QueryResultPermit<R> {
    fn as_ref(&self) -> &R {
        self.result
            .as_ref()
            .expect("live result permit retains its result buffer")
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
    draft: &QueryPlanDraft,
    snapshot: &zippy_types::storage::QueryReadSnapshotDescriptor,
) -> Result<()> {
    if snapshot.query_id != draft.query_id {
        return Err(
            invalid("query snapshot owner belongs to another query").with_query_id(draft.query_id)
        );
    }
    if snapshot.catalog_revision != draft.catalog_revision {
        return Err(
            invalid("query plan catalog revision does not match snapshot owner")
                .with_query_id(draft.query_id),
        );
    }
    if snapshot.tables.len() != draft.snapshots.len()
        || snapshot
            .tables
            .iter()
            .zip(&draft.snapshots)
            .any(|(actual, planned)| {
                actual.table_id != planned.table_id
                    || actual.persisted_snapshot_version != planned.snapshot_version
                    || actual.schema_version != planned.schema_version
            })
    {
        return Err(
            invalid("query plan table versions do not match snapshot owner")
                .with_query_id(draft.query_id),
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
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use zippy_types::query::QueryOwnerCapability;
    use zippy_types::storage::{QueryReadSnapshotDescriptor, QueryReadSnapshotGuard};
    use zippy_types::{BoxFuture, ErrorCode, QueryId, Result, ServerError, SessionId};

    use super::{
        QueryPlanDraft, QueryResourceBudget, QueryResourceCoordinator, QueryResultCharge,
        QueryServiceLifecycle, QueryServiceRegistryConfig, QueryServiceResourceConfig, QueryState,
        QuerySubmissionMetadata, QueryTaskCharge, DEFAULT_MAX_PLANNING_DURATION,
    };

    struct TestSnapshot {
        descriptor: QueryReadSnapshotDescriptor,
        drops: Arc<AtomicUsize>,
    }

    struct PanicSnapshot {
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

    impl QueryReadSnapshotGuard for PanicSnapshot {
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

    impl Drop for PanicSnapshot {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
            panic!("test snapshot drop panic");
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct AccountingDropProbe {
        coordinator: QueryResourceCoordinator,
        observed_active_tasks: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for AccountingDropProbe {
        fn drop(&mut self) {
            self.observed_active_tasks
                .store(self.coordinator.status().active_tasks, Ordering::Relaxed);
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct PanicDropProbe(Arc<AtomicUsize>);

    impl Drop for PanicDropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
            panic!("test resource drop panic");
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
            max_planning_duration: DEFAULT_MAX_PLANNING_DURATION,
            plan_ttl: Duration::from_nanos(90),
            max_registered_queries,
            max_query_tombstones,
        }
    }

    fn coordinator(resources: QueryServiceResourceConfig) -> QueryResourceCoordinator {
        QueryResourceCoordinator::with_registry_config(
            resources,
            QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                plan_ttl: Duration::from_nanos(90),
                ..QueryServiceRegistryConfig::default()
            },
        )
        .unwrap()
    }

    fn coordinator_with_planning_duration(
        resources: QueryServiceResourceConfig,
        max_planning_duration: Duration,
    ) -> QueryResourceCoordinator {
        QueryResourceCoordinator::with_registry_config(
            resources,
            QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                max_planning_duration,
                plan_ttl: Duration::from_nanos(90),
                ..QueryServiceRegistryConfig::default()
            },
        )
        .unwrap()
    }

    fn coordinator_with_plan_ttl(
        resources: QueryServiceResourceConfig,
        plan_ttl: Duration,
    ) -> QueryResourceCoordinator {
        QueryResourceCoordinator::with_registry_config(
            resources,
            QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                plan_ttl,
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

    fn draft(query_id: QueryId) -> QueryPlanDraft {
        draft_at_revision(query_id, 1)
    }

    fn draft_at_revision(query_id: QueryId, catalog_revision: u64) -> QueryPlanDraft {
        QueryPlanDraft::new(query_id, catalog_revision, Vec::new())
    }

    fn metadata(query_id: QueryId, deadline_ns: Option<u64>) -> QuerySubmissionMetadata {
        QuerySubmissionMetadata {
            query_id,
            deadline_ns,
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

    fn panic_snapshot(query_id: QueryId, drops: Arc<AtomicUsize>) -> Box<PanicSnapshot> {
        Box::new(PanicSnapshot {
            descriptor: QueryReadSnapshotDescriptor {
                query_id,
                catalog_revision: 1,
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

    fn result_charge(memory_bytes: u64) -> QueryResultCharge {
        QueryResultCharge {
            memory_bytes: NonZeroU64::new(memory_bytes).unwrap(),
        }
    }

    #[test]
    fn submission_and_completion_publish_only_honest_lifecycle_metadata() {
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, Some(90)), budget(), 10)
            .expect("submit before snapshot planning");
        let instance_id = planning.instance_id();

        let planning_status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(planning_status.state, QueryState::Planning);
        assert_eq!(planning_status.submitted_at_ns, 10);
        assert!(planning_status.plan.is_none());
        assert!(planning_status.started_at_ns.is_none());
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        let token = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::clone(&drops)),
                20,
            )
            .expect("atomically bind plan and snapshot");
        assert_eq!(token.instance_id(), instance_id);
        assert_eq!(token.info().planned_at_ns, 20);
        assert_eq!(token.info().expires_at_ns, 90);

        let planned_status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(planned_status.state, QueryState::Planned);
        let plan = planned_status.plan.expect("completed plan metadata");
        assert_eq!(plan.planned_at_ns, 20);
        assert_eq!(plan.expires_at_ns, 90);
        assert_eq!(plan.catalog_revision, 1);
        assert!(plan.snapshots.is_empty());
        assert!(planned_status.started_at_ns.is_none());
        assert_eq!(coordinator.status().active_snapshot_owners, 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 1);

        let execution = coordinator.begin_execution(token, 30).unwrap();
        let running = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(running.state, QueryState::Running);
        assert_eq!(running.started_at_ns, Some(30));
        drop(execution);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn planning_deadline_starts_at_submission_and_wakes_exact_token() {
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, Some(20)), budget(), 10)
            .unwrap();
        let instance_id = planning.instance_id();
        let cancellation = planning.cancellation();

        assert_eq!(coordinator.sweep_expired(19), 0);
        assert_eq!(coordinator.sweep_expired(20), 1);
        assert!(planning.is_cancelled());
        let signal = cancellation
            .terminal_signal()
            .expect("planning timeout publishes terminal signal");
        assert_eq!(signal.state, QueryState::TimedOut);
        assert_eq!(signal.generation, instance_id.generation().get());
        let status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert!(status.plan.is_none());
        assert_eq!(status.finished_at_ns, Some(20));
        assert_eq!(coordinator.status().scheduled_deadlines, 0);

        drop(planning);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), instance_id)
                .unwrap()
                .state,
            QueryState::TimedOut
        );
    }

    #[test]
    fn server_plan_ttl_is_the_only_plan_timestamp_authority() {
        let coordinator = coordinator_with_plan_ttl(
            QueryServiceResourceConfig {
                max_active_queries: 2,
                ..config()
            },
            Duration::from_nanos(10),
        );
        let owner = coordinator.owner(session_id(1));

        for value in [1, 2] {
            let id = query_id(value);
            let planning = coordinator
                .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 10)
                .unwrap();
            let token = coordinator
                .complete_planning_at_for_test(
                    planning,
                    draft(id),
                    snapshot(id, Arc::new(AtomicUsize::new(0))),
                    20,
                )
                .unwrap();
            assert_eq!(token.info().planned_at_ns, 20);
            assert_eq!(token.info().expires_at_ns, 30);
            let plan = coordinator
                .admin_handle()
                .status(token.instance_id())
                .unwrap()
                .plan
                .expect("completed plan status");
            assert_eq!(plan.planned_at_ns, 20);
            assert_eq!(plan.expires_at_ns, 30);
            drop(token);
        }
    }

    #[test]
    fn effective_planning_deadline_uses_the_earlier_request_or_server_limit() {
        let coordinator = coordinator_with_planning_duration(config(), Duration::from_nanos(10));
        let owner = coordinator.owner(session_id(1));

        let request_bounded = coordinator
            .submit_at_for_test(
                owner.as_ref(),
                metadata(query_id(1), Some(15)),
                budget(),
                10,
            )
            .unwrap();
        assert_eq!(coordinator.sweep_expired(14), 0);
        assert_eq!(coordinator.sweep_expired(15), 1);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), request_bounded.instance_id())
                .unwrap()
                .state,
            QueryState::TimedOut
        );

        let server_bounded = coordinator
            .submit_at_for_test(
                owner.as_ref(),
                metadata(query_id(2), Some(100)),
                budget(),
                20,
            )
            .unwrap();
        assert_eq!(coordinator.sweep_expired(29), 0);
        assert_eq!(coordinator.sweep_expired(30), 1);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), server_bounded.instance_id())
                .unwrap()
                .state,
            QueryState::TimedOut
        );

        let default_bounded = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(query_id(3), None), budget(), 40)
            .unwrap();
        assert_eq!(coordinator.sweep_expired(49), 0);
        assert_eq!(coordinator.sweep_expired(50), 1);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), default_bounded.instance_id())
                .unwrap()
                .state,
            QueryState::TimedOut
        );
        assert_eq!(coordinator.status().scheduled_deadlines, 0);
        drop((request_bounded, server_bounded, default_bounded));
    }

    #[test]
    fn completing_planning_installs_request_and_server_plan_deadlines() {
        let coordinator = coordinator_with_planning_duration(config(), Duration::from_nanos(10));
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, Some(50)), budget(), 10)
            .unwrap();
        let instance_id = planning.instance_id();
        let token = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::clone(&drops)),
                15,
            )
            .unwrap();
        assert_eq!(coordinator.status().scheduled_deadlines, 1);
        assert_eq!(token.info().expires_at_ns, 50);

        assert_eq!(coordinator.sweep_expired(20), 0);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), instance_id)
                .unwrap()
                .state,
            QueryState::Planned
        );
        assert_eq!(coordinator.sweep_expired(50), 1);
        let status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(status.state, QueryState::TimedOut);
        assert_eq!(
            status.error.expect("request deadline error").message(),
            "query exceeded its request deadline"
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().scheduled_deadlines, 0);
        drop(token);
    }

    #[test]
    fn owner_cancel_wins_over_late_planning_completion_without_attaching_snapshot() {
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 10)
            .unwrap();
        let instance_id = planning.instance_id();

        coordinator
            .cancel_owned(owner.as_ref(), instance_id)
            .expect("cancel exact planning instance");
        let error = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::clone(&drops)),
                20,
            )
            .err()
            .expect("cancelled planning cannot complete");
        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let status = coordinator
            .status_owned(owner.as_ref(), instance_id)
            .unwrap();
        assert_eq!(status.state, QueryState::Cancelled);
        assert!(status.plan.is_none());
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
    }

    #[test]
    fn invalid_or_expired_completion_is_terminal_and_never_installs_partial_plan_state() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            ..config()
        });
        let owner = coordinator.owner(session_id(1));

        let invalid_id = query_id(1);
        let invalid_drops = Arc::new(AtomicUsize::new(0));
        let invalid = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(invalid_id, None), budget(), 10)
            .unwrap();
        let invalid_instance = invalid.instance_id();
        let invalid_error = coordinator
            .complete_planning_at_for_test(
                invalid,
                draft_at_revision(invalid_id, 1),
                snapshot_at_revision(invalid_id, 2, Arc::clone(&invalid_drops)),
                20,
            )
            .err()
            .expect("mismatched snapshot completion must fail");
        assert_eq!(invalid_error.code(), ErrorCode::InvalidArgument);
        let invalid_status = coordinator
            .status_owned(owner.as_ref(), invalid_instance)
            .unwrap();
        assert_eq!(invalid_status.state, QueryState::Failed);
        assert!(invalid_status.plan.is_none());
        assert_eq!(invalid_drops.load(Ordering::Relaxed), 1);

        let wrong_query_id = query_id(3);
        let wrong_query_drops = Arc::new(AtomicUsize::new(0));
        let wrong_query = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(wrong_query_id, None), budget(), 10)
            .unwrap();
        let wrong_query_error = coordinator
            .complete_planning_at_for_test(
                wrong_query,
                draft(query_id(4)),
                snapshot(wrong_query_id, Arc::clone(&wrong_query_drops)),
                20,
            )
            .err()
            .expect("foreign query draft must fail");
        assert_eq!(wrong_query_error.code(), ErrorCode::InvalidArgument);
        assert_eq!(wrong_query_drops.load(Ordering::Relaxed), 1);

        let wrong_tables_id = query_id(5);
        let wrong_tables_drops = Arc::new(AtomicUsize::new(0));
        let wrong_tables = coordinator
            .submit_at_for_test(
                owner.as_ref(),
                metadata(wrong_tables_id, None),
                budget(),
                10,
            )
            .unwrap();
        let wrong_tables_draft = QueryPlanDraft::new(
            wrong_tables_id,
            1,
            vec![zippy_types::query::QueryTableVersion {
                table_id: zippy_types::TableId::from_u128(1).unwrap(),
                snapshot_version: 1,
                schema_version: 1,
            }],
        );
        let wrong_tables_error = coordinator
            .complete_planning_at_for_test(
                wrong_tables,
                wrong_tables_draft,
                snapshot(wrong_tables_id, Arc::clone(&wrong_tables_drops)),
                20,
            )
            .err()
            .expect("mismatched table versions must fail");
        assert_eq!(wrong_tables_error.code(), ErrorCode::InvalidArgument);
        assert_eq!(wrong_tables_drops.load(Ordering::Relaxed), 1);

        let expired_id = query_id(2);
        let expired_drops = Arc::new(AtomicUsize::new(0));
        let expired = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(expired_id, Some(20)), budget(), 10)
            .unwrap();
        let expired_instance = expired.instance_id();
        let expired_error = coordinator
            .complete_planning_at_for_test(
                expired,
                draft(expired_id),
                snapshot(expired_id, Arc::clone(&expired_drops)),
                20,
            )
            .err()
            .expect("expired plan completion must fail");
        assert_eq!(expired_error.code(), ErrorCode::DeadlineExceeded);
        let expired_status = coordinator
            .status_owned(owner.as_ref(), expired_instance)
            .unwrap();
        assert_eq!(expired_status.state, QueryState::TimedOut);
        assert!(expired_status.plan.is_none());
        assert_eq!(expired_status.finished_at_ns, Some(20));
        assert_eq!(expired_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
    }

    #[test]
    fn fail_and_drop_have_distinct_planning_terminal_semantics() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            ..config()
        });
        let owner = coordinator.owner(session_id(1));
        let failed_id = query_id(1);
        let failed = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(failed_id, None), budget(), 10)
            .unwrap();
        let failed_instance = failed.instance_id();
        coordinator
            .fail_planning(
                failed,
                ServerError::new(
                    ErrorCode::InvalidArgument,
                    "physical planner rejected input",
                )
                .with_retryable(false),
            )
            .unwrap();
        let failed_status = coordinator
            .status_owned(owner.as_ref(), failed_instance)
            .unwrap();
        assert_eq!(failed_status.state, QueryState::Failed);
        assert_eq!(
            failed_status.error.as_ref().map(ServerError::code),
            Some(ErrorCode::InvalidArgument)
        );

        let dropped_id = query_id(2);
        let dropped = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(dropped_id, None), budget(), 20)
            .unwrap();
        let dropped_instance = dropped.instance_id();
        drop(dropped);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), dropped_instance)
                .unwrap()
                .state,
            QueryState::Cancelled
        );
    }

    #[test]
    fn concurrent_cancel_and_complete_have_one_atomic_terminal_outcome() {
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 10)
            .unwrap();
        let instance_id = planning.instance_id();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker_coordinator = coordinator.clone();
        let worker_drops = Arc::clone(&drops);
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            if let Ok(token) = worker_coordinator.complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, worker_drops),
                20,
            ) {
                drop(token);
            }
        });

        barrier.wait();
        coordinator.admin_handle().cancel(instance_id).unwrap();
        worker.join().unwrap();
        let status = coordinator.admin_handle().status(instance_id).unwrap();
        assert_eq!(status.state, QueryState::Cancelled);
        assert!(!status.logical_admitted);
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
        assert_eq!(coordinator.status().scheduled_deadlines, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retained_old_planning_token_cannot_target_reused_query_id() {
        let mut registry = registry_config(4, 4, Duration::from_nanos(1));
        registry.max_planning_duration = Duration::from_nanos(10);
        let coordinator =
            QueryResourceCoordinator::with_registry_config(config(), registry).unwrap();
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let old = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 10)
            .unwrap();
        let old_instance = old.instance_id();
        assert_eq!(coordinator.sweep_expired(20), 1);

        let current = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 30)
            .expect("expired terminal retention permits exact ID reuse");
        let current_instance = current.instance_id();
        assert_ne!(old_instance, current_instance);
        assert_eq!(
            coordinator
                .admin_handle()
                .cancel(old_instance)
                .unwrap_err()
                .code(),
            ErrorCode::NotFound
        );

        drop(old);
        let current_status = coordinator
            .status_owned(owner.as_ref(), current_instance)
            .unwrap();
        assert_eq!(current_status.state, QueryState::Planning);
        assert_eq!(current_status.instance_id(), current_instance);
        assert_eq!(coordinator.sweep_expired(39), 0);
        assert_eq!(coordinator.sweep_expired(40), 1);
        assert_eq!(
            coordinator
                .status_owned(owner.as_ref(), current_instance)
                .unwrap()
                .state,
            QueryState::TimedOut
        );
        drop(current);
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
    fn session_ledger_aggregates_queries_and_removes_idle_sessions() {
        let resources = QueryServiceResourceConfig {
            max_active_queries: 3,
            max_active_tasks: 3,
            max_cpu_slots: 3,
            max_memory_bytes: 300,
        };
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        };
        let coordinator = coordinator(resources);
        let first_session = session_id(1);
        let second_session = session_id(2);
        let first_id = query_id(1);
        let second_id = query_id(2);
        let third_id = query_id(3);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, 100),
                first_session,
                None,
                query_budget,
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned_for_session(
                info(second_id, 100),
                first_session,
                None,
                query_budget,
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let third = coordinator
            .register_planned_for_session(
                info(third_id, 100),
                second_session,
                None,
                query_budget,
                snapshot(third_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();

        assert_eq!(coordinator.status().tracked_sessions, 2);
        assert_eq!(
            coordinator
                .admin_handle()
                .session_status(first_session)
                .active_logical_queries,
            2
        );
        assert_eq!(
            coordinator
                .admin_handle()
                .session_status(second_session)
                .active_logical_queries,
            1
        );

        let first = coordinator.begin_execution(first, 11).unwrap();
        let second = coordinator.begin_execution(second, 11).unwrap();
        let first_task = first.admit_task(charge(1, 20), ()).unwrap();
        let second_task = second.admit_task(charge(1, 30), ()).unwrap();
        let first_usage = coordinator.admin_handle().session_status(first_session);
        assert_eq!(first_usage.active_tasks, 2);
        assert_eq!(first_usage.active_cpu_slots, 2);
        assert_eq!(first_usage.active_task_memory_bytes, 50);
        assert_eq!(coordinator.status().active_task_memory_bytes, 50);

        drop((first_task, second_task));
        drop((first, second));
        assert_eq!(coordinator.status().tracked_sessions, 1);
        drop(third);
        assert_eq!(coordinator.status().tracked_sessions, 0);
        assert_eq!(
            coordinator
                .admin_handle()
                .session_status(first_session)
                .active_memory_bytes,
            0
        );
    }

    #[test]
    fn task_reservation_rolls_back_build_errors_and_panics() {
        let coordinator = coordinator(config());
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), budget(), 10)
            .unwrap();
        let token = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();

        let build_error = execution
            .admit_snapshot_task::<TestSnapshot, ()>(charge(1, 40), |_| {
                Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "test task build failed",
                ))
            })
            .err()
            .expect("task build error must be returned");
        assert_eq!(build_error.code(), ErrorCode::InvalidArgument);
        assert_eq!(coordinator.status().active_tasks, 0);
        assert_eq!(
            coordinator
                .session_status_owned(owner.as_ref())
                .unwrap()
                .active_task_memory_bytes,
            0
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = execution.admit_snapshot_task::<TestSnapshot, ()>(charge(1, 40), |_| {
                panic!("test task build panic")
            });
        }));
        assert!(panic.is_err());
        let service = coordinator.status();
        assert_eq!(service.active_tasks, 0);
        assert_eq!(service.active_cpu_slots, 0);
        assert_eq!(service.active_memory_bytes, 0);
        let query = coordinator
            .status_owned(owner.as_ref(), execution.instance_id())
            .unwrap();
        assert_eq!(query.active_tasks, 0);
        assert_eq!(query.active_task_memory_bytes, 0);
        drop(execution);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn task_and_result_memory_share_query_global_and_session_budget() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 1,
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        });
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        };
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), query_budget, 10)
            .unwrap();
        let token = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let task = execution.admit_task(charge(1, 80), ()).unwrap();
        let rejected_drops = Arc::new(AtomicUsize::new(0));

        let rejection = execution
            .admit_result(result_charge(30), DropProbe(Arc::clone(&rejected_drops)))
            .err()
            .expect("task and result memory must share one hard budget");
        assert_eq!(rejection.code(), ErrorCode::ResourceExhausted);
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_result_permits, 0);

        let accepted_drops = Arc::new(AtomicUsize::new(0));
        let result = execution
            .admit_result(result_charge(20), DropProbe(Arc::clone(&accepted_drops)))
            .unwrap();
        let _ = result.as_ref();
        let service = coordinator.status();
        assert_eq!(service.active_task_memory_bytes, 80);
        assert_eq!(service.active_result_permits, 1);
        assert_eq!(service.active_result_memory_bytes, 20);
        assert_eq!(service.active_memory_bytes, 100);
        let session = coordinator.session_status_owned(owner.as_ref()).unwrap();
        assert_eq!(session.active_task_memory_bytes, 80);
        assert_eq!(session.active_result_memory_bytes, 20);
        assert_eq!(session.active_memory_bytes, 100);

        drop(task);
        drop(result);
        assert_eq!(accepted_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_memory_bytes, 0);
        execution.finish().unwrap();
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn task_to_result_transfer_reclassifies_full_budget_atomically() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 1,
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 80,
        });
        let owner = coordinator.owner(session_id(1));
        let id = query_id(1);
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 80,
        };
        let planning = coordinator
            .submit_at_for_test(owner.as_ref(), metadata(id, None), query_budget, 10)
            .unwrap();
        let token = coordinator
            .complete_planning_at_for_test(
                planning,
                draft(id),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let task = execution
            .admit_task(charge(1, 80), String::from("batch"))
            .unwrap();

        let result = task
            .into_result(result_charge(80), |mut batch| {
                batch.push_str("-ready");
                Ok(batch)
            })
            .expect("same memory may move atomically from task to result accounting");
        assert_eq!(result.as_ref(), "batch-ready");
        let service = coordinator.status();
        assert_eq!(service.active_tasks, 0);
        assert_eq!(service.active_cpu_slots, 0);
        assert_eq!(service.active_task_memory_bytes, 0);
        assert_eq!(service.active_result_permits, 1);
        assert_eq!(service.active_result_memory_bytes, 80);
        assert_eq!(service.active_memory_bytes, 80);
        let query = coordinator
            .status_owned(owner.as_ref(), execution.instance_id())
            .unwrap();
        assert_eq!(query.active_tasks, 0);
        assert_eq!(query.active_result_permits, 1);
        assert_eq!(query.active_memory_bytes, 80);

        drop(result);
        execution.finish().unwrap();
        assert_eq!(coordinator.status().active_memory_bytes, 0);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn rejected_task_to_result_transfer_drops_result_before_task_charge() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        });
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        };
        let first_id = query_id(1);
        let second_id = query_id(2);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, 100),
                session_id(1),
                None,
                query_budget,
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned_for_session(
                info(second_id, 100),
                session_id(2),
                None,
                query_budget,
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let first = coordinator.begin_execution(first, 11).unwrap();
        let second = coordinator.begin_execution(second, 11).unwrap();
        let first_result = first.admit_result(result_charge(10), ()).unwrap();
        let second_task = second.admit_task(charge(1, 20), ()).unwrap();
        let observed_active_tasks = Arc::new(AtomicUsize::new(usize::MAX));
        let result_drops = Arc::new(AtomicUsize::new(0));

        let transfer = second_task.into_result(result_charge(20), |_| {
            Ok(AccountingDropProbe {
                coordinator: coordinator.clone(),
                observed_active_tasks: Arc::clone(&observed_active_tasks),
                drops: Arc::clone(&result_drops),
            })
        });
        let error = match transfer {
            Ok(_) => panic!("full result permit capacity must reject transfer"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(result_drops.load(Ordering::Relaxed), 1);
        assert_eq!(observed_active_tasks.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_tasks, 0);
        assert_eq!(coordinator.status().active_task_memory_bytes, 0);
        assert_eq!(coordinator.status().active_result_permits, 1);

        drop(first_result);
        first.finish().unwrap();
        second.finish().unwrap();
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn result_permit_count_is_bounded_per_query_and_globally() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 3,
            max_active_tasks: 2,
            max_cpu_slots: 3,
            max_memory_bytes: 100,
        });
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        };
        let execution = |id_value: u128, session_value: u128| {
            let id = query_id(id_value);
            let token = coordinator
                .register_planned_for_session(
                    info(id, 100),
                    session_id(session_value),
                    None,
                    query_budget,
                    snapshot(id, Arc::new(AtomicUsize::new(0))),
                    10,
                )
                .unwrap();
            coordinator.begin_execution(token, 11).unwrap()
        };
        let first = execution(1, 1);
        let second = execution(2, 2);
        let third = execution(3, 3);

        let first_result = first.admit_result(result_charge(10), ()).unwrap();
        let per_query = first
            .admit_result(result_charge(10), ())
            .err()
            .expect("one query cannot exceed its task-derived result permit bound");
        assert_eq!(per_query.code(), ErrorCode::ResourceExhausted);
        let second_result = second.admit_result(result_charge(10), ()).unwrap();
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let global = third
            .admit_result(result_charge(10), DropProbe(Arc::clone(&rejected_drops)))
            .err()
            .expect("global result permits cannot exceed active task capacity");
        assert_eq!(global.code(), ErrorCode::ResourceExhausted);
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_result_permits, 2);

        drop((first_result, second_result));
        first.finish().unwrap();
        second.finish().unwrap();
        third.finish().unwrap();
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn finish_rejects_outstanding_result_delivery_and_cancels_on_scope_drop() {
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
        let instance_id = token.instance_id();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let result = execution.admit_result(result_charge(20), ()).unwrap();

        let error = execution
            .finish()
            .expect_err("result delivery must drain before Finished");
        assert_eq!(error.code(), ErrorCode::Conflict);
        let status = coordinator.admin_handle().status(instance_id).unwrap();
        assert_eq!(status.state, QueryState::Cancelled);
        assert!(status.cancellation_requested);
        assert!(result.is_cancelled());
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        drop(result);
        assert_eq!(coordinator.status().active_result_permits, 0);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn execution_failure_preserves_error_and_drains_physical_resources() {
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
        let instance_id = token.instance_id();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let cancellation = execution.cancellation();
        let task = execution.admit_task(charge(1, 20), ()).unwrap();
        let result = execution.admit_result(result_charge(10), ()).unwrap();

        execution
            .fail(
                ServerError::new(ErrorCode::InvalidArgument, "physical operator failed")
                    .with_query_id(query_id(99))
                    .with_retryable(false),
            )
            .unwrap();

        let status = coordinator.admin_handle().status(instance_id).unwrap();
        assert_eq!(status.state, QueryState::Failed);
        assert!(!status.logical_admitted);
        assert!(status.cancellation_requested);
        let error = status.error.expect("execution failure is retained");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert_eq!(error.message(), "physical operator failed");
        assert_eq!(error.query_id(), Some(id));
        let signal = cancellation
            .terminal_signal()
            .expect("execution failure wakes cancellation waiters");
        assert_eq!(signal.state, QueryState::Failed);
        assert_eq!(signal.error, Some(error));
        assert!(task.is_cancelled());
        assert!(result.is_cancelled());
        assert_eq!(coordinator.status().active_tasks, 1);
        assert_eq!(coordinator.status().active_result_permits, 1);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        drop(task);
        assert_eq!(coordinator.status().active_snapshot_owners, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().tracked_sessions, 1);
        drop(result);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn concurrent_execution_failure_and_admin_cancel_keep_first_terminal() {
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
        let instance_id = token.instance_id();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let cancellation = execution.cancellation();
        let barrier = Arc::new(Barrier::new(2));
        let worker_barrier = Arc::clone(&barrier);
        let worker = thread::spawn(move || {
            worker_barrier.wait();
            execution.fail(ServerError::new(
                ErrorCode::InvalidArgument,
                "physical operator failed",
            ))
        });

        barrier.wait();
        coordinator.admin_handle().cancel(instance_id).unwrap();
        let failure_result = worker.join().unwrap();
        let status = coordinator.admin_handle().status(instance_id).unwrap();
        let signal = cancellation
            .terminal_signal()
            .expect("the winning terminal transition is published");
        assert_eq!(signal.state, status.state);
        assert_eq!(signal.error, status.error);
        match status.state {
            QueryState::Failed => {
                failure_result.expect("execution failure won the terminal race");
                assert_eq!(
                    status.error.as_ref().map(ServerError::code),
                    Some(ErrorCode::InvalidArgument)
                );
            }
            QueryState::Cancelled => {
                assert_eq!(
                    failure_result.expect_err("administrator cancellation won the terminal race"),
                    status
                        .error
                        .expect("cancelled query retains its terminal error")
                );
            }
            state => panic!("unexpected terminal state [{state:?}]"),
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn payload_drop_panics_still_release_task_and_result_reservations() {
        let coordinator = coordinator(config());
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let task_drops = Arc::new(AtomicUsize::new(0));
        let task = execution
            .admit_task(charge(1, 20), PanicDropProbe(Arc::clone(&task_drops)))
            .unwrap();
        let task_panic = catch_unwind(AssertUnwindSafe(|| drop(task)));
        assert!(task_panic.is_err());
        assert_eq!(task_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_tasks, 0);
        assert_eq!(coordinator.status().active_task_memory_bytes, 0);

        let result_drops = Arc::new(AtomicUsize::new(0));
        let result = execution
            .admit_result(result_charge(20), PanicDropProbe(Arc::clone(&result_drops)))
            .unwrap();
        let result_panic = catch_unwind(AssertUnwindSafe(|| drop(result)));
        assert!(result_panic.is_err());
        assert_eq!(result_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_result_permits, 0);
        assert_eq!(coordinator.status().active_result_memory_bytes, 0);

        execution.finish().unwrap();
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn snapshot_drop_panic_still_releases_owner_accounting() {
        let coordinator = coordinator(config());
        let drops = Arc::new(AtomicUsize::new(0));
        let id = query_id(1);
        let token = coordinator
            .register_planned_for_session(
                info(id, 100),
                session_id(1),
                None,
                budget(),
                panic_snapshot(id, Arc::clone(&drops)),
                10,
            )
            .unwrap();

        let panic = catch_unwind(AssertUnwindSafe(|| drop(token)));
        assert!(panic.is_err());
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        let status = coordinator.status();
        assert_eq!(status.active_logical_queries, 0);
        assert_eq!(status.active_snapshot_owners, 0);
        assert_eq!(status.tracked_sessions, 0);
    }

    #[test]
    fn task_and_result_memory_block_each_other_across_queries_and_sessions() {
        let coordinator = coordinator(QueryServiceResourceConfig {
            max_active_queries: 2,
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes: 100,
        });
        let query_budget = QueryResourceBudget {
            max_active_tasks: 1,
            max_cpu_slots: 1,
            max_memory_bytes: 100,
        };
        let first_id = query_id(1);
        let second_id = query_id(2);
        let first = coordinator
            .register_planned_for_session(
                info(first_id, 100),
                session_id(1),
                None,
                query_budget,
                snapshot(first_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let second = coordinator
            .register_planned_for_session(
                info(second_id, 100),
                session_id(2),
                None,
                query_budget,
                snapshot(second_id, Arc::new(AtomicUsize::new(0))),
                10,
            )
            .unwrap();
        let first_instance = first.instance_id();
        let second_instance = second.instance_id();
        let first = coordinator.begin_execution(first, 11).unwrap();
        let second = coordinator.begin_execution(second, 11).unwrap();

        let first_result = first.admit_result(result_charge(80), ()).unwrap();
        let task_error = second
            .admit_task(charge(1, 30), ())
            .err()
            .expect("another query result must consume global task memory budget");
        assert_eq!(task_error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(coordinator.status().active_task_memory_bytes, 0);
        assert_eq!(coordinator.status().active_result_memory_bytes, 80);

        let second_small_task = second.admit_task(charge(1, 20), ()).unwrap();
        let service = coordinator.status();
        let first_query = coordinator.admin_handle().status(first_instance).unwrap();
        let second_query = coordinator.admin_handle().status(second_instance).unwrap();
        let first_session = coordinator.admin_handle().session_status(session_id(1));
        let second_session = coordinator.admin_handle().session_status(session_id(2));
        assert_eq!(service.active_logical_queries, 2);
        assert_eq!(service.active_tasks, 1);
        assert_eq!(service.active_cpu_slots, 1);
        assert_eq!(service.active_task_memory_bytes, 20);
        assert_eq!(service.active_result_permits, 1);
        assert_eq!(service.active_result_memory_bytes, 80);
        assert_eq!(service.active_memory_bytes, 100);
        assert_eq!(
            service.active_logical_queries,
            first_session.active_logical_queries + second_session.active_logical_queries
        );
        assert_eq!(
            service.active_tasks,
            first_session.active_tasks + second_session.active_tasks
        );
        assert_eq!(
            service.active_cpu_slots,
            first_session.active_cpu_slots + second_session.active_cpu_slots
        );
        assert_eq!(
            service.active_task_memory_bytes,
            first_session.active_task_memory_bytes + second_session.active_task_memory_bytes
        );
        assert_eq!(
            service.active_result_permits,
            first_session.active_result_permits + second_session.active_result_permits
        );
        assert_eq!(
            service.active_result_memory_bytes,
            first_session.active_result_memory_bytes + second_session.active_result_memory_bytes
        );
        assert_eq!(
            service.active_memory_bytes,
            first_query.active_memory_bytes + second_query.active_memory_bytes
        );
        assert_eq!(first_query.active_result_memory_bytes, 80);
        assert_eq!(second_query.active_task_memory_bytes, 20);

        drop(second_small_task);
        drop(first_result);
        let second_task = second.admit_task(charge(1, 80), ()).unwrap();
        let rejected_drops = Arc::new(AtomicUsize::new(0));
        let result_error = first
            .admit_result(result_charge(30), DropProbe(Arc::clone(&rejected_drops)))
            .err()
            .expect("another query task must consume global result memory budget");
        assert_eq!(result_error.code(), ErrorCode::ResourceExhausted);
        assert_eq!(rejected_drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_task_memory_bytes, 80);
        assert_eq!(coordinator.status().active_result_memory_bytes, 0);

        drop(second_task);
        first.finish().unwrap();
        second.finish().unwrap();
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn result_permit_blocks_terminal_eviction_until_its_memory_is_released() {
        let coordinator = QueryResourceCoordinator::with_registry_config(
            config(),
            registry_config(1, 1, Duration::from_secs(60)),
        )
        .unwrap();
        let id = query_id(1);
        let drops = Arc::new(AtomicUsize::new(0));
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
        let instance_id = token.instance_id();
        let execution = coordinator.begin_execution(token, 11).unwrap();
        let result = execution.admit_result(result_charge(20), ()).unwrap();
        coordinator.admin_handle().cancel(instance_id).unwrap();
        drop(execution);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.status().active_result_permits, 1);

        let replacement_owner = coordinator.owner(session_id(2));
        let blocked = coordinator
            .submit_at_for_test(
                replacement_owner.as_ref(),
                metadata(query_id(2), None),
                budget(),
                20,
            )
            .err()
            .expect("terminal query with a result permit must not be evicted");
        assert_eq!(blocked.code(), ErrorCode::ResourceExhausted);
        assert_eq!(coordinator.status().query_tombstones, 0);

        drop(result);
        assert_eq!(coordinator.status().tracked_sessions, 0);
        let replacement = coordinator
            .submit_at_for_test(
                replacement_owner.as_ref(),
                metadata(query_id(2), None),
                budget(),
                21,
            )
            .expect("fully drained terminal query may be capacity-evicted");
        assert_eq!(coordinator.status().query_tombstones, 1);
        assert_eq!(coordinator.status().tracked_sessions, 1);
        drop(replacement);
        assert_eq!(coordinator.status().tracked_sessions, 0);
    }

    #[test]
    fn shutdown_waits_for_result_permits_without_retaining_snapshot() {
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
        let result = execution.admit_result(result_charge(20), ()).unwrap();

        let draining = coordinator.begin_shutdown();
        assert_eq!(draining.lifecycle, QueryServiceLifecycle::Draining);
        assert_eq!(draining.active_logical_queries, 0);
        assert_eq!(draining.active_result_permits, 1);
        assert_eq!(draining.active_result_memory_bytes, 20);
        assert_eq!(draining.active_snapshot_owners, 0);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(result.is_cancelled());
        let cancellation = result.cancellation();
        assert!(cancellation.terminal_signal().is_some());
        assert!(cancellation.is_cancelled());
        drop(execution);
        assert_eq!(
            coordinator.status().lifecycle,
            QueryServiceLifecycle::Draining
        );

        drop(result);
        assert_eq!(coordinator.status().active_result_permits, 0);
        assert_eq!(coordinator.status().tracked_sessions, 0);
        assert_eq!(
            coordinator.status().lifecycle,
            QueryServiceLifecycle::Stopped
        );
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
        let retry_id = query_id(3);
        let replacement = coordinator
            .register_planned_for_session(
                info(retry_id, 100),
                session_id(1),
                None,
                budget(),
                snapshot(retry_id, Arc::clone(&replacement_drops)),
                10,
            )
            .expect("released snapshot owner restores replacement admission");
        drop(replacement);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn ttl_revokes_unexecuted_snapshot_even_when_plan_token_is_retained() {
        let coordinator = coordinator_with_plan_ttl(config(), Duration::from_nanos(10));
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
                max_planning_duration: DEFAULT_MAX_PLANNING_DURATION,
                plan_ttl: Duration::from_millis(20),
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
