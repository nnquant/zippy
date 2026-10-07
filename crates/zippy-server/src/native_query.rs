//! Public native query engine backed by snapshot-bound DataFusion execution.

use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use zippy_query::{
    PersistedPlanLimits, PersistedPlanMetrics, PersistedTableBinding, QueryOperatorMemoryStatus,
    QueryProcessMemoryPool, QueryProcessMemoryStatus, QuerySessionPolicy,
};
use zippy_types::query::{
    OpaqueQueryExecutionToken, OpaqueQueryOwnerCapability, QueryEngine, QueryExecutionToken,
    QueryInput, QueryInstanceId, QueryOwnerCapability, QueryPlanInfo, QueryRequest,
    QueryResultBatch, QueryStatus, RecordBatchStream, SendableRecordBatchStream,
};
use zippy_types::table::TableRef;
use zippy_types::{BoxFuture, ErrorCode, ResourceLimit, Result, ServerError, SessionId, TableId};

use crate::query_execution::{
    NativeExecutionLease, NativeQueryBatch, NativeQueryExecutionConfig, NativeQueryResultStream,
};
use crate::query_service::{
    QueryExecutionScope, QueryPlanningToken, QueryResourceBudget, QueryResourceCoordinator,
    QueryRuntimeStatus, QuerySubmissionMetadata,
};
use crate::query_snapshot::{
    NativeKeyValueSnapshotOutput, NativeQueryReadSnapshot, NativeQuerySnapshotCoordinator,
};

static NEXT_NATIVE_ENGINE_NONCE: AtomicU64 = AtomicU64::new(1);

/// One explicit SQL name mapped to a Catalog table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeQueryBinding {
    sql_name: String,
    table: TableRef,
}

impl NativeQueryBinding {
    /// Creates a native SQL binding.
    pub fn try_new(sql_name: impl Into<String>, table: TableRef) -> Result<Self> {
        let sql_name = sql_name.into();
        PersistedTableBinding::try_new(sql_name.clone(), 0).map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("native query binding is invalid error=[{error}]"),
            )
            .with_retryable(false)
        })?;
        Ok(Self { sql_name, table })
    }

    /// Returns the SQL-visible relation name.
    #[must_use]
    pub fn sql_name(&self) -> &str {
        &self.sql_name
    }

    /// Returns the Catalog table reference resolved for this binding.
    #[must_use]
    pub const fn table(&self) -> &TableRef {
        &self.table
    }
}

/// One server-planned native SQL explanation with no result execution.
#[derive(Debug, Clone)]
pub struct NativeQueryExplanation {
    /// Exact query incarnation used for the bounded planning operation.
    pub instance_id: QueryInstanceId,
    /// Snapshot and Catalog metadata fixed by native planning.
    pub plan: QueryPlanInfo,
    /// Output Arrow schema of the prepared physical plan.
    pub output_schema: SchemaRef,
    /// Stable plan and physical-operator counters.
    pub metrics: BTreeMap<String, u64>,
}

/// One non-cloneable native query whose snapshot, physical plan, and output
/// schema are fixed but whose result operators have not started producing rows.
///
/// Dropping this value cancels the query and releases its snapshot and resource
/// leases. [`NativePreparedQuery::into_stream`] is the only execution path.
pub struct NativePreparedQuery {
    instance_id: QueryInstanceId,
    info: QueryPlanInfo,
    schema: SchemaRef,
    lease: NativeExecutionLease,
    policy: QuerySessionPolicy,
    execution: NativeQueryExecutionConfig,
    process_memory: QueryProcessMemoryPool,
    snapshot_output: Option<Arc<NativeKeyValueSnapshotOutput>>,
}

impl NativePreparedQuery {
    /// Returns immutable metadata for the exact planned snapshot.
    #[must_use]
    pub const fn info(&self) -> &QueryPlanInfo {
        &self.info
    }

    /// Returns the exact process-local query incarnation.
    #[must_use]
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    /// Returns the output Arrow schema fixed by physical preparation.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Consumes this one-use handle and starts the native result stream.
    pub fn into_stream(self) -> Result<SendableRecordBatchStream> {
        let Self {
            instance_id,
            info: _,
            schema,
            lease,
            policy,
            execution,
            process_memory,
            snapshot_output,
        } = self;
        let plan_metrics = lease.plan_metrics();
        let stream = lease.execute_in(policy, execution, &process_memory)?;
        Ok(Box::pin(NativeRecordBatchStream::new(
            instance_id,
            schema,
            stream,
            plan_metrics,
            snapshot_output,
        )))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NativeQueryEngineConfig {
    policy: QuerySessionPolicy,
    default_memory_bytes: NonZeroU64,
    default_operator_memory_bytes: NonZeroU64,
    max_active_tasks: usize,
    max_cpu_slots: u32,
    plan: PersistedPlanLimits,
}

impl NativeQueryEngineConfig {
    pub(crate) fn try_new(
        parallelism: usize,
        max_active_tasks: usize,
        max_cpu_slots: u32,
        max_memory_bytes: u64,
        default_operator_memory_bytes: u64,
    ) -> Result<Self> {
        let policy = QuerySessionPolicy::new(
            NonZeroUsize::new(parallelism)
                .ok_or_else(|| invalid("native query parallelism must be positive"))?,
        );
        if parallelism > max_active_tasks
            || parallelism > usize::try_from(max_cpu_slots).unwrap_or(usize::MAX)
        {
            return Err(invalid(
                "native query parallelism exceeds its task or CPU budget",
            ));
        }
        let default_memory_bytes = NonZeroU64::new(max_memory_bytes)
            .ok_or_else(|| invalid("native query memory budget must be positive"))?;
        let default_operator_memory_bytes = NonZeroU64::new(default_operator_memory_bytes)
            .ok_or_else(|| invalid("native query operator memory must be positive"))?;
        if default_operator_memory_bytes > default_memory_bytes {
            return Err(invalid(
                "native query operator memory exceeds its default query budget",
            ));
        }
        let plan = PersistedPlanLimits::try_new(16 * 1024, 32, 64, 512, 1024, 4096)
            .map_err(|error| invalid(format!("native query plan limits are invalid: {error}")))?;
        Ok(Self {
            policy,
            default_memory_bytes,
            default_operator_memory_bytes,
            max_active_tasks,
            max_cpu_slots,
            plan,
        })
    }
}

struct NativeQueryEngineInner {
    nonce: u64,
    config: NativeQueryEngineConfig,
    snapshots: NativeQuerySnapshotCoordinator,
    resources: QueryResourceCoordinator,
    process_memory: QueryProcessMemoryPool,
}

/// Explicit resource and table boundary for a reusable fixed snapshot.
#[derive(Debug, Clone)]
pub struct NativeFixedSnapshotRequest {
    /// Fresh caller identity for the resource reservation.
    pub query_id: zippy_types::QueryId,
    /// Table references captured together, including persisted files and live rows.
    pub tables: Vec<TableRef>,
    /// Mandatory absolute UTC expiry in Unix nanoseconds.
    pub expires_at_ns: u64,
    /// QueryService memory ceiling for the snapshot reservation.
    pub memory_limit_bytes: ResourceLimit,
}

/// A bounded, immutable multi-table snapshot reusable by its owning server session.
///
/// Every query forks separately admitted manifests. Dropping this handle prevents future use;
/// already-prepared queries retain the original GC pins until their readers finish. The handle
/// is process-local and cannot be restored after a server restart.
///
/// :example: engine.capture_fixed_snapshot(owner, request).await?
pub struct NativeFixedSnapshot {
    engine_nonce: u64,
    session_id: SessionId,
    expires_at_ns: u64,
    tables: Vec<(TableRef, TableId)>,
    info: QueryPlanInfo,
    execution: QueryExecutionScope,
}

impl NativeFixedSnapshot {
    /// Returns the exact registered query identity owning this snapshot's resource reservation.
    #[must_use]
    pub fn instance_id(&self) -> QueryInstanceId {
        self.execution.instance_id()
    }

    /// Borrows immutable table versions and the Catalog revision captured by this handle.
    #[must_use]
    pub const fn info(&self) -> &QueryPlanInfo {
        &self.info
    }

    /// Returns the absolute UTC expiry; expiration never refreshes the observed data.
    #[must_use]
    pub const fn expires_at_ns(&self) -> u64 {
        self.expires_at_ns
    }

    /// Returns whether expiry or cancellation has revoked admission of further queries.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        unix_time_ns() >= self.expires_at_ns || self.execution.cancellation().is_cancelled()
    }

    /// Finishes the snapshot reservation, retaining any pins owned by already-forked queries.
    pub fn close(self) -> Result<()> {
        self.execution.finish()
    }

    fn ensure_active(&self) -> Result<()> {
        let cancellation = self.execution.cancellation();
        if let Some(terminal) = cancellation.terminal_signal() {
            return Err(terminal.error.unwrap_or_else(|| {
                ServerError::new(ErrorCode::Cancelled, "fixed snapshot was cancelled")
            }));
        }
        if cancellation.is_cancelled() {
            return Err(ServerError::new(
                ErrorCode::Cancelled,
                "fixed snapshot was cancelled",
            ));
        }
        if unix_time_ns() >= self.expires_at_ns {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "fixed snapshot expired",
            ));
        }
        Ok(())
    }

    fn fork(
        &self,
        planning: &QueryPlanningToken,
        tables: &[TableRef],
    ) -> Result<NativeQueryReadSnapshot> {
        self.ensure_active()?;
        let requested = tables
            .iter()
            .map(|reference| {
                self.tables
                    .iter()
                    .find_map(|(captured, id)| {
                        (captured == reference
                            || matches!(reference, TableRef::Id { table_id } if table_id == id))
                        .then_some(*id)
                    })
                    .ok_or_else(|| invalid("query table is outside the fixed snapshot"))
            })
            .collect::<Result<Vec<_>>>()?;
        // Never reserve resources while holding the source snapshot ownership mutex.
        let charge = self
            .execution
            .with_snapshot::<NativeQueryReadSnapshot, _>(|snapshot| snapshot.retained_charge())?;
        let memory = planning.reserve_snapshot_memory(charge)?;
        self.execution
            .with_snapshot::<NativeQueryReadSnapshot, _>(|snapshot| {
                self.ensure_active()?;
                snapshot.fork_for_query(planning, &requested, memory)
            })
    }
}

/// Server-owned native SQL planner, lifecycle registry, and DataFusion executor.
#[derive(Clone)]
pub struct NativeQueryEngine {
    inner: Arc<NativeQueryEngineInner>,
}

impl NativeQueryEngine {
    pub(crate) fn new(
        config: NativeQueryEngineConfig,
        snapshots: NativeQuerySnapshotCoordinator,
        resources: QueryResourceCoordinator,
        process_memory: QueryProcessMemoryPool,
    ) -> Self {
        Self {
            inner: Arc::new(NativeQueryEngineInner {
                nonce: NEXT_NATIVE_ENGINE_NONCE.fetch_add(1, Ordering::Relaxed),
                config,
                snapshots,
                resources,
                process_memory,
            }),
        }
    }

    /// Issues a non-serializable owner capability for one client session.
    #[must_use]
    pub fn owner(&self, session_id: SessionId) -> OpaqueQueryOwnerCapability {
        self.inner.resources.owner(session_id)
    }

    pub(crate) fn cancel_authenticated_session(
        &self,
        session_id: SessionId,
        instance_id: QueryInstanceId,
    ) -> Result<()> {
        self.inner
            .resources
            .cancel_authenticated_session(session_id, instance_id)
    }

    /// Pins one finite, ordered native event window without SQL interpretation or state collapse.
    pub async fn prepare_log_window(
        &self,
        owner: &dyn QueryOwnerCapability,
        request: crate::native_log::NativeLogRequest,
    ) -> Result<crate::native_log::NativeLogWindow> {
        use zippy_types::query::QueryPriority;
        let resources = QueryRequest {
            query_id: request.query_id,
            session_id: owner.session_id(),
            input: QueryInput::Sql(String::new()),
            tables: Vec::new(),
            priority: QueryPriority::Normal,
            deadline_ns: request.deadline_ns,
            memory_limit_bytes: request.memory_limit_bytes,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: request.batch_rows,
        };
        let (policy, budget, _) = self.resolve_resources(&resources)?;
        let planning = self.inner.resources.submit(
            owner,
            QuerySubmissionMetadata {
                query_id: request.query_id,
                deadline_ns: request.deadline_ns,
            },
            budget,
        )?;
        let instance_id = planning.instance_id();
        let pinned: Result<_> = async {
            let admission = self.inner.snapshots.admit_event_window(&planning).await?;
            let mut snapshot = self
                .inner
                .snapshots
                .pin_persisted(&planning, &[request.table])
                .await?;
            let plan = snapshot.log_plan(&planning, request.start)?;
            Ok((admission, snapshot, plan))
        }
        .await;
        let (_admission, snapshot, plan) = match pinned {
            Ok(pinned) => pinned,
            Err(error) => return Err(fail_planning(&self.inner.resources, planning, error)),
        };
        let draft = snapshot.plan_draft();
        let token = self
            .inner
            .resources
            .complete_planning(planning, draft, snapshot)?;
        let info = token.info().clone();
        let execution = self
            .inner
            .resources
            .begin_execution(token, unix_time_ns())?;
        Ok(crate::native_log::NativeLogWindow {
            instance_id,
            info,
            plan,
            execution,
            coordinator: self.inner.snapshots.clone(),
            batch_rows: policy.batch_size(),
        })
    }

    /// Opens an event subscription whose windows share one resource and cancellation identity.
    pub async fn subscribe_log(
        &self,
        owner: &dyn QueryOwnerCapability,
        request: crate::native_log::NativeLogRequest,
    ) -> Result<crate::native_log::NativeLogSubscription> {
        let start = request.start;
        self.prepare_log_window(owner, request)
            .await?
            .follow(start)
            .await
    }

    /// Captures a fixed persisted/live boundary with an explicit deadline and resource owner.
    ///
    /// :param owner: Authenticated session capability from this engine.
    /// :param request: Fresh query identity, table references, budget and mandatory expiry.
    /// :returns: Exclusive snapshot handle that can be shared with Arc by a bounded lease store.
    /// :raises ServerError: If ownership, deadline, table resolution, or resource admission fails.
    pub async fn capture_fixed_snapshot(
        &self,
        owner: &dyn QueryOwnerCapability,
        request: NativeFixedSnapshotRequest,
    ) -> Result<NativeFixedSnapshot> {
        let expires_at_ns = request.expires_at_ns;
        if request.tables.is_empty() || request.tables.len() > self.inner.config.plan.max_bindings()
        {
            return Err(invalid(
                "fixed snapshot requires a bounded nonempty table set",
            ));
        }
        let resources = QueryRequest {
            query_id: request.query_id,
            session_id: owner.session_id(),
            input: QueryInput::Sql(String::new()),
            tables: Vec::new(),
            priority: zippy_types::query::QueryPriority::Normal,
            deadline_ns: Some(expires_at_ns),
            memory_limit_bytes: request.memory_limit_bytes,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: None,
        };
        let (_, budget, _) = self.resolve_resources(&resources)?;
        let planning = self.inner.resources.submit(
            owner,
            QuerySubmissionMetadata {
                query_id: request.query_id,
                deadline_ns: Some(expires_at_ns),
            },
            budget,
        )?;
        let pinned: Result<_> = async {
            let mut snapshot = self
                .inner
                .snapshots
                .pin_persisted(&planning, &request.tables)
                .await?;
            snapshot.compact_memory(&planning)?;
            Ok(snapshot)
        }
        .await;
        let snapshot = match pinned {
            Ok(snapshot) => snapshot,
            Err(error) => return Err(fail_planning(&self.inner.resources, planning, error)),
        };
        let tables = request
            .tables
            .into_iter()
            .zip(snapshot.requested_table_ids().iter().copied())
            .collect();
        let token =
            self.inner
                .resources
                .complete_planning(planning, snapshot.plan_draft(), snapshot)?;
        let info = token.info().clone();
        let execution = self
            .inner
            .resources
            .begin_execution(token, unix_time_ns())?;
        Ok(NativeFixedSnapshot {
            engine_nonce: self.inner.nonce,
            session_id: owner.session_id(),
            expires_at_ns,
            tables,
            info,
            execution,
        })
    }

    /// Prepares SQL against an existing fixed boundary without resolving current Catalog names.
    ///
    /// :param snapshot: Live snapshot issued by this engine for the same session.
    /// :param owner: Authenticated query owner.
    /// :param request: Independently identified and budgeted SQL request.
    /// :param bindings: Names or captured IDs selecting tables within the snapshot.
    /// :returns: Prepared query whose pins outlive release of the fixed snapshot handle.
    /// :raises ServerError: If the snapshot is expired, foreign, or lacks a requested table.
    pub fn prepare_fixed<'a>(
        &'a self,
        snapshot: &'a NativeFixedSnapshot,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<NativePreparedQuery>> {
        Box::pin(async move {
            if snapshot.engine_nonce != self.inner.nonce
                || snapshot.session_id != owner.session_id()
            {
                return Err(permission(
                    request.query_id,
                    "fixed snapshot belongs to another engine or session",
                ));
            }
            let token = self
                .plan_bound_inner(owner, request, bindings, false, Some(snapshot))
                .await?;
            self.prepare_token(transfer_native_token(token)?).await
        })
    }

    /// Plans SQL using explicit relation-to-table bindings.
    pub fn plan_bound<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<OpaqueQueryExecutionToken>> {
        Box::pin(async move {
            self.plan_bound_inner(owner, request, bindings, false, None)
                .await
        })
    }

    /// Plans and physically prepares SQL against one exact snapshot without
    /// starting result operators.
    pub fn prepare_bound<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<NativePreparedQuery>> {
        Box::pin(async move {
            let token = self
                .plan_bound_inner(owner, request, bindings, false, None)
                .await?;
            let transferred = transfer_native_token(token)?;
            self.prepare_token(transferred).await
        })
    }

    /// Prepares one complete key-value state and its cursor from a single pinned boundary.
    pub fn prepare_key_value_snapshot<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<NativePreparedQuery>> {
        Box::pin(async move {
            if request.input
                != QueryInput::Sql(zippy_protocol::query::KEY_VALUE_SNAPSHOT_SQL.to_owned())
                || bindings.len() != 1
                || bindings[0].sql_name() != zippy_protocol::query::KEY_VALUE_SNAPSHOT_BINDING
            {
                return Err(invalid(
                    "state snapshot requires the canonical complete-state query",
                ));
            }
            let token = self
                .plan_bound_inner(owner, request, bindings, true, None)
                .await?;
            self.prepare_token(transfer_native_token(token)?).await
        })
    }

    /// Prepares and explains SQL using explicit bindings without executing result operators.
    pub fn explain_bound<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<NativeQueryExplanation>> {
        Box::pin(async move {
            let token = self
                .plan_bound_inner(owner, request, bindings, false, None)
                .await?;
            let plan = token.info().clone();
            let transferred = transfer_native_token(token)?;
            let instance_id = transferred.instance_id;
            let lease = self.prepare_transferred(transferred).await?;
            let output_schema = lease.plan().schema();
            let mut metrics = BTreeMap::new();
            lease.plan_metrics().visit_stable_metrics(|name, value| {
                metrics.insert(name.to_owned(), value);
            });
            lease.finish()?;
            Ok(NativeQueryExplanation {
                instance_id,
                plan,
                output_schema,
                metrics,
            })
        })
    }

    /// Returns process-wide DataFusion operator-memory counters.
    #[must_use]
    pub fn process_memory_status(&self) -> QueryProcessMemoryStatus {
        self.inner.process_memory.status()
    }

    /// Cancels one exact query with server-internal administrator authority.
    pub fn cancel_administrator(&self, instance_id: QueryInstanceId) -> Result<()> {
        self.inner.resources.admin_handle().cancel(instance_id)
    }

    async fn plan_bound_inner(
        &self,
        owner: &dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
        key_value_snapshot: bool,
        fixed_snapshot: Option<&NativeFixedSnapshot>,
    ) -> Result<OpaqueQueryExecutionToken> {
        if owner.session_id() != request.session_id {
            return Err(permission(
                request.query_id,
                "query owner session differs from the request session",
            ));
        }
        let sql = match &request.input {
            QueryInput::Sql(sql) if !sql.trim().is_empty() => sql.clone(),
            QueryInput::Sql(_) => {
                return Err(
                    invalid("native query SQL must not be empty").with_query_id(request.query_id)
                );
            }
            QueryInput::LogicalPlan(_) => {
                return Err(ServerError::new(
                    ErrorCode::UnsupportedPlan,
                    "native logical-plan envelopes are not enabled before protocol Phase 6",
                )
                .with_query_id(request.query_id)
                .with_retryable(false));
            }
        };
        let (policy, budget, execution) = self.resolve_resources(&request)?;
        let (tables, sql_names) = self.validate_bindings(request.query_id, &bindings)?;
        let planning = self.inner.resources.submit(
            owner,
            QuerySubmissionMetadata {
                query_id: request.query_id,
                deadline_ns: request.deadline_ns,
            },
            budget,
        )?;
        let instance_id = planning.instance_id();
        let pinned = match fixed_snapshot {
            Some(snapshot) => snapshot.fork(&planning, &tables),
            None => self.inner.snapshots.pin_persisted(&planning, &tables).await,
        };
        let snapshot = match pinned {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(fail_planning(&self.inner.resources, planning, error));
            }
        };
        let ordinals = match snapshot.requested_table_ordinals() {
            Ok(ordinals) => ordinals,
            Err(error) => {
                return Err(fail_planning(&self.inner.resources, planning, error));
            }
        };
        if ordinals.len() != sql_names.len() {
            return Err(fail_planning(
                &self.inner.resources,
                planning,
                internal(
                    request.query_id,
                    "native query binding count differs from resolved table ordinals",
                ),
            ));
        }
        let persisted_bindings = match sql_names
            .into_iter()
            .zip(ordinals)
            .map(|(sql_name, ordinal)| {
                PersistedTableBinding::try_new(sql_name, ordinal).map_err(|error| {
                    internal(
                        request.query_id,
                        format!("validated native query binding could not be rebuilt: {error}"),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(bindings) => bindings,
            Err(error) => {
                return Err(fail_planning(&self.inner.resources, planning, error));
            }
        };
        let snapshot_output = if key_value_snapshot {
            match snapshot.key_value_snapshot_output(&planning) {
                Ok(output) => Some(output),
                Err(error) => return Err(fail_planning(&self.inner.resources, planning, error)),
            }
        } else {
            None
        };
        let draft = snapshot.plan_draft();
        let inner_token = self
            .inner
            .resources
            .complete_planning(planning, draft, snapshot)?;
        let info = inner_token.info().clone();
        Ok(Box::new(NativePlanToken {
            engine_nonce: self.inner.nonce,
            owner_session_id: request.session_id,
            instance_id,
            info,
            inner_token: Some(inner_token),
            snapshot_output,
            sql,
            bindings: persisted_bindings,
            policy,
            execution,
        }))
    }

    fn validate_bindings(
        &self,
        query_id: zippy_types::QueryId,
        bindings: &[NativeQueryBinding],
    ) -> Result<(Vec<TableRef>, Vec<String>)> {
        if bindings.is_empty() || bindings.len() > self.inner.config.plan.max_bindings() {
            return Err(
                invalid("native query requires a bounded non-empty binding set")
                    .with_query_id(query_id),
            );
        }
        let mut names = HashSet::with_capacity(bindings.len());
        let mut tables = Vec::with_capacity(bindings.len());
        let mut sql_names = Vec::with_capacity(bindings.len());
        for binding in bindings {
            if !names.insert(binding.sql_name.clone()) {
                return Err(invalid(format!(
                    "native query binding name is duplicated name=[{}]",
                    binding.sql_name
                ))
                .with_query_id(query_id));
            }
            tables.push(binding.table.clone());
            sql_names.push(binding.sql_name.clone());
        }
        Ok((tables, sql_names))
    }

    fn resolve_resources(
        &self,
        request: &QueryRequest,
    ) -> Result<(
        QuerySessionPolicy,
        QueryResourceBudget,
        NativeQueryExecutionConfig,
    )> {
        if matches!(request.spill_limit_bytes, ResourceLimit::Bytes(_)) {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "quota-enforced native query spill is unavailable in DataFusion 44",
            )
            .with_query_id(request.query_id)
            .with_retryable(false));
        }
        let memory_bytes = request
            .memory_limit_bytes
            .resolve_bytes(self.inner.config.default_memory_bytes)
            .ok_or_else(|| {
                invalid("native query memory cannot be disabled").with_query_id(request.query_id)
            })?;
        if memory_bytes > self.inner.config.default_memory_bytes {
            return Err(
                invalid("native query memory override exceeds the process query budget")
                    .with_query_id(request.query_id),
            );
        }
        let batch_size =
            request
                .result_batch_rows
                .map_or(self.inner.config.policy.batch_size(), |rows| {
                    NonZeroUsize::new(usize::try_from(rows.get()).unwrap_or(usize::MAX))
                        .expect("nonzero u32 result batch rows fit a nonzero usize")
                });
        let policy = self.inner.config.policy.with_batch_size(batch_size);
        let operator_memory_bytes = NonZeroU64::new(
            self.inner
                .config
                .default_operator_memory_bytes
                .get()
                .min(memory_bytes.get().saturating_div(2).max(1)),
        )
        .expect("bounded operator memory is nonzero");
        Ok((
            policy,
            QueryResourceBudget {
                max_active_tasks: self.inner.config.max_active_tasks,
                max_cpu_slots: self.inner.config.max_cpu_slots,
                max_memory_bytes: memory_bytes.get(),
            },
            NativeQueryExecutionConfig::new(operator_memory_bytes),
        ))
    }

    async fn prepare_transferred(
        &self,
        transferred: NativeTransferredToken,
    ) -> Result<NativeExecutionLease> {
        if transferred.engine_nonce != self.inner.nonce {
            return Err(
                invalid("native query token belongs to another engine instance")
                    .with_query_id(transferred.instance_id.query_id()),
            );
        }
        let execution = self
            .inner
            .resources
            .begin_execution(transferred.inner_token, unix_time_ns())?;
        let manifest = match self.inner.snapshots.project_persisted_snapshot(&execution) {
            Ok(manifest) => manifest,
            Err(error) => return Err(fail_execution(execution, error)),
        };
        NativeExecutionLease::prepare(
            self.inner.snapshots.clone(),
            execution,
            manifest,
            transferred.policy,
            &transferred.sql,
            &transferred.bindings,
            self.inner.config.plan,
        )
        .await
    }

    async fn execute_token(
        &self,
        transferred: NativeTransferredToken,
    ) -> Result<SendableRecordBatchStream> {
        self.prepare_token(transferred).await?.into_stream()
    }

    async fn prepare_token(
        &self,
        transferred: NativeTransferredToken,
    ) -> Result<NativePreparedQuery> {
        let instance_id = transferred.instance_id;
        let info = transferred.info.clone();
        let policy = transferred.policy;
        let execution = transferred.execution;
        let snapshot_output = transferred.snapshot_output.clone();
        let lease = self.prepare_transferred(transferred).await?;
        if lease.instance_id() != instance_id || lease.catalog_revision() != info.catalog_revision {
            return Err(internal(
                instance_id.query_id(),
                "prepared native query identity differs from its fixed plan metadata",
            ));
        }
        let schema = match &snapshot_output {
            Some(output) => {
                if output.schema.fields() != lease.plan().schema().fields() {
                    return Err(internal(
                        instance_id.query_id(),
                        "state snapshot schema differs from its query",
                    ));
                }
                output.schema.clone()
            }
            None => lease.plan().schema(),
        };
        Ok(NativePreparedQuery {
            instance_id,
            info,
            schema,
            lease,
            policy,
            execution,
            process_memory: self.inner.process_memory.clone(),
            snapshot_output,
        })
    }
}

impl QueryEngine for NativeQueryEngine {
    fn plan<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
    ) -> BoxFuture<'a, Result<OpaqueQueryExecutionToken>> {
        let bindings = match request
            .tables
            .iter()
            .map(|reference| match reference {
                TableRef::Name { table, .. } => {
                    NativeQueryBinding::try_new(table.as_str(), reference.clone())
                }
                TableRef::Id { .. } => Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "ID-based SQL tables require explicit native query bindings",
                )
                .with_query_id(request.query_id)
                .with_retryable(false)),
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(bindings) => bindings,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        self.plan_bound(owner, request, bindings)
    }

    fn execute(
        &self,
        token: OpaqueQueryExecutionToken,
    ) -> BoxFuture<'_, Result<SendableRecordBatchStream>> {
        let transferred = match transfer_native_token(token) {
            Ok(token) => token,
            Err(error) => {
                return Box::pin(async move { Err(error) });
            }
        };
        Box::pin(async move { self.execute_token(transferred).await })
    }

    fn cancel<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.inner.resources.cancel_owned(owner, instance_id) })
    }

    fn status<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'a, Result<QueryStatus>> {
        Box::pin(async move {
            self.inner
                .resources
                .status_owned(owner, instance_id)
                .map(public_status)
        })
    }
}

struct NativePlanToken {
    engine_nonce: u64,
    owner_session_id: SessionId,
    instance_id: QueryInstanceId,
    info: QueryPlanInfo,
    inner_token: Option<OpaqueQueryExecutionToken>,
    snapshot_output: Option<Arc<NativeKeyValueSnapshotOutput>>,
    sql: String,
    bindings: Vec<PersistedTableBinding>,
    policy: QuerySessionPolicy,
    execution: NativeQueryExecutionConfig,
}

impl QueryExecutionToken for NativePlanToken {
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
        Box::new(NativeTransferredToken {
            engine_nonce: self.engine_nonce,
            instance_id: self.instance_id,
            info: self.info.clone(),
            inner_token: self
                .inner_token
                .take()
                .expect("armed native plan token retains its QueryService token"),
            snapshot_output: self.snapshot_output.take(),
            sql: std::mem::take(&mut self.sql),
            bindings: std::mem::take(&mut self.bindings),
            policy: self.policy,
            execution: self.execution,
        })
    }
}

struct NativeTransferredToken {
    engine_nonce: u64,
    instance_id: QueryInstanceId,
    info: QueryPlanInfo,
    inner_token: OpaqueQueryExecutionToken,
    snapshot_output: Option<Arc<NativeKeyValueSnapshotOutput>>,
    sql: String,
    bindings: Vec<PersistedTableBinding>,
    policy: QuerySessionPolicy,
    execution: NativeQueryExecutionConfig,
}

fn transfer_native_token(token: OpaqueQueryExecutionToken) -> Result<NativeTransferredToken> {
    token
        .into_any()
        .downcast::<NativeTransferredToken>()
        .map(|token| *token)
        .map_err(|_| invalid("query execution token was not issued by the native engine"))
}

type PendingNativeBatch = Pin<
    Box<dyn Future<Output = (NativeQueryResultStream, Result<Option<NativeQueryBatch>>)> + Send>,
>;

struct NativeRecordBatchStream {
    instance_id: QueryInstanceId,
    schema: SchemaRef,
    plan_metrics: PersistedPlanMetrics,
    stream: Option<NativeQueryResultStream>,
    pending: Option<PendingNativeBatch>,
    operator_memory_status: QueryOperatorMemoryStatus,
    result_batches: u64,
    result_rows: u64,
    result_memory_bytes: u64,
    started_at: Instant,
    terminal: bool,
    // Drop all schema/batch references before the extra snapshot memory lease.
    snapshot_output: Option<Arc<NativeKeyValueSnapshotOutput>>,
}

impl NativeRecordBatchStream {
    fn new(
        instance_id: QueryInstanceId,
        schema: SchemaRef,
        stream: NativeQueryResultStream,
        plan_metrics: PersistedPlanMetrics,
        snapshot_output: Option<Arc<NativeKeyValueSnapshotOutput>>,
    ) -> Self {
        let operator_memory_status = stream.operator_memory_status();
        Self {
            snapshot_output,
            instance_id,
            schema,
            plan_metrics,
            stream: Some(stream),
            pending: None,
            operator_memory_status,
            result_batches: 0,
            result_rows: 0,
            result_memory_bytes: 0,
            started_at: Instant::now(),
            terminal: false,
        }
    }

    fn profile(&self) -> BTreeMap<String, u64> {
        let mut metrics = BTreeMap::new();
        self.plan_metrics.visit_stable_metrics(|name, value| {
            metrics.insert(name.to_owned(), value);
        });
        let operator_memory = self.stream.as_ref().map_or(
            self.operator_memory_status,
            NativeQueryResultStream::operator_memory_status,
        );
        metrics.insert(
            "operator.memory_limit_bytes".to_owned(),
            u64::try_from(operator_memory.limit_bytes).unwrap_or(u64::MAX),
        );
        metrics.insert(
            "operator.memory_reserved_bytes".to_owned(),
            u64::try_from(operator_memory.reserved_bytes).unwrap_or(u64::MAX),
        );
        metrics.insert(
            "operator.memory_peak_bytes".to_owned(),
            u64::try_from(operator_memory.peak_reserved_bytes).unwrap_or(u64::MAX),
        );
        metrics.insert(
            "operator.memory_rejections".to_owned(),
            u64::try_from(operator_memory.rejected_allocations).unwrap_or(u64::MAX),
        );
        metrics.insert("result.batches".to_owned(), self.result_batches);
        metrics.insert("result.rows".to_owned(), self.result_rows);
        metrics.insert(
            "result.accounted_bytes".to_owned(),
            self.result_memory_bytes,
        );
        metrics.insert(
            "query.elapsed_us".to_owned(),
            u64::try_from(self.started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        metrics
    }
}

impl RecordBatchStream for NativeRecordBatchStream {
    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn metrics(&self) -> BTreeMap<String, u64> {
        self.profile()
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<QueryResultBatch>>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        if self.pending.is_none() {
            let mut stream = self
                .stream
                .take()
                .expect("active native public stream owns its internal stream");
            self.pending = Some(Box::pin(async move {
                let next = stream.next_batch().await;
                (stream, next)
            }));
        }
        let pending = self
            .pending
            .as_mut()
            .expect("native public stream installed a batch future");
        match pending.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((stream, result)) => {
                self.operator_memory_status = stream.operator_memory_status();
                self.pending = None;
                self.stream = Some(stream);
                match result {
                    Ok(Some(batch)) => {
                        self.result_batches = self.result_batches.saturating_add(1);
                        self.result_rows = self.result_rows.saturating_add(
                            u64::try_from(batch.record_batch().num_rows()).unwrap_or(u64::MAX),
                        );
                        self.result_memory_bytes = self
                            .result_memory_bytes
                            .saturating_add(batch.memory_bytes().get());
                        let output = if let Some(snapshot) = &self.snapshot_output {
                            match batch
                                .record_batch()
                                .clone()
                                .with_schema(snapshot.schema.clone())
                            {
                                Ok(record) => Ok(QueryResultBatch::with_owner(
                                    record,
                                    (batch, snapshot.clone()),
                                )),
                                Err(error) => {
                                    self.terminal = true;
                                    drop(self.stream.take());
                                    Err(internal(
                                        self.instance_id.query_id(),
                                        format!("state result schema error=[{error}]"),
                                    ))
                                }
                            }
                        } else {
                            Ok(batch.into_public())
                        };
                        Poll::Ready(Some(output))
                    }
                    Ok(None) => {
                        self.terminal = true;
                        drop(self.stream.take());
                        Poll::Ready(None)
                    }
                    Err(error) => {
                        self.terminal = true;
                        drop(self.stream.take());
                        Poll::Ready(Some(Err(error)))
                    }
                }
            }
        }
    }
}

fn fail_planning(
    resources: &QueryResourceCoordinator,
    planning: crate::query_service::QueryPlanningToken,
    error: ServerError,
) -> ServerError {
    match resources.fail_planning(planning, error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
}

pub(crate) fn fail_execution(
    execution: crate::query_service::QueryExecutionScope,
    error: ServerError,
) -> ServerError {
    match execution.fail(error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
}

fn public_status(status: QueryRuntimeStatus) -> QueryStatus {
    let mut metrics = BTreeMap::new();
    metrics.insert(
        "query.active_memory_bytes".to_owned(),
        status.active_memory_bytes,
    );
    metrics.insert(
        "query.snapshot_memory_bytes".to_owned(),
        status.active_snapshot_memory_bytes,
    );
    metrics.insert(
        "query.task_memory_bytes".to_owned(),
        status.active_task_memory_bytes,
    );
    metrics.insert(
        "query.retained_memory_bytes".to_owned(),
        status.active_retained_memory_bytes,
    );
    metrics.insert(
        "query.result_memory_bytes".to_owned(),
        status.active_result_memory_bytes,
    );
    metrics.insert("query.active_tasks".to_owned(), status.active_tasks as u64);
    metrics.insert(
        "query.active_cpu_slots".to_owned(),
        u64::from(status.active_cpu_slots),
    );
    metrics.insert(
        "query.execution_complete".to_owned(),
        u64::from(status.execution_complete),
    );
    let (catalog_revision, snapshots) = status.plan.map_or((None, Vec::new()), |plan| {
        (Some(plan.catalog_revision), plan.snapshots)
    });
    QueryStatus {
        query_id: status.query_id,
        generation: NonZeroU64::new(status.generation).expect("QueryService generation is nonzero"),
        session_id: status.owner_session_id,
        state: status.state,
        submitted_at_ns: status.submitted_at_ns,
        started_at_ns: status.started_at_ns,
        finished_at_ns: status.finished_at_ns,
        catalog_revision,
        snapshots,
        metrics,
        error: status.error,
    }
}

fn unix_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn internal(query_id: zippy_types::QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message)
        .with_query_id(query_id)
        .with_retryable(false)
}

fn permission(query_id: zippy_types::QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::PermissionDenied, message)
        .with_query_id(query_id)
        .with_retryable(false)
}
