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
use zippy_types::{BoxFuture, ErrorCode, ResourceLimit, Result, ServerError, SessionId};

use crate::query_execution::{
    NativeExecutionLease, NativeQueryBatch, NativeQueryExecutionConfig, NativeQueryResultStream,
};
use crate::query_service::{
    QueryResourceBudget, QueryResourceCoordinator, QueryRuntimeStatus, QuerySubmissionMetadata,
};
use crate::query_snapshot::NativeQuerySnapshotCoordinator;

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

    /// Plans SQL using explicit relation-to-table bindings.
    pub fn plan_bound<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<OpaqueQueryExecutionToken>> {
        Box::pin(async move { self.plan_bound_inner(owner, request, bindings).await })
    }

    /// Prepares and explains SQL using explicit bindings without executing result operators.
    pub fn explain_bound<'a>(
        &'a self,
        owner: &'a dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
    ) -> BoxFuture<'a, Result<NativeQueryExplanation>> {
        Box::pin(async move {
            let token = self.plan_bound_inner(owner, request, bindings).await?;
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

    async fn plan_bound_inner(
        &self,
        owner: &dyn QueryOwnerCapability,
        request: QueryRequest,
        bindings: Vec<NativeQueryBinding>,
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
        let snapshot = match self.inner.snapshots.pin_persisted(&planning, &tables).await {
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
        let instance_id = transferred.instance_id;
        let policy = transferred.policy;
        let execution = transferred.execution;
        let lease = self.prepare_transferred(transferred).await?;
        let schema = lease.plan().schema();
        let plan_metrics = lease.plan_metrics();
        let stream = lease.execute_in(policy, execution, &self.inner.process_memory)?;
        Ok(Box::pin(NativeRecordBatchStream::new(
            instance_id,
            schema,
            stream,
            plan_metrics,
        )))
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
            inner_token: self
                .inner_token
                .take()
                .expect("armed native plan token retains its QueryService token"),
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
    inner_token: OpaqueQueryExecutionToken,
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
}

impl NativeRecordBatchStream {
    fn new(
        instance_id: QueryInstanceId,
        schema: SchemaRef,
        stream: NativeQueryResultStream,
        plan_metrics: PersistedPlanMetrics,
    ) -> Self {
        let operator_memory_status = stream.operator_memory_status();
        Self {
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
                        Poll::Ready(Some(Ok(batch.into_public())))
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

fn fail_execution(
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
