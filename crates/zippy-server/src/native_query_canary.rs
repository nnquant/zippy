//! Server-internal persisted-only native query canary.
//!
//! The canary intentionally consumes accounted native result batches inside Server and returns
//! only a deterministic digest report. It does not implement the public `QueryEngine` contract:
//! that contract currently exposes cloneable bare `RecordBatch` values and therefore cannot keep
//! native result-memory permits alive for the complete caller-owned batch lifetime.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Write as IoWrite};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayData, BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionFields};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zippy_core::ZippyQueryCanaryConfig;
use zippy_query::{
    PersistedPlanLimits, PersistedTableBinding, QueryProcessMemoryPool, QuerySessionPolicy,
};
use zippy_types::query::{OpaqueQueryOwnerCapability, QueryInstanceId, QueryState};
use zippy_types::table::TableRef;
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId};

use crate::query_execution::{
    NativeExecutionLease, NativeQueryExecutionConfig, NativeQueryResultStream,
};
use crate::query_service::{
    QueryAdminHandle, QueryPlanningToken, QueryResourceBudget, QueryResourceCoordinator,
    QueryRuntimeStatus, QuerySubmissionMetadata,
};
use crate::query_snapshot::NativeQuerySnapshotCoordinator;

/// Version of the row-order-independent canonical digest emitted by native query canary reports.
pub const NATIVE_QUERY_CANARY_DIGEST_FORMAT: &str = "zippy-arrow-multiset-sha256-v1";

const MAX_SQL_BYTES: usize = 16 * 1024;
const MAX_BINDINGS: usize = 16;
const MAX_RELATION_OCCURRENCES: usize = 32;
const MAX_LOGICAL_NODES: usize = 256;
const MAX_PHYSICAL_NODES: usize = 512;
const MAX_EXPRESSION_NODES: usize = 2_048;
const MAX_TRACKED_TERMINAL_INSTANCES: usize = 4_096;
const LENGTH_PREFIX_BYTES: u64 = 8;
const CANONICAL_HASH_CHUNK_BYTES: usize = 64 * 1024;

/// One SQL-visible name bound to an immutable persisted table reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeQueryCanaryBinding {
    sql_name: String,
    table: TableRef,
}

impl NativeQueryCanaryBinding {
    /// Creates a persisted table binding after validating its SQL-visible name.
    pub fn try_new(sql_name: impl Into<String>, table: TableRef) -> Result<Self> {
        let sql_name = sql_name.into();
        PersistedTableBinding::try_new(sql_name.clone(), 0).map_err(|error| {
            invalid(format!(
                "native query canary binding name is invalid name=[{sql_name}] error=[{error}]"
            ))
        })?;
        Ok(Self { sql_name, table })
    }

    /// Returns the SQL-visible identifier registered in DataFusion.
    #[must_use]
    pub fn sql_name(&self) -> &str {
        &self.sql_name
    }

    /// Returns the immutable table reference resolved by the native Catalog.
    #[must_use]
    pub const fn table(&self) -> &TableRef {
        &self.table
    }
}

/// Expected Arrow result used for a deterministic native query comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeQueryCanaryExpected {
    row_count: u64,
    digest: String,
}

impl NativeQueryCanaryExpected {
    /// Creates a validated expected result using the canary digest format.
    pub fn try_new(row_count: u64, digest: impl Into<String>) -> Result<Self> {
        let digest = digest.into().to_ascii_lowercase();
        validate_digest(&digest)?;
        Ok(Self { row_count, digest })
    }

    /// Returns the expected result row count.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the expected versioned SHA-256 digest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Incremental fixed-memory digest builder for expected Arrow results.
pub struct NativeQueryCanaryDigestBuilder {
    canonical: CanonicalResultDigest,
    max_canonical_bytes: u64,
}

impl NativeQueryCanaryDigestBuilder {
    /// Starts a bounded digest using an exact flat-scalar primary result schema.
    ///
    /// Nested, dictionary, union, map, and run-end encoded columns are rejected rather than
    /// compared through a potentially ambiguous display representation.
    pub fn try_new(query_id: QueryId, schema: SchemaRef, max_canonical_bytes: u64) -> Result<Self> {
        if max_canonical_bytes == 0 {
            return Err(
                invalid("native query canary digest byte limit must be positive")
                    .with_query_id(query_id),
            );
        }
        Ok(Self {
            canonical: CanonicalResultDigest::new(schema, max_canonical_bytes, query_id)?,
            max_canonical_bytes,
        })
    }

    /// Adds one primary Arrow batch without retaining it after the call returns.
    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.canonical
            .write_batch(batch, self.max_canonical_bytes, || false)
    }

    /// Returns the number of primary rows incorporated so far.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.canonical.row_count
    }

    /// Returns canonical input bytes charged so far.
    #[must_use]
    pub const fn canonical_bytes(&self) -> u64 {
        self.canonical.bytes
    }

    /// Finalizes the primary evidence accepted directly by a canary request.
    #[must_use]
    pub fn finish(self) -> NativeQueryCanaryExpected {
        NativeQueryCanaryExpected {
            row_count: self.canonical.row_count,
            digest: self.canonical.finish(),
        }
    }
}

/// One bounded persisted-only SQL request submitted to the internal canary.
#[derive(Debug, Clone)]
pub struct NativeQueryCanaryRequest {
    query_id: QueryId,
    session_id: SessionId,
    sql: String,
    bindings: Vec<NativeQueryCanaryBinding>,
    deadline_ns: Option<u64>,
    expected: Option<NativeQueryCanaryExpected>,
}

impl NativeQueryCanaryRequest {
    /// Creates a request. Service-specific byte and count limits are checked during submission.
    #[must_use]
    pub fn new(
        query_id: QueryId,
        session_id: SessionId,
        sql: impl Into<String>,
        bindings: Vec<NativeQueryCanaryBinding>,
    ) -> Self {
        Self {
            query_id,
            session_id,
            sql: sql.into(),
            bindings,
            deadline_ns: None,
            expected: None,
        }
    }

    /// Applies an absolute Unix-nanosecond deadline to planning, execution, and result digesting.
    #[must_use]
    pub fn with_deadline_ns(mut self, deadline_ns: u64) -> Self {
        self.deadline_ns = Some(deadline_ns);
        self
    }

    /// Attaches the primary result expected by a golden comparison.
    #[must_use]
    pub fn with_expected(mut self, expected: NativeQueryCanaryExpected) -> Self {
        self.expected = Some(expected);
        self
    }

    /// Returns the caller-assigned logical query identifier.
    #[must_use]
    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    /// Returns the owner session used for status and cancellation authority.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the persisted-only SQL text.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns the ordered SQL-visible persisted table bindings.
    #[must_use]
    pub fn bindings(&self) -> &[NativeQueryCanaryBinding] {
        &self.bindings
    }

    /// Returns the absolute request deadline, when configured.
    #[must_use]
    pub const fn deadline_ns(&self) -> Option<u64> {
        self.deadline_ns
    }
}

/// Golden comparison outcome for one completed native canary run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeQueryCanaryComparison {
    /// Expected and actual row counts and digests are identical.
    Match,
    /// At least one expected component differs from the native result.
    Mismatch {
        /// Whether result row counts match.
        row_count_matches: bool,
        /// Whether versioned result digests match.
        digest_matches: bool,
    },
}

/// Deterministic evidence from one fully consumed native canary query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeQueryCanaryReport {
    instance_id: QueryInstanceId,
    catalog_revision: u64,
    row_count: u64,
    batch_count: u64,
    arrow_memory_bytes: u64,
    canonical_bytes: u64,
    max_batch_memory_bytes: u64,
    digest_format: &'static str,
    digest: String,
    snapshot_and_plan_elapsed_us: u64,
    execution_elapsed_us: u64,
    result_wait_elapsed_us: u64,
    digest_elapsed_us: u64,
    total_elapsed_us: u64,
    comparison: Option<NativeQueryCanaryComparison>,
}

impl NativeQueryCanaryReport {
    /// Returns the exact process-local query incarnation.
    #[must_use]
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    /// Returns the immutable Catalog revision used by every bound table snapshot.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the number of result rows consumed.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the number of native Arrow batches consumed.
    #[must_use]
    pub const fn batch_count(&self) -> u64 {
        self.batch_count
    }

    /// Returns cumulative QueryService-accounted Arrow batch bytes.
    #[must_use]
    pub const fn arrow_memory_bytes(&self) -> u64 {
        self.arrow_memory_bytes
    }

    /// Returns cumulative canonical bytes fed into the digest.
    #[must_use]
    pub const fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }

    /// Returns the largest single downstream-owned result permit.
    #[must_use]
    pub const fn max_batch_memory_bytes(&self) -> u64 {
        self.max_batch_memory_bytes
    }

    /// Returns the versioned canonical digest format.
    #[must_use]
    pub const fn digest_format(&self) -> &'static str {
        self.digest_format
    }

    /// Returns the versioned SHA-256 digest of the schema and scalar-row multiset.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns snapshot resolution and physical planning time in microseconds.
    #[must_use]
    pub const fn snapshot_and_plan_elapsed_us(&self) -> u64 {
        self.snapshot_and_plan_elapsed_us
    }

    /// Returns native execution and bounded digest consumption time in microseconds.
    #[must_use]
    pub const fn execution_elapsed_us(&self) -> u64 {
        self.execution_elapsed_us
    }

    /// Returns time spent awaiting DataFusion and Parquet result production.
    #[must_use]
    pub const fn result_wait_elapsed_us(&self) -> u64 {
        self.result_wait_elapsed_us
    }

    /// Returns time spent validating limits and hashing canonical result values.
    #[must_use]
    pub const fn digest_elapsed_us(&self) -> u64 {
        self.digest_elapsed_us
    }

    /// Returns admission-to-report wall time in microseconds.
    #[must_use]
    pub const fn total_elapsed_us(&self) -> u64 {
        self.total_elapsed_us
    }

    /// Returns the primary-versus-native comparison, when an expected result was supplied.
    #[must_use]
    pub const fn comparison(&self) -> Option<NativeQueryCanaryComparison> {
        self.comparison
    }
}

/// Public, capability-free status for one exact canary query incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeQueryCanaryQueryStatus {
    instance_id: QueryInstanceId,
    state: QueryState,
    active_memory_bytes: u64,
    active_result_permits: usize,
    active_result_memory_bytes: u64,
    execution_complete: bool,
    cancellation_requested: bool,
    error: Option<ServerError>,
}

impl NativeQueryCanaryQueryStatus {
    /// Returns the exact process-local query incarnation.
    #[must_use]
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    /// Returns the current native query lifecycle state.
    #[must_use]
    pub const fn state(&self) -> QueryState {
        self.state
    }

    /// Returns all QueryService-accounted bytes currently held by this query.
    #[must_use]
    pub const fn active_memory_bytes(&self) -> u64 {
        self.active_memory_bytes
    }

    /// Returns result permits still owned by downstream canary processing.
    #[must_use]
    pub const fn active_result_permits(&self) -> usize {
        self.active_result_permits
    }

    /// Returns bytes retained by downstream-owned result permits.
    #[must_use]
    pub const fn active_result_memory_bytes(&self) -> u64 {
        self.active_result_memory_bytes
    }

    /// Returns whether DataFusion reached EOF even if a result permit remains live.
    #[must_use]
    pub const fn execution_complete(&self) -> bool {
        self.execution_complete
    }

    /// Returns whether cancellation or a deadline has been published.
    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    /// Returns the terminal structured error, when present.
    #[must_use]
    pub const fn error(&self) -> Option<&ServerError> {
        self.error.as_ref()
    }
}

/// Cumulative process-local canary service counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NativeQueryCanaryMetrics {
    /// Requests accepted by QueryService.
    pub submissions_total: u64,
    /// Requests rejected before a run capability was returned.
    pub submissions_rejected_total: u64,
    /// Runs that completed and emitted a report.
    pub succeeded_total: u64,
    /// Runs that ended with a non-cancellation failure.
    pub failed_total: u64,
    /// Runs that observed cancellation or deadline expiry.
    pub cancelled_total: u64,
    /// Accepted run capabilities dropped before successful reporting.
    pub abandoned_total: u64,
    /// Golden comparisons whose expected and actual results matched.
    pub comparisons_matched_total: u64,
    /// Golden comparisons whose expected and actual results differed.
    pub comparisons_mismatched_total: u64,
    /// Rows consumed by successful runs.
    pub rows_total: u64,
    /// Native batches consumed by successful runs.
    pub batches_total: u64,
    /// QueryService-accounted Arrow bytes consumed by successful runs.
    pub arrow_memory_bytes_total: u64,
    /// Canonical schema and scalar bytes hashed by successful runs.
    pub canonical_bytes_total: u64,
    /// Largest result permit observed across successful runs.
    pub max_batch_memory_bytes: u64,
    /// Snapshot resolution and physical planning microseconds across successful runs.
    pub snapshot_and_plan_elapsed_us_total: u64,
    /// Native execution and result consumption microseconds across successful runs.
    pub execution_elapsed_us_total: u64,
    /// DataFusion and Parquet result wait microseconds across successful runs.
    pub result_wait_elapsed_us_total: u64,
    /// Canonical validation and digest microseconds across successful runs.
    pub digest_elapsed_us_total: u64,
    /// Admission-to-report wall microseconds across successful runs.
    pub total_elapsed_us_total: u64,
}

/// Snapshot of canary admission and cumulative counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeQueryCanaryStatus {
    accepting: bool,
    active_runs: usize,
    #[serde(default)]
    tracked_instances: usize,
    #[serde(default)]
    metrics: NativeQueryCanaryMetrics,
}

impl NativeQueryCanaryStatus {
    /// Returns whether new canary requests may be submitted.
    #[must_use]
    pub const fn accepting(&self) -> bool {
        self.accepting
    }

    /// Returns accepted run capabilities that have not reached a reported terminal outcome.
    #[must_use]
    pub const fn active_runs(&self) -> usize {
        self.active_runs
    }

    /// Returns active and recently terminal canary instances authorized for status lookup.
    #[must_use]
    pub const fn tracked_instances(&self) -> usize {
        self.tracked_instances
    }

    /// Returns cumulative process-local counters.
    #[must_use]
    pub const fn metrics(&self) -> &NativeQueryCanaryMetrics {
        &self.metrics
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct NativeQueryCanaryRuntimeConfig {
    limits: ZippyQueryCanaryConfig,
    policy: QuerySessionPolicy,
    budget: QueryResourceBudget,
    execution: NativeQueryExecutionConfig,
    plan: PersistedPlanLimits,
}

impl NativeQueryCanaryRuntimeConfig {
    pub(crate) fn try_new(
        limits: ZippyQueryCanaryConfig,
        parallelism: usize,
        max_active_tasks: usize,
        max_cpu_slots: u32,
        max_memory_bytes: u64,
        operator_memory_bytes: u64,
    ) -> Result<Option<Self>> {
        if !limits.enabled {
            return Ok(None);
        }
        if limits.max_rows == 0 || limits.max_batches == 0 || limits.max_output_bytes == 0 {
            return Err(invalid(
                "native query canary row, batch, and output limits must be positive",
            ));
        }
        if max_active_tasks == 0 || max_cpu_slots == 0 || max_memory_bytes == 0 {
            return Err(invalid(
                "native query canary QueryService resource limits must be positive",
            ));
        }
        let policy = QuerySessionPolicy::new(
            NonZeroUsize::new(parallelism)
                .ok_or_else(|| invalid("native query canary parallelism must be positive"))?,
        );
        if parallelism > max_active_tasks
            || parallelism > usize::try_from(max_cpu_slots).unwrap_or(usize::MAX)
        {
            return Err(invalid(
                "native query canary parallelism exceeds its task or CPU budget",
            ));
        }
        let operator_memory_bytes = NonZeroU64::new(operator_memory_bytes)
            .ok_or_else(|| invalid("native query canary operator memory must be positive"))?;
        if operator_memory_bytes.get() > max_memory_bytes {
            return Err(invalid(
                "native query canary operator memory exceeds its query memory budget",
            ));
        }
        let plan = PersistedPlanLimits::try_new(
            MAX_SQL_BYTES,
            MAX_BINDINGS,
            MAX_RELATION_OCCURRENCES,
            MAX_LOGICAL_NODES,
            MAX_PHYSICAL_NODES,
            MAX_EXPRESSION_NODES,
        )
        .map_err(|error| {
            invalid(format!(
                "native query canary plan limits are invalid: {error}"
            ))
        })?;
        Ok(Some(Self {
            limits,
            policy,
            budget: QueryResourceBudget {
                max_active_tasks,
                max_cpu_slots,
                max_memory_bytes,
            },
            execution: NativeQueryExecutionConfig::new(operator_memory_bytes),
            plan,
        }))
    }
}

#[derive(Default)]
struct NativeQueryCanaryState {
    active_runs: usize,
    known_instances: HashSet<QueryInstanceId>,
    terminal_instances: VecDeque<QueryInstanceId>,
    metrics: NativeQueryCanaryMetrics,
}

struct NativeQueryCanaryInner {
    config: NativeQueryCanaryRuntimeConfig,
    snapshots: NativeQuerySnapshotCoordinator,
    resources: QueryResourceCoordinator,
    process_memory: QueryProcessMemoryPool,
    admin: QueryAdminHandle,
    accepting: AtomicBool,
    state: Mutex<NativeQueryCanaryState>,
}

/// Cloneable Server-internal entry point for bounded persisted-only native canary queries.
#[derive(Clone)]
pub struct NativeQueryCanaryService {
    inner: Arc<NativeQueryCanaryInner>,
}

impl NativeQueryCanaryService {
    pub(crate) fn new(
        config: NativeQueryCanaryRuntimeConfig,
        snapshots: NativeQuerySnapshotCoordinator,
        resources: QueryResourceCoordinator,
        process_memory: QueryProcessMemoryPool,
    ) -> Self {
        let admin = resources.admin_handle();
        Self {
            inner: Arc::new(NativeQueryCanaryInner {
                config,
                snapshots,
                resources,
                process_memory,
                admin,
                accepting: AtomicBool::new(true),
                state: Mutex::new(NativeQueryCanaryState::default()),
            }),
        }
    }

    /// Admits a request synchronously and returns its exact cancellable query capability.
    pub fn submit(&self, request: NativeQueryCanaryRequest) -> Result<NativeQueryCanaryRun> {
        let submitted_at = Instant::now();
        if !self.inner.accepting.load(Ordering::Acquire) {
            self.record_rejection();
            return Err(unavailable("native query canary admission is closed"));
        }
        let (tables, sql_names) = match self.validate_request(&request) {
            Ok(validated) => validated,
            Err(error) => {
                self.record_rejection();
                return Err(error);
            }
        };
        let owner = self.inner.resources.owner(request.session_id);
        let planning = match self.inner.resources.submit(
            owner.as_ref(),
            QuerySubmissionMetadata {
                query_id: request.query_id,
                deadline_ns: request.deadline_ns,
            },
            self.inner.config.budget,
        ) {
            Ok(planning) => planning,
            Err(error) => {
                self.record_rejection();
                return Err(error);
            }
        };
        let instance_id = planning.instance_id();
        if !self.try_record_submission(instance_id) {
            let _ = self
                .inner
                .resources
                .cancel_owned(owner.as_ref(), instance_id);
            return Err(
                unavailable("native query canary admission closed during submission")
                    .with_query_id(request.query_id),
            );
        }
        Ok(NativeQueryCanaryRun {
            service: self.clone(),
            request: Some(request),
            tables,
            sql_names,
            _owner: Some(owner),
            planning: Some(planning),
            instance_id,
            submitted_at,
            terminal_recorded: false,
        })
    }

    /// Cancels one exact canary query incarnation with administrator authority.
    pub fn cancel(&self, instance_id: QueryInstanceId) -> Result<()> {
        self.ensure_known_instance(instance_id)?;
        self.inner.admin.cancel(instance_id)
    }

    /// Returns current resource and lifecycle status for one exact query incarnation.
    pub fn query_status(
        &self,
        instance_id: QueryInstanceId,
    ) -> Result<NativeQueryCanaryQueryStatus> {
        self.ensure_known_instance(instance_id)?;
        self.inner
            .admin
            .status(instance_id)
            .map(public_query_status)
    }

    /// Returns canary admission state and cumulative process-local metrics.
    #[must_use]
    pub fn status(&self) -> NativeQueryCanaryStatus {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        NativeQueryCanaryStatus {
            accepting: self.inner.accepting.load(Ordering::Acquire),
            active_runs: state.active_runs,
            tracked_instances: state.known_instances.len(),
            metrics: state.metrics.clone(),
        }
    }

    pub(crate) fn close_admission(&self) {
        // The state lock is the submit/close linearization point. A submit either registers its
        // instance before this store or observes the closed state while holding the same lock.
        let _state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.inner.accepting.store(false, Ordering::Release);
    }

    fn try_record_submission(&self, instance_id: QueryInstanceId) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !self.inner.accepting.load(Ordering::Acquire) {
            state.metrics.submissions_rejected_total =
                state.metrics.submissions_rejected_total.saturating_add(1);
            return false;
        }
        state.active_runs = state.active_runs.saturating_add(1);
        state.known_instances.insert(instance_id);
        state.metrics.submissions_total = state.metrics.submissions_total.saturating_add(1);
        true
    }

    fn validate_request(
        &self,
        request: &NativeQueryCanaryRequest,
    ) -> Result<(Vec<TableRef>, Vec<String>)> {
        if request.sql.trim().is_empty() {
            return Err(invalid("native query canary SQL must not be empty")
                .with_query_id(request.query_id));
        }
        if request.sql.len() > self.inner.config.plan.max_sql_bytes() {
            return Err(exhausted(format!(
                "native query canary SQL exceeds byte limit actual=[{}] limit=[{}]",
                request.sql.len(),
                self.inner.config.plan.max_sql_bytes()
            ))
            .with_query_id(request.query_id));
        }
        if request.bindings.len() > self.inner.config.plan.max_bindings() {
            return Err(exhausted(format!(
                "native query canary bindings exceed limit actual=[{}] limit=[{}]",
                request.bindings.len(),
                self.inner.config.plan.max_bindings()
            ))
            .with_query_id(request.query_id));
        }
        let mut names = HashSet::with_capacity(request.bindings.len());
        let mut tables = Vec::with_capacity(request.bindings.len());
        let mut sql_names = Vec::with_capacity(request.bindings.len());
        for binding in &request.bindings {
            if !names.insert(binding.sql_name.clone()) {
                return Err(invalid(format!(
                    "native query canary SQL binding name is duplicated name=[{}]",
                    binding.sql_name
                ))
                .with_query_id(request.query_id));
            }
            tables.push(binding.table.clone());
            PersistedTableBinding::try_new(binding.sql_name.clone(), 0).map_err(|error| {
                invalid(format!(
                    "native query canary binding is invalid name=[{}] error=[{error}]",
                    binding.sql_name
                ))
                .with_query_id(request.query_id)
            })?;
            sql_names.push(binding.sql_name.clone());
        }
        Ok((tables, sql_names))
    }

    fn with_state(&self, update: impl FnOnce(&mut NativeQueryCanaryState)) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        update(&mut state);
    }

    fn record_rejection(&self) {
        self.with_state(|state| {
            state.metrics.submissions_rejected_total =
                state.metrics.submissions_rejected_total.saturating_add(1);
        });
    }

    fn ensure_known_instance(&self, instance_id: QueryInstanceId) -> Result<()> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.known_instances.contains(&instance_id) {
            return Ok(());
        }
        Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "query instance was not issued by the native query canary",
        )
        .with_query_id(instance_id.query_id())
        .with_retryable(false))
    }
}

/// One non-cloneable accepted run that cancels synchronously when abandoned.
pub struct NativeQueryCanaryRun {
    service: NativeQueryCanaryService,
    request: Option<NativeQueryCanaryRequest>,
    tables: Vec<TableRef>,
    sql_names: Vec<String>,
    _owner: Option<OpaqueQueryOwnerCapability>,
    planning: Option<QueryPlanningToken>,
    instance_id: QueryInstanceId,
    submitted_at: Instant,
    terminal_recorded: bool,
}

impl NativeQueryCanaryRun {
    /// Returns the exact query incarnation required for status and cancellation operations.
    #[must_use]
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }

    /// Resolves snapshots, prepares and executes the native plan, then consumes all results.
    pub async fn execute(mut self) -> Result<NativeQueryCanaryReport> {
        let snapshot_and_plan_started = Instant::now();
        let request = self
            .request
            .take()
            .expect("accepted native query canary run owns its request");
        let planning = self
            .planning
            .take()
            .expect("accepted native query canary run owns its planning token");
        let snapshot = match self
            .service
            .inner
            .snapshots
            .pin_persisted(&planning, &self.tables)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let error = match self
                    .service
                    .inner
                    .resources
                    .fail_planning(planning, error.clone())
                {
                    Ok(()) => error,
                    Err(terminal) => terminal,
                };
                return Err(self.record_failure(error));
            }
        };
        let ordinals = match snapshot.requested_table_ordinals() {
            Ok(ordinals) => ordinals,
            Err(error) => {
                let error = match self
                    .service
                    .inner
                    .resources
                    .fail_planning(planning, error.clone())
                {
                    Ok(()) => error,
                    Err(terminal) => terminal,
                };
                return Err(self.record_failure(error));
            }
        };
        if ordinals.len() != self.sql_names.len() {
            let error = internal(
                request.query_id,
                "native query canary binding count differs from resolved table ordinals",
            );
            let error = match self
                .service
                .inner
                .resources
                .fail_planning(planning, error.clone())
            {
                Ok(()) => error,
                Err(terminal) => terminal,
            };
            return Err(self.record_failure(error));
        }
        let bindings = match self
            .sql_names
            .iter()
            .zip(ordinals)
            .map(|(sql_name, ordinal)| {
                PersistedTableBinding::try_new(sql_name.clone(), ordinal).map_err(|error| {
                    internal(
                        request.query_id,
                        format!(
                            "validated native query canary binding could not be rebuilt: {error}"
                        ),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(bindings) => bindings,
            Err(error) => {
                let error = match self
                    .service
                    .inner
                    .resources
                    .fail_planning(planning, error.clone())
                {
                    Ok(()) => error,
                    Err(terminal) => terminal,
                };
                return Err(self.record_failure(error));
            }
        };
        let draft = snapshot.plan_draft();
        let token = match self
            .service
            .inner
            .resources
            .complete_planning(planning, draft, snapshot)
        {
            Ok(token) => token,
            Err(error) => return Err(self.record_failure(error)),
        };
        let execution = match self
            .service
            .inner
            .resources
            .begin_execution(token, unix_time_ns())
        {
            Ok(execution) => execution,
            Err(error) => return Err(self.record_failure(error)),
        };
        let manifest = match self
            .service
            .inner
            .snapshots
            .project_persisted_snapshot(&execution)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                let terminal = match execution.fail(error.clone()) {
                    Ok(()) => error,
                    Err(terminal) => terminal,
                };
                return Err(self.record_failure(terminal));
            }
        };
        let catalog_revision = manifest.resource().catalog_revision();
        let lease = match NativeExecutionLease::prepare(
            self.service.inner.snapshots.clone(),
            execution,
            manifest,
            self.service.inner.config.policy,
            &request.sql,
            &bindings,
            self.service.inner.config.plan,
        )
        .await
        {
            Ok(lease) => lease,
            Err(error) => return Err(self.record_failure(error)),
        };
        let schema = lease.plan().schema();
        let mut canonical = match CanonicalResultDigest::new(
            schema,
            self.service.inner.config.limits.max_output_bytes,
            request.query_id,
        ) {
            Ok(canonical) => canonical,
            Err(error) => {
                let terminal = fail_lease(lease, error);
                return Err(self.record_failure(terminal));
            }
        };
        let snapshot_and_plan_elapsed_us = elapsed_us(snapshot_and_plan_started.elapsed());
        let execution_started = Instant::now();
        let stream = match lease.execute_in(
            self.service.inner.config.policy,
            self.service.inner.config.execution,
            &self.service.inner.process_memory,
        ) {
            Ok(stream) => stream,
            Err(error) => return Err(self.record_failure(error)),
        };
        let consumed = match consume_stream(
            stream,
            &mut canonical,
            self.service.inner.config.limits,
            request.query_id,
        )
        .await
        {
            Ok(consumed) => consumed,
            Err(error) => return Err(self.record_failure(error)),
        };
        let execution_elapsed_us = elapsed_us(execution_started.elapsed());
        debug_assert_eq!(canonical.row_count, consumed.row_count);
        let digest = canonical.finish();
        let comparison = request.expected.as_ref().map(|expected| {
            if expected.row_count == consumed.row_count && expected.digest == digest {
                NativeQueryCanaryComparison::Match
            } else {
                NativeQueryCanaryComparison::Mismatch {
                    row_count_matches: expected.row_count == consumed.row_count,
                    digest_matches: expected.digest == digest,
                }
            }
        });
        let report = NativeQueryCanaryReport {
            instance_id: self.instance_id,
            catalog_revision,
            row_count: consumed.row_count,
            batch_count: consumed.batch_count,
            arrow_memory_bytes: consumed.arrow_memory_bytes,
            canonical_bytes: canonical.bytes,
            max_batch_memory_bytes: consumed.max_batch_memory_bytes,
            digest_format: NATIVE_QUERY_CANARY_DIGEST_FORMAT,
            digest,
            snapshot_and_plan_elapsed_us,
            execution_elapsed_us,
            result_wait_elapsed_us: consumed.result_wait_elapsed_us,
            digest_elapsed_us: consumed.digest_elapsed_us,
            total_elapsed_us: elapsed_us(self.submitted_at.elapsed()),
            comparison,
        };
        self.record_success(&report);
        Ok(report)
    }

    fn record_success(&mut self, report: &NativeQueryCanaryReport) {
        self.terminal_recorded = true;
        self.service.with_state(|state| {
            state.active_runs = state.active_runs.saturating_sub(1);
            retain_terminal_instance(state, self.instance_id);
            state.metrics.succeeded_total = state.metrics.succeeded_total.saturating_add(1);
            state.metrics.rows_total = state.metrics.rows_total.saturating_add(report.row_count);
            state.metrics.batches_total = state
                .metrics
                .batches_total
                .saturating_add(report.batch_count);
            state.metrics.arrow_memory_bytes_total = state
                .metrics
                .arrow_memory_bytes_total
                .saturating_add(report.arrow_memory_bytes);
            state.metrics.canonical_bytes_total = state
                .metrics
                .canonical_bytes_total
                .saturating_add(report.canonical_bytes);
            state.metrics.max_batch_memory_bytes = state
                .metrics
                .max_batch_memory_bytes
                .max(report.max_batch_memory_bytes);
            state.metrics.snapshot_and_plan_elapsed_us_total = state
                .metrics
                .snapshot_and_plan_elapsed_us_total
                .saturating_add(report.snapshot_and_plan_elapsed_us);
            state.metrics.execution_elapsed_us_total = state
                .metrics
                .execution_elapsed_us_total
                .saturating_add(report.execution_elapsed_us);
            state.metrics.result_wait_elapsed_us_total = state
                .metrics
                .result_wait_elapsed_us_total
                .saturating_add(report.result_wait_elapsed_us);
            state.metrics.digest_elapsed_us_total = state
                .metrics
                .digest_elapsed_us_total
                .saturating_add(report.digest_elapsed_us);
            state.metrics.total_elapsed_us_total = state
                .metrics
                .total_elapsed_us_total
                .saturating_add(report.total_elapsed_us);
            match report.comparison {
                Some(NativeQueryCanaryComparison::Match) => {
                    state.metrics.comparisons_matched_total =
                        state.metrics.comparisons_matched_total.saturating_add(1);
                }
                Some(NativeQueryCanaryComparison::Mismatch { .. }) => {
                    state.metrics.comparisons_mismatched_total =
                        state.metrics.comparisons_mismatched_total.saturating_add(1);
                }
                None => {}
            }
        });
    }

    fn record_failure(&mut self, error: ServerError) -> ServerError {
        self.terminal_recorded = true;
        self.service.with_state(|state| {
            state.active_runs = state.active_runs.saturating_sub(1);
            retain_terminal_instance(state, self.instance_id);
            if matches!(
                error.code(),
                ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
            ) {
                state.metrics.cancelled_total = state.metrics.cancelled_total.saturating_add(1);
            } else {
                state.metrics.failed_total = state.metrics.failed_total.saturating_add(1);
            }
        });
        error
    }
}

impl Drop for NativeQueryCanaryRun {
    fn drop(&mut self) {
        if self.terminal_recorded {
            return;
        }
        self.service.with_state(|state| {
            state.active_runs = state.active_runs.saturating_sub(1);
            retain_terminal_instance(state, self.instance_id);
            state.metrics.abandoned_total = state.metrics.abandoned_total.saturating_add(1);
        });
    }
}

struct ConsumedResult {
    row_count: u64,
    batch_count: u64,
    arrow_memory_bytes: u64,
    max_batch_memory_bytes: u64,
    result_wait_elapsed_us: u64,
    digest_elapsed_us: u64,
}

fn retain_terminal_instance(state: &mut NativeQueryCanaryState, instance_id: QueryInstanceId) {
    state.terminal_instances.push_back(instance_id);
    while state.terminal_instances.len() > MAX_TRACKED_TERMINAL_INSTANCES {
        if let Some(expired) = state.terminal_instances.pop_front() {
            state.known_instances.remove(&expired);
        }
    }
}

async fn consume_stream(
    mut stream: NativeQueryResultStream,
    canonical: &mut CanonicalResultDigest,
    limits: ZippyQueryCanaryConfig,
    query_id: QueryId,
) -> Result<ConsumedResult> {
    let max_batches = u64::try_from(limits.max_batches).map_err(|_| {
        exhausted("native query canary batch limit does not fit the result counter")
            .with_query_id(query_id)
    })?;
    let mut consumed = ConsumedResult {
        row_count: 0,
        batch_count: 0,
        arrow_memory_bytes: 0,
        max_batch_memory_bytes: 0,
        result_wait_elapsed_us: 0,
        digest_elapsed_us: 0,
    };
    loop {
        let result_wait_started = Instant::now();
        let next = stream.next_batch().await;
        consumed.result_wait_elapsed_us = consumed
            .result_wait_elapsed_us
            .saturating_add(elapsed_us(result_wait_started.elapsed()));
        let Some(batch) = next? else {
            break;
        };
        let digest_started = Instant::now();
        let batch_count = consumed.batch_count.checked_add(1).ok_or_else(|| {
            exhausted("native query canary batch count overflowed").with_query_id(query_id)
        })?;
        if batch_count > max_batches {
            return Err(stream.fail(limit_error(
                query_id,
                "batch count",
                batch_count,
                max_batches,
            )));
        }
        let row_count = consumed
            .row_count
            .checked_add(u64::try_from(batch.record_batch().num_rows()).map_err(|_| {
                exhausted("native query canary batch row count does not fit u64")
                    .with_query_id(query_id)
            })?)
            .ok_or_else(|| {
                exhausted("native query canary row count overflowed").with_query_id(query_id)
            })?;
        if row_count > limits.max_rows {
            return Err(stream.fail(limit_error(
                query_id,
                "row count",
                row_count,
                limits.max_rows,
            )));
        }
        let batch_memory = batch.memory_bytes().get();
        let arrow_memory_bytes = consumed
            .arrow_memory_bytes
            .checked_add(batch_memory)
            .ok_or_else(|| {
                exhausted("native query canary Arrow byte count overflowed").with_query_id(query_id)
            })?;
        let accounted_before_values =
            arrow_memory_bytes
                .checked_add(canonical.bytes)
                .ok_or_else(|| {
                    exhausted("native query canary output byte count overflowed")
                        .with_query_id(query_id)
                })?;
        if accounted_before_values > limits.max_output_bytes {
            return Err(stream.fail(limit_error(
                query_id,
                "output bytes",
                accounted_before_values,
                limits.max_output_bytes,
            )));
        }
        if let Err(error) = canonical.write_batch(
            batch.record_batch(),
            limits.max_output_bytes - arrow_memory_bytes,
            || batch.is_cancelled(),
        ) {
            return Err(stream.fail(error));
        }
        consumed.row_count = row_count;
        consumed.batch_count = batch_count;
        consumed.arrow_memory_bytes = arrow_memory_bytes;
        consumed.max_batch_memory_bytes = consumed.max_batch_memory_bytes.max(batch_memory);
        consumed.digest_elapsed_us = consumed
            .digest_elapsed_us
            .saturating_add(elapsed_us(digest_started.elapsed()));
        drop(batch);
    }
    Ok(consumed)
}

struct CanonicalResultDigest {
    schema: SchemaRef,
    schema_hash: [u8; 32],
    primary_sum: [u8; 32],
    secondary_sum: [u8; 32],
    row_xor: [u8; 32],
    row_count: u64,
    bytes: u64,
    query_id: QueryId,
}

impl CanonicalResultDigest {
    fn new(schema: SchemaRef, limit: u64, query_id: QueryId) -> Result<Self> {
        validate_canonical_result_schema(schema.as_ref(), query_id)?;
        let mut canonical = Self {
            schema: Arc::clone(&schema),
            schema_hash: [0; 32],
            primary_sum: [0; 32],
            secondary_sum: [0; 32],
            row_xor: [0; 32],
            row_count: 0,
            bytes: 0,
            query_id,
        };
        canonical.charge_len(
            u64::try_from(NATIVE_QUERY_CANARY_DIGEST_FORMAT.len()).map_err(|_| {
                exhausted("native query canary digest format length overflowed")
                    .with_query_id(query_id)
            })?,
            limit,
        )?;
        canonical.charge_len(LENGTH_PREFIX_BYTES, limit)?;
        let schema_limit = limit - canonical.bytes;
        let mut schema_ipc = BoundedSchemaDigestWriter::new(schema_limit);
        let normalized_schema = schema_without_metadata(schema.as_ref());
        let schema_result = (|| {
            let mut writer = StreamWriter::try_new(&mut schema_ipc, &normalized_schema)?;
            writer.finish()
        })();
        if let Err(error) = schema_result {
            if schema_ipc.limit_exceeded {
                return Err(exhausted(
                    "native query canary result schema exceeded the output bytes limit",
                )
                .with_query_id(query_id));
            }
            return Err(internal(
                query_id,
                format!("failed to encode canary result schema: {error}"),
            ));
        }
        if let Err(error) = write_schema_metadata(&mut schema_ipc, schema.as_ref()) {
            if schema_ipc.limit_exceeded {
                return Err(exhausted(
                    "native query canary result schema exceeded the output bytes limit",
                )
                .with_query_id(query_id));
            }
            return Err(internal(
                query_id,
                format!("failed to canonicalize canary schema metadata: {error}"),
            ));
        }
        let schema_bytes = schema_ipc.bytes;
        let schema_digest = schema_ipc.finish();
        canonical.charge_len(schema_bytes, limit)?;
        let mut schema_hasher = Sha256::new();
        schema_hasher.update(NATIVE_QUERY_CANARY_DIGEST_FORMAT.as_bytes());
        schema_hasher.update(schema_bytes.to_le_bytes());
        schema_hasher.update(schema_digest);
        canonical.schema_hash = schema_hasher.finalize().into();
        Ok(canonical)
    }

    fn write_batch(
        &mut self,
        batch: &RecordBatch,
        limit: u64,
        mut is_cancelled: impl FnMut() -> bool,
    ) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "native query canary result batch schema differs from the digest schema",
            )
            .with_query_id(self.query_id)
            .with_retryable(false));
        }
        let column_data = batch
            .columns()
            .iter()
            .map(|column| column.to_data())
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            if is_cancelled() {
                return Err(ServerError::new(
                    ErrorCode::Cancelled,
                    "native query canary result digesting was cancelled",
                )
                .with_query_id(self.query_id)
                .with_retryable(false));
            }
            let mut row_hasher = Sha256::new();
            self.update_hash(&mut row_hasher, &[0x52], limit)?;
            for (column, data) in batch.columns().iter().zip(&column_data) {
                if is_cancelled() {
                    return Err(ServerError::new(
                        ErrorCode::Cancelled,
                        "native query canary result digesting was cancelled",
                    )
                    .with_query_id(self.query_id)
                    .with_retryable(false));
                }
                if column.is_null(row) {
                    self.update_hash(&mut row_hasher, &[0x00], limit)?;
                    continue;
                }
                self.update_hash(&mut row_hasher, &[0x01], limit)?;
                if let Some(width) = column.data_type().primitive_width() {
                    let value = primitive_scalar_bytes(data, row, width, self.query_id)?;
                    self.update_hash(&mut row_hasher, value, limit)?;
                    continue;
                }
                if column.data_type() == &DataType::Boolean {
                    let value = column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            internal(
                                self.query_id,
                                "native query canary Boolean column has an inconsistent array type",
                            )
                        })?
                        .value(row);
                    self.update_hash(&mut row_hasher, &[u8::from(value)], limit)?;
                    continue;
                }
                let value = variable_scalar_bytes(column.as_ref(), row, self.query_id)?;
                self.update_variable_hash(&mut row_hasher, value, limit, &mut is_cancelled)?;
            }
            let primary: [u8; 32] = row_hasher.finalize().into();
            let mut secondary_hasher = Sha256::new();
            secondary_hasher.update(b"zippy-arrow-multiset-secondary-v1");
            secondary_hasher.update(primary);
            let secondary: [u8; 32] = secondary_hasher.finalize().into();
            add_256(&mut self.primary_sum, &primary);
            add_256(&mut self.secondary_sum, &secondary);
            for (accumulator, value) in self.row_xor.iter_mut().zip(primary) {
                *accumulator ^= value;
            }
            self.row_count = self.row_count.checked_add(1).ok_or_else(|| {
                exhausted("native query canary canonical row count overflowed")
                    .with_query_id(self.query_id)
            })?;
        }
        Ok(())
    }

    fn update_hash(&mut self, hasher: &mut Sha256, value: &[u8], limit: u64) -> Result<()> {
        let length = u64::try_from(value.len()).map_err(|_| {
            exhausted("native query canary canonical byte count overflowed")
                .with_query_id(self.query_id)
        })?;
        self.charge_len(length, limit)?;
        hasher.update(value);
        Ok(())
    }

    fn update_variable_hash(
        &mut self,
        hasher: &mut Sha256,
        value: &[u8],
        limit: u64,
        is_cancelled: &mut impl FnMut() -> bool,
    ) -> Result<()> {
        let length = u64::try_from(value.len()).map_err(|_| {
            exhausted("native query canary variable scalar byte count overflowed")
                .with_query_id(self.query_id)
        })?;
        self.update_hash(hasher, &length.to_le_bytes(), limit)?;
        for chunk in value.chunks(CANONICAL_HASH_CHUNK_BYTES) {
            if is_cancelled() {
                return Err(ServerError::new(
                    ErrorCode::Cancelled,
                    "native query canary result digesting was cancelled",
                )
                .with_query_id(self.query_id)
                .with_retryable(false));
            }
            self.update_hash(hasher, chunk, limit)?;
        }
        Ok(())
    }

    fn charge_len(&mut self, length: u64, limit: u64) -> Result<()> {
        let next = self.bytes.checked_add(length).ok_or_else(|| {
            exhausted("native query canary canonical byte count overflowed")
                .with_query_id(self.query_id)
        })?;
        if next > limit {
            return Err(limit_error(self.query_id, "output bytes", next, limit));
        }
        self.bytes = next;
        Ok(())
    }

    fn finish(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(NATIVE_QUERY_CANARY_DIGEST_FORMAT.as_bytes());
        hasher.update(self.schema_hash);
        hasher.update(self.row_count.to_le_bytes());
        hasher.update(self.primary_sum);
        hasher.update(self.secondary_sum);
        hasher.update(self.row_xor);
        format!(
            "{NATIVE_QUERY_CANARY_DIGEST_FORMAT}:{:x}",
            hasher.finalize()
        )
    }
}

fn validate_canonical_result_schema(schema: &Schema, query_id: QueryId) -> Result<()> {
    for field in schema.fields() {
        let data_type = field.data_type();
        if data_type.primitive_width().is_some()
            || matches!(
                data_type,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Binary
                    | DataType::FixedSizeBinary(_)
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            )
        {
            continue;
        }
        return Err(invalid(format!(
            "native query canary digest does not support nested or encoded result columns \
             field=[{}] data_type=[{data_type}]",
            field.name()
        ))
        .with_query_id(query_id));
    }
    Ok(())
}

fn primitive_scalar_bytes(
    data: &ArrayData,
    row: usize,
    width: usize,
    query_id: QueryId,
) -> Result<&[u8]> {
    let index = data.offset().checked_add(row).ok_or_else(|| {
        exhausted("native query canary primitive scalar index overflowed").with_query_id(query_id)
    })?;
    let start = index.checked_mul(width).ok_or_else(|| {
        exhausted("native query canary primitive scalar byte offset overflowed")
            .with_query_id(query_id)
    })?;
    let end = start.checked_add(width).ok_or_else(|| {
        exhausted("native query canary primitive scalar byte range overflowed")
            .with_query_id(query_id)
    })?;
    data.buffers()
        .first()
        .and_then(|buffer| buffer.as_slice().get(start..end))
        .ok_or_else(|| {
            internal(
                query_id,
                "native query canary primitive scalar buffer is inconsistent with its schema",
            )
        })
}

fn variable_scalar_bytes(column: &dyn Array, row: usize, query_id: QueryId) -> Result<&[u8]> {
    let value = match column.data_type() {
        DataType::Binary => column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| array.value(row)),
        DataType::LargeBinary => column
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|array| array.value(row)),
        DataType::BinaryView => column
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .map(|array| array.value(row)),
        DataType::FixedSizeBinary(_) => column
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|array| array.value(row)),
        DataType::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(row).as_bytes()),
        DataType::LargeUtf8 => column
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| array.value(row).as_bytes()),
        DataType::Utf8View => column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|array| array.value(row).as_bytes()),
        _ => None,
    };
    value.ok_or_else(|| {
        internal(
            query_id,
            format!(
                "native query canary variable column has an inconsistent array type \
                 data_type=[{}]",
                column.data_type()
            ),
        )
    })
}

fn add_256(accumulator: &mut [u8; 32], value: &[u8; 32]) {
    let mut carry = 0_u16;
    for index in (0..accumulator.len()).rev() {
        let sum = u16::from(accumulator[index]) + u16::from(value[index]) + carry;
        accumulator[index] = sum as u8;
        carry = sum >> 8;
    }
}

fn schema_without_metadata(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field_without_metadata(field))
            .collect::<Vec<_>>(),
    )
}

fn field_without_metadata(field: &Field) -> Field {
    // Rebuilding the field also normalizes IPC-only dictionary IDs and ordering flags, which
    // Arrow deliberately excludes from logical schema equality.
    Field::new(
        field.name().clone(),
        data_type_without_metadata(field.data_type()),
        field.is_nullable(),
    )
}

fn data_type_without_metadata(data_type: &DataType) -> DataType {
    match data_type {
        DataType::List(field) => DataType::List(Arc::new(field_without_metadata(field))),
        DataType::ListView(field) => DataType::ListView(Arc::new(field_without_metadata(field))),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(Arc::new(field_without_metadata(field)), *size)
        }
        DataType::LargeList(field) => DataType::LargeList(Arc::new(field_without_metadata(field))),
        DataType::LargeListView(field) => {
            DataType::LargeListView(Arc::new(field_without_metadata(field)))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| Arc::new(field_without_metadata(field)))
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Union(fields, mode) => {
            let (type_ids, fields): (Vec<_>, Vec<_>) = fields
                .iter()
                .map(|(type_id, field)| (type_id, Arc::new(field_without_metadata(field))))
                .unzip();
            DataType::Union(UnionFields::new(type_ids, fields), *mode)
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(data_type_without_metadata(key)),
            Box::new(data_type_without_metadata(value)),
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(Arc::new(field_without_metadata(field)), *sorted)
        }
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            Arc::new(field_without_metadata(run_ends)),
            Arc::new(field_without_metadata(values)),
        ),
        data_type => data_type.clone(),
    }
}

fn write_schema_metadata(writer: &mut impl IoWrite, schema: &Schema) -> io::Result<()> {
    write_metadata_map(writer, b'S', schema.metadata())?;
    for field in schema.fields() {
        write_field_metadata(writer, field)?;
    }
    Ok(())
}

fn write_field_metadata(writer: &mut impl IoWrite, field: &Field) -> io::Result<()> {
    write_metadata_map(writer, b'F', field.metadata())?;
    write_data_type_metadata(writer, field.data_type())
}

fn write_data_type_metadata(writer: &mut impl IoWrite, data_type: &DataType) -> io::Result<()> {
    match data_type {
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => write_field_metadata(writer, field),
        DataType::FixedSizeList(field, _) => write_field_metadata(writer, field),
        DataType::Struct(fields) => {
            for field in fields {
                write_field_metadata(writer, field)?;
            }
            Ok(())
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                write_field_metadata(writer, field)?;
            }
            Ok(())
        }
        DataType::Dictionary(key, value) => {
            write_data_type_metadata(writer, key)?;
            write_data_type_metadata(writer, value)
        }
        DataType::RunEndEncoded(run_ends, values) => {
            write_field_metadata(writer, run_ends)?;
            write_field_metadata(writer, values)
        }
        _ => Ok(()),
    }
}

fn write_metadata_map(
    writer: &mut impl IoWrite,
    tag: u8,
    metadata: &HashMap<String, String>,
) -> io::Result<()> {
    writer.write_all(&[tag])?;
    writer.write_all(&usize_to_u64(metadata.len(), "metadata entry count")?.to_le_bytes())?;
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (key, value) in entries {
        write_length_prefixed(writer, key.as_bytes(), "metadata key")?;
        write_length_prefixed(writer, value.as_bytes(), "metadata value")?;
    }
    Ok(())
}

fn write_length_prefixed(writer: &mut impl IoWrite, value: &[u8], label: &str) -> io::Result<()> {
    writer.write_all(&usize_to_u64(value.len(), label)?.to_le_bytes())?;
    writer.write_all(value)
}

fn usize_to_u64(value: usize, label: &str) -> io::Result<u64> {
    u64::try_from(value)
        .map_err(|_| io::Error::other(format!("native query canary {label} overflowed u64")))
}

struct BoundedSchemaDigestWriter {
    hasher: Sha256,
    bytes: u64,
    limit: u64,
    limit_exceeded: bool,
}

impl BoundedSchemaDigestWriter {
    fn new(limit: u64) -> Self {
        Self {
            hasher: Sha256::new(),
            bytes: 0,
            limit,
            limit_exceeded: false,
        }
    }

    fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl IoWrite for BoundedSchemaDigestWriter {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        let length = u64::try_from(value.len())
            .map_err(|_| io::Error::other("native query canary schema byte count overflowed"))?;
        if length > self.limit.saturating_sub(self.bytes) {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "native query canary schema byte limit exceeded",
            ));
        }
        self.hasher.update(value);
        self.bytes += length;
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn fail_lease(lease: NativeExecutionLease, error: ServerError) -> ServerError {
    match lease.fail(error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
}

fn public_query_status(status: QueryRuntimeStatus) -> NativeQueryCanaryQueryStatus {
    NativeQueryCanaryQueryStatus {
        instance_id: status.instance_id(),
        state: status.state,
        active_memory_bytes: status.active_memory_bytes,
        active_result_permits: status.active_result_permits,
        active_result_memory_bytes: status.active_result_memory_bytes,
        execution_complete: status.execution_complete,
        cancellation_requested: status.cancellation_requested,
        error: status.error,
    }
}

fn validate_digest(digest: &str) -> Result<()> {
    let Some(hex) = digest
        .strip_prefix(NATIVE_QUERY_CANARY_DIGEST_FORMAT)
        .and_then(|digest| digest.strip_prefix(':'))
    else {
        return Err(invalid(format!(
            "native query canary expected digest must start with \
             {NATIVE_QUERY_CANARY_DIGEST_FORMAT}:"
        )));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "native query canary expected digest must contain exactly 64 hexadecimal digits",
        ));
    }
    Ok(())
}

fn unix_time_ns() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

fn elapsed_us(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn limit_error(query_id: QueryId, resource: &str, actual: u64, limit: u64) -> ServerError {
    exhausted(format!(
        "native query canary {resource} limit exceeded actual=[{actual}] limit=[{limit}]"
    ))
    .with_query_id(query_id)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn unavailable(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, message).with_retryable(true)
}

fn internal(query_id: QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message)
        .with_query_id(query_id)
        .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use futures::future::poll_fn;
    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{OwnedDataDirectory, ZippyQueryCanaryConfig, DEFAULT_QUERY_MAX_MEMORY_BYTES};
    use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::query::{QueryEngine, QueryInput, QueryPriority, QueryRequest, QueryState};
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance,
        SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition, TableRef,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, QueryId,
        RequestId, ResourceLimit, SessionId, TableId,
    };

    use super::{
        CanonicalResultDigest, NativeQueryCanaryBinding, NativeQueryCanaryComparison,
        NativeQueryCanaryDigestBuilder, NativeQueryCanaryExpected, NativeQueryCanaryRequest,
        NativeQueryCanaryRuntimeConfig,
    };
    use crate::native_storage::{
        NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    };
    use crate::NativeQueryBinding;

    const NAMESPACE: u128 = 1;
    const TABLE: u128 = 10;
    const OTHER_TABLE: u128 = 5;

    struct TestRuntime {
        _temp: TempDir,
        _ownership: Arc<OwnedDataDirectory>,
        runtime: NativeStorageRuntime,
    }

    impl TestRuntime {
        fn shutdown(&mut self) {
            assert_eq!(
                self.runtime.shutdown(Duration::from_secs(5)),
                NativeStorageShutdownOutcome::Complete
            );
        }
    }

    fn namespace_id(value: u128) -> NamespaceId {
        NamespaceId::from_u128(value).expect("nonzero namespace ID")
    }

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).expect("nonzero table ID")
    }

    fn query_id(value: u128) -> QueryId {
        QueryId::from_u128(value).expect("nonzero query ID")
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("nonzero request ID")
    }

    fn session_id(value: u128) -> SessionId {
        SessionId::from_u128(value).expect("nonzero session ID")
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let mut bytes = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut bytes, schema().as_ref()).expect("create schema writer");
        writer.finish().expect("finish schema stream");
        drop(writer);
        SchemaIpcPayload::new(bytes).expect("nonempty schema IPC")
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "native-query-canary-test".to_owned(),
        }
    }

    fn command(
        request: u128,
        expected_revision: u64,
        command: CatalogCommandV1,
    ) -> zippy_types::catalog::VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id(request),
            Some(expected_revision),
            actor(),
            command,
        )
        .expect("valid Catalog command")
        .into()
    }

    async fn seed_catalog(catalog: &SqliteCatalog) {
        catalog
            .command(command(
                1,
                0,
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: namespace_id(NAMESPACE),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create namespace");
        catalog
            .command(command(
                2,
                1,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(TABLE),
                    namespace_id: namespace_id(NAMESPACE),
                    name: CatalogName::new("ticks").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create table");
        catalog
            .command(command(
                3,
                2,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(OTHER_TABLE),
                    namespace_id: namespace_id(NAMESPACE),
                    name: CatalogName::new("other_ticks").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create second table");
    }

    fn persist_request_for(
        table: u128,
        file: u128,
        request: u128,
        values: Vec<i64>,
    ) -> PersistRequest {
        let first = LogOffset::new(0);
        let last = LogOffset::new(u64::try_from(values.len() - 1).expect("row count fits u64"));
        let offsets = OffsetRange::new(first, last).expect("nonempty offset range");
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(vec![SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: u64::try_from(table).expect("test table ID fits segment ID"),
                    generation: 1,
                    writer_epoch: 0,
                },
                offsets,
            }])
            .expect("one source lineage"),
        )
        .expect("valid provenance");
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("build persisted batch");
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id: FileId::from_u128(file).expect("nonzero file ID"),
                table_id: table_id(table),
                schema_version: 1,
                schema,
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: 1_752_492_600_000_000_100,
            },
            vec![batch],
        )
        .expect("build persisted candidate");
        PersistRequest::new(
            request_id(request),
            table_id(table),
            1,
            0,
            vec![candidate],
            OffsetWatermark::Known(last),
        )
        .expect("build persist request")
    }

    fn canary_limits(max_rows: u64) -> ZippyQueryCanaryConfig {
        ZippyQueryCanaryConfig {
            enabled: true,
            max_rows,
            max_batches: 16,
            max_output_bytes: 16 * 1024 * 1024,
            operator_memory_bytes: Some(1024 * 1024),
        }
    }

    async fn start_runtime_with_limits(limits: ZippyQueryCanaryConfig) -> TestRuntime {
        let temp = tempfile::tempdir().expect("create temp directory");
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database"))
                .expect("own test data directory"),
        );
        let catalog_config = SqliteCatalogConfig::new(&ownership);
        let catalog = SqliteCatalog::open(catalog_config.clone()).expect("open seed Catalog");
        seed_catalog(&catalog).await;
        catalog.close().expect("close seed Catalog");
        let canary = NativeQueryCanaryRuntimeConfig::try_new(
            limits,
            1,
            16,
            4,
            DEFAULT_QUERY_MAX_MEMORY_BYTES,
            1024 * 1024,
        )
        .expect("valid canary config")
        .expect("enabled canary config");
        let runtime_config = NativeStorageRuntimeConfig::new(&ownership)
            .with_catalog_config(catalog_config)
            .expect("reuse Catalog ownership")
            .with_query_canary_config(Some(canary));
        let runtime = NativeStorageRuntime::start(runtime_config).expect("start native storage");
        runtime
            .table_actor(table_id(TABLE))
            .expect("native table actor")
            .persist(persist_request_for(
                TABLE,
                100,
                100,
                vec![42_i64, 43_i64, 44_i64],
            ))
            .await
            .expect("persist test Parquet file");
        runtime
            .table_actor(table_id(OTHER_TABLE))
            .expect("second native table actor")
            .persist(persist_request_for(
                OTHER_TABLE,
                101,
                101,
                vec![100_i64, 101_i64],
            ))
            .await
            .expect("persist second test Parquet file");
        TestRuntime {
            _temp: temp,
            _ownership: ownership,
            runtime,
        }
    }

    async fn start_runtime(max_rows: u64) -> TestRuntime {
        start_runtime_with_limits(canary_limits(max_rows)).await
    }

    async fn grow_log_history(runtime: &NativeStorageRuntime, table: u128, files: usize) {
        assert!(files >= 1);
        if files == 1 {
            return;
        }
        let actor = runtime
            .table_actor(table_id(table))
            .expect("history table actor");
        let snapshot = actor.snapshot().await.expect("history snapshot");
        let first = snapshot
            .committed_log_offset
            .known()
            .and_then(LogOffset::checked_next)
            .expect("seed history has a next offset");
        let candidates = (0..files - 1)
            .map(|index| {
                let offset = LogOffset::new(first.get() + index as u64);
                let offsets = OffsetRange::new(offset, offset).expect("one history row");
                let provenance = NativeDataFileProvenance::new(
                    offsets,
                    SourceSegmentLineages::new(vec![SourceSegmentLineage {
                        source: SourceSegmentIdentity {
                            segment_id: 10_000 + table as u64 * 1_000 + index as u64,
                            generation: 1,
                            writer_epoch: snapshot.writer_epoch,
                        },
                        offsets,
                    }])
                    .expect("one history lineage"),
                )
                .expect("history provenance");
                let schema = schema();
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![offset.get() as i64]))],
                )
                .expect("history batch");
                PersistCandidateInput::new(
                    ParquetCandidateSpec {
                        file_id: FileId::from_u128(10_000 + table * 1_000 + index as u128)
                            .expect("history file ID"),
                        table_id: table_id(table),
                        schema_version: snapshot.schema_version,
                        schema,
                        provenance,
                        partition: MetadataMap::new(),
                        metadata: MetadataMap::new(),
                        created_at_ns: 1_752_492_600_000_000_100,
                    },
                    vec![batch],
                )
                .expect("history candidate")
            })
            .collect();
        // One commit grows the manifest without growing the snapshot-version chain per file.
        actor
            .persist(
                PersistRequest::new(
                    request_id(20_000 + table),
                    table_id(table),
                    snapshot.persisted_snapshot_version,
                    snapshot.writer_epoch,
                    candidates,
                    OffsetWatermark::Known(LogOffset::new(first.get() + files as u64 - 2)),
                )
                .expect("history persist request"),
            )
            .await
            .expect("persist history files");
    }

    async fn single_live_refresh_snapshot_bytes(history_files: usize, other_files: usize) -> u64 {
        use std::num::NonZeroU32;

        use futures::StreamExt;
        use zippy_ingest::{
            TableActorLiveSegmentPublication, TableActorLiveSegmentRole,
            TableActorLiveSegmentsPublication,
        };
        use zippy_segment_store::{
            compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
        };

        let mut test = start_runtime(100).await;
        grow_log_history(&test.runtime, TABLE, history_files).await;
        grow_log_history(&test.runtime, OTHER_TABLE, other_files).await;
        let actor = test
            .runtime
            .table_actor(table_id(TABLE))
            .expect("log actor");
        let snapshot = actor.snapshot().await.expect("persisted boundary");
        let offset = snapshot
            .committed_log_offset
            .known()
            .and_then(LogOffset::checked_next)
            .expect("next live offset");
        let engine = test.runtime.query_engine();
        let owner = engine.owner(session_id(7_000));
        let subscription = engine
            .subscribe_log(
                owner.as_ref(),
                crate::NativeLogRequest {
                    query_id: query_id(7_001),
                    table: TableRef::Id {
                        table_id: table_id(TABLE),
                    },
                    start: None,
                    deadline_ns: None,
                    memory_limit_bytes: ResourceLimit::ServerDefault,
                    batch_rows: NonZeroU32::new(1),
                },
            )
            .await
            .expect("subscribe at latest boundary");
        let instance = subscription.instance_id();
        let mut stream = subscription.into_stream();
        let compiled =
            compile_schema(&[ColumnSpec::new("price", ColumnType::Int64)]).expect("live schema");
        let layout = LayoutPlan::for_schema(&compiled, 2).expect("live layout");
        let mut writer = ActiveSegmentWriter::new_for_runtime_with_epoch(
            compiled,
            layout,
            999_999,
            1,
            snapshot.writer_epoch,
            2,
        )
        .expect("live writer");
        writer.begin_row().expect("begin live row");
        writer.write_i64("price", 777).expect("write live row");
        writer.commit_row().expect("commit live row");
        let offsets = OffsetRange::new(offset, offset).expect("one live row");
        actor
            .publish_live_segments(TableActorLiveSegmentsPublication::new(
                table_id(TABLE),
                vec![TableActorLiveSegmentPublication::new(
                    TableActorLiveSegmentRole::Active,
                    SourceSegmentIdentity {
                        segment_id: 999_999,
                        generation: 1,
                        writer_epoch: snapshot.writer_epoch,
                    },
                    snapshot.schema_version,
                    offsets,
                    writer.active_descriptor(),
                    0,
                    1,
                )],
            ))
            .await
            .expect("publish the new live row");
        // The timeout only detects a stalled subscription; the regression assertion is a
        // deterministic resource count, independent of scheduler and filesystem latency.
        let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("live refresh must complete")
            .expect("subscription must remain open")
            .expect("live refresh must succeed");
        assert_eq!(item.offsets, offsets);
        assert_eq!(item.batch.record_batch().num_rows(), 1);
        assert_eq!(
            item.batch
                .record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("live Int64 column")
                .value(0),
            777
        );
        let resources = test.runtime.query_resource_coordinator();
        let bytes = resources
            .status_owned(owner.as_ref(), instance)
            .expect("live query ledger")
            .active_snapshot_memory_bytes;
        assert!(bytes > 0, "the sampled window must still own its snapshot");
        eprintln!(
            "live refresh history_files=[{history_files}] other_files=[{other_files}] \
             active_snapshot_memory_bytes=[{bytes}]"
        );
        drop(item);
        drop(stream);
        assert_eq!(resources.status().active_snapshot_memory_bytes, 0);
        drop(writer);
        test.shutdown();
        bytes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_log_refresh_memory_does_not_grow_with_persisted_history() {
        let small = single_live_refresh_snapshot_bytes(1, 1).await;
        let large = single_live_refresh_snapshot_bytes(256, 1).await;
        // Allow fixed schema/lease overhead and wider offset/version numbers, but not a
        // retained historical manifest or a source vector sized to the entire history.
        assert!(
            large <= small + 64 * 1024,
            "one-row live refresh retained history: small=[{small}] large=[{large}]"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_log_refresh_memory_excludes_same_namespace_background_history() {
        let small = single_live_refresh_snapshot_bytes(1, 1).await;
        let background = single_live_refresh_snapshot_bytes(1, 256).await;
        assert!(
            background <= small + 64 * 1024,
            "live refresh retained another table: small=[{small}] background=[{background}]"
        );
    }

    fn request(value: u128) -> NativeQueryCanaryRequest {
        NativeQueryCanaryRequest::new(
            query_id(value),
            session_id(7),
            "SELECT price FROM ticks",
            vec![NativeQueryCanaryBinding::try_new(
                "ticks",
                TableRef::Id {
                    table_id: table_id(TABLE),
                },
            )
            .expect("valid canary binding")],
        )
    }

    #[test]
    fn canonical_digest_is_independent_from_arrow_batch_and_row_order() {
        let schema = schema();
        let full = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42_i64, 43_i64, 44_i64]))],
        )
        .expect("build full batch");
        let mut one = CanonicalResultDigest::new(Arc::clone(&schema), 1024 * 1024, query_id(1))
            .expect("build digest");
        one.write_batch(&full, 1024 * 1024, || false)
            .expect("digest full batch");

        let mut split = CanonicalResultDigest::new(schema, 1024 * 1024, query_id(2))
            .expect("build split digest");
        split
            .write_batch(&full.slice(0, 1), 1024 * 1024, || false)
            .expect("digest first split");
        split
            .write_batch(&full.slice(1, 2), 1024 * 1024, || false)
            .expect("digest second split");
        let reversed = RecordBatch::try_new(
            full.schema(),
            vec![Arc::new(Int64Array::from(vec![44_i64, 42_i64, 43_i64]))],
        )
        .expect("build reordered batch");
        let mut reordered = CanonicalResultDigest::new(reversed.schema(), 1024 * 1024, query_id(3))
            .expect("build reordered digest");
        reordered
            .write_batch(&reversed, 1024 * 1024, || false)
            .expect("digest reordered batch");

        assert_eq!(one.finish(), split.finish());
        assert_eq!(one.finish(), reordered.finish());
        assert_eq!(one.bytes, split.bytes);
        assert_eq!(one.bytes, reordered.bytes);

        let mut primary =
            NativeQueryCanaryDigestBuilder::try_new(query_id(4), full.schema(), 1024 * 1024)
                .expect("build public primary digest");
        primary
            .update_batch(&full)
            .expect("digest primary batch incrementally");
        let expected = primary.finish();
        assert_eq!(expected.row_count(), 3);
        assert_eq!(expected.digest(), one.finish());

        let mismatched_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let mismatched_batch = RecordBatch::try_new(
            mismatched_schema,
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("build mismatched batch");
        let mut primary =
            NativeQueryCanaryDigestBuilder::try_new(query_id(5), full.schema(), 1024 * 1024)
                .expect("build schema-bound primary digest");
        assert_eq!(
            primary
                .update_batch(&mismatched_batch)
                .expect_err("schema mismatch must fail")
                .code(),
            ErrorCode::SchemaMismatch
        );
    }

    #[test]
    fn canonical_digest_normalizes_schema_metadata_order() {
        fn metadata(entries: &[(&str, &str)]) -> HashMap<String, String> {
            entries
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect()
        }

        fn metadata_schema(reverse: bool, unit: &str) -> SchemaRef {
            let schema_entries = if reverse {
                [("source", "tgw"), ("version", "1")]
            } else {
                [("version", "1"), ("source", "tgw")]
            };
            let field_entries = if reverse {
                [("unit", unit), ("timezone", "UTC")]
            } else {
                [("timezone", "UTC"), ("unit", unit)]
            };
            let field =
                Field::new("values", DataType::Int64, true).with_metadata(metadata(&field_entries));
            Arc::new(Schema::new_with_metadata(
                vec![field],
                metadata(&schema_entries),
            ))
        }

        let first_schema = metadata_schema(false, "lots");
        let reordered_schema = metadata_schema(true, "lots");
        let changed_schema = metadata_schema(true, "contracts");
        assert_eq!(first_schema, reordered_schema);
        assert_ne!(first_schema, changed_schema);

        let first = CanonicalResultDigest::new(first_schema, 1024 * 1024, query_id(40))
            .expect("digest first metadata schema")
            .finish();
        let reordered = CanonicalResultDigest::new(reordered_schema, 1024 * 1024, query_id(41))
            .expect("digest reordered metadata schema")
            .finish();
        let changed = CanonicalResultDigest::new(changed_schema, 1024 * 1024, query_id(42))
            .expect("digest changed metadata schema")
            .finish();

        assert_eq!(first, reordered);
        assert_ne!(first, changed);
        assert_eq!(
            NativeQueryCanaryExpected::try_new(0, first.clone())
                .expect("accept current digest format")
                .digest(),
            first
        );
        assert_eq!(
            NativeQueryCanaryExpected::try_new(0, format!("sha256-v1:{}", "0".repeat(64)))
                .expect_err("reject an unscoped digest version")
                .code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn canonical_digest_uses_primitive_bits_and_rejects_ambiguous_nested_values() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let first_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0001,
            )]))],
        )
        .expect("build first NaN payload batch");
        let second_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0002,
            )]))],
        )
        .expect("build second NaN payload batch");
        let mut first = CanonicalResultDigest::new(Arc::clone(&schema), 1024, query_id(45))
            .expect("build first primitive digest");
        first
            .write_batch(&first_batch, 1024, || false)
            .expect("digest first NaN payload");
        let mut second = CanonicalResultDigest::new(schema, 1024, query_id(46))
            .expect("build second primitive digest");
        second
            .write_batch(&second_batch, 1024, || false)
            .expect("digest second NaN payload");
        assert_ne!(first.finish(), second.finish());

        let nested = Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]));
        let error = match NativeQueryCanaryDigestBuilder::try_new(query_id(47), nested, 1024) {
            Ok(_) => panic!("nested values must not use ambiguous display encoding"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn primary_digest_streams_large_scalars_through_the_byte_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["x".repeat(1024 * 1024)]))],
        )
        .expect("build large scalar batch");
        let mut digest = NativeQueryCanaryDigestBuilder::try_new(query_id(6), schema, 4 * 1024)
            .expect("schema fits the primary digest limit");

        let error = digest
            .update_batch(&batch)
            .expect_err("large scalar must stop at the configured byte limit");

        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(error.message().contains("output bytes limit exceeded"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canary_executes_persisted_query_and_matches_golden_result() {
        let mut test = start_runtime(100).await;
        let canary = test.runtime.query_canary().expect("enabled canary");

        let foreign = zippy_types::query::QueryInstanceId::new(
            query_id(3999),
            NonZeroU64::new(1).expect("nonzero generation"),
        );
        let error = canary
            .cancel(foreign)
            .expect_err("canary admin must reject foreign query instances");
        assert_eq!(error.code(), ErrorCode::PermissionDenied);

        let first_run = canary.submit(request(1000)).expect("submit first canary");
        let first_instance = first_run.instance_id();
        let first = first_run.execute().await.expect("execute first canary");
        assert_eq!(first.row_count(), 3);
        assert!(first.batch_count() > 0);
        assert!(first.arrow_memory_bytes() > 0);
        assert!(first.canonical_bytes() > 0);
        assert_eq!(first.comparison(), None);
        let first_status = canary
            .query_status(first_instance)
            .expect("query terminal status");
        assert_eq!(
            first_status.state(),
            zippy_types::query::QueryState::Finished
        );
        assert_eq!(first_status.active_memory_bytes(), 0);
        assert_eq!(first_status.active_result_permits(), 0);

        let primary_batch = RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![42_i64, 43_i64, 44_i64]))],
        )
        .expect("build legacy-style primary batch");
        let mut primary = NativeQueryCanaryDigestBuilder::try_new(
            query_id(9000),
            primary_batch.schema(),
            1024 * 1024,
        )
        .expect("start legacy-style primary digest");
        primary
            .update_batch(&primary_batch)
            .expect("consume legacy-style primary batch");
        let expected = primary.finish();
        assert_eq!(expected.digest(), first.digest());
        let second = canary
            .submit(request(1001).with_expected(expected))
            .expect("submit golden canary")
            .execute()
            .await
            .expect("execute golden canary");
        assert_eq!(second.digest(), first.digest());
        assert_eq!(
            second.comparison(),
            Some(NativeQueryCanaryComparison::Match)
        );

        let reordered = NativeQueryCanaryRequest::new(
            query_id(1002),
            session_id(7),
            "SELECT target.price FROM target CROSS JOIN unused_lower_id",
            vec![
                NativeQueryCanaryBinding::try_new(
                    "target",
                    TableRef::Id {
                        table_id: table_id(TABLE),
                    },
                )
                .expect("valid target binding"),
                NativeQueryCanaryBinding::try_new(
                    "unused_lower_id",
                    TableRef::Id {
                        table_id: table_id(OTHER_TABLE),
                    },
                )
                .expect("valid unused binding"),
            ],
        );
        let reordered_report = canary
            .submit(reordered)
            .expect("submit request-order mapping canary")
            .execute()
            .await
            .expect("execute request-order mapping cross join");
        assert_eq!(reordered_report.row_count(), 6);
        let canonical_order = NativeQueryCanaryRequest::new(
            query_id(1003),
            session_id(7),
            "SELECT target.price FROM target CROSS JOIN unused_lower_id",
            vec![
                NativeQueryCanaryBinding::try_new(
                    "unused_lower_id",
                    TableRef::Id {
                        table_id: table_id(OTHER_TABLE),
                    },
                )
                .expect("valid canonical first binding"),
                NativeQueryCanaryBinding::try_new(
                    "target",
                    TableRef::Id {
                        table_id: table_id(TABLE),
                    },
                )
                .expect("valid canonical target binding"),
            ],
        );
        let canonical_order_report = canary
            .submit(canonical_order)
            .expect("submit canonical-order mapping canary")
            .execute()
            .await
            .expect("execute canonical-order mapping cross join");
        assert_eq!(canonical_order_report.row_count(), 6);
        assert_eq!(canonical_order_report.digest(), reordered_report.digest());

        let status = canary.status();
        assert!(status.accepting());
        assert_eq!(status.active_runs(), 0);
        assert_eq!(status.metrics().submissions_total, 4);
        assert_eq!(status.metrics().succeeded_total, 4);
        assert_eq!(status.metrics().failed_total, 0);
        assert_eq!(status.metrics().comparisons_matched_total, 1);
        assert_eq!(status.metrics().rows_total, 18);
        assert_eq!(
            status.metrics().canonical_bytes_total,
            first
                .canonical_bytes()
                .saturating_add(second.canonical_bytes())
                .saturating_add(reordered_report.canonical_bytes())
                .saturating_add(canonical_order_report.canonical_bytes())
        );
        assert_eq!(
            status.metrics().total_elapsed_us_total,
            first
                .total_elapsed_us()
                .saturating_add(second.total_elapsed_us())
                .saturating_add(reordered_report.total_elapsed_us())
                .saturating_add(canonical_order_report.total_elapsed_us())
        );
        test.shutdown();
        assert!(!canary.status().accepting());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_native_engine_streams_permit_owned_results_and_cancels_on_drop() {
        let mut test = start_runtime(100).await;
        let engine = test.runtime.query_engine();
        let owner = engine.owner(session_id(70));
        let query_request = |query: u128, sql: &str| QueryRequest {
            query_id: query_id(query),
            session_id: session_id(70),
            input: QueryInput::Sql(sql.to_owned()),
            tables: vec![TableRef::Id {
                table_id: table_id(TABLE),
            }],
            priority: QueryPriority::Normal,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: None,
        };
        let binding = || {
            vec![NativeQueryBinding::try_new(
                "ticks",
                TableRef::Id {
                    table_id: table_id(TABLE),
                },
            )
            .expect("valid native engine binding")]
        };

        let token = engine
            .plan_bound(
                owner.as_ref(),
                query_request(1100, "SELECT sum(price) AS total FROM ticks"),
                binding(),
            )
            .await
            .expect("plan public native aggregate");
        let instance_id = token.instance_id();
        let mut stream = engine
            .execute(token)
            .await
            .expect("execute public native aggregate");
        let mut rows = 0_usize;
        while let Some(batch) = poll_fn(|context| stream.as_mut().poll_next(context))
            .await
            .transpose()
            .expect("read public native result")
        {
            assert!(batch.has_owner());
            rows += batch.record_batch().num_rows();
        }
        assert_eq!(rows, 1);
        let finished = engine
            .status(owner.as_ref(), instance_id)
            .await
            .expect("read completed native query status");
        assert_eq!(finished.state, QueryState::Finished);
        assert_eq!(finished.metrics["query.active_memory_bytes"], 0);
        assert_eq!(engine.process_memory_status().reserved_bytes, 0);

        let abandoned = engine
            .plan_bound(
                owner.as_ref(),
                query_request(1101, "SELECT price FROM ticks ORDER BY price"),
                binding(),
            )
            .await
            .expect("plan cancellable native query");
        let abandoned_instance = abandoned.instance_id();
        let abandoned_stream = engine
            .execute(abandoned)
            .await
            .expect("start cancellable native query");
        drop(abandoned_stream);
        let cancelled = engine
            .status(owner.as_ref(), abandoned_instance)
            .await
            .expect("read abandoned native query status");
        assert_eq!(cancelled.state, QueryState::Cancelled);
        assert_eq!(engine.process_memory_status().reserved_bytes, 0);
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canary_row_limit_fails_query_and_releases_all_resources() {
        let mut test = start_runtime(1).await;
        let canary = test.runtime.query_canary().expect("enabled canary");
        let run = canary.submit(request(2000)).expect("submit limited canary");
        let instance = run.instance_id();

        let error = run.execute().await.expect_err("row limit must fail");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(error.message().contains("row count limit exceeded"));
        let status = canary.query_status(instance).expect("failed query status");
        assert_eq!(status.state(), zippy_types::query::QueryState::Failed);
        assert_eq!(status.active_memory_bytes(), 0);
        assert_eq!(status.active_result_permits(), 0);
        assert_eq!(canary.status().metrics().failed_total, 1);
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canary_output_limit_fails_before_unbounded_result_consumption() {
        let mut limits = canary_limits(100);
        limits.max_output_bytes = 1;
        let mut test = start_runtime_with_limits(limits).await;
        let canary = test.runtime.query_canary().expect("enabled canary");
        let run = canary
            .submit(request(2500))
            .expect("submit output-limited canary");
        let instance = run.instance_id();

        let error = run.execute().await.expect_err("output limit must fail");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(error.message().contains("output bytes limit exceeded"));
        let status = canary.query_status(instance).expect("failed query status");
        assert_eq!(status.state(), zippy_types::query::QueryState::Failed);
        assert_eq!(status.active_memory_bytes(), 0);
        assert_eq!(status.active_result_permits(), 0);
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canary_cancel_drop_and_shutdown_preserve_lifecycle_boundaries() {
        let mut test = start_runtime(100).await;
        let canary = test.runtime.query_canary().expect("enabled canary");

        let error = match canary.submit(request(2999).with_deadline_ns(1)) {
            Ok(_) => panic!("expired request must be rejected before a run is returned"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert_eq!(canary.status().metrics().submissions_rejected_total, 1);

        let cancelled = canary
            .submit(request(3000))
            .expect("submit cancellable run");
        let cancelled_instance = cancelled.instance_id();
        canary
            .cancel(cancelled_instance)
            .expect("cancel exact canary instance");
        let error = cancelled
            .execute()
            .await
            .expect_err("cancelled run must fail");
        assert_eq!(error.code(), ErrorCode::Cancelled);
        assert_eq!(
            canary
                .query_status(cancelled_instance)
                .expect("cancelled status")
                .state(),
            zippy_types::query::QueryState::Cancelled
        );

        let abandoned = canary.submit(request(3001)).expect("submit abandoned run");
        let abandoned_instance = abandoned.instance_id();
        drop(abandoned);
        assert_eq!(
            canary
                .query_status(abandoned_instance)
                .expect("abandoned status")
                .state(),
            zippy_types::query::QueryState::Cancelled
        );
        assert_eq!(canary.status().metrics().cancelled_total, 1);
        assert_eq!(canary.status().metrics().abandoned_total, 1);
        assert_eq!(canary.status().tracked_instances(), 2);

        test.runtime.begin_shutdown();
        let error = match canary.submit(request(3002)) {
            Ok(_) => panic!("shutdown must close canary admission"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Unavailable);
        test.shutdown();
    }
}
