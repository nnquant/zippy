//! Lifecycle-bound execution of persisted native query plans.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use arrow::record_batch::RecordBatch;
use zippy_query::{
    classify_persisted_execution_error, classify_persisted_plan_error, next_persisted_batch,
    prepare_persisted_plan, profile_persisted_plan, structured_persisted_execution_error,
    BoundPersistedPlan, BoxedAsyncFileReader, PersistedExecutionContext, PersistedExecutionError,
    PersistedExecutionErrorClass, PersistedFileDescription, PersistedFileOpenRequest,
    PersistedFileOpener, PersistedPlanErrorClass, PersistedPlanLimits, PersistedPlanProfile,
    PersistedRecordBatchStream, PersistedSnapshotDescription, PersistedTableBinding,
    PreparedPersistedPlan, QuerySessionPolicy,
};
use zippy_types::query::QueryInstanceId;
use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;
use zippy_types::{ErrorCode, QueryId, Result, ServerError};

use crate::query_service::{
    QueryCancellation, QueryExecutionScope, QueryResultCharge, QueryResultPermit,
    QueryRetainedCharge, QueryRetainedKind, QueryRetainedPermit, QueryTaskCharge, QueryTaskPermit,
    QueryTerminalSignal,
};
use crate::query_snapshot::{NativeQuerySnapshotCoordinator, ProjectedSnapshotManifest};

const PHYSICAL_BUILD_CPU_SLOTS: u32 = 1;
const PERSISTED_FILE_TASK_CPU_SLOTS: u32 = 1;
const PERSISTED_FILE_TASK_MEMORY_BYTES: u64 = 64 * 1024;
const OPERATOR_CONTEXT_RETAINED_MEMORY_BYTES: u64 = 1024 * 1024;
const OPERATOR_CONTEXT_BUILD_TRANSIENT_MEMORY_BYTES: u64 = 1024 * 1024;
const EXECUTION_DRIVER_MEMORY_BYTES: u64 = 64 * 1024;
const RESULT_BATCH_FIXED_MEMORY_BYTES: u64 = 4 * 1024;

/// Resource limits for one native persisted execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeQueryExecutionConfig {
    operator_pool_memory_bytes: NonZeroU64,
}

impl NativeQueryExecutionConfig {
    pub(crate) const fn new(operator_pool_memory_bytes: NonZeroU64) -> Self {
        Self {
            operator_pool_memory_bytes,
        }
    }
}

/// A prepared plan whose complete asynchronous construction retained its build reservation.
///
/// This type is intentionally private. A bare [`PreparedPersistedPlan`] cannot cross the server
/// execution boundary because it carries no proof that query task admission covered preparation.
struct AdmittedPreparedPersistedPlan {
    task: QueryTaskPermit<()>,
    prepared: PreparedPersistedPlan,
    profile: PersistedPlanProfile,
}

impl AdmittedPreparedPersistedPlan {
    async fn prepare(
        execution: &QueryExecutionScope,
        policy: QuerySessionPolicy,
        sql: &str,
        bindings: &[PersistedTableBinding],
        snapshot: &PersistedSnapshotDescription,
        limits: PersistedPlanLimits,
    ) -> Result<Self> {
        // The preflight is bounded by `limits` and yields the reservation required by the full
        // DataFusion prepare/bind phase. `prepare_persisted_plan` repeats the fingerprinted
        // preflight after admission, so inputs cannot change between profiling and construction.
        let preflight_task = execution.admit_build_task(QueryTaskCharge {
            cpu_slots: NonZeroU32::new(PHYSICAL_BUILD_CPU_SLOTS)
                .expect("physical preflight CPU charge is nonzero"),
            memory_bytes: limits.preflight_peak_bytes().map_err(|error| {
                plan_error(
                    execution.query_id(),
                    PersistedPlanStage::PreflightCharge,
                    classify_persisted_plan_error(&error),
                    &error,
                )
            })?,
        })?;
        let profile = profile_persisted_plan(sql, bindings, snapshot, limits).map_err(|error| {
            plan_error(
                execution.query_id(),
                PersistedPlanStage::Profile,
                classify_persisted_plan_error(&error),
                &error,
            )
        })?;
        drop(preflight_task);
        let task = execution.admit_build_task(QueryTaskCharge {
            cpu_slots: NonZeroU32::new(PHYSICAL_BUILD_CPU_SLOTS)
                .expect("physical build CPU charge is nonzero"),
            memory_bytes: profile.build_peak_bytes(),
        })?;
        let mut cancellation = execution.cancellation();
        let prepared = tokio::select! {
            biased;
            terminal = cancellation.wait_terminal() => {
                let terminal = terminal?;
                return Err(terminal.error.unwrap_or_else(|| {
                    ServerError::new(
                        ErrorCode::Cancelled,
                        "persisted physical plan preparation was cancelled",
                    )
                    .with_query_id(execution.query_id())
                    .with_retryable(false)
                }));
            }
            prepared = prepare_persisted_plan(policy, sql, bindings, snapshot, profile) => {
                prepared.map_err(|error| {
                    plan_error(
                        execution.query_id(),
                        PersistedPlanStage::Prepare,
                        classify_persisted_plan_error(&error),
                        &error,
                    )
                })?
            }
        };
        if prepared.profile() != profile {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "prepared persisted plan profile differs from its admitted build profile",
            )
            .with_query_id(execution.query_id())
            .with_retryable(false));
        }
        Ok(Self {
            task,
            prepared,
            profile,
        })
    }

    fn into_parts(
        self,
    ) -> (
        QueryTaskPermit<()>,
        PreparedPersistedPlan,
        PersistedPlanProfile,
    ) {
        (self.task, self.prepared, self.profile)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NativeExecutionBinding {
    instance_id: QueryInstanceId,
    catalog_revision: u64,
}

struct NativeExecutionAuthority {
    binding: NativeExecutionBinding,
    snapshot_coordinator: NativeQuerySnapshotCoordinator,
    accepting_readers: AtomicBool,
    execution: Mutex<Option<QueryExecutionScope>>,
    #[cfg(test)]
    reader_admission_pause: Mutex<Option<Arc<ReaderAdmissionPause>>>,
}

#[cfg(test)]
struct ReaderAdmissionPause {
    entered: std::sync::Barrier,
    resume: std::sync::Barrier,
}

#[cfg(test)]
impl ReaderAdmissionPause {
    fn new() -> Self {
        Self {
            entered: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
        }
    }
}

impl NativeExecutionAuthority {
    fn new(
        binding: NativeExecutionBinding,
        snapshot_coordinator: NativeQuerySnapshotCoordinator,
        execution: QueryExecutionScope,
    ) -> Self {
        Self {
            binding,
            snapshot_coordinator,
            accepting_readers: AtomicBool::new(true),
            execution: Mutex::new(Some(execution)),
            #[cfg(test)]
            reader_admission_pause: Mutex::new(None),
        }
    }

    fn close_reader_admission(&self) {
        self.accepting_readers.store(false, Ordering::Release);
    }

    fn take_execution(&self) -> Option<QueryExecutionScope> {
        self.execution
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }

    fn cancellation(&self) -> Result<QueryCancellation> {
        let execution = self
            .execution
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        execution
            .as_ref()
            .map(QueryExecutionScope::cancellation)
            .ok_or_else(|| closed_execution(self.binding.instance_id))
    }

    fn admit_operator_context(
        &self,
        policy: QuerySessionPolicy,
        operator_pool_memory_bytes: NonZeroU64,
    ) -> Result<QueryRetainedPermit<PersistedExecutionContext>> {
        let retained_memory_bytes = operator_pool_memory_bytes
            .get()
            .checked_add(OPERATOR_CONTEXT_RETAINED_MEMORY_BYTES)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| operator_context_charge_overflow(self.binding.instance_id))?;
        let build_memory_bytes = retained_memory_bytes
            .get()
            .checked_add(OPERATOR_CONTEXT_BUILD_TRANSIENT_MEMORY_BYTES)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| operator_context_charge_overflow(self.binding.instance_id))?;
        let task = {
            let execution = self
                .execution
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            execution
                .as_ref()
                .ok_or_else(|| closed_execution(self.binding.instance_id))?
                .admit_build_task(QueryTaskCharge {
                    cpu_slots: NonZeroU32::new(PHYSICAL_BUILD_CPU_SLOTS)
                        .expect("operator runtime CPU charge is nonzero"),
                    memory_bytes: build_memory_bytes,
                })?
        };
        task.into_retained(|()| {
            let context = policy.create_execution_context(operator_pool_memory_bytes)?;
            Ok((
                context,
                QueryRetainedCharge {
                    kind: QueryRetainedKind::OperatorArena,
                    memory_bytes: retained_memory_bytes,
                },
            ))
        })
    }

    fn admit_result_batch(&self, batch: RecordBatch) -> Result<NativeQueryBatch> {
        let memory_bytes = result_batch_memory_bytes(&batch, self.binding.instance_id.query_id())?;
        let permit = {
            let execution = self
                .execution
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            execution
                .as_ref()
                .ok_or_else(|| closed_execution(self.binding.instance_id))?
                .admit_result(QueryResultCharge { memory_bytes }, batch)?
        };
        Ok(NativeQueryBatch {
            permit,
            memory_bytes,
        })
    }

    fn admit_execution_driver(&self, policy: QuerySessionPolicy) -> Result<QueryTaskPermit<()>> {
        let cpu_slots = u32::try_from(policy.target_partitions().get())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "native query target partitions do not fit the CPU resource ledger",
                )
                .with_query_id(self.binding.instance_id.query_id())
                .with_retryable(false)
            })?;
        let execution = self
            .execution
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        execution
            .as_ref()
            .ok_or_else(|| closed_execution(self.binding.instance_id))?
            .admit_build_task(QueryTaskCharge {
                cpu_slots,
                memory_bytes: NonZeroU64::new(EXECUTION_DRIVER_MEMORY_BYTES)
                    .expect("execution driver memory charge is nonzero"),
            })
    }

    fn open_reader(&self, description: PersistedFileDescription) -> Result<BoxedAsyncFileReader> {
        if !self.accepting_readers.load(Ordering::Acquire) {
            return Err(closed_execution(self.binding.instance_id));
        }
        #[cfg(test)]
        self.pause_reader_after_initial_gate();
        let execution = self
            .execution
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !self.accepting_readers.load(Ordering::Acquire) {
            return Err(closed_execution(self.binding.instance_id));
        }
        let execution = execution
            .as_ref()
            .ok_or_else(|| closed_execution(self.binding.instance_id))?;
        self.snapshot_coordinator.validate_execution_binding(
            execution,
            self.binding.instance_id,
            self.binding.catalog_revision,
        )?;
        self.snapshot_coordinator.deferred_described_file_reader(
            execution,
            QueryTaskCharge {
                cpu_slots: NonZeroU32::new(PERSISTED_FILE_TASK_CPU_SLOTS)
                    .expect("persisted file CPU charge is nonzero"),
                memory_bytes: NonZeroU64::new(PERSISTED_FILE_TASK_MEMORY_BYTES)
                    .expect("persisted file memory charge is nonzero"),
            },
            description,
        )
    }

    #[cfg(test)]
    fn pause_reader_after_initial_gate(&self) {
        let pause = self
            .reader_admission_pause
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(pause) = pause {
            pause.entered.wait();
            pause.resume.wait();
        }
    }
}

struct NativePersistedFileOpener {
    authority: Weak<NativeExecutionAuthority>,
    snapshot: PersistedSnapshotDescription,
}

impl PersistedFileOpener for NativePersistedFileOpener {
    fn snapshot_description(&self) -> &PersistedSnapshotDescription {
        &self.snapshot
    }

    fn open_reader(&self, request: PersistedFileOpenRequest) -> Result<BoxedAsyncFileReader> {
        let description = request.description();
        let expected = self
            .snapshot
            .table(description.table_index())
            .and_then(|table| table.files().get(description.file_index()))
            .copied()
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::InvalidArgument,
                    "persisted file request is absent from the execution snapshot",
                )
                .with_retryable(false)
            })?;
        if expected != description {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "persisted file request differs from the execution snapshot",
            )
            .with_table_id(description.table_id())
            .with_retryable(false));
        }
        let authority = self.authority.upgrade().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Cancelled,
                "native persisted query execution authority has expired",
            )
            .with_retryable(false)
        })?;
        authority.open_reader(description)
    }
}

/// One downstream-owned Arrow batch with an exact QueryService result reservation.
pub(crate) struct NativeQueryBatch {
    permit: QueryResultPermit<RecordBatch>,
    memory_bytes: NonZeroU64,
}

impl NativeQueryBatch {
    pub(crate) fn record_batch(&self) -> &RecordBatch {
        self.permit.as_ref()
    }

    pub(crate) const fn memory_bytes(&self) -> NonZeroU64 {
        self.memory_bytes
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.permit.is_cancelled()
    }
}

enum NativeBatchPoll {
    Terminal(Result<QueryTerminalSignal>),
    Batch(std::result::Result<Option<RecordBatch>, PersistedExecutionError>),
}

/// Pull-based native persisted result stream with lifecycle-bound resource accounting.
pub(crate) struct NativeQueryResultStream {
    inner: Option<PersistedRecordBatchStream>,
    execution_driver: Option<QueryTaskPermit<()>>,
    operator_context: Option<QueryRetainedPermit<PersistedExecutionContext>>,
    lease: Option<NativeExecutionLease>,
    terminal: bool,
}

impl NativeQueryResultStream {
    fn new(
        inner: PersistedRecordBatchStream,
        execution_driver: QueryTaskPermit<()>,
        operator_context: QueryRetainedPermit<PersistedExecutionContext>,
        lease: NativeExecutionLease,
    ) -> Self {
        Self {
            inner: Some(inner),
            execution_driver: Some(execution_driver),
            operator_context: Some(operator_context),
            lease: Some(lease),
            terminal: false,
        }
    }

    /// Returns the next accounted batch, or `None` after successful execution completion.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<NativeQueryBatch>> {
        if self.terminal {
            return Ok(None);
        }
        let lease = self
            .lease
            .as_ref()
            .expect("active native result stream owns its execution lease");
        let mut cancellation = lease.authority.cancellation()?;
        let outcome = {
            let inner = self
                .inner
                .as_mut()
                .expect("active native result stream owns its DataFusion stream");
            tokio::select! {
                biased;
                terminal = cancellation.wait_terminal() => NativeBatchPoll::Terminal(terminal),
                batch = next_persisted_batch(inner) => NativeBatchPoll::Batch(batch),
            }
        };
        match outcome {
            NativeBatchPoll::Terminal(terminal) => {
                let error = terminal
                    .map(terminal_execution_error)
                    .unwrap_or_else(|error| error);
                self.close_after_terminal();
                Err(error)
            }
            NativeBatchPoll::Batch(Ok(Some(batch))) => {
                let admitted = self
                    .lease
                    .as_ref()
                    .expect("active native result stream owns its execution lease")
                    .authority
                    .admit_result_batch(batch);
                match admitted {
                    Ok(batch) => Ok(Some(batch)),
                    Err(error) => Err(self.fail_execution(error)),
                }
            }
            NativeBatchPoll::Batch(Ok(None)) => {
                self.finish_execution()?;
                Ok(None)
            }
            NativeBatchPoll::Batch(Err(source)) => {
                let query_id = self
                    .lease
                    .as_ref()
                    .expect("active native result stream owns its execution lease")
                    .instance_id()
                    .query_id();
                let error = persisted_execution_error(query_id, &source);
                Err(self.fail_execution(error))
            }
        }
    }

    fn finish_execution(&mut self) -> Result<()> {
        self.terminal = true;
        drop(self.inner.take());
        drop(self.execution_driver.take());
        drop(self.operator_context.take());
        self.lease
            .take()
            .expect("active native result stream owns its execution lease")
            .finish()
    }

    fn fail_execution(&mut self, error: ServerError) -> ServerError {
        self.terminal = true;
        let terminal = match self.lease.take() {
            None => error,
            Some(lease) => match lease.fail(error.clone()) {
                Ok(()) => error,
                Err(terminal) => terminal,
            },
        };
        drop(self.inner.take());
        drop(self.execution_driver.take());
        drop(self.operator_context.take());
        terminal
    }

    fn close_after_terminal(&mut self) {
        self.terminal = true;
        drop(self.lease.take());
        drop(self.inner.take());
        drop(self.execution_driver.take());
        drop(self.operator_context.take());
    }
}

impl Drop for NativeQueryResultStream {
    fn drop(&mut self) {
        if !self.terminal {
            // Lease drop publishes cancellation before DataFusion tasks and reservations vanish.
            drop(self.lease.take());
        }
        drop(self.inner.take());
        drop(self.execution_driver.take());
        drop(self.operator_context.take());
    }
}

/// Non-cloneable ownership of one bound persisted plan and its native execution authority.
pub(crate) struct NativeExecutionLease {
    authority: Arc<NativeExecutionAuthority>,
    opener: Option<Arc<dyn PersistedFileOpener>>,
    retained_plan: Option<QueryRetainedPermit<BoundPersistedPlan>>,
}

impl NativeExecutionLease {
    /// Profiles, admits, prepares, and binds one persisted query under a single build reservation.
    pub(crate) async fn prepare(
        snapshot_coordinator: NativeQuerySnapshotCoordinator,
        execution: QueryExecutionScope,
        manifest: QueryRetainedPermit<ProjectedSnapshotManifest>,
        policy: QuerySessionPolicy,
        sql: &str,
        bindings: &[PersistedTableBinding],
        limits: PersistedPlanLimits,
    ) -> Result<Self> {
        let instance_id = execution.instance_id();
        if let Err(error) = snapshot_coordinator.validate_execution_binding(
            &execution,
            instance_id,
            manifest.resource().catalog_revision(),
        ) {
            return Err(publish_preparation_failure(execution, error));
        }
        if let Err(error) = validate_manifest_identity(instance_id, &manifest) {
            return Err(publish_preparation_failure(execution, error));
        }
        let admitted = match AdmittedPreparedPersistedPlan::prepare(
            &execution,
            policy,
            sql,
            bindings,
            manifest.resource().description(),
            limits,
        )
        .await
        {
            Ok(admitted) => admitted,
            Err(error) => return Err(publish_preparation_failure(execution, error)),
        };
        Self::bind_admitted(snapshot_coordinator, execution, manifest, admitted)
    }

    fn bind_admitted(
        snapshot_coordinator: NativeQuerySnapshotCoordinator,
        execution: QueryExecutionScope,
        manifest: QueryRetainedPermit<ProjectedSnapshotManifest>,
        admitted: AdmittedPreparedPersistedPlan,
    ) -> Result<Self> {
        let instance_id = execution.instance_id();
        let catalog_revision = manifest.resource().catalog_revision();
        if let Err(error) = snapshot_coordinator.validate_execution_binding(
            &execution,
            instance_id,
            catalog_revision,
        ) {
            return Err(publish_preparation_failure(execution, error));
        }
        if let Err(error) = validate_manifest_identity(instance_id, &manifest) {
            return Err(publish_preparation_failure(execution, error));
        }
        let binding = NativeExecutionBinding {
            instance_id,
            catalog_revision,
        };
        let authority = Arc::new(NativeExecutionAuthority::new(
            binding,
            snapshot_coordinator,
            execution,
        ));
        let opener: Arc<dyn PersistedFileOpener> = Arc::new(NativePersistedFileOpener {
            authority: Arc::downgrade(&authority),
            snapshot: manifest.resource().description().clone(),
        });
        let (task, prepared, profile) = admitted.into_parts();
        let retained_plan = manifest.rebuild_with_build_task(task, |manifest, ()| {
            if manifest.query_id() != instance_id.query_id()
                || manifest.catalog_revision() != catalog_revision
                || manifest.description() != opener.snapshot_description()
            {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "persisted manifest identity changed during physical plan binding",
                )
                .with_query_id(instance_id.query_id())
                .with_retryable(false));
            }
            let bound = prepared.bind(&opener).map_err(|error| {
                plan_error(
                    instance_id.query_id(),
                    PersistedPlanStage::Bind,
                    classify_persisted_plan_error(&error),
                    &error,
                )
            })?;
            Ok((
                bound,
                QueryRetainedCharge {
                    kind: QueryRetainedKind::PhysicalPlan,
                    memory_bytes: profile.retained_plan_bytes(),
                },
            ))
        });
        let retained_plan = match retained_plan {
            Ok(retained_plan) => retained_plan,
            Err(error) => {
                authority.close_reader_admission();
                let error = authority
                    .take_execution()
                    .map_or(error.clone(), |execution| {
                        publish_preparation_failure(execution, error)
                    });
                drop(opener);
                return Err(error);
            }
        };
        Ok(Self {
            authority,
            opener: Some(opener),
            retained_plan: Some(retained_plan),
        })
    }

    pub(crate) fn instance_id(&self) -> QueryInstanceId {
        self.authority.binding.instance_id
    }

    pub(crate) fn catalog_revision(&self) -> u64 {
        self.authority.binding.catalog_revision
    }

    pub(crate) fn plan(&self) -> &BoundPersistedPlan {
        self.retained_plan
            .as_ref()
            .expect("live native execution lease owns its retained physical plan")
            .resource()
    }

    /// Starts persisted execution under a hard DataFusion operator-memory reservation.
    pub(crate) fn execute(
        self,
        policy: QuerySessionPolicy,
        config: NativeQueryExecutionConfig,
    ) -> Result<NativeQueryResultStream> {
        let operator_context = match self
            .authority
            .admit_operator_context(policy, config.operator_pool_memory_bytes)
        {
            Ok(context) => context,
            Err(error) => return Err(publish_lease_failure(self, error)),
        };
        let execution_driver = match self.authority.admit_execution_driver(policy) {
            Ok(driver) => driver,
            Err(error) => {
                let terminal = publish_lease_failure(self, error);
                drop(operator_context);
                return Err(terminal);
            }
        };
        let inner = match self.plan().execute_stream(operator_context.resource()) {
            Ok(inner) => inner,
            Err(source) => {
                let error = persisted_execution_error(self.instance_id().query_id(), &source);
                let terminal = publish_lease_failure(self, error);
                drop(execution_driver);
                drop(operator_context);
                return Err(terminal);
            }
        };
        Ok(NativeQueryResultStream::new(
            inner,
            execution_driver,
            operator_context,
            self,
        ))
    }

    /// Closes file admission, releases the physical plan, and publishes successful completion.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.authority.close_reader_admission();
        drop(self.opener.take());
        // QueryService requires physical resources to drain before execution can complete.
        drop(self.retained_plan.take());
        let execution = self
            .authority
            .take_execution()
            .ok_or_else(|| closed_execution(self.instance_id()))?;
        execution.finish()
    }

    /// Publishes execution failure before releasing the opener and retained physical plan.
    pub(crate) fn fail(mut self, error: ServerError) -> Result<()> {
        self.authority.close_reader_admission();
        let execution = self
            .authority
            .take_execution()
            .ok_or_else(|| closed_execution(self.instance_id()))?;
        let failed = execution.fail(error);
        drop(self.opener.take());
        drop(self.retained_plan.take());
        failed
    }
}

impl Drop for NativeExecutionLease {
    fn drop(&mut self) {
        self.authority.close_reader_admission();
        // Scope drop publishes cancellation before any physical-plan authority is released.
        drop(self.authority.take_execution());
        drop(self.opener.take());
        drop(self.retained_plan.take());
    }
}

fn validate_manifest_identity(
    instance_id: QueryInstanceId,
    manifest: &QueryRetainedPermit<ProjectedSnapshotManifest>,
) -> Result<()> {
    if manifest.instance_id() != instance_id {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "projected persisted manifest belongs to another query instance",
        )
        .with_query_id(instance_id.query_id())
        .with_retryable(false));
    }
    if manifest.resource().query_id() != instance_id.query_id() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "projected persisted manifest identity differs from its retained reservation",
        )
        .with_query_id(instance_id.query_id())
        .with_retryable(false));
    }
    Ok(())
}

fn closed_execution(instance_id: QueryInstanceId) -> ServerError {
    ServerError::new(
        ErrorCode::Cancelled,
        "native persisted query reader admission is closed",
    )
    .with_query_id(instance_id.query_id())
    .with_retryable(false)
}

fn operator_context_charge_overflow(instance_id: QueryInstanceId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native query operator context memory charge overflowed",
    )
    .with_query_id(instance_id.query_id())
    .with_retryable(false)
}

fn publish_preparation_failure(execution: QueryExecutionScope, error: ServerError) -> ServerError {
    match execution.fail(error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
}

fn publish_lease_failure(lease: NativeExecutionLease, error: ServerError) -> ServerError {
    match lease.fail(error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
}

fn result_batch_memory_bytes(batch: &RecordBatch, query_id: QueryId) -> Result<NonZeroU64> {
    let arrays = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "native query result batch size does not fit the resource ledger",
        )
        .with_query_id(query_id)
        .with_retryable(false)
    })?;
    let schema = conservative_arrow_schema_retained_bytes(batch.schema().as_ref())
        .map_err(|error| error.with_query_id(query_id).with_retryable(false))?
        .get();
    let memory_bytes = arrays
        .checked_add(schema)
        .and_then(|bytes| bytes.checked_add(RESULT_BATCH_FIXED_MEMORY_BYTES))
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "native query result batch memory charge overflowed",
            )
            .with_query_id(query_id)
            .with_retryable(false)
        })?;
    Ok(memory_bytes)
}

fn terminal_execution_error(signal: QueryTerminalSignal) -> ServerError {
    signal.error.unwrap_or_else(|| {
        ServerError::new(
            ErrorCode::Cancelled,
            format!(
                "native persisted execution terminated before the next batch state=[{:?}]",
                signal.state
            ),
        )
        .with_query_id(signal.query_id)
        .with_retryable(false)
    })
}

fn persisted_execution_error(query_id: QueryId, error: &PersistedExecutionError) -> ServerError {
    if let Some(error) = structured_persisted_execution_error(error) {
        return error.with_query_id(query_id);
    }
    let class = classify_persisted_execution_error(error);
    let code = match class {
        PersistedExecutionErrorClass::ResourceExhausted => ErrorCode::ResourceExhausted,
        PersistedExecutionErrorClass::DataCorruption => ErrorCode::DataCorruption,
        PersistedExecutionErrorClass::Unsupported => ErrorCode::UnsupportedPlan,
        PersistedExecutionErrorClass::Internal => ErrorCode::Internal,
    };
    ServerError::new(
        code,
        format!("persisted physical plan execution failed error=[{error}]"),
    )
    .with_query_id(query_id)
    .with_retryable(class == PersistedExecutionErrorClass::ResourceExhausted)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedPlanStage {
    PreflightCharge,
    Profile,
    Prepare,
    Bind,
}

impl PersistedPlanStage {
    const fn label(self) -> &'static str {
        match self {
            Self::PreflightCharge => "preflight charge",
            Self::Profile => "profile",
            Self::Prepare => "prepare",
            Self::Bind => "bind",
        }
    }
}

fn plan_error(
    query_id: QueryId,
    stage: PersistedPlanStage,
    class: PersistedPlanErrorClass,
    error: impl std::fmt::Display,
) -> ServerError {
    let code = match class {
        PersistedPlanErrorClass::InvalidInput if stage == PersistedPlanStage::Bind => {
            ErrorCode::DataCorruption
        }
        PersistedPlanErrorClass::InvalidInput => ErrorCode::InvalidArgument,
        PersistedPlanErrorClass::Unsupported => ErrorCode::UnsupportedPlan,
        PersistedPlanErrorClass::ResourceExhausted => ErrorCode::ResourceExhausted,
        PersistedPlanErrorClass::Internal => ErrorCode::Internal,
    };
    ServerError::new(
        code,
        format!(
            "persisted physical plan {} failed error=[{error}]",
            stage.label()
        ),
    )
    .with_query_id(query_id)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use parquet::errors::ParquetError;
    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{OwnedDataDirectory, DEFAULT_QUERY_MAX_MEMORY_BYTES};
    use zippy_query::{
        PersistedFileDescription, PersistedSnapshotDescription, PersistedTableBinding,
        PersistedTableDescription, QuerySessionPolicy,
    };
    use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::query::QueryState;
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance,
        SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition, TableRef,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, QueryId,
        RequestId, ServerError, SessionId, TableId,
    };

    use super::{
        AdmittedPreparedPersistedPlan, NativeExecutionLease, NativeQueryExecutionConfig,
        ReaderAdmissionPause, EXECUTION_DRIVER_MEMORY_BYTES,
        OPERATOR_CONTEXT_RETAINED_MEMORY_BYTES, PERSISTED_FILE_TASK_MEMORY_BYTES,
        PHYSICAL_BUILD_CPU_SLOTS, RESULT_BATCH_FIXED_MEMORY_BYTES,
    };
    use crate::legacy_catalog_migration::LEGACY_STREAM_NAME_CONFIG_KEY;
    use crate::native_storage::{
        NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    };
    use crate::query_service::{
        QueryExecutionScope, QueryResourceBudget, QueryRetainedPermit, QueryServiceRegistryConfig,
        QuerySubmissionMetadata, QueryTaskCharge,
    };
    use crate::query_snapshot::ProjectedSnapshotManifest;

    const NAMESPACE: u128 = 1;
    const TABLE: u128 = 10;

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

    fn persist_request() -> PersistRequest {
        let offset = LogOffset::new(0);
        let offsets = OffsetRange::new(offset, offset).expect("single offset range");
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(vec![SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: 1,
                    generation: 1,
                    writer_epoch: 0,
                },
                offsets,
            }])
            .expect("one source segment lineage"),
        )
        .expect("valid native provenance");
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42_i64]))],
        )
        .expect("build persisted query batch");
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id: FileId::from_u128(100).expect("nonzero file ID"),
                table_id: table_id(TABLE),
                schema_version: 1,
                schema,
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: 1_752_492_600_000_000_100,
            },
            vec![batch],
        )
        .expect("build persisted query candidate");
        PersistRequest::new(
            request_id(100),
            table_id(TABLE),
            1,
            vec![candidate],
            OffsetWatermark::Known(offset),
        )
        .expect("build persist request")
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "query-execution-test".to_owned(),
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

    async fn seed_catalog(catalog: &SqliteCatalog, extra_revision: bool, active_table: bool) {
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
        let mut config = BTreeMap::new();
        if !active_table {
            config.insert(LEGACY_STREAM_NAME_CONFIG_KEY.to_owned(), "ticks".to_owned());
        }
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
                    config,
                }),
            ))
            .await
            .expect("create persisted-only table");
        if extra_revision {
            catalog
                .command(command(
                    3,
                    2,
                    CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                        namespace_id: namespace_id(2),
                        name: CatalogName::new("unused").expect("valid extra namespace name"),
                    }),
                ))
                .await
                .expect("advance Catalog revision");
        }
    }

    async fn start_runtime_with_table(extra_revision: bool, active_table: bool) -> TestRuntime {
        let temp = tempfile::tempdir().expect("create temp directory");
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database"))
                .expect("own test data directory"),
        );
        let catalog_config = SqliteCatalogConfig::new(&ownership);
        let catalog = SqliteCatalog::open(catalog_config.clone()).expect("open seed Catalog");
        seed_catalog(&catalog, extra_revision, active_table).await;
        catalog.close().expect("close seed Catalog");
        let runtime_config = NativeStorageRuntimeConfig::new(&ownership)
            .with_catalog_config(catalog_config)
            .expect("reuse Catalog ownership")
            .with_query_registry_config(QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                plan_ttl: Duration::from_nanos(10),
                ..QueryServiceRegistryConfig::default()
            });
        let runtime = NativeStorageRuntime::start(runtime_config).expect("start native storage");
        TestRuntime {
            _temp: temp,
            _ownership: ownership,
            runtime,
        }
    }

    async fn start_runtime(extra_revision: bool) -> TestRuntime {
        start_runtime_with_table(extra_revision, false).await
    }

    async fn start_runtime_with_file() -> TestRuntime {
        let test = start_runtime_with_table(false, true).await;
        test.runtime
            .table_actor(table_id(TABLE))
            .expect("native table actor")
            .persist(persist_request())
            .await
            .expect("persist one native Parquet file");
        test
    }

    fn budget() -> QueryResourceBudget {
        QueryResourceBudget {
            max_active_tasks: 4,
            max_cpu_slots: 4,
            max_memory_bytes: DEFAULT_QUERY_MAX_MEMORY_BYTES,
        }
    }

    fn policy() -> QuerySessionPolicy {
        QuerySessionPolicy::new(NonZeroUsize::new(1).expect("nonzero target partitions"))
    }

    fn execution_config() -> NativeQueryExecutionConfig {
        NativeQueryExecutionConfig::new(
            NonZeroU64::new(1024 * 1024).expect("nonzero operator memory limit"),
        )
    }

    fn limits() -> zippy_query::PersistedPlanLimits {
        zippy_query::PersistedPlanLimits::try_new(4096, 1, 2, 256, 512, 2048)
            .expect("valid persisted plan limits")
    }

    fn bindings() -> Vec<PersistedTableBinding> {
        vec![PersistedTableBinding::try_new("ticks", 0).expect("valid persisted binding")]
    }

    async fn begin_query(
        test: &TestRuntime,
        value: u128,
    ) -> (
        QueryExecutionScope,
        QueryRetainedPermit<ProjectedSnapshotManifest>,
    ) {
        let query = query_id(value);
        let resources = test.runtime.query_resource_coordinator();
        let owner = resources.owner(session_id(value));
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("submit native query");
        let snapshots = test.runtime.query_snapshot_coordinator();
        let snapshot = snapshots
            .pin_persisted(
                &planning,
                &[TableRef::Id {
                    table_id: table_id(TABLE),
                }],
            )
            .await
            .expect("pin persisted snapshot");
        let draft = snapshot.plan_draft();
        let token = resources
            .complete_native_planning_at_for_test(planning, draft, snapshot, 10)
            .expect("complete native planning");
        let execution = resources
            .begin_execution(token, 11)
            .expect("begin native execution");
        let manifest = snapshots
            .project_persisted_snapshot(&execution)
            .expect("project persisted snapshot");
        (execution, manifest)
    }

    async fn prepare_lease(test: &TestRuntime, value: u128) -> NativeExecutionLease {
        let (execution, manifest) = begin_query(test, value).await;
        NativeExecutionLease::prepare(
            test.runtime.query_snapshot_coordinator(),
            execution,
            manifest,
            policy(),
            "select price from ticks",
            &bindings(),
            limits(),
        )
        .await
        .expect("prepare native execution lease")
    }

    fn lease_cancellation(lease: &NativeExecutionLease) -> crate::query_service::QueryCancellation {
        lease
            .authority
            .execution
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .expect("live lease owns execution scope")
            .cancellation()
    }

    fn persisted_description(lease: &NativeExecutionLease) -> PersistedFileDescription {
        lease
            .opener
            .as_ref()
            .expect("live lease owns its file opener")
            .snapshot_description()
            .table(0)
            .and_then(|table| table.files().first())
            .copied()
            .expect("persisted snapshot contains one file")
    }

    #[derive(Clone, Copy)]
    enum LeaseTerminalAction {
        Finish,
        Fail,
        Drop,
    }

    async fn assert_second_reader_gate(action: LeaseTerminalAction, value: u128) {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, value).await;
        let cancellation = lease_cancellation(&lease);
        let authority = Arc::clone(&lease.authority);
        let description = persisted_description(&lease);
        let pause = Arc::new(ReaderAdmissionPause::new());
        *authority
            .reader_admission_pause
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::clone(&pause));
        let worker_authority = Arc::clone(&authority);
        let worker = std::thread::spawn(move || worker_authority.open_reader(description));
        pause.entered.wait();

        let expected_state = match action {
            LeaseTerminalAction::Finish => {
                lease.finish().expect("finish before reader admission");
                QueryState::Finished
            }
            LeaseTerminalAction::Fail => {
                lease
                    .fail(ServerError::new(
                        ErrorCode::Internal,
                        "physical execution failed before reader admission",
                    ))
                    .expect("fail before reader admission");
                QueryState::Failed
            }
            LeaseTerminalAction::Drop => {
                drop(lease);
                QueryState::Cancelled
            }
        };
        pause.resume.wait();
        let open_error = worker
            .join()
            .expect("join paused persisted reader")
            .err()
            .expect("second reader gate must reject the closed execution");
        assert_eq!(open_error.code(), ErrorCode::Cancelled);
        assert_eq!(
            cancellation
                .terminal_signal()
                .expect("lease action publishes one terminal signal")
                .state,
            expected_state
        );

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_cpu_slots, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert_eq!(status.tracked_sessions, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        test.shutdown();
    }

    async fn assert_reader_outlives_lease(action: LeaseTerminalAction, value: u128) {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, value).await;
        let cancellation = lease_cancellation(&lease);
        let authority = Arc::clone(&lease.authority);
        let description = persisted_description(&lease);
        let reader = authority
            .open_reader(description)
            .expect("admit one real persisted reader");

        let expected_error = match action {
            LeaseTerminalAction::Fail => {
                let error = ServerError::new(
                    ErrorCode::Internal,
                    "physical execution failed with an admitted reader",
                )
                .with_query_id(lease.instance_id().query_id());
                lease
                    .fail(error.clone())
                    .expect("publish failure with admitted reader");
                Some(error)
            }
            LeaseTerminalAction::Drop => {
                drop(lease);
                None
            }
            LeaseTerminalAction::Finish => panic!("finish has a separate conflict test"),
        };
        let terminal = cancellation
            .terminal_signal()
            .expect("lease action publishes a terminal signal");
        match expected_error {
            Some(error) => {
                assert_eq!(terminal.state, QueryState::Failed);
                assert_eq!(terminal.error, Some(error));
            }
            None => assert_eq!(terminal.state, QueryState::Cancelled),
        }
        let closed = authority
            .open_reader(description)
            .err()
            .expect("terminated lease rejects another persisted reader");
        assert_eq!(closed.code(), ErrorCode::Cancelled);

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_cpu_slots, 1);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(
            test.runtime
                .runtime_query_snapshot_pins()
                .expect("read retained query snapshot pins")
                .len(),
            1
        );
        drop(reader);

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_cpu_slots, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert_eq!(status.tracked_sessions, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        test.shutdown();
    }

    fn deferred_open_server_error(error: &ParquetError) -> &ServerError {
        match error {
            ParquetError::External(source) => source
                .downcast_ref::<ServerError>()
                .expect("deferred native reader preserves its structured ServerError"),
            other => panic!("deferred native reader returned an unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn successful_finish_failure_and_drop_release_execution_authority() {
        let mut test = start_runtime(false).await;

        let finished = prepare_lease(&test, 101).await;
        let finished_instance = finished.instance_id();
        assert_eq!(finished_instance.query_id(), query_id(101));
        assert!(finished.catalog_revision() > 0);
        assert_eq!(finished.plan().bound_scan_count(), 0);
        let finished_signal = lease_cancellation(&finished);
        finished.finish().expect("finish native execution");
        assert_eq!(
            finished_signal
                .terminal_signal()
                .expect("finished signal")
                .state,
            QueryState::Finished
        );

        let failed = prepare_lease(&test, 102).await;
        let failed_signal = lease_cancellation(&failed);
        failed
            .fail(ServerError::new(
                ErrorCode::Internal,
                "physical execution failed",
            ))
            .expect("fail native execution");
        let terminal = failed_signal.terminal_signal().expect("failed signal");
        assert_eq!(terminal.state, QueryState::Failed);
        assert_eq!(
            terminal.error.expect("failure reason").code(),
            ErrorCode::Internal
        );

        let dropped = prepare_lease(&test, 103).await;
        let dropped_signal = lease_cancellation(&dropped);
        drop(dropped);
        assert_eq!(
            dropped_signal
                .terminal_signal()
                .expect("drop cancellation signal")
                .state,
            QueryState::Cancelled
        );

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_retained_memory_bytes, 0);
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_stream_accounts_batches_and_finishes_after_delivery_drains() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 121).await;
        let instance_id = lease.instance_id();
        let admin = test.runtime.query_resource_coordinator().admin_handle();
        let before_execute = admin
            .status(instance_id)
            .expect("read retained plan resources before execution");
        let config = execution_config();
        let mut stream = lease
            .execute(policy(), config)
            .expect("start bounded native persisted execution");
        let executing = admin
            .status(instance_id)
            .expect("read bounded operator resources after execution starts");
        assert_eq!(
            executing.active_retained_permits,
            before_execute.active_retained_permits + 1
        );
        assert_eq!(
            executing.active_retained_memory_bytes,
            before_execute.active_retained_memory_bytes
                + config.operator_pool_memory_bytes.get()
                + OPERATOR_CONTEXT_RETAINED_MEMORY_BYTES
        );
        assert_eq!(executing.active_tasks, 1);
        assert_eq!(executing.active_cpu_slots, 1);
        assert_eq!(
            executing.active_task_memory_bytes,
            EXECUTION_DRIVER_MEMORY_BYTES
        );

        let batch = stream
            .next_batch()
            .await
            .expect("read first native batch")
            .expect("persisted query returns one batch");
        let values = batch
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("price column is Int64");
        assert_eq!(values.values(), &[42]);
        assert!(batch.memory_bytes().get() >= RESULT_BATCH_FIXED_MEMORY_BYTES);
        assert!(!batch.is_cancelled());
        let delivering = admin
            .status(instance_id)
            .expect("query remains visible while delivering a batch");
        assert_eq!(delivering.state, QueryState::Running);
        assert!(!delivering.execution_complete);
        // The Parquet stream may still retain its final accounted positioned-read buffer.
        assert!(delivering.active_result_permits >= 1);
        assert!(delivering.active_result_memory_bytes >= batch.memory_bytes().get());
        assert!(delivering.active_retained_permits >= 2);

        assert!(stream
            .next_batch()
            .await
            .expect("complete native persisted stream")
            .is_none());
        let draining = admin
            .status(instance_id)
            .expect("completed query remains visible until delivery drains");
        assert_eq!(draining.state, QueryState::Running);
        assert!(draining.execution_complete);
        assert!(draining.logical_admitted);
        assert_eq!(draining.active_tasks, 0);
        assert_eq!(draining.active_retained_permits, 0);
        assert_eq!(draining.active_result_permits, 1);
        assert_eq!(
            test.runtime
                .runtime_query_snapshot_pins()
                .expect("read delivery-held query snapshot pins")
                .len(),
            1
        );
        assert!(stream
            .next_batch()
            .await
            .expect("completed stream remains fused")
            .is_none());

        drop(batch);
        let finished = admin
            .status(instance_id)
            .expect("finished query remains available during terminal retention");
        assert_eq!(finished.state, QueryState::Finished);
        assert!(finished.execution_complete);
        assert!(!finished.logical_admitted);
        assert_eq!(finished.active_result_permits, 0);
        assert_eq!(finished.active_memory_bytes, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        assert_eq!(
            test.runtime
                .query_resource_coordinator()
                .status()
                .tracked_sessions,
            0
        );
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_native_stream_cancels_before_result_reservation_release() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 122).await;
        let instance_id = lease.instance_id();
        let admin = test.runtime.query_resource_coordinator().admin_handle();
        let mut stream = lease
            .execute(policy(), execution_config())
            .expect("start bounded native persisted execution");
        let batch = stream
            .next_batch()
            .await
            .expect("read first native batch")
            .expect("persisted query returns one batch");

        drop(stream);
        let cancelled = admin
            .status(instance_id)
            .expect("cancelled query remains visible while its batch is delivered");
        assert_eq!(cancelled.state, QueryState::Cancelled);
        assert!(cancelled.cancellation_requested);
        assert_eq!(cancelled.active_tasks, 0);
        assert_eq!(cancelled.active_retained_permits, 0);
        assert_eq!(cancelled.active_result_permits, 1);
        assert!(batch.is_cancelled());
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read cancelled query snapshot pins")
            .is_empty());

        drop(batch);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_result_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert_eq!(status.tracked_sessions, 0);
        test.shutdown();
    }

    #[tokio::test]
    async fn operator_context_budget_rejection_fails_and_releases_execution_lease() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 123).await;
        let instance_id = lease.instance_id();
        let cancellation = lease_cancellation(&lease);
        let error = lease
            .execute(
                policy(),
                NativeQueryExecutionConfig::new(
                    NonZeroU64::new(DEFAULT_QUERY_MAX_MEMORY_BYTES)
                        .expect("nonzero oversized operator pool"),
                ),
            )
            .err()
            .expect("operator context must share the existing query budget");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        let terminal = cancellation
            .terminal_signal()
            .expect("operator admission failure publishes a terminal signal");
        assert_eq!(terminal.state, QueryState::Failed);
        assert_eq!(terminal.error, Some(error));

        let status = test
            .runtime
            .query_resource_coordinator()
            .admin_handle()
            .status(instance_id)
            .expect("failed query remains visible during terminal retention");
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read failed query snapshot pins")
            .is_empty());
        assert_eq!(
            test.runtime
                .query_resource_coordinator()
                .status()
                .tracked_sessions,
            0
        );
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_admission_second_gate_linearizes_finish_fail_and_drop() {
        assert_second_reader_gate(LeaseTerminalAction::Finish, 151).await;
        assert_second_reader_gate(LeaseTerminalAction::Fail, 152).await;
        assert_second_reader_gate(LeaseTerminalAction::Drop, 153).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_reader_fail_and_drop_keep_task_pin_until_reader_drop() {
        assert_reader_outlives_lease(LeaseTerminalAction::Fail, 161).await;
        assert_reader_outlives_lease(LeaseTerminalAction::Drop, 162).await;
    }

    #[tokio::test]
    async fn finish_with_outstanding_task_cancels_and_leaves_only_that_task_accounted() {
        let mut test = start_runtime(false).await;
        let lease = prepare_lease(&test, 201).await;
        let outstanding = lease
            .authority
            .execution
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .expect("live lease owns execution scope")
            .admit_build_task(QueryTaskCharge {
                cpu_slots: NonZeroU32::new(PHYSICAL_BUILD_CPU_SLOTS)
                    .expect("nonzero test CPU charge"),
                memory_bytes: NonZeroU64::new(1).expect("nonzero test memory charge"),
            })
            .expect("admit outstanding task");
        let cancellation = outstanding.cancellation();

        let error = lease
            .finish()
            .expect_err("finish must reject outstanding physical work");
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert!(cancellation.is_cancelled());
        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_retained_permits, 0);

        drop(outstanding);
        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_reader_finish_closes_admission_and_drains_task_and_snapshot_pin() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 251).await;
        assert_eq!(lease.plan().bound_scan_count(), 1);
        let cancellation = lease_cancellation(&lease);
        let authority = Arc::clone(&lease.authority);
        let description = lease
            .opener
            .as_ref()
            .expect("live lease owns its file opener")
            .snapshot_description()
            .table(0)
            .and_then(|table| table.files().first())
            .copied()
            .expect("persisted snapshot contains one file");
        let reader = authority
            .open_reader(description)
            .expect("admit one real persisted reader");

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_retained_permits, 1);
        assert_eq!(
            test.runtime
                .runtime_query_snapshot_pins()
                .expect("read active query snapshot pins")
                .len(),
            1
        );

        let error = lease
            .finish()
            .expect_err("finish must reject the admitted persisted reader");
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(
            cancellation
                .terminal_signal()
                .expect("failed finish publishes cancellation")
                .state,
            QueryState::Cancelled
        );
        let closed = authority
            .open_reader(description)
            .err()
            .expect("closed execution must reject another persisted reader");
        assert_eq!(closed.code(), ErrorCode::Cancelled);

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 1);
        assert_eq!(status.active_retained_permits, 0);
        drop(reader);

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_lease_with_reader_finish_returns_first_terminal_error() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 252).await;
        let instance_id = lease.instance_id();
        let cancellation = lease_cancellation(&lease);
        let authority = Arc::clone(&lease.authority);
        let description = lease
            .opener
            .as_ref()
            .expect("live lease owns its file opener")
            .snapshot_description()
            .table(0)
            .and_then(|table| table.files().first())
            .copied()
            .expect("persisted snapshot contains one file");
        let reader = authority
            .open_reader(description)
            .expect("admit one real persisted reader");

        let admin = test.runtime.query_resource_coordinator().admin_handle();
        admin
            .cancel(instance_id)
            .expect("administrator cancellation wins the terminal transition");
        let terminal = cancellation
            .terminal_signal()
            .expect("administrator cancellation publishes a terminal signal");
        let status = admin
            .status(instance_id)
            .expect("cancelled query remains visible while its reader is active");
        assert_eq!(terminal.state, QueryState::Cancelled);
        assert_eq!(terminal.state, status.state);
        assert_eq!(terminal.error, status.error);

        let error = lease
            .finish()
            .expect_err("late finish must return the first terminal error");
        assert_eq!(Some(error), terminal.error);
        let closed = authority
            .open_reader(description)
            .err()
            .expect("late finish closes persisted reader admission");
        assert_eq!(closed.code(), ErrorCode::Cancelled);

        drop(reader);
        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        test.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_footer_growth_rejection_is_sticky_and_releases_task_and_pin() {
        let mut test = start_runtime_with_file().await;
        let lease = prepare_lease(&test, 253).await;
        let instance_id = lease.instance_id();
        let authority = Arc::clone(&lease.authority);
        let description = persisted_description(&lease);
        let mut reader = authority
            .open_reader(description)
            .expect("admit one real persisted reader");
        let admin = test.runtime.query_resource_coordinator().admin_handle();
        let before_fill = admin
            .status(instance_id)
            .expect("read query resources before filling its budget");
        assert_eq!(
            before_fill.active_task_memory_bytes,
            PERSISTED_FILE_TASK_MEMORY_BYTES
        );
        let filler_memory = DEFAULT_QUERY_MAX_MEMORY_BYTES
            .checked_sub(before_fill.active_memory_bytes)
            .and_then(NonZeroU64::new)
            .expect("query has positive memory remaining after reader admission");
        let filler = {
            let execution = authority
                .execution
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            execution
                .as_ref()
                .expect("live lease owns execution scope")
                .admit_build_task(QueryTaskCharge {
                    cpu_slots: NonZeroU32::new(1).expect("nonzero filler CPU charge"),
                    memory_bytes: filler_memory,
                })
                .expect("fill the query memory budget after reader admission")
        };
        assert_eq!(
            admin
                .status(instance_id)
                .expect("read full query memory budget")
                .active_memory_bytes,
            DEFAULT_QUERY_MAX_MEMORY_BYTES
        );

        let first_error = reader
            .get_metadata()
            .await
            .expect_err("footer charge growth must be rejected at the full budget");
        assert_eq!(
            deferred_open_server_error(&first_error).code(),
            ErrorCode::ResourceExhausted
        );
        let after_first = admin
            .status(instance_id)
            .expect("failed deferred open remains query-visible");
        assert_eq!(after_first.active_tasks, 1);
        assert_eq!(after_first.active_task_memory_bytes, filler_memory.get());
        assert_eq!(
            after_first.active_memory_bytes,
            DEFAULT_QUERY_MAX_MEMORY_BYTES - PERSISTED_FILE_TASK_MEMORY_BYTES
        );

        let second_error = reader
            .get_metadata()
            .await
            .expect_err("deferred footer growth failure remains sticky");
        assert_eq!(
            deferred_open_server_error(&second_error).code(),
            ErrorCode::ResourceExhausted
        );
        assert_eq!(
            admin
                .status(instance_id)
                .expect("sticky failure does not change resource accounting"),
            after_first
        );

        drop(filler);
        lease
            .fail(
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "persisted Parquet metadata exceeded the query memory budget",
                )
                .with_query_id(instance_id.query_id()),
            )
            .expect("publish footer admission failure");
        drop(reader);

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_cpu_slots, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_memory_bytes, 0);
        assert_eq!(status.tracked_sessions, 0);
        assert!(test
            .runtime
            .runtime_query_snapshot_pins()
            .expect("read drained query snapshot pins")
            .is_empty());
        test.shutdown();
    }

    #[tokio::test]
    async fn foreign_instance_revision_and_storage_issuer_fail_before_preparation() {
        let mut first = start_runtime(false).await;
        let mut same_revision_foreign = start_runtime(false).await;
        let mut newer_revision = start_runtime(true).await;

        let (unused_execution, first_manifest) = begin_query(&first, 301).await;
        drop(unused_execution);
        let (foreign_execution, foreign_manifest) = begin_query(&first, 302).await;
        drop(foreign_manifest);
        let foreign_signal = foreign_execution.cancellation();
        let instance_error = NativeExecutionLease::prepare(
            first.runtime.query_snapshot_coordinator(),
            foreign_execution,
            first_manifest,
            policy(),
            "select price from ticks",
            &bindings(),
            limits(),
        )
        .await
        .err()
        .expect("foreign query instance must fail closed");
        assert_eq!(instance_error.code(), ErrorCode::InvalidArgument);
        assert_eq!(
            foreign_signal
                .terminal_signal()
                .expect("foreign binding publishes failure")
                .state,
            QueryState::Failed
        );

        let (issuer_execution, issuer_manifest) = begin_query(&first, 303).await;
        let issuer_error = NativeExecutionLease::prepare(
            same_revision_foreign.runtime.query_snapshot_coordinator(),
            issuer_execution,
            issuer_manifest,
            policy(),
            "select price from ticks",
            &bindings(),
            limits(),
        )
        .await
        .err()
        .expect("foreign storage issuer must fail closed");
        assert_eq!(issuer_error.code(), ErrorCode::InvalidArgument);

        let (old_execution, old_manifest) = begin_query(&first, 304).await;
        let old_revision = old_manifest.resource().catalog_revision();
        drop(old_execution);
        let (new_execution, new_manifest) = begin_query(&newer_revision, 304).await;
        let new_revision = new_manifest.resource().catalog_revision();
        drop(new_manifest);
        assert_ne!(old_revision, new_revision);
        let revision_error = NativeExecutionLease::prepare(
            newer_revision.runtime.query_snapshot_coordinator(),
            new_execution,
            old_manifest,
            policy(),
            "select price from ticks",
            &bindings(),
            limits(),
        )
        .await
        .err()
        .expect("foreign Catalog revision must fail closed");
        assert_eq!(revision_error.code(), ErrorCode::DataCorruption);

        for test in [&first, &same_revision_foreign, &newer_revision] {
            let status = test.runtime.query_resource_coordinator().status();
            assert_eq!(status.active_tasks, 0);
            assert_eq!(status.active_retained_permits, 0);
        }
        first.shutdown();
        same_revision_foreign.shutdown();
        newer_revision.shutdown();
    }

    #[tokio::test]
    async fn preflight_and_profile_failures_publish_failed_and_release_resources() {
        let mut test = start_runtime(false).await;

        let (preflight_execution, preflight_manifest) = begin_query(&test, 351).await;
        let preflight_signal = preflight_execution.cancellation();
        let oversized_limits =
            zippy_query::PersistedPlanLimits::try_new(4096, 1, 2, 256, 512, 1_000_000)
                .expect("build oversized preflight limits");
        let preflight_error = NativeExecutionLease::prepare(
            test.runtime.query_snapshot_coordinator(),
            preflight_execution,
            preflight_manifest,
            policy(),
            "select price from ticks",
            &bindings(),
            oversized_limits,
        )
        .await
        .err()
        .expect("oversized preflight must fail admission");
        assert_eq!(preflight_error.code(), ErrorCode::ResourceExhausted);
        let preflight_terminal = preflight_signal
            .terminal_signal()
            .expect("preflight rejection publishes failure");
        assert_eq!(preflight_terminal.state, QueryState::Failed);
        assert_eq!(
            preflight_terminal
                .error
                .expect("preflight failure reason")
                .code(),
            ErrorCode::ResourceExhausted
        );

        let (profile_execution, profile_manifest) = begin_query(&test, 352).await;
        let profile_signal = profile_execution.cancellation();
        let profile_error = NativeExecutionLease::prepare(
            test.runtime.query_snapshot_coordinator(),
            profile_execution,
            profile_manifest,
            policy(),
            "select (",
            &bindings(),
            limits(),
        )
        .await
        .err()
        .expect("malformed SQL must fail profile");
        assert_eq!(profile_error.code(), ErrorCode::InvalidArgument);
        assert_eq!(
            profile_signal
                .terminal_signal()
                .expect("profile rejection publishes failure")
                .state,
            QueryState::Failed
        );

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_retained_memory_bytes, 0);
        test.shutdown();
    }

    #[tokio::test]
    async fn bind_failure_rolls_back_build_and_manifest_reservations() {
        let mut test = start_runtime(false).await;
        let (execution, manifest) = begin_query(&test, 401).await;
        let cancellation = execution.cancellation();
        let wrong_table = PersistedTableDescription::try_new(
            0,
            table_id(999),
            NonZeroU64::new(1).expect("nonzero snapshot version"),
            NonZeroU64::new(1).expect("nonzero schema version"),
            schema(),
            Vec::new(),
        )
        .expect("build mismatched persisted table");
        let wrong_snapshot = PersistedSnapshotDescription::try_new(vec![wrong_table])
            .expect("build mismatched snapshot");
        let admitted = AdmittedPreparedPersistedPlan::prepare(
            &execution,
            policy(),
            "select price from ticks",
            &bindings(),
            &wrong_snapshot,
            limits(),
        )
        .await
        .expect("prepare mismatched plan under build admission");
        let error = NativeExecutionLease::bind_admitted(
            test.runtime.query_snapshot_coordinator(),
            execution,
            manifest,
            admitted,
        )
        .err()
        .expect("snapshot mismatch must reject physical binding");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(cancellation.is_cancelled());
        let terminal = cancellation
            .terminal_signal()
            .expect("binding failure publishes a terminal signal");
        assert_eq!(terminal.state, QueryState::Failed);
        assert_eq!(
            terminal.error.expect("binding failure reason").code(),
            ErrorCode::DataCorruption
        );

        let status = test.runtime.query_resource_coordinator().status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_retained_memory_bytes, 0);
        test.shutdown();
    }
}
