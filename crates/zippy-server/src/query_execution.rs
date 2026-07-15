//! Lifecycle-bound execution of persisted native query plans.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use zippy_query::{
    classify_persisted_plan_error, prepare_persisted_plan, profile_persisted_plan,
    BoundPersistedPlan, BoxedAsyncFileReader, PersistedFileDescription, PersistedFileOpenRequest,
    PersistedFileOpener, PersistedPlanErrorClass, PersistedPlanLimits, PersistedPlanProfile,
    PersistedSnapshotDescription, PersistedTableBinding, PreparedPersistedPlan, QuerySessionPolicy,
};
use zippy_types::query::QueryInstanceId;
use zippy_types::{ErrorCode, QueryId, Result, ServerError};

use crate::query_service::{
    QueryExecutionScope, QueryRetainedCharge, QueryRetainedKind, QueryRetainedPermit,
    QueryTaskCharge, QueryTaskPermit,
};
use crate::query_snapshot::{NativeQuerySnapshotCoordinator, ProjectedSnapshotManifest};

const PHYSICAL_BUILD_CPU_SLOTS: u32 = 1;
const PERSISTED_FILE_TASK_CPU_SLOTS: u32 = 1;
const PERSISTED_FILE_TASK_MEMORY_BYTES: u64 = 64 * 1024;

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

    fn open_reader(&self, description: PersistedFileDescription) -> Result<BoxedAsyncFileReader> {
        if !self.accepting_readers.load(Ordering::Acquire) {
            return Err(closed_execution(self.binding.instance_id));
        }
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

    /// Closes file admission, releases the physical plan, and publishes successful completion.
    pub(crate) fn finish(mut self) -> Result<()> {
        self.authority.close_reader_admission();
        drop(self.opener.take());
        // QueryService requires all retained resources to drain before entering Finished.
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

fn publish_preparation_failure(execution: QueryExecutionScope, error: ServerError) -> ServerError {
    match execution.fail(error.clone()) {
        Ok(()) => error,
        Err(terminal) => terminal,
    }
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

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{OwnedDataDirectory, DEFAULT_QUERY_MAX_MEMORY_BYTES};
    use zippy_query::{
        PersistedSnapshotDescription, PersistedTableBinding, PersistedTableDescription,
        QuerySessionPolicy,
    };
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::query::QueryState;
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, SchemaIpcPayload, TableDefinition, TableRef,
    };
    use zippy_types::{
        ErrorCode, NamespaceId, QueryId, RequestId, ServerError, SessionId, TableId,
    };

    use super::{AdmittedPreparedPersistedPlan, NativeExecutionLease, PHYSICAL_BUILD_CPU_SLOTS};
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

    async fn seed_catalog(catalog: &SqliteCatalog, extra_revision: bool) {
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
        config.insert(LEGACY_STREAM_NAME_CONFIG_KEY.to_owned(), "ticks".to_owned());
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

    async fn start_runtime(extra_revision: bool) -> TestRuntime {
        let temp = tempfile::tempdir().expect("create temp directory");
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database"))
                .expect("own test data directory"),
        );
        let catalog_config = SqliteCatalogConfig::new(&ownership);
        let catalog = SqliteCatalog::open(catalog_config.clone()).expect("open seed Catalog");
        seed_catalog(&catalog, extra_revision).await;
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
