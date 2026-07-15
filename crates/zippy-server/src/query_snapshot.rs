//! Lifecycle-bound native query snapshots over table actors and the SQLite Catalog.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
use tokio::sync::Semaphore;
use zippy_catalog::{
    PinnedTableManifest, SqliteCatalogPinnedQuerySnapshot, SqliteCatalogQuerySnapshotPinRequest,
    SqliteCatalogReadHandle,
};
use zippy_ingest::{TableActorRegistryHandle, TableActorSnapshot};
use zippy_types::catalog::{CatalogRead, CatalogSnapshotSelector};
use zippy_types::query::{QueryPlanInfo, QueryTableVersion};
use zippy_types::storage::{
    OpaqueQueryReadSnapshot, QueryReadSnapshotDescriptor, QueryReadSnapshotGuard,
    TableReadSnapshotDescriptor,
};
use zippy_types::table::{TableDescriptor, TableRef};
use zippy_types::{
    BoxFuture, ErrorCode, LeaseId, OffsetWatermark, QueryId, Result, ServerError, TableId,
};

use crate::query_service::{
    QueryExecutionScope, QueryPlanningToken, QueryTaskCharge, QueryTaskPermit,
};

const MAX_SNAPSHOT_COORDINATION_ATTEMPTS: usize = 3;
static NEXT_SNAPSHOT_COORDINATOR_NONCE: AtomicU64 = AtomicU64::new(1);

/// Coordinates serialized table observations with exact, GC-protected Catalog manifests.
#[derive(Clone)]
pub(crate) struct NativeQuerySnapshotCoordinator {
    inner: Arc<NativeQuerySnapshotCoordinatorInner>,
}

struct NativeQuerySnapshotCoordinatorInner {
    catalog: SqliteCatalogReadHandle,
    table_actors: TableActorRegistryHandle,
    nonce: u64,
    accepting: AtomicBool,
    active_plans: AtomicUsize,
    next_lease_sequence: AtomicU64,
    #[cfg(test)]
    pin_stall: Mutex<Option<TestPinStall>>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestPinStall {
    stage: TestPinStage,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TestPinStage {
    AfterCatalogPin,
}

impl NativeQuerySnapshotCoordinator {
    pub(crate) fn new(
        catalog: SqliteCatalogReadHandle,
        table_actors: TableActorRegistryHandle,
    ) -> Self {
        Self {
            inner: Arc::new(NativeQuerySnapshotCoordinatorInner {
                catalog,
                table_actors,
                nonce: next_coordinator_nonce(),
                accepting: AtomicBool::new(true),
                active_plans: AtomicUsize::new(0),
                next_lease_sequence: AtomicU64::new(1),
                #[cfg(test)]
                pin_stall: Mutex::new(None),
            }),
        }
    }

    /// Pins one immutable, persisted-only query boundary.
    ///
    /// The borrowed planning token proves QueryService registered the exact query generation
    /// before any Catalog or table-actor work begins and carries cancellation through pinning.
    ///
    /// Table references are resolved in one Catalog transaction, deduplicated by immutable table
    /// identity, and then observed in stable table-ID order. A successful return owns every
    /// process-local Catalog pin required to keep its physical files outside Safe-GC.
    pub(crate) async fn pin_persisted(
        &self,
        planning: &QueryPlanningToken,
        tables: &[TableRef],
    ) -> Result<NativeQueryReadSnapshot> {
        let query_id = planning.instance_id().query_id();
        self.ensure_planning_active(planning)?;
        let mut cancellation = planning.cancellation();
        let _admission = self.admit_planning(query_id)?;
        let mut last_retryable_conflict = None;
        for attempt in 0..MAX_SNAPSHOT_COORDINATION_ATTEMPTS {
            self.ensure_planning_active(planning)?;
            self.ensure_accepting(query_id)?;
            // Every awaited planning stage is owned by this one future. Dropping it abandons only
            // oneshot responses; Catalog runtime pins inside it are synchronous RAII guards.
            let outcome = tokio::select! {
                biased;
                terminal = cancellation.wait_terminal() => {
                    return Err(terminal?.error.unwrap_or_else(|| {
                        ServerError::new(ErrorCode::Cancelled, "query planning was cancelled")
                            .with_query_id(query_id)
                            .with_retryable(false)
                    }));
                }
                outcome = self.pin_persisted_once(query_id, tables) => outcome,
            };
            self.ensure_planning_active(planning)?;
            match outcome {
                Ok(snapshot) => {
                    return Ok(snapshot);
                }
                Err(error)
                    if error.code() == ErrorCode::Conflict
                        && error.retryable()
                        && attempt + 1 < MAX_SNAPSHOT_COORDINATION_ATTEMPTS =>
                {
                    last_retryable_conflict = Some(error);
                    tokio::select! {
                        biased;
                        terminal = cancellation.wait_terminal() => {
                            return Err(terminal?.error.unwrap_or_else(|| {
                                ServerError::new(
                                    ErrorCode::Cancelled,
                                    "query planning was cancelled",
                                )
                                .with_query_id(query_id)
                                .with_retryable(false)
                            }));
                        }
                        () = tokio::task::yield_now() => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_retryable_conflict.unwrap_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "native query snapshot retry loop ended without an outcome",
            )
            .with_query_id(query_id)
            .with_retryable(false)
        }))
    }

    fn ensure_planning_active(&self, planning: &QueryPlanningToken) -> Result<()> {
        if !planning.is_cancelled() {
            return Ok(());
        }
        let query_id = planning.instance_id().query_id();
        if let Some(error) = planning.terminal_error() {
            return Err(error);
        }
        Err(ServerError::new(
            ErrorCode::Cancelled,
            "query planning cancellation is pending",
        )
        .with_query_id(query_id)
        .with_retryable(false))
    }

    #[cfg(test)]
    fn install_pin_stall(&self, stage: TestPinStage) -> TestPinStall {
        let stall = TestPinStall {
            stage,
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        };
        let previous = self
            .inner
            .pin_stall
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(stall.clone());
        assert!(previous.is_none(), "only one pin stall may be installed");
        stall
    }

    pub(crate) fn close_admission(&self) {
        self.inner.accepting.store(false, Ordering::Release);
    }

    pub(crate) fn planning_drained(&self) -> bool {
        self.inner.active_plans.load(Ordering::Acquire) == 0
    }

    fn admit_planning(&self, query_id: QueryId) -> Result<SnapshotPlanningAdmission> {
        self.ensure_accepting(query_id)?;
        self.inner
            .active_plans
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "native query snapshot planning counter is exhausted",
                )
                .with_query_id(query_id)
                .with_retryable(true)
            })?;
        if let Err(error) = self.ensure_accepting(query_id) {
            self.inner.active_plans.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(SnapshotPlanningAdmission {
            inner: Arc::clone(&self.inner),
        })
    }

    fn ensure_accepting(&self, query_id: QueryId) -> Result<()> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "native query snapshot admission is closed",
            )
            .with_query_id(query_id)
            .with_retryable(false));
        }
        Ok(())
    }

    fn validate_origin(&self, snapshot: &NativeQueryReadSnapshot) -> Result<()> {
        if snapshot.coordinator_nonce != self.inner.nonce {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "native query snapshot belongs to another storage runtime",
            )
            .with_query_id(snapshot.descriptor.query_id)
            .with_retryable(false));
        }
        Ok(())
    }

    /// Admits one table task before cloning its capability from the pinned snapshot.
    pub(crate) fn admit_table_task(
        &self,
        execution: &QueryExecutionScope,
        charge: QueryTaskCharge,
        table_index: usize,
    ) -> Result<QueryTaskPermit<PinnedTableManifest>> {
        execution.admit_snapshot_task::<NativeQueryReadSnapshot, PinnedTableManifest>(
            charge,
            |snapshot| {
                self.validate_origin(snapshot)?;
                snapshot.pinned.table(table_index).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::InvalidArgument,
                        "native query snapshot table index is out of bounds",
                    )
                    .with_query_id(execution.query_id())
                    .with_retryable(false)
                })
            },
        )
    }

    async fn pin_persisted_once(
        &self,
        query_id: QueryId,
        tables: &[TableRef],
    ) -> Result<NativeQueryReadSnapshot> {
        let resolved = self.inner.catalog.resolve_tables(tables).await?;
        let resolved = canonicalize_tables(resolved)?;
        let mut pending = Vec::with_capacity(resolved.len());
        for table in resolved.values() {
            if crate::native_storage::is_legacy_catalog_shadow_table(table)? {
                pending.push(PendingTableBoundary::PersistedOnly {
                    table_id: table.table_id,
                    schema_version: table.current_schema_version,
                    persisted_snapshot_version: table.current_snapshot_version,
                    writer_epoch: table.writer_epoch,
                });
            } else {
                pending.push(PendingTableBoundary::Actor(
                    self.inner
                        .table_actors
                        .table(table.table_id)?
                        .try_snapshot()?,
                ));
            }
        }

        let mut boundaries = Vec::with_capacity(pending.len());
        for pending in pending {
            boundaries.push(match pending {
                PendingTableBoundary::Actor(request) => {
                    TableBoundary::from_actor(request.wait().await?)
                }
                PendingTableBoundary::PersistedOnly {
                    table_id,
                    schema_version,
                    persisted_snapshot_version,
                    writer_epoch,
                } => TableBoundary {
                    table_id,
                    schema_version,
                    persisted_snapshot_version,
                    writer_epoch,
                    actor_committed_log_offset: None,
                },
            });
        }
        validate_table_boundaries(query_id, &resolved, &boundaries)?;

        let pin_requests = boundaries
            .iter()
            .map(|boundary| CatalogSnapshotSelector {
                table_id: boundary.table_id,
                snapshot_version: boundary.persisted_snapshot_version,
                schema_version: boundary.schema_version,
            })
            .map(|selector| SqliteCatalogQuerySnapshotPinRequest {
                selector,
                lease_id: self.next_lease_id(),
            })
            .collect::<Vec<_>>();
        let pinned = self
            .inner
            .catalog
            .read_pinned_query_snapshot_at(query_id, &pin_requests)
            .await?;
        #[cfg(test)]
        self.wait_for_pin_stall(TestPinStage::AfterCatalogPin).await;
        let tables = self.finalize_descriptors(query_id, &boundaries, &pin_requests, &pinned)?;
        let descriptor = QueryReadSnapshotDescriptor {
            query_id,
            catalog_revision: pinned.catalog_revision(),
            tables,
        };
        Ok(NativeQueryReadSnapshot {
            descriptor,
            pinned,
            coordinator_nonce: self.inner.nonce,
        })
    }

    #[cfg(test)]
    async fn wait_for_pin_stall(&self, stage: TestPinStage) {
        let stall = {
            let mut slot = self
                .inner
                .pin_stall
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if slot.as_ref().is_some_and(|stall| stall.stage == stage) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(stall) = stall {
            stall.started.add_permits(1);
            let permit = stall
                .release
                .acquire()
                .await
                .expect("test pin stall semaphore remains open");
            permit.forget();
        }
    }

    fn finalize_descriptors(
        &self,
        query_id: QueryId,
        boundaries: &[TableBoundary],
        pin_requests: &[SqliteCatalogQuerySnapshotPinRequest],
        pinned: &SqliteCatalogPinnedQuerySnapshot,
    ) -> Result<Vec<TableReadSnapshotDescriptor>> {
        if boundaries.len() != pinned.table_count() || boundaries.len() != pin_requests.len() {
            return Err(corruption(
                query_id,
                "pinned Catalog table count does not match actor snapshot count",
            ));
        }
        let mut descriptors = Vec::with_capacity(boundaries.len());
        for ((boundary, pin_request), manifest) in
            boundaries.iter().zip(pin_requests).zip(pinned.tables())
        {
            let selector = manifest.selector();
            if selector.table_id != boundary.table_id
                || selector.snapshot_version != boundary.persisted_snapshot_version
                || selector.schema_version != boundary.schema_version
                || &pin_request.selector != selector
            {
                return Err(corruption(
                    query_id,
                    "pinned Catalog selector does not match its actor observation",
                )
                .with_table_id(boundary.table_id));
            }
            if manifest.table_descriptor().writer_epoch != boundary.writer_epoch
                || boundary.actor_committed_log_offset.is_some_and(|offset| {
                    manifest.snapshot_metadata().committed_log_offset != offset
                })
            {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "table actor and Catalog snapshot boundaries changed during query pinning",
                )
                .with_query_id(query_id)
                .with_table_id(boundary.table_id)
                .with_retryable(true));
            }
            let max_log_offset = boundary
                .actor_committed_log_offset
                .unwrap_or(manifest.snapshot_metadata().committed_log_offset);
            descriptors.push(TableReadSnapshotDescriptor {
                table_id: boundary.table_id,
                schema_version: boundary.schema_version,
                persisted_snapshot_version: boundary.persisted_snapshot_version,
                writer_epoch: boundary.writer_epoch,
                active_segment: None,
                retained_sealed_segments: Vec::new(),
                max_log_offset,
                lease_id: pin_request.lease_id,
            });
        }
        Ok(descriptors)
    }

    fn next_lease_id(&self) -> LeaseId {
        let sequence = self
            .inner
            .next_lease_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("native query snapshot lease sequence exhausted u64");
        let value = (u128::from(self.inner.nonce) << 64) | u128::from(sequence);
        LeaseId::from_u128(value).expect("coordinator nonce and lease sequence are nonzero")
    }
}

struct SnapshotPlanningAdmission {
    inner: Arc<NativeQuerySnapshotCoordinatorInner>,
}

impl Drop for SnapshotPlanningAdmission {
    fn drop(&mut self) {
        let previous = self.inner.active_plans.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

enum PendingTableBoundary {
    Actor(zippy_ingest::TableActorSnapshotRequest),
    PersistedOnly {
        table_id: TableId,
        schema_version: u64,
        persisted_snapshot_version: u64,
        writer_epoch: u64,
    },
}

#[derive(Clone, Copy)]
struct TableBoundary {
    table_id: TableId,
    schema_version: u64,
    persisted_snapshot_version: u64,
    writer_epoch: u64,
    actor_committed_log_offset: Option<OffsetWatermark>,
}

impl TableBoundary {
    fn from_actor(snapshot: TableActorSnapshot) -> Self {
        Self {
            table_id: snapshot.table_id,
            schema_version: snapshot.schema_version,
            persisted_snapshot_version: snapshot.persisted_snapshot_version,
            writer_epoch: snapshot.writer_epoch,
            actor_committed_log_offset: Some(snapshot.committed_log_offset),
        }
    }
}

/// One non-cloneable query lease with controlled, cloneable physical task capabilities.
pub(crate) struct NativeQueryReadSnapshot {
    descriptor: QueryReadSnapshotDescriptor,
    pinned: SqliteCatalogPinnedQuerySnapshot,
    coordinator_nonce: u64,
}

impl NativeQueryReadSnapshot {
    /// Builds public plan metadata without exposing physical file locations.
    pub(crate) fn plan_info(&self, planned_at_ns: u64, expires_at_ns: u64) -> QueryPlanInfo {
        QueryPlanInfo {
            query_id: self.descriptor.query_id,
            catalog_revision: self.descriptor.catalog_revision,
            snapshots: self
                .descriptor
                .tables
                .iter()
                .map(|table| QueryTableVersion {
                    table_id: table.table_id,
                    snapshot_version: table.persisted_snapshot_version,
                    schema_version: table.schema_version,
                })
                .collect(),
            planned_at_ns,
            expires_at_ns,
        }
    }

    pub(crate) fn into_opaque(self) -> OpaqueQueryReadSnapshot {
        Box::new(self)
    }
}

impl QueryReadSnapshotGuard for NativeQueryReadSnapshot {
    fn descriptor(&self) -> &QueryReadSnapshotDescriptor {
        &self.descriptor
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn renew(&mut self, _expires_at_ns: u64) -> BoxFuture<'_, Result<()>> {
        // Persisted Catalog pins are process-local RAII guards and do not expire while owned.
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

fn canonicalize_tables(tables: Vec<TableDescriptor>) -> Result<BTreeMap<TableId, TableDescriptor>> {
    let mut canonical = BTreeMap::new();
    for table in tables {
        match canonical.entry(table.table_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(table);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &table => {}
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "Catalog resolved one table identity to inconsistent descriptors",
                )
                .with_table_id(*entry.key())
                .with_retryable(false));
            }
        }
    }
    Ok(canonical)
}

fn validate_table_boundaries(
    query_id: QueryId,
    tables: &BTreeMap<TableId, TableDescriptor>,
    boundaries: &[TableBoundary],
) -> Result<()> {
    if tables.len() != boundaries.len() {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "native table actor snapshot count does not match resolved table count",
        )
        .with_retryable(false));
    }
    for ((table_id, table), boundary) in tables.iter().zip(boundaries) {
        if *table_id != boundary.table_id {
            return Err(corruption(
                query_id,
                "native table actor snapshots are not in stable table-ID order",
            )
            .with_table_id(*table_id));
        }
        if boundary.schema_version == 0 || boundary.persisted_snapshot_version == 0 {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "native table actor returned a zero query snapshot version",
            )
            .with_table_id(*table_id)
            .with_retryable(false));
        }
        if table.writer_epoch != boundary.writer_epoch
            || table.current_schema_version < boundary.schema_version
            || table.current_snapshot_version < boundary.persisted_snapshot_version
        {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                "resolved Catalog descriptor changed before table actor snapshot",
            )
            .with_table_id(*table_id)
            .with_retryable(true));
        }
    }
    Ok(())
}

fn next_coordinator_nonce() -> u64 {
    NEXT_SNAPSHOT_COORDINATOR_NONCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .expect("native query snapshot coordinator nonce exhausted u64")
}

fn corruption(query_id: QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_query_id(query_id)
        .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::OwnedDataDirectory;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1,
        CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, SchemaIpcPayload, TableDefinition, TableRef,
    };
    use zippy_types::{NamespaceId, QueryId, RequestId, SessionId, TableId};

    use super::{NativeQuerySnapshotCoordinator, TestPinStage};
    use crate::legacy_catalog_migration::LEGACY_STREAM_NAME_CONFIG_KEY;
    use crate::native_storage::{
        NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    };
    use crate::query_service::{
        QueryResourceBudget, QueryServiceRegistryConfig, QuerySubmissionMetadata, QueryTaskCharge,
    };

    const NAMESPACE: u128 = 1;
    const NATIVE_TABLE: u128 = 10;
    const LEGACY_TABLE: u128 = 20;

    fn namespace_id() -> NamespaceId {
        NamespaceId::from_u128(NAMESPACE).expect("nonzero namespace id")
    }

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).expect("nonzero table id")
    }

    fn query_id(value: u128) -> QueryId {
        QueryId::from_u128(value).expect("nonzero query id")
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("nonzero request id")
    }

    fn session_id() -> SessionId {
        SessionId::from_u128(1).expect("nonzero session id")
    }

    #[tokio::test]
    async fn closing_snapshot_admission_waits_for_an_inflight_planning_guard() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let coordinator = runtime.query_snapshot_coordinator();
        let raw_catalog = runtime.catalog_read_handle();
        let admission = coordinator
            .admit_planning(query_id(99))
            .expect("admit one snapshot plan");
        runtime.begin_shutdown();
        assert!(!coordinator.planning_drained());
        assert!(coordinator.admit_planning(query_id(100)).is_err());
        let raw_pin_error = raw_catalog
            .read_pinned_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(NATIVE_TABLE),
                snapshot_version: 1,
                schema_version: 1,
            }])
            .await
            .expect_err("raw Catalog pin admission must close before the drain decision");
        assert_eq!(raw_pin_error.code(), zippy_types::ErrorCode::Unavailable);
        assert_eq!(
            runtime.shutdown(Duration::from_millis(10)),
            NativeStorageShutdownOutcome::TimedOut
        );
        drop(admission);
        assert!(coordinator.planning_drained());
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("price", DataType::Int64, false)]);
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, &schema).expect("create IPC writer");
        writer.finish().expect("finish IPC stream");
        drop(writer);
        SchemaIpcPayload::new(bytes).expect("nonempty schema IPC")
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "query-snapshot-test".to_owned(),
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

    async fn seed_mixed_tables(catalog: &SqliteCatalog) {
        catalog
            .command(command(
                1,
                0,
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: namespace_id(),
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
                    table_id: table_id(NATIVE_TABLE),
                    namespace_id: namespace_id(),
                    name: CatalogName::new("ticks").expect("valid native table name"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create native table");
        let mut legacy_config = BTreeMap::new();
        legacy_config.insert(
            LEGACY_STREAM_NAME_CONFIG_KEY.to_owned(),
            "legacy_ticks".to_owned(),
        );
        catalog
            .command(command(
                3,
                2,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(LEGACY_TABLE),
                    namespace_id: namespace_id(),
                    name: CatalogName::new("legacy_ticks").expect("valid legacy table name"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: schema_ipc(),
                    config: legacy_config,
                }),
            ))
            .await
            .expect("create legacy persisted-only table");
    }

    async fn start_runtime(
        max_active_pins: usize,
    ) -> (
        tempfile::TempDir,
        Arc<OwnedDataDirectory>,
        NativeStorageRuntime,
    ) {
        let temp = tempfile::tempdir().expect("create temp directory");
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(temp.path().join("database"))
                .expect("own test data directory"),
        );
        let catalog_config = SqliteCatalogConfig::new(&ownership)
            .with_runtime_snapshot_pin_limits(max_active_pins, 8);
        let catalog = SqliteCatalog::open(catalog_config.clone()).expect("open seed Catalog");
        seed_mixed_tables(&catalog).await;
        catalog.close().expect("close seed Catalog");
        let runtime_config = NativeStorageRuntimeConfig::new(&ownership)
            .with_catalog_config(catalog_config)
            .expect("reuse Catalog ownership")
            .with_query_registry_config(QueryServiceRegistryConfig {
                sweep_interval: Duration::from_secs(3600),
                ..QueryServiceRegistryConfig::default()
            });
        let runtime = NativeStorageRuntime::start(runtime_config).expect("start native storage");
        (temp, ownership, runtime)
    }

    fn mixed_refs() -> Vec<TableRef> {
        vec![
            TableRef::Id {
                table_id: table_id(LEGACY_TABLE),
            },
            TableRef::Id {
                table_id: table_id(NATIVE_TABLE),
            },
            TableRef::Name {
                namespace: CatalogName::new("market").expect("valid namespace name"),
                table: CatalogName::new("ticks").expect("valid table name"),
            },
        ]
    }

    fn budget() -> QueryResourceBudget {
        QueryResourceBudget {
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes: 1024,
        }
    }

    fn charge() -> QueryTaskCharge {
        QueryTaskCharge {
            cpu_slots: NonZeroU32::new(1).expect("nonzero CPU charge"),
            memory_bytes: NonZeroU64::new(64).expect("nonzero memory charge"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mixed_actor_and_persisted_only_snapshot_is_sorted_audited_and_lifecycle_bound() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        assert_eq!(runtime.table_runtime_count(), 1);
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let query = query_id(101);
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
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
            .expect("register planning before snapshot pin");
        let snapshot = snapshot_coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .expect("pin mixed query snapshot");
        let info = snapshot.plan_info(10, 100);
        assert_eq!(info.snapshots.len(), 2);
        assert_eq!(info.snapshots[0].table_id, table_id(NATIVE_TABLE));
        assert_eq!(info.snapshots[1].table_id, table_id(LEGACY_TABLE));

        let status = runtime
            .runtime_snapshot_pin_status()
            .expect("read pin status");
        assert_eq!(status.active_pins, 2);
        assert_eq!(status.active_query_pins, 2);
        let records = runtime
            .runtime_query_snapshot_pins()
            .expect("read query pin records");
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.query_id == query));
        let descriptor_lease_ids = snapshot
            .descriptor
            .tables
            .iter()
            .map(|table| table.lease_id)
            .collect::<Vec<_>>();
        assert!(records
            .iter()
            .all(|record| descriptor_lease_ids.contains(&record.lease_id)));

        let token = resources
            .complete_planning_at_for_test(planning, info, snapshot.into_opaque(), 10)
            .expect("complete pinned plan");
        let execution = resources
            .begin_execution(token, 11)
            .expect("begin pinned execution");

        let foreign = NativeQuerySnapshotCoordinator::new(
            snapshot_coordinator.inner.catalog.clone(),
            snapshot_coordinator.inner.table_actors.clone(),
        );
        let foreign_error = match foreign.admit_table_task(&execution, charge(), 0) {
            Ok(_) => panic!("foreign coordinator nonce must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            foreign_error.code(),
            zippy_types::ErrorCode::InvalidArgument
        );
        assert_eq!(resources.status().active_tasks, 0);

        let task = snapshot_coordinator
            .admit_table_task(&execution, charge(), 0)
            .expect("admit lease-bound table task");
        assert_eq!(task.resource().selector().table_id, table_id(NATIVE_TABLE));
        resources
            .cancel_current_for_test(query)
            .expect("cancel running query");
        drop(execution);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("pins retained by task")
                .active_query_pins,
            2
        );
        drop(task);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("pins released after task")
                .active_query_pins,
            0
        );
        assert!(runtime
            .runtime_query_snapshot_pins()
            .expect("query pins released")
            .is_empty());
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_table_pin_budget_failure_rolls_back_every_pin_and_audit_record() {
        let (_temp, _ownership, mut runtime) = start_runtime(1).await;
        let coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query_id(102),
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("register planning before snapshot pin");
        let error = coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .err()
            .expect("second table must exhaust pin budget");
        assert_eq!(error.code(), zippy_types::ErrorCode::ResourceExhausted);
        resources
            .fail_planning(planning, error)
            .expect("snapshot planning failure is terminal");
        let status = runtime
            .runtime_snapshot_pin_status()
            .expect("read pin status");
        assert_eq!(status.active_pins, 0);
        assert_eq!(status.active_query_pins, 0);
        assert!(runtime
            .runtime_query_snapshot_pins()
            .expect("read query pin records")
            .is_empty());
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timed_out_planning_token_cannot_start_snapshot_pinning() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query_id(105),
                    deadline_ns: Some(20),
                },
                budget(),
                10,
            )
            .expect("register planning before deadline");
        assert_eq!(resources.sweep_expired(20), 1);

        let error = snapshot_coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .err()
            .expect("timed-out planning cannot pin snapshots");
        assert_eq!(error.code(), zippy_types::ErrorCode::DeadlineExceeded);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("read pin status")
                .active_query_pins,
            0
        );
        drop(planning);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_actively_interrupts_a_stalled_snapshot_pin_and_returns_bounded() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let stall = snapshot_coordinator.install_pin_stall(TestPinStage::AfterCatalogPin);
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query_id(106),
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("register stalled planning query");
        let pin_coordinator = snapshot_coordinator.clone();
        let pin_task = tokio::spawn(async move {
            let result = pin_coordinator
                .pin_persisted(&planning, &mixed_refs())
                .await
                .map(|_| ());
            drop(planning);
            result
        });
        let started = tokio::time::timeout(Duration::from_secs(1), stall.started.acquire())
            .await
            .expect("stalled pin must enter its awaited stage")
            .expect("test stall semaphore remains open");
        started.forget();
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("stalled pin status")
                .active_query_pins,
            2
        );

        let shutdown = tokio::task::spawn_blocking(move || {
            let outcome = runtime.shutdown(Duration::from_secs(1));
            let remaining_query_pins = runtime.runtime_query_snapshot_pins();
            (runtime, outcome, remaining_query_pins)
        });
        let pin_error = tokio::time::timeout(Duration::from_secs(1), pin_task)
            .await
            .expect("shutdown cancellation must wake stalled pin")
            .expect("stalled pin task must join")
            .expect_err("shutdown must cancel stalled pin");
        assert_eq!(pin_error.code(), zippy_types::ErrorCode::Cancelled);
        let (_runtime, outcome, remaining_query_pins) =
            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .expect("native shutdown must remain bounded")
                .expect("native shutdown worker must join");
        assert!(outcome.is_complete());
        match remaining_query_pins {
            Ok(records) => assert!(records.is_empty()),
            Err(error) => assert_eq!(error.code(), zippy_types::ErrorCode::Unavailable),
        }
        assert_eq!(resources.status().active_snapshot_owners, 0);
        assert!(snapshot_coordinator.planning_drained());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_plan_token_cannot_keep_catalog_pins_after_ttl() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let query = query_id(103);
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
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
            .expect("register planning before snapshot pin");
        let snapshot = snapshot_coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .expect("pin query snapshot");
        let token = resources
            .complete_planning_at_for_test(
                planning,
                snapshot.plan_info(10, 20),
                snapshot.into_opaque(),
                10,
            )
            .expect("complete plan token");
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("pins before TTL")
                .active_query_pins,
            2
        );
        assert_eq!(resources.sweep_expired(20), 1);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("pins after TTL")
                .active_query_pins,
            0
        );
        drop(token);
        assert_eq!(resources.sweep_expired(21), 0);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_storage_shutdown_waits_until_the_final_query_pin_is_released() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query_id(104),
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("register planning before snapshot pin");
        let snapshot = runtime
            .query_snapshot_coordinator()
            .pin_persisted(&planning, &mixed_refs())
            .await
            .expect("pin query snapshot");
        assert!(matches!(
            runtime.shutdown(Duration::from_millis(20)),
            crate::native_storage::NativeStorageShutdownOutcome::TimedOut
        ));
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("Catalog remains open while pin is active")
                .active_query_pins,
            2
        );
        drop(snapshot);
        drop(planning);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }
}
