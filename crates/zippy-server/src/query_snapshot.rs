//! Lifecycle-bound native query snapshots over table actors and the SQLite Catalog.

use std::any::Any;
use std::collections::BTreeMap;
use std::mem::size_of;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

#[cfg(test)]
use tokio::sync::Semaphore;
use zippy_catalog::{
    conservative_table_resolution_owned_peak_bytes, decode_persisted_table_schema, PinnedDataFile,
    PinnedTableManifest, SqliteCatalogPinnedQuerySnapshot, SqliteCatalogQuerySnapshotPinRequest,
    SqliteCatalogReadHandle, SqliteCatalogSnapshotReadLimits,
    PERSISTED_SCHEMA_DECODE_TRANSIENT_BYTES,
};
use zippy_ingest::{TableActorRegistryHandle, TableActorSnapshot};
use zippy_query::{PersistedFileDescription, PersistedFileKey, PersistedTableDescription};
use zippy_types::catalog::CatalogSnapshotSelector;
use zippy_types::query::QueryTableVersion;
use zippy_types::storage::{
    CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseDescriptor, OpaqueQueryReadSnapshot,
    QueryReadSnapshotDescriptor, QueryReadSnapshotGuard, TableReadSnapshotDescriptor,
};
use zippy_types::table::{DataFileState, TableDescriptor, TableRef};
use zippy_types::{
    BoxFuture, ErrorCode, LeaseId, OffsetWatermark, QueryId, Result, ServerError, TableId,
};

use crate::query_service::{
    QueryCancellation, QueryExecutionScope, QueryPlanDraft, QueryPlanningToken,
    QueryRetainedCharge, QueryRetainedKind, QueryRetainedPermit, QueryTaskCharge, QueryTaskPermit,
};

const MAX_SNAPSHOT_COORDINATION_ATTEMPTS: usize = 3;
const QUERY_SNAPSHOT_COORDINATION_FIXED_BYTES: u64 = 1024 * 1024;
const QUERY_SNAPSHOT_COORDINATION_BYTES_PER_REFERENCE: u64 = 4 * 1024;
const QUERY_SNAPSHOT_READ_LIMITS: SqliteCatalogSnapshotReadLimits =
    SqliteCatalogSnapshotReadLimits {
        max_visible_files: 4_096,
        max_source_lineage_rows: 16_384,
        max_raw_response_bytes: 2 * 1024 * 1024,
    };
const PROJECTED_SCHEMA_DECODE_TRANSIENT_BYTES: u64 = PERSISTED_SCHEMA_DECODE_TRANSIENT_BYTES as u64;
const PROJECTED_MANIFEST_FIXED_RETAINED_BYTES: u64 = 64 * 1024;
const PROJECTED_MANIFEST_BYTES_PER_TABLE: u64 = 4 * 1024;
const PROJECTED_MANIFEST_BYTES_PER_FILE: u64 = 256;
const PROJECTED_MANIFEST_FIXED_TASK_MARGIN_BYTES: u64 = 1024 * 1024;
const PROJECTED_MANIFEST_TASK_BYTES_PER_TABLE: u64 = 512;
const PROJECTED_MANIFEST_TASK_BYTES_PER_FILE: u64 = 512;
static NEXT_SNAPSHOT_COORDINATOR_NONCE: AtomicU64 = AtomicU64::new(1);

fn conservative_query_snapshot_planning_peak_bytes(reference_count: usize) -> Result<NonZeroU64> {
    let resolution_peak = conservative_table_resolution_owned_peak_bytes(reference_count)?.get();
    let snapshot_peak = QUERY_SNAPSHOT_READ_LIMITS
        .conservative_owned_peak_bytes(reference_count)?
        .get();
    let reference_count = u64::try_from(reference_count).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "query snapshot reference count exceeds u64",
        )
        .with_retryable(false)
    })?;
    let coordination_peak = reference_count
        .checked_mul(QUERY_SNAPSHOT_COORDINATION_BYTES_PER_REFERENCE)
        .and_then(|bytes| bytes.checked_add(QUERY_SNAPSHOT_COORDINATION_FIXED_BYTES))
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "query snapshot coordination memory estimate overflow",
            )
            .with_retryable(false)
        })?;
    let total = resolution_peak
        .checked_add(snapshot_peak)
        .and_then(|bytes| bytes.checked_add(coordination_peak))
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "query snapshot planning memory estimate overflow",
            )
            .with_retryable(false)
        })?;
    NonZeroU64::new(total).ok_or_else(|| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "query snapshot planning memory estimate must be nonzero",
        )
        .with_retryable(false)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProjectedSnapshotCharges {
    task_peak_bytes: NonZeroU64,
    retained_bytes: NonZeroU64,
}

/// Capability-free persisted tables projected from one exact native snapshot.
#[derive(Debug)]
pub(crate) struct ProjectedSnapshotManifest {
    query_id: QueryId,
    catalog_revision: u64,
    tables: Vec<PersistedTableDescription>,
    retained_memory_bytes: NonZeroU64,
}

impl ProjectedSnapshotManifest {
    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    pub(crate) fn tables(&self) -> &[PersistedTableDescription] {
        &self.tables
    }

    pub(crate) fn table(&self, table_index: usize) -> Option<&PersistedTableDescription> {
        self.tables.get(table_index)
    }

    pub(crate) const fn retained_memory_bytes(&self) -> NonZeroU64 {
        self.retained_memory_bytes
    }
}

struct PinnedProjectionInput {
    query_id: QueryId,
    catalog_revision: u64,
    tables: Box<[QueryTableVersion]>,
    pinned: SqliteCatalogPinnedQuerySnapshot,
    charges: ProjectedSnapshotCharges,
}

fn projected_snapshot_charges(
    pinned: &SqliteCatalogPinnedQuerySnapshot,
) -> Result<ProjectedSnapshotCharges> {
    let profile = pinned.projection_profile();
    let table_count = profile.table_count();
    let file_count = profile.file_count();
    let retained_schema_bytes = profile.retained_schema_bytes();

    let retained = checked_projection_add(
        PROJECTED_MANIFEST_FIXED_RETAINED_BYTES,
        u64::try_from(size_of::<ProjectedSnapshotManifest>())
            .map_err(|_| projection_exhausted("projected manifest layout exceeds u64"))?,
        "manifest root",
    )?;
    let retained = checked_projection_add(retained, retained_schema_bytes, "decoded schemas")?;
    let retained = checked_projection_add(
        retained,
        checked_projection_mul(
            table_count,
            PROJECTED_MANIFEST_BYTES_PER_TABLE,
            "table descriptions",
        )?,
        "table descriptions",
    )?;
    let retained = checked_projection_add(
        retained,
        checked_projection_mul(
            file_count,
            PROJECTED_MANIFEST_BYTES_PER_FILE,
            "file descriptions",
        )?,
        "file descriptions",
    )?;
    let retained_bytes = NonZeroU64::new(retained)
        .ok_or_else(|| projection_exhausted("projected retained charge must be nonzero"))?;

    let task_peak = checked_projection_add(
        retained,
        PROJECTED_SCHEMA_DECODE_TRANSIENT_BYTES,
        "schema decode transient",
    )?;
    let task_peak = checked_projection_add(
        task_peak,
        PROJECTED_MANIFEST_FIXED_TASK_MARGIN_BYTES,
        "task allocator margin",
    )?;
    let task_peak = checked_projection_add(
        task_peak,
        checked_projection_mul(
            table_count,
            PROJECTED_MANIFEST_TASK_BYTES_PER_TABLE,
            "transient table metadata",
        )?,
        "transient table metadata",
    )?;
    let task_peak = checked_projection_add(
        task_peak,
        checked_projection_mul(
            file_count,
            PROJECTED_MANIFEST_TASK_BYTES_PER_FILE,
            "transient file validation",
        )?,
        "transient file validation",
    )?;
    let task_peak_bytes = NonZeroU64::new(task_peak)
        .ok_or_else(|| projection_exhausted("projected task peak must be nonzero"))?;
    Ok(ProjectedSnapshotCharges {
        task_peak_bytes,
        retained_bytes,
    })
}

fn checked_projection_add(left: u64, right: u64, component: &str) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| {
        projection_exhausted(format!(
            "projected snapshot byte addition overflow component=[{component}]"
        ))
    })
}

fn checked_projection_mul(left: u64, right: u64, component: &str) -> Result<u64> {
    left.checked_mul(right).ok_or_else(|| {
        projection_exhausted(format!(
            "projected snapshot byte multiplication overflow component=[{component}]"
        ))
    })
}

fn projection_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

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
        let snapshot_memory_bytes = conservative_query_snapshot_planning_peak_bytes(tables.len())
            .map_err(|error| error.with_query_id(query_id))?;
        let mut last_retryable_conflict = None;
        for attempt in 0..MAX_SNAPSHOT_COORDINATION_ATTEMPTS {
            self.ensure_planning_active(planning)?;
            self.ensure_accepting(query_id)?;
            let snapshot_memory_lease = planning.reserve_snapshot_memory(snapshot_memory_bytes)?;
            // Every awaited planning stage is owned by this one future. Dropping it abandons only
            // oneshot responses; the Catalog worker retains both memory admission and runtime pins
            // until its synchronous work exits.
            let outcome = tokio::select! {
                biased;
                terminal = cancellation.wait_terminal() => {
                    return Err(terminal?.error.unwrap_or_else(|| {
                        ServerError::new(ErrorCode::Cancelled, "query planning was cancelled")
                            .with_query_id(query_id)
                            .with_retryable(false)
                    }));
                }
                outcome = self.pin_persisted_once(
                    query_id,
                    tables,
                    QUERY_SNAPSHOT_READ_LIMITS,
                    snapshot_memory_lease,
                ) => outcome,
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

    /// Admits one physical file task before deriving its capability from the pinned snapshot.
    pub(crate) fn admit_file_task(
        &self,
        execution: &QueryExecutionScope,
        charge: QueryTaskCharge,
        table_index: usize,
        file_index: usize,
    ) -> Result<QueryTaskPermit<PinnedDataFile>> {
        execution.admit_snapshot_task::<NativeQueryReadSnapshot, PinnedDataFile>(
            charge,
            |snapshot| {
                self.validate_origin(snapshot)?;
                let table = snapshot.pinned.table(table_index).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::InvalidArgument,
                        "native query snapshot table index is out of bounds",
                    )
                    .with_query_id(execution.query_id())
                    .with_retryable(false)
                })?;
                table.file(file_index).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::InvalidArgument,
                        "native query snapshot file index is out of bounds",
                    )
                    .with_query_id(execution.query_id())
                    .with_table_id(table.selector().table_id)
                    .with_retryable(false)
                })
            },
        )
    }

    /// Admits one exactly described physical file from the live pinned query snapshot.
    ///
    /// The description is path-free and untrusted at this boundary. Every stable identity,
    /// ordinal, version, and scalar file property is revalidated after task admission but before
    /// the pinned capability can escape the snapshot guard.
    pub(crate) fn admit_described_file_task(
        &self,
        execution: &QueryExecutionScope,
        charge: QueryTaskCharge,
        description: PersistedFileDescription,
    ) -> Result<QueryTaskPermit<PinnedDataFile>> {
        let query_id = execution.query_id();
        execution.admit_snapshot_task::<NativeQueryReadSnapshot, PinnedDataFile>(
            charge,
            |snapshot| {
                self.validate_origin(snapshot)?;
                if snapshot.descriptor.query_id != query_id {
                    return Err(corruption(
                        query_id,
                        "persisted file request belongs to another native query snapshot",
                    ));
                }
                let table = snapshot
                    .pinned
                    .table(description.table_index())
                    .ok_or_else(|| {
                        corruption(
                            query_id,
                            "persisted file request table ordinal is out of bounds",
                        )
                    })?;
                let selector = table.selector();
                if table.table_index() != description.table_index()
                    || selector.table_id != description.table_id()
                    || selector.snapshot_version != description.snapshot_version().get()
                {
                    return Err(corruption(
                        query_id,
                        "persisted file request does not match its pinned table snapshot",
                    )
                    .with_table_id(selector.table_id));
                }
                let file = table.file(description.file_index()).ok_or_else(|| {
                    corruption(
                        query_id,
                        "persisted file request file ordinal is out of bounds",
                    )
                    .with_table_id(selector.table_id)
                })?;
                if file.table_id() != description.table_id()
                    || file.file_id() != description.file_id()
                    || file.schema_version() != description.schema_version().get()
                    || file.file_size_bytes() != description.file_size_bytes().get()
                    || file.row_count() != description.row_count()
                {
                    return Err(corruption(
                        query_id,
                        "persisted file request does not match its pinned physical file",
                    )
                    .with_table_id(selector.table_id));
                }
                Ok(file)
            },
        )
    }

    /// Projects the exact pinned snapshot into capability-free persisted query metadata.
    ///
    /// Admission covers the complete retained manifest plus one bounded schema-decode transient.
    /// The first callback computes only scalar charges; cloning the pinned owner and allocating
    /// audit metadata happens only after QueryService reserves task, CPU, and memory capacity.
    pub(crate) fn project_persisted_snapshot(
        &self,
        execution: &QueryExecutionScope,
    ) -> Result<QueryRetainedPermit<ProjectedSnapshotManifest>> {
        let query_id = execution.query_id();
        let task = execution
            .admit_snapshot_task_with_charge::<NativeQueryReadSnapshot, PinnedProjectionInput>(
                |snapshot| {
                    self.validate_origin(snapshot)?;
                    let charges = projected_snapshot_charges(&snapshot.pinned)
                        .map_err(|error| error.with_query_id(query_id))?;
                    Ok(QueryTaskCharge {
                        cpu_slots: NonZeroU32::new(1).expect("projection CPU charge is nonzero"),
                        memory_bytes: charges.task_peak_bytes,
                    })
                },
                |snapshot, admitted_charge| {
                    self.validate_origin(snapshot)?;
                    if snapshot.descriptor.query_id != query_id {
                        return Err(corruption(
                            query_id,
                            "native query snapshot identity changed during persisted projection",
                        ));
                    }
                    let charges = projected_snapshot_charges(&snapshot.pinned)
                        .map_err(|error| error.with_query_id(query_id))?;
                    if admitted_charge
                        != (QueryTaskCharge {
                            cpu_slots: NonZeroU32::new(1)
                                .expect("projection CPU charge is nonzero"),
                            memory_bytes: charges.task_peak_bytes,
                        })
                    {
                        return Err(corruption(
                            query_id,
                            "persisted projection charge changed after task admission",
                        ));
                    }
                    let tables = snapshot
                        .descriptor
                        .tables
                        .iter()
                        .map(|table| QueryTableVersion {
                            table_id: table.table_id,
                            snapshot_version: table.persisted_snapshot_version,
                            schema_version: table.schema_version,
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice();
                    Ok(PinnedProjectionInput {
                        query_id,
                        catalog_revision: snapshot.descriptor.catalog_revision,
                        tables,
                        pinned: snapshot.pinned.clone(),
                        charges,
                    })
                },
            )?;

        let cancellation = task.cancellation();
        task.into_retained(|input| {
            let manifest = project_pinned_snapshot(input, &cancellation)?;
            let charge = QueryRetainedCharge {
                kind: QueryRetainedKind::ProjectedSnapshotManifest,
                memory_bytes: manifest.retained_memory_bytes(),
            };
            Ok((manifest, charge))
        })
    }

    async fn pin_persisted_once(
        &self,
        query_id: QueryId,
        tables: &[TableRef],
        limits: SqliteCatalogSnapshotReadLimits,
        mut snapshot_memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<NativeQueryReadSnapshot> {
        let resolved_with_lease = self
            .inner
            .catalog
            .resolve_tables_with_memory_lease(query_id, tables, snapshot_memory_lease)
            .await?;
        let resolved = resolved_with_lease.0;
        snapshot_memory_lease = resolved_with_lease.1;
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
            .read_pinned_query_snapshot_at(query_id, &pin_requests, limits, snapshot_memory_lease)
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
    /// Builds capability-free plan metadata without exposing physical files or server time.
    pub(crate) fn plan_draft(&self) -> QueryPlanDraft {
        QueryPlanDraft::new(
            self.descriptor.query_id,
            self.descriptor.catalog_revision,
            self.descriptor
                .tables
                .iter()
                .map(|table| QueryTableVersion {
                    table_id: table.table_id,
                    snapshot_version: table.persisted_snapshot_version,
                    schema_version: table.schema_version,
                })
                .collect(),
        )
    }

    pub(crate) fn into_opaque(self) -> OpaqueQueryReadSnapshot {
        Box::new(self)
    }

    pub(crate) fn memory_lease_descriptor(&self) -> CatalogSnapshotMemoryLeaseDescriptor {
        self.pinned.memory_lease_descriptor()
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

fn project_pinned_snapshot(
    input: PinnedProjectionInput,
    cancellation: &QueryCancellation,
) -> Result<ProjectedSnapshotManifest> {
    let PinnedProjectionInput {
        query_id,
        catalog_revision,
        tables: audit_tables,
        pinned,
        charges,
    } = input;
    ensure_projection_active(cancellation, query_id)?;
    if pinned.catalog_revision() != catalog_revision {
        return Err(corruption(
            query_id,
            "pinned Catalog revision differs from the query snapshot audit",
        ));
    }
    if pinned.table_count() != audit_tables.len() {
        return Err(corruption(
            query_id,
            "pinned Catalog table count differs from the query snapshot audit",
        ));
    }
    let actual_charges =
        projected_snapshot_charges(&pinned).map_err(|error| error.with_query_id(query_id))?;
    if actual_charges != charges {
        return Err(corruption(
            query_id,
            "pinned Catalog projection charge changed after task admission",
        ));
    }

    let mut projected_tables = Vec::with_capacity(audit_tables.len());
    let mut previous_table_id = None;
    for (table_index, (audit, table)) in audit_tables.iter().zip(pinned.tables()).enumerate() {
        ensure_projection_active(cancellation, query_id)?;
        if table.table_index() != table_index {
            return Err(
                corruption(query_id, "pinned Catalog table ordinal is not contiguous")
                    .with_table_id(audit.table_id),
            );
        }
        if previous_table_id.is_some_and(|previous| previous >= audit.table_id) {
            return Err(corruption(
                query_id,
                "query snapshot table identities are duplicated or out of order",
            )
            .with_table_id(audit.table_id));
        }
        previous_table_id = Some(audit.table_id);

        let selector = table.selector();
        if selector.table_id != audit.table_id
            || selector.snapshot_version != audit.snapshot_version
            || selector.schema_version != audit.schema_version
        {
            return Err(corruption(
                query_id,
                "pinned Catalog selector differs from the query snapshot audit",
            )
            .with_table_id(audit.table_id));
        }
        let table_descriptor = table.table_descriptor();
        let schema_descriptor = table.schema();
        let snapshot_metadata = table.snapshot_metadata();
        if table_descriptor.table_id != audit.table_id
            || table_descriptor.current_snapshot_version < audit.snapshot_version
            || table_descriptor.current_schema_version < audit.schema_version
            || schema_descriptor.table_id != audit.table_id
            || schema_descriptor.schema_version != audit.schema_version
            || snapshot_metadata.table_id != audit.table_id
            || snapshot_metadata.snapshot_version != audit.snapshot_version
        {
            return Err(corruption(
                query_id,
                "pinned table metadata differs from the selected Catalog boundary",
            )
            .with_table_id(audit.table_id));
        }
        let snapshot_version = nonzero_projection_version(
            query_id,
            audit.table_id,
            audit.snapshot_version,
            "snapshot",
        )?;
        let schema_version =
            nonzero_projection_version(query_id, audit.table_id, audit.schema_version, "schema")?;
        let decoded = decode_persisted_table_schema(table_descriptor, schema_descriptor)
            .map_err(|error| error.with_query_id(query_id).with_table_id(audit.table_id))?;
        ensure_projection_active(cancellation, query_id)?;
        let retained_schema_bytes =
            NonZeroU64::new(decoded.retained_charge_bytes()).ok_or_else(|| {
                corruption(
                    query_id,
                    "decoded persisted schema has zero retained-memory charge",
                )
                .with_table_id(audit.table_id)
            })?;
        if retained_schema_bytes != table.retained_schema_charge_bytes() {
            return Err(corruption(
                query_id,
                "decoded persisted schema charge differs from the pinned Catalog audit",
            )
            .with_table_id(audit.table_id));
        }

        let mut files = Vec::with_capacity(table.file_count());
        for (file_index, file) in table.files().enumerate() {
            files.push(project_pinned_file(
                query_id,
                table_index,
                file_index,
                audit.table_id,
                snapshot_version,
                schema_version,
                &file,
            )?);
        }
        let projected = PersistedTableDescription::try_new(
            table_index,
            audit.table_id,
            snapshot_version,
            schema_version,
            decoded.into_schema(),
            retained_schema_bytes,
            files,
        )
        .map_err(|error| {
            corruption(
                query_id,
                format!("persisted table projection is invalid error=[{error}]"),
            )
            .with_table_id(audit.table_id)
        })?;
        projected_tables.push(projected);
    }
    ensure_projection_active(cancellation, query_id)?;

    Ok(ProjectedSnapshotManifest {
        query_id,
        catalog_revision,
        tables: projected_tables,
        retained_memory_bytes: charges.retained_bytes,
    })
}

fn ensure_projection_active(cancellation: &QueryCancellation, query_id: QueryId) -> Result<()> {
    if !cancellation.is_cancelled() {
        return Ok(());
    }
    if let Some(signal) = cancellation.terminal_signal() {
        return Err(signal.error.unwrap_or_else(|| {
            ServerError::new(
                ErrorCode::Cancelled,
                "persisted snapshot projection was cancelled",
            )
            .with_query_id(query_id)
            .with_retryable(false)
        }));
    }
    Err(ServerError::new(
        ErrorCode::Cancelled,
        "persisted snapshot projection was cancelled",
    )
    .with_query_id(query_id)
    .with_retryable(false))
}

fn project_pinned_file(
    query_id: QueryId,
    table_index: usize,
    file_index: usize,
    table_id: TableId,
    snapshot_version: NonZeroU64,
    table_schema_version: NonZeroU64,
    file: &PinnedDataFile,
) -> Result<PersistedFileDescription> {
    if file.table_id() != table_id {
        return Err(
            corruption(query_id, "pinned data file belongs to another table")
                .with_table_id(table_id),
        );
    }
    if file.state() != DataFileState::Committed
        || file.added_snapshot_version() == 0
        || file.added_snapshot_version() > snapshot_version.get()
        || file
            .removed_snapshot_version()
            .is_some_and(|removed| removed <= snapshot_version.get())
    {
        return Err(corruption(
            query_id,
            "pinned data file is not visible at the selected snapshot",
        )
        .with_table_id(table_id));
    }
    let file_schema_version =
        nonzero_projection_version(query_id, table_id, file.schema_version(), "file schema")?;
    if file_schema_version > table_schema_version {
        return Err(corruption(
            query_id,
            "pinned data file schema is newer than the selected table schema",
        )
        .with_table_id(table_id));
    }
    let file_size_bytes = NonZeroU64::new(file.file_size_bytes()).ok_or_else(|| {
        corruption(query_id, "pinned data file has zero physical size").with_table_id(table_id)
    })?;
    PersistedFileDescription::try_new(
        PersistedFileKey::new(table_index, file_index, table_id, file.file_id()),
        snapshot_version,
        file_schema_version,
        file_size_bytes,
        file.row_count(),
    )
    .map_err(|error| {
        corruption(
            query_id,
            format!("persisted file projection is invalid error=[{error}]"),
        )
        .with_table_id(table_id)
    })
}

fn nonzero_projection_version(
    query_id: QueryId,
    table_id: TableId,
    version: u64,
    kind: &str,
) -> Result<NonZeroU64> {
    NonZeroU64::new(version).ok_or_else(|| {
        corruption(
            query_id,
            format!("persisted projection contains zero {kind} version"),
        )
        .with_table_id(table_id)
    })
}

fn corruption(query_id: QueryId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_query_id(query_id)
        .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Read;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::async_reader::AsyncFileReader;
    use parquet::file::reader::ChunkReader;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{OwnedDataDirectory, DEFAULT_QUERY_MAX_MEMORY_BYTES};
    use zippy_query::{AdmittedAsyncParquetReader, PersistedFileDescription, PersistedFileKey};
    use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance,
        SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition, TableRef,
    };
    use zippy_types::{
        FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, QueryId, RequestId,
        SessionId, TableId,
    };

    use super::{NativeQuerySnapshotCoordinator, TestPinStage};
    use crate::legacy_catalog_migration::LEGACY_STREAM_NAME_CONFIG_KEY;
    use crate::native_storage::{
        NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    };
    use crate::query_physical::AdmittedPinnedParquetFile;
    use crate::query_service::{
        QueryResourceBudget, QueryRetainedKind, QueryServiceRegistryConfig,
        QuerySubmissionMetadata, QueryTaskCharge,
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
        let admission = coordinator
            .admit_planning(query_id(99))
            .expect("admit one snapshot plan");
        runtime.begin_shutdown();
        assert!(!coordinator.planning_drained());
        assert!(coordinator.admit_planning(query_id(100)).is_err());
        assert!(
            !runtime
                .runtime_snapshot_pin_status()
                .expect("read closed Catalog pin admission")
                .admission_open
        );
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

    fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
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
        let schema = arrow_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42_i64]))],
        )
        .expect("build persisted query batch");
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id: FileId::from_u128(100).expect("nonzero file ID"),
                table_id: table_id(NATIVE_TABLE),
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
            table_id(NATIVE_TABLE),
            1,
            vec![candidate],
            OffsetWatermark::Known(offset),
        )
        .expect("build persist request")
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
                plan_ttl: Duration::from_nanos(10),
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
        let max_memory_bytes = snapshot_peak_bytes(mixed_refs().len())
            .checked_add(1024 * 1024)
            .expect("snapshot test budget remains within u64");
        assert!(max_memory_bytes <= DEFAULT_QUERY_MAX_MEMORY_BYTES);
        QueryResourceBudget {
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes,
        }
    }

    fn projection_budget() -> QueryResourceBudget {
        QueryResourceBudget {
            max_active_tasks: 2,
            max_cpu_slots: 2,
            max_memory_bytes: DEFAULT_QUERY_MAX_MEMORY_BYTES,
        }
    }

    fn snapshot_peak_bytes(selector_count: usize) -> u64 {
        super::conservative_query_snapshot_planning_peak_bytes(selector_count)
            .expect("query snapshot limits have a valid conservative peak")
            .get()
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
        let snapshot_memory_bytes = snapshot_peak_bytes(mixed_refs().len());
        let resource_status = resources.status();
        assert_eq!(resource_status.active_snapshot_memory_permits, 1);
        assert_eq!(
            resource_status.active_snapshot_memory_bytes,
            snapshot_memory_bytes
        );
        assert_eq!(resource_status.active_tasks, 0);
        assert_eq!(resource_status.active_cpu_slots, 0);
        let draft = snapshot.plan_draft();
        assert_eq!(draft.snapshots().len(), 2);
        assert_eq!(draft.snapshots()[0].table_id, table_id(NATIVE_TABLE));
        assert_eq!(draft.snapshots()[1].table_id, table_id(LEGACY_TABLE));

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
            .complete_native_planning_at_for_test(planning, draft, snapshot, 10)
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

        let foreign_file_error = match foreign.admit_file_task(&execution, charge(), 0, 0) {
            Ok(_) => panic!("foreign coordinator file task must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            foreign_file_error.code(),
            zippy_types::ErrorCode::InvalidArgument
        );
        assert_eq!(resources.status().active_tasks, 0);

        let missing_file_error =
            match snapshot_coordinator.admit_file_task(&execution, charge(), 0, 0) {
                Ok(_) => panic!("empty snapshot table must reject a file task"),
                Err(error) => error,
            };
        assert_eq!(
            missing_file_error.code(),
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
    async fn exact_snapshot_projection_is_capability_free_ordered_and_retained_accounted() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(113);
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                projection_budget(),
                10,
            )
            .expect("register projected snapshot query");
        let snapshot = snapshot_coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .expect("pin mixed projected snapshot");
        let expected_charges =
            super::projected_snapshot_charges(&snapshot.pinned).expect("estimate projection");
        assert!(
            expected_charges.task_peak_bytes.get()
                >= expected_charges
                    .retained_bytes
                    .get()
                    .checked_add(super::PROJECTED_SCHEMA_DECODE_TRANSIENT_BYTES)
                    .expect("projection charge comparison remains within u64")
        );
        let catalog_revision = snapshot.descriptor.catalog_revision;
        let token = resources
            .complete_native_planning_at_for_test(planning, snapshot.plan_draft(), snapshot, 10)
            .expect("complete projected snapshot plan");
        let execution = resources
            .begin_execution(token, 11)
            .expect("begin projected snapshot execution");

        let projected = snapshot_coordinator
            .project_persisted_snapshot(&execution)
            .expect("project exact persisted snapshot");
        assert_eq!(
            projected.kind(),
            QueryRetainedKind::ProjectedSnapshotManifest
        );
        assert_eq!(projected.memory_bytes(), expected_charges.retained_bytes);
        let status = resources.status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_cpu_slots, 0);
        assert_eq!(status.active_task_memory_bytes, 0);
        assert_eq!(status.active_retained_permits, 1);
        assert_eq!(
            status.active_retained_memory_bytes,
            expected_charges.retained_bytes.get()
        );

        let manifest = projected.resource();
        assert_eq!(manifest.query_id(), query);
        assert_eq!(manifest.catalog_revision(), catalog_revision);
        assert_eq!(
            manifest.retained_memory_bytes(),
            expected_charges.retained_bytes
        );
        assert_eq!(manifest.tables().len(), 2);
        let native = manifest.table(0).expect("native projected table");
        assert_eq!(native.table_index(), 0);
        assert_eq!(native.table_id(), table_id(NATIVE_TABLE));
        assert_eq!(native.snapshot_version().get(), 1);
        assert_eq!(native.schema_version().get(), 1);
        assert_eq!(native.schema().as_ref(), arrow_schema().as_ref());
        assert_eq!(native.file_count(), 0);
        let legacy = manifest.table(1).expect("legacy projected table");
        assert_eq!(legacy.table_index(), 1);
        assert_eq!(legacy.table_id(), table_id(LEGACY_TABLE));
        assert_eq!(legacy.snapshot_version().get(), 1);
        assert_eq!(legacy.schema_version().get(), 1);
        assert_eq!(legacy.schema().as_ref(), arrow_schema().as_ref());
        assert_eq!(legacy.file_count(), 0);

        drop(projected);
        assert_eq!(resources.status().active_retained_permits, 0);
        execution
            .finish()
            .expect("finish after retained manifest drop");
        assert_eq!(resources.status().active_memory_bytes, 0);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_file_projection_preserves_exact_identity_versions_and_scan_order() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        runtime
            .table_actor(table_id(NATIVE_TABLE))
            .expect("native table actor")
            .persist(persist_request())
            .await
            .expect("persist one native Parquet file");

        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(114);
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                projection_budget(),
                10,
            )
            .expect("register persisted file projection query");
        let snapshot = snapshot_coordinator
            .pin_persisted(
                &planning,
                &[TableRef::Id {
                    table_id: table_id(NATIVE_TABLE),
                }],
            )
            .await
            .expect("pin persisted file snapshot");
        let pinned_file = snapshot.pinned.table(0).unwrap().file(0).unwrap();
        let expected_file_id = pinned_file.file_id();
        let expected_size = pinned_file.file_size_bytes();
        let expected_rows = pinned_file.row_count();
        drop(pinned_file);
        let expected_snapshot_version = snapshot.descriptor.tables[0].persisted_snapshot_version;
        let token = resources
            .complete_native_planning_at_for_test(planning, snapshot.plan_draft(), snapshot, 10)
            .expect("complete persisted file projection plan");
        let execution = resources.begin_execution(token, 11).unwrap();
        let projected = snapshot_coordinator
            .project_persisted_snapshot(&execution)
            .expect("project persisted file snapshot");

        let table = projected
            .resource()
            .table(0)
            .expect("projected native table");
        assert_eq!(table.table_index(), 0);
        assert_eq!(table.table_id(), table_id(NATIVE_TABLE));
        assert_eq!(table.snapshot_version().get(), expected_snapshot_version);
        assert_eq!(table.file_count(), 1);
        let file = table.files()[0];
        assert_eq!(file.key().table_index(), 0);
        assert_eq!(file.key().file_index(), 0);
        assert_eq!(file.key().table_id(), table_id(NATIVE_TABLE));
        assert_eq!(file.file_id(), expected_file_id);
        assert_eq!(file.snapshot_version().get(), expected_snapshot_version);
        assert_eq!(file.schema_version(), table.schema_version());
        assert_eq!(file.file_size_bytes().get(), expected_size);
        assert_eq!(file.row_count(), expected_rows);

        let different_table_id = table_id(999);
        let different_file_id = FileId::from_u128(999).expect("nonzero forged file ID");
        let next_snapshot_version = NonZeroU64::new(file.snapshot_version().get() + 1)
            .expect("next snapshot version is nonzero");
        let next_schema_version = NonZeroU64::new(file.schema_version().get() + 1)
            .expect("next schema version is nonzero");
        let next_file_size =
            NonZeroU64::new(file.file_size_bytes().get() + 1).expect("next file size is nonzero");
        let next_row_count =
            NonZeroU64::new(file.row_count().get() + 1).expect("next row count is nonzero");
        let forged = [
            PersistedFileDescription::try_new(
                PersistedFileKey::new(1, 0, file.table_id(), file.file_id()),
                file.snapshot_version(),
                file.schema_version(),
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge table ordinal"),
            PersistedFileDescription::try_new(
                PersistedFileKey::new(0, 1, file.table_id(), file.file_id()),
                file.snapshot_version(),
                file.schema_version(),
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge file ordinal"),
            PersistedFileDescription::try_new(
                PersistedFileKey::new(0, 0, different_table_id, file.file_id()),
                file.snapshot_version(),
                file.schema_version(),
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge table identity"),
            PersistedFileDescription::try_new(
                PersistedFileKey::new(0, 0, file.table_id(), different_file_id),
                file.snapshot_version(),
                file.schema_version(),
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge file identity"),
            PersistedFileDescription::try_new(
                file.key(),
                next_snapshot_version,
                file.schema_version(),
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge snapshot version"),
            PersistedFileDescription::try_new(
                file.key(),
                file.snapshot_version(),
                next_schema_version,
                file.file_size_bytes(),
                file.row_count(),
            )
            .expect("forge schema version"),
            PersistedFileDescription::try_new(
                file.key(),
                file.snapshot_version(),
                file.schema_version(),
                next_file_size,
                file.row_count(),
            )
            .expect("forge file size"),
            PersistedFileDescription::try_new(
                file.key(),
                file.snapshot_version(),
                file.schema_version(),
                file.file_size_bytes(),
                next_row_count,
            )
            .expect("forge row count"),
        ];
        for description in forged {
            let error = snapshot_coordinator
                .admit_described_file_task(&execution, charge(), description)
                .err()
                .expect("forged persisted file description must be rejected");
            assert_eq!(error.code(), zippy_types::ErrorCode::DataCorruption);
            assert_eq!(resources.status().active_tasks, 0);
        }

        let task = snapshot_coordinator
            .admit_described_file_task(&execution, charge(), file)
            .expect("admit exact projected file description");
        assert_eq!(task.resource().file_id(), expected_file_id);
        let mut reader = AdmittedPinnedParquetFile::deferred(task);
        assert_eq!(resources.status().active_tasks, 1);
        assert_eq!(
            reader
                .get_bytes(0..4)
                .await
                .expect("read through deferred admitted file")
                .as_ref(),
            b"PAR1"
        );
        assert_eq!(
            reader
                .get_metadata()
                .await
                .expect("read deferred admitted metadata")
                .file_metadata()
                .num_rows(),
            1
        );
        drop(reader);
        assert_eq!(resources.status().active_tasks, 0);

        let cancelled_task = snapshot_coordinator
            .admit_described_file_task(&execution, charge(), file)
            .expect("admit exact file before cancellation");
        let mut cancelled_reader = AdmittedPinnedParquetFile::deferred(cancelled_task);
        resources
            .cancel_current_for_test(query)
            .expect("cancel before deferred file open");
        let error = cancelled_reader
            .get_metadata()
            .await
            .expect_err("cancelled deferred file must not open");
        assert!(error.to_string().contains("cancelled before Parquet open"));
        assert_eq!(resources.status().active_tasks, 0);
        drop(cancelled_reader);

        drop(projected);
        drop(execution);
        assert_eq!(resources.status().active_memory_bytes, 0);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("cancelled deferred reader releases the snapshot pin")
                .active_query_pins,
            0
        );
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn projection_budget_failure_precedes_manifest_build_and_rolls_back_task_accounting() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(115);
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
            .expect("register projection query with snapshot-only budget");
        let snapshot = snapshot_coordinator
            .pin_persisted(&planning, &mixed_refs())
            .await
            .expect("snapshot-only budget still admits pinning");
        let token = resources
            .complete_native_planning_at_for_test(planning, snapshot.plan_draft(), snapshot, 10)
            .expect("complete under-budget projection plan");
        let execution = resources.begin_execution(token, 11).unwrap();

        let error = snapshot_coordinator
            .project_persisted_snapshot(&execution)
            .err()
            .expect("projection peak must exceed the snapshot-only budget");
        assert_eq!(error.code(), zippy_types::ErrorCode::ResourceExhausted);
        let status = resources.status();
        assert_eq!(status.active_tasks, 0);
        assert_eq!(status.active_cpu_slots, 0);
        assert_eq!(status.active_task_memory_bytes, 0);
        assert_eq!(status.active_retained_permits, 0);
        assert_eq!(status.active_retained_memory_bytes, 0);

        execution.finish().unwrap();
        assert_eq!(resources.status().active_memory_bytes, 0);
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unrelated_small_reservation_cannot_replace_snapshot_owned_memory() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(111);
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
        let snapshot_memory_bytes = snapshot_peak_bytes(mixed_refs().len());
        let unrelated = planning
            .reserve_snapshot_memory(NonZeroU64::new(1).expect("nonzero test reservation"))
            .expect("reserve unrelated planning memory");
        assert_eq!(resources.status().active_snapshot_memory_permits, 2);

        let token = resources
            .complete_native_planning_at_for_test(planning, snapshot.plan_draft(), snapshot, 10)
            .expect("complete only from the snapshot-owned reservation");
        drop(unrelated);
        assert_eq!(resources.status().active_snapshot_memory_permits, 1);
        assert_eq!(
            resources.status().active_snapshot_memory_bytes,
            snapshot_memory_bytes
        );
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("read retained snapshot pins")
                .active_query_pins,
            2
        );

        drop(token);
        assert_eq!(resources.status().active_snapshot_memory_permits, 0);
        assert_eq!(resources.status().active_snapshot_memory_bytes, 0);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("read released snapshot pins")
                .active_query_pins,
            0
        );
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn foreign_catalog_reservation_is_rejected_for_the_same_query_incarnation() {
        let (_first_temp, _first_ownership, mut first_runtime) = start_runtime(8).await;
        let (_second_temp, _second_ownership, mut second_runtime) = start_runtime(8).await;
        let first_resources = first_runtime.query_resource_coordinator();
        let second_resources = second_runtime.query_resource_coordinator();
        let query = query_id(112);
        let first_owner = first_resources.owner(session_id());
        let second_owner = second_resources.owner(session_id());
        let first_planning = first_resources
            .submit_at_for_test(
                first_owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("register first runtime query");
        let second_planning = second_resources
            .submit_at_for_test(
                second_owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                budget(),
                10,
            )
            .expect("register matching second runtime query");
        assert_eq!(first_planning.instance_id(), second_planning.instance_id());
        let foreign_snapshot = second_runtime
            .query_snapshot_coordinator()
            .pin_persisted(&second_planning, &mixed_refs())
            .await
            .expect("pin snapshot under the second Catalog authority");

        let error = match first_resources.complete_native_planning_at_for_test(
            first_planning,
            foreign_snapshot.plan_draft(),
            foreign_snapshot,
            10,
        ) {
            Ok(_) => panic!("foreign reservation must not match the first query ledger"),
            Err(error) => error,
        };
        assert_eq!(error.code(), zippy_types::ErrorCode::Internal);
        assert_eq!(first_resources.status().active_snapshot_memory_permits, 0);
        assert_eq!(second_resources.status().active_snapshot_memory_permits, 0);
        assert_eq!(
            second_runtime
                .runtime_snapshot_pin_status()
                .expect("read released foreign pins")
                .active_query_pins,
            0
        );

        drop(second_planning);
        assert!(first_runtime.shutdown(Duration::from_secs(5)).is_complete());
        assert!(second_runtime
            .shutdown(Duration::from_secs(5))
            .is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_memory_admission_precedes_catalog_resolution_and_pin_work() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(110);
        let required = snapshot_peak_bytes(1);
        let planning = resources
            .submit_at_for_test(
                owner.as_ref(),
                QuerySubmissionMetadata {
                    query_id: query,
                    deadline_ns: None,
                },
                QueryResourceBudget {
                    max_active_tasks: 1,
                    max_cpu_slots: 1,
                    max_memory_bytes: required - 1,
                },
                10,
            )
            .expect("register an intentionally under-budget query");

        let error = snapshot_coordinator
            .pin_persisted(
                &planning,
                &[TableRef::Id {
                    table_id: table_id(999),
                }],
            )
            .await
            .err()
            .expect("snapshot memory must fail before resolving the missing table");
        assert_eq!(error.code(), zippy_types::ErrorCode::ResourceExhausted);
        let resource_status = resources.status();
        assert_eq!(resource_status.active_snapshot_memory_permits, 0);
        assert_eq!(resource_status.active_snapshot_memory_bytes, 0);
        assert_eq!(resource_status.active_tasks, 0);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("read pin status after rejected snapshot admission")
                .active_query_pins,
            0
        );
        resources
            .fail_planning(planning, error)
            .expect("record the admission failure as terminal");
        assert!(runtime.shutdown(Duration::from_secs(5)).is_complete());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admitted_parquet_cursor_retains_task_and_pin_until_final_drop() {
        let (_temp, _ownership, mut runtime) = start_runtime(8).await;
        runtime
            .table_actor(table_id(NATIVE_TABLE))
            .expect("native table actor")
            .persist(persist_request())
            .await
            .expect("persist one native Parquet file");

        let snapshot_coordinator = runtime.query_snapshot_coordinator();
        let resources = runtime.query_resource_coordinator();
        let owner = resources.owner(session_id());
        let query = query_id(109);
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
            .expect("register native Parquet query");
        let snapshot = snapshot_coordinator
            .pin_persisted(
                &planning,
                &[TableRef::Id {
                    table_id: table_id(NATIVE_TABLE),
                }],
            )
            .await
            .expect("pin persisted Parquet snapshot");
        let draft = snapshot.plan_draft();
        let token = resources
            .complete_native_planning_at_for_test(planning, draft, snapshot, 10)
            .expect("complete native Parquet plan");
        let execution = resources
            .begin_execution(token, 11)
            .expect("begin native Parquet execution");
        let file_task = snapshot_coordinator
            .admit_file_task(&execution, charge(), 0, 0)
            .expect("admit exact pinned file task");
        let source =
            tokio::task::spawn_blocking(move || AdmittedPinnedParquetFile::open(file_task))
                .await
                .expect("join bounded file opener")
                .expect("open admitted Parquet source");
        assert_eq!(resources.status().active_tasks, 1);
        assert_eq!(resources.status().active_result_permits, 0);

        let range = source
            .get_bytes(0, 4)
            .expect("read one query-accounted Parquet range");
        assert_eq!(range.as_ref(), b"PAR1");
        assert_eq!(resources.status().active_result_permits, 1);
        assert_eq!(resources.status().active_result_memory_bytes, 4);
        let cloned_range = range.clone();
        let second_range = source
            .get_bytes(0, 4)
            .expect("reserve a second independent Parquet range");
        assert_eq!(resources.status().active_result_permits, 2);
        assert_eq!(resources.status().active_result_memory_bytes, 8);
        assert!(source.get_bytes(0, 4).is_err());
        assert_eq!(resources.status().active_result_permits, 2);
        drop(range);
        assert_eq!(resources.status().active_result_permits, 2);
        drop(second_range);
        assert_eq!(resources.status().active_result_permits, 1);
        assert_eq!(resources.status().active_result_memory_bytes, 4);
        drop(cloned_range);
        assert_eq!(resources.status().active_result_permits, 0);
        assert_eq!(resources.status().active_result_memory_bytes, 0);

        let mut async_reader = AdmittedAsyncParquetReader::new(source.clone());
        let metadata = async_reader
            .get_metadata()
            .await
            .expect("read permit-owned async Parquet metadata");
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 1);
        drop(async_reader);
        assert_eq!(resources.status().active_result_permits, 0);
        assert_eq!(resources.status().active_result_memory_bytes, 0);

        let scan_source = source.clone();
        let batch = tokio::task::spawn_blocking(move || {
            let mut reader = ParquetRecordBatchReaderBuilder::try_new(scan_source)
                .expect("read admitted Parquet metadata")
                .build()
                .expect("build admitted Parquet reader");
            reader
                .next()
                .expect("one admitted Parquet batch")
                .expect("decode admitted Parquet batch")
        })
        .await
        .expect("join bounded Parquet scan");
        assert_eq!(resources.status().active_result_permits, 0);
        assert_eq!(resources.status().active_result_memory_bytes, 0);
        let prices = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("retain Int64 price column");
        assert_eq!(prices.len(), 1);
        assert_eq!(prices.value(0), 42);

        let mut cursor = source.get_read(0).expect("open lease-bound Parquet cursor");
        let mut magic = [0_u8; 4];
        cursor
            .read_exact(&mut magic)
            .expect("read Parquet magic through admitted cursor");
        assert_eq!(&magic, b"PAR1");
        drop(source);
        assert_eq!(resources.status().active_tasks, 1);

        resources
            .cancel_current_for_test(query)
            .expect("cancel native Parquet query");
        drop(execution);
        assert_eq!(resources.status().active_tasks, 1);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("cursor retains Catalog pin")
                .active_query_pins,
            1
        );
        let cancellation = cursor
            .read(&mut [0_u8; 1])
            .expect_err("cancelled cursor must stop producing bytes");
        assert_eq!(cancellation.kind(), std::io::ErrorKind::ConnectionAborted);

        drop(cursor);
        assert_eq!(resources.status().active_tasks, 0);
        assert_eq!(resources.status().active_cpu_slots, 0);
        assert_eq!(resources.status().active_memory_bytes, 0);
        assert_eq!(
            runtime
                .runtime_snapshot_pin_status()
                .expect("final cursor releases Catalog pin")
                .active_query_pins,
            0
        );
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
            .complete_native_planning_at_for_test(planning, snapshot.plan_draft(), snapshot, 10)
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
