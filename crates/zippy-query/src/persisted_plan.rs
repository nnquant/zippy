//! Capability-free persisted manifests and snapshot-bound DataFusion plan integration.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Weak};

use crate::BoxedAsyncFileReader;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{ParquetExec, ParquetFileReaderFactory};
use datafusion::datasource::physical_plan::{FileMeta, FileScanConfig};
use datafusion::datasource::TableProvider;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use parquet::arrow::async_reader::AsyncFileReader;
use sha2::{Digest, Sha256};
use zippy_types::schema_hash::canonical_catalog_schema_digest_v1;
use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;
use zippy_types::{FileId, TableId};

const PERSISTED_OBJECT_STORE_URL: &str = "zippy-query://persisted";
const NORMALIZED_PERSISTED_OBJECT_STORE_URL: &str = "zippy-query://persisted/";
const PLANNING_ONLY_READER_ERROR: &str =
    "planning-only Zippy Parquet scan has no snapshot-bound reader factory";
const EXPIRED_EXECUTION_LEASE_ERROR: &str =
    "snapshot-bound Zippy Parquet scan execution lease has expired";
const PERSISTED_SNAPSHOT_DIGEST_VERSION: u32 = 1;

static PLANNING_READER_FACTORY: LazyLock<Arc<dyn ParquetFileReaderFactory>> =
    LazyLock::new(|| Arc::new(PlanningOnlyParquetReaderFactory));

/// Stable, path-free identity of one file in an exact query snapshot scan order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersistedFileKey {
    table_index: usize,
    file_index: usize,
    table_id: TableId,
    file_id: FileId,
}

impl PersistedFileKey {
    /// Binds stable Catalog identities to their exact snapshot scan ordinals.
    #[must_use]
    pub const fn new(
        table_index: usize,
        file_index: usize,
        table_id: TableId,
        file_id: FileId,
    ) -> Self {
        Self {
            table_index,
            file_index,
            table_id,
            file_id,
        }
    }

    /// Returns the deterministic table ordinal in the pinned query snapshot.
    #[must_use]
    pub const fn table_index(self) -> usize {
        self.table_index
    }

    /// Returns the deterministic scan ordinal in the pinned table manifest.
    #[must_use]
    pub const fn file_index(self) -> usize {
        self.file_index
    }

    /// Returns the immutable owning table identity.
    #[must_use]
    pub const fn table_id(self) -> TableId {
        self.table_id
    }

    /// Returns the immutable physical file identity.
    #[must_use]
    pub const fn file_id(self) -> FileId {
        self.file_id
    }
}

/// Immutable, path-free description of one persisted file visible to a query snapshot.
///
/// This type deliberately has no location, reader, callback, or generic extension field. Its
/// constructor accepts only stable identities and scalar metadata. The server must retain and
/// resolve the corresponding snapshot capability outside this crate when execution begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedFileDescription {
    key: PersistedFileKey,
    snapshot_version: NonZeroU64,
    schema_version: NonZeroU64,
    file_size_bytes: NonZeroU64,
    row_count: NonZeroU64,
}

impl PersistedFileDescription {
    /// Creates a pure file description after validating DataFusion's platform-sized metadata.
    ///
    /// DataFusion 44 converts a `PartitionedFile` size to `usize` internally. Rejecting values
    /// that do not fit here prevents a silent truncation before a physical plan is built.
    pub fn try_new(
        key: PersistedFileKey,
        snapshot_version: NonZeroU64,
        schema_version: NonZeroU64,
        file_size_bytes: NonZeroU64,
        row_count: NonZeroU64,
    ) -> Result<Self> {
        usize::try_from(file_size_bytes.get()).map_err(|_| {
            DataFusionError::Plan(format!(
                "persisted file size does not fit this platform file_id=[{:032x}]",
                key.file_id.as_u128()
            ))
        })?;
        usize::try_from(row_count.get()).map_err(|_| {
            DataFusionError::Plan(format!(
                "persisted file row count does not fit this platform file_id=[{:032x}]",
                key.file_id.as_u128()
            ))
        })?;
        Ok(Self {
            key,
            snapshot_version,
            schema_version,
            file_size_bytes,
            row_count,
        })
    }

    /// Returns the exact path-free snapshot file key.
    #[must_use]
    pub const fn key(self) -> PersistedFileKey {
        self.key
    }

    /// Returns the deterministic table ordinal in the pinned query snapshot.
    #[must_use]
    pub const fn table_index(self) -> usize {
        self.key.table_index
    }

    /// Returns the deterministic scan ordinal in the pinned table manifest.
    #[must_use]
    pub const fn file_index(self) -> usize {
        self.key.file_index
    }

    /// Returns the immutable owning table identity.
    #[must_use]
    pub const fn table_id(self) -> TableId {
        self.key.table_id
    }

    /// Returns the exact table snapshot in which this scan description was planned.
    #[must_use]
    pub const fn snapshot_version(self) -> NonZeroU64 {
        self.snapshot_version
    }

    /// Returns the immutable physical file identity.
    #[must_use]
    pub const fn file_id(self) -> FileId {
        self.key.file_id
    }

    /// Returns the schema version encoded by the physical file.
    #[must_use]
    pub const fn schema_version(self) -> NonZeroU64 {
        self.schema_version
    }

    /// Returns the immutable physical length recorded by the Catalog.
    #[must_use]
    pub const fn file_size_bytes(self) -> NonZeroU64 {
        self.file_size_bytes
    }

    /// Returns the immutable logical row count recorded by the Catalog.
    #[must_use]
    pub const fn row_count(self) -> NonZeroU64 {
        self.row_count
    }

    /// Returns a synthetic object key derived only from stable identities.
    ///
    /// The key is a single opaque object-store segment, not a filesystem or object-store path
    /// supplied by a caller. A real reader factory must validate it against the typed extension
    /// before resolving the file identity through its separately owned snapshot capability.
    #[must_use]
    pub fn synthetic_object_key(self) -> String {
        format!(
            "q{:016x}{:016x}{:032x}{:016x}{:032x}",
            self.key.table_index,
            self.key.file_index,
            self.key.table_id.as_u128(),
            self.snapshot_version.get(),
            self.key.file_id.as_u128()
        )
    }

    fn into_partitioned_file(self) -> PartitionedFile {
        // try_new proves this conversion cannot truncate on the current platform.
        let file_size_bytes = self.file_size_bytes.get();
        PartitionedFile::new(self.synthetic_object_key(), file_size_bytes)
            .with_extensions(Arc::new(self))
    }
}

/// Immutable, capability-free description of one persisted table snapshot.
///
/// File descriptions are checked for table/snapshot consistency, duplicate identities, schema
/// monotonicity, and aggregate platform-size overflow at construction. The manifest contains no
/// storage location and no execution context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedTableDescription {
    table_index: usize,
    table_id: TableId,
    snapshot_version: NonZeroU64,
    schema_version: NonZeroU64,
    schema: SchemaRef,
    schema_digest: [u8; 32],
    retained_schema_bytes: NonZeroU64,
    files: Arc<[PersistedFileDescription]>,
    total_file_size_bytes: usize,
    total_rows: usize,
}

/// Validated, capability-free description of every persisted table in one query snapshot.
///
/// Construction is the only place that performs cross-table identity validation. The resulting
/// value is cheap to clone and lets planning, binding, and execution reuse the same proven table
/// and file ordering without repeatedly scanning the complete manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedSnapshotDescription {
    tables: Arc<[PersistedTableDescription]>,
    total_file_count: usize,
    max_files_per_table: usize,
    total_retained_schema_bytes: u64,
    max_retained_schema_bytes: u64,
    max_columns_per_table: usize,
    manifest_digest: [u8; 32],
}

impl PersistedSnapshotDescription {
    /// Creates one nonempty snapshot description after linear-time global identity validation.
    pub fn try_new(tables: Vec<PersistedTableDescription>) -> Result<Self> {
        if tables.is_empty() {
            return Err(binding_error(
                "persisted snapshot description must contain at least one table",
            ));
        }

        let mut table_ids = HashSet::with_capacity(tables.len());
        let total_file_count = tables.iter().try_fold(0_usize, |total, table| {
            total
                .checked_add(table.file_count())
                .ok_or_else(|| binding_error("persisted snapshot file count overflowed usize"))
        })?;
        let mut file_ids = HashSet::with_capacity(total_file_count);
        let mut max_files_per_table = 0_usize;
        let mut total_retained_schema_bytes = 0_u64;
        let mut max_retained_schema_bytes = 0_u64;
        let mut max_columns_per_table = 0_usize;

        for (expected_table_index, table) in tables.iter().enumerate() {
            if table.table_index() != expected_table_index {
                return Err(binding_error(format!(
                    "persisted table ordinal is not contiguous expected=[{expected_table_index}] actual=[{}]",
                    table.table_index()
                )));
            }
            if !table_ids.insert(table.table_id()) {
                return Err(binding_error(format!(
                    "persisted snapshot contains duplicate table identity table_id=[{}]",
                    table.table_id().as_u128()
                )));
            }

            max_files_per_table = max_files_per_table.max(table.file_count());
            let retained_schema_bytes = table.retained_schema_bytes().get();
            total_retained_schema_bytes = total_retained_schema_bytes
                .checked_add(retained_schema_bytes)
                .ok_or_else(|| binding_error("persisted snapshot schema bytes overflowed u64"))?;
            max_retained_schema_bytes = max_retained_schema_bytes.max(retained_schema_bytes);
            max_columns_per_table = max_columns_per_table.max(table.schema().fields().len());

            for (expected_file_index, file) in table.files().iter().enumerate() {
                if file.table_index() != expected_table_index
                    || file.file_index() != expected_file_index
                    || file.table_id() != table.table_id()
                    || file.snapshot_version() != table.snapshot_version()
                    || file.schema_version() > table.schema_version()
                {
                    return Err(binding_error(
                        "persisted snapshot file description is inconsistent",
                    ));
                }
                if !file_ids.insert(file.file_id()) {
                    return Err(binding_error(format!(
                        "persisted snapshot contains duplicate physical file identity file_id=[{}]",
                        file.file_id().as_u128()
                    )));
                }
            }
        }

        let manifest_digest = persisted_snapshot_digest(&tables)?;
        Ok(Self {
            tables: tables.into(),
            total_file_count,
            max_files_per_table,
            total_retained_schema_bytes,
            max_retained_schema_bytes,
            max_columns_per_table,
            manifest_digest,
        })
    }

    /// Returns the validated tables in deterministic snapshot order.
    #[must_use]
    pub fn tables(&self) -> &[PersistedTableDescription] {
        &self.tables
    }

    /// Returns one table by its deterministic snapshot ordinal.
    #[must_use]
    pub fn table(&self, table_index: usize) -> Option<&PersistedTableDescription> {
        self.tables.get(table_index)
    }

    /// Returns the number of tables in the snapshot description.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub(crate) const fn total_file_count(&self) -> usize {
        self.total_file_count
    }

    pub(crate) const fn max_files_per_table(&self) -> usize {
        self.max_files_per_table
    }

    pub(crate) const fn total_retained_schema_bytes(&self) -> u64 {
        self.total_retained_schema_bytes
    }

    pub(crate) const fn max_retained_schema_bytes(&self) -> u64 {
        self.max_retained_schema_bytes
    }

    pub(crate) const fn max_columns_per_table(&self) -> usize {
        self.max_columns_per_table
    }

    pub(crate) const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
}

fn persisted_snapshot_digest(tables: &[PersistedTableDescription]) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(PERSISTED_SNAPSHOT_DIGEST_VERSION.to_le_bytes());
    digest.update(
        u64::try_from(tables.len())
            .map_err(|_| binding_error("persisted snapshot table count exceeds u64"))?
            .to_le_bytes(),
    );
    for table in tables {
        digest.update(
            u64::try_from(table.table_index())
                .map_err(|_| binding_error("persisted snapshot table ordinal exceeds u64"))?
                .to_le_bytes(),
        );
        digest.update(table.table_id().as_u128().to_le_bytes());
        digest.update(table.snapshot_version().get().to_le_bytes());
        digest.update(table.schema_version().get().to_le_bytes());
        digest.update(table.schema_digest());
        digest.update(table.retained_schema_bytes().get().to_le_bytes());
        digest.update(
            u64::try_from(table.file_count())
                .map_err(|_| binding_error("persisted snapshot file count exceeds u64"))?
                .to_le_bytes(),
        );
        for file in table.files() {
            digest.update(
                u64::try_from(file.file_index())
                    .map_err(|_| binding_error("persisted snapshot file ordinal exceeds u64"))?
                    .to_le_bytes(),
            );
            digest.update(file.file_id().as_u128().to_le_bytes());
            digest.update(file.snapshot_version().get().to_le_bytes());
            digest.update(file.schema_version().get().to_le_bytes());
            digest.update(file.file_size_bytes().get().to_le_bytes());
            digest.update(file.row_count().get().to_le_bytes());
        }
    }
    Ok(digest.finalize().into())
}

impl PersistedTableDescription {
    /// Creates a validated persisted table manifest from pure metadata.
    pub fn try_new(
        table_index: usize,
        table_id: TableId,
        snapshot_version: NonZeroU64,
        schema_version: NonZeroU64,
        schema: SchemaRef,
        files: Vec<PersistedFileDescription>,
    ) -> Result<Self> {
        let retained_schema_bytes = conservative_arrow_schema_retained_bytes(schema.as_ref())
            .map_err(|error| {
                DataFusionError::ResourcesExhausted(format!(
                    "persisted schema retained-memory charge failed error=[{error}]"
                ))
            })?;
        let schema_digest = canonical_catalog_schema_digest_v1(schema.as_ref());
        let mut file_ids = HashSet::with_capacity(files.len());
        let mut total_file_size_bytes = 0_usize;
        let mut total_rows = 0_usize;
        for (expected_file_index, file) in files.iter().enumerate() {
            if file.key.table_index != table_index {
                return Err(DataFusionError::Plan(format!(
                    "persisted file belongs to another table ordinal file_id=[{}]",
                    file.key.file_id
                )));
            }
            if file.key.file_index != expected_file_index {
                return Err(DataFusionError::Plan(format!(
                    "persisted file scan ordinal is not contiguous file_id=[{}] expected=[{}] actual=[{}]",
                    file.key.file_id, expected_file_index, file.key.file_index
                )));
            }
            if file.key.table_id != table_id {
                return Err(DataFusionError::Plan(format!(
                    "persisted file belongs to another table file_id=[{}]",
                    file.key.file_id.as_u128()
                )));
            }
            if file.snapshot_version != snapshot_version {
                return Err(DataFusionError::Plan(format!(
                    "persisted file belongs to another snapshot file_id=[{}]",
                    file.key.file_id.as_u128()
                )));
            }
            if file.schema_version > schema_version {
                return Err(DataFusionError::Plan(format!(
                    "persisted file schema is newer than table schema file_id=[{}]",
                    file.key.file_id.as_u128()
                )));
            }
            if !file_ids.insert(file.key.file_id) {
                return Err(DataFusionError::Plan(format!(
                    "persisted manifest contains duplicate file identity file_id=[{}]",
                    file.key.file_id.as_u128()
                )));
            }
            let file_size_bytes = usize::try_from(file.file_size_bytes.get()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "persisted file size does not fit this platform file_id=[{}]",
                    file.key.file_id.as_u128()
                ))
            })?;
            let row_count = usize::try_from(file.row_count.get()).map_err(|_| {
                DataFusionError::Plan(format!(
                    "persisted file row count does not fit this platform file_id=[{}]",
                    file.key.file_id.as_u128()
                ))
            })?;
            total_file_size_bytes = total_file_size_bytes
                .checked_add(file_size_bytes)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "persisted manifest total file size exceeds this platform".to_owned(),
                    )
                })?;
            total_rows = total_rows.checked_add(row_count).ok_or_else(|| {
                DataFusionError::Plan(
                    "persisted manifest total row count exceeds this platform".to_owned(),
                )
            })?;
        }

        Ok(Self {
            table_index,
            table_id,
            snapshot_version,
            schema_version,
            schema,
            schema_digest,
            retained_schema_bytes,
            files: files.into(),
            total_file_size_bytes,
            total_rows,
        })
    }

    /// Returns the deterministic table ordinal in the pinned query snapshot.
    #[must_use]
    pub const fn table_index(&self) -> usize {
        self.table_index
    }

    /// Returns the immutable table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the exact table snapshot version represented by this manifest.
    #[must_use]
    pub const fn snapshot_version(&self) -> NonZeroU64 {
        self.snapshot_version
    }

    /// Returns the decoder schema version selected for this snapshot.
    #[must_use]
    pub const fn schema_version(&self) -> NonZeroU64 {
        self.schema_version
    }

    /// Returns the immutable Arrow schema selected for this snapshot.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the canonical content digest of the exact Arrow schema.
    #[must_use]
    pub const fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    /// Returns the shared conservative memory charge for retaining the decoded schema.
    #[must_use]
    pub const fn retained_schema_bytes(&self) -> NonZeroU64 {
        self.retained_schema_bytes
    }

    /// Returns the deterministic file descriptions in scan order.
    #[must_use]
    pub fn files(&self) -> &[PersistedFileDescription] {
        &self.files
    }

    /// Returns the number of files in the persisted manifest.
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: Precision::Exact(self.total_rows),
            total_byte_size: Precision::Exact(self.total_file_size_bytes),
            column_statistics: Statistics::unknown_column(&self.schema),
        }
    }
}

/// Pure metadata passed from DataFusion's bound scan to a server-owned file opener.
///
/// The request deliberately contains no path, file handle, snapshot capability, or execution
/// context. The opener must resolve the description through the live execution lease it owns and
/// reject any value that does not exactly match that lease's retained snapshot manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedFileOpenRequest {
    description: PersistedFileDescription,
    partition_index: usize,
}

impl PersistedFileOpenRequest {
    fn new(description: PersistedFileDescription, partition_index: usize) -> Self {
        Self {
            description,
            partition_index,
        }
    }

    /// Returns the exact path-free file description validated by the bound reader factory.
    #[must_use]
    pub const fn description(self) -> PersistedFileDescription {
        self.description
    }

    /// Returns DataFusion's physical file-group partition index.
    #[must_use]
    pub const fn partition_index(self) -> usize {
        self.partition_index
    }
}

/// Server-owned bridge from a validated persisted file description to a lazy Parquet reader.
///
/// Implementations must perform only bounded, nonblocking construction in [`Self::open_reader`].
/// File opening, checksum verification, and other blocking work must remain deferred inside the
/// returned reader. The implementation must also revalidate the description against its live
/// execution lease before admitting any physical file task.
pub trait PersistedFileOpener: Send + Sync + 'static {
    /// Returns the exact capability-free snapshot this opener is authorized to resolve.
    fn snapshot_description(&self) -> &PersistedSnapshotDescription;

    /// Creates a lazy reader for one snapshot-bound file description.
    fn open_reader(
        &self,
        request: PersistedFileOpenRequest,
    ) -> zippy_types::Result<BoxedAsyncFileReader>;
}

/// A physical plan whose persisted scans are bound to one weak execution authority.
///
/// Keeping the authority weak is intentional: cloning or retaining the DataFusion plan cannot
/// prolong access to pinned storage after the server drops its native execution lease.
pub struct BoundPersistedPlan {
    plan: Arc<dyn ExecutionPlan>,
    bound_scan_count: usize,
}

impl fmt::Debug for BoundPersistedPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundPersistedPlan")
            .field("root", &self.plan.name())
            .field("bound_scan_count", &self.bound_scan_count)
            .finish_non_exhaustive()
    }
}

impl BoundPersistedPlan {
    #[cfg(test)]
    pub(crate) const fn execution_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    /// Returns how many persisted Parquet scan nodes were bound in the complete plan tree.
    #[must_use]
    pub const fn bound_scan_count(&self) -> usize {
        self.bound_scan_count
    }
}

/// Validates and atomically binds every persisted scan in a complete physical plan tree.
///
/// The expected tables are the trust root and must be borrowed from the same retained manifest
/// owned by the server's execution lease. This function must run inside a separately admitted
/// physical-plan construction task because rewriting temporarily retains both the original and
/// replacement plan nodes. An empty or optimizer-eliminated persisted scan is valid and produces
/// a bound count of zero.
pub(crate) fn bind_persisted_snapshot_plan(
    plan: Arc<dyn ExecutionPlan>,
    expected_snapshot: &PersistedSnapshotDescription,
    opener: &Arc<dyn PersistedFileOpener>,
) -> Result<BoundPersistedPlan> {
    if opener.snapshot_description() != expected_snapshot {
        return Err(binding_error(
            "persisted file opener belongs to another snapshot description",
        ));
    }
    let expected_tables = expected_snapshot.tables();
    let bound_factory: Arc<dyn ParquetFileReaderFactory> = Arc::new(BoundParquetReaderFactory {
        opener: Arc::downgrade(opener),
    });
    let mut visited_marker_nodes = HashSet::new();
    let mut bound_scan_count = 0_usize;

    let plan = plan
        .transform_up(|node| {
            let Some(parquet) = node.as_any().downcast_ref::<ParquetExec>() else {
                return Ok(Transformed::no(node));
            };
            let has_marker = has_factory(parquet, &PLANNING_READER_FACTORY);
            let has_reserved_signal = is_reserved_zippy_scan(parquet);
            if !has_marker {
                if has_reserved_signal {
                    return Err(binding_error(
                        "persisted Parquet scan is missing the exact planning marker",
                    ));
                }
                return Ok(Transformed::no(node));
            }

            let node_identity = Arc::as_ptr(&node) as *const () as usize;
            if !visited_marker_nodes.insert(node_identity) {
                return Err(binding_error(
                    "persisted planning scan node is reused as a physical-plan DAG child",
                ));
            }
            validate_planning_scan(parquet, expected_tables)?;
            bound_scan_count = bound_scan_count
                .checked_add(1)
                .ok_or_else(|| binding_error("persisted physical plan scan count overflowed"))?;
            let replacement: Arc<dyn ExecutionPlan> = Arc::new(
                parquet
                    .clone()
                    .with_parquet_file_reader_factory(Arc::clone(&bound_factory)),
            );
            Ok(Transformed::yes(replacement))
        })?
        .data;

    validate_bound_tree(&plan, expected_tables, &bound_factory)?;
    Ok(BoundPersistedPlan {
        plan,
        bound_scan_count,
    })
}

fn validate_planning_scan(
    parquet: &ParquetExec,
    expected_tables: &[PersistedTableDescription],
) -> Result<()> {
    let config = parquet.base_config();
    if config.object_store_url.as_str() != NORMALIZED_PERSISTED_OBJECT_STORE_URL {
        return Err(binding_error(format!(
            "persisted scan object-store URL mismatch actual=[{}]",
            config.object_store_url.as_str()
        )));
    }
    if !config.table_partition_cols.is_empty() || !config.output_ordering.is_empty() {
        return Err(binding_error(
            "persisted scan introduced partition columns or output ordering",
        ));
    }
    let parquet_options = &parquet.table_parquet_options().global;
    if parquet_options.enable_page_index || parquet_options.bloom_filter_on_read {
        return Err(binding_error(
            "persisted scan enabled unaccounted page-index or bloom-filter metadata",
        ));
    }
    if config.file_groups.len() != 1 {
        return Err(binding_error(format!(
            "persisted scan must contain exactly one file group actual=[{}]",
            config.file_groups.len()
        )));
    }
    let files = &config.file_groups[0];
    let first = files
        .first()
        .ok_or_else(|| binding_error("persisted planning scan cannot represent an empty table"))?;
    let first_description = partitioned_file_description(first)?;
    let expected_table = expected_tables
        .get(first_description.table_index())
        .ok_or_else(|| {
            binding_error(format!(
                "persisted scan table ordinal is out of bounds actual=[{}]",
                first_description.table_index()
            ))
        })?;
    if config.file_schema.as_ref() != expected_table.schema().as_ref() {
        return Err(binding_error(format!(
            "persisted scan schema mismatch table_id=[{}]",
            expected_table.table_id().as_u128()
        )));
    }
    if config.statistics != expected_table.statistics() {
        return Err(binding_error(format!(
            "persisted scan statistics mismatch table_id=[{}]",
            expected_table.table_id().as_u128()
        )));
    }
    if files.len() != expected_table.file_count() {
        return Err(binding_error(format!(
            "persisted scan file count mismatch table_id=[{}] expected=[{}] actual=[{}]",
            expected_table.table_id().as_u128(),
            expected_table.file_count(),
            files.len()
        )));
    }
    if let Some(projection) = &config.projection {
        if let Some(index) = projection
            .iter()
            .copied()
            .find(|&index| index >= expected_table.schema().fields().len())
        {
            return Err(binding_error(format!(
                "persisted scan projection index is out of bounds index=[{index}]"
            )));
        }
    }
    for (partitioned, expected_file) in files.iter().zip(expected_table.files()) {
        validate_partitioned_file(partitioned, *expected_file)?;
    }
    Ok(())
}

fn validate_partitioned_file(
    partitioned: &PartitionedFile,
    expected: PersistedFileDescription,
) -> Result<()> {
    let actual = partitioned_file_description(partitioned)?;
    if actual != expected {
        return Err(binding_error(format!(
            "persisted scan file description mismatch file_id=[{}]",
            actual.file_id().as_u128()
        )));
    }
    let expected_partitioned = expected.into_partitioned_file();
    if partitioned.object_meta != expected_partitioned.object_meta {
        return Err(binding_error(format!(
            "persisted scan synthetic object metadata mismatch file_id=[{}]",
            expected.file_id().as_u128()
        )));
    }
    if !partitioned.partition_values.is_empty()
        || partitioned.range.is_some()
        || partitioned.statistics.is_some()
        || partitioned.metadata_size_hint.is_some()
    {
        return Err(binding_error(format!(
            "persisted scan file contains unsupported physical metadata file_id=[{}]",
            expected.file_id().as_u128()
        )));
    }
    Ok(())
}

fn partitioned_file_description(partitioned: &PartitionedFile) -> Result<PersistedFileDescription> {
    partitioned
        .extensions
        .as_ref()
        .and_then(|extension| extension.downcast_ref::<PersistedFileDescription>())
        .copied()
        .ok_or_else(|| {
            binding_error("persisted scan file is missing its typed snapshot description")
        })
}

fn file_meta_description(file_meta: &FileMeta) -> Result<PersistedFileDescription> {
    file_meta
        .extensions
        .as_ref()
        .and_then(|extension| extension.downcast_ref::<PersistedFileDescription>())
        .copied()
        .ok_or_else(|| {
            binding_error("persisted reader request is missing its typed snapshot description")
        })
}

fn validate_reader_file_meta(file_meta: &FileMeta) -> Result<PersistedFileDescription> {
    let description = file_meta_description(file_meta)?;
    let expected = description.into_partitioned_file();
    if file_meta.object_meta != expected.object_meta {
        return Err(binding_error(format!(
            "persisted reader synthetic object metadata mismatch file_id=[{}]",
            description.file_id().as_u128()
        )));
    }
    if file_meta.range.is_some() || file_meta.metadata_size_hint.is_some() {
        return Err(binding_error(format!(
            "persisted reader request contains unsupported physical metadata file_id=[{}]",
            description.file_id().as_u128()
        )));
    }
    Ok(description)
}

fn validate_bound_tree(
    plan: &Arc<dyn ExecutionPlan>,
    expected_tables: &[PersistedTableDescription],
    bound_factory: &Arc<dyn ParquetFileReaderFactory>,
) -> Result<()> {
    if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
        if has_factory(parquet, &PLANNING_READER_FACTORY) {
            return Err(binding_error(
                "persisted physical plan retained a planning-only reader marker",
            ));
        }
        let has_bound_factory = has_factory(parquet, bound_factory);
        if is_reserved_zippy_scan(parquet) {
            if !has_bound_factory {
                return Err(binding_error(
                    "persisted physical plan retained an unbound reader factory",
                ));
            }
            validate_planning_scan(parquet, expected_tables)?;
        } else if has_bound_factory {
            return Err(binding_error(
                "snapshot-bound reader factory escaped onto a non-Zippy Parquet scan",
            ));
        }
    }
    for child in plan.children() {
        validate_bound_tree(child, expected_tables, bound_factory)?;
    }
    Ok(())
}

fn has_factory(parquet: &ParquetExec, expected: &Arc<dyn ParquetFileReaderFactory>) -> bool {
    parquet
        .parquet_file_reader_factory()
        .is_some_and(|actual| Arc::ptr_eq(actual, expected))
}

fn is_reserved_zippy_scan(parquet: &ParquetExec) -> bool {
    parquet
        .base_config()
        .object_store_url
        .as_str()
        .starts_with("zippy-query://")
        || parquet
            .base_config()
            .file_groups
            .iter()
            .flatten()
            .any(|file| {
                file.extensions.as_ref().is_some_and(|extension| {
                    extension
                        .downcast_ref::<PersistedFileDescription>()
                        .is_some()
                })
            })
}

pub(crate) fn is_exact_planning_parquet(parquet: &ParquetExec) -> bool {
    has_factory(parquet, &PLANNING_READER_FACTORY) && is_reserved_zippy_scan(parquet)
}

fn binding_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

struct BoundParquetReaderFactory {
    opener: Weak<dyn PersistedFileOpener>,
}

impl fmt::Debug for BoundParquetReaderFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundParquetReaderFactory")
            .finish_non_exhaustive()
    }
}

impl ParquetFileReaderFactory for BoundParquetReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<BoxedAsyncFileReader> {
        if partition_index != 0 {
            return Err(binding_error(format!(
                "persisted reader partition ordinal mismatch expected=[0] actual=[{partition_index}]"
            )));
        }
        let description = validate_reader_file_meta(&file_meta)?;
        let opener = self
            .opener
            .upgrade()
            .ok_or_else(|| DataFusionError::Execution(EXPIRED_EXECUTION_LEASE_ERROR.to_owned()))?;
        let expected_file = opener
            .snapshot_description()
            .table(description.table_index())
            .and_then(|table| table.files().get(description.file_index()))
            .copied()
            .ok_or_else(|| {
                binding_error("persisted reader request is absent from the opener snapshot")
            })?;
        if expected_file != description {
            return Err(binding_error(
                "persisted reader request differs from the opener snapshot",
            ));
        }
        opener
            .open_reader(PersistedFileOpenRequest::new(description, partition_index))
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

/// A planning-only DataFusion table over one capability-free persisted manifest.
///
/// Planning produces a [`ParquetExec`] whose file metadata uses synthetic keys and typed pure
/// extensions. Its sealed marker reader factory always rejects execution. A later server-owned
/// binding stage must replace that factory with one backed by a live query execution lease.
#[derive(Debug)]
pub struct ZippyTableProvider {
    table: PersistedTableDescription,
}

impl ZippyTableProvider {
    /// Creates a provider that can plan, but cannot execute, the persisted manifest.
    #[must_use]
    pub fn new(table: PersistedTableDescription) -> Self {
        Self { table }
    }

    /// Returns the capability-free persisted manifest.
    #[must_use]
    pub const fn description(&self) -> &PersistedTableDescription {
        &self.table
    }
}

impl TableProvider for ZippyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            if let Some(projection) = projection {
                for &index in projection {
                    if index >= self.table.schema.fields().len() {
                        return Err(DataFusionError::Plan(format!(
                            "persisted table projection index is out of bounds index=[{index}]"
                        )));
                    }
                }
            }

            if self.table.files.is_empty() {
                let projected_schema = projection.map_or_else(
                    || Ok(Arc::clone(&self.table.schema)),
                    |projection| {
                        self.table
                            .schema
                            .project(projection)
                            .map(Arc::new)
                            .map_err(DataFusionError::from)
                    },
                )?;
                return Ok(Arc::new(EmptyExec::new(projected_schema)) as Arc<dyn ExecutionPlan>);
            }

            let files = self
                .table
                .files
                .iter()
                .copied()
                .map(PersistedFileDescription::into_partitioned_file)
                .collect::<Vec<_>>();
            // One group keeps the scan in one partition. DataFusion may pre-open its next file,
            // so the server's file-task admission remains the physical concurrency boundary.
            let file_groups = vec![files];
            let object_store_url = ObjectStoreUrl::parse(PERSISTED_OBJECT_STORE_URL)?;
            // An inexact predicate must be evaluated by an exact FilterExec before LIMIT. Pushing
            // the limit into ParquetExec here could return too few matching rows.
            let scan_limit = if filters.is_empty() { limit } else { None };
            let file_scan_config =
                FileScanConfig::new(object_store_url, Arc::clone(&self.table.schema))
                    .with_file_groups(file_groups)
                    .with_statistics(self.table.statistics())
                    .with_projection(projection.cloned())
                    .with_limit(scan_limit);
            let mut builder = ParquetExec::builder(file_scan_config)
                .with_parquet_file_reader_factory(Arc::clone(&PLANNING_READER_FACTORY));
            if let Some(predicate) = conjunction(filters.iter().cloned()) {
                let table_schema = DFSchema::try_from(self.table.schema.as_ref().clone())?;
                builder =
                    builder.with_predicate(state.create_physical_expr(predicate, &table_schema)?);
            }
            let plan: Arc<dyn ExecutionPlan> = Arc::new(
                builder
                    .build()
                    .with_enable_page_index(false)
                    .with_bloom_filter_on_read(false),
            );
            Ok(plan)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[derive(Debug)]
struct PlanningOnlyParquetReaderFactory;

impl ParquetFileReaderFactory for PlanningOnlyParquetReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        _file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        Err(DataFusionError::Execution(
            PLANNING_ONLY_READER_ERROR.to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::{Arc, Mutex};

    use super::{
        bind_persisted_snapshot_plan as bind_validated_snapshot_plan, has_factory,
        PersistedFileDescription, PersistedFileKey, PersistedFileOpenRequest, PersistedFileOpener,
        PersistedSnapshotDescription, PersistedTableDescription, PlanningOnlyParquetReaderFactory,
        ZippyTableProvider, PERSISTED_OBJECT_STORE_URL, PLANNING_ONLY_READER_ERROR,
        PLANNING_READER_FACTORY,
    };
    use crate::{BoxedAsyncFileReader, QuerySessionPolicy};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::{FileRange, PartitionedFile};
    use datafusion::datasource::physical_plan::parquet::{ParquetExec, ParquetFileReaderFactory};
    use datafusion::datasource::physical_plan::{FileMeta, FileScanConfig};
    use datafusion::datasource::TableProvider;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{collect, ExecutionPlan};
    use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;
    use zippy_types::{ErrorCode, FileId, ServerError, TableId};

    struct RecordingOpener {
        snapshot: PersistedSnapshotDescription,
        requests: Arc<Mutex<Vec<PersistedFileOpenRequest>>>,
    }

    impl PersistedFileOpener for RecordingOpener {
        fn snapshot_description(&self) -> &PersistedSnapshotDescription {
            &self.snapshot
        }

        fn open_reader(
            &self,
            request: PersistedFileOpenRequest,
        ) -> zippy_types::Result<BoxedAsyncFileReader> {
            self.requests
                .lock()
                .expect("recording opener lock")
                .push(request);
            Err(ServerError::new(
                ErrorCode::Internal,
                "test opener intentionally has no physical reader",
            ))
        }
    }

    fn nonzero(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).expect("test value must be nonzero")
    }

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).expect("test table ID must be nonzero")
    }

    fn file_id(value: u128) -> FileId {
        FileId::from_u128(value).expect("test file ID must be nonzero")
    }

    fn file(
        table_index: usize,
        file_index: usize,
        table_id: TableId,
        snapshot_version: u64,
        id: u128,
    ) -> PersistedFileDescription {
        PersistedFileDescription::try_new(
            PersistedFileKey::new(table_index, file_index, table_id, file_id(id)),
            nonzero(snapshot_version),
            nonzero(2),
            nonzero(128),
            nonzero(3),
        )
        .expect("build test file description")
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("volume", DataType::Int64, false),
        ]))
    }

    fn table(
        table_index: usize,
        id: u128,
        snapshot_version: u64,
        file_ids: &[u128],
    ) -> PersistedTableDescription {
        let table_id = table_id(id);
        PersistedTableDescription::try_new(
            table_index,
            table_id,
            nonzero(snapshot_version),
            nonzero(2),
            schema(),
            file_ids
                .iter()
                .enumerate()
                .map(|(file_index, &id)| {
                    file(table_index, file_index, table_id, snapshot_version, id)
                })
                .collect(),
        )
        .expect("build persisted test table")
    }

    fn recording_opener(
        tables: &[PersistedTableDescription],
    ) -> (
        Arc<dyn PersistedFileOpener>,
        Arc<Mutex<Vec<PersistedFileOpenRequest>>>,
    ) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let opener: Arc<dyn PersistedFileOpener> = Arc::new(RecordingOpener {
            snapshot: PersistedSnapshotDescription::try_new(tables.to_vec())
                .expect("build recording opener snapshot"),
            requests: Arc::clone(&requests),
        });
        (opener, requests)
    }

    fn bind_persisted_snapshot_plan(
        plan: Arc<dyn ExecutionPlan>,
        tables: &[PersistedTableDescription],
        opener: &Arc<dyn PersistedFileOpener>,
    ) -> datafusion::common::Result<super::BoundPersistedPlan> {
        let snapshot = PersistedSnapshotDescription::try_new(tables.to_vec())?;
        bind_validated_snapshot_plan(plan, &snapshot, opener)
    }

    fn planning_config(table: &PersistedTableDescription) -> FileScanConfig {
        let files = table
            .files()
            .iter()
            .copied()
            .map(PersistedFileDescription::into_partitioned_file)
            .collect();
        FileScanConfig::new(
            ObjectStoreUrl::parse(PERSISTED_OBJECT_STORE_URL)
                .expect("parse persisted object-store URL"),
            Arc::clone(table.schema()),
        )
        .with_file_groups(vec![files])
        .with_statistics(table.statistics())
    }

    fn marked_plan(config: FileScanConfig) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            ParquetExec::builder(config)
                .with_parquet_file_reader_factory(Arc::clone(&PLANNING_READER_FACTORY))
                .build()
                .with_enable_page_index(false)
                .with_bloom_filter_on_read(false),
        )
    }

    fn file_meta(partitioned: &PartitionedFile) -> FileMeta {
        FileMeta {
            object_meta: partitioned.object_meta.clone(),
            range: partitioned.range.clone(),
            extensions: partitioned.extensions.clone(),
            metadata_size_hint: partitioned.metadata_size_hint,
        }
    }

    fn reader_error(
        factory: &Arc<dyn ParquetFileReaderFactory>,
        partition_index: usize,
        file_meta: FileMeta,
    ) -> String {
        match factory.create_reader(
            partition_index,
            file_meta,
            None,
            &ExecutionPlanMetricsSet::new(),
        ) {
            Ok(_) => panic!("reader creation must fail in this test"),
            Err(error) => error.to_string(),
        }
    }

    fn find_plan<T: 'static>(plan: &Arc<dyn ExecutionPlan>) -> Option<&T> {
        if let Some(found) = plan.as_any().downcast_ref::<T>() {
            return Some(found);
        }
        plan.children()
            .into_iter()
            .find_map(|child| find_plan::<T>(child))
    }

    fn has_filter_ancestor_of_parquet(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.as_any().is::<FilterExec>()
            && plan
                .children()
                .into_iter()
                .any(|child| find_plan::<ParquetExec>(child).is_some())
        {
            return true;
        }
        plan.children()
            .into_iter()
            .any(has_filter_ancestor_of_parquet)
    }

    #[test]
    fn persisted_manifest_rejects_cross_snapshot_and_duplicate_files() {
        let other_table_id = table_id(2);
        let table_id = table_id(1);
        let first = file(0, 0, table_id, 7, 10);
        let wrong_snapshot = file(0, 1, table_id, 8, 11);
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![first, wrong_snapshot],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![first, file(0, 1, table_id, 7, 10)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![file(1, 0, table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![file(0, 1, table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![file(0, 0, other_table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(1),
            schema(),
            vec![file(0, 0, table_id, 7, 12)],
        )
        .is_err());
    }

    #[test]
    fn snapshot_description_validates_global_identities_once() {
        assert!(PersistedSnapshotDescription::try_new(Vec::new()).is_err());

        let first = table(0, 1, 7, &[10]);
        let duplicate_table = table(1, 1, 7, &[20]);
        assert!(
            PersistedSnapshotDescription::try_new(vec![first.clone(), duplicate_table,]).is_err()
        );

        let duplicate_file = table(1, 2, 7, &[10]);
        assert!(
            PersistedSnapshotDescription::try_new(vec![first.clone(), duplicate_file,]).is_err()
        );

        let noncontiguous = table(2, 3, 7, &[30]);
        assert!(PersistedSnapshotDescription::try_new(vec![first.clone(), noncontiguous]).is_err());

        let second = table(1, 2, 7, &[20, 21]);
        let snapshot = PersistedSnapshotDescription::try_new(vec![first, second])
            .expect("build globally valid snapshot description");
        assert_eq!(snapshot.table_count(), 2);
        assert_eq!(snapshot.total_file_count(), 3);
        assert_eq!(snapshot.max_files_per_table(), 2);
        assert_eq!(snapshot.tables()[1].table_id(), table_id(2));
    }

    #[test]
    fn snapshot_description_accepts_maximum_expected_file_scale_linearly() {
        let file_ids = (1_u128..=4096).collect::<Vec<_>>();
        let snapshot = PersistedSnapshotDescription::try_new(vec![table(0, 1, 7, &file_ids)])
            .expect("validate 4096-file snapshot");
        assert_eq!(snapshot.total_file_count(), 4096);
    }

    #[test]
    fn binder_rejects_an_opener_for_another_snapshot_synchronously() {
        let expected = table(0, 1, 7, &[10]);
        let foreign = table(0, 2, 8, &[20]);
        let expected_snapshot =
            PersistedSnapshotDescription::try_new(vec![expected.clone()]).unwrap();
        let (foreign_opener, requests) = recording_opener(&[foreign]);
        let error = bind_validated_snapshot_plan(
            marked_plan(planning_config(&expected)),
            &expected_snapshot,
            &foreign_opener,
        )
        .expect_err("foreign snapshot opener must fail during binding");
        assert!(error.to_string().contains("another snapshot"));
        assert!(requests.lock().expect("request log lock").is_empty());
    }

    #[tokio::test]
    async fn provider_validates_projection_and_pushes_only_unfiltered_limit() {
        let table_id = table_id(1);
        let table = PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![file(0, 0, table_id, 7, 10)],
        )
        .expect("build projection test table");
        let provider = ZippyTableProvider::new(table);
        let context =
            QuerySessionPolicy::new(std::num::NonZeroUsize::new(1).unwrap()).create_context();
        let state = context.state();

        let zero_column_projection = Vec::new();
        let plan = provider
            .scan(&state, Some(&zero_column_projection), &[], Some(1))
            .await
            .expect("plan zero-column scan");
        let parquet = plan
            .as_any()
            .downcast_ref::<ParquetExec>()
            .expect("direct provider scan is ParquetExec");
        assert_eq!(parquet.base_config().projection.as_deref(), Some(&[][..]));
        assert_eq!(parquet.base_config().limit, Some(1));

        let invalid_projection = vec![3];
        assert!(provider
            .scan(&state, Some(&invalid_projection), &[], None)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn provider_plans_projected_inexact_filter_without_opening_a_reader() {
        let table_id = table_id(1);
        let first = file(0, 0, table_id, 7, 10);
        let second = file(0, 1, table_id, 7, 11);
        let table = PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            vec![first, second],
        )
        .expect("build test table description");
        assert_eq!(
            table.retained_schema_bytes(),
            conservative_arrow_schema_retained_bytes(table.schema().as_ref()).unwrap()
        );
        let provider = Arc::new(ZippyTableProvider::new(table));
        let context =
            QuerySessionPolicy::new(std::num::NonZeroUsize::new(2).unwrap()).create_context();
        context
            .register_table("ticks", provider.clone())
            .expect("register persisted table");

        let dataframe = context
            .sql("select price from ticks where volume >= 10 limit 1")
            .await
            .expect("plan persisted query");
        let plan = dataframe
            .create_physical_plan()
            .await
            .expect("create persisted physical plan");

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "price");
        assert!(has_filter_ancestor_of_parquet(&plan));
        let parquet = find_plan::<ParquetExec>(&plan).expect("physical plan contains ParquetExec");
        assert!(parquet.predicate().is_some());
        assert!(!parquet.table_parquet_options().global.enable_page_index);
        assert!(!parquet.table_parquet_options().global.bloom_filter_on_read);
        assert_eq!(
            parquet.base_config().object_store_url.as_str(),
            PERSISTED_OBJECT_STORE_URL.to_owned() + "/"
        );
        assert_eq!(parquet.base_config().file_groups.len(), 1);
        assert_eq!(parquet.base_config().file_groups[0].len(), 2);
        assert_eq!(
            parquet.base_config().projection.as_deref(),
            Some(&[1, 2][..])
        );
        assert_eq!(parquet.base_config().limit, None);
        for (partitioned, expected) in parquet.base_config().file_groups[0]
            .iter()
            .zip([first, second])
        {
            let location = partitioned.object_meta.location.as_ref();
            assert_eq!(location, expected.synthetic_object_key());
            assert!(!location.contains('/'));
            assert!(!location.contains('\\'));
            assert!(!location.contains(".parquet"));
            assert_eq!(partitioned.object_meta.size, 128);
            let extension = partitioned
                .extensions
                .as_ref()
                .expect("typed persisted file extension")
                .downcast_ref::<PersistedFileDescription>()
                .expect("extension remains a pure file description");
            assert_eq!(*extension, expected);
        }
        let planning_plan = Arc::clone(&plan);
        let (opener, _) = recording_opener(std::slice::from_ref(provider.description()));
        let bound = bind_persisted_snapshot_plan(
            plan,
            std::slice::from_ref(provider.description()),
            &opener,
        )
        .expect("bind nested persisted scan");
        assert_eq!(bound.bound_scan_count(), 1);
        assert!(has_filter_ancestor_of_parquet(bound.execution_plan()));
        let bound_parquet = find_plan::<ParquetExec>(bound.execution_plan())
            .expect("bound physical plan contains ParquetExec");
        assert!(!has_factory(bound_parquet, &PLANNING_READER_FACTORY));
        assert!(
            !bound_parquet
                .table_parquet_options()
                .global
                .enable_page_index
        );
        assert!(
            !bound_parquet
                .table_parquet_options()
                .global
                .bloom_filter_on_read
        );

        let error = collect(planning_plan, context.task_ctx())
            .await
            .expect_err("planning-only marker factory rejects execution");
        assert!(error.to_string().contains(PLANNING_ONLY_READER_ERROR));
    }

    #[tokio::test]
    async fn empty_persisted_table_executes_without_a_reader_capability() {
        let table = PersistedTableDescription::try_new(
            0,
            table_id(1),
            nonzero(1),
            nonzero(1),
            schema(),
            Vec::new(),
        )
        .expect("build empty persisted table description");
        let provider = Arc::new(ZippyTableProvider::new(table));
        let context =
            QuerySessionPolicy::new(std::num::NonZeroUsize::new(1).unwrap()).create_context();
        context
            .register_table("ticks", provider.clone())
            .expect("register empty persisted table");

        let batches = context
            .sql("select price from ticks where volume >= 10")
            .await
            .expect("plan empty persisted query")
            .collect()
            .await
            .expect("execute empty persisted query");

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn binder_accepts_distinct_self_join_scans_and_zero_scan_plans() {
        let other_table = table(1, 2, 8, &[20]);
        let table = table(0, 1, 7, &[10, 11]);
        let provider = Arc::new(ZippyTableProvider::new(table.clone()));
        let context =
            QuerySessionPolicy::new(std::num::NonZeroUsize::new(2).unwrap()).create_context();
        context
            .register_table("ticks", provider)
            .expect("register persisted table");
        let plan = context
            .sql("select a.price from ticks a join ticks b on a.symbol = b.symbol")
            .await
            .expect("plan persisted self join")
            .create_physical_plan()
            .await
            .expect("create persisted self-join physical plan");
        let (opener, _) = recording_opener(std::slice::from_ref(&table));
        let bound = bind_persisted_snapshot_plan(plan, std::slice::from_ref(&table), &opener)
            .expect("bind distinct self-join scans");
        assert_eq!(bound.bound_scan_count(), 2);

        let eliminated: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema()));
        let bound = bind_persisted_snapshot_plan(eliminated, std::slice::from_ref(&table), &opener)
            .expect("bind optimizer-eliminated scan plan");
        assert_eq!(bound.bound_scan_count(), 0);

        context
            .register_table(
                "quotes",
                Arc::new(ZippyTableProvider::new(other_table.clone())),
            )
            .expect("register second persisted table");
        let plan = context
            .sql("select t.price from ticks t join quotes q on t.symbol = q.symbol")
            .await
            .expect("plan persisted multi-table join")
            .create_physical_plan()
            .await
            .expect("create persisted multi-table physical plan");
        let expected_tables = [table, other_table];
        let (multi_table_opener, _) = recording_opener(&expected_tables);
        let bound = bind_persisted_snapshot_plan(plan, &expected_tables, &multi_table_opener)
            .expect("bind different persisted table ordinals");
        assert_eq!(bound.bound_scan_count(), 2);
    }

    #[test]
    fn binder_rejects_missing_foreign_reused_and_already_bound_markers() {
        let table = table(0, 1, 7, &[10]);
        let (opener, _) = recording_opener(std::slice::from_ref(&table));

        let missing: Arc<dyn ExecutionPlan> =
            ParquetExec::builder(planning_config(&table)).build_arc();
        assert!(
            bind_persisted_snapshot_plan(missing, std::slice::from_ref(&table), &opener,).is_err()
        );

        let foreign_factory: Arc<dyn ParquetFileReaderFactory> =
            Arc::new(PlanningOnlyParquetReaderFactory);
        let foreign: Arc<dyn ExecutionPlan> = ParquetExec::builder(planning_config(&table))
            .with_parquet_file_reader_factory(foreign_factory)
            .build_arc();
        assert!(
            bind_persisted_snapshot_plan(foreign, std::slice::from_ref(&table), &opener,).is_err()
        );

        let shared = marked_plan(planning_config(&table));
        let reused: Arc<dyn ExecutionPlan> = Arc::new(UnionExec::new(vec![
            Arc::clone(&shared),
            Arc::clone(&shared),
        ]));
        assert!(
            bind_persisted_snapshot_plan(reused, std::slice::from_ref(&table), &opener,).is_err()
        );

        let bound = bind_persisted_snapshot_plan(
            marked_plan(planning_config(&table)),
            std::slice::from_ref(&table),
            &opener,
        )
        .expect("first binding succeeds");
        assert!(bind_persisted_snapshot_plan(
            Arc::clone(bound.execution_plan()),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let ordinary_config = FileScanConfig::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::clone(table.schema()),
        )
        .with_file_groups(vec![vec![PartitionedFile::new("ordinary.parquet", 128)]]);
        let ordinary: Arc<dyn ExecutionPlan> = ParquetExec::builder(ordinary_config).build_arc();
        let ordinary =
            bind_persisted_snapshot_plan(ordinary, std::slice::from_ref(&table), &opener)
                .expect("ordinary Parquet scan remains outside persisted binding");
        assert_eq!(ordinary.bound_scan_count(), 0);
    }

    #[test]
    fn binder_rejects_tampered_manifest_metadata_before_opening() {
        let table = table(0, 1, 7, &[10, 11]);
        let (opener, requests) = recording_opener(std::slice::from_ref(&table));

        let page_index_plan: Arc<dyn ExecutionPlan> = Arc::new(
            ParquetExec::builder(planning_config(&table))
                .with_parquet_file_reader_factory(Arc::clone(&PLANNING_READER_FACTORY))
                .build()
                .with_enable_page_index(true)
                .with_bloom_filter_on_read(false),
        );
        assert!(bind_persisted_snapshot_plan(
            page_index_plan,
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let bloom_plan: Arc<dyn ExecutionPlan> = Arc::new(
            ParquetExec::builder(planning_config(&table))
                .with_parquet_file_reader_factory(Arc::clone(&PLANNING_READER_FACTORY))
                .build()
                .with_enable_page_index(false)
                .with_bloom_filter_on_read(true),
        );
        assert!(
            bind_persisted_snapshot_plan(bloom_plan, std::slice::from_ref(&table), &opener,)
                .is_err()
        );

        let mut config = planning_config(&table);
        config.object_store_url = ObjectStoreUrl::local_filesystem();
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0].object_meta.location = "wrong-object-key".into();
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0].object_meta.size += 1;
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0].range = Some(FileRange { start: 0, end: 1 });
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0].metadata_size_hint = Some(64);
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0]
            .partition_values
            .push(datafusion::common::ScalarValue::Utf8(Some(
                "unexpected".to_owned(),
            )));
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0][0].extensions = None;
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let expected = table.files()[0];
        let mut config = planning_config(&table);
        let wrong_snapshot = PersistedFileDescription::try_new(
            expected.key(),
            nonzero(expected.snapshot_version().get() + 1),
            expected.schema_version(),
            expected.file_size_bytes(),
            expected.row_count(),
        )
        .expect("build cross-snapshot description");
        config.file_groups[0][0] = wrong_snapshot.into_partitioned_file();
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        let wrong_schema = PersistedFileDescription::try_new(
            expected.key(),
            expected.snapshot_version(),
            nonzero(1),
            expected.file_size_bytes(),
            expected.row_count(),
        )
        .expect("build cross-schema description");
        config.file_groups[0][0] = wrong_schema.into_partitioned_file();
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0].swap(0, 1);
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_groups[0].pop();
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.statistics = datafusion::common::Statistics::new_unknown(table.schema());
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        let mut config = planning_config(&table);
        config.file_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        assert!(bind_persisted_snapshot_plan(
            marked_plan(config),
            std::slice::from_ref(&table),
            &opener,
        )
        .is_err());

        assert!(requests.lock().expect("request log lock").is_empty());
    }

    #[test]
    fn bound_factory_revalidates_file_meta_and_expires_with_opener() {
        let table = table(0, 1, 7, &[10]);
        let (opener, requests) = recording_opener(std::slice::from_ref(&table));
        let bound = bind_persisted_snapshot_plan(
            marked_plan(planning_config(&table)),
            std::slice::from_ref(&table),
            &opener,
        )
        .expect("bind persisted scan");
        let parquet = find_plan::<ParquetExec>(bound.execution_plan())
            .expect("bound plan contains ParquetExec");
        let factory = parquet
            .parquet_file_reader_factory()
            .expect("bound reader factory");
        let partitioned = &parquet.base_config().file_groups[0][0];

        let error = reader_error(factory, 0, file_meta(partitioned));
        assert!(error.contains("test opener intentionally"));
        assert_eq!(requests.lock().expect("request log lock").len(), 1);

        let mut wrong_size = file_meta(partitioned);
        wrong_size.object_meta.size += 1;
        let error = reader_error(factory, 0, wrong_size);
        assert!(error.contains("object metadata mismatch"));

        let mut ranged = file_meta(partitioned);
        ranged.range = Some(FileRange { start: 0, end: 1 });
        let error = reader_error(factory, 0, ranged);
        assert!(error.contains("unsupported physical metadata"));

        let error = reader_error(factory, 1, file_meta(partitioned));
        assert!(error.contains("partition ordinal mismatch"));
        assert_eq!(requests.lock().expect("request log lock").len(), 1);

        drop(opener);
        let error = reader_error(factory, 0, file_meta(partitioned));
        assert!(error.contains("execution lease has expired"));
        assert_eq!(requests.lock().expect("request log lock").len(), 1);
    }
}
