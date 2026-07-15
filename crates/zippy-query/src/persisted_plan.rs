//! Capability-free persisted manifests and planning-only DataFusion integration.

use std::any::Any;
use std::collections::HashSet;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
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
use zippy_types::{FileId, TableId};

const PERSISTED_OBJECT_STORE_URL: &str = "zippy-query://persisted";
const PLANNING_ONLY_READER_ERROR: &str =
    "planning-only Zippy Parquet scan has no snapshot-bound reader factory";

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
#[derive(Debug, Clone)]
pub struct PersistedTableDescription {
    table_index: usize,
    table_id: TableId,
    snapshot_version: NonZeroU64,
    schema_version: NonZeroU64,
    schema: SchemaRef,
    retained_schema_bytes: NonZeroU64,
    files: Arc<[PersistedFileDescription]>,
    total_file_size_bytes: usize,
    total_rows: usize,
}

impl PersistedTableDescription {
    /// Creates a validated persisted table manifest from pure metadata.
    pub fn try_new(
        table_index: usize,
        table_id: TableId,
        snapshot_version: NonZeroU64,
        schema_version: NonZeroU64,
        schema: SchemaRef,
        retained_schema_bytes: NonZeroU64,
        files: Vec<PersistedFileDescription>,
    ) -> Result<Self> {
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

    /// Returns the Catalog-computed logical memory charge for retaining the decoded schema.
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

/// A planning-only DataFusion table over one capability-free persisted manifest.
///
/// Planning produces a [`ParquetExec`] whose file metadata uses synthetic keys and typed pure
/// extensions. Its sealed marker reader factory always rejects execution. A later server-owned
/// binding stage must replace that factory with one backed by a live query execution lease.
#[derive(Debug)]
pub struct ZippyTableProvider {
    table: PersistedTableDescription,
    planning_reader_factory: Arc<PlanningOnlyParquetReaderFactory>,
}

impl ZippyTableProvider {
    /// Creates a provider that can plan, but cannot execute, the persisted manifest.
    #[must_use]
    pub fn new(table: PersistedTableDescription) -> Self {
        Self {
            table,
            planning_reader_factory: Arc::new(PlanningOnlyParquetReaderFactory::default()),
        }
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
                .with_parquet_file_reader_factory(self.planning_reader_factory.clone());
            if let Some(predicate) = conjunction(filters.iter().cloned()) {
                let table_schema = DFSchema::try_from(self.table.schema.as_ref().clone())?;
                builder =
                    builder.with_predicate(state.create_physical_expr(predicate, &table_schema)?);
            }
            let plan: Arc<dyn ExecutionPlan> = builder.build_arc();
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

#[derive(Debug, Default)]
struct PlanningOnlyParquetReaderFactory {
    execution_attempts: AtomicUsize,
}

impl ParquetFileReaderFactory for PlanningOnlyParquetReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        _file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        self.execution_attempts.fetch_add(1, Ordering::Relaxed);
        Err(DataFusionError::Execution(
            PLANNING_ONLY_READER_ERROR.to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use super::{
        PersistedFileDescription, PersistedFileKey, PersistedTableDescription, ZippyTableProvider,
        PERSISTED_OBJECT_STORE_URL, PLANNING_ONLY_READER_ERROR,
    };
    use crate::QuerySessionPolicy;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::parquet::ParquetExec;
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::{collect, ExecutionPlan};
    use zippy_types::{FileId, TableId};

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
            nonzero(512),
            vec![first, wrong_snapshot],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            nonzero(512),
            vec![first, file(0, 1, table_id, 7, 10)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            nonzero(512),
            vec![file(1, 0, table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            nonzero(512),
            vec![file(0, 1, table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(2),
            schema(),
            nonzero(512),
            vec![file(0, 0, other_table_id, 7, 12)],
        )
        .is_err());
        assert!(PersistedTableDescription::try_new(
            0,
            table_id,
            nonzero(7),
            nonzero(1),
            schema(),
            nonzero(512),
            vec![file(0, 0, table_id, 7, 12)],
        )
        .is_err());
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
            nonzero(512),
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
            nonzero(512),
            vec![first, second],
        )
        .expect("build test table description");
        assert_eq!(table.retained_schema_bytes(), nonzero(512));
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
        assert_eq!(
            provider
                .planning_reader_factory
                .execution_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        let error = collect(plan, context.task_ctx())
            .await
            .expect_err("planning-only marker factory rejects execution");
        assert!(error.to_string().contains(PLANNING_ONLY_READER_ERROR));
        assert_eq!(
            provider
                .planning_reader_factory
                .execution_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn empty_persisted_table_executes_without_a_reader_capability() {
        let table = PersistedTableDescription::try_new(
            0,
            table_id(1),
            nonzero(1),
            nonzero(1),
            schema(),
            nonzero(512),
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
        assert_eq!(
            provider
                .planning_reader_factory
                .execution_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }
}
