//! Capability-free active-segment scans and deterministic persisted/live concatenation.

use std::any::Any;
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use zippy_types::table::SourceSegmentIdentity;
use zippy_types::{OffsetRange, TableId};

use crate::PersistedSnapshotDescription;

const PLANNING_ACTIVE_EXECUTION_ERROR: &str =
    "planning-only Zippy active segment scan has no snapshot-bound opener";
const EXPIRED_ACTIVE_EXECUTION_ERROR: &str =
    "snapshot-bound Zippy active segment execution lease has expired";

/// Role of one capability-free live segment description.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActiveSegmentRole {
    /// A sealed source segment whose rows are not fully represented by persisted files.
    RetainedSealed,
    /// The fixed committed prefix of the current append segment.
    Active,
}

/// Stable, path-free identity of one segment in exact query scan order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActiveSegmentKey {
    table_index: usize,
    segment_index: usize,
    table_id: TableId,
    source: SourceSegmentIdentity,
}

impl ActiveSegmentKey {
    /// Creates one deterministic active-segment key.
    #[must_use]
    pub const fn new(
        table_index: usize,
        segment_index: usize,
        table_id: TableId,
        source: SourceSegmentIdentity,
    ) -> Self {
        Self {
            table_index,
            segment_index,
            table_id,
            source,
        }
    }

    /// Returns the canonical table ordinal.
    #[must_use]
    pub const fn table_index(self) -> usize {
        self.table_index
    }

    /// Returns the deterministic live scan ordinal.
    #[must_use]
    pub const fn segment_index(self) -> usize {
        self.segment_index
    }

    /// Returns the owning table identity.
    #[must_use]
    pub const fn table_id(self) -> TableId {
        self.table_id
    }

    /// Returns the producer-fenced source identity.
    #[must_use]
    pub const fn source(self) -> SourceSegmentIdentity {
        self.source
    }
}

/// Immutable, capability-free description of a snapshot-pinned shared-memory row span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActiveSegmentDescription {
    key: ActiveSegmentKey,
    role: ActiveSegmentRole,
    snapshot_version: NonZeroU64,
    schema_version: NonZeroU64,
    offsets: OffsetRange,
    row_count: NonZeroU64,
}

impl ActiveSegmentDescription {
    /// Creates a path-free description and validates its logical row count.
    pub fn try_new(
        key: ActiveSegmentKey,
        role: ActiveSegmentRole,
        snapshot_version: NonZeroU64,
        schema_version: NonZeroU64,
        offsets: OffsetRange,
        row_count: NonZeroU64,
    ) -> Result<Self> {
        if u128::from(row_count.get()) != offsets.len() {
            return Err(DataFusionError::Plan(
                "active segment row count differs from its logical offset range".to_owned(),
            ));
        }
        usize::try_from(row_count.get()).map_err(|_| {
            DataFusionError::Plan("active segment row count does not fit this platform".to_owned())
        })?;
        Ok(Self {
            key,
            role,
            snapshot_version,
            schema_version,
            offsets,
            row_count,
        })
    }

    /// Returns the complete stable key.
    #[must_use]
    pub const fn key(self) -> ActiveSegmentKey {
        self.key
    }

    /// Returns the canonical table ordinal.
    #[must_use]
    pub const fn table_index(self) -> usize {
        self.key.table_index
    }

    /// Returns the live segment scan ordinal.
    #[must_use]
    pub const fn segment_index(self) -> usize {
        self.key.segment_index
    }

    /// Returns the owning table identity.
    #[must_use]
    pub const fn table_id(self) -> TableId {
        self.key.table_id
    }

    /// Returns the producer-fenced source identity.
    #[must_use]
    pub const fn source(self) -> SourceSegmentIdentity {
        self.key.source
    }

    /// Returns whether the span is retained sealed data or the active prefix.
    #[must_use]
    pub const fn role(self) -> ActiveSegmentRole {
        self.role
    }

    /// Returns the exact persisted boundary coordinated with this live span.
    #[must_use]
    pub const fn snapshot_version(self) -> NonZeroU64 {
        self.snapshot_version
    }

    /// Returns the exact schema used to decode this span.
    #[must_use]
    pub const fn schema_version(self) -> NonZeroU64 {
        self.schema_version
    }

    /// Returns the contiguous logical offsets represented by this span.
    #[must_use]
    pub const fn offsets(self) -> OffsetRange {
        self.offsets
    }

    /// Returns the fixed number of rows visible to the query snapshot.
    #[must_use]
    pub const fn row_count(self) -> NonZeroU64 {
        self.row_count
    }
}

/// Pure request passed from a bound active scan to the server-owned opener.
#[derive(Debug, Clone)]
pub struct ActiveSegmentOpenRequest {
    descriptions: Arc<[ActiveSegmentDescription]>,
    projection: Option<Arc<[usize]>>,
    batch_size: NonZeroUsize,
}

impl ActiveSegmentOpenRequest {
    /// Returns the exact ordered segment descriptions.
    #[must_use]
    pub fn descriptions(&self) -> &[ActiveSegmentDescription] {
        &self.descriptions
    }

    /// Returns projected table-schema column ordinals, when projection was pushed down.
    #[must_use]
    pub fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    /// Returns the maximum rows requested per produced Arrow batch.
    #[must_use]
    pub const fn batch_size(&self) -> NonZeroUsize {
        self.batch_size
    }
}

/// Server-owned bridge from validated active metadata to a capability-bearing batch stream.
pub trait ActiveSegmentOpener: Send + Sync + 'static {
    /// Returns the exact snapshot this opener may resolve.
    fn snapshot_description(&self) -> &PersistedSnapshotDescription;

    /// Opens an incremental stream while retaining all task and snapshot leases it requires.
    fn open_segments(
        &self,
        request: ActiveSegmentOpenRequest,
    ) -> zippy_types::Result<SendableRecordBatchStream>;
}

enum ActiveScanAuthority {
    Planning,
    Bound(Weak<dyn ActiveSegmentOpener>),
}

impl fmt::Debug for ActiveScanAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planning => formatter.write_str("Planning"),
            Self::Bound(_) => formatter.write_str("Bound(Weak)"),
        }
    }
}

/// One planning-only or snapshot-bound active segment scan.
#[derive(Debug)]
pub struct ActiveSegmentExec {
    descriptions: Arc<[ActiveSegmentDescription]>,
    table_schema: SchemaRef,
    projection: Option<Arc<[usize]>>,
    projected_schema: SchemaRef,
    batch_size: NonZeroUsize,
    authority: ActiveScanAuthority,
    properties: PlanProperties,
}

impl ActiveSegmentExec {
    pub(crate) fn try_planning(
        descriptions: Arc<[ActiveSegmentDescription]>,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: NonZeroUsize,
    ) -> Result<Self> {
        if descriptions.is_empty() {
            return Err(DataFusionError::Plan(
                "active segment scan requires at least one description".to_owned(),
            ));
        }
        let projection = projection.map(Arc::<[usize]>::from);
        let projected_schema = project_schema(&table_schema, projection.as_deref())?;
        let properties = active_properties(Arc::clone(&projected_schema));
        Ok(Self {
            descriptions,
            table_schema,
            projection,
            projected_schema,
            batch_size,
            authority: ActiveScanAuthority::Planning,
            properties,
        })
    }

    pub(crate) fn bind(&self, opener: Weak<dyn ActiveSegmentOpener>) -> Self {
        Self {
            descriptions: Arc::clone(&self.descriptions),
            table_schema: Arc::clone(&self.table_schema),
            projection: self.projection.as_ref().map(Arc::clone),
            projected_schema: Arc::clone(&self.projected_schema),
            batch_size: self.batch_size,
            authority: ActiveScanAuthority::Bound(opener),
            properties: self.properties.clone(),
        }
    }

    pub(crate) fn is_planning(&self) -> bool {
        matches!(self.authority, ActiveScanAuthority::Planning)
    }

    pub(crate) fn is_bound(&self) -> bool {
        matches!(self.authority, ActiveScanAuthority::Bound(_))
    }

    pub(crate) fn descriptions(&self) -> &[ActiveSegmentDescription] {
        &self.descriptions
    }

    pub(crate) fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }

    pub(crate) const fn batch_size(&self) -> NonZeroUsize {
        self.batch_size
    }
}

impl DisplayAs for ActiveSegmentExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActiveSegmentExec: segments={}, projection={:?}",
            self.descriptions.len(),
            self.projection
        )
    }
}

impl ExecutionPlan for ActiveSegmentExec {
    fn name(&self) -> &'static str {
        "ActiveSegmentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ActiveSegmentExec cannot accept children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "ActiveSegmentExec partition is out of bounds partition=[{partition}]"
            )));
        }
        let opener = match &self.authority {
            ActiveScanAuthority::Planning => {
                return Err(DataFusionError::Execution(
                    PLANNING_ACTIVE_EXECUTION_ERROR.to_owned(),
                ));
            }
            ActiveScanAuthority::Bound(opener) => opener.upgrade().ok_or_else(|| {
                DataFusionError::Execution(EXPIRED_ACTIVE_EXECUTION_ERROR.to_owned())
            })?,
        };
        opener
            .open_segments(ActiveSegmentOpenRequest {
                descriptions: Arc::clone(&self.descriptions),
                projection: self.projection.as_ref().map(Arc::clone),
                batch_size: self.batch_size,
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    fn statistics(&self) -> Result<Statistics> {
        let rows = self
            .descriptions
            .iter()
            .try_fold(0_usize, |total, description| {
                let rows = usize::try_from(description.row_count().get()).map_err(|_| {
                    DataFusionError::Plan("active segment rows do not fit usize".to_owned())
                })?;
                total.checked_add(rows).ok_or_else(|| {
                    DataFusionError::Plan("active segment total rows overflow usize".to_owned())
                })
            })?;
        Ok(Statistics {
            num_rows: Precision::Exact(rows),
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.projected_schema),
        })
    }
}

/// A single-partition, lazy concatenation of persisted rows followed by live rows.
#[derive(Debug)]
pub struct PersistedAndActiveExec {
    persisted: Arc<dyn ExecutionPlan>,
    active: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl PersistedAndActiveExec {
    pub(crate) fn try_new(
        persisted: Arc<dyn ExecutionPlan>,
        active: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        if persisted.schema() != active.schema() {
            return Err(DataFusionError::Plan(
                "persisted and active scan schemas differ".to_owned(),
            ));
        }
        if persisted
            .properties()
            .output_partitioning()
            .partition_count()
            != 1
            || active.properties().output_partitioning().partition_count() != 1
        {
            return Err(DataFusionError::Plan(
                "persisted and active scan inputs must each have one partition".to_owned(),
            ));
        }
        let properties = active_properties(persisted.schema());
        Ok(Self {
            persisted,
            active,
            properties,
        })
    }
}

impl DisplayAs for PersistedAndActiveExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("PersistedAndActiveExec")
    }
}

impl ExecutionPlan for PersistedAndActiveExec {
    fn name(&self) -> &'static str {
        "PersistedAndActiveExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.persisted, &self.active]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [persisted, active]: [Arc<dyn ExecutionPlan>; 2] =
            children.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "PersistedAndActiveExec requires exactly two children".to_owned(),
                )
            })?;
        Ok(Arc::new(Self::try_new(persisted, active)?))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "PersistedAndActiveExec partition is out of bounds partition=[{partition}]"
            )));
        }
        let persisted = self.persisted.execute(0, Arc::clone(&context))?;
        Ok(Box::pin(PersistedAndActiveStream {
            schema: self.schema(),
            persisted: Some(persisted),
            active_plan: Some(Arc::clone(&self.active)),
            active: None,
            context,
            finished: false,
        }))
    }

    fn statistics(&self) -> Result<Statistics> {
        let persisted = self.persisted.statistics()?;
        let active = self.active.statistics()?;
        let num_rows = match (persisted.num_rows, active.num_rows) {
            (Precision::Exact(left), Precision::Exact(right)) => left
                .checked_add(right)
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            _ => Precision::Absent,
        };
        Ok(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }
}

struct PersistedAndActiveStream {
    schema: SchemaRef,
    persisted: Option<SendableRecordBatchStream>,
    active_plan: Option<Arc<dyn ExecutionPlan>>,
    active: Option<SendableRecordBatchStream>,
    context: Arc<TaskContext>,
    finished: bool,
}

impl fmt::Debug for PersistedAndActiveStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedAndActiveStream")
            .field("reading_persisted", &self.persisted.is_some())
            .field("active_opened", &self.active.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}

impl Stream for PersistedAndActiveStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.finished {
                return Poll::Ready(None);
            }
            if let Some(persisted) = self.persisted.as_mut() {
                match persisted.as_mut().poll_next(context) {
                    Poll::Ready(None) => {
                        self.persisted = None;
                        let active_plan = self
                            .active_plan
                            .take()
                            .expect("active plan remains until persisted EOF");
                        match active_plan.execute(0, Arc::clone(&self.context)) {
                            Ok(active) => self.active = Some(active),
                            Err(error) => {
                                self.finished = true;
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                    }
                    other => return other,
                }
                continue;
            }
            if let Some(active) = self.active.as_mut() {
                match active.as_mut().poll_next(context) {
                    Poll::Ready(None) => {
                        self.active = None;
                        self.finished = true;
                        return Poll::Ready(None);
                    }
                    other => return other,
                }
            }
            self.finished = true;
            return Poll::Ready(None);
        }
    }
}

impl RecordBatchStream for PersistedAndActiveStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn project_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> Result<SchemaRef> {
    projection.map_or_else(
        || Ok(Arc::clone(schema)),
        |projection| schema.project(projection).map(Arc::new).map_err(Into::into),
    )
}

fn active_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    )
}
