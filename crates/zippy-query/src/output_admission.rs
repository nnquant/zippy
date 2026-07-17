//! Conservative pre-admission for the first bounded persisted output subset.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::physical_plan::parquet::ParquetExec;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NegativeExpr, NotExpr,
    TryCastExpr,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;

const OUTPUT_ADMISSION_FORMAT_VERSION: u32 = 1;
const ARROW_BATCH_FIXED_MEMORY_BYTES: u64 = 64 * 1024;
const ARROW_ARRAY_CONTROL_MEMORY_BYTES: u64 = 1024;
const PARQUET_DECODER_FIXED_MEMORY_BYTES: u64 = 256 * 1024;
const PARQUET_ROW_GROUP_DECODE_COPIES: u64 = 2;

/// A versioned memory envelope for one fixed-width persisted streaming scan.
///
/// The envelope is deliberately limited to the first native execution subset: one Parquet scan
/// (or an optimizer-eliminated empty scan), direct column projection, fixed-width predicates, and
/// limit/slice operators. It is not a generic DataFusion memory estimate. The server must retain
/// the resulting per-file charge before giving the corresponding reader to DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedDecodedOutputAdmission {
    format_version: u32,
    batch_size: NonZeroUsize,
    scan_column_count: NonZeroUsize,
    batch_peak_memory_bytes: NonZeroU64,
}

impl PersistedDecodedOutputAdmission {
    /// Builds a conservative Arrow batch envelope for a fixed-width scan schema.
    ///
    /// `concurrent_batch_copies` must cover every simultaneously live decoded, expression, and
    /// coalesced batch in the validated physical pipeline. Normal callers should obtain this type
    /// from [`crate::BoundPersistedPlan::streaming_output_admission`], which derives that count
    /// from the complete plan tree.
    pub fn try_for_fixed_width_schema(
        batch_size: NonZeroUsize,
        scan_schema: &Schema,
        concurrent_batch_copies: NonZeroUsize,
    ) -> Result<Self> {
        let scan_column_count = NonZeroUsize::new(scan_schema.fields().len()).ok_or_else(|| {
            unsupported("bounded persisted output requires at least one scanned column")
        })?;
        let row_width_bytes = scan_schema
            .fields()
            .iter()
            .try_fold(0_u64, |total, field| {
                fixed_width_bytes(field.data_type())
                    .and_then(|width| total.checked_add(width))
                    .ok_or_else(|| {
                        unsupported(format!(
                        "bounded persisted output does not support scan type field=[{}] type=[{}]",
                        field.name(),
                        field.data_type()
                    ))
                    })
            })?;
        let rows = u64::try_from(batch_size.get()).map_err(|_| {
            exhausted("persisted output batch row count does not fit the memory envelope")
        })?;
        let columns = u64::try_from(scan_column_count.get()).map_err(|_| {
            exhausted("persisted output column count does not fit the memory envelope")
        })?;
        // One byte per row and field is deliberately larger than Arrow's validity bitmap and
        // also covers alignment slack for fixed-width buffers.
        let row_memory_bytes = row_width_bytes
            .checked_add(columns)
            .and_then(|width| width.checked_mul(rows))
            .ok_or_else(|| exhausted("persisted output row memory envelope overflowed"))?;
        let array_control_bytes = columns
            .checked_mul(ARROW_ARRAY_CONTROL_MEMORY_BYTES)
            .ok_or_else(|| exhausted("persisted output array control envelope overflowed"))?;
        let schema_bytes = conservative_arrow_schema_retained_bytes(scan_schema)
            .map_err(|error| exhausted(error.to_string()))?
            .get();
        let one_batch_bytes = ARROW_BATCH_FIXED_MEMORY_BYTES
            .checked_add(row_memory_bytes)
            .and_then(|total| total.checked_add(array_control_bytes))
            .and_then(|total| total.checked_add(schema_bytes))
            .ok_or_else(|| exhausted("persisted output Arrow batch envelope overflowed"))?;
        let copies = u64::try_from(concurrent_batch_copies.get())
            .map_err(|_| exhausted("persisted output concurrent batch count does not fit u64"))?;
        let batch_peak_memory_bytes = one_batch_bytes
            .checked_mul(copies)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| exhausted("persisted output batch peak envelope overflowed"))?;
        Ok(Self {
            format_version: OUTPUT_ADMISSION_FORMAT_VERSION,
            batch_size,
            scan_column_count,
            batch_peak_memory_bytes,
        })
    }

    /// Returns the version of the conservative accounting formula.
    #[must_use]
    pub const fn format_version(self) -> u32 {
        self.format_version
    }

    /// Returns the maximum number of rows in one decoded Arrow batch.
    #[must_use]
    pub const fn batch_size(self) -> NonZeroUsize {
        self.batch_size
    }

    /// Returns the number of fixed-width columns decoded by the Parquet scan.
    #[must_use]
    pub const fn scan_column_count(self) -> NonZeroUsize {
        self.scan_column_count
    }

    /// Returns the Arrow/operator portion of one file's production window.
    #[must_use]
    pub const fn batch_peak_memory_bytes(self) -> NonZeroU64 {
        self.batch_peak_memory_bytes
    }

    /// Computes the memory that must be reserved before this file can be decoded.
    ///
    /// The largest row group's declared uncompressed size covers page decompression and decoder
    /// scratch. Two copies cover handoff between the page decoder and Arrow array readers. The
    /// separately profiled batch peak covers all simultaneously live fixed-width batches in the
    /// accepted physical pipeline.
    pub fn file_decode_memory_bytes(self, metadata: &ParquetMetaData) -> ParquetResult<NonZeroU64> {
        let file_rows = nonnegative_i64(
            metadata.file_metadata().num_rows(),
            "Parquet file row count",
        )?;
        let mut observed_rows = 0_u64;
        let mut max_uncompressed_row_group_bytes = 0_u64;
        for row_group in metadata.row_groups() {
            let row_group_rows = nonnegative_i64(row_group.num_rows(), "Parquet row-group rows")?;
            observed_rows = observed_rows.checked_add(row_group_rows).ok_or_else(|| {
                ParquetError::General("Parquet row-group row count overflowed u64".to_owned())
            })?;
            let uncompressed_bytes = nonnegative_i64(
                row_group.total_byte_size(),
                "Parquet row-group uncompressed bytes",
            )?;
            max_uncompressed_row_group_bytes =
                max_uncompressed_row_group_bytes.max(uncompressed_bytes);
        }
        if observed_rows != file_rows {
            return Err(ParquetError::General(format!(
                "Parquet row-group rows differ from file metadata observed=[{observed_rows}] file=[{file_rows}]"
            )));
        }
        let memory_bytes = max_uncompressed_row_group_bytes
            .checked_mul(PARQUET_ROW_GROUP_DECODE_COPIES)
            .and_then(|bytes| bytes.checked_add(self.batch_peak_memory_bytes.get()))
            .and_then(|bytes| bytes.checked_add(PARQUET_DECODER_FIXED_MEMORY_BYTES))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                ParquetError::General("Parquet decoded output memory charge overflowed".to_owned())
            })?;
        Ok(memory_bytes)
    }
}

pub(crate) fn profile_streaming_output_plan(
    plan: &Arc<dyn ExecutionPlan>,
    batch_size: NonZeroUsize,
) -> Result<PersistedDecodedOutputAdmission> {
    if plan.properties().output_partitioning().partition_count() != 1 {
        return Err(unsupported(
            "bounded persisted output requires exactly one output partition",
        ));
    }

    let mut physical_node_count = 0_usize;
    let mut expression_node_count = 0_usize;
    let mut scan_schema = None;
    let mut scan_count = 0_usize;
    plan.apply(|node| {
        physical_node_count = physical_node_count
            .checked_add(1)
            .ok_or_else(|| exhausted("bounded output physical node count overflowed"))?;
        validate_fixed_width_schema(node.schema().as_ref(), node.name())?;

        if let Some(parquet) = node.as_any().downcast_ref::<ParquetExec>() {
            scan_count = scan_count
                .checked_add(1)
                .ok_or_else(|| exhausted("bounded output scan count overflowed"))?;
            scan_schema = Some(parquet.schema());
            if let Some(predicate) = parquet.predicate() {
                profile_bounded_filter_expression(
                    predicate,
                    parquet.base_config().file_schema.as_ref(),
                    &mut expression_node_count,
                )?;
            }
        } else if let Some(projection) = node.as_any().downcast_ref::<ProjectionExec>() {
            for (expression, _) in projection.expr() {
                if expression.as_any().downcast_ref::<Column>().is_none() {
                    return Err(unsupported(
                        "bounded persisted output only supports direct column projection",
                    ));
                }
            }
        } else if let Some(filter) = node.as_any().downcast_ref::<FilterExec>() {
            let input_schema = filter.input().schema();
            profile_bounded_filter_expression(
                filter.predicate(),
                input_schema.as_ref(),
                &mut expression_node_count,
            )?;
        } else if !(node.as_any().is::<GlobalLimitExec>()
            || node.as_any().is::<LocalLimitExec>()
            || node.as_any().is::<CoalescePartitionsExec>()
            || node.as_any().is::<EmptyExec>())
        {
            return Err(unsupported(format!(
                "physical operator is outside the bounded persisted output subset operator=[{}]",
                node.name()
            )));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    if scan_count > 1 {
        return Err(unsupported(format!(
            "bounded persisted output supports at most one Parquet scan actual=[{scan_count}]"
        )));
    }
    let concurrent_batch_copies = physical_node_count
        .checked_add(expression_node_count)
        .and_then(|count| count.checked_add(2))
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| exhausted("bounded output concurrent batch envelope overflowed"))?;
    let admission_schema = scan_schema.unwrap_or_else(|| plan.schema());
    PersistedDecodedOutputAdmission::try_for_fixed_width_schema(
        batch_size,
        admission_schema.as_ref(),
        concurrent_batch_copies,
    )
}

fn profile_bounded_filter_expression(
    expression: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    expression_node_count: &mut usize,
) -> Result<()> {
    expression.apply(|expression| {
        *expression_node_count = expression_node_count
            .checked_add(1)
            .ok_or_else(|| exhausted("bounded output predicate expression count overflowed"))?;
        if !is_bounded_filter_expression(expression.as_ref()) {
            return Err(unsupported(format!(
                "physical filter expression is outside the bounded persisted output subset expression=[{expression}]"
            )));
        }
        let data_type = expression.data_type(input_schema)?;
        if fixed_width_bytes(&data_type).is_none() {
            return Err(unsupported(format!(
                "bounded persisted output predicate materializes variable-width type=[{data_type}]"
            )));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn is_bounded_filter_expression(expression: &dyn PhysicalExpr) -> bool {
    expression.as_any().is::<Column>()
        || expression.as_any().is::<Literal>()
        || expression.as_any().is::<BinaryExpr>()
        || expression.as_any().is::<CastExpr>()
        || expression.as_any().is::<TryCastExpr>()
        || expression.as_any().is::<NegativeExpr>()
        || expression.as_any().is::<NotExpr>()
        || expression.as_any().is::<IsNullExpr>()
        || expression.as_any().is::<IsNotNullExpr>()
}

fn validate_fixed_width_schema(schema: &Schema, operator: &str) -> Result<()> {
    for field in schema.fields() {
        if fixed_width_bytes(field.data_type()).is_none() {
            return Err(unsupported(format!(
                "physical operator emits unsupported variable-width type operator=[{operator}] field=[{}] type=[{}]",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn fixed_width_bytes(data_type: &DataType) -> Option<u64> {
    match data_type {
        DataType::Null => Some(0),
        DataType::Boolean => Some(1),
        DataType::FixedSizeBinary(width) if *width >= 0 => u64::try_from(*width).ok(),
        other => other
            .primitive_width()
            .and_then(|width| u64::try_from(width).ok()),
    }
}

fn nonnegative_i64(value: i64, label: &str) -> ParquetResult<u64> {
    u64::try_from(value)
        .map_err(|_| ParquetError::General(format!("{label} is negative value=[{value}]")))
}

fn unsupported(message: impl Into<String>) -> DataFusionError {
    DataFusionError::NotImplemented(message.into())
}

fn exhausted(message: impl Into<String>) -> DataFusionError {
    DataFusionError::ResourcesExhausted(message.into())
}
