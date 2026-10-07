//! Conservative pre-admission for the first bounded persisted output subset.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::physical_plan::parquet::ParquetExec;
use datafusion::functions::core::coalesce::CoalesceFunc;
use datafusion::functions::datetime::to_timestamp::ToTimestampNanosFunc;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NegativeExpr, NotExpr,
    TryCastExpr,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec, SymmetricHashJoinExec,
};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::ExecutionPlan;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaData;
use zippy_types::schema_memory::conservative_arrow_schema_retained_bytes;

use crate::active_plan::{ActiveSegmentExec, PersistedAndActiveExec};

const OUTPUT_ADMISSION_FORMAT_VERSION: u32 = 3;
const ARROW_BATCH_FIXED_MEMORY_BYTES: u64 = 64 * 1024;
const ARROW_ARRAY_CONTROL_MEMORY_BYTES: u64 = 1024;
const PARQUET_DECODER_FIXED_MEMORY_BYTES: u64 = 256 * 1024;
const PARQUET_ROW_GROUP_DECODE_COPIES: u64 = 2;
const ACTIVE_SEGMENT_READER_FIXED_MEMORY_BYTES: u64 = 64 * 1024;

/// A versioned memory envelope for bounded persisted and active analytical pipelines.
///
/// This is not a generic DataFusion memory estimate. It covers decoded batches flowing between
/// an explicit physical-operator allowlist; buffering operators are independently constrained by
/// the query-scoped and process-wide DataFusion memory pools. The server must retain the resulting
/// per-file charge before giving the corresponding reader to DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedDecodedOutputAdmission {
    format_version: u32,
    batch_size: NonZeroUsize,
    scan_column_count: NonZeroUsize,
    batch_peak_memory_bytes: NonZeroU64,
    utf8_columns: u64,
    batch_copies: u64,
    literal_value_bytes: u64,
    active_value_bytes: Option<u64>,
    active_rows: u64,
    expands_rows: bool,
}

impl PersistedDecodedOutputAdmission {
    /// Admits a sequential raw-log scan with no expressions or buffering operators.
    ///
    /// Two batch copies cover decoder handoff and the current delivered batch. UTF-8
    /// bounds come from the pinned span or from the Parquet footer before decoding.
    pub fn try_for_log_scan(
        batch_size: NonZeroUsize,
        schema: &Schema,
        active_value_bytes: Option<u64>,
        source_rows: u64,
    ) -> Result<Self> {
        let mut admission = Self::try_for_schema(
            batch_size,
            schema,
            NonZeroUsize::new(2).expect("two scan batch copies"),
            true,
        )?;
        admission.active_value_bytes = active_value_bytes;
        admission.active_rows = source_rows;
        admission.utf8_columns = utf8_column_count(schema);
        Ok(admission)
    }

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
        Self::try_for_schema(batch_size, scan_schema, concurrent_batch_copies, false)
    }

    fn try_for_schema(
        batch_size: NonZeroUsize,
        scan_schema: &Schema,
        concurrent_batch_copies: NonZeroUsize,
        allow_utf8: bool,
    ) -> Result<Self> {
        // DataFusion legitimately uses zero-column scans to preserve only row cardinality for
        // COUNT/CROSS JOIN inputs. Charge one synthetic control slot so such batches still have
        // a nonzero per-row and array-control envelope.
        let scan_column_count = NonZeroUsize::new(scan_schema.fields().len().max(1))
            .expect("synthetic scan column count is nonzero");
        let row_width_bytes = scan_schema
            .fields()
            .iter()
            .try_fold(0_u64, |total, field| {
                storage_width_bytes(field.data_type(), allow_utf8)
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
            utf8_columns: 0,
            batch_copies: copies,
            literal_value_bytes: 0,
            active_value_bytes: None,
            active_rows: 0,
            expands_rows: false,
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

    /// Returns the number of accounted fixed-width column slots.
    ///
    /// A cardinality-only zero-column scan reports one synthetic control slot.
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
        // A cell cannot exceed its row group's entire uncompressed payload, including the
        // dictionary. Multiplying by output rows covers repeated dictionary references.
        let variable_bytes = self
            .variable_memory_bytes(max_uncompressed_row_group_bytes, file_rows)
            .map_err(|error| ParquetError::General(error.to_string()))?;
        let memory_bytes = max_uncompressed_row_group_bytes
            .checked_mul(PARQUET_ROW_GROUP_DECODE_COPIES)
            .and_then(|bytes| bytes.checked_add(self.batch_peak_memory_bytes.get()))
            .and_then(|bytes| bytes.checked_add(variable_bytes))
            .and_then(|bytes| bytes.checked_add(PARQUET_DECODER_FIXED_MEMORY_BYTES))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                ParquetError::General("Parquet decoded output memory charge overflowed".to_owned())
            })?;
        Ok(memory_bytes)
    }

    /// Computes the task reservation retained while one active span stream produces batches.
    pub fn active_decode_memory_bytes(self) -> Result<NonZeroU64> {
        let maximum = if self.utf8_columns == 0 {
            0
        } else {
            self.active_value_bytes
                .ok_or_else(|| unsupported("active UTF-8 decoding has no pinned value bound"))?
        };
        let variable_bytes = self.variable_memory_bytes(maximum, self.active_rows)?;
        self.batch_peak_memory_bytes
            .get()
            .checked_add(ACTIVE_SEGMENT_READER_FIXED_MEMORY_BYTES)
            .and_then(|bytes| bytes.checked_add(variable_bytes))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| exhausted("active segment decoded output memory charge overflowed"))
    }
    fn variable_memory_bytes(self, maximum: u64, source_rows: u64) -> Result<u64> {
        let rows = if self.expands_rows {
            self.batch_size.get() as u64
        } else {
            (self.batch_size.get() as u64).min(source_rows)
        };
        maximum
            .max(self.literal_value_bytes)
            .checked_mul(self.utf8_columns)
            .and_then(|bytes| bytes.checked_mul(rows))
            .and_then(|bytes| bytes.checked_mul(self.batch_copies))
            .ok_or_else(|| exhausted("UTF-8 decoded output memory charge overflowed"))
    }
}

pub(crate) fn profile_streaming_output_plan(
    plan: &Arc<dyn ExecutionPlan>,
    batch_size: NonZeroUsize,
) -> Result<PersistedDecodedOutputAdmission> {
    // LIMIT 0 and empty manifests produce no decoded batches. Their schema is already charged
    // by plan admission; variable-width fields alone do not imply any value-buffer allocation.
    if plan.as_any().is::<EmptyExec>() {
        return PersistedDecodedOutputAdmission::try_for_fixed_width_schema(
            batch_size,
            &Schema::empty(),
            NonZeroUsize::new(1).expect("one empty plan"),
        );
    }
    if plan.properties().output_partitioning().partition_count() != 1 {
        return Err(unsupported(
            "bounded persisted output requires exactly one output partition",
        ));
    }

    let mut physical_node_count = 0_usize;
    let mut expression_node_count = 0_usize;
    let mut scan_schemas = Vec::new();
    let mut utf8_columns = 0_u64;
    let mut literal_value_bytes = 0_u64;
    let mut active_value_bytes = None;
    let mut active_rows = 0_u64;
    let mut expands_rows = false;
    plan.apply(|node| {
        physical_node_count = physical_node_count
            .checked_add(1)
            .ok_or_else(|| exhausted("bounded output physical node count overflowed"))?;
        expands_rows |= node.as_any().is::<HashJoinExec>()
            || node.as_any().is::<CrossJoinExec>()
            || node.as_any().is::<NestedLoopJoinExec>()
            || node.as_any().is::<SortMergeJoinExec>()
            || node.as_any().is::<SymmetricHashJoinExec>();
        let schema = node.schema();
        validate_bounded_schema(schema.as_ref(), node.name())?;
        utf8_columns = utf8_columns.max(utf8_column_count(&schema));
        // Every operator output, including scans and the root, is a candidate batch envelope.
        scan_schemas.push(schema);
        // Generated variable-width aggregates/windows need their own expansion proofs. Group
        // keys and the native row_number window only copy source values and remain admissible.
        if let Some(aggregate) = node.as_any().downcast_ref::<AggregateExec>() {
            expands_rows |= aggregate.group_expr().groups().len() > 1;
            for (expression, _) in aggregate
                .group_expr()
                .expr()
                .iter()
                .chain(aggregate.group_expr().null_expr())
            {
                profile_bounded_expression(
                    expression,
                    &aggregate.input().schema(),
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "group",
                )?;
            }
            for field in aggregate
                .schema()
                .fields()
                .iter()
                .skip(aggregate.group_expr().expr().len())
            {
                if field.data_type() == &DataType::Utf8 {
                    return Err(unsupported(
                        "variable-width aggregate output has no expansion bound",
                    ));
                }
            }
            let input_schema = aggregate.input_schema();
            for function in aggregate.aggr_expr() {
                for expression in function.expressions().iter().chain(
                    function
                        .order_bys()
                        .into_iter()
                        .flat_map(|order| order.iter().map(|sort| &sort.expr)),
                ) {
                    profile_bounded_expression(
                        expression,
                        &input_schema,
                        &mut expression_node_count,
                        &mut literal_value_bytes,
                        "aggregate",
                    )?;
                }
            }
            for expression in aggregate.filter_expr().iter().flatten() {
                profile_bounded_expression(
                    expression,
                    &input_schema,
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "aggregate filter",
                )?;
            }
        }
        let join = if let Some(join) = node.as_any().downcast_ref::<HashJoinExec>() {
            Some((join.on(), join.filter()))
        } else if let Some(join) = node.as_any().downcast_ref::<SortMergeJoinExec>() {
            Some((join.on(), join.filter().as_ref()))
        } else if let Some(join) = node.as_any().downcast_ref::<SymmetricHashJoinExec>() {
            Some((join.on(), join.filter()))
        } else {
            node.as_any()
                .downcast_ref::<NestedLoopJoinExec>()
                .map(|join| (&[][..], join.filter()))
        };
        if let Some((keys, filter)) = join {
            for (left, right) in keys {
                for (expression, input) in [(left, 0), (right, 1)] {
                    profile_bounded_expression(
                        expression,
                        &node.children()[input].schema(),
                        &mut expression_node_count,
                        &mut literal_value_bytes,
                        "join key",
                    )?;
                }
            }
            if let Some(filter) = filter {
                profile_bounded_expression(
                    filter.expression(),
                    filter.schema(),
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "join filter",
                )?;
            }
        }
        if let Some(repartition) = node.as_any().downcast_ref::<RepartitionExec>() {
            if let datafusion::physical_plan::Partitioning::Hash(expressions, _) =
                repartition.partitioning()
            {
                for expression in expressions {
                    profile_bounded_expression(
                        expression,
                        &node.children()[0].schema(),
                        &mut expression_node_count,
                        &mut literal_value_bytes,
                        "repartition",
                    )?;
                }
            }
        }
        let windows = if let Some(window) = node.as_any().downcast_ref::<WindowAggExec>() {
            Some(window.window_expr())
        } else {
            node.as_any()
                .downcast_ref::<BoundedWindowAggExec>()
                .map(|window| window.window_expr())
        };
        if let Some(windows) = windows {
            let input_schema = node.children()[0].schema();
            for window in windows {
                for expression in window
                    .expressions()
                    .iter()
                    .chain(window.partition_by())
                    .chain(window.order_by().iter().map(|sort| &sort.expr))
                {
                    profile_bounded_expression(
                        expression,
                        &input_schema,
                        &mut expression_node_count,
                        &mut literal_value_bytes,
                        "window",
                    )?;
                }
                if window.field()?.data_type() == &DataType::Utf8 {
                    return Err(unsupported(
                        "variable-width window output has no expansion bound",
                    ));
                }
            }
        }

        let ordering = if let Some(sort) = node.as_any().downcast_ref::<SortExec>() {
            Some(sort.expr())
        } else if let Some(sort) = node.as_any().downcast_ref::<SortPreservingMergeExec>() {
            Some(sort.expr())
        } else {
            node.as_any()
                .downcast_ref::<PartialSortExec>()
                .map(|sort| sort.expr())
        };
        if let Some(ordering) = ordering {
            let input_schema = node.children()[0].schema();
            for sort in ordering {
                profile_bounded_expression(
                    &sort.expr,
                    &input_schema,
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "sort",
                )?;
            }
        }
        if let Some(parquet) = node.as_any().downcast_ref::<ParquetExec>() {
            if let Some(predicate) = parquet.predicate() {
                profile_bounded_expression(
                    predicate,
                    parquet.base_config().file_schema.as_ref(),
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "filter",
                )?;
            }
        } else if let Some(active) = node.as_any().downcast_ref::<ActiveSegmentExec>() {
            if active.batch_size() != batch_size {
                return Err(unsupported(
                    "active segment batch size differs from output admission policy",
                ));
            }
            for description in active.descriptions() {
                active_rows = active_rows
                    .checked_add(description.row_count().get())
                    .ok_or_else(|| exhausted("active input rows overflowed"))?;
            }
            if utf8_column_count(&active.schema()) > 0 {
                for description in active.descriptions() {
                    let maximum = description.max_utf8_value_bytes().ok_or_else(|| {
                        unsupported("active UTF-8 scan requires a pinned value bound")
                    })?;
                    active_value_bytes = Some(active_value_bytes.unwrap_or(0_u64).max(maximum));
                }
            }
        } else if let Some(projection) = node.as_any().downcast_ref::<ProjectionExec>() {
            for (expression, _) in projection.expr() {
                profile_bounded_expression(
                    expression,
                    projection.input().schema().as_ref(),
                    &mut expression_node_count,
                    &mut literal_value_bytes,
                    "projection",
                )?;
            }
        } else if let Some(filter) = node.as_any().downcast_ref::<FilterExec>() {
            profile_bounded_expression(
                filter.predicate(),
                filter.input().schema().as_ref(),
                &mut expression_node_count,
                &mut literal_value_bytes,
                "filter",
            )?;
        } else if !(node.as_any().is::<GlobalLimitExec>()
            || node.as_any().is::<LocalLimitExec>()
            || node.as_any().is::<CoalescePartitionsExec>()
            || node.as_any().is::<PersistedAndActiveExec>()
            || node
                .as_any()
                .is::<crate::logical_schema::LogicalSchemaExec>()
            || node.as_any().is::<EmptyExec>()
            || node.as_any().is::<SortExec>()
            || node.as_any().is::<SortPreservingMergeExec>()
            || node.as_any().is::<PartialSortExec>()
            || node.as_any().is::<AggregateExec>()
            || node.as_any().is::<HashJoinExec>()
            || node.as_any().is::<CrossJoinExec>()
            || node.as_any().is::<NestedLoopJoinExec>()
            || node.as_any().is::<SortMergeJoinExec>()
            || node.as_any().is::<SymmetricHashJoinExec>()
            || node.as_any().is::<WindowAggExec>()
            || node.as_any().is::<BoundedWindowAggExec>()
            || node.as_any().is::<RepartitionExec>()
            || node.as_any().is::<UnionExec>())
        {
            return Err(unsupported(format!(
                "physical operator is outside the bounded persisted output subset operator=[{}]",
                node.name()
            )));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let concurrent_batch_copies = physical_node_count
        .checked_add(expression_node_count)
        .and_then(|count| count.checked_add(2))
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| exhausted("bounded output concurrent batch envelope overflowed"))?;
    scan_schemas
        .iter()
        .map(|schema| {
            let mut admission = PersistedDecodedOutputAdmission::try_for_schema(
                batch_size,
                schema.as_ref(),
                concurrent_batch_copies,
                true,
            )?;
            admission.utf8_columns = utf8_columns.max(u64::from(literal_value_bytes > 0));
            admission.literal_value_bytes = literal_value_bytes;
            admission.active_value_bytes = active_value_bytes.or(Some(0));
            admission.active_rows = active_rows;
            admission.expands_rows = expands_rows;
            Ok::<_, DataFusionError>(admission)
        })
        .try_fold(
            None,
            |widest: Option<PersistedDecodedOutputAdmission>, admission| {
                let admission = admission?;
                Ok::<Option<PersistedDecodedOutputAdmission>, DataFusionError>(Some(match widest {
                    Some(current)
                        if current.batch_peak_memory_bytes()
                            >= admission.batch_peak_memory_bytes() =>
                    {
                        current
                    }
                    _ => admission,
                }))
            },
        )?
        .ok_or_else(|| unsupported("bounded output plan has no schema to admit"))
}

fn profile_bounded_expression(
    expression: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    expression_node_count: &mut usize,
    literal_value_bytes: &mut u64,
    role: &str,
) -> Result<()> {
    expression.apply(|expression| {
        *expression_node_count = expression_node_count
            .checked_add(1)
            .ok_or_else(|| exhausted("bounded output predicate expression count overflowed"))?;
        if !is_bounded_filter_expression(expression.as_ref()) {
            return Err(unsupported(format!(
                "physical {role} expression is outside the bounded persisted output subset expression=[{expression}]"
            )));
        }
        if is_builtin_coalesce(expression.as_ref()) {
            // COALESCE selects existing argument values; UTF-8 width cannot exceed the
            // widest child. Charge its current/output arrays and boolean selection masks.
            *expression_node_count = expression_node_count.checked_add(4)
                .ok_or_else(|| exhausted("coalesce temporary batch envelope overflowed"))?;
        }
        if let Some(function) = expression.as_any().downcast_ref::<ScalarFunctionExpr>() {
            if function.fun().inner().as_any().is::<ToTimestampNanosFunc>()
                && (function.args().len() != 1 || function.args().iter().any(|argument| {
                    argument.data_type(input_schema).map_or(true, |kind| fixed_width_bytes(&kind).is_none())
                }))
            {
                return Err(unsupported("timestamp conversion requires one fixed-width argument"));
            }
        }
        let data_type = expression.data_type(input_schema)?;
        if data_type == DataType::Utf8 {
            if let Some(literal) = expression.as_any().downcast_ref::<Literal>() {
                if let datafusion::common::ScalarValue::Utf8(Some(value)) = literal.value() {
                    *literal_value_bytes = (*literal_value_bytes).max(value.len() as u64);
                }
            } else if !expression.as_any().is::<Column>()
                && !is_builtin_coalesce(expression.as_ref())
            {
                return Err(unsupported("UTF-8 expression output has no expansion bound"));
            }
        } else if fixed_width_bytes(&data_type).is_none() {
            return Err(unsupported(format!(
                "bounded persisted output {role} materializes variable-width type=[{data_type}]"
            )));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn is_builtin_coalesce(expression: &dyn PhysicalExpr) -> bool {
    expression
        .as_any()
        .downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| function.fun().inner().as_any().is::<CoalesceFunc>())
}

fn is_bounded_filter_expression(expression: &dyn PhysicalExpr) -> bool {
    is_builtin_coalesce(expression)
        || expression
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|function| function.fun().inner().as_any().is::<ToTimestampNanosFunc>())
        || expression.as_any().is::<Column>()
        || expression.as_any().is::<Literal>()
        || expression.as_any().is::<BinaryExpr>()
        || expression.as_any().is::<CastExpr>()
        || expression.as_any().is::<TryCastExpr>()
        || expression.as_any().is::<NegativeExpr>()
        || expression.as_any().is::<NotExpr>()
        || expression.as_any().is::<IsNullExpr>()
        || expression.as_any().is::<IsNotNullExpr>()
}

fn validate_bounded_schema(schema: &Schema, operator: &str) -> Result<()> {
    for field in schema.fields() {
        if storage_width_bytes(field.data_type(), true).is_none() {
            return Err(unsupported(format!(
                "physical operator emits unsupported variable-width type operator=[{operator}] field=[{}] type=[{}]",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn utf8_column_count(schema: &Schema) -> u64 {
    schema
        .fields()
        .iter()
        .filter(|field| field.data_type() == &DataType::Utf8)
        .count() as u64
}

fn storage_width_bytes(data_type: &DataType, allow_utf8: bool) -> Option<u64> {
    if allow_utf8 && data_type == &DataType::Utf8 {
        Some(8)
    } else {
        fixed_width_bytes(data_type)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn raw_log_scan_requires_utf8_bounds_and_charges_repeated_values() {
        let schema = Schema::new(vec![Field::new("key", DataType::Utf8, false)]);
        let rows = NonZeroUsize::new(1024).unwrap();
        let missing =
            PersistedDecodedOutputAdmission::try_for_log_scan(rows, &schema, None, 100).unwrap();
        assert!(missing.active_decode_memory_bytes().is_err());
        let admitted =
            PersistedDecodedOutputAdmission::try_for_log_scan(rows, &schema, Some(100_000), 100)
                .unwrap();
        assert!(admitted.active_decode_memory_bytes().unwrap().get() >= 20_000_000);
        let overflowing =
            PersistedDecodedOutputAdmission::try_for_log_scan(rows, &schema, Some(u64::MAX), 100)
                .unwrap();
        assert!(overflowing.active_decode_memory_bytes().is_err());
        let unsupported = Schema::new(vec![Field::new("payload", DataType::Binary, false)]);
        assert!(PersistedDecodedOutputAdmission::try_for_log_scan(
            rows,
            &unsupported,
            Some(100),
            100
        )
        .is_err());
    }

    #[test]
    fn empty_plan_does_not_require_a_value_buffer_bound_for_utf8_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema.clone()));
        let rows = NonZeroUsize::new(1024).unwrap();
        let admission = profile_streaming_output_plan(&plan, rows).unwrap();
        assert!(admission.batch_peak_memory_bytes().get() < 128 * 1024);
        assert!(
            PersistedDecodedOutputAdmission::try_for_fixed_width_schema(
                rows,
                &schema,
                NonZeroUsize::new(1).unwrap(),
            )
            .is_err(),
            "nonempty UTF-8 scans still require a separate payload proof"
        );
    }
    #[test]
    fn utf8_dictionary_expansion_is_charged_before_decoding() {
        use arrow::{array::StringArray, record_batch::RecordBatch};
        use bytes::Bytes;
        use parquet::{
            arrow::ArrowWriter,
            file::{
                properties::WriterProperties,
                reader::{FileReader, SerializedFileReader},
            },
        };
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let value = "long-dictionary-value".repeat(512);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![value.as_str(); 1024]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let properties = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .build();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema.clone(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let file = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        let mut admission = PersistedDecodedOutputAdmission::try_for_schema(
            NonZeroUsize::new(1024).unwrap(),
            &schema,
            NonZeroUsize::new(3).unwrap(),
            true,
        )
        .unwrap();
        admission.utf8_columns = 1;
        let charge = admission
            .file_decode_memory_bytes(file.metadata())
            .unwrap()
            .get();
        assert!(charge > value.len() as u64 * 1024 * 3);
        admission.active_value_bytes = Some(value.len() as u64);
        admission.active_rows = 1024;
        assert!(
            admission.active_decode_memory_bytes().unwrap().get() > value.len() as u64 * 1024 * 3
        );
        admission.active_value_bytes = Some(u64::MAX);
        assert!(admission.active_decode_memory_bytes().is_err());
        admission.active_value_bytes = None;
        assert!(admission.active_decode_memory_bytes().is_err());
    }

    #[test]
    fn utf8_expanding_expressions_fail_before_materialization() {
        use datafusion::common::ScalarValue;
        let schema = Schema::new(vec![Field::new("price", DataType::Int64, false)]);
        let expression: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(
            Arc::new(Column::new("price", 0)),
            DataType::Utf8,
            None,
        ));
        let mut count = 0;
        let mut literal_bytes = 0;
        assert!(profile_bounded_expression(
            &expression,
            &schema,
            &mut count,
            &mut literal_bytes,
            "projection"
        )
        .is_err());
        let expression: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some("中文".to_owned()))));
        profile_bounded_expression(
            &expression,
            &schema,
            &mut count,
            &mut literal_bytes,
            "projection",
        )
        .unwrap();
        assert_eq!(literal_bytes, 6);
    }
}
