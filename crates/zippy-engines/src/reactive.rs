use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};
pub use zippy_operators::ReactiveInvalidValuePolicy;
use zippy_operators::{ReactiveFactor, ReactiveFactorContext};

use crate::table_view::{project_columns_by_position, string_array};

struct FilteredReactiveColumns {
    columns: Vec<ArrayRef>,
    projection: &'static str,
}

fn reactive_latency_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("ZIPPY_REACTIVE_LATENCY_TRACE") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    })
}

fn write_latency_trace_line(mut line: String) {
    use std::io::Write;

    line.push('\n');
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(line.as_bytes());
}

fn instant_delta_us(start: Instant, end: Instant) -> f64 {
    end.saturating_duration_since(start).as_secs_f64() * 1_000_000.0
}

fn single_row_source_emit_ns(table: &SegmentTableView) -> Option<i64> {
    if table.num_rows() != 1 {
        return None;
    }
    let column = table.column("source_emit_ns").ok()?;
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        if !values.is_null(0) {
            return Some(values.value(0));
        }
        return None;
    }
    if let Some(values) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
        if !values.is_null(0) {
            return Some(values.value(0));
        }
    }
    None
}

/// Failure handling policy for stateful reactive factor state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReactiveStateFailurePolicy {
    /// Treat publish failures as fatal and discard the engine instance.
    FailFast,
    /// Keep a dirty-key undo log so in-process publish failures can roll back touched state.
    Rollback,
}

impl ReactiveStateFailurePolicy {
    /// Return the stable configuration string for this policy.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FailFast => "fail_fast",
            Self::Rollback => "rollback",
        }
    }
}

/// Stateful engine that appends reactive factor outputs to each input batch.
pub struct ReactiveStateEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    factors: Vec<Box<dyn ReactiveFactor>>,
    id_filter: Option<HashSet<String>>,
    id_field: Option<String>,
    id_column_index: Option<usize>,
    pending_filtered_rows: u64,
    state_failure_policy: ReactiveStateFailurePolicy,
    invalid_value_policy: ReactiveInvalidValuePolicy,
    transaction_active: bool,
}

impl ReactiveStateEngine {
    /// Create a new reactive state engine.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param factors: Stateful factors evaluated in declaration order against the current batch.
    /// :type factors: Vec<Box<dyn ReactiveFactor>>
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
    ) -> Result<Self> {
        Self::new_with_state_failure_policy(
            name,
            input_schema,
            factors,
            ReactiveStateFailurePolicy::FailFast,
        )
    }

    /// Create a new reactive state engine with explicit state failure handling.
    ///
    /// :param state_failure_policy: Controls whether state rollback is tracked.
    /// :type state_failure_policy: ReactiveStateFailurePolicy
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new_with_state_failure_policy(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        state_failure_policy: ReactiveStateFailurePolicy,
    ) -> Result<Self> {
        Self::new_with_id_filter_and_state_failure_policy(
            name,
            input_schema,
            factors,
            "",
            None,
            state_failure_policy,
        )
    }

    /// Create a new reactive state engine with explicit invalid-value handling.
    pub fn new_with_invalid_value_policy(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        invalid_value_policy: ReactiveInvalidValuePolicy,
    ) -> Result<Self> {
        Self::new_with_id_filter_state_failure_and_invalid_value_policy(
            name,
            input_schema,
            factors,
            "",
            None,
            ReactiveStateFailurePolicy::FailFast,
            invalid_value_policy,
        )
    }

    /// Create a new reactive state engine with an optional id whitelist.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param factors: Stateful factors evaluated in declaration order against the current batch.
    /// :type factors: Vec<Box<dyn ReactiveFactor>>
    /// :param id_field: Utf8 identifier column used for whitelist filtering.
    /// :type id_field: &str
    /// :param id_filter: Optional exact-match whitelist for the first input column.
    /// :type id_filter: Option<Vec<String>>
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new_with_id_filter(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        id_field: &str,
        id_filter: Option<Vec<String>>,
    ) -> Result<Self> {
        Self::new_with_id_filter_and_state_failure_policy(
            name,
            input_schema,
            factors,
            id_field,
            id_filter,
            ReactiveStateFailurePolicy::FailFast,
        )
    }

    /// Create a new reactive state engine with id filtering and explicit failure handling.
    ///
    /// :param state_failure_policy: Controls whether state rollback is tracked.
    /// :type state_failure_policy: ReactiveStateFailurePolicy
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new_with_id_filter_and_state_failure_policy(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        id_field: &str,
        id_filter: Option<Vec<String>>,
        state_failure_policy: ReactiveStateFailurePolicy,
    ) -> Result<Self> {
        Self::new_with_id_filter_state_failure_and_invalid_value_policy(
            name,
            input_schema,
            factors,
            id_field,
            id_filter,
            state_failure_policy,
            ReactiveInvalidValuePolicy::Reject,
        )
    }

    /// Create a new reactive state engine with id filtering, state-failure handling, and
    /// invalid-value handling.
    pub fn new_with_id_filter_state_failure_and_invalid_value_policy(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        id_field: &str,
        id_filter: Option<Vec<String>>,
        state_failure_policy: ReactiveStateFailurePolicy,
        invalid_value_policy: ReactiveInvalidValuePolicy,
    ) -> Result<Self> {
        let output_schema = build_output_schema(&input_schema, &factors, invalid_value_policy)?;
        let id_column_index = if id_field.is_empty() {
            None
        } else {
            Some(validate_id_field(input_schema.as_ref(), id_field)?)
        };
        let id_filter = normalize_id_filter(id_filter)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            factors,
            id_filter,
            id_field: (!id_field.is_empty()).then(|| id_field.to_string()),
            id_column_index,
            pending_filtered_rows: 0,
            state_failure_policy,
            invalid_value_policy,
            transaction_active: false,
        })
    }

    /// Return the configured state failure policy.
    pub fn state_failure_policy(&self) -> ReactiveStateFailurePolicy {
        self.state_failure_policy
    }

    /// Return the configured invalid-value policy.
    pub fn invalid_value_policy(&self) -> ReactiveInvalidValuePolicy {
        self.invalid_value_policy
    }
}

fn normalize_id_filter(id_filter: Option<Vec<String>>) -> Result<Option<HashSet<String>>> {
    match id_filter {
        Some(values) if values.is_empty() => Err(ZippyError::InvalidConfig {
            reason: "id_filter must not be empty".to_string(),
        }),
        Some(values) => Ok(Some(values.into_iter().collect::<HashSet<_>>())),
        None => Ok(None),
    }
}

impl Engine for ReactiveStateEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        let local_transaction = self.state_failure_policy == ReactiveStateFailurePolicy::Rollback
            && !self.transaction_active;
        if local_transaction {
            self.begin_transaction();
        }

        let result = self.evaluate_table(table);

        if local_transaction {
            match result {
                Ok(outputs) => {
                    self.commit_transaction();
                    Ok(outputs)
                }
                Err(err) => {
                    self.rollback_transaction()?;
                    Err(err)
                }
            }
        } else {
            result
        }
    }

    fn begin_transaction(&mut self) {
        if self.state_failure_policy != ReactiveStateFailurePolicy::Rollback
            || self.transaction_active
        {
            return;
        }

        for factor in &mut self.factors {
            factor.begin_transaction();
        }
        self.transaction_active = true;
    }

    fn commit_transaction(&mut self) {
        if self.state_failure_policy != ReactiveStateFailurePolicy::Rollback
            || !self.transaction_active
        {
            return;
        }

        for factor in &mut self.factors {
            factor.commit_transaction();
        }
        self.transaction_active = false;
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        if self.state_failure_policy != ReactiveStateFailurePolicy::Rollback
            || !self.transaction_active
        {
            return Ok(());
        }

        for factor in &mut self.factors {
            factor.rollback_transaction()?;
        }
        self.transaction_active = false;

        Ok(())
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: 0,
            filtered_rows_total: self.pending_filtered_rows,
        };
        self.pending_filtered_rows = 0;
        delta
    }
}

impl ReactiveStateEngine {
    fn evaluate_table(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        let trace_enabled = reactive_latency_trace_enabled();
        let total_started_at = trace_enabled.then(Instant::now);
        let input_rows = table.num_rows();
        let source_emit_ns = trace_enabled
            .then(|| single_row_source_emit_ns(&table))
            .flatten()
            .unwrap_or_default();
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }
        let filter_started_at = trace_enabled.then(Instant::now);
        let filtered_rows_before = self.pending_filtered_rows;
        let filtered = self.filter_by_id_whitelist(&table)?;
        let filtered_rows = self
            .pending_filtered_rows
            .saturating_sub(filtered_rows_before);
        let filter_done_at = trace_enabled.then(Instant::now);

        if filtered.columns.first().map_or(0, |column| column.len()) == 0 {
            if let (Some(total_started_at), Some(filter_started_at), Some(filter_done_at)) =
                (total_started_at, filter_started_at, filter_done_at)
            {
                write_latency_trace_line(format!(
                    "zippy_reactive_latency_trace event=[on_data] engine=[{}] rows=[{}] source_emit_ns=[{}] filtered_rows=[{}] filter_projection=[{}] project_filter_us=[{:.3}] factor_eval_us=[0.000] schema_extend_us=[0.000] output_build_us=[0.000] total_us=[{:.3}] factor_count=[{}]",
                    self.name,
                    input_rows,
                    source_emit_ns,
                    filtered_rows,
                    filtered.projection,
                    instant_delta_us(filter_started_at, filter_done_at),
                    instant_delta_us(total_started_at, Instant::now()),
                    self.factors.len(),
                ));
            }
            return Ok(vec![]);
        }

        let mut current_schema = Arc::clone(&self.input_schema);
        let mut columns = filtered.columns;
        let row_count = columns.first().map_or(0, |column| column.len());
        let mut factor_eval_us = 0.0_f64;
        let mut schema_extend_us = 0.0_f64;

        for factor in &mut self.factors {
            let context = ReactiveFactorContext::new_trusted_with_invalid_value_policy(
                &current_schema,
                &columns,
                row_count,
                self.invalid_value_policy,
            );
            let output_field = factor.output_field();
            let factor_started_at = trace_enabled.then(Instant::now);
            let output_column: ArrayRef = factor.evaluate_with_context(&context)?;
            if let Some(factor_started_at) = factor_started_at {
                factor_eval_us += instant_delta_us(factor_started_at, Instant::now());
            }
            if output_column.len() != row_count {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "reactive factor output length mismatch field=[{}] expected=[{}] actual=[{}]",
                        output_field.name(),
                        row_count,
                        output_column.len()
                    ),
                });
            }
            columns.push(output_column);

            let schema_started_at = trace_enabled.then(Instant::now);
            let mut fields = current_schema.fields().iter().cloned().collect::<Vec<_>>();
            fields.push(Arc::new(output_field_for_invalid_value_policy(
                output_field,
                self.invalid_value_policy,
            )));
            current_schema = Arc::new(Schema::new(fields));
            if let Some(schema_started_at) = schema_started_at {
                schema_extend_us += instant_delta_us(schema_started_at, Instant::now());
            }
        }

        let output_started_at = trace_enabled.then(Instant::now);
        let output =
            RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
                ZippyError::Io {
                    reason: format!("failed to build reactive output batch error=[{}]", error),
                }
            })?;
        let output_view = SegmentTableView::from_record_batch(output);
        let output_done_at = trace_enabled.then(Instant::now);

        if let (
            Some(total_started_at),
            Some(filter_started_at),
            Some(filter_done_at),
            Some(output_started_at),
            Some(output_done_at),
        ) = (
            total_started_at,
            filter_started_at,
            filter_done_at,
            output_started_at,
            output_done_at,
        ) {
            write_latency_trace_line(format!(
                "zippy_reactive_latency_trace event=[on_data] engine=[{}] rows=[{}] source_emit_ns=[{}] filtered_rows=[{}] filter_projection=[{}] project_filter_us=[{:.3}] factor_eval_us=[{:.3}] schema_extend_us=[{:.3}] output_build_us=[{:.3}] total_us=[{:.3}] factor_count=[{}]",
                self.name,
                input_rows,
                source_emit_ns,
                filtered_rows,
                filtered.projection,
                instant_delta_us(filter_started_at, filter_done_at),
                factor_eval_us,
                schema_extend_us,
                instant_delta_us(output_started_at, output_done_at),
                instant_delta_us(total_started_at, output_done_at),
                self.factors.len(),
            ));
        }

        Ok(vec![output_view])
    }
    fn filter_by_id_whitelist(
        &mut self,
        table: &SegmentTableView,
    ) -> Result<FilteredReactiveColumns> {
        let column_count = self.input_schema.fields().len();
        let Some(id_filter) = &self.id_filter else {
            return Ok(FilteredReactiveColumns {
                columns: project_columns_by_position(table, column_count)?,
                projection: "identity",
            });
        };
        let id_column_index = self
            .id_column_index
            .expect("id column index must exist when id filter is configured");

        if table.num_rows() == 0 {
            return Ok(FilteredReactiveColumns {
                columns: project_columns_by_position(table, column_count)?,
                projection: "identity",
            });
        }

        let id_field = self
            .id_field
            .as_deref()
            .expect("id field must exist when id filter is configured");
        let kept_indices = collect_whitelist_indices(table, id_filter, id_column_index, id_field)?;
        let filtered_rows = table.num_rows().saturating_sub(kept_indices.len()) as u64;

        self.pending_filtered_rows += filtered_rows;

        if kept_indices.is_empty() {
            return Ok(FilteredReactiveColumns {
                columns: Vec::new(),
                projection: "empty",
            });
        }

        let columns = project_columns_by_position(table, column_count)?;
        if kept_indices.len() == table.num_rows() {
            return Ok(FilteredReactiveColumns {
                columns,
                projection: "identity",
            });
        }

        if let Some((offset, length)) = contiguous_range(&kept_indices) {
            return Ok(FilteredReactiveColumns {
                columns: columns
                    .iter()
                    .map(|column| column.slice(offset, length))
                    .collect(),
                projection: "slice",
            });
        }

        let indices = UInt32Array::from(kept_indices);
        let columns = columns
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                    reason: format!("failed to filter reactive table view error=[{}]", error),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(FilteredReactiveColumns {
            columns,
            projection: "take",
        })
    }
}

fn contiguous_range(indices: &[u32]) -> Option<(usize, usize)> {
    let first = *indices.first()?;
    for (offset, value) in indices.iter().enumerate() {
        if *value != first + offset as u32 {
            return None;
        }
    }

    Some((first as usize, indices.len()))
}

fn collect_whitelist_indices(
    table: &SegmentTableView,
    id_filter: &HashSet<String>,
    id_column_index: usize,
    id_field: &str,
) -> Result<Vec<u32>> {
    if let Some(row_view) = table.as_segment_row_view() {
        let mut kept_indices = Vec::with_capacity(table.num_rows());
        for row_index in 0..table.num_rows() {
            match row_view
                .row_span()
                .utf8_cell_value_at(row_index, id_column_index)
            {
                Ok(Some(id_value)) if id_filter.contains(id_value) => {
                    kept_indices.push(row_index as u32);
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(ZippyError::Io {
                        reason: format!(
                            "failed to read reactive id cell field=[{}] error=[{}]",
                            id_field, error
                        ),
                    });
                }
            }
        }

        return Ok(kept_indices);
    }

    let id_column = table.column_at(id_column_index)?;
    let id_column = string_array(&id_column, id_field)?;
    let mut kept_indices = Vec::with_capacity(table.num_rows());

    for row_index in 0..table.num_rows() {
        if id_column.is_null(row_index) {
            continue;
        }

        let id_value = id_column.value(row_index);
        if id_filter.contains(id_value) {
            kept_indices.push(row_index as u32);
        }
    }

    Ok(kept_indices)
}

fn build_output_schema(
    input_schema: &SchemaRef,
    factors: &[Box<dyn ReactiveFactor>],
    invalid_value_policy: ReactiveInvalidValuePolicy,
) -> Result<SchemaRef> {
    let mut field_names = input_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();

    for factor in factors {
        let output_field = factor.output_field();
        let inserted = field_names.insert(output_field.name().clone());

        if !inserted {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "duplicate reactive output field field=[{}]",
                    output_field.name()
                ),
            });
        }

        fields.push(Arc::new(output_field_for_invalid_value_policy(
            output_field,
            invalid_value_policy,
        )));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn output_field_for_invalid_value_policy(
    field: arrow::datatypes::Field,
    invalid_value_policy: ReactiveInvalidValuePolicy,
) -> arrow::datatypes::Field {
    if invalid_value_policy != ReactiveInvalidValuePolicy::Skip || field.is_nullable() {
        return field;
    }

    arrow::datatypes::Field::new(field.name(), field.data_type().clone(), true)
        .with_metadata(field.metadata().clone())
}

fn validate_id_field(schema: &Schema, id_field: &str) -> Result<usize> {
    let index = schema
        .index_of(id_field)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_field),
        })?;
    let field = schema.field(index);
    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_field),
        });
    }

    Ok(index)
}
