use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{
    Engine, EngineMetricsDelta, LateDataPolicy, Result, SchemaRef, SegmentTableView, ZippyError,
};
use zippy_operators::{AggregationKind, AggregationSpec, ExpressionSpec, ReactiveFactor};

use crate::table_view::{
    float64_array, record_batch_from_table_rows, string_array, timestamp_ns_array,
};

const UTC_TIMEZONE: &str = "UTC";
const VWAP_DENOMINATOR_ZERO_STATUS: &str = "vwap denominator is zero";

/// Aggregate per-id float64 inputs into fixed-width nanosecond windows.
pub struct TimeSeriesEngine {
    name: String,
    input_schema: SchemaRef,
    pre_schema: SchemaRef,
    agg_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    id_filter: Option<HashSet<String>>,
    window_ns: i64,
    late_data_policy: LateDataPolicy,
    specs: Vec<Box<dyn AggregationSpec>>,
    pre_factors: Vec<Box<dyn ReactiveFactor>>,
    post_factors: Vec<Box<dyn ReactiveFactor>>,
    open_windows: BTreeMap<String, OpenWindow>,
    last_dt_by_id: BTreeMap<String, i64>,
    pending_late_rows: u64,
    pending_filtered_rows: u64,
}

impl TimeSeriesEngine {
    /// Create a new fixed-window aggregation engine.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param id_column: Utf8 identifier column name.
    /// :type id_column: &str
    /// :param dt_column: Timezone-aware nanosecond timestamp column name.
    /// :type dt_column: &str
    /// :param window_ns: Window size in nanoseconds.
    /// :type window_ns: i64
    /// :param late_data_policy: Late-row handling mode.
    /// :type late_data_policy: LateDataPolicy
    /// :param specs: Aggregation specs appended in stable order.
    /// :type specs: Vec<Box<dyn AggregationSpec>>
    /// :param pre_factor_specs: Expression specs evaluated on accepted input rows.
    /// :type pre_factor_specs: Vec<ExpressionSpec>
    /// :param post_factor_specs: Expression specs evaluated on aggregate outputs.
    /// :type post_factor_specs: Vec<ExpressionSpec>
    /// :returns: Initialized time-series engine.
    /// :rtype: Result<TimeSeriesEngine>
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        window_ns: i64,
        late_data_policy: LateDataPolicy,
        specs: Vec<Box<dyn AggregationSpec>>,
        pre_factor_specs: Vec<ExpressionSpec>,
        post_factor_specs: Vec<ExpressionSpec>,
    ) -> Result<Self> {
        Self::new_with_id_filter(
            name,
            input_schema,
            id_column,
            dt_column,
            window_ns,
            late_data_policy,
            specs,
            pre_factor_specs,
            post_factor_specs,
            None,
        )
    }

    /// Create a new fixed-window aggregation engine with an optional id whitelist.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param id_column: Utf8 identifier column name.
    /// :type id_column: &str
    /// :param dt_column: Timezone-aware nanosecond timestamp column name.
    /// :type dt_column: &str
    /// :param window_ns: Window size in nanoseconds.
    /// :type window_ns: i64
    /// :param late_data_policy: Late-row handling mode.
    /// :type late_data_policy: LateDataPolicy
    /// :param specs: Aggregation specs appended in stable order.
    /// :type specs: Vec<Box<dyn AggregationSpec>>
    /// :param pre_factor_specs: Expression specs evaluated on accepted input rows.
    /// :type pre_factor_specs: Vec<ExpressionSpec>
    /// :param post_factor_specs: Expression specs evaluated on aggregate outputs.
    /// :type post_factor_specs: Vec<ExpressionSpec>
    /// :param id_filter: Optional exact-match whitelist for the id column.
    /// :type id_filter: Option<Vec<String>>
    /// :returns: Initialized time-series engine.
    /// :rtype: Result<TimeSeriesEngine>
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_id_filter(
        name: impl Into<String>,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        window_ns: i64,
        late_data_policy: LateDataPolicy,
        specs: Vec<Box<dyn AggregationSpec>>,
        pre_factor_specs: Vec<ExpressionSpec>,
        post_factor_specs: Vec<ExpressionSpec>,
        id_filter: Option<Vec<String>>,
    ) -> Result<Self> {
        if window_ns <= 0 {
            return Err(ZippyError::InvalidConfig {
                reason: format!("window size must be positive window_ns=[{}]", window_ns),
            });
        }

        validate_id_column(input_schema.as_ref(), id_column)?;
        validate_dt_column(input_schema.as_ref(), dt_column)?;

        let (pre_factors, pre_schema) =
            build_expression_factors(Arc::clone(&input_schema), pre_factor_specs, "pre factor")?;
        let agg_schema = build_aggregation_schema(&input_schema, &pre_schema, id_column, &specs)?;
        let (post_factors, output_schema) =
            build_expression_factors(Arc::clone(&agg_schema), post_factor_specs, "post factor")?;
        let id_filter = normalize_id_filter(id_filter)?;

        Ok(Self {
            name: name.into(),
            input_schema,
            pre_schema,
            agg_schema,
            output_schema,
            id_column: id_column.to_string(),
            dt_column: dt_column.to_string(),
            id_filter,
            window_ns,
            late_data_policy,
            specs,
            pre_factors,
            post_factors,
            open_windows: BTreeMap::new(),
            last_dt_by_id: BTreeMap::new(),
            pending_late_rows: 0,
            pending_filtered_rows: 0,
        })
    }

    fn build_agg_batch(&self, windows: Vec<OpenWindow>) -> Result<RecordBatch> {
        let ids = windows
            .iter()
            .map(|window| window.id.as_str())
            .collect::<Vec<_>>();
        let window_starts = windows
            .iter()
            .map(|window| window.window_start)
            .collect::<Vec<_>>();
        let window_ends = windows
            .iter()
            .map(|window| window.window_end)
            .collect::<Vec<_>>();

        let mut columns = vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(window_starts).with_timezone(UTC_TIMEZONE))
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(window_ends).with_timezone(UTC_TIMEZONE))
                as ArrayRef,
        ];

        for (spec_index, spec) in self.specs.iter().enumerate() {
            let values = windows
                .iter()
                .map(|window| window.output_value(spec.as_ref(), spec_index))
                .collect::<Result<Vec<_>>>()?;
            columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }

        RecordBatch::try_new(Arc::clone(&self.agg_schema), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build timeseries aggregate batch error=[{}]",
                    error
                ),
            }
        })
    }

    fn finalize_windows(&mut self, windows: Vec<OpenWindow>) -> Result<RecordBatch> {
        let agg_batch = self.build_agg_batch(windows)?;
        apply_reactive_factors(
            &agg_batch,
            &mut self.post_factors,
            &self.output_schema,
            "timeseries post",
        )
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

impl Engine for TimeSeriesEngine {
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
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }

        if table.num_rows() == 0 {
            return Ok(vec![]);
        }

        let id_array = table.column(&self.id_column)?;
        let ids = checked_id_array(&id_array, &self.id_column)?;
        let dt_array = table.column(&self.dt_column)?;
        let dts = checked_dt_array(&dt_array, &self.dt_column)?;
        let candidate_rows = self.collect_id_filter_rows(ids, table.num_rows());

        if candidate_rows.is_empty() {
            return Ok(vec![]);
        }

        let accepted_rows = match self.late_data_policy {
            LateDataPolicy::Reject => {
                validate_non_decreasing_dts_for_rows(
                    ids,
                    dts,
                    &candidate_rows,
                    &self.last_dt_by_id,
                )?;
                candidate_rows
            }
            LateDataPolicy::DropWithMetric => collect_accepted_rows_for_rows(
                ids,
                dts,
                &candidate_rows,
                &self.last_dt_by_id,
                &mut self.pending_late_rows,
            ),
        };

        if accepted_rows.is_empty() {
            return Ok(vec![]);
        }

        let processed_input = if self.pre_factors.is_empty() {
            ProcessedInput::Table {
                ids,
                dts,
                row_indices: accepted_rows,
                spec_inputs: self
                    .specs
                    .iter()
                    .map(|spec| {
                        Ok(SpecInputArrays {
                            primary: extract_value_array_from_table(&table, spec.primary_column())?,
                            secondary: match spec.secondary_column() {
                                Some(column) => {
                                    Some(extract_value_array_from_table(&table, column)?)
                                }
                                None => None,
                            },
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            }
        } else {
            let accepted_batch = record_batch_from_table_rows(
                &table,
                &self.input_schema,
                &accepted_rows,
                "timeseries accepted",
            )?;
            let processed_batch = apply_reactive_factors(
                &accepted_batch,
                &mut self.pre_factors,
                &self.pre_schema,
                "timeseries pre",
            )?;
            let row_indices = (0..processed_batch.num_rows()).collect::<Vec<_>>();
            let spec_inputs = self
                .specs
                .iter()
                .map(|spec| {
                    Ok(SpecInputArrays {
                        primary: extract_value_array(&processed_batch, spec.primary_column())?,
                        secondary: match spec.secondary_column() {
                            Some(column) => Some(extract_value_array(&processed_batch, column)?),
                            None => None,
                        },
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            ProcessedInput::Batch {
                ids: extract_id_array(&processed_batch, &self.id_column)?,
                dts: extract_dt_array(&processed_batch, &self.dt_column)?,
                id_column: self.id_column.clone(),
                dt_column: self.dt_column.clone(),
                row_indices,
                spec_inputs,
            }
        };

        let mut completed = Vec::new();
        let mut next_open_windows = self.open_windows.clone();
        let mut next_last_dt_by_id = self.last_dt_by_id.clone();

        for row_index in processed_input.row_indices() {
            let id = processed_input.id_value(row_index)?.to_string();
            let dt = processed_input.dt_value(row_index)?;
            let window_start = align_window_start(dt, self.window_ns);
            let window_end =
                window_start
                    .checked_add(self.window_ns)
                    .ok_or(ZippyError::InvalidState {
                        status: "window end overflow",
                    })?;

            match next_open_windows.remove(&id) {
                Some(mut open_window) if open_window.window_start == window_start => {
                    open_window.update(&self.specs, processed_input.spec_inputs(), row_index)?;
                    next_open_windows.insert(id.clone(), open_window);
                }
                Some(open_window) => {
                    completed.push(open_window);
                    let mut next_window =
                        OpenWindow::new(id.clone(), window_start, window_end, self.specs.len());
                    next_window.update(&self.specs, processed_input.spec_inputs(), row_index)?;
                    next_open_windows.insert(id.clone(), next_window);
                }
                None => {
                    let mut open_window =
                        OpenWindow::new(id.clone(), window_start, window_end, self.specs.len());
                    open_window.update(&self.specs, processed_input.spec_inputs(), row_index)?;
                    next_open_windows.insert(id.clone(), open_window);
                }
            }

            next_last_dt_by_id.insert(id, dt);
        }

        if completed.is_empty() {
            self.open_windows = next_open_windows;
            self.last_dt_by_id = next_last_dt_by_id;
            Ok(vec![])
        } else {
            let output = self.finalize_windows(completed)?;
            self.open_windows = next_open_windows;
            self.last_dt_by_id = next_last_dt_by_id;
            Ok(vec![SegmentTableView::from_record_batch(output)])
        }
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        if self.open_windows.is_empty() {
            return Ok(vec![]);
        }

        let windows = self.open_windows.values().cloned().collect::<Vec<_>>();
        let output = self.finalize_windows(windows)?;
        self.open_windows.clear();

        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: self.pending_late_rows,
            filtered_rows_total: self.pending_filtered_rows,
        };
        self.pending_late_rows = 0;
        self.pending_filtered_rows = 0;
        delta
    }
}

impl TimeSeriesEngine {
    fn collect_id_filter_rows(&mut self, ids: &StringArray, row_count: usize) -> Vec<u32> {
        let Some(id_filter) = &self.id_filter else {
            return (0..row_count).map(|row_index| row_index as u32).collect();
        };

        let mut kept_rows = Vec::with_capacity(row_count);
        let mut filtered_rows = 0u64;

        for row_index in 0..row_count {
            let id = ids.value(row_index);

            if id_filter.contains(id) {
                kept_rows.push(row_index as u32);
            } else {
                filtered_rows += 1;
            }
        }

        self.pending_filtered_rows += filtered_rows;
        kept_rows
    }
}

#[derive(Clone)]
struct OpenWindow {
    id: String,
    window_start: i64,
    window_end: i64,
    values: Vec<f64>,
    secondary_values: Vec<f64>,
    initialized: Vec<bool>,
}

impl OpenWindow {
    fn new(id: String, window_start: i64, window_end: i64, spec_count: usize) -> Self {
        Self {
            id,
            window_start,
            window_end,
            values: vec![0.0; spec_count],
            secondary_values: vec![0.0; spec_count],
            initialized: vec![false; spec_count],
        }
    }

    fn update(
        &mut self,
        specs: &[Box<dyn AggregationSpec>],
        spec_inputs: &[SpecInputArrays],
        row_index: usize,
    ) -> Result<()> {
        for (spec_index, spec) in specs.iter().enumerate() {
            let value = spec_inputs[spec_index].primary_value(row_index)?;

            match spec.kind() {
                AggregationKind::First => {
                    if !self.initialized[spec_index] {
                        self.values[spec_index] = value;
                        self.initialized[spec_index] = true;
                    }
                }
                AggregationKind::Last => {
                    self.values[spec_index] = value;
                    self.initialized[spec_index] = true;
                }
                AggregationKind::Sum => {
                    if self.initialized[spec_index] {
                        self.values[spec_index] += value;
                    } else {
                        self.values[spec_index] = value;
                        self.initialized[spec_index] = true;
                    }
                }
                AggregationKind::Max => {
                    if self.initialized[spec_index] {
                        self.values[spec_index] = self.values[spec_index].max(value);
                    } else {
                        self.values[spec_index] = value;
                        self.initialized[spec_index] = true;
                    }
                }
                AggregationKind::Min => {
                    if self.initialized[spec_index] {
                        self.values[spec_index] = self.values[spec_index].min(value);
                    } else {
                        self.values[spec_index] = value;
                        self.initialized[spec_index] = true;
                    }
                }
                AggregationKind::Count => {
                    if self.initialized[spec_index] {
                        self.values[spec_index] += 1.0;
                    } else {
                        self.values[spec_index] = 1.0;
                        self.initialized[spec_index] = true;
                    }
                }
                AggregationKind::Vwap => {
                    let weight = spec_inputs[spec_index].secondary_value(row_index)?;

                    if self.initialized[spec_index] {
                        self.values[spec_index] += value * weight;
                        self.secondary_values[spec_index] += weight;
                    } else {
                        self.values[spec_index] = value * weight;
                        self.secondary_values[spec_index] = weight;
                        self.initialized[spec_index] = true;
                    }
                }
            }
        }
        Ok(())
    }

    fn output_value(&self, spec: &dyn AggregationSpec, spec_index: usize) -> Result<f64> {
        match spec.kind() {
            AggregationKind::Vwap => {
                let denominator = self.secondary_values[spec_index];

                if denominator == 0.0 {
                    return Err(ZippyError::InvalidState {
                        status: VWAP_DENOMINATOR_ZERO_STATUS,
                    });
                }

                Ok(self.values[spec_index] / denominator)
            }
            _ => Ok(self.values[spec_index]),
        }
    }
}

struct SpecInputArrays {
    primary: ArrayRef,
    secondary: Option<ArrayRef>,
}

impl SpecInputArrays {
    fn primary_value(&self, row_index: usize) -> Result<f64> {
        Ok(checked_value_array(&self.primary, "primary")?.value(row_index))
    }

    fn secondary_value(&self, row_index: usize) -> Result<f64> {
        let secondary = self.secondary.as_ref().ok_or(ZippyError::InvalidState {
            status: "vwap requires a secondary input column",
        })?;
        Ok(checked_value_array(secondary, "secondary")?.value(row_index))
    }
}

enum ProcessedInput<'a> {
    Table {
        ids: &'a StringArray,
        dts: &'a TimestampNanosecondArray,
        row_indices: Vec<u32>,
        spec_inputs: Vec<SpecInputArrays>,
    },
    Batch {
        ids: ArrayRef,
        dts: ArrayRef,
        id_column: String,
        dt_column: String,
        row_indices: Vec<usize>,
        spec_inputs: Vec<SpecInputArrays>,
    },
}

impl<'a> ProcessedInput<'a> {
    fn row_indices(&self) -> Vec<usize> {
        match self {
            Self::Table { row_indices, .. } => row_indices
                .iter()
                .map(|row_index| *row_index as usize)
                .collect(),
            Self::Batch { row_indices, .. } => row_indices.clone(),
        }
    }

    fn id_value(&'a self, row_index: usize) -> Result<&'a str> {
        match self {
            Self::Table { ids, .. } => Ok(ids.value(row_index)),
            Self::Batch { ids, id_column, .. } => {
                Ok(checked_id_array(ids, id_column)?.value(row_index))
            }
        }
    }

    fn dt_value(&self, row_index: usize) -> Result<i64> {
        match self {
            Self::Table { dts, .. } => Ok(dts.value(row_index)),
            Self::Batch { dts, dt_column, .. } => {
                Ok(checked_dt_array(dts, dt_column)?.value(row_index))
            }
        }
    }

    fn spec_inputs(&self) -> &[SpecInputArrays] {
        match self {
            Self::Table { spec_inputs, .. } | Self::Batch { spec_inputs, .. } => spec_inputs,
        }
    }
}

fn build_expression_factors(
    base_schema: SchemaRef,
    specs: Vec<ExpressionSpec>,
    stage_label: &str,
) -> Result<(Vec<Box<dyn ReactiveFactor>>, SchemaRef)> {
    let mut field_names = base_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    let mut fields = base_schema.fields().iter().cloned().collect::<Vec<_>>();
    let mut current_schema = Arc::clone(&base_schema);
    let mut factors = Vec::with_capacity(specs.len());

    for spec in specs {
        let factor = spec
            .build(current_schema.as_ref())
            .map_err(map_timeseries_expression_error)?;
        let output_field = factor.output_field();
        let inserted = field_names.insert(output_field.name().clone());

        if !inserted {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "duplicate {} output field field=[{}]",
                    stage_label,
                    output_field.name()
                ),
            });
        }

        fields.push(Arc::new(output_field));
        current_schema = Arc::new(Schema::new(fields.clone()));
        factors.push(factor);
    }

    Ok((factors, current_schema))
}

fn map_timeseries_expression_error(error: ZippyError) -> ZippyError {
    match error {
        ZippyError::InvalidConfig { reason }
            if reason.contains("stateful expression function requires build_reactive_plan") =>
        {
            ZippyError::InvalidConfig {
                reason: "stateful TS_* functions are only supported inside ReactiveStateEngine"
                    .to_string(),
            }
        }
        other => other,
    }
}

fn build_aggregation_schema(
    input_schema: &SchemaRef,
    pre_schema: &SchemaRef,
    id_column: &str,
    specs: &[Box<dyn AggregationSpec>],
) -> Result<SchemaRef> {
    let mut output_names = HashSet::from([
        id_column.to_string(),
        "window_start".to_string(),
        "window_end".to_string(),
    ]);
    let mut output_fields = vec![
        Arc::new(
            input_schema
                .field_with_name(id_column)
                .map_err(|_| ZippyError::SchemaMismatch {
                    reason: format!("missing utf8 id field field=[{}]", id_column),
                })?
                .clone(),
        ),
        Arc::new(Field::new(
            "window_start",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIMEZONE.into())),
            false,
        )),
        Arc::new(Field::new(
            "window_end",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TIMEZONE.into())),
            false,
        )),
    ];

    for spec in specs {
        validate_value_column(pre_schema.as_ref(), spec.primary_column())?;

        if let Some(secondary_column) = spec.secondary_column() {
            validate_value_column(pre_schema.as_ref(), secondary_column)?;
        }

        let output_field = spec.output_field();
        let inserted = output_names.insert(output_field.name().clone());

        if !inserted {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "duplicate aggregation output field field=[{}]",
                    output_field.name()
                ),
            });
        }

        output_fields.push(Arc::new(output_field));
    }

    Ok(Arc::new(Schema::new(output_fields)))
}

fn apply_reactive_factors(
    batch: &RecordBatch,
    factors: &mut [Box<dyn ReactiveFactor>],
    output_schema: &SchemaRef,
    stage_label: &str,
) -> Result<RecordBatch> {
    if factors.is_empty() {
        return Ok(batch.clone());
    }

    let mut columns = batch.columns().to_vec();
    let mut current_schema = batch.schema();

    for factor in factors {
        let current_batch = RecordBatch::try_new(Arc::clone(&current_schema), columns.clone())
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to build {} intermediate batch error=[{}]",
                    stage_label, error
                ),
            })?;
        let output_field = factor.output_field();
        let output_column = factor.evaluate(&current_batch)?;
        columns.push(output_column);

        let mut fields = current_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(output_field));
        current_schema = Arc::new(Schema::new(fields));
    }

    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build {} output batch error=[{}]",
            stage_label, error
        ),
    })
}

fn collect_accepted_rows_for_rows(
    ids: &StringArray,
    dts: &TimestampNanosecondArray,
    row_indices: &[u32],
    last_dt_by_id: &BTreeMap<String, i64>,
    pending_late_rows: &mut u64,
) -> Vec<u32> {
    let mut accepted_rows = Vec::with_capacity(row_indices.len());
    let mut batch_last_dt_by_id = BTreeMap::new();

    for row_index in row_indices.iter().map(|row_index| *row_index as usize) {
        let id = ids.value(row_index);
        let dt = dts.value(row_index);

        if is_late_row(id, dt, &batch_last_dt_by_id, last_dt_by_id) {
            *pending_late_rows += 1;
            continue;
        }

        batch_last_dt_by_id.insert(id.to_string(), dt);
        accepted_rows.push(row_index as u32);
    }

    accepted_rows
}

fn is_late_row(
    id: &str,
    dt: i64,
    batch_last_dt_by_id: &BTreeMap<String, i64>,
    last_dt_by_id: &BTreeMap<String, i64>,
) -> bool {
    let last_dt = batch_last_dt_by_id
        .get(id)
        .copied()
        .or_else(|| last_dt_by_id.get(id).copied());

    matches!(last_dt, Some(last_dt) if dt < last_dt)
}

fn validate_id_column(schema: &Schema, id_column: &str) -> Result<()> {
    let field = schema
        .field_with_name(id_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_column),
        })?;

    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_column),
        });
    }

    Ok(())
}

fn validate_dt_column(schema: &Schema, dt_column: &str) -> Result<()> {
    let field = schema
        .field_with_name(dt_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                dt_column
            ),
        })?;

    if !matches!(
        field.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
    ) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                dt_column
            ),
        });
    }

    Ok(())
}

fn validate_value_column(schema: &Schema, column: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 value field field=[{}]", column),
        })?;

    if field.data_type() != &DataType::Float64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", column),
        });
    }

    Ok(())
}

fn extract_id_array(batch: &RecordBatch, id_column: &str) -> Result<ArrayRef> {
    let id_index = batch
        .schema()
        .index_of(id_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_column),
        })?;
    let array = Arc::clone(batch.column(id_index));
    checked_id_array(&array, id_column)?;
    Ok(array)
}

fn checked_id_array<'a>(array: &'a ArrayRef, id_column: &str) -> Result<&'a StringArray> {
    let ids = string_array(array, id_column)?;

    if ids.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field contains nulls field=[{}]", id_column),
        });
    }

    Ok(ids)
}

fn extract_dt_array(batch: &RecordBatch, dt_column: &str) -> Result<ArrayRef> {
    let dt_index = batch
        .schema()
        .index_of(dt_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                dt_column
            ),
        })?;
    let array = Arc::clone(batch.column(dt_index));
    checked_dt_array(&array, dt_column)?;
    Ok(array)
}

fn checked_dt_array<'a>(
    array: &'a ArrayRef,
    dt_column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let dts = timestamp_ns_array(array, dt_column)?;

    if dts.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("dt field contains nulls field=[{}]", dt_column),
        });
    }

    Ok(dts)
}

fn extract_value_array(batch: &RecordBatch, column: &str) -> Result<ArrayRef> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 value field field=[{}]", column),
        })?;
    let array = Arc::clone(batch.column(index));
    checked_value_array(&array, column)?;
    Ok(array)
}

fn extract_value_array_from_table(table: &SegmentTableView, column: &str) -> Result<ArrayRef> {
    let array = table.column(column)?;
    checked_value_array(&array, column)?;
    Ok(array)
}

fn checked_value_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Float64Array> {
    let values = float64_array(array, column)?;

    if values.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("value field contains nulls field=[{}]", column),
        });
    }

    Ok(values)
}

fn validate_non_decreasing_dts_for_rows(
    ids: &StringArray,
    dts: &TimestampNanosecondArray,
    row_indices: &[u32],
    last_dt_by_id: &BTreeMap<String, i64>,
) -> Result<()> {
    let mut batch_last_dt_by_id = BTreeMap::new();

    for row_index in row_indices.iter().map(|row_index| *row_index as usize) {
        let id = ids.value(row_index);
        let dt = dts.value(row_index);
        let last_dt = batch_last_dt_by_id
            .get(id)
            .copied()
            .or_else(|| last_dt_by_id.get(id).copied());

        if let Some(last_dt) = last_dt {
            if dt < last_dt {
                return Err(ZippyError::LateData { dt, last_dt });
            }
        }

        batch_last_dt_by_id.insert(id.to_string(), dt);
    }

    Ok(())
}

fn align_window_start(dt: i64, window_ns: i64) -> i64 {
    dt.div_euclid(window_ns) * window_ns
}
