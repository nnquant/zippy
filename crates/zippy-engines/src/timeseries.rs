use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, LateDataPolicy, Result, SchemaRef, ZippyError};
use zippy_operators::{AggregationKind, AggregationSpec};

const UTC_TIMEZONE: &str = "UTC";
const VWAP_DENOMINATOR_ZERO_STATUS: &str = "vwap denominator is zero";

/// Aggregate per-id float64 inputs into fixed-width UTC nanosecond windows.
pub struct TimeSeriesEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    window_ns: i64,
    late_data_policy: LateDataPolicy,
    specs: Vec<Box<dyn AggregationSpec>>,
    open_windows: BTreeMap<String, OpenWindow>,
    last_dt_by_id: BTreeMap<String, i64>,
    pending_late_rows: u64,
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
    /// :param dt_column: UTC nanosecond timestamp column name.
    /// :type dt_column: &str
    /// :param window_ns: Window size in nanoseconds.
    /// :type window_ns: i64
    /// :param late_data_policy: Late-row handling mode.
    /// :type late_data_policy: LateDataPolicy
    /// :param specs: Aggregation specs appended in stable order.
    /// :type specs: Vec<Box<dyn AggregationSpec>>
    /// :returns: Initialized time-series engine.
    /// :rtype: Result<TimeSeriesEngine>
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        window_ns: i64,
        late_data_policy: LateDataPolicy,
        specs: Vec<Box<dyn AggregationSpec>>,
    ) -> Result<Self> {
        if window_ns <= 0 {
            return Err(ZippyError::InvalidConfig {
                reason: format!("window size must be positive window_ns=[{}]", window_ns),
            });
        }

        validate_id_column(input_schema.as_ref(), id_column)?;
        validate_dt_column(input_schema.as_ref(), dt_column)?;

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

        for spec in &specs {
            validate_value_column(input_schema.as_ref(), spec.primary_column())?;

            if let Some(secondary_column) = spec.secondary_column() {
                validate_value_column(input_schema.as_ref(), secondary_column)?;
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

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema: Arc::new(Schema::new(output_fields)),
            id_column: id_column.to_string(),
            dt_column: dt_column.to_string(),
            window_ns,
            late_data_policy,
            specs,
            open_windows: BTreeMap::new(),
            last_dt_by_id: BTreeMap::new(),
            pending_late_rows: 0,
        })
    }

    fn build_output_batch(&self, windows: Vec<OpenWindow>) -> Result<RecordBatch> {
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

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to build timeseries output batch error=[{}]", error),
            }
        })
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

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }

        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let ids = extract_id_array(&batch, &self.id_column)?;
        let dts = extract_dt_array(&batch, &self.dt_column)?;
        let spec_inputs = self
            .specs
            .iter()
            .map(|spec| {
                Ok(SpecInputArrays {
                    primary: extract_value_array(&batch, spec.primary_column())?,
                    secondary: match spec.secondary_column() {
                        Some(column) => Some(extract_value_array(&batch, column)?),
                        None => None,
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if matches!(self.late_data_policy, LateDataPolicy::Reject) {
            validate_non_decreasing_dts(ids, dts, &self.last_dt_by_id)?;
        }

        let mut completed = Vec::new();
        let mut batch_last_dt_by_id = BTreeMap::new();

        for row_index in 0..batch.num_rows() {
            let id = ids.value(row_index).to_string();
            let dt = dts.value(row_index);

            if matches!(self.late_data_policy, LateDataPolicy::DropWithMetric)
                && is_late_row(&id, dt, &batch_last_dt_by_id, &self.last_dt_by_id)
            {
                self.pending_late_rows += 1;
                continue;
            }

            let window_start = align_window_start(dt, self.window_ns);
            let window_end =
                window_start
                    .checked_add(self.window_ns)
                    .ok_or(ZippyError::InvalidState {
                        status: "window end overflow",
                    })?;

            match self.open_windows.remove(&id) {
                Some(mut open_window) if open_window.window_start == window_start => {
                    open_window.update(&self.specs, &spec_inputs, row_index);
                    self.open_windows.insert(id.clone(), open_window);
                }
                Some(open_window) => {
                    completed.push(open_window);
                    let mut next_window =
                        OpenWindow::new(id.clone(), window_start, window_end, self.specs.len());
                    next_window.update(&self.specs, &spec_inputs, row_index);
                    self.open_windows.insert(id.clone(), next_window);
                }
                None => {
                    let mut open_window =
                        OpenWindow::new(id.clone(), window_start, window_end, self.specs.len());
                    open_window.update(&self.specs, &spec_inputs, row_index);
                    self.open_windows.insert(id.clone(), open_window);
                }
            }

            batch_last_dt_by_id.insert(id.clone(), dt);
            self.last_dt_by_id.insert(id, dt);
        }

        if completed.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![self.build_output_batch(completed)?])
        }
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        if self.open_windows.is_empty() {
            return Ok(vec![]);
        }

        let windows = std::mem::take(&mut self.open_windows)
            .into_values()
            .collect::<Vec<_>>();

        Ok(vec![self.build_output_batch(windows)?])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: self.pending_late_rows,
        };
        self.pending_late_rows = 0;
        delta
    }
}

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
        spec_inputs: &[SpecInputArrays<'_>],
        row_index: usize,
    ) {
        for (spec_index, spec) in specs.iter().enumerate() {
            let value = spec_inputs[spec_index].primary.value(row_index);

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
                    let weight = spec_inputs[spec_index]
                        .secondary
                        .expect("vwap requires a secondary input column")
                        .value(row_index);

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

struct SpecInputArrays<'a> {
    primary: &'a Float64Array,
    secondary: Option<&'a Float64Array>,
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
            reason: format!("missing utc nanosecond dt field field=[{}]", dt_column),
        })?;

    if !matches!(
        field.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
            if timezone.as_ref() == UTC_TIMEZONE
    ) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be utc nanosecond timestamp field=[{}]",
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

fn extract_id_array<'a>(batch: &'a RecordBatch, id_column: &str) -> Result<&'a StringArray> {
    let id_index = batch
        .schema()
        .index_of(id_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_column),
        })?;
    let ids = batch
        .column(id_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_column),
        })?;

    if ids.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field contains nulls field=[{}]", id_column),
        });
    }

    Ok(ids)
}

fn extract_dt_array<'a>(
    batch: &'a RecordBatch,
    dt_column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let dt_index = batch
        .schema()
        .index_of(dt_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utc nanosecond dt field field=[{}]", dt_column),
        })?;
    let dts = batch
        .column(dt_index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be utc nanosecond timestamp field=[{}]",
                dt_column
            ),
        })?;

    if dts.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("dt field contains nulls field=[{}]", dt_column),
        });
    }

    Ok(dts)
}

fn extract_value_array<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a Float64Array> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 value field field=[{}]", column),
        })?;
    let values = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", column),
        })?;

    if values.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("value field contains nulls field=[{}]", column),
        });
    }

    Ok(values)
}

fn validate_non_decreasing_dts(
    ids: &StringArray,
    dts: &TimestampNanosecondArray,
    last_dt_by_id: &BTreeMap<String, i64>,
) -> Result<()> {
    let mut batch_last_dt_by_id = BTreeMap::new();

    for row_index in 0..ids.len() {
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
