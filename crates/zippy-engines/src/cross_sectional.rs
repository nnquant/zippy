use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, TimestampNanosecondArray, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, LateDataPolicy, Result, SchemaRef, ZippyError};
use zippy_operators::CrossSectionalFactor;

const UTC_TIMEZONE: &str = "UTC";

/// Evaluate cross-sectional factors on event-time aligned buckets.
pub struct CrossSectionalEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    trigger_interval: i64,
    late_data_policy: LateDataPolicy,
    factors: Vec<Box<dyn CrossSectionalFactor>>,
    current_bucket_start: Option<i64>,
    current_rows: BTreeMap<String, OwnedRow>,
    last_closed_bucket_start: Option<i64>,
    pending_late_rows: u64,
}

impl CrossSectionalEngine {
    /// Create a new cross-sectional engine.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        trigger_interval: i64,
        late_data_policy: LateDataPolicy,
        mut factors: Vec<Box<dyn CrossSectionalFactor>>,
    ) -> Result<Self> {
        if trigger_interval <= 0 {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "trigger interval must be positive trigger_interval=[{}]",
                    trigger_interval
                ),
            });
        }

        if factors.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "cross-sectional engine requires at least one factor".to_string(),
            });
        }

        validate_id_column(input_schema.as_ref(), id_column)?;
        validate_dt_column(input_schema.as_ref(), dt_column)?;
        if late_data_policy != LateDataPolicy::Reject {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "cross-sectional engine only supports late_data_policy=[reject] actual=[{:?}]",
                    late_data_policy
                ),
            });
        }

        let output_schema = build_output_schema(&input_schema, id_column, dt_column, &factors)?;
        let empty_batch = RecordBatch::new_empty(Arc::clone(&input_schema));
        for factor in &mut factors {
            factor.evaluate(&empty_batch)?;
        }

        Ok(Self {
            name: name.into(),
            input_schema,
            output_schema,
            id_column: id_column.to_string(),
            dt_column: dt_column.to_string(),
            trigger_interval,
            late_data_policy,
            factors,
            current_bucket_start: None,
            current_rows: BTreeMap::new(),
            last_closed_bucket_start: None,
            pending_late_rows: 0,
        })
    }

    fn finalize_bucket(
        &mut self,
        bucket_start: i64,
        rows: &BTreeMap<String, OwnedRow>,
    ) -> Result<RecordBatch> {
        let bucket_batch = build_bucket_batch(&self.input_schema, rows.values())?;
        let mut columns = vec![
            Arc::new(StringArray::from(
                rows.keys().map(|id| id.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![bucket_start; rows.len()])
                    .with_timezone(UTC_TIMEZONE),
            ) as ArrayRef,
        ];

        for factor in &mut self.factors {
            columns.push(factor.evaluate(&bucket_batch)?);
        }

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build cross-sectional output batch error=[{}]",
                    error
                ),
            }
        })
    }

    fn handle_late_row(
        &self,
        dt: i64,
        current_bucket_start: Option<i64>,
        last_closed_bucket_start: Option<i64>,
        pending_late_rows: &mut u64,
    ) -> Result<()> {
        match self.late_data_policy {
            LateDataPolicy::Reject => Err(ZippyError::LateData {
                dt,
                last_dt: current_bucket_start.or(last_closed_bucket_start).unwrap_or(dt),
            }),
            LateDataPolicy::DropWithMetric => {
                *pending_late_rows += 1;
                Ok(())
            }
        }
    }
}

impl Engine for CrossSectionalEngine {
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
        let mut outputs = Vec::new();
        let mut next_current_bucket_start = self.current_bucket_start;
        let mut next_current_rows = self.current_rows.clone();
        let mut next_last_closed_bucket_start = self.last_closed_bucket_start;
        let mut next_pending_late_rows = self.pending_late_rows;
        let mut row_indices = (0..batch.num_rows()).collect::<Vec<_>>();

        row_indices.sort_by_key(|row_index| {
            (
                align_bucket_start(dts.value(*row_index), self.trigger_interval),
                *row_index,
            )
        });

        for row_index in row_indices {
            let id = ids.value(row_index).to_string();
            let dt = dts.value(row_index);
            let bucket_start = align_bucket_start(dt, self.trigger_interval);
            let owned_row = extract_owned_row(&batch, row_index)?;

            match next_current_bucket_start {
                Some(current_bucket_start) if bucket_start < current_bucket_start => {
                    self.handle_late_row(
                        dt,
                        next_current_bucket_start,
                        next_last_closed_bucket_start,
                        &mut next_pending_late_rows,
                    )?;
                }
                Some(current_bucket_start) if bucket_start == current_bucket_start => {
                    next_current_rows.insert(id, owned_row);
                }
                Some(current_bucket_start) => {
                    outputs.push(self.finalize_bucket(current_bucket_start, &next_current_rows)?);
                    next_last_closed_bucket_start = Some(current_bucket_start);
                    next_current_bucket_start = Some(bucket_start);
                    next_current_rows.clear();
                    next_current_rows.insert(id, owned_row);
                }
                None => {
                    if matches!(
                        next_last_closed_bucket_start,
                        Some(last_closed_bucket_start) if bucket_start <= last_closed_bucket_start
                    ) {
                        self.handle_late_row(
                            dt,
                            next_current_bucket_start,
                            next_last_closed_bucket_start,
                            &mut next_pending_late_rows,
                        )?;
                    } else {
                        next_current_bucket_start = Some(bucket_start);
                        next_current_rows.insert(id, owned_row);
                    }
                }
            }
        }

        self.current_bucket_start = next_current_bucket_start;
        self.current_rows = next_current_rows;
        self.last_closed_bucket_start = next_last_closed_bucket_start;
        self.pending_late_rows = next_pending_late_rows;

        Ok(outputs)
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        let Some(bucket_start) = self.current_bucket_start else {
            return Ok(vec![]);
        };

        if self.current_rows.is_empty() {
            self.current_bucket_start = None;
            return Ok(vec![]);
        }

        let current_rows = self.current_rows.clone();
        let output = self.finalize_bucket(bucket_start, &current_rows)?;
        self.last_closed_bucket_start = Some(bucket_start);
        self.current_bucket_start = None;
        self.current_rows.clear();

        Ok(vec![output])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: self.pending_late_rows,
        };
        self.pending_late_rows = 0;
        delta
    }
}

#[derive(Clone)]
struct OwnedRow {
    batch: RecordBatch,
}

fn build_output_schema(
    input_schema: &SchemaRef,
    id_column: &str,
    dt_column: &str,
    factors: &[Box<dyn CrossSectionalFactor>],
) -> Result<SchemaRef> {
    let mut output_names = HashSet::from([id_column.to_string(), dt_column.to_string()]);
    let mut output_fields = vec![
        Arc::new(
            input_schema
                .field_with_name(id_column)
                .map_err(|_| ZippyError::SchemaMismatch {
                    reason: format!("missing utf8 id field field=[{}]", id_column),
                })?
                .clone(),
        ),
        Arc::new(
            input_schema
                .field_with_name(dt_column)
                .map_err(|_| ZippyError::SchemaMismatch {
                    reason: format!("missing utc nanosecond dt field field=[{}]", dt_column),
                })?
                .clone(),
        ),
    ];

    for factor in factors {
        let output_field = factor.output_field();
        if !output_names.insert(output_field.name().clone()) {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "duplicate cross-sectional output field field=[{}]",
                    output_field.name()
                ),
            });
        }
        output_fields.push(Arc::new(output_field));
    }

    Ok(Arc::new(Schema::new(output_fields)))
}

fn build_bucket_batch<'a>(
    schema: &SchemaRef,
    rows: impl Iterator<Item = &'a OwnedRow>,
) -> Result<RecordBatch> {
    concat_batches(schema, rows.map(|row| &row.batch)).map_err(|error| ZippyError::Io {
        reason: format!("failed to build cross-sectional bucket batch error=[{}]", error),
    })
}

fn extract_owned_row(batch: &RecordBatch, row_index: usize) -> Result<OwnedRow> {
    let indices = UInt32Array::from(vec![row_index as u32]);
    let batch = take_record_batch(batch, &indices).map_err(|error| ZippyError::Io {
        reason: format!("failed to extract cross-sectional row error=[{}]", error),
    })?;

    Ok(OwnedRow { batch })
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

fn align_bucket_start(dt: i64, trigger_interval: i64) -> i64 {
    dt.div_euclid(trigger_interval) * trigger_interval
}
