use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

/// Evaluate a stateful factor against rows in input order.
pub trait ReactiveFactor: Send {
    /// Return the output field definition for this factor.
    fn output_field(&self) -> Field;

    /// Evaluate the factor for each row in the batch.
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// Builder for a per-id exponential moving average factor.
pub struct TsEmaSpec {
    id_field: String,
    value_field: String,
    span: usize,
    output_field: String,
}

impl TsEmaSpec {
    /// Create a new EMA factor spec.
    pub fn new(id_field: &str, value_field: &str, span: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            span,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive EMA factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        if self.span == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "ema span must be positive".to_string(),
            });
        }

        Ok(Box::new(TsEmaFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, false),
            alpha: 2.0 / (self.span as f64 + 1.0),
            state_by_id: HashMap::new(),
        }))
    }
}

/// Builder for a per-id period return factor.
pub struct TsReturnSpec {
    id_field: String,
    value_field: String,
    output_field: String,
    period: usize,
}

impl TsReturnSpec {
    /// Create a new return factor spec.
    pub fn new(id_field: &str, value_field: &str, period: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            period,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive return factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        if self.period == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "return period must be positive".to_string(),
            });
        }

        Ok(Box::new(TsReturnFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, true),
            period: self.period,
            history_by_id: HashMap::new(),
        }))
    }
}

struct TsEmaFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    alpha: f64,
    state_by_id: HashMap<String, f64>,
}

impl ReactiveFactor for TsEmaFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;
        let mut builder = Float64Builder::with_capacity(batch.num_rows());

        for index in 0..batch.num_rows() {
            let id = ids.value(index);
            let value = values.value(index);
            let next = match self.state_by_id.get(id).copied() {
                Some(previous) => self.alpha * value + (1.0 - self.alpha) * previous,
                None => value,
            };
            self.state_by_id.insert(id.to_string(), next);
            builder.append_value(next);
        }

        Ok(Arc::new(builder.finish()))
    }
}

struct TsReturnFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    period: usize,
    history_by_id: HashMap<String, VecDeque<f64>>,
}

impl ReactiveFactor for TsReturnFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;
        let mut builder = Float64Builder::with_capacity(batch.num_rows());

        for index in 0..batch.num_rows() {
            let id = ids.value(index);
            let value = values.value(index);
            let history = self.history_by_id.entry(id.to_string()).or_default();

            if history.len() < self.period {
                builder.append_null();
            } else {
                let base = *history.front().expect("history length checked above");
                builder.append_value((value / base) - 1.0);
            }

            history.push_back(value);
            if history.len() > self.period {
                history.pop_front();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

fn extract_columns<'a>(
    batch: &'a RecordBatch,
    id_field: &str,
    value_field: &str,
) -> Result<(&'a StringArray, &'a Float64Array)> {
    let id_index = batch
        .schema()
        .index_of(id_field)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_field),
        })?;
    let value_index =
        batch
            .schema()
            .index_of(value_field)
            .map_err(|_| ZippyError::SchemaMismatch {
                reason: format!("missing float64 value field field=[{}]", value_field),
            })?;

    let id_array = batch
        .column(id_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_field),
        })?;
    let value_array = batch
        .column(value_index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", value_field),
        })?;

    if id_array.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field contains nulls field=[{}]", id_field),
        });
    }

    if value_array.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("value field contains nulls field=[{}]", value_field),
        });
    }

    Ok((id_array, value_array))
}
