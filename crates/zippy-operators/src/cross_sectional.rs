use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

/// Evaluate a cross-sectional factor against a single batch.
pub trait CrossSectionalFactor: Send {
    /// Return the output field definition for this factor.
    fn output_field(&self) -> Field;

    /// Evaluate the factor for each row in the batch.
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
}

/// Builder for a cross-sectional rank factor.
pub struct CSRankSpec {
    value_field: String,
    output_field: String,
}

impl CSRankSpec {
    /// Create a new cross-sectional rank factor spec.
    pub fn new(value_field: &str, output_field: &str) -> Self {
        Self {
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the cross-sectional rank factor.
    pub fn build(&self) -> Result<Box<dyn CrossSectionalFactor>> {
        Ok(Box::new(CSRankFactor {
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, true),
        }))
    }
}

/// Builder for a cross-sectional z-score factor.
pub struct CSZscoreSpec {
    value_field: String,
    output_field: String,
}

impl CSZscoreSpec {
    /// Create a new cross-sectional z-score factor spec.
    pub fn new(value_field: &str, output_field: &str) -> Self {
        Self {
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the cross-sectional z-score factor.
    pub fn build(&self) -> Result<Box<dyn CrossSectionalFactor>> {
        Ok(Box::new(CSZscoreFactor {
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, true),
        }))
    }
}

/// Builder for a cross-sectional demean factor.
pub struct CSDemeanSpec {
    value_field: String,
    output_field: String,
}

impl CSDemeanSpec {
    /// Create a new cross-sectional demean factor spec.
    pub fn new(value_field: &str, output_field: &str) -> Self {
        Self {
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the cross-sectional demean factor.
    pub fn build(&self) -> Result<Box<dyn CrossSectionalFactor>> {
        Ok(Box::new(CSDemeanFactor {
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, true),
        }))
    }
}

struct CSRankFactor {
    value_field: String,
    output_field: Field,
}

impl CrossSectionalFactor for CSRankFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let values = float64_values(batch, &self.value_field)?;
        let mut builder = Float64Builder::with_capacity(values.len());
        let mut samples = values
            .iter()
            .enumerate()
            .filter_map(|(index, value)| value.map(|sample| (index, sample)))
            .collect::<Vec<_>>();

        samples.sort_by(|left, right| total_cmp(left.1, right.1));

        let mut ranked = vec![None; values.len()];
        let mut start = 0;
        while start < samples.len() {
            let mut end = start + 1;
            while end < samples.len()
                && total_cmp(samples[start].1, samples[end].1) == Ordering::Equal
            {
                end += 1;
            }

            let average_rank = (start + 1 + end) as f64 / 2.0;
            for (row_index, _) in &samples[start..end] {
                ranked[*row_index] = Some(average_rank);
            }
            start = end;
        }

        append_optional_values(&mut builder, &ranked);
        Ok(Arc::new(builder.finish()))
    }
}

struct CSZscoreFactor {
    value_field: String,
    output_field: Field,
}

impl CrossSectionalFactor for CSZscoreFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let values = float64_values(batch, &self.value_field)?;
        let samples = values.iter().flatten().copied().collect::<Vec<_>>();
        let mut builder = Float64Builder::with_capacity(values.len());

        if samples.is_empty() {
            append_optional_values(&mut builder, &vec![None; values.len()]);
            return Ok(Arc::new(builder.finish()));
        }

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance = samples
            .iter()
            .map(|value| {
                let delta = *value - mean;
                delta * delta
            })
            .sum::<f64>()
            / samples.len() as f64;
        let std = variance.sqrt();

        for value in values {
            match value {
                Some(_) if std == 0.0 => builder.append_value(0.0),
                Some(value) => builder.append_value((value - mean) / std),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

struct CSDemeanFactor {
    value_field: String,
    output_field: Field,
}

impl CrossSectionalFactor for CSDemeanFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let values = float64_values(batch, &self.value_field)?;
        let samples = values.iter().flatten().copied().collect::<Vec<_>>();
        let mut builder = Float64Builder::with_capacity(values.len());

        if samples.is_empty() {
            append_optional_values(&mut builder, &vec![None; values.len()]);
            return Ok(Arc::new(builder.finish()));
        }

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        for value in values {
            match value {
                Some(value) => builder.append_value(value - mean),
                None => builder.append_null(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

fn float64_values(batch: &RecordBatch, value_field: &str) -> Result<Vec<Option<f64>>> {
    let column_index = batch
        .schema()
        .index_of(value_field)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing value field [{value_field}]"),
        })?;
    let column = batch.column(column_index);

    if column.data_type() != &DataType::Float64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "value field [{value_field}] must be float64 actual=[{:?}]",
                column.data_type()
            ),
        });
    }

    let values = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field [{value_field}] must downcast to float64"),
        })?;

    Ok((0..values.len())
        .map(|index| {
            if values.is_null(index) {
                return None;
            }

            let value = values.value(index);
            value.is_finite().then_some(value)
        })
        .collect())
}

fn append_optional_values(builder: &mut Float64Builder, values: &[Option<f64>]) {
    for value in values {
        match value {
            Some(value) => builder.append_value(*value),
            None => builder.append_null(),
        }
    }
}

fn total_cmp(left: f64, right: f64) -> Ordering {
    left.total_cmp(&right)
}
