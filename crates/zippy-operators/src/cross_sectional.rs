use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

/// Cached cross-sectional factor inputs for one materialized bucket.
pub struct CrossSectionalFactorContext<'a> {
    batch: &'a RecordBatch,
    float64_values_by_field: BTreeMap<String, Vec<Option<f64>>>,
    float64_stats_by_field: BTreeMap<String, Float64Stats>,
    rank_by_field: BTreeMap<String, Vec<Option<f64>>>,
}

/// Summary statistics for a finite float64 cross-section.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Float64Stats {
    pub sample_count: usize,
    pub mean: f64,
    pub variance: f64,
    pub std: f64,
}

impl<'a> CrossSectionalFactorContext<'a> {
    /// Create a factor context for a single materialized bucket.
    pub fn new(batch: &'a RecordBatch) -> Self {
        Self {
            batch,
            float64_values_by_field: BTreeMap::new(),
            float64_stats_by_field: BTreeMap::new(),
            rank_by_field: BTreeMap::new(),
        }
    }

    /// Return the underlying bucket batch for compatibility fallback paths.
    pub fn batch(&self) -> &RecordBatch {
        self.batch
    }

    /// Return cached finite float64 values for a field.
    pub fn float64_values(&mut self, field: &str) -> Result<&[Option<f64>]> {
        if !self.float64_values_by_field.contains_key(field) {
            let values = float64_values(self.batch, field)?;
            self.float64_values_by_field
                .insert(field.to_string(), values);
        }

        Ok(self
            .float64_values_by_field
            .get(field)
            .expect("cross-sectional values cache must contain requested field"))
    }

    /// Return cached population statistics for a finite float64 field.
    pub fn float64_stats(&mut self, field: &str) -> Result<Float64Stats> {
        if !self.float64_stats_by_field.contains_key(field) {
            let values = self.float64_values(field)?;
            let samples = values.iter().flatten().copied().collect::<Vec<_>>();
            let stats = compute_float64_stats(&samples);
            self.float64_stats_by_field.insert(field.to_string(), stats);
        }

        Ok(*self
            .float64_stats_by_field
            .get(field)
            .expect("cross-sectional stats cache must contain requested field"))
    }

    /// Return cached average-rank values for a finite float64 field.
    pub fn float64_ranks(&mut self, field: &str) -> Result<&[Option<f64>]> {
        if !self.rank_by_field.contains_key(field) {
            let values = self.float64_values(field)?;
            let ranks = compute_float64_ranks(values);
            self.rank_by_field.insert(field.to_string(), ranks);
        }

        Ok(self
            .rank_by_field
            .get(field)
            .expect("cross-sectional rank cache must contain requested field"))
    }
}

/// Evaluate a cross-sectional factor against a single batch.
pub trait CrossSectionalFactor: Send {
    /// Return the output field definition for this factor.
    fn output_field(&self) -> Field;

    /// Evaluate the factor for each row in the batch.
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the factor with per-bucket shared context.
    fn evaluate_with_context(
        &mut self,
        context: &mut CrossSectionalFactorContext<'_>,
    ) -> Result<ArrayRef> {
        self.evaluate(context.batch())
    }
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
        Ok(float64_array_from_options(&compute_float64_ranks(&values)))
    }

    fn evaluate_with_context(
        &mut self,
        context: &mut CrossSectionalFactorContext<'_>,
    ) -> Result<ArrayRef> {
        let ranks = context.float64_ranks(&self.value_field)?;
        Ok(float64_array_from_options(ranks))
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
        Ok(zscore_array(&values, compute_float64_stats(&samples)))
    }

    fn evaluate_with_context(
        &mut self,
        context: &mut CrossSectionalFactorContext<'_>,
    ) -> Result<ArrayRef> {
        let values = context.float64_values(&self.value_field)?.to_vec();
        let stats = context.float64_stats(&self.value_field)?;
        Ok(zscore_array(&values, stats))
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
        Ok(demean_array(&values, compute_float64_stats(&samples)))
    }

    fn evaluate_with_context(
        &mut self,
        context: &mut CrossSectionalFactorContext<'_>,
    ) -> Result<ArrayRef> {
        let values = context.float64_values(&self.value_field)?.to_vec();
        let stats = context.float64_stats(&self.value_field)?;
        Ok(demean_array(&values, stats))
    }
}

fn compute_float64_stats(samples: &[f64]) -> Float64Stats {
    if samples.is_empty() {
        return Float64Stats {
            sample_count: 0,
            mean: 0.0,
            variance: 0.0,
            std: 0.0,
        };
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

    Float64Stats {
        sample_count: samples.len(),
        mean,
        variance,
        std: variance.sqrt(),
    }
}

fn compute_float64_ranks(values: &[Option<f64>]) -> Vec<Option<f64>> {
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
        while end < samples.len() && total_cmp(samples[start].1, samples[end].1) == Ordering::Equal
        {
            end += 1;
        }

        let average_rank = (start + 1 + end) as f64 / 2.0;
        for (row_index, _) in &samples[start..end] {
            ranked[*row_index] = Some(average_rank);
        }
        start = end;
    }

    ranked
}

fn zscore_array(values: &[Option<f64>], stats: Float64Stats) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());

    if stats.sample_count == 0 {
        append_optional_values(&mut builder, &vec![None; values.len()]);
        return Arc::new(builder.finish());
    }

    for value in values {
        match value {
            Some(_) if stats.std == 0.0 => builder.append_value(0.0),
            Some(value) => builder.append_value((value - stats.mean) / stats.std),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn demean_array(values: &[Option<f64>], stats: Float64Stats) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());

    if stats.sample_count == 0 {
        append_optional_values(&mut builder, &vec![None; values.len()]);
        return Arc::new(builder.finish());
    }

    for value in values {
        match value {
            Some(value) => builder.append_value(value - stats.mean),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn float64_array_from_options(values: &[Option<f64>]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    append_optional_values(&mut builder, values);
    Arc::new(builder.finish())
}

fn float64_values(batch: &RecordBatch, value_field: &str) -> Result<Vec<Option<f64>>> {
    let column_index =
        batch
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
