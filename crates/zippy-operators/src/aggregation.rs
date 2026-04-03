use arrow::datatypes::{DataType, Field};
use zippy_core::Result;

/// Supported aggregation kinds for the v1 time-series engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregationKind {
    First,
    Last,
    Sum,
    Max,
    Min,
    Count,
    Vwap,
}

/// Declarative aggregation spec consumed by `TimeSeriesEngine`.
pub trait AggregationSpec: Send {
    /// Return the output field emitted by this aggregation.
    fn output_field(&self) -> Field;

    /// Return the primary source input column consumed by this aggregation.
    fn primary_column(&self) -> &str;

    /// Return the optional secondary source input column consumed by this aggregation.
    fn secondary_column(&self) -> Option<&str> {
        None
    }

    /// Return the aggregation kind.
    fn kind(&self) -> AggregationKind;
}

fn float64_output_field(output_field: &str) -> Field {
    Field::new(output_field, DataType::Float64, false)
}

/// Emit the first row value observed within a window.
pub struct AggFirstSpec {
    primary_column: String,
    output_field: Field,
}

impl AggFirstSpec {
    /// Create a first-value aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggFirstSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::First
    }
}

/// Emit the last row value observed within a window.
pub struct AggLastSpec {
    primary_column: String,
    output_field: Field,
}

impl AggLastSpec {
    /// Create a last-value aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggLastSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Last
    }
}

/// Emit the sum of row values observed within a window.
pub struct AggSumSpec {
    primary_column: String,
    output_field: Field,
}

impl AggSumSpec {
    /// Create a sum aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggSumSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Sum
    }
}

/// Emit the maximum row value observed within a window.
pub struct AggMaxSpec {
    primary_column: String,
    output_field: Field,
}

impl AggMaxSpec {
    /// Create a max aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggMaxSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Max
    }
}

/// Emit the minimum row value observed within a window.
pub struct AggMinSpec {
    primary_column: String,
    output_field: Field,
}

impl AggMinSpec {
    /// Create a min aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggMinSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Min
    }
}

/// Emit the count of rows observed within a window for the primary column.
pub struct AggCountSpec {
    primary_column: String,
    output_field: Field,
}

impl AggCountSpec {
    /// Create a count aggregation spec.
    pub fn new(primary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggCountSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Count
    }
}

/// Emit the volume-weighted average price observed within a window.
pub struct AggVwapSpec {
    primary_column: String,
    secondary_column: String,
    output_field: Field,
}

impl AggVwapSpec {
    /// Create a vwap aggregation spec.
    pub fn new(primary_column: &str, secondary_column: &str, output_field: &str) -> Self {
        Self {
            primary_column: primary_column.to_string(),
            secondary_column: secondary_column.to_string(),
            output_field: float64_output_field(output_field),
        }
    }

    /// Build the aggregation spec as a trait object.
    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggVwapSpec {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn primary_column(&self) -> &str {
        &self.primary_column
    }

    fn secondary_column(&self) -> Option<&str> {
        Some(&self.secondary_column)
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Vwap
    }
}
