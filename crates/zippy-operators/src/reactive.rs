use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Float64Builder, Int32Array, Int64Array,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

const LOG_INPUT_MUST_BE_POSITIVE: &str = "log input must be positive";

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

/// Builder for a per-id rolling mean factor.
pub struct TsMeanSpec {
    id_field: String,
    value_field: String,
    window: usize,
    output_field: String,
}

impl TsMeanSpec {
    /// Create a new rolling mean factor spec.
    pub fn new(id_field: &str, value_field: &str, window: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            window,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive rolling mean factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        build_window_history_factor(
            &self.id_field,
            &self.value_field,
            self.window,
            &self.output_field,
            WindowHistoryKind::Mean,
        )
    }
}

/// Builder for a per-id rolling standard deviation factor.
pub struct TsStdSpec {
    id_field: String,
    value_field: String,
    window: usize,
    output_field: String,
}

impl TsStdSpec {
    /// Create a new rolling standard deviation factor spec.
    pub fn new(id_field: &str, value_field: &str, window: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            window,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive rolling standard deviation factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        build_window_history_factor(
            &self.id_field,
            &self.value_field,
            self.window,
            &self.output_field,
            WindowHistoryKind::Std,
        )
    }
}

/// Builder for a per-id delay factor.
pub struct TsDelaySpec {
    id_field: String,
    value_field: String,
    period: usize,
    output_field: String,
}

impl TsDelaySpec {
    /// Create a new delay factor spec.
    pub fn new(id_field: &str, value_field: &str, period: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            period,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive delay factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        build_window_history_factor(
            &self.id_field,
            &self.value_field,
            self.period,
            &self.output_field,
            WindowHistoryKind::Delay,
        )
    }
}

/// Builder for a per-id difference factor.
pub struct TsDiffSpec {
    id_field: String,
    value_field: String,
    period: usize,
    output_field: String,
}

impl TsDiffSpec {
    /// Create a new diff factor spec.
    pub fn new(id_field: &str, value_field: &str, period: usize, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            period,
            output_field: output_field.to_string(),
        }
    }

    /// Build the reactive diff factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        build_window_history_factor(
            &self.id_field,
            &self.value_field,
            self.period,
            &self.output_field,
            WindowHistoryKind::Diff,
        )
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
        build_window_history_factor(
            &self.id_field,
            &self.value_field,
            self.period,
            &self.output_field,
            WindowHistoryKind::Return,
        )
    }
}

/// Builder for an absolute-value factor.
pub struct AbsSpec {
    id_field: String,
    value_field: String,
    output_field: String,
}

impl AbsSpec {
    /// Create a new absolute-value factor spec.
    pub fn new(id_field: &str, value_field: &str, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the absolute-value factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        Ok(Box::new(UnaryFloatFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, false),
            kind: UnaryFloatKind::Abs,
        }))
    }
}

/// Builder for a natural-log transform factor.
pub struct LogSpec {
    id_field: String,
    value_field: String,
    output_field: String,
}

impl LogSpec {
    /// Create a new natural-log factor spec.
    pub fn new(id_field: &str, value_field: &str, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the natural-log factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        Ok(Box::new(UnaryFloatFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, false),
            kind: UnaryFloatKind::Log,
        }))
    }
}

/// Builder for a clip factor.
pub struct ClipSpec {
    id_field: String,
    value_field: String,
    min: f64,
    max: f64,
    output_field: String,
}

impl ClipSpec {
    /// Create a new clip factor spec.
    pub fn new(id_field: &str, value_field: &str, min: f64, max: f64, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            min,
            max,
            output_field: output_field.to_string(),
        }
    }

    /// Build the clip factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        if self.min > self.max {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "clip bounds must satisfy min <= max min=[{}] max=[{}]",
                    self.min, self.max
                ),
            });
        }

        Ok(Box::new(UnaryFloatFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, false),
            kind: UnaryFloatKind::Clip {
                min: self.min,
                max: self.max,
            },
        }))
    }
}

/// Builder for an explicit cast factor.
pub struct CastSpec {
    id_field: String,
    value_field: String,
    dtype: String,
    output_field: String,
}

impl CastSpec {
    /// Create a new cast factor spec.
    pub fn new(id_field: &str, value_field: &str, dtype: &str, output_field: &str) -> Self {
        Self {
            id_field: id_field.to_string(),
            value_field: value_field.to_string(),
            dtype: dtype.to_string(),
            output_field: output_field.to_string(),
        }
    }

    /// Build the cast factor.
    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        let cast_kind = CastKind::parse(&self.dtype)?;

        Ok(Box::new(CastFactor {
            id_field: self.id_field.clone(),
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, cast_kind.data_type(), false),
            kind: cast_kind,
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

#[derive(Clone, Copy)]
enum WindowHistoryKind {
    Mean,
    Std,
    Delay,
    Diff,
    Return,
}

struct WindowHistoryFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    size: usize,
    kind: WindowHistoryKind,
    history_by_id: HashMap<String, VecDeque<f64>>,
}

impl ReactiveFactor for WindowHistoryFactor {
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

            match self.kind {
                WindowHistoryKind::Mean => {
                    history.push_back(value);
                    trim_history(history, self.size);
                    if history.len() < self.size {
                        builder.append_null();
                    } else {
                        let sum = history.iter().copied().sum::<f64>();
                        builder.append_value(sum / self.size as f64);
                    }
                }
                WindowHistoryKind::Std => {
                    history.push_back(value);
                    trim_history(history, self.size);
                    if history.len() < self.size {
                        builder.append_null();
                    } else {
                        let mean = history.iter().copied().sum::<f64>() / self.size as f64;
                        let variance = history
                            .iter()
                            .map(|item| {
                                let centered = *item - mean;
                                centered * centered
                            })
                            .sum::<f64>()
                            / self.size as f64;
                        builder.append_value(variance.sqrt());
                    }
                }
                WindowHistoryKind::Delay => {
                    if history.len() < self.size {
                        builder.append_null();
                    } else {
                        builder
                            .append_value(*history.front().expect("history length checked above"));
                    }
                    history.push_back(value);
                    trim_history(history, self.size);
                }
                WindowHistoryKind::Diff => {
                    if history.len() < self.size {
                        builder.append_null();
                    } else {
                        let base = *history.front().expect("history length checked above");
                        builder.append_value(value - base);
                    }
                    history.push_back(value);
                    trim_history(history, self.size);
                }
                WindowHistoryKind::Return => {
                    if history.len() < self.size {
                        builder.append_null();
                    } else {
                        let base = *history.front().expect("history length checked above");
                        builder.append_value((value / base) - 1.0);
                    }
                    history.push_back(value);
                    trim_history(history, self.size);
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

enum UnaryFloatKind {
    Abs,
    Log,
    Clip { min: f64, max: f64 },
}

struct UnaryFloatFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    kind: UnaryFloatKind,
}

impl ReactiveFactor for UnaryFloatFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (_ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;
        let mut builder = Float64Builder::with_capacity(batch.num_rows());

        for index in 0..batch.num_rows() {
            let value = values.value(index);
            let next = match self.kind {
                UnaryFloatKind::Abs => value.abs(),
                UnaryFloatKind::Log => {
                    if value <= 0.0 {
                        return Err(ZippyError::InvalidState {
                            status: LOG_INPUT_MUST_BE_POSITIVE,
                        });
                    }
                    value.ln()
                }
                UnaryFloatKind::Clip { min, max } => value.clamp(min, max),
            };
            builder.append_value(next);
        }

        Ok(Arc::new(builder.finish()))
    }
}

#[derive(Clone, Copy)]
enum CastKind {
    Float64,
    Float32,
    Int64,
    Int32,
    Utf8,
}

impl CastKind {
    fn parse(dtype: &str) -> Result<Self> {
        match dtype {
            "float64" => Ok(Self::Float64),
            "float32" => Ok(Self::Float32),
            "int64" => Ok(Self::Int64),
            "int32" => Ok(Self::Int32),
            "utf8" | "string" => Ok(Self::Utf8),
            _ => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported cast dtype dtype=[{}]", dtype),
            }),
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Float64 => DataType::Float64,
            Self::Float32 => DataType::Float32,
            Self::Int64 => DataType::Int64,
            Self::Int32 => DataType::Int32,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

struct CastFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    kind: CastKind,
}

impl ReactiveFactor for CastFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (_ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;

        match self.kind {
            CastKind::Float64 => Ok(Arc::new(Float64Array::from(
                (0..values.len())
                    .map(|index| values.value(index))
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            CastKind::Float32 => Ok(Arc::new(Float32Array::from(
                (0..values.len())
                    .map(|index| values.value(index) as f32)
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            CastKind::Int64 => Ok(Arc::new(Int64Array::from(
                (0..values.len())
                    .map(|index| values.value(index) as i64)
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            CastKind::Int32 => Ok(Arc::new(Int32Array::from(
                (0..values.len())
                    .map(|index| values.value(index) as i32)
                    .collect::<Vec<_>>(),
            )) as ArrayRef),
            CastKind::Utf8 => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
                for index in 0..values.len() {
                    builder.append_value(values.value(index).to_string());
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
        }
    }
}

fn build_window_history_factor(
    id_field: &str,
    value_field: &str,
    size: usize,
    output_field: &str,
    kind: WindowHistoryKind,
) -> Result<Box<dyn ReactiveFactor>> {
    if size == 0 {
        return Err(ZippyError::InvalidConfig {
            reason: "window size must be positive".to_string(),
        });
    }

    Ok(Box::new(WindowHistoryFactor {
        id_field: id_field.to_string(),
        value_field: value_field.to_string(),
        output_field: Field::new(output_field, DataType::Float64, true),
        size,
        kind,
        history_by_id: HashMap::new(),
    }))
}

fn trim_history(history: &mut VecDeque<f64>, size: usize) {
    while history.len() > size {
        history.pop_front();
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
