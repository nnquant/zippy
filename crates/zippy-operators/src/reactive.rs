use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Float64Builder, Int32Array, Int64Array,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, ZippyError};

const LOG_INPUT_MUST_BE_POSITIVE: &str = "log input must be positive";

#[derive(Clone, Copy)]
pub(crate) enum StatefulFloatKind {
    Ema { span: usize },
    Mean { window: usize },
    Std { window: usize },
    Delay { period: usize },
    Diff { period: usize },
    Return { period: usize },
}

pub struct StatefulFloatById {
    state: StatefulFloatState,
    undo_log: Option<StatefulFloatUndoLog>,
}

enum StatefulFloatState {
    Ema {
        alpha: f64,
        state_by_id: HashMap<String, f64>,
    },
    Window {
        size: usize,
        kind: WindowHistoryKind,
        history_by_id: HashMap<String, WindowHistory>,
    },
}

#[derive(Default)]
struct StatefulFloatUndoLog {
    entries: HashMap<String, StatefulFloatUndoEntry>,
}

enum StatefulFloatUndoEntry {
    Ema(Option<f64>),
    Window(Option<WindowHistory>),
}

/// Lightweight read-only view over the columns visible to a reactive factor.
pub struct ReactiveFactorContext<'a> {
    schema: Arc<Schema>,
    columns: &'a [ArrayRef],
    row_count: usize,
}

impl<'a> ReactiveFactorContext<'a> {
    /// Create a factor context from a schema and matching columns.
    pub fn new(schema: &Arc<Schema>, columns: &'a [ArrayRef]) -> Result<Self> {
        if schema.fields().len() != columns.len() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "reactive factor context schema column count mismatch schema_columns=[{}] columns=[{}]",
                    schema.fields().len(),
                    columns.len()
                ),
            });
        }

        let row_count = columns.first().map_or(0, |column| column.len());
        for (index, column) in columns.iter().enumerate() {
            if column.len() != row_count {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "reactive factor context column length mismatch column_index=[{}] expected=[{}] actual=[{}]",
                        index,
                        row_count,
                        column.len()
                    ),
                });
            }
        }

        Ok(Self {
            schema: Arc::clone(schema),
            columns,
            row_count,
        })
    }

    /// Create a factor context from an existing record batch.
    pub fn from_batch(batch: &'a RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            columns: batch.columns(),
            row_count: batch.num_rows(),
        }
    }

    /// Return the row count shared by all columns.
    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Return the schema visible to the factor.
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    /// Resolve a field name to a column index.
    pub fn index_of(&self, field: &str) -> Result<usize> {
        self.schema
            .index_of(field)
            .map_err(|_| ZippyError::SchemaMismatch {
                reason: format!("missing reactive factor field field=[{}]", field),
            })
    }

    /// Return a column by index.
    pub fn column(&self, index: usize) -> Result<&'a ArrayRef> {
        self.columns
            .get(index)
            .ok_or_else(|| ZippyError::SchemaMismatch {
                reason: format!(
                    "reactive factor column index out of bounds index=[{}]",
                    index
                ),
            })
    }

    /// Return a column by field name.
    pub fn column_by_name(&self, field: &str) -> Result<&'a ArrayRef> {
        let index = self.index_of(field)?;
        self.column(index)
    }

    /// Materialize this context as a RecordBatch for compatibility fallback.
    pub fn record_batch(&self) -> Result<RecordBatch> {
        RecordBatch::try_new(Arc::clone(&self.schema), self.columns.to_vec()).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build reactive context fallback batch error=[{}]",
                    error
                ),
            }
        })
    }
}

/// Evaluate a stateful factor against rows in input order.
pub trait ReactiveFactor: Send {
    /// Return the output field definition for this factor.
    fn output_field(&self) -> Field;

    /// Evaluate the factor for each row in the batch.
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the factor against a lightweight column context.
    fn evaluate_with_context(&mut self, context: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let batch = context.record_batch()?;
        self.evaluate(&batch)
    }

    /// Start recording dirty-key undo entries for this factor.
    fn begin_transaction(&mut self) {}

    /// Commit pending dirty-key undo entries.
    fn commit_transaction(&mut self) {}

    /// Restore pending dirty-key undo entries.
    fn rollback_transaction(&mut self) -> Result<()> {
        Ok(())
    }
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
            state: StatefulFloatById::new(StatefulFloatKind::Ema { span: self.span }),
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

impl StatefulFloatById {
    pub(crate) fn new(kind: StatefulFloatKind) -> Self {
        let state = match kind {
            StatefulFloatKind::Ema { span } => StatefulFloatState::Ema {
                alpha: 2.0 / (span as f64 + 1.0),
                state_by_id: HashMap::new(),
            },
            StatefulFloatKind::Mean { window } => StatefulFloatState::Window {
                size: window,
                kind: WindowHistoryKind::Mean,
                history_by_id: HashMap::new(),
            },
            StatefulFloatKind::Std { window } => StatefulFloatState::Window {
                size: window,
                kind: WindowHistoryKind::Std,
                history_by_id: HashMap::new(),
            },
            StatefulFloatKind::Delay { period } => StatefulFloatState::Window {
                size: period,
                kind: WindowHistoryKind::Delay,
                history_by_id: HashMap::new(),
            },
            StatefulFloatKind::Diff { period } => StatefulFloatState::Window {
                size: period,
                kind: WindowHistoryKind::Diff,
                history_by_id: HashMap::new(),
            },
            StatefulFloatKind::Return { period } => StatefulFloatState::Window {
                size: period,
                kind: WindowHistoryKind::Return,
                history_by_id: HashMap::new(),
            },
        };

        Self {
            state,
            undo_log: None,
        }
    }

    pub(crate) fn evaluate_optional(
        &mut self,
        id: &str,
        input: Option<f64>,
    ) -> Result<Option<f64>> {
        let Some(value) = input else {
            return Ok(None);
        };

        self.record_undo(id);

        match &mut self.state {
            StatefulFloatState::Ema { alpha, state_by_id } => {
                let next = match state_by_id.get(id).copied() {
                    Some(previous) => *alpha * value + (1.0 - *alpha) * previous,
                    None => value,
                };
                state_by_id.insert(id.to_string(), next);
                Ok(Some(next))
            }
            StatefulFloatState::Window {
                size,
                kind,
                history_by_id,
            } => {
                let history = history_by_id.entry(id.to_string()).or_default();

                let output = match kind {
                    WindowHistoryKind::Mean => {
                        history.push_and_trim(value, *size);
                        if history.len() < *size {
                            None
                        } else {
                            Some(history.sum() / *size as f64)
                        }
                    }
                    WindowHistoryKind::Std => {
                        history.push_and_trim(value, *size);
                        if history.len() < *size {
                            None
                        } else {
                            let mean = history.sum() / *size as f64;
                            let variance = history.sum_squares() / *size as f64 - mean * mean;
                            Some(variance.max(0.0).sqrt())
                        }
                    }
                    WindowHistoryKind::Delay => {
                        let output = if history.len() < *size {
                            None
                        } else {
                            history.front()
                        };
                        history.push_and_trim(value, *size);
                        output
                    }
                    WindowHistoryKind::Diff => {
                        let output = if history.len() < *size {
                            None
                        } else {
                            history.front().map(|base| value - base)
                        };
                        history.push_and_trim(value, *size);
                        output
                    }
                    WindowHistoryKind::Return => {
                        let output = if history.len() < *size {
                            None
                        } else {
                            history.front().and_then(|base| {
                                if base == 0.0 {
                                    None
                                } else {
                                    let ret = (value / base) - 1.0;
                                    ret.is_finite().then_some(ret)
                                }
                            })
                        };
                        history.push_and_trim(value, *size);
                        output
                    }
                };

                Ok(output)
            }
        }
    }

    pub fn begin_transaction(&mut self) {
        if self.undo_log.is_none() {
            self.undo_log = Some(StatefulFloatUndoLog::default());
        }
    }

    pub fn commit_transaction(&mut self) {
        self.undo_log = None;
    }

    pub fn rollback_transaction(&mut self) -> Result<()> {
        let Some(undo_log) = self.undo_log.take() else {
            return Ok(());
        };

        match &mut self.state {
            StatefulFloatState::Ema { state_by_id, .. } => {
                for (id, entry) in undo_log.entries {
                    match entry {
                        StatefulFloatUndoEntry::Ema(Some(previous)) => {
                            state_by_id.insert(id, previous);
                        }
                        StatefulFloatUndoEntry::Ema(None) => {
                            state_by_id.remove(&id);
                        }
                        StatefulFloatUndoEntry::Window(_) => {
                            return Err(ZippyError::InvalidState {
                                status: "reactive undo log type mismatch",
                            });
                        }
                    }
                }
            }
            StatefulFloatState::Window { history_by_id, .. } => {
                for (id, entry) in undo_log.entries {
                    match entry {
                        StatefulFloatUndoEntry::Window(Some(previous)) => {
                            history_by_id.insert(id, previous);
                        }
                        StatefulFloatUndoEntry::Window(None) => {
                            history_by_id.remove(&id);
                        }
                        StatefulFloatUndoEntry::Ema(_) => {
                            return Err(ZippyError::InvalidState {
                                status: "reactive undo log type mismatch",
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn record_undo(&mut self, id: &str) {
        let Some(undo_log) = self.undo_log.as_mut() else {
            return;
        };

        if undo_log.entries.contains_key(id) {
            return;
        }

        let entry = match &self.state {
            StatefulFloatState::Ema { state_by_id, .. } => {
                StatefulFloatUndoEntry::Ema(state_by_id.get(id).copied())
            }
            StatefulFloatState::Window { history_by_id, .. } => {
                StatefulFloatUndoEntry::Window(history_by_id.get(id).cloned())
            }
        };
        undo_log.entries.insert(id.to_string(), entry);
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
    state: StatefulFloatById,
}

impl ReactiveFactor for TsEmaFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;
        self.evaluate_arrays(ids, values)
    }

    fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let (ids, values) = extract_context_columns(ctx, &self.id_field, &self.value_field)?;
        self.evaluate_arrays(ids, values)
    }

    fn begin_transaction(&mut self) {
        self.state.begin_transaction();
    }

    fn commit_transaction(&mut self) {
        self.state.commit_transaction();
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        self.state.rollback_transaction()
    }
}

impl TsEmaFactor {
    fn evaluate_arrays(&mut self, ids: &StringArray, values: &Float64Array) -> Result<ArrayRef> {
        let mut builder = Float64Builder::with_capacity(values.len());

        for index in 0..values.len() {
            let id = ids.value(index);
            let value = values.value(index);
            let next = self
                .state
                .evaluate_optional(id, Some(value))?
                .expect("ema emits a value for non-null input");
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

#[derive(Clone, Default)]
struct WindowHistory {
    values: VecDeque<f64>,
    sum: f64,
    sum_squares: f64,
}

impl WindowHistory {
    fn push_and_trim(&mut self, value: f64, size: usize) {
        self.values.push_back(value);
        self.sum += value;
        self.sum_squares += value * value;

        while self.values.len() > size {
            let removed = self
                .values
                .pop_front()
                .expect("window history length checked above");
            self.sum -= removed;
            self.sum_squares -= removed * removed;
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn front(&self) -> Option<f64> {
        self.values.front().copied()
    }

    fn sum(&self) -> f64 {
        self.sum
    }

    fn sum_squares(&self) -> f64 {
        self.sum_squares
    }
}

struct WindowHistoryFactor {
    id_field: String,
    value_field: String,
    output_field: Field,
    state: StatefulFloatById,
}

impl ReactiveFactor for WindowHistoryFactor {
    fn output_field(&self) -> Field {
        self.output_field.clone()
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let (ids, values) = extract_columns(batch, &self.id_field, &self.value_field)?;
        self.evaluate_arrays(ids, values)
    }

    fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let (ids, values) = extract_context_columns(ctx, &self.id_field, &self.value_field)?;
        self.evaluate_arrays(ids, values)
    }

    fn begin_transaction(&mut self) {
        self.state.begin_transaction();
    }

    fn commit_transaction(&mut self) {
        self.state.commit_transaction();
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        self.state.rollback_transaction()
    }
}

impl WindowHistoryFactor {
    fn evaluate_arrays(&mut self, ids: &StringArray, values: &Float64Array) -> Result<ArrayRef> {
        let mut builder = Float64Builder::with_capacity(values.len());

        for index in 0..values.len() {
            let id = ids.value(index);
            let value = values.value(index);
            match self.state.evaluate_optional(id, Some(value))? {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
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
        self.evaluate_values(values)
    }

    fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let (_ids, values) = extract_context_columns(ctx, &self.id_field, &self.value_field)?;
        self.evaluate_values(values)
    }
}

impl UnaryFloatFactor {
    fn evaluate_values(&self, values: &Float64Array) -> Result<ArrayRef> {
        let mut builder = Float64Builder::with_capacity(values.len());

        for index in 0..values.len() {
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
        self.evaluate_values(values)
    }

    fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let (_ids, values) = extract_context_columns(ctx, &self.id_field, &self.value_field)?;
        self.evaluate_values(values)
    }
}

impl CastFactor {
    fn evaluate_values(&self, values: &Float64Array) -> Result<ArrayRef> {
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
        state: StatefulFloatById::new(match kind {
            WindowHistoryKind::Mean => StatefulFloatKind::Mean { window: size },
            WindowHistoryKind::Std => StatefulFloatKind::Std { window: size },
            WindowHistoryKind::Delay => StatefulFloatKind::Delay { period: size },
            WindowHistoryKind::Diff => StatefulFloatKind::Diff { period: size },
            WindowHistoryKind::Return => StatefulFloatKind::Return { period: size },
        }),
    }))
}

fn extract_context_columns<'a>(
    ctx: &'a ReactiveFactorContext<'_>,
    id_field: &str,
    value_field: &str,
) -> Result<(&'a StringArray, &'a Float64Array)> {
    let id_array = ctx
        .column_by_name(id_field)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_field),
        })?;
    let value_array = ctx
        .column_by_name(value_field)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", value_field),
        })?;

    validate_id_value_arrays(id_array, value_array, id_field, value_field)?;
    Ok((id_array, value_array))
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

    validate_id_value_arrays(id_array, value_array, id_field, value_field)?;
    Ok((id_array, value_array))
}

fn validate_id_value_arrays(
    id_array: &StringArray,
    value_array: &Float64Array,
    id_field: &str,
    value_field: &str,
) -> Result<()> {
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

    for index in 0..value_array.len() {
        let value = value_array.value(index);
        if !value.is_finite() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "value field contains non-finite float field=[{}] row=[{}] value=[{}]",
                    value_field, index, value
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{ReactiveFactorContext, WindowHistory};

    #[test]
    fn reactive_factor_context_resolves_columns_by_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns = vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ];
        let ctx = ReactiveFactorContext::new(&schema, &columns).unwrap();

        assert_eq!(ctx.num_rows(), 2);
        assert_eq!(ctx.index_of("value").unwrap(), 1);
        assert_eq!(ctx.column_by_name("id").unwrap().len(), 2);
    }

    #[test]
    fn reactive_factor_context_rejects_mismatched_column_lengths() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns = vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
        ];

        let error = match ReactiveFactorContext::new(&schema, &columns) {
            Ok(_) => panic!("context should reject mismatched column lengths"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("column length mismatch"));
    }

    #[test]
    fn window_history_maintains_running_sum_and_squares_when_trimmed() {
        let mut history = WindowHistory::default();

        history.push_and_trim(2.0, 3);
        history.push_and_trim(4.0, 3);
        history.push_and_trim(6.0, 3);
        history.push_and_trim(8.0, 3);

        assert_eq!(history.len(), 3);
        assert_eq!(history.front(), Some(4.0));
        assert_eq!(history.sum(), 18.0);
        assert_eq!(history.sum_squares(), 116.0);
    }
}
