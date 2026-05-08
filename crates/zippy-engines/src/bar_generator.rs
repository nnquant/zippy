use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

use crate::table_view::{float64_array, string_array, timestamp_ns_array};

/// Supported one-minute bar frequency.
pub const BAR_FREQUENCY_1M: &str = "1m";
const BAR_FREQUENCY_1M_NS: i64 = 60_000_000_000;

/// Input column names consumed by [`BarGeneratorEngine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarInputColumns {
    pub instrument: String,
    pub dt: String,
    pub price: String,
    pub volume: String,
    pub total_turnover: String,
    pub trading_day: Option<String>,
    pub num_trades: Option<String>,
    pub limit_up: Option<String>,
    pub limit_down: Option<String>,
}

/// Trading session window represented as seconds since local midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindow {
    pub start_seconds: u32,
    pub end_seconds: u32,
}

impl SessionWindow {
    /// Parse a local time session window in `HH:MM:SS` format.
    pub fn parse(start: &str, end: &str) -> Result<Self> {
        let start_seconds = parse_seconds_since_midnight(start)?;
        let end_seconds = parse_seconds_since_midnight(end)?;

        if start_seconds >= end_seconds {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "session start must be before end start=[{}] end=[{}]",
                    start, end
                ),
            });
        }

        Ok(Self {
            start_seconds,
            end_seconds,
        })
    }
}

/// Session profile used to assign ticks into bar windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSessionSpec {
    pub timezone: String,
    pub regular: Vec<SessionWindow>,
    pub auction: Vec<SessionWindow>,
}

/// Bootstrap policy for cumulative volume streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapPolicy {
    SkipFirstDelta,
    FromZero,
}

/// Tick volume interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeSpec {
    Delta,
    Cumulative {
        trading_day_column: String,
        bootstrap: BootstrapPolicy,
    },
}

/// Auction tick handling policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuctionPolicy {
    Drop,
    MergeToFirstRegularBar,
    EmitSeparateBar,
}

/// Timestamp label policy for generated bars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DtLabelPolicy {
    CloseDt,
    StartDt,
}

/// Configuration for [`BarGeneratorEngine`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarGeneratorSpec {
    pub frequency: String,
    pub columns: BarInputColumns,
    pub sessions: BarSessionSpec,
    pub volume: VolumeSpec,
    pub auction: AuctionPolicy,
    pub dt_label: DtLabelPolicy,
}

/// Stateful tick-to-bar engine.
pub struct BarGeneratorEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    spec: BarGeneratorSpec,
    open_bars: BTreeMap<String, OpenBar>,
    pending_filtered_rows: u64,
}

impl BarGeneratorEngine {
    /// Create a bar generator with a stable output schema.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        spec: BarGeneratorSpec,
    ) -> Result<Self> {
        validate_spec(input_schema.as_ref(), &spec)?;
        let output_schema = build_output_schema(input_schema.as_ref(), &spec)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            spec,
            open_bars: BTreeMap::new(),
            pending_filtered_rows: 0,
        })
    }

    fn build_output_batch(&self, bars: &[OpenBar]) -> Result<RecordBatch> {
        let instruments = bars
            .iter()
            .map(|bar| bar.instrument.as_str())
            .collect::<Vec<_>>();
        let dts = bars
            .iter()
            .map(|bar| match self.spec.dt_label {
                DtLabelPolicy::CloseDt => bar.close_dt,
                DtLabelPolicy::StartDt => bar.start_dt,
            })
            .collect::<Vec<_>>();
        let opens = bars.iter().map(|bar| bar.open).collect::<Vec<_>>();
        let highs = bars.iter().map(|bar| bar.high).collect::<Vec<_>>();
        let lows = bars.iter().map(|bar| bar.low).collect::<Vec<_>>();
        let closes = bars.iter().map(|bar| bar.close).collect::<Vec<_>>();
        let volumes = bars.iter().map(|bar| bar.volume).collect::<Vec<_>>();
        let total_turnovers = bars
            .iter()
            .map(|bar| bar.total_turnover)
            .collect::<Vec<_>>();
        let num_trades = bars.iter().map(|bar| bar.num_trades).collect::<Vec<_>>();
        let limit_ups = bars.iter().map(|bar| bar.limit_up).collect::<Vec<_>>();
        let limit_downs = bars.iter().map(|bar| bar.limit_down).collect::<Vec<_>>();
        let start_dts = bars.iter().map(|bar| bar.start_dt).collect::<Vec<_>>();
        let close_dts = bars.iter().map(|bar| bar.close_dt).collect::<Vec<_>>();
        let timezone = self.spec.sessions.timezone.clone();
        let columns = vec![
            Arc::new(StringArray::from(instruments)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(dts).with_timezone(timezone.clone()))
                as ArrayRef,
            Arc::new(Float64Array::from(opens)) as ArrayRef,
            Arc::new(Float64Array::from(highs)) as ArrayRef,
            Arc::new(Float64Array::from(lows)) as ArrayRef,
            Arc::new(Float64Array::from(closes)) as ArrayRef,
            Arc::new(Float64Array::from(volumes)) as ArrayRef,
            Arc::new(Float64Array::from(total_turnovers)) as ArrayRef,
            Arc::new(Int64Array::from(num_trades)) as ArrayRef,
            Arc::new(Float64Array::from(limit_ups)) as ArrayRef,
            Arc::new(Float64Array::from(limit_downs)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(start_dts).with_timezone(timezone.clone()))
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(close_dts).with_timezone(timezone)) as ArrayRef,
        ];

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build bar generator output batch error=[{}]",
                    error
                ),
            }
        })
    }
}

impl Engine for BarGeneratorEngine {
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

        let instrument_array = table.column(&self.spec.columns.instrument)?;
        let dt_array = table.column(&self.spec.columns.dt)?;
        let price_array = table.column(&self.spec.columns.price)?;
        let volume_array = table.column(&self.spec.columns.volume)?;
        let total_turnover_array = table.column(&self.spec.columns.total_turnover)?;
        let num_trades_array =
            optional_table_column(&table, self.spec.columns.num_trades.as_deref())?;
        let limit_up_array = optional_table_column(&table, self.spec.columns.limit_up.as_deref())?;
        let limit_down_array =
            optional_table_column(&table, self.spec.columns.limit_down.as_deref())?;

        let instruments = checked_string_array(
            &instrument_array,
            &self.spec.columns.instrument,
            "instrument",
        )?;
        let dts = checked_timestamp_array(&dt_array, &self.spec.columns.dt)?;
        let prices = checked_float64_array(&price_array, &self.spec.columns.price, "price")?;
        let volumes = checked_float64_array(&volume_array, &self.spec.columns.volume, "volume")?;
        let total_turnovers = checked_float64_array(
            &total_turnover_array,
            &self.spec.columns.total_turnover,
            "total_turnover",
        )?;
        let num_trades = optional_int64_values(
            num_trades_array.as_ref(),
            self.spec.columns.num_trades.as_deref(),
        )?;
        let limit_ups = optional_float64_values(
            limit_up_array.as_ref(),
            self.spec.columns.limit_up.as_deref(),
            "limit_up",
        )?;
        let limit_downs = optional_float64_values(
            limit_down_array.as_ref(),
            self.spec.columns.limit_down.as_deref(),
            "limit_down",
        )?;

        let mut completed = Vec::new();

        for row_index in 0..table.num_rows() {
            let tick = TickRow::from_arrays(TickRowArrays {
                instruments,
                dts,
                prices,
                volumes,
                total_turnovers,
                num_trades,
                limit_ups,
                limit_downs,
                row_index,
            })?;
            let window_start = minute_start_ns(tick.dt);
            let window_end =
                window_start
                    .checked_add(BAR_FREQUENCY_1M_NS)
                    .ok_or(ZippyError::InvalidState {
                        status: "bar window end overflow",
                    })?;

            match self.open_bars.remove(&tick.instrument) {
                Some(mut open_bar) if open_bar.start_dt == window_start => {
                    open_bar.update(&tick);
                    self.open_bars.insert(tick.instrument.clone(), open_bar);
                }
                Some(open_bar) => {
                    completed.push(open_bar);
                    self.open_bars.insert(
                        tick.instrument.clone(),
                        OpenBar::from_tick(&tick, window_start, window_end),
                    );
                }
                None => {
                    self.open_bars.insert(
                        tick.instrument.clone(),
                        OpenBar::from_tick(&tick, window_start, window_end),
                    );
                }
            }
        }

        if completed.is_empty() {
            return Ok(vec![]);
        }

        completed.sort_by(|left, right| left.instrument.cmp(&right.instrument));
        let output = self.build_output_batch(&completed)?;
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        if self.open_bars.is_empty() {
            return Ok(vec![]);
        }

        let completed = self.open_bars.values().cloned().collect::<Vec<_>>();
        let output = self.build_output_batch(&completed)?;
        self.open_bars.clear();

        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            filtered_rows_total: self.pending_filtered_rows,
            ..Default::default()
        };
        self.pending_filtered_rows = 0;
        delta
    }
}

struct TickRow {
    instrument: String,
    dt: i64,
    price: f64,
    volume: f64,
    total_turnover: f64,
    num_trades: Option<i64>,
    limit_up: Option<f64>,
    limit_down: Option<f64>,
}

struct TickRowArrays<'a> {
    instruments: &'a StringArray,
    dts: &'a TimestampNanosecondArray,
    prices: &'a Float64Array,
    volumes: &'a Float64Array,
    total_turnovers: &'a Float64Array,
    num_trades: Option<&'a Int64Array>,
    limit_ups: Option<&'a Float64Array>,
    limit_downs: Option<&'a Float64Array>,
    row_index: usize,
}

impl TickRow {
    fn from_arrays(arrays: TickRowArrays<'_>) -> Result<Self> {
        ensure_non_null(arrays.instruments, arrays.row_index, "instrument")?;
        ensure_non_null(arrays.dts, arrays.row_index, "dt")?;
        ensure_non_null(arrays.prices, arrays.row_index, "price")?;
        ensure_non_null(arrays.volumes, arrays.row_index, "volume")?;
        ensure_non_null(arrays.total_turnovers, arrays.row_index, "total_turnover")?;

        Ok(Self {
            instrument: arrays.instruments.value(arrays.row_index).to_string(),
            dt: arrays.dts.value(arrays.row_index),
            price: arrays.prices.value(arrays.row_index),
            volume: arrays.volumes.value(arrays.row_index),
            total_turnover: arrays.total_turnovers.value(arrays.row_index),
            num_trades: optional_i64_at(arrays.num_trades, arrays.row_index),
            limit_up: optional_f64_at(arrays.limit_ups, arrays.row_index),
            limit_down: optional_f64_at(arrays.limit_downs, arrays.row_index),
        })
    }
}

#[derive(Clone)]
struct OpenBar {
    instrument: String,
    start_dt: i64,
    close_dt: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    total_turnover: f64,
    num_trades: Option<i64>,
    limit_up: Option<f64>,
    limit_down: Option<f64>,
}

impl OpenBar {
    fn from_tick(tick: &TickRow, start_dt: i64, close_dt: i64) -> Self {
        Self {
            instrument: tick.instrument.clone(),
            start_dt,
            close_dt,
            open: tick.price,
            high: tick.price,
            low: tick.price,
            close: tick.price,
            volume: tick.volume,
            total_turnover: tick.total_turnover,
            num_trades: tick.num_trades,
            limit_up: tick.limit_up,
            limit_down: tick.limit_down,
        }
    }

    fn update(&mut self, tick: &TickRow) {
        self.high = self.high.max(tick.price);
        self.low = self.low.min(tick.price);
        self.close = tick.price;
        self.volume += tick.volume;
        self.total_turnover += tick.total_turnover;
        self.num_trades = tick.num_trades.or(self.num_trades);
        self.limit_up = tick.limit_up.or(self.limit_up);
        self.limit_down = tick.limit_down.or(self.limit_down);
    }
}

fn minute_start_ns(dt: i64) -> i64 {
    dt.div_euclid(BAR_FREQUENCY_1M_NS) * BAR_FREQUENCY_1M_NS
}

fn optional_table_column(
    table: &SegmentTableView,
    column: Option<&str>,
) -> Result<Option<ArrayRef>> {
    column.map(|column| table.column(column)).transpose()
}

fn checked_string_array<'a>(
    array: &'a ArrayRef,
    column: &str,
    role: &str,
) -> Result<&'a StringArray> {
    let values = string_array(array, column)?;
    if values.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field contains nulls field=[{}]", role, column),
        });
    }

    Ok(values)
}

fn checked_timestamp_array<'a>(
    array: &'a ArrayRef,
    column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let values = timestamp_ns_array(array, column)?;
    if values.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("dt field contains nulls field=[{}]", column),
        });
    }

    Ok(values)
}

fn checked_float64_array<'a>(
    array: &'a ArrayRef,
    column: &str,
    role: &str,
) -> Result<&'a Float64Array> {
    let values = float64_array(array, column)?;
    if values.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field contains nulls field=[{}]", role, column),
        });
    }

    Ok(values)
}

fn optional_int64_values<'a>(
    array: Option<&'a ArrayRef>,
    column: Option<&str>,
) -> Result<Option<&'a Int64Array>> {
    match (array, column) {
        (Some(array), Some(column)) => Ok(Some(int64_array(array, column)?)),
        _ => Ok(None),
    }
}

fn optional_float64_values<'a>(
    array: Option<&'a ArrayRef>,
    column: Option<&str>,
    role: &str,
) -> Result<Option<&'a Float64Array>> {
    match (array, column) {
        (Some(array), Some(column)) => {
            let values = float64_array(array, column).map_err(|_| ZippyError::SchemaMismatch {
                reason: format!("{} field must be float64 field=[{}]", role, column),
            })?;
            Ok(Some(values))
        }
        _ => Ok(None),
    }
}

fn int64_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Int64Array> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("num_trades field must be int64 field=[{}]", column),
        })
}

fn ensure_non_null(array: &dyn Array, row_index: usize, role: &str) -> Result<()> {
    if array.is_null(row_index) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field contains nulls row_index=[{}]", role, row_index),
        });
    }

    Ok(())
}

fn optional_i64_at(values: Option<&Int64Array>, row_index: usize) -> Option<i64> {
    values.and_then(|values| {
        if values.is_null(row_index) {
            None
        } else {
            Some(values.value(row_index))
        }
    })
}

fn optional_f64_at(values: Option<&Float64Array>, row_index: usize) -> Option<f64> {
    values.and_then(|values| {
        if values.is_null(row_index) {
            None
        } else {
            Some(values.value(row_index))
        }
    })
}

fn validate_spec(schema: &Schema, spec: &BarGeneratorSpec) -> Result<()> {
    if spec.frequency != BAR_FREQUENCY_1M {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "bar frequency must be supported frequency=[{}]",
                spec.frequency
            ),
        });
    }

    if spec.sessions.regular.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "regular sessions must not be empty".to_string(),
        });
    }
    validate_session_windows("regular", &spec.sessions.regular)?;
    validate_session_windows("auction", &spec.sessions.auction)?;

    validate_instrument_column(schema, &spec.columns.instrument)?;
    validate_dt_column(schema, &spec.columns.dt)?;
    validate_float64_column(schema, &spec.columns.price, "price")?;
    validate_float64_column(schema, &spec.columns.volume, "volume")?;
    validate_float64_column(schema, &spec.columns.total_turnover, "total_turnover")?;

    if let Some(column) = &spec.columns.trading_day {
        validate_utf8_column(schema, column, "trading_day")?;
    }

    if let VolumeSpec::Cumulative {
        trading_day_column, ..
    } = &spec.volume
    {
        validate_utf8_column(schema, trading_day_column, "trading_day")?;
    }

    if let Some(column) = &spec.columns.num_trades {
        validate_int64_column(schema, column, "num_trades")?;
    }

    if let Some(column) = &spec.columns.limit_up {
        validate_float64_column(schema, column, "limit_up")?;
    }

    if let Some(column) = &spec.columns.limit_down {
        validate_float64_column(schema, column, "limit_down")?;
    }

    Ok(())
}

fn validate_session_windows(kind: &str, windows: &[SessionWindow]) -> Result<()> {
    for (index, window) in windows.iter().enumerate() {
        if window.start_seconds >= 86_400 || window.end_seconds > 86_400 {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    concat!(
                        "session window must be inside trading day kind=[{}] index=[{}] ",
                        "start_seconds=[{}] end_seconds=[{}]"
                    ),
                    kind, index, window.start_seconds, window.end_seconds
                ),
            });
        }

        if window.start_seconds >= window.end_seconds {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    concat!(
                        "session window start must be before end kind=[{}] index=[{}] ",
                        "start_seconds=[{}] end_seconds=[{}]"
                    ),
                    kind, index, window.start_seconds, window.end_seconds
                ),
            });
        }
    }

    Ok(())
}

fn build_output_schema(schema: &Schema, spec: &BarGeneratorSpec) -> Result<SchemaRef> {
    let instrument_field = schema
        .field_with_name(&spec.columns.instrument)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing utf8 instrument field field=[{}]",
                spec.columns.instrument
            ),
        })?;
    let timestamp_type = DataType::Timestamp(
        TimeUnit::Nanosecond,
        Some(spec.sessions.timezone.clone().into()),
    );

    Ok(Arc::new(Schema::new(vec![
        Arc::new(instrument_field.clone()),
        Arc::new(Field::new("dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("open", DataType::Float64, false)),
        Arc::new(Field::new("high", DataType::Float64, false)),
        Arc::new(Field::new("low", DataType::Float64, false)),
        Arc::new(Field::new("close", DataType::Float64, false)),
        Arc::new(Field::new("volume", DataType::Float64, false)),
        Arc::new(Field::new("total_turnover", DataType::Float64, false)),
        Arc::new(Field::new("num_trades", DataType::Int64, true)),
        Arc::new(Field::new("limit_up", DataType::Float64, true)),
        Arc::new(Field::new("limit_down", DataType::Float64, true)),
        Arc::new(Field::new("start_dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("close_dt", timestamp_type, false)),
    ])))
}

fn validate_instrument_column(schema: &Schema, column: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 instrument field field=[{}]", column),
        })?;

    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("instrument field must be utf8 field=[{}]", column),
        });
    }

    if field.is_nullable() {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("instrument field must be non-nullable field=[{}]", column),
        });
    }

    Ok(())
}

fn validate_utf8_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be utf8 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn validate_dt_column(schema: &Schema, column: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                column
            ),
        })?;

    if !matches!(
        field.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
    ) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                column
            ),
        });
    }

    Ok(())
}

fn validate_float64_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Float64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be float64 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn validate_int64_column(schema: &Schema, column: &str, role: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing int64 {} field field=[{}]", role, column),
        })?;

    if field.data_type() != &DataType::Int64 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be int64 field=[{}]", role, column),
        });
    }

    Ok(())
}

fn parse_seconds_since_midnight(value: &str) -> Result<u32> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("time must use hh:mm:ss format value=[{}]", value),
        });
    }

    let hour = parse_time_part(parts[0], "hour", value)?;
    let minute = parse_time_part(parts[1], "minute", value)?;
    let second = parse_time_part(parts[2], "second", value)?;

    if hour > 23 || minute > 59 || second > 59 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("time value out of range value=[{}]", value),
        });
    }

    Ok(hour * 3600 + minute * 60 + second)
}

fn parse_time_part(part: &str, role: &str, value: &str) -> Result<u32> {
    part.parse::<u32>()
        .map_err(|error| ZippyError::InvalidConfig {
            reason: format!(
                "invalid time {} value=[{}] part=[{}] error=[{}]",
                role, value, part, error
            ),
        })
}
