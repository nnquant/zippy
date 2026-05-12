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
const SECOND_NS: i64 = 1_000_000_000;
const LOCAL_DAY_SECONDS: i64 = 86_400;
const LOCAL_DAY_NS: i64 = LOCAL_DAY_SECONDS * SECOND_NS;

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

        if start_seconds == end_seconds {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "session start must not equal end start=[{}] end=[{}]",
                    start, end
                ),
            });
        }

        Ok(Self {
            start_seconds,
            end_seconds,
        })
    }

    /// Return whether the local second belongs to this session window.
    pub fn contains(&self, seconds: u32) -> bool {
        if self.wraps_midnight() {
            self.start_seconds <= seconds || seconds < self.end_seconds
        } else {
            self.start_seconds <= seconds && seconds < self.end_seconds
        }
    }

    fn wraps_midnight(&self) -> bool {
        self.start_seconds > self.end_seconds
    }
}

/// Session profile used to assign ticks into bar windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSessionSpec {
    pub timezone: String,
    pub regular: Vec<SessionWindow>,
    pub auction: Vec<SessionWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TickSessionKind {
    Regular,
    Auction,
    Outside,
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

/// Output data type for generated bar volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeOutputDtype {
    Int64,
    Float64,
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
    pub volume_output_dtype: VolumeOutputDtype,
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
    pending_auction_bars: BTreeMap<String, OpenBar>,
    open_auction_bars: BTreeMap<AuctionBarKey, OpenBar>,
    cumulative_states: BTreeMap<String, CumulativeState>,
    timezone_offset_seconds: i64,
    pending_filtered_rows: u64,
}

impl BarGeneratorEngine {
    /// Create a bar generator with a stable output schema.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        spec: BarGeneratorSpec,
    ) -> Result<Self> {
        let timezone_offset_seconds = validate_spec(input_schema.as_ref(), &spec)?;
        let output_schema = build_output_schema(input_schema.as_ref(), &spec)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            spec,
            open_bars: BTreeMap::new(),
            pending_auction_bars: BTreeMap::new(),
            open_auction_bars: BTreeMap::new(),
            cumulative_states: BTreeMap::new(),
            timezone_offset_seconds,
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
                DtLabelPolicy::StartDt => bar.window_start_dt,
            })
            .collect::<Vec<_>>();
        let opens = bars.iter().map(|bar| bar.open).collect::<Vec<_>>();
        let highs = bars.iter().map(|bar| bar.high).collect::<Vec<_>>();
        let lows = bars.iter().map(|bar| bar.low).collect::<Vec<_>>();
        let closes = bars.iter().map(|bar| bar.close).collect::<Vec<_>>();
        let volumes = volume_array_from_bars(bars, self.spec.volume_output_dtype)?;
        let total_turnovers = bars
            .iter()
            .map(|bar| bar.total_turnover)
            .collect::<Vec<_>>();
        let num_trades = bars.iter().map(|bar| bar.num_trades).collect::<Vec<_>>();
        let limit_ups = bars.iter().map(|bar| bar.limit_up).collect::<Vec<_>>();
        let limit_downs = bars.iter().map(|bar| bar.limit_down).collect::<Vec<_>>();
        let start_dts = bars.iter().map(|bar| bar.start_dt).collect::<Vec<_>>();
        let close_dts = bars.iter().map(|bar| bar.close_dt).collect::<Vec<_>>();
        let end_dts = bars.iter().map(|bar| bar.last_tick_dt).collect::<Vec<_>>();
        let timezone = self.spec.sessions.timezone.clone();
        let columns = vec![
            Arc::new(StringArray::from(instruments)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(dts).with_timezone(timezone.clone()))
                as ArrayRef,
            Arc::new(Float64Array::from(opens)) as ArrayRef,
            Arc::new(Float64Array::from(highs)) as ArrayRef,
            Arc::new(Float64Array::from(lows)) as ArrayRef,
            Arc::new(Float64Array::from(closes)) as ArrayRef,
            volumes,
            Arc::new(Float64Array::from(total_turnovers)) as ArrayRef,
            Arc::new(Int64Array::from(num_trades)) as ArrayRef,
            Arc::new(Float64Array::from(limit_ups)) as ArrayRef,
            Arc::new(Float64Array::from(limit_downs)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(start_dts).with_timezone(timezone.clone()))
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(close_dts).with_timezone(timezone.clone()))
                as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(end_dts).with_timezone(timezone)) as ArrayRef,
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
    fn compute_volume_delta(
        &mut self,
        tick: &TickRow,
        session_kind: TickSessionKind,
    ) -> Option<CumulativeDelta> {
        match &self.spec.volume {
            VolumeSpec::Delta => Some(CumulativeDelta {
                volume: tick.volume,
                total_turnover: tick.total_turnover,
                window_start: None,
            }),
            VolumeSpec::Cumulative { bootstrap, .. } => {
                let Some(trading_day) = tick.trading_day.as_ref() else {
                    self.pending_filtered_rows += 1;
                    return None;
                };

                let state = self
                    .cumulative_states
                    .entry(tick.instrument.clone())
                    .or_insert_with(|| CumulativeState {
                        trading_day: trading_day.clone(),
                        last_volume: 0.0,
                        last_total_turnover: 0.0,
                        last_dt: tick.dt,
                        last_session_kind: session_kind,
                        initialized: false,
                        has_valid_delta: false,
                    });

                if !state.initialized || state.trading_day != *trading_day {
                    state.trading_day = trading_day.clone();
                    state.last_volume = tick.volume;
                    state.last_total_turnover = tick.total_turnover;
                    state.last_dt = tick.dt;
                    state.last_session_kind = session_kind;
                    state.initialized = true;
                    state.has_valid_delta = false;

                    return match bootstrap {
                        BootstrapPolicy::SkipFirstDelta => None,
                        BootstrapPolicy::FromZero => {
                            state.has_valid_delta = true;
                            Some(CumulativeDelta {
                                volume: tick.volume,
                                total_turnover: tick.total_turnover,
                                window_start: None,
                            })
                        }
                    };
                }

                let volume_delta = tick.volume - state.last_volume;
                let total_turnover_delta = tick.total_turnover - state.last_total_turnover;
                let window_start = if state.has_valid_delta {
                    None
                } else if state.last_session_kind == TickSessionKind::Regular {
                    Some(minute_start_ns(state.last_dt))
                } else {
                    None
                };
                state.last_volume = tick.volume;
                state.last_total_turnover = tick.total_turnover;
                state.last_dt = tick.dt;
                state.last_session_kind = session_kind;

                if volume_delta < 0.0 || total_turnover_delta < 0.0 {
                    self.pending_filtered_rows += 1;
                    return None;
                }

                state.has_valid_delta = true;
                Some(CumulativeDelta {
                    volume: volume_delta,
                    total_turnover: total_turnover_delta,
                    window_start,
                })
            }
        }
    }

    fn handle_regular_tick(
        &mut self,
        mut tick: TickRow,
        completed: &mut Vec<OpenBar>,
    ) -> Result<()> {
        let current_window_start = minute_start_ns(tick.dt);
        if self.spec.auction == AuctionPolicy::EmitSeparateBar {
            self.drain_completed_auction_bars(current_window_start, completed);
        }

        let Some(delta) = self.compute_volume_delta(&tick, TickSessionKind::Regular) else {
            return Ok(());
        };
        tick.volume = delta.volume;
        tick.total_turnover = delta.total_turnover;

        let window_start = delta.window_start.unwrap_or(current_window_start);
        let window_end =
            window_start
                .checked_add(BAR_FREQUENCY_1M_NS)
                .ok_or(ZippyError::InvalidState {
                    status: "bar window end overflow",
                })?;
        let mut incoming_bar = if self.spec.auction == AuctionPolicy::MergeToFirstRegularBar {
            self.pending_auction_bars
                .remove(&tick.instrument)
                .and_then(|mut auction_bar| {
                    if auction_bar_matches_regular_window(
                        &auction_bar,
                        window_start,
                        window_end,
                        self.timezone_offset_seconds,
                    ) {
                        auction_bar.window_start_dt = window_start;
                        auction_bar.close_dt = window_end;
                        auction_bar.update(&tick);
                        Some(auction_bar)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| OpenBar::from_tick(&tick, window_start, window_end))
        } else {
            OpenBar::from_tick(&tick, window_start, window_end)
        };
        incoming_bar.window_start_dt = window_start;
        incoming_bar.close_dt = window_end;

        self.insert_regular_bar(incoming_bar, current_window_start, completed);

        Ok(())
    }

    fn handle_auction_tick(&mut self, mut tick: TickRow) -> Result<()> {
        match self.spec.auction {
            AuctionPolicy::Drop => {
                if matches!(self.spec.volume, VolumeSpec::Cumulative { .. }) {
                    let filtered_before = self.pending_filtered_rows;
                    let _ = self.compute_volume_delta(&tick, TickSessionKind::Auction);
                    if self.pending_filtered_rows == filtered_before {
                        self.pending_filtered_rows += 1;
                    }
                } else {
                    self.pending_filtered_rows += 1;
                }
            }
            AuctionPolicy::MergeToFirstRegularBar => {
                let Some(delta) = self.compute_volume_delta(&tick, TickSessionKind::Auction) else {
                    return Ok(());
                };
                tick.volume = delta.volume;
                tick.total_turnover = delta.total_turnover;

                let Some((window_start_dt, close_dt)) = auction_window_bounds(
                    &self.spec.sessions,
                    tick.dt,
                    self.timezone_offset_seconds,
                ) else {
                    self.pending_filtered_rows += 1;
                    return Ok(());
                };

                match self.pending_auction_bars.remove(&tick.instrument) {
                    Some(mut auction_bar) => {
                        auction_bar.update(&tick);
                        self.pending_auction_bars
                            .insert(tick.instrument.clone(), auction_bar);
                    }
                    None => {
                        self.pending_auction_bars.insert(
                            tick.instrument.clone(),
                            OpenBar::from_tick(&tick, window_start_dt, close_dt),
                        );
                    }
                }
            }
            AuctionPolicy::EmitSeparateBar => {
                let Some((window_start_dt, close_dt)) = auction_window_bounds(
                    &self.spec.sessions,
                    tick.dt,
                    self.timezone_offset_seconds,
                ) else {
                    self.pending_filtered_rows += 1;
                    return Ok(());
                };
                let Some(delta) = self.compute_volume_delta(&tick, TickSessionKind::Auction) else {
                    return Ok(());
                };
                tick.volume = delta.volume;
                tick.total_turnover = delta.total_turnover;

                let key = AuctionBarKey {
                    instrument: tick.instrument.clone(),
                    window_start_dt,
                    close_dt,
                };
                match self.open_auction_bars.remove(&key) {
                    Some(mut auction_bar) => {
                        auction_bar.update(&tick);
                        self.open_auction_bars.insert(key, auction_bar);
                    }
                    None => {
                        self.open_auction_bars
                            .insert(key, OpenBar::from_tick(&tick, window_start_dt, close_dt));
                    }
                }
            }
        }

        Ok(())
    }

    fn drain_completed_auction_bars(
        &mut self,
        current_window_start: i64,
        completed: &mut Vec<OpenBar>,
    ) {
        let completed_keys = self
            .open_auction_bars
            .keys()
            .filter(|key| key.close_dt <= current_window_start)
            .cloned()
            .collect::<Vec<_>>();

        for key in completed_keys {
            if let Some(bar) = self.open_auction_bars.remove(&key) {
                completed.push(bar);
            }
        }
    }

    fn insert_regular_bar(
        &mut self,
        incoming_bar: OpenBar,
        current_window_start: i64,
        completed: &mut Vec<OpenBar>,
    ) {
        let instrument = incoming_bar.instrument.clone();
        match self.open_bars.remove(&instrument) {
            Some(mut open_bar) if open_bar.window_start_dt == incoming_bar.window_start_dt => {
                open_bar.merge_from_bar(&incoming_bar);
                self.open_bars.insert(instrument, open_bar);
            }
            Some(open_bar) => {
                completed.push(open_bar);
                if incoming_bar.close_dt <= current_window_start {
                    completed.push(incoming_bar);
                } else {
                    self.open_bars.insert(instrument, incoming_bar);
                }
            }
            None => {
                if incoming_bar.close_dt <= current_window_start {
                    completed.push(incoming_bar);
                } else {
                    self.open_bars.insert(instrument, incoming_bar);
                }
            }
        }
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
        let trading_day_array = match &self.spec.volume {
            VolumeSpec::Delta => None,
            VolumeSpec::Cumulative {
                trading_day_column, ..
            } => Some(table.column(trading_day_column)?),
        };
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
        let prices = checked_float64_array(&price_array, &self.spec.columns.price)?;
        let volumes =
            checked_numeric_f64_array(&volume_array, &self.spec.columns.volume, "volume")?;
        let total_turnovers =
            checked_float64_array(&total_turnover_array, &self.spec.columns.total_turnover)?;
        let trading_days = match (&trading_day_array, &self.spec.volume) {
            (
                Some(array),
                VolumeSpec::Cumulative {
                    trading_day_column, ..
                },
            ) => Some(string_array(array, trading_day_column)?),
            _ => None,
        };
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
            if has_missing_market_data(prices, volumes, total_turnovers, row_index) {
                self.pending_filtered_rows += 1;
                continue;
            }

            let tick = TickRow::from_arrays(TickRowArrays {
                instruments,
                dts,
                prices,
                volumes,
                total_turnovers,
                trading_days,
                num_trades,
                limit_ups,
                limit_downs,
                row_index,
            })?;

            match classify_session(&self.spec.sessions, tick.dt, self.timezone_offset_seconds) {
                TickSessionKind::Regular => self.handle_regular_tick(tick, &mut completed)?,
                TickSessionKind::Auction => self.handle_auction_tick(tick)?,
                TickSessionKind::Outside => {
                    self.pending_filtered_rows += 1;
                }
            }
        }

        if completed.is_empty() {
            return Ok(vec![]);
        }

        sort_bars(&mut completed);
        let output = self.build_output_batch(&completed)?;
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        self.pending_auction_bars.clear();

        if self.open_bars.is_empty() && self.open_auction_bars.is_empty() {
            return Ok(vec![]);
        }

        let mut completed = self.open_auction_bars.values().cloned().collect::<Vec<_>>();
        completed.extend(self.open_bars.values().cloned());
        sort_bars(&mut completed);
        let output = self.build_output_batch(&completed)?;
        self.open_bars.clear();
        self.open_auction_bars.clear();

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
    trading_day: Option<String>,
    num_trades: Option<i64>,
    limit_up: Option<f64>,
    limit_down: Option<f64>,
}

struct TickRowArrays<'a> {
    instruments: &'a StringArray,
    dts: &'a TimestampNanosecondArray,
    prices: &'a Float64Array,
    volumes: NumericF64Array<'a>,
    total_turnovers: &'a Float64Array,
    trading_days: Option<&'a StringArray>,
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
        ensure_numeric_non_null(arrays.volumes, arrays.row_index, "volume")?;
        ensure_non_null(arrays.total_turnovers, arrays.row_index, "total_turnover")?;

        Ok(Self {
            instrument: arrays.instruments.value(arrays.row_index).to_string(),
            dt: arrays.dts.value(arrays.row_index),
            price: arrays.prices.value(arrays.row_index),
            volume: arrays.volumes.value(arrays.row_index),
            total_turnover: arrays.total_turnovers.value(arrays.row_index),
            trading_day: optional_string_at(arrays.trading_days, arrays.row_index),
            num_trades: optional_i64_at(arrays.num_trades, arrays.row_index),
            limit_up: optional_f64_at(arrays.limit_ups, arrays.row_index),
            limit_down: optional_f64_at(arrays.limit_downs, arrays.row_index),
        })
    }
}

#[derive(Clone)]
struct OpenBar {
    instrument: String,
    window_start_dt: i64,
    last_tick_dt: i64,
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

struct CumulativeState {
    trading_day: String,
    last_volume: f64,
    last_total_turnover: f64,
    last_dt: i64,
    last_session_kind: TickSessionKind,
    initialized: bool,
    has_valid_delta: bool,
}

struct CumulativeDelta {
    volume: f64,
    total_turnover: f64,
    window_start: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AuctionBarKey {
    instrument: String,
    window_start_dt: i64,
    close_dt: i64,
}

impl OpenBar {
    fn from_tick(tick: &TickRow, window_start_dt: i64, close_dt: i64) -> Self {
        Self {
            instrument: tick.instrument.clone(),
            window_start_dt,
            last_tick_dt: tick.dt,
            start_dt: tick.dt,
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
        self.last_tick_dt = tick.dt;
        self.volume += tick.volume;
        self.total_turnover += tick.total_turnover;
        self.num_trades = sum_optional_i64(self.num_trades, tick.num_trades);
        self.limit_up = tick.limit_up.or(self.limit_up);
        self.limit_down = tick.limit_down.or(self.limit_down);
    }

    fn merge_from_bar(&mut self, bar: &OpenBar) {
        self.high = self.high.max(bar.high);
        self.low = self.low.min(bar.low);
        self.close = bar.close;
        self.close_dt = bar.close_dt;
        self.last_tick_dt = bar.last_tick_dt;
        self.volume += bar.volume;
        self.total_turnover += bar.total_turnover;
        self.num_trades = sum_optional_i64(self.num_trades, bar.num_trades);
        self.limit_up = bar.limit_up.or(self.limit_up);
        self.limit_down = bar.limit_down.or(self.limit_down);
    }
}

fn minute_start_ns(dt: i64) -> i64 {
    dt.div_euclid(BAR_FREQUENCY_1M_NS) * BAR_FREQUENCY_1M_NS
}

fn classify_session(
    spec: &BarSessionSpec,
    dt_ns: i64,
    timezone_offset_seconds: i64,
) -> TickSessionKind {
    let seconds = local_seconds_of_day(dt_ns, timezone_offset_seconds);
    if spec.regular.iter().any(|window| window.contains(seconds)) {
        return TickSessionKind::Regular;
    }
    if spec.auction.iter().any(|window| window.contains(seconds)) {
        return TickSessionKind::Auction;
    }

    TickSessionKind::Outside
}

fn auction_window_bounds(
    spec: &BarSessionSpec,
    dt_ns: i64,
    timezone_offset_seconds: i64,
) -> Option<(i64, i64)> {
    let seconds = local_seconds_of_day(dt_ns, timezone_offset_seconds);
    let window = spec
        .auction
        .iter()
        .find(|window| window.contains(seconds))?;
    let day_start = local_day_start_ns(dt_ns, timezone_offset_seconds);
    let (start_day, close_day) = if !window.wraps_midnight() {
        (day_start, day_start)
    } else if seconds >= window.start_seconds {
        (day_start, day_start + LOCAL_DAY_NS)
    } else {
        (day_start - LOCAL_DAY_NS, day_start)
    };
    let window_start_dt = start_day + i64::from(window.start_seconds) * SECOND_NS;
    let close_dt = close_day + i64::from(window.end_seconds) * SECOND_NS;

    Some((window_start_dt, close_dt))
}

fn sort_bars(bars: &mut [OpenBar]) {
    bars.sort_by(|left, right| {
        (
            left.close_dt,
            left.window_start_dt,
            left.instrument.as_str(),
        )
            .cmp(&(
                right.close_dt,
                right.window_start_dt,
                right.instrument.as_str(),
            ))
    });
}

fn volume_array_from_bars(bars: &[OpenBar], output_dtype: VolumeOutputDtype) -> Result<ArrayRef> {
    match output_dtype {
        VolumeOutputDtype::Float64 => Ok(Arc::new(Float64Array::from(
            bars.iter().map(|bar| bar.volume).collect::<Vec<_>>(),
        )) as ArrayRef),
        VolumeOutputDtype::Int64 => {
            let volumes = bars
                .iter()
                .map(|bar| volume_to_i64(bar.volume))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Int64Array::from(volumes)) as ArrayRef)
        }
    }
}

fn volume_to_i64(value: f64) -> Result<i64> {
    if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(ZippyError::InvalidState {
            status: "bar volume cannot be represented as int64",
        });
    }

    let rounded = value.round();
    if (value - rounded).abs() > f64::EPSILON {
        return Err(ZippyError::InvalidState {
            status: "bar volume must be integer-valued for int64 output",
        });
    }

    Ok(rounded as i64)
}

fn local_seconds_of_day(dt_ns: i64, timezone_offset_seconds: i64) -> u32 {
    let seconds = dt_ns.div_euclid(SECOND_NS);
    (seconds + timezone_offset_seconds).rem_euclid(LOCAL_DAY_SECONDS) as u32
}

fn local_day_start_ns(dt_ns: i64, timezone_offset_seconds: i64) -> i64 {
    let seconds = dt_ns.div_euclid(SECOND_NS);
    ((seconds + timezone_offset_seconds).div_euclid(LOCAL_DAY_SECONDS) * LOCAL_DAY_SECONDS
        - timezone_offset_seconds)
        * SECOND_NS
}

fn auction_bar_matches_regular_window(
    auction_bar: &OpenBar,
    regular_window_start: i64,
    regular_window_end: i64,
    timezone_offset_seconds: i64,
) -> bool {
    auction_bar.close_dt <= regular_window_end
        && local_day_start_ns(auction_bar.close_dt, timezone_offset_seconds)
            == local_day_start_ns(regular_window_start, timezone_offset_seconds)
}

fn sum_optional_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
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

fn checked_float64_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Float64Array> {
    let values = float64_array(array, column)?;

    Ok(values)
}

#[derive(Clone, Copy)]
enum NumericF64Array<'a> {
    Float64(&'a Float64Array),
    Int64(&'a Int64Array),
}

impl NumericF64Array<'_> {
    fn is_null(self, row_index: usize) -> bool {
        match self {
            Self::Float64(values) => values.is_null(row_index),
            Self::Int64(values) => values.is_null(row_index),
        }
    }

    fn value(self, row_index: usize) -> f64 {
        match self {
            Self::Float64(values) => values.value(row_index),
            Self::Int64(values) => values.value(row_index) as f64,
        }
    }
}

fn checked_numeric_f64_array<'a>(
    array: &'a ArrayRef,
    column: &str,
    role: &str,
) -> Result<NumericF64Array<'a>> {
    let values = if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        NumericF64Array::Float64(values)
    } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        NumericF64Array::Int64(values)
    } else {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("{} field must be float64 or int64 field=[{}]", role, column),
        });
    };

    Ok(values)
}

fn has_missing_market_data(
    prices: &Float64Array,
    volumes: NumericF64Array<'_>,
    total_turnovers: &Float64Array,
    row_index: usize,
) -> bool {
    prices.is_null(row_index)
        || volumes.is_null(row_index)
        || total_turnovers.is_null(row_index)
        || !prices.value(row_index).is_finite()
        || !volumes.value(row_index).is_finite()
        || !total_turnovers.value(row_index).is_finite()
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

fn ensure_numeric_non_null(
    values: NumericF64Array<'_>,
    row_index: usize,
    role: &str,
) -> Result<()> {
    if values.is_null(row_index) {
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

fn optional_string_at(values: Option<&StringArray>, row_index: usize) -> Option<String> {
    values.and_then(|values| {
        if values.is_null(row_index) {
            None
        } else {
            Some(values.value(row_index).to_string())
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

fn validate_spec(schema: &Schema, spec: &BarGeneratorSpec) -> Result<i64> {
    if spec.frequency != BAR_FREQUENCY_1M {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "bar frequency must be supported frequency=[{}]",
                spec.frequency
            ),
        });
    }
    let timezone_offset_seconds = timezone_offset_seconds(&spec.sessions.timezone)?;

    if spec.sessions.regular.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "regular sessions must not be empty".to_string(),
        });
    }
    validate_session_windows("regular", &spec.sessions.regular)?;
    validate_session_windows("auction", &spec.sessions.auction)?;

    validate_instrument_column(schema, &spec.columns.instrument)?;
    validate_dt_column(schema, &spec.columns.dt, &spec.sessions.timezone)?;
    validate_float64_column(schema, &spec.columns.price, "price")?;
    validate_volume_column(schema, &spec.columns.volume)?;
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

    Ok(timezone_offset_seconds)
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

        if window.start_seconds == window.end_seconds {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    concat!(
                        "session window start must not equal end kind=[{}] index=[{}] ",
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
        Arc::new(Field::new(
            "volume",
            match spec.volume_output_dtype {
                VolumeOutputDtype::Int64 => DataType::Int64,
                VolumeOutputDtype::Float64 => DataType::Float64,
            },
            false,
        )),
        Arc::new(Field::new("total_turnover", DataType::Float64, false)),
        Arc::new(Field::new("num_trades", DataType::Int64, true)),
        Arc::new(Field::new("limit_up", DataType::Float64, true)),
        Arc::new(Field::new("limit_down", DataType::Float64, true)),
        Arc::new(Field::new("start_dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("close_dt", timestamp_type.clone(), false)),
        Arc::new(Field::new("end_dt", timestamp_type, false)),
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

fn validate_dt_column(schema: &Schema, column: &str, profile_timezone: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                column
            ),
        })?;

    match field.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
            if timezone.as_ref() == profile_timezone =>
        {
            Ok(())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => {
            Err(ZippyError::SchemaMismatch {
                reason: format!(
                    concat!(
                        "dt field timezone must match session profile field=[{}] ",
                        "timezone=[{}] profile_timezone=[{}]"
                    ),
                    column, timezone, profile_timezone
                ),
            })
        }
        _ => Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                column
            ),
        }),
    }
}

fn timezone_offset_seconds(timezone: &str) -> Result<i64> {
    match timezone {
        "UTC" | "Etc/UTC" => Ok(0),
        "Asia/Shanghai" => Ok(8 * 60 * 60),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!("unsupported session timezone timezone=[{}]", timezone),
        }),
    }
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

fn validate_volume_column(schema: &Schema, column: &str) -> Result<()> {
    let field = schema
        .field_with_name(column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing float64 or int64 volume field field=[{}]", column),
        })?;

    if !matches!(field.data_type(), DataType::Float64 | DataType::Int64) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("volume field must be float64 or int64 field=[{}]", column),
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
