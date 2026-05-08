# BarGeneratorEngine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dedicated `BarGeneratorEngine` that generates 1-minute multi-instrument bars from tick data using a profile-based API.

**Architecture:** Add the core engine and profile spec structs in `zippy-engines`, expose them through `zippy-python`, and provide a Python `zp.bar` namespace for custom profile construction. Plugins can pass profile objects that implement `to_bar_generator_spec()`; Rust only consumes the normalized native spec.

**Tech Stack:** Rust 2021, Arrow 53.3, PyO3, PyArrow, existing Zippy `Engine` runtime, Python facade in `python/zippy/__init__.py`.

---

## File Structure

- Create: `crates/zippy-engines/src/bar_generator.rs`
  - Owns `BarGeneratorEngine`, normalized spec structs, session classification, cumulative volume state, open bar state, and output batch construction.
- Modify: `crates/zippy-engines/src/lib.rs`
  - Exports `bar_generator` module and `BarGeneratorEngine` public type.
- Create: `crates/zippy-engines/tests/bar_generator_engine.rs`
  - Covers Rust behavior: schema validation, 1m bar generation, multi-instrument state, sessions, auction policy, cumulative volume resets, and nullable optional columns.
- Modify: `crates/zippy-python/src/lib.rs`
  - Adds `BarGeneratorEngine` PyO3 wrapper, profile normalization, runtime source linking, downstream support, config output, and module export.
- Modify: `python/zippy/_internal.pyi`
  - Adds type stubs for native `BarGeneratorEngine`.
- Modify: `python/zippy/__init__.py`
  - Imports and exports top-level `BarGeneratorEngine`; adds `zp.bar` namespace with IDE-friendly custom profile classes and builders.
- Modify: `pytests/test_python_api.py`
  - Adds Python facade tests for `zp.bar.BarGeneratorProfile`, plugin profile protocol, native engine config, and session materialization.

## Implementation Tasks

### Task 1: Rust Spec And Output Schema

**Files:**
- Create: `crates/zippy-engines/src/bar_generator.rs`
- Modify: `crates/zippy-engines/src/lib.rs`
- Test: `crates/zippy-engines/tests/bar_generator_engine.rs`

- [ ] **Step 1: Write failing schema tests**

Add `crates/zippy-engines/tests/bar_generator_engine.rs` with:

```rust
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::{
    AuctionPolicy, BarGeneratorEngine, BarGeneratorSpec, BarInputColumns, BarSessionSpec,
    BootstrapPolicy, DtLabelPolicy, SessionWindow, VolumeSpec,
};

const MINUTE_NS: i64 = 60_000_000_000;
const TZ: &str = "Asia/Shanghai";

fn tick_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("dt", DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into())), false),
        Field::new("last_price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("num_trades", DataType::Int64, true),
        Field::new("upper_limit_price", DataType::Float64, true),
        Field::new("lower_limit_price", DataType::Float64, true),
    ]))
}

fn full_columns() -> BarInputColumns {
    BarInputColumns {
        instrument: "instrument_id".to_string(),
        dt: "dt".to_string(),
        price: "last_price".to_string(),
        volume: "volume".to_string(),
        total_turnover: "turnover".to_string(),
        trading_day: Some("trading_day".to_string()),
        num_trades: Some("num_trades".to_string()),
        limit_up: Some("upper_limit_price".to_string()),
        limit_down: Some("lower_limit_price".to_string()),
    }
}

fn regular_session() -> BarSessionSpec {
    BarSessionSpec {
        timezone: TZ.to_string(),
        regular: vec![SessionWindow::parse("09:30:00", "15:00:00").unwrap()],
        auction: vec![],
    }
}

fn delta_spec() -> BarGeneratorSpec {
    BarGeneratorSpec {
        frequency: "1m".to_string(),
        columns: full_columns(),
        sessions: regular_session(),
        volume: VolumeSpec::Delta,
        auction: AuctionPolicy::Drop,
        dt_label: DtLabelPolicy::CloseDt,
    }
}

#[test]
fn bar_generator_output_schema_is_stable() {
    let engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let fields = engine.output_schema().fields();
    let names = fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "instrument_id",
            "dt",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "total_turnover",
            "num_trades",
            "limit_up",
            "limit_down",
            "start_dt",
            "close_dt",
        ]
    );
    assert_eq!(fields[0].data_type(), &DataType::Utf8);
    assert_eq!(
        fields[1].data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some(TZ.into()))
    );
    assert!(fields[8].is_nullable());
    assert!(fields[9].is_nullable());
    assert!(fields[10].is_nullable());
}
```

- [ ] **Step 2: Run schema test and verify it fails**

Run:

```bash
cargo test -p zippy-engines bar_generator_output_schema_is_stable
```

Expected: FAIL with unresolved imports for `BarGeneratorEngine`, `BarGeneratorSpec`, and related spec types.

- [ ] **Step 3: Add initial Rust spec types and output schema**

Create `crates/zippy-engines/src/bar_generator.rs` with public structs:

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

pub const BAR_FREQUENCY_1M: &str = "1m";

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWindow {
    pub start_seconds: u32,
    pub end_seconds: u32,
}

impl SessionWindow {
    pub fn parse(start: &str, end: &str) -> Result<Self> {
        Ok(Self {
            start_seconds: parse_hhmmss(start)?,
            end_seconds: parse_hhmmss(end)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSessionSpec {
    pub timezone: String,
    pub regular: Vec<SessionWindow>,
    pub auction: Vec<SessionWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapPolicy {
    SkipFirstDelta,
    FromZero,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VolumeSpec {
    Delta,
    Cumulative {
        trading_day_column: String,
        bootstrap: BootstrapPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuctionPolicy {
    Drop,
    MergeToFirstRegularBar,
    EmitSeparateBar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtLabelPolicy {
    CloseDt,
    StartDt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarGeneratorSpec {
    pub frequency: String,
    pub columns: BarInputColumns,
    pub sessions: BarSessionSpec,
    pub volume: VolumeSpec,
    pub auction: AuctionPolicy,
    pub dt_label: DtLabelPolicy,
}

pub struct BarGeneratorEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    spec: BarGeneratorSpec,
    pending_filtered_rows: u64,
}

impl BarGeneratorEngine {
    pub fn new(name: impl Into<String>, input_schema: SchemaRef, spec: BarGeneratorSpec) -> Result<Self> {
        validate_spec(&spec)?;
        validate_input_schema(input_schema.as_ref(), &spec)?;
        let output_schema = build_output_schema(input_schema.as_ref(), &spec)?;

        Ok(Self {
            name: name.into(),
            input_schema,
            output_schema,
            spec,
            pending_filtered_rows: 0,
        })
    }
}
```

Also add helper functions:

```rust
fn parse_hhmmss(value: &str) -> Result<u32> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("session time must use HH:MM:SS value=[{}]", value),
        });
    }
    let hour = parts[0].parse::<u32>().map_err(|_| ZippyError::InvalidConfig {
        reason: format!("invalid session hour value=[{}]", value),
    })?;
    let minute = parts[1].parse::<u32>().map_err(|_| ZippyError::InvalidConfig {
        reason: format!("invalid session minute value=[{}]", value),
    })?;
    let second = parts[2].parse::<u32>().map_err(|_| ZippyError::InvalidConfig {
        reason: format!("invalid session second value=[{}]", value),
    })?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("session time out of range value=[{}]", value),
        });
    }
    Ok(hour * 3600 + minute * 60 + second)
}
```

Implement `Engine` minimally:

```rust
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

    fn on_data(&mut self, _table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        Ok(vec![])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            filtered_rows_total: self.pending_filtered_rows,
            ..EngineMetricsDelta::default()
        };
        self.pending_filtered_rows = 0;
        delta
    }
}
```

Add validation and output schema construction:

```rust
fn build_output_schema(input_schema: &Schema, spec: &BarGeneratorSpec) -> Result<SchemaRef> {
    let instrument_field = input_schema
        .field_with_name(&spec.columns.instrument)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing bar instrument field field=[{}]", spec.columns.instrument),
        })?
        .clone();
    let tz = spec.sessions.timezone.clone();
    Ok(Arc::new(Schema::new(vec![
        Arc::new(instrument_field),
        Arc::new(Field::new("dt", DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.clone().into())), false)),
        Arc::new(Field::new("open", DataType::Float64, false)),
        Arc::new(Field::new("high", DataType::Float64, false)),
        Arc::new(Field::new("low", DataType::Float64, false)),
        Arc::new(Field::new("close", DataType::Float64, false)),
        Arc::new(Field::new("volume", DataType::Float64, false)),
        Arc::new(Field::new("total_turnover", DataType::Float64, false)),
        Arc::new(Field::new("num_trades", DataType::Int64, true)),
        Arc::new(Field::new("limit_up", DataType::Float64, true)),
        Arc::new(Field::new("limit_down", DataType::Float64, true)),
        Arc::new(Field::new("start_dt", DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.clone().into())), false)),
        Arc::new(Field::new("close_dt", DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())), false)),
    ])))
}
```

Modify `crates/zippy-engines/src/lib.rs`:

```rust
pub mod bar_generator;
pub use bar_generator::{
    AuctionPolicy, BarGeneratorEngine, BarGeneratorSpec, BarInputColumns, BarSessionSpec,
    BootstrapPolicy, DtLabelPolicy, SessionWindow, VolumeSpec,
};
```

- [ ] **Step 4: Run schema test and verify it passes**

Run:

```bash
cargo test -p zippy-engines bar_generator_output_schema_is_stable
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add crates/zippy-engines/src/bar_generator.rs crates/zippy-engines/src/lib.rs crates/zippy-engines/tests/bar_generator_engine.rs
git commit -m "feat: add bar generator spec schema"
```

### Task 2: Basic Delta-Mode Bar Generation

**Files:**
- Modify: `crates/zippy-engines/src/bar_generator.rs`
- Modify: `crates/zippy-engines/tests/bar_generator_engine.rs`

- [ ] **Step 1: Add failing tests for delta-mode OHLC and multi-instrument state**

Append helpers and tests to `crates/zippy-engines/tests/bar_generator_engine.rs`:

```rust
fn tick_batch(
    instruments: Vec<&str>,
    dts: Vec<i64>,
    prices: Vec<f64>,
    volumes: Vec<f64>,
    turnovers: Vec<f64>,
    trading_days: Vec<&str>,
) -> SegmentTableView {
    let rows = instruments.len();
    SegmentTableView::from_record_batch(
        RecordBatch::try_new(
            tick_schema(),
            vec![
                Arc::new(StringArray::from(instruments)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(dts).with_timezone(TZ)) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
                Arc::new(Float64Array::from(volumes)) as ArrayRef,
                Arc::new(Float64Array::from(turnovers)) as ArrayRef,
                Arc::new(StringArray::from(trading_days)) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1); rows])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(999.0); rows])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.0); rows])) as ArrayRef,
            ],
        )
        .unwrap(),
    )
}

fn f64_column(table: &SegmentTableView, name: &str) -> Vec<f64> {
    let column = table.column(name).unwrap();
    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..array.len()).map(|index| array.value(index)).collect()
}

fn string_column(table: &SegmentTableView, name: &str) -> Vec<String> {
    let column = table.column(name).unwrap();
    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
    (0..array.len()).map(|index| array.value(index).to_string()).collect()
}

fn ts_column(table: &SegmentTableView, name: &str) -> Vec<i64> {
    let column = table.column(name).unwrap();
    let array = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
    (0..array.len()).map(|index| array.value(index)).collect()
}

#[test]
fn bar_generator_emits_completed_delta_bar_on_minute_transition() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![30 * 1_000_000_000, 45 * 1_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 12.0, 11.0],
            vec![2.0, 3.0, 5.0],
            vec![20.0, 36.0, 55.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_column(&outputs[0], "instrument_id"), vec!["rb2601"]);
    assert_eq!(ts_column(&outputs[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&outputs[0], "close_dt"), vec![MINUTE_NS]);
    assert_eq!(ts_column(&outputs[0], "dt"), vec![MINUTE_NS]);
    assert_eq!(f64_column(&outputs[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&outputs[0], "high"), vec![12.0]);
    assert_eq!(f64_column(&outputs[0], "low"), vec![10.0]);
    assert_eq!(f64_column(&outputs[0], "close"), vec![12.0]);
    assert_eq!(f64_column(&outputs[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&outputs[0], "total_turnover"), vec![56.0]);
}
```

- [ ] **Step 2: Run new delta test and verify it fails**

Run:

```bash
cargo test -p zippy-engines bar_generator_emits_completed_delta_bar_on_minute_transition
```

Expected: FAIL because `on_data()` currently emits no outputs.

- [ ] **Step 3: Implement `OpenBar`, row extraction, and delta updates**

In `bar_generator.rs`, add:

```rust
use std::collections::BTreeMap;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
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
    fn new(row: TickRow, start_dt: i64, close_dt: i64, delta_volume: f64, delta_turnover: f64) -> Self {
        Self {
            instrument: row.instrument,
            start_dt,
            close_dt,
            open: row.price,
            high: row.price,
            low: row.price,
            close: row.price,
            volume: delta_volume,
            total_turnover: delta_turnover,
            num_trades: row.num_trades,
            limit_up: row.limit_up,
            limit_down: row.limit_down,
        }
    }

    fn update(&mut self, row: TickRow, delta_volume: f64, delta_turnover: f64) {
        self.high = self.high.max(row.price);
        self.low = self.low.min(row.price);
        self.close = row.price;
        self.volume += delta_volume;
        self.total_turnover += delta_turnover;
        self.num_trades = row.num_trades.or(self.num_trades);
        self.limit_up = row.limit_up.or(self.limit_up);
        self.limit_down = row.limit_down.or(self.limit_down);
    }
}
```

Add `open_bars: BTreeMap<String, OpenBar>` to `BarGeneratorEngine`.

Implement window alignment:

```rust
fn minute_start_ns(dt: i64) -> i64 {
    dt.div_euclid(60_000_000_000) * 60_000_000_000
}
```

In `on_data()`, extract rows, update per-instrument open bar, and emit completed bars through `build_output_batch()`.

- [ ] **Step 4: Run delta test and schema test**

Run:

```bash
cargo test -p zippy-engines bar_generator_output_schema_is_stable
cargo test -p zippy-engines bar_generator_emits_completed_delta_bar_on_minute_transition
```

Expected: both PASS.

- [ ] **Step 5: Add and pass multi-instrument test**

Add:

```rust
#[test]
fn bar_generator_keeps_multi_instrument_state_independent() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "au2606", "rb2601", "au2606"],
            vec![30_000_000_000, 30_000_000_000, MINUTE_NS + 1, MINUTE_NS + 1],
            vec![10.0, 500.0, 11.0, 502.0],
            vec![1.0, 2.0, 3.0, 4.0],
            vec![10.0, 1000.0, 33.0, 2008.0],
            vec!["20260508", "20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_column(&outputs[0], "instrument_id"), vec!["au2606", "rb2601"]);
    assert_eq!(f64_column(&outputs[0], "close"), vec![500.0, 10.0]);
}
```

Run:

```bash
cargo test -p zippy-engines bar_generator_keeps_multi_instrument_state_independent
```

Expected: PASS after ordering completed bars deterministically by instrument using `BTreeMap`.

- [ ] **Step 6: Commit Task 2**

Run:

```bash
git add crates/zippy-engines/src/bar_generator.rs crates/zippy-engines/tests/bar_generator_engine.rs
git commit -m "feat: generate delta minute bars"
```

### Task 3: Sessions, Dt Label, Optional Columns, And Cumulative Volume

**Files:**
- Modify: `crates/zippy-engines/src/bar_generator.rs`
- Modify: `crates/zippy-engines/tests/bar_generator_engine.rs`

- [ ] **Step 1: Add failing session filter and dt-label tests**

Add tests:

```rust
#[test]
fn bar_generator_drops_non_session_ticks_without_updating_state() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![8 * 3600 * 1_000_000_000, 30_000_000_000],
            vec![99.0, 10.0],
            vec![100.0, 1.0],
            vec![9900.0, 10.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();
    assert!(outputs.is_empty());

    let flushed = engine.on_flush().unwrap();
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![1.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn bar_generator_supports_start_dt_label() {
    let mut spec = delta_spec();
    spec.dt_label = DtLabelPolicy::StartDt;
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();
    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![30_000_000_000, MINUTE_NS + 1],
            vec![10.0, 11.0],
            vec![1.0, 1.0],
            vec![10.0, 11.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();
    assert_eq!(ts_column(&outputs[0], "dt"), vec![0]);
}
```

- [ ] **Step 2: Implement session classification and dt label**

Add:

```rust
enum TickSessionKind {
    Regular,
    Auction,
    Outside,
}

fn classify_session(spec: &BarSessionSpec, dt_ns: i64) -> TickSessionKind {
    let seconds = local_seconds_of_day(dt_ns);
    if spec.regular.iter().any(|window| window.contains(seconds)) {
        TickSessionKind::Regular
    } else if spec.auction.iter().any(|window| window.contains(seconds)) {
        TickSessionKind::Auction
    } else {
        TickSessionKind::Outside
    }
}
```

For v1, `local_seconds_of_day()` can use epoch-day modulo seconds because incoming timestamp is already in the profile timezone contract used by zippy plugin schemas. Keep this explicit in a code comment:

```rust
fn local_seconds_of_day(dt_ns: i64) -> u32 {
    let seconds = dt_ns.div_euclid(1_000_000_000);
    seconds.rem_euclid(86_400) as u32
}
```

- [ ] **Step 3: Run session and dt-label tests**

Run:

```bash
cargo test -p zippy-engines bar_generator_drops_non_session_ticks_without_updating_state
cargo test -p zippy-engines bar_generator_supports_start_dt_label
```

Expected: PASS.

- [ ] **Step 4: Add failing cumulative tests**

Add:

```rust
fn cumulative_spec(bootstrap: BootstrapPolicy) -> BarGeneratorSpec {
    let mut spec = delta_spec();
    spec.volume = VolumeSpec::Cumulative {
        trading_day_column: "trading_day".to_string(),
        bootstrap,
    };
    spec
}

#[test]
fn cumulative_mode_skips_first_delta_on_intraday_bootstrap() {
    let mut engine =
        BarGeneratorEngine::new("bars", tick_schema(), cumulative_spec(BootstrapPolicy::SkipFirstDelta)).unwrap();
    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![30_000_000_000, 45_000_000_000, MINUTE_NS + 1],
            vec![10.0, 11.0, 12.0],
            vec![100.0, 105.0, 107.0],
            vec![1000.0, 1060.0, 1085.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(f64_column(&outputs[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&outputs[0], "total_turnover"), vec![60.0]);
}

#[test]
fn cumulative_mode_resets_at_trading_day_boundary() {
    let mut engine =
        BarGeneratorEngine::new("bars", tick_schema(), cumulative_spec(BootstrapPolicy::SkipFirstDelta)).unwrap();
    engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![30_000_000_000, 45_000_000_000],
            vec![10.0, 11.0],
            vec![100.0, 105.0],
            vec![1000.0, 1060.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![MINUTE_NS + 1, MINUTE_NS + 10],
            vec![12.0, 13.0],
            vec![3.0, 8.0],
            vec![30.0, 90.0],
            vec!["20260509", "20260509"],
        ))
        .unwrap();

    assert_eq!(f64_column(&outputs[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&outputs[0], "total_turnover"), vec![60.0]);
}
```

- [ ] **Step 5: Implement cumulative baseline state**

Add:

```rust
#[derive(Debug, Clone)]
struct CumulativeState {
    trading_day: String,
    last_volume: f64,
    last_total_turnover: f64,
    initialized: bool,
}
```

Add `cumulative_states: BTreeMap<String, CumulativeState>` to the engine. Implement:

```rust
fn compute_volume_delta(&mut self, row: &TickRow) -> Option<(f64, f64)> {
    match &self.spec.volume {
        VolumeSpec::Delta => Some((row.volume, row.total_turnover)),
        VolumeSpec::Cumulative { bootstrap, .. } => {
            let trading_day = row.trading_day.as_ref()?.clone();
            let state = self.cumulative_states.entry(row.instrument.clone()).or_insert(CumulativeState {
                trading_day: trading_day.clone(),
                last_volume: row.volume,
                last_total_turnover: row.total_turnover,
                initialized: false,
            });
            if state.trading_day != trading_day {
                state.trading_day = trading_day;
                state.last_volume = row.volume;
                state.last_total_turnover = row.total_turnover;
                state.initialized = false;
                return match bootstrap {
                    BootstrapPolicy::FromZero => Some((row.volume, row.total_turnover)),
                    BootstrapPolicy::SkipFirstDelta => None,
                };
            }
            let delta_volume = row.volume - state.last_volume;
            let delta_turnover = row.total_turnover - state.last_total_turnover;
            state.last_volume = row.volume;
            state.last_total_turnover = row.total_turnover;
            if !state.initialized {
                state.initialized = true;
                return match bootstrap {
                    BootstrapPolicy::FromZero => Some((row.volume, row.total_turnover)),
                    BootstrapPolicy::SkipFirstDelta => None,
                };
            }
            if delta_volume < 0.0 || delta_turnover < 0.0 {
                self.pending_filtered_rows += 1;
                return None;
            }
            Some((delta_volume, delta_turnover))
        }
    }
}
```

- [ ] **Step 6: Run cumulative tests**

Run:

```bash
cargo test -p zippy-engines cumulative_mode_
```

Expected: PASS.

- [ ] **Step 7: Add optional nullable column test**

Create schema without optional columns and assert `num_trades/limit_up/limit_down` are null in output. Use `Int64Array::is_null()` and `Float64Array::is_null()`.

Run:

```bash
cargo test -p zippy-engines bar_generator_outputs_null_optional_columns_when_inputs_missing
```

Expected: PASS after extraction helpers return `None` for absent optional fields.

- [ ] **Step 8: Commit Task 3**

Run:

```bash
git add crates/zippy-engines/src/bar_generator.rs crates/zippy-engines/tests/bar_generator_engine.rs
git commit -m "feat: handle bar sessions and cumulative volume"
```

### Task 4: Auction Policies

**Files:**
- Modify: `crates/zippy-engines/src/bar_generator.rs`
- Modify: `crates/zippy-engines/tests/bar_generator_engine.rs`

- [ ] **Step 1: Add failing auction tests**

Add tests for:

```rust
#[test]
fn auction_drop_updates_cumulative_baseline_without_output_bar() {
    let mut spec = cumulative_spec(BootstrapPolicy::SkipFirstDelta);
    spec.sessions.auction = vec![SessionWindow::parse("09:25:00", "09:30:00").unwrap()];
    spec.auction = AuctionPolicy::Drop;
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();

    let outputs = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![9 * 3600 * 1_000_000_000 + 25 * 60 * 1_000_000_000, 30_000_000_000, 45_000_000_000],
            vec![9.0, 10.0, 11.0],
            vec![100.0, 105.0, 108.0],
            vec![900.0, 960.0, 1000.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert!(outputs.is_empty());
    let flushed = engine.on_flush().unwrap();
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
}
```

Add `merge_to_first_regular_bar` test that verifies auction delta is included in first regular minute bar.

- [ ] **Step 2: Implement auction classification**

In `on_data()`:

```rust
match classify_session(&self.spec.sessions, row.dt) {
    TickSessionKind::Outside => {
        self.pending_filtered_rows += 1;
        continue;
    }
    TickSessionKind::Auction => {
        self.handle_auction_row(row)?;
        continue;
    }
    TickSessionKind::Regular => {
        self.handle_regular_row(row)?;
    }
}
```

Implement `handle_auction_row()` with three policies:

- `Drop`: compute cumulative delta to update baseline; do not update an open bar.
- `MergeToFirstRegularBar`: accumulate `PendingAuctionBar` per instrument and merge into the next regular bar.
- `EmitSeparateBar`: update an auction bar keyed by instrument and auction close timestamp.

- [ ] **Step 3: Run auction tests**

Run:

```bash
cargo test -p zippy-engines auction_
```

Expected: PASS.

- [ ] **Step 4: Commit Task 4**

Run:

```bash
git add crates/zippy-engines/src/bar_generator.rs crates/zippy-engines/tests/bar_generator_engine.rs
git commit -m "feat: support bar auction policies"
```

### Task 5: Python Native Wrapper

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`

- [ ] **Step 1: Add native wrapper stubs**

In `crates/zippy-python/src/lib.rs`, import Rust types:

```rust
use zippy_engines::{
    AuctionPolicy as RustAuctionPolicy, BarGeneratorEngine as RustBarGeneratorEngine,
    BarGeneratorSpec as RustBarGeneratorSpec, BarInputColumns as RustBarInputColumns,
    BarSessionSpec as RustBarSessionSpec, BootstrapPolicy as RustBootstrapPolicy,
    DtLabelPolicy as RustDtLabelPolicy, SessionWindow as RustSessionWindow,
    VolumeSpec as RustVolumeSpec,
};
```

Add pyclass struct near other engine wrappers:

```rust
#[pyclass]
struct BarGeneratorEngine {
    name: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    profile_spec: serde_json::Value,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustBarGeneratorEngine>,
    remote_source: Option<RemoteSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}
```

- [ ] **Step 2: Add profile parsing helpers**

Implement:

```rust
fn normalize_bar_profile(py: Python<'_>, profile: &Bound<'_, PyAny>) -> PyResult<(RustBarGeneratorSpec, serde_json::Value)> {
    let spec_obj = if profile.hasattr("to_bar_generator_spec")? {
        profile.call_method0("to_bar_generator_spec")?
    } else {
        profile.clone()
    };
    let profile_json = py_to_json_value(py, &spec_obj)?;
    let spec = parse_bar_generator_spec(&profile_json)?;
    Ok((spec, profile_json))
}
```

Use existing dict/list/string extraction style in `lib.rs`; do not add new dependencies.

- [ ] **Step 3: Implement PyO3 constructor and runtime methods**

Mirror `TimeSeriesEngine` methods, changing engine type and config name:

```rust
#[pymethods]
impl BarGeneratorEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, profile, target, *, source=None, master=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        profile: &Bound<'_, PyAny>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
    ) -> PyResult<Self> {
        let schema = Arc::new(Schema::from_pyarrow_bound(input_schema).map_err(|error| py_value_error(error.to_string()))?);
        let (spec, profile_spec) = normalize_bar_profile(py, profile)?;
        let engine = RustBarGeneratorEngine::new(&name, Arc::clone(&schema), spec).map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options = parse_runtime_options(buffer_capacity, overflow_policy, archive_buffer_capacity, xfast)?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, segment_source, python_source) = register_source(
            py,
            source,
            master,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink.as_ref().map(|config| config.write_input).unwrap_or(false),
            },
            schema.as_ref(),
            xfast,
        )?;

        Ok(Self {
            name,
            input_schema: schema,
            output_schema,
            profile_spec,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }
}
```

Implement `start/write/output_schema/status/metrics/config/flush/stop` by copying the `TimeSeriesEngine` pattern and replacing `"timeseries"` with `"bar_generator"`.

- [ ] **Step 4: Register wrapper in source linking and module export**

Add `BarGeneratorEngine` to:

- `register_source()`
- `ensure_source_stopped()`
- downstream config helpers such as `engine_has_source()` if needed
- `#[pymodule]` `module.add_class::<BarGeneratorEngine>()?`
- Python stub `python/zippy/_internal.pyi`

- [ ] **Step 5: Run Rust Python crate check**

Run:

```bash
cargo test -p zippy-python
```

Expected: PASS, or existing environment-only failures documented with exact failing test names.

- [ ] **Step 6: Commit Task 5**

Run:

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi
git commit -m "feat: expose bar generator native engine"
```

### Task 6: Python `zp.bar` Facade And API Tests

**Files:**
- Modify: `python/zippy/__init__.py`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Add failing Python facade tests**

Append to `pytests/test_python_api.py`:

```python
def test_bar_profile_normalizes_to_native_spec() -> None:
    profile = zippy.bar.BarGeneratorProfile(
        frequency="1m",
        columns=zippy.bar.InputColumns(
            instrument="instrument_id",
            dt="dt",
            price="last_price",
            volume="volume",
            total_turnover="turnover",
            trading_day="trading_day",
            num_trades=None,
            limit_up="upper_limit_price",
            limit_down="lower_limit_price",
        ),
        sessions=zippy.bar.TradingSessions(
            timezone="Asia/Shanghai",
            regular=[("09:30:00", "15:00:00")],
            auction=[],
        ),
        volume=zippy.bar.Volume.cumulative(
            trading_day_column="trading_day",
            bootstrap="skip_first_delta",
        ),
        auction=zippy.bar.Auction.drop(),
        dt_label="close_dt",
    )

    assert profile.to_bar_generator_spec()["volume"] == {
        "mode": "cumulative",
        "trading_day_column": "trading_day",
        "bootstrap": "skip_first_delta",
    }


def test_bar_engine_accepts_plugin_profile_protocol(monkeypatch) -> None:
    class PluginProfile:
        def to_bar_generator_spec(self) -> dict[str, object]:
            return {
                "frequency": "1m",
                "columns": {
                    "instrument": "instrument_id",
                    "dt": "dt",
                    "price": "last_price",
                    "volume": "volume",
                    "total_turnover": "turnover",
                    "trading_day": "trading_day",
                    "num_trades": None,
                    "limit_up": None,
                    "limit_down": None,
                },
                "sessions": {
                    "timezone": "Asia/Shanghai",
                    "regular": [("09:30:00", "15:00:00")],
                    "auction": [],
                },
                "volume": {
                    "mode": "delta",
                },
                "auction": "drop",
                "dt_label": "close_dt",
            }

    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="Asia/Shanghai")),
            ("last_price", pa.float64()),
            ("volume", pa.float64()),
            ("turnover", pa.float64()),
            ("trading_day", pa.string()),
        ]
    )
    engine = zippy.BarGeneratorEngine(
        name="bars",
        input_schema=schema,
        profile=PluginProfile(),
        target=zippy.NullPublisher(),
    )

    assert engine.config()["engine_type"] == "bar_generator"
```

- [ ] **Step 2: Run Python tests and verify failure**

Run:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run pytest pytests/test_python_api.py -k "bar_profile or bar_engine_accepts_plugin_profile" -q
```

Expected: FAIL because `zippy.bar` and/or native `BarGeneratorEngine` do not exist.

- [ ] **Step 3: Implement `zp.bar` namespace**

In `python/zippy/__init__.py`, import top-level native engine:

```python
from ._internal import BarGeneratorEngine
```

Add fallback:

```python
BarGeneratorEngine = _native_unavailable("BarGeneratorEngine")
```

Add classes near other public helper namespaces:

```python
@dataclass(frozen=True)
class _BarInputColumns:
    instrument: str
    dt: str
    price: str
    volume: str
    total_turnover: str
    trading_day: str | None = None
    num_trades: str | None = None
    limit_up: str | None = None
    limit_down: str | None = None

    def to_bar_generator_spec(self) -> dict[str, object]:
        return self.__dict__.copy()
```

Add `_BarTradingSessions`, `_BarVolume`, `_BarAuction`, `_BarGeneratorProfile`, and namespace:

```python
class _BarNamespace:
    InputColumns = _BarInputColumns
    TradingSessions = _BarTradingSessions
    Volume = _BarVolume
    Auction = _BarAuction
    BarGeneratorProfile = _BarGeneratorProfile


bar = _BarNamespace()
```

Add `"BarGeneratorEngine"` and `"bar"` to `__all__`.

- [ ] **Step 4: Rebuild native extension**

Run:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run maturin develop -m crates/zippy-python/Cargo.toml --uv
```

Expected: native extension builds and installs into the local uv environment.

- [ ] **Step 5: Run Python facade tests**

Run:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run pytest pytests/test_python_api.py -k "bar_profile or bar_engine_accepts_plugin_profile" -q
```

Expected: PASS.

- [ ] **Step 6: Commit Task 6**

Run:

```bash
git add python/zippy/__init__.py pytests/test_python_api.py
git commit -m "feat: add bar profile python facade"
```

### Task 7: End-To-End Verification And Cleanup

**Files:**
- Modify only files needed to fix verification failures found in this task.

- [ ] **Step 1: Run Rust engine suite**

Run:

```bash
cargo test -p zippy-engines bar_generator
```

Expected: PASS.

- [ ] **Step 2: Run Python bridge suite**

Run:

```bash
cargo test -p zippy-python
```

Expected: PASS, unless a pre-existing environment-only test is already known to require unrestricted socket/shared-memory access. If failure occurs, record exact failing command, test name, and error text before deciding whether to request escalation or narrow verification.

- [ ] **Step 3: Rebuild extension and run targeted pytest**

Run:

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run maturin develop -m crates/zippy-python/Cargo.toml --uv
UV_CACHE_DIR=/tmp/uv-cache uv run pytest pytests/test_python_api.py -k "bar_generator or bar_profile or bar_engine_accepts_plugin_profile" -q
```

Expected: PASS.

- [ ] **Step 4: Run formatting/lint checks relevant to touched code**

Run:

```bash
cargo fmt --check
cargo clippy -p zippy-engines -p zippy-python --tests
UV_CACHE_DIR=/tmp/uv-cache uv run black --check python/zippy pytests/test_python_api.py
```

Expected: PASS. If black is not installed in the uv environment, run `UV_CACHE_DIR=/tmp/uv-cache uv run --with black black --check python/zippy pytests/test_python_api.py`.

- [ ] **Step 5: Inspect final diff**

Run:

```bash
git status --short
git diff --stat main...HEAD
git diff --check
```

Expected: only BarGeneratorEngine-related files are changed; `git diff --check` reports no whitespace errors.

- [ ] **Step 6: Commit verification fixes if any**

If Step 1-5 required additional fixes, commit them:

```bash
git add crates/zippy-engines crates/zippy-python python/zippy pytests/test_python_api.py docs/superpowers
git commit -m "fix: complete bar generator verification"
```

If no additional fixes were needed, do not create an empty commit.

## Self-Review Checklist

- Spec coverage:
  - Dedicated engine: Tasks 1-4.
  - 1m multi-instrument bars: Task 2.
  - Standard output columns plus instrument: Tasks 1-3.
  - Non-session tick filtering: Task 3.
  - CTP cumulative mode and cross-trading-day reset: Task 3.
  - Auction policies: Task 4.
  - `dt_label`: Task 3.
  - Plugin profile protocol and `zp.bar` namespace: Tasks 5-6.
  - Session materialization path: Task 5 source/downstream integration and Task 6 Python tests.
- Placeholder scan:
  - The plan has no unresolved file paths or deferred implementation markers.
- Type consistency:
  - Public Rust names use `BarGeneratorEngine`, `BarGeneratorSpec`, `BarInputColumns`, `BarSessionSpec`, `SessionWindow`, `VolumeSpec`, `BootstrapPolicy`, `AuctionPolicy`, and `DtLabelPolicy`.
  - Public Python names use `zp.BarGeneratorEngine` and `zp.bar.*`.
