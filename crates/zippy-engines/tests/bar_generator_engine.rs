use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SchemaRef, SegmentTableView, ZippyError};
use zippy_engines::{
    AuctionPolicy, BarGeneratorEngine, BarGeneratorSpec, BarInputColumns, BarSessionSpec,
    BootstrapPolicy, DtLabelPolicy, SessionWindow, VolumeSpec,
};

const MINUTE_NS: i64 = 60_000_000_000;

fn tick_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
            false,
        ),
        Field::new("last_price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("trading_day", DataType::Utf8, false),
        Field::new("num_trades", DataType::Int64, true),
        Field::new("upper_limit_price", DataType::Float64, true),
        Field::new("lower_limit_price", DataType::Float64, true),
    ]))
}

fn nullable_instrument_schema() -> SchemaRef {
    let mut fields = tick_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[0] = Field::new("instrument_id", DataType::Utf8, true);
    Arc::new(Schema::new(fields))
}

fn tick_schema_without_optional_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
            false,
        ),
        Field::new("last_price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("trading_day", DataType::Utf8, false),
    ]))
}

fn delta_spec() -> BarGeneratorSpec {
    BarGeneratorSpec {
        columns: BarInputColumns {
            instrument: "instrument_id".to_string(),
            dt: "dt".to_string(),
            price: "last_price".to_string(),
            volume: "volume".to_string(),
            total_turnover: "turnover".to_string(),
            trading_day: Some("trading_day".to_string()),
            num_trades: Some("num_trades".to_string()),
            limit_up: Some("upper_limit_price".to_string()),
            limit_down: Some("lower_limit_price".to_string()),
        },
        sessions: BarSessionSpec {
            timezone: "Asia/Shanghai".to_string(),
            regular: vec![SessionWindow::parse("00:00:00", "01:00:00").unwrap()],
            auction: vec![],
        },
        frequency: "1m".to_string(),
        volume: VolumeSpec::Delta,
        auction: AuctionPolicy::Drop,
        dt_label: DtLabelPolicy::CloseDt,
    }
}

fn cumulative_spec(bootstrap: BootstrapPolicy) -> BarGeneratorSpec {
    let mut spec = delta_spec();
    spec.volume = VolumeSpec::Cumulative {
        trading_day_column: "trading_day".to_string(),
        bootstrap,
    };
    spec
}

fn auction_spec(policy: AuctionPolicy, volume: VolumeSpec) -> BarGeneratorSpec {
    let mut spec = delta_spec();
    spec.sessions.regular = vec![SessionWindow::parse("00:00:20", "01:00:00").unwrap()];
    spec.sessions.auction = vec![SessionWindow::parse("00:00:10", "00:00:20").unwrap()];
    spec.auction = policy;
    spec.volume = volume;

    spec
}

fn cumulative_skip_first_delta() -> VolumeSpec {
    VolumeSpec::Cumulative {
        trading_day_column: "trading_day".to_string(),
        bootstrap: BootstrapPolicy::SkipFirstDelta,
    }
}

fn mismatched_batch() -> SegmentTableView {
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]));
    let columns = vec![
        Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone("UTC"))
            as ArrayRef,
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    SegmentTableView::from_record_batch(batch)
}

fn tick_batch(
    instruments: Vec<&str>,
    dts: Vec<i64>,
    prices: Vec<f64>,
    volumes: Vec<f64>,
    turnovers: Vec<f64>,
    trading_days: Vec<&str>,
) -> SegmentTableView {
    let row_count = instruments.len();
    assert_eq!(dts.len(), row_count);
    assert_eq!(prices.len(), row_count);
    assert_eq!(volumes.len(), row_count);
    assert_eq!(turnovers.len(), row_count);
    assert_eq!(trading_days.len(), row_count);

    let columns = vec![
        Arc::new(StringArray::from(instruments)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(dts).with_timezone("Asia/Shanghai")) as ArrayRef,
        Arc::new(Float64Array::from(prices)) as ArrayRef,
        Arc::new(Float64Array::from(volumes)) as ArrayRef,
        Arc::new(Float64Array::from(turnovers)) as ArrayRef,
        Arc::new(StringArray::from(trading_days)) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(1); row_count])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(999.0); row_count])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(1.0); row_count])) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(tick_schema(), columns).unwrap();

    SegmentTableView::from_record_batch(batch)
}

fn tick_batch_with_nullable_trading_day(
    instruments: Vec<&str>,
    dts: Vec<i64>,
    prices: Vec<f64>,
    volumes: Vec<f64>,
    turnovers: Vec<f64>,
    trading_days: Vec<Option<&str>>,
) -> SegmentTableView {
    let row_count = instruments.len();
    assert_eq!(dts.len(), row_count);
    assert_eq!(prices.len(), row_count);
    assert_eq!(volumes.len(), row_count);
    assert_eq!(turnovers.len(), row_count);
    assert_eq!(trading_days.len(), row_count);

    let mut fields = tick_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[5] = Field::new("trading_day", DataType::Utf8, true);
    let schema = Arc::new(Schema::new(fields));
    let columns = vec![
        Arc::new(StringArray::from(instruments)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(dts).with_timezone("Asia/Shanghai")) as ArrayRef,
        Arc::new(Float64Array::from(prices)) as ArrayRef,
        Arc::new(Float64Array::from(volumes)) as ArrayRef,
        Arc::new(Float64Array::from(turnovers)) as ArrayRef,
        Arc::new(StringArray::from(trading_days)) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(1); row_count])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(999.0); row_count])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(1.0); row_count])) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();

    SegmentTableView::from_record_batch(batch)
}

fn tick_batch_without_optional_columns(
    instruments: Vec<&str>,
    dts: Vec<i64>,
    prices: Vec<f64>,
    volumes: Vec<f64>,
    turnovers: Vec<f64>,
    trading_days: Vec<&str>,
) -> SegmentTableView {
    let row_count = instruments.len();
    assert_eq!(dts.len(), row_count);
    assert_eq!(prices.len(), row_count);
    assert_eq!(volumes.len(), row_count);
    assert_eq!(turnovers.len(), row_count);
    assert_eq!(trading_days.len(), row_count);

    let columns = vec![
        Arc::new(StringArray::from(instruments)) as ArrayRef,
        Arc::new(TimestampNanosecondArray::from(dts).with_timezone("Asia/Shanghai")) as ArrayRef,
        Arc::new(Float64Array::from(prices)) as ArrayRef,
        Arc::new(Float64Array::from(volumes)) as ArrayRef,
        Arc::new(Float64Array::from(turnovers)) as ArrayRef,
        Arc::new(StringArray::from(trading_days)) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(tick_schema_without_optional_columns(), columns).unwrap();

    SegmentTableView::from_record_batch(batch)
}

fn f64_column(table: &SegmentTableView, name: &str) -> Vec<f64> {
    let column = table.column(name).unwrap();
    let values = column.as_any().downcast_ref::<Float64Array>().unwrap();

    (0..values.len()).map(|row| values.value(row)).collect()
}

fn string_column(table: &SegmentTableView, name: &str) -> Vec<String> {
    let column = table.column(name).unwrap();
    let values = column.as_any().downcast_ref::<StringArray>().unwrap();

    (0..values.len())
        .map(|row| values.value(row).to_string())
        .collect()
}

fn ts_column(table: &SegmentTableView, name: &str) -> Vec<i64> {
    let column = table.column(name).unwrap();
    let values = column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();

    (0..values.len()).map(|row| values.value(row)).collect()
}

fn assert_field(schema: &Schema, name: &str, data_type: &DataType, nullable: bool) {
    let field = schema.field_with_name(name).unwrap();

    assert_eq!(field.data_type(), data_type);
    assert_eq!(field.is_nullable(), nullable);
}

fn assert_column_null_at(table: &SegmentTableView, name: &str, row_index: usize) {
    let column = table.column(name).unwrap();

    assert!(column.is_null(row_index));
}

#[test]
fn bar_generator_emits_completed_delta_bar_on_minute_transition() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![30_000_000_000, 45_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 12.0, 11.0],
            vec![2.0, 3.0, 5.0],
            vec![20.0, 36.0, 55.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(string_column(&output[0], "instrument_id"), vec!["rb2601"]);
    assert_eq!(ts_column(&output[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&output[0], "close_dt"), vec![MINUTE_NS]);
    assert_eq!(ts_column(&output[0], "dt"), vec![MINUTE_NS]);
    assert_eq!(f64_column(&output[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&output[0], "high"), vec![12.0]);
    assert_eq!(f64_column(&output[0], "low"), vec![10.0]);
    assert_eq!(f64_column(&output[0], "close"), vec![12.0]);
    assert_eq!(f64_column(&output[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![56.0]);
}

#[test]
fn bar_generator_keeps_multi_instrument_state_independent() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "au2606", "rb2601", "au2606"],
            vec![30_000_000_000, 30_000_000_000, MINUTE_NS + 1, MINUTE_NS + 1],
            vec![10.0, 500.0, 11.0, 502.0],
            vec![1.0, 2.0, 3.0, 4.0],
            vec![10.0, 1000.0, 33.0, 2008.0],
            vec!["20260508", "20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
    assert_eq!(
        string_column(&output[0], "instrument_id"),
        vec!["au2606", "rb2601"]
    );
    assert_eq!(f64_column(&output[0], "close"), vec![500.0, 10.0]);
}

#[test]
fn bar_generator_drops_non_session_ticks_without_updating_state() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![8 * 60 * MINUTE_NS, 30_000_000_000],
            vec![99.0, 10.0],
            vec![99.0, 1.0],
            vec![9_999.0, 10.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![1.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn cumulative_mode_skips_first_delta_on_intraday_bootstrap() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        cumulative_spec(BootstrapPolicy::SkipFirstDelta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![30_000_000_000, 45_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 11.0, 12.0],
            vec![100.0, 105.0, 107.0],
            vec![1_000.0, 1_060.0, 1_085.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(f64_column(&output[0], "open"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "close"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![60.0]);
}

#[test]
fn cumulative_mode_from_zero_uses_first_tick_as_delta() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        cumulative_spec(BootstrapPolicy::FromZero),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![30_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 11.0],
            vec![3.0, 8.0],
            vec![30.0, 90.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(f64_column(&output[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&output[0], "close"), vec![10.0]);
    assert_eq!(f64_column(&output[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![30.0]);
}

#[test]
fn cumulative_mode_filters_null_trading_day_without_baseline_update() {
    let input = tick_batch_with_nullable_trading_day(
        vec!["rb2601", "rb2601", "rb2601"],
        vec![30_000_000_000, 45_000_000_000, MINUTE_NS + 1_000_000_000],
        vec![99.0, 10.0, 11.0],
        vec![100.0, 105.0, 108.0],
        vec![1_000.0, 1_060.0, 1_095.0],
        vec![None, Some("20260508"), Some("20260508")],
    );
    let mut engine = BarGeneratorEngine::new(
        "bars",
        input.schema(),
        cumulative_spec(BootstrapPolicy::SkipFirstDelta),
    )
    .unwrap();
    let output = engine.on_data(input).unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(f64_column(&output[0], "open"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "close"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![35.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn cumulative_mode_filters_negative_delta_and_rebases_baseline() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        cumulative_spec(BootstrapPolicy::SkipFirstDelta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601", "rb2601"],
            vec![
                30_000_000_000,
                40_000_000_000,
                50_000_000_000,
                MINUTE_NS + 1_000_000_000,
            ],
            vec![10.0, 11.0, 12.0, 13.0],
            vec![100.0, 105.0, 103.0, 107.0],
            vec![1_000.0, 1_060.0, 1_040.0, 1_090.0],
            vec!["20260508", "20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(f64_column(&output[0], "open"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "close"), vec![11.0]);
    assert_eq!(f64_column(&output[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![60.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed[0], "open"), vec![13.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![13.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![4.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![50.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn cumulative_mode_resets_at_trading_day_boundary() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        cumulative_spec(BootstrapPolicy::SkipFirstDelta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601", "rb2601"],
            vec![
                30_000_000_000,
                45_000_000_000,
                MINUTE_NS + 1_000_000_000,
                MINUTE_NS + 10_000_000_000,
            ],
            vec![10.0, 11.0, 12.0, 13.0],
            vec![100.0, 105.0, 3.0, 8.0],
            vec![1_000.0, 1_060.0, 30.0, 90.0],
            vec!["20260508", "20260508", "20260509", "20260509"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(f64_column(&output[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&output[0], "total_turnover"), vec![60.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed[0], "open"), vec![13.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![5.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![60.0]);
}

#[test]
fn bar_generator_session_window_is_start_inclusive_end_exclusive() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![0, 60 * MINUTE_NS],
            vec![10.0, 99.0],
            vec![1.0, 100.0],
            vec![10.0, 9_900.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![1.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn bar_generator_auction_drop_updates_cumulative_baseline_without_output_bar() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(AuctionPolicy::Drop, cumulative_skip_first_delta()),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![15_000_000_000, 30_000_000_000, 45_000_000_000],
            vec![99.0, 10.0, 11.0],
            vec![100.0, 100.0, 103.0],
            vec![1_000.0, 1_000.0, 1_033.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "high"), vec![11.0]);
    assert_eq!(f64_column(&flushed[0], "low"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![11.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![33.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn bar_generator_auction_merge_to_first_regular_bar_includes_auction_tick() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(AuctionPolicy::MergeToFirstRegularBar, VolumeSpec::Delta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601", "rb2601"],
            vec![15_000_000_000, 30_000_000_000, 45_000_000_000],
            vec![9.0, 10.0, 8.0],
            vec![2.0, 3.0, 5.0],
            vec![18.0, 30.0, 40.0],
            vec!["20260508", "20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&flushed[0], "close_dt"), vec![MINUTE_NS]);
    assert_eq!(f64_column(&flushed[0], "open"), vec![9.0]);
    assert_eq!(f64_column(&flushed[0], "high"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "low"), vec![8.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![8.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![88.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_merge_clears_unmatched_pending_on_flush() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(AuctionPolicy::MergeToFirstRegularBar, VolumeSpec::Delta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601"],
            vec![15_000_000_000],
            vec![9.0],
            vec![2.0],
            vec![18.0],
            vec!["20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed_without_regular = engine.on_flush().unwrap();

    assert!(flushed_without_regular.is_empty());

    let regular_output = engine
        .on_data(tick_batch(
            vec!["rb2601"],
            vec![30_000_000_000],
            vec![10.0],
            vec![3.0],
            vec![30.0],
            vec!["20260508"],
        ))
        .unwrap();

    assert!(regular_output.is_empty());

    let flushed_regular = engine.on_flush().unwrap();

    assert_eq!(flushed_regular.len(), 1);
    assert_eq!(flushed_regular[0].num_rows(), 1);
    assert_eq!(f64_column(&flushed_regular[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed_regular[0], "high"), vec![10.0]);
    assert_eq!(f64_column(&flushed_regular[0], "low"), vec![10.0]);
    assert_eq!(f64_column(&flushed_regular[0], "close"), vec![10.0]);
    assert_eq!(f64_column(&flushed_regular[0], "volume"), vec![3.0]);
    assert_eq!(
        f64_column(&flushed_regular[0], "total_turnover"),
        vec![30.0]
    );
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_merge_cumulative_empty_baseline_uses_regular_window() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(
            AuctionPolicy::MergeToFirstRegularBar,
            cumulative_skip_first_delta(),
        ),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![15_000_000_000, 30_000_000_000],
            vec![99.0, 10.0],
            vec![100.0, 103.0],
            vec![1_000.0, 1_033.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&flushed[0], "close_dt"), vec![MINUTE_NS]);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![33.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_emit_cumulative_empty_baseline_uses_regular_window() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(
            AuctionPolicy::EmitSeparateBar,
            cumulative_skip_first_delta(),
        ),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![15_000_000_000, 30_000_000_000],
            vec![99.0, 10.0],
            vec![100.0, 103.0],
            vec![1_000.0, 1_033.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&flushed[0], "close_dt"), vec![MINUTE_NS]);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![33.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_merge_cumulative_cross_minute_uses_regular_window() {
    let mut spec = auction_spec(
        AuctionPolicy::MergeToFirstRegularBar,
        cumulative_skip_first_delta(),
    );
    spec.sessions.regular = vec![SessionWindow::parse("00:01:00", "01:00:00").unwrap()];
    spec.sessions.auction = vec![SessionWindow::parse("00:00:50", "00:01:00").unwrap()];
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![55_000_000_000, MINUTE_NS + 30_000_000_000],
            vec![99.0, 10.0],
            vec![100.0, 103.0],
            vec![1_000.0, 1_033.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![MINUTE_NS]);
    assert_eq!(ts_column(&flushed[0], "close_dt"), vec![2 * MINUTE_NS]);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![33.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_emit_cumulative_cross_minute_uses_regular_window() {
    let mut spec = auction_spec(
        AuctionPolicy::EmitSeparateBar,
        cumulative_skip_first_delta(),
    );
    spec.sessions.regular = vec![SessionWindow::parse("00:01:00", "01:00:00").unwrap()];
    spec.sessions.auction = vec![SessionWindow::parse("00:00:50", "00:01:00").unwrap()];
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![55_000_000_000, MINUTE_NS + 30_000_000_000],
            vec![99.0, 10.0],
            vec![100.0, 103.0],
            vec![1_000.0, 1_033.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![MINUTE_NS]);
    assert_eq!(ts_column(&flushed[0], "close_dt"), vec![2 * MINUTE_NS]);
    assert_eq!(f64_column(&flushed[0], "open"), vec![10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![33.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_auction_emit_separate_bar_outputs_auction_window_bar() {
    let mut engine = BarGeneratorEngine::new(
        "bars",
        tick_schema(),
        auction_spec(AuctionPolicy::EmitSeparateBar, VolumeSpec::Delta),
    )
    .unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![15_000_000_000, 30_000_000_000],
            vec![9.0, 10.0],
            vec![2.0, 3.0],
            vec![18.0, 30.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 2);
    assert_eq!(
        string_column(&flushed[0], "instrument_id"),
        vec!["rb2601", "rb2601"]
    );
    assert_eq!(ts_column(&flushed[0], "start_dt"), vec![10_000_000_000, 0]);
    assert_eq!(
        ts_column(&flushed[0], "close_dt"),
        vec![20_000_000_000, MINUTE_NS]
    );
    assert_eq!(f64_column(&flushed[0], "open"), vec![9.0, 10.0]);
    assert_eq!(f64_column(&flushed[0], "close"), vec![9.0, 10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![2.0, 3.0]);
    assert_eq!(f64_column(&flushed[0], "total_turnover"), vec![18.0, 30.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 0);
}

#[test]
fn bar_generator_outputs_null_optional_columns_when_inputs_missing() {
    let mut spec = delta_spec();
    spec.columns.num_trades = None;
    spec.columns.limit_up = None;
    spec.columns.limit_down = None;
    let mut engine =
        BarGeneratorEngine::new("bars", tick_schema_without_optional_columns(), spec).unwrap();
    let output = engine
        .on_data(tick_batch_without_optional_columns(
            vec!["rb2601", "rb2601"],
            vec![30_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 11.0],
            vec![2.0, 3.0],
            vec![20.0, 33.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_column_null_at(&output[0], "num_trades", 0);
    assert_column_null_at(&output[0], "limit_up", 0);
    assert_column_null_at(&output[0], "limit_down", 0);
}

#[test]
fn bar_generator_flushes_and_clears_open_delta_bars() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "au2606"],
            vec![30_000_000_000, 45_000_000_000],
            vec![10.0, 500.0],
            vec![2.0, 3.0],
            vec![20.0, 1_500.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();
    assert!(output.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 2);
    assert_eq!(
        string_column(&flushed[0], "instrument_id"),
        vec!["au2606", "rb2601"]
    );
    assert_eq!(f64_column(&flushed[0], "close"), vec![500.0, 10.0]);
    assert_eq!(f64_column(&flushed[0], "volume"), vec![3.0, 2.0]);

    let second_flush = engine.on_flush().unwrap();

    assert!(second_flush.is_empty());
}

#[test]
fn bar_generator_uses_start_dt_label_when_configured() {
    let mut spec = delta_spec();
    spec.dt_label = DtLabelPolicy::StartDt;
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();

    let output = engine
        .on_data(tick_batch(
            vec!["rb2601", "rb2601"],
            vec![30_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 11.0],
            vec![2.0, 3.0],
            vec![20.0, 33.0],
            vec!["20260508", "20260508"],
        ))
        .unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    assert_eq!(ts_column(&output[0], "dt"), vec![0]);
    assert_eq!(ts_column(&output[0], "start_dt"), vec![0]);
    assert_eq!(ts_column(&output[0], "close_dt"), vec![MINUTE_NS]);
}

#[test]
fn bar_generator_output_schema_is_stable() {
    let engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let output_schema = engine.output_schema();
    let field_names = output_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        field_names,
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

    let timestamp_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into()));
    assert_field(
        output_schema.as_ref(),
        "instrument_id",
        &DataType::Utf8,
        false,
    );
    assert_field(output_schema.as_ref(), "dt", &timestamp_type, false);
    assert_field(output_schema.as_ref(), "open", &DataType::Float64, false);
    assert_field(output_schema.as_ref(), "high", &DataType::Float64, false);
    assert_field(output_schema.as_ref(), "low", &DataType::Float64, false);
    assert_field(output_schema.as_ref(), "close", &DataType::Float64, false);
    assert_field(output_schema.as_ref(), "volume", &DataType::Float64, false);
    assert_field(
        output_schema.as_ref(),
        "total_turnover",
        &DataType::Float64,
        false,
    );
    assert_field(output_schema.as_ref(), "num_trades", &DataType::Int64, true);
    assert_field(output_schema.as_ref(), "limit_up", &DataType::Float64, true);
    assert_field(
        output_schema.as_ref(),
        "limit_down",
        &DataType::Float64,
        true,
    );
    assert_field(output_schema.as_ref(), "start_dt", &timestamp_type, false);
    assert_field(output_schema.as_ref(), "close_dt", &timestamp_type, false);
}

#[test]
fn bar_generator_rejects_invalid_session_windows() {
    let cases = vec![
        (
            vec![SessionWindow {
                start_seconds: 34_200,
                end_seconds: 34_200,
            }],
            vec![],
        ),
        (
            vec![SessionWindow {
                start_seconds: 34_200,
                end_seconds: 86_401,
            }],
            vec![],
        ),
        (
            vec![SessionWindow::parse("09:30:00", "15:00:00").unwrap()],
            vec![SessionWindow {
                start_seconds: 86_400,
                end_seconds: 86_401,
            }],
        ),
    ];

    for (regular, auction) in cases {
        let mut spec = delta_spec();
        spec.sessions.regular = regular;
        spec.sessions.auction = auction;

        let result = BarGeneratorEngine::new("bars", tick_schema(), spec);

        assert!(matches!(result, Err(ZippyError::InvalidConfig { .. })));
    }
}

#[test]
fn bar_generator_rejects_nullable_instrument() {
    let result = BarGeneratorEngine::new("bars", nullable_instrument_schema(), delta_spec());

    assert!(matches!(result, Err(ZippyError::SchemaMismatch { .. })));
}

#[test]
fn bar_generator_rejects_on_data_schema_mismatch() {
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), delta_spec()).unwrap();
    let error = engine.on_data(mismatched_batch()).unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
    assert!(error.to_string().contains("engine=[bars]"));
}
