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
            regular: vec![SessionWindow::parse("09:30:00", "15:00:00").unwrap()],
            auction: vec![],
        },
        frequency: "1m".to_string(),
        volume: VolumeSpec::Delta,
        auction: AuctionPolicy::Drop,
        dt_label: DtLabelPolicy::CloseDt,
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
fn bar_generator_rejects_cumulative_volume_until_baseline_is_implemented() {
    let mut spec = delta_spec();
    spec.volume = VolumeSpec::Cumulative {
        trading_day_column: "trading_day".to_string(),
        bootstrap: BootstrapPolicy::SkipFirstDelta,
    };
    let mut engine = BarGeneratorEngine::new("bars", tick_schema(), spec).unwrap();

    let error = engine
        .on_data(tick_batch(
            vec!["rb2601"],
            vec![30_000_000_000],
            vec![10.0],
            vec![100.0],
            vec![1_000.0],
            vec!["20260508"],
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::InvalidConfig { .. } | ZippyError::InvalidState { .. }
    ));
    assert!(error.to_string().contains("cumulative volume mode"));
    assert!(error.to_string().contains("bar generator runtime"));
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
