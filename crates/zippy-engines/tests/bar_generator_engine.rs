use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SchemaRef, SegmentTableView, ZippyError};
use zippy_engines::{
    AuctionPolicy, BarGeneratorEngine, BarGeneratorSpec, BarInputColumns, BarSessionSpec,
    DtLabelPolicy, SessionWindow, VolumeSpec,
};

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

fn assert_field(schema: &Schema, name: &str, data_type: &DataType, nullable: bool) {
    let field = schema.field_with_name(name).unwrap();

    assert_eq!(field.data_type(), data_type);
    assert_eq!(field.is_nullable(), nullable);
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
