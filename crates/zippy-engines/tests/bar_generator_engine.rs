use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use zippy_core::{Engine, SchemaRef};
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

fn delta_spec() -> BarGeneratorSpec {
    BarGeneratorSpec {
        input: BarInputColumns {
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
        session: BarSessionSpec {
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

    assert_eq!(
        output_schema
            .field_with_name("instrument_id")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        output_schema.field_with_name("dt").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into()))
    );
    assert!(output_schema
        .field_with_name("num_trades")
        .unwrap()
        .is_nullable());
    assert!(output_schema
        .field_with_name("limit_up")
        .unwrap()
        .is_nullable());
    assert!(output_schema
        .field_with_name("limit_down")
        .unwrap()
        .is_nullable());
}
