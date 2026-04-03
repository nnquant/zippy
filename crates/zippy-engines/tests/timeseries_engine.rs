use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{spawn_engine, Engine, EngineConfig, LateDataPolicy, ZippyError};
use zippy_engines::TimeSeriesEngine;
use zippy_operators::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
};

const MINUTE_NS: i64 = 60_000_000_000;

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("weight", DataType::Float64, false),
    ]))
}

fn batch(ids: Vec<&str>, dts: Vec<i64>, values: Vec<f64>, weights: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(dts).with_timezone("UTC")) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
            Arc::new(Float64Array::from(weights)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn string_values(array: &ArrayRef) -> Vec<String> {
    let values = array.as_any().downcast_ref::<StringArray>().unwrap();
    (0..values.len())
        .map(|index| values.value(index).to_string())
        .collect()
}

fn timestamp_values(array: &ArrayRef) -> Vec<i64> {
    let values = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn float_values(array: &ArrayRef) -> Vec<f64> {
    let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn specs() -> Vec<Box<dyn zippy_operators::AggregationSpec>> {
    vec![
        AggFirstSpec::new("value", "first_value").build().unwrap(),
        AggLastSpec::new("value", "last_value").build().unwrap(),
        AggSumSpec::new("value", "sum_value").build().unwrap(),
    ]
}

fn full_v1_specs() -> Vec<Box<dyn zippy_operators::AggregationSpec>> {
    vec![
        AggFirstSpec::new("value", "first_value").build().unwrap(),
        AggLastSpec::new("value", "last_value").build().unwrap(),
        AggSumSpec::new("value", "sum_value").build().unwrap(),
        AggMaxSpec::new("value", "max_value").build().unwrap(),
        AggMinSpec::new("value", "min_value").build().unwrap(),
        AggCountSpec::new("value", "count_value").build().unwrap(),
        AggVwapSpec::new("value", "weight", "vwap_value")
            .build()
            .unwrap(),
    ]
}

#[test]
fn timeseries_engine_flushes_open_windows() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["a", "a"],
            vec![1_000_000_000, 30_000_000_000],
            vec![10.0, 12.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);

    let output = &flushed[0];

    assert_eq!(
        column_names(output),
        vec![
            "id",
            "window_start",
            "window_end",
            "first_value",
            "last_value",
            "sum_value",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
    assert_eq!(string_values(output.column(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(output.column(1)), vec![0]);
    assert_eq!(timestamp_values(output.column(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(output.column(3)), vec![10.0]);
    assert_eq!(float_values(output.column(4)), vec![12.0]);
    assert_eq!(float_values(output.column(5)), vec![22.0]);
}

#[test]
fn timeseries_engine_emits_completed_window_on_window_transition() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["a", "a"],
            vec![1_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 12.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);

    let completed = &outputs[0];

    assert_eq!(string_values(completed.column(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(completed.column(1)), vec![0]);
    assert_eq!(timestamp_values(completed.column(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(completed.column(3)), vec![10.0]);
    assert_eq!(float_values(completed.column(4)), vec![10.0]);
    assert_eq!(float_values(completed.column(5)), vec![10.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![MINUTE_NS]);
    assert_eq!(timestamp_values(flushed[0].column(2)), vec![2 * MINUTE_NS]);
    assert_eq!(float_values(flushed[0].column(3)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column(4)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column(5)), vec![12.0]);
}

#[test]
fn timeseries_engine_rejects_late_data_for_same_id() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
    )
    .unwrap();

    engine
        .on_data(batch(
            vec!["a"],
            vec![MINUTE_NS + 1_000_000_000],
            vec![12.0],
            vec![1.0],
        ))
        .unwrap();

    let error = engine
        .on_data(batch(
            vec!["a"],
            vec![30_000_000_000],
            vec![10.0],
            vec![1.0],
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::LateData {
            dt: 30_000_000_000,
            last_dt
        } if last_dt == MINUTE_NS + 1_000_000_000
    ));

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![MINUTE_NS]);
    assert_eq!(timestamp_values(flushed[0].column(2)), vec![2 * MINUTE_NS]);
    assert_eq!(float_values(flushed[0].column(3)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column(4)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column(5)), vec![12.0]);
}

#[test]
fn timeseries_engine_rejects_late_rows_and_supports_full_v1_aggregations() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        full_v1_specs(),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["a", "a", "a"],
            vec![1_000_000_000, 30_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 14.0, 20.0],
            vec![2.0, 1.0, 5.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);

    let completed = &outputs[0];

    assert_eq!(
        column_names(completed),
        vec![
            "id",
            "window_start",
            "window_end",
            "first_value",
            "last_value",
            "sum_value",
            "max_value",
            "min_value",
            "count_value",
            "vwap_value",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
    assert_eq!(string_values(completed.column(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(completed.column(1)), vec![0]);
    assert_eq!(timestamp_values(completed.column(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(completed.column(3)), vec![10.0]);
    assert_eq!(float_values(completed.column(4)), vec![14.0]);
    assert_eq!(float_values(completed.column(5)), vec![24.0]);
    assert_eq!(float_values(completed.column(6)), vec![14.0]);
    assert_eq!(float_values(completed.column(7)), vec![10.0]);
    assert_eq!(float_values(completed.column(8)), vec![2.0]);
    assert_eq!(float_values(completed.column(9)), vec![34.0 / 3.0]);

    let error = engine
        .on_data(batch(
            vec!["a"],
            vec![30_000_000_000],
            vec![11.0],
            vec![1.0],
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::LateData {
            dt: 30_000_000_000,
            last_dt
        } if last_dt == MINUTE_NS + 1_000_000_000
    ));

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![MINUTE_NS]);
    assert_eq!(timestamp_values(flushed[0].column(2)), vec![2 * MINUTE_NS]);
    assert_eq!(float_values(flushed[0].column(3)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column(4)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column(5)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column(6)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column(7)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column(8)), vec![1.0]);
    assert_eq!(float_values(flushed[0].column(9)), vec![20.0]);
}

#[test]
fn timeseries_engine_errors_on_zero_weight_vwap_window() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        full_v1_specs(),
    )
    .unwrap();

    engine
        .on_data(batch(vec!["a"], vec![1_000_000_000], vec![10.0], vec![0.0]))
        .unwrap();

    let error = engine.on_flush().unwrap_err();

    assert!(matches!(
        error,
        ZippyError::InvalidState {
            status: "vwap denominator is zero",
        }
    ));
}

#[test]
fn timeseries_engine_drop_with_metric_skips_late_rows_and_keeps_valid_rows() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::DropWithMetric,
        specs(),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["a", "a", "a"],
            vec![
                MINUTE_NS + 1_000_000_000,
                30_000_000_000,
                MINUTE_NS + 30_000_000_000,
            ],
            vec![20.0, 999.0, 22.0],
            vec![1.0, 1.0, 1.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());

    let outputs = engine
        .on_data(batch(
            vec!["a"],
            vec![2 * MINUTE_NS + 1_000_000_000],
            vec![30.0],
            vec![1.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_values(outputs[0].column(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(outputs[0].column(1)), vec![MINUTE_NS]);
    assert_eq!(timestamp_values(outputs[0].column(2)), vec![2 * MINUTE_NS]);
    assert_eq!(float_values(outputs[0].column(3)), vec![20.0]);
    assert_eq!(float_values(outputs[0].column(4)), vec![22.0]);
    assert_eq!(float_values(outputs[0].column(5)), vec![42.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![2 * MINUTE_NS]);
    assert_eq!(timestamp_values(flushed[0].column(2)), vec![3 * MINUTE_NS]);
    assert_eq!(float_values(flushed[0].column(3)), vec![30.0]);
    assert_eq!(float_values(flushed[0].column(4)), vec![30.0]);
    assert_eq!(float_values(flushed[0].column(5)), vec![30.0]);
}

#[test]
fn timeseries_runtime_records_late_rows_metric_for_drop_with_metric() {
    let engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::DropWithMetric,
        specs(),
    )
    .unwrap();
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "timeseries-late-metric".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: LateDataPolicy::DropWithMetric,
        },
    )
    .unwrap();

    handle
        .write(batch(
            vec!["a", "a", "a"],
            vec![1_000_000_000, MINUTE_NS + 1_000_000_000, 30_000_000_000],
            vec![10.0, 12.0, 11.0],
            vec![1.0, 1.0, 1.0],
        ))
        .unwrap();

    handle.stop().unwrap();

    assert_eq!(handle.metrics().late_rows_total, 1);
}
