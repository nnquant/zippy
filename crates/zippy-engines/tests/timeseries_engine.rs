use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{
    spawn_engine, Engine, EngineConfig, LateDataPolicy, SegmentTableView, ZippyError,
};
use zippy_engines::TimeSeriesEngine;
use zippy_operators::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
    ExpressionSpec,
};

const MINUTE_NS: i64 = 60_000_000_000;

fn input_schema() -> Arc<Schema> {
    input_schema_with_timezone("UTC")
}

fn input_schema_with_timezone(timezone: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone.into())),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("weight", DataType::Float64, false),
    ]))
}

fn record_batch(ids: Vec<&str>, dts: Vec<i64>, values: Vec<f64>, weights: Vec<f64>) -> RecordBatch {
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

fn batch(ids: Vec<&str>, dts: Vec<i64>, values: Vec<f64>, weights: Vec<f64>) -> SegmentTableView {
    SegmentTableView::from_record_batch(record_batch(ids, dts, values, weights))
}

fn string_values(array: zippy_core::Result<ArrayRef>) -> Vec<String> {
    let array = array.unwrap();
    let values = array.as_any().downcast_ref::<StringArray>().unwrap();
    (0..values.len())
        .map(|index| values.value(index).to_string())
        .collect()
}

fn timestamp_values(array: zippy_core::Result<ArrayRef>) -> Vec<i64> {
    let array = array.unwrap();
    let values = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn float_values(array: zippy_core::Result<ArrayRef>) -> Vec<f64> {
    let array = array.unwrap();
    let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..values.len()).map(|index| values.value(index)).collect()
}

fn column_names(table: &SegmentTableView) -> Vec<String> {
    table
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

fn pre_turnover_exprs() -> Vec<ExpressionSpec> {
    vec![ExpressionSpec::new("value * weight", "turnover")]
}

fn post_return_exprs() -> Vec<ExpressionSpec> {
    vec![ExpressionSpec::new("close / open - 1.0", "ret_1m")]
}

fn post_invalid_input_exprs() -> Vec<ExpressionSpec> {
    vec![ExpressionSpec::new("value * 2.0", "bad")]
}

fn post_invalid_runtime_exprs() -> Vec<ExpressionSpec> {
    vec![ExpressionSpec::new("LOG(close - open)", "bad_log")]
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
        vec![],
        vec![],
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
    assert_eq!(string_values(output.column_at(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(output.column_at(1)), vec![0]);
    assert_eq!(timestamp_values(output.column_at(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(output.column_at(3)), vec![10.0]);
    assert_eq!(float_values(output.column_at(4)), vec![12.0]);
    assert_eq!(float_values(output.column_at(5)), vec![22.0]);
}

#[test]
fn timeseries_engine_accepts_shanghai_timestamp_input_schema() {
    let engine = TimeSeriesEngine::new(
        "bars",
        input_schema_with_timezone("Asia/Shanghai"),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
        vec![],
        vec![],
    );

    assert!(engine.is_ok());
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
        vec![],
        vec![],
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

    assert_eq!(string_values(completed.column_at(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(completed.column_at(1)), vec![0]);
    assert_eq!(timestamp_values(completed.column_at(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(completed.column_at(3)), vec![10.0]);
    assert_eq!(float_values(completed.column_at(4)), vec![10.0]);
    assert_eq!(float_values(completed.column_at(5)), vec![10.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(timestamp_values(flushed[0].column_at(1)), vec![MINUTE_NS]);
    assert_eq!(
        timestamp_values(flushed[0].column_at(2)),
        vec![2 * MINUTE_NS]
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column_at(4)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column_at(5)), vec![12.0]);
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
        vec![],
        vec![],
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
    assert_eq!(timestamp_values(flushed[0].column_at(1)), vec![MINUTE_NS]);
    assert_eq!(
        timestamp_values(flushed[0].column_at(2)),
        vec![2 * MINUTE_NS]
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column_at(4)), vec![12.0]);
    assert_eq!(float_values(flushed[0].column_at(5)), vec![12.0]);
}

#[test]
fn timeseries_engine_id_filter_keeps_only_whitelisted_rows_and_counts_filtered_rows() {
    let mut engine = TimeSeriesEngine::new_with_id_filter(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
        vec![],
        vec![],
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "B"],
            vec![1_000_000_000, 2_000_000_000],
            vec![10.0, 99.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(
        string_values(flushed[0].column_at(0)),
        vec!["A".to_string()]
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![10.0]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn timeseries_engine_filtered_rows_do_not_increment_late_rows_metric() {
    let mut engine = TimeSeriesEngine::new_with_id_filter(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::DropWithMetric,
        specs(),
        vec![],
        vec![],
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["B", "B"],
            vec![MINUTE_NS + 1_000_000_000, 30_000_000_000],
            vec![20.0, 21.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());
    let metrics = engine.drain_metrics();
    assert_eq!(metrics.late_rows_total, 0);
    assert_eq!(metrics.filtered_rows_total, 2);
}

#[test]
fn timeseries_engine_all_filtered_batch_returns_no_output_and_no_error() {
    let mut engine = TimeSeriesEngine::new_with_id_filter(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
        vec![],
        vec![],
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["B", "C"],
            vec![1_000_000_000, 2_000_000_000],
            vec![20.0, 21.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());
    assert_eq!(engine.drain_metrics().filtered_rows_total, 2);
    assert!(engine.on_flush().unwrap().is_empty());
}

#[test]
fn timeseries_engine_rejects_empty_id_filter() {
    let result = TimeSeriesEngine::new_with_id_filter(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
        vec![],
        vec![],
        Some(vec![]),
    );

    match result {
        Err(ZippyError::InvalidConfig { reason }) => {
            assert!(reason.contains("id_filter must not be empty"));
        }
        Ok(_) => panic!("expected empty id_filter to be rejected"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
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
        vec![],
        vec![],
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
    assert_eq!(string_values(completed.column_at(0)), vec!["a".to_string()]);
    assert_eq!(timestamp_values(completed.column_at(1)), vec![0]);
    assert_eq!(timestamp_values(completed.column_at(2)), vec![MINUTE_NS]);
    assert_eq!(float_values(completed.column_at(3)), vec![10.0]);
    assert_eq!(float_values(completed.column_at(4)), vec![14.0]);
    assert_eq!(float_values(completed.column_at(5)), vec![24.0]);
    assert_eq!(float_values(completed.column_at(6)), vec![14.0]);
    assert_eq!(float_values(completed.column_at(7)), vec![10.0]);
    assert_eq!(float_values(completed.column_at(8)), vec![2.0]);
    assert_eq!(float_values(completed.column_at(9)), vec![34.0 / 3.0]);

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
    assert_eq!(timestamp_values(flushed[0].column_at(1)), vec![MINUTE_NS]);
    assert_eq!(
        timestamp_values(flushed[0].column_at(2)),
        vec![2 * MINUTE_NS]
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column_at(4)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column_at(5)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column_at(6)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column_at(7)), vec![20.0]);
    assert_eq!(float_values(flushed[0].column_at(8)), vec![1.0]);
    assert_eq!(float_values(flushed[0].column_at(9)), vec![20.0]);
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
        vec![],
        vec![],
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
        vec![],
        vec![],
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
    assert_eq!(
        string_values(outputs[0].column_at(0)),
        vec!["a".to_string()]
    );
    assert_eq!(timestamp_values(outputs[0].column_at(1)), vec![MINUTE_NS]);
    assert_eq!(
        timestamp_values(outputs[0].column_at(2)),
        vec![2 * MINUTE_NS]
    );
    assert_eq!(float_values(outputs[0].column_at(3)), vec![20.0]);
    assert_eq!(float_values(outputs[0].column_at(4)), vec![22.0]);
    assert_eq!(float_values(outputs[0].column_at(5)), vec![42.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(
        timestamp_values(flushed[0].column_at(1)),
        vec![2 * MINUTE_NS]
    );
    assert_eq!(
        timestamp_values(flushed[0].column_at(2)),
        vec![3 * MINUTE_NS]
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![30.0]);
    assert_eq!(float_values(flushed[0].column_at(4)), vec![30.0]);
    assert_eq!(float_values(flushed[0].column_at(5)), vec![30.0]);
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
        vec![],
        vec![],
    )
    .unwrap();
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "timeseries-late-metric".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: LateDataPolicy::DropWithMetric,
            xfast: false,
        },
    )
    .unwrap();

    handle
        .write(record_batch(
            vec!["a", "a", "a"],
            vec![1_000_000_000, MINUTE_NS + 1_000_000_000, 30_000_000_000],
            vec![10.0, 12.0, 11.0],
            vec![1.0, 1.0, 1.0],
        ))
        .unwrap();

    handle.stop().unwrap();

    assert_eq!(handle.metrics().late_rows_total, 1);
}

#[test]
fn timeseries_runtime_records_filtered_rows_metric() {
    let engine = TimeSeriesEngine::new_with_id_filter(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        specs(),
        vec![],
        vec![],
        Some(vec!["a".to_string()]),
    )
    .unwrap();
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "timeseries-filtered-metric".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: LateDataPolicy::Reject,
            xfast: false,
        },
    )
    .unwrap();

    handle
        .write(record_batch(
            vec!["a", "b"],
            vec![1_000_000_000, 2_000_000_000],
            vec![10.0, 99.0],
            vec![1.0, 1.0],
        ))
        .unwrap();

    handle.stop().unwrap();

    assert_eq!(handle.metrics().filtered_rows_total, 1);
    assert_eq!(handle.metrics().late_rows_total, 0);
}

#[test]
fn timeseries_engine_pre_factors_can_generate_columns_for_agg_sum_inputs() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            AggSumSpec::new("turnover", "turnover").build().unwrap(),
            AggSumSpec::new("weight", "volume").build().unwrap(),
        ],
        pre_turnover_exprs(),
        vec![],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["a", "a"],
            vec![1_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![10.0, 12.0],
            vec![2.0, 3.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(
        column_names(&outputs[0]),
        vec!["id", "window_start", "window_end", "turnover", "volume"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(float_values(outputs[0].column_at(3)), vec![20.0]);
    assert_eq!(float_values(outputs[0].column_at(4)), vec![2.0]);
}

#[test]
fn timeseries_engine_post_factors_can_extend_aggregate_outputs() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            AggFirstSpec::new("value", "open").build().unwrap(),
            AggLastSpec::new("value", "close").build().unwrap(),
        ],
        vec![],
        post_return_exprs(),
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
    assert_eq!(
        column_names(&outputs[0]),
        vec![
            "id",
            "window_start",
            "window_end",
            "open",
            "close",
            "ret_1m"
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
    assert_eq!(float_values(outputs[0].column_at(3)), vec![10.0]);
    assert_eq!(float_values(outputs[0].column_at(4)), vec![10.0]);
    assert_eq!(float_values(outputs[0].column_at(5)), vec![0.0]);
}

#[test]
fn timeseries_engine_post_factors_reject_raw_input_column_references() {
    let result = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![AggSumSpec::new("value", "sum_value").build().unwrap()],
        vec![],
        post_invalid_input_exprs(),
    );

    match result {
        Err(ZippyError::InvalidConfig { reason }) => {
            assert!(reason.contains("unknown expression identifier"));
            assert!(reason.contains("value"));
        }
        Ok(_) => panic!("expected post factor with raw input reference to be rejected"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn timeseries_engine_pre_factors_reject_ts_expr_nodes() {
    let result = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![AggSumSpec::new("value", "sum_value").build().unwrap()],
        vec![ExpressionSpec::new("TS_DIFF(value, 2)", "bad")],
        vec![],
    );

    match result {
        Err(ZippyError::InvalidConfig { reason }) => {
            assert!(reason
                .contains("stateful TS_* functions are only supported inside ReactiveStateEngine"));
        }
        Ok(_) => panic!("expected pre factor with TS_* expression to be rejected"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn timeseries_engine_post_factors_reject_ts_expr_nodes() {
    let result = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![AggSumSpec::new("value", "sum_value").build().unwrap()],
        vec![],
        vec![ExpressionSpec::new("TS_STD(sum_value, 2)", "bad")],
    );

    match result {
        Err(ZippyError::InvalidConfig { reason }) => {
            assert!(reason
                .contains("stateful TS_* functions are only supported inside ReactiveStateEngine"));
        }
        Ok(_) => panic!("expected post factor with TS_* expression to be rejected"),
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn timeseries_engine_flush_runs_post_factors() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            AggFirstSpec::new("value", "open").build().unwrap(),
            AggLastSpec::new("value", "close").build().unwrap(),
        ],
        vec![],
        post_return_exprs(),
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
    assert_eq!(
        column_names(&flushed[0]),
        vec![
            "id",
            "window_start",
            "window_end",
            "open",
            "close",
            "ret_1m"
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
    );
    assert_eq!(float_values(flushed[0].column_at(3)), vec![10.0]);
    assert_eq!(float_values(flushed[0].column_at(4)), vec![12.0]);
    assert!((float_values(flushed[0].column_at(5))[0] - 0.2).abs() < 1e-12);
}

#[test]
fn timeseries_engine_post_factor_flush_failure_preserves_pending_windows() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            AggFirstSpec::new("value", "open").build().unwrap(),
            AggLastSpec::new("value", "close").build().unwrap(),
        ],
        vec![],
        post_invalid_runtime_exprs(),
    )
    .unwrap();

    engine
        .on_data(batch(vec!["a"], vec![1_000_000_000], vec![10.0], vec![1.0]))
        .unwrap();

    let first_error = engine.on_flush().unwrap_err();
    let second_error = engine.on_flush().unwrap_err();

    assert!(matches!(
        first_error,
        ZippyError::InvalidState {
            status: "expression log input must be positive",
        }
    ));
    assert!(matches!(
        second_error,
        ZippyError::InvalidState {
            status: "expression log input must be positive",
        }
    ));
}

#[test]
fn timeseries_engine_drop_with_metric_filters_late_rows_before_pre_factors() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::DropWithMetric,
        vec![AggSumSpec::new("log_value", "sum_log_value")
            .build()
            .unwrap()],
        vec![ExpressionSpec::new("LOG(value)", "log_value")],
        vec![],
    )
    .unwrap();

    engine
        .on_data(batch(
            vec!["a"],
            vec![MINUTE_NS + 1_000_000_000],
            vec![10.0],
            vec![1.0],
        ))
        .unwrap();
    engine
        .on_data(batch(vec!["a"], vec![30_000_000_000], vec![0.0], vec![1.0]))
        .unwrap();

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(float_values(flushed[0].column_at(3)), vec![10.0_f64.ln()]);
    assert_eq!(engine.drain_metrics().late_rows_total, 1);
}
