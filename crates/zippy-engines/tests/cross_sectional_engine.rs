use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, LateDataPolicy, ZippyError};
use zippy_engines::CrossSectionalEngine;
use zippy_operators::{CSDemeanSpec, CSRankSpec, CSZscoreSpec};

const MINUTE_NS: i64 = 60_000_000_000;

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("ret_1m", DataType::Float64, false),
    ]))
}

fn batch(symbols: Vec<&str>, dts: Vec<i64>, values: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(symbols)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(dts).with_timezone("UTC")) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
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

fn assert_float_slices_eq(actual: &[f64], expected: &[f64]) {
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected.iter()) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "actual=[{actual}] expected=[{expected}]"
        );
    }
}

#[test]
fn cross_sectional_engine_keeps_last_row_per_id_within_bucket() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "A", "B"],
            vec![1_000_000_000, 2_000_000_000, 3_000_000_000],
            vec![1.0, 3.0, 2.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(string_values(flushed[0].column(0)), vec!["A", "B"]);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![0, 0]);
    assert_float_slices_eq(&float_values(flushed[0].column(2)), &[2.0, 1.0]);
}

#[test]
fn cross_sectional_engine_emits_previous_bucket_when_new_bucket_arrives() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["B", "A", "C"],
            vec![2_000_000_000, 1_000_000_000, MINUTE_NS + 1_000_000_000],
            vec![1.0, 3.0, 2.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_values(outputs[0].column(0)), vec!["A", "B"]);
    assert_eq!(timestamp_values(outputs[0].column(1)), vec![0, 0]);
    assert_float_slices_eq(&float_values(outputs[0].column(2)), &[2.0, 1.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(string_values(flushed[0].column(0)), vec!["C"]);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![MINUTE_NS]);
    assert_float_slices_eq(&float_values(flushed[0].column(2)), &[1.0]);
}

#[test]
fn cross_sectional_engine_handles_out_of_order_buckets_within_same_batch() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "C", "B"],
            vec![
                1_000_000_000,
                2 * MINUTE_NS + 1_000_000_000,
                MINUTE_NS + 1_000_000_000,
            ],
            vec![1.0, 3.0, 2.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 2);
    assert_eq!(string_values(outputs[0].column(0)), vec!["A"]);
    assert_eq!(timestamp_values(outputs[0].column(1)), vec![0]);
    assert_float_slices_eq(&float_values(outputs[0].column(2)), &[1.0]);
    assert_eq!(string_values(outputs[1].column(0)), vec!["B"]);
    assert_eq!(timestamp_values(outputs[1].column(1)), vec![MINUTE_NS]);
    assert_float_slices_eq(&float_values(outputs[1].column(2)), &[1.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(string_values(flushed[0].column(0)), vec!["C"]);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![2 * MINUTE_NS]);
    assert_float_slices_eq(&float_values(flushed[0].column(2)), &[1.0]);
}

#[test]
fn cross_sectional_engine_preserves_same_bucket_last_row_after_batch_reordering() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "C", "B", "A"],
            vec![
                MINUTE_NS + 1_000_000_000,
                2 * MINUTE_NS + 1_000_000_000,
                MINUTE_NS + 2_000_000_000,
                MINUTE_NS + 3_000_000_000,
            ],
            vec![1.0, 5.0, 2.0, 3.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_values(outputs[0].column(0)), vec!["A", "B"]);
    assert_eq!(
        timestamp_values(outputs[0].column(1)),
        vec![MINUTE_NS, MINUTE_NS]
    );
    assert_float_slices_eq(&float_values(outputs[0].column(2)), &[2.0, 1.0]);
}

#[test]
fn cross_sectional_engine_flush_emits_current_bucket_with_expected_schema() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            CSRankSpec::new("ret_1m", "ret_rank").build().unwrap(),
            CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap(),
            CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap(),
        ],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["C", "A", "B"],
            vec![3_000_000_000, 1_000_000_000, 2_000_000_000],
            vec![30.0, 10.0, 20.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(
        column_names(&flushed[0]),
        vec!["symbol", "dt", "ret_rank", "ret_z", "ret_dm"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(string_values(flushed[0].column(0)), vec!["A", "B", "C"]);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![0, 0, 0]);
    assert_float_slices_eq(&float_values(flushed[0].column(2)), &[1.0, 2.0, 3.0]);
    assert_float_slices_eq(
        &float_values(flushed[0].column(3)),
        &[-1.224744871391589, 0.0, 1.224744871391589],
    );
    assert_float_slices_eq(&float_values(flushed[0].column(4)), &[-10.0, 0.0, 10.0]);
}

#[test]
fn cross_sectional_engine_flush_without_open_bucket_returns_empty() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let flushed = engine.on_flush().unwrap();

    assert!(flushed.is_empty());
}

#[test]
fn cross_sectional_engine_does_not_backfill_skipped_empty_buckets() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "B"],
            vec![1_000_000_000, 3 * MINUTE_NS + 1_000_000_000],
            vec![1.0, 2.0],
        ))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(string_values(outputs[0].column(0)), vec!["A"]);
    assert_eq!(timestamp_values(outputs[0].column(1)), vec![0]);
    assert_float_slices_eq(&float_values(outputs[0].column(2)), &[1.0]);

    let flushed = engine.on_flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(string_values(flushed[0].column(0)), vec!["B"]);
    assert_eq!(timestamp_values(flushed[0].column(1)), vec![3 * MINUTE_NS]);
    assert_float_slices_eq(&float_values(flushed[0].column(2)), &[1.0]);
}

#[test]
fn cross_sectional_engine_rejects_late_data_for_closed_bucket() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    engine
        .on_data(batch(vec!["A"], vec![1_000_000_000], vec![1.0]))
        .unwrap();
    engine
        .on_data(batch(vec!["B"], vec![MINUTE_NS + 1_000_000_000], vec![2.0]))
        .unwrap();

    let error = engine
        .on_data(batch(vec!["A"], vec![30_000_000_000], vec![99.0]))
        .unwrap_err();

    assert!(matches!(
        error,
        ZippyError::LateData {
            dt: 30_000_000_000,
            ..
        }
    ));
}

#[test]
fn cross_sectional_engine_rejects_drop_with_metric_configuration() {
    let result = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::DropWithMetric,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    );

    assert!(matches!(result, Err(ZippyError::InvalidConfig { .. })));
}

#[test]
fn cross_sectional_engine_rejects_empty_factor_list() {
    let result = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![],
    );

    assert!(matches!(result, Err(ZippyError::InvalidConfig { .. })));
}
