use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_operators::{CSDemeanSpec, CSRankSpec, CSZscoreSpec};

fn required_schema(nullable_value: bool) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("ret_1m", DataType::Float64, nullable_value),
    ]))
}

fn batch(values: Vec<f64>) -> RecordBatch {
    let len = values.len();

    RecordBatch::try_new(
        required_schema(false),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"][..len].to_vec())) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![0_i64; len]).with_timezone("UTC".to_string()),
            ) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nullable_batch(values: Vec<Option<f64>>) -> RecordBatch {
    let len = values.len();

    RecordBatch::try_new(
        required_schema(true),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"][..len].to_vec())) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![0_i64; len]).with_timezone("UTC".to_string()),
            ) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn float_values(array: &ArrayRef) -> Vec<Option<f64>> {
    let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..values.len())
        .map(|index| (!values.is_null(index)).then(|| values.value(index)))
        .collect()
}

fn assert_float_options_eq(left: &[Option<f64>], right: &[Option<f64>]) {
    assert_eq!(left.len(), right.len());

    for (lhs, rhs) in left.iter().zip(right.iter()) {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => assert!((lhs - rhs).abs() < 1e-12),
            (None, None) => {}
            _ => panic!("mismatched optional float values"),
        }
    }
}

#[test]
fn cs_rank_uses_average_rank_for_ties() {
    let mut factor = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();

    let output = factor.evaluate(&batch(vec![1.0, 2.0, 2.0])).unwrap();

    assert_eq!(
        factor.output_field(),
        Field::new("ret_rank", DataType::Float64, true)
    );
    assert_eq!(output.data_type(), &DataType::Float64);
    assert_float_options_eq(&float_values(&output), &[Some(1.0), Some(2.5), Some(2.5)]);
}

#[test]
fn cs_zscore_returns_zero_for_zero_variance() {
    let mut factor = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();

    let output = factor.evaluate(&batch(vec![5.0, 5.0, 5.0])).unwrap();

    assert_eq!(output.data_type(), &DataType::Float64);
    assert_float_options_eq(&float_values(&output), &[Some(0.0), Some(0.0), Some(0.0)]);
}

#[test]
fn cs_zscore_returns_null_when_all_samples_missing() {
    let mut factor = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();

    let output = factor
        .evaluate(&nullable_batch(vec![None, None, None]))
        .unwrap();

    assert_eq!(output.data_type(), &DataType::Float64);
    assert_float_options_eq(&float_values(&output), &[None, None, None]);
}

#[test]
fn cs_factors_skip_null_samples_and_preserve_null_output() {
    let input = nullable_batch(vec![Some(1.0), None, Some(3.0)]);

    let mut rank = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();
    let mut zscore = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();
    let mut demean = CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap();

    let rank_output = rank.evaluate(&input).unwrap();
    let zscore_output = zscore.evaluate(&input).unwrap();
    let demean_output = demean.evaluate(&input).unwrap();

    assert_eq!(rank_output.data_type(), &DataType::Float64);
    assert_eq!(zscore_output.data_type(), &DataType::Float64);
    assert_eq!(demean_output.data_type(), &DataType::Float64);
    assert_float_options_eq(&float_values(&rank_output), &[Some(1.0), None, Some(2.0)]);
    assert_float_options_eq(
        &float_values(&zscore_output),
        &[Some(-1.0), None, Some(1.0)],
    );
    assert_float_options_eq(
        &float_values(&demean_output),
        &[Some(-1.0), None, Some(1.0)],
    );
}

#[test]
fn cs_factors_treat_nan_as_missing_sample() {
    let input = batch(vec![1.0, f64::NAN, 3.0]);

    let mut rank = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();
    let mut zscore = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();
    let mut demean = CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap();

    let rank_output = rank.evaluate(&input).unwrap();
    let zscore_output = zscore.evaluate(&input).unwrap();
    let demean_output = demean.evaluate(&input).unwrap();

    assert_float_options_eq(&float_values(&rank_output), &[Some(1.0), None, Some(2.0)]);
    assert_float_options_eq(
        &float_values(&zscore_output),
        &[Some(-1.0), None, Some(1.0)],
    );
    assert_float_options_eq(
        &float_values(&demean_output),
        &[Some(-1.0), None, Some(1.0)],
    );
}
