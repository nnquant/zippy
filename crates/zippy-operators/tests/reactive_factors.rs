use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::ZippyError;
use zippy_operators::{TsEmaSpec, TsReturnSpec};

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn batch(ids: Vec<&str>, values: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nullable_batch(ids: Vec<Option<&str>>, values: Vec<Option<f64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn float64_values(array: &ArrayRef) -> Vec<Option<f64>> {
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
fn ema_and_return_follow_row_order() {
    let ema_spec = TsEmaSpec::new("id", "value", 2, "ema_2");
    let ret_spec = TsReturnSpec::new("id", "value", 2, "ret_2");
    let mut ema = ema_spec.build().unwrap();
    let mut ret = ret_spec.build().unwrap();

    let output = batch(
        vec!["a", "b", "a", "a", "b", "a"],
        vec![10.0, 100.0, 16.0, 19.0, 95.0, 25.0],
    );

    let ema_values = float64_values(&ema.evaluate(&output).unwrap());
    let ret_values = float64_values(&ret.evaluate(&output).unwrap());

    assert_eq!(
        ema.output_field(),
        Field::new("ema_2", DataType::Float64, false)
    );
    assert_eq!(
        ret.output_field(),
        Field::new("ret_2", DataType::Float64, true)
    );

    assert_float_options_eq(
        &ema_values,
        &[
            Some(10.0),
            Some(100.0),
            Some(14.0),
            Some(17.333333333333332),
            Some(96.66666666666667),
            Some(22.444444444444443),
        ]
    );
    assert_float_options_eq(&ret_values, &[None, None, None, Some(0.9), None, Some(0.5625)]);
}

#[test]
fn return_outputs_null_during_warmup() {
    let ret_spec = TsReturnSpec::new("id", "value", 3, "ret_3");
    let mut ret = ret_spec.build().unwrap();

    let values = float64_values(&ret.evaluate(&batch(vec!["x", "x"], vec![10.0, 11.0])).unwrap());

    assert_eq!(values, vec![None, None]);
}

#[test]
fn evaluate_returns_schema_mismatch_for_wrong_value_type() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef,
        ],
    )
    .unwrap();
    let ema_spec = TsEmaSpec::new("id", "value", 2, "ema_2");
    let mut ema = ema_spec.build().unwrap();

    let error = ema.evaluate(&batch).unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
}

#[test]
fn reactive_factor_trait_object_is_send() {
    fn assert_send<T: Send>(_: &T) {}

    let spec = TsEmaSpec::new("id", "value", 2, "ema_2");
    let factor = spec.build().unwrap();

    assert_send(&factor);
}

#[test]
fn state_continues_across_evaluate_calls() {
    let ema_spec = TsEmaSpec::new("id", "value", 2, "ema_2");
    let ret_spec = TsReturnSpec::new("id", "value", 2, "ret_2");
    let mut ema = ema_spec.build().unwrap();
    let mut ret = ret_spec.build().unwrap();

    let first_batch = batch(vec!["a", "a"], vec![10.0, 16.0]);
    let second_batch = batch(vec!["a", "a"], vec![19.0, 25.0]);

    let first_ema = float64_values(&ema.evaluate(&first_batch).unwrap());
    let second_ema = float64_values(&ema.evaluate(&second_batch).unwrap());
    let first_ret = float64_values(&ret.evaluate(&first_batch).unwrap());
    let second_ret = float64_values(&ret.evaluate(&second_batch).unwrap());

    assert_float_options_eq(&first_ema, &[Some(10.0), Some(14.0)]);
    assert_float_options_eq(&second_ema, &[Some(17.333333333333332), Some(22.444444444444443)]);
    assert_float_options_eq(&first_ret, &[None, None]);
    assert_float_options_eq(&second_ret, &[Some(0.9), Some(0.5625)]);
}

#[test]
fn evaluate_returns_schema_mismatch_for_null_value() {
    let batch = nullable_batch(vec![Some("x")], vec![None]);
    let ema_spec = TsEmaSpec::new("id", "value", 2, "ema_2");
    let mut ema = ema_spec.build().unwrap();

    let error = ema.evaluate(&batch).unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
}
