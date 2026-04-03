use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, ZippyError};
use zippy_engines::ReactiveStateEngine;
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

fn mismatched_batch(ids: Vec<&str>, values: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
        .with_metadata(
            [("source".to_string(), "alternate".to_string())]
                .into_iter()
                .collect(),
        ),
    );

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

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

#[test]
fn reactive_engine_appends_factor_columns_in_order() {
    let factors = vec![
        TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        TsReturnSpec::new("id", "value", 2, "ret_2").build().unwrap(),
    ];
    let mut engine =
        ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "a", "a"], vec![10.0, 16.0, 19.0]))
        .unwrap();

    assert_eq!(outputs.len(), 1);

    let output = &outputs[0];

    assert_eq!(
        column_names(output),
        vec!["id", "value", "ema_2", "ret_2"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        output
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::Utf8,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ]
    );
    assert_float_options_eq(
        &float64_values(output.column(2)),
        vec![Some(10.0), Some(14.0), Some(17.333333333333332)]
            .as_slice(),
    );
    assert_float_options_eq(
        &float64_values(output.column(3)),
        vec![None, None, Some(0.9)]
            .as_slice(),
    );
}

#[test]
fn reactive_engine_keeps_factor_state_across_on_data_calls() {
    let factors = vec![
        TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        TsReturnSpec::new("id", "value", 2, "ret_2").build().unwrap(),
    ];
    let mut engine =
        ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let first_outputs = engine.on_data(batch(vec!["a", "a"], vec![10.0, 16.0])).unwrap();
    let second_outputs = engine.on_data(batch(vec!["a", "a"], vec![19.0, 25.0])).unwrap();

    assert_eq!(first_outputs.len(), 1);
    assert_eq!(second_outputs.len(), 1);

    let first = &first_outputs[0];
    let second = &second_outputs[0];

    assert_float_options_eq(
        &float64_values(first.column(2)),
        vec![Some(10.0), Some(14.0)].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(first.column(3)),
        vec![None, None].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(second.column(2)),
        vec![Some(17.333333333333332), Some(22.444444444444443)].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(second.column(3)),
        vec![Some(0.9), Some(0.5625)].as_slice(),
    );
}

#[test]
fn reactive_engine_new_rejects_duplicate_output_field_names() {
    let result = ReactiveStateEngine::new(
        "reactive",
        input_schema(),
        vec![
            TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
            TsReturnSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        ],
    );

    assert!(matches!(result, Err(ZippyError::InvalidConfig { .. })));
}

#[test]
fn reactive_engine_rejects_input_schema_mismatch() {
    let mut engine = ReactiveStateEngine::new(
        "reactive",
        input_schema(),
        vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()],
    )
    .unwrap();

    let error = engine
        .on_data(mismatched_batch(vec!["a"], vec![10.0]))
        .unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
}
