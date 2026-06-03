use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{
    spawn_engine, Engine, EngineConfig, EngineMetricsSnapshot, LateDataPolicy, OverflowPolicy,
    SegmentTableView, ZippyError,
};
use zippy_engines::{ReactiveInvalidValuePolicy, ReactiveStateEngine, ReactiveStateFailurePolicy};
use zippy_operators::{
    CastSpec, ExpressionSpec, ReactiveFactor, ReactiveFactorContext, TsDiffSpec, TsEmaSpec,
    TsReturnSpec,
};

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn record_batch(ids: Vec<&str>, values: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(ids)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn batch(ids: Vec<&str>, values: Vec<f64>) -> SegmentTableView {
    SegmentTableView::from_record_batch(record_batch(ids, values))
}

fn mismatched_batch(ids: Vec<&str>, values: Vec<f64>) -> SegmentTableView {
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
    .map(SegmentTableView::from_record_batch)
    .unwrap()
}

fn float64_values(array: zippy_core::Result<ArrayRef>) -> Vec<Option<f64>> {
    let array = array.unwrap();
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

fn column_names(table: &SegmentTableView) -> Vec<String> {
    table
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

struct ContextOnlyFactor {
    output: Field,
}

impl ReactiveFactor for ContextOnlyFactor {
    fn output_field(&self) -> Field {
        self.output.clone()
    }

    fn evaluate(&mut self, _batch: &RecordBatch) -> zippy_core::Result<ArrayRef> {
        panic!("engine should call evaluate_with_context for reactive factors");
    }

    fn evaluate_with_context(
        &mut self,
        ctx: &ReactiveFactorContext<'_>,
    ) -> zippy_core::Result<ArrayRef> {
        let values = ctx
            .column_by_name("value")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        Ok(Arc::new(Float64Array::from(
            (0..ctx.num_rows())
                .map(|row| values.value(row) + 1.0)
                .collect::<Vec<_>>(),
        )) as ArrayRef)
    }
}

#[test]
fn reactive_engine_uses_context_evaluation_path() {
    let mut engine = ReactiveStateEngine::new(
        "reactive",
        input_schema(),
        vec![Box::new(ContextOnlyFactor {
            output: Field::new("value_plus_one", DataType::Float64, false),
        })],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "b"], vec![10.0, 20.0]))
        .unwrap();
    let output = &outputs[0];

    assert_eq!(
        column_names(output),
        vec!["id", "value", "value_plus_one"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        float64_values(output.column_at(2)),
        vec![Some(11.0), Some(21.0)]
    );
}

#[test]
fn reactive_engine_appends_factor_columns_in_order() {
    let factors = vec![
        TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        TsReturnSpec::new("id", "value", 2, "ret_2")
            .build()
            .unwrap(),
    ];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

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
        &float64_values(output.column_at(2)),
        vec![Some(10.0), Some(14.0), Some(17.333333333333332)].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(output.column_at(3)),
        vec![None, None, Some(0.9)].as_slice(),
    );
}

#[test]
fn reactive_engine_keeps_factor_state_across_on_data_calls() {
    let factors = vec![
        TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        TsReturnSpec::new("id", "value", 2, "ret_2")
            .build()
            .unwrap(),
    ];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let first_outputs = engine
        .on_data(batch(vec!["a", "a"], vec![10.0, 16.0]))
        .unwrap();
    let second_outputs = engine
        .on_data(batch(vec!["a", "a"], vec![19.0, 25.0]))
        .unwrap();

    assert_eq!(first_outputs.len(), 1);
    assert_eq!(second_outputs.len(), 1);

    let first = &first_outputs[0];
    let second = &second_outputs[0];

    assert_float_options_eq(
        &float64_values(first.column_at(2)),
        vec![Some(10.0), Some(14.0)].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(first.column_at(3)),
        vec![None, None].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(second.column_at(2)),
        vec![Some(17.333333333333332), Some(22.444444444444443)].as_slice(),
    );
    assert_float_options_eq(
        &float64_values(second.column_at(3)),
        vec![Some(0.9), Some(0.5625)].as_slice(),
    );
}

#[test]
fn reactive_engine_rejects_dbl_max_sentinel_by_default() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let error = engine
        .on_data(batch(vec!["a"], vec![f64::MAX]))
        .unwrap_err();

    assert!(error.to_string().contains("invalid reactive state input"));
}

#[test]
fn reactive_engine_skip_invalid_values_does_not_pollute_ema_state() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let mut engine = ReactiveStateEngine::new_with_invalid_value_policy(
        "reactive",
        input_schema(),
        factors,
        ReactiveInvalidValuePolicy::Skip,
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "a", "a"], vec![10.0, f64::MAX, 16.0]))
        .unwrap();
    let output = &outputs[0];

    assert!(output.schema().field(2).is_nullable());
    assert_float_options_eq(
        &float64_values(output.column_at(2)),
        &[Some(10.0), None, Some(14.0)],
    );
}

#[test]
fn reactive_engine_skip_invalid_values_does_not_pollute_expression_ts_state() {
    let factors = vec![ExpressionSpec::new("TS_RETURN(value, 1)", "ret_1")
        .build_reactive_factor(input_schema().as_ref(), "id")
        .unwrap()];
    let mut engine = ReactiveStateEngine::new_with_invalid_value_policy(
        "reactive",
        input_schema(),
        factors,
        ReactiveInvalidValuePolicy::Skip,
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "a", "a"], vec![10.0, f64::MAX, 15.0]))
        .unwrap();

    assert_float_options_eq(
        &float64_values(outputs[0].column_at(2)),
        &[None, None, Some(0.5)],
    );
}

#[test]
fn reactive_engine_fail_fast_policy_does_not_retain_rollback_state() {
    let factors = vec![TsReturnSpec::new("id", "value", 1, "ret_1")
        .build()
        .unwrap()];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    engine.begin_transaction();
    let failed_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("batch should evaluate before simulated publish failure");
    engine.rollback_transaction().unwrap();

    let retried_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("retry should evaluate after rollback");

    assert_float_options_eq(&float64_values(failed_outputs[0].column_at(2)), &[None]);
    assert_float_options_eq(
        &float64_values(retried_outputs[0].column_at(2)),
        &[Some(0.0)],
    );
}

#[test]
fn reactive_engine_rollback_policy_restores_stateful_factor_state() {
    let factors = vec![TsReturnSpec::new("id", "value", 1, "ret_1")
        .build()
        .unwrap()];
    let mut engine = ReactiveStateEngine::new_with_state_failure_policy(
        "reactive",
        input_schema(),
        factors,
        ReactiveStateFailurePolicy::Rollback,
    )
    .unwrap();

    engine.begin_transaction();
    let failed_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("batch should evaluate before simulated publish failure");
    engine.rollback_transaction().unwrap();

    let retried_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("retry should evaluate after rollback");

    assert_float_options_eq(&float64_values(failed_outputs[0].column_at(2)), &[None]);
    assert_float_options_eq(&float64_values(retried_outputs[0].column_at(2)), &[None]);
}

#[test]
fn reactive_engine_rollback_policy_restores_expression_factor_state() {
    let factors = vec![ExpressionSpec::new("TS_RETURN(value, 1)", "ret_1")
        .build_reactive_factor(input_schema().as_ref(), "id")
        .unwrap()];
    let mut engine = ReactiveStateEngine::new_with_state_failure_policy(
        "reactive",
        input_schema(),
        factors,
        ReactiveStateFailurePolicy::Rollback,
    )
    .unwrap();

    engine.begin_transaction();
    let failed_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("batch should evaluate before simulated publish failure");
    engine.rollback_transaction().unwrap();

    let retried_outputs = engine
        .on_data(batch(vec!["a"], vec![10.0]))
        .expect("retry should evaluate after rollback");

    assert_float_options_eq(&float64_values(failed_outputs[0].column_at(2)), &[None]);
    assert_float_options_eq(&float64_values(retried_outputs[0].column_at(2)), &[None]);
}

#[test]
fn reactive_engine_new_rejects_duplicate_output_field_names() {
    let result = ReactiveStateEngine::new(
        "reactive",
        input_schema(),
        vec![
            TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
            TsReturnSpec::new("id", "value", 2, "ema_2")
                .build()
                .unwrap(),
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

#[test]
fn reactive_engine_supports_mixed_output_dtypes() {
    let factors = vec![
        TsDiffSpec::new("id", "value", 1, "diff_1").build().unwrap(),
        CastSpec::new("id", "value", "int64", "value_i64")
            .build()
            .unwrap(),
    ];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "a"], vec![10.0, 16.0]))
        .unwrap();
    let output = &outputs[0];

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
            DataType::Int64,
        ]
    );
}

#[test]
fn reactive_engine_expression_factor_can_reference_previous_factor_output() {
    let expr_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("ema_2", DataType::Float64, false),
    ]));
    let factors = vec![
        TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap(),
        ExpressionSpec::new("value + ema_2", "value_plus_ema")
            .build(expr_schema.as_ref())
            .unwrap(),
    ];
    let mut engine = ReactiveStateEngine::new("reactive", input_schema(), factors).unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "a"], vec![10.0, 16.0]))
        .unwrap();
    let output = &outputs[0];

    assert_eq!(
        column_names(output),
        vec!["id", "value", "ema_2", "value_plus_ema"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_float_options_eq(
        &float64_values(output.column_at(3)),
        &[Some(20.0), Some(30.0)],
    );
}

#[test]
fn reactive_engine_filters_rows_by_id_whitelist_before_state_updates() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let mut engine = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        factors,
        "id",
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["A", "B"], vec![10.0, 20.0]))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        column_names(output),
        vec!["id", "value", "ema_2"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_float_options_eq(&float64_values(output.column_at(2)), &[Some(10.0)]);
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn reactive_engine_returns_empty_output_when_all_rows_are_filtered() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let mut engine = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        factors,
        "id",
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine.on_data(batch(vec!["B"], vec![20.0])).unwrap();

    assert!(outputs.is_empty());
    assert_eq!(engine.drain_metrics().filtered_rows_total, 1);
}

#[test]
fn reactive_engine_filtered_rows_do_not_pollute_later_whitelist_state() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let mut engine = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        factors,
        "id",
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let filtered_outputs = engine
        .on_data(batch(vec!["B", "B"], vec![100.0, 200.0]))
        .unwrap();
    let whitelist_outputs = engine
        .on_data(batch(vec!["A", "A"], vec![10.0, 16.0]))
        .unwrap();

    assert!(filtered_outputs.is_empty());
    assert_eq!(whitelist_outputs.len(), 1);
    let output = &whitelist_outputs[0];
    assert_eq!(output.num_rows(), 2);
    assert_float_options_eq(
        &float64_values(output.column_at(2)),
        &[Some(10.0), Some(14.0)],
    );
    assert_eq!(engine.drain_metrics().filtered_rows_total, 2);
}

#[test]
fn reactive_engine_never_calls_factors_for_filtered_rows() {
    let seen_ids = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let factor = Box::new(PanicOnUnexpectedIdFactor::new(
        "id",
        "A",
        Arc::clone(&seen_ids),
    ));
    let mut engine = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        vec![factor],
        "id",
        Some(vec!["A".to_string()]),
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["B", "B"], vec![100.0, 200.0]))
        .unwrap();
    assert!(outputs.is_empty());

    let outputs = engine
        .on_data(batch(vec!["A", "A"], vec![10.0, 16.0]))
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_float_options_eq(
        &float64_values(outputs[0].column_at(2)),
        &[Some(0.0), Some(0.0)],
    );
    assert_eq!(
        seen_ids.lock().unwrap().clone(),
        vec![vec!["A".to_string(), "A".to_string()]]
    );
}

fn wait_for_filtered_rows(
    handle: &zippy_core::EngineHandle,
    expected: u64,
) -> EngineMetricsSnapshot {
    let deadline = Instant::now() + Duration::from_secs(1);

    loop {
        let metrics = handle.metrics();
        if metrics.filtered_rows_total == expected {
            return metrics;
        }

        if Instant::now() >= deadline {
            panic!(
                "filtered rows total did not reach expected value expected=[{}] actual=[{}]",
                expected, metrics.filtered_rows_total
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn reactive_engine_runtime_metrics_include_filtered_rows_total() {
    let factors = vec![TsEmaSpec::new("id", "value", 2, "ema_2").build().unwrap()];
    let engine = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        factors,
        "id",
        Some(vec!["A".to_string()]),
    )
    .unwrap();
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "reactive".to_string(),
            buffer_capacity: 16,
            overflow_policy: OverflowPolicy::Block,
            late_data_policy: LateDataPolicy::Reject,
            xfast: false,
        },
    )
    .unwrap();

    handle
        .write(record_batch(vec!["A", "B"], vec![10.0, 20.0]))
        .unwrap();
    let metrics = wait_for_filtered_rows(&handle, 1);

    assert_eq!(metrics.filtered_rows_total, 1);
    assert_eq!(metrics.processed_rows_total, 2);

    handle.stop().unwrap();
}

#[test]
fn reactive_engine_rejects_empty_id_filter() {
    let factors = vec![ExpressionSpec::new("value + 1.0", "bump")
        .build_reactive_factor(input_schema().as_ref(), "id")
        .unwrap()];

    let result = ReactiveStateEngine::new_with_id_filter(
        "reactive",
        input_schema(),
        factors,
        "id",
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

struct PanicOnUnexpectedIdFactor {
    id_field: String,
    allowed_id: String,
    seen_ids: Arc<Mutex<Vec<Vec<String>>>>,
}

impl PanicOnUnexpectedIdFactor {
    fn new(id_field: &str, allowed_id: &str, seen_ids: Arc<Mutex<Vec<Vec<String>>>>) -> Self {
        Self {
            id_field: id_field.to_string(),
            allowed_id: allowed_id.to_string(),
            seen_ids,
        }
    }
}

impl zippy_operators::ReactiveFactor for PanicOnUnexpectedIdFactor {
    fn output_field(&self) -> Field {
        Field::new("guard", DataType::Float64, false)
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> zippy_core::Result<ArrayRef> {
        let index = batch.schema().index_of(&self.id_field).unwrap();
        let ids = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let seen = (0..ids.len())
            .map(|row_index| ids.value(row_index).to_string())
            .collect::<Vec<_>>();

        for id in &seen {
            if id != &self.allowed_id {
                panic!(
                    "factor saw unexpected id id=[{}] allowed_id=[{}]",
                    id, self.allowed_id
                );
            }
        }

        self.seen_ids.lock().unwrap().push(seen);

        Ok(Arc::new(Float64Array::from(vec![0.0; batch.num_rows()])) as ArrayRef)
    }
}
