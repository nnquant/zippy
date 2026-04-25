use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::{hash_record_batches, ReactiveStateEngine};
use zippy_operators::TsEmaSpec;

#[test]
fn replaying_same_ticks_produces_same_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(Float64Array::from(vec![10.0, 11.0])),
        ],
    )
    .unwrap();

    let mut first = ReactiveStateEngine::new(
        "run_one",
        schema.clone(),
        vec![TsEmaSpec::new("symbol", "price", 2, "ema_2")
            .build()
            .unwrap()],
    )
    .unwrap();
    let mut second = ReactiveStateEngine::new(
        "run_two",
        schema,
        vec![TsEmaSpec::new("symbol", "price", 2, "ema_2")
            .build()
            .unwrap()],
    )
    .unwrap();

    let first_batches = first
        .on_data(SegmentTableView::from_record_batch(batch.clone()))
        .unwrap()
        .into_iter()
        .map(|table| table.to_record_batch().unwrap())
        .collect::<Vec<_>>();
    let second_batches = second
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap()
        .into_iter()
        .map(|table| table.to_record_batch().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        hash_record_batches(&first_batches),
        hash_record_batches(&second_batches)
    );
}
