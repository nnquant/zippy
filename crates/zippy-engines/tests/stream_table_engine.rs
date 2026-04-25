use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::StreamTableEngine;

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Arc::new(Field::new("instrument_id", DataType::Utf8, false)),
        Arc::new(Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("last_price", DataType::Float64, false)),
    ]))
}

fn input_batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec!["IF2606", "IH2606"])) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    1_710_000_000_000_000_000_i64,
                    1_710_000_000_100_000_000_i64,
                ])
                .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(Float64Array::from(vec![3912.4, 2740.8])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn empty_input_batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone("UTC"))
                as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn stream_table_on_data_passes_batch_through_without_schema_change() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
    let batch = input_batch();
    let view = SegmentTableView::from_record_batch(batch.clone());

    let outputs = engine.on_data(view).unwrap();

    assert_eq!(engine.output_schema(), batch.schema());
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].schema(), batch.schema());
    assert_eq!(outputs[0].num_rows(), batch.num_rows());
    assert_eq!(
        outputs[0].column("instrument_id").unwrap().to_data(),
        batch.column(0).to_data()
    );
    assert_eq!(
        outputs[0].column("dt").unwrap().to_data(),
        batch.column(1).to_data()
    );
    assert_eq!(
        outputs[0].column("last_price").unwrap().to_data(),
        batch.column(2).to_data()
    );
}

#[test]
fn stream_table_flush_and_stop_emit_no_extra_batches() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();

    assert!(engine.on_flush().unwrap().is_empty());
    assert!(engine.on_stop().unwrap().is_empty());
}

#[test]
fn stream_table_on_data_passes_empty_batch_through_without_modification() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
    let batch = empty_input_batch();
    let view = SegmentTableView::from_record_batch(batch.clone());

    let outputs = engine.on_data(view).unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].schema(), batch.schema());
    assert_eq!(outputs[0].num_rows(), 0);
    assert_eq!(
        outputs[0].column("instrument_id").unwrap().to_data(),
        batch.column(0).to_data()
    );
    assert_eq!(
        outputs[0].column("dt").unwrap().to_data(),
        batch.column(1).to_data()
    );
    assert_eq!(
        outputs[0].column("last_price").unwrap().to_data(),
        batch.column(2).to_data()
    );
}

#[test]
fn stream_table_rejects_schema_mismatch() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
    let wrong_schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("instrument_id", DataType::Utf8, false)),
        Arc::new(Field::new("last_price", DataType::Float64, false)),
    ]));
    let batch = RecordBatch::try_new(
        wrong_schema,
        vec![
            Arc::new(StringArray::from(vec!["IF2606"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![3912.4])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = engine
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
    assert!(error.to_string().contains("stream table input schema"));
}
