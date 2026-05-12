use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::ReactiveLatestEngine;

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Utf8, false),
        Field::new("last_price", DataType::Float64, false),
    ]))
}

fn batch(instrument_ids: Vec<&str>, exchange_ids: Vec<&str>, last_prices: Vec<f64>) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(StringArray::from(exchange_ids)) as ArrayRef,
            Arc::new(Float64Array::from(last_prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nullable_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Utf8, false),
        Field::new("last_price", DataType::Float64, true),
    ]))
}

fn nullable_batch(
    instrument_ids: Vec<&str>,
    exchange_ids: Vec<&str>,
    last_prices: Vec<Option<f64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        nullable_input_schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(StringArray::from(exchange_ids)) as ArrayRef,
            Arc::new(Float64Array::from(last_prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nullable_key_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, true),
        Field::new("exchange_id", DataType::Utf8, false),
        Field::new("last_price", DataType::Float64, false),
    ]))
}

fn nullable_key_batch(
    instrument_ids: Vec<Option<&str>>,
    exchange_ids: Vec<&str>,
    last_prices: Vec<f64>,
) -> RecordBatch {
    RecordBatch::try_new(
        nullable_key_schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(StringArray::from(exchange_ids)) as ArrayRef,
            Arc::new(Float64Array::from(last_prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn string_values(batch: &RecordBatch, index: usize) -> Vec<String> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..array.len())
        .map(|row_index| array.value(row_index).to_string())
        .collect()
}

fn float_values(batch: &RecordBatch, index: usize) -> Vec<f64> {
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..array.len())
        .map(|row_index| array.value(row_index))
        .collect()
}

#[test]
fn reactive_latest_engine_emits_latest_row_for_each_updated_group() {
    let mut engine =
        ReactiveLatestEngine::new("latest_ticks", input_schema(), vec!["instrument_id"]).unwrap();
    let input = batch(
        vec!["IF2606", "IH2606", "IF2606"],
        vec!["CFFEX", "CFFEX", "CFFEX"],
        vec![3912.4, 2740.8, 3913.2],
    );

    let outputs = engine
        .on_data(SegmentTableView::from_record_batch(input))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    let output = outputs[0].to_record_batch().unwrap();
    assert_eq!(output.schema(), input_schema());
    assert_eq!(string_values(&output, 0), vec!["IF2606", "IH2606"]);
    assert_eq!(float_values(&output, 2), vec![3913.2, 2740.8]);
}

#[test]
fn reactive_latest_engine_preserves_nullable_values_in_snapshot() {
    let mut engine = ReactiveLatestEngine::new(
        "latest_ticks",
        nullable_input_schema(),
        vec!["instrument_id"],
    )
    .unwrap();

    engine
        .on_data(SegmentTableView::from_record_batch(nullable_batch(
            vec!["IF2606", "IH2606"],
            vec!["CFFEX", "CFFEX"],
            vec![Some(3912.4), None],
        )))
        .unwrap();

    let output = engine.on_flush().unwrap()[0].to_record_batch().unwrap();
    let prices = output
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(output.num_rows(), 2);
    assert_eq!(prices.value(0), 3912.4);
    assert!(prices.is_null(1));
}

#[test]
fn reactive_latest_engine_invalid_key_batch_leaves_existing_state_unchanged() {
    let mut engine =
        ReactiveLatestEngine::new("latest_ticks", nullable_key_schema(), vec!["instrument_id"])
            .unwrap();
    engine
        .on_data(SegmentTableView::from_record_batch(nullable_key_batch(
            vec![Some("IF2606")],
            vec!["CFFEX"],
            vec![3912.4],
        )))
        .unwrap();

    let invalid = nullable_key_batch(
        vec![None, Some("IH2606")],
        vec!["CFFEX", "CFFEX"],
        vec![1.0, 2.0],
    );

    let error = engine
        .on_data(SegmentTableView::from_record_batch(invalid))
        .unwrap_err();
    assert!(error.to_string().contains("contains null"));

    let output = engine.on_flush().unwrap()[0].to_record_batch().unwrap();
    assert_eq!(string_values(&output, 0), vec!["IF2606"]);
    assert_eq!(float_values(&output, 2), vec![3912.4]);
}

#[test]
fn reactive_latest_engine_groups_by_multiple_dimensions() {
    let mut engine = ReactiveLatestEngine::new(
        "latest_ticks",
        input_schema(),
        vec!["instrument_id", "exchange_id"],
    )
    .unwrap();
    let input = batch(
        vec!["IF2606", "IF2606", "IF2606"],
        vec!["CFFEX", "SIM", "CFFEX"],
        vec![3912.4, 3911.0, 3913.2],
    );

    let outputs = engine
        .on_data(SegmentTableView::from_record_batch(input))
        .unwrap();

    let output = outputs[0].to_record_batch().unwrap();
    assert_eq!(string_values(&output, 0), vec!["IF2606", "IF2606"]);
    assert_eq!(string_values(&output, 1), vec!["CFFEX", "SIM"]);
    assert_eq!(float_values(&output, 2), vec![3913.2, 3911.0]);
}

#[test]
fn reactive_latest_engine_flush_emits_full_latest_snapshot() {
    let mut engine =
        ReactiveLatestEngine::new("latest_ticks", input_schema(), vec!["instrument_id"]).unwrap();
    engine
        .on_data(SegmentTableView::from_record_batch(batch(
            vec!["IF2606", "IH2606"],
            vec!["CFFEX", "CFFEX"],
            vec![3912.4, 2740.8],
        )))
        .unwrap();
    engine
        .on_data(SegmentTableView::from_record_batch(batch(
            vec!["IF2606"],
            vec!["CFFEX"],
            vec![3913.2],
        )))
        .unwrap();

    let outputs = engine.on_flush().unwrap();

    let output = outputs[0].to_record_batch().unwrap();
    assert_eq!(string_values(&output, 0), vec!["IF2606", "IH2606"]);
    assert_eq!(float_values(&output, 2), vec![3913.2, 2740.8]);
}
