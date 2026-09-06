use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::{KeyValueState, KeyValueTableMaterializer};

fn batch(keys: Vec<&str>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

#[test]
fn snapshot_cursor_delta_fragments_duplicates_gaps_and_budget() {
    let initial = batch(vec!["A", "B"], vec![1, 2]);
    let mut producer =
        KeyValueTableMaterializer::new("state_protocol", initial.schema(), vec!["symbol"]).unwrap();
    let capability = producer.snapshot_handle();
    let empty = capability.snapshot().unwrap();
    let mut consumer = KeyValueState::from_snapshot(empty.clone(), 4096).unwrap();
    let delta = producer
        .on_data(SegmentTableView::from_record_batch(initial))
        .unwrap()
        .remove(0)
        .to_record_batch()
        .unwrap();
    assert_eq!(delta.num_rows(), 2);
    assert_eq!(consumer.apply(delta.slice(0, 1)).unwrap(), 0);
    assert_eq!(consumer.sequence(), 0);
    assert_eq!(consumer.snapshot().unwrap().num_rows(), 0);
    assert_eq!(consumer.apply(delta.slice(1, 1)).unwrap(), 1);
    assert_eq!(consumer.sequence(), 2);
    assert_eq!(consumer.apply(delta.clone()).unwrap(), 0);
    assert_eq!(consumer.snapshot().unwrap(), capability.snapshot().unwrap());
    let second = producer
        .on_data(SegmentTableView::from_record_batch(batch(
            vec!["A"],
            vec![9],
        )))
        .unwrap()
        .remove(0)
        .to_record_batch()
        .unwrap();
    assert_eq!(
        second.num_rows(),
        1,
        "main output is a delta, not two-row full state"
    );
    consumer.apply(second.clone()).unwrap();
    assert_eq!(consumer.snapshot().unwrap(), capability.snapshot().unwrap());
    assert!(producer.on_flush().unwrap().is_empty());
    assert!(producer.on_stop().unwrap().is_empty());

    let mut gap = KeyValueState::from_snapshot(empty.clone(), 4096).unwrap();
    assert!(gap.apply(second).is_err());
    assert!(gap.snapshot().is_err());
    assert!(
        gap.apply(delta.clone()).is_err(),
        "gap must remain fail-closed"
    );
    let mut bounded = KeyValueState::from_snapshot(empty, 1).unwrap();
    assert!(bounded.apply(delta).is_err());
    assert!(bounded.snapshot().is_err());
}

#[test]
fn snapshot_bootstrap_skips_covered_rows_and_rejects_another_producer() {
    let values = batch(vec!["A", "A", "B"], vec![1, 7, 2]);
    let mut producer =
        KeyValueTableMaterializer::new("bootstrap", values.schema(), vec!["symbol"]).unwrap();
    let delta = producer
        .on_data(SegmentTableView::from_record_batch(values.clone()))
        .unwrap()
        .remove(0)
        .to_record_batch()
        .unwrap();
    assert_eq!(
        delta.num_rows(),
        2,
        "only keyed state coalesces same-batch duplicate keys"
    );
    let mut consumer =
        KeyValueState::from_snapshot(producer.snapshot_handle().snapshot().unwrap(), 4096).unwrap();
    assert_eq!(consumer.apply(delta).unwrap(), 0);
    let mut other =
        KeyValueTableMaterializer::new("bootstrap", values.schema(), vec!["symbol"]).unwrap();
    let delta = other
        .on_data(SegmentTableView::from_record_batch(values))
        .unwrap()
        .remove(0)
        .to_record_batch()
        .unwrap();
    assert!(consumer.apply(delta).is_err());
}
