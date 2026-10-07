use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_engines::KeyValueState;
use zippy_types::ingest::KeyValueMutation;
use zippy_types::key_value::{change_batch, log_schema};
use zippy_types::TableId;

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

fn empty_snapshot(table: TableId) -> RecordBatch {
    let logical = batch(vec![], vec![]).schema();
    let metadata = [
        ("zippy_kv_protocol".to_owned(), "2".to_owned()),
        ("zippy_kv_seq".to_owned(), "0".to_owned()),
        ("zippy_kv_version".to_owned(), "0".to_owned()),
        (
            "zippy_kv_stream_id".to_owned(),
            format!("{:032x}", table.as_u128()),
        ),
        ("zippy_kv_keys".to_owned(), "[\"symbol\"]".to_owned()),
    ]
    .into();
    RecordBatch::new_empty(Arc::new(logical.as_ref().clone().with_metadata(metadata)))
}

fn native_upsert(
    table: TableId,
    version: u64,
    first_sequence: u64,
    rows: RecordBatch,
) -> RecordBatch {
    let physical = log_schema(&rows.schema(), &["symbol".to_owned()]).unwrap();
    change_batch(
        physical,
        &KeyValueMutation::Upsert(rows),
        table,
        version,
        first_sequence,
    )
    .unwrap()
}

#[test]
fn native_snapshot_cursor_fragments_duplicates_gaps_and_budget() {
    let table = TableId::from_u128(41).unwrap();
    let empty = empty_snapshot(table);
    let mut consumer = KeyValueState::from_snapshot(empty.clone(), 4096).unwrap();
    let delta = native_upsert(table, 1, 1, batch(vec!["A", "B"], vec![1, 2]));
    assert_eq!(consumer.apply(delta.slice(0, 1)).unwrap(), 0);
    assert_eq!(consumer.sequence(), 0);
    assert_eq!(consumer.snapshot().unwrap().num_rows(), 0);
    assert_eq!(consumer.apply(delta.slice(1, 1)).unwrap(), 1);
    assert_eq!(consumer.sequence(), 2);
    assert_eq!(consumer.apply(delta.clone()).unwrap(), 0);
    let second = native_upsert(table, 2, 3, batch(vec!["A"], vec![9]));
    assert_eq!(second.num_rows(), 1);
    consumer.apply(second.clone()).unwrap();
    let snapshot = consumer.snapshot().unwrap();
    assert_eq!(snapshot.num_rows(), 2);
    assert_eq!(
        snapshot
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[9, 2]
    );
    assert_eq!(snapshot.schema().metadata()["zippy_kv_seq"], "3");
    assert_eq!(snapshot.schema().metadata()["zippy_kv_version"], "2");

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
fn native_bootstrap_skips_covered_events_and_rejects_another_table() {
    let table = TableId::from_u128(42).unwrap();
    let values = batch(vec!["A", "A", "B"], vec![1, 7, 2]);
    let delta = native_upsert(table, 1, 1, values.clone());
    assert_eq!(
        delta.num_rows(),
        3,
        "native log preserves every mutation event"
    );
    let mut state = KeyValueState::from_snapshot(empty_snapshot(table), 4096).unwrap();
    state.apply(delta.clone()).unwrap();
    let snapshot = state.snapshot().unwrap();
    assert_eq!(
        snapshot.num_rows(),
        2,
        "keyed snapshot coalesces repeated keys"
    );
    assert_eq!(
        snapshot
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[7, 2]
    );
    let mut consumer = KeyValueState::from_snapshot(snapshot, 4096).unwrap();
    assert_eq!(consumer.apply(delta).unwrap(), 0);
    let other = TableId::from_u128(43).unwrap();
    assert!(consumer.apply(native_upsert(other, 1, 1, values)).is_err());
    assert!(consumer.snapshot().is_err());
}

#[test]
fn native_deletes_are_atomic_idempotent_and_allow_reinsertion() {
    let logical = batch(vec![], vec![]).schema();
    let table = TableId::from_u128(42).unwrap();
    let physical = log_schema(&logical, &["symbol".to_owned()]).unwrap();
    let empty = empty_snapshot(table);
    let mut consumer = KeyValueState::from_snapshot(empty.clone(), 8192).unwrap();
    let initial = change_batch(
        physical.clone(),
        &KeyValueMutation::Upsert(batch(vec!["a", "b", "a"], vec![1, 2, 3])),
        table,
        1,
        1,
    )
    .unwrap();
    consumer.apply(initial.clone()).unwrap();
    let deletes = change_batch(
        physical.clone(),
        &KeyValueMutation::Delete(batch(vec!["a", "missing", "a"], vec![999, 999, 999])),
        table,
        2,
        4,
    )
    .unwrap();
    assert_eq!(consumer.apply(deletes.slice(0, 2)).unwrap(), 0);
    assert_eq!(consumer.snapshot().unwrap().num_rows(), 2);
    assert_eq!(consumer.apply(deletes.slice(2, 1)).unwrap(), 1);
    assert_eq!(consumer.snapshot().unwrap().num_rows(), 1);
    assert_eq!(consumer.apply(deletes.clone()).unwrap(), 0);
    let reinsert = change_batch(
        physical,
        &KeyValueMutation::Upsert(batch(vec!["a", "c"], vec![7, 8])),
        table,
        3,
        7,
    )
    .unwrap();
    consumer.apply(reinsert).unwrap();
    let state = consumer.snapshot().unwrap();
    assert_eq!(
        state
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec!["a", "b", "c"])
    );
    assert_eq!(
        state
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[7, 2, 8]
    );

    let mut malformed = KeyValueState::from_snapshot(empty, 8192).unwrap();
    malformed.apply(initial).unwrap();
    malformed.apply(deletes.slice(0, 1)).unwrap();
    let mut columns = deletes.columns().to_vec();
    columns[1] = Arc::new(StringArray::from(vec!["delete", "upsert", "delete"]));
    let changed = RecordBatch::try_new(deletes.schema(), columns).unwrap();
    assert!(malformed.apply(changed.slice(1, 2)).is_err());
    assert!(malformed.snapshot().is_err());
}
