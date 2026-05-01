use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::SegmentTableView;
use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

#[test]
fn segment_table_view_exposes_row_span_for_engine_fast_paths() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "all").unwrap();
    let writer = handle.writer();
    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2502", 4124.5).unwrap();

    let span = handle.active_row_span(0, 2).unwrap();
    let table = SegmentTableView::from_row_span(span);
    let row_view = table.as_segment_row_view().unwrap();

    assert_eq!(row_view.row_span().row_count(), 2);
}

#[test]
fn memory_table_view_has_no_segment_row_span() {
    let schema = Arc::new(Schema::new(vec![Arc::new(Field::new(
        "last_price",
        DataType::Float64,
        false,
    ))]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Float64Array::from(vec![4123.5])) as ArrayRef],
    )
    .unwrap();

    let table = SegmentTableView::from_record_batch(batch);

    assert!(table.as_segment_row_view().is_none());
}
