use arrow::array::{Float64Array, StringArray};
use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan,
    SegmentCellValue, SegmentStore, SegmentStoreConfig,
};

fn tick_schema() -> CompiledSchema {
    compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap()
}

#[test]
fn active_segment_reader_reads_incremental_spans_from_descriptor_envelope() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    let writer = partition.writer();
    writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();
    writer.append_tick_for_test(2, "IF2606", 4113.0).unwrap();

    let envelope = partition.active_descriptor_envelope_bytes().unwrap();
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();

    let first = reader.read_available().unwrap().expect("expected rows");
    assert_eq!(first.start_row(), 0);
    assert_eq!(first.end_row(), 2);
    assert!(reader.read_available().unwrap().is_none());

    writer.append_tick_for_test(3, "IF2606", 4114.5).unwrap();

    let second = reader
        .read_available()
        .unwrap()
        .expect("expected appended row");
    let batch = second.as_record_batch().unwrap();
    let instruments = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let prices = batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(second.start_row(), 2);
    assert_eq!(second.end_row(), 3);
    assert_eq!(instruments.value(0), "IF2606");
    assert_eq!(prices.value(0), 4114.5);
}

#[test]
fn active_segment_reader_can_seek_to_current_committed_tail() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    let writer = partition.writer();
    writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();
    writer.append_tick_for_test(2, "IF2607", 4113.0).unwrap();

    let envelope = partition.active_descriptor_envelope_bytes().unwrap();
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();

    assert_eq!(reader.seek_to_committed().unwrap(), 2);
    assert!(reader.read_available().unwrap().is_none());

    writer.append_tick_for_test(3, "IF2608", 4114.5).unwrap();

    let span = reader
        .read_available()
        .unwrap()
        .expect("expected appended row");
    let batch = span.as_record_batch().unwrap();
    let instruments = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(span.start_row(), 2);
    assert_eq!(span.end_row(), 3);
    assert_eq!(instruments.value(0), "IF2608");
}

#[test]
fn active_row_span_reads_cells_without_record_batch_materialization() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    let writer = partition.writer();
    writer.append_tick_for_test(101, "IF2606", 4112.5).unwrap();

    let envelope = partition.active_descriptor_envelope_bytes().unwrap();
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();
    let span = reader.read_available().unwrap().expect("expected row");

    assert_eq!(span.row_count(), 1);
    assert_eq!(
        span.cell_value(0, "dt").unwrap(),
        SegmentCellValue::TimestampNs(101)
    );
    assert_eq!(
        span.cell_value(0, "instrument_id").unwrap(),
        SegmentCellValue::Utf8("IF2606".to_string())
    );
    assert_eq!(
        span.cell_value(0, "last_price").unwrap(),
        SegmentCellValue::Float64(4112.5)
    );
}

#[test]
fn active_segment_reader_resets_cursor_after_descriptor_rollover() {
    let schema = tick_schema();
    let layout = LayoutPlan::for_schema(&schema, 32).unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let partition = store
        .open_partition_with_schema("openctp_ticks", "all", schema.clone())
        .unwrap();
    let writer = partition.writer();
    writer.append_tick_for_test(1, "IF2606", 4112.5).unwrap();

    let envelope = partition.active_descriptor_envelope_bytes().unwrap();
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&envelope, schema.clone(), layout.clone())
            .unwrap();
    assert_eq!(reader.read_available().unwrap().unwrap().end_row(), 1);

    writer.rollover().unwrap();
    writer.append_tick_for_test(2, "IF2606", 4113.5).unwrap();
    let next_envelope = partition.active_descriptor_envelope_bytes().unwrap();

    reader
        .update_descriptor_envelope(&next_envelope, schema, layout)
        .unwrap();

    let span = reader
        .read_available()
        .unwrap()
        .expect("expected new segment row");
    assert_eq!(span.start_row(), 0);
    assert_eq!(span.end_row(), 1);
}
