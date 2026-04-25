use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, ShmRegion,
};

#[test]
fn reader_only_observes_committed_prefix() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 8).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 1).unwrap();
    writer.write_utf8("instrument_id", "rb2510").unwrap();
    writer.write_f64("last_price", 4123.5).unwrap();
    writer.commit_row().unwrap();
    assert_eq!(writer.committed_row_count(), 1);

    writer.begin_row().unwrap();
    writer.write_i64("dt", 2).unwrap();
    writer.write_utf8("instrument_id", "rb2511").unwrap();

    assert_eq!(writer.committed_row_count(), 1);
    assert!(writer.read_utf8_for_test("instrument_id", 1).is_err());

    writer.write_f64("last_price", 4124.5).unwrap();
    writer.commit_row().unwrap();
    assert_eq!(writer.committed_row_count(), 2);
}

#[test]
fn utf8_offsets_only_become_readable_after_commit() {
    let schema = compile_schema(&[ColumnSpec::new("instrument_id", ColumnType::Utf8)]).unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_utf8("instrument_id", "ag2501").unwrap();
    assert_eq!(writer.committed_row_count(), 0);
    assert!(writer.read_utf8_for_test("instrument_id", 0).is_err());

    writer.commit_row().unwrap();
    assert_eq!(writer.committed_row_count(), 1);
    assert_eq!(
        writer.read_utf8_for_test("instrument_id", 0).unwrap(),
        "ag2501"
    );
}

#[test]
fn write_utf8_respects_layout_values_len_boundary() {
    let schema = compile_schema(&[ColumnSpec::new("instrument_id", ColumnType::Utf8)]).unwrap();
    let layout = LayoutPlan::for_schema(&schema, 1).unwrap();
    let capacity = layout.column("instrument_id").unwrap().values_len;
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();

    writer.begin_row().unwrap();
    let exact_fit = "x".repeat(capacity);
    assert!(writer.write_utf8("instrument_id", &exact_fit).is_ok());
    writer.commit_row().unwrap();
    assert_eq!(writer.committed_row_count(), 1);

    let mut overflow_writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();
    overflow_writer.begin_row().unwrap();
    let oversized = "x".repeat(capacity + 1);

    assert!(overflow_writer
        .write_utf8("instrument_id", &oversized)
        .is_err());
    assert_eq!(overflow_writer.committed_row_count(), 0);
}

#[test]
fn runtime_writer_mirrors_committed_rows_and_column_buffers_into_shared_memory() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout.clone()).unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 7).unwrap();
    writer.write_utf8("instrument_id", "rb2510").unwrap();
    writer.write_f64("last_price", 4123.5).unwrap();
    writer.commit_row().unwrap();

    let shm = ShmRegion::open(writer.shm_os_id_for_test()).unwrap();
    let shm_layout = writer.shm_layout_for_test();
    let payload_offset = shm_layout.payload_offset;

    let mut committed_bytes = [0_u8; 8];
    shm.read_at(shm_layout.committed_row_count_offset, &mut committed_bytes)
        .unwrap();
    assert_eq!(u64::from_ne_bytes(committed_bytes), 1);

    let dt_layout = layout.column("dt").unwrap();
    let mut dt_bytes = [0_u8; 8];
    shm.read_at(payload_offset + dt_layout.values_offset, &mut dt_bytes)
        .unwrap();
    assert_eq!(i64::from_ne_bytes(dt_bytes), 7);

    let instrument_layout = layout.column("instrument_id").unwrap();
    let mut offset_bytes = [0_u8; 4];
    shm.read_at(
        payload_offset + instrument_layout.offsets_offset + 4,
        &mut offset_bytes,
    )
    .unwrap();
    let value_end = u32::from_ne_bytes(offset_bytes) as usize;
    let mut value_bytes = vec![0_u8; value_end];
    shm.read_at(
        payload_offset + instrument_layout.values_offset,
        &mut value_bytes,
    )
    .unwrap();
    assert_eq!(std::str::from_utf8(&value_bytes).unwrap(), "rb2510");
}

#[test]
fn abort_row_discards_open_utf8_write_and_allows_retry() {
    let schema = compile_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 2).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_utf8("instrument_id", "bad").unwrap();
    writer.abort_row().unwrap();

    writer.begin_row().unwrap();
    writer.write_utf8("instrument_id", "ok").unwrap();
    writer.write_f64("last_price", 1.0).unwrap();
    writer.commit_row().unwrap();

    assert_eq!(writer.read_utf8_for_test("instrument_id", 0).unwrap(), "ok");
}
