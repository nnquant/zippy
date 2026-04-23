use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
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
