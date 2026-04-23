use zippy_segment_store::{compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan};

#[test]
fn runtime_writer_constructor_uses_explicit_segment_identity() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 2).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_runtime(schema, layout, 9, 3).unwrap();

    writer.append_tick_for_test(1, "rb2510", 4123.5).unwrap();

    assert_eq!(writer.committed_row_count(), 1);
}
