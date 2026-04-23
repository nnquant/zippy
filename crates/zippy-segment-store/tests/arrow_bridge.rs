use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
};

#[test]
fn row_span_converts_to_record_batch_for_debug_export() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 0, 1).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), schema.columns().len());
}
