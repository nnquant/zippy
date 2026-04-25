use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, SegmentStore,
    SegmentStoreConfig,
};

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

#[test]
fn custom_schema_partition_writer_supports_column_writes_and_active_snapshot() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("localtime_ns", ColumnType::Int64),
        ColumnSpec::new("source_emit_ns", ColumnType::Int64),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store
        .open_partition_with_schema("openctp.tick", "rb2510", schema)
        .unwrap();
    let writer = handle.writer();

    writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_i64("localtime_ns", 2)?;
            row.write_i64("source_emit_ns", 3)?;
            row.write_utf8("instrument_id", "rb2510")?;
            row.write_f64("last_price", 4123.5)?;
            Ok(())
        })
        .unwrap();

    let batch = writer.active_record_batch_for_test().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(writer.committed_row_count(), 1);
}
