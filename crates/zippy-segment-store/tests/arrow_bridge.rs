use arrow::{
    array::{Array, Float64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
};
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
};

#[test]
fn row_span_converts_to_record_batch_for_debug_export() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();
    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 1, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), schema.columns().len());

    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into()))
    );

    let dt = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(dt.value(0), 2);
    assert_eq!(dt.timezone().as_deref(), Some("Asia/Shanghai"));

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument.value(0), "rb2505");

    let last_price = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(last_price.value(0), 4125.0);
}

#[test]
fn row_span_exports_nullable_columns_as_true_nulls() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::nullable("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 1).unwrap();
    writer.commit_row().unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 2).unwrap();
    writer.write_utf8("instrument_id", "rb2501").unwrap();
    writer.write_f64("last_price", 4123.5).unwrap();
    writer.commit_row().unwrap();

    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 0, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(instrument.is_null(0));
    assert_eq!(instrument.value(1), "rb2501");

    let last_price = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(last_price.is_null(0));
    assert_eq!(last_price.value(1), 4123.5);
}
