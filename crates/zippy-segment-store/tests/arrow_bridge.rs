use arrow::{
    array::{Array, Float64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
};
use zippy_segment_store::{
    compile_schema, debug_snapshot_record_batch_for_test, ActiveSegmentDescriptor,
    ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView, ShmRegion,
};

const SHM_SCHEMA_ID_OFFSET: usize = 0;
const SHM_ROW_COUNT_OFFSET: usize = 32;
const SHM_COMMITTED_ROW_COUNT_OFFSET: usize = 40;
const SHM_MAGIC_OFFSET: usize = 56;
const SHM_LAYOUT_VERSION_OFFSET: usize = 60;
const SHM_PAYLOAD_OFFSET: usize = 64;

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

#[test]
fn debug_snapshot_helper_returns_expected_batch() {
    let batch = debug_snapshot_record_batch_for_test().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3);

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
fn active_descriptor_reopens_shared_segment_for_record_batch_export() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();

    let descriptor = writer.active_descriptor();
    let span = RowSpanView::from_active_descriptor(descriptor, 1, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

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
fn active_descriptor_envelope_roundtrips_for_cross_process_attach() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(&bytes, schema, layout).unwrap();
    let span = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap();
    let batch = span.as_record_batch().unwrap();

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument.value(0), "rb2501");
}

#[test]
fn active_descriptor_envelope_rejects_schema_id_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let mismatched_schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
    ])
    .unwrap();
    let mismatched_layout = LayoutPlan::for_schema(&mismatched_schema, 4).unwrap();

    let err =
        ActiveSegmentDescriptor::from_envelope_bytes(&bytes, mismatched_schema, mismatched_layout)
            .unwrap_err();

    assert!(err.contains("schema id"));
}

#[test]
fn active_descriptor_envelope_rejects_version_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let changed = text.replace("\"version\":1", "\"version\":0");
    assert_ne!(text, changed);

    let err = ActiveSegmentDescriptor::from_envelope_bytes(changed.as_bytes(), schema, layout)
        .unwrap_err();

    assert!(err.contains("version"));
}

#[test]
fn active_descriptor_envelope_rejects_payload_offset_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema.clone(), layout.clone()).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let bytes = writer.active_descriptor().to_envelope_bytes().unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let changed = text.replace(
        &format!("\"payload_offset\":{SHM_PAYLOAD_OFFSET}"),
        "\"payload_offset\":0",
    );
    assert_ne!(text, changed);

    let err = ActiveSegmentDescriptor::from_envelope_bytes(changed.as_bytes(), schema, layout)
        .unwrap_err();

    assert!(err.contains("payload offset"));
}

#[test]
fn active_descriptor_can_be_reused_after_more_rows_are_committed() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();

    let span = RowSpanView::from_active_descriptor(descriptor, 1, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument.value(0), "rb2505");
}

#[test]
fn active_descriptor_preserves_utf8_offsets_across_null_rows() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::nullable("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 1).unwrap();
    writer.write_utf8("instrument_id", "a").unwrap();
    writer.write_f64("last_price", 1.0).unwrap();
    writer.commit_row().unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 2).unwrap();
    writer.write_f64("last_price", 2.0).unwrap();
    writer.commit_row().unwrap();

    writer.begin_row().unwrap();
    writer.write_i64("dt", 3).unwrap();
    writer.write_utf8("instrument_id", "bb").unwrap();
    writer.write_f64("last_price", 3.0).unwrap();
    writer.commit_row().unwrap();

    let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, 3).unwrap();
    let batch = span.as_record_batch().unwrap();

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument.value(0), "a");
    assert!(instrument.is_null(1));
    assert_eq!(instrument.value(2), "bb");
}

#[test]
fn active_descriptor_rejects_schema_id_header_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor();
    let mut shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    shm.write_at(SHM_SCHEMA_ID_OFFSET, &0_u64.to_ne_bytes())
        .unwrap();

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap_err();

    assert!(err.contains("schema id"));
}

#[test]
fn active_descriptor_rejects_header_magic_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor();
    let mut shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    shm.write_at(SHM_MAGIC_OFFSET, &0_u32.to_ne_bytes())
        .unwrap();

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap_err();

    assert!(err.contains("magic"));
}

#[test]
fn active_descriptor_rejects_layout_version_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor();
    let mut shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    shm.write_at(SHM_LAYOUT_VERSION_OFFSET, &0_u32.to_ne_bytes())
        .unwrap();

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap_err();

    assert!(err.contains("layout version"));
}

#[test]
fn active_descriptor_uses_committed_count_as_authoritative_active_prefix() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    let descriptor = writer.active_descriptor();
    let mut shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    shm.write_at(SHM_ROW_COUNT_OFFSET, &1_u64.to_ne_bytes())
        .unwrap();

    let span = RowSpanView::from_active_descriptor(descriptor, 0, 2).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn active_descriptor_rejects_committed_row_count_exceeding_capacity() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor();
    let shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    shm.store_u64_release(SHM_COMMITTED_ROW_COUNT_OFFSET, 5)
        .unwrap();

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 5).unwrap_err();

    assert!(err.contains("committed row count"));
}

#[test]
fn active_descriptor_rejects_committed_row_count_offset_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer
        .active_descriptor()
        .with_committed_row_count_offset_for_test(SHM_SCHEMA_ID_OFFSET);

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap_err();

    assert!(err.contains("committed row count offset"));
}

#[test]
fn active_descriptor_rejects_payload_offset_mismatch() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = writer.active_descriptor().with_payload_offset_for_test(0);

    let err = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap_err();

    assert!(err.contains("payload offset"));
}
