use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use zippy_segment_store::{
    compile_schema, ActiveSegmentDescriptor, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
    SegmentStore, SegmentStoreConfig, ShmRegion, ZippySegmentStoreError,
};

const SHM_SEALED_OFFSET: usize = 48;

#[test]
fn open_partition_with_schema_supports_column_writes_and_debug_snapshot() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Int64),
    ])
    .unwrap();

    let handle = store
        .open_partition_with_schema("ticks", "rb2501", schema)
        .unwrap();
    let writer = handle.writer();

    writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_utf8("instrument_id", "rb2501")?;
            row.write_f64("last_price", 4123.5)?;
            row.write_i64("volume", 7)?;
            Ok(())
        })
        .unwrap();

    let batch = writer.debug_snapshot_record_batch().unwrap();
    let partition_batch = handle.debug_snapshot_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(partition_batch.num_rows(), 1);
    assert_eq!(partition_batch.num_columns(), 4);

    let dt = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(dt.value(0), 1);

    let instrument = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(instrument.value(0), "rb2501");

    let last_price = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(last_price.value(0), 4123.5);

    let volume = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(volume.value(0), 7);
}

#[test]
fn partition_writer_write_row_aborts_failed_transaction() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    let err = writer
        .write_row(|row| {
            row.write_utf8("instrument_id", "bad")?;
            Err(ZippySegmentStoreError::Writer("forced failure"))
        })
        .unwrap_err();

    assert!(matches!(err, ZippySegmentStoreError::Writer(_)));

    writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_utf8("instrument_id", "ok")?;
            row.write_f64("last_price", 1.0)?;
            Ok(())
        })
        .unwrap();

    let batch = writer.debug_snapshot_record_batch().unwrap();
    let instrument = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(instrument.value(0), "ok");
}

#[test]
fn partition_writer_appends_row_span_without_arrow_batch_bridge() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Int64),
    ])
    .unwrap();
    let source_store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let source_handle = source_store
        .open_partition_with_schema("ticks", "source", schema.clone())
        .unwrap();
    let source_writer = source_handle.writer();
    source_writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_utf8("instrument_id", "rb2501")?;
            row.write_f64("last_price", 4123.5)?;
            row.write_i64("volume", 7)?;
            Ok(())
        })
        .unwrap();
    source_writer
        .write_row(|row| {
            row.write_i64("dt", 2)?;
            row.write_utf8("instrument_id", "rb2502")?;
            row.write_f64("last_price", 4124.5)?;
            row.write_i64("volume", 11)?;
            Ok(())
        })
        .unwrap();
    let span = source_handle.active_row_span(0, 2).unwrap();

    let target_store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let target_handle = target_store
        .open_partition_with_schema("ticks", "target", schema)
        .unwrap();
    let copied = target_handle
        .writer()
        .append_row_span(&span, 0, span.row_count())
        .unwrap();

    assert_eq!(copied, 2);
    let batch = target_handle.debug_snapshot_record_batch().unwrap();
    let dt = batch
        .column_by_name("dt")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let instrument = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_price = batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let volume = batch
        .column_by_name("volume")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(dt.value(0), 1);
    assert_eq!(dt.value(1), 2);
    assert_eq!(instrument.value(0), "rb2501");
    assert_eq!(instrument.value(1), "rb2502");
    assert_eq!(last_price.value(0), 4123.5);
    assert_eq!(last_price.value(1), 4124.5);
    assert_eq!(volume.value(0), 7);
    assert_eq!(volume.value(1), 11);
}

#[test]
fn partition_writer_writes_columnar_rows_in_one_committed_prefix() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("exchange_id", ColumnType::Utf8),
        ColumnSpec::nullable("last_price", ColumnType::Float64),
        ColumnSpec::nullable("volume", ColumnType::Int64),
    ])
    .unwrap();
    let store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: 8,
    })
    .unwrap();
    let handle = store
        .open_partition_with_schema("ticks", "columnar", schema)
        .unwrap();
    let writer = handle.writer();

    let written = writer
        .write_columnar_rows(3, |columns, rows| {
            assert_eq!(rows, 3);
            columns.write_i64_values("dt", &[1, 2, 3])?;
            columns.write_utf8_values("instrument_id", &["IF2606", "IF2607", "IF2608"])?;
            columns.write_utf8_repeated("exchange_id", "CFFEX", rows)?;
            columns.write_f64_values("last_price", &[4112.5, 4113.5, 4114.5])?;
            columns.write_i64_values("volume", &[7, 11, 13])?;
            Ok(())
        })
        .unwrap();

    assert_eq!(written, 3);
    let batch = writer.debug_snapshot_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 3);

    let dt = batch
        .column_by_name("dt")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let instrument = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let exchange = batch
        .column_by_name("exchange_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_price = batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let volume = batch
        .column_by_name("volume")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(dt.value(0), 1);
    assert_eq!(dt.value(2), 3);
    assert_eq!(instrument.value(0), "IF2606");
    assert_eq!(instrument.value(2), "IF2608");
    assert_eq!(exchange.value(0), "CFFEX");
    assert_eq!(exchange.value(2), "CFFEX");
    assert!(exchange.is_valid(0));
    assert!(last_price.is_valid(1));
    assert_eq!(last_price.value(1), 4113.5);
    assert_eq!(volume.value(2), 13);
}

#[test]
fn active_row_span_exposes_borrowed_utf8_cell_values_for_hot_filters() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store
        .open_partition_with_schema("ticks", "source", schema)
        .unwrap();
    let writer = handle.writer();
    writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_utf8("instrument_id", "rb2501")?;
            row.write_f64("last_price", 4123.5)?;
            Ok(())
        })
        .unwrap();
    writer
        .write_row(|row| {
            row.write_i64("dt", 2)?;
            row.write_utf8("instrument_id", "rb2502")?;
            row.write_f64("last_price", 4124.5)?;
            Ok(())
        })
        .unwrap();

    let span = handle.active_row_span(0, 2).unwrap();

    assert_eq!(
        span.utf8_cell_value(0, "instrument_id").unwrap(),
        Some("rb2501")
    );
    assert_eq!(
        span.utf8_cell_value(1, "instrument_id").unwrap(),
        Some("rb2502")
    );
    assert!(matches!(
        span.utf8_cell_value(0, "missing").unwrap_err(),
        ZippySegmentStoreError::Schema("missing column")
    ));
}

#[test]
fn partition_writer_appends_active_row_span_with_nullable_columns_and_offset() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::nullable("exchange_id", ColumnType::Utf8),
        ColumnSpec::nullable("last_price", ColumnType::Float64),
        ColumnSpec::nullable("volume", ColumnType::Int64),
    ])
    .unwrap();
    let source_store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: 8,
    })
    .unwrap();
    let source_handle = source_store
        .open_partition_with_schema("ticks", "source", schema.clone())
        .unwrap();
    let source_writer = source_handle.writer();
    source_writer
        .write_row(|row| {
            row.write_i64("dt", 1)?;
            row.write_utf8("instrument_id", "skip")?;
            row.write_utf8("exchange_id", "SHFE")?;
            row.write_f64("last_price", 1.0)?;
            row.write_i64("volume", 1)?;
            Ok(())
        })
        .unwrap();
    source_writer
        .write_row(|row| {
            row.write_i64("dt", 2)?;
            row.write_utf8("instrument_id", "rb2501")?;
            row.write_f64("last_price", 4123.5)?;
            Ok(())
        })
        .unwrap();
    source_writer
        .write_row(|row| {
            row.write_i64("dt", 3)?;
            row.write_utf8("instrument_id", "rb2502")?;
            row.write_utf8("exchange_id", "SHFE")?;
            row.write_i64("volume", 11)?;
            Ok(())
        })
        .unwrap();
    let span = source_handle.active_row_span(0, 3).unwrap();

    let target_store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: 8,
    })
    .unwrap();
    let target_handle = target_store
        .open_partition_with_schema("ticks", "target", schema)
        .unwrap();
    target_handle
        .writer()
        .write_row(|row| {
            row.write_i64("dt", 0)?;
            row.write_utf8("instrument_id", "prefix")?;
            row.write_utf8("exchange_id", "DCE")?;
            row.write_f64("last_price", 0.5)?;
            row.write_i64("volume", 99)?;
            Ok(())
        })
        .unwrap();
    let copied = target_handle.writer().append_row_span(&span, 1, 2).unwrap();

    assert_eq!(copied, 2);
    let batch = target_handle.debug_snapshot_record_batch().unwrap();
    let dt = batch
        .column_by_name("dt")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let instrument = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let exchange = batch
        .column_by_name("exchange_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_price = batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let volume = batch
        .column_by_name("volume")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(dt.value(0), 0);
    assert_eq!(dt.value(1), 2);
    assert_eq!(dt.value(2), 3);
    assert_eq!(instrument.value(0), "prefix");
    assert_eq!(instrument.value(1), "rb2501");
    assert_eq!(instrument.value(2), "rb2502");
    assert_eq!(exchange.value(0), "DCE");
    assert!(exchange.is_null(1));
    assert_eq!(exchange.value(2), "SHFE");
    assert_eq!(last_price.value(0), 0.5);
    assert_eq!(last_price.value(1), 4123.5);
    assert!(last_price.is_null(2));
    assert_eq!(volume.value(0), 99);
    assert!(volume.is_null(1));
    assert_eq!(volume.value(2), 11);
}

#[test]
fn partition_writer_writes_multiple_rows_in_one_transaction() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    let written = writer
        .write_rows(2, |row, index| {
            row.write_i64("dt", 1 + index as i64)?;
            row.write_utf8(
                "instrument_id",
                if index == 0 { "rb2501" } else { "rb2502" },
            )?;
            row.write_f64("last_price", 4123.5 + index as f64)?;
            Ok(())
        })
        .unwrap();

    assert_eq!(written, 2);
    let batch = writer.debug_snapshot_record_batch().unwrap();
    let instrument = batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_price = batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(instrument.value(0), "rb2501");
    assert_eq!(instrument.value(1), "rb2502");
    assert_eq!(last_price.value(0), 4123.5);
    assert_eq!(last_price.value(1), 4124.5);
}

#[test]
fn active_descriptor_can_attach_after_rollover_before_garbage_collection() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = handle.active_descriptor_for_test();

    writer.rollover().unwrap();

    let span = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn partition_handle_exports_active_descriptor_envelope_for_cross_process_reader() {
    let config = SegmentStoreConfig::for_test();
    let row_capacity = config.default_row_capacity;
    let store = SegmentStore::new(config).unwrap();
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, row_capacity).unwrap();
    let handle = store
        .open_partition_with_schema("ticks", "rb2501", schema.clone())
        .unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();

    let bytes = handle.active_descriptor_envelope_bytes().unwrap();
    let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(&bytes, schema, layout).unwrap();
    let span = RowSpanView::from_active_descriptor(descriptor, 0, 1).unwrap();
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn rollover_marks_old_shared_segment_as_sealed() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    let descriptor = handle.active_descriptor_for_test();

    writer.rollover().unwrap();

    let shm = ShmRegion::open(descriptor.shm_os_id()).unwrap();
    let mut sealed = [0_u8; 1];
    shm.read_at(SHM_SEALED_OFFSET, &mut sealed).unwrap();

    assert_eq!(sealed[0], 1);
}

#[test]
fn reopening_partition_requires_matching_schema() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
    ])
    .unwrap();
    let different_schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();

    store
        .open_partition_with_schema("ticks", "rb2501", schema)
        .unwrap();

    let err = store
        .open_partition_with_schema("ticks", "rb2501", different_schema)
        .unwrap_err();

    assert!(matches!(
        err,
        zippy_segment_store::ZippySegmentStoreError::Schema(_)
    ));
}
