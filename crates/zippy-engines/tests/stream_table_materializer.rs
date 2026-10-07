use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::{StreamTableMaterializer, DEFAULT_STREAM_TABLE_ROW_CAPACITY};
use zippy_segment_store::{
    compile_schema, ActiveSegmentDescriptor, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
    SegmentStore, SegmentStoreConfig,
};

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Arc::new(Field::new("instrument_id", DataType::Utf8, false)),
        Arc::new(Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("last_price", DataType::Float64, false)),
    ]))
}

fn nullable_frame_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Arc::new(Field::new("instrument_id", DataType::Utf8, false)),
        Arc::new(Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )),
        Arc::new(Field::new("last_price", DataType::Float64, true)),
    ]))
}

fn segment_schema() -> zippy_segment_store::CompiledSchema {
    compile_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap()
}

fn nullable_frame_segment_schema() -> zippy_segment_store::CompiledSchema {
    compile_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::nullable("last_price", ColumnType::Float64),
    ])
    .unwrap()
}

fn input_batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec!["IF2606", "IH2606"])) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    1_710_000_000_000_000_000_i64,
                    1_710_000_000_100_000_000_i64,
                ])
                .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(Float64Array::from(vec![3912.4, 2740.8])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn descriptor_batch_with_schema(
    descriptor: &[u8],
    schema: zippy_segment_store::CompiledSchema,
    row_capacity: usize,
    row_count: usize,
) -> RecordBatch {
    let layout = LayoutPlan::for_schema(&schema, row_capacity).unwrap();
    let descriptor =
        ActiveSegmentDescriptor::from_envelope_bytes(descriptor, schema, layout).unwrap();
    RowSpanView::from_active_descriptor(descriptor, 0, row_count)
        .unwrap()
        .as_record_batch()
        .unwrap()
}

fn descriptor_shm_path(descriptor: &serde_json::Value) -> PathBuf {
    PathBuf::from(
        descriptor["shm_os_id"]
            .as_str()
            .unwrap()
            .strip_prefix("file:")
            .unwrap(),
    )
}

fn input_batch_with_rows(rows: usize) -> RecordBatch {
    let instruments = (0..rows)
        .map(|index| if index % 2 == 0 { "IF2606" } else { "IH2606" })
        .collect::<Vec<_>>();
    let timestamps = (0..rows)
        .map(|index| 1_710_000_000_000_000_000_i64 + index as i64)
        .collect::<Vec<_>>();
    let prices = (0..rows)
        .map(|index| 3912.4 + index as f64)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(instruments)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")) as ArrayRef,
            Arc::new(Float64Array::from(prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn nullable_frame_batch(timestamp_ns: i64) -> RecordBatch {
    RecordBatch::try_new(
        nullable_frame_schema(),
        vec![
            Arc::new(StringArray::from(vec!["IF2606", "IH2606"])) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![timestamp_ns, timestamp_ns])
                    .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(3912.4), None])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn segment_table_view_with_rows(rows: usize) -> SegmentTableView {
    let store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: rows.max(1),
    })
    .unwrap();
    let handle = store
        .open_partition_with_schema("ticks", "source", segment_schema())
        .unwrap();
    let writer = handle.writer();
    for row_index in 0..rows {
        let instrument_id = if row_index % 2 == 0 {
            "IF2606"
        } else {
            "IH2606"
        };
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", instrument_id)?;
                row.write_i64("dt", 1_710_000_000_000_000_000_i64 + row_index as i64)?;
                row.write_f64("last_price", 3912.4 + row_index as f64)?;
                Ok(())
            })
            .unwrap();
    }
    SegmentTableView::from_row_span(handle.active_row_span(0, rows).unwrap())
}

#[test]
fn stream_table_on_data_passes_batch_through_without_schema_change() {
    let mut engine = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    let batch = input_batch();
    let view = SegmentTableView::from_record_batch(batch.clone());

    let outputs = engine.on_data(view).unwrap();

    assert_eq!(engine.output_schema(), batch.schema());
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].schema(), batch.schema());
    assert_eq!(outputs[0].num_rows(), batch.num_rows());
    assert_eq!(
        outputs[0].column("instrument_id").unwrap().to_data(),
        batch.column(0).to_data()
    );
    assert_eq!(
        outputs[0].column("dt").unwrap().to_data(),
        batch.column(1).to_data()
    );
    assert_eq!(
        outputs[0].column("last_price").unwrap().to_data(),
        batch.column(2).to_data()
    );
}

#[test]
fn stream_table_materializer_writes_batch_into_active_segment() {
    let mut materializer = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    let batch = input_batch();

    let outputs = materializer
        .on_data(SegmentTableView::from_record_batch(batch.clone()))
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(materializer.active_committed_row_count(), batch.num_rows());

    let active = materializer.active_record_batch().unwrap();
    assert_eq!(active.schema(), batch.schema());
    assert_eq!(active.num_rows(), batch.num_rows());
    assert_eq!(
        active.column(0).to_data(),
        batch.column(0).to_data(),
        "instrument_id should be materialized into active segment"
    );
    assert_eq!(
        active.column(1).to_data(),
        batch.column(1).to_data(),
        "dt should be materialized into active segment"
    );
    assert_eq!(
        active.column(2).to_data(),
        batch.column(2).to_data(),
        "last_price should be materialized into active segment"
    );
}

#[test]
fn stream_table_materializer_writes_segment_view_into_active_segment() {
    let mut materializer = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    let view = segment_table_view_with_rows(2);

    let outputs = materializer.on_data(view).unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(materializer.active_committed_row_count(), 2);

    let active = materializer.active_record_batch().unwrap();
    let instrument_ids = active
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let timestamps = active
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let prices = active
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(active.num_rows(), 2);
    assert_eq!(instrument_ids.value(0), "IF2606");
    assert_eq!(instrument_ids.value(1), "IH2606");
    assert_eq!(timestamps.value(0), 1_710_000_000_000_000_000_i64);
    assert_eq!(timestamps.value(1), 1_710_000_000_000_000_001_i64);
    assert_eq!(prices.value(0), 3912.4);
    assert_eq!(prices.value(1), 3913.4);
}

#[test]
fn stream_table_materializer_exports_active_descriptor_envelope() {
    let mut materializer = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch()))
        .unwrap();

    let envelope = materializer.active_descriptor_envelope_bytes().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(&envelope).unwrap();

    assert_eq!(descriptor["magic"], "zippy.segment.active");
    assert_eq!(descriptor["version"], 2);
    assert_eq!(descriptor["segment_id"], 1);
    assert_eq!(descriptor["generation"], 0);
    assert!(descriptor["schema_id"].as_u64().unwrap() > 0);
    assert_eq!(materializer.active_committed_row_count(), 2);
}

#[test]
fn stream_table_default_row_capacity_is_large_enough_for_live_ingest() {
    let materializer = StreamTableMaterializer::new("ticks", input_schema()).unwrap();

    let envelope = materializer.active_descriptor_envelope_bytes().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(&envelope).unwrap();
    let row_capacity = descriptor["row_capacity"].as_u64().unwrap() as usize;

    assert_eq!(row_capacity, DEFAULT_STREAM_TABLE_ROW_CAPACITY);
    assert!(row_capacity >= 65_536);
}

#[test]
fn stream_table_materializer_rolls_over_full_active_segment_and_exports_descriptor() {
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 32).unwrap();
    let batch = input_batch_with_rows(34);

    let outputs = materializer
        .on_data(SegmentTableView::from_record_batch(batch.clone()))
        .unwrap();

    assert_eq!(outputs[0].num_rows(), 34);
    assert_eq!(materializer.active_committed_row_count(), 2);

    let descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    assert_eq!(descriptor["segment_id"], 2);
    assert_eq!(descriptor["generation"], 1);
}

#[test]
fn stream_table_frame_per_batch_seals_each_nullable_batch_as_one_segment() {
    let mut materializer = StreamTableMaterializer::new_with_row_capacity(
        "cross_section_frames",
        nullable_frame_schema(),
        4,
    )
    .unwrap()
    .with_frame_per_batch(true);

    for timestamp_ns in [1_710_000_000_000_000_000_i64, 1_710_000_060_000_000_000_i64] {
        let batch = nullable_frame_batch(timestamp_ns);
        let outputs = materializer
            .on_data(SegmentTableView::from_record_batch(batch))
            .unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].num_rows(), 2);
        assert_eq!(materializer.active_committed_row_count(), 0);
    }

    let latest: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    assert_eq!(latest["segment_id"], 3);
    let sealed = latest["sealed_segments"].as_array().unwrap();
    assert_eq!(sealed.len(), 2);
    for descriptor in sealed {
        let envelope = serde_json::to_vec(descriptor).unwrap();
        let batch = descriptor_batch_with_schema(&envelope, nullable_frame_segment_schema(), 4, 2);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(2).null_count(), 1);
    }
}

#[test]
fn stream_table_frame_per_batch_rejects_oversized_frame_without_partial_commit() {
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("cross_section_frames", input_schema(), 1)
            .unwrap()
            .with_frame_per_batch(true);

    let error = materializer
        .on_data(SegmentTableView::from_record_batch(input_batch()))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("frame rows exceed segment capacity"));
    assert_eq!(materializer.active_committed_row_count(), 0);
    assert!(materializer.live_segments().unwrap().is_empty());
}

#[test]
fn stream_table_materializer_rolls_over_segment_view_input() {
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2).unwrap();

    materializer
        .on_data(segment_table_view_with_rows(5))
        .unwrap();

    assert_eq!(materializer.active_committed_row_count(), 1);
    let active = materializer.active_record_batch().unwrap();
    let instrument_ids = active
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let prices = active
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(instrument_ids.value(0), "IF2606");
    assert_eq!(prices.value(0), 3916.4);

    let descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    assert_eq!(descriptor["segment_id"], 3);
    assert_eq!(descriptor["generation"], 2);
    assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 2);
}

#[test]
fn stream_table_descriptor_after_rollover_includes_retained_sealed_segment() {
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 32).unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            34,
        )))
        .unwrap();

    let descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    let sealed_segments = descriptor["sealed_segments"].as_array().unwrap();

    assert_eq!(sealed_segments.len(), 1);
    assert_eq!(sealed_segments[0]["magic"], "zippy.segment.active");
    assert_eq!(sealed_segments[0]["segment_id"], 1);
    assert_eq!(sealed_segments[0]["generation"], 0);
    assert_eq!(descriptor["segment_id"], 2);
    assert_eq!(descriptor["generation"], 1);
}

#[test]
fn stream_table_flush_and_stop_emit_no_extra_batches() {
    let mut engine = StreamTableMaterializer::new("ticks", input_schema()).unwrap();

    assert!(engine.on_flush().unwrap().is_empty());
    assert!(engine.on_stop().unwrap().is_empty());
}

#[test]
fn stream_table_on_data_passes_empty_batch_through_without_modification() {
    let mut engine = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    let batch = RecordBatch::new_empty(input_schema());
    let view = SegmentTableView::from_record_batch(batch.clone());

    let outputs = engine.on_data(view).unwrap();

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].schema(), batch.schema());
    assert_eq!(outputs[0].num_rows(), 0);
    assert_eq!(engine.active_committed_row_count(), 0);
    assert_eq!(
        outputs[0].column("instrument_id").unwrap().to_data(),
        batch.column(0).to_data()
    );
    assert_eq!(
        outputs[0].column("dt").unwrap().to_data(),
        batch.column(1).to_data()
    );
    assert_eq!(
        outputs[0].column("last_price").unwrap().to_data(),
        batch.column(2).to_data()
    );
}

#[test]
fn stream_table_rejects_schema_mismatch() {
    let mut engine = StreamTableMaterializer::new("ticks", input_schema()).unwrap();
    let wrong_schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("instrument_id", DataType::Utf8, false)),
        Arc::new(Field::new("last_price", DataType::Float64, false)),
    ]));
    let batch = RecordBatch::try_new(
        wrong_schema,
        vec![
            Arc::new(StringArray::from(vec!["IF2606"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![3912.4])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = engine
        .on_data(SegmentTableView::from_record_batch(batch))
        .unwrap_err();

    assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
    assert!(error.to_string().contains("stream table input schema"));
}

#[test]
fn native_durable_prefix_waits_for_pinned_reader_before_reclaiming_segment() {
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("durable_ticks", input_schema(), 2).unwrap();
    let descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    let path = descriptor_shm_path(&descriptor);
    let lease = materializer.pin_active_segment_for_test().unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            5,
        )))
        .unwrap();
    assert_eq!(materializer.retire_persisted_prefix(4).unwrap(), 0);
    assert_eq!(materializer.live_segment_count().unwrap(), 3);
    assert!(path.exists());
    drop(lease);
    assert_eq!(materializer.retire_persisted_prefix(3).unwrap(), 2);
    assert_eq!(materializer.live_segment_count().unwrap(), 2);
    assert!(!path.exists());
    assert_eq!(materializer.retire_persisted_prefix(2).unwrap(), 2);
    assert_eq!(materializer.active_record_batch().unwrap().num_rows(), 1);
}
