use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{sync::mpsc, thread};

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::{
    KeyValueTableMaterializer, StreamTableDescriptorPublisher, StreamTableEngine,
    StreamTableMaterializer, StreamTablePersistConfig, StreamTablePersistPartitionSpec,
    StreamTablePersistPublisher, StreamTableRetentionGuard, DEFAULT_STREAM_TABLE_ROW_CAPACITY,
};
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

fn segment_schema() -> zippy_segment_store::CompiledSchema {
    compile_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("last_price", ColumnType::Float64),
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

fn descriptor_batch(descriptor: &[u8], row_capacity: usize, row_count: usize) -> RecordBatch {
    let schema = segment_schema();
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

fn latest_update_batch(instrument_ids: Vec<&str>, last_prices: Vec<f64>) -> RecordBatch {
    let timestamps = (0..instrument_ids.len())
        .map(|index| 1_710_000_000_000_000_000_i64 + index as i64)
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")) as ArrayRef,
            Arc::new(Float64Array::from(last_prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

fn partitioned_input_batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec!["A/B", "A/B", "IF2606", "IF2606"])) as ArrayRef,
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    1_776_211_200_000_000_000_i64,
                    1_776_211_200_000_000_001_i64,
                    1_776_211_200_000_000_002_i64,
                    1_777_593_600_000_000_000_i64,
                ])
                .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(Float64Array::from(vec![3912.4, 3913.4, 2740.8, 2741.8])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn empty_input_batch() -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone("UTC"))
                as ArrayRef,
            Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn stream_table_on_data_passes_batch_through_without_schema_change() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
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
fn key_value_table_materializer_replaces_active_snapshot_by_key() {
    let mut materializer =
        KeyValueTableMaterializer::new("ticks_latest", input_schema(), vec!["instrument_id"])
            .unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606", "IH2606"],
            vec![4102.5, 2711.0],
        )))
        .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606"],
            vec![4103.5],
        )))
        .unwrap();

    let active = materializer.active_record_batch().unwrap();
    let instrument_ids = active
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_prices = active
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(active.num_rows(), 2);
    assert_eq!(instrument_ids.value(0), "IF2606");
    assert_eq!(last_prices.value(0), 4103.5);
    assert_eq!(instrument_ids.value(1), "IH2606");
    assert_eq!(last_prices.value(1), 2711.0);
}

#[test]
fn key_value_table_materializer_publishes_new_segment_for_snapshot_replace() {
    let mut materializer = KeyValueTableMaterializer::new_with_row_capacity(
        "ticks_latest",
        input_schema(),
        vec!["instrument_id"],
        8,
    )
    .unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606", "IH2606"],
            vec![4102.5, 2711.0],
        )))
        .unwrap();
    let first_descriptor = materializer.active_descriptor_envelope_bytes().unwrap();
    let first_descriptor_value: serde_json::Value =
        serde_json::from_slice(&first_descriptor).unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606"],
            vec![4103.5],
        )))
        .unwrap();
    let second_descriptor = materializer.active_descriptor_envelope_bytes().unwrap();
    let second_descriptor_value: serde_json::Value =
        serde_json::from_slice(&second_descriptor).unwrap();
    let old_snapshot = descriptor_batch(&first_descriptor, 8, 2);
    let old_prices = old_snapshot
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_ne!(
        first_descriptor_value["segment_id"],
        second_descriptor_value["segment_id"]
    );
    assert_ne!(
        first_descriptor_value["shm_os_id"],
        second_descriptor_value["shm_os_id"]
    );
    assert_eq!(old_prices.value(0), 4102.5);
}

#[test]
fn key_value_table_materializer_retains_and_releases_all_segments_from_old_snapshot() {
    let mut materializer = KeyValueTableMaterializer::new_with_row_capacity(
        "ticks_latest",
        input_schema(),
        vec!["instrument_id"],
        1,
    )
    .unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606", "IH2606"],
            vec![4102.5, 2711.0],
        )))
        .unwrap();
    let first_descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    let first_sealed_descriptor = first_descriptor["sealed_segments"]
        .as_array()
        .unwrap()
        .first()
        .unwrap()
        .clone();
    let first_sealed_path = descriptor_shm_path(&first_sealed_descriptor);
    let first_active_path = descriptor_shm_path(&first_descriptor);

    for update_index in 0..10 {
        materializer
            .on_data(SegmentTableView::from_record_batch(latest_update_batch(
                vec!["IF2606", "IH2606"],
                vec![4103.5 + update_index as f64, 2712.0 + update_index as f64],
            )))
            .unwrap();
    }

    assert!(
        !first_active_path.exists(),
        "old active snapshot segment should be released path=[{}]",
        first_active_path.display()
    );
    assert!(
        !first_sealed_path.exists(),
        "old sealed snapshot segment should be released path=[{}]",
        first_sealed_path.display()
    );
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

#[derive(Default)]
struct RecordingDescriptorPublisher {
    envelopes: Mutex<Vec<Vec<u8>>>,
}

impl StreamTableDescriptorPublisher for RecordingDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> zippy_core::Result<()> {
        self.envelopes.lock().unwrap().push(descriptor_envelope);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingPersistPublisher {
    files: Mutex<Vec<serde_json::Value>>,
}

impl StreamTablePersistPublisher for RecordingPersistPublisher {
    fn publish(&self, persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        self.files.lock().unwrap().push(persisted_file);
        Ok(())
    }
}

struct FailingPersistPublisher;

impl StreamTablePersistPublisher for FailingPersistPublisher {
    fn publish(&self, _persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        Err(ZippyError::Io {
            reason: "publisher failed".to_string(),
        })
    }
}

#[derive(Default)]
struct ToggleRetentionGuard {
    releasable: Mutex<bool>,
}

impl ToggleRetentionGuard {
    fn set_releasable(&self, releasable: bool) {
        *self.releasable.lock().unwrap() = releasable;
    }
}

impl StreamTableRetentionGuard for ToggleRetentionGuard {
    fn can_release(&self, _segment_id: u64, _generation: u64) -> zippy_core::Result<bool> {
        Ok(*self.releasable.lock().unwrap())
    }
}

#[derive(Default)]
struct FailingPersistEventPublisher {
    events: Mutex<Vec<serde_json::Value>>,
}

impl StreamTablePersistPublisher for FailingPersistEventPublisher {
    fn publish(&self, _persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        Err(ZippyError::Io {
            reason: "publisher failed".to_string(),
        })
    }

    fn publish_event(&self, persist_event: serde_json::Value) -> zippy_core::Result<()> {
        self.events.lock().unwrap().push(persist_event);
        Ok(())
    }
}

struct FlakyPersistPublisher {
    remaining_failures: Mutex<usize>,
    files: Mutex<Vec<serde_json::Value>>,
}

impl FlakyPersistPublisher {
    fn new(remaining_failures: usize) -> Self {
        Self {
            remaining_failures: Mutex::new(remaining_failures),
            files: Mutex::new(Vec::new()),
        }
    }
}

impl StreamTablePersistPublisher for FlakyPersistPublisher {
    fn publish(&self, persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        let mut remaining_failures = self.remaining_failures.lock().unwrap();
        if *remaining_failures > 0 {
            *remaining_failures -= 1;
            return Err(ZippyError::Io {
                reason: "transient publisher failure".to_string(),
            });
        }
        drop(remaining_failures);
        self.files.lock().unwrap().push(persisted_file);
        Ok(())
    }
}

#[derive(Default)]
struct BlockingPersistPublisher {
    state: Mutex<BlockingPersistPublisherState>,
    changed: Condvar,
}

#[derive(Default)]
struct BlockingPersistPublisherState {
    entered: bool,
    released: bool,
}

impl BlockingPersistPublisher {
    fn wait_until_entered(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let result = self.changed.wait_timeout(state, remaining).unwrap();
            state = result.0;
        }
        true
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.changed.notify_all();
    }
}

impl StreamTablePersistPublisher for BlockingPersistPublisher {
    fn publish(&self, _persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
        Ok(())
    }
}

fn temp_persist_root(name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "zippy-stream-table-persist-{name}-{}-{suffix}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    root
}

fn wait_for_persist_failure(materializer: &StreamTableMaterializer) -> Vec<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let failures = materializer.persist_failures_for_test();
        if !failures.is_empty() {
            return failures;
        }
        if std::time::Instant::now() >= deadline {
            return failures;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn stream_table_persist_commit_snapshot_tracks_pending_writing_and_committed_segments() {
    let persist_root = temp_persist_root("commit-state");
    let persist_publisher = Arc::new(BlockingPersistPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_parquet_persist(StreamTablePersistConfig::new(&persist_root))
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            5,
        )))
        .unwrap();

    assert!(persist_publisher.wait_until_entered(Duration::from_secs(2)));
    let mut statuses = materializer
        .persist_commit_snapshot()
        .into_iter()
        .map(|entry| {
            (
                entry["source_segment_id"].as_u64().unwrap(),
                entry["source_generation"].as_u64().unwrap(),
                entry["attempts"].as_u64().unwrap(),
                entry["status"].as_str().unwrap().to_string(),
            )
        })
        .collect::<Vec<_>>();
    statuses.sort();
    assert_eq!(
        statuses,
        vec![
            (1, 0, 1, "writing".to_string()),
            (2, 1, 0, "pending".to_string()),
        ]
    );

    persist_publisher.release();
    materializer
        .wait_for_persisted_files_for_test(2, Duration::from_secs(2))
        .unwrap();
    let mut statuses = materializer
        .persist_commit_snapshot()
        .into_iter()
        .map(|entry| {
            (
                entry["source_segment_id"].as_u64().unwrap(),
                entry["source_generation"].as_u64().unwrap(),
                entry["attempts"].as_u64().unwrap(),
                entry["status"].as_str().unwrap().to_string(),
                entry["file_count"].as_u64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    statuses.sort();
    assert_eq!(
        statuses,
        vec![
            (1, 0, 1, "committed".to_string(), 1),
            (2, 1, 1, "committed".to_string(), 1),
        ]
    );

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_materializer_rolls_over_full_active_segment_and_publishes_descriptor() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 32)
            .unwrap()
            .with_descriptor_publisher(publisher.clone());
    let batch = input_batch_with_rows(34);

    let outputs = materializer
        .on_data(SegmentTableView::from_record_batch(batch.clone()))
        .unwrap();

    assert_eq!(outputs[0].num_rows(), 34);
    assert_eq!(materializer.active_committed_row_count(), 2);

    let envelopes = publisher.envelopes.lock().unwrap();
    assert_eq!(envelopes.len(), 1);
    let descriptor: serde_json::Value = serde_json::from_slice(&envelopes[0]).unwrap();
    assert_eq!(descriptor["segment_id"], 2);
    assert_eq!(descriptor["generation"], 1);
}

#[test]
fn stream_table_materializer_rolls_over_segment_view_input() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_descriptor_publisher(publisher.clone());

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

    let envelopes = publisher.envelopes.lock().unwrap();
    assert_eq!(envelopes.len(), 2);
    let descriptor: serde_json::Value = serde_json::from_slice(envelopes.last().unwrap()).unwrap();
    assert_eq!(descriptor["segment_id"], 3);
    assert_eq!(descriptor["generation"], 2);
    assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 2);
}

#[test]
fn stream_table_descriptor_published_after_rollover_includes_retained_sealed_segment() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 32)
            .unwrap()
            .with_descriptor_publisher(publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            34,
        )))
        .unwrap();

    let envelopes = publisher.envelopes.lock().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(&envelopes[0]).unwrap();
    let sealed_segments = descriptor["sealed_segments"].as_array().unwrap();

    assert_eq!(sealed_segments.len(), 1);
    assert_eq!(sealed_segments[0]["magic"], "zippy.segment.active");
    assert_eq!(sealed_segments[0]["segment_id"], 1);
    assert_eq!(sealed_segments[0]["generation"], 0);
    assert_eq!(descriptor["segment_id"], 2);
    assert_eq!(descriptor["generation"], 1);
}

#[test]
fn stream_table_retention_segments_limits_published_sealed_segments_without_persist() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_retention_segments(1)
            .with_descriptor_publisher(publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            7,
        )))
        .unwrap();

    let envelopes = publisher.envelopes.lock().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(envelopes.last().unwrap()).unwrap();
    let sealed_segments = descriptor["sealed_segments"].as_array().unwrap();

    assert_eq!(sealed_segments.len(), 1);
    assert_eq!(sealed_segments[0]["segment_id"], 3);
    assert_eq!(sealed_segments[0]["generation"], 2);
    assert_eq!(descriptor["segment_id"], 4);
}

#[test]
fn stream_table_retention_waits_for_reader_lease_before_collecting_mmap() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_retention_segments(0)
            .with_descriptor_publisher(publisher.clone());
    let active_descriptor: serde_json::Value =
        serde_json::from_slice(&materializer.active_descriptor_envelope_bytes().unwrap()).unwrap();
    let old_mmap_path = PathBuf::from(
        active_descriptor["shm_os_id"]
            .as_str()
            .unwrap()
            .strip_prefix("file:")
            .unwrap(),
    );
    let lease = materializer.pin_active_segment_for_test().unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            3,
        )))
        .unwrap();
    materializer.on_flush().unwrap();

    {
        let envelopes = publisher.envelopes.lock().unwrap();
        let descriptor: serde_json::Value =
            serde_json::from_slice(envelopes.last().unwrap()).unwrap();
        assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 1);
    }
    assert!(
        old_mmap_path.exists(),
        "pinned mmap should stay available path=[{}]",
        old_mmap_path.display()
    );

    drop(lease);
    materializer.on_flush().unwrap();

    let envelopes = publisher.envelopes.lock().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(envelopes.last().unwrap()).unwrap();
    assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 0);
    assert!(
        !old_mmap_path.exists(),
        "unpinned retained mmap should be collected path=[{}]",
        old_mmap_path.display()
    );
}

#[test]
fn stream_table_retention_guard_blocks_release_until_external_lease_is_gone() {
    let publisher = Arc::new(RecordingDescriptorPublisher::default());
    let retention_guard = Arc::new(ToggleRetentionGuard::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_retention_segments(0)
            .with_descriptor_publisher(publisher.clone())
            .with_retention_guard(retention_guard.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            3,
        )))
        .unwrap();
    materializer.on_flush().unwrap();

    {
        let envelopes = publisher.envelopes.lock().unwrap();
        let descriptor: serde_json::Value =
            serde_json::from_slice(envelopes.last().unwrap()).unwrap();
        assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 1);
    }

    retention_guard.set_releasable(true);
    materializer.on_flush().unwrap();

    let envelopes = publisher.envelopes.lock().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(envelopes.last().unwrap()).unwrap();
    assert_eq!(descriptor["sealed_segments"].as_array().unwrap().len(), 0);
}

#[test]
fn stream_table_retention_waits_for_persist_commit_before_trimming_sealed_segments() {
    let persist_root = temp_persist_root("retention-commit");
    let descriptor_publisher = Arc::new(RecordingDescriptorPublisher::default());
    let persist_publisher = Arc::new(BlockingPersistPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_retention_segments(1)
            .with_descriptor_publisher(descriptor_publisher.clone())
            .with_parquet_persist(StreamTablePersistConfig::new(&persist_root))
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            5,
        )))
        .unwrap();

    {
        let envelopes = descriptor_publisher.envelopes.lock().unwrap();
        let descriptor: serde_json::Value =
            serde_json::from_slice(envelopes.last().unwrap()).unwrap();
        let sealed_segments = descriptor["sealed_segments"].as_array().unwrap();
        assert_eq!(sealed_segments.len(), 2);
        assert_eq!(sealed_segments[0]["segment_id"], 1);
        assert_eq!(sealed_segments[1]["segment_id"], 2);
    }

    persist_publisher.release();
    materializer
        .wait_for_persisted_files_for_test(2, Duration::from_secs(2))
        .unwrap();
    materializer.on_flush().unwrap();

    let envelopes = descriptor_publisher.envelopes.lock().unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(envelopes.last().unwrap()).unwrap();
    let sealed_segments = descriptor["sealed_segments"].as_array().unwrap();
    assert_eq!(sealed_segments.len(), 1);
    assert_eq!(sealed_segments[0]["segment_id"], 2);

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_materializer_persists_sealed_segment_and_publishes_metadata() {
    let persist_root = temp_persist_root("metadata");
    let persist_publisher = Arc::new(RecordingPersistPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 32)
            .unwrap()
            .with_parquet_persist(StreamTablePersistConfig::new(&persist_root))
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            34,
        )))
        .unwrap();

    materializer
        .wait_for_persisted_files_for_test(1, Duration::from_secs(2))
        .unwrap();

    let files = persist_publisher.files.lock().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["stream_name"], "ticks");
    assert_eq!(files[0]["row_count"], 32);
    assert_eq!(files[0]["source_segment_id"], 1);
    assert_eq!(files[0]["source_generation"], 0);

    let file_path = PathBuf::from(files[0]["file_path"].as_str().unwrap());
    assert!(file_path.starts_with(&persist_root));
    assert!(file_path.exists());

    let file = File::open(file_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let rows = builder
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum::<usize>();
    assert_eq!(rows, 32);

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_materializer_partitions_persisted_parquet_by_dt_part_and_id() {
    let persist_root = temp_persist_root("partitioned");
    let persist_publisher = Arc::new(RecordingPersistPublisher::default());
    let partition_spec = StreamTablePersistPartitionSpec::new(
        Some("dt".to_string()),
        Some("instrument_id".to_string()),
        Some("%Y%m".to_string()),
    )
    .unwrap();
    let persist_config =
        StreamTablePersistConfig::new(&persist_root).with_partition_spec(partition_spec);
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 4)
            .unwrap()
            .with_parquet_persist(persist_config)
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(
            partitioned_input_batch(),
        ))
        .unwrap();
    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch()))
        .unwrap();

    materializer
        .wait_for_persisted_files_for_test(1, Duration::from_secs(2))
        .unwrap();

    let files = persist_publisher.files.lock().unwrap();
    assert_eq!(files.len(), 3);
    assert!(files.iter().all(|file| file["stream_name"] == "ticks"));
    assert!(files.iter().all(|file| file["source_segment_id"] == 1));
    assert!(files.iter().all(|file| file["source_generation"] == 0));
    assert!(files
        .iter()
        .all(|file| file["persist_status"] == "committed"));
    assert!(files.iter().all(|file| file["persist_file_id"]
        .as_str()
        .unwrap()
        .starts_with("ticks:1:0:")));
    assert!(files.iter().any(|file| {
        file["partition"]["dt_part"] == "202604"
            && file["partition"]["instrument_id"] == "A/B"
            && file["partition_path"]["instrument_id"] == "A%2FB"
            && file["row_count"] == 2
            && file["file_path"]
                .as_str()
                .unwrap()
                .contains("dt_part=202604/instrument_id=A%2FB")
    }));
    assert!(files.iter().any(|file| {
        file["partition"]["dt_part"] == "202604"
            && file["partition"]["instrument_id"] == "IF2606"
            && file["row_count"] == 1
            && file["file_path"]
                .as_str()
                .unwrap()
                .contains("dt_part=202604/instrument_id=IF2606")
    }));
    assert!(files.iter().any(|file| {
        file["partition"]["dt_part"] == "202605"
            && file["partition"]["instrument_id"] == "IF2606"
            && file["row_count"] == 1
            && file["file_path"]
                .as_str()
                .unwrap()
                .contains("dt_part=202605/instrument_id=IF2606")
    }));

    for metadata in files.iter() {
        let file_path = PathBuf::from(metadata["file_path"].as_str().unwrap());
        assert!(file_path.exists());
        let file = File::open(file_path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let rows = builder
            .build()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum::<usize>();
        assert_eq!(rows, metadata["row_count"].as_u64().unwrap() as usize);
    }

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_persist_partition_rejects_legacy_dt_part_format() {
    let error = StreamTablePersistPartitionSpec::new(
        Some("dt".to_string()),
        Some("instrument_id".to_string()),
        Some("YYYYMM".to_string()),
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("unsupported stream table persist dt_part"));
}

#[test]
fn stream_table_persist_retries_transient_publish_failure_before_commit() {
    let persist_root = temp_persist_root("retry-transient");
    let persist_publisher = Arc::new(FlakyPersistPublisher::new(1));
    let persist_config = StreamTablePersistConfig::new(&persist_root)
        .with_max_attempts(3)
        .unwrap()
        .with_retry_delay(Duration::from_millis(0));
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_parquet_persist(persist_config)
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            3,
        )))
        .unwrap();

    materializer
        .wait_for_persisted_files_for_test(1, Duration::from_secs(2))
        .unwrap();
    let snapshot = materializer.persist_commit_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["status"], "committed");
    assert_eq!(snapshot[0]["attempts"], 2);
    assert_eq!(snapshot[0]["file_count"], 1);
    assert!(snapshot[0]["error"].is_null());
    assert_eq!(
        materializer.persist_failures_for_test(),
        Vec::<String>::new()
    );
    assert_eq!(persist_publisher.files.lock().unwrap().len(), 1);
    materializer.on_flush().unwrap();

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_materializer_does_not_block_on_persist_publisher_after_rollover() {
    let persist_root = temp_persist_root("nonblocking");
    let persist_publisher = Arc::new(BlockingPersistPublisher::default());
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_parquet_persist(StreamTablePersistConfig::new(&persist_root))
            .with_persist_publisher(persist_publisher.clone());
    let (done_sender, done_receiver) = mpsc::channel();

    let handle = thread::spawn(move || {
        let result = materializer.on_data(SegmentTableView::from_record_batch(
            input_batch_with_rows(3),
        ));
        done_sender.send(result.is_ok()).unwrap();
    });

    let completed_while_publish_blocked = done_receiver
        .recv_timeout(Duration::from_millis(200))
        .unwrap_or(false);
    persist_publisher.release();
    handle.join().unwrap();

    assert!(
        completed_while_publish_blocked,
        "stream table on_data should not wait for persisted metadata publication"
    );

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_flush_reports_background_persist_failure() {
    let persist_root = temp_persist_root("failure");
    let persist_publisher = Arc::new(FailingPersistPublisher);
    let persist_config = StreamTablePersistConfig::new(&persist_root)
        .with_max_attempts(2)
        .unwrap()
        .with_retry_delay(Duration::from_millis(0));
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_parquet_persist(persist_config)
            .with_persist_publisher(persist_publisher);

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            3,
        )))
        .unwrap();

    let failures = wait_for_persist_failure(&materializer);
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("publisher failed"));
    let snapshot = materializer.persist_commit_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["source_segment_id"], 1);
    assert_eq!(snapshot[0]["source_generation"], 0);
    assert_eq!(snapshot[0]["status"], "failed");
    assert_eq!(snapshot[0]["attempts"], 2);
    assert!(snapshot[0]["error"]
        .as_str()
        .unwrap()
        .contains("publisher failed"));

    let error = materializer.on_flush().unwrap_err();
    assert!(error.to_string().contains("stream table persist failed"));
    assert!(error.to_string().contains("publisher failed"));

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_persist_failure_publishes_queryable_error_event() {
    let persist_root = temp_persist_root("failure-event");
    let persist_publisher = Arc::new(FailingPersistEventPublisher::default());
    let persist_config = StreamTablePersistConfig::new(&persist_root)
        .with_max_attempts(2)
        .unwrap()
        .with_retry_delay(Duration::from_millis(0));
    let mut materializer =
        StreamTableMaterializer::new_with_row_capacity("ticks", input_schema(), 2)
            .unwrap()
            .with_parquet_persist(persist_config)
            .with_persist_publisher(persist_publisher.clone());

    materializer
        .on_data(SegmentTableView::from_record_batch(input_batch_with_rows(
            3,
        )))
        .unwrap();

    let failures = wait_for_persist_failure(&materializer);
    assert_eq!(failures.len(), 1);
    let events = persist_publisher.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["stream_name"], "ticks");
    assert_eq!(events[0]["persist_event_type"], "persist_failed");
    assert_eq!(events[0]["source_segment_id"], 1);
    assert_eq!(events[0]["source_generation"], 0);
    assert_eq!(events[0]["attempts"], 2);
    assert!(events[0]["persist_event_id"]
        .as_str()
        .unwrap()
        .starts_with("ticks:1:0:failed"));
    assert!(events[0]["error"]
        .as_str()
        .unwrap()
        .contains("publisher failed"));

    let _ = fs::remove_dir_all(persist_root);
}

#[test]
fn stream_table_flush_and_stop_emit_no_extra_batches() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();

    assert!(engine.on_flush().unwrap().is_empty());
    assert!(engine.on_stop().unwrap().is_empty());
}

#[test]
fn stream_table_on_data_passes_empty_batch_through_without_modification() {
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
    let batch = empty_input_batch();
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
    let mut engine = StreamTableEngine::new("ticks", input_schema()).unwrap();
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
