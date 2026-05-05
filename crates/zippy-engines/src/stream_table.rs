use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::{
    array::{
        Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt32Array,
    },
    compute::{concat_batches, take},
    datatypes::{DataType, Fields, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use zippy_core::{Engine, Result, SchemaRef, SegmentRowView, SegmentTableView, ZippyError};
use zippy_segment_store::{
    compile_schema, ColumnSpec, ColumnType, PartitionHandle, PartitionWriterHandle, ReaderSession,
    SealedSegmentHandle, SegmentLease, SegmentStore, SegmentStoreConfig, ZippySegmentStoreError,
};

use crate::table_view::record_batch_from_table_rows;

const STREAM_TABLE_PARTITION: &str = "all";
pub const DEFAULT_STREAM_TABLE_ROW_CAPACITY: usize = 65_536;
const DEFAULT_REPLACEMENT_RETAINED_SNAPSHOTS: usize = 8;
type SegmentIdentity = (u64, u64);

/// Materializes an input stream into an active segment-backed stream table.
pub struct StreamTableMaterializer {
    name: String,
    input_schema: SchemaRef,
    store: SegmentStore,
    partition: PartitionHandle,
    sealed_segment_descriptors: Vec<serde_json::Value>,
    replacement_retained_snapshots: Vec<Vec<serde_json::Value>>,
    retention_segments: Option<usize>,
    replacement_retention_snapshots: usize,
    persisted_segment_identities: BTreeSet<SegmentIdentity>,
    descriptor_publisher: Option<Arc<dyn StreamTableDescriptorPublisher>>,
    descriptor_forwarding: bool,
    forwarded_active_descriptor: Option<serde_json::Value>,
    persist_config: Option<StreamTablePersistConfig>,
    persist_publisher: Option<Arc<dyn StreamTablePersistPublisher>>,
    retention_guard: Option<Arc<dyn StreamTableRetentionGuard>>,
    persist_worker: Option<StreamTablePersistWorker>,
}

/// Materializes keyed rows into a replace-style active segment snapshot.
pub struct KeyValueTableMaterializer {
    name: String,
    input_schema: SchemaRef,
    by: Vec<String>,
    latest_rows: BTreeMap<Vec<String>, KeyValueOwnedRow>,
    table: StreamTableMaterializer,
}

/// Keeps a stream table segment pinned for reader-safe retention tests.
pub struct StreamTableSegmentLease {
    _session: ReaderSession,
    _lease: SegmentLease,
}

/// Publishes active segment descriptor updates after stream table rollover.
pub trait StreamTableDescriptorPublisher: Send + Sync {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> Result<()>;
}

/// Checks whether a sealed segment is safe to release from live retention.
pub trait StreamTableRetentionGuard: Send + Sync {
    fn can_release(&self, segment_id: u64, generation: u64) -> Result<bool>;
}

/// Configuration for parquet persistence files written by a stream table.
#[derive(Debug, Clone)]
pub struct StreamTablePersistConfig {
    root: PathBuf,
    partition_spec: Option<StreamTablePersistPartitionSpec>,
    max_attempts: usize,
    retry_delay: Duration,
}

/// Partitioning configuration for stream table parquet persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamTablePersistPartitionSpec {
    dt_column: Option<String>,
    id_column: Option<String>,
    dt_part: Option<StreamTableDateTimePart>,
}

/// Supported datetime partition formats derived from `dt_column`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTableDateTimePart {
    Year,
    YearMonth,
    YearMonthDay,
    YearMonthDayHour,
}

struct PersistPartitionBatch {
    values: PersistPartitionValues,
    batch: RecordBatch,
}

#[derive(Clone)]
struct KeyValueOwnedRow {
    batch: RecordBatch,
}

#[derive(Clone)]
struct PersistPartitionValues {
    raw: Vec<(String, String)>,
    encoded: Vec<(String, String)>,
}

/// Publishes persisted file metadata after a sealed segment is flushed to parquet.
pub trait StreamTablePersistPublisher: Send + Sync {
    fn publish(&self, persisted_file: serde_json::Value) -> Result<()>;

    fn publish_event(&self, _persist_event: serde_json::Value) -> Result<()> {
        Ok(())
    }
}

struct StreamTablePersistTask {
    sealed: SealedSegmentHandle,
    source_generation: serde_json::Value,
    identity: SegmentIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamTablePersistCommitStatus {
    Pending,
    Writing,
    Committed,
    Failed,
}

#[derive(Debug, Clone)]
struct StreamTablePersistCommitRecord {
    identity: SegmentIdentity,
    status: StreamTablePersistCommitStatus,
    attempts: u64,
    file_count: usize,
    error: Option<String>,
    updated_at_millis: u64,
}

struct StreamTablePersistWorker {
    sender: Option<mpsc::Sender<StreamTablePersistTask>>,
    completed: Arc<(Mutex<usize>, Condvar)>,
    completed_segments: Arc<(Mutex<Vec<SegmentIdentity>>, Condvar)>,
    failures: Arc<Mutex<Vec<String>>>,
    commit_state: Arc<Mutex<BTreeMap<SegmentIdentity, StreamTablePersistCommitRecord>>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl StreamTablePersistConfig {
    /// Create a parquet persist config rooted at the supplied directory.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            partition_spec: None,
            max_attempts: 3,
            retry_delay: Duration::from_millis(50),
        }
    }

    /// Return the persist root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Attach parquet dataset partitioning to this persist config.
    pub fn with_partition_spec(mut self, partition_spec: StreamTablePersistPartitionSpec) -> Self {
        self.partition_spec = Some(partition_spec);
        self
    }

    /// Set the maximum number of persist attempts for each sealed segment.
    pub fn with_max_attempts(mut self, max_attempts: usize) -> Result<Self> {
        if max_attempts == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table persist max_attempts must be greater than zero".to_string(),
            });
        }
        self.max_attempts = max_attempts;
        Ok(self)
    }

    /// Set the delay between failed persist attempts.
    pub fn with_retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }
}

impl StreamTablePersistPartitionSpec {
    /// Create a partition spec using an optional datetime and identifier column.
    pub fn new(
        dt_column: Option<String>,
        id_column: Option<String>,
        dt_part: Option<String>,
    ) -> Result<Self> {
        let dt_column = normalize_optional_name(dt_column);
        let id_column = normalize_optional_name(id_column);
        let dt_part = match normalize_optional_name(dt_part) {
            Some(value) => Some(StreamTableDateTimePart::parse(&value)?),
            None => None,
        };
        if dt_part.is_some() && dt_column.is_none() {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table persist dt_part requires dt_column".to_string(),
            });
        }
        if dt_column.is_some() && dt_part.is_none() {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table persist dt_column requires dt_part".to_string(),
            });
        }
        if dt_column.is_none() && id_column.is_none() {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table persist partition must set dt_column or id_column"
                    .to_string(),
            });
        }
        Ok(Self {
            dt_column,
            id_column,
            dt_part,
        })
    }

    fn is_empty(&self) -> bool {
        self.dt_column.is_none() && self.id_column.is_none()
    }
}

impl StreamTableDateTimePart {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "%Y" => Ok(Self::Year),
            "%Y%m" => Ok(Self::YearMonth),
            "%Y%m%d" => Ok(Self::YearMonthDay),
            "%Y%m%d%H" => Ok(Self::YearMonthDayHour),
            _ => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported stream table persist dt_part value=[{}]", value),
            }),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Year => "%Y",
            Self::YearMonth => "%Y%m",
            Self::YearMonthDay => "%Y%m%d",
            Self::YearMonthDayHour => "%Y%m%d%H",
        }
    }

    fn format_timestamp_ns(&self, timestamp_ns: i64) -> String {
        let (year, month, day, hour) = timestamp_ns_to_utc_parts(timestamp_ns);
        match self {
            Self::Year => format!("{year:04}"),
            Self::YearMonth => format!("{year:04}{month:02}"),
            Self::YearMonthDay => format!("{year:04}{month:02}{day:02}"),
            Self::YearMonthDayHour => format!("{year:04}{month:02}{day:02}{hour:02}"),
        }
    }
}

impl StreamTablePersistWorker {
    fn spawn(
        stream_name: String,
        config: StreamTablePersistConfig,
        publisher: Option<Arc<dyn StreamTablePersistPublisher>>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<StreamTablePersistTask>();
        let completed = Arc::new((Mutex::new(0), Condvar::new()));
        let worker_completed = Arc::clone(&completed);
        let completed_segments = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
        let worker_completed_segments = Arc::clone(&completed_segments);
        let failures = Arc::new(Mutex::new(Vec::new()));
        let worker_failures = Arc::clone(&failures);
        let commit_state = Arc::new(Mutex::new(BTreeMap::new()));
        let worker_commit_state = Arc::clone(&commit_state);
        let join_handle = thread::Builder::new()
            .name(format!("zippy-stream-table-persist-{stream_name}"))
            .spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let mut last_error = None;
                    for attempt in 1..=config.max_attempts {
                        update_persist_commit_state(
                            &worker_commit_state,
                            task.identity,
                            StreamTablePersistCommitStatus::Writing,
                            |record| {
                                record.attempts = attempt as u64;
                                record.error = None;
                            },
                        );
                        match write_and_publish_persisted_files(
                            &stream_name,
                            &task,
                            &config,
                            publisher.as_deref(),
                        ) {
                            Ok(file_count) => {
                                last_error = None;
                                update_persist_commit_state(
                                    &worker_commit_state,
                                    task.identity,
                                    StreamTablePersistCommitStatus::Committed,
                                    |record| {
                                        record.file_count = file_count;
                                        record.error = None;
                                    },
                                );
                                let (segments_lock, segments_changed) = &*worker_completed_segments;
                                segments_lock.lock().unwrap().push(task.identity);
                                segments_changed.notify_all();
                                let (lock, changed) = &*worker_completed;
                                *lock.lock().unwrap() += 1;
                                changed.notify_all();
                                break;
                            }
                            Err(error) => {
                                let error = error.to_string();
                                last_error = Some(error.clone());
                                if attempt < config.max_attempts {
                                    update_persist_commit_state(
                                        &worker_commit_state,
                                        task.identity,
                                        StreamTablePersistCommitStatus::Pending,
                                        |record| {
                                            record.error = Some(error);
                                        },
                                    );
                                    if !config.retry_delay.is_zero() {
                                        thread::sleep(config.retry_delay);
                                    }
                                }
                            }
                        }
                    }

                    if let Some(error) = last_error {
                        worker_failures.lock().unwrap().push(error.clone());
                        update_persist_commit_state(
                            &worker_commit_state,
                            task.identity,
                            StreamTablePersistCommitStatus::Failed,
                            |record| {
                                record.error = Some(error.clone());
                            },
                        );
                        if let Some(publisher) = &publisher {
                            let event = persist_failure_event(
                                &stream_name,
                                task.identity,
                                config.max_attempts as u64,
                                &error,
                            );
                            let _ = publisher.publish_event(event);
                        }
                    }
                }
            })
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to start stream table persist worker error=[{}]",
                    error
                ),
            })?;

        Ok(Self {
            sender: Some(sender),
            completed,
            completed_segments,
            failures,
            commit_state,
            join_handle: Some(join_handle),
        })
    }

    fn enqueue(&self, task: StreamTablePersistTask) -> Result<()> {
        let Some(sender) = &self.sender else {
            return Err(ZippyError::Io {
                reason: "stream table persist worker is stopped".to_string(),
            });
        };
        update_persist_commit_state(
            &self.commit_state,
            task.identity,
            StreamTablePersistCommitStatus::Pending,
            |_| {},
        );
        let identity = task.identity;
        sender.send(task).map_err(|error| {
            update_persist_commit_state(
                &self.commit_state,
                identity,
                StreamTablePersistCommitStatus::Failed,
                |record| {
                    record.error = Some(error.to_string());
                },
            );
            ZippyError::Io {
                reason: format!(
                    "failed to enqueue stream table persist task error=[{}]",
                    error
                ),
            }
        })
    }

    fn wait_for_completed_for_test(&self, expected: usize, timeout: Duration) -> Result<()> {
        let (lock, changed) = &*self.completed;
        let completed = lock.lock().unwrap();
        let result = changed
            .wait_timeout_while(completed, timeout, |count| *count < expected)
            .map_err(|_| ZippyError::Io {
                reason: "stream table persist wait lock poisoned".to_string(),
            })?;
        if *result.0 >= expected {
            return Ok(());
        }
        Err(ZippyError::Io {
            reason: format!(
                "timed out waiting for stream table persisted files expected=[{}] completed=[{}]",
                expected, *result.0
            ),
        })
    }

    fn failures(&self) -> Vec<String> {
        self.failures.lock().unwrap().clone()
    }

    fn failure_summary(&self) -> Option<String> {
        let failures = self.failures.lock().unwrap();
        if failures.is_empty() {
            return None;
        }
        Some(failures.join("; "))
    }

    fn drain_completed_segments(&self) -> Vec<SegmentIdentity> {
        let (lock, _) = &*self.completed_segments;
        std::mem::take(&mut *lock.lock().unwrap())
    }

    fn commit_snapshot(&self) -> Vec<serde_json::Value> {
        self.commit_state
            .lock()
            .unwrap()
            .values()
            .map(StreamTablePersistCommitRecord::to_json)
            .collect()
    }
}

impl StreamTablePersistCommitStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Writing => "writing",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

impl StreamTablePersistCommitRecord {
    fn new(identity: SegmentIdentity, status: StreamTablePersistCommitStatus) -> Self {
        Self {
            identity,
            status,
            attempts: 0,
            file_count: 0,
            error: None,
            updated_at_millis: current_time_millis(),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "source_segment_id": self.identity.0,
            "source_generation": self.identity.1,
            "status": self.status.as_str(),
            "attempts": self.attempts,
            "file_count": self.file_count,
            "error": self.error,
            "updated_at": self.updated_at_millis,
        })
    }
}

fn update_persist_commit_state<F>(
    commit_state: &Arc<Mutex<BTreeMap<SegmentIdentity, StreamTablePersistCommitRecord>>>,
    identity: SegmentIdentity,
    status: StreamTablePersistCommitStatus,
    update: F,
) where
    F: FnOnce(&mut StreamTablePersistCommitRecord),
{
    let mut state = commit_state.lock().unwrap();
    let record = state
        .entry(identity)
        .or_insert_with(|| StreamTablePersistCommitRecord::new(identity, status));
    record.status = status;
    record.updated_at_millis = current_time_millis();
    update(record);
}

impl Drop for StreamTablePersistWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

impl StreamTableMaterializer {
    /// Create a new stream table materializer.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :returns: Initialized materializer.
    /// :rtype: Result<StreamTableMaterializer>
    pub fn new(name: impl Into<String>, input_schema: SchemaRef) -> Result<Self> {
        Self::new_with_row_capacity(name, input_schema, DEFAULT_STREAM_TABLE_ROW_CAPACITY)
    }

    /// Create a new stream table materializer with an explicit segment row capacity.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :param row_capacity: Active segment row capacity before rollover.
    /// :type row_capacity: usize
    /// :returns: Initialized materializer.
    /// :rtype: Result<StreamTableMaterializer>
    pub fn new_with_row_capacity(
        name: impl Into<String>,
        input_schema: SchemaRef,
        row_capacity: usize,
    ) -> Result<Self> {
        if row_capacity == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table row_capacity must be greater than zero".to_string(),
            });
        }
        let name = name.into();
        let compiled_schema = compile_arrow_schema(&input_schema)?;
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: row_capacity,
        })
        .map_err(segment_error)?;
        let partition = store
            .open_partition_with_schema(&name, STREAM_TABLE_PARTITION, compiled_schema)
            .map_err(segment_error)?;

        Ok(Self {
            name,
            input_schema,
            store,
            partition,
            sealed_segment_descriptors: Vec::new(),
            replacement_retained_snapshots: Vec::new(),
            retention_segments: None,
            replacement_retention_snapshots: DEFAULT_REPLACEMENT_RETAINED_SNAPSHOTS,
            persisted_segment_identities: BTreeSet::new(),
            descriptor_publisher: None,
            descriptor_forwarding: false,
            forwarded_active_descriptor: None,
            persist_config: None,
            persist_publisher: None,
            retention_guard: None,
            persist_worker: None,
        })
    }

    /// Attach a publisher used when the active segment descriptor changes.
    pub fn with_descriptor_publisher(
        mut self,
        publisher: Arc<dyn StreamTableDescriptorPublisher>,
    ) -> Self {
        self.descriptor_publisher = Some(publisher);
        self
    }

    /// Allow zero-copy publication of upstream active segment descriptors.
    pub fn with_descriptor_forwarding(mut self, enabled: bool) -> Self {
        self.descriptor_forwarding = enabled;
        self
    }

    /// Limit how many sealed segments are advertised to readers.
    pub fn with_retention_segments(mut self, retention_segments: usize) -> Self {
        self.retention_segments = Some(retention_segments);
        self
    }

    /// Limit how many old replacement snapshots are retained for stale readers.
    pub fn with_replacement_retention_snapshots(mut self, snapshots: usize) -> Result<Self> {
        if snapshots == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "replacement_retention_snapshots must be greater than zero".to_string(),
            });
        }
        self.replacement_retention_snapshots = snapshots;
        Ok(self)
    }

    /// Enable parquet persistence writes for sealed segments.
    pub fn with_parquet_persist(mut self, config: StreamTablePersistConfig) -> Self {
        self.persist_config = Some(config);
        self
    }

    /// Attach a publisher used when parquet persistence metadata is produced.
    pub fn with_persist_publisher(
        mut self,
        publisher: Arc<dyn StreamTablePersistPublisher>,
    ) -> Self {
        self.persist_publisher = Some(publisher);
        self
    }

    /// Attach a guard used before live retained segments are released.
    pub fn with_retention_guard(mut self, guard: Arc<dyn StreamTableRetentionGuard>) -> Self {
        self.retention_guard = Some(guard);
        self
    }

    /// Return the active segment committed row count.
    pub fn active_committed_row_count(&self) -> usize {
        self.partition.active_committed_row_count()
    }

    /// Export the active segment descriptor envelope for cross-process readers.
    pub fn active_descriptor_envelope_bytes(&self) -> Result<Vec<u8>> {
        let mut descriptor = self
            .forwarded_active_descriptor
            .clone()
            .map(Ok)
            .unwrap_or_else(|| self.active_descriptor_value())?;
        descriptor["sealed_segments"] =
            serde_json::Value::Array(self.sealed_segment_descriptors.clone());
        serde_json::to_vec(&descriptor).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })
    }

    /// Return a debug Arrow snapshot of the active segment.
    pub fn active_record_batch(&self) -> Result<RecordBatch> {
        self.partition
            .debug_snapshot_record_batch()
            .map_err(segment_error)
    }

    /// Keep the owned store observable for future lifecycle integration.
    pub fn segment_store(&self) -> &SegmentStore {
        &self.store
    }

    /// Pin the current active segment until the returned lease is dropped.
    pub fn pin_active_segment_for_test(&self) -> Result<StreamTableSegmentLease> {
        let session = self
            .store
            .open_session("stream-table-test-reader")
            .map_err(segment_error)?;
        let lease = session
            .attach_active(&self.partition)
            .map_err(segment_error)?;
        Ok(StreamTableSegmentLease {
            _session: session,
            _lease: lease,
        })
    }

    /// Wait until at least `expected` persisted files have been published.
    pub fn wait_for_persisted_files_for_test(
        &self,
        expected: usize,
        timeout: Duration,
    ) -> Result<()> {
        let Some(worker) = &self.persist_worker else {
            return Err(ZippyError::Io {
                reason: "stream table persist worker is not running".to_string(),
            });
        };
        worker.wait_for_completed_for_test(expected, timeout)
    }

    /// Return persisted file failures captured by the background worker.
    pub fn persist_failures_for_test(&self) -> Vec<String> {
        self.persist_worker
            .as_ref()
            .map(StreamTablePersistWorker::failures)
            .unwrap_or_default()
    }

    /// Return a point-in-time snapshot of background persist commit states.
    pub fn persist_commit_snapshot(&self) -> Vec<serde_json::Value> {
        self.persist_worker
            .as_ref()
            .map(StreamTablePersistWorker::commit_snapshot)
            .unwrap_or_default()
    }

    fn materialize_table(&mut self, table: &SegmentTableView) -> Result<()> {
        if table.num_rows() == 0 {
            return Ok(());
        }
        if self.apply_retention()? {
            self.publish_active_descriptor()?;
        }

        self.materialize_table_rows(table, true)
    }

    fn materialize_table_rows(
        &mut self,
        table: &SegmentTableView,
        publish_rollovers: bool,
    ) -> Result<()> {
        if table.num_rows() == 0 {
            return Ok(());
        }
        if let Some(row_view) = table.as_segment_row_view() {
            return self.materialize_segment_rows(row_view, publish_rollovers);
        }

        self.forwarded_active_descriptor = None;
        let fields = self.input_schema.fields().clone();
        let columns = (0..fields.len())
            .map(|index| table.column_at(index))
            .collect::<Result<Vec<_>>>()?;
        let writer = self.partition.writer();

        for row_index in 0..table.num_rows() {
            match self.write_materialized_row(&writer, &fields, &columns, row_index) {
                Ok(()) => {}
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    let sealed_descriptor = self.active_descriptor_value()?;
                    let sealed = writer
                        .rollover_without_persistence()
                        .map_err(segment_error)?;
                    self.sealed_segment_descriptors
                        .push(sealed_descriptor.clone());
                    if publish_rollovers {
                        self.publish_active_descriptor()?;
                    }
                    if self.apply_retention()? && publish_rollovers {
                        self.publish_active_descriptor()?;
                    }
                    self.write_materialized_row(&writer, &fields, &columns, row_index)
                        .map_err(segment_error)?;
                    self.enqueue_persist_task(sealed, &sealed_descriptor)?;
                }
                Err(error) => return Err(segment_error(error)),
            }
        }

        Ok(())
    }

    fn materialize_segment_rows(
        &mut self,
        row_view: &SegmentRowView,
        publish_rollovers: bool,
    ) -> Result<()> {
        if self.try_forward_segment_descriptor(row_view)? {
            return Ok(());
        }

        self.forwarded_active_descriptor = None;
        let writer = self.partition.writer();
        let total_rows = row_view.num_rows();
        let mut copied = 0;
        while copied < total_rows {
            match writer.append_row_span(row_view.row_span(), copied, total_rows - copied) {
                Ok(rows) if rows > 0 => {
                    copied += rows;
                }
                Ok(_) => {
                    return Err(ZippyError::Io {
                        reason: "stream table segment append made no progress".to_string(),
                    });
                }
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    let sealed_descriptor = self.active_descriptor_value()?;
                    let sealed = writer
                        .rollover_without_persistence()
                        .map_err(segment_error)?;
                    self.sealed_segment_descriptors
                        .push(sealed_descriptor.clone());
                    if publish_rollovers {
                        self.publish_active_descriptor()?;
                    }
                    if self.apply_retention()? && publish_rollovers {
                        self.publish_active_descriptor()?;
                    }
                    self.enqueue_persist_task(sealed, &sealed_descriptor)?;
                }
                Err(error) => return Err(segment_error(error)),
            }
        }

        Ok(())
    }

    fn try_forward_segment_descriptor(&mut self, row_view: &SegmentRowView) -> Result<bool> {
        if !self.descriptor_forwarding
            || self.descriptor_publisher.is_none()
            || self.persist_config.is_some()
            || self.retention_segments.is_some()
            || !self.sealed_segment_descriptors.is_empty()
            || self.active_committed_row_count() != 0
        {
            return Ok(false);
        }

        let Some(envelope) = row_view
            .row_span()
            .active_descriptor_envelope_bytes()
            .map_err(|reason| ZippyError::Io {
                reason: reason.to_string(),
            })?
        else {
            return Ok(false);
        };
        let mut descriptor: serde_json::Value =
            serde_json::from_slice(&envelope).map_err(|error| ZippyError::Io {
                reason: error.to_string(),
            })?;
        descriptor["sealed_segments"] = serde_json::Value::Array(Vec::new());

        if self
            .forwarded_active_descriptor
            .as_ref()
            .is_some_and(|current| current == &descriptor)
        {
            return Ok(true);
        }

        let payload = serde_json::to_vec(&descriptor).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?;
        if let Some(publisher) = &self.descriptor_publisher {
            publisher.publish(payload)?;
        }
        self.forwarded_active_descriptor = Some(descriptor);
        Ok(true)
    }

    fn replace_with_table(&mut self, table: &SegmentTableView) -> Result<()> {
        self.ensure_persist_healthy()?;
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match stream table input schema engine=[{}]",
                    self.name
                ),
            });
        }

        let old_descriptor = self.active_descriptor_value()?;
        let mut old_snapshot = Vec::with_capacity(self.sealed_segment_descriptors.len() + 1);
        old_snapshot.extend(self.sealed_segment_descriptors.iter().cloned());
        old_snapshot.push(old_descriptor);
        let writer = self.partition.writer();
        let _sealed = writer
            .rollover_without_persistence()
            .map_err(segment_error)?;
        self.forwarded_active_descriptor = None;
        self.replacement_retained_snapshots.push(old_snapshot);
        self.sealed_segment_descriptors.clear();
        self.persisted_segment_identities.clear();

        self.materialize_table_rows(table, false)?;
        self.publish_active_descriptor()?;
        if self.apply_retention()? {
            self.publish_active_descriptor()?;
        }
        Ok(())
    }

    fn write_materialized_row(
        &self,
        writer: &PartitionWriterHandle,
        fields: &Fields,
        columns: &[ArrayRef],
        row_index: usize,
    ) -> std::result::Result<(), ZippySegmentStoreError> {
        writer.write_row(|row| {
            for (field_index, field) in fields.iter().enumerate() {
                let array = columns[field_index].as_ref();
                if array.is_null(row_index) {
                    if field.is_nullable() {
                        continue;
                    }
                    return Err(ZippySegmentStoreError::Schema(
                        "non-nullable stream table column received null",
                    ));
                }

                match field.data_type() {
                    DataType::Int64 => {
                        let values = array.as_any().downcast_ref::<Int64Array>().ok_or(
                            ZippySegmentStoreError::Schema("int64 column downcast failed"),
                        )?;
                        row.write_i64(field.name(), values.value(row_index))?;
                    }
                    DataType::Float64 => {
                        let values = array.as_any().downcast_ref::<Float64Array>().ok_or(
                            ZippySegmentStoreError::Schema("float64 column downcast failed"),
                        )?;
                        row.write_f64(field.name(), values.value(row_index))?;
                    }
                    DataType::Utf8 => {
                        let values = array.as_any().downcast_ref::<StringArray>().ok_or(
                            ZippySegmentStoreError::Schema("utf8 column downcast failed"),
                        )?;
                        row.write_utf8(field.name(), values.value(row_index))?;
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        let values = array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or(ZippySegmentStoreError::Schema(
                                "timestamp[ns] column downcast failed",
                            ))?;
                        row.write_i64(field.name(), values.value(row_index))?;
                    }
                    _ => {
                        return Err(ZippySegmentStoreError::Schema(
                            "unsupported stream table column type",
                        ));
                    }
                }
            }
            Ok(())
        })
    }

    fn publish_active_descriptor(&self) -> Result<()> {
        let Some(publisher) = &self.descriptor_publisher else {
            return Ok(());
        };
        let descriptor = self.active_descriptor_envelope_bytes()?;
        publisher.publish(descriptor)
    }

    fn active_descriptor_value(&self) -> Result<serde_json::Value> {
        let envelope = self
            .partition
            .active_descriptor_envelope_bytes()
            .map_err(segment_error)?;
        serde_json::from_slice(&envelope).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })
    }

    fn enqueue_persist_task(
        &mut self,
        sealed: SealedSegmentHandle,
        sealed_descriptor: &serde_json::Value,
    ) -> Result<()> {
        if self.persist_config.is_none() {
            return Ok(());
        }
        self.ensure_persist_worker()?;
        let Some(worker) = &self.persist_worker else {
            return Ok(());
        };
        let source_generation = sealed_descriptor
            .get("generation")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let identity = descriptor_segment_identity(sealed_descriptor).unwrap_or((
            sealed.segment_id(),
            source_generation.as_u64().unwrap_or_default(),
        ));
        worker.enqueue(StreamTablePersistTask {
            sealed,
            source_generation,
            identity,
        })
    }

    fn apply_retention(&mut self) -> Result<bool> {
        self.drain_persist_completions();
        let published_changed = if let Some(retention_segments) = self.retention_segments {
            self.trim_published_retained_segments(retention_segments)?
        } else {
            false
        };
        self.trim_replacement_retained_segments()?;
        Ok(published_changed)
    }

    fn trim_published_retained_segments(&mut self, retention_segments: usize) -> Result<bool> {
        let mut published_changed = false;
        while self.sealed_segment_descriptors.len() > retention_segments {
            let Some(identity) =
                descriptor_segment_identity(self.sealed_segment_descriptors.first().unwrap())
            else {
                break;
            };
            if self.persist_config.is_some()
                && !self.persisted_segment_identities.contains(&identity)
            {
                break;
            }
            if self
                .store
                .pin_count_for_segment(&self.partition, identity.0, identity.1)
                > 0
            {
                break;
            }
            if let Some(guard) = &self.retention_guard {
                if !guard.can_release(identity.0, identity.1)? {
                    break;
                }
            }
            self.sealed_segment_descriptors.remove(0);
            self.partition
                .release_retired_segment(identity.0, identity.1);
            published_changed = true;
        }
        Ok(published_changed)
    }

    fn trim_replacement_retained_segments(&mut self) -> Result<()> {
        while self.replacement_retained_snapshots.len() > self.replacement_retention_snapshots {
            let snapshot = self.replacement_retained_snapshots.first().unwrap();
            let identities = snapshot
                .iter()
                .map(descriptor_segment_identity)
                .collect::<Option<Vec<_>>>();
            let Some(identities) = identities else {
                break;
            };
            let has_pinned_segment = identities.iter().any(|identity| {
                self.store
                    .pin_count_for_segment(&self.partition, identity.0, identity.1)
                    > 0
            });
            if has_pinned_segment {
                break;
            }
            if let Some(guard) = &self.retention_guard {
                for identity in &identities {
                    if !guard.can_release(identity.0, identity.1)? {
                        return Ok(());
                    }
                }
            }

            self.replacement_retained_snapshots.remove(0);
            for identity in identities {
                self.partition
                    .release_retired_segment(identity.0, identity.1);
            }
        }
        Ok(())
    }

    fn drain_persist_completions(&mut self) {
        let Some(worker) = &self.persist_worker else {
            return;
        };
        for identity in worker.drain_completed_segments() {
            self.persisted_segment_identities.insert(identity);
        }
    }

    fn ensure_persist_worker(&mut self) -> Result<()> {
        if self.persist_worker.is_some() {
            return Ok(());
        }
        let Some(config) = self.persist_config.clone() else {
            return Ok(());
        };
        let worker = StreamTablePersistWorker::spawn(
            self.name.clone(),
            config,
            self.persist_publisher.clone(),
        )?;
        self.persist_worker = Some(worker);
        Ok(())
    }

    fn ensure_persist_healthy(&self) -> Result<()> {
        let Some(worker) = &self.persist_worker else {
            return Ok(());
        };
        if let Some(summary) = worker.failure_summary() {
            return Err(ZippyError::Io {
                reason: format!("stream table persist failed error=[{}]", summary),
            });
        }
        Ok(())
    }
}

impl KeyValueTableMaterializer {
    /// Create a new key-value table materializer.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :param by: UTF8 key columns identifying rows to replace.
    /// :type by: Vec<impl Into<String>>
    /// :returns: Initialized key-value table materializer.
    /// :rtype: Result<KeyValueTableMaterializer>
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        by: Vec<impl Into<String>>,
    ) -> Result<Self> {
        Self::new_with_row_capacity(name, input_schema, by, DEFAULT_STREAM_TABLE_ROW_CAPACITY)
    }

    /// Create a new key-value table materializer with explicit row capacity.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :param by: UTF8 key columns identifying rows to replace.
    /// :type by: Vec<impl Into<String>>
    /// :param row_capacity: Active segment row capacity before rollover.
    /// :type row_capacity: usize
    /// :returns: Initialized key-value table materializer.
    /// :rtype: Result<KeyValueTableMaterializer>
    pub fn new_with_row_capacity(
        name: impl Into<String>,
        input_schema: SchemaRef,
        by: Vec<impl Into<String>>,
        row_capacity: usize,
    ) -> Result<Self> {
        let name = name.into();
        let by = by.into_iter().map(Into::into).collect::<Vec<_>>();
        validate_latest_by_columns(input_schema.as_ref(), &by)?;
        let table = StreamTableMaterializer::new_with_row_capacity(
            name.clone(),
            Arc::clone(&input_schema),
            row_capacity,
        )?;

        Ok(Self {
            name,
            input_schema,
            by,
            latest_rows: BTreeMap::new(),
            table,
        })
    }

    /// Attach a publisher used when the active snapshot descriptor changes.
    pub fn with_descriptor_publisher(
        mut self,
        publisher: Arc<dyn StreamTableDescriptorPublisher>,
    ) -> Self {
        self.table = self.table.with_descriptor_publisher(publisher);
        self
    }

    /// Attach a guard used before releasing old replacement snapshots.
    pub fn with_retention_guard(mut self, guard: Arc<dyn StreamTableRetentionGuard>) -> Self {
        self.table = self.table.with_retention_guard(guard);
        self
    }

    /// Limit how many old replacement snapshots are retained for stale readers.
    pub fn with_replacement_retention_snapshots(mut self, snapshots: usize) -> Result<Self> {
        self.table = self.table.with_replacement_retention_snapshots(snapshots)?;
        Ok(self)
    }

    /// Return the grouping columns used to identify latest rows.
    pub fn by(&self) -> &[String] {
        &self.by
    }

    /// Return the active segment committed row count.
    pub fn active_committed_row_count(&self) -> usize {
        self.table.active_committed_row_count()
    }

    /// Export the active segment descriptor envelope for cross-process readers.
    pub fn active_descriptor_envelope_bytes(&self) -> Result<Vec<u8>> {
        self.table.active_descriptor_envelope_bytes()
    }

    /// Return a debug Arrow snapshot of the active key-value table segment.
    pub fn active_record_batch(&self) -> Result<RecordBatch> {
        self.table.active_record_batch()
    }

    fn update_latest_rows(&mut self, table: &SegmentTableView) -> Result<()> {
        let by_arrays = self
            .by
            .iter()
            .map(|field| table.column(field))
            .collect::<Result<Vec<_>>>()?;
        let by_columns = by_arrays
            .iter()
            .zip(&self.by)
            .map(|(column, field)| latest_string_array(column, field))
            .collect::<Result<Vec<_>>>()?;

        for row_index in 0..table.num_rows() {
            let key = latest_build_key(&by_columns, &self.by, row_index)?;
            let row = latest_extract_owned_row(table, &self.input_schema, row_index)?;
            self.latest_rows.insert(key, row);
        }

        Ok(())
    }

    fn build_snapshot_batch(&self) -> Result<RecordBatch> {
        if self.latest_rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.input_schema)));
        }

        concat_batches(
            &self.input_schema,
            self.latest_rows.values().map(|row| &row.batch),
        )
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to build key-value table snapshot batch error=[{}]",
                error
            ),
        })
    }
}

impl Engine for KeyValueTableMaterializer {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match key-value table input schema engine=[{}]",
                    self.name
                ),
            });
        }
        if table.num_rows() == 0 {
            return Ok(vec![]);
        }

        self.update_latest_rows(&table)?;
        let snapshot = self.build_snapshot_batch()?;
        let view = SegmentTableView::from_record_batch(snapshot);
        self.table.replace_with_table(&view)?;
        Ok(vec![view])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        self.table.on_flush()?;
        if self.latest_rows.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![SegmentTableView::from_record_batch(
            self.build_snapshot_batch()?,
        )])
    }

    fn on_stop(&mut self) -> Result<Vec<SegmentTableView>> {
        self.table.on_stop()?;
        self.on_flush()
    }
}

fn validate_latest_by_columns(schema: &Schema, by: &[String]) -> Result<()> {
    if by.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "key-value table materializer requires at least one by column".to_string(),
        });
    }

    let mut seen = BTreeSet::new();
    for field_name in by {
        if !seen.insert(field_name) {
            return Err(ZippyError::InvalidConfig {
                reason: format!("duplicate key-value table by column field=[{}]", field_name),
            });
        }
        let field = schema
            .field_with_name(field_name)
            .map_err(|_| ZippyError::SchemaMismatch {
                reason: format!("missing key-value table by field field=[{}]", field_name),
            })?;
        if field.data_type() != &DataType::Utf8 {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "key-value table by field must be utf8 field=[{}]",
                    field_name
                ),
            });
        }
    }

    Ok(())
}

fn latest_string_array<'a>(array: &'a ArrayRef, field: &str) -> Result<&'a StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("key-value table by field must be utf8 field=[{}]", field),
        })
}

fn latest_build_key(
    columns: &[&StringArray],
    fields: &[String],
    row_index: usize,
) -> Result<Vec<String>> {
    columns
        .iter()
        .zip(fields)
        .map(|(column, field)| {
            if column.is_null(row_index) {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!("key-value table by field contains null field=[{}]", field),
                });
            }
            Ok(column.value(row_index).to_string())
        })
        .collect()
}

fn latest_extract_owned_row(
    table: &SegmentTableView,
    schema: &SchemaRef,
    row_index: usize,
) -> Result<KeyValueOwnedRow> {
    let batch =
        record_batch_from_table_rows(table, schema, &[row_index as u32], "key-value table")?;
    Ok(KeyValueOwnedRow { batch })
}

fn write_sealed_segment_parquet(
    stream_name: &str,
    sealed: &SealedSegmentHandle,
    source_generation: serde_json::Value,
    config: &StreamTablePersistConfig,
) -> Result<Vec<serde_json::Value>> {
    let batch = sealed.as_record_batch().map_err(|error| ZippyError::Io {
        reason: format!("failed to export sealed segment to arrow error=[{}]", error),
    })?;
    let partitions = partition_record_batch(&batch, config.partition_spec.as_ref())?;
    let generation = source_generation.as_u64().unwrap_or_default();
    let mut files = Vec::with_capacity(partitions.len());
    for (part_index, partition) in partitions.into_iter().enumerate() {
        let target_dir = persist_partition_dir(config.root(), &partition.values);
        fs::create_dir_all(&target_dir).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create stream table persist directory path=[{}] error=[{}]",
                target_dir.display(),
                error
            ),
        })?;

        let file_name = if partition.values.raw.is_empty() {
            format!(
                "{}-segment-{:020}.parquet",
                sanitize_persist_file_component(stream_name),
                sealed.segment_id()
            )
        } else {
            format!(
                "{}-segment-{:020}-generation-{:020}-part-{:05}.parquet",
                sanitize_persist_file_component(stream_name),
                sealed.segment_id(),
                generation,
                part_index
            )
        };
        let target_path = target_dir.join(file_name);
        write_record_batch_parquet(&target_path, &partition.batch)?;

        files.push(serde_json::json!({
            "stream_name": stream_name,
            "persist_file_id": persist_file_id(stream_name, sealed.segment_id(), generation, part_index),
            "file_path": target_path.to_string_lossy().to_string(),
            "row_count": partition.batch.num_rows(),
            "source_segment_id": sealed.segment_id(),
            "source_generation": source_generation.clone(),
            "partition": partition_values_json(&partition.values.raw),
            "partition_path": partition_values_json(&partition.values.encoded),
            "partition_spec": partition_spec_json(config.partition_spec.as_ref()),
            "persist_status": "committed",
        }));
    }

    Ok(files)
}

fn write_and_publish_persisted_files(
    stream_name: &str,
    task: &StreamTablePersistTask,
    config: &StreamTablePersistConfig,
    publisher: Option<&dyn StreamTablePersistPublisher>,
) -> Result<usize> {
    let persisted_files = write_sealed_segment_parquet(
        stream_name,
        &task.sealed,
        task.source_generation.clone(),
        config,
    )?;
    let file_count = persisted_files.len();
    if let Some(publisher) = publisher {
        for persisted_file in persisted_files {
            publisher.publish(persisted_file)?;
        }
    }
    Ok(file_count)
}

fn persist_file_id(
    stream_name: &str,
    segment_id: u64,
    generation: u64,
    part_index: usize,
) -> String {
    format!("{stream_name}:{segment_id}:{generation}:{part_index}")
}

fn persist_failure_event(
    stream_name: &str,
    identity: SegmentIdentity,
    attempts: u64,
    error: &str,
) -> serde_json::Value {
    serde_json::json!({
        "stream_name": stream_name,
        "persist_event_id": format!("{}:{}:{}:failed", stream_name, identity.0, identity.1),
        "persist_event_type": "persist_failed",
        "source_segment_id": identity.0,
        "source_generation": identity.1,
        "attempts": attempts,
        "error": error,
        "created_at": current_time_millis(),
    })
}

fn write_record_batch_parquet(target_path: &Path, batch: &RecordBatch) -> Result<()> {
    let temp_path = temp_parquet_path_for_target(target_path);
    let file = File::create(&temp_path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to create stream table persist file path=[{}] error=[{}]",
            temp_path.display(),
            error
        ),
    })?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create stream table persist writer error=[{}]",
                error
            ),
        })?;
    writer.write(batch).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to write stream table persist batch error=[{}]",
            error
        ),
    })?;
    writer.close().map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to close stream table persist writer error=[{}]",
            error
        ),
    })?;
    fs::rename(&temp_path, target_path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        ZippyError::Io {
            reason: format!(
                "failed to commit stream table persist file path=[{}] error=[{}]",
                target_path.display(),
                error
            ),
        }
    })?;
    Ok(())
}

fn temp_parquet_path_for_target(target_path: &Path) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let file_name = target_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("segment.parquet");
    target_path.with_file_name(format!("{file_name}.tmp-{}-{suffix}", std::process::id()))
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn sanitize_persist_file_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect()
}

fn partition_record_batch(
    batch: &RecordBatch,
    partition_spec: Option<&StreamTablePersistPartitionSpec>,
) -> Result<Vec<PersistPartitionBatch>> {
    let Some(partition_spec) = partition_spec else {
        return Ok(vec![PersistPartitionBatch {
            values: PersistPartitionValues {
                raw: Vec::new(),
                encoded: Vec::new(),
            },
            batch: batch.clone(),
        }]);
    };
    if partition_spec.is_empty() {
        return Ok(vec![PersistPartitionBatch {
            values: PersistPartitionValues {
                raw: Vec::new(),
                encoded: Vec::new(),
            },
            batch: batch.clone(),
        }]);
    }

    let dt_array = match &partition_spec.dt_column {
        Some(dt_column) => Some(timestamp_partition_array(batch, dt_column)?),
        None => None,
    };
    let id_array = match &partition_spec.id_column {
        Some(id_column) => Some(string_partition_array(batch, id_column)?),
        None => None,
    };

    let mut groups: BTreeMap<String, (PersistPartitionValues, Vec<u32>)> = BTreeMap::new();
    for row_index in 0..batch.num_rows() {
        let mut raw = Vec::new();
        let mut encoded = Vec::new();
        if let (Some(dt_column), Some(dt_part), Some(array)) =
            (&partition_spec.dt_column, partition_spec.dt_part, dt_array)
        {
            if array.is_null(row_index) {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "stream table persist dt column contains null field=[{}]",
                        dt_column
                    ),
                });
            }
            let value = dt_part.format_timestamp_ns(array.value(row_index));
            raw.push(("dt_part".to_string(), value.clone()));
            encoded.push(("dt_part".to_string(), percent_encode_path_component(&value)));
        }
        if let (Some(id_column), Some(array)) = (&partition_spec.id_column, id_array) {
            if array.is_null(row_index) {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "stream table persist id column contains null field=[{}]",
                        id_column
                    ),
                });
            }
            let value = array.value(row_index).to_string();
            raw.push((id_column.clone(), value.clone()));
            encoded.push((id_column.clone(), percent_encode_path_component(&value)));
        }
        let key = encoded
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("/");
        groups
            .entry(key)
            .or_insert_with(|| (PersistPartitionValues { raw, encoded }, Vec::new()))
            .1
            .push(row_index as u32);
    }

    groups
        .into_values()
        .map(|(values, row_indices)| {
            let indices = UInt32Array::from(row_indices);
            let columns = batch
                .columns()
                .iter()
                .map(|column| {
                    take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                        reason: format!(
                            "failed to partition stream table persist batch error=[{}]",
                            error
                        ),
                    })
                })
                .collect::<Result<Vec<ArrayRef>>>()?;
            let batch =
                RecordBatch::try_new(batch.schema(), columns).map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to build partitioned stream table persist batch error=[{}]",
                        error
                    ),
                })?;
            Ok(PersistPartitionBatch { values, batch })
        })
        .collect()
}

fn timestamp_partition_array<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| ZippyError::InvalidConfig {
            reason: format!("missing stream table persist dt column field=[{}]", column),
        })?;
    match batch.schema().field(index).data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {}
        data_type => {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "stream table persist dt column must be timestamp[ns] field=[{}] data_type=[{:?}]",
                    column, data_type
                ),
            });
        }
    }
    batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "stream table persist dt column downcast failed field=[{}]",
                column
            ),
        })
}

fn string_partition_array<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a StringArray> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| ZippyError::InvalidConfig {
            reason: format!("missing stream table persist id column field=[{}]", column),
        })?;
    if batch.schema().field(index).data_type() != &DataType::Utf8 {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "stream table persist id column must be utf8 field=[{}]",
                column
            ),
        });
    }
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "stream table persist id column downcast failed field=[{}]",
                column
            ),
        })
}

fn persist_partition_dir(root: &Path, values: &PersistPartitionValues) -> PathBuf {
    values
        .encoded
        .iter()
        .fold(root.to_path_buf(), |path, (name, value)| {
            path.join(format!("{name}={value}"))
        })
}

fn partition_values_json(values: &[(String, String)]) -> serde_json::Value {
    values
        .iter()
        .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

fn partition_spec_json(
    partition_spec: Option<&StreamTablePersistPartitionSpec>,
) -> serde_json::Value {
    let Some(partition_spec) = partition_spec else {
        return serde_json::Value::Null;
    };
    serde_json::json!({
        "dt_column": partition_spec.dt_column,
        "id_column": partition_spec.id_column,
        "dt_part": partition_spec.dt_part.map(|value| value.as_str()),
    })
}

fn percent_encode_path_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(*byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn normalize_optional_name(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn timestamp_ns_to_utc_parts(timestamp_ns: i64) -> (i64, u32, u32, u32) {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let hour = (seconds_of_day / 3_600) as u32;
    let (year, month, day) = civil_from_days(days);
    (year, month, day, hour)
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

fn descriptor_segment_identity(descriptor: &serde_json::Value) -> Option<SegmentIdentity> {
    let segment_id = descriptor.get("segment_id")?.as_u64()?;
    let generation = descriptor.get("generation")?.as_u64()?;
    Some((segment_id, generation))
}

impl Engine for StreamTableMaterializer {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        self.ensure_persist_healthy()?;
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match stream table input schema engine=[{}]",
                    self.name
                ),
            });
        }

        self.materialize_table(&table)?;
        Ok(vec![table])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        self.ensure_persist_healthy()?;
        if self.apply_retention()? {
            self.publish_active_descriptor()?;
        }
        Ok(vec![])
    }

    fn on_stop(&mut self) -> Result<Vec<SegmentTableView>> {
        self.ensure_persist_healthy()?;
        Ok(vec![])
    }
}

fn compile_arrow_schema(schema: &SchemaRef) -> Result<zippy_segment_store::CompiledSchema> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let name: &'static str = Box::leak(field.name().to_string().into_boxed_str());
            let data_type = match field.data_type() {
                DataType::Int64 => ColumnType::Int64,
                DataType::Float64 => ColumnType::Float64,
                DataType::Utf8 => ColumnType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
                    let timezone = timezone
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "UTC".to_string());
                    ColumnType::TimestampNsTz(Box::leak(timezone.into_boxed_str()))
                }
                _ => {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!(
                            "unsupported stream table column type column=[{}] data_type=[{:?}]",
                            field.name(),
                            field.data_type()
                        ),
                    });
                }
            };

            Ok(if field.is_nullable() {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<Result<Vec<_>>>()?;

    compile_schema(&columns).map_err(|reason| ZippyError::SchemaMismatch {
        reason: format!("failed to compile stream table schema reason=[{}]", reason),
    })
}

fn segment_error(error: ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}
