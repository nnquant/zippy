use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{Cursor, ErrorKind};
use std::net::{TcpListener as StdTcpListener, TcpStream};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    and, concat_batches, filter_record_batch, lexsort_to_indices, or, take, SortColumn, SortOptions,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{
    arrow_reader::{
        ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
    },
    ProjectionMask,
};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use zippy_core::bus_protocol::{
    AcquireSegmentReaderLeaseRequest, ReleaseSegmentReaderLeaseRequest, WatchRequest, WatchResource,
};
use zippy_core::{
    canonical_schema_hash, schema_metadata, send_control_line_request, ControlEndpoint,
    ControlRequest, ControlResponse, Engine, GetStreamRequest, HeartbeatRequest,
    ListStreamsRequest, PublishSegmentDescriptorRequest, RegisterProcessRequest,
    RegisterSourceRequest, RegisterStreamRequest, Result, SegmentTableView, StreamInfo,
    UnregisterSourceRequest, ZippyError, DEFAULT_MAX_PARALLELISM, DEFAULT_MAX_QUERY_BUFFER_BYTES,
    DEFAULT_QUERY_MAX_BUFFERED_BYTES, MAX_PARALLELISM, MAX_QUERY_BUFFER_BYTES,
};
use zippy_engines::{
    StreamTableDescriptorPublisher, StreamTableMaterializer, DEFAULT_STREAM_TABLE_ROW_CAPACITY,
};
use zippy_segment_store::{
    compile_schema as compile_segment_schema, ActiveSegmentDescriptor, ActiveSegmentReader,
    ColumnSpec, ColumnType, CompiledSchema, LayoutPlan, RowSpanBatchReader, RowSpanView,
};

const MAX_GATEWAY_HEADER_BYTES: usize = 64 * 1024;
const MAX_GATEWAY_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_GATEWAY_CONNECTIONS: usize = 1024;
const DEFAULT_MAX_GATEWAY_SUBSCRIBERS: usize = 256;
const DEFAULT_MAX_GATEWAY_BLOCKING_REQUESTS: usize = 64;
const DEFAULT_GATEWAY_HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_GATEWAY_PAYLOAD_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_GATEWAY_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_GATEWAY_SUBSCRIBE_ACTIVE_WAIT_TIMEOUT: Duration = Duration::from_millis(100);
const DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS: usize = 8;
const PERSISTED_PARQUET_SCAN_BATCH_SIZE: usize = 1024;
const MAX_GATEWAY_SNAPSHOTS: usize = 1024;
const GATEWAY_SNAPSHOT_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Native Rust GatewayServer configuration.
#[derive(Debug, Clone)]
pub struct GatewayServerConfig {
    pub endpoint: String,
    pub master_endpoint: ControlEndpoint,
    pub token: Option<String>,
    pub max_write_rows: Option<usize>,
    pub max_connections: Option<usize>,
    pub max_subscribers: Option<usize>,
    pub max_blocking_requests: Option<usize>,
    pub write_timeout_ms: Option<u64>,
}

/// Native TCP gateway for cross-platform remote writes and queries.
pub struct GatewayServer {
    endpoint: String,
    state: Arc<GatewayState>,
    runtime: Option<GatewayRuntime>,
}

struct GatewayState {
    master: Arc<GatewayAsyncMasterClient>,
    token: Option<String>,
    max_write_rows: Option<usize>,
    writers: Mutex<BTreeMap<String, GatewayTableWriterHandle>>,
    subscribe_catalog_notifier: GatewaySubscribeCatalogNotifier,
    snapshots: Mutex<HashMap<String, GatewaySnapshot>>,
    snapshot_counter: AtomicU64,
    metrics: Arc<Mutex<GatewayMetrics>>,
    stopped: AtomicBool,
    connection_limit: Arc<tokio::sync::Semaphore>,
    blocking_limit: Arc<tokio::sync::Semaphore>,
    subscriber_limit: Arc<tokio::sync::Semaphore>,
    max_connections: usize,
    max_subscribers: usize,
    max_blocking_requests: usize,
    max_parallelism: usize,
    query_parallelism: usize,
    max_query_buffer_bytes: usize,
    query_max_buffered_bytes: usize,
    persisted_scan_pool: Arc<GatewayPersistedScanPool>,
    persisted_buffer_limit: Arc<tokio::sync::Semaphore>,
    write_timeout: Duration,
}

struct GatewayRuntime {
    shutdown: tokio::sync::watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

struct GatewayTableWriter {
    source_name: String,
    materializer: StreamTableMaterializer,
}

type GatewayTableWriterHandle = Arc<Mutex<GatewayTableWriter>>;

#[derive(Default)]
struct GatewaySubscribeCatalogNotifier {
    sequence: AtomicU64,
    notify: tokio::sync::Notify,
}

impl GatewaySubscribeCatalogNotifier {
    fn mark_activity(&self) {
        self.sequence.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    async fn wait_after(&self, observed_sequence: u64, timeout: Duration) -> bool {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        if self.sequence() != observed_sequence {
            return true;
        }
        tokio::time::timeout(timeout, &mut notified).await.is_ok()
    }
}

#[derive(Clone, Default)]
struct GatewayScanPushdown {
    filters: Vec<Value>,
    projection_columns: Option<Vec<String>>,
}

struct GatewayCollectStreamPlan {
    source: String,
    snapshot_id: Option<String>,
    plan: Vec<Value>,
    row_range_pushdown: Option<(GatewayRowRangePushdown, usize)>,
    scan_pushdown: GatewayScanPushdown,
    output_projection_columns: Option<Vec<String>>,
    chunk_rows: usize,
}

#[derive(Default)]
struct GatewayCollectStreamMetrics {
    streaming: bool,
    scanned_files: usize,
    scanned_file_paths: Vec<String>,
    scanned_rows: usize,
    returned_rows: usize,
    row_range_pushdown: Option<&'static str>,
    scan_elapsed_ms: f64,
    filter_elapsed_ms: f64,
    encode_elapsed_ms: f64,
    write_elapsed_ms: f64,
    file_scan_parallelism: usize,
    max_in_flight_file_scans: usize,
    max_pending_file_results: usize,
    query_buffer_budget_bytes: usize,
    file_buffer_budget_bytes: usize,
    buffered_bytes: usize,
    max_buffered_bytes: usize,
    buffer_wait_elapsed_ms: f64,
    global_buffer_wait_elapsed_ms: f64,
    first_chunk_elapsed_ms: f64,
    materialized_live_batches: usize,
    segment_streamed_batches: usize,
    segment_streamed_rows: usize,
}

struct OrderedGatewayFileResults {
    next_file_index: usize,
    max_pending: usize,
    pending: BTreeMap<usize, Vec<RecordBatch>>,
    max_observed_pending: usize,
}

impl OrderedGatewayFileResults {
    fn new(max_pending: usize) -> Self {
        Self {
            next_file_index: 0,
            max_pending: max_pending.max(1),
            pending: BTreeMap::new(),
            max_observed_pending: 0,
        }
    }

    fn insert(&mut self, file_index: usize, batches: Vec<RecordBatch>) -> Result<()> {
        if file_index < self.next_file_index {
            return Err(ZippyError::Io {
                reason: format!(
                    "streaming collect received stale file result file_index=[{}] next_file_index=[{}]",
                    file_index, self.next_file_index
                ),
            });
        }
        if self.pending.contains_key(&file_index) {
            return Err(ZippyError::Io {
                reason: format!(
                    "streaming collect received duplicate file result file_index=[{}]",
                    file_index
                ),
            });
        }
        if self.pending.len() >= self.max_pending {
            return Err(ZippyError::Io {
                reason: "streaming collect pending file result limit exceeded".to_string(),
            });
        }
        self.pending.insert(file_index, batches);
        self.max_observed_pending = self.max_observed_pending.max(self.pending.len());
        Ok(())
    }

    fn pop_ready(&mut self) -> Option<Vec<RecordBatch>> {
        let batches = self.pending.remove(&self.next_file_index)?;
        self.next_file_index += 1;
        Some(batches)
    }

    fn max_observed_pending(&self) -> usize {
        self.max_observed_pending
    }

    fn finish(self, total_files: usize) -> Result<()> {
        if self.next_file_index != total_files || !self.pending.is_empty() {
            return Err(ZippyError::Io {
                reason: format!(
                    "streaming collect missing ordered file results next_file_index=[{}] total_files=[{}] pending=[{}]",
                    self.next_file_index,
                    total_files,
                    self.pending.len()
                ),
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
struct GatewayPersistedScanTask {
    file_index: usize,
    file_path: String,
    projection_columns: Option<Vec<String>>,
    scan_pushdown: GatewayScanPushdown,
}

struct GatewayPersistedScanResult {
    file_index: usize,
    batches: Vec<RecordBatch>,
    scanned_rows: usize,
}

struct GatewayPersistedScanThreadResult {
    file_index: usize,
    result: Result<GatewayPersistedScanResult>,
}

#[derive(Default)]
struct GatewayBufferedBytesTracker {
    current_bytes: AtomicUsize,
    max_bytes: AtomicUsize,
    wait_nanos: AtomicU64,
}

impl GatewayBufferedBytesTracker {
    fn reserve(&self, bytes: usize) {
        let current = self.current_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.max_bytes.fetch_max(current, Ordering::AcqRel);
    }

    fn add_wait(&self, waited: Duration) {
        let wait_nanos = u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX);
        self.wait_nanos.fetch_add(wait_nanos, Ordering::Relaxed);
    }

    fn release(&self, bytes: usize) {
        let previous = self.current_bytes.fetch_sub(bytes, Ordering::AcqRel);
        assert!(previous >= bytes, "gateway buffered byte tracker underflow");
    }

    fn update_metrics(&self, metrics: &mut GatewayCollectStreamMetrics) {
        metrics.buffered_bytes = self.current_bytes.load(Ordering::Acquire);
        metrics.max_buffered_bytes = metrics
            .max_buffered_bytes
            .max(self.max_bytes.load(Ordering::Acquire));
        metrics.buffer_wait_elapsed_ms =
            self.wait_nanos.load(Ordering::Acquire) as f64 / 1_000_000.0;
    }
}

struct GatewayFileByteBudget {
    limit_bytes: usize,
    occupied: AtomicBool,
    tracker: Arc<GatewayBufferedBytesTracker>,
}

impl GatewayFileByteBudget {
    fn new(limit_bytes: usize, tracker: Arc<GatewayBufferedBytesTracker>) -> Result<Self> {
        if limit_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "gateway persisted file buffer budget must be positive".to_string(),
            });
        }
        Ok(Self {
            limit_bytes,
            occupied: AtomicBool::new(false),
            tracker,
        })
    }

    fn try_acquire_slot(self: &Arc<Self>) -> Option<GatewayFileSlotPermit> {
        self.occupied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| GatewayFileSlotPermit {
                budget: Arc::clone(self),
            })
    }

    fn buffered_permit(
        self: &Arc<Self>,
        slot: GatewayFileSlotPermit,
        bytes: usize,
        file_index: usize,
    ) -> Result<GatewayFileBytePermit> {
        if bytes > self.limit_bytes {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "persisted scan batch exceeds file buffer budget file_index=[{}] batch_bytes=[{}] file_buffer_budget_bytes=[{}]",
                    file_index, bytes, self.limit_bytes
                ),
            });
        }
        self.tracker.reserve(bytes);
        Ok(GatewayFileBytePermit {
            _slot: slot,
            tracker: Arc::clone(&self.tracker),
            bytes,
        })
    }
}

struct GatewayFileSlotPermit {
    budget: Arc<GatewayFileByteBudget>,
}

impl Drop for GatewayFileSlotPermit {
    fn drop(&mut self) {
        let was_occupied = self.budget.occupied.swap(false, Ordering::AcqRel);
        assert!(was_occupied, "gateway persisted file slot released twice");
    }
}

struct GatewayFileBytePermit {
    _slot: GatewayFileSlotPermit,
    tracker: Arc<GatewayBufferedBytesTracker>,
    bytes: usize,
}

impl Drop for GatewayFileBytePermit {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

struct GatewayPersistedBufferedBatch {
    batch: RecordBatch,
    _permit: Option<GatewayFileBytePermit>,
}

impl GatewayPersistedBufferedBatch {
    fn materialized(batch: RecordBatch) -> Self {
        Self {
            batch,
            _permit: None,
        }
    }
}

enum GatewayPersistedScanEvent {
    Batch(GatewayPersistedBufferedBatch),
    End {
        file_index: usize,
        scanned_rows: usize,
    },
    Error {
        file_index: usize,
        error: ZippyError,
    },
}

struct GatewayPersistedScanJob {
    task: GatewayPersistedScanTask,
    output_projection_columns: Option<Vec<String>>,
    event_sender: mpsc::SyncSender<GatewayPersistedScanEvent>,
    buffer_budget: Arc<GatewayFileByteBudget>,
    cancelled: Arc<AtomicBool>,
}

struct GatewayPersistedScanCursor {
    task: GatewayPersistedScanTask,
    output_projection_columns: Option<Vec<String>>,
    event_sender: mpsc::SyncSender<GatewayPersistedScanEvent>,
    buffer_budget: Arc<GatewayFileByteBudget>,
    cancelled: Arc<AtomicBool>,
    reader: ParquetRecordBatchReader,
    scanned_rows: usize,
    budget_wait_started: Option<Instant>,
}

enum GatewayPersistedScanWork {
    Start(GatewayPersistedScanJob),
    Continue(GatewayPersistedScanCursor),
}

impl GatewayPersistedScanWork {
    fn identity(
        &self,
    ) -> (
        usize,
        mpsc::SyncSender<GatewayPersistedScanEvent>,
        Arc<AtomicBool>,
    ) {
        match self {
            Self::Start(job) => (
                job.task.file_index,
                job.event_sender.clone(),
                Arc::clone(&job.cancelled),
            ),
            Self::Continue(cursor) => (
                cursor.task.file_index,
                cursor.event_sender.clone(),
                Arc::clone(&cursor.cancelled),
            ),
        }
    }
}

enum GatewayPersistedScanPoolMessage {
    Work(GatewayPersistedScanWork),
    Stop,
}

enum GatewayPersistedScanStep {
    Complete,
    Continue {
        cursor: GatewayPersistedScanCursor,
        backoff: bool,
    },
}

struct GatewayPersistedScanPool {
    sender: Mutex<Option<mpsc::Sender<GatewayPersistedScanPoolMessage>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct GatewayPersistedScanResources {
    pool: Arc<GatewayPersistedScanPool>,
    parallelism: usize,
    query_buffer_budget_bytes: usize,
}

impl GatewayPersistedScanPool {
    fn new(worker_count: usize) -> Result<Self> {
        if worker_count == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "gateway persisted scan worker count must be positive".to_string(),
            });
        }
        let (sender, receiver) = mpsc::channel::<GatewayPersistedScanPoolMessage>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let requeue_sender = sender.clone();
            let worker = thread::Builder::new()
                .name(format!("zippy-query-{worker_index}"))
                .spawn(move || loop {
                    let message = {
                        let receiver = receiver.lock().unwrap();
                        receiver.recv()
                    };
                    let Ok(message) = message else {
                        break;
                    };
                    let GatewayPersistedScanPoolMessage::Work(work) = message else {
                        break;
                    };
                    let (file_index, event_sender, cancelled) = work.identity();
                    if cancelled.load(Ordering::Acquire) {
                        continue;
                    }
                    let result =
                        catch_unwind(AssertUnwindSafe(|| advance_persisted_scan_work(work)))
                            .unwrap_or_else(|_| {
                                Err(ZippyError::Io {
                                    reason: "gateway persisted scan worker panicked".to_string(),
                                })
                            });
                    match result {
                        Ok(GatewayPersistedScanStep::Complete) => {}
                        Ok(GatewayPersistedScanStep::Continue { cursor, backoff }) => {
                            if backoff {
                                thread::sleep(Duration::from_micros(100));
                            }
                            if requeue_sender
                                .send(GatewayPersistedScanPoolMessage::Work(
                                    GatewayPersistedScanWork::Continue(cursor),
                                ))
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            if !cancelled.load(Ordering::Acquire) {
                                let _ = event_sender.try_send(GatewayPersistedScanEvent::Error {
                                    file_index,
                                    error,
                                });
                            }
                        }
                    }
                })
                .map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to start gateway persisted scan worker index=[{}] error=[{}]",
                        worker_index, error
                    ),
                })?;
            workers.push(worker);
        }
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(workers),
        })
    }

    fn submit(&self, job: GatewayPersistedScanJob) -> Result<()> {
        self.sender
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| ZippyError::Io {
                reason: "gateway persisted scan pool is stopped".to_string(),
            })?
            .send(GatewayPersistedScanPoolMessage::Work(
                GatewayPersistedScanWork::Start(job),
            ))
            .map_err(|_| ZippyError::Io {
                reason: "gateway persisted scan pool disconnected".to_string(),
            })
    }
}

impl Drop for GatewayPersistedScanPool {
    fn drop(&mut self) {
        let worker_count = self.workers.lock().unwrap().len();
        if let Some(sender) = self.sender.lock().unwrap().take() {
            for _ in 0..worker_count {
                let _ = sender.send(GatewayPersistedScanPoolMessage::Stop);
            }
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            let _ = worker.join();
        }
    }
}

impl GatewayCollectStreamMetrics {
    fn from_collect_metrics(value: &Value) -> Self {
        let mut metrics = Self {
            streaming: true,
            ..Self::default()
        };
        if let Some(object) = value.as_object() {
            metrics.scanned_files = object
                .get("scanned_files")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.scanned_file_paths = object
                .get("scanned_file_paths")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            metrics.scanned_rows = object
                .get("scanned_rows")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.returned_rows = object
                .get("returned_rows")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.row_range_pushdown = object
                .get("row_range_pushdown")
                .and_then(Value::as_str)
                .and_then(gateway_row_range_metric_name);
            metrics.scan_elapsed_ms = object
                .get("elapsed_ms")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            metrics.materialized_live_batches = object
                .get("scanned_live_rows")
                .and_then(Value::as_u64)
                .filter(|value| *value > 0)
                .map(|_| 1usize)
                .unwrap_or_default();
            metrics.segment_streamed_batches = object
                .get("segment_streamed_batches")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.segment_streamed_rows = object
                .get("segment_streamed_rows")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.file_scan_parallelism = object
                .get("file_scan_parallelism")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.max_in_flight_file_scans = object
                .get("max_in_flight_file_scans")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.max_pending_file_results = object
                .get("max_pending_file_results")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.query_buffer_budget_bytes = object
                .get("query_buffer_budget_bytes")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.file_buffer_budget_bytes = object
                .get("file_buffer_budget_bytes")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.buffered_bytes = object
                .get("buffered_bytes")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.max_buffered_bytes = object
                .get("max_buffered_bytes")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.buffer_wait_elapsed_ms = object
                .get("buffer_wait_elapsed_ms")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            metrics.global_buffer_wait_elapsed_ms = object
                .get("global_buffer_wait_elapsed_ms")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            metrics.first_chunk_elapsed_ms = object
                .get("first_chunk_elapsed_ms")
                .and_then(Value::as_f64)
                .unwrap_or_default();
        }
        metrics
    }

    fn value(&self) -> Value {
        json!({
            "streaming": self.streaming,
            "scanned_files": self.scanned_files,
            "scanned_file_paths": self.scanned_file_paths,
            "scanned_rows": self.scanned_rows,
            "returned_rows": self.returned_rows,
            "row_range_pushdown": self.row_range_pushdown,
            "scan_elapsed_ms": self.scan_elapsed_ms,
            "filter_elapsed_ms": self.filter_elapsed_ms,
            "encode_elapsed_ms": self.encode_elapsed_ms,
            "write_elapsed_ms": self.write_elapsed_ms,
            "file_scan_parallelism": self.file_scan_parallelism,
            "max_in_flight_file_scans": self.max_in_flight_file_scans,
            "max_pending_file_results": self.max_pending_file_results,
            "query_buffer_budget_bytes": self.query_buffer_budget_bytes,
            "file_buffer_budget_bytes": self.file_buffer_budget_bytes,
            "buffered_bytes": self.buffered_bytes,
            "max_buffered_bytes": self.max_buffered_bytes,
            "buffer_wait_elapsed_ms": self.buffer_wait_elapsed_ms,
            "global_buffer_wait_elapsed_ms": self.global_buffer_wait_elapsed_ms,
            "first_chunk_elapsed_ms": self.first_chunk_elapsed_ms,
            "materialized_live_batches": self.materialized_live_batches,
            "segment_streamed_batches": self.segment_streamed_batches,
            "segment_streamed_rows": self.segment_streamed_rows,
        })
    }
}

enum GatewayPersistedBatchSource {
    Materialized(VecDeque<RecordBatch>),
    Streaming(GatewayPersistedStreamScanner),
}

impl GatewayPersistedBatchSource {
    fn next_batch(
        &mut self,
        metrics: &mut GatewayCollectStreamMetrics,
    ) -> Result<Option<GatewayPersistedBufferedBatch>> {
        match self {
            Self::Materialized(batches) => Ok(batches
                .pop_front()
                .map(GatewayPersistedBufferedBatch::materialized)),
            Self::Streaming(scanner) => scanner.next_batch(metrics),
        }
    }
}

struct GatewayPersistedFileStream {
    file_index: usize,
    file_path: String,
    event_receiver: mpsc::Receiver<GatewayPersistedScanEvent>,
}

struct GatewayPersistedStreamScanner {
    pool: Arc<GatewayPersistedScanPool>,
    tasks: VecDeque<GatewayPersistedScanTask>,
    file_streams: VecDeque<GatewayPersistedFileStream>,
    cancelled: Arc<AtomicBool>,
    parallelism: usize,
    total_files: usize,
    next_file_index: usize,
    output_projection_columns: Option<Vec<String>>,
    query_buffer_budget_bytes: usize,
    file_buffer_budget_bytes: usize,
    buffered_bytes_tracker: Arc<GatewayBufferedBytesTracker>,
    _global_buffer_reservation: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GatewayPersistedStreamScanner {
    fn new(
        stream: &StreamInfo,
        scan_pushdown: &GatewayScanPushdown,
        output_projection_columns: Option<&[String]>,
        pool: Arc<GatewayPersistedScanPool>,
        parallelism: usize,
        query_buffer_budget_bytes: usize,
    ) -> Result<Self> {
        if parallelism == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "gateway persisted scan parallelism must be positive".to_string(),
            });
        }
        let file_paths = matching_non_overlapping_persisted_files(stream, scan_pushdown)
            .into_iter()
            .map(persisted_file_path)
            .collect::<Result<Vec<_>>>()?;
        let tasks = file_paths
            .iter()
            .enumerate()
            .map(|(file_index, file_path)| GatewayPersistedScanTask {
                file_index,
                file_path: file_path.clone(),
                projection_columns: scan_pushdown.projection_columns.clone(),
                scan_pushdown: scan_pushdown.clone(),
            })
            .collect::<VecDeque<_>>();
        if query_buffer_budget_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "gateway query buffer budget must be positive".to_string(),
            });
        }
        let active_file_limit = parallelism.min(file_paths.len()).max(1);
        let file_buffer_budget_bytes = query_buffer_budget_bytes / active_file_limit;
        if file_buffer_budget_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "gateway query buffer budget is smaller than active file slots query_buffer_budget_bytes=[{}] active_file_slots=[{}]",
                    query_buffer_budget_bytes, active_file_limit
                ),
            });
        }
        Ok(Self {
            pool,
            tasks,
            total_files: file_paths.len(),
            file_streams: VecDeque::new(),
            cancelled: Arc::new(AtomicBool::new(false)),
            parallelism,
            next_file_index: 0,
            output_projection_columns: output_projection_columns.map(<[String]>::to_vec),
            query_buffer_budget_bytes,
            file_buffer_budget_bytes,
            buffered_bytes_tracker: Arc::new(GatewayBufferedBytesTracker::default()),
            _global_buffer_reservation: None,
        })
    }

    fn global_buffer_reservation_bytes(&self) -> usize {
        if self.total_files == 0 {
            0
        } else {
            self.query_buffer_budget_bytes
        }
    }

    fn attach_global_buffer_reservation(&mut self, permit: tokio::sync::OwnedSemaphorePermit) {
        self._global_buffer_reservation = Some(permit);
    }

    fn next_batch(
        &mut self,
        metrics: &mut GatewayCollectStreamMetrics,
    ) -> Result<Option<GatewayPersistedBufferedBatch>> {
        loop {
            self.buffered_bytes_tracker.update_metrics(metrics);
            self.schedule_available(metrics)?;
            let Some(file_stream) = self.file_streams.front() else {
                debug_assert_eq!(self.next_file_index, self.total_files);
                self._global_buffer_reservation.take();
                self.buffered_bytes_tracker.update_metrics(metrics);
                return Ok(None);
            };
            if file_stream.file_index != self.next_file_index {
                return Err(ZippyError::Io {
                    reason: format!(
                        "streaming collect file stream order mismatch expected=[{}] actual=[{}]",
                        self.next_file_index, file_stream.file_index
                    ),
                });
            }

            let wait_started = Instant::now();
            let event = file_stream
                .event_receiver
                .recv()
                .map_err(|_| ZippyError::Io {
                    reason: format!(
                        "gateway persisted scan event channel disconnected file_index=[{}]",
                        self.next_file_index
                    ),
                })?;
            metrics.scan_elapsed_ms += wait_started.elapsed().as_secs_f64() * 1000.0;
            match event {
                GatewayPersistedScanEvent::Batch(batch) => {
                    self.buffered_bytes_tracker.update_metrics(metrics);
                    return Ok(Some(batch));
                }
                GatewayPersistedScanEvent::End {
                    file_index,
                    scanned_rows,
                } => {
                    if file_index != self.next_file_index {
                        return Err(ZippyError::Io {
                            reason: format!(
                                "streaming collect file end index mismatch expected=[{}] actual=[{}]",
                                self.next_file_index, file_index
                            ),
                        });
                    }
                    let completed = self.file_streams.pop_front().unwrap();
                    metrics.scanned_rows = metrics.scanned_rows.saturating_add(scanned_rows);
                    metrics.scanned_file_paths.push(completed.file_path);
                    metrics.scanned_files = metrics.scanned_file_paths.len();
                    self.next_file_index += 1;
                }
                GatewayPersistedScanEvent::Error { file_index, error } => {
                    if file_index != self.next_file_index {
                        return Err(ZippyError::Io {
                            reason: format!(
                                "streaming collect file error index mismatch expected=[{}] actual=[{}]",
                                self.next_file_index, file_index
                            ),
                        });
                    }
                    return Err(error);
                }
            }
        }
    }

    fn schedule_available(&mut self, metrics: &mut GatewayCollectStreamMetrics) -> Result<()> {
        while self.file_streams.len() < self.parallelism {
            let Some(task) = self.tasks.pop_front() else {
                break;
            };
            let file_index = task.file_index;
            let file_path = task.file_path.clone();
            let (event_sender, event_receiver) = mpsc::sync_channel(1);
            let buffer_budget = Arc::new(GatewayFileByteBudget::new(
                self.file_buffer_budget_bytes,
                Arc::clone(&self.buffered_bytes_tracker),
            )?);
            self.pool.submit(GatewayPersistedScanJob {
                task,
                output_projection_columns: self.output_projection_columns.clone(),
                event_sender,
                buffer_budget,
                cancelled: Arc::clone(&self.cancelled),
            })?;
            self.file_streams.push_back(GatewayPersistedFileStream {
                file_index,
                file_path,
                event_receiver,
            });
        }
        metrics.max_in_flight_file_scans = metrics
            .max_in_flight_file_scans
            .max(self.file_streams.len());
        metrics.max_pending_file_results = metrics
            .max_pending_file_results
            .max(self.file_streams.len().saturating_sub(1));
        metrics.query_buffer_budget_bytes = self.query_buffer_budget_bytes;
        metrics.file_buffer_budget_bytes = self.file_buffer_budget_bytes;
        Ok(())
    }
}

impl Drop for GatewayPersistedStreamScanner {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

enum GatewayCollectStreamProducer {
    Materialized {
        batch: RecordBatch,
        chunk_rows: usize,
        offset: usize,
        metrics: GatewayCollectStreamMetrics,
    },
    Persisted {
        schema: SchemaRef,
        source: GatewayPersistedBatchSource,
        chunk_rows: usize,
        current_batch: Option<GatewayPersistedBufferedBatch>,
        current_offset: usize,
        skip_rows: usize,
        remaining_rows: Option<usize>,
        metrics: GatewayCollectStreamMetrics,
    },
    Segment {
        schema: SchemaRef,
        readers: VecDeque<RowSpanBatchReader>,
        scan_pushdown: GatewayScanPushdown,
        output_projection_columns: Option<Vec<String>>,
        metrics: GatewayCollectStreamMetrics,
    },
}

impl GatewayCollectStreamProducer {
    fn materialized(batch: RecordBatch, chunk_rows: usize) -> Self {
        Self::Materialized {
            batch,
            chunk_rows: chunk_rows.max(1),
            offset: 0,
            metrics: GatewayCollectStreamMetrics::default(),
        }
    }

    fn materialized_with_metrics(
        batch: RecordBatch,
        chunk_rows: usize,
        metrics: GatewayCollectStreamMetrics,
    ) -> Self {
        let mut producer = Self::materialized(batch, chunk_rows);
        if let Self::Materialized {
            metrics: stored_metrics,
            ..
        } = &mut producer
        {
            *stored_metrics = metrics;
        }
        producer
    }

    fn persisted_serial(
        stream: &StreamInfo,
        scan_pushdown: &GatewayScanPushdown,
        output_projection_columns: Option<&[String]>,
        row_range_pushdown: Option<GatewayRowRangePushdown>,
        chunk_rows: usize,
        scan_resources: GatewayPersistedScanResources,
    ) -> Result<Self> {
        let GatewayPersistedScanResources {
            pool,
            parallelism,
            query_buffer_budget_bytes,
        } = scan_resources;
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let scan_schema = schema_for_scan_pushdown(&schema, scan_pushdown)?;
        let output_schema = schema_for_projection_columns(&scan_schema, output_projection_columns)?;
        if row_range_pushdown.is_none() {
            let scanner = GatewayPersistedStreamScanner::new(
                stream,
                scan_pushdown,
                output_projection_columns,
                pool,
                parallelism,
                query_buffer_budget_bytes,
            )?;
            return Ok(Self::Persisted {
                schema: output_schema,
                source: GatewayPersistedBatchSource::Streaming(scanner),
                chunk_rows: chunk_rows.max(1),
                current_batch: None,
                current_offset: 0,
                skip_rows: 0,
                remaining_rows: None,
                metrics: GatewayCollectStreamMetrics {
                    streaming: true,
                    file_scan_parallelism: parallelism,
                    ..GatewayCollectStreamMetrics::default()
                },
            });
        }

        let scan_started = Instant::now();
        let mut scanned_rows = 0usize;
        let mut scanned_files = Vec::new();
        let (skip_rows, remaining_rows, batches) = match row_range_pushdown {
            Some(GatewayRowRangePushdown::Tail(n)) => (
                0,
                Some(n),
                tail_persisted_file_record_batches(
                    stream,
                    n,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                )?,
            ),
            Some(GatewayRowRangePushdown::Head(n)) => (
                0,
                None,
                head_persisted_file_record_batches(
                    stream,
                    n,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                )?,
            ),
            Some(GatewayRowRangePushdown::Slice { offset, length }) => {
                let (batches, _, _) = slice_persisted_file_record_batches(
                    stream,
                    offset,
                    length,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                )?;
                (0, None, batches)
            }
            None => unreachable!("non-row-range persisted scans return above"),
        };
        let batches = batches
            .into_iter()
            .map(|batch| project_record_batch(&batch, output_projection_columns))
            .collect::<Result<Vec<_>>>()?;
        let metrics = GatewayCollectStreamMetrics {
            streaming: true,
            scanned_files: scanned_files.len(),
            scanned_file_paths: scanned_files,
            scanned_rows,
            row_range_pushdown: row_range_pushdown.map(GatewayRowRangePushdown::op_name),
            scan_elapsed_ms: scan_started.elapsed().as_secs_f64() * 1000.0,
            ..GatewayCollectStreamMetrics::default()
        };
        Ok(Self::Persisted {
            schema: output_schema,
            source: GatewayPersistedBatchSource::Materialized(batches.into()),
            chunk_rows: chunk_rows.max(1),
            current_batch: None,
            current_offset: 0,
            skip_rows,
            remaining_rows,
            metrics,
        })
    }

    fn segment(
        schema: SchemaRef,
        spans: Vec<RowSpanView>,
        scan_pushdown: GatewayScanPushdown,
        output_projection_columns: Option<Vec<String>>,
        chunk_rows: usize,
    ) -> Result<Self> {
        let scan_schema = schema_for_scan_pushdown(&schema, &scan_pushdown)?;
        let mut readers = VecDeque::with_capacity(spans.len());
        for span in spans {
            let reader = span
                .batch_reader(chunk_rows, scan_pushdown.projection_columns.clone())
                .map_err(|error| ZippyError::Io {
                    reason: error.to_string(),
                })?;
            readers.push_back(reader);
        }
        Ok(Self::Segment {
            schema: schema_for_projection_columns(
                &scan_schema,
                output_projection_columns.as_deref(),
            )?,
            readers,
            scan_pushdown,
            output_projection_columns,
            metrics: GatewayCollectStreamMetrics {
                streaming: true,
                ..GatewayCollectStreamMetrics::default()
            },
        })
    }

    fn schema(&self) -> SchemaRef {
        match self {
            Self::Materialized { batch, .. } => batch.schema(),
            Self::Persisted { schema, .. } => Arc::clone(schema),
            Self::Segment { schema, .. } => Arc::clone(schema),
        }
    }

    fn global_buffer_reservation_bytes(&self) -> usize {
        match self {
            Self::Persisted {
                source: GatewayPersistedBatchSource::Streaming(scanner),
                ..
            } => scanner.global_buffer_reservation_bytes(),
            _ => 0,
        }
    }

    fn attach_global_buffer_reservation(
        &mut self,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<()> {
        match self {
            Self::Persisted {
                source: GatewayPersistedBatchSource::Streaming(scanner),
                ..
            } => {
                scanner.attach_global_buffer_reservation(permit);
                Ok(())
            }
            _ => Err(ZippyError::InvalidConfig {
                reason: "gateway buffer reservation requires a persisted streaming producer"
                    .to_string(),
            }),
        }
    }

    fn metrics(&self) -> Value {
        match self {
            Self::Materialized { metrics, .. } => metrics.value(),
            Self::Persisted { metrics, .. } => metrics.value(),
            Self::Segment { metrics, .. } => metrics.value(),
        }
    }

    fn metrics_mut(&mut self) -> &mut GatewayCollectStreamMetrics {
        match self {
            Self::Materialized { metrics, .. } => metrics,
            Self::Persisted { metrics, .. } => metrics,
            Self::Segment { metrics, .. } => metrics,
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            Self::Materialized {
                batch,
                chunk_rows,
                offset,
                ..
            } => {
                if *offset >= batch.num_rows() {
                    return Ok(None);
                }
                let rows = (*chunk_rows).min(batch.num_rows() - *offset);
                let chunk = batch.slice(*offset, rows);
                *offset += rows;
                Ok(Some(chunk))
            }
            Self::Persisted {
                source,
                chunk_rows,
                current_batch,
                current_offset,
                skip_rows,
                remaining_rows,
                metrics,
                ..
            } => loop {
                if remaining_rows.is_some_and(|remaining| remaining == 0) {
                    return Ok(None);
                }
                if current_batch
                    .as_ref()
                    .is_none_or(|batch| *current_offset >= batch.batch.num_rows())
                {
                    current_batch.take();
                    *current_batch = source.next_batch(metrics)?;
                    *current_offset = 0;
                }
                let Some(batch) = current_batch.as_ref() else {
                    return Ok(None);
                };
                if batch.batch.num_rows() == 0 {
                    *current_batch = None;
                    continue;
                }
                let available_rows = batch.batch.num_rows() - *current_offset;
                if *skip_rows >= available_rows {
                    *skip_rows -= available_rows;
                    *current_offset = batch.batch.num_rows();
                    continue;
                }
                let start = *current_offset + *skip_rows;
                let available_rows = batch.batch.num_rows() - start;
                *skip_rows = 0;
                let mut rows = (*chunk_rows).min(available_rows);
                if let Some(remaining) = remaining_rows.as_mut() {
                    rows = rows.min(*remaining);
                    *remaining = remaining.saturating_sub(rows);
                }
                if rows == 0 {
                    return Ok(None);
                }
                let chunk = batch.batch.slice(start, rows);
                *current_offset = start + rows;
                metrics.returned_rows = metrics.returned_rows.saturating_add(rows);
                return Ok(Some(chunk));
            },
            Self::Segment {
                readers,
                scan_pushdown,
                output_projection_columns,
                metrics,
                ..
            } => loop {
                let Some(reader) = readers.front_mut() else {
                    return Ok(None);
                };
                let Some(mut batch) = reader.next_batch().map_err(|error| ZippyError::Io {
                    reason: error.to_string(),
                })?
                else {
                    readers.pop_front();
                    continue;
                };
                metrics.segment_streamed_batches =
                    metrics.segment_streamed_batches.saturating_add(1);
                metrics.segment_streamed_rows = metrics
                    .segment_streamed_rows
                    .saturating_add(batch.num_rows());
                metrics.scanned_rows = metrics.scanned_rows.saturating_add(batch.num_rows());
                for filter in &scan_pushdown.filters {
                    batch = apply_filter(batch, filter)?;
                }
                batch = project_record_batch(&batch, output_projection_columns.as_deref())?;
                if batch.num_rows() == 0 {
                    continue;
                }
                metrics.returned_rows = metrics.returned_rows.saturating_add(batch.num_rows());
                return Ok(Some(batch));
            },
        }
    }
}

struct GatewayScannedBatch {
    batch: RecordBatch,
    scanned_rows: usize,
    scanned_live_rows: usize,
    scanned_files: Vec<String>,
}

struct GatewaySnapshot {
    source: String,
    stream: Arc<StreamInfo>,
    active_descriptor: Option<Value>,
    active_committed_row_high_watermark: Option<usize>,
    last_accessed_at: Instant,
    _leases: Vec<SegmentReaderLeaseGuard>,
}

#[derive(Clone)]
struct GatewaySnapshotView {
    source: String,
    stream: Arc<StreamInfo>,
    active_descriptor: Option<Value>,
    active_committed_row_high_watermark: Option<usize>,
}

fn expire_gateway_snapshots(
    snapshots: &mut HashMap<String, GatewaySnapshot>,
    now: Instant,
) -> usize {
    let before = snapshots.len();
    snapshots.retain(|_, snapshot| {
        now.checked_duration_since(snapshot.last_accessed_at)
            .unwrap_or_default()
            < GATEWAY_SNAPSHOT_IDLE_TIMEOUT
    });
    before.saturating_sub(snapshots.len())
}

#[derive(Clone)]
struct GatewayMasterProcess {
    process_id: String,
    process_token: String,
}

struct GatewayAsyncMasterClient {
    endpoint: ControlEndpoint,
    process: Mutex<Option<GatewayMasterProcess>>,
    async_register_lock: tokio::sync::Mutex<()>,
    metrics: Arc<Mutex<GatewayMetrics>>,
}

struct GatewayFrameHeader {
    header: Value,
    payload_len: usize,
}

#[derive(Clone)]
struct GatewaySubscribeRequest {
    mode: GatewaySubscribeMode,
    source: String,
    filter: Option<Value>,
    instrument_ids: Option<HashSet<String>>,
    batch_size: Option<usize>,
    count: Option<usize>,
    throttle: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GatewaySubscribeMode {
    Rows,
    Table,
}

struct GatewayActiveSubscribeDriver {
    request: GatewaySubscribeRequest,
    master: Arc<GatewayAsyncMasterClient>,
    reader: ActiveSegmentReader,
    segment_schema: CompiledSchema,
    descriptor_generation: u64,
    pending: VecDeque<GatewaySubscribeEvent>,
    table_pending: Vec<RecordBatch>,
    table_pending_rows: usize,
    table_last_emit_at: Instant,
    _lease: SegmentReaderLeaseGuard,
}

enum GatewaySubscribeEvent {
    Idle,
    Row(Value),
    Table(RecordBatch),
}

struct GatewaySubscribeStep {
    driver: GatewayActiveSubscribeDriver,
    event: GatewaySubscribeEvent,
}

#[derive(Clone, Copy)]
enum GatewayRowRangePushdown {
    Tail(usize),
    Head(usize),
    Slice {
        offset: usize,
        length: Option<usize>,
    },
}

impl GatewayRowRangePushdown {
    fn op_name(self) -> &'static str {
        match self {
            Self::Tail(_) => "tail",
            Self::Head(_) => "head",
            Self::Slice { .. } => "slice",
        }
    }

    fn to_plan_op(self) -> Value {
        match self {
            Self::Tail(n) => json!({"op": "tail", "n": n}),
            Self::Head(n) => json!({"op": "head", "n": n}),
            Self::Slice { offset, length } => {
                json!({"op": "slice", "offset": offset, "length": length})
            }
        }
    }
}

struct SegmentReaderLeaseGuard {
    master: Arc<GatewayAsyncMasterClient>,
    source: String,
    lease_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct GatewayMetrics {
    requests_total: u64,
    auth_failures_total: u64,
    errors_total: u64,
    write_batches_total: u64,
    written_rows_total: u64,
    write_rejections_total: u64,
    writer_map_lock_wait_count: u64,
    writer_map_lock_wait_last_ms: f64,
    writer_map_lock_wait_total_ms: f64,
    writer_lock_wait_count: u64,
    writer_lock_wait_last_ms: f64,
    writer_lock_wait_total_ms: f64,
    writer_create_count: u64,
    writer_create_last_ms: f64,
    writer_create_total_ms: f64,
    writer_on_data_count: u64,
    writer_on_data_last_ms: f64,
    writer_on_data_total_ms: f64,
    writer_flush_count: u64,
    writer_flush_last_ms: f64,
    writer_flush_total_ms: f64,
    writer_descriptor_publish_count: u64,
    writer_descriptor_publish_last_ms: f64,
    writer_descriptor_publish_total_ms: f64,
    collect_requests_total: u64,
    subscribe_clients_total: u64,
    subscribe_rows_delivered_total: u64,
    subscribe_tables_delivered_total: u64,
    subscribe_table_rows_delivered_total: u64,
    subscribe_write_timeouts_total: u64,
    subscribe_slow_clients_total: u64,
    subscribe_last_close_reason: Option<String>,
    subscribe_last_write_elapsed_ms: f64,
    subscribe_write_elapsed_ms_total: f64,
    master_async_requests_total: u64,
    master_process_reregistrations_total: u64,
    connections_active: u64,
    connections_rejected_total: u64,
    blocking_requests_active: u64,
    blocking_requests_rejected_total: u64,
    subscribe_clients_active: u64,
    subscribe_clients_rejected_total: u64,
    request_timeouts_total: u64,
    payload_timeouts_total: u64,
    snapshots_expired_total: u64,
    snapshots_evicted_total: u64,
}

struct MasterDescriptorPublisher {
    master: Arc<GatewayAsyncMasterClient>,
    metrics: Arc<Mutex<GatewayMetrics>>,
    stream_name: String,
}

impl GatewayServer {
    /// Create a native GatewayServer.
    pub fn new(config: GatewayServerConfig) -> Result<Self> {
        Self::new_with_query_limits(
            config,
            DEFAULT_MAX_PARALLELISM,
            DEFAULT_MAX_PARALLELISM,
            DEFAULT_MAX_QUERY_BUFFER_BYTES,
            DEFAULT_QUERY_MAX_BUFFERED_BYTES,
        )
    }

    /// Create a native GatewayServer with one shared query and runtime limit.
    pub fn new_with_query_parallelism(
        config: GatewayServerConfig,
        query_parallelism: usize,
    ) -> Result<Self> {
        Self::new_with_query_limits(
            config,
            query_parallelism,
            query_parallelism,
            DEFAULT_MAX_QUERY_BUFFER_BYTES,
            DEFAULT_QUERY_MAX_BUFFERED_BYTES,
        )
    }

    /// Create a native GatewayServer with process-wide and query-specific limits.
    pub fn new_with_parallelism(
        config: GatewayServerConfig,
        max_parallelism: usize,
        query_parallelism: usize,
    ) -> Result<Self> {
        Self::new_with_query_limits(
            config,
            max_parallelism,
            query_parallelism,
            DEFAULT_MAX_QUERY_BUFFER_BYTES,
            DEFAULT_QUERY_MAX_BUFFERED_BYTES,
        )
    }

    /// Create a native GatewayServer with CPU and buffered-query limits.
    pub fn new_with_query_limits(
        config: GatewayServerConfig,
        max_parallelism: usize,
        query_parallelism: usize,
        max_query_buffer_bytes: usize,
        query_max_buffered_bytes: usize,
    ) -> Result<Self> {
        if max_parallelism == 0 || max_parallelism > MAX_PARALLELISM {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "max_parallelism must be between 1 and {} value=[{}]",
                    MAX_PARALLELISM, max_parallelism
                ),
            });
        }
        if query_parallelism == 0 || query_parallelism > max_parallelism {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "query parallelism must be between 1 and max_parallelism value=[{}] max_parallelism=[{}]",
                    query_parallelism, max_parallelism
                ),
            });
        }
        let max_supported_query_buffer_bytes =
            MAX_QUERY_BUFFER_BYTES.min(tokio::sync::Semaphore::MAX_PERMITS);
        if max_query_buffer_bytes == 0 || max_query_buffer_bytes > max_supported_query_buffer_bytes
        {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "max_query_buffer_bytes must be between 1 and {} value=[{}]",
                    max_supported_query_buffer_bytes, max_query_buffer_bytes
                ),
            });
        }
        if query_max_buffered_bytes == 0 || query_max_buffered_bytes > max_query_buffer_bytes {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "query max_buffered_bytes must be between 1 and max_query_buffer_bytes value=[{}] max_query_buffer_bytes=[{}]",
                    query_max_buffered_bytes, max_query_buffer_bytes
                ),
            });
        }
        if let Some(max_write_rows) = config.max_write_rows {
            if max_write_rows == 0 {
                return Err(ZippyError::InvalidConfig {
                    reason: "max_write_rows must be positive".to_string(),
                });
            }
        }
        let max_connections = positive_gateway_limit(
            "max_connections",
            config.max_connections,
            DEFAULT_MAX_GATEWAY_CONNECTIONS,
        )?;
        let max_subscribers = positive_gateway_limit(
            "max_subscribers",
            config.max_subscribers,
            DEFAULT_MAX_GATEWAY_SUBSCRIBERS,
        )?;
        let max_blocking_requests = positive_gateway_limit(
            "max_blocking_requests",
            config.max_blocking_requests,
            DEFAULT_MAX_GATEWAY_BLOCKING_REQUESTS,
        )?;
        let write_timeout = positive_gateway_duration_ms(
            "write_timeout_ms",
            config.write_timeout_ms,
            DEFAULT_GATEWAY_WRITE_TIMEOUT,
        )?;
        let metrics = Arc::new(Mutex::new(GatewayMetrics::default()));
        let persisted_scan_pool = Arc::new(GatewayPersistedScanPool::new(max_parallelism)?);
        let persisted_buffer_limit = Arc::new(tokio::sync::Semaphore::new(max_query_buffer_bytes));
        let master = Arc::new(GatewayAsyncMasterClient::new(
            config.master_endpoint.clone(),
            Arc::clone(&metrics),
        ));
        Ok(Self {
            endpoint: normalize_endpoint(&config.endpoint),
            state: Arc::new(GatewayState {
                master,
                token: config.token,
                max_write_rows: config.max_write_rows,
                writers: Mutex::new(BTreeMap::new()),
                subscribe_catalog_notifier: GatewaySubscribeCatalogNotifier::default(),
                snapshots: Mutex::new(HashMap::new()),
                snapshot_counter: AtomicU64::new(0),
                metrics,
                stopped: AtomicBool::new(false),
                connection_limit: Arc::new(tokio::sync::Semaphore::new(max_connections)),
                blocking_limit: Arc::new(tokio::sync::Semaphore::new(max_blocking_requests)),
                subscriber_limit: Arc::new(tokio::sync::Semaphore::new(max_subscribers)),
                max_connections,
                max_subscribers,
                max_blocking_requests,
                max_parallelism,
                query_parallelism,
                max_query_buffer_bytes,
                query_max_buffered_bytes,
                persisted_scan_pool,
                persisted_buffer_limit,
                write_timeout,
            }),
            runtime: None,
        })
    }

    /// Start serving TCP requests in a background thread.
    pub fn start(mut self) -> Result<Self> {
        let listener = StdTcpListener::bind(&self.endpoint).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to bind gateway endpoint=[{}] error=[{}]",
                self.endpoint, error
            ),
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to set gateway listener nonblocking error=[{}]",
                    error
                ),
            })?;
        self.endpoint = listener
            .local_addr()
            .map_err(|error| ZippyError::Io {
                reason: format!("failed to read gateway local addr error=[{}]", error),
            })?
            .to_string();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.state.max_parallelism)
            .enable_io()
            .enable_time()
            .thread_name("zippy-gateway")
            .build()
            .map_err(|error| ZippyError::Io {
                reason: format!("failed to build gateway tokio runtime error=[{}]", error),
            })?;
        let listener = {
            let _runtime_guard = runtime.enter();
            tokio::net::TcpListener::from_std(listener).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to create tokio gateway listener endpoint=[{}] error=[{}]",
                    self.endpoint, error
                ),
            })?
        };
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let state = Arc::clone(&self.state);
        let join_handle = thread::spawn(move || {
            runtime.block_on(async move {
                async_serve_loop(listener, state, shutdown_rx).await;
            });
        });
        self.runtime = Some(GatewayRuntime {
            shutdown,
            join_handle,
        });
        Ok(self)
    }

    /// Return the effective listening endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Return GatewayServer metrics as JSON.
    pub fn metrics(&self) -> Value {
        let metrics = self.state.metrics.lock().unwrap().clone();
        let mut value = gateway_metrics_json(&metrics);
        let fields = value.as_object_mut().unwrap();
        insert_json_metric(fields, "endpoint", self.endpoint.clone());
        insert_json_metric(
            fields,
            "running",
            !self.state.stopped.load(Ordering::SeqCst),
        );
        insert_json_metric(fields, "max_connections", self.state.max_connections);
        insert_json_metric(
            fields,
            "max_blocking_requests",
            self.state.max_blocking_requests,
        );
        insert_json_metric(fields, "max_subscribers", self.state.max_subscribers);
        insert_json_metric(fields, "max_parallelism", self.state.max_parallelism);
        insert_json_metric(fields, "query_parallelism", self.state.query_parallelism);
        insert_json_metric(
            fields,
            "max_query_buffer_bytes",
            self.state.max_query_buffer_bytes,
        );
        insert_json_metric(
            fields,
            "query_max_buffered_bytes",
            self.state.query_max_buffered_bytes,
        );
        insert_json_metric(
            fields,
            "query_buffer_reserved_bytes",
            self.state.max_query_buffer_bytes
                - self.state.persisted_buffer_limit.available_permits(),
        );
        insert_json_metric(
            fields,
            "write_timeout_ms",
            self.state.write_timeout.as_millis() as u64,
        );
        insert_json_metric(
            fields,
            "snapshots_active",
            self.state.snapshots.lock().unwrap().len(),
        );
        value
    }

    /// Stop the background gateway listener and flush table writers.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        if let Some(runtime) = self.runtime.as_ref() {
            let _ = runtime.shutdown.send(true);
        }
        let _ = TcpStream::connect(&self.endpoint);
        if let Some(runtime) = self.runtime.take() {
            let _ = runtime.join_handle.join();
        }
        self.state.close_all_writers();
        self.state.snapshots.lock().unwrap().clear();
    }
}

impl Drop for GatewayServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn positive_gateway_limit(name: &str, configured: Option<usize>, default: usize) -> Result<usize> {
    let value = configured.unwrap_or(default);
    if value == 0 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("{} must be positive", name),
        });
    }
    Ok(value)
}

fn elapsed_ms(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1000.0
}

fn insert_json_metric<T: Serialize>(metrics: &mut Map<String, Value>, name: &str, value: T) {
    metrics.insert(name.to_string(), json!(value));
}

fn gateway_metrics_json(metrics: &GatewayMetrics) -> Value {
    let mut value = Map::new();
    macro_rules! metric_fields {
        ($($field:ident),+ $(,)?) => {
            $(
                insert_json_metric(&mut value, stringify!($field), &metrics.$field);
            )+
        };
    }
    metric_fields!(
        requests_total,
        auth_failures_total,
        errors_total,
        write_batches_total,
        written_rows_total,
        write_rejections_total,
        writer_map_lock_wait_count,
        writer_map_lock_wait_last_ms,
        writer_map_lock_wait_total_ms,
        writer_lock_wait_count,
        writer_lock_wait_last_ms,
        writer_lock_wait_total_ms,
        writer_create_count,
        writer_create_last_ms,
        writer_create_total_ms,
        writer_on_data_count,
        writer_on_data_last_ms,
        writer_on_data_total_ms,
        writer_flush_count,
        writer_flush_last_ms,
        writer_flush_total_ms,
        writer_descriptor_publish_count,
        writer_descriptor_publish_last_ms,
        writer_descriptor_publish_total_ms,
        collect_requests_total,
        subscribe_clients_total,
        subscribe_rows_delivered_total,
        subscribe_tables_delivered_total,
        subscribe_table_rows_delivered_total,
        subscribe_write_timeouts_total,
        subscribe_slow_clients_total,
        subscribe_last_close_reason,
        subscribe_last_write_elapsed_ms,
        subscribe_write_elapsed_ms_total,
        master_async_requests_total,
        master_process_reregistrations_total,
        connections_active,
        connections_rejected_total,
        blocking_requests_active,
        blocking_requests_rejected_total,
        subscribe_clients_active,
        subscribe_clients_rejected_total,
        request_timeouts_total,
        payload_timeouts_total,
        snapshots_expired_total,
        snapshots_evicted_total,
    );
    Value::Object(value)
}

fn positive_gateway_duration_ms(
    name: &str,
    configured: Option<u64>,
    default: Duration,
) -> Result<Duration> {
    let Some(value) = configured else {
        return Ok(default);
    };
    if value == 0 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("{} must be positive", name),
        });
    }
    Ok(Duration::from_millis(value))
}

impl StreamTableDescriptorPublisher for MasterDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> Result<()> {
        let started = Instant::now();
        let result = self
            .master
            .publish_segment_descriptor_bytes_blocking(&self.stream_name, &descriptor_envelope)
            .map(|_| ());
        record_writer_descriptor_publish_elapsed(&self.metrics, started.elapsed());
        result
    }
}

fn record_writer_descriptor_publish_elapsed(
    metrics: &Arc<Mutex<GatewayMetrics>>,
    elapsed: Duration,
) {
    let elapsed_ms = elapsed_ms(elapsed);
    let mut metrics = metrics.lock().unwrap();
    metrics.writer_descriptor_publish_count += 1;
    metrics.writer_descriptor_publish_last_ms = elapsed_ms;
    metrics.writer_descriptor_publish_total_ms += elapsed_ms;
}

impl GatewayAsyncMasterClient {
    fn new(endpoint: ControlEndpoint, metrics: Arc<Mutex<GatewayMetrics>>) -> Self {
        Self {
            endpoint,
            process: Mutex::new(None),
            async_register_lock: tokio::sync::Mutex::new(()),
            metrics,
        }
    }

    async fn heartbeat(&self) -> Result<()> {
        self.with_process_async(|process| {
            ControlRequest::Heartbeat(HeartbeatRequest {
                process_id: process.process_id,
                process_token: Some(process.process_token),
            })
        })
        .await
        .and_then(|response| match response {
            ControlResponse::HeartbeatAccepted { .. } => Ok(()),
            ControlResponse::ShutdownRequested { process_id, reason } => {
                Err(ZippyError::MasterShutdownRequested { process_id, reason })
            }
            other => Err(unexpected_response("HeartbeatAccepted", other)),
        })
    }

    fn register_stream_blocking(
        &self,
        stream_name: &str,
        schema: SchemaRef,
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<()> {
        let schema_hash = canonical_schema_hash(&schema);
        let schema = schema_metadata(&schema);
        self.with_process_blocking(|process| {
            ControlRequest::RegisterStream(RegisterStreamRequest {
                process_id: Some(process.process_id),
                process_token: Some(process.process_token),
                token: None,
                stream_name: stream_name.to_string(),
                schema: schema.clone(),
                schema_hash: schema_hash.clone(),
                buffer_size,
                frame_size,
            })
        })
        .and_then(|response| match response {
            ControlResponse::StreamRegistered { .. } => Ok(()),
            other => Err(unexpected_response("StreamRegistered", other)),
        })
    }

    fn register_source_blocking(
        &self,
        source_name: &str,
        source_type: &str,
        output_stream: &str,
        config: Value,
    ) -> Result<()> {
        self.with_process_blocking(|process| {
            ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: source_name.to_string(),
                source_type: source_type.to_string(),
                process_id: process.process_id,
                process_token: Some(process.process_token),
                output_stream: output_stream.to_string(),
                config: config.clone(),
            })
        })
        .and_then(|response| match response {
            ControlResponse::SourceRegistered { .. } => Ok(()),
            other => Err(unexpected_response("SourceRegistered", other)),
        })
    }

    fn unregister_source_blocking(&self, source_name: &str) -> Result<()> {
        self.with_process_blocking(|process| {
            ControlRequest::UnregisterSource(UnregisterSourceRequest {
                source_name: source_name.to_string(),
                process_id: process.process_id,
                process_token: Some(process.process_token),
            })
        })
        .and_then(|response| match response {
            ControlResponse::SourceUnregistered { .. } => Ok(()),
            other => Err(unexpected_response("SourceUnregistered", other)),
        })
    }

    fn get_stream_blocking(&self, source: &str) -> Result<StreamInfo> {
        self.send_request_blocking(ControlRequest::GetStream(GetStreamRequest {
            stream_name: source.to_string(),
        }))
        .and_then(|response| match response {
            ControlResponse::StreamFetched(response) => Ok(response.stream),
            other => Err(unexpected_response("StreamFetched", other)),
        })
    }

    fn get_stream_status_blocking(&self, source: &str) -> Result<StreamInfo> {
        self.send_request_blocking(ControlRequest::GetStreamStatus(GetStreamRequest {
            stream_name: source.to_string(),
        }))
        .and_then(|response| match response {
            ControlResponse::StreamFetched(response) => Ok(response.stream),
            other => Err(unexpected_response("StreamFetched", other)),
        })
    }

    fn list_streams_blocking(&self) -> Result<Vec<StreamInfo>> {
        self.send_request_blocking(ControlRequest::ListStreams(ListStreamsRequest {}))
            .and_then(|response| match response {
                ControlResponse::StreamsListed(response) => Ok(response.streams),
                other => Err(unexpected_response("StreamsListed", other)),
            })
    }

    fn publish_segment_descriptor_bytes_blocking(
        &self,
        stream_name: &str,
        descriptor: &[u8],
    ) -> Result<()> {
        let descriptor = serde_json::from_slice::<Value>(descriptor).map_err(json_zippy_error)?;
        self.with_process_blocking(|process| {
            ControlRequest::PublishSegmentDescriptor(PublishSegmentDescriptorRequest {
                stream_name: stream_name.to_string(),
                process_id: process.process_id,
                process_token: Some(process.process_token),
                descriptor: descriptor.clone(),
            })
        })
        .and_then(|response| match response {
            ControlResponse::SegmentDescriptorPublished { .. } => Ok(()),
            other => Err(unexpected_response("SegmentDescriptorPublished", other)),
        })
    }

    fn acquire_segment_reader_lease_blocking(
        &self,
        stream_name: &str,
        source_segment_id: u64,
        source_generation: u64,
    ) -> Result<String> {
        self.with_process_blocking(|process| {
            ControlRequest::AcquireSegmentReaderLease(AcquireSegmentReaderLeaseRequest {
                stream_name: stream_name.to_string(),
                process_id: process.process_id,
                process_token: Some(process.process_token),
                source_segment_id,
                source_generation,
            })
        })
        .and_then(|response| match response {
            ControlResponse::SegmentReaderLeaseAcquired { lease_id, .. } => Ok(lease_id),
            other => Err(unexpected_response("SegmentReaderLeaseAcquired", other)),
        })
    }

    fn release_segment_reader_lease_blocking(
        &self,
        stream_name: &str,
        lease_id: &str,
    ) -> Result<()> {
        self.with_process_blocking(|process| {
            ControlRequest::ReleaseSegmentReaderLease(ReleaseSegmentReaderLeaseRequest {
                stream_name: stream_name.to_string(),
                process_id: process.process_id,
                process_token: Some(process.process_token),
                lease_id: lease_id.to_string(),
            })
        })
        .and_then(|response| match response {
            ControlResponse::SegmentReaderLeaseReleased { .. } => Ok(()),
            other => Err(unexpected_response("SegmentReaderLeaseReleased", other)),
        })
    }

    fn wait_segment_descriptor_blocking(
        &self,
        stream_name: &str,
        after_descriptor_generation: u64,
        timeout: Duration,
    ) -> Result<Option<(u64, Value)>> {
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self.with_process_blocking(|process| {
            ControlRequest::Watch(WatchRequest {
                process_id: process.process_id,
                process_token: Some(process.process_token),
                resource: WatchResource::SegmentDescriptor {
                    stream_name: stream_name.to_string(),
                },
                after_revision: after_descriptor_generation,
                timeout_ms,
            })
        })
        .and_then(|response| match response {
            ControlResponse::ResourceChanged { event: None } => Ok(None),
            ControlResponse::ResourceChanged { event: Some(event) } => match event.resource {
                WatchResource::SegmentDescriptor { .. } => {
                    let descriptor = event
                        .payload
                        .get("descriptor")
                        .cloned()
                        .filter(|value| !value.is_null());
                    Ok(descriptor.map(|descriptor| (event.revision, descriptor)))
                }
                other => Err(unexpected_watch_resource("SegmentDescriptor", other)),
            },
            other => Err(unexpected_response("ResourceChanged", other)),
        })
    }

    async fn with_process_async(
        &self,
        request: impl Fn(GatewayMasterProcess) -> ControlRequest,
    ) -> Result<ControlResponse> {
        let mut last_error = None;
        for _ in 0..2 {
            let process = self.ensure_process_async().await?;
            match self.send_request_async(request(process)).await {
                Ok(response) => return Ok(response),
                Err(error) if gateway_master_process_invalid(&error) => {
                    self.clear_process();
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("gateway async master retry must store the last error"))
    }

    fn with_process_blocking(
        &self,
        request: impl Fn(GatewayMasterProcess) -> ControlRequest,
    ) -> Result<ControlResponse> {
        let mut last_error = None;
        for _ in 0..2 {
            let process = self.ensure_process_blocking()?;
            match self.send_request_blocking(request(process)) {
                Ok(response) => return Ok(response),
                Err(error) if gateway_master_process_invalid(&error) => {
                    self.clear_process();
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("gateway blocking master retry must store the last error"))
    }

    async fn ensure_process_async(&self) -> Result<GatewayMasterProcess> {
        if let Some(process) = self.process.lock().unwrap().clone() {
            return Ok(process);
        }
        let _guard = self.async_register_lock.lock().await;
        if let Some(process) = self.process.lock().unwrap().clone() {
            return Ok(process);
        }
        let response = self
            .send_request_async(ControlRequest::RegisterProcess(RegisterProcessRequest {
                app: "zippy_gateway".to_string(),
            }))
            .await?;
        let process = match response {
            ControlResponse::ProcessRegistered {
                process_id,
                process_token,
            } => GatewayMasterProcess {
                process_id,
                process_token,
            },
            other => return Err(unexpected_response("ProcessRegistered", other)),
        };
        self.metrics
            .lock()
            .unwrap()
            .master_process_reregistrations_total += 1;
        *self.process.lock().unwrap() = Some(process.clone());
        Ok(process)
    }

    fn ensure_process_blocking(&self) -> Result<GatewayMasterProcess> {
        let mut guard = self.process.lock().unwrap();
        if let Some(process) = guard.clone() {
            return Ok(process);
        }
        let response =
            self.send_request_blocking(ControlRequest::RegisterProcess(RegisterProcessRequest {
                app: "zippy_gateway".to_string(),
            }))?;
        let process = match response {
            ControlResponse::ProcessRegistered {
                process_id,
                process_token,
            } => GatewayMasterProcess {
                process_id,
                process_token,
            },
            other => return Err(unexpected_response("ProcessRegistered", other)),
        };
        self.metrics
            .lock()
            .unwrap()
            .master_process_reregistrations_total += 1;
        *guard = Some(process.clone());
        Ok(process)
    }

    fn clear_process(&self) {
        *self.process.lock().unwrap() = None;
    }

    async fn send_request_async(&self, request: ControlRequest) -> Result<ControlResponse> {
        self.metrics.lock().unwrap().master_async_requests_total += 1;
        send_control_request_async(&self.endpoint, request).await
    }

    fn send_request_blocking(&self, request: ControlRequest) -> Result<ControlResponse> {
        self.metrics.lock().unwrap().master_async_requests_total += 1;
        send_control_line_request(&self.endpoint, request)
    }
}

impl GatewayState {
    fn handle_authorized_request(
        &self,
        header: Value,
        payload: Vec<u8>,
    ) -> Result<(Value, Vec<u8>)> {
        match header
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "write_batch" => self.handle_write_batch(header, payload),
            "close_writer" => self.handle_close_writer(header),
            "collect" => self.handle_collect(header),
            "create_snapshot" => self.handle_create_snapshot(header),
            "release_snapshot" => self.handle_release_snapshot(header),
            "get_stream" => self.handle_get_stream(header),
            "list_streams" => self.handle_list_streams(),
            "metrics" => Ok((
                json!({"status": "ok", "metrics": self.metrics_value()}),
                vec![],
            )),
            kind => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported remote gateway request kind=[{}]", kind),
            }),
        }
    }

    fn handle_write_batch(&self, header: Value, payload: Vec<u8>) -> Result<(Value, Vec<u8>)> {
        let stream_name = header
            .get("stream_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "write_batch requires stream_name".to_string(),
            })?;
        let batches = decode_ipc_batches(&payload)?;
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if let Some(max_write_rows) = self.max_write_rows {
            if row_count > max_write_rows {
                self.increment_metric(|metrics| metrics.write_rejections_total += 1);
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "write batch row count exceeds limit rows=[{}] max_write_rows=[{}]",
                        row_count, max_write_rows
                    ),
                });
            }
        }
        for batch in batches {
            self.write_batch(stream_name, batch)?;
        }
        self.increment_metric(|metrics| {
            metrics.write_batches_total += 1;
            metrics.written_rows_total += row_count as u64;
        });
        Ok((json!({"status": "ok"}), vec![]))
    }

    fn handle_close_writer(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let stream_name = header
            .get("stream_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "close_writer requires stream_name".to_string(),
            })?;
        self.close_writer(stream_name)?;
        Ok((json!({"status": "ok"}), vec![]))
    }

    fn collect_stream_producer(
        &self,
        stream_plan: GatewayCollectStreamPlan,
    ) -> Result<GatewayCollectStreamProducer> {
        if stream_plan.snapshot_id.is_none() {
            let active_writer = self
                .writers
                .lock()
                .unwrap()
                .get(&stream_plan.source)
                .cloned();
            if let Some(writer) = active_writer {
                let span = {
                    let writer = writer.lock().unwrap();
                    writer.materializer.active_row_span()?
                };
                return GatewayCollectStreamProducer::segment(
                    span.schema_ref(),
                    vec![span],
                    stream_plan.scan_pushdown,
                    stream_plan.output_projection_columns,
                    stream_plan.chunk_rows,
                );
            }
            {
                let stream = self.master.get_stream_blocking(&stream_plan.source)?;
                if stream_is_persisted_only(&stream) {
                    self.increment_metric(|metrics| metrics.collect_requests_total += 1);
                    return GatewayCollectStreamProducer::persisted_serial(
                        &stream,
                        &stream_plan.scan_pushdown,
                        stream_plan.output_projection_columns.as_deref(),
                        stream_plan.row_range_pushdown.map(|(pushdown, _)| pushdown),
                        stream_plan.chunk_rows,
                        GatewayPersistedScanResources {
                            pool: Arc::clone(&self.persisted_scan_pool),
                            parallelism: self.query_parallelism,
                            query_buffer_budget_bytes: self.query_max_buffered_bytes,
                        },
                    );
                }
            }
        }
        let mut collect_header = json!({
            "kind": "collect",
            "source": stream_plan.source,
            "plan": stream_plan.plan,
        });
        if let Some(snapshot_id) = stream_plan.snapshot_id {
            collect_header["snapshot_id"] = json!(snapshot_id);
        }
        let (response, payload) = self.handle_collect(collect_header)?;
        let mut metrics = response
            .get("metrics")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if let Some(object) = metrics.as_object_mut() {
            normalize_collect_stream_metrics(object);
            object.insert("streaming".to_string(), json!(true));
        } else {
            metrics = json!({
                "collect_metrics": metrics,
                "streaming": true,
            });
        }
        let metrics = GatewayCollectStreamMetrics::from_collect_metrics(&metrics);
        let batches = decode_ipc_batches(&payload)?;
        let batch = concat_record_batches(
            batches
                .first()
                .map(RecordBatch::schema)
                .unwrap_or_else(|| Arc::new(Schema::empty())),
            batches,
        )?;
        Ok(GatewayCollectStreamProducer::materialized_with_metrics(
            batch,
            stream_plan.chunk_rows,
            metrics,
        ))
    }

    fn handle_create_snapshot(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "create_snapshot requires source".to_string(),
            })?;
        let stream = self.master.get_stream_blocking(source)?;
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway snapshot source is not a segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        if stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    source, stream.status
                ),
            });
        }

        let mut leases = Vec::new();
        for descriptor in &stream.sealed_segments {
            leases.push(self.acquire_segment_reader_lease(source, descriptor)?);
        }
        let mut pinned_active_high_watermark = None;
        let active_descriptor = stream.active_segment_descriptor.clone();
        if let Some(descriptor) = &active_descriptor {
            leases.push(self.acquire_segment_reader_lease(source, descriptor)?);
            let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
            pinned_active_high_watermark = Some(active_committed_row_high_watermark(
                descriptor,
                segment_schema,
            )?);
        } else if stream.persisted_files.is_empty() && stream.sealed_segments.is_empty() {
            return Err(ZippyError::Io {
                reason: format!("segment descriptor is not published source=[{}]", source),
            });
        }

        let snapshot_id = format!(
            "snapshot-{}",
            self.snapshot_counter.fetch_add(1, Ordering::SeqCst) + 1
        );
        let snapshot = GatewaySnapshot {
            source: source.to_string(),
            stream: Arc::new(stream.clone()),
            active_descriptor,
            active_committed_row_high_watermark: pinned_active_high_watermark,
            last_accessed_at: Instant::now(),
            _leases: leases,
        };
        self.insert_snapshot(snapshot_id.clone(), snapshot);

        Ok((
            json!({
                "status": "ok",
                "snapshot": {
                    "snapshot_id": snapshot_id,
                    "stream_name": source,
                    "data_path": "remote_gateway",
                    "descriptor_generation": stream.descriptor_generation,
                    "writer_epoch": stream.writer_epoch,
                }
            }),
            vec![],
        ))
    }

    fn handle_release_snapshot(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let snapshot_id = header
            .get("snapshot_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "release_snapshot requires snapshot_id".to_string(),
            })?;
        self.snapshots.lock().unwrap().remove(snapshot_id);
        Ok((json!({"status": "ok"}), vec![]))
    }

    fn handle_collect(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let started = Instant::now();
        self.increment_metric(|metrics| metrics.collect_requests_total += 1);
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "collect requires source".to_string(),
            })?;
        let plan = header
            .get("plan")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let row_range_pushdown = collect_plan_row_range_prefix(&plan)?;
        let row_range_residual_start = row_range_pushdown.map_or(0, |(_, start)| start);
        let pushed_filter_count =
            collect_plan_leading_filter_count(&plan[row_range_residual_start..]);
        let scan_residual_start = row_range_residual_start + pushed_filter_count;
        let requested_scan_pushdown = GatewayScanPushdown {
            filters: plan[row_range_residual_start..scan_residual_start].to_vec(),
            projection_columns: collect_plan_scan_projection_columns(
                &plan[row_range_residual_start..],
            )?,
        };
        let snapshot_id = header.get("snapshot_id").and_then(Value::as_str);
        let batch = {
            let writers = self.writers.lock().unwrap();
            if snapshot_id.is_some() {
                None
            } else {
                writers.get(source).cloned()
            }
        };
        let batch = batch
            .map(|writer| {
                let writer = writer.lock().unwrap();
                writer.materializer.active_record_batch()
            })
            .transpose()?;
        let mut row_range_pushed = false;
        let mut scan_pushdown_applied = false;
        let scanned = if let Some(snapshot_id) = snapshot_id {
            scan_pushdown_applied = true;
            if let Some((pushdown, _)) = row_range_pushdown {
                row_range_pushed = true;
                self.collect_snapshot_source_row_range(
                    source,
                    snapshot_id,
                    pushdown,
                    &requested_scan_pushdown,
                )?
            } else {
                self.collect_snapshot_source(source, snapshot_id, &requested_scan_pushdown)?
            }
        } else {
            match batch {
                Some(batch) => GatewayScannedBatch {
                    scanned_rows: batch.num_rows(),
                    scanned_live_rows: batch.num_rows(),
                    scanned_files: Vec::new(),
                    batch,
                },
                None => match row_range_pushdown {
                    Some((pushdown, _)) => {
                        row_range_pushed = true;
                        scan_pushdown_applied = true;
                        self.collect_stream_source_row_range(
                            source,
                            pushdown,
                            &requested_scan_pushdown,
                        )?
                    }
                    None => {
                        scan_pushdown_applied = true;
                        self.collect_stream_source(source, &requested_scan_pushdown)?
                    }
                },
            }
        };
        let residual_plan = if scan_pushdown_applied {
            &plan[scan_residual_start..]
        } else if let Some((_, residual_start)) = row_range_pushdown {
            if row_range_pushed {
                &plan[residual_start..]
            } else {
                plan.as_slice()
            }
        } else {
            plan.as_slice()
        };
        let batch = apply_collect_plan(scanned.batch, residual_plan)?;
        let returned_rows = batch.num_rows();
        let tail_pushdown = row_range_pushed
            && matches!(
                row_range_pushdown,
                Some((GatewayRowRangePushdown::Tail(_), _))
            );
        let row_range_pushdown_metric = if row_range_pushed {
            row_range_pushdown.map(|(pushdown, _)| pushdown.op_name())
        } else {
            None
        };
        let residual_filters = collect_plan_filter_ops(residual_plan);
        let projection_columns = collect_plan_projection_columns(residual_plan);
        let pushed_filters = if scan_pushdown_applied {
            requested_scan_pushdown.filters.clone()
        } else {
            Vec::new()
        };
        let scan_projection_columns = if scan_pushdown_applied {
            requested_scan_pushdown.projection_columns.clone()
        } else {
            None
        };
        Ok((
            json!({
                "status": "ok",
                "metrics": {
                    "scanned_files": scanned.scanned_files,
                    "scanned_rows": scanned.scanned_rows,
                    "scanned_live_rows": scanned.scanned_live_rows,
                    "returned_rows": returned_rows,
                    "plan_ops": plan.len(),
                    "tail_pushdown": tail_pushdown,
                    "row_range_pushdown": row_range_pushdown_metric,
                    "pushed_filters": pushed_filters,
                    "residual_filters": residual_filters,
                    "projection_columns": projection_columns,
                    "scan_projection_columns": scan_projection_columns,
                    "residual_plan_ops": residual_plan.len(),
                    "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
                }
            }),
            encode_ipc_table(&batch)?,
        ))
    }

    fn collect_snapshot_source(
        &self,
        source: &str,
        snapshot_id: &str,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let snapshot = self.snapshot_view(source, snapshot_id)?;

        let schema = Arc::new(arrow_schema_from_stream_metadata(&snapshot.stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut _max_pending_file_results = 0usize;
        let mut batches = parallel_persisted_file_record_batches(
            &snapshot.stream,
            scan_pushdown,
            self.query_parallelism,
            &mut scanned_rows,
            &mut scanned_files,
            &mut _max_pending_file_results,
        )?;
        if let Some(descriptor) = &snapshot.active_descriptor {
            let Some(active_committed_row_high_watermark) =
                snapshot.active_committed_row_high_watermark
            else {
                return Err(ZippyError::Io {
                    reason: format!(
                        "remote snapshot missing active high watermark snapshot_id=[{}]",
                        snapshot_id
                    ),
                });
            };
            let segment_schema =
                compile_segment_schema_from_stream_metadata(&snapshot.stream.schema)?;
            let mut live_batches = live_segment_record_batches(
                descriptor,
                &snapshot.stream.sealed_segments,
                segment_schema,
                active_committed_row_high_watermark,
                scan_pushdown,
                &mut scanned_live_rows,
            )?;
            scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
            batches.append(&mut live_batches);
        }

        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn collect_snapshot_source_row_range(
        &self,
        source: &str,
        snapshot_id: &str,
        pushdown: GatewayRowRangePushdown,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let snapshot = self.snapshot_view(source, snapshot_id)?;
        let schema = Arc::new(arrow_schema_from_stream_metadata(&snapshot.stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut leases = Vec::new();
        let segment_schema = snapshot
            .active_descriptor
            .as_ref()
            .map(|_| compile_segment_schema_from_stream_metadata(&snapshot.stream.schema))
            .transpose()?;

        let batches = match pushdown {
            GatewayRowRangePushdown::Tail(n) => {
                let mut live_batches = Vec::new();
                if let (Some(descriptor), Some(segment_schema)) =
                    (snapshot.active_descriptor.as_ref(), segment_schema.clone())
                {
                    let high_watermark =
                        snapshot
                            .active_committed_row_high_watermark
                            .ok_or_else(|| ZippyError::Io {
                                reason: format!(
                                "remote snapshot missing active high watermark snapshot_id=[{}]",
                                snapshot_id
                            ),
                            })?;
                    live_batches = tail_live_segment_record_batches_with_leases(
                        self,
                        source,
                        descriptor,
                        &snapshot.stream.sealed_segments,
                        segment_schema,
                        high_watermark,
                        n,
                        false,
                        &mut leases,
                        scan_pushdown,
                        &mut scanned_live_rows,
                    )?;
                }
                let persisted_remaining = n.saturating_sub(scanned_live_rows);
                let mut persisted_batches = if persisted_remaining > 0 {
                    tail_persisted_file_record_batches(
                        &snapshot.stream,
                        persisted_remaining,
                        scan_pushdown,
                        &mut scanned_rows,
                        &mut scanned_files,
                    )?
                } else {
                    Vec::new()
                };
                persisted_batches.append(&mut live_batches);
                persisted_batches
            }
            GatewayRowRangePushdown::Head(n) => {
                let mut batches = head_persisted_file_record_batches(
                    &snapshot.stream,
                    n,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                )?;
                let remaining = n.saturating_sub(scanned_rows);
                if remaining > 0 {
                    if let (Some(descriptor), Some(segment_schema)) =
                        (snapshot.active_descriptor.as_ref(), segment_schema.clone())
                    {
                        let mut live_batches = head_live_segment_record_batches_with_leases(
                            self,
                            source,
                            descriptor,
                            &snapshot.stream.sealed_segments,
                            segment_schema,
                            remaining,
                            false,
                            &mut leases,
                            scan_pushdown,
                            &mut scanned_live_rows,
                        )?;
                        batches.append(&mut live_batches);
                    }
                }
                batches
            }
            GatewayRowRangePushdown::Slice { offset, length } => {
                let (mut batches, persisted_rows, selected_persisted_rows) =
                    slice_persisted_file_record_batches(
                        &snapshot.stream,
                        offset,
                        length,
                        scan_pushdown,
                        &mut scanned_rows,
                        &mut scanned_files,
                    )?;
                let remaining_length =
                    length.map(|value| value.saturating_sub(selected_persisted_rows));
                if remaining_length.is_none_or(|value| value > 0) {
                    if let (Some(descriptor), Some(segment_schema)) =
                        (snapshot.active_descriptor.as_ref(), segment_schema)
                    {
                        let mut live_batches = slice_live_segment_record_batches_with_leases(
                            self,
                            source,
                            descriptor,
                            &snapshot.stream.sealed_segments,
                            segment_schema,
                            offset.saturating_sub(persisted_rows),
                            remaining_length,
                            false,
                            &mut leases,
                            scan_pushdown,
                            &mut scanned_live_rows,
                        )?;
                        batches.append(&mut live_batches);
                    }
                }
                batches
            }
        };
        scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn snapshot_view(&self, source: &str, snapshot_id: &str) -> Result<GatewaySnapshotView> {
        let snapshot = {
            let mut snapshots = self.snapshots.lock().unwrap();
            let snapshot =
                snapshots
                    .get_mut(snapshot_id)
                    .ok_or_else(|| ZippyError::InvalidConfig {
                        reason: format!("remote snapshot not found snapshot_id=[{}]", snapshot_id),
                    })?;
            snapshot.last_accessed_at = Instant::now();
            GatewaySnapshotView {
                source: snapshot.source.clone(),
                stream: snapshot.stream.clone(),
                active_descriptor: snapshot.active_descriptor.clone(),
                active_committed_row_high_watermark: snapshot.active_committed_row_high_watermark,
            }
        };
        if snapshot.source != source {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "remote snapshot source mismatch snapshot_id=[{}] expected=[{}] got=[{}]",
                    snapshot_id, snapshot.source, source
                ),
            });
        }
        Ok(snapshot)
    }

    fn insert_snapshot(&self, snapshot_id: String, snapshot: GatewaySnapshot) {
        self.expire_idle_snapshots();
        let evicted = {
            let mut snapshots = self.snapshots.lock().unwrap();
            let mut evicted = 0usize;
            while snapshots.len() >= MAX_GATEWAY_SNAPSHOTS {
                let Some(oldest_id) = snapshots
                    .iter()
                    .min_by_key(|(_, snapshot)| snapshot.last_accessed_at)
                    .map(|(snapshot_id, _)| snapshot_id.clone())
                else {
                    break;
                };
                snapshots.remove(&oldest_id);
                evicted += 1;
            }
            snapshots.insert(snapshot_id, snapshot);
            evicted
        };
        if evicted > 0 {
            self.increment_metric(|metrics| {
                metrics.snapshots_evicted_total = metrics
                    .snapshots_evicted_total
                    .saturating_add(evicted as u64);
            });
        }
    }

    fn expire_idle_snapshots(&self) {
        let expired = {
            let mut snapshots = self.snapshots.lock().unwrap();
            expire_gateway_snapshots(&mut snapshots, Instant::now())
        };
        if expired > 0 {
            self.increment_metric(|metrics| {
                metrics.snapshots_expired_total = metrics
                    .snapshots_expired_total
                    .saturating_add(expired as u64);
            });
        }
    }

    fn write_batch(&self, stream_name: &str, batch: RecordBatch) -> Result<()> {
        let writer = self.writer_handle(stream_name, batch.schema())?;
        {
            let writer_lock_started = Instant::now();
            let mut writer = writer.lock().unwrap();
            let writer_lock_wait = writer_lock_started.elapsed();
            let on_data_started = Instant::now();
            writer
                .materializer
                .on_data(SegmentTableView::from_record_batch(batch))?;
            let on_data_elapsed = on_data_started.elapsed();
            let flush_started = Instant::now();
            writer.materializer.on_flush()?;
            let flush_elapsed = flush_started.elapsed();
            drop(writer);
            self.record_writer_lock_wait(writer_lock_wait);
            self.record_writer_on_data_elapsed(on_data_elapsed);
            self.record_writer_flush_elapsed(flush_elapsed);
        }
        self.subscribe_catalog_notifier.mark_activity();
        Ok(())
    }

    fn close_writer(&self, stream_name: &str) -> Result<()> {
        let writer = self.writers.lock().unwrap().remove(stream_name);
        if let Some(writer) = writer {
            let mut writer = writer.lock().unwrap();
            self.close_table_writer(&mut writer)?;
        }
        Ok(())
    }

    fn close_all_writers(&self) {
        let writers = {
            let mut guard = self.writers.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for writer in writers.into_values() {
            let mut writer = writer.lock().unwrap();
            let _ = self.close_table_writer(&mut writer);
        }
    }

    fn close_table_writer(&self, writer: &mut GatewayTableWriter) -> Result<()> {
        let stop_result = writer.materializer.on_stop();
        let unregister_result = self.master.unregister_source_blocking(&writer.source_name);
        stop_result.and(unregister_result)
    }

    fn writer_handle(
        &self,
        stream_name: &str,
        schema: SchemaRef,
    ) -> Result<GatewayTableWriterHandle> {
        let map_lock_started = Instant::now();
        let mut writers = self.writers.lock().unwrap();
        let map_lock_wait = map_lock_started.elapsed();
        if let Some(writer) = writers.get(stream_name) {
            let writer = Arc::clone(writer);
            drop(writers);
            self.record_writer_map_lock_wait(map_lock_wait);
            return Ok(writer);
        }

        let create_started = Instant::now();
        let writer = Arc::new(Mutex::new(self.create_writer(stream_name, schema)?));
        let create_elapsed = create_started.elapsed();
        writers.insert(stream_name.to_string(), Arc::clone(&writer));
        drop(writers);
        self.record_writer_map_lock_wait(map_lock_wait);
        self.record_writer_create_elapsed(create_elapsed);
        Ok(writer)
    }

    fn create_writer(&self, stream_name: &str, schema: SchemaRef) -> Result<GatewayTableWriter> {
        let source_name = format!("gateway.{}", stream_name);
        self.master
            .register_stream_blocking(stream_name, Arc::clone(&schema), 64, 4096)?;
        self.master
            .register_source_blocking(&source_name, "gateway", stream_name, json!({}))?;
        let writer_epoch = self
            .master
            .get_stream_status_blocking(stream_name)?
            .writer_epoch;

        let publisher = Arc::new(MasterDescriptorPublisher {
            master: Arc::clone(&self.master),
            metrics: Arc::clone(&self.metrics),
            stream_name: stream_name.to_string(),
        });
        let materializer = StreamTableMaterializer::new_with_row_capacity_and_writer_epoch(
            stream_name,
            schema,
            DEFAULT_STREAM_TABLE_ROW_CAPACITY,
            Some(writer_epoch),
        )?
        .with_descriptor_publisher(publisher);
        let descriptor = materializer.active_descriptor_envelope_bytes()?;
        let descriptor_publish_started = Instant::now();
        self.master
            .publish_segment_descriptor_bytes_blocking(stream_name, &descriptor)?;
        self.record_writer_descriptor_publish_elapsed(descriptor_publish_started.elapsed());
        Ok(GatewayTableWriter {
            source_name,
            materializer,
        })
    }

    fn record_writer_map_lock_wait(&self, elapsed: Duration) {
        self.increment_metric(|metrics| {
            let elapsed_ms = elapsed_ms(elapsed);
            metrics.writer_map_lock_wait_count += 1;
            metrics.writer_map_lock_wait_last_ms = elapsed_ms;
            metrics.writer_map_lock_wait_total_ms += elapsed_ms;
        });
    }

    fn record_writer_lock_wait(&self, elapsed: Duration) {
        self.increment_metric(|metrics| {
            let elapsed_ms = elapsed_ms(elapsed);
            metrics.writer_lock_wait_count += 1;
            metrics.writer_lock_wait_last_ms = elapsed_ms;
            metrics.writer_lock_wait_total_ms += elapsed_ms;
        });
    }

    fn record_writer_create_elapsed(&self, elapsed: Duration) {
        self.increment_metric(|metrics| {
            let elapsed_ms = elapsed_ms(elapsed);
            metrics.writer_create_count += 1;
            metrics.writer_create_last_ms = elapsed_ms;
            metrics.writer_create_total_ms += elapsed_ms;
        });
    }

    fn record_writer_on_data_elapsed(&self, elapsed: Duration) {
        self.increment_metric(|metrics| {
            let elapsed_ms = elapsed_ms(elapsed);
            metrics.writer_on_data_count += 1;
            metrics.writer_on_data_last_ms = elapsed_ms;
            metrics.writer_on_data_total_ms += elapsed_ms;
        });
    }

    fn record_writer_flush_elapsed(&self, elapsed: Duration) {
        self.increment_metric(|metrics| {
            let elapsed_ms = elapsed_ms(elapsed);
            metrics.writer_flush_count += 1;
            metrics.writer_flush_last_ms = elapsed_ms;
            metrics.writer_flush_total_ms += elapsed_ms;
        });
    }

    fn record_writer_descriptor_publish_elapsed(&self, elapsed: Duration) {
        record_writer_descriptor_publish_elapsed(&self.metrics, elapsed);
    }

    fn handle_get_stream(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "get_stream requires source".to_string(),
            })?;
        let stream = self.master.get_stream_blocking(source)?;
        let schema_payload = {
            let writers = self.writers.lock().unwrap();
            writers.get(source).cloned()
        };
        let schema_payload = match schema_payload {
            Some(writer) => {
                let writer = writer.lock().unwrap();
                encode_ipc_schema(&writer.materializer.output_schema())?
            }
            None => encode_ipc_schema(&Arc::new(arrow_schema_from_stream_metadata(
                &stream.schema,
            )?))?,
        };
        Ok((json!({"status": "ok", "stream": stream}), schema_payload))
    }

    fn handle_list_streams(&self) -> Result<(Value, Vec<u8>)> {
        let streams = self.master.list_streams_blocking()?;
        Ok((json!({"status": "ok", "streams": streams}), vec![]))
    }

    fn authorize(&self, header: &Value) -> Result<()> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        if header.get("token").and_then(Value::as_str) == Some(token.as_str()) {
            return Ok(());
        }
        self.increment_metric(|metrics| metrics.auth_failures_total += 1);
        Err(ZippyError::Io {
            reason: "unauthorized remote gateway request".to_string(),
        })
    }

    fn metrics_value(&self) -> Value {
        let metrics = self.metrics.lock().unwrap().clone();
        let mut value = gateway_metrics_json(&metrics);
        let fields = value.as_object_mut().unwrap();
        insert_json_metric(
            fields,
            "write_timeout_ms",
            self.write_timeout.as_millis() as u64,
        );
        insert_json_metric(fields, "query_parallelism", self.query_parallelism);
        insert_json_metric(fields, "max_parallelism", self.max_parallelism);
        insert_json_metric(
            fields,
            "max_query_buffer_bytes",
            self.max_query_buffer_bytes,
        );
        insert_json_metric(
            fields,
            "query_max_buffered_bytes",
            self.query_max_buffered_bytes,
        );
        insert_json_metric(
            fields,
            "query_buffer_reserved_bytes",
            self.max_query_buffer_bytes - self.persisted_buffer_limit.available_permits(),
        );
        insert_json_metric(
            fields,
            "snapshots_active",
            self.snapshots.lock().unwrap().len(),
        );
        value
    }

    fn increment_metric(&self, update: impl FnOnce(&mut GatewayMetrics)) {
        let mut metrics = self.metrics.lock().unwrap();
        update(&mut metrics);
    }

    fn parse_subscribe_request(
        &self,
        header: Value,
        mode: GatewaySubscribeMode,
    ) -> Result<GatewaySubscribeRequest> {
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: format!("{} requires source", subscribe_kind_name(mode)),
            })?
            .to_string();
        let filter = subscribe_filter_plan(header.get("filter").unwrap_or(&Value::Null));
        if mode == GatewaySubscribeMode::Table
            && header
                .get("instrument_ids")
                .is_some_and(|value| !value.is_null())
        {
            return Err(ZippyError::InvalidConfig {
                reason: "subscribe_table does not support instrument_ids".to_string(),
            });
        }
        let instrument_ids = parse_subscribe_instrument_ids(header.get("instrument_ids"))?;
        if mode == GatewaySubscribeMode::Rows {
            for field_name in ["batch_size", "throttle_ms", "count"] {
                if header.get(field_name).is_some_and(|value| !value.is_null()) {
                    return Err(ZippyError::InvalidConfig {
                        reason: format!("subscribe_rows does not support {}", field_name),
                    });
                }
            }
        }
        let batch_size =
            parse_positive_subscribe_usize(&header, "batch_size", subscribe_kind_name(mode))?;
        let count = parse_positive_subscribe_usize(&header, "count", subscribe_kind_name(mode))?;
        let throttle_ms =
            parse_positive_subscribe_u64(&header, "throttle_ms", subscribe_kind_name(mode))?;
        let throttle = throttle_ms.map(Duration::from_millis);
        Ok(GatewaySubscribeRequest {
            mode,
            source,
            filter,
            instrument_ids,
            batch_size,
            count,
            throttle,
        })
    }

    fn active_subscribe_driver(
        &self,
        request: GatewaySubscribeRequest,
    ) -> Result<GatewayActiveSubscribeDriver> {
        let stream = self.master.get_stream_blocking(&request.source)?;
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway subscribe source is not a live segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        if stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    request.source, stream.status
                ),
            });
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(ZippyError::Io {
                reason: format!(
                    "segment descriptor is not published source=[{}]",
                    request.source
                ),
            });
        };
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let reader = active_segment_reader_from_descriptor(&descriptor, segment_schema.clone())?;
        let lease = self.acquire_segment_reader_lease(&request.source, &descriptor)?;
        Ok(GatewayActiveSubscribeDriver {
            request,
            master: Arc::clone(&self.master),
            reader,
            segment_schema,
            descriptor_generation: stream.descriptor_generation,
            pending: VecDeque::new(),
            table_pending: Vec::new(),
            table_pending_rows: 0,
            table_last_emit_at: Instant::now(),
            _lease: lease,
        })
    }

    fn collect_stream_source(
        &self,
        source: &str,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let stream = self.master.get_stream_blocking(source)?;
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway collect source is not materialized by this gateway and is not a segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut _max_pending_file_results = 0usize;
        let mut batches = parallel_persisted_file_record_batches(
            &stream,
            scan_pushdown,
            self.query_parallelism,
            &mut scanned_rows,
            &mut scanned_files,
            &mut _max_pending_file_results,
        )?;
        if let Some(mut live_batches) = self.try_collect_live_segment_batches(
            source,
            &stream,
            scan_pushdown,
            &mut scanned_live_rows,
        )? {
            scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
            batches.append(&mut live_batches);
        } else if batches.is_empty() && stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    source, stream.status
                ),
            });
        } else if batches.is_empty() && stream.active_segment_descriptor.is_none() {
            return Err(ZippyError::Io {
                reason: format!("segment descriptor is not published source=[{}]", source),
            });
        }
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn try_collect_live_segment_batches(
        &self,
        source: &str,
        stream: &StreamInfo,
        scan_pushdown: &GatewayScanPushdown,
        scanned_live_rows: &mut usize,
    ) -> Result<Option<Vec<RecordBatch>>> {
        if stream.status == "stale" {
            return Ok(None);
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Ok(None);
        };
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let _leases = self.acquire_segment_reader_leases(source, &descriptor, stream)?;
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
        let mut scanned_rows = 0usize;
        let batches = live_segment_record_batches(
            &descriptor,
            &stream.sealed_segments,
            segment_schema,
            active_committed_row_high_watermark,
            scan_pushdown,
            &mut scanned_rows,
        )?;
        *scanned_live_rows = scanned_rows;
        Ok(Some(batches))
    }

    fn collect_stream_source_row_range(
        &self,
        source: &str,
        pushdown: GatewayRowRangePushdown,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let stream = self.master.get_stream_blocking(source)?;
        if stream_has_persisted_files(&stream) {
            return match pushdown {
                GatewayRowRangePushdown::Tail(n) => {
                    self.collect_stream_source_tail(source, &stream, n, scan_pushdown)
                }
                GatewayRowRangePushdown::Head(n) => {
                    self.collect_stream_source_head(source, &stream, n, scan_pushdown)
                }
                GatewayRowRangePushdown::Slice { offset, length } => {
                    self.collect_stream_source_slice(source, &stream, offset, length, scan_pushdown)
                }
            };
        }
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway collect source is not materialized by this gateway and is not a segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        if stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    source, stream.status
                ),
            });
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(ZippyError::Io {
                reason: format!("segment descriptor is not published source=[{}]", source),
            });
        };
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let mut _leases = Vec::new();
        let mut scanned_rows = 0usize;
        let batches = match pushdown {
            GatewayRowRangePushdown::Tail(n) => {
                _leases.push(self.acquire_segment_reader_lease(source, &descriptor)?);
                let active_committed_row_high_watermark =
                    active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
                tail_live_segment_record_batches_with_leases(
                    self,
                    source,
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    active_committed_row_high_watermark,
                    n,
                    true,
                    &mut _leases,
                    scan_pushdown,
                    &mut scanned_rows,
                )?
            }
            GatewayRowRangePushdown::Head(n) => head_live_segment_record_batches_with_leases(
                self,
                source,
                &descriptor,
                &stream.sealed_segments,
                segment_schema,
                n,
                true,
                &mut _leases,
                scan_pushdown,
                &mut scanned_rows,
            )?,
            GatewayRowRangePushdown::Slice { offset, length } => {
                slice_live_segment_record_batches_with_leases(
                    self,
                    source,
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    offset,
                    length,
                    true,
                    &mut _leases,
                    scan_pushdown,
                    &mut scanned_rows,
                )?
            }
        };
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(schema, batches)?,
            scanned_rows,
            scanned_live_rows: scanned_rows,
            scanned_files: Vec::new(),
        })
    }

    fn collect_stream_source_tail(
        &self,
        source: &str,
        stream: &StreamInfo,
        n: usize,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut live_batches = self
            .try_collect_live_segment_tail_batches(
                source,
                stream,
                n,
                scan_pushdown,
                &mut scanned_live_rows,
            )?
            .unwrap_or_default();
        let live_rows = scanned_live_rows;
        let persisted_remaining = n.saturating_sub(live_rows);
        let mut batches = if persisted_remaining > 0 {
            tail_persisted_file_record_batches(
                stream,
                persisted_remaining,
                scan_pushdown,
                &mut scanned_rows,
                &mut scanned_files,
            )?
        } else {
            Vec::new()
        };

        scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
        batches.append(&mut live_batches);
        let mut batch =
            concat_record_batches(schema_for_scan_pushdown(&schema, scan_pushdown)?, batches)?;
        if batch.num_rows() > n {
            batch = apply_collect_plan(batch, &[GatewayRowRangePushdown::Tail(n).to_plan_op()])?;
        }
        Ok(GatewayScannedBatch {
            batch,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn collect_stream_source_head(
        &self,
        source: &str,
        stream: &StreamInfo,
        n: usize,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut batches = head_persisted_file_record_batches(
            stream,
            n,
            scan_pushdown,
            &mut scanned_rows,
            &mut scanned_files,
        )?;
        let persisted_raw_rows = scanned_rows;
        let remaining = n.saturating_sub(persisted_raw_rows);
        if remaining > 0 && stream.status != "stale" {
            if let Some(descriptor) = stream.active_segment_descriptor.as_ref() {
                let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
                let mut leases = Vec::new();
                let mut live_batches = head_live_segment_record_batches_with_leases(
                    self,
                    source,
                    descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    remaining,
                    true,
                    &mut leases,
                    scan_pushdown,
                    &mut scanned_live_rows,
                )?;
                batches.append(&mut live_batches);
            }
        }
        scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn collect_stream_source_slice(
        &self,
        source: &str,
        stream: &StreamInfo,
        offset: usize,
        length: Option<usize>,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let (mut batches, persisted_rows, selected_persisted_rows) =
            slice_persisted_file_record_batches(
                stream,
                offset,
                length,
                scan_pushdown,
                &mut scanned_rows,
                &mut scanned_files,
            )?;
        let remaining_length = length.map(|value| value.saturating_sub(selected_persisted_rows));
        let should_scan_live = remaining_length.is_none_or(|value| value > 0);
        if should_scan_live && stream.status != "stale" {
            if let Some(descriptor) = stream.active_segment_descriptor.as_ref() {
                let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
                let mut leases = Vec::new();
                let mut live_batches = slice_live_segment_record_batches_with_leases(
                    self,
                    source,
                    descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    offset.saturating_sub(persisted_rows),
                    remaining_length,
                    true,
                    &mut leases,
                    scan_pushdown,
                    &mut scanned_live_rows,
                )?;
                batches.append(&mut live_batches);
            }
        }
        scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn try_collect_live_segment_tail_batches(
        &self,
        source: &str,
        stream: &StreamInfo,
        n: usize,
        scan_pushdown: &GatewayScanPushdown,
        scanned_live_rows: &mut usize,
    ) -> Result<Option<Vec<RecordBatch>>> {
        if stream.status == "stale" {
            return Ok(None);
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Ok(None);
        };
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let mut leases = Vec::new();
        leases.push(self.acquire_segment_reader_lease(source, &descriptor)?);
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
        let mut scanned_rows = 0usize;
        let batches = tail_live_segment_record_batches_with_leases(
            self,
            source,
            &descriptor,
            &stream.sealed_segments,
            segment_schema,
            active_committed_row_high_watermark,
            n,
            true,
            &mut leases,
            scan_pushdown,
            &mut scanned_rows,
        )?;
        *scanned_live_rows = scanned_rows;
        Ok(Some(batches))
    }

    fn acquire_segment_reader_leases(
        &self,
        source: &str,
        active_descriptor: &Value,
        stream: &StreamInfo,
    ) -> Result<Vec<SegmentReaderLeaseGuard>> {
        let mut leases = Vec::with_capacity(stream.sealed_segments.len() + 1);
        for descriptor in &stream.sealed_segments {
            leases.push(self.acquire_segment_reader_lease(source, descriptor)?);
        }
        leases.push(self.acquire_segment_reader_lease(source, active_descriptor)?);
        Ok(leases)
    }

    fn acquire_segment_reader_lease(
        &self,
        source: &str,
        descriptor: &Value,
    ) -> Result<SegmentReaderLeaseGuard> {
        let (segment_id, generation) = descriptor_segment_identity(descriptor)?;
        let lease_id = self
            .master
            .acquire_segment_reader_lease_blocking(source, segment_id, generation)?;
        Ok(SegmentReaderLeaseGuard {
            master: Arc::clone(&self.master),
            source: source.to_string(),
            lease_id: Some(lease_id),
        })
    }
}

fn gateway_master_process_invalid(error: &ZippyError) -> bool {
    let message = error.to_string();
    message.contains("process lease expired") || message.contains("process not found")
}

impl Drop for SegmentReaderLeaseGuard {
    fn drop(&mut self) {
        let Some(lease_id) = self.lease_id.take() else {
            return;
        };
        let _ = self
            .master
            .release_segment_reader_lease_blocking(&self.source, &lease_id);
    }
}

impl GatewayActiveSubscribeDriver {
    fn next_event(mut self) -> Result<GatewaySubscribeStep> {
        if let Some(event) = self.pop_event_or_flush_table()? {
            return Ok(GatewaySubscribeStep {
                driver: self,
                event,
            });
        }

        let observed = self
            .reader
            .notification_sequence()
            .map_err(segment_zippy_error)?;
        if let Some(span) = self.reader.read_available().map_err(segment_zippy_error)? {
            self.enqueue_span(span)?;
            let event = self
                .pop_event_or_flush_table()?
                .unwrap_or(GatewaySubscribeEvent::Idle);
            return Ok(GatewaySubscribeStep {
                driver: self,
                event,
            });
        }

        if self.apply_descriptor_update_if_available(Duration::from_millis(0))? {
            if let Some(span) = self.reader.read_available().map_err(segment_zippy_error)? {
                self.enqueue_span(span)?;
            }
            let event = self
                .pop_event_or_flush_table()?
                .unwrap_or(GatewaySubscribeEvent::Idle);
            return Ok(GatewaySubscribeStep {
                driver: self,
                event,
            });
        }

        if self.reader.is_sealed().map_err(segment_zippy_error)? {
            if self.apply_descriptor_update_if_available(
                DEFAULT_GATEWAY_SUBSCRIBE_ACTIVE_WAIT_TIMEOUT,
            )? {
                if let Some(span) = self.reader.read_available().map_err(segment_zippy_error)? {
                    self.enqueue_span(span)?;
                }
            }
        } else {
            self.reader
                .wait_for_notification_after(
                    observed,
                    DEFAULT_GATEWAY_SUBSCRIBE_ACTIVE_WAIT_TIMEOUT,
                )
                .map_err(segment_zippy_error)?;
        }
        if let Some(span) = self.reader.read_available().map_err(segment_zippy_error)? {
            self.enqueue_span(span)?;
        } else if self.apply_descriptor_update_if_available(Duration::from_millis(0))? {
            if let Some(span) = self.reader.read_available().map_err(segment_zippy_error)? {
                self.enqueue_span(span)?;
            }
        }
        let event = self
            .pop_event_or_flush_table()?
            .unwrap_or(GatewaySubscribeEvent::Idle);
        Ok(GatewaySubscribeStep {
            driver: self,
            event,
        })
    }

    fn pop_event_or_flush_table(&mut self) -> Result<Option<GatewaySubscribeEvent>> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
        if self.should_emit_pending_table() {
            return Ok(self
                .flush_pending_table()?
                .map(GatewaySubscribeEvent::Table));
        }
        Ok(None)
    }

    fn enqueue_span(&mut self, span: RowSpanView) -> Result<()> {
        match self.request.mode {
            GatewaySubscribeMode::Rows => self.enqueue_row_events(span),
            GatewaySubscribeMode::Table => self.enqueue_table_events(span),
        }
    }

    fn apply_descriptor_update_if_available(&mut self, timeout: Duration) -> Result<bool> {
        let Some((descriptor_generation, descriptor)) =
            self.master.wait_segment_descriptor_blocking(
                &self.request.source,
                self.descriptor_generation,
                timeout,
            )?
        else {
            return Ok(false);
        };
        let reader =
            active_segment_reader_from_descriptor(&descriptor, self.segment_schema.clone())?;
        let lease = acquire_segment_reader_lease_from_master(
            Arc::clone(&self.master),
            &self.request.source,
            &descriptor,
        )?;
        self.reader = reader;
        self._lease = lease;
        self.descriptor_generation = descriptor_generation;
        Ok(true)
    }

    fn enqueue_row_events(&mut self, span: RowSpanView) -> Result<()> {
        let batch = span.as_record_batch().map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?;
        let batch = apply_optional_subscribe_filter(batch, self.request.filter.as_ref())?;
        for row_index in 0..batch.num_rows() {
            if !record_batch_row_matches_instrument_ids(
                &batch,
                row_index,
                self.request.instrument_ids.as_ref(),
            )? {
                continue;
            }
            self.pending
                .push_back(GatewaySubscribeEvent::Row(record_batch_row_to_json(
                    &batch, row_index,
                )?));
        }
        Ok(())
    }

    fn enqueue_table_events(&mut self, span: RowSpanView) -> Result<()> {
        let chunk_rows = span.row_count().max(1);
        let mut reader = span
            .batch_reader(chunk_rows, None)
            .map_err(|error| ZippyError::Io {
                reason: error.to_string(),
            })?;
        while let Some(batch) = reader.next_batch().map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })? {
            let mut batch = apply_optional_subscribe_filter(batch, self.request.filter.as_ref())?;
            if batch.num_rows() == 0 {
                continue;
            }
            if self.request.batch_size.is_none() && self.request.throttle.is_none() {
                batch = apply_subscribe_count_tail(batch, self.request.count);
                self.pending.push_back(GatewaySubscribeEvent::Table(batch));
                continue;
            }

            self.table_pending_rows = self.table_pending_rows.saturating_add(batch.num_rows());
            self.table_pending.push(batch);
            if self.should_emit_pending_table() {
                if let Some(batch) = self.flush_pending_table()? {
                    self.pending.push_back(GatewaySubscribeEvent::Table(batch));
                }
            }
        }
        Ok(())
    }

    fn should_emit_pending_table(&self) -> bool {
        if self.table_pending_rows == 0 {
            return false;
        }
        if self
            .request
            .batch_size
            .is_some_and(|batch_size| self.table_pending_rows >= batch_size)
        {
            return true;
        }
        self.request
            .throttle
            .is_some_and(|throttle| self.table_last_emit_at.elapsed() >= throttle)
    }

    fn flush_pending_table(&mut self) -> Result<Option<RecordBatch>> {
        if self.table_pending.is_empty() {
            return Ok(None);
        }
        let batches = std::mem::take(&mut self.table_pending);
        self.table_pending_rows = 0;
        self.table_last_emit_at = Instant::now();
        let schema = batches[0].schema();
        let batch = concat_record_batches(schema, batches)?;
        let batch = apply_subscribe_count_tail(batch, self.request.count);
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(batch))
    }
}

async fn async_serve_loop(
    listener: tokio::net::TcpListener,
    state: Arc<GatewayState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let heartbeat = tokio::spawn(heartbeat_loop_async(Arc::clone(&state), shutdown.clone()));
    loop {
        if state.stopped.load(Ordering::SeqCst) {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let Ok((mut stream, _)) = accepted else {
                    break;
                };
                let Ok(connection_permit) = state.connection_limit.clone().try_acquire_owned() else {
                    state.increment_metric(|metrics| metrics.connections_rejected_total += 1);
                    let _ = write_frame_async(
                        &mut stream,
                        &json!({
                            "status": "error",
                            "reason": "gateway connection limit exceeded",
                        }),
                        &[],
                    )
                    .await;
                    continue;
                };
                state.increment_metric(|metrics| metrics.connections_active += 1);
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let _connection_permit = connection_permit;
                    handle_client_async(stream, Arc::clone(&state)).await;
                    state.increment_metric(|metrics| {
                        metrics.connections_active =
                            metrics.connections_active.saturating_sub(1);
                    });
                });
            }
        }
    }
    heartbeat.abort();
    let _ = heartbeat.await;
}

async fn heartbeat_loop_async(
    state: Arc<GatewayState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if state.stopped.load(Ordering::SeqCst) {
                    break;
                }
                let _ = state.master.heartbeat().await;
                state.expire_idle_snapshots();
            }
        }
    }
}

async fn handle_client_async(mut stream: tokio::net::TcpStream, state: Arc<GatewayState>) {
    let result = match read_frame_header_async(&mut stream).await {
        Ok(frame) => {
            state.increment_metric(|metrics| metrics.requests_total += 1);
            match state.authorize(&frame.header) {
                Err(error) => Err(error),
                Ok(())
                    if frame.header.get("kind").and_then(Value::as_str)
                        == Some("subscribe_rows") =>
                {
                    if frame.payload_len != 0 {
                        Err(ZippyError::InvalidConfig {
                            reason: "subscribe_rows request must not include payload".to_string(),
                        })
                    } else {
                        match handle_subscribe_stream_async(
                            &mut stream,
                            Arc::clone(&state),
                            frame.header,
                            GatewaySubscribeMode::Rows,
                        )
                        .await
                        {
                            Ok(()) => return,
                            Err(error) => Err(error),
                        }
                    }
                }
                Ok(())
                    if frame.header.get("kind").and_then(Value::as_str)
                        == Some("subscribe_table") =>
                {
                    if frame.payload_len != 0 {
                        Err(ZippyError::InvalidConfig {
                            reason: "subscribe_table request must not include payload".to_string(),
                        })
                    } else {
                        match handle_subscribe_stream_async(
                            &mut stream,
                            Arc::clone(&state),
                            frame.header,
                            GatewaySubscribeMode::Table,
                        )
                        .await
                        {
                            Ok(()) => return,
                            Err(error) => Err(error),
                        }
                    }
                }
                Ok(())
                    if frame.header.get("kind").and_then(Value::as_str)
                        == Some("collect_stream") =>
                {
                    if frame.payload_len != 0 {
                        Err(ZippyError::InvalidConfig {
                            reason: "collect_stream request must not include payload".to_string(),
                        })
                    } else {
                        match handle_collect_stream_async(
                            &mut stream,
                            Arc::clone(&state),
                            frame.header,
                        )
                        .await
                        {
                            Ok(()) => return,
                            Err(error) => Err(error),
                        }
                    }
                }
                Ok(()) => match read_frame_payload_async(&mut stream, frame.payload_len).await {
                    Ok(payload) => {
                        let state_for_request = Arc::clone(&state);
                        run_blocking_request(Arc::clone(&state), move || {
                            state_for_request.handle_authorized_request(frame.header, payload)
                        })
                        .await
                    }
                    Err(error) => Err(error),
                },
            }
        }
        Err(error) => Err(error),
    };
    let (header, payload) = match result {
        Ok(response) => response,
        Err(error) => {
            record_gateway_error_metric(&state, &error);
            (
                json!({"status": "error", "reason": error.to_string()}),
                vec![],
            )
        }
    };
    let _ = write_frame_async(&mut stream, &header, &payload).await;
}

async fn handle_collect_stream_async(
    stream: &mut tokio::net::TcpStream,
    state: Arc<GatewayState>,
    header: Value,
) -> Result<()> {
    let stream_plan = collect_stream_plan_from_header(header)?;
    let chunk_rows = stream_plan.chunk_rows;
    let state_for_request = Arc::clone(&state);
    let mut producer = run_blocking_request(Arc::clone(&state), move || {
        state_for_request.collect_stream_producer(stream_plan)
    })
    .await?;
    let schema = producer.schema();
    let schema_payload = encode_ipc_schema(&schema)?;
    write_frame_async(
        stream,
        &json!({
            "status": "ok",
            "kind": "collect_start",
            "chunk_rows": chunk_rows,
        }),
        &schema_payload,
    )
    .await?;
    let collect_started = Instant::now();

    let buffer_reservation_bytes = producer.global_buffer_reservation_bytes();
    if buffer_reservation_bytes > 0 {
        let reservation_started = Instant::now();
        let reservation_result = async {
            let permits =
                u32::try_from(buffer_reservation_bytes).map_err(|_| ZippyError::InvalidConfig {
                    reason: format!(
                        "gateway query buffer reservation exceeds supported range bytes=[{}]",
                        buffer_reservation_bytes
                    ),
                })?;
            tokio::time::timeout(
                state.write_timeout,
                Arc::clone(&state.persisted_buffer_limit).acquire_many_owned(permits),
            )
            .await
            .map_err(|_| ZippyError::Io {
                reason: format!(
                    "gateway query buffer reservation timed out bytes=[{}] timeout_ms=[{}]",
                    buffer_reservation_bytes,
                    state.write_timeout.as_millis()
                ),
            })?
            .map_err(|_| ZippyError::Io {
                reason: "gateway query buffer limit is closed".to_string(),
            })
        }
        .await;
        let reservation = match reservation_result {
            Ok(reservation) => reservation,
            Err(error) => {
                write_collect_stream_error_async(stream, &error).await?;
                return Ok(());
            }
        };
        if let Err(error) = producer.attach_global_buffer_reservation(reservation) {
            write_collect_stream_error_async(stream, &error).await?;
            return Ok(());
        }
        producer.metrics_mut().global_buffer_wait_elapsed_ms +=
            reservation_started.elapsed().as_secs_f64() * 1000.0;
    }

    let mut chunk_index = 0usize;
    loop {
        let producer_step = run_stream_blocking_step(Arc::clone(&state), move || {
            let mut producer = producer;
            let next = producer.next_batch();
            let next_batch = next?;
            let Some(next_batch) = next_batch else {
                return Ok((producer, None));
            };
            let rows = next_batch.num_rows();
            let encode_started = Instant::now();
            let payload = encode_ipc_table(&next_batch)?;
            producer.metrics_mut().encode_elapsed_ms +=
                encode_started.elapsed().as_secs_f64() * 1000.0;
            Ok((producer, Some((rows, payload))))
        })
        .await;
        let (next_producer, encoded_batch) = match producer_step {
            Ok(step) => step,
            Err(error) => {
                write_collect_stream_error_async(stream, &error).await?;
                return Ok(());
            }
        };
        producer = next_producer;
        let Some((rows, payload)) = encoded_batch else {
            break;
        };
        let write_started = Instant::now();
        write_frame_async(
            stream,
            &json!({
                "status": "ok",
                "kind": "collect_chunk",
                "chunk_index": chunk_index,
                "rows": rows,
            }),
            &payload,
        )
        .await?;
        producer.metrics_mut().write_elapsed_ms += write_started.elapsed().as_secs_f64() * 1000.0;
        if chunk_index == 0 {
            producer.metrics_mut().first_chunk_elapsed_ms =
                collect_started.elapsed().as_secs_f64() * 1000.0;
        }
        chunk_index += 1;
    }

    let metrics = producer.metrics();
    write_frame_async(
        stream,
        &json!({
            "status": "ok",
            "kind": "collect_end",
            "chunks": chunk_index,
            "metrics": metrics,
        }),
        &[],
    )
    .await
}

async fn write_collect_stream_error_async(
    stream: &mut tokio::net::TcpStream,
    error: &ZippyError,
) -> Result<()> {
    write_frame_async(
        stream,
        &json!({
            "status": "error",
            "kind": "collect_error",
            "reason": error.to_string(),
        }),
        &[],
    )
    .await
}

async fn handle_subscribe_stream_async(
    stream: &mut tokio::net::TcpStream,
    state: Arc<GatewayState>,
    header: Value,
    mode: GatewaySubscribeMode,
) -> Result<()> {
    let request = state.parse_subscribe_request(header, mode)?;
    let Ok(subscriber_permit) = state.subscriber_limit.clone().try_acquire_owned() else {
        state.increment_metric(|metrics| metrics.subscribe_clients_rejected_total += 1);
        return Err(ZippyError::Io {
            reason: "gateway subscriber limit exceeded".to_string(),
        });
    };
    let _subscriber_permit = subscriber_permit;
    state.increment_metric(|metrics| {
        metrics.subscribe_clients_total += 1;
        metrics.subscribe_clients_active += 1;
    });

    let result = async {
        write_frame_async(stream, &json!({"status": "ok", "kind": "subscribed"}), &[]).await?;
        let mut driver: Option<GatewayActiveSubscribeDriver> = None;
        while !state.stopped.load(Ordering::SeqCst) {
            if driver.is_none() {
                let observed_activity = state.subscribe_catalog_notifier.sequence();
                let request_for_driver = request.clone();
                let state_for_driver = Arc::clone(&state);
                match run_blocking_request(Arc::clone(&state), move || {
                    state_for_driver.active_subscribe_driver(request_for_driver)
                })
                .await
                {
                    Ok(next_driver) => {
                        driver = Some(next_driver);
                    }
                    Err(error) if subscribe_driver_start_retryable(&error) => {
                        state
                            .subscribe_catalog_notifier
                            .wait_after(
                                observed_activity,
                                DEFAULT_GATEWAY_SUBSCRIBE_ACTIVE_WAIT_TIMEOUT,
                            )
                            .await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            let next_driver = driver.take().unwrap();
            let event =
                run_blocking_request(Arc::clone(&state), move || next_driver.next_event()).await?;
            driver = Some(event.driver);
            match event.event {
                GatewaySubscribeEvent::Idle => {
                    if subscribe_peer_closed(stream)? {
                        break;
                    }
                    continue;
                }
                GatewaySubscribeEvent::Row(row) => {
                    let write_started = Instant::now();
                    let write_result = write_frame_async_with_timeout(
                        stream,
                        &json!({"status": "ok", "kind": "row", "row": row}),
                        &[],
                        state.write_timeout,
                    )
                    .await;
                    record_subscribe_write_elapsed(&state, write_started.elapsed());
                    if let Err(error) = write_result {
                        record_subscribe_write_error(&state, &error);
                        return Err(error);
                    }
                    state.increment_metric(|metrics| metrics.subscribe_rows_delivered_total += 1);
                }
                GatewaySubscribeEvent::Table(next_batch) => {
                    let delivered_rows = next_batch.num_rows() as u64;
                    let payload = run_blocking_request(Arc::clone(&state), move || {
                        encode_ipc_table(&next_batch)
                    })
                    .await?;
                    let write_started = Instant::now();
                    let write_result = write_frame_async_with_timeout(
                        stream,
                        &json!({"status": "ok", "kind": "table"}),
                        &payload,
                        state.write_timeout,
                    )
                    .await;
                    record_subscribe_write_elapsed(&state, write_started.elapsed());
                    if let Err(error) = write_result {
                        record_subscribe_write_error(&state, &error);
                        return Err(error);
                    }
                    state.increment_metric(|metrics| {
                        metrics.subscribe_tables_delivered_total += 1;
                        metrics.subscribe_table_rows_delivered_total += delivered_rows;
                    });
                }
            }
        }
        Ok(())
    }
    .await;

    state.increment_metric(|metrics| {
        metrics.subscribe_clients_active = metrics.subscribe_clients_active.saturating_sub(1);
    });
    result
}

fn record_subscribe_write_elapsed(state: &GatewayState, elapsed: Duration) {
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    state.increment_metric(|metrics| {
        metrics.subscribe_last_write_elapsed_ms = elapsed_ms;
        metrics.subscribe_write_elapsed_ms_total += elapsed_ms;
    });
}

fn record_subscribe_write_error(state: &GatewayState, error: &ZippyError) {
    let reason = match error {
        ZippyError::Io { reason } => reason.clone(),
        _ => error.to_string(),
    };
    state.increment_metric(|metrics| {
        metrics.subscribe_last_close_reason = Some(reason.clone());
        if reason.contains("gateway frame write timed out") {
            metrics.subscribe_write_timeouts_total += 1;
            metrics.subscribe_slow_clients_total += 1;
        }
    });
}

fn subscribe_peer_closed(stream: &tokio::net::TcpStream) -> Result<bool> {
    let mut buffer = [0_u8; 1];
    match stream.try_read(&mut buffer) {
        Ok(0) => Ok(true),
        Ok(_) => Err(ZippyError::InvalidConfig {
            reason: "gateway subscribe connections do not accept client payload after start"
                .to_string(),
        }),
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(ZippyError::Io {
            reason: format!("gateway subscribe peer read failed error=[{}]", error),
        }),
    }
}

fn subscribe_driver_start_retryable(error: &ZippyError) -> bool {
    let message = error.to_string();
    message.contains("stream not found")
        || message.contains("segment descriptor is not published")
        || message.contains("stream is stale")
        || message.contains("stream not registered")
}

async fn run_blocking_request<T: Send + 'static>(
    state: Arc<GatewayState>,
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let Ok(permit) = state.blocking_limit.clone().try_acquire_owned() else {
        state.increment_metric(|metrics| metrics.blocking_requests_rejected_total += 1);
        return Err(ZippyError::Io {
            reason: "gateway blocking request limit exceeded".to_string(),
        });
    };
    state.increment_metric(|metrics| metrics.blocking_requests_active += 1);
    let task_result = tokio::task::spawn_blocking(task).await;
    drop(permit);
    state.increment_metric(|metrics| {
        metrics.blocking_requests_active = metrics.blocking_requests_active.saturating_sub(1);
    });
    task_result.map_err(|error| ZippyError::Io {
        reason: format!("gateway blocking request failed error=[{}]", error),
    })?
}

async fn run_stream_blocking_step<T: Send + 'static>(
    state: Arc<GatewayState>,
    task: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let permit = state
        .blocking_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ZippyError::Io {
            reason: "gateway blocking request limit is closed".to_string(),
        })?;
    state.increment_metric(|metrics| metrics.blocking_requests_active += 1);
    let task_result = tokio::task::spawn_blocking(task).await;
    drop(permit);
    state.increment_metric(|metrics| {
        metrics.blocking_requests_active = metrics.blocking_requests_active.saturating_sub(1);
    });
    task_result.map_err(|error| ZippyError::Io {
        reason: format!("gateway streaming blocking step failed error=[{}]", error),
    })?
}

async fn read_frame_header_async(stream: &mut tokio::net::TcpStream) -> Result<GatewayFrameHeader> {
    let mut prefix = [0u8; 12];
    timeout_io(
        DEFAULT_GATEWAY_HEADER_TIMEOUT,
        "gateway header read timed out timeout_ms=[5000]",
        stream.read_exact(&mut prefix),
    )
    .await?;
    let header_len = u32::from_be_bytes(prefix[0..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..12].try_into().unwrap());
    if header_len > MAX_GATEWAY_HEADER_BYTES {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "gateway header length exceeds limit header_len=[{}] max_header_bytes=[{}]",
                header_len, MAX_GATEWAY_HEADER_BYTES
            ),
        });
    }
    if payload_len > MAX_GATEWAY_PAYLOAD_BYTES as u64 {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "gateway payload length exceeds limit payload_len=[{}] max_payload_bytes=[{}]",
                payload_len, MAX_GATEWAY_PAYLOAD_BYTES
            ),
        });
    }
    let mut header = vec![0u8; header_len];
    timeout_io(
        DEFAULT_GATEWAY_HEADER_TIMEOUT,
        "gateway header read timed out timeout_ms=[5000]",
        stream.read_exact(&mut header),
    )
    .await?;
    let header = serde_json::from_slice::<Value>(&header).map_err(|error| ZippyError::Io {
        reason: format!("failed to decode gateway header error=[{}]", error),
    })?;
    Ok(GatewayFrameHeader {
        header,
        payload_len: payload_len as usize,
    })
}

async fn read_frame_payload_async(
    stream: &mut tokio::net::TcpStream,
    payload_len: usize,
) -> Result<Vec<u8>> {
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        timeout_io(
            DEFAULT_GATEWAY_PAYLOAD_TIMEOUT,
            "gateway payload read timed out timeout_ms=[30000]",
            stream.read_exact(&mut payload),
        )
        .await?;
    }
    Ok(payload)
}

async fn write_frame_async(
    stream: &mut tokio::net::TcpStream,
    header: &Value,
    payload: &[u8],
) -> Result<()> {
    write_frame_async_with_timeout(stream, header, payload, DEFAULT_GATEWAY_WRITE_TIMEOUT).await
}

async fn write_frame_async_with_timeout(
    stream: &mut tokio::net::TcpStream,
    header: &Value,
    payload: &[u8],
    timeout: Duration,
) -> Result<()> {
    let header = serde_json::to_vec(header).map_err(|error| ZippyError::Io {
        reason: format!("failed to encode gateway header error=[{}]", error),
    })?;
    let timeout_reason = format!(
        "gateway frame write timed out timeout_ms=[{}]",
        timeout.as_millis()
    );
    timeout_io(
        timeout,
        timeout_reason.clone(),
        stream.write_all(&(header.len() as u32).to_be_bytes()),
    )
    .await?;
    timeout_io(
        timeout,
        timeout_reason.clone(),
        stream.write_all(&(payload.len() as u64).to_be_bytes()),
    )
    .await?;
    timeout_io(timeout, timeout_reason.clone(), stream.write_all(&header)).await?;
    timeout_io(timeout, timeout_reason, stream.write_all(payload)).await?;
    Ok(())
}

async fn timeout_io<T>(
    timeout: Duration,
    timeout_reason: impl Into<String>,
    future: impl std::future::Future<Output = std::io::Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(io_error(error)),
        Err(_) => Err(ZippyError::Io {
            reason: timeout_reason.into(),
        }),
    }
}

async fn send_control_request_async(
    endpoint: &ControlEndpoint,
    request: ControlRequest,
) -> Result<ControlResponse> {
    match endpoint {
        #[cfg(unix)]
        ControlEndpoint::Unix(path) => {
            let stream = tokio::net::UnixStream::connect(path)
                .await
                .map_err(io_error)?;
            send_control_line_over_async_stream(stream, request).await
        }
        #[cfg(not(unix))]
        ControlEndpoint::Unix(path) => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unix control endpoint is not supported on this platform path=[{}]",
                path.display()
            ),
        }),
        ControlEndpoint::Tcp(addr) => {
            let stream =
                tokio::time::timeout(Duration::from_secs(1), tokio::net::TcpStream::connect(addr))
                    .await
                    .map_err(|_| ZippyError::Io {
                        reason: format!("gateway master tcp connect timed out addr=[{}]", addr),
                    })?
                    .map_err(io_error)?;
            stream.set_nodelay(true).map_err(io_error)?;
            send_control_line_over_async_stream(stream, request).await
        }
    }
}

async fn send_control_line_over_async_stream<S>(
    mut stream: S,
    request: ControlRequest,
) -> Result<ControlResponse>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&request).map_err(json_zippy_error)?;
    stream
        .write_all(payload.as_bytes())
        .await
        .map_err(io_error)?;
    stream.write_all(b"\n").await.map_err(io_error)?;
    stream.flush().await.map_err(io_error)?;

    let mut response_line = String::new();
    let mut reader = tokio::io::BufReader::new(stream);
    reader
        .read_line(&mut response_line)
        .await
        .map_err(io_error)?;
    let response = serde_json::from_str::<ControlResponse>(response_line.trim_end())
        .map_err(json_zippy_error)?;
    match response {
        ControlResponse::Error { reason } => Err(ZippyError::Io { reason }),
        other => Ok(other),
    }
}

fn record_gateway_error_metric(state: &GatewayState, error: &ZippyError) {
    let message = error.to_string();
    state.increment_metric(|metrics| {
        metrics.errors_total += 1;
        if message.contains("payload read timed out") {
            metrics.payload_timeouts_total += 1;
        } else if message.contains("timed out") {
            metrics.request_timeouts_total += 1;
        }
    });
}

fn normalize_collect_stream_metrics(object: &mut serde_json::Map<String, Value>) {
    object.entry("segment_streamed_batches").or_insert(json!(0));
    object.entry("segment_streamed_rows").or_insert(json!(0));
    let Some(scanned_files) = object
        .get("scanned_files")
        .and_then(Value::as_array)
        .cloned()
    else {
        return;
    };
    object.insert("scanned_file_paths".to_string(), json!(scanned_files));
    object.insert("scanned_files".to_string(), json!(scanned_files.len()));
}

fn gateway_row_range_metric_name(name: &str) -> Option<&'static str> {
    match name {
        "head" => Some("head"),
        "tail" => Some("tail"),
        "slice" => Some("slice"),
        _ => None,
    }
}

fn collect_stream_plan_from_header(header: Value) -> Result<GatewayCollectStreamPlan> {
    let source = header
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "collect_stream requires source".to_string(),
        })?
        .to_string();
    let plan = header
        .get("plan")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let row_range_pushdown = collect_plan_row_range_prefix(&plan)?;
    let row_range_residual_start = row_range_pushdown.map_or(0, |(_, start)| start);
    let pushed_filter_count = if row_range_pushdown.is_some() {
        0
    } else {
        collect_plan_leading_filter_count(&plan[row_range_residual_start..])
    };
    let scan_residual_start = row_range_residual_start + pushed_filter_count;
    let scan_pushdown = GatewayScanPushdown {
        filters: plan[row_range_residual_start..scan_residual_start].to_vec(),
        projection_columns: collect_plan_scan_projection_columns(
            &plan[row_range_residual_start..],
        )?,
    };
    let streamable_projection_count =
        collect_plan_streamable_projection_count(&plan[scan_residual_start..])?;
    let residual_start = scan_residual_start + streamable_projection_count;
    let output_projection_columns =
        collect_plan_projection_columns(&plan[scan_residual_start..residual_start]);
    if residual_start < plan.len() {
        return Err(ZippyError::InvalidConfig {
            reason: concat!(
                "collect(stream=True) requires a fully streamable plan; ",
                "use collect() for residual operations"
            )
            .to_string(),
        });
    }
    let chunk_rows = header
        .get("chunk_rows")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(65_536);
    Ok(GatewayCollectStreamPlan {
        source,
        snapshot_id: header
            .get("snapshot_id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        plan,
        row_range_pushdown,
        scan_pushdown,
        output_projection_columns,
        chunk_rows,
    })
}

fn decode_ipc_batches(payload: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader =
        StreamReader::try_new(Cursor::new(payload), None).map_err(|error| ZippyError::Io {
            reason: format!("failed to decode arrow ipc stream error=[{}]", error),
        })?;
    reader
        .map(|batch| {
            batch.map_err(|error| ZippyError::Io {
                reason: format!("failed to read arrow ipc batch error=[{}]", error),
            })
        })
        .collect()
}

fn encode_ipc_table(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to create arrow ipc writer error=[{}]", error),
            }
        })?;
        writer.write(batch).map_err(|error| ZippyError::Io {
            reason: format!("failed to write arrow ipc batch error=[{}]", error),
        })?;
        writer.finish().map_err(|error| ZippyError::Io {
            reason: format!("failed to finish arrow ipc stream error=[{}]", error),
        })?;
    }
    Ok(payload)
}

fn encode_ipc_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut payload, schema).map_err(|error| ZippyError::Io {
                reason: format!("failed to create arrow ipc schema writer error=[{}]", error),
            })?;
        writer.finish().map_err(|error| ZippyError::Io {
            reason: format!("failed to finish arrow ipc schema stream error=[{}]", error),
        })?;
    }
    Ok(payload)
}

fn concat_record_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    concat_batches(&schema, batches.iter()).map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn parallel_persisted_file_record_batches(
    stream: &StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
    query_parallelism: usize,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
    max_pending_file_results: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let files = matching_non_overlapping_persisted_files(stream, scan_pushdown)
        .into_iter()
        .map(persisted_file_path)
        .collect::<Result<Vec<_>>>()?;
    let tasks = files
        .iter()
        .enumerate()
        .map(|(file_index, file_path)| GatewayPersistedScanTask {
            file_index,
            file_path: file_path.clone(),
            projection_columns: scan_pushdown.projection_columns.clone(),
            scan_pushdown: scan_pushdown.clone(),
        })
        .collect::<Vec<_>>();
    if tasks.len() <= 1 || query_parallelism == 1 {
        let mut batches = Vec::new();
        for task in tasks {
            let result = scan_persisted_file_task(task)?;
            *scanned_rows = scanned_rows.saturating_add(result.scanned_rows);
            batches.extend(result.batches);
        }
        scanned_files.extend(files);
        return Ok(batches);
    }
    let mut batches = Vec::new();
    let mut ordered = OrderedGatewayFileResults::new(
        DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS.max(query_parallelism),
    );
    let mut next_task_index = 0usize;
    while next_task_index < tasks.len() {
        let end_index = (next_task_index + query_parallelism).min(tasks.len());
        let chunk = &tasks[next_task_index..end_index];
        let results = std::sync::mpsc::channel::<GatewayPersistedScanThreadResult>();
        let receiver = thread::scope(|scope| {
            let (sender, receiver) = results;
            let handles = chunk
                .iter()
                .cloned()
                .map(|task| {
                    let sender = sender.clone();
                    scope.spawn(move || {
                        let file_index = task.file_index;
                        let result = scan_persisted_file_task(task);
                        let _ =
                            sender.send(GatewayPersistedScanThreadResult { file_index, result });
                    })
                })
                .collect::<Vec<_>>();
            drop(sender);
            let mut join_error = None;
            for handle in handles {
                if handle.join().is_err() && join_error.is_none() {
                    join_error = Some(ZippyError::Io {
                        reason: "gateway persisted scan worker panicked".to_string(),
                    });
                }
            }
            if let Some(error) = join_error {
                return Err(error);
            }
            Ok(receiver)
        })?;
        for thread_result in receiver {
            let result = thread_result.result?;
            if result.file_index != thread_result.file_index {
                return Err(ZippyError::Io {
                    reason: format!(
                        "streaming collect file result index mismatch expected=[{}] actual=[{}]",
                        thread_result.file_index, result.file_index
                    ),
                });
            }
            *scanned_rows = scanned_rows.saturating_add(result.scanned_rows);
            ordered.insert(thread_result.file_index, result.batches)?;
            while let Some(ready) = ordered.pop_ready() {
                batches.extend(ready);
            }
        }
        next_task_index = end_index;
    }
    *max_pending_file_results = ordered.max_observed_pending();
    ordered.finish(tasks.len())?;
    scanned_files.extend(files);
    Ok(batches)
}

fn scan_persisted_file_task(task: GatewayPersistedScanTask) -> Result<GatewayPersistedScanResult> {
    let mut scanned_rows = 0usize;
    let file_batches = read_parquet_record_batches(
        Path::new(&task.file_path),
        task.projection_columns.as_deref(),
        &task.scan_pushdown.filters,
    )?;
    let mut batches = Vec::new();
    for batch in file_batches {
        let batch =
            apply_scan_pushdown_to_record_batch(batch, &task.scan_pushdown, &mut scanned_rows)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(GatewayPersistedScanResult {
        file_index: task.file_index,
        batches,
        scanned_rows,
    })
}

fn start_persisted_scan_cursor(job: GatewayPersistedScanJob) -> Result<GatewayPersistedScanCursor> {
    let GatewayPersistedScanJob {
        task,
        output_projection_columns,
        event_sender,
        buffer_budget,
        cancelled,
    } = job;
    let file_path = task.file_path.clone();
    let (_, reader) = build_parquet_record_batch_reader(
        Path::new(&file_path),
        task.projection_columns.as_deref(),
        &task.scan_pushdown.filters,
        None,
    )?;
    Ok(GatewayPersistedScanCursor {
        task,
        output_projection_columns,
        event_sender,
        buffer_budget,
        cancelled,
        reader,
        scanned_rows: 0,
        budget_wait_started: None,
    })
}

fn advance_persisted_scan_work(work: GatewayPersistedScanWork) -> Result<GatewayPersistedScanStep> {
    let cursor = match work {
        GatewayPersistedScanWork::Start(job) => start_persisted_scan_cursor(job)?,
        GatewayPersistedScanWork::Continue(cursor) => cursor,
    };
    advance_persisted_scan_cursor(cursor)
}

fn advance_persisted_scan_cursor(
    mut cursor: GatewayPersistedScanCursor,
) -> Result<GatewayPersistedScanStep> {
    if cursor.cancelled.load(Ordering::Acquire) {
        return Ok(GatewayPersistedScanStep::Complete);
    }
    let Some(slot) = cursor.buffer_budget.try_acquire_slot() else {
        cursor.budget_wait_started.get_or_insert_with(Instant::now);
        return Ok(GatewayPersistedScanStep::Continue {
            cursor,
            backoff: true,
        });
    };
    if let Some(wait_started) = cursor.budget_wait_started.take() {
        cursor
            .buffer_budget
            .tracker
            .add_wait(wait_started.elapsed());
    }
    let Some(batch) = cursor.reader.next() else {
        drop(slot);
        let event = GatewayPersistedScanEvent::End {
            file_index: cursor.task.file_index,
            scanned_rows: cursor.scanned_rows,
        };
        return match cursor.event_sender.try_send(event) {
            Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {
                Ok(GatewayPersistedScanStep::Complete)
            }
            Err(mpsc::TrySendError::Full(_)) => Err(ZippyError::Io {
                reason: format!(
                    "gateway persisted scan end channel unexpectedly full file_index=[{}]",
                    cursor.task.file_index
                ),
            }),
        };
    };
    let batch = batch.map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to read persisted parquet batch path=[{}] error=[{}]",
            cursor.task.file_path, error
        ),
    })?;
    let batch = apply_scan_pushdown_to_record_batch(
        batch,
        &cursor.task.scan_pushdown,
        &mut cursor.scanned_rows,
    )?;
    if batch.num_rows() == 0 {
        drop(slot);
        return Ok(GatewayPersistedScanStep::Continue {
            cursor,
            backoff: false,
        });
    }
    let batch = project_record_batch(&batch, cursor.output_projection_columns.as_deref())?;
    let buffered_bytes = batch.get_array_memory_size();
    let permit =
        cursor
            .buffer_budget
            .buffered_permit(slot, buffered_bytes, cursor.task.file_index)?;
    let event = GatewayPersistedScanEvent::Batch(GatewayPersistedBufferedBatch {
        batch,
        _permit: Some(permit),
    });
    match cursor.event_sender.try_send(event) {
        Ok(()) => Ok(GatewayPersistedScanStep::Continue {
            cursor,
            backoff: false,
        }),
        Err(mpsc::TrySendError::Disconnected(_)) => Ok(GatewayPersistedScanStep::Complete),
        Err(mpsc::TrySendError::Full(_)) => Err(ZippyError::Io {
            reason: format!(
                "gateway persisted scan batch channel unexpectedly full file_index=[{}]",
                cursor.task.file_index
            ),
        }),
    }
}

fn tail_persisted_file_record_batches(
    stream: &StreamInfo,
    n: usize,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut file_groups_reversed = Vec::new();
    for persisted_file in non_overlapping_persisted_files(stream).into_iter().rev() {
        if remaining == 0 {
            break;
        }
        let file_path = persisted_file_path(persisted_file)?;
        let file_batches = read_parquet_record_batches_tail(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
            remaining,
        )?;
        let raw_rows = file_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        scanned_files.push(file_path);
        let mut filtered_batches = Vec::new();
        remaining = remaining.saturating_sub(raw_rows);
        for batch in file_batches {
            let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
            if batch.num_rows() > 0 {
                filtered_batches.push(batch);
            }
        }
        file_groups_reversed.push(filtered_batches);
    }
    scanned_files.reverse();
    file_groups_reversed.reverse();
    Ok(file_groups_reversed.into_iter().flatten().collect())
}

fn head_persisted_file_record_batches(
    stream: &StreamInfo,
    n: usize,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches = Vec::new();
    for persisted_file in non_overlapping_persisted_files(stream) {
        if remaining == 0 {
            break;
        }
        let file_path = persisted_file_path(persisted_file)?;
        let file_batches = read_parquet_record_batches_head(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
            remaining,
        )?;
        let raw_rows = file_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        scanned_files.push(file_path);
        remaining = remaining.saturating_sub(raw_rows);
        for batch in file_batches {
            let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
    }
    Ok(batches)
}

fn slice_persisted_file_record_batches(
    stream: &StreamInfo,
    offset: usize,
    length: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<(Vec<RecordBatch>, usize, usize)> {
    let files = non_overlapping_persisted_files(stream);
    let mut file_rows = Vec::with_capacity(files.len());
    let mut persisted_rows = 0usize;
    for persisted_file in &files {
        let file_path = persisted_file_path(persisted_file)?;
        let rows = persisted_file
            .get("row_count")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .map(Ok)
            .unwrap_or_else(|| persisted_parquet_row_count(Path::new(&file_path)))?;
        persisted_rows = persisted_rows.saturating_add(rows);
        file_rows.push((persisted_file, file_path, rows));
    }

    let requested_end = length.map(|value| offset.saturating_add(value));
    let mut absolute_position = 0usize;
    let mut selected_raw_rows = 0usize;
    let mut batches = Vec::new();
    for (_, file_path, rows) in file_rows {
        let file_start = absolute_position;
        let file_end = file_start.saturating_add(rows);
        absolute_position = file_end;
        if file_end <= offset {
            continue;
        }
        if requested_end.is_some_and(|end| file_start >= end) {
            break;
        }
        let start_in_file = offset.saturating_sub(file_start);
        let take_length = requested_end.map(|end| {
            end.saturating_sub(file_start)
                .min(rows)
                .saturating_sub(start_in_file)
        });
        if take_length == Some(0) {
            continue;
        }
        let file_batches = read_parquet_record_batches_slice(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
            start_in_file,
            take_length,
        )?;
        let raw_rows = file_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        selected_raw_rows = selected_raw_rows.saturating_add(raw_rows);
        scanned_files.push(file_path);
        for batch in file_batches {
            let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
    }
    Ok((batches, persisted_rows, selected_raw_rows))
}

fn read_parquet_record_batches(
    path: &Path,
    projection_columns: Option<&[String]>,
    filters: &[Value],
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks(path, projection_columns, filters, None)?.1)
}

fn read_parquet_record_batches_tail(
    path: &Path,
    projection_columns: Option<&[String]>,
    n: usize,
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks(
        path,
        projection_columns,
        &[],
        Some(PersistedParquetRowRange::Tail(n)),
    )?
    .1)
}

fn read_parquet_record_batches_head(
    path: &Path,
    projection_columns: Option<&[String]>,
    n: usize,
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks(
        path,
        projection_columns,
        &[],
        Some(PersistedParquetRowRange::Head(n)),
    )?
    .1)
}

fn read_parquet_record_batches_slice(
    path: &Path,
    projection_columns: Option<&[String]>,
    offset: usize,
    length: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks(
        path,
        projection_columns,
        &[],
        Some(PersistedParquetRowRange::Slice { offset, length }),
    )?
    .1)
}

fn persisted_parquet_row_count(path: &Path) -> Result<usize> {
    let file = File::open(path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to open persisted parquet file path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to read persisted parquet metadata path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })?;
    usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
        ZippyError::InvalidConfig {
            reason: format!(
                "persisted parquet row count overflows usize path=[{}]",
                path.display()
            ),
        }
    })
}

#[derive(Clone, Copy)]
enum PersistedParquetRowRange {
    Head(usize),
    Tail(usize),
    Slice {
        offset: usize,
        length: Option<usize>,
    },
}

fn read_parquet_record_batch_chunks(
    path: &Path,
    projection_columns: Option<&[String]>,
    filters: &[Value],
    row_range: Option<PersistedParquetRowRange>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let (schema, reader) =
        build_parquet_record_batch_reader(path, projection_columns, filters, row_range)?;
    let batches = reader
        .map(|batch| {
            batch.map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to read persisted parquet batch path=[{}] error=[{}]",
                    path.display(),
                    error
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, batches))
}

fn build_parquet_record_batch_reader(
    path: &Path,
    projection_columns: Option<&[String]>,
    filters: &[Value],
    row_range: Option<PersistedParquetRowRange>,
) -> Result<(SchemaRef, ParquetRecordBatchReader)> {
    let file = File::open(path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to open persisted parquet file path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create persisted parquet reader path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })?;
    if let Some(projection_columns) = projection_columns {
        let indices = projection_columns
            .iter()
            .map(|column_name| {
                builder
                    .schema()
                    .index_of(column_name)
                    .map_err(|error| ZippyError::SchemaMismatch {
                        reason: format!(
                            "persisted parquet projection column not found path=[{}] column=[{}] error=[{}]",
                            path.display(),
                            column_name,
                            error
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(projection);
    }
    if row_range.is_none() && !filters.is_empty() {
        let row_groups = builder
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(index, row_group)| {
                filters
                    .iter()
                    .all(|filter| {
                        parquet_row_group_may_match_filter(builder.schema(), row_group, filter)
                    })
                    .then_some(index)
            })
            .collect::<Vec<_>>();
        builder = builder.with_row_groups(row_groups);
    }
    if let Some(row_range) = row_range {
        let total_rows =
            usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                ZippyError::InvalidConfig {
                    reason: format!(
                        "persisted parquet row count overflows usize path=[{}]",
                        path.display()
                    ),
                }
            })?;
        let (skipped_rows, selected_rows) = match row_range {
            PersistedParquetRowRange::Head(rows) => (0, rows.min(total_rows)),
            PersistedParquetRowRange::Tail(rows) => {
                let selected = rows.min(total_rows);
                (total_rows.saturating_sub(selected), selected)
            }
            PersistedParquetRowRange::Slice { offset, length } => {
                let skipped = offset.min(total_rows);
                let available = total_rows.saturating_sub(skipped);
                (skipped, length.unwrap_or(available).min(available))
            }
        };
        let mut selectors = Vec::new();
        if skipped_rows > 0 {
            selectors.push(RowSelector::skip(skipped_rows));
        }
        if selected_rows > 0 {
            selectors.push(RowSelector::select(selected_rows));
        }
        builder = builder.with_row_selection(RowSelection::from(selectors));
    }
    let schema = builder.schema().clone();
    let reader = builder
        .with_batch_size(PERSISTED_PARQUET_SCAN_BATCH_SIZE)
        .build()
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to build persisted parquet reader path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })?;
    Ok((schema, reader))
}

fn parquet_row_group_may_match_filter(
    schema: &SchemaRef,
    row_group: &parquet::file::metadata::RowGroupMetaData,
    filter: &Value,
) -> bool {
    let Some(expr) = filter.get("expr") else {
        return true;
    };
    parquet_row_group_may_match_expr(schema, row_group, expr)
}

fn parquet_row_group_may_match_expr(
    schema: &SchemaRef,
    row_group: &parquet::file::metadata::RowGroupMetaData,
    expr: &Value,
) -> bool {
    if query_expr_kind(expr) == Some("is_in") {
        let Some(args) = expr
            .get("args")
            .and_then(Value::as_array)
            .filter(|args| args.len() == 2)
        else {
            return true;
        };
        let Ok(Some(column)) = query_expr_column_name(&args[0]) else {
            return true;
        };
        let Some(values) = literal_value(&args[1]).as_array() else {
            return true;
        };
        let Some((minimum, maximum)) = parquet_row_group_min_max(schema, row_group, column) else {
            return true;
        };
        return values
            .iter()
            .any(|value| numeric_range_may_match(&minimum, &maximum, "eq", value).unwrap_or(true));
    }
    if query_expr_kind(expr) != Some("binary") {
        return true;
    }
    let Some(op) = expr.get("op").and_then(Value::as_str) else {
        return true;
    };
    let Some(args) = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|args| args.len() == 2)
    else {
        return true;
    };
    if op == "and" {
        return parquet_row_group_may_match_expr(schema, row_group, &args[0])
            && parquet_row_group_may_match_expr(schema, row_group, &args[1]);
    }
    if op == "or" {
        return parquet_row_group_may_match_expr(schema, row_group, &args[0])
            || parquet_row_group_may_match_expr(schema, row_group, &args[1]);
    }
    let Ok(Some((column, literal, reverse))) = column_literal_filter_pair(&args[0], &args[1])
    else {
        return true;
    };
    let Some((minimum, maximum)) = parquet_row_group_min_max(schema, row_group, column) else {
        return true;
    };
    let normalized_op = if reverse {
        let Ok(reversed) = reverse_filter_op(op) else {
            return true;
        };
        reversed
    } else {
        op.to_string()
    };
    numeric_range_may_match(&minimum, &maximum, &normalized_op, literal_value(literal))
        .unwrap_or(true)
}

fn parquet_row_group_min_max(
    schema: &SchemaRef,
    row_group: &parquet::file::metadata::RowGroupMetaData,
    column: &str,
) -> Option<(Value, Value)> {
    let column_index = schema.index_of(column).ok()?;
    let statistics = row_group.column(column_index).statistics()?;
    if !statistics.min_is_exact() || !statistics.max_is_exact() {
        return None;
    }
    match statistics {
        Statistics::Int32(values) => Some((json!(*values.min_opt()?), json!(*values.max_opt()?))),
        Statistics::Int64(values) => Some((json!(*values.min_opt()?), json!(*values.max_opt()?))),
        Statistics::Float(values) => Some((json!(*values.min_opt()?), json!(*values.max_opt()?))),
        Statistics::Double(values) => Some((json!(*values.min_opt()?), json!(*values.max_opt()?))),
        _ => None,
    }
}

fn apply_scan_pushdown_to_record_batch(
    batch: RecordBatch,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let mut batch = project_record_batch(&batch, scan_pushdown.projection_columns.as_deref())?;
    *scanned_rows = scanned_rows.saturating_add(batch.num_rows());
    for filter in &scan_pushdown.filters {
        batch = apply_filter(batch, filter)?;
    }
    Ok(batch)
}

fn project_record_batch(
    batch: &RecordBatch,
    projection_columns: Option<&[String]>,
) -> Result<RecordBatch> {
    let Some(projection_columns) = projection_columns else {
        return Ok(batch.clone());
    };
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut arrays = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "scan projection column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(batch.schema().field(index).clone());
        arrays.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build gateway scan projection batch error=[{}]",
            error
        ),
    })
}

fn schema_for_scan_pushdown(
    schema: &SchemaRef,
    scan_pushdown: &GatewayScanPushdown,
) -> Result<SchemaRef> {
    schema_for_projection_columns(schema, scan_pushdown.projection_columns.as_deref())
}

fn schema_for_projection_columns(
    schema: &SchemaRef,
    projection_columns: Option<&[String]>,
) -> Result<SchemaRef> {
    let Some(projection_columns) = projection_columns else {
        return Ok(Arc::clone(schema));
    };
    let mut fields = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index = schema
            .index_of(column_name)
            .map_err(|error| ZippyError::SchemaMismatch {
                reason: format!(
                    "scan projection column not found column=[{}] error=[{}]",
                    column_name, error
                ),
            })?;
        fields.push(schema.field(index).clone());
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn stream_has_persisted_files(stream: &StreamInfo) -> bool {
    stream.persisted_files.iter().any(|item| {
        item.get("file_path")
            .and_then(Value::as_str)
            .is_some_and(|path| !path.is_empty())
    })
}

fn stream_is_persisted_only(stream: &StreamInfo) -> bool {
    stream_has_persisted_files(stream)
        && stream.active_segment_descriptor.is_none()
        && stream.sealed_segments.is_empty()
}

fn non_overlapping_persisted_files(stream: &StreamInfo) -> Vec<&Value> {
    let live_identities = live_segment_identities(stream);
    let mut files = stream
        .persisted_files
        .iter()
        .filter(|item| {
            item.get("file_path")
                .and_then(Value::as_str)
                .is_some_and(|path| !path.is_empty())
        })
        .filter(|item| {
            !segment_identities_overlap(&persisted_segment_identities(item), &live_identities)
        })
        .collect::<Vec<_>>();
    files.sort_by_key(|item| persisted_file_order_key(item));
    files
}

fn matching_non_overlapping_persisted_files<'a>(
    stream: &'a StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
) -> Vec<&'a Value> {
    non_overlapping_persisted_files(stream)
        .into_iter()
        .filter(|item| {
            scan_pushdown
                .filters
                .iter()
                .all(|filter| persisted_file_may_match_filter(item, filter))
        })
        .collect()
}

fn persisted_file_may_match_filter(item: &Value, filter: &Value) -> bool {
    let Some(expr) = filter.get("expr") else {
        return true;
    };
    persisted_file_may_match_expr(item, expr)
}

fn persisted_file_may_match_expr(item: &Value, expr: &Value) -> bool {
    if query_expr_kind(expr) == Some("is_in") {
        return persisted_file_may_match_is_in(item, expr);
    }
    if query_expr_kind(expr) != Some("binary") {
        return true;
    }
    let Some(op) = expr.get("op").and_then(Value::as_str) else {
        return true;
    };
    let Some(args) = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|args| args.len() == 2)
    else {
        return true;
    };
    if op == "and" {
        return persisted_file_may_match_expr(item, &args[0])
            && persisted_file_may_match_expr(item, &args[1]);
    }
    if op == "or" {
        return persisted_file_may_match_expr(item, &args[0])
            || persisted_file_may_match_expr(item, &args[1]);
    }
    let Ok(Some((column, literal, reverse))) = column_literal_filter_pair(&args[0], &args[1])
    else {
        return true;
    };
    let normalized_op = if reverse {
        let Ok(reversed) = reverse_filter_op(op) else {
            return true;
        };
        reversed
    } else {
        op.to_string()
    };
    let literal = literal_value(literal);
    if let Some(partition_value) = item.get("partition").and_then(|value| value.get(column)) {
        if !partition_value_may_match(partition_value, &normalized_op, literal) {
            return false;
        }
    }
    let Some((min_value, max_value)) = persisted_file_min_max(item, column) else {
        return true;
    };
    numeric_range_may_match(min_value, max_value, &normalized_op, literal).unwrap_or(true)
}

fn persisted_file_may_match_is_in(item: &Value, expr: &Value) -> bool {
    let Some(args) = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|args| args.len() == 2)
    else {
        return true;
    };
    let Ok(Some(column)) = query_expr_column_name(&args[0]) else {
        return true;
    };
    let Some(values) = literal_value(&args[1]).as_array() else {
        return true;
    };
    if let Some(partition_value) = item.get("partition").and_then(|value| value.get(column)) {
        return values
            .iter()
            .any(|value| partition_scalar_equality(partition_value, value).unwrap_or(true));
    }
    let Some((min_value, max_value)) = persisted_file_min_max(item, column) else {
        return true;
    };
    values
        .iter()
        .any(|value| numeric_range_may_match(min_value, max_value, "eq", value).unwrap_or(true))
}

fn persisted_file_min_max<'a>(item: &'a Value, column: &str) -> Option<(&'a Value, &'a Value)> {
    if let Some(stats) = item.get("stats").and_then(|stats| stats.get(column)) {
        return Some((stats.get("min")?, stats.get("max")?));
    }
    match column {
        "dt" | "event_ts" => Some((item.get("min_event_ts")?, item.get("max_event_ts")?)),
        "seq" => Some((item.get("min_seq")?, item.get("max_seq")?)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum ExactNumericValue {
    Integer(i128),
    Float(f64),
}

fn exact_numeric_value(value: &Value) -> Option<ExactNumericValue> {
    if let Some(value) = value.as_i64() {
        return Some(ExactNumericValue::Integer(i128::from(value)));
    }
    if let Some(value) = value.as_u64() {
        return Some(ExactNumericValue::Integer(i128::from(value)));
    }
    value
        .as_f64()
        .filter(|value| value.is_finite())
        .map(ExactNumericValue::Float)
}

fn numeric_range_may_match(
    min_value: &Value,
    max_value: &Value,
    op: &str,
    literal: &Value,
) -> Option<bool> {
    match (
        exact_numeric_value(min_value)?,
        exact_numeric_value(max_value)?,
        exact_numeric_value(literal)?,
    ) {
        (
            ExactNumericValue::Integer(minimum),
            ExactNumericValue::Integer(maximum),
            ExactNumericValue::Integer(value),
        ) => Some(range_values_may_match(minimum, maximum, op, value)),
        (
            ExactNumericValue::Float(minimum),
            ExactNumericValue::Float(maximum),
            ExactNumericValue::Float(value),
        ) => Some(range_values_may_match(minimum, maximum, op, value)),
        _ => None,
    }
}

fn range_values_may_match<T: Copy + PartialEq + PartialOrd>(
    minimum: T,
    maximum: T,
    op: &str,
    value: T,
) -> bool {
    match op {
        "eq" => minimum <= value && value <= maximum,
        "ne" => !(minimum == maximum && maximum == value),
        "gt" => maximum > value,
        "ge" => maximum >= value,
        "lt" => minimum < value,
        "le" => minimum <= value,
        _ => true,
    }
}

fn partition_value_may_match(partition_value: &Value, op: &str, literal: &Value) -> bool {
    match op {
        "eq" => partition_scalar_equality(partition_value, literal).unwrap_or(true),
        "ne" => partition_scalar_equality(partition_value, literal) != Some(true),
        _ => numeric_range_may_match(partition_value, partition_value, op, literal).unwrap_or(true),
    }
}

fn partition_scalar_equality(left: &Value, right: &Value) -> Option<bool> {
    match (exact_numeric_value(left), exact_numeric_value(right)) {
        (Some(ExactNumericValue::Integer(left)), Some(ExactNumericValue::Integer(right))) => {
            return Some(left == right);
        }
        (Some(ExactNumericValue::Float(left)), Some(ExactNumericValue::Float(right))) => {
            return Some(left == right);
        }
        (Some(_), Some(_)) => return None,
        (Some(number), None) => {
            return numeric_string_value(right.as_str()?)
                .and_then(|string_number| same_kind_numeric_equality(number, string_number));
        }
        (None, Some(number)) => {
            return numeric_string_value(left.as_str()?)
                .and_then(|string_number| same_kind_numeric_equality(string_number, number));
        }
        (None, None) => {}
    }
    match (left, right) {
        (Value::String(left), Value::String(right)) => Some(left == right),
        (Value::Bool(left), Value::Bool(right)) => Some(left == right),
        (Value::Null, Value::Null) => Some(true),
        _ => None,
    }
}

fn numeric_string_value(value: &str) -> Option<ExactNumericValue> {
    if let Ok(value) = value.parse::<i128>() {
        return Some(ExactNumericValue::Integer(value));
    }
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map(ExactNumericValue::Float)
}

fn same_kind_numeric_equality(left: ExactNumericValue, right: ExactNumericValue) -> Option<bool> {
    match (left, right) {
        (ExactNumericValue::Integer(left), ExactNumericValue::Integer(right)) => {
            Some(left == right)
        }
        (ExactNumericValue::Float(left), ExactNumericValue::Float(right)) => Some(left == right),
        _ => None,
    }
}

fn persisted_file_path(value: &Value) -> Result<String> {
    let file_path = value
        .get("file_path")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "persisted file metadata missing file_path".to_string(),
        })?;
    let Some(persist_root) = value.get("persist_data_root").and_then(Value::as_str) else {
        return Ok(file_path.to_string());
    };
    let root = fs::canonicalize(persist_root).map_err(|error| ZippyError::InvalidConfig {
        reason: format!(
            "failed to canonicalize persisted file root path=[{}] error=[{}]",
            persist_root, error
        ),
    })?;
    let path = PathBuf::from(file_path);
    let canonical_path = fs::canonicalize(&path).map_err(|error| ZippyError::InvalidConfig {
        reason: format!(
            "failed to canonicalize persisted file path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    if !canonical_path.starts_with(&root) {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "persisted file outside persist data root path=[{}] root=[{}]",
                canonical_path.display(),
                root.display()
            ),
        });
    }
    if !canonical_path.is_file() {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "persisted file path is not a file path=[{}]",
                canonical_path.display()
            ),
        });
    }
    Ok(canonical_path.to_string_lossy().to_string())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GatewaySegmentIdentity {
    segment_id: u64,
    generation: u64,
    writer_epoch: Option<u64>,
}

fn live_segment_identities(stream: &StreamInfo) -> Vec<GatewaySegmentIdentity> {
    let mut identities = Vec::new();
    if let Some(descriptor) = stream.active_segment_descriptor.as_ref() {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.push(identity);
        }
    } else {
        return identities;
    }
    for descriptor in &stream.sealed_segments {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.push(identity);
        }
    }
    identities.sort_unstable();
    identities.dedup();
    identities
}

fn persisted_segment_identities(value: &Value) -> Vec<GatewaySegmentIdentity> {
    let mut identities = Vec::new();
    if let Some(source_segments) = value.get("source_segments").and_then(Value::as_array) {
        for source_segment in source_segments {
            if let Some(identity) = segment_identity_from_value(
                source_segment,
                "source_segment_id",
                "source_generation",
            ) {
                identities.push(identity);
            }
        }
    }
    if identities.is_empty() {
        if let Some(identity) =
            segment_identity_from_value(value, "source_segment_id", "source_generation")
        {
            identities.push(identity);
        }
    }
    identities.sort_unstable();
    identities.dedup();
    identities
}

fn segment_identity_from_value(
    value: &Value,
    segment_id_key: &str,
    generation_key: &str,
) -> Option<GatewaySegmentIdentity> {
    Some(GatewaySegmentIdentity {
        segment_id: value.get(segment_id_key)?.as_u64()?,
        generation: value.get(generation_key)?.as_u64()?,
        writer_epoch: value.get("writer_epoch").and_then(Value::as_u64),
    })
}

fn segment_identities_overlap(
    persisted: &[GatewaySegmentIdentity],
    live: &[GatewaySegmentIdentity],
) -> bool {
    persisted.iter().any(|persisted_identity| {
        live.iter()
            .any(|live_identity| segment_identity_matches(*persisted_identity, *live_identity))
    })
}

fn segment_identity_matches(left: GatewaySegmentIdentity, right: GatewaySegmentIdentity) -> bool {
    if left.segment_id != right.segment_id || left.generation != right.generation {
        return false;
    }
    match (left.writer_epoch, right.writer_epoch) {
        (Some(left_epoch), Some(right_epoch)) => left_epoch == right_epoch,
        _ => true,
    }
}

fn persisted_file_order_key(value: &Value) -> (u64, u64, u64, u64, String) {
    (
        json_u64_order_value(value.get("writer_epoch")),
        json_u64_order_value(value.get("source_segment_id")),
        json_u64_order_value(value.get("source_generation")),
        json_u64_order_value(value.get("created_at")),
        value
            .get("file_path")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    )
}

fn json_u64_order_value(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or_default()
}

fn live_segment_record_batches(
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(sealed_descriptors.len() + 1);
    for sealed_descriptor in sealed_descriptors {
        let batch = descriptor_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    let active_batch = descriptor_record_batch_until(
        descriptor,
        segment_schema,
        Some(active_committed_row_high_watermark),
        scan_pushdown,
        scanned_rows,
    )?;
    if active_batch.num_rows() > 0 {
        batches.push(active_batch);
    }
    Ok(batches)
}

#[allow(clippy::too_many_arguments)]
fn tail_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
    n: usize,
    acquire_leases: bool,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches_reversed = Vec::new();

    let active_batch = descriptor_tail_record_batch_until(
        descriptor,
        segment_schema.clone(),
        n,
        Some(active_committed_row_high_watermark),
        scan_pushdown,
        scanned_rows,
    )?;
    remaining = remaining.saturating_sub(active_batch.num_rows());
    if active_batch.num_rows() > 0 {
        batches_reversed.push(active_batch);
    }

    for sealed_descriptor in sealed_descriptors.iter().rev() {
        if remaining == 0 {
            break;
        }
        if acquire_leases {
            leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        }
        let batch = descriptor_tail_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            remaining,
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        remaining = remaining.saturating_sub(batch.num_rows());
        if batch.num_rows() > 0 {
            batches_reversed.push(batch);
        }
    }

    batches_reversed.reverse();
    Ok(batches_reversed)
}

#[allow(clippy::too_many_arguments)]
fn head_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    n: usize,
    acquire_leases: bool,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches = Vec::new();

    for sealed_descriptor in sealed_descriptors {
        if remaining == 0 {
            break;
        }
        if acquire_leases {
            leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        }
        let (batch, _) = descriptor_slice_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            0,
            Some(remaining),
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        remaining = remaining.saturating_sub(batch.num_rows());
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if remaining > 0 {
        if acquire_leases {
            leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
        }
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(descriptor, segment_schema.clone())?;
        let (active_batch, _) = descriptor_slice_record_batch_until(
            descriptor,
            segment_schema,
            0,
            Some(remaining),
            Some(active_committed_row_high_watermark),
            scan_pushdown,
            scanned_rows,
        )?;
        if active_batch.num_rows() > 0 {
            batches.push(active_batch);
        }
    }

    Ok(batches)
}

#[allow(clippy::too_many_arguments)]
fn slice_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    offset: usize,
    length: Option<usize>,
    acquire_leases: bool,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut absolute_position = 0usize;
    let mut collected_rows = 0usize;
    let mut batches = Vec::new();

    for sealed_descriptor in sealed_descriptors {
        if length.is_some_and(|value| collected_rows >= value) {
            break;
        }
        if acquire_leases {
            leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        }
        let remaining_length = length.map(|value| value.saturating_sub(collected_rows));
        let start_row = offset.saturating_sub(absolute_position);
        let (batch, committed_rows) = descriptor_slice_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            start_row,
            remaining_length,
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        absolute_position = absolute_position.saturating_add(committed_rows);
        collected_rows = collected_rows.saturating_add(batch.num_rows());
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if length.is_none_or(|value| collected_rows < value) {
        if acquire_leases {
            leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
        }
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(descriptor, segment_schema.clone())?;
        let remaining_length = length.map(|value| value.saturating_sub(collected_rows));
        let start_row = offset.saturating_sub(absolute_position);
        let (active_batch, _) = descriptor_slice_record_batch_until(
            descriptor,
            segment_schema,
            start_row,
            remaining_length,
            Some(active_committed_row_high_watermark),
            scan_pushdown,
            scanned_rows,
        )?;
        if active_batch.num_rows() > 0 {
            batches.push(active_batch);
        }
    }

    Ok(batches)
}

fn descriptor_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let (batch, _) = descriptor_slice_record_batch_until(
        descriptor,
        segment_schema,
        0,
        None,
        end_row_limit,
        scan_pushdown,
        scanned_rows,
    )?;
    Ok(batch)
}

fn descriptor_tail_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    n: usize,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let committed = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let start_row = committed.saturating_sub(n);
    let span = RowSpanView::from_active_descriptor(active_descriptor, start_row, committed)
        .map_err(|status| ZippyError::InvalidState { status })?;
    row_span_record_batch(span, scan_pushdown, scanned_rows)
}

fn descriptor_slice_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    start_row: usize,
    length: Option<usize>,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<(RecordBatch, usize)> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let committed = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let start_row = start_row.min(committed);
    let available = committed.saturating_sub(start_row);
    let row_count = length.map_or(available, |value| value.min(available));
    let end_row = start_row + row_count;
    let span = RowSpanView::from_active_descriptor(active_descriptor, start_row, end_row)
        .map_err(|status| ZippyError::InvalidState { status })?;
    let batch = row_span_record_batch(span, scan_pushdown, scanned_rows)?;
    Ok((batch, committed))
}

fn row_span_record_batch(
    span: RowSpanView,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let mut batch =
        project_row_span_record_batch(&span, scan_pushdown.projection_columns.as_deref())?;
    *scanned_rows = scanned_rows.saturating_add(batch.num_rows());
    for filter in &scan_pushdown.filters {
        batch = apply_filter(batch, filter)?;
    }
    Ok(batch)
}

fn project_row_span_record_batch(
    span: &RowSpanView,
    projection_columns: Option<&[String]>,
) -> Result<RecordBatch> {
    let Some(projection_columns) = projection_columns else {
        return span.as_record_batch().map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        });
    };
    let source_schema = span.schema_ref();
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut arrays = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index =
            source_schema
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "scan projection column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(source_schema.field(index).clone());
        arrays.push(span.column(column_name).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build gateway scan projection batch error=[{}]",
            error
        ),
    })
}

fn active_descriptor_with_committed_row_count(
    descriptor: &Value,
    segment_schema: CompiledSchema,
) -> Result<(usize, ActiveSegmentDescriptor)> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    let reader = ActiveSegmentReader::from_descriptor_envelope(
        &descriptor_envelope,
        segment_schema.clone(),
        layout.clone(),
    )
    .map_err(segment_zippy_error)?;
    let committed = reader.committed_row_count().map_err(segment_zippy_error)?;
    let active_descriptor =
        ActiveSegmentDescriptor::from_envelope_bytes(&descriptor_envelope, segment_schema, layout)
            .map_err(|error| ZippyError::InvalidConfig {
                reason: error.to_string(),
            })?;
    Ok((committed, active_descriptor))
}

fn active_committed_row_high_watermark(
    descriptor: &Value,
    segment_schema: CompiledSchema,
) -> Result<usize> {
    active_segment_reader_from_descriptor(descriptor, segment_schema)?
        .committed_row_count()
        .map_err(segment_zippy_error)
}

fn active_segment_reader_from_descriptor(
    descriptor: &Value,
    segment_schema: CompiledSchema,
) -> Result<ActiveSegmentReader> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, segment_schema, layout)
        .map_err(segment_zippy_error)
}

fn acquire_segment_reader_lease_from_master(
    master: Arc<GatewayAsyncMasterClient>,
    source: &str,
    descriptor: &Value,
) -> Result<SegmentReaderLeaseGuard> {
    let (segment_id, generation) = descriptor_segment_identity(descriptor)?;
    let lease_id = master.acquire_segment_reader_lease_blocking(source, segment_id, generation)?;
    Ok(SegmentReaderLeaseGuard {
        master,
        source: source.to_string(),
        lease_id: Some(lease_id),
    })
}

fn descriptor_row_capacity(descriptor: &Value) -> Result<usize> {
    let row_capacity = descriptor
        .get("row_capacity")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing row_capacity".to_string(),
        })?;
    usize::try_from(row_capacity).map_err(|_| ZippyError::InvalidConfig {
        reason: "segment descriptor row_capacity overflows usize".to_string(),
    })
}

fn descriptor_segment_identity(descriptor: &Value) -> Result<(u64, u64)> {
    let segment_id = descriptor
        .get("segment_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing segment_id".to_string(),
        })?;
    let generation = descriptor
        .get("generation")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing generation".to_string(),
        })?;
    Ok((segment_id, generation))
}

fn arrow_schema_from_stream_metadata(schema: &Value) -> Result<Schema> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "stream schema metadata missing fields".to_string(),
        })?;
    let fields = fields
        .iter()
        .map(|field| {
            let name = field.get("name").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "stream schema field missing name".to_string(),
                }
            })?;
            let segment_type = field
                .get("segment_type")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing segment_type".to_string(),
                })?;
            let nullable = field
                .get("nullable")
                .and_then(Value::as_bool)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing nullable".to_string(),
                })?;
            let timezone = field.get("timezone").and_then(Value::as_str);
            let data_type = parse_arrow_schema_metadata_data_type(segment_type, timezone)?;
            let metadata = string_map_from_json_value(field.get("metadata"))?;
            Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = string_map_from_json_value(schema.get("metadata"))?;
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn compile_segment_schema_from_stream_metadata(schema: &Value) -> Result<CompiledSchema> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "stream schema metadata missing fields".to_string(),
        })?;
    let columns = fields
        .iter()
        .map(|field| {
            let name = field.get("name").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "stream schema field missing name".to_string(),
                }
            })?;
            let segment_type = field
                .get("segment_type")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing segment_type".to_string(),
                })?;
            let nullable = field
                .get("nullable")
                .and_then(Value::as_bool)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing nullable".to_string(),
                })?;
            let timezone = field.get("timezone").and_then(Value::as_str);
            let data_type = parse_segment_schema_metadata_data_type(segment_type, timezone)?;
            Ok(if nullable {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    compile_segment_schema(&columns).map_err(|error| ZippyError::InvalidConfig {
        reason: error.to_string(),
    })
}

fn parse_arrow_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> Result<DataType> {
    match segment_type {
        "int64" => Ok(DataType::Int64),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        "timestamp_ns_tz" => {
            let timezone = timezone.ok_or_else(|| ZippyError::InvalidConfig {
                reason: "timestamp_ns_tz stream schema field missing timezone".to_string(),
            })?;
            Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(timezone.into()),
            ))
        }
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported stream field type segment_type=[{}]",
                segment_type
            ),
        }),
    }
}

fn parse_segment_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> Result<ColumnType> {
    match segment_type {
        "int64" => Ok(ColumnType::Int64),
        "float64" => Ok(ColumnType::Float64),
        "utf8" => Ok(ColumnType::Utf8),
        "timestamp_ns_tz" => {
            let timezone = timezone.ok_or_else(|| ZippyError::InvalidConfig {
                reason: "timestamp_ns_tz stream schema field missing timezone".to_string(),
            })?;
            Ok(ColumnType::timestamp_ns_tz(timezone))
        }
        "timestamp_ns" => Err(ZippyError::InvalidConfig {
            reason: "segment stream timestamp columns must include an explicit timezone"
                .to_string(),
        }),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported segment stream field type segment_type=[{}]",
                segment_type
            ),
        }),
    }
}

fn string_map_from_json_value(value: Option<&Value>) -> Result<HashMap<String, String>> {
    let Some(value) = value else {
        return Ok(HashMap::new());
    };
    let object = value.as_object().ok_or_else(|| ZippyError::InvalidConfig {
        reason: "stream schema metadata must be an object".to_string(),
    })?;
    object
        .iter()
        .map(|(key, value)| {
            let value = value.as_str().ok_or_else(|| ZippyError::InvalidConfig {
                reason: "stream schema metadata values must be strings".to_string(),
            })?;
            Ok((key.clone(), value.to_string()))
        })
        .collect()
}

fn json_zippy_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn unexpected_response(expected: &str, response: ControlResponse) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected control response expected=[{}] actual=[{}]",
            expected, response
        ),
    }
}

fn unexpected_watch_resource(expected: &str, resource: WatchResource) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected control watch resource expected=[{}] actual=[{:?}]",
            expected, resource
        ),
    }
}

fn segment_zippy_error(error: zippy_segment_store::ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn apply_collect_plan(mut batch: RecordBatch, plan: &[Value]) -> Result<RecordBatch> {
    for op in plan {
        let op_name =
            op.get("op")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "collect plan requires op".to_string(),
                })?;
        match op_name {
            "select" => batch = apply_select(batch, op)?,
            "filter" => batch = apply_filter(batch, op)?,
            "head" => batch = apply_head(batch, op)?,
            "tail" => batch = apply_tail(batch, op)?,
            "slice" => batch = apply_slice(batch, op)?,
            "sort" => batch = apply_sort(batch, op)?,
            "drop" => batch = apply_drop(batch, op)?,
            "rename" => batch = apply_rename(batch, op)?,
            other => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("unsupported native gateway collect plan op=[{}]", other),
                });
            }
        }
    }
    Ok(batch)
}

fn subscribe_filter_plan(filter: &Value) -> Option<Value> {
    if filter.is_null() {
        return None;
    }
    Some(json!({"op": "filter", "expr": filter.clone()}))
}

fn subscribe_kind_name(mode: GatewaySubscribeMode) -> &'static str {
    match mode {
        GatewaySubscribeMode::Rows => "subscribe_rows",
        GatewaySubscribeMode::Table => "subscribe_table",
    }
}

fn parse_positive_subscribe_usize(
    header: &Value,
    field_name: &str,
    request_kind: &str,
) -> Result<Option<usize>> {
    parse_positive_subscribe_u64(header, field_name, request_kind)
        .map(|value| value.map(|item| item as usize))
}

fn parse_positive_subscribe_u64(
    header: &Value,
    field_name: &str,
    request_kind: &str,
) -> Result<Option<u64>> {
    let Some(value) = header.get(field_name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(unsigned) = value.as_u64() else {
        return Err(ZippyError::InvalidConfig {
            reason: format!("{} {} must be positive", request_kind, field_name),
        });
    };
    if unsigned == 0 {
        return Err(ZippyError::InvalidConfig {
            reason: format!("{} {} must be positive", request_kind, field_name),
        });
    }
    Ok(Some(unsigned))
}

fn parse_subscribe_instrument_ids(value: Option<&Value>) -> Result<Option<HashSet<String>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let values = match value {
        Value::String(instrument_id) => vec![instrument_id.clone()],
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| ZippyError::InvalidConfig {
                        reason: "subscribe instrument_ids must contain strings".to_string(),
                    })
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(ZippyError::InvalidConfig {
                reason: "subscribe instrument_ids must be a string or string array".to_string(),
            });
        }
    };
    if values.is_empty() {
        return Ok(Some(HashSet::new()));
    }
    Ok(Some(values.into_iter().collect()))
}

fn apply_optional_subscribe_filter(
    batch: RecordBatch,
    filter: Option<&Value>,
) -> Result<RecordBatch> {
    let Some(filter) = filter else {
        return Ok(batch);
    };
    apply_filter(batch, filter)
}

fn apply_subscribe_count_tail(batch: RecordBatch, count: Option<usize>) -> RecordBatch {
    let Some(count) = count else {
        return batch;
    };
    let row_count = count.min(batch.num_rows());
    if row_count >= batch.num_rows() {
        return batch;
    }
    batch.slice(batch.num_rows() - row_count, row_count)
}

fn record_batch_row_matches_instrument_ids(
    batch: &RecordBatch,
    row_index: usize,
    instrument_ids: Option<&HashSet<String>>,
) -> Result<bool> {
    let Some(instrument_ids) = instrument_ids else {
        return Ok(true);
    };
    let Ok(column_index) = batch.schema().index_of("instrument_id") else {
        return Ok(false);
    };
    let column = batch.column(column_index);
    if column.is_null(row_index) {
        return Ok(false);
    }
    let instruments = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: "subscribe instrument_id column must be utf8".to_string(),
        })?;
    Ok(instrument_ids.contains(instruments.value(row_index)))
}

fn record_batch_row_to_json(batch: &RecordBatch, row_index: usize) -> Result<Value> {
    let mut object = serde_json::Map::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        object.insert(
            field.name().clone(),
            arrow_value_to_json(column.as_ref(), field.data_type(), row_index)?,
        );
    }
    Ok(Value::Object(object))
}

fn arrow_value_to_json(array: &dyn Array, data_type: &DataType, row_index: usize) -> Result<Value> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }
    match data_type {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast utf8 subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ZippyError::SchemaMismatch {
                    reason: "failed to downcast int64 subscribe column".to_string(),
                }
            })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast float64 subscribe column".to_string(),
                })?;
            serde_json::Number::from_f64(array.value(row_index))
                .map(Value::Number)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "subscribe row contains non-finite float64 value".to_string(),
                })
        }
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast boolean subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast date32 subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast date64 subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast second timestamp subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast millisecond timestamp subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast microsecond timestamp subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| ZippyError::SchemaMismatch {
                    reason: "failed to downcast nanosecond timestamp subscribe column".to_string(),
                })?;
            Ok(json!(array.value(row_index)))
        }
        other => Err(ZippyError::SchemaMismatch {
            reason: format!(
                "unsupported subscribe row column type data_type=[{}]",
                other
            ),
        }),
    }
}

fn collect_plan_row_range_prefix(
    plan: &[Value],
) -> Result<Option<(GatewayRowRangePushdown, usize)>> {
    if plan.is_empty() {
        return Ok(None);
    }
    let Some(op) = plan[0].get("op").and_then(Value::as_str) else {
        return Ok(None);
    };
    let pushdown = match op {
        "tail" => collect_plan_usize(&plan[0], "n")
            .map(GatewayRowRangePushdown::Tail)
            .map(Some)?,
        "head" => collect_plan_usize(&plan[0], "n")
            .map(GatewayRowRangePushdown::Head)
            .map(Some)?,
        "slice" => {
            let offset = collect_plan_usize(&plan[0], "offset")?;
            let length = match plan[0].get("length") {
                Some(Value::Null) | None => None,
                Some(_) => Some(collect_plan_usize(&plan[0], "length")?),
            };
            Some(GatewayRowRangePushdown::Slice { offset, length })
        }
        _ => None,
    };
    Ok(pushdown.map(|value| (value, 1)))
}

fn collect_plan_filter_ops(plan: &[Value]) -> Vec<Value> {
    plan.iter()
        .filter(|op| op.get("op").and_then(Value::as_str) == Some("filter"))
        .cloned()
        .collect()
}

fn collect_plan_leading_filter_count(plan: &[Value]) -> usize {
    plan.iter()
        .take_while(|op| op.get("op").and_then(Value::as_str) == Some("filter"))
        .count()
}

fn collect_plan_streamable_projection_count(plan: &[Value]) -> Result<usize> {
    let Some(op) = plan.first() else {
        return Ok(0);
    };
    if op.get("op").and_then(Value::as_str) != Some("select") {
        return Ok(0);
    }
    let exprs =
        op.get("exprs")
            .and_then(Value::as_array)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "select plan requires exprs".to_string(),
            })?;
    let all_columns = exprs.iter().all(|expr| {
        expr.get("kind").and_then(Value::as_str) == Some("col")
            && expr.get("value").and_then(Value::as_str).is_some()
    });
    if all_columns {
        Ok(1)
    } else {
        Ok(0)
    }
}

fn collect_plan_projection_columns(plan: &[Value]) -> Option<Vec<String>> {
    let select = plan
        .iter()
        .rev()
        .find(|op| op.get("op").and_then(Value::as_str) == Some("select"))?;
    let exprs = select.get("exprs").and_then(Value::as_array)?;
    let columns = exprs
        .iter()
        .filter_map(|expr| {
            if expr.get("kind").and_then(Value::as_str) != Some("col") {
                return None;
            }
            expr.get("value")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    Some(columns)
}

fn collect_plan_scan_projection_columns(plan: &[Value]) -> Result<Option<Vec<String>>> {
    if !plan
        .iter()
        .any(|op| op.get("op").and_then(Value::as_str) == Some("select"))
    {
        return Ok(None);
    }
    let mut columns = Vec::new();
    for op in plan {
        match op.get("op").and_then(Value::as_str) {
            Some("filter") => {
                if let Some(expr) = op.get("expr") {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            Some("select") => {
                let exprs = op.get("exprs").and_then(Value::as_array).ok_or_else(|| {
                    ZippyError::InvalidConfig {
                        reason: "select plan requires exprs".to_string(),
                    }
                })?;
                for expr in exprs {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            Some("sort") => {
                let exprs = op.get("by").and_then(Value::as_array).ok_or_else(|| {
                    ZippyError::InvalidConfig {
                        reason: "sort plan requires by".to_string(),
                    }
                })?;
                for expr in exprs {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            _ => {}
        }
    }
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(columns))
}

fn collect_query_expr_columns(expr: &Value, columns: &mut Vec<String>) -> Result<()> {
    match query_expr_kind(expr) {
        Some("col") => {
            let column = expr.get("value").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "column expression requires string value".to_string(),
                }
            })?;
            if !columns.iter().any(|existing| existing == column) {
                columns.push(column.to_string());
            }
        }
        Some("binary") => {
            let args = expr.get("args").and_then(Value::as_array).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "binary expression requires args".to_string(),
                }
            })?;
            for arg in args {
                collect_query_expr_columns(arg, columns)?;
            }
        }
        Some("literal") => {}
        Some(_) | None => {}
    }
    Ok(())
}

fn apply_select(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let exprs =
        op.get("exprs")
            .and_then(Value::as_array)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "select plan requires exprs".to_string(),
            })?;
    let mut fields = Vec::with_capacity(exprs.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let column_name = expr
            .get("value")
            .and_then(Value::as_str)
            .filter(|_| expr.get("kind").and_then(Value::as_str) == Some("col"))
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "native gateway select currently supports column expressions".to_string(),
            })?;
        let index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "select column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(batch.schema().field(index).clone());
        columns.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway select batch error=[{}]", error),
    })
}

fn apply_filter(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let expr = op.get("expr").ok_or_else(|| ZippyError::InvalidConfig {
        reason: "filter plan requires expr".to_string(),
    })?;
    let mask = evaluate_filter_expr(&batch, expr)?;
    filter_record_batch(&batch, &mask).map_err(|error| ZippyError::Io {
        reason: format!("failed to filter gateway batch error=[{}]", error),
    })
}

fn evaluate_filter_expr(batch: &RecordBatch, expr: &Value) -> Result<BooleanArray> {
    let kind =
        expr.get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "filter expression requires kind".to_string(),
            })?;
    if kind == "is_in" {
        return evaluate_is_in_filter_expr(batch, expr);
    }
    if kind != "binary" {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "native gateway filter currently supports binary expressions kind=[{}]",
                kind
            ),
        });
    }
    let op = expr
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "filter binary expression requires op".to_string(),
        })?;
    let args = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|items| items.len() == 2)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "filter binary expression requires two args".to_string(),
        })?;
    if op == "and" || op == "or" {
        let left = evaluate_filter_expr(batch, &args[0])?;
        let right = evaluate_filter_expr(batch, &args[1])?;
        let mask = if op == "and" {
            and(&left, &right)
        } else {
            or(&left, &right)
        }
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to combine gateway filter masks error=[{}]", error),
        })?;
        return Ok(mask);
    }
    compare_filter_expr(batch, op, &args[0], &args[1])
}

fn evaluate_is_in_filter_expr(batch: &RecordBatch, expr: &Value) -> Result<BooleanArray> {
    let args = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|items| items.len() == 2)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "is_in expression requires two args".to_string(),
        })?;
    let column_name =
        query_expr_column_name(&args[0])?.ok_or_else(|| ZippyError::InvalidConfig {
            reason: "is_in expression requires column target".to_string(),
        })?;
    let values = args[1]
        .get("value")
        .and_then(Value::as_array)
        .filter(|_| query_expr_kind(&args[1]) == Some("literal"))
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "is_in expression requires literal value list".to_string(),
        })?;
    let column_index =
        batch
            .schema()
            .index_of(column_name)
            .map_err(|error| ZippyError::SchemaMismatch {
                reason: format!(
                    "filter column not found column=[{}] error=[{}]",
                    column_name, error
                ),
            })?;
    let column = batch.column(column_index);
    let membership = GatewayIsInMembership::from_json_values(column.data_type().clone(), values)?;
    let mask = match column.data_type() {
        DataType::Utf8 => {
            let column = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 array must downcast to StringArray");
            BooleanArray::from(
                (0..column.len())
                    .map(|index| {
                        !column.is_null(index) && membership.contains_string(column.value(index))
                    })
                    .collect::<Vec<_>>(),
            )
        }
        DataType::Float64 => {
            let column = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array must downcast to Float64Array");
            BooleanArray::from(
                (0..column.len())
                    .map(|index| {
                        !column.is_null(index) && membership.contains_f64(column.value(index))
                    })
                    .collect::<Vec<_>>(),
            )
        }
        DataType::Int64 => {
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array must downcast to Int64Array");
            BooleanArray::from(
                (0..column.len())
                    .map(|index| {
                        !column.is_null(index) && membership.contains_i64(column.value(index))
                    })
                    .collect::<Vec<_>>(),
            )
        }
        DataType::Boolean => {
            let column = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array must downcast to BooleanArray");
            BooleanArray::from(
                (0..column.len())
                    .map(|index| {
                        !column.is_null(index) && membership.contains_bool(column.value(index))
                    })
                    .collect::<Vec<_>>(),
            )
        }
        other => {
            return Err(ZippyError::InvalidConfig {
                reason: format!("unsupported gateway is_in filter type=[{:?}]", other),
            });
        }
    };
    Ok(mask)
}

enum GatewayIsInMembership {
    Strings(HashSet<String>),
    Float64(HashSet<u64>),
    Int64(HashSet<i64>),
    Boolean { allow_true: bool, allow_false: bool },
}

impl GatewayIsInMembership {
    fn from_json_values(data_type: DataType, values: &[Value]) -> Result<Self> {
        match data_type {
            DataType::Utf8 => Ok(Self::Strings(
                values
                    .iter()
                    .map(|value| {
                        value.as_str().map(str::to_string).ok_or_else(|| {
                            ZippyError::InvalidConfig {
                                reason: "string is_in literal values must be strings".to_string(),
                            }
                        })
                    })
                    .collect::<Result<HashSet<_>>>()?,
            )),
            DataType::Float64 => Ok(Self::Float64(
                values
                    .iter()
                    .map(|value| {
                        value.as_f64().map(Self::float_key).ok_or_else(|| {
                            ZippyError::InvalidConfig {
                                reason: "float is_in literal values must be numeric".to_string(),
                            }
                        })
                    })
                    .collect::<Result<HashSet<_>>>()?,
            )),
            DataType::Int64 => Ok(Self::Int64(
                values
                    .iter()
                    .map(|value| {
                        value.as_i64().ok_or_else(|| ZippyError::InvalidConfig {
                            reason: "int is_in literal values must be integers".to_string(),
                        })
                    })
                    .collect::<Result<HashSet<_>>>()?,
            )),
            DataType::Boolean => {
                let mut allow_true = false;
                let mut allow_false = false;
                for value in values {
                    match value.as_bool() {
                        Some(true) => allow_true = true,
                        Some(false) => allow_false = true,
                        None => {
                            return Err(ZippyError::InvalidConfig {
                                reason: "boolean is_in literal values must be boolean".to_string(),
                            });
                        }
                    }
                }
                Ok(Self::Boolean {
                    allow_true,
                    allow_false,
                })
            }
            other => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported gateway is_in filter type=[{:?}]", other),
            }),
        }
    }

    fn contains_string(&self, value: &str) -> bool {
        match self {
            Self::Strings(values) => values.contains(value),
            _ => false,
        }
    }

    fn contains_f64(&self, value: f64) -> bool {
        match self {
            Self::Float64(values) => values.contains(&Self::float_key(value)),
            _ => false,
        }
    }

    fn contains_i64(&self, value: i64) -> bool {
        match self {
            Self::Int64(values) => values.contains(&value),
            _ => false,
        }
    }

    fn contains_bool(&self, value: bool) -> bool {
        match self {
            Self::Boolean {
                allow_true,
                allow_false,
            } => {
                if value {
                    *allow_true
                } else {
                    *allow_false
                }
            }
            _ => false,
        }
    }

    fn float_key(value: f64) -> u64 {
        if value == 0.0 {
            0.0_f64.to_bits()
        } else {
            value.to_bits()
        }
    }
}

fn compare_filter_expr(
    batch: &RecordBatch,
    op: &str,
    left: &Value,
    right: &Value,
) -> Result<BooleanArray> {
    if let Some((column_name, literal, reverse)) = column_literal_filter_pair(left, right)? {
        let column_index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "filter column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        let column = batch.column(column_index);
        let literal = literal_array_for_type(column.data_type(), literal, batch.num_rows())?;
        let op = if reverse {
            reverse_filter_op(op)?
        } else {
            op.to_string()
        };
        let result = match op.as_str() {
            "eq" => eq(column, &literal),
            "ne" => neq(column, &literal),
            "gt" => gt(column, &literal),
            "ge" => gt_eq(column, &literal),
            "lt" => lt(column, &literal),
            "le" => lt_eq(column, &literal),
            other => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("unsupported gateway filter op=[{}]", other),
                });
            }
        };
        return result.map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to compare gateway filter expression error=[{}]",
                error
            ),
        });
    }
    Err(ZippyError::InvalidConfig {
        reason: "native gateway filter requires column-literal comparison".to_string(),
    })
}

fn column_literal_filter_pair<'a>(
    left: &'a Value,
    right: &'a Value,
) -> Result<Option<(&'a str, &'a Value, bool)>> {
    if let Some(column_name) = query_expr_column_name(left)? {
        if query_expr_kind(right) == Some("literal") {
            return Ok(Some((column_name, right, false)));
        }
    }
    if let Some(column_name) = query_expr_column_name(right)? {
        if query_expr_kind(left) == Some("literal") {
            return Ok(Some((column_name, left, true)));
        }
    }
    Ok(None)
}

fn literal_value(expr: &Value) -> &Value {
    expr.get("value").unwrap_or(&Value::Null)
}

fn query_expr_kind(expr: &Value) -> Option<&str> {
    expr.get("kind").and_then(Value::as_str)
}

fn query_expr_column_name(expr: &Value) -> Result<Option<&str>> {
    if query_expr_kind(expr) != Some("col") {
        return Ok(None);
    }
    expr.get("value")
        .and_then(Value::as_str)
        .map(Some)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "column expression requires string value".to_string(),
        })
}

fn reverse_filter_op(op: &str) -> Result<String> {
    let reversed = match op {
        "eq" => "eq",
        "ne" => "ne",
        "gt" => "lt",
        "ge" => "le",
        "lt" => "gt",
        "le" => "ge",
        other => {
            return Err(ZippyError::InvalidConfig {
                reason: format!("unsupported gateway filter op=[{}]", other),
            });
        }
    };
    Ok(reversed.to_string())
}

fn literal_array_for_type(data_type: &DataType, value: &Value, len: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let value = literal_value(value)
                .as_str()
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "string filter literal must be a string".to_string(),
                })?
                .to_string();
            Ok(Arc::new(StringArray::from(vec![value; len])))
        }
        DataType::Float64 => {
            let value = literal_value(value)
                .as_f64()
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "float filter literal must be numeric".to_string(),
                })?;
            Ok(Arc::new(Float64Array::from(vec![value; len])))
        }
        DataType::Int64 => {
            let value = literal_value(value)
                .as_i64()
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "int filter literal must be integer".to_string(),
                })?;
            Ok(Arc::new(Int64Array::from(vec![value; len])))
        }
        DataType::Boolean => {
            let value =
                literal_value(value)
                    .as_bool()
                    .ok_or_else(|| ZippyError::InvalidConfig {
                        reason: "boolean filter literal must be boolean".to_string(),
                    })?;
            Ok(Arc::new(BooleanArray::from(vec![value; len])))
        }
        DataType::Timestamp(unit, timezone) => {
            let literal_type = value.get("literal_type").and_then(Value::as_str);
            if literal_type != Some("timestamp_ns") {
                return Err(ZippyError::InvalidConfig {
                    reason: "timestamp filter literal must be typed timestamp_ns".to_string(),
                });
            }
            let epoch_ns = value.get("value").and_then(Value::as_i64).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "timestamp filter literal value must be integer epoch ns".to_string(),
                }
            })?;
            let converted = match unit {
                TimeUnit::Second => epoch_ns / 1_000_000_000,
                TimeUnit::Millisecond => epoch_ns / 1_000_000,
                TimeUnit::Microsecond => epoch_ns / 1_000,
                TimeUnit::Nanosecond => epoch_ns,
            };
            let array: ArrayRef = match unit {
                TimeUnit::Second => {
                    let array = TimestampSecondArray::from(vec![converted; len]);
                    match timezone {
                        Some(tz) => Arc::new(array.with_timezone(tz.clone())) as ArrayRef,
                        None => Arc::new(array) as ArrayRef,
                    }
                }
                TimeUnit::Millisecond => {
                    let array = TimestampMillisecondArray::from(vec![converted; len]);
                    match timezone {
                        Some(tz) => Arc::new(array.with_timezone(tz.clone())) as ArrayRef,
                        None => Arc::new(array) as ArrayRef,
                    }
                }
                TimeUnit::Microsecond => {
                    let array = TimestampMicrosecondArray::from(vec![converted; len]);
                    match timezone {
                        Some(tz) => Arc::new(array.with_timezone(tz.clone())) as ArrayRef,
                        None => Arc::new(array) as ArrayRef,
                    }
                }
                TimeUnit::Nanosecond => {
                    let array = TimestampNanosecondArray::from(vec![converted; len]);
                    match timezone {
                        Some(tz) => Arc::new(array.with_timezone(tz.clone())) as ArrayRef,
                        None => Arc::new(array) as ArrayRef,
                    }
                }
            };
            Ok(array)
        }
        DataType::Date32 => {
            let epoch_ns = typed_timestamp_ns_literal(value)?;
            let days = epoch_ns / 86_400_000_000_000;
            let days = i32::try_from(days).map_err(|error| ZippyError::InvalidConfig {
                reason: format!("date32 filter literal overflow error=[{}]", error),
            })?;
            Ok(Arc::new(Date32Array::from(vec![days; len])))
        }
        DataType::Date64 => {
            let epoch_ns = typed_timestamp_ns_literal(value)?;
            Ok(Arc::new(Date64Array::from(vec![epoch_ns / 1_000_000; len])))
        }
        other => Err(ZippyError::InvalidConfig {
            reason: format!("unsupported gateway filter literal type=[{:?}]", other),
        }),
    }
}

fn typed_timestamp_ns_literal(value: &Value) -> Result<i64> {
    let literal_type = value.get("literal_type").and_then(Value::as_str);
    if literal_type != Some("timestamp_ns") {
        return Err(ZippyError::InvalidConfig {
            reason: "temporal filter literal must be typed timestamp_ns".to_string(),
        });
    }
    value
        .get("value")
        .and_then(Value::as_i64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "temporal filter literal value must be integer epoch ns".to_string(),
        })
}

fn apply_head(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let n = collect_plan_usize(op, "n")?;
    Ok(batch.slice(0, n.min(batch.num_rows())))
}

fn apply_tail(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let n = collect_plan_usize(op, "n")?;
    let length = n.min(batch.num_rows());
    Ok(batch.slice(batch.num_rows().saturating_sub(length), length))
}

fn apply_slice(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let offset = collect_plan_usize(op, "offset")?;
    let offset = offset.min(batch.num_rows());
    let available = batch.num_rows().saturating_sub(offset);
    let length = match op.get("length") {
        Some(Value::Null) | None => available,
        Some(_) => collect_plan_usize(op, "length")?.min(available),
    };
    Ok(batch.slice(offset, length))
}

fn apply_drop(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let columns =
        op.get("columns")
            .and_then(Value::as_array)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "drop plan requires columns".to_string(),
            })?;
    let drop_names = columns
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "drop column names must be strings".to_string(),
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let fields = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !drop_names.contains(field.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let columns = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !drop_names.contains(field.name().as_str()))
        .map(|(index, _)| batch.column(index).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway drop batch error=[{}]", error),
    })
}

fn apply_rename(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let mapping = op
        .get("mapping")
        .and_then(Value::as_object)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "rename plan requires mapping".to_string(),
        })?;
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            mapping
                .get(field.name())
                .and_then(Value::as_str)
                .map(|name| field.as_ref().clone().with_name(name))
                .unwrap_or_else(|| field.as_ref().clone())
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).map_err(|error| {
        ZippyError::Io {
            reason: format!("failed to build gateway rename batch error=[{}]", error),
        }
    })
}

fn apply_sort(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let by = op
        .get("by")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "sort plan requires by".to_string(),
        })?;
    if by.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "sort plan requires at least one key".to_string(),
        });
    }
    let descending = collect_sort_descending(op, by.len())?;
    let mut sort_columns = Vec::with_capacity(by.len());
    for (index, expr) in by.iter().enumerate() {
        let column_name = collect_plan_column_expr(expr, "sort")?;
        let column_index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "sort column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        sort_columns.push(SortColumn {
            values: batch.column(column_index).clone(),
            options: Some(SortOptions {
                descending: descending[index],
                nulls_first: false,
            }),
        });
    }
    let indices = lexsort_to_indices(&sort_columns, None).map_err(|error| ZippyError::Io {
        reason: format!("failed to sort gateway batch error=[{}]", error),
    })?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                reason: format!("failed to reorder gateway column error=[{}]", error),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway sorted batch error=[{}]", error),
    })
}

fn collect_sort_descending(op: &Value, key_count: usize) -> Result<Vec<bool>> {
    match op.get("descending") {
        Some(Value::Bool(value)) => Ok(vec![*value; key_count]),
        Some(Value::Array(values)) => {
            if values.len() != key_count {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "sort descending length mismatch keys=[{}] descending=[{}]",
                        key_count,
                        values.len()
                    ),
                });
            }
            values
                .iter()
                .map(|value| {
                    value.as_bool().ok_or_else(|| ZippyError::InvalidConfig {
                        reason: "sort descending values must be booleans".to_string(),
                    })
                })
                .collect()
        }
        Some(Value::Null) | None => Ok(vec![false; key_count]),
        Some(_) => Err(ZippyError::InvalidConfig {
            reason: "sort descending must be a boolean or boolean list".to_string(),
        }),
    }
}

fn collect_plan_usize(op: &Value, name: &str) -> Result<usize> {
    let value = op
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "collect plan requires non-negative integer field=[{}]",
                name
            ),
        })?;
    usize::try_from(value).map_err(|error| ZippyError::InvalidConfig {
        reason: format!(
            "collect plan integer overflow field=[{}] error=[{}]",
            name, error
        ),
    })
}

fn collect_plan_column_expr<'a>(expr: &'a Value, op_name: &str) -> Result<&'a str> {
    expr.get("value")
        .and_then(Value::as_str)
        .filter(|_| expr.get("kind").and_then(Value::as_str) == Some("col"))
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "native gateway {} currently supports column expressions",
                op_name
            ),
        })
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("tcp://")
        .unwrap_or(endpoint)
        .split_once('/')
        .map(|(endpoint, _)| endpoint)
        .unwrap_or_else(|| endpoint.strip_prefix("tcp://").unwrap_or(endpoint))
        .to_string()
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

pub fn crate_name() -> &'static str {
    "zippy-gateway"
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, TimestampNanosecondArray};
    use parquet::arrow::ArrowWriter;
    use std::io::{BufRead, BufReader, Write};

    fn test_gateway_config() -> GatewayServerConfig {
        GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: ControlEndpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        }
    }

    #[test]
    fn gateway_query_parallelism_is_validated_and_reported() {
        let gateway = GatewayServer::new_with_query_limits(
            test_gateway_config(),
            8,
            7,
            256 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap();

        assert_eq!(gateway.metrics()["max_parallelism"], json!(8));
        assert_eq!(gateway.metrics()["query_parallelism"], json!(7));
        assert_eq!(
            gateway.metrics()["max_query_buffer_bytes"],
            json!(256 * 1024 * 1024)
        );
        assert_eq!(
            gateway.metrics()["query_max_buffered_bytes"],
            json!(64 * 1024 * 1024)
        );
        assert_eq!(gateway.metrics()["query_buffer_reserved_bytes"], json!(0));
        let reservation = Arc::clone(&gateway.state.persisted_buffer_limit)
            .try_acquire_many_owned(64 * 1024 * 1024)
            .unwrap();
        assert_eq!(
            gateway.metrics()["query_buffer_reserved_bytes"],
            json!(64 * 1024 * 1024)
        );
        drop(reservation);
        assert_eq!(gateway.metrics()["query_buffer_reserved_bytes"], json!(0));

        let error = GatewayServer::new_with_parallelism(test_gateway_config(), 4, 5)
            .err()
            .expect("query parallelism above max must fail");
        assert!(error
            .to_string()
            .contains("query parallelism must be between 1 and max_parallelism"));

        let error = GatewayServer::new_with_query_limits(
            test_gateway_config(),
            4,
            4,
            64 * 1024 * 1024,
            128 * 1024 * 1024,
        )
        .err()
        .expect("query buffer budget above max must fail");
        assert!(error
            .to_string()
            .contains("query max_buffered_bytes must be between 1 and max_query_buffer_bytes"));
    }

    #[test]
    fn gateway_writer_creation_fetches_writer_epoch_with_stream_status_request() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = ControlEndpoint::Tcp(listener.local_addr().unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let response_schema = Arc::clone(&schema);
        let server = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut line = String::new();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                reader.read_line(&mut line).unwrap();

                let request: ControlRequest = serde_json::from_str(line.trim_end()).unwrap();
                let mut done = false;
                let response = match request {
                    ControlRequest::RegisterProcess(RegisterProcessRequest { app }) => {
                        assert_eq!(app, "zippy_gateway");
                        ControlResponse::ProcessRegistered {
                            process_id: "proc_gateway".to_string(),
                            process_token: "token_gateway".to_string(),
                        }
                    }
                    ControlRequest::RegisterStream(request) => {
                        assert_eq!(request.stream_name, "gateway_status_ticks");
                        ControlResponse::StreamRegistered {
                            stream_name: request.stream_name,
                        }
                    }
                    ControlRequest::RegisterSource(request) => {
                        assert_eq!(request.source_name, "gateway.gateway_status_ticks");
                        assert_eq!(request.output_stream, "gateway_status_ticks");
                        ControlResponse::SourceRegistered {
                            source_name: request.source_name,
                        }
                    }
                    ControlRequest::GetStreamStatus(request) => {
                        assert_eq!(request.stream_name, "gateway_status_ticks");
                        ControlResponse::StreamFetched(zippy_core::GetStreamResponse {
                            stream: stream_info_for_gateway_status_test(
                                "gateway_status_ticks",
                                Arc::clone(&response_schema),
                                3,
                            ),
                        })
                    }
                    ControlRequest::GetStream(request) => {
                        let response = ControlResponse::Error {
                            reason: format!(
                                "gateway writer epoch preflight must use GetStreamStatus stream_name=[{}]",
                                request.stream_name
                            ),
                        };
                        let response = serde_json::to_string(&response).unwrap();
                        stream.write_all(response.as_bytes()).unwrap();
                        stream.write_all(b"\n").unwrap();
                        break;
                    }
                    ControlRequest::PublishSegmentDescriptor(request) => {
                        assert_eq!(request.stream_name, "gateway_status_ticks");
                        assert_eq!(request.descriptor["writer_epoch"], json!(3));
                        done = true;
                        ControlResponse::SegmentDescriptorPublished {
                            stream_name: request.stream_name,
                        }
                    }
                    other => panic!("unexpected control request request=[{other:?}]"),
                };

                let response = serde_json::to_string(&response).unwrap();
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(b"\n").unwrap();

                if done {
                    break;
                }
            }
        });

        let gateway = GatewayServer::new(GatewayServerConfig {
            endpoint: "127.0.0.1:0".to_string(),
            master_endpoint: endpoint,
            token: None,
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        })
        .unwrap();

        let result = gateway
            .state
            .create_writer("gateway_status_ticks", Arc::clone(&schema));

        server.join().unwrap();
        if let Err(error) = result {
            panic!("create_writer failed error=[{error}]");
        }
    }

    fn stream_info_for_gateway_status_test(
        stream_name: &str,
        schema: SchemaRef,
        writer_epoch: u64,
    ) -> StreamInfo {
        StreamInfo {
            stream_name: stream_name.to_string(),
            schema: schema_metadata(&schema),
            schema_hash: canonical_schema_hash(&schema),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(DEFAULT_STREAM_TABLE_ROW_CAPACITY),
            sealed_segments: Vec::new(),
            persisted_files: Vec::new(),
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: Some("proc_gateway".to_string()),
            writer_epoch,
            reader_count: 0,
            status: "active".to_string(),
        }
    }

    #[test]
    fn materialized_stream_producer_splits_batches_by_chunk_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let mut producer = GatewayCollectStreamProducer::materialized(batch, 2);

        let first = producer.next_batch().unwrap().unwrap();
        let second = producer.next_batch().unwrap().unwrap();
        let end = producer.next_batch().unwrap();

        assert_eq!(first.num_rows(), 2);
        assert_eq!(second.num_rows(), 1);
        assert!(end.is_none());
    }

    #[test]
    fn persisted_stream_producer_defers_file_scan_until_first_batch() {
        let temp = tempfile::tempdir().unwrap();
        let parquet_path = temp.path().join("deferred.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut stream = stream_info_for_gateway_status_test("deferred_ticks", schema, 1);
        stream.persisted_files = vec![json!({
            "file_path": parquet_path.to_string_lossy(),
            "row_count": 1,
            "source_segment_id": 1,
            "source_generation": 0
        })];
        stream.status = "stale".to_string();
        stream.writer_process_id = None;

        let mut producer = GatewayCollectStreamProducer::persisted_serial(
            &stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            1,
            GatewayPersistedScanResources {
                pool: Arc::new(GatewayPersistedScanPool::new(1).unwrap()),
                parallelism: 1,
                query_buffer_budget_bytes: 32 * 1024 * 1024,
            },
        )
        .unwrap();
        std::fs::remove_file(&parquet_path).unwrap();

        let error = producer.next_batch().unwrap_err();

        assert!(error
            .to_string()
            .contains("failed to open persisted parquet file"));
    }

    #[test]
    fn persisted_stream_producer_does_not_prefetch_past_in_flight_window() {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let parquet_paths = (0_i64..3)
            .map(|value| {
                let parquet_path = temp.path().join(format!("wave-{value}.parquet"));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
                )
                .unwrap();
                let file = File::create(&parquet_path).unwrap();
                let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                parquet_path
            })
            .collect::<Vec<_>>();
        let mut stream = stream_info_for_gateway_status_test("wave_ticks", schema, 1);
        stream.persisted_files = parquet_paths
            .iter()
            .enumerate()
            .map(|(index, parquet_path)| {
                json!({
                    "file_path": parquet_path.to_string_lossy(),
                    "row_count": 1,
                    "source_segment_id": index + 1,
                    "source_generation": 0
                })
            })
            .collect();
        stream.status = "stale".to_string();
        stream.writer_process_id = None;

        let mut producer = GatewayCollectStreamProducer::persisted_serial(
            &stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            1,
            GatewayPersistedScanResources {
                pool: Arc::new(GatewayPersistedScanPool::new(2).unwrap()),
                parallelism: 2,
                query_buffer_budget_bytes: 32 * 1024 * 1024,
            },
        )
        .unwrap();

        let first = producer.next_batch().unwrap().unwrap();
        std::fs::remove_file(&parquet_paths[2]).unwrap();
        let second = producer.next_batch().unwrap().unwrap();
        let GatewayCollectStreamProducer::Persisted {
            source: GatewayPersistedBatchSource::Streaming(scanner),
            ..
        } = &producer
        else {
            panic!("expected streaming persisted producer");
        };
        assert!(scanner.tasks.is_empty());
        assert_eq!(scanner.file_streams.len(), 2);
        let error = producer.next_batch().unwrap_err();

        let first_values = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second_values = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first_values.value(0), 0);
        assert_eq!(second_values.value(0), 1);
        assert!(error
            .to_string()
            .contains("failed to open persisted parquet file"));
        assert_eq!(producer.metrics()["file_scan_parallelism"], json!(2));
        assert_eq!(producer.metrics()["max_in_flight_file_scans"], json!(2));
        assert_eq!(
            producer.metrics()["file_buffer_budget_bytes"],
            json!(16 * 1024 * 1024)
        );
        assert!(producer.metrics()["max_buffered_bytes"].as_u64().unwrap() <= 32 * 1024 * 1024);
        assert!(
            producer.metrics()["max_pending_file_results"]
                .as_u64()
                .unwrap()
                <= 2
        );
    }

    #[test]
    fn persisted_stream_producer_bounds_buffered_batches_by_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let parquet_path = temp.path().join("byte-budget.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let row_count = PERSISTED_PARQUET_SCAN_BATCH_SIZE * 4 + 3;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(
                (0..row_count).map(|value| value as i64),
            )) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut stream = stream_info_for_gateway_status_test("byte_budget_ticks", schema, 1);
        stream.persisted_files = vec![json!({
            "file_path": parquet_path.to_string_lossy(),
            "row_count": row_count,
            "source_segment_id": 1,
            "source_generation": 0
        })];
        stream.status = "stale".to_string();
        stream.writer_process_id = None;
        let query_buffer_budget = 32 * 1024;
        let mut producer = GatewayCollectStreamProducer::persisted_serial(
            &stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            PERSISTED_PARQUET_SCAN_BATCH_SIZE,
            GatewayPersistedScanResources {
                pool: Arc::new(GatewayPersistedScanPool::new(1).unwrap()),
                parallelism: 1,
                query_buffer_budget_bytes: query_buffer_budget,
            },
        )
        .unwrap();

        let mut returned_rows = 0usize;
        while let Some(batch) = producer.next_batch().unwrap() {
            returned_rows += batch.num_rows();
        }
        let metrics = producer.metrics();

        assert_eq!(returned_rows, row_count);
        assert_eq!(
            metrics["query_buffer_budget_bytes"],
            json!(query_buffer_budget)
        );
        assert_eq!(
            metrics["file_buffer_budget_bytes"],
            json!(query_buffer_budget)
        );
        assert!(metrics["max_buffered_bytes"].as_u64().unwrap() > 0);
        assert!(
            metrics["max_buffered_bytes"].as_u64().unwrap()
                <= u64::try_from(query_buffer_budget).unwrap()
        );
        assert_eq!(metrics["buffered_bytes"], json!(0));
    }

    #[test]
    fn persisted_stream_producer_rejects_batch_larger_than_file_budget() {
        let temp = tempfile::tempdir().unwrap();
        let parquet_path = temp.path().join("oversized-batch.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0_i64..1024)) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut stream = stream_info_for_gateway_status_test("oversized_batch_ticks", schema, 1);
        stream.persisted_files = vec![json!({
            "file_path": parquet_path.to_string_lossy(),
            "row_count": batch.num_rows(),
            "source_segment_id": 1,
            "source_generation": 0
        })];
        stream.status = "stale".to_string();
        stream.writer_process_id = None;
        let mut producer = GatewayCollectStreamProducer::persisted_serial(
            &stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            PERSISTED_PARQUET_SCAN_BATCH_SIZE,
            GatewayPersistedScanResources {
                pool: Arc::new(GatewayPersistedScanPool::new(1).unwrap()),
                parallelism: 1,
                query_buffer_budget_bytes: 1024,
            },
        )
        .unwrap();

        let error = producer.next_batch().unwrap_err();

        assert!(error
            .to_string()
            .contains("persisted scan batch exceeds file buffer budget"));
    }

    #[test]
    fn persisted_stream_backpressure_does_not_pin_shared_scan_worker() {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let blocked_path = temp.path().join("blocked.parquet");
        let blocked_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0_i64..2048)) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&blocked_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&blocked_batch).unwrap();
        writer.close().unwrap();
        let ready_path = temp.path().join("ready.parquet");
        let ready_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![99_i64])) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&ready_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&ready_batch).unwrap();
        writer.close().unwrap();
        let mut blocked_stream =
            stream_info_for_gateway_status_test("blocked_budget_ticks", Arc::clone(&schema), 1);
        blocked_stream.persisted_files = vec![json!({
            "file_path": blocked_path.to_string_lossy(),
            "row_count": blocked_batch.num_rows(),
            "source_segment_id": 1,
            "source_generation": 0
        })];
        blocked_stream.status = "stale".to_string();
        blocked_stream.writer_process_id = None;
        let mut ready_stream = stream_info_for_gateway_status_test("ready_budget_ticks", schema, 1);
        ready_stream.persisted_files = vec![json!({
            "file_path": ready_path.to_string_lossy(),
            "row_count": ready_batch.num_rows(),
            "source_segment_id": 1,
            "source_generation": 0
        })];
        ready_stream.status = "stale".to_string();
        ready_stream.writer_process_id = None;
        let pool = Arc::new(GatewayPersistedScanPool::new(1).unwrap());
        let mut blocked_producer = GatewayCollectStreamProducer::persisted_serial(
            &blocked_stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            PERSISTED_PARQUET_SCAN_BATCH_SIZE,
            GatewayPersistedScanResources {
                pool: Arc::clone(&pool),
                parallelism: 1,
                query_buffer_budget_bytes: 32 * 1024,
            },
        )
        .unwrap();
        let mut ready_producer = GatewayCollectStreamProducer::persisted_serial(
            &ready_stream,
            &GatewayScanPushdown::default(),
            None,
            None,
            1,
            GatewayPersistedScanResources {
                pool,
                parallelism: 1,
                query_buffer_budget_bytes: 32 * 1024,
            },
        )
        .unwrap();

        let first = blocked_producer.next_batch().unwrap().unwrap();
        assert_eq!(first.num_rows(), PERSISTED_PARQUET_SCAN_BATCH_SIZE);
        let (sender, receiver) = mpsc::channel();
        let ready_join = thread::spawn(move || {
            let result = ready_producer.next_batch().map(|batch| {
                batch.map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0)
                })
            });
            let _ = sender.send(result);
        });

        let ready_result = receiver.recv_timeout(Duration::from_secs(1));
        drop(blocked_producer);
        ready_join.join().unwrap();

        assert_eq!(ready_result.unwrap().unwrap(), Some(99));
    }

    #[test]
    fn tail_persisted_file_batches_preserve_batch_order_within_file() {
        let temp = tempfile::tempdir().unwrap();
        let parquet_path = temp.path().join("tail-order.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let values = (0_i64..(PERSISTED_PARQUET_SCAN_BATCH_SIZE as i64 + 2)).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )
        .unwrap();
        let file = File::create(&parquet_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let stream = StreamInfo {
            stream_name: "tail_order_ticks".to_string(),
            schema: schema_metadata(&batch.schema()),
            schema_hash: canonical_schema_hash(&batch.schema()),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(4096),
            sealed_segments: Vec::new(),
            persisted_files: vec![json!({
                "file_path": parquet_path.to_string_lossy(),
                "row_count": batch.num_rows(),
                "source_segment_id": 1,
                "source_generation": 0
            })],
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 0,
            reader_count: 0,
            status: "active".to_string(),
        };
        let mut scanned_rows = 0usize;
        let mut scanned_files = Vec::new();
        let batches = tail_persisted_file_record_batches(
            &stream,
            PERSISTED_PARQUET_SCAN_BATCH_SIZE + 1,
            &GatewayScanPushdown::default(),
            &mut scanned_rows,
            &mut scanned_files,
        )
        .unwrap();
        let collected = concat_record_batches(batch.schema(), batches).unwrap();
        let seq = collected
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(seq[0], 1);
        assert_eq!(
            seq.last().copied(),
            Some(PERSISTED_PARQUET_SCAN_BATCH_SIZE as i64 + 1)
        );
        assert!(seq.windows(2).all(|window| window[0] < window[1]));
    }

    #[test]
    fn ordered_file_results_emit_in_file_index_order() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch_one = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .unwrap();
        let batch_two = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef],
        )
        .unwrap();
        let mut results = OrderedGatewayFileResults::new(2);

        results.insert(1, vec![batch_two]).unwrap();
        assert!(results.pop_ready().is_none());
        results.insert(0, vec![batch_one]).unwrap();

        let first = results.pop_ready().unwrap();
        let second = results.pop_ready().unwrap();

        assert_eq!(
            first[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            second[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        results.finish(2).unwrap();
    }

    #[test]
    fn ordered_file_results_reject_duplicate_and_stale_indices() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .unwrap();
        let mut results = OrderedGatewayFileResults::new(2);

        results.insert(0, vec![batch.clone()]).unwrap();
        assert!(results.insert(0, vec![batch.clone()]).is_err());
        assert!(results.pop_ready().is_some());
        assert!(results.insert(0, vec![batch]).is_err());
    }

    #[test]
    fn ordered_file_results_reject_unfinished_gap() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef],
        )
        .unwrap();
        let mut results = OrderedGatewayFileResults::new(2);

        results.insert(1, vec![batch]).unwrap();

        assert!(results.finish(2).is_err());
    }

    #[test]
    fn gateway_filter_accepts_typed_timestamp_literal() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
                false,
            ),
            Field::new("seq", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(vec![
                        1778459399000000000_i64,
                        1778459400000000000_i64,
                    ])
                    .with_timezone("Asia/Shanghai"),
                ) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
            ],
        )
        .unwrap();
        let op = json!({
            "op": "filter",
            "expr": {
                "kind": "binary",
                "op": "ge",
                "args": [
                    {"kind": "col", "value": "dt"},
                    {
                        "kind": "literal",
                        "literal_type": "timestamp_ns",
                        "value": 1778459400000000000_i64,
                        "timezone": "Asia/Shanghai",
                        "unit": "ns"
                    }
                ]
            }
        });

        let result = apply_filter(batch, &op).unwrap();

        let seq = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(seq.value(0), 2);
    }

    #[test]
    fn gateway_is_in_filter_uses_compiled_membership_for_common_types() {
        let string_values = (0..256)
            .map(|index| json!(format!("IF{index:04}")))
            .collect::<Vec<_>>();
        let string_membership =
            GatewayIsInMembership::from_json_values(DataType::Utf8, &string_values).unwrap();
        assert!(string_membership.contains_string("IF0255"));
        assert!(!string_membership.contains_string("IF9999"));

        let int_values = (0..256).map(|index| json!(index)).collect::<Vec<_>>();
        let int_membership =
            GatewayIsInMembership::from_json_values(DataType::Int64, &int_values).unwrap();
        assert!(int_membership.contains_i64(255));
        assert!(!int_membership.contains_i64(9999));

        let bool_membership =
            GatewayIsInMembership::from_json_values(DataType::Boolean, &[json!(true)]).unwrap();
        assert!(bool_membership.contains_bool(true));
        assert!(!bool_membership.contains_bool(false));
    }

    #[test]
    #[ignore = "micro profile; run explicitly with --ignored --nocapture"]
    fn gateway_is_in_large_literal_membership_profile() {
        let literal_count = 50_000;
        let row_count = 200_000;
        let values = (0..literal_count)
            .map(|index| json!(format!("IF{index:05}")))
            .collect::<Vec<_>>();

        let build_start = std::time::Instant::now();
        let membership = GatewayIsInMembership::from_json_values(DataType::Utf8, &values).unwrap();
        let build_elapsed = build_start.elapsed();

        let contains_start = std::time::Instant::now();
        let matches = (0..row_count)
            .filter(|index| membership.contains_string(&format!("IF{:05}", index % literal_count)))
            .count();
        let contains_elapsed = contains_start.elapsed();

        eprintln!(
            "gateway is_in profile literal_count=[{}] row_count=[{}] build_us=[{}] contains_us=[{}]",
            literal_count,
            row_count,
            build_elapsed.as_micros(),
            contains_elapsed.as_micros()
        );
        assert_eq!(matches, row_count);
    }

    #[test]
    fn persisted_parquet_reader_exposes_batches_before_concat() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        let row_count = PERSISTED_PARQUET_SCAN_BATCH_SIZE + 1;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from((0..row_count as i64).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0; row_count])) as ArrayRef,
            ],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("ticks.parquet");
        {
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let batches = read_parquet_record_batches(&path, None, &[]).unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            row_count
        );
    }

    #[test]
    fn persisted_parquet_reader_prunes_non_matching_row_groups() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("row-groups.parquet");
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_size(2)
            .build();
        {
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let filters = vec![json!({
            "op": "filter",
            "expr": {
                "kind": "binary",
                "op": "ge",
                "args": [
                    {"kind": "col", "value": "seq"},
                    {"kind": "literal", "value": 2}
                ]
            }
        })];

        let batches = read_parquet_record_batches(&path, None, &filters).unwrap();
        let table = concat_record_batches(batch.schema(), batches).unwrap();
        let values = table
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();

        assert_eq!(values, vec![2, 3]);
    }

    #[test]
    fn persisted_parquet_row_range_returns_no_rows_for_zero_length() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1])) as ArrayRef],
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("zero-length.parquet");
        {
            let file = File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let head = read_parquet_record_batches_head(&path, None, 0).unwrap();
        let tail = read_parquet_record_batches_tail(&path, None, 0).unwrap();
        let slice = read_parquet_record_batches_slice(&path, None, 1, Some(0)).unwrap();

        assert!(head.is_empty());
        assert!(tail.is_empty());
        assert!(slice.is_empty());
    }

    #[test]
    fn gateway_writer_handle_is_cloneable_for_per_stream_locking() {
        fn assert_clone<T: Clone>() {}

        assert_clone::<GatewayTableWriterHandle>();
    }

    #[test]
    fn persisted_only_stream_keeps_files_overlapping_stale_sealed_segments() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let stream = StreamInfo {
            stream_name: "persisted_only_ticks".to_string(),
            schema: schema_metadata(&schema),
            schema_hash: canonical_schema_hash(&schema),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(4096),
            sealed_segments: vec![json!({
                "segment_id": 1,
                "generation": 0,
                "writer_epoch": 7
            })],
            persisted_files: vec![json!({
                "file_path": "/tmp/part-000001.parquet",
                "row_count": 1,
                "source_segment_id": 1,
                "source_generation": 0,
                "writer_epoch": 7
            })],
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 7,
            reader_count: 0,
            status: "registered".to_string(),
        };

        let files = non_overlapping_persisted_files(&stream);

        assert_eq!(files.len(), 1);
    }

    #[test]
    fn persisted_overlap_distinguishes_writer_epochs() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let stream = StreamInfo {
            stream_name: "writer_restart_ticks".to_string(),
            schema: schema_metadata(&schema),
            schema_hash: canonical_schema_hash(&schema),
            data_path: "segment".to_string(),
            descriptor_generation: 1,
            active_segment_descriptor: Some(json!({
                "segment_id": 1,
                "generation": 0,
                "writer_epoch": 2
            })),
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(4096),
            sealed_segments: Vec::new(),
            persisted_files: vec![json!({
                "file_path": "/tmp/epoch-1-segment-1.parquet",
                "row_count": 1,
                "source_segment_id": 1,
                "source_generation": 0,
                "writer_epoch": 1
            })],
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: Some("writer-2".to_string()),
            writer_epoch: 2,
            reader_count: 0,
            status: "writer_attached".to_string(),
        };

        let files = non_overlapping_persisted_files(&stream);

        assert_eq!(files.len(), 1);
    }

    #[test]
    fn persisted_file_order_keeps_writer_epochs_contiguous() {
        let mut files = [
            json!({
                "writer_epoch": 2,
                "source_segment_id": 1,
                "source_generation": 0,
                "created_at": 3,
                "file_path": "epoch-2-segment-1.parquet"
            }),
            json!({
                "writer_epoch": 1,
                "source_segment_id": 2,
                "source_generation": 1,
                "created_at": 2,
                "file_path": "epoch-1-segment-2.parquet"
            }),
            json!({
                "writer_epoch": 1,
                "source_segment_id": 1,
                "source_generation": 0,
                "created_at": 1,
                "file_path": "epoch-1-segment-1.parquet"
            }),
        ];

        files.sort_by_key(persisted_file_order_key);

        assert_eq!(files[0]["file_path"], "epoch-1-segment-1.parquet");
        assert_eq!(files[1]["file_path"], "epoch-1-segment-2.parquet");
        assert_eq!(files[2]["file_path"], "epoch-2-segment-1.parquet");
    }

    #[test]
    fn persisted_file_filter_pruning_preserves_nanosecond_precision() {
        let boundary = 1_700_000_000_000_000_000_i64;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let stream = StreamInfo {
            stream_name: "filtered_ticks".to_string(),
            schema: schema_metadata(&schema),
            schema_hash: canonical_schema_hash(&schema),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(4096),
            sealed_segments: Vec::new(),
            persisted_files: vec![
                json!({
                    "file_path": "/tmp/boundary.parquet",
                    "source_segment_id": 1,
                    "source_generation": 0,
                    "stats": {"dt": {"min": boundary, "max": boundary}}
                }),
                json!({
                    "file_path": "/tmp/after-boundary.parquet",
                    "source_segment_id": 2,
                    "source_generation": 1,
                    "stats": {"dt": {"min": boundary + 1, "max": boundary + 1}}
                }),
            ],
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 1,
            reader_count: 0,
            status: "registered".to_string(),
        };
        let scan_pushdown = GatewayScanPushdown {
            filters: vec![json!({
                "op": "filter",
                "expr": {
                    "kind": "binary",
                    "op": "gt",
                    "args": [
                        {"kind": "col", "value": "dt"},
                        {
                            "kind": "literal",
                            "literal_type": "timestamp_ns",
                            "value": boundary,
                            "unit": "ns"
                        }
                    ]
                }
            })],
            projection_columns: None,
        };

        let files = matching_non_overlapping_persisted_files(&stream, &scan_pushdown);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0]["file_path"], "/tmp/after-boundary.parquet");
    }

    #[test]
    fn persisted_partition_filter_handles_mixed_scalar_encodings_conservatively() {
        let item = json!({"partition": {"bucket": "1"}});
        let equality = json!({
            "kind": "binary",
            "op": "eq",
            "args": [
                {"kind": "col", "value": "bucket"},
                {"kind": "literal", "value": 1}
            ]
        });
        let mismatch = json!({
            "kind": "binary",
            "op": "eq",
            "args": [
                {"kind": "col", "value": "bucket"},
                {"kind": "literal", "value": 2}
            ]
        });

        assert!(persisted_file_may_match_expr(&item, &equality));
        assert!(!persisted_file_may_match_expr(&item, &mismatch));
    }

    #[test]
    fn persisted_scan_fails_closed_for_missing_catalog_file() {
        let temp = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let stream = StreamInfo {
            stream_name: "missing_ticks".to_string(),
            schema: schema_metadata(&schema),
            schema_hash: canonical_schema_hash(&schema),
            data_path: "segment".to_string(),
            descriptor_generation: 0,
            active_segment_descriptor: None,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: Some(4096),
            sealed_segments: Vec::new(),
            persisted_files: vec![json!({
                "file_path": temp.path().join("missing.parquet").to_string_lossy(),
                "persist_data_root": temp.path().to_string_lossy(),
                "source_segment_id": 1,
                "source_generation": 0
            })],
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: None,
            writer_epoch: 1,
            reader_count: 0,
            status: "registered".to_string(),
        };
        let mut scanned_rows = 0;
        let mut scanned_files = Vec::new();
        let mut max_pending_file_results = 0;

        let error = parallel_persisted_file_record_batches(
            &stream,
            &GatewayScanPushdown::default(),
            DEFAULT_MAX_PARALLELISM,
            &mut scanned_rows,
            &mut scanned_files,
            &mut max_pending_file_results,
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("failed to canonicalize persisted file path"));
    }

    #[test]
    fn gateway_snapshot_expiry_removes_only_idle_snapshots() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let stream = stream_info_for_gateway_status_test("snapshot_ticks", schema, 1);
        let now = Instant::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "active".to_string(),
            GatewaySnapshot {
                source: "snapshot_ticks".to_string(),
                stream: Arc::new(stream.clone()),
                active_descriptor: None,
                active_committed_row_high_watermark: None,
                last_accessed_at: now,
                _leases: Vec::new(),
            },
        );
        snapshots.insert(
            "expired".to_string(),
            GatewaySnapshot {
                source: "snapshot_ticks".to_string(),
                stream: Arc::new(stream),
                active_descriptor: None,
                active_committed_row_high_watermark: None,
                last_accessed_at: now - GATEWAY_SNAPSHOT_IDLE_TIMEOUT - Duration::from_secs(1),
                _leases: Vec::new(),
            },
        );

        let expired = expire_gateway_snapshots(&mut snapshots, now);

        assert_eq!(expired, 1);
        assert!(snapshots.contains_key("active"));
        assert!(!snapshots.contains_key("expired"));
    }
}
