use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{Cursor, ErrorKind};
use std::net::{TcpListener as StdTcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
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
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    ProjectionMask,
};
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
    UnregisterSourceRequest, ZippyError,
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
const DEFAULT_GATEWAY_PERSISTED_SCAN_PARALLELISM: usize = 4;
const DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS: usize = 8;
const PERSISTED_PARQUET_SCAN_BATCH_SIZE: usize = 1024;

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
    max_pending_file_results: usize,
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
            "max_pending_file_results": self.max_pending_file_results,
            "materialized_live_batches": self.materialized_live_batches,
            "segment_streamed_batches": self.segment_streamed_batches,
            "segment_streamed_rows": self.segment_streamed_rows,
        })
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
        batches: VecDeque<RecordBatch>,
        chunk_rows: usize,
        current_batch: Option<RecordBatch>,
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
    ) -> Result<Self> {
        let scan_started = Instant::now();
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let scan_schema = schema_for_scan_pushdown(&schema, scan_pushdown)?;
        let mut scanned_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut max_pending_file_results = 0usize;
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
                Some(n),
                parallel_persisted_file_record_batches(
                    stream,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                    &mut max_pending_file_results,
                )?,
            ),
            Some(GatewayRowRangePushdown::Slice { offset, length }) => (
                offset,
                length,
                parallel_persisted_file_record_batches(
                    stream,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                    &mut max_pending_file_results,
                )?,
            ),
            None => (
                0,
                None,
                parallel_persisted_file_record_batches(
                    stream,
                    scan_pushdown,
                    &mut scanned_rows,
                    &mut scanned_files,
                    &mut max_pending_file_results,
                )?,
            ),
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
            max_pending_file_results,
            ..GatewayCollectStreamMetrics::default()
        };
        Ok(Self::Persisted {
            schema: schema_for_projection_columns(&scan_schema, output_projection_columns)?,
            batches: batches.into(),
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
                batches,
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
                    .is_none_or(|batch| *current_offset >= batch.num_rows())
                {
                    *current_batch = batches.pop_front();
                    *current_offset = 0;
                }
                let Some(batch) = current_batch.as_ref() else {
                    return Ok(None);
                };
                if batch.num_rows() == 0 {
                    *current_batch = None;
                    continue;
                }
                let available_rows = batch.num_rows() - *current_offset;
                if *skip_rows >= available_rows {
                    *skip_rows -= available_rows;
                    *current_offset = batch.num_rows();
                    continue;
                }
                let start = *current_offset + *skip_rows;
                let available_rows = batch.num_rows() - start;
                *skip_rows = 0;
                let mut rows = (*chunk_rows).min(available_rows);
                if let Some(remaining) = remaining_rows.as_mut() {
                    rows = rows.min(*remaining);
                    *remaining = remaining.saturating_sub(rows);
                }
                if rows == 0 {
                    return Ok(None);
                }
                let chunk = batch.slice(start, rows);
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
    stream: StreamInfo,
    active_descriptor: Option<Value>,
    active_committed_row_high_watermark: Option<usize>,
    _leases: Vec<SegmentReaderLeaseGuard>,
}

#[derive(Clone)]
struct GatewaySnapshotView {
    source: String,
    stream: StreamInfo,
    active_descriptor: Option<Value>,
    active_committed_row_high_watermark: Option<usize>,
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
}

struct MasterDescriptorPublisher {
    master: Arc<GatewayAsyncMasterClient>,
    metrics: Arc<Mutex<GatewayMetrics>>,
    stream_name: String,
}

impl GatewayServer {
    /// Create a native GatewayServer.
    pub fn new(config: GatewayServerConfig) -> Result<Self> {
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
        insert_json_metric(
            fields,
            "write_timeout_ms",
            self.state.write_timeout.as_millis() as u64,
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
            stream: stream.clone(),
            active_descriptor,
            active_committed_row_high_watermark: pinned_active_high_watermark,
            _leases: leases,
        };
        self.snapshots
            .lock()
            .unwrap()
            .insert(snapshot_id.clone(), snapshot);

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
            let mut scanned =
                self.collect_snapshot_source(source, snapshot_id, &requested_scan_pushdown)?;
            if let Some((pushdown, _)) = row_range_pushdown {
                row_range_pushed = true;
                let row_range_op = pushdown.to_plan_op();
                scanned.batch = apply_collect_plan(scanned.batch, &[row_range_op])?;
            }
            scanned
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
        let snapshot = {
            let snapshots = self.snapshots.lock().unwrap();
            let snapshot = snapshots
                .get(snapshot_id)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: format!("remote snapshot not found snapshot_id=[{}]", snapshot_id),
                })?;
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

        let schema = Arc::new(arrow_schema_from_stream_metadata(&snapshot.stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut batches = persisted_file_record_batches(
            &snapshot.stream,
            scan_pushdown,
            &mut scanned_rows,
            &mut scanned_files,
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
        let writer_epoch = self.master.get_stream_blocking(stream_name)?.writer_epoch;

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
        insert_json_metric(
            value.as_object_mut().unwrap(),
            "write_timeout_ms",
            self.write_timeout.as_millis() as u64,
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
        let mut batches = persisted_file_record_batches(
            &stream,
            scan_pushdown,
            &mut scanned_rows,
            &mut scanned_files,
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
            if let GatewayRowRangePushdown::Tail(n) = pushdown {
                return self.collect_stream_source_tail(source, &stream, n, scan_pushdown);
            }
            let mut scanned = self.collect_stream_source(source, scan_pushdown)?;
            let row_range_op = pushdown.to_plan_op();
            scanned.batch = apply_collect_plan(scanned.batch, &[row_range_op])?;
            return Ok(scanned);
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

    let mut chunk_index = 0usize;
    loop {
        let next = match producer.next_batch() {
            Ok(next) => next,
            Err(error) => {
                write_frame_async(
                    stream,
                    &json!({
                        "status": "error",
                        "kind": "collect_error",
                        "reason": error.to_string(),
                    }),
                    &[],
                )
                .await?;
                return Ok(());
            }
        };
        let Some(next_batch) = next else {
            break;
        };
        let rows = next_batch.num_rows();
        let encode_started = Instant::now();
        let payload =
            run_blocking_request(Arc::clone(&state), move || encode_ipc_table(&next_batch)).await?;
        producer.metrics_mut().encode_elapsed_ms += encode_started.elapsed().as_secs_f64() * 1000.0;
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

fn persisted_file_record_batches(
    stream: &StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for persisted_file in non_overlapping_persisted_files(stream) {
        let file_path = persisted_file_path(persisted_file)?;
        let file_batches = read_parquet_record_batches(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
        )?;
        scanned_files.push(file_path);
        for batch in file_batches {
            let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
    }
    Ok(batches)
}

fn parallel_persisted_file_record_batches(
    stream: &StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
    max_pending_file_results: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let files = non_overlapping_persisted_files(stream)
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
    let mut batches = Vec::new();
    let mut ordered =
        OrderedGatewayFileResults::new(DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS);
    let mut next_task_index = 0usize;
    while next_task_index < tasks.len() {
        let end_index =
            (next_task_index + DEFAULT_GATEWAY_PERSISTED_SCAN_PARALLELISM).min(tasks.len());
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
                        reason: "streaming collect persisted scan worker panicked".to_string(),
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

fn read_parquet_record_batches(
    path: &Path,
    projection_columns: Option<&[String]>,
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks_with_tail(path, projection_columns, None)?.1)
}

fn read_parquet_record_batches_tail(
    path: &Path,
    projection_columns: Option<&[String]>,
    n: usize,
) -> Result<Vec<RecordBatch>> {
    Ok(read_parquet_record_batch_chunks_with_tail(path, projection_columns, Some(n))?.1)
}

fn read_parquet_record_batch_chunks_with_tail(
    path: &Path,
    projection_columns: Option<&[String]>,
    tail_rows: Option<usize>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
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
    if let Some(tail_rows) = tail_rows {
        let total_rows =
            usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                ZippyError::InvalidConfig {
                    reason: format!(
                        "persisted parquet row count overflows usize path=[{}]",
                        path.display()
                    ),
                }
            })?;
        let selected_rows = tail_rows.min(total_rows);
        let skipped_rows = total_rows.saturating_sub(selected_rows);
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
    stream
        .persisted_files
        .iter()
        .any(|item| persisted_file_path(item).is_ok())
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
        .filter(|item| persisted_file_path(item).is_ok())
        .filter(|item| persisted_segment_identities(item).is_disjoint(&live_identities))
        .collect::<Vec<_>>();
    files.sort_by_key(|item| persisted_file_order_key(item));
    files
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

fn live_segment_identities(stream: &StreamInfo) -> std::collections::BTreeSet<(u64, u64)> {
    let mut identities = std::collections::BTreeSet::new();
    if let Some(descriptor) = stream.active_segment_descriptor.as_ref() {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.insert(identity);
        }
    } else {
        return identities;
    }
    for descriptor in &stream.sealed_segments {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.insert(identity);
        }
    }
    identities
}

fn persisted_segment_identities(value: &Value) -> std::collections::BTreeSet<(u64, u64)> {
    let mut identities = std::collections::BTreeSet::new();
    if let Some(source_segments) = value.get("source_segments").and_then(Value::as_array) {
        for source_segment in source_segments {
            if let Some(identity) = segment_identity_from_value(
                source_segment,
                "source_segment_id",
                "source_generation",
            ) {
                identities.insert(identity);
            }
        }
    }
    if identities.is_empty() {
        if let Some(identity) =
            segment_identity_from_value(value, "source_segment_id", "source_generation")
        {
            identities.insert(identity);
        }
    }
    identities
}

fn segment_identity_from_value(
    value: &Value,
    segment_id_key: &str,
    generation_key: &str,
) -> Option<(u64, u64)> {
    Some((
        value.get(segment_id_key)?.as_u64()?,
        value.get(generation_key)?.as_u64()?,
    ))
}

fn persisted_file_order_key(value: &Value) -> (u64, u64, u64, String) {
    (
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
    value.and_then(Value::as_u64).unwrap_or(u64::MAX)
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
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
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
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
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
        leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
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
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
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
        leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
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

        let batches = read_parquet_record_batches(&path, None).unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            row_count
        );
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
}
