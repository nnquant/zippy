#![allow(clippy::useless_conversion)]

mod native_source_bridge;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::hint::spin_loop;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver as StdReceiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule, PyTuple};
use tracing::{error, info};
use zippy_core::{
    current_log_snapshot, python_dev_version, resolve_control_endpoint, send_control_line_request,
    setup_log as setup_core_log, spawn_engine_with_publisher, ControlEndpoint, ControlRequest,
    DropTableResult, Engine, EngineConfig, EngineHandle, EngineMetricsSnapshot, EngineStatus,
    LateDataPolicy, LogConfig, MasterClient as CoreMasterClient, OverflowPolicy,
    Publisher as CorePublisher, Reader as CoreBusReader, SegmentTableView, Source, SourceEvent,
    SourceHandle, SourceMode as RustSourceMode, SourceSink, StreamHello, StreamInfo,
    Writer as CoreBusWriter, ZippyError, DEFAULT_CONTROL_ENDPOINT_URI,
};
use zippy_engines::{
    CrossSectionalEngine as RustCrossSectionalEngine,
    KeyValueTableMaterializer as RustKeyValueTableMaterializer,
    ReactiveLatestEngine as RustReactiveLatestEngine,
    ReactiveStateEngine as RustReactiveStateEngine,
    StreamTableDescriptorPublisher as RustStreamTableDescriptorPublisher,
    StreamTableMaterializer as RustStreamTableMaterializer, StreamTablePersistConfig,
    StreamTablePersistPartitionSpec,
    StreamTablePersistPublisher as RustStreamTablePersistPublisher,
    StreamTableRetentionGuard as RustStreamTableRetentionGuard,
    TimeSeriesEngine as RustTimeSeriesEngine,
};
use zippy_io::{
    FanoutPublisher as RustFanoutPublisher, NullPublisher as RustNullPublisher,
    ParquetSink as RustParquetSink, ParquetSinkWriter as RustParquetSinkWriter,
    ZmqPublisher as RustZmqPublisher, ZmqSource as RustZmqSource,
    ZmqStreamPublisher as RustZmqStreamPublisher, ZmqSubscriber as RustZmqSubscriber,
};
use zippy_master::daemon::{run_master_daemon as run_rust_master_daemon, MasterDaemonConfig};
use zippy_master::server::MasterServer as RustMasterServer;
use zippy_operators::{
    AbsSpec as RustAbsSpec, AggCountSpec as RustAggCountSpec, AggFirstSpec as RustAggFirstSpec,
    AggLastSpec as RustAggLastSpec, AggMaxSpec as RustAggMaxSpec, AggMinSpec as RustAggMinSpec,
    AggSumSpec as RustAggSumSpec, AggVwapSpec as RustAggVwapSpec,
    AggregationSpec as RustAggregationSpec, CSDemeanSpec as RustCSDemeanSpec,
    CSRankSpec as RustCSRankSpec, CSZscoreSpec as RustCSZscoreSpec, CastSpec as RustCastSpec,
    ClipSpec as RustClipSpec, ExpressionSpec as RustExpressionSpec, LogSpec as RustLogSpec,
    TsDelaySpec as RustTsDelaySpec, TsDiffSpec as RustTsDiffSpec, TsEmaSpec as RustTsEmaSpec,
    TsMeanSpec as RustTsMeanSpec, TsReturnSpec as RustTsReturnSpec, TsStdSpec as RustTsStdSpec,
};
use zippy_segment_store::{
    compile_schema as compile_segment_schema, ActiveSegmentDescriptor, ActiveSegmentReader,
    ColumnSpec, ColumnType, CompiledSchema, LayoutPlan, PartitionHandle, RowSpanView,
    SegmentCellValue, SegmentControlSnapshot, SegmentStore, SegmentStoreConfig,
    ZippySegmentStoreError,
};

use native_source_bridge::create_native_source_sink_capsule;

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn resolve_control_endpoint_value(control_endpoint: &str) -> PyResult<ControlEndpoint> {
    resolve_control_endpoint(control_endpoint).map_err(|error| py_runtime_error(error.to_string()))
}

fn resolve_uri_argument(uri: Option<String>, control_endpoint: Option<String>) -> PyResult<String> {
    match (uri, control_endpoint) {
        (Some(_), Some(_)) => Err(py_value_error(
            "only one of uri and control_endpoint may be provided",
        )),
        (Some(value), None) | (None, Some(value)) => Ok(value),
        (None, None) => Ok(DEFAULT_CONTROL_ENDPOINT_URI.to_string()),
    }
}

fn prepare_control_endpoint(control_endpoint: &str) -> PyResult<ControlEndpoint> {
    let endpoint = resolve_control_endpoint_value(control_endpoint)?;
    if let Some(path) = endpoint.unix_path() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                py_runtime_error(format!(
                    "failed to create control endpoint parent path=[{}] error=[{}]",
                    parent.display(),
                    error
                ))
            })?;
        }
    }
    Ok(endpoint)
}

fn parse_startup_timeout(startup_timeout_sec: f64) -> PyResult<Duration> {
    if !startup_timeout_sec.is_finite() || startup_timeout_sec <= 0.0 {
        return Err(py_value_error(
            "startup_timeout_sec must be a positive finite number",
        ));
    }
    Ok(Duration::from_secs_f64(startup_timeout_sec))
}

fn python_json_dumps(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json = PyModule::import_bound(py, "json")?;
    json.call_method1("dumps", (value,))?
        .extract::<String>()
        .map_err(|error| py_value_error(error.to_string()))
}

fn python_json_loads<'py>(py: Python<'py>, text: &str) -> PyResult<Bound<'py, PyAny>> {
    let json = PyModule::import_bound(py, "json")?;
    json.call_method1("loads", (text,))
        .map_err(|error| py_value_error(error.to_string()))
}

fn serde_json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    let text = serde_json::to_string(value).map_err(|error| py_value_error(error.to_string()))?;
    Ok(python_json_loads(py, &text)?.into_py(py))
}

fn serde_json_option_to_py(
    py: Python<'_>,
    value: Option<&serde_json::Value>,
) -> PyResult<PyObject> {
    match value {
        Some(value) => serde_json_value_to_py(py, value),
        None => Ok(py.None()),
    }
}

fn send_master_control_request(
    py: Python<'_>,
    control_endpoint: &str,
    request: &Bound<'_, PyAny>,
    expected_response_key: &str,
) -> PyResult<()> {
    let payload = python_json_dumps(py, request)?;
    let endpoint = resolve_control_endpoint_value(control_endpoint)?;
    let request = serde_json::from_str::<ControlRequest>(&payload)
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let response = py
        .allow_threads(|| send_control_line_request(&endpoint, request))
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let response_text =
        serde_json::to_string(&response).map_err(|error| py_runtime_error(error.to_string()))?;
    let response = python_json_loads(py, &response_text)?;
    let response = response
        .downcast::<PyDict>()
        .map_err(|_| py_runtime_error("invalid control response"))?;

    if let Some((key, value)) = response.iter().next() {
        let key = key
            .extract::<String>()
            .map_err(|error| py_value_error(error.to_string()))?;
        if key == "Error" {
            let error = value
                .downcast::<PyDict>()
                .map_err(|_| py_runtime_error("invalid control error response"))?;
            let reason = error
                .get_item("reason")?
                .ok_or_else(|| py_runtime_error("control error response is missing reason"))?
                .extract::<String>()
                .map_err(|error| py_value_error(error.to_string()))?;
            return Err(py_runtime_error(reason));
        }

        if key == expected_response_key {
            return Ok(());
        }
    }

    Err(py_runtime_error(format!(
        "unexpected control response expected_key=[{}]",
        expected_response_key
    )))
}

type SharedHandle = Arc<Mutex<Option<EngineHandle>>>;
type SharedArchive = Arc<Mutex<Option<ArchiveHandle>>>;
type SharedStatus = Arc<Mutex<EngineStatus>>;
type SharedMetrics = Arc<Mutex<EngineMetricsSnapshot>>;
type SharedMasterClient = Arc<Mutex<CoreMasterClient>>;
type SourceOwner = Option<Py<PyAny>>;
type RegisteredSource = (
    SourceOwner,
    Option<RemoteSourceConfig>,
    Option<BusSourceConfig>,
    Option<SegmentSourceConfig>,
    Option<PythonSourceConfig>,
);

#[derive(Clone)]
struct DownstreamLink {
    handle: SharedHandle,
    archive: SharedArchive,
    write_input: bool,
}

#[derive(Clone)]
struct RuntimeOptions {
    buffer_capacity: usize,
    overflow_policy: OverflowPolicy,
    archive_buffer_capacity: usize,
    xfast: bool,
}

#[derive(Clone)]
struct RemoteSourceConfig {
    endpoint: String,
    expected_schema: Arc<Schema>,
    mode: RustSourceMode,
}

#[derive(Clone)]
struct BusSourceConfig {
    stream_name: String,
    expected_schema: Arc<Schema>,
    master: SharedMasterClient,
    mode: RustSourceMode,
    xfast: bool,
}

#[derive(Clone)]
struct SegmentSourceConfig {
    stream_name: String,
    expected_schema: Arc<Schema>,
    segment_schema: CompiledSchema,
    master: SharedMasterClient,
    mode: RustSourceMode,
    start_at_tail: bool,
    xfast: bool,
}

struct PythonSourceConfig {
    owner: Py<PyAny>,
    name: String,
    output_schema: Arc<Schema>,
    mode: RustSourceMode,
}

#[derive(Clone)]
enum TargetConfig {
    Null,
    Zmq {
        endpoint: String,
    },
    BusStream {
        stream_name: String,
        master: SharedMasterClient,
    },
    ZmqStream {
        endpoint: String,
        stream_name: String,
        publisher: Arc<Mutex<Option<RustZmqStreamPublisher>>>,
    },
}

struct InProcessPublisher {
    downstream: DownstreamLink,
}

impl CorePublisher for InProcessPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> zippy_core::Result<()> {
        if self.downstream.write_input {
            let archive = self.downstream.archive.lock().unwrap();
            let archive = archive
                .as_ref()
                .ok_or(zippy_core::ZippyError::InvalidState {
                    status: "parquet sink not started",
                })?;
            archive.write(ArchiveKind::Input, batch.clone())?;
        }

        let guard = self.downstream.handle.lock().unwrap();
        let handle = guard.as_ref().ok_or(zippy_core::ZippyError::InvalidState {
            status: "engine not started",
        })?;
        handle.write(batch.clone())
    }
}

#[pyclass]
struct SourceSinkProxy {
    sink: Arc<dyn SourceSink>,
    schema: Arc<Schema>,
}

#[pymethods]
impl SourceSinkProxy {
    #[pyo3(signature = (stream_name, protocol_version=1))]
    fn emit_hello(
        &self,
        py: Python<'_>,
        stream_name: String,
        protocol_version: u16,
    ) -> PyResult<()> {
        let hello = StreamHello::new(&stream_name, Arc::clone(&self.schema), protocol_version)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        py.allow_threads(|| self.sink.emit(SourceEvent::Hello(hello)))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn emit_data(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = value_to_record_batches(py, value, self.schema.as_ref())?;
        for batch in batches {
            py.allow_threads(|| {
                self.sink
                    .emit(SourceEvent::Data(SegmentTableView::from_record_batch(
                        batch,
                    )))
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        }
        Ok(())
    }

    fn emit_flush(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.sink.emit(SourceEvent::Flush))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn emit_stop(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.sink.emit(SourceEvent::Stop))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn emit_error(&self, py: Python<'_>, reason: String) -> PyResult<()> {
        py.allow_threads(|| self.sink.emit(SourceEvent::Error(reason)))
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

struct PythonSourceBridge {
    owner: Py<PyAny>,
    name: String,
    output_schema: Arc<Schema>,
    mode: RustSourceMode,
}

struct PyStreamTableDescriptorPublisher {
    callback: Py<PyAny>,
}

impl RustStreamTableDescriptorPublisher for PyStreamTableDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> zippy_core::Result<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let callback = self.callback.bind(py);
            let payload = PyBytes::new_bound(py, &descriptor_envelope);
            callback.call1((payload,))?;
            Ok(())
        })
        .map_err(|error| ZippyError::Io {
            reason: format!("stream table descriptor publisher failed error=[{}]", error),
        })
    }
}

struct PyStreamTablePersistPublisher {
    callback: Py<PyAny>,
}

struct PyStreamTableRetentionGuard {
    callback: Py<PyAny>,
}

impl RustStreamTablePersistPublisher for PyStreamTablePersistPublisher {
    fn publish(&self, persisted_file: serde_json::Value) -> zippy_core::Result<()> {
        let payload = serde_json::to_vec(&persisted_file).map_err(|error| ZippyError::Io {
            reason: format!(
                "stream table persist metadata encode failed error=[{}]",
                error
            ),
        })?;
        Python::with_gil(|py| -> PyResult<()> {
            let callback = self.callback.bind(py);
            let payload = PyBytes::new_bound(py, &payload);
            callback.call1((payload,))?;
            Ok(())
        })
        .map_err(|error| ZippyError::Io {
            reason: format!("stream table persist publisher failed error=[{}]", error),
        })
    }

    fn publish_event(&self, persist_event: serde_json::Value) -> zippy_core::Result<()> {
        let payload = serde_json::to_vec(&persist_event).map_err(|error| ZippyError::Io {
            reason: format!("stream table persist event encode failed error=[{}]", error),
        })?;
        Python::with_gil(|py| -> PyResult<()> {
            let callback = self.callback.bind(py);
            let payload = PyBytes::new_bound(py, &payload);
            callback.call1((payload,))?;
            Ok(())
        })
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "stream table persist event publisher failed error=[{}]",
                error
            ),
        })
    }
}

impl RustStreamTableRetentionGuard for PyStreamTableRetentionGuard {
    fn can_release(&self, segment_id: u64, generation: u64) -> zippy_core::Result<bool> {
        Python::with_gil(|py| -> PyResult<bool> {
            let callback = self.callback.bind(py);
            callback.call1((segment_id, generation))?.extract()
        })
        .map_err(|error| ZippyError::Io {
            reason: format!("stream table retention guard failed error=[{}]", error),
        })
    }
}

impl Source for PythonSourceBridge {
    fn name(&self) -> &str {
        &self.name
    }

    fn output_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.output_schema)
    }

    fn mode(&self) -> RustSourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> zippy_core::Result<SourceHandle> {
        let owner = Python::with_gil(|py| self.owner.clone_ref(py));
        let schema = Arc::clone(&self.output_schema);
        let segment_schema =
            compile_segment_schema_from_arrow(schema.as_ref()).map_err(map_python_source_error)?;
        let runtime_handle = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let owner_bound = owner.bind(py);
            if owner_bound.hasattr("_zippy_start_native")? {
                let capsule = create_native_source_sink_capsule(
                    py,
                    Arc::clone(&sink),
                    Arc::clone(&schema),
                    segment_schema,
                )?;
                owner_bound
                    .call_method1("_zippy_start_native", (capsule,))
                    .map(|value| value.unbind())
            } else {
                let sink_proxy = Py::new(py, SourceSinkProxy { sink, schema })?;
                owner_bound
                    .call_method1("_zippy_start", (sink_proxy,))
                    .map(|value| value.unbind())
            }
        })
        .map_err(map_python_source_error)?;

        let join_handle_object = Python::with_gil(|py| runtime_handle.clone_ref(py));
        let join_handle = thread::spawn(move || -> zippy_core::Result<()> {
            Python::with_gil(|py| join_handle_object.bind(py).call_method0("join").map(|_| ()))
                .map_err(map_python_source_error)
        });

        let stop_handle_object = Python::with_gil(|py| runtime_handle.clone_ref(py));
        let stop_fn: Box<dyn FnMut() -> zippy_core::Result<()> + Send> = Box::new(move || {
            Python::with_gil(|py| stop_handle_object.bind(py).call_method0("stop").map(|_| ()))
                .map_err(map_python_source_error)
        });

        Ok(SourceHandle::new_with_stop(join_handle, stop_fn))
    }
}

#[derive(Clone)]
enum ParquetRotation {
    None,
    Hourly,
}

impl ParquetRotation {
    fn parse(value: &str) -> PyResult<Self> {
        match value {
            "none" => Ok(Self::None),
            "1h" => Ok(Self::Hourly),
            _ => Err(py_value_error("rotation must be 'none' or '1h'")),
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Hourly => "1h",
        }
    }
}

#[derive(Clone)]
struct ParquetSinkConfig {
    path: PathBuf,
    rotation: ParquetRotation,
    write_input: bool,
    write_output: bool,
    rows_per_batch: usize,
    flush_interval_ms: u64,
}

enum ArchiveKind {
    Input,
    Output,
}

impl ArchiveKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

enum ArchiveCommand {
    Write {
        kind: ArchiveKind,
        batch: RecordBatch,
    },
    Flush(mpsc::Sender<zippy_core::Result<()>>),
    Close(mpsc::Sender<zippy_core::Result<()>>),
}

#[derive(Clone)]
struct ArchiveHandle {
    tx: SyncSender<ArchiveCommand>,
    join_handle: Arc<Mutex<Option<JoinHandle<zippy_core::Result<()>>>>>,
    closed: Arc<AtomicBool>,
}

impl ArchiveHandle {
    fn spawn(config: ParquetSinkConfig, buffer_capacity: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(buffer_capacity);
        let join_handle = thread::spawn(move || parquet_archive_worker(config, rx));
        Self {
            tx,
            join_handle: Arc::new(Mutex::new(Some(join_handle))),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    fn write(&self, kind: ArchiveKind, batch: RecordBatch) -> zippy_core::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ZippyError::InvalidState {
                status: "parquet sink closed",
            });
        }

        self.tx
            .send(ArchiveCommand::Write { kind, batch })
            .map_err(|_| ZippyError::ChannelSend)
    }

    fn flush(&self) -> zippy_core::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        let (reply_tx, reply_rx) = mpsc::channel();
        self.tx
            .send(ArchiveCommand::Flush(reply_tx))
            .map_err(|_| ZippyError::ChannelSend)?;
        reply_rx.recv().map_err(|_| ZippyError::ChannelReceive)?
    }

    fn close(&self) -> zippy_core::Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let (reply_tx, reply_rx) = mpsc::channel();
        let send_result = self
            .tx
            .send(ArchiveCommand::Close(reply_tx))
            .map_err(|_| ZippyError::ChannelSend);
        let close_result =
            send_result.and_then(|_| reply_rx.recv().map_err(|_| ZippyError::ChannelReceive)?);
        let join_result = self
            .join_handle
            .lock()
            .unwrap()
            .take()
            .map(|join_handle| {
                join_handle.join().map_err(|_| ZippyError::Io {
                    reason: "parquet sink worker panicked".to_string(),
                })?
            })
            .unwrap_or(Ok(()));

        match (close_result, join_result) {
            (_, Err(error)) => Err(error),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

struct ParquetOutputPublisher {
    archive: ArchiveHandle,
}

impl CorePublisher for ParquetOutputPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> zippy_core::Result<()> {
        self.archive.write(ArchiveKind::Output, batch.clone())
    }

    fn flush(&mut self) -> zippy_core::Result<()> {
        self.archive.flush()
    }

    fn close(&mut self) -> zippy_core::Result<()> {
        self.archive.close()
    }
}

fn parquet_archive_worker(
    config: ParquetSinkConfig,
    rx: StdReceiver<ArchiveCommand>,
) -> zippy_core::Result<()> {
    let mut input_state = ArchiveFileState::new(config.rows_per_batch);
    let mut output_state = ArchiveFileState::new(config.rows_per_batch);

    loop {
        let command = if config.flush_interval_ms == 0 {
            match rx.recv() {
                Ok(command) => command,
                Err(_) => break,
            }
        } else {
            match rx.recv_timeout(Duration::from_millis(config.flush_interval_ms)) {
                Ok(command) => command,
                Err(RecvTimeoutError::Timeout) => {
                    input_state.flush_if_due(config.flush_interval_ms)?;
                    output_state.flush_if_due(config.flush_interval_ms)?;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        };

        match command {
            ArchiveCommand::Write { kind, batch } => match kind {
                ArchiveKind::Input => {
                    input_state.write(&config, &kind, batch)?;
                }
                ArchiveKind::Output => {
                    output_state.write(&config, &kind, batch)?;
                }
            },
            ArchiveCommand::Flush(reply_tx) => {
                let result = input_state.flush().and_then(|_| output_state.flush());
                let _ = reply_tx.send(result);
            }
            ArchiveCommand::Close(reply_tx) => {
                let result = input_state.close().and_then(|_| output_state.close());
                let _ = reply_tx.send(result);
                return Ok(());
            }
        }
    }

    input_state.close().and_then(|_| output_state.close())
}

struct ArchiveFileState {
    next_index: u64,
    active_root: Option<PathBuf>,
    active_schema: Option<Arc<Schema>>,
    writer: Option<RustParquetSinkWriter>,
    buffered_batches: Vec<RecordBatch>,
    buffered_rows: usize,
    rows_per_batch: usize,
    first_buffered_at: Option<Instant>,
}

impl ArchiveFileState {
    fn new(rows_per_batch: usize) -> Self {
        Self {
            next_index: 0,
            active_root: None,
            active_schema: None,
            writer: None,
            buffered_batches: Vec::new(),
            buffered_rows: 0,
            rows_per_batch: rows_per_batch.max(1),
            first_buffered_at: None,
        }
    }

    fn write(
        &mut self,
        config: &ParquetSinkConfig,
        kind: &ArchiveKind,
        batch: RecordBatch,
    ) -> zippy_core::Result<()> {
        let root = archive_root(config, kind)?;
        let needs_rotate = self
            .active_root
            .as_ref()
            .map(|active_root| active_root != &root)
            .unwrap_or(false);
        let needs_schema_roll = self
            .active_schema
            .as_ref()
            .map(|schema| schema.as_ref() != batch.schema().as_ref())
            .unwrap_or(false);

        if needs_rotate || needs_schema_roll {
            self.flush()?;
        }

        if self.active_root.is_none() {
            self.active_root = Some(root);
        }
        if self.active_schema.is_none() {
            self.active_schema = Some(batch.schema());
        }
        if self.first_buffered_at.is_none() {
            self.first_buffered_at = Some(Instant::now());
        }

        self.buffered_rows += batch.num_rows();
        self.buffered_batches.push(batch);

        if self.buffered_rows >= self.rows_per_batch {
            self.flush_buffer()?;
        }

        Ok(())
    }

    fn flush_if_due(&mut self, flush_interval_ms: u64) -> zippy_core::Result<()> {
        if flush_interval_ms == 0 || self.buffered_batches.is_empty() {
            return Ok(());
        }

        let Some(first_buffered_at) = self.first_buffered_at else {
            return Ok(());
        };
        if first_buffered_at.elapsed() >= Duration::from_millis(flush_interval_ms) {
            self.flush_buffer()?;
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> zippy_core::Result<()> {
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        let root = self.active_root.clone().ok_or(ZippyError::InvalidState {
            status: "parquet archive root is not available",
        })?;
        let schema = self.active_schema.clone().ok_or(ZippyError::InvalidState {
            status: "parquet archive schema is not available",
        })?;

        if self.writer.is_none() {
            self.next_index += 1;
            let sink = RustParquetSink::new(root);
            self.writer = Some(
                sink.create_writer(&format!("{:06}.parquet", self.next_index), schema.clone())?,
            );
        }

        let merged = concat_batches(&schema, self.buffered_batches.iter()).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to concat archive batches error=[{}]", error),
            }
        })?;
        self.writer
            .as_mut()
            .expect("archive writer must exist before flush")
            .write_batch(&merged)?;
        self.buffered_batches.clear();
        self.buffered_rows = 0;
        self.first_buffered_at = None;
        Ok(())
    }

    fn flush(&mut self) -> zippy_core::Result<()> {
        self.flush_buffer()?;
        if let Some(writer) = self.writer.as_mut() {
            writer.close()?;
        }
        self.writer = None;
        self.active_root = None;
        self.active_schema = None;
        Ok(())
    }

    fn close(&mut self) -> zippy_core::Result<()> {
        self.flush()
    }
}

fn archive_root(config: &ParquetSinkConfig, kind: &ArchiveKind) -> zippy_core::Result<PathBuf> {
    let mut root = config.path.join(kind.as_str());
    if let ParquetRotation::Hourly = config.rotation {
        root = root.join(format!("hour_{}", current_epoch_hour()?));
    }
    Ok(root)
}

fn current_epoch_hour() -> zippy_core::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to compute archive hour error=[{}]", error),
        })?;
    Ok(duration.as_secs() / 3_600)
}

fn current_localtime_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after unix epoch")
        .as_nanos() as i64
}

fn compile_segment_schema_from_arrow(schema: &Schema) -> PyResult<CompiledSchema> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let name: &'static str = Box::leak(field.name().clone().into_boxed_str());
            let data_type = match field.data_type() {
                DataType::Int64 => ColumnType::Int64,
                DataType::Float64 => ColumnType::Float64,
                DataType::Utf8 => ColumnType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => {
                    let timezone: &'static str = Box::leak(timezone.to_string().into_boxed_str());
                    ColumnType::TimestampNsTz(timezone)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    return Err(py_value_error(
                        "segment stream timestamp columns must include an explicit timezone",
                    ));
                }
                other => {
                    return Err(py_value_error(format!(
                        "unsupported segment stream field type name=[{}] data_type=[{}]",
                        field.name(),
                        other
                    )));
                }
            };
            Ok(if field.is_nullable() {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<PyResult<Vec<_>>>()?;

    compile_segment_schema(&columns).map_err(py_value_error)
}

fn compile_segment_schema_from_stream_metadata(
    schema: &serde_json::Value,
) -> PyResult<CompiledSchema> {
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| py_value_error("stream schema metadata missing fields"))?;
    let columns = fields
        .iter()
        .map(|field| {
            let name = field
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| py_value_error("stream schema field missing name"))?;
            let data_type = field
                .get("segment_type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| py_value_error("stream schema field missing segment_type"))?;
            let nullable = field
                .get("nullable")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| py_value_error("stream schema field missing nullable"))?;
            let name: &'static str = Box::leak(name.to_string().into_boxed_str());
            let timezone = field.get("timezone").and_then(serde_json::Value::as_str);
            let data_type = parse_segment_schema_metadata_data_type(data_type, timezone)?;
            Ok(if nullable {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<PyResult<Vec<_>>>()?;

    compile_segment_schema(&columns).map_err(py_value_error)
}

fn arrow_schema_from_stream_metadata(schema: &serde_json::Value) -> PyResult<Schema> {
    let fields = schema
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| py_value_error("stream schema metadata missing fields"))?;
    let fields = fields
        .iter()
        .map(|field| {
            let name = field
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| py_value_error("stream schema field missing name"))?;
            let segment_type = field
                .get("segment_type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| py_value_error("stream schema field missing segment_type"))?;
            let nullable = field
                .get("nullable")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| py_value_error("stream schema field missing nullable"))?;
            let timezone = field.get("timezone").and_then(serde_json::Value::as_str);
            let data_type = parse_arrow_schema_metadata_data_type(segment_type, timezone)?;
            let metadata = string_map_from_json_value(field.get("metadata"))?;
            Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let metadata = string_map_from_json_value(schema.get("metadata"))?;
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn parse_segment_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> PyResult<ColumnType> {
    if segment_type == "int64" {
        return Ok(ColumnType::Int64);
    }
    if segment_type == "float64" {
        return Ok(ColumnType::Float64);
    }
    if segment_type == "utf8" {
        return Ok(ColumnType::Utf8);
    }
    if segment_type == "timestamp_ns_tz" {
        let timezone = timezone.ok_or_else(|| {
            py_value_error("timestamp_ns_tz stream schema field missing timezone")
        })?;
        let timezone: &'static str = Box::leak(timezone.to_string().into_boxed_str());
        return Ok(ColumnType::TimestampNsTz(timezone));
    }
    if segment_type == "timestamp_ns" {
        return Err(py_value_error(
            "segment stream timestamp columns must include an explicit timezone",
        ));
    }

    Err(py_value_error(format!(
        "unsupported segment stream field type segment_type=[{}]",
        segment_type
    )))
}

fn parse_arrow_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> PyResult<DataType> {
    if segment_type == "int64" {
        return Ok(DataType::Int64);
    }
    if segment_type == "float64" {
        return Ok(DataType::Float64);
    }
    if segment_type == "utf8" {
        return Ok(DataType::Utf8);
    }
    if segment_type == "timestamp_ns_tz" {
        let timezone = timezone.ok_or_else(|| {
            py_value_error("timestamp_ns_tz stream schema field missing timezone")
        })?;
        return Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some(timezone.into()),
        ));
    }
    if segment_type == "timestamp_ns" {
        return Ok(DataType::Timestamp(TimeUnit::Nanosecond, None));
    }

    Err(py_value_error(format!(
        "unsupported stream field type segment_type=[{}]",
        segment_type
    )))
}

fn string_map_from_json_value(
    value: Option<&serde_json::Value>,
) -> PyResult<HashMap<String, String>> {
    let Some(value) = value else {
        return Ok(HashMap::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| py_value_error("stream schema metadata must be an object"))?;
    object
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .ok_or_else(|| py_value_error("stream schema metadata values must be strings"))?;
            Ok((key.clone(), value.to_string()))
        })
        .collect()
}

#[pyclass]
struct TsEmaSpec {
    _id_column: String,
    value_column: String,
    span: usize,
    output: String,
}

#[pymethods]
impl TsEmaSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, span, output))]
    fn new(id_column: String, value_column: String, span: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            span,
            output,
        }
    }
}

#[pyclass]
struct TsReturnSpec {
    _id_column: String,
    value_column: String,
    period: usize,
    output: String,
}

#[pymethods]
impl TsReturnSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, period, output))]
    fn new(id_column: String, value_column: String, period: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct TsMeanSpec {
    _id_column: String,
    value_column: String,
    window: usize,
    output: String,
}

#[pymethods]
impl TsMeanSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, window, output))]
    fn new(id_column: String, value_column: String, window: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            window,
            output,
        }
    }
}

#[pyclass]
struct TsStdSpec {
    _id_column: String,
    value_column: String,
    window: usize,
    output: String,
}

#[pymethods]
impl TsStdSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, window, output))]
    fn new(id_column: String, value_column: String, window: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            window,
            output,
        }
    }
}

#[pyclass]
struct TsDelaySpec {
    _id_column: String,
    value_column: String,
    period: usize,
    output: String,
}

#[pymethods]
impl TsDelaySpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, period, output))]
    fn new(id_column: String, value_column: String, period: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct TsDiffSpec {
    _id_column: String,
    value_column: String,
    period: usize,
    output: String,
}

#[pymethods]
impl TsDiffSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, period, output))]
    fn new(id_column: String, value_column: String, period: usize, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct AbsSpec {
    _id_column: String,
    value_column: String,
    output: String,
}

#[pymethods]
impl AbsSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, output))]
    fn new(id_column: String, value_column: String, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            output,
        }
    }
}

#[pyclass]
struct LogSpec {
    _id_column: String,
    value_column: String,
    output: String,
}

#[pymethods]
impl LogSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, output))]
    fn new(id_column: String, value_column: String, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            output,
        }
    }
}

#[pyclass]
struct ClipSpec {
    _id_column: String,
    value_column: String,
    min: f64,
    max: f64,
    output: String,
}

#[pymethods]
impl ClipSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, min, max, output))]
    fn new(id_column: String, value_column: String, min: f64, max: f64, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            min,
            max,
            output,
        }
    }
}

#[pyclass]
struct CastSpec {
    _id_column: String,
    value_column: String,
    dtype: String,
    output: String,
}

#[pymethods]
impl CastSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, dtype, output))]
    fn new(id_column: String, value_column: String, dtype: String, output: String) -> Self {
        Self {
            _id_column: id_column,
            value_column,
            dtype,
            output,
        }
    }
}

#[pyclass]
struct ExpressionFactor {
    #[pyo3(get)]
    expression: String,
    #[pyo3(get)]
    output: String,
}

#[pymethods]
impl ExpressionFactor {
    #[new]
    #[pyo3(signature = (expression, output))]
    fn new(expression: String, output: String) -> Self {
        Self { expression, output }
    }
}

#[pyclass]
struct AggFirstSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggFirstSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggLastSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggLastSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggSumSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggSumSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggMaxSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggMaxSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggMinSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggMinSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggCountSpec {
    column: String,
    output: String,
}

#[pymethods]
impl AggCountSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct AggVwapSpec {
    price_column: String,
    volume_column: String,
    output: String,
}

#[pymethods]
impl AggVwapSpec {
    #[new]
    #[pyo3(signature = (price_column, volume_column, output))]
    fn new(price_column: String, volume_column: String, output: String) -> Self {
        Self {
            price_column,
            volume_column,
            output,
        }
    }
}

#[pyclass]
struct CSRankSpec {
    column: String,
    output: String,
}

#[pymethods]
impl CSRankSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct CSZscoreSpec {
    column: String,
    output: String,
}

#[pymethods]
impl CSZscoreSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
struct CSDemeanSpec {
    column: String,
    output: String,
}

#[pymethods]
impl CSDemeanSpec {
    #[new]
    #[pyo3(signature = (column, output))]
    fn new(column: String, output: String) -> Self {
        Self { column, output }
    }
}

#[pyclass]
#[derive(Default)]
struct NullPublisher;

#[pymethods]
impl NullPublisher {
    #[new]
    fn new() -> Self {
        Self
    }
}

#[pyclass]
struct ParquetSink {
    path: String,
    rotation: String,
    write_input: bool,
    write_output: bool,
    rows_per_batch: usize,
    flush_interval_ms: u64,
}

#[pymethods]
impl ParquetSink {
    #[new]
    #[pyo3(signature = (path, rotation="none", write_input=false, write_output=true, rows_per_batch=8192, flush_interval_ms=1000))]
    fn new(
        path: String,
        rotation: &str,
        write_input: bool,
        write_output: bool,
        rows_per_batch: usize,
        flush_interval_ms: u64,
    ) -> PyResult<Self> {
        ParquetRotation::parse(rotation)?;
        if !write_input && !write_output {
            return Err(py_value_error(
                "parquet_sink must enable write_input or write_output",
            ));
        }
        if rows_per_batch == 0 {
            return Err(py_value_error(
                "parquet_sink rows_per_batch must be greater than zero",
            ));
        }

        Ok(Self {
            path,
            rotation: rotation.to_string(),
            write_input,
            write_output,
            rows_per_batch,
            flush_interval_ms,
        })
    }
}

#[pyclass]
struct ZmqPublisher {
    endpoint: String,
}

#[pymethods]
impl ZmqPublisher {
    #[new]
    #[pyo3(signature = (endpoint))]
    fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
}

#[pyclass]
struct ZmqStreamPublisher {
    stream_name: String,
    last_endpoint: String,
    schema: Arc<Schema>,
    publisher: Arc<Mutex<Option<RustZmqStreamPublisher>>>,
}

#[pymethods]
impl ZmqStreamPublisher {
    #[new]
    #[pyo3(signature = (endpoint, stream_name, schema))]
    fn new(endpoint: String, stream_name: String, schema: &Bound<'_, PyAny>) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let publisher = RustZmqStreamPublisher::bind(&endpoint, &stream_name, schema.clone())
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let last_endpoint = publisher
            .last_endpoint()
            .map_err(|error| py_runtime_error(error.to_string()))?;

        Ok(Self {
            stream_name,
            last_endpoint,
            schema,
            publisher: Arc::new(Mutex::new(Some(publisher))),
        })
    }

    fn last_endpoint(&self) -> PyResult<String> {
        Ok(self.last_endpoint.clone())
    }

    fn publish(&mut self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = value_to_record_batches(py, value, self.schema.as_ref())?;
        let mut publisher = self.publisher.lock().unwrap();
        let publisher = publisher
            .as_mut()
            .ok_or_else(|| py_runtime_error("stream publisher is closed"))?;

        for batch in batches {
            publisher
                .publish_data(&batch)
                .map_err(|error| py_runtime_error(error.to_string()))?;
        }

        Ok(())
    }

    fn publish_hello(&mut self) -> PyResult<()> {
        self.publisher
            .lock()
            .unwrap()
            .as_mut()
            .ok_or_else(|| py_runtime_error("stream publisher is closed"))?
            .publish_hello()
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn flush(&mut self) -> PyResult<()> {
        self.publisher
            .lock()
            .unwrap()
            .as_mut()
            .ok_or_else(|| py_runtime_error("stream publisher is closed"))?
            .publish_flush()
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn stop(&mut self) -> PyResult<()> {
        let mut publisher = self
            .publisher
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| py_runtime_error("stream publisher is closed"))?;
        publisher
            .publish_stop()
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pyclass]
struct ZmqSubscriber {
    subscriber: Option<RustZmqSubscriber>,
}

#[pymethods]
impl ZmqSubscriber {
    #[new]
    #[pyo3(signature = (endpoint, timeout_ms=1000))]
    fn new(endpoint: String, timeout_ms: i32) -> PyResult<Self> {
        let subscriber = RustZmqSubscriber::connect(&endpoint, timeout_ms)
            .map_err(|error| py_runtime_error(error.to_string()))?;

        Ok(Self {
            subscriber: Some(subscriber),
        })
    }

    fn recv(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let subscriber = self
            .subscriber
            .as_mut()
            .ok_or_else(|| py_runtime_error("subscriber is closed"))?;
        let batch = subscriber
            .recv()
            .map_err(|error| py_runtime_error(error.to_string()))?;

        batch
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn close(&mut self) {
        self.subscriber = None;
    }
}

#[pyclass]
struct ZmqSource {
    endpoint: String,
    expected_schema: Arc<Schema>,
    mode: RustSourceMode,
}

#[pymethods]
impl ZmqSource {
    #[new]
    #[pyo3(signature = (endpoint, expected_schema, mode))]
    fn new(
        endpoint: String,
        expected_schema: &Bound<'_, PyAny>,
        mode: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let expected_schema = Arc::new(
            Schema::from_pyarrow_bound(expected_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let mode = parse_source_mode(mode)?;

        Ok(Self {
            endpoint,
            expected_schema,
            mode,
        })
    }
}

#[pyclass]
struct MasterClient {
    control_endpoint: String,
    client: SharedMasterClient,
    schemas: Arc<Mutex<BTreeMap<String, Arc<Schema>>>>,
}

#[pymethods]
impl MasterClient {
    #[new]
    #[pyo3(signature = (uri=None, *, control_endpoint=None))]
    fn new(uri: Option<String>, control_endpoint: Option<String>) -> PyResult<Self> {
        let control_endpoint = resolve_uri_argument(uri, control_endpoint)?;
        let resolved_control_endpoint = resolve_control_endpoint_value(&control_endpoint)?;
        let resolved_control_endpoint_display = resolved_control_endpoint.display_string();
        let client = CoreMasterClient::connect_endpoint(resolved_control_endpoint)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(Self {
            control_endpoint: resolved_control_endpoint_display,
            client: Arc::new(Mutex::new(client)),
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn require_process_id(&self) -> PyResult<String> {
        self.client
            .lock()
            .unwrap()
            .process_id()
            .map(ToOwned::to_owned)
            .ok_or_else(|| py_runtime_error("master client process not registered"))
    }

    fn register_process(&self, py: Python<'_>, app: String) -> PyResult<String> {
        py.allow_threads(|| self.client.lock().unwrap().register_process(&app))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn heartbeat(&self, py: Python<'_>) -> PyResult<()> {
        py.allow_threads(|| self.client.lock().unwrap().heartbeat())
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    #[pyo3(signature = (stream_name, schema, buffer_size, frame_size))]
    fn register_stream(
        &self,
        py: Python<'_>,
        stream_name: String,
        schema: &Bound<'_, PyAny>,
        buffer_size: usize,
        frame_size: usize,
    ) -> PyResult<()> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        py.allow_threads(|| {
            self.client.lock().unwrap().register_stream(
                &stream_name,
                Arc::clone(&schema),
                buffer_size,
                frame_size,
            )
        })
        .map_err(|error| py_runtime_error(error.to_string()))?;
        self.schemas.lock().unwrap().insert(stream_name, schema);
        Ok(())
    }

    #[pyo3(signature = (source_name, source_type, output_stream, config))]
    fn register_source(
        &self,
        py: Python<'_>,
        source_name: String,
        source_type: String,
        output_stream: String,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let process_id = self.require_process_id()?;
        let request = PyDict::new_bound(py);
        let payload = PyDict::new_bound(py);
        payload.set_item("source_name", source_name)?;
        payload.set_item("source_type", source_type)?;
        payload.set_item("process_id", process_id)?;
        payload.set_item("output_stream", output_stream)?;
        payload.set_item("config", config)?;
        request.set_item("RegisterSource", payload)?;
        send_master_control_request(
            py,
            &self.control_endpoint,
            request.as_any(),
            "SourceRegistered",
        )
    }

    fn unregister_source(&self, py: Python<'_>, source_name: String) -> PyResult<()> {
        py.allow_threads(|| self.client.lock().unwrap().unregister_source(&source_name))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    #[pyo3(signature = (engine_name, engine_type, input_stream, output_stream, sink_names, config))]
    #[allow(clippy::too_many_arguments)]
    fn register_engine(
        &self,
        py: Python<'_>,
        engine_name: String,
        engine_type: String,
        input_stream: String,
        output_stream: String,
        sink_names: Vec<String>,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let process_id = self.require_process_id()?;
        let request = PyDict::new_bound(py);
        let payload = PyDict::new_bound(py);
        let sink_name_values = PyList::empty_bound(py);
        for sink_name in sink_names {
            sink_name_values.append(sink_name)?;
        }
        payload.set_item("engine_name", engine_name)?;
        payload.set_item("engine_type", engine_type)?;
        payload.set_item("process_id", process_id)?;
        payload.set_item("input_stream", input_stream)?;
        payload.set_item("output_stream", output_stream)?;
        payload.set_item("sink_names", sink_name_values)?;
        payload.set_item("config", config)?;
        request.set_item("RegisterEngine", payload)?;
        send_master_control_request(
            py,
            &self.control_endpoint,
            request.as_any(),
            "EngineRegistered",
        )
    }

    #[pyo3(signature = (sink_name, sink_type, input_stream, config))]
    fn register_sink(
        &self,
        py: Python<'_>,
        sink_name: String,
        sink_type: String,
        input_stream: String,
        config: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let process_id = self.require_process_id()?;
        let request = PyDict::new_bound(py);
        let payload = PyDict::new_bound(py);
        payload.set_item("sink_name", sink_name)?;
        payload.set_item("sink_type", sink_type)?;
        payload.set_item("process_id", process_id)?;
        payload.set_item("input_stream", input_stream)?;
        payload.set_item("config", config)?;
        request.set_item("RegisterSink", payload)?;
        send_master_control_request(
            py,
            &self.control_endpoint,
            request.as_any(),
            "SinkRegistered",
        )
    }

    #[pyo3(signature = (kind, name, status, metrics=None))]
    fn update_status(
        &self,
        py: Python<'_>,
        kind: String,
        name: String,
        status: String,
        metrics: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let request = PyDict::new_bound(py);
        let payload = PyDict::new_bound(py);
        payload.set_item("kind", kind)?;
        payload.set_item("name", name)?;
        payload.set_item("status", status)?;
        match metrics {
            Some(metrics) => payload.set_item("metrics", metrics)?,
            None => payload.set_item("metrics", py.None())?,
        }
        request.set_item("UpdateStatus", payload)?;
        send_master_control_request(
            py,
            &self.control_endpoint,
            request.as_any(),
            "StatusUpdated",
        )
    }

    fn publish_segment_descriptor(
        &self,
        py: Python<'_>,
        stream_name: String,
        descriptor: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let descriptor_text = python_json_dumps(py, descriptor)?;
        let descriptor = serde_json::from_str::<serde_json::Value>(&descriptor_text)
            .map_err(|error| py_value_error(error.to_string()))?;
        py.allow_threads(|| {
            self.client
                .lock()
                .unwrap()
                .publish_segment_descriptor(&stream_name, descriptor)
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn publish_persisted_file(
        &self,
        py: Python<'_>,
        stream_name: String,
        persisted_file: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let persisted_file_text = python_json_dumps(py, persisted_file)?;
        let persisted_file = serde_json::from_str::<serde_json::Value>(&persisted_file_text)
            .map_err(|error| py_value_error(error.to_string()))?;
        py.allow_threads(|| {
            self.client
                .lock()
                .unwrap()
                .publish_persisted_file(&stream_name, persisted_file)
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn replace_persisted_files(
        &self,
        py: Python<'_>,
        stream_name: String,
        persisted_files: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let persisted_files_text = python_json_dumps(py, persisted_files)?;
        let persisted_files = serde_json::from_str::<Vec<serde_json::Value>>(&persisted_files_text)
            .map_err(|error| py_value_error(error.to_string()))?;
        py.allow_threads(|| {
            self.client
                .lock()
                .unwrap()
                .replace_persisted_files(&stream_name, persisted_files)
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn publish_persist_event(
        &self,
        py: Python<'_>,
        stream_name: String,
        persist_event: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let persist_event_text = python_json_dumps(py, persist_event)?;
        let persist_event = serde_json::from_str::<serde_json::Value>(&persist_event_text)
            .map_err(|error| py_value_error(error.to_string()))?;
        py.allow_threads(|| {
            self.client
                .lock()
                .unwrap()
                .publish_persist_event(&stream_name, persist_event)
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn acquire_segment_reader_lease(
        &self,
        py: Python<'_>,
        stream_name: String,
        source_segment_id: u64,
        source_generation: u64,
    ) -> PyResult<String> {
        py.allow_threads(|| {
            self.client.lock().unwrap().acquire_segment_reader_lease(
                &stream_name,
                source_segment_id,
                source_generation,
            )
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn release_segment_reader_lease(
        &self,
        py: Python<'_>,
        stream_name: String,
        lease_id: String,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            self.client
                .lock()
                .unwrap()
                .release_segment_reader_lease(&stream_name, &lease_id)
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn get_segment_descriptor(&self, py: Python<'_>, stream_name: String) -> PyResult<PyObject> {
        let descriptor = py
            .allow_threads(|| {
                self.client
                    .lock()
                    .unwrap()
                    .get_segment_descriptor(&stream_name)
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let Some(descriptor) = descriptor else {
            return Ok(py.None());
        };
        let descriptor_text = serde_json::to_string(&descriptor)
            .map_err(|error| py_value_error(error.to_string()))?;
        Ok(python_json_loads(py, &descriptor_text)?.into_py(py))
    }

    fn write_to(&self, stream_name: String) -> PyResult<BusWriter> {
        let schema = self
            .schemas
            .lock()
            .unwrap()
            .get(&stream_name)
            .cloned()
            .ok_or_else(|| py_runtime_error("stream schema is not registered in this client"))?;
        let writer = self
            .client
            .lock()
            .unwrap()
            .write_to(&stream_name)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(BusWriter {
            writer: Arc::new(Mutex::new(Some(writer))),
            schema,
        })
    }

    #[pyo3(signature = (stream_name, instrument_ids=None, xfast=false))]
    fn read_from(
        &self,
        stream_name: String,
        instrument_ids: Option<&Bound<'_, PyAny>>,
        xfast: bool,
    ) -> PyResult<BusReader> {
        let instrument_ids = parse_instrument_ids(instrument_ids)?;
        let reader = attach_bus_reader_with(
            &self.client,
            &stream_name,
            instrument_ids,
            xfast,
            |client, stream_name, xfast| client.read_from_with_xfast(stream_name, xfast),
            |client, stream_name, instrument_ids, xfast| {
                client.read_from_filtered_with_xfast(stream_name, instrument_ids, xfast)
            },
        )
        .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(BusReader {
            reader: Arc::new(Mutex::new(Some(reader))),
        })
    }

    fn list_streams(&self, py: Python<'_>) -> PyResult<PyObject> {
        let streams = self
            .client
            .lock()
            .unwrap()
            .list_streams()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let records = PyList::empty_bound(py);
        for stream in streams {
            let dict = PyDict::new_bound(py);
            dict.set_item("stream_name", stream.stream_name)?;
            dict.set_item("schema", serde_json_value_to_py(py, &stream.schema)?)?;
            dict.set_item("schema_hash", stream.schema_hash)?;
            dict.set_item("data_path", stream.data_path)?;
            dict.set_item("descriptor_generation", stream.descriptor_generation)?;
            dict.set_item(
                "active_segment_descriptor",
                serde_json_option_to_py(py, stream.active_segment_descriptor.as_ref())?,
            )?;
            dict.set_item(
                "sealed_segments",
                serde_json_value_to_py(py, &stream.sealed_segments.into())?,
            )?;
            dict.set_item(
                "persisted_files",
                serde_json_value_to_py(py, &stream.persisted_files.clone().into())?,
            )?;
            dict.set_item(
                "persist_events",
                serde_json_value_to_py(py, &stream.persist_events.clone().into())?,
            )?;
            dict.set_item(
                "segment_reader_leases",
                serde_json_value_to_py(py, &stream.segment_reader_leases.clone().into())?,
            )?;
            dict.set_item("buffer_size", stream.buffer_size)?;
            dict.set_item("frame_size", stream.frame_size)?;
            dict.set_item("writer_process_id", stream.writer_process_id)?;
            dict.set_item("reader_count", stream.reader_count)?;
            dict.set_item("status", stream.status)?;
            records.append(dict)?;
        }
        Ok(records.into_py(py))
    }

    fn get_stream(&self, py: Python<'_>, stream_name: String) -> PyResult<PyObject> {
        let stream = self
            .client
            .lock()
            .unwrap()
            .get_stream(&stream_name)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let dict = PyDict::new_bound(py);
        dict.set_item("stream_name", stream.stream_name)?;
        dict.set_item("schema", serde_json_value_to_py(py, &stream.schema)?)?;
        dict.set_item("schema_hash", stream.schema_hash)?;
        dict.set_item("data_path", stream.data_path)?;
        dict.set_item("descriptor_generation", stream.descriptor_generation)?;
        dict.set_item(
            "active_segment_descriptor",
            serde_json_option_to_py(py, stream.active_segment_descriptor.as_ref())?,
        )?;
        dict.set_item(
            "sealed_segments",
            serde_json_value_to_py(py, &stream.sealed_segments.into())?,
        )?;
        dict.set_item(
            "persisted_files",
            serde_json_value_to_py(py, &stream.persisted_files.clone().into())?,
        )?;
        dict.set_item(
            "persist_events",
            serde_json_value_to_py(py, &stream.persist_events.clone().into())?,
        )?;
        dict.set_item(
            "segment_reader_leases",
            serde_json_value_to_py(py, &stream.segment_reader_leases.clone().into())?,
        )?;
        dict.set_item("buffer_size", stream.buffer_size)?;
        dict.set_item("frame_size", stream.frame_size)?;
        dict.set_item("writer_process_id", stream.writer_process_id)?;
        dict.set_item("reader_count", stream.reader_count)?;
        dict.set_item("status", stream.status)?;
        Ok(dict.into_py(py))
    }

    fn get_config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let config = self
            .client
            .lock()
            .unwrap()
            .get_config()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        serde_json_value_to_py(py, &config.to_json_value())
    }

    #[pyo3(signature = (table_name, drop_persisted=true))]
    fn drop_table(
        &self,
        py: Python<'_>,
        table_name: String,
        drop_persisted: bool,
    ) -> PyResult<PyObject> {
        let result = py
            .allow_threads(|| {
                self.client
                    .lock()
                    .unwrap()
                    .drop_table(&table_name, drop_persisted)
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        self.schemas.lock().unwrap().remove(&table_name);
        Ok(drop_table_result_to_pydict(py, &result)?.into_py(py))
    }

    fn process_id(&self) -> PyResult<Option<String>> {
        Ok(self
            .client
            .lock()
            .unwrap()
            .process_id()
            .map(ToOwned::to_owned))
    }

    fn control_endpoint(&self) -> PyResult<String> {
        Ok(self.control_endpoint.clone())
    }
}

fn stream_info_to_pydict<'py>(
    py: Python<'py>,
    stream: &StreamInfo,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("stream_name", &stream.stream_name)?;
    dict.set_item("schema", serde_json_value_to_py(py, &stream.schema)?)?;
    dict.set_item("schema_hash", &stream.schema_hash)?;
    dict.set_item("data_path", &stream.data_path)?;
    dict.set_item("descriptor_generation", stream.descriptor_generation)?;
    dict.set_item(
        "active_segment_descriptor",
        serde_json_option_to_py(py, stream.active_segment_descriptor.as_ref())?,
    )?;
    dict.set_item(
        "sealed_segments",
        serde_json_value_to_py(py, &stream.sealed_segments.clone().into())?,
    )?;
    dict.set_item(
        "persisted_files",
        serde_json_value_to_py(py, &stream.persisted_files.clone().into())?,
    )?;
    dict.set_item(
        "persist_events",
        serde_json_value_to_py(py, &stream.persist_events.clone().into())?,
    )?;
    dict.set_item(
        "segment_reader_leases",
        serde_json_value_to_py(py, &stream.segment_reader_leases.clone().into())?,
    )?;
    dict.set_item("buffer_size", stream.buffer_size)?;
    dict.set_item("frame_size", stream.frame_size)?;
    dict.set_item("writer_process_id", stream.writer_process_id.clone())?;
    dict.set_item("reader_count", stream.reader_count)?;
    dict.set_item("status", &stream.status)?;
    Ok(dict)
}

fn drop_table_result_to_pydict<'py>(
    py: Python<'py>,
    result: &DropTableResult,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("table_name", &result.table_name)?;
    dict.set_item("dropped", result.dropped)?;
    dict.set_item("sources_removed", result.sources_removed)?;
    dict.set_item("engines_removed", result.engines_removed)?;
    dict.set_item("sinks_removed", result.sinks_removed)?;
    dict.set_item("persisted_files_deleted", result.persisted_files_deleted)?;
    Ok(dict)
}

#[pyclass]
struct Query {
    source: String,
    master: SharedMasterClient,
    schema: Arc<Schema>,
    segment_schema: CompiledSchema,
}

struct SegmentReaderLeaseGuard {
    master: SharedMasterClient,
    source: String,
    lease_id: Option<String>,
}

struct SegmentReaderLeaseSet {
    _leases: Vec<SegmentReaderLeaseGuard>,
}

impl SegmentReaderLeaseGuard {
    fn acquire(
        master: SharedMasterClient,
        source: impl Into<String>,
        descriptor: &serde_json::Value,
    ) -> zippy_core::Result<Self> {
        let source = source.into();
        let (segment_id, generation) = descriptor_segment_identity(descriptor)?;
        let lease_id = master
            .lock()
            .unwrap()
            .acquire_segment_reader_lease(&source, segment_id, generation)?;
        Ok(Self {
            master,
            source,
            lease_id: Some(lease_id),
        })
    }
}

impl Drop for SegmentReaderLeaseGuard {
    fn drop(&mut self) {
        let Some(lease_id) = self.lease_id.take() else {
            return;
        };
        if let Err(error) = self
            .master
            .lock()
            .unwrap()
            .release_segment_reader_lease(&self.source, &lease_id)
        {
            error!(
                event = "release_segment_reader_lease",
                source = %self.source,
                lease_id = %lease_id,
                error = %error,
                "failed to release segment reader lease"
            );
        }
    }
}

impl SegmentReaderLeaseSet {
    fn acquire_for_descriptors(
        master: SharedMasterClient,
        source: &str,
        active_descriptor: &serde_json::Value,
        sealed_descriptors: &[serde_json::Value],
    ) -> zippy_core::Result<Self> {
        let mut leases = Vec::with_capacity(sealed_descriptors.len() + 1);
        for descriptor in sealed_descriptors {
            leases.push(SegmentReaderLeaseGuard::acquire(
                Arc::clone(&master),
                source,
                descriptor,
            )?);
        }
        leases.push(SegmentReaderLeaseGuard::acquire(
            master,
            source,
            active_descriptor,
        )?);
        Ok(Self { _leases: leases })
    }

    fn acquire_for_active(
        master: SharedMasterClient,
        source: &str,
        active_descriptor: &serde_json::Value,
    ) -> zippy_core::Result<Self> {
        Ok(Self {
            _leases: vec![SegmentReaderLeaseGuard::acquire(
                master,
                source,
                active_descriptor,
            )?],
        })
    }
}

#[pymethods]
impl Query {
    #[new]
    #[pyo3(signature = (source, master))]
    fn new(py: Python<'_>, source: String, master: &Bound<'_, PyAny>) -> PyResult<Self> {
        let master = master
            .extract::<PyRef<'_, MasterClient>>()
            .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
        let shared_master = Arc::clone(&master.client);
        let stream = py
            .allow_threads(|| shared_master.lock().unwrap().get_stream(&source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        if stream.data_path != "segment" {
            return Err(py_value_error(format!(
                "query only supports segment streams data_path=[{}]",
                stream.data_path
            )));
        }
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;

        Ok(Self {
            source,
            master: shared_master,
            schema,
            segment_schema,
        })
    }

    fn tail(&self, py: Python<'_>, n: usize) -> PyResult<PyObject> {
        let stream = py
            .allow_threads(|| self.master.lock().unwrap().get_stream(&self.source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        ensure_stream_live_readable(&stream)?;
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(py_runtime_error(format!(
                "segment descriptor is not published source=[{}]",
                self.source
            )));
        };
        let segment_schema = self.segment_schema.clone();
        let master = Arc::clone(&self.master);
        let source = self.source.clone();
        let batch = py
            .allow_threads(|| {
                let _leases = SegmentReaderLeaseSet::acquire_for_descriptors(
                    master,
                    &source,
                    &descriptor,
                    &stream.sealed_segments,
                )?;
                tail_live_segment_record_batch(
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    n,
                )
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        record_batch_to_pyarrow_table(py, batch)
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn stream_info(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stream = py
            .allow_threads(|| self.master.lock().unwrap().get_stream(&self.source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        Ok(stream_info_to_pydict(py, &stream)?.into_py(py))
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stream = py
            .allow_threads(|| self.master.lock().unwrap().get_stream(&self.source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        ensure_stream_live_readable(&stream)?;
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(py_runtime_error(format!(
                "segment descriptor is not published source=[{}]",
                self.source
            )));
        };
        let segment_schema = self.segment_schema.clone();
        let master = Arc::clone(&self.master);
        let source = self.source.clone();
        let active_segment_control = py
            .allow_threads(|| {
                let _leases =
                    SegmentReaderLeaseSet::acquire_for_active(master, &source, &descriptor)?;
                active_segment_control_snapshot(&descriptor, segment_schema)
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let snapshot = query_snapshot_value(&stream, descriptor, active_segment_control)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        serde_json_value_to_py(py, &snapshot)
    }

    fn scan_live(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stream = py
            .allow_threads(|| self.master.lock().unwrap().get_stream(&self.source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        ensure_stream_live_readable(&stream)?;
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(py_runtime_error(format!(
                "segment descriptor is not published source=[{}]",
                self.source
            )));
        };
        let segment_schema = self.segment_schema.clone();
        let master = Arc::clone(&self.master);
        let source = self.source.clone();
        let batches = py
            .allow_threads(|| {
                let _leases = SegmentReaderLeaseSet::acquire_for_descriptors(
                    master,
                    &source,
                    &descriptor,
                    &stream.sealed_segments,
                )?;
                let active_committed_row_high_watermark =
                    active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
                live_segment_record_batches(
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    active_committed_row_high_watermark,
                )
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        record_batches_to_pyarrow_record_batch_reader(py, self.schema.as_ref(), batches)
    }
}

fn ensure_stream_live_readable(stream: &StreamInfo) -> PyResult<()> {
    if stream.status == "stale" {
        return Err(py_runtime_error(format!(
            "stream is stale source=[{}] status=[{}]",
            stream.stream_name, stream.status
        )));
    }
    Ok(())
}

fn query_snapshot_value(
    stream: &StreamInfo,
    descriptor: serde_json::Value,
    active_segment_control: serde_json::Value,
) -> zippy_core::Result<serde_json::Value> {
    let mut active_segment_descriptor = descriptor;
    if let Some(object) = active_segment_descriptor.as_object_mut() {
        object.remove("sealed_segments");
    }
    let active_committed_row_high_watermark = active_segment_control
        .get("committed_row_count")
        .and_then(serde_json::Value::as_u64)
        .ok_or(ZippyError::InvalidState {
            status: "active segment control missing committed_row_count",
        })?;

    Ok(serde_json::json!({
        "stream_name": stream.stream_name,
        "schema": stream.schema,
        "schema_hash": stream.schema_hash,
        "data_path": stream.data_path,
        "descriptor_generation": stream.descriptor_generation,
        "active_segment_descriptor": active_segment_descriptor,
        "active_segment_control": active_segment_control,
        "active_committed_row_high_watermark": active_committed_row_high_watermark,
        "sealed_segments": stream.sealed_segments.clone(),
        "persisted_files": stream.persisted_files.clone(),
        "persist_events": stream.persist_events.clone(),
        "segment_reader_leases": stream.segment_reader_leases.clone(),
    }))
}

fn tail_live_segment_record_batch(
    descriptor: &serde_json::Value,
    sealed_descriptors: &[serde_json::Value],
    segment_schema: CompiledSchema,
    n: usize,
) -> zippy_core::Result<RecordBatch> {
    let mut remaining = n;
    let mut batches_reversed = Vec::new();

    let active_batch = tail_single_descriptor_record_batch(descriptor, segment_schema.clone(), n)?;
    remaining = remaining.saturating_sub(active_batch.num_rows());
    batches_reversed.push(active_batch);

    for sealed_descriptor in sealed_descriptors.iter().rev() {
        if remaining == 0 {
            break;
        }
        let batch = tail_single_descriptor_record_batch(
            sealed_descriptor,
            segment_schema.clone(),
            remaining,
        )?;
        remaining = remaining.saturating_sub(batch.num_rows());
        batches_reversed.push(batch);
    }

    batches_reversed.reverse();
    concat_tail_batches(batches_reversed)
}

fn tail_single_descriptor_record_batch(
    descriptor: &serde_json::Value,
    segment_schema: CompiledSchema,
    n: usize,
) -> zippy_core::Result<RecordBatch> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let start_row = committed.saturating_sub(n);
    let span = RowSpanView::from_active_descriptor(active_descriptor, start_row, committed)
        .map_err(|reason| ZippyError::InvalidState { status: reason })?;
    span.as_record_batch().map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn live_segment_record_batches(
    descriptor: &serde_json::Value,
    sealed_descriptors: &[serde_json::Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
) -> zippy_core::Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(sealed_descriptors.len() + 1);

    for sealed_descriptor in sealed_descriptors {
        let batch = descriptor_record_batch_until(sealed_descriptor, segment_schema.clone(), None)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    let active_batch = descriptor_record_batch_until(
        descriptor,
        segment_schema,
        Some(active_committed_row_high_watermark),
    )?;
    if active_batch.num_rows() > 0 {
        batches.push(active_batch);
    }

    Ok(batches)
}

fn descriptor_record_batch_until(
    descriptor: &serde_json::Value,
    segment_schema: CompiledSchema,
    end_row_limit: Option<usize>,
) -> zippy_core::Result<RecordBatch> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let end_row = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let span = RowSpanView::from_active_descriptor(active_descriptor, 0, end_row)
        .map_err(|reason| ZippyError::InvalidState { status: reason })?;
    span.as_record_batch().map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn active_descriptor_with_committed_row_count(
    descriptor: &serde_json::Value,
    segment_schema: CompiledSchema,
) -> zippy_core::Result<(usize, ActiveSegmentDescriptor)> {
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
            .map_err(|reason| ZippyError::InvalidConfig {
                reason: reason.to_string(),
            })?;
    Ok((committed, active_descriptor))
}

fn active_committed_row_high_watermark(
    descriptor: &serde_json::Value,
    segment_schema: CompiledSchema,
) -> zippy_core::Result<usize> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, segment_schema, layout)
        .map_err(segment_zippy_error)?
        .committed_row_count()
        .map_err(segment_zippy_error)
}

fn active_segment_control_snapshot(
    descriptor: &serde_json::Value,
    segment_schema: CompiledSchema,
) -> zippy_core::Result<serde_json::Value> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    let snapshot =
        ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, segment_schema, layout)
            .map_err(segment_zippy_error)?
            .control_snapshot()
            .map_err(segment_zippy_error)?;
    Ok(segment_control_snapshot_to_json(snapshot))
}

fn segment_control_snapshot_to_json(snapshot: SegmentControlSnapshot) -> serde_json::Value {
    serde_json::json!({
        "magic": snapshot.magic,
        "layout_version": snapshot.layout_version,
        "schema_id": snapshot.schema_id,
        "segment_id": snapshot.segment_id,
        "generation": snapshot.generation,
        "writer_epoch": snapshot.writer_epoch,
        "descriptor_generation": snapshot.descriptor_generation,
        "capacity_rows": snapshot.capacity_rows,
        "row_count": snapshot.row_count,
        "committed_row_count": snapshot.committed_row_count,
        "notify_seq": snapshot.notify_seq,
        "waiter_count": snapshot.waiter_count,
        "sealed": snapshot.sealed,
        "payload_offset": snapshot.payload_offset,
        "committed_row_count_offset": snapshot.committed_row_count_offset,
    })
}

fn concat_tail_batches(mut batches: Vec<RecordBatch>) -> zippy_core::Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches.remove(0));
    }
    let schema = batches
        .first()
        .ok_or(ZippyError::InvalidState {
            status: "tail produced no record batches",
        })?
        .schema();
    concat_batches(&schema, batches.iter()).map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn descriptor_row_capacity(descriptor: &serde_json::Value) -> zippy_core::Result<usize> {
    let row_capacity = descriptor
        .get("row_capacity")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing row_capacity".to_string(),
        })?;
    usize::try_from(row_capacity).map_err(|_| ZippyError::InvalidConfig {
        reason: "segment descriptor row_capacity overflows usize".to_string(),
    })
}

fn record_batch_to_pyarrow_table(py: Python<'_>, batch: RecordBatch) -> PyResult<PyObject> {
    let py_batch = batch
        .to_pyarrow(py)
        .map_err(|error| py_value_error(error.to_string()))?;
    let pyarrow = PyModule::import_bound(py, "pyarrow")
        .map_err(|error| py_runtime_error(format!("failed to import pyarrow error=[{}]", error)))?;
    let batches = PyList::empty_bound(py);
    batches.append(py_batch)?;
    Ok(pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (batches,))?
        .into_py(py))
}

fn record_batches_to_pyarrow_record_batch_reader(
    py: Python<'_>,
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> PyResult<PyObject> {
    let pyarrow = PyModule::import_bound(py, "pyarrow")
        .map_err(|error| py_runtime_error(format!("failed to import pyarrow error=[{}]", error)))?;
    let py_schema = schema
        .to_pyarrow(py)
        .map_err(|error| py_value_error(error.to_string()))?;
    let py_batches = PyList::empty_bound(py);
    for batch in batches {
        py_batches.append(
            batch
                .to_pyarrow(py)
                .map_err(|error| py_value_error(error.to_string()))?,
        )?;
    }
    Ok(pyarrow
        .getattr("RecordBatchReader")?
        .call_method1("from_batches", (py_schema, py_batches))?
        .into_py(py))
}

const SEGMENT_DESCRIPTOR_WATCH_TIMEOUT: Duration = Duration::from_millis(500);
const SEGMENT_READER_IDLE_SPIN_CHECKS: u32 = 64;

#[derive(Clone, Debug)]
struct DescriptorUpdate {
    descriptor_generation: u64,
    text: String,
    descriptor: serde_json::Value,
}

#[derive(Debug, Default)]
struct DescriptorUpdateSlot {
    update: Mutex<Option<DescriptorUpdate>>,
    changed: Condvar,
}

impl DescriptorUpdateSlot {
    fn set(&self, update: DescriptorUpdate) {
        *self.update.lock().unwrap() = Some(update);
        self.changed.notify_all();
    }

    fn notify(&self) {
        self.changed.notify_all();
    }

    fn take_after_poll(
        &self,
        read_outcome: SubscriberReadOutcome,
        current_descriptor_text: &str,
    ) -> Option<DescriptorUpdate> {
        if read_outcome != SubscriberReadOutcome::Empty {
            return None;
        }

        let mut guard = self.update.lock().unwrap();
        Self::take_distinct_update(&mut guard, current_descriptor_text)
    }

    fn wait_for_update_after(
        &self,
        current_descriptor_text: &str,
        timeout: Duration,
    ) -> Option<DescriptorUpdate> {
        let deadline = Instant::now() + timeout;
        let mut guard = self.update.lock().unwrap();
        loop {
            if let Some(update) = Self::take_distinct_update(&mut guard, current_descriptor_text) {
                return Some(update);
            }

            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, wait_result) = self.changed.wait_timeout(guard, remaining).unwrap();
            guard = next_guard;
            if wait_result.timed_out() {
                return Self::take_distinct_update(&mut guard, current_descriptor_text);
            }
        }
    }

    fn take_distinct_update(
        guard: &mut Option<DescriptorUpdate>,
        current_descriptor_text: &str,
    ) -> Option<DescriptorUpdate> {
        match guard.take() {
            Some(update) if update.text != current_descriptor_text => Some(update),
            _ => None,
        }
    }
}

#[derive(Default)]
struct SubscriberMetrics {
    rows_delivered_total: AtomicU64,
    batches_delivered_total: AtomicU64,
    descriptor_updates_total: AtomicU64,
    current_descriptor_generation: AtomicU64,
    last_descriptor_update_ns: AtomicU64,
    mmap_spin_checks_total: AtomicU64,
    mmap_futex_waits_total: AtomicU64,
    mmap_futex_notifications_total: AtomicU64,
    mmap_futex_timeouts_total: AtomicU64,
}

impl SubscriberMetrics {
    fn record_descriptor_update(&self, descriptor_generation: u64) {
        self.descriptor_updates_total.fetch_add(1, Ordering::SeqCst);
        self.current_descriptor_generation
            .store(descriptor_generation, Ordering::SeqCst);
        self.last_descriptor_update_ns
            .store(current_localtime_ns() as u64, Ordering::SeqCst);
    }

    fn record_rows_delivered(&self, row_count: usize) {
        self.rows_delivered_total
            .fetch_add(row_count as u64, Ordering::SeqCst);
    }

    fn record_batch_delivered(&self, row_count: usize) {
        self.batches_delivered_total.fetch_add(1, Ordering::SeqCst);
        self.record_rows_delivered(row_count);
    }

    fn record_mmap_spin_check(&self) {
        self.mmap_spin_checks_total.fetch_add(1, Ordering::SeqCst);
    }

    fn record_mmap_futex_wait(&self, notified: bool) {
        self.mmap_futex_waits_total.fetch_add(1, Ordering::SeqCst);
        if notified {
            self.mmap_futex_notifications_total
                .fetch_add(1, Ordering::SeqCst);
        } else {
            self.mmap_futex_timeouts_total
                .fetch_add(1, Ordering::SeqCst);
        }
    }
}

type SubscriberJoinHandle = Arc<Mutex<Option<JoinHandle<std::result::Result<(), String>>>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriberReadOutcome {
    Rows,
    Empty,
}

fn descriptor_update_from_value(
    descriptor_generation: u64,
    descriptor: serde_json::Value,
) -> std::result::Result<DescriptorUpdate, String> {
    let text = serde_json::to_string(&descriptor).map_err(|error| error.to_string())?;
    Ok(DescriptorUpdate {
        descriptor_generation,
        text,
        descriptor,
    })
}

fn take_descriptor_update_after_poll(
    read_outcome: SubscriberReadOutcome,
    descriptor_updates: &DescriptorUpdateSlot,
    current_descriptor_text: &str,
) -> Option<DescriptorUpdate> {
    descriptor_updates.take_after_poll(read_outcome, current_descriptor_text)
}

fn take_descriptor_refresh_error(error_slot: &Mutex<Option<String>>) -> Option<String> {
    error_slot.lock().unwrap().take()
}

fn apply_segment_descriptor_update(
    reader: &mut ActiveSegmentReader,
    descriptor_text: &mut String,
    update: DescriptorUpdate,
    segment_schema: &CompiledSchema,
    reader_lease: Option<&mut SegmentReaderLeaseGuard>,
) -> std::result::Result<(), String> {
    let next_lease = match reader_lease.as_ref() {
        Some(lease) => Some(
            SegmentReaderLeaseGuard::acquire(
                Arc::clone(&lease.master),
                lease.source.clone(),
                &update.descriptor,
            )
            .map_err(|error| error.to_string())?,
        ),
        None => None,
    };
    let (next_reader, _) = build_active_segment_reader(&update.descriptor, segment_schema)
        .map_err(|error| error.to_string())?;
    *reader = next_reader;
    *descriptor_text = update.text;
    if let (Some(reader_lease), Some(next_lease)) = (reader_lease, next_lease) {
        *reader_lease = next_lease;
    }
    Ok(())
}

enum SegmentReaderDriverEvent {
    Rows(RowSpanView),
    DescriptorUpdated(u64),
    Idle,
}

impl fmt::Debug for SegmentReaderDriverEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rows(_) => formatter.write_str("Rows(..)"),
            Self::DescriptorUpdated(generation) => formatter
                .debug_tuple("DescriptorUpdated")
                .field(generation)
                .finish(),
            Self::Idle => formatter.write_str("Idle"),
        }
    }
}

struct SegmentReaderDriver {
    reader: ActiveSegmentReader,
    descriptor_text: String,
    reader_lease: Option<SegmentReaderLeaseGuard>,
    segment_schema: CompiledSchema,
    descriptor_updates: Arc<DescriptorUpdateSlot>,
    descriptor_refresh_error: Arc<Mutex<Option<String>>>,
    xfast: bool,
    idle_wait: Duration,
    idle_spin_checks: u32,
    metrics: Option<Arc<SubscriberMetrics>>,
}

impl SegmentReaderDriver {
    #[allow(clippy::too_many_arguments)]
    fn new(
        reader: ActiveSegmentReader,
        descriptor_text: String,
        reader_lease: Option<SegmentReaderLeaseGuard>,
        segment_schema: CompiledSchema,
        descriptor_updates: Arc<DescriptorUpdateSlot>,
        descriptor_refresh_error: Arc<Mutex<Option<String>>>,
        xfast: bool,
        idle_wait: Duration,
        idle_spin_checks: u32,
        metrics: Option<Arc<SubscriberMetrics>>,
    ) -> Self {
        Self {
            reader,
            descriptor_text,
            reader_lease,
            segment_schema,
            descriptor_updates,
            descriptor_refresh_error,
            xfast,
            idle_wait,
            idle_spin_checks,
            metrics,
        }
    }

    fn poll_next(&mut self) -> std::result::Result<SegmentReaderDriverEvent, String> {
        if let Some(error) = take_descriptor_refresh_error(self.descriptor_refresh_error.as_ref()) {
            return Err(error);
        }

        let observed_notification_seq = self
            .reader
            .notification_sequence()
            .map_err(|error| error.to_string())?;
        match self
            .reader
            .read_available()
            .map_err(|error| error.to_string())?
        {
            Some(span) => {
                let _ = take_descriptor_update_after_poll(
                    SubscriberReadOutcome::Rows,
                    self.descriptor_updates.as_ref(),
                    &self.descriptor_text,
                );
                Ok(SegmentReaderDriverEvent::Rows(span))
            }
            None => {
                if let Some(update) = take_descriptor_update_after_poll(
                    SubscriberReadOutcome::Empty,
                    self.descriptor_updates.as_ref(),
                    &self.descriptor_text,
                ) {
                    let descriptor_generation = update.descriptor_generation;
                    apply_segment_descriptor_update(
                        &mut self.reader,
                        &mut self.descriptor_text,
                        update,
                        &self.segment_schema,
                        self.reader_lease.as_mut(),
                    )?;
                    return Ok(SegmentReaderDriverEvent::DescriptorUpdated(
                        descriptor_generation,
                    ));
                }

                let sealed = self.reader.is_sealed().map_err(|error| error.to_string())?;
                if sealed {
                    let update = if self.xfast {
                        spin_loop();
                        None
                    } else {
                        self.descriptor_updates
                            .wait_for_update_after(&self.descriptor_text, self.idle_wait)
                    };
                    if let Some(error) =
                        take_descriptor_refresh_error(self.descriptor_refresh_error.as_ref())
                    {
                        return Err(error);
                    }
                    if let Some(update) = update {
                        let descriptor_generation = update.descriptor_generation;
                        apply_segment_descriptor_update(
                            &mut self.reader,
                            &mut self.descriptor_text,
                            update,
                            &self.segment_schema,
                            self.reader_lease.as_mut(),
                        )?;
                        return Ok(SegmentReaderDriverEvent::DescriptorUpdated(
                            descriptor_generation,
                        ));
                    }
                    return Ok(SegmentReaderDriverEvent::Idle);
                }

                if self.xfast {
                    spin_loop();
                } else {
                    self.wait_for_mmap_notification_after(observed_notification_seq)?;
                }
                Ok(SegmentReaderDriverEvent::Idle)
            }
        }
    }

    fn wait_for_mmap_notification_after(&self, observed: u32) -> std::result::Result<bool, String> {
        for _ in 0..self.idle_spin_checks {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.record_mmap_spin_check();
            }
            spin_loop();
            if self
                .reader
                .notification_sequence()
                .map_err(|error| error.to_string())?
                != observed
            {
                return Ok(true);
            }
        }

        let notified = self
            .reader
            .wait_for_notification_after(observed, self.idle_wait)
            .map_err(|error| error.to_string())?;
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.record_mmap_futex_wait(notified);
        }
        Ok(notified)
    }

    fn finish(&self) -> std::result::Result<(), String> {
        if let Some(error) = take_descriptor_refresh_error(self.descriptor_refresh_error.as_ref()) {
            return Err(error);
        }
        Ok(())
    }
}

fn spawn_segment_descriptor_watcher(
    running: Arc<AtomicBool>,
    master: SharedMasterClient,
    stream_name: String,
    mut descriptor_generation: u64,
    descriptor_updates: Arc<DescriptorUpdateSlot>,
    descriptor_refresh_error: Arc<Mutex<Option<String>>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        while running.load(Ordering::SeqCst) {
            let client = master.lock().unwrap().clone();
            let update = match client.wait_segment_descriptor(
                &stream_name,
                descriptor_generation,
                SEGMENT_DESCRIPTOR_WATCH_TIMEOUT,
            ) {
                Ok(Some(update)) => update,
                Ok(None) => continue,
                Err(error) => {
                    *descriptor_refresh_error.lock().unwrap() = Some(error.to_string());
                    descriptor_updates.notify();
                    running.store(false, Ordering::SeqCst);
                    break;
                }
            };

            descriptor_generation = update.descriptor_generation;
            match descriptor_update_from_value(update.descriptor_generation, update.descriptor) {
                Ok(update) => {
                    descriptor_updates.set(update);
                }
                Err(error) => {
                    *descriptor_refresh_error.lock().unwrap() = Some(error);
                    descriptor_updates.notify();
                    running.store(false, Ordering::SeqCst);
                    break;
                }
            }
        }
    })
}

#[pyclass]
struct StreamSubscriber {
    source: String,
    master: SharedMasterClient,
    segment_schema: CompiledSchema,
    callback: Py<PyAny>,
    row_factory: Option<Py<PyAny>>,
    instrument_filter: Option<BTreeSet<String>>,
    poll_interval: Duration,
    xfast: bool,
    idle_spin_checks: u32,
    running: Arc<AtomicBool>,
    metrics: Arc<SubscriberMetrics>,
    join_handle: SubscriberJoinHandle,
}

#[pymethods]
impl StreamSubscriber {
    #[new]
    #[pyo3(signature = (source, master, callback, poll_interval_ms=1, xfast=false, idle_spin_checks=SEGMENT_READER_IDLE_SPIN_CHECKS, row_factory=None, instrument_ids=None))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        source: String,
        master: &Bound<'_, PyAny>,
        callback: Py<PyAny>,
        poll_interval_ms: u64,
        xfast: bool,
        idle_spin_checks: u32,
        row_factory: Option<Py<PyAny>>,
        instrument_ids: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        if poll_interval_ms == 0 && !xfast {
            return Err(py_value_error(
                "poll_interval_ms must be greater than zero unless xfast is true",
            ));
        }
        let instrument_filter =
            parse_instrument_ids(instrument_ids)?.map(|values| values.into_iter().collect());
        if instrument_filter.is_some() && row_factory.is_none() {
            return Err(py_value_error(
                "instrument_ids is only supported by stream row callbacks",
            ));
        }
        let master = master
            .extract::<PyRef<'_, MasterClient>>()
            .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
        let shared_master = Arc::clone(&master.client);
        let stream = py
            .allow_threads(|| shared_master.lock().unwrap().get_stream(&source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        if stream.data_path != "segment" {
            return Err(py_value_error(format!(
                "stream subscriber only supports segment streams data_path=[{}]",
                stream.data_path
            )));
        }
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;

        Ok(Self {
            source,
            master: shared_master,
            segment_schema,
            callback,
            row_factory,
            instrument_filter,
            poll_interval: Duration::from_millis(poll_interval_ms),
            xfast,
            idle_spin_checks,
            running: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(SubscriberMetrics::default()),
            join_handle: Arc::new(Mutex::new(None)),
        })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut guard = self.join_handle.lock().unwrap();
        if guard.is_some() {
            return Err(py_runtime_error("stream subscriber is already started"));
        }

        let stream = py
            .allow_threads(|| self.master.lock().unwrap().get_stream(&self.source))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let descriptor = py
            .allow_threads(|| {
                self.master
                    .lock()
                    .unwrap()
                    .get_segment_descriptor(&self.source)
            })
            .map_err(|error| py_runtime_error(error.to_string()))?
            .ok_or_else(|| {
                py_runtime_error(format!(
                    "segment descriptor is not published source=[{}]",
                    self.source
                ))
            })?;
        let descriptor_text = serde_json::to_string(&descriptor)
            .map_err(|error| py_value_error(error.to_string()))?;
        let reader_lease = py
            .allow_threads(|| {
                SegmentReaderLeaseGuard::acquire(
                    Arc::clone(&self.master),
                    self.source.clone(),
                    &descriptor,
                )
            })
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let reader = active_segment_reader_from_descriptor(&descriptor, &self.segment_schema)
            .map_err(|error| py_runtime_error(error.to_string()))?;

        self.metrics
            .current_descriptor_generation
            .store(stream.descriptor_generation, Ordering::SeqCst);
        self.running.store(true, Ordering::SeqCst);
        let running = Arc::clone(&self.running);
        let metrics = Arc::clone(&self.metrics);
        let master = Arc::clone(&self.master);
        let source = self.source.clone();
        let segment_schema = self.segment_schema.clone();
        let callback = self.callback.clone_ref(py);
        let row_factory = self
            .row_factory
            .as_ref()
            .map(|factory| factory.clone_ref(py));
        let instrument_filter = self.instrument_filter.clone();
        let poll_interval = self.poll_interval;
        let xfast = self.xfast;
        let idle_spin_checks = self.idle_spin_checks;

        *guard = Some(thread::spawn(move || {
            let descriptor_updates = Arc::new(DescriptorUpdateSlot::default());
            let descriptor_refresh_error = Arc::new(Mutex::new(None));
            let descriptor_refresh_handle = spawn_segment_descriptor_watcher(
                Arc::clone(&running),
                Arc::clone(&master),
                source.clone(),
                stream.descriptor_generation,
                Arc::clone(&descriptor_updates),
                Arc::clone(&descriptor_refresh_error),
            );

            let mut driver = SegmentReaderDriver::new(
                reader,
                descriptor_text,
                Some(reader_lease),
                segment_schema,
                Arc::clone(&descriptor_updates),
                Arc::clone(&descriptor_refresh_error),
                xfast,
                poll_interval,
                idle_spin_checks,
                Some(Arc::clone(&metrics)),
            );

            let result = (|| -> std::result::Result<(), String> {
                while running.load(Ordering::SeqCst) {
                    match driver.poll_next()? {
                        SegmentReaderDriverEvent::Rows(span) => {
                            if let Some(row_factory) = row_factory.as_ref() {
                                let delivered_rows = invoke_stream_row_callbacks(
                                    &callback,
                                    row_factory,
                                    span,
                                    instrument_filter.as_ref(),
                                )?;
                                metrics.record_rows_delivered(delivered_rows);
                            } else {
                                let batch =
                                    span.as_record_batch().map_err(|error| error.to_string())?;
                                let row_count = batch.num_rows();
                                invoke_stream_callback(&callback, batch)?;
                                metrics.record_batch_delivered(row_count);
                            }
                        }
                        SegmentReaderDriverEvent::DescriptorUpdated(descriptor_generation) => {
                            metrics.record_descriptor_update(descriptor_generation);
                        }
                        SegmentReaderDriverEvent::Idle => {}
                    }
                }

                driver.finish()
            })();

            running.store(false, Ordering::SeqCst);
            let _ = descriptor_refresh_handle.join();
            result
        }));
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        self.running.store(false, Ordering::SeqCst);
        let join_handle = self.join_handle.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            py.allow_threads(|| join_stream_subscriber_thread(join_handle))?;
        }
        Ok(())
    }

    fn join(&mut self, py: Python<'_>) -> PyResult<()> {
        let join_handle = self.join_handle.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            py.allow_threads(|| join_stream_subscriber_thread(join_handle))?;
        }
        Ok(())
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);
        dict.set_item("source", &self.source)?;
        dict.set_item("running", self.running.load(Ordering::SeqCst))?;
        dict.set_item("idle_spin_checks", self.idle_spin_checks)?;
        dict.set_item(
            "rows_delivered_total",
            self.metrics.rows_delivered_total.load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "batches_delivered_total",
            self.metrics.batches_delivered_total.load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "descriptor_updates_total",
            self.metrics.descriptor_updates_total.load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "current_descriptor_generation",
            self.metrics
                .current_descriptor_generation
                .load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "last_descriptor_update_ns",
            self.metrics
                .last_descriptor_update_ns
                .load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "mmap_spin_checks_total",
            self.metrics.mmap_spin_checks_total.load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "mmap_futex_waits_total",
            self.metrics.mmap_futex_waits_total.load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "mmap_futex_notifications_total",
            self.metrics
                .mmap_futex_notifications_total
                .load(Ordering::SeqCst),
        )?;
        dict.set_item(
            "mmap_futex_timeouts_total",
            self.metrics
                .mmap_futex_timeouts_total
                .load(Ordering::SeqCst),
        )?;
        Ok(dict.into_py(py))
    }
}

fn invoke_stream_callback(
    callback: &Py<PyAny>,
    batch: RecordBatch,
) -> std::result::Result<(), String> {
    Python::with_gil(|py| -> PyResult<()> {
        let table = record_batch_to_pyarrow_table(py, batch)?;
        callback.call1(py, (table,))?;
        Ok(())
    })
    .map_err(|error| error.to_string())
}

fn invoke_stream_row_callbacks(
    callback: &Py<PyAny>,
    row_factory: &Py<PyAny>,
    span: RowSpanView,
    instrument_filter: Option<&BTreeSet<String>>,
) -> std::result::Result<usize, String> {
    if let Some(instrument_filter) = instrument_filter {
        let matching_rows = matching_instrument_row_indices(&span, instrument_filter)?;
        return Python::with_gil(|py| -> PyResult<usize> {
            for row_index in matching_rows.iter().copied() {
                let values = row_span_row_to_pydict(py, &span, row_index)?;
                let row = row_factory.call1(py, (values,))?;
                callback.call1(py, (row,))?;
            }
            Ok(matching_rows.len())
        })
        .map_err(|error| error.to_string());
    }

    Python::with_gil(|py| -> PyResult<usize> {
        for row_index in 0..span.row_count() {
            let values = row_span_row_to_pydict(py, &span, row_index)?;
            let row = row_factory.call1(py, (values,))?;
            callback.call1(py, (row,))?;
        }
        Ok(span.row_count())
    })
    .map_err(|error| error.to_string())
}

fn matching_instrument_row_indices(
    span: &RowSpanView,
    instrument_filter: &BTreeSet<String>,
) -> std::result::Result<Vec<usize>, String> {
    let instrument_column_index = span
        .column_index("instrument_id")
        .map_err(instrument_filter_error_message)?;
    let mut matching_rows = Vec::new();
    for row_index in 0..span.row_count() {
        if row_matches_instrument_filter_at(
            span,
            row_index,
            instrument_column_index,
            instrument_filter,
        )
        .map_err(instrument_filter_error_message)?
        {
            matching_rows.push(row_index);
        }
    }
    Ok(matching_rows)
}

#[cfg(test)]
fn row_matches_instrument_filter(
    span: &RowSpanView,
    row_offset: usize,
    instrument_filter: Option<&BTreeSet<String>>,
) -> PyResult<bool> {
    let Some(instrument_filter) = instrument_filter else {
        return Ok(true);
    };
    let instrument_column_index = span
        .column_index("instrument_id")
        .map_err(|error| py_runtime_error(error.to_string()))?;
    row_matches_instrument_filter_at(span, row_offset, instrument_column_index, instrument_filter)
        .map_err(instrument_filter_py_error)
}

fn row_matches_instrument_filter_at(
    span: &RowSpanView,
    row_offset: usize,
    instrument_column_index: usize,
    instrument_filter: &BTreeSet<String>,
) -> std::result::Result<bool, ZippySegmentStoreError> {
    match span.utf8_cell_value_at(row_offset, instrument_column_index)? {
        Some(instrument_id) => Ok(instrument_filter.contains(instrument_id)),
        None => Ok(false),
    }
}

fn instrument_filter_error_message(error: ZippySegmentStoreError) -> String {
    match error {
        ZippySegmentStoreError::Schema("column is not utf8") => {
            "instrument_id column must be utf8 when instrument_ids is used".to_string()
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
fn instrument_filter_py_error(error: ZippySegmentStoreError) -> PyErr {
    match error {
        ZippySegmentStoreError::Schema("column is not utf8") => {
            py_value_error("instrument_id column must be utf8 when instrument_ids is used")
        }
        other => py_runtime_error(other.to_string()),
    }
}

fn row_span_row_to_pydict<'py>(
    py: Python<'py>,
    span: &RowSpanView,
    row_offset: usize,
) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new_bound(py);
    for column in span.schema().columns() {
        let value = span
            .cell_value(row_offset, column.name)
            .map_err(|error| py_runtime_error(error.to_string()))?;
        values.set_item(column.name, segment_cell_value_to_py(py, value)?)?;
    }
    Ok(values)
}

fn segment_cell_value_to_py(py: Python<'_>, value: SegmentCellValue) -> PyResult<PyObject> {
    Ok(match value {
        SegmentCellValue::Null => py.None(),
        SegmentCellValue::Int64(value) | SegmentCellValue::TimestampNs(value) => value.into_py(py),
        SegmentCellValue::Float64(value) => value.into_py(py),
        SegmentCellValue::Utf8(value) => value.into_py(py),
    })
}

fn join_stream_subscriber_thread(
    join_handle: JoinHandle<std::result::Result<(), String>>,
) -> PyResult<()> {
    join_handle
        .join()
        .map_err(|_| py_runtime_error("stream subscriber thread panicked"))?
        .map_err(py_runtime_error)
}

#[pyclass(name = "MasterServer")]
struct MasterDaemon {
    control_endpoint: String,
    server: RustMasterServer,
    join_handle: Arc<Mutex<Option<JoinHandle<zippy_core::Result<()>>>>>,
}

#[pymethods]
impl MasterDaemon {
    #[new]
    #[pyo3(signature = (uri=None, *, control_endpoint=None))]
    fn new(uri: Option<String>, control_endpoint: Option<String>) -> PyResult<Self> {
        let control_endpoint = resolve_uri_argument(uri, control_endpoint)?;
        let resolved_control_endpoint = resolve_control_endpoint_value(&control_endpoint)?;
        let resolved_control_endpoint = resolved_control_endpoint.display_string();
        Ok(Self {
            control_endpoint: resolved_control_endpoint,
            server: RustMasterServer::default(),
            join_handle: Arc::new(Mutex::new(None)),
        })
    }

    #[pyo3(signature = (startup_timeout_sec=10.0))]
    fn start(&self, startup_timeout_sec: f64) -> PyResult<()> {
        let mut guard = self.join_handle.lock().unwrap();
        if guard.is_some() {
            return Err(py_runtime_error("master daemon already started"));
        }
        let startup_timeout = parse_startup_timeout(startup_timeout_sec)?;
        let endpoint = prepare_control_endpoint(&self.control_endpoint)?;
        let server = self.server.clone();
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let join_handle =
            thread::spawn(move || server.serve_endpoint_with_ready(&endpoint, Some(startup_tx)));

        match startup_rx.recv_timeout(startup_timeout) {
            Ok(Ok(())) => {
                *guard = Some(join_handle);
                Ok(())
            }
            Ok(Err(error)) => {
                let _ = join_handle.join();
                Err(py_runtime_error(error))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.server.shutdown();
                let _ = join_handle.join();
                Err(py_runtime_error(format!(
                    "master daemon did not become ready before timeout timeout_sec=[{}]",
                    startup_timeout_sec
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let result = join_handle
                    .join()
                    .map_err(|_| py_runtime_error("master daemon thread panicked"))?;
                result.map_err(|error| py_runtime_error(error.to_string()))
            }
        }
    }

    fn stop(&self) {
        self.server.shutdown();
    }

    fn control_endpoint(&self) -> String {
        self.control_endpoint.clone()
    }

    fn join(&self, py: Python<'_>) -> PyResult<()> {
        let join_handle = self.join_handle.lock().unwrap().take();
        let Some(join_handle) = join_handle else {
            return Ok(());
        };
        py.allow_threads(move || {
            join_handle.join().map_err(|_| ZippyError::Io {
                reason: "master daemon thread panicked".to_string(),
            })?
        })
        .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pyclass]
struct BusStreamTarget {
    stream_name: String,
    master: SharedMasterClient,
}

#[pymethods]
impl BusStreamTarget {
    #[new]
    #[pyo3(signature = (stream_name, master))]
    fn new(stream_name: String, master: &Bound<'_, PyAny>) -> PyResult<Self> {
        let master = master
            .extract::<PyRef<'_, MasterClient>>()
            .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
        Ok(Self {
            stream_name,
            master: Arc::clone(&master.client),
        })
    }
}

#[pyclass]
struct BusStreamSource {
    stream_name: String,
    expected_schema: Arc<Schema>,
    mode: RustSourceMode,
    master: SharedMasterClient,
    xfast: bool,
}

#[pymethods]
impl BusStreamSource {
    #[new]
    #[pyo3(signature = (stream_name, expected_schema, master, mode=None, xfast=false))]
    fn new(
        stream_name: String,
        expected_schema: &Bound<'_, PyAny>,
        master: &Bound<'_, PyAny>,
        mode: Option<&Bound<'_, PyAny>>,
        xfast: bool,
    ) -> PyResult<Self> {
        let expected_schema = Arc::new(
            Schema::from_pyarrow_bound(expected_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let master = master
            .extract::<PyRef<'_, MasterClient>>()
            .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
        Ok(Self {
            stream_name,
            expected_schema,
            mode: match mode {
                Some(mode) => parse_source_mode(mode)?,
                None => RustSourceMode::Pipeline,
            },
            master: Arc::clone(&master.client),
            xfast,
        })
    }
}

#[pyclass]
struct SegmentStreamSource {
    stream_name: String,
    expected_schema: Arc<Schema>,
    segment_schema: CompiledSchema,
    mode: RustSourceMode,
    master: SharedMasterClient,
    xfast: bool,
}

#[pymethods]
impl SegmentStreamSource {
    #[new]
    #[pyo3(signature = (stream_name, expected_schema, master, mode=None, xfast=false))]
    fn new(
        stream_name: String,
        expected_schema: &Bound<'_, PyAny>,
        master: &Bound<'_, PyAny>,
        mode: Option<&Bound<'_, PyAny>>,
        xfast: bool,
    ) -> PyResult<Self> {
        let expected_schema = Arc::new(
            Schema::from_pyarrow_bound(expected_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let segment_schema = compile_segment_schema_from_arrow(expected_schema.as_ref())?;
        let master = master
            .extract::<PyRef<'_, MasterClient>>()
            .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
        Ok(Self {
            stream_name,
            expected_schema,
            segment_schema,
            mode: match mode {
                Some(mode) => parse_source_mode(mode)?,
                None => RustSourceMode::Pipeline,
            },
            master: Arc::clone(&master.client),
            xfast,
        })
    }
}

#[pyclass(name = "_SegmentTestWriter")]
struct SegmentTestWriter {
    _store: SegmentStore,
    partition: PartitionHandle,
}

#[pymethods]
impl SegmentTestWriter {
    #[new]
    #[pyo3(signature = (stream_name, schema, row_capacity=32))]
    fn new(stream_name: String, schema: &Bound<'_, PyAny>, row_capacity: usize) -> PyResult<Self> {
        if row_capacity == 0 {
            return Err(py_value_error("row_capacity must be greater than zero"));
        }

        let arrow_schema = Schema::from_pyarrow_bound(schema)
            .map_err(|error| py_value_error(error.to_string()))?;
        let segment_schema = compile_segment_schema_from_arrow(&arrow_schema)?;
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: row_capacity,
        })
        .map_err(|error| py_runtime_error(error.to_string()))?;
        let partition = store
            .open_partition_with_schema(&stream_name, "all", segment_schema)
            .map_err(|error| py_runtime_error(error.to_string()))?;

        Ok(Self {
            _store: store,
            partition,
        })
    }

    fn descriptor(&self, py: Python<'_>) -> PyResult<PyObject> {
        let envelope = self
            .partition
            .active_descriptor_envelope_bytes()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let envelope_text =
            std::str::from_utf8(&envelope).map_err(|error| py_value_error(error.to_string()))?;
        Ok(python_json_loads(py, envelope_text)?.into_py(py))
    }

    fn append_tick(&self, dt: i64, instrument_id: String, last_price: f64) -> PyResult<()> {
        self.partition
            .writer()
            .append_tick_for_test(dt, &instrument_id, last_price)
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn committed_row_count(&self) -> usize {
        self.partition.active_committed_row_count()
    }

    fn rollover(&self) -> PyResult<()> {
        self.partition
            .writer()
            .rollover_without_persistence()
            .map(|_| ())
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pyclass]
struct BusWriter {
    writer: Arc<Mutex<Option<CoreBusWriter>>>,
    schema: Arc<Schema>,
}

#[pymethods]
impl BusWriter {
    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let batches = value_to_record_batches(py, value, self.schema.as_ref())?;
        let mut guard = self.writer.lock().unwrap();
        let writer = guard
            .as_mut()
            .ok_or_else(|| py_runtime_error("bus writer is closed"))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| py_runtime_error(error.to_string()))?;
        }
        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        let mut guard = self.writer.lock().unwrap();
        let writer = guard
            .as_mut()
            .ok_or_else(|| py_runtime_error("bus writer is closed"))?;
        writer
            .flush()
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn close(&self) -> PyResult<()> {
        let mut guard = self.writer.lock().unwrap();
        let mut writer = guard
            .take()
            .ok_or_else(|| py_runtime_error("bus writer is closed"))?;
        writer
            .close()
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pyclass]
struct BusReader {
    reader: Arc<Mutex<Option<CoreBusReader>>>,
}

#[pymethods]
impl BusReader {
    #[pyo3(signature = (timeout_ms=1000))]
    fn read(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<PyObject> {
        let mut guard = self.reader.lock().unwrap();
        let reader = guard
            .as_mut()
            .ok_or_else(|| py_runtime_error("bus reader is closed"))?;
        let batch = reader
            .read(Some(timeout_ms))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        batch
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn seek_latest(&self) -> PyResult<()> {
        let mut guard = self.reader.lock().unwrap();
        let reader = guard
            .as_mut()
            .ok_or_else(|| py_runtime_error("bus reader is closed"))?;
        reader
            .seek_latest()
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn close(&self) -> PyResult<()> {
        let mut guard = self.reader.lock().unwrap();
        let mut reader = guard
            .take()
            .ok_or_else(|| py_runtime_error("bus reader is closed"))?;
        reader
            .close()
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

struct BusTargetPublisher {
    writer: CoreBusWriter,
}

impl CorePublisher for BusTargetPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> zippy_core::Result<()> {
        self.writer
            .write_with_target_publish_enter_ns(batch.clone(), current_localtime_ns())
    }

    fn flush(&mut self) -> zippy_core::Result<()> {
        self.writer.flush()
    }

    fn close(&mut self) -> zippy_core::Result<()> {
        self.writer.close()
    }
}

struct BusSourceBridge {
    stream_name: String,
    expected_schema: Arc<Schema>,
    mode: RustSourceMode,
    master: SharedMasterClient,
    xfast: bool,
}

impl Source for BusSourceBridge {
    fn name(&self) -> &str {
        &self.stream_name
    }

    fn output_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.expected_schema)
    }

    fn mode(&self) -> RustSourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> zippy_core::Result<SourceHandle> {
        let mut reader = self
            .master
            .lock()
            .unwrap()
            .read_from_with_xfast(&self.stream_name, self.xfast)?;
        let hello = StreamHello::new(&self.stream_name, Arc::clone(&self.expected_schema), 1)?;
        sink.emit(SourceEvent::Hello(hello))?;

        let running = Arc::new(AtomicBool::new(true));
        let running_flag = Arc::clone(&running);
        let join_handle = thread::spawn(move || {
            while running_flag.load(Ordering::SeqCst) {
                match reader.read(Some(100)) {
                    Ok(batch) => sink.emit(SourceEvent::Data(
                        SegmentTableView::from_record_batch(batch),
                    ))?,
                    Err(ZippyError::Io { reason }) if reason.contains("reader timed out") => {
                        continue;
                    }
                    Err(error) => {
                        sink.emit(SourceEvent::Error(error.to_string()))?;
                        return Err(error);
                    }
                }
            }

            Ok(())
        });

        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                running.store(false, Ordering::SeqCst);
                Ok(())
            }),
        ))
    }
}

struct SegmentSourceBridge {
    stream_name: String,
    expected_schema: Arc<Schema>,
    segment_schema: CompiledSchema,
    mode: RustSourceMode,
    master: SharedMasterClient,
    start_at_tail: bool,
    xfast: bool,
}

impl Source for SegmentSourceBridge {
    fn name(&self) -> &str {
        &self.stream_name
    }

    fn output_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.expected_schema)
    }

    fn mode(&self) -> RustSourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> zippy_core::Result<SourceHandle> {
        let stream = self.master.lock().unwrap().get_stream(&self.stream_name)?;
        let descriptor = self
            .master
            .lock()
            .unwrap()
            .get_segment_descriptor(&self.stream_name)?
            .ok_or(ZippyError::InvalidState {
                status: "segment descriptor is not published",
            })?;
        let descriptor_text = serde_json::to_string(&descriptor).map_err(json_zippy_error)?;
        let reader_lease = SegmentReaderLeaseGuard::acquire(
            Arc::clone(&self.master),
            self.stream_name.clone(),
            &descriptor,
        )?;
        let mut reader = active_segment_reader_from_descriptor(&descriptor, &self.segment_schema)?;
        if self.start_at_tail {
            reader.seek_to_committed().map_err(segment_zippy_error)?;
        }
        let hello = StreamHello::new(&self.stream_name, Arc::clone(&self.expected_schema), 1)?;
        sink.emit(SourceEvent::Hello(hello))?;

        let running = Arc::new(AtomicBool::new(true));
        let running_flag = Arc::clone(&running);
        let master = Arc::clone(&self.master);
        let stream_name = self.stream_name.clone();
        let segment_schema = self.segment_schema.clone();
        let xfast = self.xfast;
        let join_handle = thread::spawn(move || {
            let descriptor_updates = Arc::new(DescriptorUpdateSlot::default());
            let descriptor_refresh_error = Arc::new(Mutex::new(None));
            let descriptor_refresh_handle = spawn_segment_descriptor_watcher(
                Arc::clone(&running_flag),
                Arc::clone(&master),
                stream_name.clone(),
                stream.descriptor_generation,
                Arc::clone(&descriptor_updates),
                Arc::clone(&descriptor_refresh_error),
            );

            let mut driver = SegmentReaderDriver::new(
                reader,
                descriptor_text,
                Some(reader_lease),
                segment_schema,
                Arc::clone(&descriptor_updates),
                Arc::clone(&descriptor_refresh_error),
                xfast,
                Duration::from_millis(1),
                SEGMENT_READER_IDLE_SPIN_CHECKS,
                None,
            );

            let result = (|| -> zippy_core::Result<()> {
                while running_flag.load(Ordering::SeqCst) {
                    match driver.poll_next().map_err(string_zippy_error)? {
                        SegmentReaderDriverEvent::Rows(span) => {
                            sink.emit(SourceEvent::Data(SegmentTableView::from_row_span(span)))?;
                        }
                        SegmentReaderDriverEvent::DescriptorUpdated(_) => {}
                        SegmentReaderDriverEvent::Idle => {}
                    }
                }

                driver.finish().map_err(string_zippy_error)
            })();

            running_flag.store(false, Ordering::SeqCst);
            let _ = descriptor_refresh_handle.join();
            result
        });

        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                running.store(false, Ordering::SeqCst);
                Ok(())
            }),
        ))
    }
}

fn active_segment_reader_from_descriptor(
    descriptor: &serde_json::Value,
    segment_schema: &CompiledSchema,
) -> zippy_core::Result<ActiveSegmentReader> {
    Ok(build_active_segment_reader(descriptor, segment_schema)?.0)
}

fn descriptor_segment_identity(descriptor: &serde_json::Value) -> zippy_core::Result<(u64, u64)> {
    let segment_id = descriptor
        .get("segment_id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing segment_id".to_string(),
        })?;
    let generation = descriptor
        .get("generation")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing generation".to_string(),
        })?;
    Ok((segment_id, generation))
}

fn build_active_segment_reader(
    descriptor: &serde_json::Value,
    segment_schema: &CompiledSchema,
) -> zippy_core::Result<(ActiveSegmentReader, LayoutPlan)> {
    let row_capacity = descriptor
        .get("row_capacity")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing row_capacity".to_string(),
        })?;
    let row_capacity = usize::try_from(row_capacity).map_err(|_| ZippyError::InvalidConfig {
        reason: "segment descriptor row_capacity overflows usize".to_string(),
    })?;
    let layout = LayoutPlan::for_schema(segment_schema, row_capacity).map_err(|error| {
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
    Ok((reader, layout))
}

fn json_zippy_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn segment_zippy_error(error: zippy_segment_store::ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn string_zippy_error(reason: String) -> ZippyError {
    ZippyError::Io { reason }
}

#[pyclass]
struct ReactiveStateEngine {
    name: String,
    id_column: String,
    id_filter: Option<Vec<String>>,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustReactiveStateEngine>,
    remote_source: Option<RemoteSourceConfig>,
    bus_source: Option<BusSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct ReactiveLatestEngine {
    name: String,
    by: Vec<String>,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustReactiveLatestEngine>,
    remote_source: Option<RemoteSourceConfig>,
    bus_source: Option<BusSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct TimeSeriesEngine {
    name: String,
    id_column: String,
    id_filter: Option<Vec<String>>,
    dt_column: String,
    window_ns: i64,
    late_data_policy: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustTimeSeriesEngine>,
    remote_source: Option<RemoteSourceConfig>,
    bus_source: Option<BusSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct CrossSectionalEngine {
    name: String,
    id_column: String,
    dt_column: String,
    trigger_interval_ns: i64,
    late_data_policy: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustCrossSectionalEngine>,
    remote_source: Option<RemoteSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct StreamTableMaterializer {
    name: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustStreamTableMaterializer>,
    remote_source: Option<RemoteSourceConfig>,
    bus_source: Option<BusSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct KeyValueTableMaterializer {
    name: String,
    by: Vec<String>,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustKeyValueTableMaterializer>,
    remote_source: Option<RemoteSourceConfig>,
    bus_source: Option<BusSourceConfig>,
    segment_source: Option<SegmentSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pymethods]
impl ReactiveStateEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, factors, target, *, id_filter=None, source=None, master=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        id_filter: Option<&Bound<'_, PyAny>>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let id_filter = parse_id_filter(id_filter)?;
        let factor_specs = build_reactive_specs(py, &schema, &id_column, factors)?;
        let engine = RustReactiveStateEngine::new_with_id_filter(
            &name,
            Arc::clone(&schema),
            factor_specs,
            &id_column,
            id_filter.clone(),
        )
        .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, bus_source, segment_source, python_source) =
            register_source(
                py,
                source,
                master,
                DownstreamLink {
                    handle: Arc::clone(&handle),
                    archive: Arc::clone(&archive),
                    write_input: parquet_sink
                        .as_ref()
                        .map(|config| config.write_input)
                        .unwrap_or(false),
                },
                schema.as_ref(),
                xfast,
            )?;

        Ok(Self {
            name,
            id_column,
            id_filter,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            bus_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            self.bus_source.as_ref(),
            self.segment_source.as_ref(),
            self.python_source.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "reactive",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            engine_has_source(
                &self._source_owner,
                &self.remote_source,
                &self.bus_source,
                &self.segment_source,
                &self.python_source,
            ),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        dict.set_item("id_filter", self.id_filter.clone())?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl ReactiveLatestEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema=None, by=None, target=None, *, source=None, master=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: Option<&Bound<'_, PyAny>>,
        by: Option<&Bound<'_, PyAny>>,
        target: Option<&Bound<'_, PyAny>>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
    ) -> PyResult<Self> {
        let by = by.ok_or_else(|| py_value_error("by is required"))?;
        let target = target.ok_or_else(|| py_value_error("target is required"))?;
        let by = parse_by_columns(by)?;
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));

        let named_source = source.and_then(|source| source.extract::<String>().ok());
        let (schema, resolved_segment_source) = if let Some(input_schema) = input_schema {
            (
                Arc::new(
                    Schema::from_pyarrow_bound(input_schema)
                        .map_err(|error| py_value_error(error.to_string()))?,
                ),
                None,
            )
        } else {
            let Some(stream_name) = named_source.as_deref() else {
                return Err(py_value_error(
                    "input_schema is required unless source is a stream name",
                ));
            };
            let (schema, segment_source) =
                segment_source_config_from_named_stream(py, stream_name, master, None, xfast)?;
            (schema, Some(segment_source))
        };

        let engine = RustReactiveLatestEngine::new(&name, Arc::clone(&schema), by.clone())
            .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let (source_owner, remote_source, bus_source, segment_source, python_source) =
            if let Some(segment_source) = resolved_segment_source {
                (None, None, None, Some(segment_source), None)
            } else if let Some(stream_name) = named_source.as_deref() {
                let (_, segment_source) = segment_source_config_from_named_stream(
                    py,
                    stream_name,
                    master,
                    Some(schema.as_ref()),
                    xfast,
                )?;
                (None, None, None, Some(segment_source), None)
            } else {
                register_source(
                    py,
                    source,
                    master,
                    DownstreamLink {
                        handle: Arc::clone(&handle),
                        archive: Arc::clone(&archive),
                        write_input: parquet_sink
                            .as_ref()
                            .map(|config| config.write_input)
                            .unwrap_or(false),
                    },
                    schema.as_ref(),
                    xfast,
                )?
            };

        Ok(Self {
            name,
            by,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            bus_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            self.bus_source.as_ref(),
            self.segment_source.as_ref(),
            self.python_source.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "reactive_latest",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            engine_has_source(
                &self._source_owner,
                &self.remote_source,
                &self.bus_source,
                &self.segment_source,
                &self.python_source,
            ),
        )?;
        dict.set_item("by", self.by.clone())?;
        if let Some(source) = self.segment_source.as_ref() {
            dict.set_item("source", &source.stream_name)?;
        }
        Ok(dict.into_any().unbind())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl StreamTableMaterializer {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, target, *, source=None, master=None, sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false, descriptor_publisher=None, row_capacity=None, retention_segments=None, retention_guard=None, dt_column=None, id_column=None, dt_part=None, persist_path=None, persist_publisher=None, descriptor_forwarding=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
        descriptor_publisher: Option<Py<PyAny>>,
        row_capacity: Option<usize>,
        retention_segments: Option<usize>,
        retention_guard: Option<Py<PyAny>>,
        dt_column: Option<String>,
        id_column: Option<String>,
        dt_part: Option<String>,
        persist_path: Option<String>,
        persist_publisher: Option<Py<PyAny>>,
        descriptor_forwarding: bool,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let mut engine = match row_capacity {
            Some(row_capacity) => RustStreamTableMaterializer::new_with_row_capacity(
                &name,
                Arc::clone(&schema),
                row_capacity,
            ),
            None => RustStreamTableMaterializer::new(&name, Arc::clone(&schema)),
        }
        .map_err(|error| py_value_error(error.to_string()))?;
        if let Some(retention_segments) = retention_segments {
            engine = engine.with_retention_segments(retention_segments);
        }
        if let Some(callback) = retention_guard {
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err("retention_guard must be callable"));
            }
            engine =
                engine.with_retention_guard(Arc::new(PyStreamTableRetentionGuard { callback }));
        }
        if let Some(callback) = descriptor_publisher {
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err(
                    "descriptor_publisher must be callable",
                ));
            }
            engine = engine
                .with_descriptor_publisher(Arc::new(PyStreamTableDescriptorPublisher { callback }));
        }
        if descriptor_forwarding {
            engine = engine.with_descriptor_forwarding(true);
        }
        let persist_path_provided = persist_path.is_some();
        if let Some(path) = persist_path {
            let mut config = StreamTablePersistConfig::new(PathBuf::from(path));
            if dt_column.is_some() || id_column.is_some() || dt_part.is_some() {
                let partition_spec =
                    StreamTablePersistPartitionSpec::new(dt_column, id_column, dt_part)
                        .map_err(|error| py_value_error(error.to_string()))?;
                config = config.with_partition_spec(partition_spec);
            }
            engine = engine.with_parquet_persist(config);
        }
        if let Some(callback) = persist_publisher {
            if !persist_path_provided {
                return Err(PyValueError::new_err(
                    "persist_path is required when persist_publisher is provided",
                ));
            }
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err("persist_publisher must be callable"));
            }
            engine =
                engine.with_persist_publisher(Arc::new(PyStreamTablePersistPublisher { callback }));
        }
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(sink).map_err(|error| {
            let message = error.to_string();
            if message.contains("parquet_sink must be zippy.ParquetSink") {
                PyTypeError::new_err("sink must be zippy.ParquetSink")
            } else {
                error
            }
        })?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, bus_source, segment_source, python_source) =
            register_source(
                py,
                source,
                master,
                DownstreamLink {
                    handle: Arc::clone(&handle),
                    archive: Arc::clone(&archive),
                    write_input: parquet_sink
                        .as_ref()
                        .map(|config| config.write_input)
                        .unwrap_or(false),
                },
                schema.as_ref(),
                xfast,
            )?;

        Ok(Self {
            name,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            bus_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            self.bus_source.as_ref(),
            self.segment_source.as_ref(),
            self.python_source.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "stream_table",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            engine_has_source(
                &self._source_owner,
                &self.remote_source,
                &self.bus_source,
                &self.segment_source,
                &self.python_source,
            ),
        )?;
        dict.del_item("parquet_sink")?;
        dict.set_item("sink", parquet_sink_to_pyobject(py, &self.parquet_sink)?)?;
        Ok(dict.into_any().unbind())
    }

    fn active_descriptor(&self, py: Python<'_>) -> PyResult<PyObject> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            py_runtime_error(
                "stream table active descriptor is unavailable after the engine has started",
            )
        })?;
        let envelope = engine
            .active_descriptor_envelope_bytes()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let envelope_text =
            std::str::from_utf8(&envelope).map_err(|error| py_value_error(error.to_string()))?;
        Ok(python_json_loads(py, envelope_text)?.into_py(py))
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl KeyValueTableMaterializer {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, by, target, *, source=None, master=None, sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false, descriptor_publisher=None, row_capacity=None, retention_guard=None, replacement_retention_snapshots=None))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        by: &Bound<'_, PyAny>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
        descriptor_publisher: Option<Py<PyAny>>,
        row_capacity: Option<usize>,
        retention_guard: Option<Py<PyAny>>,
        replacement_retention_snapshots: Option<usize>,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let by = parse_by_columns(by)?;
        let mut engine = match row_capacity {
            Some(row_capacity) => RustKeyValueTableMaterializer::new_with_row_capacity(
                &name,
                Arc::clone(&schema),
                by.clone(),
                row_capacity,
            ),
            None => RustKeyValueTableMaterializer::new(&name, Arc::clone(&schema), by.clone()),
        }
        .map_err(|error| py_value_error(error.to_string()))?;
        if let Some(callback) = descriptor_publisher {
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err(
                    "descriptor_publisher must be callable",
                ));
            }
            engine = engine
                .with_descriptor_publisher(Arc::new(PyStreamTableDescriptorPublisher { callback }));
        }
        if let Some(callback) = retention_guard {
            if !callback.bind(py).is_callable() {
                return Err(PyTypeError::new_err("retention_guard must be callable"));
            }
            engine =
                engine.with_retention_guard(Arc::new(PyStreamTableRetentionGuard { callback }));
        }
        if let Some(snapshots) = replacement_retention_snapshots {
            engine = engine
                .with_replacement_retention_snapshots(snapshots)
                .map_err(|error| py_value_error(error.to_string()))?;
        }
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(sink).map_err(|error| {
            let message = error.to_string();
            if message.contains("parquet_sink must be zippy.ParquetSink") {
                PyTypeError::new_err("sink must be zippy.ParquetSink")
            } else {
                error
            }
        })?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, bus_source, segment_source, python_source) =
            register_source(
                py,
                source,
                master,
                DownstreamLink {
                    handle: Arc::clone(&handle),
                    archive: Arc::clone(&archive),
                    write_input: parquet_sink
                        .as_ref()
                        .map(|config| config.write_input)
                        .unwrap_or(false),
                },
                schema.as_ref(),
                xfast,
            )?;

        Ok(Self {
            name,
            by,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            bus_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            self.bus_source.as_ref(),
            self.segment_source.as_ref(),
            self.python_source.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "key_value_table",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            engine_has_source(
                &self._source_owner,
                &self.remote_source,
                &self.bus_source,
                &self.segment_source,
                &self.python_source,
            ),
        )?;
        dict.del_item("parquet_sink")?;
        dict.set_item("sink", parquet_sink_to_pyobject(py, &self.parquet_sink)?)?;
        dict.set_item("by", self.by.clone())?;
        Ok(dict.into_any().unbind())
    }

    fn active_descriptor(&self, py: Python<'_>) -> PyResult<PyObject> {
        let engine = self.engine.as_ref().ok_or_else(|| {
            py_runtime_error(
                "key-value table active descriptor is unavailable after the engine has started",
            )
        })?;
        let envelope = engine
            .active_descriptor_envelope_bytes()
            .map_err(|error| py_runtime_error(error.to_string()))?;
        let envelope_text =
            std::str::from_utf8(&envelope).map_err(|error| py_value_error(error.to_string()))?;
        Ok(python_json_loads(py, envelope_text)?.into_py(py))
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl TimeSeriesEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, dt_column, late_data_policy, factors, target, *, window=None, window_type=None, window_ns=None, pre_factors=None, post_factors=None, id_filter=None, source=None, master=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        dt_column: String,
        late_data_policy: &Bound<'_, PyAny>,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        window: Option<&Bound<'_, PyAny>>,
        window_type: Option<&Bound<'_, PyAny>>,
        window_ns: Option<i64>,
        pre_factors: Option<Vec<Py<PyAny>>>,
        post_factors: Option<Vec<Py<PyAny>>>,
        id_filter: Option<&Bound<'_, PyAny>>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = build_aggregation_specs(py, factors)?;
        let pre_factor_specs =
            build_expression_specs(py, pre_factors.unwrap_or_default(), "pre_factors")?;
        let post_factor_specs =
            build_expression_specs(py, post_factors.unwrap_or_default(), "post_factors")?;
        let id_filter = parse_id_filter(id_filter)?;
        let late_data_policy_value = parse_required_policy_value(
            late_data_policy,
            "late_data_policy",
            "late_data_policy",
            "LateDataPolicy",
        )?;
        let late_data_policy_enum = parse_late_data_policy(&late_data_policy_value)?;
        let window_ns = parse_window_ns(window, window_type, window_ns)?;
        let engine = RustTimeSeriesEngine::new_with_id_filter(
            &name,
            Arc::clone(&schema),
            &id_column,
            &dt_column,
            window_ns,
            late_data_policy_enum,
            factor_specs,
            pre_factor_specs,
            post_factor_specs,
            id_filter.clone(),
        )
        .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, bus_source, segment_source, python_source) =
            register_source(
                py,
                source,
                master,
                DownstreamLink {
                    handle: Arc::clone(&handle),
                    archive: Arc::clone(&archive),
                    write_input: parquet_sink
                        .as_ref()
                        .map(|config| config.write_input)
                        .unwrap_or(false),
                },
                schema.as_ref(),
                xfast,
            )?;

        Ok(Self {
            name,
            id_column,
            id_filter,
            dt_column,
            window_ns,
            late_data_policy: late_data_policy_value,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            bus_source,
            segment_source,
            python_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            self.bus_source.as_ref(),
            self.segment_source.as_ref(),
            self.python_source.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "timeseries",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            engine_has_source(
                &self._source_owner,
                &self.remote_source,
                &self.bus_source,
                &self.segment_source,
                &self.python_source,
            ),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        dict.set_item("id_filter", self.id_filter.clone())?;
        dict.set_item("dt_column", &self.dt_column)?;
        dict.set_item("window_ns", self.window_ns)?;
        dict.set_item("late_data_policy", &self.late_data_policy)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl CrossSectionalEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, dt_column, trigger_interval, late_data_policy, factors, target, *, source=None, master=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        dt_column: String,
        trigger_interval: &Bound<'_, PyAny>,
        late_data_policy: &Bound<'_, PyAny>,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        master: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
        xfast: bool,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = build_cross_sectional_specs(py, factors)?;
        let late_data_policy_value = parse_required_policy_value(
            late_data_policy,
            "late_data_policy",
            "late_data_policy",
            "LateDataPolicy",
        )?;
        if late_data_policy_value != "reject" {
            return Err(py_value_error(
                "cross-sectional engine only supports late_data_policy=zippy.LateDataPolicy.REJECT",
            ));
        }
        let late_data_policy_enum = parse_late_data_policy(&late_data_policy_value)?;
        let trigger_interval_ns = parse_duration_ns(trigger_interval, "trigger_interval")?;
        let engine = RustCrossSectionalEngine::new(
            &name,
            Arc::clone(&schema),
            &id_column,
            &dt_column,
            trigger_interval_ns,
            late_data_policy_enum,
            factor_specs,
        )
        .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options = parse_runtime_options(
            buffer_capacity,
            overflow_policy,
            archive_buffer_capacity,
            xfast,
        )?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, segment_source) = register_timeseries_source(
            py,
            source,
            master,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink
                    .as_ref()
                    .map(|config| config.write_input)
                    .unwrap_or(false),
            },
            schema.as_ref(),
            xfast,
        )?;

        Ok(Self {
            name,
            id_column,
            dt_column,
            trigger_interval_ns,
            late_data_policy: late_data_policy_value,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            runtime_options,
            status,
            metrics,
            archive,
            handle,
            engine: Some(engine),
            remote_source,
            segment_source,
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.runtime_options,
            &self.target,
            self.parquet_sink.as_ref(),
            self.remote_source.as_ref(),
            None,
            self.segment_source.as_ref(),
            None,
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        *self.archive.lock().unwrap() = archive;
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        let result = write_runtime_input(
            py,
            &self.handle,
            &self.archive,
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        );
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn status(&self) -> String {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        self.status.lock().unwrap().as_str().to_string()
    }

    fn metrics(&self, py: Python<'_>) -> PyResult<PyObject> {
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        metrics_snapshot_to_pydict(py, *self.metrics.lock().unwrap())
    }

    fn config(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = engine_base_config_dict(
            py,
            "cross_sectional",
            &self.name,
            &self.target,
            &self.parquet_sink,
            &self.runtime_options,
            self._source_owner.is_some()
                || self.remote_source.is_some()
                || self.segment_source.is_some(),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        dict.set_item("dt_column", &self.dt_column)?;
        dict.set_item("trigger_interval_ns", self.trigger_interval_ns)?;
        dict.set_item("late_data_policy", &self.late_data_policy)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let result =
            flush_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymodule]
fn _internal(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", python_dev_version())?;
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(run_master_daemon, module)?)?;
    module.add_function(wrap_pyfunction!(setup_log, module)?)?;
    module.add_function(wrap_pyfunction!(log_info, module)?)?;
    module.add_class::<TsEmaSpec>()?;
    module.add_class::<TsReturnSpec>()?;
    module.add_class::<TsMeanSpec>()?;
    module.add_class::<TsStdSpec>()?;
    module.add_class::<TsDelaySpec>()?;
    module.add_class::<TsDiffSpec>()?;
    module.add_class::<AbsSpec>()?;
    module.add_class::<LogSpec>()?;
    module.add_class::<ClipSpec>()?;
    module.add_class::<CastSpec>()?;
    module.add_class::<ExpressionFactor>()?;
    module.add_class::<AggFirstSpec>()?;
    module.add_class::<AggLastSpec>()?;
    module.add_class::<AggSumSpec>()?;
    module.add_class::<AggMaxSpec>()?;
    module.add_class::<AggMinSpec>()?;
    module.add_class::<AggCountSpec>()?;
    module.add_class::<AggVwapSpec>()?;
    module.add_class::<CSRankSpec>()?;
    module.add_class::<CSZscoreSpec>()?;
    module.add_class::<CSDemeanSpec>()?;
    module.add_class::<NullPublisher>()?;
    module.add_class::<ParquetSink>()?;
    module.add_class::<ZmqPublisher>()?;
    module.add_class::<ZmqStreamPublisher>()?;
    module.add_class::<ZmqSubscriber>()?;
    module.add_class::<ZmqSource>()?;
    module.add_class::<MasterDaemon>()?;
    module.add_class::<MasterClient>()?;
    module.add_class::<Query>()?;
    module.add_class::<StreamSubscriber>()?;
    module.add_class::<BusWriter>()?;
    module.add_class::<BusReader>()?;
    module.add_class::<BusStreamTarget>()?;
    module.add_class::<BusStreamSource>()?;
    module.add_class::<SegmentStreamSource>()?;
    module.add_class::<SegmentTestWriter>()?;
    module.add_class::<ReactiveStateEngine>()?;
    module.add_class::<ReactiveLatestEngine>()?;
    module.add_class::<StreamTableMaterializer>()?;
    module.add_class::<KeyValueTableMaterializer>()?;
    module.add_class::<TimeSeriesEngine>()?;
    module.add_class::<CrossSectionalEngine>()?;
    Ok(())
}

#[pyfunction]
fn version() -> String {
    python_dev_version()
}

#[pyfunction]
#[pyo3(signature = (uri=None, *, control_endpoint=None, config=None))]
fn run_master_daemon(
    py: Python<'_>,
    uri: Option<String>,
    control_endpoint: Option<String>,
    config: Option<String>,
) -> PyResult<()> {
    let control_endpoint = resolve_uri_argument(uri, control_endpoint)?;
    let control_endpoint = prepare_control_endpoint(&control_endpoint)?;
    let mut daemon_config = MasterDaemonConfig::new(control_endpoint);
    if let Some(config) = config {
        daemon_config = daemon_config.with_config_path(PathBuf::from(config));
    }
    py.allow_threads(move || run_rust_master_daemon(daemon_config))
        .map_err(|error| py_runtime_error(error.to_string()))
}

#[pyfunction]
#[pyo3(signature = (app, level="info", log_dir="logs", to_console=true, to_file=true))]
fn setup_log(
    py: Python<'_>,
    app: String,
    level: &str,
    log_dir: &str,
    to_console: bool,
    to_file: bool,
) -> PyResult<PyObject> {
    let snapshot = setup_core_log(LogConfig::new(
        app,
        level,
        PathBuf::from(log_dir),
        to_console,
        to_file,
    ))
    .map_err(|error| py_runtime_error(error.to_string()))?;

    let active_snapshot = current_log_snapshot()
        .ok_or_else(|| py_runtime_error("logging setup completed without active snapshot"))?;

    let dict = PyDict::new_bound(py);
    dict.set_item("app", active_snapshot.app)?;
    dict.set_item("level", active_snapshot.level)?;
    dict.set_item("run_id", active_snapshot.run_id)?;
    dict.set_item(
        "file_path",
        snapshot
            .file_path
            .map(|path| path.to_string_lossy().to_string()),
    )?;
    Ok(dict.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (component, event, message, status=None))]
fn log_info(component: String, event: String, message: String, status: Option<String>) {
    if let Some(status) = status {
        info!(
            component = component.as_str(),
            event = event.as_str(),
            status = status.as_str(),
            message = message.as_str(),
            "{message}"
        );
    } else {
        info!(
            component = component.as_str(),
            event = event.as_str(),
            message = message.as_str(),
            "{message}"
        );
    }
}

fn parse_targets(target: &Bound<'_, PyAny>) -> PyResult<Vec<TargetConfig>> {
    if let Ok(targets) = target.downcast::<PyList>() {
        if targets.is_empty() {
            return Err(PyTypeError::new_err("target list must not be empty"));
        }

        return targets
            .iter()
            .map(|item| parse_single_target(&item))
            .collect();
    }

    Ok(vec![parse_single_target(target)?])
}

fn parse_id_filter(id_filter: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<String>>> {
    let Some(id_filter) = id_filter else {
        return Ok(None);
    };

    let values = id_filter
        .extract::<Vec<String>>()
        .map_err(|_| py_value_error("id_filter must be a sequence of strings"))?;

    if values.is_empty() {
        return Err(py_value_error("id_filter must not be empty"));
    }

    Ok(Some(values))
}

fn parse_by_columns(by: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(value) = by.extract::<String>() {
        if value.is_empty() {
            return Err(py_value_error("by must not contain empty column names"));
        }
        return Ok(vec![value]);
    }

    let values = by
        .extract::<Vec<String>>()
        .map_err(|_| py_value_error("by must be a string or a sequence of strings"))?;
    if values.is_empty() {
        return Err(py_value_error("by must not be empty"));
    }
    if values.iter().any(|value| value.is_empty()) {
        return Err(py_value_error("by must not contain empty column names"));
    }
    Ok(values)
}

fn parse_instrument_ids(
    instrument_ids: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Vec<String>>> {
    let Some(instrument_ids) = instrument_ids else {
        return Ok(None);
    };

    if instrument_ids.is_instance_of::<PyList>() || instrument_ids.is_instance_of::<PyTuple>() {
        let values = instrument_ids.extract::<Vec<String>>().map_err(|_| {
            py_value_error("instrument_ids must be a string or a sequence of strings")
        })?;
        if values.is_empty() {
            return Ok(None);
        }
        if values.iter().any(|value| value.is_empty()) {
            return Err(py_value_error("instrument filter contains empty value"));
        }
        return Ok(Some(values));
    }

    let value = instrument_ids
        .extract::<String>()
        .map_err(|_| py_value_error("instrument_ids must be a string or a sequence of strings"))?;
    if value.is_empty() {
        return Err(py_value_error("instrument filter contains empty value"));
    }
    Ok(Some(vec![value]))
}

fn attach_bus_reader_with<F, G>(
    client: &SharedMasterClient,
    stream_name: &str,
    instrument_ids: Option<Vec<String>>,
    xfast: bool,
    read_from: F,
    read_from_filtered: G,
) -> zippy_core::Result<CoreBusReader>
where
    F: FnOnce(&mut CoreMasterClient, &str, bool) -> zippy_core::Result<CoreBusReader>,
    G: FnOnce(&mut CoreMasterClient, &str, Vec<String>, bool) -> zippy_core::Result<CoreBusReader>,
{
    let mut guard = client.lock().unwrap();
    match instrument_ids {
        None => read_from(&mut guard, stream_name, xfast),
        Some(instrument_ids) => read_from_filtered(&mut guard, stream_name, instrument_ids, xfast),
    }
}

fn parse_single_target(target: &Bound<'_, PyAny>) -> PyResult<TargetConfig> {
    if target.extract::<PyRef<'_, NullPublisher>>().is_ok() {
        return Ok(TargetConfig::Null);
    }

    if let Ok(publisher) = target.extract::<PyRef<'_, ZmqPublisher>>() {
        return Ok(TargetConfig::Zmq {
            endpoint: publisher.endpoint.clone(),
        });
    }

    if let Ok(publisher) = target.extract::<PyRef<'_, ZmqStreamPublisher>>() {
        return Ok(TargetConfig::ZmqStream {
            endpoint: publisher.last_endpoint.clone(),
            stream_name: publisher.stream_name.clone(),
            publisher: publisher.publisher.clone(),
        });
    }

    if let Ok(target) = target.extract::<PyRef<'_, BusStreamTarget>>() {
        return Ok(TargetConfig::BusStream {
            stream_name: target.stream_name.clone(),
            master: Arc::clone(&target.master),
        });
    }

    Err(PyTypeError::new_err(
        "target must be NullPublisher, ZmqPublisher, ZmqStreamPublisher, BusStreamTarget, or a non-empty list of them",
    ))
}

fn parse_parquet_sink(
    parquet_sink: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<ParquetSinkConfig>> {
    let Some(parquet_sink) = parquet_sink else {
        return Ok(None);
    };

    let sink = parquet_sink
        .extract::<PyRef<'_, ParquetSink>>()
        .map_err(|_| PyTypeError::new_err("parquet_sink must be zippy.ParquetSink"))?;
    let rotation = ParquetRotation::parse(&sink.rotation)?;

    Ok(Some(ParquetSinkConfig {
        path: PathBuf::from(&sink.path),
        rotation,
        write_input: sink.write_input,
        write_output: sink.write_output,
        rows_per_batch: sink.rows_per_batch,
        flush_interval_ms: sink.flush_interval_ms,
    }))
}

fn segment_source_config_from_named_stream(
    py: Python<'_>,
    stream_name: &str,
    master: Option<&Bound<'_, PyAny>>,
    expected_schema: Option<&Schema>,
    xfast: bool,
) -> PyResult<(Arc<Schema>, SegmentSourceConfig)> {
    if stream_name.is_empty() {
        return Err(py_value_error("source stream name must not be empty"));
    }

    let master = master
        .ok_or_else(|| py_value_error("master is required when source is a stream name"))?
        .extract::<PyRef<'_, MasterClient>>()
        .map_err(|_| py_value_error("master must be zippy.MasterClient"))?;
    let shared_master = Arc::clone(&master.client);
    let stream = py
        .allow_threads(|| shared_master.lock().unwrap().get_stream(stream_name))
        .map_err(|error| py_runtime_error(error.to_string()))?;
    if stream.data_path != "segment" {
        return Err(py_value_error(format!(
            "source stream must use segment data path data_path=[{}]",
            stream.data_path
        )));
    }

    let stream_schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
    let schema = if let Some(expected_schema) = expected_schema {
        if stream_schema.as_ref() != expected_schema {
            return Err(py_value_error(
                "source stream schema must match downstream input_schema",
            ));
        }
        Arc::new(expected_schema.clone())
    } else {
        Arc::clone(&stream_schema)
    };
    let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;

    Ok((
        Arc::clone(&schema),
        SegmentSourceConfig {
            stream_name: stream_name.to_string(),
            expected_schema: schema,
            segment_schema,
            master: shared_master,
            mode: RustSourceMode::Pipeline,
            start_at_tail: true,
            xfast,
        },
    ))
}

fn register_source(
    py: Python<'_>,
    source: Option<&Bound<'_, PyAny>>,
    master: Option<&Bound<'_, PyAny>>,
    downstream: DownstreamLink,
    input_schema: &Schema,
    xfast: bool,
) -> PyResult<RegisteredSource> {
    let Some(source) = source else {
        return Ok((None, None, None, None, None));
    };

    if let Ok(stream_name) = source.extract::<String>() {
        let (_, segment_source) = segment_source_config_from_named_stream(
            py,
            &stream_name,
            master,
            Some(input_schema),
            xfast,
        )?;
        return Ok((None, None, None, Some(segment_source), None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, ReactiveStateEngine>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream.clone());
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, ReactiveLatestEngine>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream.clone());
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, StreamTableMaterializer>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream.clone());
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, KeyValueTableMaterializer>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream.clone());
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, TimeSeriesEngine>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream);
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, CrossSectionalEngine>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream);
        return Ok((Some(source.clone().unbind()), None, None, None, None));
    }

    if let Ok(remote_source) = source.extract::<PyRef<'_, ZmqSource>>() {
        if remote_source.expected_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }

        return Ok((
            Some(source.clone().unbind()),
            Some(RemoteSourceConfig {
                endpoint: remote_source.endpoint.clone(),
                expected_schema: remote_source.expected_schema.clone(),
                mode: remote_source.mode,
            }),
            None,
            None,
            None,
        ));
    }

    if let Ok(bus_source) = source.extract::<PyRef<'_, BusStreamSource>>() {
        if bus_source.expected_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }

        return Ok((
            Some(source.clone().unbind()),
            None,
            Some(BusSourceConfig {
                stream_name: bus_source.stream_name.clone(),
                expected_schema: bus_source.expected_schema.clone(),
                master: Arc::clone(&bus_source.master),
                mode: bus_source.mode,
                xfast: bus_source.xfast,
            }),
            None,
            None,
        ));
    }

    if let Ok(segment_source) = source.extract::<PyRef<'_, SegmentStreamSource>>() {
        if segment_source.expected_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }

        return Ok((
            Some(source.clone().unbind()),
            None,
            None,
            Some(SegmentSourceConfig {
                stream_name: segment_source.stream_name.clone(),
                expected_schema: segment_source.expected_schema.clone(),
                segment_schema: segment_source.segment_schema.clone(),
                master: Arc::clone(&segment_source.master),
                mode: segment_source.mode,
                start_at_tail: false,
                xfast: segment_source.xfast,
            }),
            None,
        ));
    }

    if source.hasattr("_zippy_start")? && source.hasattr("_zippy_output_schema")? {
        let output_schema = Arc::new(
            Schema::from_pyarrow_bound(&source.call_method0("_zippy_output_schema")?)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        if output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }

        let mode = if source.hasattr("_zippy_source_mode")? {
            parse_python_source_mode(&source.call_method0("_zippy_source_mode")?)?
        } else {
            RustSourceMode::Pipeline
        };
        let name = if source.hasattr("_zippy_source_name")? {
            source
                .call_method0("_zippy_source_name")?
                .extract::<String>()?
        } else {
            "python-source".to_string()
        };

        return Ok((
            Some(source.clone().unbind()),
            None,
            None,
            None,
            Some(PythonSourceConfig {
                owner: source.clone().unbind(),
                name,
                output_schema,
                mode,
            }),
        ));
    }

    Err(PyTypeError::new_err(
        "source must be ReactiveStateEngine, ReactiveLatestEngine, StreamTableMaterializer, KeyValueTableMaterializer, TimeSeriesEngine, CrossSectionalEngine, ZmqSource, BusStreamSource, SegmentStreamSource, or a Python source plugin",
    ))
}

fn register_timeseries_source(
    py: Python<'_>,
    source: Option<&Bound<'_, PyAny>>,
    master: Option<&Bound<'_, PyAny>>,
    downstream: DownstreamLink,
    input_schema: &Schema,
    xfast: bool,
) -> PyResult<(
    SourceOwner,
    Option<RemoteSourceConfig>,
    Option<SegmentSourceConfig>,
)> {
    let Some(source) = source else {
        return Ok((None, None, None));
    };

    if let Ok(stream_name) = source.extract::<String>() {
        let (_, segment_source) = segment_source_config_from_named_stream(
            py,
            &stream_name,
            master,
            Some(input_schema),
            xfast,
        )?;
        return Ok((None, None, Some(segment_source)));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, TimeSeriesEngine>>() {
        if engine.engine.is_none() {
            return Err(py_runtime_error(
                "source engine must be linked before it is started",
            ));
        }
        if engine.output_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }
        engine.downstreams.push(downstream);
        return Ok((Some(source.clone().unbind()), None, None));
    }

    if let Ok(remote_source) = source.extract::<PyRef<'_, ZmqSource>>() {
        if remote_source.expected_schema.as_ref() != input_schema {
            return Err(py_value_error(
                "source output schema must match downstream input_schema",
            ));
        }

        return Ok((
            Some(source.clone().unbind()),
            Some(RemoteSourceConfig {
                endpoint: remote_source.endpoint.clone(),
                expected_schema: remote_source.expected_schema.clone(),
                mode: remote_source.mode,
            }),
            None,
        ));
    }

    Err(PyTypeError::new_err(
        "source must be a named segment stream, TimeSeriesEngine, or ZmqSource for CrossSectionalEngine",
    ))
}

fn build_publisher(
    targets: &[TargetConfig],
    parquet_sink: Option<&ParquetSinkConfig>,
    archive: Option<ArchiveHandle>,
    downstreams: &[DownstreamLink],
) -> PyResult<Box<dyn CorePublisher>> {
    let mut publishers =
        Vec::<Box<dyn CorePublisher>>::with_capacity(targets.len() + downstreams.len() + 1);

    for target in targets {
        match target {
            TargetConfig::Null => publishers.push(Box::new(RustNullPublisher::default())),
            TargetConfig::Zmq { endpoint } => {
                let publisher = RustZmqPublisher::bind(endpoint)
                    .map_err(|error| py_runtime_error(error.to_string()))?;
                publishers.push(Box::new(publisher));
            }
            TargetConfig::ZmqStream { publisher, .. } => {
                let publisher = publisher.lock().unwrap().take().ok_or_else(|| {
                    py_runtime_error(
                        "zmq stream publisher target is closed or already owned by an engine",
                    )
                })?;
                publishers.push(Box::new(publisher));
            }
            TargetConfig::BusStream {
                stream_name,
                master,
            } => {
                let writer = master
                    .lock()
                    .unwrap()
                    .write_to(stream_name)
                    .map_err(|error| py_runtime_error(error.to_string()))?;
                publishers.push(Box::new(BusTargetPublisher { writer }));
            }
        }
    }

    if parquet_sink
        .map(|config| config.write_output)
        .unwrap_or(false)
    {
        let archive = archive.ok_or_else(|| {
            py_runtime_error("parquet sink archive must be started before publisher setup")
        })?;
        publishers.push(Box::new(ParquetOutputPublisher { archive }));
    }

    for downstream in downstreams {
        publishers.push(Box::new(InProcessPublisher {
            downstream: downstream.clone(),
        }));
    }

    if publishers.len() == 1 {
        return Ok(publishers.pop().expect("single publisher checked above"));
    }

    Ok(Box::new(RustFanoutPublisher::new(publishers)))
}

fn ensure_downstreams_running(downstreams: &[DownstreamLink]) -> PyResult<()> {
    for downstream in downstreams {
        let guard = downstream.handle.lock().unwrap();
        let runtime = guard.as_ref().ok_or_else(|| {
            py_runtime_error("downstream engine must be started before source engine writes")
        })?;

        if runtime.status() != EngineStatus::Running {
            return Err(py_runtime_error(
                "downstream engine must be started before source engine writes",
            ));
        }
    }

    Ok(())
}

fn ensure_source_stopped(py: Python<'_>, source_owner: &Option<Py<PyAny>>) -> PyResult<()> {
    let Some(source_owner) = source_owner else {
        return Ok(());
    };
    let source = source_owner.bind(py);

    if let Ok(engine) = source.extract::<PyRef<'_, ReactiveStateEngine>>() {
        return ensure_runtime_is_not_running(&engine.handle);
    }

    if let Ok(engine) = source.extract::<PyRef<'_, ReactiveLatestEngine>>() {
        return ensure_runtime_is_not_running(&engine.handle);
    }

    if let Ok(engine) = source.extract::<PyRef<'_, StreamTableMaterializer>>() {
        return ensure_runtime_is_not_running(&engine.handle);
    }

    if let Ok(engine) = source.extract::<PyRef<'_, TimeSeriesEngine>>() {
        return ensure_runtime_is_not_running(&engine.handle);
    }

    Ok(())
}

fn ensure_runtime_is_not_running(handle: &SharedHandle) -> PyResult<()> {
    let guard = handle.lock().unwrap();

    if let Some(runtime) = guard.as_ref() {
        if runtime.status() == EngineStatus::Running {
            return Err(py_runtime_error(
                "source engine must be stopped before downstream engine stops",
            ));
        }
    }

    Ok(())
}

fn start_prepared_source_runtime<S, E>(
    source: Box<S>,
    config: EngineConfig,
    publisher: Box<dyn CorePublisher>,
    engine: &mut Option<E>,
) -> zippy_core::Result<EngineHandle>
where
    S: Source + ?Sized,
    E: Engine,
{
    let prepared = zippy_core::runtime::prepare_source_runtime(source, config)?;
    let engine = engine
        .take()
        .expect("engine must be available after pre-start validation");
    prepared.spawn_with_publisher(engine, publisher)
}

#[allow(clippy::too_many_arguments)]
fn start_runtime_engine<E: Engine>(
    name: &str,
    runtime_options: &RuntimeOptions,
    targets: &[TargetConfig],
    parquet_sink: Option<&ParquetSinkConfig>,
    remote_source: Option<&RemoteSourceConfig>,
    bus_source: Option<&BusSourceConfig>,
    segment_source: Option<&SegmentSourceConfig>,
    python_source: Option<&PythonSourceConfig>,
    downstreams: &[DownstreamLink],
    engine: &mut Option<E>,
) -> PyResult<(EngineHandle, Option<ArchiveHandle>)> {
    let archive = parquet_sink
        .cloned()
        .map(|config| ArchiveHandle::spawn(config, runtime_options.archive_buffer_capacity));
    let publisher = match build_publisher(targets, parquet_sink, archive.clone(), downstreams) {
        Ok(publisher) => publisher,
        Err(error) => {
            if let Some(archive) = archive {
                let _ = archive.close();
            }
            return Err(error);
        }
    };
    let config = EngineConfig {
        name: name.to_string(),
        buffer_capacity: runtime_options.buffer_capacity,
        overflow_policy: runtime_options.overflow_policy,
        late_data_policy: Default::default(),
        xfast: runtime_options.xfast,
    };
    config
        .validate()
        .map_err(|error| py_runtime_error(error.to_string()))?;
    if engine.is_none() {
        return Err(py_runtime_error("engine already started"));
    }

    let handle = match (remote_source, bus_source, segment_source, python_source) {
        (Some(remote_source), None, None, None) => {
            let source = Box::new(
                RustZmqSource::connect(
                    &format!("{name}_source"),
                    &remote_source.endpoint,
                    remote_source.expected_schema.clone(),
                    remote_source.mode,
                )
                .map_err(|error| py_runtime_error(error.to_string()))?,
            );
            start_prepared_source_runtime(source, config, publisher, engine)
        }
        (None, Some(bus_source), None, None) => {
            let source = Box::new(BusSourceBridge {
                stream_name: bus_source.stream_name.clone(),
                expected_schema: Arc::clone(&bus_source.expected_schema),
                mode: bus_source.mode,
                master: Arc::clone(&bus_source.master),
                xfast: bus_source.xfast,
            });
            start_prepared_source_runtime(source, config, publisher, engine)
        }
        (None, None, Some(segment_source), None) => {
            let source = Box::new(SegmentSourceBridge {
                stream_name: segment_source.stream_name.clone(),
                expected_schema: Arc::clone(&segment_source.expected_schema),
                segment_schema: segment_source.segment_schema.clone(),
                mode: segment_source.mode,
                master: Arc::clone(&segment_source.master),
                start_at_tail: segment_source.start_at_tail,
                xfast: segment_source.xfast,
            });
            start_prepared_source_runtime(source, config, publisher, engine)
        }
        (None, None, None, Some(python_source)) => {
            let source = Box::new(PythonSourceBridge {
                owner: Python::with_gil(|py| python_source.owner.clone_ref(py)),
                name: python_source.name.clone(),
                output_schema: Arc::clone(&python_source.output_schema),
                mode: python_source.mode,
            });
            start_prepared_source_runtime(source, config, publisher, engine)
        }
        (None, None, None, None) => {
            let engine = engine
                .take()
                .expect("engine must be available after pre-start validation");
            spawn_engine_with_publisher(engine, config, publisher)
        }
        _ => {
            return Err(py_runtime_error(
                "engine cannot use more than one external source at the same time",
            ));
        }
    };
    let handle = match handle {
        Ok(handle) => handle,
        Err(error) => {
            error!(
                component = "python_bridge",
                engine = name,
                event = "start_failure",
                error = %error,
                "python runtime bridge start failed"
            );
            return Err(py_runtime_error(error.to_string()));
        }
    };

    info!(
        component = "python_bridge",
        engine = name,
        event = "start",
        "python runtime bridge started"
    );

    Ok((handle, archive))
}

fn write_runtime_input(
    py: Python<'_>,
    handle: &SharedHandle,
    archive: &SharedArchive,
    parquet_sink: Option<&ParquetSinkConfig>,
    value: &Bound<'_, PyAny>,
    input_schema: &Schema,
) -> PyResult<()> {
    let batches = value_to_record_batches(py, value, input_schema)?;

    for batch in batches {
        if parquet_sink
            .map(|config| config.write_input)
            .unwrap_or(false)
        {
            archive
                .lock()
                .unwrap()
                .as_ref()
                .cloned()
                .ok_or_else(|| py_runtime_error("parquet sink archive is not available"))?
                .write(ArchiveKind::Input, batch.clone())
                .map_err(|error| py_runtime_error(error.to_string()))?;
        }
        with_handle(handle, |runtime| {
            runtime
                .write(batch)
                .map_err(|error| py_runtime_error(error.to_string()))
        })?;
    }

    Ok(())
}

fn flush_runtime_engine(
    py: Python<'_>,
    handle: &SharedHandle,
    archive: &SharedArchive,
    status: &SharedStatus,
    metrics: &SharedMetrics,
) -> PyResult<()> {
    with_handle(handle, |runtime| {
        py.allow_threads(|| runtime.flush())
            .map(|_| ())
            .map_err(|error| py_runtime_error(error.to_string()))
    })?;
    if let Some(archive) = archive.lock().unwrap().as_ref().cloned() {
        py.allow_threads(|| archive.flush())
            .map_err(|error| py_runtime_error(error.to_string()))?;
    }
    sync_runtime_state(handle, status, metrics);
    Ok(())
}

fn stop_runtime_engine(
    py: Python<'_>,
    handle: &SharedHandle,
    archive: &SharedArchive,
    status: &SharedStatus,
    metrics: &SharedMetrics,
) -> PyResult<()> {
    let mut guard = handle.lock().unwrap();
    let mut runtime = match guard.take() {
        Some(handle) => handle,
        None => return Err(py_runtime_error("engine not started")),
    };

    let stop_result = match py.allow_threads(|| runtime.stop()) {
        Ok(()) => Ok(()),
        Err(_error) if runtime.status() == EngineStatus::Stopped => Ok(()),
        Err(error) => Err(py_runtime_error(error.to_string())),
    };
    let archive_result = if let Some(archive) = archive.lock().unwrap().take() {
        py.allow_threads(|| archive.close())
            .map_err(|error| py_runtime_error(error.to_string()))
    } else {
        Ok(())
    };
    let mut final_status = runtime.status();
    if archive_result.is_err() {
        final_status = EngineStatus::Failed;
    }
    set_cached_runtime_state(status, metrics, final_status, runtime.metrics());

    if let Err(error) = &stop_result {
        error!(
            component = "python_bridge",
            event = "stop_failure",
            error = %error,
            "python runtime bridge stop failed"
        );
    } else if let Err(error) = &archive_result {
        error!(
            component = "python_bridge",
            event = "stop_failure",
            error = %error,
            "python runtime bridge stop failed"
        );
    } else {
        info!(
            component = "python_bridge",
            event = "stop",
            status = final_status.as_str(),
            "python runtime bridge stopped"
        );
    }

    match (stop_result, archive_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn with_handle<T>(
    handle: &SharedHandle,
    callback: impl FnOnce(&EngineHandle) -> PyResult<T>,
) -> PyResult<T> {
    let guard = handle.lock().unwrap();
    let runtime = guard
        .as_ref()
        .ok_or_else(|| py_runtime_error("engine not started"))?;
    callback(runtime)
}

fn sync_runtime_state(handle: &SharedHandle, status: &SharedStatus, metrics: &SharedMetrics) {
    let guard = handle.lock().unwrap();
    if let Some(runtime) = guard.as_ref() {
        set_cached_runtime_state(status, metrics, runtime.status(), runtime.metrics());
    }
}

fn set_cached_runtime_state(
    status: &SharedStatus,
    metrics: &SharedMetrics,
    engine_status: EngineStatus,
    snapshot: EngineMetricsSnapshot,
) {
    *status.lock().unwrap() = engine_status;
    *metrics.lock().unwrap() = snapshot;
}

fn parse_late_data_policy(value: &str) -> PyResult<LateDataPolicy> {
    match value {
        "reject" => Ok(LateDataPolicy::Reject),
        "drop_with_metric" => Ok(LateDataPolicy::DropWithMetric),
        _ => Err(py_value_error(
            "late_data_policy must be 'reject' or 'drop_with_metric'",
        )),
    }
}

fn parse_source_mode(value: &Bound<'_, PyAny>) -> PyResult<RustSourceMode> {
    let value = parse_required_policy_value(value, "mode", "source_mode", "SourceMode")?;
    match value.as_str() {
        "pipeline" => Ok(RustSourceMode::Pipeline),
        "consumer" => Ok(RustSourceMode::Consumer),
        _ => Err(py_value_error("mode must be zippy.SourceMode")),
    }
}

fn parse_python_source_mode(value: &Bound<'_, PyAny>) -> PyResult<RustSourceMode> {
    match value.extract::<String>()?.as_str() {
        "pipeline" => Ok(RustSourceMode::Pipeline),
        "consumer" => Ok(RustSourceMode::Consumer),
        _ => Err(py_value_error(
            "python source mode must be 'pipeline' or 'consumer'",
        )),
    }
}

fn map_python_source_error(error: PyErr) -> ZippyError {
    ZippyError::Io {
        reason: format!("python source bridge failed error=[{}]", error),
    }
}

fn parse_runtime_options(
    buffer_capacity: usize,
    overflow_policy: Option<&Bound<'_, PyAny>>,
    archive_buffer_capacity: usize,
    xfast: bool,
) -> PyResult<RuntimeOptions> {
    if buffer_capacity == 0 {
        return Err(py_value_error("buffer_capacity must be greater than zero"));
    }

    if archive_buffer_capacity == 0 {
        return Err(py_value_error(
            "archive_buffer_capacity must be greater than zero",
        ));
    }

    Ok(RuntimeOptions {
        buffer_capacity,
        overflow_policy: parse_overflow_policy(overflow_policy)?,
        archive_buffer_capacity,
        xfast,
    })
}

fn parse_overflow_policy(value: Option<&Bound<'_, PyAny>>) -> PyResult<OverflowPolicy> {
    let value = parse_optional_policy_value(
        value,
        "overflow_policy",
        "overflow_policy",
        "OverflowPolicy",
        "block",
    )?;
    match value.as_str() {
        "block" => Ok(OverflowPolicy::Block),
        "reject" => Ok(OverflowPolicy::Reject),
        "drop_oldest" => Ok(OverflowPolicy::DropOldest),
        _ => Err(py_value_error(
            "overflow_policy must be zippy.OverflowPolicy",
        )),
    }
}

fn overflow_policy_as_str(value: OverflowPolicy) -> &'static str {
    match value {
        OverflowPolicy::Block => "block",
        OverflowPolicy::Reject => "reject",
        OverflowPolicy::DropOldest => "drop_oldest",
    }
}

fn metrics_snapshot_to_pydict(
    py: Python<'_>,
    snapshot: EngineMetricsSnapshot,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("processed_batches_total", snapshot.processed_batches_total)?;
    dict.set_item("processed_rows_total", snapshot.processed_rows_total)?;
    dict.set_item("output_batches_total", snapshot.output_batches_total)?;
    dict.set_item("dropped_batches_total", snapshot.dropped_batches_total)?;
    dict.set_item("late_rows_total", snapshot.late_rows_total)?;
    dict.set_item("filtered_rows_total", snapshot.filtered_rows_total)?;
    dict.set_item("publish_errors_total", snapshot.publish_errors_total)?;
    dict.set_item("queue_depth", snapshot.queue_depth)?;
    Ok(dict.into_any().unbind())
}

fn engine_base_config_dict<'py>(
    py: Python<'py>,
    engine_type: &str,
    name: &str,
    targets: &[TargetConfig],
    parquet_sink: &Option<ParquetSinkConfig>,
    runtime_options: &RuntimeOptions,
    source_linked: bool,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("engine_type", engine_type)?;
    dict.set_item("name", name)?;
    dict.set_item("buffer_capacity", runtime_options.buffer_capacity)?;
    dict.set_item(
        "overflow_policy",
        overflow_policy_as_str(runtime_options.overflow_policy),
    )?;
    dict.set_item(
        "archive_buffer_capacity",
        runtime_options.archive_buffer_capacity,
    )?;
    dict.set_item("xfast", runtime_options.xfast)?;
    dict.set_item("source_linked", source_linked)?;
    dict.set_item("has_sink", parquet_sink.is_some())?;
    dict.set_item("targets", target_configs_to_pylist(py, targets)?)?;
    dict.set_item("parquet_sink", parquet_sink_to_pyobject(py, parquet_sink)?)?;
    Ok(dict)
}

fn engine_has_source(
    source_owner: &SourceOwner,
    remote_source: &Option<RemoteSourceConfig>,
    bus_source: &Option<BusSourceConfig>,
    segment_source: &Option<SegmentSourceConfig>,
    python_source: &Option<PythonSourceConfig>,
) -> bool {
    source_owner.is_some()
        || remote_source.is_some()
        || bus_source.is_some()
        || segment_source.is_some()
        || python_source.is_some()
}

fn target_configs_to_pylist(py: Python<'_>, targets: &[TargetConfig]) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for target in targets {
        let item = PyDict::new_bound(py);
        match target {
            TargetConfig::Null => {
                item.set_item("type", "null")?;
            }
            TargetConfig::Zmq { endpoint } => {
                item.set_item("type", "zmq")?;
                item.set_item("endpoint", endpoint)?;
            }
            TargetConfig::ZmqStream {
                endpoint,
                stream_name,
                ..
            } => {
                item.set_item("type", "zmq_stream")?;
                item.set_item("endpoint", endpoint)?;
                item.set_item("stream_name", stream_name)?;
            }
            TargetConfig::BusStream { stream_name, .. } => {
                item.set_item("type", "bus_stream")?;
                item.set_item("stream_name", stream_name)?;
            }
        }
        list.append(item)?;
    }
    Ok(list.into_any().unbind())
}

fn parquet_sink_to_pyobject(
    py: Python<'_>,
    parquet_sink: &Option<ParquetSinkConfig>,
) -> PyResult<PyObject> {
    let Some(parquet_sink) = parquet_sink else {
        return Ok(py.None());
    };

    let dict = PyDict::new_bound(py);
    dict.set_item("path", parquet_sink.path.to_string_lossy().into_owned())?;
    dict.set_item("rotation", parquet_sink.rotation.as_str())?;
    dict.set_item("write_input", parquet_sink.write_input)?;
    dict.set_item("write_output", parquet_sink.write_output)?;
    dict.set_item("rows_per_batch", parquet_sink.rows_per_batch)?;
    dict.set_item("flush_interval_ms", parquet_sink.flush_interval_ms)?;
    Ok(dict.into_any().unbind())
}

fn parse_window_ns(
    window: Option<&Bound<'_, PyAny>>,
    window_type: Option<&Bound<'_, PyAny>>,
    window_ns: Option<i64>,
) -> PyResult<i64> {
    let window_type = parse_optional_policy_value(
        window_type,
        "window_type",
        "window_type",
        "WindowType",
        "tumbling",
    )?;
    if window_type != "tumbling" {
        return Err(py_value_error(
            "window_type must be zippy.WindowType.TUMBLING in v1",
        ));
    }

    if window.is_some() && window_ns.is_some() {
        return Err(py_value_error(
            "window and window_ns are mutually exclusive",
        ));
    }

    if let Some(window_ns) = window_ns {
        return Ok(window_ns);
    }

    let window = window
        .ok_or_else(|| py_value_error("window is required when window_ns is not provided"))?;

    if let Ok(window_ns) = window.extract::<i64>() {
        return Ok(window_ns);
    }

    let duration_attr = window.getattr("total_nanoseconds").map_err(|_| {
        py_value_error("window must be an integer nanosecond value or zippy.Duration")
    })?;
    duration_attr
        .extract::<i64>()
        .map_err(|_| py_value_error("window must be an integer nanosecond value or zippy.Duration"))
}

fn parse_duration_ns(value: &Bound<'_, PyAny>, parameter_name: &str) -> PyResult<i64> {
    if let Ok(value) = value.extract::<i64>() {
        return Ok(value);
    }

    let duration_attr = value.getattr("total_nanoseconds").map_err(|_| {
        py_value_error(format!(
            "{parameter_name} must be an integer nanosecond value or zippy.Duration"
        ))
    })?;
    duration_attr.extract::<i64>().map_err(|_| {
        py_value_error(format!(
            "{parameter_name} must be an integer nanosecond value or zippy.Duration"
        ))
    })
}

fn parse_required_policy_value(
    value: &Bound<'_, PyAny>,
    parameter_name: &str,
    expected_kind: &str,
    expected_namespace: &str,
) -> PyResult<String> {
    parse_policy_value(value, parameter_name, expected_kind, expected_namespace)
}

fn parse_optional_policy_value(
    value: Option<&Bound<'_, PyAny>>,
    parameter_name: &str,
    expected_kind: &str,
    expected_namespace: &str,
    default_value: &str,
) -> PyResult<String> {
    let Some(value) = value else {
        return Ok(default_value.to_string());
    };

    parse_policy_value(value, parameter_name, expected_kind, expected_namespace)
}

fn parse_policy_value(
    value: &Bound<'_, PyAny>,
    parameter_name: &str,
    expected_kind: &str,
    expected_namespace: &str,
) -> PyResult<String> {
    let constant_kind = value.getattr("_zippy_constant_kind").map_err(|_| {
        py_value_error(format!(
            "{parameter_name} must be zippy.{expected_namespace}"
        ))
    })?;
    let constant_kind = constant_kind.extract::<String>().map_err(|_| {
        py_value_error(format!(
            "{parameter_name} must be zippy.{expected_namespace}"
        ))
    })?;

    if constant_kind != expected_kind {
        return Err(py_value_error(format!(
            "{parameter_name} must be zippy.{expected_namespace}"
        )));
    }

    value
        .getattr("_zippy_constant_value")
        .map_err(|_| {
            py_value_error(format!(
                "{parameter_name} must be zippy.{expected_namespace}"
            ))
        })?
        .extract::<String>()
        .map_err(|_| {
            py_value_error(format!(
                "{parameter_name} must be zippy.{expected_namespace}"
            ))
        })
}

fn build_reactive_specs(
    py: Python<'_>,
    input_schema: &Arc<Schema>,
    id_column: &str,
    factors: Vec<Py<PyAny>>,
) -> PyResult<Vec<Box<dyn zippy_operators::ReactiveFactor>>> {
    let mut current_schema = Arc::clone(input_schema);
    let mut built_factors = Vec::with_capacity(factors.len());

    for factor in factors {
        let built = build_reactive_spec(py, id_column, current_schema.as_ref(), factor.bind(py))?;
        let output_field = built.output_field();

        if current_schema.index_of(output_field.name()).is_ok() {
            return Err(py_value_error(format!(
                "duplicate reactive output field field=[{}]",
                output_field.name()
            )));
        }

        let mut fields = current_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(output_field));
        current_schema = Arc::new(Schema::new(fields));
        built_factors.push(built);
    }

    Ok(built_factors)
}

fn build_reactive_spec(
    py: Python<'_>,
    id_column: &str,
    current_schema: &Schema,
    factor: &Bound<'_, PyAny>,
) -> PyResult<Box<dyn zippy_operators::ReactiveFactor>> {
    if let Ok(spec) = factor.extract::<PyRef<'_, TsEmaSpec>>() {
        return RustTsEmaSpec::new(id_column, &spec.value_column, spec.span, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsReturnSpec>>() {
        return RustTsReturnSpec::new(id_column, &spec.value_column, spec.period, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsMeanSpec>>() {
        return RustTsMeanSpec::new(id_column, &spec.value_column, spec.window, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsStdSpec>>() {
        return RustTsStdSpec::new(id_column, &spec.value_column, spec.window, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsDelaySpec>>() {
        return RustTsDelaySpec::new(id_column, &spec.value_column, spec.period, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsDiffSpec>>() {
        return RustTsDiffSpec::new(id_column, &spec.value_column, spec.period, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AbsSpec>>() {
        return RustAbsSpec::new(id_column, &spec.value_column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, LogSpec>>() {
        return RustLogSpec::new(id_column, &spec.value_column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, ClipSpec>>() {
        return RustClipSpec::new(
            id_column,
            &spec.value_column,
            spec.min,
            spec.max,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, CastSpec>>() {
        return RustCastSpec::new(id_column, &spec.value_column, &spec.dtype, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, ExpressionFactor>>() {
        return RustExpressionSpec::new(&spec.expression, &spec.output)
            .build_reactive_factor(current_schema, id_column)
            .map_err(|error| py_value_error(error.to_string()));
    }

    let _ = py;

    Err(PyTypeError::new_err(
        "factors must contain TsEmaSpec, TsReturnSpec, TsMeanSpec, TsStdSpec, TsDelaySpec, TsDiffSpec, AbsSpec, LogSpec, ClipSpec, CastSpec, or ExpressionFactor",
    ))
}

fn build_aggregation_specs(
    py: Python<'_>,
    factors: Vec<Py<PyAny>>,
) -> PyResult<Vec<Box<dyn RustAggregationSpec>>> {
    factors
        .into_iter()
        .map(|factor| build_aggregation_spec(py, factor.bind(py)))
        .collect()
}

fn build_aggregation_spec(
    py: Python<'_>,
    factor: &Bound<'_, PyAny>,
) -> PyResult<Box<dyn RustAggregationSpec>> {
    if let Ok(spec) = factor.extract::<PyRef<'_, AggFirstSpec>>() {
        return RustAggFirstSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggLastSpec>>() {
        return RustAggLastSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggSumSpec>>() {
        return RustAggSumSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggMaxSpec>>() {
        return RustAggMaxSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggMinSpec>>() {
        return RustAggMinSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggCountSpec>>() {
        return RustAggCountSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AggVwapSpec>>() {
        return RustAggVwapSpec::new(&spec.price_column, &spec.volume_column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    let _ = py;

    Err(PyTypeError::new_err(
        "factors must contain AggFirstSpec, AggLastSpec, AggSumSpec, AggMaxSpec, AggMinSpec, AggCountSpec, or AggVwapSpec",
    ))
}

fn build_cross_sectional_specs(
    py: Python<'_>,
    factors: Vec<Py<PyAny>>,
) -> PyResult<Vec<Box<dyn zippy_operators::CrossSectionalFactor>>> {
    factors
        .into_iter()
        .map(|factor| build_cross_sectional_spec(py, factor.bind(py)))
        .collect()
}

fn build_cross_sectional_spec(
    py: Python<'_>,
    factor: &Bound<'_, PyAny>,
) -> PyResult<Box<dyn zippy_operators::CrossSectionalFactor>> {
    if let Ok(spec) = factor.extract::<PyRef<'_, CSRankSpec>>() {
        return RustCSRankSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, CSZscoreSpec>>() {
        return RustCSZscoreSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, CSDemeanSpec>>() {
        return RustCSDemeanSpec::new(&spec.column, &spec.output)
            .build()
            .map_err(|error| py_value_error(error.to_string()));
    }

    let _ = py;

    Err(PyTypeError::new_err(
        "factors must contain CSRankSpec, CSZscoreSpec, or CSDemeanSpec",
    ))
}

fn build_expression_specs(
    py: Python<'_>,
    factors: Vec<Py<PyAny>>,
    parameter_name: &str,
) -> PyResult<Vec<RustExpressionSpec>> {
    factors
        .into_iter()
        .map(|factor| build_expression_spec(py, factor.bind(py), parameter_name))
        .collect()
}

fn build_expression_spec(
    py: Python<'_>,
    factor: &Bound<'_, PyAny>,
    parameter_name: &str,
) -> PyResult<RustExpressionSpec> {
    if let Ok(spec) = factor.extract::<PyRef<'_, ExpressionFactor>>() {
        return Ok(RustExpressionSpec::new(&spec.expression, &spec.output));
    }

    let _ = py;

    Err(PyTypeError::new_err(format!(
        "{parameter_name} must contain ExpressionFactor"
    )))
}

fn value_to_record_batches(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    input_schema: &Schema,
) -> PyResult<Vec<RecordBatch>> {
    let py_schema = input_schema
        .to_pyarrow(py)
        .map_err(|error| py_value_error(error.to_string()))?;

    if let Some(batch) = try_record_batch(value, py, &py_schema)? {
        return Ok(vec![batch]);
    }

    let arrow_value = if value.hasattr("to_arrow")? {
        value
            .call_method0("to_arrow")
            .map_err(|error| py_value_error(error.to_string()))?
    } else {
        value.clone()
    };

    if let Some(batch) = try_record_batch(&arrow_value, py, &py_schema)? {
        return Ok(vec![batch]);
    }

    if let Ok(casted) = arrow_value.call_method1("cast", (py_schema.clone_ref(py),)) {
        if let Ok(batches) = casted.call_method0("to_batches") {
            return py_batches_to_record_batches(&batches);
        }
    }

    if let Ok(batches) = arrow_value.call_method0("to_batches") {
        return py_batches_to_record_batches(&batches);
    }

    if let Some(batches) = try_python_native_batches(py, value, &py_schema)? {
        return Ok(batches);
    }

    Err(PyTypeError::new_err(
        "write() accepts polars.DataFrame, pyarrow.RecordBatch, pyarrow.Table, dict[str, list], dict[str, scalar], or list[dict] in v1",
    ))
}

fn try_record_batch(
    value: &Bound<'_, PyAny>,
    py: Python<'_>,
    py_schema: &Py<PyAny>,
) -> PyResult<Option<RecordBatch>> {
    if let Ok(casted) = value.call_method1("cast", (py_schema.clone_ref(py),)) {
        if let Ok(batch) = RecordBatch::from_pyarrow_bound(&casted) {
            return Ok(Some(batch));
        }
    }

    if let Ok(batch) = RecordBatch::from_pyarrow_bound(value) {
        return Ok(Some(batch));
    }

    Ok(None)
}

fn py_batches_to_record_batches(batches: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let batches = batches
        .downcast::<PyList>()
        .map_err(|error| py_value_error(error.to_string()))?;

    if batches.is_empty() {
        return Err(py_value_error(
            "input value produced no record batches after schema cast",
        ));
    }

    batches
        .iter()
        .map(|batch| {
            RecordBatch::from_pyarrow_bound(&batch)
                .map_err(|error| py_value_error(error.to_string()))
        })
        .collect()
}

fn try_python_native_batches(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    py_schema: &Py<PyAny>,
) -> PyResult<Option<Vec<RecordBatch>>> {
    let pyarrow = PyModule::import_bound(py, "pyarrow")
        .map_err(|error| py_runtime_error(format!("failed to import pyarrow error=[{}]", error)))?;
    let kwargs = PyDict::new_bound(py);
    kwargs
        .set_item("schema", py_schema.clone_ref(py))
        .map_err(|error| py_value_error(error.to_string()))?;

    if value.downcast::<PyDict>().is_ok() {
        if let Ok(table) = pyarrow.call_method("table", (value,), Some(&kwargs)) {
            return py_table_to_record_batches(&table).map(Some);
        }

        let rows = PyList::empty_bound(py);
        rows.append(value)
            .map_err(|error| py_value_error(error.to_string()))?;
        let table = pyarrow
            .getattr("Table")
            .and_then(|table| table.call_method("from_pylist", (rows,), Some(&kwargs)))
            .map_err(|error| py_value_error(error.to_string()))?;
        return py_table_to_record_batches(&table).map(Some);
    }

    if value.downcast::<PyList>().is_ok() {
        let table = pyarrow
            .getattr("Table")
            .and_then(|table| table.call_method("from_pylist", (value,), Some(&kwargs)))
            .map_err(|error| py_value_error(error.to_string()))?;
        return py_table_to_record_batches(&table).map(Some);
    }

    Ok(None)
}

fn py_table_to_record_batches(table: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    let batches = table
        .call_method0("to_batches")
        .map_err(|error| py_value_error(error.to_string()))?;
    py_batches_to_record_batches(&batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use pyo3::types::PyList;
    use zippy_engines::{
        ReactiveStateEngine as RustReactiveStateEngine, TimeSeriesEngine as RustTimeSeriesEngine,
    };
    use zippy_operators::{AggFirstSpec as RustAggFirstSpec, TsEmaSpec as RustTsEmaSpec};

    const MINUTE_NS: i64 = 60_000_000_000;

    fn tick_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]))
    }

    fn tick_batch(symbols: Vec<&str>, dts: Vec<i64>, prices: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            tick_schema(),
            vec![
                Arc::new(StringArray::from(symbols)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(dts).with_timezone("UTC")) as ArrayRef,
                Arc::new(Float64Array::from(prices)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn string_values(array: &ArrayRef) -> Vec<String> {
        let values = array.as_any().downcast_ref::<StringArray>().unwrap();
        (0..values.len())
            .map(|index| values.value(index).to_string())
            .collect()
    }

    fn timestamp_values(array: &ArrayRef) -> Vec<i64> {
        let values = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        (0..values.len()).map(|index| values.value(index)).collect()
    }

    fn float_values(array: &ArrayRef) -> Vec<f64> {
        let values = array.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..values.len()).map(|index| values.value(index)).collect()
    }

    #[test]
    fn descriptor_update_slot_is_consumed_only_after_empty_segment_poll() {
        let descriptor_updates = DescriptorUpdateSlot::default();
        descriptor_updates.set(DescriptorUpdate {
            descriptor_generation: 2,
            text: "next".to_string(),
            descriptor: serde_json::json!({"generation": 2}),
        });

        assert!(take_descriptor_update_after_poll(
            SubscriberReadOutcome::Rows,
            &descriptor_updates,
            "current",
        )
        .is_none());
        assert!(descriptor_updates.update.lock().unwrap().is_some());

        let update = take_descriptor_update_after_poll(
            SubscriberReadOutcome::Empty,
            &descriptor_updates,
            "current",
        )
        .expect("empty segment poll should consume pending descriptor update");

        assert_eq!(update.text, "next");
        assert!(descriptor_updates.update.lock().unwrap().is_none());
    }

    #[test]
    fn descriptor_watcher_uses_long_poll_timeout_instead_of_hot_polling() {
        assert!(SEGMENT_DESCRIPTOR_WATCH_TIMEOUT >= Duration::from_millis(100));
    }

    #[test]
    fn segment_reader_driver_switches_descriptor_only_after_draining_current_rows() {
        let schema = compile_segment_schema(&[
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])
        .unwrap();
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: 4,
        })
        .unwrap();
        let current_partition = store
            .open_partition_with_schema("ticks", "current", schema.clone())
            .unwrap();
        current_partition
            .writer()
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2606")?;
                row.write_f64("last_price", 4102.5)?;
                Ok(())
            })
            .unwrap();
        let current_descriptor: serde_json::Value = serde_json::from_slice(
            &current_partition
                .active_descriptor_envelope_bytes()
                .unwrap(),
        )
        .unwrap();

        let next_partition = store
            .open_partition_with_schema("ticks", "next", schema.clone())
            .unwrap();
        next_partition
            .writer()
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2607")?;
                row.write_f64("last_price", 4104.5)?;
                Ok(())
            })
            .unwrap();
        let next_descriptor: serde_json::Value =
            serde_json::from_slice(&next_partition.active_descriptor_envelope_bytes().unwrap())
                .unwrap();

        let (reader, _) = build_active_segment_reader(&current_descriptor, &schema).unwrap();
        let descriptor_updates = Arc::new(DescriptorUpdateSlot::default());
        descriptor_updates.set(descriptor_update_from_value(2, next_descriptor).unwrap());
        let descriptor_refresh_error = Arc::new(Mutex::new(None));
        let mut driver = SegmentReaderDriver::new(
            reader,
            serde_json::to_string(&current_descriptor).unwrap(),
            None,
            schema,
            Arc::clone(&descriptor_updates),
            Arc::clone(&descriptor_refresh_error),
            false,
            Duration::from_millis(1),
            SEGMENT_READER_IDLE_SPIN_CHECKS,
            None,
        );

        match driver.poll_next().unwrap() {
            SegmentReaderDriverEvent::Rows(span) => assert_eq!(span.row_count(), 1),
            other => panic!("expected current rows before descriptor switch got [{other:?}]"),
        }
        assert!(descriptor_updates.update.lock().unwrap().is_some());

        match driver.poll_next().unwrap() {
            SegmentReaderDriverEvent::DescriptorUpdated(generation) => assert_eq!(generation, 2),
            other => panic!("expected descriptor update after current rows got [{other:?}]"),
        }

        match driver.poll_next().unwrap() {
            SegmentReaderDriverEvent::Rows(span) => assert_eq!(span.row_count(), 1),
            other => panic!("expected rows from next descriptor got [{other:?}]"),
        }
    }

    #[test]
    fn stream_row_instrument_filter_skips_non_matching_rows_before_python_conversion() {
        let schema = compile_segment_schema(&[
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])
        .unwrap();
        let row_capacity = 4;
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: row_capacity,
        })
        .unwrap();
        let partition = store
            .open_partition_with_schema("ticks", "all", schema.clone())
            .unwrap();
        let writer = partition.writer();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2607")?;
                row.write_f64("last_price", 4104.5)?;
                Ok(())
            })
            .unwrap();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2606")?;
                row.write_f64("last_price", 4102.5)?;
                Ok(())
            })
            .unwrap();

        let envelope = partition.active_descriptor_envelope_bytes().unwrap();
        let layout = LayoutPlan::for_schema(&schema, row_capacity).unwrap();
        let descriptor =
            ActiveSegmentDescriptor::from_envelope_bytes(&envelope, schema, layout).unwrap();
        let span = RowSpanView::from_active_descriptor(descriptor, 0, 2).unwrap();
        let instrument_filter = BTreeSet::from(["IF2606".to_string()]);

        assert!(!row_matches_instrument_filter(&span, 0, Some(&instrument_filter)).unwrap());
        assert!(row_matches_instrument_filter(&span, 1, Some(&instrument_filter)).unwrap());
        assert!(row_matches_instrument_filter(&span, 0, None).unwrap());
    }

    #[test]
    fn live_segment_record_batches_reads_retained_sealed_before_active_boundary() {
        let schema = compile_segment_schema(&[
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])
        .unwrap();
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: 4,
        })
        .unwrap();
        let partition = store
            .open_partition_with_schema("ticks", "all", schema.clone())
            .unwrap();
        let writer = partition.writer();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2606")?;
                row.write_f64("last_price", 4102.5)?;
                Ok(())
            })
            .unwrap();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2607")?;
                row.write_f64("last_price", 4103.5)?;
                Ok(())
            })
            .unwrap();
        let sealed_descriptor =
            serde_json::from_slice(&partition.active_descriptor_envelope_bytes().unwrap()).unwrap();

        writer.rollover_without_persistence().unwrap();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2608")?;
                row.write_f64("last_price", 4104.5)?;
                Ok(())
            })
            .unwrap();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2609")?;
                row.write_f64("last_price", 4105.5)?;
                Ok(())
            })
            .unwrap();
        writer
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2610")?;
                row.write_f64("last_price", 4106.5)?;
                Ok(())
            })
            .unwrap();
        let active_descriptor =
            serde_json::from_slice(&partition.active_descriptor_envelope_bytes().unwrap()).unwrap();

        let batches =
            live_segment_record_batches(&active_descriptor, &[sealed_descriptor], schema, 2)
                .unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(
            string_values(batches[0].column(0)),
            vec!["IF2606", "IF2607"]
        );
        assert_eq!(
            string_values(batches[1].column(0)),
            vec!["IF2608", "IF2609"]
        );
        assert_eq!(batches[0].num_rows() + batches[1].num_rows(), 4);
    }

    #[test]
    fn query_snapshot_value_separates_active_descriptor_from_retained_sealed_segments() {
        let stream = StreamInfo {
            stream_name: "ticks".to_string(),
            schema: serde_json::json!({"fields": [], "metadata": {}}),
            schema_hash: "schema_hash".to_string(),
            data_path: "segment".to_string(),
            descriptor_generation: 7,
            active_segment_descriptor: None,
            sealed_segments: vec![serde_json::json!({
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 1,
                "row_capacity": 32,
                "shm_os_id": "/tmp/zippy-segment-old",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 2,
                "generation": 1
            })],
            persisted_files: Vec::new(),
            persist_events: Vec::new(),
            segment_reader_leases: Vec::new(),
            buffer_size: 64,
            frame_size: 4096,
            write_seq: 0,
            writer_process_id: Some("proc_1".to_string()),
            reader_count: 2,
            status: "registered".to_string(),
        };
        let descriptor = serde_json::json!({
            "magic": "zippy.segment.active",
            "version": 1,
            "schema_id": 1,
            "row_capacity": 32,
            "shm_os_id": "/tmp/zippy-segment",
            "payload_offset": 64,
            "committed_row_count_offset": 40,
            "segment_id": 3,
            "generation": 2
        });

        let active_segment_control = serde_json::json!({
            "committed_row_count": 11,
            "segment_id": 3,
            "generation": 2,
        });
        let snapshot = query_snapshot_value(&stream, descriptor, active_segment_control).unwrap();

        assert_eq!(snapshot["stream_name"], "ticks");
        assert_eq!(snapshot["schema_hash"], "schema_hash");
        assert_eq!(snapshot["descriptor_generation"], 7);
        assert_eq!(snapshot["active_committed_row_high_watermark"], 11);
        assert_eq!(
            snapshot["active_segment_control"]["committed_row_count"],
            11
        );
        assert_eq!(snapshot["active_segment_descriptor"]["segment_id"], 3);
        assert!(snapshot["active_segment_descriptor"]
            .get("sealed_segments")
            .is_none());
        assert_eq!(snapshot["sealed_segments"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["persisted_files"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn normalize_and_dispatch_bus_reader_respects_instrument_ids() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let empty = PyList::empty_bound(py);
            assert_eq!(parse_instrument_ids(Some(empty.as_any())).unwrap(), None);

            let tuple_values = PyTuple::new_bound(py, ["IH2606", "IF2606"]);
            assert_eq!(
                parse_instrument_ids(Some(tuple_values.as_any())).unwrap(),
                Some(vec!["IH2606".to_string(), "IF2606".to_string()])
            );

            let scalar = pyo3::types::PyString::new_bound(py, "IF2606");
            assert_eq!(
                parse_instrument_ids(Some(scalar.as_any())).unwrap(),
                Some(vec!["IF2606".to_string()])
            );

            let values = PyList::empty_bound(py);
            values.append("IF2606").unwrap();
            assert_eq!(
                parse_instrument_ids(Some(values.as_any())).unwrap(),
                Some(vec!["IF2606".to_string()])
            );

            let invalid = PyList::empty_bound(py);
            invalid.append("").unwrap();
            assert!(parse_instrument_ids(Some(invalid.as_any())).is_err());

            let empty_string = pyo3::types::PyString::new_bound(py, "");
            assert!(parse_instrument_ids(Some(empty_string.as_any())).is_err());
        });

        let client: SharedMasterClient = Arc::new(Mutex::new(
            CoreMasterClient::connect("/tmp/zippy-python-test").unwrap(),
        ));
        let calls: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let unfiltered_read_calls = Arc::clone(&calls);
        let unfiltered_filtered_calls = Arc::clone(&calls);
        let unfiltered_result = attach_bus_reader_with(
            &client,
            "ticks",
            None,
            false,
            move |_, stream_name, xfast| {
                unfiltered_read_calls
                    .lock()
                    .unwrap()
                    .push(format!("read_from:[{}]:[{}]", stream_name, xfast));
                Err(ZippyError::Io {
                    reason: "stop".to_string(),
                })
            },
            move |_, stream_name, instrument_ids, xfast| {
                unfiltered_filtered_calls.lock().unwrap().push(format!(
                    "read_from_filtered:[{}]:{:?}:[{}]",
                    stream_name, instrument_ids, xfast
                ));
                Err(ZippyError::Io {
                    reason: "stop".to_string(),
                })
            },
        );
        assert!(unfiltered_result.is_err());
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &["read_from:[ticks]:[false]".to_string()]
        );

        calls.lock().unwrap().clear();

        let filtered_read_calls = Arc::clone(&calls);
        let filtered_filtered_calls = Arc::clone(&calls);
        let filtered_result = attach_bus_reader_with(
            &client,
            "ticks",
            Some(vec!["IF2606".to_string()]),
            true,
            move |_, stream_name, xfast| {
                filtered_read_calls
                    .lock()
                    .unwrap()
                    .push(format!("read_from:[{}]:[{}]", stream_name, xfast));
                Err(ZippyError::Io {
                    reason: "stop".to_string(),
                })
            },
            move |_, stream_name, instrument_ids, xfast| {
                filtered_filtered_calls.lock().unwrap().push(format!(
                    "read_from_filtered:[{}]:{:?}:[{}]",
                    stream_name, instrument_ids, xfast
                ));
                Err(ZippyError::Io {
                    reason: "stop".to_string(),
                })
            },
        );
        assert!(filtered_result.is_err());
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &["read_from_filtered:[ticks]:[\"IF2606\"]:[true]".to_string()]
        );
    }

    #[test]
    fn in_process_publisher_routes_source_batches_to_downstream_engine() {
        let downstream_handle: SharedHandle = Arc::new(Mutex::new(None));
        let downstream_archive: SharedArchive = Arc::new(Mutex::new(None));
        let upstream_engine = RustReactiveStateEngine::new(
            "tick_factors",
            tick_schema(),
            vec![RustTsEmaSpec::new("symbol", "price", 2, "ema_2")
                .build()
                .unwrap()],
        )
        .unwrap();
        let downstream_engine = RustTimeSeriesEngine::new(
            "bars",
            upstream_engine.output_schema(),
            "symbol",
            "dt",
            MINUTE_NS,
            LateDataPolicy::Reject,
            vec![RustAggFirstSpec::new("price", "open").build().unwrap()],
            vec![],
            vec![],
        )
        .unwrap();

        let downstream_runtime = spawn_engine_with_publisher(
            downstream_engine,
            EngineConfig {
                name: "bars".to_string(),
                buffer_capacity: 1024,
                overflow_policy: Default::default(),
                late_data_policy: Default::default(),
                xfast: false,
            },
            Box::new(RustNullPublisher::default()),
        )
        .unwrap();
        *downstream_handle.lock().unwrap() = Some(downstream_runtime);

        let mut upstream_handle = spawn_engine_with_publisher(
            upstream_engine,
            EngineConfig {
                name: "tick_factors".to_string(),
                buffer_capacity: 1024,
                overflow_policy: Default::default(),
                late_data_policy: Default::default(),
                xfast: false,
            },
            InProcessPublisher {
                downstream: DownstreamLink {
                    handle: Arc::clone(&downstream_handle),
                    archive: Arc::clone(&downstream_archive),
                    write_input: false,
                },
            },
        )
        .unwrap();

        upstream_handle
            .write(tick_batch(vec!["A"], vec![1_000_000_000], vec![10.0]))
            .unwrap();
        upstream_handle.flush().unwrap();

        let flushed = {
            let guard = downstream_handle.lock().unwrap();
            guard.as_ref().unwrap().flush().unwrap()
        };

        assert_eq!(flushed.len(), 1);
        assert_eq!(string_values(flushed[0].column(0)), vec!["A".to_string()]);
        assert_eq!(timestamp_values(flushed[0].column(1)), vec![0]);
        assert_eq!(timestamp_values(flushed[0].column(2)), vec![MINUTE_NS]);
        assert_eq!(float_values(flushed[0].column(3)), vec![10.0]);

        upstream_handle.stop().unwrap();
        let mut guard = downstream_handle.lock().unwrap();
        guard.as_mut().unwrap().stop().unwrap();
    }
}
