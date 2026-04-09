#![allow(clippy::useless_conversion)]

mod native_source_bridge;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver as StdReceiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use zippy_core::{
    current_log_snapshot, python_dev_version, setup_log as setup_core_log, spawn_engine_with_publisher,
    spawn_source_engine_with_publisher, Engine, EngineConfig, EngineHandle,
    EngineMetricsSnapshot, EngineStatus, LateDataPolicy, LogConfig, OverflowPolicy,
    Publisher as CorePublisher, Source, SourceEvent, SourceHandle, SourceMode as RustSourceMode,
    SourceSink, StreamHello, ZippyError,
};
use zippy_engines::{
    CrossSectionalEngine as RustCrossSectionalEngine,
    ReactiveStateEngine as RustReactiveStateEngine, StreamTableEngine as RustStreamTableEngine,
    TimeSeriesEngine as RustTimeSeriesEngine,
};
use zippy_io::{
    FanoutPublisher as RustFanoutPublisher, NullPublisher as RustNullPublisher,
    ParquetSink as RustParquetSink, ParquetSinkWriter as RustParquetSinkWriter,
    ZmqPublisher as RustZmqPublisher, ZmqSource as RustZmqSource,
    ZmqStreamPublisher as RustZmqStreamPublisher, ZmqSubscriber as RustZmqSubscriber,
};
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

use native_source_bridge::create_native_source_sink_capsule;

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

type SharedHandle = Arc<Mutex<Option<EngineHandle>>>;
type SharedArchive = Arc<Mutex<Option<ArchiveHandle>>>;
type SharedStatus = Arc<Mutex<EngineStatus>>;
type SharedMetrics = Arc<Mutex<EngineMetricsSnapshot>>;
type SourceOwner = Option<Py<PyAny>>;
type RegisteredSource = (
    SourceOwner,
    Option<RemoteSourceConfig>,
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
}

#[derive(Clone)]
struct RemoteSourceConfig {
    endpoint: String,
    expected_schema: Arc<Schema>,
    mode: RustSourceMode,
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
            py.allow_threads(|| self.sink.emit(SourceEvent::Data(batch)))
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
        let runtime_handle = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let owner_bound = owner.bind(py);
            if owner_bound.hasattr("_zippy_start_native")? {
                let capsule =
                    create_native_source_sink_capsule(py, Arc::clone(&sink), Arc::clone(&schema))?;
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

        let root = self
            .active_root
            .clone()
            .ok_or_else(|| ZippyError::InvalidState {
                status: "parquet archive root is not available",
            })?;
        let schema = self
            .active_schema
            .clone()
            .ok_or_else(|| ZippyError::InvalidState {
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
    expression: String,
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
struct ReactiveStateEngine {
    name: String,
    id_column: String,
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
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct TimeSeriesEngine {
    name: String,
    id_column: String,
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
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct StreamTableEngine {
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
    engine: Option<RustStreamTableEngine>,
    remote_source: Option<RemoteSourceConfig>,
    python_source: Option<PythonSourceConfig>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pymethods]
impl ReactiveStateEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, factors, target, *, source=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = build_reactive_specs(py, &schema, &id_column, factors)?;
        let engine = RustReactiveStateEngine::new(&name, Arc::clone(&schema), factor_specs)
            .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options =
            parse_runtime_options(buffer_capacity, overflow_policy, archive_buffer_capacity)?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, _, python_source) = register_source(
            source,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink
                    .as_ref()
                    .map(|config| config.write_input)
                    .unwrap_or(false),
            },
            schema.as_ref(),
        )?;

        Ok(Self {
            name,
            id_column,
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
            None,
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
            self._source_owner.is_some(),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self) -> PyResult<()> {
        let result = flush_runtime_engine(&self.handle, &self.archive, &self.status, &self.metrics);
        sync_runtime_state(&self.handle, &self.status, &self.metrics);
        result
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(py, &self.handle, &self.archive, &self.status, &self.metrics)
    }
}

#[pymethods]
impl StreamTableEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, target, *, source=None, sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024))]
    fn new(
        name: String,
        input_schema: &Bound<'_, PyAny>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let engine = RustStreamTableEngine::new(&name, Arc::clone(&schema))
            .map_err(|error| py_value_error(error.to_string()))?;
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
        let runtime_options =
            parse_runtime_options(buffer_capacity, overflow_policy, archive_buffer_capacity)?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, python_source) = register_source(
            source,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink
                    .as_ref()
                    .map(|config| config.write_input)
                    .unwrap_or(false),
            },
            schema.as_ref(),
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
            self._source_owner.is_some(),
        )?;
        dict.del_item("parquet_sink")?;
        dict.set_item("sink", parquet_sink_to_pyobject(py, &self.parquet_sink)?)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self) -> PyResult<()> {
        let result = flush_runtime_engine(&self.handle, &self.archive, &self.status, &self.metrics);
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
    #[pyo3(signature = (name, input_schema, id_column, dt_column, late_data_policy, factors, target, *, window=None, window_type=None, window_ns=None, pre_factors=None, post_factors=None, source=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024))]
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
        source: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
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
        let late_data_policy_value = parse_required_policy_value(
            late_data_policy,
            "late_data_policy",
            "late_data_policy",
            "LateDataPolicy",
        )?;
        let late_data_policy_enum = parse_late_data_policy(&late_data_policy_value)?;
        let window_ns = parse_window_ns(window, window_type, window_ns)?;
        let engine = RustTimeSeriesEngine::new(
            &name,
            Arc::clone(&schema),
            &id_column,
            &dt_column,
            window_ns,
            late_data_policy_enum,
            factor_specs,
            pre_factor_specs,
            post_factor_specs,
        )
        .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let runtime_options =
            parse_runtime_options(buffer_capacity, overflow_policy, archive_buffer_capacity)?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source, python_source) = register_source(
            source,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink
                    .as_ref()
                    .map(|config| config.write_input)
                    .unwrap_or(false),
            },
            schema.as_ref(),
        )?;

        Ok(Self {
            name,
            id_column,
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
            self._source_owner.is_some(),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        dict.set_item("dt_column", &self.dt_column)?;
        dict.set_item("window_ns", self.window_ns)?;
        dict.set_item("late_data_policy", &self.late_data_policy)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self) -> PyResult<()> {
        let result = flush_runtime_engine(&self.handle, &self.archive, &self.status, &self.metrics);
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
    #[pyo3(signature = (name, input_schema, id_column, dt_column, trigger_interval, late_data_policy, factors, target, *, source=None, parquet_sink=None, buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024))]
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
        parquet_sink: Option<&Bound<'_, PyAny>>,
        buffer_capacity: usize,
        overflow_policy: Option<&Bound<'_, PyAny>>,
        archive_buffer_capacity: usize,
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
        let runtime_options =
            parse_runtime_options(buffer_capacity, overflow_policy, archive_buffer_capacity)?;
        let handle = Arc::new(Mutex::new(None));
        let archive = Arc::new(Mutex::new(None));
        let status = Arc::new(Mutex::new(EngineStatus::Created));
        let metrics = Arc::new(Mutex::new(EngineMetricsSnapshot::default()));
        let (source_owner, remote_source) = register_timeseries_source(
            source,
            DownstreamLink {
                handle: Arc::clone(&handle),
                archive: Arc::clone(&archive),
                write_input: parquet_sink
                    .as_ref()
                    .map(|config| config.write_input)
                    .unwrap_or(false),
            },
            schema.as_ref(),
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
            self._source_owner.is_some(),
        )?;
        dict.set_item("id_column", &self.id_column)?;
        dict.set_item("dt_column", &self.dt_column)?;
        dict.set_item("trigger_interval_ns", self.trigger_interval_ns)?;
        dict.set_item("late_data_policy", &self.late_data_policy)?;
        Ok(dict.into_any().unbind())
    }

    fn flush(&self) -> PyResult<()> {
        let result = flush_runtime_engine(&self.handle, &self.archive, &self.status, &self.metrics);
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
    module.add_function(wrap_pyfunction!(setup_log, module)?)?;
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
    module.add_class::<ReactiveStateEngine>()?;
    module.add_class::<StreamTableEngine>()?;
    module.add_class::<TimeSeriesEngine>()?;
    module.add_class::<CrossSectionalEngine>()?;
    Ok(())
}

#[pyfunction]
fn version() -> String {
    python_dev_version()
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

    Err(PyTypeError::new_err(
        "target must be NullPublisher, ZmqPublisher, ZmqStreamPublisher, or a non-empty list of them",
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

fn register_source(
    source: Option<&Bound<'_, PyAny>>,
    downstream: DownstreamLink,
    input_schema: &Schema,
) -> PyResult<RegisteredSource> {
    let Some(source) = source else {
        return Ok((None, None, None));
    };

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
        return Ok((Some(source.clone().unbind()), None, None));
    }

    if let Ok(mut engine) = source.extract::<PyRefMut<'_, StreamTableEngine>>() {
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
        return Ok((Some(source.clone().unbind()), None, None));
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
            Some(PythonSourceConfig {
                owner: source.clone().unbind(),
                name,
                output_schema,
                mode,
            }),
        ));
    }

    Err(PyTypeError::new_err(
        "source must be ReactiveStateEngine, StreamTableEngine, TimeSeriesEngine, ZmqSource, or a Python source plugin",
    ))
}

fn register_timeseries_source(
    source: Option<&Bound<'_, PyAny>>,
    downstream: DownstreamLink,
    input_schema: &Schema,
) -> PyResult<(Option<Py<PyAny>>, Option<RemoteSourceConfig>)> {
    let Some(source) = source else {
        return Ok((None, None));
    };

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
        return Ok((Some(source.clone().unbind()), None));
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
        ));
    }

    Err(PyTypeError::new_err(
        "source must be TimeSeriesEngine or ZmqSource for CrossSectionalEngine",
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

    if let Ok(engine) = source.extract::<PyRef<'_, StreamTableEngine>>() {
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

#[allow(clippy::too_many_arguments)]
fn start_runtime_engine<E: Engine>(
    name: &str,
    runtime_options: &RuntimeOptions,
    targets: &[TargetConfig],
    parquet_sink: Option<&ParquetSinkConfig>,
    remote_source: Option<&RemoteSourceConfig>,
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
    };
    config
        .validate()
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let engine = match engine.take() {
        Some(engine) => engine,
        None => return Err(py_runtime_error("engine already started")),
    };

    let handle = match (remote_source, python_source) {
        (Some(remote_source), None) => {
            let source = Box::new(
                RustZmqSource::connect(
                    &format!("{name}_source"),
                    &remote_source.endpoint,
                    remote_source.expected_schema.clone(),
                    remote_source.mode,
                )
                .map_err(|error| py_runtime_error(error.to_string()))?,
            );
            spawn_source_engine_with_publisher(source, engine, config, publisher)
        }
        (None, Some(python_source)) => {
            let source = Box::new(PythonSourceBridge {
                owner: Python::with_gil(|py| python_source.owner.clone_ref(py)),
                name: python_source.name.clone(),
                output_schema: Arc::clone(&python_source.output_schema),
                mode: python_source.mode,
            });
            spawn_source_engine_with_publisher(source, engine, config, publisher)
        }
        (None, None) => spawn_engine_with_publisher(engine, config, publisher),
        (Some(_), Some(_)) => {
            return Err(py_runtime_error(
                "engine cannot use remote source and python source at the same time",
            ));
        }
    }
    .map_err(|error| py_runtime_error(error.to_string()))?;

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
    handle: &SharedHandle,
    archive: &SharedArchive,
    status: &SharedStatus,
    metrics: &SharedMetrics,
) -> PyResult<()> {
    with_handle(handle, |runtime| {
        runtime
            .flush()
            .map(|_| ())
            .map_err(|error| py_runtime_error(error.to_string()))
    })?;
    if let Some(archive) = archive.lock().unwrap().as_ref().cloned() {
        archive
            .flush()
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
    dict.set_item("source_linked", source_linked)?;
    dict.set_item("has_sink", parquet_sink.is_some())?;
    dict.set_item("targets", target_configs_to_pylist(py, targets)?)?;
    dict.set_item("parquet_sink", parquet_sink_to_pyobject(py, parquet_sink)?)?;
    Ok(dict)
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
            .build(current_schema)
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
