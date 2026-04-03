#![allow(clippy::useless_conversion)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver as StdReceiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use zippy_core::{
    python_dev_version, spawn_engine_with_publisher, Engine, EngineConfig, EngineHandle,
    EngineStatus, LateDataPolicy, Publisher as CorePublisher, ZippyError,
};
use zippy_engines::{
    ReactiveStateEngine as RustReactiveStateEngine, TimeSeriesEngine as RustTimeSeriesEngine,
};
use zippy_io::{
    FanoutPublisher as RustFanoutPublisher, NullPublisher as RustNullPublisher,
    ParquetSink as RustParquetSink,
    ZmqPublisher as RustZmqPublisher, ZmqSubscriber as RustZmqSubscriber,
};
use zippy_operators::{
    AbsSpec as RustAbsSpec, AggCountSpec as RustAggCountSpec, AggFirstSpec as RustAggFirstSpec,
    AggLastSpec as RustAggLastSpec, AggMaxSpec as RustAggMaxSpec, AggMinSpec as RustAggMinSpec,
    AggSumSpec as RustAggSumSpec, AggVwapSpec as RustAggVwapSpec,
    AggregationSpec as RustAggregationSpec, CastSpec as RustCastSpec, ClipSpec as RustClipSpec,
    LogSpec as RustLogSpec, TsDelaySpec as RustTsDelaySpec, TsDiffSpec as RustTsDiffSpec,
    TsEmaSpec as RustTsEmaSpec, TsMeanSpec as RustTsMeanSpec,
    TsReturnSpec as RustTsReturnSpec, TsStdSpec as RustTsStdSpec,
};

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

type SharedHandle = Arc<Mutex<Option<EngineHandle>>>;
type DownstreamLink = SharedHandle;

#[derive(Clone)]
enum TargetConfig {
    Null,
    Zmq { endpoint: String },
}

struct InProcessPublisher {
    downstream: DownstreamLink,
}

impl CorePublisher for InProcessPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> zippy_core::Result<()> {
        let guard = self.downstream.lock().unwrap();
        let handle = guard.as_ref().ok_or(zippy_core::ZippyError::InvalidState {
            status: "engine not started",
        })?;
        handle.write(batch.clone())
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
}

#[derive(Clone)]
struct ParquetSinkConfig {
    path: PathBuf,
    rotation: ParquetRotation,
    write_input: bool,
    write_output: bool,
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
    fn spawn(config: ParquetSinkConfig) -> Self {
        let (tx, rx) = mpsc::sync_channel(1024);
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
        let close_result = send_result.and_then(|_| {
            reply_rx.recv().map_err(|_| ZippyError::ChannelReceive)?
        });
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
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
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
    let mut input_index = 0_u64;
    let mut output_index = 0_u64;

    while let Ok(command) = rx.recv() {
        match command {
            ArchiveCommand::Write { kind, batch } => match kind {
                ArchiveKind::Input => {
                    input_index += 1;
                    write_archive_batch(&config, &kind, input_index, &batch)?;
                }
                ArchiveKind::Output => {
                    output_index += 1;
                    write_archive_batch(&config, &kind, output_index, &batch)?;
                }
            },
            ArchiveCommand::Flush(reply_tx) => {
                let _ = reply_tx.send(Ok(()));
            }
            ArchiveCommand::Close(reply_tx) => {
                let _ = reply_tx.send(Ok(()));
                return Ok(());
            }
        }
    }

    Ok(())
}

fn write_archive_batch(
    config: &ParquetSinkConfig,
    kind: &ArchiveKind,
    index: u64,
    batch: &RecordBatch,
) -> zippy_core::Result<()> {
    let root = archive_root(config, kind)?;
    let sink = RustParquetSink::new(root);
    sink.write_batch(&format!("{index:06}.parquet"), batch)
}

fn archive_root(
    config: &ParquetSinkConfig,
    kind: &ArchiveKind,
) -> zippy_core::Result<PathBuf> {
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
    fn new(
        id_column: String,
        value_column: String,
        min: f64,
        max: f64,
        output: String,
    ) -> Self {
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
}

#[pymethods]
impl ParquetSink {
    #[new]
    #[pyo3(signature = (path, rotation="none", write_input=false, write_output=true))]
    fn new(
        path: String,
        rotation: &str,
        write_input: bool,
        write_output: bool,
    ) -> PyResult<Self> {
        ParquetRotation::parse(rotation)?;
        if !write_input && !write_output {
            return Err(py_value_error(
                "parquet_sink must enable write_input or write_output",
            ));
        }

        Ok(Self {
            path,
            rotation: rotation.to_string(),
            write_input,
            write_output,
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
struct ZmqSubscriber {
    subscriber: Option<RustZmqSubscriber>,
}

#[pymethods]
impl ZmqSubscriber {
    #[new]
    #[pyo3(signature = (endpoint, timeout_ms=1000))]
    fn new(endpoint: String, timeout_ms: i32) -> PyResult<Self> {
        let subscriber =
            RustZmqSubscriber::connect(&endpoint, timeout_ms).map_err(|error| {
                py_runtime_error(error.to_string())
            })?;

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
struct ReactiveStateEngine {
    name: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    archive: Option<ArchiveHandle>,
    handle: SharedHandle,
    engine: Option<RustReactiveStateEngine>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pyclass]
struct TimeSeriesEngine {
    name: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    archive: Option<ArchiveHandle>,
    handle: SharedHandle,
    engine: Option<RustTimeSeriesEngine>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}

#[pymethods]
impl ReactiveStateEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, factors, target, *, source=None, parquet_sink=None))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        source: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = build_reactive_specs(py, &id_column, factors)?;
        let engine = RustReactiveStateEngine::new(&name, Arc::clone(&schema), factor_specs)
            .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let handle = Arc::new(Mutex::new(None));
        let source_owner = register_source(source, Arc::clone(&handle), schema.as_ref())?;

        Ok(Self {
            name,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            archive: None,
            handle,
            engine: Some(engine),
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.target,
            self.parquet_sink.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        self.archive = archive;
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        write_runtime_input(
            py,
            &self.handle,
            self.archive.as_ref(),
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        )
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        flush_runtime_engine(&self.handle, self.archive.as_ref())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(&self.handle, self.archive.as_ref())
    }
}

#[pymethods]
impl TimeSeriesEngine {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, dt_column, late_data_policy, factors, target, *, window=None, window_type="tumbling", window_ns=None, source=None, parquet_sink=None))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        dt_column: String,
        late_data_policy: String,
        factors: Vec<Py<PyAny>>,
        target: &Bound<'_, PyAny>,
        window: Option<&Bound<'_, PyAny>>,
        window_type: &str,
        window_ns: Option<i64>,
        source: Option<&Bound<'_, PyAny>>,
        parquet_sink: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = build_aggregation_specs(py, factors)?;
        let late_data_policy = parse_late_data_policy(&late_data_policy)?;
        let window_ns = parse_window_ns(window, window_type, window_ns)?;
        let engine = RustTimeSeriesEngine::new(
            &name,
            Arc::clone(&schema),
            &id_column,
            &dt_column,
            window_ns,
            late_data_policy,
            factor_specs,
        )
        .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;
        let parquet_sink = parse_parquet_sink(parquet_sink)?;
        let handle = Arc::new(Mutex::new(None));
        let source_owner = register_source(source, Arc::clone(&handle), schema.as_ref())?;

        Ok(Self {
            name,
            input_schema: schema,
            output_schema,
            target,
            parquet_sink,
            archive: None,
            handle,
            engine: Some(engine),
            downstreams: Vec::new(),
            _source_owner: source_owner,
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let (handle, archive) = start_runtime_engine(
            &self.name,
            &self.target,
            self.parquet_sink.as_ref(),
            &self.downstreams,
            &mut self.engine,
        )?;
        *self.handle.lock().unwrap() = Some(handle);
        self.archive = archive;
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        ensure_downstreams_running(&self.downstreams)?;
        write_runtime_input(
            py,
            &self.handle,
            self.archive.as_ref(),
            self.parquet_sink.as_ref(),
            value,
            &self.input_schema,
        )
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        flush_runtime_engine(&self.handle, self.archive.as_ref())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        ensure_source_stopped(py, &self._source_owner)?;
        stop_runtime_engine(&self.handle, self.archive.as_ref())
    }
}

#[pymodule]
fn _internal(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", python_dev_version())?;
    module.add_function(wrap_pyfunction!(version, module)?)?;
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
    module.add_class::<AggFirstSpec>()?;
    module.add_class::<AggLastSpec>()?;
    module.add_class::<AggSumSpec>()?;
    module.add_class::<AggMaxSpec>()?;
    module.add_class::<AggMinSpec>()?;
    module.add_class::<AggCountSpec>()?;
    module.add_class::<AggVwapSpec>()?;
    module.add_class::<NullPublisher>()?;
    module.add_class::<ParquetSink>()?;
    module.add_class::<ZmqPublisher>()?;
    module.add_class::<ZmqSubscriber>()?;
    module.add_class::<ReactiveStateEngine>()?;
    module.add_class::<TimeSeriesEngine>()?;
    Ok(())
}

#[pyfunction]
fn version() -> String {
    python_dev_version()
}

fn parse_targets(target: &Bound<'_, PyAny>) -> PyResult<Vec<TargetConfig>> {
    if let Ok(targets) = target.downcast::<PyList>() {
        if targets.is_empty() {
            return Err(PyTypeError::new_err("target list must not be empty"));
        }

        return targets.iter().map(|item| parse_single_target(&item)).collect();
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

    Err(PyTypeError::new_err(
        "target must be NullPublisher, ZmqPublisher, or a non-empty list of them",
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
    }))
}

fn register_source(
    source: Option<&Bound<'_, PyAny>>,
    downstream: DownstreamLink,
    input_schema: &Schema,
) -> PyResult<Option<Py<PyAny>>> {
    let Some(source) = source else {
        return Ok(None);
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
        engine.downstreams.push(downstream);
        return Ok(Some(source.clone().unbind()));
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
        return Ok(Some(source.clone().unbind()));
    }

    Err(PyTypeError::new_err(
        "source must be ReactiveStateEngine or TimeSeriesEngine",
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
            downstream: Arc::clone(downstream),
        }));
    }

    if publishers.len() == 1 {
        return Ok(publishers.pop().expect("single publisher checked above"));
    }

    Ok(Box::new(RustFanoutPublisher::new(publishers)))
}

fn ensure_downstreams_running(downstreams: &[DownstreamLink]) -> PyResult<()> {
    for downstream in downstreams {
        let guard = downstream.lock().unwrap();
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

fn start_runtime_engine<E: Engine>(
    name: &str,
    targets: &[TargetConfig],
    parquet_sink: Option<&ParquetSinkConfig>,
    downstreams: &[DownstreamLink],
    engine: &mut Option<E>,
) -> PyResult<(EngineHandle, Option<ArchiveHandle>)> {
    let engine = match engine.take() {
        Some(engine) => engine,
        None => return Err(py_runtime_error("engine already started")),
    };
    let archive = parquet_sink.cloned().map(ArchiveHandle::spawn);
    let publisher = build_publisher(targets, parquet_sink, archive.clone(), downstreams)?;

    let handle = spawn_engine_with_publisher(
        engine,
        EngineConfig {
            name: name.to_string(),
            buffer_capacity: 1024,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
        publisher,
    )
    .map_err(|error| py_runtime_error(error.to_string()))?;

    Ok((handle, archive))
}

fn write_runtime_input(
    py: Python<'_>,
    handle: &SharedHandle,
    archive: Option<&ArchiveHandle>,
    parquet_sink: Option<&ParquetSinkConfig>,
    value: &Bound<'_, PyAny>,
    input_schema: &Schema,
) -> PyResult<()> {
    let batches = value_to_record_batches(py, value, input_schema)?;

    for batch in batches {
        if parquet_sink.map(|config| config.write_input).unwrap_or(false) {
            archive
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

fn flush_runtime_engine(handle: &SharedHandle, archive: Option<&ArchiveHandle>) -> PyResult<()> {
    with_handle(handle, |runtime| {
        runtime
            .flush()
            .map(|_| ())
            .map_err(|error| py_runtime_error(error.to_string()))
    })?;
    if let Some(archive) = archive {
        archive
            .flush()
            .map_err(|error| py_runtime_error(error.to_string()))?;
    }
    Ok(())
}

fn stop_runtime_engine(handle: &SharedHandle, archive: Option<&ArchiveHandle>) -> PyResult<()> {
    let mut guard = handle.lock().unwrap();
    let mut runtime = match guard.take() {
        Some(handle) => handle,
        None => return Err(py_runtime_error("engine not started")),
    };

    let stop_result = runtime
        .stop()
        .map_err(|error| py_runtime_error(error.to_string()));
    if let Some(archive) = archive {
        archive
            .close()
            .map_err(|error| py_runtime_error(error.to_string()))?;
    }
    stop_result
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

fn parse_late_data_policy(value: &str) -> PyResult<LateDataPolicy> {
    match value {
        "reject" => Ok(LateDataPolicy::Reject),
        "drop_with_metric" => Ok(LateDataPolicy::DropWithMetric),
        _ => Err(py_value_error(
            "late_data_policy must be 'reject' or 'drop_with_metric'",
        )),
    }
}

fn parse_window_ns(
    window: Option<&Bound<'_, PyAny>>,
    window_type: &str,
    window_ns: Option<i64>,
) -> PyResult<i64> {
    if window_type != "tumbling" {
        return Err(py_value_error("window_type must be 'tumbling' in v1"));
    }

    if window.is_some() && window_ns.is_some() {
        return Err(py_value_error(
            "window and window_ns are mutually exclusive",
        ));
    }

    if let Some(window_ns) = window_ns {
        return Ok(window_ns);
    }

    let window = window.ok_or_else(|| {
        py_value_error("window is required when window_ns is not provided")
    })?;

    if let Ok(window_ns) = window.extract::<i64>() {
        return Ok(window_ns);
    }

    let duration_attr = window
        .getattr("total_nanoseconds")
        .map_err(|_| py_value_error("window must be an integer nanosecond value or zippy.Duration"))?;
    duration_attr
        .extract::<i64>()
        .map_err(|_| py_value_error("window must be an integer nanosecond value or zippy.Duration"))
}

fn build_reactive_specs(
    py: Python<'_>,
    id_column: &str,
    factors: Vec<Py<PyAny>>,
) -> PyResult<Vec<Box<dyn zippy_operators::ReactiveFactor>>> {
    factors
        .into_iter()
        .map(|factor| build_reactive_spec(py, id_column, factor.bind(py)))
        .collect()
}

fn build_reactive_spec(
    py: Python<'_>,
    id_column: &str,
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

    let _ = py;

    Err(PyTypeError::new_err(
        "factors must contain TsEmaSpec, TsReturnSpec, TsMeanSpec, TsStdSpec, TsDelaySpec, TsDiffSpec, AbsSpec, LogSpec, ClipSpec, or CastSpec",
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
            RecordBatch::from_pyarrow_bound(&batch).map_err(|error| py_value_error(error.to_string()))
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
                downstream: Arc::clone(&downstream_handle),
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
