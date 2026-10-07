//! Native Python kernels, state consumption and server lifecycle bindings.
#![allow(clippy::useless_conversion)]

mod native_compute;

use std::fs;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyModule, PyTuple};
use tracing::info;
use zippy_core::{
    base_version, build_revision, current_log_snapshot, resolve_control_endpoint,
    setup_log as setup_core_log, ControlEndpoint, LateDataPolicy, LogConfig, ZippyConfig,
    DEFAULT_CONTROL_ENDPOINT_URI,
};
use zippy_engines::{
    AuctionPolicy as RustAuctionPolicy, BarGeneratorSpec as RustBarGeneratorSpec,
    BarInputColumns as RustBarInputColumns, BarSessionSpec as RustBarSessionSpec,
    BootstrapPolicy as RustBootstrapPolicy, DtLabelPolicy as RustDtLabelPolicy,
    ReactiveInvalidValuePolicy as RustReactiveInvalidValuePolicy,
    ReactiveStateFailurePolicy as RustReactiveStateFailurePolicy,
    SessionWindow as RustSessionWindow, VolumeOutputDtype as RustVolumeOutputDtype,
    VolumeSpec as RustVolumeSpec,
};
use zippy_master::daemon::MasterDaemonConfig;
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
use zippy_server::{run_server_daemon as run_rust_server_daemon, ServerRuntimeConfig};

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn py_runtime_error_with_code(
    py: Python<'_>,
    message: impl Into<String>,
    code: &str,
) -> PyResult<PyErr> {
    let error = py_runtime_error(message);
    error.value_bound(py).setattr("zippy_error_code", code)?;
    Ok(error)
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
    Duration::try_from_secs_f64(startup_timeout_sec)
        .map_err(|_| py_value_error("startup_timeout_sec exceeds the supported duration range"))
}

#[pyclass]
struct TsEmaSpec {
    id_column: String,
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
            id_column,
            value_column,
            span,
            output,
        }
    }
}

#[pyclass]
struct TsReturnSpec {
    id_column: String,
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
            id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct TsMeanSpec {
    id_column: String,
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
            id_column,
            value_column,
            window,
            output,
        }
    }
}

#[pyclass]
struct TsStdSpec {
    id_column: String,
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
            id_column,
            value_column,
            window,
            output,
        }
    }
}

#[pyclass]
struct TsDelaySpec {
    id_column: String,
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
            id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct TsDiffSpec {
    id_column: String,
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
            id_column,
            value_column,
            period,
            output,
        }
    }
}

#[pyclass]
struct AbsSpec {
    id_column: String,
    value_column: String,
    output: String,
}

#[pymethods]
impl AbsSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, output))]
    fn new(id_column: String, value_column: String, output: String) -> Self {
        Self {
            id_column,
            value_column,
            output,
        }
    }
}

#[pyclass]
struct LogSpec {
    id_column: String,
    value_column: String,
    output: String,
}

#[pymethods]
impl LogSpec {
    #[new]
    #[pyo3(signature = (id_column, value_column, output))]
    fn new(id_column: String, value_column: String, output: String) -> Self {
        Self {
            id_column,
            value_column,
            output,
        }
    }
}

#[pyclass]
struct ClipSpec {
    id_column: String,
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
            id_column,
            value_column,
            min,
            max,
            output,
        }
    }
}

#[pyclass]
struct CastSpec {
    id_column: String,
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
            id_column,
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

/// Native, columnar protocol-v2 consumer. Python orchestration never expands delta rows.
#[pyclass]
struct KeyValueState {
    inner: zippy_engines::KeyValueState,
}

#[pymethods]
impl KeyValueState {
    fn select(&self, py: Python<'_>, keys: Vec<Vec<String>>) -> PyResult<PyObject> {
        let batch = py
            .allow_threads(|| self.inner.select(&keys))
            .map_err(|error| py_runtime_error(error.to_string()))?;
        batch
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    #[new]
    fn new(
        py: Python<'_>,
        snapshot: &Bound<'_, PyAny>,
        max_pending_bytes: usize,
    ) -> PyResult<Self> {
        let snapshot = RecordBatch::from_pyarrow_bound(snapshot)
            .map_err(|error| py_value_error(error.to_string()))?;
        let inner = py
            .allow_threads(|| {
                zippy_engines::KeyValueState::from_snapshot(snapshot, max_pending_bytes)
            })
            .map_err(|error| py_value_error(error.to_string()))?;
        Ok(Self { inner })
    }

    fn apply(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<usize> {
        let batch = RecordBatch::from_pyarrow_bound(batch)
            .map_err(|error| py_value_error(error.to_string()))?;
        py.allow_threads(|| self.inner.apply(batch))
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let batch = py
            .allow_threads(|| self.inner.snapshot())
            .map_err(|error| py_runtime_error(error.to_string()))?;
        batch
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    #[getter]
    fn sequence(&self) -> u64 {
        self.inner.sequence()
    }
}

#[pymodule]
fn _internal(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", base_version())?;
    module.add("__build_revision__", build_revision())?;
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(_signal_server_process, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_server_control_endpoint, module)?)?;
    module.add_function(wrap_pyfunction!(run_server_daemon, module)?)?;
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
    module.add_class::<native_compute::NativeProcessor>()?;
    module.add_class::<KeyValueState>()?;
    Ok(())
}

#[pyfunction]
fn version() -> &'static str {
    base_version()
}

#[pyfunction]
fn _signal_server_process(
    pid: u32,
    boot_id: &str,
    start_ticks: &str,
    signal_number: i32,
) -> PyResult<bool> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::OwnedFd;

        if boot_id.is_empty() || start_ticks.is_empty() {
            return Err(py_value_error("server process identity must be complete"));
        }
        if !matches!(signal_number, 0 | libc::SIGTERM | libc::SIGKILL) {
            return Err(py_value_error("unsupported server process signal"));
        }
        let native_pid = libc::pid_t::try_from(pid)
            .map_err(|_| py_value_error("server process PID is out of range"))?;
        // Opening the pidfd first pins the process generation before `/proc` identity validation.
        let raw_pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, native_pid, 0) };
        if raw_pidfd < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(false);
            }
            return Err(py_runtime_error(format!("open server pidfd: {error}")));
        }
        let pidfd = unsafe { OwnedFd::from_raw_fd(raw_pidfd as libc::c_int) };
        let actual_boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(|error| py_runtime_error(format!("read Linux boot identity: {error}")))?;
        let stat_line = match fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                return Err(py_runtime_error(format!(
                    "read server process identity: {error}"
                )))
            }
        };
        let actual_start_ticks = stat_line
            .rfind(')')
            .and_then(|index| stat_line.get(index + 2..))
            .and_then(|suffix| suffix.split_whitespace().nth(19))
            .ok_or_else(|| py_runtime_error("parse server process identity"))?;
        if actual_boot_id.trim() != boot_id || actual_start_ticks != start_ticks {
            return Ok(false);
        }
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                std::os::fd::AsRawFd::as_raw_fd(&pidfd),
                signal_number,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(false);
            }
            return Err(py_runtime_error(format!(
                "signal server process through pidfd: {error}"
            )));
        }
        Ok(true)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (pid, boot_id, start_ticks, signal_number);
        Err(py_runtime_error(
            "pidfd server process control requires Linux",
        ))
    }
}

#[pyfunction]
#[pyo3(signature = (uri=None, *, config=None, endpoint_explicit=false))]
fn resolve_server_control_endpoint(
    uri: Option<String>,
    config: Option<String>,
    endpoint_explicit: bool,
) -> PyResult<String> {
    let requested = uri.unwrap_or_else(|| DEFAULT_CONTROL_ENDPOINT_URI.to_string());
    let mut endpoint = resolve_control_endpoint_value(&requested)?;
    let runtime_config = match config {
        Some(path) => ZippyConfig::load_from_path(Some(Path::new(&path))),
        None => ZippyConfig::load_default(),
    }
    .map_err(|_| py_value_error("load server configuration failed"))?;
    if !endpoint_explicit {
        if let Some(configured) = runtime_config.master_endpoint() {
            endpoint = resolve_control_endpoint_value(&format!("tcp://{configured}"))?;
        }
    }
    Ok(endpoint.display_string())
}

#[pyfunction]
#[pyo3(signature = (
    uri=None,
    *,
    control_endpoint=None,
    config=None,
    ready_file=None,
    launch_nonce=None,
    log_dir=None,
    startup_timeout_sec=None,
    endpoint_explicit=false
))]
#[allow(clippy::too_many_arguments)]
fn run_server_daemon(
    py: Python<'_>,
    uri: Option<String>,
    control_endpoint: Option<String>,
    config: Option<String>,
    ready_file: Option<String>,
    launch_nonce: Option<String>,
    log_dir: Option<String>,
    startup_timeout_sec: Option<f64>,
    endpoint_explicit: bool,
) -> PyResult<()> {
    let control_endpoint = resolve_uri_argument(uri, control_endpoint)?;
    let control_endpoint = prepare_control_endpoint(&control_endpoint)?;
    let mut daemon_config = MasterDaemonConfig::new(control_endpoint);
    if endpoint_explicit {
        daemon_config = daemon_config.with_explicit_control_endpoint();
    }
    if let Some(config) = config {
        daemon_config = daemon_config.with_config_path(PathBuf::from(config));
    }
    if let Some(log_dir) = log_dir {
        daemon_config.log_dir = PathBuf::from(log_dir);
    }
    let mut runtime_config = ServerRuntimeConfig::new(daemon_config);
    if let Some(startup_timeout_sec) = startup_timeout_sec {
        runtime_config =
            runtime_config.with_startup_timeout(parse_startup_timeout(startup_timeout_sec)?);
    }
    match (ready_file, launch_nonce) {
        (Some(ready_file), Some(launch_nonce)) => {
            runtime_config = runtime_config.with_ready_file(ready_file, launch_nonce);
        }
        (None, None) => {}
        _ => {
            return Err(py_value_error(
                "ready_file and launch_nonce must be set together".to_string(),
            ));
        }
    }
    match py.allow_threads(move || run_rust_server_daemon(runtime_config)) {
        Ok(()) => Ok(()),
        Err(error) => {
            let code = error.code().as_str();
            Err(py_runtime_error_with_code(py, error.to_string(), code)?)
        }
    }
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

/// Parses a bar profile object, or anything exposing `to_bar_generator_spec()`, into a spec.
fn parse_bar_profile(profile: &Bound<'_, PyAny>) -> PyResult<RustBarGeneratorSpec> {
    let raw_profile = if profile.hasattr("to_bar_generator_spec")? {
        profile.call_method0("to_bar_generator_spec")?
    } else {
        profile.clone()
    };
    parse_bar_generator_spec(&py_to_json_value_at(&raw_profile, "profile")?)
}

fn py_to_json_value_at(value: &Bound<'_, PyAny>, path: &str) -> PyResult<serde_json::Value> {
    if value.is_none() {
        return Ok(serde_json::Value::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(serde_json::Value::Bool(value.extract::<bool>()?));
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(serde_json::Value::String(text));
    }
    if let Ok(integer) = value.extract::<i64>() {
        return Ok(serde_json::Value::Number(integer.into()));
    }
    if let Ok(number) = value.extract::<f64>() {
        let Some(number) = serde_json::Number::from_f64(number) else {
            return Err(py_value_error(format!(
                "bar profile field=[{path}] contains non-finite float"
            )));
        };
        return Ok(serde_json::Value::Number(number));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut object = serde_json::Map::new();
        for (key, item) in dict.iter() {
            let key = key.extract::<String>().map_err(|_| {
                py_value_error(format!(
                    "bar profile field=[{path}] dict keys must be strings"
                ))
            })?;
            let item_path = format!("{path}.{key}");
            object.insert(key, py_to_json_value_at(&item, &item_path)?);
        }
        return Ok(serde_json::Value::Object(object));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        return list
            .iter()
            .enumerate()
            .map(|(index, item)| py_to_json_value_at(&item, &format!("{path}[{index}]")))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array);
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        return tuple
            .iter()
            .enumerate()
            .map(|(index, item)| py_to_json_value_at(&item, &format!("{path}[{index}]")))
            .collect::<PyResult<Vec<_>>>()
            .map(serde_json::Value::Array);
    }

    Err(py_value_error(format!(
        concat!(
            "bar profile field=[{}] must be dict, list, tuple, str, int, float, bool, ",
            "or None"
        ),
        path
    )))
}

fn parse_bar_generator_spec(value: &serde_json::Value) -> PyResult<RustBarGeneratorSpec> {
    let object = json_object(value, "profile")?;
    let frequency = json_required_string(object, "profile", "frequency")?;
    let columns = parse_bar_input_columns(json_required_field(object, "profile", "columns")?)?;
    let sessions = parse_bar_session_spec(json_required_field(object, "profile", "sessions")?)?;
    let volume_value = json_required_field(object, "profile", "volume")?;
    let volume = parse_bar_volume_spec(volume_value)?;
    let volume_output_dtype = parse_bar_volume_output_dtype(volume_value)?;
    let auction = parse_bar_auction_policy(json_required_field(object, "profile", "auction")?)?;
    let dt_label = parse_bar_dt_label_policy(json_required_field(object, "profile", "dt_label")?)?;

    Ok(RustBarGeneratorSpec {
        frequency,
        columns,
        sessions,
        volume,
        volume_output_dtype,
        auction,
        dt_label,
    })
}

fn parse_bar_input_columns(value: &serde_json::Value) -> PyResult<RustBarInputColumns> {
    let object = json_object(value, "columns")?;
    Ok(RustBarInputColumns {
        instrument: json_required_string(object, "columns", "instrument")?,
        dt: json_required_string(object, "columns", "dt")?,
        price: json_required_string(object, "columns", "price")?,
        volume: json_required_string(object, "columns", "volume")?,
        total_turnover: json_required_string(object, "columns", "total_turnover")?,
        trading_day: json_optional_string(object, "columns", "trading_day")?,
        num_trades: json_optional_string(object, "columns", "num_trades")?,
        limit_up: json_optional_string(object, "columns", "limit_up")?,
        limit_down: json_optional_string(object, "columns", "limit_down")?,
    })
}

fn parse_bar_session_spec(value: &serde_json::Value) -> PyResult<RustBarSessionSpec> {
    let object = json_object(value, "sessions")?;
    let auction = match object.get("auction") {
        Some(value) => parse_session_windows(value, "sessions.auction")?,
        None => Vec::new(),
    };
    Ok(RustBarSessionSpec {
        timezone: json_required_string(object, "sessions", "timezone")?,
        regular: parse_session_windows(
            json_required_field(object, "sessions", "regular")?,
            "sessions.regular",
        )?,
        auction,
    })
}

fn parse_session_windows(
    value: &serde_json::Value,
    path: &str,
) -> PyResult<Vec<RustSessionWindow>> {
    let serde_json::Value::Array(windows) = value else {
        return Err(py_value_error(format!(
            "bar profile field=[{path}] must be a list of session windows"
        )));
    };

    windows
        .iter()
        .enumerate()
        .map(|(index, window)| {
            let serde_json::Value::Array(bounds) = window else {
                return Err(py_value_error(format!(
                    "bar profile field=[{path}[{index}]] must be a two-item window"
                )));
            };
            if bounds.len() != 2 {
                return Err(py_value_error(format!(
                    "bar profile field=[{path}[{index}]] must contain start and end"
                )));
            }
            let start = json_string_value(&bounds[0], &format!("{path}[{index}][0]"))?;
            let end = json_string_value(&bounds[1], &format!("{path}[{index}][1]"))?;
            RustSessionWindow::parse(&start, &end).map_err(|error| {
                py_value_error(format!(
                    "bar profile field=[{path}[{index}]] invalid session window: {error}"
                ))
            })
        })
        .collect()
}

fn parse_bar_volume_spec(value: &serde_json::Value) -> PyResult<RustVolumeSpec> {
    let object = json_object(value, "volume")?;
    let mode = json_required_string(object, "volume", "mode")?;
    match mode.as_str() {
        "delta" => Ok(RustVolumeSpec::Delta),
        "cumulative" => {
            let trading_day_column = json_required_string(object, "volume", "trading_day_column")?;
            let bootstrap = match json_required_string(object, "volume", "bootstrap")?.as_str() {
                "skip_first_delta" => RustBootstrapPolicy::SkipFirstDelta,
                "from_zero" => RustBootstrapPolicy::FromZero,
                value => {
                    return Err(py_value_error(format!(
                        "volume.bootstrap must be skip_first_delta or from_zero value=[{value}]"
                    )));
                }
            };
            Ok(RustVolumeSpec::Cumulative {
                trading_day_column,
                bootstrap,
            })
        }
        value => Err(py_value_error(format!(
            "volume.mode must be delta or cumulative value=[{value}]"
        ))),
    }
}

fn parse_bar_volume_output_dtype(value: &serde_json::Value) -> PyResult<RustVolumeOutputDtype> {
    let object = json_object(value, "volume")?;
    match json_optional_string(object, "volume", "output_dtype")?.as_deref() {
        None | Some("int64") => Ok(RustVolumeOutputDtype::Int64),
        Some("float64") => Ok(RustVolumeOutputDtype::Float64),
        Some(value) => Err(py_value_error(format!(
            "volume.output_dtype must be int64 or float64 value=[{value}]"
        ))),
    }
}

fn parse_bar_auction_policy(value: &serde_json::Value) -> PyResult<RustAuctionPolicy> {
    let policy = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Object(object) => object
            .get("policy")
            .or_else(|| object.get("mode"))
            .ok_or_else(|| py_value_error("bar profile missing field=[auction.policy]"))
            .and_then(|value| json_string_value(value, "auction.policy"))?,
        _ => {
            return Err(py_value_error(
                "auction must be a string or object containing policy/mode",
            ));
        }
    };

    match policy.as_str() {
        "drop" => Ok(RustAuctionPolicy::Drop),
        "merge_to_first_regular_bar" => Ok(RustAuctionPolicy::MergeToFirstRegularBar),
        "emit_separate_bar" => Ok(RustAuctionPolicy::EmitSeparateBar),
        value => Err(py_value_error(format!(
            concat!(
                "auction must be drop, merge_to_first_regular_bar, or emit_separate_bar ",
                "value=[{}]"
            ),
            value
        ))),
    }
}

fn parse_bar_dt_label_policy(value: &serde_json::Value) -> PyResult<RustDtLabelPolicy> {
    let value = json_string_value(value, "dt_label")?;
    match value.as_str() {
        "close_dt" => Ok(RustDtLabelPolicy::CloseDt),
        "start_dt" => Ok(RustDtLabelPolicy::StartDt),
        value => Err(py_value_error(format!(
            "dt_label must be close_dt or start_dt value=[{value}]"
        ))),
    }
}

fn json_object<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> PyResult<&'a serde_json::Map<String, serde_json::Value>> {
    match value {
        serde_json::Value::Object(object) => Ok(object),
        _ => Err(py_value_error(format!(
            "bar profile field=[{path}] must be an object"
        ))),
    }
}

fn json_required_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    path: &str,
    field: &str,
) -> PyResult<&'a serde_json::Value> {
    object
        .get(field)
        .ok_or_else(|| py_value_error(format!("bar profile missing field=[{path}.{field}]")))
}

fn json_required_string(
    object: &serde_json::Map<String, serde_json::Value>,
    path: &str,
    field: &str,
) -> PyResult<String> {
    json_string_value(
        json_required_field(object, path, field)?,
        &format!("{path}.{field}"),
    )
}

fn json_optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    path: &str,
    field: &str,
) -> PyResult<Option<String>> {
    match object.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => json_string_value(value, &format!("{path}.{field}")).map(Some),
    }
}

fn json_string_value(value: &serde_json::Value, path: &str) -> PyResult<String> {
    match value {
        serde_json::Value::String(value) if !value.is_empty() => Ok(value.clone()),
        serde_json::Value::String(_) => Err(py_value_error(format!(
            "bar profile field=[{path}] must not be empty"
        ))),
        _ => Err(py_value_error(format!(
            "bar profile field=[{path}] must be a string"
        ))),
    }
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

fn parse_reactive_state_failure_policy(value: &str) -> PyResult<RustReactiveStateFailurePolicy> {
    match value {
        "fail_fast" => Ok(RustReactiveStateFailurePolicy::FailFast),
        "rollback" => Ok(RustReactiveStateFailurePolicy::Rollback),
        _ => Err(py_value_error(
            "state_failure_policy must be 'fail_fast' or 'rollback'",
        )),
    }
}

fn parse_reactive_invalid_value_policy(value: &str) -> PyResult<RustReactiveInvalidValuePolicy> {
    match value {
        "reject" => Ok(RustReactiveInvalidValuePolicy::Reject),
        "skip" => Ok(RustReactiveInvalidValuePolicy::Skip),
        _ => Err(py_value_error(
            "invalid_value_policy must be 'reject' or 'skip'",
        )),
    }
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
        let built = build_reactive_spec(id_column, current_schema.as_ref(), factor.bind(py))?;
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
    id_column: &str,
    current_schema: &Schema,
    factor: &Bound<'_, PyAny>,
) -> PyResult<Box<dyn zippy_operators::ReactiveFactor>> {
    if let Ok(spec) = factor.extract::<PyRef<'_, TsEmaSpec>>() {
        return RustTsEmaSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.span,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsReturnSpec>>() {
        return RustTsReturnSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.period,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsMeanSpec>>() {
        return RustTsMeanSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.window,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsStdSpec>>() {
        return RustTsStdSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.window,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsDelaySpec>>() {
        return RustTsDelaySpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.period,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, TsDiffSpec>>() {
        return RustTsDiffSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.period,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, AbsSpec>>() {
        return RustAbsSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, LogSpec>>() {
        return RustLogSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, ClipSpec>>() {
        return RustClipSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            spec.min,
            spec.max,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, CastSpec>>() {
        return RustCastSpec::new(
            reactive_factor_id_column(id_column, &spec.id_column),
            &spec.value_column,
            &spec.dtype,
            &spec.output,
        )
        .build()
        .map_err(|error| py_value_error(error.to_string()));
    }

    if let Ok(spec) = factor.extract::<PyRef<'_, ExpressionFactor>>() {
        return RustExpressionSpec::new(&spec.expression, &spec.output)
            .build_reactive_factor(current_schema, id_column)
            .map_err(|error| py_value_error(error.to_string()));
    }

    Err(PyTypeError::new_err(
        "factors must contain TsEmaSpec, TsReturnSpec, TsMeanSpec, TsStdSpec, TsDelaySpec, TsDiffSpec, AbsSpec, LogSpec, ClipSpec, CastSpec, or ExpressionFactor",
    ))
}

fn reactive_factor_id_column<'a>(engine_id_column: &'a str, factor_id_column: &'a str) -> &'a str {
    if factor_id_column.is_empty() {
        engine_id_column
    } else {
        factor_id_column
    }
}

fn build_aggregation_specs(
    py: Python<'_>,
    factors: Vec<Py<PyAny>>,
) -> PyResult<Vec<Box<dyn RustAggregationSpec>>> {
    factors
        .into_iter()
        .map(|factor| build_aggregation_spec(factor.bind(py)))
        .collect()
}

fn build_aggregation_spec(factor: &Bound<'_, PyAny>) -> PyResult<Box<dyn RustAggregationSpec>> {
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
        .map(|factor| build_cross_sectional_spec(factor.bind(py)))
        .collect()
}

fn build_cross_sectional_spec(
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
        .map(|factor| build_expression_spec(factor.bind(py), parameter_name))
        .collect()
}

fn build_expression_spec(
    factor: &Bound<'_, PyAny>,
    parameter_name: &str,
) -> PyResult<RustExpressionSpec> {
    if let Ok(spec) = factor.extract::<PyRef<'_, ExpressionFactor>>() {
        return Ok(RustExpressionSpec::new(&spec.expression, &spec.output));
    }

    Err(PyTypeError::new_err(format!(
        "{parameter_name} must contain ExpressionFactor"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::Once;
    static PYTHON_TEST_INIT: Once = Once::new();
    #[test]
    fn server_runtime_error_exposes_stable_error_code() {
        prepare_python_for_tests();
        Python::with_gil(|py| {
            let error =
                py_runtime_error_with_code(py, "native storage failed", "DATA_CORRUPTION").unwrap();
            let code = error
                .value_bound(py)
                .getattr("zippy_error_code")
                .unwrap()
                .extract::<String>()
                .unwrap();

            assert_eq!(code, "DATA_CORRUPTION");
        });
    }

    fn prepare_python_for_tests() {
        PYTHON_TEST_INIT.call_once(|| {
            if std::env::var_os("PYTHONHOME").is_none() {
                if let Some(home) = python_base_prefix_for_tests() {
                    std::env::set_var("PYTHONHOME", home);
                }
            }
            pyo3::prepare_freethreaded_python();
        });
    }

    fn python_base_prefix_for_tests() -> Option<String> {
        let python = std::env::var_os("PYO3_PYTHON")?;
        let output = Command::new(python)
            .arg("-c")
            .arg("import sys; print(sys.base_prefix)")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let home = String::from_utf8(output.stdout).ok()?.trim().to_string();
        (!home.is_empty()).then_some(home)
    }
}
