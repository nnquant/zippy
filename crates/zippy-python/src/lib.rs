#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use zippy_core::{
    spawn_engine_with_publisher, Engine, EngineConfig, EngineHandle, Publisher as CorePublisher,
};
use zippy_engines::ReactiveStateEngine as RustReactiveStateEngine;
use zippy_io::{
    FanoutPublisher as RustFanoutPublisher, NullPublisher as RustNullPublisher,
    ZmqPublisher as RustZmqPublisher,
};
use zippy_operators::TsEmaSpec as RustTsEmaSpec;

fn py_value_error(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn py_runtime_error(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

#[derive(Clone)]
enum TargetConfig {
    Null,
    Zmq { endpoint: String },
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
struct ReactiveStateEngine {
    name: String,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    handle: Option<EngineHandle>,
    engine: Option<RustReactiveStateEngine>,
}

#[pymethods]
impl ReactiveStateEngine {
    #[new]
    #[pyo3(signature = (name, input_schema, id_column, factors, target))]
    fn new(
        py: Python<'_>,
        name: String,
        input_schema: &Bound<'_, PyAny>,
        id_column: String,
        factors: Vec<Py<TsEmaSpec>>,
        target: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let schema = Arc::new(
            Schema::from_pyarrow_bound(input_schema)
                .map_err(|error| py_value_error(error.to_string()))?,
        );
        let factor_specs = factors
            .into_iter()
            .map(|factor| {
                let factor_ref = factor.borrow(py);
                RustTsEmaSpec::new(
                    &factor_ref.id_column,
                    &factor_ref.value_column,
                    factor_ref.span,
                    &factor_ref.output,
                )
                .build()
                .map_err(|error| py_value_error(error.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let engine = RustReactiveStateEngine::new(&name, Arc::clone(&schema), factor_specs)
            .map_err(|error| py_value_error(error.to_string()))?;
        let output_schema = engine.output_schema();
        let target = parse_targets(target)?;

        let _ = id_column;

        Ok(Self {
            name,
            input_schema: schema,
            output_schema,
            target,
            handle: None,
            engine: Some(engine),
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let engine = match self.engine.take() {
            Some(engine) => engine,
            None => return Err(py_runtime_error("engine already started")),
        };
        let publisher = build_publisher(&self.target)?;
        let handle = spawn_engine_with_publisher(
            engine,
            EngineConfig {
                name: self.name.clone(),
                buffer_capacity: 1024,
                overflow_policy: Default::default(),
                late_data_policy: Default::default(),
            },
            publisher,
        )
        .map_err(|error| py_runtime_error(error.to_string()))?;
        self.handle = Some(handle);
        Ok(())
    }

    fn write(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let handle = match self.handle.as_ref() {
            Some(handle) => handle,
            None => return Err(py_runtime_error("engine not started")),
        };
        let batches = value_to_record_batches(py, value, &self.input_schema)?;

        for batch in batches {
            handle
                .write(batch)
                .map_err(|error| py_runtime_error(error.to_string()))?;
        }

        Ok(())
    }

    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|error| py_value_error(error.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        let handle = match self.handle.as_ref() {
            Some(handle) => handle,
            None => return Err(py_runtime_error("engine not started")),
        };

        handle
            .flush()
            .map(|_| ())
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    fn stop(&mut self) -> PyResult<()> {
        let mut handle = match self.handle.take() {
            Some(handle) => handle,
            None => return Err(py_runtime_error("engine not started")),
        };
        handle
            .stop()
            .map_err(|error| py_runtime_error(error.to_string()))
    }
}

#[pymodule]
fn _internal(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TsEmaSpec>()?;
    module.add_class::<NullPublisher>()?;
    module.add_class::<ZmqPublisher>()?;
    module.add_class::<ReactiveStateEngine>()?;
    Ok(())
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

fn build_publisher(targets: &[TargetConfig]) -> PyResult<Box<dyn CorePublisher>> {
    let mut publishers = Vec::<Box<dyn CorePublisher>>::with_capacity(targets.len());

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

    if publishers.len() == 1 {
        return Ok(publishers.pop().expect("single publisher checked above"));
    }

    Ok(Box::new(RustFanoutPublisher::new(publishers)))
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
