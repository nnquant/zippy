#![allow(clippy::useless_conversion)]
#![forbid(unsafe_code)]

use std::hint::spin_loop;
use std::time::Duration;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan,
    SegmentControlSnapshot,
};

const CLIENT_SHM_PROTOCOL_VERSION: u32 = 1;

#[pyclass]
struct ShmReader {
    schema: CompiledSchema,
    reader: ActiveSegmentReader,
}

#[pymethods]
impl ShmReader {
    #[new]
    #[pyo3(signature = (descriptor_json, schema_json, start_from=-1))]
    fn new(descriptor_json: &str, schema_json: &str, start_from: i64) -> PyResult<Self> {
        if start_from < -1 {
            return Err(PyValueError::new_err(
                "start_from must be -1 or a non-negative row offset",
            ));
        }
        let descriptor = parse_json(descriptor_json, "descriptor")?;
        let schema_metadata = parse_json(schema_json, "schema")?;
        let schema = compile_stream_schema(&schema_metadata)?;
        let mut reader = build_reader(&descriptor, &schema)?;
        if start_from == -1 {
            reader.seek_to_committed().map_err(segment_runtime_error)?;
        } else {
            reader
                .seek_to_row_offset(start_from as usize)
                .map_err(segment_runtime_error)?;
        }
        Ok(Self { schema, reader })
    }

    fn read_available(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let span = self
            .reader
            .read_available()
            .map_err(segment_runtime_error)?;
        span.map(|span| {
            let batch = span
                .as_record_batch()
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
            record_batch_to_pyarrow(py, batch)
        })
        .transpose()
    }

    fn committed_row_count(&self) -> PyResult<usize> {
        self.reader
            .committed_row_count()
            .map_err(segment_runtime_error)
    }

    fn is_sealed(&self) -> PyResult<bool> {
        self.reader.is_sealed().map_err(segment_runtime_error)
    }

    fn notification_sequence(&self) -> PyResult<u32> {
        self.reader
            .notification_sequence()
            .map_err(segment_runtime_error)
    }

    #[pyo3(signature = (observed, timeout_ms, idle_spin_checks=64))]
    fn wait_for_notification_after(
        &self,
        py: Python<'_>,
        observed: u32,
        timeout_ms: u64,
        idle_spin_checks: u32,
    ) -> PyResult<bool> {
        py.allow_threads(|| {
            if spin_notification_changed(&self.reader, observed, idle_spin_checks)? {
                return Ok(true);
            }
            self.reader
                .wait_for_notification_after(observed, Duration::from_millis(timeout_ms))
                .map_err(segment_runtime_error)
        })
    }

    fn spin_for_notification_after(
        &self,
        py: Python<'_>,
        observed: u32,
        checks: u32,
    ) -> PyResult<bool> {
        py.allow_threads(|| spin_notification_changed(&self.reader, observed, checks))
    }

    fn try_rebind(
        &mut self,
        py: Python<'_>,
        descriptor_json: &str,
    ) -> PyResult<(bool, Option<PyObject>)> {
        if let Some(batch) = read_available_batch(&mut self.reader)? {
            return Ok((false, Some(record_batch_to_pyarrow(py, batch)?)));
        }
        if !self.reader.is_sealed().map_err(segment_runtime_error)? {
            return Ok((false, None));
        }
        // Recheck after observing seal so a row committed between the first empty read and seal
        // observation is delivered before descriptor replacement.
        if let Some(batch) = read_available_batch(&mut self.reader)? {
            return Ok((false, Some(record_batch_to_pyarrow(py, batch)?)));
        }
        let descriptor = parse_json(descriptor_json, "descriptor")?;
        let mut replacement = build_reader(&descriptor, &self.schema)?;
        replacement
            .seek_to_row_offset(0)
            .map_err(segment_runtime_error)?;
        self.reader = replacement;
        Ok((true, None))
    }

    fn control_snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let snapshot = self
            .reader
            .control_snapshot()
            .map_err(segment_runtime_error)?;
        snapshot_to_dict(py, snapshot)
    }
}

fn read_available_batch(reader: &mut ActiveSegmentReader) -> PyResult<Option<RecordBatch>> {
    reader
        .read_available()
        .map_err(segment_runtime_error)?
        .map(|span| {
            span.as_record_batch()
                .map_err(|error| PyRuntimeError::new_err(error.to_string()))
        })
        .transpose()
}

fn spin_notification_changed(
    reader: &ActiveSegmentReader,
    observed: u32,
    checks: u32,
) -> PyResult<bool> {
    for _ in 0..checks {
        spin_loop();
        let current = reader
            .notification_sequence()
            .map_err(segment_runtime_error)?;
        if current != observed {
            return Ok(true);
        }
    }
    Ok(false)
}

fn build_reader(
    descriptor: &serde_json::Value,
    schema: &CompiledSchema,
) -> PyResult<ActiveSegmentReader> {
    let row_capacity = descriptor
        .get("row_capacity")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| PyValueError::new_err("segment descriptor missing row_capacity"))?;
    let row_capacity = usize::try_from(row_capacity)
        .map_err(|_| PyValueError::new_err("segment descriptor row_capacity overflows usize"))?;
    let layout = LayoutPlan::for_schema(schema, row_capacity).map_err(PyValueError::new_err)?;
    let envelope =
        serde_json::to_vec(descriptor).map_err(|error| PyValueError::new_err(error.to_string()))?;
    ActiveSegmentReader::from_descriptor_envelope(&envelope, schema.clone(), layout)
        .map_err(segment_runtime_error)
}

fn compile_stream_schema(metadata: &serde_json::Value) -> PyResult<CompiledSchema> {
    let fields = metadata
        .get("fields")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| PyValueError::new_err("stream schema metadata missing fields"))?;
    let columns = fields
        .iter()
        .map(|field| {
            let name = field
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| PyValueError::new_err("stream schema field missing name"))?;
            let segment_type = field
                .get("segment_type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| PyValueError::new_err("stream schema field missing segment_type"))?;
            let nullable = field
                .get("nullable")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| PyValueError::new_err("stream schema field missing nullable"))?;
            let timezone = field.get("timezone").and_then(serde_json::Value::as_str);
            let data_type = segment_column_type(segment_type, timezone)?;
            Ok(if nullable {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    compile_schema(&columns).map_err(PyValueError::new_err)
}

fn segment_column_type(segment_type: &str, timezone: Option<&str>) -> PyResult<ColumnType> {
    match segment_type {
        "int64" => Ok(ColumnType::Int64),
        "float64" => Ok(ColumnType::Float64),
        "utf8" => Ok(ColumnType::Utf8),
        "timestamp_ns_tz" => {
            let timezone = timezone.ok_or_else(|| {
                PyValueError::new_err("timestamp_ns_tz stream schema field missing timezone")
            })?;
            Ok(ColumnType::timestamp_ns_tz(timezone))
        }
        "timestamp_ns" => Err(PyValueError::new_err(
            "segment stream timestamp columns must include an explicit timezone",
        )),
        other => Err(PyValueError::new_err(format!(
            "unsupported stream field type segment_type=[{other}]"
        ))),
    }
}

fn parse_json(value: &str, name: &str) -> PyResult<serde_json::Value> {
    serde_json::from_str(value)
        .map_err(|error| PyValueError::new_err(format!("invalid {name} JSON error=[{error}]")))
}

fn record_batch_to_pyarrow(py: Python<'_>, batch: RecordBatch) -> PyResult<PyObject> {
    batch
        .to_pyarrow(py)
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

fn snapshot_to_dict(py: Python<'_>, snapshot: SegmentControlSnapshot) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("magic", snapshot.magic)?;
    dict.set_item("layout_version", snapshot.layout_version)?;
    dict.set_item("schema_id", snapshot.schema_id)?;
    dict.set_item("segment_id", snapshot.segment_id)?;
    dict.set_item("generation", snapshot.generation)?;
    dict.set_item("writer_epoch", snapshot.writer_epoch)?;
    dict.set_item("descriptor_generation", snapshot.descriptor_generation)?;
    dict.set_item("capacity_rows", snapshot.capacity_rows)?;
    dict.set_item("row_count", snapshot.row_count)?;
    dict.set_item("committed_row_count", snapshot.committed_row_count)?;
    dict.set_item("notify_seq", snapshot.notify_seq)?;
    dict.set_item("waiter_count", snapshot.waiter_count)?;
    dict.set_item("sealed", snapshot.sealed)?;
    Ok(dict.into_any().unbind())
}

fn segment_runtime_error(error: zippy_segment_store::ZippySegmentStoreError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

#[pymodule]
fn _shm(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add("PROTOCOL_VERSION", CLIENT_SHM_PROTOCOL_VERSION)?;
    module.add_class::<ShmReader>()?;
    Ok(())
}
