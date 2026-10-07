//! Synchronous Arrow execution of the existing factor engines without a legacy runtime.

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use zippy_core::{Engine, SegmentTableView};
use zippy_engines::{
    BarGeneratorEngine, CrossSectionalEngine, EngineChain, IdentityEngine, ReactiveLatestEngine,
    ReactiveStateEngine, TimeSeriesEngine,
};

use super::{
    build_aggregation_specs, build_cross_sectional_specs, build_expression_specs,
    build_reactive_specs, parse_bar_profile, parse_late_data_policy,
    parse_reactive_invalid_value_policy, parse_reactive_state_failure_policy, py_runtime_error,
    py_value_error,
};

/// Own one factor kernel, with no source registry, SHM publisher or background worker.
///
/// :note: A kernel failure is terminal because not every stateful kernel can roll back.
/// :example: processor = NativeProcessor.latest("latest", schema, ["symbol"])
#[pyclass]
pub(crate) struct NativeProcessor {
    engine: Option<Box<dyn Engine>>,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    failed: bool,
}

impl NativeProcessor {
    fn new(engine: impl Engine) -> Self {
        Self {
            input_schema: engine.input_schema(),
            output_schema: engine.output_schema(),
            engine: Some(Box::new(engine)),
            failed: false,
        }
    }

    fn execute(
        &mut self,
        py: Python<'_>,
        operation: impl FnOnce(&mut dyn Engine) -> zippy_core::Result<Vec<SegmentTableView>> + Send,
    ) -> PyResult<Vec<PyObject>> {
        let engine = self.engine.as_mut().ok_or_else(|| {
            py_runtime_error(if self.failed {
                "processor has failed"
            } else {
                "processor is closed"
            })
        })?;
        let result = py
            .allow_threads(|| {
                operation(engine.as_mut())?
                    .into_iter()
                    .map(|table| table.to_record_batch())
                    .collect::<zippy_core::Result<Vec<_>>>()
            })
            .map_err(|error| py_runtime_error(error.to_string()))
            .and_then(|batches| {
                batches
                    .into_iter()
                    .map(|batch| {
                        batch
                            .to_pyarrow(py)
                            .map_err(|error| py_runtime_error(error.to_string()))
                    })
                    .collect::<PyResult<Vec<PyObject>>>()
            });
        if result.is_err() {
            self.engine = None;
            self.failed = true;
        }
        result
    }
}

fn schema(value: &Bound<'_, PyAny>) -> PyResult<Arc<Schema>> {
    Schema::from_pyarrow_bound(value)
        .map(Arc::new)
        .map_err(|error| py_value_error(error.to_string()))
}

fn validate_id_filter(values: Option<&[String]>) -> PyResult<()> {
    if let Some(value) = values
        .into_iter()
        .flatten()
        .find(|value| value.trim().is_empty())
    {
        return Err(py_value_error(format!(
            "id_filter contains empty value value=[{value}]"
        )));
    }
    Ok(())
}

#[pymethods]
impl NativeProcessor {
    /// Create a plain Arrow producer without filtering, deduplication or field requirements.
    /// :param name: Diagnostic name.
    /// :param input_schema: Exact input and output schema.
    /// :returns: Processor preserving complete input batches unchanged.
    #[staticmethod]
    fn identity(name: &str, input_schema: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self::new(IdentityEngine::new(name, schema(input_schema)?)))
    }

    /// Transfer existing kernels into one ordered native chain after validating all links.
    /// :param name: Diagnostic name.
    /// :param processors: Nonempty ordered list of distinct, active processor owners.
    /// :param max_intermediate_bytes: Maximum estimated bytes per stage output collection.
    /// :returns: Exclusive processor owning all current stage states.
    /// :raises ValueError: Adjacent schemas or the byte limit are invalid, without transferring.
    /// :raises RuntimeError: An owner is closed, failed or repeated, without transferring.
    #[staticmethod]
    #[pyo3(signature = (name, processors, *, max_intermediate_bytes=67_108_864))]
    fn chain(
        py: Python<'_>,
        name: &str,
        processors: Vec<Py<NativeProcessor>>,
        max_intermediate_bytes: usize,
    ) -> PyResult<Self> {
        let mut owners = processors
            .iter()
            .map(|processor| {
                processor.try_borrow_mut(py).map_err(|_| {
                    py_runtime_error("chain processors require distinct unborrowed owners")
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let stages = owners
            .iter()
            .map(|owner| {
                owner
                    .engine
                    .as_deref()
                    .ok_or_else(|| py_runtime_error("chain processor is not active"))
            })
            .collect::<PyResult<Vec<_>>>()?;
        EngineChain::validate(&stages, max_intermediate_bytes)
            .map_err(|error| py_value_error(error.to_string()))?;
        drop(stages);
        let engines = owners
            .iter_mut()
            .map(|owner| owner.engine.take().expect("validated active owner"))
            .collect();
        EngineChain::new(name, engines, max_intermediate_bytes)
            .map(Self::new)
            .map_err(|error| py_value_error(error.to_string()))
    }

    /// Create a reactive factor kernel using the shared Rust expression compiler.
    ///
    /// :param name: Diagnostic engine name.
    /// :param input_schema: Exact Arrow input schema; no implicit casts.
    /// :param id_column: Instrument state key.
    /// :param factors: Existing Zippy factor/expression specs in output order.
    /// :param id_filter: Optional exact instrument whitelist.
    /// :param state_failure_policy: fail_fast or rollback.
    /// :param invalid_value_policy: reject or the engine's supported explicit policy.
    /// :returns: A single-owner processor.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, factors, *, id_filter=None, state_failure_policy="fail_fast", invalid_value_policy="reject"))]
    fn reactive(
        py: Python<'_>,
        name: &str,
        input_schema: &Bound<'_, PyAny>,
        id_column: &str,
        factors: Vec<Py<PyAny>>,
        id_filter: Option<Vec<String>>,
        state_failure_policy: &str,
        invalid_value_policy: &str,
    ) -> PyResult<Self> {
        validate_id_filter(id_filter.as_deref())?;
        let schema = schema(input_schema)?;
        let specs = build_reactive_specs(py, &schema, id_column, factors)?;
        ReactiveStateEngine::new_with_id_filter_state_failure_and_invalid_value_policy(
            name,
            schema,
            specs,
            id_column,
            id_filter,
            parse_reactive_state_failure_policy(state_failure_policy)?,
            parse_reactive_invalid_value_policy(invalid_value_policy)?,
        )
        .map(Self::new)
        .map_err(|error| py_value_error(error.to_string()))
    }

    /// Create the existing latest-per-key kernel; each result is an update, not a full snapshot.
    ///
    /// :param name: Diagnostic name.
    /// :param input_schema: Exact input schema.
    /// :param by: Ordered nonempty key columns.
    /// :returns: Latest-state update processor.
    #[staticmethod]
    fn latest(name: &str, input_schema: &Bound<'_, PyAny>, by: Vec<String>) -> PyResult<Self> {
        ReactiveLatestEngine::new(name, schema(input_schema)?, by)
            .map(Self::new)
            .map_err(|error| py_value_error(error.to_string()))
    }

    /// Create fixed-window time-series aggregation with optional pre/post expressions.
    ///
    /// :param name: Diagnostic name.
    /// :param input_schema: Exact Arrow input schema.
    /// :param id_column: Instrument key.
    /// :param dt_column: UTC nanosecond timestamp column.
    /// :param window_ns: Positive window width in nanoseconds.
    /// :param factors: Ordered aggregation specs.
    /// :param late_data_policy: Explicit reject or supported late-row policy.
    /// :param pre_factors: Expressions before aggregation.
    /// :param post_factors: Expressions after aggregation.
    /// :param id_filter: Optional exact instrument whitelist.
    /// :returns: Stateful time-series processor.
    #[staticmethod]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (name, input_schema, id_column, dt_column, window_ns, factors, *, late_data_policy="reject", pre_factors=None, post_factors=None, id_filter=None))]
    fn timeseries(
        py: Python<'_>,
        name: &str,
        input_schema: &Bound<'_, PyAny>,
        id_column: &str,
        dt_column: &str,
        window_ns: i64,
        factors: Vec<Py<PyAny>>,
        late_data_policy: &str,
        pre_factors: Option<Vec<Py<PyAny>>>,
        post_factors: Option<Vec<Py<PyAny>>>,
        id_filter: Option<Vec<String>>,
    ) -> PyResult<Self> {
        validate_id_filter(id_filter.as_deref())?;
        TimeSeriesEngine::new_with_id_filter(
            name,
            schema(input_schema)?,
            id_column,
            dt_column,
            window_ns,
            parse_late_data_policy(late_data_policy)?,
            build_aggregation_specs(py, factors)?,
            build_expression_specs(py, pre_factors.unwrap_or_default(), "pre_factors")?,
            build_expression_specs(py, post_factors.unwrap_or_default(), "post_factors")?,
            id_filter,
        )
        .map(Self::new)
        .map_err(|error| py_value_error(error.to_string()))
    }

    /// Create the shared complete-bucket cross-sectional kernel.
    ///
    /// :param name: Diagnostic name.
    /// :param input_schema: Exact Arrow input schema.
    /// :param id_column: Cross-section member key.
    /// :param dt_column: UTC nanosecond timestamp column.
    /// :param trigger_interval_ns: Positive bucket width in nanoseconds.
    /// :param factors: Ordered cross-sectional specs.
    /// :returns: Processor emitting complete cross-sections.
    #[staticmethod]
    fn cross_sectional(
        py: Python<'_>,
        name: &str,
        input_schema: &Bound<'_, PyAny>,
        id_column: &str,
        dt_column: &str,
        trigger_interval_ns: i64,
        factors: Vec<Py<PyAny>>,
    ) -> PyResult<Self> {
        CrossSectionalEngine::new(
            name,
            schema(input_schema)?,
            id_column,
            dt_column,
            trigger_interval_ns,
            parse_late_data_policy("reject")?,
            build_cross_sectional_specs(py, factors)?,
        )
        .map(Self::new)
        .map_err(|error| py_value_error(error.to_string()))
    }

    /// Create the shared session-aware OHLCV kernel from an explicit trading profile.
    ///
    /// :param name: Diagnostic name.
    /// :param input_schema: Exact Arrow input schema.
    /// :param profile: Existing BarGenerator profile with explicit sessions and policies.
    /// :returns: Stateful bar processor.
    #[staticmethod]
    fn bars(
        name: &str,
        input_schema: &Bound<'_, PyAny>,
        profile: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let spec = parse_bar_profile(profile)?;
        BarGeneratorEngine::new(name, schema(input_schema)?, spec)
            .map(Self::new)
            .map_err(|error| py_value_error(error.to_string()))
    }

    /// Read the immutable input schema.
    /// :returns: Arrow schema.
    fn input_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.input_schema
            .to_pyarrow(py)
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    /// Read the immutable output schema.
    /// :returns: Arrow schema.
    fn output_schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        self.output_schema
            .to_pyarrow(py)
            .map_err(|error| py_runtime_error(error.to_string()))
    }

    /// Process exactly one Arrow RecordBatch with the GIL released during computation.
    /// :param batch: Complete input batch with the exact declared schema.
    /// :returns: Ordered Arrow output batches; empty while awaiting a bucket boundary.
    /// :raises ValueError: Input schema differs, before changing engine state.
    /// :raises RuntimeError: A kernel failed or the processor is no longer active.
    fn process(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Vec<PyObject>> {
        let batch = RecordBatch::from_pyarrow_bound(batch)
            .map_err(|error| py_value_error(error.to_string()))?;
        if batch.schema() != self.input_schema {
            return Err(py_value_error(
                "processor input schema differs; implicit casts are forbidden",
            ));
        }
        self.execute(py, move |engine| {
            engine.on_data(SegmentTableView::from_record_batch(batch))
        })
    }

    /// Explicitly flush pending windows without closing the kernel.
    /// :returns: Ordered final batches for currently buffered windows.
    /// :raises RuntimeError: Kernel execution failed or was already terminated.
    fn flush(&mut self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.execute(py, |engine| engine.on_flush())
    }

    /// Finish once, delivering remaining kernel outputs and closing computation.
    /// :returns: Final output batches; an empty list after an already successful finish.
    /// :raises RuntimeError: A failed processor cannot be finalized or reused.
    fn finish(&mut self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        if self.engine.is_none() && !self.failed {
            return Ok(Vec::new());
        }
        let result = self.execute(py, |engine| engine.on_stop());
        self.engine = None;
        result
    }

    /// Transfer the current kernel state to a new exclusive processor, closing this handle.
    /// :returns: The new owner of the exact current kernel state.
    /// :raises RuntimeError: This handle is already closed or failed.
    fn transfer(&mut self) -> PyResult<Self> {
        let engine = self
            .engine
            .take()
            .ok_or_else(|| py_runtime_error("processor is not active"))?;
        Ok(Self {
            engine: Some(engine),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            failed: false,
        })
    }

    /// Discard pending state without publishing any further output.
    /// :returns: None.
    fn close(&mut self) {
        self.engine = None;
    }
}
