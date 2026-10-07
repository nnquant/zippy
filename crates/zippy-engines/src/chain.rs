//! Ordered, single-owner engine composition without registry or intermediate publishers.

use std::mem::size_of;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

/// Feed complete outputs of each kernel into the next, preserving batch order and boundaries.
///
/// Flush/stop propagate upstream outputs before flushing/stopping downstream state. A failure
/// after execution begins is terminal: stages do not share a transactional checkpoint.
pub struct EngineChain {
    name: String,
    stages: Vec<Box<dyn Engine>>,
    input: SchemaRef,
    output: SchemaRef,
    stage_outputs: Vec<SchemaRef>,
    max_intermediate_bytes: usize,
    failed: bool,
    stopped: bool,
}

impl EngineChain {
    /// Check composition without consuming any stage ownership.
    pub fn validate(stages: &[&dyn Engine], max_intermediate_bytes: usize) -> Result<()> {
        if stages.is_empty() || max_intermediate_bytes == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "engine chain requires stages and a positive intermediate byte limit"
                    .into(),
            });
        }
        for (index, pair) in stages.windows(2).enumerate() {
            if pair[0].output_schema() != pair[1].input_schema() {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "engine chain schema differs stage=[{}] next=[{}]",
                        index,
                        index + 1
                    ),
                });
            }
        }
        Ok(())
    }

    /// Own validated stages, including their current in-process state.
    /// The byte limit applies to each stage's output collection and combined flush/stop output;
    /// it is not a bound on internal kernel state or transient allocation inside an engine call.
    pub fn new(
        name: impl Into<String>,
        stages: Vec<Box<dyn Engine>>,
        max_intermediate_bytes: usize,
    ) -> Result<Self> {
        Self::validate(
            &stages
                .iter()
                .map(|stage| stage.as_ref())
                .collect::<Vec<_>>(),
            max_intermediate_bytes,
        )?;
        let input = stages[0].input_schema();
        let output = stages[stages.len() - 1].output_schema();
        let stage_outputs = stages.iter().map(|stage| stage.output_schema()).collect();
        Ok(Self {
            name: name.into(),
            stages,
            input,
            output,
            stage_outputs,
            max_intermediate_bytes,
            failed: false,
            stopped: false,
        })
    }

    fn require_active(&self) -> Result<()> {
        if self.failed || self.stopped {
            return Err(ZippyError::InvalidState {
                status: "engine chain is failed or stopped",
            });
        }
        Ok(())
    }

    fn append_checked(
        &self,
        schema: &SchemaRef,
        output: &mut Vec<SegmentTableView>,
        bytes: &mut usize,
        batches: Vec<SegmentTableView>,
    ) -> Result<()> {
        for batch in batches {
            if batch.schema() != *schema {
                return Err(ZippyError::SchemaMismatch {
                    reason: "engine chain stage changed its declared output schema".into(),
                });
            }
            let size = batch.to_record_batch()?.get_array_memory_size();
            *bytes = bytes
                .checked_add(size)
                .and_then(|value| value.checked_add(size_of::<RecordBatch>()))
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "engine chain output byte count overflow".into(),
                })?;
            if *bytes > self.max_intermediate_bytes {
                return Err(ZippyError::InvalidConfig {
                    reason: "engine chain intermediate output exceeds byte limit".into(),
                });
            }
            output.push(batch);
        }
        Ok(())
    }

    fn route(
        &mut self,
        first_stage: usize,
        mut tables: Vec<SegmentTableView>,
    ) -> Result<Vec<SegmentTableView>> {
        for index in first_stage..self.stages.len() {
            let mut next = Vec::new();
            let mut bytes = 0;
            for table in tables {
                let emitted = self.stages[index].on_data(table)?;
                self.append_checked(&self.stage_outputs[index], &mut next, &mut bytes, emitted)?;
            }
            tables = next;
        }
        Ok(tables)
    }

    fn drain(&mut self, stop: bool) -> Result<Vec<SegmentTableView>> {
        let mut result = Vec::new();
        let mut bytes = 0;
        for index in 0..self.stages.len() {
            let emitted = if stop {
                self.stages[index].on_stop()?
            } else {
                self.stages[index].on_flush()?
            };
            let mut checked = Vec::new();
            let mut stage_bytes = 0;
            self.append_checked(
                &self.stage_outputs[index],
                &mut checked,
                &mut stage_bytes,
                emitted,
            )?;
            let output = self.route(index + 1, checked)?;
            self.append_checked(&self.output, &mut result, &mut bytes, output)?;
        }
        Ok(result)
    }

    fn complete(&mut self, result: Result<Vec<SegmentTableView>>) -> Result<Vec<SegmentTableView>> {
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl Engine for EngineChain {
    fn name(&self) -> &str {
        &self.name
    }
    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input)
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        self.require_active()?;
        if table.schema() != self.input {
            return Err(ZippyError::SchemaMismatch {
                reason: "engine chain input schema differs".into(),
            });
        }
        let result = self.route(0, vec![table]);
        self.complete(result)
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        self.require_active()?;
        let result = self.drain(false);
        self.complete(result)
    }

    fn on_stop(&mut self) -> Result<Vec<SegmentTableView>> {
        if self.stopped && !self.failed {
            return Ok(Vec::new());
        }
        self.require_active()?;
        let result = self.drain(true);
        self.stopped = true;
        self.complete(result)
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let mut total = EngineMetricsDelta::default();
        for stage in &mut self.stages {
            let delta = stage.drain_metrics();
            total.late_rows_total = total.late_rows_total.saturating_add(delta.late_rows_total);
            total.filtered_rows_total = total
                .filtered_rows_total
                .saturating_add(delta.filtered_rows_total);
        }
        total
    }
}

/// Publish complete input unchanged through the same processor/output lifecycle as factor kernels.
/// No key, timestamp, deduplication or invented factor is required for a plain Arrow producer.
pub struct IdentityEngine {
    name: String,
    schema: SchemaRef,
}

impl IdentityEngine {
    /// Bind a strict Arrow schema without allocating data or external runtime resources.
    pub fn new(name: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            name: name.into(),
            schema,
        }
    }
}

impl Engine for IdentityEngine {
    fn name(&self) -> &str {
        &self.name
    }
    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema() != self.schema {
            return Err(ZippyError::SchemaMismatch {
                reason: "identity engine input schema differs".into(),
            });
        }
        Ok(vec![table])
    }
}
