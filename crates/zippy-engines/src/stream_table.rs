use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, Result, SchemaRef, ZippyError};

/// Pass-through engine for stream table ingestion.
pub struct StreamTableEngine {
    name: String,
    input_schema: SchemaRef,
}

impl StreamTableEngine {
    /// Create a new stream table engine.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and forwarded by the engine.
    /// :type input_schema: SchemaRef
    /// :returns: Initialized pass-through engine.
    /// :rtype: Result<StreamTableEngine>
    pub fn new(name: impl Into<String>, input_schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            name: name.into(),
            input_schema,
        })
    }
}

impl Engine for StreamTableEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match stream table input schema engine=[{}]",
                    self.name
                ),
            });
        }

        Ok(vec![batch])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}
