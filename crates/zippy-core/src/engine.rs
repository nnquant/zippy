use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::{metrics::EngineMetricsDelta, Result};

pub type SchemaRef = Arc<Schema>;

pub trait Engine: Send + 'static {
    fn name(&self) -> &str;
    fn input_schema(&self) -> SchemaRef;
    fn output_schema(&self) -> SchemaRef;
    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>>;

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        self.on_flush()
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}
