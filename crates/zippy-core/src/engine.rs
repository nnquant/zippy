use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::{metrics::EngineMetricsDelta, Result, SegmentTableView};

pub type SchemaRef = Arc<Schema>;

pub trait Engine: Send + 'static {
    fn name(&self) -> &str;
    fn input_schema(&self) -> SchemaRef;
    fn output_schema(&self) -> SchemaRef;
    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>>;

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        Ok(vec![])
    }

    fn on_stop(&mut self) -> Result<Vec<SegmentTableView>> {
        self.on_flush()
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}
