//! Restores user schema metadata after a native key-value event projection.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

#[derive(Debug)]
pub(crate) struct LogicalSchemaExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl LogicalSchemaExec {
    pub(crate) fn try_new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        if input.schema().fields() != schema.fields() {
            return Err(DataFusionError::Internal(
                "logical schema adapter cannot change fields".to_owned(),
            ));
        }
        let equivalence = input
            .properties()
            .eq_properties
            .clone()
            .with_new_schema(schema)?;
        let properties = input.properties().clone().with_eq_properties(equivalence);
        Ok(Self { input, properties })
    }
}

impl DisplayAs for LogicalSchemaExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("LogicalSchemaExec")
    }
}

impl ExecutionPlan for LogicalSchemaExec {
    fn name(&self) -> &'static str {
        "LogicalSchemaExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] = children.try_into().map_err(|_| {
            DataFusionError::Internal("logical schema adapter requires one child".to_owned())
        })?;
        Ok(Arc::new(Self::try_new(input, self.schema())?))
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let output_schema = schema.clone();
        let stream = self.input.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            Ok(RecordBatch::try_new_with_options(
                output_schema.clone(),
                batch.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}
