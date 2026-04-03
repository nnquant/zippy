use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, Result, SchemaRef, ZippyError};
use zippy_operators::ReactiveFactor;

/// Stateful engine that appends reactive factor outputs to each input batch.
pub struct ReactiveStateEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    factors: Vec<Box<dyn ReactiveFactor>>,
}

impl ReactiveStateEngine {
    /// Create a new reactive state engine.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param factors: Stateful factors evaluated against the original input batch.
    /// :type factors: Vec<Box<dyn ReactiveFactor>>
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
    ) -> Result<Self> {
        let mut field_names = input_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();

        for factor in &factors {
            let output_field = factor.output_field();
            let inserted = field_names.insert(output_field.name().clone());

            if !inserted {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "duplicate reactive output field field=[{}]",
                        output_field.name()
                    ),
                });
            }

            fields.push(Arc::new(output_field));
        }

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema: Arc::new(Schema::new(fields)),
            factors,
        })
    }
}

impl Engine for ReactiveStateEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }

        let mut columns = batch.columns().to_vec();

        for factor in &mut self.factors {
            let output_column: ArrayRef = factor.evaluate(&batch)?;
            columns.push(output_column);
        }

        let output =
            RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
                ZippyError::Io {
                    reason: format!("failed to build reactive output batch error=[{}]", error),
                }
            })?;

        Ok(vec![output])
    }
}
