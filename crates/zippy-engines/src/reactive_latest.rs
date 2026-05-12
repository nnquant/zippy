use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

use crate::latest_state::LatestColumnarState;

/// Maintains the latest input row for each configured key.
pub struct ReactiveLatestEngine {
    name: String,
    input_schema: SchemaRef,
    state: LatestColumnarState,
}

impl ReactiveLatestEngine {
    /// Create a new reactive latest engine.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        by: Vec<impl Into<String>>,
    ) -> Result<Self> {
        let by = by.into_iter().map(Into::into).collect::<Vec<_>>();
        validate_by_columns(input_schema.as_ref(), &by)?;
        let state = LatestColumnarState::new(Arc::clone(&input_schema), by)?;

        Ok(Self {
            name: name.into(),
            input_schema,
            state,
        })
    }

    /// Return the grouping columns used to identify latest rows.
    pub fn by(&self) -> &[String] {
        self.state.key_fields()
    }
}

impl Engine for ReactiveLatestEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }
        if table.num_rows() == 0 {
            return Ok(vec![]);
        }

        let update = self.state.apply_batch(&table)?;
        if update.is_empty() {
            return Ok(vec![]);
        }
        let output = self.state.materialize_update(&update)?;
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        if self.state.is_empty() {
            return Ok(vec![]);
        }
        let output = self.state.materialize_snapshot()?;
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}

fn validate_by_columns(schema: &Schema, by: &[String]) -> Result<()> {
    if by.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "reactive latest engine requires at least one by column".to_string(),
        });
    }

    let mut seen = BTreeSet::new();
    for field_name in by {
        if !seen.insert(field_name) {
            return Err(ZippyError::InvalidConfig {
                reason: format!("duplicate reactive latest by column field=[{}]", field_name),
            });
        }
        let field = schema
            .field_with_name(field_name)
            .map_err(|_| ZippyError::SchemaMismatch {
                reason: format!("missing reactive latest by field field=[{}]", field_name),
            })?;
        if field.data_type() != &DataType::Utf8 {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "reactive latest by field must be utf8 field=[{}]",
                    field_name
                ),
            });
        }
    }

    Ok(())
}
