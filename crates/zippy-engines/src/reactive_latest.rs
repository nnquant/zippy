use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};

use crate::table_view::{record_batch_from_table_rows, string_array};

/// Maintains the latest input row for each configured key.
pub struct ReactiveLatestEngine {
    name: String,
    input_schema: SchemaRef,
    by: Vec<String>,
    latest_rows: BTreeMap<Vec<String>, OwnedRow>,
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

        Ok(Self {
            name: name.into(),
            input_schema,
            by,
            latest_rows: BTreeMap::new(),
        })
    }

    /// Return the grouping columns used to identify latest rows.
    pub fn by(&self) -> &[String] {
        &self.by
    }

    fn build_rows_batch<'a>(
        &self,
        rows: impl Iterator<Item = &'a OwnedRow>,
        stage_label: &str,
    ) -> Result<RecordBatch> {
        concat_batches(&self.input_schema, rows.map(|row| &row.batch)).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build reactive latest {} batch error=[{}]",
                    stage_label, error
                ),
            }
        })
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

        let by_arrays = self
            .by
            .iter()
            .map(|field| table.column(field))
            .collect::<Result<Vec<_>>>()?;
        let by_columns = by_arrays
            .iter()
            .zip(&self.by)
            .map(|(column, field)| string_array(column, field))
            .collect::<Result<Vec<_>>>()?;
        let mut next_latest_rows = self.latest_rows.clone();
        let mut updated_keys = BTreeSet::new();

        for row_index in 0..table.num_rows() {
            let key = build_key(&by_columns, &self.by, row_index)?;
            let row = extract_owned_row(&table, &self.input_schema, row_index)?;
            next_latest_rows.insert(key.clone(), row);
            updated_keys.insert(key);
        }

        let output = self.build_rows_batch(
            updated_keys
                .iter()
                .filter_map(|key| next_latest_rows.get(key)),
            "updates",
        )?;
        self.latest_rows = next_latest_rows;

        if output.num_rows() == 0 {
            return Ok(vec![]);
        }
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        if self.latest_rows.is_empty() {
            return Ok(vec![]);
        }
        let output = self.build_rows_batch(self.latest_rows.values(), "snapshot")?;
        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}

#[derive(Clone)]
struct OwnedRow {
    batch: RecordBatch,
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

fn build_key(columns: &[&StringArray], fields: &[String], row_index: usize) -> Result<Vec<String>> {
    columns
        .iter()
        .zip(fields)
        .map(|(column, field)| {
            if column.is_null(row_index) {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!("reactive latest by field contains null field=[{}]", field),
                });
            }
            Ok(column.value(row_index).to_string())
        })
        .collect()
}

fn extract_owned_row(
    table: &SegmentTableView,
    schema: &SchemaRef,
    row_index: usize,
) -> Result<OwnedRow> {
    let batch =
        record_batch_from_table_rows(table, schema, &[row_index as u32], "reactive latest")?;
    Ok(OwnedRow { batch })
}
