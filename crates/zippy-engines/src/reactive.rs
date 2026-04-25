use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineMetricsDelta, Result, SchemaRef, SegmentTableView, ZippyError};
use zippy_operators::ReactiveFactor;

use crate::table_view::{project_columns, string_array};

/// Stateful engine that appends reactive factor outputs to each input batch.
pub struct ReactiveStateEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    factors: Vec<Box<dyn ReactiveFactor>>,
    id_filter: Option<HashSet<String>>,
    id_field: Option<String>,
    id_column_index: Option<usize>,
    pending_filtered_rows: u64,
}

impl ReactiveStateEngine {
    /// Create a new reactive state engine.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param factors: Stateful factors evaluated in declaration order against the current batch.
    /// :type factors: Vec<Box<dyn ReactiveFactor>>
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
    ) -> Result<Self> {
        let output_schema = build_output_schema(&input_schema, &factors)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            factors,
            id_filter: None,
            id_field: None,
            id_column_index: None,
            pending_filtered_rows: 0,
        })
    }

    /// Create a new reactive state engine with an optional id whitelist.
    ///
    /// :param name: Engine instance name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed by the engine.
    /// :type input_schema: SchemaRef
    /// :param factors: Stateful factors evaluated in declaration order against the current batch.
    /// :type factors: Vec<Box<dyn ReactiveFactor>>
    /// :param id_field: Utf8 identifier column used for whitelist filtering.
    /// :type id_field: &str
    /// :param id_filter: Optional exact-match whitelist for the first input column.
    /// :type id_filter: Option<Vec<String>>
    /// :returns: Initialized engine with stable output schema ordering.
    /// :rtype: Result<ReactiveStateEngine>
    pub fn new_with_id_filter(
        name: impl Into<String>,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
        id_field: &str,
        id_filter: Option<Vec<String>>,
    ) -> Result<Self> {
        let output_schema = build_output_schema(&input_schema, &factors)?;
        let id_column_index = validate_id_field(input_schema.as_ref(), id_field)?;
        let id_filter = normalize_id_filter(id_filter)?;

        Ok(Self {
            name: name.into(),
            input_schema: Arc::clone(&input_schema),
            output_schema,
            factors,
            id_filter,
            id_field: Some(id_field.to_string()),
            id_column_index: Some(id_column_index),
            pending_filtered_rows: 0,
        })
    }
}

fn normalize_id_filter(id_filter: Option<Vec<String>>) -> Result<Option<HashSet<String>>> {
    match id_filter {
        Some(values) if values.is_empty() => Err(ZippyError::InvalidConfig {
            reason: "id_filter must not be empty".to_string(),
        }),
        Some(values) => Ok(Some(values.into_iter().collect::<HashSet<_>>())),
        None => Ok(None),
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

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }
        let mut columns = self.filter_by_id_whitelist(&table)?;

        if columns.first().map_or(0, |column| column.len()) == 0 {
            return Ok(vec![]);
        }

        let mut current_schema = Arc::clone(&self.input_schema);

        for factor in &mut self.factors {
            let current_batch = RecordBatch::try_new(Arc::clone(&current_schema), columns.clone())
                .map_err(|error| ZippyError::Io {
                    reason: format!(
                        "failed to build reactive intermediate batch error=[{}]",
                        error
                    ),
                })?;
            let output_field = factor.output_field();
            let output_column: ArrayRef = factor.evaluate(&current_batch)?;
            columns.push(output_column);

            let mut fields = current_schema.fields().iter().cloned().collect::<Vec<_>>();
            fields.push(Arc::new(output_field));
            current_schema = Arc::new(Schema::new(fields));
        }

        let output =
            RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
                ZippyError::Io {
                    reason: format!("failed to build reactive output batch error=[{}]", error),
                }
            })?;

        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: 0,
            filtered_rows_total: self.pending_filtered_rows,
        };
        self.pending_filtered_rows = 0;
        delta
    }
}

impl ReactiveStateEngine {
    fn filter_by_id_whitelist(&mut self, table: &SegmentTableView) -> Result<Vec<ArrayRef>> {
        let columns = project_columns(table, &self.input_schema)?;
        let Some(id_filter) = &self.id_filter else {
            return Ok(columns);
        };
        let id_column_index = self
            .id_column_index
            .expect("id column index must exist when id filter is configured");

        if table.num_rows() == 0 {
            return Ok(columns);
        }

        let id_field = self
            .id_field
            .as_deref()
            .expect("id field must exist when id filter is configured");
        let id_column = string_array(&columns[id_column_index], id_field)?;

        let mut kept_indices = Vec::with_capacity(table.num_rows());
        let mut filtered_rows = 0u64;

        for row_index in 0..table.num_rows() {
            if id_column.is_null(row_index) {
                filtered_rows += 1;
                continue;
            }

            let id_value = id_column.value(row_index);
            if id_filter.contains(id_value) {
                kept_indices.push(row_index as u32);
            } else {
                filtered_rows += 1;
            }
        }

        self.pending_filtered_rows += filtered_rows;

        if kept_indices.len() == table.num_rows() {
            return Ok(columns);
        }

        let indices = UInt32Array::from(kept_indices);
        columns
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                    reason: format!("failed to filter reactive table view error=[{}]", error),
                })
            })
            .collect::<Result<Vec<_>>>()
    }
}

fn build_output_schema(
    input_schema: &SchemaRef,
    factors: &[Box<dyn ReactiveFactor>],
) -> Result<SchemaRef> {
    let mut field_names = input_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<HashSet<_>>();
    let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();

    for factor in factors {
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

    Ok(Arc::new(Schema::new(fields)))
}

fn validate_id_field(schema: &Schema, id_field: &str) -> Result<usize> {
    let index = schema
        .index_of(id_field)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_field),
        })?;
    let field = schema.field(index);
    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_field),
        });
    }

    Ok(index)
}
