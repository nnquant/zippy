use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, StringArray, TimestampNanosecondArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SegmentTableView, ZippyError};

pub(crate) fn project_columns(
    table: &SegmentTableView,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>> {
    schema
        .fields()
        .iter()
        .map(|field| table.column(field.name()))
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn record_batch_from_table_rows(
    table: &SegmentTableView,
    schema: &SchemaRef,
    row_indices: &[u32],
    stage_label: &str,
) -> Result<RecordBatch> {
    if row_indices.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    let columns = project_columns(table, schema)?;
    if row_indices.len() == table.num_rows() {
        return RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to build {} table batch error=[{}]",
                stage_label, error
            ),
        });
    }

    let indices = UInt32Array::from(row_indices.to_vec());
    let columns = columns
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to filter {} table view error=[{}]",
                    stage_label, error
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build {} filtered batch error=[{}]",
            stage_label, error
        ),
    })
}

pub(crate) fn string_array<'a>(array: &'a ArrayRef, field: &str) -> Result<&'a StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", field),
        })
}

pub(crate) fn timestamp_ns_array<'a>(
    array: &'a ArrayRef,
    field: &str,
) -> Result<&'a TimestampNanosecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                field
            ),
        })
}

pub(crate) fn float64_array<'a>(array: &'a ArrayRef, field: &str) -> Result<&'a Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", field),
        })
}
