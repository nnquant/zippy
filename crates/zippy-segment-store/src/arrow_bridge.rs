use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::{ColumnType, CompiledSchema, RowSpanView, SealedSegmentHandle};

impl RowSpanView {
    /// 将行范围导出为调试用 `RecordBatch`。
    pub fn as_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        let schema = self.handle.arrow_schema();
        let arrays = schema
            .fields()
            .iter()
            .map(|field| self.project_array(field.name()))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        RecordBatch::try_new(schema, arrays)
    }

    fn project_array(&self, field_name: &str) -> Result<ArrayRef, ArrowError> {
        let spec = self
            .handle
            .schema()
            .columns()
            .iter()
            .find(|spec| spec.name == field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("missing column [{field_name}]")))?;

        match &spec.data_type {
            ColumnType::Int64 => {
                let values = self
                    .handle
                    .inner
                    .i64_columns
                    .get(field_name)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("missing column [{field_name}]"))
                    })?;
                Ok(Arc::new(Int64Array::from(
                    values[self.start_row..self.end_row].to_vec(),
                )))
            }
            ColumnType::Float64 => {
                let values = self
                    .handle
                    .inner
                    .f64_columns
                    .get(field_name)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("missing column [{field_name}]"))
                    })?;
                Ok(Arc::new(Float64Array::from(
                    values[self.start_row..self.end_row].to_vec(),
                )))
            }
            ColumnType::Utf8 => {
                let values = self
                    .handle
                    .inner
                    .utf8_columns
                    .get(field_name)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("missing column [{field_name}]"))
                    })?;
                let strings = (self.start_row..self.end_row)
                    .map(|row| {
                        let start = values.offsets[row] as usize;
                        let end = values.offsets[row + 1] as usize;
                        std::str::from_utf8(&values.values[start..end])
                            .map(str::to_owned)
                            .map_err(|err| ArrowError::ParseError(err.to_string()))
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                Ok(Arc::new(StringArray::from(strings)))
            }
            ColumnType::TimestampNsTz(timezone) => {
                let values = self
                    .handle
                    .inner
                    .i64_columns
                    .get(field_name)
                    .ok_or_else(|| {
                        ArrowError::SchemaError(format!("missing column [{field_name}]"))
                    })?;
                Ok(Arc::new(
                    TimestampNanosecondArray::from(values[self.start_row..self.end_row].to_vec())
                        .with_timezone((*timezone).to_string()),
                ))
            }
        }
    }
}

impl SealedSegmentHandle {
    pub(crate) fn arrow_schema(&self) -> SchemaRef {
        build_arrow_schema(self.schema())
    }

    /// 将整个 sealed segment 导出为 `RecordBatch`。
    pub fn as_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        RowSpanView::new(self.clone(), 0, self.row_count())
            .map_err(|err| ArrowError::SchemaError(err.to_string()))?
            .as_record_batch()
    }
}

pub(crate) fn build_arrow_schema(schema: &CompiledSchema) -> SchemaRef {
    let fields: Vec<Field> = schema
        .columns()
        .iter()
        .map(|spec| {
            let data_type = match &spec.data_type {
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Utf8 => DataType::Utf8,
                ColumnType::TimestampNsTz(timezone) => {
                    DataType::Timestamp(TimeUnit::Nanosecond, Some((*timezone).into()))
                }
            };
            Field::new(spec.name, data_type, spec.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}
