use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::{
    view::{read_active_payload_consistent, ActiveSegmentAttachment, RowSpanBacking},
    ColumnType, CompiledSchema, RowSpanView, SealedSegmentHandle,
};

/// Incrementally exports a row span as Arrow `RecordBatch` chunks.
#[derive(Debug, Clone)]
pub struct RowSpanBatchReader {
    span: RowSpanView,
    chunk_rows: usize,
    projection_columns: Option<Vec<String>>,
    offset: usize,
}

impl RowSpanBatchReader {
    /// Returns the next chunk, or `None` once the span is exhausted.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.offset >= self.span.row_count() {
            return Ok(None);
        }

        let rows = self.chunk_rows.min(self.span.row_count() - self.offset);
        let start = self.span.start_row + self.offset;
        let end = start + rows;
        let chunk = self.span.subspan(start, end)?;
        self.offset += rows;

        let batch = match self.projection_columns.as_ref() {
            Some(columns) => {
                let field_names = columns.iter().map(String::as_str).collect::<Vec<_>>();
                chunk.as_record_batch_with_projection(&field_names)?
            }
            None => chunk.as_record_batch()?,
        };
        Ok(Some(batch))
    }
}

impl RowSpanView {
    /// 将行范围导出为调试用 `RecordBatch`。
    pub fn as_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        let schema = self.arrow_schema();
        let arrays = schema
            .fields()
            .iter()
            .map(|field| self.project_array(field.name()))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        RecordBatch::try_new(schema, arrays)
    }

    /// 将指定列投影为 `RecordBatch`，避免物化未请求列。
    pub fn as_record_batch_with_projection(
        &self,
        field_names: &[&str],
    ) -> Result<RecordBatch, ArrowError> {
        let schema = self.projected_arrow_schema(field_names)?;
        let arrays = field_names
            .iter()
            .map(|field_name| self.project_array(field_name))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        RecordBatch::try_new(schema, arrays)
    }

    /// 创建按固定行数导出当前 span 的 batch reader。
    pub fn batch_reader(
        &self,
        chunk_rows: usize,
        projection_columns: Option<Vec<String>>,
    ) -> Result<RowSpanBatchReader, ArrowError> {
        if let Some(columns) = projection_columns.as_ref() {
            let field_names = columns.iter().map(String::as_str).collect::<Vec<_>>();
            self.projected_arrow_schema(&field_names)?;
        }

        Ok(RowSpanBatchReader {
            span: self.clone(),
            chunk_rows: chunk_rows.max(1),
            projection_columns,
            offset: 0,
        })
    }

    /// 返回当前行范围的 Arrow schema。
    pub fn schema_ref(&self) -> SchemaRef {
        self.arrow_schema()
    }

    /// 按列名投影当前行范围，避免把整个 span 先导出成 `RecordBatch`。
    pub fn column(&self, field_name: &str) -> Result<ArrayRef, ArrowError> {
        self.project_array(field_name)
    }

    fn project_array(&self, field_name: &str) -> Result<ArrayRef, ArrowError> {
        let spec = self
            .schema()
            .columns()
            .iter()
            .find(|spec| spec.name == field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("missing column [{field_name}]")))?;

        match &self.backing {
            RowSpanBacking::Sealed(handle) => self.project_sealed_array(handle, field_name, spec),
            RowSpanBacking::Active(attachment) => read_active_payload_consistent(
                attachment,
                || self.project_active_array_unchecked(attachment, field_name, spec),
                |error| ArrowError::ParseError(error.to_string()),
            ),
        }
    }

    fn project_sealed_array(
        &self,
        handle: &SealedSegmentHandle,
        field_name: &str,
        spec: &crate::ColumnSpec,
    ) -> Result<ArrayRef, ArrowError> {
        match &spec.data_type {
            ColumnType::Int64 => {
                let values = handle.inner.i64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                if spec.nullable {
                    let validity = self.sealed_validity_slice(handle, field_name)?;
                    Ok(Arc::new(Int64Array::from(
                        values[self.start_row..self.end_row]
                            .iter()
                            .zip(validity.iter().copied())
                            .map(|(value, is_valid)| is_valid.then_some(*value))
                            .collect::<Vec<_>>(),
                    )))
                } else {
                    Ok(Arc::new(Int64Array::from(
                        values[self.start_row..self.end_row].to_vec(),
                    )))
                }
            }
            ColumnType::Float64 => {
                let values = handle.inner.f64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                if spec.nullable {
                    let validity = self.sealed_validity_slice(handle, field_name)?;
                    Ok(Arc::new(Float64Array::from(
                        values[self.start_row..self.end_row]
                            .iter()
                            .zip(validity.iter().copied())
                            .map(|(value, is_valid)| is_valid.then_some(*value))
                            .collect::<Vec<_>>(),
                    )))
                } else {
                    Ok(Arc::new(Float64Array::from(
                        values[self.start_row..self.end_row].to_vec(),
                    )))
                }
            }
            ColumnType::Utf8 => {
                let values = handle.inner.utf8_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                if spec.nullable {
                    let validity = self.sealed_validity_slice(handle, field_name)?;
                    let strings = (self.start_row..self.end_row)
                        .zip(validity.iter().copied())
                        .map(|(row, is_valid)| {
                            if !is_valid {
                                return Ok(None);
                            }
                            let start = values.offsets[row] as usize;
                            let end = values.offsets[row + 1] as usize;
                            std::str::from_utf8(&values.values[start..end])
                                .map(|value| Some(value.to_owned()))
                                .map_err(|err| ArrowError::ParseError(err.to_string()))
                        })
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(StringArray::from(strings)))
                } else {
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
            }
            ColumnType::TimestampNsTz(timezone) => {
                let values = handle.inner.i64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                if spec.nullable {
                    let validity = self.sealed_validity_slice(handle, field_name)?;
                    Ok(Arc::new(
                        TimestampNanosecondArray::from(
                            values[self.start_row..self.end_row]
                                .iter()
                                .zip(validity.iter().copied())
                                .map(|(value, is_valid)| is_valid.then_some(*value))
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone((*timezone).to_string()),
                    ))
                } else {
                    Ok(Arc::new(
                        TimestampNanosecondArray::from(
                            values[self.start_row..self.end_row].to_vec(),
                        )
                        .with_timezone((*timezone).to_string()),
                    ))
                }
            }
        }
    }

    fn project_active_array_unchecked(
        &self,
        attachment: &ActiveSegmentAttachment,
        field_name: &str,
        spec: &crate::ColumnSpec,
    ) -> Result<ArrayRef, ArrowError> {
        let layout = attachment
            .descriptor
            .layout
            .column(field_name)
            .ok_or_else(|| ArrowError::SchemaError(format!("missing column [{field_name}]")))?;

        match &spec.data_type {
            ColumnType::Int64 => {
                if spec.nullable {
                    let values = (self.start_row..self.end_row)
                        .map(|row| {
                            let is_valid = read_active_validity(attachment, layout, row)?;
                            if !is_valid {
                                return Ok(None);
                            }
                            Ok(Some(read_active_i64(attachment, layout, row)?))
                        })
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(Int64Array::from(values)))
                } else {
                    let values = (self.start_row..self.end_row)
                        .map(|row| read_active_i64(attachment, layout, row))
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(Int64Array::from(values)))
                }
            }
            ColumnType::Float64 => {
                if spec.nullable {
                    let values = (self.start_row..self.end_row)
                        .map(|row| {
                            let is_valid = read_active_validity(attachment, layout, row)?;
                            if !is_valid {
                                return Ok(None);
                            }
                            Ok(Some(read_active_f64(attachment, layout, row)?))
                        })
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(Float64Array::from(values)))
                } else {
                    let values = (self.start_row..self.end_row)
                        .map(|row| read_active_f64(attachment, layout, row))
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(Float64Array::from(values)))
                }
            }
            ColumnType::Utf8 => {
                if spec.nullable {
                    let values = (self.start_row..self.end_row)
                        .map(|row| {
                            let is_valid = read_active_validity(attachment, layout, row)?;
                            if !is_valid {
                                return Ok(None);
                            }
                            Ok(Some(read_active_utf8(attachment, layout, row)?))
                        })
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(StringArray::from(values)))
                } else {
                    let values = (self.start_row..self.end_row)
                        .map(|row| read_active_utf8(attachment, layout, row))
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(StringArray::from(values)))
                }
            }
            ColumnType::TimestampNsTz(timezone) => {
                if spec.nullable {
                    let values = (self.start_row..self.end_row)
                        .map(|row| {
                            let is_valid = read_active_validity(attachment, layout, row)?;
                            if !is_valid {
                                return Ok(None);
                            }
                            Ok(Some(read_active_i64(attachment, layout, row)?))
                        })
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(
                        TimestampNanosecondArray::from(values)
                            .with_timezone((*timezone).to_string()),
                    ))
                } else {
                    let values = (self.start_row..self.end_row)
                        .map(|row| read_active_i64(attachment, layout, row))
                        .collect::<Result<Vec<_>, ArrowError>>()?;
                    Ok(Arc::new(
                        TimestampNanosecondArray::from(values)
                            .with_timezone((*timezone).to_string()),
                    ))
                }
            }
        }
    }

    fn sealed_validity_slice<'a>(
        &self,
        handle: &'a SealedSegmentHandle,
        field_name: &str,
    ) -> Result<&'a [bool], ArrowError> {
        let validity =
            handle.inner.validity.get(field_name).ok_or_else(|| {
                ArrowError::SchemaError(format!("missing validity [{field_name}]"))
            })?;
        Ok(&validity[self.start_row..self.end_row])
    }

    fn arrow_schema(&self) -> SchemaRef {
        build_arrow_schema(self.schema())
    }

    fn projected_arrow_schema(&self, field_names: &[&str]) -> Result<SchemaRef, ArrowError> {
        let full_schema = self.arrow_schema();
        let fields = field_names
            .iter()
            .map(|field_name| {
                full_schema
                    .field_with_name(field_name)
                    .cloned()
                    .map_err(|_| ArrowError::SchemaError(format!("missing column [{field_name}]")))
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    fn subspan(&self, start_row: usize, end_row: usize) -> Result<Self, ArrowError> {
        if start_row < self.start_row || end_row > self.end_row || start_row > end_row {
            return Err(ArrowError::SchemaError(
                "row span chunk is out of bounds".to_string(),
            ));
        }

        Ok(Self {
            backing: self.backing.clone(),
            start_row,
            end_row,
        })
    }
}

impl SealedSegmentHandle {
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

fn read_active_validity(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    row: usize,
) -> Result<bool, ArrowError> {
    if layout.validity_len == 0 {
        return Ok(true);
    }
    let byte_index = row / 8;
    let bit_index = row % 8;
    let mut byte = [0_u8; 1];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.validity_offset + byte_index,
            &mut byte,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    Ok((byte[0] & (1 << bit_index)) != 0)
}

fn read_active_i64(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    row: usize,
) -> Result<i64, ArrowError> {
    let mut bytes = [0_u8; 8];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.values_offset + (row * 8),
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    Ok(i64::from_ne_bytes(bytes))
}

fn read_active_f64(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    row: usize,
) -> Result<f64, ArrowError> {
    let mut bytes = [0_u8; 8];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.values_offset + (row * 8),
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    Ok(f64::from_ne_bytes(bytes))
}

fn read_active_utf8(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    row: usize,
) -> Result<String, ArrowError> {
    let start = read_active_u32(attachment, layout.offsets_offset + (row * 4))? as usize;
    let end = read_active_u32(attachment, layout.offsets_offset + ((row + 1) * 4))? as usize;
    if start > end || end > layout.values_len {
        return Err(ArrowError::ParseError(format!(
            "invalid utf8 offset range start=[{start}] end=[{end}] values_len=[{}]",
            layout.values_len
        )));
    }
    let len = end - start;
    let mut bytes = vec![0_u8; len];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.values_offset + start,
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    std::str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|error| ArrowError::ParseError(error.to_string()))
}

fn read_active_u32(
    attachment: &ActiveSegmentAttachment,
    relative_offset: usize,
) -> Result<u32, ArrowError> {
    let mut bytes = [0_u8; 4];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + relative_offset,
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    Ok(u32::from_ne_bytes(bytes))
}
