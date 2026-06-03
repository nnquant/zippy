use std::{ptr::NonNull, sync::Arc};

use arrow::{
    alloc::Allocation,
    array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::ArrowNativeType,
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::{
    segment::SealedSegmentData,
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
            .find(|spec| spec.name() == field_name)
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
                let values = sealed_scalar_buffer(handle, &values[self.start_row..self.end_row]);
                let nulls = self.sealed_null_buffer(handle, field_name, spec.nullable)?;
                Ok(Arc::new(Int64Array::new(values, nulls)))
            }
            ColumnType::Float64 => {
                let values = handle.inner.f64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                let values = sealed_scalar_buffer(handle, &values[self.start_row..self.end_row]);
                let nulls = self.sealed_null_buffer(handle, field_name, spec.nullable)?;
                Ok(Arc::new(Float64Array::new(values, nulls)))
            }
            ColumnType::Utf8 => {
                let values = handle.inner.utf8_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                let offset_slice = &values.offsets[self.start_row..=self.end_row];
                let offsets = OffsetBuffer::new(sealed_scalar_buffer(handle, offset_slice));
                let value_buffer = sealed_buffer_from_slice(handle, values.values.as_slice());
                let nulls = self.sealed_null_buffer(handle, field_name, spec.nullable)?;
                Ok(Arc::new(StringArray::new(offsets, value_buffer, nulls)))
            }
            ColumnType::TimestampNsTz(timezone) => {
                let values = handle.inner.i64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                let values = sealed_scalar_buffer(handle, &values[self.start_row..self.end_row]);
                let nulls = self.sealed_null_buffer(handle, field_name, spec.nullable)?;
                Ok(Arc::new(
                    TimestampNanosecondArray::new(values, nulls)
                        .with_timezone((*timezone).to_string()),
                ))
            }
            ColumnType::TimestampNsTzOwned(timezone) => {
                let values = handle.inner.i64_columns.get(field_name).ok_or_else(|| {
                    ArrowError::SchemaError(format!("missing column [{field_name}]"))
                })?;
                let values = sealed_scalar_buffer(handle, &values[self.start_row..self.end_row]);
                let nulls = self.sealed_null_buffer(handle, field_name, spec.nullable)?;
                Ok(Arc::new(
                    TimestampNanosecondArray::new(values, nulls)
                        .with_timezone(timezone.to_string()),
                ))
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
                let values =
                    read_active_i64_values(attachment, layout, self.start_row, self.end_row)?;
                let nulls = read_active_null_buffer(
                    attachment,
                    layout,
                    self.start_row,
                    self.end_row,
                    spec.nullable,
                )?;
                if spec.nullable {
                    Ok(Arc::new(Int64Array::new(values.into(), nulls)))
                } else {
                    Ok(Arc::new(Int64Array::new(values.into(), None)))
                }
            }
            ColumnType::Float64 => {
                let values =
                    read_active_f64_values(attachment, layout, self.start_row, self.end_row)?;
                let nulls = read_active_null_buffer(
                    attachment,
                    layout,
                    self.start_row,
                    self.end_row,
                    spec.nullable,
                )?;
                if spec.nullable {
                    Ok(Arc::new(Float64Array::new(values.into(), nulls)))
                } else {
                    Ok(Arc::new(Float64Array::new(values.into(), None)))
                }
            }
            ColumnType::Utf8 => {
                let (offsets, values) =
                    read_active_utf8_buffers(attachment, layout, self.start_row, self.end_row)?;
                let nulls = read_active_null_buffer(
                    attachment,
                    layout,
                    self.start_row,
                    self.end_row,
                    spec.nullable,
                )?;
                Ok(Arc::new(StringArray::new(
                    OffsetBuffer::new(offsets.into()),
                    Buffer::from_vec(values),
                    nulls,
                )))
            }
            ColumnType::TimestampNsTz(timezone) => {
                let values =
                    read_active_i64_values(attachment, layout, self.start_row, self.end_row)?;
                let nulls = read_active_null_buffer(
                    attachment,
                    layout,
                    self.start_row,
                    self.end_row,
                    spec.nullable,
                )?;
                if spec.nullable {
                    Ok(Arc::new(
                        TimestampNanosecondArray::new(values.into(), nulls)
                            .with_timezone((*timezone).to_string()),
                    ))
                } else {
                    Ok(Arc::new(
                        TimestampNanosecondArray::new(values.into(), None)
                            .with_timezone((*timezone).to_string()),
                    ))
                }
            }
            ColumnType::TimestampNsTzOwned(timezone) => {
                let values =
                    read_active_i64_values(attachment, layout, self.start_row, self.end_row)?;
                let nulls = read_active_null_buffer(
                    attachment,
                    layout,
                    self.start_row,
                    self.end_row,
                    spec.nullable,
                )?;
                if spec.nullable {
                    Ok(Arc::new(
                        TimestampNanosecondArray::new(values.into(), nulls)
                            .with_timezone(timezone.to_string()),
                    ))
                } else {
                    Ok(Arc::new(
                        TimestampNanosecondArray::new(values.into(), None)
                            .with_timezone(timezone.to_string()),
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

    fn sealed_null_buffer(
        &self,
        handle: &SealedSegmentHandle,
        field_name: &str,
        nullable: bool,
    ) -> Result<Option<NullBuffer>, ArrowError> {
        if !nullable {
            return Ok(None);
        }
        Ok(Some(NullBuffer::from(
            self.sealed_validity_slice(handle, field_name)?,
        )))
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
                ColumnType::TimestampNsTzOwned(timezone) => {
                    DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone.to_string().into()))
                }
            };
            Field::new(spec.name(), data_type, spec.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn sealed_scalar_buffer<T: ArrowNativeType>(
    handle: &SealedSegmentHandle,
    values: &[T],
) -> ScalarBuffer<T> {
    ScalarBuffer::new(sealed_buffer_from_slice(handle, values), 0, values.len())
}

fn sealed_buffer_from_slice<T: ArrowNativeType>(
    handle: &SealedSegmentHandle,
    values: &[T],
) -> Buffer {
    if values.is_empty() {
        return Buffer::from_vec(Vec::<T>::new());
    }

    let ptr = NonNull::new(values.as_ptr() as *mut u8).expect("sealed column pointer is null");
    let len = std::mem::size_of_val(values);
    let owner: Arc<SealedSegmentData> = Arc::clone(&handle.inner);
    let owner: Arc<dyn Allocation> = owner;
    // SAFETY: `values` points into immutable data owned by `handle.inner`. The custom allocation
    // keeps that `Arc` alive for at least as long as Arrow can read the buffer.
    unsafe { Buffer::from_custom_allocation(ptr, len, owner) }
}

fn read_active_null_buffer(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
    nullable: bool,
) -> Result<Option<NullBuffer>, ArrowError> {
    if !nullable {
        return Ok(None);
    }
    Ok(Some(NullBuffer::from(
        read_active_validity_values(attachment, layout, start_row, end_row)?.as_slice(),
    )))
}

fn read_active_validity_values(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
) -> Result<Vec<bool>, ArrowError> {
    if layout.validity_len == 0 {
        return Ok(vec![true; end_row - start_row]);
    }

    let start_byte = start_row / 8;
    let end_byte = end_row.div_ceil(8);
    let mut bytes = vec![0_u8; end_byte - start_byte];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.validity_offset + start_byte,
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;

    Ok((start_row..end_row)
        .map(|row| {
            let absolute_byte = row / 8;
            let bit_index = row % 8;
            let byte = bytes[absolute_byte - start_byte];
            (byte & (1 << bit_index)) != 0
        })
        .collect())
}

fn read_active_i64_values(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
) -> Result<Vec<i64>, ArrowError> {
    let bytes = read_active_fixed_width_bytes(attachment, layout, start_row, end_row)?;
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| i64::from_ne_bytes(chunk.try_into().expect("chunk length is 8")))
        .collect())
}

fn read_active_f64_values(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
) -> Result<Vec<f64>, ArrowError> {
    let bytes = read_active_fixed_width_bytes(attachment, layout, start_row, end_row)?;
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_ne_bytes(chunk.try_into().expect("chunk length is 8")))
        .collect())
}

fn read_active_fixed_width_bytes(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
) -> Result<Vec<u8>, ArrowError> {
    let mut bytes = vec![0_u8; (end_row - start_row) * 8];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.values_offset + (start_row * 8),
            &mut bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    Ok(bytes)
}

fn read_active_utf8_buffers(
    attachment: &ActiveSegmentAttachment,
    layout: &crate::ColumnLayout,
    start_row: usize,
    end_row: usize,
) -> Result<(Vec<i32>, Vec<u8>), ArrowError> {
    let row_count = end_row - start_row;
    let mut offset_bytes = vec![0_u8; (row_count + 1) * 4];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.offsets_offset + (start_row * 4),
            &mut offset_bytes,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;

    let absolute_offsets = offset_bytes
        .chunks_exact(4)
        .map(|chunk| u32::from_ne_bytes(chunk.try_into().expect("chunk length is 4")) as usize)
        .collect::<Vec<_>>();
    if absolute_offsets
        .windows(2)
        .any(|window| window[0] > window[1])
    {
        return Err(ArrowError::ParseError(
            "invalid utf8 offsets: offsets are not monotonic".to_string(),
        ));
    }

    let value_start = *absolute_offsets.first().unwrap_or(&0);
    let value_end = *absolute_offsets.last().unwrap_or(&0);
    if value_end > layout.values_len {
        return Err(ArrowError::ParseError(format!(
            "invalid utf8 offset range end=[{value_end}] values_len=[{}]",
            layout.values_len
        )));
    }

    let mut values = vec![0_u8; value_end - value_start];
    attachment
        .shm_region
        .read_at(
            attachment.descriptor.payload_offset + layout.values_offset + value_start,
            &mut values,
        )
        .map_err(|error| ArrowError::ParseError(error.to_string()))?;
    std::str::from_utf8(&values).map_err(|error| ArrowError::ParseError(error.to_string()))?;

    let offsets = absolute_offsets
        .iter()
        .map(|offset| {
            i32::try_from(offset - value_start)
                .map_err(|_| ArrowError::ParseError("utf8 offset exceeds i32".to_string()))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    Ok((offsets, values))
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;
    use crate::{compile_schema, ActiveSegmentWriter, ColumnSpec, LayoutPlan};

    #[test]
    fn sealed_non_nullable_numeric_array_reuses_value_buffer() {
        let schema = compile_schema(&[ColumnSpec::new("last_price", ColumnType::Float64)]).unwrap();
        let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
        let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();
        for value in [1.0, 2.0, 3.0] {
            writer.begin_row().unwrap();
            writer.write_f64("last_price", value).unwrap();
            writer.commit_row().unwrap();
        }

        let handle = writer.sealed_handle_for_test().unwrap();
        let source_values = handle.inner.f64_columns.get("last_price").unwrap();
        let expected_ptr = source_values[1..].as_ptr();
        let span = RowSpanView::new(handle, 1, 3).unwrap();

        let array = span.column("last_price").unwrap();
        let prices = array.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(prices.values().as_ptr(), expected_ptr);
        assert_eq!(prices.values(), &[2.0, 3.0]);
    }

    #[test]
    fn sealed_nullable_numeric_array_reuses_value_buffer_and_preserves_nulls() {
        let schema =
            compile_schema(&[ColumnSpec::nullable("last_price", ColumnType::Float64)]).unwrap();
        let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
        let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

        writer.begin_row().unwrap();
        writer.write_f64("last_price", 1.0).unwrap();
        writer.commit_row().unwrap();

        writer.begin_row().unwrap();
        writer.commit_row().unwrap();

        writer.begin_row().unwrap();
        writer.write_f64("last_price", 3.0).unwrap();
        writer.commit_row().unwrap();

        let handle = writer.sealed_handle_for_test().unwrap();
        let source_values = handle.inner.f64_columns.get("last_price").unwrap();
        let expected_ptr = source_values.as_ptr();
        let span = RowSpanView::new(handle, 0, 3).unwrap();

        let array = span.column("last_price").unwrap();
        let prices = array.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(prices.values().as_ptr(), expected_ptr);
        assert_eq!(prices.value(0), 1.0);
        assert!(prices.is_null(1));
        assert_eq!(prices.value(2), 3.0);
    }

    #[test]
    fn sealed_utf8_array_reuses_offsets_and_value_buffers() {
        let schema = compile_schema(&[ColumnSpec::new("instrument_id", ColumnType::Utf8)]).unwrap();
        let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
        let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();
        for value in ["rb2501", "rb2505", "rb2510"] {
            writer.begin_row().unwrap();
            writer.write_utf8("instrument_id", value).unwrap();
            writer.commit_row().unwrap();
        }

        let handle = writer.sealed_handle_for_test().unwrap();
        let source_values = handle.inner.utf8_columns.get("instrument_id").unwrap();
        let expected_offsets_ptr = source_values.offsets[1..].as_ptr();
        let expected_values_ptr = source_values.values.as_ptr();
        let span = RowSpanView::new(handle, 1, 3).unwrap();

        let array = span.column("instrument_id").unwrap();
        let ids = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ids.value_offsets().as_ptr(), expected_offsets_ptr);
        assert_eq!(ids.values().as_ptr(), expected_values_ptr);
        assert_eq!(ids.value(0), "rb2505");
        assert_eq!(ids.value(1), "rb2510");
    }
}
