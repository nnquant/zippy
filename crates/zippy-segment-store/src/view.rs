use std::sync::Arc;

use crate::{
    layout::ColumnLayout,
    segment::{
        SHM_CAPACITY_ROWS_OFFSET, SHM_COMMITTED_ROW_COUNT_OFFSET, SHM_DESCRIPTOR_GENERATION_OFFSET,
        SHM_GENERATION_OFFSET, SHM_LAYOUT_VERSION, SHM_LAYOUT_VERSION_OFFSET, SHM_MAGIC,
        SHM_MAGIC_OFFSET, SHM_PAYLOAD_OFFSET, SHM_ROW_COUNT_OFFSET, SHM_SCHEMA_ID_OFFSET,
        SHM_SEALED_OFFSET, SHM_SEGMENT_ID_OFFSET, SHM_WRITER_EPOCH_OFFSET,
    },
    ActiveSegmentDescriptor, ColumnSpec, ColumnType, CompiledSchema, SealedSegmentHandle,
    ShmRegion, ZippySegmentStoreError,
};

#[derive(Debug, Clone, PartialEq)]
pub enum SegmentCellValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampNs(i64),
}

#[derive(Debug, Clone)]
pub(crate) enum RowSpanBacking {
    Sealed(SealedSegmentHandle),
    Active(ActiveSegmentAttachment),
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveSegmentAttachment {
    pub(crate) descriptor: ActiveSegmentDescriptor,
    pub(crate) shm_region: Arc<ShmRegion>,
}

/// 已 seal segment 上的连续行视图。
#[derive(Debug, Clone)]
pub struct RowSpanView {
    pub(crate) backing: RowSpanBacking,
    pub(crate) start_row: usize,
    pub(crate) end_row: usize,
}

impl RowSpanView {
    /// 构造一个连续行范围视图。
    pub fn new(
        handle: SealedSegmentHandle,
        start_row: usize,
        end_row: usize,
    ) -> Result<Self, &'static str> {
        if start_row > end_row {
            return Err("start row exceeds end row");
        }
        if end_row > handle.row_count() {
            return Err("row span out of bounds");
        }

        Ok(Self {
            backing: RowSpanBacking::Sealed(handle),
            start_row,
            end_row,
        })
    }

    /// 通过 active segment 描述符重新 attach shm，构造连续行范围视图。
    pub fn from_active_descriptor(
        descriptor: ActiveSegmentDescriptor,
        start_row: usize,
        end_row: usize,
    ) -> Result<Self, &'static str> {
        if start_row > end_row {
            return Err("start row exceeds end row");
        }
        let shm_region =
            Arc::new(ShmRegion::open(descriptor.shm_os_id()).map_err(|_| {
                "failed to reopen shared memory region from active segment descriptor"
            })?);
        validate_active_descriptor_header(&descriptor, &shm_region)?;
        let committed = shm_region
            .load_u64_acquire(descriptor.committed_row_count_offset())
            .map_err(|_| "failed to read active segment committed row count")?;
        let committed = usize::try_from(committed)
            .map_err(|_| "active segment committed row count overflows usize")?;
        if end_row > committed {
            return Err("row span out of bounds");
        }

        Ok(Self {
            backing: RowSpanBacking::Active(ActiveSegmentAttachment {
                descriptor,
                shm_region,
            }),
            start_row,
            end_row,
        })
    }

    pub(crate) fn from_active_attachment(
        descriptor: ActiveSegmentDescriptor,
        shm_region: Arc<ShmRegion>,
        start_row: usize,
        end_row: usize,
    ) -> Result<Self, &'static str> {
        if start_row > end_row {
            return Err("start row exceeds end row");
        }
        validate_active_descriptor_header(&descriptor, &shm_region)?;
        let committed = shm_region
            .load_u64_acquire(descriptor.committed_row_count_offset())
            .map_err(|_| "failed to read active segment committed row count")?;
        let committed = usize::try_from(committed)
            .map_err(|_| "active segment committed row count overflows usize")?;
        if end_row > committed {
            return Err("row span out of bounds");
        }

        Ok(Self {
            backing: RowSpanBacking::Active(ActiveSegmentAttachment {
                descriptor,
                shm_region,
            }),
            start_row,
            end_row,
        })
    }

    /// 返回起始行。
    pub fn start_row(&self) -> usize {
        self.start_row
    }

    /// 返回结束行。
    pub fn end_row(&self) -> usize {
        self.end_row
    }

    /// 返回当前 span 的行数。
    pub fn row_count(&self) -> usize {
        self.end_row - self.start_row
    }

    /// 返回当前 span 的 schema。
    pub fn schema(&self) -> &CompiledSchema {
        match &self.backing {
            RowSpanBacking::Sealed(handle) => handle.schema(),
            RowSpanBacking::Active(attachment) => attachment.descriptor.schema(),
        }
    }

    /// 直接读取 span 内某行某列的标量值，不构造 Arrow array 或 RecordBatch。
    pub fn cell_value(
        &self,
        row_offset: usize,
        field_name: &str,
    ) -> Result<SegmentCellValue, ZippySegmentStoreError> {
        if row_offset >= self.row_count() {
            return Err(ZippySegmentStoreError::Lifecycle(
                "row offset out of bounds",
            ));
        }
        let absolute_row = self.start_row + row_offset;
        let spec = self
            .schema()
            .columns()
            .iter()
            .find(|spec| spec.name == field_name)
            .ok_or(ZippySegmentStoreError::Schema("missing column"))?;

        match &self.backing {
            RowSpanBacking::Sealed(handle) => self.sealed_cell_value(handle, spec, absolute_row),
            RowSpanBacking::Active(attachment) => {
                self.active_cell_value(attachment, spec, absolute_row)
            }
        }
    }

    fn sealed_cell_value(
        &self,
        handle: &SealedSegmentHandle,
        spec: &ColumnSpec,
        row: usize,
    ) -> Result<SegmentCellValue, ZippySegmentStoreError> {
        if spec.nullable {
            let validity = handle
                .inner
                .validity
                .get(spec.name)
                .ok_or(ZippySegmentStoreError::Schema("missing validity"))?;
            if !validity[row] {
                return Ok(SegmentCellValue::Null);
            }
        }

        match &spec.data_type {
            ColumnType::Int64 => handle
                .inner
                .i64_columns
                .get(spec.name)
                .map(|values| SegmentCellValue::Int64(values[row]))
                .ok_or(ZippySegmentStoreError::Schema("missing int64 column")),
            ColumnType::Float64 => handle
                .inner
                .f64_columns
                .get(spec.name)
                .map(|values| SegmentCellValue::Float64(values[row]))
                .ok_or(ZippySegmentStoreError::Schema("missing float64 column")),
            ColumnType::Utf8 => {
                let values = handle
                    .inner
                    .utf8_columns
                    .get(spec.name)
                    .ok_or(ZippySegmentStoreError::Schema("missing utf8 column"))?;
                let start = values.offsets[row] as usize;
                let end = values.offsets[row + 1] as usize;
                let value = std::str::from_utf8(&values.values[start..end])
                    .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
                Ok(SegmentCellValue::Utf8(value.to_string()))
            }
            ColumnType::TimestampNsTz(_) => handle
                .inner
                .i64_columns
                .get(spec.name)
                .map(|values| SegmentCellValue::TimestampNs(values[row]))
                .ok_or(ZippySegmentStoreError::Schema("missing timestamp column")),
        }
    }

    fn active_cell_value(
        &self,
        attachment: &ActiveSegmentAttachment,
        spec: &ColumnSpec,
        row: usize,
    ) -> Result<SegmentCellValue, ZippySegmentStoreError> {
        let layout = attachment
            .descriptor
            .layout()
            .column(spec.name)
            .ok_or(ZippySegmentStoreError::Schema("missing column layout"))?;

        if spec.nullable && !read_active_validity(attachment, layout, row)? {
            return Ok(SegmentCellValue::Null);
        }

        match spec.data_type {
            ColumnType::Int64 => Ok(SegmentCellValue::Int64(read_active_i64(
                attachment, layout, row,
            )?)),
            ColumnType::Float64 => Ok(SegmentCellValue::Float64(read_active_f64(
                attachment, layout, row,
            )?)),
            ColumnType::Utf8 => Ok(SegmentCellValue::Utf8(read_active_utf8(
                attachment, layout, row,
            )?)),
            ColumnType::TimestampNsTz(_) => Ok(SegmentCellValue::TimestampNs(read_active_i64(
                attachment, layout, row,
            )?)),
        }
    }
}

fn read_active_validity(
    attachment: &ActiveSegmentAttachment,
    layout: &ColumnLayout,
    row: usize,
) -> Result<bool, ZippySegmentStoreError> {
    if layout.validity_len == 0 {
        return Ok(true);
    }
    let byte_index = row / 8;
    let bit_index = row % 8;
    let mut byte = [0_u8; 1];
    attachment.shm_region.read_at(
        attachment.descriptor.payload_offset() + layout.validity_offset + byte_index,
        &mut byte,
    )?;
    Ok((byte[0] & (1 << bit_index)) != 0)
}

fn read_active_i64(
    attachment: &ActiveSegmentAttachment,
    layout: &ColumnLayout,
    row: usize,
) -> Result<i64, ZippySegmentStoreError> {
    let mut bytes = [0_u8; 8];
    attachment.shm_region.read_at(
        attachment.descriptor.payload_offset() + layout.values_offset + (row * 8),
        &mut bytes,
    )?;
    Ok(i64::from_ne_bytes(bytes))
}

fn read_active_f64(
    attachment: &ActiveSegmentAttachment,
    layout: &ColumnLayout,
    row: usize,
) -> Result<f64, ZippySegmentStoreError> {
    let mut bytes = [0_u8; 8];
    attachment.shm_region.read_at(
        attachment.descriptor.payload_offset() + layout.values_offset + (row * 8),
        &mut bytes,
    )?;
    Ok(f64::from_ne_bytes(bytes))
}

fn read_active_utf8(
    attachment: &ActiveSegmentAttachment,
    layout: &ColumnLayout,
    row: usize,
) -> Result<String, ZippySegmentStoreError> {
    let start = read_active_u32(attachment, layout.offsets_offset + (row * 4))? as usize;
    let end = read_active_u32(attachment, layout.offsets_offset + ((row + 1) * 4))? as usize;
    if start > end || end > layout.values_len {
        return Err(ZippySegmentStoreError::Shmem(format!(
            "invalid utf8 offset range start=[{start}] end=[{end}] values_len=[{}]",
            layout.values_len
        )));
    }
    let len = end - start;
    let mut bytes = vec![0_u8; len];
    attachment.shm_region.read_at(
        attachment.descriptor.payload_offset() + layout.values_offset + start,
        &mut bytes,
    )?;
    std::str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))
}

fn read_active_u32(
    attachment: &ActiveSegmentAttachment,
    relative_offset: usize,
) -> Result<u32, ZippySegmentStoreError> {
    let mut bytes = [0_u8; 4];
    attachment.shm_region.read_at(
        attachment.descriptor.payload_offset() + relative_offset,
        &mut bytes,
    )?;
    Ok(u32::from_ne_bytes(bytes))
}

fn validate_active_descriptor_header(
    descriptor: &ActiveSegmentDescriptor,
    shm_region: &ShmRegion,
) -> Result<(), &'static str> {
    let magic = read_u32_header(shm_region, SHM_MAGIC_OFFSET)?;
    if magic != SHM_MAGIC {
        return Err("active segment magic mismatch");
    }

    let layout_version = read_u32_header(shm_region, SHM_LAYOUT_VERSION_OFFSET)?;
    if layout_version != SHM_LAYOUT_VERSION {
        return Err("active segment layout version mismatch");
    }

    if descriptor.payload_offset() != SHM_PAYLOAD_OFFSET {
        return Err("active segment payload offset mismatch");
    }

    if descriptor.committed_row_count_offset() != SHM_COMMITTED_ROW_COUNT_OFFSET {
        return Err("active segment committed row count offset mismatch");
    }

    let schema_id = read_u64_header(shm_region, SHM_SCHEMA_ID_OFFSET)?;
    if schema_id != descriptor.schema().schema_id() {
        return Err("active segment schema id mismatch");
    }

    let segment_id = read_u64_header(shm_region, SHM_SEGMENT_ID_OFFSET)?;
    if segment_id != descriptor.segment_id() {
        return Err("active segment id mismatch");
    }

    let generation = read_u64_header(shm_region, SHM_GENERATION_OFFSET)?;
    if generation != descriptor.generation() {
        return Err("active segment generation mismatch");
    }

    let writer_epoch = read_u64_header(shm_region, SHM_WRITER_EPOCH_OFFSET)?;
    if writer_epoch != descriptor.writer_epoch() {
        return Err("active segment writer epoch mismatch");
    }

    let descriptor_generation = read_u64_header(shm_region, SHM_DESCRIPTOR_GENERATION_OFFSET)?;
    if descriptor_generation != descriptor.descriptor_generation() {
        return Err("active segment descriptor generation mismatch");
    }

    let capacity = read_usize_header(shm_region, SHM_CAPACITY_ROWS_OFFSET)?;
    if capacity != descriptor.layout().row_capacity() {
        return Err("active segment capacity mismatch");
    }

    let row_count = read_usize_header(shm_region, SHM_ROW_COUNT_OFFSET)?;
    if row_count > capacity {
        return Err("active segment row count exceeds capacity");
    }

    let committed = shm_region
        .load_u64_acquire(descriptor.committed_row_count_offset())
        .map_err(|_| "failed to read active segment committed row count")?;
    let committed = usize::try_from(committed)
        .map_err(|_| "active segment committed row count overflows usize")?;
    if committed > capacity {
        return Err("active segment committed row count exceeds capacity");
    }

    let mut sealed = [0_u8; 1];
    shm_region
        .read_at(SHM_SEALED_OFFSET, &mut sealed)
        .map_err(|_| "failed to read active segment sealed flag")?;
    if sealed[0] > 1 {
        return Err("active segment sealed flag is invalid");
    }

    Ok(())
}

fn read_u32_header(shm_region: &ShmRegion, offset: usize) -> Result<u32, &'static str> {
    let mut bytes = [0_u8; 4];
    shm_region
        .read_at(offset, &mut bytes)
        .map_err(|_| "failed to read active segment header")?;
    Ok(u32::from_ne_bytes(bytes))
}

fn read_u64_header(shm_region: &ShmRegion, offset: usize) -> Result<u64, &'static str> {
    let mut bytes = [0_u8; 8];
    shm_region
        .read_at(offset, &mut bytes)
        .map_err(|_| "failed to read active segment header")?;
    Ok(u64::from_ne_bytes(bytes))
}

fn read_usize_header(shm_region: &ShmRegion, offset: usize) -> Result<usize, &'static str> {
    let value = read_u64_header(shm_region, offset)?;
    usize::try_from(value).map_err(|_| "active segment header overflows usize")
}
