use std::sync::Arc;

use crate::{
    segment::{
        SHM_CAPACITY_ROWS_OFFSET, SHM_COMMITTED_ROW_COUNT_OFFSET, SHM_GENERATION_OFFSET,
        SHM_LAYOUT_VERSION, SHM_LAYOUT_VERSION_OFFSET, SHM_MAGIC, SHM_MAGIC_OFFSET,
        SHM_PAYLOAD_OFFSET, SHM_ROW_COUNT_OFFSET, SHM_SCHEMA_ID_OFFSET, SHM_SEALED_OFFSET,
        SHM_SEGMENT_ID_OFFSET,
    },
    ActiveSegmentDescriptor, SealedSegmentHandle, ShmRegion,
};

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
