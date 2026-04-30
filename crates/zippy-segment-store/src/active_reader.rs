use std::{sync::Arc, time::Duration};

use crate::{
    segment::{
        SHM_CAPACITY_ROWS_OFFSET, SHM_COMMITTED_ROW_COUNT_OFFSET, SHM_GENERATION_OFFSET,
        SHM_LAYOUT_VERSION, SHM_LAYOUT_VERSION_OFFSET, SHM_MAGIC, SHM_MAGIC_OFFSET,
        SHM_NOTIFY_SEQ_OFFSET, SHM_PAYLOAD_OFFSET, SHM_ROW_COUNT_OFFSET, SHM_SCHEMA_ID_OFFSET,
        SHM_SEALED_OFFSET, SHM_SEGMENT_ID_OFFSET,
    },
    ActiveSegmentDescriptor, CompiledSchema, LayoutPlan, RowSpanView, SegmentControlSnapshot,
    ShmRegion, ZippySegmentStoreError,
};

/// 基于 active segment descriptor 的跨进程只读 cursor。
///
/// 该 reader 不依赖本进程的 `PartitionHandle`。控制面只要提供 descriptor
/// envelope，读端即可重新 attach 共享内存，并用 `committed_row_count` 的
/// acquire load 获取安全的已提交前缀。
#[derive(Debug, Clone)]
pub struct ActiveSegmentReader {
    descriptor: ActiveSegmentDescriptor,
    shm_region: Arc<ShmRegion>,
    cursor: usize,
    active_identity: (u64, u64),
}

impl ActiveSegmentReader {
    /// 从跨进程 descriptor envelope 创建 reader。
    pub fn from_descriptor_envelope(
        descriptor_envelope: &[u8],
        schema: CompiledSchema,
        layout: LayoutPlan,
    ) -> Result<Self, ZippySegmentStoreError> {
        let (descriptor, shm_region) =
            attach_descriptor_envelope(descriptor_envelope, schema, layout)?;
        let active_identity = (descriptor.segment_id(), descriptor.generation());
        Ok(Self {
            descriptor,
            shm_region,
            cursor: 0,
            active_identity,
        })
    }

    /// 用新的 descriptor envelope 更新 reader。
    ///
    /// 当 `(segment_id, generation)` 变化时，新 active segment 的行号空间从 0
    /// 开始，因此 reader cursor 必须重置。
    pub fn update_descriptor_envelope(
        &mut self,
        descriptor_envelope: &[u8],
        schema: CompiledSchema,
        layout: LayoutPlan,
    ) -> Result<(), ZippySegmentStoreError> {
        let (descriptor, shm_region) =
            attach_descriptor_envelope(descriptor_envelope, schema, layout)?;
        let active_identity = (descriptor.segment_id(), descriptor.generation());
        if active_identity != self.active_identity {
            self.cursor = 0;
        }
        self.descriptor = descriptor;
        self.shm_region = shm_region;
        self.active_identity = active_identity;
        Ok(())
    }

    /// 返回当前已提交行数。
    pub fn committed_row_count(&self) -> Result<usize, ZippySegmentStoreError> {
        let committed = self
            .shm_region
            .load_u64_acquire(self.descriptor.committed_row_count_offset())?;
        usize::try_from(committed).map_err(|_| {
            ZippySegmentStoreError::Shmem(
                "active segment committed row count overflows usize".to_string(),
            )
        })
    }

    /// 读取并校验当前 active segment 的 mmap control/header 快照。
    pub fn control_snapshot(&self) -> Result<SegmentControlSnapshot, ZippySegmentStoreError> {
        read_control_snapshot(&self.descriptor, &self.shm_region)
    }

    /// 返回当前 notify sequence，用于后续 futex 等待。
    pub fn notification_sequence(&self) -> Result<u32, ZippySegmentStoreError> {
        self.shm_region.load_u32_acquire(SHM_NOTIFY_SEQ_OFFSET)
    }

    /// 等待 notify sequence 相对 `observed` 发生变化。
    pub fn wait_for_notification_after(
        &self,
        observed: u32,
        timeout: Duration,
    ) -> Result<bool, ZippySegmentStoreError> {
        self.shm_region
            .wait_u32_changed(SHM_NOTIFY_SEQ_OFFSET, observed, timeout)
    }

    /// 将 cursor 移到当前已提交行尾，后续只读取新提交的行。
    pub fn seek_to_committed(&mut self) -> Result<usize, ZippySegmentStoreError> {
        let committed = self.committed_row_count()?;
        self.cursor = committed;
        Ok(committed)
    }

    /// 读取当前 active segment 上新增的连续行区间。
    pub fn read_available(&mut self) -> Result<Option<RowSpanView>, ZippySegmentStoreError> {
        let committed = self.committed_row_count()?;
        if committed <= self.cursor {
            return Ok(None);
        }

        let span = RowSpanView::from_active_attachment(
            self.descriptor.clone(),
            self.shm_region.clone(),
            self.cursor,
            committed,
        )
        .map_err(ZippySegmentStoreError::Layout)?;
        self.cursor = committed;
        Ok(Some(span))
    }
}

fn read_control_snapshot(
    descriptor: &ActiveSegmentDescriptor,
    shm_region: &ShmRegion,
) -> Result<SegmentControlSnapshot, ZippySegmentStoreError> {
    let magic = read_u32_header(shm_region, SHM_MAGIC_OFFSET)?;
    if magic != SHM_MAGIC {
        return Err(ZippySegmentStoreError::Layout(
            "active segment magic mismatch",
        ));
    }

    let layout_version = read_u32_header(shm_region, SHM_LAYOUT_VERSION_OFFSET)?;
    if layout_version != SHM_LAYOUT_VERSION {
        return Err(ZippySegmentStoreError::Layout(
            "active segment layout version mismatch",
        ));
    }

    if descriptor.payload_offset() != SHM_PAYLOAD_OFFSET {
        return Err(ZippySegmentStoreError::Layout(
            "active segment payload offset mismatch",
        ));
    }

    if descriptor.committed_row_count_offset() != SHM_COMMITTED_ROW_COUNT_OFFSET {
        return Err(ZippySegmentStoreError::Layout(
            "active segment committed row count offset mismatch",
        ));
    }

    let schema_id = read_u64_header(shm_region, SHM_SCHEMA_ID_OFFSET)?;
    if schema_id != descriptor.schema().schema_id() {
        return Err(ZippySegmentStoreError::Layout(
            "active segment schema id mismatch",
        ));
    }

    let segment_id = read_u64_header(shm_region, SHM_SEGMENT_ID_OFFSET)?;
    if segment_id != descriptor.segment_id() {
        return Err(ZippySegmentStoreError::Layout("active segment id mismatch"));
    }

    let generation = read_u64_header(shm_region, SHM_GENERATION_OFFSET)?;
    if generation != descriptor.generation() {
        return Err(ZippySegmentStoreError::Layout(
            "active segment generation mismatch",
        ));
    }

    let capacity_rows = read_usize_header(shm_region, SHM_CAPACITY_ROWS_OFFSET)?;
    if capacity_rows != descriptor.layout().row_capacity() {
        return Err(ZippySegmentStoreError::Layout(
            "active segment capacity mismatch",
        ));
    }

    let row_count = read_usize_header(shm_region, SHM_ROW_COUNT_OFFSET)?;
    if row_count > capacity_rows {
        return Err(ZippySegmentStoreError::Layout(
            "active segment row count exceeds capacity",
        ));
    }

    let committed_row_count = shm_region
        .load_u64_acquire(descriptor.committed_row_count_offset())
        .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
    let committed_row_count = usize::try_from(committed_row_count).map_err(|_| {
        ZippySegmentStoreError::Layout("active segment committed row count overflows usize")
    })?;
    if committed_row_count > capacity_rows {
        return Err(ZippySegmentStoreError::Layout(
            "active segment committed row count exceeds capacity",
        ));
    }

    let notify_seq = shm_region.load_u32_acquire(SHM_NOTIFY_SEQ_OFFSET)?;

    let mut sealed = [0_u8; 1];
    shm_region
        .read_at(SHM_SEALED_OFFSET, &mut sealed)
        .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
    let sealed = match sealed[0] {
        0 => false,
        1 => true,
        _ => {
            return Err(ZippySegmentStoreError::Layout(
                "active segment sealed flag is invalid",
            ));
        }
    };

    Ok(SegmentControlSnapshot {
        magic,
        layout_version,
        schema_id,
        segment_id,
        generation,
        capacity_rows,
        row_count,
        committed_row_count,
        notify_seq,
        sealed,
        payload_offset: descriptor.payload_offset(),
        committed_row_count_offset: descriptor.committed_row_count_offset(),
    })
}

fn read_u32_header(shm_region: &ShmRegion, offset: usize) -> Result<u32, ZippySegmentStoreError> {
    let mut bytes = [0_u8; 4];
    shm_region
        .read_at(offset, &mut bytes)
        .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
    Ok(u32::from_ne_bytes(bytes))
}

fn read_u64_header(shm_region: &ShmRegion, offset: usize) -> Result<u64, ZippySegmentStoreError> {
    let mut bytes = [0_u8; 8];
    shm_region
        .read_at(offset, &mut bytes)
        .map_err(|error| ZippySegmentStoreError::Shmem(error.to_string()))?;
    Ok(u64::from_ne_bytes(bytes))
}

fn read_usize_header(
    shm_region: &ShmRegion,
    offset: usize,
) -> Result<usize, ZippySegmentStoreError> {
    let value = read_u64_header(shm_region, offset)?;
    usize::try_from(value)
        .map_err(|_| ZippySegmentStoreError::Layout("active segment header overflows usize"))
}

fn attach_descriptor_envelope(
    descriptor_envelope: &[u8],
    schema: CompiledSchema,
    layout: LayoutPlan,
) -> Result<(ActiveSegmentDescriptor, Arc<ShmRegion>), ZippySegmentStoreError> {
    let descriptor =
        ActiveSegmentDescriptor::from_envelope_bytes(descriptor_envelope, schema, layout)
            .map_err(ZippySegmentStoreError::Layout)?;
    let shm_region = Arc::new(ShmRegion::open(descriptor.shm_os_id())?);
    RowSpanView::from_active_attachment(descriptor.clone(), shm_region.clone(), 0, 0)
        .map_err(ZippySegmentStoreError::Layout)?;
    Ok((descriptor, shm_region))
}
