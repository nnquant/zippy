use std::sync::Arc;

use crate::{
    ActiveSegmentDescriptor, CompiledSchema, LayoutPlan, RowSpanView, ShmRegion,
    ZippySegmentStoreError,
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
