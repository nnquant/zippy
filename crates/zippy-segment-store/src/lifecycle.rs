use crate::{
    catalog::{LeaseKey, PartitionHandle},
    SegmentNotifier, SegmentStore, ZippySegmentStoreError,
};

/// Reader session 句柄。
#[derive(Debug)]
pub struct ReaderSession {
    pub(crate) session_id: u64,
    pub(crate) store: SegmentStore,
}

impl ReaderSession {
    /// 附着当前 active segment 并返回 lease。
    pub fn attach_active(
        &self,
        handle: &PartitionHandle,
    ) -> Result<SegmentLease, ZippySegmentStoreError> {
        let lease_key = handle.active_lease_key()?;
        self.store.attach_lease(self.session_id, lease_key)?;
        Ok(SegmentLease {
            segment_id: lease_key.segment_id,
            generation: lease_key.generation,
            session_id: self.session_id,
            store: self.store.clone(),
        })
    }

    /// 订阅分区通知。
    pub fn subscribe(
        &self,
        handle: &PartitionHandle,
    ) -> Result<SegmentNotifier, ZippySegmentStoreError> {
        handle.subscribe()
    }
}

impl Drop for ReaderSession {
    fn drop(&mut self) {
        self.store.mark_session_dead(self.session_id);
    }
}

/// Segment lease，drop 时释放 pin。
#[derive(Debug)]
pub struct SegmentLease {
    pub(crate) segment_id: u64,
    pub(crate) generation: u64,
    pub(crate) session_id: u64,
    pub(crate) store: SegmentStore,
}

impl SegmentLease {
    /// 返回被 pin 的 segment id。
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// 返回 generation。
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl Drop for SegmentLease {
    fn drop(&mut self) {
        let _ = self.store.release_lease(
            self.session_id,
            LeaseKey {
                segment_id: self.segment_id,
                generation: self.generation,
            },
        );
    }
}
