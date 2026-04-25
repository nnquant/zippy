use std::time::Duration;

use zippy_segment_store::{
    PartitionHandle, ReaderSession, RowSpanView, SegmentLease, SegmentNotifier, SegmentStore,
    ZippySegmentStoreError,
};

/// 单分区最小 reader。
pub struct PartitionReader {
    handle: PartitionHandle,
    session: ReaderSession,
    notifier: SegmentNotifier,
    cursor: usize,
    active_segment_identity: (u64, u64),
    active_lease: Option<SegmentLease>,
}

impl PartitionReader {
    /// 创建一个 reader，会复用同一 partition 上的通知与 session。
    pub fn new(
        store: SegmentStore,
        handle: PartitionHandle,
        session_name: &str,
    ) -> Result<Self, String> {
        let session = store.open_session(session_name).map_err(map_store_error)?;
        let notifier = session.subscribe(&handle).map_err(map_store_error)?;
        Ok(Self {
            handle,
            session,
            notifier,
            cursor: 0,
            active_segment_identity: (0, 0),
            active_lease: None,
        })
    }

    /// 在给定超时内等待新增数据通知。
    pub fn wait_timeout(&self, timeout: Duration) -> Result<bool, String> {
        self.notifier
            .wait_timeout(timeout)
            .map_err(|error| error.to_string())
    }

    /// 读取当前 partition 上新增的连续行区间。
    pub fn read_available(&mut self) -> Result<Option<RowSpanView>, String> {
        let active_identity = self.handle.active_segment_identity();
        if active_identity != self.active_segment_identity {
            self.active_segment_identity = active_identity;
            self.cursor = 0;
            self.active_lease = None;
        }

        let committed = self.handle.active_committed_row_count();
        if committed <= self.cursor {
            return Ok(None);
        }

        let lease = self
            .session
            .attach_active(&self.handle)
            .map_err(map_store_error)?;
        let span = self
            .handle
            .active_row_span(self.cursor, committed)
            .map_err(map_store_error)?;
        self.cursor = committed;
        self.active_lease = Some(lease);
        Ok(Some(span))
    }
}

fn map_store_error(error: ZippySegmentStoreError) -> String {
    error.to_string()
}
