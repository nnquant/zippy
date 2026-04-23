use crate::SealedSegmentHandle;

/// 已 seal segment 上的连续行视图。
#[derive(Debug, Clone)]
pub struct RowSpanView {
    pub(crate) handle: SealedSegmentHandle,
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
            handle,
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
