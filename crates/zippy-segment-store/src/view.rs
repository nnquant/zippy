use std::collections::HashMap;

use crate::{
    compile_schema,
    segment::SealedSegmentData,
    CompiledSchema,
    SealedSegmentHandle,
};

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

    /// 为跨 crate 测试构造一个只包含行边界的最小视图。
    #[doc(hidden)]
    pub fn for_test(start_row: usize, end_row: usize) -> Result<Self, String> {
        let schema = empty_schema_for_test().map_err(str::to_owned)?;
        let handle = SealedSegmentHandle::new(SealedSegmentData {
            schema,
            persistence_key: "row-span-test".to_owned(),
            segment_id: 0,
            _generation: 0,
            row_count: end_row,
            validity: HashMap::new(),
            i64_columns: HashMap::new(),
            f64_columns: HashMap::new(),
            utf8_columns: HashMap::new(),
        });
        Self::new(handle, start_row, end_row).map_err(str::to_owned)
    }
}

fn empty_schema_for_test() -> Result<CompiledSchema, &'static str> {
    static EMPTY_COLUMNS: [crate::ColumnSpec; 0] = [];
    compile_schema(&EMPTY_COLUMNS)
}
