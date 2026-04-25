use arrow::record_batch::RecordBatch;

use crate::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
    ZippySegmentStoreError,
};

pub(crate) fn active_segment_record_batch(
    writer: &ActiveSegmentWriter,
) -> Result<RecordBatch, ZippySegmentStoreError> {
    writer
        .sealed_handle_for_test()
        .map_err(ZippySegmentStoreError::Writer)?
        .as_record_batch()
        .map_err(|error| ZippySegmentStoreError::Arrow(error.to_string()))
}

/// 构造只读调试用的最小 `RecordBatch` 快照。
pub fn debug_snapshot_record_batch_for_test() -> Result<RecordBatch, ZippySegmentStoreError> {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .map_err(ZippySegmentStoreError::Schema)?;
    let layout = LayoutPlan::for_schema(&schema, 1).map_err(ZippySegmentStoreError::Layout)?;
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout)
        .map_err(ZippySegmentStoreError::Writer)?;
    writer
        .append_tick_for_test(2, "rb2505", 4125.0)
        .map_err(ZippySegmentStoreError::Writer)?;
    let sealed = writer
        .sealed_handle_for_test()
        .map_err(ZippySegmentStoreError::Writer)?;
    RowSpanView::new(sealed, 0, 1)
        .map_err(ZippySegmentStoreError::Layout)?
        .as_record_batch()
        .map_err(|error| ZippySegmentStoreError::Arrow(error.to_string()))
}
