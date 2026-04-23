use crate::PartitionRuntime;

impl PartitionRuntime {
    /// 测试辅助：将同一 span 分发给所有已注册算子。
    pub fn dispatch_test_span(&mut self, start: usize, end: usize) -> Result<(), String> {
        let span = build_test_span(start, end)?;
        for operator in &mut self.operators {
            operator.on_rows(&span)?;
        }
        Ok(())
    }
}

fn build_test_span(start: usize, end: usize) -> Result<zippy_segment_store::RowSpanView, String> {
    let schema = zippy_segment_store::compile_schema(&[
        zippy_segment_store::ColumnSpec::new("dt", zippy_segment_store::ColumnType::Int64),
        zippy_segment_store::ColumnSpec::new(
            "instrument_id",
            zippy_segment_store::ColumnType::Utf8,
        ),
        zippy_segment_store::ColumnSpec::new(
            "last_price",
            zippy_segment_store::ColumnType::Float64,
        ),
    ])
    .map_err(str::to_owned)?;
    let layout = zippy_segment_store::LayoutPlan::for_schema(&schema, end).map_err(str::to_owned)?;
    let mut writer =
        zippy_segment_store::ActiveSegmentWriter::new_for_test(schema, layout).map_err(str::to_owned)?;

    for row in 0..end {
        writer
            .append_tick_for_test(row as i64, "rb2501", row as f64)
            .map_err(str::to_owned)?;
    }

    let handle = writer.sealed_handle_for_test().map_err(str::to_owned)?;
    zippy_segment_store::RowSpanView::new(handle, start, end).map_err(str::to_owned)
}
