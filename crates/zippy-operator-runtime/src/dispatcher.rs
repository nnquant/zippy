use crate::PartitionRuntime;

impl PartitionRuntime {
    /// 测试辅助：将同一 span 分发给所有已注册算子。
    pub fn dispatch_test_span(&mut self, start: usize, end: usize) -> Result<(), String> {
        let span = zippy_segment_store::RowSpanView::for_test(start, end)?;
        for operator in &mut self.operators {
            operator.on_rows(&span)?;
        }
        Ok(())
    }
}
