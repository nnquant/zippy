/// 运行时算子最小 trait。
pub trait Operator: Send {
    /// 返回算子名称。
    fn name(&self) -> &'static str;

    /// 返回执行所需的列名。
    fn required_columns(&self) -> &'static [&'static str];

    /// 消费一段连续行视图。
    fn on_rows(&mut self, span: &zippy_segment_store::RowSpanView) -> Result<(), String> {
        let _ = span;
        Ok(())
    }
}
