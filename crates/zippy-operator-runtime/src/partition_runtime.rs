use crate::Operator;

/// 单分区算子运行时最小骨架。
pub struct PartitionRuntime {
    pub(crate) operators: Vec<Box<dyn Operator>>,
}

impl PartitionRuntime {
    /// 为测试构造空运行时。
    pub fn for_test() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    /// 注册一个算子实例。
    pub fn add_operator(&mut self, operator: Box<dyn Operator>) {
        self.operators.push(operator);
    }
}
