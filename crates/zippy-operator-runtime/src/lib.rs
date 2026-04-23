//! 新的 operator runtime workspace crate 的最小公共导出面。

mod operator {
    /// 运行时算子最小 trait。
    pub trait Operator {
        /// 返回算子名称。
        fn name(&self) -> &'static str;

        /// 返回执行所需的列名。
        fn required_columns(&self) -> &'static [&'static str];
    }
}

pub use operator::Operator;
