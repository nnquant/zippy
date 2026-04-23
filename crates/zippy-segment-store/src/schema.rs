/// 列的数据类型定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Float64,
    TimestampNsTz(&'static str),
}

/// 列规格定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    name: &'static str,
    data_type: ColumnType,
}

impl ColumnSpec {
    /// 构造一个列规格。
    pub fn new(name: &'static str, data_type: ColumnType) -> Self {
        Self { name, data_type }
    }
}

/// 编译后的 schema 占位结构。
#[derive(Debug, Clone)]
pub struct CompiledSchema {
    columns: Vec<ColumnSpec>,
}

impl CompiledSchema {
    /// 返回编译后的列集合。
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }
}

/// 编译列规格为 schema 占位结构。
pub fn compile_schema(columns: &[ColumnSpec]) -> Result<CompiledSchema, &'static str> {
    Ok(CompiledSchema {
        columns: columns.to_vec(),
    })
}
