/// 列的数据类型定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Int64,
    Float64,
    Utf8,
    TimestampNsTz(&'static str),
}

/// 列规格定义。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: &'static str,
    pub data_type: ColumnType,
    pub nullable: bool,
}

impl ColumnSpec {
    /// 构造一个列规格。
    pub fn new(name: &'static str, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            nullable: false,
        }
    }

    /// 构造一个可空列规格。
    pub fn nullable(name: &'static str, data_type: ColumnType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
        }
    }
}

/// 编译后的 schema 占位结构。
#[derive(Debug, Clone)]
pub struct CompiledSchema {
    schema_id: u64,
    columns: Vec<ColumnSpec>,
}

impl CompiledSchema {
    /// 返回 schema 标识。
    pub fn schema_id(&self) -> u64 {
        self.schema_id
    }

    /// 返回编译后的列集合。
    pub fn columns(&self) -> &[ColumnSpec] {
        &self.columns
    }
}

/// 编译列规格为 schema 占位结构。
pub fn compile_schema(columns: &[ColumnSpec]) -> Result<CompiledSchema, &'static str> {
    Ok(CompiledSchema {
        schema_id: columns.len() as u64,
        columns: columns.to_vec(),
    })
}
