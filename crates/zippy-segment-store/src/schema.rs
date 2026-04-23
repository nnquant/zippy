use std::collections::HashSet;

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
    fn write_u64(bytes: &mut Vec<u8>, value: u64) {
        bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn push_type_fingerprint(bytes: &mut Vec<u8>, data_type: &ColumnType) {
        match data_type {
            ColumnType::Int64 => bytes.push(1),
            ColumnType::Float64 => bytes.push(2),
            ColumnType::Utf8 => bytes.push(3),
            ColumnType::TimestampNsTz(timezone) => {
                bytes.push(4);
                write_u64(bytes, timezone.len() as u64);
                bytes.extend_from_slice(timezone.as_bytes());
            }
        }
    }

    fn fingerprint_schema(columns: &[ColumnSpec]) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut bytes = Vec::new();
        write_u64(&mut bytes, columns.len() as u64);

        for column in columns {
            write_u64(&mut bytes, column.name.len() as u64);
            bytes.extend_from_slice(column.name.as_bytes());
            bytes.push(u8::from(column.nullable));
            push_type_fingerprint(&mut bytes, &column.data_type);
        }

        let mut hash = FNV_OFFSET_BASIS;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }

        hash
    }

    let mut names = HashSet::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name) {
            return Err("duplicate column name");
        }
    }

    Ok(CompiledSchema {
        schema_id: fingerprint_schema(columns),
        columns: columns.to_vec(),
    })
}
