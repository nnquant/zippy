use crate::{ColumnType, CompiledSchema};

/// 单列的物理布局描述。
#[derive(Debug, Clone)]
pub struct ColumnLayout {
    pub name: &'static str,
    pub values_offset: usize,
    /// Values buffer bytes reserved by the layout plan.
    ///
    /// For `Utf8`, this is a capacity hint for the backing buffer rather than
    /// the number of logical bytes written by a future writer.
    pub values_len: usize,
    pub offsets_offset: usize,
    pub offsets_len: usize,
    pub validity_offset: usize,
    pub validity_len: usize,
}

/// 整个 schema 的物理布局计划。
#[derive(Debug, Clone)]
pub struct LayoutPlan {
    row_capacity: usize,
    columns: Vec<ColumnLayout>,
}

impl LayoutPlan {
    /// 为给定 schema 构建最小布局计划。
    pub fn for_schema(schema: &CompiledSchema, row_capacity: usize) -> Result<Self, &'static str> {
        fn checked_add(lhs: usize, rhs: usize) -> Result<usize, &'static str> {
            lhs.checked_add(rhs).ok_or("layout size overflow")
        }

        fn checked_mul(lhs: usize, rhs: usize) -> Result<usize, &'static str> {
            lhs.checked_mul(rhs).ok_or("layout size overflow")
        }

        fn align_to(cursor: usize, align: usize) -> Result<usize, &'static str> {
            let rem = cursor % align;
            if rem == 0 {
                Ok(cursor)
            } else {
                checked_add(cursor, align - rem)
            }
        }

        // Utf8 在布局阶段只预留 values buffer 容量，不表示真实已写入字节数。
        fn reserved_value_bytes(
            data_type: &ColumnType,
            rows: usize,
        ) -> Result<usize, &'static str> {
            match data_type {
                ColumnType::Int64 | ColumnType::Float64 | ColumnType::TimestampNsTz(_) => {
                    checked_mul(rows, 8)
                }
                ColumnType::Utf8 => checked_mul(rows, 32),
            }
        }

        let mut cursor = 0usize;
        let mut columns = Vec::with_capacity(schema.columns().len());

        for spec in schema.columns() {
            let validity_len = if spec.nullable {
                row_capacity.div_ceil(8)
            } else {
                0
            };
            let validity_offset = if validity_len == 0 {
                0
            } else {
                cursor = align_to(cursor, 8)?;
                let offset = cursor;
                cursor = checked_add(cursor, validity_len)?;
                offset
            };

            let offsets_len = if matches!(spec.data_type, ColumnType::Utf8) {
                checked_mul(checked_add(row_capacity, 1)?, 4)?
            } else {
                0
            };
            let offsets_offset = if offsets_len == 0 {
                0
            } else {
                cursor = align_to(cursor, 8)?;
                let offset = cursor;
                cursor = checked_add(cursor, offsets_len)?;
                offset
            };

            cursor = align_to(cursor, 8)?;
            let values_offset = cursor;
            let values_len = reserved_value_bytes(&spec.data_type, row_capacity)?;
            cursor = checked_add(cursor, values_len)?;

            columns.push(ColumnLayout {
                name: spec.name,
                values_offset,
                values_len,
                offsets_offset,
                offsets_len,
                validity_offset,
                validity_len,
            });
        }

        Ok(Self {
            row_capacity,
            columns,
        })
    }

    /// 返回布局的行容量。
    pub fn row_capacity(&self) -> usize {
        self.row_capacity
    }

    /// 按列名查找布局。
    pub fn column(&self, name: &str) -> Option<&ColumnLayout> {
        self.columns.iter().find(|col| col.name == name)
    }
}
