use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{ColumnType, CompiledSchema, LayoutPlan, SegmentHeader};

#[derive(Debug, Clone)]
struct Utf8ColumnBuffer {
    offsets: Vec<u32>,
    values: Vec<u8>,
    values_capacity: usize,
}

/// Active segment 的最小写入器。
#[derive(Debug)]
pub struct ActiveSegmentWriter {
    header: SegmentHeader,
    schema: CompiledSchema,
    layout: LayoutPlan,
    row_cursor: usize,
    current_row_open: bool,
    i64_columns: HashMap<&'static str, Vec<i64>>,
    f64_columns: HashMap<&'static str, Vec<f64>>,
    utf8_columns: HashMap<&'static str, Utf8ColumnBuffer>,
}

impl ActiveSegmentWriter {
    /// 构造指定 segment 标识的最小 active segment writer。
    pub fn new_with_ids_for_test(
        schema: CompiledSchema,
        layout: LayoutPlan,
        segment_id: u64,
        generation: u64,
    ) -> Result<Self, &'static str> {
        let mut i64_columns = HashMap::new();
        let mut f64_columns = HashMap::new();
        let mut utf8_columns = HashMap::new();

        for spec in schema.columns() {
            let column_layout = layout.column(spec.name).ok_or("missing column layout")?;
            match spec.data_type {
                ColumnType::Int64 | ColumnType::TimestampNsTz(_) => {
                    i64_columns.insert(spec.name, vec![0; column_layout.values_len / 8]);
                }
                ColumnType::Float64 => {
                    f64_columns.insert(spec.name, vec![0.0; column_layout.values_len / 8]);
                }
                ColumnType::Utf8 => {
                    utf8_columns.insert(
                        spec.name,
                        Utf8ColumnBuffer {
                            offsets: vec![0; column_layout.offsets_len / 4],
                            values: Vec::with_capacity(column_layout.values_len),
                            values_capacity: column_layout.values_len,
                        },
                    );
                }
            }
        }

        Ok(Self {
            header: SegmentHeader {
                schema_id: schema.schema_id(),
                segment_id,
                generation,
                capacity_rows: layout.row_capacity(),
                row_count: 0,
                committed_row_count: Arc::new(AtomicUsize::new(0)),
                sealed: false,
            },
            schema,
            layout,
            row_cursor: 0,
            current_row_open: false,
            i64_columns,
            f64_columns,
            utf8_columns,
        })
    }

    /// 为测试构造一个最小 active segment writer。
    pub fn new_for_test(schema: CompiledSchema, layout: LayoutPlan) -> Result<Self, &'static str> {
        Self::new_with_ids_for_test(schema, layout, 1, 0)
    }

    /// 打开当前行写入。
    pub fn begin_row(&mut self) -> Result<(), &'static str> {
        if self.header.sealed {
            return Err("segment is sealed");
        }
        if self.current_row_open {
            return Err("row already open");
        }
        if self.row_cursor >= self.layout.row_capacity() {
            return Err("segment is full");
        }

        self.current_row_open = true;
        Ok(())
    }

    /// 提交当前行，使 reader 可见 committed prefix。
    pub fn commit_row(&mut self) -> Result<(), &'static str> {
        if !self.current_row_open {
            return Err("row is not open");
        }

        for utf8 in self.utf8_columns.values_mut() {
            let current = utf8.offsets[self.row_cursor];
            let next = &mut utf8.offsets[self.row_cursor + 1];
            *next = (*next).max(current);
        }

        self.header.row_count += 1;
        self.row_cursor += 1;
        self.current_row_open = false;
        self.header
            .committed_row_count
            .store(self.row_cursor, Ordering::Release);
        Ok(())
    }

    /// 返回对外可见的已提交行数。
    pub fn committed_row_count(&self) -> usize {
        self.header.committed_row_count.load(Ordering::Acquire)
    }

    /// 写入 i64 值的最小占位接口。
    pub fn write_i64(&mut self, column: &str, value: i64) -> Result<(), &'static str> {
        self.ensure_row_open()?;
        match self.column_type(column)? {
            ColumnType::Int64 | ColumnType::TimestampNsTz(_) => {
                let buffer = self
                    .i64_columns
                    .get_mut(column)
                    .ok_or("missing i64 column")?;
                buffer[self.row_cursor] = value;
                Ok(())
            }
            _ => Err("column type mismatch"),
        }
    }

    /// 写入 f64 值的最小占位接口。
    pub fn write_f64(&mut self, column: &str, value: f64) -> Result<(), &'static str> {
        self.ensure_row_open()?;
        match self.column_type(column)? {
            ColumnType::Float64 => {
                let buffer = self
                    .f64_columns
                    .get_mut(column)
                    .ok_or("missing f64 column")?;
                buffer[self.row_cursor] = value;
                Ok(())
            }
            _ => Err("column type mismatch"),
        }
    }

    /// 写入 utf8 值，并推进 offsets。
    pub fn write_utf8(&mut self, column: &str, value: &str) -> Result<(), &'static str> {
        self.ensure_row_open()?;
        match self.column_type(column)? {
            ColumnType::Utf8 => {
                let utf8 = self
                    .utf8_columns
                    .get_mut(column)
                    .ok_or("missing utf8 column")?;
                let start = utf8.offsets[self.row_cursor] as usize;
                if start != utf8.values.len() {
                    return Err("utf8 row already written");
                }
                let next_len = utf8
                    .values
                    .len()
                    .checked_add(value.len())
                    .ok_or("utf8 values overflow")?;
                if next_len > utf8.values_capacity {
                    return Err("utf8 values exceed layout capacity");
                }

                utf8.values.extend_from_slice(value.as_bytes());
                let end = u32::try_from(utf8.values.len()).map_err(|_| "utf8 values overflow")?;
                utf8.offsets[self.row_cursor + 1] = end;
                Ok(())
            }
            _ => Err("column type mismatch"),
        }
    }

    /// 读取测试用的 utf8 值，只暴露 committed prefix。
    pub fn read_utf8_for_test(&self, column: &str, row: usize) -> Result<String, &'static str> {
        if row >= self.committed_row_count() {
            return Err("row not committed");
        }

        let utf8 = self.utf8_columns.get(column).ok_or("missing utf8 column")?;
        let start = utf8.offsets[row] as usize;
        let end = utf8.offsets[row + 1] as usize;
        let bytes = &utf8.values[start..end];
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8")
    }

    fn ensure_row_open(&self) -> Result<(), &'static str> {
        if !self.current_row_open {
            return Err("row is not open");
        }
        if self.row_cursor >= self.layout.row_capacity() {
            return Err("segment is full");
        }
        Ok(())
    }

    fn column_type(&self, column: &str) -> Result<&ColumnType, &'static str> {
        self.schema
            .columns()
            .iter()
            .find(|spec| spec.name == column)
            .map(|spec| &spec.data_type)
            .ok_or("missing column")
    }
}
