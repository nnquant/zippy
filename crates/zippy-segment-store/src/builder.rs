use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    segment::ActiveSegmentDescriptor,
    segment::{SealedSegmentData, SealedUtf8Column},
    segment::{
        SHM_CAPACITY_ROWS_OFFSET, SHM_COMMITTED_ROW_COUNT_OFFSET, SHM_DESCRIPTOR_GENERATION_OFFSET,
        SHM_GENERATION_OFFSET, SHM_LAYOUT_VERSION, SHM_LAYOUT_VERSION_OFFSET, SHM_MAGIC,
        SHM_MAGIC_OFFSET, SHM_NOTIFY_SEQ_OFFSET, SHM_PAYLOAD_OFFSET, SHM_ROW_COUNT_OFFSET,
        SHM_SCHEMA_ID_OFFSET, SHM_SEALED_OFFSET, SHM_SEGMENT_ID_OFFSET, SHM_WRITER_EPOCH_OFFSET,
    },
    ColumnType, CompiledSchema, LayoutPlan, RowSpanView, SealedSegmentHandle, SegmentCellValue,
    SegmentHeader, ShmRegion,
};

/// Active segment 在共享内存中的最小布局信息。
#[derive(Debug, Clone, Copy)]
pub struct ActiveSegmentShmLayout {
    pub payload_offset: usize,
    pub committed_row_count_offset: usize,
}

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
    persistence_key: String,
    layout: LayoutPlan,
    row_cursor: usize,
    current_row_open: bool,
    validity: HashMap<&'static str, Vec<bool>>,
    i64_columns: HashMap<&'static str, Vec<i64>>,
    f64_columns: HashMap<&'static str, Vec<f64>>,
    utf8_columns: HashMap<&'static str, Utf8ColumnBuffer>,
    shm_region: ShmRegion,
}

impl ActiveSegmentWriter {
    /// 构造 runtime 使用的 active segment writer。
    pub fn new_for_runtime(
        schema: CompiledSchema,
        layout: LayoutPlan,
        segment_id: u64,
        generation: u64,
    ) -> Result<Self, &'static str> {
        Self::new_with_origin_for_test(
            schema,
            layout,
            segment_id,
            generation,
            0,
            generation.saturating_add(1),
            format!("segment-{segment_id}-generation-{generation}"),
        )
    }

    /// 构造指定 segment 标识的最小 active segment writer。
    pub fn new_with_ids_for_test(
        schema: CompiledSchema,
        layout: LayoutPlan,
        segment_id: u64,
        generation: u64,
    ) -> Result<Self, &'static str> {
        Self::new_for_runtime(schema, layout, segment_id, generation)
    }

    pub(crate) fn new_with_origin_for_test(
        schema: CompiledSchema,
        layout: LayoutPlan,
        segment_id: u64,
        generation: u64,
        writer_epoch: u64,
        descriptor_generation: u64,
        persistence_key: String,
    ) -> Result<Self, &'static str> {
        let mut i64_columns = HashMap::new();
        let mut f64_columns = HashMap::new();
        let mut utf8_columns = HashMap::new();
        let mut validity = HashMap::new();

        for spec in schema.columns() {
            let column_layout = layout.column(spec.name).ok_or("missing column layout")?;
            if spec.nullable {
                validity.insert(spec.name, vec![false; layout.row_capacity()]);
            }
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

        let shm_len = SHM_PAYLOAD_OFFSET
            .checked_add(layout.total_bytes())
            .ok_or("shared memory region size overflow")?;
        let mut shm_region =
            ShmRegion::create(shm_len).map_err(|_| "failed to create shared memory region")?;
        write_u64_header(&mut shm_region, SHM_SCHEMA_ID_OFFSET, schema.schema_id())?;
        write_u64_header(&mut shm_region, SHM_SEGMENT_ID_OFFSET, segment_id)?;
        write_u64_header(&mut shm_region, SHM_GENERATION_OFFSET, generation)?;
        write_u64_header(&mut shm_region, SHM_WRITER_EPOCH_OFFSET, writer_epoch)?;
        write_u64_header(
            &mut shm_region,
            SHM_DESCRIPTOR_GENERATION_OFFSET,
            descriptor_generation,
        )?;
        write_u64_header(
            &mut shm_region,
            SHM_CAPACITY_ROWS_OFFSET,
            layout.row_capacity() as u64,
        )?;
        write_u64_header(&mut shm_region, SHM_ROW_COUNT_OFFSET, 0)?;
        shm_region
            .store_u64_release(SHM_COMMITTED_ROW_COUNT_OFFSET, 0)
            .map_err(|_| "failed to initialize shared memory committed row count")?;
        shm_region
            .write_at(SHM_SEALED_OFFSET, &[0_u8])
            .map_err(|_| "failed to initialize shared memory sealed flag")?;
        shm_region
            .store_u32_release(SHM_NOTIFY_SEQ_OFFSET, 0)
            .map_err(|_| "failed to initialize shared memory notify sequence")?;
        write_u32_header(&mut shm_region, SHM_MAGIC_OFFSET, SHM_MAGIC)?;
        write_u32_header(
            &mut shm_region,
            SHM_LAYOUT_VERSION_OFFSET,
            SHM_LAYOUT_VERSION,
        )?;

        Ok(Self {
            header: SegmentHeader {
                schema_id: schema.schema_id(),
                segment_id,
                generation,
                writer_epoch,
                descriptor_generation,
                capacity_rows: layout.row_capacity(),
                row_count: 0,
                committed_row_count: Arc::new(AtomicUsize::new(0)),
                sealed: false,
            },
            schema,
            persistence_key,
            layout,
            row_cursor: 0,
            current_row_open: false,
            validity,
            i64_columns,
            f64_columns,
            utf8_columns,
            shm_region,
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
        self.finish_open_row()?;
        self.publish_committed_prefix()
    }

    pub(crate) fn finish_open_row(&mut self) -> Result<(), &'static str> {
        if !self.current_row_open {
            return Err("row is not open");
        }

        for (column_name, utf8) in self.utf8_columns.iter_mut() {
            let current = utf8.offsets[self.row_cursor];
            let next = &mut utf8.offsets[self.row_cursor + 1];
            *next = (*next).max(current);
            let column_layout = self
                .layout
                .column(column_name)
                .ok_or("missing column layout")?;
            self.shm_region
                .write_at(
                    SHM_PAYLOAD_OFFSET + column_layout.offsets_offset + ((self.row_cursor + 1) * 4),
                    &next.to_ne_bytes(),
                )
                .map_err(|_| "failed to mirror utf8 offset into shared memory")?;
        }

        self.header.row_count += 1;
        self.row_cursor += 1;
        self.current_row_open = false;
        Ok(())
    }

    pub(crate) fn publish_committed_prefix(&mut self) -> Result<(), &'static str> {
        self.header
            .committed_row_count
            .store(self.row_cursor, Ordering::Release);
        write_u64_header(
            &mut self.shm_region,
            SHM_ROW_COUNT_OFFSET,
            self.header.row_count as u64,
        )?;
        self.shm_region
            .store_u64_release(SHM_COMMITTED_ROW_COUNT_OFFSET, self.row_cursor as u64)
            .map_err(|_| "failed to publish shared memory committed row count")?;
        self.notify_readers()?;
        Ok(())
    }

    /// 放弃当前未提交行，允许调用方重试同一行槽位。
    pub fn abort_row(&mut self) -> Result<(), &'static str> {
        if !self.current_row_open {
            return Err("row is not open");
        }

        for (column_name, utf8) in self.utf8_columns.iter_mut() {
            let start = utf8.offsets[self.row_cursor] as usize;
            utf8.values.truncate(start);
            utf8.offsets[self.row_cursor + 1] = start as u32;
            let column_layout = self
                .layout
                .column(column_name)
                .ok_or("missing column layout")?;
            self.shm_region
                .write_at(
                    SHM_PAYLOAD_OFFSET + column_layout.offsets_offset + ((self.row_cursor + 1) * 4),
                    &(start as u32).to_ne_bytes(),
                )
                .map_err(|_| "failed to mirror utf8 abort offset into shared memory")?;
        }

        let validity_columns = self.validity.keys().copied().collect::<Vec<_>>();
        for column in validity_columns {
            self.mark_invalid(column)?;
        }
        self.current_row_open = false;
        Ok(())
    }

    /// 返回对外可见的已提交行数。
    pub fn committed_row_count(&self) -> usize {
        self.header.committed_row_count.load(Ordering::Acquire)
    }

    /// 清空当前 active segment 中的已提交行，用于原地重写快照。
    pub fn clear_rows(&mut self) -> Result<(), &'static str> {
        if self.header.sealed {
            return Err("segment is sealed");
        }
        if self.current_row_open {
            return Err("open row exists during clear");
        }

        self.header.committed_row_count.store(0, Ordering::Release);
        self.shm_region
            .store_u64_release(SHM_COMMITTED_ROW_COUNT_OFFSET, 0)
            .map_err(|_| "failed to clear shared memory committed row count")?;
        self.header.row_count = 0;
        self.row_cursor = 0;
        write_u64_header(&mut self.shm_region, SHM_ROW_COUNT_OFFSET, 0)?;
        self.notify_readers()?;

        for (column_name, validity) in self.validity.iter_mut() {
            validity.fill(false);
            let Some(column_layout) = self.layout.column(column_name) else {
                continue;
            };
            if column_layout.validity_len == 0 {
                continue;
            }
            let offset = SHM_PAYLOAD_OFFSET + column_layout.validity_offset;
            let zeros = vec![0_u8; column_layout.validity_len];
            self.shm_region
                .write_at(offset, &zeros)
                .map_err(|_| "failed to clear shared memory validity bytes")?;
        }

        for (column_name, utf8) in self.utf8_columns.iter_mut() {
            utf8.offsets.fill(0);
            utf8.values.clear();
            let column_layout = self
                .layout
                .column(column_name)
                .ok_or("missing column layout")?;
            let offset = SHM_PAYLOAD_OFFSET + column_layout.offsets_offset;
            let zeros = vec![0_u8; column_layout.offsets_len];
            self.shm_region
                .write_at(offset, &zeros)
                .map_err(|_| "failed to clear shared memory utf8 offsets")?;
        }

        Ok(())
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
                let column_layout = self.layout.column(column).ok_or("missing column layout")?;
                self.shm_region
                    .write_at(
                        SHM_PAYLOAD_OFFSET + column_layout.values_offset + (self.row_cursor * 8),
                        &value.to_ne_bytes(),
                    )
                    .map_err(|_| "failed to mirror i64 value into shared memory")?;
                self.mark_valid(column)?;
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
                let column_layout = self.layout.column(column).ok_or("missing column layout")?;
                self.shm_region
                    .write_at(
                        SHM_PAYLOAD_OFFSET + column_layout.values_offset + (self.row_cursor * 8),
                        &value.to_ne_bytes(),
                    )
                    .map_err(|_| "failed to mirror f64 value into shared memory")?;
                self.mark_valid(column)?;
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
                let column_layout = self.layout.column(column).ok_or("missing column layout")?;
                self.shm_region
                    .write_at(
                        SHM_PAYLOAD_OFFSET + column_layout.values_offset + start,
                        value.as_bytes(),
                    )
                    .map_err(|_| "failed to mirror utf8 value into shared memory")?;
                self.shm_region
                    .write_at(
                        SHM_PAYLOAD_OFFSET
                            + column_layout.offsets_offset
                            + ((self.row_cursor + 1) * 4),
                        &end.to_ne_bytes(),
                    )
                    .map_err(|_| "failed to mirror utf8 offset into shared memory")?;
                self.mark_valid(column)?;
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

    /// 追加一条测试 tick，仅写入 schema 中存在的测试列。
    pub fn append_tick_for_test(
        &mut self,
        dt: i64,
        instrument_id: &str,
        last_price: f64,
    ) -> Result<(), &'static str> {
        self.begin_row()?;

        if self.column_type("dt").is_ok() {
            self.write_i64("dt", dt)?;
        }
        if self.column_type("instrument_id").is_ok() {
            self.write_utf8("instrument_id", instrument_id)?;
        }
        if self.column_type("last_price").is_ok() {
            self.write_f64("last_price", last_price)?;
        }

        self.commit_row()
    }

    /// 从 segment 行视图直接追加多行，避免先物化成 Arrow batch。
    pub fn append_row_span(
        &mut self,
        span: &RowSpanView,
        start_offset: usize,
        max_rows: usize,
    ) -> Result<usize, &'static str> {
        if self.header.sealed {
            return Err("segment is sealed");
        }
        if self.current_row_open {
            return Err("row already open");
        }
        if start_offset > span.row_count() {
            return Err("row span start offset out of bounds");
        }
        if max_rows == 0 {
            return Ok(0);
        }

        let source_remaining = span.row_count() - start_offset;
        let target_remaining = self.layout.row_capacity().saturating_sub(self.row_cursor);
        let rows_to_copy = source_remaining.min(max_rows).min(target_remaining);
        if rows_to_copy == 0 {
            return Err("segment is full");
        }

        let columns = self.schema.columns().to_vec();
        let mut copied = 0;
        for row_offset in start_offset..start_offset + rows_to_copy {
            self.begin_row()?;
            let row_result = self.write_row_span_row(span, row_offset, &columns);
            match row_result {
                Ok(()) => {
                    self.finish_open_row()?;
                    copied += 1;
                }
                Err(error) => {
                    if self.current_row_open {
                        let _ = self.abort_row();
                    }
                    if copied > 0 {
                        self.publish_committed_prefix()?;
                        return Ok(copied);
                    }
                    return Err(error);
                }
            }
        }

        self.publish_committed_prefix()?;
        Ok(copied)
    }

    /// 导出测试用 sealed snapshot。
    pub fn sealed_handle_for_test(&self) -> Result<SealedSegmentHandle, &'static str> {
        Ok(self.snapshot_sealed_handle())
    }

    /// 返回当前 active segment 的共享描述符。
    pub fn active_descriptor(&self) -> ActiveSegmentDescriptor {
        ActiveSegmentDescriptor {
            schema: self.schema.clone(),
            layout: self.layout.clone(),
            shm_os_id: self.shm_region.os_id().to_string(),
            payload_offset: SHM_PAYLOAD_OFFSET,
            committed_row_count_offset: SHM_COMMITTED_ROW_COUNT_OFFSET,
            segment_id: self.header.segment_id,
            generation: self.header.generation,
            writer_epoch: self.header.writer_epoch,
            descriptor_generation: self.header.descriptor_generation,
        }
    }

    /// 返回当前 active segment 的共享内存标识，仅用于测试。
    pub fn shm_os_id_for_test(&self) -> &str {
        self.shm_region.os_id()
    }

    /// 返回当前 active segment 在共享内存中的最小布局信息，仅用于测试。
    pub fn shm_layout_for_test(&self) -> ActiveSegmentShmLayout {
        ActiveSegmentShmLayout {
            payload_offset: SHM_PAYLOAD_OFFSET,
            committed_row_count_offset: SHM_COMMITTED_ROW_COUNT_OFFSET,
        }
    }

    pub(crate) fn seal_for_rollover(&mut self) -> Result<SealedSegmentHandle, &'static str> {
        let sealed = self.snapshot_sealed_handle();
        self.shm_region
            .write_at(SHM_SEALED_OFFSET, &[1_u8])
            .map_err(|_| "failed to mark shared memory segment as sealed")?;
        self.header.sealed = true;
        self.notify_readers()?;
        Ok(sealed)
    }

    pub(crate) fn into_shm_region(self) -> ShmRegion {
        self.shm_region
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

    fn notify_readers(&self) -> Result<(), &'static str> {
        self.shm_region
            .fetch_add_u32_release(SHM_NOTIFY_SEQ_OFFSET, 1)
            .map_err(|_| "failed to increment shared memory notify sequence")?;
        self.shm_region
            .wake_u32(SHM_NOTIFY_SEQ_OFFSET)
            .map_err(|_| "failed to wake shared memory readers")?;
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

    pub(crate) fn has_open_row(&self) -> bool {
        self.current_row_open
    }

    pub(crate) fn remaining_row_capacity(&self) -> usize {
        self.layout.row_capacity().saturating_sub(self.row_cursor)
    }

    fn write_row_span_row(
        &mut self,
        span: &RowSpanView,
        row_offset: usize,
        columns: &[crate::ColumnSpec],
    ) -> Result<(), &'static str> {
        for spec in columns {
            match span
                .cell_value(row_offset, spec.name)
                .map_err(|_| "failed to read row span cell")?
            {
                SegmentCellValue::Null => {
                    if !spec.nullable {
                        return Err("non-nullable column received null");
                    }
                    self.mark_invalid(spec.name)?;
                }
                SegmentCellValue::Int64(value) => self.write_i64(spec.name, value)?,
                SegmentCellValue::Float64(value) => self.write_f64(spec.name, value)?,
                SegmentCellValue::Utf8(value) => self.write_utf8(spec.name, &value)?,
                SegmentCellValue::TimestampNs(value) => self.write_i64(spec.name, value)?,
            }
        }
        Ok(())
    }

    fn snapshot_sealed_handle(&self) -> SealedSegmentHandle {
        let row_count = self.committed_row_count();
        let validity = self
            .validity
            .iter()
            .map(|(name, bits)| (*name, bits[..row_count].to_vec()))
            .collect();
        let i64_columns = self
            .i64_columns
            .iter()
            .map(|(name, values)| (*name, values[..row_count].to_vec()))
            .collect();
        let f64_columns = self
            .f64_columns
            .iter()
            .map(|(name, values)| (*name, values[..row_count].to_vec()))
            .collect();
        let utf8_columns = self
            .utf8_columns
            .iter()
            .map(|(name, values)| {
                let end = values.offsets[row_count] as usize;
                (
                    *name,
                    SealedUtf8Column {
                        offsets: values.offsets[..=row_count].to_vec(),
                        values: values.values[..end].to_vec(),
                    },
                )
            })
            .collect();

        SealedSegmentHandle::new(SealedSegmentData {
            schema: self.schema.clone(),
            persistence_key: self.persistence_key.clone(),
            segment_id: self.header.segment_id,
            _generation: self.header.generation,
            row_count,
            validity,
            i64_columns,
            f64_columns,
            utf8_columns,
        })
    }

    fn mark_valid(&mut self, column: &str) -> Result<(), &'static str> {
        if let Some(validity) = self.validity.get_mut(column) {
            validity[self.row_cursor] = true;
            let Some(column_layout) = self.layout.column(column) else {
                return Ok(());
            };
            if column_layout.validity_len == 0 {
                return Ok(());
            }
            let byte_index = self.row_cursor / 8;
            let bit_index = self.row_cursor % 8;
            let offset = SHM_PAYLOAD_OFFSET + column_layout.validity_offset + byte_index;
            let mut byte = [0_u8; 1];
            self.shm_region
                .read_at(offset, &mut byte)
                .map_err(|_| "failed to read shared memory validity byte")?;
            byte[0] |= 1 << bit_index;
            self.shm_region
                .write_at(offset, &byte)
                .map_err(|_| "failed to write shared memory validity byte")?;
        }
        Ok(())
    }

    fn mark_invalid(&mut self, column: &str) -> Result<(), &'static str> {
        if let Some(validity) = self.validity.get_mut(column) {
            validity[self.row_cursor] = false;
            let Some(column_layout) = self.layout.column(column) else {
                return Ok(());
            };
            if column_layout.validity_len == 0 {
                return Ok(());
            }
            let byte_index = self.row_cursor / 8;
            let bit_index = self.row_cursor % 8;
            let offset = SHM_PAYLOAD_OFFSET + column_layout.validity_offset + byte_index;
            let mut byte = [0_u8; 1];
            self.shm_region
                .read_at(offset, &mut byte)
                .map_err(|_| "failed to read shared memory validity byte")?;
            byte[0] &= !(1 << bit_index);
            self.shm_region
                .write_at(offset, &byte)
                .map_err(|_| "failed to clear shared memory validity byte")?;
        }
        Ok(())
    }
}

fn write_u64_header(
    shm_region: &mut ShmRegion,
    offset: usize,
    value: u64,
) -> Result<(), &'static str> {
    shm_region
        .write_at(offset, &value.to_ne_bytes())
        .map_err(|_| "failed to write shared memory header")
}

fn write_u32_header(
    shm_region: &mut ShmRegion,
    offset: usize,
    value: u32,
) -> Result<(), &'static str> {
    shm_region
        .write_at(offset, &value.to_ne_bytes())
        .map_err(|_| "failed to write shared memory header")
}
