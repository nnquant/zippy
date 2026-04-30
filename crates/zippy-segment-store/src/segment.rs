use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{CompiledSchema, LayoutPlan};

pub(crate) const SHM_MAGIC: u32 = 0x5448_535A;
pub(crate) const SHM_LAYOUT_VERSION: u32 = 1;
pub(crate) const SHM_SCHEMA_ID_OFFSET: usize = 0;
pub(crate) const SHM_SEGMENT_ID_OFFSET: usize = 8;
pub(crate) const SHM_GENERATION_OFFSET: usize = 16;
pub(crate) const SHM_CAPACITY_ROWS_OFFSET: usize = 24;
pub(crate) const SHM_ROW_COUNT_OFFSET: usize = 32;
pub(crate) const SHM_COMMITTED_ROW_COUNT_OFFSET: usize = 40;
pub(crate) const SHM_SEALED_OFFSET: usize = 48;
pub(crate) const SHM_NOTIFY_SEQ_OFFSET: usize = 52;
pub(crate) const SHM_MAGIC_OFFSET: usize = 56;
pub(crate) const SHM_LAYOUT_VERSION_OFFSET: usize = 60;
pub(crate) const SHM_PAYLOAD_OFFSET: usize = 64;

/// Active segment mmap header 的只读快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentControlSnapshot {
    pub magic: u32,
    pub layout_version: u32,
    pub schema_id: u64,
    pub segment_id: u64,
    pub generation: u64,
    pub capacity_rows: usize,
    pub row_count: usize,
    pub committed_row_count: usize,
    pub notify_seq: u32,
    pub sealed: bool,
    pub payload_offset: usize,
    pub committed_row_count_offset: usize,
}

/// Active segment 头部元数据。
#[derive(Debug)]
pub struct SegmentHeader {
    pub schema_id: u64,
    pub segment_id: u64,
    pub generation: u64,
    pub capacity_rows: usize,
    pub row_count: usize,
    pub committed_row_count: Arc<AtomicUsize>,
    pub sealed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SealedUtf8Column {
    pub(crate) offsets: Vec<u32>,
    pub(crate) values: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct SealedSegmentData {
    pub(crate) schema: CompiledSchema,
    pub(crate) persistence_key: String,
    pub(crate) segment_id: u64,
    pub(crate) _generation: u64,
    pub(crate) row_count: usize,
    pub(crate) validity: HashMap<&'static str, Vec<bool>>,
    pub(crate) i64_columns: HashMap<&'static str, Vec<i64>>,
    pub(crate) f64_columns: HashMap<&'static str, Vec<f64>>,
    pub(crate) utf8_columns: HashMap<&'static str, SealedUtf8Column>,
}

/// 已 seal segment 的最小只读句柄。
#[derive(Debug, Clone)]
pub struct SealedSegmentHandle {
    pub(crate) inner: Arc<SealedSegmentData>,
}

/// 可通过共享内存重新 attach 的 active segment 描述符。
#[derive(Debug, Clone)]
pub struct ActiveSegmentDescriptor {
    pub(crate) schema: CompiledSchema,
    pub(crate) layout: LayoutPlan,
    pub(crate) shm_os_id: String,
    pub(crate) payload_offset: usize,
    pub(crate) committed_row_count_offset: usize,
    pub(crate) segment_id: u64,
    pub(crate) generation: u64,
}

impl ActiveSegmentDescriptor {
    /// 返回 schema。
    pub fn schema(&self) -> &CompiledSchema {
        &self.schema
    }

    /// 返回布局 plan。
    pub fn layout(&self) -> &LayoutPlan {
        &self.layout
    }

    /// 返回 shm os id。
    pub fn shm_os_id(&self) -> &str {
        &self.shm_os_id
    }

    /// 返回 payload 区偏移。
    pub fn payload_offset(&self) -> usize {
        self.payload_offset
    }

    /// 返回 committed row count 在 shm header 中的偏移。
    pub fn committed_row_count_offset(&self) -> usize {
        self.committed_row_count_offset
    }

    /// 返回 active segment id。
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// 返回 generation。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 返回篡改 committed row count offset 后的描述符，仅用于测试。
    pub fn with_committed_row_count_offset_for_test(mut self, offset: usize) -> Self {
        self.committed_row_count_offset = offset;
        self
    }

    /// 返回篡改 payload offset 后的描述符，仅用于测试。
    pub fn with_payload_offset_for_test(mut self, offset: usize) -> Self {
        self.payload_offset = offset;
        self
    }
}

impl SealedSegmentHandle {
    pub(crate) fn new(data: SealedSegmentData) -> Self {
        Self {
            inner: Arc::new(data),
        }
    }

    /// 返回 segment 标识。
    pub fn segment_id(&self) -> u64 {
        self.inner.segment_id
    }

    /// 返回 sealed segment 的已提交行数。
    pub fn row_count(&self) -> usize {
        self.inner.row_count
    }

    pub(crate) fn schema(&self) -> &CompiledSchema {
        &self.inner.schema
    }

    pub(crate) fn persistence_key(&self) -> &str {
        &self.inner.persistence_key
    }
}
