use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::CompiledSchema;

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
    pub(crate) segment_id: u64,
    pub(crate) _generation: u64,
    pub(crate) row_count: usize,
    pub(crate) i64_columns: HashMap<&'static str, Vec<i64>>,
    pub(crate) f64_columns: HashMap<&'static str, Vec<f64>>,
    pub(crate) utf8_columns: HashMap<&'static str, SealedUtf8Column>,
}

/// 已 seal segment 的最小只读句柄。
#[derive(Debug, Clone)]
pub struct SealedSegmentHandle {
    pub(crate) inner: Arc<SealedSegmentData>,
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

    pub(crate) fn row_count(&self) -> usize {
        self.inner.row_count
    }

    pub(crate) fn schema(&self) -> &CompiledSchema {
        &self.inner.schema
    }
}
