use std::sync::{atomic::AtomicUsize, Arc};

/// Active segment 头部元数据。
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub schema_id: u64,
    pub segment_id: u64,
    pub generation: u64,
    pub capacity_rows: usize,
    pub row_count: usize,
    pub committed_row_count: Arc<AtomicUsize>,
    pub sealed: bool,
}
