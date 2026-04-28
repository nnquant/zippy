mod active_reader;
mod arrow_bridge;
mod builder;
mod catalog;
mod debug;
mod descriptor;
mod layout;
mod lifecycle;
mod notify;
pub mod perf;
mod persistence;
mod schema;
mod segment;
mod shm;
mod view;

pub use active_reader::ActiveSegmentReader;
pub use builder::{ActiveSegmentShmLayout, ActiveSegmentWriter};
pub use catalog::{PartitionHandle, PartitionWriterHandle, SegmentStore, SegmentStoreConfig};
pub use debug::debug_snapshot_record_batch_for_test;
pub use layout::{ColumnLayout, LayoutPlan};
pub use lifecycle::{ReaderSession, SegmentLease};
pub use notify::SegmentNotifier;
pub use persistence::{
    PersistenceQueue, PersistenceRetryPolicy, PersistenceWorker, PersistenceWorkerMetricsSnapshot,
    PersistenceWorkerShutdownReport,
};
pub use schema::{compile_schema, ColumnSpec, ColumnType, CompiledSchema};
pub use segment::{ActiveSegmentDescriptor, SealedSegmentHandle, SegmentHeader};
pub use shm::ShmRegion;
pub use view::{RowSpanView, SegmentCellValue};

use thiserror::Error;

/// Segment store 的最小错误类型。
#[derive(Debug, Error)]
pub enum ZippySegmentStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("schema error: {0}")]
    Schema(&'static str),
    #[error("layout error: {0}")]
    Layout(&'static str),
    #[error("shared memory error: {0}")]
    Shmem(String),
    #[error("writer error: {0}")]
    Writer(&'static str),
    #[error("lifecycle error: {0}")]
    Lifecycle(&'static str),
    #[error("arrow error: {0}")]
    Arrow(String),
}
