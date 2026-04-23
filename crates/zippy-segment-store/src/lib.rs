mod arrow_bridge;
mod builder;
mod catalog;
mod layout;
mod lifecycle;
mod notify;
mod persistence;
mod schema;
mod segment;
mod shm;
mod view;

pub use builder::ActiveSegmentWriter;
pub use catalog::{PartitionHandle, PartitionWriterHandle, SegmentStore, SegmentStoreConfig};
pub use layout::{ColumnLayout, LayoutPlan};
pub use lifecycle::{ReaderSession, SegmentLease};
pub use notify::SegmentNotifier;
pub use persistence::PersistenceQueue;
pub use schema::{compile_schema, ColumnSpec, ColumnType, CompiledSchema};
pub use segment::{SealedSegmentHandle, SegmentHeader};
pub use view::RowSpanView;

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
    #[error("writer error: {0}")]
    Writer(&'static str),
    #[error("lifecycle error: {0}")]
    Lifecycle(&'static str),
}
