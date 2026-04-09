pub mod engine;
pub mod error;
pub mod logging;
pub mod metrics;
pub mod publisher;
pub mod queue;
pub mod runtime;
pub mod source;
pub mod types;
pub mod version;

pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use logging::{current_log_snapshot, setup_log, LogConfig, LogSnapshot};
pub use metrics::{EngineMetrics, EngineMetricsDelta, EngineMetricsSnapshot};
pub use publisher::Publisher;
pub use queue::BoundedQueue;
pub use runtime::{
    spawn_engine, spawn_engine_with_publisher, spawn_source_engine_with_publisher, EngineHandle,
};
pub use source::{Source, SourceEvent, SourceHandle, SourceMode, SourceSink, StreamHello};
pub use types::{EngineConfig, EngineStatus, LateDataPolicy, OverflowPolicy};
pub use version::{base_version, python_dev_version, rust_dev_version};

pub fn crate_name() -> &'static str {
    "zippy-core"
}
