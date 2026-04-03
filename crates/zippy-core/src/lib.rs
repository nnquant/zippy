pub mod engine;
pub mod error;
pub mod metrics;
pub mod publisher;
pub mod queue;
pub mod runtime;
pub mod types;
pub mod version;

pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use metrics::{EngineMetrics, EngineMetricsDelta, EngineMetricsSnapshot};
pub use publisher::Publisher;
pub use queue::BoundedQueue;
pub use runtime::{spawn_engine, spawn_engine_with_publisher, EngineHandle};
pub use types::{EngineConfig, EngineStatus, LateDataPolicy, OverflowPolicy};
pub use version::{base_version, python_dev_version, rust_dev_version};

pub fn crate_name() -> &'static str {
    "zippy-core"
}
