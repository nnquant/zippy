//! Owning single-node server runtime for control, storage, ingest and query services.
//!
//! SQLite owns durable metadata; DataFusion and Flight serve the native data path.
//! The control registry owns only process leases and boot-scoped runtime observations.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod consumer_log;
mod fixed_snapshot;
mod flight_query;
mod flight_security;
mod health;
mod native_log;
mod native_query;
mod native_query_canary;
mod native_storage;
mod query_execution;
mod query_physical;
mod query_service;
mod query_snapshot;
mod runtime;
mod table_admin;

pub use consumer_log::{NativeConsumerDelivery, NativeConsumerLog, NativeConsumerLogService};
pub use flight_query::NativeFlightQueryFrontend;
pub use health::{ServerCapabilities, ServerHealth, ServiceHealth};
pub use native_log::{
    NativeLogBatch, NativeLogRequest, NativeLogStream, NativeLogSubscription, NativeLogWindow,
};
pub use native_query::{
    NativeFixedSnapshot, NativeFixedSnapshotRequest, NativePreparedQuery, NativeQueryBinding,
    NativeQueryEngine, NativeQueryExplanation,
};
pub use native_query_canary::{
    NativeQueryCanaryBinding, NativeQueryCanaryComparison, NativeQueryCanaryDigestBuilder,
    NativeQueryCanaryExpected, NativeQueryCanaryMetrics, NativeQueryCanaryQueryStatus,
    NativeQueryCanaryReport, NativeQueryCanaryRequest, NativeQueryCanaryRun,
    NativeQueryCanaryService, NativeQueryCanaryStatus, NATIVE_QUERY_CANARY_DIGEST_FORMAT,
};
pub use native_storage::{
    NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    NativeStorageStartOutcome, NativeStorageStartup,
};
pub use runtime::{
    run_server_daemon, ServerMode, ServerReadiness, ServerRuntime, ServerRuntimeConfig,
    ServerRuntimeHealth, ServerShutdownOutcome,
};
pub use table_admin::NativeTableAdmin;
