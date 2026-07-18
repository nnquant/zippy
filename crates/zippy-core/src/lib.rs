pub mod bus_protocol;
pub mod config;
pub mod control_transport;
pub mod data_directory;
pub mod data_directory_ownership;
pub mod durable_file;
pub mod endpoint;
pub mod engine;
pub mod error;
pub mod logging;
pub mod master_client;
pub mod metrics;
pub mod publisher;
pub mod queue;
pub mod runtime;
pub mod schema;
pub mod source;
pub mod spsc_data_queue;
pub mod table;
pub mod types;
pub mod version;

pub use bus_protocol::{
    ControlEnvelopeRequest, ControlEnvelopeResponse, ControlRequest, ControlResponse,
    DropTableRequest, DropTableResult, GetConfigRequest, GetSegmentDescriptorRequest,
    GetStreamRequest, GetStreamResponse, HeartbeatRequest, ListStreamsRequest, ListStreamsResponse,
    PublishPersistedFileRequest, PublishSegmentDescriptorRequest, RegisterEngineRequest,
    RegisterProcessRequest, RegisterSourceRequest, RegisterStreamRequest,
    ReplacePersistedFilesRequest, ResourceEvent, StreamInfo, UnregisterProcessRequest,
    UnregisterSourceRequest, UpdateRecordStatusRequest, WatchRequest, WatchResource,
    CONTROL_PROTOCOL_VERSION,
};
pub use config::{
    default_config_path, ZippyCatalogBackend, ZippyCatalogConfig,
    ZippyCatalogRuntimeSnapshotPinConfig, ZippyCatalogShadowConfig, ZippyConfig,
    ZippyGatewayConfig, ZippyLogConfig, ZippyQueryCanaryConfig, ZippyQueryConfig,
    ZippyQueryResourceConfig, ZippyRuntimeConfig, ZippyTableConfig, ZippyTablePersistConfig,
    ZippyTablePersistPartitionConfig, DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS,
    DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES,
    DEFAULT_CATALOG_SHADOW_COMPARISON_TIMEOUT_MS, DEFAULT_CATALOG_SHADOW_DEDUP_CAPACITY,
    DEFAULT_CATALOG_SHADOW_MAX_FILES_PER_COMPARISON,
    DEFAULT_CATALOG_SHADOW_MAX_METADATA_BYTES_PER_COMPARISON,
    DEFAULT_CATALOG_SHADOW_METADATA_BUDGET_BYTES, DEFAULT_CATALOG_SHADOW_QUEUE_CAPACITY,
    DEFAULT_CONFIG_PATH, DEFAULT_LOG_LEVEL, DEFAULT_MAX_CPU_CORES, DEFAULT_MAX_PARALLELISM,
    DEFAULT_MAX_QUERY_BUFFER_BYTES, DEFAULT_QUERY_CANARY_MAX_BATCHES,
    DEFAULT_QUERY_CANARY_MAX_OUTPUT_BYTES, DEFAULT_QUERY_CANARY_MAX_ROWS,
    DEFAULT_QUERY_CANARY_OPERATOR_MEMORY_BYTES, DEFAULT_QUERY_MAX_ACTIVE_TASKS,
    DEFAULT_QUERY_MAX_BUFFERED_BYTES, DEFAULT_QUERY_MAX_CONCURRENT_QUERIES,
    DEFAULT_QUERY_MAX_MEMORY_BYTES, DEFAULT_QUERY_PLAN_TTL_MS, DEFAULT_TABLE_PERSIST_DATA_DIR,
    DEFAULT_TABLE_PERSIST_METHOD, DEFAULT_TABLE_ROW_CAPACITY, MAX_CATALOG_RUNTIME_SNAPSHOT_PINS,
    MAX_CATALOG_RUNTIME_SNAPSHOT_PIN_TABLES, MAX_CPU_CORES, MAX_PARALLELISM,
    MAX_QUERY_BUFFER_BYTES, MAX_QUERY_CONCURRENT_QUERIES, MAX_QUERY_PLAN_TTL_MS,
};
pub use control_transport::{
    connect_control_endpoint, control_response_result, send_control_line_request,
    send_control_line_request_cancellable_with_deadline, send_control_line_request_with_timeout,
    ControlStream, MAX_CONTROL_RESPONSE_LINE_BYTES,
};
pub use data_directory::{
    DataDirectory, DataDirectoryError, ManagedDataDirectory, DATA_FORMAT_VERSION,
    DATA_FORMAT_VERSION_FILE,
};
pub use data_directory_ownership::{
    CatalogRuntimeOwnership, DataDirectoryOwner, OwnedDataDirectory, DATA_DIRECTORY_LOCK_FILE,
};
pub use durable_file::{
    DurableFilePublicationState, DurableFilePublishError, DurableFileTransaction,
    DurablePublishedFile, DurableRemoveOutcome,
};
pub use endpoint::{
    default_control_endpoint, default_control_endpoint_path, resolve_control_endpoint,
    resolve_control_endpoint_uri, resolve_control_endpoint_with_home, ControlEndpoint,
    ControlEndpointKind, DEFAULT_CONTROL_ENDPOINT_URI,
};
pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use logging::{current_log_snapshot, setup_log, LogConfig, LogSnapshot};
pub use master_client::MasterClient;
pub use metrics::{EngineMetrics, EngineMetricsDelta, EngineMetricsSnapshot};
pub use publisher::Publisher;
pub use queue::BoundedQueue;
pub use runtime::{
    spawn_engine, spawn_engine_with_publisher, spawn_source_engine_with_publisher, EngineHandle,
};
pub use schema::{canonical_schema_hash, schema_metadata};
pub use source::{Source, SourceEvent, SourceHandle, SourceMode, SourceSink, StreamHello};
pub use spsc_data_queue::SpscDataQueue;
pub use table::{SegmentRowView, SegmentTableView};
pub use types::{EngineConfig, EngineStatus, LateDataPolicy, OverflowPolicy};
pub use version::{base_version, python_dev_version, rust_dev_version};
pub use zippy_types::ErrorCode as ControlErrorCode;

pub fn crate_name() -> &'static str {
    "zippy-core"
}
