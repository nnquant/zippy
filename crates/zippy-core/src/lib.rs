pub mod bus_frame;
pub mod bus_protocol;
pub mod config;
pub mod control_transport;
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

pub use bus_frame::{
    encode_bus_frame, encode_bus_frame_with_timing, parse_bus_frame, patch_bus_frame_publish_done,
    BusFrameKind, BusFrameTiming, ParsedBusFrame, BUS_FRAME_MAGIC, BUS_FRAME_VERSION,
    FLAG_HAS_INSTRUMENT_DIRECTORY, FLAG_HAS_TIMING_METADATA,
};
pub use bus_protocol::{
    AttachStreamRequest, ControlRequest, ControlResponse, DetachReaderRequest, DetachWriterRequest,
    DropTableRequest, DropTableResult, GetConfigRequest, GetSegmentDescriptorRequest,
    GetStreamRequest, GetStreamResponse, HeartbeatRequest, ListStreamsRequest, ListStreamsResponse,
    PublishPersistedFileRequest, PublishSegmentDescriptorRequest, ReaderDescriptor,
    RegisterEngineRequest, RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest,
    RegisterStreamRequest, ReplacePersistedFilesRequest, StreamInfo, UnregisterSourceRequest,
    UpdateRecordStatusRequest, WriterDescriptor, BUS_LAYOUT_VERSION,
};
pub use config::{
    default_config_path, ZippyConfig, ZippyLogConfig, ZippyTableConfig, ZippyTablePersistConfig,
    ZippyTablePersistPartitionConfig, DEFAULT_CONFIG_PATH, DEFAULT_LOG_LEVEL,
    DEFAULT_TABLE_PERSIST_DATA_DIR, DEFAULT_TABLE_PERSIST_METHOD, DEFAULT_TABLE_ROW_CAPACITY,
};
pub use control_transport::{connect_control_endpoint, send_control_line_request, ControlStream};
pub use endpoint::{
    default_control_endpoint, default_control_endpoint_path, resolve_control_endpoint,
    resolve_control_endpoint_uri, resolve_control_endpoint_with_home, ControlEndpoint,
    ControlEndpointKind, DEFAULT_CONTROL_ENDPOINT_URI,
};
pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use logging::{current_log_snapshot, setup_log, LogConfig, LogSnapshot};
pub use master_client::{MasterClient, Reader, TimedReadBatch, Writer};
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

pub fn crate_name() -> &'static str {
    "zippy-core"
}
