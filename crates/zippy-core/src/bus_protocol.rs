use serde::{Deserialize, Serialize};
use std::fmt;

pub const CONTROL_PROTOCOL_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterProcessRequest {
    pub app: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchResource {
    Shutdown,
    Process { process_id: String },
    Source { source_name: String },
    PersistedFile { stream_name: String },
    GatewayConfig,
    Stream { stream_name: String },
    SegmentDescriptor { stream_name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub resource: WatchResource,
    pub after_revision: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEvent {
    pub resource: WatchResource,
    pub revision: u64,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlEnvelopeRequest {
    pub version: u32,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<WatchResource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inner: Option<Box<ControlRequest>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlEnvelopeResponse {
    pub version: u32,
    pub request_id: String,
    pub inner: Box<ControlResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterProcessRequest {
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterStreamRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub stream_name: String,
    #[serde(default = "default_stream_schema")]
    pub schema: serde_json::Value,
    #[serde(default)]
    pub schema_hash: String,
    pub buffer_size: usize,
    pub frame_size: usize,
}

fn default_stream_schema() -> serde_json::Value {
    serde_json::json!({
        "fields": [],
        "metadata": {},
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSourceRequest {
    pub source_name: String,
    pub source_type: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub output_stream: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnregisterSourceRequest {
    pub source_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterEngineRequest {
    pub engine_name: String,
    pub engine_type: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub input_stream: String,
    pub output_stream: String,
    pub sink_names: Vec<String>,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSinkRequest {
    pub sink_name: String,
    pub sink_type: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub input_stream: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRecordStatusRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub kind: String,
    pub name: String,
    pub status: String,
    pub metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_name: String,
    pub schema: serde_json::Value,
    pub schema_hash: String,
    pub data_path: String,
    pub descriptor_generation: u64,
    #[serde(default)]
    pub active_segment_descriptor: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_segment_preflight: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_row_capacity: Option<usize>,
    #[serde(default)]
    pub sealed_segments: Vec<serde_json::Value>,
    #[serde(default)]
    pub persisted_files: Vec<serde_json::Value>,
    #[serde(default)]
    pub persist_events: Vec<serde_json::Value>,
    #[serde(default)]
    pub segment_reader_leases: Vec<serde_json::Value>,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub write_seq: u64,
    pub writer_process_id: Option<String>,
    #[serde(default)]
    pub writer_epoch: u64,
    pub reader_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListStreamsRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStreamRequest {
    pub stream_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTableRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub table_name: String,
    pub drop_persisted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishSegmentDescriptorRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub descriptor: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishPersistedFileRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub persisted_file: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplacePersistedFilesRequest {
    pub stream_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub persisted_files: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishPersistEventRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub persist_event: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquireSegmentReaderLeaseRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub source_segment_id: u64,
    pub source_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseSegmentReaderLeaseRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
    pub lease_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSegmentDescriptorRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListStreamsResponse {
    pub streams: Vec<StreamInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetStreamResponse {
    pub stream: StreamInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropTableResult {
    pub table_name: String,
    pub dropped: bool,
    pub sources_removed: usize,
    pub engines_removed: usize,
    pub sinks_removed: usize,
    pub persisted_files_deleted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    Envelope(ControlEnvelopeRequest),
    RegisterProcess(RegisterProcessRequest),
    Heartbeat(HeartbeatRequest),
    Watch(WatchRequest),
    UnregisterProcess(UnregisterProcessRequest),
    RegisterStream(RegisterStreamRequest),
    RegisterSource(RegisterSourceRequest),
    UnregisterSource(UnregisterSourceRequest),
    RegisterEngine(RegisterEngineRequest),
    RegisterSink(RegisterSinkRequest),
    UpdateStatus(UpdateRecordStatusRequest),
    ListStreams(ListStreamsRequest),
    GetStream(GetStreamRequest),
    GetConfig(GetConfigRequest),
    DropTable(DropTableRequest),
    PublishSegmentDescriptor(PublishSegmentDescriptorRequest),
    PublishPersistedFile(PublishPersistedFileRequest),
    ReplacePersistedFiles(ReplacePersistedFilesRequest),
    PublishPersistEvent(PublishPersistEventRequest),
    AcquireSegmentReaderLease(AcquireSegmentReaderLeaseRequest),
    ReleaseSegmentReaderLease(ReleaseSegmentReaderLeaseRequest),
    GetSegmentDescriptor(GetSegmentDescriptorRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Envelope(ControlEnvelopeResponse),
    ProcessRegistered {
        process_id: String,
        process_token: String,
    },
    HeartbeatAccepted {
        process_id: String,
    },
    ShutdownRequested {
        process_id: String,
        reason: String,
    },
    ProcessUnregistered {
        process_id: String,
    },
    StreamRegistered {
        stream_name: String,
    },
    SourceRegistered {
        source_name: String,
    },
    SourceUnregistered {
        source_name: String,
    },
    EngineRegistered {
        engine_name: String,
    },
    SinkRegistered {
        sink_name: String,
    },
    StatusUpdated {
        kind: String,
        name: String,
    },
    StreamsListed(ListStreamsResponse),
    StreamFetched(GetStreamResponse),
    ConfigFetched {
        config: serde_json::Value,
    },
    TableDropped(DropTableResult),
    SegmentDescriptorPublished {
        stream_name: String,
    },
    PersistedFilePublished {
        stream_name: String,
    },
    PersistedFilesReplaced {
        stream_name: String,
    },
    PersistEventPublished {
        stream_name: String,
    },
    SegmentReaderLeaseAcquired {
        stream_name: String,
        lease_id: String,
    },
    SegmentReaderLeaseReleased {
        stream_name: String,
        lease_id: String,
    },
    SegmentDescriptorFetched {
        stream_name: String,
        descriptor: Option<serde_json::Value>,
    },
    ResourceChanged {
        event: Option<ResourceEvent>,
    },
    Error {
        reason: String,
    },
}

impl fmt::Display for ControlResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Envelope(envelope) => write!(
                f,
                "control envelope version=[{}] request_id=[{}] response=[{}]",
                envelope.version, envelope.request_id, envelope.inner
            ),
            Self::ProcessRegistered { process_id, .. } => {
                write!(f, "process registered process_id=[{}]", process_id)
            }
            Self::HeartbeatAccepted { process_id } => {
                write!(f, "heartbeat accepted process_id=[{}]", process_id)
            }
            Self::ShutdownRequested { process_id, reason } => write!(
                f,
                "shutdown requested process_id=[{}] reason=[{}]",
                process_id, reason
            ),
            Self::ProcessUnregistered { process_id } => {
                write!(f, "process unregistered process_id=[{}]", process_id)
            }
            Self::StreamRegistered { stream_name } => {
                write!(f, "stream registered stream_name=[{}]", stream_name)
            }
            Self::SourceRegistered { source_name } => {
                write!(f, "source registered source_name=[{}]", source_name)
            }
            Self::SourceUnregistered { source_name } => {
                write!(f, "source unregistered source_name=[{}]", source_name)
            }
            Self::EngineRegistered { engine_name } => {
                write!(f, "engine registered engine_name=[{}]", engine_name)
            }
            Self::SinkRegistered { sink_name } => {
                write!(f, "sink registered sink_name=[{}]", sink_name)
            }
            Self::StatusUpdated { kind, name } => {
                write!(f, "status updated kind=[{}] name=[{}]", kind, name)
            }
            Self::StreamsListed(response) => write!(
                f,
                "streams listed count=[{}]",
                response.streams.len()
            ),
            Self::StreamFetched(response) => write!(
                f,
                "stream fetched stream_name=[{}] buffer_size=[{}] frame_size=[{}] write_seq=[{}] writer_process_id=[{:?}] writer_epoch=[{}] reader_count=[{}] status=[{}]",
                response.stream.stream_name,
                response.stream.buffer_size,
                response.stream.frame_size,
                response.stream.write_seq,
                response.stream.writer_process_id,
                response.stream.writer_epoch,
                response.stream.reader_count,
                response.stream.status
            ),
            Self::ConfigFetched { config } => write!(
                f,
                "config fetched table_row_capacity=[{}] table_persist_enabled=[{}]",
                config
                    .pointer("/table/row_capacity")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                config
                    .pointer("/table/persist/enabled")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false)
            ),
            Self::TableDropped(result) => write!(
                f,
                "table dropped table_name=[{}] dropped=[{}] sources_removed=[{}] engines_removed=[{}] sinks_removed=[{}] persisted_files_deleted=[{}]",
                result.table_name,
                result.dropped,
                result.sources_removed,
                result.engines_removed,
                result.sinks_removed,
                result.persisted_files_deleted
            ),
            Self::SegmentDescriptorPublished { stream_name } => write!(
                f,
                "segment descriptor published stream_name=[{}]",
                stream_name
            ),
            Self::PersistedFilePublished { stream_name } => {
                write!(f, "persisted file published stream_name=[{}]", stream_name)
            }
            Self::PersistedFilesReplaced { stream_name } => {
                write!(f, "persisted files replaced stream_name=[{}]", stream_name)
            }
            Self::PersistEventPublished { stream_name } => {
                write!(f, "persist event published stream_name=[{}]", stream_name)
            }
            Self::SegmentReaderLeaseAcquired {
                stream_name,
                lease_id,
            } => write!(
                f,
                "segment reader lease acquired stream_name=[{}] lease_id=[{}]",
                stream_name, lease_id
            ),
            Self::SegmentReaderLeaseReleased {
                stream_name,
                lease_id,
            } => write!(
                f,
                "segment reader lease released stream_name=[{}] lease_id=[{}]",
                stream_name, lease_id
            ),
            Self::SegmentDescriptorFetched {
                stream_name,
                descriptor,
            } => write!(
                f,
                "segment descriptor fetched stream_name=[{}] has_descriptor=[{}]",
                stream_name,
                descriptor.is_some()
            ),
            Self::ResourceChanged { event } => write!(
                f,
                "resource changed has_event=[{}]",
                event.is_some()
            ),
            Self::Error { reason } => write!(f, "control error reason=[{}]", reason),
        }
    }
}
