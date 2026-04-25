use serde::{Deserialize, Serialize};
use std::fmt;

pub const BUS_LAYOUT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterProcessRequest {
    pub app: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub process_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterStreamRequest {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSourceRequest {
    pub source_name: String,
    pub source_type: String,
    pub process_id: String,
    pub output_stream: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterEngineRequest {
    pub engine_name: String,
    pub engine_type: String,
    pub process_id: String,
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
    pub input_stream: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateRecordStatusRequest {
    pub kind: String,
    pub name: String,
    pub status: String,
    pub metrics: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachStreamRequest {
    pub stream_name: String,
    pub process_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetachWriterRequest {
    pub stream_name: String,
    pub process_id: String,
    pub writer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetachReaderRequest {
    pub stream_name: String,
    pub process_id: String,
    pub reader_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub write_seq: u64,
    pub writer_process_id: Option<String>,
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
pub struct PublishSegmentDescriptorRequest {
    pub stream_name: String,
    pub process_id: String,
    pub descriptor: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSegmentDescriptorRequest {
    pub stream_name: String,
    pub process_id: String,
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
pub struct WriterDescriptor {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub layout_version: u32,
    pub shm_name: String,
    pub writer_id: String,
    pub process_id: String,
    pub next_write_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderDescriptor {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub layout_version: u32,
    pub shm_name: String,
    pub reader_id: String,
    pub process_id: String,
    pub next_read_seq: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instrument_filter: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
    Heartbeat(HeartbeatRequest),
    RegisterStream(RegisterStreamRequest),
    RegisterSource(RegisterSourceRequest),
    RegisterEngine(RegisterEngineRequest),
    RegisterSink(RegisterSinkRequest),
    UpdateStatus(UpdateRecordStatusRequest),
    WriteTo(AttachStreamRequest),
    ReadFrom(AttachStreamRequest),
    CloseWriter(DetachWriterRequest),
    CloseReader(DetachReaderRequest),
    ListStreams(ListStreamsRequest),
    GetStream(GetStreamRequest),
    PublishSegmentDescriptor(PublishSegmentDescriptorRequest),
    GetSegmentDescriptor(GetSegmentDescriptorRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    ProcessRegistered {
        process_id: String,
    },
    HeartbeatAccepted {
        process_id: String,
    },
    StreamRegistered {
        stream_name: String,
    },
    SourceRegistered {
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
    WriterAttached {
        descriptor: WriterDescriptor,
    },
    ReaderAttached {
        descriptor: ReaderDescriptor,
    },
    WriterDetached {
        stream_name: String,
        writer_id: String,
    },
    ReaderDetached {
        stream_name: String,
        reader_id: String,
    },
    StreamsListed(ListStreamsResponse),
    StreamFetched(GetStreamResponse),
    SegmentDescriptorPublished {
        stream_name: String,
    },
    SegmentDescriptorFetched {
        stream_name: String,
        descriptor: Option<serde_json::Value>,
    },
    Error {
        reason: String,
    },
}

impl fmt::Display for ControlResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProcessRegistered { process_id } => {
                write!(f, "process registered process_id=[{}]", process_id)
            }
            Self::HeartbeatAccepted { process_id } => {
                write!(f, "heartbeat accepted process_id=[{}]", process_id)
            }
            Self::StreamRegistered { stream_name } => {
                write!(f, "stream registered stream_name=[{}]", stream_name)
            }
            Self::SourceRegistered { source_name } => {
                write!(f, "source registered source_name=[{}]", source_name)
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
            Self::WriterAttached { descriptor } => write!(
                f,
                "writer attached stream_name=[{}] writer_id=[{}] process_id=[{}] buffer_size=[{}] frame_size=[{}] layout_version=[{}] shm_name=[{}] next_write_seq=[{}]",
                descriptor.stream_name,
                descriptor.writer_id,
                descriptor.process_id,
                descriptor.buffer_size,
                descriptor.frame_size,
                descriptor.layout_version,
                descriptor.shm_name,
                descriptor.next_write_seq
            ),
            Self::ReaderAttached { descriptor } => write!(
                f,
                "reader attached stream_name=[{}] reader_id=[{}] process_id=[{}] buffer_size=[{}] frame_size=[{}] layout_version=[{}] shm_name=[{}] next_read_seq=[{}] instrument_filter=[{}]",
                descriptor.stream_name,
                descriptor.reader_id,
                descriptor.process_id,
                descriptor.buffer_size,
                descriptor.frame_size,
                descriptor.layout_version,
                descriptor.shm_name,
                descriptor.next_read_seq,
                descriptor
                    .instrument_filter
                    .as_ref()
                    .map(|filters| filters.join(","))
                    .unwrap_or_default()
            ),
            Self::WriterDetached {
                stream_name,
                writer_id,
            } => write!(
                f,
                "writer detached stream_name=[{}] writer_id=[{}]",
                stream_name, writer_id
            ),
            Self::ReaderDetached {
                stream_name,
                reader_id,
            } => write!(
                f,
                "reader detached stream_name=[{}] reader_id=[{}]",
                stream_name, reader_id
            ),
            Self::StreamsListed(response) => write!(
                f,
                "streams listed count=[{}]",
                response.streams.len()
            ),
            Self::StreamFetched(response) => write!(
                f,
                "stream fetched stream_name=[{}] buffer_size=[{}] frame_size=[{}] write_seq=[{}] writer_process_id=[{:?}] reader_count=[{}] status=[{}]",
                response.stream.stream_name,
                response.stream.buffer_size,
                response.stream.frame_size,
                response.stream.write_seq,
                response.stream.writer_process_id,
                response.stream.reader_count,
                response.stream.status
            ),
            Self::SegmentDescriptorPublished { stream_name } => write!(
                f,
                "segment descriptor published stream_name=[{}]",
                stream_name
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
            Self::Error { reason } => write!(f, "control error reason=[{}]", reason),
        }
    }
}
