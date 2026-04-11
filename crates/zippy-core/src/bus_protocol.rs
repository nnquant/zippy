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
    pub ring_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachStreamRequest {
    pub stream_name: String,
    pub process_id: String,
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
    pub ring_capacity: usize,
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
    pub ring_capacity: usize,
    pub layout_version: u32,
    pub shm_name: String,
    pub writer_id: String,
    pub process_id: String,
    pub next_write_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderDescriptor {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub layout_version: u32,
    pub shm_name: String,
    pub reader_id: String,
    pub process_id: String,
    pub next_read_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
    Heartbeat(HeartbeatRequest),
    RegisterStream(RegisterStreamRequest),
    WriteTo(AttachStreamRequest),
    ReadFrom(AttachStreamRequest),
    CloseWriter(DetachWriterRequest),
    CloseReader(DetachReaderRequest),
    ListStreams(ListStreamsRequest),
    GetStream(GetStreamRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    ProcessRegistered { process_id: String },
    HeartbeatAccepted { process_id: String },
    StreamRegistered { stream_name: String },
    WriterAttached { descriptor: WriterDescriptor },
    ReaderAttached { descriptor: ReaderDescriptor },
    WriterDetached { stream_name: String, writer_id: String },
    ReaderDetached { stream_name: String, reader_id: String },
    StreamsListed(ListStreamsResponse),
    StreamFetched(GetStreamResponse),
    Error { reason: String },
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
            Self::WriterAttached { descriptor } => write!(
                f,
                "writer attached stream_name=[{}] writer_id=[{}] process_id=[{}] ring_capacity=[{}] layout_version=[{}] shm_name=[{}] next_write_seq=[{}]",
                descriptor.stream_name,
                descriptor.writer_id,
                descriptor.process_id,
                descriptor.ring_capacity,
                descriptor.layout_version,
                descriptor.shm_name,
                descriptor.next_write_seq
            ),
            Self::ReaderAttached { descriptor } => write!(
                f,
                "reader attached stream_name=[{}] reader_id=[{}] process_id=[{}] ring_capacity=[{}] layout_version=[{}] shm_name=[{}] next_read_seq=[{}]",
                descriptor.stream_name,
                descriptor.reader_id,
                descriptor.process_id,
                descriptor.ring_capacity,
                descriptor.layout_version,
                descriptor.shm_name,
                descriptor.next_read_seq
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
                "stream fetched stream_name=[{}] ring_capacity=[{}] writer_process_id=[{:?}] reader_count=[{}] status=[{}]",
                response.stream.stream_name,
                response.stream.ring_capacity,
                response.stream.writer_process_id,
                response.stream.reader_count,
                response.stream.status
            ),
            Self::Error { reason } => write!(f, "control error reason=[{}]", reason),
        }
    }
}
