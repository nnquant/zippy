use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Cursor, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::bus_protocol::{
    AttachStreamRequest, ControlRequest, ControlResponse, DetachReaderRequest, DetachWriterRequest,
    GetStreamRequest, HeartbeatRequest, ListStreamsRequest, ReaderDescriptor,
    RegisterEngineRequest, RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest,
    RegisterStreamRequest, StreamInfo, UpdateRecordStatusRequest, WriterDescriptor,
};
use crate::{Result, SchemaRef, ZippyError};

/// Synchronous control-plane client for zippy-master.
#[derive(Debug, Clone)]
pub struct MasterClient {
    socket_path: PathBuf,
    process_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Writer {
    socket_path: PathBuf,
    descriptor: WriterDescriptor,
    next_write_seq: u64,
    closed: bool,
}

#[derive(Debug, Clone)]
pub struct Reader {
    socket_path: PathBuf,
    descriptor: ReaderDescriptor,
    next_read_seq: u64,
    closed: bool,
}

#[derive(Debug, Clone)]
struct LocalFrame {
    seq: u64,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct LocalFrameRing {
    buffer_size: usize,
    frame_size: usize,
    latest_seq: u64,
    frames: VecDeque<LocalFrame>,
}

#[derive(Debug)]
enum LocalReadResult {
    Ready(Vec<u8>),
    Pending,
    Lagged { oldest_seq: u64, latest_seq: u64 },
}

type SharedFrameRing = Arc<Mutex<LocalFrameRing>>;
type FrameRingRegistry = Mutex<BTreeMap<String, SharedFrameRing>>;

static FRAME_RING_REGISTRY: OnceLock<FrameRingRegistry> = OnceLock::new();

impl LocalFrameRing {
    fn new(buffer_size: usize, frame_size: usize) -> Self {
        Self {
            buffer_size,
            frame_size,
            latest_seq: 0,
            frames: VecDeque::new(),
        }
    }

    fn next_seq(&self) -> u64 {
        self.latest_seq + 1
    }

    fn publish(&mut self, payload: Vec<u8>) -> Result<u64> {
        if payload.len() > self.frame_size {
            return Err(ZippyError::Io {
                reason: format!(
                    "frame size exceeds configured limit frame_len=[{}] frame_size=[{}]",
                    payload.len(),
                    self.frame_size
                ),
            });
        }

        self.latest_seq += 1;
        self.frames.push_back(LocalFrame {
            seq: self.latest_seq,
            payload,
        });

        while self.frames.len() > self.buffer_size {
            self.frames.pop_front();
        }

        Ok(self.latest_seq)
    }

    fn read(&self, requested_seq: u64) -> LocalReadResult {
        let Some(oldest_seq) = self.frames.front().map(|frame| frame.seq) else {
            return LocalReadResult::Pending;
        };

        if requested_seq < oldest_seq {
            return LocalReadResult::Lagged {
                oldest_seq,
                latest_seq: self.latest_seq,
            };
        }

        for frame in &self.frames {
            if frame.seq == requested_seq {
                return LocalReadResult::Ready(frame.payload.clone());
            }
        }

        LocalReadResult::Pending
    }
}

impl MasterClient {
    pub fn connect(socket_path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            socket_path: socket_path.into(),
            process_id: None,
        })
    }

    pub fn process_id(&self) -> Option<&str> {
        self.process_id.as_deref()
    }

    pub fn register_process(&mut self, app: &str) -> Result<String> {
        let response =
            self.send_request(ControlRequest::RegisterProcess(RegisterProcessRequest {
                app: app.to_string(),
            }))?;

        match response {
            ControlResponse::ProcessRegistered { process_id } => {
                self.process_id = Some(process_id.clone());
                Ok(process_id)
            }
            other => Err(unexpected_response("ProcessRegistered", other)),
        }
    }

    pub fn heartbeat(&self) -> Result<()> {
        let process_id = self.require_process_id()?;
        let response =
            self.send_request(ControlRequest::Heartbeat(HeartbeatRequest { process_id }))?;

        match response {
            ControlResponse::HeartbeatAccepted { .. } => Ok(()),
            other => Err(unexpected_response("HeartbeatAccepted", other)),
        }
    }

    pub fn register_stream(
        &mut self,
        stream_name: &str,
        _schema: SchemaRef,
        ring_capacity: usize,
    ) -> Result<()> {
        let response =
            self.send_request(ControlRequest::RegisterStream(RegisterStreamRequest {
                stream_name: stream_name.to_string(),
                buffer_size: ring_capacity,
                frame_size: ring_capacity,
            }))?;

        match response {
            ControlResponse::StreamRegistered { .. } => Ok(()),
            other => Err(unexpected_response("StreamRegistered", other)),
        }
    }

    pub fn register_source(
        &mut self,
        source_name: &str,
        source_type: &str,
        output_stream: &str,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let response =
            self.send_request(ControlRequest::RegisterSource(RegisterSourceRequest {
                source_name: source_name.to_string(),
                source_type: source_type.to_string(),
                process_id,
                output_stream: output_stream.to_string(),
                config,
            }))?;

        match response {
            ControlResponse::SourceRegistered { .. } => Ok(()),
            other => Err(unexpected_response("SourceRegistered", other)),
        }
    }

    pub fn register_engine(
        &mut self,
        engine_name: &str,
        engine_type: &str,
        input_stream: &str,
        output_stream: &str,
        sink_names: Vec<String>,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let response =
            self.send_request(ControlRequest::RegisterEngine(RegisterEngineRequest {
                engine_name: engine_name.to_string(),
                engine_type: engine_type.to_string(),
                process_id,
                input_stream: input_stream.to_string(),
                output_stream: output_stream.to_string(),
                sink_names,
                config,
            }))?;

        match response {
            ControlResponse::EngineRegistered { .. } => Ok(()),
            other => Err(unexpected_response("EngineRegistered", other)),
        }
    }

    pub fn register_sink(
        &mut self,
        sink_name: &str,
        sink_type: &str,
        input_stream: &str,
        config: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::RegisterSink(RegisterSinkRequest {
            sink_name: sink_name.to_string(),
            sink_type: sink_type.to_string(),
            process_id,
            input_stream: input_stream.to_string(),
            config,
        }))?;

        match response {
            ControlResponse::SinkRegistered { .. } => Ok(()),
            other => Err(unexpected_response("SinkRegistered", other)),
        }
    }

    pub fn update_status(
        &self,
        kind: &str,
        name: &str,
        status: &str,
        metrics: Option<serde_json::Value>,
    ) -> Result<()> {
        let response =
            self.send_request(ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
                kind: kind.to_string(),
                name: name.to_string(),
                status: status.to_string(),
                metrics,
            }))?;

        match response {
            ControlResponse::StatusUpdated { .. } => Ok(()),
            other => Err(unexpected_response("StatusUpdated", other)),
        }
    }

    pub fn write_to(&mut self, stream_name: &str) -> Result<Writer> {
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::WriteTo(AttachStreamRequest {
            stream_name: stream_name.to_string(),
            process_id,
        }))?;

        match response {
            ControlResponse::WriterAttached { descriptor } => {
                let next_write_seq = next_write_seq_for_writer(&descriptor)?;
                Ok(Writer {
                    socket_path: self.socket_path.clone(),
                    next_write_seq,
                    descriptor,
                    closed: false,
                })
            }
            other => Err(unexpected_response("WriterAttached", other)),
        }
    }

    pub fn read_from(&mut self, stream_name: &str) -> Result<Reader> {
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::ReadFrom(AttachStreamRequest {
            stream_name: stream_name.to_string(),
            process_id,
        }))?;

        match response {
            ControlResponse::ReaderAttached { descriptor } => {
                let next_read_seq = next_read_seq_for_reader(&descriptor)?;
                Ok(Reader {
                    socket_path: self.socket_path.clone(),
                    next_read_seq,
                    descriptor,
                    closed: false,
                })
            }
            other => Err(unexpected_response("ReaderAttached", other)),
        }
    }

    pub fn list_streams(&self) -> Result<Vec<StreamInfo>> {
        let response = self.send_request(ControlRequest::ListStreams(ListStreamsRequest {}))?;

        match response {
            ControlResponse::StreamsListed(response) => Ok(response.streams),
            other => Err(unexpected_response("StreamsListed", other)),
        }
    }

    pub fn get_stream(&self, stream_name: &str) -> Result<StreamInfo> {
        let response = self.send_request(ControlRequest::GetStream(GetStreamRequest {
            stream_name: stream_name.to_string(),
        }))?;

        match response {
            ControlResponse::StreamFetched(response) => Ok(response.stream),
            other => Err(unexpected_response("StreamFetched", other)),
        }
    }

    fn require_process_id(&self) -> Result<String> {
        self.process_id.clone().ok_or(ZippyError::InvalidState {
            status: "master client process not registered",
        })
    }

    fn send_request(&self, request: ControlRequest) -> Result<ControlResponse> {
        let mut stream = UnixStream::connect(&self.socket_path).map_err(io_error)?;
        let payload = serde_json::to_string(&request).map_err(json_error)?;
        stream.write_all(payload.as_bytes()).map_err(io_error)?;
        stream.write_all(b"\n").map_err(io_error)?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(io_error)?;

        let mut response_line = String::new();
        let mut reader = BufReader::new(stream);
        reader.read_line(&mut response_line).map_err(io_error)?;
        let response: ControlResponse =
            serde_json::from_str(response_line.trim_end()).map_err(json_error)?;

        match response {
            ControlResponse::Error { reason } => Err(ZippyError::Io { reason }),
            other => Ok(other),
        }
    }
}

impl Writer {
    pub fn descriptor(&self) -> &WriterDescriptor {
        &self.descriptor
    }

    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let payload = encode_batch(&batch)?;
        let ring = shared_frame_ring(
            &self.descriptor.shm_name,
            self.descriptor.buffer_size,
            self.descriptor.frame_size,
        )?;
        let mut ring = lock_frame_ring(&ring)?;
        let published_seq = ring.publish(payload)?;
        self.next_write_seq = published_seq + 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let response = send_control_request(
            &self.socket_path,
            ControlRequest::CloseWriter(DetachWriterRequest {
                stream_name: self.descriptor.stream_name.clone(),
                process_id: self.descriptor.process_id.clone(),
                writer_id: self.descriptor.writer_id.clone(),
            }),
        )?;

        match response {
            ControlResponse::WriterDetached { .. } => {
                self.closed = true;
                Ok(())
            }
            other => Err(unexpected_response("WriterDetached", other)),
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Reader {
    pub fn descriptor(&self) -> &ReaderDescriptor {
        &self.descriptor
    }

    pub fn read(&mut self, timeout_ms: Option<u64>) -> Result<RecordBatch> {
        let deadline = timeout_ms.map(|timeout| Instant::now() + Duration::from_millis(timeout));
        let ring = shared_frame_ring(
            &self.descriptor.shm_name,
            self.descriptor.buffer_size,
            self.descriptor.frame_size,
        )?;

        loop {
            match lock_frame_ring(&ring)?.read(self.next_read_seq) {
                LocalReadResult::Ready(payload) => {
                    let batch = decode_batch(&payload)?;
                    self.next_read_seq += 1;
                    return Ok(batch);
                }
                LocalReadResult::Lagged {
                    oldest_seq,
                    latest_seq,
                } => {
                    return Err(ZippyError::Io {
                        reason: format!(
                            "reader lagged reader_id=[{}] requested_seq=[{}] oldest_available_seq=[{}] latest_write_seq=[{}]",
                            self.descriptor.reader_id,
                            self.next_read_seq,
                            oldest_seq,
                            latest_seq
                        ),
                    });
                }
                LocalReadResult::Pending => {}
            }

            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return Err(ZippyError::Io {
                        reason: format!(
                            "reader timed out reader_id=[{}] stream_name=[{}] next_read_seq=[{}]",
                            self.descriptor.reader_id,
                            self.descriptor.stream_name,
                            self.next_read_seq
                        ),
                    });
                }
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn seek_latest(&mut self) -> Result<()> {
        let ring = shared_frame_ring(
            &self.descriptor.shm_name,
            self.descriptor.buffer_size,
            self.descriptor.frame_size,
        )?;
        self.next_read_seq = lock_frame_ring(&ring)?.next_seq();
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let response = send_control_request(
            &self.socket_path,
            ControlRequest::CloseReader(DetachReaderRequest {
                stream_name: self.descriptor.stream_name.clone(),
                process_id: self.descriptor.process_id.clone(),
                reader_id: self.descriptor.reader_id.clone(),
            }),
        )?;

        match response {
            ControlResponse::ReaderDetached { .. } => {
                self.closed = true;
                Ok(())
            }
            other => Err(unexpected_response("ReaderDetached", other)),
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn send_control_request(socket_path: &Path, request: ControlRequest) -> Result<ControlResponse> {
    let mut stream = UnixStream::connect(socket_path).map_err(io_error)?;
    let payload = serde_json::to_string(&request).map_err(json_error)?;
    stream.write_all(payload.as_bytes()).map_err(io_error)?;
    stream.write_all(b"\n").map_err(io_error)?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(io_error)?;

    let mut response_line = String::new();
    let mut reader = BufReader::new(stream);
    reader.read_line(&mut response_line).map_err(io_error)?;
    let response: ControlResponse =
        serde_json::from_str(response_line.trim_end()).map_err(json_error)?;

    match response {
        ControlResponse::Error { reason } => Err(ZippyError::Io { reason }),
        other => Ok(other),
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut payload, &batch.schema()).map_err(arrow_error)?;
        writer.write(batch).map_err(arrow_error)?;
        writer.finish().map_err(arrow_error)?;
    }
    Ok(payload)
}

fn decode_batch(payload: &[u8]) -> Result<RecordBatch> {
    let mut reader =
        StreamReader::try_new(Cursor::new(payload.to_vec()), None).map_err(arrow_error)?;
    match reader.next() {
        Some(batch) => batch.map_err(arrow_error),
        None => Err(ZippyError::Io {
            reason: "missing record batch payload=[empty]".to_string(),
        }),
    }
}

fn next_write_seq_for_writer(descriptor: &WriterDescriptor) -> Result<u64> {
    let ring = shared_frame_ring(
        &descriptor.shm_name,
        descriptor.buffer_size,
        descriptor.frame_size,
    )?;
    let next_seq = lock_frame_ring(&ring)?.next_seq();
    Ok(descriptor.next_write_seq.max(next_seq))
}

fn next_read_seq_for_reader(descriptor: &ReaderDescriptor) -> Result<u64> {
    let ring = shared_frame_ring(
        &descriptor.shm_name,
        descriptor.buffer_size,
        descriptor.frame_size,
    )?;
    let next_seq = lock_frame_ring(&ring)?.next_seq();
    Ok(descriptor.next_read_seq.max(next_seq))
}

fn shared_frame_ring(
    shm_name: &str,
    buffer_size: usize,
    frame_size: usize,
) -> Result<SharedFrameRing> {
    let registry = FRAME_RING_REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut registry = registry.lock().map_err(|_| ZippyError::Io {
        reason: "frame ring registry lock poisoned".to_string(),
    })?;

    if let Some(existing) = registry.get(shm_name) {
        let ring = existing.clone();
        let ring_guard = lock_frame_ring(&ring)?;
        if ring_guard.buffer_size != buffer_size || ring_guard.frame_size != frame_size {
            return Err(ZippyError::Io {
                reason: format!(
                    "frame ring descriptor mismatch shm_name=[{}] buffer_size=[{}] frame_size=[{}]",
                    shm_name, buffer_size, frame_size
                ),
            });
        }
        drop(ring_guard);
        return Ok(ring);
    }

    let ring = Arc::new(Mutex::new(LocalFrameRing::new(buffer_size, frame_size)));
    registry.insert(shm_name.to_string(), ring.clone());
    Ok(ring)
}

fn lock_frame_ring(ring: &SharedFrameRing) -> Result<std::sync::MutexGuard<'_, LocalFrameRing>> {
    ring.lock().map_err(|_| ZippyError::Io {
        reason: "frame ring lock poisoned".to_string(),
    })
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn json_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn arrow_error(error: arrow::error::ArrowError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn unexpected_response(expected: &str, response: ControlResponse) -> ZippyError {
    ZippyError::Io {
        reason: format!(
            "unexpected control response expected=[{}] actual=[{}]",
            expected, response
        ),
    }
}
