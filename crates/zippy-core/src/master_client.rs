use std::fs;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::bus_protocol::{
    AttachStreamRequest, ControlRequest, ControlResponse, DetachReaderRequest,
    DetachWriterRequest, GetStreamRequest, HeartbeatRequest, ListStreamsRequest,
    ReaderDescriptor, RegisterEngineRequest, RegisterProcessRequest, RegisterSinkRequest,
    RegisterSourceRequest, RegisterStreamRequest, StreamInfo, UpdateRecordStatusRequest,
    WriterDescriptor,
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
        let response = self.send_request(ControlRequest::Heartbeat(HeartbeatRequest {
            process_id,
        }))?;

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
                ring_capacity,
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
        let response = self.send_request(ControlRequest::RegisterSource(RegisterSourceRequest {
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
        let response = self.send_request(ControlRequest::RegisterEngine(RegisterEngineRequest {
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
        let response = self.send_request(ControlRequest::UpdateStatus(UpdateRecordStatusRequest {
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
            ControlResponse::WriterAttached { descriptor } => Ok(Writer {
                socket_path: self.socket_path.clone(),
                next_write_seq: descriptor.next_write_seq,
                descriptor,
                closed: false,
            }),
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
            ControlResponse::ReaderAttached { descriptor } => Ok(Reader {
                socket_path: self.socket_path.clone(),
                next_read_seq: descriptor.next_read_seq,
                descriptor,
                closed: false,
            }),
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
        let stream_dir = Path::new(&self.descriptor.shm_name);
        fs::create_dir_all(stream_dir).map_err(io_error)?;

        let file_name = seq_file_name(self.next_write_seq);
        let temp_name = format!("{file_name}.tmp");
        let temp_path = stream_dir.join(temp_name);
        let final_path = stream_dir.join(file_name);

        fs::write(&temp_path, payload).map_err(io_error)?;
        fs::rename(&temp_path, &final_path).map_err(io_error)?;
        prune_old_batch_files(stream_dir, self.descriptor.ring_capacity)?;
        self.next_write_seq += 1;
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
        let stream_dir = Path::new(&self.descriptor.shm_name);

        loop {
            let sequences = list_available_sequences(stream_dir)?;
            if let Some((&oldest_seq, &latest_seq)) = sequences.first().zip(sequences.last()) {
                if self.next_read_seq < oldest_seq {
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

                if sequences.binary_search(&self.next_read_seq).is_ok() {
                    let path = stream_dir.join(seq_file_name(self.next_read_seq));
                    let payload = fs::read(path).map_err(io_error)?;
                    let batch = decode_batch(&payload)?;
                    self.next_read_seq += 1;
                    return Ok(batch);
                }
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
        let stream_dir = Path::new(&self.descriptor.shm_name);
        let sequences = list_available_sequences(stream_dir)?;
        self.next_read_seq = sequences
            .last()
            .map(|seq| seq + 1)
            .unwrap_or(self.next_read_seq);
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

fn prune_old_batch_files(stream_dir: &Path, ring_capacity: usize) -> Result<()> {
    let sequences = list_available_sequences(stream_dir)?;
    if sequences.len() <= ring_capacity {
        return Ok(());
    }

    let remove_count = sequences.len() - ring_capacity;
    for sequence in sequences.into_iter().take(remove_count) {
        fs::remove_file(stream_dir.join(seq_file_name(sequence))).map_err(io_error)?;
    }
    Ok(())
}

fn list_available_sequences(stream_dir: &Path) -> Result<Vec<u64>> {
    if !stream_dir.exists() {
        return Ok(Vec::new());
    }

    let mut sequences = Vec::new();
    for entry in fs::read_dir(stream_dir).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        let file_type = entry.file_type().map_err(io_error)?;
        if !file_type.is_file() {
            continue;
        }

        let Some(sequence) = parse_seq_file_name(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        sequences.push(sequence);
    }
    sequences.sort_unstable();
    Ok(sequences)
}

fn seq_file_name(sequence: u64) -> String {
    format!("seq_{sequence:020}.ipc")
}

fn parse_seq_file_name(name: &str) -> Option<u64> {
    if !name.starts_with("seq_") || !name.ends_with(".ipc") {
        return None;
    }
    let value = &name[4..name.len() - 4];
    value.parse::<u64>().ok()
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
