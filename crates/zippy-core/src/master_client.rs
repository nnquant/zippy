use std::collections::BTreeSet;
use std::hint::spin_loop;
use std::io::{BufRead, BufReader, Cursor, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, StringArray};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use zippy_shm_bridge::{ReadResult as SharedReadResult, SharedFrameRing};

use crate::bus_frame::{
    encode_bus_frame_with_timing, parse_bus_frame, patch_bus_frame_publish_done, BusFrameKind,
    BusFrameTiming,
};
use crate::bus_protocol::{
    AttachStreamRequest, ControlRequest, ControlResponse, DetachReaderRequest, DetachWriterRequest,
    GetSegmentDescriptorRequest, GetStreamRequest, HeartbeatRequest, ListStreamsRequest,
    PublishSegmentDescriptorRequest, ReaderDescriptor, RegisterEngineRequest,
    RegisterProcessRequest, RegisterSinkRequest, RegisterSourceRequest, RegisterStreamRequest,
    StreamInfo, UpdateRecordStatusRequest, WriterDescriptor,
};
use crate::{Result, SchemaRef, ZippyError};

/// Synchronous control-plane client for zippy-master.
#[derive(Debug, Clone)]
pub struct MasterClient {
    socket_path: PathBuf,
    process_id: Option<String>,
}

pub struct Writer {
    socket_path: PathBuf,
    descriptor: WriterDescriptor,
    next_write_seq: u64,
    ring: SharedFrameRing,
    closed: bool,
}

pub struct Reader {
    socket_path: PathBuf,
    descriptor: ReaderDescriptor,
    instrument_filter: Option<BTreeSet<String>>,
    xfast: bool,
    next_read_seq: u64,
    ring: SharedFrameRing,
    closed: bool,
}

pub struct TimedReadBatch {
    pub batch: RecordBatch,
    pub bus_timing: Option<BusFrameTiming>,
    pub frame_ready_ns: i64,
    pub read_return_ns: i64,
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
        buffer_size: usize,
        frame_size: usize,
    ) -> Result<()> {
        let response =
            self.send_request(ControlRequest::RegisterStream(RegisterStreamRequest {
                stream_name: stream_name.to_string(),
                buffer_size,
                frame_size,
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
            instrument_ids: None,
        }))?;

        match response {
            ControlResponse::WriterAttached { descriptor } => {
                let ring = open_shared_ring(
                    &descriptor.shm_name,
                    descriptor.buffer_size,
                    descriptor.frame_size,
                )?;
                let next_write_seq = descriptor
                    .next_write_seq
                    .max(ring.next_seq().map_err(shared_ring_error)?);
                Ok(Writer {
                    socket_path: self.socket_path.clone(),
                    next_write_seq,
                    ring,
                    descriptor,
                    closed: false,
                })
            }
            other => Err(unexpected_response("WriterAttached", other)),
        }
    }

    pub fn read_from(&mut self, stream_name: &str) -> Result<Reader> {
        self.attach_reader(stream_name, None, false)
    }

    pub fn read_from_filtered(
        &mut self,
        stream_name: &str,
        instrument_ids: Vec<String>,
    ) -> Result<Reader> {
        self.attach_reader(
            stream_name,
            normalize_instrument_filter(Some(instrument_ids))?,
            false,
        )
    }

    pub fn read_from_with_xfast(&mut self, stream_name: &str, xfast: bool) -> Result<Reader> {
        self.attach_reader(stream_name, None, xfast)
    }

    pub fn read_from_filtered_with_xfast(
        &mut self,
        stream_name: &str,
        instrument_ids: Vec<String>,
        xfast: bool,
    ) -> Result<Reader> {
        self.attach_reader(
            stream_name,
            normalize_instrument_filter(Some(instrument_ids))?,
            xfast,
        )
    }

    fn attach_reader(
        &mut self,
        stream_name: &str,
        instrument_ids: Option<Vec<String>>,
        xfast: bool,
    ) -> Result<Reader> {
        let requested_filter = instrument_filter_request_set(&instrument_ids);
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::ReadFrom(AttachStreamRequest {
            stream_name: stream_name.to_string(),
            process_id,
            instrument_ids,
        }))?;

        match response {
            ControlResponse::ReaderAttached { descriptor } => {
                let ring = open_shared_ring(
                    &descriptor.shm_name,
                    descriptor.buffer_size,
                    descriptor.frame_size,
                )?;
                let next_read_seq = descriptor
                    .next_read_seq
                    .max(ring.seek_latest().map_err(shared_ring_error)?);
                let descriptor_filter = instrument_filter_set(&descriptor.instrument_filter)?;
                if let Some(requested_filter) = &requested_filter {
                    match &descriptor_filter {
                        Some(returned_filter) if returned_filter == requested_filter => {}
                        _ => {
                            return Err(ZippyError::Io {
                                reason: format!(
                                    "descriptor instrument filter mismatch requested=[{}] actual=[{}]",
                                    format_instrument_filter_set(Some(requested_filter)),
                                    format_instrument_filter_set(descriptor_filter.as_ref())
                                ),
                            });
                        }
                    }
                }
                Ok(Reader {
                    socket_path: self.socket_path.clone(),
                    instrument_filter: descriptor_filter,
                    xfast,
                    next_read_seq,
                    ring,
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

    pub fn publish_segment_descriptor(
        &self,
        stream_name: &str,
        descriptor: serde_json::Value,
    ) -> Result<()> {
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::PublishSegmentDescriptor(
            PublishSegmentDescriptorRequest {
                stream_name: stream_name.to_string(),
                process_id,
                descriptor,
            },
        ))?;

        match response {
            ControlResponse::SegmentDescriptorPublished { .. } => Ok(()),
            other => Err(unexpected_response("SegmentDescriptorPublished", other)),
        }
    }

    pub fn publish_segment_descriptor_bytes(
        &self,
        stream_name: &str,
        descriptor_envelope: &[u8],
    ) -> Result<()> {
        let descriptor =
            serde_json::from_slice::<serde_json::Value>(descriptor_envelope).map_err(json_error)?;
        self.publish_segment_descriptor(stream_name, descriptor)
    }

    pub fn get_segment_descriptor(&self, stream_name: &str) -> Result<Option<serde_json::Value>> {
        let process_id = self.require_process_id()?;
        let response = self.send_request(ControlRequest::GetSegmentDescriptor(
            GetSegmentDescriptorRequest {
                stream_name: stream_name.to_string(),
                process_id,
            },
        ))?;

        match response {
            ControlResponse::SegmentDescriptorFetched { descriptor, .. } => Ok(descriptor),
            other => Err(unexpected_response("SegmentDescriptorFetched", other)),
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
        let target_publish_enter_ns = sample_localtime_ns();
        self.write_with_target_publish_enter_ns(batch, target_publish_enter_ns)
    }

    pub fn write_with_target_publish_enter_ns(
        &mut self,
        batch: RecordBatch,
        target_publish_enter_ns: i64,
    ) -> Result<()> {
        let writer_enter_ns = sample_localtime_ns();
        let arrow_payload = encode_batch(&batch)?;
        let arrow_encoded_ns = sample_localtime_ns();
        let instrument_ids = extract_instrument_ids(&batch)?;
        let instrument_ids_done_ns = sample_localtime_ns();
        let publish_start_ns = sample_localtime_ns();
        let frame = match instrument_ids {
            Some(ids) => encode_bus_frame_with_timing(
                &ids,
                &arrow_payload,
                Some(BusFrameTiming {
                    target_publish_enter_ns,
                    writer_enter_ns,
                    arrow_encoded_ns,
                    instrument_ids_done_ns,
                    publish_start_ns,
                    publish_done_ns: 0,
                }),
            )?,
            None => encode_bus_frame_with_timing::<String>(
                &[],
                &arrow_payload,
                Some(BusFrameTiming {
                    target_publish_enter_ns,
                    writer_enter_ns,
                    arrow_encoded_ns,
                    instrument_ids_done_ns,
                    publish_start_ns,
                    publish_done_ns: 0,
                }),
            )?,
        };
        let published_seq = self
            .ring
            .publish_with_builder(frame.len(), |payload| {
                payload.copy_from_slice(&frame);
                let publish_done_ns = sample_localtime_ns();
                patch_bus_frame_publish_done(payload, publish_done_ns)
                    .expect("timing frame patch should succeed");
            })
            .map_err(shared_ring_error)?;
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
        Ok(self.read_with_timing(timeout_ms)?.batch)
    }

    pub fn read_with_timing(&mut self, timeout_ms: Option<u64>) -> Result<TimedReadBatch> {
        let deadline = timeout_ms.map(|timeout| Instant::now() + Duration::from_millis(timeout));

        loop {
            match self
                .ring
                .read(self.next_read_seq)
                .map_err(shared_ring_error)?
            {
                SharedReadResult::Ready(frame) => {
                    let parsed = parse_bus_frame(&frame.payload)?;
                    if let Some(filter) = &self.instrument_filter {
                        match &parsed.kind {
                            BusFrameKind::Legacy => {
                                return Err(ZippyError::Io {
                                    reason: format!(
                                        "filtered reader requires enveloped bus frame reader_id=[{}] stream_name=[{}] seq=[{}]",
                                        self.descriptor.reader_id,
                                        self.descriptor.stream_name,
                                        self.next_read_seq
                                    ),
                                });
                            }
                            BusFrameKind::EnvelopedWithoutDirectory => {
                                self.next_read_seq += 1;
                                continue;
                            }
                            BusFrameKind::EnvelopedWithDirectory { instrument_ids } => {
                                if !instrument_filter_matches(filter, instrument_ids) {
                                    self.next_read_seq += 1;
                                    continue;
                                }
                            }
                        }
                    }

                    let frame_ready_ns = sample_localtime_ns();
                    let batch = decode_batch(parsed.arrow_payload)?;
                    let read_return_ns = sample_localtime_ns();
                    self.next_read_seq += 1;
                    return Ok(TimedReadBatch {
                        batch,
                        bus_timing: parsed.timing,
                        frame_ready_ns,
                        read_return_ns,
                    });
                }
                SharedReadResult::Lagged {
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
                SharedReadResult::Pending => {}
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

            if self.xfast {
                spin_loop();
            } else {
                thread::sleep(Duration::from_micros(10));
            }
        }
    }

    pub fn seek_latest(&mut self) -> Result<()> {
        self.next_read_seq = self.ring.seek_latest().map_err(shared_ring_error)?;
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

fn sample_localtime_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after unix epoch")
        .as_nanos() as i64
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

fn open_shared_ring(
    shm_name: &str,
    buffer_size: usize,
    frame_size: usize,
) -> Result<SharedFrameRing> {
    SharedFrameRing::create_or_open(shm_name, buffer_size, frame_size).map_err(shared_ring_error)
}

fn normalize_instrument_filter(instrument_ids: Option<Vec<String>>) -> Result<Option<Vec<String>>> {
    match instrument_ids {
        Some(ids) if ids.is_empty() => Ok(None),
        Some(ids) => {
            if ids.iter().any(|id| id.is_empty()) {
                return Err(ZippyError::Io {
                    reason: "instrument filter contains empty value".to_string(),
                });
            }
            Ok(Some(ids))
        }
        None => Ok(None),
    }
}

fn instrument_filter_request_set(instrument_ids: &Option<Vec<String>>) -> Option<BTreeSet<String>> {
    instrument_ids
        .as_ref()
        .map(|ids| ids.iter().cloned().collect())
}

fn instrument_filter_set(instrument_ids: &Option<Vec<String>>) -> Result<Option<BTreeSet<String>>> {
    match instrument_ids {
        Some(ids) => {
            if ids.iter().any(|id| id.is_empty()) {
                return Err(ZippyError::Io {
                    reason: "descriptor instrument filter contains empty value".to_string(),
                });
            }

            let normalized = ids.iter().cloned().collect::<BTreeSet<_>>();
            if normalized.is_empty() {
                Ok(None)
            } else {
                Ok(Some(normalized))
            }
        }
        None => Ok(None),
    }
}

fn format_instrument_filter_set(filter: Option<&BTreeSet<String>>) -> String {
    match filter {
        Some(filter) => filter.iter().cloned().collect::<Vec<_>>().join(","),
        None => "unfiltered".to_string(),
    }
}

fn instrument_filter_matches(filter: &BTreeSet<String>, instrument_ids: &[&str]) -> bool {
    instrument_ids
        .iter()
        .any(|instrument_id| filter.contains(*instrument_id))
}

fn extract_instrument_ids(batch: &RecordBatch) -> Result<Option<Vec<String>>> {
    let Ok(instrument_column_index) = batch.schema().index_of("instrument_id") else {
        return Ok(None);
    };
    let instrument_column = batch.column(instrument_column_index);
    let Some(instrument_ids) = instrument_column.as_any().downcast_ref::<StringArray>() else {
        return Err(ZippyError::Io {
            reason: format!(
                "instrument_id column must be utf8 actual_type=[{:?}]",
                instrument_column.data_type()
            ),
        });
    };

    let mut unique_instrument_ids = BTreeSet::new();
    for row_index in 0..instrument_ids.len() {
        if instrument_ids.is_null(row_index) {
            return Err(ZippyError::Io {
                reason: format!(
                    "instrument_id column contains null row_index=[{}]",
                    row_index
                ),
            });
        }
        let instrument_id = instrument_ids.value(row_index);
        if instrument_id.is_empty() {
            return Err(ZippyError::Io {
                reason: format!(
                    "instrument_id column contains empty value row_index=[{}]",
                    row_index
                ),
            });
        }
        unique_instrument_ids.insert(instrument_id.to_string());
    }

    Ok(Some(unique_instrument_ids.into_iter().collect()))
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

fn shared_ring_error(error: zippy_shm_bridge::SharedFrameRingError) -> ZippyError {
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
