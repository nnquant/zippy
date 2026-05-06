use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use zippy_core::{
    ControlEndpoint, Engine, MasterClient, Result, SegmentTableView, StreamInfo, ZippyError,
};
use zippy_engines::{StreamTableDescriptorPublisher, StreamTableMaterializer};
use zippy_segment_store::{
    compile_schema as compile_segment_schema, ActiveSegmentDescriptor, ActiveSegmentReader,
    ColumnSpec, ColumnType, CompiledSchema, LayoutPlan, RowSpanView,
};

/// Native Rust GatewayServer configuration.
#[derive(Debug, Clone)]
pub struct GatewayServerConfig {
    pub endpoint: String,
    pub master_endpoint: ControlEndpoint,
    pub token: Option<String>,
    pub max_write_rows: Option<usize>,
}

/// Native TCP gateway for cross-platform remote writes and queries.
pub struct GatewayServer {
    endpoint: String,
    state: Arc<GatewayState>,
    join_handle: Option<JoinHandle<()>>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

struct GatewayState {
    master_endpoint: ControlEndpoint,
    master: Arc<Mutex<Option<MasterClient>>>,
    token: Option<String>,
    max_write_rows: Option<usize>,
    writers: Mutex<BTreeMap<String, GatewayTableWriter>>,
    metrics: Mutex<GatewayMetrics>,
    stopped: AtomicBool,
}

struct GatewayTableWriter {
    materializer: StreamTableMaterializer,
}

struct SegmentReaderLeaseGuard {
    master: Arc<Mutex<Option<MasterClient>>>,
    source: String,
    lease_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct GatewayMetrics {
    requests_total: u64,
    auth_failures_total: u64,
    errors_total: u64,
    write_batches_total: u64,
    written_rows_total: u64,
    write_rejections_total: u64,
    collect_requests_total: u64,
    subscribe_clients_total: u64,
}

struct MasterDescriptorPublisher {
    master: Arc<Mutex<Option<MasterClient>>>,
    stream_name: String,
}

impl GatewayServer {
    /// Create a native GatewayServer.
    pub fn new(config: GatewayServerConfig) -> Result<Self> {
        if let Some(max_write_rows) = config.max_write_rows {
            if max_write_rows == 0 {
                return Err(ZippyError::InvalidConfig {
                    reason: "max_write_rows must be positive".to_string(),
                });
            }
        }
        Ok(Self {
            endpoint: normalize_endpoint(&config.endpoint),
            state: Arc::new(GatewayState {
                master_endpoint: config.master_endpoint,
                master: Arc::new(Mutex::new(None)),
                token: config.token,
                max_write_rows: config.max_write_rows,
                writers: Mutex::new(BTreeMap::new()),
                metrics: Mutex::new(GatewayMetrics::default()),
                stopped: AtomicBool::new(false),
            }),
            join_handle: None,
            heartbeat_handle: None,
        })
    }

    /// Start serving TCP requests in a background thread.
    pub fn start(mut self) -> Result<Self> {
        let listener = TcpListener::bind(&self.endpoint).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to bind gateway endpoint=[{}] error=[{}]",
                self.endpoint, error
            ),
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to set gateway listener nonblocking error=[{}]",
                    error
                ),
            })?;
        self.endpoint = listener
            .local_addr()
            .map_err(|error| ZippyError::Io {
                reason: format!("failed to read gateway local addr error=[{}]", error),
            })?
            .to_string();

        let state = Arc::clone(&self.state);
        self.join_handle = Some(thread::spawn(move || serve_loop(listener, state)));
        let state = Arc::clone(&self.state);
        self.heartbeat_handle = Some(thread::spawn(move || heartbeat_loop(state)));
        Ok(self)
    }

    /// Return the effective listening endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Return GatewayServer metrics as JSON.
    pub fn metrics(&self) -> Value {
        let metrics = self.state.metrics.lock().unwrap().clone();
        json!({
            "endpoint": self.endpoint,
            "running": !self.state.stopped.load(Ordering::SeqCst),
            "requests_total": metrics.requests_total,
            "auth_failures_total": metrics.auth_failures_total,
            "errors_total": metrics.errors_total,
            "write_batches_total": metrics.write_batches_total,
            "written_rows_total": metrics.written_rows_total,
            "write_rejections_total": metrics.write_rejections_total,
            "collect_requests_total": metrics.collect_requests_total,
            "subscribe_clients_total": metrics.subscribe_clients_total,
        })
    }

    /// Stop the background gateway listener and flush table writers.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(&self.endpoint);
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.heartbeat_handle.take() {
            let _ = handle.join();
        }
        let mut writers = self.state.writers.lock().unwrap();
        for writer in writers.values_mut() {
            let _ = writer.materializer.on_stop();
        }
        writers.clear();
    }
}

impl Drop for GatewayServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl StreamTableDescriptorPublisher for MasterDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> Result<()> {
        let mut master = self.master.lock().unwrap();
        let master = master.as_mut().ok_or_else(|| ZippyError::InvalidState {
            status: "gateway master client is not initialized",
        })?;
        master.publish_segment_descriptor_bytes(&self.stream_name, &descriptor_envelope)
    }
}

impl GatewayState {
    fn handle_request(&self, header: Value, payload: Vec<u8>) -> Result<(Value, Vec<u8>)> {
        self.increment_metric(|metrics| metrics.requests_total += 1);
        self.authorize(&header)?;
        match header
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "write_batch" => self.handle_write_batch(header, payload),
            "collect" => self.handle_collect(header),
            "get_stream" => self.handle_get_stream(header),
            "list_streams" => self.handle_list_streams(),
            "metrics" => Ok((
                json!({"status": "ok", "metrics": self.metrics_value()}),
                vec![],
            )),
            kind => Err(ZippyError::InvalidConfig {
                reason: format!("unsupported remote gateway request kind=[{}]", kind),
            }),
        }
    }

    fn handle_write_batch(&self, header: Value, payload: Vec<u8>) -> Result<(Value, Vec<u8>)> {
        let stream_name = header
            .get("stream_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "write_batch requires stream_name".to_string(),
            })?;
        let batches = decode_ipc_batches(&payload)?;
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if let Some(max_write_rows) = self.max_write_rows {
            if row_count > max_write_rows {
                self.increment_metric(|metrics| metrics.write_rejections_total += 1);
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "write batch row count exceeds limit rows=[{}] max_write_rows=[{}]",
                        row_count, max_write_rows
                    ),
                });
            }
        }
        for batch in batches {
            self.write_batch(stream_name, batch)?;
        }
        self.increment_metric(|metrics| {
            metrics.write_batches_total += 1;
            metrics.written_rows_total += row_count as u64;
        });
        Ok((json!({"status": "ok"}), vec![]))
    }

    fn handle_collect(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        self.increment_metric(|metrics| metrics.collect_requests_total += 1);
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "collect requires source".to_string(),
            })?;
        let plan = header
            .get("plan")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let batch = {
            let writers = self.writers.lock().unwrap();
            writers
                .get(source)
                .map(|writer| writer.materializer.active_record_batch())
                .transpose()?
        };
        let batch = match batch {
            Some(batch) => batch,
            None => self.collect_segment_source(source)?,
        };
        let batch = apply_collect_plan(batch, &plan)?;
        Ok((json!({"status": "ok"}), encode_ipc_table(&batch)?))
    }

    fn write_batch(&self, stream_name: &str, batch: RecordBatch) -> Result<()> {
        let mut writers = self.writers.lock().unwrap();
        if !writers.contains_key(stream_name) {
            let writer = self.create_writer(stream_name, batch.schema())?;
            writers.insert(stream_name.to_string(), writer);
        }
        let writer = writers.get_mut(stream_name).expect("writer just inserted");
        writer
            .materializer
            .on_data(SegmentTableView::from_record_batch(batch))?;
        writer.materializer.on_flush()?;
        Ok(())
    }

    fn create_writer(&self, stream_name: &str, schema: SchemaRef) -> Result<GatewayTableWriter> {
        {
            self.with_master(|master| {
                master.register_stream(stream_name, Arc::clone(&schema), 64, 4096)?;
                master.register_source(
                    &format!("gateway.{}", stream_name),
                    "gateway",
                    stream_name,
                    json!({}),
                )?;
                Ok(())
            })?;
        }

        let publisher = Arc::new(MasterDescriptorPublisher {
            master: Arc::clone(&self.master),
            stream_name: stream_name.to_string(),
        });
        let materializer =
            StreamTableMaterializer::new(stream_name, schema)?.with_descriptor_publisher(publisher);
        let descriptor = materializer.active_descriptor_envelope_bytes()?;
        self.with_master(|master| {
            master.publish_segment_descriptor_bytes(stream_name, &descriptor)
        })?;
        Ok(GatewayTableWriter { materializer })
    }

    fn handle_get_stream(&self, header: Value) -> Result<(Value, Vec<u8>)> {
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "get_stream requires source".to_string(),
            })?;
        let stream = self.with_master(|master| master.get_stream(source))?;
        let schema_payload = {
            let writers = self.writers.lock().unwrap();
            writers
                .get(source)
                .map(|writer| encode_ipc_schema(&writer.materializer.output_schema()))
                .transpose()?
        };
        let schema_payload = match schema_payload {
            Some(payload) => payload,
            None => encode_ipc_schema(&Arc::new(arrow_schema_from_stream_metadata(
                &stream.schema,
            )?))?,
        };
        Ok((json!({"status": "ok", "stream": stream}), schema_payload))
    }

    fn handle_list_streams(&self) -> Result<(Value, Vec<u8>)> {
        let streams = self.with_master(|master| master.list_streams())?;
        Ok((json!({"status": "ok", "streams": streams}), vec![]))
    }

    fn authorize(&self, header: &Value) -> Result<()> {
        let Some(token) = &self.token else {
            return Ok(());
        };
        if header.get("token").and_then(Value::as_str) == Some(token.as_str()) {
            return Ok(());
        }
        self.increment_metric(|metrics| metrics.auth_failures_total += 1);
        Err(ZippyError::Io {
            reason: "unauthorized remote gateway request".to_string(),
        })
    }

    fn metrics_value(&self) -> Value {
        let metrics = self.metrics.lock().unwrap().clone();
        json!({
            "requests_total": metrics.requests_total,
            "auth_failures_total": metrics.auth_failures_total,
            "errors_total": metrics.errors_total,
            "write_batches_total": metrics.write_batches_total,
            "written_rows_total": metrics.written_rows_total,
            "write_rejections_total": metrics.write_rejections_total,
            "collect_requests_total": metrics.collect_requests_total,
            "subscribe_clients_total": metrics.subscribe_clients_total,
        })
    }

    fn increment_metric(&self, update: impl FnOnce(&mut GatewayMetrics)) {
        let mut metrics = self.metrics.lock().unwrap();
        update(&mut metrics);
    }

    fn handle_subscribe_table_stream(&self, stream: &mut TcpStream, header: Value) -> Result<()> {
        let source = header
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "subscribe_table requires source".to_string(),
            })?;
        if !header.get("filter").unwrap_or(&Value::Null).is_null() {
            return Err(ZippyError::InvalidConfig {
                reason: "native gateway subscribe_table filter is not implemented".to_string(),
            });
        }
        let batch_size = header
            .get("batch_size")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .filter(|value| *value > 0);
        let count = header
            .get("count")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .filter(|value| *value > 0);
        let throttle = header
            .get("throttle_ms")
            .and_then(Value::as_u64)
            .map(Duration::from_millis);
        self.increment_metric(|metrics| metrics.subscribe_clients_total += 1);
        write_frame(stream, &json!({"status": "ok", "kind": "subscribed"}), &[])?;

        let mut sent_row_count = 0usize;
        while !self.stopped.load(Ordering::SeqCst) {
            let batch = {
                let writers = self.writers.lock().unwrap();
                writers
                    .get(source)
                    .map(|writer| writer.materializer.active_record_batch())
                    .transpose()?
            };
            let batch = match batch {
                Some(batch) => Some(batch),
                None => match self.collect_segment_source(source) {
                    Ok(batch) => Some(batch),
                    Err(_) => None,
                },
            };
            let Some(batch) = batch else {
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            let total_rows = batch.num_rows();
            if total_rows < sent_row_count {
                sent_row_count = 0;
            }
            let next_batch = if let Some(count) = count {
                if total_rows == sent_row_count {
                    None
                } else {
                    let row_count = count.min(total_rows);
                    Some(batch.slice(total_rows - row_count, row_count))
                }
            } else if total_rows > sent_row_count {
                let row_count = batch_size
                    .map(|value| value.min(total_rows - sent_row_count))
                    .unwrap_or(total_rows - sent_row_count);
                Some(batch.slice(sent_row_count, row_count))
            } else {
                None
            };

            let Some(next_batch) = next_batch else {
                thread::sleep(Duration::from_millis(5));
                continue;
            };
            if next_batch.num_rows() > 0 {
                write_frame(
                    stream,
                    &json!({"status": "ok", "kind": "table"}),
                    &encode_ipc_table(&next_batch)?,
                )?;
            }
            sent_row_count = if count.is_some() {
                total_rows
            } else {
                sent_row_count + next_batch.num_rows()
            };
            if let Some(throttle) = throttle {
                thread::sleep(throttle);
            }
        }
        Ok(())
    }

    fn with_master<T>(&self, mut f: impl FnMut(&mut MasterClient) -> Result<T>) -> Result<T> {
        let mut guard = self.master.lock().unwrap();
        let mut last_error = None;
        for _ in 0..2 {
            if guard.is_none() {
                let mut client = MasterClient::connect_endpoint(self.master_endpoint.clone())?;
                client.register_process("zippy_gateway")?;
                *guard = Some(client);
            }
            let result = f(guard.as_mut().expect("master client initialized"));
            match result {
                Ok(value) => return Ok(value),
                Err(error) if gateway_master_process_invalid(&error) => {
                    *guard = None;
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("gateway master process retry must store the last error"))
    }

    fn collect_segment_source(&self, source: &str) -> Result<RecordBatch> {
        let stream = self.with_master(|master| master.get_stream(source))?;
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway collect source is not materialized by this gateway and is not a segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        if stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    source, stream.status
                ),
            });
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Err(ZippyError::Io {
                reason: format!("segment descriptor is not published source=[{}]", source),
            });
        };
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let _leases = self.acquire_segment_reader_leases(source, &descriptor, &stream)?;
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
        let batches = live_segment_record_batches(
            &descriptor,
            &stream.sealed_segments,
            segment_schema,
            active_committed_row_high_watermark,
        )?;
        concat_record_batches(schema, batches)
    }

    fn acquire_segment_reader_leases(
        &self,
        source: &str,
        active_descriptor: &Value,
        stream: &StreamInfo,
    ) -> Result<Vec<SegmentReaderLeaseGuard>> {
        let mut leases = Vec::with_capacity(stream.sealed_segments.len() + 1);
        for descriptor in &stream.sealed_segments {
            leases.push(self.acquire_segment_reader_lease(source, descriptor)?);
        }
        leases.push(self.acquire_segment_reader_lease(source, active_descriptor)?);
        Ok(leases)
    }

    fn acquire_segment_reader_lease(
        &self,
        source: &str,
        descriptor: &Value,
    ) -> Result<SegmentReaderLeaseGuard> {
        let (segment_id, generation) = descriptor_segment_identity(descriptor)?;
        let lease_id = self.with_master(|master| {
            master.acquire_segment_reader_lease(source, segment_id, generation)
        })?;
        Ok(SegmentReaderLeaseGuard {
            master: Arc::clone(&self.master),
            source: source.to_string(),
            lease_id: Some(lease_id),
        })
    }
}

fn gateway_master_process_invalid(error: &ZippyError) -> bool {
    let message = error.to_string();
    message.contains("process lease expired") || message.contains("process not found")
}

impl Drop for SegmentReaderLeaseGuard {
    fn drop(&mut self) {
        let Some(lease_id) = self.lease_id.take() else {
            return;
        };
        let mut guard = self.master.lock().unwrap();
        let Some(master) = guard.as_mut() else {
            return;
        };
        let _ = master.release_segment_reader_lease(&self.source, &lease_id);
    }
}

fn serve_loop(listener: TcpListener, state: Arc<GatewayState>) {
    while !state.stopped.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                let state = Arc::clone(&state);
                thread::spawn(move || handle_client(stream, state));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
}

fn heartbeat_loop(state: Arc<GatewayState>) {
    while !state.stopped.load(Ordering::SeqCst) {
        let mut elapsed = Duration::ZERO;
        while elapsed < Duration::from_secs(1) {
            thread::sleep(Duration::from_millis(100));
            if state.stopped.load(Ordering::SeqCst) {
                return;
            }
            elapsed += Duration::from_millis(100);
        }

        let mut guard = state.master.lock().unwrap();
        let Some(master) = guard.as_mut() else {
            continue;
        };
        if let Err(error) = master.heartbeat() {
            if gateway_master_process_invalid(&error) {
                *guard = None;
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, state: Arc<GatewayState>) {
    let result = match read_frame(&mut stream) {
        Ok((header, _payload))
            if header.get("kind").and_then(Value::as_str) == Some("subscribe_table") =>
        {
            state.increment_metric(|metrics| metrics.requests_total += 1);
            match state
                .authorize(&header)
                .and_then(|_| state.handle_subscribe_table_stream(&mut stream, header))
            {
                Ok(()) => return,
                Err(error) => Err(error),
            }
        }
        Ok((header, payload)) => state.handle_request(header, payload),
        Err(error) => Err(error),
    };
    let (header, payload) = match result {
        Ok(response) => response,
        Err(error) => {
            state.increment_metric(|metrics| metrics.errors_total += 1);
            (
                json!({"status": "error", "reason": error.to_string()}),
                vec![],
            )
        }
    };
    let _ = write_frame(&mut stream, &header, &payload);
}

fn read_frame(stream: &mut TcpStream) -> Result<(Value, Vec<u8>)> {
    let mut prefix = [0u8; 12];
    stream.read_exact(&mut prefix).map_err(io_error)?;
    let header_len = u32::from_be_bytes(prefix[0..4].try_into().unwrap()) as usize;
    let payload_len = u64::from_be_bytes(prefix[4..12].try_into().unwrap()) as usize;
    let mut header = vec![0u8; header_len];
    stream.read_exact(&mut header).map_err(io_error)?;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).map_err(io_error)?;
    }
    let header = serde_json::from_slice::<Value>(&header).map_err(|error| ZippyError::Io {
        reason: format!("failed to decode gateway header error=[{}]", error),
    })?;
    Ok((header, payload))
}

fn write_frame(stream: &mut TcpStream, header: &Value, payload: &[u8]) -> Result<()> {
    let header = serde_json::to_vec(header).map_err(|error| ZippyError::Io {
        reason: format!("failed to encode gateway header error=[{}]", error),
    })?;
    stream
        .write_all(&(header.len() as u32).to_be_bytes())
        .map_err(io_error)?;
    stream
        .write_all(&(payload.len() as u64).to_be_bytes())
        .map_err(io_error)?;
    stream.write_all(&header).map_err(io_error)?;
    stream.write_all(payload).map_err(io_error)?;
    Ok(())
}

fn decode_ipc_batches(payload: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader =
        StreamReader::try_new(Cursor::new(payload), None).map_err(|error| ZippyError::Io {
            reason: format!("failed to decode arrow ipc stream error=[{}]", error),
        })?;
    reader
        .map(|batch| {
            batch.map_err(|error| ZippyError::Io {
                reason: format!("failed to read arrow ipc batch error=[{}]", error),
            })
        })
        .collect()
}

fn encode_ipc_table(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to create arrow ipc writer error=[{}]", error),
            }
        })?;
        writer.write(batch).map_err(|error| ZippyError::Io {
            reason: format!("failed to write arrow ipc batch error=[{}]", error),
        })?;
        writer.finish().map_err(|error| ZippyError::Io {
            reason: format!("failed to finish arrow ipc stream error=[{}]", error),
        })?;
    }
    Ok(payload)
}

fn encode_ipc_schema(schema: &SchemaRef) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut payload, schema).map_err(|error| ZippyError::Io {
                reason: format!("failed to create arrow ipc schema writer error=[{}]", error),
            })?;
        writer.finish().map_err(|error| ZippyError::Io {
            reason: format!("failed to finish arrow ipc schema stream error=[{}]", error),
        })?;
    }
    Ok(payload)
}

fn concat_record_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    concat_batches(&schema, batches.iter()).map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn live_segment_record_batches(
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(sealed_descriptors.len() + 1);
    for sealed_descriptor in sealed_descriptors {
        let batch = descriptor_record_batch_until(sealed_descriptor, segment_schema.clone(), None)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    let active_batch = descriptor_record_batch_until(
        descriptor,
        segment_schema,
        Some(active_committed_row_high_watermark),
    )?;
    if active_batch.num_rows() > 0 {
        batches.push(active_batch);
    }
    Ok(batches)
}

fn descriptor_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    end_row_limit: Option<usize>,
) -> Result<RecordBatch> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let end_row = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let span = RowSpanView::from_active_descriptor(active_descriptor, 0, end_row)
        .map_err(|status| ZippyError::InvalidState { status })?;
    span.as_record_batch().map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn active_descriptor_with_committed_row_count(
    descriptor: &Value,
    segment_schema: CompiledSchema,
) -> Result<(usize, ActiveSegmentDescriptor)> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    let reader = ActiveSegmentReader::from_descriptor_envelope(
        &descriptor_envelope,
        segment_schema.clone(),
        layout.clone(),
    )
    .map_err(segment_zippy_error)?;
    let committed = reader.committed_row_count().map_err(segment_zippy_error)?;
    let active_descriptor =
        ActiveSegmentDescriptor::from_envelope_bytes(&descriptor_envelope, segment_schema, layout)
            .map_err(|error| ZippyError::InvalidConfig {
                reason: error.to_string(),
            })?;
    Ok((committed, active_descriptor))
}

fn active_committed_row_high_watermark(
    descriptor: &Value,
    segment_schema: CompiledSchema,
) -> Result<usize> {
    let row_capacity = descriptor_row_capacity(descriptor)?;
    let layout = LayoutPlan::for_schema(&segment_schema, row_capacity).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: error.to_string(),
        }
    })?;
    let descriptor_envelope = serde_json::to_vec(descriptor).map_err(json_zippy_error)?;
    ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, segment_schema, layout)
        .map_err(segment_zippy_error)?
        .committed_row_count()
        .map_err(segment_zippy_error)
}

fn descriptor_row_capacity(descriptor: &Value) -> Result<usize> {
    let row_capacity = descriptor
        .get("row_capacity")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing row_capacity".to_string(),
        })?;
    usize::try_from(row_capacity).map_err(|_| ZippyError::InvalidConfig {
        reason: "segment descriptor row_capacity overflows usize".to_string(),
    })
}

fn descriptor_segment_identity(descriptor: &Value) -> Result<(u64, u64)> {
    let segment_id = descriptor
        .get("segment_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing segment_id".to_string(),
        })?;
    let generation = descriptor
        .get("generation")
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "segment descriptor missing generation".to_string(),
        })?;
    Ok((segment_id, generation))
}

fn arrow_schema_from_stream_metadata(schema: &Value) -> Result<Schema> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "stream schema metadata missing fields".to_string(),
        })?;
    let fields = fields
        .iter()
        .map(|field| {
            let name = field.get("name").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "stream schema field missing name".to_string(),
                }
            })?;
            let segment_type = field
                .get("segment_type")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing segment_type".to_string(),
                })?;
            let nullable = field
                .get("nullable")
                .and_then(Value::as_bool)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing nullable".to_string(),
                })?;
            let timezone = field.get("timezone").and_then(Value::as_str);
            let data_type = parse_arrow_schema_metadata_data_type(segment_type, timezone)?;
            let metadata = string_map_from_json_value(field.get("metadata"))?;
            Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    let metadata = string_map_from_json_value(schema.get("metadata"))?;
    Ok(Schema::new_with_metadata(fields, metadata))
}

fn compile_segment_schema_from_stream_metadata(schema: &Value) -> Result<CompiledSchema> {
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "stream schema metadata missing fields".to_string(),
        })?;
    let columns = fields
        .iter()
        .map(|field| {
            let name = field.get("name").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "stream schema field missing name".to_string(),
                }
            })?;
            let segment_type = field
                .get("segment_type")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing segment_type".to_string(),
                })?;
            let nullable = field
                .get("nullable")
                .and_then(Value::as_bool)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "stream schema field missing nullable".to_string(),
                })?;
            let timezone = field.get("timezone").and_then(Value::as_str);
            let name: &'static str = Box::leak(name.to_string().into_boxed_str());
            let data_type = parse_segment_schema_metadata_data_type(segment_type, timezone)?;
            Ok(if nullable {
                ColumnSpec::nullable(name, data_type)
            } else {
                ColumnSpec::new(name, data_type)
            })
        })
        .collect::<Result<Vec<_>>>()?;
    compile_segment_schema(&columns).map_err(|error| ZippyError::InvalidConfig {
        reason: error.to_string(),
    })
}

fn parse_arrow_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> Result<DataType> {
    match segment_type {
        "int64" => Ok(DataType::Int64),
        "float64" => Ok(DataType::Float64),
        "utf8" => Ok(DataType::Utf8),
        "timestamp_ns_tz" => {
            let timezone = timezone.ok_or_else(|| ZippyError::InvalidConfig {
                reason: "timestamp_ns_tz stream schema field missing timezone".to_string(),
            })?;
            Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(timezone.into()),
            ))
        }
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported stream field type segment_type=[{}]",
                segment_type
            ),
        }),
    }
}

fn parse_segment_schema_metadata_data_type(
    segment_type: &str,
    timezone: Option<&str>,
) -> Result<ColumnType> {
    match segment_type {
        "int64" => Ok(ColumnType::Int64),
        "float64" => Ok(ColumnType::Float64),
        "utf8" => Ok(ColumnType::Utf8),
        "timestamp_ns_tz" => {
            let timezone = timezone.ok_or_else(|| ZippyError::InvalidConfig {
                reason: "timestamp_ns_tz stream schema field missing timezone".to_string(),
            })?;
            let timezone: &'static str = Box::leak(timezone.to_string().into_boxed_str());
            Ok(ColumnType::TimestampNsTz(timezone))
        }
        "timestamp_ns" => Err(ZippyError::InvalidConfig {
            reason: "segment stream timestamp columns must include an explicit timezone"
                .to_string(),
        }),
        _ => Err(ZippyError::InvalidConfig {
            reason: format!(
                "unsupported segment stream field type segment_type=[{}]",
                segment_type
            ),
        }),
    }
}

fn string_map_from_json_value(value: Option<&Value>) -> Result<HashMap<String, String>> {
    let Some(value) = value else {
        return Ok(HashMap::new());
    };
    let object = value.as_object().ok_or_else(|| ZippyError::InvalidConfig {
        reason: "stream schema metadata must be an object".to_string(),
    })?;
    object
        .iter()
        .map(|(key, value)| {
            let value = value.as_str().ok_or_else(|| ZippyError::InvalidConfig {
                reason: "stream schema metadata values must be strings".to_string(),
            })?;
            Ok((key.clone(), value.to_string()))
        })
        .collect()
}

fn json_zippy_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn segment_zippy_error(error: zippy_segment_store::ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn apply_collect_plan(mut batch: RecordBatch, plan: &[Value]) -> Result<RecordBatch> {
    for op in plan {
        if op.get("op").and_then(Value::as_str) == Some("select") {
            batch = apply_select(batch, op)?;
        }
    }
    Ok(batch)
}

fn apply_select(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let exprs =
        op.get("exprs")
            .and_then(Value::as_array)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "select plan requires exprs".to_string(),
            })?;
    let mut fields = Vec::with_capacity(exprs.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let column_name = expr
            .get("value")
            .and_then(Value::as_str)
            .filter(|_| expr.get("kind").and_then(Value::as_str) == Some("col"))
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "native gateway select currently supports column expressions".to_string(),
            })?;
        let index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "select column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(batch.schema().field(index).clone());
        columns.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway select batch error=[{}]", error),
    })
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("tcp://")
        .unwrap_or(endpoint)
        .split_once('/')
        .map(|(endpoint, _)| endpoint)
        .unwrap_or_else(|| endpoint.strip_prefix("tcp://").unwrap_or(endpoint))
        .to_string()
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

pub fn crate_name() -> &'static str {
    "zippy-gateway"
}
