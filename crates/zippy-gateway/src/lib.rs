use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::array::ArrayRef;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use zippy_core::{ControlEndpoint, Engine, MasterClient, Result, SegmentTableView, ZippyError};
use zippy_engines::{StreamTableDescriptorPublisher, StreamTableMaterializer};

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
            let writer = writers
                .get(source)
                .ok_or_else(|| ZippyError::InvalidState {
                    status: "gateway collect source is not materialized by this gateway",
                })?;
            writer.materializer.active_record_batch()?
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
                .unwrap_or_default()
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

    fn with_master<T>(&self, f: impl FnOnce(&mut MasterClient) -> Result<T>) -> Result<T> {
        let mut guard = self.master.lock().unwrap();
        if guard.is_none() {
            let mut client = MasterClient::connect_endpoint(self.master_endpoint.clone())?;
            client.register_process("zippy_gateway")?;
            *guard = Some(client);
        }
        f(guard.as_mut().expect("master client initialized"))
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
