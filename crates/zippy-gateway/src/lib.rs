use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    and, concat_batches, filter_record_batch, lexsort_to_indices, or, take, SortColumn, SortOptions,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector},
    ProjectionMask,
};
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

#[derive(Default)]
struct GatewayScanPushdown {
    filters: Vec<Value>,
    projection_columns: Option<Vec<String>>,
}

struct GatewayScannedBatch {
    batch: RecordBatch,
    scanned_rows: usize,
    scanned_live_rows: usize,
    scanned_files: Vec<String>,
}

#[derive(Clone, Copy)]
enum GatewayRowRangePushdown {
    Tail(usize),
    Head(usize),
    Slice {
        offset: usize,
        length: Option<usize>,
    },
}

impl GatewayRowRangePushdown {
    fn op_name(self) -> &'static str {
        match self {
            Self::Tail(_) => "tail",
            Self::Head(_) => "head",
            Self::Slice { .. } => "slice",
        }
    }

    fn to_plan_op(self) -> Value {
        match self {
            Self::Tail(n) => json!({"op": "tail", "n": n}),
            Self::Head(n) => json!({"op": "head", "n": n}),
            Self::Slice { offset, length } => {
                json!({"op": "slice", "offset": offset, "length": length})
            }
        }
    }
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
        let started = Instant::now();
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
        let row_range_pushdown = collect_plan_row_range_prefix(&plan)?;
        let row_range_residual_start = row_range_pushdown.map_or(0, |(_, start)| start);
        let pushed_filter_count =
            collect_plan_leading_filter_count(&plan[row_range_residual_start..]);
        let scan_residual_start = row_range_residual_start + pushed_filter_count;
        let requested_scan_pushdown = GatewayScanPushdown {
            filters: plan[row_range_residual_start..scan_residual_start].to_vec(),
            projection_columns: collect_plan_scan_projection_columns(
                &plan[row_range_residual_start..],
            )?,
        };
        let batch = {
            let writers = self.writers.lock().unwrap();
            writers
                .get(source)
                .map(|writer| writer.materializer.active_record_batch())
                .transpose()?
        };
        let mut row_range_pushed = false;
        let mut scan_pushdown_applied = false;
        let scanned = match batch {
            Some(batch) => GatewayScannedBatch {
                scanned_rows: batch.num_rows(),
                scanned_live_rows: batch.num_rows(),
                scanned_files: Vec::new(),
                batch,
            },
            None => match row_range_pushdown {
                Some((pushdown, _)) => {
                    row_range_pushed = true;
                    scan_pushdown_applied = true;
                    self.collect_stream_source_row_range(
                        source,
                        pushdown,
                        &requested_scan_pushdown,
                    )?
                }
                None => {
                    scan_pushdown_applied = true;
                    self.collect_stream_source(source, &requested_scan_pushdown)?
                }
            },
        };
        let residual_plan = if scan_pushdown_applied {
            &plan[scan_residual_start..]
        } else if let Some((_, residual_start)) = row_range_pushdown {
            if row_range_pushed {
                &plan[residual_start..]
            } else {
                plan.as_slice()
            }
        } else {
            plan.as_slice()
        };
        let batch = apply_collect_plan(scanned.batch, residual_plan)?;
        let returned_rows = batch.num_rows();
        let tail_pushdown = row_range_pushed
            && matches!(
                row_range_pushdown,
                Some((GatewayRowRangePushdown::Tail(_), _))
            );
        let row_range_pushdown_metric = if row_range_pushed {
            row_range_pushdown.map(|(pushdown, _)| pushdown.op_name())
        } else {
            None
        };
        let residual_filters = collect_plan_filter_ops(residual_plan);
        let projection_columns = collect_plan_projection_columns(residual_plan);
        let pushed_filters = if scan_pushdown_applied {
            requested_scan_pushdown.filters.clone()
        } else {
            Vec::new()
        };
        let scan_projection_columns = if scan_pushdown_applied {
            requested_scan_pushdown.projection_columns.clone()
        } else {
            None
        };
        Ok((
            json!({
                "status": "ok",
                "metrics": {
                    "scanned_files": scanned.scanned_files,
                    "scanned_rows": scanned.scanned_rows,
                    "scanned_live_rows": scanned.scanned_live_rows,
                    "returned_rows": returned_rows,
                    "plan_ops": plan.len(),
                    "tail_pushdown": tail_pushdown,
                    "row_range_pushdown": row_range_pushdown_metric,
                    "pushed_filters": pushed_filters,
                    "residual_filters": residual_filters,
                    "projection_columns": projection_columns,
                    "scan_projection_columns": scan_projection_columns,
                    "residual_plan_ops": residual_plan.len(),
                    "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
                }
            }),
            encode_ipc_table(&batch)?,
        ))
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
                None => match self.collect_stream_source(source, &GatewayScanPushdown::default()) {
                    Ok(scanned) => Some(scanned.batch),
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

    fn collect_stream_source(
        &self,
        source: &str,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let stream = self.with_master(|master| master.get_stream(source))?;
        if stream.data_path != "segment" {
            return Err(ZippyError::Io {
                reason: format!(
                    "gateway collect source is not materialized by this gateway and is not a segment stream data_path=[{}]",
                    stream.data_path
                ),
            });
        }
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut batches = persisted_file_record_batches(
            &stream,
            scan_pushdown,
            &mut scanned_rows,
            &mut scanned_files,
        )?;
        if let Some(mut live_batches) = self.try_collect_live_segment_batches(
            source,
            &stream,
            scan_pushdown,
            &mut scanned_live_rows,
        )? {
            scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
            batches.append(&mut live_batches);
        } else if batches.is_empty() && stream.status == "stale" {
            return Err(ZippyError::Io {
                reason: format!(
                    "stream is stale source=[{}] status=[{}]",
                    source, stream.status
                ),
            });
        } else if batches.is_empty() && stream.active_segment_descriptor.is_none() {
            return Err(ZippyError::Io {
                reason: format!("segment descriptor is not published source=[{}]", source),
            });
        }
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(
                schema_for_scan_pushdown(&schema, scan_pushdown)?,
                batches,
            )?,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn try_collect_live_segment_batches(
        &self,
        source: &str,
        stream: &StreamInfo,
        scan_pushdown: &GatewayScanPushdown,
        scanned_live_rows: &mut usize,
    ) -> Result<Option<Vec<RecordBatch>>> {
        if stream.status == "stale" {
            return Ok(None);
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Ok(None);
        };
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let _leases = self.acquire_segment_reader_leases(source, &descriptor, stream)?;
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
        let mut scanned_rows = 0usize;
        let batches = live_segment_record_batches(
            &descriptor,
            &stream.sealed_segments,
            segment_schema,
            active_committed_row_high_watermark,
            scan_pushdown,
            &mut scanned_rows,
        )?;
        *scanned_live_rows = scanned_rows;
        Ok(Some(batches))
    }

    fn collect_stream_source_row_range(
        &self,
        source: &str,
        pushdown: GatewayRowRangePushdown,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let stream = self.with_master(|master| master.get_stream(source))?;
        if stream_has_persisted_files(&stream) {
            if let GatewayRowRangePushdown::Tail(n) = pushdown {
                return self.collect_stream_source_tail(source, &stream, n, scan_pushdown);
            }
            let mut scanned = self.collect_stream_source(source, scan_pushdown)?;
            let row_range_op = pushdown.to_plan_op();
            scanned.batch = apply_collect_plan(scanned.batch, &[row_range_op])?;
            return Ok(scanned);
        }
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
        let mut _leases = Vec::new();
        let mut scanned_rows = 0usize;
        let batches = match pushdown {
            GatewayRowRangePushdown::Tail(n) => {
                _leases.push(self.acquire_segment_reader_lease(source, &descriptor)?);
                let active_committed_row_high_watermark =
                    active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
                tail_live_segment_record_batches_with_leases(
                    self,
                    source,
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    active_committed_row_high_watermark,
                    n,
                    &mut _leases,
                    scan_pushdown,
                    &mut scanned_rows,
                )?
            }
            GatewayRowRangePushdown::Head(n) => head_live_segment_record_batches_with_leases(
                self,
                source,
                &descriptor,
                &stream.sealed_segments,
                segment_schema,
                n,
                &mut _leases,
                scan_pushdown,
                &mut scanned_rows,
            )?,
            GatewayRowRangePushdown::Slice { offset, length } => {
                slice_live_segment_record_batches_with_leases(
                    self,
                    source,
                    &descriptor,
                    &stream.sealed_segments,
                    segment_schema,
                    offset,
                    length,
                    &mut _leases,
                    scan_pushdown,
                    &mut scanned_rows,
                )?
            }
        };
        Ok(GatewayScannedBatch {
            batch: concat_record_batches(schema, batches)?,
            scanned_rows,
            scanned_live_rows: scanned_rows,
            scanned_files: Vec::new(),
        })
    }

    fn collect_stream_source_tail(
        &self,
        source: &str,
        stream: &StreamInfo,
        n: usize,
        scan_pushdown: &GatewayScanPushdown,
    ) -> Result<GatewayScannedBatch> {
        let schema = Arc::new(arrow_schema_from_stream_metadata(&stream.schema)?);
        let mut scanned_rows = 0usize;
        let mut scanned_live_rows = 0usize;
        let mut scanned_files = Vec::new();
        let mut live_batches = self
            .try_collect_live_segment_tail_batches(
                source,
                stream,
                n,
                scan_pushdown,
                &mut scanned_live_rows,
            )?
            .unwrap_or_default();
        let live_rows = scanned_live_rows;
        let persisted_remaining = n.saturating_sub(live_rows);
        let mut batches = if persisted_remaining > 0 {
            tail_persisted_file_record_batches(
                stream,
                persisted_remaining,
                scan_pushdown,
                &mut scanned_rows,
                &mut scanned_files,
            )?
        } else {
            Vec::new()
        };

        scanned_rows = scanned_rows.saturating_add(scanned_live_rows);
        batches.append(&mut live_batches);
        let mut batch =
            concat_record_batches(schema_for_scan_pushdown(&schema, scan_pushdown)?, batches)?;
        if batch.num_rows() > n {
            batch = apply_collect_plan(batch, &[GatewayRowRangePushdown::Tail(n).to_plan_op()])?;
        }
        Ok(GatewayScannedBatch {
            batch,
            scanned_rows,
            scanned_live_rows,
            scanned_files,
        })
    }

    fn try_collect_live_segment_tail_batches(
        &self,
        source: &str,
        stream: &StreamInfo,
        n: usize,
        scan_pushdown: &GatewayScanPushdown,
        scanned_live_rows: &mut usize,
    ) -> Result<Option<Vec<RecordBatch>>> {
        if stream.status == "stale" {
            return Ok(None);
        }
        let Some(descriptor) = stream.active_segment_descriptor.clone() else {
            return Ok(None);
        };
        let segment_schema = compile_segment_schema_from_stream_metadata(&stream.schema)?;
        let mut leases = Vec::new();
        leases.push(self.acquire_segment_reader_lease(source, &descriptor)?);
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(&descriptor, segment_schema.clone())?;
        let mut scanned_rows = 0usize;
        let batches = tail_live_segment_record_batches_with_leases(
            self,
            source,
            &descriptor,
            &stream.sealed_segments,
            segment_schema,
            active_committed_row_high_watermark,
            n,
            &mut leases,
            scan_pushdown,
            &mut scanned_rows,
        )?;
        *scanned_live_rows = scanned_rows;
        Ok(Some(batches))
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

fn persisted_file_record_batches(
    stream: &StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for persisted_file in non_overlapping_persisted_files(stream) {
        let file_path = persisted_file_path(persisted_file)?;
        let batch = read_parquet_record_batch(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
        )?;
        scanned_files.push(file_path);
        let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
}

fn tail_persisted_file_record_batches(
    stream: &StreamInfo,
    n: usize,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
    scanned_files: &mut Vec<String>,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches_reversed = Vec::new();
    for persisted_file in non_overlapping_persisted_files(stream).into_iter().rev() {
        if remaining == 0 {
            break;
        }
        let file_path = persisted_file_path(persisted_file)?;
        let batch = read_parquet_record_batch_tail(
            Path::new(&file_path),
            scan_pushdown.projection_columns.as_deref(),
            remaining,
        )?;
        let raw_rows = batch.num_rows();
        scanned_files.push(file_path);
        let batch = apply_scan_pushdown_to_record_batch(batch, scan_pushdown, scanned_rows)?;
        remaining = remaining.saturating_sub(raw_rows);
        if batch.num_rows() > 0 {
            batches_reversed.push(batch);
        }
    }
    scanned_files.reverse();
    batches_reversed.reverse();
    Ok(batches_reversed)
}

fn read_parquet_record_batch(
    path: &Path,
    projection_columns: Option<&[String]>,
) -> Result<RecordBatch> {
    read_parquet_record_batch_with_tail(path, projection_columns, None)
}

fn read_parquet_record_batch_tail(
    path: &Path,
    projection_columns: Option<&[String]>,
    n: usize,
) -> Result<RecordBatch> {
    read_parquet_record_batch_with_tail(path, projection_columns, Some(n))
}

fn read_parquet_record_batch_with_tail(
    path: &Path,
    projection_columns: Option<&[String]>,
    tail_rows: Option<usize>,
) -> Result<RecordBatch> {
    let file = File::open(path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to open persisted parquet file path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    let mut builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create persisted parquet reader path=[{}] error=[{}]",
                path.display(),
                error
            ),
        })?;
    if let Some(projection_columns) = projection_columns {
        let indices = projection_columns
            .iter()
            .map(|column_name| {
                builder
                    .schema()
                    .index_of(column_name)
                    .map_err(|error| ZippyError::SchemaMismatch {
                        reason: format!(
                            "persisted parquet projection column not found path=[{}] column=[{}] error=[{}]",
                            path.display(),
                            column_name,
                            error
                        ),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(projection);
    }
    if let Some(tail_rows) = tail_rows {
        let total_rows =
            usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                ZippyError::InvalidConfig {
                    reason: format!(
                        "persisted parquet row count overflows usize path=[{}]",
                        path.display()
                    ),
                }
            })?;
        let selected_rows = tail_rows.min(total_rows);
        let skipped_rows = total_rows.saturating_sub(selected_rows);
        let mut selectors = Vec::new();
        if skipped_rows > 0 {
            selectors.push(RowSelector::skip(skipped_rows));
        }
        if selected_rows > 0 {
            selectors.push(RowSelector::select(selected_rows));
        }
        builder = builder
            .with_batch_size(selected_rows.max(1))
            .with_row_selection(RowSelection::from(selectors));
    }
    let schema = builder.schema().clone();
    let reader = builder.build().map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build persisted parquet reader path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    let batches = reader
        .map(|batch| {
            batch.map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to read persisted parquet batch path=[{}] error=[{}]",
                    path.display(),
                    error
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    concat_record_batches(schema, batches)
}

fn apply_scan_pushdown_to_record_batch(
    batch: RecordBatch,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let mut batch = project_record_batch(&batch, scan_pushdown.projection_columns.as_deref())?;
    *scanned_rows = scanned_rows.saturating_add(batch.num_rows());
    for filter in &scan_pushdown.filters {
        batch = apply_filter(batch, filter)?;
    }
    Ok(batch)
}

fn project_record_batch(
    batch: &RecordBatch,
    projection_columns: Option<&[String]>,
) -> Result<RecordBatch> {
    let Some(projection_columns) = projection_columns else {
        return Ok(batch.clone());
    };
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut arrays = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "scan projection column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(batch.schema().field(index).clone());
        arrays.push(batch.column(index).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build gateway scan projection batch error=[{}]",
            error
        ),
    })
}

fn schema_for_scan_pushdown(
    schema: &SchemaRef,
    scan_pushdown: &GatewayScanPushdown,
) -> Result<SchemaRef> {
    let Some(projection_columns) = scan_pushdown.projection_columns.as_deref() else {
        return Ok(Arc::clone(schema));
    };
    let mut fields = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index = schema
            .index_of(column_name)
            .map_err(|error| ZippyError::SchemaMismatch {
                reason: format!(
                    "scan projection column not found column=[{}] error=[{}]",
                    column_name, error
                ),
            })?;
        fields.push(schema.field(index).clone());
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn stream_has_persisted_files(stream: &StreamInfo) -> bool {
    stream
        .persisted_files
        .iter()
        .any(|item| persisted_file_path(item).is_ok())
}

fn non_overlapping_persisted_files(stream: &StreamInfo) -> Vec<&Value> {
    let live_identities = live_segment_identities(stream);
    let mut files = stream
        .persisted_files
        .iter()
        .filter(|item| persisted_file_path(item).is_ok())
        .filter(|item| persisted_segment_identities(item).is_disjoint(&live_identities))
        .collect::<Vec<_>>();
    files.sort_by_key(|item| persisted_file_order_key(item));
    files
}

fn persisted_file_path(value: &Value) -> Result<String> {
    value
        .get("file_path")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "persisted file metadata missing file_path".to_string(),
        })
}

fn live_segment_identities(stream: &StreamInfo) -> std::collections::BTreeSet<(u64, u64)> {
    let mut identities = std::collections::BTreeSet::new();
    if let Some(descriptor) = stream.active_segment_descriptor.as_ref() {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.insert(identity);
        }
    }
    for descriptor in &stream.sealed_segments {
        if let Some(identity) = segment_identity_from_value(descriptor, "segment_id", "generation")
        {
            identities.insert(identity);
        }
    }
    identities
}

fn persisted_segment_identities(value: &Value) -> std::collections::BTreeSet<(u64, u64)> {
    let mut identities = std::collections::BTreeSet::new();
    if let Some(source_segments) = value.get("source_segments").and_then(Value::as_array) {
        for source_segment in source_segments {
            if let Some(identity) = segment_identity_from_value(
                source_segment,
                "source_segment_id",
                "source_generation",
            ) {
                identities.insert(identity);
            }
        }
    }
    if identities.is_empty() {
        if let Some(identity) =
            segment_identity_from_value(value, "source_segment_id", "source_generation")
        {
            identities.insert(identity);
        }
    }
    identities
}

fn segment_identity_from_value(
    value: &Value,
    segment_id_key: &str,
    generation_key: &str,
) -> Option<(u64, u64)> {
    Some((
        value.get(segment_id_key)?.as_u64()?,
        value.get(generation_key)?.as_u64()?,
    ))
}

fn persisted_file_order_key(value: &Value) -> (u64, u64, u64, String) {
    (
        json_u64_order_value(value.get("source_segment_id")),
        json_u64_order_value(value.get("source_generation")),
        json_u64_order_value(value.get("created_at")),
        value
            .get("file_path")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    )
}

fn json_u64_order_value(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(u64::MAX)
}

fn live_segment_record_batches(
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::with_capacity(sealed_descriptors.len() + 1);
    for sealed_descriptor in sealed_descriptors {
        let batch = descriptor_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    let active_batch = descriptor_record_batch_until(
        descriptor,
        segment_schema,
        Some(active_committed_row_high_watermark),
        scan_pushdown,
        scanned_rows,
    )?;
    if active_batch.num_rows() > 0 {
        batches.push(active_batch);
    }
    Ok(batches)
}

fn tail_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    active_committed_row_high_watermark: usize,
    n: usize,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches_reversed = Vec::new();

    let active_batch = descriptor_tail_record_batch_until(
        descriptor,
        segment_schema.clone(),
        n,
        Some(active_committed_row_high_watermark),
        scan_pushdown,
        scanned_rows,
    )?;
    remaining = remaining.saturating_sub(active_batch.num_rows());
    if active_batch.num_rows() > 0 {
        batches_reversed.push(active_batch);
    }

    for sealed_descriptor in sealed_descriptors.iter().rev() {
        if remaining == 0 {
            break;
        }
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        let batch = descriptor_tail_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            remaining,
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        remaining = remaining.saturating_sub(batch.num_rows());
        if batch.num_rows() > 0 {
            batches_reversed.push(batch);
        }
    }

    batches_reversed.reverse();
    Ok(batches_reversed)
}

fn head_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    n: usize,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining = n;
    let mut batches = Vec::new();

    for sealed_descriptor in sealed_descriptors {
        if remaining == 0 {
            break;
        }
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        let (batch, _) = descriptor_slice_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            0,
            Some(remaining),
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        remaining = remaining.saturating_sub(batch.num_rows());
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if remaining > 0 {
        leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(descriptor, segment_schema.clone())?;
        let (active_batch, _) = descriptor_slice_record_batch_until(
            descriptor,
            segment_schema,
            0,
            Some(remaining),
            Some(active_committed_row_high_watermark),
            scan_pushdown,
            scanned_rows,
        )?;
        if active_batch.num_rows() > 0 {
            batches.push(active_batch);
        }
    }

    Ok(batches)
}

fn slice_live_segment_record_batches_with_leases(
    state: &GatewayState,
    source: &str,
    descriptor: &Value,
    sealed_descriptors: &[Value],
    segment_schema: CompiledSchema,
    offset: usize,
    length: Option<usize>,
    leases: &mut Vec<SegmentReaderLeaseGuard>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<Vec<RecordBatch>> {
    let mut absolute_position = 0usize;
    let mut collected_rows = 0usize;
    let mut batches = Vec::new();

    for sealed_descriptor in sealed_descriptors {
        if length.is_some_and(|value| collected_rows >= value) {
            break;
        }
        leases.push(state.acquire_segment_reader_lease(source, sealed_descriptor)?);
        let remaining_length = length.map(|value| value.saturating_sub(collected_rows));
        let start_row = offset.saturating_sub(absolute_position);
        let (batch, committed_rows) = descriptor_slice_record_batch_until(
            sealed_descriptor,
            segment_schema.clone(),
            start_row,
            remaining_length,
            None,
            scan_pushdown,
            scanned_rows,
        )?;
        absolute_position = absolute_position.saturating_add(committed_rows);
        collected_rows = collected_rows.saturating_add(batch.num_rows());
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    if !length.is_some_and(|value| collected_rows >= value) {
        leases.push(state.acquire_segment_reader_lease(source, descriptor)?);
        let active_committed_row_high_watermark =
            active_committed_row_high_watermark(descriptor, segment_schema.clone())?;
        let remaining_length = length.map(|value| value.saturating_sub(collected_rows));
        let start_row = offset.saturating_sub(absolute_position);
        let (active_batch, _) = descriptor_slice_record_batch_until(
            descriptor,
            segment_schema,
            start_row,
            remaining_length,
            Some(active_committed_row_high_watermark),
            scan_pushdown,
            scanned_rows,
        )?;
        if active_batch.num_rows() > 0 {
            batches.push(active_batch);
        }
    }

    Ok(batches)
}

fn descriptor_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let (batch, _) = descriptor_slice_record_batch_until(
        descriptor,
        segment_schema,
        0,
        None,
        end_row_limit,
        scan_pushdown,
        scanned_rows,
    )?;
    Ok(batch)
}

fn descriptor_tail_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    n: usize,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let committed = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let start_row = committed.saturating_sub(n);
    let span = RowSpanView::from_active_descriptor(active_descriptor, start_row, committed)
        .map_err(|status| ZippyError::InvalidState { status })?;
    row_span_record_batch(span, scan_pushdown, scanned_rows)
}

fn descriptor_slice_record_batch_until(
    descriptor: &Value,
    segment_schema: CompiledSchema,
    start_row: usize,
    length: Option<usize>,
    end_row_limit: Option<usize>,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<(RecordBatch, usize)> {
    let (committed, active_descriptor) =
        active_descriptor_with_committed_row_count(descriptor, segment_schema)?;
    let committed = end_row_limit.map_or(committed, |limit| limit.min(committed));
    let start_row = start_row.min(committed);
    let available = committed.saturating_sub(start_row);
    let row_count = length.map_or(available, |value| value.min(available));
    let end_row = start_row + row_count;
    let span = RowSpanView::from_active_descriptor(active_descriptor, start_row, end_row)
        .map_err(|status| ZippyError::InvalidState { status })?;
    let batch = row_span_record_batch(span, scan_pushdown, scanned_rows)?;
    Ok((batch, committed))
}

fn row_span_record_batch(
    span: RowSpanView,
    scan_pushdown: &GatewayScanPushdown,
    scanned_rows: &mut usize,
) -> Result<RecordBatch> {
    let mut batch =
        project_row_span_record_batch(&span, scan_pushdown.projection_columns.as_deref())?;
    *scanned_rows = scanned_rows.saturating_add(batch.num_rows());
    for filter in &scan_pushdown.filters {
        batch = apply_filter(batch, filter)?;
    }
    Ok(batch)
}

fn project_row_span_record_batch(
    span: &RowSpanView,
    projection_columns: Option<&[String]>,
) -> Result<RecordBatch> {
    let Some(projection_columns) = projection_columns else {
        return span.as_record_batch().map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        });
    };
    let source_schema = span.schema_ref();
    let mut fields = Vec::with_capacity(projection_columns.len());
    let mut arrays = Vec::with_capacity(projection_columns.len());
    for column_name in projection_columns {
        let index =
            source_schema
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "scan projection column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        fields.push(source_schema.field(index).clone());
        arrays.push(span.column(column_name).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to build gateway scan projection batch error=[{}]",
            error
        ),
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
        let op_name =
            op.get("op")
                .and_then(Value::as_str)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "collect plan requires op".to_string(),
                })?;
        match op_name {
            "select" => batch = apply_select(batch, op)?,
            "filter" => batch = apply_filter(batch, op)?,
            "head" => batch = apply_head(batch, op)?,
            "tail" => batch = apply_tail(batch, op)?,
            "slice" => batch = apply_slice(batch, op)?,
            "sort" => batch = apply_sort(batch, op)?,
            "drop" => batch = apply_drop(batch, op)?,
            "rename" => batch = apply_rename(batch, op)?,
            other => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("unsupported native gateway collect plan op=[{}]", other),
                });
            }
        }
    }
    Ok(batch)
}

fn collect_plan_row_range_prefix(
    plan: &[Value],
) -> Result<Option<(GatewayRowRangePushdown, usize)>> {
    if plan.is_empty() {
        return Ok(None);
    }
    let Some(op) = plan[0].get("op").and_then(Value::as_str) else {
        return Ok(None);
    };
    let pushdown = match op {
        "tail" => collect_plan_usize(&plan[0], "n")
            .map(GatewayRowRangePushdown::Tail)
            .map(Some)?,
        "head" => collect_plan_usize(&plan[0], "n")
            .map(GatewayRowRangePushdown::Head)
            .map(Some)?,
        "slice" => {
            let offset = collect_plan_usize(&plan[0], "offset")?;
            let length = match plan[0].get("length") {
                Some(Value::Null) | None => None,
                Some(_) => Some(collect_plan_usize(&plan[0], "length")?),
            };
            Some(GatewayRowRangePushdown::Slice { offset, length })
        }
        _ => None,
    };
    Ok(pushdown.map(|value| (value, 1)))
}

fn collect_plan_filter_ops(plan: &[Value]) -> Vec<Value> {
    plan.iter()
        .filter(|op| op.get("op").and_then(Value::as_str) == Some("filter"))
        .cloned()
        .collect()
}

fn collect_plan_leading_filter_count(plan: &[Value]) -> usize {
    plan.iter()
        .take_while(|op| op.get("op").and_then(Value::as_str) == Some("filter"))
        .count()
}

fn collect_plan_projection_columns(plan: &[Value]) -> Option<Vec<String>> {
    let select = plan
        .iter()
        .rev()
        .find(|op| op.get("op").and_then(Value::as_str) == Some("select"))?;
    let exprs = select.get("exprs").and_then(Value::as_array)?;
    let columns = exprs
        .iter()
        .filter_map(|expr| {
            if expr.get("kind").and_then(Value::as_str) != Some("col") {
                return None;
            }
            expr.get("value")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    Some(columns)
}

fn collect_plan_scan_projection_columns(plan: &[Value]) -> Result<Option<Vec<String>>> {
    if !plan
        .iter()
        .any(|op| op.get("op").and_then(Value::as_str) == Some("select"))
    {
        return Ok(None);
    }
    let mut columns = Vec::new();
    for op in plan {
        match op.get("op").and_then(Value::as_str) {
            Some("filter") => {
                if let Some(expr) = op.get("expr") {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            Some("select") => {
                let exprs = op.get("exprs").and_then(Value::as_array).ok_or_else(|| {
                    ZippyError::InvalidConfig {
                        reason: "select plan requires exprs".to_string(),
                    }
                })?;
                for expr in exprs {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            Some("sort") => {
                let exprs = op.get("by").and_then(Value::as_array).ok_or_else(|| {
                    ZippyError::InvalidConfig {
                        reason: "sort plan requires by".to_string(),
                    }
                })?;
                for expr in exprs {
                    collect_query_expr_columns(expr, &mut columns)?;
                }
            }
            _ => {}
        }
    }
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(columns))
}

fn collect_query_expr_columns(expr: &Value, columns: &mut Vec<String>) -> Result<()> {
    match query_expr_kind(expr) {
        Some("col") => {
            let column = expr.get("value").and_then(Value::as_str).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "column expression requires string value".to_string(),
                }
            })?;
            if !columns.iter().any(|existing| existing == column) {
                columns.push(column.to_string());
            }
        }
        Some("binary") => {
            let args = expr.get("args").and_then(Value::as_array).ok_or_else(|| {
                ZippyError::InvalidConfig {
                    reason: "binary expression requires args".to_string(),
                }
            })?;
            for arg in args {
                collect_query_expr_columns(arg, columns)?;
            }
        }
        Some("literal") => {}
        Some(_) | None => {}
    }
    Ok(())
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

fn apply_filter(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let expr = op.get("expr").ok_or_else(|| ZippyError::InvalidConfig {
        reason: "filter plan requires expr".to_string(),
    })?;
    let mask = evaluate_filter_expr(&batch, expr)?;
    filter_record_batch(&batch, &mask).map_err(|error| ZippyError::Io {
        reason: format!("failed to filter gateway batch error=[{}]", error),
    })
}

fn evaluate_filter_expr(batch: &RecordBatch, expr: &Value) -> Result<BooleanArray> {
    let kind =
        expr.get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "filter expression requires kind".to_string(),
            })?;
    if kind != "binary" {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "native gateway filter currently supports binary expressions kind=[{}]",
                kind
            ),
        });
    }
    let op = expr
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "filter binary expression requires op".to_string(),
        })?;
    let args = expr
        .get("args")
        .and_then(Value::as_array)
        .filter(|items| items.len() == 2)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "filter binary expression requires two args".to_string(),
        })?;
    if op == "and" || op == "or" {
        let left = evaluate_filter_expr(batch, &args[0])?;
        let right = evaluate_filter_expr(batch, &args[1])?;
        let mask = if op == "and" {
            and(&left, &right)
        } else {
            or(&left, &right)
        }
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to combine gateway filter masks error=[{}]", error),
        })?;
        return Ok(mask);
    }
    compare_filter_expr(batch, op, &args[0], &args[1])
}

fn compare_filter_expr(
    batch: &RecordBatch,
    op: &str,
    left: &Value,
    right: &Value,
) -> Result<BooleanArray> {
    if let Some((column_name, literal, reverse)) = column_literal_filter_pair(left, right)? {
        let column_index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "filter column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        let column = batch.column(column_index);
        let literal = literal_array_for_type(column.data_type(), literal, batch.num_rows())?;
        let op = if reverse {
            reverse_filter_op(op)?
        } else {
            op.to_string()
        };
        let result = match op.as_str() {
            "eq" => eq(column, &literal),
            "ne" => neq(column, &literal),
            "gt" => gt(column, &literal),
            "ge" => gt_eq(column, &literal),
            "lt" => lt(column, &literal),
            "le" => lt_eq(column, &literal),
            other => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("unsupported gateway filter op=[{}]", other),
                });
            }
        };
        return result.map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to compare gateway filter expression error=[{}]",
                error
            ),
        });
    }
    Err(ZippyError::InvalidConfig {
        reason: "native gateway filter requires column-literal comparison".to_string(),
    })
}

fn column_literal_filter_pair<'a>(
    left: &'a Value,
    right: &'a Value,
) -> Result<Option<(&'a str, &'a Value, bool)>> {
    if let Some(column_name) = query_expr_column_name(left)? {
        if query_expr_kind(right) == Some("literal") {
            return Ok(Some((
                column_name,
                right.get("value").unwrap_or(&Value::Null),
                false,
            )));
        }
    }
    if let Some(column_name) = query_expr_column_name(right)? {
        if query_expr_kind(left) == Some("literal") {
            return Ok(Some((
                column_name,
                left.get("value").unwrap_or(&Value::Null),
                true,
            )));
        }
    }
    Ok(None)
}

fn query_expr_kind(expr: &Value) -> Option<&str> {
    expr.get("kind").and_then(Value::as_str)
}

fn query_expr_column_name(expr: &Value) -> Result<Option<&str>> {
    if query_expr_kind(expr) != Some("col") {
        return Ok(None);
    }
    expr.get("value")
        .and_then(Value::as_str)
        .map(Some)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "column expression requires string value".to_string(),
        })
}

fn reverse_filter_op(op: &str) -> Result<String> {
    let reversed = match op {
        "eq" => "eq",
        "ne" => "ne",
        "gt" => "lt",
        "ge" => "le",
        "lt" => "gt",
        "le" => "ge",
        other => {
            return Err(ZippyError::InvalidConfig {
                reason: format!("unsupported gateway filter op=[{}]", other),
            });
        }
    };
    Ok(reversed.to_string())
}

fn literal_array_for_type(data_type: &DataType, value: &Value, len: usize) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
            let value = value
                .as_str()
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "string filter literal must be a string".to_string(),
                })?
                .to_string();
            Ok(Arc::new(StringArray::from(vec![value; len])))
        }
        DataType::Float64 => {
            let value = value.as_f64().ok_or_else(|| ZippyError::InvalidConfig {
                reason: "float filter literal must be numeric".to_string(),
            })?;
            Ok(Arc::new(Float64Array::from(vec![value; len])))
        }
        DataType::Int64 => {
            let value = value.as_i64().ok_or_else(|| ZippyError::InvalidConfig {
                reason: "int filter literal must be integer".to_string(),
            })?;
            Ok(Arc::new(Int64Array::from(vec![value; len])))
        }
        DataType::Boolean => {
            let value = value.as_bool().ok_or_else(|| ZippyError::InvalidConfig {
                reason: "boolean filter literal must be boolean".to_string(),
            })?;
            Ok(Arc::new(BooleanArray::from(vec![value; len])))
        }
        other => Err(ZippyError::InvalidConfig {
            reason: format!("unsupported gateway filter literal type=[{:?}]", other),
        }),
    }
}

fn apply_head(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let n = collect_plan_usize(op, "n")?;
    Ok(batch.slice(0, n.min(batch.num_rows())))
}

fn apply_tail(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let n = collect_plan_usize(op, "n")?;
    let length = n.min(batch.num_rows());
    Ok(batch.slice(batch.num_rows().saturating_sub(length), length))
}

fn apply_slice(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let offset = collect_plan_usize(op, "offset")?;
    let offset = offset.min(batch.num_rows());
    let available = batch.num_rows().saturating_sub(offset);
    let length = match op.get("length") {
        Some(Value::Null) | None => available,
        Some(_) => collect_plan_usize(op, "length")?.min(available),
    };
    Ok(batch.slice(offset, length))
}

fn apply_drop(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let columns =
        op.get("columns")
            .and_then(Value::as_array)
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "drop plan requires columns".to_string(),
            })?;
    let drop_names = columns
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| ZippyError::InvalidConfig {
                    reason: "drop column names must be strings".to_string(),
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let fields = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !drop_names.contains(field.name().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let columns = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !drop_names.contains(field.name().as_str()))
        .map(|(index, _)| batch.column(index).clone())
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway drop batch error=[{}]", error),
    })
}

fn apply_rename(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let mapping = op
        .get("mapping")
        .and_then(Value::as_object)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "rename plan requires mapping".to_string(),
        })?;
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            mapping
                .get(field.name())
                .and_then(Value::as_str)
                .map(|name| field.as_ref().clone().with_name(name))
                .unwrap_or_else(|| field.as_ref().clone())
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).map_err(|error| {
        ZippyError::Io {
            reason: format!("failed to build gateway rename batch error=[{}]", error),
        }
    })
}

fn apply_sort(batch: RecordBatch, op: &Value) -> Result<RecordBatch> {
    let by = op
        .get("by")
        .and_then(Value::as_array)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "sort plan requires by".to_string(),
        })?;
    if by.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "sort plan requires at least one key".to_string(),
        });
    }
    let descending = collect_sort_descending(op, by.len())?;
    let mut sort_columns = Vec::with_capacity(by.len());
    for (index, expr) in by.iter().enumerate() {
        let column_name = collect_plan_column_expr(expr, "sort")?;
        let column_index =
            batch
                .schema()
                .index_of(column_name)
                .map_err(|error| ZippyError::SchemaMismatch {
                    reason: format!(
                        "sort column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                })?;
        sort_columns.push(SortColumn {
            values: batch.column(column_index).clone(),
            options: Some(SortOptions {
                descending: descending[index],
                nulls_first: false,
            }),
        });
    }
    let indices = lexsort_to_indices(&sort_columns, None).map_err(|error| ZippyError::Io {
        reason: format!("failed to sort gateway batch error=[{}]", error),
    })?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                reason: format!("failed to reorder gateway column error=[{}]", error),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build gateway sorted batch error=[{}]", error),
    })
}

fn collect_sort_descending(op: &Value, key_count: usize) -> Result<Vec<bool>> {
    match op.get("descending") {
        Some(Value::Bool(value)) => Ok(vec![*value; key_count]),
        Some(Value::Array(values)) => {
            if values.len() != key_count {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "sort descending length mismatch keys=[{}] descending=[{}]",
                        key_count,
                        values.len()
                    ),
                });
            }
            values
                .iter()
                .map(|value| {
                    value.as_bool().ok_or_else(|| ZippyError::InvalidConfig {
                        reason: "sort descending values must be booleans".to_string(),
                    })
                })
                .collect()
        }
        Some(Value::Null) | None => Ok(vec![false; key_count]),
        Some(_) => Err(ZippyError::InvalidConfig {
            reason: "sort descending must be a boolean or boolean list".to_string(),
        }),
    }
}

fn collect_plan_usize(op: &Value, name: &str) -> Result<usize> {
    let value = op
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "collect plan requires non-negative integer field=[{}]",
                name
            ),
        })?;
    usize::try_from(value).map_err(|error| ZippyError::InvalidConfig {
        reason: format!(
            "collect plan integer overflow field=[{}] error=[{}]",
            name, error
        ),
    })
}

fn collect_plan_column_expr<'a>(expr: &'a Value, op_name: &str) -> Result<&'a str> {
    expr.get("value")
        .and_then(Value::as_str)
        .filter(|_| expr.get("kind").and_then(Value::as_str) == Some("col"))
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: format!(
                "native gateway {} currently supports column expressions",
                op_name
            ),
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
