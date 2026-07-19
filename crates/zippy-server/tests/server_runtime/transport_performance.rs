//! Manual release benchmark for the legacy Gateway and Arrow Flight transports.

use std::fs;
use std::io::{Cursor, Write};
use std::net::TcpStream;
use std::num::NonZeroU32;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::FlightClient;
use futures::TryStreamExt;
use prost::Message;
use serde::Serialize;
use zippy_protocol::ingest::encode_put_descriptor;
use zippy_protocol::query::{
    encode_query_descriptor, encode_query_request, FlightQueryRequest, FlightTableBinding,
};
use zippy_protocol::wire::{
    PutBatchResult, PutCommand, PutDurability, PutMutation, SessionOpenRequest,
    SessionOpenResponse, SessionRole,
};
use zippy_protocol::{AUTHORIZATION_METADATA_KEY, CURRENT_PROTOCOL_VERSION};
use zippy_types::query::{QueryInput, QueryPriority, QueryRequest};
use zippy_types::table::{CatalogName, TableRef};
use zippy_types::{QueryId, RequestId, ResourceLimit, SessionId};

use super::{
    append_table_schema, read_gateway_frame, reserve_tcp_addr,
    runtime_config_with_native_query_frontend_and_persist_root, seed_legacy_append_table,
    wait_until_bindable, write_test_parquet, LegacyServerRuntime,
};

const GATEWAY_TOKEN: &str = "shadow-catalog-test";
const FLIGHT_READ_TOKEN: &str = "transport-perf-flight-read-token-00000001";
const FLIGHT_WRITER_TOKEN: &str = "transport-perf-flight-writer-token-000001";
const DEFAULT_SOURCE_ROWS: usize = 262_144;
const DEFAULT_WARMUP_SAMPLES: usize = 3;
const DEFAULT_MEASURED_SAMPLES: usize = 100;
const DEFAULT_QUERY_ROWS: &[usize] = &[1, 1_024, 65_536];
const DEFAULT_WRITE_ROWS: &[usize] = &[1, 1_024, 65_536];
const DEFAULT_RESULT_BATCH_ROWS: usize = 8_192;
const DEFAULT_MIN_FLIGHT_THROUGHPUT_RATIO: f64 = 0.90;
const DEFAULT_MAX_FLIGHT_P99_RATIO: f64 = 1.20;
const REPORT_VERSION: u32 = 2;

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    source_rows: usize,
    warmup_samples: usize,
    measured_samples: usize,
    query_rows: Vec<usize>,
    write_rows: Vec<usize>,
    min_flight_throughput_ratio: f64,
    max_flight_p99_ratio: f64,
    output_path: Option<std::path::PathBuf>,
}

impl BenchmarkConfig {
    fn from_environment() -> Self {
        let source_rows = env_usize("ZIPPY_TRANSPORT_PERF_SOURCE_ROWS", DEFAULT_SOURCE_ROWS);
        let warmup_samples = env_usize(
            "ZIPPY_TRANSPORT_PERF_WARMUP_SAMPLES",
            DEFAULT_WARMUP_SAMPLES,
        );
        let measured_samples = env_usize_with_legacy_alias(
            "ZIPPY_TRANSPORT_PERF_MEASURED_SAMPLES",
            "ZIPPY_TRANSPORT_PERF_SAMPLES",
            DEFAULT_MEASURED_SAMPLES,
        );
        assert!(source_rows > 0, "source row count must be positive");
        assert!(
            measured_samples > 0,
            "measured sample count must be positive"
        );
        let query_rows = env_row_counts("ZIPPY_TRANSPORT_PERF_QUERY_ROWS", DEFAULT_QUERY_ROWS);
        let write_rows = env_row_counts("ZIPPY_TRANSPORT_PERF_WRITE_ROWS", DEFAULT_WRITE_ROWS);
        let min_flight_throughput_ratio = env_f64(
            "ZIPPY_TRANSPORT_PERF_MIN_FLIGHT_THROUGHPUT_RATIO",
            DEFAULT_MIN_FLIGHT_THROUGHPUT_RATIO,
        );
        let max_flight_p99_ratio = env_f64(
            "ZIPPY_TRANSPORT_PERF_MAX_FLIGHT_P99_RATIO",
            DEFAULT_MAX_FLIGHT_P99_RATIO,
        );
        assert!(
            query_rows.iter().all(|rows| *rows <= source_rows),
            "query row counts must not exceed source rows"
        );
        assert!(
            min_flight_throughput_ratio.is_finite() && min_flight_throughput_ratio > 0.0,
            "minimum Flight throughput ratio must be positive and finite"
        );
        assert!(
            max_flight_p99_ratio.is_finite() && max_flight_p99_ratio > 0.0,
            "maximum Flight p99 ratio must be positive and finite"
        );
        Self {
            source_rows,
            warmup_samples,
            measured_samples,
            query_rows,
            write_rows,
            min_flight_throughput_ratio,
            max_flight_p99_ratio,
            output_path: std::env::var_os("ZIPPY_TRANSPORT_PERF_REPORT_JSON").map(Into::into),
        }
    }
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    format_version: u32,
    git_sha: String,
    target: String,
    started_at_unix_ms: u128,
    source_rows: usize,
    warmup_samples: usize,
    measured_samples: usize,
    flight_session_open_micros: FlightSessionOpenLatency,
    scenarios: Vec<ScenarioReport>,
    gates: GateReport,
    gateway_metrics: serde_json::Value,
    flight_metrics: FlightMetrics,
    process_rss_kib: ProcessRss,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    operation: &'static str,
    rows_per_operation: usize,
    logical_bytes_per_operation: usize,
    gateway: SampleSummary,
    flight: SampleSummary,
    flight_to_gateway_rows_per_sec: f64,
    flight_to_gateway_p99_latency: f64,
}

#[derive(Debug, Serialize)]
struct SampleSummary {
    samples: usize,
    total_rows: u64,
    total_logical_bytes: u64,
    elapsed_seconds: f64,
    rows_per_second: f64,
    mebibytes_per_second: f64,
    latency_micros: LatencySummary,
    prepare_latency_micros: Option<LatencySummary>,
    time_to_first_batch_micros: Option<LatencySummary>,
}

#[derive(Debug, Serialize)]
struct LatencySummary {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct FlightMetrics {
    issued_tickets: u64,
    consumed_tickets: u64,
    put_batches: u64,
    put_rows: u64,
    put_bytes: u64,
    active_puts: usize,
    transport_buffer_bytes_in_use: usize,
}

#[derive(Debug, Serialize)]
struct ProcessRss {
    before_kib: u64,
    after_kib: u64,
    peak_kib: u64,
}

#[derive(Debug, Serialize)]
struct FlightSessionOpenLatency {
    read: f64,
    writer: f64,
}

#[derive(Debug, Serialize)]
struct GateReport {
    min_flight_throughput_ratio: f64,
    max_flight_p99_ratio: f64,
    pass: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct OperationSample {
    total: Duration,
    prepare: Option<Duration>,
    time_to_first_batch: Option<Duration>,
}

impl OperationSample {
    fn total(total: Duration) -> Self {
        Self {
            total,
            prepare: None,
            time_to_first_batch: None,
        }
    }
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_kib: Arc<AtomicU64>,
    join: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start(initial_kib: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_kib = Arc::new(AtomicU64::new(initial_kib));
        let thread_stop = Arc::clone(&stop);
        let thread_peak = Arc::clone(&peak_kib);
        let join = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                if let Some(current) = process_rss_kib() {
                    thread_peak.fetch_max(current, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        Self {
            stop,
            peak_kib,
            join: Some(join),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            join.join().expect("RSS sampler thread must stop cleanly");
        }
        self.peak_kib.load(Ordering::Relaxed)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual release benchmark; run with --ignored --test-threads=1"]
async fn flight_gateway_transport_performance_baseline() {
    let config = BenchmarkConfig::from_environment();
    let started_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must follow the Unix epoch")
        .as_millis();
    let temp = tempfile::tempdir().expect("create transport benchmark directory");
    let data_dir = temp.path().join("database");
    let persist_root = temp.path().join("persisted");
    fs::create_dir_all(&persist_root).expect("create persisted benchmark directory");
    let parquet_path = persist_root.join("events.parquet");
    write_test_parquet(
        &parquet_path,
        (0..config.source_rows)
            .map(|value| i64::try_from(value).expect("source row index fits i64"))
            .collect(),
    );
    seed_legacy_append_table(&data_dir, &persist_root, &parquet_path, config.source_rows);

    let gateway_addr = reserve_tcp_addr();
    let flight_addr = reserve_tcp_addr();
    let runtime_config =
        transport_runtime_config(&temp, &data_dir, &persist_root, gateway_addr, flight_addr);
    let mut runtime = LegacyServerRuntime::start(runtime_config)
        .expect("start combined Gateway and Flight benchmark runtime");
    let health = runtime.health();
    assert!(health.readiness.gateway_listener_ready);
    assert!(health.readiness.flight_protocol_ready);

    let (mut flight_reader, read_session, read_session_open) =
        authenticated_flight_client(flight_addr, FLIGHT_READ_TOKEN, SessionRole::Read).await;
    let (mut flight_writer, write_session, writer_session_open) =
        authenticated_flight_client(flight_addr, FLIGHT_WRITER_TOKEN, SessionRole::Writer).await;
    let gateway_endpoint = gateway_addr.to_string();
    let rss_before = process_rss_kib().unwrap_or(0);
    let rss_sampler = RssSampler::start(rss_before);
    let mut identity = 1_u128;
    let mut scenarios = Vec::new();

    for rows in config.query_rows.iter().copied() {
        let logical_bytes = rows.saturating_mul(std::mem::size_of::<i64>());
        let mut gateway_samples = Vec::with_capacity(config.measured_samples);
        let mut flight_samples = Vec::with_capacity(config.measured_samples);
        for sample in 0..config
            .warmup_samples
            .saturating_add(config.measured_samples)
        {
            let gateway_first = sample % 2 == 0;
            if gateway_first {
                let elapsed = timed_gateway_query(&gateway_endpoint, identity, rows);
                identity += 2;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut gateway_samples);
                let elapsed =
                    timed_flight_query(&mut flight_reader, read_session, identity, rows).await;
                identity += 1;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut flight_samples);
            } else {
                let elapsed =
                    timed_flight_query(&mut flight_reader, read_session, identity, rows).await;
                identity += 1;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut flight_samples);
                let elapsed = timed_gateway_query(&gateway_endpoint, identity, rows);
                identity += 2;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut gateway_samples);
            }
        }
        scenarios.push(scenario_report(
            "query",
            rows,
            logical_bytes,
            gateway_samples,
            flight_samples,
        ));
    }

    for rows in config.write_rows.iter().copied() {
        let batch = benchmark_batch(rows);
        let logical_bytes = batch.get_array_memory_size();
        let gateway_stream = format!("transport_perf_gateway_{rows}");
        let flight_stream = format!("transport_perf_flight_{rows}");
        let mut gateway_samples = Vec::with_capacity(config.measured_samples);
        let mut flight_samples = Vec::with_capacity(config.measured_samples);
        for sample in 0..config
            .warmup_samples
            .saturating_add(config.measured_samples)
        {
            let sequence = u64::try_from(sample).expect("sample index fits u64");
            let gateway_first = sample % 2 == 0;
            if gateway_first {
                let elapsed = timed_gateway_write(&gateway_endpoint, &gateway_stream, &batch);
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut gateway_samples);
                let elapsed = timed_flight_write(
                    &mut flight_writer,
                    write_session,
                    identity,
                    &flight_stream,
                    sequence,
                    batch.clone(),
                )
                .await;
                identity += 1;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut flight_samples);
            } else {
                let elapsed = timed_flight_write(
                    &mut flight_writer,
                    write_session,
                    identity,
                    &flight_stream,
                    sequence,
                    batch.clone(),
                )
                .await;
                identity += 1;
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut flight_samples);
                let elapsed = timed_gateway_write(&gateway_endpoint, &gateway_stream, &batch);
                record_after_warmup(sample, config.warmup_samples, elapsed, &mut gateway_samples);
            }
        }
        scenarios.push(scenario_report(
            "write_memory_ack",
            rows,
            logical_bytes,
            gateway_samples,
            flight_samples,
        ));
    }

    let gateway_metrics = runtime
        .gateway_metrics()
        .expect("combined runtime exposes Gateway metrics");
    let memory_ingest = runtime
        .memory_ingest_status()
        .expect("combined runtime exposes server-owned memory ingest metrics");
    let flight_status = runtime
        .flight_status()
        .expect("combined runtime exposes Flight metrics");
    let samples_per_transport = config
        .warmup_samples
        .saturating_add(config.measured_samples);
    let expected_batches_per_transport = config
        .write_rows
        .len()
        .saturating_mul(samples_per_transport);
    let expected_rows_per_transport = config
        .write_rows
        .iter()
        .copied()
        .sum::<usize>()
        .saturating_mul(samples_per_transport);
    assert_eq!(
        memory_ingest.write_batches_total,
        u64::try_from(expected_batches_per_transport.saturating_mul(2))
            .expect("combined batch count fits u64")
    );
    assert_eq!(
        memory_ingest.written_rows_total,
        u64::try_from(expected_rows_per_transport.saturating_mul(2))
            .expect("combined row count fits u64")
    );
    assert_eq!(
        gateway_metrics["memory_ingest"]["write_batches_total"],
        memory_ingest.write_batches_total
    );
    assert_eq!(
        gateway_metrics["memory_ingest"]["written_rows_total"],
        memory_ingest.written_rows_total
    );
    assert_eq!(flight_status.pending_tickets, 0);
    assert_eq!(flight_status.active_puts, 0);
    let rss_after = process_rss_kib().unwrap_or(0);
    let rss_peak = rss_sampler.stop().max(rss_before).max(rss_after);
    let gates = evaluate_gates(
        &scenarios,
        config.min_flight_throughput_ratio,
        config.max_flight_p99_ratio,
    );
    let gates_pass = gates.pass;
    let report = BenchmarkReport {
        format_version: REPORT_VERSION,
        git_sha: current_git_sha(),
        target: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        started_at_unix_ms,
        source_rows: config.source_rows,
        warmup_samples: config.warmup_samples,
        measured_samples: config.measured_samples,
        flight_session_open_micros: FlightSessionOpenLatency {
            read: duration_micros(read_session_open),
            writer: duration_micros(writer_session_open),
        },
        scenarios,
        gates,
        gateway_metrics,
        flight_metrics: FlightMetrics {
            issued_tickets: flight_status.issued_tickets,
            consumed_tickets: flight_status.consumed_tickets,
            put_batches: flight_status.put_batches,
            put_rows: flight_status.put_rows,
            put_bytes: flight_status.put_bytes,
            active_puts: flight_status.active_puts,
            transport_buffer_bytes_in_use: flight_status.transport_buffer_bytes_in_use,
        },
        process_rss_kib: ProcessRss {
            before_kib: rss_before,
            after_kib: rss_after,
            peak_kib: rss_peak,
        },
    };
    let encoded = serde_json::to_string_pretty(&report).expect("encode benchmark report");
    println!("{encoded}");
    if let Some(path) = config.output_path.as_ref() {
        write_report(path, encoded.as_bytes());
    }

    drop(flight_reader);
    drop(flight_writer);
    let outcome = runtime.shutdown();
    assert!(outcome.is_clean(), "shutdown outcome: {outcome:?}");
    wait_until_bindable(flight_addr);
    wait_until_bindable(gateway_addr);
    assert!(
        gates_pass,
        "Flight/Gateway transport performance gate failed"
    );
}

fn transport_runtime_config(
    temp: &tempfile::TempDir,
    data_dir: &Path,
    persist_root: &Path,
    gateway_addr: std::net::SocketAddr,
    flight_addr: std::net::SocketAddr,
) -> super::LegacyServerRuntimeConfig {
    let config = runtime_config_with_native_query_frontend_and_persist_root(
        temp,
        data_dir,
        gateway_addr,
        persist_root,
    );
    let config_path = config
        .master()
        .config_path
        .as_ref()
        .expect("benchmark runtime config has an explicit path");
    let read_token_path = temp.path().join("flight-read.token");
    let writer_token_path = temp.path().join("flight-writer.token");
    fs::write(&read_token_path, format!("{FLIGHT_READ_TOKEN}\n")).expect("write Flight read token");
    fs::write(&writer_token_path, format!("{FLIGHT_WRITER_TOKEN}\n"))
        .expect("write Flight writer token");
    fs::set_permissions(&read_token_path, fs::Permissions::from_mode(0o600))
        .expect("protect Flight read token");
    fs::set_permissions(&writer_token_path, fs::Permissions::from_mode(0o600))
        .expect("protect Flight writer token");
    let mut source = fs::read_to_string(config_path).expect("read benchmark runtime config");
    source.push_str(&format!(
        concat!(
            "\n[flight]\n",
            "enabled = true\n",
            "endpoint = {:?}\n",
            "max_message_bytes = 2097152\n",
            "max_pending_tickets = 64\n\n",
            "[flight.auth]\n",
            "enabled = true\n",
            "token_file = {:?}\n",
            "writer_token_file = {:?}\n",
            "session_ttl_ms = 600000\n",
            "max_sessions = 8\n\n",
            "[flight.ingest]\n",
            "enabled = true\n",
            "max_concurrent_puts = 4\n",
            "max_batches_per_put = 1024\n",
            "max_rows_per_batch = 65536\n",
            "max_rows_per_put = 1048576\n",
            "max_batch_bytes = 2097152\n",
            "max_bytes_per_put = 268435456\n"
        ),
        flight_addr.to_string(),
        read_token_path.display().to_string(),
        writer_token_path.display().to_string(),
    ));
    fs::write(config_path, source).expect("write combined benchmark runtime config");
    config
}

async fn authenticated_flight_client(
    flight_addr: std::net::SocketAddr,
    token: &str,
    expected_role: SessionRole,
) -> (FlightClient, SessionId, Duration) {
    let started = Instant::now();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{flight_addr}"))
        .expect("valid benchmark Flight endpoint")
        .connect()
        .await
        .expect("connect benchmark Flight endpoint");
    let mut client = FlightClient::new(channel);
    client
        .add_header(AUTHORIZATION_METADATA_KEY, &format!("Bearer {token}"))
        .expect("set Flight bootstrap bearer");
    let response = client
        .handshake(
            SessionOpenRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
            }
            .encode_to_vec(),
        )
        .await
        .expect("open benchmark Flight session");
    let opened = SessionOpenResponse::decode(response).expect("decode Flight session response");
    assert_eq!(
        SessionRole::try_from(opened.role).expect("valid Flight session role"),
        expected_role
    );
    let session_id = SessionId::from_bytes(
        opened
            .session_id
            .as_ref()
            .try_into()
            .expect("Flight session ID has 16 bytes"),
    )
    .expect("Flight session ID is nonzero");
    client
        .add_header(
            AUTHORIZATION_METADATA_KEY,
            &format!("Bearer {}", opened.bearer_token),
        )
        .expect("set Flight session bearer");
    (client, session_id, started.elapsed())
}

fn timed_gateway_query(endpoint: &str, identity: u128, rows: usize) -> OperationSample {
    let header = serde_json::json!({
        "kind": "query_sql",
        "token": GATEWAY_TOKEN,
        "query_id": QueryId::from_u128(identity).expect("query ID is nonzero"),
        "session_id": SessionId::from_u128(identity + 1).expect("session ID is nonzero"),
        "sql": format!("SELECT value FROM events LIMIT {rows}"),
        "bindings": [{
            "sql_name": "events",
            "table": {"type": "name", "namespace": "default", "table": "events"},
        }],
        "result_batch_rows": DEFAULT_RESULT_BATCH_ROWS,
    });
    let encoded_header = serde_json::to_vec(&header).expect("encode Gateway query header");
    let started = Instant::now();
    let mut stream = TcpStream::connect(endpoint).expect("connect Gateway query endpoint");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set Gateway query timeout");
    stream
        .write_all(&(encoded_header.len() as u32).to_be_bytes())
        .expect("write Gateway query header length");
    stream
        .write_all(&0_u64.to_be_bytes())
        .expect("write Gateway query payload length");
    stream
        .write_all(&encoded_header)
        .expect("write Gateway query header");
    let mut returned_rows = 0_usize;
    let mut prepare = None;
    let mut time_to_first_batch = None;
    loop {
        let (header, payload) = read_gateway_frame(&mut stream);
        assert_ne!(header["status"], "error", "Gateway query failed: {header}");
        match header.get("kind").and_then(serde_json::Value::as_str) {
            Some("collect_start") => prepare = Some(started.elapsed()),
            Some("collect_chunk") => {
                let reader = StreamReader::try_new(Cursor::new(payload), None)
                    .expect("decode Gateway query Arrow stream");
                returned_rows += reader
                    .map(|batch| batch.expect("decode Gateway query batch").num_rows())
                    .sum::<usize>();
                time_to_first_batch.get_or_insert_with(|| started.elapsed());
            }
            Some("collect_end") => break,
            kind => panic!("unexpected Gateway query frame kind=[{kind:?}]"),
        }
    }
    let elapsed = started.elapsed();
    assert_eq!(returned_rows, rows);
    OperationSample {
        total: elapsed,
        prepare,
        time_to_first_batch,
    }
}

async fn timed_flight_query(
    client: &mut FlightClient,
    session_id: SessionId,
    identity: u128,
    rows: usize,
) -> OperationSample {
    let table = TableRef::Name {
        namespace: CatalogName::new("default").expect("valid namespace"),
        table: CatalogName::new("events").expect("valid table name"),
    };
    let request = FlightQueryRequest {
        request: QueryRequest {
            query_id: QueryId::from_u128(identity).expect("query ID is nonzero"),
            session_id,
            input: QueryInput::Sql(format!("SELECT value FROM events LIMIT {rows}")),
            tables: vec![table.clone()],
            priority: QueryPriority::Interactive,
            deadline_ns: None,
            memory_limit_bytes: ResourceLimit::ServerDefault,
            spill_limit_bytes: ResourceLimit::Disabled,
            result_batch_rows: NonZeroU32::new(
                u32::try_from(DEFAULT_RESULT_BATCH_ROWS).expect("result batch size fits u32"),
            ),
        },
        bindings: vec![FlightTableBinding {
            sql_name: "events".to_owned(),
            table,
        }],
    };
    let descriptor = encode_query_descriptor(
        &encode_query_request(&request).expect("encode benchmark Flight query"),
    );
    let started = Instant::now();
    let info = client
        .get_flight_info(descriptor)
        .await
        .expect("prepare benchmark Flight query");
    let prepare = started.elapsed();
    let mut stream = client
        .do_get(
            info.endpoint[0]
                .ticket
                .clone()
                .expect("Flight query has a ticket"),
        )
        .await
        .expect("start benchmark Flight query");
    let mut returned_rows = 0_usize;
    let mut time_to_first_batch = None;
    while let Some(batch) = stream
        .try_next()
        .await
        .expect("decode benchmark Flight query batch")
    {
        returned_rows = returned_rows.saturating_add(batch.num_rows());
        time_to_first_batch.get_or_insert_with(|| started.elapsed());
    }
    let elapsed = started.elapsed();
    assert_eq!(returned_rows, rows);
    OperationSample {
        total: elapsed,
        prepare: Some(prepare),
        time_to_first_batch,
    }
}

fn timed_gateway_write(endpoint: &str, stream_name: &str, batch: &RecordBatch) -> OperationSample {
    let payload = encode_gateway_batch(batch);
    let header = serde_json::json!({
        "kind": "write_batch",
        "stream_name": stream_name,
        "token": GATEWAY_TOKEN,
        "rows": batch.num_rows(),
    });
    let encoded_header = serde_json::to_vec(&header).expect("encode Gateway write header");
    let started = Instant::now();
    let mut stream = TcpStream::connect(endpoint).expect("connect Gateway write endpoint");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set Gateway write timeout");
    stream
        .write_all(&(encoded_header.len() as u32).to_be_bytes())
        .expect("write Gateway header length");
    stream
        .write_all(&(payload.len() as u64).to_be_bytes())
        .expect("write Gateway payload length");
    stream
        .write_all(&encoded_header)
        .expect("write Gateway header");
    stream.write_all(&payload).expect("write Gateway payload");
    let (response, response_payload) = read_gateway_frame(&mut stream);
    let elapsed = started.elapsed();
    assert_eq!(response["status"], "ok", "Gateway write failed: {response}");
    assert!(response_payload.is_empty());
    OperationSample::total(elapsed)
}

async fn timed_flight_write(
    client: &mut FlightClient,
    session_id: SessionId,
    identity: u128,
    stream_name: &str,
    batch_sequence: u64,
    batch: RecordBatch,
) -> OperationSample {
    let rows = batch.num_rows();
    let descriptor = encode_put_descriptor(&PutCommand {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        session_id: session_id.into_bytes().to_vec().into(),
        put_id: RequestId::from_u128(identity)
            .expect("put ID is nonzero")
            .into_bytes()
            .to_vec()
            .into(),
        stream_name: stream_name.to_owned(),
        first_batch_sequence: batch_sequence,
        mutation: PutMutation::Append as i32,
        durability: PutDurability::Memory as i32,
    });
    let input = FlightDataEncoderBuilder::new()
        .with_schema(batch.schema())
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::iter([Ok(batch)]));
    let started = Instant::now();
    let results = client
        .do_put(input)
        .await
        .expect("start benchmark Flight put")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect benchmark Flight put receipts");
    let elapsed = started.elapsed();
    assert_eq!(results.len(), 1);
    let receipt = PutBatchResult::decode(results[0].app_metadata.clone())
        .expect("decode benchmark Flight receipt");
    assert_eq!(receipt.batch_sequence, batch_sequence);
    assert_eq!(
        usize::try_from(receipt.row_count).expect("receipt row count fits usize"),
        rows
    );
    OperationSample::total(elapsed)
}

fn benchmark_batch(rows: usize) -> RecordBatch {
    let values = (0..rows)
        .map(|value| i64::try_from(value).expect("write row index fits i64"))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        append_table_schema(),
        vec![Arc::new(Int64Array::from(values))],
    )
    .expect("construct benchmark write batch")
}

fn encode_gateway_batch(batch: &RecordBatch) -> Vec<u8> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema())
            .expect("create Gateway benchmark Arrow writer");
        writer
            .write(batch)
            .expect("write Gateway benchmark Arrow batch");
        writer
            .finish()
            .expect("finish Gateway benchmark Arrow stream");
    }
    payload
}

fn record_after_warmup(
    sample: usize,
    warmup_samples: usize,
    elapsed: OperationSample,
    output: &mut Vec<OperationSample>,
) {
    if sample >= warmup_samples {
        output.push(elapsed);
    }
}

fn scenario_report(
    operation: &'static str,
    rows_per_operation: usize,
    logical_bytes_per_operation: usize,
    gateway_samples: Vec<OperationSample>,
    flight_samples: Vec<OperationSample>,
) -> ScenarioReport {
    let gateway = sample_summary(
        &gateway_samples,
        rows_per_operation,
        logical_bytes_per_operation,
    );
    let flight = sample_summary(
        &flight_samples,
        rows_per_operation,
        logical_bytes_per_operation,
    );
    ScenarioReport {
        operation,
        rows_per_operation,
        logical_bytes_per_operation,
        flight_to_gateway_rows_per_sec: ratio(flight.rows_per_second, gateway.rows_per_second),
        flight_to_gateway_p99_latency: ratio(flight.latency_micros.p99, gateway.latency_micros.p99),
        gateway,
        flight,
    }
}

fn sample_summary(
    samples: &[OperationSample],
    rows_per_operation: usize,
    logical_bytes_per_operation: usize,
) -> SampleSummary {
    assert!(
        !samples.is_empty(),
        "benchmark sample set must not be empty"
    );
    let elapsed_seconds = samples
        .iter()
        .map(|sample| sample.total.as_secs_f64())
        .sum::<f64>();
    let total_rows = u64::try_from(rows_per_operation)
        .expect("rows per operation fit u64")
        .saturating_mul(u64::try_from(samples.len()).expect("sample count fits u64"));
    let total_logical_bytes = u64::try_from(logical_bytes_per_operation)
        .expect("bytes per operation fit u64")
        .saturating_mul(u64::try_from(samples.len()).expect("sample count fits u64"));
    SampleSummary {
        samples: samples.len(),
        total_rows,
        total_logical_bytes,
        elapsed_seconds,
        rows_per_second: total_rows as f64 / elapsed_seconds,
        mebibytes_per_second: total_logical_bytes as f64 / elapsed_seconds / (1024.0 * 1024.0),
        latency_micros: summarize_latencies(
            &samples
                .iter()
                .map(|sample| sample.total)
                .collect::<Vec<_>>(),
        ),
        prepare_latency_micros: summarize_optional_latencies(
            samples.iter().map(|sample| sample.prepare),
        ),
        time_to_first_batch_micros: summarize_optional_latencies(
            samples.iter().map(|sample| sample.time_to_first_batch),
        ),
    }
}

fn summarize_optional_latencies(
    samples: impl Iterator<Item = Option<Duration>>,
) -> Option<LatencySummary> {
    let samples = samples.flatten().collect::<Vec<_>>();
    (!samples.is_empty()).then(|| summarize_latencies(&samples))
}

fn summarize_latencies(samples: &[Duration]) -> LatencySummary {
    let mut micros = samples
        .iter()
        .map(|duration| duration.as_secs_f64() * 1_000_000.0)
        .collect::<Vec<_>>();
    micros.sort_by(f64::total_cmp);
    LatencySummary {
        p50: percentile(&micros, 0.50),
        p95: percentile(&micros, 0.95),
        p99: percentile(&micros, 0.99),
        max: *micros.last().expect("latency samples are nonempty"),
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    assert!(!sorted.is_empty(), "percentile input must not be empty");
    assert!(
        (0.0..=1.0).contains(&quantile),
        "percentile quantile must be within [0, 1]"
    );
    let rank = ((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[rank]
}

#[test]
fn percentile_uses_nearest_rank() {
    let samples = (1..=100).map(f64::from).collect::<Vec<_>>();

    assert_eq!(percentile(&samples, 0.50), 50.0);
    assert_eq!(percentile(&samples, 0.95), 95.0);
    assert_eq!(percentile(&samples, 0.99), 99.0);
    assert_eq!(percentile(&samples, 1.00), 100.0);
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn evaluate_gates(
    scenarios: &[ScenarioReport],
    min_flight_throughput_ratio: f64,
    max_flight_p99_ratio: f64,
) -> GateReport {
    let mut failures = Vec::new();
    for scenario in scenarios {
        if scenario.flight_to_gateway_rows_per_sec < min_flight_throughput_ratio {
            failures.push(format!(
                "Flight throughput ratio below gate operation=[{}] rows=[{}] ratio=[{:.6}] minimum=[{:.6}]",
                scenario.operation,
                scenario.rows_per_operation,
                scenario.flight_to_gateway_rows_per_sec,
                min_flight_throughput_ratio,
            ));
        }
        if scenario.flight_to_gateway_p99_latency > max_flight_p99_ratio {
            failures.push(format!(
                "Flight p99 latency ratio above gate operation=[{}] rows=[{}] ratio=[{:.6}] maximum=[{:.6}]",
                scenario.operation,
                scenario.rows_per_operation,
                scenario.flight_to_gateway_p99_latency,
                max_flight_p99_ratio,
            ));
        }
    }
    GateReport {
        min_flight_throughput_ratio,
        max_flight_p99_ratio,
        pass: failures.is_empty(),
        failures,
    }
}

fn duration_micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} value=[{value}] error=[{error}]"))
    })
}

fn env_usize_with_legacy_alias(name: &str, legacy_name: &str, default: usize) -> usize {
    if std::env::var_os(name).is_some() {
        env_usize(name, default)
    } else {
        env_usize(legacy_name, default)
    }
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name).map_or(default, |value| {
        value
            .parse::<f64>()
            .unwrap_or_else(|error| panic!("invalid {name} value=[{value}] error=[{error}]"))
    })
}

fn env_row_counts(name: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(name) {
        Ok(value) => {
            let rows = value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(|part| {
                    part.parse::<usize>().unwrap_or_else(|error| {
                        panic!("invalid {name} row count=[{part}] error=[{error}]")
                    })
                })
                .collect::<Vec<_>>();
            assert!(
                !rows.is_empty() && rows.iter().all(|row_count| *row_count > 0),
                "{name} must contain positive comma-separated row counts"
            );
            rows
        }
        Err(_) => default.to_vec(),
    }
}

fn process_rss_kib() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

fn current_git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|sha| sha.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn write_report(path: &Path, encoded: &[u8]) {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).expect("create benchmark report directory");
    let mut output = encoded.to_vec();
    output.push(b'\n');
    fs::write(path, output).expect("write benchmark report JSON");
}
