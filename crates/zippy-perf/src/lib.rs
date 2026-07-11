use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use zippy_core::{
    spawn_engine_with_publisher, spawn_source_engine_with_publisher, Engine, EngineConfig,
    EngineMetricsSnapshot, EngineStatus, LateDataPolicy, OverflowPolicy, Publisher, Result,
    SchemaRef, SegmentTableView, Source, SourceEvent, SourceHandle, SourceMode, SourceSink,
    StreamHello,
};
use zippy_engines::{
    CrossSectionalEngine, StreamTableDescriptorPublisher, StreamTableMaterializer, TimeSeriesEngine,
};
use zippy_io::{NullPublisher, ZmqSource, ZmqStreamPublisher};
use zippy_operators::{AggLastSpec, CSDemeanSpec, CSRankSpec, CSZscoreSpec};
use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
    RowSpanView, SegmentCellValue,
};

const DEFAULT_WINDOW_NS: i64 = 1_000_000;
const REMOTE_STARTUP_DELAY_MS: u64 = 500;
const REMOTE_SHUTDOWN_GRACE_SEC: u64 = 5;
const REMOTE_IDLE_GRACE_SEC: u64 = 3;
const REMOTE_STARTUP_GRACE_SEC: u64 = 30;
const MIN_PASS_RATIO: f64 = 0.90;
const WRITE_TO_SUBSCRIBE_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TO_SUBSCRIBE_WAIT_SLICE: Duration = Duration::from_millis(10);
const WRITE_TO_SUBSCRIBE_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(1);

type PerfResult<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerfProfile {
    InprocTimeseries,
    RemotePipelineUpstream,
    RemotePipelineDownstream,
    StreamTableSegmentCopy,
    StreamTableSegmentForward,
    SingleRowLowRate,
    WriteToSubscribeE2e,
    RuntimeStreamTableE2e,
    StreamTableSubscriberE2e,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverflowPolicyConfig {
    Block,
    Reject,
    DropOldest,
}

impl OverflowPolicyConfig {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Reject => "reject",
            Self::DropOldest => "drop_oldest",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfConfig {
    pub profile: PerfProfile,
    pub rows_per_batch: usize,
    pub target_rows_per_sec: u64,
    pub duration_sec: u64,
    pub warmup_sec: u64,
    pub symbols: usize,
    pub endpoint: String,
    pub buffer_capacity: usize,
    pub overflow_policy: OverflowPolicyConfig,
    pub max_p95_micros: Option<f64>,
    pub max_p99_micros: Option<f64>,
    pub max_queue_depth: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    #[serde(default)]
    pub sample_count: u64,
    pub p50_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
    #[serde(default)]
    pub max_micros: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineMetricsReport {
    pub processed_batches_total: u64,
    pub processed_rows_total: u64,
    pub output_batches_total: u64,
    pub dropped_batches_total: u64,
    pub late_rows_total: u64,
    pub publish_errors_total: u64,
    pub queue_depth: usize,
    #[serde(default)]
    pub max_observed_queue_depth: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceMetricsReport {
    pub source_events_total: u64,
    pub source_data_batches_total: u64,
    pub source_control_events_total: u64,
    pub source_decode_errors_total: u64,
    pub source_schema_mismatches_total: u64,
    pub source_sequence_gaps_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfReportMetadata {
    pub git_sha: String,
    pub target: String,
    pub started_at_unix_ms: u128,
}

impl PerfReportMetadata {
    fn capture() -> Self {
        Self {
            git_sha: current_git_sha(),
            target: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            started_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfReport {
    pub profile: PerfProfile,
    pub config: PerfConfig,
    pub metadata: PerfReportMetadata,
    pub input_rows_total: u64,
    pub output_rows_total: u64,
    pub actual_average_rows_per_sec: f64,
    pub actual_peak_rows_per_sec: f64,
    pub batches_per_sec: f64,
    pub latency: LatencySummary,
    pub secondary_latency: Option<LatencySummary>,
    pub engine_status: String,
    pub engine_metrics: EngineMetricsReport,
    pub source_metrics: Option<SourceMetricsReport>,
    pub pass: bool,
}

pub fn run_profile(config: &PerfConfig) -> PerfResult<PerfReport> {
    validate_config(config)?;

    match config.profile {
        PerfProfile::InprocTimeseries => run_inproc_timeseries(config),
        PerfProfile::RemotePipelineUpstream => run_remote_pipeline_upstream(config),
        PerfProfile::RemotePipelineDownstream => run_remote_pipeline_downstream(config),
        PerfProfile::StreamTableSegmentCopy => run_stream_table_segment(config, false),
        PerfProfile::StreamTableSegmentForward => run_stream_table_segment(config, true),
        PerfProfile::SingleRowLowRate => run_single_row_low_rate(config),
        PerfProfile::WriteToSubscribeE2e => run_write_to_subscribe_e2e(config),
        PerfProfile::RuntimeStreamTableE2e => run_runtime_stream_table_e2e(config),
        PerfProfile::StreamTableSubscriberE2e => run_stream_table_subscriber_e2e(config),
    }
}

pub fn write_report_json(path: &Path, report: &PerfReport) -> PerfResult<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| error.to_string())?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    fs::write(path, json).map_err(|error| error.to_string())
}

fn current_git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|sha| sha.trim().to_string())
        .filter(|sha| !sha.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn format_report(report: &PerfReport) -> String {
    let mut lines = vec![
        format!("profile={:?}", report.profile),
        format!("git_sha={}", report.metadata.git_sha),
        format!("target={}", report.metadata.target),
        format!("pass={}", report.pass),
        format!("engine_status={}", report.engine_status),
        format!("input_rows_total={}", report.input_rows_total),
        format!("output_rows_total={}", report.output_rows_total),
        format!(
            "actual_average_rows_per_sec={:.2}",
            report.actual_average_rows_per_sec
        ),
        format!(
            "actual_peak_rows_per_sec={:.2}",
            report.actual_peak_rows_per_sec
        ),
        format!("batches_per_sec={:.2}", report.batches_per_sec),
        format!(
            "latency_micros[count={}, p50={:.2}, p95={:.2}, p99={:.2}, max={:.2}]",
            report.latency.sample_count,
            report.latency.p50_micros,
            report.latency.p95_micros,
            report.latency.p99_micros,
            report.latency.max_micros
        ),
    ];

    if let Some(secondary_latency) = report.secondary_latency.as_ref() {
        lines.push(format!(
            "secondary_latency_micros[count={}, p50={:.2}, p95={:.2}, p99={:.2}, max={:.2}]",
            secondary_latency.sample_count,
            secondary_latency.p50_micros,
            secondary_latency.p95_micros,
            secondary_latency.p99_micros,
            secondary_latency.max_micros
        ));
    }

    if let Some(source_metrics) = report.source_metrics.as_ref() {
        lines.push(format!(
            "source_metrics[events={}, data_batches={}, control_events={}, decode_errors={}, schema_mismatches={}, sequence_gaps={}]",
            source_metrics.source_events_total,
            source_metrics.source_data_batches_total,
            source_metrics.source_control_events_total,
            source_metrics.source_decode_errors_total,
            source_metrics.source_schema_mismatches_total,
            source_metrics.source_sequence_gaps_total
        ));
    }

    lines.join("\n")
}

fn run_inproc_timeseries(config: &PerfConfig) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let mut factory = TickBatchFactory::new(
        Arc::clone(&tick_schema),
        config.rows_per_batch,
        config.symbols,
        config.target_rows_per_sec,
    )?;
    let engine = build_timeseries_engine("perf_inproc_timeseries", Arc::clone(&tick_schema))
        .map_err(|error| error.to_string())?;
    let publisher = CountingPublisher::new(NullPublisher::default());
    let publisher_counters = publisher.counters();
    let mut handle = spawn_engine_with_publisher(engine, engine_config(config), publisher)
        .map_err(|error| error.to_string())?;

    let drive_stats = drive_inproc_generator(config, &mut factory, &mut handle)?;

    handle.flush().map_err(|error| error.to_string())?;
    let drain_completed_at = Instant::now();
    handle.stop().map_err(|error| error.to_string())?;

    let mut engine_metrics = engine_metrics_report(handle.metrics());
    engine_metrics.max_observed_queue_depth = engine_metrics
        .max_observed_queue_depth
        .max(drive_stats.max_observed_queue_depth);
    validate_inproc_completion(
        drive_stats.input_rows_total,
        engine_metrics.processed_rows_total,
    )?;
    let steady_elapsed = drain_completed_at.duration_since(drive_stats.steady_started_at);
    let actual_average_rows_per_sec =
        count_per_sec_over_elapsed(drive_stats.steady_rows_total, steady_elapsed);
    let batches_per_sec =
        count_per_sec_over_elapsed(drive_stats.steady_batches_total, steady_elapsed);
    let output_rows_total = publisher_counters.output_rows_total.load(Ordering::Relaxed);
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let pass = evaluate_pass(
        config,
        actual_average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        None,
        &latency,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec,
        latency,
        secondary_latency: None,
        engine_status: handle.status().as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn run_remote_pipeline_upstream(config: &PerfConfig) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let bar_schema = build_bar_schema(&tick_schema).map_err(|error| error.to_string())?;
    let stream_publisher =
        ZmqStreamPublisher::bind(&config.endpoint, "perf_bars", Arc::clone(&bar_schema))
            .map_err(|error| error.to_string())?;
    let publisher = CountingPublisher::new(stream_publisher);
    let publisher_counters = publisher.counters();
    let mut factory = TickBatchFactory::new(
        Arc::clone(&tick_schema),
        config.rows_per_batch,
        config.symbols,
        config.target_rows_per_sec,
    )?;
    let engine = build_timeseries_engine("perf_remote_upstream", Arc::clone(&tick_schema))
        .map_err(|error| error.to_string())?;
    let mut handle = spawn_engine_with_publisher(engine, engine_config(config), publisher)
        .map_err(|error| error.to_string())?;

    thread::sleep(Duration::from_millis(REMOTE_STARTUP_DELAY_MS));
    let drive_stats = drive_generator(config, &mut factory, |batch| {
        handle.write(batch).map_err(|error| error.to_string())
    })?;

    handle.flush().map_err(|error| error.to_string())?;
    handle.stop().map_err(|error| error.to_string())?;

    let engine_metrics = engine_metrics_report(handle.metrics());
    let output_rows_total = publisher_counters.output_rows_total.load(Ordering::Relaxed);
    let output_batches_total = publisher_counters
        .output_batches_total
        .load(Ordering::Relaxed);
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let pass = evaluate_pass(
        config,
        drive_stats.actual_average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        None,
        &latency,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(output_batches_total, config.duration_sec as f64),
        latency,
        secondary_latency: None,
        engine_status: handle.status().as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn run_remote_pipeline_downstream(config: &PerfConfig) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let bar_schema = build_bar_schema(&tick_schema).map_err(|error| error.to_string())?;
    let source = ZmqSource::connect(
        "perf_remote_source",
        &config.endpoint,
        Arc::clone(&bar_schema),
        SourceMode::Pipeline,
    )
    .map_err(|error| error.to_string())?;
    let counting_source = CountingSource::new(source);
    let source_counters = counting_source.counters();
    let publisher = CountingPublisher::new(NullPublisher::default());
    let publisher_counters = publisher.counters();
    let engine = build_cross_sectional_engine("perf_remote_downstream", Arc::clone(&bar_schema))
        .map_err(|error| error.to_string())?;
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(counting_source),
        engine,
        engine_config(config),
        publisher,
    )
    .map_err(|error| error.to_string())?;

    wait_for_remote_shutdown(&mut handle, config, &source_counters)?;

    let engine_metrics = engine_metrics_report(handle.metrics());
    let source_metrics = source_metrics_report(&source_counters);
    let output_rows_total = publisher_counters.output_rows_total.load(Ordering::Relaxed);
    let output_batches_total = publisher_counters
        .output_batches_total
        .load(Ordering::Relaxed);
    let observed_secs = source_counters
        .observed_seconds()
        .unwrap_or(config.duration_sec as f64)
        .max(1e-9);
    let average_rows_per_sec = engine_metrics.processed_rows_total as f64 / observed_secs;
    let latency = LatencySummary::default();
    let pass = evaluate_pass(
        config,
        average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        Some(&source_metrics),
        &latency,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: engine_metrics.processed_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: average_rows_per_sec,
        actual_peak_rows_per_sec: average_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(output_batches_total, observed_secs),
        latency,
        secondary_latency: None,
        engine_status: handle.status().as_str().to_string(),
        engine_metrics,
        source_metrics: Some(source_metrics),
        pass,
    })
}

fn run_stream_table_segment(config: &PerfConfig, forwarding: bool) -> PerfResult<PerfReport> {
    let row_capacity = config
        .rows_per_batch
        .saturating_mul(64)
        .max(config.rows_per_batch)
        .min(1_048_576);
    run_stream_table_segment_with_row_capacity(config, forwarding, row_capacity)
}

fn run_single_row_low_rate(config: &PerfConfig) -> PerfResult<PerfReport> {
    let profile_config = single_row_profile_config(config);
    let row_capacity = expected_single_row_capacity(&profile_config)?;
    run_stream_table_segment_with_row_capacity(&profile_config, false, row_capacity)
}

fn run_stream_table_segment_with_row_capacity(
    config: &PerfConfig,
    forwarding: bool,
    row_capacity: usize,
) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let table = build_tick_segment_table(
        config.rows_per_batch,
        config.symbols,
        config.target_rows_per_sec,
    )?;
    let mut materializer = StreamTableMaterializer::new_with_row_capacity(
        "perf_stream_table",
        tick_schema,
        row_capacity,
    )
    .map_err(|error| error.to_string())?;
    let descriptor_publisher = Arc::new(CountingDescriptorPublisher::default());
    if forwarding {
        materializer = materializer
            .with_descriptor_publisher(descriptor_publisher)
            .with_descriptor_forwarding(true);
    }

    let drive_stats = drive_segment_view(config, &table, |input| {
        materializer
            .on_data(input)
            .map(|_| ())
            .map_err(|error| error.to_string())
    })?;
    materializer.on_flush().map_err(|error| error.to_string())?;

    let processed_batches_total = drive_stats.input_rows_total / config.rows_per_batch as u64;
    let engine_metrics = EngineMetricsReport {
        processed_batches_total,
        processed_rows_total: drive_stats.input_rows_total,
        output_batches_total: processed_batches_total,
        dropped_batches_total: 0,
        late_rows_total: 0,
        publish_errors_total: 0,
        queue_depth: 0,
        max_observed_queue_depth: 0,
    };
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let pass = evaluate_pass(
        config,
        drive_stats.actual_average_rows_per_sec,
        EngineStatus::Stopped,
        &engine_metrics,
        None,
        &latency,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total: drive_stats.input_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(
            processed_batches_total,
            config.duration_sec as f64,
        ),
        latency,
        secondary_latency: None,
        engine_status: EngineStatus::Stopped.as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn run_write_to_subscribe_e2e(config: &PerfConfig) -> PerfResult<PerfReport> {
    let profile_config = single_row_profile_config(config);
    let row_capacity = expected_single_row_capacity(&profile_config)?;
    let schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("sent_elapsed_ns", ColumnType::Int64),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .map_err(str::to_string)?;
    let layout = LayoutPlan::for_schema(&schema, row_capacity).map_err(str::to_string)?;
    let mut writer =
        ActiveSegmentWriter::new_for_runtime(schema.clone(), layout.clone(), 10_001, 0)
            .map_err(str::to_string)?;
    let envelope = writer
        .active_descriptor()
        .to_envelope_bytes()
        .map_err(|error| error.to_string())?;
    let reader = ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout)
        .map_err(|error| error.to_string())?;
    let drive_stats = drive_segment_write_to_consumer(&profile_config, &mut writer, reader)?;
    debug_assert_eq!(drive_stats.measured_produced, drive_stats.measured_consumed);
    let processed_batches_total = drive_stats.consumed;
    let steady_duration = Duration::from_secs(profile_config.duration_sec);
    let actual_average_rows_per_sec =
        drive_stats.measured_consumed as f64 / steady_duration.as_secs_f64();
    let engine_metrics = EngineMetricsReport {
        processed_batches_total,
        processed_rows_total: drive_stats.consumed,
        output_batches_total: processed_batches_total,
        dropped_batches_total: 0,
        late_rows_total: 0,
        publish_errors_total: 0,
        queue_depth: 0,
        max_observed_queue_depth: 0,
    };
    let latency = build_latency_summary(&drive_stats.latency_samples_micros);
    let pass = evaluate_pass(
        &profile_config,
        actual_average_rows_per_sec,
        EngineStatus::Stopped,
        &engine_metrics,
        None,
        &latency,
    );

    Ok(PerfReport {
        profile: profile_config.profile,
        config: profile_config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: drive_stats.produced,
        output_rows_total: drive_stats.consumed,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(
            drive_stats.measured_consumed,
            profile_config.duration_sec as f64,
        ),
        latency,
        secondary_latency: None,
        engine_status: EngineStatus::Stopped.as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn run_runtime_stream_table_e2e(config: &PerfConfig) -> PerfResult<PerfReport> {
    let profile_config = single_row_profile_config(config);
    let row_capacity = expected_single_row_capacity(&profile_config)?;
    let tick_schema = build_tick_schema();
    let emit_latencies = Arc::new(Mutex::new(Vec::new()));
    let source = RuntimeSegmentSource {
        config: profile_config.clone(),
        schema: Arc::clone(&tick_schema),
        row_capacity,
        emit_latencies: Arc::clone(&emit_latencies),
    };
    let publisher = RuntimeLatencyPublisher::default();
    let publisher_state = publisher.state();
    let engine = StreamTableMaterializer::new_with_row_capacity(
        "perf_runtime_stream_table",
        tick_schema,
        row_capacity,
    )
    .map_err(|error| error.to_string())?;
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        engine_config(&profile_config),
        publisher,
    )
    .map_err(|error| error.to_string())?;
    wait_for_runtime_profile_stop(&mut handle, &profile_config)?;

    let engine_metrics = engine_metrics_report(handle.metrics());
    let output_rows_total = publisher_state.output_rows_total.load(Ordering::Relaxed);
    let output_batches_total = publisher_state.output_batches_total.load(Ordering::Relaxed);
    let publish_latencies = publisher_state.publish_latencies_micros.lock().unwrap();
    let emit_latencies = emit_latencies.lock().unwrap();
    let steady_rows_total = publish_latencies.len() as u64;
    let steady_rows_per_sec = steady_rows_total as f64 / profile_config.duration_sec as f64;
    let latency = build_latency_summary(&publish_latencies);
    let secondary_latency = build_latency_summary(&emit_latencies);
    let pass = evaluate_pass(
        &profile_config,
        steady_rows_per_sec,
        handle.status(),
        &engine_metrics,
        None,
        &latency,
    );
    handle.stop().map_err(|error| error.to_string())?;

    Ok(PerfReport {
        profile: profile_config.profile,
        config: profile_config.clone(),
        metadata: PerfReportMetadata::capture(),
        input_rows_total: engine_metrics.processed_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: steady_rows_per_sec,
        actual_peak_rows_per_sec: steady_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(
            output_batches_total,
            profile_config.duration_sec as f64,
        ),
        latency,
        secondary_latency: Some(secondary_latency),
        engine_status: handle.status().as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn run_stream_table_subscriber_e2e(config: &PerfConfig) -> PerfResult<PerfReport> {
    let profile_config = single_row_profile_config(config);
    let row_capacity = expected_single_row_capacity(&profile_config)?;
    let tick_schema = build_tick_schema();
    let input_table = build_tick_segment_table(
        profile_config.rows_per_batch,
        profile_config.symbols,
        profile_config.target_rows_per_sec,
    )?;
    let mut materializer = StreamTableMaterializer::new_with_row_capacity(
        "perf_stream_table_subscriber",
        tick_schema,
        row_capacity,
    )
    .map_err(|error| error.to_string())?;
    let reader_schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .map_err(str::to_string)?;
    let reader_layout =
        LayoutPlan::for_schema(&reader_schema, row_capacity).map_err(str::to_string)?;
    let descriptor = materializer
        .active_descriptor_envelope_bytes()
        .map_err(|error| error.to_string())?;
    let mut reader =
        ActiveSegmentReader::from_descriptor_envelope(&descriptor, reader_schema, reader_layout)
            .map_err(|error| error.to_string())?;
    let drive_stats = drive_stream_table_to_reader(
        &profile_config,
        &input_table,
        &mut materializer,
        &mut reader,
    )?;
    materializer.on_flush().map_err(|error| error.to_string())?;

    let processed_batches_total = drive_stats.input_rows_total;
    let engine_metrics = EngineMetricsReport {
        processed_batches_total,
        processed_rows_total: drive_stats.input_rows_total,
        output_batches_total: processed_batches_total,
        dropped_batches_total: 0,
        late_rows_total: 0,
        publish_errors_total: 0,
        queue_depth: 0,
        max_observed_queue_depth: 0,
    };
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let secondary_latency = build_latency_summary(&drive_stats.secondary_latencies_micros);
    let pass = evaluate_pass(
        &profile_config,
        drive_stats.actual_average_rows_per_sec,
        EngineStatus::Stopped,
        &engine_metrics,
        None,
        &latency,
    );

    Ok(PerfReport {
        profile: profile_config.profile,
        config: profile_config,
        metadata: PerfReportMetadata::capture(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total: drive_stats.input_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(
            processed_batches_total,
            config.duration_sec as f64,
        ),
        latency,
        secondary_latency: Some(secondary_latency),
        engine_status: EngineStatus::Stopped.as_str().to_string(),
        engine_metrics,
        source_metrics: None,
        pass,
    })
}

fn wait_for_remote_shutdown(
    handle: &mut zippy_core::EngineHandle,
    config: &PerfConfig,
    source_counters: &Arc<SourceCountersState>,
) -> PerfResult<()> {
    let startup_deadline = Instant::now() + Duration::from_secs(REMOTE_STARTUP_GRACE_SEC);

    while handle.status() == EngineStatus::Running {
        let now = Instant::now();
        if let Some(first_observed_at) = source_counters.first_observed_at() {
            let deadline = compute_remote_downstream_deadline(
                first_observed_at,
                config.warmup_sec,
                config.duration_sec,
                REMOTE_SHUTDOWN_GRACE_SEC,
            );
            if remote_downstream_should_stop_for_idle(
                Some(first_observed_at),
                source_counters.last_event_at(),
                now,
            ) {
                handle.stop().map_err(|error| error.to_string())?;
                break;
            }
            if now >= deadline {
                handle.stop().map_err(|error| error.to_string())?;
                return Err("remote downstream did not stop before deadline".to_string());
            }
        } else if now >= startup_deadline {
            handle.stop().map_err(|error| error.to_string())?;
            return Err(
                "remote downstream did not observe source events before startup deadline"
                    .to_string(),
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    let shutdown_deadline = Instant::now() + Duration::from_secs(REMOTE_SHUTDOWN_GRACE_SEC);
    while handle.status() == EngineStatus::Running && Instant::now() < shutdown_deadline {
        thread::sleep(Duration::from_millis(20));
    }

    if handle.status() == EngineStatus::Running {
        return Err("remote downstream did not stop after shutdown request".to_string());
    }

    handle.stop().map_err(|error| error.to_string())
}

fn compute_remote_downstream_deadline(
    first_observed_at: Instant,
    warmup_sec: u64,
    duration_sec: u64,
    shutdown_grace_sec: u64,
) -> Instant {
    first_observed_at + Duration::from_secs(warmup_sec + duration_sec + shutdown_grace_sec)
}

fn remote_downstream_should_stop_for_idle(
    first_observed_at: Option<Instant>,
    last_event_at: Option<Instant>,
    now: Instant,
) -> bool {
    match (first_observed_at, last_event_at) {
        (Some(_), Some(last_event_at)) => {
            now.saturating_duration_since(last_event_at)
                >= Duration::from_secs(REMOTE_IDLE_GRACE_SEC)
        }
        _ => false,
    }
}

fn validate_config(config: &PerfConfig) -> PerfResult<()> {
    if config.rows_per_batch == 0 {
        return Err("rows_per_batch must be greater than zero".to_string());
    }
    if config.target_rows_per_sec == 0 {
        return Err("target_rows_per_sec must be greater than zero".to_string());
    }
    if config.duration_sec == 0 {
        return Err("duration_sec must be greater than zero".to_string());
    }
    if config.symbols == 0 {
        return Err("symbols must be greater than zero".to_string());
    }
    if config.buffer_capacity == 0 {
        return Err("buffer_capacity must be greater than zero".to_string());
    }
    if config.endpoint.is_empty() {
        return Err("endpoint must not be empty".to_string());
    }
    Ok(())
}

fn engine_config(config: &PerfConfig) -> EngineConfig {
    EngineConfig {
        name: format!("{:?}", config.profile),
        buffer_capacity: config.buffer_capacity,
        overflow_policy: match config.overflow_policy {
            OverflowPolicyConfig::Block => OverflowPolicy::Block,
            OverflowPolicyConfig::Reject => OverflowPolicy::Reject,
            OverflowPolicyConfig::DropOldest => OverflowPolicy::DropOldest,
        },
        late_data_policy: LateDataPolicy::Reject,
        xfast: false,
    }
}

fn build_tick_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]))
}

#[derive(Default)]
struct CountingDescriptorPublisher {
    published_total: AtomicU64,
}

impl StreamTableDescriptorPublisher for CountingDescriptorPublisher {
    fn publish(&self, _descriptor_envelope: Vec<u8>) -> Result<()> {
        self.published_total.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn build_tick_segment_table(
    rows_per_batch: usize,
    symbol_count: usize,
    target_rows_per_sec: u64,
) -> PerfResult<SegmentTableView> {
    let schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .map_err(str::to_string)?;
    let layout = LayoutPlan::for_schema(&schema, rows_per_batch).map_err(str::to_string)?;
    let mut writer =
        ActiveSegmentWriter::new_for_runtime(schema, layout, 9_001, 0).map_err(str::to_string)?;
    let step_ns = ((1_000_000_000_u64 / target_rows_per_sec).max(1)) as i64;

    for row_index in 0..rows_per_batch {
        writer.begin_row().map_err(str::to_string)?;
        writer
            .write_utf8("symbol", &format!("S{:04}", row_index % symbol_count))
            .map_err(str::to_string)?;
        writer
            .write_i64("dt", row_index as i64 * step_ns)
            .map_err(str::to_string)?;
        writer
            .write_f64("price", 10.0 + (row_index % 100) as f64 * 0.01)
            .map_err(str::to_string)?;
        writer
            .write_f64("volume", 100.0 + (row_index % 17) as f64)
            .map_err(str::to_string)?;
        writer.commit_row().map_err(str::to_string)?;
    }

    let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, rows_per_batch)
        .map_err(str::to_string)?;
    Ok(SegmentTableView::from_row_span(span))
}

fn build_bar_schema(tick_schema: &SchemaRef) -> Result<SchemaRef> {
    let engine = build_timeseries_engine("perf_schema_probe", Arc::clone(tick_schema))?;
    Ok(engine.output_schema())
}

fn build_timeseries_engine(name: &str, tick_schema: SchemaRef) -> Result<TimeSeriesEngine> {
    TimeSeriesEngine::new(
        name,
        tick_schema,
        "symbol",
        "dt",
        DEFAULT_WINDOW_NS,
        LateDataPolicy::Reject,
        vec![AggLastSpec::new("price", "close").build().unwrap()],
        vec![],
        vec![],
    )
}

fn build_cross_sectional_engine(name: &str, bar_schema: SchemaRef) -> Result<CrossSectionalEngine> {
    CrossSectionalEngine::new(
        name,
        bar_schema,
        "symbol",
        "window_start",
        DEFAULT_WINDOW_NS,
        LateDataPolicy::Reject,
        vec![
            CSRankSpec::new("close", "close_rank").build().unwrap(),
            CSZscoreSpec::new("close", "close_z").build().unwrap(),
            CSDemeanSpec::new("close", "close_dm").build().unwrap(),
        ],
    )
}

struct TickBatchFactory {
    schema: SchemaRef,
    rows_per_batch: usize,
    step_ns: i64,
    next_dt_ns: i64,
    symbols: ArrayRef,
    prices: ArrayRef,
    volumes: ArrayRef,
}

impl TickBatchFactory {
    fn new(
        schema: SchemaRef,
        rows_per_batch: usize,
        symbol_count: usize,
        target_rows_per_sec: u64,
    ) -> PerfResult<Self> {
        let symbols = (0..rows_per_batch)
            .map(|index| format!("S{:04}", index % symbol_count))
            .collect::<Vec<_>>();
        let prices = (0..rows_per_batch)
            .map(|index| 10.0 + (index % 100) as f64 * 0.01)
            .collect::<Vec<_>>();
        let volumes = (0..rows_per_batch)
            .map(|index| 100.0 + (index % 17) as f64)
            .collect::<Vec<_>>();
        let step_ns = ((1_000_000_000_u64 / target_rows_per_sec).max(1)) as i64;

        Ok(Self {
            schema,
            rows_per_batch,
            step_ns,
            next_dt_ns: 0,
            symbols: Arc::new(StringArray::from(symbols)) as ArrayRef,
            prices: Arc::new(Float64Array::from(prices)) as ArrayRef,
            volumes: Arc::new(Float64Array::from(volumes)) as ArrayRef,
        })
    }

    fn next_batch(&mut self) -> PerfResult<RecordBatch> {
        let timestamps = (0..self.rows_per_batch)
            .map(|offset| self.next_dt_ns + offset as i64 * self.step_ns)
            .collect::<Vec<_>>();
        self.next_dt_ns += self.rows_per_batch as i64 * self.step_ns;

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::clone(&self.symbols),
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC"))
                    as ArrayRef,
                Arc::clone(&self.prices),
                Arc::clone(&self.volumes),
            ],
        )
        .map_err(|error| error.to_string())
    }
}

#[derive(Default)]
struct PublisherCounters {
    output_batches_total: AtomicU64,
    output_rows_total: AtomicU64,
}

struct CountingPublisher<P> {
    inner: P,
    counters: Arc<PublisherCounters>,
}

impl<P> CountingPublisher<P> {
    fn new(inner: P) -> Self {
        Self {
            inner,
            counters: Arc::new(PublisherCounters::default()),
        }
    }

    fn counters(&self) -> Arc<PublisherCounters> {
        Arc::clone(&self.counters)
    }
}

impl<P> Publisher for CountingPublisher<P>
where
    P: Publisher,
{
    fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
        self.counters
            .output_batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .output_rows_total
            .fetch_add(table.num_rows() as u64, Ordering::Relaxed);
        self.inner.publish_table(table)
    }

    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        self.publish_table(&SegmentTableView::from_record_batch(batch.clone()))
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    fn close(&mut self) -> Result<()> {
        self.inner.close()
    }
}

#[derive(Default)]
struct SourceCountersState {
    source_events_total: AtomicU64,
    source_data_batches_total: AtomicU64,
    source_control_events_total: AtomicU64,
    source_decode_errors_total: AtomicU64,
    source_schema_mismatches_total: AtomicU64,
    source_sequence_gaps_total: AtomicU64,
    first_observed_at: Mutex<Option<Instant>>,
    first_data_at: Mutex<Option<Instant>>,
    last_event_at: Mutex<Option<Instant>>,
}

impl SourceCountersState {
    fn first_observed_at(&self) -> Option<Instant> {
        *self.first_observed_at.lock().unwrap()
    }

    fn last_event_at(&self) -> Option<Instant> {
        *self.last_event_at.lock().unwrap()
    }

    fn observed_seconds(&self) -> Option<f64> {
        let first = *self.first_data_at.lock().unwrap();
        let last = *self.last_event_at.lock().unwrap();
        match (first, last) {
            (Some(first), Some(last)) if last >= first => Some((last - first).as_secs_f64()),
            _ => None,
        }
    }
}

struct CountingSource<S> {
    inner: S,
    counters: Arc<SourceCountersState>,
}

impl<S> CountingSource<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            counters: Arc::new(SourceCountersState::default()),
        }
    }

    fn counters(&self) -> Arc<SourceCountersState> {
        Arc::clone(&self.counters)
    }
}

struct CountingSourceSink {
    inner: Arc<dyn SourceSink>,
    counters: Arc<SourceCountersState>,
}

impl SourceSink for CountingSourceSink {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        let now = Instant::now();
        self.counters
            .source_events_total
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut first_observed_at = self.counters.first_observed_at.lock().unwrap();
            if first_observed_at.is_none() {
                *first_observed_at = Some(now);
            }
        }
        *self.counters.last_event_at.lock().unwrap() = Some(now);
        match &event {
            SourceEvent::Data(_) => {
                self.counters
                    .source_data_batches_total
                    .fetch_add(1, Ordering::Relaxed);
                let mut first_data_at = self.counters.first_data_at.lock().unwrap();
                if first_data_at.is_none() {
                    *first_data_at = Some(now);
                }
            }
            SourceEvent::Hello(_)
            | SourceEvent::Flush
            | SourceEvent::Stop
            | SourceEvent::Error(_) => {
                self.counters
                    .source_control_events_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        self.inner.emit(event)
    }
}

impl<S> Source for CountingSource<S>
where
    S: Source,
{
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn output_schema(&self) -> SchemaRef {
        self.inner.output_schema()
    }

    fn mode(&self) -> SourceMode {
        self.inner.mode()
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let Self { inner, counters } = *self;
        let counting_sink = Arc::new(CountingSourceSink {
            inner: sink,
            counters: Arc::clone(&counters),
        });
        let inner_handle = Box::new(inner).start(counting_sink)?;
        let stop_handle = inner_handle.clone();
        let join_handle = thread::spawn(move || {
            let result = inner_handle.join();
            if let Err(error) = &result {
                classify_source_error(error, &counters);
            }
            result
        });

        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || stop_handle.stop()),
        ))
    }
}

#[derive(Default)]
struct RuntimeLatencyPublisherState {
    output_batches_total: AtomicU64,
    output_rows_total: AtomicU64,
    publish_latencies_micros: Mutex<Vec<u64>>,
}

#[derive(Default)]
struct RuntimeLatencyPublisher {
    state: Arc<RuntimeLatencyPublisherState>,
}

impl RuntimeLatencyPublisher {
    fn state(&self) -> Arc<RuntimeLatencyPublisherState> {
        Arc::clone(&self.state)
    }
}

impl Publisher for RuntimeLatencyPublisher {
    fn publish_table(&mut self, table: &SegmentTableView) -> Result<()> {
        let observed_ns = current_unix_epoch_ns();
        if let Some(sent_ns) = first_dt_ns(table)? {
            if sent_ns > 0 {
                self.state
                    .publish_latencies_micros
                    .lock()
                    .unwrap()
                    .push(observed_ns.saturating_sub(sent_ns) as u64 / 1_000);
            }
        }
        self.state
            .output_batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .output_rows_total
            .fetch_add(table.num_rows() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        self.publish_table(&SegmentTableView::from_record_batch(batch.clone()))
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

struct RuntimeSegmentSource {
    config: PerfConfig,
    schema: SchemaRef,
    row_capacity: usize,
    emit_latencies: Arc<Mutex<Vec<u64>>>,
}

impl Source for RuntimeSegmentSource {
    fn name(&self) -> &str {
        "perf_runtime_segment_source"
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn mode(&self) -> SourceMode {
        SourceMode::Pipeline
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let Self {
            config,
            schema,
            row_capacity,
            emit_latencies,
        } = *self;
        let join_handle = thread::spawn(move || -> Result<()> {
            sink.emit(SourceEvent::Hello(StreamHello::new(
                "perf_runtime_stream",
                schema,
                1,
            )?))?;
            run_runtime_segment_source(config, row_capacity, emit_latencies, sink)?;
            Ok(())
        });
        Ok(SourceHandle::new(join_handle))
    }
}

struct DriveStats {
    input_rows_total: u64,
    actual_average_rows_per_sec: f64,
    actual_peak_rows_per_sec: f64,
    batch_latencies_micros: Vec<u64>,
    secondary_latencies_micros: Vec<u64>,
}

struct InprocDriveStats {
    input_rows_total: u64,
    steady_rows_total: u64,
    steady_batches_total: u64,
    steady_started_at: Instant,
    actual_peak_rows_per_sec: f64,
    batch_latencies_micros: Vec<u64>,
    max_observed_queue_depth: usize,
}

fn drive_inproc_generator(
    config: &PerfConfig,
    factory: &mut TickBatchFactory,
    handle: &mut zippy_core::EngineHandle,
) -> PerfResult<InprocDriveStats> {
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration + steady_duration;
    let batch_interval_ns = ((1_000_000_000_u128 * config.rows_per_batch as u128)
        / config.target_rows_per_sec as u128)
        .max(1) as u64;
    let batch_interval = Duration::from_nanos(batch_interval_ns);
    let start = Instant::now();
    let steady_started_at = start + warmup_duration;
    let end = steady_started_at + steady_duration;
    let mut next_tick = start;
    let mut input_rows_total = 0_u64;
    let mut steady_rows_total = 0_u64;
    let mut steady_batches_total = 0_u64;
    let mut peak_rows_per_sec = 0_f64;
    let mut batch_latencies_micros = Vec::new();
    let mut second_window_start = steady_started_at;
    let mut second_window_rows = 0_u64;
    let mut max_observed_queue_depth = handle.metrics().queue_depth;

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let batch = factory.next_batch()?;
        let batch_start = Instant::now();
        handle.write(batch).map_err(|error| error.to_string())?;
        let write_completed_at = Instant::now();
        max_observed_queue_depth = max_observed_queue_depth.max(handle.metrics().queue_depth);
        let write_elapsed = write_completed_at.duration_since(batch_start);
        input_rows_total += config.rows_per_batch as u64;
        if batch_start >= steady_started_at {
            steady_rows_total += config.rows_per_batch as u64;
            steady_batches_total += 1;
            batch_latencies_micros.push(write_elapsed.as_micros() as u64);
            second_window_rows += config.rows_per_batch as u64;
            while batch_start >= second_window_start + Duration::from_secs(1) {
                peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
                second_window_rows = 0;
                second_window_start += Duration::from_secs(1);
            }
        }
        next_tick += batch_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);

    Ok(InprocDriveStats {
        input_rows_total,
        steady_rows_total,
        steady_batches_total,
        steady_started_at,
        actual_peak_rows_per_sec: peak_rows_per_sec,
        batch_latencies_micros,
        max_observed_queue_depth,
    })
}

fn drive_generator<F>(
    config: &PerfConfig,
    factory: &mut TickBatchFactory,
    mut send_batch: F,
) -> PerfResult<DriveStats>
where
    F: FnMut(RecordBatch) -> PerfResult<()>,
{
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration + steady_duration;
    let batch_interval_ns = ((1_000_000_000_u128 * config.rows_per_batch as u128)
        / config.target_rows_per_sec as u128)
        .max(1) as u64;
    let batch_interval = Duration::from_nanos(batch_interval_ns);
    let start = Instant::now();
    let warmup_end = start + warmup_duration;
    let end = warmup_end + steady_duration;
    let mut next_tick = start;
    let mut input_rows_total = 0_u64;
    let mut steady_rows_total = 0_u64;
    let mut steady_batches_total = 0_u64;
    let mut peak_rows_per_sec = 0_f64;
    let mut batch_latencies_micros = Vec::new();
    let mut second_window_start = warmup_end;
    let mut second_window_rows = 0_u64;

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let batch = factory.next_batch()?;
        let batch_start = Instant::now();
        send_batch(batch)?;
        let send_elapsed = batch_start.elapsed();
        input_rows_total += config.rows_per_batch as u64;
        if batch_start >= warmup_end {
            steady_rows_total += config.rows_per_batch as u64;
            steady_batches_total += 1;
            batch_latencies_micros.push(send_elapsed.as_micros() as u64);
            second_window_rows += config.rows_per_batch as u64;
            while batch_start >= second_window_start + Duration::from_secs(1) {
                peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
                second_window_rows = 0;
                second_window_start += Duration::from_secs(1);
            }
        }
        next_tick += batch_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
    let actual_average_rows_per_sec = steady_rows_total as f64 / steady_duration.as_secs_f64();
    let _ = steady_batches_total;

    Ok(DriveStats {
        input_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: peak_rows_per_sec,
        batch_latencies_micros,
        secondary_latencies_micros: Vec::new(),
    })
}

fn drive_segment_view<F>(
    config: &PerfConfig,
    table: &SegmentTableView,
    mut send_table: F,
) -> PerfResult<DriveStats>
where
    F: FnMut(SegmentTableView) -> PerfResult<()>,
{
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration + steady_duration;
    let batch_interval_ns = ((1_000_000_000_u128 * config.rows_per_batch as u128)
        / config.target_rows_per_sec as u128)
        .max(1) as u64;
    let batch_interval = Duration::from_nanos(batch_interval_ns);
    let start = Instant::now();
    let warmup_end = start + warmup_duration;
    let end = warmup_end + steady_duration;
    let mut next_tick = start;
    let mut input_rows_total = 0_u64;
    let mut steady_rows_total = 0_u64;
    let mut peak_rows_per_sec = 0_f64;
    let mut batch_latencies_micros = Vec::new();
    let mut second_window_start = warmup_end;
    let mut second_window_rows = 0_u64;

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let batch_start = Instant::now();
        send_table(table.clone())?;
        let send_elapsed = batch_start.elapsed();
        input_rows_total += config.rows_per_batch as u64;
        if batch_start >= warmup_end {
            steady_rows_total += config.rows_per_batch as u64;
            batch_latencies_micros.push(send_elapsed.as_micros() as u64);
            second_window_rows += config.rows_per_batch as u64;
            while batch_start >= second_window_start + Duration::from_secs(1) {
                peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
                second_window_rows = 0;
                second_window_start += Duration::from_secs(1);
            }
        }
        next_tick += batch_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
    let actual_average_rows_per_sec = steady_rows_total as f64 / steady_duration.as_secs_f64();

    Ok(DriveStats {
        input_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: peak_rows_per_sec,
        batch_latencies_micros,
        secondary_latencies_micros: Vec::new(),
    })
}

#[derive(Default)]
struct ProducerState {
    produced: AtomicU64,
    measured_produced: AtomicU64,
    done: AtomicBool,
    cancelled: AtomicBool,
}

struct ProducerStats {
    produced: u64,
    measured_produced: u64,
    actual_peak_rows_per_sec: f64,
}

enum ProducerOutcome {
    Completed(ProducerStats),
    CancelledByConsumer,
}

#[derive(Debug, Default)]
struct ConsumerStats {
    consumed: u64,
    measured_consumed: u64,
    latency_samples_micros: Vec<u64>,
}

struct WriteToSubscribeStats {
    produced: u64,
    consumed: u64,
    measured_produced: u64,
    measured_consumed: u64,
    actual_peak_rows_per_sec: f64,
    latency_samples_micros: Vec<u64>,
}

#[derive(Clone, Copy)]
struct WriteToSubscribeTiming {
    origin: Instant,
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
    consumer_deadline: Instant,
}

struct SteadyPeakTracker {
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
    current_window_index: Option<u128>,
    current_window_rows: u64,
    peak_rows: u64,
}

impl SteadyPeakTracker {
    fn new(warmup_end_elapsed_ns: u128, steady_end_elapsed_ns: u128) -> Self {
        Self {
            warmup_end_elapsed_ns,
            steady_end_elapsed_ns,
            current_window_index: None,
            current_window_rows: 0,
            peak_rows: 0,
        }
    }

    fn record(&mut self, sent_elapsed_ns: u128) -> PerfResult<()> {
        const SECOND_NS: u128 = 1_000_000_000;

        if !is_write_to_subscribe_measured(
            sent_elapsed_ns,
            self.warmup_end_elapsed_ns,
            self.steady_end_elapsed_ns,
        ) {
            return Ok(());
        }
        let window_index = (sent_elapsed_ns - self.warmup_end_elapsed_ns) / SECOND_NS;
        match self.current_window_index {
            Some(current_window_index) if current_window_index == window_index => {
                self.current_window_rows =
                    self.current_window_rows.checked_add(1).ok_or_else(|| {
                        "write-to-subscribe peak window row count overflowed".to_string()
                    })?;
            }
            Some(current_window_index) if current_window_index < window_index => {
                self.peak_rows = self.peak_rows.max(self.current_window_rows);
                self.current_window_index = Some(window_index);
                self.current_window_rows = 1;
            }
            Some(_) => {
                return Err(
                    "write-to-subscribe peak tracker received out-of-order timestamp".to_string(),
                );
            }
            None => {
                self.current_window_index = Some(window_index);
                self.current_window_rows = 1;
            }
        }
        Ok(())
    }

    fn finish(self) -> u64 {
        self.peak_rows.max(self.current_window_rows)
    }
}

struct ConsumerJoinGuard {
    state: Arc<ProducerState>,
    handle: Option<thread::JoinHandle<PerfResult<ConsumerStats>>>,
}

impl ConsumerJoinGuard {
    fn new(
        state: Arc<ProducerState>,
        handle: thread::JoinHandle<PerfResult<ConsumerStats>>,
    ) -> Self {
        Self {
            state,
            handle: Some(handle),
        }
    }

    fn join(&mut self) -> PerfResult<ConsumerStats> {
        let handle = self
            .handle
            .take()
            .ok_or_else(|| "write-to-subscribe consumer was already joined".to_string())?;
        match handle.join() {
            Ok(result) => result,
            Err(payload) => {
                self.state.cancelled.store(true, Ordering::Release);
                self.state.done.store(true, Ordering::Release);
                Err(format!(
                    "write-to-subscribe consumer panicked: {}",
                    panic_payload_message(payload.as_ref())
                ))
            }
        }
    }
}

impl Drop for ConsumerJoinGuard {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.state.cancelled.store(true, Ordering::Release);
        self.state.done.store(true, Ordering::Release);
        let _ = handle.join();
    }
}

fn drive_segment_write_to_consumer(
    config: &PerfConfig,
    writer: &mut ActiveSegmentWriter,
    reader: ActiveSegmentReader,
) -> PerfResult<WriteToSubscribeStats> {
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let warmup_end_elapsed_ns = warmup_duration.as_nanos();
    let steady_end_elapsed_ns = (warmup_duration + steady_duration).as_nanos();
    let observer = reader.clone();
    let state = Arc::new(ProducerState::default());
    let timing = Arc::new(OnceLock::new());
    let consumer_state = Arc::clone(&state);
    let consumer_timing = Arc::clone(&timing);
    let consumer_handle = match thread::Builder::new()
        .name("zippy-perf-write-to-subscribe-consumer".to_string())
        .spawn(move || {
            match panic::catch_unwind(AssertUnwindSafe(|| {
                consume_write_to_subscribe_rows(
                    reader,
                    consumer_timing,
                    Arc::clone(&consumer_state),
                )
            })) {
                Ok(result) => result,
                Err(payload) => {
                    consumer_state.cancelled.store(true, Ordering::Release);
                    Err(format!(
                        "write-to-subscribe consumer panicked: {}",
                        panic_payload_message(payload.as_ref())
                    ))
                }
            }
        }) {
        Ok(handle) => handle,
        Err(error) => {
            state.cancelled.store(true, Ordering::Release);
            state.done.store(true, Ordering::Release);
            return Err(format!(
                "failed to spawn write-to-subscribe consumer: {error}"
            ));
        }
    };
    let mut consumer = ConsumerJoinGuard::new(Arc::clone(&state), consumer_handle);

    if let Err(startup_error) = wait_for_write_to_subscribe_consumer_waiter(
        &observer,
        &state,
        Instant::now() + WRITE_TO_SUBSCRIBE_STARTUP_TIMEOUT,
    ) {
        state.cancelled.store(true, Ordering::Release);
        state.done.store(true, Ordering::Release);
        return Err(merge_write_to_subscribe_startup_error(
            startup_error,
            consumer.join(),
        ));
    }

    let origin = Instant::now();
    let producer_end = origin + warmup_duration + steady_duration;
    let write_timing = WriteToSubscribeTiming {
        origin,
        warmup_end_elapsed_ns,
        steady_end_elapsed_ns,
        consumer_deadline: producer_end + Duration::from_secs(5),
    };
    if timing.set(write_timing).is_err() {
        let startup_error = "write-to-subscribe timing was already initialized".to_string();
        state.cancelled.store(true, Ordering::Release);
        state.done.store(true, Ordering::Release);
        return Err(merge_write_to_subscribe_startup_error(
            startup_error,
            consumer.join(),
        ));
    }

    let producer_result = match panic::catch_unwind(AssertUnwindSafe(|| {
        produce_write_to_subscribe_rows(
            config,
            writer,
            origin,
            producer_end,
            warmup_end_elapsed_ns,
            steady_end_elapsed_ns,
            Arc::clone(&state),
        )
    })) {
        Ok(result) => result,
        Err(payload) => {
            state.cancelled.store(true, Ordering::Release);
            state.done.store(true, Ordering::Release);
            Err(format!(
                "write-to-subscribe producer panicked: {}",
                panic_payload_message(payload.as_ref())
            ))
        }
    };
    let consumer_result = consumer.join();

    let (producer, consumer) = merge_write_to_subscribe_results(producer_result, consumer_result)?;
    let latency_sample_count = u64::try_from(consumer.latency_samples_micros.len())
        .map_err(|_| "write-to-subscribe latency sample count overflowed".to_string())?;
    validate_write_to_subscribe_counts(
        producer.produced,
        consumer.consumed,
        producer.measured_produced,
        consumer.measured_consumed,
        latency_sample_count,
    )?;
    Ok(WriteToSubscribeStats {
        produced: producer.produced,
        consumed: consumer.consumed,
        measured_produced: producer.measured_produced,
        measured_consumed: consumer.measured_consumed,
        actual_peak_rows_per_sec: producer.actual_peak_rows_per_sec,
        latency_samples_micros: consumer.latency_samples_micros,
    })
}

fn merge_write_to_subscribe_results(
    producer_result: PerfResult<ProducerOutcome>,
    consumer_result: PerfResult<ConsumerStats>,
) -> PerfResult<(ProducerStats, ConsumerStats)> {
    match (producer_result, consumer_result) {
        (Err(producer_error), Err(consumer_error)) => Err(format!(
            "write-to-subscribe consumer error: {consumer_error}; producer error: {producer_error}"
        )),
        (Err(producer_error), Ok(_)) => Err(producer_error),
        (Ok(ProducerOutcome::CancelledByConsumer), Err(consumer_error)) => Err(consumer_error),
        (Ok(ProducerOutcome::CancelledByConsumer), Ok(_)) => {
            Err("write-to-subscribe producer was cancelled without a consumer error".to_string())
        }
        (Ok(ProducerOutcome::Completed(_)), Err(consumer_error)) => Err(consumer_error),
        (Ok(ProducerOutcome::Completed(producer)), Ok(consumer)) => Ok((producer, consumer)),
    }
}

fn merge_write_to_subscribe_startup_error(
    startup_error: String,
    consumer_result: PerfResult<ConsumerStats>,
) -> String {
    match consumer_result {
        Ok(_) => startup_error,
        Err(consumer_error) => {
            format!("{startup_error}; write-to-subscribe consumer cleanup error: {consumer_error}")
        }
    }
}

fn wait_for_write_to_subscribe_consumer_waiter(
    observer: &ActiveSegmentReader,
    state: &ProducerState,
    deadline: Instant,
) -> PerfResult<()> {
    loop {
        if state.cancelled.load(Ordering::Acquire) {
            return Err(
                "write-to-subscribe consumer stopped before entering notification wait".to_string(),
            );
        }
        let snapshot = observer
            .control_snapshot()
            .map_err(|error| error.to_string())?;
        if snapshot.waiter_count > 0 {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(
                "write-to-subscribe consumer did not enter notification wait within 5s".to_string(),
            );
        }
        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(WRITE_TO_SUBSCRIBE_STARTUP_POLL_INTERVAL));
    }
}

fn produce_write_to_subscribe_rows(
    config: &PerfConfig,
    writer: &mut ActiveSegmentWriter,
    origin: Instant,
    end: Instant,
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
    state: Arc<ProducerState>,
) -> PerfResult<ProducerOutcome> {
    let result = produce_write_to_subscribe_rows_inner(
        config,
        writer,
        origin,
        end,
        warmup_end_elapsed_ns,
        steady_end_elapsed_ns,
        &state,
    );
    if result.is_err() {
        state.cancelled.store(true, Ordering::Release);
    }
    state.done.store(true, Ordering::Release);
    result
}

fn produce_write_to_subscribe_rows_inner(
    config: &PerfConfig,
    writer: &mut ActiveSegmentWriter,
    origin: Instant,
    end: Instant,
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
    state: &ProducerState,
) -> PerfResult<ProducerOutcome> {
    let mut row_index = 0_u64;
    let mut measured_produced = 0_u64;
    let mut peak_tracker = SteadyPeakTracker::new(warmup_end_elapsed_ns, steady_end_elapsed_ns);

    loop {
        if state.cancelled.load(Ordering::Acquire) {
            return Ok(ProducerOutcome::CancelledByConsumer);
        }
        let deadline_offset =
            write_to_subscribe_deadline_offset(row_index, config.target_rows_per_sec)?;
        if deadline_offset.as_nanos() >= steady_end_elapsed_ns {
            break;
        }
        let next_deadline = origin
            .checked_add(deadline_offset)
            .ok_or_else(|| "write-to-subscribe producer deadline overflowed Instant".to_string())?;
        let now = Instant::now();
        if now >= end {
            break;
        }
        if next_deadline > now {
            if state.cancelled.load(Ordering::Acquire) {
                return Ok(ProducerOutcome::CancelledByConsumer);
            }
            thread::sleep(next_deadline - now);
            if state.cancelled.load(Ordering::Acquire) {
                return Ok(ProducerOutcome::CancelledByConsumer);
            }
        }
        if Instant::now() >= end {
            break;
        }
        if state.cancelled.load(Ordering::Acquire) {
            return Ok(ProducerOutcome::CancelledByConsumer);
        }

        let row_offset = usize::try_from(row_index)
            .map_err(|_| "write-to-subscribe row index overflowed usize".to_string())?;
        let sent_elapsed_ns_u128 = origin.elapsed().as_nanos();
        let sent_elapsed_ns = checked_write_to_subscribe_elapsed_ns(sent_elapsed_ns_u128)?;
        writer.begin_row().map_err(str::to_string)?;
        writer
            .write_utf8("symbol", &format!("S{:04}", row_offset % config.symbols))
            .map_err(str::to_string)?;
        writer
            .write_f64("price", 10.0 + (row_offset % 100) as f64 * 0.01)
            .map_err(str::to_string)?;
        writer
            .write_f64("volume", 100.0 + (row_offset % 17) as f64)
            .map_err(str::to_string)?;
        writer
            .write_i64("sent_elapsed_ns", sent_elapsed_ns)
            .map_err(str::to_string)?;
        writer.commit_row().map_err(str::to_string)?;

        row_index = row_index
            .checked_add(1)
            .ok_or_else(|| "write-to-subscribe produced row count overflowed".to_string())?;
        state.produced.fetch_add(1, Ordering::Release);
        if is_write_to_subscribe_measured(
            sent_elapsed_ns_u128,
            warmup_end_elapsed_ns,
            steady_end_elapsed_ns,
        ) {
            measured_produced = measured_produced.checked_add(1).ok_or_else(|| {
                "write-to-subscribe measured produced row count overflowed".to_string()
            })?;
            state.measured_produced.fetch_add(1, Ordering::Release);
        }
        peak_tracker.record(sent_elapsed_ns_u128)?;
    }

    Ok(ProducerOutcome::Completed(ProducerStats {
        produced: row_index,
        measured_produced,
        actual_peak_rows_per_sec: peak_tracker.finish() as f64,
    }))
}

fn consume_write_to_subscribe_rows(
    mut reader: ActiveSegmentReader,
    timing: Arc<OnceLock<WriteToSubscribeTiming>>,
    state: Arc<ProducerState>,
) -> PerfResult<ConsumerStats> {
    let result = consume_write_to_subscribe_rows_after_startup(&mut reader, &timing, &state);
    if result.is_err() {
        state.cancelled.store(true, Ordering::Release);
    }
    result
}

fn consume_write_to_subscribe_rows_after_startup(
    reader: &mut ActiveSegmentReader,
    timing: &OnceLock<WriteToSubscribeTiming>,
    state: &ProducerState,
) -> PerfResult<ConsumerStats> {
    let startup_deadline = Instant::now() + WRITE_TO_SUBSCRIBE_STARTUP_TIMEOUT;
    loop {
        if let Some(timing) = timing.get().copied() {
            return consume_write_to_subscribe_rows_inner(reader, timing, state);
        }
        if write_to_subscribe_consumer_complete(state, 0) {
            return Ok(ConsumerStats::default());
        }

        let observed = reader
            .notification_sequence()
            .map_err(|error| error.to_string())?;
        let now = Instant::now();
        if now >= startup_deadline {
            return Err(
                "write-to-subscribe consumer did not receive timing before startup deadline"
                    .to_string(),
            );
        }
        let remaining = startup_deadline.saturating_duration_since(now);
        reader
            .wait_for_notification_after(observed, remaining.min(WRITE_TO_SUBSCRIBE_WAIT_SLICE))
            .map_err(|error| error.to_string())?;
    }
}

fn consume_write_to_subscribe_rows_inner(
    reader: &mut ActiveSegmentReader,
    timing: WriteToSubscribeTiming,
    state: &ProducerState,
) -> PerfResult<ConsumerStats> {
    let mut consumed = 0_u64;
    let mut measured_consumed = 0_u64;
    let mut latency_samples_micros = Vec::new();

    loop {
        let observed = reader
            .notification_sequence()
            .map_err(|error| error.to_string())?;
        if let Some(span) = reader.read_available().map_err(|error| error.to_string())? {
            let span_observed_elapsed_ns = timing.origin.elapsed().as_nanos();
            consume_write_to_subscribe_span(
                &span,
                span_observed_elapsed_ns,
                timing.warmup_end_elapsed_ns,
                timing.steady_end_elapsed_ns,
                &mut consumed,
                &mut measured_consumed,
                &mut latency_samples_micros,
            )?;
            continue;
        }
        if write_to_subscribe_consumer_complete(state, consumed) {
            return Ok(ConsumerStats {
                consumed,
                measured_consumed,
                latency_samples_micros,
            });
        }

        let now = Instant::now();
        if now >= timing.consumer_deadline {
            return Err(format!(
                "write-to-subscribe consumer timed out produced=[{}] consumed=[{}] measured_produced=[{}] measured_consumed=[{}]",
                state.produced.load(Ordering::Acquire),
                consumed,
                state.measured_produced.load(Ordering::Acquire),
                measured_consumed,
            ));
        }
        let remaining = timing.consumer_deadline.saturating_duration_since(now);
        reader
            .wait_for_notification_after(observed, remaining.min(WRITE_TO_SUBSCRIBE_WAIT_SLICE))
            .map_err(|error| error.to_string())?;
    }
}

fn consume_write_to_subscribe_span(
    span: &RowSpanView,
    span_observed_elapsed_ns: u128,
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
    consumed: &mut u64,
    measured_consumed: &mut u64,
    latency_samples_micros: &mut Vec<u64>,
) -> PerfResult<()> {
    for row_offset in 0..span.row_count() {
        let sent_elapsed_ns = match span
            .cell_value(row_offset, "sent_elapsed_ns")
            .map_err(|error| error.to_string())?
        {
            SegmentCellValue::Int64(value) if value >= 0 => value as u128,
            SegmentCellValue::Int64(value) => {
                return Err(format!(
                    "write-to-subscribe sent_elapsed_ns is negative value=[{value}]"
                ));
            }
            value => {
                return Err(format!(
                    "write-to-subscribe sent_elapsed_ns must be Int64 value=[{value:?}]"
                ));
            }
        };
        *consumed = consumed
            .checked_add(1)
            .ok_or_else(|| "write-to-subscribe consumed row count overflowed".to_string())?;
        if is_write_to_subscribe_measured(
            sent_elapsed_ns,
            warmup_end_elapsed_ns,
            steady_end_elapsed_ns,
        ) {
            let latency_ns = span_observed_elapsed_ns
                .checked_sub(sent_elapsed_ns)
                .ok_or_else(|| {
                    format!(
                        "write-to-subscribe observed row before it was sent observed_elapsed_ns=[{span_observed_elapsed_ns}] sent_elapsed_ns=[{sent_elapsed_ns}]"
                    )
                })?;
            let latency_micros = u64::try_from(latency_ns / 1_000)
                .map_err(|_| "write-to-subscribe latency sample overflowed u64".to_string())?;
            latency_samples_micros.push(latency_micros);
            *measured_consumed = measured_consumed.checked_add(1).ok_or_else(|| {
                "write-to-subscribe measured consumed row count overflowed".to_string()
            })?;
        }
    }
    Ok(())
}

fn validate_write_to_subscribe_counts(
    produced: u64,
    consumed: u64,
    measured_produced: u64,
    measured_consumed: u64,
    latency_sample_count: u64,
) -> PerfResult<()> {
    if produced != consumed {
        return Err(format!(
            "write-to-subscribe row count mismatch produced=[{produced}] consumed=[{consumed}]"
        ));
    }
    if measured_produced != measured_consumed {
        return Err(format!(
            "write-to-subscribe measured row count mismatch measured_produced=[{measured_produced}] measured_consumed=[{measured_consumed}]"
        ));
    }
    if measured_consumed != latency_sample_count {
        return Err(format!(
            "write-to-subscribe latency sample count mismatch measured_consumed=[{measured_consumed}] latency_samples=[{latency_sample_count}]"
        ));
    }
    Ok(())
}

fn is_write_to_subscribe_measured(
    sent_elapsed_ns: u128,
    warmup_end_elapsed_ns: u128,
    steady_end_elapsed_ns: u128,
) -> bool {
    sent_elapsed_ns >= warmup_end_elapsed_ns && sent_elapsed_ns < steady_end_elapsed_ns
}

fn checked_write_to_subscribe_elapsed_ns(elapsed_ns: u128) -> PerfResult<i64> {
    i64::try_from(elapsed_ns)
        .map_err(|_| "write-to-subscribe sent_elapsed_ns overflowed i64".to_string())
}

fn write_to_subscribe_deadline_offset(
    row_index: u64,
    target_rows_per_sec: u64,
) -> PerfResult<Duration> {
    if target_rows_per_sec == 0 {
        return Err("write-to-subscribe target_rows_per_sec must be positive".to_string());
    }
    let offset_ns = u128::from(row_index)
        .checked_mul(1_000_000_000)
        .ok_or_else(|| "write-to-subscribe producer deadline offset overflowed".to_string())?
        / u128::from(target_rows_per_sec);
    let offset_ns = u64::try_from(offset_ns)
        .map_err(|_| "write-to-subscribe producer deadline offset overflowed u64".to_string())?;
    Ok(Duration::from_nanos(offset_ns))
}

fn write_to_subscribe_consumer_complete(state: &ProducerState, consumed: u64) -> bool {
    state.done.load(Ordering::Acquire) && consumed >= state.produced.load(Ordering::Acquire)
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    }
}

fn drive_stream_table_to_reader(
    config: &PerfConfig,
    table: &SegmentTableView,
    materializer: &mut StreamTableMaterializer,
    reader: &mut ActiveSegmentReader,
) -> PerfResult<DriveStats> {
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration + steady_duration;
    let row_interval_ns = (1_000_000_000_u128 / config.target_rows_per_sec as u128).max(1) as u64;
    let row_interval = Duration::from_nanos(row_interval_ns);
    let start = Instant::now();
    let warmup_end = start + warmup_duration;
    let end = warmup_end + steady_duration;
    let mut next_tick = start;
    let mut input_rows_total = 0_u64;
    let mut steady_rows_total = 0_u64;
    let mut peak_rows_per_sec = 0_f64;
    let mut batch_latencies_micros = Vec::new();
    let mut secondary_latencies_micros = Vec::new();
    let mut second_window_start = warmup_end;
    let mut second_window_rows = 0_u64;

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let sample_start = Instant::now();
        let on_data_start = Instant::now();
        materializer
            .on_data(table.clone())
            .map_err(|error| error.to_string())?;
        let on_data_elapsed = on_data_start.elapsed();
        let span = reader
            .read_available()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "stream table subscriber did not observe committed row".to_string())?;
        let full_elapsed = sample_start.elapsed();
        let observed_rows = span.row_count() as u64;
        input_rows_total += observed_rows;
        if sample_start >= warmup_end {
            steady_rows_total += observed_rows;
            batch_latencies_micros.push(full_elapsed.as_micros() as u64);
            secondary_latencies_micros.push(on_data_elapsed.as_micros() as u64);
            second_window_rows += observed_rows;
            while sample_start >= second_window_start + Duration::from_secs(1) {
                peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
                second_window_rows = 0;
                second_window_start += Duration::from_secs(1);
            }
        }
        next_tick += row_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    peak_rows_per_sec = peak_rows_per_sec.max(second_window_rows as f64);
    let actual_average_rows_per_sec = steady_rows_total as f64 / steady_duration.as_secs_f64();

    Ok(DriveStats {
        input_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: peak_rows_per_sec,
        batch_latencies_micros,
        secondary_latencies_micros,
    })
}

fn run_runtime_segment_source(
    config: PerfConfig,
    row_capacity: usize,
    emit_latencies: Arc<Mutex<Vec<u64>>>,
    sink: Arc<dyn SourceSink>,
) -> Result<()> {
    let schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .map_err(|reason| zippy_core::ZippyError::Io {
        reason: reason.to_string(),
    })?;
    let layout = LayoutPlan::for_schema(&schema, row_capacity).map_err(|reason| {
        zippy_core::ZippyError::Io {
            reason: reason.to_string(),
        }
    })?;
    let mut writer =
        ActiveSegmentWriter::new_for_runtime(schema, layout, 20_001, 0).map_err(|reason| {
            zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            }
        })?;
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration + steady_duration;
    let row_interval_ns = (1_000_000_000_u128 / config.target_rows_per_sec as u128).max(1) as u64;
    let row_interval = Duration::from_nanos(row_interval_ns);
    let start = Instant::now();
    let warmup_end = start + warmup_duration;
    let end = warmup_end + steady_duration;
    let mut next_tick = start;
    let mut row_index = 0_usize;

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let sample_start = Instant::now();
        let timestamp_ns = if sample_start >= warmup_end {
            current_unix_epoch_ns()
        } else {
            0
        };
        writer
            .begin_row()
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        writer
            .write_utf8("symbol", &format!("S{:04}", row_index % config.symbols))
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        writer
            .write_i64("dt", timestamp_ns)
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        writer
            .write_f64("price", 10.0 + (row_index % 100) as f64 * 0.01)
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        writer
            .write_f64("volume", 100.0 + (row_index % 17) as f64)
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        writer
            .commit_row()
            .map_err(|reason| zippy_core::ZippyError::Io {
                reason: reason.to_string(),
            })?;
        let span = RowSpanView::from_active_descriptor(
            writer.active_descriptor(),
            row_index,
            row_index + 1,
        )
        .map_err(|reason| zippy_core::ZippyError::Io {
            reason: reason.to_string(),
        })?;
        let emit_start = Instant::now();
        sink.emit(SourceEvent::Data(SegmentTableView::from_row_span(span)))?;
        if sample_start >= warmup_end {
            emit_latencies
                .lock()
                .unwrap()
                .push(emit_start.elapsed().as_micros() as u64);
        }
        row_index += 1;
        next_tick += row_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    sink.emit(SourceEvent::Stop)
}

fn wait_for_runtime_profile_stop(
    handle: &mut zippy_core::EngineHandle,
    config: &PerfConfig,
) -> PerfResult<()> {
    let deadline =
        Instant::now() + Duration::from_secs(config.duration_sec + config.warmup_sec + 5);
    while handle.status() == EngineStatus::Running {
        if Instant::now() >= deadline {
            handle.stop().map_err(|error| error.to_string())?;
            return Err("runtime stream table profile did not stop before deadline".to_string());
        }
        thread::sleep(Duration::from_millis(1));
    }
    if handle.status() == EngineStatus::Failed {
        return Err("runtime stream table profile failed".to_string());
    }
    Ok(())
}

fn first_dt_ns(table: &SegmentTableView) -> Result<Option<i64>> {
    if table.num_rows() == 0 {
        return Ok(None);
    }
    if let Some(row_view) = table.as_segment_row_view() {
        return match row_view.row_span().cell_value(0, "dt").map_err(|error| {
            zippy_core::ZippyError::Io {
                reason: error.to_string(),
            }
        })? {
            SegmentCellValue::TimestampNs(value) | SegmentCellValue::Int64(value) => {
                Ok(Some(value))
            }
            SegmentCellValue::Null => Ok(None),
            _ => Err(zippy_core::ZippyError::SchemaMismatch {
                reason: "dt column must be timestamp or int64".to_string(),
            }),
        };
    }
    let column = table.column("dt")?;
    let timestamps = column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| zippy_core::ZippyError::SchemaMismatch {
            reason: "dt column must be timestamp[ns]".to_string(),
        })?;
    Ok(Some(timestamps.value(0)))
}

fn current_unix_epoch_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn single_row_profile_config(config: &PerfConfig) -> PerfConfig {
    let mut normalized = config.clone();
    normalized.rows_per_batch = 1;
    normalized
}

fn expected_single_row_capacity(config: &PerfConfig) -> PerfResult<usize> {
    const MAX_LOW_LATENCY_ROWS: u128 = 1_048_576;

    let total_seconds = u128::from(config.warmup_sec) + u128::from(config.duration_sec);
    let expected_rows = u128::from(config.target_rows_per_sec)
        .checked_mul(total_seconds)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| "single-row profile expected row count overflowed".to_string())?;
    if expected_rows > MAX_LOW_LATENCY_ROWS {
        return Err(format!(
            "single-row profile expected_rows=[{}] exceeds max_rows=[{}]; lower duration_sec or target_rows_per_sec",
            expected_rows, MAX_LOW_LATENCY_ROWS
        ));
    }
    usize::try_from(expected_rows.max(1))
        .map_err(|_| "single-row profile expected row count overflows usize".to_string())
}

fn build_latency_summary(samples_micros: &[u64]) -> LatencySummary {
    if samples_micros.is_empty() {
        return LatencySummary::default();
    }
    let mut sorted = samples_micros.to_vec();
    sorted.sort_unstable();
    LatencySummary {
        sample_count: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
        p50_micros: percentile(&sorted, 0.50),
        p95_micros: percentile(&sorted, 0.95),
        p99_micros: percentile(&sorted, 0.99),
        max_micros: sorted.last().copied().unwrap_or_default() as f64,
    }
}

fn percentile(sorted: &[u64], percentile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index] as f64
}

fn engine_metrics_report(snapshot: EngineMetricsSnapshot) -> EngineMetricsReport {
    EngineMetricsReport {
        processed_batches_total: snapshot.processed_batches_total,
        processed_rows_total: snapshot.processed_rows_total,
        output_batches_total: snapshot.output_batches_total,
        dropped_batches_total: snapshot.dropped_batches_total,
        late_rows_total: snapshot.late_rows_total,
        publish_errors_total: snapshot.publish_errors_total,
        queue_depth: snapshot.queue_depth,
        max_observed_queue_depth: snapshot.queue_depth,
    }
}

fn source_metrics_report(counters: &Arc<SourceCountersState>) -> SourceMetricsReport {
    SourceMetricsReport {
        source_events_total: counters.source_events_total.load(Ordering::Relaxed),
        source_data_batches_total: counters.source_data_batches_total.load(Ordering::Relaxed),
        source_control_events_total: counters.source_control_events_total.load(Ordering::Relaxed),
        source_decode_errors_total: counters.source_decode_errors_total.load(Ordering::Relaxed),
        source_schema_mismatches_total: counters
            .source_schema_mismatches_total
            .load(Ordering::Relaxed),
        source_sequence_gaps_total: counters.source_sequence_gaps_total.load(Ordering::Relaxed),
    }
}

fn compute_batches_per_sec(output_batches_total: u64, observed_secs: f64) -> f64 {
    output_batches_total as f64 / observed_secs.max(1e-9)
}

fn count_per_sec_over_elapsed(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(1e-9)
}

fn validate_inproc_completion(accepted_rows: u64, processed_rows: u64) -> PerfResult<()> {
    if accepted_rows != processed_rows {
        return Err(format!(
            "in-process row count mismatch accepted_rows=[{accepted_rows}] \
             processed_rows=[{processed_rows}]"
        ));
    }
    Ok(())
}

fn classify_source_error(error: &zippy_core::ZippyError, counters: &Arc<SourceCountersState>) {
    match error {
        zippy_core::ZippyError::SchemaMismatch { .. } => {
            counters
                .source_schema_mismatches_total
                .fetch_add(1, Ordering::Relaxed);
        }
        zippy_core::ZippyError::Io { .. } => {
            counters
                .source_decode_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

fn evaluate_pass(
    config: &PerfConfig,
    average_rows_per_sec: f64,
    status: EngineStatus,
    engine_metrics: &EngineMetricsReport,
    source_metrics: Option<&SourceMetricsReport>,
    latency: &LatencySummary,
) -> bool {
    if status == EngineStatus::Failed {
        return false;
    }
    if average_rows_per_sec < config.target_rows_per_sec as f64 * MIN_PASS_RATIO {
        return false;
    }
    if engine_metrics.dropped_batches_total != 0 || engine_metrics.publish_errors_total != 0 {
        return false;
    }
    if config
        .max_queue_depth
        .is_some_and(|max_queue_depth| engine_metrics.max_observed_queue_depth > max_queue_depth)
    {
        return false;
    }
    if config
        .max_p95_micros
        .is_some_and(|max_p95_micros| latency.p95_micros > max_p95_micros)
    {
        return false;
    }
    if config
        .max_p99_micros
        .is_some_and(|max_p99_micros| latency.p99_micros > max_p99_micros)
    {
        return false;
    }
    if let Some(source_metrics) = source_metrics {
        if source_metrics.source_decode_errors_total != 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{
        checked_write_to_subscribe_elapsed_ns, compute_remote_downstream_deadline,
        consume_write_to_subscribe_rows, consume_write_to_subscribe_span,
        count_per_sec_over_elapsed, drive_segment_write_to_consumer,
        is_write_to_subscribe_measured, merge_write_to_subscribe_results,
        merge_write_to_subscribe_startup_error, produce_write_to_subscribe_rows,
        remote_downstream_should_stop_for_idle, run_profile, validate_inproc_completion,
        validate_write_to_subscribe_counts, write_report_json,
        write_to_subscribe_consumer_complete, write_to_subscribe_deadline_offset,
        ConsumerJoinGuard, ConsumerStats, EngineMetricsReport, LatencySummary,
        OverflowPolicyConfig, PerfConfig, PerfProfile, ProducerOutcome, ProducerState,
        SteadyPeakTracker, WriteToSubscribeTiming, REMOTE_IDLE_GRACE_SEC,
    };
    use zippy_core::EngineStatus;
    use zippy_segment_store::{
        compile_schema, ActiveSegmentReader, ActiveSegmentWriter, ColumnSpec, ColumnType,
        LayoutPlan,
    };

    fn test_endpoint() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("tcp://127.0.0.1:{}", 20_000 + (nanos % 20_000) as u16)
    }

    fn test_config(profile: PerfProfile, endpoint: String) -> PerfConfig {
        PerfConfig {
            profile,
            rows_per_batch: 256,
            target_rows_per_sec: 10_000,
            duration_sec: 1,
            warmup_sec: 0,
            symbols: 32,
            endpoint,
            buffer_capacity: 1024,
            overflow_policy: OverflowPolicyConfig::Block,
            max_p95_micros: None,
            max_p99_micros: None,
            max_queue_depth: None,
        }
    }

    fn write_to_subscribe_test_segment(
        row_capacity: usize,
    ) -> (ActiveSegmentWriter, ActiveSegmentReader) {
        let schema = compile_schema(&[
            ColumnSpec::new("symbol", ColumnType::Utf8),
            ColumnSpec::new("sent_elapsed_ns", ColumnType::Int64),
            ColumnSpec::new("price", ColumnType::Float64),
            ColumnSpec::new("volume", ColumnType::Float64),
        ])
        .unwrap();
        let layout = LayoutPlan::for_schema(&schema, row_capacity).unwrap();
        let writer =
            ActiveSegmentWriter::new_for_runtime(schema.clone(), layout.clone(), 90_001, 0)
                .unwrap();
        let envelope = writer.active_descriptor().to_envelope_bytes().unwrap();
        let reader =
            ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();
        (writer, reader)
    }

    fn write_to_subscribe_test_row(
        writer: &mut ActiveSegmentWriter,
        row_index: usize,
        sent_elapsed_ns: i64,
    ) {
        writer.begin_row().unwrap();
        writer
            .write_utf8("symbol", &format!("S{row_index:04}"))
            .unwrap();
        writer.write_f64("price", 10.0 + row_index as f64).unwrap();
        writer
            .write_f64("volume", 100.0 + row_index as f64)
            .unwrap();
        writer
            .write_i64("sent_elapsed_ns", sent_elapsed_ns)
            .unwrap();
        writer.commit_row().unwrap();
    }

    #[test]
    fn inproc_timeseries_profile_produces_nonzero_rows_and_passes() {
        let report =
            run_profile(&test_config(PerfProfile::InprocTimeseries, test_endpoint())).unwrap();

        assert!(report.input_rows_total > 0);
        assert_eq!(
            report.input_rows_total,
            report.engine_metrics.processed_rows_total
        );
        assert!(
            report.engine_metrics.max_observed_queue_depth >= report.engine_metrics.queue_depth
        );
        assert!(report.actual_average_rows_per_sec > 0.0);
        assert!(report.batches_per_sec > 0.0);
        assert!(report.pass);
    }

    #[test]
    fn drain_inclusive_throughput_uses_total_processing_elapsed_time() {
        assert_eq!(
            count_per_sec_over_elapsed(100, Duration::from_secs(2)),
            50.0
        );
    }

    #[test]
    fn inproc_completeness_rejects_unprocessed_accepted_rows() {
        let error = validate_inproc_completion(100, 99).unwrap_err();

        assert_eq!(
            error,
            "in-process row count mismatch accepted_rows=[100] processed_rows=[99]"
        );
    }

    #[test]
    fn stream_table_segment_profiles_produce_nonzero_rows_and_pass() {
        for profile in [
            PerfProfile::StreamTableSegmentCopy,
            PerfProfile::StreamTableSegmentForward,
        ] {
            let report = run_profile(&test_config(profile, test_endpoint())).unwrap();

            assert!(report.input_rows_total > 0);
            assert!(report.actual_average_rows_per_sec > 0.0);
            assert!(report.pass);
        }
    }

    #[test]
    fn single_row_low_rate_profile_produces_nonzero_rows_and_passes() {
        let mut config = test_config(PerfProfile::SingleRowLowRate, test_endpoint());
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 200;
        config.symbols = 1;

        let report = run_profile(&config).unwrap();

        assert!(report.input_rows_total > 0);
        assert!(report.output_rows_total > 0);
        assert!(report.latency.p99_micros >= report.latency.p50_micros);
        assert!(report.pass);
    }

    #[test]
    fn write_to_subscribe_live_profile_reports_complete_consumer_boundary_samples() {
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e, test_endpoint());
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 200;
        config.warmup_sec = 0;
        config.symbols = 1;

        let report = run_profile(&config).unwrap();

        assert!(report.input_rows_total > 0);
        assert_eq!(report.input_rows_total, report.output_rows_total);
        assert_eq!(report.output_rows_total, report.latency.sample_count);
        assert!(report.actual_peak_rows_per_sec >= report.actual_average_rows_per_sec);
        assert!(report.latency.p99_micros >= report.latency.p50_micros);
        assert!(report.latency.max_micros >= report.latency.p99_micros);
        assert!(report.pass);
    }

    #[test]
    fn write_to_subscribe_rejects_produced_consumed_mismatch() {
        let error = validate_write_to_subscribe_counts(3, 2, 2, 2, 2).unwrap_err();

        assert_eq!(
            error,
            "write-to-subscribe row count mismatch produced=[3] consumed=[2]"
        );
    }

    #[test]
    fn write_to_subscribe_rejects_measured_produced_consumed_mismatch() {
        let error = validate_write_to_subscribe_counts(3, 3, 2, 1, 1).unwrap_err();

        assert_eq!(
            error,
            "write-to-subscribe measured row count mismatch measured_produced=[2] measured_consumed=[1]"
        );
    }

    #[test]
    fn write_to_subscribe_rejects_measured_sample_mismatch() {
        let error = validate_write_to_subscribe_counts(3, 3, 2, 2, 1).unwrap_err();

        assert_eq!(
            error,
            "write-to-subscribe latency sample count mismatch measured_consumed=[2] latency_samples=[1]"
        );
    }

    #[test]
    fn write_to_subscribe_uses_half_open_steady_measurement_window() {
        assert!(!is_write_to_subscribe_measured(9, 10, 20));
        assert!(is_write_to_subscribe_measured(10, 10, 20));
        assert!(is_write_to_subscribe_measured(19, 10, 20));
        assert!(!is_write_to_subscribe_measured(20, 10, 20));
    }

    #[test]
    fn write_to_subscribe_rational_deadline_matches_expected_capacity() {
        let target_rows_per_sec = 31_741_u64;
        let duration_sec = 30_u64;
        let capacity = target_rows_per_sec * duration_sec + 1;
        let steady_duration = Duration::from_secs(duration_sec);

        let last_write_deadline =
            write_to_subscribe_deadline_offset(capacity - 2, target_rows_per_sec).unwrap();
        let first_outside_deadline =
            write_to_subscribe_deadline_offset(capacity - 1, target_rows_per_sec).unwrap();
        let old_floor_interval = Duration::from_nanos(1_000_000_000 / target_rows_per_sec);
        let old_first_outside_deadline = old_floor_interval * (capacity - 1) as u32;

        assert!(last_write_deadline < steady_duration);
        assert!(first_outside_deadline >= steady_duration);
        assert!(old_first_outside_deadline < steady_duration);
    }

    #[test]
    fn write_to_subscribe_done_waits_until_all_produced_rows_are_consumed() {
        let state = ProducerState::default();
        state.produced.store(2, Ordering::Release);

        assert!(!write_to_subscribe_consumer_complete(&state, 2));
        state.done.store(true, Ordering::Release);

        assert!(!write_to_subscribe_consumer_complete(&state, 1));
        assert!(write_to_subscribe_consumer_complete(&state, 2));
    }

    #[test]
    fn write_to_subscribe_elapsed_ns_conversion_checks_i64_boundary() {
        assert_eq!(
            checked_write_to_subscribe_elapsed_ns(i64::MAX as u128).unwrap(),
            i64::MAX
        );

        let error = checked_write_to_subscribe_elapsed_ns(i64::MAX as u128 + 1).unwrap_err();
        assert_eq!(error, "write-to-subscribe sent_elapsed_ns overflowed i64");
    }

    #[test]
    fn write_to_subscribe_peak_tracker_reports_burst_window_not_average() {
        const SECOND_NS: u128 = 1_000_000_000;
        let mut tracker = SteadyPeakTracker::new(SECOND_NS, SECOND_NS * 3);

        for sent_elapsed_ns in [
            SECOND_NS - 1,
            SECOND_NS,
            SECOND_NS + 1,
            SECOND_NS * 2 - 1,
            SECOND_NS * 2,
            SECOND_NS * 3,
        ] {
            tracker.record(sent_elapsed_ns).unwrap();
        }

        let peak_rows_per_sec = tracker.finish();
        let average_rows_per_sec = 4.0 / 2.0;
        assert_eq!(peak_rows_per_sec, 3);
        assert_ne!(peak_rows_per_sec as f64, average_rows_per_sec);
    }

    #[test]
    fn write_to_subscribe_startup_error_remains_primary_during_cleanup_failure() {
        let consumer_result = Err("consumer-cleanup-original".to_string());

        let error =
            merge_write_to_subscribe_startup_error("startup-original".to_string(), consumer_result);

        assert_eq!(
            error,
            "startup-original; write-to-subscribe consumer cleanup error: consumer-cleanup-original"
        );
    }

    #[test]
    fn write_to_subscribe_consumer_join_guard_records_real_panic_and_joins() {
        let state = Arc::new(ProducerState::default());
        let handle = thread::spawn(|| -> Result<ConsumerStats, String> {
            panic!("consumer-guard-original");
        });
        let mut guard = ConsumerJoinGuard::new(Arc::clone(&state), handle);

        let error = guard.join().unwrap_err();

        assert!(error.contains("consumer-guard-original"));
        assert!(state.cancelled.load(Ordering::Acquire));
        assert!(state.done.load(Ordering::Acquire));
    }

    #[test]
    fn write_to_subscribe_multi_row_span_reuses_one_observation_timestamp() {
        let (mut writer, mut reader) = write_to_subscribe_test_segment(2);
        write_to_subscribe_test_row(&mut writer, 0, 1_000);
        write_to_subscribe_test_row(&mut writer, 1, 1_000);
        let span = reader.read_available().unwrap().unwrap();
        let mut consumed = 0;
        let mut measured_consumed = 0;
        let mut latency_samples_micros = Vec::new();

        consume_write_to_subscribe_span(
            &span,
            6_000,
            0,
            10_000,
            &mut consumed,
            &mut measured_consumed,
            &mut latency_samples_micros,
        )
        .unwrap();

        assert_eq!(consumed, 2);
        assert_eq!(measured_consumed, 2);
        assert_eq!(latency_samples_micros, vec![5, 5]);
    }

    #[test]
    fn write_to_subscribe_producer_error_sets_done_and_cancelled() {
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e, test_endpoint());
        config.target_rows_per_sec = 1_000_000_000;
        config.symbols = 1;
        let (mut writer, _reader) = write_to_subscribe_test_segment(1);
        let origin = Instant::now();
        let end = origin + Duration::from_millis(20);
        let state = Arc::new(ProducerState::default());

        let result = produce_write_to_subscribe_rows(
            &config,
            &mut writer,
            origin,
            end,
            0,
            Duration::from_millis(20).as_nanos(),
            Arc::clone(&state),
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("small writer must fail before producer deadline"),
        };

        assert_eq!(error, "segment is full");
        assert_eq!(state.produced.load(Ordering::Acquire), 1);
        assert!(state.cancelled.load(Ordering::Acquire));
        assert!(state.done.load(Ordering::Acquire));
    }

    #[test]
    fn write_to_subscribe_producer_error_before_first_row_joins_consumer_and_clears_waiter() {
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e, test_endpoint());
        config.target_rows_per_sec = 1_000_000_000;
        config.symbols = 1;
        let (mut writer, mut reader) = write_to_subscribe_test_segment(1);
        write_to_subscribe_test_row(&mut writer, 0, 0);
        reader.seek_to_committed().unwrap();
        let observer = reader.clone();

        let result = drive_segment_write_to_consumer(&config, &mut writer, reader);
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("small writer must fail before producer deadline"),
        };

        assert_eq!(error, "segment is full");
        assert_eq!(observer.control_snapshot().unwrap().waiter_count, 0);
    }

    #[test]
    fn write_to_subscribe_combines_consumer_and_producer_errors_consumer_first() {
        let producer_result = Err("producer-original".to_string());
        let consumer_result = Err("consumer-original".to_string());

        let result = merge_write_to_subscribe_results(producer_result, consumer_result);
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("double error must remain an error"),
        };

        assert_eq!(
            error,
            "write-to-subscribe consumer error: consumer-original; producer error: producer-original"
        );
    }

    #[test]
    fn write_to_subscribe_cancelled_producer_preserves_consumer_error() {
        let producer_result = Ok(ProducerOutcome::CancelledByConsumer);
        let consumer_result: Result<ConsumerStats, String> = Err("consumer-original".to_string());

        let result = merge_write_to_subscribe_results(producer_result, consumer_result);
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("consumer error must remain an error"),
        };

        assert_eq!(error, "consumer-original");
    }

    #[test]
    fn write_to_subscribe_empty_real_segment_consumer_times_out() {
        let (writer, reader) = write_to_subscribe_test_segment(1);
        let observer = reader.clone();
        let state = Arc::new(ProducerState::default());
        let origin = Instant::now();
        let timing = Arc::new(OnceLock::new());
        assert!(timing
            .set(WriteToSubscribeTiming {
                origin,
                warmup_end_elapsed_ns: 0,
                steady_end_elapsed_ns: 0,
                consumer_deadline: origin + Duration::from_millis(20),
            })
            .is_ok());
        let error =
            consume_write_to_subscribe_rows(reader, timing, Arc::clone(&state)).unwrap_err();

        assert!(
            error.contains("consumer timed out"),
            "unexpected error: {error}"
        );
        assert!(state.cancelled.load(Ordering::Acquire));
        assert_eq!(observer.control_snapshot().unwrap().waiter_count, 0);
        drop(writer);
    }

    #[test]
    fn runtime_stream_table_e2e_reports_publish_and_emit_return_latency() {
        let mut config = test_config(PerfProfile::RuntimeStreamTableE2e, test_endpoint());
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 200;
        config.symbols = 1;

        let report = run_profile(&config).unwrap();

        assert!(report.input_rows_total > 0);
        assert!(report.output_rows_total > 0);
        assert!(report.latency.p99_micros >= report.latency.p50_micros);
        let secondary = report
            .secondary_latency
            .as_ref()
            .expect("runtime profile must report source emit return latency");
        assert!(secondary.p99_micros >= secondary.p50_micros);
        assert!(report.pass);
    }

    #[test]
    fn stream_table_subscriber_e2e_reports_target_reader_latency() {
        let mut config = test_config(PerfProfile::StreamTableSubscriberE2e, test_endpoint());
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 200;
        config.symbols = 1;

        let report = run_profile(&config).unwrap();

        assert!(report.input_rows_total > 0);
        assert!(report.output_rows_total > 0);
        assert!(report.latency.p99_micros >= report.latency.p50_micros);
        let secondary = report
            .secondary_latency
            .as_ref()
            .expect("stream table subscriber profile must report on_data latency");
        assert!(secondary.p99_micros >= secondary.p50_micros);
        assert!(report.pass);
    }

    #[test]
    fn single_row_profiles_reject_capacity_above_single_segment_limit() {
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e, test_endpoint());
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 1_000_000;
        config.duration_sec = 60;

        let error = run_profile(&config).unwrap_err();

        assert!(error.contains("expected_rows"));
        assert!(error.contains("lower duration_sec or target_rows_per_sec"));
    }

    #[test]
    fn remote_pipeline_profiles_exchange_data_and_pass() {
        let endpoint = test_endpoint();
        let downstream_handle = std::thread::spawn({
            let endpoint = endpoint.clone();
            move || {
                run_profile(&test_config(
                    PerfProfile::RemotePipelineDownstream,
                    endpoint,
                ))
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(200));

        let upstream_report = run_profile(&test_config(
            PerfProfile::RemotePipelineUpstream,
            endpoint.clone(),
        ))
        .unwrap();
        let downstream_report = downstream_handle.join().unwrap().unwrap();

        assert!(upstream_report.input_rows_total > 0);
        assert!(downstream_report.output_rows_total > 0);
        assert!(upstream_report.pass);
        assert!(downstream_report.pass);
    }

    #[test]
    fn writes_report_json_with_profile_and_pass_fields() {
        let report =
            run_profile(&test_config(PerfProfile::InprocTimeseries, test_endpoint())).unwrap();
        let path = std::env::temp_dir().join(format!(
            "zippy-perf-report-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        write_report_json(&path, &report).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        assert!(content.contains("profile"));
        assert!(content.contains("pass"));
        assert!(content.contains("metadata"));
        assert!(content.contains("git_sha"));
        assert!(content.contains("target"));
        assert!(content.contains("started_at_unix_ms"));
        let formatted = super::format_report(&report);
        assert!(formatted.contains("latency_micros[count="));
        assert!(formatted.contains("max="));
        assert!(content.contains("sample_count"));
        assert!(content.contains("max_micros"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn latency_summary_reports_sample_count_and_maximum() {
        let summary = super::build_latency_summary(&[9, 1, 4]);

        assert_eq!(summary.sample_count, 3);
        assert_eq!(summary.max_micros, 9.0);
        assert_eq!(summary.p50_micros, 4.0);
        assert_eq!(summary.p95_micros, 9.0);
        assert_eq!(summary.p99_micros, 9.0);

        let empty = super::build_latency_summary(&[]);
        assert_eq!(empty.sample_count, 0);
        assert_eq!(empty.max_micros, 0.0);
        assert_eq!(empty.p50_micros, 0.0);
        assert_eq!(empty.p95_micros, 0.0);
        assert_eq!(empty.p99_micros, 0.0);
    }

    #[test]
    fn latency_summary_deserializes_legacy_json_without_additive_fields() {
        let summary: LatencySummary =
            serde_json::from_str(r#"{"p50_micros": 4.0, "p95_micros": 9.0, "p99_micros": 9.0}"#)
                .unwrap();

        assert_eq!(summary.sample_count, 0);
        assert_eq!(summary.max_micros, 0.0);
        assert_eq!(summary.p50_micros, 4.0);
        assert_eq!(summary.p95_micros, 9.0);
        assert_eq!(summary.p99_micros, 9.0);
    }

    #[test]
    fn evaluate_pass_rejects_latency_above_configured_p99_threshold() {
        let mut config = test_config(PerfProfile::InprocTimeseries, test_endpoint());
        config.max_p99_micros = Some(100.0);
        let latency = LatencySummary {
            sample_count: 1,
            p50_micros: 10.0,
            p95_micros: 50.0,
            p99_micros: 101.0,
            max_micros: 101.0,
        };

        let pass = super::evaluate_pass(
            &config,
            10_000.0,
            EngineStatus::Stopped,
            &EngineMetricsReport::default(),
            None,
            &latency,
        );

        assert!(!pass);
    }

    #[test]
    fn evaluate_pass_rejects_latency_above_configured_p95_threshold() {
        let mut config = test_config(PerfProfile::InprocTimeseries, test_endpoint());
        config.max_p95_micros = Some(50.0);
        let latency = LatencySummary {
            sample_count: 1,
            p50_micros: 10.0,
            p95_micros: 50.1,
            p99_micros: 80.0,
            max_micros: 80.0,
        };

        let pass = super::evaluate_pass(
            &config,
            10_000.0,
            EngineStatus::Stopped,
            &EngineMetricsReport::default(),
            None,
            &latency,
        );

        assert!(!pass);
    }

    #[test]
    fn evaluate_pass_rejects_queue_depth_above_configured_threshold() {
        let mut config = test_config(PerfProfile::InprocTimeseries, test_endpoint());
        config.max_queue_depth = Some(2);
        let engine_metrics = EngineMetricsReport {
            queue_depth: 0,
            max_observed_queue_depth: 3,
            ..EngineMetricsReport::default()
        };

        let pass = super::evaluate_pass(
            &config,
            10_000.0,
            EngineStatus::Stopped,
            &engine_metrics,
            None,
            &LatencySummary::default(),
        );

        assert!(!pass);
    }

    #[test]
    fn engine_metrics_report_deserializes_legacy_json_without_observed_queue_depth() {
        let metrics: EngineMetricsReport = serde_json::from_str(
            r#"{"processed_batches_total":1,"processed_rows_total":2,"output_batches_total":1,"dropped_batches_total":0,"late_rows_total":0,"publish_errors_total":0,"queue_depth":0}"#,
        )
        .unwrap();

        assert_eq!(metrics.max_observed_queue_depth, 0);
        assert_eq!(metrics.queue_depth, 0);
    }

    #[test]
    fn remote_downstream_deadline_starts_after_first_observed_event() {
        let observed_at = std::time::Instant::now();
        let deadline =
            compute_remote_downstream_deadline(observed_at, 10, 60, REMOTE_IDLE_GRACE_SEC);

        assert_eq!(
            deadline.saturating_duration_since(observed_at),
            std::time::Duration::from_secs(10 + 60 + REMOTE_IDLE_GRACE_SEC),
        );
    }

    #[test]
    fn remote_downstream_stops_after_idle_once_stream_was_observed() {
        let now = std::time::Instant::now();

        assert!(remote_downstream_should_stop_for_idle(
            Some(now - std::time::Duration::from_secs(10)),
            Some(now - std::time::Duration::from_secs(REMOTE_IDLE_GRACE_SEC + 1)),
            now,
        ));
    }

    #[test]
    fn remote_downstream_does_not_stop_for_idle_before_observing_stream() {
        let now = std::time::Instant::now();

        assert!(!remote_downstream_should_stop_for_idle(
            None,
            Some(now - std::time::Duration::from_secs(REMOTE_IDLE_GRACE_SEC + 1)),
            now,
        ));
    }
}
