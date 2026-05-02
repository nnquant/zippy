use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use zippy_core::{
    spawn_engine_with_publisher, spawn_source_engine_with_publisher, Engine, EngineConfig,
    EngineMetricsSnapshot, EngineStatus, LateDataPolicy, OverflowPolicy, Publisher, Result,
    SchemaRef, SegmentTableView, Source, SourceEvent, SourceHandle, SourceMode, SourceSink,
};
use zippy_engines::{
    CrossSectionalEngine, StreamTableDescriptorPublisher, StreamTableMaterializer, TimeSeriesEngine,
};
use zippy_io::{NullPublisher, ZmqSource, ZmqStreamPublisher};
use zippy_operators::{AggLastSpec, CSDemeanSpec, CSRankSpec, CSZscoreSpec};
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
};

const DEFAULT_WINDOW_NS: i64 = 1_000_000;
const REMOTE_STARTUP_DELAY_MS: u64 = 500;
const REMOTE_SHUTDOWN_GRACE_SEC: u64 = 5;
const REMOTE_IDLE_GRACE_SEC: u64 = 3;
const REMOTE_STARTUP_GRACE_SEC: u64 = 30;
const MIN_PASS_RATIO: f64 = 0.90;

type PerfResult<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerfProfile {
    InprocTimeseries,
    RemotePipelineUpstream,
    RemotePipelineDownstream,
    StreamTableSegmentCopy,
    StreamTableSegmentForward,
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
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
pub struct PerfReport {
    pub profile: PerfProfile,
    pub config: PerfConfig,
    pub input_rows_total: u64,
    pub output_rows_total: u64,
    pub actual_average_rows_per_sec: f64,
    pub actual_peak_rows_per_sec: f64,
    pub batches_per_sec: f64,
    pub latency: LatencySummary,
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
    }
}

pub fn write_report_json(path: &Path, report: &PerfReport) -> PerfResult<()> {
    let json = serde_json::to_string_pretty(report).map_err(|error| error.to_string())?;
    fs::write(path, json).map_err(|error| error.to_string())
}

pub fn format_report(report: &PerfReport) -> String {
    let mut lines = vec![
        format!("profile={:?}", report.profile),
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
            "latency_micros[p50={:.2}, p95={:.2}, p99={:.2}]",
            report.latency.p50_micros, report.latency.p95_micros, report.latency.p99_micros
        ),
    ];

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
    let pass = evaluate_pass(
        config,
        drive_stats.actual_average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        None,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(output_batches_total, config.duration_sec as f64),
        latency: build_latency_summary(&drive_stats.batch_latencies_micros),
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
    let pass = evaluate_pass(
        config,
        drive_stats.actual_average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        None,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(output_batches_total, config.duration_sec as f64),
        latency: build_latency_summary(&drive_stats.batch_latencies_micros),
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
    let pass = evaluate_pass(
        config,
        average_rows_per_sec,
        handle.status(),
        &engine_metrics,
        Some(&source_metrics),
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        input_rows_total: engine_metrics.processed_rows_total,
        output_rows_total,
        actual_average_rows_per_sec: average_rows_per_sec,
        actual_peak_rows_per_sec: average_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(output_batches_total, observed_secs),
        latency: LatencySummary::default(),
        engine_status: handle.status().as_str().to_string(),
        engine_metrics,
        source_metrics: Some(source_metrics),
        pass,
    })
}

fn run_stream_table_segment(config: &PerfConfig, forwarding: bool) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let table = build_tick_segment_table(
        config.rows_per_batch,
        config.symbols,
        config.target_rows_per_sec,
    )?;
    let row_capacity = config
        .rows_per_batch
        .saturating_mul(64)
        .max(config.rows_per_batch)
        .min(1_048_576);
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
    };
    let pass = evaluate_pass(
        config,
        drive_stats.actual_average_rows_per_sec,
        EngineStatus::Stopped,
        &engine_metrics,
        None,
    );

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total: drive_stats.input_rows_total,
        actual_average_rows_per_sec: drive_stats.actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec: compute_batches_per_sec(
            processed_batches_total,
            config.duration_sec as f64,
        ),
        latency: build_latency_summary(&drive_stats.batch_latencies_micros),
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
    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        self.counters
            .output_batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .output_rows_total
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.inner.publish(batch)
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

struct DriveStats {
    input_rows_total: u64,
    actual_average_rows_per_sec: f64,
    actual_peak_rows_per_sec: f64,
    batch_latencies_micros: Vec<u64>,
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
    })
}

fn build_latency_summary(samples_micros: &[u64]) -> LatencySummary {
    if samples_micros.is_empty() {
        return LatencySummary::default();
    }
    let mut sorted = samples_micros.to_vec();
    sorted.sort_unstable();
    LatencySummary {
        p50_micros: percentile(&sorted, 0.50),
        p95_micros: percentile(&sorted, 0.95),
        p99_micros: percentile(&sorted, 0.99),
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
    if let Some(source_metrics) = source_metrics {
        if source_metrics.source_decode_errors_total != 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        compute_remote_downstream_deadline, remote_downstream_should_stop_for_idle, run_profile,
        write_report_json, OverflowPolicyConfig, PerfConfig, PerfProfile, REMOTE_IDLE_GRACE_SEC,
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
        }
    }

    #[test]
    fn inproc_timeseries_profile_produces_nonzero_rows_and_passes() {
        let report =
            run_profile(&test_config(PerfProfile::InprocTimeseries, test_endpoint())).unwrap();

        assert!(report.input_rows_total > 0);
        assert!(report.actual_average_rows_per_sec > 0.0);
        assert!(report.pass);
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
        let path = PathBuf::from(format!(
            "/tmp/zippy-perf-report-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        write_report_json(&path, &report).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();

        assert!(content.contains("profile"));
        assert!(content.contains("pass"));
        std::fs::remove_file(path).unwrap();
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
