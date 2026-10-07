use std::fs;
use std::io::{Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zippy_core::{Engine, LateDataPolicy, Result, SchemaRef, SegmentTableView};
use zippy_engines::{StreamTableMaterializer, TimeSeriesEngine};
use zippy_operators::AggLastSpec;
use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
    RowSpanView, SegmentCellValue,
};

const DEFAULT_WINDOW_NS: i64 = 1_000_000;
const MIN_PASS_RATIO: f64 = 0.90;
const WRITE_TO_SUBSCRIBE_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TO_SUBSCRIBE_WAIT_SLICE: Duration = Duration::from_millis(10);
const WRITE_TO_SUBSCRIBE_STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(1);
const BASELINE_MANIFEST_FORMAT_VERSION: u32 = 2;
const SINGLE_NODE_REFACTOR_SUITE: &str = "single-node-refactor";
const DEFAULT_SYNTHETIC_DATASET_ID: &str = "zippy-perf.synthetic.v1";

type PerfResult<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerfProfile {
    TimeseriesKernel,
    StreamTableSegmentCopy,
    SingleRowLowRate,
    WriteToSubscribeE2e,
    StreamTableSubscriberE2e,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerfConfig {
    pub profile: PerfProfile,
    pub rows_per_batch: usize,
    pub target_rows_per_sec: u64,
    pub duration_sec: u64,
    pub warmup_sec: u64,
    pub symbols: usize,
    pub max_p95_micros: Option<f64>,
    pub max_p99_micros: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub sample_count: u64,
    pub p50_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
    pub max_micros: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineMetricsReport {
    pub processed_batches_total: u64,
    pub processed_rows_total: u64,
    pub output_batches_total: u64,
    pub late_rows_total: u64,
    pub filtered_rows_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfReportMetadata {
    pub git_sha: String,
    pub target: String,
    pub started_at_unix_ms: u128,
}

impl PerfReportMetadata {
    fn capture() -> Self {
        Self::capture_with_git_sha(current_git_sha())
    }

    fn capture_with_git_sha(git_sha: String) -> Self {
        Self {
            git_sha,
            target: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            started_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub engine_metrics: EngineMetricsReport,
    pub pass: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state of a single-node refactor baseline run.
pub enum BaselineRunState {
    /// The manifest was captured before benchmark dispatch.
    Planned,
    /// The benchmark completed and its report is embedded.
    Completed,
    /// The benchmark failed before it could produce a report.
    Failed,
}

/// Identity of the exact executable used to run a baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfBinaryIdentity {
    /// File name of the running executable.
    pub binary_name: String,
    /// Cargo package version compiled into the executable.
    pub package_version: String,
    /// Coarse Cargo build profile, either `debug` or `release`.
    pub build_profile: String,
    /// Runtime operating system and architecture.
    pub target: String,
    /// Canonical path of the running executable.
    pub executable_path: PathBuf,
    /// Size of the running executable in bytes.
    pub executable_size_bytes: u64,
    /// Lowercase SHA-256 digest of the running executable.
    pub executable_sha256: String,
}

impl PerfBinaryIdentity {
    fn capture() -> PerfResult<Self> {
        let executable_path = std::env::current_exe().map_err(|error| error.to_string())?;
        let executable_path = fs::canonicalize(&executable_path).unwrap_or(executable_path);
        let executable_file = open_running_executable(&executable_path)?;
        let executable_metadata = executable_file
            .metadata()
            .map_err(|error| error.to_string())?;
        let binary_name = executable_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "current executable has no valid UTF-8 file name".to_string())?
            .to_string();
        let executable_sha256 =
            sha256_reader(executable_file).map_err(|error| error.to_string())?;

        Ok(Self {
            binary_name,
            package_version: env!("CARGO_PKG_VERSION").to_string(),
            build_profile: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            target: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            executable_path,
            executable_size_bytes: executable_metadata.len(),
            executable_sha256,
        })
    }
}

/// Git source tree identity embedded when the benchmark binary was built.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfSourceIdentity {
    /// Full Git object ID for `HEAD`.
    pub git_sha: String,
    /// Whether tracked or untracked changes were present at build time.
    pub git_dirty: bool,
}

impl PerfSourceIdentity {
    fn capture() -> PerfResult<Self> {
        let git_sha = option_env!("ZIPPY_PERF_GIT_SHA")
            .ok_or_else(|| "build did not embed a full git object id".to_string())?;
        let git_dirty = option_env!("ZIPPY_PERF_GIT_DIRTY")
            .ok_or_else(|| "build did not embed git dirty state".to_string())?;
        parse_git_source_identity(git_sha.to_string(), git_dirty)
    }
}

/// Data-generation parameters that determine the benchmark input shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfDatasetParameters {
    /// Number of rows produced in each input batch.
    pub rows_per_batch: usize,
    /// Configured input rate.
    pub target_rows_per_sec: u64,
    /// Measured benchmark duration.
    pub duration_sec: u64,
    /// Warm-up duration excluded from steady-state results.
    pub warmup_sec: u64,
    /// Symbol cardinality generated or expected by the profile.
    pub symbols: usize,
}

/// Identity and generation contract for a benchmark dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfDatasetManifest {
    /// Stable operator-supplied or derived dataset identifier.
    pub dataset_id: String,
    /// Input source category, currently `synthetic`.
    pub source_kind: String,
    /// Versioned logical input schema used by the profile.
    pub input_contract: String,
    /// Parameters affecting generated or expected input data.
    pub parameters: PerfDatasetParameters,
}

impl PerfDatasetManifest {
    fn from_config(dataset_id: String, config: &PerfConfig) -> Self {
        let input_contract = match config.profile {
            PerfProfile::WriteToSubscribeE2e | PerfProfile::StreamTableSubscriberE2e => {
                "zippy-perf.timestamped-tick.v1"
            }
            _ => "zippy-perf.tick.v1",
        };

        Self {
            dataset_id,
            source_kind: "synthetic".to_string(),
            input_contract: input_contract.to_string(),
            parameters: PerfDatasetParameters {
                rows_per_batch: config.rows_per_batch,
                target_rows_per_sec: config.target_rows_per_sec,
                duration_sec: config.duration_sec,
                warmup_sec: config.warmup_sec,
                symbols: config.symbols,
            },
        }
    }
}

/// Machine-readable evidence envelope for a single-node refactor baseline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineManifest {
    /// Manifest schema version.
    pub format_version: u32,
    /// Stable benchmark suite identifier.
    pub suite: String,
    /// Current lifecycle state.
    pub state: BaselineRunState,
    /// Start time, 12-character Git SHA, and runtime target used by the report.
    pub metadata: PerfReportMetadata,
    /// Full Git SHA and working-tree state embedded at build time.
    pub source: PerfSourceIdentity,
    /// Identity of the exact executable used for the run.
    pub binary: PerfBinaryIdentity,
    /// Complete benchmark configuration.
    pub config: PerfConfig,
    /// Dataset identity, contract, and generation parameters.
    pub dataset: PerfDatasetManifest,
    /// Completed report, absent for planned and failed runs.
    pub report: Option<PerfReport>,
    /// Failure reason, present only for failed runs.
    pub error: Option<String>,
}

impl BaselineManifest {
    /// Transition a planned manifest to completed using the resulting report.
    pub fn completed(mut self, report: &PerfReport) -> PerfResult<Self> {
        if report.config != self.config {
            return Err("baseline report config does not match planned config".to_string());
        }
        let expected_report_sha = short_git_sha(&self.source.git_sha);
        if report.metadata.git_sha != expected_report_sha {
            return Err("baseline report git sha does not match planned source".to_string());
        }
        self.state = BaselineRunState::Completed;
        self.metadata = report.metadata.clone();
        self.report = Some(report.clone());
        self.error = None;
        Ok(self)
    }

    /// Transition a planned manifest to failed and retain its evidence identity.
    pub fn failed(mut self, error: impl Into<String>) -> Self {
        self.state = BaselineRunState::Failed;
        self.report = None;
        self.error = Some(error.into());
        self
    }
}

/// Capture a planned manifest without dispatching the benchmark profile.
///
/// Remote profiles require an explicit dataset ID because their peer process is
/// outside this process's authority. Local profiles default to the versioned
/// synthetic dataset ID when `dataset_id` is `None`.
pub fn plan_baseline_manifest(
    config: &PerfConfig,
    dataset_id: Option<&str>,
) -> PerfResult<BaselineManifest> {
    validate_config(config)?;
    let dataset_id = resolve_dataset_id(dataset_id)?;
    let source = PerfSourceIdentity::capture()?;

    Ok(BaselineManifest {
        format_version: BASELINE_MANIFEST_FORMAT_VERSION,
        suite: SINGLE_NODE_REFACTOR_SUITE.to_string(),
        state: BaselineRunState::Planned,
        metadata: PerfReportMetadata::capture_with_git_sha(short_git_sha(&source.git_sha)),
        source,
        binary: PerfBinaryIdentity::capture()?,
        config: config.clone(),
        dataset: PerfDatasetManifest::from_config(dataset_id, config),
        report: None,
        error: None,
    })
}

/// Atomically write a baseline manifest as formatted JSON.
///
/// The temporary file is created in the destination directory and renamed over
/// the prior state, so readers never observe a partially written manifest.
pub fn write_baseline_manifest_json(path: &Path, manifest: &BaselineManifest) -> PerfResult<()> {
    write_pretty_json(path, manifest)
}

pub fn run_profile(config: &PerfConfig) -> PerfResult<PerfReport> {
    run_profile_with_hooks(config, PerfReportMetadata::capture, dispatch_profile)
}

fn run_profile_with_hooks<CaptureMetadata, Dispatch>(
    config: &PerfConfig,
    capture_metadata: CaptureMetadata,
    dispatch: Dispatch,
) -> PerfResult<PerfReport>
where
    CaptureMetadata: FnOnce() -> PerfReportMetadata,
    Dispatch: FnOnce(&PerfConfig, PerfReportMetadata) -> PerfResult<PerfReport>,
{
    validate_config(config)?;
    let metadata = capture_metadata();
    dispatch(config, metadata)
}

fn dispatch_profile(config: &PerfConfig, metadata: PerfReportMetadata) -> PerfResult<PerfReport> {
    match config.profile {
        PerfProfile::TimeseriesKernel => run_timeseries_kernel(config, metadata),
        PerfProfile::StreamTableSegmentCopy => run_stream_table_segment(config, metadata),
        PerfProfile::SingleRowLowRate => run_single_row_low_rate(config, metadata),
        PerfProfile::WriteToSubscribeE2e => run_write_to_subscribe_e2e(config, metadata),
        PerfProfile::StreamTableSubscriberE2e => run_stream_table_subscriber_e2e(config, metadata),
    }
}

pub fn write_report_json(path: &Path, report: &PerfReport) -> PerfResult<()> {
    write_pretty_json(path, report)
}

fn write_pretty_json(path: &Path, value: &impl Serialize) -> PerfResult<()> {
    let mut json = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    json.push(b'\n');
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".zippy-json-")
        .tempfile_in(parent)
        .map_err(|error| error.to_string())?;
    temporary
        .write_all(&json)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| error.to_string())?;
    temporary
        .persist(path)
        .map_err(|error| error.error.to_string())?;
    sync_json_parent(parent).map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(unix)]
fn sync_json_parent(parent: &Path) -> std::io::Result<()> {
    fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_json_parent(_parent: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn open_running_executable(_reported_path: &Path) -> PerfResult<fs::File> {
    fs::File::open("/proc/self/exe").map_err(|error| error.to_string())
}

#[cfg(not(target_os = "linux"))]
fn open_running_executable(reported_path: &Path) -> PerfResult<fs::File> {
    fs::File::open(reported_path).map_err(|error| error.to_string())
}

fn sha256_reader(mut reader: impl Read) -> std::io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(format!("{:x}", hasher.finalize()));
        }
        hasher.update(&buffer[..read]);
    }
}

fn resolve_dataset_id(dataset_id: Option<&str>) -> PerfResult<String> {
    if let Some(dataset_id) = dataset_id {
        let dataset_id = dataset_id.trim();
        if dataset_id.is_empty() {
            return Err("dataset_id must not be empty".to_string());
        }
        return Ok(dataset_id.to_string());
    }
    Ok(DEFAULT_SYNTHETIC_DATASET_ID.to_string())
}

fn parse_git_source_identity(git_sha: String, git_dirty: &str) -> PerfResult<PerfSourceIdentity> {
    if git_sha.len() < 40 || !git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("git rev-parse returned an invalid full object id".to_string());
    }
    let git_dirty = match git_dirty {
        "true" => true,
        "false" => false,
        value => return Err(format!("invalid embedded git dirty state value=[{value}]")),
    };
    Ok(PerfSourceIdentity { git_sha, git_dirty })
}

fn current_git_sha() -> String {
    option_env!("ZIPPY_PERF_GIT_SHA")
        .map(short_git_sha)
        .unwrap_or_else(|| "unknown".to_string())
}

fn short_git_sha(full_sha: &str) -> String {
    full_sha.chars().take(12).collect()
}

pub fn format_report(report: &PerfReport) -> String {
    let mut lines = vec![
        format!("profile={:?}", report.profile),
        format!("git_sha={}", report.metadata.git_sha),
        format!("target={}", report.metadata.target),
        format!("pass={}", report.pass),
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

    lines.join("\n")
}

fn run_timeseries_kernel(
    config: &PerfConfig,
    metadata: PerfReportMetadata,
) -> PerfResult<PerfReport> {
    let tick_schema = build_tick_schema();
    let mut factory = TickBatchFactory::new(
        Arc::clone(&tick_schema),
        config.rows_per_batch,
        config.symbols,
        config.target_rows_per_sec,
    )?;
    let mut engine = build_timeseries_engine("perf_timeseries_kernel", Arc::clone(&tick_schema))
        .map_err(|error| error.to_string())?;
    let mut engine_metrics = EngineMetricsReport::default();
    let mut output_rows_total = 0;
    let drive_stats = drive_engine_generator(config, &mut factory, &mut |batch: RecordBatch| {
        let rows = batch.num_rows();
        let output = engine
            .on_data(SegmentTableView::from_record_batch(batch))
            .map_err(|error| error.to_string())?;
        engine_metrics.processed_batches_total += 1;
        engine_metrics.processed_rows_total += rows as u64;
        engine_metrics.output_batches_total += output.len() as u64;
        output_rows_total += total_table_rows(&output);
        Ok(())
    })?;
    let output = engine.on_flush().map_err(|error| error.to_string())?;
    let drain_completed_at = Instant::now();
    engine_metrics.output_batches_total += output.len() as u64;
    output_rows_total += total_table_rows(&output);
    let delta = engine.drain_metrics();
    engine_metrics.late_rows_total = delta.late_rows_total;
    engine_metrics.filtered_rows_total = delta.filtered_rows_total;
    validate_inproc_completion(
        drive_stats.input_rows_total,
        engine_metrics.processed_rows_total,
    )?;
    let steady_elapsed = inproc_processing_elapsed(
        drive_stats.steady_started_at,
        drive_stats.steady_ends_at,
        drain_completed_at,
    );
    let actual_average_rows_per_sec =
        count_per_sec_over_elapsed(drive_stats.steady_rows_total, steady_elapsed);
    let batches_per_sec =
        count_per_sec_over_elapsed(drive_stats.steady_batches_total, steady_elapsed);
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let pass = evaluate_pass(config, actual_average_rows_per_sec, &latency);

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata,
        input_rows_total: drive_stats.input_rows_total,
        output_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: drive_stats.actual_peak_rows_per_sec,
        batches_per_sec,
        latency,
        secondary_latency: None,
        engine_metrics,
        pass,
    })
}

fn total_table_rows(tables: &[SegmentTableView]) -> u64 {
    tables.iter().map(|table| table.num_rows() as u64).sum()
}

fn run_stream_table_segment(
    config: &PerfConfig,
    metadata: PerfReportMetadata,
) -> PerfResult<PerfReport> {
    let row_capacity = config
        .rows_per_batch
        .saturating_mul(64)
        .max(config.rows_per_batch)
        .min(1_048_576);
    run_stream_table_segment_with_row_capacity(config, row_capacity, metadata)
}

fn run_single_row_low_rate(
    config: &PerfConfig,
    metadata: PerfReportMetadata,
) -> PerfResult<PerfReport> {
    let profile_config = single_row_profile_config(config);
    let row_capacity = expected_single_row_capacity(&profile_config)?;
    run_stream_table_segment_with_row_capacity(&profile_config, row_capacity, metadata)
}

fn run_stream_table_segment_with_row_capacity(
    config: &PerfConfig,
    row_capacity: usize,
    metadata: PerfReportMetadata,
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
        late_rows_total: 0,
        filtered_rows_total: 0,
    };
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let pass = evaluate_pass(config, drive_stats.actual_average_rows_per_sec, &latency);

    Ok(PerfReport {
        profile: config.profile,
        config: config.clone(),
        metadata,
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
        engine_metrics,
        pass,
    })
}

fn run_write_to_subscribe_e2e(
    config: &PerfConfig,
    metadata: PerfReportMetadata,
) -> PerfResult<PerfReport> {
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
        late_rows_total: 0,
        filtered_rows_total: 0,
    };
    let latency = build_latency_summary(&drive_stats.latency_samples_micros);
    let pass = evaluate_pass(&profile_config, actual_average_rows_per_sec, &latency);

    Ok(PerfReport {
        profile: profile_config.profile,
        config: profile_config.clone(),
        metadata,
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
        engine_metrics,
        pass,
    })
}

fn run_stream_table_subscriber_e2e(
    config: &PerfConfig,
    metadata: PerfReportMetadata,
) -> PerfResult<PerfReport> {
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
        late_rows_total: 0,
        filtered_rows_total: 0,
    };
    let latency = build_latency_summary(&drive_stats.batch_latencies_micros);
    let secondary_latency = build_latency_summary(&drive_stats.secondary_latencies_micros);
    let pass = evaluate_pass(
        &profile_config,
        drive_stats.actual_average_rows_per_sec,
        &latency,
    );

    Ok(PerfReport {
        profile: profile_config.profile,
        config: profile_config,
        metadata,
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
        engine_metrics,
        pass,
    })
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
    Ok(())
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

fn build_tick_segment_table(
    rows_per_batch: usize,
    symbol_count: usize,
    target_rows_per_sec: u64,
) -> PerfResult<SegmentTableView> {
    let row_count = tick_symbol_buffer_rows(rows_per_batch, symbol_count)?;
    let schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("UTC")),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .map_err(str::to_string)?;
    let layout = LayoutPlan::for_schema(&schema, row_count).map_err(str::to_string)?;
    let mut writer =
        ActiveSegmentWriter::new_for_runtime(schema, layout, 9_001, 0).map_err(str::to_string)?;
    let step_ns = ((1_000_000_000_u64 / target_rows_per_sec).max(1)) as i64;

    for row_index in 0..row_count {
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

    let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, row_count)
        .map_err(str::to_string)?;
    Ok(SegmentTableView::from_row_span(span))
}

fn tick_symbol_buffer_rows(rows_per_batch: usize, symbol_count: usize) -> PerfResult<usize> {
    if rows_per_batch == 0 || symbol_count == 0 {
        return Err("tick batch size and symbol count must be positive".to_string());
    }
    symbol_count
        .checked_add(rows_per_batch - 1)
        .ok_or_else(|| "tick symbol buffer size overflow".to_string())
}

fn tick_segment_batch(
    table: &SegmentTableView,
    config: &PerfConfig,
    input_rows: u64,
) -> PerfResult<SegmentTableView> {
    let start = (input_rows % config.symbols as u64) as usize;
    let span = table
        .as_segment_row_view()
        .ok_or_else(|| "tick segment generator requires a segment-backed table".to_string())?
        .row_span()
        .slice_rows(start, start + config.rows_per_batch)
        .map_err(str::to_string)?;
    Ok(SegmentTableView::from_row_span(span))
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

struct TickBatchFactory {
    schema: SchemaRef,
    rows_per_batch: usize,
    step_ns: i64,
    next_dt_ns: i64,
    symbols: ArrayRef,
    symbol_count: usize,
    next_symbol: usize,
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
        let symbol_rows = tick_symbol_buffer_rows(rows_per_batch, symbol_count)?;
        let symbols = (0..symbol_rows)
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
            symbol_count,
            next_symbol: 0,
            prices: Arc::new(Float64Array::from(prices)) as ArrayRef,
            volumes: Arc::new(Float64Array::from(volumes)) as ArrayRef,
        })
    }

    fn next_batch(&mut self) -> PerfResult<RecordBatch> {
        let timestamps = (0..self.rows_per_batch)
            .map(|offset| self.next_dt_ns + offset as i64 * self.step_ns)
            .collect::<Vec<_>>();
        self.next_dt_ns += self.rows_per_batch as i64 * self.step_ns;
        let symbols = self.symbols.slice(self.next_symbol, self.rows_per_batch);
        self.next_symbol = (self.next_symbol + self.rows_per_batch) % self.symbol_count;

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                symbols,
                Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC"))
                    as ArrayRef,
                Arc::clone(&self.prices),
                Arc::clone(&self.volumes),
            ],
        )
        .map_err(|error| error.to_string())
    }
}

struct DriveStats {
    input_rows_total: u64,
    actual_average_rows_per_sec: f64,
    actual_peak_rows_per_sec: f64,
    batch_latencies_micros: Vec<u64>,
    secondary_latencies_micros: Vec<u64>,
}

struct RowPeakTracker {
    origin: Instant,
    current_bucket_index: u64,
    current_bucket_rows: u64,
    peak_rows: u64,
}

impl RowPeakTracker {
    fn new(origin: Instant) -> Self {
        Self {
            origin,
            current_bucket_index: 0,
            current_bucket_rows: 0,
            peak_rows: 0,
        }
    }

    fn record(&mut self, observed_at: Instant, rows: u64) -> PerfResult<()> {
        let observed_bucket = observed_at
            .checked_duration_since(self.origin)
            .ok_or_else(|| "row peak observation predates origin".to_string())?
            .as_secs();
        if observed_bucket < self.current_bucket_index {
            return Err(format!(
                "row peak tracker received out-of-order bucket current=[{}] observed=[{}]",
                self.current_bucket_index, observed_bucket
            ));
        }
        if observed_bucket > self.current_bucket_index {
            self.peak_rows = self.peak_rows.max(self.current_bucket_rows);
            self.current_bucket_index = observed_bucket;
            self.current_bucket_rows = 0;
        }
        self.current_bucket_rows = self
            .current_bucket_rows
            .checked_add(rows)
            .ok_or_else(|| "row peak bucket row count overflowed".to_string())?;
        Ok(())
    }

    fn finish(self) -> u64 {
        self.peak_rows.max(self.current_bucket_rows)
    }
}

struct EngineDriveStats {
    input_rows_total: u64,
    steady_rows_total: u64,
    steady_batches_total: u64,
    steady_started_at: Instant,
    steady_ends_at: Instant,
    actual_peak_rows_per_sec: f64,
    batch_latencies_micros: Vec<u64>,
}

fn drive_kernel_batch<F, N>(
    process: &mut F,
    batch: RecordBatch,
    started_at: Instant,
    completion_clock: &mut N,
    measured: bool,
    latencies_micros: &mut Vec<u64>,
) -> PerfResult<()>
where
    F: FnMut(RecordBatch) -> PerfResult<()>,
    N: FnMut() -> Instant,
{
    process(batch)?;
    let completed_at = completion_clock();
    if measured {
        latencies_micros.push(completed_at.duration_since(started_at).as_micros() as u64);
    }
    Ok(())
}

fn engine_batch_deadline_offset(
    batch_index: u64,
    rows_per_batch: usize,
    target_rows_per_sec: u64,
) -> PerfResult<Duration> {
    if target_rows_per_sec == 0 {
        return Err("engine drive target_rows_per_sec must be positive".to_string());
    }
    let deadline_ns = u128::from(batch_index)
        .checked_mul(rows_per_batch as u128)
        .and_then(|rows| rows.checked_mul(1_000_000_000))
        .ok_or_else(|| {
            format!(
                "engine drive deadline overflowed batch_index=[{batch_index}] \
                 rows_per_batch=[{rows_per_batch}]"
            )
        })?
        / u128::from(target_rows_per_sec);
    let deadline_ns = u64::try_from(deadline_ns).map_err(|_| {
        format!("engine drive deadline nanoseconds overflowed u64 deadline_ns=[{deadline_ns}]")
    })?;
    Ok(Duration::from_nanos(deadline_ns))
}

fn should_admit_engine_batch(batch_start: Instant, end: Instant) -> bool {
    batch_start < end
}

fn is_engine_drive_measured(
    batch_start: Instant,
    steady_started_at: Instant,
    end: Instant,
) -> bool {
    batch_start >= steady_started_at && should_admit_engine_batch(batch_start, end)
}

fn drive_engine_generator<F>(
    config: &PerfConfig,
    factory: &mut TickBatchFactory,
    process: &mut F,
) -> PerfResult<EngineDriveStats>
where
    F: FnMut(RecordBatch) -> PerfResult<()>,
{
    let warmup_duration = Duration::from_secs(config.warmup_sec);
    let steady_duration = Duration::from_secs(config.duration_sec);
    let total_duration = warmup_duration
        .checked_add(steady_duration)
        .ok_or_else(|| "engine drive total duration overflowed".to_string())?;
    let start = Instant::now();
    let steady_started_at = start
        .checked_add(warmup_duration)
        .ok_or_else(|| "engine drive steady start overflowed Instant".to_string())?;
    let end = start
        .checked_add(total_duration)
        .ok_or_else(|| "engine drive end overflowed Instant".to_string())?;
    let mut batch_index = 0_u64;
    let mut input_rows_total = 0_u64;
    let mut steady_rows_total = 0_u64;
    let mut steady_batches_total = 0_u64;
    let mut peak_tracker = RowPeakTracker::new(steady_started_at);
    let mut batch_latencies_micros = Vec::new();
    let mut write_completion_clock = Instant::now;

    loop {
        let deadline_offset = engine_batch_deadline_offset(
            batch_index,
            config.rows_per_batch,
            config.target_rows_per_sec,
        )?;
        if deadline_offset >= total_duration {
            break;
        }
        let deadline = start
            .checked_add(deadline_offset)
            .ok_or_else(|| "engine drive deadline overflowed Instant".to_string())?;
        let now = Instant::now();
        if deadline > now {
            thread::sleep(deadline - now);
        }
        let batch = factory.next_batch()?;
        let batch_start = Instant::now();
        if !should_admit_engine_batch(batch_start, end) {
            break;
        }
        let measured = is_engine_drive_measured(batch_start, steady_started_at, end);
        drive_kernel_batch(
            process,
            batch,
            batch_start,
            &mut write_completion_clock,
            measured,
            &mut batch_latencies_micros,
        )?;
        input_rows_total += config.rows_per_batch as u64;
        if measured {
            steady_rows_total += config.rows_per_batch as u64;
            steady_batches_total += 1;
            peak_tracker.record(batch_start, config.rows_per_batch as u64)?;
        }
        batch_index = batch_index
            .checked_add(1)
            .ok_or_else(|| "engine drive batch index overflowed".to_string())?;
    }

    let peak_rows_per_sec = peak_tracker.finish() as f64;

    Ok(EngineDriveStats {
        input_rows_total,
        steady_rows_total,
        steady_batches_total,
        steady_started_at,
        steady_ends_at: end,
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
    let mut peak_tracker = RowPeakTracker::new(warmup_end);
    let mut batch_latencies_micros = Vec::new();

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let batch_start = Instant::now();
        send_table(tick_segment_batch(table, config, input_rows_total)?)?;
        let send_elapsed = batch_start.elapsed();
        input_rows_total += config.rows_per_batch as u64;
        if batch_start >= warmup_end {
            steady_rows_total += config.rows_per_batch as u64;
            batch_latencies_micros.push(send_elapsed.as_micros() as u64);
            peak_tracker.record(batch_start, config.rows_per_batch as u64)?;
        }
        next_tick += batch_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    let peak_rows_per_sec = peak_tracker.finish() as f64;
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
    let mut peak_tracker = RowPeakTracker::new(warmup_end);
    let mut batch_latencies_micros = Vec::new();
    let mut secondary_latencies_micros = Vec::new();

    while Instant::now() < end {
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        }
        let sample_start = Instant::now();
        let on_data_start = Instant::now();
        materializer
            .on_data(tick_segment_batch(table, config, input_rows_total)?)
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
            peak_tracker.record(sample_start, observed_rows)?;
        }
        next_tick += row_interval;
        if Instant::now() - start >= total_duration {
            break;
        }
    }

    let peak_rows_per_sec = peak_tracker.finish() as f64;
    let actual_average_rows_per_sec = steady_rows_total as f64 / steady_duration.as_secs_f64();

    Ok(DriveStats {
        input_rows_total,
        actual_average_rows_per_sec,
        actual_peak_rows_per_sec: peak_rows_per_sec,
        batch_latencies_micros,
        secondary_latencies_micros,
    })
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

fn compute_batches_per_sec(output_batches_total: u64, observed_secs: f64) -> f64 {
    output_batches_total as f64 / observed_secs.max(1e-9)
}

fn count_per_sec_over_elapsed(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(1e-9)
}

fn inproc_processing_elapsed(
    steady_started_at: Instant,
    steady_ends_at: Instant,
    drain_completed_at: Instant,
) -> Duration {
    steady_ends_at
        .max(drain_completed_at)
        .duration_since(steady_started_at)
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

fn evaluate_pass(config: &PerfConfig, average_rows_per_sec: f64, latency: &LatencySummary) -> bool {
    if average_rows_per_sec < config.target_rows_per_sec as f64 * MIN_PASS_RATIO {
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
    true
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, OnceLock};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::{
        checked_write_to_subscribe_elapsed_ns, consume_write_to_subscribe_rows,
        consume_write_to_subscribe_span, count_per_sec_over_elapsed,
        drive_segment_write_to_consumer, engine_batch_deadline_offset, inproc_processing_elapsed,
        is_engine_drive_measured, is_write_to_subscribe_measured, merge_write_to_subscribe_results,
        merge_write_to_subscribe_startup_error, produce_write_to_subscribe_rows, run_profile,
        run_profile_with_hooks, should_admit_engine_batch, validate_inproc_completion,
        validate_write_to_subscribe_counts, write_report_json,
        write_to_subscribe_consumer_complete, write_to_subscribe_deadline_offset, BaselineManifest,
        BaselineRunState, ConsumerJoinGuard, ConsumerStats, EngineMetricsReport, LatencySummary,
        PerfBinaryIdentity, PerfConfig, PerfDatasetManifest, PerfDatasetParameters, PerfProfile,
        PerfReport, PerfReportMetadata, PerfSourceIdentity, ProducerOutcome, ProducerState,
        RowPeakTracker, SteadyPeakTracker, WriteToSubscribeTiming,
        BASELINE_MANIFEST_FORMAT_VERSION, SINGLE_NODE_REFACTOR_SUITE,
    };
    use zippy_segment_store::{
        compile_schema, ActiveSegmentReader, ActiveSegmentWriter, ColumnSpec, ColumnType,
        LayoutPlan,
    };

    fn test_config(profile: PerfProfile) -> PerfConfig {
        PerfConfig {
            profile,
            rows_per_batch: 256,
            target_rows_per_sec: 10_000,
            duration_sec: 1,
            warmup_sec: 0,
            symbols: 32,
            max_p95_micros: None,
            max_p99_micros: None,
        }
    }

    #[test]
    fn tick_generators_cover_all_symbols_across_batch_boundaries() {
        use arrow::array::{Array, StringArray};
        for (rows, symbols) in [(64, 10_000), (8, 3), (1, 1)] {
            let mut config = test_config(PerfProfile::TimeseriesKernel);
            config.rows_per_batch = rows;
            config.symbols = symbols;
            let mut factory = super::TickBatchFactory::new(
                super::build_tick_schema(),
                rows,
                symbols,
                config.target_rows_per_sec,
            )
            .unwrap();
            let table =
                super::build_tick_segment_table(rows, symbols, config.target_rows_per_sec).unwrap();
            let mut observed = std::collections::BTreeSet::new();
            for batch_index in 0..(symbols * 2).div_ceil(rows) {
                let batch = factory.next_batch().unwrap();
                let segment =
                    super::tick_segment_batch(&table, &config, (batch_index * rows) as u64)
                        .unwrap();
                let column = segment.column("symbol").unwrap();
                let segment_symbols = column.as_any().downcast_ref::<StringArray>().unwrap();
                let batch_symbols = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(segment.num_rows(), rows);
                for row in 0..rows {
                    let expected = format!("S{:04}", (batch_index * rows + row) % symbols);
                    assert_eq!(batch_symbols.value(row), expected);
                    assert_eq!(segment_symbols.value(row), expected);
                    observed.insert(expected);
                }
            }
            assert_eq!(observed.len(), symbols);
        }
    }

    fn empty_report(config: &PerfConfig, metadata: PerfReportMetadata) -> PerfReport {
        PerfReport {
            profile: config.profile,
            config: config.clone(),
            metadata,
            input_rows_total: 0,
            output_rows_total: 0,
            actual_average_rows_per_sec: 0.0,
            actual_peak_rows_per_sec: 0.0,
            batches_per_sec: 0.0,
            latency: LatencySummary::default(),
            secondary_latency: None,
            engine_metrics: EngineMetricsReport::default(),
            pass: true,
        }
    }

    #[test]
    fn profile_report_uses_metadata_captured_before_dispatch() {
        let config = test_config(PerfProfile::TimeseriesKernel);
        let logical_time = Cell::new(1_000_u128);

        let report = run_profile_with_hooks(
            &config,
            || PerfReportMetadata {
                git_sha: "sentinel-sha".to_string(),
                target: "sentinel-target".to_string(),
                started_at_unix_ms: logical_time.get(),
            },
            |config, metadata| {
                logical_time.set(9_000);
                Ok(empty_report(config, metadata))
            },
        )
        .unwrap();

        assert_eq!(logical_time.get(), 9_000);
        assert_eq!(report.metadata.started_at_unix_ms, 1_000);
        assert_eq!(report.metadata.git_sha, "sentinel-sha");
        assert_eq!(report.metadata.target, "sentinel-target");
    }

    #[test]
    fn invalid_profile_config_skips_metadata_capture_and_dispatch() {
        let mut config = test_config(PerfProfile::TimeseriesKernel);
        config.rows_per_batch = 0;
        let capture_called = Cell::new(false);
        let dispatch_called = Cell::new(false);

        let error = run_profile_with_hooks(
            &config,
            || {
                capture_called.set(true);
                PerfReportMetadata {
                    git_sha: "unused-sha".to_string(),
                    target: "unused-target".to_string(),
                    started_at_unix_ms: 0,
                }
            },
            |config, metadata| {
                dispatch_called.set(true);
                Ok(empty_report(config, metadata))
            },
        )
        .unwrap_err();

        assert_eq!(error, "rows_per_batch must be greater than zero");
        assert!(!capture_called.get());
        assert!(!dispatch_called.get());
    }

    #[test]
    fn baseline_manifest_serializes_build_config_dataset_and_completed_report() {
        let config = test_config(PerfProfile::TimeseriesKernel);
        let metadata = PerfReportMetadata {
            git_sha: "0123456789ab".to_string(),
            target: "linux-x86_64".to_string(),
            started_at_unix_ms: 1_000,
        };
        let manifest = BaselineManifest {
            format_version: BASELINE_MANIFEST_FORMAT_VERSION,
            suite: SINGLE_NODE_REFACTOR_SUITE.to_string(),
            state: BaselineRunState::Planned,
            metadata: metadata.clone(),
            source: PerfSourceIdentity {
                git_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
                git_dirty: true,
            },
            binary: PerfBinaryIdentity {
                binary_name: "zippy-perf".to_string(),
                package_version: "0.5.0".to_string(),
                build_profile: "release".to_string(),
                target: "linux-x86_64".to_string(),
                executable_path: PathBuf::from("/opt/zippy/bin/zippy-perf"),
                executable_size_bytes: 42,
                executable_sha256:
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            },
            config: config.clone(),
            dataset: PerfDatasetManifest {
                dataset_id: "fixture.tick.v1".to_string(),
                source_kind: "synthetic".to_string(),
                input_contract: "zippy-perf.tick.v1".to_string(),
                parameters: PerfDatasetParameters {
                    rows_per_batch: config.rows_per_batch,
                    target_rows_per_sec: config.target_rows_per_sec,
                    duration_sec: config.duration_sec,
                    warmup_sec: config.warmup_sec,
                    symbols: config.symbols,
                },
            },
            report: None,
            error: None,
        };
        let completed = manifest
            .completed(&empty_report(&config, metadata))
            .unwrap();
        let json = serde_json::to_value(completed).unwrap();

        assert_eq!(json["format_version"], 2);
        assert_eq!(json["suite"], "single-node-refactor");
        assert_eq!(json["state"], "completed");
        assert_eq!(json["metadata"]["git_sha"], "0123456789ab");
        assert_eq!(json["source"]["git_dirty"], true);
        assert_eq!(json["binary"]["binary_name"], "zippy-perf");
        assert_eq!(json["binary"]["build_profile"], "release");
        assert_eq!(json["binary"]["executable_size_bytes"], 42);
        assert_eq!(json["config"]["rows_per_batch"], config.rows_per_batch);
        assert_eq!(json["dataset"]["dataset_id"], "fixture.tick.v1");
        assert_eq!(json["dataset"]["parameters"]["symbols"], config.symbols);
        assert!(json["report"].is_object());
        assert!(json["error"].is_null());
    }

    #[test]
    fn executable_sha256_matches_standard_known_vector() {
        assert_eq!(
            super::sha256_reader(Cursor::new(b"a")).unwrap(),
            "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb"
        );
    }

    #[test]
    fn source_identity_preserves_full_git_sha_and_dirty_state() {
        let git_sha = "0123456789abcdef0123456789abcdef01234567";
        let clean = super::parse_git_source_identity(git_sha.to_string(), "false").unwrap();
        let dirty = super::parse_git_source_identity(git_sha.to_string(), "true").unwrap();

        assert_eq!(clean.git_sha, git_sha);
        assert!(!clean.git_dirty);
        assert_eq!(dirty.git_sha, git_sha);
        assert!(dirty.git_dirty);
    }

    #[test]
    fn pretty_json_replacement_is_atomic_and_removes_temporary_file() {
        let output_dir = std::env::temp_dir().join(format!(
            "zippy-perf-atomic-json-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = output_dir.join("manifest.json");

        super::write_pretty_json(&path, &serde_json::json!({"state": "planned"})).unwrap();
        super::write_pretty_json(&path, &serde_json::json!({"state": "completed"})).unwrap();

        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(json["state"], "completed");
        let entries = std::fs::read_dir(&output_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec![std::ffi::OsString::from("manifest.json")]);

        std::fs::remove_dir_all(output_dir).unwrap();
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
    fn timeseries_kernel_profile_produces_nonzero_rows_and_passes() {
        let report = run_profile(&test_config(PerfProfile::TimeseriesKernel)).unwrap();

        assert!(report.input_rows_total > 0);
        assert_eq!(
            report.input_rows_total,
            report.engine_metrics.processed_rows_total
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
    fn inproc_processing_elapsed_preserves_full_steady_window_before_drain() {
        let steady_started_at = Instant::now();
        let steady_ends_at = steady_started_at + Duration::from_secs(3);
        let drain_completed_at = steady_started_at + Duration::from_secs(2);

        assert_eq!(
            inproc_processing_elapsed(steady_started_at, steady_ends_at, drain_completed_at),
            Duration::from_secs(3),
        );
    }

    #[test]
    fn inproc_processing_elapsed_extends_through_late_drain() {
        let steady_started_at = Instant::now();
        let steady_ends_at = steady_started_at + Duration::from_secs(3);
        let drain_completed_at = steady_started_at + Duration::from_secs(4);

        assert_eq!(
            inproc_processing_elapsed(steady_started_at, steady_ends_at, drain_completed_at),
            Duration::from_secs(4),
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
    fn engine_drive_rational_deadline_excludes_first_batch_after_total_window() {
        let total_duration = Duration::from_secs(4);

        let last_inside = engine_batch_deadline_offset(97, 4_096, 100_000).unwrap();
        let first_outside = engine_batch_deadline_offset(98, 4_096, 100_000).unwrap();

        assert_eq!(last_inside, Duration::from_nanos(3_973_120_000));
        assert_eq!(first_outside, Duration::from_nanos(4_014_080_000));
        assert!(last_inside < total_duration);
        assert!(first_outside >= total_duration);
    }

    #[test]
    fn engine_drive_measurement_window_is_half_open() {
        let steady_started_at = Instant::now();
        let end = steady_started_at + Duration::from_secs(3);

        assert!(is_engine_drive_measured(
            steady_started_at,
            steady_started_at,
            end,
        ));
        assert!(is_engine_drive_measured(
            end - Duration::from_nanos(1),
            steady_started_at,
            end,
        ));
        assert!(!is_engine_drive_measured(end, steady_started_at, end,));
    }

    #[test]
    fn engine_drive_does_not_admit_after_sleep_crosses_end() {
        let end = Instant::now() + Duration::from_secs(1);

        assert!(should_admit_engine_batch(
            end - Duration::from_nanos(1),
            end,
        ));
        assert!(!should_admit_engine_batch(end, end));
        assert!(!should_admit_engine_batch(
            end + Duration::from_nanos(1),
            end,
        ));
    }

    #[test]
    fn row_peak_tracker_accumulates_rows_within_absolute_second_bucket() {
        let origin = Instant::now();
        let mut tracker = RowPeakTracker::new(origin);

        tracker.record(origin, 4_096).unwrap();
        tracker
            .record(origin + Duration::from_millis(900), 4_096)
            .unwrap();

        assert_eq!(tracker.finish(), 8_192);
    }

    #[test]
    fn row_peak_tracker_rolls_before_counting_current_batch() {
        let origin = Instant::now();
        let mut tracker = RowPeakTracker::new(origin);

        tracker.record(origin, 4_096).unwrap();
        tracker
            .record(origin + Duration::from_millis(1_100), 4_096)
            .unwrap();

        assert_eq!(tracker.finish(), 4_096);
    }

    #[test]
    fn row_peak_tracker_uses_exact_second_boundary_and_skips_empty_buckets() {
        let origin = Instant::now();
        let mut tracker = RowPeakTracker::new(origin);

        tracker.record(origin, 10).unwrap();
        tracker.record(origin + Duration::from_secs(1), 20).unwrap();
        tracker.record(origin + Duration::from_secs(4), 15).unwrap();

        assert_eq!(tracker.finish(), 20);
    }

    #[test]
    fn row_peak_tracker_rejects_pre_origin_and_out_of_order_buckets() {
        let origin = Instant::now();
        let mut tracker = RowPeakTracker::new(origin);
        let before_origin = origin.checked_sub(Duration::from_nanos(1)).unwrap();

        assert_eq!(
            tracker.record(before_origin, 1).unwrap_err(),
            "row peak observation predates origin"
        );
        tracker.record(origin + Duration::from_secs(2), 1).unwrap();
        assert_eq!(
            tracker
                .record(origin + Duration::from_secs(1), 1)
                .unwrap_err(),
            "row peak tracker received out-of-order bucket current=[2] observed=[1]"
        );
    }

    #[test]
    fn row_peak_tracker_rejects_bucket_row_overflow() {
        let origin = Instant::now();
        let mut tracker = RowPeakTracker::new(origin);

        tracker.record(origin, u64::MAX).unwrap();

        assert_eq!(
            tracker.record(origin, 1).unwrap_err(),
            "row peak bucket row count overflowed"
        );
    }

    #[test]
    fn empty_row_peak_tracker_finishes_at_zero() {
        assert_eq!(RowPeakTracker::new(Instant::now()).finish(), 0);
    }

    #[test]
    fn stream_table_segment_copy_profile_produces_nonzero_rows_and_passes() {
        let report = run_profile(&test_config(PerfProfile::StreamTableSegmentCopy)).unwrap();
        assert!(report.input_rows_total > 0);
        assert!(report.actual_average_rows_per_sec > 0.0);
        assert!(report.pass);
    }

    #[test]
    fn single_row_low_rate_profile_produces_nonzero_rows_and_passes() {
        let mut config = test_config(PerfProfile::SingleRowLowRate);
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
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e);
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
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e);
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
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e);
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
    fn stream_table_subscriber_e2e_reports_target_reader_latency() {
        let mut config = test_config(PerfProfile::StreamTableSubscriberE2e);
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
        let mut config = test_config(PerfProfile::WriteToSubscribeE2e);
        config.rows_per_batch = 1;
        config.target_rows_per_sec = 1_000_000;
        config.duration_sec = 60;

        let error = run_profile(&config).unwrap_err();

        assert!(error.contains("expected_rows"));
        assert!(error.contains("lower duration_sec or target_rows_per_sec"));
    }

    #[test]
    fn writes_report_json_with_profile_and_pass_fields() {
        let report = run_profile(&test_config(PerfProfile::TimeseriesKernel)).unwrap();
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
    fn evaluate_pass_rejects_latency_above_configured_p99_threshold() {
        let mut config = test_config(PerfProfile::TimeseriesKernel);
        config.max_p99_micros = Some(100.0);
        let latency = LatencySummary {
            sample_count: 1,
            p50_micros: 10.0,
            p95_micros: 50.0,
            p99_micros: 101.0,
            max_micros: 101.0,
        };

        let pass = super::evaluate_pass(&config, 10_000.0, &latency);

        assert!(!pass);
    }

    #[test]
    fn evaluate_pass_rejects_latency_above_configured_p95_threshold() {
        let mut config = test_config(PerfProfile::TimeseriesKernel);
        config.max_p95_micros = Some(50.0);
        let latency = LatencySummary {
            sample_count: 1,
            p50_micros: 10.0,
            p95_micros: 50.1,
            p99_micros: 80.0,
            max_micros: 80.0,
        };

        let pass = super::evaluate_pass(&config, 10_000.0, &latency);

        assert!(!pass);
    }
    #[test]
    fn kernel_latency_includes_processing_and_excludes_unmeasured_batches() {
        let processed = Cell::new(false);
        let start = Instant::now();
        let mut process = |_| {
            processed.set(true);
            Ok(())
        };
        let mut clock = || {
            assert!(processed.get());
            start + Duration::from_micros(7)
        };
        let batch = arrow::record_batch::RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        ));
        let mut samples = Vec::new();
        super::drive_kernel_batch(
            &mut process,
            batch.clone(),
            start,
            &mut clock,
            false,
            &mut samples,
        )
        .unwrap();
        assert!(samples.is_empty());
        super::drive_kernel_batch(&mut process, batch, start, &mut clock, true, &mut samples)
            .unwrap();
        assert_eq!(samples, vec![7]);
    }

    #[test]
    fn failed_kernel_batch_does_not_produce_success_latency() {
        let mut process = |_| Err("kernel rejected input".to_string());
        let mut clock = || panic!("failed work must not be sampled");
        let batch = arrow::record_batch::RecordBatch::new_empty(Arc::new(
            arrow::datatypes::Schema::empty(),
        ));
        let mut samples = Vec::new();
        let error = super::drive_kernel_batch(
            &mut process,
            batch,
            Instant::now(),
            &mut clock,
            true,
            &mut samples,
        )
        .unwrap_err();
        assert_eq!(error, "kernel rejected input");
        assert!(samples.is_empty());
    }

    #[test]
    fn performance_contract_rejects_retired_profiles_and_queue_configuration() {
        for profile in [
            "InprocTimeseries",
            "RemotePipelineUpstream",
            "RemotePipelineDownstream",
            "RuntimeStreamTableE2e",
            "StreamTableSegmentForward",
        ] {
            assert!(serde_json::from_value::<PerfProfile>(serde_json::json!(profile)).is_err());
        }
        let config = test_config(PerfProfile::TimeseriesKernel);
        for (field, value) in [
            ("endpoint", serde_json::json!("tcp://127.0.0.1:5560")),
            ("buffer_capacity", serde_json::json!(1024)),
            ("overflow_policy", serde_json::json!("Block")),
            ("max_queue_depth", serde_json::json!(64)),
        ] {
            let mut wire = serde_json::to_value(&config).unwrap();
            wire[field] = value;
            assert!(
                serde_json::from_value::<PerfConfig>(wire).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn latency_contract_requires_complete_current_measurements() {
        for field in ["sample_count", "max_micros"] {
            let mut wire = serde_json::to_value(LatencySummary::default()).unwrap();
            wire.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<LatencySummary>(wire).is_err(),
                "{field}"
            );
        }
        let mut wire = serde_json::to_value(EngineMetricsReport::default()).unwrap();
        wire["queue_depth"] = serde_json::json!(0);
        assert!(serde_json::from_value::<EngineMetricsReport>(wire).is_err());
    }
}
