use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use tracing::{error, info};
use zippy_core::{setup_log, LogConfig};
use zippy_perf::{
    format_report, run_profile, write_report_json, OverflowPolicyConfig, PerfConfig, PerfProfile,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliProfile {
    InprocTimeseries,
    RemotePipelineUpstream,
    RemotePipelineDownstream,
    StreamTableSegmentCopy,
    StreamTableSegmentForward,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliOverflowPolicy {
    Block,
    Reject,
    DropOldest,
}

#[derive(Debug, Parser)]
#[command(name = "zippy-perf")]
#[command(about = "performance harness for sustained in-process and remote zippy pipelines")]
struct Cli {
    #[arg(value_enum)]
    profile: CliProfile,
    #[arg(long, default_value_t = 4096)]
    rows_per_batch: usize,
    #[arg(long, default_value_t = 1_000_000)]
    target_rows_per_sec: u64,
    #[arg(long, default_value_t = 60)]
    duration_sec: u64,
    #[arg(long, default_value_t = 10)]
    warmup_sec: u64,
    #[arg(long, default_value_t = 1024)]
    symbols: usize,
    #[arg(long, default_value_t = 4096)]
    buffer_capacity: usize,
    #[arg(long, value_enum, default_value_t = CliOverflowPolicy::Block)]
    overflow_policy: CliOverflowPolicy,
    #[arg(long, default_value = "tcp://127.0.0.1:5560")]
    endpoint: String,
    #[arg(long)]
    report_json: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let _ = setup_log(LogConfig::new(
        "zippy_perf",
        "info",
        PathBuf::from("logs"),
        true,
        true,
    ));
    let config = PerfConfig {
        profile: match cli.profile {
            CliProfile::InprocTimeseries => PerfProfile::InprocTimeseries,
            CliProfile::RemotePipelineUpstream => PerfProfile::RemotePipelineUpstream,
            CliProfile::RemotePipelineDownstream => PerfProfile::RemotePipelineDownstream,
            CliProfile::StreamTableSegmentCopy => PerfProfile::StreamTableSegmentCopy,
            CliProfile::StreamTableSegmentForward => PerfProfile::StreamTableSegmentForward,
        },
        rows_per_batch: cli.rows_per_batch,
        target_rows_per_sec: cli.target_rows_per_sec,
        duration_sec: cli.duration_sec,
        warmup_sec: cli.warmup_sec,
        symbols: cli.symbols,
        endpoint: cli.endpoint,
        buffer_capacity: cli.buffer_capacity,
        overflow_policy: match cli.overflow_policy {
            CliOverflowPolicy::Block => OverflowPolicyConfig::Block,
            CliOverflowPolicy::Reject => OverflowPolicyConfig::Reject,
            CliOverflowPolicy::DropOldest => OverflowPolicyConfig::DropOldest,
        },
    };

    info!(
        component = "perf",
        event = "profile_start",
        profile = ?config.profile,
        "performance profile started"
    );
    let report = match run_profile(&config) {
        Ok(report) => report,
        Err(error) => {
            error!(
                component = "perf",
                event = "profile_error",
                error = %error,
                "performance profile failed"
            );
            std::process::exit(1);
        }
    };

    println!("{}", format_report(&report));

    if let Some(path) = cli.report_json.as_ref() {
        if let Err(error) = write_report_json(path, &report) {
            error!(
                component = "perf",
                event = "report_write_error",
                error = %error,
                path = %path.display(),
                "performance report write failed"
            );
            std::process::exit(1);
        }
    }

    info!(
        component = "perf",
        event = "profile_finish",
        profile = ?report.profile,
        pass = report.pass,
        "performance profile finished"
    );

    if !report.pass {
        std::process::exit(2);
    }
}
