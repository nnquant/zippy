use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use tracing::{error, info};
use zippy_core::{setup_log, LogConfig};
use zippy_perf::{
    format_report, plan_baseline_manifest, run_profile, write_baseline_manifest_json,
    write_report_json, BaselineManifest, PerfConfig, PerfProfile,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliProfile {
    TimeseriesKernel,
    StreamTableSegmentCopy,
    SingleRowLowRate,
    SegmentWriteToRead,
    StreamTableSubscriberE2e,
}

#[derive(Debug, Parser)]
#[command(name = "zippy-perf")]
#[command(about = "performance harness for synchronous kernels and native shared-memory storage")]
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
    #[arg(long)]
    max_p95_micros: Option<f64>,
    #[arg(long)]
    max_p99_micros: Option<f64>,
    #[arg(long)]
    report_json: Option<PathBuf>,
    #[arg(long)]
    baseline_manifest_json: Option<PathBuf>,
    #[arg(long)]
    dataset_id: Option<String>,
    #[arg(long, requires = "baseline_manifest_json")]
    dry_run: bool,
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
            CliProfile::TimeseriesKernel => PerfProfile::TimeseriesKernel,
            CliProfile::StreamTableSegmentCopy => PerfProfile::StreamTableSegmentCopy,
            CliProfile::SingleRowLowRate => PerfProfile::SingleRowLowRate,
            CliProfile::SegmentWriteToRead => PerfProfile::SegmentWriteToRead,
            CliProfile::StreamTableSubscriberE2e => PerfProfile::StreamTableSubscriberE2e,
        },
        rows_per_batch: cli.rows_per_batch,
        target_rows_per_sec: cli.target_rows_per_sec,
        duration_sec: cli.duration_sec,
        warmup_sec: cli.warmup_sec,
        symbols: cli.symbols,
        max_p95_micros: cli.max_p95_micros,
        max_p99_micros: cli.max_p99_micros,
    };

    let planned_manifest = match cli.baseline_manifest_json.as_ref() {
        Some(path) => {
            let manifest = match plan_baseline_manifest(&config, cli.dataset_id.as_deref()) {
                Ok(manifest) => manifest,
                Err(error) => {
                    error!(
                        component = "perf",
                        event = "baseline_manifest_plan_error",
                        error = %error,
                        "baseline manifest planning failed"
                    );
                    std::process::exit(1);
                }
            };
            if let Err(error) = write_baseline_manifest_json(path, &manifest) {
                error!(
                    component = "perf",
                    event = "baseline_manifest_write_error",
                    error = %error,
                    path = %path.display(),
                    "baseline manifest write failed"
                );
                std::process::exit(1);
            }
            Some(manifest)
        }
        None => None,
    };

    if cli.dry_run {
        println!(
            "baseline_manifest={}",
            cli.baseline_manifest_json.as_ref().unwrap().display()
        );
        return;
    }

    info!(
        component = "perf",
        event = "profile_start",
        profile = ?config.profile,
        "performance profile started"
    );
    let report = match run_profile(&config) {
        Ok(report) => report,
        Err(error) => {
            write_failed_baseline_manifest(
                cli.baseline_manifest_json.as_deref(),
                planned_manifest.as_ref(),
                &error,
            );
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
            write_failed_baseline_manifest(
                cli.baseline_manifest_json.as_deref(),
                planned_manifest.as_ref(),
                &format!("performance report write failed: {error}"),
            );
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

    if let (Some(path), Some(manifest)) = (
        cli.baseline_manifest_json.as_ref(),
        planned_manifest.as_ref(),
    ) {
        let manifest = match manifest.clone().completed(&report) {
            Ok(manifest) => manifest,
            Err(error) => {
                error!(
                    component = "perf",
                    event = "baseline_manifest_complete_error",
                    error = %error,
                    "baseline manifest completion failed"
                );
                std::process::exit(1);
            }
        };
        if let Err(error) = write_baseline_manifest_json(path, &manifest) {
            error!(
                component = "perf",
                event = "baseline_manifest_write_error",
                error = %error,
                path = %path.display(),
                "completed baseline manifest write failed"
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

fn write_failed_baseline_manifest(
    path: Option<&std::path::Path>,
    manifest: Option<&BaselineManifest>,
    profile_error: &str,
) {
    let (Some(path), Some(manifest)) = (path, manifest) else {
        return;
    };
    let failed = manifest.clone().failed(profile_error);
    if let Err(error) = write_baseline_manifest_json(path, &failed) {
        error!(
            component = "perf",
            event = "baseline_manifest_write_error",
            error = %error,
            path = %path.display(),
            "failed baseline manifest write failed"
        );
    }
}

#[cfg(test)]
mod tests {
    use clap::{error::ErrorKind, Parser};

    use super::{Cli, CliProfile};

    #[test]
    fn segment_write_to_read_cli_accepts_new_profile_name() {
        let cli = Cli::try_parse_from(["zippy-perf", "segment-write-to-read"]).unwrap();

        assert_eq!(cli.profile, CliProfile::SegmentWriteToRead);
    }

    #[test]
    fn segment_write_to_read_cli_rejects_retired_profile_name() {
        let error = Cli::try_parse_from(["zippy-perf", "write-to-subscribe-e2e"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidValue);
        assert!(error.to_string().contains("segment-write-to-read"));
    }
}
