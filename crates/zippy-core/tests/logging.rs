use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const LOGGING_CASE_ENV: &str = "ZIPPY_LOGGING_TEST_CASE";
const LOGGING_TEMP_ENV: &str = "ZIPPY_LOGGING_TEST_TEMP";

#[test]
fn logging_case_dispatch() {
    let Ok(case) = env::var(LOGGING_CASE_ENV) else {
        return;
    };
    let temp_dir = PathBuf::from(env::var(LOGGING_TEMP_ENV).unwrap());

    match case.as_str() {
        "create_snapshot" => create_snapshot_case(&temp_dir),
        "reuse_snapshot" => reuse_snapshot_case(&temp_dir),
        "reject_different_config" => reject_different_config_case(&temp_dir),
        other => panic!("unknown logging test case: {other}"),
    }
}

#[test]
fn setup_log_creates_jsonl_file_and_returns_snapshot() {
    run_logging_case("create_snapshot");
}

#[test]
fn setup_log_reuses_existing_snapshot_without_creating_extra_files() {
    run_logging_case("reuse_snapshot");
}

#[test]
fn setup_log_rejects_different_config_without_creating_extra_files() {
    run_logging_case("reject_different_config");
}

fn run_logging_case(case: &str) {
    let temp = tempfile::tempdir().unwrap();
    let current_exe = env::current_exe().unwrap();
    let status = Command::new(current_exe)
        .arg("--exact")
        .arg("logging_case_dispatch")
        .env(LOGGING_CASE_ENV, case)
        .env(LOGGING_TEMP_ENV, temp.path())
        .status()
        .unwrap();

    assert!(status.success(), "logging child case failed case=[{case}]");
}

fn create_snapshot_case(temp_dir: &Path) {
    let snapshot = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp_dir,
        true,
        true,
    ))
    .unwrap();

    assert_eq!(snapshot.app, "runtime_test");
    assert_eq!(snapshot.level, "info");
    assert!(!snapshot.run_id.is_empty());

    let file_path = snapshot.file_path.clone().expect("missing file path");
    assert!(!file_path.exists());

    tracing::info!(
        component = "logging_test",
        event = "create_snapshot",
        "created logging snapshot"
    );

    assert!(file_path.exists());
    assert!(file_path.starts_with(temp_dir.join("runtime_test")));
    let file_name = file_path.file_name().unwrap().to_string_lossy();
    assert!(file_name.ends_with(".jsonl"));
    assert!(file_name.contains('_'));
    let (date_part, run_part) = file_name.trim_end_matches(".jsonl").split_once('_').unwrap();
    assert_eq!(date_part.len(), 10);
    assert_eq!(&date_part[4..5], "-");
    assert_eq!(&date_part[7..8], "-");
    assert_eq!(run_part, snapshot.run_id);

    let active_snapshot = zippy_core::current_log_snapshot().expect("missing active snapshot");
    assert_eq!(active_snapshot.app, snapshot.app);
    assert_eq!(active_snapshot.run_id, snapshot.run_id);
    assert_eq!(active_snapshot.file_path, snapshot.file_path);

    let contents = std::fs::read_to_string(&file_path).expect("missing log contents");
    let record: Value = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
    assert_eq!(record["app"], snapshot.app);
    assert_eq!(record["run_id"], snapshot.run_id);
    assert_eq!(record["component"], "logging_test");
    assert_eq!(record["event"], "create_snapshot");
    assert_eq!(record["message"], "created logging snapshot");
}

fn reuse_snapshot_case(temp_dir: &Path) {
    let first = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp_dir,
        true,
        true,
    ))
    .unwrap();
    let second = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp_dir,
        true,
        true,
    ))
    .unwrap();

    assert_eq!(first, second);

    let file_path = first.file_path.expect("missing file path");
    tracing::info!(
        component = "logging_test",
        event = "reuse_snapshot",
        "reused logging snapshot"
    );
    assert!(file_path.exists());

    let file_count = std::fs::read_dir(temp_dir.join("runtime_test"))
        .unwrap()
        .count();
    assert_eq!(file_count, 1);
}

fn reject_different_config_case(temp_dir: &Path) {
    let first = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp_dir,
        true,
        true,
    ))
    .unwrap();

    let second_app_dir = temp_dir.join("runtime_other");
    let error = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_other",
        "info",
        temp_dir,
        true,
        true,
    ))
    .unwrap_err();

    let error_message = error.to_string();
    assert!(error_message.contains("logging already initialized"));
    let file_path = first.file_path.expect("missing first file path");
    assert!(!file_path.exists());
    assert!(!second_app_dir.exists());
}
