#![cfg(feature = "test-failpoints")]

use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use zippy_segment_store::{
    test_failpoints::{ACTIVE_FAILPOINT_ENV, PERSISTENCE_AFTER_PARQUET_CLOSE},
    SegmentStore, SegmentStoreConfig,
};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

const CHILD_OUTPUT_DIR_ENV: &str = "ZIPPY_TEST_FAILPOINT_OUTPUT_DIR";

#[test]
fn process_abort_after_parquet_close_leaves_only_replayable_temporary_file() {
    let output_dir = unique_persistence_dir();
    let _ = std::fs::remove_dir_all(&output_dir);

    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_child_after_parquet_close_before_rename",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_OUTPUT_DIR_ENV, &output_dir)
        .env(ACTIVE_FAILPOINT_ENV, PERSISTENCE_AFTER_PARQUET_CLOSE)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();

    assert!(!status.success(), "expected failpoint child to abort");
    #[cfg(unix)]
    assert_eq!(status.signal(), Some(libc::SIGABRT));

    let (temporary_paths, committed_paths) = persisted_paths(&output_dir);
    assert_eq!(temporary_paths.len(), 1);
    assert!(committed_paths.is_empty());
    assert_eq!(parquet_row_count(&temporary_paths[0]), 1);

    let replayed_path = persist_one_segment(&output_dir);
    assert!(replayed_path.exists());
    assert_eq!(parquet_row_count(&replayed_path), 1);

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
#[ignore = "spawned by the parent crash-harness test"]
fn crash_child_after_parquet_close_before_rename() {
    let Some(output_dir) = std::env::var_os(CHILD_OUTPUT_DIR_ENV) else {
        return;
    };
    let _ = persist_one_segment(Path::new(&output_dir));
    panic!("armed persistence failpoint did not abort the child process");
}

fn persist_one_segment(output_dir: &Path) -> PathBuf {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store
        .start_persistence_worker(output_dir.to_path_buf())
        .unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    handle
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    handle.writer().rollover().unwrap();

    worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(2))
        .unwrap()
        .expect("expected persistence worker to flush one segment")
}

fn persisted_paths(output_dir: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut temporary_paths = Vec::new();
    let mut committed_paths = Vec::new();
    for entry in std::fs::read_dir(output_dir).unwrap() {
        let path = entry.unwrap().path();
        let file_name = path.file_name().unwrap().to_string_lossy();
        if file_name.contains(".parquet.tmp-") {
            temporary_paths.push(path);
        } else if file_name.ends_with(".parquet") {
            committed_paths.push(path);
        }
    }
    (temporary_paths, committed_paths)
}

fn parquet_row_count(path: &Path) -> usize {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap().num_rows())
        .sum()
}

fn unique_persistence_dir() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "zippy-persistence-crash-failpoint-{}-{timestamp}",
        std::process::id()
    ))
}
