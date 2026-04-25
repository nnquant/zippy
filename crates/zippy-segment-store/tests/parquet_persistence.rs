use std::time::Duration;

use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

#[test]
fn rollover_enqueues_sealed_segment_for_persistence() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    handle
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    handle.writer().rollover().unwrap();

    let path = store
        .flush_one_timeout_for_test(Duration::from_millis(50))
        .unwrap()
        .expect("expected rollover to enqueue sealed segment");
    assert!(path.to_string_lossy().ends_with(".parquet"));
}

#[test]
fn manual_enqueue_is_rejected_after_rollover_auto_enqueue() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    handle
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    let sealed = handle.writer().rollover().unwrap();

    let err = store.enqueue_persistence(sealed).unwrap_err();
    assert!(err.to_string().contains("disabled"));

    let path = store
        .flush_one_timeout_for_test(Duration::from_millis(50))
        .unwrap()
        .expect("expected automatic rollover enqueue");
    assert!(path.to_string_lossy().ends_with(".parquet"));

    let duplicate = store
        .flush_one_timeout_for_test(Duration::from_millis(10))
        .unwrap();
    assert!(duplicate.is_none());
}

#[test]
fn parquet_flush_paths_do_not_conflict_across_stores() {
    let store_a = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle_a = store_a.open_partition("ticks", "rb2501").unwrap();
    handle_a
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    handle_a.writer().rollover().unwrap();

    let store_b = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle_b = store_b.open_partition("ticks", "rb2505").unwrap();
    handle_b
        .writer()
        .append_tick_for_test(1, "rb2505", 4125.0)
        .unwrap();
    handle_b.writer().rollover().unwrap();

    let path_a = store_a
        .flush_one_timeout_for_test(Duration::from_millis(50))
        .unwrap()
        .expect("expected store_a sealed segment");
    let path_b = store_b
        .flush_one_timeout_for_test(Duration::from_millis(50))
        .unwrap()
        .expect("expected store_b sealed segment");

    assert_ne!(path_a, path_b);
    assert!(path_a.to_string_lossy().ends_with(".parquet"));
    assert!(path_b.to_string_lossy().ends_with(".parquet"));
}

#[test]
fn persistence_worker_flushes_rollover_segments_in_background() {
    let output_dir = unique_persistence_dir("background-worker");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();

    assert_eq!(writer.committed_row_count(), 1);

    let path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected background worker to flush sealed segment");

    assert!(path.starts_with(&output_dir));
    assert!(path.exists());

    drop(worker);
    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_worker_shutdown_drains_queued_segments() {
    let output_dir = unique_persistence_dir("shutdown-drain");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    for row in 1..=3 {
        writer
            .append_tick_for_test(row, "rb2501", 4123.5 + f64::from(row as i32))
            .unwrap();
        writer.rollover().unwrap();
    }

    let report = worker.shutdown();

    assert_eq!(report.errors().len(), 0);
    assert_eq!(report.completed_paths().len(), 3);
    for path in report.completed_paths() {
        assert!(path.starts_with(&output_dir));
        assert!(path.exists());
    }

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_worker_reports_failed_segment_with_target_path() {
    let output_dir = unique_persistence_dir("failure-report");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    let first_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected first segment flush");

    let first_name = first_path.file_name().unwrap().to_string_lossy();
    let conflict_name = first_name
        .strip_suffix("-1.parquet")
        .map(|prefix| format!("{prefix}-2.parquet"))
        .expect("expected first segment path to end with segment id");
    let conflict_path = output_dir.join(conflict_name);
    std::fs::create_dir(&conflict_path).unwrap();

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    writer.rollover().unwrap();

    let report = worker.shutdown();

    assert_eq!(report.failed_segments().len(), 1);

    let failure = &report.failed_segments()[0];
    assert_eq!(failure.segment_id(), 2);
    assert_eq!(failure.target_path(), conflict_path.as_path());
    assert!(failure.error().contains("parquet"));

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_worker_uses_atomic_rename_and_cleans_temp_on_rename_failure() {
    let output_dir = unique_persistence_dir("atomic-rename");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    let first_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected first segment flush");

    let first_name = first_path.file_name().unwrap().to_string_lossy();
    let conflict_name = first_name
        .strip_suffix("-1.parquet")
        .map(|prefix| format!("{prefix}-2.parquet"))
        .expect("expected first segment path to end with segment id");
    let conflict_path = output_dir.join(conflict_name);
    std::fs::create_dir(&conflict_path).unwrap();

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    writer.rollover().unwrap();

    let report = worker.shutdown();

    assert_eq!(report.failed_segments().len(), 1);
    let failure = &report.failed_segments()[0];
    assert_eq!(failure.segment_id(), 2);
    assert!(failure.error().contains("rename parquet"));

    let temp_paths = std::fs::read_dir(&output_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.to_string_lossy().contains(".tmp"))
        .collect::<Vec<_>>();
    assert!(
        temp_paths.is_empty(),
        "expected temporary parquet files to be cleaned temp_paths=[{:?}]",
        temp_paths
    );

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_failure_can_retry_after_target_is_repaired() {
    let output_dir = unique_persistence_dir("retry-failure");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    let first_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected first segment flush");

    let first_name = first_path.file_name().unwrap().to_string_lossy();
    let conflict_name = first_name
        .strip_suffix("-1.parquet")
        .map(|prefix| format!("{prefix}-2.parquet"))
        .expect("expected first segment path to end with segment id");
    let conflict_path = output_dir.join(conflict_name);
    std::fs::create_dir(&conflict_path).unwrap();

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    writer.rollover().unwrap();

    let report = worker.shutdown();
    let failure = &report.failed_segments()[0];
    assert_eq!(failure.target_path(), conflict_path.as_path());

    std::fs::remove_dir(&conflict_path).unwrap();
    let retry_path = failure.retry_to_dir(&output_dir).unwrap();

    assert_eq!(retry_path, conflict_path);
    assert!(retry_path.exists());

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_worker_retries_failed_flush_until_target_is_repaired() {
    let output_dir = unique_persistence_dir("retry-policy");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let policy = zippy_segment_store::PersistenceRetryPolicy {
        max_attempts: 3,
        backoff: Duration::from_millis(25),
    };
    let worker = store
        .start_persistence_worker_with_policy(output_dir.clone(), policy)
        .unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    let first_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected first segment flush");
    let first_name = first_path.file_name().unwrap().to_string_lossy();
    let conflict_name = first_name
        .strip_suffix("-1.parquet")
        .map(|prefix| format!("{prefix}-2.parquet"))
        .expect("expected first segment path to end with segment id");
    let conflict_path = output_dir.join(conflict_name);
    std::fs::create_dir(&conflict_path).unwrap();

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    writer.rollover().unwrap();

    std::thread::sleep(Duration::from_millis(10));
    std::fs::remove_dir(&conflict_path).unwrap();
    let second_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected retry to flush repaired target");

    assert_eq!(second_path, conflict_path);
    assert!(second_path.exists());
    assert_eq!(worker.metrics_snapshot().retry_attempts(), 1);

    let report = worker.shutdown();
    assert_eq!(report.failed_segments().len(), 0);
    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn persistence_worker_writes_dead_letter_manifest_after_retries_exhaust() {
    let output_dir = unique_persistence_dir("dead-letter");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let policy = zippy_segment_store::PersistenceRetryPolicy {
        max_attempts: 2,
        backoff: Duration::from_millis(0),
    };
    let worker = store
        .start_persistence_worker_with_policy(output_dir.clone(), policy)
        .unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let writer = handle.writer();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.rollover().unwrap();
    let first_path = worker
        .recv_flushed_path_timeout_for_test(Duration::from_secs(1))
        .unwrap()
        .expect("expected first segment flush");
    let first_name = first_path.file_name().unwrap().to_string_lossy();
    let conflict_name = first_name
        .strip_suffix("-1.parquet")
        .map(|prefix| format!("{prefix}-2.parquet"))
        .expect("expected first segment path to end with segment id");
    let conflict_path = output_dir.join(conflict_name);
    std::fs::create_dir(&conflict_path).unwrap();

    writer.append_tick_for_test(2, "rb2501", 4124.5).unwrap();
    writer.rollover().unwrap();

    let report = worker.shutdown();
    assert_eq!(report.failed_segments().len(), 1);
    assert_eq!(report.metrics().failed_flushes(), 1);
    assert_eq!(report.metrics().dead_lettered_segments(), 1);

    let manifest = std::fs::read_to_string(output_dir.join("dead-letter.log")).unwrap();
    assert!(manifest.contains("segment_id=2"));
    assert!(manifest.contains("attempts=2"));
    assert!(manifest.contains(conflict_path.to_string_lossy().as_ref()));

    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn sync_flush_is_rejected_while_persistence_worker_is_running() {
    let output_dir = unique_persistence_dir("sync-flush-rejected");
    let _ = std::fs::remove_dir_all(&output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();

    let err = store
        .flush_one_timeout_for_test(Duration::from_millis(1))
        .unwrap_err();
    assert!(err.to_string().contains("worker is running"));

    drop(worker);
    let _ = std::fs::remove_dir_all(output_dir);
}

#[test]
fn second_persistence_worker_is_rejected() {
    let output_dir = unique_persistence_dir("single-worker-a");
    let other_output_dir = unique_persistence_dir("single-worker-b");
    let _ = std::fs::remove_dir_all(&output_dir);
    let _ = std::fs::remove_dir_all(&other_output_dir);

    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let worker = store.start_persistence_worker(output_dir.clone()).unwrap();

    let err = store
        .start_persistence_worker(other_output_dir.clone())
        .unwrap_err();
    assert!(err.to_string().contains("worker is running"));

    drop(worker);
    let _ = std::fs::remove_dir_all(output_dir);
    let _ = std::fs::remove_dir_all(other_output_dir);
}

fn unique_persistence_dir(test_name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "zippy-segment-store-{test_name}-{}",
        std::process::id()
    ))
}
