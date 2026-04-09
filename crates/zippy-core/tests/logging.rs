#[test]
fn setup_log_creates_jsonl_file_and_returns_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp.path(),
        true,
        true,
    ))
    .unwrap();

    assert_eq!(snapshot.app, "runtime_test");
    assert_eq!(snapshot.level, "info");
    assert!(!snapshot.run_id.is_empty());

    let file_path = snapshot.file_path.expect("missing file path");
    assert!(file_path.exists());
    assert!(file_path.starts_with(temp.path().join("runtime_test")));
}
