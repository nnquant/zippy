use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

#[test]
fn sealed_segment_flushes_to_parquet_without_blocking_writer() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    handle
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    let sealed = handle.writer().rollover().unwrap();
    store.enqueue_persistence(sealed.clone()).unwrap();

    let path = store.flush_one_for_test().unwrap();
    assert!(path.to_string_lossy().ends_with(".parquet"));
}

#[test]
fn parquet_flush_paths_do_not_conflict_across_stores() {
    let store_a = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle_a = store_a.open_partition("ticks", "rb2501").unwrap();
    handle_a
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    let sealed_a = handle_a.writer().rollover().unwrap();
    store_a.enqueue_persistence(sealed_a).unwrap();

    let store_b = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle_b = store_b.open_partition("ticks", "rb2505").unwrap();
    handle_b
        .writer()
        .append_tick_for_test(1, "rb2505", 4125.0)
        .unwrap();
    let sealed_b = handle_b.writer().rollover().unwrap();
    store_b.enqueue_persistence(sealed_b).unwrap();

    let path_a = store_a.flush_one_for_test().unwrap();
    let path_b = store_b.flush_one_for_test().unwrap();

    assert_ne!(path_a, path_b);
    assert!(path_a.to_string_lossy().ends_with(".parquet"));
    assert!(path_b.to_string_lossy().ends_with(".parquet"));
}
