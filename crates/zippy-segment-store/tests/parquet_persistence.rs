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
