use zippy_segment_store::{SegmentStore, SegmentStoreConfig};

#[test]
fn dropping_reader_session_releases_segment_lease() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let session = store.open_session("reader-a").unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();

    let lease = session.attach_active(&handle).unwrap();
    assert_eq!(store.pin_count(&handle), 1);

    drop(lease);
    store.collect_garbage().unwrap();
    assert_eq!(store.pin_count(&handle), 0);
}

#[test]
fn leases_do_not_collide_across_partitions_with_same_segment_id() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let session = store.open_session("reader-a").unwrap();
    let first = store.open_partition("ticks", "rb2501").unwrap();
    let second = store.open_partition("ticks", "rb2505").unwrap();

    let first_lease = session.attach_active(&first).unwrap();
    let second_lease = session.attach_active(&second).unwrap();

    assert_eq!(first.segment_id(), second.segment_id());
    assert_eq!(store.pin_count(&first), 1);
    assert_eq!(store.pin_count(&second), 1);

    drop(first_lease);
    assert_eq!(store.pin_count(&first), 0);
    assert_eq!(store.pin_count(&second), 1);

    drop(second_lease);
    assert_eq!(store.pin_count(&second), 0);
}

#[test]
fn eventfd_notifies_committed_rows_and_rollover() {
    let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
    let handle = store.open_partition("ticks", "rb2501").unwrap();
    let reader = store.open_session("reader-b").unwrap();
    let notifier = reader.subscribe(&handle).unwrap();

    handle
        .writer()
        .append_tick_for_test(1, "rb2501", 4123.5)
        .unwrap();
    assert!(notifier
        .wait_timeout(std::time::Duration::from_millis(50))
        .unwrap());

    handle.writer().rollover().unwrap();
    assert!(notifier
        .wait_timeout(std::time::Duration::from_millis(50))
        .unwrap());
}
