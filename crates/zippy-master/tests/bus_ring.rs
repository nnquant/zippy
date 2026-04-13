use zippy_master::ring::{ReadError, ReaderLagged, ReaderNotFound, RingConfigError, StreamRing};

#[test]
fn stream_ring_supports_independent_reader_offsets() {
    let mut ring = StreamRing::new(4, 1024).unwrap();
    let reader_a = ring.attach_reader();
    let reader_b = ring.attach_reader();

    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();

    assert_eq!(
        ring.read(&reader_a.reader_id).unwrap(),
        vec![vec![1], vec![2]]
    );
    assert_eq!(
        ring.read(&reader_b.reader_id).unwrap(),
        vec![vec![1], vec![2]]
    );
}

#[test]
fn stream_ring_reports_lagged_reader_when_overwritten() {
    let mut ring = StreamRing::new(2, 1024).unwrap();
    let reader = ring.attach_reader();
    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();
    ring.write(vec![3]).unwrap();

    let error = ring.read(&reader.reader_id).unwrap_err();
    assert!(matches!(
        error,
        ReadError::ReaderLagged(ReaderLagged { .. })
    ));
}

#[test]
fn stream_ring_seek_latest_moves_reader_to_tail() {
    let mut ring = StreamRing::new(2, 1024).unwrap();
    let reader = ring.attach_reader();
    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();
    ring.seek_latest(&reader.reader_id).unwrap();
    assert!(ring.read(&reader.reader_id).unwrap().is_empty());
}

#[test]
fn stream_ring_reports_missing_reader_distinct_from_lagged_reader() {
    let mut ring = StreamRing::new(2, 1024).unwrap();

    let error = ring.read("reader_missing").unwrap_err();
    assert!(matches!(
        error,
        ReadError::ReaderNotFound(ReaderNotFound { ref reader_id })
        if reader_id == "reader_missing"
    ));
}

#[test]
fn stream_ring_rejects_zero_capacity() {
    let error = StreamRing::new(0, 1024).unwrap_err();
    assert!(matches!(
        error,
        RingConfigError { buffer_size } if buffer_size == 0
    ));
}

#[test]
fn stream_ring_reader_can_seek_latest_and_continue_after_lag() {
    let mut ring = StreamRing::new(2, 1024).unwrap();
    let reader = ring.attach_reader();
    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();
    ring.write(vec![3]).unwrap();

    let error = ring.read(&reader.reader_id).unwrap_err();
    assert!(matches!(
        error,
        ReadError::ReaderLagged(ReaderLagged { .. })
    ));

    ring.seek_latest(&reader.reader_id).unwrap();
    ring.write(vec![4]).unwrap();

    assert_eq!(ring.read(&reader.reader_id).unwrap(), vec![vec![4]]);
}
