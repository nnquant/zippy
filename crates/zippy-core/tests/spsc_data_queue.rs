use zippy_core::SpscDataQueue;

#[test]
fn spsc_queue_preserves_fifo_order() {
    let queue = SpscDataQueue::new(2).unwrap();
    queue.push_blocking(1).unwrap();
    queue.push_blocking(2).unwrap();

    assert_eq!(queue.try_pop(), Some(1));
    assert_eq!(queue.try_pop(), Some(2));
    assert_eq!(queue.try_pop(), None);
}

#[test]
fn spsc_queue_rejects_zero_capacity() {
    match SpscDataQueue::<u64>::new(0) {
        Ok(_) => panic!("zero capacity should fail"),
        Err(err) => assert!(err
            .to_string()
            .contains("capacity must be greater than zero")),
    }
}
