use zippy_core::BoundedQueue;

#[test]
fn queue_try_recv_returns_none_when_empty() {
    let queue = BoundedQueue::<u64>::new(4);
    let rx = queue.receiver();
    assert!(rx.try_recv().is_none());
}

#[test]
fn queue_try_recv_returns_value_without_blocking() {
    let queue = BoundedQueue::<u64>::new(4);
    let tx = queue.sender();
    let rx = queue.receiver();
    assert!(tx.send(7u64).is_ok());
    assert_eq!(rx.try_recv(), Some(7));
}
