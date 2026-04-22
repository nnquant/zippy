use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

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

#[test]
fn spsc_queue_close_wakes_blocked_producer() {
    let queue = Arc::new(SpscDataQueue::new(1).unwrap());
    queue.push_blocking(1).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let producer_queue = Arc::clone(&queue);
    let producer_barrier = Arc::clone(&barrier);

    let producer = thread::spawn(move || {
        producer_barrier.wait();
        started_tx.send(()).unwrap();
        let result = producer_queue.push_blocking(2);
        done_tx.send(result.map_err(|err| err.to_string())).unwrap();
    });

    barrier.wait();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

    queue.close();

    let outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(outcome.is_err());
    assert!(outcome.unwrap_err().contains("queue closed"));

    producer.join().unwrap();
}

#[test]
fn spsc_queue_rejects_push_after_close() {
    let queue = SpscDataQueue::new(1).unwrap();
    queue.close();

    let err = queue.push_blocking(1).unwrap_err();
    assert!(err.to_string().contains("queue closed"));
}
