use std::fs;
use std::path::Path;
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
fn spsc_queue_preserves_order_across_concurrent_wraparound() {
    let queue = Arc::new(SpscDataQueue::new(64).unwrap());
    let producer_queue = Arc::clone(&queue);
    let total = 10_000_u64;

    let producer = thread::spawn(move || {
        for value in 0..total {
            producer_queue.push_blocking(value).unwrap();
        }
    });

    let mut next = 0_u64;
    while next < total {
        if let Some(value) = queue.try_pop() {
            assert_eq!(value, next);
            next += 1;
        } else {
            thread::yield_now();
        }
    }

    producer.join().unwrap();
    assert!(queue.is_empty());
}

#[test]
fn spsc_queue_rejects_push_after_close() {
    let queue = SpscDataQueue::new(1).unwrap();
    queue.close();

    let err = queue.push_blocking(1).unwrap_err();
    assert!(err.to_string().contains("queue closed"));
}

#[test]
fn spsc_queue_uses_atomic_ring_storage() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = fs::read_to_string(manifest_dir.join("src/spsc_data_queue.rs")).unwrap();

    assert!(source.contains("AtomicUsize"));
    assert!(source.contains("UnsafeCell<MaybeUninit<T>>"));
    assert!(!source.contains("VecDeque"));
    assert!(!source.contains("Mutex<State"));
}
