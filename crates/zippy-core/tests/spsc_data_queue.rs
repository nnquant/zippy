use std::hint::black_box;
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use zippy_core::{SpscDataQueue, ZippyError};

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
#[ignore = "manual release microbenchmark; run with --ignored --nocapture --test-threads=1"]
fn data_queue_performance_baseline() {
    for capacity in [64, 4096] {
        let queue = Arc::new(SpscDataQueue::new(capacity).unwrap());
        let total = 1_000_000_u64;
        let started = Instant::now();
        for value in 0..total {
            queue.push_blocking(black_box(value)).unwrap();
            black_box(queue.try_pop().unwrap());
        }
        let serial_ns = started.elapsed().as_nanos() as f64 / total as f64;
        let producer_queue = Arc::clone(&queue);
        let started = Instant::now();
        let producer = thread::spawn(move || {
            for value in 0..total {
                producer_queue.push_blocking(value).unwrap();
            }
        });
        for expected in 0..total {
            loop {
                if let Some(value) = queue.try_pop() {
                    assert_eq!(value, expected);
                    break;
                }
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
        let concurrent_ns = started.elapsed().as_nanos() as f64 / total as f64;
        println!("capacity=[{capacity}] samples=[{total}] serial_ns=[{serial_ns:.2}] concurrent_ns=[{concurrent_ns:.2}]");
    }
}

#[test]
fn shared_queue_delivers_every_owned_value_once() {
    let queue = Arc::new(SpscDataQueue::new(7).unwrap());
    let (finished_tx, finished_rx) = mpsc::channel();
    let mut threads = Vec::new();
    for producer in 0..4 {
        let queue = Arc::clone(&queue);
        let finished = finished_tx.clone();
        threads.push(thread::spawn(move || {
            for sequence in 0..1000 {
                queue
                    .push_blocking(Box::new(producer * 1000 + sequence))
                    .unwrap();
            }
            finished.send(()).unwrap();
        }));
    }
    let (values_tx, values_rx) = mpsc::channel();
    for _ in 0..4 {
        let queue = Arc::clone(&queue);
        let values_tx = values_tx.clone();
        threads.push(thread::spawn(move || {
            for _ in 0..1000 {
                loop {
                    if let Some(value) = queue.try_pop() {
                        values_tx.send(*value).unwrap();
                        break;
                    }
                    thread::yield_now();
                }
            }
        }));
    }
    for _ in 0..4 {
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let mut values = (0..4000)
        .map(|_| values_rx.recv_timeout(Duration::from_secs(5)).unwrap())
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }
    values.sort_unstable();
    assert_eq!(values, (0..4000).collect::<Vec<_>>());
    assert!(queue.is_empty());
}

#[test]
fn closing_full_queue_releases_all_writers_and_preserves_buffered_value() {
    let queue = Arc::new(SpscDataQueue::new(1).unwrap());
    queue.push_blocking(0).unwrap();
    let barrier = Arc::new(Barrier::new(5));
    let (done_tx, done_rx) = mpsc::channel();
    let mut threads = Vec::new();
    for value in 1..5 {
        let queue = Arc::clone(&queue);
        let barrier = Arc::clone(&barrier);
        let done = done_tx.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            done.send(queue.push_blocking(value)).unwrap();
        }));
    }
    barrier.wait();
    queue.close();
    queue.close();
    for _ in 1..5 {
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Err(ZippyError::QueueClosed)
        ));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(queue.try_pop(), Some(0));
    assert!(queue.is_empty());
}
