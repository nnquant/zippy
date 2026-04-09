use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zippy_core::{
    spawn_engine, spawn_engine_with_publisher, Engine, EngineConfig, EngineStatus, OverflowPolicy,
    Publisher, Result, ZippyError,
};

struct FlushEngine {
    schema: Arc<Schema>,
    flushed: bool,
}

impl Engine for FlushEngine {
    fn name(&self) -> &str {
        "flush-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        self.flushed = true;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Float64Array::from(vec![99.0]))],
        )
        .unwrap();
        Ok(vec![batch])
    }
}

struct FailingFlushEngine {
    schema: Arc<Schema>,
}

impl Engine for FailingFlushEngine {
    fn name(&self) -> &str {
        "failing-flush-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        Err(ZippyError::Io {
            reason: "flush failure".to_string(),
        })
    }
}

struct FailingDataEngine {
    schema: Arc<Schema>,
}

impl Engine for FailingDataEngine {
    fn name(&self) -> &str {
        "failing-data-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, _batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        Err(ZippyError::Io {
            reason: "data failure".to_string(),
        })
    }
}

struct BlockingDataEngine {
    schema: Arc<Schema>,
    started_tx: Sender<f64>,
    release_rx: Receiver<()>,
    seen: Arc<Mutex<Vec<f64>>>,
}

struct FailingPublisher;

impl Publisher for FailingPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        Err(ZippyError::Io {
            reason: "publish failure".to_string(),
        })
    }
}

struct BlockingFailingDataEngine {
    schema: Arc<Schema>,
    started_tx: Sender<f64>,
    release_rx: Receiver<()>,
}

impl Engine for BlockingFailingDataEngine {
    fn name(&self) -> &str {
        "blocking-failing-data-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let prices = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        self.started_tx.send(prices.value(0)).unwrap();
        self.release_rx.recv().unwrap();
        Err(ZippyError::Io {
            reason: "data failure".to_string(),
        })
    }
}

impl Engine for BlockingDataEngine {
    fn name(&self) -> &str {
        "blocking-data-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let prices = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let value = prices.value(0);
        self.seen.lock().unwrap().push(value);
        self.started_tx.send(value).unwrap();
        self.release_rx.recv().unwrap();
        Ok(vec![batch])
    }
}

fn price_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]))
}

fn single_price_batch(schema: Arc<Schema>, value: f64) -> RecordBatch {
    RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![value]))]).unwrap()
}

fn wait_for_status(handle: &zippy_core::EngineHandle, expected: EngineStatus) {
    let deadline = Instant::now() + Duration::from_secs(1);

    while Instant::now() < deadline {
        if handle.status() == expected {
            return;
        }

        thread::sleep(Duration::from_millis(10));
    }

    panic!(
        "engine status did not reach expected state expected=[{}] actual=[{}]",
        expected.as_str(),
        handle.status().as_str()
    );
}

#[test]
fn reject_overflow_returns_error() {
    let schema = price_schema();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let engine = BlockingDataEngine {
        schema: schema.clone(),
        started_tx,
        release_rx,
        seen: seen.clone(),
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "reject-overflow-engine".to_string(),
            buffer_capacity: 1,
            overflow_policy: OverflowPolicy::Reject,
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    handle
        .write(single_price_batch(schema.clone(), 1.0))
        .unwrap();
    assert_eq!(started_rx.recv().unwrap(), 1.0);
    handle
        .write(single_price_batch(schema.clone(), 2.0))
        .unwrap();
    let err = handle.write(single_price_batch(schema, 3.0)).unwrap_err();
    assert!(matches!(err, ZippyError::ChannelSend));

    for _ in 0..2 {
        release_tx.send(()).unwrap();
    }
    handle.stop().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1.0, 2.0]);
}

#[test]
fn drop_oldest_overflow_discards_queued_batch_and_updates_metrics() {
    let schema = price_schema();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let engine = BlockingDataEngine {
        schema: schema.clone(),
        started_tx,
        release_rx,
        seen: seen.clone(),
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "drop-oldest-engine".to_string(),
            buffer_capacity: 1,
            overflow_policy: OverflowPolicy::DropOldest,
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    handle
        .write(single_price_batch(schema.clone(), 1.0))
        .unwrap();
    assert_eq!(started_rx.recv().unwrap(), 1.0);
    handle
        .write(single_price_batch(schema.clone(), 2.0))
        .unwrap();
    handle.write(single_price_batch(schema, 3.0)).unwrap();

    for _ in 0..2 {
        release_tx.send(()).unwrap();
    }
    handle.stop().unwrap();

    assert_eq!(*seen.lock().unwrap(), vec![1.0, 3.0]);
    assert_eq!(handle.metrics().dropped_batches_total, 1);
}

#[test]
fn blocked_write_after_data_failure_returns_invalid_state() {
    let schema = price_schema();
    let (started_tx, started_rx) = bounded(2);
    let (release_tx, release_rx) = bounded(1);
    let (fail_tx, fail_rx) = bounded(1);
    let engine = BlockingFailingDataEngine {
        schema: schema.clone(),
        started_tx,
        release_rx,
    };
    let handle = spawn_engine(
        engine,
        EngineConfig {
            name: "blocking-failing-data-engine".to_string(),
            buffer_capacity: 1,
            overflow_policy: OverflowPolicy::Block,
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    handle
        .write(single_price_batch(schema.clone(), 1.0))
        .unwrap();
    assert_eq!(started_rx.recv().unwrap(), 1.0);
    handle
        .write(single_price_batch(schema.clone(), 2.0))
        .unwrap();

    let releaser = thread::spawn(move || {
        fail_rx.recv().unwrap();
        release_tx.send(()).unwrap();
    });

    fail_tx.send(()).unwrap();
    let err = handle
        .write(single_price_batch(schema.clone(), 3.0))
        .unwrap_err();
    match err {
        ZippyError::InvalidState { status } => assert_eq!(status, "failed"),
        other => panic!("unexpected write error: {other:?}"),
    }

    let flush_err = handle.flush().unwrap_err();
    match flush_err {
        ZippyError::InvalidState { status } => assert_eq!(status, "failed"),
        other => panic!("unexpected flush error: {other:?}"),
    }
    assert_eq!(handle.status(), EngineStatus::Failed);
    releaser.join().unwrap();
}

#[test]
fn stop_preserves_control_semantics_when_reject_queue_is_full() {
    let schema = price_schema();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (started_tx, started_rx) = bounded(4);
    let (release_tx, release_rx) = bounded(4);
    let (stop_tx, stop_rx) = bounded(1);
    let engine = BlockingDataEngine {
        schema: schema.clone(),
        started_tx,
        release_rx,
        seen: seen.clone(),
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "reject-stop-engine".to_string(),
            buffer_capacity: 1,
            overflow_policy: OverflowPolicy::Reject,
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    handle
        .write(single_price_batch(schema.clone(), 1.0))
        .unwrap();
    assert_eq!(started_rx.recv().unwrap(), 1.0);
    handle.write(single_price_batch(schema, 2.0)).unwrap();

    let releaser = thread::spawn(move || {
        stop_rx.recv().unwrap();
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
    });

    stop_tx.send(()).unwrap();
    handle.stop().unwrap();
    assert_eq!(handle.status(), EngineStatus::Stopped);
    assert_eq!(*seen.lock().unwrap(), vec![1.0, 2.0]);
    releaser.join().unwrap();
}

#[test]
fn lifecycle_flush_and_stop_emit_pending_batches() {
    let schema = price_schema();
    let engine = FlushEngine {
        schema: schema.clone(),
        flushed: false,
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "flush-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    let input = single_price_batch(schema, 1.0);
    handle.write(input).unwrap();
    let flushed = handle.flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(handle.metrics().queue_depth, 0);
    assert_eq!(handle.status(), EngineStatus::Running);
    handle.stop().unwrap();
    assert_eq!(handle.status(), EngineStatus::Stopped);
}

#[test]
fn flush_failure_marks_engine_failed() {
    let schema = price_schema();
    let engine = FailingFlushEngine {
        schema: schema.clone(),
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "failing-flush-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    let err = handle.flush().unwrap_err();

    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "flush failure"),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(handle.status(), EngineStatus::Failed);
    let stop_err = handle.stop().unwrap_err();
    match stop_err {
        ZippyError::Io { reason } => assert_eq!(reason, "flush failure"),
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn stop_after_data_failure_returns_worker_error_and_failed_status() {
    let schema = price_schema();
    let engine = FailingDataEngine {
        schema: schema.clone(),
    };
    let mut handle = spawn_engine(
        engine,
        EngineConfig {
            name: "failing-data-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    handle.write(single_price_batch(schema, 1.0)).unwrap();
    let err = handle.stop().unwrap_err();

    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "data failure"),
        other => panic!("unexpected stop error: {other:?}"),
    }
    assert_eq!(handle.status(), EngineStatus::Failed);
}

#[test]
fn write_after_worker_failure_returns_invalid_state() {
    let schema = price_schema();
    let engine = FailingFlushEngine {
        schema: schema.clone(),
    };
    let handle = spawn_engine(
        engine,
        EngineConfig {
            name: "failing-flush-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    let err = handle.flush().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "flush failure"),
        other => panic!("unexpected error: {other:?}"),
    }

    let write_err = handle.write(single_price_batch(schema, 2.0)).unwrap_err();
    match write_err {
        ZippyError::InvalidState { status } => assert_eq!(status, "failed"),
        other => panic!("unexpected write error: {other:?}"),
    }
}

#[test]
fn flush_after_worker_failure_returns_invalid_state_quickly() {
    let schema = price_schema();
    let engine = FailingFlushEngine { schema };
    let handle = spawn_engine(
        engine,
        EngineConfig {
            name: "failing-flush-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
    )
    .unwrap();

    let err = handle.flush().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "flush failure"),
        other => panic!("unexpected error: {other:?}"),
    }

    let flush_err = handle.flush().unwrap_err();
    match flush_err {
        ZippyError::InvalidState { status } => assert_eq!(status, "failed"),
        other => panic!("unexpected flush error: {other:?}"),
    }
}

#[test]
fn publish_failure_marks_engine_failed_and_updates_metrics() {
    let schema = price_schema();
    let engine = FlushEngine {
        schema: schema.clone(),
        flushed: false,
    };
    let handle = spawn_engine_with_publisher(
        engine,
        EngineConfig {
            name: "publish-failure-engine".to_string(),
            buffer_capacity: 16,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        },
        FailingPublisher,
    )
    .unwrap();

    handle.write(single_price_batch(schema, 1.0)).unwrap();
    wait_for_status(&handle, EngineStatus::Failed);

    assert_eq!(handle.metrics().publish_errors_total, 1);

    let flush_err = handle.flush().unwrap_err();
    match flush_err {
        ZippyError::InvalidState { status } => assert_eq!(status, "failed"),
        other => panic!("unexpected flush error: {other:?}"),
    }
}
