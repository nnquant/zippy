use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Receiver, Sender};
use zippy_core::{
    spawn_source_engine_with_publisher, Engine, EngineConfig, EngineMetricsDelta, EngineStatus,
    OverflowPolicy, Publisher, Result, Source, SourceEvent, SourceHandle, SourceMode, SourceSink,
    StreamHello, ZippyError,
};

#[derive(Default)]
struct NoopPublisher;

impl Publisher for NoopPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct TrackingPublisher {
    published_batches: Arc<AtomicUsize>,
    flushes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl Publisher for TrackingPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        self.published_batches.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.closes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
struct RecordedCalls {
    hello_count: usize,
    data_count: usize,
    flush_count: usize,
    stop_count: usize,
}

struct RecordingEngine {
    schema: Arc<Schema>,
    calls: Arc<Mutex<RecordedCalls>>,
}

impl RecordingEngine {
    fn new(schema: Arc<Schema>, calls: Arc<Mutex<RecordedCalls>>) -> Self {
        Self { schema, calls }
    }
}

impl Engine for RecordingEngine {
    fn name(&self) -> &str {
        "recording-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        self.calls.lock().unwrap().data_count += 1;
        Ok(vec![batch])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        self.calls.lock().unwrap().flush_count += 1;
        Ok(vec![test_batch()])
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        self.calls.lock().unwrap().stop_count += 1;
        Ok(vec![test_batch()])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}

struct StaticSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    events: Vec<SourceEvent>,
}

impl StaticSource {
    fn new(mode: SourceMode, schema: Arc<Schema>, events: Vec<SourceEvent>) -> Self {
        Self {
            mode,
            schema,
            events,
        }
    }
}

impl Source for StaticSource {
    fn name(&self) -> &str {
        "static-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let join_handle = thread::spawn(move || -> Result<()> {
            for event in self.events {
                sink.emit(event)?;
            }
            Ok(())
        });

        Ok(SourceHandle::new(join_handle))
    }
}

struct BlockingSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    stop_calls: Arc<AtomicUsize>,
    release_tx: Sender<()>,
    release_rx: Receiver<()>,
}

impl BlockingSource {
    fn new(mode: SourceMode, schema: Arc<Schema>, stop_calls: Arc<AtomicUsize>) -> Self {
        let (release_tx, release_rx) = bounded(1);
        Self {
            mode,
            schema,
            stop_calls,
            release_tx,
            release_rx,
        }
    }
}

impl Source for BlockingSource {
    fn name(&self) -> &str {
        "blocking-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let schema = self.schema.clone();
        let release_rx = self.release_rx;
        let stop_calls = self.stop_calls.clone();
        let release_tx = self.release_tx.clone();
        let join_handle = thread::spawn(move || -> Result<()> {
            sink.emit(SourceEvent::Hello(StreamHello::new("bars", schema, 1)?))?;
            release_rx.recv().map_err(|_| ZippyError::ChannelReceive)?;
            Ok(())
        });

        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                stop_calls.fetch_add(1, Ordering::Relaxed);
                release_tx.send(()).map_err(|_| ZippyError::ChannelSend)
            }),
        ))
    }
}

struct ErrorAfterHelloSource {
    mode: SourceMode,
    schema: Arc<Schema>,
}

impl Source for ErrorAfterHelloSource {
    fn name(&self) -> &str {
        "error-after-hello-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let schema = self.schema.clone();
        let join_handle = thread::spawn(move || -> Result<()> {
            sink.emit(SourceEvent::Hello(StreamHello::new("bars", schema, 1)?))?;
            Err(ZippyError::Io {
                reason: "source thread failed".to_string(),
            })
        });
        Ok(SourceHandle::new(join_handle))
    }
}

struct RetryableStopSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    stop_attempts: Arc<AtomicUsize>,
}

impl Source for RetryableStopSource {
    fn name(&self) -> &str {
        "retryable-stop-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let schema = self.schema.clone();
        let stop_attempts = self.stop_attempts.clone();
        let (release_tx, release_rx) = bounded(1);
        let join_handle = thread::spawn(move || -> Result<()> {
            sink.emit(SourceEvent::Hello(StreamHello::new("bars", schema, 1)?))?;
            release_rx.recv().map_err(|_| ZippyError::ChannelReceive)?;
            Ok(())
        });
        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                let attempt = stop_attempts.fetch_add(1, Ordering::Relaxed);
                if attempt == 0 {
                    return Err(ZippyError::Io {
                        reason: "stop callback failed".to_string(),
                    });
                }
                release_tx.send(()).map_err(|_| ZippyError::ChannelSend)
            }),
        ))
    }
}

struct StopThenErrorSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    stop_attempts: Arc<AtomicUsize>,
}

impl Source for StopThenErrorSource {
    fn name(&self) -> &str {
        "stop-then-error-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        let schema = self.schema.clone();
        let stop_attempts = self.stop_attempts.clone();
        let (release_tx, release_rx) = bounded(1);
        let join_handle = thread::spawn(move || -> Result<()> {
            sink.emit(SourceEvent::Hello(StreamHello::new("bars", schema, 1)?))?;
            release_rx.recv().map_err(|_| ZippyError::ChannelReceive)?;
            Err(ZippyError::Io {
                reason: "source failed after stop".to_string(),
            })
        });
        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                stop_attempts.fetch_add(1, Ordering::Relaxed);
                release_tx.send(()).map_err(|_| ZippyError::ChannelSend)
            }),
        ))
    }
}

struct StartFailSource {
    mode: SourceMode,
    schema: Arc<Schema>,
}

impl Source for StartFailSource {
    fn name(&self) -> &str {
        "start-fail-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, _sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        Err(ZippyError::Io {
            reason: "source start failed".to_string(),
        })
    }
}

struct DropTrackingEngine {
    schema: Arc<Schema>,
    drop_count: Arc<AtomicUsize>,
}

impl DropTrackingEngine {
    fn new(schema: Arc<Schema>, drop_count: Arc<AtomicUsize>) -> Self {
        Self { schema, drop_count }
    }
}

impl Drop for DropTrackingEngine {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Engine for DropTrackingEngine {
    fn name(&self) -> &str {
        "drop-tracking-engine"
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
        Ok(Vec::new())
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Float64,
        false,
    )]))
}

fn schema_with_metadata() -> Arc<Schema> {
    let mut metadata = HashMap::new();
    metadata.insert("exchange".to_string(), "sse".to_string());
    Arc::new(Schema::new_with_metadata(
        vec![Field::new("price", DataType::Float64, false)],
        metadata,
    ))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Float64Array::from(vec![1.0_f64]))],
    )
    .unwrap()
}

fn test_engine_config(name: &str) -> EngineConfig {
    test_engine_config_with_xfast(name, false)
}

fn test_engine_config_with_xfast(name: &str, xfast: bool) -> EngineConfig {
    EngineConfig {
        name: name.to_string(),
        buffer_capacity: 16,
        overflow_policy: OverflowPolicy::Block,
        late_data_policy: Default::default(),
        xfast,
    }
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
fn pipeline_source_runtime_forwards_flush_into_engine() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
            SourceEvent::Flush,
            SourceEvent::Stop,
        ],
    );
    let engine = RecordingEngine::new(schema, calls.clone());

    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("pipeline"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Stopped);
    handle.stop().unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        RecordedCalls {
            hello_count: 0,
            data_count: 1,
            flush_count: 1,
            stop_count: 1,
        }
    );
    assert_eq!(handle.metrics().output_batches_total, 3);
}

#[test]
fn source_runtime_consumer_stop_ends_runtime_without_engine_stop() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let source = StaticSource::new(
        SourceMode::Consumer,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
            SourceEvent::Flush,
            SourceEvent::Stop,
        ],
    );
    let engine = RecordingEngine::new(schema, calls.clone());

    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("consumer"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Stopped);
    assert_eq!(
        *calls.lock().unwrap(),
        RecordedCalls {
            hello_count: 0,
            data_count: 1,
            flush_count: 0,
            stop_count: 0,
        }
    );

    handle.stop().unwrap();
    assert_eq!(handle.status(), EngineStatus::Stopped);
    assert_eq!(
        *calls.lock().unwrap(),
        RecordedCalls {
            hello_count: 0,
            data_count: 1,
            flush_count: 0,
            stop_count: 0,
        }
    );
}

#[test]
fn source_runtime_data_event_drives_engine_on_data() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let source = StaticSource::new(
        SourceMode::Consumer,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
            SourceEvent::Stop,
        ],
    );
    let engine = RecordingEngine::new(schema, calls.clone());

    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("data"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Stopped);
    assert_eq!(calls.lock().unwrap().data_count, 1);
    assert_eq!(handle.metrics().processed_batches_total, 1);

    handle.stop().unwrap();
}

#[test]
fn source_runtime_requires_hello_before_data() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![SourceEvent::Data(test_batch())],
    );
    let engine = RecordingEngine::new(schema, calls.clone());

    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("hello-first"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    assert_eq!(calls.lock().unwrap().data_count, 0);

    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::InvalidConfig { reason } => {
            assert_eq!(reason, "source hello must arrive before data")
        }
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_consumer_handle_stop_requests_source_shutdown_before_joining() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let stop_calls = Arc::new(AtomicUsize::new(0));
    let source = BlockingSource::new(SourceMode::Consumer, schema.clone(), stop_calls.clone());
    let engine = RecordingEngine::new(schema, calls.clone());
    let handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("consumer-stop-source"),
        NoopPublisher,
    )
    .unwrap();

    thread::sleep(Duration::from_millis(50));

    let (done_tx, done_rx) = bounded(1);
    thread::spawn(move || {
        let mut handle = handle;
        let result = handle.stop();
        done_tx.send(result).unwrap();
    });

    let result = done_rx
        .recv_timeout(Duration::from_millis(300))
        .expect("handle.stop should finish after requesting source shutdown");
    result.unwrap();
    assert_eq!(stop_calls.load(Ordering::Relaxed), 1);
    assert_eq!(calls.lock().unwrap().stop_count, 1);
}

#[test]
fn source_runtime_pipeline_flush_flushes_publisher_barrier() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let publisher = TrackingPublisher::default();
    let flushes = publisher.flushes.clone();
    let closes = publisher.closes.clone();
    let published_batches = publisher.published_batches.clone();
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
            SourceEvent::Flush,
            SourceEvent::Stop,
        ],
    );
    let engine = RecordingEngine::new(schema, calls);
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("pipeline-flush-barrier"),
        publisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Stopped);
    handle.stop().unwrap();

    assert_eq!(published_batches.load(Ordering::Relaxed), 3);
    assert_eq!(flushes.load(Ordering::Relaxed), 2);
    assert_eq!(closes.load(Ordering::Relaxed), 1);
}

#[test]
fn source_runtime_stream_hello_schema_hash_is_stable_and_structure_sensitive() {
    let hash_a = StreamHello::new("bars", schema_with_metadata(), 1)
        .unwrap()
        .schema_hash;
    let hash_b = StreamHello::new("bars", schema_with_metadata(), 1)
        .unwrap()
        .schema_hash;
    let hash_c = StreamHello::new("bars", test_schema(), 1)
        .unwrap()
        .schema_hash;

    assert_eq!(hash_a, hash_b);
    assert_ne!(hash_a, hash_c);
    assert_eq!(hash_a, "fde956dbb45da8fa");
}

#[test]
fn source_runtime_rejects_duplicate_hello() {
    let schema = test_schema();
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
        ],
    );
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("duplicate-hello"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::InvalidConfig { reason } => assert_eq!(reason, "source hello already received"),
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_rejects_flush_before_hello() {
    let schema = test_schema();
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![SourceEvent::Flush],
    );
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("flush-before-hello"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::InvalidConfig { reason } => {
            assert_eq!(reason, "source hello must arrive before flush")
        }
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_rejects_stop_before_hello() {
    let schema = test_schema();
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![SourceEvent::Stop],
    );
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("stop-before-hello"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::InvalidConfig { reason } => {
            assert_eq!(reason, "source hello must arrive before stop")
        }
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_marks_failed_when_source_thread_returns_error() {
    let schema = test_schema();
    let source = ErrorAfterHelloSource {
        mode: SourceMode::Consumer,
        schema: schema.clone(),
    };
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("source-thread-error"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "source thread failed"),
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_stop_callback_failure_can_be_retried() {
    let schema = test_schema();
    let stop_attempts = Arc::new(AtomicUsize::new(0));
    let source = RetryableStopSource {
        mode: SourceMode::Consumer,
        schema: schema.clone(),
        stop_attempts: stop_attempts.clone(),
    };
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("retry-stop-callback"),
        NoopPublisher,
    )
    .unwrap();

    thread::sleep(Duration::from_millis(50));

    let first_err = handle.stop().unwrap_err();
    match first_err {
        ZippyError::Io { reason } => assert_eq!(reason, "stop callback failed"),
        other => panic!("unexpected first stop error: {other:?}"),
    }

    handle.stop().unwrap();
    assert_eq!(stop_attempts.load(Ordering::Relaxed), 2);
    assert_eq!(handle.status(), EngineStatus::Stopped);
}

#[test]
fn source_runtime_stop_requested_does_not_swallow_source_error() {
    let schema = test_schema();
    let stop_attempts = Arc::new(AtomicUsize::new(0));
    let source = StopThenErrorSource {
        mode: SourceMode::Consumer,
        schema: schema.clone(),
        stop_attempts: stop_attempts.clone(),
    };
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("stop-then-source-error"),
        NoopPublisher,
    )
    .unwrap();

    thread::sleep(Duration::from_millis(50));

    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "source failed after stop"),
        other => panic!("unexpected stop error: {other:?}"),
    }
    assert_eq!(stop_attempts.load(Ordering::Relaxed), 1);
}

#[test]
fn source_runtime_consumer_natural_exit_without_stop_is_failed() {
    let schema = test_schema();
    let source = StaticSource::new(
        SourceMode::Consumer,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
        ],
    );
    let engine = RecordingEngine::new(schema, Arc::new(Mutex::new(RecordedCalls::default())));
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("consumer-natural-exit-failed"),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);

    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "source terminated without stop event"),
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_pipeline_xfast_drains_fast_data_before_source_terminated_failure() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let source = StaticSource::new(
        SourceMode::Pipeline,
        schema.clone(),
        vec![
            SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
        ],
    );
    let engine = RecordingEngine::new(schema, calls.clone());
    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config_with_xfast("pipeline-natural-exit-xfast", true),
        NoopPublisher,
    )
    .unwrap();

    wait_for_status(&handle, EngineStatus::Failed);
    assert_eq!(calls.lock().unwrap().data_count, 1);

    let err = handle.stop().unwrap_err();
    match err {
        ZippyError::Io { reason } => assert_eq!(reason, "source terminated without stop event"),
        other => panic!("unexpected stop error: {other:?}"),
    }
}

#[test]
fn source_runtime_handle_concurrent_joiners_observe_same_error() {
    let (ready_tx, ready_rx) = bounded(1);
    let handle = SourceHandle::new(thread::spawn(move || -> Result<()> {
        ready_rx.recv().map_err(|_| ZippyError::ChannelReceive)?;
        Err(ZippyError::Io {
            reason: "shared join failure".to_string(),
        })
    }));

    let handle_a = handle.clone();
    let joiner_a = thread::spawn(move || handle_a.join());
    let handle_b = handle.clone();
    let joiner_b = thread::spawn(move || handle_b.join());

    ready_tx.send(()).unwrap();

    let result_a = joiner_a.join().unwrap();
    let result_b = joiner_b.join().unwrap();

    match result_a {
        Err(ZippyError::Io { reason }) => assert_eq!(reason, "shared join failure"),
        other => panic!("unexpected join result a: {other:?}"),
    }
    match result_b {
        Err(ZippyError::Io { reason }) => assert_eq!(reason, "shared join failure"),
        other => panic!("unexpected join result b: {other:?}"),
    }
}

#[test]
fn source_runtime_pipeline_xfast_start_failure_cleans_up_fast_path_worker() {
    let schema = test_schema();
    let drop_count = Arc::new(AtomicUsize::new(0));
    let source = StartFailSource {
        mode: SourceMode::Pipeline,
        schema: schema.clone(),
    };
    let engine = DropTrackingEngine::new(schema, drop_count.clone());

    let result = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config_with_xfast("start-fail-xfast", true),
        NoopPublisher,
    );

    match result {
        Err(ZippyError::Io { reason }) => assert_eq!(reason, "source start failed"),
        Ok(_) => panic!("spawn should fail when source start returns an error"),
        Err(other) => panic!("unexpected spawn error: {other:?}"),
    }

    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if drop_count.load(Ordering::Relaxed) == 1 {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }

    panic!(
        "drop count did not reach expected value expected=[1] actual=[{}]",
        drop_count.load(Ordering::Relaxed)
    );
}
