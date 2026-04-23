use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{
    spawn_source_engine_with_publisher, Engine, EngineConfig, EngineMetricsDelta, EngineStatus,
    OverflowPolicy, Publisher, Result, Source, SourceEvent, SourceHandle, SourceMode, SourceSink,
    StreamHello, ZippyError,
};

struct BlockingEngine {
    schema: Arc<Schema>,
    state: Arc<BlockingEngineState>,
    fail_after_first_release: bool,
}

struct BlockingEngineState {
    first_data_started: Mutex<bool>,
    first_data_release_rx: Mutex<Receiver<()>>,
    records: Mutex<Vec<String>>,
}

impl BlockingEngine {
    fn new(schema: Arc<Schema>, state: Arc<BlockingEngineState>, fail_after_first_release: bool) -> Self {
        Self {
            schema,
            state,
            fail_after_first_release,
        }
    }
}

impl Engine for BlockingEngine {
    fn name(&self) -> &str {
        "blocking-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        self.state
            .records
            .lock()
            .unwrap()
            .push(format!("data:{value}"));
        if value == 1 {
            *self.state.first_data_started.lock().unwrap() = true;
            self.state
                .first_data_release_rx
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| ZippyError::ChannelReceive)?;
            if self.fail_after_first_release {
                return Err(ZippyError::Io {
                    reason: "first data failed".to_string(),
                });
            }
        }
        Ok(Vec::new())
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        self.state.records.lock().unwrap().push("flush".to_string());
        Ok(Vec::new())
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        self.state.records.lock().unwrap().push("stop".to_string());
        Ok(Vec::new())
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        EngineMetricsDelta::default()
    }
}

#[derive(Default)]
struct NoopPublisher;

impl Publisher for NoopPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        Ok(())
    }
}

struct ManualSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    state: Arc<ManualSourceState>,
}

struct ManualSourceState {
    sink: Mutex<Option<Arc<dyn SourceSink>>>,
    release_tx: Sender<()>,
    release_rx: Mutex<Option<Receiver<()>>>,
}

impl ManualSourceState {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        let sink = self
            .sink
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .expect("source sink should be initialized");
        sink.emit(event)
    }
}

impl ManualSource {
    fn new(mode: SourceMode, schema: Arc<Schema>, state: Arc<ManualSourceState>) -> Self {
        Self { mode, schema, state }
    }
}

impl Source for ManualSource {
    fn name(&self) -> &str {
        "manual-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        *self.state.sink.lock().unwrap() = Some(sink);
        let release_rx = self
            .state
            .release_rx
            .lock()
            .unwrap()
            .take()
            .expect("release receiver should be initialized");
        let join_handle = thread::spawn(move || -> Result<()> {
            release_rx.recv().map_err(|_| ZippyError::ChannelReceive)?;
            Ok(())
        });
        let release_tx = self.state.release_tx.clone();
        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || release_tx.send(()).map_err(|_| ZippyError::ChannelSend)),
        ))
    }
}

struct TestPipelineRuntime {
    handle: Option<zippy_core::EngineHandle>,
    engine_state: Arc<BlockingEngineState>,
    source_state: Arc<ManualSourceState>,
}

impl TestPipelineRuntime {
    fn source_emit_data(&self, batch: RecordBatch) -> Result<()> {
        self.source_state.emit(SourceEvent::Data(batch))
    }

    fn source_emit_flush(&self) -> Result<()> {
        self.source_state.emit(SourceEvent::Flush)
    }

    fn source_emit_stop(&self) -> Result<()> {
        self.source_state.emit(SourceEvent::Stop)?;
        self.source_state
            .release_tx
            .send(())
            .map_err(|_| ZippyError::ChannelSend)
    }

    fn source_emit_stop_async(&self) -> Receiver<Result<()>> {
        let (tx, rx) = channel();
        let source_state = self.source_state.clone();
        thread::spawn(move || {
            let result = source_state.emit(SourceEvent::Stop);
            let _ = source_state.release_tx.send(());
            tx.send(result).unwrap();
        });
        rx
    }

    fn source_emit_data_async(&self, batch: RecordBatch) -> Receiver<Result<()>> {
        let (tx, rx) = channel();
        let source_state = self.source_state.clone();
        thread::spawn(move || {
            tx.send(source_state.emit(SourceEvent::Data(batch))).unwrap();
        });
        rx
    }

    fn wait_until_first_data_started(&self) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if *self.engine_state.first_data_started.lock().unwrap() {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }

        panic!("first data did not start within deadline");
    }

    fn wait_for_records_len(&self, expected_len: usize) -> Vec<String> {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            let snapshot = self.engine_state.records.lock().unwrap().clone();
            if snapshot.len() >= expected_len {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(5));
        }

        panic!(
            "record count did not reach expected value expected=[{}] actual=[{}]",
            expected_len,
            self.engine_state.records.lock().unwrap().len()
        );
    }

    fn wait_until_stopped(&self) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if self.handle.as_ref().unwrap().status() == EngineStatus::Stopped {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }

        panic!(
            "engine status did not reach expected state expected=[{}] actual=[{}]",
            EngineStatus::Stopped.as_str(),
            self.handle.as_ref().unwrap().status().as_str()
        );
    }

    fn wait_until_failed(&self) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            if self.handle.as_ref().unwrap().status() == EngineStatus::Failed {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }

        panic!(
            "engine status did not reach expected state expected=[{}] actual=[{}]",
            EngineStatus::Failed.as_str(),
            self.handle.as_ref().unwrap().status().as_str()
        );
    }
}

impl Drop for TestPipelineRuntime {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.as_mut() {
            let _ = handle.stop();
        }
    }
}

fn spawn_test_runtime_with_behavior(
    source_mode: SourceMode,
    overflow_policy: OverflowPolicy,
    xfast: bool,
    fail_after_first_release: bool,
) -> (TestPipelineRuntime, Sender<()>) {
    let schema = test_schema();
    let (source_release_tx, source_release_rx) = channel();
    let source_state = Arc::new(ManualSourceState {
        sink: Mutex::new(None),
        release_tx: source_release_tx,
        release_rx: Mutex::new(Some(source_release_rx)),
    });
    let source = ManualSource::new(source_mode, schema.clone(), source_state.clone());

    let (first_data_release_tx, first_data_release_rx) = channel();
    let engine_state = Arc::new(BlockingEngineState {
        first_data_started: Mutex::new(false),
        first_data_release_rx: Mutex::new(first_data_release_rx),
        records: Mutex::new(Vec::new()),
    });
    let engine = BlockingEngine::new(schema.clone(), engine_state.clone(), fail_after_first_release);

    let handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config(overflow_policy, xfast),
        NoopPublisher,
    )
    .unwrap();

    source_state
        .emit(SourceEvent::Hello(
            StreamHello::new("bars", schema, 1).unwrap(),
        ))
        .unwrap();

    let runtime = TestPipelineRuntime {
        handle: Some(handle),
        engine_state,
        source_state,
    };
    (runtime, first_data_release_tx)
}

fn spawn_test_pipeline_runtime() -> (TestPipelineRuntime, Sender<()>) {
    spawn_test_runtime_with_behavior(SourceMode::Pipeline, OverflowPolicy::Block, false, false)
}

fn spawn_test_pipeline_runtime_with_behavior(
    fail_after_first_release: bool,
) -> (TestPipelineRuntime, Sender<()>) {
    spawn_test_runtime_with_behavior(
        SourceMode::Pipeline,
        OverflowPolicy::Block,
        false,
        fail_after_first_release,
    )
}

fn spawn_test_consumer_runtime() -> (TestPipelineRuntime, Sender<()>) {
    spawn_test_runtime_with_behavior(SourceMode::Consumer, OverflowPolicy::Block, false, false)
}

fn test_engine_config(overflow_policy: OverflowPolicy, xfast: bool) -> EngineConfig {
    EngineConfig {
        name: "runtime-fast-path".to_string(),
        buffer_capacity: 1,
        overflow_policy,
        late_data_policy: Default::default(),
        xfast,
    }
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn single_row_batch(value: i64) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .unwrap()
}

#[test]
fn source_pipeline_block_mode_uses_fast_data_path() {
    let (runtime, first_data_release_tx) = spawn_test_pipeline_runtime();
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    runtime.source_emit_flush().unwrap();

    let second_data_result = runtime.source_emit_data_async(single_row_batch(2));
    match second_data_result.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("second data send failed unexpectedly: {err}"),
        Err(_) => panic!("second data send should not block on a full control queue"),
    }

    first_data_release_tx.send(()).unwrap();
    let records = runtime.wait_for_records_len(3);
    assert_eq!(records, vec!["data:1", "flush", "data:2"]);

    runtime.source_emit_stop().unwrap();
    runtime.wait_until_stopped();
}

#[test]
fn source_pipeline_fast_path_keeps_control_events_on_control_queue() {
    let (runtime, first_data_release_tx) = spawn_test_pipeline_runtime();
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    runtime.source_emit_flush().unwrap();

    let stop_result = runtime.source_emit_stop_async();
    assert!(stop_result.recv_timeout(Duration::from_millis(100)).is_err());

    first_data_release_tx.send(()).unwrap();
    match stop_result.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("stop send failed unexpectedly: {err}"),
        Err(_) => panic!("stop send did not complete after control queue drained"),
    }

    runtime.wait_until_stopped();
    let records = runtime.wait_for_records_len(3);
    assert_eq!(records, vec!["data:1", "flush", "stop"]);
}

#[test]
fn source_pipeline_block_mode_with_xfast_keeps_control_ordering() {
    let (runtime, first_data_release_tx) =
        spawn_test_runtime_with_behavior(SourceMode::Pipeline, OverflowPolicy::Block, true, false);
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    runtime.source_emit_flush().unwrap();
    let stop_result = runtime.source_emit_stop_async();
    assert!(stop_result.recv_timeout(Duration::from_millis(100)).is_err());

    first_data_release_tx.send(()).unwrap();
    assert!(matches!(
        stop_result.recv_timeout(Duration::from_secs(1)),
        Ok(Ok(()))
    ));

    runtime.wait_until_stopped();
    let records = runtime.wait_for_records_len(3);
    assert_eq!(records, vec!["data:1", "flush", "stop"]);
}

#[test]
fn engine_handle_flush_with_xfast_does_not_overtake_fast_queue_data() {
    let (mut runtime, first_data_release_tx) =
        spawn_test_runtime_with_behavior(SourceMode::Pipeline, OverflowPolicy::Block, true, false);
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    let second_data_result = runtime.source_emit_data_async(single_row_batch(2));
    match second_data_result.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("second data send failed unexpectedly: {err}"),
        Err(_) => panic!("second data send should enqueue onto the fast data queue"),
    }

    thread::scope(|scope| {
        let handle = runtime.handle.as_ref().unwrap();
        let (flush_tx, flush_rx) = channel();
        scope.spawn(move || {
            flush_tx.send(handle.flush()).unwrap();
        });

        assert!(flush_rx.recv_timeout(Duration::from_millis(100)).is_err());

        first_data_release_tx.send(()).unwrap();

        match flush_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(outputs)) => assert!(outputs.is_empty()),
            Ok(Err(err)) => panic!("flush failed unexpectedly: {err}"),
            Err(_) => panic!("flush did not complete after fast data drained"),
        }
    });

    let records = runtime.wait_for_records_len(3);
    assert_eq!(records, vec!["data:1", "data:2", "flush"]);

    runtime.handle.as_mut().unwrap().stop().unwrap();
}

#[test]
fn engine_handle_stop_with_xfast_does_not_overtake_fast_queue_data() {
    let (mut runtime, first_data_release_tx) =
        spawn_test_runtime_with_behavior(SourceMode::Pipeline, OverflowPolicy::Block, true, false);
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    let second_data_result = runtime.source_emit_data_async(single_row_batch(2));
    match second_data_result.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("second data send failed unexpectedly: {err}"),
        Err(_) => panic!("second data send should enqueue onto the fast data queue"),
    }

    thread::scope(|scope| {
        let handle = runtime.handle.as_mut().unwrap();
        let (stop_tx, stop_rx) = channel();
        scope.spawn(move || {
            stop_tx.send(handle.stop()).unwrap();
        });

        assert!(stop_rx.recv_timeout(Duration::from_millis(100)).is_err());

        first_data_release_tx.send(()).unwrap();

        match stop_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => panic!("stop failed unexpectedly: {err}"),
            Err(_) => panic!("stop did not complete after fast data drained"),
        }
    });

    runtime.wait_until_stopped();
    let records = runtime.wait_for_records_len(3);
    assert_eq!(records, vec!["data:1", "data:2", "stop"]);
}

#[test]
fn source_pipeline_worker_failure_with_pending_fast_data_does_not_hang_control_events() {
    let (runtime, first_data_release_tx) = spawn_test_pipeline_runtime_with_behavior(true);
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    let second_data_result = runtime.source_emit_data_async(single_row_batch(2));
    match second_data_result.recv_timeout(Duration::from_millis(100)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("second data send failed unexpectedly: {err}"),
        Err(_) => panic!("second data send should enqueue before worker failure"),
    }

    first_data_release_tx.send(()).unwrap();
    runtime.wait_until_failed();

    let stop_result = runtime.source_emit_stop_async();
    match stop_result.recv_timeout(Duration::from_millis(100)) {
        Ok(_) => {}
        Err(_) => panic!("stop send should not hang after worker failure"),
    }
}

#[test]
fn source_consumer_block_mode_keeps_data_on_control_queue() {
    let (runtime, first_data_release_tx) = spawn_test_consumer_runtime();
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    runtime.source_emit_flush().unwrap();

    let second_data_result = runtime.source_emit_data_async(single_row_batch(2));
    assert!(second_data_result
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    first_data_release_tx.send(()).unwrap();
    match second_data_result.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => panic!("second data send failed unexpectedly: {err}"),
        Err(_) => panic!("second data send did not complete after control queue drained"),
    }

    let records = runtime.wait_for_records_len(2);
    assert_eq!(records, vec!["data:1", "data:2"]);

    runtime.source_emit_stop().unwrap();
    runtime.wait_until_stopped();
}
