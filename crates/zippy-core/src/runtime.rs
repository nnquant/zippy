use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Sender};
use tracing::{debug, error, info};

use crate::{
    queue::{QueueReceiver, QueueSendError, QueueSender},
    Engine, EngineConfig, EngineMetrics, EngineMetricsSnapshot, EngineStatus, OverflowPolicy,
    Publisher, Result, SegmentTableView, Source, SourceEvent, SourceHandle as RuntimeSourceHandle,
    SourceMode, SourceSink, SpscDataQueue, ZippyError,
};

fn zippy_debug_stop_enabled() -> bool {
    match std::env::var("ZIPPY_DEBUG_STOP") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    }
}

fn zippy_debug_stop_log(message: &str) {
    if zippy_debug_stop_enabled() {
        debug!(
            component = "runtime",
            event = "stop_debug",
            message = message,
            "{message}"
        );
    }
}

#[derive(Default)]
struct NoopPublisher;

impl Publisher for NoopPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        Ok(())
    }
}

enum Command {
    Data(SegmentTableView),
    Flush(Sender<Result<Vec<RecordBatch>>>),
    Stop,
    SourceEvent(SourceEvent),
    SourceEventWithFastDataBarrier(SourceEvent),
    SourceTerminated(Result<()>),
}

struct FastDataPath {
    data_queue: Arc<SpscDataQueue<SegmentTableView>>,
    worker_thread: Mutex<Option<std::thread::Thread>>,
    worker_running: Arc<AtomicBool>,
    xfast: bool,
    emit_lock: Mutex<()>,
}

impl FastDataPath {
    fn install_worker(&self, worker_thread: std::thread::Thread) {
        let _emit_guard = self.emit_lock.lock().unwrap();
        *self.worker_thread.lock().unwrap() = Some(worker_thread.clone());
        self.worker_running.store(true, Ordering::Release);
        worker_thread.unpark();
    }

    fn notify_worker(&self) {
        if let Some(worker_thread) = self.worker_thread.lock().unwrap().as_ref() {
            worker_thread.unpark();
        }
    }

    fn worker_installed(&self) -> bool {
        self.worker_thread.lock().unwrap().is_some()
    }
}

pub struct PreparedSourceRuntime {
    inner: Option<PreparedSourceRuntimeInner>,
}

struct PreparedSourceRuntimeInner {
    config: EngineConfig,
    status: Arc<Mutex<EngineStatus>>,
    tx: QueueSender<Command>,
    rx: QueueReceiver<Command>,
    metrics: Arc<EngineMetrics>,
    sink: Arc<SourceRuntimeSink>,
    source_handle: RuntimeSourceHandle,
    source_mode: SourceMode,
    source_schema: Arc<Schema>,
    source_name: String,
}

pub struct EngineHandle {
    status: Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: QueueSender<Command>,
    join_handle: Option<JoinHandle<Result<()>>>,
    source_handle: Option<RuntimeSourceHandle>,
    source_monitor_handle: Option<JoinHandle<()>>,
    metrics: Arc<EngineMetrics>,
    fast_data_path: Option<Arc<FastDataPath>>,
}

struct SourceRuntimeSink {
    status: Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: QueueSender<Command>,
    metrics: Arc<EngineMetrics>,
    fast_data_path: Option<Arc<FastDataPath>>,
}

impl EngineHandle {
    pub fn write(&self, batch: RecordBatch) -> Result<()> {
        self.ensure_running()?;
        self.enqueue_command(Command::Data(SegmentTableView::from_record_batch(batch)))
    }

    pub fn flush(&self) -> Result<Vec<RecordBatch>> {
        self.ensure_running()?;
        let (tx, rx) = bounded(1);
        self.enqueue_control_command_with_fast_data_barrier(Command::Flush(tx))?;
        rx.recv().map_err(|_| ZippyError::ChannelReceive)?
    }

    pub fn stop(&mut self) -> Result<()> {
        zippy_debug_stop_log(&format!(
            "engine_handle.stop entered status=[{}] has_source=[{}]",
            self.status().as_str(),
            self.source_handle.is_some()
        ));
        if self.status() == EngineStatus::Stopped {
            let join_result = self.join_worker();
            let source_result = self.join_source();

            return match (join_result, source_result) {
                (Err(err), _) => Err(err),
                (Ok(()), Err(err)) => Err(err),
                (Ok(()), Ok(())) => Ok(()),
            };
        }

        let command_result = self.enqueue_stop_command_with_fast_data_barrier();
        zippy_debug_stop_log(&format!(
            "engine_handle.stop enqueue stop returned ok=[{}]",
            command_result.is_ok()
        ));
        if let Err(err) = &command_result {
            if !matches!(err, ZippyError::InvalidState { .. }) {
                return Err(err.clone());
            }
        }
        let join_result = self.join_worker();
        zippy_debug_stop_log(&format!(
            "engine_handle.stop join_worker returned ok=[{}]",
            join_result.is_ok()
        ));
        let source_result = self.join_source();
        zippy_debug_stop_log(&format!(
            "engine_handle.stop join_source returned ok=[{}]",
            source_result.is_ok()
        ));

        match (command_result, join_result, source_result) {
            (_, Err(err), _) => Err(err),
            (_, Ok(()), Err(err)) => Err(err),
            (Err(err), Ok(()), Ok(())) => Err(err),
            (Ok(()), Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn status(&self) -> EngineStatus {
        *self.status.lock().unwrap()
    }

    pub fn metrics(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot(self.tx.len())
    }

    fn join_worker(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            zippy_debug_stop_log("engine_handle.join_worker waiting for worker thread");
            return join_handle.join().map_err(|_| ZippyError::Io {
                reason: "worker thread panicked".to_string(),
            })?;
        }

        Ok(())
    }

    fn join_source(&mut self) -> Result<()> {
        if let Some(source_handle) = self.source_handle.as_ref() {
            zippy_debug_stop_log("engine_handle.join_source waiting for source handle");
            source_handle.join()?;
        }
        self.source_handle = None;
        if let Some(source_monitor_handle) = self.source_monitor_handle.take() {
            zippy_debug_stop_log("engine_handle.join_source waiting for source monitor thread");
            source_monitor_handle.join().map_err(|_| ZippyError::Io {
                reason: "source monitor thread panicked".to_string(),
            })?;
        }
        Ok(())
    }

    fn ensure_running(&self) -> Result<()> {
        let status = self.status();
        if status != EngineStatus::Running {
            return Err(ZippyError::InvalidState {
                status: status.as_str(),
            });
        }

        Ok(())
    }

    fn enqueue_command(&self, command: Command) -> Result<()> {
        enqueue_runtime_command(
            &self.status,
            self.overflow_policy,
            &self.tx,
            &self.metrics,
            command,
        )
    }

    fn enqueue_control_command_with_fast_data_barrier(&self, command: Command) -> Result<()> {
        match &self.fast_data_path {
            Some(path) => {
                let _emit_guard = path.emit_lock.lock().unwrap();
                wait_for_fast_data_drain(path.as_ref());
                self.enqueue_command(command)
            }
            None => self.enqueue_command(command),
        }
    }

    fn enqueue_stop_command_with_fast_data_barrier(&self) -> Result<()> {
        match &self.fast_data_path {
            Some(path) => {
                let _emit_guard = path.emit_lock.lock().unwrap();
                match self.source_handle.as_ref() {
                    Some(source_handle) => {
                        zippy_debug_stop_log("engine_handle.stop calling source_handle.stop");
                        source_handle.stop()?;
                        zippy_debug_stop_log("engine_handle.stop source_handle.stop returned");
                    }
                    None => {}
                }
                wait_for_fast_data_drain(path.as_ref());
                self.enqueue_command(Command::Stop)
            }
            None => {
                match self.source_handle.as_ref() {
                    Some(source_handle) => {
                        zippy_debug_stop_log("engine_handle.stop calling source_handle.stop");
                        source_handle.stop()?;
                        zippy_debug_stop_log("engine_handle.stop source_handle.stop returned");
                    }
                    None => {}
                }
                self.enqueue_command(Command::Stop)
            }
        }
    }
}

impl SourceSink for SourceRuntimeSink {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        if let Some(path) = &self.fast_data_path {
            let _emit_guard = path.emit_lock.lock().unwrap();
            return match event {
                SourceEvent::Data(batch) => {
                    path.data_queue.push_blocking(batch)?;
                    path.notify_worker();
                    Ok(())
                }
                other => {
                    let command = match &other {
                        SourceEvent::Flush | SourceEvent::Stop | SourceEvent::Error(_) => {
                            if path.worker_installed() {
                                wait_for_fast_data_drain(path.as_ref());
                                Command::SourceEvent(other)
                            } else if !path.data_queue.is_empty() {
                                Command::SourceEventWithFastDataBarrier(other)
                            } else {
                                Command::SourceEvent(other)
                            }
                        }
                        _ => Command::SourceEvent(other),
                    };

                    enqueue_runtime_command(
                        &self.status,
                        self.overflow_policy,
                        &self.tx,
                        &self.metrics,
                        command,
                    )
                }
            };
        }

        match event {
            SourceEvent::Data(batch) => enqueue_runtime_command(
                &self.status,
                self.overflow_policy,
                &self.tx,
                &self.metrics,
                Command::SourceEvent(SourceEvent::Data(batch)),
            ),
            other => enqueue_runtime_command(
                &self.status,
                self.overflow_policy,
                &self.tx,
                &self.metrics,
                Command::SourceEvent(other),
            ),
        }
    }
}

pub fn spawn_engine<E>(engine: E, config: EngineConfig) -> Result<EngineHandle>
where
    E: Engine,
{
    spawn_engine_with_publisher(engine, config, NoopPublisher)
}

pub fn spawn_engine_with_publisher<E, P>(
    mut engine: E,
    config: EngineConfig,
    mut publisher: P,
) -> Result<EngineHandle>
where
    E: Engine,
    P: Publisher,
{
    config.validate()?;
    let engine_name = config.name.clone();
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let status_clone = status.clone();
    let metrics_clone = metrics.clone();
    let rx = queue.receiver();
    let tx = queue.sender();
    let worker_engine_name = engine_name.clone();

    let join_handle = thread::spawn(move || -> Result<()> {
        let worker_result = (|| -> Result<()> {
            while let Ok(command) = rx.recv() {
                match command {
                    Command::Data(table) => {
                        metrics_clone.inc_processed_batch(table.num_rows());
                        match engine.on_data(table) {
                            Ok(outputs) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                metrics_clone.inc_output_batches(outputs.len());
                                publish_tables(&mut publisher, &metrics_clone, &outputs)?;
                            }
                            Err(err) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                return Err(err);
                            }
                        }
                    }
                    Command::Flush(reply_tx) => match engine.on_flush() {
                        Ok(outputs) => {
                            metrics_clone.apply_delta(engine.drain_metrics());
                            metrics_clone.inc_output_batches(outputs.len());
                            if let Err(err) =
                                publish_tables(&mut publisher, &metrics_clone, &outputs)
                                    .and_then(|()| flush_publisher(&mut publisher, &metrics_clone))
                            {
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                let _ = reply_tx.send(Err(err.clone()));
                                return Err(err);
                            }
                            info!(
                                component = "runtime",
                                engine = worker_engine_name.as_str(),
                                event = "flush",
                                batch_rows =
                                    outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                                "engine flushed"
                            );
                            let reply = materialize_tables(&outputs);
                            let _ = reply_tx.send(reply);
                        }
                        Err(err) => {
                            metrics_clone.apply_delta(engine.drain_metrics());
                            *status_clone.lock().unwrap() = EngineStatus::Failed;
                            let _ = reply_tx.send(Err(err.clone()));
                            return Err(err);
                        }
                    },
                    Command::Stop => {
                        *status_clone.lock().unwrap() = EngineStatus::Stopping;
                        match engine.on_stop() {
                            Ok(outputs) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                metrics_clone.inc_output_batches(outputs.len());
                                if let Err(err) =
                                    publish_tables(&mut publisher, &metrics_clone, &outputs)
                                        .and_then(|()| {
                                            flush_publisher(&mut publisher, &metrics_clone)
                                        })
                                        .and_then(|()| {
                                            close_publisher(&mut publisher, &metrics_clone)
                                        })
                                {
                                    *status_clone.lock().unwrap() = EngineStatus::Failed;
                                    return Err(err);
                                }
                                *status_clone.lock().unwrap() = EngineStatus::Stopped;
                                info!(
                                    component = "runtime",
                                    engine = worker_engine_name.as_str(),
                                    event = "stop",
                                    batch_rows =
                                        outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                                    "engine stopped"
                                );
                                return Ok(());
                            }
                            Err(err) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                return Err(err);
                            }
                        }
                    }
                    Command::SourceEvent(_) => {
                        unreachable!("source events are not dispatched into spawn_engine worker");
                    }
                    Command::SourceEventWithFastDataBarrier(_) => {
                        unreachable!("source events are not dispatched into spawn_engine worker");
                    }
                    Command::SourceTerminated(_) => {
                        unreachable!(
                            "source termination is not dispatched into spawn_engine worker"
                        );
                    }
                }
            }
            Ok(())
        })();

        if let Err(err) = &worker_result {
            error!(
                component = "runtime",
                engine = worker_engine_name.as_str(),
                event = "worker_failure",
                error = %err,
                "worker failed"
            );
            *status_clone.lock().unwrap() = EngineStatus::Failed;
        }

        worker_result
    });

    info!(
        component = "runtime",
        engine = engine_name.as_str(),
        event = "start",
        status = EngineStatus::Running.as_str(),
        "engine started"
    );

    Ok(EngineHandle {
        status,
        overflow_policy: config.overflow_policy,
        tx,
        join_handle: Some(join_handle),
        source_handle: None,
        source_monitor_handle: None,
        metrics,
        fast_data_path: None,
    })
}

pub fn spawn_source_engine_with_publisher<S, E, P>(
    source: Box<S>,
    engine: E,
    config: EngineConfig,
    publisher: P,
) -> Result<EngineHandle>
where
    S: Source + ?Sized,
    E: Engine,
    P: Publisher,
{
    prepare_source_runtime(source, config)?.spawn_with_publisher(engine, publisher)
}

pub fn prepare_source_runtime<S>(
    source: Box<S>,
    config: EngineConfig,
) -> Result<PreparedSourceRuntime>
where
    S: Source + ?Sized,
{
    config.validate()?;
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let tx = queue.sender();
    let source_mode = source.mode();
    let source_schema = source.output_schema();
    let source_name = source.name().to_string();
    let sink = Arc::new(SourceRuntimeSink {
        status: status.clone(),
        overflow_policy: config.overflow_policy,
        tx: tx.clone(),
        metrics: metrics.clone(),
        fast_data_path: if source_mode == SourceMode::Pipeline
            && config.overflow_policy == OverflowPolicy::Block
        {
            Some(Arc::new(FastDataPath {
                data_queue: Arc::new(SpscDataQueue::new(config.buffer_capacity)?),
                worker_thread: Mutex::new(None),
                worker_running: Arc::new(AtomicBool::new(false)),
                xfast: config.xfast,
                emit_lock: Mutex::new(()),
            }))
        } else {
            None
        },
    });
    let source_handle = source.start(sink.clone())?;

    Ok(PreparedSourceRuntime {
        inner: Some(PreparedSourceRuntimeInner {
            config,
            status,
            tx,
            rx: queue.receiver(),
            metrics,
            sink,
            source_handle,
            source_mode,
            source_schema,
            source_name,
        }),
    })
}

impl PreparedSourceRuntime {
    pub fn abort(mut self) -> Result<()> {
        self.abort_inner()
    }

    pub fn spawn_with_publisher<E, P>(
        mut self,
        mut engine: E,
        mut publisher: P,
    ) -> Result<EngineHandle>
    where
        E: Engine,
        P: Publisher,
    {
        let PreparedSourceRuntimeInner {
            config,
            status,
            tx,
            rx,
            metrics,
            sink,
            source_handle,
            source_mode,
            source_schema,
            source_name,
        } = self
            .inner
            .take()
            .expect("prepared source runtime can only be consumed once");
        let engine_name = config.name.clone();
        let overflow_policy = config.overflow_policy;
        let xfast = config.xfast;
        let engine_schema = engine.input_schema();
        let status_clone = status.clone();
        let metrics_clone = metrics.clone();
        let worker_fast_data_queue = sink
            .fast_data_path
            .as_ref()
            .map(|path| path.data_queue.clone());
        let worker_fast_data_running = sink
            .fast_data_path
            .as_ref()
            .map(|path| path.worker_running.clone());
        let worker_engine_name = engine_name.clone();
        let worker_source_name = source_name.clone();

        let join_handle = thread::spawn(move || -> Result<()> {
            let worker_result = (|| -> Result<()> {
                let mut hello_seen = false;

                match &worker_fast_data_queue {
                    Some(data_queue) => loop {
                        if let Some(command) = rx.try_recv() {
                            if handle_source_control_command(
                                &mut engine,
                                &mut publisher,
                                &metrics_clone,
                                &status_clone,
                                Some(data_queue.as_ref()),
                                source_mode,
                                &source_schema,
                                &engine_schema,
                                worker_engine_name.as_str(),
                                worker_source_name.as_str(),
                                &mut hello_seen,
                                command,
                            )? {
                                return Ok(());
                            }
                            continue;
                        }

                        if let Some(batch) = data_queue.try_pop() {
                            if !hello_seen {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::InvalidConfig {
                                        reason: "source hello must arrive before data".to_string(),
                                    },
                                );
                            }
                            process_data_event(&mut engine, &mut publisher, &metrics_clone, batch)
                                .inspect_err(|_| {
                                    *status_clone.lock().unwrap() = EngineStatus::Failed;
                                })?;
                            continue;
                        }

                        runtime_idle_wait(xfast);
                    },
                    None => {
                        while let Ok(command) = rx.recv() {
                            if handle_source_control_command(
                                &mut engine,
                                &mut publisher,
                                &metrics_clone,
                                &status_clone,
                                None,
                                source_mode,
                                &source_schema,
                                &engine_schema,
                                worker_engine_name.as_str(),
                                worker_source_name.as_str(),
                                &mut hello_seen,
                                command,
                            )? {
                                return Ok(());
                            }
                        }
                        Ok(())
                    }
                }
            })();

            if let Some(data_queue) = &worker_fast_data_queue {
                data_queue.close();
                drain_fast_data_queue(data_queue.as_ref());
            }
            if let Some(worker_running) = &worker_fast_data_running {
                worker_running.store(false, Ordering::Release);
            }

            if let Err(err) = &worker_result {
                error!(
                    component = "runtime",
                    engine = worker_engine_name.as_str(),
                    source = worker_source_name.as_str(),
                    event = "worker_failure",
                    error = %err,
                    "worker failed"
                );
                *status_clone.lock().unwrap() = EngineStatus::Failed;
            }

            worker_result
        });
        if let Some(path) = &sink.fast_data_path {
            path.install_worker(join_handle.thread().clone());
        }

        let monitor_source_handle = source_handle.clone();
        let monitor_status = status.clone();
        let monitor_tx = tx.clone();
        let monitor_metrics = metrics.clone();
        let monitor_fast_data_queue = sink
            .fast_data_path
            .as_ref()
            .map(|path| path.data_queue.clone());
        let monitor_fast_data_running = sink
            .fast_data_path
            .as_ref()
            .map(|path| path.worker_running.clone());
        let monitor_engine_name = engine_name.clone();
        let source_monitor_handle = thread::spawn(move || {
            let source_result = monitor_source_handle.join();
            if monitor_source_handle.stop_requested() && source_result.is_ok() {
                return;
            }

            if let (Some(data_queue), Some(worker_running)) =
                (&monitor_fast_data_queue, &monitor_fast_data_running)
            {
                wait_for_fast_data_queue_drain(data_queue.as_ref(), worker_running.as_ref(), xfast);
            }

            let current_status = *monitor_status.lock().unwrap();
            if current_status != EngineStatus::Running {
                return;
            }

            let _ = enqueue_runtime_command(
                &monitor_status,
                OverflowPolicy::Block,
                &monitor_tx,
                &monitor_metrics,
                Command::SourceTerminated(source_result),
            );
            debug!(
                component = "runtime",
                engine = monitor_engine_name.as_str(),
                event = "source_terminated",
                "source termination forwarded"
            );
        });

        info!(
            component = "runtime",
            engine = engine_name.as_str(),
            source = source_name.as_str(),
            event = "start",
            status = EngineStatus::Running.as_str(),
            "engine started"
        );

        Ok(EngineHandle {
            status,
            overflow_policy,
            tx,
            join_handle: Some(join_handle),
            source_handle: Some(source_handle),
            source_monitor_handle: Some(source_monitor_handle),
            metrics,
            fast_data_path: sink.fast_data_path.clone(),
        })
    }

    fn abort_inner(&mut self) -> Result<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(());
        };

        *inner.status.lock().unwrap() = EngineStatus::Stopping;
        inner.source_handle.stop()?;
        let join_result = inner.source_handle.join();
        *inner.status.lock().unwrap() = if join_result.is_ok() {
            EngineStatus::Stopped
        } else {
            EngineStatus::Failed
        };
        self.inner = None;
        join_result
    }
}

impl Drop for PreparedSourceRuntime {
    fn drop(&mut self) {
        if let Err(err) = self.abort_inner() {
            error!(
                component = "runtime",
                event = "prepared_runtime_drop_cleanup_failed",
                error = %err,
                "prepared runtime cleanup failed"
            );
        }
    }
}

fn enqueue_runtime_command(
    status: &Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: &QueueSender<Command>,
    metrics: &Arc<EngineMetrics>,
    command: Command,
) -> Result<()> {
    let is_data = command_is_data(&command);
    let send_result = match (is_data, overflow_policy) {
        (false, _) | (true, OverflowPolicy::Block) => tx.send(command).map(|_| None),
        (true, OverflowPolicy::Reject) => tx.try_send(command).map(|_| None),
        (true, OverflowPolicy::DropOldest) => tx.send_drop_oldest(command, command_is_data),
    };

    match send_result {
        Ok(Some(_)) => {
            metrics.inc_dropped_batches(1);
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(QueueSendError::Full(_)) => Err(ZippyError::ChannelSend),
        Err(QueueSendError::Disconnected(_)) => {
            let current_status = *status.lock().unwrap();
            if current_status != EngineStatus::Running {
                return Err(ZippyError::InvalidState {
                    status: current_status.as_str(),
                });
            }
            Err(ZippyError::ChannelSend)
        }
    }
}

fn command_is_data(command: &Command) -> bool {
    matches!(
        command,
        Command::Data(_) | Command::SourceEvent(SourceEvent::Data(_))
    )
}

fn wait_for_fast_data_drain(path: &FastDataPath) {
    wait_for_fast_data_queue_drain(
        path.data_queue.as_ref(),
        path.worker_running.as_ref(),
        path.xfast,
    );
}

fn runtime_idle_wait(xfast: bool) {
    std::hint::spin_loop();
    if !xfast {
        thread::park_timeout(Duration::from_micros(50));
    }
}

fn wait_for_fast_data_queue_drain(
    data_queue: &SpscDataQueue<SegmentTableView>,
    worker_running: &AtomicBool,
    xfast: bool,
) {
    while worker_running.load(Ordering::Acquire) && !data_queue.is_empty() {
        runtime_idle_wait(xfast);
    }
}

fn drain_fast_data_queue(data_queue: &SpscDataQueue<SegmentTableView>) {
    while data_queue.try_pop().is_some() {}
}

fn handle_source_control_command<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
    status: &Arc<Mutex<EngineStatus>>,
    fast_data_queue: Option<&SpscDataQueue<SegmentTableView>>,
    source_mode: SourceMode,
    source_schema: &Arc<Schema>,
    engine_schema: &Arc<Schema>,
    worker_engine_name: &str,
    worker_source_name: &str,
    hello_seen: &mut bool,
    command: Command,
) -> Result<bool>
where
    E: Engine,
    P: Publisher,
{
    match command {
        Command::Data(batch) => {
            process_data_event(engine, publisher, metrics, batch)?;
        }
        Command::Flush(reply_tx) => {
            process_flush_event(engine, publisher, metrics, true)
                .map(|outputs| {
                    let _ = reply_tx.send(Ok(outputs));
                })
                .inspect_err(|err| {
                    *status.lock().unwrap() = EngineStatus::Failed;
                    let _ = reply_tx.send(Err(err.clone()));
                })?;
        }
        Command::Stop => {
            *status.lock().unwrap() = EngineStatus::Stopping;
            process_stop_event(engine, publisher, metrics)
                .map(|_outputs| {
                    *status.lock().unwrap() = EngineStatus::Stopped;
                })
                .inspect_err(|_err| {
                    *status.lock().unwrap() = EngineStatus::Failed;
                })?;
            return Ok(true);
        }
        Command::SourceTerminated(source_result) => match source_result {
            Ok(()) => {
                fail_worker(
                    engine,
                    status,
                    ZippyError::Io {
                        reason: "source terminated without stop event".to_string(),
                    },
                )?;
            }
            Err(err) => {
                fail_worker(engine, status, err)?;
            }
        },
        Command::SourceEventWithFastDataBarrier(event) => {
            let data_queue = fast_data_queue.expect("fast data barrier requires fast data queue");
            drain_fast_data_events(engine, publisher, metrics, status, data_queue, hello_seen)?;
            return handle_source_control_command(
                engine,
                publisher,
                metrics,
                status,
                fast_data_queue,
                source_mode,
                source_schema,
                engine_schema,
                worker_engine_name,
                worker_source_name,
                hello_seen,
                Command::SourceEvent(event),
            );
        }
        Command::SourceEvent(event) => match event {
            SourceEvent::Hello(hello) => {
                if *hello_seen {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::InvalidConfig {
                            reason: "source hello already received".to_string(),
                        },
                    )?;
                }
                if hello.schema != *source_schema {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::SchemaMismatch {
                            reason: "source hello schema does not match source output schema"
                                .to_string(),
                        },
                    )?;
                }
                if hello.schema != *engine_schema {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::SchemaMismatch {
                            reason: "source hello schema does not match engine input schema"
                                .to_string(),
                        },
                    )?;
                }
                *hello_seen = true;
            }
            SourceEvent::Data(batch) => {
                if !*hello_seen {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::InvalidConfig {
                            reason: "source hello must arrive before data".to_string(),
                        },
                    )?;
                }
                process_data_event(engine, publisher, metrics, batch).inspect_err(|_| {
                    *status.lock().unwrap() = EngineStatus::Failed;
                })?;
            }
            SourceEvent::Flush => {
                if !*hello_seen {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::InvalidConfig {
                            reason: "source hello must arrive before flush".to_string(),
                        },
                    )?;
                }
                if source_mode == SourceMode::Pipeline {
                    let outputs = process_flush_event(engine, publisher, metrics, true)
                        .inspect_err(|_| {
                            *status.lock().unwrap() = EngineStatus::Failed;
                        })?;
                    info!(
                        component = "runtime",
                        engine = worker_engine_name,
                        source = worker_source_name,
                        event = "flush",
                        batch_rows = outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        "engine flushed"
                    );
                }
            }
            SourceEvent::Stop => {
                if !*hello_seen {
                    fail_worker(
                        engine,
                        status,
                        ZippyError::InvalidConfig {
                            reason: "source hello must arrive before stop".to_string(),
                        },
                    )?;
                }
                if source_mode == SourceMode::Pipeline {
                    *status.lock().unwrap() = EngineStatus::Stopping;
                    let outputs =
                        process_stop_event(engine, publisher, metrics).inspect_err(|_| {
                            *status.lock().unwrap() = EngineStatus::Failed;
                        })?;
                    *status.lock().unwrap() = EngineStatus::Stopped;
                    info!(
                        component = "runtime",
                        engine = worker_engine_name,
                        source = worker_source_name,
                        event = "stop",
                        batch_rows = outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                        "engine stopped"
                    );
                    return Ok(true);
                }
                *status.lock().unwrap() = EngineStatus::Stopped;
                shutdown_source_pipeline(publisher, metrics)?;
                info!(
                    component = "runtime",
                    engine = worker_engine_name,
                    source = worker_source_name,
                    event = "stop",
                    batch_rows = 0usize,
                    "engine stopped"
                );
                return Ok(true);
            }
            SourceEvent::Error(reason) => {
                fail_worker(engine, status, ZippyError::Io { reason })?;
            }
        },
    }

    Ok(false)
}

fn drain_fast_data_events<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
    status: &Arc<Mutex<EngineStatus>>,
    data_queue: &SpscDataQueue<SegmentTableView>,
    hello_seen: &mut bool,
) -> Result<()>
where
    E: Engine,
    P: Publisher,
{
    while let Some(batch) = data_queue.try_pop() {
        if !*hello_seen {
            return fail_worker(
                engine,
                status,
                ZippyError::InvalidConfig {
                    reason: "source hello must arrive before data".to_string(),
                },
            );
        }
        process_data_event(engine, publisher, metrics, batch).inspect_err(|_| {
            *status.lock().unwrap() = EngineStatus::Failed;
        })?;
    }

    Ok(())
}

fn process_data_event<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
    table: SegmentTableView,
) -> Result<()>
where
    E: Engine,
    P: Publisher,
{
    metrics.inc_processed_batch(table.num_rows());
    let outputs = engine.on_data(table).inspect_err(|_| {
        metrics.apply_delta(engine.drain_metrics());
    })?;
    metrics.apply_delta(engine.drain_metrics());
    metrics.inc_output_batches(outputs.len());
    publish_tables(publisher, metrics, &outputs)
}

fn process_flush_event<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
    flush_publisher_after: bool,
) -> Result<Vec<RecordBatch>>
where
    E: Engine,
    P: Publisher,
{
    let outputs = engine.on_flush().inspect_err(|_| {
        metrics.apply_delta(engine.drain_metrics());
    })?;
    metrics.apply_delta(engine.drain_metrics());
    metrics.inc_output_batches(outputs.len());
    publish_tables(publisher, metrics, &outputs)?;
    if flush_publisher_after {
        flush_publisher(publisher, metrics)?;
    }
    materialize_tables(&outputs)
}

fn process_stop_event<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
) -> Result<Vec<RecordBatch>>
where
    E: Engine,
    P: Publisher,
{
    let outputs = engine.on_stop().inspect_err(|_| {
        metrics.apply_delta(engine.drain_metrics());
    })?;
    metrics.apply_delta(engine.drain_metrics());
    metrics.inc_output_batches(outputs.len());
    publish_tables(publisher, metrics, &outputs)?;
    flush_publisher(publisher, metrics)?;
    close_publisher(publisher, metrics)?;
    materialize_tables(&outputs)
}

fn shutdown_source_pipeline<P>(publisher: &mut P, metrics: &Arc<EngineMetrics>) -> Result<()>
where
    P: Publisher,
{
    flush_publisher(publisher, metrics)?;
    close_publisher(publisher, metrics)
}

fn fail_worker<E>(engine: &mut E, status: &Arc<Mutex<EngineStatus>>, err: ZippyError) -> Result<()>
where
    E: Engine,
{
    let _ = engine.drain_metrics();
    *status.lock().unwrap() = EngineStatus::Failed;
    Err(err)
}

fn publish_tables<P>(
    publisher: &mut P,
    metrics: &EngineMetrics,
    outputs: &[SegmentTableView],
) -> Result<()>
where
    P: Publisher,
{
    for table in outputs {
        let batch = table.to_record_batch()?;
        publisher.publish(&batch).inspect_err(|_| {
            metrics.inc_publish_errors(1);
        })?;
    }

    Ok(())
}

fn materialize_tables(outputs: &[SegmentTableView]) -> Result<Vec<RecordBatch>> {
    outputs
        .iter()
        .map(SegmentTableView::to_record_batch)
        .collect::<Result<Vec<_>>>()
}

fn flush_publisher<P>(publisher: &mut P, metrics: &EngineMetrics) -> Result<()>
where
    P: Publisher,
{
    publisher.flush().inspect_err(|_| {
        metrics.inc_publish_errors(1);
    })
}

fn close_publisher<P>(publisher: &mut P, metrics: &EngineMetrics) -> Result<()>
where
    P: Publisher,
{
    publisher.close().inspect_err(|_| {
        metrics.inc_publish_errors(1);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crossbeam_channel::bounded;

    #[test]
    fn fast_data_path_install_worker_waits_for_emit_lock() {
        let path = Arc::new(FastDataPath {
            data_queue: Arc::new(SpscDataQueue::new(1).unwrap()),
            worker_thread: Mutex::new(None),
            worker_running: Arc::new(AtomicBool::new(false)),
            xfast: false,
            emit_lock: Mutex::new(()),
        });
        let emit_guard = path.emit_lock.lock().unwrap();
        let (done_tx, done_rx) = bounded(1);
        let install_path = path.clone();
        let installer = thread::spawn(move || {
            install_path.install_worker(thread::current());
            done_tx.send(()).unwrap();
        });

        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "install_worker should not finish while emit_lock is held"
        );

        drop(emit_guard);

        done_rx
            .recv_timeout(Duration::from_millis(300))
            .expect("install_worker should finish after emit_lock is released");
        installer.join().unwrap();
        assert!(path.worker_installed());
        assert!(path.worker_running.load(Ordering::Acquire));
    }
}
