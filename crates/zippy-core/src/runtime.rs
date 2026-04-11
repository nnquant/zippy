use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Sender};
use tracing::{debug, error, info};

use crate::{
    queue::{QueueSendError, QueueSender},
    Engine, EngineConfig, EngineMetrics, EngineMetricsSnapshot, EngineStatus, OverflowPolicy,
    Publisher, Result, Source, SourceEvent, SourceHandle as RuntimeSourceHandle, SourceMode,
    SourceSink, ZippyError,
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
    Data(RecordBatch),
    Flush(Sender<Result<Vec<RecordBatch>>>),
    Stop,
    SourceEvent(SourceEvent),
    SourceTerminated(Result<()>),
}

pub struct EngineHandle {
    status: Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: QueueSender<Command>,
    join_handle: Option<JoinHandle<Result<()>>>,
    source_handle: Option<RuntimeSourceHandle>,
    source_monitor_handle: Option<JoinHandle<()>>,
    metrics: Arc<EngineMetrics>,
}

struct SourceRuntimeSink {
    status: Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: QueueSender<Command>,
    metrics: Arc<EngineMetrics>,
}

impl EngineHandle {
    pub fn write(&self, batch: RecordBatch) -> Result<()> {
        self.ensure_running()?;
        self.enqueue_command(Command::Data(batch))
    }

    pub fn flush(&self) -> Result<Vec<RecordBatch>> {
        self.ensure_running()?;
        let (tx, rx) = bounded(1);
        self.enqueue_command(Command::Flush(tx))?;
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

        match self.source_handle.as_ref() {
            Some(source_handle) => {
                zippy_debug_stop_log("engine_handle.stop calling source_handle.stop");
                source_handle.stop()
            }
            None => Ok(()),
        }?;
        zippy_debug_stop_log("engine_handle.stop source_handle.stop returned");
        let command_result = self.enqueue_command(Command::Stop);
        zippy_debug_stop_log(&format!(
            "engine_handle.stop enqueue stop returned ok=[{}]",
            command_result.is_ok()
        ));
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
}

impl SourceSink for SourceRuntimeSink {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        enqueue_runtime_command(
            &self.status,
            self.overflow_policy,
            &self.tx,
            &self.metrics,
            Command::SourceEvent(event),
        )
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
                    Command::Data(batch) => {
                        metrics_clone.inc_processed_batch(batch.num_rows());
                        match engine.on_data(batch) {
                            Ok(outputs) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                metrics_clone.inc_output_batches(outputs.len());
                                publish_batches(&mut publisher, &metrics_clone, &outputs)?;
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
                                publish_batches(&mut publisher, &metrics_clone, &outputs)
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
                            let _ = reply_tx.send(Ok(outputs));
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
                                    publish_batches(&mut publisher, &metrics_clone, &outputs)
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
    })
}

pub fn spawn_source_engine_with_publisher<S, E, P>(
    source: Box<S>,
    mut engine: E,
    config: EngineConfig,
    mut publisher: P,
) -> Result<EngineHandle>
where
    S: Source + ?Sized,
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
    let source_mode = source.mode();
    let source_schema = source.output_schema();
    let engine_schema = engine.input_schema();
    let source_name = source.name().to_string();
    let sink = Arc::new(SourceRuntimeSink {
        status: status.clone(),
        overflow_policy: config.overflow_policy,
        tx: tx.clone(),
        metrics: metrics.clone(),
    });
    let source_handle = source.start(sink)?;
    let monitor_source_handle = source_handle.clone();
    let monitor_status = status.clone();
    let monitor_tx = tx.clone();
    let monitor_metrics = metrics.clone();
    let monitor_engine_name = engine_name.clone();
    let source_monitor_handle = thread::spawn(move || {
        let source_result = monitor_source_handle.join();
        if monitor_source_handle.stop_requested() && source_result.is_ok() {
            return;
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
    let worker_engine_name = engine_name.clone();
    let worker_source_name = source_name.clone();

    let join_handle = thread::spawn(move || -> Result<()> {
        let worker_result = (|| -> Result<()> {
            let mut hello_seen = false;

            while let Ok(command) = rx.recv() {
                match command {
                    Command::Data(batch) => {
                        process_data_event(&mut engine, &mut publisher, &metrics_clone, batch)?;
                    }
                    Command::Flush(reply_tx) => {
                        process_flush_event(&mut engine, &mut publisher, &metrics_clone, true)
                            .map(|outputs| {
                                let _ = reply_tx.send(Ok(outputs));
                            })
                            .inspect_err(|err| {
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                let _ = reply_tx.send(Err(err.clone()));
                            })?;
                    }
                    Command::Stop => {
                        *status_clone.lock().unwrap() = EngineStatus::Stopping;
                        process_stop_event(&mut engine, &mut publisher, &metrics_clone)
                            .map(|_outputs| {
                                *status_clone.lock().unwrap() = EngineStatus::Stopped;
                            })
                            .inspect_err(|_err| {
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                            })?;
                        return Ok(());
                    }
                    Command::SourceTerminated(source_result) => match source_result {
                        Ok(()) => {
                            return fail_worker(
                                &mut engine,
                                &status_clone,
                                ZippyError::Io {
                                    reason: "source terminated without stop event".to_string(),
                                },
                            );
                        }
                        Err(err) => {
                            return fail_worker(&mut engine, &status_clone, err);
                        }
                    },
                    Command::SourceEvent(event) => match event {
                        SourceEvent::Hello(hello) => {
                            if hello_seen {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::InvalidConfig {
                                        reason: "source hello already received".to_string(),
                                    },
                                );
                            }
                            if hello.schema != source_schema {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::SchemaMismatch {
                                        reason: "source hello schema does not match source output schema"
                                            .to_string(),
                                    },
                                );
                            }
                            if hello.schema != engine_schema {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::SchemaMismatch {
                                        reason:
                                            "source hello schema does not match engine input schema"
                                                .to_string(),
                                    },
                                );
                            }
                            hello_seen = true;
                        }
                        SourceEvent::Data(batch) => {
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
                        }
                        SourceEvent::Flush => {
                            if !hello_seen {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::InvalidConfig {
                                        reason: "source hello must arrive before flush".to_string(),
                                    },
                                );
                            }
                            if source_mode == SourceMode::Pipeline {
                                let outputs = process_flush_event(
                                    &mut engine,
                                    &mut publisher,
                                    &metrics_clone,
                                    true,
                                )
                                .inspect_err(|_| {
                                    *status_clone.lock().unwrap() = EngineStatus::Failed;
                                })?;
                                info!(
                                    component = "runtime",
                                    engine = worker_engine_name.as_str(),
                                    source = worker_source_name.as_str(),
                                    event = "flush",
                                    batch_rows =
                                        outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                                    "engine flushed"
                                );
                            }
                        }
                        SourceEvent::Stop => {
                            if !hello_seen {
                                return fail_worker(
                                    &mut engine,
                                    &status_clone,
                                    ZippyError::InvalidConfig {
                                        reason: "source hello must arrive before stop".to_string(),
                                    },
                                );
                            }
                            if source_mode == SourceMode::Pipeline {
                                *status_clone.lock().unwrap() = EngineStatus::Stopping;
                                let outputs =
                                    process_stop_event(&mut engine, &mut publisher, &metrics_clone)
                                        .inspect_err(|_| {
                                            *status_clone.lock().unwrap() = EngineStatus::Failed;
                                        })?;
                                *status_clone.lock().unwrap() = EngineStatus::Stopped;
                                info!(
                                    component = "runtime",
                                    engine = worker_engine_name.as_str(),
                                    source = worker_source_name.as_str(),
                                    event = "stop",
                                    batch_rows =
                                        outputs.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                                    "engine stopped"
                                );
                                return Ok(());
                            }
                            *status_clone.lock().unwrap() = EngineStatus::Stopped;
                            shutdown_source_pipeline(&mut publisher, &metrics_clone)?;
                            info!(
                                component = "runtime",
                                engine = worker_engine_name.as_str(),
                                source = worker_source_name.as_str(),
                                event = "stop",
                                batch_rows = 0usize,
                                "engine stopped"
                            );
                            return Ok(());
                        }
                        SourceEvent::Error(reason) => {
                            return fail_worker(
                                &mut engine,
                                &status_clone,
                                ZippyError::Io { reason },
                            );
                        }
                    },
                }
            }
            Ok(())
        })();

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
        overflow_policy: config.overflow_policy,
        tx,
        join_handle: Some(join_handle),
        source_handle: Some(source_handle),
        source_monitor_handle: Some(source_monitor_handle),
        metrics,
    })
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

fn process_data_event<E, P>(
    engine: &mut E,
    publisher: &mut P,
    metrics: &Arc<EngineMetrics>,
    batch: RecordBatch,
) -> Result<()>
where
    E: Engine,
    P: Publisher,
{
    metrics.inc_processed_batch(batch.num_rows());
    let outputs = engine.on_data(batch).inspect_err(|_| {
        metrics.apply_delta(engine.drain_metrics());
    })?;
    metrics.apply_delta(engine.drain_metrics());
    metrics.inc_output_batches(outputs.len());
    publish_batches(publisher, metrics, &outputs)
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
    publish_batches(publisher, metrics, &outputs)?;
    if flush_publisher_after {
        flush_publisher(publisher, metrics)?;
    }
    Ok(outputs)
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
    publish_batches(publisher, metrics, &outputs)?;
    flush_publisher(publisher, metrics)?;
    close_publisher(publisher, metrics)?;
    Ok(outputs)
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

fn publish_batches<P>(
    publisher: &mut P,
    metrics: &EngineMetrics,
    outputs: &[RecordBatch],
) -> Result<()>
where
    P: Publisher,
{
    for batch in outputs {
        publisher.publish(batch).inspect_err(|_| {
            metrics.inc_publish_errors(1);
        })?;
    }

    Ok(())
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
