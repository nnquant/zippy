use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, RecvTimeoutError, Sender};

use crate::{
    queue::{QueueSendError, QueueSender},
    Engine, EngineConfig, EngineMetrics, EngineMetricsSnapshot, EngineStatus, OverflowPolicy,
    Publisher, Result, ZippyError,
};

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
    Stop(Sender<Result<Vec<RecordBatch>>>),
}

pub struct EngineHandle {
    status: Arc<Mutex<EngineStatus>>,
    overflow_policy: OverflowPolicy,
    tx: QueueSender<Command>,
    join_handle: Option<JoinHandle<Result<()>>>,
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
        let (reply_tx, reply_rx) = bounded(1);
        let command_result = self.enqueue_command(Command::Stop(reply_tx)).and_then(|_| {
            self.recv_stop_response(reply_rx)
                .and_then(|result| result.map(|_| ()))
        });
        let join_result = self.join_worker();

        match (command_result, join_result) {
            (_, Err(err)) => Err(err),
            (Err(err), Ok(())) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub fn status(&self) -> EngineStatus {
        *self.status.lock().unwrap()
    }

    pub fn metrics(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot(self.tx.len())
    }

    fn recv_stop_response(
        &mut self,
        reply_rx: crossbeam_channel::Receiver<Result<Vec<RecordBatch>>>,
    ) -> Result<Result<Vec<RecordBatch>>> {
        loop {
            match reply_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(result) => return Ok(result),
                Err(RecvTimeoutError::Timeout) => {
                    if self
                        .join_handle
                        .as_ref()
                        .map(JoinHandle::is_finished)
                        .unwrap_or(true)
                    {
                        return Err(ZippyError::ChannelReceive);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(ZippyError::ChannelReceive);
                }
            }
        }
    }

    fn join_worker(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            return join_handle.join().map_err(|_| ZippyError::Io {
                reason: "worker thread panicked".to_string(),
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
        let is_data = matches!(&command, Command::Data(_));
        let send_result = match (is_data, self.overflow_policy) {
            (false, _) | (true, OverflowPolicy::Block) => self.tx.send(command).map(|_| None),
            (true, OverflowPolicy::Reject) => self.tx.try_send(command).map(|_| None),
            (true, OverflowPolicy::DropOldest) => self
                .tx
                .send_drop_oldest(command, |queued| matches!(queued, Command::Data(_))),
        };

        match send_result {
            Ok(Some(_)) => {
                self.metrics.inc_dropped_batches(1);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(QueueSendError::Full(_)) => Err(ZippyError::ChannelSend),
            Err(QueueSendError::Disconnected(_)) => {
                let status = self.status();
                if status != EngineStatus::Running {
                    return Err(ZippyError::InvalidState {
                        status: status.as_str(),
                    });
                }
                Err(ZippyError::ChannelSend)
            }
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
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let status_clone = status.clone();
    let metrics_clone = metrics.clone();
    let rx = queue.receiver();
    let tx = queue.sender();

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
                            if let Err(err) = publish_batches(&mut publisher, &metrics_clone, &outputs)
                                .and_then(|()| flush_publisher(&mut publisher, &metrics_clone))
                            {
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                let _ = reply_tx.send(Err(err.clone()));
                                return Err(err);
                            }
                            let _ = reply_tx.send(Ok(outputs));
                        }
                        Err(err) => {
                            metrics_clone.apply_delta(engine.drain_metrics());
                            *status_clone.lock().unwrap() = EngineStatus::Failed;
                            let _ = reply_tx.send(Err(err.clone()));
                            return Err(err);
                        }
                    },
                    Command::Stop(reply_tx) => {
                        *status_clone.lock().unwrap() = EngineStatus::Stopping;
                        match engine.on_stop() {
                            Ok(outputs) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                metrics_clone.inc_output_batches(outputs.len());
                                if let Err(err) = publish_batches(&mut publisher, &metrics_clone, &outputs)
                                    .and_then(|()| flush_publisher(&mut publisher, &metrics_clone))
                                    .and_then(|()| close_publisher(&mut publisher, &metrics_clone))
                                {
                                    *status_clone.lock().unwrap() = EngineStatus::Failed;
                                    let _ = reply_tx.send(Err(err.clone()));
                                    return Err(err);
                                }
                                let _ = reply_tx.send(Ok(outputs));
                                *status_clone.lock().unwrap() = EngineStatus::Stopped;
                                return Ok(());
                            }
                            Err(err) => {
                                metrics_clone.apply_delta(engine.drain_metrics());
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                                let _ = reply_tx.send(Err(err.clone()));
                                return Err(err);
                            }
                        }
                    }
                }
            }
            Ok(())
        })();

        if worker_result.is_err() {
            *status_clone.lock().unwrap() = EngineStatus::Failed;
        }

        worker_result
    });

    Ok(EngineHandle {
        status,
        overflow_policy: config.overflow_policy,
        tx,
        join_handle: Some(join_handle),
        metrics,
    })
}

fn publish_batches<P>(publisher: &mut P, metrics: &EngineMetrics, outputs: &[RecordBatch]) -> Result<()>
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
