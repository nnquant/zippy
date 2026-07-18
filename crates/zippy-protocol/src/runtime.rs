//! Independent lifecycle for the Arrow Flight network listener.

use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_server::FlightServiceServer;
use tokio::runtime::Builder;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use zippy_types::{ErrorCode, Result, ServerError};

use crate::server::{FlightServiceStatus, ZippyFlightService};

const FLIGHT_THREAD_NAME: &str = "zippy-flight";

/// Outcome of one explicit Flight listener shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightShutdownOutcome {
    /// Number of unconsumed tickets released before listener shutdown.
    pub released_pending_tickets: usize,
}

/// One independently bound Arrow Flight listener and its owning runtime thread.
pub struct FlightServerRuntime {
    endpoint: SocketAddr,
    service: ZippyFlightService,
    shutdown: watch::Sender<bool>,
    completed: Receiver<Result<()>>,
    thread: Option<JoinHandle<()>>,
}

impl FlightServerRuntime {
    /// Binds the endpoint before returning and starts an owned Tokio runtime.
    pub fn start(
        endpoint: SocketAddr,
        service: ZippyFlightService,
        startup_timeout: Duration,
    ) -> Result<Self> {
        if startup_timeout.is_zero() {
            return Err(invalid("Flight startup timeout must be positive"));
        }
        let listener = TcpListener::bind(endpoint).map_err(|error| {
            unavailable(format!(
                "bind Flight listener failed endpoint=[{endpoint}] error=[{error}]"
            ))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            unavailable(format!("configure Flight listener failed error=[{error}]"))
        })?;
        let endpoint = listener.local_addr().map_err(|error| {
            unavailable(format!(
                "resolve Flight listener endpoint failed error=[{error}]"
            ))
        })?;
        let (shutdown, shutdown_rx) = watch::channel(true);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (completed_tx, completed) = mpsc::sync_channel(1);
        let thread_service = service.clone();
        let max_message_bytes = service_configured_message_bytes(&service);
        let thread = thread::Builder::new()
            .name(FLIGHT_THREAD_NAME.to_owned())
            .spawn(move || {
                run_listener(
                    listener,
                    thread_service,
                    max_message_bytes,
                    shutdown_rx,
                    ready_tx,
                    completed_tx,
                );
            })
            .map_err(|error| {
                unavailable(format!(
                    "spawn Flight listener thread failed error=[{error}]"
                ))
            })?;
        match ready_rx.recv_timeout(startup_timeout) {
            Ok(Ok(())) => Ok(Self {
                endpoint,
                service,
                shutdown,
                completed,
                thread: Some(thread),
            }),
            Ok(Err(error)) => {
                service.close_admission();
                let _ = shutdown.send(false);
                detach_unfinished_thread(thread, "startup failure");
                Err(error)
            }
            Err(RecvTimeoutError::Timeout) => {
                service.close_admission();
                let _ = shutdown.send(false);
                detach_unfinished_thread(thread, "startup timeout");
                Err(unavailable("Flight listener startup timed out"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                service.close_admission();
                let _ = shutdown.send(false);
                detach_unfinished_thread(thread, "startup channel disconnect");
                Err(internal("Flight listener exited before startup readiness"))
            }
        }
    }

    /// Returns the bound endpoint, including a kernel-assigned port when port zero was requested.
    #[must_use]
    pub const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    /// Returns listener admission and bounded ticket counters.
    #[must_use]
    pub fn status(&self) -> FlightServiceStatus {
        self.service.status()
    }

    /// Closes query and ticket admission while leaving the listener alive for
    /// deterministic ordered shutdown.
    pub fn close_admission(&self) {
        self.service.close_admission();
    }

    /// Returns whether the listener thread has not terminated.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.thread
            .as_ref()
            .is_some_and(|thread| !thread.is_finished())
    }

    /// Stops admission before closing the listener and waits for its owned thread.
    pub fn shutdown(&mut self, timeout: Duration) -> Result<FlightShutdownOutcome> {
        if timeout.is_zero() {
            return Err(invalid("Flight shutdown timeout must be positive"));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| invalid("Flight shutdown timeout exceeds monotonic clock range"))?;
        let before = self.service.status().pending_tickets;
        self.service.close_admission();
        let _ = self.shutdown.send(false);
        let thread = self
            .thread
            .as_ref()
            .ok_or_else(|| internal("Flight listener was already joined"))?;
        while !thread.is_finished() {
            let now = Instant::now();
            if now >= deadline {
                return Err(deadline_exceeded("Flight listener shutdown timed out"));
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(now)
                    .min(Duration::from_millis(1)),
            );
        }
        self.thread
            .take()
            .expect("finished Flight listener remains owned by the runtime")
            .join()
            .map_err(|_| internal("Flight listener thread panicked"))?;
        let outcome = match self.completed.try_recv() {
            Ok(outcome) => outcome,
            Err(TryRecvError::Empty) => {
                return Err(internal(
                    "Flight listener exited without publishing a completion outcome",
                ));
            }
            Err(TryRecvError::Disconnected) => {
                return Err(internal("Flight listener completion channel disconnected"));
            }
        };
        outcome?;
        Ok(FlightShutdownOutcome {
            released_pending_tickets: before,
        })
    }
}

impl Drop for FlightServerRuntime {
    fn drop(&mut self) {
        self.service.close_admission();
        let _ = self.shutdown.send(false);
        if let Some(thread) = self.thread.take() {
            detach_unfinished_thread(thread, "runtime drop");
        }
    }
}

fn detach_unfinished_thread(thread: JoinHandle<()>, context: &'static str) {
    if thread.is_finished() {
        if thread.join().is_err() {
            tracing::warn!(
                component = "flight",
                event = "flight_listener_join_failed",
                context,
                "flight listener thread panicked during cleanup"
            );
        }
        return;
    }
    tracing::warn!(
        component = "flight",
        event = "flight_listener_cleanup_detached",
        context,
        "flight listener cleanup continues after runtime handle drop"
    );
    drop(thread);
}

fn run_listener(
    listener: TcpListener,
    service: ZippyFlightService,
    max_message_bytes: usize,
    mut shutdown: watch::Receiver<bool>,
    ready: SyncSender<Result<()>>,
    completed: SyncSender<Result<()>>,
) {
    let runtime = match Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name("zippy-flight-worker")
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = internal(format!("build Flight Tokio runtime failed error=[{error}]"));
            let _ = ready.send(Err(error.clone()));
            let _ = completed.send(Err(error));
            return;
        }
    };
    let result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener).map_err(|error| {
            unavailable(format!("adopt Flight listener failed error=[{error}]"))
        })?;
        let incoming = TcpListenerStream::new(listener);
        let flight = FlightServiceServer::new(service.clone())
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes);
        ready
            .send(Ok(()))
            .map_err(|_| internal("Flight startup readiness receiver disconnected"))?;
        let server = Server::builder()
            .add_service(flight)
            .serve_with_incoming(incoming);
        tokio::pin!(server);
        let mut sweep = tokio::time::interval(Duration::from_millis(100));
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                outcome = &mut server => {
                    return outcome.map_err(|error| {
                        unavailable(format!("Flight listener failed error=[{error}]"))
                    });
                }
                _ = sweep.tick() => {
                    let _ = service.sweep_expired();
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || !*shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    });
    let _ = completed.send(result);
}

fn service_configured_message_bytes(service: &ZippyFlightService) -> usize {
    service.max_message_bytes()
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unavailable(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Unavailable, message).with_retryable(true)
}

fn deadline_exceeded(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DeadlineExceeded, message).with_retryable(false)
}

fn internal(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message).with_retryable(false)
}
