//! Independent lifecycle for the Arrow Flight network listener.

use std::net::{SocketAddr, TcpListener};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow_flight::flight_service_server::FlightServiceServer;
use sha2::{Digest, Sha256};
use tokio::runtime::Builder;
use tokio::sync::watch;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::server::{FlightServiceStatus, ZippyFlightService};

const FLIGHT_THREAD_NAME: &str = "zippy-flight";

/// Outcome of one explicit Flight listener shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightShutdownOutcome {
    /// Number of unconsumed tickets released before listener shutdown.
    pub released_pending_tickets: usize,
}

/// In-memory TLS identity loaded from protected credential files before listener startup.
#[derive(Clone)]
pub struct FlightServerTlsConfig {
    certificate_chain_pem: Vec<u8>,
    private_key_pem: Vec<u8>,
    client_ca_pem: Option<Vec<u8>>,
    require_client_certificate: bool,
    certificate_sha256: String,
}

impl std::fmt::Debug for FlightServerTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlightServerTlsConfig")
            .field("certificate_sha256", &self.certificate_sha256)
            .field("private_key", &"[REDACTED]")
            .field("client_ca_configured", &self.client_ca_pem.is_some())
            .field(
                "require_client_certificate",
                &self.require_client_certificate,
            )
            .finish()
    }
}

impl FlightServerTlsConfig {
    /// Validates non-empty PEM payloads and computes the public certificate fingerprint.
    pub fn try_new(
        certificate_chain_pem: Vec<u8>,
        private_key_pem: Vec<u8>,
        client_ca_pem: Option<Vec<u8>>,
        require_client_certificate: bool,
    ) -> Result<Self> {
        if certificate_chain_pem.is_empty() || private_key_pem.is_empty() {
            return Err(invalid(
                "Flight TLS certificate chain and private key must not be empty",
            ));
        }
        if client_ca_pem.as_ref().is_some_and(Vec::is_empty) {
            return Err(invalid("Flight TLS client CA must not be empty"));
        }
        if require_client_certificate && client_ca_pem.is_none() {
            return Err(invalid(
                "Flight TLS client certificate enforcement requires a client CA",
            ));
        }
        let certificate_sha256 = format!("{:x}", Sha256::digest(&certificate_chain_pem));
        Ok(Self {
            certificate_chain_pem,
            private_key_pem,
            client_ca_pem,
            require_client_certificate,
            certificate_sha256,
        })
    }

    fn status(&self) -> FlightTlsStatus {
        FlightTlsStatus {
            certificate_sha256: self.certificate_sha256.clone(),
            client_certificate_required: self.require_client_certificate,
        }
    }

    fn into_tonic(self) -> ServerTlsConfig {
        let mut config = ServerTlsConfig::new().identity(Identity::from_pem(
            self.certificate_chain_pem,
            self.private_key_pem,
        ));
        if let Some(client_ca_pem) = self.client_ca_pem {
            config = config
                .client_ca_root(Certificate::from_pem(client_ca_pem))
                .client_auth_optional(!self.require_client_certificate);
        }
        config
    }
}

/// Non-secret TLS diagnostics for one running Flight listener.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightTlsStatus {
    certificate_sha256: String,
    client_certificate_required: bool,
}

impl FlightTlsStatus {
    /// Returns the SHA-256 fingerprint of the configured certificate-chain PEM payload.
    #[must_use]
    pub fn certificate_sha256(&self) -> &str {
        &self.certificate_sha256
    }

    /// Returns whether the listener requires a trusted client certificate.
    #[must_use]
    pub const fn client_certificate_required(&self) -> bool {
        self.client_certificate_required
    }
}

/// One independently bound Arrow Flight listener and its owning runtime thread.
pub struct FlightServerRuntime {
    endpoint: SocketAddr,
    service: ZippyFlightService,
    shutdown: watch::Sender<bool>,
    completed: Receiver<Result<()>>,
    thread: Option<JoinHandle<()>>,
    tls_status: Option<FlightTlsStatus>,
}

impl FlightServerRuntime {
    /// Binds the endpoint before returning and starts an owned Tokio runtime.
    pub fn start(
        endpoint: SocketAddr,
        service: ZippyFlightService,
        startup_timeout: Duration,
    ) -> Result<Self> {
        Self::start_configured(endpoint, service, None, startup_timeout)
    }

    /// Starts a TLS-protected Flight listener with an optional client-certificate requirement.
    pub fn start_with_tls(
        endpoint: SocketAddr,
        service: ZippyFlightService,
        tls: FlightServerTlsConfig,
        startup_timeout: Duration,
    ) -> Result<Self> {
        Self::start_configured(endpoint, service, Some(tls), startup_timeout)
    }

    fn start_configured(
        endpoint: SocketAddr,
        service: ZippyFlightService,
        tls: Option<FlightServerTlsConfig>,
        startup_timeout: Duration,
    ) -> Result<Self> {
        if startup_timeout.is_zero() {
            return Err(invalid("Flight startup timeout must be positive"));
        }
        if !endpoint.ip().is_loopback()
            && (!service.status().authentication_required || tls.is_none())
        {
            return Err(invalid(
                "non-loopback Flight listeners require TLS and bearer authentication",
            ));
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
        let tls_status = tls.as_ref().map(FlightServerTlsConfig::status);
        let thread = thread::Builder::new()
            .name(FLIGHT_THREAD_NAME.to_owned())
            .spawn(move || {
                run_listener(
                    listener,
                    thread_service,
                    max_message_bytes,
                    tls,
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
                tls_status,
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

    /// Returns TLS diagnostics, or `None` for a plaintext loopback listener.
    #[must_use]
    pub fn tls_status(&self) -> Option<&FlightTlsStatus> {
        self.tls_status.as_ref()
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
    tls: Option<FlightServerTlsConfig>,
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
        let startup = (|| -> Result<_> {
            let listener = tokio::net::TcpListener::from_std(listener).map_err(|error| {
                unavailable(format!("adopt Flight listener failed error=[{error}]"))
            })?;
            let incoming = flight_incoming(listener)?;
            let flight = FlightServiceServer::new(service.clone())
                .max_decoding_message_size(max_message_bytes)
                .max_encoding_message_size(max_message_bytes);
            let mut server = Server::builder();
            if let Some(tls) = tls {
                server = server.tls_config(tls.into_tonic()).map_err(|error| {
                    invalid(format!(
                        "configure Flight TLS identity failed error=[{error}]"
                    ))
                })?;
            }
            Ok(server.add_service(flight).serve_with_incoming(incoming))
        })();
        let server = match startup {
            Ok(server) => server,
            Err(error) => {
                let _ = ready.send(Err(error.clone()));
                return Err(error);
            }
        };
        ready
            .send(Ok(()))
            .map_err(|_| internal("Flight startup readiness receiver disconnected"))?;
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

fn flight_incoming(listener: tokio::net::TcpListener) -> Result<TcpIncoming> {
    TcpIncoming::from_listener(listener, true, None).map_err(|error| {
        unavailable(format!(
            "configure Flight accepted sockets failed error=[{error}]"
        ))
    })
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

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use tokio_stream::StreamExt;

    use super::{flight_incoming, Builder};

    #[test]
    fn accepted_flight_socket_disables_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind Flight socket test listener");
        listener
            .set_nonblocking(true)
            .expect("set Flight socket test listener nonblocking");
        let endpoint = listener
            .local_addr()
            .expect("read Flight socket test endpoint");
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build Flight socket test runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener)
                .expect("adopt Flight socket test listener");
            let mut incoming = flight_incoming(listener).expect("configure Flight socket incoming");
            let connect = tokio::net::TcpStream::connect(endpoint);
            let accept = incoming.next();
            let (client, server) = tokio::join!(connect, accept);
            let _client = client.expect("connect Flight socket test client");
            let server = server
                .expect("accept Flight socket test connection")
                .expect("Flight socket test accept succeeds");
            assert!(server.nodelay().expect("read server TCP_NODELAY"));
        });
    }
}
