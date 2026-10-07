use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

use zippy_core::bus_protocol::{
    ResourceEvent, WatchRequest, WatchResource, MAX_CONTROL_WATCH_TIMEOUT_MS,
};
use zippy_core::{
    ControlEndpoint, ControlRequest, ControlResponse, Result, ZippyConfig, ZippyError,
};
use zippy_types::ErrorCode;

use crate::process_runtime::ProcessRuntimeRegistry;
use crate::registry::{Registry, RegistryError};
// Bound unauthenticated control traffic before dispatch or process registration.
const MAX_CONTROL_CONNECTIONS: usize = 256;
const MAX_CONTROL_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const CONTROL_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(1);

struct ControlConnectionPermit(Arc<AtomicUsize>);

impl ControlConnectionPermit {
    fn try_acquire(active: &Arc<AtomicUsize>) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < MAX_CONTROL_CONNECTIONS).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(active)))
    }
}

impl Drop for ControlConnectionPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

trait ControlSocket: Read + Write {
    fn set_control_read_timeout(&self, timeout: Duration) -> std::io::Result<()>;
}

impl ControlSocket for TcpStream {
    fn set_control_read_timeout(&self, timeout: Duration) -> std::io::Result<()> {
        self.set_read_timeout(Some(timeout))
    }
}

#[cfg(unix)]
impl ControlSocket for UnixStream {
    fn set_control_read_timeout(&self, timeout: Duration) -> std::io::Result<()> {
        self.set_read_timeout(Some(timeout))
    }
}

fn read_control_request(stream: &mut impl ControlSocket) -> Result<String> {
    let deadline = Instant::now() + CONTROL_REQUEST_READ_TIMEOUT;
    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io_error(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "control request read deadline exceeded",
            )));
        }
        stream
            .set_control_read_timeout(remaining)
            .map_err(|error| ZippyError::Io {
                reason: format!("set control read timeout failed error=[{error}]"),
            })?;
        let count = match stream.read(&mut chunk) {
            Ok(0) if request.is_empty() => return Ok(String::new()),
            Ok(0) => {
                return Err(io_error(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "control request must end with a newline",
                )));
            }
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(ZippyError::Io {
                    reason: format!("read control request failed error=[{error}]"),
                });
            }
        };
        let newline = chunk[..count].iter().position(|byte| *byte == b'\n');
        let line_bytes = newline.unwrap_or(count);
        if line_bytes > MAX_CONTROL_REQUEST_BYTES - request.len() {
            return Err(ZippyError::ResourceExhausted {
                reason: format!(
                    "control request exceeds limit max_bytes=[{}]",
                    MAX_CONTROL_REQUEST_BYTES
                ),
            });
        }
        request.extend_from_slice(&chunk[..line_bytes]);
        if newline.is_some() {
            return String::from_utf8(request).map_err(|error| ZippyError::Io {
                reason: format!("invalid control request utf8 error=[{}]", error),
            });
        }
    }
}

const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_LEASE_REAPER_INTERVAL: Duration = Duration::from_secs(2);
const MASTER_ACCEPT_IDLE_SLEEP: Duration = Duration::from_millis(1);
const MASTER_SHUTDOWN_REASON: &str = "master shutdown requested";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GracefulShutdownOutcome {
    pub timed_out_processes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MasterServer {
    registry: Arc<Mutex<Registry>>,
    active_control_connections: Arc<AtomicUsize>,
    control_changed: Arc<Condvar>,
    shutdown_changed: Arc<Condvar>,
    running: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
    lease_timeout: Duration,
    lease_reaper_interval: Duration,
    config: ZippyConfig,
}

impl Default for MasterServer {
    fn default() -> Self {
        Self::with_runtime_config(DEFAULT_LEASE_TIMEOUT, DEFAULT_LEASE_REAPER_INTERVAL)
    }
}

impl MasterServer {
    pub fn with_runtime_config(lease_timeout: Duration, lease_reaper_interval: Duration) -> Self {
        Self::with_runtime_config_and_config(
            lease_timeout,
            lease_reaper_interval,
            ZippyConfig::default(),
        )
    }

    pub fn with_config(config: ZippyConfig) -> Self {
        Self::with_runtime_config_and_config(
            DEFAULT_LEASE_TIMEOUT,
            DEFAULT_LEASE_REAPER_INTERVAL,
            config,
        )
    }

    pub fn with_runtime_config_and_config(
        lease_timeout: Duration,
        lease_reaper_interval: Duration,
        config: ZippyConfig,
    ) -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
            active_control_connections: Arc::new(AtomicUsize::new(0)),
            control_changed: Arc::new(Condvar::new()),
            shutdown_changed: Arc::new(Condvar::new()),
            running: Arc::new(AtomicBool::new(true)),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            lease_timeout,
            lease_reaper_interval,
            config,
        }
    }

    pub fn runtime_config(&self) -> &ZippyConfig {
        &self.config
    }

    pub fn with_runtime_config_values(mut self, config: ZippyConfig) -> Self {
        self.config = config;
        self
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn registry(&self) -> Arc<Mutex<Registry>> {
        Arc::clone(&self.registry)
    }

    /// Creates the boot-scoped process runtime registry for this control listener.
    pub fn process_runtime_registry(&self) -> ProcessRuntimeRegistry {
        ProcessRuntimeRegistry::new(Arc::clone(&self.registry), Arc::clone(&self.running))
    }

    /// Creates an inactive runtime adapter for composition-root admission.
    pub fn process_runtime_registry_inactive(&self) -> ProcessRuntimeRegistry {
        ProcessRuntimeRegistry::new_inactive(Arc::clone(&self.registry), Arc::clone(&self.running))
    }

    pub fn serve(&self, socket_path: &Path) -> Result<()> {
        self.serve_with_ready(socket_path, None)
    }

    pub fn serve_endpoint_with_ready(
        &self,
        endpoint: &ControlEndpoint,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        match endpoint {
            ControlEndpoint::Unix(path) => self.serve_with_ready(path, ready_tx),
            ControlEndpoint::Tcp(addr) => self.serve_tcp_with_ready(addr, ready_tx),
        }
    }

    #[cfg(unix)]
    pub fn serve_with_ready(
        &self,
        socket_path: &Path,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if !self.is_running() {
            report_stopped_before_ready(ready_tx);
            return Ok(());
        }

        if let Some(pause_ms) = std::env::var_os("ZIPPY_MASTER_TEST_PAUSE_BEFORE_READY_MS") {
            let pause_ms = pause_ms.to_string_lossy().parse::<u64>().map_err(|error| {
                ZippyError::InvalidConfig {
                    reason: format!(
                        "env var must parse as u64 name=[ZIPPY_MASTER_TEST_PAUSE_BEFORE_READY_MS] error=[{}]",
                        error
                    ),
                }
            })?;
            thread::sleep(Duration::from_millis(pause_ms));
        }

        if let Err(error) = remove_stale_socket(socket_path) {
            return fail_before_ready(
                ready_tx,
                ZippyError::Io {
                    reason: format!(
                        "remove stale control socket failed path=[{}] error=[{}]",
                        socket_path.display(),
                        error
                    ),
                },
            );
        }

        let listener = match UnixListener::bind(socket_path).map_err(io_error) {
            Ok(listener) => listener,
            Err(error) => {
                return fail_before_ready(
                    ready_tx,
                    ZippyError::Io {
                        reason: format!(
                            "bind control socket failed path=[{}] error=[{}]",
                            socket_path.display(),
                            error
                        ),
                    },
                );
            }
        };
        if let Err(error) = listener.set_nonblocking(true).map_err(io_error) {
            return fail_before_ready(
                ready_tx,
                ZippyError::Io {
                    reason: format!(
                        "set control socket nonblocking failed path=[{}] error=[{}]",
                        socket_path.display(),
                        error
                    ),
                },
            );
        }
        let socket_ownership = match SocketOwnership::create(socket_path) {
            Ok(socket_ownership) => socket_ownership,
            Err(error) => {
                return fail_before_ready(
                    ready_tx,
                    ZippyError::Io {
                        reason: format!(
                            "create control socket owner file failed path=[{}] error=[{}]",
                            socket_path.display(),
                            error
                        ),
                    },
                );
            }
        };

        if let Some(ready_tx) = ready_tx {
            let _ = ready_tx.send(Ok(()));
        }

        tracing::info!(
            component = "master",
            event = "master_listening",
            status = "ready",
            control_endpoint = socket_path.display().to_string(),
            "master listening"
        );

        self.start_lease_reaper();

        let accept_result = self.run_accept_loop(
            || listener.accept().map(|(stream, _)| stream),
            Self::handle_unix_stream,
        );

        drop(listener);

        if let Some(marker_path) = std::env::var_os("ZIPPY_MASTER_TEST_CLEANUP_READY_FILE") {
            fs::write(marker_path, b"ready").map_err(io_error)?;
        }

        if std::env::var_os("ZIPPY_MASTER_TEST_PAUSE_BEFORE_CLEANUP").is_some() {
            thread::sleep(Duration::from_millis(1000));
        }

        let cleanup_result = cleanup_socket(socket_path, &socket_ownership);
        match (accept_result, cleanup_result) {
            (Ok(()), Ok(())) => {
                tracing::info!(
                    component = "master",
                    event = "master_stopped",
                    status = "stopped",
                    control_endpoint = socket_path.display().to_string(),
                    "master stopped"
                );
                Ok(())
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => {
                tracing::error!(
                    component = "master",
                    event = "master_cleanup_error",
                    status = "error",
                    control_endpoint = socket_path.display().to_string(),
                    error = %error,
                    "failed to clean up master socket"
                );
                Err(error)
            }
            (Err(error), Err(cleanup_error)) => {
                tracing::error!(
                    component = "master",
                    event = "master_cleanup_error",
                    status = "error",
                    control_endpoint = socket_path.display().to_string(),
                    error = %cleanup_error,
                    "failed to clean up master socket"
                );
                Err(error)
            }
        }
    }

    #[cfg(not(unix))]
    pub fn serve_with_ready(
        &self,
        socket_path: &Path,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        fail_before_ready(
            ready_tx,
            ZippyError::InvalidConfig {
                reason: format!(
                    "unix control endpoint is not supported on this platform path=[{}]",
                    socket_path.display()
                ),
            },
        )
    }

    pub fn shutdown(&self) {
        let _registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.running.store(false, Ordering::SeqCst);
    }

    /// Notifies registered processes and waits for them without stopping the Master listener.
    ///
    /// This is the first phase of server-owned shutdown. Keeping the Master alive after process
    /// drain keeps the control listener available until registered processes acknowledge shutdown.
    pub fn drain_registered_processes_for_shutdown(
        &self,
        timeout: Duration,
    ) -> GracefulShutdownOutcome {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.control_changed.notify_all();
        self.shutdown_changed.notify_all();
        let deadline = Instant::now() + timeout;
        loop {
            let alive = self.registry.lock().unwrap().alive_process_ids();
            if alive.is_empty() {
                return GracefulShutdownOutcome {
                    timed_out_processes: Vec::new(),
                };
            }
            if Instant::now() >= deadline {
                tracing::error!(
                    component = "master",
                    event = "master_shutdown_timeout",
                    status = "timeout",
                    timed_out_processes = ?alive,
                    "timed out waiting for child processes to unregister"
                );
                return GracefulShutdownOutcome {
                    timed_out_processes: alive,
                };
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            );
        }
    }

    pub fn request_graceful_shutdown(&self, timeout: Duration) -> GracefulShutdownOutcome {
        let outcome = self.drain_registered_processes_for_shutdown(timeout);
        self.shutdown();
        outcome
    }

    fn serve_tcp_with_ready(
        &self,
        addr: &SocketAddr,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if !self.is_running() {
            report_stopped_before_ready(ready_tx);
            return Ok(());
        }

        let listener = match TcpListener::bind(addr).map_err(io_error) {
            Ok(listener) => listener,
            Err(error) => {
                return fail_before_ready(
                    ready_tx,
                    ZippyError::Io {
                        reason: format!(
                            "bind tcp control endpoint failed addr=[{}] error=[{}]",
                            addr, error
                        ),
                    },
                );
            }
        };
        self.serve_tcp_listener_with_ready(listener, ready_tx)
    }

    /// Serves an already bound TCP listener without releasing its address between admission
    /// and startup. The caller transfers listener ownership; the ready signal is emitted only
    /// after the listener is configured, and normal shutdown closes it before returning.
    pub fn serve_tcp_listener_with_ready(
        &self,
        listener: TcpListener,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if !self.is_running() {
            report_stopped_before_ready(ready_tx);
            return Ok(());
        }
        let addr = listener.local_addr().map_err(io_error)?;
        if let Err(error) = listener.set_nonblocking(true).map_err(io_error) {
            return fail_before_ready(
                ready_tx,
                ZippyError::Io {
                    reason: format!(
                        "set tcp control endpoint nonblocking failed addr=[{}] error=[{}]",
                        addr, error
                    ),
                },
            );
        }

        if let Some(ready_tx) = ready_tx {
            let _ = ready_tx.send(Ok(()));
        }

        tracing::info!(
            component = "master",
            event = "master_listening",
            status = "ready",
            control_endpoint = format!("tcp://{addr}"),
            "master listening"
        );

        self.start_lease_reaper();

        let accept_result = self.run_accept_loop(
            || listener.accept().map(|(stream, _)| stream),
            Self::handle_tcp_stream,
        );

        drop(listener);

        accept_result?;
        tracing::info!(
            component = "master",
            event = "master_stopped",
            status = "stopped",
            control_endpoint = format!("tcp://{addr}"),
            "master stopped"
        );
        Ok(())
    }

    /// Accepts control connections until shutdown, serving each admitted one on its own thread.
    fn run_accept_loop<S: Send + 'static>(
        &self,
        mut accept: impl FnMut() -> std::io::Result<S>,
        handle: fn(&Self, S) -> Result<()>,
    ) -> Result<()> {
        loop {
            if !self.running.load(Ordering::SeqCst) {
                return Ok(());
            }

            match accept() {
                Ok(stream) => {
                    let Some(permit) =
                        ControlConnectionPermit::try_acquire(&self.active_control_connections)
                    else {
                        drop(stream);
                        continue;
                    };
                    let server = self.clone();
                    thread::spawn(move || {
                        let _permit = permit;
                        if let Err(error) = handle(&server, stream) {
                            tracing::warn!(
                                component = "master",
                                event = "control_connection_error",
                                status = "error",
                                error = %error,
                                "control connection failed"
                            );
                        }
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(MASTER_ACCEPT_IDLE_SLEEP);
                }
                Err(error) => {
                    let error = io_error(error);
                    tracing::error!(
                        component = "master",
                        event = "master_accept_error",
                        status = "error",
                        error = %error,
                        "accept loop failed"
                    );
                    return Err(error);
                }
            }
        }
    }

    #[cfg(unix)]
    fn handle_unix_stream(&self, stream: UnixStream) -> Result<()> {
        stream
            .set_write_timeout(Some(Duration::from_secs(1)))
            .map_err(io_error)?;
        self.handle_stream(stream)
    }

    fn handle_tcp_stream(&self, stream: TcpStream) -> Result<()> {
        stream
            .set_write_timeout(Some(Duration::from_secs(1)))
            .map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        self.handle_stream(stream)
    }

    fn handle_watch_request(&self, request: WatchRequest) -> ControlResponse {
        if request.timeout_ms > MAX_CONTROL_WATCH_TIMEOUT_MS {
            return ControlResponse::Error {
                code: ErrorCode::InvalidArgument,
                retryable: false,
                reason: format!(
                    "control watch timeout exceeds maximum requested_ms=[{}] maximum_ms=[{}]",
                    request.timeout_ms, MAX_CONTROL_WATCH_TIMEOUT_MS
                ),
            };
        }
        match request.resource.clone() {
            WatchResource::Shutdown => self.handle_watch_shutdown_request(request),
            WatchResource::Process { process_id } => {
                self.handle_watch_process_request(request, process_id)
            }
        }
    }

    fn handle_watch_shutdown_request(&self, request: WatchRequest) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let registry = self.registry.lock().unwrap();
        let validate_result = registry
            .validate_process_capability(&request.process_id, request.process_token.as_deref());
        if validate_result.is_ok() && !self.shutdown_requested.load(Ordering::SeqCst) {
            let (_next_registry, _) = self
                .shutdown_changed
                .wait_timeout_while(registry, timeout, |_| {
                    !self.shutdown_requested.load(Ordering::SeqCst)
                })
                .unwrap();
        }

        match validate_result {
            Ok(())
                if self.shutdown_requested.load(Ordering::SeqCst) && request.after_revision < 1 =>
            {
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "shutdown",
                    process_id = request.process_id.as_str(),
                    revision = 1,
                    "shutdown resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::Shutdown,
                        revision: 1,
                        payload: serde_json::json!({
                            "process_id": request.process_id,
                            "reason": MASTER_SHUTDOWN_REASON,
                        }),
                    }),
                }
            }
            Ok(()) => ControlResponse::ResourceChanged { event: None },
            Err(error) => {
                tracing::error!(
                    component = "master_server",
                    event = "watch",
                    status = "error",
                    resource = "shutdown",
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch shutdown resource"
                );
                registry_error_response(&error)
            }
        }
    }

    fn handle_watch_process_request(
        &self,
        request: WatchRequest,
        target_process_id: String,
    ) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let mut registry = self.registry.lock().unwrap();
        let wait_result = registry.process_update_for_process(
            &target_process_id,
            &request.process_id,
            request.after_revision,
        );
        let response_result = match wait_result {
            Ok(None) => {
                let (next_registry, _) = self
                    .control_changed
                    .wait_timeout_while(registry, timeout, |registry| {
                        matches!(
                            registry.process_update_for_process(
                                &target_process_id,
                                &request.process_id,
                                request.after_revision,
                            ),
                            Ok(None)
                        )
                    })
                    .unwrap();
                registry = next_registry;
                registry.process_update_for_process(
                    &target_process_id,
                    &request.process_id,
                    request.after_revision,
                )
            }
            result => result,
        };

        match response_result {
            Ok(Some((revision, process))) => {
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "process",
                    target_process_id = target_process_id.as_str(),
                    process_id = request.process_id.as_str(),
                    revision,
                    "process resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::Process {
                            process_id: target_process_id,
                        },
                        revision,
                        payload: serde_json::json!({
                            "process": process,
                        }),
                    }),
                }
            }
            Ok(None) => ControlResponse::ResourceChanged { event: None },
            Err(error) => {
                tracing::error!(
                    component = "master_server",
                    event = "watch",
                    status = "error",
                    resource = "process",
                    target_process_id = target_process_id.as_str(),
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch process resource"
                );
                registry_error_response(&error)
            }
        }
    }

    fn require_process_capability_response(
        &self,
        process_id: &str,
        process_token: Option<&str>,
    ) -> Option<ControlResponse> {
        self.registry
            .lock()
            .unwrap()
            .validate_process_capability(process_id, process_token)
            .err()
            .map(|error| registry_error_response(&error))
    }

    fn handle_stream<S>(&self, mut stream: S) -> Result<()>
    where
        S: ControlSocket,
    {
        let request_line = read_control_request(&mut stream)?;
        if request_line.is_empty() {
            return Ok(());
        }

        let request = serde_json::from_str::<ControlRequest>(&request_line).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to decode control request error=[{}]", error),
            }
        })?;
        let response = match request {
            ControlRequest::RegisterProcess(request) => {
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    ControlResponse::Error {
                        code: ErrorCode::Unavailable,
                        retryable: true,
                        reason: MASTER_SHUTDOWN_REASON.to_string(),
                    }
                } else {
                    let mut registry = self.registry.lock().unwrap();
                    match registry.register_process(&request.app) {
                        Ok(process_id) => {
                            let process_token = registry
                                .get_process_token(&process_id)
                                .expect("registered process has a token")
                                .to_string();
                            drop(registry);
                            self.control_changed.notify_all();
                            tracing::info!(
                                component = "master_server",
                                event = "register_process",
                                status = "success",
                                process_id = process_id.as_str(),
                                app = request.app.as_str(),
                                "registered process"
                            );
                            ControlResponse::ProcessRegistered {
                                process_id,
                                process_token,
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                component = "master_server",
                                event = "register_process",
                                status = "failure",
                                error = %error,
                                "process registration failed"
                            );
                            ControlResponse::Error {
                                code: error.code(),
                                retryable: error.retryable(),
                                reason: error.to_string(),
                            }
                        }
                    }
                }
            }
            ControlRequest::Heartbeat(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    tracing::info!(
                        component = "master_server",
                        event = "shutdown_notify",
                        status = "requested",
                        process_id = request.process_id.as_str(),
                        "notified process of master shutdown"
                    );
                    ControlResponse::ShutdownRequested {
                        process_id: request.process_id,
                        reason: MASTER_SHUTDOWN_REASON.to_string(),
                    }
                } else {
                    match self
                        .registry
                        .lock()
                        .unwrap()
                        .record_heartbeat(&request.process_id)
                    {
                        Ok(()) => {
                            self.control_changed.notify_all();
                            tracing::debug!(
                                component = "master_server",
                                event = "heartbeat",
                                status = "success",
                                process_id = request.process_id.as_str(),
                                "accepted process heartbeat"
                            );
                            ControlResponse::HeartbeatAccepted {
                                process_id: request.process_id,
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                component = "master_server",
                                event = "heartbeat",
                                status = "error",
                                process_id = request.process_id.as_str(),
                                error = %error,
                                "failed to accept process heartbeat"
                            );
                            registry_error_response(&error)
                        }
                    }
                }
            }
            ControlRequest::Watch(request) => self.handle_watch_request(request),
            ControlRequest::UnregisterProcess(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let mut registry = self.registry.lock().unwrap();

                match registry.unregister_process(&request.process_id) {
                    Ok(()) => {
                        tracing::info!(
                            component = "master_server",
                            event = "unregister_process",
                            status = "success",
                            process_id = request.process_id.as_str(),
                            "unregistered process"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::ProcessUnregistered {
                            process_id: request.process_id,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "unregister_process",
                            status = "error",
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to unregister process"
                        );
                        registry_error_response(&error)
                    }
                }
            }
            ControlRequest::GetConfig(_) => {
                tracing::debug!(
                    component = "master_server",
                    event = "get_config",
                    status = "success",
                    "fetched config"
                );
                ControlResponse::ConfigFetched {
                    config: self.config.to_json_value(),
                }
            }
        };

        write_control_response(&mut stream, &response)
    }

    fn start_lease_reaper(&self) {
        let server = self.clone();
        thread::spawn(move || {
            while server.is_running() {
                thread::sleep(server.lease_reaper_interval);
                if !server.is_running() {
                    break;
                }

                let expired_processes = {
                    let registry = server.registry.lock().unwrap();
                    registry.expired_processes(server.lease_timeout.as_millis() as u64)
                };

                for process_id in expired_processes {
                    if let Err(error) = server.expire_process_attachments(&process_id) {
                        tracing::error!(
                            component = "master",
                            event = "process_lease_expired",
                            status = "error",
                            process_id = process_id.as_str(),
                            error = %error,
                            "failed to reclaim expired process attachments"
                        );
                    }
                }
            }
        });
    }

    fn expire_process_attachments(&self, process_id: &str) -> Result<()> {
        let claimed = self
            .registry
            .lock()
            .unwrap()
            .claim_expired_process(process_id, self.lease_timeout.as_millis() as u64)
            .map_err(registry_error)?;
        if !claimed {
            return Ok(());
        }
        tracing::warn!(
            component = "master",
            event = "process_lease_expired",
            status = "expired",
            process_id = process_id,
            "process lease expired"
        );
        self.control_changed.notify_all();
        Ok(())
    }
}

/// Reports a startup failure to the optional readiness channel and returns it.
fn fail_before_ready(
    ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    error: ZippyError,
) -> Result<()> {
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(Err(error.to_string()));
    }
    Err(error)
}

fn report_stopped_before_ready(ready_tx: Option<SyncSender<std::result::Result<(), String>>>) {
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(Err(String::from(
            "master daemon stopped before it became ready",
        )));
    }
}

fn write_control_response(stream: &mut impl Write, response: &ControlResponse) -> Result<()> {
    let payload = serde_json::to_string(response).map_err(|error| ZippyError::Io {
        reason: format!("failed to encode control response error=[{}]", error),
    })?;
    stream.write_all(payload.as_bytes()).map_err(io_error)?;
    stream.write_all(b"\n").map_err(io_error)?;
    stream.flush().map_err(io_error)?;
    Ok(())
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn registry_error_response(error: &RegistryError) -> ControlResponse {
    ControlResponse::Error {
        code: error.code(),
        retryable: error.retryable(),
        reason: error.to_string(),
    }
}

fn registry_error(error: RegistryError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

#[cfg(unix)]
fn remove_stale_socket(socket_path: &Path) -> Result<()> {
    if !socket_path.exists() {
        return Ok(());
    }

    let metadata = fs::symlink_metadata(socket_path).map_err(io_error)?;
    if metadata.file_type().is_socket() {
        if socket_is_active(socket_path)? {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "control endpoint socket is already active path=[{}]",
                    socket_path.display()
                ),
            });
        }

        fs::remove_file(socket_path).map_err(io_error)?;
        cleanup_socket_owner_file(socket_path)?;
        return Ok(());
    }

    Err(ZippyError::InvalidConfig {
        reason: format!(
            "control endpoint path exists and is not a unix socket path=[{}]",
            socket_path.display()
        ),
    })
}

#[cfg(unix)]
fn socket_is_active(socket_path: &Path) -> Result<bool> {
    match UnixStream::connect(socket_path) {
        Ok(stream) => {
            drop(stream);
            Ok(true)
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SocketOwnership {
    token: String,
    owner_path: std::path::PathBuf,
}

#[cfg(unix)]
impl SocketOwnership {
    fn create(socket_path: &Path) -> Result<Self> {
        let owner_path = socket_owner_path(socket_path);
        let token = format!(
            "pid=[{}] created_ns=[{}]",
            std::process::id(),
            socket_creation_timestamp()?
        );
        fs::write(&owner_path, token.as_bytes()).map_err(io_error)?;
        Ok(Self { token, owner_path })
    }

    fn matches_socket_path(&self, socket_path: &Path) -> Result<bool> {
        match fs::read_to_string(socket_owner_path(socket_path)) {
            Ok(contents) => Ok(contents == self.token),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_error(error)),
        }
    }
}

#[cfg(unix)]
fn cleanup_socket(socket_path: &Path, socket_ownership: &SocketOwnership) -> Result<()> {
    if !socket_ownership.matches_socket_path(socket_path)? {
        return Ok(());
    }

    match fs::remove_file(socket_path) {
        Ok(()) => cleanup_socket_owner_file(socket_path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            cleanup_socket_owner_file(socket_path)
        }
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(unix)]
fn cleanup_socket_owner_file(socket_path: &Path) -> Result<()> {
    let owner_path = socket_owner_path(socket_path);
    match fs::remove_file(owner_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(unix)]
fn socket_owner_path(socket_path: &Path) -> std::path::PathBuf {
    let mut owner_path = socket_path.as_os_str().to_os_string();
    owner_path.push(".owner");
    std::path::PathBuf::from(owner_path)
}

#[cfg(unix)]
fn socket_creation_timestamp() -> Result<u128> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to compute socket token error=[{}]", error),
        })?;

    Ok(duration.as_nanos())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn control_connection_limit_is_shared_and_released() {
        let server = MasterServer::default();
        let clone = server.clone();
        let permits: Vec<_> = (0..MAX_CONTROL_CONNECTIONS)
            .map(|_| {
                ControlConnectionPermit::try_acquire(&server.active_control_connections).unwrap()
            })
            .collect();
        assert!(ControlConnectionPermit::try_acquire(&clone.active_control_connections).is_none());
        drop(permits);
        assert!(ControlConnectionPermit::try_acquire(&clone.active_control_connections).is_some());
        assert_eq!(server.active_control_connections.load(Ordering::Acquire), 0);
    }

    #[cfg(unix)]
    #[test]
    fn control_request_size_and_newline_are_required() {
        for bytes in [vec![b'x'; MAX_CONTROL_REQUEST_BYTES + 1], b"{}".to_vec()] {
            let (mut sender, mut receiver) = UnixStream::pair().unwrap();
            let writer = thread::spawn(move || {
                // The reader closes immediately when admission fails.
                let _ = sender.write_all(&bytes);
            });
            let error = read_control_request(&mut receiver).unwrap_err();
            assert!(
                error.to_string().contains("limit") || error.to_string().contains("newline"),
                "unexpected control read error=[{error}]"
            );
            drop(receiver);
            writer.join().unwrap();
        }
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();
        let writer = thread::spawn(move || {
            sender
                .write_all(&vec![b' '; MAX_CONTROL_REQUEST_BYTES])
                .unwrap();
            sender.write_all(b"\n").unwrap();
        });
        assert_eq!(
            read_control_request(&mut receiver).unwrap().len(),
            MAX_CONTROL_REQUEST_BYTES
        );
        writer.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn partial_control_input_cannot_extend_absolute_deadline() {
        let (mut sender, mut receiver) = UnixStream::pair().unwrap();
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                if sender.write_all(b" ").is_err() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        let started = Instant::now();
        assert!(read_control_request(&mut receiver).is_err());
        assert!(started.elapsed() < Duration::from_secs(3));
        drop(receiver);
        writer.join().unwrap();
    }

    #[test]
    fn accept_loop_idle_sleep_stays_below_control_plane_latency_budget() {
        assert!(MASTER_ACCEPT_IDLE_SLEEP <= Duration::from_millis(1));
    }
}
