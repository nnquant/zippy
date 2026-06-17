use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use zippy_core::bus_protocol::{
    DropTableResult, GetStreamResponse, ListStreamsResponse, ResourceEvent, StreamInfo,
    WatchRequest, WatchResource,
};
use zippy_core::{
    ControlEndpoint, ControlRequest, ControlResponse, Result, ZippyConfig, ZippyError,
    CONTROL_PROTOCOL_VERSION,
};
use zippy_segment_store::ShmRegion;

use crate::registry::{Registry, RegistryError, StreamRecord};
use crate::snapshot::{
    RegistrySnapshot, SnapshotEngineRecord, SnapshotSourceRecord, SnapshotStore,
    SnapshotStreamRecord,
};

const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_LEASE_REAPER_INTERVAL: Duration = Duration::from_secs(2);
const MASTER_ACCEPT_IDLE_SLEEP: Duration = Duration::from_millis(1);
const COMPACTION_SHUTDOWN_POLL: Duration = Duration::from_millis(100);
const MASTER_SHUTDOWN_REASON: &str = "master shutdown requested";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GracefulShutdownOutcome {
    pub timed_out_processes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MasterServer {
    registry: Arc<Mutex<Registry>>,
    descriptor_changed: Arc<Condvar>,
    control_changed: Arc<Condvar>,
    shutdown_changed: Arc<Condvar>,
    running: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
    envelope_responses: Arc<Mutex<BTreeMap<String, ControlResponse>>>,
    snapshot_lock: Arc<Mutex<()>>,
    snapshot_path: Option<PathBuf>,
    lease_timeout: Duration,
    lease_reaper_interval: Duration,
    config: ZippyConfig,
    token: String,
    token_generated: bool,
}

impl Default for MasterServer {
    fn default() -> Self {
        Self::with_runtime_config(None, DEFAULT_LEASE_TIMEOUT, DEFAULT_LEASE_REAPER_INTERVAL)
    }
}

impl MasterServer {
    pub fn with_runtime_config(
        snapshot_path: Option<PathBuf>,
        lease_timeout: Duration,
        lease_reaper_interval: Duration,
    ) -> Self {
        Self::with_runtime_config_and_config(
            snapshot_path,
            lease_timeout,
            lease_reaper_interval,
            ZippyConfig::default(),
        )
    }

    pub fn with_config(config: ZippyConfig) -> Self {
        Self::with_runtime_config_and_config(
            None,
            DEFAULT_LEASE_TIMEOUT,
            DEFAULT_LEASE_REAPER_INTERVAL,
            config,
        )
    }

    pub fn with_runtime_config_and_config(
        snapshot_path: Option<PathBuf>,
        lease_timeout: Duration,
        lease_reaper_interval: Duration,
        config: ZippyConfig,
    ) -> Self {
        let (token, token_generated) = match config.master.token.clone() {
            Some(token) => (token, false),
            None => (generate_control_token("master"), true),
        };
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
            descriptor_changed: Arc::new(Condvar::new()),
            control_changed: Arc::new(Condvar::new()),
            shutdown_changed: Arc::new(Condvar::new()),
            running: Arc::new(AtomicBool::new(true)),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            envelope_responses: Arc::new(Mutex::new(BTreeMap::new())),
            snapshot_lock: Arc::new(Mutex::new(())),
            snapshot_path,
            lease_timeout,
            lease_reaper_interval,
            config,
            token,
            token_generated,
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    fn log_generated_token(&self) {
        if self.token_generated {
            tracing::info!(
                component = "master",
                event = "master_token_generated",
                status = "ready",
                token = self.token.as_str(),
                "master generated control token"
            );
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

    pub fn from_snapshot_path(snapshot_path: &Path) -> Result<Self> {
        let snapshot = SnapshotStore::load(snapshot_path)?;
        let server = Self::with_runtime_config(
            Some(snapshot_path.to_path_buf()),
            DEFAULT_LEASE_TIMEOUT,
            DEFAULT_LEASE_REAPER_INTERVAL,
        );

        {
            let mut registry = server.registry.lock().unwrap();

            for stream in snapshot.streams {
                registry
                    .ensure_stream(
                        &stream.stream_name,
                        stream.schema,
                        &stream.schema_hash,
                        stream.buffer_size,
                        stream.frame_size,
                    )
                    .map_err(registry_error)?;
                registry
                    .set_stream_status(&stream.stream_name, restored_record_status(&stream.status))
                    .map_err(registry_error)?;
                registry
                    .set_stream_segment_metadata(
                        &stream.stream_name,
                        stream.descriptor_generation,
                        stream.sealed_segments,
                        stream.persisted_files,
                        stream.persist_events,
                        stream.persist_revision,
                        Vec::new(),
                        stream.writer_epoch,
                    )
                    .map_err(registry_error)?;
            }

            for source in snapshot.sources {
                registry.reserve_process_id(&source.process_id);
                let status = restored_record_status(&source.status).to_string();
                registry
                    .register_source(
                        &source.source_name,
                        &source.source_type,
                        &source.process_id,
                        &source.output_stream,
                        source.config,
                    )
                    .map_err(registry_error)?;
                registry
                    .set_source_writer_epoch(&source.source_name, source.writer_epoch)
                    .map_err(registry_error)?;
                registry
                    .set_source_status(&source.source_name, &status, Some(source.metrics))
                    .map_err(registry_error)?;
            }

            for engine in snapshot.engines {
                registry.reserve_process_id(&engine.process_id);
                let status = restored_record_status(&engine.status).to_string();
                registry
                    .register_engine(
                        &engine.engine_name,
                        &engine.engine_type,
                        &engine.process_id,
                        &engine.input_stream,
                        &engine.output_stream,
                        engine.config,
                    )
                    .map_err(registry_error)?;
                registry
                    .set_engine_status(&engine.engine_name, &status, Some(engine.metrics))
                    .map_err(registry_error)?;
            }
        }

        Ok(server)
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
            #[cfg(unix)]
            ControlEndpoint::Unix(path) => self.serve_with_ready(path, ready_tx),
            #[cfg(not(unix))]
            ControlEndpoint::Unix(path) => {
                let error = ZippyError::InvalidConfig {
                    reason: format!(
                        "unix control endpoint is not supported on this platform path=[{}]",
                        path.display()
                    ),
                };
                if let Some(ready_tx) = ready_tx {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                Err(error)
            }
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
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(String::from(
                    "master daemon stopped before it became ready",
                )));
            }
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
            let error = ZippyError::Io {
                reason: format!(
                    "remove stale control socket failed path=[{}] error=[{}]",
                    socket_path.display(),
                    error
                ),
            };
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(error.to_string()));
            }
            return Err(error);
        }

        let listener = match UnixListener::bind(socket_path).map_err(io_error) {
            Ok(listener) => listener,
            Err(error) => {
                let error = ZippyError::Io {
                    reason: format!(
                        "bind control socket failed path=[{}] error=[{}]",
                        socket_path.display(),
                        error
                    ),
                };
                if let Some(ready_tx) = ready_tx {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                return Err(error);
            }
        };
        if let Err(error) = listener.set_nonblocking(true).map_err(io_error) {
            let error = ZippyError::Io {
                reason: format!(
                    "set control socket nonblocking failed path=[{}] error=[{}]",
                    socket_path.display(),
                    error
                ),
            };
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(error.to_string()));
            }
            return Err(error);
        }
        let socket_ownership = match SocketOwnership::create(socket_path) {
            Ok(socket_ownership) => socket_ownership,
            Err(error) => {
                let error = ZippyError::Io {
                    reason: format!(
                        "create control socket owner file failed path=[{}] error=[{}]",
                        socket_path.display(),
                        error
                    ),
                };
                if let Some(ready_tx) = ready_tx {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                return Err(error);
            }
        };

        if let Some(ready_tx) = ready_tx {
            let _ = ready_tx.send(Ok(()));
        }

        self.log_generated_token();

        tracing::info!(
            component = "master",
            event = "master_listening",
            status = "ready",
            control_endpoint = socket_path.display().to_string(),
            "master listening"
        );

        self.start_lease_reaper();
        self.start_compaction_worker();

        let accept_result = loop {
            if !self.running.load(Ordering::SeqCst) {
                break Ok(());
            }

            match listener.accept() {
                Ok((stream, _)) => {
                    let server = self.clone();
                    thread::spawn(move || {
                        if let Err(error) = server.handle_unix_stream(stream) {
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
                    break Err(error);
                }
            }
        };

        drop(listener);

        if let Some(marker_path) = std::env::var_os("ZIPPY_MASTER_TEST_CLEANUP_READY_FILE") {
            fs::write(marker_path, b"ready").map_err(io_error)?;
        }

        if std::env::var_os("ZIPPY_MASTER_TEST_PAUSE_BEFORE_CLEANUP").is_some() {
            thread::sleep(Duration::from_millis(1000));
        }

        let cleanup_result = cleanup_socket(socket_path, &socket_ownership);
        let snapshot_flush_result = self.write_snapshot();

        if let Err(error) = &snapshot_flush_result {
            tracing::error!(
                component = "master",
                event = "snapshot_write_failure",
                status = "error",
                error = %error,
                "failed to write registry snapshot"
            );
        }

        match (accept_result, cleanup_result, snapshot_flush_result) {
            (Ok(()), Ok(()), Ok(())) => {
                tracing::info!(
                    component = "master",
                    event = "master_stopped",
                    status = "stopped",
                    control_endpoint = socket_path.display().to_string(),
                    "master stopped"
                );
                Ok(())
            }
            (Err(error), Ok(()), _) => Err(error),
            (Ok(()), Ok(()), Err(error)) => Err(error),
            (Ok(()), Err(error), _) => {
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
            (Err(error), Err(cleanup_error), _) => {
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
        let error = ZippyError::InvalidConfig {
            reason: format!(
                "unix control endpoint is not supported on this platform path=[{}]",
                socket_path.display()
            ),
        };
        if let Some(ready_tx) = ready_tx {
            let _ = ready_tx.send(Err(error.to_string()));
        }
        Err(error)
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn request_graceful_shutdown(&self, timeout: Duration) -> GracefulShutdownOutcome {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.descriptor_changed.notify_all();
        self.control_changed.notify_all();
        self.shutdown_changed.notify_all();
        let deadline = Instant::now() + timeout;
        loop {
            let alive = self.registry.lock().unwrap().alive_process_ids();
            if alive.is_empty() {
                self.running.store(false, Ordering::SeqCst);
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
                self.running.store(false, Ordering::SeqCst);
                return GracefulShutdownOutcome {
                    timed_out_processes: alive,
                };
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn serve_tcp_with_ready(
        &self,
        addr: &SocketAddr,
        ready_tx: Option<SyncSender<std::result::Result<(), String>>>,
    ) -> Result<()> {
        if !self.is_running() {
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(String::from(
                    "master daemon stopped before it became ready",
                )));
            }
            return Ok(());
        }

        let listener = match TcpListener::bind(addr).map_err(io_error) {
            Ok(listener) => listener,
            Err(error) => {
                let error = ZippyError::Io {
                    reason: format!(
                        "bind tcp control endpoint failed addr=[{}] error=[{}]",
                        addr, error
                    ),
                };
                if let Some(ready_tx) = ready_tx {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                return Err(error);
            }
        };
        if let Err(error) = listener.set_nonblocking(true).map_err(io_error) {
            let error = ZippyError::Io {
                reason: format!(
                    "set tcp control endpoint nonblocking failed addr=[{}] error=[{}]",
                    addr, error
                ),
            };
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(error.to_string()));
            }
            return Err(error);
        }

        if let Some(ready_tx) = ready_tx {
            let _ = ready_tx.send(Ok(()));
        }

        self.log_generated_token();

        tracing::info!(
            component = "master",
            event = "master_listening",
            status = "ready",
            control_endpoint = format!("tcp://{addr}"),
            "master listening"
        );

        self.start_lease_reaper();
        self.start_compaction_worker();

        let accept_result = loop {
            if !self.running.load(Ordering::SeqCst) {
                break Ok(());
            }

            match listener.accept() {
                Ok((stream, _)) => {
                    let server = self.clone();
                    thread::spawn(move || {
                        if let Err(error) = server.handle_tcp_stream(stream) {
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
                    break Err(error);
                }
            }
        };

        drop(listener);
        let snapshot_flush_result = self.write_snapshot();

        match (accept_result, snapshot_flush_result) {
            (Ok(()), Ok(())) => {
                tracing::info!(
                    component = "master",
                    event = "master_stopped",
                    status = "stopped",
                    control_endpoint = format!("tcp://{addr}"),
                    "master stopped"
                );
                Ok(())
            }
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    #[cfg(unix)]
    fn handle_unix_stream(&self, stream: UnixStream) -> Result<()> {
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(io_error)?;
        self.handle_stream(stream)
    }

    fn handle_tcp_stream(&self, stream: TcpStream) -> Result<()> {
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        self.handle_stream(stream)
    }

    fn handle_watch_request(&self, request: WatchRequest) -> ControlResponse {
        match request.resource.clone() {
            WatchResource::Shutdown => self.handle_watch_shutdown_request(request),
            WatchResource::Process { process_id } => {
                self.handle_watch_process_request(request, process_id)
            }
            WatchResource::Source { source_name } => {
                self.handle_watch_source_request(request, source_name)
            }
            WatchResource::PersistedFile { stream_name } => {
                self.handle_watch_persisted_file_request(request, stream_name)
            }
            WatchResource::GatewayConfig => self.handle_watch_gateway_config_request(request),
            WatchResource::Stream { stream_name } => {
                self.handle_watch_stream_request(request, stream_name)
            }
            WatchResource::SegmentDescriptor { stream_name } => {
                self.handle_watch_segment_descriptor_request(request, stream_name)
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
                ControlResponse::Error {
                    reason: error.to_string(),
                }
            }
        }
    }

    fn handle_watch_segment_descriptor_request(
        &self,
        request: WatchRequest,
        stream_name: String,
    ) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let mut registry = self.registry.lock().unwrap();
        let wait_result = registry.segment_descriptor_update_for_process(
            &stream_name,
            &request.process_id,
            request.after_revision,
        );
        let response_result = match wait_result {
            Ok(Some(update)) => Ok(Some(update)),
            Ok(None) => {
                let (next_registry, _) = self
                    .descriptor_changed
                    .wait_timeout_while(registry, timeout, |registry| {
                        matches!(
                            registry.segment_descriptor_update_for_process(
                                &stream_name,
                                &request.process_id,
                                request.after_revision,
                            ),
                            Ok(None)
                        )
                    })
                    .unwrap();
                registry = next_registry;
                registry.segment_descriptor_update_for_process(
                    &stream_name,
                    &request.process_id,
                    request.after_revision,
                )
            }
            Err(error) => Err(error),
        };

        match response_result {
            Ok(Some((descriptor_generation, descriptor))) => {
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "segment_descriptor",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    revision = descriptor_generation,
                    "segment descriptor resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::SegmentDescriptor { stream_name },
                        revision: descriptor_generation,
                        payload: serde_json::json!({
                            "descriptor": descriptor,
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
                    resource = "segment_descriptor",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch segment descriptor resource"
                );
                ControlResponse::Error {
                    reason: error.to_string(),
                }
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
            Ok(Some(update)) => Ok(Some(update)),
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
            Err(error) => Err(error),
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
                ControlResponse::Error {
                    reason: error.to_string(),
                }
            }
        }
    }

    fn handle_watch_source_request(
        &self,
        request: WatchRequest,
        source_name: String,
    ) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let mut registry = self.registry.lock().unwrap();
        let wait_result = registry.source_update_for_process(
            &source_name,
            &request.process_id,
            request.after_revision,
        );
        let response_result = match wait_result {
            Ok(Some(update)) => Ok(Some(update)),
            Ok(None) => {
                let (next_registry, _) = self
                    .control_changed
                    .wait_timeout_while(registry, timeout, |registry| {
                        matches!(
                            registry.source_update_for_process(
                                &source_name,
                                &request.process_id,
                                request.after_revision,
                            ),
                            Ok(None)
                        )
                    })
                    .unwrap();
                registry = next_registry;
                registry.source_update_for_process(
                    &source_name,
                    &request.process_id,
                    request.after_revision,
                )
            }
            Err(error) => Err(error),
        };

        match response_result {
            Ok(Some((revision, source))) => {
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "source",
                    source_name = source_name.as_str(),
                    process_id = request.process_id.as_str(),
                    revision,
                    "source resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::Source { source_name },
                        revision,
                        payload: serde_json::json!({
                            "source": source,
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
                    resource = "source",
                    source_name = source_name.as_str(),
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch source resource"
                );
                ControlResponse::Error {
                    reason: error.to_string(),
                }
            }
        }
    }

    fn handle_watch_persisted_file_request(
        &self,
        request: WatchRequest,
        stream_name: String,
    ) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let mut registry = self.registry.lock().unwrap();
        let wait_result = registry.persisted_file_update_for_process(
            &stream_name,
            &request.process_id,
            request.after_revision,
        );
        let response_result = match wait_result {
            Ok(Some(update)) => Ok(Some(update)),
            Ok(None) => {
                let (next_registry, _) = self
                    .control_changed
                    .wait_timeout_while(registry, timeout, |registry| {
                        matches!(
                            registry.persisted_file_update_for_process(
                                &stream_name,
                                &request.process_id,
                                request.after_revision,
                            ),
                            Ok(None)
                        )
                    })
                    .unwrap();
                registry = next_registry;
                registry.persisted_file_update_for_process(
                    &stream_name,
                    &request.process_id,
                    request.after_revision,
                )
            }
            Err(error) => Err(error),
        };

        match response_result {
            Ok(Some((revision, persisted_files, persist_events))) => {
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "persisted_file",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    revision,
                    "persisted file resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::PersistedFile {
                            stream_name: stream_name.clone(),
                        },
                        revision,
                        payload: serde_json::json!({
                            "stream_name": stream_name,
                            "persisted_files": persisted_files,
                            "persist_events": persist_events,
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
                    resource = "persisted_file",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch persisted file resource"
                );
                ControlResponse::Error {
                    reason: error.to_string(),
                }
            }
        }
    }

    fn handle_watch_gateway_config_request(&self, request: WatchRequest) -> ControlResponse {
        let registry = self.registry.lock().unwrap();
        let validate_result = registry
            .validate_process_capability(&request.process_id, request.process_token.as_deref());
        drop(registry);
        match validate_result {
            Ok(()) if request.after_revision < 1 => ControlResponse::ResourceChanged {
                event: Some(ResourceEvent {
                    resource: WatchResource::GatewayConfig,
                    revision: 1,
                    payload: serde_json::json!({
                        "config": self.config.to_json_value(),
                    }),
                }),
            },
            Ok(()) => ControlResponse::ResourceChanged { event: None },
            Err(error) => ControlResponse::Error {
                reason: error.to_string(),
            },
        }
    }

    fn handle_watch_stream_request(
        &self,
        request: WatchRequest,
        stream_name: String,
    ) -> ControlResponse {
        let timeout = Duration::from_millis(request.timeout_ms);
        let mut registry = self.registry.lock().unwrap();
        let validate_result = registry
            .validate_process_capability(&request.process_id, request.process_token.as_deref());
        let response_result = match validate_result {
            Ok(()) => {
                let should_wait = registry
                    .get_stream(&stream_name)
                    .map(|stream| stream.descriptor_generation <= request.after_revision)
                    .unwrap_or(false);
                if should_wait {
                    let (next_registry, _) = self
                        .descriptor_changed
                        .wait_timeout_while(registry, timeout, |registry| {
                            registry
                                .get_stream(&stream_name)
                                .map(|stream| {
                                    stream.descriptor_generation <= request.after_revision
                                })
                                .unwrap_or(false)
                        })
                        .unwrap();
                    registry = next_registry;
                }
                registry.get_stream(&stream_name).cloned().ok_or_else(|| {
                    crate::registry::RegistryError::StreamNotFound {
                        stream_name: stream_name.clone(),
                    }
                })
            }
            Err(error) => Err(error),
        };

        match response_result {
            Ok(stream) if stream.descriptor_generation > request.after_revision => {
                let revision = stream.descriptor_generation;
                tracing::info!(
                    component = "master_server",
                    event = "watch",
                    status = "changed",
                    resource = "stream",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    revision,
                    "stream resource changed"
                );
                ControlResponse::ResourceChanged {
                    event: Some(ResourceEvent {
                        resource: WatchResource::Stream { stream_name },
                        revision,
                        payload: serde_json::json!({
                            "stream": stream_info_with_active_preflight(stream),
                        }),
                    }),
                }
            }
            Ok(_) => ControlResponse::ResourceChanged { event: None },
            Err(error) => {
                tracing::error!(
                    component = "master_server",
                    event = "watch",
                    status = "error",
                    resource = "stream",
                    stream_name = stream_name.as_str(),
                    process_id = request.process_id.as_str(),
                    error = %error,
                    "failed to watch stream resource"
                );
                ControlResponse::Error {
                    reason: error.to_string(),
                }
            }
        }
    }

    fn handle_enveloped_control_request(&self, request: ControlRequest) -> ControlResponse {
        match request {
            ControlRequest::Envelope(_) => ControlResponse::Error {
                reason: "nested control envelope is not supported".to_string(),
            },
            ControlRequest::Watch(request) => self.handle_watch_request(request),
            ControlRequest::ListStreams(_) => {
                let registry = self.registry.lock().unwrap();
                let streams: Vec<_> = registry
                    .list_streams()
                    .into_iter()
                    .map(|stream| stream_info_with_registry_metadata(&registry, stream))
                    .collect();
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            ControlRequest::ListStreamStatuses(_) => {
                let registry = self.registry.lock().unwrap();
                let streams: Vec<_> = registry
                    .stream_records()
                    .map(|stream| stream_status_with_registry_metadata(&registry, stream))
                    .collect();
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            ControlRequest::GetConfig(_) => ControlResponse::ConfigFetched {
                config: self.config.to_json_value(),
            },
            other => ControlResponse::Error {
                reason: format!(
                    "control envelope does not support request kind kind=[{:?}]",
                    other
                ),
            },
        }
    }

    fn handle_control_envelope_request(
        &self,
        envelope: zippy_core::ControlEnvelopeRequest,
    ) -> ControlResponse {
        if let Some(inner) = envelope.inner {
            return self.handle_enveloped_control_request(*inner);
        }

        match envelope.verb.as_deref() {
            Some("register_process") => {
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    return ControlResponse::Error {
                        reason: MASTER_SHUTDOWN_REASON.to_string(),
                    };
                }
                let app = envelope
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("app"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("control_v2");
                let process_id = self.registry.lock().unwrap().register_process(app);
                let process_token = self
                    .registry
                    .lock()
                    .unwrap()
                    .get_process_token(&process_id)
                    .unwrap_or_default()
                    .to_string();
                self.control_changed.notify_all();
                ControlResponse::ProcessRegistered {
                    process_id,
                    process_token,
                }
            }
            Some("heartbeat") => {
                let Some(process_id) = envelope.process_id else {
                    return ControlResponse::Error {
                        reason: "control envelope heartbeat requires process_id".to_string(),
                    };
                };
                if let Some(response) = self.require_process_capability_response(
                    &process_id,
                    envelope.process_token.as_deref(),
                ) {
                    return response;
                }
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    return ControlResponse::ShutdownRequested {
                        process_id,
                        reason: MASTER_SHUTDOWN_REASON.to_string(),
                    };
                }
                match self.registry.lock().unwrap().record_heartbeat(&process_id) {
                    Ok(()) => {
                        self.control_changed.notify_all();
                        ControlResponse::HeartbeatAccepted { process_id }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            Some("update_status") => self.handle_control_envelope_update_status(
                envelope.process_id,
                envelope.process_token,
                envelope.token,
                envelope.payload,
            ),
            Some("watch") => {
                let Some(process_id) = envelope.process_id else {
                    return ControlResponse::Error {
                        reason: "control envelope watch requires process_id".to_string(),
                    };
                };
                let Some(resource) = envelope.resource else {
                    return ControlResponse::Error {
                        reason: "control envelope watch requires resource".to_string(),
                    };
                };
                if let Some(response) = self.require_process_capability_response(
                    &process_id,
                    envelope.process_token.as_deref(),
                ) {
                    return response;
                }
                self.handle_watch_request(WatchRequest {
                    process_id,
                    process_token: envelope.process_token,
                    resource,
                    after_revision: envelope.revision.unwrap_or_default(),
                    timeout_ms: envelope.timeout_ms.unwrap_or_default(),
                })
            }
            Some("get") if envelope.resource == Some(WatchResource::GatewayConfig) => {
                ControlResponse::ConfigFetched {
                    config: self.config.to_json_value(),
                }
            }
            Some("get") => match envelope.resource {
                Some(WatchResource::Stream { stream_name }) => {
                    let registry = self.registry.lock().unwrap();
                    match registry.get_stream(&stream_name).cloned() {
                        Some(stream) => ControlResponse::StreamFetched(GetStreamResponse {
                            stream: stream_info_with_registry_metadata(&registry, stream),
                        }),
                        None => ControlResponse::Error {
                            reason: format!("stream not found stream_name=[{}]", stream_name),
                        },
                    }
                }
                _ => ControlResponse::Error {
                    reason: "control envelope get requires supported resource".to_string(),
                },
            },
            Some("list") => {
                let registry = self.registry.lock().unwrap();
                let streams: Vec<_> = registry
                    .list_streams()
                    .into_iter()
                    .map(|stream| stream_info_with_registry_metadata(&registry, stream))
                    .collect();
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            Some(verb) => ControlResponse::Error {
                reason: format!("unsupported control envelope verb verb=[{}]", verb),
            },
            None => ControlResponse::Error {
                reason: "control envelope requires inner or verb".to_string(),
            },
        }
    }

    fn handle_control_envelope_update_status(
        &self,
        process_id: Option<String>,
        process_token: Option<String>,
        token: Option<String>,
        payload: Option<serde_json::Value>,
    ) -> ControlResponse {
        let Some(payload) = payload else {
            return ControlResponse::Error {
                reason: "control envelope update_status requires payload".to_string(),
            };
        };
        let Some(kind) = payload.get("kind").and_then(serde_json::Value::as_str) else {
            return ControlResponse::Error {
                reason: "control envelope update_status requires payload.kind".to_string(),
            };
        };
        let Some(name) = payload.get("name").and_then(serde_json::Value::as_str) else {
            return ControlResponse::Error {
                reason: "control envelope update_status requires payload.name".to_string(),
            };
        };
        let Some(status) = payload.get("status").and_then(serde_json::Value::as_str) else {
            return ControlResponse::Error {
                reason: "control envelope update_status requires payload.status".to_string(),
            };
        };
        let metrics = payload.get("metrics").cloned();
        if !self.token_matches(token.as_deref()) {
            let Some(process_id) = process_id.as_deref() else {
                return ControlResponse::Error {
                    reason: "control envelope update_status requires process_id".to_string(),
                };
            };
            if let Err(error) =
                self.validate_process_capability(process_id, process_token.as_deref())
            {
                return ControlResponse::Error {
                    reason: error.to_string(),
                };
            }
            let registry = self.registry.lock().unwrap();
            let owner_process_id = match kind {
                "source" => registry
                    .get_source(name)
                    .map(|record| record.process_id.as_str()),
                "engine" => registry
                    .get_engine(name)
                    .map(|record| record.process_id.as_str()),
                _ => None,
            };
            if owner_process_id != Some(process_id) {
                return ControlResponse::Error {
                    reason: format!(
                        "status update not authorized kind=[{}] name=[{}] process_id=[{}]",
                        kind, name, process_id
                    ),
                };
            }
        }

        let _snapshot_guard = self.snapshot_lock.lock().unwrap();
        let mut registry = self.registry.lock().unwrap();
        let update_result = match kind {
            "source" => {
                let previous = registry.get_source(name).cloned();
                let result = registry.set_source_status(name, status, metrics);
                (
                    result,
                    previous.map(|record| ("source", serde_json::to_value(record).unwrap())),
                )
            }
            "engine" => {
                let previous = registry.get_engine(name).cloned();
                let result = registry.set_engine_status(name, status, metrics);
                (
                    result,
                    previous.map(|record| ("engine", serde_json::to_value(record).unwrap())),
                )
            }
            _ => (
                Err(crate::registry::RegistryError::InvalidRecordKind {
                    kind: kind.to_string(),
                }),
                None,
            ),
        };

        match update_result.0 {
            Ok(()) => {
                let snapshot = Self::snapshot_from_registry(&registry);
                if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                    if let Some((kind, previous)) = update_result.1 {
                        let _ = restore_previous_record(&mut registry, kind, previous);
                    }
                    return ControlResponse::Error {
                        reason: error.to_string(),
                    };
                }
                self.control_changed.notify_all();
                ControlResponse::StatusUpdated {
                    kind: kind.to_string(),
                    name: name.to_string(),
                }
            }
            Err(error) => ControlResponse::Error {
                reason: error.to_string(),
            },
        }
    }

    fn validate_process_capability(
        &self,
        process_id: &str,
        process_token: Option<&str>,
    ) -> std::result::Result<(), RegistryError> {
        self.registry
            .lock()
            .unwrap()
            .validate_process_capability(process_id, process_token)
    }

    fn token_matches(&self, token: Option<&str>) -> bool {
        token == Some(self.token.as_str())
    }

    fn require_process_capability_response(
        &self,
        process_id: &str,
        process_token: Option<&str>,
    ) -> Option<ControlResponse> {
        self.validate_process_capability(process_id, process_token)
            .err()
            .map(|error| ControlResponse::Error {
                reason: error.to_string(),
            })
    }

    fn handle_stream<S>(&self, mut stream: S) -> Result<()>
    where
        S: Read + Write,
    {
        let mut request_line = String::new();
        {
            let mut reader = BufReader::new(&mut stream);
            reader.read_line(&mut request_line).map_err(io_error)?;
        }
        if request_line.is_empty() {
            return Ok(());
        }

        if let Some(response) = self.try_handle_test_control_request(&request_line)? {
            return write_control_response(&mut stream, &response);
        }

        let request = serde_json::from_str::<ControlRequest>(&request_line).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to decode control request error=[{}]", error),
            }
        })?;

        let response = match request {
            ControlRequest::Envelope(envelope) => {
                let request_id = envelope.request_id.clone();
                if let Some(cached) = self
                    .envelope_responses
                    .lock()
                    .unwrap()
                    .get(&request_id)
                    .cloned()
                {
                    return write_control_response(
                        &mut stream,
                        &ControlResponse::Envelope(zippy_core::ControlEnvelopeResponse {
                            version: CONTROL_PROTOCOL_VERSION,
                            request_id,
                            inner: Box::new(cached),
                        }),
                    );
                }
                let inner = if envelope.version == CONTROL_PROTOCOL_VERSION {
                    self.handle_control_envelope_request(envelope)
                } else {
                    ControlResponse::Error {
                        reason: format!(
                            "unsupported control protocol version version=[{}]",
                            envelope.version
                        ),
                    }
                };
                self.envelope_responses
                    .lock()
                    .unwrap()
                    .insert(request_id.clone(), inner.clone());
                ControlResponse::Envelope(zippy_core::ControlEnvelopeResponse {
                    version: CONTROL_PROTOCOL_VERSION,
                    request_id,
                    inner: Box::new(inner),
                })
            }
            ControlRequest::RegisterProcess(request) => {
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    ControlResponse::Error {
                        reason: MASTER_SHUTDOWN_REASON.to_string(),
                    }
                } else {
                    let process_id = self.registry.lock().unwrap().register_process(&request.app);
                    let process_token = self
                        .registry
                        .lock()
                        .unwrap()
                        .get_process_token(&process_id)
                        .unwrap_or_default()
                        .to_string();
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
                            ControlResponse::Error {
                                reason: error.to_string(),
                            }
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
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.unregister_process(&request.process_id) {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            tracing::error!(
                                component = "master_server",
                                event = "unregister_process",
                                status = "error",
                                process_id = request.process_id.as_str(),
                                error = %error,
                                "failed to persist process unregister snapshot"
                            );
                            ControlResponse::Error {
                                reason: error.to_string(),
                            }
                        } else {
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
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::RegisterStream(request) => {
                let token_authorized = self.token_matches(request.token.as_deref());
                if !token_authorized {
                    let Some(process_id) = request.process_id.as_deref() else {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: "register_stream requires process_id".to_string(),
                            },
                        );
                    };
                    if let Some(response) = self.require_process_capability_response(
                        process_id,
                        request.process_token.as_deref(),
                    ) {
                        return write_control_response(&mut stream, &response);
                    }
                }
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                if let Err(error) = validate_register_stream_request(
                    &registry,
                    &request.stream_name,
                    &request.schema_hash,
                    request.buffer_size,
                    request.frame_size,
                ) {
                    tracing::error!(
                        component = "master_server",
                        event = "register_stream",
                        status = "error",
                        stream_name = request.stream_name.as_str(),
                        buffer_size = request.buffer_size,
                        frame_size = request.frame_size,
                        error = %error,
                        "failed to register stream"
                    );
                    return write_control_response(
                        &mut stream,
                        &ControlResponse::Error {
                            reason: error.to_string(),
                        },
                    );
                }
                match registry.ensure_stream(
                    &request.stream_name,
                    request.schema.clone(),
                    &request.schema_hash,
                    request.buffer_size,
                    request.frame_size,
                ) {
                    Ok(registry_created) => {
                        if let Some(process_id) = request.process_id.as_deref() {
                            if let Err(error) =
                                registry.set_stream_owner(&request.stream_name, process_id)
                            {
                                return write_control_response(
                                    &mut stream,
                                    &ControlResponse::Error {
                                        reason: error.to_string(),
                                    },
                                );
                            }
                        }
                        let existing = !registry_created;
                        tracing::info!(
                            component = "master_server",
                            event = "register_stream",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            buffer_size = request.buffer_size,
                            frame_size = request.frame_size,
                            existing = existing,
                            "{}",
                            if existing {
                                "stream already registered"
                            } else {
                                "registered stream"
                            }
                        );
                        if !existing {
                            let snapshot = Self::snapshot_from_registry(&registry);
                            if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                                if registry_created {
                                    registry.unregister_stream(&request.stream_name);
                                }
                                tracing::error!(
                                    component = "master_server",
                                    event = "snapshot_write_failure",
                                    status = "error",
                                    stream_name = request.stream_name.as_str(),
                                    error = %error,
                                    "failed to persist stream snapshot"
                                );
                                return write_control_response(
                                    &mut stream,
                                    &ControlResponse::Error {
                                        reason: error.to_string(),
                                    },
                                );
                            }
                        }
                        ControlResponse::StreamRegistered {
                            stream_name: request.stream_name,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "register_stream",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            buffer_size = request.buffer_size,
                            frame_size = request.frame_size,
                            error = %error,
                            "failed to register stream"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::RegisterSource(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.register_source(
                    &request.source_name,
                    &request.source_type,
                    &request.process_id,
                    &request.output_stream,
                    request.config,
                ) {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            registry.unregister_source(&request.source_name);
                            tracing::error!(
                                component = "master_server",
                                event = "snapshot_write_failure",
                                status = "error",
                                source_name = request.source_name.as_str(),
                                error = %error,
                                "failed to persist source snapshot"
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "register_source",
                            status = "success",
                            source_name = request.source_name.as_str(),
                            process_id = request.process_id.as_str(),
                            output_stream = request.output_stream.as_str(),
                            "registered source"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::SourceRegistered {
                            source_name: request.source_name,
                        }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            ControlRequest::UnregisterSource(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                let previous_registry = registry.clone();
                match registry
                    .unregister_source_for_process(&request.source_name, &request.process_id)
                {
                    Ok(_source) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            *registry = previous_registry;
                            tracing::error!(
                                component = "master_server",
                                event = "snapshot_write_failure",
                                status = "error",
                                source_name = request.source_name.as_str(),
                                error = %error,
                                "failed to persist source unregister snapshot"
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "unregister_source",
                            status = "success",
                            source_name = request.source_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "unregistered source"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::SourceUnregistered {
                            source_name: request.source_name,
                        }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            ControlRequest::RegisterEngine(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.register_engine(
                    &request.engine_name,
                    &request.engine_type,
                    &request.process_id,
                    &request.input_stream,
                    &request.output_stream,
                    request.config,
                ) {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            registry.unregister_engine(&request.engine_name);
                            tracing::error!(
                                component = "master_server",
                                event = "snapshot_write_failure",
                                status = "error",
                                engine_name = request.engine_name.as_str(),
                                error = %error,
                                "failed to persist engine snapshot"
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "register_engine",
                            status = "success",
                            engine_name = request.engine_name.as_str(),
                            process_id = request.process_id.as_str(),
                            input_stream = request.input_stream.as_str(),
                            output_stream = request.output_stream.as_str(),
                            "registered engine"
                        );
                        ControlResponse::EngineRegistered {
                            engine_name: request.engine_name,
                        }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            ControlRequest::UpdateStatus(request) => {
                if !self.token_matches(request.token.as_deref()) {
                    let Some(process_id) = request.process_id.as_deref() else {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: "update_status requires process_id".to_string(),
                            },
                        );
                    };
                    if let Some(response) = self.require_process_capability_response(
                        process_id,
                        request.process_token.as_deref(),
                    ) {
                        return write_control_response(&mut stream, &response);
                    }
                    let registry = self.registry.lock().unwrap();
                    let owner_process_id = match request.kind.as_str() {
                        "source" => registry
                            .get_source(&request.name)
                            .map(|record| record.process_id.as_str()),
                        "engine" => registry
                            .get_engine(&request.name)
                            .map(|record| record.process_id.as_str()),
                        _ => None,
                    };
                    if owner_process_id != Some(process_id) {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: format!(
                                    "status update not authorized kind=[{}] name=[{}] process_id=[{}]",
                                    request.kind, request.name, process_id
                                ),
                            },
                        );
                    }
                }
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                let update_result = match request.kind.as_str() {
                    "source" => {
                        let previous = registry.get_source(&request.name).cloned();
                        let result = registry.set_source_status(
                            &request.name,
                            &request.status,
                            request.metrics.clone(),
                        );
                        (
                            result,
                            previous
                                .map(|record| ("source", serde_json::to_value(record).unwrap())),
                        )
                    }
                    "engine" => {
                        let previous = registry.get_engine(&request.name).cloned();
                        let result = registry.set_engine_status(
                            &request.name,
                            &request.status,
                            request.metrics.clone(),
                        );
                        (
                            result,
                            previous
                                .map(|record| ("engine", serde_json::to_value(record).unwrap())),
                        )
                    }
                    _ => (
                        Err(crate::registry::RegistryError::InvalidRecordKind {
                            kind: request.kind.clone(),
                        }),
                        None,
                    ),
                };
                match update_result.0 {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            if let Some((kind, previous)) = update_result.1 {
                                let _ = restore_previous_record(&mut registry, kind, previous);
                            }
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "update_status",
                            status = "success",
                            kind = request.kind.as_str(),
                            name = request.name.as_str(),
                            record_status = request.status.as_str(),
                            "updated record status"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::StatusUpdated {
                            kind: request.kind,
                            name: request.name,
                        }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            ControlRequest::PublishSegmentDescriptor(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let publish_result = {
                    let mut registry = self.registry.lock().unwrap();
                    registry.publish_segment_descriptor(
                        &request.stream_name,
                        &request.process_id,
                        request.descriptor,
                    )
                };
                match publish_result {
                    Ok(()) => {
                        self.descriptor_changed.notify_all();
                        tracing::info!(
                            component = "master_server",
                            event = "publish_segment_descriptor",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "published segment descriptor"
                        );
                        ControlResponse::SegmentDescriptorPublished {
                            stream_name: request.stream_name,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "publish_segment_descriptor",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to publish segment descriptor"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::PublishPersistedFile(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let persist_data_root = {
                    let registry = self.registry.lock().unwrap();
                    persist_data_root_for_process(
                        &registry,
                        &request.stream_name,
                        &request.process_id,
                    )
                };
                let persisted_file = match normalize_persisted_file_path(
                    &self.config,
                    persist_data_root.as_deref(),
                    request.persisted_file,
                ) {
                    Ok(persisted_file) => persisted_file,
                    Err(error) => {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: error.to_string(),
                            },
                        );
                    }
                };
                let mut registry = self.registry.lock().unwrap();
                let previous_persisted_files = registry
                    .get_stream(&request.stream_name)
                    .map(|stream| stream.persisted_files.clone())
                    .unwrap_or_default();
                let publish_result = registry.publish_persisted_file(
                    &request.stream_name,
                    &request.process_id,
                    persisted_file,
                );
                match publish_result {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            let _ = registry.set_stream_persisted_files(
                                &request.stream_name,
                                previous_persisted_files,
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "publish_persisted_file",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "published persisted file"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::PersistedFilePublished {
                            stream_name: request.stream_name,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "publish_persisted_file",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to publish persisted file"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::ReplacePersistedFiles(request) => {
                if !self.token_matches(request.token.as_deref()) {
                    let Some(process_id) = request.process_id.as_deref() else {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: "replace_persisted_files requires process_id".to_string(),
                            },
                        );
                    };
                    if let Some(response) = self.require_process_capability_response(
                        process_id,
                        request.process_token.as_deref(),
                    ) {
                        return write_control_response(&mut stream, &response);
                    }
                    let registry = self.registry.lock().unwrap();
                    let authorized = registry
                        .get_stream(&request.stream_name)
                        .map(|stream| {
                            stream.writer_process_id.as_deref() == Some(process_id)
                                || stream.owner_process_id.as_deref() == Some(process_id)
                        })
                        .unwrap_or(false)
                        || registry
                            .sources_for_stream(&request.stream_name)
                            .iter()
                            .any(|source| {
                                source.process_id == process_id && source.status != "lost"
                            });
                    if !authorized {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: format!(
                                    "replace persisted files not authorized stream_name=[{}] process_id=[{}]",
                                    request.stream_name, process_id
                                ),
                            },
                        );
                    }
                }
                let persisted_files =
                    match normalize_persisted_file_paths(&self.config, request.persisted_files) {
                        Ok(persisted_files) => persisted_files,
                        Err(error) => {
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                    };
                let mut registry = self.registry.lock().unwrap();
                let previous_persisted_files = registry
                    .get_stream(&request.stream_name)
                    .map(|stream| stream.persisted_files.clone())
                    .unwrap_or_default();
                let replace_result =
                    registry.replace_persisted_files(&request.stream_name, persisted_files);
                match replace_result {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            let _ = registry.set_stream_persisted_files(
                                &request.stream_name,
                                previous_persisted_files,
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "replace_persisted_files",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            "replaced persisted files"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::PersistedFilesReplaced {
                            stream_name: request.stream_name,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "replace_persisted_files",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            error = %error,
                            "failed to replace persisted files"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::PublishPersistEvent(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let mut registry = self.registry.lock().unwrap();
                let previous_persist_events = registry
                    .get_stream(&request.stream_name)
                    .map(|stream| stream.persist_events.clone())
                    .unwrap_or_default();
                let publish_result = registry.publish_persist_event(
                    &request.stream_name,
                    &request.process_id,
                    request.persist_event,
                );
                match publish_result {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            let _ = registry.set_stream_persist_events(
                                &request.stream_name,
                                previous_persist_events,
                            );
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                        tracing::info!(
                            component = "master_server",
                            event = "publish_persist_event",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "published persist event"
                        );
                        self.control_changed.notify_all();
                        ControlResponse::PersistEventPublished {
                            stream_name: request.stream_name,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "publish_persist_event",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to publish persist event"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::AcquireSegmentReaderLease(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let mut registry = self.registry.lock().unwrap();
                let acquire_result = registry.acquire_segment_reader_lease(
                    &request.stream_name,
                    &request.process_id,
                    request.source_segment_id,
                    request.source_generation,
                );
                match acquire_result {
                    Ok(lease_id) => {
                        tracing::info!(
                            component = "master_server",
                            event = "acquire_segment_reader_lease",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            lease_id = lease_id.as_str(),
                            "acquired segment reader lease"
                        );
                        ControlResponse::SegmentReaderLeaseAcquired {
                            stream_name: request.stream_name,
                            lease_id,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "acquire_segment_reader_lease",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to acquire segment reader lease"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::ReleaseSegmentReaderLease(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let mut registry = self.registry.lock().unwrap();
                let release_result = registry.release_segment_reader_lease(
                    &request.stream_name,
                    &request.process_id,
                    &request.lease_id,
                );
                match release_result {
                    Ok(()) => {
                        tracing::info!(
                            component = "master_server",
                            event = "release_segment_reader_lease",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            lease_id = request.lease_id.as_str(),
                            "released segment reader lease"
                        );
                        ControlResponse::SegmentReaderLeaseReleased {
                            stream_name: request.stream_name,
                            lease_id: request.lease_id,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "release_segment_reader_lease",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            lease_id = request.lease_id.as_str(),
                            error = %error,
                            "failed to release segment reader lease"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::GetSegmentDescriptor(request) => {
                if let Some(response) = self.require_process_capability_response(
                    &request.process_id,
                    request.process_token.as_deref(),
                ) {
                    return write_control_response(&mut stream, &response);
                }
                let registry = self.registry.lock().unwrap();
                match registry
                    .segment_descriptor_for_process(&request.stream_name, &request.process_id)
                {
                    Ok(descriptor) => {
                        tracing::debug!(
                            component = "master_server",
                            event = "get_segment_descriptor",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            descriptor_present = descriptor.is_some(),
                            "fetched segment descriptor"
                        );
                        ControlResponse::SegmentDescriptorFetched {
                            stream_name: request.stream_name,
                            descriptor,
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "get_segment_descriptor",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to fetch segment descriptor"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::ListStreams(_) => {
                let registry = self.registry.lock().unwrap();
                let streams: Vec<_> = registry
                    .list_streams()
                    .into_iter()
                    .map(|stream| stream_info_with_registry_metadata(&registry, stream))
                    .collect();
                tracing::debug!(
                    component = "master_server",
                    event = "list_streams",
                    status = "success",
                    stream_count = streams.len(),
                    "listed streams"
                );
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            ControlRequest::ListStreamStatuses(_) => {
                let registry = self.registry.lock().unwrap();
                let streams: Vec<_> = registry
                    .stream_records()
                    .map(|stream| stream_status_with_registry_metadata(&registry, stream))
                    .collect();
                tracing::debug!(
                    component = "master_server",
                    event = "list_stream_statuses",
                    status = "success",
                    stream_count = streams.len(),
                    "listed stream statuses"
                );
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            ControlRequest::GetStream(request) => {
                let registry = self.registry.lock().unwrap();
                match registry.get_stream(&request.stream_name).cloned() {
                    Some(stream) => {
                        tracing::debug!(
                            component = "master_server",
                            event = "get_stream",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            "fetched stream"
                        );
                        ControlResponse::StreamFetched(GetStreamResponse {
                            stream: stream_info_with_registry_metadata(&registry, stream),
                        })
                    }
                    None => {
                        tracing::error!(
                            component = "master_server",
                            event = "get_stream",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            error = "stream not found",
                            "failed to fetch stream"
                        );
                        ControlResponse::Error {
                            reason: format!(
                                "stream not found stream_name=[{}]",
                                request.stream_name
                            ),
                        }
                    }
                }
            }
            ControlRequest::GetStreamStatus(request) => {
                let registry = self.registry.lock().unwrap();
                match registry.get_stream(&request.stream_name) {
                    Some(stream) => {
                        tracing::debug!(
                            component = "master_server",
                            event = "get_stream_status",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            "fetched stream status"
                        );
                        ControlResponse::StreamFetched(GetStreamResponse {
                            stream: stream_status_with_registry_metadata(&registry, stream),
                        })
                    }
                    None => {
                        tracing::error!(
                            component = "master_server",
                            event = "get_stream_status",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            error = "stream not found",
                            "failed to fetch stream status"
                        );
                        ControlResponse::Error {
                            reason: format!(
                                "stream not found stream_name=[{}]",
                                request.stream_name
                            ),
                        }
                    }
                }
            }
            ControlRequest::DropTable(request) => {
                if !self.token_matches(request.token.as_deref()) {
                    let Some(process_id) = request.process_id.as_deref() else {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: "drop_table requires process_id or token".to_string(),
                            },
                        );
                    };
                    if let Some(response) = self.require_process_capability_response(
                        process_id,
                        request.process_token.as_deref(),
                    ) {
                        return write_control_response(&mut stream, &response);
                    }
                    let registry = self.registry.lock().unwrap();
                    let authorized =
                        registry
                            .get_stream(&request.table_name)
                            .map(|stream| {
                                stream.owner_process_id.as_deref() == Some(process_id)
                                    || stream.writer_process_id.as_deref() == Some(process_id)
                            })
                            .unwrap_or(false)
                            || registry.sources_for_stream(&request.table_name).iter().any(
                                |source| source.process_id == process_id && source.status != "lost",
                            );
                    if !authorized {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: format!(
                                    "drop_table not authorized table_name=[{}] process_id=[{}]",
                                    request.table_name, process_id
                                ),
                            },
                        );
                    }
                }
                let table_name = request.table_name.clone();
                let persisted_files_to_delete = {
                    let registry = self.registry.lock().unwrap();
                    registry
                        .get_stream(&table_name)
                        .map(|stream| stream.persisted_files.clone())
                        .unwrap_or_default()
                };
                if request.drop_persisted {
                    if let Err(error) = prevalidate_persisted_files_for_delete(
                        &self.config,
                        &persisted_files_to_delete,
                    ) {
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: error.to_string(),
                            },
                        );
                    }
                }
                let drop_result = {
                    let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                    let mut registry = self.registry.lock().unwrap();
                    let previous_registry = registry.clone();
                    let dropped_records = registry.drop_table(&table_name);
                    let snapshot = Self::snapshot_from_registry(&registry);
                    if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                        *registry = previous_registry;
                        return write_control_response(
                            &mut stream,
                            &ControlResponse::Error {
                                reason: error.to_string(),
                            },
                        );
                    }
                    self.descriptor_changed.notify_all();
                    self.control_changed.notify_all();
                    dropped_records
                };
                let persisted_files_deleted = if request.drop_persisted {
                    match delete_persisted_files(&self.config, &persisted_files_to_delete) {
                        Ok(deleted) => deleted,
                        Err(error) => {
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
                    }
                } else {
                    0
                };
                tracing::info!(
                    component = "master_server",
                    event = "drop_table",
                    status = "success",
                    table_name = table_name.as_str(),
                    dropped = drop_result.stream.is_some(),
                    sources_removed = drop_result.sources.len(),
                    engines_removed = drop_result.engines.len(),
                    persisted_files_deleted,
                    "dropped table"
                );
                ControlResponse::TableDropped(DropTableResult {
                    table_name,
                    dropped: drop_result.stream.is_some(),
                    sources_removed: drop_result.sources.len(),
                    engines_removed: drop_result.engines.len(),
                    persisted_files_deleted,
                })
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

    #[cfg(debug_assertions)]
    fn try_handle_test_control_request(
        &self,
        request_line: &str,
    ) -> Result<Option<ControlResponse>> {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(request_line) else {
            return Ok(None);
        };

        let Some(process_id) = value
            .get("ExpireProcessForTest")
            .and_then(|payload| payload.get("process_id"))
            .and_then(|payload| payload.as_str())
        else {
            return Ok(None);
        };

        self.expire_process_for_test_internal(process_id)?;
        Ok(Some(ControlResponse::HeartbeatAccepted {
            process_id: process_id.to_string(),
        }))
    }

    #[cfg(not(debug_assertions))]
    fn try_handle_test_control_request(
        &self,
        _request_line: &str,
    ) -> Result<Option<ControlResponse>> {
        Ok(None)
    }

    #[cfg(debug_assertions)]
    fn expire_process_for_test_internal(&self, process_id: &str) -> Result<()> {
        let snapshot = {
            let mut registry = self.registry.lock().unwrap();
            registry
                .force_expire_process(process_id)
                .map_err(registry_error)?;

            let stale_streams = registry.streams_for_writer_process(process_id);
            for stream_name in stale_streams {
                registry
                    .detach_writer(&stream_name)
                    .map_err(registry_error)?;
                registry
                    .set_stream_status(&stream_name, "stale")
                    .map_err(registry_error)?;
            }

            let stale_readers = registry.readers_for_process(process_id);
            for (stream_name, reader_id) in stale_readers {
                registry
                    .detach_reader(&stream_name, &reader_id)
                    .map_err(registry_error)?;
            }
            registry.remove_segment_reader_leases_for_process(process_id);
            registry.mark_records_lost_for_process(process_id);
            Self::snapshot_from_registry(&registry)
        };
        self.control_changed.notify_all();
        self.write_snapshot_from_snapshot(&snapshot)
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

    fn start_compaction_worker(&self) {
        let config = self.config.table.persist.compaction.clone();
        if !config.enabled {
            return;
        }
        let server = self.clone();
        thread::spawn(move || {
            tracing::info!(
                component = "master",
                event = "compaction_worker_started",
                status = "started",
                interval_sec = config.interval_sec,
                min_files = config.min_files as u64,
                sort_column = config.sort_column.as_deref().unwrap_or(""),
                "started persisted parquet compaction worker"
            );

            while server.is_running() {
                if let Err(error) = server.compact_persisted_tables_once() {
                    tracing::error!(
                        component = "master",
                        event = "compaction_worker_pass",
                        status = "error",
                        error = %error,
                        "persisted parquet compaction pass failed"
                    );
                }

                let mut slept = Duration::ZERO;
                let interval = Duration::from_secs_f64(config.interval_sec);
                while server.is_running() && slept < interval {
                    let remaining = interval.saturating_sub(slept);
                    let sleep_for = remaining.min(COMPACTION_SHUTDOWN_POLL);
                    thread::sleep(sleep_for);
                    slept += sleep_for;
                }
            }
        });
    }

    fn compact_persisted_tables_once(&self) -> Result<()> {
        let streams = self.registry.lock().unwrap().list_streams();
        for stream in streams {
            let groups = compaction_groups(&stream.persisted_files, &stream);
            for group_files in groups.values() {
                if group_files.len() < self.config.table.persist.compaction.min_files {
                    continue;
                }
                let compacted_file = compact_persisted_file_group(
                    &stream.stream_name,
                    &stream.schema_hash,
                    group_files,
                    self.config.table.persist.compaction.sort_column.as_deref(),
                )?;
                self.replace_compacted_persisted_files(
                    &stream.stream_name,
                    group_files,
                    compacted_file,
                )?;
            }
        }
        Ok(())
    }

    fn replace_compacted_persisted_files(
        &self,
        stream_name: &str,
        source_files: &[serde_json::Value],
        compacted_file: serde_json::Value,
    ) -> Result<()> {
        let source_keys = source_files
            .iter()
            .map(persisted_file_compaction_key)
            .collect::<HashSet<_>>();
        let source_paths = source_files
            .iter()
            .filter_map(|item| item.get("file_path").and_then(serde_json::Value::as_str))
            .map(PathBuf::from)
            .collect::<Vec<_>>();
        let _snapshot_guard = self.snapshot_lock.lock().unwrap();
        let mut registry = self.registry.lock().unwrap();
        let previous_persisted_files = registry
            .get_stream(stream_name)
            .map(|stream| stream.persisted_files.clone())
            .unwrap_or_default();
        let mut next_files = previous_persisted_files
            .iter()
            .filter(|item| !source_keys.contains(&persisted_file_compaction_key(item)))
            .cloned()
            .collect::<Vec<_>>();
        next_files.push(compacted_file);
        next_files.sort_by_key(persisted_file_order_key);
        registry
            .replace_persisted_files(stream_name, next_files)
            .map_err(registry_error)?;
        let snapshot = Self::snapshot_from_registry(&registry);
        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
            let _ = registry.set_stream_persisted_files(stream_name, previous_persisted_files);
            return Err(error);
        }
        self.control_changed.notify_all();
        drop(registry);

        if self.config.table.persist.compaction.delete_sources {
            for path in source_paths {
                if let Err(error) = fs::remove_file(&path) {
                    tracing::warn!(
                        component = "master",
                        event = "compaction_delete_source",
                        status = "error",
                        file_path = path.display().to_string(),
                        error = %error,
                        "failed to delete compacted source parquet"
                    );
                }
            }
        }
        Ok(())
    }

    fn expire_process_attachments(&self, process_id: &str) -> Result<()> {
        let snapshot = {
            let mut registry = self.registry.lock().unwrap();
            let claimed = registry
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

            let stale_streams = registry.streams_for_writer_process(process_id);
            for stream_name in stale_streams {
                registry
                    .detach_writer(&stream_name)
                    .map_err(registry_error)?;
                registry
                    .set_stream_status(&stream_name, "stale")
                    .map_err(registry_error)?;
                tracing::info!(
                    component = "master",
                    event = "writer_reclaimed",
                    status = "success",
                    process_id = process_id,
                    stream_name = stream_name.as_str(),
                    "reclaimed stale writer"
                );
            }

            let stale_readers = registry.readers_for_process(process_id);
            for (stream_name, reader_id) in stale_readers {
                registry
                    .detach_reader(&stream_name, &reader_id)
                    .map_err(registry_error)?;
                tracing::info!(
                    component = "master",
                    event = "reader_reclaimed",
                    status = "success",
                    process_id = process_id,
                    stream_name = stream_name.as_str(),
                    reader_id = reader_id.as_str(),
                    "reclaimed stale reader"
                );
            }

            let removed_segment_reader_leases =
                registry.remove_segment_reader_leases_for_process(process_id);
            if removed_segment_reader_leases > 0 {
                tracing::info!(
                    component = "master",
                    event = "segment_reader_lease_reclaimed",
                    status = "success",
                    process_id = process_id,
                    lease_count = removed_segment_reader_leases as u64,
                    "reclaimed stale segment reader leases"
                );
            }
            registry.mark_records_lost_for_process(process_id);
            Self::snapshot_from_registry(&registry)
        };
        self.control_changed.notify_all();
        self.write_snapshot_from_snapshot(&snapshot)
    }

    fn snapshot_from_registry(registry: &Registry) -> RegistrySnapshot {
        RegistrySnapshot {
            streams: registry
                .list_streams()
                .into_iter()
                .map(|stream| SnapshotStreamRecord {
                    stream_name: stream.stream_name,
                    schema: stream.schema,
                    schema_hash: stream.schema_hash,
                    data_path: stream.data_path,
                    descriptor_generation: stream.descriptor_generation,
                    sealed_segments: stream.sealed_segments,
                    persisted_files: stream.persisted_files,
                    persist_events: stream.persist_events,
                    persist_revision: stream.persist_revision,
                    segment_reader_leases: Vec::new(),
                    writer_epoch: stream.writer_epoch,
                    buffer_size: stream.buffer_size,
                    frame_size: stream.frame_size,
                    status: stream.status,
                })
                .collect(),
            sources: registry
                .list_sources()
                .into_iter()
                .map(|source| SnapshotSourceRecord {
                    source_name: source.source_name,
                    source_type: source.source_type,
                    process_id: source.process_id,
                    output_stream: source.output_stream,
                    config: source.config,
                    status: source.status,
                    metrics: source.metrics,
                    writer_epoch: source.writer_epoch,
                    revision: source.revision,
                })
                .collect(),
            engines: registry
                .list_engines()
                .into_iter()
                .map(|engine| SnapshotEngineRecord {
                    engine_name: engine.engine_name,
                    engine_type: engine.engine_type,
                    process_id: engine.process_id,
                    input_stream: engine.input_stream,
                    output_stream: engine.output_stream,
                    config: engine.config,
                    status: engine.status,
                    metrics: engine.metrics,
                })
                .collect(),
        }
    }

    fn write_snapshot(&self) -> Result<()> {
        let _snapshot_guard = self.snapshot_lock.lock().unwrap();
        let snapshot = {
            let registry = self.registry.lock().unwrap();
            Self::snapshot_from_registry(&registry)
        };
        self.write_snapshot_from_snapshot(&snapshot)
    }

    fn write_snapshot_from_snapshot(&self, snapshot: &RegistrySnapshot) -> Result<()> {
        let Some(snapshot_path) = self.snapshot_path.as_ref() else {
            return Ok(());
        };

        SnapshotStore::write(snapshot_path, snapshot)?;
        tracing::info!(
            component = "master",
            event = "snapshot_write_success",
            status = "success",
            snapshot_path = snapshot_path.display().to_string(),
            stream_count = snapshot.streams.len() as u64,
            "wrote registry snapshot"
        );
        Ok(())
    }
}

fn restore_previous_record(
    registry: &mut Registry,
    kind: &str,
    previous: serde_json::Value,
) -> Result<()> {
    match kind {
        "source" => {
            let record: crate::snapshot::SnapshotSourceRecord =
                serde_json::from_value(previous).map_err(json_error)?;
            registry
                .set_source_status(&record.source_name, &record.status, Some(record.metrics))
                .map_err(registry_error)
        }
        "engine" => {
            let record: crate::snapshot::SnapshotEngineRecord =
                serde_json::from_value(previous).map_err(json_error)?;
            registry
                .set_engine_status(&record.engine_name, &record.status, Some(record.metrics))
                .map_err(registry_error)
        }
        _ => Err(ZippyError::Io {
            reason: format!("invalid restore kind kind=[{}]", kind),
        }),
    }
}

fn normalize_persisted_file_paths(
    config: &ZippyConfig,
    persisted_files: Vec<serde_json::Value>,
) -> Result<Vec<serde_json::Value>> {
    persisted_files
        .into_iter()
        .map(|persisted_file| normalize_persisted_file_path(config, None, persisted_file))
        .collect()
}

fn normalize_persisted_file_path(
    config: &ZippyConfig,
    persist_data_root: Option<&str>,
    mut persisted_file: serde_json::Value,
) -> Result<serde_json::Value> {
    let root = match persist_data_root {
        Some(root) => canonical_persist_data_root_value(root)?,
        None => canonical_persist_data_root(config)?,
    };
    let path = persisted_file_canonical_path(&root, &persisted_file)?;
    let Some(object) = persisted_file.as_object_mut() else {
        return Err(ZippyError::Io {
            reason: "persisted_file must be an object".to_string(),
        });
    };
    object.insert(
        "file_path".to_string(),
        serde_json::Value::String(path.to_string_lossy().to_string()),
    );
    object.insert(
        "persist_data_root".to_string(),
        serde_json::Value::String(root.to_string_lossy().to_string()),
    );
    Ok(persisted_file)
}

fn persist_data_root_for_process(
    registry: &Registry,
    stream_name: &str,
    process_id: &str,
) -> Option<String> {
    registry
        .sources_for_stream(stream_name)
        .into_iter()
        .find(|source| source.process_id == process_id && source.status != "lost")
        .and_then(|source| {
            source
                .config
                .get("persist_data_root")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn prevalidate_persisted_files_for_delete(
    config: &ZippyConfig,
    persisted_files: &[serde_json::Value],
) -> Result<()> {
    let root = canonical_persist_data_root(config)?;
    for persisted_file in persisted_files {
        let _ = persisted_file_canonical_path(&root, persisted_file)?;
    }
    Ok(())
}

fn canonical_persist_data_root(config: &ZippyConfig) -> Result<PathBuf> {
    canonical_persist_data_root_value(&config.table.persist.data_dir)
}

fn canonical_persist_data_root_value(path: &str) -> Result<PathBuf> {
    fs::canonicalize(path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to canonicalize persist data root path=[{}] error=[{}]",
            path, error
        ),
    })
}

fn persisted_file_canonical_path(
    persist_root: &Path,
    persisted_file: &serde_json::Value,
) -> Result<PathBuf> {
    let Some(file_path) = persisted_file
        .get("file_path")
        .and_then(serde_json::Value::as_str)
    else {
        return Err(ZippyError::Io {
            reason: "persisted_file.file_path must be a string".to_string(),
        });
    };
    if file_path.is_empty() {
        return Err(ZippyError::Io {
            reason: "persisted_file.file_path must not be empty".to_string(),
        });
    }
    let path = PathBuf::from(file_path);
    let canonical_path = fs::canonicalize(&path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to canonicalize persisted table file path=[{}] error=[{}]",
            path.display(),
            error
        ),
    })?;
    if !canonical_path.starts_with(persist_root) {
        return Err(ZippyError::Io {
            reason: format!(
                "persisted table file outside persist data root path=[{}] root=[{}]",
                canonical_path.display(),
                persist_root.display()
            ),
        });
    }
    if !canonical_path.is_file() {
        return Err(ZippyError::Io {
            reason: format!(
                "persisted table path is not a file path=[{}]",
                canonical_path.display()
            ),
        });
    }
    Ok(canonical_path)
}

fn delete_persisted_files(
    config: &ZippyConfig,
    persisted_files: &[serde_json::Value],
) -> Result<usize> {
    let root = canonical_persist_data_root(config)?;
    let mut deleted = 0;
    let mut parent_dirs = Vec::new();
    for persisted_file in persisted_files {
        let path = persisted_file_canonical_path(&root, persisted_file)?;
        match fs::remove_file(&path) {
            Ok(()) => {
                deleted += 1;
                if let Some(parent) = path.parent() {
                    parent_dirs.push(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ZippyError::Io {
                    reason: format!(
                        "failed to delete persisted table file path=[{}] error=[{}]",
                        path.display(),
                        error
                    ),
                });
            }
        }
    }

    parent_dirs.sort_by(|left, right| {
        right
            .components()
            .count()
            .cmp(&left.components().count())
            .then_with(|| left.cmp(right))
    });
    parent_dirs.dedup();
    for parent in parent_dirs {
        if parent == root {
            continue;
        }
        let _ = fs::remove_dir(parent);
    }

    Ok(deleted)
}

fn compaction_groups(
    persisted_files: &[serde_json::Value],
    stream: &StreamRecord,
) -> BTreeMap<String, Vec<serde_json::Value>> {
    let live_identities = live_segment_identities(stream);
    let mut groups = BTreeMap::<String, Vec<serde_json::Value>>::new();
    for item in persisted_files {
        let Some(file_path) = item.get("file_path").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let path = PathBuf::from(file_path);
        if !path.exists() {
            continue;
        }
        let identities = persisted_segment_identities(item);
        if segment_identities_overlap(&identities, &live_identities) {
            continue;
        }
        let key = persisted_compaction_group_key(item, &path);
        groups.entry(key).or_default().push(item.clone());
    }
    for group_files in groups.values_mut() {
        group_files.sort_by_key(persisted_file_order_key);
    }
    groups
}

fn compact_persisted_file_group(
    stream_name: &str,
    schema_hash: &str,
    group_files: &[serde_json::Value],
    configured_sort_column: Option<&str>,
) -> Result<serde_json::Value> {
    if group_files.is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "compaction group must not be empty".to_string(),
        });
    }
    let batches = group_files
        .iter()
        .map(read_persisted_parquet_batch)
        .collect::<Result<Vec<_>>>()?;
    let schema = batches[0].schema();
    let batch = concat_batches(&schema, batches.iter()).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to concatenate compacted parquet batches error=[{}]",
            error
        ),
    })?;
    let sort_columns =
        resolve_compaction_sort_columns(&batch, group_files, configured_sort_column)?;
    let batch = if sort_columns.is_empty() {
        batch
    } else {
        sort_record_batch(&batch, &sort_columns)?
    };
    let first_file_path = group_files[0]
        .get("file_path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "persisted file missing file_path".to_string(),
        })?;
    let target_dir =
        Path::new(first_file_path)
            .parent()
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: format!(
                    "persisted file path has no parent path=[{}]",
                    first_file_path
                ),
            })?;
    let digest = compact_group_digest(group_files);
    let target_path = target_dir.join(format!(
        "compact-{}-{:016x}.parquet",
        safe_file_token(stream_name),
        digest
    ));
    let temp_path = target_path.with_file_name(format!(
        "{}.tmp-{}",
        target_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("compact.parquet"),
        persisted_created_at_millis_for_compaction()
    ));
    write_record_batch_parquet(&temp_path, &batch)?;
    fs::rename(&temp_path, &target_path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to rename compacted parquet temp_path=[{}] target_path=[{}] error=[{}]",
            temp_path.display(),
            target_path.display(),
            error
        ),
    })?;

    Ok(compacted_persisted_file_metadata(
        stream_name,
        schema_hash,
        group_files,
        &target_path,
        batch.num_rows(),
        &sort_columns,
        &batch,
        digest,
    ))
}

fn read_persisted_parquet_batch(item: &serde_json::Value) -> Result<RecordBatch> {
    let file_path = item
        .get("file_path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "persisted file missing file_path".to_string(),
        })?;
    let file = File::open(file_path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to open persisted parquet file path=[{}] error=[{}]",
            file_path, error
        ),
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create persisted parquet reader path=[{}] error=[{}]",
                file_path, error
            ),
        })?
        .build()
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to build persisted parquet reader path=[{}] error=[{}]",
                file_path, error
            ),
        })?;
    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to read persisted parquet file path=[{}] error=[{}]",
                file_path, error
            ),
        })?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| ZippyError::Io {
            reason: format!("persisted parquet file is empty path=[{}]", file_path),
        })?;
    concat_batches(&schema, batches.iter()).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to concatenate persisted parquet file path=[{}] error=[{}]",
            file_path, error
        ),
    })
}

fn resolve_compaction_sort_columns(
    batch: &RecordBatch,
    group_files: &[serde_json::Value],
    configured_sort_column: Option<&str>,
) -> Result<Vec<String>> {
    let requested = configured_sort_column
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| partition_dt_column(group_files))
        .or_else(|| {
            ["dt", "event_ts", "localtime_ns"]
                .into_iter()
                .find(|candidate| batch.schema().index_of(candidate).is_ok())
                .map(str::to_string)
        });
    let Some(requested) = requested else {
        return Ok(Vec::new());
    };
    if batch.schema().index_of(&requested).is_err() {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("compaction sort column not found column=[{}]", requested),
        });
    }
    let mut columns = vec![requested.clone()];
    for tie_breaker in ["localtime_ns", "seq"] {
        if tie_breaker != requested && batch.schema().index_of(tie_breaker).is_ok() {
            columns.push(tie_breaker.to_string());
        }
    }
    Ok(columns)
}

fn sort_record_batch(batch: &RecordBatch, sort_column_names: &[String]) -> Result<RecordBatch> {
    let sort_columns = sort_column_names
        .iter()
        .map(|column_name| {
            let column_index = batch.schema().index_of(column_name).map_err(|error| {
                ZippyError::SchemaMismatch {
                    reason: format!(
                        "compaction sort column not found column=[{}] error=[{}]",
                        column_name, error
                    ),
                }
            })?;
            Ok(SortColumn {
                values: batch.column(column_index).clone(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&sort_columns, None).map_err(|error| ZippyError::Io {
        reason: format!("failed to sort compacted parquet batch error=[{}]", error),
    })?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to reorder compacted parquet column error=[{}]",
                    error
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| ZippyError::Io {
        reason: format!("failed to build compacted sorted batch error=[{}]", error),
    })
}

fn write_record_batch_parquet(target_path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(target_path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to create compacted parquet file path=[{}] error=[{}]",
            target_path.display(),
            error
        ),
    })?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to create compacted parquet writer path=[{}] error=[{}]",
                target_path.display(),
                error
            ),
        })?;
    writer.write(batch).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to write compacted parquet path=[{}] error=[{}]",
            target_path.display(),
            error
        ),
    })?;
    writer.close().map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to close compacted parquet path=[{}] error=[{}]",
            target_path.display(),
            error
        ),
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compacted_persisted_file_metadata(
    stream_name: &str,
    schema_hash: &str,
    group_files: &[serde_json::Value],
    target_path: &Path,
    row_count: usize,
    sort_columns: &[String],
    batch: &RecordBatch,
    digest: u64,
) -> serde_json::Value {
    let mut metadata = serde_json::json!({
        "persist_file_id": format!("compact:{}:{:016x}", stream_name, digest),
        "stream_name": stream_name,
        "schema_hash": schema_hash,
        "file_path": target_path.to_string_lossy(),
        "row_count": row_count,
        "created_at": persisted_created_at_millis_for_compaction(),
        "compacted": true,
        "compacted_file_count": group_files.len(),
        "compacted_source_file_ids": group_files
            .iter()
            .map(|item| item.get("persist_file_id").cloned().unwrap_or_else(|| {
                item.get("file_path").cloned().unwrap_or(serde_json::Value::Null)
            }))
            .collect::<Vec<_>>(),
    });
    if let Some(first_file) = group_files.first() {
        for key in ["partition", "partition_path", "partition_spec"] {
            if let Some(value) = first_file.get(key) {
                metadata[key] = value.clone();
            }
        }
    }
    let source_segments = group_files
        .iter()
        .flat_map(persisted_segment_identities)
        .map(|identity| {
            let mut source_segment = serde_json::json!({
                "source_segment_id": identity.segment_id,
                "source_generation": identity.generation,
            });
            if let Some(writer_epoch) = identity.writer_epoch {
                source_segment["writer_epoch"] = serde_json::json!(writer_epoch);
            }
            source_segment
        })
        .collect::<Vec<_>>();
    if !source_segments.is_empty() {
        metadata["source_segments"] = serde_json::Value::Array(source_segments.clone());
        metadata["source_segment_id"] = source_segments[0]["source_segment_id"].clone();
        metadata["source_generation"] = source_segments[0]["source_generation"].clone();
    }
    if !sort_columns.is_empty() {
        metadata["sort_columns"] = serde_json::json!(sort_columns);
    }
    let stats = compaction_stats(batch, sort_columns);
    if !stats.is_null() {
        metadata["stats"] = stats.clone();
    }
    if matches!(
        sort_columns.first().map(String::as_str),
        Some("dt" | "event_ts")
    ) {
        if let Some(primary_stats) = sort_columns
            .first()
            .and_then(|column| stats.get(column))
            .and_then(serde_json::Value::as_object)
        {
            if let Some(min) = primary_stats.get("min") {
                metadata["min_event_ts"] = min.clone();
            }
            if let Some(max) = primary_stats.get("max") {
                metadata["max_event_ts"] = max.clone();
            }
        }
    }
    metadata
}

fn compaction_stats(batch: &RecordBatch, sort_columns: &[String]) -> serde_json::Value {
    let mut stats = serde_json::Map::new();
    for column_name in sort_columns {
        let Ok(column_index) = batch.schema().index_of(column_name) else {
            continue;
        };
        let column = batch.column(column_index);
        if let Some((min, max)) = numeric_min_max(column) {
            stats.insert(
                column_name.clone(),
                serde_json::json!({
                    "min": min,
                    "max": max,
                }),
            );
        }
    }
    if stats.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::Object(stats)
    }
}

fn numeric_min_max(column: &ArrayRef) -> Option<(serde_json::Value, serde_json::Value)> {
    match column.data_type() {
        DataType::Int64 | DataType::Timestamp(_, _) => {
            let values = column.as_any().downcast_ref::<Int64Array>()?;
            let mut min = None::<i64>;
            let mut max = None::<i64>;
            for index in 0..values.len() {
                if values.is_null(index) {
                    continue;
                }
                let value = values.value(index);
                min = Some(min.map_or(value, |current| current.min(value)));
                max = Some(max.map_or(value, |current| current.max(value)));
            }
            Some((serde_json::json!(min?), serde_json::json!(max?)))
        }
        DataType::Float64 => {
            let values = column.as_any().downcast_ref::<Float64Array>()?;
            let mut min = None::<f64>;
            let mut max = None::<f64>;
            for index in 0..values.len() {
                if values.is_null(index) {
                    continue;
                }
                let value = values.value(index);
                min = Some(min.map_or(value, |current| current.min(value)));
                max = Some(max.map_or(value, |current| current.max(value)));
            }
            Some((serde_json::json!(min?), serde_json::json!(max?)))
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SegmentIdentity {
    segment_id: u64,
    generation: u64,
    writer_epoch: Option<u64>,
}

fn live_segment_identities(stream: &StreamRecord) -> HashSet<SegmentIdentity> {
    let mut identities = HashSet::new();
    if let Some(identity) = stream
        .active_segment_descriptor
        .as_ref()
        .and_then(descriptor_segment_identity_from_value)
    {
        identities.insert(identity);
    }
    for segment in &stream.sealed_segments {
        if let Some(identity) = descriptor_segment_identity_from_value(segment) {
            identities.insert(identity);
        }
    }
    identities
}

fn persisted_segment_identities(value: &serde_json::Value) -> Vec<SegmentIdentity> {
    if let Some(source_segments) = value
        .get("source_segments")
        .and_then(serde_json::Value::as_array)
    {
        return source_segments
            .iter()
            .filter_map(persisted_segment_identity_from_value)
            .collect();
    }
    persisted_segment_identity_from_value(value)
        .into_iter()
        .collect()
}

fn descriptor_segment_identity_from_value(value: &serde_json::Value) -> Option<SegmentIdentity> {
    segment_identity_from_value(value, "segment_id", "generation")
}

fn persisted_segment_identity_from_value(value: &serde_json::Value) -> Option<SegmentIdentity> {
    segment_identity_from_value(value, "source_segment_id", "source_generation")
}

fn segment_identity_from_value(
    value: &serde_json::Value,
    segment_key: &str,
    generation_key: &str,
) -> Option<SegmentIdentity> {
    Some(SegmentIdentity {
        segment_id: value.get(segment_key).and_then(serde_json::Value::as_u64)?,
        generation: value
            .get(generation_key)
            .and_then(serde_json::Value::as_u64)?,
        writer_epoch: value
            .get("writer_epoch")
            .and_then(serde_json::Value::as_u64),
    })
}

fn segment_identities_overlap(
    persisted: &[SegmentIdentity],
    live: &HashSet<SegmentIdentity>,
) -> bool {
    persisted.iter().any(|persisted_identity| {
        live.iter()
            .any(|live_identity| segment_identity_matches(*persisted_identity, *live_identity))
    })
}

fn segment_identity_matches(left: SegmentIdentity, right: SegmentIdentity) -> bool {
    if left.segment_id != right.segment_id || left.generation != right.generation {
        return false;
    }
    match (left.writer_epoch, right.writer_epoch) {
        (Some(left_epoch), Some(right_epoch)) => left_epoch == right_epoch,
        _ => true,
    }
}

fn persisted_compaction_group_key(item: &serde_json::Value, path: &Path) -> String {
    if let Some(partition_path) = item
        .get("partition_path")
        .and_then(serde_json::Value::as_str)
    {
        return format!("partition_path:{partition_path}");
    }
    if let Some(partition) = item.get("partition") {
        if partition.is_object() {
            return format!("partition:{partition}");
        }
    }
    format!(
        "parent:{}",
        path.parent()
            .map(|path| path.display().to_string())
            .unwrap_or_default()
    )
}

fn partition_dt_column(group_files: &[serde_json::Value]) -> Option<String> {
    group_files.iter().find_map(|item| {
        item.get("partition_spec")
            .and_then(serde_json::Value::as_object)
            .and_then(|partition_spec| partition_spec.get("dt_column"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
    })
}

fn persisted_file_compaction_key(item: &serde_json::Value) -> (String, String) {
    item.get("persist_file_id")
        .and_then(serde_json::Value::as_str)
        .map(|value| ("id".to_string(), value.to_string()))
        .unwrap_or_else(|| {
            (
                "path".to_string(),
                item.get("file_path")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            )
        })
}

fn persisted_file_order_key(item: &serde_json::Value) -> (u64, u64, u64, String) {
    (
        item.get("source_segment_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        item.get("source_generation")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        item.get("created_at")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default(),
        item.get("file_path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
    )
}

fn compact_group_digest(group_files: &[serde_json::Value]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for item in group_files {
        persisted_file_compaction_key(item).hash(&mut hasher);
    }
    hasher.finish()
}

fn safe_file_token(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn persisted_created_at_millis_for_compaction() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn generate_control_token(label: &str) -> String {
    if let Some(token) = random_token_from_os() {
        return token;
    }
    let now = persisted_created_at_millis_for_compaction();
    let mut hasher = DefaultHasher::new();
    label.hash(&mut hasher);
    now.hash(&mut hasher);
    thread::current().id().hash(&mut hasher);
    format!("{:016x}{:016x}", hasher.finish(), now)
}

fn random_token_from_os() -> Option<String> {
    let mut bytes = [0_u8; 32];
    let mut file = File::open("/dev/urandom").ok()?;
    file.read_exact(&mut bytes).ok()?;
    Some(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
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

fn validate_register_stream_request(
    registry: &Registry,
    stream_name: &str,
    schema_hash: &str,
    buffer_size: usize,
    frame_size: usize,
) -> std::result::Result<(), crate::registry::RegistryError> {
    if buffer_size == 0 || frame_size == 0 {
        return Err(crate::registry::RegistryError::InvalidStreamConfig {
            stream_name: stream_name.to_string(),
            buffer_size,
            frame_size,
        });
    }

    if let Some(existing) = registry.get_stream(stream_name) {
        if existing.buffer_size != buffer_size || existing.frame_size != frame_size {
            return Err(crate::registry::RegistryError::StreamConfigMismatch {
                stream_name: stream_name.to_string(),
                existing_buffer_size: existing.buffer_size,
                existing_frame_size: existing.frame_size,
                requested_buffer_size: buffer_size,
                requested_frame_size: frame_size,
            });
        }
        if existing.schema_hash != schema_hash {
            return Err(crate::registry::RegistryError::StreamSchemaMismatch {
                stream_name: stream_name.to_string(),
                existing_schema_hash: existing.schema_hash.clone(),
                requested_schema_hash: schema_hash.to_string(),
            });
        }
    }

    Ok(())
}

fn io_error(error: std::io::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn json_error(error: serde_json::Error) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn registry_error(error: crate::registry::RegistryError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn restored_record_status(status: &str) -> &str {
    match status {
        "lost" | "stale" => status,
        _ => "restored",
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

impl From<crate::registry::StreamRecord> for StreamInfo {
    fn from(stream: crate::registry::StreamRecord) -> Self {
        Self {
            stream_name: stream.stream_name,
            schema: stream.schema,
            schema_hash: stream.schema_hash,
            data_path: stream.data_path,
            descriptor_generation: stream.descriptor_generation,
            active_segment_descriptor: stream.active_segment_descriptor,
            source_configs: Vec::new(),
            active_segment_preflight: None,
            segment_row_capacity: None,
            sealed_segments: stream.sealed_segments,
            persisted_files: stream.persisted_files,
            persist_events: stream.persist_events,
            segment_reader_leases: stream.segment_reader_leases,
            buffer_size: stream.buffer_size,
            frame_size: stream.frame_size,
            write_seq: 0,
            writer_process_id: stream.writer_process_id,
            writer_epoch: stream.writer_epoch,
            reader_count: stream.reader_count,
            status: stream.status,
        }
    }
}

fn stream_info_with_active_preflight(stream: crate::registry::StreamRecord) -> StreamInfo {
    let mut info = StreamInfo::from(stream);
    attach_active_preflight(&mut info);
    info
}

fn stream_info_with_registry_metadata(
    registry: &Registry,
    stream: crate::registry::StreamRecord,
) -> StreamInfo {
    let stream_name = stream.stream_name.clone();
    let mut info = stream_info_with_active_preflight(stream);
    info.source_configs = source_configs_for_stream(registry, &stream_name);
    info
}

fn stream_status_with_active_preflight(stream: &crate::registry::StreamRecord) -> StreamInfo {
    let mut info = StreamInfo {
        stream_name: stream.stream_name.clone(),
        schema: stream.schema.clone(),
        schema_hash: stream.schema_hash.clone(),
        data_path: stream.data_path.clone(),
        descriptor_generation: stream.descriptor_generation,
        active_segment_descriptor: stream.active_segment_descriptor.clone(),
        source_configs: Vec::new(),
        active_segment_preflight: None,
        segment_row_capacity: None,
        sealed_segments: Vec::new(),
        persisted_files: Vec::new(),
        persist_events: Vec::new(),
        segment_reader_leases: stream.segment_reader_leases.clone(),
        buffer_size: stream.buffer_size,
        frame_size: stream.frame_size,
        write_seq: 0,
        writer_process_id: stream.writer_process_id.clone(),
        writer_epoch: stream.writer_epoch,
        reader_count: stream.reader_count,
        status: stream.status.clone(),
    };
    attach_active_preflight(&mut info);
    info
}

fn stream_status_with_registry_metadata(
    registry: &Registry,
    stream: &crate::registry::StreamRecord,
) -> StreamInfo {
    let mut info = stream_status_with_active_preflight(stream);
    info.source_configs = source_configs_for_stream(registry, &stream.stream_name);
    info
}

fn source_configs_for_stream(registry: &Registry, stream_name: &str) -> Vec<serde_json::Value> {
    registry
        .sources_for_stream(stream_name)
        .into_iter()
        .map(|source| source.config)
        .collect()
}

fn attach_active_preflight(info: &mut StreamInfo) {
    if let Some(descriptor) = info.active_segment_descriptor.as_ref() {
        info.segment_row_capacity = descriptor_row_capacity(descriptor);
        info.active_segment_preflight = Some(active_segment_preflight(descriptor));
    }
}

fn active_segment_preflight(descriptor: &serde_json::Value) -> serde_json::Value {
    let row_capacity = descriptor_row_capacity(descriptor);
    if row_capacity.is_none() {
        return active_segment_preflight_error(
            "active_descriptor_invalid",
            "active segment descriptor missing row_capacity",
        );
    }
    let Some(shm_os_id) = descriptor
        .get("shm_os_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return active_segment_preflight_error(
            "active_descriptor_invalid",
            "active segment descriptor missing shm_os_id",
        );
    };
    let Some(committed_row_count_offset) = descriptor
        .get("committed_row_count_offset")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
    else {
        return active_segment_preflight_error(
            "active_descriptor_invalid",
            "active segment descriptor missing committed_row_count_offset",
        );
    };

    match read_active_segment_committed_row_count(shm_os_id, committed_row_count_offset) {
        Ok(committed_row_count) => serde_json::json!({
            "status": "ok",
            "kind": "active_segment_readable",
            "readable": true,
            "shm_os_id": shm_os_id,
            "row_capacity": row_capacity,
            "committed_row_count": committed_row_count,
            "message": format!("active segment is readable shm_os_id=[{}]", shm_os_id),
        }),
        Err(reason) => active_segment_preflight_error(
            "active_segment_unreadable",
            &format!(
                "failed to open active segment shm_os_id=[{}] reason=[{}]",
                shm_os_id, reason
            ),
        )
        .with_row_capacity(row_capacity),
    }
}

fn active_segment_preflight_error(kind: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "error",
        "kind": kind,
        "readable": false,
        "message": message,
    })
}

trait ActiveSegmentPreflightExt {
    fn with_row_capacity(self, row_capacity: Option<usize>) -> serde_json::Value;
}

impl ActiveSegmentPreflightExt for serde_json::Value {
    fn with_row_capacity(mut self, row_capacity: Option<usize>) -> serde_json::Value {
        if let Some(object) = self.as_object_mut() {
            object.insert("row_capacity".to_string(), serde_json::json!(row_capacity));
        }
        self
    }
}

fn descriptor_row_capacity(descriptor: &serde_json::Value) -> Option<usize> {
    descriptor
        .get("row_capacity")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn read_active_segment_committed_row_count(
    shm_os_id: &str,
    committed_row_count_offset: usize,
) -> std::result::Result<u64, String> {
    let region = ShmRegion::open(shm_os_id).map_err(|error| error.to_string())?;
    let mut bytes = [0_u8; 8];
    region
        .read_at(committed_row_count_offset, &mut bytes)
        .map_err(|error| error.to_string())?;
    Ok(u64::from_le_bytes(bytes))
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
    fn accept_loop_idle_sleep_stays_below_control_plane_latency_budget() {
        assert!(MASTER_ACCEPT_IDLE_SLEEP <= Duration::from_millis(1));
    }

    #[test]
    fn compaction_groups_keep_persisted_file_when_writer_epoch_differs_from_live() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("segment-1-epoch-1.parquet");
        std::fs::write(&file_path, []).unwrap();
        let stream = stream_record_for_segment_identity_test(serde_json::json!({
            "segment_id": 1,
            "generation": 0,
            "writer_epoch": 2,
        }));
        let persisted_files = vec![serde_json::json!({
            "file_path": file_path.to_string_lossy(),
            "source_segment_id": 1,
            "source_generation": 0,
            "writer_epoch": 1,
        })];

        let groups = compaction_groups(&persisted_files, &stream);

        assert_eq!(
            groups.values().map(Vec::len).sum::<usize>(),
            1,
            "different writer_epoch identifies a different writer lifetime"
        );
    }

    #[test]
    fn compaction_groups_skip_persisted_file_when_writer_epoch_matches_live() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("segment-1-epoch-2.parquet");
        std::fs::write(&file_path, []).unwrap();
        let stream = stream_record_for_segment_identity_test(serde_json::json!({
            "segment_id": 1,
            "generation": 0,
            "writer_epoch": 2,
        }));
        let persisted_files = vec![serde_json::json!({
            "file_path": file_path.to_string_lossy(),
            "source_segment_id": 1,
            "source_generation": 0,
            "writer_epoch": 2,
        })];

        let groups = compaction_groups(&persisted_files, &stream);

        assert!(groups.is_empty());
    }

    fn stream_record_for_segment_identity_test(
        active_segment_descriptor: serde_json::Value,
    ) -> StreamRecord {
        serde_json::from_value(serde_json::json!({
            "stream_name": "ctp_ticks",
            "schema": {},
            "schema_hash": "schema-a",
            "data_path": "",
            "descriptor_generation": 1,
            "buffer_size": 1024,
            "frame_size": 256,
            "writer_process_id": null,
            "writer_epoch": 2,
            "reader_count": 0,
            "status": "active",
            "active_segment_descriptor": active_segment_descriptor,
            "reader_process_ids": {},
        }))
        .unwrap()
    }
}
