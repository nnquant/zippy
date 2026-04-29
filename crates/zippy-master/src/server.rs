use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use zippy_core::bus_protocol::{
    DropTableResult, GetStreamResponse, ListStreamsResponse, StreamInfo,
};
use zippy_core::{ControlRequest, ControlResponse, Result, ZippyConfig, ZippyError};

use crate::bus::{Bus, BusError};
use crate::registry::Registry;
use crate::snapshot::{
    RegistrySnapshot, SnapshotEngineRecord, SnapshotSinkRecord, SnapshotSourceRecord,
    SnapshotStore, SnapshotStreamRecord,
};

const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_LEASE_REAPER_INTERVAL: Duration = Duration::from_secs(2);
const MASTER_ACCEPT_IDLE_SLEEP: Duration = Duration::from_millis(1);

#[derive(Clone, Debug)]
pub struct MasterServer {
    registry: Arc<Mutex<Registry>>,
    descriptor_changed: Arc<Condvar>,
    #[allow(dead_code)]
    bus: Arc<Mutex<Bus>>,
    running: Arc<AtomicBool>,
    snapshot_lock: Arc<Mutex<()>>,
    snapshot_path: Option<PathBuf>,
    lease_timeout: Duration,
    lease_reaper_interval: Duration,
    config: ZippyConfig,
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
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
            descriptor_changed: Arc::new(Condvar::new()),
            bus: Arc::new(Mutex::new(Bus::default())),
            running: Arc::new(AtomicBool::new(true)),
            snapshot_lock: Arc::new(Mutex::new(())),
            snapshot_path,
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

    pub fn from_snapshot_path(snapshot_path: &Path) -> Result<Self> {
        let snapshot = SnapshotStore::load(snapshot_path)?;
        let server = Self::with_runtime_config(
            Some(snapshot_path.to_path_buf()),
            DEFAULT_LEASE_TIMEOUT,
            DEFAULT_LEASE_REAPER_INTERVAL,
        );

        {
            let mut bus = server.bus.lock().unwrap();
            let mut registry = server.registry.lock().unwrap();

            for stream in snapshot.streams {
                bus.ensure_stream_with_sizes(
                    &stream.stream_name,
                    stream.buffer_size,
                    stream.frame_size,
                )
                .map_err(bus_error)?;
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
                    .set_stream_status(&stream.stream_name, "restored")
                    .map_err(registry_error)?;
                registry
                    .set_stream_segment_metadata(
                        &stream.stream_name,
                        stream.descriptor_generation,
                        stream.sealed_segments,
                        stream.persisted_files,
                        stream.persist_events,
                        stream.segment_reader_leases,
                    )
                    .map_err(registry_error)?;
            }

            for source in snapshot.sources {
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
                    .set_source_status(&source.source_name, "restored", Some(source.metrics))
                    .map_err(registry_error)?;
            }

            for engine in snapshot.engines {
                registry
                    .register_engine(
                        &engine.engine_name,
                        &engine.engine_type,
                        &engine.process_id,
                        &engine.input_stream,
                        &engine.output_stream,
                        engine.sink_names,
                        engine.config,
                    )
                    .map_err(registry_error)?;
                registry
                    .set_engine_status(&engine.engine_name, "restored", Some(engine.metrics))
                    .map_err(registry_error)?;
            }

            for sink in snapshot.sinks {
                registry
                    .register_sink(
                        &sink.sink_name,
                        &sink.sink_type,
                        &sink.process_id,
                        &sink.input_stream,
                        sink.config,
                    )
                    .map_err(registry_error)?;
                registry
                    .set_sink_status(&sink.sink_name, "restored")
                    .map_err(registry_error)?;
            }
        }

        Ok(server)
    }

    pub fn serve(&self, socket_path: &Path) -> Result<()> {
        self.serve_with_ready(socket_path, None)
    }

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

        tracing::info!(
            component = "master",
            event = "master_listening",
            status = "ready",
            control_endpoint = socket_path.display().to_string(),
            "master listening"
        );

        self.start_lease_reaper();

        let accept_result = loop {
            if !self.running.load(Ordering::SeqCst) {
                break Ok(());
            }

            match listener.accept() {
                Ok((stream, _)) => {
                    let server = self.clone();
                    thread::spawn(move || {
                        if let Err(error) = server.handle_stream(stream) {
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

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    fn handle_stream(&self, mut stream: UnixStream) -> Result<()> {
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(io_error)?;
        let mut request_line = String::new();
        let mut reader = BufReader::new(stream.try_clone().map_err(io_error)?);
        reader.read_line(&mut request_line).map_err(io_error)?;

        if let Some(response) = self.try_handle_test_control_request(&request_line)? {
            return write_control_response(&mut stream, &response);
        }

        let request = serde_json::from_str::<ControlRequest>(&request_line).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to decode control request error=[{}]", error),
            }
        })?;

        let response = match request {
            ControlRequest::RegisterProcess(request) => {
                let process_id = self.registry.lock().unwrap().register_process(&request.app);
                tracing::info!(
                    component = "master_server",
                    event = "register_process",
                    status = "success",
                    process_id = process_id.as_str(),
                    app = request.app.as_str(),
                    "registered process"
                );
                ControlResponse::ProcessRegistered { process_id }
            }
            ControlRequest::Heartbeat(request) => {
                match self
                    .registry
                    .lock()
                    .unwrap()
                    .record_heartbeat(&request.process_id)
                {
                    Ok(()) => {
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
            ControlRequest::RegisterStream(request) => {
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut bus = self.bus.lock().unwrap();
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
                match bus.ensure_stream_with_sizes(
                    &request.stream_name,
                    request.buffer_size,
                    request.frame_size,
                ) {
                    Ok(bus_created) => match registry.ensure_stream(
                        &request.stream_name,
                        request.schema.clone(),
                        &request.schema_hash,
                        request.buffer_size,
                        request.frame_size,
                    ) {
                        Ok(registry_created) => {
                            let existing = !(bus_created || registry_created);
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
                                    if bus_created {
                                        bus.remove_stream(&request.stream_name);
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
                            if bus_created {
                                bus.remove_stream(&request.stream_name);
                            }
                            ControlResponse::Error {
                                reason: error.to_string(),
                            }
                        }
                    },
                    Err(error) => {
                        let error = normalize_register_stream_bus_error(
                            &registry,
                            &request.stream_name,
                            request.buffer_size,
                            request.frame_size,
                            error,
                        );
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
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry
                    .unregister_source_for_process(&request.source_name, &request.process_id)
                {
                    Ok(source) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            let _ = registry.register_source(
                                &source.source_name,
                                &source.source_type,
                                &source.process_id,
                                &source.output_stream,
                                source.config,
                            );
                            let _ = registry.set_source_status(
                                &source.source_name,
                                &source.status,
                                Some(source.metrics),
                            );
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
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.register_engine(
                    &request.engine_name,
                    &request.engine_type,
                    &request.process_id,
                    &request.input_stream,
                    &request.output_stream,
                    request.sink_names,
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
            ControlRequest::RegisterSink(request) => {
                let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.register_sink(
                    &request.sink_name,
                    &request.sink_type,
                    &request.process_id,
                    &request.input_stream,
                    request.config,
                ) {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            registry.unregister_sink(&request.sink_name);
                            tracing::error!(
                                component = "master_server",
                                event = "snapshot_write_failure",
                                status = "error",
                                sink_name = request.sink_name.as_str(),
                                error = %error,
                                "failed to persist sink snapshot"
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
                            event = "register_sink",
                            status = "success",
                            sink_name = request.sink_name.as_str(),
                            process_id = request.process_id.as_str(),
                            input_stream = request.input_stream.as_str(),
                            "registered sink"
                        );
                        ControlResponse::SinkRegistered {
                            sink_name: request.sink_name,
                        }
                    }
                    Err(error) => ControlResponse::Error {
                        reason: error.to_string(),
                    },
                }
            }
            ControlRequest::UpdateStatus(request) => {
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
                    "sink" => {
                        let previous = registry.get_sink(&request.name).cloned();
                        let result = registry.set_sink_status(&request.name, &request.status);
                        (
                            result,
                            previous.map(|record| ("sink", serde_json::to_value(record).unwrap())),
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
                let mut registry = self.registry.lock().unwrap();
                let previous_persisted_files = registry
                    .get_stream(&request.stream_name)
                    .map(|stream| stream.persisted_files.clone())
                    .unwrap_or_default();
                let publish_result = registry.publish_persisted_file(
                    &request.stream_name,
                    &request.process_id,
                    request.persisted_file,
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
            ControlRequest::PublishPersistEvent(request) => {
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
                let mut registry = self.registry.lock().unwrap();
                let acquire_result = registry.acquire_segment_reader_lease(
                    &request.stream_name,
                    &request.process_id,
                    request.source_segment_id,
                    request.source_generation,
                );
                match acquire_result {
                    Ok(lease_id) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
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
                let mut registry = self.registry.lock().unwrap();
                let release_result = registry.release_segment_reader_lease(
                    &request.stream_name,
                    &request.process_id,
                    &request.lease_id,
                );
                match release_result {
                    Ok(()) => {
                        let snapshot = Self::snapshot_from_registry(&registry);
                        if let Err(error) = self.write_snapshot_from_snapshot(&snapshot) {
                            return write_control_response(
                                &mut stream,
                                &ControlResponse::Error {
                                    reason: error.to_string(),
                                },
                            );
                        }
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
            ControlRequest::WaitSegmentDescriptor(request) => {
                let timeout = Duration::from_millis(request.timeout_ms);
                let mut registry = self.registry.lock().unwrap();
                let wait_result = registry.segment_descriptor_update_for_process(
                    &request.stream_name,
                    &request.process_id,
                    request.after_descriptor_generation,
                );
                let response_result = match wait_result {
                    Ok(Some(update)) => Ok(Some(update)),
                    Ok(None) => {
                        let (next_registry, _) = self
                            .descriptor_changed
                            .wait_timeout_while(registry, timeout, |registry| {
                                matches!(
                                    registry.segment_descriptor_update_for_process(
                                        &request.stream_name,
                                        &request.process_id,
                                        request.after_descriptor_generation,
                                    ),
                                    Ok(None)
                                )
                            })
                            .unwrap();
                        registry = next_registry;
                        registry.segment_descriptor_update_for_process(
                            &request.stream_name,
                            &request.process_id,
                            request.after_descriptor_generation,
                        )
                    }
                    Err(error) => Err(error),
                };

                match response_result {
                    Ok(Some((descriptor_generation, descriptor))) => {
                        tracing::info!(
                            component = "master_server",
                            event = "wait_segment_descriptor",
                            status = "changed",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            descriptor_generation,
                            "segment descriptor changed"
                        );
                        ControlResponse::SegmentDescriptorChanged {
                            stream_name: request.stream_name,
                            descriptor_generation,
                            descriptor: Some(descriptor),
                        }
                    }
                    Ok(None) => ControlResponse::SegmentDescriptorChanged {
                        stream_name: request.stream_name,
                        descriptor_generation: request.after_descriptor_generation,
                        descriptor: None,
                    },
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "wait_segment_descriptor",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to wait for segment descriptor"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                }
            }
            ControlRequest::GetSegmentDescriptor(request) => {
                let registry = self.registry.lock().unwrap();
                match registry
                    .segment_descriptor_for_process(&request.stream_name, &request.process_id)
                {
                    Ok(descriptor) => {
                        tracing::info!(
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
            ControlRequest::WriteTo(request) => {
                let mut bus = self.bus.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match bus.write_to(&request.stream_name, &request.process_id) {
                    Ok(descriptor) => {
                        if let Err(error) =
                            registry.attach_writer(&request.stream_name, &request.process_id)
                        {
                            let _ = bus.detach_writer(&request.stream_name, &descriptor.writer_id);
                            tracing::error!(
                                component = "master_server",
                                event = "write_to",
                                status = "error",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                error = %error,
                                "failed to attach writer"
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
                            event = "write_to",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "attached writer"
                        );
                        ControlResponse::WriterAttached { descriptor }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "write_to",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to attach writer"
                        );
                        ControlResponse::Error {
                            reason: format!("{}", error),
                        }
                    }
                }
            }
            ControlRequest::ReadFrom(request) => {
                let mut bus = self.bus.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match bus.read_from(
                    &request.stream_name,
                    &request.process_id,
                    request.instrument_ids.clone(),
                ) {
                    Ok(descriptor) => {
                        if let Err(error) = registry.attach_reader(
                            &request.stream_name,
                            &request.process_id,
                            &descriptor.reader_id,
                        ) {
                            let _ = bus.detach_reader(&request.stream_name, &descriptor.reader_id);
                            tracing::error!(
                                component = "master_server",
                                event = "read_from",
                                status = "error",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                error = %error,
                                "failed to attach reader"
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
                            event = "read_from",
                            status = "success",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            "attached reader"
                        );
                        ControlResponse::ReaderAttached { descriptor }
                    }
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "read_from",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            error = %error,
                            "failed to attach reader"
                        );
                        ControlResponse::Error {
                            reason: format!("{}", error),
                        }
                    }
                }
            }
            ControlRequest::CloseWriter(request) => {
                let mut bus = self.bus.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.validate_writer_owner(&request.stream_name, &request.process_id) {
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "close_writer",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            writer_id = request.writer_id.as_str(),
                            error = %error,
                            "failed to detach writer"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                    Ok(()) => match bus.detach_writer(&request.stream_name, &request.writer_id) {
                        Ok(()) => {
                            let _ = registry.detach_writer(&request.stream_name);
                            tracing::info!(
                                component = "master_server",
                                event = "close_writer",
                                status = "success",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                writer_id = request.writer_id.as_str(),
                                "detached writer"
                            );
                            ControlResponse::WriterDetached {
                                stream_name: request.stream_name,
                                writer_id: request.writer_id,
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                component = "master_server",
                                event = "close_writer",
                                status = "error",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                writer_id = request.writer_id.as_str(),
                                error = %error,
                                "failed to detach writer"
                            );
                            ControlResponse::Error {
                                reason: format!("{}", error),
                            }
                        }
                    },
                }
            }
            ControlRequest::CloseReader(request) => {
                let mut bus = self.bus.lock().unwrap();
                let mut registry = self.registry.lock().unwrap();
                match registry.validate_reader_owner(
                    &request.stream_name,
                    &request.reader_id,
                    &request.process_id,
                ) {
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "close_reader",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            process_id = request.process_id.as_str(),
                            reader_id = request.reader_id.as_str(),
                            error = %error,
                            "failed to detach reader"
                        );
                        ControlResponse::Error {
                            reason: error.to_string(),
                        }
                    }
                    Ok(()) => match bus.detach_reader(&request.stream_name, &request.reader_id) {
                        Ok(()) => {
                            let _ =
                                registry.detach_reader(&request.stream_name, &request.reader_id);
                            tracing::info!(
                                component = "master_server",
                                event = "close_reader",
                                status = "success",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                reader_id = request.reader_id.as_str(),
                                "detached reader"
                            );
                            ControlResponse::ReaderDetached {
                                stream_name: request.stream_name,
                                reader_id: request.reader_id,
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                component = "master_server",
                                event = "close_reader",
                                status = "error",
                                stream_name = request.stream_name.as_str(),
                                process_id = request.process_id.as_str(),
                                reader_id = request.reader_id.as_str(),
                                error = %error,
                                "failed to detach reader"
                            );
                            ControlResponse::Error {
                                reason: format!("{}", error),
                            }
                        }
                    },
                }
            }
            ControlRequest::ListStreams(_) => {
                let streams: Vec<_> = self
                    .registry
                    .lock()
                    .unwrap()
                    .list_streams()
                    .into_iter()
                    .map(StreamInfo::from)
                    .collect();
                tracing::info!(
                    component = "master_server",
                    event = "list_streams",
                    status = "success",
                    stream_count = streams.len(),
                    "listed streams"
                );
                ControlResponse::StreamsListed(ListStreamsResponse { streams })
            }
            ControlRequest::GetStream(request) => match self
                .registry
                .lock()
                .unwrap()
                .get_stream(&request.stream_name)
                .cloned()
            {
                Some(stream) => {
                    tracing::info!(
                        component = "master_server",
                        event = "get_stream",
                        status = "success",
                        stream_name = request.stream_name.as_str(),
                        "fetched stream"
                    );
                    ControlResponse::StreamFetched(GetStreamResponse {
                        stream: StreamInfo::from(stream),
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
                        reason: format!("stream not found stream_name=[{}]", request.stream_name),
                    }
                }
            },
            ControlRequest::DropTable(request) => {
                let table_name = request.table_name.clone();
                let drop_result = {
                    let _snapshot_guard = self.snapshot_lock.lock().unwrap();
                    let mut bus = self.bus.lock().unwrap();
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
                    bus.remove_stream(&table_name);
                    self.descriptor_changed.notify_all();
                    dropped_records
                };
                let persisted_files = drop_result
                    .stream
                    .as_ref()
                    .map(|stream| stream.persisted_files.clone())
                    .unwrap_or_default();
                let persisted_files_deleted = if request.drop_persisted {
                    match delete_persisted_files(&persisted_files) {
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
                    sinks_removed = drop_result.sinks.len(),
                    persisted_files_deleted,
                    "dropped table"
                );
                ControlResponse::TableDropped(DropTableResult {
                    table_name,
                    dropped: drop_result.stream.is_some(),
                    sources_removed: drop_result.sources.len(),
                    engines_removed: drop_result.engines.len(),
                    sinks_removed: drop_result.sinks.len(),
                    persisted_files_deleted,
                })
            }
            ControlRequest::GetConfig(_) => {
                tracing::info!(
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
            let mut bus = self.bus.lock().unwrap();
            let mut registry = self.registry.lock().unwrap();
            registry
                .force_expire_process(process_id)
                .map_err(registry_error)?;

            let stale_streams = registry.streams_for_writer_process(process_id);
            for stream_name in stale_streams {
                let writer_id = format!("{stream_name}_writer");
                match bus.detach_writer(&stream_name, &writer_id) {
                    Ok(()) => {}
                    Err(BusError::WriterNotFound { .. } | BusError::StreamNotFound { .. }) => {}
                    Err(error) => return Err(bus_error(error)),
                }
                registry
                    .detach_writer(&stream_name)
                    .map_err(registry_error)?;
            }

            let stale_readers = registry.readers_for_process(process_id);
            for (stream_name, reader_id) in stale_readers {
                match bus.detach_reader(&stream_name, &reader_id) {
                    Ok(()) => {}
                    Err(BusError::ReaderNotFound { .. } | BusError::StreamNotFound { .. }) => {}
                    Err(error) => return Err(bus_error(error)),
                }
                registry
                    .detach_reader(&stream_name, &reader_id)
                    .map_err(registry_error)?;
            }
            registry.remove_segment_reader_leases_for_process(process_id);
            registry.mark_records_lost_for_process(process_id);
            Self::snapshot_from_registry(&registry)
        };
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

    fn expire_process_attachments(&self, process_id: &str) -> Result<()> {
        let snapshot = {
            let mut bus = self.bus.lock().unwrap();
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
                let writer_id = format!("{stream_name}_writer");
                match bus.detach_writer(&stream_name, &writer_id) {
                    Ok(()) => {}
                    Err(BusError::WriterNotFound { .. } | BusError::StreamNotFound { .. }) => {}
                    Err(error) => return Err(bus_error(error)),
                }
                registry
                    .detach_writer(&stream_name)
                    .map_err(registry_error)?;
                tracing::info!(
                    component = "master",
                    event = "writer_reclaimed",
                    status = "success",
                    process_id = process_id,
                    stream_name = stream_name.as_str(),
                    writer_id = writer_id.as_str(),
                    "reclaimed stale writer"
                );
            }

            let stale_readers = registry.readers_for_process(process_id);
            for (stream_name, reader_id) in stale_readers {
                match bus.detach_reader(&stream_name, &reader_id) {
                    Ok(()) => {}
                    Err(BusError::ReaderNotFound { .. } | BusError::StreamNotFound { .. }) => {}
                    Err(error) => return Err(bus_error(error)),
                }
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
                    segment_reader_leases: stream.segment_reader_leases,
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
                    sink_names: engine.sink_names,
                    config: engine.config,
                    status: engine.status,
                    metrics: engine.metrics,
                })
                .collect(),
            sinks: registry
                .list_sinks()
                .into_iter()
                .map(|sink| SnapshotSinkRecord {
                    sink_name: sink.sink_name,
                    sink_type: sink.sink_type,
                    process_id: sink.process_id,
                    input_stream: sink.input_stream,
                    config: sink.config,
                    status: sink.status,
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
        "sink" => {
            let record: crate::snapshot::SnapshotSinkRecord =
                serde_json::from_value(previous).map_err(json_error)?;
            registry
                .set_sink_status(&record.sink_name, &record.status)
                .map_err(registry_error)
        }
        _ => Err(ZippyError::Io {
            reason: format!("invalid restore kind kind=[{}]", kind),
        }),
    }
}

fn delete_persisted_files(persisted_files: &[serde_json::Value]) -> Result<usize> {
    let mut deleted = 0;
    let mut parent_dirs = Vec::new();
    for persisted_file in persisted_files {
        let Some(file_path) = persisted_file
            .get("file_path")
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        let path = PathBuf::from(file_path);
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
        let _ = fs::remove_dir(parent);
    }

    Ok(deleted)
}

fn write_control_response(stream: &mut UnixStream, response: &ControlResponse) -> Result<()> {
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

fn normalize_register_stream_bus_error(
    registry: &Registry,
    stream_name: &str,
    buffer_size: usize,
    frame_size: usize,
    error: crate::bus::BusError,
) -> ZippyError {
    match error {
        BusError::InvalidBufferOrFrameSize { .. } => {
            registry_error(crate::registry::RegistryError::InvalidStreamConfig {
                stream_name: stream_name.to_string(),
                buffer_size,
                frame_size,
            })
        }
        BusError::StreamConfigMismatch { .. } => {
            if let Some(existing) = registry.get_stream(stream_name) {
                registry_error(crate::registry::RegistryError::StreamConfigMismatch {
                    stream_name: stream_name.to_string(),
                    existing_buffer_size: existing.buffer_size,
                    existing_frame_size: existing.frame_size,
                    requested_buffer_size: buffer_size,
                    requested_frame_size: frame_size,
                })
            } else {
                ZippyError::Io {
                    reason: format!(
                        "stream configuration mismatch stream_name=[{}] requested_buffer_size=[{}] requested_frame_size=[{}] bus state differs from registry",
                        stream_name, buffer_size, frame_size
                    ),
                }
            }
        }
        other => bus_error(other),
    }
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

fn bus_error(error: crate::bus::BusError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

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
            sealed_segments: stream.sealed_segments,
            persisted_files: stream.persisted_files,
            persist_events: stream.persist_events,
            segment_reader_leases: stream.segment_reader_leases,
            buffer_size: stream.buffer_size,
            frame_size: stream.frame_size,
            write_seq: 0,
            writer_process_id: stream.writer_process_id,
            reader_count: stream.reader_count,
            status: stream.status,
        }
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct SocketOwnership {
    token: String,
    owner_path: std::path::PathBuf,
}

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

fn cleanup_socket_owner_file(socket_path: &Path) -> Result<()> {
    let owner_path = socket_owner_path(socket_path);
    match fs::remove_file(owner_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn socket_owner_path(socket_path: &Path) -> std::path::PathBuf {
    let mut owner_path = socket_path.as_os_str().to_os_string();
    owner_path.push(".owner");
    std::path::PathBuf::from(owner_path)
}

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
}
