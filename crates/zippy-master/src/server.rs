use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::mpsc::SyncSender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use zippy_core::bus_protocol::{GetStreamResponse, ListStreamsResponse, StreamInfo};
use zippy_core::{ControlRequest, ControlResponse, Result, ZippyError};

use crate::bus::Bus;
use crate::registry::Registry;

#[derive(Clone, Debug)]
pub struct MasterServer {
    registry: Arc<Mutex<Registry>>,
    #[allow(dead_code)]
    bus: Arc<Mutex<Bus>>,
    running: Arc<AtomicBool>,
}

impl Default for MasterServer {
    fn default() -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry::default())),
            bus: Arc::new(Mutex::new(Bus::default())),
            running: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl MasterServer {
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
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
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(error.to_string()));
            }
            return Err(error);
        }

        let listener = match UnixListener::bind(socket_path).map_err(io_error) {
            Ok(listener) => listener,
            Err(error) => {
                if let Some(ready_tx) = ready_tx {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                return Err(error);
            }
        };
        if let Err(error) = listener.set_nonblocking(true).map_err(io_error) {
            if let Some(ready_tx) = ready_tx {
                let _ = ready_tx.send(Err(error.to_string()));
            }
            return Err(error);
        }
        let socket_ownership = match SocketOwnership::create(socket_path) {
            Ok(socket_ownership) => socket_ownership,
            Err(error) => {
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
                    thread::sleep(Duration::from_millis(10));
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
            ControlRequest::RegisterStream(request) => {
                match self
                    .registry
                    .lock()
                    .unwrap()
                    .register_stream(&request.stream_name, request.ring_capacity)
                {
                    Ok(()) => match self
                        .bus
                        .lock()
                        .unwrap()
                        .create_stream(&request.stream_name, request.ring_capacity)
                    {
                        Ok(()) => {
                            tracing::info!(
                                component = "master_server",
                                event = "register_stream",
                                status = "success",
                                stream_name = request.stream_name.as_str(),
                                ring_capacity = request.ring_capacity,
                                "registered stream"
                            );
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
                                ring_capacity = request.ring_capacity,
                                error = %error,
                                "failed to register stream"
                            );
                            self.registry
                                .lock()
                                .unwrap()
                                .unregister_stream(&request.stream_name);
                            ControlResponse::Error {
                                reason: format!("{}", error),
                            }
                        }
                    },
                    Err(error) => {
                        tracing::error!(
                            component = "master_server",
                            event = "register_stream",
                            status = "error",
                            stream_name = request.stream_name.as_str(),
                            ring_capacity = request.ring_capacity,
                            error = %error,
                            "failed to register stream"
                        );
                        ControlResponse::Error {
                            reason: format!("{}", error),
                        }
                    }
                }
            }
            ControlRequest::WriteTo(request) => {
                match self
                    .bus
                    .lock()
                    .unwrap()
                    .write_to(&request.stream_name, &request.process_id)
                {
                    Ok(descriptor) => {
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
                match self
                    .bus
                    .lock()
                    .unwrap()
                    .read_from(&request.stream_name, &request.process_id)
                {
                    Ok(descriptor) => {
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
        };

        let payload = serde_json::to_string(&response).map_err(|error| ZippyError::Io {
            reason: format!("failed to encode control response error=[{}]", error),
        })?;
        stream.write_all(payload.as_bytes()).map_err(io_error)?;
        stream.write_all(b"\n").map_err(io_error)?;
        stream.flush().map_err(io_error)?;
        Ok(())
    }
}

fn io_error(error: std::io::Error) -> ZippyError {
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
            ring_capacity: stream.ring_capacity,
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
