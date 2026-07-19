//! Legacy Master publication adapter for the server-owned memory ingest authority.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use zippy_core::{ControlEndpoint, MasterClient, ZippyError};
use zippy_ingest::{MemoryIngestCatalog, MemoryIngestRegistration};
use zippy_types::{ErrorCode, Result, ServerError};

const INGEST_PROCESS_APP: &str = "zippy-server-ingest";
const INGEST_SOURCE_PREFIX: &str = "server.ingest.";
const LEGACY_STREAM_BUFFER_SIZE: usize = 64;
const LEGACY_STREAM_FRAME_SIZE: usize = 4_096;
const INGEST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const INGEST_HEARTBEAT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const INGEST_HEARTBEAT_STOP_POLL_INTERVAL: Duration = Duration::from_millis(5);

struct MemoryIngestHeartbeatWorker {
    stop_tx: SyncSender<()>,
    join_handle: Option<JoinHandle<()>>,
    stop_requested: bool,
}

impl MemoryIngestHeartbeatWorker {
    fn start(client: Arc<Mutex<MasterClient>>) -> Result<Self> {
        let (stop_tx, stop_rx) = sync_channel(1);
        let join_handle = thread::Builder::new()
            .name("zippy-server-ingest-heartbeat".to_string())
            .spawn(move || memory_ingest_heartbeat_loop(client, stop_rx))
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("spawn memory ingest heartbeat worker error=[{error}]"),
                )
                .with_retryable(true)
            })?;
        Ok(Self {
            stop_tx,
            join_handle: Some(join_handle),
            stop_requested: false,
        })
    }

    fn stop_until(&mut self, deadline: Instant) -> zippy_core::Result<()> {
        if !self.stop_requested {
            match self.stop_tx.try_send(()) {
                Ok(()) | Err(TrySendError::Full(())) | Err(TrySendError::Disconnected(())) => {}
            }
            self.stop_requested = true;
        }
        let Some(join_handle) = self.join_handle.as_ref() else {
            return Ok(());
        };
        while !join_handle.is_finished() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ZippyError::DeadlineExceeded {
                    reason: "memory ingest heartbeat shutdown deadline expired".to_string(),
                });
            }
            thread::sleep(remaining.min(INGEST_HEARTBEAT_STOP_POLL_INTERVAL));
        }
        self.join_handle
            .take()
            .expect("finished memory ingest heartbeat retains its join handle")
            .join()
            .map_err(|_| ZippyError::InvalidState {
                status: "memory ingest heartbeat worker panicked",
            })
    }
}

impl Drop for MemoryIngestHeartbeatWorker {
    fn drop(&mut self) {
        if !self.stop_requested {
            let _ = self.stop_tx.try_send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

fn memory_ingest_heartbeat_loop(client: Arc<Mutex<MasterClient>>, stop_rx: Receiver<()>) {
    loop {
        match stop_rx.recv_timeout(INGEST_HEARTBEAT_INTERVAL) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let heartbeat = client.lock().unwrap().heartbeat();
                if let Err(error) = heartbeat {
                    tracing::warn!(
                        component = "memory_ingest",
                        event = "memory_ingest_heartbeat_failure",
                        error = %error,
                        "memory ingest publisher heartbeat failed"
                    );
                }
            }
        }
    }
}

/// Process-scoped descriptor publisher retained only while legacy readers need Master discovery.
pub(crate) struct LegacyMasterMemoryIngestCatalog {
    client: Arc<Mutex<MasterClient>>,
    heartbeat: Mutex<MemoryIngestHeartbeatWorker>,
    operation_timeout: Duration,
}

impl LegacyMasterMemoryIngestCatalog {
    pub(crate) fn start(
        endpoint: ControlEndpoint,
        startup_timeout: Duration,
        operation_timeout: Duration,
    ) -> Result<Arc<Self>> {
        if startup_timeout.is_zero() || operation_timeout.is_zero() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "memory ingest Master timeouts must be positive",
            )
            .with_retryable(false));
        }
        let mut client = MasterClient::connect_endpoint(endpoint)
            .map_err(|error| map_master_error("connect memory ingest Master publisher", error))?;
        client
            .set_request_timeout(Some(startup_timeout))
            .map_err(|error| map_master_error("configure memory ingest startup timeout", error))?;
        client
            .register_process(INGEST_PROCESS_APP)
            .map_err(|error| map_master_error("register memory ingest publisher process", error))?;
        let request_timeout = operation_timeout.min(INGEST_HEARTBEAT_REQUEST_TIMEOUT);
        client
            .set_request_timeout(Some(request_timeout))
            .map_err(|error| {
                map_master_error("configure memory ingest operation timeout", error)
            })?;
        let client = Arc::new(Mutex::new(client));
        let heartbeat = match MemoryIngestHeartbeatWorker::start(Arc::clone(&client)) {
            Ok(heartbeat) => heartbeat,
            Err(error) => {
                let _ = client.lock().unwrap().unregister_process();
                return Err(error);
            }
        };
        Ok(Arc::new(Self {
            client,
            heartbeat: Mutex::new(heartbeat),
            operation_timeout,
        }))
    }

    fn source_name(stream_name: &str) -> String {
        format!("{INGEST_SOURCE_PREFIX}{stream_name}")
    }

    fn close_timeout(&self, deadline: Instant) -> zippy_core::Result<Duration> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ZippyError::DeadlineExceeded {
                reason: "memory ingest Master close deadline expired".to_string(),
            });
        }
        Ok(remaining.min(self.operation_timeout))
    }
}

impl MemoryIngestCatalog for LegacyMasterMemoryIngestCatalog {
    fn open_table(
        &self,
        stream_name: &str,
        schema: SchemaRef,
    ) -> zippy_core::Result<MemoryIngestRegistration> {
        let source_name = Self::source_name(stream_name);
        let mut client = self.client.lock().unwrap();
        client.register_stream(
            stream_name,
            schema,
            LEGACY_STREAM_BUFFER_SIZE,
            LEGACY_STREAM_FRAME_SIZE,
        )?;
        client.register_source(
            &source_name,
            "server_memory_ingest",
            stream_name,
            serde_json::json!({}),
        )?;
        match client.get_stream_status(stream_name) {
            Ok(stream) => Ok(MemoryIngestRegistration::new(
                source_name,
                stream.writer_epoch,
            )),
            Err(error) => {
                let _ = client.unregister_source(&source_name);
                Err(error)
            }
        }
    }

    fn publish_active_descriptor(
        &self,
        stream_name: &str,
        descriptor_envelope: &[u8],
    ) -> zippy_core::Result<()> {
        self.client
            .lock()
            .unwrap()
            .publish_segment_descriptor_bytes(stream_name, descriptor_envelope)
    }

    fn close_table(
        &self,
        registration: &MemoryIngestRegistration,
        deadline: Instant,
    ) -> zippy_core::Result<()> {
        let timeout = self.close_timeout(deadline)?;
        let mut client = self.client.lock().unwrap();
        let previous = client.request_timeout();
        client.set_request_timeout(Some(timeout))?;
        let result = client.unregister_source(registration.source_name());
        let restore = client.set_request_timeout(previous);
        result.and(restore)
    }

    fn close(&self, deadline: Instant) -> zippy_core::Result<()> {
        self.heartbeat.lock().unwrap().stop_until(deadline)?;
        let timeout = self.close_timeout(deadline)?;
        let mut client = self.client.lock().unwrap();
        if client.process_id().is_none() {
            return Ok(());
        }
        client.set_request_timeout(Some(timeout))?;
        client.unregister_process()
    }
}

fn map_master_error(stage: &str, error: ZippyError) -> ServerError {
    let code = match &error {
        ZippyError::InvalidConfig { .. } => ErrorCode::InvalidArgument,
        ZippyError::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
        ZippyError::NotFound { .. } => ErrorCode::NotFound,
        ZippyError::Conflict { .. } => ErrorCode::Conflict,
        ZippyError::DataCorruption { .. } => ErrorCode::DataCorruption,
        ZippyError::SchemaMismatch { .. } => ErrorCode::SchemaMismatch,
        ZippyError::Cancelled { .. } => ErrorCode::Cancelled,
        ZippyError::DeadlineExceeded { .. } => ErrorCode::DeadlineExceeded,
        ZippyError::MasterShutdownRequested { .. }
        | ZippyError::InvalidState { .. }
        | ZippyError::Io { .. }
        | ZippyError::ChannelSend
        | ZippyError::ChannelReceive
        | ZippyError::QueueClosed => ErrorCode::Unavailable,
        ZippyError::LateData { .. } => ErrorCode::Internal,
    };
    ServerError::new(code, format!("{stage}: {error}")).with_retryable(matches!(
        code,
        ErrorCode::ResourceExhausted | ErrorCode::Unavailable | ErrorCode::DeadlineExceeded
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;

    use zippy_core::{ControlRequest, ControlResponse};

    use super::*;

    #[test]
    fn publisher_heartbeats_until_bounded_close_unregisters_process() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("memory-ingest-heartbeat.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let (heartbeat_tx, heartbeat_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || loop {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request_line = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut request_line)
                .unwrap();
            let request = serde_json::from_str::<ControlRequest>(request_line.trim_end()).unwrap();
            let (response, complete) = match request {
                ControlRequest::RegisterProcess(_) => (
                    ControlResponse::ProcessRegistered {
                        process_id: "memory-ingest-test-process".to_string(),
                        process_token: "memory-ingest-test-token".to_string(),
                    },
                    false,
                ),
                ControlRequest::Heartbeat(request) => {
                    let _ = heartbeat_tx.try_send(());
                    (
                        ControlResponse::HeartbeatAccepted {
                            process_id: request.process_id,
                        },
                        false,
                    )
                }
                ControlRequest::UnregisterProcess(request) => (
                    ControlResponse::ProcessUnregistered {
                        process_id: request.process_id,
                    },
                    true,
                ),
                request => panic!("unexpected control request: {request:?}"),
            };
            serde_json::to_writer(&mut stream, &response).unwrap();
            stream.write_all(b"\n").unwrap();
            if complete {
                break;
            }
        });

        let catalog = LegacyMasterMemoryIngestCatalog::start(
            ControlEndpoint::Unix(socket_path),
            Duration::from_secs(1),
            Duration::from_secs(2),
        )
        .unwrap();
        heartbeat_rx.recv_timeout(Duration::from_secs(3)).unwrap();

        catalog
            .close(Instant::now() + Duration::from_secs(2))
            .unwrap();
        server.join().unwrap();
    }
}
