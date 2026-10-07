//! Server-owned lifecycle for the native storage and query runtime.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use serde::{Deserialize, Serialize};
#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::iterator::Signals;
use zippy_catalog::{SqliteCatalogConfig, SqliteCatalogRuntimeSnapshotPinStatus};
use zippy_core::bus_protocol::GetConfigRequest;
use zippy_core::{
    base_version, current_log_snapshot, resolve_control_endpoint,
    send_control_line_request_with_timeout, ControlEndpoint, ControlRequest, ControlResponse,
    OwnedDataDirectory, Result as CoreResult, ZippyConfig, ZippyError,
};
use zippy_ingest::NativeIngestStatus;
use zippy_master::daemon::{prepare_master_runtime, MasterDaemonConfig};
use zippy_master::process_runtime::ProcessRuntimeRegistry;
use zippy_master::server::MasterServer;
use zippy_protocol::ingest::FlightPutLimits;
use zippy_protocol::runtime::{FlightServerRuntime, FlightShutdownOutcome, FlightTlsStatus};
use zippy_protocol::server::{FlightServiceConfig, FlightServiceStatus, ZippyFlightService};
use zippy_protocol::sql::FlightSqlMetadataLimits;
use zippy_storage::{PersistControllerConfig, MAX_PERSIST_PARALLEL_WRITERS};
use zippy_types::{ErrorCode, ServerError};

use crate::flight_query::NativeFlightQueryFrontend;
use crate::flight_security::load_flight_security;
use crate::{
    NativeStorageRuntime, NativeStorageRuntimeConfig, NativeStorageShutdownOutcome,
    NativeStorageStartOutcome, ServerHealth,
};

const DEFAULT_SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(50);
const SERVER_LIFECYCLE_SCHEMA_VERSION: u8 = 1;
const SERVER_READY_SCHEMA_VERSION: u8 = 11;

/// Runs the server-owned native runtime until shutdown or dynamic readiness loss.
///
/// On Unix, this function installs and owns the `SIGINT` and `SIGTERM` subscriptions before any
/// listener is started. After the first signal it closes service admission and stops Flight
/// before master. A running component that loses readiness triggers the same ordered shutdown and
/// returns an error so detached launchers cannot keep reporting a stale ready record.
///
/// # Errors
///
/// Returns an error when signal installation, runtime startup, signal waiting, or shutdown fails.
pub fn run_server_daemon(config: ServerRuntimeConfig) -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("install server signal handlers: {error}"),
            )
        })?;
        run_server_daemon_with_waiter(config, move |runtime| {
            wait_for_shutdown_or_runtime_failure(runtime, || {
                signals.pending().next().map(signal_name)
            })
        })
    }

    #[cfg(not(unix))]
    {
        let (sender, receiver) = mpsc::channel::<()>();
        run_server_daemon_with_waiter(config, move |runtime| {
            let _sender = sender;
            wait_for_shutdown_or_runtime_failure(runtime, || {
                receiver.try_recv().ok().map(|()| "external")
            })
        })
    }
}

fn wait_for_shutdown_or_runtime_failure<F>(
    runtime: &ServerRuntime,
    mut poll_shutdown: F,
) -> Result<&'static str, ServerError>
where
    F: FnMut() -> Option<&'static str>,
{
    loop {
        if let Some(error) = runtime_failure(runtime) {
            return Err(error);
        }
        if let Some(signal) = poll_shutdown() {
            if let Some(error) = runtime_failure(runtime) {
                return Err(error);
            }
            return Ok(signal);
        }
        thread::sleep(DAEMON_HEALTH_POLL_INTERVAL);
    }
}

fn runtime_failure(runtime: &ServerRuntime) -> Option<ServerError> {
    if let Some(error) = runtime.terminal_failure() {
        return Some(error);
    }
    let health = runtime.health();
    if health.readiness.is_ready() {
        return None;
    }
    Some(ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "server runtime lost readiness facts=[{:?}]",
            health.readiness
        ),
    ))
}

fn run_server_daemon_with_waiter<F>(
    config: ServerRuntimeConfig,
    wait_for_shutdown: F,
) -> Result<(), ServerError>
where
    F: FnOnce(&mut ServerRuntime) -> Result<&'static str, ServerError>,
{
    let launcher_state = validate_ready_configuration(&config)?;
    let ready_file = config.ready_file.clone();
    let launch_nonce = config.launch_nonce.clone();
    let mut runtime = ServerRuntime::start(config)?;
    if let Some(path) = ready_file.as_deref() {
        if let Err(ready_error) = publish_ready_record(
            &runtime,
            path,
            launch_nonce.as_deref(),
            launcher_state.as_ref(),
        ) {
            runtime.begin_shutdown();
            let outcome = runtime.shutdown();
            return Err(combine_daemon_failure(
                ready_error,
                None,
                shutdown_outcome_error(&outcome),
            ));
        }
    }
    let shutdown_signal = match wait_for_shutdown(&mut runtime) {
        Ok(signal) => signal,
        Err(wait_error) => {
            let state_transition_error = transition_daemon_state(
                ready_file.as_deref(),
                launch_nonce.as_deref(),
                launcher_state.as_ref(),
                "failed",
                Some(wait_error.code()),
            );
            runtime.begin_shutdown();
            let outcome = runtime.shutdown();
            return Err(combine_daemon_failure(
                wait_error,
                state_transition_error,
                shutdown_outcome_error(&outcome),
            ));
        }
    };

    tracing::info!(
        component = "server",
        event = "server_shutdown_requested",
        status = "stopping",
        signal = shutdown_signal,
        control_endpoint = runtime.control_endpoint().display_string(),
        "received server shutdown signal"
    );

    let stopping_transition_error = transition_daemon_state(
        ready_file.as_deref(),
        launch_nonce.as_deref(),
        launcher_state.as_ref(),
        "stopping",
        None,
    );
    runtime.begin_shutdown();
    let outcome = runtime.shutdown();
    let shutdown_error = shutdown_outcome_error(&outcome);
    if let Some(terminal_error) = runtime.terminal_failure() {
        let failed_transition_error = transition_daemon_state(
            ready_file.as_deref(),
            launch_nonce.as_deref(),
            launcher_state.as_ref(),
            "failed",
            Some(terminal_error.code()),
        );
        return Err(combine_daemon_failure(
            terminal_error,
            combine_optional_errors(stopping_transition_error, failed_transition_error),
            shutdown_error,
        ));
    }
    if let Some(shutdown_error) = shutdown_error {
        let failed_transition_error = transition_daemon_state(
            ready_file.as_deref(),
            launch_nonce.as_deref(),
            launcher_state.as_ref(),
            "failed",
            Some(shutdown_error.code()),
        );
        return Err(combine_daemon_failure(
            shutdown_error,
            combine_optional_errors(stopping_transition_error, failed_transition_error),
            None,
        ));
    }
    match stopping_transition_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn invalidate_ready_record(path: &Path) -> Result<(), ServerError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ready_io_error("invalidate", path, error)),
    }
}

fn transition_daemon_state(
    ready_file: Option<&Path>,
    launch_nonce: Option<&str>,
    launcher_state: Option<&ServerLauncherState>,
    status: &'static str,
    error_code: Option<ErrorCode>,
) -> Option<ServerError> {
    let ready_file = ready_file?;
    let transition =
        publish_daemon_transition(ready_file, launch_nonce, launcher_state, status, error_code);
    match transition {
        Ok(()) => None,
        Err(transition_error) => match invalidate_ready_record(ready_file) {
            Ok(()) => Some(transition_error),
            Err(invalidation_error) => Some(append_secondary_error(
                transition_error,
                &invalidation_error,
            )),
        },
    }
}

fn publish_daemon_transition(
    ready_file: &Path,
    launch_nonce: Option<&str>,
    launcher_state: Option<&ServerLauncherState>,
    status: &'static str,
    error_code: Option<ErrorCode>,
) -> Result<(), ServerError> {
    if !matches!(status, "stopping" | "failed") || (status == "failed") != error_code.is_some() {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server daemon transition requires stopping or failed state with a matching code",
        ));
    }
    let launch_nonce = launch_nonce
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                "server daemon transition requires a non-empty launch nonce",
            )
        })?;
    let launcher_state = launcher_state.ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "server daemon transition requires launcher state",
        )
    })?;
    let record = ServerTransitionRecord {
        schema_version: SERVER_LIFECYCLE_SCHEMA_VERSION,
        status,
        launch_nonce,
        pid: std::process::id(),
        error_code: error_code.map(ErrorCode::as_str),
        launcher: launcher_state,
    };
    let payload = serde_json::to_vec_pretty(&record).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("serialize server daemon transition: {error}"),
        )
    })?;
    write_owner_only_transition(ready_file, status, &payload)
}

fn write_owner_only_transition(
    ready_file: &Path,
    status: &str,
    payload: &[u8],
) -> Result<(), ServerError> {
    let parent = ready_file.parent().ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "server ready file has no parent path=[{}]",
                ready_file.display()
            ),
        )
    })?;
    let temporary = ready_file.with_extension(format!("{}.{}.tmp", std::process::id(), status));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let write_result = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(|error| ready_io_error("create transition file", &temporary, error))?;
        file.write_all(payload)
            .map_err(|error| ready_io_error("write transition file", &temporary, error))?;
        file.sync_all()
            .map_err(|error| ready_io_error("sync transition file", &temporary, error))?;
        fs::rename(&temporary, ready_file)
            .map_err(|error| ready_io_error("publish transition", ready_file, error))?;
        #[cfg(unix)]
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ready_io_error("sync transition parent", parent, error))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn combine_daemon_failure(
    primary: ServerError,
    state_transition: Option<ServerError>,
    shutdown: Option<ServerError>,
) -> ServerError {
    let mut combined = primary;
    if let Some(state_error) = state_transition {
        combined = append_secondary_error(combined, &state_error);
    }
    if let Some(shutdown_error) = shutdown {
        combined = append_secondary_error(combined, &shutdown_error);
    }
    combined
}

fn combine_optional_errors(
    primary: Option<ServerError>,
    secondary: Option<ServerError>,
) -> Option<ServerError> {
    match (primary, secondary) {
        (Some(primary), Some(secondary)) => Some(append_secondary_error(primary, &secondary)),
        (Some(error), None) | (None, Some(error)) => Some(error),
        (None, None) => None,
    }
}

fn append_secondary_error(primary: ServerError, secondary: &ServerError) -> ServerError {
    let request_id = primary.request_id();
    let query_id = primary.query_id();
    let table_id = primary.table_id();
    let details = primary.details().cloned();
    let mut combined = ServerError::new(
        primary.code(),
        format!("{primary}; secondary daemon failure=[{secondary}]"),
    )
    .with_retryable(primary.retryable());
    if let Some(request_id) = request_id {
        combined = combined.with_request_id(request_id);
    }
    if let Some(query_id) = query_id {
        combined = combined.with_query_id(query_id);
    }
    if let Some(table_id) = table_id {
        combined = combined.with_table_id(table_id);
    }
    if let Some(details) = details {
        if let Ok(with_details) = combined.clone().with_details(details) {
            combined = with_details;
        }
    }
    combined
}

fn shutdown_outcome_error(outcome: &ServerShutdownOutcome) -> Option<ServerError> {
    if outcome.is_clean() {
        return None;
    }
    let native_error = outcome
        .native_storage_outcome
        .as_ref()
        .and_then(NativeStorageShutdownOutcome::error);
    let error = outcome
        .flight_error
        .as_ref()
        .or(outcome.master_error.as_ref())
        .or(native_error);
    let timed_out = !outcome.timed_out_processes.is_empty()
        || error.is_some_and(|error| error.code() == ErrorCode::DeadlineExceeded)
        || matches!(
            outcome.native_storage_outcome,
            Some(NativeStorageShutdownOutcome::TimedOut)
        );
    let code = if timed_out {
        ErrorCode::DeadlineExceeded
    } else {
        error.map_or(ErrorCode::Unavailable, ServerError::code)
    };
    Some(ServerError::new(
        code,
        format!("server shutdown incomplete outcome=[{outcome:?}]"),
    ))
}

#[cfg(unix)]
fn signal_name(signal: i32) -> &'static str {
    match signal {
        SIGINT => "SIGINT",
        SIGTERM => "SIGTERM",
        _ => "UNKNOWN",
    }
}

/// Configuration for one server-owned native runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    master: MasterDaemonConfig,
    startup_timeout: Duration,
    ready_file: Option<PathBuf>,
    launch_nonce: Option<String>,
}

impl ServerRuntimeConfig {
    /// Builds a runtime configuration around the existing master configuration.
    #[must_use]
    pub fn new(master: MasterDaemonConfig) -> Self {
        Self {
            master,
            startup_timeout: DEFAULT_SERVER_STARTUP_TIMEOUT,
            ready_file: None,
            launch_nonce: None,
        }
    }

    /// Overrides the bounded listener and dependency-probe startup timeout.
    #[must_use]
    pub fn with_startup_timeout(mut self, startup_timeout: Duration) -> Self {
        self.startup_timeout = startup_timeout;
        self
    }

    /// Promotes an existing typed `starting` record after every server component is ready.
    #[must_use]
    pub fn with_ready_file(
        mut self,
        ready_file: impl AsRef<Path>,
        launch_nonce: impl Into<String>,
    ) -> Self {
        self.ready_file = Some(ready_file.as_ref().to_path_buf());
        self.launch_nonce = Some(launch_nonce.into());
        self
    }

    /// Returns the embedded master configuration.
    #[must_use]
    pub fn master(&self) -> &MasterDaemonConfig {
        &self.master
    }

    /// Returns the startup timeout.
    #[must_use]
    pub const fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }
}

#[derive(Serialize)]
struct ServerReadyFlight<'a> {
    enabled: bool,
    endpoint: Option<&'a str>,
    protocol_version: u16,
    tls_enabled: bool,
    authentication_required: bool,
    administrator_authentication_enabled: bool,
    writer_authentication_enabled: bool,
    query_control_enabled: bool,
    flight_sql_metadata_enabled: bool,
    flight_ingest_enabled: bool,
    metadata_max_rows: usize,
    metadata_max_bytes: usize,
    ingest_max_concurrent_puts: usize,
    ingest_max_batches_per_put: usize,
    ingest_max_rows_per_batch: u64,
    ingest_max_rows_per_put: u64,
    ingest_max_batch_bytes: usize,
    ingest_max_bytes_per_put: usize,
    certificate_sha256: Option<&'a str>,
    client_certificate_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerLauncherProcessIdentity {
    boot_id: String,
    start_ticks: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerLauncherWebUi {
    managed: bool,
    status: String,
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerLauncherState {
    process_identity: ServerLauncherProcessIdentity,
    started_at: String,
    requested_control_endpoint: String,
    config_path: Option<String>,
    webui: ServerLauncherWebUi,
    bootstrap_log: String,
    state_file: String,
    lock_file: String,
    outcome_file: String,
    run_dir: String,
    log_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerStartingRecord {
    schema_version: u8,
    status: String,
    launch_nonce: String,
    pid: u32,
    launcher: ServerLauncherState,
}

#[derive(Serialize)]
struct ServerReadyRecord<'a> {
    schema_version: u8,
    status: &'static str,
    launch_nonce: &'a str,
    pid: u32,
    version: String,
    ready_at_unix_ns: u128,
    control_endpoint: String,
    flight: ServerReadyFlight<'a>,
    mode: ServerMode,
    health: ServerRuntimeHealth,
    log_file: Option<String>,
    launcher: Option<&'a ServerLauncherState>,
}

#[derive(Serialize)]
struct ServerTransitionRecord<'a> {
    schema_version: u8,
    status: &'static str,
    launch_nonce: &'a str,
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'static str>,
    launcher: &'a ServerLauncherState,
}

fn validate_ready_configuration(
    config: &ServerRuntimeConfig,
) -> Result<Option<ServerLauncherState>, ServerError> {
    match (&config.ready_file, &config.launch_nonce) {
        (None, None) => Ok(None),
        (Some(ready_file), Some(nonce)) if !nonce.trim().is_empty() => {
            load_launcher_state(ready_file, nonce).map(Some)
        }
        (Some(_), Some(_)) => Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server ready file requires a non-empty launch nonce",
        )),
        _ => Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server ready file and launch nonce must be set together",
        )),
    }
}

fn load_launcher_state(
    ready_file: &Path,
    launch_nonce: &str,
) -> Result<ServerLauncherState, ServerError> {
    const MAX_STARTING_RECORD_BYTES: u64 = 64 * 1024;
    let metadata = fs::metadata(ready_file)
        .map_err(|error| ready_io_error("read starting metadata", ready_file, error))?;
    if metadata.len() > MAX_STARTING_RECORD_BYTES {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server starting record exceeds the size limit",
        ));
    }
    let payload = fs::read(ready_file)
        .map_err(|error| ready_io_error("read starting record", ready_file, error))?;
    let record: ServerStartingRecord = serde_json::from_slice(&payload).map_err(|_| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "server starting record does not match the typed schema",
        )
    })?;
    if record.schema_version != SERVER_LIFECYCLE_SCHEMA_VERSION
        || record.status != "starting"
        || record.launch_nonce != launch_nonce
        || record.pid != std::process::id()
        || !valid_launcher_state(&record.launcher, ready_file)
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server starting record does not match this process generation",
        ));
    }
    Ok(record.launcher)
}

fn valid_launcher_state(state: &ServerLauncherState, ready_file: &Path) -> bool {
    let Some(run_dir) = ready_file.parent() else {
        return false;
    };
    let log_dir = Path::new(&state.log_dir);
    let webui_url = state.webui.url.as_str();
    !state.process_identity.boot_id.is_empty()
        && !state.process_identity.start_ticks.is_empty()
        && state
            .process_identity
            .start_ticks
            .bytes()
            .all(|byte| byte.is_ascii_digit())
        && state.started_at.contains('T')
        && state.started_at.ends_with("+00:00")
        && resolve_control_endpoint(&state.requested_control_endpoint).is_ok()
        && state
            .config_path
            .as_ref()
            .is_none_or(|path| !path.is_empty() && Path::new(path).is_absolute())
        && !state.webui.managed
        && state.webui.status == "unknown"
        && (webui_url.starts_with("http://") || webui_url.starts_with("https://"))
        && !webui_url.contains(['@', '?', '#'])
        && Path::new(&state.state_file) == ready_file
        && Path::new(&state.run_dir) == run_dir
        && Path::new(&state.lock_file).parent() == Some(run_dir)
        && Path::new(&state.outcome_file).parent() == Some(run_dir)
        && log_dir.is_absolute()
        && Path::new(&state.bootstrap_log).parent() == Some(log_dir)
}

fn publish_ready_record(
    runtime: &ServerRuntime,
    ready_file: &Path,
    launch_nonce: Option<&str>,
    launcher_state: Option<&ServerLauncherState>,
) -> Result<(), ServerError> {
    let launch_nonce = launch_nonce
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                "server ready file requires a non-empty launch nonce",
            )
        })?;
    if launcher_state.is_some_and(|state| {
        resolve_control_endpoint(&state.requested_control_endpoint)
            .map(|endpoint| endpoint != *runtime.control_endpoint())
            .unwrap_or(true)
    }) {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server launcher endpoint does not match the running server",
        ));
    }
    let parent = ready_file.parent().ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "server ready file has no parent path=[{}]",
                ready_file.display()
            ),
        )
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ready_io_error("create parent", ready_file, error))?;

    let ready_at_unix_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("read system time for server ready record: {error}"),
            )
        })?
        .as_nanos();
    let log_file = current_log_snapshot()
        .and_then(|snapshot| snapshot.file_path)
        .map(|path| path.display().to_string());
    let flight_tls = runtime
        .flight
        .as_ref()
        .and_then(FlightServerRuntime::tls_status);
    let flight_status = runtime.flight_status();
    let flight_enabled = runtime.flight_endpoint().is_some();
    let flight_config = &runtime.runtime_config().flight;
    let record = ServerReadyRecord {
        schema_version: SERVER_READY_SCHEMA_VERSION,
        status: "ready",
        launch_nonce,
        pid: std::process::id(),
        version: base_version().to_string(),
        ready_at_unix_ns,
        control_endpoint: runtime.control_endpoint().display_string(),
        flight: ServerReadyFlight {
            enabled: flight_enabled,
            endpoint: runtime.flight_endpoint(),
            protocol_version: runtime.runtime_config().flight.protocol_version,
            tls_enabled: flight_tls.is_some(),
            authentication_required: flight_status
                .is_some_and(|status| status.authentication_required),
            administrator_authentication_enabled: flight_enabled
                && flight_config.auth.enabled
                && (flight_config.auth.admin_token_file.is_some()
                    || flight_config.auth.admin_bootstrap_token().is_some()),
            writer_authentication_enabled: flight_enabled
                && flight_config.auth.enabled
                && (flight_config.auth.writer_token_file.is_some()
                    || flight_config.auth.writer_bootstrap_token().is_some()),
            query_control_enabled: flight_enabled,
            flight_sql_metadata_enabled: flight_enabled,
            flight_ingest_enabled: flight_enabled && flight_config.ingest.enabled,
            metadata_max_rows: if flight_enabled {
                flight_config.metadata.max_rows
            } else {
                0
            },
            metadata_max_bytes: if flight_enabled {
                flight_config.metadata.max_bytes
            } else {
                0
            },
            ingest_max_concurrent_puts: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_concurrent_puts
            } else {
                0
            },
            ingest_max_batches_per_put: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_batches_per_put
            } else {
                0
            },
            ingest_max_rows_per_batch: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_rows_per_batch
            } else {
                0
            },
            ingest_max_rows_per_put: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_rows_per_put
            } else {
                0
            },
            ingest_max_batch_bytes: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_batch_bytes
            } else {
                0
            },
            ingest_max_bytes_per_put: if flight_enabled && flight_config.ingest.enabled {
                flight_config.ingest.max_bytes_per_put
            } else {
                0
            },
            certificate_sha256: flight_tls.map(|status| status.certificate_sha256()),
            client_certificate_required: flight_tls
                .is_some_and(|status| status.client_certificate_required()),
        },
        mode: runtime.mode(),
        health: runtime.health(),
        log_file,
        launcher: launcher_state,
    };
    let payload = serde_json::to_vec_pretty(&record).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("serialize server ready record: {error}"),
        )
    })?;

    let temporary = ready_file.with_extension(format!("{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let write_result = (|| {
        let mut file = options
            .open(&temporary)
            .map_err(|error| ready_io_error("create temporary file", &temporary, error))?;
        file.write_all(&payload)
            .map_err(|error| ready_io_error("write temporary file", &temporary, error))?;
        file.sync_all()
            .map_err(|error| ready_io_error("sync temporary file", &temporary, error))?;
        fs::rename(&temporary, ready_file)
            .map_err(|error| ready_io_error("publish", ready_file, error))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

fn ready_io_error(stage: &str, path: &Path, error: std::io::Error) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "{stage} server ready record path=[{}] error=[{error}]",
            path.display()
        ),
    )
}

/// Active control-only or Flight-enabled server mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerMode {
    /// Only the control listener is enabled.
    ControlOnly,
    /// Control, Catalog and native Flight queries are enabled in one process.
    CatalogAndQuery,
}

/// Listener and dependency facts used to decide whether admission is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerReadiness {
    /// Whether the runtime currently permits new service work.
    pub admission_open: bool,
    /// Whether the master listener crossed its explicit bind barrier.
    pub master_listener_ready: bool,
    /// Whether the effective configuration enables Arrow Flight.
    pub flight_configured: bool,
    /// Whether Flight successfully crossed its listener bind barrier.
    pub flight_listener_ready: bool,
    /// Whether the versioned Flight service completed protocol construction.
    pub flight_protocol_ready: bool,
    /// Whether the native SQLite Catalog opened and passed integrity checks.
    pub native_catalog_ready: bool,
    /// Whether every current-snapshot managed file passed physical verification.
    pub native_current_snapshot_files_ready: bool,
    /// Whether final revision-stable physical-to-Catalog and reverse verification completed.
    pub storage_reconciliation_complete: bool,
    /// Whether every durable Catalog commit intent was replayed before readiness.
    pub commit_recovery_complete: bool,
}

impl ServerReadiness {
    /// Returns whether every configured service dependency is ready for admission.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        self.admission_open
            && self.master_listener_ready
            && self.native_catalog_ready
            && self.native_current_snapshot_files_ready
            && self.storage_reconciliation_complete
            && self.commit_recovery_complete
            && (!self.flight_configured
                || (self.flight_listener_ready && self.flight_protocol_ready))
    }
}

/// Unified programmatic health view of the owning runtime and active services.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerRuntimeHealth {
    /// Active deployment mode.
    pub mode: ServerMode,
    /// Concrete listener and dependency readiness facts.
    pub readiness: ServerReadiness,
    /// Capability-aware catalog and query health.
    pub services: ServerHealth,
    /// Process-local snapshot pin admission state while native storage is active.
    pub catalog_runtime_snapshot_pins: Option<SqliteCatalogRuntimeSnapshotPinStatus>,
    /// Internal persisted-only native query canary state, when explicitly enabled.
    #[serde(default)]
    pub native_query_canary: Option<crate::NativeQueryCanaryStatus>,
}

/// Complete result of a server-owned shutdown attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerShutdownOutcome {
    /// Runtime process identifiers that did not leave before the master timeout.
    pub timed_out_processes: Vec<String>,
    /// Flight cleanup failure after closing query and ticket admission.
    pub flight_error: Option<ServerError>,
    /// Structured Flight cleanup facts when the listener was configured.
    pub flight_outcome: Option<FlightShutdownOutcome>,
    /// Control listener failure observed while joining.
    pub master_error: Option<ServerError>,
    /// Ordered native coordinator and Catalog cleanup state, when configured.
    pub native_storage_outcome: Option<NativeStorageShutdownOutcome>,
}

impl ServerShutdownOutcome {
    /// Returns whether every admitted service stopped without losing cleanup ownership.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.timed_out_processes.is_empty()
            && self.flight_error.is_none()
            && self.master_error.is_none()
            && self
                .native_storage_outcome
                .as_ref()
                .is_none_or(NativeStorageShutdownOutcome::is_complete)
    }
}

struct MasterAdapters {
    runtime: Arc<ProcessRuntimeRegistry>,
}

struct MasterListenerRuntime {
    completed: mpsc::Receiver<CoreResult<()>>,
    completion_result: Option<CoreResult<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MasterListenerRuntime {
    fn is_finished(&self) -> bool {
        self.join_handle
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
    }
}

/// Owning single-process runtime for control, native storage and Flight.
pub struct ServerRuntime {
    control_endpoint: ControlEndpoint,
    runtime_config: ZippyConfig,
    shutdown_timeout: Duration,
    master: MasterServer,
    master_listener: Option<MasterListenerRuntime>,
    flight: Option<FlightServerRuntime>,
    flight_endpoint: Option<String>,
    services: MasterAdapters,
    readiness: ServerReadiness,
    shutdown_outcome: Option<ServerShutdownOutcome>,
    native_storage: Option<NativeStorageRuntime>,
    data_directory: Option<Arc<OwnedDataDirectory>>,
}

impl ServerRuntime {
    /// Starts all configured listeners, verifies dependencies, and opens service admission.
    pub fn start(config: ServerRuntimeConfig) -> Result<Self, ServerError> {
        if config.startup_timeout.is_zero() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "server startup timeout must be positive",
            ));
        }
        let startup_deadline = deadline_after(
            config.startup_timeout,
            "server startup timeout exceeds monotonic clock range",
        )?;

        let prepared = prepare_master_runtime(config.master)
            .map_err(|error| map_core_error("prepare master runtime", error))?;
        let (control_endpoint, runtime_config, master, shutdown_timeout, data_directory) =
            prepared.into_parts();
        reject_ephemeral_master_endpoint(&control_endpoint)?;

        let mut native_storage = Some(start_native_storage(
            &runtime_config,
            &data_directory,
            startup_deadline,
        )?);
        if runtime_config.flight.ingest.enabled {
            let wal_root = data_directory.directory().root().join("wal").join("ingest");
            let start_result = native_storage
                .as_mut()
                .ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "Flight ingest requires the native storage runtime",
                    )
                    .with_retryable(false)
                })
                .and_then(|storage| storage.start_native_ingest(wal_root));
            if let Err(error) = start_result {
                let cleanup = native_storage
                    .as_mut()
                    .map(|storage| storage.shutdown_until(startup_deadline));
                let error = match cleanup {
                    Some(NativeStorageShutdownOutcome::Failed(cleanup_error)) => {
                        append_secondary_error(error, &cleanup_error)
                    }
                    Some(NativeStorageShutdownOutcome::TimedOut) => append_secondary_error(
                        error,
                        &ServerError::new(
                            ErrorCode::DeadlineExceeded,
                            "native storage cleanup timed out after ingest startup failure",
                        ),
                    ),
                    Some(NativeStorageShutdownOutcome::Complete) | None => error,
                };
                return Err(error);
            }
        }
        let rollback_context = StartupRollbackContext {
            master: &master,
            shutdown_timeout,
        };
        if let Err(error) = remaining_before(startup_deadline, "start master listener") {
            let mut no_listener = None;
            let rollback = rollback_startup(
                rollback_context,
                &mut no_listener,
                &mut native_storage,
                None,
            );
            return Err(with_startup_rollback(error, rollback));
        }
        let mut master_listener = match start_master_listener(
            &master,
            &control_endpoint,
            startup_deadline,
            Arc::clone(&data_directory),
        ) {
            Ok(listener) => Some(listener),
            Err(error) => {
                let mut no_listener = None;
                let rollback = rollback_startup(
                    rollback_context,
                    &mut no_listener,
                    &mut native_storage,
                    None,
                );
                return Err(with_startup_rollback(error, rollback));
            }
        };
        let probe_timeout = match remaining_before(startup_deadline, "probe master dependency") {
            Ok(timeout) => timeout,
            Err(error) => {
                let rollback = rollback_startup(
                    rollback_context,
                    &mut master_listener,
                    &mut native_storage,
                    None,
                );
                return Err(with_startup_rollback(error, rollback));
            }
        };
        let dependency_probe = probe_master_dependency(&control_endpoint, probe_timeout);
        if let Err(error) = dependency_probe {
            let rollback = rollback_startup(
                rollback_context,
                &mut master_listener,
                &mut native_storage,
                None,
            );
            return Err(with_startup_rollback(
                map_core_error("probe master dependency", error),
                rollback,
            ));
        }
        if master_listener
            .as_ref()
            .is_some_and(MasterListenerRuntime::is_finished)
        {
            let rollback = rollback_startup(
                rollback_context,
                &mut master_listener,
                &mut native_storage,
                None,
            );
            return Err(with_startup_rollback(
                ServerError::new(
                    ErrorCode::Unavailable,
                    "master listener exited during server startup",
                ),
                rollback,
            ));
        }

        let services = MasterAdapters {
            runtime: Arc::new(master.process_runtime_registry_inactive()),
        };

        if let Err(error) = services.runtime.activate() {
            let rollback = rollback_startup(
                rollback_context,
                &mut master_listener,
                &mut native_storage,
                Some(&services),
            );
            return Err(with_startup_rollback(
                with_stage("activate control registry", error),
                rollback,
            ));
        }
        let mut flight = match start_flight_if_enabled(
            &runtime_config,
            native_storage.as_ref(),
            startup_deadline,
        ) {
            Ok(flight) => flight,
            Err(error) => {
                let rollback = rollback_startup(
                    rollback_context,
                    &mut master_listener,
                    &mut native_storage,
                    Some(&services),
                );
                return Err(with_startup_rollback(
                    with_stage("start Flight listener", error),
                    rollback,
                ));
            }
        };
        let flight_configured = flight.is_some();
        let flight_endpoint = flight.as_ref().map(|flight| flight.endpoint().to_string());
        if let Err(error) = remaining_before(startup_deadline, "publish server readiness") {
            let flight_error = stop_flight_until(&mut flight, startup_deadline).err();
            let rollback = rollback_startup(
                rollback_context,
                &mut master_listener,
                &mut native_storage,
                Some(&services),
            );
            return Err(with_startup_rollback(
                combine_optional_errors(Some(error), flight_error)
                    .expect("readiness timeout always supplies one error"),
                rollback,
            ));
        }

        let readiness = ServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            flight_configured,
            flight_listener_ready: flight_configured,
            flight_protocol_ready: flight_configured,
            native_catalog_ready: true,
            native_current_snapshot_files_ready: true,
            storage_reconciliation_complete: true,
            commit_recovery_complete: true,
        };
        tracing::info!(
            component = "server",
            event = "server_ready",
            status = "ready",
            control_endpoint = control_endpoint.display_string(),
            flight_endpoint = flight_endpoint.as_deref().unwrap_or("disabled"),
            mode = if flight_configured {
                "catalog_and_query"
            } else {
                "control_only"
            },
            "zippy server runtime is ready"
        );
        Ok(Self {
            control_endpoint,
            runtime_config,
            shutdown_timeout,
            master,
            master_listener,
            flight,
            flight_endpoint,
            services,
            readiness,
            shutdown_outcome: None,
            native_storage,
            data_directory: Some(data_directory),
        })
    }

    /// Returns the effective master control endpoint.
    #[must_use]
    pub fn control_endpoint(&self) -> &ControlEndpoint {
        &self.control_endpoint
    }

    /// Returns the effective Arrow Flight listener endpoint when configured.
    #[must_use]
    pub fn flight_endpoint(&self) -> Option<&str> {
        self.flight_endpoint.as_deref()
    }

    /// Returns bounded Flight ticket and transport counters when configured.
    #[must_use]
    pub fn flight_status(&self) -> Option<FlightServiceStatus> {
        self.flight.as_ref().map(FlightServerRuntime::status)
    }

    /// Returns producer-fenced native ingest lifecycle and WAL counters when configured.
    #[must_use]
    pub fn native_ingest_status(&self) -> Option<NativeIngestStatus> {
        self.native_storage
            .as_ref()
            .and_then(NativeStorageRuntime::native_ingest_status)
    }

    /// Returns non-secret TLS diagnostics for the Flight listener when configured.
    #[must_use]
    pub fn flight_tls_status(&self) -> Option<&FlightTlsStatus> {
        self.flight
            .as_ref()
            .and_then(FlightServerRuntime::tls_status)
    }

    /// Returns the validated runtime configuration shared by control and Flight.
    #[must_use]
    pub fn runtime_config(&self) -> &ZippyConfig {
        &self.runtime_config
    }

    /// Returns the process-owned managed data directory while the runtime is active.
    #[must_use]
    pub fn data_directory(&self) -> Option<&OwnedDataDirectory> {
        self.data_directory.as_deref()
    }

    /// Returns the final startup reconciliation evidence while native storage is active.
    #[must_use]
    pub fn native_storage_reconciliation_report(
        &self,
    ) -> Option<&zippy_storage::StartupOrphanReconciliationReport> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::reconciliation_report)
    }

    /// Returns Catalog-to-physical verification evidence while native storage is active.
    #[must_use]
    pub fn native_storage_catalog_file_verification_report(
        &self,
    ) -> Option<&zippy_storage::StartupCatalogFileVerificationReport> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::catalog_file_verification_report)
    }

    /// Returns the active deployment mode.
    #[must_use]
    pub const fn mode(&self) -> ServerMode {
        if self.readiness.flight_configured {
            ServerMode::CatalogAndQuery
        } else {
            ServerMode::ControlOnly
        }
    }

    /// Returns the native Catalog read handle until storage shutdown completes.
    #[must_use]
    pub fn catalog(&self) -> Option<zippy_catalog::SqliteCatalogReadHandle> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::catalog_read_handle)
    }

    /// Returns the internal persisted-only native canary without granting frontend authority.
    #[must_use]
    pub fn native_query_canary(&self) -> Option<crate::NativeQueryCanaryService> {
        self.native_storage
            .as_ref()
            .and_then(NativeStorageRuntime::query_canary)
    }

    /// Returns the server-owned native SQL engine until storage shutdown completes.
    #[must_use]
    pub fn native_query_engine(&self) -> Option<crate::NativeQueryEngine> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::query_engine)
    }

    /// Returns boot-scoped process leases and runtime observations.
    #[must_use]
    pub fn runtime_registry(&self) -> Arc<ProcessRuntimeRegistry> {
        Arc::clone(&self.services.runtime)
    }

    /// Returns concrete listener facts and capability-aware service health.
    #[must_use]
    pub fn health(&self) -> ServerRuntimeHealth {
        let mut readiness = self.readiness;
        readiness.master_listener_ready &= self
            .master_listener
            .as_ref()
            .is_some_and(|listener| !listener.is_finished());
        if readiness.flight_configured {
            let running = self
                .flight
                .as_ref()
                .is_some_and(FlightServerRuntime::is_running);
            readiness.flight_listener_ready &= running;
            readiness.flight_protocol_ready &= running;
        }
        apply_native_storage_runtime_health(
            &mut readiness,
            self.native_storage
                .as_ref()
                .map(NativeStorageRuntime::is_healthy),
        );
        let mut services = ServerHealth::control_only(
            readiness.admission_open,
            self.master.is_running() && readiness.master_listener_ready,
        );
        services.end_to_end_liveness = Some(readiness.is_ready());
        services.set_native_catalog_ready(readiness.is_ready());
        services.set_native_query_ready(
            readiness.is_ready()
                && self
                    .flight
                    .as_ref()
                    .is_some_and(FlightServerRuntime::is_running),
        );
        ServerRuntimeHealth {
            mode: self.mode(),
            readiness,
            services,
            catalog_runtime_snapshot_pins: self
                .native_storage
                .as_ref()
                .and_then(|storage| storage.runtime_snapshot_pin_status().ok()),
            native_query_canary: self
                .native_storage
                .as_ref()
                .and_then(NativeStorageRuntime::query_canary)
                .map(|canary| canary.status()),
        }
    }

    fn terminal_failure(&self) -> Option<ServerError> {
        if self.readiness.admission_open
            && self.readiness.flight_configured
            && !self
                .flight
                .as_ref()
                .is_some_and(FlightServerRuntime::is_running)
        {
            return Some(ServerError::new(
                ErrorCode::Unavailable,
                "Flight listener exited while server admission remained open",
            ));
        }
        self.native_storage
            .as_ref()
            .and_then(NativeStorageRuntime::terminal_failure)
    }

    /// Irreversibly closes service admission before component shutdown begins.
    pub fn begin_shutdown(&mut self) {
        if !self.readiness.admission_open {
            return;
        }
        self.readiness.admission_open = false;
        self.services.runtime.begin_shutdown();
        if let Some(flight) = &self.flight {
            flight.close_admission();
        }
        if let Some(native_storage) = &self.native_storage {
            native_storage.close_query_canary_admission();
        }
    }

    /// Stops protocol frontends and ingest before control and native storage.
    pub fn shutdown(&mut self) -> ServerShutdownOutcome {
        if let Some(outcome) = &self.shutdown_outcome {
            return outcome.clone();
        }
        self.begin_shutdown();

        let (deadline, budget_error) = shutdown_deadline(
            self.shutdown_timeout,
            "server shutdown timeout exceeds monotonic clock range",
        );

        let (flight_outcome, flight_error) = match self.flight.take() {
            Some(mut flight) => {
                match flight.shutdown(deadline.saturating_duration_since(Instant::now())) {
                    Ok(outcome) => (Some(outcome), None),
                    Err(error) => (None, Some(error)),
                }
            }
            None => (None, None),
        };
        self.readiness.flight_listener_ready = false;
        self.readiness.flight_protocol_ready = false;

        let timed_out_processes = self
            .master
            .drain_registered_processes_for_shutdown(
                deadline.saturating_duration_since(Instant::now()),
            )
            .timed_out_processes;
        self.master.shutdown();
        self.readiness.master_listener_ready = false;
        let listener_error =
            finish_master_listener_until(&mut self.master_listener, deadline).err();
        let native_storage_outcome = if listener_error.is_none() {
            shutdown_native_storage_until(&mut self.native_storage, deadline)
        } else {
            None
        };
        let master_error = combine_optional_errors(budget_error, listener_error);
        let outcome = ServerShutdownOutcome {
            timed_out_processes,
            flight_error,
            flight_outcome,
            master_error,
            native_storage_outcome,
        };
        tracing::info!(
            component = "server",
            event = "server_stopped",
            status = if outcome.is_clean() {
                "stopped"
            } else {
                "incomplete"
            },
            timed_out_processes = ?outcome.timed_out_processes,
            flight_error = ?outcome.flight_error,
            flight_outcome = ?outcome.flight_outcome,
            native_storage_outcome = ?outcome.native_storage_outcome,
            master_error = ?outcome.master_error,
            "zippy server runtime stopped"
        );
        if outcome.is_clean() {
            self.data_directory.take();
        }
        let retryable_ordered_cleanup = self.master.is_running()
            || self.master_listener.is_some()
            || matches!(
                outcome.native_storage_outcome.as_ref(),
                Some(NativeStorageShutdownOutcome::TimedOut)
            )
            || (self.native_storage.is_some() && outcome.native_storage_outcome.is_none());
        if !retryable_ordered_cleanup {
            self.shutdown_outcome = Some(outcome.clone());
        }
        outcome
    }
}

fn apply_native_storage_runtime_health(
    readiness: &mut ServerReadiness,
    native_storage_healthy: Option<bool>,
) {
    if native_storage_healthy != Some(true) {
        readiness.native_catalog_ready = false;
        readiness.native_current_snapshot_files_ready = false;
    }
}

impl Drop for ServerRuntime {
    fn drop(&mut self) {
        let shutdown_was_attempted = self.shutdown_outcome.is_some();
        let outcome = self.shutdown();
        if !shutdown_was_attempted && !outcome.is_clean() {
            tracing::warn!(
                component = "server",
                event = "server_drop_incomplete",
                timed_out_processes = ?outcome.timed_out_processes,
                flight_error = ?outcome.flight_error,
                flight_outcome = ?outcome.flight_outcome,
                native_storage_outcome = ?outcome.native_storage_outcome,
                master_error = ?outcome.master_error,
                "zippy server runtime drop completed with cleanup errors"
            );
        }
    }
}

fn start_native_storage(
    runtime_config: &ZippyConfig,
    data_directory: &Arc<OwnedDataDirectory>,
    startup_deadline: Instant,
) -> Result<NativeStorageRuntime, ServerError> {
    remaining_before(startup_deadline, "start native storage")?;
    let persist_controller = PersistControllerConfig::new().with_max_parallel_writers(
        runtime_config
            .runtime
            .max_cpu_cores
            .min(MAX_PERSIST_PARALLEL_WRITERS),
    );
    let runtime_snapshot_pins = runtime_config.catalog.runtime_snapshot_pins;
    let query_resources = native_query_resource_config(runtime_config)?;
    let query_registry = native_query_registry_config(runtime_config);
    let query_engine = native_query_engine_config(runtime_config, query_resources)?;
    let query_canary = native_query_canary_config(runtime_config, query_resources)?;
    let catalog = SqliteCatalogConfig::new(data_directory).with_runtime_snapshot_pin_limits(
        runtime_snapshot_pins.max_active_pins,
        runtime_snapshot_pins.max_active_tables,
    );
    let native_storage = NativeStorageRuntimeConfig::new(data_directory)
        .with_catalog_config(catalog)?
        .with_persist_controller_config(persist_controller)
        .with_query_resource_config(query_resources)
        .with_query_registry_config(query_registry)
        .with_query_engine_config(query_engine)
        .with_query_canary_config(query_canary);
    match NativeStorageRuntime::start_until(native_storage, startup_deadline) {
        NativeStorageStartOutcome::Ready(runtime) => Ok(runtime),
        NativeStorageStartOutcome::TimedOut(startup) => {
            let deadline_error = ServerError::new(
                ErrorCode::DeadlineExceeded,
                "native storage did not complete recovery before the server startup deadline",
            );
            match startup.cancel_in_background() {
                Ok(()) => Err(deadline_error),
                Err(cleanup_error) => Err(ServerError::new(
                    cleanup_error.code(),
                    format!(
                        "{}; native storage startup cleanup handoff failed: {}",
                        deadline_error.message(),
                        cleanup_error.message()
                    ),
                )),
            }
        }
        NativeStorageStartOutcome::Failed(error) => Err(with_stage("start native storage", error)),
    }
}

fn native_query_resource_config(
    runtime_config: &ZippyConfig,
) -> Result<crate::query_service::QueryServiceResourceConfig, ServerError> {
    Ok(crate::query_service::QueryServiceResourceConfig {
        max_active_queries: runtime_config.query.resources.max_concurrent_queries,
        max_active_tasks: runtime_config.query.resources.max_active_tasks,
        max_cpu_slots: u32::try_from(runtime_config.runtime.max_cpu_cores).map_err(|_| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                "configured runtime CPU core budget exceeds u32",
            )
            .with_retryable(false)
        })?,
        max_memory_bytes: runtime_config.query.resources.max_memory_bytes,
    })
}

fn native_query_registry_config(
    runtime_config: &ZippyConfig,
) -> crate::query_service::QueryServiceRegistryConfig {
    crate::query_service::QueryServiceRegistryConfig {
        plan_ttl: Duration::from_millis(runtime_config.query.plan_ttl_ms),
        ..crate::query_service::QueryServiceRegistryConfig::default()
    }
}

fn native_query_engine_config(
    runtime_config: &ZippyConfig,
    resources: crate::query_service::QueryServiceResourceConfig,
) -> Result<crate::native_query::NativeQueryEngineConfig, ServerError> {
    crate::native_query::NativeQueryEngineConfig::try_new(
        runtime_config.query_parallelism(),
        resources.max_active_tasks,
        resources.max_cpu_slots,
        resources.max_memory_bytes,
        runtime_config.query_operator_memory_bytes(),
    )
}

fn native_query_canary_config(
    runtime_config: &ZippyConfig,
    resources: crate::query_service::QueryServiceResourceConfig,
) -> Result<Option<crate::native_query_canary::NativeQueryCanaryRuntimeConfig>, ServerError> {
    crate::native_query_canary::NativeQueryCanaryRuntimeConfig::try_new(
        runtime_config.query.canary,
        runtime_config.query_parallelism(),
        resources.max_active_tasks,
        resources.max_cpu_slots,
        resources.max_memory_bytes,
        runtime_config.query_canary_operator_memory_bytes(),
    )
}

fn start_flight_if_enabled(
    runtime_config: &ZippyConfig,
    native_storage: Option<&NativeStorageRuntime>,
    startup_deadline: Instant,
) -> Result<Option<FlightServerRuntime>, ServerError> {
    if !runtime_config.flight.enabled {
        return Ok(None);
    }
    let native_storage = native_storage.ok_or_else(|| {
        ServerError::new(
            ErrorCode::Internal,
            "validated Flight configuration did not start native storage",
        )
        .with_retryable(false)
    })?;
    let endpoint = runtime_config.flight.endpoint.parse().map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("validated Flight endpoint could not be parsed error=[{error}]"),
        )
        .with_retryable(false)
    })?;
    let transport_buffer_bytes = runtime_config
        .query
        .max_buffered_bytes
        .unwrap_or(runtime_config.runtime.max_query_buffer_bytes);
    let security = load_flight_security(&runtime_config.flight)?;
    let mut config = FlightServiceConfig::try_new(
        env!("CARGO_PKG_VERSION"),
        runtime_config.flight.max_message_bytes,
        runtime_config.flight.max_pending_tickets,
        transport_buffer_bytes,
    )?
    .with_query_control()
    .with_sql_metadata(FlightSqlMetadataLimits::try_new(
        runtime_config.flight.metadata.max_rows,
        runtime_config.flight.metadata.max_bytes,
        Duration::from_millis(runtime_config.flight.metadata.ticket_ttl_ms),
    )?);
    if let Some(bearer_auth) = security.bearer_auth {
        config = config.with_bearer_auth(bearer_auth);
    } else {
        config = config.with_local_read_sessions(
            Duration::from_millis(runtime_config.flight.auth.session_ttl_ms),
            runtime_config.flight.auth.max_sessions,
        )?;
    }
    let mut frontend = NativeFlightQueryFrontend::new(
        native_storage.query_engine(),
        native_storage.catalog_read_handle(),
    );
    if let Some(consumers) = native_storage.consumer_logs() {
        frontend = frontend.with_consumer_logs(consumers);
    }
    if runtime_config.flight.ingest.enabled {
        let native_ingest = native_storage.native_ingest_handle().ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "validated Flight ingest configuration did not start native ingest authority",
            )
            .with_retryable(false)
        })?;
        config = config.with_ingest(FlightPutLimits::try_new(
            runtime_config.flight.ingest.max_concurrent_puts,
            runtime_config.flight.ingest.max_batches_per_put,
            runtime_config.flight.ingest.max_rows_per_batch,
            runtime_config.flight.ingest.max_rows_per_put,
            runtime_config.flight.ingest.max_batch_bytes,
            runtime_config.flight.ingest.max_bytes_per_put,
        )?);
        frontend = frontend.with_native_ingest(native_ingest);
        if let Some(admin) = native_storage.table_admin() {
            frontend = frontend.with_table_admin(admin);
            config = config.with_table_management();
        }
    }
    let frontend: Arc<dyn zippy_protocol::server::FlightQueryFrontend> = Arc::new(frontend);
    let service = ZippyFlightService::try_new(config, frontend)?;
    let startup_timeout = remaining_before(startup_deadline, "start Flight listener")?;
    match security.tls {
        Some(tls) => FlightServerRuntime::start_with_tls(endpoint, service, tls, startup_timeout),
        None => FlightServerRuntime::start(endpoint, service, startup_timeout),
    }
    .map(Some)
}

fn stop_flight_until(
    flight: &mut Option<FlightServerRuntime>,
    deadline: Instant,
) -> Result<Option<FlightShutdownOutcome>, ServerError> {
    let Some(mut runtime) = flight.take() else {
        return Ok(None);
    };
    runtime
        .shutdown(deadline.saturating_duration_since(Instant::now()))
        .map(Some)
}

fn start_master_listener(
    master: &MasterServer,
    endpoint: &ControlEndpoint,
    deadline: Instant,
    data_directory: Arc<OwnedDataDirectory>,
) -> Result<MasterListenerRuntime, ServerError> {
    let startup_timeout = remaining_before(deadline, "start master listener")?;
    let listener_master = master.clone();
    let listener_endpoint = endpoint.clone();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let (completed_tx, completed) = mpsc::sync_channel(1);
    let join_handle = thread::Builder::new()
        .name("zippy-master-listener".to_string())
        .spawn(move || {
            let _data_directory = data_directory;
            let result =
                listener_master.serve_endpoint_with_ready(&listener_endpoint, Some(ready_tx));
            listener_master.shutdown();
            let _ = completed_tx.send(result);
        })
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("failed to start master listener thread error=[{error}]"),
            )
        })?;
    let mut runtime = Some(MasterListenerRuntime {
        completed,
        completion_result: None,
        join_handle: Some(join_handle),
    });

    match ready_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(Ok(())) => Ok(runtime.take().expect("master listener runtime exists")),
        Ok(Err(reason)) => {
            master.shutdown();
            let _ = finish_master_listener_until(&mut runtime, deadline);
            Err(ServerError::new(
                ErrorCode::Unavailable,
                format!("master listener failed before readiness: {reason}"),
            ))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            master.shutdown();
            let _ = finish_master_listener_until(&mut runtime, deadline);
            Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                format!(
                    "master listener did not become ready before timeout timeout_ms=[{}]",
                    startup_timeout.as_millis()
                ),
            ))
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let error = finish_master_listener_until(&mut runtime, deadline).err();
            Err(error.unwrap_or_else(|| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    "master listener exited before readiness",
                )
            }))
        }
    }
}

fn probe_master_dependency(endpoint: &ControlEndpoint, timeout: Duration) -> CoreResult<()> {
    let response = send_control_line_request_with_timeout(
        endpoint,
        ControlRequest::GetConfig(GetConfigRequest {}),
        timeout,
    )?;
    match response {
        ControlResponse::ConfigFetched { .. } => Ok(()),
        other => Err(ZippyError::InvalidConfig {
            reason: format!("unexpected master startup probe response actual=[{other}]"),
        }),
    }
}

#[derive(Clone, Copy)]
struct StartupRollbackContext<'a> {
    master: &'a MasterServer,
    shutdown_timeout: Duration,
}

fn rollback_startup(
    context: StartupRollbackContext<'_>,
    master_listener: &mut Option<MasterListenerRuntime>,
    native_storage: &mut Option<NativeStorageRuntime>,
    services: Option<&MasterAdapters>,
) -> ServerShutdownOutcome {
    let master = context.master;
    let (deadline, budget_error) = shutdown_deadline(
        context.shutdown_timeout,
        "startup rollback timeout exceeds monotonic clock range",
    );
    if let Some(services) = services {
        services.runtime.begin_shutdown();
    }
    let graceful = master.drain_registered_processes_for_shutdown(
        deadline.saturating_duration_since(Instant::now()),
    );
    if !graceful.timed_out_processes.is_empty() {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_timeout",
            timed_out_processes = ?graceful.timed_out_processes,
            "startup rollback timed out waiting for runtime processes"
        );
    }
    master.shutdown();
    let listener_error = finish_master_listener_until(master_listener, deadline).err();
    if let Some(error) = &listener_error {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_master_error",
            error = %error,
            "master listener cleanup failed during startup rollback"
        );
    }
    let native_storage_outcome = if listener_error.is_none() {
        shutdown_native_storage_until(native_storage, deadline)
    } else {
        None
    };
    if let Some(outcome) = native_storage_outcome
        .as_ref()
        .filter(|outcome| !outcome.is_complete())
    {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_native_storage_incomplete",
            outcome = ?outcome,
            "native storage cleanup was incomplete during startup rollback"
        );
    }
    ServerShutdownOutcome {
        timed_out_processes: graceful.timed_out_processes,
        flight_error: None,
        flight_outcome: None,
        master_error: combine_optional_errors(budget_error, listener_error),
        native_storage_outcome,
    }
}

fn finish_master_listener_until(
    runtime: &mut Option<MasterListenerRuntime>,
    deadline: Instant,
) -> Result<(), ServerError> {
    let Some(listener) = runtime.as_mut() else {
        return Ok(());
    };
    if listener.completion_result.is_none() {
        match listener
            .completed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(result) => listener.completion_result = Some(result),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                return Err(ServerError::new(
                    ErrorCode::DeadlineExceeded,
                    "master listener cleanup exceeded the server shutdown deadline",
                ))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                join_master_listener_thread_until(&mut listener.join_handle, deadline)?;
                runtime.take();
                return Err(ServerError::new(
                    ErrorCode::Internal,
                    "master listener exited without a completion result",
                ));
            }
        }
    }
    join_master_listener_thread_until(&mut listener.join_handle, deadline)?;
    let listener = runtime.take().expect("listener remains owned until joined");
    listener
        .completion_result
        .expect("listener completion was received")
        .map_err(|error| map_core_error("join master listener", error))
}

fn join_master_listener_thread_until(
    join_handle: &mut Option<JoinHandle<()>>,
    deadline: Instant,
) -> Result<(), ServerError> {
    let Some(handle) = join_handle.as_ref() else {
        return Ok(());
    };
    while !handle.is_finished() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ServerError::new(
                ErrorCode::DeadlineExceeded,
                "master listener thread did not exit before the server shutdown deadline",
            ));
        }
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    if join_handle
        .take()
        .is_some_and(|handle| handle.join().is_err())
    {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "master listener thread panicked",
        ));
    }
    Ok(())
}

fn reject_ephemeral_master_endpoint(endpoint: &ControlEndpoint) -> Result<(), ServerError> {
    if matches!(endpoint, ControlEndpoint::Tcp(address) if address.port() == 0) {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "server runtime requires a nonzero master TCP port",
        ));
    }
    Ok(())
}

/// Releases native storage ownership only after its shutdown completed.
fn shutdown_native_storage_until(
    native_storage: &mut Option<NativeStorageRuntime>,
    deadline: Instant,
) -> Option<NativeStorageShutdownOutcome> {
    let outcome = native_storage
        .as_mut()
        .map(|storage| storage.shutdown_until(deadline));
    if outcome
        .as_ref()
        .is_some_and(NativeStorageShutdownOutcome::is_complete)
    {
        native_storage.take();
    }
    outcome
}

/// Falls back to an immediate deadline and reports an unrepresentable cleanup budget.
fn shutdown_deadline(timeout: Duration, overflow_message: &str) -> (Instant, Option<ServerError>) {
    match deadline_after(timeout, overflow_message) {
        Ok(deadline) => (deadline, None),
        Err(error) => (Instant::now(), Some(error)),
    }
}

fn deadline_after(timeout: Duration, overflow_message: &str) -> Result<Instant, ServerError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| ServerError::new(ErrorCode::InvalidArgument, overflow_message.to_string()))
}

fn remaining_before(deadline: Instant, stage: &str) -> Result<Duration, ServerError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(ServerError::new(
            ErrorCode::DeadlineExceeded,
            format!("server deadline exceeded before stage=[{stage}]"),
        ));
    }
    Ok(remaining)
}

fn with_startup_rollback(
    startup_error: ServerError,
    rollback: ServerShutdownOutcome,
) -> ServerError {
    let Some(rollback_error) = shutdown_outcome_error(&rollback) else {
        return startup_error;
    };
    ServerError::new(
        rollback_error.code(),
        format!(
            "{}; startup rollback incomplete: {}",
            startup_error.message(),
            rollback_error.message()
        ),
    )
}

fn with_stage(stage: &str, error: ServerError) -> ServerError {
    ServerError::new(error.code(), format!("{stage}: {}", error.message()))
}

fn map_core_error(stage: &str, error: ZippyError) -> ServerError {
    let code = error.code();
    ServerError::new(code, format!("{stage}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::OwnedDataDirectory;
    use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogWrite,
        CreateNamespaceCommand, CreateTableCommand, VersionedCatalogCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NativeDataFileProvenance,
        SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition,
    };
    use zippy_types::{
        FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
    };

    use super::*;

    const DAEMON_TEST_NAMESPACE_ID: u128 = 91;
    const DAEMON_TEST_TABLE_ID: u128 = 92;

    fn runtime_test_log_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "zippy-server-runtime-unit-test-logs-{}",
            std::process::id()
        ))
    }

    #[test]
    fn native_query_resources_use_query_buffer_budget() {
        let mut lower_buffer = ZippyConfig::default();
        lower_buffer.runtime.max_cpu_cores = 6;
        lower_buffer.runtime.max_query_buffer_bytes = 64 * 1024 * 1024;
        lower_buffer.query.parallelism = Some(2);
        lower_buffer.query.resources.max_concurrent_queries = 23;
        lower_buffer.query.resources.max_active_tasks = 31;
        lower_buffer.query.resources.max_memory_bytes = 384 * 1024 * 1024;
        let mut higher_buffer = lower_buffer.clone();
        higher_buffer.runtime.max_query_buffer_bytes = 1024 * 1024 * 1024;

        let lower_resources = native_query_resource_config(&lower_buffer).unwrap();
        let higher_resources = native_query_resource_config(&higher_buffer).unwrap();

        assert_eq!(lower_resources, higher_resources);
        assert_eq!(lower_resources.max_active_queries, 23);
        assert_eq!(lower_resources.max_active_tasks, 31);
        assert_eq!(lower_resources.max_cpu_slots, 6);
        assert_eq!(lower_resources.max_memory_bytes, 384 * 1024 * 1024);
    }

    #[test]
    fn native_query_registry_uses_the_server_plan_ttl() {
        let mut config = ZippyConfig::default();
        config.query.plan_ttl_ms = 12_345;

        let registry = native_query_registry_config(&config);

        assert_eq!(registry.plan_ttl, Duration::from_millis(12_345));
    }

    #[test]
    fn native_query_canary_is_explicit() {
        let disabled = ZippyConfig::default();
        let resources = native_query_resource_config(&disabled).unwrap();
        assert!(native_query_canary_config(&disabled, resources)
            .unwrap()
            .is_none());

        let mut enabled = disabled;
        enabled.query.canary.enabled = true;
        let resources = native_query_resource_config(&enabled).unwrap();
        assert!(native_query_canary_config(&enabled, resources)
            .unwrap()
            .is_some());
        assert!(!crate::ServerCapabilities::control_only().query_sql);
        assert!(!crate::ServerCapabilities::control_only().query_manifest_snapshot_pinning);
    }

    fn daemon_test_namespace_id() -> NamespaceId {
        NamespaceId::from_u128(DAEMON_TEST_NAMESPACE_ID).unwrap()
    }

    fn daemon_test_table_id() -> TableId {
        TableId::from_u128(DAEMON_TEST_TABLE_ID).unwrap()
    }

    fn daemon_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
    }

    fn daemon_test_schema_ipc(schema: &Schema) -> SchemaIpcPayload {
        let mut bytes = Vec::new();
        let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
        writer.finish().unwrap();
        drop(writer);
        SchemaIpcPayload::new(bytes).unwrap()
    }

    fn daemon_catalog_command(
        request_id: u128,
        expected_revision: u64,
        command: CatalogCommandV1,
    ) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(request_id).unwrap(),
            Some(expected_revision),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "daemon-runtime-test".to_owned(),
            },
            command,
        )
        .unwrap()
        .into()
    }

    fn prepare_daemon_native_table(data_path: &Path) {
        let ownership = Arc::new(OwnedDataDirectory::acquire_or_initialize(data_path).unwrap());
        let catalog = SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap();
        let async_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        async_runtime.block_on(async {
            catalog
                .command(daemon_catalog_command(
                    1,
                    0,
                    CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                        namespace_id: daemon_test_namespace_id(),
                        name: CatalogName::new("market").unwrap(),
                    }),
                ))
                .await
                .unwrap();
            catalog
                .command(daemon_catalog_command(
                    2,
                    1,
                    CatalogCommandV1::CreateTable(CreateTableCommand {
                        table_id: daemon_test_table_id(),
                        namespace_id: daemon_test_namespace_id(),
                        name: CatalogName::new("ticks").unwrap(),
                        definition: TableDefinition::Append(AppendTableDefinition::default()),
                        schema_ipc: daemon_test_schema_ipc(daemon_test_schema().as_ref()),
                        config: BTreeMap::new(),
                    }),
                ))
                .await
                .unwrap();
        });
        catalog.close().unwrap();
    }

    fn daemon_test_persist_request(file_id: FileId) -> PersistRequest {
        let offsets = OffsetRange::new(LogOffset::new(0), LogOffset::new(0)).unwrap();
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(vec![SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: 1,
                    generation: 1,
                    writer_epoch: 0,
                },
                offsets,
            }])
            .unwrap(),
        )
        .unwrap();
        let schema = daemon_test_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![42]))],
        )
        .unwrap();
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id,
                table_id: daemon_test_table_id(),
                schema_version: 1,
                schema,
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: 1_752_492_600_000_000_000,
            },
            vec![batch],
        )
        .unwrap();
        PersistRequest::new(
            RequestId::from_u128(93).unwrap(),
            daemon_test_table_id(),
            1,
            0,
            vec![candidate],
            OffsetWatermark::Known(LogOffset::new(0)),
        )
        .unwrap()
    }

    fn test_starting_record(
        ready_file: &Path,
        control_endpoint: &ControlEndpoint,
        launch_nonce: &str,
    ) -> ServerStartingRecord {
        let run_dir = ready_file.parent().unwrap();
        let log_dir = run_dir.join("logs");
        ServerStartingRecord {
            schema_version: 1,
            status: "starting".to_string(),
            launch_nonce: launch_nonce.to_string(),
            pid: std::process::id(),
            launcher: ServerLauncherState {
                process_identity: ServerLauncherProcessIdentity {
                    boot_id: "boot-1".to_string(),
                    start_ticks: "123".to_string(),
                },
                started_at: "2026-07-13T00:00:00+00:00".to_string(),
                requested_control_endpoint: control_endpoint.display_string(),
                config_path: None,
                webui: ServerLauncherWebUi {
                    managed: false,
                    status: "unknown".to_string(),
                    url: "http://127.0.0.1:17688/".to_string(),
                },
                bootstrap_log: log_dir.join("server.log").display().to_string(),
                state_file: ready_file.display().to_string(),
                lock_file: run_dir.join("server.lock").display().to_string(),
                outcome_file: run_dir.join("server.outcome.json").display().to_string(),
                run_dir: run_dir.display().to_string(),
                log_dir: log_dir.display().to_string(),
            },
        }
    }

    fn write_test_starting_record(
        ready_file: &Path,
        control_endpoint: &ControlEndpoint,
        launch_nonce: &str,
    ) {
        let record = test_starting_record(ready_file, control_endpoint, launch_nonce);
        fs::write(ready_file, serde_json::to_vec(&record).unwrap()).unwrap();
    }

    #[test]
    fn readiness_allows_control_only_mode() {
        let readiness = ServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            flight_configured: false,
            flight_listener_ready: false,
            flight_protocol_ready: false,
            native_catalog_ready: true,
            native_current_snapshot_files_ready: true,
            storage_reconciliation_complete: true,
            commit_recovery_complete: true,
        };

        assert!(readiness.is_ready());
    }

    #[test]
    fn readiness_always_requires_every_native_storage_gate() {
        let mut readiness = ServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            flight_configured: false,
            flight_listener_ready: false,
            flight_protocol_ready: false,
            native_catalog_ready: true,
            native_current_snapshot_files_ready: true,
            storage_reconciliation_complete: true,
            commit_recovery_complete: false,
        };

        assert!(!readiness.is_ready());
        readiness.commit_recovery_complete = true;
        assert!(readiness.is_ready());
        readiness.native_current_snapshot_files_ready = false;
        assert!(!readiness.is_ready());
        readiness.native_current_snapshot_files_ready = true;
        readiness.storage_reconciliation_complete = false;
        assert!(!readiness.is_ready());
    }

    #[test]
    fn readiness_requires_both_flight_listener_and_protocol_barriers() {
        let mut readiness = ServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            flight_configured: true,
            flight_listener_ready: true,
            flight_protocol_ready: false,
            native_catalog_ready: true,
            native_current_snapshot_files_ready: true,
            storage_reconciliation_complete: true,
            commit_recovery_complete: true,
        };

        assert!(!readiness.is_ready());
        readiness.flight_protocol_ready = true;
        assert!(readiness.is_ready());
        readiness.flight_listener_ready = false;
        assert!(!readiness.is_ready());
    }

    #[test]
    fn runtime_native_failure_revokes_dynamic_readiness() {
        let mut readiness = ServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            flight_configured: false,
            flight_listener_ready: false,
            flight_protocol_ready: false,
            native_catalog_ready: true,
            native_current_snapshot_files_ready: true,
            storage_reconciliation_complete: true,
            commit_recovery_complete: true,
        };

        apply_native_storage_runtime_health(&mut readiness, Some(false));

        assert!(!readiness.native_catalog_ready);
        assert!(!readiness.native_current_snapshot_files_ready);
        assert!(!readiness.is_ready());
    }

    #[cfg(unix)]
    #[test]
    fn daemon_leaf_starts_waits_and_stops_the_owned_runtime() {
        {
            let temp = tempfile::tempdir().unwrap();
            let socket_path = temp.path().join("daemon.sock");
            let config_path = temp.path().join("zippy.toml");
            fs::write(
                &config_path,
                "[master]\nshutdown_timeout_ms = 1000\n\n[flight]\nenabled = false\n",
            )
            .unwrap();
            let master = MasterDaemonConfig::new(ControlEndpoint::Unix(socket_path.clone()))
                .with_config_path(config_path)
                .with_logging(runtime_test_log_dir(), "info", false);

            run_server_daemon_with_waiter(ServerRuntimeConfig::new(master), |runtime| {
                assert!(runtime.health().readiness.is_ready());
                assert!(runtime.terminal_failure().is_none());
                assert!(socket_path.exists());
                Ok("TEST")
            })
            .unwrap();

            assert!(!socket_path.exists());
            assert!(!socket_path.with_extension("sock.owner").exists());
        }
    }

    #[cfg(unix)]
    #[test]
    fn daemon_publishes_owner_only_ready_record_after_full_startup() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("ready-daemon.sock");
        let ready_path = temp.path().join("server-ready.json");
        let config_path = temp.path().join("zippy.toml");
        fs::write(
            &config_path,
            r#"
[master]
shutdown_timeout_ms = 1000

[flight]
enabled = false
"#,
        )
        .unwrap();
        let control_endpoint = ControlEndpoint::Unix(socket_path);
        write_test_starting_record(&ready_path, &control_endpoint, "launch-nonce-1");
        let master = MasterDaemonConfig::new(control_endpoint)
            .with_config_path(config_path)
            .with_logging(runtime_test_log_dir(), "info", false);
        let config =
            ServerRuntimeConfig::new(master).with_ready_file(&ready_path, "launch-nonce-1");

        run_server_daemon_with_waiter(config, |runtime| {
            assert!(runtime.health().readiness.is_ready());
            let payload = fs::read_to_string(&ready_path).unwrap();
            let record: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(record["schema_version"], SERVER_READY_SCHEMA_VERSION);
            assert_eq!(record["status"], "ready");
            assert_eq!(record["launch_nonce"], "launch-nonce-1");
            assert_eq!(record["pid"], std::process::id());
            assert_eq!(record["version"], base_version());
            assert_eq!(record["mode"], "control_only");
            assert_eq!(
                record["launcher"]["state_file"],
                ready_path.display().to_string()
            );
            assert_eq!(record["launcher"]["webui"]["managed"], false);
            assert_eq!(record["health"]["readiness"]["admission_open"], true);
            assert_eq!(record["flight"]["tls_enabled"], false);
            assert_eq!(record["flight"]["authentication_required"], false);
            assert_eq!(
                record["flight"]["administrator_authentication_enabled"],
                false
            );
            assert_eq!(record["flight"]["writer_authentication_enabled"], false);
            assert_eq!(record["flight"]["query_control_enabled"], false);
            assert_eq!(record["flight"]["flight_sql_metadata_enabled"], false);
            assert_eq!(record["flight"]["flight_ingest_enabled"], false);
            assert_eq!(record["flight"]["metadata_max_rows"], 0);
            assert_eq!(record["flight"]["metadata_max_bytes"], 0);
            assert_eq!(record["flight"]["ingest_max_concurrent_puts"], 0);
            assert_eq!(record["flight"]["ingest_max_batches_per_put"], 0);
            assert_eq!(record["flight"]["ingest_max_rows_per_batch"], 0);
            assert_eq!(record["flight"]["ingest_max_rows_per_put"], 0);
            assert_eq!(record["flight"]["ingest_max_batch_bytes"], 0);
            assert_eq!(record["flight"]["ingest_max_bytes_per_put"], 0);
            assert!(record["flight"]["certificate_sha256"].is_null());
            assert_eq!(record["flight"]["client_certificate_required"], false);
            let mut client =
                zippy_core::MasterClient::connect_endpoint(runtime.control_endpoint().clone())
                    .unwrap();
            client.register_process("ready_record_probe").unwrap();
            assert!(!payload.contains(client.process_token().unwrap()));
            client.unregister_process().unwrap();
            assert_eq!(
                fs::metadata(&ready_path).unwrap().permissions().mode() & 0o077,
                0
            );
            Ok("TEST")
        })
        .unwrap();

        let stopping: serde_json::Value =
            serde_json::from_slice(&fs::read(&ready_path).unwrap()).unwrap();
        assert_eq!(stopping["schema_version"], SERVER_LIFECYCLE_SCHEMA_VERSION);
        assert_eq!(stopping["status"], "stopping");
        assert!(stopping.get("error_code").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn daemon_fails_fast_and_transitions_state_after_native_health_loss() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("failed-native-daemon.sock");
        let ready_path = temp.path().join("server-ready.json");
        let data_path = temp.path().join("database");
        let config_path = temp.path().join("zippy.toml");
        prepare_daemon_native_table(&data_path);
        fs::write(
            &config_path,
            format!(
                concat!(
                    "[server]\n",
                    "data_dir = {:?}\n",
                    "",
                    "[master]\n",
                    "shutdown_timeout_ms = 1000\n"
                ),
                data_path.display().to_string()
            ),
        )
        .unwrap();
        let control_endpoint = ControlEndpoint::Unix(socket_path.clone());
        write_test_starting_record(
            &ready_path,
            &control_endpoint,
            "launch-nonce-native-failure",
        );
        let master = MasterDaemonConfig::new(control_endpoint)
            .with_config_path(config_path)
            .with_logging(runtime_test_log_dir(), "info", false);
        let config = ServerRuntimeConfig::new(master)
            .with_ready_file(&ready_path, "launch-nonce-native-failure");

        let error = run_server_daemon_with_waiter(config, |runtime| {
            assert!(runtime.health().readiness.is_ready());
            assert!(ready_path.is_file());
            let actor = runtime
                .native_storage
                .as_ref()
                .expect("native storage is configured")
                .table_actor(daemon_test_table_id())
                .unwrap();
            let file_id = FileId::from_u128(94).unwrap();
            let table_directory = data_path
                .join("parquet")
                .join(daemon_test_table_id().to_string());
            fs::create_dir(&table_directory).unwrap();
            fs::set_permissions(&table_directory, fs::Permissions::from_mode(0o700)).unwrap();
            let conflicting_path = table_directory.join(format!("{file_id}.parquet"));
            fs::write(&conflicting_path, b"injected conflicting parquet identity").unwrap();
            fs::set_permissions(&conflicting_path, fs::Permissions::from_mode(0o600)).unwrap();
            let async_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut failure_injected = false;
            wait_for_shutdown_or_runtime_failure(runtime, || {
                if !failure_injected {
                    failure_injected = true;
                    let persist_error = async_runtime
                        .block_on(actor.persist(daemon_test_persist_request(file_id)))
                        .unwrap_err();
                    assert_eq!(persist_error.code(), ErrorCode::DataCorruption);
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while runtime.terminal_failure().is_none() && Instant::now() < deadline {
                        thread::sleep(Duration::from_millis(1));
                    }
                    assert!(runtime.terminal_failure().is_some());
                }
                Some("SIGTERM")
            })
        })
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::DataCorruption);
        let failed: serde_json::Value =
            serde_json::from_slice(&fs::read(&ready_path).unwrap()).unwrap();
        assert_eq!(failed["schema_version"], SERVER_LIFECYCLE_SCHEMA_VERSION);
        assert_eq!(failed["status"], "failed");
        assert_eq!(failed["error_code"], "DATA_CORRUPTION");
        assert_eq!(
            fs::metadata(&ready_path).unwrap().permissions().mode() & 0o077,
            0
        );
        assert!(!socket_path.exists());
        assert!(!socket_path.with_extension("sock.owner").exists());
    }

    #[cfg(unix)]
    #[test]
    fn daemon_rejects_empty_ready_nonce_before_binding_listener() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("invalid-ready-daemon.sock");
        let ready_path = temp.path().join("server-ready.json");
        let master = MasterDaemonConfig::new(ControlEndpoint::Unix(socket_path.clone()))
            .with_logging(runtime_test_log_dir(), "info", false);
        let config = ServerRuntimeConfig::new(master).with_ready_file(&ready_path, "");

        let error = run_server_daemon_with_waiter(config, |_| Ok("TEST")).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!socket_path.exists());
        assert!(!ready_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn daemon_rejects_unknown_launcher_state_fields_before_binding_listener() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("invalid-launcher-daemon.sock");
        let ready_path = temp.path().join("server-ready.json");
        let control_endpoint = ControlEndpoint::Unix(socket_path.clone());
        let master = MasterDaemonConfig::new(control_endpoint.clone()).with_logging(
            runtime_test_log_dir(),
            "info",
            false,
        );
        let record = test_starting_record(&ready_path, &control_endpoint, "launch-nonce-2");
        let mut record = serde_json::to_value(record).unwrap();
        record["launcher"].as_object_mut().unwrap().insert(
            "token".to_string(),
            serde_json::Value::String("must-not-persist".to_string()),
        );
        fs::write(&ready_path, serde_json::to_vec(&record).unwrap()).unwrap();
        let config =
            ServerRuntimeConfig::new(master).with_ready_file(&ready_path, "launch-nonce-2");

        let error = run_server_daemon_with_waiter(config, |_| Ok("TEST")).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!socket_path.exists());
        assert!(ready_path.exists());
    }

    #[test]
    fn incomplete_shutdown_is_promoted_to_server_error() {
        let outcome = ServerShutdownOutcome {
            timed_out_processes: vec!["process-1".to_string()],
            flight_error: None,
            flight_outcome: None,
            master_error: None,
            native_storage_outcome: None,
        };

        let error = shutdown_outcome_error(&outcome).unwrap();

        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert!(error.message().contains("process-1"));
    }

    #[test]
    fn daemon_cleanup_failures_do_not_replace_the_terminal_error_code() {
        let primary = ServerError::new(ErrorCode::DataCorruption, "native storage failed")
            .with_table_id(daemon_test_table_id())
            .with_retryable(false);
        let transition = ServerError::new(ErrorCode::Unavailable, "state transition failed");
        let shutdown = shutdown_outcome_error(&ServerShutdownOutcome {
            native_storage_outcome: Some(NativeStorageShutdownOutcome::TimedOut),
            ..ServerShutdownOutcome::default()
        })
        .unwrap();

        let error = combine_daemon_failure(primary, Some(transition), Some(shutdown));

        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.table_id(), Some(daemon_test_table_id()));
        assert!(!error.retryable());
        assert!(error.message().contains("state transition failed"));
        assert!(error.message().contains("server shutdown incomplete"));
    }

    #[test]
    fn incomplete_startup_rollback_is_attached_to_the_startup_error() {
        let startup_error = ServerError::new(ErrorCode::Unavailable, "dependency probe failed");
        let rollback = ServerShutdownOutcome {
            timed_out_processes: vec!["process-rollback".to_string()],
            flight_error: None,
            flight_outcome: None,
            master_error: None,
            native_storage_outcome: None,
        };

        let error = with_startup_rollback(startup_error, rollback);

        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert!(error.message().contains("dependency probe failed"));
        assert!(error.message().contains("startup rollback incomplete"));
        assert!(error.message().contains("process-rollback"));
    }

    #[test]
    fn completed_listener_is_joined_before_cleanup_reports_success() {
        let (completed_tx, completed) = mpsc::sync_channel(1);
        let join_handle = thread::spawn(move || {
            completed_tx.send(Ok::<(), ZippyError>(())).unwrap();
            thread::sleep(Duration::from_millis(40));
        });
        let mut runtime = Some(MasterListenerRuntime {
            completed,
            completion_result: None,
            join_handle: Some(join_handle),
        });
        let started = Instant::now();

        finish_master_listener_until(&mut runtime, Instant::now() + Duration::from_millis(250))
            .unwrap();

        assert!(runtime.is_none());
        assert!(started.elapsed() >= Duration::from_millis(20));
    }
    #[test]
    fn listener_cleanup_retries_after_receive_and_join_deadlines() {
        for send_before_timeout in [false, true] {
            let (completed_tx, completed) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(1);
            let (sent_tx, sent_rx) = mpsc::sync_channel(1);
            let join_handle = thread::spawn(move || {
                if send_before_timeout {
                    completed_tx.send(Ok::<(), ZippyError>(())).unwrap();
                    sent_tx.send(()).unwrap();
                }
                release_rx.recv().unwrap();
                if !send_before_timeout {
                    completed_tx.send(Ok::<(), ZippyError>(())).unwrap();
                }
            });
            if send_before_timeout {
                sent_rx.recv().unwrap();
            }
            let mut runtime = Some(MasterListenerRuntime {
                completed,
                completion_result: None,
                join_handle: Some(join_handle),
            });
            let error = finish_master_listener_until(&mut runtime, Instant::now()).unwrap_err();
            assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
            assert!(runtime.is_some());
            release_tx.send(()).unwrap();
            finish_master_listener_until(&mut runtime, Instant::now() + Duration::from_secs(1))
                .unwrap();
            assert!(runtime.is_none());
        }
    }
}
