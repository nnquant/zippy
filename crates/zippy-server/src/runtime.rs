//! Server-owned lifecycle for the Phase 1 legacy runtime.

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
    current_log_snapshot, python_dev_version, resolve_control_endpoint,
    send_control_line_request_with_timeout, ControlEndpoint, ControlRequest, ControlResponse,
    OwnedDataDirectory, Result as CoreResult, ZippyCatalogBackend, ZippyConfig, ZippyError,
};
use zippy_gateway::{
    GatewayServer, GatewayShutdownOutcome, LegacyPersistedReadSnapshotSource,
    PersistedReadShadowConfig, PersistedReadSnapshotSource, ShadowPersistedReadSnapshotSource,
};
use zippy_master::daemon::{
    prepare_master_runtime, start_legacy_gateway_with_persisted_read_snapshot_source_if_enabled,
    MasterDaemonConfig,
};
use zippy_master::legacy_catalog::LegacyCatalogAdapter;
use zippy_master::legacy_runtime::LegacyRuntimeRegistryAdapter;
use zippy_master::server::MasterServer;
use zippy_storage::{PersistControllerConfig, MAX_PERSIST_PARALLEL_WRITERS};
use zippy_types::{ErrorCode, ServerError};

use crate::legacy_catalog_migration::{
    plan_legacy_catalog_metadata_import, LegacyCatalogMetadataImportPlan,
    LegacyCatalogMigrationStartup,
};
use crate::{
    catalog_command_shadow::{LegacyCatalogShadowWorker, LegacyCatalogShadowWorkerConfig},
    catalog_shadow::SqliteCatalogPersistedReadShadowSource,
    LegacyCatalogMigrationDegradation, LegacyCatalogMigrationDegradedReason,
    LegacyCatalogMigrationStatus, LegacyCatalogShadowShutdownOutcome, LegacyCatalogShadowStatus,
    LegacyQueryEngineAdapter, LegacyServerHealth, LegacyServerServices, NativeStorageRuntime,
    NativeStorageRuntimeConfig, NativeStorageShutdownOutcome, NativeStorageStartOutcome,
};

const DEFAULT_SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const DAEMON_HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(50);
const SERVER_LIFECYCLE_SCHEMA_VERSION: u8 = 1;
const SERVER_READY_SCHEMA_VERSION: u8 = 2;

/// Runs the server-owned legacy runtime until shutdown or dynamic readiness loss.
///
/// On Unix, this function installs and owns the `SIGINT` and `SIGTERM` subscriptions before any
/// listener is started. After the first signal it closes adapter admission and stops Gateway
/// before master. A running component that loses readiness triggers the same ordered shutdown and
/// returns an error so detached launchers cannot keep reporting a stale ready record.
///
/// # Errors
///
/// Returns an error when signal installation, runtime startup, signal waiting, or shutdown fails.
pub fn run_server_daemon(config: LegacyServerRuntimeConfig) -> Result<(), ServerError> {
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
    runtime: &LegacyServerRuntime,
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

fn runtime_failure(runtime: &LegacyServerRuntime) -> Option<ServerError> {
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
            "server runtime lost readiness admission_open=[{}] \
             master_listener_ready=[{}] gateway_configured=[{}] \
             gateway_listener_ready=[{}] gateway_master_reachable=[{}] \
             native_storage_configured=[{}] native_catalog_ready=[{}] \
             native_current_snapshot_files_ready=[{}]",
            health.readiness.admission_open,
            health.readiness.master_listener_ready,
            health.readiness.gateway_configured,
            health.readiness.gateway_listener_ready,
            health.readiness.gateway_master_reachable,
            health.readiness.native_storage_configured,
            health.readiness.native_catalog_ready,
            health.readiness.native_current_snapshot_files_ready,
        ),
    ))
}

fn run_server_daemon_with_waiter<F>(
    config: LegacyServerRuntimeConfig,
    wait_for_shutdown: F,
) -> Result<(), ServerError>
where
    F: FnOnce(&mut LegacyServerRuntime) -> Result<&'static str, ServerError>,
{
    let launcher_state = validate_ready_configuration(&config)?;
    let ready_file = config.ready_file.clone();
    let launch_nonce = config.launch_nonce.clone();
    let mut runtime = LegacyServerRuntime::start(config)?;
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

fn shutdown_outcome_error(outcome: &LegacyServerShutdownOutcome) -> Option<ServerError> {
    if outcome.is_clean() {
        return None;
    }
    let deadline_exceeded = !outcome.timed_out_processes.is_empty()
        || outcome
            .gateway_outcome
            .as_ref()
            .is_some_and(|gateway| gateway.deadline_exceeded)
        || outcome
            .gateway_error
            .as_ref()
            .is_some_and(|error| error.code() == ErrorCode::DeadlineExceeded)
        || outcome
            .master_error
            .as_ref()
            .is_some_and(|error| error.code() == ErrorCode::DeadlineExceeded)
        || matches!(
            outcome.legacy_catalog_shadow_outcome.as_ref(),
            Some(LegacyCatalogShadowShutdownOutcome::TimedOut)
        )
        || outcome
            .legacy_catalog_shadow_outcome
            .as_ref()
            .and_then(LegacyCatalogShadowShutdownOutcome::error)
            .is_some_and(|error| error.code() == ErrorCode::DeadlineExceeded)
        || matches!(
            outcome.native_storage_outcome.as_ref(),
            Some(NativeStorageShutdownOutcome::TimedOut)
        )
        || outcome
            .native_storage_outcome
            .as_ref()
            .and_then(NativeStorageShutdownOutcome::error)
            .is_some_and(|error| error.code() == ErrorCode::DeadlineExceeded);
    let code = if deadline_exceeded {
        ErrorCode::DeadlineExceeded
    } else {
        outcome
            .gateway_error
            .as_ref()
            .or(outcome.master_error.as_ref())
            .or_else(|| {
                outcome
                    .legacy_catalog_shadow_outcome
                    .as_ref()
                    .and_then(LegacyCatalogShadowShutdownOutcome::error)
            })
            .or_else(|| {
                outcome
                    .native_storage_outcome
                    .as_ref()
                    .and_then(NativeStorageShutdownOutcome::error)
            })
            .map_or(ErrorCode::Unavailable, ServerError::code)
    };
    Some(ServerError::new(
        code,
        format!(
            "server shutdown incomplete timed_out_processes=[{}] gateway_error=[{}] \
             gateway_outcome=[{:?}] legacy_catalog_shadow_outcome=[{:?}] \
             native_storage_outcome=[{:?}] master_error=[{}]",
            outcome.timed_out_processes.join(","),
            outcome
                .gateway_error
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            outcome.gateway_outcome,
            outcome.legacy_catalog_shadow_outcome,
            outcome.native_storage_outcome,
            outcome
                .master_error
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        ),
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

/// Configuration for one server-owned legacy runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyServerRuntimeConfig {
    master: MasterDaemonConfig,
    startup_timeout: Duration,
    ready_file: Option<PathBuf>,
    launch_nonce: Option<String>,
}

impl LegacyServerRuntimeConfig {
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
struct ServerReadyGateway<'a> {
    enabled: bool,
    endpoint: Option<&'a str>,
    protocol_version: u16,
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
    gateway: ServerReadyGateway<'a>,
    mode: LegacyServerMode,
    health: LegacyServerRuntimeHealth,
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
    config: &LegacyServerRuntimeConfig,
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
    runtime: &LegacyServerRuntime,
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
    let record = ServerReadyRecord {
        schema_version: SERVER_READY_SCHEMA_VERSION,
        status: "ready",
        launch_nonce,
        pid: std::process::id(),
        version: python_dev_version(),
        ready_at_unix_ns,
        control_endpoint: runtime.control_endpoint().display_string(),
        gateway: ServerReadyGateway {
            enabled: runtime.gateway_endpoint().is_some(),
            endpoint: runtime.gateway_endpoint(),
            protocol_version: runtime.runtime_config().gateway.protocol_version,
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

/// Active compatibility mode selected from the existing gateway configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyServerMode {
    /// Only the master listener and legacy catalog adapter are enabled.
    CatalogOnly,
    /// Master, Gateway, catalog, and query adapters are enabled in one process.
    CatalogAndQuery,
}

/// Listener and dependency facts used to decide whether admission is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyServerReadiness {
    /// Whether the runtime currently permits new adapter work.
    pub admission_open: bool,
    /// Whether the master listener crossed its explicit bind barrier.
    pub master_listener_ready: bool,
    /// Whether the effective configuration enables Gateway.
    pub gateway_configured: bool,
    /// Whether Gateway successfully bound its listener.
    pub gateway_listener_ready: bool,
    /// Whether Gateway completed a real control request against its configured master.
    pub gateway_master_reachable: bool,
    /// Whether configuration requires the server-owned native storage lifecycle.
    pub native_storage_configured: bool,
    /// Whether the SQLite Catalog passed migration and integrity checks.
    pub native_catalog_ready: bool,
    /// Whether every current-snapshot managed file passed physical verification.
    pub native_current_snapshot_files_ready: bool,
    /// Whether final revision-stable physical-to-Catalog and reverse verification completed.
    pub storage_reconciliation_complete: bool,
    /// Whether every durable Catalog commit intent was replayed before readiness.
    pub commit_recovery_complete: bool,
}

impl LegacyServerReadiness {
    /// Returns whether every configured service dependency is ready for admission.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        self.admission_open
            && self.master_listener_ready
            && (!self.native_storage_configured
                || (self.native_catalog_ready
                    && self.native_current_snapshot_files_ready
                    && self.storage_reconciliation_complete
                    && self.commit_recovery_complete))
            && (!self.gateway_configured
                || (self.gateway_listener_ready && self.gateway_master_reachable))
    }
}

/// Unified programmatic health view of the owning runtime and compatibility services.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyServerRuntimeHealth {
    /// Active compatibility mode.
    pub mode: LegacyServerMode,
    /// Concrete listener and dependency readiness facts.
    pub readiness: LegacyServerReadiness,
    /// Capability-aware catalog and query health.
    pub services: LegacyServerHealth,
    /// Shadow migration status; degradation does not close Legacy authority admission.
    pub legacy_catalog_migration: LegacyCatalogMigrationStatus,
    /// Online legacy-to-SQLite command shadow state; it never controls primary readiness.
    pub legacy_catalog_shadow: Option<LegacyCatalogShadowStatus>,
    /// Process-local snapshot pin admission state when native storage is configured.
    pub catalog_runtime_snapshot_pins: Option<SqliteCatalogRuntimeSnapshotPinStatus>,
}

/// Complete result of a server-owned shutdown attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyServerShutdownOutcome {
    /// Runtime process identifiers that did not leave before the master timeout.
    pub timed_out_processes: Vec<String>,
    /// Gateway cleanup failure, after its listener and heartbeat were stopped.
    pub gateway_error: Option<ServerError>,
    /// Structured Gateway cleanup facts when Gateway was configured.
    pub gateway_outcome: Option<GatewayShutdownOutcome>,
    /// Master listener or final snapshot failure observed while joining.
    pub master_error: Option<ServerError>,
    /// Bounded event drain and final exact reconcile outcome, when shadowing was enabled.
    pub legacy_catalog_shadow_outcome: Option<LegacyCatalogShadowShutdownOutcome>,
    /// Ordered native coordinator and Catalog cleanup state, when configured.
    pub native_storage_outcome: Option<NativeStorageShutdownOutcome>,
}

impl LegacyServerShutdownOutcome {
    /// Returns whether all components stopped and released their master leases cleanly.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.timed_out_processes.is_empty()
            && self.gateway_error.is_none()
            && self
                .gateway_outcome
                .as_ref()
                .is_none_or(GatewayShutdownOutcome::is_clean)
            && self.master_error.is_none()
            && self
                .legacy_catalog_shadow_outcome
                .as_ref()
                .is_none_or(LegacyCatalogShadowShutdownOutcome::is_complete)
            && self
                .native_storage_outcome
                .as_ref()
                .is_none_or(NativeStorageShutdownOutcome::is_complete)
    }
}

enum LegacyServiceSet {
    CatalogOnly {
        catalog: Arc<LegacyCatalogAdapter>,
        runtime: Arc<LegacyRuntimeRegistryAdapter>,
    },
    CatalogAndQuery {
        services: LegacyServerServices,
    },
}

struct MasterListenerRuntime {
    completed: mpsc::Receiver<CoreResult<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MasterListenerRuntime {
    fn is_finished(&self) -> bool {
        self.join_handle
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
    }
}

impl LegacyServiceSet {
    fn catalog(&self) -> Arc<LegacyCatalogAdapter> {
        match self {
            Self::CatalogOnly { catalog, .. } => Arc::clone(catalog),
            Self::CatalogAndQuery { services } => services.catalog(),
        }
    }

    fn query(&self) -> Option<Arc<LegacyQueryEngineAdapter>> {
        match self {
            Self::CatalogOnly { .. } => None,
            Self::CatalogAndQuery { services } => Some(services.query()),
        }
    }

    fn runtime(&self) -> Arc<LegacyRuntimeRegistryAdapter> {
        match self {
            Self::CatalogOnly { runtime, .. } => Arc::clone(runtime),
            Self::CatalogAndQuery { services } => services.runtime(),
        }
    }

    fn activate(&self) -> Result<(), ServerError> {
        match self {
            Self::CatalogOnly { runtime, .. } => runtime.activate(),
            Self::CatalogAndQuery { services } => services.activate(),
        }
    }

    fn begin_shutdown(&self) {
        match self {
            Self::CatalogOnly { runtime, .. } => runtime.begin_shutdown(),
            Self::CatalogAndQuery { services } => services.begin_shutdown(),
        }
    }

    fn health(&self, admission_open: bool, master_running: bool) -> LegacyServerHealth {
        match self {
            Self::CatalogOnly { .. } => {
                LegacyServerHealth::phase_one(admission_open, master_running, false)
            }
            Self::CatalogAndQuery { services } => services.health(),
        }
    }
}

/// Owning single-process runtime for legacy master, Gateway, and service adapters.
pub struct LegacyServerRuntime {
    control_endpoint: ControlEndpoint,
    runtime_config: ZippyConfig,
    shutdown_timeout: Duration,
    master: MasterServer,
    master_listener: Option<MasterListenerRuntime>,
    gateway: Option<GatewayServer>,
    gateway_endpoint: Option<String>,
    services: LegacyServiceSet,
    readiness: LegacyServerReadiness,
    shutdown_outcome: Option<LegacyServerShutdownOutcome>,
    native_storage: Option<NativeStorageRuntime>,
    legacy_catalog_shadow: Option<LegacyCatalogShadowWorker>,
    legacy_catalog_migration: LegacyCatalogMigrationStatus,
    data_directory: Option<Arc<OwnedDataDirectory>>,
}

impl LegacyServerRuntime {
    /// Starts all configured listeners, verifies dependencies, and opens adapter admission.
    pub fn start(config: LegacyServerRuntimeConfig) -> Result<Self, ServerError> {
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

        let native_storage_configured = runtime_config.server.native_storage_enabled;
        let legacy_catalog_migration = prepare_legacy_catalog_migration(&runtime_config, &master);
        let mut native_storage = start_native_storage_if_enabled(
            &runtime_config,
            &data_directory,
            startup_deadline,
            legacy_catalog_migration,
        )?;
        let mut legacy_catalog_shadow = start_legacy_catalog_shadow_if_enabled(
            &runtime_config,
            &master,
            native_storage.as_ref(),
        );
        let mut master_listener = match start_master_listener(
            &master,
            &control_endpoint,
            startup_deadline,
            Arc::clone(&data_directory),
        ) {
            Ok(listener) => Some(listener),
            Err(error) => {
                let mut no_listener = None;
                let mut no_gateway = None;
                let rollback = rollback_startup(
                    &master,
                    &mut no_listener,
                    &mut no_gateway,
                    &mut legacy_catalog_shadow,
                    &mut native_storage,
                    None,
                    shutdown_timeout,
                );
                return Err(with_startup_rollback(error, rollback));
            }
        };
        if let Err(error) = remaining_before(startup_deadline, "start gateway") {
            let mut no_gateway = None;
            let rollback = rollback_startup(
                &master,
                &mut master_listener,
                &mut no_gateway,
                &mut legacy_catalog_shadow,
                &mut native_storage,
                None,
                shutdown_timeout,
            );
            return Err(with_startup_rollback(error, rollback));
        }
        let persisted_read_snapshot_source = match gateway_persisted_read_snapshot_source(
            &runtime_config,
            native_storage.as_ref(),
        ) {
            Ok(source) => source,
            Err(error) => {
                let mut no_gateway = None;
                let rollback = rollback_startup(
                    &master,
                    &mut master_listener,
                    &mut no_gateway,
                    &mut legacy_catalog_shadow,
                    &mut native_storage,
                    None,
                    shutdown_timeout,
                );
                return Err(with_startup_rollback(error, rollback));
            }
        };
        let mut gateway = match start_legacy_gateway_with_persisted_read_snapshot_source_if_enabled(
            &runtime_config,
            &control_endpoint,
            persisted_read_snapshot_source,
        ) {
            Ok(gateway) => gateway,
            Err(error) => {
                let mut no_gateway = None;
                let rollback = rollback_startup(
                    &master,
                    &mut master_listener,
                    &mut no_gateway,
                    &mut legacy_catalog_shadow,
                    &mut native_storage,
                    None,
                    shutdown_timeout,
                );
                return Err(with_startup_rollback(
                    map_core_error("bind gateway listener", error),
                    rollback,
                ));
            }
        };
        let gateway_configured = gateway.is_some();
        let gateway_endpoint = gateway
            .as_ref()
            .map(|gateway| gateway.endpoint().to_string());

        let probe_timeout = match remaining_before(startup_deadline, "probe master dependency") {
            Ok(timeout) => timeout,
            Err(error) => {
                let rollback = rollback_startup(
                    &master,
                    &mut master_listener,
                    &mut gateway,
                    &mut legacy_catalog_shadow,
                    &mut native_storage,
                    None,
                    shutdown_timeout,
                );
                return Err(with_startup_rollback(error, rollback));
            }
        };
        let dependency_probe = match gateway.as_ref() {
            Some(gateway) => gateway.probe_master_dependency(probe_timeout),
            None => probe_master_dependency(&control_endpoint, probe_timeout),
        };
        if let Err(error) = dependency_probe {
            let rollback = rollback_startup(
                &master,
                &mut master_listener,
                &mut gateway,
                &mut legacy_catalog_shadow,
                &mut native_storage,
                None,
                shutdown_timeout,
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
                &master,
                &mut master_listener,
                &mut gateway,
                &mut legacy_catalog_shadow,
                &mut native_storage,
                None,
                shutdown_timeout,
            );
            return Err(with_startup_rollback(
                ServerError::new(
                    ErrorCode::Unavailable,
                    "master listener exited during server startup",
                ),
                rollback,
            ));
        }

        let services = match gateway.as_ref() {
            Some(gateway_server) => match LegacyServerServices::new(&master, gateway_server) {
                Ok(services) => LegacyServiceSet::CatalogAndQuery { services },
                Err(error) => {
                    let rollback = rollback_startup(
                        &master,
                        &mut master_listener,
                        &mut gateway,
                        &mut legacy_catalog_shadow,
                        &mut native_storage,
                        None,
                        shutdown_timeout,
                    );
                    return Err(with_startup_rollback(
                        with_stage("compose legacy services", error),
                        rollback,
                    ));
                }
            },
            None => LegacyServiceSet::CatalogOnly {
                catalog: Arc::new(master.legacy_catalog_adapter()),
                runtime: Arc::new(master.legacy_runtime_registry_adapter_inactive()),
            },
        };

        if let Err(error) = services.activate() {
            let rollback = rollback_startup(
                &master,
                &mut master_listener,
                &mut gateway,
                &mut legacy_catalog_shadow,
                &mut native_storage,
                Some(&services),
                shutdown_timeout,
            );
            return Err(with_startup_rollback(
                with_stage("activate legacy services", error),
                rollback,
            ));
        }
        if let Err(error) = remaining_before(startup_deadline, "publish server readiness") {
            let rollback = rollback_startup(
                &master,
                &mut master_listener,
                &mut gateway,
                &mut legacy_catalog_shadow,
                &mut native_storage,
                Some(&services),
                shutdown_timeout,
            );
            return Err(with_startup_rollback(error, rollback));
        }

        let readiness = LegacyServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            gateway_configured,
            gateway_listener_ready: gateway_configured,
            gateway_master_reachable: gateway_configured,
            native_storage_configured,
            native_catalog_ready: native_storage_configured,
            native_current_snapshot_files_ready: native_storage_configured,
            storage_reconciliation_complete: native_storage_configured,
            commit_recovery_complete: native_storage_configured,
        };
        tracing::info!(
            component = "server",
            event = "server_ready",
            status = "ready",
            control_endpoint = control_endpoint.display_string(),
            gateway_endpoint = gateway_endpoint.as_deref().unwrap_or("disabled"),
            mode = if gateway_configured {
                "catalog_and_query"
            } else {
                "catalog_only"
            },
            "zippy server runtime is ready"
        );
        let legacy_catalog_migration = native_storage
            .as_ref()
            .map(NativeStorageRuntime::legacy_catalog_migration_status)
            .cloned()
            .unwrap_or_else(LegacyCatalogMigrationStatus::disabled);

        Ok(Self {
            control_endpoint,
            runtime_config,
            shutdown_timeout,
            master,
            master_listener,
            gateway,
            gateway_endpoint,
            services,
            readiness,
            shutdown_outcome: None,
            native_storage,
            legacy_catalog_shadow,
            legacy_catalog_migration,
            data_directory: Some(data_directory),
        })
    }

    /// Returns the effective master control endpoint.
    #[must_use]
    pub fn control_endpoint(&self) -> &ControlEndpoint {
        &self.control_endpoint
    }

    /// Returns the effective Gateway listener endpoint when configured.
    #[must_use]
    pub fn gateway_endpoint(&self) -> Option<&str> {
        self.gateway_endpoint.as_deref()
    }

    /// Returns a point-in-time Gateway metrics snapshot when the listener is enabled.
    ///
    /// In `sqlite_shadow` mode this includes bounded persisted-read comparison counters and the
    /// worker lifecycle state used by readiness diagnostics and the operations UI.
    #[must_use]
    pub fn gateway_metrics(&self) -> Option<serde_json::Value> {
        self.gateway.as_ref().map(GatewayServer::metrics)
    }

    /// Returns the structured legacy-to-SQLite Catalog startup migration status.
    #[must_use]
    pub const fn legacy_catalog_migration_status(&self) -> &LegacyCatalogMigrationStatus {
        &self.legacy_catalog_migration
    }

    /// Returns bounded migration counters suitable for status and metrics endpoints.
    #[must_use]
    pub fn legacy_catalog_migration_metrics(&self) -> serde_json::Value {
        self.legacy_catalog_migration.metrics()
    }

    /// Returns the validated runtime configuration shared by master and Gateway.
    #[must_use]
    pub fn runtime_config(&self) -> &ZippyConfig {
        &self.runtime_config
    }

    /// Returns the process-owned managed data directory while the runtime is active.
    #[must_use]
    pub fn data_directory(&self) -> Option<&OwnedDataDirectory> {
        self.data_directory.as_deref()
    }

    /// Returns the final startup reconciliation evidence when native storage is enabled.
    #[must_use]
    pub fn native_storage_reconciliation_report(
        &self,
    ) -> Option<&zippy_storage::StartupOrphanReconciliationReport> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::reconciliation_report)
    }

    /// Returns Catalog-to-physical verification evidence when native storage is enabled.
    #[must_use]
    pub fn native_storage_catalog_file_verification_report(
        &self,
    ) -> Option<&zippy_storage::StartupCatalogFileVerificationReport> {
        self.native_storage
            .as_ref()
            .map(NativeStorageRuntime::catalog_file_verification_report)
    }

    /// Returns the active compatibility mode.
    #[must_use]
    pub const fn mode(&self) -> LegacyServerMode {
        if self.readiness.gateway_configured {
            LegacyServerMode::CatalogAndQuery
        } else {
            LegacyServerMode::CatalogOnly
        }
    }

    /// Returns the catalog adapter in both full and catalog-only modes.
    #[must_use]
    pub fn catalog(&self) -> Arc<LegacyCatalogAdapter> {
        self.services.catalog()
    }

    /// Returns the query adapter only when Gateway is configured and dependency-ready.
    #[must_use]
    pub fn query(&self) -> Option<Arc<LegacyQueryEngineAdapter>> {
        self.services.query()
    }

    /// Returns the boot-scoped runtime registry adapter in both compatibility modes.
    #[must_use]
    pub fn runtime_registry(&self) -> Arc<LegacyRuntimeRegistryAdapter> {
        self.services.runtime()
    }

    /// Returns concrete listener facts and capability-aware service health.
    #[must_use]
    pub fn health(&self) -> LegacyServerRuntimeHealth {
        let mut readiness = self.readiness;
        readiness.master_listener_ready &= self
            .master_listener
            .as_ref()
            .is_some_and(|listener| !listener.is_finished());
        apply_native_storage_runtime_health(
            &mut readiness,
            self.native_storage
                .as_ref()
                .map(NativeStorageRuntime::is_healthy),
        );
        let mut services = self.services.health(
            readiness.admission_open,
            self.master.is_running() && readiness.master_listener_ready,
        );
        if readiness.gateway_configured && !services.gateway_facade_available {
            readiness.gateway_listener_ready = false;
            readiness.gateway_master_reachable = false;
        }
        services.end_to_end_liveness = Some(readiness.is_ready());
        LegacyServerRuntimeHealth {
            mode: self.mode(),
            readiness,
            services,
            legacy_catalog_migration: self.legacy_catalog_migration.clone(),
            legacy_catalog_shadow: self
                .legacy_catalog_shadow
                .as_ref()
                .map(LegacyCatalogShadowWorker::status),
            catalog_runtime_snapshot_pins: self
                .native_storage
                .as_ref()
                .and_then(|storage| storage.runtime_snapshot_pin_status().ok()),
        }
    }

    fn terminal_failure(&self) -> Option<ServerError> {
        self.native_storage
            .as_ref()
            .and_then(NativeStorageRuntime::terminal_failure)
    }

    /// Irreversibly closes adapter admission before component shutdown begins.
    pub fn begin_shutdown(&mut self) {
        if !self.readiness.admission_open {
            return;
        }
        self.readiness.admission_open = false;
        self.services.begin_shutdown();
    }

    /// Seals primary mutations, drains the shadow, then stops Master and native storage.
    pub fn shutdown(&mut self) -> LegacyServerShutdownOutcome {
        if let Some(outcome) = &self.shutdown_outcome {
            return outcome.clone();
        }
        self.begin_shutdown();

        let (deadline, budget_error) = match Instant::now().checked_add(self.shutdown_timeout) {
            Some(deadline) => (deadline, None),
            None => (
                Instant::now(),
                Some(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "server shutdown timeout exceeds monotonic clock range",
                )),
            ),
        };

        let (gateway_outcome, gateway_error) = match self.gateway.take() {
            Some(gateway) => match gateway
                .stop_gracefully_with_timeout(deadline.saturating_duration_since(Instant::now()))
            {
                Ok(outcome) => (Some(outcome), None),
                Err(error) => (None, Some(map_core_error("stop gateway", error))),
            },
            None => (None, None),
        };
        self.readiness.gateway_listener_ready = false;
        self.readiness.gateway_master_reachable = false;

        let graceful = self.master.drain_registered_processes_for_shutdown(
            deadline.saturating_duration_since(Instant::now()),
        );
        let seal_error = self
            .master
            .seal_legacy_catalog_commit_events(deadline.saturating_duration_since(Instant::now()))
            .err()
            .map(|error| {
                ServerError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("seal legacy Catalog commit events error=[{error}]"),
                )
            });
        let legacy_catalog_shadow_outcome = self
            .legacy_catalog_shadow
            .as_mut()
            .map(|shadow| shadow.shutdown_until(deadline));
        if legacy_catalog_shadow_outcome
            .as_ref()
            .is_some_and(LegacyCatalogShadowShutdownOutcome::is_complete)
        {
            self.legacy_catalog_shadow.take();
        }
        let shadow_joined = !matches!(
            legacy_catalog_shadow_outcome.as_ref(),
            Some(&LegacyCatalogShadowShutdownOutcome::TimedOut)
        );
        let listener_error = if shadow_joined && seal_error.is_none() {
            self.master.shutdown();
            self.readiness.master_listener_ready = false;
            finish_master_listener_until(&mut self.master_listener, deadline).err()
        } else {
            None
        };
        let native_storage_outcome =
            if shadow_joined && seal_error.is_none() && listener_error.is_none() {
                let outcome = self
                    .native_storage
                    .as_mut()
                    .map(|storage| storage.shutdown_until(deadline));
                if outcome
                    .as_ref()
                    .is_some_and(NativeStorageShutdownOutcome::is_complete)
                {
                    self.native_storage.take();
                }
                outcome
            } else {
                None
            };
        let master_error = combine_optional_errors(
            budget_error,
            combine_optional_errors(seal_error, listener_error),
        );
        let outcome = LegacyServerShutdownOutcome {
            timed_out_processes: graceful.timed_out_processes,
            gateway_error,
            gateway_outcome,
            master_error,
            legacy_catalog_shadow_outcome,
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
            gateway_error = ?outcome.gateway_error,
            gateway_outcome = ?outcome.gateway_outcome,
            legacy_catalog_shadow_outcome = ?outcome.legacy_catalog_shadow_outcome,
            native_storage_outcome = ?outcome.native_storage_outcome,
            master_error = ?outcome.master_error,
            "zippy server runtime stopped"
        );
        if outcome.is_clean() {
            self.data_directory.take();
        }
        let retryable_ordered_cleanup = self.master.is_running()
            || matches!(
                outcome.legacy_catalog_shadow_outcome.as_ref(),
                Some(&LegacyCatalogShadowShutdownOutcome::TimedOut)
            )
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
    readiness: &mut LegacyServerReadiness,
    native_storage_healthy: Option<bool>,
) {
    if readiness.native_storage_configured && native_storage_healthy != Some(true) {
        readiness.native_catalog_ready = false;
        readiness.native_current_snapshot_files_ready = false;
    }
}

impl Drop for LegacyServerRuntime {
    fn drop(&mut self) {
        let shutdown_was_attempted = self.shutdown_outcome.is_some();
        let outcome = self.shutdown();
        if !shutdown_was_attempted && !outcome.is_clean() {
            tracing::warn!(
                component = "server",
                event = "server_drop_incomplete",
                timed_out_processes = ?outcome.timed_out_processes,
                gateway_error = ?outcome.gateway_error,
                gateway_outcome = ?outcome.gateway_outcome,
                native_storage_outcome = ?outcome.native_storage_outcome,
                master_error = ?outcome.master_error,
                "zippy server runtime drop completed with cleanup errors"
            );
        }
    }
}

fn start_native_storage_if_enabled(
    runtime_config: &ZippyConfig,
    data_directory: &Arc<OwnedDataDirectory>,
    startup_deadline: Instant,
    legacy_catalog_migration: LegacyCatalogMigrationStartup,
) -> Result<Option<NativeStorageRuntime>, ServerError> {
    if !runtime_config.server.native_storage_enabled {
        return Ok(None);
    }
    remaining_before(startup_deadline, "start native storage")?;
    let persist_controller = PersistControllerConfig::new().with_max_parallel_writers(
        runtime_config
            .runtime
            .max_parallelism
            .min(MAX_PERSIST_PARALLEL_WRITERS),
    );
    let runtime_snapshot_pins = runtime_config.catalog.runtime_snapshot_pins;
    let catalog = SqliteCatalogConfig::new(data_directory).with_runtime_snapshot_pin_limits(
        runtime_snapshot_pins.max_active_pins,
        runtime_snapshot_pins.max_active_tables,
    );
    let native_storage = NativeStorageRuntimeConfig::new(data_directory)
        .with_catalog_config(catalog)?
        .with_persist_controller_config(persist_controller)
        .with_legacy_catalog_migration(legacy_catalog_migration);
    match NativeStorageRuntime::start_until(native_storage, startup_deadline) {
        NativeStorageStartOutcome::Ready(runtime) => Ok(Some(runtime)),
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

fn prepare_legacy_catalog_migration(
    runtime_config: &ZippyConfig,
    master: &MasterServer,
) -> LegacyCatalogMigrationStartup {
    if !runtime_config.server.native_storage_enabled
        || runtime_config.catalog.backend != ZippyCatalogBackend::SqliteShadow
    {
        return LegacyCatalogMigrationStartup::Disabled;
    }

    let snapshot = match master.legacy_catalog_adapter().capture_migration_snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return LegacyCatalogMigrationStartup::Degraded(
                LegacyCatalogMigrationDegradation::from_error(
                    LegacyCatalogMigrationDegradedReason::CaptureFailed,
                    &error,
                ),
            );
        }
    };
    match plan_legacy_catalog_migration(runtime_config, &snapshot) {
        Ok(plan) => LegacyCatalogMigrationStartup::Planned(plan),
        Err(error) => {
            LegacyCatalogMigrationStartup::Degraded(LegacyCatalogMigrationDegradation::from_error(
                LegacyCatalogMigrationDegradedReason::PlanningFailed,
                &error,
            ))
        }
    }
}

fn start_legacy_catalog_shadow_if_enabled(
    runtime_config: &ZippyConfig,
    master: &MasterServer,
    native_storage: Option<&NativeStorageRuntime>,
) -> Option<LegacyCatalogShadowWorker> {
    if runtime_config.catalog.backend != ZippyCatalogBackend::SqliteShadow {
        return None;
    }
    let Some(native_storage) = native_storage else {
        tracing::warn!(
            component = "legacy_catalog_shadow",
            event = "legacy_catalog_shadow_start_blocked",
            reason = "native_storage_unavailable",
            "legacy Catalog command shadow remains disabled"
        );
        return None;
    };
    let trusted_fallback_root =
        match resolve_legacy_fallback_root(&runtime_config.table.persist.data_dir) {
            Ok(root) => Some(root),
            Err(error) => {
                tracing::warn!(
                    component = "legacy_catalog_shadow",
                    event = "legacy_catalog_shadow_fallback_root_unavailable",
                    error_code = %error.code(),
                    "legacy Catalog shadow will fail closed for files without a declared root"
                );
                None
            }
        };
    let configured = &runtime_config.catalog.shadow;
    let config = LegacyCatalogShadowWorkerConfig {
        queue_capacity: configured.queue_capacity,
        max_files_per_table: configured.max_files_per_comparison,
        max_metadata_bytes_per_table: configured.max_metadata_bytes_per_comparison,
        operation_timeout: Duration::from_millis(configured.comparison_timeout_ms),
        trusted_fallback_root,
    };
    match LegacyCatalogShadowWorker::install_and_start(
        master,
        native_storage.catalog_read_handle(),
        native_storage.catalog_legacy_reconcile_handle(),
        config,
    ) {
        Ok(worker) => Some(worker),
        Err(error) => {
            tracing::warn!(
                component = "legacy_catalog_shadow",
                event = "legacy_catalog_shadow_start_failed",
                error_code = %error.code(),
                "legacy Catalog command shadow failed to start; primary legacy authority remains active"
            );
            None
        }
    }
}

fn plan_legacy_catalog_migration(
    runtime_config: &ZippyConfig,
    snapshot: &zippy_master::legacy_catalog::LegacyCatalogMigrationSnapshot,
) -> Result<LegacyCatalogMetadataImportPlan, ServerError> {
    let needs_fallback = snapshot.tables().iter().any(|table| {
        table
            .migration_files()
            .iter()
            .any(|file| file.persist_data_root().is_none())
    });
    if !needs_fallback {
        return plan_legacy_catalog_metadata_import(snapshot);
    }
    let fallback_root = resolve_legacy_fallback_root(&runtime_config.table.persist.data_dir)?;
    LegacyCatalogMetadataImportPlan::with_trusted_fallback_root(snapshot, &fallback_root)
}

fn resolve_legacy_fallback_root(configured: &str) -> Result<PathBuf, ServerError> {
    let canonical = fs::canonicalize(configured).map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "legacy fallback data root cannot be resolved error_kind=[{:?}]",
                error.kind()
            ),
        )
        .with_retryable(false)
    })?;
    if canonical == Path::new("/") {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy fallback data root cannot be the filesystem root",
        )
        .with_retryable(false));
    }
    let metadata = fs::metadata(&canonical).map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "legacy fallback data root cannot be inspected error_kind=[{:?}]",
                error.kind()
            ),
        )
        .with_retryable(false)
    })?;
    if !metadata.is_dir() {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy fallback data root must resolve to a directory",
        )
        .with_retryable(false));
    }
    if canonical.to_str().is_none() {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy fallback data root must be valid UTF-8",
        )
        .with_retryable(false));
    }
    Ok(canonical)
}

fn gateway_persisted_read_snapshot_source(
    runtime_config: &ZippyConfig,
    native_storage: Option<&NativeStorageRuntime>,
) -> Result<Arc<dyn PersistedReadSnapshotSource>, ServerError> {
    let legacy: Arc<dyn PersistedReadSnapshotSource> = Arc::new(LegacyPersistedReadSnapshotSource);
    if !runtime_config.gateway.enabled
        || runtime_config.catalog.backend == ZippyCatalogBackend::Legacy
    {
        return Ok(legacy);
    }

    match runtime_config.catalog.backend {
        ZippyCatalogBackend::Legacy => Ok(legacy),
        ZippyCatalogBackend::SqliteShadow => {
            let native_storage = native_storage.ok_or_else(|| {
                ServerError::new(
                    ErrorCode::InvalidArgument,
                    "catalog sqlite_shadow requires a running native storage runtime",
                )
            })?;
            let configured = &runtime_config.catalog.shadow;
            let shadow = Arc::new(
                SqliteCatalogPersistedReadShadowSource::new(
                    native_storage.catalog_read_handle(),
                    configured.max_files_per_comparison,
                    configured.max_metadata_bytes_per_comparison,
                )
                .map_err(|error| map_core_error("configure SQLite Catalog shadow source", error))?,
            );
            let source = ShadowPersistedReadSnapshotSource::new(
                legacy,
                shadow,
                PersistedReadShadowConfig {
                    queue_capacity: configured.queue_capacity,
                    metadata_budget_bytes: configured.metadata_budget_bytes,
                    max_files_per_comparison: configured.max_files_per_comparison,
                    max_metadata_bytes_per_comparison: configured.max_metadata_bytes_per_comparison,
                    comparison_timeout_ms: configured.comparison_timeout_ms,
                    dedup_capacity: configured.dedup_capacity,
                },
            )
            .map_err(|error| map_core_error("configure gateway persisted-read shadow", error))?;
            Ok(Arc::new(source))
        }
        ZippyCatalogBackend::Sqlite => Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog sqlite authority is unavailable before migration cutover",
        )),
    }
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

fn rollback_startup(
    master: &MasterServer,
    master_listener: &mut Option<MasterListenerRuntime>,
    gateway: &mut Option<GatewayServer>,
    legacy_catalog_shadow: &mut Option<LegacyCatalogShadowWorker>,
    native_storage: &mut Option<NativeStorageRuntime>,
    services: Option<&LegacyServiceSet>,
    shutdown_timeout: Duration,
) -> LegacyServerShutdownOutcome {
    let (deadline, budget_error) = match Instant::now().checked_add(shutdown_timeout) {
        Some(deadline) => (deadline, None),
        None => (
            Instant::now(),
            Some(ServerError::new(
                ErrorCode::InvalidArgument,
                "startup rollback timeout exceeds monotonic clock range",
            )),
        ),
    };
    if let Some(services) = services {
        services.begin_shutdown();
    }
    let (gateway_outcome, gateway_error) = match gateway.take() {
        Some(gateway) => match gateway
            .stop_gracefully_with_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            Ok(outcome) if outcome.is_clean() => (Some(outcome), None),
            Ok(outcome) => {
                tracing::warn!(
                    component = "server",
                    event = "server_start_rollback_gateway_incomplete",
                    outcome = ?outcome,
                    "gateway cleanup was incomplete during startup rollback"
                );
                (Some(outcome), None)
            }
            Err(error) => {
                tracing::warn!(
                    component = "server",
                    event = "server_start_rollback_gateway_error",
                    error = %error,
                    "failed to release Gateway master lease during startup rollback"
                );
                (None, Some(map_core_error("rollback Gateway", error)))
            }
        },
        None => (None, None),
    };
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
    let seal_error = master
        .seal_legacy_catalog_commit_events(deadline.saturating_duration_since(Instant::now()))
        .err()
        .map(|error| {
            ServerError::new(
                ErrorCode::DeadlineExceeded,
                format!("seal legacy Catalog commit events during rollback error=[{error}]"),
            )
        });
    let legacy_catalog_shadow_outcome = legacy_catalog_shadow
        .as_mut()
        .map(|shadow| shadow.shutdown_until(deadline));
    if legacy_catalog_shadow_outcome
        .as_ref()
        .is_some_and(LegacyCatalogShadowShutdownOutcome::is_complete)
    {
        legacy_catalog_shadow.take();
    }
    let shadow_joined = !matches!(
        legacy_catalog_shadow_outcome.as_ref(),
        Some(&LegacyCatalogShadowShutdownOutcome::TimedOut)
    );
    let listener_error = if shadow_joined && seal_error.is_none() {
        master.shutdown();
        finish_master_listener_until(master_listener, deadline).err()
    } else {
        None
    };
    if let Some(error) = &listener_error {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_master_error",
            error = %error,
            "master listener cleanup failed during startup rollback"
        );
    }
    let native_storage_outcome =
        if shadow_joined && seal_error.is_none() && listener_error.is_none() {
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
    LegacyServerShutdownOutcome {
        timed_out_processes: graceful.timed_out_processes,
        gateway_error,
        gateway_outcome,
        master_error: combine_optional_errors(
            budget_error,
            combine_optional_errors(seal_error, listener_error),
        ),
        legacy_catalog_shadow_outcome,
        native_storage_outcome,
    }
}

fn finish_master_listener_until(
    runtime: &mut Option<MasterListenerRuntime>,
    deadline: Instant,
) -> Result<(), ServerError> {
    let Some(mut runtime) = runtime.take() else {
        return Ok(());
    };
    match runtime
        .completed
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
    {
        Ok(result) => {
            join_master_listener_thread_until(&mut runtime.join_handle, deadline)?;
            result.map_err(|error| map_core_error("join master listener", error))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => Err(ServerError::new(
            ErrorCode::DeadlineExceeded,
            "master listener cleanup exceeded the server shutdown deadline",
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            join_master_listener_thread_until(&mut runtime.join_handle, deadline)?;
            Err(ServerError::new(
                ErrorCode::Internal,
                "master listener exited without a completion result",
            ))
        }
    }
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
    rollback: LegacyServerShutdownOutcome,
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
    ServerError::new(code, format!("{stage}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::net::TcpListener;
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
    fn legacy_fallback_root_requires_an_existing_scoped_directory() {
        let temp = tempfile::tempdir().unwrap();
        let resolved = resolve_legacy_fallback_root(temp.path().to_str().unwrap()).unwrap();
        assert_eq!(resolved, fs::canonicalize(temp.path()).unwrap());

        let missing = temp.path().join("missing");
        let error = resolve_legacy_fallback_root(missing.to_str().unwrap()).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!error.message().contains(missing.to_str().unwrap()));

        let file = temp.path().join("not-a-directory");
        fs::write(&file, b"not a root").unwrap();
        let error = resolve_legacy_fallback_root(file.to_str().unwrap()).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);

        let error = resolve_legacy_fallback_root("/").unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn readiness_treats_disabled_gateway_as_not_applicable() {
        let readiness = LegacyServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            gateway_configured: false,
            gateway_listener_ready: false,
            gateway_master_reachable: false,
            native_storage_configured: false,
            native_catalog_ready: false,
            native_current_snapshot_files_ready: false,
            storage_reconciliation_complete: false,
            commit_recovery_complete: false,
        };

        assert!(readiness.is_ready());
    }

    #[test]
    fn readiness_requires_gateway_listener_and_dependency_when_configured() {
        let mut readiness = LegacyServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            gateway_configured: true,
            gateway_listener_ready: true,
            gateway_master_reachable: false,
            native_storage_configured: false,
            native_catalog_ready: false,
            native_current_snapshot_files_ready: false,
            storage_reconciliation_complete: false,
            commit_recovery_complete: false,
        };

        assert!(!readiness.is_ready());
        readiness.gateway_master_reachable = true;
        assert!(readiness.is_ready());
        readiness.admission_open = false;
        assert!(!readiness.is_ready());
    }

    #[test]
    fn readiness_requires_every_configured_native_storage_gate() {
        let mut readiness = LegacyServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            gateway_configured: false,
            gateway_listener_ready: false,
            gateway_master_reachable: false,
            native_storage_configured: true,
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
    fn runtime_native_failure_revokes_dynamic_readiness() {
        let mut readiness = LegacyServerReadiness {
            admission_open: true,
            master_listener_ready: true,
            gateway_configured: false,
            gateway_listener_ready: false,
            gateway_master_reachable: false,
            native_storage_configured: true,
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
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("daemon.sock");
        let config_path = temp.path().join("zippy.toml");
        fs::write(
            &config_path,
            r#"
[master]
shutdown_timeout_ms = 1000
"#,
        )
        .unwrap();
        let master = MasterDaemonConfig::new(ControlEndpoint::Unix(socket_path.clone()))
            .with_config_path(config_path)
            .with_logging(runtime_test_log_dir(), "info", false);

        run_server_daemon_with_waiter(LegacyServerRuntimeConfig::new(master), |runtime| {
            assert!(runtime.health().readiness.is_ready());
            assert!(socket_path.exists());
            Ok("TEST")
        })
        .unwrap();

        assert!(!socket_path.exists());
        assert!(!socket_path.with_extension("sock.owner").exists());
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
token = "must-not-appear-in-ready-record"
"#,
        )
        .unwrap();
        let control_endpoint = ControlEndpoint::Unix(socket_path);
        write_test_starting_record(&ready_path, &control_endpoint, "launch-nonce-1");
        let master = MasterDaemonConfig::new(control_endpoint)
            .with_config_path(config_path)
            .with_logging(runtime_test_log_dir(), "info", false);
        let config =
            LegacyServerRuntimeConfig::new(master).with_ready_file(&ready_path, "launch-nonce-1");

        run_server_daemon_with_waiter(config, |runtime| {
            assert!(runtime.health().readiness.is_ready());
            let payload = fs::read_to_string(&ready_path).unwrap();
            let record: serde_json::Value = serde_json::from_str(&payload).unwrap();
            assert_eq!(record["schema_version"], SERVER_READY_SCHEMA_VERSION);
            assert_eq!(record["status"], "ready");
            assert_eq!(record["launch_nonce"], "launch-nonce-1");
            assert_eq!(record["pid"], std::process::id());
            assert_eq!(record["version"], python_dev_version());
            assert_eq!(record["mode"], "catalog_only");
            assert_eq!(
                record["launcher"]["state_file"],
                ready_path.display().to_string()
            );
            assert_eq!(record["launcher"]["webui"]["managed"], false);
            assert_eq!(record["health"]["readiness"]["admission_open"], true);
            assert!(!payload.contains("must-not-appear-in-ready-record"));
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
                    "native_storage_enabled = true\n\n",
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
        let config = LegacyServerRuntimeConfig::new(master)
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
        let config = LegacyServerRuntimeConfig::new(master).with_ready_file(&ready_path, "");

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
            LegacyServerRuntimeConfig::new(master).with_ready_file(&ready_path, "launch-nonce-2");

        let error = run_server_daemon_with_waiter(config, |_| Ok("TEST")).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(!socket_path.exists());
        assert!(ready_path.exists());
    }

    #[test]
    fn incomplete_shutdown_is_promoted_to_server_error() {
        let outcome = LegacyServerShutdownOutcome {
            timed_out_processes: vec!["process-1".to_string()],
            gateway_error: None,
            gateway_outcome: None,
            master_error: None,
            legacy_catalog_shadow_outcome: None,
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
        let shutdown = shutdown_outcome_error(&LegacyServerShutdownOutcome {
            native_storage_outcome: Some(NativeStorageShutdownOutcome::TimedOut),
            ..LegacyServerShutdownOutcome::default()
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
        let rollback = LegacyServerShutdownOutcome {
            timed_out_processes: vec!["process-rollback".to_string()],
            gateway_error: None,
            gateway_outcome: None,
            master_error: None,
            legacy_catalog_shadow_outcome: None,
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
            join_handle: Some(join_handle),
        });
        let started = Instant::now();

        finish_master_listener_until(&mut runtime, Instant::now() + Duration::from_millis(250))
            .unwrap();

        assert!(runtime.is_none());
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[cfg(unix)]
    #[test]
    fn zero_remaining_server_budget_does_not_fall_back_to_default_gateway_timeout() {
        let temp = tempfile::tempdir().unwrap();
        let socket_path = temp.path().join("zero-budget.sock");
        let config_path = temp.path().join("zippy.toml");
        let gateway_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let gateway_endpoint = gateway_listener.local_addr().unwrap();
        drop(gateway_listener);
        fs::write(
            &config_path,
            format!(
                r#"
[master]
shutdown_timeout_ms = 1000

[gateway]
enabled = true
endpoint = "{gateway_endpoint}"
token = "zero-budget-test"
"#,
            ),
        )
        .unwrap();
        let master = MasterDaemonConfig::new(ControlEndpoint::Unix(socket_path))
            .with_config_path(config_path)
            .with_logging(runtime_test_log_dir(), "info", false);
        let mut runtime = LegacyServerRuntime::start(LegacyServerRuntimeConfig::new(master))
            .expect("full runtime should start");
        runtime.shutdown_timeout = Duration::ZERO;
        let started = Instant::now();

        let outcome = runtime.shutdown();

        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(outcome
            .gateway_outcome
            .as_ref()
            .is_some_and(|gateway| gateway.deadline_exceeded));
        assert!(!outcome.is_clean());
    }
}
