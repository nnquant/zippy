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
use zippy_core::bus_protocol::GetConfigRequest;
use zippy_core::{
    current_log_snapshot, python_dev_version, resolve_control_endpoint,
    send_control_line_request_with_timeout, ControlEndpoint, ControlRequest, ControlResponse,
    OwnedDataDirectory, Result as CoreResult, ZippyConfig, ZippyError,
};
use zippy_gateway::{GatewayServer, GatewayShutdownOutcome};
use zippy_master::daemon::{
    prepare_master_runtime, start_legacy_gateway_if_enabled, MasterDaemonConfig,
};
use zippy_master::legacy_catalog::LegacyCatalogAdapter;
use zippy_master::legacy_runtime::LegacyRuntimeRegistryAdapter;
use zippy_master::server::MasterServer;
use zippy_storage::{PersistControllerConfig, MAX_PERSIST_PARALLEL_WRITERS};
use zippy_types::{ErrorCode, ServerError};

use crate::{
    LegacyQueryEngineAdapter, LegacyServerHealth, LegacyServerServices, NativeStorageRuntime,
    NativeStorageRuntimeConfig, NativeStorageShutdownOutcome, NativeStorageStartOutcome,
};

const DEFAULT_SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const SERVER_READY_SCHEMA_VERSION: u8 = 2;

/// Runs the server-owned legacy runtime until the process receives a shutdown signal.
///
/// On Unix, this function installs and owns the `SIGINT` and `SIGTERM` subscriptions before any
/// listener is started. After the first signal it closes adapter admission, stops Gateway before
/// master, and returns an error if any component fails to stop cleanly.
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
        run_server_daemon_with_waiter(config, move |_| {
            signals.forever().next().map(signal_name).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    "server signal iterator closed before shutdown",
                )
            })
        })
    }

    #[cfg(not(unix))]
    {
        run_server_daemon_with_waiter(config, |_| {
            let (sender, receiver) = mpsc::channel::<()>();
            let result = receiver.recv();
            drop(sender);
            result.map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("wait for server shutdown: {error}"),
                )
            })?;
            Ok("external")
        })
    }
}

fn run_server_daemon_with_waiter<F>(
    config: LegacyServerRuntimeConfig,
    wait_for_shutdown: F,
) -> Result<(), ServerError>
where
    F: FnOnce(&LegacyServerRuntime) -> Result<&'static str, ServerError>,
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
            return match shutdown_outcome_error(&outcome) {
                Some(shutdown_error) => Err(ServerError::new(
                    ErrorCode::Unavailable,
                    format!("{ready_error}; {shutdown_error}"),
                )),
                None => Err(ready_error),
            };
        }
    }
    let shutdown_signal = match wait_for_shutdown(&runtime) {
        Ok(signal) => signal,
        Err(wait_error) => {
            runtime.begin_shutdown();
            let outcome = runtime.shutdown();
            return match shutdown_outcome_error(&outcome) {
                Some(shutdown_error) => Err(ServerError::new(
                    ErrorCode::Unavailable,
                    format!("{wait_error}; {shutdown_error}"),
                )),
                None => Err(wait_error),
            };
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

    runtime.begin_shutdown();
    let outcome = runtime.shutdown();
    match shutdown_outcome_error(&outcome) {
        Some(error) => Err(error),
        None => Ok(()),
    }
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
             gateway_outcome=[{:?}] native_storage_outcome=[{:?}] master_error=[{}]",
            outcome.timed_out_processes.join(","),
            outcome
                .gateway_error
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            outcome.gateway_outcome,
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
    if record.schema_version != 1
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
        let mut native_storage =
            start_native_storage_if_enabled(&runtime_config, &data_directory, startup_deadline)?;
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
                &mut native_storage,
                None,
                shutdown_timeout,
            );
            return Err(with_startup_rollback(error, rollback));
        }
        let mut gateway = match start_legacy_gateway_if_enabled(&runtime_config, &control_endpoint)
        {
            Ok(gateway) => gateway,
            Err(error) => {
                let mut no_gateway = None;
                let rollback = rollback_startup(
                    &master,
                    &mut master_listener,
                    &mut no_gateway,
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
        }
    }

    /// Irreversibly closes adapter admission before component shutdown begins.
    pub fn begin_shutdown(&mut self) {
        if !self.readiness.admission_open {
            return;
        }
        self.readiness.admission_open = false;
        self.services.begin_shutdown();
        if let Some(native_storage) = self.native_storage.as_mut() {
            native_storage.begin_shutdown();
        }
    }

    /// Stops Gateway, native storage, and master in ownership order and reports every failure.
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

        let native_storage_outcome = self
            .native_storage
            .as_mut()
            .map(|storage| storage.shutdown_until(deadline));
        if native_storage_outcome
            .as_ref()
            .is_some_and(NativeStorageShutdownOutcome::is_complete)
        {
            self.native_storage.take();
        }

        let graceful = self
            .master
            .request_graceful_shutdown(deadline.saturating_duration_since(Instant::now()));
        self.readiness.master_listener_ready = false;
        let listener_error =
            finish_master_listener_until(&mut self.master_listener, deadline).err();
        let master_error = budget_error.or(listener_error);
        let outcome = LegacyServerShutdownOutcome {
            timed_out_processes: graceful.timed_out_processes,
            gateway_error,
            gateway_outcome,
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
            gateway_error = ?outcome.gateway_error,
            gateway_outcome = ?outcome.gateway_outcome,
            native_storage_outcome = ?outcome.native_storage_outcome,
            master_error = ?outcome.master_error,
            "zippy server runtime stopped"
        );
        if outcome.is_clean() {
            self.data_directory.take();
        }
        self.shutdown_outcome = Some(outcome.clone());
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
    match NativeStorageRuntime::start_until(
        NativeStorageRuntimeConfig::new(data_directory)
            .with_persist_controller_config(persist_controller),
        startup_deadline,
    ) {
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
    if let Some(native_storage) = native_storage.as_mut() {
        native_storage.begin_shutdown();
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
    let native_storage_outcome = native_storage
        .as_mut()
        .map(|storage| storage.shutdown_until(deadline));
    if native_storage_outcome
        .as_ref()
        .is_some_and(NativeStorageShutdownOutcome::is_complete)
    {
        native_storage.take();
    } else if let Some(outcome) = native_storage_outcome.as_ref() {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_native_storage_incomplete",
            outcome = ?outcome,
            "native storage cleanup was incomplete during startup rollback"
        );
    }
    let graceful =
        master.request_graceful_shutdown(deadline.saturating_duration_since(Instant::now()));
    if !graceful.timed_out_processes.is_empty() {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_timeout",
            timed_out_processes = ?graceful.timed_out_processes,
            "startup rollback timed out waiting for runtime processes"
        );
    }
    let listener_error = finish_master_listener_until(master_listener, deadline).err();
    if let Some(error) = &listener_error {
        tracing::warn!(
            component = "server",
            event = "server_start_rollback_master_error",
            error = %error,
            "master listener cleanup failed during startup rollback"
        );
    }
    LegacyServerShutdownOutcome {
        timed_out_processes: graceful.timed_out_processes,
        gateway_error,
        gateway_outcome,
        master_error: budget_error.or(listener_error),
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
    use std::fs;
    use std::net::TcpListener;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn runtime_test_log_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "zippy-server-runtime-unit-test-logs-{}",
            std::process::id()
        ))
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
            native_storage_outcome: None,
        };

        let error = shutdown_outcome_error(&outcome).unwrap();

        assert_eq!(error.code(), ErrorCode::DeadlineExceeded);
        assert!(error.message().contains("process-1"));
    }

    #[test]
    fn incomplete_startup_rollback_is_attached_to_the_startup_error() {
        let startup_error = ServerError::new(ErrorCode::Unavailable, "dependency probe failed");
        let rollback = LegacyServerShutdownOutcome {
            timed_out_processes: vec!["process-rollback".to_string()],
            gateway_error: None,
            gateway_outcome: None,
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
