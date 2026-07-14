use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::time::Instant;

#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::iterator::Signals;
use zippy_core::{
    default_control_endpoint, resolve_control_endpoint, ControlEndpoint, LogConfig,
    ManagedDataDirectory, OwnedDataDirectory, ZippyConfig, ZippyError,
};
use zippy_gateway::{
    GatewayServer, GatewayServerConfig, LegacyPersistedReadSnapshotSource,
    PersistedReadSnapshotSource,
};

use crate::server::MasterServer;
use crate::snapshot::SnapshotStore;

const LEGACY_MASTER_SNAPSHOT_FILE: &str = "legacy-master-registry.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterDaemonConfig {
    pub control_endpoint: ControlEndpoint,
    /// Whether the caller explicitly selected the endpoint, including the default value.
    pub control_endpoint_explicit: bool,
    pub config_path: Option<PathBuf>,
    pub log_dir: PathBuf,
    pub log_level: Option<String>,
    pub console_log: bool,
}

/// Validated legacy master components prepared without starting any listener.
#[derive(Debug)]
pub struct PreparedMasterRuntime {
    control_endpoint: ControlEndpoint,
    runtime_config: ZippyConfig,
    server: MasterServer,
    shutdown_timeout: Duration,
    data_directory: Arc<OwnedDataDirectory>,
}

impl PreparedMasterRuntime {
    /// Returns the effective master control endpoint.
    pub fn control_endpoint(&self) -> &ControlEndpoint {
        &self.control_endpoint
    }

    /// Returns the merged and validated runtime configuration.
    pub fn runtime_config(&self) -> &ZippyConfig {
        &self.runtime_config
    }

    /// Returns the prepared master service.
    pub fn server(&self) -> &MasterServer {
        &self.server
    }

    /// Returns the configured graceful shutdown timeout.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the process-owned managed server data directory.
    pub fn data_directory(&self) -> &OwnedDataDirectory {
        self.data_directory.as_ref()
    }

    /// Consumes the prepared runtime into its owning components.
    pub fn into_parts(
        self,
    ) -> (
        ControlEndpoint,
        ZippyConfig,
        MasterServer,
        Duration,
        Arc<OwnedDataDirectory>,
    ) {
        (
            self.control_endpoint,
            self.runtime_config,
            self.server,
            self.shutdown_timeout,
            self.data_directory,
        )
    }
}

impl MasterDaemonConfig {
    pub fn new(control_endpoint: impl Into<ControlEndpoint>) -> Self {
        Self {
            control_endpoint: control_endpoint.into(),
            control_endpoint_explicit: false,
            config_path: None,
            log_dir: PathBuf::from("logs"),
            log_level: None,
            console_log: true,
        }
    }

    pub fn with_config_path(mut self, config_path: impl AsRef<Path>) -> Self {
        self.config_path = Some(config_path.as_ref().to_path_buf());
        self
    }

    /// Prevents file or environment configuration from replacing the caller-selected endpoint.
    #[must_use]
    pub fn with_explicit_control_endpoint(mut self) -> Self {
        self.control_endpoint_explicit = true;
        self
    }

    pub fn with_logging(
        mut self,
        log_dir: impl AsRef<Path>,
        log_level: impl Into<String>,
        console_log: bool,
    ) -> Self {
        self.log_dir = log_dir.as_ref().to_path_buf();
        self.log_level = Some(log_level.into());
        self.console_log = console_log;
        self
    }
}

pub fn run_master_daemon(config: MasterDaemonConfig) -> zippy_core::Result<()> {
    let prepared = prepare_master_runtime(config)?;
    let gateway = Arc::new(Mutex::new(start_legacy_gateway_if_enabled(
        prepared.runtime_config(),
        prepared.control_endpoint(),
    )?));
    let control_endpoint = prepared.control_endpoint().clone();
    let control_endpoint_display = control_endpoint.display_string();
    let server = prepared.server().clone();
    install_shutdown_handlers(
        &control_endpoint,
        server.clone(),
        Arc::clone(&gateway),
        prepared.shutdown_timeout(),
    )?;

    if std::env::var_os("ZIPPY_MASTER_TEST_PAUSE_BEFORE_SERVE").is_some() {
        std::thread::sleep(Duration::from_millis(150));
    }

    if !server.is_running() {
        stop_legacy_gateway_after_master_exit(&gateway, prepared.shutdown_timeout())?;
        tracing::info!(
            component = "master",
            event = "master_stopped",
            status = "stopped",
            control_endpoint = control_endpoint_display.as_str(),
            "master stopped"
        );
        return Ok(());
    }

    let result = server.serve_endpoint_with_ready(&control_endpoint, None);
    let gateway_result =
        stop_legacy_gateway_after_master_exit(&gateway, prepared.shutdown_timeout());
    match (result, gateway_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
    }
}

fn stop_legacy_gateway_after_master_exit(
    gateway: &Arc<Mutex<Option<GatewayServer>>>,
    timeout: Duration,
) -> zippy_core::Result<()> {
    let Some(gateway) = gateway
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
    else {
        return Ok(());
    };
    match gateway.stop_gracefully_with_timeout(timeout) {
        Ok(outcome) if outcome.is_clean() => Ok(()),
        Ok(outcome) => {
            tracing::error!(
                component = "master",
                event = "legacy_gateway_cleanup_after_master_incomplete",
                status = "incomplete",
                outcome = ?outcome,
                "legacy Gateway cleanup after master exit was incomplete"
            );
            Err(ZippyError::Io {
                reason: format!("legacy Gateway cleanup was incomplete outcome=[{outcome:?}]"),
            })
        }
        Err(error) => {
            tracing::error!(
                component = "master",
                event = "legacy_gateway_cleanup_after_master_error",
                status = "error",
                error = %error,
                "legacy Gateway cleanup after master exit failed"
            );
            Err(error)
        }
    }
}

/// Loads configuration, logging, snapshot state, and master service without binding listeners.
pub fn prepare_master_runtime(
    config: MasterDaemonConfig,
) -> zippy_core::Result<PreparedMasterRuntime> {
    let MasterDaemonConfig {
        mut control_endpoint,
        control_endpoint_explicit,
        config_path,
        log_dir,
        log_level,
        console_log,
    } = config;
    let mut runtime_config = load_runtime_config(config_path.as_deref())?;
    if !control_endpoint_explicit && control_endpoint == default_control_endpoint() {
        if let Some(endpoint) = runtime_config.master_endpoint() {
            control_endpoint = resolve_control_endpoint(format!("tcp://{endpoint}"))?;
        }
    }
    if let Some(log_level) = log_level {
        runtime_config.log.level = log_level;
    }
    let effective_log_level = runtime_config.log.level.clone();

    let snapshot_dir = control_endpoint.snapshot_dir();
    let consider_endpoint_snapshot = runtime_config.server.data_dir.is_none();
    let import_endpoint_snapshot = runtime_config.server.import_legacy_endpoint_snapshot;
    if consider_endpoint_snapshot {
        std::fs::create_dir_all(&snapshot_dir).map_err(|error| ZippyError::Io {
            reason: format!(
                "create control endpoint state directory path=[{}] error=[{}]",
                snapshot_dir.display(),
                error
            ),
        })?;
    }
    let server_data_dir = runtime_config.server_data_dir(snapshot_dir.join("data"));
    let data_directory = Arc::new(
        OwnedDataDirectory::acquire_or_initialize(&server_data_dir).map_err(|error| {
            let reason = format!(
                "acquire managed server data directory path=[{}] error=[{}]",
                server_data_dir.display(),
                error
            );
            if matches!(error, zippy_core::DataDirectoryError::OwnershipHeld { .. }) {
                ZippyError::Conflict { reason }
            } else {
                ZippyError::Io { reason }
            }
        })?,
    );
    if let Some(socket_path) = control_endpoint.unix_path() {
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
                reason: format!(
                    "create control endpoint parent path=[{}] error=[{}]",
                    parent.display(),
                    error
                ),
            })?;
        }
    }
    let control_endpoint_display = control_endpoint.display_string();
    let (control_host, control_port) = control_endpoint_host_port(&control_endpoint);

    let snapshot = zippy_core::setup_log(LogConfig::new(
        "zippy-master",
        effective_log_level.clone(),
        &log_dir,
        console_log,
        true,
    ))?;

    tracing::info!(
        component = "master",
        event = "master_start",
        status = "starting",
        control_endpoint = control_endpoint_display.as_str(),
        host = control_host.as_str(),
        port = control_port.as_str(),
        log_dir = log_dir.display().to_string(),
        log_level = effective_log_level,
        server_data_dir = data_directory.directory().root().display().to_string(),
        server_data_owner_pid = data_directory.owner().pid(),
        table_row_capacity = runtime_config.table.row_capacity,
        table_persist_enabled = runtime_config.table.persist.enabled,
        table_persist_method = runtime_config.table.persist.method.as_str(),
        table_persist_data_dir = runtime_config.table.persist.data_dir.as_str(),
        max_parallelism = runtime_config.runtime.max_parallelism,
        query_parallelism = runtime_config.query_parallelism(),
        log_file = snapshot
            .file_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "starting zippy master"
    );

    let endpoint_snapshot_path = control_endpoint.legacy_snapshot_path();
    if consider_endpoint_snapshot
        && import_endpoint_snapshot
        && endpoint_snapshot_path
            .try_exists()
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "inspect endpoint legacy snapshot path=[{}] error=[{}]",
                    endpoint_snapshot_path.display(),
                    error
                ),
            })?
    {
        ensure_control_endpoint_offline(&control_endpoint)?;
    }
    let snapshot_path = prepare_legacy_snapshot_path(
        data_directory.as_ref(),
        &endpoint_snapshot_path,
        consider_endpoint_snapshot,
        import_endpoint_snapshot,
    )?;
    let shutdown_timeout = Duration::from_millis(runtime_config.master.shutdown_timeout_ms);

    let server = if snapshot_path.exists() {
        tracing::info!(
            component = "master",
            event = "snapshot_load_start",
            status = "starting",
            snapshot_path = snapshot_path.display().to_string(),
            "loading registry snapshot"
        );
        match MasterServer::from_snapshot_path(&snapshot_path) {
            Ok(server) => {
                tracing::info!(
                    component = "master",
                    event = "snapshot_load_success",
                    status = "success",
                    snapshot_path = snapshot_path.display().to_string(),
                    "loaded registry snapshot"
                );
                server.with_runtime_config_values(runtime_config.clone())
            }
            Err(error) => {
                tracing::error!(
                    component = "master",
                    event = "snapshot_load_failure",
                    status = "error",
                    snapshot_path = snapshot_path.display().to_string(),
                    error = %error,
                    "failed to load registry snapshot"
                );
                return Err(error);
            }
        }
    } else {
        MasterServer::with_runtime_config_and_config(
            Some(snapshot_path),
            Duration::from_secs(10),
            Duration::from_secs(2),
            runtime_config,
        )
    };
    Ok(PreparedMasterRuntime {
        control_endpoint,
        runtime_config: server.runtime_config().clone(),
        server,
        shutdown_timeout,
        data_directory,
    })
}

fn prepare_legacy_snapshot_path(
    data_directory: &OwnedDataDirectory,
    endpoint_snapshot_path: &Path,
    consider_endpoint_snapshot: bool,
    import_endpoint_snapshot: bool,
) -> zippy_core::Result<PathBuf> {
    let managed_snapshot_path = data_directory
        .directory()
        .path(ManagedDataDirectory::Catalog)
        .join(LEGACY_MASTER_SNAPSHOT_FILE);
    if managed_snapshot_path
        .try_exists()
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "inspect managed legacy snapshot path=[{}] error=[{}]",
                managed_snapshot_path.display(),
                error
            ),
        })?
    {
        return Ok(managed_snapshot_path);
    }
    if !consider_endpoint_snapshot {
        return Ok(managed_snapshot_path);
    }
    if !endpoint_snapshot_path
        .try_exists()
        .map_err(|error| ZippyError::Io {
            reason: format!(
                "inspect endpoint legacy snapshot path=[{}] error=[{}]",
                endpoint_snapshot_path.display(),
                error
            ),
        })?
    {
        return Ok(managed_snapshot_path);
    }
    if !import_endpoint_snapshot {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "legacy endpoint snapshot requires an explicit offline import source_path=[{}] set=[ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT=1] after stopping the old server",
                endpoint_snapshot_path.display()
            ),
        });
    }
    tracing::info!(
        component = "master",
        event = "legacy_snapshot_import_start",
        status = "starting",
        source_path = endpoint_snapshot_path.display().to_string(),
        target_path = managed_snapshot_path.display().to_string(),
        "importing endpoint-scoped legacy snapshot into managed data root"
    );
    let snapshot = SnapshotStore::load(endpoint_snapshot_path)?;
    SnapshotStore::write(&managed_snapshot_path, &snapshot)?;
    tracing::info!(
        component = "master",
        event = "legacy_snapshot_import_success",
        status = "success",
        source_path = endpoint_snapshot_path.display().to_string(),
        target_path = managed_snapshot_path.display().to_string(),
        "imported endpoint-scoped legacy snapshot into managed data root"
    );
    Ok(managed_snapshot_path)
}

fn ensure_control_endpoint_offline(endpoint: &ControlEndpoint) -> zippy_core::Result<()> {
    let active = match endpoint {
        #[cfg(unix)]
        ControlEndpoint::Unix(path) => match UnixStream::connect(path) {
            Ok(stream) => {
                drop(stream);
                true
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) =>
            {
                false
            }
            Err(error) => {
                return Err(ZippyError::Io {
                    reason: format!(
                        "verify legacy control endpoint is offline endpoint=[{}] error=[{}]",
                        endpoint.display_string(),
                        error
                    ),
                });
            }
        },
        #[cfg(not(unix))]
        ControlEndpoint::Unix(path) => {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "unix control endpoint is unsupported path=[{}]",
                    path.display()
                ),
            });
        }
        ControlEndpoint::Tcp(address) => {
            match TcpStream::connect_timeout(address, Duration::from_millis(200)) {
                Ok(stream) => {
                    drop(stream);
                    true
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::NotFound
                            | std::io::ErrorKind::AddrNotAvailable
                    ) =>
                {
                    false
                }
                Err(error) => {
                    return Err(ZippyError::Io {
                        reason: format!(
                            "verify legacy control endpoint is offline endpoint=[{}] error=[{}]",
                            endpoint.display_string(),
                            error
                        ),
                    });
                }
            }
        }
    };
    if active {
        return Err(ZippyError::Conflict {
            reason: format!(
                "legacy control endpoint is still active endpoint=[{}]",
                endpoint.display_string()
            ),
        });
    }
    Ok(())
}

fn control_endpoint_host_port(endpoint: &ControlEndpoint) -> (String, String) {
    match endpoint {
        ControlEndpoint::Tcp(addr) => (addr.ip().to_string(), addr.port().to_string()),
        ControlEndpoint::Unix(path) => (path.display().to_string(), String::new()),
    }
}

/// Starts the compatibility Gateway when enabled in the validated runtime configuration.
pub fn start_legacy_gateway_if_enabled(
    runtime_config: &ZippyConfig,
    control_endpoint: &ControlEndpoint,
) -> zippy_core::Result<Option<GatewayServer>> {
    start_legacy_gateway_with_persisted_read_snapshot_source_if_enabled(
        runtime_config,
        control_endpoint,
        Arc::new(LegacyPersistedReadSnapshotSource),
    )
}

/// Starts the compatibility Gateway with an explicit persisted-read authority when enabled.
///
/// The unified server uses this boundary to install migration shadow observation without changing
/// the standalone legacy master or the primary query result.
pub fn start_legacy_gateway_with_persisted_read_snapshot_source_if_enabled(
    runtime_config: &ZippyConfig,
    control_endpoint: &ControlEndpoint,
    persisted_read_snapshot_source: Arc<dyn PersistedReadSnapshotSource>,
) -> zippy_core::Result<Option<GatewayServer>> {
    if !runtime_config.gateway.enabled {
        return Ok(None);
    }
    let endpoint =
        runtime_config
            .gateway
            .endpoint
            .clone()
            .ok_or_else(|| ZippyError::InvalidConfig {
                reason: "gateway endpoint is required when gateway is enabled".to_string(),
            })?;
    let gateway = GatewayServer::new_with_query_limits_and_persisted_read_snapshot_source(
        GatewayServerConfig {
            endpoint,
            master_endpoint: control_endpoint.clone(),
            token: runtime_config.gateway.token.clone(),
            max_write_rows: None,
            max_connections: None,
            max_subscribers: None,
            max_blocking_requests: None,
            write_timeout_ms: None,
        },
        runtime_config.runtime.max_parallelism,
        runtime_config.query_parallelism(),
        runtime_config.runtime.max_query_buffer_bytes,
        runtime_config.query_max_buffered_bytes(),
        persisted_read_snapshot_source,
    )?
    .start()?;
    let (host, port) = gateway_endpoint_host_port(gateway.endpoint());
    tracing::info!(
        component = "gateway",
        event = "gateway_start",
        status = "ready",
        endpoint = gateway.endpoint(),
        host = host.as_str(),
        port = port.as_str(),
        max_parallelism = runtime_config.runtime.max_parallelism,
        query_parallelism = runtime_config.query_parallelism(),
        max_query_buffer_bytes = runtime_config.runtime.max_query_buffer_bytes,
        query_max_buffered_bytes = runtime_config.query_max_buffered_bytes(),
        "started zippy gateway"
    );
    Ok(Some(gateway))
}

fn gateway_endpoint_host_port(endpoint: &str) -> (String, String) {
    match endpoint.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (endpoint.to_string(), String::new()),
    }
}

fn load_runtime_config(config_path: Option<&Path>) -> zippy_core::Result<ZippyConfig> {
    if let Some(path) = config_path {
        if !path.exists() {
            return Err(ZippyError::InvalidConfig {
                reason: format!("config file does not exist path=[{}]", path.display()),
            });
        }
        return ZippyConfig::load_from_path(Some(path));
    }
    ZippyConfig::load_default()
}

fn install_shutdown_handlers(
    control_endpoint: &ControlEndpoint,
    server: MasterServer,
    gateway: Arc<Mutex<Option<GatewayServer>>>,
    shutdown_timeout: Duration,
) -> zippy_core::Result<()> {
    #[cfg(unix)]
    {
        let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|error| ZippyError::Io {
            reason: format!("failed to install signal handler error=[{}]", error),
        })?;
        let shutdown_logged = Arc::new(AtomicBool::new(false));
        let control_endpoint = control_endpoint.display_string();
        let control_endpoint_for_thread = control_endpoint.clone();
        let shutdown_logged_for_thread = Arc::clone(&shutdown_logged);

        std::thread::spawn(move || {
            for signal in signals.forever() {
                if !shutdown_logged_for_thread.swap(true, Ordering::SeqCst) {
                    tracing::info!(
                        component = "master",
                        event = "master_shutdown_requested",
                        status = "stopping",
                        control_endpoint = control_endpoint_for_thread.as_str(),
                        signal = signal_name(signal),
                        "received shutdown signal"
                    );
                }
                let deadline = Instant::now()
                    .checked_add(shutdown_timeout)
                    .unwrap_or_else(Instant::now);
                if let Some(gateway) = gateway
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    match gateway.stop_gracefully_with_timeout(
                        deadline.saturating_duration_since(Instant::now()),
                    ) {
                        Ok(outcome) if outcome.is_clean() => {}
                        Ok(outcome) => {
                            tracing::error!(
                                component = "master",
                                event = "legacy_gateway_shutdown_incomplete",
                                status = "incomplete",
                                outcome = ?outcome,
                                "legacy master Gateway cleanup was incomplete"
                            );
                        }
                        Err(error) => {
                            tracing::error!(
                                component = "master",
                                event = "legacy_gateway_shutdown_error",
                                status = "error",
                                error = %error,
                                "legacy master failed to stop Gateway"
                            );
                        }
                    }
                }
                let outcome = server
                    .request_graceful_shutdown(deadline.saturating_duration_since(Instant::now()));
                if !outcome.timed_out_processes.is_empty() {
                    tracing::error!(
                        component = "master",
                        event = "master_shutdown_timeout",
                        status = "timeout",
                        timed_out_processes = ?outcome.timed_out_processes,
                        "master shutdown timed out waiting for child processes"
                    );
                }
            }
        });
    }

    #[cfg(not(unix))]
    {
        let _ = control_endpoint;
        let _ = server;
        let _ = gateway;
        let _ = shutdown_timeout;
    }

    Ok(())
}

#[cfg(unix)]
fn signal_name(signal: i32) -> &'static str {
    match signal {
        SIGINT => "SIGINT",
        SIGTERM => "SIGTERM",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::{prepare_legacy_snapshot_path, LEGACY_MASTER_SNAPSHOT_FILE};
    use crate::snapshot::{RegistrySnapshot, SnapshotStore};
    use zippy_core::{ManagedDataDirectory, OwnedDataDirectory};

    fn snapshot(catalog_revision: u64) -> RegistrySnapshot {
        RegistrySnapshot {
            catalog_revision,
            ..RegistrySnapshot::default()
        }
    }

    #[test]
    fn default_data_root_imports_endpoint_snapshot_once() {
        let endpoint = tempfile::tempdir().unwrap();
        let root = endpoint.path().join("data");
        let owner = OwnedDataDirectory::acquire_or_initialize(&root).unwrap();
        let endpoint_snapshot = endpoint.path().join("master-registry.json");
        SnapshotStore::write(&endpoint_snapshot, &snapshot(17)).unwrap();

        let managed = prepare_legacy_snapshot_path(&owner, &endpoint_snapshot, true, true).unwrap();

        assert_eq!(
            managed,
            owner
                .directory()
                .path(ManagedDataDirectory::Catalog)
                .join(LEGACY_MASTER_SNAPSHOT_FILE)
        );
        assert_eq!(SnapshotStore::load(&managed).unwrap().catalog_revision, 17);
        assert!(endpoint_snapshot.exists());
    }

    #[test]
    fn managed_snapshot_remains_authoritative_when_endpoint_snapshot_exists() {
        let endpoint = tempfile::tempdir().unwrap();
        let root = endpoint.path().join("data");
        let owner = OwnedDataDirectory::acquire_or_initialize(&root).unwrap();
        let managed = owner
            .directory()
            .path(ManagedDataDirectory::Catalog)
            .join(LEGACY_MASTER_SNAPSHOT_FILE);
        SnapshotStore::write(&managed, &snapshot(23)).unwrap();
        SnapshotStore::write(&endpoint.path().join("master-registry.json"), &snapshot(11)).unwrap();

        let selected = prepare_legacy_snapshot_path(
            &owner,
            &endpoint.path().join("master-registry.json"),
            true,
            false,
        )
        .unwrap();

        assert_eq!(selected, managed);
        assert_eq!(SnapshotStore::load(&selected).unwrap().catalog_revision, 23);
    }

    #[test]
    fn explicit_data_root_does_not_import_endpoint_snapshot() {
        let endpoint = tempfile::tempdir().unwrap();
        let root_parent = tempfile::tempdir().unwrap();
        let owner =
            OwnedDataDirectory::acquire_or_initialize(root_parent.path().join("database")).unwrap();
        SnapshotStore::write(&endpoint.path().join("master-registry.json"), &snapshot(31)).unwrap();

        let managed = prepare_legacy_snapshot_path(
            &owner,
            &endpoint.path().join("master-registry.json"),
            false,
            false,
        )
        .unwrap();

        assert!(!managed.exists());
    }

    #[test]
    fn default_data_root_requires_explicit_offline_import() {
        let endpoint = tempfile::tempdir().unwrap();
        let owner =
            OwnedDataDirectory::acquire_or_initialize(endpoint.path().join("data")).unwrap();
        let endpoint_snapshot = endpoint.path().join("master-registry.json");
        SnapshotStore::write(&endpoint_snapshot, &snapshot(37)).unwrap();
        let error = prepare_legacy_snapshot_path(&owner, &endpoint_snapshot, true, false)
            .expect_err("implicit endpoint snapshot import must fail closed");

        assert!(error.to_string().contains("explicit offline import"));
    }
}
