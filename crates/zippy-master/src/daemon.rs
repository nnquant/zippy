use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use zippy_core::{
    default_control_endpoint, resolve_control_endpoint, ControlEndpoint, LogConfig,
    ManagedDataDirectory, OwnedDataDirectory, ZippyConfig, ZippyError,
};

use crate::server::MasterServer;

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

/// Validated control runtime components prepared without starting any listener.
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
    let retired_registry = data_directory
        .directory()
        .path(ManagedDataDirectory::Catalog)
        .join(LEGACY_MASTER_SNAPSHOT_FILE);
    match std::fs::symlink_metadata(&retired_registry) {
        Ok(_) => {
            return Err(ZippyError::InvalidConfig {
                reason: "removed legacy master registry; use an empty native data directory"
                    .to_owned(),
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ZippyError::Io {
                reason: format!("inspect retired registry failed error=[{error}]"),
            })
        }
    }
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
        max_cpu_cores = runtime_config.runtime.max_cpu_cores,
        query_parallelism = runtime_config.query_parallelism(),
        log_file = snapshot
            .file_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "starting zippy master"
    );

    let shutdown_timeout = Duration::from_millis(runtime_config.master.shutdown_timeout_ms);
    let server = MasterServer::with_config(runtime_config);
    Ok(PreparedMasterRuntime {
        control_endpoint,
        runtime_config: server.runtime_config().clone(),
        server,
        shutdown_timeout,
        data_directory,
    })
}

fn control_endpoint_host_port(endpoint: &ControlEndpoint) -> (String, String) {
    match endpoint {
        ControlEndpoint::Tcp(addr) => (addr.ip().to_string(), addr.port().to_string()),
        ControlEndpoint::Unix(path) => (path.display().to_string(), String::new()),
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

#[cfg(test)]
mod tests {
    use super::{prepare_master_runtime, MasterDaemonConfig};
    use zippy_core::ControlEndpoint;

    #[cfg(unix)]
    #[test]
    fn endpoint_scoped_legacy_snapshot_is_never_read_or_copied() {
        let temp = tempfile::tempdir().unwrap();
        let old_path = temp.path().join("master-registry.json");
        let old_bytes = b"not current registry data; leave untouched";
        std::fs::write(&old_path, old_bytes).unwrap();
        let endpoint = ControlEndpoint::Unix(temp.path().join("server.sock"));
        let config_path = temp.path().join("config.toml");
        std::fs::write(&config_path, "").unwrap();
        let config = MasterDaemonConfig::new(endpoint.clone())
            .with_config_path(config_path)
            .with_logging(temp.path().join("logs"), "error", false);
        let prepared = prepare_master_runtime(config).unwrap();
        assert!(!endpoint
            .snapshot_dir()
            .join("data/catalog/legacy-master-registry.json")
            .exists());
        assert_eq!(std::fs::read(old_path).unwrap(), old_bytes);
        drop(prepared);
    }

    #[test]
    fn removed_import_configuration_fails_before_creating_data_directory() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("database");
        let config_path = temp.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                "[server]\ndata_dir = {:?}\nimport_legacy_endpoint_snapshot = true\n",
                data.to_str().unwrap(),
            ),
        )
        .unwrap();
        let config =
            MasterDaemonConfig::new(ControlEndpoint::Tcp("127.0.0.1:17690".parse().unwrap()))
                .with_config_path(config_path)
                .with_logging(temp.path().join("logs"), "error", false);
        let error = prepare_master_runtime(config).err().unwrap();
        assert!(error.to_string().contains("unknown field"));
        assert!(!data.exists());
    }
    #[test]
    fn managed_legacy_registry_is_rejected_without_reading_or_rewriting_it() {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("database");
        let owned = zippy_core::OwnedDataDirectory::acquire_or_initialize(&data).unwrap();
        let old_path = data.join("catalog/legacy-master-registry.json");
        let original = b"unparseable retired registry bytes";
        std::fs::write(&old_path, original).unwrap();
        drop(owned);
        let config_path = temp.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!("[server]\ndata_dir = {:?}\n", data.to_str().unwrap()),
        )
        .unwrap();
        let config =
            MasterDaemonConfig::new(ControlEndpoint::Tcp("127.0.0.1:17690".parse().unwrap()))
                .with_config_path(config_path)
                .with_logging(temp.path().join("logs"), "error", false);
        let error = prepare_master_runtime(config).err().unwrap();
        assert!(error.to_string().contains("removed legacy master registry"));
        assert_eq!(std::fs::read(&old_path).unwrap(), original);
        assert!(!data.join("catalog/catalog.db").exists());
        assert!(zippy_core::OwnedDataDirectory::acquire_or_initialize(&data).is_ok());
    }
}
