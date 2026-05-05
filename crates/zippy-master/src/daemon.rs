use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;

#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::iterator::Signals;
use zippy_core::{
    default_control_endpoint, resolve_control_endpoint, ControlEndpoint, LogConfig, ZippyConfig,
    ZippyError,
};
use zippy_gateway::{GatewayServer, GatewayServerConfig};

use crate::server::MasterServer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterDaemonConfig {
    pub control_endpoint: ControlEndpoint,
    pub config_path: Option<PathBuf>,
    pub log_dir: PathBuf,
    pub log_level: Option<String>,
    pub console_log: bool,
}

impl MasterDaemonConfig {
    pub fn new(control_endpoint: impl Into<ControlEndpoint>) -> Self {
        Self {
            control_endpoint: control_endpoint.into(),
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
    let MasterDaemonConfig {
        mut control_endpoint,
        config_path,
        log_dir,
        log_level,
        console_log,
    } = config;
    let mut runtime_config = load_runtime_config(config_path.as_deref())?;
    if control_endpoint == default_control_endpoint() {
        if let Some(endpoint) = runtime_config.master_endpoint() {
            control_endpoint = resolve_control_endpoint(format!("tcp://{endpoint}"))?;
        }
    }
    if let Some(log_level) = log_level {
        runtime_config.log.level = log_level;
    }
    let effective_log_level = runtime_config.log.level.clone();

    if let Some(path) = control_endpoint.unix_path() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
                reason: error.to_string(),
            })?;
        }
    }
    let _gateway = start_gateway_if_enabled(&runtime_config, &control_endpoint)?;
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
        table_row_capacity = runtime_config.table.row_capacity,
        table_persist_enabled = runtime_config.table.persist.enabled,
        table_persist_method = runtime_config.table.persist.method.as_str(),
        table_persist_data_dir = runtime_config.table.persist.data_dir.as_str(),
        log_file = snapshot
            .file_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "starting zippy master"
    );

    let snapshot_dir = control_endpoint.snapshot_dir();
    std::fs::create_dir_all(&snapshot_dir).map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })?;
    let snapshot_path = snapshot_dir.join("master-registry.json");

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
    install_shutdown_handlers(&control_endpoint, server.clone())?;

    if std::env::var_os("ZIPPY_MASTER_TEST_PAUSE_BEFORE_SERVE").is_some() {
        std::thread::sleep(Duration::from_millis(150));
    }

    if !server.is_running() {
        tracing::info!(
            component = "master",
            event = "master_stopped",
            status = "stopped",
            control_endpoint = control_endpoint_display.as_str(),
            "master stopped"
        );
        return Ok(());
    }

    server.serve_endpoint_with_ready(&control_endpoint, None)
}

fn control_endpoint_host_port(endpoint: &ControlEndpoint) -> (String, String) {
    match endpoint {
        ControlEndpoint::Tcp(addr) => (addr.ip().to_string(), addr.port().to_string()),
        ControlEndpoint::Unix(path) => (path.display().to_string(), String::new()),
    }
}

fn start_gateway_if_enabled(
    runtime_config: &ZippyConfig,
    control_endpoint: &ControlEndpoint,
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
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint,
        master_endpoint: control_endpoint.clone(),
        token: runtime_config.gateway.token.clone(),
        max_write_rows: None,
    })?
    .start()?;
    let (host, port) = gateway_endpoint_host_port(gateway.endpoint());
    tracing::info!(
        component = "gateway",
        event = "gateway_start",
        status = "ready",
        endpoint = gateway.endpoint(),
        host = host.as_str(),
        port = port.as_str(),
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
                server.shutdown();
            }
        });
    }

    #[cfg(not(unix))]
    {
        let _ = control_endpoint;
        let _ = server;
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
