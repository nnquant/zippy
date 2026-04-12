use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use zippy_core::{LogConfig, ZippyError};

use crate::server::MasterServer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterDaemonConfig {
    pub control_endpoint: PathBuf,
    pub log_dir: PathBuf,
    pub log_level: String,
    pub console_log: bool,
}

impl MasterDaemonConfig {
    pub fn new(control_endpoint: impl AsRef<Path>) -> Self {
        Self {
            control_endpoint: control_endpoint.as_ref().to_path_buf(),
            log_dir: PathBuf::from("logs"),
            log_level: String::from("info"),
            console_log: true,
        }
    }

    pub fn with_logging(
        mut self,
        log_dir: impl AsRef<Path>,
        log_level: impl Into<String>,
        console_log: bool,
    ) -> Self {
        self.log_dir = log_dir.as_ref().to_path_buf();
        self.log_level = log_level.into();
        self.console_log = console_log;
        self
    }
}

pub fn run_master_daemon(config: MasterDaemonConfig) -> zippy_core::Result<()> {
    let MasterDaemonConfig {
        control_endpoint,
        log_dir,
        log_level,
        console_log,
    } = config;

    if let Some(parent) = control_endpoint.parent() {
        std::fs::create_dir_all(parent).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?;
    }

    let snapshot = zippy_core::setup_log(LogConfig::new(
        "zippy-master",
        log_level.clone(),
        &log_dir,
        console_log,
        true,
    ))?;

    tracing::info!(
        component = "master",
        event = "master_start",
        status = "starting",
        control_endpoint = control_endpoint.display().to_string(),
        log_dir = log_dir.display().to_string(),
        log_level = log_level,
        log_file = snapshot
            .file_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        "starting zippy master"
    );

    let snapshot_path = control_endpoint
        .parent()
        .map(|parent| parent.join("master-registry.json"))
        .unwrap_or_else(|| PathBuf::from("master-registry.json"));

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
                server
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
        MasterServer::with_runtime_config(
            Some(snapshot_path),
            Duration::from_secs(10),
            Duration::from_secs(2),
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
            control_endpoint = control_endpoint.display().to_string(),
            "master stopped"
        );
        return Ok(());
    }

    server.serve(&control_endpoint)
}

fn install_shutdown_handlers(socket_path: &Path, server: MasterServer) -> zippy_core::Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM]).map_err(|error| ZippyError::Io {
        reason: format!("failed to install signal handler error=[{}]", error),
    })?;
    let shutdown_logged = Arc::new(AtomicBool::new(false));
    let control_endpoint = socket_path.display().to_string();
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

    Ok(())
}

fn signal_name(signal: i32) -> &'static str {
    match signal {
        SIGINT => "SIGINT",
        SIGTERM => "SIGTERM",
        _ => "UNKNOWN",
    }
}
