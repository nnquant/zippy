//! Logging setup primitives for the shared `zippy-core` subscriber.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_subscriber::prelude::*;

use crate::error::{Result, ZippyError};

/// Logging configuration used to initialize the global subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    pub app: String,
    pub level: String,
    pub log_dir: PathBuf,
    pub to_console: bool,
    pub to_file: bool,
}

impl LogConfig {
    /// Build a logging configuration from borrowed inputs.
    pub fn new(
        app: impl Into<String>,
        level: impl Into<String>,
        log_dir: impl AsRef<Path>,
        to_console: bool,
        to_file: bool,
    ) -> Self {
        Self {
            app: app.into(),
            level: level.into(),
            log_dir: log_dir.as_ref().to_path_buf(),
            to_console,
            to_file,
        }
    }
}

/// Snapshot of the active logging configuration returned after setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSnapshot {
    pub app: String,
    pub level: String,
    pub run_id: String,
    pub file_path: Option<PathBuf>,
}

struct SharedFileWriter {
    file: Arc<Mutex<File>>,
}

impl SharedFileWriter {
    fn new(file: Arc<Mutex<File>>) -> Self {
        Self { file }
    }
}

impl Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "logging file lock poisoned"))?;
        file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "logging file lock poisoned"))?;
        file.flush()
    }
}

/// Initialize the process-wide tracing subscriber and return the active snapshot.
pub fn setup_log(config: LogConfig) -> Result<LogSnapshot> {
    validate_config(&config)?;

    let filter = tracing_subscriber::EnvFilter::try_new(config.level.clone()).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: format!("invalid log level error=[{error}]"),
        }
    })?;

    let run_id = generate_run_id()?;
    let file_path = if config.to_file {
        Some(prepare_file_path(&config.log_dir, &config.app, &run_id)?)
    } else {
        None
    };

    let console_layer = config
        .to_console
        .then(|| tracing_subscriber::fmt::layer().with_target(false));

    let file_layer = if let Some(path) = file_path.as_ref() {
        let file = File::options()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| ZippyError::Io {
                reason: format!(
                    "failed to open log file path=[{}] error=[{}]",
                    path.display(),
                    error
                ),
            })?;
        let writer = Arc::new(Mutex::new(file));

        Some(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(move || SharedFileWriter::new(writer.clone())),
        )
    } else {
        None
    };

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer);

    tracing::subscriber::set_global_default(subscriber).map_err(|_| ZippyError::InvalidState {
        status: "logging already initialized",
    })?;

    Ok(LogSnapshot {
        app: config.app,
        level: config.level,
        run_id,
        file_path,
    })
}

fn validate_config(config: &LogConfig) -> Result<()> {
    if config.app.trim().is_empty() {
        return Err(ZippyError::InvalidConfig {
            reason: "log app is empty".to_string(),
        });
    }

    if !config.to_console && !config.to_file {
        return Err(ZippyError::InvalidConfig {
            reason: "logging requires at least one output".to_string(),
        });
    }

    Ok(())
}

fn prepare_file_path(log_dir: &Path, app: &str, run_id: &str) -> Result<PathBuf> {
    let app_dir = log_dir.join(app);
    fs::create_dir_all(&app_dir).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to create log directory path=[{}] error=[{}]",
            app_dir.display(),
            error
        ),
    })?;

    let file_path = app_dir.join(format!("{run_id}.jsonl"));
    File::create(&file_path).map_err(|error| ZippyError::Io {
        reason: format!(
            "failed to create log file path=[{}] error=[{}]",
            file_path.display(),
            error
        ),
    })?;

    Ok(file_path)
}

fn generate_run_id() -> Result<String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to compute run id error=[{}]", error),
        })?;

    Ok(format!("{:x}", duration.as_nanos()))
}
