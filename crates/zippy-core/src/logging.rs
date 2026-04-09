//! Logging setup primitives for the shared `zippy-core` subscriber.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::Subscriber;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

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

#[derive(Debug, Clone)]
struct InitializedLogState {
    config: LogConfig,
    snapshot: LogSnapshot,
}

static LOG_STATE: OnceLock<InitializedLogState> = OnceLock::new();
static LOG_INIT_LOCK: Mutex<()> = Mutex::new(());

struct LazyFileWriter {
    state: Arc<Mutex<LazyFileState>>,
}

struct LazyFileState {
    path: PathBuf,
    file: Option<File>,
}

impl LazyFileWriter {
    fn new(path: PathBuf) -> Self {
        Self {
            state: Arc::new(Mutex::new(LazyFileState { path, file: None })),
        }
    }
}

impl Write for LazyFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "logging file lock poisoned"))?;
        if state.file.is_none() {
            if let Some(parent) = state.path.parent() {
                fs::create_dir_all(parent)?;
            }

            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&state.path)?;
            state.file = Some(file);
        }

        state
            .file
            .as_mut()
            .expect("logging file must be initialized")
            .write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "logging file lock poisoned"))?;
        match state.file.as_mut() {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}

struct JsonLogFormatter {
    app: String,
    run_id: String,
}

impl JsonLogFormatter {
    fn new(app: String, run_id: String) -> Self {
        Self { app, run_id }
    }
}

impl<S, N> FormatEvent<S, N> for JsonLogFormatter
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let metadata = event.metadata();
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);

        let mut record = Map::new();
        record.insert("app".to_string(), Value::String(self.app.clone()));
        record.insert("run_id".to_string(), Value::String(self.run_id.clone()));
        record.insert(
            "level".to_string(),
            Value::String(metadata.level().to_string()),
        );
        record.insert(
            "target".to_string(),
            Value::String(metadata.target().to_string()),
        );
        record.extend(visitor.fields);

        if !record.contains_key("message") {
            record.insert("message".to_string(), Value::String(String::new()));
        }

        writeln!(
            writer,
            "{}",
            serde_json::to_string(&record).map_err(|_| std::fmt::Error)?
        )
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    fields: Map<String, Value>,
}

impl Visit for JsonFieldVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::Number(value.into()));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::String(value.to_string()));
    }

    fn record_error(
        &mut self,
        field: &Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(value.to_string()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(number) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_string(), Value::Number(number));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::String(format!("{value:?}")),
        );
    }
}

/// Initialize the process-wide tracing subscriber and return the active snapshot.
pub fn setup_log(config: LogConfig) -> Result<LogSnapshot> {
    validate_config(&config)?;

    if let Some(state) = LOG_STATE.get() {
        return resolve_existing_setup(state, &config);
    }

    let _guard = LOG_INIT_LOCK.lock().map_err(|_| ZippyError::InvalidState {
        status: "logging init lock poisoned",
    })?;

    if let Some(state) = LOG_STATE.get() {
        return resolve_existing_setup(state, &config);
    }

    let filter = tracing_subscriber::EnvFilter::try_new(config.level.clone()).map_err(|error| {
        ZippyError::InvalidConfig {
            reason: format!("invalid log level error=[{error}]"),
        }
    })?;

    let run_id = generate_run_id()?;
    let file_path = if config.to_file {
        Some(prepare_file_path(&config.log_dir, &config.app, &run_id))
    } else {
        None
    };

    let console_layer = config
        .to_console
        .then(|| tracing_subscriber::fmt::layer().with_target(false));

    let file_layer = if let Some(path) = file_path.clone() {
        Some(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .event_format(JsonLogFormatter::new(
                    config.app.clone(),
                    run_id.clone(),
                ))
                .with_writer(move || LazyFileWriter::new(path.clone())),
        )
    } else {
        None
    };

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer);

    let snapshot = LogSnapshot {
        app: config.app.clone(),
        level: config.level.clone(),
        run_id,
        file_path,
    };

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        return Err(ZippyError::InvalidState {
            status: "logging already initialized",
        });
    }

    LOG_STATE
        .set(InitializedLogState {
            config,
            snapshot: snapshot.clone(),
        })
        .map_err(|_| ZippyError::InvalidState {
            status: "logging state already initialized",
        })?;

    Ok(snapshot)
}

/// Return the process-wide logging snapshot after setup succeeds.
pub fn current_log_snapshot() -> Option<LogSnapshot> {
    LOG_STATE.get().map(|state| state.snapshot.clone())
}

fn resolve_existing_setup(state: &InitializedLogState, config: &LogConfig) -> Result<LogSnapshot> {
    if state.config == *config {
        return Ok(state.snapshot.clone());
    }

    Err(ZippyError::InvalidState {
        status: "logging already initialized with different settings",
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
    let date = utc_date_string()?;
    Ok(log_dir.join(app).join(format!("{date}_{run_id}.jsonl")))
}

fn generate_run_id() -> Result<String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to compute run id error=[{}]", error),
        })?;

    Ok(format!("{:x}", duration.as_nanos()))
}

fn utc_date_string() -> Result<String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: format!("failed to compute utc date error=[{}]", error),
        })?;
    let days_since_epoch = (duration.as_secs() / 86_400) as i64;
    let (year, month, day) = civil_from_days(days_since_epoch);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let days = days_since_epoch + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}
