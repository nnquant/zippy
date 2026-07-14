//! SQLite Catalog migration and typed command foundation for the single-node server.
//!
//! Physical schema v2 stores the complete first domain layout with lossless
//! unsigned encodings, while v3 adds the active-table cursor indexes.
//! [`SqliteCatalog`] implements namespace and table-creation command slices plus
//! exact SQLite snapshot reads, but legacy metadata remains authoritative in the
//! server runtime. Physical SQLite schema versions are independent of
//! CatalogCommand wire versions. Each handle confines its writable connection and
//! read-only connections to dedicated OS worker threads.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod domain;
mod encoding;
mod file_domain;
mod gc_domain;
mod lease_domain;
mod legacy_import_domain;
mod legacy_reconcile_domain;
mod manifest_domain;
mod migration;
mod pinned_snapshot;
mod runtime_pins;
mod service;
mod snapshot_domain;
mod storage_codec;
mod table_domain;

pub use pinned_snapshot::{
    PinnedDataFile, PinnedDataFileLocationKind, PinnedDataFileReader, PinnedTableManifest,
    SqliteCatalogPinnedQuerySnapshot,
};
pub use runtime_pins::{SqliteCatalogRuntimeSnapshotPin, SqliteCatalogRuntimeSnapshotPinStatus};
pub use service::{
    SqliteCatalog, SqliteCatalogLegacyImportSession, SqliteCatalogLegacyReconcileHandle,
    SqliteCatalogReadHandle,
};
pub use snapshot_domain::SqliteCatalogSnapshotReadLimits;
pub use storage_codec::{
    canonical_schema_hash, canonical_schema_hash_from_ipc, decode_persisted_schema,
    decode_persisted_table_schema, DecodedPersistedTableSchema,
};

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rusqlite::{Connection, Error as SqliteError, OpenFlags};
use zippy_core::{ManagedDataDirectory, OwnedDataDirectory};
use zippy_types::{ErrorCode, Result, ServerError};

/// The latest SQLite schema version understood by this crate.
pub const CATALOG_SCHEMA_VERSION: u32 = 6;

/// SQLite header marker identifying a Zippy Catalog database.
pub const CATALOG_APPLICATION_ID: u32 = 0x5A49_5059;

/// The stable catalog format marker stored in `catalog_meta`.
pub const CATALOG_FORMAT: &str = "zippy.sqlite.catalog";

const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_INTEGRITY_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_COMMAND_CAPACITY: usize = 64;
const DEFAULT_CONTROL_READ_CAPACITY: usize = 64;
const DEFAULT_SNAPSHOT_READ_CAPACITY: usize = 8;
const DEFAULT_MANIFEST_QUEUE_CAPACITY_BYTES: usize = 128 * 1024 * 1024;
const MAX_COMMAND_CAPACITY: usize = 4_096;
const MAX_READ_CAPACITY: usize = 4_096;
const MAX_MANIFEST_QUEUE_CAPACITY_BYTES: usize = 1024 * 1024 * 1024;
const MAX_BUSY_TIMEOUT_MILLIS: u128 = i32::MAX as u128;
const ACTOR_THREAD_NAME: &str = "zippy-catalog-actor";
const QUICK_CHECK_PROGRESS_INTERVAL: i32 = 1_000;

/// The authority state exposed by the inspection-only migration handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogAuthorityState {
    /// The handle may migrate and inspect the database but cannot serve Catalog APIs.
    FoundationOnly,
}

impl CatalogAuthorityState {
    /// Returns whether this state may act as metadata authority.
    ///
    /// Authority is intentionally impossible in this implementation slice.
    #[must_use]
    pub const fn is_authoritative(self) -> bool {
        false
    }
}

/// Configuration for opening the non-authoritative SQLite Catalog foundation.
#[derive(Debug, Clone)]
pub struct SqliteCatalogConfig {
    ownership: Arc<OwnedDataDirectory>,
    data_root: PathBuf,
    catalog_directory: PathBuf,
    path: PathBuf,
    busy_timeout: Duration,
    integrity_check_timeout: Duration,
    command_capacity: usize,
    control_read_capacity: usize,
    snapshot_read_capacity: usize,
    manifest_queue_capacity_bytes: usize,
    runtime_snapshot_pin_max_active_pins: usize,
    runtime_snapshot_pin_max_active_tables: usize,
}

impl SqliteCatalogConfig {
    /// Creates a writable configuration under an exclusively owned data root.
    ///
    /// The configured path is always `<data_root>/catalog/catalog.db`. The actor
    /// retains a clone of the ownership capability, revalidates the managed
    /// directories, and opens the final file with `SQLITE_OPEN_NOFOLLOW`.
    /// Network-filesystem detection remains a pre-authority requirement.
    #[must_use]
    pub fn new(ownership: &Arc<OwnedDataDirectory>) -> Self {
        let data_directory = ownership.directory();
        let catalog_directory = data_directory.path(ManagedDataDirectory::Catalog);
        Self {
            ownership: Arc::clone(ownership),
            data_root: data_directory.root().to_path_buf(),
            path: catalog_directory.join("catalog.db"),
            catalog_directory,
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            integrity_check_timeout: DEFAULT_INTEGRITY_CHECK_TIMEOUT,
            command_capacity: DEFAULT_COMMAND_CAPACITY,
            control_read_capacity: DEFAULT_CONTROL_READ_CAPACITY,
            snapshot_read_capacity: DEFAULT_SNAPSHOT_READ_CAPACITY,
            manifest_queue_capacity_bytes: DEFAULT_MANIFEST_QUEUE_CAPACITY_BYTES,
            runtime_snapshot_pin_max_active_pins:
                zippy_core::DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_PINS,
            runtime_snapshot_pin_max_active_tables:
                zippy_core::DEFAULT_CATALOG_RUNTIME_SNAPSHOT_PIN_MAX_ACTIVE_TABLES,
        }
    }

    /// Overrides SQLite's lock wait timeout.
    #[must_use]
    pub const fn with_busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
    }

    /// Overrides the elapsed-time threshold checked between SQLite VM steps.
    #[must_use]
    pub const fn with_integrity_check_timeout(mut self, integrity_check_timeout: Duration) -> Self {
        self.integrity_check_timeout = integrity_check_timeout;
        self
    }

    /// Overrides the bounded actor command queue capacity.
    #[must_use]
    pub const fn with_command_capacity(mut self, command_capacity: usize) -> Self {
        self.command_capacity = command_capacity;
        self
    }

    /// Overrides the bounded queue capacity for control-plane Catalog reads.
    #[must_use]
    pub const fn with_control_read_capacity(mut self, control_read_capacity: usize) -> Self {
        self.control_read_capacity = control_read_capacity;
        self
    }

    /// Overrides the bounded queue capacity for exact Catalog snapshot reads.
    #[must_use]
    pub const fn with_snapshot_read_capacity(mut self, snapshot_read_capacity: usize) -> Self {
        self.snapshot_read_capacity = snapshot_read_capacity;
        self
    }

    /// Overrides the aggregate bytes reserved by queued native manifest commands.
    #[must_use]
    pub const fn with_manifest_queue_capacity_bytes(
        mut self,
        manifest_queue_capacity_bytes: usize,
    ) -> Self {
        self.manifest_queue_capacity_bytes = manifest_queue_capacity_bytes;
        self
    }

    /// Overrides process-local query snapshot pin admission limits.
    ///
    /// `max_active_pins` bounds retained table-snapshot guards. `max_active_tables` bounds table
    /// entries shared by query pins and garbage-collection exclusion. Values are validated when
    /// the Catalog is opened.
    #[must_use]
    pub const fn with_runtime_snapshot_pin_limits(
        mut self,
        max_active_pins: usize,
        max_active_tables: usize,
    ) -> Self {
        self.runtime_snapshot_pin_max_active_pins = max_active_pins;
        self.runtime_snapshot_pin_max_active_tables = max_active_tables;
        self
    }

    /// Borrows the SQLite catalog path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Borrows the process ownership retained for writable Catalog workers.
    #[must_use]
    pub fn ownership(&self) -> &Arc<OwnedDataDirectory> {
        &self.ownership
    }
}

/// Result of SQLite's catalog `quick_check` and foreign-key validation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogIntegrity {
    /// SQLite reported `ok` and no additional diagnostics.
    Ok,
}

/// Immutable metadata read from the initialized SQLite catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMetadata {
    /// Stable on-disk catalog format marker.
    pub catalog_format: String,
    /// Latest successfully committed migration version.
    pub schema_version: u32,
    /// Monotonic metadata revision, initialized to zero in this foundation.
    pub catalog_revision: u64,
}

/// Observable state returned by the SQLite Catalog actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteCatalogFoundationStatus {
    /// The explicitly non-authoritative Catalog state.
    pub authority_state: CatalogAuthorityState,
    /// Persistent Catalog metadata.
    pub metadata: CatalogMetadata,
    /// SQLite journal mode; opening fails unless this is exactly `wal`.
    pub journal_mode: String,
    /// SQLite foreign-key enforcement state.
    pub foreign_keys_enabled: bool,
    /// Configured SQLite busy timeout in milliseconds.
    pub busy_timeout_ms: u64,
    /// VM execution timeout threshold for each startup or on-demand integrity check.
    pub integrity_check_timeout_ms: u64,
    /// Disabled automatic WAL checkpoint threshold; zero means coordinator-owned checkpoints.
    pub wal_autocheckpoint_pages: u32,
    /// Latest successful actor-thread integrity result.
    ///
    /// Inspection fails instead of returning this status while a failed
    /// explicit integrity check remains cached.
    pub integrity: CatalogIntegrity,
    /// Debug identity proving all SQLite work was serialized by one actor thread.
    pub actor_thread_id: String,
}

/// A versioned SQLite Catalog foundation whose connection is actor-thread confined.
///
/// This handle only initializes, validates, and inspects the physical Catalog;
/// [`SqliteCatalog`] is the separate typed command handle. Neither handle takes
/// authority from the legacy Registry in this migration slice.
///
/// ```compile_fail
/// fn require_catalog<T: zippy_types::Catalog>() {}
/// require_catalog::<zippy_catalog::SqliteCatalogFoundation>();
/// ```
pub struct SqliteCatalogFoundation {
    sender: Option<SyncSender<ActorMessage>>,
    actor: Option<JoinHandle<()>>,
    initial_status: SqliteCatalogFoundationStatus,
}

impl SqliteCatalogFoundation {
    /// Opens or migrates a SQLite catalog on its dedicated actor thread.
    ///
    /// The call returns only after WAL configuration, transactional migrations,
    /// metadata validation, and `quick_check` all succeed.
    pub fn open(config: SqliteCatalogConfig) -> Result<Self> {
        validate_config(&config)?;
        let (sender, receiver) = mpsc::sync_channel(config.command_capacity);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let actor = thread::Builder::new()
            .name(ACTOR_THREAD_NAME.to_owned())
            .spawn(move || run_actor(config, receiver, startup_sender))
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start catalog actor thread error=[{error}]"),
                )
            })?;
        let initial_status = match startup_receiver.recv() {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                join_startup_actor(actor)?;
                return Err(error);
            }
            Err(_) => {
                join_startup_actor(actor)?;
                return Err(actor_stopped());
            }
        };
        Ok(Self {
            sender: Some(sender),
            actor: Some(actor),
            initial_status,
        })
    }

    /// Returns the status proven during actor startup without touching SQLite.
    #[must_use]
    pub fn initial_status(&self) -> &SqliteCatalogFoundationStatus {
        &self.initial_status
    }

    /// Re-reads metadata and SQLite invariants on the owning actor thread.
    pub fn inspect(&self) -> Result<SqliteCatalogFoundationStatus> {
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        self.send(ActorMessage::Inspect(response_sender))?;
        response_receiver.recv().map_err(|_| actor_stopped())?
    }

    /// Runs SQLite `PRAGMA quick_check` on the owning actor thread.
    pub fn quick_check(&self) -> Result<CatalogIntegrity> {
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        self.send(ActorMessage::QuickCheck(response_sender))?;
        response_receiver.recv().map_err(|_| actor_stopped())?
    }

    /// Gracefully closes the SQLite connection and joins its actor thread.
    ///
    /// This foundation close is reliable but not deadline-bounded. It must gain
    /// an absolute deadline before the actor is wired into the server runtime.
    pub fn close(mut self) -> Result<()> {
        self.shutdown()
    }

    fn send(&self, message: ActorMessage) -> Result<()> {
        use std::sync::mpsc::TrySendError;

        match self
            .sender
            .as_ref()
            .ok_or_else(actor_stopped)?
            .try_send(message)
        {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "catalog actor command queue is full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(actor_stopped()),
        }
    }

    fn shutdown(&mut self) -> Result<()> {
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        let send_result = sender
            .send(ActorMessage::Shutdown(response_sender))
            .map_err(|_| actor_stopped());
        drop(sender);
        let actor_result = match send_result {
            Ok(()) => response_receiver.recv().map_err(|_| actor_stopped())?,
            Err(error) => Err(error),
        };
        let join_result = match self.actor.take() {
            Some(actor) => actor.join().map_err(|_| {
                ServerError::new(ErrorCode::Internal, "catalog actor thread panicked")
            }),
            None => Ok(()),
        };
        actor_result.and(join_result)
    }
}

impl Drop for SqliteCatalogFoundation {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            tracing::error!(error = %error, "catalog foundation shutdown failed");
        }
    }
}

enum ActorMessage {
    Inspect(SyncSender<Result<SqliteCatalogFoundationStatus>>),
    QuickCheck(SyncSender<Result<CatalogIntegrity>>),
    Shutdown(SyncSender<Result<()>>),
}

fn run_actor(
    config: SqliteCatalogConfig,
    receiver: Receiver<ActorMessage>,
    startup_sender: SyncSender<Result<SqliteCatalogFoundationStatus>>,
) {
    let startup = open_on_actor(&config);
    let (connection, status) = match startup {
        Ok(value) => value,
        Err(error) => {
            let _ = startup_sender.send(Err(error));
            return;
        }
    };
    if startup_sender.send(Ok(status)).is_err() {
        return;
    }
    actor_loop(connection, &config, receiver, Ok(CatalogIntegrity::Ok));
}

fn open_on_actor(
    config: &SqliteCatalogConfig,
) -> Result<(Connection, SqliteCatalogFoundationStatus)> {
    validate_managed_catalog_path(config)?;
    let flags = OpenFlags::default() | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let mut connection = Connection::open_with_flags(&config.path, flags)
        .map_err(|error| map_sqlite_error("open catalog database", error))?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(|error| map_sqlite_error("configure catalog busy timeout", error))?;
    migration::preflight(&connection)?;
    // Never advance the physical schema of a corrupt database. A failed post-migration check
    // cannot roll back a transaction that has already committed, so the old version must first be
    // proven healthy to preserve binary rollback.
    run_quick_check(&connection, config.integrity_check_timeout)?;
    configure_durable_pragmas(&connection)?;
    migration::migrate(&mut connection)?;
    let integrity = run_quick_check(&connection, config.integrity_check_timeout)?;
    let status = inspect_connection(&connection, config, integrity)?;
    Ok((connection, status))
}

fn open_read_connection(config: &SqliteCatalogConfig) -> Result<Connection> {
    validate_managed_catalog_path(config)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let connection = Connection::open_with_flags(&config.path, flags)
        .map_err(|error| map_sqlite_error("open read-only catalog database", error))?;
    connection
        .busy_timeout(config.busy_timeout)
        .map_err(|error| map_sqlite_error("configure read-only catalog busy timeout", error))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|error| map_sqlite_error("enable read-only catalog foreign keys", error))?;
    connection
        .pragma_update(None, "read_uncommitted", false)
        .map_err(|error| map_sqlite_error("disable dirty catalog reads", error))?;
    connection
        .pragma_update(None, "query_only", true)
        .map_err(|error| map_sqlite_error("enable query-only catalog connection", error))?;

    let journal_mode: String = pragma(&connection, "journal_mode")?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog read connection requires WAL journal mode actual=[{journal_mode}]"),
        ));
    }
    let query_only: u32 = pragma(&connection, "query_only")?;
    let foreign_keys: u32 = pragma(&connection, "foreign_keys")?;
    let read_uncommitted: u32 = pragma(&connection, "read_uncommitted")?;
    if query_only != 1 || foreign_keys != 1 || read_uncommitted != 0 {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!(
                concat!(
                    "catalog read connection pragmas are invalid query_only=[{}] ",
                    "foreign_keys=[{}] read_uncommitted=[{}]"
                ),
                query_only, foreign_keys, read_uncommitted
            ),
        ));
    }
    let _ = migration::validate_metadata(&connection)?;
    Ok(connection)
}

fn actor_loop(
    connection: Connection,
    config: &SqliteCatalogConfig,
    receiver: Receiver<ActorMessage>,
    mut last_integrity: Result<CatalogIntegrity>,
) {
    let mut shutdown_response = None;
    while let Ok(message) = receiver.recv() {
        match message {
            ActorMessage::Inspect(response) => {
                let result = match &last_integrity {
                    Ok(integrity) => inspect_connection(&connection, config, *integrity),
                    Err(error) => Err(error.clone()),
                };
                let _ = response.send(result);
            }
            ActorMessage::QuickCheck(response) => {
                let result = run_quick_check(&connection, config.integrity_check_timeout);
                last_integrity = result.clone();
                let _ = response.send(result);
            }
            ActorMessage::Shutdown(response) => {
                shutdown_response = Some(response);
                break;
            }
        }
    }
    let close_result = connection
        .close()
        .map_err(|(_, error)| map_sqlite_error("close catalog database", error));
    if let Some(response) = shutdown_response {
        let _ = response.send(close_result);
    }
}

fn configure_durable_pragmas(connection: &Connection) -> Result<()> {
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|error| map_sqlite_error("enable catalog WAL mode", error))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ServerError::new(
            ErrorCode::Unavailable,
            format!("catalog requires WAL journal mode actual=[{journal_mode}]"),
        ));
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|error| map_sqlite_error("enable full catalog synchronization", error))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|error| map_sqlite_error("enable catalog foreign keys", error))?;
    connection
        .pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(|error| map_sqlite_error("disable automatic catalog checkpoint", error))
}

fn inspect_connection(
    connection: &Connection,
    config: &SqliteCatalogConfig,
    last_integrity: CatalogIntegrity,
) -> Result<SqliteCatalogFoundationStatus> {
    let journal_mode: String = pragma(connection, "journal_mode")?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog journal mode changed actual=[{journal_mode}]"),
        ));
    }
    let foreign_keys: u32 = pragma(connection, "foreign_keys")?;
    if foreign_keys != 1 {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "catalog foreign key enforcement is disabled",
        ));
    }
    let synchronous: u32 = pragma(connection, "synchronous")?;
    if synchronous != 2 {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!("catalog synchronous mode is not FULL actual=[{synchronous}]"),
        ));
    }
    let busy_timeout_ms: u64 = pragma(connection, "busy_timeout")?;
    let wal_autocheckpoint_pages: u32 = pragma(connection, "wal_autocheckpoint")?;
    if wal_autocheckpoint_pages != 0 {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!(
                "catalog automatic WAL checkpoint is enabled actual=[{wal_autocheckpoint_pages}]"
            ),
        ));
    }
    let (catalog_format, schema_version, catalog_revision) =
        migration::validate_metadata(connection)?;
    Ok(SqliteCatalogFoundationStatus {
        authority_state: CatalogAuthorityState::FoundationOnly,
        metadata: CatalogMetadata {
            catalog_format,
            schema_version,
            catalog_revision,
        },
        journal_mode: journal_mode.to_ascii_lowercase(),
        foreign_keys_enabled: true,
        busy_timeout_ms,
        integrity_check_timeout_ms: duration_millis(config.integrity_check_timeout)?,
        wal_autocheckpoint_pages,
        integrity: last_integrity,
        actor_thread_id: format!("{:?}", thread::current().id()),
    })
}

fn pragma<T>(connection: &Connection, name: &str) -> Result<T>
where
    T: rusqlite::types::FromSql,
{
    connection
        .pragma_query_value(None, name, |row| row.get(0))
        .map_err(|error| map_sqlite_error("inspect catalog pragma", error))
}

fn run_quick_check(connection: &Connection, timeout: Duration) -> Result<CatalogIntegrity> {
    let started = Instant::now();
    connection.progress_handler(
        QUICK_CHECK_PROGRESS_INTERVAL,
        Some(move || started.elapsed() >= timeout),
    );
    let result = run_quick_check_inner(connection);
    connection.progress_handler(0, None::<fn() -> bool>);
    result
}

fn run_quick_check_inner(connection: &Connection) -> Result<CatalogIntegrity> {
    let mut statement = connection
        .prepare("PRAGMA quick_check(1)")
        .map_err(|error| map_sqlite_error("prepare catalog quick check", error))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| map_sqlite_error("run catalog quick check", error))?;
    let mut diagnostics = Vec::new();
    for row in rows {
        diagnostics
            .push(row.map_err(|error| map_sqlite_error("read catalog quick check result", error))?);
    }
    if diagnostics != ["ok"] {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog quick check failed diagnostics=[{}]",
                diagnostics.join("; ")
            ),
        ));
    }
    run_foreign_key_check(connection)?;
    Ok(CatalogIntegrity::Ok)
}

fn run_foreign_key_check(connection: &Connection) -> Result<()> {
    let mut statement = connection
        .prepare("PRAGMA foreign_key_check")
        .map_err(|error| map_sqlite_error("prepare catalog foreign key check", error))?;
    let mut rows = statement
        .query([])
        .map_err(|error| map_sqlite_error("run catalog foreign key check", error))?;
    let Some(row) = rows
        .next()
        .map_err(|error| map_sqlite_error("read catalog foreign key check result", error))?
    else {
        return Ok(());
    };
    let table: String = row
        .get(0)
        .map_err(|error| map_sqlite_error("decode catalog foreign key table", error))?;
    let row_id: Option<i64> = row
        .get(1)
        .map_err(|error| map_sqlite_error("decode catalog foreign key row id", error))?;
    let parent: String = row
        .get(2)
        .map_err(|error| map_sqlite_error("decode catalog foreign key parent", error))?;
    let constraint: i64 = row
        .get(3)
        .map_err(|error| map_sqlite_error("decode catalog foreign key constraint", error))?;
    Err(ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            concat!(
                "catalog foreign key check failed table=[{}] row_id=[{:?}] ",
                "parent=[{}] constraint=[{}]"
            ),
            table, row_id, parent, constraint
        ),
    ))
}

fn validate_managed_catalog_path(config: &SqliteCatalogConfig) -> Result<()> {
    validate_managed_directory(&config.data_root, "data root")?;
    validate_managed_directory(&config.catalog_directory, "catalog directory")?;
    let canonical_catalog = config.catalog_directory.canonicalize().map_err(|error| {
        ServerError::new(
            ErrorCode::Unavailable,
            format!("canonicalize managed catalog directory error=[{error}]"),
        )
    })?;
    if canonical_catalog != config.catalog_directory {
        return Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "managed catalog directory escaped its validated data root",
        ));
    }
    match std::fs::symlink_metadata(&config.path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "catalog database must not be a symbolic link",
        )),
        Ok(metadata) if !metadata.is_file() => Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog database path is not a regular file",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ServerError::new(
            ErrorCode::Unavailable,
            format!("inspect catalog database path error=[{error}]"),
        )),
    }
}

fn validate_managed_directory(path: &Path, role: &str) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        ServerError::new(
            ErrorCode::Unavailable,
            format!("inspect managed {role} error=[{error}]"),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ServerError::new(
            ErrorCode::PermissionDenied,
            format!("managed {role} must not be a symbolic link"),
        ));
    }
    if !metadata.is_dir() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("managed {role} is not a directory"),
        ));
    }
    Ok(())
}

fn validate_config(config: &SqliteCatalogConfig) -> Result<()> {
    debug_assert!(!config.path.as_os_str().is_empty());
    if !(1..=MAX_COMMAND_CAPACITY).contains(&config.command_capacity) {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog command capacity is outside bounds min=[1] max=[{}] actual=[{}]",
                MAX_COMMAND_CAPACITY, config.command_capacity
            ),
        ));
    }
    for (name, capacity) in [
        ("control read", config.control_read_capacity),
        ("snapshot read", config.snapshot_read_capacity),
    ] {
        if !(1..=MAX_READ_CAPACITY).contains(&capacity) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!(
                        "catalog {} capacity is outside bounds ",
                        "min=[1] max=[{}] actual=[{}]"
                    ),
                    name, MAX_READ_CAPACITY, capacity
                ),
            ));
        }
    }
    if !(1..=MAX_MANIFEST_QUEUE_CAPACITY_BYTES).contains(&config.manifest_queue_capacity_bytes) {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog manifest queue byte capacity is outside bounds min=[1] max=[{}] actual=[{}]",
                MAX_MANIFEST_QUEUE_CAPACITY_BYTES, config.manifest_queue_capacity_bytes
            ),
        ));
    }
    for (name, value, maximum) in [
        (
            "active pins",
            config.runtime_snapshot_pin_max_active_pins,
            zippy_core::MAX_CATALOG_RUNTIME_SNAPSHOT_PINS,
        ),
        (
            "active tables",
            config.runtime_snapshot_pin_max_active_tables,
            zippy_core::MAX_CATALOG_RUNTIME_SNAPSHOT_PIN_TABLES,
        ),
    ] {
        if !(1..=maximum).contains(&value) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    concat!(
                        "catalog runtime snapshot pin {} limit is outside bounds ",
                        "min=[1] max=[{}] actual=[{}]"
                    ),
                    name, maximum, value
                ),
            ));
        }
    }
    if config.busy_timeout.as_millis() > MAX_BUSY_TIMEOUT_MILLIS {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog busy timeout exceeds SQLite limit max_ms=[{}] actual_ms=[{}]",
                MAX_BUSY_TIMEOUT_MILLIS,
                config.busy_timeout.as_millis()
            ),
        ));
    }
    if config.integrity_check_timeout.as_millis() < 1 {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog integrity check timeout must be at least one millisecond",
        ));
    }
    let _ = duration_millis(config.integrity_check_timeout)?;
    Ok(())
}

fn duration_millis(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).map_err(|_| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog duration exceeds supported millisecond range",
        )
    })
}

fn actor_stopped() -> ServerError {
    ServerError::new(ErrorCode::Unavailable, "catalog actor is not running")
}

fn join_startup_actor(actor: JoinHandle<()>) -> Result<()> {
    actor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "catalog actor thread panicked during startup",
        )
    })
}

pub(crate) fn map_sqlite_error(context: &str, error: SqliteError) -> ServerError {
    let code = match &error {
        SqliteError::SqliteFailure(inner, _) => map_sqlite_failure_code(inner.code),
        SqliteError::FromSqlConversionFailure(_, _, _)
        | SqliteError::IntegralValueOutOfRange(_, _)
        | SqliteError::Utf8Error(_)
        | SqliteError::InvalidColumnType(_, _, _)
        | SqliteError::QueryReturnedNoRows => ErrorCode::DataCorruption,
        _ => ErrorCode::Internal,
    };
    ServerError::new(code, format!("{context} error=[{error}]"))
}

fn map_sqlite_failure_code(code: rusqlite::ffi::ErrorCode) -> ErrorCode {
    use rusqlite::ffi::ErrorCode as SqliteErrorCode;

    match code {
        SqliteErrorCode::DatabaseCorrupt | SqliteErrorCode::NotADatabase => {
            ErrorCode::DataCorruption
        }
        SqliteErrorCode::DatabaseBusy
        | SqliteErrorCode::DatabaseLocked
        | SqliteErrorCode::CannotOpen
        | SqliteErrorCode::SystemIoFailure => ErrorCode::Unavailable,
        SqliteErrorCode::OperationInterrupted => ErrorCode::DeadlineExceeded,
        _ => ErrorCode::Internal,
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::ffi::ErrorCode as SqliteErrorCode;
    use zippy_types::ErrorCode;

    use super::map_sqlite_failure_code;

    #[test]
    fn maps_progress_handler_interrupt_to_deadline_exceeded() {
        assert_eq!(
            map_sqlite_failure_code(SqliteErrorCode::OperationInterrupted),
            ErrorCode::DeadlineExceeded
        );
    }
}
