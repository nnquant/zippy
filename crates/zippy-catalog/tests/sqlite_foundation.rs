use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rusqlite::Connection;
use tempfile::TempDir;
use zippy_catalog::{
    CatalogAuthorityState, CatalogIntegrity, SqliteCatalogConfig, SqliteCatalogFoundation,
    CATALOG_FORMAT, CATALOG_SCHEMA_VERSION,
};
use zippy_core::{DataDirectory, ManagedDataDirectory};
use zippy_types::ErrorCode;

const EXPECTED_TABLES: [&str; 2] = ["catalog_meta", "catalog_migrations"];

fn data_directory(directory: &TempDir) -> DataDirectory {
    DataDirectory::initialize(directory.path().join("data")).expect("initialize data root")
}

fn catalog_path(data_directory: &DataDirectory) -> std::path::PathBuf {
    data_directory
        .path(ManagedDataDirectory::Catalog)
        .join("catalog.db")
}

#[test]
fn rolls_back_an_interrupted_transaction_and_reopens_in_wal_mode() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog");
    let status = catalog.inspect().expect("inspect catalog");

    assert_eq!(
        status.authority_state,
        CatalogAuthorityState::FoundationOnly
    );
    assert!(!status.authority_state.is_authoritative());
    assert_eq!(status.metadata.catalog_format, CATALOG_FORMAT);
    assert_eq!(status.metadata.schema_version, CATALOG_SCHEMA_VERSION);
    assert_eq!(status.metadata.catalog_revision, 0);
    assert_eq!(status.journal_mode, "wal");
    assert!(status.foreign_keys_enabled);
    assert_eq!(status.busy_timeout_ms, 5_000);
    assert_eq!(status.integrity_check_timeout_ms, 5_000);
    assert_eq!(status.wal_autocheckpoint_pages, 0);
    assert_eq!(status.integrity, CatalogIntegrity::Ok);
    catalog.close().expect("close catalog");

    let raw = Connection::open(&path).expect("open raw catalog");
    raw.execute_batch("BEGIN IMMEDIATE")
        .expect("begin interrupted transaction");
    raw.execute(
        "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
        ("uncommitted", &[1_u8] as &[u8]),
    )
    .expect("insert uncommitted row");
    drop(raw);

    let reopened = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("recover catalog");
    assert_eq!(reopened.quick_check(), Ok(CatalogIntegrity::Ok));
    assert_eq!(
        reopened
            .inspect()
            .expect("inspect reopened")
            .authority_state,
        CatalogAuthorityState::FoundationOnly
    );
    reopened.close().expect("close reopened catalog");

    let raw = Connection::open(path).expect("inspect recovered catalog");
    let count: u64 = raw
        .query_row(
            "SELECT count(*) FROM catalog_meta WHERE key = 'uncommitted'",
            [],
            |row| row.get(0),
        )
        .expect("count interrupted metadata");
    assert_eq!(count, 0, "interrupted transaction must roll back");
}

#[test]
fn migration_is_idempotent_and_creates_the_complete_v1_foundation() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    for _ in 0..3 {
        SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
            .expect("open catalog")
            .close()
            .expect("close catalog");
    }

    let raw = Connection::open(path).expect("inspect catalog");
    let tables = raw
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*'")
        .expect("prepare table list")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query table list")
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .expect("collect tables");
    assert_eq!(
        tables,
        EXPECTED_TABLES.map(str::to_owned).into_iter().collect()
    );
    let version: u32 = raw
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read schema version");
    assert_eq!(version, CATALOG_SCHEMA_VERSION);
    let application_id: u32 = raw
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .expect("read application id");
    assert_eq!(application_id, zippy_catalog::CATALOG_APPLICATION_ID);
    for forbidden_domain_table in [
        "namespaces",
        "tables",
        "table_schemas",
        "table_snapshots",
        "data_files",
        "idempotent_requests",
        "maintenance_jobs",
    ] {
        assert!(!tables.contains(forbidden_domain_table));
    }
}

#[test]
fn rejects_catalog_from_a_future_schema_version() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog")
        .close()
        .expect("close catalog");

    let raw = Connection::open(&path).expect("open raw catalog");
    raw.pragma_update(None, "user_version", CATALOG_SCHEMA_VERSION + 1)
        .expect("write future version");
    drop(raw);

    let error = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .err()
        .expect("future catalog must be rejected");
    assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
}

#[test]
fn serializes_all_inspection_on_one_actor_thread() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let catalog = Arc::new(
        SqliteCatalogFoundation::open(
            SqliteCatalogConfig::new(&data_directory)
                .with_command_capacity(16)
                .with_busy_timeout(Duration::from_millis(750)),
        )
        .expect("initialize catalog"),
    );
    let expected_actor = catalog.initial_status().actor_thread_id.clone();
    let workers = (0..8)
        .map(|_| {
            let catalog = Arc::clone(&catalog);
            thread::spawn(move || catalog.inspect().expect("inspect catalog").actor_thread_id)
        })
        .collect::<Vec<_>>();

    for worker in workers {
        assert_eq!(worker.join().expect("join inspector"), expected_actor);
    }
    let status = catalog.inspect().expect("inspect actor");
    assert_eq!(status.busy_timeout_ms, 750);
    assert_eq!(status.integrity, CatalogIntegrity::Ok);
}

#[test]
fn rejects_an_unbounded_actor_configuration() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let error = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory).with_command_capacity(0),
    )
    .err()
    .expect("zero-capacity actor must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
}

#[test]
fn rejects_an_unbounded_integrity_check_configuration() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let error = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory).with_integrity_check_timeout(Duration::ZERO),
    )
    .err()
    .expect("zero integrity timeout must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
}

#[test]
fn rejects_sub_millisecond_integrity_timeout() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let error = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory)
            .with_integrity_check_timeout(Duration::from_nanos(999_999)),
    )
    .err()
    .expect("sub-millisecond integrity timeout must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
}

#[test]
fn rejects_busy_timeout_above_sqlite_integer_limit_without_panicking() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let error = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory)
            .with_busy_timeout(Duration::from_millis(i32::MAX as u64 + 1)),
    )
    .err()
    .expect("oversized busy timeout must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
}

#[test]
fn rejects_actor_capacity_above_the_allocation_limit() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let error = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory).with_command_capacity(4_097),
    )
    .err()
    .expect("oversized actor queue must be rejected");
    assert_eq!(error.code(), ErrorCode::InvalidArgument);
}

#[test]
fn inspect_rejects_tampered_catalog_format_metadata() {
    assert_live_tamper_rejected(
        "UPDATE catalog_meta SET value = CAST('other-format' AS BLOB) \
         WHERE key = 'catalog_format'",
    );
}

#[test]
fn inspect_rejects_tampered_schema_metadata() {
    assert_live_tamper_rejected(
        "UPDATE catalog_meta SET value = X'0000000000000002' \
         WHERE key = 'schema_version'",
    );
}

#[test]
fn inspect_rejects_tampered_migration_checksum() {
    assert_live_tamper_rejected("UPDATE catalog_migrations SET migration_checksum = zeroblob(32)");
}

#[test]
fn inspect_rejects_invalid_migration_audit_timestamp_width() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog");
    let raw = Connection::open(path).expect("open raw catalog");
    raw.execute_batch(
        "PRAGMA ignore_check_constraints = ON; \
         UPDATE catalog_migrations SET applied_at_ns = X'00'",
    )
    .expect("tamper migration timestamp");
    drop(raw);

    let error = catalog
        .inspect()
        .expect_err("invalid timestamp must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[test]
fn inspect_maps_integer_migration_timestamp_to_data_corruption() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog");
    let raw = Connection::open(path).expect("open raw catalog");
    raw.execute_batch(
        "PRAGMA ignore_check_constraints = ON; \
         UPDATE catalog_migrations SET applied_at_ns = 1",
    )
    .expect("change migration timestamp storage class");
    drop(raw);

    let error = catalog
        .inspect()
        .expect_err("invalid storage class must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[test]
fn inspect_maps_negative_migration_version_to_data_corruption() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog");
    let raw = Connection::open(path).expect("open raw catalog");
    raw.execute_batch(
        "PRAGMA ignore_check_constraints = ON; \
         UPDATE catalog_migrations SET schema_version = -1",
    )
    .expect("write negative migration version");
    drop(raw);

    let error = catalog
        .inspect()
        .expect_err("negative version must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[test]
fn inspect_rejects_an_extra_migration_audit_record() {
    assert_live_tamper_rejected(
        "INSERT INTO catalog_migrations( \
             schema_version, migration_name, migration_checksum, applied_at_ns \
         ) VALUES (2, 'unexpected', zeroblob(32), zeroblob(8))",
    );
}

#[test]
fn inspect_rejects_a_missing_migration_audit_record() {
    assert_live_tamper_rejected("DELETE FROM catalog_migrations");
}

#[test]
fn inspect_rejects_an_unexpected_schema_object() {
    assert_live_tamper_rejected("CREATE TABLE unexpected(value TEXT)");
}

#[test]
fn inspect_rejects_a_sqlitex_prefixed_user_schema_object() {
    assert_live_tamper_rejected("CREATE TABLE sqliteXevil(value TEXT)");
}

#[test]
fn inspect_rejects_catalog_table_column_drift() {
    assert_live_tamper_rejected("ALTER TABLE catalog_meta ADD COLUMN unexpected TEXT");
}

#[test]
fn inspect_maps_a_dropped_catalog_table_to_data_corruption() {
    assert_live_tamper_rejected("DROP TABLE catalog_meta");
}

#[test]
fn inspect_maps_a_renamed_catalog_table_to_data_corruption() {
    assert_live_tamper_rejected("ALTER TABLE catalog_meta RENAME TO renamed_catalog_meta");
}

#[test]
fn sqlite_analyze_statistics_do_not_count_as_schema_drift() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog")
        .close()
        .expect("close catalog");

    let raw = Connection::open(&path).expect("open raw catalog");
    raw.execute_batch("ANALYZE").expect("analyze catalog");
    let statistics_objects: u64 = raw
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name GLOB 'sqlite_stat*'",
            [],
            |row| row.get(0),
        )
        .expect("count SQLite statistics objects");
    assert!(statistics_objects > 0);
    drop(raw);

    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("SQLite statistics must not count as schema drift")
        .close()
        .expect("close reopened catalog");
}

#[test]
fn rejects_negative_user_version_as_data_corruption() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog")
        .close()
        .expect("close catalog");

    let raw = Connection::open(path).expect("open raw catalog");
    raw.execute_batch("PRAGMA user_version = -1")
        .expect("write negative schema version");
    drop(raw);

    let error = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .err()
        .expect("negative schema version must be rejected");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

#[test]
fn rejects_future_catalog_before_enabling_wal() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let raw = Connection::open(&path).expect("create future catalog");
    raw.pragma_update(
        None,
        "application_id",
        zippy_catalog::CATALOG_APPLICATION_ID,
    )
    .expect("mark future catalog");
    raw.pragma_update(None, "user_version", CATALOG_SCHEMA_VERSION + 1)
        .expect("write future version");
    let journal_before: String = raw
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read initial journal mode");
    assert_eq!(journal_before, "delete");
    drop(raw);
    let main_before = std::fs::read(&path).expect("read future catalog before rejection");
    assert_no_sqlite_sidecars(&path);

    let error = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .err()
        .expect("future catalog must be rejected");
    assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
    assert_eq!(
        std::fs::read(&path).expect("read future catalog after rejection"),
        main_before
    );
    assert_no_sqlite_sidecars(&path);

    let raw = Connection::open(&path).expect("reopen rejected future catalog");
    let journal_after: String = raw
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read unchanged journal mode");
    assert_eq!(journal_after, "delete");
}

#[test]
fn rejects_foreign_schema_before_enabling_wal() {
    assert_foreign_schema_rejected("CREATE TABLE foreign_table(value TEXT)");
}

#[test]
fn rejects_a_sqlitex_prefixed_foreign_schema_before_enabling_wal() {
    assert_foreign_schema_rejected("CREATE TABLE sqliteXevil(value TEXT)");
}

fn assert_foreign_schema_rejected(sql: &str) {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let raw = Connection::open(&path).expect("create foreign database");
    raw.execute_batch(sql).expect("create foreign schema");
    drop(raw);
    let main_before = std::fs::read(&path).expect("read foreign database before rejection");
    assert_no_sqlite_sidecars(&path);

    let error = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .err()
        .expect("foreign database must be rejected");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert_eq!(
        std::fs::read(&path).expect("read foreign database after rejection"),
        main_before
    );
    assert_no_sqlite_sidecars(&path);

    let raw = Connection::open(&path).expect("reopen rejected foreign database");
    let journal_mode: String = raw
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("read unchanged journal mode");
    assert_eq!(journal_mode, "delete");
}

#[cfg(unix)]
#[test]
fn rejects_a_symbolic_link_catalog_database() {
    use std::os::unix::fs::symlink;

    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let target = directory.path().join("outside.db");
    Connection::open(&target).expect("create symlink target");
    symlink(target, path).expect("create catalog symlink");

    let error = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .err()
        .expect("catalog symlink must be rejected");
    assert_eq!(error.code(), ErrorCode::PermissionDenied);
}

#[test]
fn failed_explicit_integrity_check_blocks_inspect_until_a_successful_recheck() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(
        SqliteCatalogConfig::new(&data_directory)
            .with_integrity_check_timeout(Duration::from_millis(1)),
    )
    .expect("initialize catalog");
    let raw = Connection::open(&path).expect("open raw catalog");
    raw.execute_batch(
        "WITH RECURSIVE counter(value) AS ( \
             VALUES(1) UNION ALL SELECT value + 1 FROM counter WHERE value < 50000 \
         ) \
         INSERT INTO catalog_meta(key, value) \
         SELECT printf('bulk-%06d', value), zeroblob(128) FROM counter",
    )
    .expect("grow catalog for timeout");
    drop(raw);

    let check_error = catalog
        .quick_check()
        .expect_err("integrity check must exceed its VM execution threshold");
    assert_eq!(check_error.code(), ErrorCode::DeadlineExceeded);
    let inspect_error = catalog
        .inspect()
        .expect_err("failed integrity state must fail closed");
    assert_eq!(inspect_error.code(), ErrorCode::DeadlineExceeded);

    let raw = Connection::open(path).expect("open raw catalog for cleanup");
    raw.execute("DELETE FROM catalog_meta WHERE key LIKE 'bulk-%'", [])
        .expect("delete bulk metadata");
    raw.execute_batch("VACUUM").expect("compact test catalog");
    drop(raw);

    assert_eq!(catalog.quick_check(), Ok(CatalogIntegrity::Ok));
    assert_eq!(
        catalog
            .inspect()
            .expect("inspect recovered catalog")
            .integrity,
        CatalogIntegrity::Ok
    );
}

fn assert_live_tamper_rejected(sql: &str) {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    let catalog = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog");
    let raw = Connection::open(path).expect("open raw catalog");
    raw.execute_batch(sql).expect("tamper catalog");
    drop(raw);

    let error = catalog
        .inspect()
        .expect_err("tampered catalog must fail closed");
    assert_eq!(error.code(), ErrorCode::DataCorruption);
}

fn assert_no_sqlite_sidecars(path: &std::path::Path) {
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        assert!(
            !std::path::Path::new(&sidecar).exists(),
            "preflight rejection must not create {suffix}"
        );
    }
}
