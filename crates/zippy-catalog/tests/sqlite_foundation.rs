use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rusqlite::{params, Connection};
use tempfile::TempDir;
use zippy_catalog::{
    CatalogAuthorityState, CatalogIntegrity, SqliteCatalogConfig, SqliteCatalogFoundation,
    CATALOG_FORMAT, CATALOG_SCHEMA_VERSION,
};
use zippy_core::{ManagedDataDirectory, OwnedDataDirectory};
use zippy_types::ErrorCode;

const EXPECTED_TABLES: [&str; 14] = [
    "catalog_meta",
    "catalog_migrations",
    "data_file_sources",
    "data_files",
    "durable_consumer_offsets",
    "durable_consumers",
    "idempotent_requests",
    "maintenance_jobs",
    "namespaces",
    "producers",
    "snapshot_leases",
    "table_schemas",
    "table_snapshots",
    "tables",
];

const EXPECTED_ACTIVE_TABLE_CURSOR_INDEXES: [&str; 2] =
    ["tables_by_namespace_state_id", "tables_by_state_id"];

fn data_directory(directory: &TempDir) -> Arc<OwnedDataDirectory> {
    Arc::new(
        OwnedDataDirectory::acquire_or_initialize(directory.path().join("data"))
            .expect("own data root"),
    )
}

fn catalog_path(data_directory: &Arc<OwnedDataDirectory>) -> std::path::PathBuf {
    data_directory
        .directory()
        .path(ManagedDataDirectory::Catalog)
        .join("catalog.db")
}

fn stable_id_bytes(suffix: u8) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[15] = suffix;
    bytes
}

fn tagged_some(value: u64) -> [u8; 9] {
    let mut encoded = [0_u8; 9];
    encoded[0] = 1;
    encoded[1..].copy_from_slice(&value.to_be_bytes());
    encoded
}

fn insert_test_namespace_and_table(
    connection: &Connection,
    current_schema_version: &[u8; 8],
    current_snapshot_version: &[u8; 8],
) {
    let zero_u64 = 0_u64.to_be_bytes();
    let payload = [0_u8];
    connection
        .execute(
            "INSERT INTO namespaces(namespace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
            params![
                stable_id_bytes(1).as_slice(),
                "foreign_key_test",
                zero_u64.as_slice(),
            ],
        )
        .expect("insert test namespace");
    connection
        .execute(
            "INSERT INTO tables(
                 table_id, namespace_id, name, table_kind, definition_blob,
                 current_schema_version, current_snapshot_version, writer_epoch,
                 state, config_blob, created_at_ns, updated_at_ns
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                stable_id_bytes(2).as_slice(),
                stable_id_bytes(1).as_slice(),
                "foreign_key_table",
                "append_table",
                payload.as_slice(),
                current_schema_version.as_slice(),
                current_snapshot_version.as_slice(),
                zero_u64.as_slice(),
                "active",
                payload.as_slice(),
                zero_u64.as_slice(),
                zero_u64.as_slice(),
            ],
        )
        .expect("insert test table");
}

fn insert_test_schema(connection: &Connection, schema_version: &[u8; 8]) {
    let zero_u64 = 0_u64.to_be_bytes();
    connection
        .execute(
            "INSERT INTO table_schemas(
                 table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                stable_id_bytes(2).as_slice(),
                schema_version.as_slice(),
                [1_u8].as_slice(),
                "sha256:test",
                "initial",
                zero_u64.as_slice(),
            ],
        )
        .expect("insert test schema");
}

fn insert_test_snapshot(
    connection: &Connection,
    snapshot_version: &[u8; 8],
    parent_snapshot_version: &[u8; 9],
    operation: &str,
) {
    let zero_u64 = 0_u64.to_be_bytes();
    let empty_watermark = [0_u8; 9];
    connection
        .execute(
            "INSERT INTO table_snapshots(
                 table_id, snapshot_version, parent_snapshot_version, operation,
                 commit_lsn, committed_log_offset, created_at_ns, summary_blob
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                stable_id_bytes(2).as_slice(),
                snapshot_version.as_slice(),
                parent_snapshot_version.as_slice(),
                operation,
                zero_u64.as_slice(),
                empty_watermark.as_slice(),
                zero_u64.as_slice(),
                [0_u8].as_slice(),
            ],
        )
        .expect("insert test snapshot");
}

fn insert_test_data_file(
    connection: &Connection,
    file_id: &[u8; 16],
    file_path: &str,
    removed_snapshot_version: &[u8; 9],
    state: &str,
) {
    let zero = 0_u64.to_be_bytes();
    let none = [0_u8; 9];
    connection
        .execute(
            "INSERT INTO data_files(
                 file_id, table_id, schema_version, location_kind, file_path,
                 file_size_bytes, row_count, provenance_kind, min_log_offset,
                 max_log_offset, legacy_preserve_discovery_order,
                 legacy_discovery_order, partition_blob, statistics_blob,
                 metadata_blob, checksum, added_snapshot_version,
                 removed_snapshot_version, state, created_at_ns
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                 ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
             )",
            params![
                file_id.as_slice(),
                stable_id_bytes(2).as_slice(),
                zero.as_slice(),
                "managed",
                file_path,
                zero.as_slice(),
                1_u64.to_be_bytes().as_slice(),
                "legacy",
                none.as_slice(),
                none.as_slice(),
                0_i64,
                none.as_slice(),
                [0_u8].as_slice(),
                [0_u8].as_slice(),
                [0_u8].as_slice(),
                "sha256:test",
                zero.as_slice(),
                removed_snapshot_version.as_slice(),
                state,
                zero.as_slice(),
            ],
        )
        .expect("insert test data file");
}

fn assert_constraint_violation(error: rusqlite::Error) {
    assert_eq!(
        error.sqlite_error_code(),
        Some(rusqlite::ffi::ErrorCode::ConstraintViolation)
    );
}

fn initialized_raw_catalog(directory: &TempDir) -> Connection {
    let data_directory = data_directory(directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize test catalog")
        .close()
        .expect("close test catalog");
    let connection = Connection::open(path).expect("open raw test catalog");
    connection
        .pragma_update(None, "foreign_keys", true)
        .expect("enable test foreign keys");
    connection
}

fn assert_active_table_cursor_indexes(connection: &Connection) {
    let cursor_indexes = connection
        .prepare(
            "SELECT name FROM sqlite_schema
              WHERE type = 'index'
                AND name IN ('tables_by_state_id', 'tables_by_namespace_state_id')",
        )
        .expect("prepare cursor index inventory")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query cursor index inventory")
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .expect("collect cursor index inventory");
    assert_eq!(
        cursor_indexes,
        EXPECTED_ACTIVE_TABLE_CURSOR_INDEXES
            .map(str::to_owned)
            .into_iter()
            .collect()
    );
}

fn initialize_catalog_at_physical_version(
    data_directory: &Arc<OwnedDataDirectory>,
    physical_version: u32,
) {
    if physical_version == 0 {
        return;
    }
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(data_directory))
        .expect("initialize current catalog fixture")
        .close()
        .expect("close current catalog fixture");

    let connection =
        Connection::open(catalog_path(data_directory)).expect("open catalog fixture for downgrade");
    match physical_version {
        1 => connection
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 DROP TABLE idempotent_requests;
                 DROP TABLE maintenance_jobs;
                 DROP TABLE snapshot_leases;
                 DROP TABLE durable_consumer_offsets;
                 DROP TABLE durable_consumers;
                 DROP TABLE producers;
                 DROP TABLE data_file_sources;
                 DROP TABLE data_files;
                 DROP TABLE table_snapshots;
                 DROP TABLE table_schemas;
                 DROP TABLE tables;
                 DROP TABLE namespaces;
                 DELETE FROM catalog_migrations WHERE schema_version > 1;
                 UPDATE catalog_meta
                    SET value = X'0000000000000001'
                  WHERE key = 'schema_version';
                 PRAGMA user_version = 1;",
            )
            .expect("construct exact physical v1 fixture"),
        2 => connection
            .execute_batch(
                "DROP INDEX tables_by_state_id;
                 DROP INDEX tables_by_namespace_state_id;
                 DELETE FROM catalog_migrations WHERE schema_version > 2;
                 UPDATE catalog_meta
                    SET value = X'0000000000000002'
                  WHERE key = 'schema_version';
                 PRAGMA user_version = 2;",
            )
            .expect("construct exact physical v2 fixture"),
        unsupported => panic!("unsupported physical catalog fixture version [{unsupported}]"),
    }
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
fn migration_is_idempotent_and_creates_the_current_domain_schema() {
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
    let migration_versions = raw
        .prepare("SELECT schema_version FROM catalog_migrations ORDER BY schema_version")
        .expect("prepare migration audit query")
        .query_map([], |row| row.get::<_, u32>(0))
        .expect("query migration audit")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect migration versions");
    assert_eq!(migration_versions, vec![1, 2, 3]);
}

#[test]
fn concurrent_opens_migrate_fresh_v1_and_v2_catalogs_exactly_once() {
    const ACTOR_COUNT: usize = 8;

    for physical_version in [0, 1, 2] {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        initialize_catalog_at_physical_version(&data_directory, physical_version);

        let barrier = Arc::new(Barrier::new(ACTOR_COUNT));
        let config = SqliteCatalogConfig::new(&data_directory);
        let actors = (0..ACTOR_COUNT)
            .map(|_| {
                let actor_barrier = Arc::clone(&barrier);
                let actor_config = config.clone();
                thread::spawn(move || {
                    actor_barrier.wait();
                    let catalog = SqliteCatalogFoundation::open(actor_config)
                        .expect("concurrently open and migrate catalog");
                    let migrated_version = catalog
                        .inspect()
                        .expect("inspect concurrently migrated catalog")
                        .metadata
                        .schema_version;
                    catalog.close().expect("close concurrent catalog actor");
                    migrated_version
                })
            })
            .collect::<Vec<_>>();

        for actor in actors {
            assert_eq!(
                actor.join().expect("join concurrent catalog actor"),
                CATALOG_SCHEMA_VERSION,
                "all actors must observe the current schema from physical version \
                 [{physical_version}]"
            );
        }

        let raw = Connection::open(catalog_path(&data_directory))
            .expect("inspect concurrently migrated catalog");
        let migration_versions = raw
            .prepare("SELECT schema_version FROM catalog_migrations ORDER BY schema_version")
            .expect("prepare concurrent migration audit query")
            .query_map([], |row| row.get::<_, u32>(0))
            .expect("query concurrent migration audit")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect concurrent migration versions");
        assert_eq!(
            migration_versions,
            vec![1, 2, 3],
            "each migration must be recorded exactly once from physical version \
             [{physical_version}]"
        );
        assert_active_table_cursor_indexes(&raw);
    }
}

#[test]
fn upgrades_an_existing_physical_v1_catalog_to_v3_and_reopens() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize current catalog")
        .close()
        .expect("close current catalog");

    let raw = Connection::open(&path).expect("open catalog for v1 fixture downgrade");
    raw.execute_batch(
        "PRAGMA foreign_keys = OFF;
         DROP TABLE idempotent_requests;
         DROP TABLE maintenance_jobs;
         DROP TABLE snapshot_leases;
         DROP TABLE durable_consumer_offsets;
         DROP TABLE durable_consumers;
         DROP TABLE producers;
         DROP TABLE data_file_sources;
         DROP TABLE data_files;
         DROP TABLE table_snapshots;
         DROP TABLE table_schemas;
         DROP TABLE tables;
         DROP TABLE namespaces;
         DELETE FROM catalog_migrations WHERE schema_version > 1;
         UPDATE catalog_meta
            SET value = X'0000000000000001'
          WHERE key = 'schema_version';
         PRAGMA user_version = 1;",
    )
    .expect("construct exact physical v1 fixture");
    drop(raw);

    let upgraded = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("upgrade physical v1 catalog");
    assert_eq!(
        upgraded
            .inspect()
            .expect("inspect upgraded catalog")
            .metadata
            .schema_version,
        CATALOG_SCHEMA_VERSION
    );
    upgraded.close().expect("close upgraded catalog");

    let reopened = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen upgraded catalog");
    assert_eq!(
        reopened
            .inspect()
            .expect("inspect reopened catalog")
            .metadata
            .schema_version,
        CATALOG_SCHEMA_VERSION
    );
    reopened.close().expect("close reopened catalog");

    let raw = Connection::open(path).expect("inspect upgraded schema");
    let tables = raw
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT GLOB 'sqlite_*'")
        .expect("prepare upgraded table list")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query upgraded table list")
        .collect::<rusqlite::Result<BTreeSet<_>>>()
        .expect("collect upgraded tables");
    assert_eq!(
        tables,
        EXPECTED_TABLES.map(str::to_owned).into_iter().collect()
    );
    let migration_count: u32 = raw
        .query_row("SELECT count(*) FROM catalog_migrations", [], |row| {
            row.get(0)
        })
        .expect("count upgraded migration audit");
    assert_eq!(migration_count, 3);
    assert_active_table_cursor_indexes(&raw);
}

#[test]
fn upgrades_an_existing_physical_v2_catalog_to_v3_and_reopens() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize current catalog")
        .close()
        .expect("close current catalog");

    let raw = Connection::open(&path).expect("open catalog for v2 fixture downgrade");
    raw.execute_batch(
        "DROP INDEX tables_by_state_id;
         DROP INDEX tables_by_namespace_state_id;
         DELETE FROM catalog_migrations WHERE schema_version = 3;
         UPDATE catalog_meta
            SET value = X'0000000000000002'
          WHERE key = 'schema_version';
         PRAGMA user_version = 2;",
    )
    .expect("construct exact physical v2 fixture");
    drop(raw);

    let upgraded = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("upgrade physical v2 catalog");
    assert_eq!(
        upgraded
            .inspect()
            .expect("inspect upgraded catalog")
            .metadata
            .schema_version,
        CATALOG_SCHEMA_VERSION
    );
    upgraded.close().expect("close upgraded catalog");

    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen upgraded catalog")
        .close()
        .expect("close reopened catalog");

    let raw = Connection::open(path).expect("inspect upgraded schema");
    let migration_versions = raw
        .prepare("SELECT schema_version FROM catalog_migrations ORDER BY schema_version")
        .expect("prepare upgraded migration audit query")
        .query_map([], |row| row.get::<_, u32>(0))
        .expect("query upgraded migration audit")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect upgraded migration versions");
    assert_eq!(migration_versions, vec![1, 2, 3]);
    assert_active_table_cursor_indexes(&raw);
}

#[test]
fn rejects_a_dangling_current_schema_at_deferred_commit() {
    let directory = TempDir::new().expect("create temp directory");
    let mut raw = initialized_raw_catalog(&directory);
    let zero = 0_u64.to_be_bytes();
    let missing_schema = 1_u64.to_be_bytes();
    let none = [0_u8; 9];
    let transaction = raw
        .transaction()
        .expect("begin dangling schema transaction");
    insert_test_namespace_and_table(&transaction, &missing_schema, &zero);
    insert_test_snapshot(&transaction, &zero, &none, "create");

    let error = transaction
        .commit()
        .expect_err("dangling current schema must fail deferred commit");
    assert_constraint_violation(error);
}

#[test]
fn rejects_a_dangling_current_snapshot_at_deferred_commit() {
    let directory = TempDir::new().expect("create temp directory");
    let mut raw = initialized_raw_catalog(&directory);
    let zero = 0_u64.to_be_bytes();
    let missing_snapshot = 1_u64.to_be_bytes();
    let transaction = raw
        .transaction()
        .expect("begin dangling snapshot transaction");
    insert_test_namespace_and_table(&transaction, &zero, &missing_snapshot);
    insert_test_schema(&transaction, &zero);

    let error = transaction
        .commit()
        .expect_err("dangling current snapshot must fail deferred commit");
    assert_constraint_violation(error);
}

#[test]
fn generated_parent_snapshot_payload_rejects_a_dangling_reference() {
    let directory = TempDir::new().expect("create temp directory");
    let mut raw = initialized_raw_catalog(&directory);
    let zero = 0_u64.to_be_bytes();
    let none = [0_u8; 9];
    let root = raw.transaction().expect("begin valid root transaction");
    insert_test_namespace_and_table(&root, &zero, &zero);
    insert_test_schema(&root, &zero);
    insert_test_snapshot(&root, &zero, &none, "create");
    root.commit().expect("commit valid root catalog records");

    let snapshot_one = 1_u64.to_be_bytes();
    let dangling_parent = tagged_some(2);
    let transaction = raw
        .transaction()
        .expect("begin dangling parent transaction");
    insert_test_snapshot(&transaction, &snapshot_one, &dangling_parent, "persist");
    let generated_payload: Vec<u8> = transaction
        .query_row(
            "SELECT parent_snapshot_version_value
             FROM table_snapshots WHERE table_id = ?1 AND snapshot_version = ?2",
            params![stable_id_bytes(2).as_slice(), snapshot_one.as_slice()],
            |row| row.get(0),
        )
        .expect("read generated parent payload");
    assert_eq!(generated_payload, 2_u64.to_be_bytes());

    let error = transaction
        .commit()
        .expect_err("dangling parent snapshot must fail deferred commit");
    assert_constraint_violation(error);
}

#[test]
fn generated_removed_snapshot_payload_rejects_a_dangling_reference() {
    let directory = TempDir::new().expect("create temp directory");
    let mut raw = initialized_raw_catalog(&directory);
    let zero = 0_u64.to_be_bytes();
    let none = [0_u8; 9];
    let root = raw.transaction().expect("begin valid root transaction");
    insert_test_namespace_and_table(&root, &zero, &zero);
    insert_test_schema(&root, &zero);
    insert_test_snapshot(&root, &zero, &none, "create");
    root.commit().expect("commit valid root catalog records");

    let dangling_removed = tagged_some(2);
    let transaction = raw
        .transaction()
        .expect("begin dangling removal transaction");
    insert_test_data_file(
        &transaction,
        &stable_id_bytes(3),
        "dangling-removed.parquet",
        &dangling_removed,
        "deleting",
    );
    let generated_payload: Vec<u8> = transaction
        .query_row(
            "SELECT removed_snapshot_version_value FROM data_files WHERE file_id = ?1",
            [stable_id_bytes(3).as_slice()],
            |row| row.get(0),
        )
        .expect("read generated removal payload");
    assert_eq!(generated_payload, 2_u64.to_be_bytes());

    let error = transaction
        .commit()
        .expect_err("dangling removed snapshot must fail deferred commit");
    assert_constraint_violation(error);
}

#[test]
fn current_manifest_query_uses_a_covering_manifest_index() {
    let directory = TempDir::new().expect("create temp directory");
    let mut raw = initialized_raw_catalog(&directory);
    let zero = 0_u64.to_be_bytes();
    let snapshot_one = 1_u64.to_be_bytes();
    let none = [0_u8; 9];
    let parent_zero = tagged_some(0);
    let removed_at_one = tagged_some(1);
    let root = raw.transaction().expect("begin manifest root transaction");
    insert_test_namespace_and_table(&root, &zero, &snapshot_one);
    insert_test_schema(&root, &zero);
    insert_test_snapshot(&root, &zero, &none, "create");
    insert_test_snapshot(&root, &snapshot_one, &parent_zero, "persist");
    root.commit().expect("commit manifest root records");

    let files = raw.transaction().expect("begin manifest files transaction");
    insert_test_data_file(
        &files,
        &stable_id_bytes(3),
        "active.parquet",
        &none,
        "committed",
    );
    for suffix in 10_u8..74 {
        insert_test_data_file(
            &files,
            &stable_id_bytes(suffix),
            &format!("removed-{suffix:03}.parquet"),
            &removed_at_one,
            "deleted",
        );
    }
    files.commit().expect("commit manifest file distribution");
    raw.execute_batch("ANALYZE")
        .expect("analyze manifest index distribution");

    let mut statement = raw
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT file_id FROM data_files
              WHERE table_id = ?1
                AND state = 'committed'
                AND removed_snapshot_version = X'000000000000000000'
              ORDER BY file_id",
        )
        .expect("prepare current manifest query plan");
    let plan = statement
        .query_map([stable_id_bytes(2).as_slice()], |row| {
            row.get::<_, String>(3)
        })
        .expect("query current manifest plan")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect current manifest plan");
    assert!(
        plan.iter().any(|detail| {
            detail.contains("data_files_active_manifest")
                || detail.contains("data_files_by_gc_state")
        }),
        "current manifest plan must use a covering manifest index plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("SCAN data_files")),
        "current manifest plan must not scan the data_files table plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE")),
        "current manifest plan must preserve file-id order in-index plan=[{plan:?}]"
    );
}

#[test]
fn namespace_cursor_listing_seeks_by_namespace_id_without_a_temp_sort() {
    let directory = TempDir::new().expect("create temp directory");
    let raw = initialized_raw_catalog(&directory);
    let cursor = stable_id_bytes(2);
    let mut statement = raw
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT namespace_id, name, created_at_ns FROM namespaces
              WHERE namespace_id > ?1
              ORDER BY namespace_id
              LIMIT ?2",
        )
        .expect("prepare namespace cursor query plan");
    let plan = statement
        .query_map(params![cursor.as_slice(), 100_u32], |row| {
            row.get::<_, String>(3)
        })
        .expect("query namespace cursor plan")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect namespace cursor plan");
    assert!(
        plan.iter().any(|detail| {
            detail.contains("SEARCH namespaces") && detail.contains("namespace_id>?")
        }),
        "namespace cursor plan must seek from namespace_id plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("SCAN namespaces")),
        "namespace cursor plan must not scan namespaces plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE")),
        "namespace cursor plan must preserve namespace-id order in-index plan=[{plan:?}]"
    );
}

#[test]
fn active_table_cursor_listing_uses_the_state_id_index() {
    let directory = TempDir::new().expect("create temp directory");
    let raw = initialized_raw_catalog(&directory);
    let cursor = stable_id_bytes(2);
    let mut statement = raw
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT table_id FROM tables
              WHERE state = 'active'
                AND table_id > ?1
              ORDER BY table_id
              LIMIT ?2",
        )
        .expect("prepare global active table cursor query plan");
    let plan = statement
        .query_map(params![cursor.as_slice(), 100_u32], |row| {
            row.get::<_, String>(3)
        })
        .expect("query global active table cursor plan")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect global active table cursor plan");
    assert!(
        plan.iter()
            .any(|detail| detail.contains("tables_by_state_id")),
        "global active table cursor plan must use the state-id index plan=[{plan:?}]"
    );
    assert!(
        plan.iter().all(|detail| !detail.contains("SCAN tables")),
        "global active table cursor plan must not scan the tables table plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE")),
        "global active table cursor plan must preserve table-id order in-index plan=[{plan:?}]"
    );
}

#[test]
fn active_table_cursor_listing_uses_the_namespace_state_id_index() {
    let directory = TempDir::new().expect("create temp directory");
    let raw = initialized_raw_catalog(&directory);
    let namespace_id = stable_id_bytes(1);
    let cursor = stable_id_bytes(2);
    let mut statement = raw
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT table_id FROM tables
              WHERE namespace_id = ?1
                AND state = 'active'
                AND table_id > ?2
              ORDER BY table_id
              LIMIT ?3",
        )
        .expect("prepare active table cursor query plan");
    let plan = statement
        .query_map(
            params![namespace_id.as_slice(), cursor.as_slice(), 100_u32],
            |row| row.get::<_, String>(3),
        )
        .expect("query active table cursor plan")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect active table cursor plan");
    assert!(
        plan.iter()
            .any(|detail| detail.contains("tables_by_namespace_state_id")),
        "active table cursor plan must use the namespace-state-id index plan=[{plan:?}]"
    );
    assert!(
        plan.iter().all(|detail| !detail.contains("SCAN tables")),
        "active table cursor plan must not scan the tables table plan=[{plan:?}]"
    );
    assert!(
        plan.iter()
            .all(|detail| !detail.contains("USE TEMP B-TREE")),
        "active table cursor plan must preserve table-id order in-index plan=[{plan:?}]"
    );
}

#[test]
fn reopen_rejects_foreign_key_corruption_written_with_enforcement_disabled() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog")
        .close()
        .expect("close catalog");

    let raw = Connection::open(path).expect("open raw catalog");
    raw.pragma_update(None, "foreign_keys", false)
        .expect("disable foreign keys for corruption fixture");
    raw.execute(
        "INSERT INTO snapshot_leases(
             lease_id, table_id, snapshot_version, owner, expires_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            stable_id_bytes(1).as_slice(),
            stable_id_bytes(2).as_slice(),
            0_u64.to_be_bytes().as_slice(),
            "corrupt-fixture",
            0_u64.to_be_bytes().as_slice(),
        ],
    )
    .expect("insert dangling lease fixture");
    drop(raw);

    let error = match SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory)) {
        Ok(catalog) => {
            catalog.close().expect("close unexpectedly healthy catalog");
            panic!("foreign key corruption must fail catalog startup");
        }
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::DataCorruption);
    assert!(error.message().contains("foreign key check failed"));
}

#[test]
fn domain_schema_preserves_unsigned_extremes_and_canonical_tags_across_reopen() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    let path = catalog_path(&data_directory);
    SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("initialize catalog")
        .close()
        .expect("close catalog");

    let max_u64 = u64::MAX.to_be_bytes();
    let zero_u64 = 0_u64.to_be_bytes();
    let none_u64 = [0_u8; 9];
    let mut known_max = [u8::MAX; 9];
    known_max[0] = 1;
    let payload = [0xA5_u8];
    let namespace_id = stable_id_bytes(1);
    let table_id = stable_id_bytes(2);
    let file_id = stable_id_bytes(3);
    let producer_id = stable_id_bytes(4);
    let consumer_id = stable_id_bytes(5);
    let lease_id = stable_id_bytes(6);
    let job_id = stable_id_bytes(7);
    let request_id = stable_id_bytes(8);

    let mut raw = Connection::open(&path).expect("open raw catalog");
    raw.pragma_update(None, "foreign_keys", true)
        .expect("enable foreign keys");
    let transaction = raw.transaction().expect("begin extreme-value transaction");
    transaction
        .execute(
            "INSERT INTO namespaces(namespace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
            params![namespace_id.as_slice(), "extreme", max_u64.as_slice()],
        )
        .expect("insert extreme namespace");
    transaction
        .execute(
            "INSERT INTO tables(
             table_id, namespace_id, name, table_kind, definition_blob,
             current_schema_version, current_snapshot_version, writer_epoch,
             state, config_blob, created_at_ns, updated_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                table_id.as_slice(),
                namespace_id.as_slice(),
                "extreme_table",
                "append_table",
                payload.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
                "active",
                payload.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme table");
    transaction
        .execute(
            "INSERT INTO table_schemas(
             table_id, schema_version, schema_ipc, schema_hash, compatibility, created_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                table_id.as_slice(),
                max_u64.as_slice(),
                payload.as_slice(),
                "sha256:max",
                "initial",
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme table schema");
    transaction
        .execute(
            "INSERT INTO table_snapshots(
             table_id, snapshot_version, parent_snapshot_version, operation,
             commit_lsn, committed_log_offset, created_at_ns, summary_blob
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                table_id.as_slice(),
                max_u64.as_slice(),
                none_u64.as_slice(),
                "create",
                max_u64.as_slice(),
                known_max.as_slice(),
                max_u64.as_slice(),
                payload.as_slice(),
            ],
        )
        .expect("insert extreme table snapshot");
    transaction
        .execute(
            "INSERT INTO data_files(
             file_id, table_id, schema_version, location_kind, file_path,
             file_size_bytes, row_count, provenance_kind, min_log_offset,
             max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order,
             partition_blob, statistics_blob, metadata_blob, checksum,
             added_snapshot_version, removed_snapshot_version, state, created_at_ns
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
         )",
            params![
                file_id.as_slice(),
                table_id.as_slice(),
                max_u64.as_slice(),
                "managed",
                "extreme.parquet",
                max_u64.as_slice(),
                max_u64.as_slice(),
                "native",
                known_max.as_slice(),
                known_max.as_slice(),
                0_i64,
                none_u64.as_slice(),
                payload.as_slice(),
                payload.as_slice(),
                payload.as_slice(),
                "sha256:max",
                max_u64.as_slice(),
                none_u64.as_slice(),
                "committed",
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme data file");
    transaction
        .execute(
            "INSERT INTO data_file_sources(
             file_id, source_segment_id, source_generation, writer_epoch,
             min_log_offset, max_log_offset
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                file_id.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme data-file lineage");
    transaction
        .execute(
            "INSERT INTO producers(
             producer_id, table_id, producer_epoch, last_batch_sequence,
             last_committed_log_offset, updated_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                producer_id.as_slice(),
                table_id.as_slice(),
                max_u64.as_slice(),
                known_max.as_slice(),
                known_max.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme producer checkpoint");
    transaction
        .execute(
            "INSERT INTO durable_consumers(
             consumer_id, table_id, created_at_ns, updated_at_ns
         ) VALUES (?1, ?2, ?3, ?4)",
            params![
                consumer_id.as_slice(),
                table_id.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme durable consumer");
    transaction
        .execute(
            "INSERT INTO durable_consumer_offsets(
             consumer_id, table_id, authority_kind, authority_id,
             committed_log_offset, updated_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                consumer_id.as_slice(),
                table_id.as_slice(),
                "consumer",
                consumer_id.as_slice(),
                known_max.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme durable consumer offset");
    transaction
        .execute(
            "INSERT INTO snapshot_leases(
             lease_id, table_id, snapshot_version, owner, expires_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                lease_id.as_slice(),
                table_id.as_slice(),
                max_u64.as_slice(),
                "worker",
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme snapshot lease");
    transaction
        .execute(
            "INSERT INTO maintenance_jobs(
             job_id, job_kind, table_id, state, priority, attempt,
             lease_generation, lease_owner, lease_expires_at_ns,
             input_blob, progress_blob, error_text, created_at_ns, updated_at_ns
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
         )",
            params![
                job_id.as_slice(),
                "compaction",
                table_id.as_slice(),
                "running",
                i32::MAX,
                u32::MAX,
                max_u64.as_slice(),
                "worker",
                known_max.as_slice(),
                payload.as_slice(),
                payload.as_slice(),
                Option::<&str>::None,
                max_u64.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme maintenance job");
    transaction
        .execute(
            "INSERT INTO idempotent_requests(
             request_id, command_format_version, operation, command_fingerprint,
             response_blob, committed_at_ns, expires_at_ns
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                request_id.as_slice(),
                1_i64,
                "create_namespace",
                [0x5A_u8; 32].as_slice(),
                payload.as_slice(),
                max_u64.as_slice(),
                max_u64.as_slice(),
            ],
        )
        .expect("insert extreme idempotency record");
    transaction
        .commit()
        .expect("commit extreme-value transaction");

    let lossy_error = raw
        .execute(
            "INSERT INTO namespaces(namespace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
            params![stable_id_bytes(9).as_slice(), "lossy", i64::MAX],
        )
        .expect_err("integer timestamps must be rejected");
    assert_eq!(
        lossy_error.sqlite_error_code(),
        Some(rusqlite::ffi::ErrorCode::ConstraintViolation)
    );
    let mut noncanonical_none = none_u64;
    noncanonical_none[8] = 1;
    let tag_error = raw
        .execute(
            "INSERT INTO table_snapshots(
                 table_id, snapshot_version, parent_snapshot_version, operation,
                 commit_lsn, committed_log_offset, created_at_ns, summary_blob
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                table_id.as_slice(),
                zero_u64.as_slice(),
                noncanonical_none.as_slice(),
                "persist",
                zero_u64.as_slice(),
                known_max.as_slice(),
                max_u64.as_slice(),
                payload.as_slice(),
            ],
        )
        .expect_err("noncanonical optional u64 payload must be rejected");
    assert_eq!(
        tag_error.sqlite_error_code(),
        Some(rusqlite::ffi::ErrorCode::ConstraintViolation)
    );
    drop(raw);

    let reopened = SqliteCatalogFoundation::open(SqliteCatalogConfig::new(&data_directory))
        .expect("reopen catalog containing unsigned extremes");
    assert_eq!(reopened.quick_check(), Ok(CatalogIntegrity::Ok));
    reopened.close().expect("close reopened catalog");

    let raw = Connection::open(path).expect("verify unsigned extremes");
    let stored = raw
        .query_row(
            "SELECT writer_epoch, last_batch_sequence, last_committed_log_offset
             FROM tables JOIN producers USING(table_id)",
            [],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .expect("read persisted unsigned extremes");
    assert_eq!(stored.0, max_u64);
    assert_eq!(stored.1, known_max);
    assert_eq!(stored.2, known_max);
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
fn rejects_read_lane_capacities_outside_allocation_limits() {
    let directory = TempDir::new().expect("create temp directory");
    let data_directory = data_directory(&directory);
    for config in [
        SqliteCatalogConfig::new(&data_directory).with_control_read_capacity(0),
        SqliteCatalogConfig::new(&data_directory).with_control_read_capacity(4_097),
        SqliteCatalogConfig::new(&data_directory).with_snapshot_read_capacity(0),
        SqliteCatalogConfig::new(&data_directory).with_snapshot_read_capacity(4_097),
    ] {
        let error = SqliteCatalogFoundation::open(config)
            .err()
            .expect("invalid read lane capacity must be rejected");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }
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
        "UPDATE catalog_meta SET value = X'0000000000000004' \
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
         UPDATE catalog_migrations SET schema_version = -1 WHERE schema_version = 1",
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
         ) VALUES (4, 'unexpected', zeroblob(32), zeroblob(8))",
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
fn inspect_rejects_global_table_cursor_index_drift() {
    assert_live_tamper_rejected("DROP INDEX tables_by_state_id");
}

#[test]
fn inspect_rejects_namespace_table_cursor_index_drift() {
    assert_live_tamper_rejected("DROP INDEX tables_by_namespace_state_id");
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
