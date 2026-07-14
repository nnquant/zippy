use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::encoding::{decode_u64, encode_u64};
use crate::{CATALOG_APPLICATION_ID, CATALOG_FORMAT, CATALOG_SCHEMA_VERSION};

const MIGRATION_V1_VERSION: u32 = 1;
const MIGRATION_V1_NAME: &str = "catalog_migration_foundation";
const MIGRATION_V2_VERSION: u32 = 2;
const MIGRATION_V2_NAME: &str = "catalog_domain_schema";
const MIGRATION_V3_VERSION: u32 = 3;
const MIGRATION_V3_NAME: &str = "active_tables_cursor_indexes";
const MIGRATION_V4_VERSION: u32 = 4;
const MIGRATION_V4_NAME: &str = "safe_gc_claim_indexes";
const MIGRATION_V5_VERSION: u32 = 5;
const MIGRATION_V5_NAME: &str = "legacy_import_requests";

// Physical SQLite schema versions are independent of CatalogCommand wire
// versions. Physical v1 intentionally creates no domain tables: those must
// wait for a golden typed encoding that preserves the full zippy-types domain.
const CATALOG_MIGRATIONS_SQL: &str = r#"CREATE TABLE catalog_migrations (
    schema_version INTEGER PRIMARY KEY CHECK(schema_version > 0),
    migration_name TEXT NOT NULL UNIQUE CHECK(length(migration_name) > 0),
    migration_checksum BLOB NOT NULL CHECK(
        typeof(migration_checksum) = 'blob' AND length(migration_checksum) = 32
    ),
    applied_at_ns BLOB NOT NULL CHECK(
        typeof(applied_at_ns) = 'blob' AND length(applied_at_ns) = 8
    )
 )"#;

const CATALOG_META_SQL: &str = r#"CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY CHECK(length(key) > 0),
    value BLOB NOT NULL CHECK(typeof(value) = 'blob')
 )"#;

const MIGRATION_V1: &str = r#"
CREATE TABLE catalog_migrations (
    schema_version INTEGER PRIMARY KEY CHECK(schema_version > 0),
    migration_name TEXT NOT NULL UNIQUE CHECK(length(migration_name) > 0),
    migration_checksum BLOB NOT NULL CHECK(
        typeof(migration_checksum) = 'blob' AND length(migration_checksum) = 32
    ),
    applied_at_ns BLOB NOT NULL CHECK(
        typeof(applied_at_ns) = 'blob' AND length(applied_at_ns) = 8
    )
);

CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY CHECK(length(key) > 0),
    value BLOB NOT NULL CHECK(typeof(value) = 'blob')
);
"#;

macro_rules! define_domain_schema {
    ($($name:ident = $sql:literal;)+) => {
        $(const $name: &str = $sql;)+
        const MIGRATION_V2: &str = concat!($($sql, ";\n",)+);
    };
}

define_domain_schema! {
    NAMESPACES_SQL = r#"CREATE TABLE namespaces (
    namespace_id BLOB PRIMARY KEY CHECK(
        typeof(namespace_id) = 'blob'
        AND length(namespace_id) = 16
        AND namespace_id != zeroblob(16)
    ),
    name TEXT NOT NULL UNIQUE CHECK(
        typeof(name) = 'text' AND length(name) > 0 AND length(trim(name)) > 0
    ),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    )
 )"#;

    TABLES_SQL = r#"CREATE TABLE tables (
    table_id BLOB PRIMARY KEY CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    namespace_id BLOB NOT NULL CHECK(
        typeof(namespace_id) = 'blob'
        AND length(namespace_id) = 16
        AND namespace_id != zeroblob(16)
    ),
    name TEXT NOT NULL CHECK(
        typeof(name) = 'text' AND length(name) > 0 AND length(trim(name)) > 0
    ),
    table_kind TEXT NOT NULL CHECK(
        table_kind IN ('append_table', 'frame_table', 'key_value_table')
    ),
    definition_blob BLOB NOT NULL CHECK(typeof(definition_blob) = 'blob'),
    current_schema_version BLOB NOT NULL CHECK(
        typeof(current_schema_version) = 'blob' AND length(current_schema_version) = 8
    ),
    current_snapshot_version BLOB NOT NULL CHECK(
        typeof(current_snapshot_version) = 'blob' AND length(current_snapshot_version) = 8
    ),
    writer_epoch BLOB NOT NULL CHECK(
        typeof(writer_epoch) = 'blob' AND length(writer_epoch) = 8
    ),
    state TEXT NOT NULL CHECK(state IN ('active', 'dropping', 'dropped', 'failed')),
    config_blob BLOB NOT NULL CHECK(typeof(config_blob) = 'blob'),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    UNIQUE(namespace_id, name),
    FOREIGN KEY(namespace_id) REFERENCES namespaces(namespace_id),
    FOREIGN KEY(table_id, current_schema_version)
        REFERENCES table_schemas(table_id, schema_version)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(table_id, current_snapshot_version)
        REFERENCES table_snapshots(table_id, snapshot_version)
        DEFERRABLE INITIALLY DEFERRED
 )"#;

    TABLE_SCHEMAS_SQL = r#"CREATE TABLE table_schemas (
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    schema_version BLOB NOT NULL CHECK(
        typeof(schema_version) = 'blob' AND length(schema_version) = 8
    ),
    schema_ipc BLOB NOT NULL CHECK(
        typeof(schema_ipc) = 'blob' AND length(schema_ipc) > 0
    ),
    schema_hash TEXT NOT NULL CHECK(
        typeof(schema_hash) = 'text'
        AND length(schema_hash) > 0
        AND schema_hash = lower(schema_hash)
    ),
    compatibility TEXT NOT NULL CHECK(
        compatibility IN ('initial', 'full', 'backward', 'forward', 'breaking')
    ),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    PRIMARY KEY(table_id, schema_version),
    UNIQUE(table_id, schema_hash),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
 )"#;

    TABLE_SNAPSHOTS_SQL = r#"CREATE TABLE table_snapshots (
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    snapshot_version BLOB NOT NULL CHECK(
        typeof(snapshot_version) = 'blob' AND length(snapshot_version) = 8
    ),
    parent_snapshot_version BLOB NOT NULL CHECK(
        typeof(parent_snapshot_version) = 'blob'
        AND length(parent_snapshot_version) = 9
        AND (
            (
                substr(parent_snapshot_version, 1, 1) = X'00'
                AND substr(parent_snapshot_version, 2, 8) = zeroblob(8)
            )
            OR substr(parent_snapshot_version, 1, 1) = X'01'
        )
    ),
    parent_snapshot_version_value BLOB GENERATED ALWAYS AS (
        CASE
            WHEN substr(parent_snapshot_version, 1, 1) = X'01'
            THEN substr(parent_snapshot_version, 2, 8)
        END
    ) STORED,
    operation TEXT NOT NULL CHECK(
        operation IN ('create', 'persist', 'compaction', 'import', 'retention')
    ),
    commit_lsn BLOB NOT NULL CHECK(
        typeof(commit_lsn) = 'blob' AND length(commit_lsn) = 8
    ),
    committed_log_offset BLOB NOT NULL CHECK(
        typeof(committed_log_offset) = 'blob'
        AND length(committed_log_offset) = 9
        AND (
            substr(committed_log_offset, 1, 1) = X'01'
            OR (
                substr(committed_log_offset, 1, 1) IN (X'00', X'02')
                AND substr(committed_log_offset, 2, 8) = zeroblob(8)
            )
        )
    ),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    summary_blob BLOB NOT NULL CHECK(typeof(summary_blob) = 'blob'),
    PRIMARY KEY(table_id, snapshot_version),
    FOREIGN KEY(table_id) REFERENCES tables(table_id),
    FOREIGN KEY(table_id, parent_snapshot_version_value)
        REFERENCES table_snapshots(table_id, snapshot_version)
        DEFERRABLE INITIALLY DEFERRED
 )"#;

    DATA_FILES_SQL = r#"CREATE TABLE data_files (
    file_id BLOB PRIMARY KEY CHECK(
        typeof(file_id) = 'blob'
        AND length(file_id) = 16
        AND file_id != zeroblob(16)
    ),
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    schema_version BLOB NOT NULL CHECK(
        typeof(schema_version) = 'blob' AND length(schema_version) = 8
    ),
    location_kind TEXT NOT NULL CHECK(location_kind IN ('managed', 'external_legacy')),
    file_path TEXT NOT NULL UNIQUE CHECK(
        typeof(file_path) = 'text' AND length(file_path) > 0
    ),
    file_size_bytes BLOB NOT NULL CHECK(
        typeof(file_size_bytes) = 'blob' AND length(file_size_bytes) = 8
    ),
    row_count BLOB NOT NULL CHECK(
        typeof(row_count) = 'blob'
        AND length(row_count) = 8
        AND row_count != zeroblob(8)
    ),
    provenance_kind TEXT NOT NULL CHECK(provenance_kind IN ('native', 'legacy')),
    min_log_offset BLOB NOT NULL CHECK(
        typeof(min_log_offset) = 'blob'
        AND length(min_log_offset) = 9
        AND (
            (
                substr(min_log_offset, 1, 1) = X'00'
                AND substr(min_log_offset, 2, 8) = zeroblob(8)
            )
            OR substr(min_log_offset, 1, 1) = X'01'
        )
    ),
    max_log_offset BLOB NOT NULL CHECK(
        typeof(max_log_offset) = 'blob'
        AND length(max_log_offset) = 9
        AND (
            (
                substr(max_log_offset, 1, 1) = X'00'
                AND substr(max_log_offset, 2, 8) = zeroblob(8)
            )
            OR substr(max_log_offset, 1, 1) = X'01'
        )
    ),
    legacy_preserve_discovery_order INTEGER NOT NULL CHECK(
        typeof(legacy_preserve_discovery_order) = 'integer'
        AND legacy_preserve_discovery_order IN (0, 1)
    ),
    legacy_discovery_order BLOB NOT NULL CHECK(
        typeof(legacy_discovery_order) = 'blob'
        AND length(legacy_discovery_order) = 9
        AND (
            (
                substr(legacy_discovery_order, 1, 1) = X'00'
                AND substr(legacy_discovery_order, 2, 8) = zeroblob(8)
            )
            OR substr(legacy_discovery_order, 1, 1) = X'01'
        )
    ),
    partition_blob BLOB NOT NULL CHECK(typeof(partition_blob) = 'blob'),
    statistics_blob BLOB NOT NULL CHECK(typeof(statistics_blob) = 'blob'),
    metadata_blob BLOB NOT NULL CHECK(typeof(metadata_blob) = 'blob'),
    checksum TEXT NOT NULL CHECK(
        typeof(checksum) = 'text' AND length(checksum) > 0 AND checksum = lower(checksum)
    ),
    added_snapshot_version BLOB NOT NULL CHECK(
        typeof(added_snapshot_version) = 'blob' AND length(added_snapshot_version) = 8
    ),
    removed_snapshot_version BLOB NOT NULL CHECK(
        typeof(removed_snapshot_version) = 'blob'
        AND length(removed_snapshot_version) = 9
        AND (
            (
                substr(removed_snapshot_version, 1, 1) = X'00'
                AND substr(removed_snapshot_version, 2, 8) = zeroblob(8)
            )
            OR substr(removed_snapshot_version, 1, 1) = X'01'
        )
    ),
    removed_snapshot_version_value BLOB GENERATED ALWAYS AS (
        CASE
            WHEN substr(removed_snapshot_version, 1, 1) = X'01'
            THEN substr(removed_snapshot_version, 2, 8)
        END
    ) STORED,
    state TEXT NOT NULL CHECK(state IN ('committed', 'deleting', 'deleted')),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    CHECK(substr(min_log_offset, 1, 1) = substr(max_log_offset, 1, 1)),
    CHECK(
        substr(min_log_offset, 1, 1) = X'00'
        OR substr(min_log_offset, 2, 8) <= substr(max_log_offset, 2, 8)
    ),
    CHECK(
        (provenance_kind = 'native'
            AND substr(min_log_offset, 1, 1) = X'01'
            AND legacy_preserve_discovery_order = 0
            AND substr(legacy_discovery_order, 1, 1) = X'00')
        OR
        (provenance_kind = 'legacy'
            AND (
                (legacy_preserve_discovery_order = 0
                    AND substr(legacy_discovery_order, 1, 1) = X'00')
                OR
                (legacy_preserve_discovery_order = 1
                    AND substr(legacy_discovery_order, 1, 1) = X'01')
            ))
    ),
    CHECK(
        substr(removed_snapshot_version, 1, 1) = X'00'
        OR substr(removed_snapshot_version, 2, 8) > added_snapshot_version
    ),
    CHECK(
        state = 'committed'
        OR substr(removed_snapshot_version, 1, 1) = X'01'
    ),
    FOREIGN KEY(table_id, schema_version)
        REFERENCES table_schemas(table_id, schema_version),
    FOREIGN KEY(table_id, added_snapshot_version)
        REFERENCES table_snapshots(table_id, snapshot_version),
    FOREIGN KEY(table_id, removed_snapshot_version_value)
        REFERENCES table_snapshots(table_id, snapshot_version)
        DEFERRABLE INITIALLY DEFERRED
 )"#;

    DATA_FILE_SOURCES_SQL = r#"CREATE TABLE data_file_sources (
    file_id BLOB NOT NULL CHECK(
        typeof(file_id) = 'blob'
        AND length(file_id) = 16
        AND file_id != zeroblob(16)
    ),
    source_segment_id BLOB NOT NULL CHECK(
        typeof(source_segment_id) = 'blob' AND length(source_segment_id) = 8
    ),
    source_generation BLOB NOT NULL CHECK(
        typeof(source_generation) = 'blob' AND length(source_generation) = 8
    ),
    writer_epoch BLOB NOT NULL CHECK(
        typeof(writer_epoch) = 'blob' AND length(writer_epoch) = 8
    ),
    min_log_offset BLOB NOT NULL CHECK(
        typeof(min_log_offset) = 'blob' AND length(min_log_offset) = 8
    ),
    max_log_offset BLOB NOT NULL CHECK(
        typeof(max_log_offset) = 'blob' AND length(max_log_offset) = 8
    ),
    CHECK(min_log_offset <= max_log_offset),
    PRIMARY KEY(file_id, source_segment_id, source_generation, writer_epoch),
    FOREIGN KEY(file_id) REFERENCES data_files(file_id)
 )"#;

    PRODUCERS_SQL = r#"CREATE TABLE producers (
    producer_id BLOB PRIMARY KEY CHECK(
        typeof(producer_id) = 'blob'
        AND length(producer_id) = 16
        AND producer_id != zeroblob(16)
    ),
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    producer_epoch BLOB NOT NULL CHECK(
        typeof(producer_epoch) = 'blob' AND length(producer_epoch) = 8
    ),
    last_batch_sequence BLOB NOT NULL CHECK(
        typeof(last_batch_sequence) = 'blob'
        AND length(last_batch_sequence) = 9
        AND (
            (
                substr(last_batch_sequence, 1, 1) = X'00'
                AND substr(last_batch_sequence, 2, 8) = zeroblob(8)
            )
            OR substr(last_batch_sequence, 1, 1) = X'01'
        )
    ),
    last_committed_log_offset BLOB NOT NULL CHECK(
        typeof(last_committed_log_offset) = 'blob'
        AND length(last_committed_log_offset) = 9
        AND (
            substr(last_committed_log_offset, 1, 1) = X'01'
            OR (
                substr(last_committed_log_offset, 1, 1) IN (X'00', X'02')
                AND substr(last_committed_log_offset, 2, 8) = zeroblob(8)
            )
        )
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
 )"#;

    DURABLE_CONSUMERS_SQL = r#"CREATE TABLE durable_consumers (
    consumer_id BLOB PRIMARY KEY CHECK(
        typeof(consumer_id) = 'blob'
        AND length(consumer_id) = 16
        AND consumer_id != zeroblob(16)
    ),
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    UNIQUE(consumer_id, table_id),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
 )"#;

    DURABLE_CONSUMER_OFFSETS_SQL = r#"CREATE TABLE durable_consumer_offsets (
    consumer_id BLOB NOT NULL CHECK(
        typeof(consumer_id) = 'blob'
        AND length(consumer_id) = 16
        AND consumer_id != zeroblob(16)
    ),
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    authority_kind TEXT NOT NULL CHECK(authority_kind IN ('consumer', 'group')),
    authority_id BLOB NOT NULL CHECK(
        typeof(authority_id) = 'blob'
        AND length(authority_id) = 16
        AND authority_id != zeroblob(16)
    ),
    committed_log_offset BLOB NOT NULL CHECK(
        typeof(committed_log_offset) = 'blob'
        AND length(committed_log_offset) = 9
        AND (
            (
                substr(committed_log_offset, 1, 1) = X'00'
                AND substr(committed_log_offset, 2, 8) = zeroblob(8)
            )
            OR substr(committed_log_offset, 1, 1) = X'01'
        )
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    CHECK(authority_kind = 'group' OR authority_id = consumer_id),
    PRIMARY KEY(table_id, authority_kind, authority_id),
    FOREIGN KEY(consumer_id, table_id)
        REFERENCES durable_consumers(consumer_id, table_id)
 )"#;

    SNAPSHOT_LEASES_SQL = r#"CREATE TABLE snapshot_leases (
    lease_id BLOB PRIMARY KEY CHECK(
        typeof(lease_id) = 'blob'
        AND length(lease_id) = 16
        AND lease_id != zeroblob(16)
    ),
    table_id BLOB NOT NULL CHECK(
        typeof(table_id) = 'blob'
        AND length(table_id) = 16
        AND table_id != zeroblob(16)
    ),
    snapshot_version BLOB NOT NULL CHECK(
        typeof(snapshot_version) = 'blob' AND length(snapshot_version) = 8
    ),
    owner TEXT NOT NULL CHECK(
        typeof(owner) = 'text' AND length(owner) > 0
    ),
    expires_at_ns BLOB NOT NULL CHECK(
        typeof(expires_at_ns) = 'blob' AND length(expires_at_ns) = 8
    ),
    FOREIGN KEY(table_id, snapshot_version)
        REFERENCES table_snapshots(table_id, snapshot_version)
 )"#;

    MAINTENANCE_JOBS_SQL = r#"CREATE TABLE maintenance_jobs (
    job_id BLOB PRIMARY KEY CHECK(
        typeof(job_id) = 'blob'
        AND length(job_id) = 16
        AND job_id != zeroblob(16)
    ),
    job_kind TEXT NOT NULL CHECK(
        job_kind IN (
            'compaction', 'retention', 'garbage_collection',
            'verify', 'backup', 'import'
        )
    ),
    table_id BLOB CHECK(
        table_id IS NULL
        OR (
            typeof(table_id) = 'blob'
            AND length(table_id) = 16
            AND table_id != zeroblob(16)
        )
    ),
    state TEXT NOT NULL CHECK(
        state IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')
    ),
    priority INTEGER NOT NULL CHECK(
        typeof(priority) = 'integer'
        AND priority >= -2147483648
        AND priority <= 2147483647
    ),
    attempt INTEGER NOT NULL CHECK(
        typeof(attempt) = 'integer' AND attempt >= 0 AND attempt <= 4294967295
    ),
    lease_generation BLOB NOT NULL CHECK(
        typeof(lease_generation) = 'blob' AND length(lease_generation) = 8
    ),
    lease_owner TEXT CHECK(
        lease_owner IS NULL OR (typeof(lease_owner) = 'text' AND length(lease_owner) > 0)
    ),
    lease_expires_at_ns BLOB NOT NULL CHECK(
        typeof(lease_expires_at_ns) = 'blob'
        AND length(lease_expires_at_ns) = 9
        AND (
            (
                substr(lease_expires_at_ns, 1, 1) = X'00'
                AND substr(lease_expires_at_ns, 2, 8) = zeroblob(8)
            )
            OR substr(lease_expires_at_ns, 1, 1) = X'01'
        )
    ),
    input_blob BLOB NOT NULL CHECK(typeof(input_blob) = 'blob'),
    progress_blob BLOB NOT NULL CHECK(typeof(progress_blob) = 'blob'),
    error_text TEXT CHECK(error_text IS NULL OR typeof(error_text) = 'text'),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
 )"#;

    IDEMPOTENT_REQUESTS_SQL = r#"CREATE TABLE idempotent_requests (
    request_id BLOB PRIMARY KEY CHECK(
        typeof(request_id) = 'blob'
        AND length(request_id) = 16
        AND request_id != zeroblob(16)
    ),
    command_format_version INTEGER NOT NULL CHECK(
        typeof(command_format_version) = 'integer' AND command_format_version > 0
    ),
    operation TEXT NOT NULL CHECK(
        operation IN (
            'create_namespace', 'create_table', 'alter_table_schema',
            'acquire_producer', 'commit_data_files', 'commit_compaction',
            'commit_consumer_offset', 'acquire_snapshot_lease',
            'release_snapshot_lease', 'create_maintenance_job',
            'transition_maintenance_job', 'drop_table'
        )
    ),
    command_fingerprint BLOB NOT NULL CHECK(
        typeof(command_fingerprint) = 'blob' AND length(command_fingerprint) = 32
    ),
    response_blob BLOB NOT NULL CHECK(
        typeof(response_blob) = 'blob' AND length(response_blob) > 0
    ),
    committed_at_ns BLOB NOT NULL CHECK(
        typeof(committed_at_ns) = 'blob' AND length(committed_at_ns) = 8
    ),
    expires_at_ns BLOB NOT NULL CHECK(
        typeof(expires_at_ns) = 'blob' AND length(expires_at_ns) = 8
    )
 )"#;

    TABLES_BY_NAMESPACE_STATE_NAME_SQL = r#"CREATE INDEX tables_by_namespace_state_name
 ON tables(namespace_id, state, name, table_id)"#;

    DATA_FILES_VISIBLE_BY_TABLE_SQL = r#"CREATE INDEX data_files_visible_by_table
 ON data_files(table_id, state, added_snapshot_version, removed_snapshot_version)"#;

    DATA_FILES_OFFSETS_SQL = r#"CREATE INDEX data_files_offsets
 ON data_files(table_id, min_log_offset, max_log_offset)"#;

    DATA_FILES_BY_SCHEMA_SQL = r#"CREATE INDEX data_files_by_schema
 ON data_files(table_id, schema_version, file_id)"#;

    DATA_FILES_ACTIVE_MANIFEST_SQL = r#"CREATE INDEX data_files_active_manifest
 ON data_files(table_id, file_id)
 WHERE state = 'committed'
   AND removed_snapshot_version = X'000000000000000000'"#;

    DATA_FILES_BY_GC_STATE_SQL = r#"CREATE INDEX data_files_by_gc_state
 ON data_files(state, removed_snapshot_version, table_id, file_id)"#;

    PRODUCERS_BY_TABLE_SQL = r#"CREATE INDEX producers_by_table
 ON producers(table_id, producer_id)"#;

    DURABLE_CONSUMERS_BY_TABLE_SQL = r#"CREATE INDEX durable_consumers_by_table
 ON durable_consumers(table_id, consumer_id)"#;

    DURABLE_CONSUMER_OFFSETS_BY_CONSUMER_SQL = r#"CREATE INDEX durable_consumer_offsets_by_consumer
 ON durable_consumer_offsets(consumer_id, table_id, authority_kind, authority_id)"#;

    SNAPSHOT_LEASES_BY_EXPIRY_SQL = r#"CREATE INDEX snapshot_leases_by_expiry
 ON snapshot_leases(expires_at_ns, table_id, snapshot_version, lease_id)"#;

    MAINTENANCE_JOBS_BY_ADMISSION_SQL = r#"CREATE INDEX maintenance_jobs_by_admission
 ON maintenance_jobs(state, job_kind, priority DESC, created_at_ns, job_id)"#;

    IDEMPOTENT_REQUESTS_BY_EXPIRY_SQL = r#"CREATE INDEX idempotent_requests_by_expiry
 ON idempotent_requests(expires_at_ns, request_id)"#;
}

macro_rules! define_v5_schema_addition {
    ($name:ident = $sql:literal;) => {
        const $name: &str = $sql;
        const MIGRATION_V5: &str = concat!($sql, ";\n");
    };
}

define_v5_schema_addition! {
    LEGACY_IMPORT_REQUESTS_SQL = r#"CREATE TABLE legacy_import_requests (
    request_id BLOB PRIMARY KEY CHECK(
        typeof(request_id) = 'blob'
        AND length(request_id) = 16
        AND request_id != zeroblob(16)
    ),
    format_version INTEGER NOT NULL CHECK(
        typeof(format_version) = 'integer' AND format_version > 0
    ),
    fingerprint BLOB NOT NULL CHECK(
        typeof(fingerprint) = 'blob' AND length(fingerprint) = 32
    ),
    response_blob BLOB NOT NULL CHECK(
        typeof(response_blob) = 'blob' AND length(response_blob) > 0
    ),
    committed_at_ns BLOB NOT NULL CHECK(
        typeof(committed_at_ns) = 'blob' AND length(committed_at_ns) = 8
    )
 )"#;
}

macro_rules! define_v3_schema_additions {
    ($($name:ident = $sql:literal;)+) => {
        $(const $name: &str = $sql;)+
        const MIGRATION_V3: &str = concat!($($sql, ";\n",)+);
    };
}

define_v3_schema_additions! {
    TABLES_BY_STATE_ID_SQL = r#"CREATE INDEX tables_by_state_id
 ON tables(state, table_id)"#;

    TABLES_BY_NAMESPACE_STATE_ID_SQL = r#"CREATE INDEX tables_by_namespace_state_id
 ON tables(namespace_id, state, table_id)"#;
}

macro_rules! define_v4_schema_additions {
    ($($name:ident = $sql:literal;)+) => {
        $(const $name: &str = $sql;)+
        const MIGRATION_V4: &str = concat!($($sql, ";\n",)+);
    };
}

define_v4_schema_additions! {
    DATA_FILES_GC_RETRY_BY_TABLE_SQL = r#"CREATE INDEX data_files_gc_retry_by_table
 ON data_files(table_id, file_id)
 WHERE state = 'deleting'
   AND location_kind = 'managed'
   AND removed_snapshot_version_value IS NOT NULL"#;

    DATA_FILES_GC_CLAIM_BY_TABLE_SQL = r#"CREATE INDEX data_files_gc_claim_by_table
 ON data_files(table_id, removed_snapshot_version_value, file_id, added_snapshot_version)
 WHERE state = 'committed'
   AND location_kind = 'managed'
   AND removed_snapshot_version_value IS NOT NULL"#;

    SNAPSHOT_LEASES_GC_BY_TABLE_SQL = r#"CREATE INDEX snapshot_leases_gc_by_table
 ON snapshot_leases(table_id, snapshot_version, expires_at_ns)"#;
}

struct ExpectedSchemaObject {
    object_type: &'static str,
    name: &'static str,
    table_name: &'static str,
    sql: Option<&'static str>,
}

const MIGRATION_V1_SCHEMA: [ExpectedSchemaObject; 4] = [
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_meta_1",
        table_name: "catalog_meta",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_migrations_1",
        table_name: "catalog_migrations",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_meta",
        table_name: "catalog_meta",
        sql: Some(CATALOG_META_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_migrations",
        table_name: "catalog_migrations",
        sql: Some(CATALOG_MIGRATIONS_SQL),
    },
];

const MIGRATION_V2_SCHEMA: &[ExpectedSchemaObject] = &[
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_meta_1",
        table_name: "catalog_meta",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_catalog_migrations_1",
        table_name: "catalog_migrations",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_namespaces_1",
        table_name: "namespaces",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_namespaces_2",
        table_name: "namespaces",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_tables_1",
        table_name: "tables",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_tables_2",
        table_name: "tables",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_table_schemas_1",
        table_name: "table_schemas",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_table_schemas_2",
        table_name: "table_schemas",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_table_snapshots_1",
        table_name: "table_snapshots",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_data_files_1",
        table_name: "data_files",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_data_files_2",
        table_name: "data_files",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_data_file_sources_1",
        table_name: "data_file_sources",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_producers_1",
        table_name: "producers",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_durable_consumers_1",
        table_name: "durable_consumers",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_durable_consumers_2",
        table_name: "durable_consumers",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_durable_consumer_offsets_1",
        table_name: "durable_consumer_offsets",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_snapshot_leases_1",
        table_name: "snapshot_leases",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_maintenance_jobs_1",
        table_name: "maintenance_jobs",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_idempotent_requests_1",
        table_name: "idempotent_requests",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "tables_by_namespace_state_name",
        table_name: "tables",
        sql: Some(TABLES_BY_NAMESPACE_STATE_NAME_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_visible_by_table",
        table_name: "data_files",
        sql: Some(DATA_FILES_VISIBLE_BY_TABLE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_offsets",
        table_name: "data_files",
        sql: Some(DATA_FILES_OFFSETS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_by_schema",
        table_name: "data_files",
        sql: Some(DATA_FILES_BY_SCHEMA_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_active_manifest",
        table_name: "data_files",
        sql: Some(DATA_FILES_ACTIVE_MANIFEST_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_by_gc_state",
        table_name: "data_files",
        sql: Some(DATA_FILES_BY_GC_STATE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "producers_by_table",
        table_name: "producers",
        sql: Some(PRODUCERS_BY_TABLE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "durable_consumers_by_table",
        table_name: "durable_consumers",
        sql: Some(DURABLE_CONSUMERS_BY_TABLE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "durable_consumer_offsets_by_consumer",
        table_name: "durable_consumer_offsets",
        sql: Some(DURABLE_CONSUMER_OFFSETS_BY_CONSUMER_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "snapshot_leases_by_expiry",
        table_name: "snapshot_leases",
        sql: Some(SNAPSHOT_LEASES_BY_EXPIRY_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "maintenance_jobs_by_admission",
        table_name: "maintenance_jobs",
        sql: Some(MAINTENANCE_JOBS_BY_ADMISSION_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "idempotent_requests_by_expiry",
        table_name: "idempotent_requests",
        sql: Some(IDEMPOTENT_REQUESTS_BY_EXPIRY_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_meta",
        table_name: "catalog_meta",
        sql: Some(CATALOG_META_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "catalog_migrations",
        table_name: "catalog_migrations",
        sql: Some(CATALOG_MIGRATIONS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "namespaces",
        table_name: "namespaces",
        sql: Some(NAMESPACES_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "tables",
        table_name: "tables",
        sql: Some(TABLES_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "table_schemas",
        table_name: "table_schemas",
        sql: Some(TABLE_SCHEMAS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "table_snapshots",
        table_name: "table_snapshots",
        sql: Some(TABLE_SNAPSHOTS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "data_files",
        table_name: "data_files",
        sql: Some(DATA_FILES_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "data_file_sources",
        table_name: "data_file_sources",
        sql: Some(DATA_FILE_SOURCES_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "producers",
        table_name: "producers",
        sql: Some(PRODUCERS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "durable_consumers",
        table_name: "durable_consumers",
        sql: Some(DURABLE_CONSUMERS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "durable_consumer_offsets",
        table_name: "durable_consumer_offsets",
        sql: Some(DURABLE_CONSUMER_OFFSETS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "snapshot_leases",
        table_name: "snapshot_leases",
        sql: Some(SNAPSHOT_LEASES_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "maintenance_jobs",
        table_name: "maintenance_jobs",
        sql: Some(MAINTENANCE_JOBS_SQL),
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "idempotent_requests",
        table_name: "idempotent_requests",
        sql: Some(IDEMPOTENT_REQUESTS_SQL),
    },
];

const MIGRATION_V3_SCHEMA_ADDITIONS: [ExpectedSchemaObject; 2] = [
    ExpectedSchemaObject {
        object_type: "index",
        name: "tables_by_state_id",
        table_name: "tables",
        sql: Some(TABLES_BY_STATE_ID_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "tables_by_namespace_state_id",
        table_name: "tables",
        sql: Some(TABLES_BY_NAMESPACE_STATE_ID_SQL),
    },
];

const MIGRATION_V4_SCHEMA_ADDITIONS: [ExpectedSchemaObject; 3] = [
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_gc_retry_by_table",
        table_name: "data_files",
        sql: Some(DATA_FILES_GC_RETRY_BY_TABLE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "data_files_gc_claim_by_table",
        table_name: "data_files",
        sql: Some(DATA_FILES_GC_CLAIM_BY_TABLE_SQL),
    },
    ExpectedSchemaObject {
        object_type: "index",
        name: "snapshot_leases_gc_by_table",
        table_name: "snapshot_leases",
        sql: Some(SNAPSHOT_LEASES_GC_BY_TABLE_SQL),
    },
];

const MIGRATION_V5_SCHEMA_ADDITIONS: [ExpectedSchemaObject; 2] = [
    ExpectedSchemaObject {
        object_type: "index",
        name: "sqlite_autoindex_legacy_import_requests_1",
        table_name: "legacy_import_requests",
        sql: None,
    },
    ExpectedSchemaObject {
        object_type: "table",
        name: "legacy_import_requests",
        table_name: "legacy_import_requests",
        sql: Some(LEGACY_IMPORT_REQUESTS_SQL),
    },
];

struct Migration {
    version: u32,
    name: &'static str,
    sql: &'static str,
    base_schema_version: Option<u32>,
    schema_additions: &'static [ExpectedSchemaObject],
}

const MIGRATIONS: [Migration; 5] = [
    Migration {
        version: MIGRATION_V1_VERSION,
        name: MIGRATION_V1_NAME,
        sql: MIGRATION_V1,
        base_schema_version: None,
        schema_additions: &MIGRATION_V1_SCHEMA,
    },
    Migration {
        version: MIGRATION_V2_VERSION,
        name: MIGRATION_V2_NAME,
        sql: MIGRATION_V2,
        base_schema_version: None,
        schema_additions: MIGRATION_V2_SCHEMA,
    },
    Migration {
        version: MIGRATION_V3_VERSION,
        name: MIGRATION_V3_NAME,
        sql: MIGRATION_V3,
        base_schema_version: Some(MIGRATION_V2_VERSION),
        schema_additions: &MIGRATION_V3_SCHEMA_ADDITIONS,
    },
    Migration {
        version: MIGRATION_V4_VERSION,
        name: MIGRATION_V4_NAME,
        sql: MIGRATION_V4,
        base_schema_version: Some(MIGRATION_V3_VERSION),
        schema_additions: &MIGRATION_V4_SCHEMA_ADDITIONS,
    },
    Migration {
        version: MIGRATION_V5_VERSION,
        name: MIGRATION_V5_NAME,
        sql: MIGRATION_V5,
        base_schema_version: Some(MIGRATION_V4_VERSION),
        schema_additions: &MIGRATION_V5_SCHEMA_ADDITIONS,
    },
];

pub(crate) fn migrate(connection: &mut Connection) -> Result<()> {
    validate_migration_registry()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| crate::map_sqlite_error("begin catalog migration", error))?;
    let mut actual = validate_catalog_state(&transaction)?;
    while actual < CATALOG_SCHEMA_VERSION {
        let next = actual.checked_add(1).ok_or_else(|| {
            ServerError::new(ErrorCode::Internal, "catalog migration version overflow")
        })?;
        let migration = migration_by_version(next)?;
        apply_migration(&transaction, migration)?;
        actual = next;
    }
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog migration", error))
}

fn validate_migration_registry() -> Result<()> {
    for (index, migration) in MIGRATIONS.iter().enumerate() {
        let expected = u32::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Internal,
                    "catalog migration registry is too large",
                )
            })?;
        if migration.version != expected {
            return Err(ServerError::new(
                ErrorCode::Internal,
                format!(
                    "catalog migration registry is not contiguous expected=[{}] actual=[{}]",
                    expected, migration.version
                ),
            ));
        }
        if let Some(base_schema_version) = migration.base_schema_version {
            if base_schema_version >= migration.version
                || !MIGRATIONS
                    .iter()
                    .any(|candidate| candidate.version == base_schema_version)
            {
                return Err(ServerError::new(
                    ErrorCode::Internal,
                    format!(
                        concat!(
                            "catalog migration has invalid base schema version ",
                            "version=[{}] base=[{}]"
                        ),
                        migration.version, base_schema_version
                    ),
                ));
            }
        }
    }
    if MIGRATIONS.last().map(|migration| migration.version) != Some(CATALOG_SCHEMA_VERSION) {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "catalog migration registry does not end at the supported schema version",
        ));
    }
    Ok(())
}

fn migration_by_version(version: u32) -> Result<&'static Migration> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.version == version)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                format!("catalog migration is missing version=[{version}]"),
            )
        })
}

fn reject_future_version(version: u32) -> Result<()> {
    if version > CATALOG_SCHEMA_VERSION {
        Err(ServerError::new(
            ErrorCode::UnsupportedPlan,
            format!(
                "catalog schema is newer than this server supported=[{}] actual=[{}]",
                CATALOG_SCHEMA_VERSION, version
            ),
        ))
    } else {
        Ok(())
    }
}

fn application_id(connection: &Connection) -> Result<u32> {
    let value: i64 = connection
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|error| crate::map_sqlite_error("read catalog application id", error))?;
    u32::try_from(value).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog application id is outside the supported domain actual=[{value}]"),
        )
    })
}

pub(crate) fn preflight(connection: &Connection) -> Result<()> {
    let transaction = Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog preflight", error))?;
    let _ = validate_catalog_state(&transaction)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog preflight", error))
}

fn validate_catalog_state(connection: &Connection) -> Result<u32> {
    let application_id = application_id(connection)?;
    let version = schema_version(connection)?;
    let schema_objects: u64 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("inspect catalog schema objects", error))?;

    if application_id == 0 && version == 0 && schema_objects == 0 {
        return Ok(version);
    }
    if application_id != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("database is not a Zippy catalog application_id=[{application_id}]"),
        ));
    }
    reject_future_version(version)?;
    if version == 0 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "Zippy catalog application marker has no physical schema version",
        ));
    }
    let _ = validate_metadata_at_version(connection, version)?;
    Ok(version)
}

pub(crate) fn schema_version(connection: &Connection) -> Result<u32> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| crate::map_sqlite_error("read catalog schema version", error))?;
    u32::try_from(version).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog schema version is outside the supported domain actual=[{version}]"),
        )
    })
}

fn apply_migration(connection: &Connection, migration: &Migration) -> Result<()> {
    connection
        .execute_batch(migration.sql)
        .map_err(|error| crate::map_sqlite_error("apply catalog migration", error))?;
    if migration.version == MIGRATION_V1_VERSION {
        connection
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params!["catalog_format", CATALOG_FORMAT.as_bytes()],
            )
            .map_err(|error| crate::map_sqlite_error("initialize catalog format", error))?;
        connection
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params![
                    "schema_version",
                    encode_u64(u64::from(migration.version)).as_slice()
                ],
            )
            .map_err(|error| {
                crate::map_sqlite_error("initialize catalog schema metadata", error)
            })?;
        connection
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES (?1, ?2)",
                params!["catalog_revision", encode_u64(0).as_slice()],
            )
            .map_err(|error| crate::map_sqlite_error("initialize catalog revision", error))?;
    } else {
        let updated = connection
            .execute(
                "UPDATE catalog_meta SET value = ?1 WHERE key = 'schema_version'",
                [encode_u64(u64::from(migration.version)).as_slice()],
            )
            .map_err(|error| crate::map_sqlite_error("advance catalog schema metadata", error))?;
        if updated != 1 {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "catalog schema metadata update affected unexpected rows actual=[{updated}]"
                ),
            ));
        }
    }
    connection
        .execute(
            "INSERT INTO catalog_migrations(
                schema_version, migration_name, migration_checksum, applied_at_ns
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                migration.version,
                migration.name,
                migration_checksum(migration).as_slice(),
                encode_u64(unix_time_ns()?).as_slice()
            ],
        )
        .map_err(|error| crate::map_sqlite_error("record catalog migration", error))?;
    connection
        .pragma_update(None, "application_id", CATALOG_APPLICATION_ID)
        .map_err(|error| crate::map_sqlite_error("record catalog application id", error))?;
    connection
        .pragma_update(None, "user_version", migration.version)
        .map_err(|error| crate::map_sqlite_error("record catalog schema version", error))?;
    let _ = validate_metadata_at_version_with_migration(connection, migration.version, migration)?;
    Ok(())
}

pub(crate) fn validate_metadata(connection: &Connection) -> Result<(String, u32, u64)> {
    let physical_version = schema_version(connection)?;
    reject_future_version(physical_version)?;
    if physical_version != CATALOG_SCHEMA_VERSION {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog physical schema version mismatch expected=[{}] actual=[{}]",
                CATALOG_SCHEMA_VERSION, physical_version
            ),
        ));
    }
    validate_metadata_at_version(connection, physical_version)
}

fn validate_metadata_at_version(
    connection: &Connection,
    expected_version: u32,
) -> Result<(String, u32, u64)> {
    let migration = migration_by_version(expected_version)?;
    validate_metadata_at_version_with_migration(connection, expected_version, migration)
}

fn validate_metadata_at_version_with_migration(
    connection: &Connection,
    expected_version: u32,
    migration: &Migration,
) -> Result<(String, u32, u64)> {
    let actual_application_id = application_id(connection)?;
    if actual_application_id != CATALOG_APPLICATION_ID {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog application id mismatch expected=[{}] actual=[{}]",
                CATALOG_APPLICATION_ID, actual_application_id
            ),
        ));
    }
    let physical_version = schema_version(connection)?;
    if physical_version != expected_version {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog physical schema version mismatch expected=[{}] actual=[{}]",
                expected_version, physical_version
            ),
        ));
    }
    validate_schema_fingerprint_for_migration(connection, migration)?;
    let format = metadata_blob(connection, "catalog_format")?;
    if format != CATALOG_FORMAT.as_bytes() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog format metadata mismatch",
        ));
    }
    let metadata_version = decode_u64(&metadata_blob(connection, "schema_version")?)?;
    if metadata_version != u64::from(expected_version) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog schema metadata mismatch expected=[{}] actual=[{}]",
                expected_version, metadata_version
            ),
        ));
    }
    let catalog_revision = catalog_revision(connection)?;
    validate_migration_audit(connection, physical_version)?;
    let format = String::from_utf8(format).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            "catalog format metadata is not UTF-8",
        )
    })?;
    Ok((format, physical_version, catalog_revision))
}

fn validate_migration_audit(connection: &Connection, physical_version: u32) -> Result<()> {
    let mut statement = connection
        .prepare(
            "SELECT schema_version, migration_name, migration_checksum, applied_at_ns
             FROM catalog_migrations ORDER BY schema_version",
        )
        .map_err(|error| crate::map_sqlite_error("prepare catalog migration audit", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, u32>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
                row.get::<_, Vec<u8>>(3)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("read catalog migration audit", error))?;
    let mut actual = Vec::new();
    for row in rows {
        actual.push(
            row.map_err(|error| crate::map_sqlite_error("decode catalog migration audit", error))?,
        );
    }
    let expected = MIGRATIONS
        .iter()
        .filter(|migration| migration.version <= physical_version)
        .collect::<Vec<_>>();
    if actual.len() != expected.len() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog migration audit length mismatch expected=[{}] actual=[{}]",
                expected.len(),
                actual.len()
            ),
        ));
    }
    for ((version, name, checksum, applied_at_ns), migration) in actual.iter().zip(expected) {
        if *version != migration.version
            || name != migration.name
            || checksum.as_slice() != migration_checksum(migration)
        {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "catalog migration audit mismatch expected_version=[{}] actual_version=[{}]",
                    migration.version, version
                ),
            ));
        }
        let _ = decode_u64(applied_at_ns)?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalSchemaObject {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

fn validate_schema_fingerprint_for_migration(
    connection: &Connection,
    migration: &Migration,
) -> Result<()> {
    let mut expected = expected_schema_for_migration(migration)?;
    expected.sort();
    if expected.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!(
                "catalog expected schema contains duplicate objects version=[{}]",
                migration.version
            ),
        ));
    }

    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_schema
             WHERE name NOT GLOB 'sqlite_*'
                OR (type = 'index' AND name GLOB 'sqlite_autoindex_*')",
        )
        .map_err(|error| crate::map_sqlite_error("prepare catalog schema inventory", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("read catalog schema inventory", error))?;
    let mut actual = Vec::new();
    for row in rows {
        let (object_type, name, table_name, sql) =
            row.map_err(|error| crate::map_sqlite_error("decode catalog schema inventory", error))?;
        actual.push(canonical_schema_object(
            &object_type,
            &name,
            &table_name,
            sql.as_deref(),
        )?);
    }
    actual.sort();

    let expected_fingerprint = schema_fingerprint(&expected);
    let actual_fingerprint = schema_fingerprint(&actual);
    if expected_fingerprint != actual_fingerprint {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                concat!(
                    "catalog schema fingerprint mismatch version=[{}] ",
                    "expected_objects=[{}] actual_objects=[{}]"
                ),
                migration.version,
                expected.len(),
                actual.len()
            ),
        ));
    }
    Ok(())
}

fn expected_schema_for_migration(migration: &Migration) -> Result<Vec<CanonicalSchemaObject>> {
    let mut expected = match migration.base_schema_version {
        Some(base_schema_version) => {
            if base_schema_version >= migration.version {
                return Err(ServerError::new(
                    ErrorCode::Internal,
                    format!(
                        concat!(
                            "catalog migration has invalid base schema version ",
                            "version=[{}] base=[{}]"
                        ),
                        migration.version, base_schema_version
                    ),
                ));
            }
            expected_schema_for_migration(migration_by_version(base_schema_version)?)?
        }
        None => Vec::new(),
    };
    expected.extend(
        migration
            .schema_additions
            .iter()
            .map(|object| {
                canonical_schema_object(
                    object.object_type,
                    object.name,
                    object.table_name,
                    object.sql,
                )
            })
            .collect::<Result<Vec<_>>>()?,
    );
    Ok(expected)
}

fn canonical_schema_object(
    object_type: &str,
    name: &str,
    table_name: &str,
    sql: Option<&str>,
) -> Result<CanonicalSchemaObject> {
    Ok(CanonicalSchemaObject {
        object_type: object_type.to_owned(),
        name: name.to_owned(),
        table_name: table_name.to_owned(),
        sql: sql.map(normalize_schema_sql).transpose()?,
    })
}

fn schema_fingerprint(objects: &[CanonicalSchemaObject]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for object in objects {
        for field in [
            object.object_type.as_str(),
            object.name.as_str(),
            object.table_name.as_str(),
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        match object.sql.as_deref() {
            Some(sql) => {
                digest.update([1]);
                digest.update((sql.len() as u64).to_be_bytes());
                digest.update(sql.as_bytes());
            }
            None => digest.update([0]),
        }
    }
    digest.finalize().into()
}

fn normalize_schema_sql(sql: &str) -> Result<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        Single,
        Double,
        Backtick,
        Bracket,
    }

    let mut normalized = String::with_capacity(sql.len());
    let mut characters = sql.chars().peekable();
    let mut quote = None;
    let mut whitespace_pending = false;
    while let Some(character) = characters.next() {
        if let Some(active) = quote {
            normalized.push(character);
            let terminator = match active {
                Quote::Single => '\'',
                Quote::Double => '"',
                Quote::Backtick => '`',
                Quote::Bracket => ']',
            };
            if character == terminator {
                let doubled = !matches!(active, Quote::Bracket)
                    && characters.peek().is_some_and(|next| *next == terminator);
                if doubled {
                    normalized.push(characters.next().expect("peeked quote must exist"));
                } else {
                    quote = None;
                }
            }
            continue;
        }

        if character.is_whitespace() {
            whitespace_pending = !normalized.is_empty();
            continue;
        }
        if whitespace_pending {
            normalized.push(' ');
            whitespace_pending = false;
        }
        normalized.push(character);
        quote = match character {
            '\'' => Some(Quote::Single),
            '"' => Some(Quote::Double),
            '`' => Some(Quote::Backtick),
            '[' => Some(Quote::Bracket),
            _ => None,
        };
    }
    if quote.is_some() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog schema SQL contains an unterminated quoted token",
        ));
    }
    Ok(normalized)
}

pub(crate) fn catalog_revision(connection: &Connection) -> Result<u64> {
    decode_u64(&metadata_blob(connection, "catalog_revision")?)
}

fn metadata_blob(connection: &Connection, key: &str) -> Result<Vec<u8>> {
    connection
        .query_row(
            "SELECT value FROM catalog_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("read catalog metadata", error))
}

fn migration_checksum(migration: &Migration) -> [u8; 32] {
    Sha256::digest(migration.sql.as_bytes()).into()
}

fn unix_time_ns() -> Result<u64> {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("system clock precedes Unix epoch error=[{error}]"),
            )
        })?
        .as_nanos();
    u64::try_from(nanoseconds).map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "system time exceeds catalog timestamp representation",
        )
    })
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, TransactionBehavior};
    use zippy_types::ErrorCode;

    use super::{
        apply_migration, decode_u64, encode_u64, expected_schema_for_migration, migration_checksum,
        schema_fingerprint, validate_migration_registry, Migration, MIGRATIONS, MIGRATION_V1,
        MIGRATION_V1_NAME, MIGRATION_V1_SCHEMA, MIGRATION_V1_VERSION, MIGRATION_V5,
        MIGRATION_V5_NAME, MIGRATION_V5_VERSION,
    };

    #[test]
    fn physical_migration_registry_is_contiguous_and_complete() {
        assert_eq!(validate_migration_registry(), Ok(()));
    }

    #[test]
    fn unsigned_metadata_encoding_round_trips_extremes_and_preserves_order() {
        let values = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        for value in values {
            assert_eq!(decode_u64(&encode_u64(value)), Ok(value));
        }
        for pair in values.windows(2) {
            assert!(encode_u64(pair[0]) < encode_u64(pair[1]));
        }
    }

    #[test]
    fn frozen_physical_v1_v4_migration_and_schema_fingerprints_match_golden() {
        const GOLDENS: [(u32, &str, &str); 4] = [
            (
                1,
                "1d76944b8a0385993c31e6d3c9c02735bb0b206109d9f05de26270b9fec8ea0d",
                "e1a0af37a36ed5d38cac35cfe3ebb74ec5d12ef7c835ac14488c5effb62a7c08",
            ),
            (
                2,
                "a392f1c27d9d810890fd739b52656489baf02881d5a8fc177115619b0ecd3b2a",
                "4fe9f32a22b45fb0b5add76bdb7ae3aaea9ee87c31fde84cab5a3342176494a6",
            ),
            (
                3,
                "30055f90285c90809e0969da420796d96b92840ad881847ce4a7545fd353796d",
                "53d8d1070662bcb429a8b99d4ffc29f6fab0f5f2967ba5028541efbc850b2ae2",
            ),
            (
                4,
                "c8038a185b4aca4d1424da4394c20b5b33be5bc48d75a6372d1e21c3db944c54",
                "8cebe1c67d71abbf7bb71b6f21d335b174334993cfd7989e055acca19a721f59",
            ),
        ];

        for (migration, (version, migration_golden, schema_golden)) in
            MIGRATIONS.iter().take(4).zip(GOLDENS)
        {
            let mut schema = expected_schema_for_migration(migration).expect("build frozen schema");
            schema.sort();
            assert_eq!(migration.version, version);
            assert_eq!(hex(&migration_checksum(migration)), migration_golden);
            assert_eq!(hex(&schema_fingerprint(&schema)), schema_golden);
        }
    }

    #[test]
    fn physical_v5_schema_failure_rolls_back_the_dedicated_replay_table() {
        let mut connection = Connection::open_in_memory().expect("open in-memory Catalog");
        for migration in MIGRATIONS.iter().take(4) {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("begin frozen migration");
            apply_migration(&transaction, migration).expect("apply frozen migration");
            transaction.commit().expect("commit frozen migration");
        }

        let drifted_v5 = Migration {
            version: MIGRATION_V5_VERSION,
            name: MIGRATION_V5_NAME,
            sql: MIGRATION_V5,
            base_schema_version: Some(4),
            schema_additions: &[],
        };
        let error = {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("begin failing physical v5 migration");
            apply_migration(&transaction, &drifted_v5)
                .expect_err("schema validation must reject incomplete physical v5 inventory")
        };
        assert_eq!(error.code(), ErrorCode::DataCorruption);

        let user_version: u32 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read rolled-back physical version");
        assert_eq!(user_version, 4);
        let migration_count: u32 = connection
            .query_row("SELECT COUNT(*) FROM catalog_migrations", [], |row| {
                row.get(0)
            })
            .expect("count rolled-back migration ledger");
        assert_eq!(migration_count, 4);
        let replay_tables: u32 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema \
                 WHERE type = 'table' AND name = 'legacy_import_requests'",
                [],
                |row| row.get(0),
            )
            .expect("count rolled-back replay tables");
        assert_eq!(replay_tables, 0);
        let idempotency_tables: u32 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema \
                 WHERE type = 'table' AND name = 'idempotent_requests'",
                [],
                |row| row.get(0),
            )
            .expect("count retained idempotency tables");
        assert_eq!(idempotency_tables, 1);
    }

    #[test]
    fn fresh_migration_manifest_drift_rolls_back_before_commit() {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        let drifted_manifest = Migration {
            version: MIGRATION_V1_VERSION,
            name: MIGRATION_V1_NAME,
            sql: MIGRATION_V1,
            base_schema_version: None,
            schema_additions: &[],
        };

        assert_migration_rolls_back(&mut connection, &drifted_manifest);
    }

    #[test]
    fn fresh_migration_ledger_drift_rolls_back_before_commit() {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        let drifted_manifest = Migration {
            version: MIGRATION_V1_VERSION,
            name: "drifted_test_migration",
            sql: MIGRATION_V1,
            base_schema_version: None,
            schema_additions: &MIGRATION_V1_SCHEMA,
        };

        assert_migration_rolls_back(&mut connection, &drifted_manifest);
    }

    fn assert_migration_rolls_back(connection: &mut Connection, migration: &Migration) {
        let error = {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .expect("begin drifted migration");
            apply_migration(&transaction, migration)
                .expect_err("migration drift must be rejected before commit")
        };
        assert_eq!(error.code(), ErrorCode::DataCorruption);

        let schema_objects: u64 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'",
                [],
                |row| row.get(0),
            )
            .expect("count schema objects after rollback");
        assert_eq!(schema_objects, 0);
        let user_version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("read schema version after rollback");
        assert_eq!(user_version, 0);
        let application_id: u32 = connection
            .query_row("PRAGMA application_id", [], |row| row.get(0))
            .expect("read application id after rollback");
        assert_eq!(application_id, 0);
    }

    fn hex(bytes: &[u8; 32]) -> String {
        use std::fmt::Write;

        let mut encoded = String::with_capacity(64);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").expect("write to String");
        }
        encoded
    }
}
