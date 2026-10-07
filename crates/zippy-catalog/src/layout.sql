-- Current Catalog layout only; no online upgrades from previous layouts.

CREATE TABLE catalog_meta (
    key TEXT PRIMARY KEY CHECK(length(key) > 0),
    value BLOB NOT NULL CHECK(typeof(value) = 'blob')
);

CREATE TABLE catalog_layout (
    schema_version INTEGER PRIMARY KEY CHECK(schema_version > 0),
    layout_name TEXT NOT NULL UNIQUE CHECK(length(layout_name) > 0),
    layout_checksum BLOB NOT NULL CHECK(
        typeof(layout_checksum) = 'blob' AND length(layout_checksum) = 32
    ),
    created_at_ns BLOB NOT NULL CHECK(
        typeof(created_at_ns) = 'blob' AND length(created_at_ns) = 8
    )
);

CREATE TABLE catalog_request_ids (
    request_id BLOB PRIMARY KEY CHECK(
        typeof(request_id) = 'blob'
        AND length(request_id) = 16
        AND request_id != zeroblob(16)
    ),
    format_version INTEGER NOT NULL CHECK(
        typeof(format_version) = 'integer' AND format_version = 1
    ),
    operation TEXT NOT NULL CHECK(
        typeof(operation) = 'text'
        AND (
            (format_version = 1 AND operation IN (
                'create_namespace', 'create_table', 'alter_table_schema',
                'acquire_producer', 'commit_data_files', 'commit_compaction',
                'commit_consumer_offset', 'acquire_snapshot_lease',
                'release_snapshot_lease', 'create_maintenance_job',
                'transition_maintenance_job', 'drop_table'
            ))
        )
    ),
    fingerprint BLOB NOT NULL CHECK(
        typeof(fingerprint) = 'blob' AND length(fingerprint) = 32
    ),
    committed_at_ns BLOB NOT NULL CHECK(
        typeof(committed_at_ns) = 'blob' AND length(committed_at_ns) = 8
    )
 );

CREATE TABLE data_file_sources (
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
 );

CREATE TABLE data_files (
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
    location_kind TEXT NOT NULL CHECK(location_kind = 'managed'),
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
    provenance_kind TEXT NOT NULL CHECK(provenance_kind = 'native'),
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
    CHECK(substr(min_log_offset, 1, 1) = X'01'),
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
 );

CREATE TABLE durable_consumer_offsets (
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
 );

CREATE TABLE durable_consumers (
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
 );

CREATE TABLE idempotent_requests (
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
 );








CREATE TABLE maintenance_jobs (
    job_id BLOB PRIMARY KEY CHECK(
        typeof(job_id) = 'blob'
        AND length(job_id) = 16
        AND job_id != zeroblob(16)
    ),
    job_kind TEXT NOT NULL CHECK(
        job_kind IN (
            'compaction', 'retention', 'garbage_collection',
            'verify', 'backup'
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
 );

CREATE TABLE namespaces (
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
 );

CREATE TABLE producers (
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
                substr(last_committed_log_offset, 1, 1) = X'00'
                AND substr(last_committed_log_offset, 2, 8) = zeroblob(8)
            )
        )
    ),
    updated_at_ns BLOB NOT NULL CHECK(
        typeof(updated_at_ns) = 'blob' AND length(updated_at_ns) = 8
    ),
    FOREIGN KEY(table_id) REFERENCES tables(table_id)
 );

CREATE TABLE snapshot_leases (
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
 );

CREATE TABLE table_schemas (
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
 );

CREATE TABLE table_snapshots (
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
        operation IN ('create', 'persist', 'compaction', 'retention')
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
                substr(committed_log_offset, 1, 1) = X'00'
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
 );

CREATE TABLE tables (
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
    FOREIGN KEY(namespace_id) REFERENCES namespaces(namespace_id),
    FOREIGN KEY(table_id, current_schema_version)
        REFERENCES table_schemas(table_id, schema_version)
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(table_id, current_snapshot_version)
        REFERENCES table_snapshots(table_id, snapshot_version)
        DEFERRABLE INITIALLY DEFERRED
 );

CREATE INDEX data_files_active_manifest
 ON data_files(table_id, file_id)
 WHERE state = 'committed'
   AND removed_snapshot_version = X'000000000000000000';

CREATE INDEX data_files_by_gc_state
 ON data_files(state, removed_snapshot_version, table_id, file_id);

CREATE INDEX data_files_by_schema
 ON data_files(table_id, schema_version, file_id);

CREATE INDEX data_files_gc_claim_by_table
 ON data_files(table_id, removed_snapshot_version_value, file_id, added_snapshot_version)
 WHERE state = 'committed'
   AND location_kind = 'managed'
   AND removed_snapshot_version_value IS NOT NULL;

CREATE INDEX data_files_gc_retry_by_table
 ON data_files(table_id, file_id)
 WHERE state = 'deleting'
   AND location_kind = 'managed'
   AND removed_snapshot_version_value IS NOT NULL;

CREATE INDEX data_files_offsets
 ON data_files(table_id, min_log_offset, max_log_offset);

CREATE INDEX data_files_visible_by_table
 ON data_files(table_id, state, added_snapshot_version, removed_snapshot_version);

CREATE INDEX durable_consumer_offsets_by_consumer
 ON durable_consumer_offsets(consumer_id, table_id, authority_kind, authority_id);

CREATE INDEX durable_consumers_by_table
 ON durable_consumers(table_id, consumer_id);

CREATE INDEX idempotent_requests_by_expiry
 ON idempotent_requests(expires_at_ns, request_id);

CREATE INDEX maintenance_jobs_by_admission
 ON maintenance_jobs(state, job_kind, priority DESC, created_at_ns, job_id);

CREATE INDEX producers_by_table
 ON producers(table_id, producer_id);

CREATE INDEX snapshot_leases_by_expiry
 ON snapshot_leases(expires_at_ns, table_id, snapshot_version, lease_id);

CREATE INDEX snapshot_leases_gc_by_table
 ON snapshot_leases(table_id, snapshot_version, expires_at_ns);

CREATE INDEX tables_by_namespace_state_id
 ON tables(namespace_id, state, table_id);

CREATE INDEX tables_by_namespace_state_name
 ON tables(namespace_id, state, name, table_id);

CREATE INDEX tables_by_state_id
 ON tables(state, table_id);

CREATE UNIQUE INDEX tables_reserved_name ON tables(namespace_id, name) WHERE state != 'dropped';

