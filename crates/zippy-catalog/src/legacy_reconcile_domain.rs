//! Trusted online V3 reconciliation of a migrated table's complete legacy file view.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;

use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use zippy_types::catalog::{
    CatalogActorKind, LegacyCatalogReconcileEnvelopeV3, LegacyCatalogReconcileResponseV3,
    LegacyFileViewContinuity, LegacyFileViewSnapshotMapping,
    LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3, LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY,
    LEGACY_REGISTRY_INDEX_METADATA_KEY, MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{
    DataFileLocation, MetadataMap, MetadataScalar, TableDescriptor, TableState,
};
use zippy_types::{ErrorCode, FileId, OffsetWatermark, RequestId, Result, ServerError, TableId};

use crate::encoding::{decode_u64, encode_offset_watermark, encode_optional_u64, encode_u64};
use crate::legacy_import_domain::{
    prepare_verified_files, validate_source_snapshot_hash, PreparedLegacyFile, MAX_IMPORT_FILES,
};
use crate::storage_codec::{decode_data_file_metadata_blob, encode_snapshot_summary_blob};

const MAX_RECONCILE_COMMAND_BYTES: usize = 64 * 1024 * 1024;
const SQLITE_IDENTITY_QUERY_CHUNK: usize = 256;
const LEGACY_STREAM_NAME_CONFIG_KEY: &str = "zippy.migration.legacy_stream_name";
const LEGACY_REVISION_BASE_CONFIG_KEY: &str =
    "zippy.migration.legacy_persisted_files_revision_base";
const NATIVE_SNAPSHOT_BASE_CONFIG_KEY: &str = "zippy.migration.native_snapshot_version_base";
const LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY: &str = "zippy.migration.source_snapshot_hash";
const LEGACY_MIGRATION_VERSION_CONFIG_KEY: &str = "zippy.migration.version";

pub(crate) struct PreparedLegacyCatalogReconcileV3 {
    envelope: LegacyCatalogReconcileEnvelopeV3,
    files: Vec<PreparedLegacyFile>,
    target_rows: u64,
    target_bytes: u64,
}

struct CurrentLegacyFile {
    file_id: FileId,
    file_path: String,
    schema_version: Vec<u8>,
    file_size_bytes: Vec<u8>,
    row_count: Vec<u8>,
    min_log_offset: Vec<u8>,
    max_log_offset: Vec<u8>,
    legacy_preserve_discovery_order: i64,
    partition_blob: Vec<u8>,
    statistics_blob: Vec<u8>,
    metadata_blob: Vec<u8>,
    checksum: String,
    created_at_ns: Vec<u8>,
}

pub(crate) async fn legacy_reconcile_preflight_bytes(
    envelope: &LegacyCatalogReconcileEnvelopeV3,
) -> Result<usize> {
    let request_id = envelope.request_id;
    if envelope.actor.principal.len() > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
        return Err(resource_exhausted(
            request_id,
            "legacy reconcile actor principal exceeds byte limit",
        ));
    }
    let mut work_since_yield = 0;
    let mut total = crate::manifest_domain::legacy_import_files_preflight_bytes(
        &envelope.command.files,
        &mut work_since_yield,
    )
    .await
    .map_err(|error| error.with_request_id(request_id))?;
    for bytes in [
        envelope.actor.principal.len(),
        envelope.source_snapshot_hash.len(),
        envelope.command.expected_schema_hash.len(),
        size_of::<LegacyCatalogReconcileEnvelopeV3>(),
    ] {
        total = total.checked_add(bytes).ok_or_else(|| {
            resource_exhausted(
                request_id,
                "legacy reconcile command byte estimate overflow",
            )
        })?;
    }
    if total > MAX_RECONCILE_COMMAND_BYTES {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy reconcile command exceeds byte limit actual=[{total}] \
                 maximum=[{MAX_RECONCILE_COMMAND_BYTES}]"
            ),
        ));
    }
    Ok(total.max(1))
}

pub(crate) fn prepare_legacy_catalog_reconcile_v3(
    envelope: LegacyCatalogReconcileEnvelopeV3,
) -> Result<PreparedLegacyCatalogReconcileV3> {
    let request_id = envelope.request_id;
    envelope.verify_fingerprint().map_err(|error| {
        invalid(
            request_id,
            format!("invalid legacy Catalog V3 reconcile fingerprint error=[{error}]"),
        )
    })?;
    if envelope.actor.kind != CatalogActorKind::Server {
        return Err(invalid(
            request_id,
            "legacy Catalog V3 reconcile requires a server actor",
        ));
    }
    validate_source_snapshot_hash(request_id, &envelope.source_snapshot_hash)?;
    validate_source_snapshot_hash(request_id, &envelope.command.expected_schema_hash)
        .map_err(|_| invalid(request_id, "expected Schema hash is not canonical SHA-256"))?;
    if envelope.command.expected_schema_version == 0 {
        return Err(invalid(
            request_id,
            "legacy reconcile Schema version must be nonzero",
        ));
    }
    if envelope.command.legacy_files_revision <= envelope.command.expected_legacy_files_revision {
        return Err(invalid(
            request_id,
            "legacy reconcile target revision must advance the expected revision",
        ));
    }
    if envelope.command.writer_epoch < envelope.command.expected_writer_epoch {
        return Err(invalid(
            request_id,
            "legacy reconcile writer epoch must not move backwards",
        ));
    }
    let files = prepare_verified_files(
        request_id,
        envelope.command.table_id,
        envelope.command.expected_schema_version,
        &envelope.command.expected_schema_hash,
        &envelope.command.files,
        true,
    )?;
    let target_rows = files.iter().try_fold(0_u64, |total, prepared| {
        total
            .checked_add(prepared.file.file.row_count.get())
            .ok_or_else(|| resource_exhausted(request_id, "legacy reconcile row count overflow"))
    })?;
    let target_bytes = files.iter().try_fold(0_u64, |total, prepared| {
        total
            .checked_add(prepared.file.file.file_size_bytes)
            .ok_or_else(|| resource_exhausted(request_id, "legacy reconcile byte count overflow"))
    })?;
    Ok(PreparedLegacyCatalogReconcileV3 {
        envelope,
        files,
        target_rows,
        target_bytes,
    })
}

pub(crate) fn commit_legacy_catalog_reconcile_v3(
    connection: &mut Connection,
    prepared: PreparedLegacyCatalogReconcileV3,
) -> Result<LegacyCatalogReconcileResponseV3> {
    let request_id = prepared.envelope.request_id;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            crate::map_sqlite_error("begin legacy Catalog V3 reconcile", error)
                .with_request_id(request_id)
        })?;
    if let Some(response) = replay(&transaction, &prepared.envelope)? {
        transaction.commit().map_err(|error| {
            crate::map_sqlite_error("commit legacy Catalog V3 replay", error)
                .with_request_id(request_id)
        })?;
        return Ok(response);
    }
    reject_existing_target_revision(&transaction, &prepared.envelope)?;

    let table = load_table(&transaction, prepared.envelope.command.table_id)?;
    validate_table_and_schema(&transaction, &prepared.envelope, &table)?;
    ensure_base_mapping(
        &transaction,
        &table,
        prepared.envelope.command.expected_legacy_files_revision,
    )?;
    let current_mapping =
        load_mapping_by_snapshot(&transaction, table.table_id, table.current_snapshot_version)?
            .ok_or_else(|| corruption("active migrated table has no explicit legacy mapping"))?;
    if current_mapping.legacy_files_revision
        != prepared.envelope.command.expected_legacy_files_revision
    {
        return Err(conflict(
            request_id,
            format!(
                "legacy file revision changed expected=[{}] actual=[{}]",
                prepared.envelope.command.expected_legacy_files_revision,
                current_mapping.legacy_files_revision
            ),
        ));
    }
    let current =
        load_current_legacy_files(&transaction, table.table_id, table.current_snapshot_version)?;
    let (retained, added, removed, new_files) =
        classify_target_files(&transaction, request_id, &prepared.files, &current)?;

    let actual_catalog_revision =
        crate::domain::revision(&transaction).map_err(|error| error.with_request_id(request_id))?;
    let next_catalog_revision = actual_catalog_revision.checked_add(1).ok_or_else(|| {
        resource_exhausted(
            request_id,
            "Catalog revision overflow during legacy reconcile",
        )
    })?;
    let next_snapshot = table
        .current_snapshot_version
        .checked_add(1)
        .ok_or_else(|| resource_exhausted(request_id, "legacy snapshot version overflow"))?;
    let committed_at_ns =
        crate::domain::unix_time_ns().map_err(|error| error.with_request_id(request_id))?;
    let continuity = if prepared.envelope.command.legacy_files_revision
        == prepared
            .envelope
            .command
            .expected_legacy_files_revision
            .checked_add(1)
            .ok_or_else(|| resource_exhausted(request_id, "legacy revision overflow"))?
    {
        LegacyFileViewContinuity::Contiguous
    } else {
        LegacyFileViewContinuity::Gap
    };
    let summary = reconcile_summary(&prepared, retained, added, removed, continuity)?;
    let summary_blob = encode_snapshot_summary_blob(&summary)
        .map_err(|error| error.with_request_id(request_id))?;

    insert_reconcile_snapshot(
        &transaction,
        table.table_id,
        table.current_snapshot_version,
        next_snapshot,
        next_catalog_revision,
        committed_at_ns,
        &summary_blob,
    )?;
    insert_new_files(&transaction, next_snapshot, &new_files)?;
    remove_absent_files(&transaction, next_snapshot, &current, &prepared.files)?;
    insert_snapshot_order(&transaction, table.table_id, next_snapshot, &prepared.files)?;
    update_table_cursor(
        &transaction,
        &prepared.envelope,
        &table,
        next_snapshot,
        committed_at_ns,
    )?;
    crate::domain::update_revision(&transaction, next_catalog_revision)
        .map_err(|error| error.with_request_id(request_id))?;
    insert_reconcile_mapping(
        &transaction,
        &prepared.envelope,
        next_snapshot,
        next_catalog_revision,
        committed_at_ns,
        continuity,
    )?;
    crate::legacy_projection_domain::sync_projection_after_v3_reconcile(
        &transaction,
        table.table_id,
        prepared.envelope.command.expected_schema_version,
        &prepared.envelope.command.expected_schema_hash,
        table.current_snapshot_version,
        prepared.envelope.command.expected_legacy_files_revision,
        prepared.envelope.command.expected_writer_epoch,
        next_snapshot,
        prepared.envelope.command.legacy_files_revision,
        prepared.envelope.command.writer_epoch,
        committed_at_ns,
    )?;
    crate::legacy_projection_domain::downgrade_ready_after_table_mutation(
        &transaction,
        committed_at_ns,
    )?;
    crate::request_ledger::insert_owner(
        &transaction,
        request_id,
        i64::from(LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3),
        crate::request_ledger::V3_RECONCILE_OPERATION,
        &prepared.envelope.canonical_fingerprint.into_bytes(),
        committed_at_ns,
    )?;
    transaction.commit().map_err(|error| {
        crate::map_sqlite_error("commit legacy Catalog V3 reconcile", error)
            .with_request_id(request_id)
    })?;
    Ok(LegacyCatalogReconcileResponseV3 {
        request_id,
        catalog_revision: next_catalog_revision,
        table_id: table.table_id,
        previous_legacy_files_revision: prepared.envelope.command.expected_legacy_files_revision,
        legacy_files_revision: prepared.envelope.command.legacy_files_revision,
        native_snapshot_version: next_snapshot,
        continuity,
    })
}

fn replay(
    connection: &Connection,
    envelope: &LegacyCatalogReconcileEnvelopeV3,
) -> Result<Option<LegacyCatalogReconcileResponseV3>> {
    let fingerprint = envelope.canonical_fingerprint.into_bytes();
    let owner_exists = crate::request_ledger::exact_owner_exists(
        connection,
        envelope.request_id,
        i64::from(LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3),
        crate::request_ledger::V3_RECONCILE_OPERATION,
        &fingerprint,
    )?;
    let stored = load_mapping_by_request_id(connection, envelope.request_id)?;
    let Some(stored) = stored else {
        return if owner_exists {
            Err(corruption(
                "V3 reconcile mapping is missing for its global request owner",
            ))
        } else {
            Ok(None)
        };
    };
    if !owner_exists {
        return Err(corruption(
            "V3 reconcile mapping has no global request owner",
        ));
    }
    let (format_version, fingerprint, previous) = connection
        .query_row(
            "SELECT format_version, fingerprint, previous_legacy_files_revision \
               FROM legacy_file_view_mappings WHERE request_id = ?1",
            [envelope.request_id.into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .map_err(|error| crate::map_sqlite_error("read reconcile replay proof", error))?;
    if format_version != i64::from(LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3) {
        return Err(corruption(
            "V3 reconcile mapping has the wrong format version",
        ));
    }
    let fingerprint =
        fingerprint.ok_or_else(|| corruption("V3 reconcile mapping is missing its fingerprint"))?;
    if fingerprint.len() != 32 {
        return Err(corruption(
            "V3 reconcile mapping fingerprint has an invalid width",
        ));
    }
    if fingerprint.as_slice() != envelope.canonical_fingerprint.into_bytes() {
        return Err(conflict(
            envelope.request_id,
            "legacy reconcile request id was reused with a different payload",
        ));
    }
    if stored.table_id != envelope.command.table_id
        || stored.legacy_files_revision != envelope.command.legacy_files_revision
        || stored.request_id != Some(envelope.request_id)
        || stored.continuity == LegacyFileViewContinuity::Base
        || stored.source_catalog_revision != envelope.source_catalog_revision
        || stored.source_snapshot_hash != envelope.source_snapshot_hash
    {
        return Err(corruption(
            "V3 reconcile replay mapping does not match its fingerprinted command",
        ));
    }
    let previous = decode_u64(&previous)?;
    if previous != envelope.command.expected_legacy_files_revision {
        return Err(corruption(
            "V3 reconcile replay previous revision does not match its command",
        ));
    }
    validate_replay_snapshot(connection, &stored)?;
    Ok(Some(LegacyCatalogReconcileResponseV3 {
        request_id: envelope.request_id,
        catalog_revision: stored.catalog_revision,
        table_id: stored.table_id,
        previous_legacy_files_revision: previous,
        legacy_files_revision: stored.legacy_files_revision,
        native_snapshot_version: stored.native_snapshot_version,
        continuity: stored.continuity,
    }))
}

fn validate_replay_snapshot(
    connection: &Connection,
    mapping: &LegacyFileViewSnapshotMapping,
) -> Result<()> {
    let (parent, operation, commit_lsn, created_at_ns) = connection
        .query_row(
            "SELECT parent_snapshot_version, operation, commit_lsn, created_at_ns \
               FROM table_snapshots WHERE table_id = ?1 AND snapshot_version = ?2",
            params![
                mapping.table_id.into_bytes().as_slice(),
                encode_u64(mapping.native_snapshot_version).as_slice(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read V3 reconcile replay snapshot", error))?
        .ok_or_else(|| corruption("V3 reconcile replay snapshot is missing"))?;
    let parent = crate::encoding::decode_optional_u64(&parent)?;
    if !valid_replay_snapshot_shape(mapping.native_snapshot_version, parent, &operation)
        || decode_u64(&commit_lsn)? != mapping.catalog_revision
        || decode_u64(&created_at_ns)? != mapping.committed_at_ns
    {
        return Err(corruption(
            "V3 reconcile replay snapshot does not match its explicit mapping",
        ));
    }
    let observed_catalog_revision = crate::domain::revision(connection)?;
    if mapping.catalog_revision > observed_catalog_revision {
        return Err(corruption(
            "V3 reconcile replay mapping is ahead of the Catalog revision",
        ));
    }
    Ok(())
}

fn valid_replay_snapshot_shape(
    native_snapshot_version: u64,
    parent_snapshot_version: Option<u64>,
    operation: &str,
) -> bool {
    native_snapshot_version >= 2
        && parent_snapshot_version == native_snapshot_version.checked_sub(1)
        && operation == "import"
}

fn reject_existing_target_revision(
    connection: &Connection,
    envelope: &LegacyCatalogReconcileEnvelopeV3,
) -> Result<()> {
    if read_legacy_file_view_mapping(
        connection,
        envelope.command.table_id,
        envelope.command.legacy_files_revision,
    )?
    .is_some()
    {
        return Err(conflict(
            envelope.request_id,
            "legacy revision already maps to a different committed request",
        ));
    }
    Ok(())
}

fn load_table(connection: &Connection, table_id: TableId) -> Result<TableDescriptor> {
    let raw = connection
        .query_row(
            crate::table_domain::GET_TABLE_BY_ID_INCLUDING_INACTIVE_SQL,
            [table_id.into_bytes().as_slice()],
            crate::table_domain::table_row,
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy reconcile table", error))?
        .ok_or_else(|| {
            ServerError::new(ErrorCode::NotFound, "legacy reconcile table was not found")
                .with_table_id(table_id)
        })?;
    crate::table_domain::decode_table_row(raw)
}

fn validate_table_and_schema(
    connection: &Connection,
    envelope: &LegacyCatalogReconcileEnvelopeV3,
    table: &TableDescriptor,
) -> Result<()> {
    let request_id = envelope.request_id;
    let command = &envelope.command;
    if table.state != TableState::Active {
        return Err(conflict(request_id, "legacy reconcile table is not active"));
    }
    if table.current_schema_version != command.expected_schema_version {
        return Err(conflict(
            request_id,
            format!(
                "legacy reconcile Schema version changed expected=[{}] actual=[{}]",
                command.expected_schema_version, table.current_schema_version
            ),
        ));
    }
    if table.writer_epoch != command.expected_writer_epoch {
        return Err(conflict(
            request_id,
            format!(
                "legacy reconcile writer epoch changed expected=[{}] actual=[{}]",
                command.expected_writer_epoch, table.writer_epoch
            ),
        ));
    }
    let schema_hash = connection
        .query_row(
            "SELECT schema_hash FROM table_schemas WHERE table_id = ?1 AND schema_version = ?2",
            params![
                table.table_id.into_bytes().as_slice(),
                encode_u64(table.current_schema_version).as_slice(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy reconcile Schema", error))?
        .ok_or_else(|| corruption("active legacy table is missing its current Schema"))?;
    if schema_hash != command.expected_schema_hash {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            "legacy reconcile Schema hash does not match the active table",
        )
        .with_request_id(request_id)
        .with_table_id(table.table_id)
        .with_retryable(false));
    }
    validate_migration_config(table)?;
    Ok(())
}

pub(crate) fn validate_migration_config(table: &TableDescriptor) -> Result<(u64, u64, u64)> {
    let value = |key: &str| {
        table.config.get(key).ok_or_else(|| {
            corruption(format!(
                "legacy table is missing migration config key=[{key}]"
            ))
        })
    };
    if value(LEGACY_STREAM_NAME_CONFIG_KEY)?.as_str() != table.name.as_str() {
        return Err(corruption(
            "legacy migration stream identity does not match the Catalog table name",
        ));
    }
    if !is_canonical_sha256(value(LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY)?) {
        return Err(corruption(
            "legacy migration source snapshot hash is invalid",
        ));
    }
    let migration_version = parse_config_u64(
        value(LEGACY_MIGRATION_VERSION_CONFIG_KEY)?,
        "migration version",
    )?;
    if !matches!(migration_version, 1 | 2) {
        return Err(corruption("legacy migration version is unsupported"));
    }
    let legacy_base = parse_config_u64(
        value(LEGACY_REVISION_BASE_CONFIG_KEY)?,
        "legacy revision base",
    )?;
    let native_base = parse_config_u64(
        value(NATIVE_SNAPSHOT_BASE_CONFIG_KEY)?,
        "native snapshot base",
    )?;
    if native_base == 0 || native_base > table.current_snapshot_version {
        return Err(corruption(
            "legacy native snapshot base is outside the table history",
        ));
    }
    if (migration_version == 1 && native_base != 1) || (migration_version == 2 && native_base != 2)
    {
        return Err(corruption(
            "legacy migration version does not match its native snapshot base",
        ));
    }
    Ok((migration_version, legacy_base, native_base))
}

fn is_canonical_sha256(value: &str) -> bool {
    value.strip_prefix("sha256-v1:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn parse_config_u64(value: &str, field: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| corruption(format!("legacy {field} is not a canonical u64")))
        .and_then(|parsed| {
            if parsed.to_string() == value {
                Ok(parsed)
            } else {
                Err(corruption(format!("legacy {field} is not canonical")))
            }
        })
}

pub(crate) fn ensure_base_mapping(
    connection: &Connection,
    table: &TableDescriptor,
    expected_legacy_files_revision: u64,
) -> Result<()> {
    let (migration_version, legacy_base, native_base) = validate_migration_config(table)?;
    if load_mapping_by_snapshot(connection, table.table_id, table.current_snapshot_version)?
        .is_some()
    {
        let base = read_legacy_file_view_mapping(connection, table.table_id, legacy_base)?
            .ok_or_else(|| corruption("legacy mapping history is missing its migration base"))?;
        if base.native_snapshot_version != native_base
            || base.continuity != LegacyFileViewContinuity::Base
        {
            return Err(corruption(
                "legacy mapping history has an invalid migration base",
            ));
        }
        return Ok(());
    }
    if table.current_snapshot_version != native_base
        || expected_legacy_files_revision != legacy_base
    {
        return Err(corruption(
            "legacy mapping history is missing beyond the validated migration base",
        ));
    }
    let existing_mapping_count: u64 = connection
        .query_row(
            "SELECT COUNT(*) FROM legacy_file_view_mappings WHERE table_id = ?1",
            [table.table_id.into_bytes().as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("count legacy base mappings", error))?;
    if existing_mapping_count != 0 {
        return Err(corruption(
            "legacy mapping history has no active mapping but contains older mappings",
        ));
    }
    let (parent, operation, commit_lsn, created_at_ns) = connection
        .query_row(
            "SELECT parent_snapshot_version, operation, commit_lsn, created_at_ns \
               FROM table_snapshots \
              WHERE table_id = ?1 AND snapshot_version = ?2",
            params![
                table.table_id.into_bytes().as_slice(),
                encode_u64(native_base).as_slice(),
            ],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy base snapshot", error))?
        .ok_or_else(|| corruption("legacy migration base snapshot was not found"))?;
    let parent = crate::encoding::decode_optional_u64(&parent)?;
    let snapshot_shape_is_valid = match migration_version {
        1 => operation == "create" && parent.is_none(),
        2 => operation == "import" && parent == Some(1),
        _ => unreachable!("migration version was validated"),
    };
    if !snapshot_shape_is_valid {
        return Err(corruption(
            "legacy migration base snapshot has an invalid operation or parent",
        ));
    }
    decode_u64(&commit_lsn)?;
    decode_u64(&created_at_ns)?;
    connection
        .execute(
            "INSERT INTO legacy_file_view_mappings(\
                 table_id, legacy_files_revision, native_snapshot_version, request_id, \
                 format_version, fingerprint, previous_legacy_files_revision, \
                 source_catalog_revision, source_snapshot_hash, catalog_revision, continuity, \
                 committed_at_ns\
             ) VALUES (?1, ?2, ?3, NULL, 0, NULL, ?2, ?4, ?5, ?6, 'base', ?7)",
            params![
                table.table_id.into_bytes().as_slice(),
                encode_u64(legacy_base).as_slice(),
                encode_u64(native_base).as_slice(),
                encode_u64(0).as_slice(),
                table.config[LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY].as_str(),
                commit_lsn.as_slice(),
                created_at_ns.as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("bootstrap explicit legacy base mapping", error)
        })?;
    let base_file_count = bootstrap_snapshot_order(connection, table.table_id, native_base)?;
    if (migration_version == 1 && base_file_count != 0)
        || (migration_version == 2 && base_file_count == 0)
    {
        return Err(corruption(
            "legacy migration base file set does not match its migration version",
        ));
    }
    Ok(())
}

pub(crate) fn validate_exact_current_legacy_view(
    connection: &Connection,
    request_id: RequestId,
    table_id: TableId,
    snapshot_version: u64,
    expected: &[PreparedLegacyFile],
) -> Result<()> {
    let current = load_current_legacy_files(connection, table_id, snapshot_version)?;
    if current.len() != expected.len() {
        return Err(conflict(
            request_id,
            "existing legacy file count does not match the adopted full view",
        ));
    }
    let by_id = current
        .iter()
        .map(|file| (file.file_id, file))
        .collect::<HashMap<_, _>>();
    for prepared in expected {
        let Some(current) = by_id.get(&prepared.file.file.file_id) else {
            return Err(conflict(
                request_id,
                "existing legacy file identity is missing from the adopted full view",
            ));
        };
        let expected_path = match &prepared.file.file.location {
            DataFileLocation::ExternalLegacy(path) => path.as_str(),
            DataFileLocation::Managed(_) => unreachable!("verified files are external legacy"),
        };
        if current.file_path != expected_path
            || !immutable_file_matches(current, prepared)
                .map_err(|error| error.with_request_id(request_id))?
        {
            return Err(conflict(
                request_id,
                "existing legacy file immutable metadata does not match adoption",
            ));
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT file_id, discovery_order FROM legacy_reconcile_snapshot_files \
              WHERE table_id = ?1 AND snapshot_version = ?2 ORDER BY discovery_order",
        )
        .map_err(|error| crate::map_sqlite_error("prepare adopted legacy order", error))?;
    let rows = statement
        .query_map(
            params![
                table_id.into_bytes().as_slice(),
                encode_u64(snapshot_version).as_slice(),
            ],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .map_err(|error| crate::map_sqlite_error("read adopted legacy order", error))?;
    let mut actual = Vec::new();
    for row in rows {
        actual.push(
            row.map_err(|error| crate::map_sqlite_error("decode adopted legacy order", error))?,
        );
    }
    if actual.len() != expected.len()
        || actual.iter().zip(expected).any(|((file_id, order), file)| {
            file_id.as_slice() != file.file.file.file_id.into_bytes()
                || decode_u64(order).ok() != Some(file.file.discovery_order)
        })
    {
        return Err(corruption(
            "existing legacy snapshot order does not match adoption",
        ));
    }
    Ok(())
}

fn bootstrap_snapshot_order(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
) -> Result<usize> {
    let absent = encode_optional_u64(None);
    connection
        .execute(
            "INSERT INTO legacy_reconcile_snapshot_files(\
                 table_id, snapshot_version, file_id, discovery_order\
             ) SELECT table_id, ?2, file_id, substr(legacy_discovery_order, 2, 8) \
                 FROM data_files \
                WHERE table_id = ?1 AND state = 'committed' \
                  AND added_snapshot_version <= ?2 \
                  AND (removed_snapshot_version = ?3 OR removed_snapshot_version > ?4) \
                ORDER BY legacy_discovery_order",
            params![
                table_id.into_bytes().as_slice(),
                encode_u64(snapshot_version).as_slice(),
                absent.as_slice(),
                encode_optional_u64(Some(snapshot_version)).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("bootstrap legacy snapshot order", error))
}

fn load_current_legacy_files(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
) -> Result<Vec<CurrentLegacyFile>> {
    let mut statement = connection
        .prepare(
            "SELECT file_id, file_path, schema_version, file_size_bytes, row_count, \
                    min_log_offset, max_log_offset, legacy_preserve_discovery_order, \
                    partition_blob, statistics_blob, metadata_blob, checksum, created_at_ns, \
                    legacy_discovery_order, location_kind, provenance_kind \
               FROM data_files \
              WHERE table_id = ?1 AND state = 'committed' \
                AND added_snapshot_version <= ?2 \
                AND (removed_snapshot_version = ?3 OR removed_snapshot_version > ?4) \
              ORDER BY file_id LIMIT ?5",
        )
        .map_err(|error| crate::map_sqlite_error("prepare current legacy files", error))?;
    let mut rows = statement
        .query(params![
            table_id.into_bytes().as_slice(),
            encode_u64(snapshot_version).as_slice(),
            encode_optional_u64(None).as_slice(),
            encode_optional_u64(Some(snapshot_version)).as_slice(),
            i64::try_from(MAX_IMPORT_FILES + 1).expect("file bound fits i64"),
        ])
        .map_err(|error| crate::map_sqlite_error("query current legacy files", error))?;
    let mut files = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read current legacy file", error))?
    {
        let location_kind: String = row.get(14).map_err(|error| {
            crate::map_sqlite_error("decode current legacy location kind", error)
        })?;
        let provenance_kind: String = row.get(15).map_err(|error| {
            crate::map_sqlite_error("decode current legacy provenance kind", error)
        })?;
        if location_kind != "external_legacy" || provenance_kind != "legacy" {
            return Err(corruption(
                "migrated legacy view contains a non-legacy data file",
            ));
        }
        let raw_id: Vec<u8> = row
            .get(0)
            .map_err(|error| crate::map_sqlite_error("decode current legacy file id", error))?;
        let file_id = decode_file_id(&raw_id)?;
        let tagged_order: Vec<u8> = row.get(13).map_err(|error| {
            crate::map_sqlite_error("decode current legacy discovery order", error)
        })?;
        crate::encoding::decode_optional_u64(&tagged_order)?
            .ok_or_else(|| corruption("legacy file is missing its discovery order"))?;
        files.push(CurrentLegacyFile {
            file_id,
            file_path: row
                .get(1)
                .map_err(|error| crate::map_sqlite_error("decode current legacy path", error))?,
            schema_version: row
                .get(2)
                .map_err(|error| crate::map_sqlite_error("decode current legacy Schema", error))?,
            file_size_bytes: row
                .get(3)
                .map_err(|error| crate::map_sqlite_error("decode current legacy size", error))?,
            row_count: row
                .get(4)
                .map_err(|error| crate::map_sqlite_error("decode current legacy rows", error))?,
            min_log_offset: row.get(5).map_err(|error| {
                crate::map_sqlite_error("decode current legacy minimum offset", error)
            })?,
            max_log_offset: row.get(6).map_err(|error| {
                crate::map_sqlite_error("decode current legacy maximum offset", error)
            })?,
            legacy_preserve_discovery_order: row.get(7).map_err(|error| {
                crate::map_sqlite_error("decode current legacy ordering flag", error)
            })?,
            partition_blob: row.get(8).map_err(|error| {
                crate::map_sqlite_error("decode current legacy partition", error)
            })?,
            statistics_blob: row.get(9).map_err(|error| {
                crate::map_sqlite_error("decode current legacy statistics", error)
            })?,
            metadata_blob: row.get(10).map_err(|error| {
                crate::map_sqlite_error("decode current legacy metadata", error)
            })?,
            checksum: row.get(11).map_err(|error| {
                crate::map_sqlite_error("decode current legacy checksum", error)
            })?,
            created_at_ns: row.get(12).map_err(|error| {
                crate::map_sqlite_error("decode current legacy timestamp", error)
            })?,
        });
    }
    if files.len() > MAX_IMPORT_FILES {
        return Err(resource_exhausted(
            RequestId::from_u128(1).expect("one is nonzero"),
            "current legacy file view exceeds reconcile file limit",
        ));
    }
    Ok(files)
}

fn classify_target_files<'a>(
    connection: &Connection,
    request_id: RequestId,
    target: &'a [PreparedLegacyFile],
    current: &'a [CurrentLegacyFile],
) -> Result<(usize, usize, usize, Vec<&'a PreparedLegacyFile>)> {
    let by_id = current
        .iter()
        .enumerate()
        .map(|(index, file)| (file.file_id, index))
        .collect::<HashMap<_, _>>();
    let by_path = current
        .iter()
        .enumerate()
        .map(|(index, file)| (file.file_path.as_str(), index))
        .collect::<HashMap<_, _>>();
    if by_id.len() != current.len() || by_path.len() != current.len() {
        return Err(corruption(
            "current legacy view contains duplicate identities",
        ));
    }
    let mut retained_indexes = HashSet::with_capacity(target.len());
    let mut new_files = Vec::new();
    for prepared in target {
        let file = &prepared.file.file;
        let path = match &file.location {
            DataFileLocation::ExternalLegacy(path) => path.as_str(),
            DataFileLocation::Managed(_) => unreachable!("verified files are external legacy"),
        };
        match (by_id.get(&file.file_id), by_path.get(path)) {
            (Some(by_id), Some(by_path)) if by_id == by_path => {
                if !immutable_file_matches(&current[*by_id], prepared)
                    .map_err(|error| error.with_request_id(request_id))?
                {
                    return Err(conflict(
                        request_id,
                        "retained legacy file immutable identity changed",
                    ));
                }
                retained_indexes.insert(*by_id);
            }
            (None, None) => new_files.push(prepared),
            _ => {
                return Err(conflict(
                    request_id,
                    "legacy target reuses a current file id or path with a different identity",
                ));
            }
        }
    }
    ensure_new_identities_unseen(connection, request_id, &new_files)?;
    let retained = retained_indexes.len();
    let removed = current.len().checked_sub(retained).ok_or_else(|| {
        corruption("legacy reconcile retained file count exceeds current file count")
    })?;
    Ok((retained, new_files.len(), removed, new_files))
}

fn immutable_file_matches(
    current: &CurrentLegacyFile,
    expected: &PreparedLegacyFile,
) -> Result<bool> {
    let file = &expected.file.file;
    let physical_fields_match = current.schema_version == encode_u64(file.schema_version)
        && current.file_size_bytes == encode_u64(file.file_size_bytes)
        && current.row_count == encode_u64(file.row_count.get())
        && current.min_log_offset == encode_optional_u64(None)
        && current.max_log_offset == encode_optional_u64(None)
        && current.legacy_preserve_discovery_order == 1
        && current.partition_blob == expected.partition_blob
        && current.statistics_blob == expected.statistics_blob
        && current.checksum == file.checksum
        && current.created_at_ns == encode_u64(file.created_at_ns);
    if !physical_fields_match {
        return Ok(false);
    }
    if current.metadata_blob == expected.metadata_blob {
        return Ok(true);
    }

    let mut current_metadata = decode_data_file_metadata_blob(&current.metadata_blob)?;
    let mut expected_metadata = file.metadata.clone();
    for key in [
        LEGACY_REGISTRY_INDEX_METADATA_KEY,
        LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY,
    ] {
        current_metadata.remove(key);
        expected_metadata.remove(key);
    }
    Ok(current_metadata == expected_metadata)
}

fn ensure_new_identities_unseen(
    connection: &Connection,
    request_id: RequestId,
    files: &[&PreparedLegacyFile],
) -> Result<()> {
    for chunk in files.chunks(SQLITE_IDENTITY_QUERY_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT 1 FROM data_files WHERE file_id IN ({placeholders}) LIMIT 1");
        let ids = chunk
            .iter()
            .map(|prepared| prepared.file.file.file_id.into_bytes())
            .collect::<Vec<_>>();
        if connection
            .query_row(
                &sql,
                params_from_iter(ids.iter().map(|value| value.as_slice())),
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| crate::map_sqlite_error("check reconcile file identity", error))?
            .is_some()
        {
            return Err(conflict(
                request_id,
                "legacy reconcile cannot resurrect a removed file identity",
            ));
        }
        let sql = format!("SELECT 1 FROM data_files WHERE file_path IN ({placeholders}) LIMIT 1");
        let paths = chunk
            .iter()
            .map(|prepared| match &prepared.file.file.location {
                DataFileLocation::ExternalLegacy(path) => path.as_str(),
                DataFileLocation::Managed(_) => unreachable!("verified files are external legacy"),
            });
        if connection
            .query_row(&sql, params_from_iter(paths), |_| Ok(()))
            .optional()
            .map_err(|error| crate::map_sqlite_error("check reconcile file path", error))?
            .is_some()
        {
            return Err(conflict(
                request_id,
                "legacy reconcile cannot reuse a removed or foreign file path",
            ));
        }
    }
    Ok(())
}

fn reconcile_summary(
    prepared: &PreparedLegacyCatalogReconcileV3,
    retained: usize,
    added: usize,
    removed: usize,
    continuity: LegacyFileViewContinuity,
) -> Result<MetadataMap> {
    let count = |value: usize, name: &str| {
        u64::try_from(value).map_err(|_| {
            resource_exhausted(
                prepared.envelope.request_id,
                format!("{name} count overflow"),
            )
        })
    };
    Ok(MetadataMap::from([
        (
            "target_files".to_owned(),
            MetadataScalar::UInt64(count(prepared.files.len(), "target file")?),
        ),
        (
            "target_rows".to_owned(),
            MetadataScalar::UInt64(prepared.target_rows),
        ),
        (
            "target_bytes".to_owned(),
            MetadataScalar::UInt64(prepared.target_bytes),
        ),
        (
            "retained_files".to_owned(),
            MetadataScalar::UInt64(count(retained, "retained file")?),
        ),
        (
            "added_files".to_owned(),
            MetadataScalar::UInt64(count(added, "added file")?),
        ),
        (
            "removed_files".to_owned(),
            MetadataScalar::UInt64(count(removed, "removed file")?),
        ),
        (
            "previous_legacy_files_revision".to_owned(),
            MetadataScalar::UInt64(prepared.envelope.command.expected_legacy_files_revision),
        ),
        (
            "legacy_files_revision".to_owned(),
            MetadataScalar::UInt64(prepared.envelope.command.legacy_files_revision),
        ),
        (
            "continuity_gap".to_owned(),
            MetadataScalar::UInt64(u64::from(continuity == LegacyFileViewContinuity::Gap)),
        ),
    ]))
}

fn insert_reconcile_snapshot(
    connection: &Connection,
    table_id: TableId,
    parent_snapshot: u64,
    snapshot_version: u64,
    commit_lsn: u64,
    committed_at_ns: u64,
    summary_blob: &[u8],
) -> Result<()> {
    connection
        .execute(
            "INSERT INTO table_snapshots(\
                 table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                 committed_log_offset, created_at_ns, summary_blob\
             ) VALUES (?1, ?2, ?3, 'import', ?4, ?5, ?6, ?7)",
            params![
                table_id.into_bytes().as_slice(),
                encode_u64(snapshot_version).as_slice(),
                encode_optional_u64(Some(parent_snapshot)).as_slice(),
                encode_u64(commit_lsn).as_slice(),
                encode_offset_watermark(OffsetWatermark::LegacyUnknown).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                summary_blob,
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert legacy reconcile snapshot", error))?;
    Ok(())
}

fn insert_new_files(
    connection: &Connection,
    snapshot_version: u64,
    files: &[&PreparedLegacyFile],
) -> Result<()> {
    let mut statement = connection
        .prepare_cached(
            "INSERT INTO data_files(\
                 file_id, table_id, schema_version, location_kind, file_path, \
                 file_size_bytes, row_count, provenance_kind, min_log_offset, \
                 max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
                 partition_blob, statistics_blob, metadata_blob, checksum, \
                 added_snapshot_version, removed_snapshot_version, state, created_at_ns\
             ) VALUES (?1, ?2, ?3, 'external_legacy', ?4, ?5, ?6, 'legacy', ?7, ?7, 1, \
                 ?8, ?9, ?10, ?11, ?12, ?13, ?7, 'committed', ?14)",
        )
        .map_err(|error| crate::map_sqlite_error("prepare reconcile file insert", error))?;
    let absent = encode_optional_u64(None);
    for prepared in files {
        let file = &prepared.file.file;
        let path = match &file.location {
            DataFileLocation::ExternalLegacy(path) => path.as_str(),
            DataFileLocation::Managed(_) => unreachable!("verified files are external legacy"),
        };
        statement
            .execute(params![
                file.file_id.into_bytes().as_slice(),
                file.table_id.into_bytes().as_slice(),
                encode_u64(file.schema_version).as_slice(),
                path,
                encode_u64(file.file_size_bytes).as_slice(),
                encode_u64(file.row_count.get()).as_slice(),
                absent.as_slice(),
                encode_optional_u64(Some(prepared.file.discovery_order)).as_slice(),
                prepared.partition_blob.as_slice(),
                prepared.statistics_blob.as_slice(),
                prepared.metadata_blob.as_slice(),
                file.checksum.as_str(),
                encode_u64(snapshot_version).as_slice(),
                encode_u64(file.created_at_ns).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("insert reconcile legacy file", error))?;
    }
    Ok(())
}

fn remove_absent_files(
    connection: &Connection,
    snapshot_version: u64,
    current: &[CurrentLegacyFile],
    target: &[PreparedLegacyFile],
) -> Result<()> {
    let retained = target
        .iter()
        .map(|prepared| prepared.file.file.file_id)
        .collect::<HashSet<_>>();
    let mut statement = connection
        .prepare_cached(
            "UPDATE data_files SET removed_snapshot_version = ?1 \
              WHERE file_id = ?2 AND state = 'committed' \
                AND removed_snapshot_version = ?3",
        )
        .map_err(|error| crate::map_sqlite_error("prepare reconcile file removal", error))?;
    for file in current
        .iter()
        .filter(|file| !retained.contains(&file.file_id))
    {
        let updated = statement
            .execute(params![
                encode_optional_u64(Some(snapshot_version)).as_slice(),
                file.file_id.into_bytes().as_slice(),
                encode_optional_u64(None).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("remove absent legacy file", error))?;
        if updated != 1 {
            return Err(corruption(
                "legacy reconcile removed an unexpected data-file row count",
            ));
        }
    }
    Ok(())
}

pub(crate) fn insert_snapshot_order(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
    files: &[PreparedLegacyFile],
) -> Result<()> {
    let mut statement = connection
        .prepare_cached(
            "INSERT INTO legacy_reconcile_snapshot_files(\
                 table_id, snapshot_version, file_id, discovery_order\
             ) VALUES (?1, ?2, ?3, ?4)",
        )
        .map_err(|error| crate::map_sqlite_error("prepare reconcile snapshot order", error))?;
    for prepared in files {
        statement
            .execute(params![
                table_id.into_bytes().as_slice(),
                encode_u64(snapshot_version).as_slice(),
                prepared.file.file.file_id.into_bytes().as_slice(),
                encode_u64(prepared.file.discovery_order).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("insert reconcile snapshot order", error))?;
    }
    Ok(())
}

fn update_table_cursor(
    connection: &Connection,
    envelope: &LegacyCatalogReconcileEnvelopeV3,
    table: &TableDescriptor,
    next_snapshot: u64,
    committed_at_ns: u64,
) -> Result<()> {
    let updated = connection
        .execute(
            "UPDATE tables SET current_snapshot_version = ?1, writer_epoch = ?2, \
                    updated_at_ns = ?3 \
              WHERE table_id = ?4 AND current_snapshot_version = ?5 \
                AND current_schema_version = ?6 AND writer_epoch = ?7 AND state = 'active'",
            params![
                encode_u64(next_snapshot).as_slice(),
                encode_u64(envelope.command.writer_epoch).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                table.table_id.into_bytes().as_slice(),
                encode_u64(table.current_snapshot_version).as_slice(),
                encode_u64(envelope.command.expected_schema_version).as_slice(),
                encode_u64(envelope.command.expected_writer_epoch).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("advance legacy table cursor", error))?;
    if updated != 1 {
        return Err(conflict(
            envelope.request_id,
            "legacy table cursor changed during reconcile",
        ));
    }
    Ok(())
}

fn insert_reconcile_mapping(
    connection: &Connection,
    envelope: &LegacyCatalogReconcileEnvelopeV3,
    snapshot_version: u64,
    catalog_revision: u64,
    committed_at_ns: u64,
    continuity: LegacyFileViewContinuity,
) -> Result<()> {
    let continuity = match continuity {
        LegacyFileViewContinuity::Contiguous => "contiguous",
        LegacyFileViewContinuity::Gap => "gap",
        LegacyFileViewContinuity::Base => unreachable!("V3 commit is not a base mapping"),
    };
    connection
        .execute(
            "INSERT INTO legacy_file_view_mappings(\
                 table_id, legacy_files_revision, native_snapshot_version, request_id, \
                 format_version, fingerprint, previous_legacy_files_revision, \
                 source_catalog_revision, source_snapshot_hash, catalog_revision, continuity, \
                 committed_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                envelope.command.table_id.into_bytes().as_slice(),
                encode_u64(envelope.command.legacy_files_revision).as_slice(),
                encode_u64(snapshot_version).as_slice(),
                envelope.request_id.into_bytes().as_slice(),
                i64::from(LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3),
                envelope.canonical_fingerprint.into_bytes().as_slice(),
                encode_u64(envelope.command.expected_legacy_files_revision).as_slice(),
                encode_u64(envelope.source_catalog_revision).as_slice(),
                envelope.source_snapshot_hash.as_str(),
                encode_u64(catalog_revision).as_slice(),
                continuity,
                encode_u64(committed_at_ns).as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("insert legacy reconcile mapping", error)
                .with_request_id(envelope.request_id)
        })?;
    Ok(())
}

pub(crate) fn read_legacy_file_view_mapping(
    connection: &Connection,
    table_id: TableId,
    legacy_files_revision: u64,
) -> Result<Option<LegacyFileViewSnapshotMapping>> {
    load_mapping(
        connection,
        "table_id = ?1 AND legacy_files_revision = ?2",
        params![
            table_id.into_bytes().as_slice(),
            encode_u64(legacy_files_revision).as_slice(),
        ],
    )
}

pub(crate) fn read_current_legacy_file_view_mapping(
    connection: &Connection,
    table_id: TableId,
) -> Result<LegacyFileViewSnapshotMapping> {
    connection
        .query_row(
            "SELECT mappings.table_id, mappings.legacy_files_revision, \
                    mappings.native_snapshot_version, mappings.request_id, \
                    mappings.catalog_revision, mappings.source_catalog_revision, \
                    mappings.source_snapshot_hash, mappings.continuity, \
                    mappings.committed_at_ns \
               FROM tables \
               JOIN legacy_file_view_mappings AS mappings \
                 ON mappings.table_id = tables.table_id \
                AND mappings.native_snapshot_version = tables.current_snapshot_version \
              WHERE tables.table_id = ?1 AND tables.state = 'active'",
            [table_id.into_bytes().as_slice()],
            mapping_row,
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read current explicit legacy mapping", error))?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::NotFound,
                "active table has no explicit mapping for its current snapshot",
            )
            .with_table_id(table_id)
            .with_retryable(false)
        })
        .and_then(decode_mapping)
}

fn load_mapping_by_snapshot(
    connection: &Connection,
    table_id: TableId,
    snapshot_version: u64,
) -> Result<Option<LegacyFileViewSnapshotMapping>> {
    load_mapping(
        connection,
        "table_id = ?1 AND native_snapshot_version = ?2",
        params![
            table_id.into_bytes().as_slice(),
            encode_u64(snapshot_version).as_slice(),
        ],
    )
}

fn load_mapping_by_request_id(
    connection: &Connection,
    request_id: RequestId,
) -> Result<Option<LegacyFileViewSnapshotMapping>> {
    load_mapping(
        connection,
        "request_id = ?1",
        params![request_id.into_bytes().as_slice()],
    )
}

fn load_mapping<P>(
    connection: &Connection,
    predicate: &str,
    parameters: P,
) -> Result<Option<LegacyFileViewSnapshotMapping>>
where
    P: rusqlite::Params,
{
    let sql = format!(
        "SELECT table_id, legacy_files_revision, native_snapshot_version, request_id, \
                catalog_revision, source_catalog_revision, source_snapshot_hash, continuity, \
                committed_at_ns \
           FROM legacy_file_view_mappings WHERE {predicate}"
    );
    connection
        .query_row(&sql, parameters, mapping_row)
        .optional()
        .map_err(|error| crate::map_sqlite_error("read explicit legacy mapping", error))?
        .map(decode_mapping)
        .transpose()
}

type RawLegacyMapping = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Option<Vec<u8>>,
    Vec<u8>,
    Vec<u8>,
    String,
    String,
    Vec<u8>,
);

fn mapping_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawLegacyMapping> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
    ))
}

fn decode_mapping(raw: RawLegacyMapping) -> Result<LegacyFileViewSnapshotMapping> {
    let request_id = raw.3.map(|bytes| decode_request_id(&bytes)).transpose()?;
    let continuity = match raw.7.as_str() {
        "base" => LegacyFileViewContinuity::Base,
        "contiguous" => LegacyFileViewContinuity::Contiguous,
        "gap" => LegacyFileViewContinuity::Gap,
        _ => return Err(corruption("explicit legacy mapping has invalid continuity")),
    };
    let mapping = LegacyFileViewSnapshotMapping {
        table_id: decode_table_id(&raw.0)?,
        legacy_files_revision: decode_u64(&raw.1)?,
        native_snapshot_version: decode_u64(&raw.2)?,
        request_id,
        catalog_revision: decode_u64(&raw.4)?,
        source_catalog_revision: decode_u64(&raw.5)?,
        source_snapshot_hash: raw.6,
        continuity,
        committed_at_ns: decode_u64(&raw.8)?,
    };
    if mapping.native_snapshot_version == 0
        || mapping.catalog_revision == 0
        || mapping.committed_at_ns == 0
        || !is_canonical_sha256(&mapping.source_snapshot_hash)
        || (mapping.request_id.is_none() && mapping.continuity != LegacyFileViewContinuity::Base)
    {
        return Err(corruption(
            "explicit legacy mapping violates intrinsic invariants",
        ));
    }
    Ok(mapping)
}

fn decode_table_id(bytes: &[u8]) -> Result<TableId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        corruption(format!(
            "explicit legacy mapping table id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    TableId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "explicit legacy mapping table id is invalid error=[{error}]"
        ))
    })
}

fn decode_file_id(bytes: &[u8]) -> Result<FileId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        corruption(format!(
            "legacy file id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    FileId::from_bytes(bytes)
        .map_err(|error| corruption(format!("legacy file id is invalid error=[{error}]")))
}

fn decode_request_id(bytes: &[u8]) -> Result<RequestId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        corruption(format!(
            "explicit legacy mapping request id has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    RequestId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "explicit legacy mapping request id is invalid error=[{error}]"
        ))
    })
}

fn invalid(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn conflict(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Conflict, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn resource_exhausted(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

#[cfg(test)]
mod tests {
    use super::valid_replay_snapshot_shape;

    #[test]
    fn replay_snapshot_shape_accepts_first_reconcile_snapshot() {
        assert!(valid_replay_snapshot_shape(2, Some(1), "import"));
    }

    #[test]
    fn replay_snapshot_shape_accepts_later_contiguous_snapshot() {
        assert!(valid_replay_snapshot_shape(3, Some(2), "import"));
    }

    #[test]
    fn replay_snapshot_shape_rejects_initial_snapshot() {
        assert!(!valid_replay_snapshot_shape(1, Some(0), "import"));
    }

    #[test]
    fn replay_snapshot_shape_rejects_wrong_parent_or_operation() {
        assert!(!valid_replay_snapshot_shape(3, Some(1), "import"));
        assert!(!valid_replay_snapshot_shape(3, Some(2), "append"));
    }
}
