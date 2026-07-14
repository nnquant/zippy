//! Migration-only V2 legacy table import transaction.

use std::collections::BTreeSet;
use std::mem::size_of;

use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use zippy_types::catalog::{
    CatalogActorKind, CreateTableCommand, LegacyCatalogImportCodecV2, LegacyCatalogImportCommandV2,
    LegacyCatalogImportEnvelopeV2, LegacyCatalogImportReplayProbeV2, LegacyCatalogImportResponseV2,
    LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2, LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY,
    MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{
    DataFileLocation, DataFileProvenance, MetadataMap, MetadataScalar, SnapshotOperation,
    TableSnapshot, TableState,
};
use zippy_types::{ErrorCode, OffsetWatermark, RequestId, Result, ServerError, ServerErrorDetails};

use crate::encoding::{decode_u64, encode_offset_watermark, encode_optional_u64, encode_u64};
use crate::storage_codec::{
    encode_data_file_metadata_blob, encode_data_file_partition_blob,
    encode_data_file_statistics_blob, encode_snapshot_summary_blob,
};

const IMPORT_SNAPSHOT_VERSION: u64 = 2;
const INITIAL_SNAPSHOT_VERSION: u64 = 1;
const INITIAL_SCHEMA_VERSION: u64 = 1;
const MAX_IMPORT_FILES: usize = 4_096;
const SQLITE_IDENTITY_QUERY_CHUNK: usize = 256;
const MAX_LEGACY_IMPORT_COMMAND_BYTES: usize = 64 * 1024 * 1024;
const MAX_LEGACY_IMPORT_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

struct StoredLegacyImportReplayHeader {
    format_version: i64,
    fingerprint: Vec<u8>,
    response_blob_type: String,
    response_blob_len: i64,
    committed_at_ns: Vec<u8>,
}

pub(crate) struct PreparedLegacyCatalogImportV2 {
    envelope: LegacyCatalogImportEnvelopeV2,
    table: crate::table_domain::PreparedCreateTable,
    files: Vec<PreparedLegacyFile>,
    summary_blob: Vec<u8>,
    summary: MetadataMap,
}

struct PreparedLegacyFile {
    file: zippy_types::catalog::LegacyCatalogImportFileV2,
    partition_blob: Vec<u8>,
    statistics_blob: Vec<u8>,
    metadata_blob: Vec<u8>,
}

pub(crate) async fn legacy_import_preflight_bytes(
    envelope: &LegacyCatalogImportEnvelopeV2,
) -> Result<usize> {
    let request_id = envelope.request_id;
    if envelope.actor.principal.len() > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy import actor principal exceeds byte limit",
        )
        .with_request_id(request_id)
        .with_retryable(false));
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
        envelope.command.name.as_str().len(),
        envelope.command.schema_ipc.as_bytes().len(),
        envelope.command.canonical_schema_hash.len(),
        size_of::<LegacyCatalogImportEnvelopeV2>(),
    ] {
        total = total.checked_add(bytes).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy import command byte estimate overflow",
            )
            .with_request_id(request_id)
        })?;
    }
    for (key, value) in &envelope.command.config {
        total = total
            .checked_add(key.len())
            .and_then(|value_bytes| value_bytes.checked_add(value.len()))
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "legacy import config byte estimate overflow",
                )
                .with_request_id(request_id)
            })?;
    }
    if total > MAX_LEGACY_IMPORT_COMMAND_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "legacy import command exceeds byte limit actual=[{total}] maximum=[{MAX_LEGACY_IMPORT_COMMAND_BYTES}]"
            ),
        )
        .with_request_id(request_id)
        .with_retryable(false));
    }
    Ok(total.max(1))
}

pub(crate) fn prepare_legacy_catalog_import_v2(
    envelope: LegacyCatalogImportEnvelopeV2,
) -> Result<PreparedLegacyCatalogImportV2> {
    let request_id = envelope.request_id;
    envelope.verify_fingerprint().map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("invalid legacy Catalog V2 import fingerprint error=[{error}]"),
        )
        .with_request_id(request_id)
        .with_retryable(false)
    })?;
    if envelope.actor.kind != CatalogActorKind::Migration {
        return Err(invalid(
            request_id,
            "legacy Catalog V2 import requires a migration actor",
        ));
    }
    validate_source_snapshot_hash(request_id, &envelope.source_snapshot_hash)?;
    if envelope.expected_revision.is_none() {
        return Err(invalid(
            request_id,
            "legacy Catalog V2 import requires an exact expected revision",
        ));
    }
    if envelope.command.files.is_empty() {
        return Err(invalid(
            request_id,
            "legacy Catalog V2 import requires at least one verified file",
        ));
    }
    if envelope.command.files.len() > MAX_IMPORT_FILES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "legacy Catalog V2 import file count exceeds maximum actual=[{}] maximum=[{MAX_IMPORT_FILES}]",
                envelope.command.files.len()
            ),
        )
        .with_request_id(request_id));
    }
    if envelope.command.legacy_files_revision == 0 {
        return Err(invalid(
            request_id,
            "nonempty legacy file import requires a nonzero source file revision",
        ));
    }

    let table = crate::table_domain::prepare_create_table(CreateTableCommand {
        table_id: envelope.command.table_id,
        namespace_id: envelope.command.namespace_id,
        name: envelope.command.name.clone(),
        definition: envelope.command.definition.clone(),
        schema_ipc: envelope.command.schema_ipc.clone(),
        config: envelope.command.config.clone(),
    })
    .map_err(|error| error.with_request_id(request_id))?;
    if table.schema_hash() != envelope.command.canonical_schema_hash {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "legacy import canonical Schema hash mismatch expected=[{}] actual=[{}]",
                envelope.command.canonical_schema_hash,
                table.schema_hash()
            ),
        )
        .with_request_id(request_id)
        .with_retryable(false));
    }

    let files = prepare_files(request_id, &envelope.command)?;
    let summary = import_summary(&envelope.command)?;
    let summary_blob = encode_snapshot_summary_blob(&summary)
        .map_err(|error| error.with_request_id(request_id))?;
    Ok(PreparedLegacyCatalogImportV2 {
        envelope,
        table,
        files,
        summary_blob,
        summary,
    })
}

fn prepare_files(
    request_id: RequestId,
    command: &LegacyCatalogImportCommandV2,
) -> Result<Vec<PreparedLegacyFile>> {
    let mut paths = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut prepared = Vec::with_capacity(command.files.len());
    for (index, import) in command.files.iter().enumerate() {
        let expected_order = u64::try_from(index).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy discovery order overflow",
            )
            .with_request_id(request_id)
        })?;
        if import.discovery_order != expected_order {
            return Err(invalid(
                request_id,
                format!(
                    "legacy file discovery order is not contiguous expected=[{expected_order}] actual=[{}]",
                    import.discovery_order
                ),
            ));
        }
        let file = &import.file;
        if !file_ids.insert(file.file_id) {
            return Err(invalid(
                request_id,
                "legacy import contains a duplicate file id",
            ));
        }
        if file.table_id != command.table_id || file.schema_version != 1 {
            return Err(invalid(
                request_id,
                "legacy file table identity or schema version does not match its import",
            ));
        }
        if file.file_size_bytes == 0 {
            return Err(invalid(
                request_id,
                "legacy import requires nonempty verified files",
            ));
        }
        let DataFileLocation::ExternalLegacy(path) = &file.location else {
            return Err(invalid(
                request_id,
                "legacy Catalog V2 import accepts only external legacy locations",
            ));
        };
        if !paths.insert(path.as_str()) {
            return Err(invalid(
                request_id,
                "legacy import contains a duplicate file path",
            ));
        }
        match file.provenance {
            DataFileProvenance::Legacy {
                offsets: None,
                preserve_discovery_order: true,
            } => {}
            _ => {
                return Err(invalid(
                    request_id,
                    "legacy file must retain unknown offsets and discovery order",
                ));
            }
        }
        match file.metadata.get(LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY) {
            Some(MetadataScalar::String(verified_schema_hash))
                if verified_schema_hash == &command.canonical_schema_hash => {}
            _ => {
                return Err(ServerError::new(
                    ErrorCode::SchemaMismatch,
                    "legacy file verifier Schema proof does not match the imported table Schema",
                )
                .with_request_id(request_id)
                .with_table_id(command.table_id)
                .with_retryable(false));
            }
        }
        validate_sha256_checksum(request_id, &file.checksum)?;
        let partition_blob = encode_data_file_partition_blob(&file.partition)
            .map_err(|error| error.with_request_id(request_id))?;
        let statistics_blob = encode_data_file_statistics_blob(&file.statistics)
            .map_err(|error| error.with_request_id(request_id))?;
        let metadata_blob = encode_data_file_metadata_blob(&file.metadata)
            .map_err(|error| error.with_request_id(request_id))?;
        prepared.push(PreparedLegacyFile {
            file: import.clone(),
            partition_blob,
            statistics_blob,
            metadata_blob,
        });
    }
    Ok(prepared)
}

fn validate_source_snapshot_hash(request_id: RequestId, value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256-v1:") else {
        return Err(invalid(
            request_id,
            "legacy source snapshot hash must use sha256-v1",
        ));
    };
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            request_id,
            "legacy source snapshot hash is not canonical lowercase SHA-256",
        ));
    }
    Ok(())
}

fn validate_sha256_checksum(request_id: RequestId, checksum: &str) -> Result<()> {
    let Some(digest) = checksum.strip_prefix("sha256-v1:") else {
        return Err(invalid(
            request_id,
            "legacy file checksum must use sha256-v1",
        ));
    };
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            request_id,
            "legacy file checksum is not canonical lowercase SHA-256",
        ));
    }
    Ok(())
}

fn import_summary(command: &LegacyCatalogImportCommandV2) -> Result<MetadataMap> {
    let rows = command.files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.file.row_count.get()).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy import row count overflow",
            )
        })
    })?;
    let bytes = command.files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.file.file_size_bytes).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy import byte count overflow",
            )
        })
    })?;
    let file_count = u64::try_from(command.files.len()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy import file count overflow",
        )
    })?;
    Ok(MetadataMap::from([
        ("added_files".to_owned(), MetadataScalar::UInt64(file_count)),
        ("added_rows".to_owned(), MetadataScalar::UInt64(rows)),
        ("added_bytes".to_owned(), MetadataScalar::UInt64(bytes)),
        (
            "legacy_files_revision".to_owned(),
            MetadataScalar::UInt64(command.legacy_files_revision),
        ),
    ]))
}

pub(crate) fn commit_legacy_catalog_import_v2(
    connection: &mut Connection,
    prepared: PreparedLegacyCatalogImportV2,
) -> Result<LegacyCatalogImportResponseV2> {
    let request_id = prepared.envelope.request_id;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            crate::map_sqlite_error("begin legacy Catalog V2 import", error)
                .with_request_id(request_id)
        })?;
    if let Some(response) = replay(&transaction, &prepared.envelope)? {
        transaction.commit().map_err(|error| {
            crate::map_sqlite_error("commit legacy Catalog V2 replay", error)
                .with_request_id(request_id)
        })?;
        return Ok(response);
    }

    let actual_revision =
        crate::domain::revision(&transaction).map_err(|error| error.with_request_id(request_id))?;
    if let Some(expected) = prepared.envelope.expected_revision {
        if expected != actual_revision {
            return Err(revision_conflict(request_id, expected, actual_revision));
        }
    }
    let next_revision = actual_revision.checked_add(1).ok_or_else(|| {
        ServerError::new(ErrorCode::ResourceExhausted, "Catalog revision overflow")
            .with_request_id(request_id)
    })?;
    let committed_at_ns =
        crate::domain::unix_time_ns().map_err(|error| error.with_request_id(request_id))?;
    let mut table = crate::table_domain::create_table(
        &transaction,
        prepared.table,
        next_revision,
        committed_at_ns,
    )
    .map_err(|error| error.with_request_id(request_id))?;
    ensure_file_identities_available(&transaction, request_id, &prepared.files)?;
    insert_import_snapshot(
        &transaction,
        prepared.envelope.command.table_id,
        next_revision,
        committed_at_ns,
        &prepared.summary_blob,
    )?;
    insert_legacy_files(
        &transaction,
        prepared.envelope.command.table_id,
        &prepared.files,
    )?;
    update_imported_table(
        &transaction,
        prepared.envelope.command.table_id,
        prepared.envelope.command.writer_epoch,
        committed_at_ns,
    )?;
    table.current_snapshot_version = IMPORT_SNAPSHOT_VERSION;
    table.writer_epoch = prepared.envelope.command.writer_epoch;
    table.updated_at_ns = committed_at_ns;
    let snapshot = TableSnapshot {
        table_id: table.table_id,
        snapshot_version: IMPORT_SNAPSHOT_VERSION,
        parent_snapshot_version: Some(INITIAL_SNAPSHOT_VERSION),
        operation: SnapshotOperation::Import,
        commit_lsn: next_revision,
        committed_log_offset: OffsetWatermark::LegacyUnknown,
        created_at_ns: committed_at_ns,
        summary: prepared.summary,
    };
    let response = LegacyCatalogImportResponseV2 {
        request_id,
        catalog_revision: next_revision,
        table,
        snapshot,
    };
    let response_blob =
        LegacyCatalogImportCodecV2::encode_response(&response).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("encode frozen legacy Catalog V2 response error=[{error}]"),
            )
            .with_request_id(request_id)
        })?;
    if response_blob.len() > MAX_LEGACY_IMPORT_RESPONSE_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "legacy Catalog V2 response exceeds byte limit actual=[{}] maximum=[{MAX_LEGACY_IMPORT_RESPONSE_BYTES}]",
                response_blob.len()
            ),
        )
        .with_request_id(request_id)
        .with_retryable(false));
    }
    let fingerprint = prepared.envelope.canonical_fingerprint.into_bytes();
    crate::domain::update_revision(&transaction, next_revision)
        .map_err(|error| error.with_request_id(request_id))?;
    transaction
        .execute(
            "INSERT INTO legacy_import_requests(\
                 request_id, format_version, fingerprint, response_blob, committed_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                request_id.into_bytes().as_slice(),
                i64::from(LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2),
                fingerprint.as_slice(),
                response_blob,
                encode_u64(committed_at_ns).as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("persist legacy Catalog V2 permanent replay response", error)
                .with_request_id(request_id)
        })?;
    transaction.commit().map_err(|error| {
        crate::map_sqlite_error("commit legacy Catalog V2 import", error)
            .with_request_id(request_id)
    })?;
    Ok(response)
}

fn replay(
    connection: &Connection,
    envelope: &LegacyCatalogImportEnvelopeV2,
) -> Result<Option<LegacyCatalogImportResponseV2>> {
    reject_v1_request_id(connection, envelope.request_id)?;
    let Some(stored) = load_legacy_import_replay_header(connection, envelope.request_id)? else {
        return Ok(None);
    };
    validate_replay_fingerprint_width(envelope.request_id, &stored.fingerprint)?;
    if stored.fingerprint.as_slice() != envelope.canonical_fingerprint.into_bytes() {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "legacy Catalog request id was reused with a different fingerprint",
        )
        .with_request_id(envelope.request_id)
        .with_retryable(false));
    }
    let observed_catalog_revision = crate::domain::revision(connection)
        .map_err(|error| error.with_request_id(envelope.request_id))?;
    let response = load_and_validate_intrinsic_replay_response(
        connection,
        envelope.request_id,
        observed_catalog_revision,
        &stored,
    )?;
    validate_replay_response_matches_command(envelope, &response)?;
    Ok(Some(response))
}

/// Looks up one permanent V2 response without materializing or validating an import command.
///
/// Revision, cross-version request-ID conflict, and the retained V2 record are observed in one
/// deferred read transaction. The response remains a startup hint: callers must still compare it
/// with their independently reconstructed migration plan.
pub(crate) fn probe_legacy_catalog_import_v2(
    connection: &mut Connection,
    request_id: RequestId,
) -> Result<LegacyCatalogImportReplayProbeV2> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| {
            crate::map_sqlite_error("begin legacy Catalog V2 replay probe", error)
                .with_request_id(request_id)
        })?;
    let observed_catalog_revision =
        crate::domain::revision(&transaction).map_err(|error| error.with_request_id(request_id))?;
    reject_v1_request_id(&transaction, request_id)?;
    let probe = match load_legacy_import_replay_header(&transaction, request_id)? {
        None => LegacyCatalogImportReplayProbeV2::Missing {
            observed_catalog_revision,
        },
        Some(stored) => {
            validate_replay_fingerprint_width(request_id, &stored.fingerprint)?;
            let response = load_and_validate_intrinsic_replay_response(
                &transaction,
                request_id,
                observed_catalog_revision,
                &stored,
            )?;
            LegacyCatalogImportReplayProbeV2::Committed {
                observed_catalog_revision,
                response,
            }
        }
    };
    transaction.commit().map_err(|error| {
        crate::map_sqlite_error("commit legacy Catalog V2 replay probe", error)
            .with_request_id(request_id)
    })?;
    Ok(probe)
}

fn load_legacy_import_replay_header(
    connection: &Connection,
    request_id: RequestId,
) -> Result<Option<StoredLegacyImportReplayHeader>> {
    connection
        .query_row(
            "SELECT format_version, fingerprint, typeof(response_blob), length(response_blob), \
                    committed_at_ns \
             FROM legacy_import_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| {
                Ok(StoredLegacyImportReplayHeader {
                    format_version: row.get(0)?,
                    fingerprint: row.get(1)?,
                    response_blob_type: row.get(2)?,
                    response_blob_len: row.get(3)?,
                    committed_at_ns: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(|error| {
            crate::map_sqlite_error("read legacy Catalog V2 replay header", error)
                .with_request_id(request_id)
        })
}

fn validate_replay_fingerprint_width(request_id: RequestId, fingerprint: &[u8]) -> Result<()> {
    if fingerprint.len() != 32 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 replay fingerprint has invalid width",
        )
        .with_request_id(request_id));
    }
    Ok(())
}

fn validate_replay_blob_header(
    request_id: RequestId,
    stored: &StoredLegacyImportReplayHeader,
) -> Result<usize> {
    if stored.format_version != i64::from(LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 permanent replay format is inconsistent",
        )
        .with_request_id(request_id));
    }
    let response_blob_len = usize::try_from(stored.response_blob_len).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 response has an invalid byte length",
        )
        .with_request_id(request_id)
    })?;
    if stored.response_blob_type != "blob" || response_blob_len == 0 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 response is not a nonempty blob",
        )
        .with_request_id(request_id));
    }
    if response_blob_len > MAX_LEGACY_IMPORT_RESPONSE_BYTES {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "legacy Catalog V2 response exceeds byte limit actual=[{response_blob_len}] maximum=[{MAX_LEGACY_IMPORT_RESPONSE_BYTES}]"
            ),
        )
        .with_request_id(request_id));
    }
    Ok(response_blob_len)
}

fn load_and_validate_intrinsic_replay_response(
    connection: &Connection,
    request_id: RequestId,
    observed_catalog_revision: u64,
    stored: &StoredLegacyImportReplayHeader,
) -> Result<LegacyCatalogImportResponseV2> {
    let expected_blob_len = validate_replay_blob_header(request_id, stored)?;
    let committed_at_ns = decode_u64(&stored.committed_at_ns).map_err(|_| {
        ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 replay commit time has an invalid encoding",
        )
        .with_request_id(request_id)
    })?;
    let response_blob = connection
        .query_row(
            "SELECT response_blob FROM legacy_import_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| {
            crate::map_sqlite_error("read bounded legacy Catalog V2 response", error)
                .with_request_id(request_id)
        })?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::DataCorruption,
                "legacy Catalog V2 response disappeared inside a replay transaction",
            )
            .with_request_id(request_id)
        })?;
    if response_blob.len() != expected_blob_len {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 response length changed inside a replay transaction",
        )
        .with_request_id(request_id));
    }
    let response =
        LegacyCatalogImportCodecV2::decode_response(&response_blob).map_err(|error| {
            ServerError::new(
                ErrorCode::DataCorruption,
                format!("decode frozen legacy Catalog V2 response error=[{error}]"),
            )
            .with_request_id(request_id)
        })?;
    validate_intrinsic_replay_response(
        request_id,
        observed_catalog_revision,
        committed_at_ns,
        &response,
    )?;
    Ok(response)
}

fn validate_intrinsic_replay_response(
    request_id: RequestId,
    observed_catalog_revision: u64,
    committed_at_ns: u64,
    response: &LegacyCatalogImportResponseV2,
) -> Result<()> {
    let summary = &response.snapshot.summary;
    let summary_value = |key: &str| match summary.get(key) {
        Some(MetadataScalar::UInt64(value)) => Some(*value),
        _ => None,
    };
    let added_files = summary_value("added_files");
    let added_rows = summary_value("added_rows");
    let added_bytes = summary_value("added_bytes");
    let legacy_files_revision = summary_value("legacy_files_revision");
    let summary_is_valid = summary.len() == 4
        && added_files.is_some_and(|value| {
            value > 0 && value <= u64::try_from(MAX_IMPORT_FILES).expect("file limit fits u64")
        })
        && added_rows.is_some_and(|value| value > 0)
        && added_bytes.is_some_and(|value| value > 0)
        && legacy_files_revision.is_some_and(|value| value > 0);
    let response_is_valid = response.request_id == request_id
        && response.catalog_revision > 0
        && response.catalog_revision <= observed_catalog_revision
        && committed_at_ns > 0
        && response.table.state == TableState::Active
        && response.table.current_schema_version == INITIAL_SCHEMA_VERSION
        && response.table.current_snapshot_version == IMPORT_SNAPSHOT_VERSION
        && response.table.created_at_ns == committed_at_ns
        && response.table.updated_at_ns == committed_at_ns
        && response.snapshot.table_id == response.table.table_id
        && response.snapshot.snapshot_version == IMPORT_SNAPSHOT_VERSION
        && response.snapshot.parent_snapshot_version == Some(INITIAL_SNAPSHOT_VERSION)
        && response.snapshot.operation == SnapshotOperation::Import
        && response.snapshot.commit_lsn == response.catalog_revision
        && response.snapshot.committed_log_offset == OffsetWatermark::LegacyUnknown
        && response.snapshot.created_at_ns == committed_at_ns
        && summary_is_valid;
    if !response_is_valid {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 replay response violates intrinsic import invariants",
        )
        .with_request_id(request_id));
    }
    Ok(())
}

fn validate_replay_response_matches_command(
    envelope: &LegacyCatalogImportEnvelopeV2,
    response: &LegacyCatalogImportResponseV2,
) -> Result<()> {
    let expected_catalog_revision = envelope
        .expected_revision
        .and_then(|revision| revision.checked_add(1))
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::DataCorruption,
                "legacy Catalog V2 replay has an invalid expected revision",
            )
            .with_request_id(envelope.request_id)
        })?;
    let command = &envelope.command;
    let response_matches = response.catalog_revision == expected_catalog_revision
        && response.table.table_id == command.table_id
        && response.table.namespace_id == command.namespace_id
        && response.table.name == command.name
        && response.table.definition == command.definition
        && response.table.current_schema_version == 1
        && response.table.current_snapshot_version == IMPORT_SNAPSHOT_VERSION
        && response.table.writer_epoch == command.writer_epoch
        && response.table.config == command.config
        && response.snapshot.table_id == command.table_id
        && response.snapshot.summary == import_summary(command)?;
    if !response_matches {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy Catalog V2 replay response does not match its command",
        )
        .with_request_id(envelope.request_id));
    }
    Ok(())
}

fn reject_v1_request_id(connection: &Connection, request_id: RequestId) -> Result<()> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM idempotent_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| {
            crate::map_sqlite_error("check V1 request-id conflict", error)
                .with_request_id(request_id)
        })?
        .is_some();
    if exists {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "catalog request id is already committed in the V1 command namespace",
        )
        .with_request_id(request_id)
        .with_retryable(false));
    }
    Ok(())
}

fn ensure_file_identities_available(
    connection: &Connection,
    request_id: RequestId,
    files: &[PreparedLegacyFile],
) -> Result<()> {
    for chunk in files.chunks(SQLITE_IDENTITY_QUERY_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT 1 FROM data_files WHERE file_id IN ({placeholders}) LIMIT 1");
        let encoded = chunk
            .iter()
            .map(|prepared| prepared.file.file.file_id.into_bytes())
            .collect::<Vec<_>>();
        let found = connection
            .query_row(
                &sql,
                params_from_iter(encoded.iter().map(|value| value.as_slice())),
                |_| Ok(()),
            )
            .optional()
            .map_err(|error| crate::map_sqlite_error("check legacy file identity", error))?;
        if found.is_some() {
            return Err(ServerError::new(
                ErrorCode::AlreadyExists,
                "legacy file identity already exists",
            )
            .with_request_id(request_id));
        }
    }
    for chunk in files.chunks(SQLITE_IDENTITY_QUERY_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT 1 FROM data_files WHERE file_path IN ({placeholders}) LIMIT 1");
        let paths = chunk
            .iter()
            .map(|prepared| match &prepared.file.file.location {
                DataFileLocation::ExternalLegacy(path) => path.as_str(),
                DataFileLocation::Managed(_) => unreachable!("legacy files were validated"),
            })
            .collect::<Vec<_>>();
        let found = connection
            .query_row(&sql, params_from_iter(paths), |_| Ok(()))
            .optional()
            .map_err(|error| crate::map_sqlite_error("check legacy file path", error))?;
        if found.is_some() {
            return Err(ServerError::new(
                ErrorCode::AlreadyExists,
                "legacy file path already exists",
            )
            .with_request_id(request_id));
        }
    }
    Ok(())
}

fn insert_import_snapshot(
    connection: &Connection,
    table_id: zippy_types::TableId,
    commit_lsn: u64,
    created_at_ns: u64,
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
                encode_u64(IMPORT_SNAPSHOT_VERSION).as_slice(),
                encode_optional_u64(Some(INITIAL_SNAPSHOT_VERSION)).as_slice(),
                encode_u64(commit_lsn).as_slice(),
                encode_offset_watermark(OffsetWatermark::LegacyUnknown).as_slice(),
                encode_u64(created_at_ns).as_slice(),
                summary_blob,
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert legacy import snapshot", error))?;
    Ok(())
}

fn insert_legacy_files(
    connection: &Connection,
    table_id: zippy_types::TableId,
    files: &[PreparedLegacyFile],
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
        .map_err(|error| crate::map_sqlite_error("prepare legacy file insert", error))?;
    let absent = encode_optional_u64(None);
    for prepared in files {
        let file = &prepared.file.file;
        let path = match &file.location {
            DataFileLocation::ExternalLegacy(path) => path.as_str(),
            DataFileLocation::Managed(_) => unreachable!("legacy files were validated"),
        };
        statement
            .execute(params![
                file.file_id.into_bytes().as_slice(),
                table_id.into_bytes().as_slice(),
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
                encode_u64(IMPORT_SNAPSHOT_VERSION).as_slice(),
                encode_u64(file.created_at_ns).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("insert legacy Catalog file", error))?;
    }
    Ok(())
}

fn update_imported_table(
    connection: &Connection,
    table_id: zippy_types::TableId,
    writer_epoch: u64,
    updated_at_ns: u64,
) -> Result<()> {
    let updated = connection
        .execute(
            "UPDATE tables SET current_snapshot_version = ?1, writer_epoch = ?2, \
             updated_at_ns = ?3 WHERE table_id = ?4",
            params![
                encode_u64(IMPORT_SNAPSHOT_VERSION).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("update imported legacy table", error))?;
    if updated != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy imported table update affected an unexpected row count",
        ));
    }
    Ok(())
}

fn revision_conflict(request_id: RequestId, expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        format!("Catalog revision changed expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_details(ServerErrorDetails::RevisionConflict { expected, actual })
    .expect("revision conflict details match error code")
}

fn invalid(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
        .with_request_id(request_id)
        .with_retryable(false)
}
