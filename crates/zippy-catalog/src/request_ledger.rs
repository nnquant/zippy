//! Permanent global ownership ledger for Catalog request identifiers.

use rusqlite::{params, Connection, OptionalExtension};
use zippy_types::{ErrorCode, RequestId, Result, ServerError};

use crate::encoding::{decode_u64, encode_u64};

pub(crate) const V2_IMPORT_OPERATION: &str = "legacy_import_table";
pub(crate) const V3_RECONCILE_OPERATION: &str = "legacy_reconcile_files";
pub(crate) const V4_BOOTSTRAP_OPERATION: &str = "legacy_bootstrap_table";
pub(crate) const V4_ADOPT_OPERATION: &str = "legacy_adopt_existing_table";
pub(crate) const V4_SYNC_OPERATION: &str = "legacy_sync_table_metadata";
pub(crate) const V4_DROP_OPERATION: &str = "legacy_drop_table";
pub(crate) const V4_CHECKPOINT_OPERATION: &str = "legacy_advance_checkpoint";

struct RequestOwner {
    format_version: i64,
    operation: String,
    fingerprint: Vec<u8>,
    committed_at_ns: Vec<u8>,
}

/// Returns whether the exact request owner is already committed.
///
/// Any other owner of the same request id is a permanent conflict. Callers must additionally
/// validate their version-specific durable response when this returns `true`.
pub(crate) fn exact_owner_exists(
    connection: &Connection,
    request_id: RequestId,
    format_version: i64,
    operation: &str,
    fingerprint: &[u8; 32],
) -> Result<bool> {
    let Some(owner) = load_owner(connection, request_id)? else {
        return Ok(false);
    };
    validate_owner(&owner)?;
    if owner.format_version == format_version
        && owner.operation == operation
        && owner.fingerprint.as_slice() == fingerprint
    {
        return Ok(true);
    }
    Err(ServerError::new(
        ErrorCode::Conflict,
        "catalog request id is already owned by another committed command",
    )
    .with_request_id(request_id)
    .with_retryable(false))
}

/// Returns whether a request id belongs to one format/operation namespace without comparing a
/// command fingerprint. This is only for fixed-cost replay probes that do not possess a command.
pub(crate) fn namespace_owner_exists(
    connection: &Connection,
    request_id: RequestId,
    format_version: i64,
    operation: &str,
) -> Result<bool> {
    let Some(owner) = load_owner(connection, request_id)? else {
        return Ok(false);
    };
    validate_owner(&owner)?;
    if owner.format_version == format_version && owner.operation == operation {
        Ok(true)
    } else {
        Err(ServerError::new(
            ErrorCode::Conflict,
            "catalog request id is already owned by another committed command",
        )
        .with_request_id(request_id)
        .with_retryable(false))
    }
}

/// Permanently records one request id after its version-specific fact row is prepared.
pub(crate) fn insert_owner(
    connection: &Connection,
    request_id: RequestId,
    format_version: i64,
    operation: &str,
    fingerprint: &[u8; 32],
    committed_at_ns: u64,
) -> Result<()> {
    if !(1..=4).contains(&format_version) || operation.is_empty() {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "invalid Catalog request owner attempted to enter the ledger",
        )
        .with_request_id(request_id));
    }
    connection
        .execute(
            "INSERT INTO catalog_request_ids(\
                 request_id, format_version, operation, fingerprint, committed_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                request_id.into_bytes().as_slice(),
                format_version,
                operation,
                fingerprint.as_slice(),
                encode_u64(committed_at_ns).as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("persist global Catalog request owner", error)
                .with_request_id(request_id)
        })?;
    Ok(())
}

fn load_owner(connection: &Connection, request_id: RequestId) -> Result<Option<RequestOwner>> {
    connection
        .query_row(
            "SELECT format_version, operation, fingerprint, committed_at_ns \
               FROM catalog_request_ids WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| {
                Ok(RequestOwner {
                    format_version: row.get(0)?,
                    operation: row.get(1)?,
                    fingerprint: row.get(2)?,
                    committed_at_ns: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read global Catalog request owner", error))
}

fn validate_owner(owner: &RequestOwner) -> Result<()> {
    if !(1..=4).contains(&owner.format_version)
        || owner.operation.is_empty()
        || owner.fingerprint.len() != 32
    {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "global Catalog request owner violates intrinsic invariants",
        ));
    }
    decode_u64(&owner.committed_at_ns)?;
    Ok(())
}
