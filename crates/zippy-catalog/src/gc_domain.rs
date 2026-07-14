//! Atomic safe-GC file lifecycle transitions.

use rusqlite::{params, params_from_iter, Connection, OptionalExtension, TransactionBehavior};
use zippy_types::catalog::{
    ClaimGarbageCollectionFilesRequest, ClaimedGarbageCollectionFiles,
    FinalizeGarbageCollectionFilesRequest, FinalizedGarbageCollectionFiles,
};
use zippy_types::{ErrorCode, FileId, Result, ServerError, TableId};

use crate::encoding::{decode_u64, encode_u64};

const MAX_GARBAGE_COLLECTION_FILES: usize = 4_096;
const SQLITE_IN_CHUNK: usize = 512;

pub(crate) struct CommittedGarbageCollectionClaim {
    table_id: TableId,
    catalog_revision: u64,
    retry_file_ids: Vec<FileId>,
    new_file_ids: Vec<FileId>,
}

pub(crate) fn validate_claim_request(
    request: &ClaimGarbageCollectionFilesRequest,
    runtime_min_pinned_snapshot: Option<u64>,
) -> Result<()> {
    let retry_limit = usize::try_from(request.retry_limit).map_err(|_| {
        resource_exhausted("garbage collection retry limit exceeds platform capacity")
    })?;
    if retry_limit == 0 {
        return Err(invalid(
            "garbage collection retry limit must be greater than zero",
        ));
    }
    let new_claim_limit = usize::try_from(request.new_claim_limit).map_err(|_| {
        resource_exhausted("garbage collection new-claim limit exceeds platform capacity")
    })?;
    if new_claim_limit == 0 {
        return Err(invalid(
            "garbage collection new-claim limit must be greater than zero",
        ));
    }
    let total_limit = retry_limit.checked_add(new_claim_limit).ok_or_else(|| {
        resource_exhausted("garbage collection total claim limit exceeds platform capacity")
    })?;
    if total_limit > MAX_GARBAGE_COLLECTION_FILES {
        return Err(resource_exhausted(format!(
            "garbage collection total claim limit exceeds maximum actual=[{total_limit}] maximum=[{MAX_GARBAGE_COLLECTION_FILES}]"
        )));
    }
    if runtime_min_pinned_snapshot == Some(0) {
        return Err(invalid(
            "garbage collection runtime minimum pinned snapshot must be nonzero",
        ));
    }
    Ok(())
}

pub(crate) fn validate_finalize_request(
    request: &FinalizeGarbageCollectionFilesRequest,
) -> Result<()> {
    if request.file_ids.is_empty() {
        return Err(invalid(
            "garbage collection finalize file identities must not be empty",
        ));
    }
    if request.file_ids.len() > MAX_GARBAGE_COLLECTION_FILES {
        return Err(resource_exhausted(format!(
            "garbage collection finalize file count exceeds maximum actual=[{}] maximum=[{MAX_GARBAGE_COLLECTION_FILES}]",
            request.file_ids.len()
        )));
    }
    for pair in request.file_ids.windows(2) {
        match pair[0].cmp(&pair[1]) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                return Err(invalid(format!(
                    "garbage collection finalize contains duplicate file identity file_id=[{}]",
                    pair[0]
                )));
            }
            std::cmp::Ordering::Greater => {
                return Err(invalid(
                    "garbage collection finalize file identities must be strictly sorted",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn commit_garbage_collection_claim(
    connection: &mut Connection,
    request: ClaimGarbageCollectionFilesRequest,
    runtime_min_pinned_snapshot: Option<u64>,
) -> Result<CommittedGarbageCollectionClaim> {
    validate_claim_request(&request, runtime_min_pinned_snapshot)?;
    let now_ns = crate::domain::unix_time_ns()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| crate::map_sqlite_error("begin garbage collection claim", error))?;
    validate_runtime_pin(&transaction, &request, runtime_min_pinned_snapshot)?;
    let retry_file_ids = select_retry_candidates(&transaction, &request)?;
    let new_file_ids =
        select_new_candidates(&transaction, &request, runtime_min_pinned_snapshot, now_ns)?;
    let claimed = transition_candidates_to_deleting(&transaction, request.table_id, &new_file_ids)?;
    let catalog_revision = advance_revision_if_changed(&transaction, claimed)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit garbage collection claim", error))?;
    Ok(CommittedGarbageCollectionClaim {
        table_id: request.table_id,
        catalog_revision,
        retry_file_ids,
        new_file_ids,
    })
}

pub(crate) fn materialize_garbage_collection_claim(
    connection: &Connection,
    claim: CommittedGarbageCollectionClaim,
) -> Result<ClaimedGarbageCollectionFiles> {
    // Descriptor decoding can touch large metadata and lineage blobs. The Catalog actor remains
    // serial here, so materializing after commit preserves a stable deleting set without holding
    // SQLite's write lock for the expensive read and decode work.
    let mut files = crate::snapshot_domain::read_deleting_file_descriptors(
        connection,
        claim.table_id,
        &claim.retry_file_ids,
    )?;
    files.extend(crate::snapshot_domain::read_deleting_file_descriptors(
        connection,
        claim.table_id,
        &claim.new_file_ids,
    )?);
    Ok(ClaimedGarbageCollectionFiles {
        catalog_revision: claim.catalog_revision,
        files,
    })
}

pub(crate) fn finalize_garbage_collection_files(
    connection: &mut Connection,
    request: FinalizeGarbageCollectionFilesRequest,
) -> Result<FinalizedGarbageCollectionFiles> {
    validate_finalize_request(&request)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| crate::map_sqlite_error("begin garbage collection finalize", error))?;
    let deleting = validate_finalize_set(&transaction, &request)?;
    let finalized = transition_files_to_deleted(&transaction, request.table_id, &deleting)?;
    let catalog_revision = advance_revision_if_changed(&transaction, finalized)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit garbage collection finalize", error))?;
    Ok(FinalizedGarbageCollectionFiles {
        catalog_revision,
        file_ids: request.file_ids,
    })
}

fn validate_runtime_pin(
    connection: &Connection,
    request: &ClaimGarbageCollectionFilesRequest,
    runtime_min_pinned_snapshot: Option<u64>,
) -> Result<()> {
    let current = connection
        .query_row(
            "SELECT current_snapshot_version FROM tables WHERE table_id = ?1",
            [request.table_id.into_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("resolve garbage collection table", error))?;
    let Some(current) = current else {
        return Err(ServerError::new(
            ErrorCode::NotFound,
            "garbage collection table was not found",
        )
        .with_table_id(request.table_id)
        .with_retryable(false));
    };
    let current = decode_u64(&current)?;
    if let Some(pinned) = runtime_min_pinned_snapshot {
        if pinned > current {
            return Err(invalid(format!(
                "garbage collection runtime pin exceeds current snapshot pinned=[{pinned}] current=[{current}]"
            ))
            .with_table_id(request.table_id));
        }
    }
    Ok(())
}

fn select_retry_candidates(
    connection: &Connection,
    request: &ClaimGarbageCollectionFilesRequest,
) -> Result<Vec<FileId>> {
    let encoded_table_id = request.table_id.into_bytes();
    let mut statement = connection
        .prepare(
            "SELECT file_id FROM data_files \
              WHERE table_id = ?1 \
                AND state = 'deleting' \
                AND location_kind = 'managed' \
                AND removed_snapshot_version_value IS NOT NULL \
              ORDER BY file_id \
              LIMIT ?2",
        )
        .map_err(|error| {
            crate::map_sqlite_error("prepare garbage collection retry selection", error)
        })?;
    let mut rows = statement
        .query(params![
            encoded_table_id.as_slice(),
            i64::from(request.retry_limit)
        ])
        .map_err(|error| {
            crate::map_sqlite_error("query garbage collection retry selection", error)
        })?;
    let mut file_ids = Vec::new();
    while let Some(row) = rows.next().map_err(|error| {
        crate::map_sqlite_error("read garbage collection retry candidate", error)
    })? {
        let file_id = decode_file_id(row.get(0).map_err(|error| {
            crate::map_sqlite_error("decode garbage collection retry identity", error)
        })?)?;
        if file_ids.last().is_some_and(|previous| *previous >= file_id) {
            return Err(corruption(
                "garbage collection retry candidates are not strictly file-id ordered",
            ));
        }
        file_ids.push(file_id);
    }
    Ok(file_ids)
}

fn select_new_candidates(
    connection: &Connection,
    request: &ClaimGarbageCollectionFilesRequest,
    runtime_min_pinned_snapshot: Option<u64>,
    now_ns: u64,
) -> Result<Vec<FileId>> {
    let encoded_table_id = request.table_id.into_bytes();
    let encoded_now = encode_u64(now_ns);
    let encoded_runtime_pin = runtime_min_pinned_snapshot.map(encode_u64);
    let runtime_pin_predicate = if encoded_runtime_pin.is_some() {
        "AND files.removed_snapshot_version_value <= ?3"
    } else {
        ""
    };
    let sql = format!(
        "SELECT files.file_id \
           FROM data_files AS files \
          WHERE files.table_id = ?1 \
            AND files.state = 'committed' \
            AND files.location_kind = 'managed' \
            AND files.removed_snapshot_version_value IS NOT NULL \
            {runtime_pin_predicate} \
            AND NOT EXISTS ( \
                SELECT 1 FROM snapshot_leases AS leases \
                    INDEXED BY snapshot_leases_gc_by_table \
                 WHERE leases.table_id = files.table_id \
                   AND leases.snapshot_version >= files.added_snapshot_version \
                   AND leases.snapshot_version < files.removed_snapshot_version_value \
                   AND leases.expires_at_ns > ?2 \
            ) \
          ORDER BY files.removed_snapshot_version_value, files.file_id \
          LIMIT ?4"
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| crate::map_sqlite_error("prepare garbage collection claim", error))?;
    let mut rows = statement
        .query(params![
            encoded_table_id.as_slice(),
            encoded_now.as_slice(),
            encoded_runtime_pin.as_ref().map(<[u8; 8]>::as_slice),
            i64::from(request.new_claim_limit),
        ])
        .map_err(|error| crate::map_sqlite_error("query garbage collection claim", error))?;
    let mut file_ids = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read garbage collection candidate", error))?
    {
        let file_id = decode_file_id(row.get(0).map_err(|error| {
            crate::map_sqlite_error("decode garbage collection file identity", error)
        })?)?;
        file_ids.push(file_id);
    }
    // Selection is oldest-removal-first so a large table cannot permanently strand old files.
    // Descriptor reconstruction requires and exposes a strict file-id order within this segment.
    file_ids.sort_unstable();
    if file_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(corruption(
            "garbage collection new candidates are not unique file identities",
        ));
    }
    Ok(file_ids)
}

fn transition_candidates_to_deleting(
    connection: &Connection,
    table_id: TableId,
    file_ids: &[FileId],
) -> Result<usize> {
    let encoded_table_id = table_id.into_bytes();
    let mut statement = connection
        .prepare_cached(
            "UPDATE data_files SET state = 'deleting' \
              WHERE table_id = ?1 AND file_id = ?2 AND state = 'committed' \
                AND location_kind = 'managed' \
                AND removed_snapshot_version_value IS NOT NULL",
        )
        .map_err(|error| {
            crate::map_sqlite_error("prepare garbage collection lifecycle claim", error)
        })?;
    let mut claimed = 0_usize;
    for file_id in file_ids {
        let encoded_file_id = file_id.into_bytes();
        let updated = statement
            .execute(params![
                encoded_table_id.as_slice(),
                encoded_file_id.as_slice()
            ])
            .map_err(|error| crate::map_sqlite_error("claim garbage collection file", error))?;
        if updated != 1 {
            return Err(conflict(format!(
                "garbage collection candidate changed before claim file_id=[{}]",
                file_id
            )));
        }
        claimed = claimed
            .checked_add(1)
            .ok_or_else(|| corruption("garbage collection claimed file count overflow"))?;
    }
    Ok(claimed)
}

fn validate_finalize_set(
    connection: &Connection,
    request: &FinalizeGarbageCollectionFilesRequest,
) -> Result<Vec<FileId>> {
    let encoded_table_id = request.table_id.into_bytes();
    let mut deleting = Vec::new();
    for chunk in request.file_ids.chunks(SQLITE_IN_CHUNK) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT file_id, state, location_kind FROM data_files \
              WHERE table_id = ? AND file_id IN ({placeholders}) ORDER BY file_id"
        );
        let encoded_ids = chunk
            .iter()
            .map(|file_id| file_id.into_bytes())
            .collect::<Vec<_>>();
        let parameters = std::iter::once(encoded_table_id.as_slice())
            .chain(encoded_ids.iter().map(|file_id| file_id.as_slice()));
        let mut statement = connection.prepare(&sql).map_err(|error| {
            crate::map_sqlite_error("prepare garbage collection finalize set", error)
        })?;
        let mut rows = statement
            .query(params_from_iter(parameters))
            .map_err(|error| {
                crate::map_sqlite_error("query garbage collection finalize set", error)
            })?;
        let mut actual = Vec::with_capacity(chunk.len());
        while let Some(row) = rows.next().map_err(|error| {
            crate::map_sqlite_error("read garbage collection finalize set", error)
        })? {
            let file_id = decode_file_id(row.get(0).map_err(|error| {
                crate::map_sqlite_error("decode finalized file identity", error)
            })?)?;
            let state: String = row
                .get(1)
                .map_err(|error| crate::map_sqlite_error("decode finalized file state", error))?;
            let location_kind: String = row.get(2).map_err(|error| {
                crate::map_sqlite_error("decode finalized file location", error)
            })?;
            if location_kind != "managed" {
                return Err(conflict(format!(
                    "garbage collection cannot finalize an external file file_id=[{file_id}]"
                )));
            }
            match state.as_str() {
                "deleting" => deleting.push(file_id),
                "deleted" => {}
                _ => {
                    return Err(conflict(format!(
                        "garbage collection file is not deleting file_id=[{file_id}] state=[{state}]"
                    )));
                }
            }
            actual.push(file_id);
        }
        if actual.as_slice() != chunk {
            return Err(conflict(
                "garbage collection finalize set does not exactly match Catalog rows",
            ));
        }
    }
    Ok(deleting)
}

fn transition_files_to_deleted(
    connection: &Connection,
    table_id: TableId,
    file_ids: &[FileId],
) -> Result<usize> {
    let encoded_table_id = table_id.into_bytes();
    let mut statement = connection
        .prepare_cached(
            "UPDATE data_files SET state = 'deleted' \
              WHERE table_id = ?1 AND file_id = ?2 AND state = 'deleting' \
                AND location_kind = 'managed'",
        )
        .map_err(|error| {
            crate::map_sqlite_error("prepare garbage collection finalize transition", error)
        })?;
    for file_id in file_ids {
        let encoded_file_id = file_id.into_bytes();
        let updated = statement
            .execute(params![
                encoded_table_id.as_slice(),
                encoded_file_id.as_slice()
            ])
            .map_err(|error| crate::map_sqlite_error("finalize garbage collection file", error))?;
        if updated != 1 {
            return Err(conflict(format!(
                "garbage collection file changed before finalize file_id=[{file_id}]"
            )));
        }
    }
    Ok(file_ids.len())
}

fn advance_revision_if_changed(connection: &Connection, changed: usize) -> Result<u64> {
    let current = crate::domain::revision(connection)?;
    if changed == 0 {
        return Ok(current);
    }
    let next = current.checked_add(1).ok_or_else(|| {
        ServerError::new(
            ErrorCode::Internal,
            "catalog revision overflow during garbage collection",
        )
        .with_retryable(false)
    })?;
    crate::domain::update_revision(connection, next)?;
    Ok(next)
}

fn decode_file_id(bytes: Vec<u8>) -> Result<FileId> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        corruption(format!(
            "garbage collection file identity has invalid width actual=[{}]",
            bytes.len()
        ))
    })?;
    FileId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "garbage collection file identity is invalid error=[{error}]"
        ))
    })
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn resource_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn conflict(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Conflict, message).with_retryable(false)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use zippy_types::catalog::{
        ClaimGarbageCollectionFilesRequest, FinalizeGarbageCollectionFilesRequest,
    };
    use zippy_types::{ErrorCode, FileId, TableId};

    use super::{validate_claim_request, validate_finalize_request};

    fn table_id() -> TableId {
        TableId::from_u128(1).expect("nonzero table id")
    }

    fn file_id(value: u128) -> FileId {
        FileId::from_u128(value).expect("nonzero file id")
    }

    #[test]
    fn claim_validation_rejects_unbounded_or_invalid_boundaries() {
        let mut request = ClaimGarbageCollectionFilesRequest {
            table_id: table_id(),
            retry_limit: 1,
            new_claim_limit: 1,
        };
        assert!(validate_claim_request(&request, None).is_ok());

        request.retry_limit = 0;
        assert_eq!(
            validate_claim_request(&request, None)
                .expect_err("zero retry limit must fail")
                .code(),
            ErrorCode::InvalidArgument
        );
        request.retry_limit = 1;
        request.new_claim_limit = 0;
        assert_eq!(
            validate_claim_request(&request, None)
                .expect_err("zero new-claim limit must fail")
                .code(),
            ErrorCode::InvalidArgument
        );
        request.retry_limit = 2_048;
        request.new_claim_limit = 2_049;
        assert_eq!(
            validate_claim_request(&request, None)
                .expect_err("oversized total limit must fail")
                .code(),
            ErrorCode::ResourceExhausted
        );
        request.retry_limit = 1;
        request.new_claim_limit = 1;
        assert_eq!(
            validate_claim_request(&request, Some(0))
                .expect_err("zero runtime pin must fail")
                .code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn finalize_validation_requires_a_bounded_strictly_sorted_set() {
        let mut request = FinalizeGarbageCollectionFilesRequest {
            table_id: table_id(),
            file_ids: Vec::new(),
        };
        assert_eq!(
            validate_finalize_request(&request)
                .expect_err("empty finalize set must fail")
                .code(),
            ErrorCode::InvalidArgument
        );

        request.file_ids = vec![file_id(2), file_id(1)];
        assert_eq!(
            validate_finalize_request(&request)
                .expect_err("unsorted finalize set must fail")
                .code(),
            ErrorCode::InvalidArgument
        );
        request.file_ids = vec![file_id(1), file_id(1)];
        assert_eq!(
            validate_finalize_request(&request)
                .expect_err("duplicate finalize identity must fail")
                .code(),
            ErrorCode::InvalidArgument
        );
        request.file_ids = (1_u128..=4_097).map(file_id).collect();
        assert_eq!(
            validate_finalize_request(&request)
                .expect_err("oversized finalize set must fail")
                .code(),
            ErrorCode::ResourceExhausted
        );
    }
}
