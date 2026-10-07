//! Transactional native table tombstones and snapshot-based file retirement.

use rusqlite::{params, Connection};
use zippy_types::catalog::{CatalogActorKind, DropTableCommand};
use zippy_types::table::{MetadataMap, MetadataScalar, TableRef};
use zippy_types::{ErrorCode, Result, ServerError, TableId};

use crate::encoding::{encode_optional_u64, encode_u64};

pub(crate) fn prepare_drop_table(
    actor: CatalogActorKind,
    command: DropTableCommand,
) -> Result<DropTableCommand> {
    if !matches!(actor, CatalogActorKind::User | CatalogActorKind::Server) {
        return Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "table deletion requires a user or server authority",
        )
        .with_retryable(false));
    }
    if command.expected_snapshot_version == 0 {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "table deletion requires a nonzero expected snapshot version",
        )
        .with_retryable(false));
    }
    Ok(command)
}

/// Retires the entire visible file set without deleting files or historical identities.
///
/// The caller owns the enclosing Catalog write transaction. Runtime write admission must
/// be fenced by Server before exposing this primitive as an online management operation.
pub(crate) fn drop_table(
    connection: &Connection,
    command: DropTableCommand,
    next_revision: u64,
    committed_at_ns: u64,
) -> Result<TableId> {
    let table = crate::table_domain::get_table(
        connection,
        TableRef::Id {
            table_id: command.table_id,
        },
    )?;
    if table.current_snapshot_version != command.expected_snapshot_version {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "table deletion snapshot changed since the caller observed it",
        )
        .with_table_id(table.table_id)
        .with_retryable(false));
    }
    let next_snapshot = table
        .current_snapshot_version
        .checked_add(1)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "table snapshot version exhausted",
            )
            .with_table_id(table.table_id)
            .with_retryable(false)
        })?;
    let next_epoch = table.writer_epoch.checked_add(1).ok_or_else(|| {
        ServerError::new(ErrorCode::ResourceExhausted, "table writer epoch exhausted")
            .with_table_id(table.table_id)
            .with_retryable(false)
    })?;
    let identity = table.table_id.into_bytes();
    let mut summary = MetadataMap::new();
    summary.insert(
        "reason".to_owned(),
        MetadataScalar::String("table_drop".to_owned()),
    );
    let summary = crate::storage_codec::encode_snapshot_summary_blob(&summary)?;
    // An empty retention snapshot is the exact file visibility boundary. Historical
    // snapshots and both durable leases and in-process pins continue protecting files.
    let inserted = connection
        .execute(
            "INSERT INTO table_snapshots(\
             table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
             committed_log_offset, created_at_ns, summary_blob\
         ) SELECT table_id, ?2, ?3, 'retention', ?4, committed_log_offset, ?5, ?6 \
             FROM table_snapshots WHERE table_id = ?1 AND snapshot_version = ?7",
            params![
                identity.as_slice(),
                encode_u64(next_snapshot).as_slice(),
                encode_optional_u64(Some(table.current_snapshot_version)).as_slice(),
                encode_u64(next_revision).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                summary,
                encode_u64(table.current_snapshot_version).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert table drop retention snapshot", error))?;
    if inserted != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "table deletion cannot find its current snapshot",
        ));
    }
    connection
        .execute(
            "UPDATE data_files SET removed_snapshot_version = ?2 \
         WHERE table_id = ?1 AND state = 'committed' AND removed_snapshot_version_value IS NULL",
            params![
                identity.as_slice(),
                encode_optional_u64(Some(next_snapshot)).as_slice()
            ],
        )
        .map_err(|error| crate::map_sqlite_error("retire dropped table files", error))?;
    let updated = connection
        .execute(
            "UPDATE tables SET state = 'dropped', current_snapshot_version = ?2, \
             writer_epoch = ?3, updated_at_ns = ?4 \
         WHERE table_id = ?1 AND state = 'active' AND current_snapshot_version = ?5",
            params![
                identity.as_slice(),
                encode_u64(next_snapshot).as_slice(),
                encode_u64(next_epoch).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                encode_u64(command.expected_snapshot_version).as_slice()
            ],
        )
        .map_err(|error| crate::map_sqlite_error("commit table tombstone", error))?;
    if updated != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "table deletion affected an unexpected row count",
        ));
    }
    Ok(table.table_id)
}
