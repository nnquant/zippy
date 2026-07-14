//! Durable snapshot-lease command validation and SQLite mutations.

use rusqlite::{params, Connection, Error as SqliteError, OptionalExtension};
use zippy_types::catalog::{
    AcquireSnapshotLeaseCommand, ReleaseSnapshotLeaseCommand, SnapshotLeaseDescriptor,
};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::encoding::encode_u64;

const MAX_SNAPSHOT_LEASE_OWNER_BYTES: usize = 4 * 1024;

pub(crate) struct PreparedAcquireSnapshotLease {
    command: AcquireSnapshotLeaseCommand,
}

pub(crate) struct PreparedReleaseSnapshotLease {
    command: ReleaseSnapshotLeaseCommand,
}

pub(crate) fn prepare_acquire_snapshot_lease(
    command: AcquireSnapshotLeaseCommand,
) -> Result<PreparedAcquireSnapshotLease> {
    validate_snapshot_version(command.snapshot_version)?;
    validate_owner(&command.owner)?;
    if command.expires_at_ns == 0 {
        return Err(invalid("snapshot lease expiration must be nonzero"));
    }
    Ok(PreparedAcquireSnapshotLease { command })
}

pub(crate) fn prepare_release_snapshot_lease(
    command: ReleaseSnapshotLeaseCommand,
) -> Result<PreparedReleaseSnapshotLease> {
    validate_owner(&command.owner)?;
    Ok(PreparedReleaseSnapshotLease { command })
}

pub(crate) fn acquire_snapshot_lease(
    connection: &Connection,
    prepared: PreparedAcquireSnapshotLease,
    committed_at_ns: u64,
) -> Result<SnapshotLeaseDescriptor> {
    let command = prepared.command;
    if command.expires_at_ns <= committed_at_ns {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "snapshot lease expiration must be later than Catalog commit time",
        )
        .with_table_id(command.table_id)
        .with_retryable(false));
    }
    let table_state = connection
        .query_row(
            "SELECT tables.state FROM table_snapshots \
             JOIN tables ON tables.table_id = table_snapshots.table_id \
             WHERE table_snapshots.table_id = ?1 \
               AND table_snapshots.snapshot_version = ?2",
            params![
                command.table_id.into_bytes().as_slice(),
                encode_u64(command.snapshot_version).as_slice(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("resolve snapshot lease target", error))?;
    let Some(table_state) = table_state else {
        return Err(ServerError::new(
            ErrorCode::NotFound,
            "snapshot lease target snapshot was not found",
        )
        .with_table_id(command.table_id)
        .with_retryable(false));
    };
    if table_state != "active" {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!("snapshot lease target table is not active state=[{table_state}]"),
        )
        .with_table_id(command.table_id)
        .with_retryable(false));
    }

    let incomplete: i64 = connection
        .query_row(
            "SELECT EXISTS( \
                 SELECT 1 FROM data_files \
                  WHERE table_id = ?1 \
                    AND state IN ('deleting', 'deleted') \
                    AND added_snapshot_version <= ?2 \
                    AND removed_snapshot_version_value > ?2 \
             )",
            params![
                command.table_id.into_bytes().as_slice(),
                encode_u64(command.snapshot_version).as_slice(),
            ],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("check snapshot lease completeness", error))?;
    if incomplete != 0 {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "snapshot lease target is incomplete because a visible file entered deletion",
        )
        .with_table_id(command.table_id)
        .with_retryable(false));
    }

    connection
        .execute(
            "INSERT INTO snapshot_leases(\
                 lease_id, table_id, snapshot_version, owner, expires_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                command.lease_id.into_bytes().as_slice(),
                command.table_id.into_bytes().as_slice(),
                encode_u64(command.snapshot_version).as_slice(),
                command.owner.as_str(),
                encode_u64(command.expires_at_ns).as_slice(),
            ],
        )
        .map_err(|error| map_acquire_error(error, command.table_id))?;
    Ok(SnapshotLeaseDescriptor {
        lease_id: command.lease_id,
        table_id: command.table_id,
        snapshot_version: command.snapshot_version,
        owner: command.owner,
        expires_at_ns: command.expires_at_ns,
    })
}

pub(crate) fn release_snapshot_lease(
    connection: &Connection,
    prepared: PreparedReleaseSnapshotLease,
) -> Result<zippy_types::LeaseId> {
    let command = prepared.command;
    let current_owner = connection
        .query_row(
            "SELECT owner FROM snapshot_leases WHERE lease_id = ?1",
            [command.lease_id.into_bytes().as_slice()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read snapshot lease owner", error))?;
    let Some(current_owner) = current_owner else {
        return Err(
            ServerError::new(ErrorCode::NotFound, "snapshot lease was not found")
                .with_retryable(false),
        );
    };
    if current_owner != command.owner {
        return Err(ServerError::new(
            ErrorCode::PermissionDenied,
            "snapshot lease owner does not match release principal",
        )
        .with_retryable(false));
    }
    let deleted = connection
        .execute(
            "DELETE FROM snapshot_leases WHERE lease_id = ?1 AND owner = ?2",
            params![command.lease_id.into_bytes().as_slice(), command.owner],
        )
        .map_err(|error| crate::map_sqlite_error("release snapshot lease", error))?;
    if deleted != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("snapshot lease release affected unexpected rows actual=[{deleted}]"),
        )
        .with_retryable(false));
    }
    Ok(command.lease_id)
}

fn validate_snapshot_version(snapshot_version: u64) -> Result<()> {
    if snapshot_version == 0 {
        Err(invalid("snapshot lease version must be nonzero"))
    } else {
        Ok(())
    }
}

fn validate_owner(owner: &str) -> Result<()> {
    let bytes = owner.len();
    if owner.trim().is_empty() {
        return Err(invalid("snapshot lease owner must not be empty"));
    }
    if bytes > MAX_SNAPSHOT_LEASE_OWNER_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "snapshot lease owner exceeds byte limit actual=[{bytes}] maximum=[{MAX_SNAPSHOT_LEASE_OWNER_BYTES}]"
            ),
        )
        .with_retryable(false));
    }
    Ok(())
}

fn map_acquire_error(error: SqliteError, table_id: zippy_types::TableId) -> ServerError {
    if error.sqlite_error_code() == Some(rusqlite::ffi::ErrorCode::ConstraintViolation) {
        return ServerError::new(
            ErrorCode::AlreadyExists,
            "snapshot lease identity already exists",
        )
        .with_table_id(table_id)
        .with_retryable(false);
    }
    crate::map_sqlite_error("acquire snapshot lease", error).with_table_id(table_id)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}
