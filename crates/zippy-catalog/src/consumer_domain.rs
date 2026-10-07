//! Durable consumer progress with table identity, schema fencing and atomic offset CAS.

use rusqlite::{params, Connection, OptionalExtension};
use zippy_types::catalog::{
    CatalogActorKind, CommitConsumerOffsetCommand, ConsumerOffsetSnapshot,
    ReadConsumerOffsetRequest,
};
use zippy_types::table::TableRef;
use zippy_types::{
    CommittedOffset, ConsumerOffsetAuthority, DurableConsumerId, ErrorCode, OffsetWatermark,
    Result, ServerError, TableId,
};

use crate::encoding::{
    decode_committed_offset, decode_offset_watermark, encode_committed_offset, encode_u64,
};

pub(crate) fn prepare_commit_consumer_offset(
    actor: CatalogActorKind,
    command: CommitConsumerOffsetCommand,
) -> Result<CommitConsumerOffsetCommand> {
    if actor != CatalogActorKind::Server {
        return Err(failure(
            ErrorCode::PermissionDenied,
            "consumer offset commits require the server subscription authority",
            command.table_id,
        ));
    }
    validate_identity(
        command.consumer_id,
        command.table_id,
        command.offset_authority,
    )?;
    if command.expected_schema_version == 0 {
        return Err(failure(
            ErrorCode::InvalidArgument,
            "consumer offset commit requires a nonzero schema version",
            command.table_id,
        ));
    }
    if position(command.committed_log_offset) < position(command.expected_committed_log_offset) {
        return Err(failure(
            ErrorCode::InvalidArgument,
            "consumer offset cannot move backwards",
            command.table_id,
        ));
    }
    Ok(command)
}

/// Advances a consumer position only within the native persisted snapshot watermark.
///
/// The caller owns the enclosing immediate transaction, including revision and request replay.
/// SubscriptionService must additionally fence connection ownership and validate delivery progress.
pub(crate) fn commit_consumer_offset(
    connection: &Connection,
    command: CommitConsumerOffsetCommand,
    committed_at_ns: u64,
) -> Result<CommittedOffset> {
    let current = read_consumer_offset(
        connection,
        ReadConsumerOffsetRequest {
            consumer_id: command.consumer_id,
            table_id: command.table_id,
            offset_authority: command.offset_authority,
        },
    )?;
    if current.schema_version != command.expected_schema_version {
        return Err(failure(
            ErrorCode::SchemaMismatch,
            "consumer acknowledgement belongs to an obsolete table schema",
            command.table_id,
        ));
    }
    if current.committed_log_offset != command.expected_committed_log_offset {
        return Err(failure(
            ErrorCode::Conflict,
            "consumer offset changed since it was observed",
            command.table_id,
        ));
    }
    if position(command.committed_log_offset) > position(current.persisted_log_offset) {
        return Err(failure(
            ErrorCode::InvalidArgument,
            "consumer acknowledgement exceeds the persisted table watermark",
            command.table_id,
        ));
    }
    let consumer = command.consumer_id.into_bytes();
    let table = command.table_id.into_bytes();
    let timestamp = encode_u64(committed_at_ns);
    connection
        .execute(
            "INSERT INTO durable_consumers(consumer_id, table_id, created_at_ns, updated_at_ns) \
         VALUES (?1, ?2, ?3, ?3) ON CONFLICT(consumer_id) DO UPDATE SET \
         updated_at_ns = excluded.updated_at_ns",
            params![consumer.as_slice(), table.as_slice(), timestamp.as_slice()],
        )
        .map_err(|error| crate::map_sqlite_error("persist durable consumer identity", error))?;
    let (kind, authority) = authority_key(command.offset_authority);
    connection
        .execute(
            "INSERT INTO durable_consumer_offsets(consumer_id, table_id, authority_kind, \
         authority_id, committed_log_offset, updated_at_ns) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(table_id, authority_kind, authority_id) DO UPDATE SET \
         consumer_id = excluded.consumer_id, \
         committed_log_offset = excluded.committed_log_offset, \
         updated_at_ns = excluded.updated_at_ns",
            params![
                consumer.as_slice(),
                table.as_slice(),
                kind,
                authority.as_slice(),
                encode_committed_offset(command.committed_log_offset).as_slice(),
                timestamp.as_slice()
            ],
        )
        .map_err(|error| crate::map_sqlite_error("persist durable consumer offset", error))?;
    Ok(command.committed_log_offset)
}

/// Reads a constant-size replay boundary inside the caller's consistent transaction.
pub(crate) fn read_consumer_offset(
    connection: &Connection,
    request: ReadConsumerOffsetRequest,
) -> Result<ConsumerOffsetSnapshot> {
    validate_identity(
        request.consumer_id,
        request.table_id,
        request.offset_authority,
    )?;
    let table = crate::table_domain::get_table(
        connection,
        TableRef::Id {
            table_id: request.table_id,
        },
    )?;
    let identity = table.table_id.into_bytes();
    if table.current_schema_version == 0 || table.current_snapshot_version == 0 {
        return Err(failure(
            ErrorCode::DataCorruption,
            "consumer table has an invalid current version",
            request.table_id,
        ));
    }
    check_consumer_binding(connection, request.consumer_id, request.table_id)?;
    let encoded: Option<Vec<u8>> = connection
        .query_row(
            "SELECT CASE WHEN typeof(committed_log_offset) = 'blob' \
         AND length(committed_log_offset) = 9 THEN committed_log_offset END \
         FROM table_snapshots WHERE table_id = ?1 AND snapshot_version = ?2",
            params![
                identity.as_slice(),
                encode_u64(table.current_snapshot_version).as_slice()
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read consumer table persisted watermark", error))?
        .flatten();
    let encoded = encoded.ok_or_else(|| {
        failure(
            ErrorCode::DataCorruption,
            "consumer table snapshot watermark is missing or malformed",
            request.table_id,
        )
    })?;
    let persisted = match decode_offset_watermark(&encoded)
        .map_err(|error| error.with_table_id(request.table_id))?
    {
        OffsetWatermark::Empty => CommittedOffset::Empty,
        OffsetWatermark::Known(offset) => CommittedOffset::Known(offset),
    };
    let (kind, authority) = authority_key(request.offset_authority);
    let stored = connection
        .query_row(
            "SELECT CASE WHEN typeof(o.committed_log_offset) = 'blob' \
         AND length(o.committed_log_offset) = 9 THEN o.committed_log_offset END, \
         CASE WHEN typeof(o.consumer_id) = 'blob' AND length(o.consumer_id) = 16 \
         THEN o.consumer_id END, \
         CASE WHEN typeof(c.table_id) = 'blob' AND length(c.table_id) = 16 THEN c.table_id END \
         FROM durable_consumer_offsets o LEFT JOIN durable_consumers c \
         ON c.consumer_id = o.consumer_id \
         WHERE o.table_id = ?1 AND o.authority_kind = ?2 AND o.authority_id = ?3",
            params![identity.as_slice(), kind, authority.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read durable consumer offset", error))?;
    let committed = match stored {
        None => CommittedOffset::Empty,
        Some((offset, consumer, bound_table)) => {
            let corrupt = || {
                failure(
                    ErrorCode::DataCorruption,
                    "durable consumer offset or identity binding is malformed",
                    request.table_id,
                )
            };
            let consumer = consumer.ok_or_else(corrupt)?;
            let consumer =
                DurableConsumerId::from_bytes(consumer.try_into().map_err(|_| corrupt())?)
                    .map_err(|_| corrupt())?;
            let wrong_consumer = matches!(request.offset_authority,
                ConsumerOffsetAuthority::Consumer(expected) if consumer != expected);
            if bound_table.as_deref() != Some(identity.as_slice()) || wrong_consumer {
                return Err(corrupt());
            }
            decode_committed_offset(&offset.ok_or_else(corrupt)?)
                .map_err(|error| error.with_table_id(request.table_id))?
        }
    };
    if position(committed) > position(persisted) {
        return Err(failure(
            ErrorCode::DataCorruption,
            "stored consumer offset exceeds the persisted table watermark",
            request.table_id,
        ));
    }
    Ok(ConsumerOffsetSnapshot {
        table_id: table.table_id,
        schema_version: table.current_schema_version,
        offset_authority: request.offset_authority,
        committed_log_offset: committed,
        persisted_log_offset: persisted,
    })
}

fn check_consumer_binding(
    connection: &Connection,
    consumer_id: DurableConsumerId,
    table_id: TableId,
) -> Result<()> {
    let existing = connection.query_row(
        "SELECT CASE WHEN typeof(table_id) = 'blob' AND length(table_id) = 16 THEN table_id END \
         FROM durable_consumers WHERE consumer_id = ?1",
        [consumer_id.into_bytes().as_slice()], |row| row.get::<_, Option<Vec<u8>>>(0),
    ).optional().map_err(|error| crate::map_sqlite_error("read durable consumer identity", error))?;
    if let Some(existing) = existing {
        let corrupt = || {
            failure(
                ErrorCode::DataCorruption,
                "durable consumer table identity is malformed",
                table_id,
            )
        };
        let bound = existing.ok_or_else(corrupt)?;
        let bound =
            TableId::from_bytes(bound.try_into().map_err(|_| corrupt())?).map_err(|_| corrupt())?;
        if bound != table_id {
            return Err(failure(
                ErrorCode::Conflict,
                "consumer identity is already bound to another table",
                table_id,
            ));
        }
    }
    Ok(())
}

fn validate_identity(
    consumer: DurableConsumerId,
    table: TableId,
    authority: ConsumerOffsetAuthority,
) -> Result<()> {
    if matches!(authority, ConsumerOffsetAuthority::Consumer(expected) if consumer != expected) {
        return Err(failure(
            ErrorCode::InvalidArgument,
            "individual offset authority must match consumer identity",
            table,
        ));
    }
    Ok(())
}

fn authority_key(authority: ConsumerOffsetAuthority) -> (&'static str, [u8; 16]) {
    match authority {
        ConsumerOffsetAuthority::Consumer(id) => ("consumer", id.into_bytes()),
        ConsumerOffsetAuthority::Group(id) => ("group", id.into_bytes()),
    }
}

fn position(offset: CommittedOffset) -> Option<u64> {
    match offset {
        CommittedOffset::Empty => None,
        CommittedOffset::Known(offset) => Some(offset.get()),
    }
}

fn failure(code: ErrorCode, message: &str, table_id: TableId) -> ServerError {
    ServerError::new(code, message)
        .with_table_id(table_id)
        .with_retryable(false)
}
