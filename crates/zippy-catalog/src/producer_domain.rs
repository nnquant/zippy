//! Transactional producer fencing and durable checkpoint acquisition.

use rusqlite::{params, Connection, OptionalExtension};
use zippy_types::catalog::{AcquireProducerCommand, ProducerDescriptor};
use zippy_types::{
    ErrorCode, ProducerId, Result, SequenceWatermark, ServerError, ServerErrorDetails, TableId,
};

use crate::encoding::{
    decode_offset_watermark, decode_u64, encode_offset_watermark, encode_sequence_watermark,
    encode_u64,
};

/// A producer acquisition whose stable identifiers have passed domain preparation.
pub(crate) struct PreparedAcquireProducer {
    table_id: TableId,
    producer_id: ProducerId,
    expected_writer_epoch: Option<u64>,
}

/// Prepares one producer acquisition before the SQLite writer transaction begins.
pub(crate) fn prepare_acquire_producer(
    command: AcquireProducerCommand,
) -> Result<PreparedAcquireProducer> {
    Ok(PreparedAcquireProducer {
        table_id: command.table_id,
        producer_id: command.producer_id,
        expected_writer_epoch: command.expected_writer_epoch,
    })
}

/// Acquires the next table-wide fencing epoch and resets this producer's sequence watermark.
pub(crate) fn acquire_producer(
    connection: &Connection,
    prepared: PreparedAcquireProducer,
    updated_at_ns: u64,
) -> Result<ProducerDescriptor> {
    let table_id = prepared.table_id.into_bytes();
    let current = connection
        .query_row(
            "SELECT t.writer_epoch, t.state, s.committed_log_offset \
             FROM tables t \
             JOIN table_snapshots s \
               ON s.table_id = t.table_id \
              AND s.snapshot_version = t.current_snapshot_version \
             WHERE t.table_id = ?1",
            [table_id.as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read producer acquisition table", error))?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::NotFound,
                "catalog producer acquisition table was not found",
            )
            .with_table_id(prepared.table_id)
            .with_retryable(false)
        })?;
    if current.1 != "active" {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "catalog producer acquisition requires an active table state=[{}]",
                current.1
            ),
        )
        .with_table_id(prepared.table_id)
        .with_retryable(false));
    }
    let current_epoch = decode_u64(&current.0)?;
    if let Some(expected_epoch) = prepared.expected_writer_epoch {
        if expected_epoch != current_epoch {
            return Err(ServerError::new(
                ErrorCode::StaleProducerEpoch,
                format!(
                    "catalog producer epoch changed expected=[{current_epoch}] actual=[{expected_epoch}]"
                ),
            )
            .with_table_id(prepared.table_id)
            .with_retryable(false)
            .with_details(ServerErrorDetails::ProducerEpoch {
                expected: current_epoch,
                actual: expected_epoch,
            })
            .expect("producer epoch details match the stale epoch error code"));
        }
    }
    let last_committed_log_offset = decode_offset_watermark(&current.2)?;
    let next_epoch = current_epoch.checked_add(1).ok_or_else(|| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "catalog producer epoch domain is exhausted",
        )
        .with_table_id(prepared.table_id)
        .with_retryable(false)
    })?;

    let producer_id = prepared.producer_id.into_bytes();
    let existing_table = connection
        .query_row(
            "SELECT table_id FROM producers WHERE producer_id = ?1",
            [producer_id.as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read existing producer identity", error))?;
    if existing_table
        .as_deref()
        .is_some_and(|stored| stored != table_id)
    {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "catalog producer identity is already bound to another table",
        )
        .with_table_id(prepared.table_id)
        .with_retryable(false));
    }

    let next_epoch_encoded = encode_u64(next_epoch);
    let timestamp = encode_u64(updated_at_ns);
    let updated = connection
        .execute(
            "UPDATE tables \
             SET writer_epoch = ?1, updated_at_ns = ?2 \
             WHERE table_id = ?3 AND writer_epoch = ?4 AND state = 'active'",
            params![
                next_epoch_encoded.as_slice(),
                timestamp.as_slice(),
                table_id.as_slice(),
                current.0.as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("advance table producer epoch", error))?;
    if updated != 1 {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "catalog table producer epoch changed during acquisition",
        )
        .with_table_id(prepared.table_id)
        .with_retryable(true));
    }

    let empty_sequence = encode_sequence_watermark(SequenceWatermark::Empty);
    let committed_offset = encode_offset_watermark(last_committed_log_offset);
    connection
        .execute(
            "INSERT INTO producers(\
                 producer_id, table_id, producer_epoch, last_batch_sequence, \
                 last_committed_log_offset, updated_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(producer_id) DO UPDATE SET \
                 producer_epoch = excluded.producer_epoch, \
                 last_batch_sequence = excluded.last_batch_sequence, \
                 last_committed_log_offset = excluded.last_committed_log_offset, \
                 updated_at_ns = excluded.updated_at_ns",
            params![
                producer_id.as_slice(),
                table_id.as_slice(),
                next_epoch_encoded.as_slice(),
                empty_sequence.as_slice(),
                committed_offset.as_slice(),
                timestamp.as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("persist producer fencing checkpoint", error))?;

    Ok(ProducerDescriptor {
        producer_id: prepared.producer_id,
        table_id: prepared.table_id,
        producer_epoch: next_epoch,
        last_batch_sequence: SequenceWatermark::Empty,
        last_committed_log_offset,
    })
}
