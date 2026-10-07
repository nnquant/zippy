//! Bounded Arrow Flight `DoPut` descriptors and native ingest receipts.

use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::FlightDescriptor;
use bytes::Bytes;
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use zippy_types::ingest::{Durability, WriteReceipt};
use zippy_types::{
    ConnectionId, ErrorCode, LogOffset, OffsetRange, OffsetWatermark, ProducerId, RequestId,
    Result, ServerError, SessionId,
};

use crate::error::watermark_to_wire;
use crate::query::STABLE_ID_BYTES;
use crate::wire::{PutBatchResult, PutCommand, PutDurability, PutMutation};
use crate::CURRENT_PROTOCOL_VERSION;

/// Maximum serialized `PutCommand` accepted by protocol version 1.
pub const MAX_PUT_COMMAND_BYTES: usize = 64 * 1024;
/// Maximum table-name size accepted by the Flight write descriptor.
pub const MAX_PUT_STREAM_NAME_BYTES: usize = 512;
/// Maximum configurable concurrent `DoPut` streams.
pub const MAX_CONCURRENT_PUTS: usize = 1_024;
/// Maximum configurable record batches consumed by one `DoPut` stream.
pub const MAX_BATCHES_PER_PUT: usize = 65_536;
/// Maximum configurable rows in one decoded batch or complete `DoPut` stream.
pub const MAX_ROWS_PER_PUT: u64 = 1_000_000_000;
/// Maximum configurable decoded bytes in one batch or complete `DoPut` stream.
pub const MAX_BYTES_PER_PUT: usize = 4 * 1024 * 1024 * 1024;

/// Validated typed mutation command for one `DoPut` stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightPutCommand {
    /// Server-issued writer session bound to the request metadata.
    pub session_id: SessionId,
    /// Client-stable identity for this stream attempt and its batch receipts.
    pub put_id: RequestId,
    /// Logical table name resolved by the authoritative Catalog.
    pub stream_name: String,
    /// Optional immutable identity guarding every stream open against name reuse.
    pub expected_table_id: Option<zippy_types::TableId>,
    /// Explicit Catalog namespace; empty names are rejected.
    pub namespace: String,
    /// Sequence assigned to the first decoded record batch.
    pub first_batch_sequence: u64,
    /// Durable producer identity shared by reconnecting writer attempts.
    pub producer_id: ProducerId,
    /// Boot-scoped connection identity for this `DoPut` writer lease.
    pub connection_id: ConnectionId,
    /// Previously observed fencing epoch, when supplied by the client.
    pub expected_writer_epoch: Option<u64>,
    /// Durability boundary requested for every batch in this stream.
    pub durability: Durability,
    /// Explicit table mutation; the backend rejects mismatched table definitions.
    pub mutation: PutMutation,
}

/// One decoded batch admitted to the configured Flight ingest frontend.
#[derive(Debug)]
pub struct FlightPutBatch {
    /// Validated command shared by this stream.
    pub command: Arc<FlightPutCommand>,
    /// Monotonic connection-local batch sequence.
    pub batch_sequence: u64,
    /// Decoded Arrow batch whose buffers remain owned until acknowledgement.
    pub batch: RecordBatch,
}

/// Bounded admission policy for one Flight `DoPut` service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightPutLimits {
    max_concurrent_puts: usize,
    max_batches_per_put: usize,
    max_rows_per_batch: u64,
    max_rows_per_put: u64,
    max_batch_bytes: usize,
    max_bytes_per_put: usize,
}

impl FlightPutLimits {
    /// Creates a validated set of concurrency, row, and byte limits.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_concurrent_puts: usize,
        max_batches_per_put: usize,
        max_rows_per_batch: u64,
        max_rows_per_put: u64,
        max_batch_bytes: usize,
        max_bytes_per_put: usize,
    ) -> Result<Self> {
        if !(1..=MAX_CONCURRENT_PUTS).contains(&max_concurrent_puts) {
            return Err(invalid(format!(
                "Flight ingest concurrent puts must be between 1 and {MAX_CONCURRENT_PUTS}"
            )));
        }
        if !(1..=MAX_BATCHES_PER_PUT).contains(&max_batches_per_put) {
            return Err(invalid(format!(
                "Flight ingest batches per put must be between 1 and {MAX_BATCHES_PER_PUT}"
            )));
        }
        if max_rows_per_batch == 0
            || max_rows_per_put == 0
            || max_rows_per_batch > max_rows_per_put
            || max_rows_per_put > MAX_ROWS_PER_PUT
        {
            return Err(invalid(
                "Flight ingest row limits are zero, inverted, or exceed the protocol ceiling",
            ));
        }
        if max_batch_bytes == 0
            || max_bytes_per_put == 0
            || max_batch_bytes > max_bytes_per_put
            || max_bytes_per_put > MAX_BYTES_PER_PUT
        {
            return Err(invalid(
                "Flight ingest byte limits are zero, inverted, or exceed the protocol ceiling",
            ));
        }
        Ok(Self {
            max_concurrent_puts,
            max_batches_per_put,
            max_rows_per_batch,
            max_rows_per_put,
            max_batch_bytes,
            max_bytes_per_put,
        })
    }

    /// Returns the maximum simultaneously active `DoPut` streams.
    #[must_use]
    pub const fn max_concurrent_puts(self) -> usize {
        self.max_concurrent_puts
    }

    /// Returns the maximum decoded batches in one stream.
    #[must_use]
    pub const fn max_batches_per_put(self) -> usize {
        self.max_batches_per_put
    }

    /// Returns the maximum rows in one decoded batch.
    #[must_use]
    pub const fn max_rows_per_batch(self) -> u64 {
        self.max_rows_per_batch
    }

    /// Returns the maximum cumulative rows in one stream.
    #[must_use]
    pub const fn max_rows_per_put(self) -> u64 {
        self.max_rows_per_put
    }

    /// Returns the maximum retained Arrow bytes in one decoded batch.
    #[must_use]
    pub const fn max_batch_bytes(self) -> usize {
        self.max_batch_bytes
    }

    /// Returns the maximum cumulative decoded bytes in one stream.
    #[must_use]
    pub const fn max_bytes_per_put(self) -> usize {
        self.max_bytes_per_put
    }
}

/// Encodes a validated wire command as an Arrow Flight command descriptor.
#[must_use]
pub fn encode_put_descriptor(command: &PutCommand) -> FlightDescriptor {
    FlightDescriptor::new_cmd(command.encode_to_vec())
}

/// Decodes and validates the command descriptor attached to the first schema message.
pub fn decode_put_descriptor(descriptor: &FlightDescriptor) -> Result<FlightPutCommand> {
    if DescriptorType::try_from(descriptor.r#type).ok() != Some(DescriptorType::Cmd)
        || !descriptor.path.is_empty()
    {
        return Err(invalid(
            "Flight ingest descriptor must be a command with an empty path",
        ));
    }
    if descriptor.cmd.is_empty() || descriptor.cmd.len() > MAX_PUT_COMMAND_BYTES {
        return Err(invalid(
            "Flight ingest command size is outside the protocol limit",
        ));
    }
    let command = PutCommand::decode(descriptor.cmd.clone())
        .map_err(|_| invalid("Flight ingest command protobuf is malformed"))?;
    decode_put_command(command)
}

/// Decodes and validates an already parsed protocol command.
pub fn decode_put_command(command: PutCommand) -> Result<FlightPutCommand> {
    if command.protocol_version != CURRENT_PROTOCOL_VERSION {
        return Err(invalid("Flight ingest protocol version is unsupported"));
    }
    let session_id = decode_id(&command.session_id, SessionId::from_bytes, "session_id")?;
    let put_id = decode_id(&command.put_id, RequestId::from_bytes, "put_id")?;
    if command.stream_name.trim().is_empty()
        || command.stream_name.len() > MAX_PUT_STREAM_NAME_BYTES
    {
        return Err(
            invalid("Flight ingest stream name is empty or too large").with_request_id(put_id)
        );
    }
    let mutation = match PutMutation::try_from(command.mutation).ok() {
        Some(
            mutation @ (PutMutation::Append
            | PutMutation::Frame
            | PutMutation::KeyValueUpsert
            | PutMutation::KeyValueDelete),
        ) => mutation,
        _ => return Err(unsupported("unsupported Flight mutation").with_request_id(put_id)),
    };
    let durability = match PutDurability::try_from(command.durability).ok() {
        Some(PutDurability::Memory) => Durability::Memory,
        Some(PutDurability::Wal) => Durability::Wal,
        Some(PutDurability::Persisted) => Durability::Persisted,
        Some(PutDurability::Unspecified) | None => {
            return Err(unsupported(
                "Flight ingest requires explicit memory, WAL or persisted durability",
            )
            .with_request_id(put_id));
        }
    };
    zippy_types::table::CatalogName::new(command.namespace.clone())
        .map_err(|error| invalid(format!("invalid Flight ingest namespace error=[{error}]")))?;
    let producer_id = decode_id(&command.producer_id, ProducerId::from_bytes, "producer_id")?;
    let connection_id = decode_id(
        &command.connection_id,
        ConnectionId::from_bytes,
        "connection_id",
    )?;
    Ok(FlightPutCommand {
        session_id,
        put_id,
        expected_table_id: command
            .expected_table_id
            .as_deref()
            .map(|value| decode_id(value, zippy_types::TableId::from_bytes, "expected_table_id"))
            .transpose()?,
        stream_name: command.stream_name,
        namespace: command.namespace,
        first_batch_sequence: command.first_batch_sequence,
        producer_id,
        connection_id,
        expected_writer_epoch: command.expected_writer_epoch,
        durability,
        mutation,
    })
}

/// Derives the native idempotency key for one zero-based batch position in a `DoPut` stream.
#[must_use]
pub fn put_batch_request_id(put_id: RequestId, batch_index: u64) -> RequestId {
    let mut hasher = Sha256::new();
    hasher.update(b"zippy-flight-put-batch-v1\0");
    hasher.update(put_id.into_bytes());
    hasher.update(batch_index.to_le_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; STABLE_ID_BYTES];
    bytes.copy_from_slice(&digest[..STABLE_ID_BYTES]);
    if bytes.iter().all(|value| *value == 0) {
        bytes[STABLE_ID_BYTES - 1] = 1;
    }
    RequestId::from_bytes(bytes).expect("SHA-256-derived request identity is nonzero")
}

/// Builds a deterministic memory-durability result for protocol fixtures.
#[must_use]
pub fn put_batch_result(
    command: &FlightPutCommand,
    batch_sequence: u64,
    row_count: u64,
) -> PutBatchResult {
    let batch_index = batch_sequence.saturating_sub(command.first_batch_sequence);
    let (first_log_offset, last_log_offset, committed_log_offset) = if row_count == 0 {
        (None, None, OffsetWatermark::Empty)
    } else {
        (
            Some(0),
            Some(row_count - 1),
            OffsetWatermark::Known(zippy_types::LogOffset::new(row_count - 1)),
        )
    };
    PutBatchResult {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        put_id: Bytes::copy_from_slice(&command.put_id.into_bytes()),
        batch_sequence,
        row_count,
        durability_reached: PutDurability::Memory as i32,
        request_id: Bytes::copy_from_slice(
            &put_batch_request_id(command.put_id, batch_index).into_bytes(),
        ),
        producer_epoch: 1,
        first_log_offset,
        last_log_offset,
        previous_log_offset: Some(watermark_to_wire(OffsetWatermark::Empty)),
        committed_log_offset: Some(watermark_to_wire(committed_log_offset)),
        persisted_snapshot_version: None,
    }
}

/// Verifies that a frontend receipt exactly binds the submitted Flight batch and durability.
pub fn validate_put_batch_result(
    command: &FlightPutCommand,
    batch_sequence: u64,
    row_count: u64,
    result: &PutBatchResult,
) -> Result<()> {
    let batch_index = batch_sequence
        .checked_sub(command.first_batch_sequence)
        .ok_or_else(|| internal_receipt(command, "batch sequence precedes command prefix"))?;
    let expected_request_id = put_batch_request_id(command.put_id, batch_index);
    if result.protocol_version != CURRENT_PROTOCOL_VERSION
        || result.put_id.as_ref() != command.put_id.into_bytes()
        || result.batch_sequence != batch_sequence
        || result.row_count != row_count
        || result.request_id.as_ref() != expected_request_id.into_bytes()
        || result.producer_epoch == 0
    {
        return Err(internal_receipt(
            command,
            "frontend receipt identity or batch metadata mismatch",
        ));
    }
    let reached = match PutDurability::try_from(result.durability_reached).ok() {
        Some(PutDurability::Memory) => Durability::Memory,
        Some(PutDurability::Wal) => Durability::Wal,
        Some(PutDurability::Persisted) => Durability::Persisted,
        Some(PutDurability::Unspecified) | None => {
            return Err(internal_receipt(
                command,
                "frontend receipt has unknown durability",
            ));
        }
    };
    if !reached.satisfies(command.durability) {
        return Err(internal_receipt(
            command,
            "frontend receipt did not reach requested durability",
        ));
    }
    let offsets = match (result.first_log_offset, result.last_log_offset) {
        (None, None) => None,
        (Some(first), Some(last)) => Some(
            OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).map_err(|_| {
                internal_receipt(command, "frontend receipt offset range is invalid")
            })?,
        ),
        _ => {
            return Err(internal_receipt(
                command,
                "frontend receipt contains a partial logical offset range",
            ));
        }
    };
    let previous = result
        .previous_log_offset
        .as_ref()
        .ok_or_else(|| internal_receipt(command, "frontend receipt omitted previous watermark"))
        .and_then(|value| decode_watermark(command, value))?;
    let committed = result
        .committed_log_offset
        .as_ref()
        .ok_or_else(|| internal_receipt(command, "frontend receipt omitted committed watermark"))
        .and_then(|value| decode_watermark(command, value))?;
    WriteReceipt::new(
        expected_request_id,
        batch_sequence,
        row_count,
        offsets,
        previous,
        committed,
        reached,
        result.persisted_snapshot_version,
    )
    .map_err(|error| {
        internal_receipt(
            command,
            &format!("frontend receipt invariants failed error=[{error}]"),
        )
    })?;
    Ok(())
}

fn decode_watermark(
    command: &FlightPutCommand,
    value: &crate::wire::OffsetWatermark,
) -> Result<OffsetWatermark> {
    use crate::wire::offset_watermark::State;

    match State::try_from(value.state).ok() {
        Some(State::Empty) if value.offset == 0 => Ok(OffsetWatermark::Empty),
        Some(State::Known) => Ok(OffsetWatermark::Known(LogOffset::new(value.offset))),
        Some(State::Empty | State::Unspecified) | None => Err(internal_receipt(
            command,
            "frontend receipt watermark state is invalid",
        )),
    }
}

fn internal_receipt(command: &FlightPutCommand, message: &str) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("invalid Flight ingest frontend receipt reason=[{message}]"),
    )
    .with_request_id(command.put_id)
    .with_retryable(false)
}

/// Converts an authoritative native receipt into the versioned Flight batch result.
#[must_use]
pub fn put_batch_result_from_receipt(
    command: &FlightPutCommand,
    batch_sequence: u64,
    producer_epoch: u64,
    receipt: &WriteReceipt,
) -> PutBatchResult {
    let (first_log_offset, last_log_offset) = receipt.offsets().map_or((None, None), |offsets| {
        (Some(offsets.first().get()), Some(offsets.last().get()))
    });
    PutBatchResult {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        put_id: Bytes::copy_from_slice(&command.put_id.into_bytes()),
        batch_sequence,
        row_count: receipt.row_count(),
        durability_reached: durability_to_wire(receipt.durability_reached()) as i32,
        request_id: Bytes::copy_from_slice(&receipt.request_id().into_bytes()),
        producer_epoch,
        first_log_offset,
        last_log_offset,
        previous_log_offset: Some(watermark_to_wire(receipt.previous_log_offset())),
        committed_log_offset: Some(watermark_to_wire(receipt.committed_log_offset())),
        persisted_snapshot_version: receipt.persisted_snapshot_version(),
    }
}

fn durability_to_wire(durability: Durability) -> PutDurability {
    match durability {
        Durability::Memory => PutDurability::Memory,
        Durability::Wal => PutDurability::Wal,
        Durability::Persisted => PutDurability::Persisted,
    }
}

fn decode_id<T, E>(
    value: &[u8],
    decode: impl FnOnce([u8; STABLE_ID_BYTES]) -> std::result::Result<T, E>,
    name: &str,
) -> Result<T>
where
    E: std::fmt::Display,
{
    let bytes: [u8; STABLE_ID_BYTES] = value
        .try_into()
        .map_err(|_| invalid(format!("Flight ingest {name} has invalid width")))?;
    decode(bytes).map_err(|_| invalid(format!("Flight ingest {name} must be nonzero")))
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unsupported(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::UnsupportedPlan, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use zippy_types::ingest::{Durability, WriteReceipt};
    use zippy_types::{
        ConnectionId, ErrorCode, LogOffset, OffsetRange, OffsetWatermark, ProducerId, RequestId,
        SessionId,
    };

    use super::{
        decode_put_command, put_batch_request_id, put_batch_result_from_receipt,
        validate_put_batch_result, FlightPutLimits,
    };
    use crate::wire::{PutCommand, PutDurability, PutMutation};
    use crate::CURRENT_PROTOCOL_VERSION;

    fn command() -> PutCommand {
        PutCommand {
            expected_table_id: None,
            namespace: "default".to_owned(),
            protocol_version: CURRENT_PROTOCOL_VERSION,
            session_id: Bytes::copy_from_slice(&SessionId::from_u128(1).unwrap().into_bytes()),
            put_id: Bytes::copy_from_slice(&RequestId::from_u128(2).unwrap().into_bytes()),
            stream_name: "ticks".to_owned(),
            first_batch_sequence: 7,
            mutation: PutMutation::Append as i32,
            durability: PutDurability::Memory as i32,
            producer_id: Bytes::from_static(&[3; 16]),
            connection_id: Bytes::from_static(&[4; 16]),
            expected_writer_epoch: None,
        }
    }

    #[test]
    fn command_validates_optional_table_identity_guard() {
        assert!(decode_put_command(command())
            .unwrap()
            .expected_table_id
            .is_none());
        let mut guarded = command();
        guarded.expected_table_id = Some(Bytes::from_static(&[9; 16]));
        assert_eq!(
            decode_put_command(guarded)
                .unwrap()
                .expected_table_id
                .unwrap()
                .into_bytes(),
            [9; 16]
        );
        for invalid in [vec![0; 16], vec![1; 15], vec![1; 17], vec![]] {
            let mut guarded = command();
            guarded.expected_table_id = Some(Bytes::from(invalid));
            assert_eq!(
                decode_put_command(guarded).unwrap_err().code(),
                ErrorCode::InvalidArgument
            );
        }
    }

    #[test]
    fn command_requires_explicit_mutation_supported_durability_and_nonzero_ids() {
        let decoded = decode_put_command(command()).unwrap();
        assert_eq!(decoded.stream_name, "ticks");
        assert_eq!(decoded.first_batch_sequence, 7);

        let mut durable = command();
        durable.durability = PutDurability::Unspecified as i32;
        assert_eq!(
            decode_put_command(durable).unwrap_err().code(),
            ErrorCode::UnsupportedPlan
        );

        let mut frame = command();
        frame.mutation = PutMutation::Frame as i32;
        assert_eq!(
            decode_put_command(frame).unwrap().mutation,
            PutMutation::Frame
        );
        let mut missing = command();
        missing.mutation = PutMutation::Unspecified as i32;
        assert_eq!(
            decode_put_command(missing).unwrap_err().code(),
            ErrorCode::UnsupportedPlan
        );

        let mut nil = command();
        nil.put_id = Bytes::from_static(&[0; 16]);
        assert_eq!(
            decode_put_command(nil).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn command_rejects_implicit_namespace_and_fencing_identities() {
        let mut missing = command();
        missing.namespace.clear();
        assert_eq!(
            decode_put_command(missing).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        let mut missing = command();
        missing.producer_id.clear();
        assert_eq!(
            decode_put_command(missing).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        let mut missing = command();
        missing.connection_id.clear();
        assert_eq!(
            decode_put_command(missing).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn command_accepts_wal_and_persisted_with_explicit_fencing_ids() {
        let producer_id = ProducerId::from_u128(3).unwrap();
        let connection_id = ConnectionId::from_u128(4).unwrap();
        let mut wal = command();
        wal.durability = PutDurability::Wal as i32;
        wal.producer_id = Bytes::copy_from_slice(&producer_id.into_bytes());
        wal.connection_id = Bytes::copy_from_slice(&connection_id.into_bytes());
        wal.expected_writer_epoch = Some(9);

        let decoded = decode_put_command(wal).unwrap();
        assert_eq!(decoded.durability, Durability::Wal);
        assert_eq!(decoded.producer_id, producer_id);
        assert_eq!(decoded.connection_id, connection_id);
        assert_eq!(decoded.expected_writer_epoch, Some(9));

        let mut persisted = command();
        persisted.durability = PutDurability::Persisted as i32;
        assert_eq!(
            decode_put_command(persisted).unwrap().durability,
            Durability::Persisted
        );
    }

    #[test]
    fn native_receipt_round_trip_uses_stable_batch_ids_and_validates_watermarks() {
        let mut wire = command();
        wire.durability = PutDurability::Wal as i32;
        let decoded = decode_put_command(wire).unwrap();
        let request_id = put_batch_request_id(decoded.put_id, 0);
        assert_eq!(request_id, put_batch_request_id(decoded.put_id, 0));
        assert_ne!(request_id, put_batch_request_id(decoded.put_id, 1));

        let receipt = WriteReceipt::new(
            request_id,
            0,
            2,
            Some(OffsetRange::new(LogOffset::new(0), LogOffset::new(1)).unwrap()),
            OffsetWatermark::Empty,
            OffsetWatermark::Known(LogOffset::new(1)),
            Durability::Wal,
            None,
        )
        .unwrap();
        let result = put_batch_result_from_receipt(&decoded, 7, 3, &receipt);
        validate_put_batch_result(&decoded, 7, 2, &result).unwrap();

        let mut malformed = result;
        malformed.committed_log_offset.as_mut().unwrap().offset = 0;
        assert_eq!(
            validate_put_batch_result(&decoded, 7, 2, &malformed)
                .unwrap_err()
                .code(),
            ErrorCode::Internal
        );
    }

    #[test]
    fn limits_reject_zero_inverted_and_unbounded_values() {
        assert!(FlightPutLimits::try_new(4, 16, 1_000, 10_000, 1_048_576, 8_388_608).is_ok());
        assert!(FlightPutLimits::try_new(0, 16, 1_000, 10_000, 1_048_576, 8_388_608).is_err());
        assert!(FlightPutLimits::try_new(4, 16, 10_001, 10_000, 1_048_576, 8_388_608).is_err());
        assert!(FlightPutLimits::try_new(4, 16, 1_000, 10_000, 8_388_609, 8_388_608).is_err());
    }
}
