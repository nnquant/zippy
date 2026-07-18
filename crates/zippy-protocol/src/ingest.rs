//! Bounded Arrow Flight `DoPut` descriptors and memory-durability receipts.

use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::FlightDescriptor;
use bytes::Bytes;
use prost::Message;
use std::sync::Arc;
use zippy_types::{ErrorCode, RequestId, Result, ServerError, SessionId};

use crate::query::STABLE_ID_BYTES;
use crate::wire::{PutBatchResult, PutCommand, PutDurability, PutMutation};
use crate::CURRENT_PROTOCOL_VERSION;

/// Maximum serialized `PutCommand` accepted by protocol version 1.
pub const MAX_PUT_COMMAND_BYTES: usize = 64 * 1024;
/// Maximum legacy stream-name size accepted at the transport boundary.
pub const MAX_PUT_STREAM_NAME_BYTES: usize = 512;
/// Maximum configurable concurrent `DoPut` streams.
pub const MAX_CONCURRENT_PUTS: usize = 1_024;
/// Maximum configurable record batches consumed by one `DoPut` stream.
pub const MAX_BATCHES_PER_PUT: usize = 65_536;
/// Maximum configurable rows in one decoded batch or complete `DoPut` stream.
pub const MAX_ROWS_PER_PUT: u64 = 1_000_000_000;
/// Maximum configurable decoded bytes in one batch or complete `DoPut` stream.
pub const MAX_BYTES_PER_PUT: usize = 4 * 1024 * 1024 * 1024;

/// Validated append-only, memory-durability command for one `DoPut` stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightPutCommand {
    /// Server-issued writer session bound to the request metadata.
    pub session_id: SessionId,
    /// Client-stable identity for this stream attempt and its batch receipts.
    pub put_id: RequestId,
    /// Legacy stream name used by the Stage 6.4 compatibility backend.
    pub stream_name: String,
    /// Sequence assigned to the first decoded record batch.
    pub first_batch_sequence: u64,
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
    if PutMutation::try_from(command.mutation).ok() != Some(PutMutation::Append) {
        return Err(
            unsupported("Flight protocol version 1 only supports append mutation")
                .with_request_id(put_id),
        );
    }
    if PutDurability::try_from(command.durability).ok() != Some(PutDurability::Memory) {
        return Err(unsupported(
            "Flight Stage 6.4 only acknowledges process-local memory durability",
        )
        .with_request_id(put_id));
    }
    Ok(FlightPutCommand {
        session_id,
        put_id,
        stream_name: command.stream_name,
        first_batch_sequence: command.first_batch_sequence,
    })
}

/// Builds one stable memory-durability result for a successfully applied batch.
#[must_use]
pub fn put_batch_result(
    command: &FlightPutCommand,
    batch_sequence: u64,
    row_count: u64,
) -> PutBatchResult {
    PutBatchResult {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        put_id: Bytes::copy_from_slice(&command.put_id.into_bytes()),
        batch_sequence,
        row_count,
        durability_reached: PutDurability::Memory as i32,
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
    use zippy_types::{ErrorCode, RequestId, SessionId};

    use super::{decode_put_command, FlightPutLimits};
    use crate::wire::{PutCommand, PutDurability, PutMutation};
    use crate::CURRENT_PROTOCOL_VERSION;

    fn command() -> PutCommand {
        PutCommand {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            session_id: Bytes::copy_from_slice(&SessionId::from_u128(1).unwrap().into_bytes()),
            put_id: Bytes::copy_from_slice(&RequestId::from_u128(2).unwrap().into_bytes()),
            stream_name: "ticks".to_owned(),
            first_batch_sequence: 7,
            mutation: PutMutation::Append as i32,
            durability: PutDurability::Memory as i32,
        }
    }

    #[test]
    fn command_requires_explicit_append_memory_semantics_and_nonzero_ids() {
        let decoded = decode_put_command(command()).unwrap();
        assert_eq!(decoded.stream_name, "ticks");
        assert_eq!(decoded.first_batch_sequence, 7);

        let mut durable = command();
        durable.durability = PutDurability::Unspecified as i32;
        assert_eq!(
            decode_put_command(durable).unwrap_err().code(),
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
    fn limits_reject_zero_inverted_and_unbounded_values() {
        assert!(FlightPutLimits::try_new(4, 16, 1_000, 10_000, 1_048_576, 8_388_608).is_ok());
        assert!(FlightPutLimits::try_new(0, 16, 1_000, 10_000, 1_048_576, 8_388_608).is_err());
        assert!(FlightPutLimits::try_new(4, 16, 10_001, 10_000, 1_048_576, 8_388_608).is_err());
        assert!(FlightPutLimits::try_new(4, 16, 1_000, 10_000, 8_388_609, 8_388_608).is_err());
    }
}
