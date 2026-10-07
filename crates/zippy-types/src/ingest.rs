//! Long-lived producer-fenced ingest handles and explicit durability acknowledgements.

use arrow::record_batch::RecordBatch;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{
    BoxFuture, ConnectionId, ErrorCode, OffsetRange, OffsetWatermark, ProducerId, RequestId,
    Result, SequenceWatermark, ServerError, TableId,
};

/// The current canonical mutation fingerprint format.
pub const CURRENT_MUTATION_FINGERPRINT_FORMAT_VERSION: u32 = 1;

/// A canonical mutation fingerprint format understood by ingest and WAL replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MutationFingerprintFormatVersion(u32);

impl MutationFingerprintFormatVersion {
    /// Creates a decoded version for explicit compatibility validation.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the current ingress and WAL format version.
    #[must_use]
    pub const fn current() -> Self {
        Self(CURRENT_MUTATION_FINGERPRINT_FORMAT_VERSION)
    }

    /// Returns the fixed-width version value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A SHA-256 digest of one canonical table mutation.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MutationFingerprintDigest([u8; 32]);

impl MutationFingerprintDigest {
    /// Creates a digest from raw SHA-256 bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw SHA-256 bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for MutationFingerprintDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("MutationFingerprintDigest")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for MutationFingerprintDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for MutationFingerprintDigest {
    type Err = InvalidMutationFingerprint;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(InvalidMutationFingerprint::InvalidDigestLength {
                actual: value.len(),
            });
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(pair)
                .map_err(|_| InvalidMutationFingerprint::InvalidDigestHex { index: index * 2 })?;
            bytes[index] = u8::from_str_radix(text, 16)
                .map_err(|_| InvalidMutationFingerprint::InvalidDigestHex { index: index * 2 })?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for MutationFingerprintDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for MutationFingerprintDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// A versioned canonical mutation fingerprint persisted in durable WAL records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MutationFingerprint {
    /// The canonical mutation encoding version.
    pub format_version: MutationFingerprintFormatVersion,
    /// SHA-256 of the versioned canonical mutation bytes.
    pub digest: MutationFingerprintDigest,
}

/// An invalid mutation fingerprint encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidMutationFingerprint {
    /// A SHA-256 digest must contain exactly 64 hexadecimal digits.
    #[error("invalid mutation fingerprint length actual=[{actual}]")]
    InvalidDigestLength {
        /// The number of input bytes.
        actual: usize,
    },
    /// A digest contains a non-hexadecimal digit.
    #[error("invalid mutation fingerprint hex index=[{index}]")]
    InvalidDigestHex {
        /// The zero-based digit position.
        index: usize,
    },
}

/// The durability boundary reached by an ingest write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// Data is visible in the current process but may be lost on a crash.
    Memory,
    /// Data is recoverable from the ingest write-ahead log.
    Wal,
    /// Data is covered by a committed persistent manifest snapshot.
    Persisted,
}

impl Durability {
    /// Returns true when this durability level satisfies the required level.
    #[must_use]
    pub fn satisfies(self, required: Self) -> bool {
        self >= required
    }
}

/// A durability target that necessarily requires asynchronous acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableWriteTarget {
    /// Wait until the batch is recoverable from WAL.
    Wal,
    /// Wait until a manifest snapshot covers the batch.
    Persisted,
}

impl From<DurableWriteTarget> for Durability {
    fn from(target: DurableWriteTarget) -> Self {
        match target {
            DurableWriteTarget::Wal => Self::Wal,
            DurableWriteTarget::Persisted => Self::Persisted,
        }
    }
}

/// A typed key-value table mutation.
#[derive(Debug, Clone)]
pub enum KeyValueMutation {
    /// Upsert rows keyed by the table definition's ordered key columns.
    Upsert(RecordBatch),
    /// Delete keys encoded by a batch containing all ordered key columns.
    Delete(RecordBatch),
}

impl KeyValueMutation {
    /// Returns the number of logical changelog events.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        match self {
            Self::Upsert(batch) | Self::Delete(batch) => batch.num_rows() as u64,
        }
    }
}

/// One table-kind-specific atomic mutation.
#[derive(Debug, Clone)]
pub enum TableMutation {
    /// Append one atomic record batch to an append table.
    Append(RecordBatch),
    /// Commit one complete atomic frame to a frame table.
    Frame(RecordBatch),
    /// Apply one typed key-value changelog mutation.
    KeyValue(KeyValueMutation),
}

impl TableMutation {
    /// Returns the number of logical log positions requested.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        match self {
            Self::Append(batch) | Self::Frame(batch) => batch.num_rows() as u64,
            Self::KeyValue(mutation) => mutation.row_count(),
        }
    }
}

/// A request to open one long-lived writer connection and acquire fencing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenWriterRequest {
    /// The idempotency key for producer acquisition.
    pub request_id: RequestId,
    /// The table that the writer will mutate.
    pub table_id: TableId,
    /// The durable producer identity that survives reconnects.
    pub producer_id: ProducerId,
    /// The boot-scoped connection identity owning the live writer lease.
    pub connection_id: ConnectionId,
    /// The previous epoch observed by the caller, when known.
    pub expected_writer_epoch: Option<u64>,
}

/// Immutable identity and fencing metadata for an open writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterInfo {
    /// The table owned by the writer.
    pub table_id: TableId,
    /// The durable producer identity.
    pub producer_id: ProducerId,
    /// The boot-scoped writer connection identity.
    pub connection_id: ConnectionId,
    /// The active fencing epoch.
    pub producer_epoch: u64,
    /// The schema version accepted by new writes.
    pub schema_version: u64,
    /// The last durable sequence, distinct from an empty producer epoch.
    pub last_durable_batch_sequence: SequenceWatermark,
    /// The last durable logical boundary recovered at open time.
    pub last_durable_log_offset: OffsetWatermark,
}

/// One unverified producer-sequenced mutation at the ingest transport boundary.
#[derive(Debug, Clone)]
pub struct IncomingWriteRequest {
    /// The idempotency key for this logical batch.
    pub request_id: RequestId,
    /// The schema version used by the Arrow batch.
    pub schema_version: u64,
    /// A strictly increasing sequence within the writer epoch.
    pub batch_sequence: u64,
    /// The table-kind-specific Arrow mutation.
    pub mutation: TableMutation,
}

/// One fingerprinted producer mutation accepted by a writer or persisted in WAL.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    request_id: RequestId,
    schema_version: u64,
    batch_sequence: u64,
    mutation: TableMutation,
    mutation_fingerprint: MutationFingerprint,
    prepared_payload: Option<Arc<[u8]>>,
}

impl WriteRequest {
    /// Returns the idempotency key for this logical batch.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the schema version used by the Arrow batch.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Returns the producer sequence within its fencing epoch.
    #[must_use]
    pub const fn batch_sequence(&self) -> u64 {
        self.batch_sequence
    }

    /// Borrows the typed table mutation.
    #[must_use]
    pub const fn mutation(&self) -> &TableMutation {
        &self.mutation
    }

    /// Returns the versioned canonical fingerprint persisted with this write.
    #[must_use]
    pub const fn mutation_fingerprint(&self) -> MutationFingerprint {
        self.mutation_fingerprint
    }

    /// Borrows an ingress-prepared canonical payload reusable by the persistence layer.
    #[must_use]
    pub const fn prepared_payload(&self) -> Option<&Arc<[u8]>> {
        self.prepared_payload.as_ref()
    }
}

/// Computes and verifies canonical mutation fingerprints at ingress and replay.
///
/// Implementations own the exact Arrow canonicalization for each supported
/// format. Ingress must call [`MutationFingerprintComputer::prepare`] before
/// sequence resolution. WAL replay must call `verify` before trusting a stored
/// fingerprint, so a format remains readable after a newer format is introduced.
pub trait MutationFingerprintComputer: Send + Sync {
    /// Computes a digest for an explicit, supported canonical format version.
    fn compute(
        &self,
        format_version: MutationFingerprintFormatVersion,
        schema_version: u64,
        mutation: &TableMutation,
    ) -> Result<MutationFingerprintDigest>;

    /// Computes the fingerprint and optionally returns the same encoded canonical payload.
    ///
    /// Implementations that already encode a mutation while hashing should return that immutable
    /// payload so the WAL path does not repeat the serialization. The default preserves existing
    /// implementations and leaves persistence to encode the mutation itself.
    fn compute_prepared(
        &self,
        format_version: MutationFingerprintFormatVersion,
        schema_version: u64,
        mutation: &TableMutation,
    ) -> Result<(MutationFingerprintDigest, Option<Arc<[u8]>>)> {
        self.compute(format_version, schema_version, mutation)
            .map(|digest| (digest, None))
    }

    /// Converts an unverified transport request into the only writer-accepted form.
    fn prepare(&self, request: IncomingWriteRequest) -> Result<WriteRequest> {
        let format_version = MutationFingerprintFormatVersion::current();
        let (digest, prepared_payload) =
            self.compute_prepared(format_version, request.schema_version, &request.mutation)?;
        Ok(WriteRequest {
            request_id: request.request_id,
            schema_version: request.schema_version,
            batch_sequence: request.batch_sequence,
            mutation: request.mutation,
            mutation_fingerprint: MutationFingerprint {
                format_version,
                digest,
            },
            prepared_payload,
        })
    }

    /// Restores one WAL mutation and verifies its persisted canonical fingerprint.
    fn restore(
        &self,
        request_id: RequestId,
        schema_version: u64,
        batch_sequence: u64,
        mutation: TableMutation,
        mutation_fingerprint: MutationFingerprint,
    ) -> Result<WriteRequest> {
        let request = WriteRequest {
            request_id,
            schema_version,
            batch_sequence,
            mutation,
            mutation_fingerprint,
            prepared_payload: None,
        };
        self.verify(&request)?;
        Ok(request)
    }

    /// Recomputes a WAL fingerprint using its historical format version.
    fn verify(&self, request: &WriteRequest) -> Result<()> {
        let actual = self.compute(
            request.mutation_fingerprint.format_version,
            request.schema_version,
            &request.mutation,
        )?;
        if actual != request.mutation_fingerprint.digest {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "mutation fingerprint mismatch",
            )
            .with_request_id(request.request_id));
        }
        Ok(())
    }
}

/// A validated acknowledgement for one write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteReceipt {
    request_id: RequestId,
    batch_sequence: u64,
    row_count: u64,
    offsets: Option<OffsetRange>,
    previous_log_offset: OffsetWatermark,
    committed_log_offset: OffsetWatermark,
    durability_reached: Durability,
    persisted_snapshot_version: Option<u64>,
}

impl WriteReceipt {
    /// Creates a receipt while enforcing row/offset and durability invariants.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: RequestId,
        batch_sequence: u64,
        row_count: u64,
        offsets: Option<OffsetRange>,
        previous_log_offset: OffsetWatermark,
        committed_log_offset: OffsetWatermark,
        durability_reached: Durability,
        persisted_snapshot_version: Option<u64>,
    ) -> std::result::Result<Self, InvalidWriteReceipt> {
        match (row_count, offsets) {
            (0, None) if previous_log_offset == committed_log_offset => {}
            (0, None) => return Err(InvalidWriteReceipt::ZeroRowsChangedWatermark),
            (0, Some(_)) => return Err(InvalidWriteReceipt::ZeroRowsWithOffsets),
            (_, None) => return Err(InvalidWriteReceipt::RowsWithoutOffsets),
            (rows, Some(range)) if range.len() != rows as u128 => {
                return Err(InvalidWriteReceipt::RowOffsetCountMismatch {
                    rows,
                    offsets: range.len(),
                });
            }
            (_, Some(_)) => {}
        }
        if row_count != 0 {
            let range = offsets.expect("non-empty writes were validated to have offsets");
            let expected_first = match previous_log_offset {
                OffsetWatermark::Empty => crate::LogOffset::new(0),
                OffsetWatermark::Known(previous) => previous
                    .checked_next()
                    .ok_or(InvalidWriteReceipt::OffsetSpaceExhausted)?,
            };
            if range.first() != expected_first {
                return Err(InvalidWriteReceipt::NonContiguousOffsets {
                    expected_first,
                    actual_first: range.first(),
                });
            }
            if committed_log_offset != OffsetWatermark::Known(range.last()) {
                return Err(InvalidWriteReceipt::ResultWatermarkMismatch);
            }
        }
        if durability_reached == Durability::Persisted
            && persisted_snapshot_version.is_none_or(|version| version == 0)
        {
            return Err(InvalidWriteReceipt::PersistedWithoutSnapshot);
        }
        if durability_reached != Durability::Persisted && persisted_snapshot_version.is_some() {
            return Err(InvalidWriteReceipt::SnapshotWithoutPersistedDurability);
        }
        Ok(Self {
            request_id,
            batch_sequence,
            row_count,
            offsets,
            previous_log_offset,
            committed_log_offset,
            durability_reached,
            persisted_snapshot_version,
        })
    }

    /// Returns the idempotent request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the accepted producer sequence.
    #[must_use]
    pub const fn batch_sequence(&self) -> u64 {
        self.batch_sequence
    }

    /// Returns the number of logical events written.
    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the assigned range, absent exactly when `row_count == 0`.
    #[must_use]
    pub const fn offsets(&self) -> Option<OffsetRange> {
        self.offsets
    }

    /// Returns the authoritative table watermark immediately before the write.
    #[must_use]
    pub const fn previous_log_offset(&self) -> OffsetWatermark {
        self.previous_log_offset
    }

    /// Returns the table watermark after applying the write.
    #[must_use]
    pub const fn committed_log_offset(&self) -> OffsetWatermark {
        self.committed_log_offset
    }

    /// Returns the acknowledgement durability boundary.
    #[must_use]
    pub const fn durability_reached(&self) -> Durability {
        self.durability_reached
    }

    /// Returns the covering manifest version for persisted writes.
    #[must_use]
    pub const fn persisted_snapshot_version(&self) -> Option<u64> {
        self.persisted_snapshot_version
    }
}

impl<'de> Deserialize<'de> for WriteReceipt {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawReceipt {
            request_id: RequestId,
            batch_sequence: u64,
            row_count: u64,
            offsets: Option<OffsetRange>,
            previous_log_offset: OffsetWatermark,
            committed_log_offset: OffsetWatermark,
            durability_reached: Durability,
            persisted_snapshot_version: Option<u64>,
        }

        let raw = RawReceipt::deserialize(deserializer)?;
        Self::new(
            raw.request_id,
            raw.batch_sequence,
            raw.row_count,
            raw.offsets,
            raw.previous_log_offset,
            raw.committed_log_offset,
            raw.durability_reached,
            raw.persisted_snapshot_version,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// An invalid write acknowledgement invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidWriteReceipt {
    /// A zero-row write must not consume offsets.
    #[error("zero-row write receipt must not contain offsets")]
    ZeroRowsWithOffsets,
    /// A zero-row mutation is an idempotent no-op and cannot move a watermark.
    #[error("zero-row write receipt must preserve the log watermark")]
    ZeroRowsChangedWatermark,
    /// A non-empty write must contain an offset range.
    #[error("non-empty write receipt must contain offsets")]
    RowsWithoutOffsets,
    /// The range width must match the logical row count.
    #[error("write receipt row/offset mismatch rows=[{rows}] offsets=[{offsets}]")]
    RowOffsetCountMismatch {
        /// The acknowledged logical row count.
        rows: u64,
        /// The number of positions in the offset range.
        offsets: u128,
    },
    /// The previous watermark exhausted the fixed-width logical offset space.
    #[error("write receipt cannot allocate an offset after u64::MAX")]
    OffsetSpaceExhausted,
    /// A write range must begin immediately after the previous watermark.
    #[error(
        "write offsets are not contiguous expected=[{expected_first}] actual=[{actual_first}]"
    )]
    NonContiguousOffsets {
        /// The first position required by the previous watermark.
        expected_first: crate::LogOffset,
        /// The first position found in the receipt.
        actual_first: crate::LogOffset,
    },
    /// The resulting watermark must equal the last assigned offset.
    #[error("write receipt result watermark does not equal the assigned range end")]
    ResultWatermarkMismatch,
    /// Persisted durability always names its covering manifest version.
    #[error("persisted write receipt must contain a positive snapshot version")]
    PersistedWithoutSnapshot,
    /// Weaker durability must not imply that a manifest is already committed.
    #[error("snapshot version requires persisted durability")]
    SnapshotWithoutPersistedDurability,
}

/// An implementation-owned asynchronous acknowledgement for a durable write.
///
/// Dropping this handle stops waiting but does not roll back an accepted write.
/// A client may retry the same request and sequence to recover its idempotent
/// receipt. `wait` consumes the handle so only one waiter owns completion.
pub trait DurableWriteAck: Send {
    /// Returns the accepted request identity before awaiting durability.
    fn request_id(&self) -> RequestId;

    /// Waits for WAL or persisted durability without blocking the ingest actor.
    fn wait(self: Box<Self>) -> BoxFuture<'static, Result<WriteReceipt>>;
}

/// A type-erased durable acknowledgement handle.
pub type OpaqueDurableWriteAck = Box<dyn DurableWriteAck>;

/// A long-lived producer-fenced writer hot-path handle.
///
/// `write_memory` and `submit_durable` are synchronous bounded submissions and
/// therefore allocate no per-batch boxed future on the memory path. Dropping
/// the writer releases only its boot-scoped connection lease; durable producer
/// checkpoints and already accepted writes remain authoritative. Within one
/// producer epoch, sequences are strictly contiguous: retrying the same
/// sequence with the same canonical mutation returns its original receipt,
/// while different mutation content returns `CONFLICT`. Durable WAL records
/// must persist the canonical mutation fingerprint used by this comparison.
/// Skipping a sequence requires a new producer epoch or a separate audited
/// control operation; ordinary writes cannot opt out of continuity.
/// Every zero-row mutation is an accepted idempotent no-op: it consumes the
/// producer batch sequence but assigns no offsets, preserves the table
/// watermark, and emits no row, callback, or frame-completion marker.
pub trait IngestWriter: Send {
    /// Returns immutable writer identity and fencing metadata.
    fn info(&self) -> &WriterInfo;

    /// Applies and acknowledges one process-local memory write synchronously.
    fn write_memory(&mut self, request: WriteRequest) -> Result<WriteReceipt>;

    /// Submits one durable write and returns a separately awaited acknowledgement.
    fn submit_durable(
        &mut self,
        request: WriteRequest,
        target: DurableWriteTarget,
    ) -> Result<OpaqueDurableWriteAck>;
}

/// A type-erased long-lived ingest writer.
pub type OpaqueIngestWriter = Box<dyn IngestWriter>;

/// The producer-fenced ingest service boundary.
pub trait IngestService: Send + Sync {
    /// Acquires producer fencing once and opens a long-lived writer handle.
    fn open_writer(&self, request: OpenWriterRequest) -> BoxFuture<'_, Result<OpaqueIngestWriter>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;

    use super::{
        Durability, IncomingWriteRequest, IngestService, MutationFingerprintComputer,
        MutationFingerprintDigest, MutationFingerprintFormatVersion, TableMutation, WriteReceipt,
    };
    use crate::{LogOffset, OffsetRange, OffsetWatermark, RequestId, Result};

    struct TestFingerprintComputer;

    impl MutationFingerprintComputer for TestFingerprintComputer {
        fn compute(
            &self,
            format_version: MutationFingerprintFormatVersion,
            schema_version: u64,
            mutation: &TableMutation,
        ) -> Result<MutationFingerprintDigest> {
            assert_eq!(format_version, MutationFingerprintFormatVersion::current());
            let mut digest = [0_u8; 32];
            digest[..8].copy_from_slice(&schema_version.to_le_bytes());
            digest[8..16].copy_from_slice(&mutation.row_count().to_le_bytes());
            Ok(MutationFingerprintDigest::from_bytes(digest))
        }
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).unwrap()
    }

    fn assert_object_safe(_: Option<&dyn IngestService>) {}

    #[test]
    fn ingest_service_is_object_safe() {
        assert_object_safe(None);
    }

    #[test]
    fn durability_order_matches_ack_strength() {
        assert!(Durability::Persisted.satisfies(Durability::Wal));
        assert!(Durability::Wal.satisfies(Durability::Memory));
        assert!(!Durability::Memory.satisfies(Durability::Wal));
    }

    #[test]
    fn write_request_clone_shares_arrow_buffers() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let batch = RecordBatch::try_from_iter([("value", Arc::clone(&values))])
            .expect("build record batch");
        let request = TestFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(1),
                schema_version: 1,
                batch_sequence: 1,
                mutation: TableMutation::Append(batch),
            })
            .unwrap();
        let cloned = request.clone();
        let TableMutation::Append(original) = request.mutation() else {
            panic!("expected append mutation");
        };
        let TableMutation::Append(cloned) = cloned.mutation() else {
            panic!("expected append mutation");
        };

        assert!(Arc::ptr_eq(original.column(0), cloned.column(0)));
        TestFingerprintComputer.verify(&request).unwrap();
        let mut tampered = request.clone();
        tampered.mutation_fingerprint.digest = MutationFingerprintDigest::from_bytes([0; 32]);
        assert!(TestFingerprintComputer.verify(&tampered).is_err());
        assert_eq!(request.mutation_fingerprint().format_version.get(), 1);
        assert_eq!(
            serde_json::to_string(&request.mutation_fingerprint()).unwrap(),
            concat!(
                r#"{"format_version":1,"digest":"01000000000000000200000000000000"#,
                r#"00000000000000000000000000000000"}"#,
            )
        );
        assert!(serde_json::from_str::<MutationFingerprintDigest>(r#""xyz""#).is_err());
    }

    #[test]
    fn persisted_receipt_requires_a_positive_manifest_version() {
        for version in [None, Some(0), Some(1)] {
            let receipt = WriteReceipt::new(
                request_id(1),
                0,
                0,
                None,
                OffsetWatermark::Empty,
                OffsetWatermark::Empty,
                Durability::Persisted,
                version,
            );
            assert_eq!(receipt.is_ok(), version == Some(1));
        }
    }

    #[test]
    fn zero_row_receipt_is_distinct_from_offset_zero() {
        let empty = WriteReceipt::new(
            request_id(1),
            1,
            0,
            None,
            OffsetWatermark::Empty,
            OffsetWatermark::Empty,
            Durability::Memory,
            None,
        )
        .unwrap();
        let first = WriteReceipt::new(
            request_id(2),
            2,
            1,
            Some(OffsetRange::single(LogOffset::new(0))),
            OffsetWatermark::Empty,
            OffsetWatermark::Known(LogOffset::new(0)),
            Durability::Wal,
            None,
        )
        .unwrap();

        assert_eq!(empty.offsets(), None);
        assert_eq!(first.offsets().unwrap().first(), LogOffset::new(0));
        assert!(WriteReceipt::new(
            request_id(3),
            3,
            0,
            Some(OffsetRange::single(LogOffset::new(0))),
            OffsetWatermark::Empty,
            OffsetWatermark::Known(LogOffset::new(0)),
            Durability::Memory,
            None,
        )
        .is_err());
        assert!(WriteReceipt::new(
            request_id(4),
            4,
            0,
            None,
            OffsetWatermark::Empty,
            OffsetWatermark::Known(LogOffset::new(0)),
            Durability::Memory,
            None,
        )
        .is_err());
        assert!(WriteReceipt::new(
            request_id(5),
            5,
            1,
            Some(OffsetRange::single(LogOffset::new(2))),
            OffsetWatermark::Known(LogOffset::new(0)),
            OffsetWatermark::Known(LogOffset::new(2)),
            Durability::Wal,
            None,
        )
        .is_err());
    }
}
