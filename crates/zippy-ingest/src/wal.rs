//! Versioned per-table ingest WAL encoding, commit markers, and bounded recovery.

use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use sha2::{Digest, Sha256};
use zippy_types::ingest::{
    Durability, KeyValueMutation, MutationFingerprint, MutationFingerprintComputer,
    MutationFingerprintDigest, MutationFingerprintFormatVersion, TableMutation, WriteReceipt,
    WriteRequest,
};
use zippy_types::schema_hash::canonical_catalog_schema_digest_v1;
use zippy_types::{
    ErrorCode, LogOffset, OffsetRange, OffsetWatermark, ProducerId, RequestId, Result,
    SequenceWatermark, ServerError, ServerErrorDetails, TableId,
};

/// The first on-disk ingest WAL format understood by this server.
pub const CURRENT_INGEST_WAL_FORMAT_VERSION: u32 = 1;

const WAL_MAGIC: &[u8; 8] = b"ZPWAL001";
const WAL_HEADER_LEN: usize = 208;
const WAL_HEADER_CRC_OFFSET: usize = 184;
const WAL_RECORD_DATA: u8 = 1;
const WAL_RECORD_COMMIT: u8 = 2;
const MUTATION_APPEND: u8 = 1;
const MUTATION_FRAME: u8 = 2;
const MUTATION_KEY_VALUE_UPSERT: u8 = 3;
const MUTATION_KEY_VALUE_DELETE: u8 = 4;
const WATERMARK_EMPTY: u8 = 0;
const WATERMARK_KNOWN: u8 = 1;
const WATERMARK_LEGACY_UNKNOWN: u8 = 2;
const OFFSETS_NONE: u8 = 0;
const OFFSETS_PRESENT: u8 = 1;
const DEFAULT_MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_RECOVERY_RECORDS: usize = 1_000_000;
const DEFAULT_MAX_RECOVERY_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_MAX_WAL_FILES_PER_TABLE: usize = 4_096;
const MAX_CONFIGURED_RECORD_BYTES: usize = 1024 * 1024 * 1024;
const MAX_CONFIGURED_RECOVERY_RECORDS: usize = 100_000_000;
const MAX_CONFIGURED_RECOVERY_BYTES: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const MAX_CONFIGURED_WAL_FILES_PER_TABLE: usize = 1_000_000;
const WAL_SEGMENT_SEQUENCE: u64 = 1;
const FINGERPRINT_DOMAIN: &[u8] = b"zippy.ingest.mutation.v1\0";

/// Bounded record and startup-scan limits for one table WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestWalConfig {
    max_record_bytes: usize,
    max_recovery_records: usize,
    max_recovery_bytes: u64,
    max_wal_files_per_table: usize,
}

impl Default for IngestWalConfig {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_recovery_records: DEFAULT_MAX_RECOVERY_RECORDS,
            max_recovery_bytes: DEFAULT_MAX_RECOVERY_BYTES,
            max_wal_files_per_table: DEFAULT_MAX_WAL_FILES_PER_TABLE,
        }
    }
}

impl IngestWalConfig {
    /// Creates conservative production defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the maximum encoded Arrow payload accepted per record.
    #[must_use]
    pub const fn with_max_record_bytes(mut self, maximum: usize) -> Self {
        self.max_record_bytes = maximum;
        self
    }

    /// Replaces the maximum durable data records decoded during one table recovery.
    #[must_use]
    pub const fn with_max_recovery_records(mut self, maximum: usize) -> Self {
        self.max_recovery_records = maximum;
        self
    }

    /// Replaces the maximum total WAL file bytes inspected for one table.
    #[must_use]
    pub const fn with_max_recovery_bytes(mut self, maximum: u64) -> Self {
        self.max_recovery_bytes = maximum;
        self
    }

    /// Replaces the maximum WAL files inspected for one table.
    #[must_use]
    pub const fn with_max_wal_files_per_table(mut self, maximum: usize) -> Self {
        self.max_wal_files_per_table = maximum;
        self
    }

    /// Validates every allocation and scan bound before filesystem access.
    pub fn validate(self) -> Result<()> {
        if self.max_record_bytes == 0 || self.max_record_bytes > MAX_CONFIGURED_RECORD_BYTES {
            return Err(invalid_config(format!(
                "ingest WAL max record bytes must be between 1 and {MAX_CONFIGURED_RECORD_BYTES}"
            )));
        }
        if self.max_recovery_records == 0
            || self.max_recovery_records > MAX_CONFIGURED_RECOVERY_RECORDS
        {
            return Err(invalid_config(format!(
                "ingest WAL max recovery records must be between 1 and {MAX_CONFIGURED_RECOVERY_RECORDS}"
            )));
        }
        if self.max_recovery_bytes == 0 || self.max_recovery_bytes > MAX_CONFIGURED_RECOVERY_BYTES {
            return Err(invalid_config(format!(
                "ingest WAL max recovery bytes must be between 1 and {MAX_CONFIGURED_RECOVERY_BYTES}"
            )));
        }
        if self.max_wal_files_per_table == 0
            || self.max_wal_files_per_table > MAX_CONFIGURED_WAL_FILES_PER_TABLE
        {
            return Err(invalid_config(format!(
                "ingest WAL max files per table must be between 1 and {MAX_CONFIGURED_WAL_FILES_PER_TABLE}"
            )));
        }
        Ok(())
    }
}

/// Immutable table and producer identity encoded into every WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestWalIdentity {
    /// The table whose logical log receives the mutation.
    pub table_id: TableId,
    /// The durable producer identity owning this epoch.
    pub producer_id: ProducerId,
    /// The Catalog-issued fencing epoch.
    pub producer_epoch: u64,
}

/// Byte positions assigned to one appended but not necessarily durable record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalAppendPosition {
    /// Inclusive byte position of the record header.
    pub start: u64,
    /// Exclusive byte position after the payload.
    pub end: u64,
}

/// One fsync boundary published by a commit marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCommitReceipt {
    /// Durable file length including the commit marker.
    pub durable_bytes: u64,
    /// Number of newly committed data records in this group.
    pub committed_records: u64,
    /// Greatest producer sequence known durable, or empty when the segment has no records.
    pub last_batch_sequence: SequenceWatermark,
    /// Greatest logical table offset covered by this marker.
    pub committed_log_offset: OffsetWatermark,
}

/// One mutation recovered from a checksum-valid committed WAL prefix.
#[derive(Debug, Clone)]
pub struct RecoveredWalRecord {
    /// The table and producer epoch recorded with the mutation.
    pub identity: IngestWalIdentity,
    /// The fingerprint-verified typed mutation.
    pub request: WriteRequest,
    /// The exact idempotent receipt promoted to WAL durability.
    pub receipt: WriteReceipt,
    /// Original byte range occupied by the data record.
    pub position: WalAppendPosition,
}

/// Bounded recovery output across every WAL segment for one table.
#[derive(Debug)]
pub struct IngestWalRecovery {
    /// Durable records in epoch, segment, and file order.
    pub records: Vec<RecoveredWalRecord>,
    /// Total bytes removed after the last checksum-valid commit markers.
    pub truncated_tail_bytes: u64,
    /// Total WAL bytes inspected before truncation.
    pub inspected_bytes: u64,
    /// Number of segment files inspected.
    pub inspected_files: usize,
    /// Greatest recovered logical table boundary.
    pub committed_log_offset: OffsetWatermark,
}

impl IngestWalRecovery {
    fn empty() -> Self {
        Self {
            records: Vec::new(),
            truncated_tail_bytes: 0,
            inspected_bytes: 0,
            inspected_files: 0,
            committed_log_offset: OffsetWatermark::Empty,
        }
    }
}

/// Canonical SHA-256 mutation fingerprint based on versioned Arrow IPC bytes.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArrowIpcMutationFingerprintComputer;

impl MutationFingerprintComputer for ArrowIpcMutationFingerprintComputer {
    fn compute(
        &self,
        format_version: MutationFingerprintFormatVersion,
        schema_version: u64,
        mutation: &TableMutation,
    ) -> Result<MutationFingerprintDigest> {
        if format_version.get() != CURRENT_INGEST_WAL_FORMAT_VERSION {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                format!(
                    "ingest mutation fingerprint format is unsupported version=[{}]",
                    format_version.get()
                ),
            )
            .with_retryable(false));
        }
        let (mutation_kind, batch) = mutation_parts(mutation);
        let payload = encode_batch(batch)?;
        let record_payload = canonical_record_payload(&payload)?;
        let mut digest = Sha256::new();
        digest.update(FINGERPRINT_DOMAIN);
        digest.update(format_version.get().to_be_bytes());
        digest.update(schema_version.to_be_bytes());
        digest.update([mutation_kind]);
        digest.update(canonical_catalog_schema_digest_v1(batch.schema().as_ref()));
        digest.update(
            u64::try_from(record_payload.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        digest.update(record_payload);
        Ok(MutationFingerprintDigest::from_bytes(
            digest.finalize().into(),
        ))
    }
}

/// Exclusive append and fsync owner for one producer-epoch WAL segment.
pub struct IngestWalWriter {
    config: IngestWalConfig,
    identity: IngestWalIdentity,
    path: PathBuf,
    file: File,
    file_len: u64,
    durable_len: u64,
    pending_records: u64,
    last_batch_sequence: Option<u64>,
    last_log_offset: OffsetWatermark,
    poisoned: bool,
}

impl IngestWalWriter {
    /// Creates a new producer-epoch segment and durably publishes its directory entry.
    pub fn create(
        wal_root: &Path,
        identity: IngestWalIdentity,
        config: IngestWalConfig,
    ) -> Result<Self> {
        Self::create_segment(wal_root, identity, WAL_SEGMENT_SEQUENCE, config)
    }

    /// Creates the next segment for a recovered or reconnected producer epoch.
    ///
    /// Callers must complete [`recover_table_wal`] before invoking this method. Recovery validates
    /// and truncates every existing segment; this method then reserves the next monotonically
    /// increasing segment number with `create_new`, so an exact acquire replay never appends to an
    /// unverified file.
    pub fn create_next(
        wal_root: &Path,
        identity: IngestWalIdentity,
        config: IngestWalConfig,
    ) -> Result<Self> {
        config.validate()?;
        let table_directory = table_wal_directory(wal_root, identity.table_id);
        fs::create_dir_all(&table_directory)
            .map_err(|error| wal_io_error("create table WAL directory", &table_directory, error))?;
        let mut maximum_segment = 0_u64;
        let mut matching_files = 0_usize;
        for entry in fs::read_dir(&table_directory)
            .map_err(|error| wal_io_error("read table WAL directory", &table_directory, error))?
        {
            let entry = entry.map_err(|error| {
                wal_io_error("read table WAL directory entry", &table_directory, error)
            })?;
            if !entry
                .file_type()
                .map_err(|error| wal_io_error("inspect WAL directory entry", &entry.path(), error))?
                .is_file()
            {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                wal_corruption(
                    identity.table_id,
                    &entry.path(),
                    "WAL file name is not UTF-8",
                )
            })?;
            let Some((epoch, segment)) = parse_wal_file_name(name) else {
                if name.ends_with(".wal") {
                    return Err(wal_corruption(
                        identity.table_id,
                        &entry.path(),
                        "WAL file name is malformed",
                    ));
                }
                continue;
            };
            matching_files = matching_files
                .checked_add(1)
                .ok_or_else(|| wal_space_exhausted(identity.table_id))?;
            if matching_files >= config.max_wal_files_per_table {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "ingest WAL file count exhausted limit maximum=[{}]",
                        config.max_wal_files_per_table
                    ),
                )
                .with_table_id(identity.table_id)
                .with_retryable(false));
            }
            if epoch == identity.producer_epoch {
                maximum_segment = maximum_segment.max(segment);
            }
        }
        let next_segment = maximum_segment
            .checked_add(1)
            .ok_or_else(|| wal_space_exhausted(identity.table_id))?;
        Self::create_segment(wal_root, identity, next_segment, config)
    }

    fn create_segment(
        wal_root: &Path,
        identity: IngestWalIdentity,
        segment: u64,
        config: IngestWalConfig,
    ) -> Result<Self> {
        config.validate()?;
        let table_directory = table_wal_directory(wal_root, identity.table_id);
        fs::create_dir_all(&table_directory)
            .map_err(|error| wal_io_error("create table WAL directory", &table_directory, error))?;
        let path = table_directory.join(wal_file_name(identity.producer_epoch, segment));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                let code = if error.kind() == std::io::ErrorKind::AlreadyExists {
                    ErrorCode::Conflict
                } else {
                    ErrorCode::Unavailable
                };
                ServerError::new(
                    code,
                    format!(
                        "create producer WAL segment path=[{}] error=[{}]",
                        path.display(),
                        error
                    ),
                )
                .with_table_id(identity.table_id)
                .with_retryable(false)
            })?;
        sync_directory(&table_directory)?;
        Ok(Self {
            config,
            identity,
            path,
            file,
            file_len: 0,
            durable_len: 0,
            pending_records: 0,
            last_batch_sequence: None,
            last_log_offset: OffsetWatermark::Empty,
            poisoned: false,
        })
    }

    /// Returns the immutable segment path for diagnostics and recovery tests.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one fingerprinted mutation without claiming that it is durable yet.
    pub fn append(
        &mut self,
        request: &WriteRequest,
        receipt: &WriteReceipt,
    ) -> Result<WalAppendPosition> {
        self.require_healthy()?;
        let encoded = match encode_data_record(self.identity, request, receipt, self.config) {
            Ok(encoded) => encoded,
            Err(error) => return Err(error.with_table_id(self.identity.table_id)),
        };
        let start = self.file_len;
        if let Err(error) = self.file.write_all(&encoded.header) {
            self.poisoned = true;
            return Err(wal_io_error("append WAL record header", &self.path, error)
                .with_table_id(self.identity.table_id));
        }
        if let Err(error) = self.file.write_all(&encoded.payload) {
            self.poisoned = true;
            return Err(wal_io_error("append WAL record payload", &self.path, error)
                .with_table_id(self.identity.table_id));
        }
        self.file_len = self
            .file_len
            .checked_add(u64::try_from(WAL_HEADER_LEN + encoded.payload.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| wal_space_exhausted(self.identity.table_id))?;
        self.pending_records = self
            .pending_records
            .checked_add(1)
            .ok_or_else(|| wal_space_exhausted(self.identity.table_id))?;
        self.last_batch_sequence = Some(request.batch_sequence());
        self.last_log_offset = receipt.committed_log_offset();
        Ok(WalAppendPosition {
            start,
            end: self.file_len,
        })
    }

    /// Removes the most recent uncommitted append after an atomic sink rejection.
    ///
    /// The supplied position must be the current file tail and must start at or after the latest
    /// durable commit marker. The caller restores the producer sequence and logical watermark that
    /// preceded the append. Any invariant or IO failure poisons the writer.
    pub fn rollback_uncommitted_append(
        &mut self,
        position: WalAppendPosition,
        previous_batch_sequence: Option<u64>,
        previous_log_offset: OffsetWatermark,
    ) -> Result<()> {
        self.require_healthy()?;
        if self.pending_records == 0
            || position.end != self.file_len
            || position.start < self.durable_len
            || position.start > position.end
        {
            self.poisoned = true;
            return Err(ServerError::new(
                ErrorCode::Internal,
                "ingest WAL rollback does not reference the current uncommitted tail",
            )
            .with_table_id(self.identity.table_id)
            .with_retryable(false));
        }
        if let Err(error) = self.file.set_len(position.start) {
            self.poisoned = true;
            return Err(
                wal_io_error("truncate rejected WAL append", &self.path, error)
                    .with_table_id(self.identity.table_id),
            );
        }
        if let Err(error) = self.file.seek(SeekFrom::Start(position.start)) {
            self.poisoned = true;
            return Err(
                wal_io_error("seek rejected WAL append boundary", &self.path, error)
                    .with_table_id(self.identity.table_id),
            );
        }
        self.file_len = position.start;
        self.pending_records -= 1;
        self.last_batch_sequence = previous_batch_sequence;
        self.last_log_offset = previous_log_offset;
        Ok(())
    }

    /// Appends a group commit marker and fsyncs every preceding data record.
    pub fn commit(&mut self) -> Result<WalCommitReceipt> {
        self.require_healthy()?;
        let Some(last_batch_sequence) = self.last_batch_sequence else {
            return Ok(WalCommitReceipt {
                durable_bytes: self.durable_len,
                committed_records: 0,
                last_batch_sequence: SequenceWatermark::Empty,
                committed_log_offset: self.last_log_offset,
            });
        };
        if self.pending_records == 0 {
            return Ok(WalCommitReceipt {
                durable_bytes: self.durable_len,
                committed_records: 0,
                last_batch_sequence: SequenceWatermark::Known(last_batch_sequence),
                committed_log_offset: self.last_log_offset,
            });
        }
        let header = encode_commit_marker(
            self.identity,
            last_batch_sequence,
            self.pending_records,
            self.last_log_offset,
        );
        if let Err(error) = self.file.write_all(&header) {
            self.poisoned = true;
            return Err(wal_io_error("append WAL commit marker", &self.path, error)
                .with_table_id(self.identity.table_id));
        }
        self.file_len = self
            .file_len
            .checked_add(WAL_HEADER_LEN as u64)
            .ok_or_else(|| wal_space_exhausted(self.identity.table_id))?;
        if let Err(error) = self.file.sync_data() {
            self.poisoned = true;
            return Err(wal_io_error("sync WAL commit group", &self.path, error)
                .with_table_id(self.identity.table_id));
        }
        let committed_records = self.pending_records;
        self.pending_records = 0;
        self.durable_len = self.file_len;
        Ok(WalCommitReceipt {
            durable_bytes: self.durable_len,
            committed_records,
            last_batch_sequence: SequenceWatermark::Known(last_batch_sequence),
            committed_log_offset: self.last_log_offset,
        })
    }

    fn require_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "ingest WAL writer is poisoned after an earlier I/O failure",
            )
            .with_table_id(self.identity.table_id)
            .with_retryable(false));
        }
        Ok(())
    }
}

/// Recovers every committed producer epoch for one table and truncates only incomplete tails.
pub fn recover_table_wal(
    wal_root: &Path,
    table_id: TableId,
    config: IngestWalConfig,
    fingerprints: &dyn MutationFingerprintComputer,
) -> Result<IngestWalRecovery> {
    config.validate()?;
    let table_directory = table_wal_directory(wal_root, table_id);
    let entries = match fs::read_dir(&table_directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(IngestWalRecovery::empty());
        }
        Err(error) => {
            return Err(
                wal_io_error("read table WAL directory", &table_directory, error)
                    .with_table_id(table_id),
            );
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            wal_io_error("read table WAL directory entry", &table_directory, error)
                .with_table_id(table_id)
        })?;
        let file_type = entry.file_type().map_err(|error| {
            wal_io_error("inspect table WAL directory entry", &entry.path(), error)
                .with_table_id(table_id)
        })?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| wal_corruption(table_id, &entry.path(), "WAL file name is not UTF-8"))?;
        if !name.ends_with(".wal") {
            continue;
        }
        let (epoch, segment) = parse_wal_file_name(name)
            .ok_or_else(|| wal_corruption(table_id, &entry.path(), "WAL file name is malformed"))?;
        files.push((epoch, segment, entry.path()));
    }
    files.sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    if files.len() > config.max_wal_files_per_table {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "ingest WAL file count exceeds recovery limit actual=[{}] maximum=[{}]",
                files.len(),
                config.max_wal_files_per_table
            ),
        )
        .with_table_id(table_id)
        .with_retryable(false));
    }

    let mut recovery = IngestWalRecovery::empty();
    for (epoch, _segment, path) in files {
        let file_bytes = fs::metadata(&path)
            .map_err(|error| wal_io_error("inspect WAL segment", &path, error))?
            .len();
        recovery.inspected_bytes = recovery
            .inspected_bytes
            .checked_add(file_bytes)
            .ok_or_else(|| wal_space_exhausted(table_id))?;
        if recovery.inspected_bytes > config.max_recovery_bytes {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "ingest WAL recovery bytes exceed limit actual=[{}] maximum=[{}]",
                    recovery.inspected_bytes, config.max_recovery_bytes
                ),
            )
            .with_table_id(table_id)
            .with_retryable(false));
        }
        let scanned = scan_wal_file(&path, table_id, epoch, config, fingerprints)?;
        recovery.truncated_tail_bytes = recovery
            .truncated_tail_bytes
            .saturating_add(scanned.truncated_tail_bytes);
        recovery.inspected_files = recovery.inspected_files.saturating_add(1);
        recovery.records.extend(scanned.records);
        if recovery.records.len() > config.max_recovery_records {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "ingest WAL recovery record count exceeds limit actual=[{}] maximum=[{}]",
                    recovery.records.len(),
                    config.max_recovery_records
                ),
            )
            .with_table_id(table_id)
            .with_retryable(false));
        }
    }
    validate_recovered_sequence(table_id, &recovery.records)?;
    recovery.committed_log_offset = recovery
        .records
        .last()
        .map_or(OffsetWatermark::Empty, |record| {
            record.receipt.committed_log_offset()
        });
    Ok(recovery)
}

struct EncodedWalRecord {
    header: [u8; WAL_HEADER_LEN],
    payload: Vec<u8>,
}

#[derive(Debug)]
struct ScannedWalFile {
    records: Vec<RecoveredWalRecord>,
    truncated_tail_bytes: u64,
}

#[derive(Debug)]
struct DecodedHeader {
    record_kind: u8,
    mutation_kind: u8,
    previous_log_offset: OffsetWatermark,
    offsets: Option<OffsetRange>,
    payload_len: usize,
    table_id: TableId,
    producer_id: Option<ProducerId>,
    request_id: Option<RequestId>,
    producer_epoch: u64,
    schema_version: u64,
    batch_sequence: u64,
    row_count: u64,
    fingerprint: Option<MutationFingerprint>,
    payload_crc32c: u32,
}

fn encode_data_record(
    identity: IngestWalIdentity,
    request: &WriteRequest,
    receipt: &WriteReceipt,
    config: IngestWalConfig,
) -> Result<EncodedWalRecord> {
    if request.request_id() != receipt.request_id()
        || request.batch_sequence() != receipt.batch_sequence()
        || request.mutation().row_count() != receipt.row_count()
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "ingest WAL request and receipt identities do not match",
        )
        .with_request_id(request.request_id())
        .with_retryable(false));
    }
    if receipt.durability_reached() == Durability::Persisted {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "ingest WAL cannot append a manifest-persisted receipt",
        )
        .with_request_id(request.request_id())
        .with_retryable(false));
    }
    let (mutation_kind, batch) = mutation_parts(request.mutation());
    let payload = encode_batch(batch)?;
    if payload.len() > config.max_record_bytes {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "ingest WAL record exceeds payload limit actual=[{}] maximum=[{}]",
                payload.len(),
                config.max_record_bytes
            ),
        )
        .with_request_id(request.request_id())
        .with_retryable(false));
    }
    let mut header = empty_header(WAL_RECORD_DATA);
    header[17] = mutation_kind;
    encode_watermark(&mut header, receipt.previous_log_offset());
    encode_offsets(&mut header, receipt.offsets());
    put_u64(&mut header, 24, payload.len() as u64);
    header[32..48].copy_from_slice(&identity.table_id.into_bytes());
    header[48..64].copy_from_slice(&identity.producer_id.into_bytes());
    header[64..80].copy_from_slice(&request.request_id().into_bytes());
    put_u64(&mut header, 80, identity.producer_epoch);
    put_u64(&mut header, 88, request.schema_version());
    put_u64(&mut header, 96, request.batch_sequence());
    put_u64(&mut header, 104, receipt.row_count());
    put_u32(
        &mut header,
        136,
        request.mutation_fingerprint().format_version.get(),
    );
    put_u32(&mut header, 140, crc32c(&payload));
    header[144..176].copy_from_slice(&request.mutation_fingerprint().digest.into_bytes());
    seal_header_crc(&mut header);
    Ok(EncodedWalRecord { header, payload })
}

fn encode_commit_marker(
    identity: IngestWalIdentity,
    last_batch_sequence: u64,
    committed_records: u64,
    committed_log_offset: OffsetWatermark,
) -> [u8; WAL_HEADER_LEN] {
    let mut header = empty_header(WAL_RECORD_COMMIT);
    encode_watermark(&mut header, committed_log_offset);
    header[32..48].copy_from_slice(&identity.table_id.into_bytes());
    header[48..64].copy_from_slice(&identity.producer_id.into_bytes());
    put_u64(&mut header, 80, identity.producer_epoch);
    put_u64(&mut header, 96, last_batch_sequence);
    put_u64(&mut header, 104, committed_records);
    seal_header_crc(&mut header);
    header
}

fn empty_header(record_kind: u8) -> [u8; WAL_HEADER_LEN] {
    let mut header = [0_u8; WAL_HEADER_LEN];
    header[0..8].copy_from_slice(WAL_MAGIC);
    put_u32(&mut header, 8, CURRENT_INGEST_WAL_FORMAT_VERSION);
    put_u32(&mut header, 12, WAL_HEADER_LEN as u32);
    header[16] = record_kind;
    header
}

fn seal_header_crc(header: &mut [u8; WAL_HEADER_LEN]) {
    put_u32(header, WAL_HEADER_CRC_OFFSET, 0);
    let checksum = crc32c(header);
    put_u32(header, WAL_HEADER_CRC_OFFSET, checksum);
}

fn scan_wal_file(
    path: &Path,
    expected_table_id: TableId,
    expected_epoch: u64,
    config: IngestWalConfig,
    fingerprints: &dyn MutationFingerprintComputer,
) -> Result<ScannedWalFile> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| wal_io_error("open WAL segment for recovery", path, error))?;
    let original_len = file
        .metadata()
        .map_err(|error| wal_io_error("inspect WAL recovery segment", path, error))?
        .len();
    let mut cursor = 0_u64;
    let mut last_commit_end = 0_u64;
    let mut committed_record_count = 0_usize;
    let mut candidates = Vec::new();
    let mut records_since_commit = 0_u64;
    let mut last_identity = None;
    let mut last_sequence = None;
    let mut last_watermark = OffsetWatermark::Empty;

    while cursor < original_len {
        let remaining = original_len - cursor;
        if remaining < WAL_HEADER_LEN as u64 {
            break;
        }
        file.seek(SeekFrom::Start(cursor))
            .map_err(|error| wal_io_error("seek WAL recovery header", path, error))?;
        let mut header_bytes = [0_u8; WAL_HEADER_LEN];
        file.read_exact(&mut header_bytes)
            .map_err(|error| wal_io_error("read WAL recovery header", path, error))?;
        let header = decode_header(path, expected_table_id, &header_bytes, config)?;
        if header.table_id != expected_table_id || header.producer_epoch != expected_epoch {
            return Err(wal_corruption(
                expected_table_id,
                path,
                "WAL record identity differs from its directory and file name",
            ));
        }
        let record_len = (WAL_HEADER_LEN as u64)
            .checked_add(header.payload_len as u64)
            .ok_or_else(|| wal_space_exhausted(expected_table_id))?;
        if remaining < record_len {
            break;
        }
        let mut payload = vec![0_u8; header.payload_len];
        if !payload.is_empty() {
            file.read_exact(&mut payload)
                .map_err(|error| wal_io_error("read WAL recovery payload", path, error))?;
            let actual_crc = crc32c(&payload);
            if actual_crc != header.payload_crc32c {
                return Err(wal_checksum_corruption(
                    expected_table_id,
                    path,
                    header.payload_crc32c,
                    actual_crc,
                ));
            }
        }
        let end = cursor + record_len;
        match header.record_kind {
            WAL_RECORD_DATA => {
                let record = decode_data_record(
                    path,
                    header,
                    payload,
                    WalAppendPosition { start: cursor, end },
                    fingerprints,
                )?;
                last_identity = Some(record.identity);
                last_sequence = Some(record.request.batch_sequence());
                last_watermark = record.receipt.committed_log_offset();
                records_since_commit = records_since_commit
                    .checked_add(1)
                    .ok_or_else(|| wal_space_exhausted(expected_table_id))?;
                candidates.push(record);
                if candidates.len() > config.max_recovery_records {
                    return Err(ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "ingest WAL segment record count exceeds recovery limit",
                    )
                    .with_table_id(expected_table_id)
                    .with_retryable(false));
                }
            }
            WAL_RECORD_COMMIT => {
                let identity = last_identity.ok_or_else(|| {
                    wal_corruption(
                        expected_table_id,
                        path,
                        "WAL commit marker does not follow a data record",
                    )
                })?;
                if header.producer_id != Some(identity.producer_id)
                    || last_sequence != Some(header.batch_sequence)
                    || header.row_count != records_since_commit
                    || header.previous_log_offset != last_watermark
                    || header.payload_len != 0
                    || header.offsets.is_some()
                    || header.request_id.is_some()
                    || header.fingerprint.is_some()
                {
                    return Err(wal_corruption(
                        expected_table_id,
                        path,
                        "WAL commit marker does not describe its preceding record group",
                    ));
                }
                committed_record_count = candidates.len();
                records_since_commit = 0;
                last_commit_end = end;
            }
            _ => {
                return Err(wal_corruption(
                    expected_table_id,
                    path,
                    "WAL record kind is unsupported",
                ));
            }
        }
        cursor = end;
    }

    candidates.truncate(committed_record_count);
    if last_commit_end != original_len {
        file.set_len(last_commit_end)
            .map_err(|error| wal_io_error("truncate incomplete WAL tail", path, error))?;
        file.sync_data()
            .map_err(|error| wal_io_error("sync truncated WAL tail", path, error))?;
    }
    Ok(ScannedWalFile {
        records: candidates,
        truncated_tail_bytes: original_len.saturating_sub(last_commit_end),
    })
}

fn decode_header(
    path: &Path,
    table_id: TableId,
    bytes: &[u8; WAL_HEADER_LEN],
    config: IngestWalConfig,
) -> Result<DecodedHeader> {
    if &bytes[0..8] != WAL_MAGIC {
        return Err(wal_corruption(table_id, path, "WAL magic is invalid"));
    }
    if read_u32(bytes, 8) != CURRENT_INGEST_WAL_FORMAT_VERSION {
        return Err(wal_corruption(
            table_id,
            path,
            "WAL format version is unsupported",
        ));
    }
    if read_u32(bytes, 12) as usize != WAL_HEADER_LEN {
        return Err(wal_corruption(
            table_id,
            path,
            "WAL header length is invalid",
        ));
    }
    let expected_crc = read_u32(bytes, WAL_HEADER_CRC_OFFSET);
    let mut crc_bytes = *bytes;
    put_u32(&mut crc_bytes, WAL_HEADER_CRC_OFFSET, 0);
    let actual_crc = crc32c(&crc_bytes);
    if expected_crc != actual_crc {
        return Err(wal_checksum_corruption(
            table_id,
            path,
            expected_crc,
            actual_crc,
        ));
    }
    if bytes[20..24].iter().any(|byte| *byte != 0)
        || bytes[188..WAL_HEADER_LEN].iter().any(|byte| *byte != 0)
    {
        return Err(wal_corruption(
            table_id,
            path,
            "WAL reserved header bytes are nonzero",
        ));
    }
    let payload_len = usize::try_from(read_u64(bytes, 24))
        .map_err(|_| wal_corruption(table_id, path, "WAL payload length exceeds usize"))?;
    if payload_len > config.max_record_bytes {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "ingest WAL recovery record exceeds payload limit actual=[{payload_len}] maximum=[{}]",
                config.max_record_bytes
            ),
        )
        .with_table_id(table_id)
        .with_retryable(false));
    }
    let decoded_table_id = decode_table_id(path, &bytes[32..48])?;
    let record_kind = bytes[16];
    let producer_id = if record_kind == WAL_RECORD_COMMIT || record_kind == WAL_RECORD_DATA {
        Some(decode_producer_id(path, table_id, &bytes[48..64])?)
    } else {
        None
    };
    let request_id = if bytes[64..80].iter().all(|byte| *byte == 0) {
        None
    } else {
        Some(decode_request_id(path, table_id, &bytes[64..80])?)
    };
    let offsets = decode_offsets(
        path,
        table_id,
        bytes[19],
        read_u64(bytes, 120),
        read_u64(bytes, 128),
    )?;
    let previous_log_offset = decode_watermark(path, table_id, bytes[18], read_u64(bytes, 112))?;
    let fingerprint_version = read_u32(bytes, 136);
    let fingerprint = if fingerprint_version == 0 && bytes[144..176].iter().all(|byte| *byte == 0) {
        None
    } else {
        let digest: [u8; 32] = bytes[144..176]
            .try_into()
            .expect("fixed WAL digest slice has exact width");
        Some(MutationFingerprint {
            format_version: MutationFingerprintFormatVersion::new(fingerprint_version),
            digest: MutationFingerprintDigest::from_bytes(digest),
        })
    };
    Ok(DecodedHeader {
        record_kind,
        mutation_kind: bytes[17],
        previous_log_offset,
        offsets,
        payload_len,
        table_id: decoded_table_id,
        producer_id,
        request_id,
        producer_epoch: read_u64(bytes, 80),
        schema_version: read_u64(bytes, 88),
        batch_sequence: read_u64(bytes, 96),
        row_count: read_u64(bytes, 104),
        fingerprint,
        payload_crc32c: read_u32(bytes, 140),
    })
}

fn decode_data_record(
    path: &Path,
    header: DecodedHeader,
    payload: Vec<u8>,
    position: WalAppendPosition,
    fingerprints: &dyn MutationFingerprintComputer,
) -> Result<RecoveredWalRecord> {
    if header.record_kind != WAL_RECORD_DATA
        || header.payload_len == 0
        || header.producer_id.is_none()
        || header.request_id.is_none()
        || header.fingerprint.is_none()
    {
        return Err(wal_corruption(
            header.table_id,
            path,
            "WAL data record omits required identity or payload fields",
        ));
    }
    let batch = decode_batch(path, header.table_id, &payload)?;
    let mutation = match header.mutation_kind {
        MUTATION_APPEND => TableMutation::Append(batch),
        MUTATION_FRAME => TableMutation::Frame(batch),
        MUTATION_KEY_VALUE_UPSERT => TableMutation::KeyValue(KeyValueMutation::Upsert(batch)),
        MUTATION_KEY_VALUE_DELETE => TableMutation::KeyValue(KeyValueMutation::Delete(batch)),
        _ => {
            return Err(wal_corruption(
                header.table_id,
                path,
                "WAL mutation kind is unsupported",
            ));
        }
    };
    if mutation.row_count() != header.row_count {
        return Err(wal_corruption(
            header.table_id,
            path,
            "WAL Arrow row count differs from its record header",
        ));
    }
    let request_id = header.request_id.expect("validated data request id");
    let request = fingerprints.restore(
        request_id,
        header.schema_version,
        header.batch_sequence,
        mutation,
        header.fingerprint.expect("validated data fingerprint"),
    )?;
    let committed_log_offset = header.offsets.map_or(header.previous_log_offset, |range| {
        OffsetWatermark::Known(range.last())
    });
    let receipt = WriteReceipt::new(
        request_id,
        header.batch_sequence,
        header.row_count,
        header.offsets,
        header.previous_log_offset,
        committed_log_offset,
        Durability::Wal,
        None,
    )
    .map_err(|error| {
        wal_corruption(
            header.table_id,
            path,
            &format!("WAL write receipt invariants failed error=[{error}]"),
        )
    })?;
    Ok(RecoveredWalRecord {
        identity: IngestWalIdentity {
            table_id: header.table_id,
            producer_id: header.producer_id.expect("validated data producer id"),
            producer_epoch: header.producer_epoch,
        },
        request,
        receipt,
        position,
    })
}

fn validate_recovered_sequence(table_id: TableId, records: &[RecoveredWalRecord]) -> Result<()> {
    let mut previous_epoch: Option<u64> = None;
    let mut previous_producer: Option<ProducerId> = None;
    let mut previous_sequence: Option<u64> = None;
    let mut previous_offset: Option<OffsetWatermark> = None;
    for record in records {
        if record.identity.table_id != table_id {
            return Err(wal_corruption(
                table_id,
                Path::new("<table-recovery>"),
                "recovered WAL record belongs to another table",
            ));
        }
        match previous_epoch {
            Some(epoch) if epoch == record.identity.producer_epoch => {
                if previous_producer != Some(record.identity.producer_id) {
                    return Err(wal_corruption(
                        table_id,
                        Path::new("<table-recovery>"),
                        "one WAL producer epoch contains multiple producer identities",
                    ));
                }
                let expected = previous_sequence
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or_else(|| wal_space_exhausted(table_id))?;
                if record.request.batch_sequence() != expected {
                    return Err(sequence_corruption(
                        table_id,
                        expected,
                        record.request.batch_sequence(),
                    ));
                }
            }
            Some(epoch) if record.identity.producer_epoch <= epoch => {
                return Err(wal_corruption(
                    table_id,
                    Path::new("<table-recovery>"),
                    "WAL producer epochs are not strictly increasing",
                ));
            }
            _ => {
                if record.request.batch_sequence() != 0 {
                    return Err(sequence_corruption(
                        table_id,
                        0,
                        record.request.batch_sequence(),
                    ));
                }
            }
        }
        if let Some(expected_previous) = previous_offset {
            if record.receipt.previous_log_offset() != expected_previous {
                return Err(wal_corruption(
                    table_id,
                    Path::new("<table-recovery>"),
                    "WAL logical offsets are not contiguous across records",
                ));
            }
        }
        previous_epoch = Some(record.identity.producer_epoch);
        previous_producer = Some(record.identity.producer_id);
        previous_sequence = Some(record.request.batch_sequence());
        previous_offset = Some(record.receipt.committed_log_offset());
    }
    Ok(())
}

fn mutation_parts(mutation: &TableMutation) -> (u8, &RecordBatch) {
    match mutation {
        TableMutation::Append(batch) => (MUTATION_APPEND, batch),
        TableMutation::Frame(batch) => (MUTATION_FRAME, batch),
        TableMutation::KeyValue(KeyValueMutation::Upsert(batch)) => {
            (MUTATION_KEY_VALUE_UPSERT, batch)
        }
        TableMutation::KeyValue(KeyValueMutation::Delete(batch)) => {
            (MUTATION_KEY_VALUE_DELETE, batch)
        }
    }
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut payload, batch.schema().as_ref()).map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("encode ingest WAL Arrow schema error=[{error}]"),
            )
            .with_retryable(false)
        })?;
    writer.write(batch).map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("encode ingest WAL Arrow batch error=[{error}]"),
        )
        .with_retryable(false)
    })?;
    writer.finish().map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("finish ingest WAL Arrow stream error=[{error}]"),
        )
        .with_retryable(false)
    })?;
    drop(writer);
    Ok(payload)
}

fn canonical_record_payload(payload: &[u8]) -> Result<&[u8]> {
    if payload.len() < 8 || payload[0..4] != [0xff; 4] {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "ingest WAL Arrow stream omits the canonical continuation marker",
        )
        .with_retryable(false));
    }
    let metadata_len = i32::from_le_bytes(
        payload[4..8]
            .try_into()
            .expect("fixed Arrow metadata length slice has exact width"),
    );
    if metadata_len <= 0 {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "ingest WAL Arrow stream has an invalid schema metadata length",
        )
        .with_retryable(false));
    }
    let schema_end = 8 + usize::try_from(metadata_len).expect("positive i32 fits usize");
    if schema_end >= payload.len() {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "ingest WAL Arrow stream contains no record payload after its schema",
        )
        .with_retryable(false));
    }
    Ok(&payload[schema_end..])
}

struct TrackedSliceReader<'a> {
    payload: &'a [u8],
    position: Rc<Cell<usize>>,
}

impl Read for TrackedSliceReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let position = self.position.get();
        let remaining = &self.payload[position..];
        let read_len = remaining.len().min(output.len());
        output[..read_len].copy_from_slice(&remaining[..read_len]);
        self.position.set(position + read_len);
        Ok(read_len)
    }
}

fn decode_batch(path: &Path, table_id: TableId, payload: &[u8]) -> Result<RecordBatch> {
    let position = Rc::new(Cell::new(0));
    let tracked = TrackedSliceReader {
        payload,
        position: Rc::clone(&position),
    };
    let mut reader = StreamReader::try_new(tracked, None).map_err(|error| {
        wal_corruption(
            table_id,
            path,
            &format!("decode WAL Arrow stream error=[{error}]"),
        )
    })?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| {
            wal_corruption(
                table_id,
                path,
                &format!("decode WAL Arrow batch error=[{error}]"),
            )
        })?
        .ok_or_else(|| wal_corruption(table_id, path, "WAL Arrow stream has no record batch"))?;
    if reader
        .next()
        .transpose()
        .map_err(|error| {
            wal_corruption(
                table_id,
                path,
                &format!("decode trailing WAL Arrow batch error=[{error}]"),
            )
        })?
        .is_some()
    {
        return Err(wal_corruption(
            table_id,
            path,
            "WAL Arrow stream contains more than one record batch",
        ));
    }
    drop(reader);
    if position.get() != payload.len() {
        return Err(wal_corruption(
            table_id,
            path,
            "WAL Arrow payload contains trailing bytes after the stream terminator",
        ));
    }
    Ok(batch)
}

fn encode_watermark(header: &mut [u8; WAL_HEADER_LEN], watermark: OffsetWatermark) {
    let (state, value) = match watermark {
        OffsetWatermark::Empty => (WATERMARK_EMPTY, 0),
        OffsetWatermark::Known(offset) => (WATERMARK_KNOWN, offset.get()),
        OffsetWatermark::LegacyUnknown => (WATERMARK_LEGACY_UNKNOWN, 0),
    };
    header[18] = state;
    put_u64(header, 112, value);
}

fn decode_watermark(
    path: &Path,
    table_id: TableId,
    state: u8,
    value: u64,
) -> Result<OffsetWatermark> {
    match (state, value) {
        (WATERMARK_EMPTY, 0) => Ok(OffsetWatermark::Empty),
        (WATERMARK_KNOWN, value) => Ok(OffsetWatermark::Known(LogOffset::new(value))),
        (WATERMARK_LEGACY_UNKNOWN, 0) => Ok(OffsetWatermark::LegacyUnknown),
        _ => Err(wal_corruption(
            table_id,
            path,
            "WAL previous watermark encoding is invalid",
        )),
    }
}

fn encode_offsets(header: &mut [u8; WAL_HEADER_LEN], offsets: Option<OffsetRange>) {
    match offsets {
        None => {
            header[19] = OFFSETS_NONE;
            put_u64(header, 120, 0);
            put_u64(header, 128, 0);
        }
        Some(offsets) => {
            header[19] = OFFSETS_PRESENT;
            put_u64(header, 120, offsets.first().get());
            put_u64(header, 128, offsets.last().get());
        }
    }
}

fn decode_offsets(
    path: &Path,
    table_id: TableId,
    state: u8,
    first: u64,
    last: u64,
) -> Result<Option<OffsetRange>> {
    match (state, first, last) {
        (OFFSETS_NONE, 0, 0) => Ok(None),
        (OFFSETS_PRESENT, first, last) => {
            OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
                .map(Some)
                .map_err(|_| wal_corruption(table_id, path, "WAL offset range is reversed"))
        }
        _ => Err(wal_corruption(
            table_id,
            path,
            "WAL offset range encoding is invalid",
        )),
    }
}

fn table_wal_directory(wal_root: &Path, table_id: TableId) -> PathBuf {
    wal_root.join(table_id.to_string())
}

fn wal_file_name(producer_epoch: u64, segment: u64) -> String {
    format!("{producer_epoch:020}-{segment:020}.wal")
}

fn parse_wal_file_name(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".wal")?;
    let (epoch, segment) = stem.split_once('-')?;
    if epoch.len() != 20
        || segment.len() != 20
        || !epoch.bytes().all(|byte| byte.is_ascii_digit())
        || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((epoch.parse().ok()?, segment.parse().ok()?))
}

fn decode_table_id(path: &Path, bytes: &[u8]) -> Result<TableId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .expect("fixed WAL table id has exact width");
    TableId::from_bytes(bytes)
        .map_err(|_| wal_corruption_raw(path, "WAL table id is zero or invalid"))
}

fn decode_producer_id(path: &Path, table_id: TableId, bytes: &[u8]) -> Result<ProducerId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .expect("fixed WAL producer id has exact width");
    ProducerId::from_bytes(bytes)
        .map_err(|_| wal_corruption(table_id, path, "WAL producer id is zero or invalid"))
}

fn decode_request_id(path: &Path, table_id: TableId, bytes: &[u8]) -> Result<RequestId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .expect("fixed WAL request id has exact width");
    RequestId::from_bytes(bytes)
        .map_err(|_| wal_corruption(table_id, path, "WAL request id is zero or invalid"))
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed WAL u32 field has exact width"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed WAL u64 field has exact width"),
    )
}

const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    while index < table.len() {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                0x82F6_3B78_u32 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut checksum = u32::MAX;
    for byte in bytes {
        let index = ((checksum as u8) ^ byte) as usize;
        checksum = CRC32C_TABLE[index] ^ (checksum >> 8);
    }
    !checksum
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| wal_io_error("sync WAL directory", path, error))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn invalid_config(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn wal_space_exhausted(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "ingest WAL byte, record, or sequence domain is exhausted",
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

fn wal_io_error(action: &str, path: &Path, error: std::io::Error) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!("{action} path=[{}] error=[{error}]", path.display()),
    )
    .with_retryable(true)
}

fn wal_corruption(table_id: TableId, path: &Path, reason: &str) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "ingest WAL corruption path=[{}] reason=[{reason}]",
            path.display()
        ),
    )
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::DataCorruption {
        object: path.display().to_string(),
        expected_checksum: None,
        actual_checksum: None,
    })
    .expect("WAL corruption details match the data corruption error code")
}

fn wal_corruption_raw(path: &Path, reason: &str) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "ingest WAL corruption path=[{}] reason=[{reason}]",
            path.display()
        ),
    )
    .with_retryable(false)
}

fn wal_checksum_corruption(
    table_id: TableId,
    path: &Path,
    expected: u32,
    actual: u32,
) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "ingest WAL checksum mismatch path=[{}] expected=[{expected:08x}] actual=[{actual:08x}]",
            path.display()
        ),
    )
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::DataCorruption {
        object: path.display().to_string(),
        expected_checksum: Some(format!("crc32c:{expected:08x}")),
        actual_checksum: Some(format!("crc32c:{actual:08x}")),
    })
    .expect("WAL checksum details match the data corruption error code")
}

fn sequence_corruption(table_id: TableId, expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::SequenceGap,
        format!("recovered WAL sequence gap expected=[{expected}] actual=[{actual}]"),
    )
    .with_table_id(table_id)
    .with_retryable(false)
    .with_details(ServerErrorDetails::SequenceGap { expected, actual })
    .expect("WAL sequence details match the sequence gap error code")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use tempfile::TempDir;
    use zippy_types::ingest::{
        Durability, IncomingWriteRequest, MutationFingerprintComputer,
        MutationFingerprintFormatVersion, TableMutation, WriteReceipt, WriteRequest,
    };
    use zippy_types::{
        ErrorCode, LogOffset, OffsetRange, OffsetWatermark, ProducerId, RequestId,
        SequenceWatermark, TableId,
    };

    use super::{
        crc32c, decode_batch, encode_batch, recover_table_wal, ArrowIpcMutationFingerprintComputer,
        IngestWalConfig, IngestWalIdentity, IngestWalWriter, WAL_HEADER_LEN,
    };

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).unwrap()
    }

    fn producer_id(value: u128) -> ProducerId {
        ProducerId::from_u128(value).unwrap()
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).unwrap()
    }

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn request(sequence: u64, values: &[i64]) -> WriteRequest {
        ArrowIpcMutationFingerprintComputer
            .prepare(IncomingWriteRequest {
                request_id: request_id(100 + u128::from(sequence)),
                schema_version: 1,
                batch_sequence: sequence,
                mutation: TableMutation::Append(batch(values)),
            })
            .unwrap()
    }

    fn receipt(request: &WriteRequest, previous: OffsetWatermark) -> WriteReceipt {
        let rows = request.mutation().row_count();
        let offsets = if rows == 0 {
            None
        } else {
            let first = match previous {
                OffsetWatermark::Empty => LogOffset::new(0),
                OffsetWatermark::Known(offset) => offset.checked_next().unwrap(),
                OffsetWatermark::LegacyUnknown => panic!("test does not use legacy offsets"),
            };
            Some(
                OffsetRange::new(
                    first,
                    LogOffset::new(first.get().checked_add(rows - 1).unwrap()),
                )
                .unwrap(),
            )
        };
        let committed = offsets.map_or(previous, |range| OffsetWatermark::Known(range.last()));
        WriteReceipt::new(
            request.request_id(),
            request.batch_sequence(),
            rows,
            offsets,
            previous,
            committed,
            Durability::Memory,
            None,
        )
        .unwrap()
    }

    fn identity(epoch: u64) -> IngestWalIdentity {
        IngestWalIdentity {
            table_id: table_id(1),
            producer_id: producer_id(2),
            producer_epoch: epoch,
        }
    }

    #[test]
    fn crc32c_matches_the_castagnoli_check_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn arrow_payload_rejects_trailing_noncanonical_bytes() {
        let mut payload = encode_batch(&batch(&[1, 2])).unwrap();
        payload.extend_from_slice(b"trailing");
        let error = decode_batch(Path::new("<test-wal>"), table_id(1), &payload).unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn committed_records_recover_and_complete_uncommitted_tail_is_truncated() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("wal");
        let mut writer =
            IngestWalWriter::create(&root, identity(1), IngestWalConfig::new()).unwrap();
        let first = request(0, &[10, 11]);
        let first_receipt = receipt(&first, OffsetWatermark::Empty);
        writer.append(&first, &first_receipt).unwrap();
        let second = request(1, &[12]);
        let second_receipt = receipt(&second, first_receipt.committed_log_offset());
        writer.append(&second, &second_receipt).unwrap();
        let committed = writer.commit().unwrap();
        assert_eq!(committed.committed_records, 2);
        assert_eq!(committed.last_batch_sequence, SequenceWatermark::Known(1));
        let durable_bytes = committed.durable_bytes;
        let already_durable = writer.commit().unwrap();
        assert_eq!(already_durable.committed_records, 0);
        assert_eq!(already_durable.durable_bytes, durable_bytes);
        assert_eq!(
            already_durable.last_batch_sequence,
            SequenceWatermark::Known(1)
        );

        let third = request(2, &[13, 14]);
        let third_receipt = receipt(&third, second_receipt.committed_log_offset());
        writer.append(&third, &third_receipt).unwrap();
        let path = writer.path().to_path_buf();
        let uncommitted_len = std::fs::metadata(&path).unwrap().len();
        assert!(uncommitted_len > durable_bytes);
        drop(writer);

        let recovery = recover_table_wal(
            &root,
            table_id(1),
            IngestWalConfig::new(),
            &ArrowIpcMutationFingerprintComputer,
        )
        .unwrap();
        assert_eq!(recovery.records.len(), 2);
        assert_eq!(recovery.records[0].request.batch_sequence(), 0);
        assert_eq!(recovery.records[1].request.batch_sequence(), 1);
        assert_eq!(
            recovery.records[1].receipt.durability_reached(),
            Durability::Wal
        );
        assert_eq!(
            recovery.truncated_tail_bytes,
            uncommitted_len - durable_bytes
        );
        assert_eq!(std::fs::metadata(path).unwrap().len(), durable_bytes);
    }

    #[test]
    fn exact_epoch_reconnect_uses_the_next_segment_and_recovers_one_sequence() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("wal");
        let mut first_writer =
            IngestWalWriter::create(&root, identity(1), IngestWalConfig::new()).unwrap();
        let first = request(0, &[10]);
        let first_receipt = receipt(&first, OffsetWatermark::Empty);
        first_writer.append(&first, &first_receipt).unwrap();
        first_writer.commit().unwrap();
        let first_path = first_writer.path().to_path_buf();
        drop(first_writer);

        let mut second_writer =
            IngestWalWriter::create_next(&root, identity(1), IngestWalConfig::new()).unwrap();
        let second = request(1, &[11, 12]);
        let second_receipt = receipt(&second, first_receipt.committed_log_offset());
        second_writer.append(&second, &second_receipt).unwrap();
        second_writer.commit().unwrap();
        let second_path = second_writer.path().to_path_buf();
        drop(second_writer);

        assert_ne!(first_path, second_path);
        assert!(first_path.ends_with("00000000000000000001-00000000000000000001.wal"));
        assert!(second_path.ends_with("00000000000000000001-00000000000000000002.wal"));
        let recovery = recover_table_wal(
            &root,
            table_id(1),
            IngestWalConfig::new(),
            &ArrowIpcMutationFingerprintComputer,
        )
        .unwrap();
        assert_eq!(recovery.records.len(), 2);
        assert_eq!(recovery.records[0].request.batch_sequence(), 0);
        assert_eq!(recovery.records[1].request.batch_sequence(), 1);
        assert_eq!(
            recovery.committed_log_offset,
            second_receipt.committed_log_offset()
        );
    }

    #[test]
    fn rejected_sink_append_rolls_back_only_the_current_uncommitted_tail() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("wal");
        let mut writer =
            IngestWalWriter::create(&root, identity(1), IngestWalConfig::new()).unwrap();
        let first = request(0, &[10]);
        let first_receipt = receipt(&first, OffsetWatermark::Empty);
        writer.append(&first, &first_receipt).unwrap();
        let first_commit = writer.commit().unwrap();

        let rejected = request(1, &[11]);
        let rejected_receipt = receipt(&rejected, first_receipt.committed_log_offset());
        let rejected_position = writer.append(&rejected, &rejected_receipt).unwrap();
        writer
            .rollback_uncommitted_append(
                rejected_position,
                Some(0),
                first_receipt.committed_log_offset(),
            )
            .unwrap();
        assert_eq!(
            std::fs::metadata(writer.path()).unwrap().len(),
            first_commit.durable_bytes
        );

        let accepted = request(1, &[12, 13]);
        let accepted_receipt = receipt(&accepted, first_receipt.committed_log_offset());
        writer.append(&accepted, &accepted_receipt).unwrap();
        writer.commit().unwrap();
        drop(writer);

        let recovery = recover_table_wal(
            &root,
            table_id(1),
            IngestWalConfig::new(),
            &ArrowIpcMutationFingerprintComputer,
        )
        .unwrap();
        assert_eq!(recovery.records.len(), 2);
        assert_eq!(recovery.records[1].request.batch_sequence(), 1);
        assert_eq!(recovery.records[1].request.mutation().row_count(), 2);
        assert_eq!(
            recovery.committed_log_offset,
            accepted_receipt.committed_log_offset()
        );
    }

    #[test]
    fn partial_tail_is_truncated_but_checksum_corruption_fails_closed() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("wal");
        let mut writer =
            IngestWalWriter::create(&root, identity(1), IngestWalConfig::new()).unwrap();
        let first = request(0, &[10]);
        let first_receipt = receipt(&first, OffsetWatermark::Empty);
        writer.append(&first, &first_receipt).unwrap();
        let committed = writer.commit().unwrap();
        let path = writer.path().to_path_buf();
        drop(writer);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0x5a; WAL_HEADER_LEN / 2])
            .unwrap();
        let recovered = recover_table_wal(
            &root,
            table_id(1),
            IngestWalConfig::new(),
            &ArrowIpcMutationFingerprintComputer,
        )
        .unwrap();
        assert_eq!(recovered.records.len(), 1);
        assert_eq!(recovered.truncated_tail_bytes, (WAL_HEADER_LEN / 2) as u64);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            committed.durable_bytes
        );

        let mut corrupt = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        corrupt.seek(SeekFrom::Start(144)).unwrap();
        corrupt.write_all(&[0xff]).unwrap();
        corrupt.sync_data().unwrap();
        drop(corrupt);
        let error = recover_table_wal(
            &root,
            table_id(1),
            IngestWalConfig::new(),
            &ArrowIpcMutationFingerprintComputer,
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn fingerprint_distinguishes_schema_and_mutation_kind() {
        let batch = batch(&[1, 2]);
        let computer = ArrowIpcMutationFingerprintComputer;
        let append = computer
            .compute(
                MutationFingerprintFormatVersion::current(),
                1,
                &TableMutation::Append(batch.clone()),
            )
            .unwrap();
        let schema_changed = computer
            .compute(
                MutationFingerprintFormatVersion::current(),
                2,
                &TableMutation::Append(batch.clone()),
            )
            .unwrap();
        let frame = computer
            .compute(
                MutationFingerprintFormatVersion::current(),
                1,
                &TableMutation::Frame(batch),
            )
            .unwrap();
        assert_ne!(append, schema_changed);
        assert_ne!(append, frame);
    }

    #[test]
    fn fingerprint_is_independent_from_schema_metadata_map_order() {
        let mut schema_metadata_left = HashMap::new();
        schema_metadata_left.insert("source".to_owned(), "tgw".to_owned());
        schema_metadata_left.insert("version".to_owned(), "1".to_owned());
        let mut schema_metadata_right = HashMap::new();
        schema_metadata_right.insert("version".to_owned(), "1".to_owned());
        schema_metadata_right.insert("source".to_owned(), "tgw".to_owned());
        let mut field_metadata_left = HashMap::new();
        field_metadata_left.insert("unit".to_owned(), "cny".to_owned());
        field_metadata_left.insert("scale".to_owned(), "4".to_owned());
        let mut field_metadata_right = HashMap::new();
        field_metadata_right.insert("scale".to_owned(), "4".to_owned());
        field_metadata_right.insert("unit".to_owned(), "cny".to_owned());

        let left_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int64, false).with_metadata(field_metadata_left)],
            schema_metadata_left,
        ));
        let right_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int64, false).with_metadata(field_metadata_right)],
            schema_metadata_right,
        ));
        let left = RecordBatch::try_new(
            left_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let right = RecordBatch::try_new(
            right_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let computer = ArrowIpcMutationFingerprintComputer;
        let left = computer
            .compute(
                MutationFingerprintFormatVersion::current(),
                1,
                &TableMutation::Append(left),
            )
            .unwrap();
        let right = computer
            .compute(
                MutationFingerprintFormatVersion::current(),
                1,
                &TableMutation::Append(right),
            )
            .unwrap();
        assert_eq!(left, right);
    }
}
