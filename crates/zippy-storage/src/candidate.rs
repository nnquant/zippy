use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{arrow_to_parquet_schema, ArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::format::KeyValue;
use serde::{Deserialize, Serialize};
use zippy_core::{DataDirectoryError, DurableFileTransaction, OwnedDataDirectory};
use zippy_types::table::{
    DataFileLocation, DataFileProvenance, ManagedDataPath, MetadataMap, MetadataScalar,
    NativeDataFileProvenance, PendingDataFile,
};
use zippy_types::{ErrorCode, FileId, Result, ServerError, ServerErrorDetails, TableId};

use crate::checksum::sha256_v1_reader;

/// Version of the Zippy metadata envelope embedded in candidate Parquet footers.
pub const PARQUET_CANDIDATE_FORMAT_VERSION: u32 = 1;

/// Parquet key containing the JSON-encoded candidate recovery envelope.
pub const CANDIDATE_FOOTER_METADATA_KEY: &str = "zippy.candidate.metadata.v1";

const PARQUET_CREATED_BY: &str = "zippy-storage/parquet-candidate-v1";
const MAX_ROW_GROUP_ROWS: usize = 128 * 1024;
const DATA_PAGE_SIZE_BYTES: usize = 1024 * 1024;
const DATA_PAGE_ROWS: usize = 20_000;
const DICTIONARY_PAGE_SIZE_BYTES: usize = 1024 * 1024;
const WRITE_BATCH_ROWS: usize = 1024;
const METADATA_TRUNCATE_BYTES: usize = 64;
const MAX_CANDIDATE_FOOTER_METADATA_BYTES: usize = 1024 * 1024;
const CANDIDATE_FOOTER_BUFFER_CHUNK_BYTES: usize = 64 * 1024;
const MAX_PARQUET_FOOTER_BYTES: u64 = 64 * 1024 * 1024;

/// Immutable metadata fixed before writing one native Parquet candidate.
#[derive(Debug, Clone)]
pub struct ParquetCandidateSpec {
    /// Identity assigned before any bytes are written.
    pub file_id: FileId,
    /// Table that owns the candidate.
    pub table_id: TableId,
    /// Nonzero table schema version encoded by every batch.
    pub schema_version: u64,
    /// Exact Arrow schema expected from every input batch.
    pub schema: SchemaRef,
    /// Exact native offset coverage and source-segment lineage.
    pub provenance: NativeDataFileProvenance,
    /// Stable partition values recorded in the Catalog manifest.
    pub partition: MetadataMap,
    /// Additional implementation-independent file metadata.
    pub metadata: MetadataMap,
    /// Candidate creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
}

/// A validated candidate specification paired with its bounded footer bytes.
///
/// Keeping the opaque pair together prevents the persist queue from retaining
/// metadata that has not passed the footer limit and prevents a writer from
/// accidentally attaching a footer generated for another specification.
#[derive(Debug)]
pub(crate) struct PreparedParquetCandidateSpec {
    spec: ParquetCandidateSpec,
    footer_metadata: String,
}

impl PreparedParquetCandidateSpec {
    pub(crate) fn new(spec: ParquetCandidateSpec) -> Result<Self> {
        validate_spec(&spec)?;
        validate_parquet_schema(&spec)?;
        let footer_metadata = encode_footer_metadata(&spec)?;
        Ok(Self {
            spec,
            footer_metadata,
        })
    }

    pub(crate) const fn spec(&self) -> &ParquetCandidateSpec {
        &self.spec
    }

    pub(crate) fn footer_capacity(&self) -> usize {
        self.footer_metadata.capacity()
    }

    fn into_parts(self) -> (ParquetCandidateSpec, String) {
        (self.spec, self.footer_metadata)
    }
}

/// A complete, durable Parquet file that is not yet Catalog-visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableParquetCandidate {
    pending: PendingDataFile,
    path: PathBuf,
}

impl DurableParquetCandidate {
    /// Borrows the exact descriptor accepted by `CommitDataFiles`.
    #[must_use]
    pub const fn pending(&self) -> &PendingDataFile {
        &self.pending
    }

    /// Returns the absolute local path of the verified published file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consumes the durable proof and returns its Catalog candidate descriptor.
    #[must_use]
    pub fn into_pending(self) -> PendingDataFile {
        self.pending
    }
}

/// Writes immutable native Parquet candidates under one owned data root.
#[derive(Debug)]
pub struct ParquetCandidateWriter {
    data_directory: Arc<OwnedDataDirectory>,
}

impl ParquetCandidateWriter {
    /// Creates a writer that retains data-root ownership for its full lifetime.
    #[must_use]
    pub fn new(data_directory: Arc<OwnedDataDirectory>) -> Self {
        Self { data_directory }
    }

    /// Writes, checksums, and durably publishes one native Parquet candidate.
    ///
    /// The target path is deterministically
    /// `parquet/<table_id>/<file_id>.parquet`. Repeating the same file identity
    /// with byte-identical content returns the existing candidate. Different
    /// content under the same identity fails without replacing the file.
    pub fn write_candidate<I>(
        &self,
        spec: ParquetCandidateSpec,
        batches: I,
    ) -> Result<DurableParquetCandidate>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        self.write_prepared_candidate(PreparedParquetCandidateSpec::new(spec)?, batches)
    }

    pub(crate) fn write_prepared_candidate<I>(
        &self,
        prepared: PreparedParquetCandidateSpec,
        batches: I,
    ) -> Result<DurableParquetCandidate>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let (spec, footer_metadata) = prepared.into_parts();
        let managed_path = candidate_managed_path(spec.table_id, spec.file_id)?;
        let final_path = self
            .data_directory
            .directory()
            .root()
            .join(managed_path.as_str());
        let mut transaction = self
            .data_directory
            .begin_durable_file(&managed_path)
            .map_err(|error| map_data_directory_error(spec.table_id, error))?;

        let (row_count, row_group_count) =
            write_parquet_batches(&mut transaction, &spec, batches, footer_metadata)?;
        let expected_size = transaction
            .file_mut()
            .metadata()
            .map_err(|error| io_error(spec.table_id, "read candidate size", &final_path, error))?
            .len();
        let expected_checksum = checksum_transaction(&mut transaction, spec.table_id)?;
        let statistics = candidate_statistics(row_group_count, spec.schema.fields().len())?;

        match transaction.publish_noclobber() {
            Ok(published) => {
                if published.size_bytes() != expected_size {
                    return Err(ServerError::new(
                        ErrorCode::DataCorruption,
                        format!(
                            "published candidate size changed path=[{}] expected=[{}] actual=[{}]",
                            published.path().display(),
                            expected_size,
                            published.size_bytes()
                        ),
                    )
                    .with_table_id(spec.table_id));
                }
                build_candidate(
                    spec,
                    managed_path,
                    published.path().to_path_buf(),
                    expected_size,
                    row_count,
                    statistics,
                    expected_checksum,
                )
            }
            Err(DataDirectoryError::AlreadyExists { .. }) => {
                let (actual_size, actual_checksum) =
                    inspect_existing_candidate(spec.table_id, &final_path)?;
                if actual_size != expected_size || actual_checksum != expected_checksum {
                    return Err(corruption_error(
                        spec.table_id,
                        &final_path,
                        Some(expected_checksum),
                        Some(actual_checksum),
                        "file identity already exists with different content",
                    ));
                }
                build_candidate(
                    spec,
                    managed_path,
                    final_path,
                    actual_size,
                    row_count,
                    statistics,
                    actual_checksum,
                )
            }
            Err(error) => Err(map_data_directory_error(spec.table_id, error)),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCandidateMetadataV1 {
    format_version: u32,
    file_id: FileId,
    table_id: TableId,
    schema_version: u64,
    provenance: NativeDataFileProvenance,
    partition: MetadataMap,
    metadata: MetadataMap,
    created_at_ns: u64,
}

fn validate_spec(spec: &ParquetCandidateSpec) -> Result<()> {
    if spec.schema_version == 0 {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "candidate schema version must be nonzero",
        )
        .with_table_id(spec.table_id));
    }
    Ok(())
}

fn validate_parquet_schema(spec: &ParquetCandidateSpec) -> Result<()> {
    arrow_to_parquet_schema(spec.schema.as_ref()).map_err(|error| {
        ServerError::new(
            ErrorCode::SchemaMismatch,
            format!("schema is not representable as Parquet error=[{error}]"),
        )
        .with_table_id(spec.table_id)
    })?;
    Ok(())
}

#[derive(Serialize)]
struct StoredCandidateMetadataRefV1<'a> {
    format_version: u32,
    file_id: &'a FileId,
    table_id: &'a TableId,
    schema_version: u64,
    provenance: &'a NativeDataFileProvenance,
    partition: &'a MetadataMap,
    metadata: &'a MetadataMap,
    created_at_ns: u64,
}

fn encode_footer_metadata(spec: &ParquetCandidateSpec) -> Result<String> {
    let metadata = StoredCandidateMetadataRefV1 {
        format_version: PARQUET_CANDIDATE_FORMAT_VERSION,
        file_id: &spec.file_id,
        table_id: &spec.table_id,
        schema_version: spec.schema_version,
        provenance: &spec.provenance,
        partition: &spec.partition,
        metadata: &spec.metadata,
        created_at_ns: spec.created_at_ns,
    };
    let mut output = BoundedFooterBuffer::new();
    if let Err(error) = serde_json::to_writer(&mut output, &metadata) {
        if output.limit_exceeded {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "candidate footer metadata exceeds limit maximum=[{}]",
                    MAX_CANDIDATE_FOOTER_METADATA_BYTES
                ),
            )
            .with_table_id(spec.table_id)
            .with_retryable(false));
        }
        return Err(ServerError::new(
            if output.allocation_failed {
                ErrorCode::ResourceExhausted
            } else {
                ErrorCode::Internal
            },
            format!("serialize candidate footer metadata error=[{error}]"),
        )
        .with_table_id(spec.table_id));
    }
    output.into_string(spec.table_id)
}

struct BoundedFooterBuffer {
    bytes: Vec<u8>,
    limit_exceeded: bool,
    allocation_failed: bool,
}

impl BoundedFooterBuffer {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            limit_exceeded: false,
            allocation_failed: false,
        }
    }

    fn into_string(self, table_id: TableId) -> Result<String> {
        let mut compact = Vec::new();
        compact
            .try_reserve_exact(self.bytes.len())
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!("allocate compact candidate footer error=[{error}]"),
                )
                .with_table_id(table_id)
            })?;
        if compact.capacity() > MAX_CANDIDATE_FOOTER_METADATA_BYTES {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "compact candidate footer allocation exceeds its hard limit",
            )
            .with_table_id(table_id));
        }
        compact.extend_from_slice(&self.bytes);
        String::from_utf8(compact).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("candidate footer serializer produced invalid UTF-8 error=[{error}]"),
            )
            .with_table_id(table_id)
        })
    }
}

impl Write for BoundedFooterBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(required) = self.bytes.len().checked_add(bytes.len()) else {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "candidate footer byte count overflow",
            ));
        };
        if required > MAX_CANDIDATE_FOOTER_METADATA_BYTES {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "candidate footer metadata byte limit exceeded",
            ));
        }
        if required > self.bytes.capacity() {
            let target_capacity = required
                .checked_add(CANDIDATE_FOOTER_BUFFER_CHUNK_BYTES - 1)
                .map(|bytes| bytes / CANDIDATE_FOOTER_BUFFER_CHUNK_BYTES)
                .and_then(|chunks| chunks.checked_mul(CANDIDATE_FOOTER_BUFFER_CHUNK_BYTES))
                .filter(|capacity| *capacity <= MAX_CANDIDATE_FOOTER_METADATA_BYTES)
                .ok_or_else(|| {
                    self.limit_exceeded = true;
                    std::io::Error::other("candidate footer capacity limit exceeded")
                })?;
            if let Err(error) = self
                .bytes
                .try_reserve_exact(target_capacity.saturating_sub(self.bytes.len()))
            {
                self.allocation_failed = true;
                return Err(std::io::Error::other(format!(
                    "reserve candidate footer buffer error=[{error}]"
                )));
            }
            if self.bytes.capacity() > MAX_CANDIDATE_FOOTER_METADATA_BYTES {
                self.limit_exceeded = true;
                return Err(std::io::Error::other(
                    "candidate footer allocation exceeds its hard limit",
                ));
            }
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn candidate_managed_path(
    table_id: TableId,
    file_id: FileId,
) -> Result<ManagedDataPath> {
    ManagedDataPath::new(format!("parquet/{table_id}/{file_id}.parquet")).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("construct candidate managed path error=[{error}]"),
        )
        .with_table_id(table_id)
    })
}

fn write_parquet_batches<I>(
    transaction: &mut DurableFileTransaction,
    spec: &ParquetCandidateSpec,
    batches: I,
    footer_metadata: String,
) -> Result<(NonZeroU64, usize)>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let properties = candidate_writer_properties();
    let mut writer = ArrowWriter::try_new(
        transaction.file_mut(),
        Arc::clone(&spec.schema),
        Some(properties),
    )
    .map_err(|error| {
        ServerError::new(
            ErrorCode::SchemaMismatch,
            format!("initialize Parquet candidate writer error=[{error}]"),
        )
        .with_table_id(spec.table_id)
    })?;
    writer.append_key_value_metadata(KeyValue::new(
        CANDIDATE_FOOTER_METADATA_KEY.to_string(),
        footer_metadata,
    ));

    let mut rows = 0_usize;
    for batch in batches {
        if batch.schema().as_ref() != spec.schema.as_ref() {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "candidate batch schema does not match the bound schema",
            )
            .with_table_id(spec.table_id));
        }
        rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
            ServerError::new(ErrorCode::ResourceExhausted, "candidate row count overflow")
                .with_table_id(spec.table_id)
        })?;
        writer.write(&batch).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("write Parquet candidate batch error=[{error}]"),
            )
            .with_table_id(spec.table_id)
        })?;
    }

    let rows = u64::try_from(rows)
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                "candidate must contain at least one row",
            )
            .with_table_id(spec.table_id)
        })?;
    if spec.provenance.offsets().len() != u128::from(rows.get()) {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "candidate row count does not equal offset coverage rows=[{}] offsets=[{}]",
                rows,
                spec.provenance.offsets().len()
            ),
        )
        .with_table_id(spec.table_id));
    }

    let metadata = writer.close().map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("close Parquet candidate writer error=[{error}]"),
        )
        .with_table_id(spec.table_id)
    })?;
    if metadata.num_rows != i64::try_from(rows.get()).unwrap_or(i64::MAX) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "Parquet footer row count mismatch expected=[{}] actual=[{}]",
                rows, metadata.num_rows
            ),
        )
        .with_table_id(spec.table_id));
    }
    Ok((rows, metadata.row_groups.len()))
}

fn candidate_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_created_by(PARQUET_CREATED_BY.to_string())
        .set_max_row_group_size(MAX_ROW_GROUP_ROWS)
        .set_data_page_size_limit(DATA_PAGE_SIZE_BYTES)
        .set_data_page_row_count_limit(DATA_PAGE_ROWS)
        .set_dictionary_page_size_limit(DICTIONARY_PAGE_SIZE_BYTES)
        .set_write_batch_size(WRITE_BATCH_ROWS)
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(true)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_index_truncate_length(Some(METADATA_TRUNCATE_BYTES))
        .set_statistics_truncate_length(Some(METADATA_TRUNCATE_BYTES))
        .build()
}

fn checksum_transaction(
    transaction: &mut DurableFileTransaction,
    table_id: TableId,
) -> Result<String> {
    let path = transaction.temporary_path().to_path_buf();
    let file = transaction.file_mut();
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(table_id, "rewind candidate for checksum", &path, error))?;
    let checksum = sha256_v1_reader(&mut *file)
        .map_err(|error| io_error(table_id, "checksum candidate", &path, error))?;
    Ok(checksum)
}

fn candidate_statistics(row_groups: usize, columns: usize) -> Result<MetadataMap> {
    let row_groups = u64::try_from(row_groups).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "candidate row-group count exceeds u64",
        )
    })?;
    let columns = u64::try_from(columns).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "candidate column count exceeds u64",
        )
    })?;
    Ok(MetadataMap::from([
        (
            "zippy.parquet.row_group_count".to_string(),
            MetadataScalar::UInt64(row_groups),
        ),
        (
            "zippy.parquet.column_count".to_string(),
            MetadataScalar::UInt64(columns),
        ),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn build_candidate(
    spec: ParquetCandidateSpec,
    managed_path: ManagedDataPath,
    path: PathBuf,
    file_size_bytes: u64,
    row_count: NonZeroU64,
    statistics: MetadataMap,
    checksum: String,
) -> Result<DurableParquetCandidate> {
    Ok(DurableParquetCandidate {
        pending: PendingDataFile {
            file_id: spec.file_id,
            table_id: spec.table_id,
            schema_version: spec.schema_version,
            location: DataFileLocation::Managed(managed_path),
            file_size_bytes,
            row_count,
            provenance: DataFileProvenance::Native(spec.provenance),
            partition: spec.partition,
            statistics,
            metadata: spec.metadata,
            checksum,
            created_at_ns: spec.created_at_ns,
        },
        path,
    })
}

fn inspect_existing_candidate(table_id: TableId, path: &Path) -> Result<(u64, String)> {
    let (file, size) = open_existing_candidate(table_id, path)?;
    let checksum = sha256_v1_reader(file)
        .map_err(|error| io_error(table_id, "checksum existing candidate", path, error))?;
    Ok((size, checksum))
}

pub(crate) fn open_existing_candidate(table_id: TableId, path: &Path) -> Result<(File, u64)> {
    let path_metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            corruption_error(
                table_id,
                path,
                None,
                None,
                "catalog-visible candidate is missing",
            )
        } else {
            io_error(table_id, "inspect existing candidate", path, error)
        }
    })?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "existing candidate is not a regular non-symlink file",
        ));
    }
    validate_existing_file_security(table_id, path, &path_metadata)?;
    let file = open_candidate_nofollow(table_id, path)?;
    let descriptor_metadata = file.metadata().map_err(|error| {
        io_error(
            table_id,
            "inspect existing candidate descriptor",
            path,
            error,
        )
    })?;
    validate_same_file_identity(table_id, path, &path_metadata, &descriptor_metadata)?;
    validate_existing_file_security(table_id, path, &descriptor_metadata)?;
    Ok((file, descriptor_metadata.len()))
}

#[cfg(unix)]
fn open_candidate_nofollow(table_id: TableId, path: &Path) -> Result<File> {
    let descriptor = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        io_error(
            table_id,
            "open existing candidate without following links",
            path,
            std::io::Error::from(error),
        )
    })?;
    Ok(File::from(descriptor))
}

#[cfg(not(unix))]
fn open_candidate_nofollow(table_id: TableId, path: &Path) -> Result<File> {
    File::open(path).map_err(|error| io_error(table_id, "open existing candidate", path, error))
}

pub(crate) fn recover_parquet_candidate(
    data_directory: &OwnedDataDirectory,
    table_id: TableId,
    file_id: FileId,
    managed_path: ManagedDataPath,
    expected_file_size_bytes: u64,
) -> Result<DurableParquetCandidate> {
    let path = data_directory
        .directory()
        .root()
        .join(managed_path.as_str());
    let (mut file, file_size_bytes) = open_existing_candidate(table_id, &path)?;
    if file_size_bytes != expected_file_size_bytes {
        return Err(corruption_error(
            table_id,
            &path,
            None,
            None,
            "candidate size changed after the bounded startup scan",
        ));
    }
    validate_parquet_footer_length(&mut file, file_size_bytes, table_id, &path)?;
    let checksum = sha256_v1_reader(&mut file)
        .map_err(|error| io_error(table_id, "checksum recovered candidate", &path, error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(table_id, "rewind recovered candidate", &path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        corruption_error(
            table_id,
            &path,
            None,
            Some(checksum.clone()),
            &format!("recovered candidate Parquet footer is invalid error=[{error}]"),
        )
    })?;
    let parquet_metadata = builder.metadata();
    if parquet_metadata.file_metadata().created_by() != Some(PARQUET_CREATED_BY) {
        return Err(corruption_error(
            table_id,
            &path,
            None,
            Some(checksum),
            "recovered candidate was not written by the frozen Zippy writer profile",
        ));
    }
    let row_count = u64::try_from(parquet_metadata.file_metadata().num_rows())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            corruption_error(
                table_id,
                &path,
                None,
                Some(checksum.clone()),
                "recovered candidate has an invalid row count",
            )
        })?;
    let footer_json = parquet_metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.key == CANDIDATE_FOOTER_METADATA_KEY)
        })
        .and_then(|entry| entry.value.as_deref())
        .ok_or_else(|| {
            corruption_error(
                table_id,
                &path,
                None,
                Some(checksum.clone()),
                "recovered candidate is missing its Zippy footer envelope",
            )
        })?;
    if footer_json.len() > MAX_CANDIDATE_FOOTER_METADATA_BYTES {
        return Err(corruption_error(
            table_id,
            &path,
            None,
            Some(checksum),
            "recovered candidate footer exceeds the metadata byte limit",
        ));
    }
    let stored =
        serde_json::from_str::<StoredCandidateMetadataV1>(footer_json).map_err(|error| {
            corruption_error(
                table_id,
                &path,
                None,
                Some(checksum.clone()),
                &format!("recovered candidate footer is invalid error=[{error}]"),
            )
        })?;
    if stored.format_version != PARQUET_CANDIDATE_FORMAT_VERSION
        || stored.file_id != file_id
        || stored.table_id != table_id
        || stored.schema_version == 0
        || stored.provenance.offsets().len() != u128::from(row_count.get())
    {
        return Err(corruption_error(
            table_id,
            &path,
            None,
            Some(checksum),
            "recovered candidate path, footer identity, schema, or offsets do not agree",
        ));
    }
    let statistics = candidate_statistics(
        parquet_metadata.num_row_groups(),
        builder.schema().fields().len(),
    )?;
    Ok(DurableParquetCandidate {
        pending: PendingDataFile {
            file_id,
            table_id,
            schema_version: stored.schema_version,
            location: DataFileLocation::Managed(managed_path),
            file_size_bytes,
            row_count,
            provenance: DataFileProvenance::Native(stored.provenance),
            partition: stored.partition,
            statistics,
            metadata: stored.metadata,
            checksum,
            created_at_ns: stored.created_at_ns,
        },
        path,
    })
}

pub(crate) fn verify_pending_candidate(
    data_directory: &OwnedDataDirectory,
    pending: &PendingDataFile,
) -> Result<()> {
    let DataFileLocation::Managed(managed_path) = &pending.location else {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "native candidate verification requires a managed file location",
        )
        .with_table_id(pending.table_id));
    };
    let DataFileProvenance::Native(provenance) = &pending.provenance else {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "native candidate verification requires native provenance",
        )
        .with_table_id(pending.table_id));
    };
    let path = data_directory
        .directory()
        .root()
        .join(managed_path.as_str());
    let (file, actual_size) = open_existing_candidate(pending.table_id, &path)?;
    verify_pending_candidate_file(file, actual_size, &path, pending, provenance)
}

fn verify_pending_candidate_file(
    mut file: File,
    actual_size: u64,
    path: &Path,
    pending: &PendingDataFile,
    provenance: &NativeDataFileProvenance,
) -> Result<()> {
    if actual_size != pending.file_size_bytes {
        return Err(corruption_error(
            pending.table_id,
            path,
            Some(pending.checksum.clone()),
            None,
            "pending candidate size changed before verification",
        ));
    }
    validate_parquet_footer_length(&mut file, actual_size, pending.table_id, path)?;
    let actual_checksum = sha256_v1_reader(&mut file)
        .map_err(|error| io_error(pending.table_id, "checksum pending candidate", path, error))?;
    if actual_checksum != pending.checksum {
        return Err(corruption_error(
            pending.table_id,
            path,
            Some(pending.checksum.clone()),
            Some(actual_checksum),
            "candidate no longer matches its pending descriptor",
        ));
    }

    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(pending.table_id, "rewind pending candidate", path, error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        corruption_error(
            pending.table_id,
            path,
            Some(pending.checksum.clone()),
            Some(pending.checksum.clone()),
            &format!("candidate Parquet footer is invalid error=[{error}]"),
        )
    })?;
    let metadata = builder.metadata();
    let actual_rows = u64::try_from(metadata.file_metadata().num_rows()).map_err(|_| {
        corruption_error(
            pending.table_id,
            path,
            None,
            None,
            "candidate Parquet footer has a negative row count",
        )
    })?;
    if actual_rows != pending.row_count.get() {
        return Err(corruption_error(
            pending.table_id,
            path,
            None,
            None,
            "candidate Parquet row count does not match its pending descriptor",
        ));
    }
    let footer_json = metadata
        .file_metadata()
        .key_value_metadata()
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.key == CANDIDATE_FOOTER_METADATA_KEY)
        })
        .and_then(|entry| entry.value.as_deref())
        .ok_or_else(|| {
            corruption_error(
                pending.table_id,
                path,
                None,
                None,
                "candidate Parquet footer is missing its Zippy metadata envelope",
            )
        })?;
    let actual_footer =
        serde_json::from_str::<serde_json::Value>(footer_json).map_err(|error| {
            corruption_error(
                pending.table_id,
                path,
                None,
                None,
                &format!("candidate footer metadata is invalid error=[{error}]"),
            )
        })?;
    let expected_footer = serde_json::to_value(StoredCandidateMetadataV1 {
        format_version: PARQUET_CANDIDATE_FORMAT_VERSION,
        file_id: pending.file_id,
        table_id: pending.table_id,
        schema_version: pending.schema_version,
        provenance: provenance.clone(),
        partition: pending.partition.clone(),
        metadata: pending.metadata.clone(),
        created_at_ns: pending.created_at_ns,
    })
    .map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("serialize expected candidate footer metadata error=[{error}]"),
        )
        .with_table_id(pending.table_id)
    })?;
    if actual_footer != expected_footer {
        return Err(corruption_error(
            pending.table_id,
            path,
            None,
            None,
            "candidate Parquet footer does not match its pending descriptor",
        ));
    }
    Ok(())
}

pub(crate) fn verify_catalog_visible_candidate(
    data_directory: &OwnedDataDirectory,
    pending: &PendingDataFile,
) -> Result<()> {
    let DataFileLocation::Managed(managed_path) = &pending.location else {
        return Err(corruption_error(
            pending.table_id,
            data_directory.directory().root(),
            None,
            None,
            "catalog-visible native candidate is not managed by the data root",
        ));
    };
    let DataFileProvenance::Native(provenance) = &pending.provenance else {
        return Err(corruption_error(
            pending.table_id,
            &data_directory
                .directory()
                .root()
                .join(managed_path.as_str()),
            Some(pending.checksum.clone()),
            None,
            "catalog-visible managed candidate does not have native provenance",
        ));
    };
    let expected = candidate_managed_path(pending.table_id, pending.file_id)?;
    if managed_path != &expected {
        return Err(corruption_error(
            pending.table_id,
            &data_directory
                .directory()
                .root()
                .join(managed_path.as_str()),
            Some(pending.checksum.clone()),
            None,
            "catalog-visible candidate path is not canonical for its table and file identity",
        ));
    }

    let path = data_directory
        .directory()
        .root()
        .join(managed_path.as_str());
    let (file, actual_size) = open_catalog_visible_candidate(data_directory, pending, &path)?;
    verify_pending_candidate_file(file, actual_size, &path, pending, provenance)
}

#[cfg(unix)]
fn open_catalog_visible_candidate(
    data_directory: &OwnedDataDirectory,
    pending: &PendingDataFile,
    path: &Path,
) -> Result<(File, u64)> {
    let root = data_directory.try_clone_root_directory().map_err(|error| {
        ServerError::new(
            ErrorCode::Unavailable,
            format!("clone owned data-root descriptor error=[{error}]"),
        )
        .with_table_id(pending.table_id)
    })?;
    let parquet_path = data_directory.directory().root().join("parquet");
    let parquet = open_catalog_directory_at(&root, "parquet", &parquet_path, pending.table_id)?;
    let table_name = pending.table_id.to_string();
    let table_path = parquet_path.join(&table_name);
    let table = open_catalog_directory_at(&parquet, &table_name, &table_path, pending.table_id)?;
    let file_name = format!("{}.parquet", pending.file_id);
    let descriptor = rustix::fs::openat(
        &table,
        file_name.as_str(),
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| catalog_openat_error(pending.table_id, path, "file", error))?;
    let file = File::from(descriptor);
    let metadata = file.metadata().map_err(|error| {
        io_error(
            pending.table_id,
            "inspect catalog-visible candidate descriptor",
            path,
            error,
        )
    })?;
    if !metadata.is_file() {
        return Err(corruption_error(
            pending.table_id,
            path,
            None,
            None,
            "catalog-visible candidate is not a regular file",
        ));
    }
    validate_existing_file_security(pending.table_id, path, &metadata)?;
    Ok((file, metadata.len()))
}

#[cfg(unix)]
fn open_catalog_directory_at(
    parent: &File,
    name: &str,
    path: &Path,
    table_id: TableId,
) -> Result<File> {
    let descriptor = rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| catalog_openat_error(table_id, path, "directory", error))?;
    let directory = File::from(descriptor);
    let metadata = directory.metadata().map_err(|error| {
        io_error(
            table_id,
            "inspect catalog-visible directory descriptor",
            path,
            error,
        )
    })?;
    crate::reconcile::validate_directory(path, &metadata)?;
    Ok(directory)
}

#[cfg(unix)]
fn catalog_openat_error(
    table_id: TableId,
    path: &Path,
    kind: &str,
    error: rustix::io::Errno,
) -> ServerError {
    if matches!(
        error,
        rustix::io::Errno::NOENT | rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR
    ) {
        return corruption_error(
            table_id,
            path,
            None,
            None,
            &format!("catalog-visible candidate {kind} is missing or unsafe"),
        );
    }
    io_error(
        table_id,
        "open catalog-visible candidate relative to owned directory",
        path,
        std::io::Error::from(error),
    )
}

#[cfg(not(unix))]
fn open_catalog_visible_candidate(
    _data_directory: &OwnedDataDirectory,
    pending: &PendingDataFile,
    path: &Path,
) -> Result<(File, u64)> {
    Err(ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "catalog-visible candidate verification requires descriptor-relative I/O path=[{}]",
            path.display()
        ),
    )
    .with_table_id(pending.table_id)
    .with_retryable(false))
}

fn validate_parquet_footer_length(
    file: &mut File,
    file_size_bytes: u64,
    table_id: TableId,
    path: &Path,
) -> Result<()> {
    if file_size_bytes < 8 {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "candidate is shorter than the Parquet footer trailer",
        ));
    }
    file.seek(SeekFrom::End(-8))
        .map_err(|error| io_error(table_id, "seek candidate footer trailer", path, error))?;
    let mut trailer = [0_u8; 8];
    file.read_exact(&mut trailer)
        .map_err(|error| io_error(table_id, "read candidate footer trailer", path, error))?;
    if &trailer[4..] != b"PAR1" {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "candidate lacks the canonical Parquet footer magic",
        ));
    }
    let footer_bytes = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let footer_exceeds_file = match footer_bytes.checked_add(8) {
        Some(required) => required > file_size_bytes,
        None => true,
    };
    if footer_bytes > MAX_PARQUET_FOOTER_BYTES || footer_exceeds_file {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "candidate Parquet footer length is outside the recovery budget",
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|error| {
        io_error(
            table_id,
            "rewind candidate after footer preflight",
            path,
            error,
        )
    })?;
    Ok(())
}

#[cfg(unix)]
fn validate_existing_file_security(
    table_id: TableId,
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let current_uid = rustix::process::geteuid().as_raw();
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != current_uid || metadata.nlink() != 1 || mode & 0o077 != 0 {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "existing candidate ownership, links, or permissions are invalid",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_existing_file_security(
    _table_id: TableId,
    _path: &Path,
    _metadata: &fs::Metadata,
) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn validate_same_file_identity(
    table_id: TableId,
    path: &Path,
    path_metadata: &fs::Metadata,
    descriptor_metadata: &fs::Metadata,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    if path_metadata.dev() != descriptor_metadata.dev()
        || path_metadata.ino() != descriptor_metadata.ino()
    {
        return Err(corruption_error(
            table_id,
            path,
            None,
            None,
            "existing candidate identity changed during secure open",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_file_identity(
    _table_id: TableId,
    _path: &Path,
    _path_metadata: &fs::Metadata,
    _descriptor_metadata: &fs::Metadata,
) -> Result<()> {
    Ok(())
}

fn map_data_directory_error(table_id: TableId, error: DataDirectoryError) -> ServerError {
    let code = match &error {
        DataDirectoryError::OwnershipHeld { .. } | DataDirectoryError::AlreadyExists { .. } => {
            ErrorCode::Conflict
        }
        DataDirectoryError::Io { .. } => ErrorCode::Unavailable,
        DataDirectoryError::InvalidManagedPath { .. } => ErrorCode::Internal,
        DataDirectoryError::NotDirectory { .. }
        | DataDirectoryError::NotRegularFile { .. }
        | DataDirectoryError::SymbolicLink { .. }
        | DataDirectoryError::PathIdentityChanged { .. }
        | DataDirectoryError::UnversionedNonEmpty { .. }
        | DataDirectoryError::InvalidFormatVersion { .. }
        | DataDirectoryError::UnsupportedFormatVersion { .. } => ErrorCode::DataCorruption,
        #[cfg(unix)]
        DataDirectoryError::WrongOwner { .. }
        | DataDirectoryError::UnexpectedLinkCount { .. }
        | DataDirectoryError::InsecurePermissions { .. } => ErrorCode::DataCorruption,
        #[cfg(not(unix))]
        DataDirectoryError::UnsupportedPlatformSecurity => ErrorCode::Unavailable,
    };
    ServerError::new(
        code,
        format!("durable candidate publication failed error=[{error}]"),
    )
    .with_table_id(table_id)
}

fn io_error(table_id: TableId, operation: &str, path: &Path, error: std::io::Error) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "{operation} failed path=[{}] error=[{}]",
            path.display(),
            error
        ),
    )
    .with_table_id(table_id)
}

fn corruption_error(
    table_id: TableId,
    path: &Path,
    expected_checksum: Option<String>,
    actual_checksum: Option<String>,
    message: &str,
) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!("{message} path=[{}]", path.display()),
    )
    .with_table_id(table_id)
    .with_details(ServerErrorDetails::DataCorruption {
        object: path.display().to_string(),
        expected_checksum,
        actual_checksum,
    })
    .expect("data corruption detail must match its error code")
}
