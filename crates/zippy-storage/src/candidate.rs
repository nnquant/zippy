use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{arrow_to_parquet_schema, ArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::format::KeyValue;
use serde::Serialize;
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
        validate_spec(&spec)?;
        arrow_to_parquet_schema(spec.schema.as_ref()).map_err(|error| {
            ServerError::new(
                ErrorCode::SchemaMismatch,
                format!("schema is not representable as Parquet error=[{error}]"),
            )
            .with_table_id(spec.table_id)
        })?;
        let footer_metadata = encode_footer_metadata(&spec)?;
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

#[derive(Serialize)]
struct StoredCandidateMetadataV1<'a> {
    format_version: u32,
    file_id: FileId,
    table_id: TableId,
    schema_version: u64,
    provenance: &'a NativeDataFileProvenance,
    partition: &'a MetadataMap,
    metadata: &'a MetadataMap,
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

fn encode_footer_metadata(spec: &ParquetCandidateSpec) -> Result<String> {
    let value = serde_json::to_string(&StoredCandidateMetadataV1 {
        format_version: PARQUET_CANDIDATE_FORMAT_VERSION,
        file_id: spec.file_id,
        table_id: spec.table_id,
        schema_version: spec.schema_version,
        provenance: &spec.provenance,
        partition: &spec.partition,
        metadata: &spec.metadata,
        created_at_ns: spec.created_at_ns,
    })
    .map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("serialize candidate footer metadata error=[{error}]"),
        )
        .with_table_id(spec.table_id)
    })?;
    if value.len() > MAX_CANDIDATE_FOOTER_METADATA_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "candidate footer metadata exceeds limit actual=[{}] maximum=[{}]",
                value.len(),
                MAX_CANDIDATE_FOOTER_METADATA_BYTES
            ),
        )
        .with_table_id(spec.table_id));
    }
    Ok(value)
}

fn candidate_managed_path(table_id: TableId, file_id: FileId) -> Result<ManagedDataPath> {
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
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error(table_id, "inspect existing candidate", path, error))?;
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
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| io_error(table_id, "open existing candidate", path, error))?;
    let descriptor_metadata = file.metadata().map_err(|error| {
        io_error(
            table_id,
            "inspect existing candidate descriptor",
            path,
            error,
        )
    })?;
    validate_same_file_identity(table_id, path, &path_metadata, &descriptor_metadata)?;
    let size = descriptor_metadata.len();
    let checksum = sha256_v1_reader(file)
        .map_err(|error| io_error(table_id, "checksum existing candidate", path, error))?;
    Ok((size, checksum))
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
