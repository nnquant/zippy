//! Physical verification for read-only legacy Parquet Catalog imports.

use std::ffi::OsString;
use std::fs::{self, File, Metadata};
use std::io::{Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow::datatypes::Schema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use zippy_types::catalog::LegacyCatalogImportFileV2;
use zippy_types::catalog::LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY;
use zippy_types::table::{
    DataFileLocation, DataFileProvenance, ExternalDataPath, MetadataMap, MetadataScalar,
    PendingDataFile,
};
use zippy_types::{ErrorCode, FileId, Result, ServerError, TableId};

use crate::checksum::sha256_v1_reader;

const LEGACY_FILE_ID_DOMAIN: &[u8] = b"zippy.legacy_catalog.external_file.v2\0";

/// Complete evidence required to verify one legacy Parquet file before Catalog admission.
///
/// This is a trusted, startup-only internal migration input. It must not be populated directly
/// from an RPC or another untrusted ingress; the caller is responsible for supplying a trusted
/// allowed root and an immutable legacy-registry snapshot.
#[derive(Debug, Clone)]
pub struct LegacyParquetImportSpec {
    /// Owning table identity.
    pub table_id: TableId,
    /// Schema version represented by the physical file.
    pub schema_version: u64,
    /// Validated absolute external location.
    pub path: ExternalDataPath,
    /// Optional absolute legacy data root that must contain the file.
    pub allowed_root: Option<PathBuf>,
    /// Exact logical Arrow Schema expected from the Parquet Arrow footer.
    pub expected_schema: Arc<Schema>,
    /// Expected nonzero row count when the legacy registry recorded one.
    pub expected_row_count: Option<NonZeroU64>,
    /// Expected canonical full-file checksum when the legacy registry recorded one.
    pub expected_checksum: Option<String>,
    /// Zero-based position after applying the legacy Gateway's stable persisted-file ordering key.
    /// The original registry index is audit metadata and must not be used here.
    pub discovery_order: u64,
    /// Stable partition metadata retained from the legacy registry.
    pub partition: MetadataMap,
    /// Stable statistics retained from the legacy registry.
    pub statistics: MetadataMap,
    /// Additional implementation-independent legacy metadata.
    pub metadata: MetadataMap,
}

/// Verifies one immutable legacy Parquet file and returns a typed Catalog import entry.
///
/// This function is an internal startup-migration boundary, not an authorization boundary or an
/// RPC handler. Its result may be passed only to the trusted Catalog migration importer.
///
/// This verification does not grant a permanent path capability. Query readers must reopen the
/// external file through the same no-follow policy and revalidate its recorded physical identity.
pub fn verify_legacy_parquet_import(
    spec: LegacyParquetImportSpec,
) -> Result<LegacyCatalogImportFileV2> {
    let path = Path::new(spec.path.as_str());
    validate_expected_checksum_format(spec.table_id, spec.expected_checksum.as_deref())?;
    validate_canonical_path(path, spec.allowed_root.as_deref(), spec.table_id)?;
    let path_metadata = fs::symlink_metadata(path)
        .map_err(|error| physical_error(spec.table_id, path, "inspect legacy file", error))?;
    validate_regular_file(spec.table_id, path, &path_metadata)?;
    let mut file = open_nofollow(spec.table_id, path, spec.allowed_root.as_deref())?;
    let opened_metadata = file.metadata().map_err(|error| {
        physical_error(spec.table_id, path, "inspect opened legacy file", error)
    })?;
    validate_regular_file(spec.table_id, path, &opened_metadata)?;
    validate_same_identity(spec.table_id, path, &path_metadata, &opened_metadata)?;
    if opened_metadata.len() == 0 {
        return Err(blocked(spec.table_id, path, "legacy Parquet file is empty"));
    }
    let parquet_file = file.try_clone().map_err(|error| {
        physical_error(
            spec.table_id,
            path,
            "clone legacy Parquet descriptor",
            error,
        )
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file).map_err(|error| {
        blocked(
            spec.table_id,
            path,
            format!("legacy Parquet footer is invalid error=[{error}]"),
        )
    })?;
    if builder.schema().as_ref() != spec.expected_schema.as_ref() {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            format!(
                "legacy Parquet Schema does not match imported table path=[{}]",
                path.display()
            ),
        )
        .with_table_id(spec.table_id)
        .with_retryable(false));
    }
    let canonical_schema_hash = zippy_catalog::canonical_schema_hash(&spec.expected_schema)
        .map_err(|error| error.with_table_id(spec.table_id))?;
    let row_count = u64::try_from(builder.metadata().file_metadata().num_rows())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            blocked(
                spec.table_id,
                path,
                "legacy Parquet row count is not positive",
            )
        })?;
    if spec
        .expected_row_count
        .is_some_and(|expected| expected != row_count)
    {
        return Err(blocked(
            spec.table_id,
            path,
            format!(
                "legacy Parquet row count mismatch expected=[{}] actual=[{}]",
                spec.expected_row_count.expect("checked Some"),
                row_count
            ),
        ));
    }
    let footer_metadata = file
        .metadata()
        .map_err(|error| physical_error(spec.table_id, path, "reinspect legacy file", error))?;
    validate_same_identity(spec.table_id, path, &opened_metadata, &footer_metadata)?;
    file.seek(SeekFrom::Start(0)).map_err(|error| {
        physical_error(
            spec.table_id,
            path,
            "rewind legacy file before checksum",
            error,
        )
    })?;
    let checksum = sha256_v1_reader(&mut file).map_err(|error| {
        physical_error(spec.table_id, path, "checksum legacy Parquet file", error)
    })?;
    validate_expected_checksum(
        spec.table_id,
        path,
        spec.expected_checksum.as_deref(),
        &checksum,
    )?;
    let final_metadata = file
        .metadata()
        .map_err(|error| physical_error(spec.table_id, path, "reinspect legacy file", error))?;
    validate_same_identity(spec.table_id, path, &opened_metadata, &final_metadata)?;
    let created_at_ns = modified_at_ns(spec.table_id, path, &final_metadata)?;
    let mut statistics = spec.statistics;
    statistics.insert(
        "zippy.parquet.row_group_count".to_owned(),
        MetadataScalar::UInt64(builder.metadata().num_row_groups() as u64),
    );
    statistics.insert(
        "zippy.parquet.column_count".to_owned(),
        MetadataScalar::UInt64(builder.schema().fields().len() as u64),
    );
    let file_id = stable_file_id(
        spec.table_id,
        path,
        &checksum,
        final_metadata.len(),
        row_count,
        &canonical_schema_hash,
    );
    let mut metadata = spec.metadata;
    if metadata
        .insert(
            LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY.to_owned(),
            MetadataScalar::String(canonical_schema_hash),
        )
        .is_some()
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy import metadata contains a verifier-reserved key",
        )
        .with_table_id(spec.table_id)
        .with_retryable(false));
    }
    Ok(LegacyCatalogImportFileV2 {
        discovery_order: spec.discovery_order,
        file: PendingDataFile {
            file_id,
            table_id: spec.table_id,
            schema_version: spec.schema_version,
            location: DataFileLocation::ExternalLegacy(spec.path),
            file_size_bytes: final_metadata.len(),
            row_count,
            provenance: DataFileProvenance::Legacy {
                offsets: None,
                preserve_discovery_order: true,
            },
            partition: spec.partition,
            statistics,
            metadata,
            checksum,
            created_at_ns,
        },
    })
}

/// Revalidates one Catalog-visible external legacy Parquet file before read admission opens.
///
/// The absolute Catalog path is never copied into an error. Every component is opened with
/// `NOFOLLOW`; all content checks use that retained file descriptor and compare its identity
/// before and after checksum and footer reads.
pub(crate) fn verify_catalog_visible_external_legacy_file(
    pending: &PendingDataFile,
    expected_schema: &Schema,
) -> Result<()> {
    let DataFileLocation::ExternalLegacy(external_path) = &pending.location else {
        return Err(catalog_legacy_corruption(
            pending,
            "catalog external legacy verifier received a managed location",
        ));
    };
    if !matches!(pending.provenance, DataFileProvenance::Legacy { .. }) {
        return Err(catalog_legacy_corruption(
            pending,
            "catalog external legacy file has non-legacy provenance",
        ));
    }
    let path = Path::new(external_path.as_str());
    validate_expected_checksum_format(pending.table_id, Some(&pending.checksum)).map_err(|_| {
        catalog_legacy_corruption(
            pending,
            "catalog external legacy checksum format is invalid",
        )
    })?;
    let mut file = open_nofollow(pending.table_id, path, None).map_err(|error| {
        sanitize_catalog_legacy_error(pending, "open external legacy file", error)
    })?;
    let opened_metadata = file.metadata().map_err(|error| {
        sanitize_catalog_legacy_io_error(pending, "inspect external legacy descriptor", error)
    })?;
    validate_regular_file(pending.table_id, path, &opened_metadata).map_err(|_| {
        catalog_legacy_corruption(pending, "external legacy descriptor is not a regular file")
    })?;
    if opened_metadata.len() != pending.file_size_bytes {
        return Err(catalog_legacy_corruption(
            pending,
            "external legacy file size does not match Catalog metadata",
        ));
    }
    crate::candidate::validate_parquet_footer_length(
        &mut file,
        opened_metadata.len(),
        pending.table_id,
        path,
    )
    .map_err(|_| {
        catalog_legacy_corruption(pending, "external legacy Parquet footer length is invalid")
    })?;

    file.seek(SeekFrom::Start(0)).map_err(|error| {
        sanitize_catalog_legacy_io_error(
            pending,
            "rewind external legacy file before checksum",
            error,
        )
    })?;
    let actual_checksum = sha256_v1_reader(&mut file).map_err(|error| {
        sanitize_catalog_legacy_io_error(pending, "checksum external legacy file", error)
    })?;
    let checksummed_metadata = file.metadata().map_err(|error| {
        sanitize_catalog_legacy_io_error(
            pending,
            "reinspect external legacy descriptor after checksum",
            error,
        )
    })?;
    validate_same_identity(
        pending.table_id,
        path,
        &opened_metadata,
        &checksummed_metadata,
    )
    .map_err(|_| {
        catalog_legacy_corruption(pending, "external legacy file changed during checksum")
    })?;
    if actual_checksum != pending.checksum {
        return Err(catalog_legacy_corruption(
            pending,
            "external legacy checksum does not match Catalog metadata",
        ));
    }

    file.seek(SeekFrom::Start(0)).map_err(|error| {
        sanitize_catalog_legacy_io_error(
            pending,
            "rewind external legacy file before footer validation",
            error,
        )
    })?;
    let parquet_file = file.try_clone().map_err(|error| {
        sanitize_catalog_legacy_io_error(
            pending,
            "clone external legacy descriptor for footer validation",
            error,
        )
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file).map_err(|_| {
        catalog_legacy_corruption(pending, "external legacy Parquet footer is invalid")
    })?;
    if builder.schema().as_ref() != expected_schema {
        return Err(catalog_legacy_corruption(
            pending,
            "external legacy Parquet schema does not match the Catalog snapshot",
        ));
    }
    let row_count = u64::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
        catalog_legacy_corruption(pending, "external legacy Parquet row count is invalid")
    })?;
    if row_count != pending.row_count.get() {
        return Err(catalog_legacy_corruption(
            pending,
            "external legacy Parquet row count does not match Catalog metadata",
        ));
    }
    let final_metadata = file.metadata().map_err(|error| {
        sanitize_catalog_legacy_io_error(
            pending,
            "reinspect external legacy descriptor after footer validation",
            error,
        )
    })?;
    validate_same_identity(pending.table_id, path, &opened_metadata, &final_metadata).map_err(
        |_| {
            catalog_legacy_corruption(
                pending,
                "external legacy file changed during footer validation",
            )
        },
    )?;
    Ok(())
}

fn catalog_legacy_corruption(pending: &PendingDataFile, message: &str) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "{message} table_id=[{}] file_id=[{}]",
            pending.table_id, pending.file_id
        ),
    )
    .with_table_id(pending.table_id)
    .with_retryable(false)
}

fn sanitize_catalog_legacy_error(
    pending: &PendingDataFile,
    operation: &str,
    error: ServerError,
) -> ServerError {
    let code = match error.code() {
        ErrorCode::Unavailable | ErrorCode::UnsupportedPlan => error.code(),
        _ => ErrorCode::DataCorruption,
    };
    ServerError::new(
        code,
        format!(
            "{operation} failed table_id=[{}] file_id=[{}]",
            pending.table_id, pending.file_id
        ),
    )
    .with_table_id(pending.table_id)
    .with_retryable(false)
}

fn sanitize_catalog_legacy_io_error(
    pending: &PendingDataFile,
    operation: &str,
    error: std::io::Error,
) -> ServerError {
    let code = if error.kind() == std::io::ErrorKind::NotFound {
        ErrorCode::DataCorruption
    } else {
        ErrorCode::Unavailable
    };
    ServerError::new(
        code,
        format!(
            "{operation} failed table_id=[{}] file_id=[{}]",
            pending.table_id, pending.file_id
        ),
    )
    .with_table_id(pending.table_id)
    .with_retryable(false)
}

fn validate_canonical_path(
    path: &Path,
    allowed_root: Option<&Path>,
    table_id: TableId,
) -> Result<()> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| physical_error(table_id, path, "canonicalize legacy file", error))?;
    if canonical != path {
        return Err(blocked(
            table_id,
            path,
            "legacy file path contains a symbolic-link or non-canonical component",
        ));
    }
    if let Some(root) = allowed_root {
        if !root.is_absolute() {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "legacy allowed data root must be absolute",
            )
            .with_table_id(table_id)
            .with_retryable(false));
        }
        let canonical_root = fs::canonicalize(root).map_err(|error| {
            physical_error(table_id, root, "canonicalize legacy data root", error)
        })?;
        if canonical_root != root || !path.starts_with(&canonical_root) {
            return Err(blocked(
                table_id,
                path,
                "legacy file is outside its canonical allowed data root",
            ));
        }
    }
    Ok(())
}

fn validate_regular_file(table_id: TableId, path: &Path, metadata: &Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(blocked(
            table_id,
            path,
            "legacy path is not a regular non-symlink file",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_nofollow(table_id: TableId, path: &Path, allowed_root: Option<&Path>) -> Result<File> {
    let root = rustix::fs::open(
        "/",
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        physical_error(
            table_id,
            path,
            "open filesystem root for legacy file",
            std::io::Error::from(error),
        )
    })?;
    let mut parent = File::from(root);
    let file_components = if let Some(allowed_root) = allowed_root {
        for component in absolute_normal_components(table_id, path, allowed_root)? {
            parent = open_directory_at(table_id, path, &parent, &component)?;
        }
        let relative = path.strip_prefix(allowed_root).map_err(|_| {
            blocked(
                table_id,
                path,
                "legacy file is outside its allowed data-root descriptor",
            )
        })?;
        relative_normal_components(table_id, path, relative)?
    } else {
        absolute_normal_components(table_id, path, path)?
    };
    let mut components = file_components.iter().peekable();
    while let Some(name) = components.next() {
        let final_component = components.peek().is_none();
        let flags = if final_component {
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC
        } else {
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC
        };
        let descriptor = rustix::fs::openat(&parent, name, flags, rustix::fs::Mode::empty())
            .map_err(|error| {
                nofollow_open_error(
                    table_id,
                    path,
                    "open legacy path component without following links",
                    error,
                )
            })?;
        let opened = File::from(descriptor);
        if final_component {
            return Ok(opened);
        }
        parent = opened;
    }
    Err(blocked(table_id, path, "legacy path does not name a file"))
}

#[cfg(unix)]
fn absolute_normal_components(
    table_id: TableId,
    display_path: &Path,
    path: &Path,
) -> Result<Vec<OsString>> {
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(
            ServerError::new(ErrorCode::InvalidArgument, "legacy path must be absolute")
                .with_table_id(table_id)
                .with_retryable(false),
        );
    }
    collect_normal_components(table_id, display_path, components)
}

#[cfg(unix)]
fn relative_normal_components(
    table_id: TableId,
    display_path: &Path,
    path: &Path,
) -> Result<Vec<OsString>> {
    collect_normal_components(table_id, display_path, path.components())
}

#[cfg(unix)]
fn collect_normal_components<'a>(
    table_id: TableId,
    display_path: &Path,
    components: impl Iterator<Item = Component<'a>>,
) -> Result<Vec<OsString>> {
    components
        .map(|component| match component {
            Component::Normal(name) => Ok(name.to_os_string()),
            _ => Err(blocked(
                table_id,
                display_path,
                "legacy path contains an unsafe component",
            )),
        })
        .collect()
}

#[cfg(unix)]
fn open_directory_at(
    table_id: TableId,
    path: &Path,
    parent: &File,
    name: &OsString,
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
    .map_err(|error| {
        nofollow_open_error(
            table_id,
            path,
            "open legacy data-root component without following links",
            error,
        )
    })?;
    Ok(File::from(descriptor))
}

#[cfg(unix)]
fn nofollow_open_error(
    table_id: TableId,
    path: &Path,
    operation: &str,
    error: rustix::io::Errno,
) -> ServerError {
    if matches!(
        error,
        rustix::io::Errno::NOENT | rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR
    ) {
        return blocked(
            table_id,
            path,
            "legacy path is missing or contains an unsafe component",
        );
    }
    physical_error(table_id, path, operation, std::io::Error::from(error))
}

#[cfg(not(unix))]
fn open_nofollow(table_id: TableId, _path: &Path, _allowed_root: Option<&Path>) -> Result<File> {
    Err(ServerError::new(
        ErrorCode::UnsupportedPlan,
        "secure legacy Parquet import is unavailable on this platform",
    )
    .with_table_id(table_id)
    .with_retryable(false))
}

#[cfg(unix)]
fn validate_same_identity(
    table_id: TableId,
    path: &Path,
    expected: &Metadata,
    actual: &Metadata,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    if expected.dev() != actual.dev()
        || expected.ino() != actual.ino()
        || expected.len() != actual.len()
        || expected.mtime() != actual.mtime()
        || expected.mtime_nsec() != actual.mtime_nsec()
        || expected.ctime() != actual.ctime()
        || expected.ctime_nsec() != actual.ctime_nsec()
    {
        return Err(blocked(
            table_id,
            path,
            "legacy file identity changed during verification",
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_same_identity(
    table_id: TableId,
    path: &Path,
    expected: &Metadata,
    actual: &Metadata,
) -> Result<()> {
    if expected.len() != actual.len() || expected.modified().ok() != actual.modified().ok() {
        return Err(blocked(
            table_id,
            path,
            "legacy file identity changed during verification",
        ));
    }
    Ok(())
}

fn validate_expected_checksum(
    table_id: TableId,
    path: &Path,
    expected: Option<&str>,
    actual: &str,
) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if expected != actual {
        return Err(blocked(
            table_id,
            path,
            format!("legacy Parquet checksum mismatch expected=[{expected}] actual=[{actual}]"),
        ));
    }
    Ok(())
}

fn validate_expected_checksum_format(table_id: TableId, expected: Option<&str>) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let digest = expected.strip_prefix("sha256-v1:").ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy checksum uses an unsupported algorithm",
        )
        .with_table_id(table_id)
        .with_retryable(false)
    })?;
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy checksum is not canonical lowercase SHA-256",
        )
        .with_table_id(table_id)
        .with_retryable(false));
    }
    Ok(())
}

fn modified_at_ns(table_id: TableId, path: &Path, metadata: &Metadata) -> Result<u64> {
    let nanos = metadata
        .modified()
        .map_err(|error| {
            physical_error(table_id, path, "read legacy file modification time", error)
        })?
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            blocked(
                table_id,
                path,
                format!("legacy file modification time precedes Unix epoch error=[{error}]"),
            )
        })?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| {
        blocked(
            table_id,
            path,
            "legacy file modification time exceeds timestamp representation",
        )
    })
}

fn stable_file_id(
    table_id: TableId,
    path: &Path,
    checksum: &str,
    file_size: u64,
    row_count: NonZeroU64,
    canonical_schema_hash: &str,
) -> FileId {
    let mut hasher = Sha256::new();
    for component in [
        LEGACY_FILE_ID_DOMAIN,
        table_id.into_bytes().as_slice(),
        path.as_os_str().as_encoded_bytes(),
        checksum.as_bytes(),
        file_size.to_be_bytes().as_slice(),
        row_count.get().to_be_bytes().as_slice(),
        canonical_schema_hash.as_bytes(),
    ] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes.iter().all(|byte| *byte == 0) {
        bytes[15] = 1;
    }
    FileId::from_bytes(bytes).expect("stable legacy file identity is forced nonzero")
}

fn blocked(table_id: TableId, path: &Path, message: impl Into<String>) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!("{} path=[{}]", message.into(), path.display()),
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

fn physical_error(
    table_id: TableId,
    path: &Path,
    operation: &str,
    error: std::io::Error,
) -> ServerError {
    let code = if error.kind() == std::io::ErrorKind::NotFound {
        ErrorCode::DataCorruption
    } else {
        ErrorCode::Unavailable
    };
    ServerError::new(
        code,
        format!("{operation} path=[{}] error=[{error}]", path.display()),
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::fs::File;
    use std::num::NonZeroU64;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use zippy_types::table::{ExternalDataPath, MetadataMap, PendingDataFile};

    use super::{
        open_nofollow, validate_regular_file, validate_same_identity,
        verify_catalog_visible_external_legacy_file, verify_legacy_parquet_import,
        LegacyParquetImportSpec,
    };
    use zippy_types::{ErrorCode, TableId};

    fn verified_catalog_file(root: &Path) -> (PathBuf, PendingDataFile, Arc<Schema>) {
        let path = root.join("catalog-visible.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![100, 101, 102]))],
        )
        .expect("create legacy test batch");
        let mut writer = ArrowWriter::try_new(
            File::create(&path).expect("create legacy Parquet test file"),
            Arc::clone(&schema),
            None,
        )
        .expect("create legacy Parquet test writer");
        writer
            .write(&batch)
            .expect("write legacy Parquet test batch");
        writer.close().expect("close legacy Parquet test writer");
        let verified = verify_legacy_parquet_import(LegacyParquetImportSpec {
            table_id: TableId::from_u128(1).expect("nonzero table id"),
            schema_version: 1,
            path: ExternalDataPath::new(path.to_str().expect("temporary path is UTF-8"))
                .expect("absolute external data path"),
            allowed_root: Some(root.to_path_buf()),
            expected_schema: Arc::clone(&schema),
            expected_row_count: NonZeroU64::new(3),
            expected_checksum: None,
            discovery_order: 0,
            partition: MetadataMap::new(),
            statistics: MetadataMap::new(),
            metadata: MetadataMap::new(),
        })
        .expect("verify legacy Parquet test file");
        (path, verified.file, schema)
    }

    #[test]
    fn final_component_fifo_replacement_does_not_block_and_fails_closed() {
        let root = tempfile::tempdir().expect("create legacy import test root");
        let path = root.path().join("replaced.parquet");
        std::fs::write(&path, b"regular-before-open").expect("create prechecked regular file");
        let prechecked = std::fs::symlink_metadata(&path).expect("inspect prechecked file");
        validate_regular_file(TableId::from_u128(1).unwrap(), &path, &prechecked)
            .expect("prechecked path is regular");

        std::fs::remove_file(&path).expect("remove prechecked file");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &path,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .expect("replace final component with FIFO");

        let (cancel_sender, cancel_receiver) = mpsc::channel();
        let release_path = path.clone();
        let release = std::thread::spawn(move || {
            if cancel_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_err()
            {
                let descriptor = rustix::fs::open(
                    &release_path,
                    rustix::fs::OFlags::RDWR
                        | rustix::fs::OFlags::NONBLOCK
                        | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )
                .expect("release a mistakenly blocking FIFO open");
                drop(descriptor);
            }
        });

        let table_id = TableId::from_u128(1).unwrap();
        let started = Instant::now();
        let opened = open_nofollow(table_id, &path, Some(root.path()))
            .expect("open replaced FIFO without blocking");
        let open_elapsed = started.elapsed();
        let _ = cancel_sender.send(());
        release.join().expect("join blocking-open watchdog");

        assert!(
            open_elapsed < Duration::from_millis(500),
            "FIFO replacement blocked final open for {open_elapsed:?}"
        );
        let opened_metadata = opened.metadata().expect("inspect opened FIFO");
        let error = validate_regular_file(table_id, &path, &opened_metadata)
            .expect_err("FIFO replacement must fail closed");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn same_inode_rewrite_with_restored_mtime_is_rejected_by_ctime() {
        let root = tempfile::tempdir().expect("create legacy identity test root");
        let path = root.path().join("identity.parquet");
        std::fs::write(&path, b"version-one").expect("write initial identity fixture");
        let before = std::fs::metadata(&path).expect("inspect initial identity fixture");
        let modified = before.modified().expect("read initial modification time");

        let deadline = Instant::now() + Duration::from_secs(2);
        let after = loop {
            std::thread::sleep(Duration::from_millis(10));
            std::fs::write(&path, b"version-two").expect("rewrite same-size identity fixture");
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open identity fixture to restore mtime");
            file.set_times(std::fs::FileTimes::new().set_modified(modified))
                .expect("restore original modification time");
            let after = file.metadata().expect("inspect rewritten identity fixture");
            if (before.ctime(), before.ctime_nsec()) != (after.ctime(), after.ctime_nsec()) {
                break after;
            }
            assert!(
                Instant::now() < deadline,
                "filesystem ctime did not advance after repeated same-inode rewrites"
            );
        };

        assert_eq!(before.dev(), after.dev());
        assert_eq!(before.ino(), after.ino());
        assert_eq!(before.len(), after.len());
        assert_eq!(before.mtime(), after.mtime());
        assert_eq!(before.mtime_nsec(), after.mtime_nsec());
        assert_ne!(
            (before.ctime(), before.ctime_nsec()),
            (after.ctime(), after.ctime_nsec())
        );

        let table_id = TableId::from_u128(1).unwrap();
        let error = validate_same_identity(table_id, &path, &before, &after)
            .expect_err("ctime change must reject the rewritten identity");
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn catalog_visible_external_file_requires_snapshot_schema() {
        let root = tempfile::tempdir().expect("create legacy schema test root");
        let (path, pending, _schema) = verified_catalog_file(root.path());
        let wrong_schema = Schema::new(vec![Field::new("price", DataType::Utf8, false)]);

        let error = verify_catalog_visible_external_legacy_file(&pending, &wrong_schema)
            .expect_err("physical schema mismatch must fail closed");

        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.message().contains(path.to_string_lossy().as_ref()));
    }

    #[test]
    fn catalog_visible_external_file_requires_catalog_row_count() {
        let root = tempfile::tempdir().expect("create legacy row-count test root");
        let (path, mut pending, schema) = verified_catalog_file(root.path());
        pending.row_count = NonZeroU64::new(2).expect("nonzero mismatched row count");

        let error = verify_catalog_visible_external_legacy_file(&pending, schema.as_ref())
            .expect_err("physical row-count mismatch must fail closed");

        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.message().contains(path.to_string_lossy().as_ref()));
    }
}
