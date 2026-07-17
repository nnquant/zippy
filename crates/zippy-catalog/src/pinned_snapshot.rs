//! Lease-bound query manifests and controlled physical file readers.

use std::fmt;
use std::fs::{File, Metadata};
use std::io::{self, Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Component, Path};
use std::sync::Arc;

use sha2::{Digest, Sha256};
use zippy_core::OwnedDataDirectory;
use zippy_types::catalog::{CatalogQuerySnapshot, CatalogSnapshotSelector};
use zippy_types::storage::{
    CatalogSnapshotMemoryLease, CatalogSnapshotMemoryLeaseDescriptor, CatalogSnapshotPinDescriptor,
    CatalogSnapshotPinGuard,
};
use zippy_types::table::{
    DataFileDescriptor, DataFileLocation, DataFileProvenance, DataFileState, MetadataMap,
    SchemaDescriptor, TableDescriptor, TableSnapshot,
};
use zippy_types::{ErrorCode, FileId, Result, ServerError, TableId};

/// Exact Catalog metadata retained together with its in-process GC pins.
///
/// The raw Catalog snapshot and every physical location remain private. Callers split work by
/// cloning [`PinnedTableManifest`] or [`PinnedDataFile`] values; each clone retains the same
/// opaque owner, so the final task or reader releases all table pins.
///
/// ```compile_fail
/// fn raw_snapshot_cannot_escape(
///     pinned: &zippy_catalog::SqliteCatalogPinnedQuerySnapshot,
/// ) {
///     let _ = pinned.snapshot();
/// }
/// ```
#[derive(Clone)]
pub struct SqliteCatalogPinnedQuerySnapshot {
    owner: Arc<PinnedSnapshotOwner>,
}

/// Pure scalar metadata used to admit one capability-free snapshot projection.
///
/// Reading this value does not clone a table/file capability or expose a physical location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PinnedSnapshotProjectionProfile {
    table_count: u64,
    file_count: u64,
    retained_schema_bytes: u64,
}

impl PinnedSnapshotProjectionProfile {
    /// Returns the number of exact table manifests in the pinned snapshot.
    #[must_use]
    pub const fn table_count(self) -> u64 {
        self.table_count
    }

    /// Returns the number of visible files across every exact table manifest.
    #[must_use]
    pub const fn file_count(self) -> u64 {
        self.file_count
    }

    /// Returns the sum of verified decoded-schema retained-memory charges.
    #[must_use]
    pub const fn retained_schema_bytes(self) -> u64 {
        self.retained_schema_bytes
    }
}

struct PinnedSnapshotOwner {
    snapshot: CatalogQuerySnapshot,
    descriptor: CatalogSnapshotPinDescriptor,
    scan_file_indices: Vec<Box<[usize]>>,
    retained_schema_charge_bytes: Vec<NonZeroU64>,
    projection_profile: PinnedSnapshotProjectionProfile,
    _pins: Vec<crate::SqliteCatalogRuntimeSnapshotPin>,
    _data_directory: Arc<OwnedDataDirectory>,
    // Keep this field last so owned snapshot data and sidecars drop before admission is released.
    _memory_lease: CatalogSnapshotMemoryLease,
}

impl SqliteCatalogPinnedQuerySnapshot {
    pub(crate) fn try_new(
        snapshot: CatalogQuerySnapshot,
        pins: Vec<crate::SqliteCatalogRuntimeSnapshotPin>,
        data_directory: Arc<OwnedDataDirectory>,
        memory_lease: CatalogSnapshotMemoryLease,
    ) -> Result<Self> {
        debug_assert!(!pins.is_empty());
        debug_assert_eq!(snapshot.tables().len(), pins.len());
        let retained_schema_charge_bytes = snapshot
            .tables()
            .iter()
            .map(|table| {
                let decoded = crate::decode_persisted_table_schema(table.table(), table.schema())?;
                NonZeroU64::new(decoded.retained_charge_bytes()).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::DataCorruption,
                        "decoded persisted schema has a zero retained-memory charge",
                    )
                    .with_table_id(table.selector().table_id)
                    .with_retryable(false)
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let descriptor = CatalogSnapshotPinDescriptor {
            catalog_revision: snapshot.catalog_revision(),
            selectors: snapshot
                .tables()
                .iter()
                .map(|table| table.selector().clone())
                .collect(),
        };
        let scan_file_indices = snapshot
            .tables()
            .iter()
            .map(|table| {
                let files = table.files().as_slice();
                let mut indices = (0..files.len()).collect::<Vec<_>>();
                indices.sort_unstable_by(|left, right| {
                    let left_file = &files[*left];
                    let right_file = &files[*right];
                    match (
                        left_file.legacy_discovery_order(),
                        right_file.legacy_discovery_order(),
                    ) {
                        (Some(left), Some(right)) => left
                            .cmp(&right)
                            .then_with(|| left_file.file_id().cmp(&right_file.file_id())),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => left_file.file_id().cmp(&right_file.file_id()),
                    }
                });
                indices.into_boxed_slice()
            })
            .collect::<Vec<_>>();
        let projection_profile = projection_profile(
            snapshot.tables().len(),
            scan_file_indices.as_slice(),
            retained_schema_charge_bytes.as_slice(),
        )?;
        Ok(Self {
            owner: Arc::new(PinnedSnapshotOwner {
                snapshot,
                descriptor,
                scan_file_indices,
                retained_schema_charge_bytes,
                projection_profile,
                _pins: pins,
                _data_directory: data_directory,
                _memory_lease: memory_lease,
            }),
        })
    }

    /// Returns the Catalog revision shared by every pinned table manifest.
    #[must_use]
    pub fn catalog_revision(&self) -> u64 {
        self.owner.snapshot.catalog_revision()
    }

    /// Borrows path-free audit data for this exact pinned Catalog snapshot.
    #[must_use]
    pub fn descriptor(&self) -> &CatalogSnapshotPinDescriptor {
        &self.owner.descriptor
    }

    /// Returns the authenticated memory reservation retained by this pinned owner.
    ///
    /// The descriptor is audit data, not an independent capability. This snapshot can only be
    /// constructed after its private Catalog verifier accepts the structurally retained lease.
    #[must_use]
    pub fn memory_lease_descriptor(&self) -> CatalogSnapshotMemoryLeaseDescriptor {
        self.owner._memory_lease.descriptor()
    }

    /// Returns the number of exact table manifests retained by this owner.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.owner.snapshot.tables().len()
    }

    /// Returns scalar projection metadata without deriving a table or file capability.
    #[must_use]
    pub fn projection_profile(&self) -> PinnedSnapshotProjectionProfile {
        self.owner.projection_profile
    }

    /// Returns true when the query references no tables.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.owner.snapshot.is_empty()
    }

    /// Creates lease-bound task manifests in deterministic table-id order.
    pub fn tables(&self) -> impl ExactSizeIterator<Item = PinnedTableManifest> + '_ {
        (0..self.table_count()).map(|table_index| PinnedTableManifest {
            owner: Arc::clone(&self.owner),
            table_index,
        })
    }

    /// Creates one lease-bound table task by deterministic manifest index.
    #[must_use]
    pub fn table(&self, table_index: usize) -> Option<PinnedTableManifest> {
        (table_index < self.table_count()).then(|| PinnedTableManifest {
            owner: Arc::clone(&self.owner),
            table_index,
        })
    }
}

fn projection_profile(
    expected_table_count: usize,
    scan_file_indices: &[Box<[usize]>],
    retained_schema_charge_bytes: &[NonZeroU64],
) -> Result<PinnedSnapshotProjectionProfile> {
    if scan_file_indices.len() != expected_table_count
        || retained_schema_charge_bytes.len() != expected_table_count
    {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "pinned projection sidecar table counts differ",
        )
        .with_retryable(false));
    }
    let table_count = u64::try_from(scan_file_indices.len()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "pinned projection table count exceeds u64",
        )
        .with_retryable(false)
    })?;
    let file_count = scan_file_indices.iter().try_fold(0_u64, |total, files| {
        let count = u64::try_from(files.len()).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "pinned projection file count exceeds u64",
            )
            .with_retryable(false)
        })?;
        total.checked_add(count).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "pinned projection file count overflow",
            )
            .with_retryable(false)
        })
    })?;
    let retained_schema_bytes =
        retained_schema_charge_bytes
            .iter()
            .try_fold(0_u64, |total, charge| {
                total.checked_add(charge.get()).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "pinned projection schema charge overflow",
                    )
                    .with_retryable(false)
                })
            })?;
    Ok(PinnedSnapshotProjectionProfile {
        table_count,
        file_count,
        retained_schema_bytes,
    })
}

impl CatalogSnapshotPinGuard for SqliteCatalogPinnedQuerySnapshot {
    fn descriptor(&self) -> &CatalogSnapshotPinDescriptor {
        self.descriptor()
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
        self
    }
}

impl fmt::Debug for SqliteCatalogPinnedQuerySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteCatalogPinnedQuerySnapshot")
            .field("catalog_revision", &self.catalog_revision())
            .field("table_count", &self.table_count())
            .finish_non_exhaustive()
    }
}

/// One exact table manifest whose clones retain the query snapshot owner.
///
/// This type exposes planning metadata that contains no data-file location. Physical files are
/// represented only by [`PinnedDataFile`] task values.
#[derive(Clone)]
pub struct PinnedTableManifest {
    owner: Arc<PinnedSnapshotOwner>,
    table_index: usize,
}

impl PinnedTableManifest {
    /// Returns the deterministic table ordinal in the pinned query snapshot.
    #[must_use]
    pub const fn table_index(&self) -> usize {
        self.table_index
    }

    /// Returns the exact Catalog selector protected by this manifest.
    #[must_use]
    pub fn selector(&self) -> &CatalogSnapshotSelector {
        self.raw_table().selector()
    }

    /// Borrows persistent table metadata, which contains no physical file location.
    #[must_use]
    pub fn table_descriptor(&self) -> &TableDescriptor {
        self.raw_table().table()
    }

    /// Borrows the exact decoder schema selected for this table.
    #[must_use]
    pub fn schema(&self) -> &SchemaDescriptor {
        self.raw_table().schema()
    }

    /// Borrows logical snapshot metadata without exposing its physical manifest.
    #[must_use]
    pub fn snapshot_metadata(&self) -> &TableSnapshot {
        self.raw_table().snapshot()
    }

    /// Returns the deterministic logical memory charge for decoding and retaining this schema.
    ///
    /// The pinned owner computes this scalar with the same verified persisted-schema decoder used
    /// at execution time, but deliberately does not retain the decoded Arrow schema itself.
    #[must_use]
    pub fn retained_schema_charge_bytes(&self) -> NonZeroU64 {
        self.owner.retained_schema_charge_bytes[self.table_index]
    }

    /// Returns the number of visible physical files in this exact snapshot.
    #[must_use]
    pub fn file_count(&self) -> usize {
        self.scan_file_indices().len()
    }

    /// Returns true when the exact snapshot contains no persisted files.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw_table().files().is_empty()
    }

    /// Creates lease-bound file tasks in the exact deterministic Catalog manifest order.
    pub fn files(&self) -> impl ExactSizeIterator<Item = PinnedDataFile> + '_ {
        self.scan_file_indices()
            .iter()
            .copied()
            .map(|file_index| PinnedDataFile {
                owner: Arc::clone(&self.owner),
                table_index: self.table_index,
                file_index,
            })
    }

    /// Creates one lease-bound file task by deterministic manifest index.
    #[must_use]
    pub fn file(&self, file_index: usize) -> Option<PinnedDataFile> {
        self.scan_file_indices()
            .get(file_index)
            .copied()
            .map(|file_index| PinnedDataFile {
                owner: Arc::clone(&self.owner),
                table_index: self.table_index,
                file_index,
            })
    }

    fn scan_file_indices(&self) -> &[usize] {
        &self.owner.scan_file_indices[self.table_index]
    }

    fn raw_table(&self) -> &zippy_types::catalog::CatalogTableSnapshot {
        &self.owner.snapshot.tables()[self.table_index]
    }
}

impl fmt::Debug for PinnedTableManifest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedTableManifest")
            .field("selector", self.selector())
            .field("file_count", &self.file_count())
            .finish_non_exhaustive()
    }
}

/// The non-sensitive class of a pinned data-file location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinnedDataFileLocationKind {
    /// A server-managed file below the exclusively owned data root.
    Managed,
    /// An explicitly mounted read-only legacy file outside the managed root.
    ExternalLegacy,
}

/// One physical scan task that retains the complete query snapshot owner.
///
/// No method exposes a path or a cloneable [`DataFileDescriptor`]. Use [`Self::open_reader`] to
/// obtain a controlled reader that independently keeps the same owner alive.
///
/// ```compile_fail
/// fn raw_location_cannot_escape(file: &zippy_catalog::PinnedDataFile) {
///     let _ = file.location();
/// }
/// ```
#[derive(Clone)]
pub struct PinnedDataFile {
    owner: Arc<PinnedSnapshotOwner>,
    table_index: usize,
    file_index: usize,
}

impl PinnedDataFile {
    /// Returns the immutable Catalog file identity.
    #[must_use]
    pub fn file_id(&self) -> FileId {
        self.raw_file().file_id()
    }

    /// Returns the immutable owning table identity.
    #[must_use]
    pub fn table_id(&self) -> TableId {
        self.raw_file().table_id()
    }

    /// Returns the schema version encoded in this physical file.
    #[must_use]
    pub fn schema_version(&self) -> u64 {
        self.raw_file().file.schema_version
    }

    /// Returns the expected immutable physical length in bytes.
    #[must_use]
    pub fn file_size_bytes(&self) -> u64 {
        self.raw_file().file.file_size_bytes
    }

    /// Returns the nonzero logical row count recorded at commit time.
    #[must_use]
    pub fn row_count(&self) -> NonZeroU64 {
        self.raw_file().file.row_count
    }

    /// Returns whether the file is managed or an external read-only legacy file.
    #[must_use]
    pub fn location_kind(&self) -> PinnedDataFileLocationKind {
        match &self.raw_file().file.location {
            DataFileLocation::Managed(_) => PinnedDataFileLocationKind::Managed,
            DataFileLocation::ExternalLegacy(_) => PinnedDataFileLocationKind::ExternalLegacy,
        }
    }

    /// Borrows typed source provenance, which contains no physical location.
    #[must_use]
    pub fn provenance(&self) -> &DataFileProvenance {
        &self.raw_file().file.provenance
    }

    /// Borrows stable partition metadata used for pruning.
    #[must_use]
    pub fn partition(&self) -> &MetadataMap {
        &self.raw_file().file.partition
    }

    /// Borrows stable statistics used for pruning.
    #[must_use]
    pub fn statistics(&self) -> &MetadataMap {
        &self.raw_file().file.statistics
    }

    /// Borrows additional implementation-independent file metadata.
    #[must_use]
    pub fn metadata(&self) -> &MetadataMap {
        &self.raw_file().file.metadata
    }

    /// Borrows the content checksum recorded at commit time.
    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.raw_file().file.checksum
    }

    /// Returns when the complete physical candidate was created, in UTC Unix nanoseconds.
    #[must_use]
    pub fn created_at_ns(&self) -> u64 {
        self.raw_file().file.created_at_ns
    }

    /// Returns the first snapshot version that exposed this file.
    #[must_use]
    pub fn added_snapshot_version(&self) -> u64 {
        self.raw_file().added_snapshot_version
    }

    /// Returns the first snapshot version that stopped exposing this file, when present.
    #[must_use]
    pub fn removed_snapshot_version(&self) -> Option<u64> {
        self.raw_file().removed_snapshot_version
    }

    /// Returns the committed physical lifecycle state.
    #[must_use]
    pub fn state(&self) -> DataFileState {
        self.raw_file().state
    }

    /// Opens the physical file without exposing its Catalog path.
    ///
    /// Managed paths are traversed descriptor-relative with `NOFOLLOW`. The opened object must be
    /// a secure regular file of the Catalog-recorded size. External legacy files are additionally
    /// hashed through the same opened descriptor and compared with the Catalog checksum before the
    /// cursor is rewound to zero. The returned reader retains the same opaque snapshot owner.
    ///
    /// This is synchronous physical I/O and must run on a bounded blocking scan worker, never on an
    /// async runtime worker. External legacy opens perform a full-file checksum pass, so an
    /// executor must open one reader per physical file scan rather than one reader per parallel
    /// worker for the same file.
    pub fn open_reader(&self) -> Result<PinnedDataFileReader> {
        let file = open_pinned_file(&self.owner._data_directory, self.raw_file())?;
        Ok(PinnedDataFileReader {
            file,
            _owner: Arc::clone(&self.owner),
            table_id: self.table_id(),
            file_id: self.file_id(),
            file_size_bytes: self.file_size_bytes(),
            row_count: self.row_count(),
        })
    }

    fn raw_file(&self) -> &DataFileDescriptor {
        &self.owner.snapshot.tables()[self.table_index]
            .files()
            .as_slice()[self.file_index]
    }
}

impl fmt::Debug for PinnedDataFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedDataFile")
            .field("file_id", &self.file_id())
            .field("table_id", &self.table_id())
            .field("schema_version", &self.schema_version())
            .field("file_size_bytes", &self.file_size_bytes())
            .field("row_count", &self.row_count())
            .field("location_kind", &self.location_kind())
            .finish_non_exhaustive()
    }
}

/// A controlled physical reader that retains its pinned snapshot owner.
pub struct PinnedDataFileReader {
    file: File,
    _owner: Arc<PinnedSnapshotOwner>,
    table_id: TableId,
    file_id: FileId,
    file_size_bytes: u64,
    row_count: NonZeroU64,
}

impl PinnedDataFileReader {
    /// Returns the immutable Catalog file identity associated with this descriptor.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.file_id
    }

    /// Returns the immutable owning table identity associated with this descriptor.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the physical length verified when the descriptor was opened.
    #[must_use]
    pub const fn file_size_bytes(&self) -> u64 {
        self.file_size_bytes
    }

    /// Returns the logical row count recorded when this immutable file was committed.
    #[must_use]
    pub const fn row_count(&self) -> NonZeroU64 {
        self.row_count
    }

    /// Reads from an absolute byte offset without changing this reader's sequential cursor.
    ///
    /// The request is capped at the Catalog-verified file length even if the physical descriptor
    /// later observes a larger object. This method does not clone or expose the descriptor, so
    /// concurrent Parquet range readers continue to retain the same opaque snapshot owner.
    pub fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || offset >= self.file_size_bytes {
            return Ok(0);
        }
        let remaining = self.file_size_bytes - offset;
        let read_length = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        read_file_at(&self.file, &mut buffer[..read_length], offset)
    }

    /// Fills the complete buffer from an absolute byte offset.
    ///
    /// Requests outside the Catalog-verified range fail with [`io::ErrorKind::UnexpectedEof`]
    /// before physical I/O begins. Interrupted reads are retried without changing another
    /// cursor's position.
    pub fn read_exact_at(&self, mut offset: u64, mut buffer: &mut [u8]) -> io::Result<()> {
        let requested = u64::try_from(buffer.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "pinned data-file range length does not fit u64",
            )
        })?;
        if offset
            .checked_add(requested)
            .is_none_or(|end| end > self.file_size_bytes)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pinned data-file range exceeds verified length",
            ));
        }
        while !buffer.is_empty() {
            match self.read_at(offset, buffer) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "pinned data-file descriptor ended before verified length",
                    ));
                }
                Ok(read) => {
                    offset = offset.checked_add(read as u64).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "pinned data-file range offset overflowed",
                        )
                    })?;
                    buffer = &mut buffer[read..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn read_file_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buffer, offset)
}

#[cfg(not(unix))]
fn read_file_at(_file: &File, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "pinned data-file random access is unsupported on this platform",
    ))
}

impl fmt::Debug for PinnedDataFileReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PinnedDataFileReader")
            .field("file_id", &self.file_id)
            .field("table_id", &self.table_id)
            .field("file_size_bytes", &self.file_size_bytes)
            .finish_non_exhaustive()
    }
}

impl Read for PinnedDataFileReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(buffer)
    }
}

impl Seek for PinnedDataFileReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

#[cfg(unix)]
fn open_pinned_file(
    data_directory: &OwnedDataDirectory,
    descriptor: &DataFileDescriptor,
) -> Result<File> {
    let managed;
    let (root, path) = match &descriptor.file.location {
        DataFileLocation::Managed(path) => {
            managed = true;
            let root = data_directory.try_clone_root_directory().map_err(|_| {
                file_io_error(
                    descriptor,
                    "clone owned data-root descriptor for pinned read",
                )
            })?;
            (root, Path::new(path.as_str()))
        }
        DataFileLocation::ExternalLegacy(path) => {
            managed = false;
            let root = rustix::fs::open(
                Path::new("/"),
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::DIRECTORY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map(File::from)
            .map_err(|error| map_open_error(descriptor, "open external root", error))?;
            (root, Path::new(path.as_str()))
        }
    };

    let components = normal_path_components(path, managed, descriptor)?;
    let Some((file_name, directories)) = components.split_last() else {
        return Err(file_corruption(
            descriptor,
            "catalog data-file location has no file component",
        ));
    };
    let mut parent = root;
    for directory_name in directories {
        parent = open_directory_at(&parent, directory_name, managed, descriptor)?;
    }
    let file_descriptor = rustix::fs::openat(
        &parent,
        *file_name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| map_open_error(descriptor, "open pinned data file", error))?;
    let mut file = File::from(file_descriptor);
    let opened_metadata = validate_open_file(&file, managed, descriptor)?;
    if !managed {
        validate_external_legacy_content(&mut file, &opened_metadata, descriptor)?;
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_pinned_file(
    _data_directory: &OwnedDataDirectory,
    descriptor: &DataFileDescriptor,
) -> Result<File> {
    Err(ServerError::new(
        ErrorCode::UnsupportedPlan,
        "secure pinned data-file opening is unsupported on this platform",
    )
    .with_table_id(descriptor.table_id()))
}

#[cfg(unix)]
fn normal_path_components<'a>(
    path: &'a Path,
    managed: bool,
    descriptor: &DataFileDescriptor,
) -> Result<Vec<&'a std::ffi::OsStr>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir if !managed => {}
            Component::Normal(value) => components.push(value),
            _ => {
                return Err(file_corruption(
                    descriptor,
                    "catalog data-file location contains an unsafe component",
                ));
            }
        }
    }
    Ok(components)
}

#[cfg(unix)]
fn open_directory_at(
    parent: &File,
    name: &std::ffi::OsStr,
    managed: bool,
    descriptor: &DataFileDescriptor,
) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let directory_descriptor = rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| map_open_error(descriptor, "open pinned data-file directory", error))?;
    let directory = File::from(directory_descriptor);
    let metadata = directory
        .metadata()
        .map_err(|_| file_io_error(descriptor, "inspect pinned data-file directory descriptor"))?;
    if !metadata.is_dir() {
        return Err(file_corruption(
            descriptor,
            "pinned data-file parent is not a directory",
        ));
    }
    if managed {
        let mode = metadata.permissions().mode() & 0o777;
        if metadata.uid() != rustix::process::geteuid().as_raw() || mode & 0o077 != 0 {
            return Err(file_corruption(
                descriptor,
                "managed data-file directory ownership or permissions are invalid",
            ));
        }
    }
    Ok(directory)
}

#[cfg(unix)]
fn validate_open_file(
    file: &File,
    managed: bool,
    descriptor: &DataFileDescriptor,
) -> Result<Metadata> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = file
        .metadata()
        .map_err(|_| file_io_error(descriptor, "inspect pinned data-file descriptor"))?;
    if !metadata.is_file() {
        return Err(file_corruption(
            descriptor,
            "pinned data-file descriptor is not a regular file",
        ));
    }
    if metadata.len() != descriptor.file.file_size_bytes {
        return Err(file_corruption(
            descriptor,
            "pinned data-file length does not match Catalog metadata",
        ));
    }
    if managed {
        let mode = metadata.permissions().mode() & 0o777;
        if metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.nlink() != 1
            || mode & 0o077 != 0
        {
            return Err(file_corruption(
                descriptor,
                "managed data-file ownership, links, or permissions are invalid",
            ));
        }
    }
    Ok(metadata)
}

#[cfg(unix)]
fn validate_external_legacy_content(
    file: &mut File,
    opened_metadata: &Metadata,
    descriptor: &DataFileDescriptor,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    file.seek(SeekFrom::Start(0)).map_err(|_| {
        file_io_error(
            descriptor,
            "rewind external legacy data file before checksum",
        )
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => hasher.update(&buffer[..read]),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                return Err(file_io_error(
                    descriptor,
                    "checksum external legacy data file",
                ));
            }
        }
    }

    let checked_metadata = file
        .metadata()
        .map_err(|_| file_io_error(descriptor, "reinspect external legacy data-file descriptor"))?;
    if !checked_metadata.is_file()
        || opened_metadata.dev() != checked_metadata.dev()
        || opened_metadata.ino() != checked_metadata.ino()
        || opened_metadata.len() != checked_metadata.len()
        || opened_metadata.mtime() != checked_metadata.mtime()
        || opened_metadata.mtime_nsec() != checked_metadata.mtime_nsec()
        || opened_metadata.ctime() != checked_metadata.ctime()
        || opened_metadata.ctime_nsec() != checked_metadata.ctime_nsec()
    {
        return Err(file_corruption(
            descriptor,
            "external legacy data file changed during checksum validation",
        ));
    }

    let actual_checksum = format!("sha256-v1:{:x}", hasher.finalize());
    if actual_checksum != descriptor.file.checksum {
        return Err(file_corruption(
            descriptor,
            "external legacy data-file checksum does not match Catalog metadata",
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(|_| {
        file_io_error(
            descriptor,
            "rewind external legacy data file after checksum",
        )
    })?;
    Ok(())
}

#[cfg(unix)]
fn map_open_error(
    descriptor: &DataFileDescriptor,
    operation: &str,
    error: rustix::io::Errno,
) -> ServerError {
    if matches!(
        error,
        rustix::io::Errno::NOENT | rustix::io::Errno::LOOP | rustix::io::Errno::NOTDIR
    ) {
        file_corruption(descriptor, "pinned data-file location is missing or unsafe")
    } else {
        file_io_error(descriptor, operation)
    }
}

fn file_corruption(descriptor: &DataFileDescriptor, reason: &str) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!("{reason} file_id=[{}]", descriptor.file_id()),
    )
    .with_table_id(descriptor.table_id())
}

fn file_io_error(descriptor: &DataFileDescriptor, operation: &str) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!("{operation} failed file_id=[{}]", descriptor.file_id()),
    )
    .with_table_id(descriptor.table_id())
}
