//! Lease-bound query manifests and controlled physical file readers.

use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Component, Path};
use std::sync::Arc;

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
    retained_schema_charge_bytes: Vec<NonZeroU64>,
    projection_profile: PinnedSnapshotProjectionProfile,
    retained_memory_bytes: NonZeroU64,
    _pins: Vec<crate::SqliteCatalogRuntimeSnapshotPin>,
    _data_directory: Arc<OwnedDataDirectory>,
    // Forks retain the original GC pins and accounting until their last reader completes.
    _parent: Option<Arc<PinnedSnapshotOwner>>,
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
        let projection_profile =
            projection_profile(&snapshot, retained_schema_charge_bytes.as_slice())?;
        let retained_memory_bytes = retained_snapshot_bytes(&snapshot, &projection_profile)?;
        Ok(Self {
            owner: Arc::new(PinnedSnapshotOwner {
                snapshot,
                descriptor,
                retained_schema_charge_bytes,
                projection_profile,
                retained_memory_bytes,
                _pins: pins,
                _data_directory: data_directory,
                _parent: None,
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

    /// Conservative post-decode ownership charge; excludes the completed schema decode peak.
    pub fn retained_memory_bytes(&self) -> NonZeroU64 {
        self.owner.retained_memory_bytes
    }

    /// Rebinds a unique snapshot to an already-admitted lease from the same issuer and query.
    ///
    /// All old accounting remains active until replacement succeeds. Once a table or file
    /// capability has escaped, callers cannot change its authenticated ownership descriptor.
    pub fn rebind_memory_lease(&mut self, lease: CatalogSnapshotMemoryLease) -> Result<()> {
        if !self.owner._memory_lease.has_same_issuer(&lease)
            || self.owner._memory_lease.query_instance_id() != lease.query_instance_id()
            || lease.reserved_bytes() < self.owner.retained_memory_bytes
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "replacement snapshot memory lease has invalid authority, query, or size",
            ));
        }
        let owner = Arc::get_mut(&mut self.owner).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Conflict,
                "shared snapshot cannot change its memory lease",
            )
        })?;
        owner._memory_lease = lease;
        Ok(())
    }

    /// Forks selected immutable manifests under a newly admitted query reservation.
    ///
    /// The original GC pins remain retained by the child, including after the fixed snapshot
    /// lease is released. No Catalog lookup occurs, so later schema changes or table replacement
    /// cannot change the child's identity, schema, file order, or visibility boundary.
    ///
    /// :param indices: Nonempty, strictly increasing canonical table ordinals.
    /// :param lease: Same-issuer reservation attributed to the destination query, covering at
    ///     least the source snapshot's retained charge before metadata is cloned.
    /// :returns: Independently accounted child whose physical readers retain both owners.
    /// :raises ServerError: If authority, attribution, size, or table selection is invalid.
    pub fn fork_for_query(
        &self,
        indices: &[usize],
        lease: CatalogSnapshotMemoryLease,
    ) -> Result<Self> {
        if !self.owner._memory_lease.has_same_issuer(&lease)
            || lease.query_instance_id().is_none()
            || lease.reserved_bytes() < self.owner.retained_memory_bytes
            || indices.is_empty()
            || indices.windows(2).any(|pair| pair[0] >= pair[1])
            || indices
                .last()
                .is_some_and(|index| *index >= self.table_count())
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "snapshot fork requires valid table ordinals and admitted query authority",
            ));
        }
        let snapshot = CatalogQuerySnapshot::new(
            self.catalog_revision(),
            indices
                .iter()
                .map(|index| self.owner.snapshot.tables()[*index].clone())
                .collect(),
        )
        .map_err(|error| ServerError::new(ErrorCode::DataCorruption, error.to_string()))?;
        let retained_schema_charge_bytes = indices
            .iter()
            .map(|index| self.owner.retained_schema_charge_bytes[*index])
            .collect::<Vec<_>>();
        let projection_profile = projection_profile(&snapshot, &retained_schema_charge_bytes)?;
        let retained_memory_bytes = retained_snapshot_bytes(&snapshot, &projection_profile)?;
        if retained_memory_bytes > lease.reserved_bytes() {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "snapshot fork exceeds its admitted retained metadata charge",
            ));
        }
        Ok(Self {
            owner: Arc::new(PinnedSnapshotOwner {
                descriptor: CatalogSnapshotPinDescriptor {
                    catalog_revision: snapshot.catalog_revision(),
                    selectors: snapshot
                        .tables()
                        .iter()
                        .map(|table| table.selector().clone())
                        .collect(),
                },
                snapshot,
                retained_schema_charge_bytes,
                projection_profile,
                retained_memory_bytes,
                _pins: Vec::new(),
                _data_directory: Arc::clone(&self.owner._data_directory),
                _parent: Some(Arc::clone(&self.owner)),
                _memory_lease: lease,
            }),
        })
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

/// Both callers build `retained_schema_charge_bytes` one-to-one with `snapshot.tables()`.
fn projection_profile(
    snapshot: &CatalogQuerySnapshot,
    retained_schema_charge_bytes: &[NonZeroU64],
) -> Result<PinnedSnapshotProjectionProfile> {
    let table_count = u64::try_from(snapshot.tables().len()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "pinned projection table count exceeds u64",
        )
        .with_retryable(false)
    })?;
    let file_count = snapshot.tables().iter().try_fold(0_u64, |total, table| {
        let count = u64::try_from(table.files().len()).map_err(|_| {
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

/// The worker constructs tightly sized table/file/lineage containers. Count the complete
/// serialized metadata without allocating a copy, then apply the same 32x expansion used by
/// snapshot read admission. Explicit sidecar and decoded-schema charges cover nonserialized
/// ownership. This is a logical allocator envelope, not an RSS measurement.
fn retained_snapshot_bytes(
    snapshot: &CatalogQuerySnapshot,
    profile: &PinnedSnapshotProjectionProfile,
) -> Result<NonZeroU64> {
    struct Counter(u64);
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("snapshot metadata length overflowed"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, snapshot).map_err(|error| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("cannot measure retained snapshot metadata error=[{error}]"),
        )
    })?;
    counter
        .0
        .checked_mul(32)
        .and_then(|bytes| bytes.checked_add(profile.table_count.checked_mul(4096)?))
        .and_then(|bytes| bytes.checked_add(profile.file_count.checked_mul(512)?))
        .and_then(|bytes| bytes.checked_add(profile.retained_schema_bytes))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .and_then(NonZeroU64::new)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "retained snapshot memory charge overflowed",
            )
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
        self.raw_table().files().len()
    }

    /// Returns true when the exact snapshot contains no persisted files.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw_table().files().is_empty()
    }

    /// Creates lease-bound file tasks in the exact deterministic Catalog manifest order.
    pub fn files(&self) -> impl ExactSizeIterator<Item = PinnedDataFile> + '_ {
        (0..self.file_count()).map(|file_index| PinnedDataFile {
            owner: Arc::clone(&self.owner),
            table_index: self.table_index,
            file_index,
        })
    }

    /// Creates one lease-bound file task by deterministic manifest index.
    #[must_use]
    pub fn file(&self, file_index: usize) -> Option<PinnedDataFile> {
        (file_index < self.file_count()).then(|| PinnedDataFile {
            owner: Arc::clone(&self.owner),
            table_index: self.table_index,
            file_index,
        })
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

    /// Returns the managed file ownership class.
    #[must_use]
    pub fn location_kind(&self) -> PinnedDataFileLocationKind {
        match &self.raw_file().file.location {
            DataFileLocation::Managed(_) => PinnedDataFileLocationKind::Managed,
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
    /// a secure regular file of the Catalog-recorded size. Opening may perform blocking
    /// filesystem operations and must run on a blocking scan worker.
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
    let DataFileLocation::Managed(path) = &descriptor.file.location;
    let root = data_directory.try_clone_root_directory().map_err(|_| {
        file_io_error(
            descriptor,
            "clone owned data-root descriptor for pinned read",
        )
    })?;
    let components = normal_path_components(Path::new(path.as_str()), descriptor)?;
    let Some((file_name, directories)) = components.split_last() else {
        return Err(file_corruption(
            descriptor,
            "catalog data-file location has no file component",
        ));
    };
    let mut parent = root;
    for directory_name in directories {
        parent = open_directory_at(&parent, directory_name, descriptor)?;
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
    let file = File::from(file_descriptor);
    validate_open_file(&file, descriptor)?;
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
    descriptor: &DataFileDescriptor,
) -> Result<Vec<&'a std::ffi::OsStr>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
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
    let mode = metadata.permissions().mode() & 0o777;
    if metadata.uid() != rustix::process::geteuid().as_raw() || mode & 0o077 != 0 {
        return Err(file_corruption(
            descriptor,
            "managed data-file directory ownership or permissions are invalid",
        ));
    }
    Ok(directory)
}

#[cfg(unix)]
fn validate_open_file(file: &File, descriptor: &DataFileDescriptor) -> Result<()> {
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
