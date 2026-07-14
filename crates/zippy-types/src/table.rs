//! Persistent namespace, table, schema, snapshot, and data-file metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU64;
use std::path::{Component, Path};
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, TableId};

/// A deterministic finite floating-point metadata value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FiniteMetadataFloat(u64);

impl FiniteMetadataFloat {
    /// Creates a finite value and canonicalizes both signed zeros to positive zero.
    pub fn new(value: f64) -> Result<Self, NonFiniteMetadataFloat> {
        if !value.is_finite() {
            return Err(NonFiniteMetadataFloat);
        }
        let canonical = if value == 0.0 { 0.0 } else { value };
        Ok(Self(canonical.to_bits()))
    }

    /// Returns the validated finite floating-point value.
    #[must_use]
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl Serialize for FiniteMetadataFloat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.get())
    }
}

impl<'de> Deserialize<'de> for FiniteMetadataFloat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(f64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Metadata floats must not contain NaN or either infinity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("metadata floating-point value must be finite")]
pub struct NonFiniteMetadataFloat;

/// A deterministic metadata value with no lossy floating-point representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MetadataScalar {
    /// An explicit missing value.
    Null,
    /// A Boolean value.
    Boolean(bool),
    /// A signed integer value.
    Int64(i64),
    /// An unsigned integer value.
    #[serde(rename = "uint64")]
    UInt64(u64),
    /// A deterministic finite IEEE-754 binary64 value.
    Float64(FiniteMetadataFloat),
    /// A UTF-8 string value.
    String(String),
    /// An opaque byte sequence.
    Bytes(Vec<u8>),
}

/// Deterministically ordered structured metadata.
pub type MetadataMap = BTreeMap<String, MetadataScalar>;

/// The byte-exact, case-sensitive name of the default namespace.
pub const DEFAULT_NAMESPACE_NAME: &str = "default";

/// A validated catalog namespace or table name.
///
/// Names are compared byte-for-byte and are case-sensitive. Zippy performs no
/// implicit Unicode normalization, case folding, trimming, or locale mapping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CatalogName(String);

impl CatalogName {
    /// Creates a non-empty name without control characters.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidCatalogName> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidCatalogName::Empty);
        }
        if value.trim().is_empty() {
            return Err(InvalidCatalogName::WhitespaceOnly);
        }
        if value.chars().any(char::is_control) {
            return Err(InvalidCatalogName::ControlCharacter);
        }
        Ok(Self(value))
    }

    /// Creates the conventional `default` namespace name.
    #[must_use]
    pub fn default_namespace() -> Self {
        Self(DEFAULT_NAMESPACE_NAME.to_owned())
    }

    /// Borrows the byte-exact name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CatalogName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// An invalid catalog name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidCatalogName {
    /// Empty catalog names are ambiguous.
    #[error("catalog name must not be empty")]
    Empty,
    /// Whitespace-only names are not discoverable safely.
    #[error("catalog name must not contain only whitespace")]
    WhitespaceOnly,
    /// Control characters, including NUL, are forbidden.
    #[error("catalog name must not contain control characters")]
    ControlCharacter,
}

/// The persistent storage and delivery semantics of a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableKind {
    /// An append-only logical log whose atomic write unit is a batch.
    #[serde(rename = "append_table")]
    Append,
    /// An append log with explicit, atomic frame boundaries.
    #[serde(rename = "frame_table")]
    Frame,
    /// A changelog with a materialized latest-value snapshot.
    #[serde(rename = "key_value_table")]
    KeyValue,
}

impl TableKind {
    /// Returns the stable catalog representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append_table",
            Self::Frame => "frame_table",
            Self::KeyValue => "key_value_table",
        }
    }
}

impl fmt::Display for TableKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TableKind {
    type Err = ParseTableKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "append_table" => Ok(Self::Append),
            "frame_table" => Ok(Self::Frame),
            "key_value_table" => Ok(Self::KeyValue),
            _ => Err(ParseTableKindError {
                value: value.to_owned(),
            }),
        }
    }
}

/// An error returned when parsing an unknown table kind.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown table kind value=[{value}]")]
pub struct ParseTableKindError {
    /// The unrecognized value.
    pub value: String,
}

/// A validated, non-empty Arrow column name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ColumnName(String);

impl ColumnName {
    /// Creates a validated column name.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidTableDefinition> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidTableDefinition::EmptyColumnName);
        }
        if value.trim().is_empty() {
            return Err(InvalidTableDefinition::WhitespaceOnlyColumnName);
        }
        if value.chars().any(char::is_control) {
            return Err(InvalidTableDefinition::ControlCharacterInColumnName);
        }
        Ok(Self(value))
    }

    /// Borrows the column name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the column name.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for ColumnName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// An invalid table-definition invariant.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidTableDefinition {
    /// A referenced column name is empty.
    #[error("table definition column name must not be empty")]
    EmptyColumnName,
    /// A referenced column name contains only whitespace.
    #[error("table definition column name must not contain only whitespace")]
    WhitespaceOnlyColumnName,
    /// A referenced column contains a control character, including NUL.
    #[error("table definition column name must not contain control characters")]
    ControlCharacterInColumnName,
    /// A column appears more than once in an ordered column set.
    #[error("duplicate table definition column name=[{name}]")]
    DuplicateColumn {
        /// The duplicate column name.
        name: String,
    },
    /// A key-value table has no key columns.
    #[error("key-value table must define at least one key column")]
    MissingKeyColumns,
}

/// An ordered set of validated, unique column names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ColumnSet(Vec<ColumnName>);

impl ColumnSet {
    /// Creates an ordered column set while rejecting duplicates.
    pub fn new(columns: Vec<ColumnName>) -> Result<Self, InvalidTableDefinition> {
        let mut seen = BTreeSet::new();
        for column in &columns {
            if !seen.insert(column.as_str()) {
                return Err(InvalidTableDefinition::DuplicateColumn {
                    name: column.as_str().to_owned(),
                });
            }
        }
        Ok(Self(columns))
    }

    /// Borrows the ordered columns.
    #[must_use]
    pub fn as_slice(&self) -> &[ColumnName] {
        &self.0
    }

    /// Returns true when no columns are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<'de> Deserialize<'de> for ColumnSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let columns = Vec::<ColumnName>::deserialize(deserializer)?;
        Self::new(columns).map_err(serde::de::Error::custom)
    }
}

/// Definition fields specific to an append table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendTableDefinition {
    /// An optional event-time column used by explicit time-aware operations.
    pub event_time_column: Option<ColumnName>,
}

/// Definition fields specific to a frame table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameTableDefinition {
    /// The mandatory column that groups rows into independently updated frames.
    pub by: ColumnName,
}

/// Definition fields specific to a key-value table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyValueTableDefinition {
    keys: ColumnSet,
}

impl KeyValueTableDefinition {
    /// Creates a key-value definition with at least one unique key column.
    pub fn new(keys: ColumnSet) -> Result<Self, InvalidTableDefinition> {
        if keys.is_empty() {
            return Err(InvalidTableDefinition::MissingKeyColumns);
        }
        Ok(Self { keys })
    }

    /// Borrows the ordered primary-key columns.
    #[must_use]
    pub fn keys(&self) -> &ColumnSet {
        &self.keys
    }
}

impl<'de> Deserialize<'de> for KeyValueTableDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawDefinition {
            keys: ColumnSet,
        }

        Self::new(RawDefinition::deserialize(deserializer)?.keys).map_err(serde::de::Error::custom)
    }
}

/// The typed definition that determines table storage and mutation semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "definition")]
pub enum TableDefinition {
    /// An append-only table definition.
    #[serde(rename = "append_table")]
    Append(AppendTableDefinition),
    /// An atomic-frame table definition.
    #[serde(rename = "frame_table")]
    Frame(FrameTableDefinition),
    /// A materialized key-value table definition.
    #[serde(rename = "key_value_table")]
    KeyValue(KeyValueTableDefinition),
}

impl TableDefinition {
    /// Returns the table kind implied by this definition.
    #[must_use]
    pub const fn kind(&self) -> TableKind {
        match self {
            Self::Append(_) => TableKind::Append,
            Self::Frame(_) => TableKind::Frame,
            Self::KeyValue(_) => TableKind::KeyValue,
        }
    }
}

/// A table reference accepted by catalog and query APIs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableRef {
    /// Resolve a table by its immutable identifier.
    Id {
        /// The immutable table identifier.
        table_id: TableId,
    },
    /// Resolve a table by its namespace and table names.
    Name {
        /// The catalog namespace name.
        namespace: CatalogName,
        /// The table name inside the namespace.
        table: CatalogName,
    },
}

impl From<TableId> for TableRef {
    fn from(table_id: TableId) -> Self {
        Self::Id { table_id }
    }
}

/// The persistent descriptor of a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDescriptor {
    /// The immutable namespace identifier.
    pub namespace_id: NamespaceId,
    /// The globally unique namespace name.
    pub name: CatalogName,
    /// The creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
}

/// The lifecycle state of a persistent table descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableState {
    /// The table accepts operations allowed by its definition.
    Active,
    /// A drop operation is waiting for leases or cleanup.
    Dropping,
    /// The table is logically removed from normal discovery.
    Dropped,
    /// The table requires operator intervention before use.
    Failed,
}

/// The compatibility computed by the catalog for a schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCompatibility {
    /// The first schema of a table has no predecessor.
    Initial,
    /// Existing readers and writers remain compatible.
    Full,
    /// Existing stored data can be read using the new schema.
    Backward,
    /// New data can be read using the previous schema.
    Forward,
    /// Compatibility is intentionally broken and requires explicit migration.
    Breaking,
}

/// An Arrow schema encoded for persistence or transport.
///
/// Catalog commands interpret the bytes as one schema-only Arrow IPC stream:
/// the stream must contain a schema message, no record batches, one end marker,
/// and no trailing bytes. Other protocols may define a different envelope and
/// must not pass their bare schema flatbuffers to the Catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SchemaIpcPayload(Vec<u8>);

impl SchemaIpcPayload {
    /// Creates a non-empty opaque schema payload.
    ///
    /// This transport-level constructor does not parse Arrow IPC. Catalog
    /// command execution performs the protocol-specific validation before any
    /// write transaction begins.
    pub fn new(bytes: Vec<u8>) -> Result<Self, EmptySchemaPayload> {
        if bytes.is_empty() {
            return Err(EmptySchemaPayload);
        }
        Ok(Self(bytes))
    }

    /// Borrows the encoded bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the payload and returns its encoded bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl<'de> Deserialize<'de> for SchemaIpcPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::<u8>::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// An error returned for an empty encoded schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("schema IPC payload must not be empty")]
pub struct EmptySchemaPayload;

/// The persistent descriptor of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDescriptor {
    /// The immutable table identifier.
    pub table_id: TableId,
    /// The owning namespace identifier.
    pub namespace_id: NamespaceId,
    /// The table name, unique within its namespace.
    pub name: CatalogName,
    /// The typed table definition.
    pub definition: TableDefinition,
    /// The schema used by new writes.
    pub current_schema_version: u64,
    /// The latest committed persisted snapshot.
    pub current_snapshot_version: u64,
    /// The latest durable producer fencing epoch.
    pub writer_epoch: u64,
    /// The table lifecycle state.
    pub state: TableState,
    /// Stable, implementation-independent table configuration.
    pub config: BTreeMap<String, String>,
    /// The creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
    /// The last metadata update time as Unix nanoseconds in UTC.
    pub updated_at_ns: u64,
}

impl TableDescriptor {
    /// Returns the kind implied by the table definition.
    #[must_use]
    pub const fn kind(&self) -> TableKind {
        self.definition.kind()
    }
}

/// An immutable version of a table schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDescriptor {
    /// The owning table identifier.
    pub table_id: TableId,
    /// The monotonically increasing schema version.
    pub schema_version: u64,
    /// The Arrow schema encoded as IPC.
    pub schema_ipc: SchemaIpcPayload,
    /// The versioned catalog digest, currently `sha256-v1:<64 lowercase hex>`.
    pub schema_hash: String,
    /// Compatibility computed by the catalog against the preceding version.
    pub compatibility: SchemaCompatibility,
    /// The creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
}

/// The operation that created a persisted table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotOperation {
    /// The empty initial snapshot created with the table.
    Create,
    /// One or more sealed segments were persisted.
    Persist,
    /// Existing data files were compacted into replacement files.
    Compaction,
    /// Existing data was imported from a legacy or external source.
    Import,
    /// Retention changed the visible file set.
    Retention,
}

/// An immutable persisted snapshot descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSnapshot {
    /// The owning table identifier.
    pub table_id: TableId,
    /// The monotonically increasing snapshot version.
    pub snapshot_version: u64,
    /// The preceding snapshot, absent only for the initial version.
    pub parent_snapshot_version: Option<u64>,
    /// The operation that created this snapshot.
    pub operation: SnapshotOperation,
    /// The catalog commit log sequence associated with this snapshot.
    pub commit_lsn: u64,
    /// The durable logical log boundary included in the snapshot.
    pub committed_log_offset: OffsetWatermark,
    /// The creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
    /// Stable summary values used for inspection and audit.
    pub summary: MetadataMap,
}

/// A managed path that is relative to the configured data root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ManagedDataPath(String);

impl ManagedDataPath {
    /// Validates and creates a managed relative data path.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidDataPath> {
        let value = value.into();
        validate_path(&value, false)?;
        Ok(Self(value))
    }

    /// Borrows the relative UTF-8 path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ManagedDataPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// An explicitly mounted absolute legacy path outside the managed data root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ExternalDataPath(String);

impl ExternalDataPath {
    /// Validates and creates an absolute external legacy path.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidDataPath> {
        let value = value.into();
        validate_path(&value, true)?;
        Ok(Self(value))
    }

    /// Borrows the absolute UTF-8 path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ExternalDataPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

fn validate_path(value: &str, must_be_absolute: bool) -> Result<(), InvalidDataPath> {
    if value.is_empty() {
        return Err(InvalidDataPath::Empty);
    }
    let path = Path::new(value);
    if path.is_absolute() != must_be_absolute {
        return if must_be_absolute {
            Err(InvalidDataPath::ExpectedAbsolute)
        } else {
            Err(InvalidDataPath::ExpectedRelative)
        };
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::CurDir | Component::Prefix(_)
        )
    }) {
        return Err(InvalidDataPath::UnsafeComponent);
    }
    Ok(())
}

/// An invalid managed or external data-file path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidDataPath {
    /// The path is empty.
    #[error("data path must not be empty")]
    Empty,
    /// A managed path must be relative to the data root.
    #[error("managed data path must be relative")]
    ExpectedRelative,
    /// An external legacy path must be absolute.
    #[error("external data path must be absolute")]
    ExpectedAbsolute,
    /// The path contains current, parent, or platform-prefix components.
    #[error("data path contains unsafe components")]
    UnsafeComponent,
}

/// The ownership and cleanup policy of a data-file location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "path", rename_all = "snake_case")]
pub enum DataFileLocation {
    /// A server-managed path relative to the data root.
    Managed(ManagedDataPath),
    /// An absolute read-only legacy path never removed by automatic GC.
    ExternalLegacy(ExternalDataPath),
}

/// The identity of a source segment across slot reuse and producer lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SourceSegmentIdentity {
    /// The stable segment slot identifier within a table.
    pub segment_id: u64,
    /// The generation that prevents slot reuse from causing false identity.
    pub generation: u64,
    /// The producer fencing epoch that created the segment.
    pub writer_epoch: u64,
}

/// The exact source-segment rows represented by a persisted data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSegmentLineage {
    /// The source segment identity.
    pub source: SourceSegmentIdentity,
    /// The non-empty source offset range materialized into the file.
    pub offsets: OffsetRange,
}

/// A non-empty, deterministic list of source segments represented by a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct SourceSegmentLineages(Vec<SourceSegmentLineage>);

impl SourceSegmentLineages {
    /// Creates lineage in strict source-identity order without duplicates.
    pub fn new(values: Vec<SourceSegmentLineage>) -> Result<Self, InvalidSourceSegmentLineages> {
        if values.is_empty() {
            return Err(InvalidSourceSegmentLineages::Empty);
        }
        for pair in values.windows(2) {
            match pair[0].source.cmp(&pair[1].source) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(InvalidSourceSegmentLineages::DuplicateIdentity {
                        identity: pair[0].source,
                    });
                }
                std::cmp::Ordering::Greater => {
                    return Err(InvalidSourceSegmentLineages::UnsortedIdentity {
                        previous: pair[0].source,
                        next: pair[1].source,
                    });
                }
            }
        }
        Ok(Self(values))
    }

    /// Borrows the ordered source lineage.
    #[must_use]
    pub fn as_slice(&self) -> &[SourceSegmentLineage] {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SourceSegmentLineages {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Invalid ordering or identity invariants for native source lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidSourceSegmentLineages {
    /// Native files must identify at least one source segment.
    #[error("native data-file source lineage must not be empty")]
    Empty,
    /// A source identity must occur exactly once.
    #[error("duplicate source segment identity identity=[{identity:?}]")]
    DuplicateIdentity {
        /// The repeated stable source identity.
        identity: SourceSegmentIdentity,
    },
    /// Source identities must be supplied in deterministic ascending order.
    #[error("source segment identities are not sorted previous=[{previous:?}] next=[{next:?}]")]
    UnsortedIdentity {
        /// The preceding out-of-order identity.
        previous: SourceSegmentIdentity,
        /// The following out-of-order identity.
        next: SourceSegmentIdentity,
    },
}

/// Validated exact offset coverage for one native data file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NativeDataFileProvenance {
    offsets: OffsetRange,
    source_lineage: SourceSegmentLineages,
}

impl NativeDataFileProvenance {
    /// Creates native provenance whose lineage is a disjoint contiguous union.
    pub fn new(
        offsets: OffsetRange,
        source_lineage: SourceSegmentLineages,
    ) -> Result<Self, InvalidNativeDataFileProvenance> {
        let mut ranges: Vec<_> = source_lineage
            .as_slice()
            .iter()
            .map(|lineage| lineage.offsets)
            .collect();
        ranges.sort_unstable_by_key(|range| (range.first(), range.last()));
        if ranges[0].first() != offsets.first() {
            return Err(InvalidNativeDataFileProvenance::CoverageMismatch);
        }
        for pair in ranges.windows(2) {
            if pair[1].first() <= pair[0].last() {
                return Err(InvalidNativeDataFileProvenance::OverlappingOffsets {
                    previous: pair[0],
                    next: pair[1],
                });
            }
            if pair[0].last().checked_next() != Some(pair[1].first()) {
                return Err(InvalidNativeDataFileProvenance::OffsetGap {
                    previous: pair[0].last(),
                    next: pair[1].first(),
                });
            }
        }
        if ranges.last().map(|range| range.last()) != Some(offsets.last()) {
            return Err(InvalidNativeDataFileProvenance::CoverageMismatch);
        }
        Ok(Self {
            offsets,
            source_lineage,
        })
    }

    /// Returns the exact total logical offset coverage.
    #[must_use]
    pub const fn offsets(&self) -> OffsetRange {
        self.offsets
    }

    /// Borrows the validated deterministic source lineage.
    #[must_use]
    pub const fn source_lineage(&self) -> &SourceSegmentLineages {
        &self.source_lineage
    }
}

impl<'de> Deserialize<'de> for NativeDataFileProvenance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawNativeProvenance {
            offsets: OffsetRange,
            source_lineage: SourceSegmentLineages,
        }
        let raw = RawNativeProvenance::deserialize(deserializer)?;
        Self::new(raw.offsets, raw.source_lineage).map_err(serde::de::Error::custom)
    }
}

/// Invalid native lineage offset coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidNativeDataFileProvenance {
    /// The lineage union endpoints do not equal the declared total range.
    #[error("native lineage union does not equal total offsets")]
    CoverageMismatch,
    /// Two source lineages claim at least one common logical offset.
    #[error("native lineage offsets overlap previous=[{previous:?}] next=[{next:?}]")]
    OverlappingOffsets {
        /// The preceding range in logical-offset order.
        previous: OffsetRange,
        /// The overlapping following range.
        next: OffsetRange,
    },
    /// Source lineage ranges leave an unrepresented logical offset.
    #[error("native lineage offsets contain a gap previous=[{previous}] next=[{next}]")]
    OffsetGap {
        /// The preceding range's last offset.
        previous: LogOffset,
        /// The following range's first offset.
        next: LogOffset,
    },
}

/// Mutually exclusive native and legacy data-file provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataFileProvenance {
    /// A server-produced file with exact offsets and non-empty source lineage.
    Native(NativeDataFileProvenance),
    /// An imported file whose logical boundary may be unavailable.
    Legacy {
        /// Reconstructed offsets, when migration could prove them.
        offsets: Option<OffsetRange>,
        /// Whether reads must preserve discovery order during migration.
        preserve_discovery_order: bool,
    },
}

/// A complete, fsynced file candidate not yet visible in any catalog snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingDataFile {
    /// The immutable file identity assigned before writing.
    pub file_id: FileId,
    /// The owning table.
    pub table_id: TableId,
    /// The schema version encoded in the file.
    pub schema_version: u64,
    /// The typed managed or external location.
    pub location: DataFileLocation,
    /// The physical file size in bytes.
    pub file_size_bytes: u64,
    /// The nonzero logical row count in the file.
    pub row_count: NonZeroU64,
    /// Typed native or legacy origin and its compatible ordering semantics.
    pub provenance: DataFileProvenance,
    /// Stable partition key/value pairs.
    pub partition: MetadataMap,
    /// Stable statistics used for pruning and inspection.
    pub statistics: MetadataMap,
    /// Additional implementation-independent metadata.
    pub metadata: MetadataMap,
    /// A lowercase content checksum including its algorithm prefix.
    pub checksum: String,
    /// The time the complete candidate was created as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
}

/// The lifecycle state of a committed catalog data-file record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFileState {
    /// The file is visible according to its snapshot version range.
    Committed,
    /// The file is no longer visible and is awaiting physical deletion.
    Deleting,
    /// The managed physical file has been deleted.
    Deleted,
}

/// Catalog-assigned visibility metadata for a committed immutable data file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataFileDescriptor {
    /// The complete pre-commit file metadata.
    pub file: PendingDataFile,
    /// Gateway-compatible scan position for a preserved legacy manifest.
    ///
    /// Frozen mutation wires carry this value separately from `PendingDataFile`; descriptor JSON
    /// includes it so inspection round trips remain lossless.
    pub legacy_discovery_order: Option<u64>,
    /// The first snapshot version in which the file is visible.
    pub added_snapshot_version: u64,
    /// The first snapshot version in which the file is no longer visible.
    pub removed_snapshot_version: Option<u64>,
    /// The committed file lifecycle state.
    pub state: DataFileState,
}

impl DataFileDescriptor {
    /// Validates lifecycle state against the immutable visibility interval.
    pub fn validate(&self) -> Result<(), InvalidDataFileDescriptor> {
        let preserve_discovery_order = matches!(
            &self.file.provenance,
            DataFileProvenance::Legacy {
                preserve_discovery_order: true,
                ..
            }
        );
        if preserve_discovery_order != self.legacy_discovery_order.is_some() {
            return Err(InvalidDataFileDescriptor::LegacyDiscoveryOrderMismatch {
                file_id: self.file_id(),
                preserve_discovery_order,
                has_discovery_order: self.legacy_discovery_order.is_some(),
            });
        }
        if matches!(self.state, DataFileState::Deleting | DataFileState::Deleted)
            && self.removed_snapshot_version.is_none()
        {
            return Err(InvalidDataFileDescriptor::RemovedStateWithoutVersion {
                file_id: self.file_id(),
                state: self.state,
            });
        }
        if self
            .removed_snapshot_version
            .is_some_and(|removed| removed <= self.added_snapshot_version)
        {
            return Err(InvalidDataFileDescriptor::InvalidVisibilityInterval {
                file_id: self.file_id(),
            });
        }
        Ok(())
    }

    /// Returns the immutable file identity.
    #[must_use]
    pub const fn file_id(&self) -> FileId {
        self.file.file_id
    }

    /// Returns the owning table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.file.table_id
    }

    /// Returns the Gateway-compatible scan position for a preserved legacy file.
    #[must_use]
    pub const fn legacy_discovery_order(&self) -> Option<u64> {
        self.legacy_discovery_order
    }

    /// Returns whether this file is visible at an exact historical snapshot.
    #[must_use]
    pub const fn is_visible_at(&self, snapshot_version: u64) -> bool {
        matches!(self.state, DataFileState::Committed)
            && self.added_snapshot_version <= snapshot_version
            && match self.removed_snapshot_version {
                Some(removed) => snapshot_version < removed,
                None => true,
            }
    }
}

impl<'de> Deserialize<'de> for DataFileDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawDataFileDescriptor {
            file: PendingDataFile,
            #[serde(default)]
            legacy_discovery_order: Option<u64>,
            added_snapshot_version: u64,
            removed_snapshot_version: Option<u64>,
            state: DataFileState,
        }
        let raw = RawDataFileDescriptor::deserialize(deserializer)?;
        let descriptor = Self {
            file: raw.file,
            legacy_discovery_order: raw.legacy_discovery_order,
            added_snapshot_version: raw.added_snapshot_version,
            removed_snapshot_version: raw.removed_snapshot_version,
            state: raw.state,
        };
        descriptor.validate().map_err(serde::de::Error::custom)?;
        Ok(descriptor)
    }
}

/// An invalid relationship between file lifecycle state and visibility versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidDataFileDescriptor {
    /// Preserved legacy provenance and its Catalog scan position must appear together.
    #[error(
        "legacy discovery order presence mismatch file_id=[{file_id}] preserve=[{preserve_discovery_order}] has_order=[{has_discovery_order}]"
    )]
    LegacyDiscoveryOrderMismatch {
        /// The invalid file identity.
        file_id: FileId,
        /// Whether provenance requires the old Gateway ordering contract.
        preserve_discovery_order: bool,
        /// Whether the Catalog descriptor retained a concrete scan position.
        has_discovery_order: bool,
    },
    /// Deleting and deleted files require the snapshot that removed them.
    #[error("removed file state lacks removal version file_id=[{file_id}] state=[{state:?}]")]
    RemovedStateWithoutVersion {
        /// The invalid file identity.
        file_id: FileId,
        /// The deleting or deleted state.
        state: DataFileState,
    },
    /// Removal must occur strictly after addition.
    #[error("invalid data file visibility interval file_id=[{file_id}]")]
    InvalidVisibilityInterval {
        /// The file with an invalid interval.
        file_id: FileId,
    },
}

/// A file list shared immutably by catalog and query snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFiles(Arc<[DataFileDescriptor]>);

impl DataFiles {
    /// Creates a deterministic immutable list in strict file-id order.
    pub fn new(files: Vec<DataFileDescriptor>) -> Result<Self, InvalidDataFiles> {
        for file in &files {
            file.validate()?;
            if file.state != DataFileState::Committed {
                return Err(InvalidDataFiles::UnreadableFileState {
                    file_id: file.file_id(),
                    state: file.state,
                });
            }
        }
        for pair in files.windows(2) {
            match pair[0].file_id().cmp(&pair[1].file_id()) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(InvalidDataFiles::DuplicateFileId {
                        file_id: pair[0].file_id(),
                    });
                }
                std::cmp::Ordering::Greater => return Err(InvalidDataFiles::UnsortedFileIds),
            }
        }
        Ok(Self(files.into()))
    }

    /// Borrows all file descriptors in manifest order.
    #[must_use]
    pub fn as_slice(&self) -> &[DataFileDescriptor] {
        &self.0
    }

    /// Returns the number of file descriptors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true when the snapshot contains no data files.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the wrapper and returns the shared immutable slice.
    #[must_use]
    pub fn into_arc(self) -> Arc<[DataFileDescriptor]> {
        self.0
    }
}

impl Default for DataFiles {
    fn default() -> Self {
        Self(Arc::from([]))
    }
}

/// Invalid immutable manifest file ordering or visibility metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidDataFiles {
    /// One descriptor has an invalid state/version relationship.
    #[error(transparent)]
    InvalidDescriptor(#[from] InvalidDataFileDescriptor),
    /// Read snapshots may contain only committed files.
    #[error("file state is not readable file_id=[{file_id}] state=[{state:?}]")]
    UnreadableFileState {
        /// The unreadable file identity.
        file_id: FileId,
        /// The deleting or deleted lifecycle state.
        state: DataFileState,
    },
    /// Files must be sorted by immutable identity for deterministic snapshots.
    #[error("data files must be strictly sorted by file id")]
    UnsortedFileIds,
    /// The same immutable file identity appeared more than once.
    #[error("duplicate data file id file_id=[{file_id}]")]
    DuplicateFileId {
        /// The repeated immutable file identity.
        file_id: FileId,
    },
}

impl Serialize for DataFiles {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DataFiles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::<DataFileDescriptor>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CatalogName, ColumnName, ColumnSet, DataFileLocation, DataFileProvenance, ExternalDataPath,
        FrameTableDefinition, KeyValueTableDefinition, ManagedDataPath, MetadataScalar,
        NativeDataFileProvenance, SourceSegmentIdentity, SourceSegmentLineage,
        SourceSegmentLineages, TableDefinition, TableKind,
    };
    use crate::{LogOffset, OffsetRange};

    #[test]
    fn table_kind_uses_stable_catalog_names() {
        for (kind, expected) in [
            (TableKind::Append, "append_table"),
            (TableKind::Frame, "frame_table"),
            (TableKind::KeyValue, "key_value_table"),
        ] {
            assert_eq!(kind.as_str(), expected);
            assert_eq!(expected.parse::<TableKind>(), Ok(kind));
            assert_eq!(
                serde_json::to_string(&kind).unwrap(),
                format!("\"{expected}\"")
            );
        }
    }

    #[test]
    fn key_value_definition_rejects_empty_and_duplicate_keys() {
        assert!(KeyValueTableDefinition::new(ColumnSet::default()).is_err());
        assert!(ColumnName::new("   ").is_err());
        assert!(ColumnName::new("bad\0column").is_err());
        let key = ColumnName::new("symbol").unwrap();
        assert!(ColumnSet::new(vec![key.clone(), key]).is_err());
        assert!(serde_json::from_str::<KeyValueTableDefinition>(r#"{"keys":[]}"#).is_err());
    }

    #[test]
    fn catalog_names_are_byte_exact_and_validated() {
        assert_ne!(
            CatalogName::new("Market").unwrap(),
            CatalogName::new("market").unwrap()
        );
        assert_eq!(CatalogName::default_namespace().as_str(), "default");
        assert!(CatalogName::new("").is_err());
        assert!(CatalogName::new(" \t ").is_err());
        assert!(CatalogName::new("bad\nname").is_err());
        assert!(serde_json::from_str::<CatalogName>("\"bad\\u0000name\"").is_err());
    }

    #[test]
    fn catalog_names_preserve_historical_long_values() {
        let name = CatalogName::new("x".repeat(2 * 1024)).unwrap();
        let encoded = serde_json::to_vec(&name).unwrap();
        assert_eq!(
            serde_json::from_slice::<CatalogName>(&encoded).unwrap(),
            name
        );
    }

    #[test]
    fn table_definition_has_a_golden_key_value_form() {
        let keys = ColumnSet::new(vec![ColumnName::new("symbol").unwrap()]).unwrap();
        let definition = TableDefinition::KeyValue(KeyValueTableDefinition::new(keys).unwrap());

        assert_eq!(
            serde_json::to_string(&definition).unwrap(),
            r#"{"kind":"key_value_table","definition":{"keys":["symbol"]}}"#
        );
    }

    #[test]
    fn frame_definition_requires_exactly_one_by_column() {
        let definition = TableDefinition::Frame(FrameTableDefinition {
            by: ColumnName::new("symbol").unwrap(),
        });

        assert_eq!(
            serde_json::to_string(&definition).unwrap(),
            r#"{"kind":"frame_table","definition":{"by":"symbol"}}"#
        );
        assert!(serde_json::from_str::<TableDefinition>(
            r#"{"kind":"frame_table","definition":{}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<TableDefinition>(
            r#"{"kind":"frame_table","definition":{"by":["symbol","venue"]}}"#
        )
        .is_err());
    }

    #[test]
    fn file_provenance_prevents_native_legacy_contradictions() {
        let lineage = SourceSegmentLineage {
            source: super::SourceSegmentIdentity {
                segment_id: 1,
                generation: 2,
                writer_epoch: 3,
            },
            offsets: OffsetRange::single(LogOffset::new(0)),
        };
        let provenance = DataFileProvenance::Native(
            NativeDataFileProvenance::new(
                OffsetRange::single(LogOffset::new(0)),
                SourceSegmentLineages::new(vec![lineage]).unwrap(),
            )
            .unwrap(),
        );

        assert!(serde_json::to_string(&provenance)
            .unwrap()
            .contains(r#""type":"native""#));
        assert!(SourceSegmentLineages::new(Vec::new()).is_err());
        assert!(serde_json::from_str::<DataFileProvenance>(
            r#"{"type":"native","offsets":{"first":0,"last":0},"source_lineage":[]}"#
        )
        .is_err());
        assert_eq!(
            serde_json::to_string(&MetadataScalar::UInt64(4)).unwrap(),
            r#"{"type":"uint64","value":4}"#
        );
        assert_eq!(
            serde_json::to_string(&MetadataScalar::Float64(
                super::FiniteMetadataFloat::new(1.25).unwrap()
            ))
            .unwrap(),
            r#"{"type":"float64","value":1.25}"#
        );
        assert!(super::FiniteMetadataFloat::new(f64::NAN).is_err());
        assert_eq!(
            super::FiniteMetadataFloat::new(-0.0).unwrap(),
            super::FiniteMetadataFloat::new(0.0).unwrap()
        );
    }

    #[test]
    fn native_lineage_rejects_identity_and_offset_ambiguity() {
        let source = |segment_id| SourceSegmentIdentity {
            segment_id,
            generation: 1,
            writer_epoch: 1,
        };
        let lineage = |segment_id, first, last| SourceSegmentLineage {
            source: source(segment_id),
            offsets: OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).unwrap(),
        };

        assert!(SourceSegmentLineages::new(vec![lineage(2, 1, 1), lineage(1, 0, 0)]).is_err());
        assert!(SourceSegmentLineages::new(vec![lineage(1, 0, 0), lineage(1, 1, 1)]).is_err());

        let overlapping =
            SourceSegmentLineages::new(vec![lineage(1, 0, 1), lineage(2, 1, 2)]).unwrap();
        assert!(NativeDataFileProvenance::new(
            OffsetRange::new(LogOffset::new(0), LogOffset::new(2)).unwrap(),
            overlapping,
        )
        .is_err());
        let gap = SourceSegmentLineages::new(vec![lineage(1, 0, 0), lineage(2, 2, 2)]).unwrap();
        assert!(NativeDataFileProvenance::new(
            OffsetRange::new(LogOffset::new(0), LogOffset::new(2)).unwrap(),
            gap,
        )
        .is_err());
        assert!(serde_json::from_str::<DataFileProvenance>(concat!(
            r#"{"type":"native","offsets":{"first":0,"last":2},"source_lineage":["#,
            r#"{"source":{"segment_id":1,"generation":1,"writer_epoch":1},"#,
            r#""offsets":{"first":0,"last":1}},{"source":{"segment_id":2,"#,
            r#""generation":1,"writer_epoch":1},"offsets":{"first":1,"last":2}}]}"#,
        ))
        .is_err());
    }

    #[test]
    fn data_file_locations_enforce_ownership_boundary() {
        assert!(ManagedDataPath::new("parquet/table/file.parquet").is_ok());
        assert!(ManagedDataPath::new("../escape.parquet").is_err());
        assert!(ManagedDataPath::new("/absolute.parquet").is_err());
        assert!(ExternalDataPath::new("/legacy/file.parquet").is_ok());
        assert!(ExternalDataPath::new("legacy/file.parquet").is_err());

        let location =
            DataFileLocation::Managed(ManagedDataPath::new("parquet/table/file.parquet").unwrap());
        assert_eq!(
            serde_json::to_string(&location).unwrap(),
            r#"{"type":"managed","path":"parquet/table/file.parquet"}"#
        );
    }
}
