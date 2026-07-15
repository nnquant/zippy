//! Persistent catalog commands, immutable historical snapshots, and service traits.

mod wire_v1;
mod wire_v2;
mod wire_v3;
mod wire_v4;

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Write;
use std::num::NonZeroU32;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::table::{
    CatalogName, DataFileDescriptor, DataFileLocation, DataFileState, DataFiles,
    NamespaceDescriptor, PendingDataFile, SchemaDescriptor, SchemaIpcPayload, TableDefinition,
    TableDescriptor, TableRef, TableSnapshot,
};
use crate::{
    BoxFuture, CommittedOffset, ConsumerOffsetAuthority, DurableConsumerId, FileId, JobId, LeaseId,
    NamespaceId, OffsetWatermark, ProducerId, RequestId, Result, SequenceWatermark, TableId,
};

/// The only catalog command encoding currently accepted for new writes.
pub const CURRENT_CATALOG_COMMAND_FORMAT_VERSION: u32 = 1;
/// The permanently assigned wire version for frozen V1 catalog records.
pub const CATALOG_COMMAND_FORMAT_VERSION_V1: u32 = 1;
/// The migration-only typed legacy table import format.
pub const LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2: u32 = 2;
/// The online typed legacy file-view reconcile format.
pub const LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3: u32 = 3;
/// The full legacy metadata projection and checkpoint format.
pub const LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4: u32 = 4;
/// Maximum files carried by one frozen V4 bootstrap or adoption command.
pub const MAX_LEGACY_CATALOG_PROJECTION_FILES_V4: usize = 4_096;
/// Maximum stable source records carried by one V4 table projection.
pub const MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4: usize = 4_096;
/// Maximum recursive value depth admitted by the V4 canonical wire.
pub const MAX_LEGACY_CATALOG_VALUE_DEPTH_V4: usize = 32;
/// Maximum recursive value nodes admitted across one V4 command or digest table.
pub const MAX_LEGACY_CATALOG_VALUE_NODES_V4: usize = 65_536;
/// Maximum canonical V4 fingerprint or projection-digest input bytes.
pub const MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4: usize = 64 * 1024 * 1024;
/// Reserved file metadata key binding verifier-observed Arrow Schema to a V2 import command.
pub const LEGACY_VERIFIED_SCHEMA_HASH_METADATA_KEY: &str =
    "zippy.legacy_import.verified_schema_hash";
/// Legacy planner audit metadata recording the file's source registry position.
///
/// This value is snapshot-local planner evidence, not immutable physical file identity.
pub const LEGACY_REGISTRY_INDEX_METADATA_KEY: &str = "zippy.migration.legacy_registry_index";
/// Legacy planner audit metadata recording the file's effective scan position.
///
/// This value is snapshot-local planner evidence, not immutable physical file identity.
pub const LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY: &str =
    "zippy.migration.legacy_effective_scan_order";
/// Maximum UTF-8 byte length of an audited catalog actor principal.
pub const MAX_CATALOG_ACTOR_PRINCIPAL_BYTES: usize = 4 * 1024;

/// The canonical encoding version used to validate an idempotent catalog command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CatalogCommandFormatVersion(u32);

impl CatalogCommandFormatVersion {
    /// Creates a version value for decode and compatibility checks.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the current command format version.
    #[must_use]
    pub const fn current() -> Self {
        Self(CURRENT_CATALOG_COMMAND_FORMAT_VERSION)
    }

    /// Returns the permanently assigned frozen V1 version.
    #[must_use]
    pub const fn v1() -> Self {
        Self(CATALOG_COMMAND_FORMAT_VERSION_V1)
    }

    /// Returns the underlying fixed-width version value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// A SHA-256 digest of the canonical catalog command fingerprint input.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommandFingerprint([u8; 32]);

impl CommandFingerprint {
    /// Creates a fingerprint from its raw SHA-256 bytes.
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

impl fmt::Debug for CommandFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CommandFingerprint")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for CommandFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for CommandFingerprint {
    type Err = ParseCommandFingerprintError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(ParseCommandFingerprintError::InvalidLength {
                actual: value.len(),
            });
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(pair)
                .map_err(|_| ParseCommandFingerprintError::InvalidHex { index: index * 2 })?;
            bytes[index] = u8::from_str_radix(text, 16)
                .map_err(|_| ParseCommandFingerprintError::InvalidHex { index: index * 2 })?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for CommandFingerprint {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CommandFingerprint {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// An invalid hexadecimal command fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseCommandFingerprintError {
    /// A SHA-256 fingerprint must contain exactly 64 hexadecimal digits.
    #[error("invalid command fingerprint length actual=[{actual}]")]
    InvalidLength {
        /// The number of input bytes.
        actual: usize,
    },
    /// A non-hexadecimal digit occurs at or after the reported position.
    #[error("invalid command fingerprint hex index=[{index}]")]
    InvalidHex {
        /// The zero-based digit position.
        index: usize,
    },
}

/// The class of principal responsible for a catalog mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogActorKind {
    /// An authenticated user or client session.
    User,
    /// The primary Zippy server process.
    Server,
    /// The local maintenance worker.
    Maintenance,
    /// A versioned import or migration tool.
    Migration,
}

/// The audited principal responsible for a catalog mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogActor {
    /// The class of actor.
    pub kind: CatalogActorKind,
    /// A stable principal, service, or migration name, bounded by
    /// [`MAX_CATALOG_ACTOR_PRINCIPAL_BYTES`] when fingerprinted or admitted.
    pub principal: String,
}

/// Fields hashed to establish catalog-command idempotency.
///
/// Version 1 canonical bytes are compact UTF-8 JSON produced from this exact
/// structure. All maps reachable from commands are `BTreeMap`, so key order is
/// deterministic. `request_id` is intentionally excluded: it selects the
/// idempotency record, while the fingerprint proves the operation bound to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCommandFingerprintInputV1 {
    /// The canonical encoding version.
    pub format_version: CatalogCommandFormatVersion,
    /// The guarded catalog revision, when present.
    pub expected_revision: Option<u64>,
    /// The audited command actor.
    pub actor: CatalogActor,
    /// The typed command body.
    pub command: CatalogCommandV1,
}

impl CatalogCommandFingerprintInputV1 {
    /// Encodes the canonical versioned bytes used by the fingerprint.
    pub fn canonical_bytes(&self) -> std::result::Result<Vec<u8>, CatalogFingerprintError> {
        validate_fingerprint_fields(self.format_version, &self.actor)?;
        serde_json::to_vec(&wire_v1::FingerprintInputV1::from(self))
            .map_err(CatalogFingerprintError::Encode)
    }

    /// Computes the canonical SHA-256 fingerprint.
    pub fn fingerprint(&self) -> std::result::Result<CommandFingerprint, CatalogFingerprintError> {
        fingerprint_fields(
            self.format_version,
            self.expected_revision,
            &self.actor,
            &self.command,
        )
    }
}

/// A failure while encoding or validating a canonical command fingerprint.
#[derive(Debug, Error)]
pub enum CatalogFingerprintError {
    /// The command uses an unsupported canonical format version.
    #[error("unsupported catalog command format version actual=[{actual}]")]
    UnsupportedFormatVersion {
        /// The unsupported version value.
        actual: u32,
    },
    /// The audited actor principal exceeds the protocol resource bound.
    #[error("catalog actor principal exceeds byte limit actual=[{actual}] maximum=[{maximum}]")]
    ActorPrincipalTooLarge {
        /// The actual UTF-8 byte length.
        actual: usize,
        /// The maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// A frozen V4 command or digest exceeds a permanent resource boundary.
    #[error(
        "legacy Catalog V4 resource exceeds limit resource=[{resource}] actual=[{actual}] maximum=[{maximum}]"
    )]
    LegacyProjectionResourceLimit {
        /// Stable name of the bounded resource.
        resource: &'static str,
        /// Observed count.
        actual: usize,
        /// Permanent maximum.
        maximum: usize,
    },
    /// A frozen V4 identity set is empty, duplicate, or not in its required order.
    #[error("invalid legacy Catalog V4 identity set resource=[{resource}]")]
    InvalidLegacyProjectionIdentity {
        /// Stable name of the invalid identity domain.
        resource: &'static str,
    },
    /// Canonical JSON encoding failed.
    #[error("failed to encode canonical catalog command error=[{0}]")]
    Encode(serde_json::Error),
    /// The supplied digest does not match the canonical command fields.
    #[error("catalog command fingerprint mismatch")]
    Mismatch,
}

/// A failure while encoding or validating frozen catalog protocol bytes.
#[derive(Debug, Error)]
pub enum CatalogCodecError {
    /// JSON encoding or decoding failed.
    #[error("invalid catalog codec payload error=[{0}]")]
    Json(#[from] serde_json::Error),
    /// The encoded protocol version has no decoder in this server.
    #[error("unsupported catalog codec version actual=[{actual}]")]
    UnsupportedVersion {
        /// The unsupported version value.
        actual: u32,
    },
    /// A decoded V1 command failed canonical fingerprint validation.
    #[error(transparent)]
    Fingerprint(#[from] CatalogFingerprintError),
    /// Frozen V1 fields failed explicit conversion into validated domain types.
    #[error("invalid frozen V1 catalog payload error=[{0}]")]
    InvalidV1(String),
    /// Frozen V2 legacy-import fields failed explicit conversion into validated domain types.
    #[error("invalid frozen V2 legacy import payload error=[{0}]")]
    InvalidLegacyImportV2(String),
    /// Frozen V4 legacy-projection fields failed explicit conversion into validated domain types.
    #[error("invalid frozen V4 legacy projection payload error=[{0}]")]
    InvalidLegacyProjectionV4(String),
}

/// An idempotent and optionally revision-guarded catalog mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCommandEnvelopeV1 {
    /// The canonical command encoding version.
    pub format_version: CatalogCommandFormatVersion,
    /// The idempotency key for this logical mutation.
    pub request_id: RequestId,
    /// The catalog revision that must still be current, when specified.
    pub expected_revision: Option<u64>,
    /// The audited actor responsible for the mutation.
    pub actor: CatalogActor,
    /// The typed mutation to execute atomically.
    pub command: CatalogCommandV1,
    /// SHA-256 of the canonical fingerprint input.
    pub canonical_fingerprint: CommandFingerprint,
}

/// One externally verified legacy file and its Gateway-compatible scan position.
///
/// This DTO belongs to a trusted, startup-only internal migration boundary. It must be constructed
/// from physical verifier output and must not be accepted directly from an RPC or untrusted input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogImportFileV2 {
    /// Complete immutable physical metadata proven before Catalog admission.
    pub file: PendingDataFile,
    /// Zero-based position after applying the legacy Gateway's stable persisted-file ordering key.
    /// The original registry index is audit metadata and must not be used here.
    pub discovery_order: u64,
}

/// Migration-only command that atomically creates one table and imports its legacy files.
///
/// This is a trusted, startup-only internal DTO, not a public RPC request. Every file entry must
/// already have passed the local physical verifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogImportCommandV2 {
    /// The caller-assigned immutable table identifier.
    pub table_id: TableId,
    /// The existing namespace that will own the table.
    pub namespace_id: NamespaceId,
    /// The name, unique within the namespace.
    pub name: CatalogName,
    /// The typed table storage and mutation definition.
    pub definition: TableDefinition,
    /// The first Arrow schema encoded as IPC.
    pub schema_ipc: SchemaIpcPayload,
    /// Catalog canonical logical Schema hash, independent of IPC metadata map order.
    pub canonical_schema_hash: String,
    /// Stable table configuration values.
    pub config: BTreeMap<String, String>,
    /// Exact writer epoch captured atomically from the legacy registry.
    pub writer_epoch: u64,
    /// Externally verified files in strict Gateway-compatible scan order.
    pub files: Vec<LegacyCatalogImportFileV2>,
    /// Legacy persisted-file view revision represented by this import.
    pub legacy_files_revision: u64,
}

/// Idempotent migration-only V2 envelope.
///
/// This envelope is restricted to trusted startup-time migration orchestration and must not be
/// exposed as an RPC contract. `actor` is audit metadata only; neither its kind nor principal
/// proves authentication, authorization, or possession of a migration capability.
///
/// `schema_ipc` is intentionally excluded from the fingerprint. Its decoded logical Schema must
/// match `canonical_schema_hash` before the first transaction; exact replays may therefore use a
/// byte-different IPC stream that represents the same canonical Schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogImportEnvelopeV2 {
    /// The fixed migration import format version.
    pub format_version: CatalogCommandFormatVersion,
    /// The idempotency key for this logical table import.
    pub request_id: RequestId,
    /// The Catalog revision that must still be current on first execution.
    pub expected_revision: Option<u64>,
    /// The audited migration actor; this field does not grant migration authority.
    pub actor: CatalogActor,
    /// Digest of the atomic legacy registry snapshot used to build the command.
    pub source_snapshot_hash: String,
    /// Legacy Catalog revision captured with the source snapshot.
    pub source_catalog_revision: u64,
    /// The typed table and verified-file import body.
    pub command: LegacyCatalogImportCommandV2,
    /// SHA-256 of the canonical logical import fields.
    pub canonical_fingerprint: CommandFingerprint,
}

impl LegacyCatalogImportEnvelopeV2 {
    /// Creates a V2 envelope and computes its canonical logical fingerprint.
    pub fn new(
        request_id: RequestId,
        expected_revision: Option<u64>,
        actor: CatalogActor,
        source_snapshot_hash: String,
        source_catalog_revision: u64,
        command: LegacyCatalogImportCommandV2,
    ) -> std::result::Result<Self, CatalogFingerprintError> {
        let mut envelope = Self {
            format_version: CatalogCommandFormatVersion::new(
                LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2,
            ),
            request_id,
            expected_revision,
            actor,
            source_snapshot_hash,
            source_catalog_revision,
            command,
            canonical_fingerprint: CommandFingerprint::from_bytes([0; 32]),
        };
        envelope.canonical_fingerprint = envelope.fingerprint()?;
        Ok(envelope)
    }

    /// Recomputes and validates the canonical logical fingerprint.
    pub fn verify_fingerprint(&self) -> std::result::Result<(), CatalogFingerprintError> {
        if self.fingerprint()? == self.canonical_fingerprint {
            Ok(())
        } else {
            Err(CatalogFingerprintError::Mismatch)
        }
    }

    fn fingerprint(&self) -> std::result::Result<CommandFingerprint, CatalogFingerprintError> {
        if self.format_version.get() != LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2 {
            return Err(CatalogFingerprintError::UnsupportedFormatVersion {
                actual: self.format_version.get(),
            });
        }
        let actual = self.actor.principal.len();
        if actual > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
            return Err(CatalogFingerprintError::ActorPrincipalTooLarge {
                actual,
                maximum: MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
            });
        }
        let mut writer = FingerprintWriter(Sha256::new());
        serde_json::to_writer(&mut writer, &wire_v2::FingerprintInputRefV2::from(self))
            .map_err(CatalogFingerprintError::Encode)?;
        let digest = writer.0.finalize();
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&digest);
        Ok(CommandFingerprint::from_bytes(fingerprint))
    }
}

/// Durable result of one atomic legacy table import.
///
/// This response belongs to the trusted startup-only internal migration boundary and is not an RPC
/// response contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogImportResponseV2 {
    /// The idempotency key from the command envelope.
    pub request_id: RequestId,
    /// The Catalog revision after the import committed.
    pub catalog_revision: u64,
    /// The imported table descriptor.
    pub table: TableDescriptor,
    /// The final nonempty-table `import` snapshot.
    pub snapshot: TableSnapshot,
}

/// One online legacy file-view reconcile command.
///
/// Unlike [`LegacyCatalogImportCommandV2`], this command does not create a table. It replaces the
/// complete externally visible legacy file set of an existing migrated table. Every file must be
/// produced by the trusted physical verifier before Catalog admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogReconcileCommandV3 {
    /// The migrated table whose legacy file view is being reconciled.
    pub table_id: TableId,
    /// The table schema version proven by the physical verifier.
    pub expected_schema_version: u64,
    /// The Catalog canonical hash of the verifier-observed Arrow schema.
    pub expected_schema_hash: String,
    /// Writer epoch that must still be current for the migrated table.
    pub expected_writer_epoch: u64,
    /// Authoritative writer epoch captured with the target legacy view.
    pub writer_epoch: u64,
    /// Per-table legacy file revision that must currently be mapped to the active snapshot.
    pub expected_legacy_files_revision: u64,
    /// New authoritative legacy file revision represented by `files`.
    pub legacy_files_revision: u64,
    /// Complete verified target file set in strict legacy Gateway scan order.
    pub files: Vec<LegacyCatalogImportFileV2>,
}

/// Idempotent trusted V3 envelope for an online full legacy file-view reconcile.
///
/// This DTO is an internal server boundary, not a public RPC contract. `actor` is audit metadata;
/// it does not grant authority. A revision jump is allowed so a restarted worker can recover from
/// lost notifications, while the resulting mapping records degraded continuity explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogReconcileEnvelopeV3 {
    /// The fixed reconcile format version.
    pub format_version: CatalogCommandFormatVersion,
    /// The idempotency key for this logical reconcile.
    pub request_id: RequestId,
    /// The audited server actor; this field does not grant reconcile authority.
    pub actor: CatalogActor,
    /// Digest of the atomic legacy registry snapshot used to construct the target view.
    pub source_snapshot_hash: String,
    /// Legacy Catalog revision captured with the source snapshot.
    pub source_catalog_revision: u64,
    /// Complete typed reconcile body.
    pub command: LegacyCatalogReconcileCommandV3,
    /// SHA-256 of the canonical logical reconcile fields.
    pub canonical_fingerprint: CommandFingerprint,
}

impl LegacyCatalogReconcileEnvelopeV3 {
    /// Creates a V3 envelope and computes its canonical logical fingerprint.
    pub fn new(
        request_id: RequestId,
        actor: CatalogActor,
        source_snapshot_hash: String,
        source_catalog_revision: u64,
        command: LegacyCatalogReconcileCommandV3,
    ) -> std::result::Result<Self, CatalogFingerprintError> {
        let mut envelope = Self {
            format_version: CatalogCommandFormatVersion::new(
                LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3,
            ),
            request_id,
            actor,
            source_snapshot_hash,
            source_catalog_revision,
            command,
            canonical_fingerprint: CommandFingerprint::from_bytes([0; 32]),
        };
        envelope.canonical_fingerprint = envelope.fingerprint()?;
        Ok(envelope)
    }

    /// Recomputes and validates the canonical logical fingerprint.
    pub fn verify_fingerprint(&self) -> std::result::Result<(), CatalogFingerprintError> {
        if self.fingerprint()? == self.canonical_fingerprint {
            Ok(())
        } else {
            Err(CatalogFingerprintError::Mismatch)
        }
    }

    fn fingerprint(&self) -> std::result::Result<CommandFingerprint, CatalogFingerprintError> {
        if self.format_version.get() != LEGACY_CATALOG_RECONCILE_FORMAT_VERSION_V3 {
            return Err(CatalogFingerprintError::UnsupportedFormatVersion {
                actual: self.format_version.get(),
            });
        }
        let actual = self.actor.principal.len();
        if actual > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
            return Err(CatalogFingerprintError::ActorPrincipalTooLarge {
                actual,
                maximum: MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
            });
        }
        let mut writer = FingerprintWriter(Sha256::new());
        serde_json::to_writer(&mut writer, &wire_v3::FingerprintInputRefV3::from(self))
            .map_err(CatalogFingerprintError::Encode)?;
        let digest = writer.0.finalize();
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&digest);
        Ok(CommandFingerprint::from_bytes(fingerprint))
    }
}

/// Whether a legacy revision-to-snapshot mapping preserved notification continuity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyFileViewContinuity {
    /// The startup import established the first explicit mapping.
    Base,
    /// The reconcile advanced by exactly one legacy revision.
    Contiguous,
    /// The reconcile recovered to a later authoritative revision after a notification gap.
    Gap,
}

/// Durable explicit mapping from one per-table legacy revision to one native snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyFileViewSnapshotMapping {
    /// The migrated table.
    pub table_id: TableId,
    /// The authoritative legacy persisted-files revision.
    pub legacy_files_revision: u64,
    /// The exact immutable native snapshot representing that legacy revision.
    pub native_snapshot_version: u64,
    /// The request that created the mapping, absent for a strictly validated V1 base bootstrap.
    pub request_id: Option<RequestId>,
    /// Catalog revision committed with the mapping.
    pub catalog_revision: u64,
    /// Source legacy Catalog revision captured by the request.
    pub source_catalog_revision: u64,
    /// Source legacy snapshot digest captured by the request.
    pub source_snapshot_hash: String,
    /// Notification continuity observed when the mapping was created.
    pub continuity: LegacyFileViewContinuity,
    /// Commit time in Unix nanoseconds.
    pub committed_at_ns: u64,
}

/// Durable minimal result of one online full legacy file-view reconcile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogReconcileResponseV3 {
    /// The idempotency key from the command envelope.
    pub request_id: RequestId,
    /// Catalog revision after the reconcile committed.
    pub catalog_revision: u64,
    /// The reconciled table.
    pub table_id: TableId,
    /// Legacy revision replaced by this reconcile.
    pub previous_legacy_files_revision: u64,
    /// New authoritative legacy revision.
    pub legacy_files_revision: u64,
    /// New native snapshot representing the target file view.
    pub native_snapshot_version: u64,
    /// Notification continuity recorded by the mapping.
    pub continuity: LegacyFileViewContinuity,
}

/// A deterministic recursive value used by the frozen legacy source projection.
///
/// Object keys are stored in a [`BTreeMap`], and floating-point values use
/// [`crate::table::FiniteMetadataFloat`], so canonical fingerprints cannot depend on hash-map
/// iteration or non-finite JSON values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum LegacyCatalogValueV4 {
    /// JSON null.
    Null,
    /// A Boolean value.
    Boolean(bool),
    /// A signed integer value.
    Int64(i64),
    /// An unsigned integer value.
    #[serde(rename = "uint64")]
    UInt64(u64),
    /// A finite IEEE-754 binary64 value.
    Float64(crate::table::FiniteMetadataFloat),
    /// A UTF-8 string.
    String(String),
    /// An ordered list of recursive values.
    Array(Vec<Self>),
    /// A key-sorted object of recursive values.
    Object(BTreeMap<String, Self>),
}

/// Complete typed projection of one legacy source record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogSourceV4 {
    /// Stable source identity within the legacy registry.
    pub source_name: String,
    /// Legacy source implementation kind.
    pub source_type: String,
    /// Legacy stream receiving source output.
    pub output_stream: String,
    /// Complete deterministic source configuration.
    pub config: LegacyCatalogValueV4,
    /// Source writer fencing epoch.
    pub writer_epoch: u64,
}

/// Authority phase of the singleton legacy projection checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogProjectionPhaseV4 {
    /// Only the migration foundation is available.
    FoundationOnly,
    /// File views are shadowed, but complete metadata is not yet proven.
    FileShadow,
    /// Complete table and source metadata is shadowed.
    FullShadow,
    /// Final reconcile and checkpoint gates permit read-cutover evaluation.
    ReadCutoverReady,
    /// SQLite Catalog is the native metadata authority.
    NativeAuthoritative,
}

/// How one source Catalog checkpoint advance proves notification continuity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogCheckpointModeV4 {
    /// The source revision advances by exactly one notification.
    Contiguous,
    /// A complete source snapshot supersedes any missed notifications.
    FullReconcile,
}

/// Durable lifecycle state of one legacy table projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogProjectionTableStateV4 {
    /// The projected table is active in both Catalogs.
    Active,
    /// The projected table was logically dropped; its history remains readable.
    Dropped,
}

/// Whether a per-table row has a complete stable source projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogProjectionCompletenessV4 {
    /// V2/V3 synchronized table and file cursors, but no complete stable source set is available.
    Seeded,
    /// V4 bootstrap, adoption, or sync proved the complete stable source set and projection hash.
    Complete,
}

/// Online creation of one complete legacy table projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogBootstrapTableCommandV4 {
    /// Immutable table identity.
    pub table_id: TableId,
    /// Existing namespace owning the table.
    pub namespace_id: NamespaceId,
    /// Byte-exact table name.
    pub name: CatalogName,
    /// Typed table definition.
    pub definition: TableDefinition,
    /// Initial Arrow schema IPC. It is logically validated but excluded from the fingerprint.
    pub schema_ipc: SchemaIpcPayload,
    /// Canonical logical hash of `schema_ipc`.
    pub canonical_schema_hash: String,
    /// Complete stable table configuration.
    pub config: BTreeMap<String, String>,
    /// Captured table writer epoch.
    pub writer_epoch: u64,
    /// Complete typed source set in strict source-name order.
    pub sources: Vec<LegacyCatalogSourceV4>,
    /// Complete physically verified file view in legacy scan order.
    pub files: Vec<LegacyCatalogImportFileV2>,
    /// Legacy file-view revision represented by `files`.
    pub legacy_files_revision: u64,
}

/// Strictly adopts one already migrated V1/V2/V3 table into complete V4 projection state.
///
/// Adoption never creates or mutates a table, snapshot, or file. An existing mapping is immutable
/// and must match exactly. The sole exception is an empty V1-migrated base table: after strict
/// migration-config and snapshot verification, adoption atomically creates its missing V4 base
/// mapping while attaching complete projection state and the stable source set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogAdoptExistingTableCommandV4 {
    /// Existing immutable table identity.
    pub table_id: TableId,
    /// Existing namespace identity.
    pub namespace_id: NamespaceId,
    /// Existing byte-exact table name.
    pub name: CatalogName,
    /// Existing typed table definition.
    pub definition: TableDefinition,
    /// Current schema version.
    pub expected_schema_version: u64,
    /// Current canonical Schema hash.
    pub expected_schema_hash: String,
    /// Existing complete stable table configuration.
    pub config: BTreeMap<String, String>,
    /// Current native snapshot cursor.
    pub expected_snapshot_version: u64,
    /// Current legacy file-view revision.
    pub expected_legacy_files_revision: u64,
    /// Current writer epoch.
    pub expected_writer_epoch: u64,
    /// Complete typed stable source set in strict source-name order.
    pub sources: Vec<LegacyCatalogSourceV4>,
    /// Complete physically verified current file view in legacy scan order.
    pub files: Vec<LegacyCatalogImportFileV2>,
}

/// Metadata-only synchronization of an existing projected table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogSyncTableMetadataCommandV4 {
    /// Projected table identity.
    pub table_id: TableId,
    /// Projection revision that must still be current.
    pub expected_projection_revision: u64,
    /// Schema version that must still be current.
    pub expected_schema_version: u64,
    /// Canonical Schema hash that must still be current.
    pub expected_schema_hash: String,
    /// Native snapshot cursor that must remain unchanged.
    pub expected_snapshot_version: u64,
    /// Legacy file-view revision that must remain unchanged.
    pub expected_legacy_files_revision: u64,
    /// Writer epoch that must still be current.
    pub expected_writer_epoch: u64,
    /// Table-local stable source metadata hash that must still be current.
    pub expected_source_metadata_hash: String,
    /// New captured writer epoch.
    pub writer_epoch: u64,
    /// Complete replacement typed source set in strict source-name order.
    pub sources: Vec<LegacyCatalogSourceV4>,
}

/// Logical removal of an existing projected table while retaining its history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogDropTableCommandV4 {
    /// Projected table identity.
    pub table_id: TableId,
    /// Projection revision that must still be current.
    pub expected_projection_revision: u64,
    /// Schema version that must still be current.
    pub expected_schema_version: u64,
    /// Canonical Schema hash that must still be current.
    pub expected_schema_hash: String,
    /// Native snapshot cursor that must still be current.
    pub expected_snapshot_version: u64,
    /// Legacy file-view revision that must still be current.
    pub expected_legacy_files_revision: u64,
    /// Writer epoch that must still be current.
    pub expected_writer_epoch: u64,
    /// Table-local stable source metadata hash expected for complete projections.
    ///
    /// Seeded projections have not captured complete source metadata and therefore require
    /// `None` when they are removed after disappearing from the full legacy snapshot. The
    /// combination `None` plus `expected_projection_revision = 0` is reserved for atomically
    /// absorbing and dropping a strictly validated empty, unprojected V1 migration base.
    pub expected_source_metadata_hash: Option<String>,
}

/// Durable acknowledgement of one source Catalog revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogAdvanceCheckpointCommandV4 {
    /// Checkpoint generation that must still be current.
    pub expected_checkpoint_generation: u64,
    /// Previously acknowledged source Catalog revision.
    pub expected_source_catalog_revision: u64,
    /// Previously acknowledged complete stable projection digest.
    pub expected_source_projection_hash: String,
    /// Authority phase that must still be current.
    pub expected_phase: LegacyCatalogProjectionPhaseV4,
    /// Target shadow epoch. It may advance only during a full reconcile.
    pub shadow_epoch: u64,
    /// Target authority phase.
    pub phase: LegacyCatalogProjectionPhaseV4,
    /// Continuity proof for this checkpoint advance.
    pub mode: LegacyCatalogCheckpointModeV4,
    /// Worker-computed digest of the complete logical source projection.
    pub target_projection_digest: String,
    /// Publisher continuity-loss epoch observed with the target snapshot.
    pub observed_continuity_loss_epoch: u64,
}

/// The five legacy projection operations assigned to frozen internal format V4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "command", rename_all = "snake_case")]
pub enum LegacyCatalogProjectionCommandV4 {
    /// Create an empty or nonempty table and its explicit base mapping.
    BootstrapTable(LegacyCatalogBootstrapTableCommandV4),
    /// Attach complete projection state to a strictly verified pre-V4 migrated table.
    AdoptExistingTable(LegacyCatalogAdoptExistingTableCommandV4),
    /// Replace writer and complete typed source metadata without a new snapshot.
    SyncTableMetadata(LegacyCatalogSyncTableMetadataCommandV4),
    /// Logically drop a table while retaining snapshots and mappings.
    DropTable(LegacyCatalogDropTableCommandV4),
    /// Persist source progress after recomputing the complete projection digest.
    AdvanceCheckpoint(LegacyCatalogAdvanceCheckpointCommandV4),
}

/// Idempotent trusted V4 envelope for complete legacy metadata projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogProjectionEnvelopeV4 {
    /// Fixed V4 format version.
    pub format_version: CatalogCommandFormatVersion,
    /// Globally unique idempotency key.
    pub request_id: RequestId,
    /// Audited server actor; this field does not grant authority.
    pub actor: CatalogActor,
    /// Shadow epoch that fences this worker instance.
    pub expected_shadow_epoch: u64,
    /// Digest of the complete stable table/source projection used to build the command.
    ///
    /// This is a distinct domain from the startup migration snapshot hash: it must bind the full
    /// stable source set and must exclude boot-scoped process, status, and metrics state.
    pub source_projection_hash: String,
    /// Legacy Catalog revision captured with that snapshot.
    pub source_catalog_revision: u64,
    /// Typed projection operation.
    pub command: LegacyCatalogProjectionCommandV4,
    /// SHA-256 of the canonical logical V4 fields.
    pub canonical_fingerprint: CommandFingerprint,
}

impl LegacyCatalogProjectionEnvelopeV4 {
    /// Creates a V4 envelope and computes its canonical logical fingerprint.
    pub fn new(
        request_id: RequestId,
        actor: CatalogActor,
        expected_shadow_epoch: u64,
        source_projection_hash: String,
        source_catalog_revision: u64,
        command: LegacyCatalogProjectionCommandV4,
    ) -> std::result::Result<Self, CatalogFingerprintError> {
        let mut envelope = Self {
            format_version: CatalogCommandFormatVersion::new(
                LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
            ),
            request_id,
            actor,
            expected_shadow_epoch,
            source_projection_hash,
            source_catalog_revision,
            command,
            canonical_fingerprint: CommandFingerprint::from_bytes([0; 32]),
        };
        envelope.canonical_fingerprint = envelope.fingerprint()?;
        Ok(envelope)
    }

    /// Recomputes and validates the canonical logical fingerprint.
    pub fn verify_fingerprint(&self) -> std::result::Result<(), CatalogFingerprintError> {
        if self.fingerprint()? == self.canonical_fingerprint {
            Ok(())
        } else {
            Err(CatalogFingerprintError::Mismatch)
        }
    }

    fn fingerprint(&self) -> std::result::Result<CommandFingerprint, CatalogFingerprintError> {
        if self.format_version.get() != LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4 {
            return Err(CatalogFingerprintError::UnsupportedFormatVersion {
                actual: self.format_version.get(),
            });
        }
        let actual = self.actor.principal.len();
        if actual > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
            return Err(CatalogFingerprintError::ActorPrincipalTooLarge {
                actual,
                maximum: MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
            });
        }
        validate_v4_command_shape(&self.command)?;
        let mut writer = BoundedFingerprintWriter::new(MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4);
        serde_json::to_writer(&mut writer, &wire_v4::FingerprintInputRefV4::from(self))
            .map_err(CatalogFingerprintError::Encode)?;
        let digest = writer.finish();
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(&digest);
        Ok(CommandFingerprint::from_bytes(fingerprint))
    }
}

/// Minimal durable table result returned by a V4 projection mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogProjectionTableResultV4 {
    /// Mutated table identity.
    pub table_id: TableId,
    /// New per-table projection revision.
    pub projection_revision: u64,
    /// Current immutable native snapshot cursor.
    pub native_snapshot_version: u64,
    /// Current mapped legacy file-view revision.
    pub legacy_files_revision: u64,
    /// Current writer epoch.
    pub writer_epoch: u64,
    /// Whether complete stable source metadata is attached.
    pub completeness: LegacyCatalogProjectionCompletenessV4,
    /// Current projection lifecycle state.
    pub state: LegacyCatalogProjectionTableStateV4,
}

/// Durable singleton checkpoint returned after a V4 checkpoint advance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogProjectionCheckpointV4 {
    /// Monotonic checkpoint CAS generation.
    pub checkpoint_generation: u64,
    /// Fencing epoch for projection workers.
    pub shadow_epoch: u64,
    /// Current authority phase.
    pub phase: LegacyCatalogProjectionPhaseV4,
    /// Last durably acknowledged source Catalog revision.
    pub source_catalog_revision: u64,
    /// Digest of the acknowledged complete stable legacy projection.
    pub source_projection_hash: String,
    /// Digest recomputed from complete typed projected table and source state.
    pub projection_digest: String,
    /// Continuity proof used for the last advance.
    pub mode: LegacyCatalogCheckpointModeV4,
    /// Highest publisher continuity-loss epoch resolved by a full reconcile.
    pub resolved_continuity_loss_epoch: u64,
    /// Last checkpoint commit time in Unix nanoseconds.
    pub updated_at_ns: u64,
}

/// Complete typed state of one projected legacy table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogTableProjectionV4 {
    /// Projected table identity.
    pub table_id: TableId,
    /// Per-table projection CAS revision.
    pub projection_revision: u64,
    /// Current schema version.
    pub schema_version: u64,
    /// Current canonical Schema hash.
    pub schema_hash: String,
    /// Current native snapshot cursor.
    pub native_snapshot_version: u64,
    /// Current legacy file-view revision.
    pub legacy_files_revision: u64,
    /// Current table writer epoch.
    pub writer_epoch: u64,
    /// Whether complete stable source metadata is attached.
    pub completeness: LegacyCatalogProjectionCompletenessV4,
    /// Projection lifecycle state.
    pub state: LegacyCatalogProjectionTableStateV4,
    /// Table-local digest of complete stable sources, absent only for a V2/V3-seeded row.
    pub source_metadata_hash: Option<String>,
    /// Complete typed source set in strict source-name order.
    pub sources: Vec<LegacyCatalogSourceV4>,
    /// Last table projection commit time in Unix nanoseconds.
    pub updated_at_ns: u64,
}

/// Canonical logical table input to the cross-Catalog V4 projection digest.
///
/// Native-only snapshot and projection revisions are intentionally absent. The digest binds the
/// complete logical descriptor, stable sources, and exact legacy file-view revision available to
/// both the legacy source and SQLite Catalog. Physical file identity remains proven by the V3
/// full-view mapping bound to that revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogProjectionDigestTableV4 {
    /// Immutable table identity.
    pub table_id: TableId,
    /// Owning namespace identity.
    pub namespace_id: NamespaceId,
    /// Byte-exact table name.
    pub name: CatalogName,
    /// Typed table definition.
    pub definition: TableDefinition,
    /// Current logical Schema version.
    pub schema_version: u64,
    /// Current canonical Schema hash.
    pub schema_hash: String,
    /// Complete stable table configuration.
    pub config: BTreeMap<String, String>,
    /// Current legacy file-view revision.
    pub legacy_files_revision: u64,
    /// Current table writer epoch.
    pub writer_epoch: u64,
    /// Logical projection lifecycle state.
    pub state: LegacyCatalogProjectionTableStateV4,
    /// Complete stable source set.
    pub sources: Vec<LegacyCatalogSourceV4>,
}

/// Computes the shared canonical digest of a complete V4 logical projection.
///
/// Tables and sources are canonicalized by identity. Boot-scoped process, status, metrics, and
/// active-writer liveness are absent by type and cannot influence authority continuity.
pub fn compute_legacy_catalog_projection_digest_v4(
    tables: &[LegacyCatalogProjectionDigestTableV4],
) -> std::result::Result<String, CatalogFingerprintError> {
    let mut canonical = tables.to_vec();
    canonical.sort_by_key(|table| table.table_id);
    if canonical
        .windows(2)
        .any(|pair| pair[0].table_id == pair[1].table_id)
    {
        return Err(CatalogFingerprintError::InvalidLegacyProjectionIdentity {
            resource: "table_id",
        });
    }
    for table in &mut canonical {
        if table.state != LegacyCatalogProjectionTableStateV4::Active {
            return Err(CatalogFingerprintError::InvalidLegacyProjectionIdentity {
                resource: "active_table_state",
            });
        }
        table
            .sources
            .sort_by(|left, right| left.source_name.cmp(&right.source_name));
        validate_v4_sources(&table.sources)?;
    }
    let mut writer = BoundedFingerprintWriter::with_domain(
        MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4,
        b"zippy.legacy_catalog.projection_digest.v4\0",
    );
    serde_json::to_writer(
        &mut writer,
        &wire_v4::ProjectionDigestInputRefV4::from(canonical.as_slice()),
    )
    .map_err(CatalogFingerprintError::Encode)?;
    Ok(format!("sha256-v1:{:x}", writer.finish()))
}

/// Computes the canonical table-local digest of a complete stable V4 source set.
pub fn compute_legacy_catalog_source_metadata_hash_v4(
    sources: &[LegacyCatalogSourceV4],
) -> std::result::Result<String, CatalogFingerprintError> {
    let mut canonical = sources.to_vec();
    canonical.sort_by(|left, right| left.source_name.cmp(&right.source_name));
    validate_v4_sources(&canonical)?;
    let mut writer = BoundedFingerprintWriter::with_domain(
        MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4,
        b"zippy.legacy_catalog.source_metadata_hash.v4\0",
    );
    serde_json::to_writer(
        &mut writer,
        &wire_v4::SourceMetadataDigestInputRefV4::from(canonical.as_slice()),
    )
    .map_err(CatalogFingerprintError::Encode)?;
    Ok(format!("sha256-v1:{:x}", writer.finish()))
}

/// V4 response outcome stored for exact idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "outcome", rename_all = "snake_case")]
pub enum LegacyCatalogProjectionOutcomeV4 {
    /// Result of `BootstrapTable`.
    BootstrapTable(LegacyCatalogProjectionTableResultV4),
    /// Result of `AdoptExistingTable`.
    AdoptExistingTable(LegacyCatalogProjectionTableResultV4),
    /// Result of `SyncTableMetadata`.
    SyncTableMetadata(LegacyCatalogProjectionTableResultV4),
    /// Result of `DropTable`.
    DropTable(LegacyCatalogProjectionTableResultV4),
    /// Result of `AdvanceCheckpoint`.
    AdvanceCheckpoint(LegacyCatalogProjectionCheckpointV4),
}

/// Durable exact result of one V4 projection command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogProjectionResponseV4 {
    /// Request id from the command envelope.
    pub request_id: RequestId,
    /// Catalog revision after the mutation committed.
    pub catalog_revision: u64,
    /// Typed operation result.
    pub outcome: LegacyCatalogProjectionOutcomeV4,
}

/// Stable codec for durable V4 projection response records.
pub struct LegacyCatalogProjectionCodecV4;

impl LegacyCatalogProjectionCodecV4 {
    /// Encodes one recursive source configuration through the frozen V4 value mirror.
    pub fn encode_source_config(
        value: &LegacyCatalogValueV4,
    ) -> std::result::Result<Vec<u8>, CatalogCodecError> {
        validate_v4_value_shapes(std::iter::once(value))
            .map_err(|error| CatalogCodecError::InvalidLegacyProjectionV4(error.to_string()))?;
        let mut writer = BoundedVecWriter::new(MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4);
        if let Err(error) = wire_v4::write_value_v4(&mut writer, value) {
            return if writer.limit_exceeded() {
                Err(CatalogCodecError::InvalidLegacyProjectionV4(
                    "V4 source configuration exceeds the canonical byte limit".to_owned(),
                ))
            } else {
                Err(CatalogCodecError::Json(error))
            };
        }
        Ok(writer.finish())
    }

    /// Decodes one recursive source configuration through the frozen V4 value mirror.
    pub fn decode_source_config(
        bytes: &[u8],
    ) -> std::result::Result<LegacyCatalogValueV4, CatalogCodecError> {
        if bytes.len() > MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4 {
            return Err(CatalogCodecError::InvalidLegacyProjectionV4(
                "V4 source configuration exceeds the canonical byte limit".to_owned(),
            ));
        }
        let value = wire_v4::decode_value_v4(bytes)
            .map_err(CatalogCodecError::InvalidLegacyProjectionV4)?;
        validate_v4_value_shapes(std::iter::once(&value))
            .map_err(|error| CatalogCodecError::InvalidLegacyProjectionV4(error.to_string()))?;
        let mut writer = BoundedFingerprintWriter::new(MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4);
        if let Err(error) = wire_v4::write_value_v4(&mut writer, &value) {
            return if writer.limit_exceeded() {
                Err(CatalogCodecError::InvalidLegacyProjectionV4(
                    "V4 source configuration exceeds the canonical byte limit".to_owned(),
                ))
            } else {
                Err(CatalogCodecError::Json(error))
            };
        }
        Ok(value)
    }

    /// Encodes one response through the permanently assigned V4 response mirror.
    pub fn encode_response(
        response: &LegacyCatalogProjectionResponseV4,
    ) -> std::result::Result<Vec<u8>, CatalogCodecError> {
        Ok(serde_json::to_vec(&wire_v4::ResponseEnvelopeV4::from(
            response,
        ))?)
    }

    /// Decodes one frozen V4 response and rejects unknown versions or fields.
    pub fn decode_response(
        bytes: &[u8],
    ) -> std::result::Result<LegacyCatalogProjectionResponseV4, CatalogCodecError> {
        #[derive(Deserialize)]
        struct VersionProbe {
            format_version: u32,
        }

        let version = serde_json::from_slice::<VersionProbe>(bytes)?.format_version;
        if version != LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4 {
            return Err(CatalogCodecError::UnsupportedVersion { actual: version });
        }
        serde_json::from_slice::<wire_v4::ResponseEnvelopeV4>(bytes)?
            .into_domain()
            .map_err(CatalogCodecError::InvalidLegacyProjectionV4)
    }
}

/// Fixed-cost lookup result for one startup-only V2 legacy-import request ID.
///
/// The probe validates the retained response's intrinsic import invariants but intentionally does
/// not prove that it matches a newly constructed import command. Startup orchestration must derive
/// request IDs deterministically from its source snapshot and compare committed response fields
/// with the current migration plan before accepting a replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyCatalogImportReplayProbeV2 {
    /// No V2 import exists for this request ID in the observed Catalog revision.
    Missing {
        /// Catalog revision read in the same transaction as the replay lookup.
        observed_catalog_revision: u64,
    },
    /// A structurally and intrinsically valid permanent V2 response exists.
    Committed {
        /// Catalog revision read in the same transaction as the replay lookup.
        observed_catalog_revision: u64,
        /// The validated permanent response.
        response: LegacyCatalogImportResponseV2,
    },
}

/// Stable codec for durable V2 legacy-import response records.
///
/// The codec uses a private frozen wire mirror and never serializes domain structs directly.
pub struct LegacyCatalogImportCodecV2;

impl LegacyCatalogImportCodecV2 {
    /// Encodes one response in the permanently assigned V2 response envelope.
    pub fn encode_response(
        response: &LegacyCatalogImportResponseV2,
    ) -> std::result::Result<Vec<u8>, CatalogCodecError> {
        Ok(serde_json::to_vec(&wire_v2::ResponseEnvelopeV2::from(
            response,
        ))?)
    }

    /// Decodes one frozen V2 response and rejects unknown versions or fields.
    pub fn decode_response(
        bytes: &[u8],
    ) -> std::result::Result<LegacyCatalogImportResponseV2, CatalogCodecError> {
        #[derive(Deserialize)]
        struct VersionProbe {
            format_version: u32,
        }

        let version = serde_json::from_slice::<VersionProbe>(bytes)?.format_version;
        if version != LEGACY_CATALOG_IMPORT_FORMAT_VERSION_V2 {
            return Err(CatalogCodecError::UnsupportedVersion { actual: version });
        }
        serde_json::from_slice::<wire_v2::ResponseEnvelopeV2>(bytes)?
            .into_domain()
            .map_err(CatalogCodecError::InvalidLegacyImportV2)
    }
}

impl CatalogCommandEnvelopeV1 {
    /// Creates a current-version envelope and computes its canonical fingerprint.
    pub fn new(
        request_id: RequestId,
        expected_revision: Option<u64>,
        actor: CatalogActor,
        command: CatalogCommandV1,
    ) -> std::result::Result<Self, CatalogFingerprintError> {
        let input = CatalogCommandFingerprintInputV1 {
            format_version: CatalogCommandFormatVersion::v1(),
            expected_revision,
            actor,
            command,
        };
        let canonical_fingerprint = input.fingerprint()?;
        Ok(Self {
            format_version: input.format_version,
            request_id,
            expected_revision: input.expected_revision,
            actor: input.actor,
            command: input.command,
            canonical_fingerprint,
        })
    }

    /// Recomputes and validates the canonical fingerprint before idempotency lookup.
    pub fn verify_fingerprint(&self) -> std::result::Result<(), CatalogFingerprintError> {
        if fingerprint_fields(
            self.format_version,
            self.expected_revision,
            &self.actor,
            &self.command,
        )? == self.canonical_fingerprint
        {
            Ok(())
        } else {
            Err(CatalogFingerprintError::Mismatch)
        }
    }
}

fn fingerprint_fields(
    format_version: CatalogCommandFormatVersion,
    expected_revision: Option<u64>,
    actor: &CatalogActor,
    command: &CatalogCommandV1,
) -> std::result::Result<CommandFingerprint, CatalogFingerprintError> {
    validate_fingerprint_fields(format_version, actor)?;
    let mut writer = FingerprintWriter(Sha256::new());
    if let Some(wire) = wire_v1::ManifestFingerprintInputRefV1::from_parts(
        format_version,
        expected_revision,
        actor,
        command,
    ) {
        serde_json::to_writer(&mut writer, &wire).map_err(CatalogFingerprintError::Encode)?;
    } else {
        let wire = wire_v1::FingerprintInputV1::from_parts(
            format_version,
            expected_revision,
            actor,
            command,
        );
        serde_json::to_writer(&mut writer, &wire).map_err(CatalogFingerprintError::Encode)?;
    }
    let digest = writer.0.finalize();
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(&digest);
    Ok(CommandFingerprint::from_bytes(bytes))
}

fn validate_fingerprint_fields(
    format_version: CatalogCommandFormatVersion,
    actor: &CatalogActor,
) -> std::result::Result<(), CatalogFingerprintError> {
    if format_version != CatalogCommandFormatVersion::v1() {
        return Err(CatalogFingerprintError::UnsupportedFormatVersion {
            actual: format_version.get(),
        });
    }
    let actual = actor.principal.len();
    if actual > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
        return Err(CatalogFingerprintError::ActorPrincipalTooLarge {
            actual,
            maximum: MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
        });
    }
    Ok(())
}

fn validate_v4_command_shape(
    command: &LegacyCatalogProjectionCommandV4,
) -> std::result::Result<(), CatalogFingerprintError> {
    match command {
        LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
            validate_v4_files(command.files.len())?;
            validate_v4_sources(&command.sources)
        }
        LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
            validate_v4_files(command.files.len())?;
            validate_v4_sources(&command.sources)
        }
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
            validate_v4_sources(&command.sources)
        }
        LegacyCatalogProjectionCommandV4::DropTable(_)
        | LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(_) => Ok(()),
    }
}

fn validate_v4_files(actual: usize) -> std::result::Result<(), CatalogFingerprintError> {
    if actual > MAX_LEGACY_CATALOG_PROJECTION_FILES_V4 {
        return Err(CatalogFingerprintError::LegacyProjectionResourceLimit {
            resource: "files",
            actual,
            maximum: MAX_LEGACY_CATALOG_PROJECTION_FILES_V4,
        });
    }
    Ok(())
}

fn validate_v4_sources(
    sources: &[LegacyCatalogSourceV4],
) -> std::result::Result<(), CatalogFingerprintError> {
    if sources.len() > MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4 {
        return Err(CatalogFingerprintError::LegacyProjectionResourceLimit {
            resource: "sources",
            actual: sources.len(),
            maximum: MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4,
        });
    }
    if sources.iter().any(|source| {
        source.source_name.is_empty()
            || source.source_type.is_empty()
            || source.output_stream.is_empty()
    }) || sources
        .windows(2)
        .any(|pair| pair[0].source_name.as_str() >= pair[1].source_name.as_str())
    {
        return Err(CatalogFingerprintError::InvalidLegacyProjectionIdentity {
            resource: "source_name",
        });
    }

    validate_v4_value_shapes(sources.iter().map(|source| &source.config))
}

fn validate_v4_value_shapes<'a>(
    values: impl IntoIterator<Item = &'a LegacyCatalogValueV4>,
) -> std::result::Result<(), CatalogFingerprintError> {
    let mut nodes = 0_usize;
    let mut stack = values
        .into_iter()
        .map(|value| (value, 1_usize))
        .collect::<Vec<_>>();
    while let Some((value, depth)) = stack.pop() {
        nodes =
            nodes
                .checked_add(1)
                .ok_or(CatalogFingerprintError::LegacyProjectionResourceLimit {
                    resource: "value_nodes",
                    actual: usize::MAX,
                    maximum: MAX_LEGACY_CATALOG_VALUE_NODES_V4,
                })?;
        if nodes > MAX_LEGACY_CATALOG_VALUE_NODES_V4 {
            return Err(CatalogFingerprintError::LegacyProjectionResourceLimit {
                resource: "value_nodes",
                actual: nodes,
                maximum: MAX_LEGACY_CATALOG_VALUE_NODES_V4,
            });
        }
        if depth > MAX_LEGACY_CATALOG_VALUE_DEPTH_V4 {
            return Err(CatalogFingerprintError::LegacyProjectionResourceLimit {
                resource: "value_depth",
                actual: depth,
                maximum: MAX_LEGACY_CATALOG_VALUE_DEPTH_V4,
            });
        }
        let next_depth = depth.saturating_add(1);
        match value {
            LegacyCatalogValueV4::Array(values) => {
                stack.extend(values.iter().map(|value| (value, next_depth)));
            }
            LegacyCatalogValueV4::Object(values) => {
                stack.extend(values.values().map(|value| (value, next_depth)));
            }
            LegacyCatalogValueV4::Null
            | LegacyCatalogValueV4::Boolean(_)
            | LegacyCatalogValueV4::Int64(_)
            | LegacyCatalogValueV4::UInt64(_)
            | LegacyCatalogValueV4::Float64(_)
            | LegacyCatalogValueV4::String(_) => {}
        }
    }
    Ok(())
}

struct BoundedFingerprintWriter {
    digest: Sha256,
    written: usize,
    maximum: usize,
    limit_exceeded: bool,
}

struct BoundedVecWriter {
    bytes: Vec<u8>,
    maximum: usize,
    limit_exceeded: bool,
}

impl BoundedVecWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            limit_exceeded: false,
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn limit_exceeded(&self) -> bool {
        self.limit_exceeded
    }
}

impl Write for BoundedVecWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.bytes.len().checked_add(bytes.len()) else {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "legacy Catalog V4 canonical byte overflow",
            ));
        };
        if next > self.maximum {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "legacy Catalog V4 canonical bytes exceed limit",
            ));
        }
        if next > self.bytes.capacity() {
            let doubled = self.bytes.capacity().max(1_024).saturating_mul(2);
            let target_capacity = next.max(doubled).min(self.maximum);
            self.bytes
                .try_reserve_exact(target_capacity.saturating_sub(self.bytes.len()))
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl BoundedFingerprintWriter {
    fn new(maximum: usize) -> Self {
        Self {
            digest: Sha256::new(),
            written: 0,
            maximum,
            limit_exceeded: false,
        }
    }

    fn with_domain(maximum: usize, domain: &[u8]) -> Self {
        let mut writer = Self::new(maximum);
        writer.digest.update(domain);
        writer
    }

    fn finish(self) -> sha2::digest::Output<Sha256> {
        self.digest.finalize()
    }

    fn limit_exceeded(&self) -> bool {
        self.limit_exceeded
    }
}

impl Write for BoundedFingerprintWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.written.checked_add(bytes.len()) else {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "legacy Catalog V4 canonical byte overflow",
            ));
        };
        if next > self.maximum {
            self.limit_exceeded = true;
            return Err(std::io::Error::other(
                "legacy Catalog V4 canonical bytes exceed limit",
            ));
        }
        self.digest.update(bytes);
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct FingerprintWriter(Sha256);

impl Write for FingerprintWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A catalog command record routed by its permanently assigned wire version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedCatalogCommand {
    /// A frozen V1 catalog command.
    V1(CatalogCommandEnvelopeV1),
}

impl From<CatalogCommandEnvelopeV1> for VersionedCatalogCommand {
    fn from(command: CatalogCommandEnvelopeV1) -> Self {
        Self::V1(command)
    }
}

/// The stable codec for frozen V1 catalog command and response records.
pub struct CatalogCodec;

impl CatalogCodec {
    /// Encodes and validates one V1 command for RPC, WAL, or audit storage.
    pub fn encode_command(
        command: &VersionedCatalogCommand,
    ) -> std::result::Result<Vec<u8>, CatalogCodecError> {
        match command {
            VersionedCatalogCommand::V1(command) => {
                Self::validate_v1(command)?;
                Ok(serde_json::to_vec(&wire_v1::EnvelopeV1::from(command))?)
            }
        }
    }

    /// Decodes historical V1 bytes without consulting a mutable current schema.
    pub fn decode_command(
        bytes: &[u8],
    ) -> std::result::Result<VersionedCatalogCommand, CatalogCodecError> {
        #[derive(Deserialize)]
        struct VersionProbe {
            format_version: CatalogCommandFormatVersion,
        }
        let version = serde_json::from_slice::<VersionProbe>(bytes)?.format_version;
        if version != CatalogCommandFormatVersion::v1() {
            return Err(CatalogCodecError::UnsupportedVersion {
                actual: version.get(),
            });
        }
        let command = serde_json::from_slice::<wire_v1::EnvelopeV1>(bytes)?
            .into_domain()
            .map_err(CatalogCodecError::InvalidV1)?;
        Self::validate_v1(&command)?;
        Ok(VersionedCatalogCommand::V1(command))
    }

    /// Encodes one V1 response in an explicit versioned outer envelope.
    pub fn encode_response(
        response: &VersionedCatalogResponse,
    ) -> std::result::Result<Vec<u8>, CatalogCodecError> {
        match response {
            VersionedCatalogResponse::V1(response) => Ok(serde_json::to_vec(
                &wire_v1::ResponseEnvelopeV1::from(response),
            )?),
        }
    }

    /// Decodes one frozen V1 response and rejects unknown outer versions.
    pub fn decode_response(
        bytes: &[u8],
    ) -> std::result::Result<VersionedCatalogResponse, CatalogCodecError> {
        #[derive(Deserialize)]
        struct VersionProbe {
            format_version: CatalogCommandFormatVersion,
        }
        let version = serde_json::from_slice::<VersionProbe>(bytes)?.format_version;
        if version != CatalogCommandFormatVersion::v1() {
            return Err(CatalogCodecError::UnsupportedVersion {
                actual: version.get(),
            });
        }
        let response = serde_json::from_slice::<wire_v1::ResponseEnvelopeV1>(bytes)?
            .into_domain()
            .map_err(CatalogCodecError::InvalidV1)?;
        Ok(VersionedCatalogResponse::V1(response))
    }

    fn validate_v1(
        command: &CatalogCommandEnvelopeV1,
    ) -> std::result::Result<(), CatalogCodecError> {
        if command.format_version != CatalogCommandFormatVersion::v1() {
            return Err(CatalogCodecError::UnsupportedVersion {
                actual: command.format_version.get(),
            });
        }
        command.verify_fingerprint()?;
        Ok(())
    }
}

/// A typed catalog mutation executed by the single catalog writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "command", rename_all = "snake_case")]
pub enum CatalogCommandV1 {
    /// Create a namespace.
    CreateNamespace(CreateNamespaceCommand),
    /// Create a table and let the catalog derive schema metadata.
    CreateTable(CreateTableCommand),
    /// Add a schema version after catalog-owned compatibility analysis.
    AlterTableSchema(AlterTableSchemaCommand),
    /// Fence an old producer and acquire a new durable epoch.
    AcquireProducer(AcquireProducerCommand),
    /// Atomically expose newly persisted immutable data files.
    CommitDataFiles(CommitDataFilesCommand),
    /// Atomically replace compacted input files with output files.
    CommitCompaction(CommitCompactionCommand),
    /// Persist a durable consumer offset.
    CommitConsumerOffset(CommitConsumerOffsetCommand),
    /// Acquire a long-lived snapshot lease.
    AcquireSnapshotLease(AcquireSnapshotLeaseCommand),
    /// Release a long-lived snapshot lease.
    ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand),
    /// Create an idempotent maintenance job.
    CreateMaintenanceJob(CreateMaintenanceJobCommand),
    /// Move a maintenance job through its durable state machine.
    TransitionMaintenanceJob(TransitionMaintenanceJobCommand),
    /// Logically drop a table while retaining leased snapshots.
    DropTable(DropTableCommand),
}

/// A command that creates one persistent namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNamespaceCommand {
    /// The caller-assigned immutable namespace identity.
    pub namespace_id: NamespaceId,
    /// The globally unique namespace name.
    pub name: CatalogName,
}

/// A command that creates one persistent table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableCommand {
    /// The caller-assigned immutable table identifier.
    pub table_id: TableId,
    /// The existing namespace that will own the table.
    pub namespace_id: NamespaceId,
    /// The name, unique within the namespace.
    pub name: CatalogName,
    /// The typed table storage and mutation definition.
    pub definition: TableDefinition,
    /// The first Arrow schema encoded as IPC.
    ///
    /// The catalog decodes this value and computes its canonical hash and
    /// initial compatibility classification; callers cannot supply either.
    pub schema_ipc: SchemaIpcPayload,
    /// Stable table configuration values.
    pub config: BTreeMap<String, String>,
}

/// A command that proposes one immutable schema version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlterTableSchemaCommand {
    /// The table whose schema will change.
    pub table_id: TableId,
    /// The schema version that must be current.
    pub expected_schema_version: u64,
    /// The proposed schema encoded as Arrow IPC.
    ///
    /// The catalog computes the canonical hash and compatibility result.
    pub schema_ipc: SchemaIpcPayload,
}

/// A command that acquires a new producer fencing epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquireProducerCommand {
    /// The table that the producer will write.
    pub table_id: TableId,
    /// The durable producer identity.
    pub producer_id: ProducerId,
    /// The previous table epoch observed by the caller, when known.
    pub expected_writer_epoch: Option<u64>,
}

/// Exposes one server-internal native append file set as a new snapshot.
///
/// Frozen V1 is intentionally narrower than the general persistence domain. It
/// accepts only a non-empty, strictly file-id-ordered set of native files for an
/// append table. File row counts, offset ranges, and source lineage must agree;
/// their aggregate coverage must start immediately after the parent snapshot's
/// watermark and end at `committed_log_offset`.
///
/// This is not a producer-fenced public ingest request: it carries no producer
/// identity, epoch, or batch sequence. The server must complete producer
/// fencing and file/directory durability before submitting it. Legacy import,
/// frame-aware persistence, and key-value materialization require a later
/// command version.
///
/// The SQLite V1 executor admits at most 4,096 files, 4,096 lineage rows per
/// file, 65,536 lineage rows in total, 64 MiB of encoded metadata, and 1 MiB per
/// typed metadata BLOB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitDataFilesCommand {
    /// The table receiving the files.
    pub table_id: TableId,
    /// The snapshot version that must still be current.
    pub expected_snapshot_version: u64,
    /// Complete native files in strict file-id order, not yet catalog-visible.
    pub files: Vec<PendingDataFile>,
    /// The exact contiguous durable boundary reached by the new files.
    pub committed_log_offset: OffsetWatermark,
}

/// Atomically replaces one append manifest set with row-preserving outputs.
///
/// Frozen V1 accepts only non-empty input and output sets in strict file-id
/// order. Every input must be visible at `expected_snapshot_version`; aggregate
/// output row count, logical offsets, schema-version coverage, and source
/// lineage must exactly equal the inputs. Compaction cannot advance or regress
/// the parent snapshot's committed log watermark.
///
/// This is a server-internal Catalog primitive, not a maintenance worker
/// protocol. It carries no maintenance job, lease generation, or lease-bound
/// worker identity, so the coordinator must fence the worker before submission.
/// The command envelope actor remains audit metadata rather than a lease token.
/// V1 does not represent preserve-order legacy discovery, frame-aware rewrite,
/// or key-value materialization; those require a later command version.
///
/// The SQLite V1 executor admits at most 4,096 input files, 4,096 output files,
/// 4,096 lineage rows per output, 65,536 lineage rows in total, 64 MiB of encoded
/// metadata, and 1 MiB per typed metadata BLOB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitCompactionCommand {
    /// The table being compacted.
    pub table_id: TableId,
    /// The snapshot version used to plan the compaction.
    pub expected_snapshot_version: u64,
    /// The exact currently visible source set in strict file-id order.
    pub input_file_ids: Vec<FileId>,
    /// Complete native row-preserving outputs in strict file-id order.
    pub output_files: Vec<PendingDataFile>,
    /// The parent snapshot's unchanged durable logical boundary.
    pub committed_log_offset: OffsetWatermark,
}

/// A command that advances a durable consumer position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitConsumerOffsetCommand {
    /// The durable consumer identity.
    pub consumer_id: DurableConsumerId,
    /// The owning table.
    pub table_id: TableId,
    /// The individual or group record whose offset is advanced.
    pub offset_authority: ConsumerOffsetAuthority,
    /// The greatest completely processed logical offset.
    pub committed_log_offset: CommittedOffset,
}

/// A command that acquires a cross-process or cross-restart snapshot lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquireSnapshotLeaseCommand {
    /// The caller-assigned lease identity.
    pub lease_id: LeaseId,
    /// The table whose files must remain available.
    pub table_id: TableId,
    /// The persisted snapshot version to protect.
    pub snapshot_version: u64,
    /// The audited owner of the lease.
    pub owner: String,
    /// The expiration time as Unix nanoseconds in UTC.
    pub expires_at_ns: u64,
}

/// A command that releases a persistent snapshot lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseSnapshotLeaseCommand {
    /// The lease identity to release.
    pub lease_id: LeaseId,
    /// The owner expected to hold the lease.
    pub owner: String,
}

/// The kind of work performed by a maintenance job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceJobKind {
    /// Rewrite files to reduce count or improve layout.
    Compaction,
    /// Apply logical retention and schedule physical cleanup.
    Retention,
    /// Delete unreferenced or no-longer-leased physical files.
    GarbageCollection,
    /// Verify catalog and data-file integrity.
    Verify,
    /// Create a consistent backup snapshot.
    Backup,
    /// Import a legacy catalog or data layout.
    Import,
}

/// The durable state of a maintenance job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceJobState {
    /// The job is waiting to be leased.
    Pending,
    /// A worker currently owns a time-bounded lease.
    Running,
    /// The job completed successfully.
    Succeeded,
    /// The job failed and may be retried according to policy.
    Failed,
    /// The job was explicitly cancelled.
    Cancelled,
}

impl MaintenanceJobState {
    /// Returns whether the durable state machine permits a transition.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Cancelled
                )
                | (Self::Failed, Self::Pending | Self::Cancelled)
        )
    }
}

/// A command that creates a durable maintenance job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateMaintenanceJobCommand {
    /// The caller-assigned job identity.
    pub job_id: JobId,
    /// The kind of maintenance work.
    pub kind: MaintenanceJobKind,
    /// The target table, absent for server-wide work.
    pub table_id: Option<TableId>,
    /// Higher values are admitted first within a job class.
    pub priority: i32,
    /// Stable input values required to replay the job.
    pub input: BTreeMap<String, String>,
}

/// A command that transitions an existing maintenance job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionMaintenanceJobCommand {
    /// The job to transition.
    pub job_id: JobId,
    /// The state that must still be current.
    pub expected_state: MaintenanceJobState,
    /// The requested next state, validated by the catalog state machine.
    pub next_state: MaintenanceJobState,
    /// The worker holding the job lease, when applicable.
    pub lease_owner: Option<String>,
    /// The lease expiration as Unix nanoseconds in UTC, when applicable.
    pub lease_expires_at_ns: Option<u64>,
    /// Stable progress values for inspection or recovery.
    pub progress: BTreeMap<String, String>,
    /// A concise failure description for failed jobs.
    pub error: Option<String>,
}

/// A command that logically drops a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropTableCommand {
    /// The table to drop.
    pub table_id: TableId,
    /// The table snapshot that must still be current.
    pub expected_snapshot_version: u64,
}

/// A durable producer checkpoint returned from the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerDescriptor {
    /// The durable producer identity.
    pub producer_id: ProducerId,
    /// The owning table.
    pub table_id: TableId,
    /// The active fencing epoch.
    pub producer_epoch: u64,
    /// The greatest durable batch sequence, or an empty producer epoch.
    pub last_batch_sequence: SequenceWatermark,
    /// The greatest durable logical boundary.
    pub last_committed_log_offset: OffsetWatermark,
}

/// A long-lived snapshot lease stored in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLeaseDescriptor {
    /// The stable lease identity.
    pub lease_id: LeaseId,
    /// The table whose files are protected.
    pub table_id: TableId,
    /// The protected persisted snapshot version.
    pub snapshot_version: u64,
    /// The audited lease owner.
    pub owner: String,
    /// The expiration time as Unix nanoseconds in UTC.
    pub expires_at_ns: u64,
}

/// A durable maintenance job descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceJobDescriptor {
    /// The stable job identity.
    pub job_id: JobId,
    /// The kind of work.
    pub kind: MaintenanceJobKind,
    /// The target table, absent for server-wide work.
    pub table_id: Option<TableId>,
    /// The current durable state.
    pub state: MaintenanceJobState,
    /// Higher values are admitted first within a job class.
    pub priority: i32,
    /// The number of times a worker has attempted the job.
    pub attempt: u32,
    /// The current worker lease owner.
    pub lease_owner: Option<String>,
    /// The worker lease expiration as Unix nanoseconds in UTC.
    pub lease_expires_at_ns: Option<u64>,
    /// Stable job input values.
    pub input: BTreeMap<String, String>,
    /// Stable progress values.
    pub progress: BTreeMap<String, String>,
    /// The most recent failure description.
    pub error: Option<String>,
    /// The creation time as Unix nanoseconds in UTC.
    pub created_at_ns: u64,
    /// The last transition time as Unix nanoseconds in UTC.
    pub updated_at_ns: u64,
}

/// The typed outcome of a committed catalog command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CatalogOutcomeV1 {
    /// A namespace descriptor was created.
    Namespace(NamespaceDescriptor),
    /// A table descriptor was created or changed.
    Table(TableDescriptor),
    /// A catalog-computed schema descriptor was added.
    Schema(SchemaDescriptor),
    /// A producer epoch was acquired.
    Producer(ProducerDescriptor),
    /// A persisted snapshot was committed.
    Snapshot(TableSnapshot),
    /// A consumer offset was committed.
    ConsumerOffset {
        /// The individual or group record whose offset was advanced.
        offset_authority: ConsumerOffsetAuthority,
        /// The committed logical offset.
        committed_log_offset: CommittedOffset,
    },
    /// A long-lived snapshot lease was acquired.
    SnapshotLease(SnapshotLeaseDescriptor),
    /// A long-lived snapshot lease was released.
    SnapshotLeaseReleased {
        /// The released lease identity.
        lease_id: LeaseId,
    },
    /// A maintenance job was created or transitioned.
    MaintenanceJob(MaintenanceJobDescriptor),
    /// A table was logically dropped.
    TableDropped {
        /// The dropped table identity.
        table_id: TableId,
    },
}

/// The committed response to an idempotent catalog command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogResponseV1 {
    /// The idempotency key from the command envelope.
    pub request_id: RequestId,
    /// The catalog revision after the command committed.
    pub catalog_revision: u64,
    /// The typed command result.
    pub outcome: CatalogOutcomeV1,
}

/// A committed catalog response routed by its frozen wire version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedCatalogResponse {
    /// A frozen V1 catalog response.
    V1(CatalogResponseV1),
}

impl From<CatalogResponseV1> for VersionedCatalogResponse {
    fn from(response: CatalogResponseV1) -> Self {
        Self::V1(response)
    }
}

/// An exact historical table snapshot requested from the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshotSelector {
    /// The immutable table identity.
    pub table_id: TableId,
    /// The exact persisted snapshot version observed while pinning live state.
    pub snapshot_version: u64,
    /// The exact schema version observed in the same actor turn.
    pub schema_version: u64,
}

/// One exact table version resolved within an immutable catalog read transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogTableSnapshot {
    /// The exact actor-observed versions this aggregate satisfies.
    selector: CatalogSnapshotSelector,
    /// The persistent table descriptor.
    table: TableDescriptor,
    /// The schema selected by the historical snapshot.
    schema: SchemaDescriptor,
    /// The exact persisted snapshot descriptor.
    snapshot: TableSnapshot,
    /// Files visible at the exact persisted snapshot version.
    files: DataFiles,
}

impl CatalogTableSnapshot {
    /// Creates and validates one exact selector-bound table snapshot aggregate.
    pub fn new(
        selector: CatalogSnapshotSelector,
        table: TableDescriptor,
        schema: SchemaDescriptor,
        snapshot: TableSnapshot,
        files: DataFiles,
    ) -> std::result::Result<Self, InvalidCatalogTableSnapshot> {
        if table.table_id != selector.table_id
            || schema.table_id != selector.table_id
            || snapshot.table_id != selector.table_id
            || files
                .as_slice()
                .iter()
                .any(|file| file.table_id() != selector.table_id)
        {
            return Err(InvalidCatalogTableSnapshot::TableIdentityMismatch);
        }
        if selector.schema_version > table.current_schema_version {
            return Err(InvalidCatalogTableSnapshot::SchemaBeyondCurrent {
                selected: selector.schema_version,
                current: table.current_schema_version,
            });
        }
        if selector.snapshot_version > table.current_snapshot_version {
            return Err(InvalidCatalogTableSnapshot::SnapshotBeyondCurrent {
                selected: selector.snapshot_version,
                current: table.current_snapshot_version,
            });
        }
        if schema.schema_version != selector.schema_version {
            return Err(InvalidCatalogTableSnapshot::SchemaVersionMismatch {
                expected: selector.schema_version,
                actual: schema.schema_version,
            });
        }
        if snapshot.snapshot_version != selector.snapshot_version {
            return Err(InvalidCatalogTableSnapshot::SnapshotVersionMismatch {
                expected: selector.snapshot_version,
                actual: snapshot.snapshot_version,
            });
        }
        if let Some(file) = files
            .as_slice()
            .iter()
            .find(|file| !file.is_visible_at(selector.snapshot_version))
        {
            return Err(InvalidCatalogTableSnapshot::FileNotVisible {
                file_id: file.file_id(),
                snapshot_version: selector.snapshot_version,
            });
        }
        if let Some(file) = files
            .as_slice()
            .iter()
            .find(|file| file.file.schema_version > selector.schema_version)
        {
            return Err(InvalidCatalogTableSnapshot::FileSchemaTooNew {
                file_id: file.file_id(),
                file_schema_version: file.file.schema_version,
                selected_schema_version: selector.schema_version,
            });
        }
        Ok(Self {
            selector,
            table,
            schema,
            snapshot,
            files,
        })
    }

    /// Returns the exact selector proven by this aggregate.
    #[must_use]
    pub const fn selector(&self) -> &CatalogSnapshotSelector {
        &self.selector
    }

    /// Borrows the persistent table descriptor.
    #[must_use]
    pub const fn table(&self) -> &TableDescriptor {
        &self.table
    }

    /// Borrows the exact selected schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Borrows the exact persisted snapshot descriptor.
    #[must_use]
    pub const fn snapshot(&self) -> &TableSnapshot {
        &self.snapshot
    }

    /// Borrows the deterministic visible file list.
    #[must_use]
    pub const fn files(&self) -> &DataFiles {
        &self.files
    }
}

impl<'de> Deserialize<'de> for CatalogTableSnapshot {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawTableSnapshot {
            selector: CatalogSnapshotSelector,
            table: TableDescriptor,
            schema: SchemaDescriptor,
            snapshot: TableSnapshot,
            files: DataFiles,
        }
        let raw = RawTableSnapshot::deserialize(deserializer)?;
        Self::new(raw.selector, raw.table, raw.schema, raw.snapshot, raw.files)
            .map_err(serde::de::Error::custom)
    }
}

/// An inconsistent selector-bound catalog table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidCatalogTableSnapshot {
    /// Every nested descriptor and file must belong to the selected table.
    #[error("catalog table snapshot contains mismatched table identities")]
    TableIdentityMismatch,
    /// Historical selection cannot name a schema newer than table metadata.
    #[error("selected schema is newer than current selected=[{selected}] current=[{current}]")]
    SchemaBeyondCurrent {
        /// The invalid selected version.
        selected: u64,
        /// The table's current schema version.
        current: u64,
    },
    /// Historical selection cannot name a snapshot newer than table metadata.
    #[error("selected snapshot is newer than current selected=[{selected}] current=[{current}]")]
    SnapshotBeyondCurrent {
        /// The invalid selected version.
        selected: u64,
        /// The table's current snapshot version.
        current: u64,
    },
    /// The schema descriptor must equal the exact selected version.
    #[error("catalog schema version mismatch expected=[{expected}] actual=[{actual}]")]
    SchemaVersionMismatch {
        /// The selector's exact schema version.
        expected: u64,
        /// The nested schema descriptor version.
        actual: u64,
    },
    /// The snapshot descriptor must equal the exact selected version.
    #[error("catalog snapshot version mismatch expected=[{expected}] actual=[{actual}]")]
    SnapshotVersionMismatch {
        /// The selector's exact snapshot version.
        expected: u64,
        /// The nested snapshot descriptor version.
        actual: u64,
    },
    /// Every returned file must be visible at the selected snapshot version.
    #[error("catalog file not visible file_id=[{file_id}] snapshot_version=[{snapshot_version}]")]
    FileNotVisible {
        /// The incorrectly included file.
        file_id: FileId,
        /// The exact selected snapshot version.
        snapshot_version: u64,
    },
    /// A file cannot require a schema newer than the selected decoder schema.
    #[error("file schema newer file=[{file_schema_version}] max=[{selected_schema_version}]")]
    FileSchemaTooNew {
        /// The incompatible file identity.
        file_id: FileId,
        /// The schema encoded by the file.
        file_schema_version: u64,
        /// The exact selected schema version.
        selected_schema_version: u64,
    },
}

/// All exact table versions resolved in one catalog read transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogQuerySnapshot {
    /// The catalog revision shared by all resolved historical versions.
    catalog_revision: u64,
    /// The resolved table snapshots in deterministic table-id order.
    tables: Vec<CatalogTableSnapshot>,
}

impl CatalogQuerySnapshot {
    /// Creates a revision-consistent aggregate in strict table-id order.
    pub fn new(
        catalog_revision: u64,
        tables: Vec<CatalogTableSnapshot>,
    ) -> std::result::Result<Self, InvalidCatalogQuerySnapshot> {
        for pair in tables.windows(2) {
            match pair[0].table().table_id.cmp(&pair[1].table().table_id) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(InvalidCatalogQuerySnapshot::DuplicateTableId {
                        table_id: pair[0].table().table_id,
                    });
                }
                std::cmp::Ordering::Greater => {
                    return Err(InvalidCatalogQuerySnapshot::UnsortedTableIds);
                }
            }
        }
        Ok(Self {
            catalog_revision,
            tables,
        })
    }

    /// Creates a valid empty-table snapshot at one authoritative catalog revision.
    #[must_use]
    pub const fn empty(catalog_revision: u64) -> Self {
        Self {
            catalog_revision,
            tables: Vec::new(),
        }
    }

    /// Returns true when the query references no tables.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Returns the catalog revision shared by all nested snapshots.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Borrows snapshots in deterministic table-id order.
    #[must_use]
    pub fn tables(&self) -> &[CatalogTableSnapshot] {
        &self.tables
    }
}

impl<'de> Deserialize<'de> for CatalogQuerySnapshot {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawQuerySnapshot {
            catalog_revision: u64,
            tables: Vec<CatalogTableSnapshot>,
        }
        let raw = RawQuerySnapshot::deserialize(deserializer)?;
        Self::new(raw.catalog_revision, raw.tables).map_err(serde::de::Error::custom)
    }
}

/// Invalid deterministic ordering in a query-level catalog snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum InvalidCatalogQuerySnapshot {
    /// Table aggregates must be sorted by immutable identity.
    #[error("catalog query snapshots must be strictly sorted by table id")]
    UnsortedTableIds,
    /// One table may appear only once in a query snapshot.
    #[error("duplicate catalog query table id table_id=[{table_id}]")]
    DuplicateTableId {
        /// The repeated immutable table identity.
        table_id: TableId,
    },
}

/// A namespace reference accepted by catalog discovery APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum NamespaceRef {
    /// Resolve by immutable identity.
    Id(NamespaceId),
    /// Resolve by globally unique name.
    Name(CatalogName),
}

/// A bounded namespace listing request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNamespacesRequest {
    /// Continue strictly after this namespace identity, when present.
    pub after: Option<NamespaceId>,
    /// Maximum number of descriptors to return.
    pub limit: NonZeroU32,
}

/// One revision-consistent page of namespaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    /// The catalog revision used by the read transaction.
    pub catalog_revision: u64,
    /// Namespace descriptors ordered by immutable identity.
    pub namespaces: Vec<NamespaceDescriptor>,
    /// The cursor for the next page, absent when the listing is complete.
    pub next_after: Option<NamespaceId>,
}

/// A bounded lightweight table listing request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTablesRequest {
    /// Restrict results to one namespace, or list across all namespaces.
    pub namespace_id: Option<NamespaceId>,
    /// Continue strictly after this table identity, when present.
    pub after: Option<TableId>,
    /// Maximum number of descriptors to return.
    pub limit: NonZeroU32,
}

/// One revision-consistent page of lightweight table descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTablesResponse {
    /// The catalog revision used by the read transaction.
    pub catalog_revision: u64,
    /// Table descriptors ordered by immutable identity, without file manifests.
    pub tables: Vec<TableDescriptor>,
    /// The cursor for the next page, absent when the listing is complete.
    pub next_after: Option<TableId>,
}

/// One canonical managed file identity observed by a storage reconciler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedDataFileProbe {
    /// The immutable file identity encoded by the candidate path and footer.
    pub file_id: FileId,
    /// The table identity encoded by the candidate path and footer.
    pub table_id: TableId,
    /// The canonical candidate path relative to the managed data root.
    pub path: crate::table::ManagedDataPath,
}

/// Lightweight physical identity and lifecycle state for one Catalog data file.
///
/// This projection intentionally omits partition values, statistics, source
/// lineage, and other manifest payloads. Recovery and maintenance callers can
/// therefore reconcile bounded batches of local file identities without
/// loading query manifests into memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDataFileReference {
    /// The immutable file identity.
    pub file_id: FileId,
    /// The table that owns the file.
    pub table_id: TableId,
    /// The managed or external physical location recorded by the Catalog.
    pub location: DataFileLocation,
    /// The expected physical length in bytes.
    pub file_size_bytes: u64,
    /// The canonical content checksum stored at commit time.
    pub checksum: String,
    /// The first snapshot version that exposed this file.
    pub added_snapshot_version: u64,
    /// The first snapshot version that stopped exposing this file.
    pub removed_snapshot_version: Option<u64>,
    /// The current physical-deletion lifecycle state.
    pub state: DataFileState,
}

/// Catalog identity outcome for one managed candidate probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogDataFileProbeMatch {
    /// Neither the immutable ID nor the managed path has ever been registered.
    Unreferenced,
    /// Both keys resolve the exact same Catalog row and table identity.
    Exact {
        /// The matching immutable Catalog record.
        reference: CatalogDataFileReference,
    },
    /// One or both keys resolve a different immutable Catalog identity.
    IdentityConflict {
        /// The row found through the requested file ID, when one exists.
        by_file_id: Option<CatalogDataFileReference>,
        /// The row found through the requested managed path, when one exists.
        by_path: Option<CatalogDataFileReference>,
    },
}

/// Dense Catalog result corresponding to one managed file probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDataFileProbeResult {
    /// The exact validated probe supplied by the caller.
    pub probe: ManagedDataFileProbe,
    /// The Catalog rows found through both immutable keys.
    pub matched: CatalogDataFileProbeMatch,
}

/// One revision-consistent result for a bounded managed-file identity probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDataFileReferenceSnapshot {
    /// The Catalog revision observed by the read transaction.
    pub catalog_revision: u64,
    /// Dense results in the same strict file-id order as the request.
    pub results: Vec<CatalogDataFileProbeResult>,
}

/// Bounded request to claim managed historical files for physical deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimGarbageCollectionFilesRequest {
    /// The table whose removed files are being maintained.
    pub table_id: TableId,
    /// Maximum existing `deleting` files returned for physical-deletion retry.
    pub retry_limit: u32,
    /// Maximum `committed` files newly transitioned to `deleting`.
    pub new_claim_limit: u32,
}

/// Query-admission exclusion retained through one garbage-collection claim transaction.
///
/// The server creates this value while holding exclusive local snapshot-pin admission and after
/// sampling the lowest in-process pinned snapshot. Moving the value into Catalog claim admission
/// prevents caller cancellation from releasing that exclusion before the single writer executes
/// the transaction. Durable lease expiry is evaluated separately with the Catalog-owned clock.
pub struct CatalogGarbageCollectionGuard {
    guard: Box<dyn Any + Send + Sync>,
}

impl fmt::Debug for CatalogGarbageCollectionGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogGarbageCollectionGuard")
            .finish_non_exhaustive()
    }
}

impl CatalogGarbageCollectionGuard {
    /// Wraps a server-owned query-admission exclusion and its protected pin sample.
    ///
    /// Application code must obtain the opaque value from the server's snapshot lease registry;
    /// this constructor exists so that registry can cross the Catalog crate boundary.
    #[doc(hidden)]
    pub fn from_opaque<T>(guard: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            guard: Box::new(guard),
        }
    }

    /// Consumes and type-checks one implementation-owned exclusion token.
    #[doc(hidden)]
    pub fn try_into_opaque<T>(self) -> std::result::Result<T, Self>
    where
        T: Any + Send + Sync,
    {
        match self.guard.downcast::<T>() {
            Ok(guard) => Ok(*guard),
            Err(guard) => Err(Self { guard }),
        }
    }
}

/// One atomic garbage-collection claim result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedGarbageCollectionFiles {
    /// Catalog revision after newly claimed files entered `deleting`.
    pub catalog_revision: u64,
    /// Complete deleting descriptors, with retries before newly claimed files.
    ///
    /// Each segment is in strict file-id order. The retry segment contains
    /// previously claimed files whose physical deletion must resume after a
    /// worker failure or process restart.
    pub files: Vec<DataFileDescriptor>,
}

/// Exact managed-file set whose successful physical deletion is being finalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizeGarbageCollectionFilesRequest {
    /// The table owning every supplied file identity.
    pub table_id: TableId,
    /// Nonempty file identities in strict ascending order.
    pub file_ids: Vec<FileId>,
}

/// Durable result of finalizing physical data-file deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedGarbageCollectionFiles {
    /// Catalog revision after files transitioned from `deleting` to `deleted`.
    pub catalog_revision: u64,
    /// The exact finalized file identities in strict ascending order.
    pub file_ids: Vec<FileId>,
}

/// Read-only outcome for one complete Catalog command idempotency probe.
///
/// A missing record proves only that the supplied envelope has no retained
/// response at the instant of the read. It does not authorize the caller to
/// execute the mutation without the normal Catalog write protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogCommandReplayProbe {
    /// No retained idempotency record exists for the command request identity.
    Missing,
    /// The exact command fingerprint has an authoritative committed response.
    Exact {
        /// The original response committed atomically with the mutation.
        response: VersionedCatalogResponse,
    },
}

/// Shared authority held from physical publication through one Catalog mutation.
///
/// The token is opaque outside the Catalog implementation. Moving it into a
/// mutation keeps publication excluded from concurrent physical cleanup even
/// when the caller cancels its response future after actor admission.
pub struct CatalogPublicationPermit {
    permit: Box<dyn Any + Send + Sync>,
}

impl fmt::Debug for CatalogPublicationPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogPublicationPermit")
            .finish_non_exhaustive()
    }
}

impl CatalogPublicationPermit {
    /// Wraps one implementation-owned shared publication token.
    ///
    /// This constructor exists for Catalog implementations; application code
    /// obtains tokens through [`CatalogPublicationControl`].
    #[doc(hidden)]
    pub fn from_opaque<T>(permit: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            permit: Box::new(permit),
        }
    }

    /// Consumes and type-checks one implementation-owned publication token.
    ///
    /// Catalog implementations must use an unexported concrete token type and
    /// reject a failed downcast. A caller can wrap an arbitrary value with
    /// [`Self::from_opaque`], but cannot construct the implementation's private
    /// capability type.
    #[doc(hidden)]
    pub fn try_into_opaque<T>(self) -> std::result::Result<T, Self>
    where
        T: Any + Send + Sync,
    {
        match self.permit.downcast::<T>() {
            Ok(permit) => Ok(*permit),
            Err(permit) => Err(Self { permit }),
        }
    }
}

/// Exclusive authority preventing Catalog file publication during physical cleanup.
///
/// Implementations issue this token only after every admitted shared
/// publication permit has reached a terminal Catalog result.
pub struct CatalogPublicationExclusion {
    _permit: Box<dyn Any + Send + Sync>,
}

impl fmt::Debug for CatalogPublicationExclusion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogPublicationExclusion")
            .finish_non_exhaustive()
    }
}

impl CatalogPublicationExclusion {
    /// Wraps one implementation-owned exclusive publication token.
    ///
    /// This constructor exists for Catalog implementations; storage obtains
    /// exclusions through [`CatalogPublicationControl`].
    #[doc(hidden)]
    pub fn from_opaque<T>(permit: T) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            _permit: Box::new(permit),
        }
    }
}

/// Read-only access to immutable persistent catalog state.
pub trait CatalogRead: Send + Sync {
    /// Returns the current catalog revision.
    fn revision(&self) -> BoxFuture<'_, Result<u64>>;

    /// Resolves a namespace without loading table state.
    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>>;

    /// Lists namespaces in one bounded, revision-consistent page.
    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>>;

    /// Resolves a table reference without loading its file manifest.
    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>>;

    /// Lists lightweight table descriptors without loading file manifests.
    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>>;

    /// Resolves table descriptors in one transaction without reading manifests.
    ///
    /// These descriptors identify actors for the live pin step. Their current
    /// snapshot fields are advisory only; the actor-observed versions returned
    /// by the pin step are the exact inputs to `read_snapshot_at`.
    /// An empty input succeeds with an empty vector from one read transaction.
    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>>;

    /// Reads exact historical versions in one catalog read transaction.
    ///
    /// Selectors must use nonzero schema and snapshot versions and be supplied
    /// in strict table-id order without duplicates. This lets implementations
    /// reject malformed input before copying it into a bounded actor queue.
    /// Implementations must never silently substitute a newer snapshot when a
    /// requested version is unavailable.
    /// An empty selector list succeeds with [`CatalogQuerySnapshot::empty`] at
    /// the catalog revision observed by that read transaction.
    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>>;
}

/// Bounded Catalog projection used by storage recovery and maintenance.
///
/// This is separate from [`CatalogRead`] because legacy adapters cannot
/// provide stable file identities. New storage code should require this trait
/// explicitly instead of silently degrading to legacy path matching.
pub trait CatalogFileRead: CatalogRead {
    /// Inspects exact file identities and paths in one Catalog read transaction.
    ///
    /// Probes must be supplied in strict file-id order without duplicate IDs or
    /// paths. Implementations return one dense result per probe and include
    /// every lifecycle state. An unreferenced result is an observation at the
    /// returned revision, not authorization to delete the physical file;
    /// publication admission must also be excluded until deletion completes.
    /// The response must not load file lineage or manifest blobs.
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>>;
}

/// Server-internal durable lifecycle boundary for managed historical files.
///
/// Both operations run through the single Catalog writer. Once either future
/// has been polled and admitted, cancelling its waiter does not cancel the
/// mutation. Claim is replayable because existing `deleting` rows are returned;
/// finalize is replayable for an exact set already in `deleted`.
pub trait CatalogGarbageCollection: Send + Sync {
    /// Claims eligible removed files and returns complete deleting descriptors.
    ///
    /// Implementations must evaluate durable leases, the supplied runtime pin,
    /// candidate selection, and `committed -> deleting` transitions in one
    /// write transaction. Existing `deleting` retries and newly claimed files
    /// have independent nonzero bounds so neither class can consume the other
    /// class's progress budget. Results return retries first, then newly claimed
    /// files; each segment is strictly file-id ordered.
    fn claim_garbage_collection_files(
        &self,
        guard: CatalogGarbageCollectionGuard,
        request: ClaimGarbageCollectionFilesRequest,
    ) -> BoxFuture<'_, Result<ClaimedGarbageCollectionFiles>>;

    /// Finalizes an exact physically deleted set as durable `deleted` rows.
    ///
    /// Every identity must belong to the requested table and already be either
    /// `deleting` or `deleted`; otherwise the complete operation fails without
    /// partial state changes.
    fn finalize_garbage_collection_files(
        &self,
        request: FinalizeGarbageCollectionFilesRequest,
    ) -> BoxFuture<'_, Result<FinalizedGarbageCollectionFiles>>;
}

/// Read-only access to durable Catalog command idempotency records.
///
/// Implementations must validate the complete versioned envelope and compare
/// its canonical fingerprint, operation, and format against the retained
/// record. Reusing a request ID with a different fingerprint must return a
/// conflict. This capability must never prepare or execute the mutation.
pub trait CatalogIdempotencyRead: Send + Sync {
    /// Probes whether an exact command already has a committed response.
    ///
    /// Constructing and dropping the returned future before polling must not
    /// perform work. A missing result is a read observation, not a reservation;
    /// callers that may execute a new mutation must still use [`CatalogWrite`].
    fn probe_command_replay<'a>(
        &'a self,
        command: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>>;
}

/// Serialized mutation access to persistent catalog state.
pub trait CatalogWrite: Send + Sync {
    /// Validates format, fingerprint, revision, and invariants before commit.
    ///
    /// The catalog writer assigns all audit and commit timestamps inside its
    /// transaction. Command callers cannot supply or override server time. A
    /// successful mutation and its idempotency response must become durable in
    /// the same atomic transaction. Replaying the exact envelope, including
    /// its request ID, must return the original response; reusing that request
    /// ID with a different fingerprint must conflict.
    ///
    /// Once this future has been polled, cancellation may abandon only the
    /// response while the mutation still commits. Callers must treat that state
    /// as commit-unknown and replay the unchanged envelope. Implementations
    /// must mark an error retryable only when replaying that exact envelope can
    /// make progress without changing its payload or server configuration.
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>>;
}

/// Coordinates physical file publication and deletion with Catalog mutations.
///
/// Every file-producing path must acquire a shared permit before publishing a
/// final directory entry, then move that permit into the corresponding Catalog
/// command. Cleanup acquires the exclusive token before its first reference
/// probe and retains it through physical verification and unlink.
pub trait CatalogPublicationControl: Send + Sync {
    /// Acquires one shared permit for a file publication and Catalog mutation.
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>>;

    /// Executes a mutation while transferring an acquired shared permit to the Catalog actor.
    ///
    /// Implementations must reject a permit issued by another Catalog instance.
    /// Once admitted, the actor owns the permit until the mutation reaches a
    /// terminal result, independently of response-future cancellation.
    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>>;

    /// Acquires exclusive cleanup authority after all shared permits drain.
    fn acquire_publication_exclusion(&self) -> BoxFuture<'_, Result<CatalogPublicationExclusion>>;
}

/// Native persistence Catalog capabilities required for safe file publication.
///
/// The explicit composition keeps legacy adapters that cannot expose stable
/// file identities or durable idempotency records out of native persistence.
pub trait CatalogPersistence:
    CatalogFileRead + CatalogIdempotencyRead + CatalogWrite + CatalogPublicationControl
{
}

impl<T> CatalogPersistence for T where
    T: CatalogFileRead + CatalogIdempotencyRead + CatalogWrite + CatalogPublicationControl + ?Sized
{
}

/// The complete persistent catalog boundary.
pub trait Catalog: CatalogRead + CatalogWrite {}

impl<T> Catalog for T where T: CatalogRead + CatalogWrite + ?Sized {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use sha2::{Digest, Sha256};

    use super::wire_v1;

    use super::{
        AcquireProducerCommand, AcquireSnapshotLeaseCommand, AlterTableSchemaCommand, Catalog,
        CatalogActor, CatalogActorKind, CatalogCodec, CatalogCodecError, CatalogCommandEnvelopeV1,
        CatalogCommandFingerprintInputV1, CatalogCommandFormatVersion, CatalogCommandV1,
        CatalogFileRead, CatalogFingerprintError, CatalogGarbageCollection, CatalogIdempotencyRead,
        CatalogOutcomeV1, CatalogPersistence, CatalogPublicationControl, CatalogQuerySnapshot,
        CatalogRead, CatalogResponseV1, CatalogSnapshotSelector, CatalogTableSnapshot,
        CatalogWrite, CommitCompactionCommand, CommitConsumerOffsetCommand, CommitDataFilesCommand,
        CreateMaintenanceJobCommand, CreateNamespaceCommand, CreateTableCommand, DropTableCommand,
        MaintenanceJobDescriptor, MaintenanceJobKind, MaintenanceJobState, ProducerDescriptor,
        ReleaseSnapshotLeaseCommand, SnapshotLeaseDescriptor, TransitionMaintenanceJobCommand,
        VersionedCatalogCommand, VersionedCatalogResponse, MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
    };
    use crate::table::{
        AppendTableDefinition, CatalogName, ColumnName, ColumnSet, DataFileDescriptor,
        DataFileLocation, DataFileProvenance, DataFileState, DataFiles, ExternalDataPath,
        FiniteMetadataFloat, FrameTableDefinition, KeyValueTableDefinition, ManagedDataPath,
        MetadataMap, MetadataScalar, NamespaceDescriptor, NativeDataFileProvenance,
        PendingDataFile, SchemaCompatibility, SchemaDescriptor, SchemaIpcPayload,
        SnapshotOperation, SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages,
        TableDefinition, TableDescriptor, TableSnapshot, TableState,
    };
    use crate::{
        CommittedOffset, ConsumerGroupId, ConsumerOffsetAuthority, DurableConsumerId, FileId,
        JobId, LeaseId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, ProducerId,
        RequestId, SequenceWatermark, TableId,
    };

    fn namespace_id() -> NamespaceId {
        NamespaceId::from_u128(1).unwrap()
    }

    fn request_id() -> RequestId {
        RequestId::from_u128(2).unwrap()
    }

    fn command() -> CatalogCommandV1 {
        CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: namespace_id(),
            name: CatalogName::new("market").unwrap(),
        })
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::User,
            principal: "alice".to_owned(),
        }
    }

    fn all_metadata_scalars() -> MetadataMap {
        BTreeMap::from([
            ("boolean".to_owned(), MetadataScalar::Boolean(true)),
            ("bytes".to_owned(), MetadataScalar::Bytes(vec![1, 2, 3])),
            (
                "float64".to_owned(),
                MetadataScalar::Float64(FiniteMetadataFloat::new(1.5).unwrap()),
            ),
            ("int64".to_owned(), MetadataScalar::Int64(-4)),
            ("null".to_owned(), MetadataScalar::Null),
            (
                "string".to_owned(),
                MetadataScalar::String("value".to_owned()),
            ),
            ("uint64".to_owned(), MetadataScalar::UInt64(5)),
        ])
    }

    fn native_pending_file(file_id: u128, table_id: TableId) -> PendingDataFile {
        let offsets = OffsetRange::new(LogOffset::new(0), LogOffset::new(1)).unwrap();
        let lineage = SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: 10,
                generation: 11,
                writer_epoch: 12,
            },
            offsets,
        }])
        .unwrap();
        PendingDataFile {
            file_id: FileId::from_u128(file_id).unwrap(),
            table_id,
            schema_version: 2,
            location: DataFileLocation::Managed(
                ManagedDataPath::new(format!("table/{file_id}.parquet")).unwrap(),
            ),
            file_size_bytes: 100,
            row_count: NonZeroU64::new(2).unwrap(),
            provenance: DataFileProvenance::Native(
                NativeDataFileProvenance::new(offsets, lineage).unwrap(),
            ),
            partition: BTreeMap::from([(
                "trade_date".to_owned(),
                MetadataScalar::String("2026-07-13".to_owned()),
            )]),
            statistics: BTreeMap::from([("rows".to_owned(), MetadataScalar::UInt64(2))]),
            metadata: all_metadata_scalars(),
            checksum: "sha256:01".to_owned(),
            created_at_ns: 13,
        }
    }

    fn legacy_pending_file(file_id: u128, table_id: TableId) -> PendingDataFile {
        PendingDataFile {
            file_id: FileId::from_u128(file_id).unwrap(),
            table_id,
            schema_version: 1,
            location: DataFileLocation::ExternalLegacy(
                ExternalDataPath::new(format!("/legacy/{file_id}.parquet")).unwrap(),
            ),
            file_size_bytes: 200,
            row_count: NonZeroU64::new(3).unwrap(),
            provenance: DataFileProvenance::Legacy {
                offsets: Some(OffsetRange::new(LogOffset::new(2), LogOffset::new(4)).unwrap()),
                preserve_discovery_order: true,
            },
            partition: BTreeMap::new(),
            statistics: BTreeMap::new(),
            metadata: BTreeMap::new(),
            checksum: "sha256:02".to_owned(),
            created_at_ns: 14,
        }
    }

    fn nested_wire_commands() -> Vec<CatalogCommandV1> {
        let table_id = TableId::from_u128(3).unwrap();
        let keys = ColumnSet::new(vec![
            ColumnName::new("symbol").unwrap(),
            ColumnName::new("venue").unwrap(),
        ])
        .unwrap();
        let native = native_pending_file(30, table_id);
        let legacy = legacy_pending_file(31, table_id);
        vec![
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id,
                namespace_id: namespace_id(),
                name: CatalogName::new("frames").unwrap(),
                definition: TableDefinition::Frame(FrameTableDefinition {
                    by: ColumnName::new("symbol").unwrap(),
                }),
                schema_ipc: SchemaIpcPayload::new(vec![1, 2]).unwrap(),
                config: BTreeMap::new(),
            }),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id,
                namespace_id: namespace_id(),
                name: CatalogName::new("state").unwrap(),
                definition: TableDefinition::KeyValue(KeyValueTableDefinition::new(keys).unwrap()),
                schema_ipc: SchemaIpcPayload::new(vec![2, 3]).unwrap(),
                config: BTreeMap::from([("mode".to_owned(), "latest".to_owned())]),
            }),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id,
                expected_snapshot_version: 7,
                files: vec![native.clone(), legacy],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(4)),
            }),
            CatalogCommandV1::CommitCompaction(CommitCompactionCommand {
                table_id,
                expected_snapshot_version: 8,
                input_file_ids: vec![FileId::from_u128(20).unwrap()],
                output_files: vec![native],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(4)),
            }),
        ]
    }

    fn all_response_outcomes() -> Vec<CatalogOutcomeV1> {
        let table_id = TableId::from_u128(3).unwrap();
        let consumer_group_id = ConsumerGroupId::from_u128(9).unwrap();
        let keys = ColumnSet::new(vec![ColumnName::new("symbol").unwrap()]).unwrap();
        vec![
            CatalogOutcomeV1::Namespace(NamespaceDescriptor {
                namespace_id: namespace_id(),
                name: CatalogName::new("market").unwrap(),
                created_at_ns: 1,
            }),
            CatalogOutcomeV1::Table(TableDescriptor {
                table_id,
                namespace_id: namespace_id(),
                name: CatalogName::new("state").unwrap(),
                definition: TableDefinition::KeyValue(KeyValueTableDefinition::new(keys).unwrap()),
                current_schema_version: 2,
                current_snapshot_version: 3,
                writer_epoch: 4,
                state: TableState::Dropping,
                config: BTreeMap::from([("mode".to_owned(), "latest".to_owned())]),
                created_at_ns: 5,
                updated_at_ns: 6,
            }),
            CatalogOutcomeV1::Schema(SchemaDescriptor {
                table_id,
                schema_version: 2,
                schema_ipc: SchemaIpcPayload::new(vec![7, 8]).unwrap(),
                schema_hash: "sha256:03".to_owned(),
                compatibility: SchemaCompatibility::Backward,
                created_at_ns: 9,
            }),
            CatalogOutcomeV1::Producer(ProducerDescriptor {
                producer_id: ProducerId::from_u128(5).unwrap(),
                table_id,
                producer_epoch: 6,
                last_batch_sequence: SequenceWatermark::Known(7),
                last_committed_log_offset: OffsetWatermark::LegacyUnknown,
            }),
            CatalogOutcomeV1::Snapshot(TableSnapshot {
                table_id,
                snapshot_version: 8,
                parent_snapshot_version: Some(7),
                operation: SnapshotOperation::Compaction,
                commit_lsn: 9,
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(10)),
                created_at_ns: 11,
                summary: all_metadata_scalars(),
            }),
            CatalogOutcomeV1::ConsumerOffset {
                offset_authority: ConsumerOffsetAuthority::Group(consumer_group_id),
                committed_log_offset: CommittedOffset::Known(LogOffset::new(12)),
            },
            CatalogOutcomeV1::SnapshotLease(SnapshotLeaseDescriptor {
                lease_id: LeaseId::from_u128(7).unwrap(),
                table_id,
                snapshot_version: 8,
                owner: "query".to_owned(),
                expires_at_ns: 13,
            }),
            CatalogOutcomeV1::SnapshotLeaseReleased {
                lease_id: LeaseId::from_u128(7).unwrap(),
            },
            CatalogOutcomeV1::MaintenanceJob(MaintenanceJobDescriptor {
                job_id: JobId::from_u128(8).unwrap(),
                kind: MaintenanceJobKind::Backup,
                table_id: Some(table_id),
                state: MaintenanceJobState::Failed,
                priority: 14,
                attempt: 2,
                lease_owner: Some("worker".to_owned()),
                lease_expires_at_ns: Some(15),
                input: BTreeMap::from([("source".to_owned(), "catalog".to_owned())]),
                progress: BTreeMap::from([("files".to_owned(), "3".to_owned())]),
                error: Some("checksum mismatch".to_owned()),
                created_at_ns: 16,
                updated_at_ns: 17,
            }),
            CatalogOutcomeV1::TableDropped { table_id },
        ]
    }

    fn file_descriptor(
        file_id: u128,
        table_id: TableId,
        added: u64,
        removed: Option<u64>,
    ) -> DataFileDescriptor {
        DataFileDescriptor {
            file: crate::table::PendingDataFile {
                file_id: FileId::from_u128(file_id).unwrap(),
                table_id,
                schema_version: 2,
                location: DataFileLocation::Managed(
                    ManagedDataPath::new(format!("table/{file_id}.parquet")).unwrap(),
                ),
                file_size_bytes: 1,
                row_count: NonZeroU64::new(1).unwrap(),
                provenance: DataFileProvenance::Legacy {
                    offsets: None,
                    preserve_discovery_order: false,
                },
                partition: BTreeMap::new(),
                statistics: BTreeMap::new(),
                metadata: BTreeMap::new(),
                checksum: "sha256:00".to_owned(),
                created_at_ns: 1,
            },
            legacy_discovery_order: None,
            added_snapshot_version: added,
            removed_snapshot_version: removed,
            state: DataFileState::Committed,
        }
    }

    fn table_snapshot(table_value: u128, file: Option<DataFileDescriptor>) -> CatalogTableSnapshot {
        let table_id = TableId::from_u128(table_value).unwrap();
        CatalogTableSnapshot::new(
            CatalogSnapshotSelector {
                table_id,
                snapshot_version: 3,
                schema_version: 2,
            },
            TableDescriptor {
                table_id,
                namespace_id: namespace_id(),
                name: CatalogName::new(format!("table_{table_value}")).unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                current_schema_version: 4,
                current_snapshot_version: 5,
                writer_epoch: 1,
                state: TableState::Active,
                config: BTreeMap::new(),
                created_at_ns: 1,
                updated_at_ns: 2,
            },
            SchemaDescriptor {
                table_id,
                schema_version: 2,
                schema_ipc: SchemaIpcPayload::new(vec![1]).unwrap(),
                schema_hash: "sha256:01".to_owned(),
                compatibility: SchemaCompatibility::Initial,
                created_at_ns: 1,
            },
            TableSnapshot {
                table_id,
                snapshot_version: 3,
                parent_snapshot_version: Some(2),
                operation: SnapshotOperation::Persist,
                commit_lsn: 1,
                committed_log_offset: OffsetWatermark::Empty,
                created_at_ns: 1,
                summary: BTreeMap::new(),
            },
            DataFiles::new(file.into_iter().collect()).unwrap(),
        )
        .unwrap()
    }

    fn all_commands() -> Vec<CatalogCommandV1> {
        let table_id = TableId::from_u128(3).unwrap();
        let consumer_id = DurableConsumerId::from_u128(4).unwrap();
        vec![
            command(),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id,
                namespace_id: namespace_id(),
                name: CatalogName::new("ticks").unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: SchemaIpcPayload::new(vec![1, 2]).unwrap(),
                config: BTreeMap::new(),
            }),
            CatalogCommandV1::AlterTableSchema(AlterTableSchemaCommand {
                table_id,
                expected_schema_version: 1,
                schema_ipc: SchemaIpcPayload::new(vec![2, 3]).unwrap(),
            }),
            CatalogCommandV1::AcquireProducer(AcquireProducerCommand {
                table_id,
                producer_id: ProducerId::from_u128(5).unwrap(),
                expected_writer_epoch: Some(6),
            }),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id,
                expected_snapshot_version: 7,
                files: Vec::new(),
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(8)),
            }),
            CatalogCommandV1::CommitCompaction(CommitCompactionCommand {
                table_id,
                expected_snapshot_version: 9,
                input_file_ids: vec![FileId::from_u128(6).unwrap()],
                output_files: Vec::new(),
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(10)),
            }),
            CatalogCommandV1::CommitConsumerOffset(CommitConsumerOffsetCommand {
                consumer_id,
                table_id,
                offset_authority: ConsumerOffsetAuthority::Consumer(consumer_id),
                committed_log_offset: CommittedOffset::Known(LogOffset::new(11)),
            }),
            CatalogCommandV1::AcquireSnapshotLease(AcquireSnapshotLeaseCommand {
                lease_id: LeaseId::from_u128(7).unwrap(),
                table_id,
                snapshot_version: 12,
                owner: "query".to_owned(),
                expires_at_ns: 13,
            }),
            CatalogCommandV1::ReleaseSnapshotLease(ReleaseSnapshotLeaseCommand {
                lease_id: LeaseId::from_u128(7).unwrap(),
                owner: "query".to_owned(),
            }),
            CatalogCommandV1::CreateMaintenanceJob(CreateMaintenanceJobCommand {
                job_id: JobId::from_u128(8).unwrap(),
                kind: MaintenanceJobKind::Verify,
                table_id: Some(table_id),
                priority: 14,
                input: BTreeMap::new(),
            }),
            CatalogCommandV1::TransitionMaintenanceJob(TransitionMaintenanceJobCommand {
                job_id: JobId::from_u128(8).unwrap(),
                expected_state: MaintenanceJobState::Pending,
                next_state: MaintenanceJobState::Running,
                lease_owner: Some("worker".to_owned()),
                lease_expires_at_ns: Some(15),
                progress: BTreeMap::new(),
                error: None,
            }),
            CatalogCommandV1::DropTable(DropTableCommand {
                table_id,
                expected_snapshot_version: 16,
            }),
        ]
    }

    fn assert_catalog_is_object_safe(_: Option<&dyn Catalog>) {}
    fn assert_catalog_read_is_object_safe(_: Option<&dyn CatalogRead>) {}
    fn assert_catalog_file_read_is_object_safe(_: Option<&dyn CatalogFileRead>) {}
    fn assert_catalog_garbage_collection_is_object_safe(_: Option<&dyn CatalogGarbageCollection>) {}
    fn assert_catalog_idempotency_read_is_object_safe(_: Option<&dyn CatalogIdempotencyRead>) {}
    fn assert_catalog_publication_control_is_object_safe(
        _: Option<&dyn CatalogPublicationControl>,
    ) {
    }
    fn assert_catalog_persistence_is_object_safe(_: Option<&dyn CatalogPersistence>) {}
    fn assert_catalog_write_is_object_safe(_: Option<&dyn CatalogWrite>) {}

    #[test]
    fn catalog_traits_are_object_safe() {
        assert_catalog_is_object_safe(None);
        assert_catalog_read_is_object_safe(None);
        assert_catalog_file_read_is_object_safe(None);
        assert_catalog_garbage_collection_is_object_safe(None);
        assert_catalog_idempotency_read_is_object_safe(None);
        assert_catalog_publication_control_is_object_safe(None);
        assert_catalog_persistence_is_object_safe(None);
        assert_catalog_write_is_object_safe(None);
    }

    #[test]
    fn command_canonical_bytes_have_a_golden_form() {
        let input = CatalogCommandFingerprintInputV1 {
            format_version: CatalogCommandFormatVersion::current(),
            expected_revision: None,
            actor: actor(),
            command: command(),
        };
        let encoded = String::from_utf8(input.canonical_bytes().unwrap()).unwrap();

        assert_eq!(
            encoded,
            concat!(
                r#"{"format_version":1,"expected_revision":null,"#,
                r#""actor":{"kind":"user","principal":"alice"},"#,
                r#""command":{"type":"create_namespace","command":{"#,
                r#""namespace_id":"00000000-0000-0000-0000-000000000001","#,
                r#""name":"market"}}}"#
            )
        );
    }

    #[test]
    fn envelope_detects_command_reuse_with_a_different_body() {
        let mut envelope =
            CatalogCommandEnvelopeV1::new(request_id(), None, actor(), command()).unwrap();
        assert!(envelope.verify_fingerprint().is_ok());

        envelope.command = CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: namespace_id(),
            name: CatalogName::new("changed").unwrap(),
        });
        assert!(envelope.verify_fingerprint().is_err());
    }

    #[test]
    fn command_fingerprint_has_a_golden_sha256_value() {
        let envelope =
            CatalogCommandEnvelopeV1::new(request_id(), None, actor(), command()).unwrap();

        assert_eq!(
            envelope.canonical_fingerprint.to_string(),
            "d148e59fcc48fc11c53e25dd64683db613ea64c2562905dab4214828d982c5e0"
        );
    }

    #[test]
    fn borrowed_manifest_fingerprint_wire_matches_owned_v1_wire() {
        let mut manifest_commands = 0;
        for command in nested_wire_commands() {
            if !matches!(
                &command,
                CatalogCommandV1::CommitDataFiles(_) | CatalogCommandV1::CommitCompaction(_)
            ) {
                continue;
            }
            manifest_commands += 1;
            let input = CatalogCommandFingerprintInputV1 {
                format_version: CatalogCommandFormatVersion::v1(),
                expected_revision: Some(17),
                actor: actor(),
                command,
            };
            let owned = serde_json::to_vec(&wire_v1::FingerprintInputV1::from(&input)).unwrap();
            let borrowed = wire_v1::ManifestFingerprintInputRefV1::from_parts(
                input.format_version,
                input.expected_revision,
                &input.actor,
                &input.command,
            )
            .unwrap();

            assert_eq!(serde_json::to_vec(&borrowed).unwrap(), owned);
        }
        assert_eq!(manifest_commands, 2);
    }

    #[test]
    fn actor_principal_is_bounded_before_canonical_fingerprint_encoding() {
        let at_limit = CatalogCommandFingerprintInputV1 {
            format_version: CatalogCommandFormatVersion::v1(),
            expected_revision: None,
            actor: CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "p".repeat(MAX_CATALOG_ACTOR_PRINCIPAL_BYTES),
            },
            command: command(),
        };
        assert!(at_limit.fingerprint().is_ok());

        let oversized = CatalogCommandFingerprintInputV1 {
            format_version: CatalogCommandFormatVersion::v1(),
            expected_revision: None,
            actor: CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "p".repeat(MAX_CATALOG_ACTOR_PRINCIPAL_BYTES + 1),
            },
            command: command(),
        };
        assert!(matches!(
            oversized.canonical_bytes(),
            Err(CatalogFingerprintError::ActorPrincipalTooLarge { .. })
        ));
        assert!(matches!(
            oversized.fingerprint(),
            Err(CatalogFingerprintError::ActorPrincipalTooLarge { .. })
        ));
    }

    #[test]
    fn unsupported_command_format_cannot_be_fingerprinted() {
        let input = CatalogCommandFingerprintInputV1 {
            format_version: CatalogCommandFormatVersion::new(99),
            expected_revision: None,
            actor: actor(),
            command: command(),
        };
        assert!(input.fingerprint().is_err());
    }

    #[test]
    fn v1_codec_validates_historical_version_and_fingerprint() {
        let command =
            CatalogCommandEnvelopeV1::new(request_id(), None, actor(), command()).unwrap();
        let encoded = CatalogCodec::encode_command(&command.clone().into()).unwrap();

        assert_eq!(
            CatalogCodec::decode_command(&encoded).unwrap(),
            VersionedCatalogCommand::V1(command)
        );
        let unknown = String::from_utf8(encoded.clone()).unwrap().replacen(
            r#""format_version":1"#,
            r#""format_version":99"#,
            1,
        );
        assert!(CatalogCodec::decode_command(unknown.as_bytes()).is_err());
        let tampered = String::from_utf8(encoded)
            .unwrap()
            .replace(r#""name":"market""#, r#""name":"changed""#);
        assert!(CatalogCodec::decode_command(tampered.as_bytes()).is_err());
    }

    #[test]
    fn every_v1_command_has_frozen_canonical_snapshot_and_roundtrip() {
        let mut hashes = Vec::new();
        let mut variants = Vec::new();
        for (index, command) in all_commands().into_iter().enumerate() {
            let input = CatalogCommandFingerprintInputV1 {
                format_version: CatalogCommandFormatVersion::v1(),
                expected_revision: Some(17),
                actor: actor(),
                command: command.clone(),
            };
            let canonical = input.canonical_bytes().unwrap();
            hashes.push(format!("{:x}", Sha256::digest(&canonical)));
            variants.push(
                serde_json::from_slice::<serde_json::Value>(&canonical).unwrap()["command"]["type"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            let envelope = CatalogCommandEnvelopeV1::new(
                RequestId::from_u128(100 + index as u128).unwrap(),
                Some(17),
                actor(),
                command,
            )
            .unwrap();
            let encoded = CatalogCodec::encode_command(&envelope.clone().into()).unwrap();
            assert_eq!(
                CatalogCodec::decode_command(&encoded).unwrap(),
                VersionedCatalogCommand::V1(envelope)
            );
        }

        assert_eq!(
            variants,
            [
                "create_namespace",
                "create_table",
                "alter_table_schema",
                "acquire_producer",
                "commit_data_files",
                "commit_compaction",
                "commit_consumer_offset",
                "acquire_snapshot_lease",
                "release_snapshot_lease",
                "create_maintenance_job",
                "transition_maintenance_job",
                "drop_table",
            ]
        );
        assert_eq!(
            hashes,
            [
                "35a2b83e612445649a627e2236de7ef60d3fa3ab99b9d9b500c6c2d67c57c963",
                "7e8935d17fdd1e6468657bd2ab7b9bf049a6348ec803fc093df2b7fff45cd774",
                "e9e4ff8de0c4bea5a255cc5222cb47998da3d86c59d093d6b140ec075f39dec6",
                "5ad5952d5efb68928821c92fbd21e8f0bc0ccbe528cd3dd4a3b3ee9dfcc3717a",
                "75521f08438c3c3bfbf60872fe235ff52246cad22e74949d29e35ef16b7c9f88",
                "962e0591c8a3e0eb9d807e836e071997147aaf4a566a7914a34abca536b49e11",
                "c9812e72707c884b47ce5f78e58b1b5a2657c53b7cca7ce0dae1a85aef516bb8",
                "1e55d05dad427bd67c6c14f01058dc482ea444fc460bf413b9f12026f1a99a85",
                "97d57dd56366772dcd22c0ad4ca37b5cbac67b2cd2c5d5b00dca8d9f1241003c",
                "cec7177e99e73ea7422698e003f6ae88e0c4ae380658f83fb7749b1d391bb607",
                "a0d54a7476b498fdddde648c88976c2c4076b013135dbf2cae36cc3e09615c29",
                "6d8fbc714a1d40ad6725cc2e073a8fa5d9fa389c98cdd98db87623ee570ab521",
            ]
        );
    }

    #[test]
    fn nested_v1_command_branches_have_frozen_snapshots_and_roundtrip() {
        let mut hashes = Vec::new();
        for (index, command) in nested_wire_commands().into_iter().enumerate() {
            let input = CatalogCommandFingerprintInputV1 {
                format_version: CatalogCommandFormatVersion::v1(),
                expected_revision: Some(18),
                actor: actor(),
                command: command.clone(),
            };
            hashes.push(format!(
                "{:x}",
                Sha256::digest(input.canonical_bytes().unwrap())
            ));
            let envelope = CatalogCommandEnvelopeV1::new(
                RequestId::from_u128(200 + index as u128).unwrap(),
                Some(18),
                actor(),
                command,
            )
            .unwrap();
            let encoded = CatalogCodec::encode_command(&envelope.clone().into()).unwrap();
            assert_eq!(
                CatalogCodec::decode_command(&encoded).unwrap(),
                VersionedCatalogCommand::V1(envelope)
            );
        }

        assert_eq!(
            hashes,
            [
                "e328dd91e5f24d53b07ad411789e4041e341a73964f988f140d05183037a3485",
                "94f1363738163f00ce289bd813b2ea3a5b7742f67b31279d978e8e72aea6c8a6",
                "5f0ed204ebb7dcc366beb4d14808067202266dd8ae204fd41f0cf1b580e8d5e8",
                "6f8d6624a7c118ec1d524c8a28758b146d72fa6c7017bb30720390182f2a2f47",
            ]
        );
    }

    #[test]
    fn v1_command_and_response_mirrors_reject_top_and_deep_unknown_fields() {
        let envelope =
            CatalogCommandEnvelopeV1::new(request_id(), None, actor(), command()).unwrap();
        let mut top_command = serde_json::from_slice::<serde_json::Value>(
            &CatalogCodec::encode_command(&envelope.into()).unwrap(),
        )
        .unwrap();
        top_command["unsigned"] = serde_json::json!(true);
        assert!(CatalogCodec::decode_command(&serde_json::to_vec(&top_command).unwrap()).is_err());

        let nested = nested_wire_commands().remove(2);
        let envelope = CatalogCommandEnvelopeV1::new(request_id(), None, actor(), nested).unwrap();
        let mut deep_command = serde_json::from_slice::<serde_json::Value>(
            &CatalogCodec::encode_command(&envelope.into()).unwrap(),
        )
        .unwrap();
        deep_command["command"]["command"]["files"][0]["provenance"]["source_lineage"][0]
            ["source"]["unsigned"] = serde_json::json!(true);
        assert!(CatalogCodec::decode_command(&serde_json::to_vec(&deep_command).unwrap()).is_err());

        let response = CatalogResponseV1 {
            request_id: request_id(),
            catalog_revision: 7,
            outcome: all_response_outcomes().remove(1),
        };
        let encoded = CatalogCodec::encode_response(&response.into()).unwrap();
        let mut top_response = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
        top_response["unsigned"] = serde_json::json!(true);
        assert!(
            CatalogCodec::decode_response(&serde_json::to_vec(&top_response).unwrap()).is_err()
        );

        let mut deep_response = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
        deep_response["response"]["outcome"]["value"]["definition"]["definition"]["unsigned"] =
            serde_json::json!(true);
        assert!(
            CatalogCodec::decode_response(&serde_json::to_vec(&deep_response).unwrap()).is_err()
        );
    }

    #[test]
    fn every_v1_response_outcome_has_a_frozen_snapshot_and_roundtrip() {
        let mut hashes = Vec::new();
        let mut variants = Vec::new();
        for (index, outcome) in all_response_outcomes().into_iter().enumerate() {
            let response = CatalogResponseV1 {
                request_id: RequestId::from_u128(300 + index as u128).unwrap(),
                catalog_revision: 20 + index as u64,
                outcome,
            };
            let encoded = CatalogCodec::encode_response(&response.clone().into()).unwrap();
            let wire = serde_json::from_slice::<serde_json::Value>(&encoded).unwrap();
            variants.push(
                wire["response"]["outcome"]["type"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            hashes.push(format!("{:x}", Sha256::digest(&encoded)));
            assert_eq!(
                CatalogCodec::decode_response(&encoded).unwrap(),
                VersionedCatalogResponse::V1(response)
            );
        }

        assert_eq!(
            variants,
            [
                "namespace",
                "table",
                "schema",
                "producer",
                "snapshot",
                "consumer_offset",
                "snapshot_lease",
                "snapshot_lease_released",
                "maintenance_job",
                "table_dropped",
            ]
        );
        assert_eq!(
            hashes,
            [
                "084774b71cfc1d68e116ad42d110126d7abcd1dc77f7a2327fad91d8b2cfb292",
                "bee0227b6d92b13395f93ace32a9e5b5d8c7b3ac06da6f1353a92ba78ff09a0e",
                "0fafa64d555bc4214fd48d58b7843cf4c8f7e1f8ac8b1aa03c5c40cfa5fcea6c",
                "6dd105ac7e0ff1cb35ef0c7b4a3cbd89e49cb5171e05d1882f521c5949657fe8",
                "f339a6cfba0a555a3915c67101b9669977949e8d45947b3d7867d86578432b12",
                "a08ce8e72a8843b59a59d327cb42a26fd013fad0e064834f5cffedff0c8ed069",
                "a89810f745928858549589879e00919b1e346567178a975dea632079c5411222",
                "8fb18e80f86394abaa6af1daabdcf9d8ea54aa23a4ee83e8089004366c109a8d",
                "1cac029293886a132dbc1b28147498675d1ba73f1c54af6668208ea87ccde0b9",
                "93c1be881059aeb5316ef2cf42a7f768693a59335cf46d81952f23cf06bfa2ec",
            ]
        );
    }

    #[test]
    fn v1_response_decode_revalidates_nested_domain_values() {
        let response = CatalogResponseV1 {
            request_id: request_id(),
            catalog_revision: 7,
            outcome: all_response_outcomes().remove(2),
        };
        let mut encoded = serde_json::from_slice::<serde_json::Value>(
            &CatalogCodec::encode_response(&response.into()).unwrap(),
        )
        .unwrap();
        encoded["response"]["outcome"]["value"]["schema_ipc"] = serde_json::json!([]);
        assert!(matches!(
            CatalogCodec::decode_response(&serde_json::to_vec(&encoded).unwrap()),
            Err(CatalogCodecError::InvalidV1(_))
        ));
    }

    #[test]
    fn v1_response_codec_and_empty_query_snapshot_are_explicit() {
        let response = CatalogResponseV1 {
            request_id: request_id(),
            catalog_revision: 7,
            outcome: CatalogOutcomeV1::TableDropped {
                table_id: TableId::from_u128(3).unwrap(),
            },
        };
        let encoded = CatalogCodec::encode_response(&response.clone().into()).unwrap();

        assert_eq!(
            CatalogCodec::decode_response(&encoded).unwrap(),
            VersionedCatalogResponse::V1(response)
        );
        let unknown = String::from_utf8(encoded).unwrap().replacen(
            r#""format_version":1"#,
            r#""format_version":99"#,
            1,
        );
        assert!(CatalogCodec::decode_response(unknown.as_bytes()).is_err());
        assert!(matches!(
            CatalogCodec::decode_response(br#"{"format_version":99,"response":"not-v1"}"#),
            Err(CatalogCodecError::UnsupportedVersion { actual: 99 })
        ));
        let snapshot = CatalogQuerySnapshot::empty(7);
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.catalog_revision(), 7);
    }

    #[test]
    fn catalog_snapshot_aggregates_reject_mismatch_visibility_and_order_bypass() {
        let table_id = TableId::from_u128(20).unwrap();
        let visible = file_descriptor(1, table_id, 2, None);
        let valid = table_snapshot(20, Some(visible.clone()));
        assert_eq!(valid.selector().snapshot_version, 3);
        assert_eq!(valid.files().len(), 1);

        let mut mismatched_json = serde_json::to_value(&valid).unwrap();
        mismatched_json["schema"]["schema_version"] = serde_json::json!(99);
        assert!(serde_json::from_value::<CatalogTableSnapshot>(mismatched_json).is_err());

        let mut future_schema_json = serde_json::to_value(&valid).unwrap();
        future_schema_json["selector"]["schema_version"] = serde_json::json!(5);
        future_schema_json["schema"]["schema_version"] = serde_json::json!(5);
        assert!(serde_json::from_value::<CatalogTableSnapshot>(future_schema_json).is_err());

        let mut future_snapshot_json = serde_json::to_value(&valid).unwrap();
        future_snapshot_json["selector"]["snapshot_version"] = serde_json::json!(6);
        future_snapshot_json["snapshot"]["snapshot_version"] = serde_json::json!(6);
        assert!(serde_json::from_value::<CatalogTableSnapshot>(future_snapshot_json).is_err());

        let mut newer_file_schema_json = serde_json::to_value(&valid).unwrap();
        newer_file_schema_json["files"][0]["file"]["schema_version"] = serde_json::json!(3);
        assert!(serde_json::from_value::<CatalogTableSnapshot>(newer_file_schema_json).is_err());

        let invisible = file_descriptor(1, table_id, 4, None);
        let mut invisible_json = serde_json::to_value(&valid).unwrap();
        invisible_json["files"] = serde_json::to_value([invisible]).unwrap();
        assert!(serde_json::from_value::<CatalogTableSnapshot>(invisible_json).is_err());

        let mut cross_table_json = serde_json::to_value(&valid).unwrap();
        cross_table_json["files"][0]["file"]["table_id"] =
            serde_json::json!(TableId::from_u128(21).unwrap());
        assert!(serde_json::from_value::<CatalogTableSnapshot>(cross_table_json).is_err());

        let first = file_descriptor(1, table_id, 1, None);
        let second = file_descriptor(2, table_id, 1, None);
        assert!(DataFiles::new(vec![second.clone(), first.clone()]).is_err());
        assert!(DataFiles::new(vec![first.clone(), first]).is_err());
        assert!(serde_json::from_value::<DataFiles>(
            serde_json::to_value([second, file_descriptor(1, table_id, 1, None)]).unwrap()
        )
        .is_err());

        let low = table_snapshot(20, None);
        let high = table_snapshot(21, None);
        assert!(CatalogQuerySnapshot::new(7, vec![high.clone(), low.clone()]).is_err());
        assert!(CatalogQuerySnapshot::new(7, vec![low.clone(), low]).is_err());
        let duplicate_json = serde_json::json!({
            "catalog_revision": 7,
            "tables": [high.clone(), high],
        });
        assert!(serde_json::from_value::<CatalogQuerySnapshot>(duplicate_json).is_err());
    }

    #[test]
    fn historical_snapshot_visibility_respects_removal_boundary_and_file_state() {
        let table_id = TableId::from_u128(30).unwrap();
        let committed = file_descriptor(1, table_id, 2, None);
        assert!(committed.validate().is_ok());
        assert!(committed.is_visible_at(3));

        let historical = file_descriptor(1, table_id, 2, Some(4));
        assert!(historical.validate().is_ok());
        assert!(historical.is_visible_at(3));
        assert!(!historical.is_visible_at(4));
        assert_eq!(
            serde_json::from_value::<DataFileDescriptor>(
                serde_json::to_value(&historical).unwrap()
            )
            .unwrap(),
            historical
        );
        assert!(DataFiles::new(vec![historical.clone()]).is_ok());

        let old_snapshot = table_snapshot(30, Some(historical.clone()));
        assert_eq!(old_snapshot.selector().snapshot_version, 3);
        assert_eq!(old_snapshot.files().as_slice(), [historical.clone()]);

        let mut removal_snapshot = serde_json::to_value(&old_snapshot).unwrap();
        removal_snapshot["selector"]["snapshot_version"] = serde_json::json!(4);
        removal_snapshot["snapshot"]["snapshot_version"] = serde_json::json!(4);
        assert!(serde_json::from_value::<CatalogTableSnapshot>(removal_snapshot).is_err());

        for state in [DataFileState::Deleting, DataFileState::Deleted] {
            let mut removed = historical.clone();
            removed.state = state;
            assert!(removed.validate().is_ok());
            for snapshot_version in [0, 2, 3, 4, u64::MAX] {
                assert!(!removed.is_visible_at(snapshot_version));
            }
            assert_eq!(
                serde_json::from_value::<DataFileDescriptor>(
                    serde_json::to_value(&removed).unwrap()
                )
                .unwrap(),
                removed
            );
            assert!(DataFiles::new(vec![removed.clone()]).is_err());
            assert!(
                serde_json::from_value::<DataFiles>(serde_json::to_value([removed]).unwrap())
                    .is_err()
            );

            let mut missing_removal = committed.clone();
            missing_removal.state = state;
            assert!(missing_removal.validate().is_err());
        }

        let mut invalid_interval = committed;
        invalid_interval.removed_snapshot_version = Some(2);
        assert!(invalid_interval.validate().is_err());
    }

    #[test]
    fn maintenance_state_machine_rejects_terminal_reentry() {
        assert!(MaintenanceJobState::Pending.can_transition_to(MaintenanceJobState::Running));
        assert!(MaintenanceJobState::Running.can_transition_to(MaintenanceJobState::Failed));
        assert!(MaintenanceJobState::Failed.can_transition_to(MaintenanceJobState::Pending));
        assert!(!MaintenanceJobState::Succeeded.can_transition_to(MaintenanceJobState::Running));
    }
}
