//! Idempotent V1 metadata and V2 physical import from an atomic legacy registry snapshot.
//!
//! Frozen V1 Catalog commands remain an immutable namespace/empty-table prefix. Eligible nonempty
//! tables are appended as typed V2 operations: every file is physically verified in bounded
//! sequence before one atomic table import. Tables without a declared or explicitly trusted root,
//! and empty tables whose historical schema IPC is not deterministic, remain absent from SQLite.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zippy_catalog::{
    canonical_schema_hash_from_ipc, decode_persisted_schema, SqliteCatalog,
    SqliteCatalogLegacyImportSession,
};
use zippy_master::legacy_catalog::{
    LegacyCatalogMigrationSnapshot, LegacyCatalogMigrationTable, LegacyMigrationFile,
};
use zippy_storage::{verify_legacy_parquet_import, LegacyParquetImportSpec};
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
    CatalogCommandV1, CatalogIdempotencyRead, CatalogOutcomeV1, CatalogRead, CatalogWrite,
    CreateNamespaceCommand, CreateTableCommand, LegacyCatalogImportCommandV2,
    LegacyCatalogImportEnvelopeV2, LegacyCatalogImportFileV2, LegacyCatalogImportReplayProbeV2,
    LegacyCatalogImportResponseV2, VersionedCatalogCommand, VersionedCatalogResponse,
    LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY, LEGACY_REGISTRY_INDEX_METADATA_KEY,
};
use zippy_types::table::{
    CatalogName, ExternalDataPath, MetadataMap, MetadataScalar, SchemaCompatibility,
    SchemaDescriptor, SnapshotOperation, TableState,
};
use zippy_types::{ErrorCode, OffsetWatermark, RequestId, Result, ServerError, TableId};

/// Version of the metadata-only legacy Catalog import protocol.
pub const LEGACY_CATALOG_METADATA_MIGRATION_VERSION: u32 = 1;

/// Version of the typed physical legacy Catalog import protocol.
pub const LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION: u32 = 2;

/// SQLite Catalog revision required before the first import command.
pub const LEGACY_CATALOG_TARGET_REVISION_BASE: u64 = 0;

/// Initial empty snapshot assigned by frozen V1 `CreateTable`.
pub const LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE: u64 = 1;

/// Final import snapshot created atomically by a nonempty V2 table import.
pub const LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE: u64 = 2;

/// Table config key retaining the legacy stream identity.
pub const LEGACY_STREAM_NAME_CONFIG_KEY: &str = "zippy.migration.legacy_stream_name";

/// Table config key retaining the first exactly mapped legacy file-view revision.
pub const LEGACY_REVISION_BASE_CONFIG_KEY: &str =
    "zippy.migration.legacy_persisted_files_revision_base";

/// Table config key retaining the corresponding native snapshot version.
pub const NATIVE_SNAPSHOT_BASE_CONFIG_KEY: &str = "zippy.migration.native_snapshot_version_base";

/// Table config key retaining the normalized legacy source snapshot hash.
pub const LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY: &str = "zippy.migration.source_snapshot_hash";

/// Table config key retaining the importer protocol version.
pub const LEGACY_MIGRATION_VERSION_CONFIG_KEY: &str = "zippy.migration.version";

const MIGRATION_ACTOR_PRINCIPAL: &str = "zippy-server/legacy-catalog-metadata-v1";
const REQUEST_ID_DOMAIN: &[u8] = b"zippy.legacy_catalog.metadata_import.request.v1";
const MIGRATION_V2_ACTOR_PRINCIPAL: &str = "zippy-server/legacy-catalog-physical-v2";
const REQUEST_ID_V2_DOMAIN: &[u8] = b"zippy.legacy_catalog.physical_import.request.v2";
const MAX_LEGACY_FILES_PER_TABLE: usize = 4_096;

/// A capability required before a blocked table can be imported without weakening V1 invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogV2ImportRequirement {
    /// Represent an absolute read-only file outside the managed data root.
    ExternalLegacyLocation,
    /// Represent legacy provenance without inventing native source lineage.
    LegacyProvenance,
    /// Represent a snapshot whose durable logical offset is not provable.
    LegacyUnknownWatermark,
    /// Preserve the effective legacy Gateway scan order in the query read DTO.
    PreserveDiscoveryOrder,
    /// Derive and persist a collision-checked file identity stable across retries.
    StableFileId,
    /// Validate the original schema and retain its schema-only Arrow IPC bytes.
    SchemaIpc,
    /// Atomically map the captured legacy revision to the committed native snapshot.
    CompleteSnapshotRevisionMapping,
    /// Canonicalize custom Arrow metadata so regenerated command bytes are deterministic.
    DeterministicSchemaIpc,
    /// Verify size, row count, checksum, Parquet footer, and schema identity before visibility.
    VerifiedPhysicalFileMetadata,
    /// Resolve an explicitly trusted canonical data root for legacy entries that omitted one.
    TrustedDataRoot,
    /// Commit every preceding blocked V2 table before assigning this table a stable revision.
    CommittedImportPrefix,
}

/// Dry-run disposition for one legacy table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogTablePlanStatus {
    /// Frozen V1 can create an exactly mapped empty table.
    EligibleMetadataV1,
    /// Typed V2 can verify and atomically import this nonempty table after the V1 prefix.
    EligiblePhysicalV2,
    /// The table must remain absent because the current safe V1/V2 paths cannot represent it.
    BlockedRequiresV2,
}

/// One table entry in an immutable metadata import plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogTablePlan {
    /// Stable table identity derived by the legacy adapter.
    pub table_id: TableId,
    /// Stable table name in the default namespace.
    pub table_name: CatalogName,
    /// Writer epoch observed in the same atomic legacy snapshot.
    pub legacy_writer_epoch: u64,
    /// Complete persisted-file view revision observed atomically.
    pub legacy_persisted_files_revision: u64,
    /// Number of raw legacy files retained in discovery order.
    pub legacy_file_count: usize,
    /// Whether frozen V1, typed V2, or neither path can import this table safely.
    pub status: LegacyCatalogTablePlanStatus,
    /// First mapped legacy revision, present for every eligible V1 or V2 table.
    pub legacy_revision_base: Option<u64>,
    /// Corresponding native snapshot, present for every eligible V1 or V2 table.
    pub native_snapshot_base: Option<u64>,
    /// Deterministic V1 command or V2 import identity, absent for blocked tables.
    pub request_id: Option<RequestId>,
    /// Explicit capabilities required before a blocked table may become visible in SQLite.
    pub v2_requirements: Vec<LegacyCatalogV2ImportRequirement>,
    command_index: Option<usize>,
    v2_import_index: Option<usize>,
}

/// Immutable dry-run output containing a frozen V1 prefix and appended V2 table imports.
#[derive(Debug, Clone)]
pub struct LegacyCatalogMetadataImportPlan {
    migration_version: u32,
    source_snapshot_hash: String,
    source_catalog_revision: u64,
    commands: Vec<PlannedCommand>,
    v2_imports: Vec<PlannedLegacyTableImportV2>,
    tables: Vec<LegacyCatalogTablePlan>,
}

impl LegacyCatalogMetadataImportPlan {
    /// Returns the highest importer protocol version used by this plan.
    #[must_use]
    pub const fn migration_version(&self) -> u32 {
        self.migration_version
    }

    /// Returns the normalized legacy source snapshot hash.
    #[must_use]
    pub fn source_snapshot_hash(&self) -> &str {
        &self.source_snapshot_hash
    }

    /// Returns the source Catalog revision captured with every table.
    #[must_use]
    pub const fn source_catalog_revision(&self) -> u64 {
        self.source_catalog_revision
    }

    /// Returns table decisions in stable table-id order.
    #[must_use]
    pub fn tables(&self) -> &[LegacyCatalogTablePlan] {
        &self.tables
    }

    /// Returns the exact number of idempotent V1 commands in the plan.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    /// Returns the number of typed V2 table imports appended after the complete V1 prefix.
    #[must_use]
    pub fn v2_import_count(&self) -> usize {
        self.v2_imports.len()
    }

    /// Plans the same frozen V1 prefix plus eligible V2 imports using an explicit trusted root.
    ///
    /// A file-level `persist_data_root` remains authoritative. The supplied root is used only for
    /// old entries that omitted that field, and every such file must be a strict lexical
    /// descendant. The root is not an authorization token; startup configuration must establish
    /// its trust before calling this constructor.
    pub fn with_trusted_fallback_root(
        snapshot: &LegacyCatalogMigrationSnapshot,
        trusted_fallback_root: &Path,
    ) -> Result<Self> {
        plan_legacy_catalog_import(snapshot, Some(trusted_fallback_root))
    }

    /// Returns whether every source table is representable by this V1 metadata import.
    #[must_use]
    pub fn is_complete_for_v1_metadata(&self) -> bool {
        self.tables
            .iter()
            .all(|table| table.status == LegacyCatalogTablePlanStatus::EligibleMetadataV1)
    }
}

#[derive(Debug, Clone)]
struct PlannedLegacyTableImportV2 {
    table_plan_index: usize,
    request_id: RequestId,
    expected_revision: u64,
    command: LegacyCatalogImportCommandV2,
    file_specs: Vec<LegacyParquetImportSpec>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedLegacyCatalogTableView {
    pub(crate) canonical_schema_hash: String,
    pub(crate) files: Vec<LegacyCatalogImportFileV2>,
    pub(crate) cache: BTreeMap<String, VerifiedLegacyCatalogFileCacheEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedLegacyCatalogFileCacheEntry {
    source_metadata: serde_json::Value,
    canonical_schema_hash: String,
    verification_root: Option<PathBuf>,
    file: LegacyCatalogImportFileV2,
}

#[derive(Debug, Clone)]
struct PlannedCommand {
    command: VersionedCatalogCommand,
    kind: PlannedCommandKind,
}

#[derive(Debug, Clone, Copy)]
enum PlannedCommandKind {
    Namespace,
    Table { table_plan_index: usize },
}

/// Durable command disposition observed while executing an import plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogCommandDisposition {
    /// The exact command already had a committed idempotency response.
    Replayed,
    /// The executor submitted the previously missing command successfully.
    Submitted,
}

/// Final outcome for one table in a migration report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogTableMigrationOutcome {
    /// Metadata was submitted and committed during this execution.
    MetadataSubmitted,
    /// Metadata had already committed under the exact deterministic command.
    MetadataReplayed,
    /// Verified physical files and their table were atomically imported during this execution.
    PhysicalSubmitted,
    /// The exact verified V2 table import already had a durable idempotency response.
    PhysicalReplayed,
    /// No SQLite table was created because the current safe migration paths cannot represent it.
    BlockedRequiresV2,
}

/// Observable execution result for one legacy table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogTableMigrationReport {
    /// Stable table identity.
    pub table_id: TableId,
    /// Stable table name.
    pub table_name: CatalogName,
    /// Legacy persisted-file view revision.
    pub legacy_persisted_files_revision: u64,
    /// Raw legacy file count.
    pub legacy_file_count: usize,
    /// Final metadata or physical import outcome.
    pub outcome: LegacyCatalogTableMigrationOutcome,
    /// Exact legacy revision base written to table config, when imported.
    pub legacy_revision_base: Option<u64>,
    /// Exact native snapshot base written to table config, when imported.
    pub native_snapshot_base: Option<u64>,
    /// Capabilities still required for a blocked table.
    pub v2_requirements: Vec<LegacyCatalogV2ImportRequirement>,
}

/// Auditable result of one complete V1/V2 import attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogMetadataMigrationReport {
    /// Import protocol version.
    pub migration_version: u32,
    /// Hash of the immutable normalized source view.
    pub source_snapshot_hash: String,
    /// Legacy Catalog revision captured atomically.
    pub source_catalog_revision: u64,
    /// SQLite Catalog revision before probing or submitting commands.
    pub target_revision_before: u64,
    /// SQLite Catalog revision after the complete successful attempt.
    pub target_revision_after: u64,
    /// Namespace command disposition.
    pub namespace: LegacyCatalogCommandDisposition,
    /// Number of exact idempotency responses reused.
    pub replayed_commands: usize,
    /// Number of previously missing commands submitted successfully.
    pub submitted_commands: usize,
    /// Per-table outcomes in stable table-id order.
    pub tables: Vec<LegacyCatalogTableMigrationReport>,
}

/// Stable lifecycle state of the startup legacy-to-SQLite Catalog migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogMigrationState {
    /// The effective configuration does not request shadow migration.
    Disabled,
    /// Every source table is represented by a completed V1 or V2 import.
    Complete,
    /// Legacy remains authoritative while the shadow migration requires operator attention.
    Degraded,
}

/// Machine-readable reason why shadow migration is degraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogMigrationDegradedReason {
    /// The atomic legacy registry snapshot could not be captured.
    CaptureFailed,
    /// The detached legacy snapshot could not produce a deterministic import plan.
    PlanningFailed,
    /// SQLite rejected or failed an idempotent import attempt.
    ExecutionFailed,
    /// At least one legacy table cannot enter the current typed V2 physical-file import path.
    BlockedRequiresV2,
}

/// Structured details retained for one degraded shadow migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogMigrationDegradation {
    /// Stable degradation category.
    pub reason: LegacyCatalogMigrationDegradedReason,
    /// Stable server error code when degradation came from a failed operation.
    pub error_code: Option<ErrorCode>,
    /// Whether retrying the failed operation may succeed without configuration changes.
    pub retryable: bool,
    /// Human-readable diagnostics for status surfaces and logs.
    pub message: Option<String>,
}

impl LegacyCatalogMigrationDegradation {
    pub(crate) fn from_error(
        reason: LegacyCatalogMigrationDegradedReason,
        error: &ServerError,
    ) -> Self {
        Self {
            reason,
            error_code: Some(error.code()),
            retryable: error.retryable(),
            message: Some(sanitized_migration_error(error)),
        }
    }

    fn blocked_requires_v2() -> Self {
        Self {
            reason: LegacyCatalogMigrationDegradedReason::BlockedRequiresV2,
            error_code: None,
            retryable: false,
            message: None,
        }
    }
}

fn sanitized_migration_error(error: &ServerError) -> String {
    format!(
        "legacy Catalog migration failed code=[{}] request_id=[{}] table_id=[{}]",
        error.code(),
        error
            .request_id()
            .map_or_else(|| "NONE".to_owned(), |value| value.to_string()),
        error
            .table_id()
            .map_or_else(|| "NONE".to_owned(), |value| value.to_string())
    )
}

/// Observable startup result for the legacy-to-SQLite Catalog shadow migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyCatalogMigrationStatus {
    /// Stable lifecycle state.
    pub state: LegacyCatalogMigrationState,
    /// Auditable import report when SQLite execution completed.
    pub report: Option<LegacyCatalogMetadataMigrationReport>,
    /// Structured diagnostics when the migration is degraded.
    pub degradation: Option<LegacyCatalogMigrationDegradation>,
}

impl LegacyCatalogMigrationStatus {
    /// Returns the status used when shadow migration is not configured.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            state: LegacyCatalogMigrationState::Disabled,
            report: None,
            degradation: None,
        }
    }

    pub(crate) fn degraded_before_execution(
        degradation: LegacyCatalogMigrationDegradation,
    ) -> Self {
        Self {
            state: LegacyCatalogMigrationState::Degraded,
            report: None,
            degradation: Some(degradation),
        }
    }

    pub(crate) fn from_report(report: LegacyCatalogMetadataMigrationReport) -> Self {
        let degradation = (report.blocked_table_count() > 0)
            .then(LegacyCatalogMigrationDegradation::blocked_requires_v2);
        Self {
            state: if degradation.is_some() {
                LegacyCatalogMigrationState::Degraded
            } else {
                LegacyCatalogMigrationState::Complete
            },
            report: Some(report),
            degradation,
        }
    }

    /// Returns a bounded, label-safe metrics snapshot without the diagnostic error message.
    #[must_use]
    pub fn metrics(&self) -> serde_json::Value {
        let report = self.report.as_ref();
        let attempted = report.is_some()
            || self.degradation.as_ref().is_some_and(|degradation| {
                degradation.reason == LegacyCatalogMigrationDegradedReason::ExecutionFailed
            });
        serde_json::json!({
            "configured": self.state != LegacyCatalogMigrationState::Disabled,
            "state": match self.state {
                LegacyCatalogMigrationState::Disabled => "disabled",
                LegacyCatalogMigrationState::Complete => "complete",
                LegacyCatalogMigrationState::Degraded => "degraded",
            },
            "attempts": usize::from(attempted),
            "completed_attempts": usize::from(report.is_some()),
            "degraded": usize::from(self.state == LegacyCatalogMigrationState::Degraded),
            "source_catalog_revision": report.map(|report| report.source_catalog_revision),
            "target_revision_after": report.map(|report| report.target_revision_after),
            "submitted_commands": report.map_or(0, |report| report.submitted_commands),
            "replayed_commands": report.map_or(0, |report| report.replayed_commands),
            "blocked_tables": report.map_or(0, LegacyCatalogMetadataMigrationReport::blocked_table_count),
            "degraded_reason": self.degradation.as_ref().map(|degradation| degradation.reason),
            "error_code": self
                .degradation
                .as_ref()
                .and_then(|degradation| degradation.error_code),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum LegacyCatalogMigrationStartup {
    Disabled,
    Planned(LegacyCatalogMetadataImportPlan),
    Degraded(LegacyCatalogMigrationDegradation),
}

impl LegacyCatalogMetadataMigrationReport {
    /// Returns the number of tables intentionally left absent for V2.
    #[must_use]
    pub fn blocked_table_count(&self) -> usize {
        self.tables
            .iter()
            .filter(|table| table.outcome == LegacyCatalogTableMigrationOutcome::BlockedRequiresV2)
            .count()
    }
}

/// Builds a deterministic V1 metadata prefix and eligible V2 imports from one detached snapshot.
///
/// The plan always starts at SQLite Catalog revision zero. Every request ID, expected revision,
/// command fingerprint, and table order is frozen by the source snapshot. A changed source must
/// produce a new migration attempt rather than silently extending a partially imported snapshot.
pub fn plan_legacy_catalog_metadata_import(
    snapshot: &LegacyCatalogMigrationSnapshot,
) -> Result<LegacyCatalogMetadataImportPlan> {
    plan_legacy_catalog_import(snapshot, None)
}

fn plan_legacy_catalog_import(
    snapshot: &LegacyCatalogMigrationSnapshot,
    trusted_fallback_root: Option<&Path>,
) -> Result<LegacyCatalogMetadataImportPlan> {
    preflight_legacy_file_set(snapshot)?;
    let trusted_fallback_root = trusted_fallback_root
        .map(validate_trusted_fallback_root)
        .transpose()?;
    let mut plan = build_frozen_v1_metadata_plan(snapshot)?;
    append_v2_table_imports(&mut plan, snapshot, trusted_fallback_root.as_deref())?;
    if !plan.v2_imports.is_empty() {
        plan.migration_version = LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION;
    }
    Ok(plan)
}

fn build_frozen_v1_metadata_plan(
    snapshot: &LegacyCatalogMigrationSnapshot,
) -> Result<LegacyCatalogMetadataImportPlan> {
    let actor = migration_actor();
    let mut commands = Vec::new();
    let namespace = snapshot.namespace();
    let namespace_request_id = derive_request_id(
        snapshot.source_snapshot_hash(),
        "create_namespace",
        &namespace.namespace_id.into_bytes(),
        LEGACY_CATALOG_TARGET_REVISION_BASE,
    );
    commands.push(PlannedCommand {
        command: envelope(
            namespace_request_id,
            LEGACY_CATALOG_TARGET_REVISION_BASE,
            actor.clone(),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: namespace.namespace_id,
                name: namespace.name.clone(),
            }),
        )?,
        kind: PlannedCommandKind::Namespace,
    });

    let mut table_plans = Vec::with_capacity(snapshot.tables().len());
    for table in snapshot.tables() {
        let descriptor = table.descriptor();
        let mut requirements = Vec::new();
        if !table.persisted_files().is_empty() {
            requirements.extend([
                LegacyCatalogV2ImportRequirement::ExternalLegacyLocation,
                LegacyCatalogV2ImportRequirement::LegacyProvenance,
                LegacyCatalogV2ImportRequirement::LegacyUnknownWatermark,
                LegacyCatalogV2ImportRequirement::PreserveDiscoveryOrder,
                LegacyCatalogV2ImportRequirement::StableFileId,
                LegacyCatalogV2ImportRequirement::SchemaIpc,
                LegacyCatalogV2ImportRequirement::CompleteSnapshotRevisionMapping,
                LegacyCatalogV2ImportRequirement::VerifiedPhysicalFileMetadata,
            ]);
        }
        if table.schema_has_custom_metadata() {
            requirements.push(LegacyCatalogV2ImportRequirement::DeterministicSchemaIpc);
        }
        requirements.sort_unstable();
        requirements.dedup();

        if !requirements.is_empty() {
            table_plans.push(LegacyCatalogTablePlan {
                table_id: descriptor.table_id,
                table_name: descriptor.name.clone(),
                legacy_writer_epoch: descriptor.writer_epoch,
                legacy_persisted_files_revision: table.persisted_files_revision(),
                legacy_file_count: table.persisted_files().len(),
                status: LegacyCatalogTablePlanStatus::BlockedRequiresV2,
                legacy_revision_base: None,
                native_snapshot_base: None,
                request_id: None,
                v2_requirements: requirements,
                command_index: None,
                v2_import_index: None,
            });
            continue;
        }

        let expected_revision = u64::try_from(commands.len()).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy metadata import command count exceeds the Catalog revision domain",
            )
        })?;
        let request_id = derive_request_id(
            snapshot.source_snapshot_hash(),
            "create_table",
            &descriptor.table_id.into_bytes(),
            expected_revision,
        );
        let mut config = descriptor.config.clone();
        insert_migration_config(
            &mut config,
            LEGACY_STREAM_NAME_CONFIG_KEY,
            descriptor.name.as_str(),
        )?;
        insert_migration_config(
            &mut config,
            LEGACY_REVISION_BASE_CONFIG_KEY,
            &table.persisted_files_revision().to_string(),
        )?;
        insert_migration_config(
            &mut config,
            NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
            &LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE.to_string(),
        )?;
        insert_migration_config(
            &mut config,
            LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY,
            snapshot.source_snapshot_hash(),
        )?;
        insert_migration_config(
            &mut config,
            LEGACY_MIGRATION_VERSION_CONFIG_KEY,
            &LEGACY_CATALOG_METADATA_MIGRATION_VERSION.to_string(),
        )?;
        let table_plan_index = table_plans.len();
        let command_index = commands.len();
        commands.push(PlannedCommand {
            command: envelope(
                request_id,
                expected_revision,
                actor.clone(),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: descriptor.table_id,
                    namespace_id: descriptor.namespace_id,
                    name: descriptor.name.clone(),
                    definition: descriptor.definition.clone(),
                    schema_ipc: table.schema_ipc().clone(),
                    config,
                }),
            )?,
            kind: PlannedCommandKind::Table { table_plan_index },
        });
        table_plans.push(LegacyCatalogTablePlan {
            table_id: descriptor.table_id,
            table_name: descriptor.name.clone(),
            legacy_writer_epoch: descriptor.writer_epoch,
            legacy_persisted_files_revision: table.persisted_files_revision(),
            legacy_file_count: 0,
            status: LegacyCatalogTablePlanStatus::EligibleMetadataV1,
            legacy_revision_base: Some(table.persisted_files_revision()),
            native_snapshot_base: Some(LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE),
            request_id: Some(request_id),
            v2_requirements: Vec::new(),
            command_index: Some(command_index),
            v2_import_index: None,
        });
    }

    Ok(LegacyCatalogMetadataImportPlan {
        migration_version: LEGACY_CATALOG_METADATA_MIGRATION_VERSION,
        source_snapshot_hash: snapshot.source_snapshot_hash().to_owned(),
        source_catalog_revision: snapshot.source_catalog_revision(),
        commands,
        v2_imports: Vec::new(),
        tables: table_plans,
    })
}

fn preflight_legacy_file_set(snapshot: &LegacyCatalogMigrationSnapshot) -> Result<()> {
    let mut paths = BTreeSet::new();
    for table in snapshot.tables() {
        let files = table.migration_files();
        if files.len() != table.persisted_files().len() {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "legacy migration typed-file projection is incomplete",
            )
            .with_table_id(table.descriptor().table_id)
            .with_retryable(false));
        }
        if files.len() > MAX_LEGACY_FILES_PER_TABLE {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "legacy migration table file count exceeds limit actual=[{}] maximum=[{}]",
                    files.len(),
                    MAX_LEGACY_FILES_PER_TABLE
                ),
            )
            .with_table_id(table.descriptor().table_id)
            .with_retryable(false));
        }
        for file in files {
            if !paths.insert(file.file_path()) {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    format!(
                        "legacy migration snapshot contains a duplicate canonical file path \
                         registry_index=[{}]",
                        file.registry_index()
                    ),
                )
                .with_table_id(table.descriptor().table_id)
                .with_retryable(false));
            }
        }
    }
    Ok(())
}

fn validate_trusted_fallback_root(root: &Path) -> Result<PathBuf> {
    let value = root.to_str().ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "trusted legacy fallback root must be valid UTF-8",
        )
        .with_retryable(false)
    })?;
    canonical_absolute_path_components(value, true).map_err(|reason| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("trusted legacy fallback root is not canonical reason=[{reason}]"),
        )
        .with_retryable(false)
    })?;
    Ok(root.to_path_buf())
}

fn append_v2_table_imports(
    plan: &mut LegacyCatalogMetadataImportPlan,
    snapshot: &LegacyCatalogMigrationSnapshot,
    trusted_fallback_root: Option<&Path>,
) -> Result<()> {
    let mut import_prefix_open = true;
    for (table_plan_index, table) in snapshot.tables().iter().enumerate() {
        if plan.tables[table_plan_index].status == LegacyCatalogTablePlanStatus::EligibleMetadataV1
        {
            continue;
        }
        if !import_prefix_open {
            let requirements = &mut plan.tables[table_plan_index].v2_requirements;
            requirements.push(LegacyCatalogV2ImportRequirement::CommittedImportPrefix);
            requirements.sort_unstable();
            requirements.dedup();
            continue;
        }
        if table.migration_files().is_empty() {
            import_prefix_open = false;
            continue;
        }
        if trusted_fallback_root.is_none()
            && table
                .migration_files()
                .iter()
                .any(|file| file.persist_data_root().is_none())
        {
            let requirements = &mut plan.tables[table_plan_index].v2_requirements;
            requirements.push(LegacyCatalogV2ImportRequirement::TrustedDataRoot);
            requirements.sort_unstable();
            requirements.dedup();
            import_prefix_open = false;
            continue;
        }
        let expected_revision = u64::try_from(plan.commands.len())
            .ok()
            .and_then(|prefix| {
                u64::try_from(plan.v2_imports.len())
                    .ok()
                    .and_then(|suffix| prefix.checked_add(suffix))
            })
            .ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "legacy migration operation count exceeds the Catalog revision domain",
                )
                .with_retryable(false)
            })?;
        let request_id = derive_v2_request_id(
            snapshot.source_snapshot_hash(),
            table.descriptor().table_id,
            expected_revision,
        );
        let v2_import = build_v2_table_import(
            snapshot,
            table,
            table_plan_index,
            request_id,
            expected_revision,
            trusted_fallback_root,
        )?;
        let v2_import_index = plan.v2_imports.len();
        let table_plan = plan.tables.get_mut(table_plan_index).ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "legacy V1 and V2 table plan indexes diverged",
            )
            .with_retryable(false)
        })?;
        table_plan.status = LegacyCatalogTablePlanStatus::EligiblePhysicalV2;
        table_plan.legacy_revision_base = Some(table.persisted_files_revision());
        table_plan.native_snapshot_base = Some(LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE);
        table_plan.request_id = Some(request_id);
        table_plan.v2_requirements.clear();
        table_plan.v2_import_index = Some(v2_import_index);
        plan.v2_imports.push(v2_import);
    }
    Ok(())
}

fn build_v2_table_import(
    snapshot: &LegacyCatalogMigrationSnapshot,
    table: &LegacyCatalogMigrationTable,
    table_plan_index: usize,
    request_id: RequestId,
    expected_revision: u64,
    trusted_fallback_root: Option<&Path>,
) -> Result<PlannedLegacyTableImportV2> {
    if table.persisted_files_revision() == 0 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "nonempty legacy migration table has a zero file-view revision",
        )
        .with_table_id(table.descriptor().table_id)
        .with_retryable(false));
    }
    let canonical_schema_hash = canonical_schema_hash_from_ipc(table.schema_ipc())?;
    let expected_schema = decode_persisted_schema(&SchemaDescriptor {
        table_id: table.descriptor().table_id,
        schema_version: 1,
        schema_ipc: table.schema_ipc().clone(),
        schema_hash: canonical_schema_hash.clone(),
        compatibility: SchemaCompatibility::Initial,
        created_at_ns: 0,
    })?;
    let mut ordered_files = table.migration_files().iter().collect::<Vec<_>>();
    ordered_files.sort_by_key(|file| file.effective_scan_order());
    let mut file_specs = Vec::with_capacity(ordered_files.len());
    for (expected_order, file) in ordered_files.into_iter().enumerate() {
        let expected_order = u64::try_from(expected_order).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy migration discovery order exceeds u64",
            )
            .with_table_id(table.descriptor().table_id)
            .with_retryable(false)
        })?;
        if file.effective_scan_order() != expected_order {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "legacy effective scan order is not contiguous expected=[{expected_order}] \
                     actual=[{}] registry_index=[{}]",
                    file.effective_scan_order(),
                    file.registry_index()
                ),
            )
            .with_table_id(table.descriptor().table_id)
            .with_retryable(false));
        }
        let allowed_root = file
            .persist_data_root()
            .map(PathBuf::from)
            .or_else(|| trusted_fallback_root.map(Path::to_path_buf))
            .expect("append preflight requires a root for every V2 file");
        validate_file_root_relation(table.descriptor().table_id, file, &allowed_root)?;
        let path = ExternalDataPath::new(file.file_path().to_owned()).map_err(|error| {
            ServerError::new(
                ErrorCode::DataCorruption,
                format!(
                    "legacy migration file path cannot enter the Catalog error=[{error}] \
                     registry_index=[{}]",
                    file.registry_index()
                ),
            )
            .with_table_id(table.descriptor().table_id)
            .with_retryable(false)
        })?;
        let mut metadata = MetadataMap::new();
        metadata.insert(
            LEGACY_REGISTRY_INDEX_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(file.registry_index()),
        );
        metadata.insert(
            LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY.to_owned(),
            MetadataScalar::UInt64(file.effective_scan_order()),
        );
        file_specs.push(LegacyParquetImportSpec {
            table_id: table.descriptor().table_id,
            schema_version: 1,
            path,
            allowed_root: Some(allowed_root),
            expected_schema: expected_schema.clone(),
            expected_row_count: file.row_count(),
            expected_checksum: file.checksum().map(str::to_owned),
            discovery_order: file.effective_scan_order(),
            partition: MetadataMap::new(),
            statistics: MetadataMap::new(),
            metadata,
        });
    }
    let config = v2_migration_config(snapshot, table)?;
    Ok(PlannedLegacyTableImportV2 {
        table_plan_index,
        request_id,
        expected_revision,
        command: LegacyCatalogImportCommandV2 {
            table_id: table.descriptor().table_id,
            namespace_id: table.descriptor().namespace_id,
            name: table.descriptor().name.clone(),
            definition: table.descriptor().definition.clone(),
            schema_ipc: table.schema_ipc().clone(),
            canonical_schema_hash,
            config,
            writer_epoch: table.descriptor().writer_epoch,
            files: Vec::new(),
            legacy_files_revision: table.persisted_files_revision(),
        },
        file_specs,
    })
}

pub(crate) async fn verify_legacy_catalog_table_view(
    snapshot: &LegacyCatalogMigrationSnapshot,
    table: &LegacyCatalogMigrationTable,
    trusted_fallback_root: Option<&Path>,
    retained: Option<&BTreeMap<String, VerifiedLegacyCatalogFileCacheEntry>>,
) -> Result<VerifiedLegacyCatalogTableView> {
    let canonical_schema_hash = canonical_schema_hash_from_ipc(table.schema_ipc())?;
    if table.migration_files().is_empty() {
        return Ok(VerifiedLegacyCatalogTableView {
            canonical_schema_hash,
            files: Vec::new(),
            cache: BTreeMap::new(),
        });
    }
    if trusted_fallback_root.is_none()
        && table
            .migration_files()
            .iter()
            .any(|file| file.persist_data_root().is_none())
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy reconcile file lacks a trusted persistence root",
        )
        .with_table_id(table.descriptor().table_id)
        .with_retryable(false));
    }
    let request_id = RequestId::from_u128(1).expect("one is a nonzero request id");
    let planned = build_v2_table_import(snapshot, table, 0, request_id, 0, trusted_fallback_root)?;
    let mut ordered_source = table.migration_files().iter().collect::<Vec<_>>();
    ordered_source.sort_by_key(|file| file.effective_scan_order());
    if ordered_source.len() != planned.file_specs.len() {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "legacy reconcile verification plan length changed",
        )
        .with_table_id(table.descriptor().table_id)
        .with_retryable(false));
    }
    let mut files = Vec::with_capacity(planned.file_specs.len());
    let mut cache = BTreeMap::new();
    for (source, spec) in ordered_source.into_iter().zip(planned.file_specs) {
        let verification_root = spec.allowed_root.clone();
        let cached = retained
            .and_then(|entries| entries.get(source.file_path()))
            .filter(|entry| {
                entry.source_metadata == *source.canonical_value()
                    && entry.canonical_schema_hash == canonical_schema_hash
                    && entry.verification_root == verification_root
            });
        let file = match cached {
            Some(entry) => {
                let mut file = entry.file.clone();
                file.discovery_order = source.effective_scan_order();
                file
            }
            None => {
                let table_id = spec.table_id;
                tokio::task::spawn_blocking(move || verify_legacy_parquet_import(spec))
                    .await
                    .map_err(|error| {
                        ServerError::new(
                            ErrorCode::Internal,
                            format!(
                                "legacy reconcile physical verifier task failed error=[{error}]"
                            ),
                        )
                        .with_table_id(table_id)
                        .with_retryable(false)
                    })??
            }
        };
        cache.insert(
            source.file_path().to_owned(),
            VerifiedLegacyCatalogFileCacheEntry {
                source_metadata: source.canonical_value().clone(),
                canonical_schema_hash: canonical_schema_hash.clone(),
                verification_root,
                file: file.clone(),
            },
        );
        files.push(file);
    }
    Ok(VerifiedLegacyCatalogTableView {
        canonical_schema_hash,
        files,
        cache,
    })
}

fn v2_migration_config(
    snapshot: &LegacyCatalogMigrationSnapshot,
    table: &LegacyCatalogMigrationTable,
) -> Result<BTreeMap<String, String>> {
    let mut config = table.descriptor().config.clone();
    for (key, value) in [
        (
            LEGACY_STREAM_NAME_CONFIG_KEY,
            table.descriptor().name.as_str().to_owned(),
        ),
        (
            LEGACY_REVISION_BASE_CONFIG_KEY,
            table.persisted_files_revision().to_string(),
        ),
        (
            NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
            LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE.to_string(),
        ),
        (
            LEGACY_SOURCE_SNAPSHOT_HASH_CONFIG_KEY,
            snapshot.source_snapshot_hash().to_owned(),
        ),
        (
            LEGACY_MIGRATION_VERSION_CONFIG_KEY,
            LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION.to_string(),
        ),
    ] {
        insert_migration_config(&mut config, key, &value)?;
    }
    Ok(config)
}

fn validate_file_root_relation(
    table_id: TableId,
    file: &LegacyMigrationFile,
    root: &Path,
) -> Result<()> {
    let root = root.to_str().ok_or_else(|| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "legacy migration root must be valid UTF-8",
        )
        .with_table_id(table_id)
        .with_retryable(false)
    })?;
    let root_components = canonical_absolute_path_components(root, true).map_err(|reason| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "legacy migration root is not canonical reason=[{reason}] registry_index=[{}]",
                file.registry_index()
            ),
        )
        .with_table_id(table_id)
        .with_retryable(false)
    })?;
    let file_components = canonical_absolute_path_components(file.file_path(), false)
        .expect("master migration projection guarantees canonical file paths");
    if file_components.len() <= root_components.len()
        || !file_components.starts_with(&root_components)
    {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "legacy migration file is not a strict descendant of its trusted root \
                 registry_index=[{}]",
                file.registry_index()
            ),
        )
        .with_table_id(table_id)
        .with_retryable(false));
    }
    Ok(())
}

fn canonical_absolute_path_components(
    value: &str,
    allow_root: bool,
) -> std::result::Result<Vec<&str>, &'static str> {
    if !value.starts_with('/') {
        return Err("path must be absolute");
    }
    if value.chars().any(char::is_control) {
        return Err("path contains a control character");
    }
    if value == "/" {
        return allow_root
            .then(Vec::new)
            .ok_or("file path must name a component below root");
    }
    if value.ends_with('/') {
        return Err("path has a trailing separator");
    }
    let mut components = Vec::new();
    for component in value[1..].split('/') {
        if component.is_empty() {
            return Err("path contains repeated separators");
        }
        if matches!(component, "." | "..") {
            return Err("path contains a traversal component");
        }
        components.push(component);
    }
    Ok(components)
}

/// Executes an immutable V1/V2 plan through Catalog idempotency and revision guards.
///
/// It acquires and permanently seals the Catalog's one-time startup import session. Committed V2
/// request IDs take a bounded metadata-only replay path after plan comparison. A missing request at
/// its exact expected revision verifies physical files one at a time on the blocking pool; the
/// first failure stops the plan. This function owns no legacy handle and cannot mutate or stop the
/// legacy runtime, so callers may report any error as shadow degradation while keeping the legacy
/// authority available. A successful replay proves Catalog metadata only; the native-storage
/// startup verifier must still validate every visible physical file before readiness opens.
pub(crate) async fn execute_legacy_catalog_metadata_import(
    plan: &LegacyCatalogMetadataImportPlan,
    catalog: &SqliteCatalog,
) -> Result<LegacyCatalogMetadataMigrationReport> {
    let session = catalog.begin_legacy_import_session()?;
    let result = execute_import(plan, catalog, &session).await;
    session.seal();
    match &result {
        Ok(report) => tracing::info!(
            component = "legacy_catalog_migration",
            event = "legacy_catalog_metadata_import_complete",
            source_snapshot_hash = %report.source_snapshot_hash,
            source_catalog_revision = report.source_catalog_revision,
            target_revision_before = report.target_revision_before,
            target_revision_after = report.target_revision_after,
            submitted_commands = report.submitted_commands,
            replayed_commands = report.replayed_commands,
            blocked_tables = report.blocked_table_count(),
            "legacy catalog metadata import completed"
        ),
        Err(error) => tracing::error!(
            component = "legacy_catalog_migration",
            event = "legacy_catalog_metadata_import_failed",
            source_snapshot_hash = %plan.source_snapshot_hash,
            source_catalog_revision = plan.source_catalog_revision,
            error_code = %error.code(),
            request_id = error.request_id().map_or_else(|| "NONE".to_owned(), |value| value.to_string()),
            table_id = error.table_id().map_or_else(|| "NONE".to_owned(), |value| value.to_string()),
            "legacy catalog metadata import failed without changing legacy authority"
        ),
    }
    result
}

async fn execute_import(
    plan: &LegacyCatalogMetadataImportPlan,
    catalog: &SqliteCatalog,
    session: &SqliteCatalogLegacyImportSession<'_>,
) -> Result<LegacyCatalogMetadataMigrationReport> {
    let target_revision_before = catalog.revision().await?;
    let mut dispositions = vec![None; plan.commands.len()];
    let mut first_missing = None;
    for (index, planned) in plan.commands.iter().enumerate() {
        match catalog.probe_command_replay(&planned.command).await? {
            CatalogCommandReplayProbe::Exact { response } => {
                if first_missing.is_some() {
                    return Err(ServerError::new(
                        ErrorCode::DataCorruption,
                        "legacy metadata import idempotency records are not a committed prefix",
                    ));
                }
                validate_response(planned, &response)?;
                dispositions[index] = Some(LegacyCatalogCommandDisposition::Replayed);
            }
            CatalogCommandReplayProbe::Missing => {
                first_missing.get_or_insert(index);
            }
        }
    }

    let revision_after_probes = catalog.revision().await?;
    if let Some(first_missing) = first_missing {
        let expected_revision = command_expected_revision(&plan.commands[first_missing].command);
        if revision_after_probes != expected_revision {
            return Err(ServerError::new(
                ErrorCode::Conflict,
                format!(
                    concat!(
                        "legacy metadata import target revision does not match the exact ",
                        "committed command prefix expected=[{}] actual=[{}]"
                    ),
                    expected_revision, revision_after_probes
                ),
            )
            .with_request_id(command_request_id(&plan.commands[first_missing].command)));
        }
        for (index, planned) in plan.commands.iter().enumerate().skip(first_missing) {
            let response = catalog.command(planned.command.clone()).await?;
            validate_response(planned, &response)?;
            dispositions[index] = Some(LegacyCatalogCommandDisposition::Submitted);
        }
    }

    let mut v2_dispositions = Vec::with_capacity(plan.v2_imports.len());
    for planned in &plan.v2_imports {
        match session.probe_legacy_import_v2(planned.request_id).await? {
            LegacyCatalogImportReplayProbeV2::Committed {
                observed_catalog_revision,
                response,
            } => {
                validate_v2_replay_response(planned, observed_catalog_revision, &response)?;
                v2_dispositions.push(LegacyCatalogCommandDisposition::Replayed);
                continue;
            }
            LegacyCatalogImportReplayProbeV2::Missing {
                observed_catalog_revision,
            } => {
                if observed_catalog_revision != planned.expected_revision {
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        format!(
                            concat!(
                                "legacy V2 import target revision does not match the exact ",
                                "committed import prefix expected=[{}] actual=[{}]"
                            ),
                            planned.expected_revision, observed_catalog_revision
                        ),
                    )
                    .with_request_id(planned.request_id)
                    .with_table_id(planned.command.table_id));
                }
            }
        }
        let files = verify_v2_table_files(planned).await?;
        let mut command = planned.command.clone();
        command.files = files;
        let expected_summary = expected_v2_summary(&command)?;
        let envelope = LegacyCatalogImportEnvelopeV2::new(
            planned.request_id,
            Some(planned.expected_revision),
            migration_v2_actor(),
            plan.source_snapshot_hash.clone(),
            plan.source_catalog_revision,
            command,
        )
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("failed to fingerprint legacy V2 table import error=[{error}]"),
            )
            .with_request_id(planned.request_id)
            .with_table_id(planned.command.table_id)
            .with_retryable(false)
        })?;
        let response = session.import_legacy_table_v2(envelope).await?;
        validate_v2_response(planned, &expected_summary, &response)?;
        let revision_after = catalog.revision().await?;
        if revision_after != response.catalog_revision {
            return Err(v2_revision_corruption(
                planned,
                planned.expected_revision,
                revision_after,
                "submitted import did not advance to its response revision",
            ));
        }
        v2_dispositions.push(LegacyCatalogCommandDisposition::Submitted);
    }

    let target_revision_after = catalog.revision().await?;
    let dispositions = dispositions
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::Internal,
                "legacy metadata import completed without a command disposition",
            )
        })?;
    let namespace = dispositions[0];
    let replayed_commands = dispositions
        .iter()
        .filter(|disposition| **disposition == LegacyCatalogCommandDisposition::Replayed)
        .count()
        + v2_dispositions
            .iter()
            .filter(|disposition| **disposition == LegacyCatalogCommandDisposition::Replayed)
            .count();
    let submitted_commands = dispositions.len() + v2_dispositions.len() - replayed_commands;
    let tables = plan
        .tables
        .iter()
        .map(|table| {
            let outcome = match table.command_index {
                Some(command_index) => match dispositions[command_index] {
                    LegacyCatalogCommandDisposition::Replayed => {
                        LegacyCatalogTableMigrationOutcome::MetadataReplayed
                    }
                    LegacyCatalogCommandDisposition::Submitted => {
                        LegacyCatalogTableMigrationOutcome::MetadataSubmitted
                    }
                },
                None => match table.v2_import_index {
                    Some(import_index) => match v2_dispositions[import_index] {
                        LegacyCatalogCommandDisposition::Replayed => {
                            LegacyCatalogTableMigrationOutcome::PhysicalReplayed
                        }
                        LegacyCatalogCommandDisposition::Submitted => {
                            LegacyCatalogTableMigrationOutcome::PhysicalSubmitted
                        }
                    },
                    None => LegacyCatalogTableMigrationOutcome::BlockedRequiresV2,
                },
            };
            LegacyCatalogTableMigrationReport {
                table_id: table.table_id,
                table_name: table.table_name.clone(),
                legacy_persisted_files_revision: table.legacy_persisted_files_revision,
                legacy_file_count: table.legacy_file_count,
                outcome,
                legacy_revision_base: table.legacy_revision_base,
                native_snapshot_base: table.native_snapshot_base,
                v2_requirements: table.v2_requirements.clone(),
            }
        })
        .collect();
    Ok(LegacyCatalogMetadataMigrationReport {
        migration_version: plan.migration_version,
        source_snapshot_hash: plan.source_snapshot_hash.clone(),
        source_catalog_revision: plan.source_catalog_revision,
        target_revision_before,
        target_revision_after,
        namespace,
        replayed_commands,
        submitted_commands,
        tables,
    })
}

async fn verify_v2_table_files(
    planned: &PlannedLegacyTableImportV2,
) -> Result<Vec<zippy_types::catalog::LegacyCatalogImportFileV2>> {
    let mut files = Vec::with_capacity(planned.file_specs.len());
    for spec in &planned.file_specs {
        let spec = spec.clone();
        let table_id = spec.table_id;
        let verified = tokio::task::spawn_blocking(move || verify_legacy_parquet_import(spec))
            .await
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!(
                        "legacy V2 physical verifier task failed table_plan_index=[{}] \
                         error=[{error}]",
                        planned.table_plan_index
                    ),
                )
                .with_request_id(planned.request_id)
                .with_table_id(table_id)
                .with_retryable(false)
            })??;
        files.push(verified);
    }
    Ok(files)
}

fn validate_v2_response(
    planned: &PlannedLegacyTableImportV2,
    expected_summary: &MetadataMap,
    response: &LegacyCatalogImportResponseV2,
) -> Result<()> {
    validate_v2_response_shape(
        planned,
        response,
        response.snapshot.summary == *expected_summary,
    )
}

fn validate_v2_replay_response(
    planned: &PlannedLegacyTableImportV2,
    observed_catalog_revision: u64,
    response: &LegacyCatalogImportResponseV2,
) -> Result<()> {
    let file_count = u64::try_from(planned.file_specs.len()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy V2 replay file count overflow",
        )
        .with_request_id(planned.request_id)
        .with_table_id(planned.command.table_id)
        .with_retryable(false)
    })?;
    let expected_rows = planned
        .file_specs
        .iter()
        .try_fold(Some(0_u64), |total, file| {
            match (total, file.expected_row_count) {
                (Some(total), Some(rows)) => {
                    total.checked_add(rows.get()).map(Some).ok_or_else(|| {
                        ServerError::new(
                            ErrorCode::ResourceExhausted,
                            "legacy V2 replay row count overflow",
                        )
                        .with_request_id(planned.request_id)
                        .with_table_id(planned.command.table_id)
                        .with_retryable(false)
                    })
                }
                _ => Ok(None),
            }
        })?;
    let summary = &response.snapshot.summary;
    let summary_matches = summary.get("added_files") == Some(&MetadataScalar::UInt64(file_count))
        && summary.get("legacy_files_revision")
            == Some(&MetadataScalar::UInt64(
                planned.command.legacy_files_revision,
            ))
        && expected_rows
            .is_none_or(|rows| summary.get("added_rows") == Some(&MetadataScalar::UInt64(rows)));
    validate_v2_response_shape(planned, response, summary_matches)?;
    if observed_catalog_revision < response.catalog_revision {
        return Err(v2_revision_corruption(
            planned,
            response.catalog_revision,
            observed_catalog_revision,
            "replay response is newer than the observed Catalog revision",
        ));
    }
    Ok(())
}

fn validate_v2_response_shape(
    planned: &PlannedLegacyTableImportV2,
    response: &LegacyCatalogImportResponseV2,
    summary_matches: bool,
) -> Result<()> {
    let expected_revision = planned.expected_revision.checked_add(1).ok_or_else(|| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy V2 import response revision overflow",
        )
        .with_request_id(planned.request_id)
        .with_table_id(planned.command.table_id)
        .with_retryable(false)
    })?;
    let matches = response.request_id == planned.request_id
        && response.catalog_revision == expected_revision
        && response.table.table_id == planned.command.table_id
        && response.table.namespace_id == planned.command.namespace_id
        && response.table.name == planned.command.name
        && response.table.definition == planned.command.definition
        && response.table.config == planned.command.config
        && response.table.current_schema_version == 1
        && response.table.current_snapshot_version == LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE
        && response.table.writer_epoch == planned.command.writer_epoch
        && response.table.state == TableState::Active
        && response.table.created_at_ns == response.table.updated_at_ns
        && response.snapshot.table_id == planned.command.table_id
        && response.snapshot.snapshot_version == LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE
        && response.snapshot.parent_snapshot_version == Some(LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE)
        && response.snapshot.operation == SnapshotOperation::Import
        && response.snapshot.commit_lsn == response.catalog_revision
        && response.snapshot.created_at_ns == response.table.updated_at_ns
        && response.snapshot.committed_log_offset == OffsetWatermark::LegacyUnknown
        && summary_matches;
    if matches {
        return Ok(());
    }
    Err(ServerError::new(
        ErrorCode::DataCorruption,
        "legacy V2 import response does not match its planned table import",
    )
    .with_request_id(planned.request_id)
    .with_table_id(planned.command.table_id)
    .with_retryable(false))
}

fn expected_v2_summary(command: &LegacyCatalogImportCommandV2) -> Result<MetadataMap> {
    let rows = command.files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.file.row_count.get()).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy V2 response row count overflow",
            )
            .with_table_id(command.table_id)
            .with_retryable(false)
        })
    })?;
    let bytes = command.files.iter().try_fold(0_u64, |total, file| {
        total.checked_add(file.file.file_size_bytes).ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy V2 response byte count overflow",
            )
            .with_table_id(command.table_id)
            .with_retryable(false)
        })
    })?;
    let file_count = u64::try_from(command.files.len()).map_err(|_| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy V2 response file count overflow",
        )
        .with_table_id(command.table_id)
        .with_retryable(false)
    })?;
    Ok(MetadataMap::from([
        ("added_files".to_owned(), MetadataScalar::UInt64(file_count)),
        ("added_rows".to_owned(), MetadataScalar::UInt64(rows)),
        ("added_bytes".to_owned(), MetadataScalar::UInt64(bytes)),
        (
            "legacy_files_revision".to_owned(),
            MetadataScalar::UInt64(command.legacy_files_revision),
        ),
    ]))
}

fn v2_revision_corruption(
    planned: &PlannedLegacyTableImportV2,
    revision_before: u64,
    revision_after: u64,
    reason: &str,
) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "legacy V2 import revision transition is invalid before=[{revision_before}] \
             after=[{revision_after}] reason=[{reason}]"
        ),
    )
    .with_request_id(planned.request_id)
    .with_table_id(planned.command.table_id)
    .with_retryable(false)
}

fn validate_response(planned: &PlannedCommand, response: &VersionedCatalogResponse) -> Result<()> {
    let VersionedCatalogResponse::V1(response) = response;
    let request_id = command_request_id(&planned.command);
    let expected_revision = command_expected_revision(&planned.command)
        .checked_add(1)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy metadata import response revision overflow",
            )
        })?;
    if response.request_id != request_id || response.catalog_revision != expected_revision {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                concat!(
                    "legacy metadata import response identity mismatch expected_request=[{}] ",
                    "actual_request=[{}] expected_revision=[{}] actual_revision=[{}]"
                ),
                request_id, response.request_id, expected_revision, response.catalog_revision
            ),
        )
        .with_request_id(request_id));
    }

    let VersionedCatalogCommand::V1(envelope) = &planned.command;
    let matches = match (&envelope.command, &response.outcome, planned.kind) {
        (
            CatalogCommandV1::CreateNamespace(command),
            CatalogOutcomeV1::Namespace(namespace),
            PlannedCommandKind::Namespace,
        ) => namespace.namespace_id == command.namespace_id && namespace.name == command.name,
        (
            CatalogCommandV1::CreateTable(command),
            CatalogOutcomeV1::Table(table),
            PlannedCommandKind::Table { table_plan_index },
        ) => {
            let _ = table_plan_index;
            table.table_id == command.table_id
                && table.namespace_id == command.namespace_id
                && table.name == command.name
                && table.definition == command.definition
                && table.config == command.config
                && table.current_schema_version == 1
                && table.current_snapshot_version == LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE
                && table.writer_epoch == 0
                && table.state == TableState::Active
        }
        _ => false,
    };
    if !matches {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "legacy metadata import response does not match its planned command",
        )
        .with_request_id(request_id));
    }
    Ok(())
}

fn migration_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Migration,
        principal: MIGRATION_ACTOR_PRINCIPAL.to_owned(),
    }
}

fn migration_v2_actor() -> CatalogActor {
    CatalogActor {
        kind: CatalogActorKind::Migration,
        principal: MIGRATION_V2_ACTOR_PRINCIPAL.to_owned(),
    }
}

fn envelope(
    request_id: RequestId,
    expected_revision: u64,
    actor: CatalogActor,
    command: CatalogCommandV1,
) -> Result<VersionedCatalogCommand> {
    CatalogCommandEnvelopeV1::new(request_id, Some(expected_revision), actor, command)
        .map(VersionedCatalogCommand::V1)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("failed to fingerprint legacy metadata import command error=[{error}]"),
            )
            .with_request_id(request_id)
        })
}

fn insert_migration_config(
    config: &mut BTreeMap<String, String>,
    key: &str,
    value: &str,
) -> Result<()> {
    match config.insert(key.to_owned(), value.to_owned()) {
        Some(existing) if existing != value => Err(ServerError::new(
            ErrorCode::Conflict,
            format!(
                "legacy table config collides with migration metadata key=[{key}] existing=[{existing}]"
            ),
        )),
        _ => Ok(()),
    }
}

fn derive_request_id(
    source_snapshot_hash: &str,
    operation: &str,
    entity_id: &[u8],
    expected_revision: u64,
) -> RequestId {
    let mut hasher = Sha256::new();
    for component in [
        REQUEST_ID_DOMAIN,
        source_snapshot_hash.as_bytes(),
        operation.as_bytes(),
        entity_id,
        &expected_revision.to_be_bytes(),
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
    RequestId::from_bytes(bytes).expect("request hash is forced nonzero")
}

fn derive_v2_request_id(
    source_snapshot_hash: &str,
    table_id: TableId,
    expected_revision: u64,
) -> RequestId {
    let mut hasher = Sha256::new();
    for component in [
        REQUEST_ID_V2_DOMAIN,
        source_snapshot_hash.as_bytes(),
        &table_id.into_bytes(),
        &expected_revision.to_be_bytes(),
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
    RequestId::from_bytes(bytes).expect("V2 request hash is forced nonzero")
}

fn command_request_id(command: &VersionedCatalogCommand) -> RequestId {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope.request_id
}

fn command_expected_revision(command: &VersionedCatalogCommand) -> u64 {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope
        .expected_revision
        .expect("legacy migration commands always guard a target revision")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::num::NonZeroU32;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{canonical_schema_hash, schema_metadata, OwnedDataDirectory};
    use zippy_master::registry::Registry;
    use zippy_master::server::MasterServer;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandV1,
        CatalogRead, CatalogWrite, CreateNamespaceCommand, ListTablesRequest,
        VersionedCatalogCommand, LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY,
        LEGACY_REGISTRY_INDEX_METADATA_KEY,
    };
    use zippy_types::table::{CatalogName, DataFileLocation, MetadataScalar, TableRef};
    use zippy_types::{ErrorCode, NamespaceId, RequestId};

    use super::{
        execute_legacy_catalog_metadata_import, plan_legacy_catalog_metadata_import,
        LegacyCatalogCommandDisposition, LegacyCatalogMetadataImportPlan,
        LegacyCatalogTableMigrationOutcome, LegacyCatalogTablePlanStatus,
        LegacyCatalogV2ImportRequirement, LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION,
        LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE, LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE,
        LEGACY_MIGRATION_VERSION_CONFIG_KEY, LEGACY_REVISION_BASE_CONFIG_KEY,
        LEGACY_STREAM_NAME_CONFIG_KEY, NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
    };

    fn open_catalog(directory: &TempDir) -> SqliteCatalog {
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(directory.path().join("data")).unwrap(),
        );
        SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap()
    }

    fn append_schema(custom_metadata: bool) -> Arc<Schema> {
        let metadata = custom_metadata
            .then(|| HashMap::from([("unit".to_owned(), "ticks".to_owned())]))
            .unwrap_or_default();
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )
        .with_metadata(metadata)]))
    }

    fn add_append_table(registry: &mut Registry, stream_name: &str, custom_metadata: bool) {
        let schema = append_schema(custom_metadata);
        registry
            .register_stream(
                stream_name,
                schema_metadata(&schema),
                &canonical_schema_hash(&schema),
                64,
                64,
            )
            .unwrap();
        registry
            .register_source(
                &format!("{stream_name}_source"),
                "test",
                &format!("{stream_name}_process"),
                stream_name,
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
    }

    fn write_parquet(path: &Path, values: Vec<i64>) {
        let schema = append_schema(false);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn legacy_file_value(
        path: &Path,
        root: Option<&Path>,
        writer_epoch: u64,
        created_at: u64,
        row_count: u64,
    ) -> serde_json::Value {
        let mut value = json!({
            "file_path": path.to_str().unwrap(),
            "writer_epoch": writer_epoch,
            "source_segment_id": 1,
            "source_generation": 0,
            "created_at": created_at,
            "row_count": row_count,
        });
        if let Some(root) = root {
            value.as_object_mut().unwrap().insert(
                "persist_data_root".to_owned(),
                serde_json::Value::String(root.to_str().unwrap().to_owned()),
            );
        }
        value
    }

    fn snapshot_with_tables(
        tables: Vec<(&str, Vec<serde_json::Value>)>,
    ) -> (
        MasterServer,
        zippy_master::legacy_catalog::LegacyCatalogMigrationSnapshot,
    ) {
        let master = MasterServer::default();
        {
            let registry = master.registry();
            let mut registry = registry.lock().unwrap();
            for (stream_name, files) in tables {
                add_append_table(&mut registry, stream_name, false);
                if !files.is_empty() {
                    registry
                        .replace_persisted_files(stream_name, files)
                        .unwrap();
                }
            }
        }
        let snapshot = master
            .legacy_catalog_adapter()
            .capture_migration_snapshot()
            .unwrap();
        (master, snapshot)
    }

    fn migration_snapshot() -> (
        MasterServer,
        zippy_master::legacy_catalog::LegacyCatalogMigrationSnapshot,
    ) {
        let master = MasterServer::default();
        {
            let registry = master.registry();
            let mut registry = registry.lock().unwrap();
            add_append_table(&mut registry, "empty_events", false);
            registry
                .replace_persisted_files(
                    "empty_events",
                    vec![json!({"file_path": "/legacy/removed.parquet", "row_count": 1})],
                )
                .unwrap();
            registry
                .replace_persisted_files("empty_events", Vec::new())
                .unwrap();

            add_append_table(&mut registry, "legacy_events", false);
            registry
                .replace_persisted_files(
                    "legacy_events",
                    vec![json!({
                        "file_path": "/legacy/events.parquet",
                        "row_count": 2,
                        "created_at": 1
                    })],
                )
                .unwrap();

            add_append_table(&mut registry, "metadata_events", true);
        }
        let snapshot = master
            .legacy_catalog_adapter()
            .capture_migration_snapshot()
            .unwrap();
        (master, snapshot)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn online_verifier_reuses_retained_files_and_evicts_removed_entries() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let retained_path = root.join("retained.parquet");
        let removed_path = root.join("removed.parquet");
        let added_path = root.join("added.parquet");
        write_parquet(&retained_path, vec![1, 2]);
        write_parquet(&removed_path, vec![3]);
        let retained = legacy_file_value(&retained_path, Some(root), 1, 10, 2);
        let removed = legacy_file_value(&removed_path, Some(root), 1, 20, 1);
        let (master, first_snapshot) =
            snapshot_with_tables(vec![("events", vec![retained.clone(), removed])]);
        let first_table = first_snapshot.tables().first().unwrap();
        let first =
            super::verify_legacy_catalog_table_view(&first_snapshot, first_table, None, None)
                .await
                .unwrap();
        assert_eq!(first.files.len(), 2);

        std::fs::remove_file(&retained_path).unwrap();
        write_parquet(&added_path, vec![4, 5, 6]);
        let added = legacy_file_value(&added_path, Some(root), 1, 1, 3);
        master
            .registry()
            .lock()
            .unwrap()
            .replace_persisted_files("events", vec![retained, added])
            .unwrap();
        let second_snapshot = master
            .legacy_catalog_adapter()
            .capture_migration_snapshot()
            .unwrap();
        let second = super::verify_legacy_catalog_table_view(
            &second_snapshot,
            second_snapshot.tables().first().unwrap(),
            None,
            Some(&first.cache),
        )
        .await
        .unwrap();

        assert_eq!(second.cache.len(), 2);
        assert!(!second.cache.contains_key(removed_path.to_str().unwrap()));
        assert_eq!(
            second
                .files
                .iter()
                .map(|file| file.discovery_order)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            second
                .files
                .iter()
                .map(|file| match &file.file.location {
                    DataFileLocation::ExternalLegacy(path) => path.as_str(),
                    DataFileLocation::Managed(_) => panic!("legacy verifier returned managed path"),
                })
                .collect::<Vec<_>>(),
            vec![
                added_path.to_str().unwrap(),
                retained_path.to_str().unwrap()
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn imports_only_exact_empty_tables_and_replays_without_advancing_revision() {
        let (master, snapshot) = migration_snapshot();
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        let frozen_commands = plan
            .commands
            .iter()
            .map(|planned| CatalogCodec::encode_command(&planned.command).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            snapshot.source_snapshot_hash(),
            "sha256-v1:385d1c8fa1fd33702a7ced410e1c5003f137b65e6df6f7529adf75aefd4b679f"
        );
        assert_eq!(
            plan.commands
                .iter()
                .map(|planned| super::command_request_id(&planned.command).to_string())
                .collect::<Vec<_>>(),
            vec![
                "3fba792a-53b4-a1f2-a7f9-e0a549fa3197",
                "823aa12b-ad39-43e6-2499-7307e951193e",
            ]
        );
        assert_eq!(
            format!(
                "{:x}",
                <sha2::Sha256 as sha2::Digest>::digest(
                    serde_json::to_vec(&frozen_commands).unwrap()
                )
            ),
            "f5bf586ea8b2f119d3eb514aab1a948944ddab0439848f48c2a4aaed317c50c2"
        );
        assert_eq!(plan.command_count(), 2);
        assert!(!plan.is_complete_for_v1_metadata());

        let empty = plan
            .tables()
            .iter()
            .find(|table| table.table_name.as_str() == "empty_events")
            .unwrap();
        assert_eq!(
            empty.status,
            LegacyCatalogTablePlanStatus::EligibleMetadataV1
        );
        assert_eq!(empty.legacy_persisted_files_revision, 2);
        assert_eq!(empty.legacy_revision_base, Some(2));
        assert_eq!(
            empty.native_snapshot_base,
            Some(LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE)
        );

        let legacy = plan
            .tables()
            .iter()
            .find(|table| table.table_name.as_str() == "legacy_events")
            .unwrap();
        assert_eq!(
            legacy.status,
            LegacyCatalogTablePlanStatus::BlockedRequiresV2
        );
        assert!(legacy
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::ExternalLegacyLocation));
        assert!(legacy
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::LegacyUnknownWatermark));
        let metadata = plan
            .tables()
            .iter()
            .find(|table| table.table_name.as_str() == "metadata_events")
            .unwrap();
        assert!(metadata
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::DeterministicSchemaIpc));

        let directory = TempDir::new().unwrap();
        let catalog = open_catalog(&directory);
        let first = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap();
        assert_eq!(first.submitted_commands, 2);
        assert_eq!(first.replayed_commands, 0);
        assert_eq!(first.blocked_table_count(), 2);
        assert_eq!(first.namespace, LegacyCatalogCommandDisposition::Submitted);
        let imported = first
            .tables
            .iter()
            .find(|table| table.table_name.as_str() == "empty_events")
            .unwrap();
        assert_eq!(
            imported.outcome,
            LegacyCatalogTableMigrationOutcome::MetadataSubmitted
        );

        let native = catalog
            .get_table(TableRef::Name {
                namespace: CatalogName::default_namespace(),
                table: CatalogName::new("empty_events").unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(
            native.config.get(LEGACY_STREAM_NAME_CONFIG_KEY),
            Some(&"empty_events".to_owned())
        );
        assert_eq!(
            native.config.get(LEGACY_REVISION_BASE_CONFIG_KEY),
            Some(&"2".to_owned())
        );
        assert_eq!(
            native.config.get(NATIVE_SNAPSHOT_BASE_CONFIG_KEY),
            Some(&"1".to_owned())
        );
        for blocked in ["legacy_events", "metadata_events"] {
            let error = catalog
                .get_table(TableRef::Name {
                    namespace: CatalogName::default_namespace(),
                    table: CatalogName::new(blocked).unwrap(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::NotFound);
        }

        catalog.close().unwrap();
        let catalog = open_catalog(&directory);
        let second = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap();
        assert_eq!(second.submitted_commands, 0);
        assert_eq!(second.replayed_commands, 2);
        assert_eq!(second.target_revision_before, first.target_revision_after);
        assert_eq!(second.target_revision_after, first.target_revision_after);
        assert_eq!(second.namespace, LegacyCatalogCommandDisposition::Replayed);

        let legacy_tables = master
            .legacy_catalog_adapter()
            .list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(10).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(legacy_tables.tables.len(), 3);
        catalog.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_nonempty_target_without_mutating_the_legacy_registry() {
        let (master, snapshot) = migration_snapshot();
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        let directory = TempDir::new().unwrap();
        let catalog = open_catalog(&directory);
        let preexisting = CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(1).unwrap(),
            Some(0),
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "migration-test".to_owned(),
            },
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: NamespaceId::from_u128(1).unwrap(),
                name: CatalogName::new("unrelated").unwrap(),
            }),
        )
        .unwrap();
        catalog
            .command(VersionedCatalogCommand::V1(preexisting))
            .await
            .unwrap();

        let error = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(catalog.revision().await.unwrap(), 1);

        let legacy_tables = master
            .legacy_catalog_adapter()
            .list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(10).unwrap(),
            })
            .await
            .unwrap();
        assert_eq!(legacy_tables.tables.len(), 3);
        catalog.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumes_only_from_an_exact_committed_command_prefix() {
        let (_master, snapshot) = migration_snapshot();
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        let directory = TempDir::new().unwrap();
        let catalog = open_catalog(&directory);
        catalog
            .command(plan.commands[0].command.clone())
            .await
            .unwrap();

        let report = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap();

        assert_eq!(report.replayed_commands, 1);
        assert_eq!(report.submitted_commands, 1);
        assert_eq!(report.namespace, LegacyCatalogCommandDisposition::Replayed);
        assert_eq!(report.target_revision_before, 1);
        assert_eq!(report.target_revision_after, 2);
        catalog.close().unwrap();
    }

    #[test]
    fn v2_plans_are_appended_after_frozen_v1_prefix_in_gateway_scan_order() {
        let legacy_root = Path::new("/legacy");
        let late = legacy_root.join("late.parquet");
        let early = legacy_root.join("early.parquet");
        let other = legacy_root.join("other.parquet");
        let (_master, snapshot) = snapshot_with_tables(vec![
            ("empty_events", Vec::new()),
            (
                "scan_events",
                vec![
                    legacy_file_value(&late, Some(legacy_root), 2, 20, 2),
                    legacy_file_value(&early, Some(legacy_root), 1, 10, 2),
                ],
            ),
            (
                "other_events",
                vec![legacy_file_value(&other, Some(legacy_root), 3, 30, 2)],
            ),
        ]);

        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();

        assert_eq!(plan.command_count(), 2);
        assert_eq!(plan.v2_import_count(), 2);
        assert_eq!(
            plan.v2_imports
                .iter()
                .map(|table| table.request_id.to_string())
                .collect::<Vec<_>>(),
            vec![
                "637330da-0111-818a-4f92-cc4ae63d5a45",
                "70b8a792-1c0b-867c-06f2-574a29114ad3",
            ]
        );
        assert_eq!(
            plan.commands
                .iter()
                .map(|command| super::command_expected_revision(&command.command))
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            plan.v2_imports
                .iter()
                .map(|table| table.expected_revision)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            plan.migration_version(),
            LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION
        );
        let scan = plan
            .v2_imports
            .iter()
            .find(|table| table.command.name.as_str() == "scan_events")
            .unwrap();
        assert_eq!(
            scan.file_specs
                .iter()
                .map(|spec| spec.path.as_str())
                .collect::<Vec<_>>(),
            vec![early.to_str().unwrap(), late.to_str().unwrap()]
        );
        assert_eq!(
            scan.file_specs
                .iter()
                .map(|spec| spec.discovery_order)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            scan.file_specs[0]
                .metadata
                .get(LEGACY_REGISTRY_INDEX_METADATA_KEY),
            Some(&MetadataScalar::UInt64(1))
        );
        assert_eq!(
            scan.file_specs[0]
                .metadata
                .get(LEGACY_EFFECTIVE_SCAN_ORDER_METADATA_KEY),
            Some(&MetadataScalar::UInt64(0))
        );
        assert_eq!(
            scan.command.config.get(NATIVE_SNAPSHOT_BASE_CONFIG_KEY),
            Some(&LEGACY_IMPORTED_NATIVE_SNAPSHOT_BASE.to_string())
        );
        assert_eq!(
            scan.command.config.get(LEGACY_MIGRATION_VERSION_CONFIG_KEY),
            Some(&LEGACY_CATALOG_PHYSICAL_MIGRATION_VERSION.to_string())
        );
        let replay_plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(
            replay_plan
                .v2_imports
                .iter()
                .map(|table| table.request_id)
                .collect::<Vec<_>>(),
            plan.v2_imports
                .iter()
                .map(|table| table.request_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn file_root_precedes_explicit_fallback_and_missing_root_requires_it() {
        let root = TempDir::new().unwrap();
        let declared_root = root.path().join("declared");
        let fallback_root = root.path().join("fallback");
        std::fs::create_dir(&declared_root).unwrap();
        std::fs::create_dir(&fallback_root).unwrap();
        let declared_file = declared_root.join("declared.parquet");
        let fallback_file = fallback_root.join("fallback.parquet");
        write_parquet(&declared_file, vec![1]);
        write_parquet(&fallback_file, vec![2]);
        let (_master, snapshot) = snapshot_with_tables(vec![
            (
                "declared_events",
                vec![legacy_file_value(
                    &declared_file,
                    Some(&declared_root),
                    1,
                    1,
                    1,
                )],
            ),
            (
                "fallback_events",
                vec![legacy_file_value(&fallback_file, None, 1, 1, 1)],
            ),
        ]);

        let without_fallback = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert!(without_fallback
            .v2_imports
            .iter()
            .all(|table| { table.command.name.as_str() == "declared_events" }));
        let missing_root = without_fallback
            .tables()
            .iter()
            .find(|table| table.table_name.as_str() == "fallback_events")
            .unwrap();
        assert!(missing_root
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::TrustedDataRoot));
        let plan =
            LegacyCatalogMetadataImportPlan::with_trusted_fallback_root(&snapshot, &fallback_root)
                .unwrap();
        assert_eq!(plan.v2_import_count(), 2);
        let declared = plan
            .v2_imports
            .iter()
            .find(|table| table.command.name.as_str() == "declared_events")
            .unwrap();
        assert_eq!(
            declared.file_specs[0].allowed_root.as_deref(),
            Some(declared_root.as_path())
        );
        let fallback = plan
            .v2_imports
            .iter()
            .find(|table| table.command.name.as_str() == "fallback_events")
            .unwrap();
        assert_eq!(
            fallback.file_specs[0].allowed_root.as_deref(),
            Some(fallback_root.as_path())
        );

        let outside = declared_root.join("outside.parquet");
        let (_master, outside_snapshot) = snapshot_with_tables(vec![(
            "outside_events",
            vec![legacy_file_value(&outside, None, 1, 1, 1)],
        )]);
        let error = LegacyCatalogMetadataImportPlan::with_trusted_fallback_root(
            &outside_snapshot,
            &fallback_root,
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn v2_planner_preserves_a_stable_contiguous_import_prefix() {
        let root = TempDir::new().unwrap();
        let legacy_root = root.path().join("legacy");
        std::fs::create_dir(&legacy_root).unwrap();
        let names = ["prefix_a", "prefix_b", "prefix_c"];
        let seed_tables = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let path = legacy_root.join(format!("seed-{index}.parquet"));
                write_parquet(&path, vec![index as i64]);
                (
                    *name,
                    vec![legacy_file_value(&path, Some(&legacy_root), 1, 1, 1)],
                )
            })
            .collect();
        let (_master, seed_snapshot) = snapshot_with_tables(seed_tables);
        let ordered_names = seed_snapshot
            .tables()
            .iter()
            .map(|table| table.descriptor().name.as_str().to_owned())
            .collect::<Vec<_>>();

        let tables = ordered_names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let path = legacy_root.join(format!("ordered-{index}.parquet"));
                write_parquet(&path, vec![index as i64]);
                let declared_root = (index != 1).then_some(legacy_root.as_path());
                (
                    name.as_str(),
                    vec![legacy_file_value(&path, declared_root, 1, 1, 1)],
                )
            })
            .collect();
        let (_master, snapshot) = snapshot_with_tables(tables);

        let prefix = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(prefix.v2_import_count(), 1);
        assert_eq!(prefix.v2_imports[0].command.name.as_str(), ordered_names[0]);
        assert!(prefix.tables()[1]
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::TrustedDataRoot));
        assert!(prefix.tables()[2]
            .v2_requirements
            .contains(&LegacyCatalogV2ImportRequirement::CommittedImportPrefix));

        let complete =
            LegacyCatalogMetadataImportPlan::with_trusted_fallback_root(&snapshot, &legacy_root)
                .unwrap();
        assert_eq!(complete.v2_import_count(), 3);
        assert_eq!(
            complete.v2_imports[0].request_id,
            prefix.v2_imports[0].request_id
        );
        assert_eq!(
            complete
                .v2_imports
                .iter()
                .map(|table| table.expected_revision)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn planner_rejects_cross_table_duplicate_paths_and_oversized_tables_before_io() {
        let duplicate = Path::new("/legacy/shared.parquet");
        let root = Path::new("/legacy");
        let (_master, duplicate_snapshot) = snapshot_with_tables(vec![
            (
                "first_events",
                vec![legacy_file_value(duplicate, Some(root), 1, 1, 1)],
            ),
            (
                "second_events",
                vec![legacy_file_value(duplicate, Some(root), 1, 1, 1)],
            ),
        ]);
        let error = plan_legacy_catalog_metadata_import(&duplicate_snapshot).unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);

        let files = (0..=super::MAX_LEGACY_FILES_PER_TABLE)
            .map(|index| {
                legacy_file_value(
                    &PathBuf::from(format!("/legacy/{index}.parquet")),
                    Some(root),
                    1,
                    index as u64,
                    1,
                )
            })
            .collect();
        let (_master, oversized_snapshot) = snapshot_with_tables(vec![("oversized_events", files)]);
        let error = plan_legacy_catalog_metadata_import(&oversized_snapshot).unwrap_err();
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn committed_v2_import_replays_without_physical_io_after_catalog_restart() {
        let root = TempDir::new().unwrap();
        let legacy_root = root.path().join("legacy");
        std::fs::create_dir(&legacy_root).unwrap();
        let file = legacy_root.join("events.parquet");
        write_parquet(&file, vec![1, 2, 3]);
        let (_master, snapshot) = snapshot_with_tables(vec![(
            "physical_events",
            vec![legacy_file_value(&file, Some(&legacy_root), 7, 11, 3)],
        )]);
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(plan.command_count(), 1);
        assert_eq!(plan.v2_import_count(), 1);
        let request_id = plan.v2_imports[0].request_id;

        let catalog = open_catalog(&root);
        let first = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap();
        assert_eq!(first.submitted_commands, 2);
        assert_eq!(first.replayed_commands, 0);
        assert_eq!(first.target_revision_after, 2);
        assert_eq!(
            first.tables[0].outcome,
            LegacyCatalogTableMigrationOutcome::PhysicalSubmitted
        );
        let imported = catalog
            .get_table(TableRef::from(plan.v2_imports[0].command.table_id))
            .await
            .unwrap();
        assert_eq!(imported.current_snapshot_version, 2);
        assert_eq!(
            imported.writer_epoch,
            plan.v2_imports[0].command.writer_epoch
        );
        catalog.close().unwrap();

        std::fs::remove_file(&file).unwrap();
        let replay_plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(replay_plan.v2_imports[0].request_id, request_id);
        let catalog = open_catalog(&root);
        let replay = execute_legacy_catalog_metadata_import(&replay_plan, &catalog)
            .await
            .unwrap();
        assert_eq!(replay.submitted_commands, 0);
        assert_eq!(replay.replayed_commands, 2);
        assert_eq!(replay.target_revision_before, 2);
        assert_eq!(replay.target_revision_after, 2);
        assert_eq!(
            replay.tables[0].outcome,
            LegacyCatalogTableMigrationOutcome::PhysicalReplayed
        );
        catalog.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_v2_replay_with_advanced_revision_fails_before_file_io() {
        let root = TempDir::new().unwrap();
        let legacy_root = root.path().join("legacy");
        std::fs::create_dir(&legacy_root).unwrap();
        let file = legacy_root.join("events.parquet");
        write_parquet(&file, vec![1, 2, 3]);
        let (_master, snapshot) = snapshot_with_tables(vec![(
            "physical_events",
            vec![legacy_file_value(&file, Some(&legacy_root), 7, 11, 3)],
        )]);
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(plan.command_count(), 1);
        assert_eq!(plan.v2_import_count(), 1);

        let catalog = open_catalog(&root);
        catalog
            .command(plan.commands[0].command.clone())
            .await
            .unwrap();
        let unrelated = CatalogCommandEnvelopeV1::new(
            RequestId::from_u128(99).unwrap(),
            Some(1),
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "migration-test".to_owned(),
            },
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: NamespaceId::from_u128(99).unwrap(),
                name: CatalogName::new("unrelated").unwrap(),
            }),
        )
        .unwrap();
        catalog
            .command(VersionedCatalogCommand::V1(unrelated))
            .await
            .unwrap();
        std::fs::remove_file(&file).unwrap();

        let error = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(catalog.revision().await.unwrap(), 2);
        catalog.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bad_file_stops_before_its_table_and_every_later_v2_table() {
        let root = TempDir::new().unwrap();
        let legacy_root = root.path().join("legacy");
        std::fs::create_dir(&legacy_root).unwrap();
        let mut tables = Vec::new();
        for index in 0..3 {
            let path = legacy_root.join(format!("{index}.parquet"));
            write_parquet(&path, vec![index]);
            tables.push((
                match index {
                    0 => "first_physical_events",
                    1 => "second_physical_events",
                    _ => "third_physical_events",
                },
                vec![legacy_file_value(
                    &path,
                    Some(&legacy_root),
                    1,
                    index as u64,
                    1,
                )],
            ));
        }
        let (_master, snapshot) = snapshot_with_tables(tables);
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
        assert_eq!(plan.v2_import_count(), 3);
        let bad_path = PathBuf::from(plan.v2_imports[1].file_specs[0].path.as_str());
        std::fs::remove_file(&bad_path).unwrap();
        let first_table = plan.v2_imports[0].command.table_id;
        let bad_table = plan.v2_imports[1].command.table_id;
        let later_table = plan.v2_imports[2].command.table_id;

        let catalog = open_catalog(&root);
        let error = execute_legacy_catalog_metadata_import(&plan, &catalog)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(catalog.revision().await.unwrap(), 2);
        assert!(catalog.get_table(TableRef::from(first_table)).await.is_ok());
        for table_id in [bad_table, later_table] {
            let error = catalog
                .get_table(TableRef::from(table_id))
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::NotFound);
        }
        catalog.close().unwrap();
    }
}
