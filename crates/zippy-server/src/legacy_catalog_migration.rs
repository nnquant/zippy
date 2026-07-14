//! Idempotent metadata-only import from an atomic legacy registry snapshot.
//!
//! Frozen V1 Catalog manifest commands accept only native append files. This module therefore
//! imports namespace, table, and schema metadata only when the legacy persisted-file view is
//! empty and an exact revision mapping can be recorded. Tables requiring legacy files or
//! nondeterministically regenerated schema IPC remain absent from SQLite and are reported as
//! blocked for a typed V2 import.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zippy_master::legacy_catalog::LegacyCatalogMigrationSnapshot;
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
    CatalogCommandV1, CatalogIdempotencyRead, CatalogOutcomeV1, CatalogRead, CatalogWrite,
    CreateNamespaceCommand, CreateTableCommand, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{CatalogName, TableState};
use zippy_types::{ErrorCode, RequestId, Result, ServerError, TableId};

/// Version of the metadata-only legacy Catalog import protocol.
pub const LEGACY_CATALOG_METADATA_MIGRATION_VERSION: u32 = 1;

/// SQLite Catalog revision required before the first import command.
pub const LEGACY_CATALOG_TARGET_REVISION_BASE: u64 = 0;

/// Initial empty snapshot assigned by frozen V1 `CreateTable`.
pub const LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE: u64 = 1;

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
    /// Preserve the original file discovery order in the query read DTO.
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
}

/// Dry-run disposition for one legacy table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyCatalogTablePlanStatus {
    /// Frozen V1 can create an exactly mapped empty table.
    EligibleMetadataV1,
    /// The table must remain absent until a typed V2 import is available.
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
    /// Whether frozen V1 can import this table metadata safely.
    pub status: LegacyCatalogTablePlanStatus,
    /// First mapped legacy revision, present only for an eligible empty table.
    pub legacy_revision_base: Option<u64>,
    /// Corresponding native snapshot, present only for an eligible empty table.
    pub native_snapshot_base: Option<u64>,
    /// Deterministic table command identity, absent for blocked tables.
    pub request_id: Option<RequestId>,
    /// Explicit capabilities required before a blocked table may become visible in SQLite.
    pub v2_requirements: Vec<LegacyCatalogV2ImportRequirement>,
    command_index: Option<usize>,
}

/// Immutable dry-run output and exact command sequence for one legacy snapshot.
#[derive(Debug, Clone)]
pub struct LegacyCatalogMetadataImportPlan {
    migration_version: u32,
    source_snapshot_hash: String,
    source_catalog_revision: u64,
    commands: Vec<PlannedCommand>,
    tables: Vec<LegacyCatalogTablePlan>,
}

impl LegacyCatalogMetadataImportPlan {
    /// Returns the importer protocol version frozen into every request identity.
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

    /// Returns whether every source table is representable by this V1 metadata import.
    #[must_use]
    pub fn is_complete_for_v1_metadata(&self) -> bool {
        self.tables
            .iter()
            .all(|table| table.status == LegacyCatalogTablePlanStatus::EligibleMetadataV1)
    }
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
    /// No SQLite table was created because V1 cannot represent the source safely.
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
    /// Final metadata import outcome.
    pub outcome: LegacyCatalogTableMigrationOutcome,
    /// Exact legacy revision base written to table config, when imported.
    pub legacy_revision_base: Option<u64>,
    /// Exact native snapshot base written to table config, when imported.
    pub native_snapshot_base: Option<u64>,
    /// Capabilities still required for a blocked table.
    pub v2_requirements: Vec<LegacyCatalogV2ImportRequirement>,
}

/// Auditable result of one complete metadata import attempt.
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
    /// Every source table is represented by the completed V1 metadata import.
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
    /// At least one legacy table needs the future typed V2 physical-file import protocol.
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
            message: Some(error.to_string()),
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

/// Builds a deterministic metadata-only import plan from one detached legacy snapshot.
///
/// The plan always starts at SQLite Catalog revision zero. Every request ID, expected revision,
/// command fingerprint, and table order is frozen by the source snapshot. A changed source must
/// produce a new migration attempt rather than silently extending a partially imported snapshot.
pub fn plan_legacy_catalog_metadata_import(
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
        });
    }

    Ok(LegacyCatalogMetadataImportPlan {
        migration_version: LEGACY_CATALOG_METADATA_MIGRATION_VERSION,
        source_snapshot_hash: snapshot.source_snapshot_hash().to_owned(),
        source_catalog_revision: snapshot.source_catalog_revision(),
        commands,
        tables: table_plans,
    })
}

/// Executes an immutable metadata plan through Catalog idempotency and revision guards.
///
/// This function owns no legacy handle and cannot mutate or stop the legacy runtime. Callers may
/// treat any returned error as shadow degradation while keeping the legacy authority available.
pub async fn execute_legacy_catalog_metadata_import<C>(
    plan: &LegacyCatalogMetadataImportPlan,
    catalog: &C,
) -> Result<LegacyCatalogMetadataMigrationReport>
where
    C: CatalogRead + CatalogWrite + CatalogIdempotencyRead + ?Sized,
{
    let result = execute_import(plan, catalog).await;
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
            error = %error,
            "legacy catalog metadata import failed without changing legacy authority"
        ),
    }
    result
}

async fn execute_import<C>(
    plan: &LegacyCatalogMetadataImportPlan,
    catalog: &C,
) -> Result<LegacyCatalogMetadataMigrationReport>
where
    C: CatalogRead + CatalogWrite + CatalogIdempotencyRead + ?Sized,
{
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
        .count();
    let submitted_commands = dispositions.len() - replayed_commands;
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
                None => LegacyCatalogTableMigrationOutcome::BlockedRequiresV2,
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
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;
    use tempfile::TempDir;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_core::{canonical_schema_hash, schema_metadata, OwnedDataDirectory};
    use zippy_master::registry::Registry;
    use zippy_master::server::MasterServer;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
        CatalogWrite, CreateNamespaceCommand, ListTablesRequest, VersionedCatalogCommand,
    };
    use zippy_types::table::{CatalogName, TableRef};
    use zippy_types::{ErrorCode, NamespaceId, RequestId};

    use super::{
        execute_legacy_catalog_metadata_import, plan_legacy_catalog_metadata_import,
        LegacyCatalogCommandDisposition, LegacyCatalogTableMigrationOutcome,
        LegacyCatalogTablePlanStatus, LegacyCatalogV2ImportRequirement,
        LEGACY_EMPTY_NATIVE_SNAPSHOT_BASE, LEGACY_REVISION_BASE_CONFIG_KEY,
        LEGACY_STREAM_NAME_CONFIG_KEY, NATIVE_SNAPSHOT_BASE_CONFIG_KEY,
    };

    fn catalog(directory: &TempDir) -> SqliteCatalog {
        let ownership = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(directory.path().join("data")).unwrap(),
        );
        SqliteCatalog::open(SqliteCatalogConfig::new(&ownership)).unwrap()
    }

    fn add_append_table(registry: &mut Registry, stream_name: &str, custom_metadata: bool) {
        let metadata = custom_metadata
            .then(|| HashMap::from([("unit".to_owned(), "ticks".to_owned())]))
            .unwrap_or_default();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )
        .with_metadata(metadata)]));
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
                    vec![json!({"file_path": "/legacy/events.parquet", "row_count": 2})],
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
    async fn imports_only_exact_empty_tables_and_replays_without_advancing_revision() {
        let (master, snapshot) = migration_snapshot();
        let plan = plan_legacy_catalog_metadata_import(&snapshot).unwrap();
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
        assert_eq!(
            metadata.v2_requirements,
            vec![LegacyCatalogV2ImportRequirement::DeterministicSchemaIpc]
        );

        let directory = TempDir::new().unwrap();
        let catalog = catalog(&directory);
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
        let catalog = catalog(&directory);
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
        let catalog = catalog(&directory);
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
}
