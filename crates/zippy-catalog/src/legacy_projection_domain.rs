//! SQLite domain logic for durable V4 legacy catalog projection commands.

use std::collections::BTreeMap;
use std::mem::size_of;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use zippy_types::catalog::{
    compute_legacy_catalog_projection_digest_v4, compute_legacy_catalog_source_metadata_hash_v4,
    CatalogActorKind, CreateTableCommand, LegacyCatalogAdoptExistingTableCommandV4,
    LegacyCatalogAdvanceCheckpointCommandV4, LegacyCatalogBootstrapTableCommandV4,
    LegacyCatalogCheckpointModeV4, LegacyCatalogDropTableCommandV4,
    LegacyCatalogProjectionCheckpointV4, LegacyCatalogProjectionCodecV4,
    LegacyCatalogProjectionCommandV4, LegacyCatalogProjectionCompletenessV4,
    LegacyCatalogProjectionDigestTableV4, LegacyCatalogProjectionEnvelopeV4,
    LegacyCatalogProjectionOutcomeV4, LegacyCatalogProjectionPhaseV4,
    LegacyCatalogProjectionResponseV4, LegacyCatalogProjectionTableResultV4,
    LegacyCatalogProjectionTableStateV4, LegacyCatalogSourceV4,
    LegacyCatalogSyncTableMetadataCommandV4, LegacyCatalogTableProjectionV4, LegacyCatalogValueV4,
    LegacyFileViewSnapshotMapping, LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4,
    MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4, MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4,
};
use zippy_types::table::{CatalogName, MetadataMap, MetadataScalar, TableDescriptor, TableState};
use zippy_types::{ErrorCode, NamespaceId, RequestId, Result, ServerError, TableId};

use crate::encoding::{decode_u64, encode_u64};
use crate::legacy_import_domain::{prepare_verified_files, PreparedLegacyFile, MAX_IMPORT_FILES};
use crate::storage_codec::{
    decode_table_config_blob, decode_table_definition_blob, encode_snapshot_summary_blob,
    encode_table_config_blob, encode_table_definition_blob,
};

const FORMAT_VERSION: i64 = LEGACY_CATALOG_PROJECTION_FORMAT_VERSION_V4 as i64;
const INITIAL_SCHEMA_VERSION: u64 = 1;
const INITIAL_SNAPSHOT_VERSION: u64 = 1;
const IMPORT_SNAPSHOT_VERSION: u64 = 2;
const INITIAL_PROJECTION_REVISION: u64 = 1;
const MAX_TABLE_CONFIG_ENTRIES: usize = 4_096;
const MAX_TABLE_CONFIG_TEXT_BYTES: usize = 1024 * 1024;
const MAX_PROJECTION_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MIGRATION_CONFIG_PREFIX: &str = "zippy.migration.";

/// A V4 command whose expensive schema, file, and source encoding work is complete.
pub(crate) struct PreparedLegacyCatalogProjectionV4 {
    envelope: LegacyCatalogProjectionEnvelopeV4,
    command: PreparedProjectionCommand,
}

/// A complete V4 envelope verified before the writer-side permanent replay probe.
#[derive(Debug)]
pub(crate) struct VerifiedLegacyCatalogProjectionV4 {
    envelope: LegacyCatalogProjectionEnvelopeV4,
}

impl VerifiedLegacyCatalogProjectionV4 {
    /// Returns the request identity used to attribute worker failures.
    pub(crate) const fn request_id(&self) -> RequestId {
        self.envelope.request_id
    }
}

/// Writer-side result of probing a verified V4 command before expensive preparation.
#[derive(Debug)]
pub(crate) enum LegacyCatalogProjectionStartV4 {
    /// The permanent exact response is already committed.
    Replay(LegacyCatalogProjectionResponseV4),
    /// This first execution still needs schema, file, and source preparation.
    Prepare(VerifiedLegacyCatalogProjectionV4),
}

enum PreparedProjectionCommand {
    Bootstrap(PreparedBootstrap),
    Adopt(PreparedAdopt),
    Sync(PreparedSync),
    Drop(LegacyCatalogDropTableCommandV4),
    Checkpoint(LegacyCatalogAdvanceCheckpointCommandV4),
}

struct PreparedBootstrap {
    command: LegacyCatalogBootstrapTableCommandV4,
    table: crate::table_domain::PreparedCreateTable,
    files: Vec<PreparedLegacyFile>,
    summary_blob: Vec<u8>,
    sources: PreparedSources,
}

struct PreparedAdopt {
    command: LegacyCatalogAdoptExistingTableCommandV4,
    files: Vec<PreparedLegacyFile>,
    sources: PreparedSources,
}

struct PreparedSync {
    command: LegacyCatalogSyncTableMetadataCommandV4,
    sources: PreparedSources,
}

struct PreparedSources {
    values: Vec<LegacyCatalogSourceV4>,
    config_blobs: Vec<Vec<u8>>,
    hash: String,
}

#[derive(Debug)]
struct CurrentTable {
    descriptor: TableDescriptor,
    schema_hash: String,
}

/// Verifies a frozen V4 envelope without consulting mutable physical or SQLite state.
pub(crate) fn verify_legacy_catalog_projection_v4(
    envelope: LegacyCatalogProjectionEnvelopeV4,
) -> Result<VerifiedLegacyCatalogProjectionV4> {
    let request_id = envelope.request_id;
    envelope.verify_fingerprint().map_err(|error| {
        invalid(
            request_id,
            format!("invalid legacy Catalog V4 projection fingerprint error=[{error}]"),
        )
    })?;
    validate_hash(
        request_id,
        "source projection hash",
        &envelope.source_projection_hash,
    )?;
    match &envelope.command {
        LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
            require_actor(request_id, envelope.actor.kind, CatalogActorKind::Migration)?;
            validate_hash(
                request_id,
                "canonical Schema hash",
                &command.canonical_schema_hash,
            )?;
            validate_table_config(request_id, &command.config)?;
            if !command.files.is_empty() && command.legacy_files_revision == 0 {
                return Err(invalid(
                    request_id,
                    "nonempty legacy V4 bootstrap requires a nonzero file revision",
                ));
            }
        }
        LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
            require_actor(request_id, envelope.actor.kind, CatalogActorKind::Migration)?;
            validate_hash(
                request_id,
                "expected Schema hash",
                &command.expected_schema_hash,
            )?;
            validate_table_config(request_id, &command.config)?;
        }
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
            require_actor(request_id, envelope.actor.kind, CatalogActorKind::Server)?;
            validate_table_cas_hashes(
                request_id,
                &command.expected_schema_hash,
                &command.expected_source_metadata_hash,
            )?;
            if command.writer_epoch < command.expected_writer_epoch {
                return Err(invalid(
                    request_id,
                    "legacy V4 table writer epoch cannot move backwards",
                ));
            }
        }
        LegacyCatalogProjectionCommandV4::DropTable(command) => {
            require_actor(request_id, envelope.actor.kind, CatalogActorKind::Server)?;
            validate_hash(
                request_id,
                "expected Schema hash",
                &command.expected_schema_hash,
            )?;
            if let Some(expected_source_metadata_hash) =
                command.expected_source_metadata_hash.as_deref()
            {
                validate_hash(
                    request_id,
                    "expected source metadata hash",
                    expected_source_metadata_hash,
                )?;
            }
        }
        LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(command) => {
            require_actor(request_id, envelope.actor.kind, CatalogActorKind::Server)?;
            validate_checkpoint_hashes(request_id, command)?;
        }
    }
    Ok(VerifiedLegacyCatalogProjectionV4 { envelope })
}

/// Probes permanent V4 replay on the writer before any expensive command preparation.
pub(crate) fn begin_verified_legacy_catalog_projection_v4(
    connection: &Connection,
    verified: VerifiedLegacyCatalogProjectionV4,
) -> Result<LegacyCatalogProjectionStartV4> {
    if let Some(response) = replay(connection, &verified.envelope)? {
        Ok(LegacyCatalogProjectionStartV4::Replay(response))
    } else {
        Ok(LegacyCatalogProjectionStartV4::Prepare(verified))
    }
}

/// Performs expensive V4 schema, file, and source preparation after replay misses.
pub(crate) fn prepare_verified_legacy_catalog_projection_v4(
    verified: VerifiedLegacyCatalogProjectionV4,
) -> Result<PreparedLegacyCatalogProjectionV4> {
    let envelope = verified.envelope;
    let request_id = envelope.request_id;
    let command = match envelope.command.clone() {
        LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
            PreparedProjectionCommand::Bootstrap(prepare_bootstrap(request_id, command)?)
        }
        LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
            PreparedProjectionCommand::Adopt(prepare_adopt(request_id, command)?)
        }
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
            PreparedProjectionCommand::Sync(prepare_sync(request_id, command)?)
        }
        LegacyCatalogProjectionCommandV4::DropTable(command) => {
            PreparedProjectionCommand::Drop(command)
        }
        LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(command) => {
            PreparedProjectionCommand::Checkpoint(command)
        }
    };
    Ok(PreparedLegacyCatalogProjectionV4 { envelope, command })
}

/// Convenience preparation entry for direct tests; service code should use the replay split.
#[cfg(test)]
pub(crate) fn prepare_legacy_catalog_projection_v4(
    envelope: LegacyCatalogProjectionEnvelopeV4,
) -> Result<PreparedLegacyCatalogProjectionV4> {
    let verified = verify_legacy_catalog_projection_v4(envelope)?;
    prepare_verified_legacy_catalog_projection_v4(verified)
}

fn prepare_bootstrap(
    request_id: RequestId,
    command: LegacyCatalogBootstrapTableCommandV4,
) -> Result<PreparedBootstrap> {
    validate_hash(
        request_id,
        "canonical Schema hash",
        &command.canonical_schema_hash,
    )?;
    validate_table_config(request_id, &command.config)?;
    if command.files.len() > MAX_IMPORT_FILES {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy V4 bootstrap file count exceeds maximum actual=[{}] maximum=[{}]",
                command.files.len(),
                MAX_IMPORT_FILES
            ),
        ));
    }
    if !command.files.is_empty() && command.legacy_files_revision == 0 {
        return Err(invalid(
            request_id,
            "nonempty legacy V4 bootstrap requires a nonzero file revision",
        ));
    }
    let table = crate::table_domain::prepare_create_table(CreateTableCommand {
        table_id: command.table_id,
        namespace_id: command.namespace_id,
        name: command.name.clone(),
        definition: command.definition.clone(),
        schema_ipc: command.schema_ipc.clone(),
        config: command.config.clone(),
    })
    .map_err(|error| error.with_request_id(request_id))?;
    if table.schema_hash() != command.canonical_schema_hash {
        return Err(ServerError::new(
            ErrorCode::SchemaMismatch,
            "legacy V4 bootstrap canonical Schema hash does not match Schema IPC",
        )
        .with_request_id(request_id)
        .with_table_id(command.table_id)
        .with_retryable(false));
    }
    let files = prepare_verified_files(
        request_id,
        command.table_id,
        INITIAL_SCHEMA_VERSION,
        &command.canonical_schema_hash,
        &command.files,
        true,
    )?;
    let summary_blob = encode_snapshot_summary_blob(&file_summary(
        request_id,
        &files,
        command.legacy_files_revision,
    )?)
    .map_err(|error| error.with_request_id(request_id))?;
    let sources = prepare_sources(request_id, &command.sources)?;
    Ok(PreparedBootstrap {
        command,
        table,
        files,
        summary_blob,
        sources,
    })
}

fn prepare_adopt(
    request_id: RequestId,
    command: LegacyCatalogAdoptExistingTableCommandV4,
) -> Result<PreparedAdopt> {
    validate_hash(
        request_id,
        "expected Schema hash",
        &command.expected_schema_hash,
    )?;
    validate_table_config(request_id, &command.config)?;
    encode_table_definition_blob(&command.definition)
        .and_then(|_| encode_table_config_blob(&command.config))
        .map_err(|error| error.with_request_id(request_id))?;
    let files = prepare_verified_files(
        request_id,
        command.table_id,
        command.expected_schema_version,
        &command.expected_schema_hash,
        &command.files,
        true,
    )?;
    let sources = prepare_sources(request_id, &command.sources)?;
    Ok(PreparedAdopt {
        command,
        files,
        sources,
    })
}

fn prepare_sync(
    request_id: RequestId,
    command: LegacyCatalogSyncTableMetadataCommandV4,
) -> Result<PreparedSync> {
    validate_table_cas_hashes(
        request_id,
        &command.expected_schema_hash,
        &command.expected_source_metadata_hash,
    )?;
    if command.writer_epoch < command.expected_writer_epoch {
        return Err(invalid(
            request_id,
            "legacy V4 table writer epoch cannot move backwards",
        ));
    }
    let sources = prepare_sources(request_id, &command.sources)?;
    Ok(PreparedSync { command, sources })
}

fn prepare_sources(
    request_id: RequestId,
    sources: &[LegacyCatalogSourceV4],
) -> Result<PreparedSources> {
    let hash = compute_legacy_catalog_source_metadata_hash_v4(sources).map_err(|error| {
        invalid(
            request_id,
            format!("invalid legacy V4 stable source set error=[{error}]"),
        )
    })?;
    let config_blobs = sources
        .iter()
        .map(|source| {
            LegacyCatalogProjectionCodecV4::encode_source_config(&source.config).map_err(|error| {
                invalid(
                    request_id,
                    format!("encode legacy V4 source configuration error=[{error}]"),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedSources {
        values: sources.to_vec(),
        config_blobs,
        hash,
    })
}

/// Executes one prepared V4 mutation atomically, or returns its permanent exact replay.
pub(crate) fn execute_legacy_catalog_projection_v4(
    connection: &mut Connection,
    prepared: PreparedLegacyCatalogProjectionV4,
) -> Result<LegacyCatalogProjectionResponseV4> {
    let request_id = prepared.envelope.request_id;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            crate::map_sqlite_error("begin legacy Catalog V4 projection", error)
                .with_request_id(request_id)
        })?;
    if let Some(response) = replay(&transaction, &prepared.envelope)? {
        transaction.commit().map_err(|error| {
            crate::map_sqlite_error("commit legacy Catalog V4 replay", error)
                .with_request_id(request_id)
        })?;
        return Ok(response);
    }

    let checkpoint = read_checkpoint(&transaction)?;
    if checkpoint.phase == LegacyCatalogProjectionPhaseV4::NativeAuthoritative {
        return Err(conflict(
            request_id,
            "legacy V4 writes are forbidden after native authority",
        ));
    }
    if checkpoint.shadow_epoch != prepared.envelope.expected_shadow_epoch {
        return Err(conflict(
            request_id,
            format!(
                "legacy V4 shadow epoch changed expected=[{}] actual=[{}]",
                prepared.envelope.expected_shadow_epoch, checkpoint.shadow_epoch
            ),
        ));
    }
    if !matches!(&prepared.command, PreparedProjectionCommand::Checkpoint(_)) {
        if prepared.envelope.source_catalog_revision < checkpoint.source_catalog_revision {
            return Err(conflict(
                request_id,
                "legacy V4 table mutation is older than the durable source checkpoint",
            ));
        }
        if prepared.envelope.source_catalog_revision == checkpoint.source_catalog_revision
            && prepared.envelope.source_projection_hash != checkpoint.source_projection_hash
        {
            return Err(conflict(
                request_id,
                "legacy V4 table mutation changed the projection hash at one source revision",
            ));
        }
    }
    let current_catalog_revision =
        crate::domain::revision(&transaction).map_err(|error| error.with_request_id(request_id))?;
    let next_catalog_revision = current_catalog_revision.checked_add(1).ok_or_else(|| {
        resource_exhausted(
            request_id,
            "Catalog revision overflow during legacy V4 projection",
        )
    })?;
    let committed_at_ns =
        crate::domain::unix_time_ns().map_err(|error| error.with_request_id(request_id))?;
    let outcome = match prepared.command {
        PreparedProjectionCommand::Bootstrap(command) => {
            LegacyCatalogProjectionOutcomeV4::BootstrapTable(commit_bootstrap(
                &transaction,
                &prepared.envelope,
                command,
                next_catalog_revision,
                committed_at_ns,
            )?)
        }
        PreparedProjectionCommand::Adopt(command) => {
            LegacyCatalogProjectionOutcomeV4::AdoptExistingTable(commit_adopt(
                &transaction,
                &prepared.envelope,
                command,
                committed_at_ns,
            )?)
        }
        PreparedProjectionCommand::Sync(command) => {
            LegacyCatalogProjectionOutcomeV4::SyncTableMetadata(commit_sync(
                &transaction,
                request_id,
                command,
                committed_at_ns,
            )?)
        }
        PreparedProjectionCommand::Drop(command) => LegacyCatalogProjectionOutcomeV4::DropTable(
            commit_drop(&transaction, request_id, command, committed_at_ns)?,
        ),
        PreparedProjectionCommand::Checkpoint(command) => {
            LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(commit_checkpoint(
                &transaction,
                &prepared.envelope,
                &checkpoint,
                command,
                committed_at_ns,
            )?)
        }
    };
    if !matches!(
        outcome,
        LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(_)
    ) {
        downgrade_ready_after_table_mutation(&transaction, committed_at_ns)?;
    }
    let response = LegacyCatalogProjectionResponseV4 {
        request_id,
        catalog_revision: next_catalog_revision,
        outcome,
    };
    let response_blob =
        LegacyCatalogProjectionCodecV4::encode_response(&response).map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("encode frozen legacy Catalog V4 response error=[{error}]"),
            )
            .with_request_id(request_id)
        })?;
    if response_blob.is_empty() || response_blob.len() > MAX_PROJECTION_RESPONSE_BYTES {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy Catalog V4 response exceeds byte limit actual=[{}] maximum=[{}]",
                response_blob.len(),
                MAX_PROJECTION_RESPONSE_BYTES
            ),
        ));
    }
    crate::domain::update_revision(&transaction, next_catalog_revision)
        .map_err(|error| error.with_request_id(request_id))?;
    let fingerprint = prepared.envelope.canonical_fingerprint.into_bytes();
    crate::request_ledger::insert_owner(
        &transaction,
        request_id,
        FORMAT_VERSION,
        operation(&prepared.envelope.command),
        &fingerprint,
        committed_at_ns,
    )?;
    transaction
        .execute(
            "INSERT INTO legacy_projection_requests(request_id, response_blob) VALUES (?1, ?2)",
            params![request_id.into_bytes().as_slice(), response_blob],
        )
        .map_err(|error| {
            crate::map_sqlite_error("persist legacy Catalog V4 permanent response", error)
                .with_request_id(request_id)
        })?;
    transaction.commit().map_err(|error| {
        crate::map_sqlite_error("commit legacy Catalog V4 projection", error)
            .with_request_id(request_id)
    })?;
    Ok(response)
}

fn commit_bootstrap(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
    prepared: PreparedBootstrap,
    catalog_revision: u64,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionTableResultV4> {
    let request_id = envelope.request_id;
    let table_id = prepared.command.table_id;
    let descriptor = crate::table_domain::create_table(
        connection,
        prepared.table,
        catalog_revision,
        committed_at_ns,
    )
    .map_err(|error| error.with_request_id(request_id))?;
    let expected_native_snapshot = if prepared.files.is_empty() {
        INITIAL_SNAPSHOT_VERSION
    } else {
        IMPORT_SNAPSHOT_VERSION
    };
    let expected_migration_version = if prepared.files.is_empty() { 1 } else { 2 };
    let migration = crate::legacy_reconcile_domain::validate_migration_config(&descriptor)
        .map_err(|error| error.with_request_id(request_id))?;
    if migration
        != (
            expected_migration_version,
            prepared.command.legacy_files_revision,
            expected_native_snapshot,
        )
    {
        return Err(invalid(
            request_id,
            "legacy V4 bootstrap migration base configuration does not match its file view",
        ));
    }
    if prepared.files.is_empty() {
        update_table_writer_epoch(
            connection,
            table_id,
            prepared.command.writer_epoch,
            committed_at_ns,
        )?;
    } else {
        crate::legacy_import_domain::ensure_file_identities_available(
            connection,
            request_id,
            &prepared.files,
        )?;
        crate::legacy_import_domain::insert_import_snapshot(
            connection,
            table_id,
            catalog_revision,
            committed_at_ns,
            &prepared.summary_blob,
        )?;
        crate::legacy_import_domain::insert_legacy_files(connection, table_id, &prepared.files)?;
        crate::legacy_import_domain::update_imported_table(
            connection,
            table_id,
            prepared.command.writer_epoch,
            committed_at_ns,
        )?;
    }
    insert_v4_base_mapping(
        connection,
        envelope,
        table_id,
        prepared.command.legacy_files_revision,
        expected_native_snapshot,
        catalog_revision,
        committed_at_ns,
    )?;
    crate::legacy_reconcile_domain::insert_snapshot_order(
        connection,
        table_id,
        expected_native_snapshot,
        &prepared.files,
    )?;
    insert_projection(
        connection,
        table_id,
        INITIAL_PROJECTION_REVISION,
        INITIAL_SCHEMA_VERSION,
        &prepared.command.canonical_schema_hash,
        expected_native_snapshot,
        prepared.command.legacy_files_revision,
        prepared.command.writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Complete,
        LegacyCatalogProjectionTableStateV4::Active,
        Some(&prepared.sources.hash),
        committed_at_ns,
    )?;
    replace_sources(connection, table_id, &prepared.sources)?;
    Ok(table_result(
        table_id,
        INITIAL_PROJECTION_REVISION,
        expected_native_snapshot,
        prepared.command.legacy_files_revision,
        prepared.command.writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Complete,
        LegacyCatalogProjectionTableStateV4::Active,
    ))
}

fn commit_adopt(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
    prepared: PreparedAdopt,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionTableResultV4> {
    let request_id = envelope.request_id;
    let command = &prepared.command;
    let table = load_current_table(connection, command.table_id)?;
    validate_adoption_table(request_id, command, &table)?;

    let (current_mapping, created_v1_base_mapping) =
        match crate::legacy_reconcile_domain::read_legacy_file_view_mapping(
            connection,
            command.table_id,
            command.expected_legacy_files_revision,
        )? {
            Some(mapping) => (mapping, false),
            None => {
                let migration =
                    crate::legacy_reconcile_domain::validate_migration_config(&table.descriptor)
                        .map_err(|error| error.with_request_id(request_id))?;
                if migration
                    != (
                        1,
                        command.expected_legacy_files_revision,
                        INITIAL_SNAPSHOT_VERSION,
                    )
                    || command.expected_snapshot_version != INITIAL_SNAPSHOT_VERSION
                    || !prepared.files.is_empty()
                {
                    return Err(conflict(
                        request_id,
                        "legacy V4 adoption may create only a validated empty V1 base mapping",
                    ));
                }
                crate::legacy_reconcile_domain::ensure_base_mapping(
                    connection,
                    &table.descriptor,
                    command.expected_legacy_files_revision,
                )?;
                let mapping = crate::legacy_reconcile_domain::read_legacy_file_view_mapping(
                    connection,
                    command.table_id,
                    command.expected_legacy_files_revision,
                )?
                .ok_or_else(|| {
                    corruption("legacy V4 adoption failed to create its validated V1 base mapping")
                        .with_request_id(request_id)
                })?;
                (mapping, true)
            }
        };
    if current_mapping.native_snapshot_version != command.expected_snapshot_version {
        return Err(conflict(
            request_id,
            "legacy V4 adoption mapping does not match the current native snapshot",
        ));
    }
    crate::legacy_reconcile_domain::validate_exact_current_legacy_view(
        connection,
        request_id,
        command.table_id,
        command.expected_snapshot_version,
        &prepared.files,
    )?;

    let next_projection_revision = match read_table_projection(connection, command.table_id)? {
        None if created_v1_base_mapping => INITIAL_PROJECTION_REVISION,
        None => {
            return Err(
                corruption("existing legacy mapping is missing its V7 seeded projection")
                    .with_request_id(request_id),
            );
        }
        Some(projection) => {
            if projection.state != LegacyCatalogProjectionTableStateV4::Active
                || projection.completeness != LegacyCatalogProjectionCompletenessV4::Seeded
            {
                return Err(conflict(
                    request_id,
                    "legacy V4 adoption requires an unadopted active seeded projection",
                ));
            }
            validate_projection_matches_table(&projection, &table, &current_mapping)?;
            projection
                .projection_revision
                .checked_add(1)
                .ok_or_else(|| {
                    resource_exhausted(request_id, "legacy table projection revision overflow")
                })?
        }
    };
    upsert_complete_projection(
        connection,
        command.table_id,
        next_projection_revision,
        command.expected_schema_version,
        &command.expected_schema_hash,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        &prepared.sources.hash,
        committed_at_ns,
    )?;
    replace_sources(connection, command.table_id, &prepared.sources)?;
    Ok(table_result(
        command.table_id,
        next_projection_revision,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Complete,
        LegacyCatalogProjectionTableStateV4::Active,
    ))
}

fn commit_sync(
    connection: &Connection,
    request_id: RequestId,
    prepared: PreparedSync,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionTableResultV4> {
    let command = &prepared.command;
    let (table, projection, mapping) =
        load_and_validate_complete_active_projection(connection, request_id, command.table_id)?;
    validate_projection_cas(
        request_id,
        &table,
        &projection,
        &mapping,
        command.expected_projection_revision,
        command.expected_schema_version,
        &command.expected_schema_hash,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        Some(&command.expected_source_metadata_hash),
    )?;
    let next_projection_revision =
        projection
            .projection_revision
            .checked_add(1)
            .ok_or_else(|| {
                resource_exhausted(request_id, "legacy table projection revision overflow")
            })?;
    update_table_writer_epoch(
        connection,
        command.table_id,
        command.writer_epoch,
        committed_at_ns,
    )?;
    update_complete_projection(
        connection,
        command.table_id,
        next_projection_revision,
        command.writer_epoch,
        &prepared.sources.hash,
        committed_at_ns,
    )?;
    replace_sources(connection, command.table_id, &prepared.sources)?;
    Ok(table_result(
        command.table_id,
        next_projection_revision,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Complete,
        LegacyCatalogProjectionTableStateV4::Active,
    ))
}

fn commit_drop(
    connection: &Connection,
    request_id: RequestId,
    command: LegacyCatalogDropTableCommandV4,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionTableResultV4> {
    let Some(projection) = read_table_projection(connection, command.table_id)? else {
        return commit_unprojected_empty_v1_drop(connection, request_id, &command, committed_at_ns);
    };
    let table = load_current_table(connection, command.table_id)?;
    let (table, projection, mapping) =
        validate_loaded_active_projection(connection, request_id, table, projection)?;
    let expected_completeness = if command.expected_source_metadata_hash.is_some() {
        LegacyCatalogProjectionCompletenessV4::Complete
    } else {
        LegacyCatalogProjectionCompletenessV4::Seeded
    };
    if projection.completeness != expected_completeness {
        return Err(conflict(
            request_id,
            "legacy V4 drop source metadata expectation disagrees with projection completeness",
        ));
    }
    validate_projection_cas(
        request_id,
        &table,
        &projection,
        &mapping,
        command.expected_projection_revision,
        command.expected_schema_version,
        &command.expected_schema_hash,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        command.expected_source_metadata_hash.as_deref(),
    )?;
    let next_projection_revision =
        projection
            .projection_revision
            .checked_add(1)
            .ok_or_else(|| {
                resource_exhausted(request_id, "legacy table projection revision overflow")
            })?;
    let updated = connection
        .execute(
            "UPDATE tables SET state = 'dropped', updated_at_ns = ?1 \
             WHERE table_id = ?2 AND state = 'active'",
            params![
                encode_u64(committed_at_ns).as_slice(),
                command.table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("drop legacy V4 projected table", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 table drop affected an unexpected row count",
        ));
    }
    let updated = connection
        .execute(
            "UPDATE legacy_table_projections \
                SET projection_revision = ?1, state = 'dropped', updated_at_ns = ?2 \
              WHERE table_id = ?3 AND state = 'active' AND completeness = ?4",
            params![
                encode_u64(next_projection_revision).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                command.table_id.into_bytes().as_slice(),
                encode_completeness(projection.completeness),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("drop legacy V4 projection", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 projection drop affected an unexpected row count",
        ));
    }
    Ok(table_result(
        command.table_id,
        next_projection_revision,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        projection.completeness,
        LegacyCatalogProjectionTableStateV4::Dropped,
    ))
}

fn commit_unprojected_empty_v1_drop(
    connection: &Connection,
    request_id: RequestId,
    command: &LegacyCatalogDropTableCommandV4,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionTableResultV4> {
    if command.expected_projection_revision != 0 || command.expected_source_metadata_hash.is_some()
    {
        return Err(conflict(
            request_id,
            "legacy V4 unprojected drop requires projection revision zero and no source hash",
        ));
    }

    let table = load_current_table(connection, command.table_id)?;
    if table.descriptor.state != TableState::Active
        || table.descriptor.current_schema_version != command.expected_schema_version
        || table.schema_hash != command.expected_schema_hash
        || table.descriptor.current_snapshot_version != command.expected_snapshot_version
        || table.descriptor.writer_epoch != command.expected_writer_epoch
    {
        return Err(conflict(
            request_id,
            "legacy V4 unprojected drop table compare-and-swap state changed",
        ));
    }
    let migration = crate::legacy_reconcile_domain::validate_migration_config(&table.descriptor)
        .map_err(|error| error.with_request_id(request_id))?;
    if migration
        != (
            1,
            command.expected_legacy_files_revision,
            INITIAL_SNAPSHOT_VERSION,
        )
        || command.expected_snapshot_version != INITIAL_SNAPSHOT_VERSION
    {
        return Err(conflict(
            request_id,
            "legacy V4 unprojected drop requires an exact empty V1 migration base",
        ));
    }

    let history: (i64, i64, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT \
                (SELECT COUNT(*) FROM legacy_table_projections WHERE table_id = ?1), \
                (SELECT COUNT(*) FROM legacy_projection_sources WHERE table_id = ?1), \
                (SELECT COUNT(*) FROM legacy_file_view_mappings WHERE table_id = ?1), \
                (SELECT COUNT(*) FROM legacy_reconcile_snapshot_files WHERE table_id = ?1), \
                (SELECT COUNT(*) FROM data_files WHERE table_id = ?1), \
                (SELECT COUNT(*) FROM table_snapshots WHERE table_id = ?1)",
            [command.table_id.into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(|error| crate::map_sqlite_error("validate unprojected V1 drop history", error))?;
    if history != (0, 0, 0, 0, 0, 1) {
        return Err(conflict(
            request_id,
            "legacy V4 unprojected drop requires no projection, source, mapping, or file history",
        ));
    }

    crate::legacy_reconcile_domain::ensure_base_mapping(
        connection,
        &table.descriptor,
        command.expected_legacy_files_revision,
    )?;
    insert_projection(
        connection,
        command.table_id,
        INITIAL_PROJECTION_REVISION,
        command.expected_schema_version,
        &command.expected_schema_hash,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Seeded,
        LegacyCatalogProjectionTableStateV4::Dropped,
        None,
        committed_at_ns,
    )?;
    let updated = connection
        .execute(
            "UPDATE tables SET state = 'dropped', updated_at_ns = ?1 \
             WHERE table_id = ?2 AND state = 'active'",
            params![
                encode_u64(committed_at_ns).as_slice(),
                command.table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("drop unprojected V1 table", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 unprojected table drop affected an unexpected row count",
        ));
    }
    Ok(table_result(
        command.table_id,
        INITIAL_PROJECTION_REVISION,
        command.expected_snapshot_version,
        command.expected_legacy_files_revision,
        command.expected_writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Seeded,
        LegacyCatalogProjectionTableStateV4::Dropped,
    ))
}

fn commit_checkpoint(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
    current: &LegacyCatalogProjectionCheckpointV4,
    command: LegacyCatalogAdvanceCheckpointCommandV4,
    committed_at_ns: u64,
) -> Result<LegacyCatalogProjectionCheckpointV4> {
    let request_id = envelope.request_id;
    if command.phase == LegacyCatalogProjectionPhaseV4::NativeAuthoritative {
        return Err(conflict(
            request_id,
            "legacy V4 checkpoint cannot enter native authority",
        ));
    }
    if command.expected_checkpoint_generation != current.checkpoint_generation
        || command.expected_source_catalog_revision != current.source_catalog_revision
        || command.expected_source_projection_hash != current.source_projection_hash
        || command.expected_phase != current.phase
    {
        return Err(conflict(
            request_id,
            "legacy V4 checkpoint compare-and-swap state changed",
        ));
    }
    if phase_rank(command.phase) < phase_rank(current.phase) {
        return Err(conflict(
            request_id,
            "legacy V4 checkpoint phase cannot move backwards",
        ));
    }
    if command.target_projection_digest != envelope.source_projection_hash {
        return Err(conflict(
            request_id,
            "legacy V4 checkpoint target digest differs from the source projection hash",
        ));
    }
    let resolved_continuity_loss_epoch = match command.mode {
        LegacyCatalogCheckpointModeV4::Contiguous => {
            let expected_revision =
                current
                    .source_catalog_revision
                    .checked_add(1)
                    .ok_or_else(|| {
                        resource_exhausted(request_id, "legacy source Catalog revision overflow")
                    })?;
            if envelope.source_catalog_revision != expected_revision
                || command.shadow_epoch != current.shadow_epoch
                || command.observed_continuity_loss_epoch != current.resolved_continuity_loss_epoch
            {
                return Err(conflict(
                    request_id,
                    "legacy V4 contiguous checkpoint lacks exact revision and continuity",
                ));
            }
            current.resolved_continuity_loss_epoch
        }
        LegacyCatalogCheckpointModeV4::FullReconcile => {
            let maximum_shadow_epoch = current
                .shadow_epoch
                .checked_add(1)
                .ok_or_else(|| resource_exhausted(request_id, "legacy V4 shadow epoch overflow"))?;
            if envelope.source_catalog_revision < current.source_catalog_revision
                || !(current.shadow_epoch..=maximum_shadow_epoch).contains(&command.shadow_epoch)
                || command.observed_continuity_loss_epoch < current.resolved_continuity_loss_epoch
            {
                return Err(conflict(
                    request_id,
                    "legacy V4 full reconcile checkpoint does not monotonically resolve continuity",
                ));
            }
            command.observed_continuity_loss_epoch
        }
    };
    if command.phase == LegacyCatalogProjectionPhaseV4::ReadCutoverReady
        && command.mode != LegacyCatalogCheckpointModeV4::FullReconcile
    {
        return Err(conflict(
            request_id,
            "legacy V4 read-cutover readiness requires a full reconcile",
        ));
    }
    if matches!(
        command.phase,
        LegacyCatalogProjectionPhaseV4::FullShadow
            | LegacyCatalogProjectionPhaseV4::ReadCutoverReady
    ) {
        ensure_all_active_projections_complete(connection)?;
    }
    let projection_digest = compute_active_projection_digest(connection)?;
    if projection_digest != envelope.source_projection_hash
        || projection_digest != command.target_projection_digest
    {
        return Err(conflict(
            request_id,
            "legacy V4 checkpoint digest does not match the active SQLite projection",
        ));
    }
    let checkpoint_generation = current
        .checkpoint_generation
        .checked_add(1)
        .ok_or_else(|| {
            resource_exhausted(request_id, "legacy V4 checkpoint generation overflow")
        })?;
    let updated = connection
        .execute(
            "UPDATE legacy_projection_checkpoint \
                SET checkpoint_generation = ?1, shadow_epoch = ?2, phase = ?3, \
                    source_catalog_revision = ?4, source_projection_hash = ?5, \
                    projection_digest = ?6, continuity_mode = ?7, \
                    resolved_continuity_loss_epoch = ?8, updated_at_ns = ?9 \
              WHERE singleton = 1 AND checkpoint_generation = ?10",
            params![
                encode_u64(checkpoint_generation).as_slice(),
                encode_u64(command.shadow_epoch).as_slice(),
                encode_phase(command.phase),
                encode_u64(envelope.source_catalog_revision).as_slice(),
                envelope.source_projection_hash.as_str(),
                projection_digest.as_str(),
                encode_checkpoint_mode(command.mode),
                encode_u64(resolved_continuity_loss_epoch).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
                encode_u64(current.checkpoint_generation).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("advance legacy V4 checkpoint", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 checkpoint update affected an unexpected row count",
        ));
    }
    Ok(LegacyCatalogProjectionCheckpointV4 {
        checkpoint_generation,
        shadow_epoch: command.shadow_epoch,
        phase: command.phase,
        source_catalog_revision: envelope.source_catalog_revision,
        source_projection_hash: envelope.source_projection_hash.clone(),
        projection_digest,
        mode: command.mode,
        resolved_continuity_loss_epoch,
        updated_at_ns: committed_at_ns,
    })
}

/// Reads and validates the durable singleton V4 projection checkpoint.
pub(crate) fn read_checkpoint(
    connection: &Connection,
) -> Result<LegacyCatalogProjectionCheckpointV4> {
    let raw = connection
        .query_row(
            "SELECT checkpoint_generation, shadow_epoch, phase, source_catalog_revision, \
                    source_projection_hash, projection_digest, continuity_mode, \
                    resolved_continuity_loss_epoch, updated_at_ns \
               FROM legacy_projection_checkpoint WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Vec<u8>>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy V4 checkpoint", error))?
        .ok_or_else(|| corruption("legacy V4 projection checkpoint is missing"))?;
    let checkpoint = LegacyCatalogProjectionCheckpointV4 {
        checkpoint_generation: decode_u64(&raw.0)?,
        shadow_epoch: decode_u64(&raw.1)?,
        phase: decode_phase(&raw.2)?,
        source_catalog_revision: decode_u64(&raw.3)?,
        source_projection_hash: raw.4,
        projection_digest: raw.5,
        mode: decode_checkpoint_mode(&raw.6)?,
        resolved_continuity_loss_epoch: decode_u64(&raw.7)?,
        updated_at_ns: decode_u64(&raw.8)?,
    };
    validate_stored_hash(
        "checkpoint source projection hash",
        &checkpoint.source_projection_hash,
    )?;
    validate_stored_hash(
        "checkpoint projection digest",
        &checkpoint.projection_digest,
    )?;
    Ok(checkpoint)
}

/// Reads one complete durable V4 per-table projection, including dropped history.
pub(crate) fn read_table_projection(
    connection: &Connection,
    table_id: TableId,
) -> Result<Option<LegacyCatalogTableProjectionV4>> {
    let raw = connection
        .query_row(
            "SELECT table_id, projection_revision, schema_version, schema_hash, \
                    native_snapshot_version, legacy_files_revision, writer_epoch, \
                    completeness, state, source_metadata_hash, updated_at_ns \
               FROM legacy_table_projections WHERE table_id = ?1",
            [table_id.into_bytes().as_slice()],
            projection_row,
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy V4 table projection", error))?;
    raw.map(|raw| decode_projection(connection, raw))
        .transpose()
}

/// Lists all durable V4 table projections in strict identity order, including history.
pub(crate) fn list_table_projections(
    connection: &Connection,
) -> Result<Vec<LegacyCatalogTableProjectionV4>> {
    let mut statement = connection
        .prepare(
            "SELECT table_id, projection_revision, schema_version, schema_hash, \
                    native_snapshot_version, legacy_files_revision, writer_epoch, \
                    completeness, state, source_metadata_hash, updated_at_ns \
               FROM legacy_table_projections ORDER BY table_id",
        )
        .map_err(|error| crate::map_sqlite_error("prepare legacy V4 projection listing", error))?;
    let rows = statement
        .query_map([], projection_row)
        .map_err(|error| crate::map_sqlite_error("query legacy V4 projection listing", error))?;
    let mut projections = Vec::new();
    for row in rows {
        let raw =
            row.map_err(|error| crate::map_sqlite_error("read legacy V4 projection row", error))?;
        projections.push(decode_projection(connection, raw)?);
    }
    if projections
        .windows(2)
        .any(|pair| pair[0].table_id >= pair[1].table_id)
    {
        return Err(corruption(
            "legacy V4 projection listing is not strictly ordered by table identity",
        ));
    }
    Ok(projections)
}

/// Lists canonical digest inputs for active complete projections only.
pub(crate) fn list_active_digest_inputs(
    connection: &Connection,
) -> Result<Vec<LegacyCatalogProjectionDigestTableV4>> {
    validate_projection_lifecycle_consistency(connection)?;
    let mut statement = connection
        .prepare(
            "SELECT t.table_id, t.namespace_id, t.name, t.definition_blob, \
                    t.current_schema_version, s.schema_hash, t.config_blob, \
                    p.legacy_files_revision, t.writer_epoch, p.state \
               FROM tables AS t \
               JOIN table_schemas AS s \
                 ON s.table_id = t.table_id \
                AND s.schema_version = t.current_schema_version \
               JOIN legacy_table_projections AS p ON p.table_id = t.table_id \
              WHERE t.state = 'active' AND p.state = 'active' \
                AND p.completeness = 'complete' \
              ORDER BY t.table_id",
        )
        .map_err(|error| crate::map_sqlite_error("prepare legacy V4 digest input", error))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, Vec<u8>>(7)?,
                row.get::<_, Vec<u8>>(8)?,
                row.get::<_, String>(9)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("query legacy V4 digest input", error))?;
    let mut inputs = Vec::new();
    for row in rows {
        let raw =
            row.map_err(|error| crate::map_sqlite_error("read legacy V4 digest row", error))?;
        let table_id = decode_table_id(&raw.0)?;
        if raw.9 != "active" {
            return Err(corruption(
                "active legacy V4 digest row has an invalid state",
            ));
        }
        let projection = read_table_projection(connection, table_id)?
            .ok_or_else(|| corruption("legacy V4 digest row lost its projection"))?;
        let table = load_current_table(connection, table_id)?;
        let mapping = crate::legacy_reconcile_domain::read_current_legacy_file_view_mapping(
            connection, table_id,
        )?;
        validate_projection_matches_table(&projection, &table, &mapping)?;
        if projection.schema_version != decode_u64(&raw.4)?
            || projection.schema_hash != raw.5
            || projection.writer_epoch != decode_u64(&raw.8)?
        {
            return Err(corruption(
                "legacy V4 digest row disagrees with its projection cursor",
            ));
        }
        let mut config = decode_table_config_blob(&raw.6)?;
        config.retain(|key, _| !key.starts_with(MIGRATION_CONFIG_PREFIX));
        inputs.push(LegacyCatalogProjectionDigestTableV4 {
            table_id,
            namespace_id: decode_namespace_id(&raw.1)?,
            name: decode_catalog_name(&raw.2, "table")?,
            definition: decode_table_definition_blob(&raw.3)?,
            schema_version: projection.schema_version,
            schema_hash: projection.schema_hash,
            config,
            legacy_files_revision: decode_u64(&raw.7)?,
            writer_epoch: projection.writer_epoch,
            state: LegacyCatalogProjectionTableStateV4::Active,
            sources: projection.sources,
        });
    }
    if inputs
        .windows(2)
        .any(|pair| pair[0].table_id >= pair[1].table_id)
    {
        return Err(corruption(
            "legacy V4 digest inputs are not strictly ordered by table identity",
        ));
    }
    Ok(inputs)
}

/// Recomputes the canonical active complete projection digest from SQLite state.
pub(crate) fn compute_active_projection_digest(connection: &Connection) -> Result<String> {
    let inputs = list_active_digest_inputs(connection)?;
    compute_legacy_catalog_projection_digest_v4(&inputs).map_err(|error| {
        corruption(format!(
            "compute active legacy V4 projection digest error=[{error}]"
        ))
    })
}

type RawProjectionRow = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    String,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    String,
    String,
    Option<String>,
    Vec<u8>,
);

fn projection_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawProjectionRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    ))
}

fn decode_projection(
    connection: &Connection,
    raw: RawProjectionRow,
) -> Result<LegacyCatalogTableProjectionV4> {
    let table_id = decode_table_id(&raw.0)?;
    let completeness = decode_completeness(&raw.7)?;
    let state = decode_projection_state(&raw.8)?;
    let sources = load_sources(connection, table_id)?;
    match (completeness, raw.9.as_deref()) {
        (LegacyCatalogProjectionCompletenessV4::Seeded, None) if sources.is_empty() => {}
        (LegacyCatalogProjectionCompletenessV4::Complete, Some(hash)) => {
            validate_stored_hash("table source metadata hash", hash)?;
            let computed =
                compute_legacy_catalog_source_metadata_hash_v4(&sources).map_err(|error| {
                    corruption(format!(
                        "compute stored legacy V4 source metadata hash error=[{error}]"
                    ))
                })?;
            if computed != hash {
                return Err(corruption(
                    "legacy V4 source rows do not match their stored metadata hash",
                ));
            }
        }
        _ => {
            return Err(corruption(
                "legacy V4 projection completeness disagrees with its source metadata",
            ));
        }
    }
    validate_stored_hash("table Schema hash", &raw.3)?;
    Ok(LegacyCatalogTableProjectionV4 {
        table_id,
        projection_revision: decode_u64(&raw.1)?,
        schema_version: decode_u64(&raw.2)?,
        schema_hash: raw.3,
        native_snapshot_version: decode_u64(&raw.4)?,
        legacy_files_revision: decode_u64(&raw.5)?,
        writer_epoch: decode_u64(&raw.6)?,
        completeness,
        state,
        source_metadata_hash: raw.9,
        sources,
        updated_at_ns: decode_u64(&raw.10)?,
    })
}

fn load_sources(connection: &Connection, table_id: TableId) -> Result<Vec<LegacyCatalogSourceV4>> {
    let (source_count, encoded_bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(\
                    length(CAST(source_name AS BLOB)) \
                  + length(CAST(source_type AS BLOB)) \
                  + length(CAST(output_stream AS BLOB)) \
                  + length(config_blob) + 8\
                ), 0) \
               FROM legacy_projection_sources WHERE table_id = ?1",
            [table_id.into_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| crate::map_sqlite_error("bound stored legacy V4 sources", error))?;
    let source_count = usize::try_from(source_count)
        .map_err(|_| corruption("stored legacy V4 source count is invalid"))?;
    let encoded_bytes = usize::try_from(encoded_bytes)
        .map_err(|_| corruption("stored legacy V4 source byte count is invalid"))?;
    if source_count > MAX_LEGACY_CATALOG_PROJECTION_SOURCES_V4
        || encoded_bytes > MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4
    {
        return Err(corruption(
            "stored legacy V4 sources exceed their durable resource bounds",
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT source_name, source_type, output_stream, config_blob, writer_epoch \
               FROM legacy_projection_sources WHERE table_id = ?1 ORDER BY source_name",
        )
        .map_err(|error| crate::map_sqlite_error("prepare legacy V4 source read", error))?;
    let rows = statement
        .query_map([table_id.into_bytes().as_slice()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Vec<u8>>(4)?,
            ))
        })
        .map_err(|error| crate::map_sqlite_error("query legacy V4 sources", error))?;
    let mut sources = Vec::new();
    for row in rows {
        let (source_name, source_type, output_stream, config_blob, writer_epoch) =
            row.map_err(|error| crate::map_sqlite_error("read legacy V4 source row", error))?;
        let config = LegacyCatalogProjectionCodecV4::decode_source_config(&config_blob).map_err(
            |error| {
                corruption(format!(
                    "decode stored legacy V4 source configuration error=[{error}]"
                ))
            },
        )?;
        sources.push(LegacyCatalogSourceV4 {
            source_name,
            source_type,
            output_stream,
            config,
            writer_epoch: decode_u64(&writer_epoch)?,
        });
    }
    if sources
        .windows(2)
        .any(|pair| pair[0].source_name >= pair[1].source_name)
    {
        return Err(corruption(
            "legacy V4 stable sources are not strictly ordered by source name",
        ));
    }
    Ok(sources)
}

fn load_current_table(connection: &Connection, table_id: TableId) -> Result<CurrentTable> {
    let raw = connection
        .query_row(
            "SELECT t.table_id, t.namespace_id, t.name, t.definition_blob, \
                    t.current_schema_version, t.current_snapshot_version, t.writer_epoch, \
                    t.state, t.config_blob, t.created_at_ns, t.updated_at_ns, s.schema_hash \
               FROM tables AS t \
               JOIN table_schemas AS s \
                 ON s.table_id = t.table_id \
                AND s.schema_version = t.current_schema_version \
              WHERE t.table_id = ?1",
            [table_id.into_bytes().as_slice()],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, Vec<u8>>(9)?,
                    row.get::<_, Vec<u8>>(10)?,
                    row.get::<_, String>(11)?,
                ))
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read legacy V4 current table", error))?
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::NotFound,
                "legacy V4 projected table was not found",
            )
            .with_table_id(table_id)
            .with_retryable(false)
        })?;
    if decode_table_id(&raw.0)? != table_id {
        return Err(corruption(
            "legacy V4 table lookup returned another identity",
        ));
    }
    let schema_hash = raw.11;
    validate_stored_hash("current table Schema hash", &schema_hash)?;
    Ok(CurrentTable {
        descriptor: TableDescriptor {
            table_id,
            namespace_id: decode_namespace_id(&raw.1)?,
            name: decode_catalog_name(&raw.2, "table")?,
            definition: decode_table_definition_blob(&raw.3)?,
            current_schema_version: decode_u64(&raw.4)?,
            current_snapshot_version: decode_u64(&raw.5)?,
            writer_epoch: decode_u64(&raw.6)?,
            state: decode_table_state(&raw.7)?,
            config: decode_table_config_blob(&raw.8)?,
            created_at_ns: decode_u64(&raw.9)?,
            updated_at_ns: decode_u64(&raw.10)?,
        },
        schema_hash,
    })
}

fn validate_adoption_table(
    request_id: RequestId,
    command: &LegacyCatalogAdoptExistingTableCommandV4,
    current: &CurrentTable,
) -> Result<()> {
    let table = &current.descriptor;
    if table.state != TableState::Active {
        return Err(conflict(
            request_id,
            "legacy V4 adoption table is not active",
        ));
    }
    if table.table_id != command.table_id
        || table.namespace_id != command.namespace_id
        || table.name != command.name
        || table.definition != command.definition
        || table.config != command.config
        || table.current_schema_version != command.expected_schema_version
        || current.schema_hash != command.expected_schema_hash
        || table.current_snapshot_version != command.expected_snapshot_version
        || table.writer_epoch != command.expected_writer_epoch
    {
        return Err(conflict(
            request_id,
            "legacy V4 adoption descriptor or cursor does not match the existing table",
        ));
    }
    crate::legacy_reconcile_domain::validate_migration_config(table)
        .map_err(|error| error.with_request_id(request_id))?;
    Ok(())
}

fn load_and_validate_complete_active_projection(
    connection: &Connection,
    request_id: RequestId,
    table_id: TableId,
) -> Result<(
    CurrentTable,
    LegacyCatalogTableProjectionV4,
    LegacyFileViewSnapshotMapping,
)> {
    let (table, projection, mapping) =
        load_and_validate_active_projection(connection, request_id, table_id)?;
    if projection.completeness != LegacyCatalogProjectionCompletenessV4::Complete {
        return Err(conflict(
            request_id,
            "legacy V4 mutation requires a complete active projection",
        ));
    }
    Ok((table, projection, mapping))
}

fn load_and_validate_active_projection(
    connection: &Connection,
    request_id: RequestId,
    table_id: TableId,
) -> Result<(
    CurrentTable,
    LegacyCatalogTableProjectionV4,
    LegacyFileViewSnapshotMapping,
)> {
    let table = load_current_table(connection, table_id)?;
    let projection = read_table_projection(connection, table_id)?.ok_or_else(|| {
        conflict(
            request_id,
            "legacy V4 table has no durable projection state",
        )
    })?;
    validate_loaded_active_projection(connection, request_id, table, projection)
}

fn validate_loaded_active_projection(
    connection: &Connection,
    request_id: RequestId,
    table: CurrentTable,
    projection: LegacyCatalogTableProjectionV4,
) -> Result<(
    CurrentTable,
    LegacyCatalogTableProjectionV4,
    LegacyFileViewSnapshotMapping,
)> {
    if table.descriptor.state != TableState::Active {
        return Err(conflict(
            request_id,
            "legacy V4 projected table is not active",
        ));
    }
    if projection.state != LegacyCatalogProjectionTableStateV4::Active {
        return Err(conflict(
            request_id,
            "legacy V4 mutation requires an active projection",
        ));
    }
    let mapping = crate::legacy_reconcile_domain::read_current_legacy_file_view_mapping(
        connection,
        table.descriptor.table_id,
    )?;
    validate_projection_matches_table(&projection, &table, &mapping)?;
    Ok((table, projection, mapping))
}

fn validate_projection_matches_table(
    projection: &LegacyCatalogTableProjectionV4,
    table: &CurrentTable,
    mapping: &LegacyFileViewSnapshotMapping,
) -> Result<()> {
    if projection.table_id != table.descriptor.table_id
        || projection.schema_version != table.descriptor.current_schema_version
        || projection.schema_hash != table.schema_hash
        || projection.native_snapshot_version != table.descriptor.current_snapshot_version
        || projection.legacy_files_revision != mapping.legacy_files_revision
        || mapping.native_snapshot_version != table.descriptor.current_snapshot_version
        || projection.writer_epoch != table.descriptor.writer_epoch
    {
        return Err(corruption(
            "legacy V4 projection cursor disagrees with its table or file mapping",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_projection_cas(
    request_id: RequestId,
    table: &CurrentTable,
    projection: &LegacyCatalogTableProjectionV4,
    mapping: &LegacyFileViewSnapshotMapping,
    expected_projection_revision: u64,
    expected_schema_version: u64,
    expected_schema_hash: &str,
    expected_snapshot_version: u64,
    expected_legacy_files_revision: u64,
    expected_writer_epoch: u64,
    expected_source_metadata_hash: Option<&str>,
) -> Result<()> {
    validate_projection_matches_table(projection, table, mapping)?;
    if projection.projection_revision != expected_projection_revision
        || projection.schema_version != expected_schema_version
        || projection.schema_hash != expected_schema_hash
        || projection.native_snapshot_version != expected_snapshot_version
        || projection.legacy_files_revision != expected_legacy_files_revision
        || projection.writer_epoch != expected_writer_epoch
        || projection.source_metadata_hash.as_deref() != expected_source_metadata_hash
    {
        return Err(conflict(
            request_id,
            "legacy V4 table projection compare-and-swap state changed",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_projection(
    connection: &Connection,
    table_id: TableId,
    projection_revision: u64,
    schema_version: u64,
    schema_hash: &str,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    completeness: LegacyCatalogProjectionCompletenessV4,
    state: LegacyCatalogProjectionTableStateV4,
    source_metadata_hash: Option<&str>,
    updated_at_ns: u64,
) -> Result<()> {
    connection
        .execute(
            "INSERT INTO legacy_table_projections(\
                 table_id, projection_revision, schema_version, schema_hash, \
                 native_snapshot_version, legacy_files_revision, writer_epoch, \
                 completeness, state, source_metadata_hash, updated_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                table_id.into_bytes().as_slice(),
                encode_u64(projection_revision).as_slice(),
                encode_u64(schema_version).as_slice(),
                schema_hash,
                encode_u64(native_snapshot_version).as_slice(),
                encode_u64(legacy_files_revision).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                encode_completeness(completeness),
                encode_projection_state(state),
                source_metadata_hash,
                encode_u64(updated_at_ns).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("insert legacy V4 table projection", error))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn upsert_complete_projection(
    connection: &Connection,
    table_id: TableId,
    projection_revision: u64,
    schema_version: u64,
    schema_hash: &str,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    source_metadata_hash: &str,
    updated_at_ns: u64,
) -> Result<()> {
    let exists = read_table_projection(connection, table_id)?.is_some();
    if !exists {
        return insert_projection(
            connection,
            table_id,
            projection_revision,
            schema_version,
            schema_hash,
            native_snapshot_version,
            legacy_files_revision,
            writer_epoch,
            LegacyCatalogProjectionCompletenessV4::Complete,
            LegacyCatalogProjectionTableStateV4::Active,
            Some(source_metadata_hash),
            updated_at_ns,
        );
    }
    let updated = connection
        .execute(
            "UPDATE legacy_table_projections \
                SET projection_revision = ?1, schema_version = ?2, schema_hash = ?3, \
                    native_snapshot_version = ?4, legacy_files_revision = ?5, \
                    writer_epoch = ?6, completeness = 'complete', state = 'active', \
                    source_metadata_hash = ?7, updated_at_ns = ?8 \
              WHERE table_id = ?9 AND completeness = 'seeded' AND state = 'active'",
            params![
                encode_u64(projection_revision).as_slice(),
                encode_u64(schema_version).as_slice(),
                schema_hash,
                encode_u64(native_snapshot_version).as_slice(),
                encode_u64(legacy_files_revision).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                source_metadata_hash,
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("adopt legacy V4 table projection", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 projection adoption affected an unexpected row count",
        ));
    }
    Ok(())
}

fn update_complete_projection(
    connection: &Connection,
    table_id: TableId,
    projection_revision: u64,
    writer_epoch: u64,
    source_metadata_hash: &str,
    updated_at_ns: u64,
) -> Result<()> {
    let updated = connection
        .execute(
            "UPDATE legacy_table_projections \
                SET projection_revision = ?1, writer_epoch = ?2, \
                    source_metadata_hash = ?3, updated_at_ns = ?4 \
              WHERE table_id = ?5 AND completeness = 'complete' AND state = 'active'",
            params![
                encode_u64(projection_revision).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                source_metadata_hash,
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("sync legacy V4 table projection", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 projection sync affected an unexpected row count",
        ));
    }
    Ok(())
}

fn replace_sources(
    connection: &Connection,
    table_id: TableId,
    sources: &PreparedSources,
) -> Result<()> {
    if sources.values.len() != sources.config_blobs.len() {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "prepared legacy V4 source values and encodings disagree",
        ));
    }
    connection
        .execute(
            "DELETE FROM legacy_projection_sources WHERE table_id = ?1",
            [table_id.into_bytes().as_slice()],
        )
        .map_err(|error| crate::map_sqlite_error("replace legacy V4 sources", error))?;
    let mut insert = connection
        .prepare_cached(
            "INSERT INTO legacy_projection_sources(\
                 table_id, source_name, source_type, output_stream, config_blob, writer_epoch\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .map_err(|error| crate::map_sqlite_error("prepare legacy V4 source insert", error))?;
    for (source, config_blob) in sources.values.iter().zip(&sources.config_blobs) {
        insert
            .execute(params![
                table_id.into_bytes().as_slice(),
                source.source_name.as_str(),
                source.source_type.as_str(),
                source.output_stream.as_str(),
                config_blob,
                encode_u64(source.writer_epoch).as_slice(),
            ])
            .map_err(|error| crate::map_sqlite_error("insert legacy V4 source", error))?;
    }
    Ok(())
}

fn update_table_writer_epoch(
    connection: &Connection,
    table_id: TableId,
    writer_epoch: u64,
    updated_at_ns: u64,
) -> Result<()> {
    let updated = connection
        .execute(
            "UPDATE tables SET writer_epoch = ?1, updated_at_ns = ?2 \
             WHERE table_id = ?3 AND state = 'active'",
            params![
                encode_u64(writer_epoch).as_slice(),
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("update legacy V4 table writer epoch", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy V4 table writer update affected an unexpected row count",
        ));
    }
    Ok(())
}

fn insert_v4_base_mapping(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
    table_id: TableId,
    legacy_files_revision: u64,
    native_snapshot_version: u64,
    catalog_revision: u64,
    committed_at_ns: u64,
) -> Result<()> {
    let fingerprint = envelope.canonical_fingerprint.into_bytes();
    connection
        .execute(
            "INSERT INTO legacy_file_view_mappings(\
                 table_id, legacy_files_revision, native_snapshot_version, request_id, \
                 format_version, fingerprint, previous_legacy_files_revision, \
                 source_catalog_revision, source_snapshot_hash, catalog_revision, continuity, \
                 committed_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, 4, ?5, ?2, ?6, ?7, ?8, 'base', ?9)",
            params![
                table_id.into_bytes().as_slice(),
                encode_u64(legacy_files_revision).as_slice(),
                encode_u64(native_snapshot_version).as_slice(),
                envelope.request_id.into_bytes().as_slice(),
                fingerprint.as_slice(),
                encode_u64(envelope.source_catalog_revision).as_slice(),
                envelope.source_projection_hash.as_str(),
                encode_u64(catalog_revision).as_slice(),
                encode_u64(committed_at_ns).as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("insert legacy V4 base mapping", error)
                .with_request_id(envelope.request_id)
        })?;
    Ok(())
}

fn ensure_all_active_projections_complete(connection: &Connection) -> Result<()> {
    let incomplete: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tables AS t \
               LEFT JOIN legacy_table_projections AS p ON p.table_id = t.table_id \
              WHERE t.state = 'active' \
                AND (p.table_id IS NULL OR p.state != 'active' OR p.completeness != 'complete')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("gate complete legacy V4 projections", error))?;
    let orphaned: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM legacy_table_projections AS p \
               JOIN tables AS t ON t.table_id = p.table_id \
              WHERE p.state = 'active' AND t.state != 'active'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("gate active legacy V4 projection set", error))?;
    if incomplete != 0 || orphaned != 0 {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "legacy V4 full-shadow gate requires every active table projection to be complete",
        )
        .with_retryable(false));
    }
    Ok(())
}

fn validate_projection_lifecycle_consistency(connection: &Connection) -> Result<()> {
    let mismatched: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM legacy_table_projections AS p \
               JOIN tables AS t ON t.table_id = p.table_id \
              WHERE (t.state = 'active' AND p.state != 'active') \
                 OR (t.state != 'active' AND p.state = 'active')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::map_sqlite_error("validate legacy V4 projection lifecycle", error)
        })?;
    if mismatched != 0 {
        return Err(corruption(
            "legacy V4 table and projection lifecycle states disagree",
        ));
    }
    Ok(())
}

/// Seeds a V2/V3-managed table into V7 projection state without stable source metadata.
#[allow(clippy::too_many_arguments)]
pub(crate) fn seed_legacy_table_projection(
    connection: &Connection,
    table_id: TableId,
    schema_version: u64,
    schema_hash: &str,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    updated_at_ns: u64,
) -> Result<()> {
    if read_table_projection(connection, table_id)?.is_some() {
        return Err(corruption(
            "new legacy import unexpectedly already has projection state",
        ));
    }
    insert_projection(
        connection,
        table_id,
        INITIAL_PROJECTION_REVISION,
        schema_version,
        schema_hash,
        native_snapshot_version,
        legacy_files_revision,
        writer_epoch,
        LegacyCatalogProjectionCompletenessV4::Seeded,
        LegacyCatalogProjectionTableStateV4::Active,
        None,
        updated_at_ns,
    )
}

/// Advances a V3-reconciled table cursor while preserving its V4 completeness class.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sync_projection_after_v3_reconcile(
    connection: &Connection,
    table_id: TableId,
    schema_version: u64,
    schema_hash: &str,
    previous_native_snapshot_version: u64,
    previous_legacy_files_revision: u64,
    previous_writer_epoch: u64,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    updated_at_ns: u64,
) -> Result<()> {
    let Some(current) = read_table_projection(connection, table_id)? else {
        return insert_projection(
            connection,
            table_id,
            INITIAL_PROJECTION_REVISION,
            schema_version,
            schema_hash,
            native_snapshot_version,
            legacy_files_revision,
            writer_epoch,
            LegacyCatalogProjectionCompletenessV4::Seeded,
            LegacyCatalogProjectionTableStateV4::Active,
            None,
            updated_at_ns,
        );
    };
    if current.state != LegacyCatalogProjectionTableStateV4::Active {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "V3 reconcile cannot mutate a dropped legacy projection",
        )
        .with_retryable(false));
    }
    if current.schema_version != schema_version
        || current.schema_hash != schema_hash
        || current.native_snapshot_version != previous_native_snapshot_version
        || current.legacy_files_revision != previous_legacy_files_revision
        || current.writer_epoch != previous_writer_epoch
    {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "V3 reconcile projection cursor changed before synchronization",
        )
        .with_retryable(false));
    }
    let next_revision = current.projection_revision.checked_add(1).ok_or_else(|| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            "legacy projection revision overflow after V3 reconcile",
        )
    })?;
    let updated = connection
        .execute(
            "UPDATE legacy_table_projections \
                SET projection_revision = ?1, schema_version = ?2, schema_hash = ?3, \
                    native_snapshot_version = ?4, legacy_files_revision = ?5, \
                    writer_epoch = ?6, updated_at_ns = ?7 \
              WHERE table_id = ?8 AND state = 'active'",
            params![
                encode_u64(next_revision).as_slice(),
                encode_u64(schema_version).as_slice(),
                schema_hash,
                encode_u64(native_snapshot_version).as_slice(),
                encode_u64(legacy_files_revision).as_slice(),
                encode_u64(writer_epoch).as_slice(),
                encode_u64(updated_at_ns).as_slice(),
                table_id.into_bytes().as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("sync projection after V3 reconcile", error))?;
    if updated != 1 {
        return Err(corruption(
            "V3 reconcile projection sync affected an unexpected row count",
        ));
    }
    Ok(())
}

/// Revokes read-cutover readiness after a table, writer, Schema, or file-view mutation.
pub(crate) fn downgrade_ready_after_table_mutation(
    connection: &Connection,
    updated_at_ns: u64,
) -> Result<()> {
    downgrade_ready(connection, updated_at_ns, true)
}

/// Revokes readiness after a native V1 mutation while allowing native-authority writes.
pub(crate) fn downgrade_ready_after_native_table_mutation(
    connection: &Connection,
    updated_at_ns: u64,
) -> Result<()> {
    downgrade_ready(connection, updated_at_ns, false)
}

fn downgrade_ready(
    connection: &Connection,
    updated_at_ns: u64,
    reject_native_authority: bool,
) -> Result<()> {
    let checkpoint = read_checkpoint(connection)?;
    if checkpoint.phase == LegacyCatalogProjectionPhaseV4::NativeAuthoritative {
        return if reject_native_authority {
            Err(ServerError::new(
                ErrorCode::Conflict,
                "legacy Catalog table mutation is forbidden after native authority",
            )
            .with_retryable(false))
        } else {
            Ok(())
        };
    }
    if checkpoint.phase != LegacyCatalogProjectionPhaseV4::ReadCutoverReady {
        return Ok(());
    }
    let incomplete: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM tables AS t \
               LEFT JOIN legacy_table_projections AS p ON p.table_id = t.table_id \
              WHERE t.state = 'active' \
                AND (p.table_id IS NULL OR p.state != 'active' OR p.completeness != 'complete')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::map_sqlite_error("classify legacy readiness downgrade", error))?;
    let phase = if incomplete == 0 {
        LegacyCatalogProjectionPhaseV4::FullShadow
    } else {
        LegacyCatalogProjectionPhaseV4::FileShadow
    };
    let generation = checkpoint
        .checkpoint_generation
        .checked_add(1)
        .ok_or_else(|| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "legacy checkpoint generation overflow during readiness downgrade",
            )
        })?;
    let updated = connection
        .execute(
            "UPDATE legacy_projection_checkpoint \
                SET checkpoint_generation = ?1, phase = ?2, updated_at_ns = ?3 \
              WHERE singleton = 1 AND checkpoint_generation = ?4 \
                AND phase = 'read_cutover_ready'",
            params![
                encode_u64(generation).as_slice(),
                encode_phase(phase),
                encode_u64(updated_at_ns).as_slice(),
                encode_u64(checkpoint.checkpoint_generation).as_slice(),
            ],
        )
        .map_err(|error| crate::map_sqlite_error("downgrade legacy read readiness", error))?;
    if updated != 1 {
        return Err(corruption(
            "legacy readiness downgrade affected an unexpected checkpoint row count",
        ));
    }
    Ok(())
}

fn replay(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
) -> Result<Option<LegacyCatalogProjectionResponseV4>> {
    let request_id = envelope.request_id;
    let fingerprint = envelope.canonical_fingerprint.into_bytes();
    let operation = operation(&envelope.command);
    let owner_exists = crate::request_ledger::exact_owner_exists(
        connection,
        request_id,
        FORMAT_VERSION,
        operation,
        &fingerprint,
    )?;
    let header = connection
        .query_row(
            "SELECT typeof(response_blob), length(response_blob) \
               FROM legacy_projection_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|error| {
            crate::map_sqlite_error("read legacy V4 replay header", error)
                .with_request_id(request_id)
        })?;
    let Some((blob_type, blob_len)) = header else {
        return if owner_exists {
            Err(
                corruption("legacy V4 response is missing for its global request owner")
                    .with_request_id(request_id),
            )
        } else {
            Ok(None)
        };
    };
    if !owner_exists {
        return Err(corruption("legacy V4 response has no global request owner")
            .with_request_id(request_id));
    }
    let blob_len = usize::try_from(blob_len).map_err(|_| {
        corruption("legacy V4 response has an invalid byte length").with_request_id(request_id)
    })?;
    if blob_type != "blob" || blob_len == 0 || blob_len > MAX_PROJECTION_RESPONSE_BYTES {
        return Err(
            corruption("legacy V4 permanent response violates its byte bounds")
                .with_request_id(request_id),
        );
    }
    let response_blob = connection
        .query_row(
            "SELECT response_blob FROM legacy_projection_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| {
            crate::map_sqlite_error("read legacy V4 replay response", error)
                .with_request_id(request_id)
        })?;
    if response_blob.len() != blob_len {
        return Err(
            corruption("legacy V4 response length changed inside its replay transaction")
                .with_request_id(request_id),
        );
    }
    let response =
        LegacyCatalogProjectionCodecV4::decode_response(&response_blob).map_err(|error| {
            corruption(format!(
                "decode frozen legacy Catalog V4 response error=[{error}]"
            ))
            .with_request_id(request_id)
        })?;
    validate_replay_response(connection, envelope, &response)?;
    Ok(Some(response))
}

fn validate_replay_response(
    connection: &Connection,
    envelope: &LegacyCatalogProjectionEnvelopeV4,
    response: &LegacyCatalogProjectionResponseV4,
) -> Result<()> {
    let request_id = envelope.request_id;
    let observed_catalog_revision = crate::domain::revision(connection)?;
    if response.request_id != request_id
        || response.catalog_revision == 0
        || response.catalog_revision > observed_catalog_revision
    {
        return Err(corruption(
            "legacy V4 replay response has an invalid request or Catalog revision",
        )
        .with_request_id(request_id));
    }
    match (&envelope.command, &response.outcome) {
        (
            LegacyCatalogProjectionCommandV4::BootstrapTable(command),
            LegacyCatalogProjectionOutcomeV4::BootstrapTable(result),
        ) => {
            let native_snapshot_version = if command.files.is_empty() {
                INITIAL_SNAPSHOT_VERSION
            } else {
                IMPORT_SNAPSHOT_VERSION
            };
            validate_exact_table_result(
                request_id,
                result,
                command.table_id,
                INITIAL_PROJECTION_REVISION,
                native_snapshot_version,
                command.legacy_files_revision,
                command.writer_epoch,
                LegacyCatalogProjectionCompletenessV4::Complete,
                LegacyCatalogProjectionTableStateV4::Active,
            )?;
            validate_historical_table_fact(connection, result, false)?;
        }
        (
            LegacyCatalogProjectionCommandV4::AdoptExistingTable(command),
            LegacyCatalogProjectionOutcomeV4::AdoptExistingTable(result),
        ) => {
            if result.table_id != command.table_id
                || result.projection_revision == 0
                || result.native_snapshot_version != command.expected_snapshot_version
                || result.legacy_files_revision != command.expected_legacy_files_revision
                || result.writer_epoch != command.expected_writer_epoch
                || result.completeness != LegacyCatalogProjectionCompletenessV4::Complete
                || result.state != LegacyCatalogProjectionTableStateV4::Active
            {
                return Err(corruption(
                    "legacy V4 adoption replay response does not match its command",
                )
                .with_request_id(request_id));
            }
            validate_historical_table_fact(connection, result, false)?;
        }
        (
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(command),
            LegacyCatalogProjectionOutcomeV4::SyncTableMetadata(result),
        ) => {
            let expected_revision = command
                .expected_projection_revision
                .checked_add(1)
                .ok_or_else(|| corruption("legacy V4 sync replay revision overflow"))?;
            validate_exact_table_result(
                request_id,
                result,
                command.table_id,
                expected_revision,
                command.expected_snapshot_version,
                command.expected_legacy_files_revision,
                command.writer_epoch,
                LegacyCatalogProjectionCompletenessV4::Complete,
                LegacyCatalogProjectionTableStateV4::Active,
            )?;
            validate_historical_table_fact(connection, result, false)?;
        }
        (
            LegacyCatalogProjectionCommandV4::DropTable(command),
            LegacyCatalogProjectionOutcomeV4::DropTable(result),
        ) => {
            let expected_revision = command
                .expected_projection_revision
                .checked_add(1)
                .ok_or_else(|| corruption("legacy V4 drop replay revision overflow"))?;
            validate_exact_table_result(
                request_id,
                result,
                command.table_id,
                expected_revision,
                command.expected_snapshot_version,
                command.expected_legacy_files_revision,
                command.expected_writer_epoch,
                if command.expected_source_metadata_hash.is_some() {
                    LegacyCatalogProjectionCompletenessV4::Complete
                } else {
                    LegacyCatalogProjectionCompletenessV4::Seeded
                },
                LegacyCatalogProjectionTableStateV4::Dropped,
            )?;
            validate_historical_table_fact(connection, result, true)?;
        }
        (
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(command),
            LegacyCatalogProjectionOutcomeV4::AdvanceCheckpoint(result),
        ) => {
            let expected_generation = command
                .expected_checkpoint_generation
                .checked_add(1)
                .ok_or_else(|| corruption("legacy V4 replay checkpoint generation overflow"))?;
            if result.checkpoint_generation != expected_generation
                || result.shadow_epoch != command.shadow_epoch
                || result.phase != command.phase
                || result.source_catalog_revision != envelope.source_catalog_revision
                || result.source_projection_hash != envelope.source_projection_hash
                || result.projection_digest != command.target_projection_digest
                || result.mode != command.mode
                || result.resolved_continuity_loss_epoch != command.observed_continuity_loss_epoch
                || result.updated_at_ns == 0
            {
                return Err(corruption(
                    "legacy V4 checkpoint replay response does not match its command",
                )
                .with_request_id(request_id));
            }
            let current = read_checkpoint(connection)?;
            if current.checkpoint_generation < result.checkpoint_generation {
                return Err(corruption(
                    "legacy V4 checkpoint replay is ahead of the durable checkpoint",
                )
                .with_request_id(request_id));
            }
        }
        _ => {
            return Err(
                corruption("legacy V4 replay outcome does not match its operation")
                    .with_request_id(request_id),
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_exact_table_result(
    request_id: RequestId,
    result: &LegacyCatalogProjectionTableResultV4,
    table_id: TableId,
    projection_revision: u64,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    completeness: LegacyCatalogProjectionCompletenessV4,
    state: LegacyCatalogProjectionTableStateV4,
) -> Result<()> {
    if result.table_id != table_id
        || result.projection_revision != projection_revision
        || result.native_snapshot_version != native_snapshot_version
        || result.legacy_files_revision != legacy_files_revision
        || result.writer_epoch != writer_epoch
        || result.completeness != completeness
        || result.state != state
    {
        return Err(
            corruption("legacy V4 table replay response does not match its command")
                .with_request_id(request_id),
        );
    }
    Ok(())
}

fn validate_historical_table_fact(
    connection: &Connection,
    result: &LegacyCatalogProjectionTableResultV4,
    require_dropped: bool,
) -> Result<()> {
    let current = read_table_projection(connection, result.table_id)?
        .ok_or_else(|| corruption("legacy V4 replay table projection is missing"))?;
    if current.projection_revision < result.projection_revision
        || (require_dropped && current.state != LegacyCatalogProjectionTableStateV4::Dropped)
    {
        return Err(corruption(
            "legacy V4 replay response is ahead of its durable table fact",
        ));
    }
    load_current_table(connection, result.table_id)?;
    Ok(())
}

fn table_result(
    table_id: TableId,
    projection_revision: u64,
    native_snapshot_version: u64,
    legacy_files_revision: u64,
    writer_epoch: u64,
    completeness: LegacyCatalogProjectionCompletenessV4,
    state: LegacyCatalogProjectionTableStateV4,
) -> LegacyCatalogProjectionTableResultV4 {
    LegacyCatalogProjectionTableResultV4 {
        table_id,
        projection_revision,
        native_snapshot_version,
        legacy_files_revision,
        writer_epoch,
        completeness,
        state,
    }
}

/// Cooperatively estimates the bounded queue bytes retained by one V4 command.
pub(crate) async fn legacy_projection_preflight_bytes(
    envelope: &LegacyCatalogProjectionEnvelopeV4,
) -> Result<usize> {
    let request_id = envelope.request_id;
    let mut work_since_yield = 0_usize;
    let mut total = size_of::<LegacyCatalogProjectionEnvelopeV4>();
    for bytes in [
        envelope.actor.principal.len(),
        envelope.source_projection_hash.len(),
    ] {
        total = checked_preflight_add(request_id, total, bytes)?;
    }
    match &envelope.command {
        LegacyCatalogProjectionCommandV4::BootstrapTable(command) => {
            total = checked_preflight_add(
                request_id,
                total,
                crate::manifest_domain::legacy_import_files_preflight_bytes(
                    &command.files,
                    &mut work_since_yield,
                )
                .await?,
            )?;
            total = checked_preflight_add(request_id, total, command.name.as_str().len())?;
            total = checked_preflight_add(request_id, total, command.schema_ipc.as_bytes().len())?;
            total = checked_preflight_add(request_id, total, command.canonical_schema_hash.len())?;
            total = preflight_table_config(request_id, total, &command.config)?;
            total = preflight_sources(request_id, total, &command.sources).await?;
        }
        LegacyCatalogProjectionCommandV4::AdoptExistingTable(command) => {
            total = checked_preflight_add(
                request_id,
                total,
                crate::manifest_domain::legacy_import_files_preflight_bytes(
                    &command.files,
                    &mut work_since_yield,
                )
                .await?,
            )?;
            total = checked_preflight_add(request_id, total, command.name.as_str().len())?;
            total = checked_preflight_add(request_id, total, command.expected_schema_hash.len())?;
            total = preflight_table_config(request_id, total, &command.config)?;
            total = preflight_sources(request_id, total, &command.sources).await?;
        }
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(command) => {
            total = checked_preflight_add(request_id, total, command.expected_schema_hash.len())?;
            total = checked_preflight_add(
                request_id,
                total,
                command.expected_source_metadata_hash.len(),
            )?;
            total = preflight_sources(request_id, total, &command.sources).await?;
        }
        LegacyCatalogProjectionCommandV4::DropTable(command) => {
            total = checked_preflight_add(request_id, total, command.expected_schema_hash.len())?;
            if let Some(expected_source_metadata_hash) =
                command.expected_source_metadata_hash.as_deref()
            {
                total =
                    checked_preflight_add(request_id, total, expected_source_metadata_hash.len())?;
            }
        }
        LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(command) => {
            for bytes in [
                command.expected_source_projection_hash.len(),
                command.target_projection_digest.len(),
            ] {
                total = checked_preflight_add(request_id, total, bytes)?;
            }
        }
    }
    if total > MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4 {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy V4 projection command exceeds byte limit actual=[{total}] maximum=[{MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4}]"
            ),
        ));
    }
    Ok(total.max(1))
}

fn preflight_table_config(
    request_id: RequestId,
    mut total: usize,
    config: &BTreeMap<String, String>,
) -> Result<usize> {
    for (key, value) in config {
        total = checked_preflight_add(request_id, total, key.len())?;
        total = checked_preflight_add(request_id, total, value.len())?;
    }
    Ok(total)
}

async fn preflight_sources(
    request_id: RequestId,
    mut total: usize,
    sources: &[LegacyCatalogSourceV4],
) -> Result<usize> {
    let mut work = 0_usize;
    let mut stack = sources
        .iter()
        .map(|source| &source.config)
        .collect::<Vec<_>>();
    for source in sources {
        for bytes in [
            source.source_name.len(),
            source.source_type.len(),
            source.output_stream.len(),
            size_of::<u64>(),
        ] {
            total = checked_preflight_add(request_id, total, bytes)?;
        }
    }
    while let Some(value) = stack.pop() {
        total = checked_preflight_add(request_id, total, size_of::<LegacyCatalogValueV4>())?;
        match value {
            LegacyCatalogValueV4::String(value) => {
                total = checked_preflight_add(request_id, total, value.len())?;
            }
            LegacyCatalogValueV4::Array(values) => stack.extend(values),
            LegacyCatalogValueV4::Object(values) => {
                for (key, value) in values {
                    total = checked_preflight_add(request_id, total, key.len())?;
                    stack.push(value);
                }
            }
            LegacyCatalogValueV4::Null
            | LegacyCatalogValueV4::Boolean(_)
            | LegacyCatalogValueV4::Int64(_)
            | LegacyCatalogValueV4::UInt64(_)
            | LegacyCatalogValueV4::Float64(_) => {}
        }
        work += 1;
        if work >= 256 {
            work = 0;
            tokio::task::yield_now().await;
        }
    }
    Ok(total)
}

fn checked_preflight_add(request_id: RequestId, total: usize, bytes: usize) -> Result<usize> {
    let total = total.checked_add(bytes).ok_or_else(|| {
        resource_exhausted(request_id, "legacy V4 projection byte estimate overflow")
    })?;
    if total > MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4 {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy V4 projection command exceeds byte limit actual=[{total}] maximum=[{MAX_LEGACY_CATALOG_CANONICAL_BYTES_V4}]"
            ),
        ));
    }
    Ok(total)
}

fn file_summary(
    request_id: RequestId,
    files: &[PreparedLegacyFile],
    legacy_files_revision: u64,
) -> Result<MetadataMap> {
    let rows = files.iter().try_fold(0_u64, |total, prepared| {
        total
            .checked_add(prepared.file.file.row_count.get())
            .ok_or_else(|| resource_exhausted(request_id, "legacy V4 row count overflow"))
    })?;
    let bytes = files.iter().try_fold(0_u64, |total, prepared| {
        total
            .checked_add(prepared.file.file.file_size_bytes)
            .ok_or_else(|| resource_exhausted(request_id, "legacy V4 byte count overflow"))
    })?;
    let file_count = u64::try_from(files.len())
        .map_err(|_| resource_exhausted(request_id, "legacy V4 file count does not fit u64"))?;
    Ok(MetadataMap::from([
        ("added_files".to_owned(), MetadataScalar::UInt64(file_count)),
        ("added_rows".to_owned(), MetadataScalar::UInt64(rows)),
        ("added_bytes".to_owned(), MetadataScalar::UInt64(bytes)),
        (
            "legacy_files_revision".to_owned(),
            MetadataScalar::UInt64(legacy_files_revision),
        ),
    ]))
}

fn validate_table_config(request_id: RequestId, config: &BTreeMap<String, String>) -> Result<()> {
    if config.len() > MAX_TABLE_CONFIG_ENTRIES {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy V4 table config entries exceed maximum actual=[{}] maximum=[{}]",
                config.len(),
                MAX_TABLE_CONFIG_ENTRIES
            ),
        ));
    }
    let text_bytes = config.iter().try_fold(0_usize, |total, (key, value)| {
        total
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| resource_exhausted(request_id, "legacy V4 table config size overflow"))
    })?;
    if text_bytes > MAX_TABLE_CONFIG_TEXT_BYTES {
        return Err(resource_exhausted(
            request_id,
            format!(
                "legacy V4 table config text exceeds maximum actual=[{text_bytes}] maximum=[{MAX_TABLE_CONFIG_TEXT_BYTES}]"
            ),
        ));
    }
    Ok(())
}

fn validate_table_cas_hashes(
    request_id: RequestId,
    schema_hash: &str,
    source_metadata_hash: &str,
) -> Result<()> {
    validate_hash(request_id, "expected Schema hash", schema_hash)?;
    validate_hash(
        request_id,
        "expected source metadata hash",
        source_metadata_hash,
    )
}

fn validate_checkpoint_hashes(
    request_id: RequestId,
    command: &LegacyCatalogAdvanceCheckpointCommandV4,
) -> Result<()> {
    validate_hash(
        request_id,
        "expected source projection hash",
        &command.expected_source_projection_hash,
    )?;
    validate_hash(
        request_id,
        "target projection digest",
        &command.target_projection_digest,
    )
}

fn validate_hash(request_id: RequestId, field: &str, value: &str) -> Result<()> {
    if is_canonical_hash(value) {
        Ok(())
    } else {
        Err(invalid(
            request_id,
            format!("legacy V4 {field} is not canonical lowercase SHA-256"),
        ))
    }
}

fn validate_stored_hash(field: &str, value: &str) -> Result<()> {
    if is_canonical_hash(value) {
        Ok(())
    } else {
        Err(corruption(format!(
            "stored legacy V4 {field} is not canonical lowercase SHA-256"
        )))
    }
}

fn is_canonical_hash(value: &str) -> bool {
    value.strip_prefix("sha256-v1:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn operation(command: &LegacyCatalogProjectionCommandV4) -> &'static str {
    match command {
        LegacyCatalogProjectionCommandV4::BootstrapTable(_) => {
            crate::request_ledger::V4_BOOTSTRAP_OPERATION
        }
        LegacyCatalogProjectionCommandV4::AdoptExistingTable(_) => {
            crate::request_ledger::V4_ADOPT_OPERATION
        }
        LegacyCatalogProjectionCommandV4::SyncTableMetadata(_) => {
            crate::request_ledger::V4_SYNC_OPERATION
        }
        LegacyCatalogProjectionCommandV4::DropTable(_) => crate::request_ledger::V4_DROP_OPERATION,
        LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(_) => {
            crate::request_ledger::V4_CHECKPOINT_OPERATION
        }
    }
}

fn require_actor(
    request_id: RequestId,
    actual: CatalogActorKind,
    expected: CatalogActorKind,
) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid(
            request_id,
            format!("legacy V4 operation requires actor kind=[{expected:?}] actual=[{actual:?}]"),
        ))
    }
}

fn decode_table_id(bytes: &[u8]) -> Result<TableId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| corruption("legacy V4 table identity has an invalid byte width"))?;
    TableId::from_bytes(bytes)
        .map_err(|error| corruption(format!("decode legacy V4 table identity error=[{error}]")))
}

fn decode_namespace_id(bytes: &[u8]) -> Result<NamespaceId> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| corruption("legacy V4 namespace identity has an invalid byte width"))?;
    NamespaceId::from_bytes(bytes).map_err(|error| {
        corruption(format!(
            "decode legacy V4 namespace identity error=[{error}]"
        ))
    })
}

fn decode_catalog_name(value: &str, entity: &str) -> Result<CatalogName> {
    CatalogName::new(value)
        .map_err(|error| corruption(format!("decode legacy V4 {entity} name error=[{error}]")))
}

fn decode_table_state(value: &str) -> Result<TableState> {
    match value {
        "active" => Ok(TableState::Active),
        "dropping" => Ok(TableState::Dropping),
        "dropped" => Ok(TableState::Dropped),
        "failed" => Ok(TableState::Failed),
        _ => Err(corruption("legacy V4 table has an invalid lifecycle state")),
    }
}

fn encode_phase(value: LegacyCatalogProjectionPhaseV4) -> &'static str {
    match value {
        LegacyCatalogProjectionPhaseV4::FoundationOnly => "foundation_only",
        LegacyCatalogProjectionPhaseV4::FileShadow => "file_shadow",
        LegacyCatalogProjectionPhaseV4::FullShadow => "full_shadow",
        LegacyCatalogProjectionPhaseV4::ReadCutoverReady => "read_cutover_ready",
        LegacyCatalogProjectionPhaseV4::NativeAuthoritative => "native_authoritative",
    }
}

fn decode_phase(value: &str) -> Result<LegacyCatalogProjectionPhaseV4> {
    match value {
        "foundation_only" => Ok(LegacyCatalogProjectionPhaseV4::FoundationOnly),
        "file_shadow" => Ok(LegacyCatalogProjectionPhaseV4::FileShadow),
        "full_shadow" => Ok(LegacyCatalogProjectionPhaseV4::FullShadow),
        "read_cutover_ready" => Ok(LegacyCatalogProjectionPhaseV4::ReadCutoverReady),
        "native_authoritative" => Ok(LegacyCatalogProjectionPhaseV4::NativeAuthoritative),
        _ => Err(corruption("legacy V4 checkpoint has an invalid phase")),
    }
}

fn phase_rank(value: LegacyCatalogProjectionPhaseV4) -> u8 {
    match value {
        LegacyCatalogProjectionPhaseV4::FoundationOnly => 0,
        LegacyCatalogProjectionPhaseV4::FileShadow => 1,
        LegacyCatalogProjectionPhaseV4::FullShadow => 2,
        LegacyCatalogProjectionPhaseV4::ReadCutoverReady => 3,
        LegacyCatalogProjectionPhaseV4::NativeAuthoritative => 4,
    }
}

fn encode_checkpoint_mode(value: LegacyCatalogCheckpointModeV4) -> &'static str {
    match value {
        LegacyCatalogCheckpointModeV4::Contiguous => "contiguous",
        LegacyCatalogCheckpointModeV4::FullReconcile => "full_reconcile",
    }
}

fn decode_checkpoint_mode(value: &str) -> Result<LegacyCatalogCheckpointModeV4> {
    match value {
        "contiguous" => Ok(LegacyCatalogCheckpointModeV4::Contiguous),
        "full_reconcile" => Ok(LegacyCatalogCheckpointModeV4::FullReconcile),
        _ => Err(corruption(
            "legacy V4 checkpoint has an invalid continuity mode",
        )),
    }
}

fn encode_completeness(value: LegacyCatalogProjectionCompletenessV4) -> &'static str {
    match value {
        LegacyCatalogProjectionCompletenessV4::Seeded => "seeded",
        LegacyCatalogProjectionCompletenessV4::Complete => "complete",
    }
}

fn decode_completeness(value: &str) -> Result<LegacyCatalogProjectionCompletenessV4> {
    match value {
        "seeded" => Ok(LegacyCatalogProjectionCompletenessV4::Seeded),
        "complete" => Ok(LegacyCatalogProjectionCompletenessV4::Complete),
        _ => Err(corruption(
            "legacy V4 table projection has an invalid completeness state",
        )),
    }
}

fn encode_projection_state(value: LegacyCatalogProjectionTableStateV4) -> &'static str {
    match value {
        LegacyCatalogProjectionTableStateV4::Active => "active",
        LegacyCatalogProjectionTableStateV4::Dropped => "dropped",
    }
}

fn decode_projection_state(value: &str) -> Result<LegacyCatalogProjectionTableStateV4> {
    match value {
        "active" => Ok(LegacyCatalogProjectionTableStateV4::Active),
        "dropped" => Ok(LegacyCatalogProjectionTableStateV4::Dropped),
        _ => Err(corruption(
            "legacy V4 table projection has an invalid lifecycle state",
        )),
    }
}

fn invalid(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn conflict(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Conflict, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

fn resource_exhausted(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use rusqlite::params;
    use zippy_types::catalog::{
        CatalogActor, LegacyCatalogProjectionCommandV4, LegacyCatalogProjectionOutcomeV4,
    };
    use zippy_types::table::{AppendTableDefinition, SchemaIpcPayload, TableDefinition};

    use super::*;

    const NAMESPACE_VALUE: u128 = 11;

    fn canonical_hash(digit: char) -> String {
        format!("sha256-v1:{}", digit.to_string().repeat(64))
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("nonzero request id")
    }

    fn make_table_id(value: u128) -> TableId {
        TableId::from_u128(value).expect("nonzero table id")
    }

    fn namespace_id() -> NamespaceId {
        NamespaceId::from_u128(NAMESPACE_VALUE).expect("nonzero namespace id")
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).expect("create IPC writer");
        writer.finish().expect("finish schema-only IPC stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("nonempty schema IPC payload")
    }

    fn migrated_catalog() -> Connection {
        let mut connection = Connection::open_in_memory().expect("open in-memory catalog");
        crate::migration::migrate(&mut connection).expect("migrate in-memory catalog");
        connection
            .execute(
                "INSERT INTO namespaces(namespace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
                params![
                    namespace_id().into_bytes().as_slice(),
                    "market",
                    encode_u64(1).as_slice(),
                ],
            )
            .expect("insert namespace fixture");
        connection
    }

    fn migration_config(name: &str, legacy_revision: u64) -> BTreeMap<String, String> {
        migration_config_with_base(name, 1, legacy_revision, 1)
    }

    fn migration_config_with_base(
        name: &str,
        migration_version: u64,
        legacy_revision: u64,
        native_snapshot_version: u64,
    ) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "zippy.migration.legacy_stream_name".to_owned(),
                name.to_owned(),
            ),
            (
                "zippy.migration.legacy_persisted_files_revision_base".to_owned(),
                legacy_revision.to_string(),
            ),
            (
                "zippy.migration.native_snapshot_version_base".to_owned(),
                native_snapshot_version.to_string(),
            ),
            (
                "zippy.migration.source_snapshot_hash".to_owned(),
                canonical_hash('a'),
            ),
            (
                "zippy.migration.version".to_owned(),
                migration_version.to_string(),
            ),
        ])
    }

    fn create_unprojected_table(
        connection: &mut Connection,
        table_id: TableId,
        name: &str,
        config: BTreeMap<String, String>,
    ) -> String {
        let schema_ipc = schema_ipc();
        let schema_hash =
            crate::canonical_schema_hash_from_ipc(&schema_ipc).expect("hash V1 Schema");
        let command = CreateTableCommand {
            table_id,
            namespace_id: namespace_id(),
            name: CatalogName::new(name).expect("valid V1 table name"),
            definition: TableDefinition::Append(AppendTableDefinition {
                event_time_column: None,
            }),
            schema_ipc,
            config,
        };
        let prepared =
            crate::table_domain::prepare_create_table(command).expect("prepare V1 table");
        let transaction = connection.transaction().expect("begin V1 fixture");
        crate::table_domain::create_table(&transaction, prepared, 1, 10)
            .expect("create V1 fixture");
        crate::domain::update_revision(&transaction, 1).expect("advance V1 fixture revision");
        transaction.commit().expect("commit V1 fixture");
        schema_hash
    }

    #[allow(clippy::too_many_arguments)]
    fn drop_envelope(
        request: u128,
        table_id: TableId,
        projection_revision: u64,
        schema_version: u64,
        schema_hash: String,
        snapshot_version: u64,
        legacy_files_revision: u64,
        writer_epoch: u64,
        source_metadata_hash: Option<String>,
        source_catalog_revision: u64,
    ) -> LegacyCatalogProjectionEnvelopeV4 {
        LegacyCatalogProjectionEnvelopeV4::new(
            request_id(request),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            compute_legacy_catalog_projection_digest_v4(&[]).expect("empty projection digest"),
            source_catalog_revision,
            LegacyCatalogProjectionCommandV4::DropTable(LegacyCatalogDropTableCommandV4 {
                table_id,
                expected_projection_revision: projection_revision,
                expected_schema_version: schema_version,
                expected_schema_hash: schema_hash,
                expected_snapshot_version: snapshot_version,
                expected_legacy_files_revision: legacy_files_revision,
                expected_writer_epoch: writer_epoch,
                expected_source_metadata_hash: source_metadata_hash,
            }),
        )
        .expect("build V4 drop envelope")
    }

    fn add_second_snapshot(connection: &Connection, table_id: TableId, operation: &str) {
        connection
            .execute(
                "INSERT INTO table_snapshots(\
                     table_id, snapshot_version, parent_snapshot_version, operation, commit_lsn, \
                     committed_log_offset, created_at_ns, summary_blob\
                 ) SELECT table_id, ?1, ?2, ?3, ?1, committed_log_offset, created_at_ns, \
                          summary_blob \
                     FROM table_snapshots WHERE table_id = ?4 AND snapshot_version = ?5",
                params![
                    encode_u64(2).as_slice(),
                    crate::encoding::encode_optional_u64(Some(1)).as_slice(),
                    operation,
                    table_id.into_bytes().as_slice(),
                    encode_u64(1).as_slice(),
                ],
            )
            .expect("insert second snapshot fixture");
        connection
            .execute(
                "UPDATE tables SET current_snapshot_version = ?1 WHERE table_id = ?2",
                params![encode_u64(2).as_slice(), table_id.into_bytes().as_slice()],
            )
            .expect("advance table snapshot fixture");
    }

    fn add_file_at_first_snapshot(connection: &Connection, table_id: TableId) {
        let absent = crate::encoding::encode_optional_u64(None);
        connection
            .execute(
                "INSERT INTO data_files(\
                     file_id, table_id, schema_version, location_kind, file_path, \
                     file_size_bytes, row_count, provenance_kind, min_log_offset, \
                     max_log_offset, legacy_preserve_discovery_order, legacy_discovery_order, \
                     partition_blob, statistics_blob, metadata_blob, checksum, \
                     added_snapshot_version, removed_snapshot_version, state, created_at_ns\
                 ) VALUES (?1, ?2, ?3, 'external_legacy', ?4, ?5, ?5, 'legacy', ?6, ?6, \
                           0, ?6, X'', X'', X'', 'sha256:00', ?3, ?6, 'committed', ?5)",
                params![
                    zippy_types::FileId::from_u128(99)
                        .expect("nonzero file id")
                        .into_bytes()
                        .as_slice(),
                    table_id.into_bytes().as_slice(),
                    encode_u64(1).as_slice(),
                    "/legacy/nonempty.parquet",
                    encode_u64(1).as_slice(),
                    absent.as_slice(),
                ],
            )
            .expect("insert first-snapshot file fixture");
    }

    fn assert_unprojected_drop_rolled_back(connection: &Connection, table_id: TableId) {
        assert!(read_table_projection(connection, table_id)
            .expect("read absent projection")
            .is_none());
        let mapping_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM legacy_file_view_mappings WHERE table_id = ?1",
                [table_id.into_bytes().as_slice()],
                |row| row.get(0),
            )
            .expect("count mappings");
        assert_eq!(mapping_count, 0);
        let state: String = connection
            .query_row(
                "SELECT state FROM tables WHERE table_id = ?1",
                [table_id.into_bytes().as_slice()],
                |row| row.get(0),
            )
            .expect("read table state");
        assert_eq!(state, "active");
        assert_eq!(crate::domain::revision(connection).unwrap(), 1);
    }

    fn bootstrap_envelope(
        request: u128,
        table_id: TableId,
        legacy_revision: u64,
        source_revision: u64,
        source_hash: String,
    ) -> LegacyCatalogProjectionEnvelopeV4 {
        let schema_ipc = schema_ipc();
        let schema_hash =
            crate::canonical_schema_hash_from_ipc(&schema_ipc).expect("hash bootstrap Schema");
        LegacyCatalogProjectionEnvelopeV4::new(
            request_id(request),
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "legacy-v4-test-migration".to_owned(),
            },
            0,
            source_hash,
            source_revision,
            LegacyCatalogProjectionCommandV4::BootstrapTable(
                LegacyCatalogBootstrapTableCommandV4 {
                    table_id,
                    namespace_id: namespace_id(),
                    name: CatalogName::new("ticks").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition {
                        event_time_column: None,
                    }),
                    schema_ipc,
                    canonical_schema_hash: schema_hash,
                    config: migration_config("ticks", legacy_revision),
                    writer_epoch: 0,
                    sources: Vec::new(),
                    files: Vec::new(),
                    legacy_files_revision: legacy_revision,
                },
            ),
        )
        .expect("build V4 bootstrap envelope")
    }

    fn execute_envelope(
        connection: &mut Connection,
        envelope: LegacyCatalogProjectionEnvelopeV4,
    ) -> Result<LegacyCatalogProjectionResponseV4> {
        let prepared = prepare_legacy_catalog_projection_v4(envelope)?;
        execute_legacy_catalog_projection_v4(connection, prepared)
    }

    #[test]
    fn bootstrap_replays_conflicts_and_preserves_empty_high_revision_history() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(21);
        let envelope = bootstrap_envelope(101, table_id, 7, 1, canonical_hash('b'));
        let response = execute_envelope(&mut connection, envelope.clone()).expect("bootstrap");
        let LegacyCatalogProjectionOutcomeV4::BootstrapTable(result) = &response.outcome else {
            panic!("expected bootstrap result");
        };
        assert_eq!(result.projection_revision, 1);
        assert_eq!(result.native_snapshot_version, 1);
        assert_eq!(result.legacy_files_revision, 7);
        let projection = read_table_projection(&connection, table_id)
            .expect("read projection")
            .expect("projection exists");
        assert_eq!(
            projection.completeness,
            LegacyCatalogProjectionCompletenessV4::Complete
        );
        assert_eq!(projection.legacy_files_revision, 7);
        let format: i64 = connection
            .query_row(
                "SELECT format_version FROM legacy_file_view_mappings WHERE table_id = ?1",
                [table_id.into_bytes().as_slice()],
                |row| row.get(0),
            )
            .expect("read mapping format");
        assert_eq!(format, 4);
        let inputs = list_active_digest_inputs(&connection).expect("list digest inputs");
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].config.is_empty());

        let verified =
            verify_legacy_catalog_projection_v4(envelope.clone()).expect("verify replay");
        let LegacyCatalogProjectionStartV4::Replay(replayed) =
            begin_verified_legacy_catalog_projection_v4(&connection, verified)
                .expect("probe replay")
        else {
            panic!("expected permanent replay");
        };
        assert_eq!(replayed, response);
        let checkpoint_before = read_checkpoint(&connection).expect("read checkpoint");
        let replayed_again = execute_envelope(&mut connection, envelope).expect("execute replay");
        assert_eq!(replayed_again, response);
        assert_eq!(
            read_checkpoint(&connection).expect("read checkpoint after replay"),
            checkpoint_before
        );

        let reused = bootstrap_envelope(101, make_table_id(22), 7, 2, canonical_hash('c'));
        let verified = verify_legacy_catalog_projection_v4(reused).expect("verify reused id");
        let error = begin_verified_legacy_catalog_projection_v4(&connection, verified)
            .expect_err("cross-command request id reuse must fail");
        assert_eq!(error.code(), ErrorCode::Conflict);
    }

    #[test]
    fn checkpoint_sync_drop_enforce_cas_demotion_active_digest_and_continuity() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(31);
        let bootstrap = bootstrap_envelope(201, table_id, 9, 1, canonical_hash('b'));
        execute_envelope(&mut connection, bootstrap).expect("bootstrap");
        let schema_hash = read_table_projection(&connection, table_id)
            .expect("read projection")
            .expect("projection exists")
            .schema_hash;
        let source_metadata_hash =
            compute_legacy_catalog_source_metadata_hash_v4(&[]).expect("hash empty sources");
        let bootstrap_digest = compute_active_projection_digest(&connection).expect("digest");
        let initial_checkpoint = read_checkpoint(&connection).expect("initial checkpoint");
        let ready = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(202),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            bootstrap_digest.clone(),
            1,
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
                LegacyCatalogAdvanceCheckpointCommandV4 {
                    expected_checkpoint_generation: initial_checkpoint.checkpoint_generation,
                    expected_source_catalog_revision: initial_checkpoint.source_catalog_revision,
                    expected_source_projection_hash: initial_checkpoint.source_projection_hash,
                    expected_phase: initial_checkpoint.phase,
                    shadow_epoch: 0,
                    phase: LegacyCatalogProjectionPhaseV4::ReadCutoverReady,
                    mode: LegacyCatalogCheckpointModeV4::FullReconcile,
                    target_projection_digest: bootstrap_digest,
                    observed_continuity_loss_epoch: 0,
                },
            ),
        )
        .expect("build ready checkpoint");
        execute_envelope(&mut connection, ready).expect("advance ready checkpoint");
        assert_eq!(
            read_checkpoint(&connection)
                .expect("read ready checkpoint")
                .phase,
            LegacyCatalogProjectionPhaseV4::ReadCutoverReady
        );

        let mut next_inputs = list_active_digest_inputs(&connection).expect("list current digest");
        next_inputs[0].writer_epoch = 1;
        let sync_digest = compute_legacy_catalog_projection_digest_v4(&next_inputs)
            .expect("compute sync target digest");
        let sync_command = LegacyCatalogSyncTableMetadataCommandV4 {
            table_id,
            expected_projection_revision: 1,
            expected_schema_version: 1,
            expected_schema_hash: schema_hash.clone(),
            expected_snapshot_version: 1,
            expected_legacy_files_revision: 9,
            expected_writer_epoch: 0,
            expected_source_metadata_hash: source_metadata_hash.clone(),
            writer_epoch: 1,
            sources: Vec::new(),
        };
        let sync = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(203),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            sync_digest,
            2,
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(sync_command.clone()),
        )
        .expect("build sync");
        let sync_response = execute_envelope(&mut connection, sync.clone()).expect("sync");
        let demoted = read_checkpoint(&connection).expect("read demoted checkpoint");
        assert_eq!(demoted.phase, LegacyCatalogProjectionPhaseV4::FullShadow);
        assert_eq!(demoted.checkpoint_generation, 2);
        assert_eq!(
            execute_envelope(&mut connection, sync).expect("replay sync"),
            sync_response
        );
        assert_eq!(
            read_checkpoint(&connection)
                .expect("read checkpoint after sync replay")
                .checkpoint_generation,
            2
        );

        let stale = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(204),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            canonical_hash('c'),
            3,
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(sync_command),
        )
        .expect("build stale sync");
        assert_eq!(
            execute_envelope(&mut connection, stale)
                .expect_err("stale projection CAS must fail")
                .code(),
            ErrorCode::Conflict
        );

        let fenced_sync = LegacyCatalogSyncTableMetadataCommandV4 {
            table_id,
            expected_projection_revision: 2,
            expected_schema_version: 1,
            expected_schema_hash: schema_hash.clone(),
            expected_snapshot_version: 1,
            expected_legacy_files_revision: 9,
            expected_writer_epoch: 1,
            expected_source_metadata_hash: source_metadata_hash.clone(),
            writer_epoch: 2,
            sources: Vec::new(),
        };
        let older_source = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(210),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            canonical_hash('c'),
            0,
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(fenced_sync.clone()),
        )
        .expect("build old-source sync");
        assert_eq!(
            execute_envelope(&mut connection, older_source)
                .expect_err("source revision older than checkpoint must fail")
                .code(),
            ErrorCode::Conflict
        );
        let same_revision_other_hash = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(211),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            canonical_hash('c'),
            1,
            LegacyCatalogProjectionCommandV4::SyncTableMetadata(fenced_sync),
        )
        .expect("build same-revision divergent sync");
        assert_eq!(
            execute_envelope(&mut connection, same_revision_other_hash)
                .expect_err("one source revision cannot carry two projection hashes")
                .code(),
            ErrorCode::Conflict
        );

        let empty_digest = compute_legacy_catalog_projection_digest_v4(&[])
            .expect("compute empty projection digest");
        let drop = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(205),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            empty_digest.clone(),
            3,
            LegacyCatalogProjectionCommandV4::DropTable(LegacyCatalogDropTableCommandV4 {
                table_id,
                expected_projection_revision: 2,
                expected_schema_version: 1,
                expected_schema_hash: schema_hash,
                expected_snapshot_version: 1,
                expected_legacy_files_revision: 9,
                expected_writer_epoch: 1,
                expected_source_metadata_hash: Some(source_metadata_hash),
            }),
        )
        .expect("build drop");
        execute_envelope(&mut connection, drop).expect("drop table");
        assert!(list_active_digest_inputs(&connection)
            .expect("list active after drop")
            .is_empty());
        assert_eq!(list_table_projections(&connection).unwrap().len(), 1);
        assert_eq!(
            list_table_projections(&connection).unwrap()[0].state,
            LegacyCatalogProjectionTableStateV4::Dropped
        );
        let resurrection = bootstrap_envelope(206, table_id, 9, 4, empty_digest.clone());
        assert_eq!(
            execute_envelope(&mut connection, resurrection)
                .expect_err("dropped identity cannot be recreated")
                .code(),
            ErrorCode::AlreadyExists
        );

        let checkpoint = read_checkpoint(&connection).expect("checkpoint before continuity");
        let contiguous = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(207),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            empty_digest.clone(),
            2,
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
                LegacyCatalogAdvanceCheckpointCommandV4 {
                    expected_checkpoint_generation: checkpoint.checkpoint_generation,
                    expected_source_catalog_revision: checkpoint.source_catalog_revision,
                    expected_source_projection_hash: checkpoint.source_projection_hash,
                    expected_phase: checkpoint.phase,
                    shadow_epoch: 0,
                    phase: LegacyCatalogProjectionPhaseV4::FullShadow,
                    mode: LegacyCatalogCheckpointModeV4::Contiguous,
                    target_projection_digest: empty_digest.clone(),
                    observed_continuity_loss_epoch: 0,
                },
            ),
        )
        .expect("build contiguous checkpoint");
        execute_envelope(&mut connection, contiguous).expect("advance contiguous checkpoint");
        let checkpoint = read_checkpoint(&connection).expect("checkpoint after contiguous");
        let gap = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(208),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            empty_digest.clone(),
            4,
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
                LegacyCatalogAdvanceCheckpointCommandV4 {
                    expected_checkpoint_generation: checkpoint.checkpoint_generation,
                    expected_source_catalog_revision: checkpoint.source_catalog_revision,
                    expected_source_projection_hash: checkpoint.source_projection_hash.clone(),
                    expected_phase: checkpoint.phase,
                    shadow_epoch: 0,
                    phase: checkpoint.phase,
                    mode: LegacyCatalogCheckpointModeV4::Contiguous,
                    target_projection_digest: empty_digest.clone(),
                    observed_continuity_loss_epoch: 0,
                },
            ),
        )
        .expect("build invalid gap checkpoint");
        assert_eq!(
            execute_envelope(&mut connection, gap)
                .expect_err("contiguous checkpoint cannot skip revisions")
                .code(),
            ErrorCode::Conflict
        );
        let reconcile = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(209),
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "legacy-v4-test-server".to_owned(),
            },
            0,
            empty_digest.clone(),
            4,
            LegacyCatalogProjectionCommandV4::AdvanceCheckpoint(
                LegacyCatalogAdvanceCheckpointCommandV4 {
                    expected_checkpoint_generation: checkpoint.checkpoint_generation,
                    expected_source_catalog_revision: checkpoint.source_catalog_revision,
                    expected_source_projection_hash: checkpoint.source_projection_hash,
                    expected_phase: checkpoint.phase,
                    shadow_epoch: 1,
                    phase: checkpoint.phase,
                    mode: LegacyCatalogCheckpointModeV4::FullReconcile,
                    target_projection_digest: empty_digest,
                    observed_continuity_loss_epoch: 1,
                },
            ),
        )
        .expect("build full reconcile checkpoint");
        execute_envelope(&mut connection, reconcile).expect("resolve continuity gap");
        let resolved = read_checkpoint(&connection).expect("read resolved checkpoint");
        assert_eq!(resolved.shadow_epoch, 1);
        assert_eq!(resolved.resolved_continuity_loss_epoch, 1);
    }

    #[test]
    fn drop_preserves_seeded_completeness_and_exact_replay() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(35);
        execute_envelope(
            &mut connection,
            bootstrap_envelope(401, table_id, 7, 1, canonical_hash('b')),
        )
        .expect("bootstrap complete projection");
        let projection = read_table_projection(&connection, table_id)
            .expect("read complete projection")
            .expect("complete projection exists");
        let source_metadata_hash = projection
            .source_metadata_hash
            .clone()
            .expect("complete source hash");

        let complete_without_hash = drop_envelope(
            402,
            table_id,
            1,
            1,
            projection.schema_hash.clone(),
            1,
            7,
            0,
            None,
            2,
        );
        assert_eq!(
            execute_envelope(&mut connection, complete_without_hash)
                .expect_err("complete projection requires a source hash")
                .code(),
            ErrorCode::Conflict
        );

        connection
            .execute(
                "UPDATE legacy_table_projections \
                    SET completeness = 'seeded', source_metadata_hash = NULL \
                  WHERE table_id = ?1",
                [table_id.into_bytes().as_slice()],
            )
            .expect("convert fixture to seeded projection");
        let seeded_with_hash = drop_envelope(
            403,
            table_id,
            1,
            1,
            projection.schema_hash.clone(),
            1,
            7,
            0,
            Some(source_metadata_hash),
            2,
        );
        assert_eq!(
            execute_envelope(&mut connection, seeded_with_hash)
                .expect_err("seeded projection forbids a source hash")
                .code(),
            ErrorCode::Conflict
        );

        let drop = drop_envelope(
            404,
            table_id,
            1,
            1,
            projection.schema_hash,
            1,
            7,
            0,
            None,
            2,
        );
        let response = execute_envelope(&mut connection, drop.clone()).expect("drop seeded table");
        let LegacyCatalogProjectionOutcomeV4::DropTable(result) = &response.outcome else {
            panic!("expected seeded drop result");
        };
        assert_eq!(result.projection_revision, 2);
        assert_eq!(
            result.completeness,
            LegacyCatalogProjectionCompletenessV4::Seeded
        );
        assert_eq!(result.state, LegacyCatalogProjectionTableStateV4::Dropped);
        let projection = read_table_projection(&connection, table_id)
            .expect("read dropped seeded projection")
            .expect("dropped seeded projection exists");
        assert_eq!(projection.completeness, result.completeness);
        assert!(projection.source_metadata_hash.is_none());
        let revision = crate::domain::revision(&connection).expect("read revision before replay");
        let checkpoint = read_checkpoint(&connection).expect("read checkpoint before replay");
        assert_eq!(
            execute_envelope(&mut connection, drop).expect("replay seeded drop"),
            response
        );
        assert_eq!(crate::domain::revision(&connection).unwrap(), revision);
        assert_eq!(read_checkpoint(&connection).unwrap(), checkpoint);
    }

    #[test]
    fn unprojected_empty_v1_drop_bootstraps_dropped_seed_and_replays() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(36);
        let schema_hash = create_unprojected_table(
            &mut connection,
            table_id,
            "vanished",
            migration_config("vanished", 13),
        );
        let drop = drop_envelope(501, table_id, 0, 1, schema_hash, 1, 13, 0, None, 1);
        let response = execute_envelope(&mut connection, drop.clone())
            .expect("drop unprojected empty V1 table");
        let LegacyCatalogProjectionOutcomeV4::DropTable(result) = &response.outcome else {
            panic!("expected unprojected drop result");
        };
        assert_eq!(result.projection_revision, 1);
        assert_eq!(
            result.completeness,
            LegacyCatalogProjectionCompletenessV4::Seeded
        );
        assert_eq!(result.state, LegacyCatalogProjectionTableStateV4::Dropped);
        let projection = read_table_projection(&connection, table_id)
            .expect("read dropped projection")
            .expect("dropped projection exists");
        assert_eq!(projection.projection_revision, 1);
        assert_eq!(projection.completeness, result.completeness);
        assert_eq!(projection.state, result.state);
        assert!(projection.source_metadata_hash.is_none());
        let mapping = crate::legacy_reconcile_domain::read_legacy_file_view_mapping(
            &connection,
            table_id,
            13,
        )
        .expect("read bootstrapped mapping")
        .expect("bootstrapped mapping exists");
        assert_eq!(mapping.native_snapshot_version, 1);
        assert!(mapping.request_id.is_none());

        let revision = crate::domain::revision(&connection).expect("read revision before replay");
        let checkpoint = read_checkpoint(&connection).expect("read checkpoint before replay");
        assert_eq!(
            execute_envelope(&mut connection, drop).expect("replay unprojected drop"),
            response
        );
        assert_eq!(crate::domain::revision(&connection).unwrap(), revision);
        assert_eq!(read_checkpoint(&connection).unwrap(), checkpoint);
    }

    #[test]
    fn unprojected_drop_rejects_wrong_option_revision_and_table_cas() {
        for (request, projection_revision, source_hash, schema_version) in [
            (511, 1, None, 1),
            (512, 0, Some(canonical_hash('c')), 1),
            (513, 0, None, 2),
        ] {
            let mut connection = migrated_catalog();
            let table_id = make_table_id(request);
            let schema_hash = create_unprojected_table(
                &mut connection,
                table_id,
                "vanished",
                migration_config("vanished", 13),
            );
            let drop = drop_envelope(
                request,
                table_id,
                projection_revision,
                schema_version,
                schema_hash,
                1,
                13,
                0,
                source_hash,
                1,
            );
            assert_eq!(
                execute_envelope(&mut connection, drop)
                    .expect_err("invalid unprojected drop combination must fail")
                    .code(),
                ErrorCode::Conflict
            );
            assert_unprojected_drop_rolled_back(&connection, table_id);
        }
    }

    #[test]
    fn unprojected_drop_rejects_nonempty_advanced_v2_and_existing_history() {
        {
            let mut connection = migrated_catalog();
            let table_id = make_table_id(521);
            let schema_hash = create_unprojected_table(
                &mut connection,
                table_id,
                "nonempty",
                migration_config("nonempty", 13),
            );
            add_file_at_first_snapshot(&connection, table_id);
            let drop = drop_envelope(521, table_id, 0, 1, schema_hash, 1, 13, 0, None, 1);
            assert_eq!(
                execute_envelope(&mut connection, drop)
                    .expect_err("nonempty V1 base must fail")
                    .code(),
                ErrorCode::Conflict
            );
            assert_unprojected_drop_rolled_back(&connection, table_id);
        }
        {
            let mut connection = migrated_catalog();
            let table_id = make_table_id(522);
            let schema_hash = create_unprojected_table(
                &mut connection,
                table_id,
                "advanced",
                migration_config("advanced", 13),
            );
            add_second_snapshot(&connection, table_id, "persist");
            let drop = drop_envelope(522, table_id, 0, 1, schema_hash, 2, 13, 0, None, 1);
            assert_eq!(
                execute_envelope(&mut connection, drop)
                    .expect_err("advanced V1 snapshot must fail")
                    .code(),
                ErrorCode::Conflict
            );
            assert_unprojected_drop_rolled_back(&connection, table_id);
        }
        {
            let mut connection = migrated_catalog();
            let table_id = make_table_id(523);
            let schema_hash = create_unprojected_table(
                &mut connection,
                table_id,
                "imported",
                migration_config_with_base("imported", 2, 13, 2),
            );
            add_second_snapshot(&connection, table_id, "import");
            let drop = drop_envelope(523, table_id, 0, 1, schema_hash, 2, 13, 0, None, 1);
            assert_eq!(
                execute_envelope(&mut connection, drop)
                    .expect_err("unprojected V2 table must fail")
                    .code(),
                ErrorCode::Conflict
            );
            assert_unprojected_drop_rolled_back(&connection, table_id);
        }
        {
            let mut connection = migrated_catalog();
            let table_id = make_table_id(524);
            let schema_hash = create_unprojected_table(
                &mut connection,
                table_id,
                "mapped",
                migration_config("mapped", 13),
            );
            let table = load_current_table(&connection, table_id).expect("read mapped table");
            crate::legacy_reconcile_domain::ensure_base_mapping(&connection, &table.descriptor, 13)
                .expect("create prior mapping history");
            let drop = drop_envelope(524, table_id, 0, 1, schema_hash, 1, 13, 0, None, 1);
            assert_eq!(
                execute_envelope(&mut connection, drop)
                    .expect_err("prior mapping history must fail")
                    .code(),
                ErrorCode::Conflict
            );
            assert!(read_table_projection(&connection, table_id)
                .expect("read absent projection")
                .is_none());
            let mapping_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM legacy_file_view_mappings WHERE table_id = ?1",
                    [table_id.into_bytes().as_slice()],
                    |row| row.get(0),
                )
                .expect("count existing mapping");
            assert_eq!(mapping_count, 1);
            assert_eq!(crate::domain::revision(&connection).unwrap(), 1);
        }
    }

    #[test]
    fn unprojected_drop_rolls_back_bootstrapped_mapping_on_late_failure() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(531);
        let schema_hash = create_unprojected_table(
            &mut connection,
            table_id,
            "rollback",
            migration_config("rollback", 13),
        );
        connection
            .execute_batch(
                "CREATE TEMP TRIGGER fail_unprojected_projection_insert \
                 BEFORE INSERT ON legacy_table_projections \
                 BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END;",
            )
            .expect("install projection failure trigger");
        let drop = drop_envelope(531, table_id, 0, 1, schema_hash, 1, 13, 0, None, 1);
        execute_envelope(&mut connection, drop.clone())
            .expect_err("late projection insert failure must abort drop");
        assert_unprojected_drop_rolled_back(&connection, table_id);
        connection
            .execute_batch("DROP TRIGGER fail_unprojected_projection_insert;")
            .expect("remove projection failure trigger");
        execute_envelope(&mut connection, drop).expect("retry fully rolled-back drop");
    }

    #[test]
    fn adopt_creates_only_valid_empty_v1_mapping_and_native_authority_splits_writes() {
        let mut connection = migrated_catalog();
        let table_id = make_table_id(41);
        let schema_ipc = schema_ipc();
        let schema_hash =
            crate::canonical_schema_hash_from_ipc(&schema_ipc).expect("hash adopted Schema");
        let command = CreateTableCommand {
            table_id,
            namespace_id: namespace_id(),
            name: CatalogName::new("adopted").expect("valid table name"),
            definition: TableDefinition::Append(AppendTableDefinition {
                event_time_column: None,
            }),
            schema_ipc,
            config: migration_config("adopted", 13),
        };
        let prepared =
            crate::table_domain::prepare_create_table(command.clone()).expect("prepare V1 table");
        let transaction = connection.transaction().expect("begin V1 fixture");
        crate::table_domain::create_table(&transaction, prepared, 1, 10).expect("create V1 table");
        crate::domain::update_revision(&transaction, 1).expect("advance fixture revision");
        transaction.commit().expect("commit V1 fixture");
        let adopt = LegacyCatalogProjectionEnvelopeV4::new(
            request_id(301),
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "legacy-v4-test-migration".to_owned(),
            },
            0,
            canonical_hash('b'),
            1,
            LegacyCatalogProjectionCommandV4::AdoptExistingTable(
                LegacyCatalogAdoptExistingTableCommandV4 {
                    table_id,
                    namespace_id: namespace_id(),
                    name: command.name,
                    definition: command.definition,
                    expected_schema_version: 1,
                    expected_schema_hash: schema_hash,
                    config: command.config,
                    expected_snapshot_version: 1,
                    expected_legacy_files_revision: 13,
                    expected_writer_epoch: 0,
                    sources: Vec::new(),
                    files: Vec::new(),
                },
            ),
        )
        .expect("build adoption");
        execute_envelope(&mut connection, adopt).expect("adopt V1 table");
        let projection = read_table_projection(&connection, table_id)
            .expect("read adopted projection")
            .expect("adopted projection exists");
        assert_eq!(projection.legacy_files_revision, 13);
        assert_eq!(
            projection.completeness,
            LegacyCatalogProjectionCompletenessV4::Complete
        );
        let mapping = crate::legacy_reconcile_domain::read_current_legacy_file_view_mapping(
            &connection,
            table_id,
        )
        .expect("read adopted mapping");
        assert_eq!(mapping.legacy_files_revision, 13);
        let mapping_format: i64 = connection
            .query_row(
                "SELECT format_version FROM legacy_file_view_mappings WHERE table_id = ?1",
                [table_id.into_bytes().as_slice()],
                |row| row.get(0),
            )
            .expect("read adopted mapping format");
        assert_eq!(mapping_format, 0);

        connection
            .execute(
                "UPDATE legacy_projection_checkpoint SET phase = 'native_authoritative' \
                 WHERE singleton = 1",
                [],
            )
            .expect("set native authority fixture");
        downgrade_ready_after_native_table_mutation(&connection, 20)
            .expect("native V1 mutation remains allowed");
        assert_eq!(
            downgrade_ready_after_table_mutation(&connection, 20)
                .expect_err("legacy mutation is fenced after native authority")
                .code(),
            ErrorCode::Conflict
        );
    }
}
