//! Transactional catalog domain operations executed by the SQLite actor.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{
    params, Connection, Error as SqliteError, OptionalExtension, Params, TransactionBehavior,
};
use zippy_types::catalog::{
    CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandReplayProbe,
    CatalogCommandV1, CatalogOutcomeV1, CatalogResponseV1, CreateNamespaceCommand,
    ListNamespacesRequest, ListNamespacesResponse, NamespaceRef, VersionedCatalogCommand,
    VersionedCatalogResponse,
};
use zippy_types::table::{CatalogName, NamespaceDescriptor};
use zippy_types::{ErrorCode, NamespaceId, RequestId, Result, ServerError, ServerErrorDetails};

use crate::encoding::{decode_u64, encode_u64};

const CREATE_NAMESPACE_OPERATION: &str = "create_namespace";
const DEFAULT_IDEMPOTENCY_RETENTION_NS: u64 = 24 * 60 * 60 * 1_000_000_000;
const MAX_NAMESPACE_PAGE_SIZE: u32 = 4_096;
const LIST_NAMESPACES_SQL: &str = "SELECT namespace_id, name, created_at_ns FROM namespaces \
     ORDER BY namespace_id LIMIT ?1";
const LIST_NAMESPACES_AFTER_SQL: &str = "SELECT namespace_id, name, created_at_ns FROM namespaces \
     WHERE namespace_id > ?1 ORDER BY namespace_id LIMIT ?2";
const MAX_SQLITE_CATALOG_NAME_BYTES: usize = 1024;

struct StoredIdempotentResponse {
    command_format_version: i64,
    operation: String,
    command_fingerprint: Vec<u8>,
    response_blob: Vec<u8>,
}

enum PreparedCommand {
    CreateNamespace(CreateNamespaceCommand),
    CreateTable(crate::table_domain::PreparedCreateTable),
    AcquireProducer(crate::producer_domain::PreparedAcquireProducer),
    CommitDataFiles(crate::manifest_domain::PreparedCommitDataFiles),
    CommitCompaction(crate::manifest_domain::PreparedCommitCompaction),
    AcquireSnapshotLease(crate::lease_domain::PreparedAcquireSnapshotLease),
    ReleaseSnapshotLease(crate::lease_domain::PreparedReleaseSnapshotLease),
}

#[derive(Clone)]
struct CommandContext {
    request_id: RequestId,
    expected_revision: Option<u64>,
    command_format_version: i64,
    operation: &'static str,
    fingerprint: [u8; 32],
}

/// A fixed-size replay lookup key derived from one completely verified command envelope.
pub(crate) struct VerifiedCommandReplayKey {
    context: CommandContext,
}

/// A manifest command whose complete frozen fingerprint has been validated.
pub(crate) struct VerifiedManifestCommand(VersionedCatalogCommand);

/// The result of probing idempotency for one verified manifest command.
pub(crate) enum ManifestCommandStart {
    /// The exact committed response can be returned without semantic preparation.
    Replay(VersionedCatalogResponse),
    /// The first execution must be prepared outside the SQLite actor.
    Prepare(ManifestCommandPreparation),
}

/// An owned, database-independent manifest preparation request.
pub(crate) struct ManifestCommandPreparation {
    context: CommandContext,
    actor_kind: CatalogActorKind,
    command: CatalogCommandV1,
}

impl ManifestCommandPreparation {
    /// Returns the request identity used for structured worker failures.
    pub(crate) const fn request_id(&self) -> RequestId {
        self.context.request_id
    }

    fn context(&self) -> &CommandContext {
        &self.context
    }
}

/// A manifest semantic-preparation outcome that always retains its command context.
pub(crate) struct ManifestPreparationResult {
    context: CommandContext,
    prepared: Result<PreparedCommand>,
}

impl ManifestPreparationResult {
    /// Creates a terminal preparation failure while preserving idempotency context.
    pub(crate) fn failed(preparation: &ManifestCommandPreparation, error: ServerError) -> Self {
        Self {
            context: preparation.context().clone(),
            prepared: Err(error.with_request_id(preparation.request_id())),
        }
    }
}

/// Replays a committed response without a writer lock or executes one mutation atomically.
pub(crate) fn execute_command(
    connection: &mut Connection,
    command: VersionedCatalogCommand,
) -> Result<VersionedCatalogResponse> {
    let VersionedCatalogCommand::V1(envelope) = command;
    verify_envelope(&envelope)?;
    let context = command_context(&envelope);
    if let Some(response) = try_replay_context(connection, &context)? {
        return Ok(response);
    }
    let prepared = prepare_command(envelope.actor.kind, envelope.command, context.request_id)?;
    commit_prepared_command(connection, context, prepared)
}

/// Validates one complete command envelope and reduces it to a fixed-size replay lookup key.
pub(crate) fn verify_command_replay_key(
    command: &VersionedCatalogCommand,
) -> Result<VerifiedCommandReplayKey> {
    let VersionedCatalogCommand::V1(envelope) = command;
    verify_envelope(envelope)?;
    Ok(VerifiedCommandReplayKey {
        context: command_context(envelope),
    })
}

/// Returns an exact retained response without preparing or executing a mutation.
pub(crate) fn probe_command_replay(
    connection: &Connection,
    key: &VerifiedCommandReplayKey,
) -> Result<CatalogCommandReplayProbe> {
    Ok(match try_replay_context(connection, &key.context)? {
        Some(response) => CatalogCommandReplayProbe::Exact { response },
        None => CatalogCommandReplayProbe::Missing,
    })
}

/// Returns whether a command requires the database-independent manifest pipeline.
pub(crate) fn is_manifest_command(command: &VersionedCatalogCommand) -> bool {
    let VersionedCatalogCommand::V1(envelope) = command;
    matches!(
        &envelope.command,
        CatalogCommandV1::CommitDataFiles(_) | CatalogCommandV1::CommitCompaction(_)
    )
}

/// Validates the full frozen fingerprint without performing semantic preparation.
pub(crate) fn verify_manifest_command(
    command: VersionedCatalogCommand,
) -> Result<VerifiedManifestCommand> {
    let VersionedCatalogCommand::V1(envelope) = &command;
    if !is_manifest_command(&command) {
        return Err(ServerError::new(
            ErrorCode::Internal,
            "non-manifest command entered the manifest verification worker",
        )
        .with_request_id(envelope.request_id));
    }
    verify_envelope(envelope)?;
    Ok(VerifiedManifestCommand(command))
}

/// Probes idempotency for a verified manifest before expensive semantic preparation.
pub(crate) fn begin_verified_manifest_command(
    connection: &Connection,
    verified: VerifiedManifestCommand,
) -> Result<ManifestCommandStart> {
    let VersionedCatalogCommand::V1(envelope) = verified.0;
    let context = command_context(&envelope);
    if let Some(response) = try_replay_context(connection, &context)? {
        return Ok(ManifestCommandStart::Replay(response));
    }
    Ok(ManifestCommandStart::Prepare(ManifestCommandPreparation {
        context,
        actor_kind: envelope.actor.kind,
        command: envelope.command,
    }))
}

/// Performs pure manifest validation and encoding without accessing SQLite.
pub(crate) fn prepare_manifest_command(
    preparation: ManifestCommandPreparation,
) -> ManifestPreparationResult {
    let request_id = preparation.context.request_id;
    let prepared = match preparation.command {
        CatalogCommandV1::CommitDataFiles(commit) => {
            crate::manifest_domain::prepare_commit_data_files(preparation.actor_kind, commit)
                .map(PreparedCommand::CommitDataFiles)
        }
        CatalogCommandV1::CommitCompaction(commit) => {
            crate::manifest_domain::prepare_commit_compaction(preparation.actor_kind, commit)
                .map(PreparedCommand::CommitCompaction)
        }
        _ => Err(ServerError::new(
            ErrorCode::Internal,
            "non-manifest command entered manifest semantic preparation",
        )),
    }
    .map_err(|error| error.with_request_id(request_id));
    ManifestPreparationResult {
        context: preparation.context,
        prepared,
    }
}

/// Finalizes a prepared manifest or resolves a concurrent idempotent commit first.
pub(crate) fn finalize_manifest_preparation(
    connection: &mut Connection,
    result: ManifestPreparationResult,
) -> Result<VersionedCatalogResponse> {
    match result.prepared {
        Ok(prepared) => commit_prepared_command(connection, result.context, prepared),
        Err(error) => match try_replay_context(connection, &result.context)? {
            Some(response) => Ok(response),
            None => Err(error),
        },
    }
}

fn verify_envelope(envelope: &CatalogCommandEnvelopeV1) -> Result<()> {
    envelope.verify_fingerprint().map_err(|error| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("invalid catalog command fingerprint error=[{error}]"),
        )
        .with_request_id(envelope.request_id)
    })
}

fn command_context(envelope: &CatalogCommandEnvelopeV1) -> CommandContext {
    CommandContext {
        request_id: envelope.request_id,
        expected_revision: envelope.expected_revision,
        command_format_version: i64::from(envelope.format_version.get()),
        operation: command_operation(&envelope.command),
        fingerprint: envelope.canonical_fingerprint.into_bytes(),
    }
}

fn try_replay_context(
    connection: &Connection,
    context: &CommandContext,
) -> Result<Option<VersionedCatalogResponse>> {
    try_replay_idempotent_response(
        connection,
        context.request_id,
        context.command_format_version,
        context.operation,
        &context.fingerprint,
    )
}

fn prepare_command(
    actor_kind: CatalogActorKind,
    command: CatalogCommandV1,
    request_id: RequestId,
) -> Result<PreparedCommand> {
    match command {
        CatalogCommandV1::CreateNamespace(create) => {
            validate_new_catalog_name(&create.name, "namespace")
                .map_err(|error| error.with_request_id(request_id))?;
            Ok(PreparedCommand::CreateNamespace(create))
        }
        CatalogCommandV1::CreateTable(create) => Ok(PreparedCommand::CreateTable(
            crate::table_domain::prepare_create_table(create)
                .map_err(|error| error.with_request_id(request_id))?,
        )),
        CatalogCommandV1::AcquireProducer(acquire) => Ok(PreparedCommand::AcquireProducer(
            crate::producer_domain::prepare_acquire_producer(acquire)
                .map_err(|error| error.with_request_id(request_id))?,
        )),
        CatalogCommandV1::CommitDataFiles(commit) => Ok(PreparedCommand::CommitDataFiles(
            crate::manifest_domain::prepare_commit_data_files(actor_kind, commit)
                .map_err(|error| error.with_request_id(request_id))?,
        )),
        CatalogCommandV1::CommitCompaction(commit) => Ok(PreparedCommand::CommitCompaction(
            crate::manifest_domain::prepare_commit_compaction(actor_kind, commit)
                .map_err(|error| error.with_request_id(request_id))?,
        )),
        CatalogCommandV1::AcquireSnapshotLease(acquire) => {
            Ok(PreparedCommand::AcquireSnapshotLease(
                crate::lease_domain::prepare_acquire_snapshot_lease(acquire)
                    .map_err(|error| error.with_request_id(request_id))?,
            ))
        }
        CatalogCommandV1::ReleaseSnapshotLease(release) => {
            Ok(PreparedCommand::ReleaseSnapshotLease(
                crate::lease_domain::prepare_release_snapshot_lease(release)
                    .map_err(|error| error.with_request_id(request_id))?,
            ))
        }
        ref unsupported => Err(unsupported_command(unsupported).with_request_id(request_id)),
    }
}

fn commit_prepared_command(
    connection: &mut Connection,
    context: CommandContext,
    prepared: PreparedCommand,
) -> Result<VersionedCatalogResponse> {
    let request_id = context.request_id;
    let mutates_table_projection = matches!(
        context.operation,
        "create_table"
            | "alter_table_schema"
            | "acquire_producer"
            | "commit_data_files"
            | "commit_compaction"
            | "drop_table"
    );
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            crate::map_sqlite_error("begin catalog command", error).with_request_id(request_id)
        })?;
    if let Some(response) = try_replay_idempotent_response(
        &transaction,
        request_id,
        context.command_format_version,
        context.operation,
        &context.fingerprint,
    )? {
        transaction.commit().map_err(|error| {
            crate::map_sqlite_error("commit catalog command replay", error)
                .with_request_id(request_id)
        })?;
        return Ok(response);
    }

    let actual_revision =
        revision(&transaction).map_err(|error| error.with_request_id(request_id))?;
    if let Some(expected_revision) = context.expected_revision {
        if expected_revision != actual_revision {
            return Err(revision_conflict(
                request_id,
                expected_revision,
                actual_revision,
            ));
        }
    }
    let next_revision = actual_revision.checked_add(1).ok_or_else(|| {
        ServerError::new(ErrorCode::Internal, "catalog revision overflow")
            .with_request_id(request_id)
    })?;
    let committed_at_ns = unix_time_ns().map_err(|error| error.with_request_id(request_id))?;
    let expires_at_ns = committed_at_ns
        .checked_add(DEFAULT_IDEMPOTENCY_RETENTION_NS)
        .ok_or_else(|| {
            ServerError::new(ErrorCode::Internal, "catalog idempotency expiry overflow")
                .with_request_id(request_id)
        })?;
    let outcome = match prepared {
        PreparedCommand::CreateNamespace(create) => CatalogOutcomeV1::Namespace(create_namespace(
            &transaction,
            create,
            committed_at_ns,
            request_id,
        )?),
        PreparedCommand::CreateTable(create) => CatalogOutcomeV1::Table(
            crate::table_domain::create_table(&transaction, create, next_revision, committed_at_ns)
                .map_err(|error| error.with_request_id(request_id))?,
        ),
        PreparedCommand::AcquireProducer(acquire) => CatalogOutcomeV1::Producer(
            crate::producer_domain::acquire_producer(&transaction, acquire, committed_at_ns)
                .map_err(|error| error.with_request_id(request_id))?,
        ),
        PreparedCommand::CommitDataFiles(commit) => CatalogOutcomeV1::Snapshot(
            crate::manifest_domain::commit_data_files(
                &transaction,
                commit,
                next_revision,
                committed_at_ns,
            )
            .map_err(|error| error.with_request_id(request_id))?,
        ),
        PreparedCommand::CommitCompaction(commit) => CatalogOutcomeV1::Snapshot(
            crate::manifest_domain::commit_compaction(
                &transaction,
                commit,
                next_revision,
                committed_at_ns,
            )
            .map_err(|error| error.with_request_id(request_id))?,
        ),
        PreparedCommand::AcquireSnapshotLease(acquire) => CatalogOutcomeV1::SnapshotLease(
            crate::lease_domain::acquire_snapshot_lease(&transaction, acquire, committed_at_ns)
                .map_err(|error| error.with_request_id(request_id))?,
        ),
        PreparedCommand::ReleaseSnapshotLease(release) => CatalogOutcomeV1::SnapshotLeaseReleased {
            lease_id: crate::lease_domain::release_snapshot_lease(&transaction, release)
                .map_err(|error| error.with_request_id(request_id))?,
        },
    };

    let response = VersionedCatalogResponse::V1(CatalogResponseV1 {
        request_id,
        catalog_revision: next_revision,
        outcome,
    });
    if mutates_table_projection {
        crate::legacy_projection_domain::downgrade_ready_after_native_table_mutation(
            &transaction,
            committed_at_ns,
        )
        .map_err(|error| error.with_request_id(request_id))?;
    }
    let response_blob = CatalogCodec::encode_response(&response).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("encode catalog response error=[{error}]"),
        )
        .with_request_id(request_id)
    })?;
    update_revision(&transaction, next_revision)
        .map_err(|error| error.with_request_id(request_id))?;
    transaction
        .execute(
            "INSERT INTO idempotent_requests(\
                 request_id, command_format_version, operation, command_fingerprint, response_blob, \
                 committed_at_ns, expires_at_ns\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                request_id.into_bytes().as_slice(),
                context.command_format_version,
                context.operation,
                context.fingerprint.as_slice(),
                response_blob,
                encode_u64(committed_at_ns).as_slice(),
                encode_u64(expires_at_ns).as_slice(),
            ],
        )
        .map_err(|error| {
            crate::map_sqlite_error("persist catalog idempotency response", error)
                .with_request_id(request_id)
        })?;
    crate::request_ledger::insert_owner(
        &transaction,
        request_id,
        context.command_format_version,
        context.operation,
        &context.fingerprint,
        committed_at_ns,
    )?;
    transaction.commit().map_err(|error| {
        crate::map_sqlite_error("commit catalog command", error).with_request_id(request_id)
    })?;
    Ok(response)
}

/// Enforces the SQLite Catalog admission limit without changing frozen V1 decoding.
pub(crate) fn validate_new_catalog_name(name: &CatalogName, entity: &str) -> Result<()> {
    let actual_bytes = name.as_str().len();
    if actual_bytes > MAX_SQLITE_CATALOG_NAME_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                concat!(
                    "sqlite catalog {} name exceeds byte limit ",
                    "actual=[{}] maximum=[{}]"
                ),
                entity, actual_bytes, MAX_SQLITE_CATALOG_NAME_BYTES
            ),
        ));
    }
    Ok(())
}

fn create_namespace(
    connection: &Connection,
    create: CreateNamespaceCommand,
    created_at_ns: u64,
    request_id: RequestId,
) -> Result<NamespaceDescriptor> {
    let descriptor = NamespaceDescriptor {
        namespace_id: create.namespace_id,
        name: create.name,
        created_at_ns,
    };
    connection
        .execute(
            "INSERT INTO namespaces(namespace_id, name, created_at_ns) VALUES (?1, ?2, ?3)",
            params![
                descriptor.namespace_id.into_bytes().as_slice(),
                descriptor.name.as_str(),
                encode_u64(descriptor.created_at_ns).as_slice(),
            ],
        )
        .map_err(|error| map_namespace_insert_error(error, request_id))?;
    Ok(descriptor)
}

/// Returns the current durable catalog revision.
pub(crate) fn revision(connection: &Connection) -> Result<u64> {
    crate::migration::catalog_revision(connection)
}

/// Resolves one namespace by immutable identity or exact name.
pub(crate) fn get_namespace(
    connection: &Connection,
    namespace: NamespaceRef,
) -> Result<NamespaceDescriptor> {
    let row = match namespace {
        NamespaceRef::Id(namespace_id) => connection
            .query_row(
                "SELECT namespace_id, name, created_at_ns FROM namespaces WHERE namespace_id = ?1",
                [namespace_id.into_bytes().as_slice()],
                namespace_row,
            )
            .optional(),
        NamespaceRef::Name(name) => connection
            .query_row(
                "SELECT namespace_id, name, created_at_ns FROM namespaces WHERE name = ?1",
                [name.as_str()],
                namespace_row,
            )
            .optional(),
    }
    .map_err(|error| crate::map_sqlite_error("read catalog namespace", error))?;
    row.map(decode_namespace_row)
        .transpose()?
        .ok_or_else(namespace_not_found)
}

/// Lists one deterministic, revision-consistent namespace page.
pub(crate) fn list_namespaces(
    connection: &mut Connection,
    request: ListNamespacesRequest,
) -> Result<ListNamespacesResponse> {
    if request.limit.get() > MAX_NAMESPACE_PAGE_SIZE {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog namespace page limit exceeds maximum requested=[{}] maximum=[{}]",
                request.limit.get(),
                MAX_NAMESPACE_PAGE_SIZE
            ),
        ));
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin catalog namespace listing", error))?;
    let catalog_revision = revision(&transaction)?;
    let after = request.after.map(|namespace_id| namespace_id.into_bytes());
    let requested_limit = usize::try_from(request.limit.get()).map_err(|_| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog namespace page limit is outside the supported domain",
        )
    })?;
    let sql_limit = i64::from(request.limit.get()) + 1;
    let mut namespaces = match after {
        None => query_namespaces(
            &transaction,
            LIST_NAMESPACES_SQL,
            [sql_limit],
            requested_limit,
        )?,
        Some(after) => query_namespaces(
            &transaction,
            LIST_NAMESPACES_AFTER_SQL,
            params![after.as_slice(), sql_limit],
            requested_limit,
        )?,
    };
    let has_more = namespaces.len() > requested_limit;
    namespaces.truncate(requested_limit);
    let next_after = has_more.then(|| {
        namespaces
            .last()
            .expect("nonzero namespace page limit")
            .namespace_id
    });
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit catalog namespace listing", error))?;
    Ok(ListNamespacesResponse {
        catalog_revision,
        namespaces,
        next_after,
    })
}

fn query_namespaces<P>(
    connection: &Connection,
    sql: &str,
    params: P,
    requested_limit: usize,
) -> Result<Vec<NamespaceDescriptor>>
where
    P: Params,
{
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| crate::map_sqlite_error("prepare catalog namespace listing", error))?;
    let mut rows = statement
        .query(params)
        .map_err(|error| crate::map_sqlite_error("query catalog namespace listing", error))?;
    let mut descriptors = Vec::with_capacity(requested_limit.saturating_add(1));
    while let Some(row) = rows
        .next()
        .map_err(|error| crate::map_sqlite_error("read catalog namespace page", error))?
    {
        let raw = namespace_row(row)
            .map_err(|error| crate::map_sqlite_error("decode catalog namespace row", error))?;
        descriptors.push(decode_namespace_row(raw)?);
    }
    Ok(descriptors)
}

fn load_idempotent_response(
    connection: &Connection,
    request_id: RequestId,
) -> Result<Option<StoredIdempotentResponse>> {
    connection
        .query_row(
            "SELECT command_format_version, operation, command_fingerprint, response_blob \
             FROM idempotent_requests WHERE request_id = ?1",
            [request_id.into_bytes().as_slice()],
            |row| {
                Ok(StoredIdempotentResponse {
                    command_format_version: row.get(0)?,
                    operation: row.get(1)?,
                    command_fingerprint: row.get(2)?,
                    response_blob: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(|error| crate::map_sqlite_error("read catalog idempotency response", error))
}

fn try_replay_idempotent_response(
    connection: &Connection,
    request_id: RequestId,
    command_format_version: i64,
    operation: &str,
    fingerprint: &[u8; 32],
) -> Result<Option<VersionedCatalogResponse>> {
    let owner_exists = crate::request_ledger::exact_owner_exists(
        connection,
        request_id,
        command_format_version,
        operation,
        fingerprint,
    )?;
    let stored = load_idempotent_response(connection, request_id)?;
    match (owner_exists, stored) {
        (true, Some(stored)) => Ok(Some(replay_idempotent_response(
            stored,
            request_id,
            command_format_version,
            operation,
            fingerprint,
        )?)),
        (false, None) => Ok(None),
        _ => Err(ServerError::new(
            ErrorCode::DataCorruption,
            "V1 idempotency fact and global request owner disagree",
        )
        .with_request_id(request_id)),
    }
}

fn replay_idempotent_response(
    stored: StoredIdempotentResponse,
    request_id: RequestId,
    command_format_version: i64,
    operation: &str,
    fingerprint: &[u8; 32],
) -> Result<VersionedCatalogResponse> {
    if stored.command_fingerprint.len() != fingerprint.len() {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog idempotency fingerprint has invalid width",
        )
        .with_request_id(request_id));
    }
    if stored.command_fingerprint.as_slice() != fingerprint {
        return Err(ServerError::new(
            ErrorCode::Conflict,
            "catalog request id was reused with a different command fingerprint",
        )
        .with_request_id(request_id));
    }
    if stored.command_format_version != command_format_version || stored.operation != operation {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog idempotency metadata does not match its command fingerprint",
        )
        .with_request_id(request_id));
    }
    let response = CatalogCodec::decode_response(&stored.response_blob).map_err(|error| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("decode catalog idempotency response error=[{error}]"),
        )
        .with_request_id(request_id)
    })?;
    let VersionedCatalogResponse::V1(decoded) = &response;
    if decoded.request_id != request_id {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog idempotency response has a mismatched request id",
        )
        .with_request_id(request_id));
    }
    if !response_matches_operation(&decoded.outcome, operation) {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            "catalog idempotency response outcome does not match its operation",
        )
        .with_request_id(request_id));
    }
    Ok(response)
}

fn response_matches_operation(outcome: &CatalogOutcomeV1, operation: &str) -> bool {
    matches!(
        (operation, outcome),
        ("create_namespace", CatalogOutcomeV1::Namespace(_))
            | ("create_table", CatalogOutcomeV1::Table(_))
            | ("alter_table_schema", CatalogOutcomeV1::Schema(_))
            | ("acquire_producer", CatalogOutcomeV1::Producer(_))
            | ("commit_data_files", CatalogOutcomeV1::Snapshot(_))
            | ("commit_compaction", CatalogOutcomeV1::Snapshot(_))
            | (
                "commit_consumer_offset",
                CatalogOutcomeV1::ConsumerOffset { .. }
            )
            | ("acquire_snapshot_lease", CatalogOutcomeV1::SnapshotLease(_))
            | (
                "release_snapshot_lease",
                CatalogOutcomeV1::SnapshotLeaseReleased { .. }
            )
            | (
                "create_maintenance_job",
                CatalogOutcomeV1::MaintenanceJob(_)
            )
            | (
                "transition_maintenance_job",
                CatalogOutcomeV1::MaintenanceJob(_)
            )
            | ("drop_table", CatalogOutcomeV1::TableDropped { .. })
    )
}

pub(crate) fn update_revision(connection: &Connection, next_revision: u64) -> Result<()> {
    let encoded = encode_u64(next_revision);
    let updated = connection
        .execute(
            "UPDATE catalog_meta SET value = ?1 WHERE key = 'catalog_revision'",
            [encoded.as_slice()],
        )
        .map_err(|error| crate::map_sqlite_error("advance catalog revision", error))?;
    if updated != 1 {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog revision update affected unexpected rows actual=[{updated}]"),
        ));
    }
    Ok(())
}

type RawNamespaceRow = (Vec<u8>, String, Vec<u8>);

fn namespace_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawNamespaceRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

fn decode_namespace_row(raw: RawNamespaceRow) -> Result<NamespaceDescriptor> {
    let (namespace_id, name, created_at_ns) = raw;
    let namespace_id: [u8; 16] = namespace_id.try_into().map_err(|bytes: Vec<u8>| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "catalog namespace id has invalid width actual=[{}]",
                bytes.len()
            ),
        )
    })?;
    let namespace_id = NamespaceId::from_bytes(namespace_id).map_err(|error| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog namespace id is invalid error=[{error}]"),
        )
    })?;
    let name = CatalogName::new(name).map_err(|error| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("catalog namespace name is invalid error=[{error}]"),
        )
    })?;
    Ok(NamespaceDescriptor {
        namespace_id,
        name,
        created_at_ns: decode_u64(&created_at_ns)?,
    })
}

fn revision_conflict(request_id: RequestId, expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        format!("catalog revision changed expected=[{expected}] actual=[{actual}]"),
    )
    .with_request_id(request_id)
    .with_details(ServerErrorDetails::RevisionConflict { expected, actual })
    .expect("revision conflict details match the conflict error code")
}

fn unsupported_command(command: &CatalogCommandV1) -> ServerError {
    let operator = command_operation(command);
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("catalog command is not implemented operator=[{operator}]"),
    )
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operator.to_owned(),
    })
    .expect("unsupported-plan details match the unsupported-plan error code")
}

fn command_operation(command: &CatalogCommandV1) -> &'static str {
    match command {
        CatalogCommandV1::CreateNamespace(_) => CREATE_NAMESPACE_OPERATION,
        CatalogCommandV1::CreateTable(_) => "create_table",
        CatalogCommandV1::AlterTableSchema(_) => "alter_table_schema",
        CatalogCommandV1::AcquireProducer(_) => "acquire_producer",
        CatalogCommandV1::CommitDataFiles(_) => "commit_data_files",
        CatalogCommandV1::CommitCompaction(_) => "commit_compaction",
        CatalogCommandV1::CommitConsumerOffset(_) => "commit_consumer_offset",
        CatalogCommandV1::AcquireSnapshotLease(_) => "acquire_snapshot_lease",
        CatalogCommandV1::ReleaseSnapshotLease(_) => "release_snapshot_lease",
        CatalogCommandV1::CreateMaintenanceJob(_) => "create_maintenance_job",
        CatalogCommandV1::TransitionMaintenanceJob(_) => "transition_maintenance_job",
        CatalogCommandV1::DropTable(_) => "drop_table",
    }
}

fn map_namespace_insert_error(error: SqliteError, request_id: RequestId) -> ServerError {
    if error.sqlite_error_code() == Some(rusqlite::ffi::ErrorCode::ConstraintViolation) {
        return ServerError::new(
            ErrorCode::AlreadyExists,
            "catalog namespace identity or name already exists",
        )
        .with_request_id(request_id);
    }
    crate::map_sqlite_error("insert catalog namespace", error).with_request_id(request_id)
}

fn namespace_not_found() -> ServerError {
    ServerError::new(ErrorCode::NotFound, "catalog namespace was not found")
}

pub(crate) fn unix_time_ns() -> Result<u64> {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("system clock precedes Unix epoch error=[{error}]"),
            )
        })?
        .as_nanos();
    u64::try_from(nanoseconds).map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "system time exceeds catalog timestamp representation",
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;

    use rusqlite::Connection;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1,
        CatalogCommandReplayProbe, CatalogCommandV1, CreateNamespaceCommand, CreateTableCommand,
        DropTableCommand, ListNamespacesRequest, NamespaceRef, VersionedCatalogCommand,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, SchemaIpcPayload, TableDefinition,
    };
    use zippy_types::{ErrorCode, NamespaceId, RequestId, ServerErrorDetails, TableId};

    use super::{
        execute_command, get_namespace, list_namespaces, probe_command_replay, revision,
        verify_command_replay_key,
    };
    use crate::encoding::encode_u64;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE catalog_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);\
                 CREATE TABLE namespaces (\
                     namespace_id BLOB PRIMARY KEY,\
                     name TEXT NOT NULL UNIQUE,\
                     created_at_ns BLOB NOT NULL\
                 );\
                 CREATE TABLE idempotent_requests (\
                     request_id BLOB PRIMARY KEY,\
                     command_format_version INTEGER NOT NULL,\
                     operation TEXT NOT NULL,\
                     command_fingerprint BLOB NOT NULL,\
                     response_blob BLOB NOT NULL,\
                     committed_at_ns BLOB NOT NULL,\
                     expires_at_ns BLOB NOT NULL\
                 );\
                 CREATE TABLE catalog_request_ids (\
                     request_id BLOB PRIMARY KEY,\
                     format_version INTEGER NOT NULL,\
                     operation TEXT NOT NULL,\
                     fingerprint BLOB NOT NULL,\
                     committed_at_ns BLOB NOT NULL\
                 );\
                 CREATE TABLE legacy_import_requests (\
                     request_id BLOB PRIMARY KEY,\
                     format_version INTEGER NOT NULL,\
                     fingerprint BLOB NOT NULL,\
                     response_blob BLOB NOT NULL,\
                     committed_at_ns BLOB NOT NULL\
                 );\
                 CREATE TABLE legacy_file_view_mappings (\
                     request_id BLOB UNIQUE\
                 );",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO catalog_meta(key, value) VALUES ('catalog_revision', ?1)",
                [encode_u64(0).as_slice()],
            )
            .unwrap();
        connection
    }

    fn actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::User,
            principal: "alice".to_owned(),
        }
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).unwrap()
    }

    fn namespace_id(value: u128) -> NamespaceId {
        NamespaceId::from_u128(value).unwrap()
    }

    fn create_command(
        request_id: RequestId,
        expected_revision: Option<u64>,
        namespace_id: NamespaceId,
        name: &str,
    ) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id,
            expected_revision,
            actor(),
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id,
                name: CatalogName::new(name).unwrap(),
            }),
        )
        .unwrap()
        .into()
    }

    fn create_table_command(request_id: RequestId) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            actor(),
            CatalogCommandV1::CreateTable(CreateTableCommand {
                table_id: TableId::from_u128(1).unwrap(),
                namespace_id: namespace_id(1),
                name: CatalogName::new("ticks").unwrap(),
                definition: TableDefinition::Append(AppendTableDefinition::default()),
                schema_ipc: SchemaIpcPayload::new(vec![1]).unwrap(),
                config: BTreeMap::new(),
            }),
        )
        .unwrap()
        .into()
    }

    fn unsupported_command(request_id: RequestId) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            actor(),
            CatalogCommandV1::DropTable(DropTableCommand {
                table_id: TableId::from_u128(1).unwrap(),
                expected_snapshot_version: 1,
            }),
        )
        .unwrap()
        .into()
    }

    #[test]
    fn create_namespace_replays_the_exact_response_without_advancing_revision() {
        let mut connection = connection();
        let command = create_command(request_id(1), Some(0), namespace_id(1), "market");
        let first = execute_command(&mut connection, command.clone()).unwrap();
        let replay = execute_command(&mut connection, command).unwrap();

        assert_eq!(
            CatalogCodec::encode_response(&first).unwrap(),
            CatalogCodec::encode_response(&replay).unwrap()
        );
        assert_eq!(revision(&connection).unwrap(), 1);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM namespaces", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM idempotent_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn request_id_reuse_with_a_different_fingerprint_conflicts() {
        let mut connection = connection();
        execute_command(
            &mut connection,
            create_command(request_id(1), None, namespace_id(1), "market"),
        )
        .unwrap();
        let error = execute_command(
            &mut connection,
            create_command(request_id(1), None, namespace_id(2), "changed"),
        )
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(error.request_id(), Some(request_id(1)));
        assert_eq!(revision(&connection).unwrap(), 1);

        let error =
            execute_command(&mut connection, create_table_command(request_id(1))).unwrap_err();
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(revision(&connection).unwrap(), 1);
    }

    #[test]
    fn command_replay_probe_is_read_only_and_matches_only_the_exact_fingerprint() {
        let mut connection = connection();
        let command = create_command(request_id(1), Some(0), namespace_id(1), "market");
        let key = verify_command_replay_key(&command).unwrap();

        assert_eq!(
            probe_command_replay(&connection, &key).unwrap(),
            CatalogCommandReplayProbe::Missing
        );
        assert_eq!(revision(&connection).unwrap(), 0);

        let committed = execute_command(&mut connection, command.clone()).unwrap();
        assert_eq!(
            probe_command_replay(&connection, &key).unwrap(),
            CatalogCommandReplayProbe::Exact {
                response: committed
            }
        );
        assert_eq!(revision(&connection).unwrap(), 1);

        let changed = create_command(request_id(1), None, namespace_id(2), "changed");
        let changed_key = verify_command_replay_key(&changed).unwrap();
        let conflict = probe_command_replay(&connection, &changed_key).unwrap_err();
        assert_eq!(conflict.code(), ErrorCode::Conflict);
        assert_eq!(conflict.request_id(), Some(request_id(1)));
        assert_eq!(revision(&connection).unwrap(), 1);
    }

    #[test]
    fn command_replay_probe_fails_closed_on_a_corrupt_retained_response() {
        let mut connection = connection();
        let command = create_command(request_id(1), Some(0), namespace_id(1), "market");
        execute_command(&mut connection, command.clone()).unwrap();
        let key = verify_command_replay_key(&command).unwrap();
        connection
            .execute(
                "UPDATE idempotent_requests SET response_blob = X'00' WHERE request_id = ?1",
                [request_id(1).into_bytes().as_slice()],
            )
            .unwrap();

        let error = probe_command_replay(&connection, &key).unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert_eq!(error.request_id(), Some(request_id(1)));
        assert_eq!(revision(&connection).unwrap(), 1);
    }

    #[test]
    fn expected_revision_conflict_rolls_back_every_domain_write() {
        let mut connection = connection();
        let error = execute_command(
            &mut connection,
            create_command(request_id(1), Some(7), namespace_id(1), "market"),
        )
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(
            error.details(),
            Some(&ServerErrorDetails::RevisionConflict {
                expected: 7,
                actual: 0,
            })
        );
        assert_eq!(revision(&connection).unwrap(), 0);
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM namespaces", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT count(*) FROM idempotent_requests", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn tampered_and_unsupported_commands_never_open_domain_writes() {
        let mut connection = connection();
        let VersionedCatalogCommand::V1(mut tampered) =
            create_command(request_id(1), None, namespace_id(1), "market");
        tampered.command = CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
            namespace_id: namespace_id(1),
            name: CatalogName::new("changed").unwrap(),
        });
        let error = execute_command(&mut connection, tampered.into()).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);

        let error =
            execute_command(&mut connection, unsupported_command(request_id(2))).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert_eq!(revision(&connection).unwrap(), 0);
    }

    #[test]
    fn get_and_list_namespaces_use_exact_identity_and_stable_cursor_order() {
        let mut connection = connection();
        for (request, namespace, name) in [(1, 3, "three"), (2, 1, "one"), (3, 2, "two")] {
            execute_command(
                &mut connection,
                create_command(request_id(request), None, namespace_id(namespace), name),
            )
            .unwrap();
        }

        let by_id = get_namespace(&connection, NamespaceRef::Id(namespace_id(2))).unwrap();
        let by_name = get_namespace(
            &connection,
            NamespaceRef::Name(CatalogName::new("two").unwrap()),
        )
        .unwrap();
        assert_eq!(by_id, by_name);

        let first = list_namespaces(
            &mut connection,
            ListNamespacesRequest {
                after: None,
                limit: NonZeroU32::new(2).unwrap(),
            },
        )
        .unwrap();
        assert_eq!(first.catalog_revision, 3);
        assert_eq!(
            first
                .namespaces
                .iter()
                .map(|namespace| namespace.namespace_id)
                .collect::<Vec<_>>(),
            [namespace_id(1), namespace_id(2)]
        );
        assert_eq!(first.next_after, Some(namespace_id(2)));

        let second = list_namespaces(
            &mut connection,
            ListNamespacesRequest {
                after: first.next_after,
                limit: NonZeroU32::new(2).unwrap(),
            },
        )
        .unwrap();
        assert_eq!(second.catalog_revision, 3);
        assert_eq!(second.namespaces.len(), 1);
        assert_eq!(second.namespaces[0].namespace_id, namespace_id(3));
        assert_eq!(second.next_after, None);
    }
}
