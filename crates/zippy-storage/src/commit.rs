use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use zippy_core::OwnedDataDirectory;
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1, CatalogCommandV1,
    CatalogOutcomeV1, CatalogWrite, CommitDataFilesCommand, VersionedCatalogCommand,
    VersionedCatalogResponse, MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{SnapshotOperation, TableSnapshot};
use zippy_types::{ErrorCode, OffsetWatermark, RequestId, Result, ServerError, TableId};

use crate::commit_intent::{CommitIntentJournal, DurableCommitIntent, MAX_FILES_PER_COMMIT_INTENT};
use crate::DurableParquetCandidate;

const DEFAULT_MAX_OUTSTANDING_COMMITS: usize = 64;
const DEFAULT_MAX_OUTSTANDING_COMMAND_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_UNCOMMITTED_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
const CANDIDATE_BUDGET_UNIT_BYTES: u64 = 64 * 1024;

/// Bounded resource and retry policy for serialized Catalog file commits.
#[derive(Debug, Clone)]
pub struct CatalogCommitCoordinatorConfig {
    max_outstanding_commits: usize,
    max_outstanding_command_bytes: usize,
    max_uncommitted_file_bytes: Option<u64>,
    retry_initial_delay: Duration,
    retry_max_delay: Duration,
}

impl Default for CatalogCommitCoordinatorConfig {
    fn default() -> Self {
        Self {
            max_outstanding_commits: DEFAULT_MAX_OUTSTANDING_COMMITS,
            max_outstanding_command_bytes: DEFAULT_MAX_OUTSTANDING_COMMAND_BYTES,
            max_uncommitted_file_bytes: Some(DEFAULT_MAX_UNCOMMITTED_FILE_BYTES),
            retry_initial_delay: DEFAULT_RETRY_INITIAL_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
        }
    }
}

impl CatalogCommitCoordinatorConfig {
    /// Creates the default 64-command, 128 MiB manifest, and 8 GiB file budget.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum active plus queued commit count.
    #[must_use]
    pub const fn with_max_outstanding_commits(mut self, maximum: usize) -> Self {
        self.max_outstanding_commits = maximum;
        self
    }

    /// Sets the aggregate frozen Catalog command bytes held by outstanding jobs.
    #[must_use]
    pub const fn with_max_outstanding_command_bytes(mut self, maximum: usize) -> Self {
        self.max_outstanding_command_bytes = maximum;
        self
    }

    /// Sets the aggregate durable candidate bytes held by outstanding jobs.
    ///
    /// `None` disables this disk-backlog admission budget. Accounting rounds
    /// each request up to 64 KiB, making the configured bound conservative.
    #[must_use]
    pub const fn with_max_uncommitted_file_bytes(mut self, maximum: Option<u64>) -> Self {
        self.max_uncommitted_file_bytes = maximum;
        self
    }

    /// Sets the first delay before replaying a commit-unknown envelope.
    #[must_use]
    pub const fn with_retry_initial_delay(mut self, delay: Duration) -> Self {
        self.retry_initial_delay = delay;
        self
    }

    /// Sets the capped delay between repeated commit-unknown envelopes.
    #[must_use]
    pub const fn with_retry_max_delay(mut self, delay: Duration) -> Self {
        self.retry_max_delay = delay;
        self
    }
}

/// One contiguous durable candidate set awaiting atomic Catalog visibility.
#[derive(Debug)]
pub struct CommitDataFilesIntent {
    request_id: RequestId,
    table_id: TableId,
    expected_snapshot_version: u64,
    candidates: Vec<DurableParquetCandidate>,
    committed_log_offset: OffsetWatermark,
}

impl CommitDataFilesIntent {
    /// Creates one table-scoped persist intent and sorts candidates by file ID.
    pub fn new(
        request_id: RequestId,
        table_id: TableId,
        expected_snapshot_version: u64,
        mut candidates: Vec<DurableParquetCandidate>,
        committed_log_offset: OffsetWatermark,
    ) -> Result<Self> {
        if expected_snapshot_version == 0 {
            return Err(terminal_invalid(
                "catalog data-file commit expected snapshot version must be nonzero",
            )
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        if candidates.is_empty() {
            return Err(
                terminal_invalid("catalog data-file commit candidates must not be empty")
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            );
        }
        if candidates.len() > MAX_FILES_PER_COMMIT_INTENT {
            return Err(terminal_resource(format!(
                concat!(
                    "catalog data-file commit candidate count exceeds maximum ",
                    "actual=[{}] maximum=[{}]"
                ),
                candidates.len(),
                MAX_FILES_PER_COMMIT_INTENT
            ))
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        if !matches!(committed_log_offset, OffsetWatermark::Known(_)) {
            return Err(terminal_invalid(
                "catalog data-file commit requires a known committed log offset",
            )
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        candidates.sort_unstable_by_key(|candidate| candidate.pending().file_id);
        for candidate in &candidates {
            if candidate.pending().table_id != table_id {
                return Err(terminal_invalid(format!(
                    "catalog data-file candidate belongs to another table file_id=[{}]",
                    candidate.pending().file_id
                ))
                .with_request_id(request_id)
                .with_table_id(table_id));
            }
        }
        if candidates
            .windows(2)
            .any(|pair| pair[0].pending().file_id == pair[1].pending().file_id)
        {
            return Err(terminal_invalid(
                "catalog data-file commit contains duplicate file identities",
            )
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        Ok(Self {
            request_id,
            table_id,
            expected_snapshot_version,
            candidates,
            committed_log_offset,
        })
    }

    fn into_prepared(self, actor: CatalogActor) -> Result<PreparedCommit> {
        let candidate_bytes = self.candidates.iter().try_fold(0_u64, |total, candidate| {
            total
                .checked_add(candidate.pending().file_size_bytes)
                .ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "catalog data-file candidate byte total overflow",
                    )
                    .with_request_id(self.request_id)
                    .with_table_id(self.table_id)
                    .with_retryable(false)
                })
        })?;
        let files = self
            .candidates
            .into_iter()
            .map(DurableParquetCandidate::into_pending)
            .collect();
        let envelope = CatalogCommandEnvelopeV1::new(
            self.request_id,
            None,
            actor,
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: self.table_id,
                expected_snapshot_version: self.expected_snapshot_version,
                files,
                committed_log_offset: self.committed_log_offset,
            }),
        )
        .map_err(|error| {
            terminal_invalid(format!(
                "construct catalog data-file commit envelope error=[{error}]"
            ))
            .with_request_id(self.request_id)
            .with_table_id(self.table_id)
        })?;
        let command: VersionedCatalogCommand = envelope.into();
        let command_bytes = CatalogCodec::encode_command(&command)
            .map_err(|error| {
                terminal_invalid(format!(
                    "encode catalog data-file commit envelope error=[{error}]"
                ))
                .with_request_id(self.request_id)
                .with_table_id(self.table_id)
            })?
            .len();
        Ok(PreparedCommit {
            request_id: self.request_id,
            table_id: self.table_id,
            command,
            command_bytes,
            candidate_bytes,
        })
    }
}

/// Durable proof that one candidate set became visible in a Catalog snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitDataFilesReceipt {
    /// The exact idempotency identity accepted by the Catalog.
    pub request_id: RequestId,
    /// The Catalog revision atomically assigned to the snapshot commit.
    pub catalog_revision: u64,
    /// The persisted table snapshot created by the Catalog.
    pub snapshot: TableSnapshot,
    /// Whether the already-committed intent sidecar still needs cleanup.
    pub intent_cleanup_pending: bool,
}

/// Single-worker coordinator for bounded, cancellation-safe file visibility.
pub struct CatalogCommitCoordinator {
    sender: Option<mpsc::Sender<CommitJob>>,
    accepting: Arc<AtomicBool>,
    count_budget: Arc<Semaphore>,
    command_budget: Arc<Semaphore>,
    candidate_budget: Option<Arc<Semaphore>>,
    max_command_bytes: usize,
    max_candidate_bytes: Option<u64>,
    actor: Option<JoinHandle<Result<()>>>,
    catalog_actor: CatalogActor,
}

/// Internal failure disposition for one coordinator request.
///
/// A definitive rejection is returned only after the Catalog proved that the
/// command did not commit and the corresponding durable intent was removed.
/// Callers may fail that request without degrading the coordinator. Every
/// other failure is fatal because commit state or durable recovery is not
/// safely resolved.
#[derive(Debug)]
pub(crate) enum CatalogCommitFailure {
    DefinitiveRejection(ServerError),
    Fatal(ServerError),
}

impl CatalogCommitFailure {
    pub(crate) fn into_error(self) -> ServerError {
        match self {
            Self::DefinitiveRejection(error) | Self::Fatal(error) => error,
        }
    }
}

impl From<ServerError> for CatalogCommitFailure {
    fn from(error: ServerError) -> Self {
        Self::Fatal(error)
    }
}

pub(crate) type ClassifiedCommitResult =
    std::result::Result<CommitDataFilesReceipt, CatalogCommitFailure>;

impl CatalogCommitCoordinator {
    /// Starts the coordinator after replaying every durable pending intent.
    ///
    /// Startup does not accept new work until recovery has returned the
    /// original Catalog response for every prior FIFO intent.
    pub async fn start(
        config: CatalogCommitCoordinatorConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<dyn CatalogWrite>,
        actor: CatalogActor,
    ) -> Result<Self> {
        validate_config(&config)?;
        validate_actor(&actor)?;
        let journal = Arc::new(CommitIntentJournal::new(data_directory));
        let recovery_journal = Arc::clone(&journal);
        let (pending, next_sequence) = tokio::task::spawn_blocking(move || recovery_journal.scan())
            .await
            .map_err(|error| actor_join_error("scan durable commit intents", error))??;

        for path in pending {
            let request_id = path.request_id;
            let recovery_journal = Arc::clone(&journal);
            let loaded = tokio::task::spawn_blocking(move || recovery_journal.load(&path))
                .await
                .map_err(|error| actor_join_error("load durable commit intent", error))??;
            match execute_intent(&journal, &catalog, &config, loaded).await? {
                IntentExecution::Committed(_) => {}
                IntentExecution::DefinitiveRejection(error) => {
                    tracing::warn!(
                        request_id = %error.request_id().unwrap_or(request_id),
                        code = %error.code(),
                        error = %error,
                        "discarded recovered durable intent after definitive Catalog rejection"
                    );
                }
            }
        }

        let (sender, receiver) = mpsc::channel(config.max_outstanding_commits);
        let worker = tokio::spawn(run_worker(
            receiver,
            next_sequence,
            journal,
            catalog,
            config.clone(),
        ));
        let count_budget = Arc::new(Semaphore::new(config.max_outstanding_commits));
        let command_budget = Arc::new(Semaphore::new(config.max_outstanding_command_bytes));
        let candidate_budget = config
            .max_uncommitted_file_bytes
            .map(candidate_budget_permits)
            .transpose()?
            .map(|permits| Arc::new(Semaphore::new(permits as usize)));
        Ok(Self {
            sender: Some(sender),
            accepting: Arc::new(AtomicBool::new(true)),
            count_budget,
            command_budget,
            candidate_budget,
            max_command_bytes: config.max_outstanding_command_bytes,
            max_candidate_bytes: config.max_uncommitted_file_bytes,
            actor: Some(worker),
            catalog_actor: actor,
        })
    }

    /// Admits one intent and waits for its durable Catalog snapshot receipt.
    ///
    /// Dropping this future before admission does nothing. Once the job enters
    /// the bounded worker queue, dropping the waiter does not cancel staging,
    /// Catalog replay, or intent cleanup.
    pub async fn commit(&self, intent: CommitDataFilesIntent) -> Result<CommitDataFilesReceipt> {
        self.commit_classified(intent)
            .await
            .map_err(CatalogCommitFailure::into_error)
    }

    /// Commits one intent while preserving whether a failure is request-local.
    ///
    /// [`CatalogCommitFailure::DefinitiveRejection`] proves both that Catalog
    /// did not commit the command and that its durable intent was removed.
    /// This internal contract lets the persistence controller keep serving
    /// later requests without inferring safety from a bare error code.
    pub(crate) async fn commit_classified(
        &self,
        intent: CommitDataFilesIntent,
    ) -> ClassifiedCommitResult {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(coordinator_closed().into());
        }
        let prepared = intent
            .into_prepared(self.catalog_actor.clone())
            .map_err(CatalogCommitFailure::Fatal)?;
        self.validate_single_job_budget(&prepared)
            .map_err(CatalogCommitFailure::Fatal)?;
        let request_id = prepared.request_id;
        let permits = self
            .acquire_permits(&prepared)
            .await
            .map_err(CatalogCommitFailure::Fatal)?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(CatalogCommitFailure::Fatal(
                coordinator_closed().with_request_id(request_id),
            ));
        }
        let (response_sender, response_receiver) = oneshot::channel();
        let job = CommitJob {
            prepared,
            response: response_sender,
            _permits: permits,
        };
        self.sender
            .as_ref()
            .ok_or_else(|| CatalogCommitFailure::Fatal(coordinator_closed()))?
            .send(job)
            .await
            .map_err(|_| {
                CatalogCommitFailure::Fatal(coordinator_closed().with_request_id(request_id))
            })?;
        response_receiver.await.map_err(|_| {
            CatalogCommitFailure::Fatal(
                ServerError::new(
                    ErrorCode::Unavailable,
                    "catalog commit coordinator stopped after accepting the request",
                )
                .with_request_id(request_id),
            )
        })?
    }

    /// Stops admission while allowing already accepted jobs to finish.
    pub fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Closes admission and reliably drains all accepted jobs without a deadline.
    ///
    /// A caller imposing an external timeout must treat timeout as
    /// commit-unknown; any already staged intent remains durable for restart
    /// recovery.
    pub async fn drain(mut self) -> Result<()> {
        self.close_admission();
        self.sender.take();
        let Some(actor) = self.actor.take() else {
            return Ok(());
        };
        actor
            .await
            .map_err(|error| actor_join_error("drain catalog commit coordinator", error))?
    }

    fn validate_single_job_budget(&self, prepared: &PreparedCommit) -> Result<()> {
        if prepared.command_bytes > self.max_command_bytes {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    concat!(
                        "catalog commit command exceeds coordinator byte budget ",
                        "requested=[{}] maximum=[{}]"
                    ),
                    prepared.command_bytes, self.max_command_bytes
                ),
            )
            .with_request_id(prepared.request_id)
            .with_table_id(prepared.table_id)
            .with_retryable(false));
        }
        if self
            .max_candidate_bytes
            .is_some_and(|maximum| prepared.candidate_bytes > maximum)
        {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    concat!(
                        "catalog commit candidates exceed coordinator file byte budget ",
                        "requested=[{}] maximum=[{}]"
                    ),
                    prepared.candidate_bytes,
                    self.max_candidate_bytes.expect("checked candidate budget")
                ),
            )
            .with_request_id(prepared.request_id)
            .with_table_id(prepared.table_id)
            .with_retryable(false));
        }
        Ok(())
    }

    async fn acquire_permits(&self, prepared: &PreparedCommit) -> Result<AdmissionPermits> {
        let count = Arc::clone(&self.count_budget)
            .acquire_owned()
            .await
            .map_err(|_| coordinator_closed().with_request_id(prepared.request_id))?;
        let command = Arc::clone(&self.command_budget)
            .acquire_many_owned(u32::try_from(prepared.command_bytes).map_err(|_| {
                terminal_resource("catalog commit command byte count exceeds semaphore domain")
                    .with_request_id(prepared.request_id)
            })?)
            .await
            .map_err(|_| coordinator_closed().with_request_id(prepared.request_id))?;
        let candidate = match &self.candidate_budget {
            Some(budget) => Some(
                Arc::clone(budget)
                    .acquire_many_owned(candidate_request_permits(prepared.candidate_bytes)?)
                    .await
                    .map_err(|_| coordinator_closed().with_request_id(prepared.request_id))?,
            ),
            None => None,
        };
        Ok(AdmissionPermits {
            _count: count,
            _command: command,
            _candidate: candidate,
        })
    }
}

struct PreparedCommit {
    request_id: RequestId,
    table_id: TableId,
    command: VersionedCatalogCommand,
    command_bytes: usize,
    candidate_bytes: u64,
}

struct AdmissionPermits {
    _count: OwnedSemaphorePermit,
    _command: OwnedSemaphorePermit,
    _candidate: Option<OwnedSemaphorePermit>,
}

struct CommitJob {
    prepared: PreparedCommit,
    response: oneshot::Sender<ClassifiedCommitResult>,
    _permits: AdmissionPermits,
}

enum IntentExecution {
    Committed(CommitDataFilesReceipt),
    DefinitiveRejection(ServerError),
}

async fn run_worker(
    mut receiver: mpsc::Receiver<CommitJob>,
    mut next_sequence: u64,
    journal: Arc<CommitIntentJournal>,
    catalog: Arc<dyn CatalogWrite>,
    config: CatalogCommitCoordinatorConfig,
) -> Result<()> {
    while let Some(job) = receiver.recv().await {
        let sequence = next_sequence;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| terminal_resource("catalog commit intent sequence exhausted u64"))?;
        let request_id = job.prepared.request_id;
        let stage_journal = Arc::clone(&journal);
        let command = job.prepared.command;
        let staged = tokio::task::spawn_blocking(move || stage_journal.stage(sequence, command))
            .await
            .map_err(|error| actor_join_error("stage durable commit intent", error))
            .and_then(|result| result);
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = job
                    .response
                    .send(Err(CatalogCommitFailure::Fatal(error.clone())));
                return Err(error);
            }
        };
        match execute_intent(&journal, &catalog, &config, staged).await {
            Ok(IntentExecution::Committed(receipt)) => {
                let _ = job.response.send(Ok(receipt));
            }
            Ok(IntentExecution::DefinitiveRejection(error)) => {
                tracing::warn!(
                    request_id = %request_id,
                    code = %error.code(),
                    error = %error,
                    "Catalog definitively rejected commit; removed durable intent and continuing"
                );
                let _ = job
                    .response
                    .send(Err(CatalogCommitFailure::DefinitiveRejection(error)));
            }
            Err(error) => {
                let _ = job
                    .response
                    .send(Err(CatalogCommitFailure::Fatal(error.clone())));
                tracing::error!(
                    request_id = %request_id,
                    error = %error,
                    "catalog commit coordinator stopped with a retained durable intent"
                );
                return Err(error);
            }
        }
    }
    Ok(())
}

async fn execute_intent(
    journal: &Arc<CommitIntentJournal>,
    catalog: &Arc<dyn CatalogWrite>,
    config: &CatalogCommitCoordinatorConfig,
    intent: DurableCommitIntent,
) -> Result<IntentExecution> {
    let verify_journal = Arc::clone(journal);
    let verify_intent = intent.clone();
    tokio::task::spawn_blocking(move || verify_journal.verify_candidates(&verify_intent))
        .await
        .map_err(|error| actor_join_error("verify durable commit candidates", error))??;

    let mut delay = config.retry_initial_delay;
    let mut attempts = 0_u64;
    let response = loop {
        match catalog.command(intent.command.clone()).await {
            Ok(response) => break response,
            Err(error) if is_commit_unknown(&error) => {
                attempts = attempts.saturating_add(1);
                if attempts == 1 || attempts.is_power_of_two() {
                    tracing::warn!(
                        request_id = %intent.path.request_id,
                        attempt = attempts,
                        delay_ms = delay.as_millis(),
                        error = %error,
                        "catalog commit response is unknown; replaying the unchanged envelope"
                    );
                } else {
                    tracing::debug!(
                        request_id = %intent.path.request_id,
                        attempt = attempts,
                        delay_ms = delay.as_millis(),
                        "catalog commit replay remains pending"
                    );
                }
                tokio::time::sleep(delay).await;
                delay = delay
                    .checked_mul(2)
                    .unwrap_or(config.retry_max_delay)
                    .min(config.retry_max_delay);
            }
            Err(error) if is_definitive_catalog_rejection(&error) => {
                remove_rejected_intent(journal, &intent).await?;
                return Ok(IntentExecution::DefinitiveRejection(error));
            }
            Err(error) => return Err(error),
        }
    };
    let mut receipt = validate_response(&intent.command, response)?;
    let cleanup_journal = Arc::clone(journal);
    let cleanup_path = intent.path.clone();
    match tokio::task::spawn_blocking(move || cleanup_journal.complete(&cleanup_path)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            receipt.intent_cleanup_pending = true;
            tracing::error!(
                request_id = %receipt.request_id,
                error = %error,
                "committed catalog intent cleanup remains pending"
            );
        }
        Err(error) => {
            receipt.intent_cleanup_pending = true;
            tracing::error!(
                request_id = %receipt.request_id,
                error = %error,
                "catalog intent cleanup task failed after commit"
            );
        }
    }
    Ok(IntentExecution::Committed(receipt))
}

async fn remove_rejected_intent(
    journal: &Arc<CommitIntentJournal>,
    intent: &DurableCommitIntent,
) -> Result<()> {
    let cleanup_journal = Arc::clone(journal);
    let cleanup_path = intent.path.clone();
    tokio::task::spawn_blocking(move || cleanup_journal.complete(&cleanup_path))
        .await
        .map_err(|error| actor_join_error("remove definitively rejected commit intent", error))??;
    Ok(())
}

fn validate_response(
    command: &VersionedCatalogCommand,
    response: VersionedCatalogResponse,
) -> Result<CommitDataFilesReceipt> {
    let VersionedCatalogCommand::V1(envelope) = command;
    let CatalogCommandV1::CommitDataFiles(commit) = &envelope.command else {
        return Err(response_corruption(
            envelope.request_id,
            "coordinator command is not CommitDataFiles",
        ));
    };
    let VersionedCatalogResponse::V1(response) = response;
    if response.request_id != envelope.request_id {
        return Err(response_corruption(
            envelope.request_id,
            "Catalog response request id does not match the commit intent",
        ));
    }
    let CatalogOutcomeV1::Snapshot(snapshot) = response.outcome else {
        return Err(response_corruption(
            envelope.request_id,
            "Catalog data-file commit returned a non-snapshot outcome",
        ));
    };
    let expected_snapshot = commit
        .expected_snapshot_version
        .checked_add(1)
        .ok_or_else(|| {
            response_corruption(
                envelope.request_id,
                "Catalog data-file snapshot version overflowed u64",
            )
        })?;
    if snapshot.table_id != commit.table_id
        || snapshot.snapshot_version != expected_snapshot
        || snapshot.parent_snapshot_version != Some(commit.expected_snapshot_version)
        || snapshot.operation != SnapshotOperation::Persist
        || snapshot.committed_log_offset != commit.committed_log_offset
        || snapshot.commit_lsn != response.catalog_revision
    {
        return Err(response_corruption(
            envelope.request_id,
            "Catalog snapshot response does not match the durable commit intent",
        ));
    }
    Ok(CommitDataFilesReceipt {
        request_id: response.request_id,
        catalog_revision: response.catalog_revision,
        snapshot,
        intent_cleanup_pending: false,
    })
}

fn validate_config(config: &CatalogCommitCoordinatorConfig) -> Result<()> {
    if config.max_outstanding_commits == 0
        || config.max_outstanding_commits > Semaphore::MAX_PERMITS
    {
        return Err(terminal_invalid(format!(
            "catalog commit outstanding count is outside supported range actual=[{}] maximum=[{}]",
            config.max_outstanding_commits,
            Semaphore::MAX_PERMITS
        )));
    }
    if config.max_outstanding_command_bytes == 0
        || config.max_outstanding_command_bytes > u32::MAX as usize
        || config.max_outstanding_command_bytes > Semaphore::MAX_PERMITS
    {
        return Err(terminal_invalid(format!(
            concat!(
                "catalog commit command byte budget is outside supported range ",
                "actual=[{}] maximum=[{}]"
            ),
            config.max_outstanding_command_bytes,
            u32::MAX
        )));
    }
    if let Some(maximum) = config.max_uncommitted_file_bytes {
        candidate_budget_permits(maximum)?;
    }
    if config.retry_initial_delay.is_zero()
        || config.retry_max_delay.is_zero()
        || config.retry_initial_delay > config.retry_max_delay
    {
        return Err(terminal_invalid(
            "catalog commit retry delays must be nonzero and ordered",
        ));
    }
    Ok(())
}

fn validate_actor(actor: &CatalogActor) -> Result<()> {
    if actor.kind != CatalogActorKind::Server {
        return Err(terminal_invalid(
            "catalog data-file commit coordinator requires a server actor",
        ));
    }
    if actor.principal.len() > MAX_CATALOG_ACTOR_PRINCIPAL_BYTES {
        return Err(terminal_resource(format!(
            "catalog commit actor principal exceeds byte limit actual=[{}] maximum=[{}]",
            actor.principal.len(),
            MAX_CATALOG_ACTOR_PRINCIPAL_BYTES
        )));
    }
    Ok(())
}

fn candidate_budget_permits(maximum: u64) -> Result<u32> {
    if maximum < CANDIDATE_BUDGET_UNIT_BYTES {
        return Err(terminal_invalid(format!(
            concat!(
                "catalog commit file byte budget is below accounting unit ",
                "actual=[{}] minimum=[{}]"
            ),
            maximum, CANDIDATE_BUDGET_UNIT_BYTES
        )));
    }
    let permits = maximum / CANDIDATE_BUDGET_UNIT_BYTES;
    u32::try_from(permits).map_err(|_| {
        terminal_invalid(format!(
            "catalog commit file byte budget exceeds supported range actual=[{maximum}]"
        ))
    })
}

fn candidate_request_permits(bytes: u64) -> Result<u32> {
    let rounded = bytes
        .checked_add(CANDIDATE_BUDGET_UNIT_BYTES - 1)
        .ok_or_else(|| terminal_resource("catalog commit candidate byte accounting overflow"))?;
    u32::try_from(rounded / CANDIDATE_BUDGET_UNIT_BYTES)
        .map_err(|_| terminal_resource("catalog commit candidate bytes exceed semaphore domain"))
}

fn is_commit_unknown(error: &ServerError) -> bool {
    error.retryable()
        || matches!(
            error.code(),
            ErrorCode::Cancelled | ErrorCode::DeadlineExceeded
        )
}

fn is_definitive_catalog_rejection(error: &ServerError) -> bool {
    if is_commit_unknown(error) {
        return false;
    }
    matches!(
        error.code(),
        ErrorCode::InvalidArgument
            | ErrorCode::Unauthenticated
            | ErrorCode::PermissionDenied
            | ErrorCode::NotFound
            | ErrorCode::AlreadyExists
            | ErrorCode::Conflict
            | ErrorCode::StaleProducerEpoch
            | ErrorCode::SequenceGap
            | ErrorCode::SchemaMismatch
            | ErrorCode::OffsetExpired
            | ErrorCode::UnsupportedPlan
    )
}

fn coordinator_closed() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "catalog commit coordinator is not accepting requests",
    )
}

fn terminal_invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn terminal_resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn response_corruption(request_id: RequestId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_request_id(request_id)
        .with_retryable(false)
}

fn actor_join_error(operation: &str, error: tokio::task::JoinError) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("{operation} task failed error=[{error}]"),
    )
    .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_error_disposition_is_conservative() {
        for code in [
            ErrorCode::InvalidArgument,
            ErrorCode::Unauthenticated,
            ErrorCode::PermissionDenied,
            ErrorCode::NotFound,
            ErrorCode::AlreadyExists,
            ErrorCode::Conflict,
            ErrorCode::StaleProducerEpoch,
            ErrorCode::SequenceGap,
            ErrorCode::SchemaMismatch,
            ErrorCode::OffsetExpired,
            ErrorCode::UnsupportedPlan,
        ] {
            let error = ServerError::new(code, "definitive rejection").with_retryable(false);
            assert!(is_definitive_catalog_rejection(&error), "code=[{code}]");
        }

        for code in [
            ErrorCode::ResourceExhausted,
            ErrorCode::Unavailable,
            ErrorCode::Cancelled,
            ErrorCode::DeadlineExceeded,
            ErrorCode::DataCorruption,
            ErrorCode::Internal,
        ] {
            let error = ServerError::new(code, "not definitive").with_retryable(false);
            assert!(!is_definitive_catalog_rejection(&error), "code=[{code}]");
        }

        let retryable_conflict =
            ServerError::new(ErrorCode::Conflict, "commit state unknown").with_retryable(true);
        assert!(!is_definitive_catalog_rejection(&retryable_conflict));
    }
}
