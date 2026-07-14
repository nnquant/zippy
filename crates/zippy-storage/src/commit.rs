use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use zippy_core::OwnedDataDirectory;
use zippy_types::catalog::{
    CatalogActor, CatalogActorKind, CatalogCodec, CatalogCommandEnvelopeV1,
    CatalogCommandReplayProbe, CatalogCommandV1, CatalogDataFileProbeMatch, CatalogOutcomeV1,
    CatalogPersistence, CatalogPublicationPermit, CatalogWrite, CommitDataFilesCommand,
    ManagedDataFileProbe, VersionedCatalogCommand, VersionedCatalogResponse,
    MAX_CATALOG_ACTOR_PRINCIPAL_BYTES,
};
use zippy_types::table::{SnapshotOperation, TableSnapshot};
use zippy_types::{ErrorCode, OffsetWatermark, RequestId, Result, ServerError, TableId};

use crate::candidate::{map_data_directory_error, verify_pending_candidate};
use crate::candidate::{PublishedCandidateCleanup, StagedParquetCandidate};
use crate::commit_intent::{CommitIntentJournal, DurableCommitIntent, MAX_FILES_PER_COMMIT_INTENT};
use crate::{DurableParquetCandidate, PersistMode};

const DEFAULT_MAX_OUTSTANDING_COMMITS: usize = 64;
const DEFAULT_MAX_OUTSTANDING_COMMAND_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_MAX_UNCOMMITTED_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
const CANDIDATE_BUDGET_UNIT_BYTES: u64 = 64 * 1024;
const CLEANUP_PROBE_BATCH_SIZE: usize = 512;

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
        prepare_commit(
            self.request_id,
            self.table_id,
            self.expected_snapshot_version,
            self.committed_log_offset,
            actor,
            files,
            candidate_bytes,
            Vec::new(),
        )
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
    persistence_catalog: Option<Arc<dyn CatalogPersistence>>,
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

struct PersistenceCatalogWriter {
    catalog: Arc<dyn CatalogPersistence>,
}

impl CatalogWrite for PersistenceCatalogWriter {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> zippy_types::BoxFuture<'_, Result<VersionedCatalogResponse>> {
        self.catalog.command(command)
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_commit(
    request_id: RequestId,
    table_id: TableId,
    expected_snapshot_version: u64,
    committed_log_offset: OffsetWatermark,
    actor: CatalogActor,
    files: Vec<zippy_types::table::PendingDataFile>,
    candidate_bytes: u64,
    cleanup_tokens: Vec<PublishedCandidateCleanup>,
) -> Result<PreparedCommit> {
    let envelope = CatalogCommandEnvelopeV1::new(
        request_id,
        None,
        actor,
        CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
            table_id,
            expected_snapshot_version,
            files,
            committed_log_offset,
        }),
    )
    .map_err(|error| {
        terminal_invalid(format!(
            "construct catalog data-file commit envelope error=[{error}]"
        ))
        .with_request_id(request_id)
        .with_table_id(table_id)
    })?;
    let command: VersionedCatalogCommand = envelope.into();
    let command_bytes = CatalogCodec::encode_command(&command)
        .map_err(|error| {
            terminal_invalid(format!(
                "encode catalog data-file commit envelope error=[{error}]"
            ))
            .with_request_id(request_id)
            .with_table_id(table_id)
        })?
        .len();
    Ok(PreparedCommit {
        request_id,
        table_id,
        command,
        command_bytes,
        candidate_bytes,
        cleanup_tokens,
    })
}

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
        Self::start_inner(config, data_directory, catalog, None, actor).await
    }

    /// Starts a native persistence coordinator with read-before-publish support.
    ///
    /// This is the production constructor for [`crate::PersistController`]. It
    /// requires durable idempotency probing and exact file-reference reads so
    /// historical replay never publishes and rejected publications can be
    /// removed only after Catalog proves them unreferenced.
    pub async fn start_with_persistence(
        config: CatalogCommitCoordinatorConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<dyn CatalogPersistence>,
        actor: CatalogActor,
    ) -> Result<Self> {
        let writer: Arc<dyn CatalogWrite> = Arc::new(PersistenceCatalogWriter {
            catalog: Arc::clone(&catalog),
        });
        Self::start_inner(config, data_directory, writer, Some(catalog), actor).await
    }

    async fn start_inner(
        config: CatalogCommitCoordinatorConfig,
        data_directory: Arc<OwnedDataDirectory>,
        catalog: Arc<dyn CatalogWrite>,
        persistence_catalog: Option<Arc<dyn CatalogPersistence>>,
        actor: CatalogActor,
    ) -> Result<Self> {
        validate_config(&config)?;
        validate_actor(&actor)?;
        let journal = Arc::new(CommitIntentJournal::new(Arc::clone(&data_directory)));
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
            match execute_intent(
                &journal,
                &catalog,
                persistence_catalog.as_deref(),
                &config,
                loaded,
                None,
            )
            .await?
            {
                IntentExecution::Committed(_) => {}
                IntentExecution::DefinitiveRejection { error, intent } => {
                    remove_rejected_intent(&journal, &intent).await?;
                    tracing::warn!(
                        request_id = %error.request_id().unwrap_or(request_id),
                        code = %error.code(),
                        error = %error,
                        concat!(
                            "discarded recovered durable intent after definitive Catalog ",
                            "rejection; startup reconciliation owns any resulting orphan"
                        )
                    );
                }
            }
        }

        let (sender, receiver) = mpsc::channel(config.max_outstanding_commits);
        let worker = tokio::spawn(run_worker(
            receiver,
            next_sequence,
            journal,
            Arc::clone(&data_directory),
            catalog,
            persistence_catalog.clone(),
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
            persistence_catalog,
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
        self.commit_prepared_classified(prepared).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_staged_classified(
        &self,
        mode: PersistMode,
        request_id: RequestId,
        table_id: TableId,
        expected_snapshot_version: u64,
        candidates: Vec<StagedParquetCandidate>,
        committed_log_offset: OffsetWatermark,
    ) -> ClassifiedCommitResult {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(CatalogCommitFailure::Fatal(
                coordinator_closed()
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            ));
        }
        let candidate_bytes = candidates
            .iter()
            .try_fold(0_u64, |total, candidate| {
                if candidate.pending().table_id != table_id {
                    return Err(terminal_invalid(
                        "staged persist candidate belongs to another table",
                    )
                    .with_request_id(request_id)
                    .with_table_id(table_id));
                }
                total
                    .checked_add(candidate.pending().file_size_bytes)
                    .ok_or_else(|| {
                        terminal_resource("staged persist candidate byte total overflow")
                            .with_request_id(request_id)
                            .with_table_id(table_id)
                    })
            })
            .map_err(CatalogCommitFailure::Fatal)?;
        let files = candidates
            .iter()
            .map(|candidate| candidate.pending().clone())
            .collect();
        let prepared = prepare_commit(
            request_id,
            table_id,
            expected_snapshot_version,
            committed_log_offset,
            self.catalog_actor.clone(),
            files,
            candidate_bytes,
            Vec::new(),
        )
        .map_err(CatalogCommitFailure::Fatal)?;
        match &self.persistence_catalog {
            Some(catalog) => match catalog.probe_command_replay(&prepared.command).await {
                Ok(CatalogCommandReplayProbe::Exact { response }) => {
                    drop(candidates);
                    return validate_response(&prepared.command, response)
                        .map_err(CatalogCommitFailure::Fatal);
                }
                Ok(CatalogCommandReplayProbe::Missing) => {
                    if mode == PersistMode::HistoricalReplay {
                        drop(candidates);
                        return Err(CatalogCommitFailure::DefinitiveRejection(
                            historical_replay_missing(request_id, table_id),
                        ));
                    }
                }
                Err(error) if error.code() == ErrorCode::Conflict && !error.retryable() => {
                    drop(candidates);
                    return Err(CatalogCommitFailure::DefinitiveRejection(
                        error.with_request_id(request_id).with_table_id(table_id),
                    ));
                }
                Err(error) => {
                    drop(candidates);
                    return Err(CatalogCommitFailure::Fatal(
                        error.with_request_id(request_id).with_table_id(table_id),
                    ));
                }
            },
            None => {
                drop(candidates);
                return Err(CatalogCommitFailure::Fatal(
                    ServerError::new(
                        ErrorCode::UnsupportedPlan,
                        "staged persistence requires Catalog publication control",
                    )
                    .with_request_id(request_id)
                    .with_table_id(table_id)
                    .with_retryable(false),
                ));
            }
        }

        self.validate_single_job_budget(&prepared)
            .map_err(CatalogCommitFailure::Fatal)?;
        self.enqueue_prepared_classified(prepared, Some(candidates))
            .await
    }

    async fn commit_prepared_classified(&self, prepared: PreparedCommit) -> ClassifiedCommitResult {
        self.enqueue_prepared_classified(prepared, None).await
    }

    async fn enqueue_prepared_classified(
        &self,
        prepared: PreparedCommit,
        staged_candidates: Option<Vec<StagedParquetCandidate>>,
    ) -> ClassifiedCommitResult {
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
        let staged = staged_candidates.map(|candidates| StagedCommit { candidates });
        if !self.accepting.load(Ordering::Acquire) {
            return Err(CatalogCommitFailure::Fatal(
                coordinator_closed().with_request_id(request_id),
            ));
        }
        let (response_sender, response_receiver) = oneshot::channel();
        let job = CommitJob {
            prepared,
            staged,
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

    pub(crate) fn supports_staged_persistence(&self) -> bool {
        self.persistence_catalog.is_some()
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
    cleanup_tokens: Vec<PublishedCandidateCleanup>,
}

struct StagedPublicationFailure {
    error: ServerError,
    cleanup_tokens: Vec<PublishedCandidateCleanup>,
}

fn publish_staged(
    candidates: Vec<StagedParquetCandidate>,
) -> std::result::Result<Vec<PublishedCandidateCleanup>, Box<StagedPublicationFailure>> {
    let mut cleanup_tokens = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        match candidate.publish() {
            Ok(candidate) => {
                cleanup_tokens.extend(candidate.into_cleanup());
            }
            Err(failure) => {
                let (error, cleanup) = failure.into_parts();
                cleanup_tokens.extend(cleanup);
                return Err(Box::new(StagedPublicationFailure {
                    error,
                    cleanup_tokens,
                }));
            }
        }
    }
    Ok(cleanup_tokens)
}

struct AdmissionPermits {
    _count: OwnedSemaphorePermit,
    _command: OwnedSemaphorePermit,
    _candidate: Option<OwnedSemaphorePermit>,
}

struct CommitJob {
    prepared: PreparedCommit,
    staged: Option<StagedCommit>,
    response: oneshot::Sender<ClassifiedCommitResult>,
    _permits: AdmissionPermits,
}

struct StagedCommit {
    candidates: Vec<StagedParquetCandidate>,
}

enum IntentExecution {
    Committed(CommitDataFilesReceipt),
    DefinitiveRejection {
        error: ServerError,
        intent: DurableCommitIntent,
    },
}

async fn run_worker(
    mut receiver: mpsc::Receiver<CommitJob>,
    mut next_sequence: u64,
    journal: Arc<CommitIntentJournal>,
    data_directory: Arc<OwnedDataDirectory>,
    catalog: Arc<dyn CatalogWrite>,
    persistence_catalog: Option<Arc<dyn CatalogPersistence>>,
    config: CatalogCommitCoordinatorConfig,
) -> Result<()> {
    'worker: while let Some(job) = receiver.recv().await {
        let CommitJob {
            mut prepared,
            staged,
            response,
            _permits,
        } = job;
        let sequence = next_sequence;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| terminal_resource("catalog commit intent sequence exhausted u64"))?;
        let request_id = prepared.request_id;
        let table_id = prepared.table_id;
        let initial_publication_permit = match staged {
            Some(staged) => {
                let StagedCommit { candidates } = staged;
                let publication_catalog = persistence_catalog.as_deref().ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "queued staged commit lost Catalog publication control",
                    )
                    .with_request_id(request_id)
                    .with_table_id(table_id)
                    .with_retryable(false)
                })?;
                let publication_permit =
                    match publication_catalog.acquire_publication_permit().await {
                        Ok(permit) => permit,
                        Err(error) => {
                            let error = error.with_request_id(request_id).with_table_id(table_id);
                            let _ = response.send(Err(CatalogCommitFailure::Fatal(error.clone())));
                            return Err(error);
                        }
                    };
                let published = tokio::task::spawn_blocking(move || publish_staged(candidates))
                    .await
                    .map_err(|error| actor_join_error("publish staged Parquet candidates", error));
                match published {
                    Ok(Ok(cleanup_tokens)) => {
                        prepared.cleanup_tokens = cleanup_tokens;
                        Some(publication_permit)
                    }
                    Ok(Err(failure)) => {
                        let StagedPublicationFailure {
                            error,
                            cleanup_tokens,
                        } = *failure;
                        drop(publication_permit);
                        if let Err(cleanup_error) = cleanup_rejected_candidates(
                            Arc::clone(&data_directory),
                            Some(publication_catalog),
                            request_id,
                            table_id,
                            cleanup_tokens,
                        )
                        .await
                        {
                            let _ = response
                                .send(Err(CatalogCommitFailure::Fatal(cleanup_error.clone())));
                            return Err(cleanup_error);
                        }
                        let _ = response.send(Err(CatalogCommitFailure::Fatal(error.clone())));
                        tracing::error!(
                            request_id = %request_id,
                            error = %error,
                            concat!(
                                "staged candidate publication failed; draining already accepted ",
                                "Catalog commit jobs"
                            )
                        );
                        continue 'worker;
                    }
                    Err(error) => {
                        drop(publication_permit);
                        let _ = response.send(Err(CatalogCommitFailure::Fatal(error.clone())));
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        let cleanup_tokens = prepared.cleanup_tokens;
        let stage_journal = Arc::clone(&journal);
        let command = prepared.command;
        let staged = tokio::task::spawn_blocking(move || stage_journal.stage(sequence, command))
            .await
            .map_err(|error| actor_join_error("stage durable commit intent", error))
            .and_then(|result| result);
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = response.send(Err(CatalogCommitFailure::Fatal(error.clone())));
                return Err(error);
            }
        };
        match execute_intent(
            &journal,
            &catalog,
            persistence_catalog.as_deref(),
            &config,
            staged,
            initial_publication_permit,
        )
        .await
        {
            Ok(IntentExecution::Committed(receipt)) => {
                let _ = response.send(Ok(receipt));
            }
            Ok(IntentExecution::DefinitiveRejection { error, intent }) => {
                if let Err(cleanup_error) = remove_rejected_intent(&journal, &intent).await {
                    let _ = response.send(Err(CatalogCommitFailure::Fatal(cleanup_error.clone())));
                    return Err(cleanup_error);
                }
                if let Err(cleanup_error) = cleanup_rejected_candidates(
                    Arc::clone(&data_directory),
                    persistence_catalog.as_deref(),
                    request_id,
                    table_id,
                    cleanup_tokens,
                )
                .await
                {
                    let _ = response.send(Err(CatalogCommitFailure::Fatal(cleanup_error.clone())));
                    return Err(cleanup_error);
                }
                tracing::warn!(
                    request_id = %request_id,
                    code = %error.code(),
                    error = %error,
                    "Catalog definitively rejected commit; removed durable intent and continuing"
                );
                let _ = response.send(Err(CatalogCommitFailure::DefinitiveRejection(error)));
            }
            Err(error) => {
                let _ = response.send(Err(CatalogCommitFailure::Fatal(error.clone())));
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

async fn cleanup_rejected_candidates(
    data_directory: Arc<OwnedDataDirectory>,
    catalog: Option<&dyn CatalogPersistence>,
    request_id: RequestId,
    table_id: TableId,
    mut tokens: Vec<PublishedCandidateCleanup>,
) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    let catalog = catalog.ok_or_else(|| {
        ServerError::new(
            ErrorCode::Internal,
            "candidate cleanup requires Catalog persistence reads",
        )
        .with_request_id(request_id)
        .with_table_id(table_id)
        .with_retryable(false)
    })?;
    tokens.sort_unstable_by_key(PublishedCandidateCleanup::file_id);
    validate_cleanup_tokens(request_id, table_id, &tokens)?;
    let probes = tokens
        .iter()
        .map(|token| ManagedDataFileProbe {
            file_id: token.file_id(),
            table_id: token.table_id(),
            path: token.path().clone(),
        })
        .collect::<Vec<_>>();
    let _publication_exclusion = catalog
        .acquire_publication_exclusion()
        .await
        .map_err(|error| error.with_request_id(request_id).with_table_id(table_id))?;
    let catalog_revision = catalog.revision().await?;
    for chunk in probes.chunks(CLEANUP_PROBE_BATCH_SIZE) {
        let inspected = catalog.inspect_data_file_references(chunk).await?;
        if inspected.catalog_revision != catalog_revision {
            return Err(cleanup_revision_changed(
                request_id,
                table_id,
                catalog_revision,
                inspected.catalog_revision,
            ));
        }
        if inspected.results.len() != chunk.len()
            || !inspected.results.iter().zip(chunk).all(|(result, probe)| {
                result.probe == *probe && result.matched == CatalogDataFileProbeMatch::Unreferenced
            })
        {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "Catalog did not prove every rejected candidate unreferenced",
            )
            .with_request_id(request_id)
            .with_table_id(table_id)
            .with_retryable(false));
        }
    }
    let ending_revision = catalog.revision().await?;
    if ending_revision != catalog_revision {
        return Err(cleanup_revision_changed(
            request_id,
            table_id,
            catalog_revision,
            ending_revision,
        ));
    }
    let removed = tokens.len();
    tokio::task::spawn_blocking(move || {
        for token in tokens {
            verify_pending_candidate(&data_directory, token.pending())
                .map_err(|error| error.with_request_id(request_id).with_table_id(table_id))?;
            data_directory
                .remove_durable_file(token.path())
                .map_err(|error| {
                    map_data_directory_error(table_id, error)
                        .with_request_id(request_id)
                        .with_table_id(table_id)
                })?;
        }
        Ok::<(), ServerError>(())
    })
    .await
    .map_err(|error| actor_join_error("remove definitively rejected candidates", error))??;
    tracing::info!(
        request_id = %request_id,
        table_id = %table_id,
        removed,
        "removed controller-published candidates after Catalog rejection"
    );
    Ok(())
}

async fn execute_intent(
    journal: &Arc<CommitIntentJournal>,
    catalog: &Arc<dyn CatalogWrite>,
    persistence_catalog: Option<&dyn CatalogPersistence>,
    config: &CatalogCommitCoordinatorConfig,
    intent: DurableCommitIntent,
    mut initial_publication_permit: Option<CatalogPublicationPermit>,
) -> Result<IntentExecution> {
    let verify_journal = Arc::clone(journal);
    let verify_intent = intent.clone();
    tokio::task::spawn_blocking(move || verify_journal.verify_candidates(&verify_intent))
        .await
        .map_err(|error| actor_join_error("verify durable commit candidates", error))??;

    let mut delay = config.retry_initial_delay;
    let mut attempts = 0_u64;
    let response = loop {
        let result = match initial_publication_permit.take() {
            Some(permit) => {
                let persistence_catalog = persistence_catalog.ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "staged commit lost Catalog publication control before mutation",
                    )
                    .with_request_id(intent.path.request_id)
                    .with_retryable(false)
                })?;
                persistence_catalog
                    .command_with_publication_permit(permit, intent.command.clone())
                    .await
            }
            None => catalog.command(intent.command.clone()).await,
        };
        match result {
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
                return Ok(IntentExecution::DefinitiveRejection { error, intent });
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

fn historical_replay_missing(request_id: RequestId, table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        "historical persist replay has no retained Catalog idempotency record",
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
    .with_retryable(false)
}

fn validate_cleanup_tokens(
    request_id: RequestId,
    table_id: TableId,
    tokens: &[PublishedCandidateCleanup],
) -> Result<()> {
    for token in tokens {
        if token.table_id() != table_id {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "candidate cleanup token belongs to another table",
            )
            .with_request_id(request_id)
            .with_table_id(table_id)
            .with_retryable(false));
        }
    }
    for pair in tokens.windows(2) {
        if pair[0].file_id() == pair[1].file_id() || pair[0].path() == pair[1].path() {
            return Err(ServerError::new(
                ErrorCode::DataCorruption,
                "candidate cleanup tokens contain duplicate identities or paths",
            )
            .with_request_id(request_id)
            .with_table_id(table_id)
            .with_retryable(false));
        }
    }
    Ok(())
}

fn cleanup_revision_changed(
    request_id: RequestId,
    table_id: TableId,
    expected: u64,
    actual: u64,
) -> ServerError {
    ServerError::new(
        ErrorCode::Conflict,
        format!(
            "Catalog revision changed during candidate cleanup expected=[{expected}] actual=[{actual}]"
        ),
    )
    .with_request_id(request_id)
    .with_table_id(table_id)
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
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use tempfile::TempDir;
    use tokio::sync::Semaphore;
    use zippy_catalog::{SqliteCatalog, SqliteCatalogConfig};
    use zippy_types::catalog::{
        CatalogDataFileReferenceSnapshot, CatalogFileRead, CatalogIdempotencyRead,
        CatalogPublicationControl, CatalogPublicationExclusion, CatalogQuerySnapshot, CatalogRead,
        CatalogSnapshotSelector, CreateNamespaceCommand, CreateTableCommand, ListNamespacesRequest,
        ListNamespacesResponse, ListTablesRequest, ListTablesResponse, NamespaceRef,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, MetadataMap, NamespaceDescriptor,
        NativeDataFileProvenance, SchemaIpcPayload, SourceSegmentIdentity, SourceSegmentLineage,
        SourceSegmentLineages, TableDefinition, TableDescriptor, TableRef,
    };
    use zippy_types::{FileId, LogOffset, NamespaceId, OffsetRange};

    use crate::candidate::{
        ParquetCandidateSpec, ParquetCandidateWriter, PreparedParquetCandidateSpec,
    };

    use super::*;

    const TEST_NAMESPACE_ID: u128 = 1;
    const TEST_TABLE_ID: u128 = 2;
    const SECOND_REQUEST_ID: u128 = 21;

    struct RejectFirstPublicationCatalog {
        inner: Arc<SqliteCatalog>,
        reject_next: AtomicBool,
        acquire_error: Option<ServerError>,
        first_command_started: Semaphore,
        release_first_command: Semaphore,
        publication_acquisitions: AtomicUsize,
    }

    impl RejectFirstPublicationCatalog {
        fn new(inner: Arc<SqliteCatalog>) -> Self {
            Self {
                inner,
                reject_next: AtomicBool::new(true),
                acquire_error: None,
                first_command_started: Semaphore::new(0),
                release_first_command: Semaphore::new(0),
                publication_acquisitions: AtomicUsize::new(0),
            }
        }

        fn with_acquire_error(inner: Arc<SqliteCatalog>, error: ServerError) -> Self {
            Self {
                inner,
                reject_next: AtomicBool::new(false),
                acquire_error: Some(error),
                first_command_started: Semaphore::new(0),
                release_first_command: Semaphore::new(0),
                publication_acquisitions: AtomicUsize::new(0),
            }
        }

        fn publication_acquisitions(&self) -> usize {
            self.publication_acquisitions.load(Ordering::Acquire)
        }
    }

    impl CatalogRead for RejectFirstPublicationCatalog {
        fn revision(&self) -> zippy_types::BoxFuture<'_, Result<u64>> {
            self.inner.revision()
        }

        fn get_namespace(
            &self,
            namespace: NamespaceRef,
        ) -> zippy_types::BoxFuture<'_, Result<NamespaceDescriptor>> {
            self.inner.get_namespace(namespace)
        }

        fn list_namespaces(
            &self,
            request: ListNamespacesRequest,
        ) -> zippy_types::BoxFuture<'_, Result<ListNamespacesResponse>> {
            self.inner.list_namespaces(request)
        }

        fn get_table(
            &self,
            table: TableRef,
        ) -> zippy_types::BoxFuture<'_, Result<TableDescriptor>> {
            self.inner.get_table(table)
        }

        fn list_tables(
            &self,
            request: ListTablesRequest,
        ) -> zippy_types::BoxFuture<'_, Result<ListTablesResponse>> {
            self.inner.list_tables(request)
        }

        fn resolve_tables<'a>(
            &'a self,
            tables: &'a [TableRef],
        ) -> zippy_types::BoxFuture<'a, Result<Vec<TableDescriptor>>> {
            self.inner.resolve_tables(tables)
        }

        fn read_snapshot_at<'a>(
            &'a self,
            selectors: &'a [CatalogSnapshotSelector],
        ) -> zippy_types::BoxFuture<'a, Result<CatalogQuerySnapshot>> {
            self.inner.read_snapshot_at(selectors)
        }
    }

    impl CatalogFileRead for RejectFirstPublicationCatalog {
        fn inspect_data_file_references<'a>(
            &'a self,
            probes: &'a [ManagedDataFileProbe],
        ) -> zippy_types::BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
            self.inner.inspect_data_file_references(probes)
        }
    }

    impl CatalogIdempotencyRead for RejectFirstPublicationCatalog {
        fn probe_command_replay<'a>(
            &'a self,
            command: &'a VersionedCatalogCommand,
        ) -> zippy_types::BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
            self.inner.probe_command_replay(command)
        }
    }

    impl CatalogWrite for RejectFirstPublicationCatalog {
        fn command(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::BoxFuture<'_, Result<VersionedCatalogResponse>> {
            self.inner.command(command)
        }
    }

    impl CatalogPublicationControl for RejectFirstPublicationCatalog {
        fn acquire_publication_permit(
            &self,
        ) -> zippy_types::BoxFuture<'_, Result<CatalogPublicationPermit>> {
            Box::pin(async move {
                self.publication_acquisitions.fetch_add(1, Ordering::AcqRel);
                if let Some(error) = &self.acquire_error {
                    return Err(error.clone());
                }
                self.inner.acquire_publication_permit().await
            })
        }

        fn command_with_publication_permit(
            &self,
            permit: CatalogPublicationPermit,
            command: VersionedCatalogCommand,
        ) -> zippy_types::BoxFuture<'_, Result<VersionedCatalogResponse>> {
            Box::pin(async move {
                if self.reject_next.swap(false, Ordering::AcqRel) {
                    self.first_command_started.add_permits(1);
                    self.release_first_command
                        .acquire()
                        .await
                        .expect("release first Catalog command")
                        .forget();
                    drop(permit);
                    let VersionedCatalogCommand::V1(envelope) = &command;
                    return Err(ServerError::new(
                        ErrorCode::Conflict,
                        "injected first staged commit rejection",
                    )
                    .with_request_id(envelope.request_id)
                    .with_retryable(false));
                }
                self.inner
                    .command_with_publication_permit(permit, command)
                    .await
            })
        }

        fn acquire_publication_exclusion(
            &self,
        ) -> zippy_types::BoxFuture<'_, Result<CatalogPublicationExclusion>> {
            self.inner.acquire_publication_exclusion()
        }
    }

    fn test_request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("test request id must be nonzero")
    }

    fn test_namespace_id() -> NamespaceId {
        NamespaceId::from_u128(TEST_NAMESPACE_ID).expect("test namespace id must be nonzero")
    }

    fn test_table_id() -> TableId {
        TableId::from_u128(TEST_TABLE_ID).expect("test table id must be nonzero")
    }

    fn test_file_id(value: u128) -> FileId {
        FileId::from_u128(value).expect("test file id must be nonzero")
    }

    fn test_actor() -> CatalogActor {
        CatalogActor {
            kind: CatalogActorKind::Server,
            principal: "zippy-storage-commit-test".to_owned(),
        }
    }

    fn test_command(request: u128, command: CatalogCommandV1) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(test_request_id(request), None, test_actor(), command)
            .expect("construct test Catalog command")
            .into()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
    }

    fn test_schema_ipc(schema: &Schema) -> SchemaIpcPayload {
        let mut bytes = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut bytes, schema).expect("create test schema IPC writer");
        writer.finish().expect("finish test schema IPC writer");
        drop(writer);
        SchemaIpcPayload::new(bytes).expect("nonempty test schema IPC")
    }

    async fn create_test_table(catalog: &SqliteCatalog) {
        catalog
            .command(test_command(
                1,
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: test_namespace_id(),
                    name: CatalogName::new("market").expect("valid test namespace"),
                }),
            ))
            .await
            .expect("create test namespace");
        let schema = test_schema();
        catalog
            .command(test_command(
                2,
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: test_table_id(),
                    namespace_id: test_namespace_id(),
                    name: CatalogName::new("ticks").expect("valid test table"),
                    definition: TableDefinition::Append(AppendTableDefinition::default()),
                    schema_ipc: test_schema_ipc(schema.as_ref()),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create test table");
    }

    fn stage_test_candidate(
        writer: &ParquetCandidateWriter,
        file: u128,
        offset: u64,
        value: i64,
    ) -> StagedParquetCandidate {
        let schema = test_schema();
        let offsets = OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset))
            .expect("one-row test offset range");
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(vec![SourceSegmentLineage {
                source: SourceSegmentIdentity {
                    segment_id: offset + 1,
                    generation: 1,
                    writer_epoch: 0,
                },
                offsets,
            }])
            .expect("test source lineage"),
        )
        .expect("test file provenance");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .expect("one-row test batch");
        let prepared = PreparedParquetCandidateSpec::new(ParquetCandidateSpec {
            file_id: test_file_id(file),
            table_id: test_table_id(),
            schema_version: 1,
            schema,
            provenance,
            partition: MetadataMap::new(),
            metadata: MetadataMap::new(),
            created_at_ns: 1_752_492_600_000_000_000 + file as u64,
        })
        .expect("prepare test candidate");
        writer
            .stage_prepared_candidate(prepared, vec![batch])
            .expect("stage test candidate")
    }

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queued_staged_job_does_not_hold_publication_permit_during_rejection_cleanup() {
        let root = TempDir::new().expect("create test root");
        let data_directory = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
                .expect("own test data root"),
        );
        let sqlite = Arc::new(
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
                .expect("open test Catalog"),
        );
        create_test_table(&sqlite).await;
        let catalog = Arc::new(RejectFirstPublicationCatalog::new(Arc::clone(&sqlite)));
        let persistence: Arc<dyn CatalogPersistence> = catalog.clone();
        let coordinator = Arc::new(
            CatalogCommitCoordinator::start_with_persistence(
                CatalogCommitCoordinatorConfig::new().with_max_outstanding_commits(2),
                Arc::clone(&data_directory),
                persistence,
                test_actor(),
            )
            .await
            .expect("start test commit coordinator"),
        );
        let writer = ParquetCandidateWriter::new(Arc::clone(&data_directory));
        let first_candidate = stage_test_candidate(&writer, 10, 0, 100);
        let second_candidate = stage_test_candidate(&writer, 11, 0, 101);

        let first_coordinator = Arc::clone(&coordinator);
        let first = tokio::spawn(async move {
            first_coordinator
                .commit_staged_classified(
                    PersistMode::Advance,
                    test_request_id(20),
                    test_table_id(),
                    1,
                    vec![first_candidate],
                    OffsetWatermark::Known(LogOffset::new(0)),
                )
                .await
        });
        catalog
            .first_command_started
            .acquire()
            .await
            .expect("first staged command must reach Catalog")
            .forget();
        assert_eq!(catalog.publication_acquisitions(), 1);

        let second_coordinator = Arc::clone(&coordinator);
        let second = tokio::spawn(async move {
            second_coordinator
                .commit_staged_classified(
                    PersistMode::Advance,
                    test_request_id(SECOND_REQUEST_ID),
                    test_table_id(),
                    1,
                    vec![second_candidate],
                    OffsetWatermark::Known(LogOffset::new(0)),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let capacity = coordinator
                    .sender
                    .as_ref()
                    .expect("coordinator remains open")
                    .capacity();
                if capacity == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second staged job must enter the FIFO queue");
        assert_eq!(
            catalog.publication_acquisitions(),
            1,
            "a queued staged job must not hold a publication permit"
        );

        catalog.release_first_command.add_permits(1);
        let first_result = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first rejection cleanup must not deadlock")
            .expect("join first staged commit");
        assert!(matches!(
            first_result,
            Err(CatalogCommitFailure::DefinitiveRejection(ref error))
                if error.code() == ErrorCode::Conflict
        ));
        let second_receipt = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second staged commit must run after cleanup")
            .expect("join second staged commit")
            .expect("second staged commit succeeds");
        assert_eq!(second_receipt.snapshot.snapshot_version, 2);
        assert_eq!(catalog.publication_acquisitions(), 2);

        let coordinator = Arc::try_unwrap(coordinator)
            .unwrap_or_else(|_| panic!("test tasks must release coordinator references"));
        coordinator.drain().await.expect("drain test coordinator");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publication_permit_failure_reaches_the_current_commit_caller() {
        let root = TempDir::new().expect("create test root");
        let data_directory = Arc::new(
            OwnedDataDirectory::acquire_or_initialize(root.path().join("data"))
                .expect("own test data root"),
        );
        let sqlite = Arc::new(
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory))
                .expect("open test Catalog"),
        );
        create_test_table(&sqlite).await;
        let injected = ServerError::new(
            ErrorCode::DataCorruption,
            "injected publication permit acquisition failure",
        )
        .with_retryable(false);
        let catalog = Arc::new(RejectFirstPublicationCatalog::with_acquire_error(
            Arc::clone(&sqlite),
            injected.clone(),
        ));
        let persistence: Arc<dyn CatalogPersistence> = catalog;
        let coordinator = CatalogCommitCoordinator::start_with_persistence(
            CatalogCommitCoordinatorConfig::new(),
            Arc::clone(&data_directory),
            persistence,
            test_actor(),
        )
        .await
        .expect("start test commit coordinator");
        let candidate =
            stage_test_candidate(&ParquetCandidateWriter::new(data_directory), 10, 0, 100);
        let expected = injected
            .with_request_id(test_request_id(20))
            .with_table_id(test_table_id());

        let failure = coordinator
            .commit_staged_classified(
                PersistMode::Advance,
                test_request_id(20),
                test_table_id(),
                1,
                vec![candidate],
                OffsetWatermark::Known(LogOffset::new(0)),
            )
            .await
            .expect_err("permit acquisition failure must reject the current commit");
        assert!(matches!(failure, CatalogCommitFailure::Fatal(error) if error == expected));

        let drain_error = coordinator
            .drain()
            .await
            .expect_err("terminal permit failure must stop the coordinator");
        assert_eq!(drain_error, expected);
    }
}
