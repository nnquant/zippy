//! Bounded per-table serialization and lifecycle ownership.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use zippy_types::{ErrorCode, OffsetWatermark, Result, ServerError, TableId};

use crate::{TableRuntime, TableRuntimeRegistry};

const DEFAULT_PER_TABLE_MAILBOX_CAPACITY: usize = 64;
const DEFAULT_MAX_OUTSTANDING_COMMANDS: usize = 4_096;
const MAX_PER_TABLE_MAILBOX_CAPACITY: usize = 65_536;
const MAX_OUTSTANDING_COMMANDS: usize = 1_048_576;

/// Bounded admission policy shared by all table actors in one server runtime.
#[derive(Debug, Clone)]
pub struct TableActorRegistryConfig {
    per_table_mailbox_capacity: usize,
    max_outstanding_commands: usize,
}

impl Default for TableActorRegistryConfig {
    fn default() -> Self {
        Self {
            per_table_mailbox_capacity: DEFAULT_PER_TABLE_MAILBOX_CAPACITY,
            max_outstanding_commands: DEFAULT_MAX_OUTSTANDING_COMMANDS,
        }
    }
}

impl TableActorRegistryConfig {
    /// Creates the default 64-command per-table and 4,096-command global policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum commands queued for any one table.
    #[must_use]
    pub const fn with_per_table_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.per_table_mailbox_capacity = capacity;
        self
    }

    /// Sets the maximum commands admitted across every table actor.
    #[must_use]
    pub const fn with_max_outstanding_commands(mut self, maximum: usize) -> Self {
        self.max_outstanding_commands = maximum;
        self
    }

    /// Validates all allocation bounds without starting a runtime or actor.
    pub fn validate(&self) -> Result<()> {
        validate_config(self)
    }
}

/// A compact observation taken in one table actor turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableActorSnapshot {
    /// The observed table identity.
    pub table_id: TableId,
    /// The current schema version bound to future native writes.
    pub schema_version: u64,
    /// The latest persisted snapshot observed by the actor.
    pub persisted_snapshot_version: u64,
    /// The current producer fencing epoch.
    pub writer_epoch: u64,
    /// The durable logical offset represented by the persisted snapshot.
    pub committed_log_offset: OffsetWatermark,
    /// Monotonic process-local command sequence assigned by this actor.
    pub actor_command_sequence: u64,
}

/// One successfully admitted snapshot request awaiting its actor turn.
///
/// Dropping this value abandons only the response. The admitted actor command
/// still executes and releases its global budget permit.
#[derive(Debug)]
pub struct TableActorSnapshotRequest {
    table_id: TableId,
    response: oneshot::Receiver<Result<TableActorSnapshot>>,
}

impl TableActorSnapshotRequest {
    /// Waits for the serialized actor observation.
    pub async fn wait(self) -> Result<TableActorSnapshot> {
        self.response.await.unwrap_or_else(|_| {
            Err(ServerError::new(
                ErrorCode::Internal,
                "table actor stopped before returning an accepted snapshot request",
            )
            .with_table_id(self.table_id)
            .with_retryable(false))
        })
    }
}

/// Cloneable admission handle for one server-owned table actor.
#[derive(Clone, Debug)]
pub struct TableActorHandle {
    table_id: TableId,
    sender: mpsc::Sender<TableActorCommand>,
    accepting: Arc<AtomicBool>,
    global_budget: Arc<Semaphore>,
}

impl TableActorHandle {
    /// Returns the immutable table identity served by this actor.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Attempts immediate bounded admission without waiting for queue capacity.
    pub fn try_snapshot(&self) -> Result<TableActorSnapshotRequest> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(actor_closed(self.table_id));
        }
        let permit = Arc::clone(&self.global_budget)
            .try_acquire_owned()
            .map_err(|_| actor_global_budget_exhausted(self.table_id))?;
        let (response_sender, response) = oneshot::channel();
        let command = TableActorCommand::Snapshot {
            response: response_sender,
            _permit: permit,
        };
        match self.sender.try_send(command) {
            Ok(()) => Ok(TableActorSnapshotRequest {
                table_id: self.table_id,
                response,
            }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(actor_mailbox_full(self.table_id)),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(actor_closed(self.table_id)),
        }
    }

    /// Admits and waits for one serialized actor observation.
    pub async fn snapshot(&self) -> Result<TableActorSnapshot> {
        self.try_snapshot()?.wait().await
    }
}

/// Cloneable lookup surface published after every table actor has started.
#[derive(Clone)]
pub struct TableActorRegistryHandle {
    catalog_revision: u64,
    tables: Arc<BTreeMap<TableId, TableActorHandle>>,
    accepting: Arc<AtomicBool>,
}

/// Non-cloneable composition-root control for immediately closing actor admission.
///
/// This value is deliberately not cloneable and is not carried by normal table
/// handles. The server lifecycle owner uses it to make `begin_shutdown`
/// synchronous from the perspective of new command admission.
pub struct TableActorAdmissionControl {
    accepting: Arc<AtomicBool>,
}

impl TableActorAdmissionControl {
    /// Irreversibly rejects all future commands across the actor registry.
    pub fn close(&self) {
        self.accepting.store(false, Ordering::Release);
    }
}

impl TableActorRegistryHandle {
    /// Returns the Catalog revision shared by all actor bootstrap states.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the number of table actors published atomically at startup.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns whether no active table actor was recovered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Returns whether this registry still admits actor commands.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Resolves a table actor by immutable identity.
    pub fn table(&self, table_id: TableId) -> Result<TableActorHandle> {
        self.tables.get(&table_id).cloned().ok_or_else(|| {
            ServerError::new(ErrorCode::NotFound, "native table actor was not found")
                .with_table_id(table_id)
                .with_retryable(false)
        })
    }
}

/// Exclusive lifecycle owner for all table actors in one native storage runtime.
///
/// The owner is intentionally not cloneable. [`TableActorRegistryHandle`] may
/// be shared with service adapters, while only this value can close admission
/// and join every actor during ordered server shutdown.
pub struct TableActorRegistry {
    handle: TableActorRegistryHandle,
    shutdown: watch::Sender<bool>,
    actors: Vec<(TableId, JoinHandle<Result<()>>)>,
    shutdown_started: bool,
}

impl TableActorRegistry {
    /// Starts one bounded serial actor for every bootstrapped active table.
    pub fn start(config: TableActorRegistryConfig, runtimes: TableRuntimeRegistry) -> Result<Self> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("start table actors outside a Tokio runtime error=[{error}]"),
            )
            .with_retryable(false)
        })?;
        let accepting = Arc::new(AtomicBool::new(true));
        let global_budget = Arc::new(Semaphore::new(config.max_outstanding_commands));
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let catalog_revision = runtimes.catalog_revision();
        let mut tables = BTreeMap::new();
        let mut actors = Vec::with_capacity(runtimes.len());

        for runtime in runtimes.iter() {
            let table_id = runtime.table_id();
            let (sender, receiver) = mpsc::channel(config.per_table_mailbox_capacity);
            let actor = tokio::spawn(run_table_actor(
                Arc::clone(runtime),
                receiver,
                shutdown_receiver.clone(),
            ));
            let handle = TableActorHandle {
                table_id,
                sender,
                accepting: Arc::clone(&accepting),
                global_budget: Arc::clone(&global_budget),
            };
            if tables.insert(table_id, handle).is_some() {
                accepting.store(false, Ordering::Release);
                let _ = shutdown.send(true);
                return Err(corruption(format!(
                    "table actor registry contains duplicate table id table_id=[{table_id}]"
                ))
                .with_table_id(table_id));
            }
            actors.push((table_id, actor));
        }
        drop(shutdown_receiver);
        Ok(Self {
            handle: TableActorRegistryHandle {
                catalog_revision,
                tables: Arc::new(tables),
                accepting,
            },
            shutdown,
            actors,
            shutdown_started: false,
        })
    }

    /// Returns the cloneable, read-only admission and lookup surface.
    #[must_use]
    pub fn handle(&self) -> TableActorRegistryHandle {
        self.handle.clone()
    }

    /// Transfers a non-cloneable admission control to the server lifecycle owner.
    #[must_use]
    pub fn admission_control(&self) -> TableActorAdmissionControl {
        TableActorAdmissionControl {
            accepting: Arc::clone(&self.handle.accepting),
        }
    }

    /// Irreversibly closes new command admission for every table actor.
    pub fn close_admission(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.shutdown_started = true;
        self.handle.accepting.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
    }

    /// Closes admission, drains accepted commands, and joins every actor.
    pub async fn drain(mut self) -> Result<()> {
        self.close_admission();
        let mut first_error = None;
        for (table_id, actor) in self.actors.drain(..) {
            let outcome = actor.await.map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("join table actor task failed error=[{error}]"),
                )
                .with_table_id(table_id)
                .with_retryable(false)
            });
            let outcome = outcome.and_then(|result| result);
            if first_error.is_none() {
                first_error = outcome.err();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for TableActorRegistry {
    fn drop(&mut self) {
        self.close_admission();
    }
}

enum TableActorCommand {
    Snapshot {
        response: oneshot::Sender<Result<TableActorSnapshot>>,
        _permit: OwnedSemaphorePermit,
    },
    #[cfg(test)]
    Panic,
}

async fn run_table_actor(
    runtime: Arc<TableRuntime>,
    mut receiver: mpsc::Receiver<TableActorCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut command_sequence = 0_u64;
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                receiver.close();
                while let Some(command) = receiver.recv().await {
                    process_command(&runtime, &mut command_sequence, command);
                }
                return Ok(());
            }
            command = receiver.recv() => {
                match command {
                    Some(command) => process_command(&runtime, &mut command_sequence, command),
                    None => return Ok(()),
                }
            }
        }
    }
}

fn process_command(runtime: &TableRuntime, command_sequence: &mut u64, command: TableActorCommand) {
    let next_sequence = command_sequence.checked_add(1).ok_or_else(|| {
        ServerError::new(
            ErrorCode::Internal,
            "table actor command sequence exhausted u64",
        )
        .with_table_id(runtime.table_id())
        .with_retryable(false)
    });
    match command {
        TableActorCommand::Snapshot { response, .. } => {
            let result = next_sequence.map(|sequence| {
                *command_sequence = sequence;
                TableActorSnapshot {
                    table_id: runtime.table_id(),
                    schema_version: runtime.schema_descriptor().schema_version,
                    persisted_snapshot_version: runtime.snapshot().snapshot_version,
                    writer_epoch: runtime.writer_epoch(),
                    committed_log_offset: runtime.committed_log_offset(),
                    actor_command_sequence: sequence,
                }
            });
            let _ = response.send(result);
        }
        #[cfg(test)]
        TableActorCommand::Panic => panic!("injected table actor panic"),
    }
}

fn validate_config(config: &TableActorRegistryConfig) -> Result<()> {
    if config.per_table_mailbox_capacity == 0
        || config.per_table_mailbox_capacity > MAX_PER_TABLE_MAILBOX_CAPACITY
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                concat!(
                    "table actor per-table mailbox capacity is outside supported range ",
                    "actual=[{}] maximum=[{}]"
                ),
                config.per_table_mailbox_capacity, MAX_PER_TABLE_MAILBOX_CAPACITY
            ),
        )
        .with_retryable(false));
    }
    if config.max_outstanding_commands == 0
        || config.max_outstanding_commands > MAX_OUTSTANDING_COMMANDS
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                concat!(
                    "table actor global command capacity is outside supported range ",
                    "actual=[{}] maximum=[{}]"
                ),
                config.max_outstanding_commands, MAX_OUTSTANDING_COMMANDS
            ),
        )
        .with_retryable(false));
    }
    Ok(())
}

fn actor_closed(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "native table actor is not accepting commands",
    )
    .with_table_id(table_id)
    .with_retryable(false)
}

fn actor_mailbox_full(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native table actor mailbox is full",
    )
    .with_table_id(table_id)
    .with_retryable(true)
}

fn actor_global_budget_exhausted(table_id: TableId) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        "native table actor global command budget is exhausted",
    )
    .with_table_id(table_id)
    .with_retryable(true)
}

fn corruption(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, SchemaCompatibility, SchemaDescriptor,
        SchemaIpcPayload, SnapshotOperation, TableDefinition, TableDescriptor, TableSnapshot,
        TableState,
    };
    use zippy_types::{ErrorCode, NamespaceId, OffsetWatermark, TableId};

    use crate::{TableRuntimeBootstrap, TableRuntimeRegistry};

    use super::{TableActorCommand, TableActorRegistry, TableActorRegistryConfig};

    fn table_id(value: u128) -> TableId {
        TableId::from_u128(value).unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]))
    }

    fn runtime_registry(table_values: &[u128]) -> TableRuntimeRegistry {
        let bootstraps = table_values
            .iter()
            .map(|value| {
                let table_id = table_id(*value);
                TableRuntimeBootstrap::new(
                    TableDescriptor {
                        table_id,
                        namespace_id: NamespaceId::from_u128(1).unwrap(),
                        name: CatalogName::new(format!("table_{value}")).unwrap(),
                        definition: TableDefinition::Append(AppendTableDefinition::default()),
                        current_schema_version: 1,
                        current_snapshot_version: 1,
                        writer_epoch: 7,
                        state: TableState::Active,
                        config: BTreeMap::new(),
                        created_at_ns: 1,
                        updated_at_ns: 1,
                    },
                    SchemaDescriptor {
                        table_id,
                        schema_version: 1,
                        schema_ipc: SchemaIpcPayload::new(vec![1]).unwrap(),
                        schema_hash: "sha256-v1:test".to_owned(),
                        compatibility: SchemaCompatibility::Initial,
                        created_at_ns: 1,
                    },
                    schema(),
                    TableSnapshot {
                        table_id,
                        snapshot_version: 1,
                        parent_snapshot_version: None,
                        operation: SnapshotOperation::Create,
                        commit_lsn: 1,
                        committed_log_offset: OffsetWatermark::Empty,
                        created_at_ns: 1,
                        summary: BTreeMap::new(),
                    },
                )
                .unwrap()
            })
            .collect();
        TableRuntimeRegistry::from_bootstraps(9, bootstraps).unwrap()
    }

    #[tokio::test]
    async fn actor_serializes_observations_and_drains_cleanly() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();

        let first = actor.snapshot().await.unwrap();
        let second = actor.snapshot().await.unwrap();

        assert_eq!(first.actor_command_sequence, 1);
        assert_eq!(second.actor_command_sequence, 2);
        assert_eq!(second.writer_epoch, 7);
        assert_eq!(second.committed_log_offset, OffsetWatermark::Empty);
        registry.drain().await.unwrap();
        assert!(!handle.is_accepting());
        assert_eq!(
            actor.snapshot().await.unwrap_err().code(),
            ErrorCode::Unavailable
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_table_and_global_admission_are_strictly_bounded() {
        let registry = TableActorRegistry::start(
            TableActorRegistryConfig::new()
                .with_per_table_mailbox_capacity(1)
                .with_max_outstanding_commands(2),
            runtime_registry(&[10, 20]),
        )
        .unwrap();
        let handle = registry.handle();
        let first_actor = handle.table(table_id(10)).unwrap();
        let second_actor = handle.table(table_id(20)).unwrap();

        let first = first_actor.try_snapshot().unwrap();
        let table_full = first_actor.try_snapshot().unwrap_err();
        assert_eq!(table_full.code(), ErrorCode::ResourceExhausted);
        assert!(table_full.retryable());
        let second = second_actor.try_snapshot().unwrap();
        let global_full = second_actor.try_snapshot().unwrap_err();
        assert_eq!(global_full.code(), ErrorCode::ResourceExhausted);
        assert!(global_full.retryable());

        assert_eq!(first.wait().await.unwrap().actor_command_sequence, 1);
        assert_eq!(second.wait().await.unwrap().actor_command_sequence, 1);
        registry.drain().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_drains_requests_accepted_before_admission_closed() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let handle = registry.handle();
        let actor = handle.table(table_id(10)).unwrap();
        let accepted = actor.try_snapshot().unwrap();

        let drain = tokio::spawn(registry.drain());
        let snapshot = accepted.wait().await.unwrap();

        assert_eq!(snapshot.actor_command_sequence, 1);
        drain.await.unwrap().unwrap();
        assert!(!handle.is_accepting());
    }

    #[tokio::test]
    async fn actor_panic_closes_its_mailbox_and_is_reported_on_drain() {
        let registry =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .unwrap();
        let actor = registry.handle().table(table_id(10)).unwrap();
        assert!(actor.sender.try_send(TableActorCommand::Panic).is_ok());
        tokio::task::yield_now().await;

        let request_error = actor.snapshot().await.unwrap_err();
        assert_eq!(request_error.code(), ErrorCode::Unavailable);
        let drain_error = registry.drain().await.unwrap_err();
        assert_eq!(drain_error.code(), ErrorCode::Internal);
        assert_eq!(drain_error.table_id(), Some(table_id(10)));
    }

    #[test]
    fn invalid_actor_budget_fails_before_spawning_tasks() {
        let error = TableActorRegistry::start(
            TableActorRegistryConfig::new().with_per_table_mailbox_capacity(0),
            runtime_registry(&[10]),
        )
        .err()
        .expect("invalid config must not return an actor registry");
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn actor_registry_requires_an_explicit_tokio_runtime() {
        let error =
            TableActorRegistry::start(TableActorRegistryConfig::new(), runtime_registry(&[10]))
                .err()
                .expect("starting actors without a runtime must fail structurally");
        assert_eq!(error.code(), ErrorCode::Unavailable);
    }
}
