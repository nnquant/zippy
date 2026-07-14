//! Asynchronous Catalog traits backed by isolated SQLite writer and read workers.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, TrySendError};
use rusqlite::Connection;
use tokio::sync::oneshot;
use zippy_types::catalog::{
    CatalogDataFileReferenceSnapshot, CatalogFileRead, CatalogQuerySnapshot, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, ListNamespacesRequest, ListNamespacesResponse,
    ListTablesRequest, ListTablesResponse, ManagedDataFileProbe, NamespaceRef,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, ErrorCode, RequestId, Result, ServerError};

use crate::SqliteCatalogConfig;
use crate::{domain, file_domain, manifest_domain, snapshot_domain, table_domain};

const SERVICE_ACTOR_THREAD_NAME: &str = "zippy-catalog-service";
const MANIFEST_PREPARATION_THREAD_NAME: &str = "zippy-catalog-manifest-prepare";
const CONTROL_READ_THREAD_NAME: &str = "zippy-catalog-control-read";
const SNAPSHOT_READ_THREAD_NAME: &str = "zippy-catalog-snapshot-read";

/// A typed SQLite Catalog with thread-confined writer and read-only connections.
///
/// Catalog trait futures submit through a bounded nonblocking queue and await a
/// Tokio oneshot response. SQLite work therefore never blocks an async executor
/// thread. Every trait future is cold: constructing and dropping it before its
/// first poll neither occupies the actor queue nor executes any operation. Once
/// a command future has been polled and successfully enqueued, cancelling it
/// abandons only the response; the actor still executes the command. Callers
/// must treat that case as commit-unknown and replay the same request ID.
/// Cancelled reads may be skipped before execution.
///
/// Mutations retain their actor admission order. While a manifest mutation is
/// being prepared outside SQLite, a later read may complete first and observe
/// the preceding committed revision. A caller requiring read-after-write must
/// await the mutation response before issuing the read. `open`, `close`, and
/// `Drop` remain synchronous in this migration slice and must not be wired into
/// the server runtime until shutdown receives an absolute deadline.
pub struct SqliteCatalog {
    sender: Option<Sender<ServiceMessage>>,
    actor: Option<JoinHandle<()>>,
    read_pool: Option<CatalogReadPool>,
    command_budget: Arc<CommandBudget>,
    manifest_queue_budget: Arc<ManifestQueueBudget>,
}

impl SqliteCatalog {
    /// Opens or migrates the Catalog and starts its connection-owning actor.
    ///
    /// Startup is synchronous and returns only after the existing foundation
    /// preflight, durable SQLite configuration, migrations, and integrity check
    /// have completed on the actor thread.
    pub fn open(config: SqliteCatalogConfig) -> Result<Self> {
        Self::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            Arc::new(DomainCatalogReadBackend),
        )
    }

    #[cfg(test)]
    fn open_with_manifest_backend(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
    ) -> Result<Self> {
        Self::open_with_backends(config, manifest_backend, Arc::new(DomainCatalogReadBackend))
    }

    fn open_with_backends(
        config: SqliteCatalogConfig,
        manifest_backend: Arc<dyn ManifestPreparationBackend>,
        read_backend: Arc<dyn CatalogReadBackend>,
    ) -> Result<Self> {
        crate::validate_config(&config)?;
        let (sender, receiver) = crossbeam_channel::bounded(config.command_capacity);
        let (startup_sender, startup_receiver) = crossbeam_channel::bounded(1);
        let command_budget = Arc::new(CommandBudget::new(config.command_capacity));
        let manifest_queue_budget = Arc::new(ManifestQueueBudget::new(
            config.manifest_queue_capacity_bytes,
        ));
        let actor_config = config.clone();
        let actor = thread::Builder::new()
            .name(SERVICE_ACTOR_THREAD_NAME.to_owned())
            .spawn(move || {
                run_actor(actor_config, receiver, startup_sender, manifest_backend);
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start sqlite catalog actor thread error=[{error}]"),
                )
            })?;
        match startup_receiver.recv() {
            Ok(Ok(())) => {
                let mut catalog = Self {
                    read_pool: None,
                    sender: Some(sender),
                    actor: Some(actor),
                    command_budget,
                    manifest_queue_budget,
                };
                match CatalogReadPool::start(&config, read_backend) {
                    Ok(read_pool) => {
                        catalog.read_pool = Some(read_pool);
                        Ok(catalog)
                    }
                    Err(error) => {
                        if let Err(cleanup_error) = catalog.shutdown() {
                            tracing::error!(
                                error = %cleanup_error,
                                "sqlite catalog writer cleanup failed after read pool startup error"
                            );
                        }
                        Err(error)
                    }
                }
            }
            Ok(Err(error)) => {
                join_startup_actor(actor)?;
                Err(error)
            }
            Err(_) => {
                join_startup_actor(actor)?;
                Err(actor_stopped())
            }
        }
    }

    /// Gracefully closes SQLite and joins the actor thread synchronously.
    ///
    /// This method is reliable but not deadline-bounded. It is intentionally a
    /// pre-server lifecycle API in the current migration slice.
    pub fn close(mut self) -> Result<()> {
        self.shutdown()
    }

    fn writer_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ServiceMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        let sender = self.sender.as_ref().ok_or_else(actor_stopped)?;
        let (response_sender, response_receiver) = oneshot::channel();
        match sender.try_send(build(response_sender)) {
            Ok(()) => Ok(response_receiver),
            Err(TrySendError::Full(_)) => Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "sqlite catalog actor command queue is full",
            )),
            Err(TrySendError::Disconnected(_)) => Err(actor_stopped()),
        }
    }

    fn read_pool(&self) -> Result<&CatalogReadPool> {
        self.read_pool.as_ref().ok_or_else(read_pool_stopped)
    }

    fn shutdown(&mut self) -> Result<()> {
        let read_result = match self.read_pool.take() {
            Some(mut read_pool) => read_pool.shutdown(),
            None => Ok(()),
        };
        let writer_result = self.shutdown_writer();
        read_result.and(writer_result)
    }

    fn shutdown_writer(&mut self) -> Result<()> {
        let Some(sender) = self.sender.take() else {
            return match self.actor.take() {
                Some(actor) => actor.join().map_err(|_| {
                    ServerError::new(ErrorCode::Internal, "sqlite catalog actor thread panicked")
                }),
                None => Ok(()),
            };
        };
        let (response_sender, response_receiver) = crossbeam_channel::bounded(1);
        let send_result = sender
            .send(ServiceMessage::Shutdown(response_sender))
            .map_err(|_| actor_stopped());
        drop(sender);
        let actor_result = match send_result {
            Ok(()) => match response_receiver.recv() {
                Ok(result) => result,
                Err(_) => Err(actor_stopped()),
            },
            Err(error) => Err(error),
        };
        let join_result = match self.actor.take() {
            Some(actor) => actor.join().map_err(|_| {
                ServerError::new(ErrorCode::Internal, "sqlite catalog actor thread panicked")
            }),
            None => Ok(()),
        };
        actor_result.and(join_result)
    }
}

impl CatalogRead for SqliteCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            await_response(
                self.read_pool()
                    .and_then(|pool| pool.control_request(ControlReadMessage::Revision)),
            )
            .await
        })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::GetNamespace(namespace, sender))
            }))
            .await
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::ListNamespaces(request, sender))
            }))
            .await
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::GetTable(table, sender))
            }))
            .await
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::ListTables(request, sender))
            }))
            .await
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            table_domain::validate_table_resolution_request(tables)?;
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| {
                    ControlReadMessage::ResolveTables(tables.to_vec(), sender)
                })
            }))
            .await
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            snapshot_domain::validate_snapshot_selectors(selectors)?;
            let selectors = selectors.to_vec();
            await_response(self.read_pool().and_then(|pool| {
                pool.snapshot_request(|sender| SnapshotReadMessage::Read(selectors, sender))
            }))
            .await
        })
    }
}

impl CatalogFileRead for SqliteCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        Box::pin(async move {
            file_domain::validate_file_probes(probes)?;
            let probes = probes.to_vec();
            await_response(self.read_pool().and_then(|pool| {
                pool.control_request(|sender| ControlReadMessage::InspectFiles(probes, sender))
            }))
            .await
        })
    }
}

impl CatalogWrite for SqliteCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let VersionedCatalogCommand::V1(envelope) = &command;
            let request_id = envelope.request_id;
            let command_permit = self
                .command_budget
                .reserve()
                .map_err(|error| error.with_request_id(request_id))?;
            let queued_bytes = manifest_domain::manifest_command_preflight_bytes(&command).await?;
            let permit = self
                .manifest_queue_budget
                .reserve(queued_bytes)
                .map_err(|error| error.with_request_id(request_id))?;
            await_response(self.writer_request(|response| {
                ServiceMessage::Command(CommandRequest {
                    command,
                    state: CommandRequestState {
                        request_id,
                        response,
                        _command_permit: command_permit,
                        _manifest_permit: permit,
                    },
                })
            }))
            .await
        })
    }
}

struct CommandBudget {
    used: AtomicUsize,
    maximum: usize,
}

impl CommandBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            maximum,
        }
    }

    fn reserve(self: &Arc<Self>) -> Result<CommandPermit> {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if current >= self.maximum {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "sqlite catalog outstanding command budget exhausted used=[{current}] maximum=[{}]",
                        self.maximum
                    ),
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(CommandPermit {
                        budget: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct CommandPermit {
    budget: Arc<CommandBudget>,
}

impl Drop for CommandPermit {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

struct ManifestQueueBudget {
    used: AtomicUsize,
    maximum: usize,
}

impl ManifestQueueBudget {
    const fn new(maximum: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            maximum,
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Option<ManifestQueuePermit>> {
        if bytes == 0 {
            return Ok(None);
        }
        if bytes > self.maximum {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "sqlite catalog manifest command exceeds queue byte budget requested=[{bytes}] maximum=[{}]",
                    self.maximum
                ),
            )
            .with_retryable(false));
        }
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "sqlite catalog manifest queue byte reservation overflow",
                )
                .with_retryable(false)
            })?;
            if next > self.maximum {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    format!(
                        "sqlite catalog manifest queue byte budget exhausted used=[{current}] requested=[{bytes}] maximum=[{}]",
                        self.maximum
                    ),
                ));
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(Some(ManifestQueuePermit {
                        budget: Arc::clone(self),
                        bytes,
                    }));
                }
                Err(actual) => current = actual,
            }
        }
    }
}

struct ManifestQueuePermit {
    budget: Arc<ManifestQueueBudget>,
    bytes: usize,
}

impl Drop for ManifestQueuePermit {
    fn drop(&mut self) {
        let previous = self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}

trait CatalogReadBackend: Send + Sync + 'static {
    fn revision(&self, connection: &Connection) -> Result<u64> {
        domain::revision(connection)
    }

    fn get_namespace(
        &self,
        connection: &Connection,
        namespace: NamespaceRef,
    ) -> Result<NamespaceDescriptor> {
        domain::get_namespace(connection, namespace)
    }

    fn list_namespaces(
        &self,
        connection: &mut Connection,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        domain::list_namespaces(connection, request)
    }

    fn get_table(&self, connection: &Connection, table: TableRef) -> Result<TableDescriptor> {
        table_domain::get_table(connection, table)
    }

    fn list_tables(
        &self,
        connection: &mut Connection,
        request: ListTablesRequest,
    ) -> Result<ListTablesResponse> {
        table_domain::list_tables(connection, request)
    }

    fn resolve_tables(
        &self,
        connection: &mut Connection,
        tables: &[TableRef],
    ) -> Result<Vec<TableDescriptor>> {
        table_domain::resolve_tables(connection, tables)
    }

    fn read_snapshot(
        &self,
        connection: &mut Connection,
        selectors: &[CatalogSnapshotSelector],
    ) -> Result<CatalogQuerySnapshot> {
        snapshot_domain::read_snapshot_at(connection, selectors)
    }

    fn inspect_data_file_references(
        &self,
        connection: &mut Connection,
        probes: &[ManagedDataFileProbe],
    ) -> Result<CatalogDataFileReferenceSnapshot> {
        file_domain::inspect_data_file_references(connection, probes)
    }
}

struct DomainCatalogReadBackend;

impl CatalogReadBackend for DomainCatalogReadBackend {}

enum ControlReadMessage {
    Revision(oneshot::Sender<Result<u64>>),
    GetNamespace(NamespaceRef, oneshot::Sender<Result<NamespaceDescriptor>>),
    ListNamespaces(
        ListNamespacesRequest,
        oneshot::Sender<Result<ListNamespacesResponse>>,
    ),
    GetTable(TableRef, oneshot::Sender<Result<TableDescriptor>>),
    ListTables(
        ListTablesRequest,
        oneshot::Sender<Result<ListTablesResponse>>,
    ),
    ResolveTables(Vec<TableRef>, oneshot::Sender<Result<Vec<TableDescriptor>>>),
    InspectFiles(
        Vec<ManagedDataFileProbe>,
        oneshot::Sender<Result<CatalogDataFileReferenceSnapshot>>,
    ),
}

enum SnapshotReadMessage {
    Read(
        Vec<CatalogSnapshotSelector>,
        oneshot::Sender<Result<CatalogQuerySnapshot>>,
    ),
}

struct CatalogReadPool {
    control_sender: Option<Sender<ControlReadMessage>>,
    snapshot_sender: Option<Sender<SnapshotReadMessage>>,
    control_actor: Option<JoinHandle<Result<()>>>,
    snapshot_actor: Option<JoinHandle<Result<()>>>,
}

impl CatalogReadPool {
    fn start(config: &SqliteCatalogConfig, backend: Arc<dyn CatalogReadBackend>) -> Result<Self> {
        let (control_sender, control_receiver) =
            crossbeam_channel::bounded(config.control_read_capacity);
        let control_actor = start_read_worker(
            CONTROL_READ_THREAD_NAME,
            config.clone(),
            control_receiver,
            Arc::clone(&backend),
            control_read_loop,
        )?;

        let (snapshot_sender, snapshot_receiver) =
            crossbeam_channel::bounded(config.snapshot_read_capacity);
        let snapshot_actor = match start_read_worker(
            SNAPSHOT_READ_THREAD_NAME,
            config.clone(),
            snapshot_receiver,
            backend,
            snapshot_read_loop,
        ) {
            Ok(actor) => actor,
            Err(error) => {
                drop(control_sender);
                if let Err(cleanup_error) = join_read_worker(control_actor, "control read") {
                    tracing::error!(
                        error = %cleanup_error,
                        "sqlite catalog control read cleanup failed after snapshot startup error"
                    );
                }
                return Err(error);
            }
        };

        Ok(Self {
            control_sender: Some(control_sender),
            snapshot_sender: Some(snapshot_sender),
            control_actor: Some(control_actor),
            snapshot_actor: Some(snapshot_actor),
        })
    }

    fn control_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ControlReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        submit_read_request(
            self.control_sender.as_ref(),
            build,
            "sqlite catalog control read queue is full",
        )
    }

    fn snapshot_request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> SnapshotReadMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        submit_read_request(
            self.snapshot_sender.as_ref(),
            build,
            "sqlite catalog snapshot read queue is full",
        )
    }

    fn shutdown(&mut self) -> Result<()> {
        self.control_sender.take();
        self.snapshot_sender.take();
        let control_result = match self.control_actor.take() {
            Some(actor) => join_read_worker(actor, "control read"),
            None => Ok(()),
        };
        let snapshot_result = match self.snapshot_actor.take() {
            Some(actor) => join_read_worker(actor, "snapshot read"),
            None => Ok(()),
        };
        control_result.and(snapshot_result)
    }
}

fn submit_read_request<M, T>(
    sender: Option<&Sender<M>>,
    build: impl FnOnce(oneshot::Sender<Result<T>>) -> M,
    full_message: &'static str,
) -> Result<oneshot::Receiver<Result<T>>>
where
    M: Send + 'static,
    T: Send + 'static,
{
    let sender = sender.ok_or_else(read_pool_stopped)?;
    let (response_sender, response_receiver) = oneshot::channel();
    match sender.try_send(build(response_sender)) {
        Ok(()) => Ok(response_receiver),
        Err(TrySendError::Full(_)) => {
            Err(ServerError::new(ErrorCode::ResourceExhausted, full_message))
        }
        Err(TrySendError::Disconnected(_)) => Err(read_pool_stopped()),
    }
}

fn start_read_worker<M>(
    thread_name: &'static str,
    config: SqliteCatalogConfig,
    receiver: Receiver<M>,
    backend: Arc<dyn CatalogReadBackend>,
    run: fn(Connection, Receiver<M>, Arc<dyn CatalogReadBackend>) -> Result<()>,
) -> Result<JoinHandle<Result<()>>>
where
    M: Send + 'static,
{
    let (startup_sender, startup_receiver) = crossbeam_channel::bounded(1);
    let actor = thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            let connection = match crate::open_read_connection(&config) {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = startup_sender.send(Err(error.clone()));
                    return Err(error);
                }
            };
            if startup_sender.send(Ok(())).is_err() {
                return connection.close().map_err(|(_, error)| {
                    crate::map_sqlite_error("close unclaimed catalog read connection", error)
                });
            }
            run(connection, receiver, backend)
        })
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Unavailable,
                format!("start sqlite catalog read worker thread=[{thread_name}] error=[{error}]"),
            )
        })?;
    match startup_receiver.recv() {
        Ok(Ok(())) => Ok(actor),
        Ok(Err(error)) => {
            let _ = join_read_worker(actor, thread_name);
            Err(error)
        }
        Err(_) => match join_read_worker(actor, thread_name) {
            Ok(()) => Err(read_pool_stopped()),
            Err(error) => Err(error),
        },
    }
}

fn control_read_loop(
    mut connection: Connection,
    receiver: Receiver<ControlReadMessage>,
    backend: Arc<dyn CatalogReadBackend>,
) -> Result<()> {
    while let Ok(message) = receiver.recv() {
        match message {
            ControlReadMessage::Revision(response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.revision(&connection));
                }
            }
            ControlReadMessage::GetNamespace(namespace, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.get_namespace(&connection, namespace));
                }
            }
            ControlReadMessage::ListNamespaces(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.list_namespaces(&mut connection, request));
                }
            }
            ControlReadMessage::GetTable(table, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.get_table(&connection, table));
                }
            }
            ControlReadMessage::ListTables(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.list_tables(&mut connection, request));
                }
            }
            ControlReadMessage::ResolveTables(tables, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.resolve_tables(&mut connection, &tables));
                }
            }
            ControlReadMessage::InspectFiles(probes, response) => {
                if !response.is_closed() {
                    let _ = response
                        .send(backend.inspect_data_file_references(&mut connection, &probes));
                }
            }
        }
    }
    connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close catalog control read", error))
}

fn snapshot_read_loop(
    mut connection: Connection,
    receiver: Receiver<SnapshotReadMessage>,
    backend: Arc<dyn CatalogReadBackend>,
) -> Result<()> {
    while let Ok(message) = receiver.recv() {
        match message {
            SnapshotReadMessage::Read(selectors, response) => {
                if !response.is_closed() {
                    let _ = response.send(backend.read_snapshot(&mut connection, &selectors));
                }
            }
        }
    }
    connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close catalog snapshot read", error))
}

fn join_read_worker(actor: JoinHandle<Result<()>>, role: &str) -> Result<()> {
    actor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            format!("sqlite catalog {role} worker thread panicked"),
        )
    })?
}

enum ServiceMessage {
    Command(CommandRequest),
    Shutdown(Sender<Result<()>>),
}

struct CommandRequest {
    command: VersionedCatalogCommand,
    state: CommandRequestState,
}

struct CommandRequestState {
    request_id: RequestId,
    response: oneshot::Sender<Result<VersionedCatalogResponse>>,
    _command_permit: CommandPermit,
    _manifest_permit: Option<ManifestQueuePermit>,
}

impl CommandRequestState {
    fn send(self, result: Result<VersionedCatalogResponse>) {
        let _ = self.response.send(result);
    }
}

trait ManifestPreparationBackend: Send + Sync + 'static {
    fn verify(&self, command: VersionedCatalogCommand) -> Result<domain::VerifiedManifestCommand>;

    fn prepare(
        &self,
        preparation: domain::ManifestCommandPreparation,
    ) -> domain::ManifestPreparationResult;
}

struct DomainManifestPreparationBackend;

impl ManifestPreparationBackend for DomainManifestPreparationBackend {
    fn verify(&self, command: VersionedCatalogCommand) -> Result<domain::VerifiedManifestCommand> {
        domain::verify_manifest_command(command)
    }

    fn prepare(
        &self,
        preparation: domain::ManifestCommandPreparation,
    ) -> domain::ManifestPreparationResult {
        domain::prepare_manifest_command(preparation)
    }
}

enum ManifestWorkerJob {
    Verify(VersionedCatalogCommand),
    Prepare(domain::ManifestCommandPreparation),
}

impl ManifestWorkerJob {
    fn request_id(&self) -> RequestId {
        match self {
            Self::Verify(command) => command_request_id(command),
            Self::Prepare(preparation) => preparation.request_id(),
        }
    }
}

enum ManifestWorkerCompletion {
    Verified(Result<domain::VerifiedManifestCommand>),
    Prepared(domain::ManifestPreparationResult),
}

enum ManifestWorkerSubmitError {
    Full(ManifestWorkerJob),
    Stopped(ManifestWorkerJob),
}

struct ManifestPreparationWorker {
    sender: Option<Sender<ManifestWorkerJob>>,
    actor: Option<JoinHandle<()>>,
}

impl ManifestPreparationWorker {
    fn start(
        backend: Arc<dyn ManifestPreparationBackend>,
    ) -> Result<(Self, Receiver<ManifestWorkerCompletion>)> {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let (completion_sender, completion_receiver) = crossbeam_channel::bounded(1);
        let actor = thread::Builder::new()
            .name(MANIFEST_PREPARATION_THREAD_NAME.to_owned())
            .spawn(move || {
                manifest_worker_loop(receiver, completion_sender, backend);
            })
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start sqlite catalog manifest preparation worker error=[{error}]"),
                )
            })?;
        Ok((
            Self {
                sender: Some(sender),
                actor: Some(actor),
            },
            completion_receiver,
        ))
    }

    fn submit(&self, job: ManifestWorkerJob) -> std::result::Result<(), ManifestWorkerSubmitError> {
        let Some(sender) = &self.sender else {
            return Err(ManifestWorkerSubmitError::Stopped(job));
        };
        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(job)) => Err(ManifestWorkerSubmitError::Full(job)),
            Err(TrySendError::Disconnected(job)) => Err(ManifestWorkerSubmitError::Stopped(job)),
        }
    }

    fn shutdown(&mut self) -> Result<()> {
        self.sender.take();
        match self.actor.take() {
            Some(actor) => actor.join().map_err(|_| {
                ServerError::new(
                    ErrorCode::Internal,
                    "sqlite catalog manifest preparation worker panicked",
                )
            }),
            None => Ok(()),
        }
    }
}

fn manifest_worker_loop(
    receiver: Receiver<ManifestWorkerJob>,
    completion_sender: Sender<ManifestWorkerCompletion>,
    backend: Arc<dyn ManifestPreparationBackend>,
) {
    while let Ok(job) = receiver.recv() {
        let request_id = job.request_id();
        let completion = match job {
            ManifestWorkerJob::Verify(command) => {
                let result = catch_unwind(AssertUnwindSafe(|| backend.verify(command)))
                    .unwrap_or_else(|_| Err(manifest_worker_panicked(request_id, "verify")));
                ManifestWorkerCompletion::Verified(result)
            }
            ManifestWorkerJob::Prepare(preparation) => {
                let panic_result = domain::ManifestPreparationResult::failed(
                    &preparation,
                    manifest_worker_panicked(request_id, "prepare"),
                );
                let result = catch_unwind(AssertUnwindSafe(|| backend.prepare(preparation)))
                    .unwrap_or(panic_result);
                ManifestWorkerCompletion::Prepared(result)
            }
        };
        if completion_sender.send(completion).is_err() {
            break;
        }
    }
}

fn manifest_worker_panicked(request_id: RequestId, stage: &str) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("sqlite catalog manifest preparation worker panicked stage=[{stage}]"),
    )
    .with_request_id(request_id)
}

fn command_request_id(command: &VersionedCatalogCommand) -> RequestId {
    let VersionedCatalogCommand::V1(envelope) = command;
    envelope.request_id
}

fn run_actor(
    config: SqliteCatalogConfig,
    receiver: Receiver<ServiceMessage>,
    startup_sender: Sender<Result<()>>,
    manifest_backend: Arc<dyn ManifestPreparationBackend>,
) {
    let (connection, _) = match crate::open_on_actor(&config) {
        Ok(opened) => opened,
        Err(error) => {
            let _ = startup_sender.send(Err(error));
            return;
        }
    };
    let (mut manifest_worker, manifest_completions) =
        match ManifestPreparationWorker::start(manifest_backend) {
            Ok(started) => started,
            Err(error) => {
                let _ = connection.close();
                let _ = startup_sender.send(Err(error));
                return;
            }
        };
    if startup_sender.send(Ok(())).is_err() {
        let _ = manifest_worker.shutdown();
        let _ = connection.close();
        return;
    }
    actor_loop(
        connection,
        receiver,
        &mut manifest_worker,
        manifest_completions,
    );
}

fn actor_loop(
    mut connection: Connection,
    receiver: Receiver<ServiceMessage>,
    manifest_worker: &mut ManifestPreparationWorker,
    manifest_completions: Receiver<ManifestWorkerCompletion>,
) {
    let mut active_manifest = None;
    let mut pending_commands = VecDeque::new();
    let mut draining = false;
    let mut shutdown_response = None;

    loop {
        if active_manifest.is_none() {
            if let Some(command) = pending_commands.pop_front() {
                start_command(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    command,
                );
                continue;
            }
            if draining {
                break;
            }
        }

        if active_manifest.is_some() {
            if let Ok(completion) = manifest_completions.try_recv() {
                handle_manifest_completion(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    completion,
                );
                continue;
            }
        }

        if draining {
            match manifest_completions.recv() {
                Ok(completion) => handle_manifest_completion(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    completion,
                ),
                Err(_) => fail_active_manifest_worker(
                    &mut active_manifest,
                    "sqlite catalog manifest completion channel stopped during shutdown",
                ),
            }
            continue;
        }

        if active_manifest.is_some() {
            crossbeam_channel::select! {
                recv(manifest_completions) -> completion => {
                    match completion {
                        Ok(completion) => handle_manifest_completion(
                            &mut connection,
                            manifest_worker,
                            &mut active_manifest,
                            completion,
                        ),
                        Err(_) => fail_active_manifest_worker(
                            &mut active_manifest,
                            "sqlite catalog manifest completion channel stopped",
                        ),
                    }
                }
                recv(receiver) -> message => {
                    match message {
                        Ok(message) => handle_service_message(
                            &mut connection,
                            manifest_worker,
                            &mut active_manifest,
                            &mut pending_commands,
                            &mut draining,
                            &mut shutdown_response,
                            message,
                        ),
                        Err(_) => draining = true,
                    }
                }
            }
        } else {
            match receiver.recv() {
                Ok(message) => handle_service_message(
                    &mut connection,
                    manifest_worker,
                    &mut active_manifest,
                    &mut pending_commands,
                    &mut draining,
                    &mut shutdown_response,
                    message,
                ),
                Err(_) => draining = true,
            }
        }
    }

    let worker_result = manifest_worker.shutdown();
    let close_result = connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close sqlite catalog database", error));
    let shutdown_result = worker_result.and(close_result);
    if let Some(response) = shutdown_response {
        let _ = response.send(shutdown_result);
    }
}

fn handle_service_message(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    pending_commands: &mut VecDeque<CommandRequest>,
    draining: &mut bool,
    shutdown_response: &mut Option<Sender<Result<()>>>,
    message: ServiceMessage,
) {
    match message {
        ServiceMessage::Command(command) => {
            if active_manifest.is_some() || !pending_commands.is_empty() {
                pending_commands.push_back(command);
            } else {
                start_command(connection, manifest_worker, active_manifest, command);
            }
        }
        ServiceMessage::Shutdown(response) => {
            *draining = true;
            *shutdown_response = Some(response);
        }
    }
}

fn start_command(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    request: CommandRequest,
) {
    let CommandRequest { command, state } = request;
    if domain::is_manifest_command(&command) {
        submit_manifest_job(
            manifest_worker,
            active_manifest,
            ManifestWorkerJob::Verify(command),
            state,
        );
    } else {
        state.send(domain::execute_command(connection, command));
    }
}

fn handle_manifest_completion(
    connection: &mut Connection,
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    completion: ManifestWorkerCompletion,
) {
    let Some(state) = active_manifest.take() else {
        tracing::error!("sqlite catalog received manifest completion without active command");
        return;
    };
    match completion {
        ManifestWorkerCompletion::Verified(Err(error)) => state.send(Err(error)),
        ManifestWorkerCompletion::Verified(Ok(verified)) => {
            match domain::begin_verified_manifest_command(connection, verified) {
                Ok(domain::ManifestCommandStart::Replay(response)) => state.send(Ok(response)),
                Ok(domain::ManifestCommandStart::Prepare(preparation)) => submit_manifest_job(
                    manifest_worker,
                    active_manifest,
                    ManifestWorkerJob::Prepare(preparation),
                    state,
                ),
                Err(error) => state.send(Err(error)),
            }
        }
        ManifestWorkerCompletion::Prepared(result) => {
            state.send(domain::finalize_manifest_preparation(connection, result));
        }
    }
}

fn submit_manifest_job(
    manifest_worker: &ManifestPreparationWorker,
    active_manifest: &mut Option<CommandRequestState>,
    job: ManifestWorkerJob,
    state: CommandRequestState,
) {
    match manifest_worker.submit(job) {
        Ok(()) => *active_manifest = Some(state),
        Err(ManifestWorkerSubmitError::Full(job)) => {
            let request_id = job.request_id();
            state.send(Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                "sqlite catalog manifest preparation queue is full",
            )
            .with_request_id(request_id)));
        }
        Err(ManifestWorkerSubmitError::Stopped(job)) => {
            let request_id = job.request_id();
            state.send(Err(ServerError::new(
                ErrorCode::Unavailable,
                "sqlite catalog manifest preparation worker is not running",
            )
            .with_request_id(request_id)));
        }
    }
}

fn fail_active_manifest_worker(
    active_manifest: &mut Option<CommandRequestState>,
    message: &'static str,
) {
    if let Some(state) = active_manifest.take() {
        let request_id = state.request_id;
        state.send(Err(
            ServerError::new(ErrorCode::Unavailable, message).with_request_id(request_id)
        ));
    }
}

async fn await_response<T>(response: Result<oneshot::Receiver<Result<T>>>) -> Result<T> {
    response?.await.map_err(|_| actor_stopped())?
}

fn actor_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "sqlite catalog actor is not running",
    )
}

fn read_pool_stopped() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "sqlite catalog read pool is not running",
    )
}

fn join_startup_actor(actor: JoinHandle<()>) -> Result<()> {
    actor.join().map_err(|_| {
        ServerError::new(
            ErrorCode::Internal,
            "sqlite catalog actor thread panicked during startup",
        )
    })
}

impl Drop for SqliteCatalog {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            tracing::error!(error = %error, "sqlite catalog shutdown failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use tempfile::TempDir;
    use zippy_core::OwnedDataDirectory;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
        CatalogSnapshotSelector, CatalogWrite, CommitDataFilesCommand, CreateNamespaceCommand,
        CreateTableCommand, VersionedCatalogCommand, VersionedCatalogResponse,
    };
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, DataFileLocation, DataFileProvenance, ManagedDataPath,
        MetadataMap, MetadataScalar, NativeDataFileProvenance, PendingDataFile, SchemaIpcPayload,
        SourceSegmentIdentity, SourceSegmentLineage, SourceSegmentLineages, TableDefinition,
    };
    use zippy_types::{
        ErrorCode, FileId, LogOffset, NamespaceId, OffsetRange, OffsetWatermark, RequestId, TableId,
    };

    use super::{
        domain, snapshot_domain, CatalogReadBackend, DomainManifestPreparationBackend,
        ManifestPreparationBackend, ManifestQueueBudget, SqliteCatalog, SqliteCatalogConfig,
    };

    const NAMESPACE_ID: u128 = 1;
    const TABLE_ID: u128 = 2;

    struct BlockingManifestBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    }

    struct PanickingManifestBackend {
        panic_prepare_once: AtomicBool,
    }

    struct BlockingSnapshotBackend {
        started: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
        block_once: AtomicBool,
        calls: Arc<AtomicUsize>,
    }

    impl ManifestPreparationBackend for BlockingManifestBackend {
        fn verify(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::Result<domain::VerifiedManifestCommand> {
            domain::verify_manifest_command(command)
        }

        fn prepare(
            &self,
            preparation: domain::ManifestCommandPreparation,
        ) -> domain::ManifestPreparationResult {
            self.started
                .send(())
                .expect("test preparation start observer must remain connected");
            self.release
                .recv()
                .expect("test preparation release must be signalled");
            domain::prepare_manifest_command(preparation)
        }
    }

    impl ManifestPreparationBackend for PanickingManifestBackend {
        fn verify(
            &self,
            command: VersionedCatalogCommand,
        ) -> zippy_types::Result<domain::VerifiedManifestCommand> {
            domain::verify_manifest_command(command)
        }

        fn prepare(
            &self,
            preparation: domain::ManifestCommandPreparation,
        ) -> domain::ManifestPreparationResult {
            if self.panic_prepare_once.swap(false, Ordering::AcqRel) {
                panic!("injected manifest preparation panic");
            }
            domain::prepare_manifest_command(preparation)
        }
    }

    impl CatalogReadBackend for BlockingSnapshotBackend {
        fn read_snapshot(
            &self,
            connection: &mut rusqlite::Connection,
            selectors: &[CatalogSnapshotSelector],
        ) -> zippy_types::Result<zippy_types::catalog::CatalogQuerySnapshot> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            if self.block_once.swap(false, Ordering::AcqRel) {
                self.started
                    .send(())
                    .expect("test snapshot start observer must remain connected");
                self.release
                    .recv()
                    .expect("test snapshot release must be signalled");
            }
            snapshot_domain::read_snapshot_at(connection, selectors)
        }
    }

    fn data_directory(directory: &TempDir) -> Arc<OwnedDataDirectory> {
        Arc::new(
            OwnedDataDirectory::acquire_or_initialize(directory.path().join("data"))
                .expect("own data root"),
        )
    }

    fn request_id(value: u128) -> RequestId {
        RequestId::from_u128(value).expect("nonzero request id")
    }

    fn table_id() -> TableId {
        TableId::from_u128(TABLE_ID).expect("nonzero table id")
    }

    fn command(
        request: u128,
        expected_revision: Option<u64>,
        command: CatalogCommandV1,
    ) -> VersionedCatalogCommand {
        CatalogCommandEnvelopeV1::new(
            request_id(request),
            expected_revision,
            CatalogActor {
                kind: CatalogActorKind::Server,
                principal: "catalog-service-test".to_owned(),
            },
            command,
        )
        .expect("valid command envelope")
        .into()
    }

    fn schema_ipc() -> SchemaIpcPayload {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).expect("create IPC writer");
        writer.finish().expect("finish schema-only IPC stream");
        drop(writer);
        SchemaIpcPayload::new(payload).expect("nonempty schema IPC payload")
    }

    async fn create_append_table(catalog: &SqliteCatalog) {
        catalog
            .command(command(
                1,
                Some(0),
                CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("market").expect("valid namespace name"),
                }),
            ))
            .await
            .expect("create namespace");
        catalog
            .command(command(
                2,
                Some(1),
                CatalogCommandV1::CreateTable(CreateTableCommand {
                    table_id: table_id(),
                    namespace_id: NamespaceId::from_u128(NAMESPACE_ID)
                        .expect("nonzero namespace id"),
                    name: CatalogName::new("ticks").expect("valid table name"),
                    definition: TableDefinition::Append(AppendTableDefinition {
                        event_time_column: None,
                    }),
                    schema_ipc: schema_ipc(),
                    config: BTreeMap::new(),
                }),
            ))
            .await
            .expect("create append table");
    }

    fn pending_file(file: u128, offset: u64) -> PendingDataFile {
        let offsets = OffsetRange::new(LogOffset::new(offset), LogOffset::new(offset))
            .expect("valid one-row offset range");
        let lineage = SourceSegmentLineages::new(vec![SourceSegmentLineage {
            source: SourceSegmentIdentity {
                segment_id: offset + 1,
                generation: 1,
                writer_epoch: 0,
            },
            offsets,
        }])
        .expect("valid source lineage");
        PendingDataFile {
            file_id: FileId::from_u128(file).expect("nonzero file id"),
            table_id: table_id(),
            schema_version: 1,
            location: DataFileLocation::Managed(
                ManagedDataPath::new(format!("tables/ticks/{file}.parquet"))
                    .expect("valid managed path"),
            ),
            file_size_bytes: 128,
            row_count: NonZeroU64::new(1).expect("nonzero row count"),
            provenance: DataFileProvenance::Native(
                NativeDataFileProvenance::new(offsets, lineage).expect("valid native provenance"),
            ),
            partition: MetadataMap::new(),
            statistics: MetadataMap::new(),
            metadata: MetadataMap::new(),
            checksum: format!("sha256-v1:{file:064x}"),
            created_at_ns: 1,
        }
    }

    fn commit_data_file(
        request: u128,
        file: u128,
        expected_revision: u64,
        expected_snapshot_version: u64,
        offset: u64,
    ) -> VersionedCatalogCommand {
        command(
            request,
            Some(expected_revision),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(),
                expected_snapshot_version,
                files: vec![pending_file(file, offset)],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(offset)),
            }),
        )
    }

    fn commit_data_file_with_metadata(request: u128, entries: usize) -> VersionedCatalogCommand {
        let mut file = pending_file(10, 0);
        for index in 0..entries {
            file.metadata.insert(
                format!("field_{index:04}"),
                MetadataScalar::String(format!("value_{index:04}")),
            );
        }
        command(
            request,
            Some(2),
            CatalogCommandV1::CommitDataFiles(CommitDataFilesCommand {
                table_id: table_id(),
                expected_snapshot_version: 1,
                files: vec![file],
                committed_log_offset: OffsetWatermark::Known(LogOffset::new(0)),
            }),
        )
    }

    fn blocking_backend() -> (
        Arc<dyn ManifestPreparationBackend>,
        crossbeam_channel::Receiver<()>,
        crossbeam_channel::Sender<()>,
    ) {
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        (
            Arc::new(BlockingManifestBackend {
                started: started_sender,
                release: release_receiver,
            }),
            started_receiver,
            release_sender,
        )
    }

    fn blocking_snapshot_backend() -> (
        Arc<dyn CatalogReadBackend>,
        crossbeam_channel::Receiver<()>,
        crossbeam_channel::Sender<()>,
        Arc<AtomicUsize>,
    ) {
        let (started_sender, started_receiver) = crossbeam_channel::bounded(1);
        let (release_sender, release_receiver) = crossbeam_channel::bounded(1);
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(BlockingSnapshotBackend {
                started: started_sender,
                release: release_receiver,
                block_once: AtomicBool::new(true),
                calls: Arc::clone(&calls),
            }),
            started_receiver,
            release_sender,
            calls,
        )
    }

    #[test]
    fn manifest_queue_budget_releases_completed_reservations() {
        let budget = Arc::new(ManifestQueueBudget::new(8));
        assert!(budget.reserve(0).expect("zero-byte reservation").is_none());

        let permit = budget
            .reserve(8)
            .expect("reserve complete byte budget")
            .expect("nonzero reservation returns a permit");
        let error = budget
            .reserve(1)
            .err()
            .expect("occupied byte budget must reject another reservation");
        assert_eq!(error.code(), ErrorCode::ResourceExhausted);
        assert!(error.retryable());

        let oversized = budget
            .reserve(9)
            .err()
            .expect("single oversized command must be rejected permanently");
        assert_eq!(oversized.code(), ErrorCode::ResourceExhausted);
        assert!(!oversized.retryable());

        drop(permit);
        assert!(budget
            .reserve(8)
            .expect("released byte budget can be reserved again")
            .is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cooperative_manifest_preflight_yields_and_releases_cancelled_command_budget() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog =
            SqliteCatalog::open(SqliteCatalogConfig::new(&data_directory).with_command_capacity(1))
                .expect("open single-command catalog");
        create_append_table(&catalog).await;

        let command = commit_data_file_with_metadata(3, 512);
        let mut scanning = catalog.command(command.clone());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            scanning.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(catalog.revision().await, Ok(2));

        let occupied = catalog
            .command(command.clone())
            .await
            .expect_err("preflight must hold the global command permit until cancellation");
        assert_eq!(occupied.code(), ErrorCode::ResourceExhausted);
        assert!(occupied.retryable());
        drop(scanning);

        let VersionedCatalogResponse::V1(response) = catalog
            .command(command)
            .await
            .expect("cancelled preflight must release its command permit");
        assert_eq!(response.catalog_revision, 3);
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocked_snapshot_does_not_block_control_reads_or_writer_commit() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (read_backend, started, release, _) = blocking_snapshot_backend();
        let catalog = SqliteCatalog::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            read_backend,
        )
        .expect("open catalog with blocking snapshot backend");
        create_append_table(&catalog).await;

        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let mut snapshot = catalog.read_snapshot_at(&selectors);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            snapshot.as_mut().poll(&mut context),
            Poll::Pending
        ));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("snapshot must reach the dedicated read worker");

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), catalog.revision())
                .await
                .expect("control read must not wait for snapshot"),
            Ok(2)
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            catalog.command(commit_data_file(3, 10, 2, 1, 0)),
        )
        .await
        .expect("writer commit must not wait for snapshot")
        .expect("commit one manifest while snapshot is blocked");
        assert_eq!(catalog.revision().await, Ok(3));

        release.send(()).expect("release snapshot read");
        let snapshot = snapshot.await.expect("complete exact snapshot read");
        assert_eq!(snapshot.catalog_revision(), 3);
        assert!(snapshot.tables()[0].files().as_slice().is_empty());
        catalog
            .close()
            .expect("close catalog read lanes and writer");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_queued_snapshot_is_skipped_and_releases_queue_capacity() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory).with_snapshot_read_capacity(1);
        let (read_backend, started, release, calls) = blocking_snapshot_backend();
        let catalog = SqliteCatalog::open_with_backends(
            config,
            Arc::new(DomainManifestPreparationBackend),
            read_backend,
        )
        .expect("open catalog with one queued snapshot slot");
        create_append_table(&catalog).await;

        let selectors = [CatalogSnapshotSelector {
            table_id: table_id(),
            snapshot_version: 1,
            schema_version: 1,
        }];
        let mut active = catalog.read_snapshot_at(&selectors);
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(active.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("first snapshot must occupy its worker");

        let mut cancelled = catalog.read_snapshot_at(&selectors);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        let full = catalog
            .read_snapshot_at(&selectors)
            .await
            .expect_err("active and queued snapshots must saturate the lane");
        assert_eq!(full.code(), ErrorCode::ResourceExhausted);
        assert_eq!(full.message(), "sqlite catalog snapshot read queue is full");
        drop(cancelled);

        release.send(()).expect("release active snapshot");
        active.await.expect("finish active snapshot");
        let final_snapshot = loop {
            match catalog.read_snapshot_at(&selectors).await {
                Err(error) if error.code() == ErrorCode::ResourceExhausted => {
                    tokio::task::yield_now().await;
                }
                result => break result,
            }
        };
        final_snapshot.expect("snapshot lane must recover after skipping cancellation");
        assert_eq!(calls.load(Ordering::Acquire), 2);
        catalog.close().expect("close catalog after cancellation");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_manifest_preparation_keeps_reads_live_and_shutdown_drains_cancelled_commit() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let catalog = Arc::new(
            SqliteCatalog::open_with_manifest_backend(config.clone(), backend)
                .expect("open catalog with blocking manifest backend"),
        );
        create_append_table(&catalog).await;

        let task_catalog = Arc::clone(&catalog);
        let mut commit =
            Box::pin(async move { task_catalog.command(commit_data_file(3, 10, 2, 1, 0)).await });
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("manifest must reach semantic preparation");

        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), catalog.revision())
                .await
                .expect("revision read must not wait for manifest preparation"),
            Ok(2)
        );
        drop(commit);

        let catalog = match Arc::try_unwrap(catalog) {
            Ok(catalog) => catalog,
            Err(_) => panic!("cancelled manifest future must release its catalog Arc"),
        };
        let (closed_sender, closed_receiver) = crossbeam_channel::bounded(1);
        let close_thread = std::thread::spawn(move || {
            let _ = closed_sender.send(catalog.close());
        });
        assert!(matches!(
            closed_receiver.recv_timeout(Duration::from_millis(100)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)
        ));

        release.send(()).expect("release manifest preparation");
        closed_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("catalog close must finish after preparation release")
            .expect("catalog close must drain the accepted manifest");
        close_thread.join().expect("join catalog close thread");

        let reopened = SqliteCatalog::open(config).expect("reopen catalog after drained close");
        assert_eq!(reopened.revision().await, Ok(3));
        let snapshot = reopened
            .read_snapshot_at(&[CatalogSnapshotSelector {
                table_id: table_id(),
                snapshot_version: 2,
                schema_version: 1,
            }])
            .await
            .expect("read committed manifest after restart");
        assert_eq!(snapshot.tables()[0].files().as_slice().len(), 1);
        reopened.close().expect("close reopened catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_manifest_preserves_pending_mutation_fifo() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let catalog = SqliteCatalog::open_with_manifest_backend(config, backend)
            .expect("open catalog with blocking manifest backend");
        create_append_table(&catalog).await;

        let mut first = Box::pin(catalog.command(commit_data_file(3, 10, 2, 1, 0)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("first manifest must reach semantic preparation");

        let mut second = Box::pin(catalog.command(commit_data_file(4, 11, 3, 2, 1)));
        assert!(matches!(second.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(catalog.revision().await, Ok(2));

        release
            .send(())
            .expect("release first manifest preparation");
        let VersionedCatalogResponse::V1(first_response) = first
            .await
            .expect("first manifest must commit before the pending manifest");
        assert_eq!(first_response.catalog_revision, 3);
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("second manifest must start after the first terminal result");
        assert_eq!(catalog.revision().await, Ok(3));

        release
            .send(())
            .expect("release second manifest preparation");
        let VersionedCatalogResponse::V1(second_response) = second
            .await
            .expect("second manifest must commit from the updated parent state");
        assert_eq!(second_response.catalog_revision, 4);
        assert_eq!(catalog.revision().await, Ok(4));
        catalog.close().expect("close catalog");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn preparation_panic_is_terminal_and_the_same_request_can_retry() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let catalog = SqliteCatalog::open_with_manifest_backend(
            SqliteCatalogConfig::new(&data_directory),
            Arc::new(PanickingManifestBackend {
                panic_prepare_once: AtomicBool::new(true),
            }),
        )
        .expect("open catalog with panicking manifest backend");
        create_append_table(&catalog).await;

        let command = commit_data_file(3, 10, 2, 1, 0);
        let error = catalog
            .command(command.clone())
            .await
            .expect_err("injected preparation panic must be terminal");
        assert_eq!(error.code(), ErrorCode::Internal);
        assert_eq!(error.request_id(), Some(request_id(3)));
        assert_eq!(catalog.revision().await, Ok(2));

        let VersionedCatalogResponse::V1(response) = catalog
            .command(command)
            .await
            .expect("worker must remain live and the released request can retry");
        assert_eq!(response.catalog_revision, 3);
        catalog.close().expect("close catalog after panic recovery");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_idempotency_commit_overrides_a_preparation_error() {
        let directory = TempDir::new().expect("create temp directory");
        let data_directory = data_directory(&directory);
        let config = SqliteCatalogConfig::new(&data_directory);
        let (backend, started, release) = blocking_backend();
        let first = Arc::new(
            SqliteCatalog::open_with_manifest_backend(config.clone(), backend)
                .expect("open first catalog actor"),
        );
        create_append_table(&first).await;
        let second = SqliteCatalog::open_with_manifest_backend(
            config,
            Arc::new(DomainManifestPreparationBackend),
        )
        .expect("open concurrent catalog actor");

        let first_catalog = Arc::clone(&first);
        let first_command = tokio::spawn(async move {
            first_catalog
                .command(commit_data_file(3, 11, 2, 1, 1))
                .await
        });
        started
            .recv_timeout(Duration::from_secs(2))
            .expect("invalid command must pass the initial idempotency probe");

        second
            .command(commit_data_file(3, 10, 2, 1, 0))
            .await
            .expect("concurrent actor commits the request id first");
        release.send(()).expect("release invalid preparation");
        let error = first_command
            .await
            .expect("join first catalog command")
            .expect_err("different fingerprint must conflict");
        assert_eq!(error.code(), ErrorCode::Conflict);
        assert_eq!(error.request_id(), Some(request_id(3)));
        assert_eq!(first.revision().await, Ok(3));

        let first = match Arc::try_unwrap(first) {
            Ok(catalog) => catalog,
            Err(_) => panic!("manifest task must release the first catalog Arc"),
        };
        first.close().expect("close first catalog actor");
        second.close().expect("close second catalog actor");
    }
}
