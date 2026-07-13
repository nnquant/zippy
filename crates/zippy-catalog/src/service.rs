//! Asynchronous Catalog traits backed by one connection-owning SQLite actor.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use rusqlite::Connection;
use tokio::sync::oneshot;
use zippy_types::catalog::{
    CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector, CatalogWrite,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, ErrorCode, Result, ServerError};

use crate::SqliteCatalogConfig;
use crate::{domain, manifest_domain, snapshot_domain, table_domain};

const SERVICE_ACTOR_THREAD_NAME: &str = "zippy-catalog-service";

/// A typed SQLite Catalog whose only connection is owned by one OS actor thread.
///
/// Catalog trait futures submit through a bounded nonblocking queue and await a
/// Tokio oneshot response. SQLite work therefore never blocks an async executor
/// thread. Every trait future is cold: constructing and dropping it before its
/// first poll neither occupies the actor queue nor executes any operation. Once
/// a command future has been polled and successfully enqueued, cancelling it
/// abandons only the response; the actor still executes the command. Callers
/// must treat that case as commit-unknown and replay the same request ID.
/// Cancelled reads may be skipped before execution. `open`, `close`, and `Drop`
/// remain synchronous in this migration slice and must not be wired into the
/// server runtime until shutdown receives an absolute deadline.
pub struct SqliteCatalog {
    sender: Option<SyncSender<ServiceMessage>>,
    actor: Option<JoinHandle<()>>,
    manifest_queue_budget: Arc<ManifestQueueBudget>,
}

impl SqliteCatalog {
    /// Opens or migrates the Catalog and starts its connection-owning actor.
    ///
    /// Startup is synchronous and returns only after the existing foundation
    /// preflight, durable SQLite configuration, migrations, and integrity check
    /// have completed on the actor thread.
    pub fn open(config: SqliteCatalogConfig) -> Result<Self> {
        crate::validate_config(&config)?;
        let (sender, receiver) = mpsc::sync_channel(config.command_capacity);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let manifest_queue_budget = Arc::new(ManifestQueueBudget::new(
            config.manifest_queue_capacity_bytes,
        ));
        let actor = thread::Builder::new()
            .name(SERVICE_ACTOR_THREAD_NAME.to_owned())
            .spawn(move || run_actor(config, receiver, startup_sender))
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    format!("start sqlite catalog actor thread error=[{error}]"),
                )
            })?;
        match startup_receiver.recv() {
            Ok(Ok(())) => Ok(Self {
                sender: Some(sender),
                actor: Some(actor),
                manifest_queue_budget,
            }),
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

    fn request<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> ServiceMessage,
    ) -> Result<oneshot::Receiver<Result<T>>>
    where
        T: Send + 'static,
    {
        use std::sync::mpsc::TrySendError;

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

    fn shutdown(&mut self) -> Result<()> {
        let Some(sender) = self.sender.take() else {
            return Ok(());
        };
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
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
        Box::pin(async move { await_response(self.request(ServiceMessage::Revision)).await })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            await_response(self.request(|sender| ServiceMessage::GetNamespace(namespace, sender)))
                .await
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            await_response(self.request(|sender| ServiceMessage::ListNamespaces(request, sender)))
                .await
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            await_response(self.request(|sender| ServiceMessage::GetTable(table, sender))).await
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            await_response(self.request(|sender| ServiceMessage::ListTables(request, sender))).await
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            table_domain::validate_table_resolution_request(tables)?;
            await_response(
                self.request(|sender| ServiceMessage::ResolveTables(tables.to_vec(), sender)),
            )
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
            await_response(self.request(|sender| ServiceMessage::ReadSnapshot(selectors, sender)))
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
            let queued_bytes = manifest_domain::manifest_command_preflight_bytes(&command)?;
            let VersionedCatalogCommand::V1(envelope) = &command;
            let permit = self
                .manifest_queue_budget
                .reserve(queued_bytes)
                .map_err(|error| error.with_request_id(envelope.request_id))?;
            await_response(self.request(|sender| ServiceMessage::Command(command, permit, sender)))
                .await
        })
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
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "sqlite catalog manifest queue byte reservation overflow",
                )
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

enum ServiceMessage {
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
    ReadSnapshot(
        Vec<CatalogSnapshotSelector>,
        oneshot::Sender<Result<CatalogQuerySnapshot>>,
    ),
    Command(
        VersionedCatalogCommand,
        Option<ManifestQueuePermit>,
        oneshot::Sender<Result<VersionedCatalogResponse>>,
    ),
    Shutdown(SyncSender<Result<()>>),
}

fn run_actor(
    config: SqliteCatalogConfig,
    receiver: Receiver<ServiceMessage>,
    startup_sender: SyncSender<Result<()>>,
) {
    let (connection, _) = match crate::open_on_actor(&config) {
        Ok(opened) => opened,
        Err(error) => {
            let _ = startup_sender.send(Err(error));
            return;
        }
    };
    if startup_sender.send(Ok(())).is_err() {
        return;
    }
    actor_loop(connection, receiver);
}

fn actor_loop(mut connection: Connection, receiver: Receiver<ServiceMessage>) {
    let mut shutdown_response = None;
    while let Ok(message) = receiver.recv() {
        match message {
            ServiceMessage::Revision(response) => {
                if !response.is_closed() {
                    let _ = response.send(domain::revision(&connection));
                }
            }
            ServiceMessage::GetNamespace(namespace, response) => {
                if !response.is_closed() {
                    let _ = response.send(domain::get_namespace(&connection, namespace));
                }
            }
            ServiceMessage::ListNamespaces(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(domain::list_namespaces(&mut connection, request));
                }
            }
            ServiceMessage::GetTable(table, response) => {
                if !response.is_closed() {
                    let _ = response.send(table_domain::get_table(&connection, table));
                }
            }
            ServiceMessage::ListTables(request, response) => {
                if !response.is_closed() {
                    let _ = response.send(table_domain::list_tables(&mut connection, request));
                }
            }
            ServiceMessage::ResolveTables(tables, response) => {
                if !response.is_closed() {
                    let _ = response.send(table_domain::resolve_tables(&mut connection, &tables));
                }
            }
            ServiceMessage::ReadSnapshot(selectors, response) => {
                if !response.is_closed() {
                    let _ = response.send(snapshot_domain::read_snapshot_at(
                        &mut connection,
                        &selectors,
                    ));
                }
            }
            ServiceMessage::Command(command, _manifest_permit, response) => {
                let _ = response.send(domain::execute_command(&mut connection, command));
            }
            ServiceMessage::Shutdown(response) => {
                shutdown_response = Some(response);
                break;
            }
        }
    }
    let close_result = connection
        .close()
        .map_err(|(_, error)| crate::map_sqlite_error("close sqlite catalog database", error));
    if let Some(response) = shutdown_response {
        let _ = response.send(close_result);
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
    use std::sync::Arc;

    use zippy_types::ErrorCode;

    use super::ManifestQueueBudget;

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

        drop(permit);
        assert!(budget
            .reserve(8)
            .expect("released byte budget can be reserved again")
            .is_some());
    }
}
