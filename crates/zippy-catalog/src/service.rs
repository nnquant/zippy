//! Asynchronous Catalog traits backed by one connection-owning SQLite actor.

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread::{self, JoinHandle};

use rusqlite::{Connection, TransactionBehavior};
use tokio::sync::oneshot;
use zippy_types::catalog::{
    CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector, CatalogWrite,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, ErrorCode, Result, ServerError, ServerErrorDetails};

use crate::domain;
use crate::SqliteCatalogConfig;

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

    fn get_table(&self, _table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async { Err(unsupported("catalog.read.get_table")) })
    }

    fn list_tables(
        &self,
        _request: ListTablesRequest,
    ) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async { Err(unsupported("catalog.read.list_tables")) })
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            if !tables.is_empty() {
                return Err(unsupported("catalog.read.resolve_tables"));
            }
            await_response(self.request(ServiceMessage::ResolveEmptyTables)).await
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            if !selectors.is_empty() {
                return Err(unsupported("catalog.read.read_snapshot_at"));
            }
            await_response(self.request(ServiceMessage::ReadEmptySnapshot)).await
        })
    }
}

impl CatalogWrite for SqliteCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            await_response(self.request(|sender| ServiceMessage::Command(command, sender))).await
        })
    }
}

enum ServiceMessage {
    Revision(oneshot::Sender<Result<u64>>),
    GetNamespace(NamespaceRef, oneshot::Sender<Result<NamespaceDescriptor>>),
    ListNamespaces(
        ListNamespacesRequest,
        oneshot::Sender<Result<ListNamespacesResponse>>,
    ),
    ResolveEmptyTables(oneshot::Sender<Result<Vec<TableDescriptor>>>),
    ReadEmptySnapshot(oneshot::Sender<Result<CatalogQuerySnapshot>>),
    Command(
        VersionedCatalogCommand,
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
            ServiceMessage::ResolveEmptyTables(response) => {
                if !response.is_closed() {
                    let _ = response.send(resolve_empty_tables(&mut connection));
                }
            }
            ServiceMessage::ReadEmptySnapshot(response) => {
                if !response.is_closed() {
                    let _ = response.send(read_empty_snapshot(&mut connection));
                }
            }
            ServiceMessage::Command(command, response) => {
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

fn read_empty_snapshot(connection: &mut Connection) -> Result<CatalogQuerySnapshot> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin empty catalog snapshot read", error))?;
    let revision = domain::revision(&transaction)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit empty catalog snapshot read", error))?;
    Ok(CatalogQuerySnapshot::empty(revision))
}

fn resolve_empty_tables(connection: &mut Connection) -> Result<Vec<TableDescriptor>> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| crate::map_sqlite_error("begin empty catalog table resolution", error))?;
    let _ = domain::revision(&transaction)?;
    transaction
        .commit()
        .map_err(|error| crate::map_sqlite_error("commit empty catalog table resolution", error))?;
    Ok(Vec::new())
}

async fn await_response<T>(response: Result<oneshot::Receiver<Result<T>>>) -> Result<T> {
    response?.await.map_err(|_| actor_stopped())?
}

fn unsupported(operator: &str) -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        format!("sqlite catalog operation is not implemented operator=[{operator}]"),
    )
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operator.to_owned(),
    })
    .expect("unsupported-plan details match the unsupported-plan error code")
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
