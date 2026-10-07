//! Test-only fault injection around a real SQLite persistence boundary.

use std::sync::Arc;

use zippy_catalog::SqliteCatalog;
use zippy_types::catalog::{
    CatalogCommandReplayProbe, CatalogDataFileReferenceSnapshot, CatalogFileRead,
    CatalogIdempotencyRead, CatalogPersistence, CatalogPublicationControl,
    CatalogPublicationExclusion, CatalogPublicationPermit, CatalogQuerySnapshot, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, ListNamespacesRequest, ListNamespacesResponse,
    ListTablesRequest, ListTablesResponse, ManagedDataFileProbe, NamespaceRef,
    VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{NamespaceDescriptor, TableDescriptor, TableRef};
use zippy_types::{BoxFuture, Result};

struct FaultInjectedCatalog {
    writer: Arc<dyn CatalogWrite>,
    sqlite: Arc<SqliteCatalog>,
}

/// Reuse the one active SQLite owner when a test already has one.
pub fn with_existing_sqlite(
    writer: Arc<dyn CatalogWrite>,
    sqlite: Arc<SqliteCatalog>,
) -> Arc<dyn CatalogPersistence> {
    Arc::new(FaultInjectedCatalog { writer, sqlite })
}

impl CatalogRead for FaultInjectedCatalog {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        self.sqlite.revision()
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        self.sqlite.get_namespace(namespace)
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        self.sqlite.list_namespaces(request)
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        self.sqlite.get_table(table)
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        self.sqlite.list_tables(request)
    }

    fn resolve_tables<'a>(
        &'a self,
        tables: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        self.sqlite.resolve_tables(tables)
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        self.sqlite.read_snapshot_at(selectors)
    }
}

impl CatalogFileRead for FaultInjectedCatalog {
    fn inspect_data_file_references<'a>(
        &'a self,
        probes: &'a [ManagedDataFileProbe],
    ) -> BoxFuture<'a, Result<CatalogDataFileReferenceSnapshot>> {
        self.sqlite.inspect_data_file_references(probes)
    }
}

impl CatalogIdempotencyRead for FaultInjectedCatalog {
    fn probe_command_replay<'a>(
        &'a self,
        command: &'a VersionedCatalogCommand,
    ) -> BoxFuture<'a, Result<CatalogCommandReplayProbe>> {
        self.sqlite.probe_command_replay(command)
    }
}

impl CatalogWrite for FaultInjectedCatalog {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        self.writer.command(command)
    }
}

impl CatalogPublicationControl for FaultInjectedCatalog {
    fn acquire_publication_permit(&self) -> BoxFuture<'_, Result<CatalogPublicationPermit>> {
        self.sqlite.acquire_publication_permit()
    }

    fn command_with_publication_permit(
        &self,
        permit: CatalogPublicationPermit,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        Box::pin(async move {
            let _permit = permit;
            self.writer.command(command).await
        })
    }

    fn acquire_publication_exclusion(&self) -> BoxFuture<'_, Result<CatalogPublicationExclusion>> {
        self.sqlite.acquire_publication_exclusion()
    }
}
