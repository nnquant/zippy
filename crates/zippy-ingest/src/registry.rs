//! Atomic construction of server-owned table runtime identities.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use zippy_types::table::{SchemaDescriptor, TableDescriptor, TableSnapshot, TableState};
use zippy_types::{ErrorCode, OffsetWatermark, Result, ServerError, TableId};

/// One fully decoded current Catalog state used to create a table runtime.
///
/// Callers must obtain `schema` from the Catalog's bounded persisted-schema
/// decoder. The constructor verifies descriptor identity and current-version
/// relationships before any value can enter a registry.
#[derive(Debug, Clone)]
pub struct TableRuntimeBootstrap {
    table: TableDescriptor,
    schema_descriptor: SchemaDescriptor,
    schema: SchemaRef,
    snapshot: TableSnapshot,
}

impl TableRuntimeBootstrap {
    /// Creates one structurally consistent active-table bootstrap record.
    pub fn new(
        table: TableDescriptor,
        schema_descriptor: SchemaDescriptor,
        schema: SchemaRef,
        snapshot: TableSnapshot,
    ) -> Result<Self> {
        validate_bootstrap(&table, &schema_descriptor, &schema, &snapshot)?;
        Ok(Self {
            table,
            schema_descriptor,
            schema,
            snapshot,
        })
    }

    /// Returns the immutable table identity.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table.table_id
    }
}

/// The bootstrapped state owned by one future table actor.
///
/// This first implementation is immutable. It establishes the authoritative
/// identity, schema, persisted snapshot, writer epoch, and logical watermark
/// that the actor must start from; it does not yet admit writes.
#[derive(Debug)]
pub struct TableRuntime {
    table: TableDescriptor,
    schema_descriptor: SchemaDescriptor,
    schema: SchemaRef,
    snapshot: TableSnapshot,
}

impl TableRuntime {
    /// Creates runtime state from a validated Catalog bootstrap.
    pub fn from_bootstrap(bootstrap: TableRuntimeBootstrap) -> Self {
        Self {
            table: bootstrap.table,
            schema_descriptor: bootstrap.schema_descriptor,
            schema: bootstrap.schema,
            snapshot: bootstrap.snapshot,
        }
    }

    /// Returns the immutable table identifier.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table.table_id
    }

    /// Borrows the persistent table descriptor recovered at startup.
    #[must_use]
    pub const fn table(&self) -> &TableDescriptor {
        &self.table
    }

    /// Borrows the exact current schema descriptor recovered at startup.
    #[must_use]
    pub const fn schema_descriptor(&self) -> &SchemaDescriptor {
        &self.schema_descriptor
    }

    /// Borrows the decoded Arrow schema used by native writers.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Borrows the exact current persisted snapshot recovered at startup.
    #[must_use]
    pub const fn snapshot(&self) -> &TableSnapshot {
        &self.snapshot
    }

    /// Returns the writer fencing epoch from the authoritative table row.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.table.writer_epoch
    }

    /// Returns the durable logical boundary represented by the current snapshot.
    #[must_use]
    pub const fn committed_log_offset(&self) -> OffsetWatermark {
        self.snapshot.committed_log_offset
    }
}

/// An immutable, revision-consistent set of server-owned table runtimes.
///
/// Construction is all-or-nothing: inputs are validated into a local map and
/// the registry becomes observable only after every table succeeds.
#[derive(Debug)]
pub struct TableRuntimeRegistry {
    catalog_revision: u64,
    tables: BTreeMap<TableId, Arc<TableRuntime>>,
}

impl TableRuntimeRegistry {
    /// Builds a registry from strictly table-ID-ordered bootstrap records.
    pub fn from_bootstraps(
        catalog_revision: u64,
        bootstraps: Vec<TableRuntimeBootstrap>,
    ) -> Result<Self> {
        if bootstraps
            .windows(2)
            .any(|pair| pair[0].table_id() >= pair[1].table_id())
        {
            return Err(corruption(
                "startup table runtime bootstraps are not strictly ordered by table id",
            ));
        }

        let mut tables = BTreeMap::new();
        for bootstrap in bootstraps {
            let table_id = bootstrap.table_id();
            if tables
                .insert(table_id, Arc::new(TableRuntime::from_bootstrap(bootstrap)))
                .is_some()
            {
                return Err(corruption(format!(
                    "startup table runtime contains duplicate table id table_id=[{table_id}]"
                ))
                .with_table_id(table_id));
            }
        }
        Ok(Self {
            catalog_revision,
            tables,
        })
    }

    /// Returns the Catalog revision shared by every recovered table.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the number of active table runtimes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns whether no active table was recovered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Resolves one runtime by immutable table identity.
    #[must_use]
    pub fn get(&self, table_id: TableId) -> Option<Arc<TableRuntime>> {
        self.tables.get(&table_id).cloned()
    }

    /// Iterates runtimes in deterministic table-ID order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Arc<TableRuntime>> {
        self.tables.values()
    }
}

fn validate_bootstrap(
    table: &TableDescriptor,
    schema_descriptor: &SchemaDescriptor,
    _schema: &SchemaRef,
    snapshot: &TableSnapshot,
) -> Result<()> {
    if table.state != TableState::Active {
        return Err(corruption(format!(
            "startup table runtime requires active table state actual=[{:?}]",
            table.state
        ))
        .with_table_id(table.table_id));
    }
    if table.table_id != schema_descriptor.table_id || table.table_id != snapshot.table_id {
        return Err(corruption(
            "startup table runtime descriptors have mismatched table identities",
        )
        .with_table_id(table.table_id));
    }
    if table.current_schema_version == 0 || table.current_snapshot_version == 0 {
        return Err(corruption(
            "startup table runtime requires nonzero current schema and snapshot versions",
        )
        .with_table_id(table.table_id));
    }
    if schema_descriptor.schema_version != table.current_schema_version {
        return Err(corruption(format!(
            concat!("startup table runtime schema version mismatch expected=[{}] actual=[{}]"),
            table.current_schema_version, schema_descriptor.schema_version
        ))
        .with_table_id(table.table_id));
    }
    if snapshot.snapshot_version != table.current_snapshot_version {
        return Err(corruption(format!(
            concat!("startup table runtime snapshot version mismatch expected=[{}] actual=[{}]"),
            table.current_snapshot_version, snapshot.snapshot_version
        ))
        .with_table_id(table.table_id));
    }
    Ok(())
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

    use super::{TableRuntimeBootstrap, TableRuntimeRegistry};

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

    fn bootstrap(value: u128) -> TableRuntimeBootstrap {
        let table_id = table_id(value);
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
    }

    #[test]
    fn registry_is_published_in_table_id_order_with_exact_watermarks() {
        let registry =
            TableRuntimeRegistry::from_bootstraps(9, vec![bootstrap(10), bootstrap(20)]).unwrap();

        assert_eq!(registry.catalog_revision(), 9);
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry
                .iter()
                .map(|runtime| runtime.table_id())
                .collect::<Vec<_>>(),
            vec![table_id(10), table_id(20)]
        );
        let runtime = registry.get(table_id(10)).unwrap();
        assert_eq!(runtime.writer_epoch(), 7);
        assert_eq!(runtime.committed_log_offset(), OffsetWatermark::Empty);
        assert_eq!(runtime.schema().fields().len(), 1);
    }

    #[test]
    fn registry_rejects_unsorted_or_duplicate_bootstraps_atomically() {
        for bootstraps in [
            vec![bootstrap(20), bootstrap(10)],
            vec![bootstrap(10), bootstrap(10)],
        ] {
            let error = TableRuntimeRegistry::from_bootstraps(9, bootstraps).unwrap_err();
            assert_eq!(error.code(), ErrorCode::DataCorruption);
        }
    }

    #[test]
    fn bootstrap_rejects_cross_table_descriptors() {
        let value = bootstrap(10);
        let mut schema_descriptor = value.schema_descriptor;
        schema_descriptor.table_id = table_id(20);

        let error = TableRuntimeBootstrap::new(
            value.table,
            schema_descriptor,
            value.schema,
            value.snapshot,
        )
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }
}
