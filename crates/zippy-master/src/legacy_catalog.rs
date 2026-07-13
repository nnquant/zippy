//! Read-only catalog compatibility over persistent legacy registry metadata.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use zippy_types::catalog::{
    CatalogCommandV1, CatalogQuerySnapshot, CatalogRead, CatalogSnapshotSelector, CatalogWrite,
    ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    NamespaceRef, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
    KeyValueTableDefinition, NamespaceDescriptor, TableDefinition, TableDescriptor, TableRef,
    TableState,
};
use zippy_types::{
    BoxFuture, ErrorCode, NamespaceId, Result, ServerError, ServerErrorDetails, TableId,
};

use crate::registry::Registry;

const LEGACY_ID_SCHEME: &[u8] = b"zippy.legacy_catalog.id.v1";
const TABLE_KIND_FIELD: &str = "zippy_table_kind";
const FRAME_MODE_FIELD: &str = "zippy_frame_mode";
const FRAME_BY_FIELD: &str = "zippy_frame_by";
const FRAME_PROTOCOL_VERSION_FIELD: &str = "zippy_frame_protocol_version";
const KEY_VALUE_KEYS_FIELD: &str = "zippy_key_value_keys";
const LEGACY_LATEST_BY_FIELD: &str = "zippy_latest_by";
const INTERNAL_TABLE_FIELD: &str = "zippy_internal";
const SEGMENT_PER_BATCH_FRAME_MODE: &str = "segment_per_batch";
const FRAME_PROTOCOL_VERSION: u64 = 1;

/// Read-only bridge from the legacy master registry to the persistent catalog contract.
#[derive(Clone)]
pub struct LegacyCatalogAdapter {
    registry: Arc<Mutex<Registry>>,
    running: Arc<AtomicBool>,
}

#[derive(Debug)]
struct LegacyCatalogView {
    catalog_revision: u64,
    streams: Vec<LegacyStreamView>,
}

#[derive(Debug)]
struct LegacyStreamView {
    stream_name: String,
    persisted_files_revision: u64,
    writer_epoch: u64,
    source_configs: Vec<serde_json::Value>,
}

impl LegacyCatalogAdapter {
    pub(crate) fn new(registry: Arc<Mutex<Registry>>, running: Arc<AtomicBool>) -> Self {
        Self { registry, running }
    }

    fn lock_registry(&self) -> Result<std::sync::MutexGuard<'_, Registry>> {
        let registry = self.registry.lock().map_err(|_| {
            ServerError::new(
                ErrorCode::Internal,
                "legacy catalog registry lock is poisoned",
            )
        })?;
        if !self.running.load(Ordering::Acquire) {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "legacy catalog master lifecycle is stopped",
            ));
        }
        Ok(registry)
    }

    fn capture_view(&self) -> Result<LegacyCatalogView> {
        let registry = self.lock_registry()?;
        let mut source_configs = registry.source_configs_by_stream();
        let streams = registry
            .stream_records()
            .map(|stream| LegacyStreamView {
                stream_name: stream.stream_name.clone(),
                persisted_files_revision: stream.persisted_files_revision,
                writer_epoch: stream.writer_epoch,
                source_configs: source_configs
                    .remove(&stream.stream_name)
                    .unwrap_or_default(),
            })
            .collect();
        Ok(LegacyCatalogView {
            catalog_revision: registry.catalog_revision(),
            streams,
        })
    }

    fn read_revision(&self) -> Result<u64> {
        Ok(self.lock_registry()?.catalog_revision())
    }
}

impl CatalogRead for LegacyCatalogAdapter {
    fn revision(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move { self.read_revision() })
    }

    fn get_namespace(&self, namespace: NamespaceRef) -> BoxFuture<'_, Result<NamespaceDescriptor>> {
        Box::pin(async move {
            self.read_revision()?;
            let descriptor = default_namespace_descriptor();
            let found = match namespace {
                NamespaceRef::Id(namespace_id) => descriptor.namespace_id == namespace_id,
                NamespaceRef::Name(name) => descriptor.name == name,
            };
            if found {
                Ok(descriptor)
            } else {
                Err(ServerError::new(
                    ErrorCode::NotFound,
                    "legacy catalog namespace was not found",
                ))
            }
        })
    }

    fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> BoxFuture<'_, Result<ListNamespacesResponse>> {
        Box::pin(async move {
            let catalog_revision = self.read_revision()?;
            let descriptor = default_namespace_descriptor();
            let namespaces = if request
                .after
                .is_none_or(|after| descriptor.namespace_id > after)
            {
                vec![descriptor]
            } else {
                Vec::new()
            };
            Ok(ListNamespacesResponse {
                catalog_revision,
                namespaces,
                next_after: None,
            })
        })
    }

    fn get_table(&self, table: TableRef) -> BoxFuture<'_, Result<TableDescriptor>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let tables = map_tables(&view)?;
            resolve_table(&tables, &table).cloned().ok_or_else(|| {
                ServerError::new(ErrorCode::NotFound, "legacy catalog table was not found")
            })
        })
    }

    fn list_tables(&self, request: ListTablesRequest) -> BoxFuture<'_, Result<ListTablesResponse>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let namespace_id = legacy_namespace_id();
            let mut tables = if request
                .namespace_id
                .is_none_or(|requested| requested == namespace_id)
            {
                map_tables(&view)?
            } else {
                Vec::new()
            };
            if let Some(after) = request.after {
                tables.retain(|table| table.table_id > after);
            }
            let limit = usize::try_from(request.limit.get()).unwrap_or(usize::MAX);
            let has_more = tables.len() > limit;
            tables.truncate(limit);
            let next_after = has_more.then(|| tables.last().expect("nonzero page limit").table_id);
            Ok(ListTablesResponse {
                catalog_revision: view.catalog_revision,
                tables,
                next_after,
            })
        })
    }

    fn resolve_tables<'a>(
        &'a self,
        table_refs: &'a [TableRef],
    ) -> BoxFuture<'a, Result<Vec<TableDescriptor>>> {
        Box::pin(async move {
            let view = self.capture_view()?;
            let tables = map_tables(&view)?;
            table_refs
                .iter()
                .map(|table_ref| {
                    resolve_table(&tables, table_ref).cloned().ok_or_else(|| {
                        ServerError::new(ErrorCode::NotFound, "legacy catalog table was not found")
                    })
                })
                .collect()
        })
    }

    fn read_snapshot_at<'a>(
        &'a self,
        selectors: &'a [CatalogSnapshotSelector],
    ) -> BoxFuture<'a, Result<CatalogQuerySnapshot>> {
        Box::pin(async move {
            let revision = self.read_revision()?;
            if selectors.is_empty() {
                return Ok(CatalogQuerySnapshot::empty(revision));
            }
            Err(unsupported_plan("legacy_catalog.read_snapshot_at"))
        })
    }
}

impl CatalogWrite for LegacyCatalogAdapter {
    fn command(
        &self,
        command: VersionedCatalogCommand,
    ) -> BoxFuture<'_, Result<VersionedCatalogResponse>> {
        let VersionedCatalogCommand::V1(envelope) = command;
        let operator = match &envelope.command {
            CatalogCommandV1::CreateNamespace(_) => "legacy_catalog.command.create_namespace",
            CatalogCommandV1::CreateTable(_) => "legacy_catalog.command.create_table",
            CatalogCommandV1::AlterTableSchema(_) => "legacy_catalog.command.alter_table_schema",
            CatalogCommandV1::AcquireProducer(_) => "legacy_catalog.command.acquire_producer",
            CatalogCommandV1::CommitDataFiles(_) => "legacy_catalog.command.commit_data_files",
            CatalogCommandV1::CommitCompaction(_) => "legacy_catalog.command.commit_compaction",
            CatalogCommandV1::CommitConsumerOffset(_) => {
                "legacy_catalog.command.commit_consumer_offset"
            }
            CatalogCommandV1::AcquireSnapshotLease(_) => {
                "legacy_catalog.command.acquire_snapshot_lease"
            }
            CatalogCommandV1::ReleaseSnapshotLease(_) => {
                "legacy_catalog.command.release_snapshot_lease"
            }
            CatalogCommandV1::CreateMaintenanceJob(_) => {
                "legacy_catalog.command.create_maintenance_job"
            }
            CatalogCommandV1::TransitionMaintenanceJob(_) => {
                "legacy_catalog.command.transition_maintenance_job"
            }
            CatalogCommandV1::DropTable(_) => "legacy_catalog.command.drop_table",
        };
        let error = unsupported_plan(operator).with_request_id(envelope.request_id);
        Box::pin(async move { Err(error) })
    }
}

fn map_tables(view: &LegacyCatalogView) -> Result<Vec<TableDescriptor>> {
    let mut tables = Vec::new();
    for stream in &view.streams {
        if !has_explicit_table_kind(stream) || is_internal_table(stream)? {
            continue;
        }
        tables.push(map_table(stream)?);
    }
    tables.sort_by_key(|table| table.table_id);
    if let Some(duplicate) = tables
        .windows(2)
        .find(|pair| pair[0].table_id == pair[1].table_id)
    {
        return Err(data_corruption(
            duplicate[0].name.as_str(),
            "deterministic legacy table id collision",
        ));
    }
    Ok(tables)
}

fn map_table(stream: &LegacyStreamView) -> Result<TableDescriptor> {
    let kind = unique_metadata(stream, TABLE_KIND_FIELD)?
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| data_corruption(&stream.stream_name, "invalid zippy_table_kind"))?;
    let definition = match kind {
        "append_table" => TableDefinition::Append(AppendTableDefinition {
            event_time_column: None,
        }),
        "frame_table" => map_frame_definition(stream)?,
        "key_value_table" => map_key_value_definition(stream)?,
        _ => {
            return Err(data_corruption(
                &stream.stream_name,
                "unknown zippy_table_kind",
            ));
        }
    };
    let name = CatalogName::new(stream.stream_name.clone())
        .map_err(|_| data_corruption(&stream.stream_name, "invalid legacy table name"))?;
    Ok(TableDescriptor {
        table_id: legacy_table_id(&stream.stream_name),
        namespace_id: legacy_namespace_id(),
        name,
        definition,
        current_schema_version: 1,
        current_snapshot_version: stream.persisted_files_revision,
        writer_epoch: stream.writer_epoch,
        state: TableState::Active,
        config: BTreeMap::new(),
        created_at_ns: 0,
        updated_at_ns: 0,
    })
}

fn map_frame_definition(stream: &LegacyStreamView) -> Result<TableDefinition> {
    if let Some(mode) = unique_metadata(stream, FRAME_MODE_FIELD)? {
        if mode.as_str() != Some(SEGMENT_PER_BATCH_FRAME_MODE) {
            return Err(data_corruption(
                &stream.stream_name,
                "invalid zippy_frame_mode",
            ));
        }
    }
    if let Some(version) = unique_metadata(stream, FRAME_PROTOCOL_VERSION_FIELD)? {
        if version.as_u64() != Some(FRAME_PROTOCOL_VERSION) {
            return Err(data_corruption(
                &stream.stream_name,
                "invalid zippy_frame_protocol_version",
            ));
        }
    }
    let by = unique_metadata(stream, FRAME_BY_FIELD)?
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| data_corruption(&stream.stream_name, "missing or invalid zippy_frame_by"))?;
    let by = ColumnName::new(by.to_owned())
        .map_err(|_| data_corruption(&stream.stream_name, "invalid zippy_frame_by"))?;
    Ok(TableDefinition::Frame(FrameTableDefinition { by }))
}

fn map_key_value_definition(stream: &LegacyStreamView) -> Result<TableDefinition> {
    let keys = unique_metadata(stream, KEY_VALUE_KEYS_FIELD)?;
    let legacy_keys = unique_metadata(stream, LEGACY_LATEST_BY_FIELD)?;
    if keys.is_some() && legacy_keys.is_some() && keys != legacy_keys {
        return Err(data_corruption(
            &stream.stream_name,
            "conflicting key-value key metadata",
        ));
    }
    let values = keys
        .or(legacy_keys)
        .ok_or_else(|| data_corruption(&stream.stream_name, "missing key-value key metadata"))?;
    let values = values
        .as_array()
        .ok_or_else(|| data_corruption(&stream.stream_name, "invalid key-value key metadata"))?;
    let columns = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| data_corruption(&stream.stream_name, "invalid key-value key column"))
                .and_then(|column| {
                    ColumnName::new(column.to_owned()).map_err(|_| {
                        data_corruption(&stream.stream_name, "invalid key-value key column")
                    })
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = ColumnSet::new(columns)
        .map_err(|_| data_corruption(&stream.stream_name, "invalid key-value key columns"))?;
    let definition = KeyValueTableDefinition::new(columns)
        .map_err(|_| data_corruption(&stream.stream_name, "invalid key-value key columns"))?;
    Ok(TableDefinition::KeyValue(definition))
}

fn has_explicit_table_kind(stream: &LegacyStreamView) -> bool {
    stream.source_configs.iter().any(|config| {
        config
            .as_object()
            .is_some_and(|object| object.contains_key(TABLE_KIND_FIELD))
    })
}

fn is_internal_table(stream: &LegacyStreamView) -> Result<bool> {
    match unique_metadata(stream, INTERNAL_TABLE_FIELD)? {
        Some(value) => value
            .as_bool()
            .ok_or_else(|| data_corruption(&stream.stream_name, "invalid zippy_internal")),
        None => Ok(is_physical_shard_name(&stream.stream_name)),
    }
}

fn is_physical_shard_name(stream_name: &str) -> bool {
    let Some((parent, shard_index)) = stream_name.rsplit_once(".__shard_") else {
        return false;
    };
    !parent.is_empty()
        && shard_index.len() == 4
        && shard_index.chars().all(|value| value.is_ascii_digit())
}

fn unique_metadata<'a>(
    stream: &'a LegacyStreamView,
    key: &str,
) -> Result<Option<&'a serde_json::Value>> {
    let mut values = stream
        .source_configs
        .iter()
        .filter_map(|config| config.as_object().and_then(|object| object.get(key)));
    let Some(first) = values.next() else {
        return Ok(None);
    };
    if values.any(|value| value != first) {
        return Err(data_corruption(
            &stream.stream_name,
            &format!("conflicting {key} metadata"),
        ));
    }
    Ok(Some(first))
}

fn resolve_table<'a>(
    tables: &'a [TableDescriptor],
    table_ref: &TableRef,
) -> Option<&'a TableDescriptor> {
    match table_ref {
        TableRef::Id { table_id } => tables.iter().find(|table| table.table_id == *table_id),
        TableRef::Name { namespace, table } => {
            if namespace != &CatalogName::default_namespace() {
                return None;
            }
            tables.iter().find(|descriptor| descriptor.name == *table)
        }
    }
}

fn default_namespace_descriptor() -> NamespaceDescriptor {
    NamespaceDescriptor {
        namespace_id: legacy_namespace_id(),
        name: CatalogName::default_namespace(),
        created_at_ns: 0,
    }
}

fn legacy_namespace_id() -> NamespaceId {
    NamespaceId::from_bytes(derive_legacy_id("namespace", &[b"default"]))
        .expect("legacy namespace id is forced nonzero")
}

fn legacy_table_id(stream_name: &str) -> TableId {
    TableId::from_bytes(derive_legacy_id(
        "table",
        &[b"default", stream_name.as_bytes()],
    ))
    .expect("legacy table id is forced nonzero")
}

fn derive_legacy_id(domain: &str, components: &[&[u8]]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hash_component(&mut hasher, LEGACY_ID_SCHEME);
    hash_component(&mut hasher, domain.as_bytes());
    for component in components {
        hash_component(&mut hasher, component);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    if bytes.iter().all(|byte| *byte == 0) {
        bytes[15] = 1;
    }
    bytes
}

fn hash_component(hasher: &mut Sha256, component: &[u8]) {
    hasher.update((component.len() as u64).to_be_bytes());
    hasher.update(component);
}

fn data_corruption(stream_name: &str, message: &str) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
        .with_details(ServerErrorDetails::DataCorruption {
            object: format!("legacy_catalog/table/{stream_name}"),
            expected_checksum: None,
            actual_checksum: None,
        })
        .expect("data corruption detail matches error code")
}

fn unsupported_plan(operator: &str) -> ServerError {
    ServerError::new(
        ErrorCode::UnsupportedPlan,
        "legacy catalog adapter does not implement this operation",
    )
    .with_details(ServerErrorDetails::UnsupportedPlan {
        operator: operator.to_owned(),
    })
    .expect("unsupported plan detail matches error code")
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::NonZeroU32;
    use std::pin::pin;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    use serde_json::json;
    use zippy_types::catalog::{
        CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1, CatalogCommandV1, CatalogRead,
        CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, ListNamespacesRequest,
        ListTablesRequest, NamespaceRef, VersionedCatalogCommand,
    };
    use zippy_types::table::{CatalogName, TableRef};
    use zippy_types::{ErrorCode, NamespaceId, RequestId, ServerErrorDetails, TableId};

    use super::{legacy_namespace_id, map_tables, LegacyCatalogAdapter};
    use crate::registry::Registry;
    use crate::server::MasterServer;
    use crate::snapshot::{
        RegistrySnapshot, SnapshotSourceRecord, SnapshotStore, SnapshotStreamRecord,
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn add_stream(registry: &mut Registry, stream_name: &str, config: serde_json::Value) {
        registry
            .register_stream(stream_name, json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        registry
            .register_source(
                &format!("{stream_name}_source"),
                "test",
                &format!("{stream_name}_process"),
                stream_name,
                config,
            )
            .unwrap();
    }

    fn test_adapter() -> (LegacyCatalogAdapter, Arc<Mutex<Registry>>) {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "append_events",
            json!({"zippy_table_kind": "append_table"}),
        );
        add_stream(
            &mut registry,
            "cross_section",
            json!({
                "zippy_table_kind": "frame_table",
                "zippy_frame_mode": "segment_per_batch",
                "zippy_frame_by": "dt",
                "zippy_frame_protocol_version": 1
            }),
        );
        add_stream(
            &mut registry,
            "latest_factors",
            json!({
                "zippy_table_kind": "key_value_table",
                "zippy_key_value_keys": ["instrument_id"]
            }),
        );
        add_stream(
            &mut registry,
            "append_events.__shard_0000",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": true,
                "zippy_logical_parent": "append_events"
            }),
        );
        add_stream(
            &mut registry,
            "legacy_events.__shard_0001",
            json!({"zippy_table_kind": "append_table"}),
        );
        add_stream(&mut registry, "plain_stream", json!({"source": "test"}));
        let registry = Arc::new(Mutex::new(registry));
        (
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true))),
            registry,
        )
    }

    #[test]
    fn discovery_exposes_only_explicit_tables_with_stable_ids() {
        let (adapter, _) = test_adapter();
        let namespace =
            block_on(adapter.get_namespace(NamespaceRef::Name(CatalogName::default_namespace())))
                .unwrap();
        assert_eq!(namespace.namespace_id, legacy_namespace_id());
        assert_eq!(
            namespace.namespace_id.to_string(),
            "2dd41c3d-f74d-fb68-e1e5-e58084d335d6"
        );

        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: Some(namespace.namespace_id),
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        assert_eq!(page.tables.len(), 3);
        assert!(page
            .tables
            .windows(2)
            .all(|pair| pair[0].table_id < pair[1].table_id));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "plain_stream"));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "append_events.__shard_0000"));
        assert!(page
            .tables
            .iter()
            .all(|table| table.name.as_str() != "legacy_events.__shard_0001"));
        let append = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "append_events")
            .unwrap();
        assert_eq!(
            append.table_id.to_string(),
            "28b27ce0-4cb2-a769-efa9-2deb5cc3c29d"
        );
        let frame = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "cross_section")
            .unwrap();
        assert!(matches!(
            &frame.definition,
            zippy_types::table::TableDefinition::Frame(definition)
                if definition.by.as_str() == "dt"
        ));
        let key_value = page
            .tables
            .iter()
            .find(|table| table.name.as_str() == "latest_factors")
            .unwrap();
        assert!(matches!(
            &key_value.definition,
            zippy_types::table::TableDefinition::KeyValue(definition)
                if definition.keys().as_slice()[0].as_str() == "instrument_id"
        ));

        let second = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        assert_eq!(page.tables, second.tables);
        assert_ne!(
            namespace.namespace_id.as_u128(),
            page.tables[0].table_id.as_u128()
        );
    }

    #[test]
    fn table_pagination_uses_strict_stable_id_cursors() {
        let (adapter, _) = test_adapter();
        let mut after = None;
        let mut table_ids = Vec::new();
        loop {
            let page = block_on(adapter.list_tables(ListTablesRequest {
                namespace_id: None,
                after,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap();
            table_ids.extend(page.tables.iter().map(|table| table.table_id));
            let Some(next) = page.next_after else {
                break;
            };
            after = Some(next);
        }
        assert_eq!(table_ids.len(), 3);
        assert!(table_ids.windows(2).all(|pair| pair[0] < pair[1]));

        let exhausted = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: table_ids.last().copied(),
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert!(exhausted.tables.is_empty());
        assert_eq!(exhausted.next_after, None);
    }

    #[test]
    fn namespace_pagination_is_strict_and_bounded() {
        let (adapter, _) = test_adapter();
        let first = block_on(adapter.list_namespaces(ListNamespacesRequest {
            after: None,
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert_eq!(first.namespaces.len(), 1);
        assert_eq!(first.next_after, None);
        let exhausted = block_on(adapter.list_namespaces(ListNamespacesRequest {
            after: Some(first.namespaces[0].namespace_id),
            limit: NonZeroU32::new(1).unwrap(),
        }))
        .unwrap();
        assert!(exhausted.namespaces.is_empty());
    }

    #[test]
    fn resolution_preserves_input_order_and_reports_missing_tables() {
        let (adapter, _) = test_adapter();
        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();
        let refs = [
            TableRef::from(page.tables[2].table_id),
            TableRef::from(page.tables[0].table_id),
        ];
        let resolved = block_on(adapter.resolve_tables(&refs)).unwrap();
        assert_eq!(resolved[0].table_id, page.tables[2].table_id);
        assert_eq!(resolved[1].table_id, page.tables[0].table_id);

        let missing = block_on(adapter.get_table(TableRef::from(TableId::from_u128(1).unwrap())))
            .unwrap_err();
        assert_eq!(missing.code(), ErrorCode::NotFound);
    }

    #[test]
    fn malformed_explicit_metadata_is_data_corruption() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "broken_frame",
            json!({"zippy_table_kind": "frame_table"}),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );
        let error = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::DataCorruption { object, .. })
                if object == "legacy_catalog/table/broken_frame"
        ));
    }

    #[test]
    fn malformed_internal_marker_is_data_corruption() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "broken_internal",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": "yes"
            }),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let error = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap_err();

        assert_eq!(error.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn explicit_public_marker_overrides_physical_shard_name_fallback() {
        let mut registry = Registry::default();
        add_stream(
            &mut registry,
            "public.__shard_0000",
            json!({
                "zippy_table_kind": "append_table",
                "zippy_internal": false
            }),
        );
        let adapter = LegacyCatalogAdapter::new(
            Arc::new(Mutex::new(registry)),
            Arc::new(AtomicBool::new(true)),
        );

        let page = block_on(adapter.list_tables(ListTablesRequest {
            namespace_id: None,
            after: None,
            limit: NonZeroU32::new(10).unwrap(),
        }))
        .unwrap();

        assert_eq!(page.tables.len(), 1);
        assert_eq!(page.tables[0].name.as_str(), "public.__shard_0000");
    }

    #[test]
    fn empty_snapshot_read_succeeds_and_nonempty_read_is_unsupported() {
        let (adapter, _) = test_adapter();
        let empty = block_on(adapter.read_snapshot_at(&[])).unwrap();
        assert!(empty.is_empty());
        let selectors = [CatalogSnapshotSelector {
            table_id: TableId::from_u128(1).unwrap(),
            snapshot_version: 0,
            schema_version: 1,
        }];
        let error = block_on(adapter.read_snapshot_at(&selectors)).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::UnsupportedPlan { operator })
                if operator == "legacy_catalog.read_snapshot_at"
        ));
    }

    #[test]
    fn stopped_master_rejects_every_catalog_read_entrypoint() {
        let master = MasterServer::default();
        let adapter = master.legacy_catalog_adapter();
        master.shutdown();

        let namespace = NamespaceRef::Name(CatalogName::default_namespace());
        let table = TableRef::Id {
            table_id: TableId::from_u128(1).unwrap(),
        };
        let selector = CatalogSnapshotSelector {
            table_id: TableId::from_u128(1).unwrap(),
            snapshot_version: 0,
            schema_version: 1,
        };
        let errors = [
            block_on(adapter.revision()).unwrap_err(),
            block_on(adapter.get_namespace(namespace)).unwrap_err(),
            block_on(adapter.list_namespaces(ListNamespacesRequest {
                after: None,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap_err(),
            block_on(adapter.get_table(table.clone())).unwrap_err(),
            block_on(adapter.list_tables(ListTablesRequest {
                namespace_id: None,
                after: None,
                limit: NonZeroU32::new(1).unwrap(),
            }))
            .unwrap_err(),
            block_on(adapter.resolve_tables(&[table])).unwrap_err(),
            block_on(adapter.read_snapshot_at(&[])).unwrap_err(),
            block_on(adapter.read_snapshot_at(&[selector])).unwrap_err(),
        ];

        assert!(errors
            .iter()
            .all(|error| error.code() == ErrorCode::Unavailable));
    }

    #[test]
    fn every_legacy_write_is_rejected_with_request_context() {
        let (adapter, _) = test_adapter();
        let request_id = RequestId::from_u128(7).unwrap();
        let envelope = CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            CatalogActor {
                kind: CatalogActorKind::Migration,
                principal: "test".to_owned(),
            },
            CatalogCommandV1::CreateNamespace(CreateNamespaceCommand {
                namespace_id: NamespaceId::from_u128(9).unwrap(),
                name: CatalogName::new("unsupported").unwrap(),
            }),
        )
        .unwrap();
        let error = block_on(adapter.command(VersionedCatalogCommand::V1(envelope))).unwrap_err();
        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert_eq!(error.request_id(), Some(request_id));
        assert!(matches!(
            error.details(),
            Some(ServerErrorDetails::UnsupportedPlan { operator })
                if operator == "legacy_catalog.command.create_namespace"
        ));
    }

    #[test]
    fn table_mapping_does_not_hold_or_reacquire_registry_lock() {
        let (adapter, registry) = test_adapter();
        let view = adapter.capture_view().unwrap();
        let _guard = registry.lock().unwrap();
        let tables = map_tables(&view).unwrap();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn catalog_revision_ignores_runtime_only_control_changes() {
        let mut registry = Registry::default();
        assert_eq!(registry.catalog_revision(), 0);
        let process_id = registry.register_process("test");
        assert_eq!(registry.catalog_revision(), 0);

        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        assert_eq!(registry.catalog_revision(), 1);
        registry
            .register_source(
                "events_source",
                "test",
                &process_id,
                "events",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry
            .set_source_status("events_source", "running", Some(json!({"rows": 1})))
            .unwrap();
        registry.set_stream_status("events", "active").unwrap();
        registry.record_heartbeat(&process_id).unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry.attach_writer("events", &process_id).unwrap();
        assert_eq!(registry.catalog_revision(), 3);
        registry.detach_writer("events").unwrap();
        assert_eq!(registry.catalog_revision(), 3);
    }

    #[test]
    fn snapshot_version_tracks_only_committed_file_view() {
        let mut registry = Registry::default();
        let process_id = registry.register_process("test");
        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        registry
            .register_source(
                "events_source",
                "test",
                &process_id,
                "events",
                json!({"zippy_table_kind": "append_table"}),
            )
            .unwrap();
        let registry = Arc::new(Mutex::new(registry));
        let adapter =
            LegacyCatalogAdapter::new(Arc::clone(&registry), Arc::new(AtomicBool::new(true)));
        let table_ref = TableRef::Name {
            namespace: CatalogName::default_namespace(),
            table: CatalogName::new("events").unwrap(),
        };
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            0
        );

        registry
            .lock()
            .unwrap()
            .publish_persist_event(
                "events",
                &process_id,
                json!({"persist_event_type": "persist_failed"}),
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            0
        );

        registry
            .lock()
            .unwrap()
            .replace_persisted_files(
                "events",
                vec![json!({"file_path": "events.parquet", "created_at": 1})],
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref.clone()))
                .unwrap()
                .current_snapshot_version,
            1
        );

        registry
            .lock()
            .unwrap()
            .publish_persist_event(
                "events",
                &process_id,
                json!({"persist_event_type": "persist_metadata_sync_failed"}),
            )
            .unwrap();
        assert_eq!(
            block_on(adapter.get_table(table_ref))
                .unwrap()
                .current_snapshot_version,
            1
        );
    }

    #[test]
    fn repeated_segment_metadata_does_not_advance_catalog_revision() {
        let mut registry = Registry::default();
        registry
            .register_stream("events", json!({"fields": []}), "schema", 64, 64)
            .unwrap();
        assert_eq!(registry.catalog_revision(), 1);

        let persisted_files = vec![json!({"file_path": "events.parquet"})];
        let persist_events = vec![json!({"persist_event_type": "flush"})];
        registry
            .set_stream_segment_metadata(
                "events",
                1,
                vec![json!({"source_segment_id": 1})],
                persisted_files.clone(),
                persist_events.clone(),
                5,
                4,
                vec![json!({"lease_id": "lease-1"})],
                3,
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);

        registry
            .set_stream_segment_metadata(
                "events",
                2,
                vec![json!({"source_segment_id": 2})],
                persisted_files,
                persist_events,
                5,
                4,
                vec![json!({"lease_id": "lease-2"})],
                3,
            )
            .unwrap();
        assert_eq!(registry.catalog_revision(), 2);
    }

    #[test]
    fn catalog_revision_and_ids_survive_snapshot_restore() {
        let temp = tempfile::tempdir().unwrap();
        let snapshot_path = temp.path().join("master-registry.json");
        SnapshotStore::write(
            &snapshot_path,
            &RegistrySnapshot {
                catalog_revision: 41,
                streams: vec![SnapshotStreamRecord {
                    stream_name: "restored_frames".to_owned(),
                    schema: json!({"fields": []}),
                    schema_hash: "schema".to_owned(),
                    data_path: "segment".to_owned(),
                    descriptor_generation: 2,
                    sealed_segments: Vec::new(),
                    persisted_files: Vec::new(),
                    persist_events: Vec::new(),
                    persist_revision: 5,
                    persisted_files_revision: 5,
                    segment_reader_leases: Vec::new(),
                    writer_epoch: 7,
                    buffer_size: 64,
                    frame_size: 64,
                    status: "active".to_owned(),
                }],
                sources: vec![SnapshotSourceRecord {
                    source_name: "restored_frames_source".to_owned(),
                    source_type: "test".to_owned(),
                    process_id: "proc_1".to_owned(),
                    output_stream: "restored_frames".to_owned(),
                    config: json!({
                        "zippy_table_kind": "frame_table",
                        "zippy_frame_mode": "segment_per_batch",
                        "zippy_frame_by": "dt",
                        "zippy_frame_protocol_version": 1
                    }),
                    status: "running".to_owned(),
                    metrics: json!({}),
                    writer_epoch: 7,
                    revision: 9,
                }],
                engines: Vec::new(),
            },
        )
        .unwrap();

        let first = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
        let second = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
        let first_adapter = first.legacy_catalog_adapter();
        let second_adapter = second.legacy_catalog_adapter();
        assert_eq!(block_on(first_adapter.revision()).unwrap(), 41);
        assert_eq!(block_on(second_adapter.revision()).unwrap(), 41);

        let table_ref = TableRef::Name {
            namespace: CatalogName::default_namespace(),
            table: CatalogName::new("restored_frames").unwrap(),
        };
        let first_table = block_on(first_adapter.get_table(table_ref.clone())).unwrap();
        let second_table = block_on(second_adapter.get_table(table_ref)).unwrap();
        assert_eq!(first_table.table_id, second_table.table_id);
        assert_eq!(
            first_table.table_id.to_string(),
            "7e38750c-487e-853a-3e22-b0f7ed898aa7"
        );
        assert_eq!(first_table.current_snapshot_version, 5);
        assert_eq!(first_table.writer_epoch, 7);
    }
}
