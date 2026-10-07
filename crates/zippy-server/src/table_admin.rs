//! Native table management: durable Catalog commands followed by runtime installation.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};
use zippy_catalog::{
    decode_persisted_table_schema, SqliteCatalogReadHandle, SqliteCatalogWriteHandle,
};
use zippy_ingest::{
    NativeIngestHandle, NativeIngestTableBootstrap, NativeTableMaterializerSink,
    NativeTableRegistration, TableActorRegistrar, TableActorRegistryHandle, TableRuntime,
    TableRuntimeBootstrap,
};
use zippy_types::catalog::{
    AlterTableSchemaCommand, CatalogActor, CatalogActorKind, CatalogCommandEnvelopeV1,
    CatalogCommandReplayProbe, CatalogCommandV1, CatalogIdempotencyRead, CatalogRead,
    CatalogSnapshotSelector, CatalogWrite, CreateNamespaceCommand, CreateTableCommand,
    DropTableCommand, VersionedCatalogCommand, VersionedCatalogResponse,
};
use zippy_types::table::{KeyValueTableDefinition, SchemaIpcPayload, TableDefinition, TableRef};
use zippy_types::{ErrorCode, RequestId, Result, ServerError, TableId};

/// Server-owned authority for installing tables in an already running native stack.
///
/// Calls must be authorized by the transport before reaching this capability.
/// Repeating the same request ID and payload repairs an interrupted installation.
#[derive(Clone)]
pub struct NativeTableAdmin {
    read: SqliteCatalogReadHandle,
    write: SqliteCatalogWriteHandle,
    actors: TableActorRegistryHandle,
    registrar: TableActorRegistrar,
    sink: Arc<NativeTableMaterializerSink>,
    ingest: NativeIngestHandle,
    management: Arc<Mutex<TableManagementBudget>>,
    limits: NativeTableAdminLimits,
}

#[derive(Clone)]
pub(crate) struct NativeTableAdminLimits {
    pub max_tables: usize,
    pub max_metadata_bytes: u64,
    pub initial_metadata_bytes: u64,
    pub initial_table_bytes: BTreeMap<TableId, u64>,
}

struct TableManagementBudget {
    used: u64,
    table_bytes: BTreeMap<TableId, u64>,
}

impl NativeTableAdmin {
    pub(crate) fn new(
        read: SqliteCatalogReadHandle,
        write: SqliteCatalogWriteHandle,
        actors: TableActorRegistryHandle,
        registrar: TableActorRegistrar,
        sink: Arc<NativeTableMaterializerSink>,
        ingest: NativeIngestHandle,
        mut limits: NativeTableAdminLimits,
    ) -> Self {
        let table_bytes = std::mem::take(&mut limits.initial_table_bytes);
        Self {
            read,
            write,
            actors,
            registrar,
            sink,
            ingest,
            management: Arc::new(Mutex::new(TableManagementBudget {
                used: limits.initial_metadata_bytes,
                table_bytes,
            })),
            limits,
        }
    }

    /// Creates a namespace using the caller's stable idempotency key.
    pub async fn create_namespace(
        &self,
        request_id: RequestId,
        command: CreateNamespaceCommand,
    ) -> Result<VersionedCatalogResponse> {
        self.execute(request_id, CatalogCommandV1::CreateNamespace(command))
            .await
    }

    /// Creates a table and acknowledges only after all runtime services admit it.
    ///
    /// Key-value commands carry the logical user schema. Catalog retains the canonical event
    /// schema so live segments and immutable files share one representation.
    pub async fn create_table(
        &self,
        request_id: RequestId,
        mut command: CreateTableCommand,
    ) -> Result<VersionedCatalogResponse> {
        command.schema_ipc = management_storage_schema(
            command.schema_ipc,
            match &command.definition {
                TableDefinition::KeyValue(definition) => Some(definition),
                _ => None,
            },
            request_id,
        )?;
        self.execute(request_id, CatalogCommandV1::CreateTable(command))
            .await
    }

    /// Atomically retires a table's Catalog visibility and all native runtime authorities.
    ///
    /// Busy persistence or mutation work returns a retryable error before Catalog commit.
    /// An admitted task survives cancellation, and repeating the request repairs cleanup.
    pub async fn drop_table(
        &self,
        request_id: RequestId,
        command: DropTableCommand,
    ) -> Result<VersionedCatalogResponse> {
        let mut budget = self.lock_management(request_id)?;
        let admission = self.ingest.begin_table_registration()?;
        let table_id = command.table_id;
        let envelope = CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "flight-administrator".to_owned(),
            },
            CatalogCommandV1::DropTable(command),
        )
        .map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("invalid table deletion command error=[{error}]"),
            )
            .with_request_id(request_id)
            .with_retryable(false)
        })?;
        let service = self.clone();
        tokio::spawn(async move {
            let _admission = admission;
            let ingest = if service.ingest.contains_table(table_id)? {
                Some(service.ingest.begin_table_transition(table_id)?)
            } else {
                None
            };
            let actor = match service.actors.table(table_id) {
                Ok(_) => Some(service.registrar.prepare_retirement(table_id).await?),
                Err(error) if error.code() == ErrorCode::NotFound => None,
                Err(error) => return Err(error),
            };
            let response = service.write.command(envelope.into()).await?;
            service.sink.retire_table(table_id);
            if let Some(ingest) = ingest {
                ingest.retire(zippy_types::table::TableRetirementReason::Dropped);
            }
            if let Some(actor) = actor {
                actor
                    .retire(zippy_types::table::TableRetirementReason::Dropped)
                    .await?;
            }
            if let Some(bytes) = budget.table_bytes.remove(&table_id) {
                budget.used = budget.used.checked_sub(bytes).ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::Internal,
                        "table metadata retirement budget underflow",
                    )
                    .with_table_id(table_id)
                    .with_retryable(false)
                })?;
            }
            Ok(response)
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("native table deletion task failed error=[{error}]"),
            )
            .with_request_id(request_id)
        })?
        .map_err(|error: ServerError| error.with_request_id(request_id))
    }

    /// Evolves a native table after durably draining all writes under its previous schema.
    ///
    /// The command carries the logical user schema. Key-value callers must explicitly supply
    /// the unchanged ordered primary keys, making retries independent of live table discovery.
    /// Existing columns and identity fields follow the native evolution policy.
    /// The admitted task survives caller cancellation. Exact retries repair installation
    /// without retiring a newer runtime or resurrecting a subsequently dropped table.
    pub async fn alter_table_schema(
        &self,
        request_id: RequestId,
        mut command: AlterTableSchemaCommand,
        key_value: Option<KeyValueTableDefinition>,
    ) -> Result<VersionedCatalogResponse> {
        command.schema_ipc =
            management_storage_schema(command.schema_ipc, key_value.as_ref(), request_id)?;
        let mut budget = self.lock_management(request_id)?;
        let admission = self.ingest.begin_table_registration()?;
        let table_id = command.table_id;
        let envelope: VersionedCatalogCommand = CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "flight-administrator".to_owned(),
            },
            CatalogCommandV1::AlterTableSchema(command.clone()),
        )
        .map_err(|error| ServerError::new(ErrorCode::InvalidArgument, error.to_string()))?
        .into();
        let service = self.clone();
        tokio::spawn(async move {
            if let CatalogCommandReplayProbe::Exact { response } =
                service.read.probe_command_replay(&envelope).await?
            {
                match service.read.get_table(TableRef::Id { table_id }).await {
                    Ok(_) => service.install(table_id, admission).await?,
                    Err(error) if error.code() == ErrorCode::NotFound => {}
                    Err(error) => return Err(error),
                }
                return Ok(response);
            }
            let table = service.read.get_table(TableRef::Id { table_id }).await?;
            if table.current_schema_version != command.expected_schema_version {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "table schema changed since the caller observed it",
                )
                .with_retryable(false));
            }
            zippy_catalog::canonical_schema_hash_from_ipc(&command.schema_ipc)?;
            let proposed = arrow::ipc::reader::StreamReader::try_new(
                Cursor::new(command.schema_ipc.as_bytes()),
                None,
            )
            .map_err(|error| ServerError::new(ErrorCode::InvalidArgument, error.to_string()))?
            .schema();
            let current = service
                .read
                .read_schemas_at_with_limits(
                    &[zippy_catalog::SqliteCatalogSchemaSelector::try_new(
                        table_id,
                        table.current_schema_version,
                    )?],
                    zippy_catalog::SqliteCatalogSchemaReadLimits::default(),
                )
                .await?;
            let current =
                decode_persisted_table_schema(&table, &current.schemas()[0])?.into_schema();
            NativeTableMaterializerSink::validate_schema(&proposed)?;
            zippy_types::schema_evolution::classify_schema_change(
                &table.definition,
                &current,
                &proposed,
            )?;
            let schema_bytes =
                zippy_types::schema_memory::conservative_arrow_schema_retained_bytes(&proposed)?
                    .get();
            let descriptor_bytes = serde_json::to_vec(&table)
                .map_err(|error| {
                    ServerError::new(
                        ErrorCode::Internal,
                        format!("cannot measure table metadata error=[{error}]"),
                    )
                })?
                .len() as u64;
            let table_bytes = descriptor_bytes
                .saturating_add(command.schema_ipc.as_bytes().len() as u64)
                .saturating_add(schema_bytes)
                .saturating_add(4096);
            let previous_bytes = budget.table_bytes.get(&table_id).copied().ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Internal,
                    "table schema change has no metadata reservation",
                )
            })?;
            let next_used = budget
                .used
                .checked_sub(previous_bytes)
                .ok_or_else(|| {
                    ServerError::new(ErrorCode::Internal, "table metadata budget underflow")
                })?
                .saturating_add(table_bytes);
            if next_used > service.limits.max_metadata_bytes {
                return Err(ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "table schema change exceeds the startup metadata byte limit",
                )
                .with_retryable(false));
            }
            let ingest = service.ingest.begin_table_transition(table_id)?;
            ingest.checkpoint().await?;
            let actor = service.registrar.prepare_retirement(table_id).await?;
            let response = service.write.command(envelope).await?;
            // The durable schema now owns this identity. Old writers must remain fenced even
            // if retiring or installing runtime instances fails; an exact retry repairs it.
            service.sink.retire_table(table_id);
            ingest.retire(zippy_types::table::TableRetirementReason::SchemaChanged);
            budget.used = next_used;
            budget.table_bytes.insert(table_id, table_bytes);
            actor
                .retire(zippy_types::table::TableRetirementReason::SchemaChanged)
                .await?;
            service.install(table_id, admission).await?;
            Ok(response)
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("native schema change task failed error=[{error}]"),
            )
        })?
        .map_err(|error: ServerError| error.with_request_id(request_id))
    }

    async fn execute(
        &self,
        request_id: RequestId,
        command: CatalogCommandV1,
    ) -> Result<VersionedCatalogResponse> {
        // Bound pending management work. Once admitted, cancellation must not leave an
        // otherwise healthy server with committed metadata but missing runtime state.
        let mut guard = self.lock_management(request_id)?;
        let admission = self.ingest.begin_table_registration()?;
        let envelope: VersionedCatalogCommand = CatalogCommandEnvelopeV1::new(
            request_id,
            None,
            CatalogActor {
                kind: CatalogActorKind::User,
                principal: "flight-administrator".to_owned(),
            },
            command,
        )
        .map_err(|error| {
            ServerError::new(
                ErrorCode::InvalidArgument,
                format!("invalid table management command error=[{error}]"),
            )
            .with_request_id(request_id)
            .with_retryable(false)
        })?
        .into();
        let VersionedCatalogCommand::V1(ref body) = envelope;
        let table_id = match &body.command {
            CatalogCommandV1::CreateTable(command) => Some(command.table_id),
            _ => None,
        };
        let mut additional_bytes = 0;
        if let CatalogCommandV1::CreateTable(command) = &body.command {
            match self
                .read
                .get_table(TableRef::Id {
                    table_id: command.table_id,
                })
                .await
            {
                Ok(_) => {}
                Err(error) if error.code() == ErrorCode::NotFound => {
                    // A tombstoned create replay allocates no table or metadata. Resolve it
                    // before admission checks, even when a replacement occupies all capacity.
                    if let CatalogCommandReplayProbe::Exact { response } =
                        self.read.probe_command_replay(&envelope).await?
                    {
                        return Ok(response);
                    }
                    if self.actors.len() >= self.limits.max_tables {
                        return Err(ServerError::new(
                            ErrorCode::ResourceExhausted,
                            "online table creation exceeds the startup table limit",
                        )
                        .with_request_id(request_id)
                        .with_retryable(false));
                    }
                    // Validate IPC budgets before Arrow decoding, then conservatively charge
                    // raw command metadata, decoded schema, and fixed descriptor overhead.
                    zippy_catalog::canonical_schema_hash_from_ipc(&command.schema_ipc)?;
                    let reader = arrow::ipc::reader::StreamReader::try_new(
                        Cursor::new(command.schema_ipc.as_bytes()),
                        None,
                    )
                    .map_err(|error| {
                        ServerError::new(
                            ErrorCode::InvalidArgument,
                            format!("invalid table schema error=[{error}]"),
                        )
                    })?;
                    NativeTableMaterializerSink::validate_schema(&reader.schema())?;
                    let schema_bytes =
                        zippy_types::schema_memory::conservative_arrow_schema_retained_bytes(
                            reader.schema().as_ref(),
                        )?
                        .get();
                    let command_bytes = serde_json::to_vec(command)
                        .map_err(|error| {
                            ServerError::new(
                                ErrorCode::InvalidArgument,
                                format!("cannot measure table command error=[{error}]"),
                            )
                        })?
                        .len() as u64;
                    additional_bytes = command_bytes
                        .saturating_add(schema_bytes)
                        .saturating_add(4096);
                    if guard.used.saturating_add(additional_bytes) > self.limits.max_metadata_bytes
                    {
                        return Err(ServerError::new(
                            ErrorCode::ResourceExhausted,
                            "online table creation exceeds the startup metadata byte limit",
                        )
                        .with_request_id(request_id)
                        .with_retryable(false));
                    }
                }
                Err(error) => return Err(error),
            }
        }
        let service = self.clone();
        tokio::spawn(async move {
            let response = service.write.command(envelope).await?;
            if let Some(table_id) = table_id {
                // A retained create response must not resurrect a table dropped later.
                match service.read.get_table(TableRef::Id { table_id }).await {
                    Ok(_) => {}
                    Err(error) if error.code() == ErrorCode::NotFound => return Ok(response),
                    Err(error) => return Err(error),
                }
                if additional_bytes != 0 {
                    guard.used = guard.used.saturating_add(additional_bytes);
                    guard.table_bytes.insert(table_id, additional_bytes);
                }
                service.install(table_id, admission).await?;
            }
            Ok(response)
        })
        .await
        .map_err(|error| {
            ServerError::new(
                ErrorCode::Internal,
                format!("native table management task failed error=[{error}]"),
            )
            .with_request_id(request_id)
        })?
        .map_err(|error: ServerError| error.with_request_id(request_id))
    }

    /// Bounds pending management work to one admitted command at a time.
    fn lock_management(
        &self,
        request_id: RequestId,
    ) -> Result<OwnedMutexGuard<TableManagementBudget>> {
        Arc::clone(&self.management).try_lock_owned().map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "native table management is busy",
            )
            .with_request_id(request_id)
            .with_retryable(true)
        })
    }

    async fn install(&self, table_id: TableId, admission: NativeTableRegistration) -> Result<()> {
        if self.ingest.contains_table(table_id)? {
            return Ok(());
        }
        let table = self.read.get_table(TableRef::Id { table_id }).await?;
        let selectors = [CatalogSnapshotSelector {
            table_id,
            snapshot_version: table.current_snapshot_version,
            schema_version: table.current_schema_version,
        }];
        let snapshot = self.read.read_snapshot_at(&selectors).await?;
        let table = snapshot.tables().first().ok_or_else(|| {
            ServerError::new(
                ErrorCode::DataCorruption,
                "created table is absent from its Catalog snapshot",
            )
        })?;
        let schema = decode_persisted_table_schema(table.table(), table.schema())?.into_schema();
        let bootstrap = NativeIngestTableBootstrap::from_catalog_snapshot(
            table.table(),
            Arc::clone(&schema),
            table.snapshot(),
        )?;
        match self.actors.table(table_id) {
            Ok(_) => {}
            Err(error) if error.code() == ErrorCode::NotFound => {
                self.registrar
                    .register(Arc::new(TableRuntime::from_bootstrap(
                        TableRuntimeBootstrap::new(
                            table.table().clone(),
                            table.schema().clone(),
                            schema,
                            table.snapshot().clone(),
                        )?,
                    )))?;
            }
            Err(error) => return Err(error),
        }
        match self.sink.register_table(&bootstrap, &self.actors) {
            Ok(()) => {}
            Err(error) if error.code() == ErrorCode::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        admission.register(bootstrap).await
    }
}

/// Converts the only public schema representation into the canonical storage representation.
fn management_storage_schema(
    schema_ipc: SchemaIpcPayload,
    key_value: Option<&KeyValueTableDefinition>,
    request_id: RequestId,
) -> Result<SchemaIpcPayload> {
    zippy_catalog::canonical_schema_hash_from_ipc(&schema_ipc)?;
    let arrow_error = |error: arrow::error::ArrowError| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            format!("invalid management schema error=[{error}]"),
        )
        .with_request_id(request_id)
        .with_retryable(false)
    };
    let schema =
        arrow::ipc::reader::StreamReader::try_new(Cursor::new(schema_ipc.as_bytes()), None)
            .map_err(arrow_error)?
            .schema();
    if schema
        .metadata()
        .contains_key(zippy_types::key_value::LOG_KEYS_METADATA)
    {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "table management requires a logical user schema without an event envelope",
        )
        .with_request_id(request_id)
        .with_retryable(false));
    }
    let Some(definition) = key_value else {
        return Ok(schema_ipc);
    };
    let keys = definition
        .keys()
        .as_slice()
        .iter()
        .map(|key| key.as_str().to_owned())
        .collect::<Vec<_>>();
    let physical = zippy_types::key_value::log_schema(&schema, &keys)?;
    let mut bytes = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &physical).map_err(arrow_error)?;
    writer.finish().map_err(arrow_error)?;
    drop(writer);
    SchemaIpcPayload::new(bytes).map_err(|error| {
        ServerError::new(ErrorCode::InvalidArgument, error.to_string())
            .with_request_id(request_id)
            .with_retryable(false)
    })
}
