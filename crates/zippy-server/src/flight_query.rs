//! Arrow Flight adapter for the server-owned native query authority.

use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::datatypes::Schema;
use zippy_catalog::{
    decode_persisted_schema, SqliteCatalogReadHandle, SqliteCatalogSchemaReadLimits,
    SqliteCatalogSchemaSelector,
};
use zippy_ingest::{ArrowIpcMutationFingerprintComputer, NativeIngestHandle};
use zippy_protocol::ingest::{
    put_batch_request_id, put_batch_result_from_receipt, FlightPutBatch, FlightPutCommand,
};
use zippy_protocol::query::FlightQueryRequest;
use zippy_protocol::server::{
    FlightIngestWriter, FlightQueryFrontend, OpaqueFlightIngestWriter, OpaquePreparedFlightQuery,
    PreparedFlightQuery,
};
use zippy_protocol::sql::{
    FlightSqlMetadataLimits, FlightSqlMetadataRequest, PreparedFlightSqlMetadata,
};
use zippy_types::catalog::{CatalogRead, ListNamespacesRequest, ListTablesRequest};
use zippy_types::ingest::{
    Durability, DurableWriteTarget, IncomingWriteRequest, IngestService,
    MutationFingerprintComputer, OpaqueDurableWriteAck, OpaqueIngestWriter, OpenWriterRequest,
    TableMutation, WriteReceipt,
};
use zippy_types::query::{QueryInstanceId, QueryPlanInfo, SendableRecordBatchStream};
use zippy_types::table::{CatalogName, TableRef};
use zippy_types::{BoxFuture, ErrorCode, Result, ServerError, SessionId};

use crate::{NativePreparedQuery, NativeQueryBinding, NativeQueryEngine};

const FLIGHT_SQL_CATALOG_NAME: &str = "zippy";
const FLIGHT_SQL_TABLE_TYPE: &str = "TABLE";
const MAX_METADATA_PREPARATION_ATTEMPTS: usize = 3;

#[derive(Debug)]
enum MetadataPreparationError {
    CatalogChanged { expected: u64, actual: u64 },
    Server(ServerError),
}

impl From<ServerError> for MetadataPreparationError {
    fn from(error: ServerError) -> Self {
        Self::Server(error)
    }
}

impl MetadataPreparationError {
    fn into_server_error(self) -> ServerError {
        match self {
            Self::CatalogChanged { expected, actual } => ServerError::new(
                ErrorCode::Unavailable,
                format!(
                    "catalog changed while preparing Flight SQL metadata expected_revision=[{expected}] actual_revision=[{actual}]"
                ),
            )
            .with_retryable(true),
            Self::Server(error) => error,
        }
    }
}

async fn prepare_metadata_with_catalog_retry<T, F, P>(mut prepare: F) -> Result<T>
where
    F: FnMut() -> P,
    P: Future<Output = std::result::Result<T, MetadataPreparationError>>,
{
    for attempt in 1..MAX_METADATA_PREPARATION_ATTEMPTS {
        match prepare().await {
            Err(MetadataPreparationError::CatalogChanged { expected, actual }) => {
                tracing::debug!(
                    attempt,
                    expected_revision = expected,
                    actual_revision = actual,
                    "retrying Flight SQL metadata after catalog change"
                );
                // Abandon the whole inconsistent capture before admitting the next one.
                tokio::task::yield_now().await;
            }
            outcome => return outcome.map_err(MetadataPreparationError::into_server_error),
        }
    }
    prepare()
        .await
        .map_err(MetadataPreparationError::into_server_error)
}

/// Protocol adapter that sends Flight requests directly through the native
/// snapshot planner and execution authority.
#[derive(Clone)]
pub struct NativeFlightQueryFrontend {
    engine: NativeQueryEngine,
    fixed_snapshots: Arc<crate::fixed_snapshot::FixedSnapshotStore>,
    catalog: SqliteCatalogReadHandle,
    native_ingest: Option<NativeIngestHandle>,
    table_admin: Option<crate::NativeTableAdmin>,
    consumer_logs: Option<crate::NativeConsumerLogService>,
}

impl NativeFlightQueryFrontend {
    /// Creates a Flight frontend over one server-owned native query engine.
    #[must_use]
    pub fn new(engine: NativeQueryEngine, catalog: SqliteCatalogReadHandle) -> Self {
        Self {
            engine,
            fixed_snapshots: Arc::new(crate::fixed_snapshot::FixedSnapshotStore::new()),
            catalog,
            native_ingest: None,
            table_admin: None,
            consumer_logs: None,
        }
    }

    /// Installs the server-owned producer-fenced native append authority.
    #[must_use]
    pub fn with_native_ingest(mut self, native_ingest: NativeIngestHandle) -> Self {
        self.native_ingest = Some(native_ingest);
        self
    }

    /// Installs the server-owned online table-management authority.
    #[must_use]
    pub fn with_table_admin(mut self, admin: crate::NativeTableAdmin) -> Self {
        self.table_admin = Some(admin);
        self
    }

    /// Installs the runtime's shared durable consumer and group authority.
    #[must_use]
    pub fn with_consumer_logs(mut self, service: crate::NativeConsumerLogService) -> Self {
        self.consumer_logs = Some(service);
        self
    }

    fn table_admin(&self) -> Result<&crate::NativeTableAdmin> {
        self.table_admin.as_ref().ok_or_else(|| {
            ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native table management is not configured",
            )
        })
    }

    async fn prepare_metadata_inner(
        &self,
        request: FlightSqlMetadataRequest,
        limits: FlightSqlMetadataLimits,
    ) -> std::result::Result<PreparedFlightSqlMetadata, MetadataPreparationError> {
        match request {
            FlightSqlMetadataRequest::Catalogs(command) => {
                let revision = self.catalog.revision().await?;
                let mut builder = command.into_builder();
                builder.append(FLIGHT_SQL_CATALOG_NAME);
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits).map_err(Into::into)
            }
            FlightSqlMetadataRequest::DbSchemas(command) => {
                let namespaces = self.list_namespaces(limits).await?;
                let revision = namespaces.catalog_revision;
                let mut builder = command.into_builder();
                for namespace in namespaces.namespaces {
                    builder.append(FLIGHT_SQL_CATALOG_NAME, namespace.name.as_str());
                }
                self.ensure_revision(revision).await?;
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits).map_err(Into::into)
            }
            FlightSqlMetadataRequest::Tables(command) => {
                let include_schema = command.include_schema;
                let namespaces = self.list_namespaces(limits).await?;
                let tables = self
                    .catalog
                    .list_tables(ListTablesRequest {
                        namespace_id: None,
                        after: None,
                        limit: metadata_page_limit(limits)?,
                    })
                    .await?;
                if tables.next_after.is_some() {
                    return Err(metadata_rows_exhausted(limits.max_rows()).into());
                }
                if tables.catalog_revision != namespaces.catalog_revision {
                    return Err(catalog_changed(
                        namespaces.catalog_revision,
                        tables.catalog_revision,
                    ));
                }
                let revision = tables.catalog_revision;
                let schema_snapshot = if include_schema && !tables.tables.is_empty() {
                    let selectors = tables
                        .tables
                        .iter()
                        .map(|table| {
                            SqliteCatalogSchemaSelector::try_new(
                                table.table_id,
                                table.current_schema_version,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let snapshot = self
                        .catalog
                        .read_schemas_at_with_limits(
                            &selectors,
                            SqliteCatalogSchemaReadLimits {
                                max_raw_response_bytes: limits.max_bytes(),
                            },
                        )
                        .await?;
                    if snapshot.catalog_revision() != revision {
                        return Err(catalog_changed(revision, snapshot.catalog_revision()));
                    }
                    Some(snapshot)
                } else {
                    None
                };
                self.ensure_revision(revision).await?;
                let namespace_names = namespaces
                    .namespaces
                    .into_iter()
                    .map(|namespace| (namespace.namespace_id, namespace.name))
                    .collect::<HashMap<_, _>>();
                let schemas = schema_snapshot
                    .map(|snapshot| {
                        snapshot
                            .into_schemas()
                            .into_iter()
                            .map(|schema| {
                                let physical = decode_persisted_schema(&schema)?;
                                let logical = zippy_types::key_value::decode_log_schema(&physical)?
                                    .map_or(physical, |decoded| decoded.logical);
                                Ok((schema.table_id, logical))
                            })
                            .collect::<Result<HashMap<_, _>>>()
                    })
                    .transpose()?
                    .unwrap_or_default();
                let mut builder = command.into_builder();
                let empty_schema = Schema::empty();
                let mut input_bytes = 0_usize;
                for table in tables.tables {
                    let namespace = namespace_names.get(&table.namespace_id).ok_or_else(|| {
                        internal("Flight SQL table references an unknown Catalog namespace")
                    })?;
                    let schema = schemas
                        .get(&table.table_id)
                        .map_or(&empty_schema, std::convert::AsRef::as_ref);
                    input_bytes = charge_metadata_input(
                        input_bytes,
                        namespace.as_str().len(),
                        limits.max_bytes(),
                    )?;
                    input_bytes = charge_metadata_input(
                        input_bytes,
                        table.name.as_str().len(),
                        limits.max_bytes(),
                    )?;
                    if include_schema {
                        input_bytes = charge_metadata_input(
                            input_bytes,
                            schema.fields().len().saturating_mul(64),
                            limits.max_bytes(),
                        )?;
                    }
                    builder
                        .append(
                            FLIGHT_SQL_CATALOG_NAME,
                            namespace.as_str(),
                            table.name.as_str(),
                            FLIGHT_SQL_TABLE_TYPE,
                            schema,
                        )
                        .map_err(metadata_build_error)?;
                }
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits).map_err(Into::into)
            }
            FlightSqlMetadataRequest::TableTypes(command) => {
                let revision = self.catalog.revision().await?;
                let mut builder = command.into_builder();
                builder.append(FLIGHT_SQL_TABLE_TYPE);
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits).map_err(Into::into)
            }
        }
    }

    async fn list_namespaces(
        &self,
        limits: FlightSqlMetadataLimits,
    ) -> Result<zippy_types::catalog::ListNamespacesResponse> {
        let namespaces = self
            .catalog
            .list_namespaces(ListNamespacesRequest {
                after: None,
                limit: metadata_page_limit(limits)?,
            })
            .await?;
        if namespaces.next_after.is_some() {
            return Err(metadata_rows_exhausted(limits.max_rows()));
        }
        Ok(namespaces)
    }

    async fn ensure_revision(
        &self,
        expected: u64,
    ) -> std::result::Result<(), MetadataPreparationError> {
        let actual = self.catalog.revision().await?;
        if actual != expected {
            return Err(catalog_changed(expected, actual));
        }
        Ok(())
    }
}

impl FlightQueryFrontend for NativeFlightQueryFrontend {
    fn supports_durable_log_subscription(&self) -> bool {
        self.consumer_logs.is_some()
    }

    fn supports_log_subscription(&self) -> bool {
        true
    }

    fn open_log(
        &self,
        request: zippy_protocol::log::FlightLogRequest,
    ) -> BoxFuture<'_, Result<zippy_protocol::log::FlightLogSubscription>> {
        use futures::StreamExt;
        Box::pin(async move {
            let owner = self.engine.owner(request.session_id);
            if let Some(consumer) = request.consumer {
                let service = self.consumer_logs.as_ref().ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::UnsupportedPlan,
                        "native durable log service is not configured",
                    )
                })?;
                let table = self.catalog.get_table(request.table).await?;
                let log = service
                    .open(
                        owner.as_ref(),
                        zippy_types::ConnectionId::from_bytes(request.query_id.into_bytes())
                            .expect("query identity is non-nil"),
                        zippy_types::catalog::ReadConsumerOffsetRequest {
                            table_id: table.table_id,
                            consumer_id: consumer.consumer_id,
                            offset_authority: consumer.offset_authority,
                        },
                        crate::NativeLogRequest {
                            query_id: request.query_id,
                            table: TableRef::Id {
                                table_id: table.table_id,
                            },
                            start: None,
                            deadline_ns: request.deadline_ns,
                            memory_limit_bytes: request.memory_limit_bytes,
                            batch_rows: request.batch_rows,
                        },
                    )
                    .await?;
                return Ok(log.into_flight());
            }
            let subscription = self
                .engine
                .subscribe_log(
                    owner.as_ref(),
                    crate::NativeLogRequest {
                        query_id: request.query_id,
                        table: request.table,
                        start: request.start,
                        deadline_ns: request.deadline_ns,
                        memory_limit_bytes: request.memory_limit_bytes,
                        batch_rows: request.batch_rows,
                    },
                )
                .await?;
            Ok(zippy_protocol::log::FlightLogSubscription {
                instance_id: subscription.instance_id(),
                table_id: subscription.table_id(),
                kind: subscription.kind(),
                schema: subscription.schema(),
                watermark: subscription.watermark(),
                consumer: None,
                next_offset: None,
                acknowledger: None,
                stream: subscription
                    .into_stream()
                    .map(|result| {
                        result.map(|batch| zippy_protocol::log::FlightLogBatch {
                            batch: batch.batch,
                            offsets: batch.offsets,
                            frame: batch.frame,
                            watermark: batch.watermark,
                            acknowledgement: None,
                        })
                    })
                    .boxed(),
            })
        })
    }

    fn supports_key_value_snapshot(&self) -> bool {
        true
    }
    fn supports_fixed_snapshots(&self) -> bool {
        true
    }

    fn create_fixed_snapshot(
        &self,
        request: zippy_protocol::fixed_snapshot::FlightFixedSnapshotRequest,
    ) -> BoxFuture<'_, Result<zippy_protocol::wire::FixedSnapshotInfo>> {
        Box::pin(async move { self.fixed_snapshots.create(&self.engine, request).await })
    }

    fn release_fixed_snapshot(
        &self,
        session: SessionId,
        snapshot: zippy_types::QueryId,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move { self.fixed_snapshots.release(session, snapshot) })
    }

    fn active_fixed_snapshots(&self) -> usize {
        self.fixed_snapshots.len()
    }

    fn sweep_fixed_snapshots(&self, now_ns: u64, expired_sessions: &[SessionId]) -> usize {
        self.fixed_snapshots.sweep(now_ns, expired_sessions)
    }

    fn prepare(
        &self,
        request: FlightQueryRequest,
    ) -> BoxFuture<'_, Result<OpaquePreparedFlightQuery>> {
        Box::pin(async move {
            let bindings = request
                .bindings
                .into_iter()
                .map(|binding| NativeQueryBinding::try_new(binding.sql_name, binding.table))
                .collect::<Result<Vec<_>>>()?;
            let owner = self.engine.owner(request.request.session_id);
            let prepared = if let Some(snapshot_id) = request.fixed_snapshot_id {
                if request.key_value_snapshot {
                    return Err(ServerError::new(
                        ErrorCode::InvalidArgument,
                        "atomic key-value cursor snapshots cannot use a fixed query snapshot",
                    ));
                }
                let lease = self
                    .fixed_snapshots
                    .get(request.request.session_id, snapshot_id)?;
                self.engine
                    .prepare_fixed(&lease.snapshot, owner.as_ref(), request.request, bindings)
                    .await?
            } else if request.key_value_snapshot {
                self.engine
                    .prepare_key_value_snapshot(owner.as_ref(), request.request, bindings)
                    .await?
            } else {
                self.engine
                    .prepare_bound(owner.as_ref(), request.request, bindings)
                    .await?
            };
            Ok(Box::new(prepared) as OpaquePreparedFlightQuery)
        })
    }

    fn cancel_owned(
        &self,
        session_id: SessionId,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.engine
                .cancel_authenticated_session(session_id, instance_id)
        })
    }

    fn cancel_administrator(&self, instance_id: QueryInstanceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.engine.cancel_administrator(instance_id) })
    }

    fn prepare_metadata(
        &self,
        request: FlightSqlMetadataRequest,
        limits: FlightSqlMetadataLimits,
    ) -> BoxFuture<'_, Result<PreparedFlightSqlMetadata>> {
        Box::pin(prepare_metadata_with_catalog_retry(move || {
            self.prepare_metadata_inner(request.clone(), limits)
        }))
    }

    fn frame_row_capacity(&self) -> Option<u64> {
        self.native_ingest
            .as_ref()
            .and_then(NativeIngestHandle::frame_row_capacity)
            .and_then(|rows| u64::try_from(rows).ok())
    }

    fn supports_persisted_ingest(&self) -> bool {
        self.native_ingest
            .as_ref()
            .is_some_and(NativeIngestHandle::supports_persisted)
    }

    fn supports_ingest_resume(&self) -> bool {
        self.native_ingest.is_some()
    }

    fn supports_ingest_table_identity(&self) -> bool {
        self.native_ingest.is_some()
    }

    fn supports_key_value_ingest(&self) -> bool {
        self.native_ingest.is_some()
    }

    fn create_namespace(
        &self,
        request_id: zippy_types::RequestId,
        command: zippy_types::catalog::CreateNamespaceCommand,
    ) -> BoxFuture<'_, Result<zippy_types::catalog::VersionedCatalogResponse>> {
        Box::pin(async move {
            self.table_admin()?
                .create_namespace(request_id, command)
                .await
        })
    }

    fn create_table(
        &self,
        request_id: zippy_types::RequestId,
        command: zippy_types::catalog::CreateTableCommand,
    ) -> BoxFuture<'_, Result<zippy_types::catalog::VersionedCatalogResponse>> {
        Box::pin(async move { self.table_admin()?.create_table(request_id, command).await })
    }

    fn drop_table(
        &self,
        request_id: zippy_types::RequestId,
        command: zippy_types::catalog::DropTableCommand,
    ) -> BoxFuture<'_, Result<zippy_types::catalog::VersionedCatalogResponse>> {
        Box::pin(async move { self.table_admin()?.drop_table(request_id, command).await })
    }

    fn alter_table_schema(
        &self,
        request_id: zippy_types::RequestId,
        command: zippy_types::catalog::AlterTableSchemaCommand,
        key_value: Option<zippy_types::table::KeyValueTableDefinition>,
    ) -> BoxFuture<'_, Result<zippy_types::catalog::VersionedCatalogResponse>> {
        Box::pin(async move {
            self.table_admin()?
                .alter_table_schema(request_id, command, key_value)
                .await
        })
    }

    fn open_ingest_writer(
        &self,
        command: Arc<FlightPutCommand>,
    ) -> BoxFuture<'_, Result<OpaqueFlightIngestWriter>> {
        let Some(native_ingest) = self.native_ingest.clone() else {
            return Box::pin(async move {
                Err(ServerError::new(
                    ErrorCode::UnsupportedPlan,
                    "Flight native ingest authority is not configured",
                )
                .with_request_id(command.put_id)
                .with_retryable(false))
            });
        };
        Box::pin(async move {
            let table = self
                .catalog
                .get_table(TableRef::Name {
                    namespace: CatalogName::new(command.namespace.clone()).map_err(|error| {
                        ServerError::new(
                            ErrorCode::InvalidArgument,
                            format!("invalid Flight ingest namespace error=[{error}]"),
                        )
                    })?,
                    table: CatalogName::new(command.stream_name.clone()).map_err(|error| {
                        ServerError::new(
                            ErrorCode::InvalidArgument,
                            format!("invalid Flight ingest table name error=[{error}]"),
                        )
                        .with_request_id(command.put_id)
                        .with_retryable(false)
                    })?,
                })
                .await
                .map_err(|error| error.with_request_id(command.put_id))?;
            if command
                .expected_table_id
                .is_some_and(|expected| expected != table.table_id)
            {
                return Err(ServerError::new(
                    ErrorCode::Conflict,
                    "Flight output table identity changed before producer acquisition",
                )
                .with_request_id(command.put_id)
                .with_table_id(table.table_id)
                .with_retryable(false));
            }
            let expected_kind = match command.mutation {
                zippy_protocol::wire::PutMutation::Append => zippy_types::table::TableKind::Append,
                zippy_protocol::wire::PutMutation::Frame => zippy_types::table::TableKind::Frame,
                zippy_protocol::wire::PutMutation::KeyValueUpsert
                | zippy_protocol::wire::PutMutation::KeyValueDelete => {
                    zippy_types::table::TableKind::KeyValue
                }
                _ => {
                    return Err(ServerError::new(
                        ErrorCode::UnsupportedPlan,
                        "unsupported native mutation",
                    ))
                }
            };
            if table.definition.kind() != expected_kind {
                return Err(ServerError::new(
                    ErrorCode::InvalidArgument,
                    "Flight mutation does not match the Catalog table kind",
                ));
            }
            let writer = native_ingest
                .open_writer(OpenWriterRequest {
                    request_id: command.put_id,
                    table_id: table.table_id,
                    producer_id: command.producer_id,
                    connection_id: command.connection_id,
                    expected_writer_epoch: command.expected_writer_epoch,
                })
                .await?;
            Ok(Box::new(NativeFlightIngestWriter {
                command,
                writer: Some(writer),
                next_batch_index: 0,
            }) as OpaqueFlightIngestWriter)
        })
    }
}

enum NativeFlightWriteSubmission {
    Ready(WriteReceipt),
    Durable(OpaqueDurableWriteAck),
}

struct NativeFlightIngestWriter {
    command: Arc<FlightPutCommand>,
    writer: Option<OpaqueIngestWriter>,
    next_batch_index: u64,
}

impl FlightIngestWriter for NativeFlightIngestWriter {
    fn write_batch(
        &mut self,
        batch: FlightPutBatch,
    ) -> BoxFuture<'_, Result<zippy_protocol::wire::PutBatchResult>> {
        Box::pin(async move {
            if batch.command.put_id != self.command.put_id {
                return Err(ServerError::new(
                    ErrorCode::Internal,
                    "Flight ingest writer received a batch from another put",
                )
                .with_request_id(batch.command.put_id)
                .with_retryable(false));
            }
            let batch_index = self.next_batch_index;
            let request_id = put_batch_request_id(self.command.put_id, batch_index);
            let mut writer = self.writer.take().ok_or_else(|| {
                ServerError::new(
                    ErrorCode::Unavailable,
                    "Flight native ingest writer is no longer available",
                )
                .with_request_id(request_id)
                .with_retryable(false)
            })?;
            let producer_epoch = writer.info().producer_epoch;
            let schema_version = writer.info().schema_version;
            let durability = self.command.durability;
            let joined = tokio::task::spawn_blocking(move || {
                let result = ArrowIpcMutationFingerprintComputer.prepare(IncomingWriteRequest {
                    request_id,
                    schema_version,
                    batch_sequence: batch.batch_sequence,
                    mutation: match batch.command.mutation {
                        zippy_protocol::wire::PutMutation::Append => {
                            TableMutation::Append(batch.batch)
                        }
                        zippy_protocol::wire::PutMutation::Frame => {
                            TableMutation::Frame(batch.batch)
                        }
                        zippy_protocol::wire::PutMutation::KeyValueUpsert => {
                            TableMutation::KeyValue(zippy_types::ingest::KeyValueMutation::Upsert(
                                batch.batch,
                            ))
                        }
                        zippy_protocol::wire::PutMutation::KeyValueDelete => {
                            TableMutation::KeyValue(zippy_types::ingest::KeyValueMutation::Delete(
                                batch.batch,
                            ))
                        }
                        _ => {
                            return (
                                writer,
                                Err(ServerError::new(
                                    ErrorCode::UnsupportedPlan,
                                    "unsupported native mutation",
                                )),
                            )
                        }
                    },
                });
                let submission = result.and_then(|request| match durability {
                    Durability::Memory => writer
                        .write_memory(request)
                        .map(NativeFlightWriteSubmission::Ready),
                    Durability::Wal => writer
                        .submit_durable(request, DurableWriteTarget::Wal)
                        .map(NativeFlightWriteSubmission::Durable),
                    Durability::Persisted => writer
                        .submit_durable(request, DurableWriteTarget::Persisted)
                        .map(NativeFlightWriteSubmission::Durable),
                });
                (writer, submission)
            })
            .await
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("join Flight native ingest writer failed error=[{error}]"),
                )
                .with_request_id(request_id)
                .with_retryable(false)
            })?;
            self.writer = Some(joined.0);
            let receipt = match joined.1? {
                NativeFlightWriteSubmission::Ready(receipt) => receipt,
                NativeFlightWriteSubmission::Durable(acknowledgement) => {
                    acknowledgement.wait().await?
                }
            };
            self.next_batch_index = self.next_batch_index.checked_add(1).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "Flight native ingest batch index exhausted u64",
                )
                .with_request_id(request_id)
                .with_retryable(false)
            })?;
            Ok(put_batch_result_from_receipt(
                &self.command,
                batch.batch_sequence,
                producer_epoch,
                &receipt,
            ))
        })
    }
}

impl PreparedFlightQuery for NativePreparedQuery {
    fn info(&self) -> &QueryPlanInfo {
        self.info()
    }

    fn instance_id(&self) -> QueryInstanceId {
        self.instance_id()
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema()
    }

    fn execute(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        (*self).into_stream()
    }
}

fn metadata_page_limit(limits: FlightSqlMetadataLimits) -> Result<NonZeroU32> {
    let rows = u32::try_from(limits.max_rows())
        .map_err(|_| internal("Flight SQL metadata row limit exceeds u32"))?;
    NonZeroU32::new(rows).ok_or_else(|| internal("Flight SQL metadata row limit is zero"))
}

fn charge_metadata_input(current: usize, additional: usize, maximum: usize) -> Result<usize> {
    let actual = current.checked_add(additional).ok_or_else(|| {
        ServerError::new(
            ErrorCode::ResourceExhausted,
            format!("Flight SQL metadata input byte accounting overflow maximum=[{maximum}]"),
        )
        .with_retryable(false)
    })?;
    if actual > maximum {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "Flight SQL metadata input exceeds byte budget actual=[{actual}] maximum=[{maximum}]"
            ),
        )
        .with_retryable(false));
    }
    Ok(actual)
}

fn metadata_rows_exhausted(maximum: usize) -> ServerError {
    ServerError::new(
        ErrorCode::ResourceExhausted,
        format!("Flight SQL metadata exceeds row budget maximum=[{maximum}]"),
    )
    .with_retryable(false)
}

fn catalog_changed(expected: u64, actual: u64) -> MetadataPreparationError {
    MetadataPreparationError::CatalogChanged { expected, actual }
}

fn metadata_build_error(error: impl std::fmt::Display) -> ServerError {
    internal(format!(
        "build Flight SQL metadata batch failed error=[{error}]"
    ))
}

fn internal(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use super::*;

    #[tokio::test]
    async fn metadata_revision_conflict_restarts_capture_and_returns_only_stable_result() {
        let mut captures = 0;
        let revision = prepare_metadata_with_catalog_retry(|| {
            captures += 1;
            ready(if captures == 1 {
                Err(catalog_changed(10, 11))
            } else {
                Ok(11)
            })
        })
        .await
        .unwrap();
        assert_eq!(revision, 11);
        assert_eq!(captures, 2);
    }

    #[tokio::test]
    async fn repeated_metadata_revision_changes_stop_at_budget_with_last_conflict() {
        let mut captures = 0;
        let error = prepare_metadata_with_catalog_retry::<(), _, _>(|| {
            captures += 1;
            ready(Err(catalog_changed(captures, captures + 1)))
        })
        .await
        .unwrap_err();
        assert_eq!(captures, MAX_METADATA_PREPARATION_ATTEMPTS as u64);
        assert_eq!(error.code(), ErrorCode::Unavailable);
        assert!(error.retryable());
        assert!(error
            .to_string()
            .contains("expected_revision=[3] actual_revision=[4]"));
    }

    #[tokio::test]
    async fn metadata_retry_preserves_resource_corruption_and_unavailable_errors() {
        for code in [
            ErrorCode::ResourceExhausted,
            ErrorCode::DataCorruption,
            ErrorCode::Unavailable,
        ] {
            let mut captures = 0;
            let error = prepare_metadata_with_catalog_retry::<(), _, _>(|| {
                captures += 1;
                ready(Err(ServerError::new(code, "metadata read failed")
                    .with_retryable(true)
                    .into()))
            })
            .await
            .unwrap_err();
            assert_eq!(captures, 1);
            assert_eq!(error.code(), code);
            assert!(error.retryable());
            assert!(error.to_string().contains("metadata read failed"));
        }
    }
}
