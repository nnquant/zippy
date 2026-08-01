//! Arrow Flight adapter for the server-owned native query authority.

use std::collections::HashMap;
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
use zippy_types::query::{QueryEngine, QueryInstanceId, QueryPlanInfo, SendableRecordBatchStream};
use zippy_types::table::{CatalogName, TableRef};
use zippy_types::{BoxFuture, ErrorCode, Result, ServerError, SessionId};

use crate::{NativePreparedQuery, NativeQueryBinding, NativeQueryEngine};

const FLIGHT_SQL_CATALOG_NAME: &str = "zippy";
const FLIGHT_SQL_TABLE_TYPE: &str = "TABLE";

/// Protocol adapter that sends Flight requests directly through the native
/// snapshot planner and execution authority.
#[derive(Clone)]
pub struct NativeFlightQueryFrontend {
    engine: NativeQueryEngine,
    catalog: SqliteCatalogReadHandle,
    native_ingest: Option<NativeIngestHandle>,
}

impl NativeFlightQueryFrontend {
    /// Creates a Flight frontend over one server-owned native query engine.
    #[must_use]
    pub const fn new(engine: NativeQueryEngine, catalog: SqliteCatalogReadHandle) -> Self {
        Self {
            engine,
            catalog,
            native_ingest: None,
        }
    }

    /// Installs the server-owned producer-fenced native append authority.
    #[must_use]
    pub fn with_native_ingest(mut self, native_ingest: NativeIngestHandle) -> Self {
        self.native_ingest = Some(native_ingest);
        self
    }

    async fn prepare_metadata_inner(
        &self,
        request: FlightSqlMetadataRequest,
        limits: FlightSqlMetadataLimits,
    ) -> Result<PreparedFlightSqlMetadata> {
        match request {
            FlightSqlMetadataRequest::Catalogs(command) => {
                let revision = self.catalog.revision().await?;
                let mut builder = command.into_builder();
                builder.append(FLIGHT_SQL_CATALOG_NAME);
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits)
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
                PreparedFlightSqlMetadata::try_new(revision, batch, limits)
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
                    return Err(metadata_rows_exhausted(limits.max_rows()));
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
                            .map(|schema| Ok((schema.table_id, decode_persisted_schema(&schema)?)))
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
                PreparedFlightSqlMetadata::try_new(revision, batch, limits)
            }
            FlightSqlMetadataRequest::TableTypes(command) => {
                let revision = self.catalog.revision().await?;
                let mut builder = command.into_builder();
                builder.append(FLIGHT_SQL_TABLE_TYPE);
                let batch = builder.build().map_err(metadata_build_error)?;
                PreparedFlightSqlMetadata::try_new(revision, batch, limits)
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

    async fn ensure_revision(&self, expected: u64) -> Result<()> {
        let actual = self.catalog.revision().await?;
        if actual != expected {
            return Err(catalog_changed(expected, actual));
        }
        Ok(())
    }
}

impl FlightQueryFrontend for NativeFlightQueryFrontend {
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
            let prepared = self
                .engine
                .prepare_bound(owner.as_ref(), request.request, bindings)
                .await?;
            Ok(Box::new(prepared) as OpaquePreparedFlightQuery)
        })
    }

    fn cancel_owned(
        &self,
        session_id: SessionId,
        instance_id: QueryInstanceId,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let owner = self.engine.owner(session_id);
            self.engine.cancel(owner.as_ref(), instance_id).await
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
        Box::pin(async move { self.prepare_metadata_inner(request, limits).await })
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
                    namespace: CatalogName::default_namespace(),
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
                    mutation: TableMutation::Append(batch.batch),
                });
                let submission = result.and_then(|request| match durability {
                    Durability::Memory => writer
                        .write_memory(request)
                        .map(NativeFlightWriteSubmission::Ready),
                    Durability::Wal => writer
                        .submit_durable(request, DurableWriteTarget::Wal)
                        .map(NativeFlightWriteSubmission::Durable),
                    Durability::Persisted => Err(ServerError::new(
                        ErrorCode::UnsupportedPlan,
                        "Flight persisted acknowledgement is not implemented",
                    )
                    .with_request_id(request_id)
                    .with_retryable(false)),
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

fn catalog_changed(expected: u64, actual: u64) -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        format!(
            "Catalog changed while preparing Flight SQL metadata expected_revision=[{expected}] actual_revision=[{actual}]"
        ),
    )
    .with_retryable(true)
}

fn metadata_build_error(error: impl std::fmt::Display) -> ServerError {
    internal(format!(
        "build Flight SQL metadata batch failed error=[{error}]"
    ))
}

fn internal(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::Internal, message).with_retryable(false)
}
