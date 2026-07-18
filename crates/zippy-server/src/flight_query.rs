//! Arrow Flight adapter for the server-owned native query authority.

use std::collections::HashMap;
use std::num::NonZeroU32;

use arrow::datatypes::Schema;
use zippy_catalog::{
    decode_persisted_schema, SqliteCatalogReadHandle, SqliteCatalogSchemaReadLimits,
    SqliteCatalogSchemaSelector,
};
use zippy_protocol::query::FlightQueryRequest;
use zippy_protocol::server::{FlightQueryFrontend, OpaquePreparedFlightQuery, PreparedFlightQuery};
use zippy_protocol::sql::{
    FlightSqlMetadataLimits, FlightSqlMetadataRequest, PreparedFlightSqlMetadata,
};
use zippy_types::catalog::{CatalogRead, ListNamespacesRequest, ListTablesRequest};
use zippy_types::query::{QueryEngine, QueryInstanceId, QueryPlanInfo, SendableRecordBatchStream};
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
}

impl NativeFlightQueryFrontend {
    /// Creates a Flight frontend over one server-owned native query engine.
    #[must_use]
    pub const fn new(engine: NativeQueryEngine, catalog: SqliteCatalogReadHandle) -> Self {
        Self { engine, catalog }
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
