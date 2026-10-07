//! Bounded, explicit Flight table-management commands. No internal Catalog RPC passthrough.

use std::collections::BTreeMap;

use bytes::Bytes;
use prost::Message;
use zippy_types::catalog::{
    AlterTableSchemaCommand, CatalogOutcomeV1, CreateNamespaceCommand, CreateTableCommand,
    DropTableCommand, VersionedCatalogResponse,
};
use zippy_types::table::{
    AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FrameTableDefinition,
    KeyValueTableDefinition, SchemaIpcPayload, TableDefinition,
};
use zippy_types::{ErrorCode, NamespaceId, RequestId, Result, ServerError, TableId};

use crate::server::FlightQueryFrontend;
use crate::wire::{
    AlterTableSchemaRequest, CatalogMutationResult, CreateNamespaceRequest, CreateTableRequest,
    DropTableRequest, KeyValueTableOptions,
};
use crate::CURRENT_PROTOCOL_VERSION;

/// Administrator action for an idempotent namespace creation.
pub const CREATE_NAMESPACE_ACTION: &str = "zippy.create_namespace.v1";
/// Administrator action for an idempotent native table creation.
pub const CREATE_TABLE_ACTION: &str = "zippy.create_table.v1";
/// Administrator action for an idempotent native table deletion.
pub const DROP_TABLE_ACTION: &str = "zippy.drop_table.v1";
/// Administrator action for native schema evolution with expected-version fencing.
pub const ALTER_TABLE_SCHEMA_ACTION: &str = "zippy.alter_table_schema.v1";
/// Capability for runtime-coordinated schema evolution.
pub const TABLE_SCHEMA_CAPABILITY: &str = "catalog.schema.v1";
/// Capability for runtime-coordinated logical table deletion.
pub const TABLE_DROP_CAPABILITY: &str = "catalog.drop.v1";
/// Capability advertised only when authenticated online management is installed.
pub const TABLE_MANAGEMENT_CAPABILITY: &str = "catalog.create.v1";

pub(crate) async fn execute(
    frontend: &dyn FlightQueryFrontend,
    action: &str,
    body: Bytes,
) -> Result<CatalogMutationResult> {
    let response = if action == CREATE_NAMESPACE_ACTION {
        let request = CreateNamespaceRequest::decode(body).map_err(invalid)?;
        validate_version(request.protocol_version)?;
        let request_id = decode_id(&request.request_id, RequestId::from_bytes)?;
        frontend
            .create_namespace(
                request_id,
                CreateNamespaceCommand {
                    namespace_id: decode_id(&request.namespace_id, NamespaceId::from_bytes)?,
                    name: CatalogName::new(request.name).map_err(invalid)?,
                },
            )
            .await?
    } else if action == ALTER_TABLE_SCHEMA_ACTION {
        let request = AlterTableSchemaRequest::decode(body).map_err(invalid)?;
        validate_version(request.protocol_version)?;
        if request.expected_schema_version == 0 {
            return Err(invalid(
                "schema changes require a nonzero expected schema version",
            ));
        }
        let key_value = request.key_value.map(key_value_definition).transpose()?;
        frontend
            .alter_table_schema(
                decode_id(&request.request_id, RequestId::from_bytes)?,
                AlterTableSchemaCommand {
                    table_id: decode_id(&request.table_id, TableId::from_bytes)?,
                    expected_schema_version: request.expected_schema_version,
                    schema_ipc: SchemaIpcPayload::new(request.schema_ipc.to_vec())
                        .map_err(invalid)?,
                },
                key_value,
            )
            .await?
    } else if action == DROP_TABLE_ACTION {
        let request = DropTableRequest::decode(body).map_err(invalid)?;
        validate_version(request.protocol_version)?;
        if request.expected_snapshot_version == 0 {
            return Err(invalid(
                "table deletion requires a nonzero expected snapshot version",
            ));
        }
        frontend
            .drop_table(
                decode_id(&request.request_id, RequestId::from_bytes)?,
                DropTableCommand {
                    table_id: decode_id(&request.table_id, TableId::from_bytes)?,
                    expected_snapshot_version: request.expected_snapshot_version,
                },
            )
            .await?
    } else if action == CREATE_TABLE_ACTION {
        let request = CreateTableRequest::decode(body).map_err(invalid)?;
        validate_version(request.protocol_version)?;
        let request_id = decode_id(&request.request_id, RequestId::from_bytes)?;
        frontend
            .create_table(
                request_id,
                CreateTableCommand {
                    table_id: decode_id(&request.table_id, TableId::from_bytes)?,
                    namespace_id: decode_id(&request.namespace_id, NamespaceId::from_bytes)?,
                    name: CatalogName::new(request.name).map_err(invalid)?,
                    definition: match request.definition {
                        Some(crate::wire::create_table_request::Definition::Append(options)) => {
                            TableDefinition::Append(AppendTableDefinition {
                                event_time_column: options
                                    .event_time_column
                                    .map(ColumnName::new)
                                    .transpose()
                                    .map_err(invalid)?,
                            })
                        }
                        Some(crate::wire::create_table_request::Definition::Frame(options)) => {
                            TableDefinition::Frame(FrameTableDefinition {
                                by: ColumnName::new(options.by).map_err(invalid)?,
                            })
                        }
                        Some(crate::wire::create_table_request::Definition::KeyValue(options)) => {
                            TableDefinition::KeyValue(key_value_definition(options)?)
                        }
                        None => return Err(invalid("table definition must be explicit")),
                    },
                    schema_ipc: SchemaIpcPayload::new(request.schema_ipc.to_vec())
                        .map_err(invalid)?,
                    config: BTreeMap::new(),
                },
            )
            .await?
    } else {
        return Err(invalid("unknown table management action"));
    };
    let VersionedCatalogResponse::V1(response) = response;
    let (object_id, schema_version) = match response.outcome {
        CatalogOutcomeV1::Namespace(namespace) => (namespace.namespace_id.into_bytes(), 0),
        CatalogOutcomeV1::Table(table) => {
            (table.table_id.into_bytes(), table.current_schema_version)
        }
        CatalogOutcomeV1::Schema(schema) => (schema.table_id.into_bytes(), schema.schema_version),
        CatalogOutcomeV1::TableDropped { table_id } => (table_id.into_bytes(), 0),
        _ => {
            return Err(ServerError::new(
                ErrorCode::Internal,
                "table management returned an unexpected Catalog outcome",
            ))
        }
    };
    Ok(CatalogMutationResult {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        request_id: Bytes::copy_from_slice(&response.request_id.into_bytes()),
        catalog_revision: response.catalog_revision,
        object_id: Bytes::copy_from_slice(&object_id),
        schema_version,
    })
}

fn decode_id<T, E: std::fmt::Display>(
    value: &[u8],
    from_bytes: impl FnOnce([u8; 16]) -> std::result::Result<T, E>,
) -> Result<T> {
    from_bytes(value.try_into().map_err(invalid)?).map_err(invalid)
}

fn key_value_definition(options: KeyValueTableOptions) -> Result<KeyValueTableDefinition> {
    let keys = options
        .keys
        .into_iter()
        .map(ColumnName::new)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(invalid)?;
    KeyValueTableDefinition::new(ColumnSet::new(keys).map_err(invalid)?).map_err(invalid)
}

fn validate_version(version: u32) -> Result<()> {
    if version != CURRENT_PROTOCOL_VERSION {
        return Err(invalid("unsupported table management protocol version"));
    }
    Ok(())
}

fn invalid(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::InvalidArgument,
        format!("invalid table management request error=[{error}]"),
    )
    .with_retryable(false)
}
