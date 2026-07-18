//! Versioned query descriptors, tickets, and plan metadata.

use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroU64};

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{FlightDescriptor, Ticket};
use bytes::Bytes;
use prost::Message;
use zippy_types::query::{QueryInput, QueryPlanInfo, QueryPriority, QueryRequest};
use zippy_types::table::{CatalogName, TableRef};
use zippy_types::{ErrorCode, QueryId, Result, ServerError, SessionId, TableId};

use crate::error::{resource_limit_from_wire, resource_limit_to_wire};
use crate::wire::table_binding::Table;
use crate::wire::{self, QueryCommand, QueryInfo, QueryTableVersion, QueryTicket, TableBinding};
use crate::CURRENT_PROTOCOL_VERSION;

/// Maximum serialized query descriptor accepted by protocol version 1.
pub const MAX_QUERY_COMMAND_BYTES: usize = 64 * 1024;
/// Maximum UTF-8 SQL text accepted by protocol version 1.
pub const MAX_QUERY_SQL_BYTES: usize = 16 * 1024;
/// Maximum explicit SQL table bindings accepted by protocol version 1.
pub const MAX_QUERY_BINDINGS: usize = 32;
/// Maximum SQL alias, namespace, or table-name byte length.
pub const MAX_QUERY_NAME_BYTES: usize = 512;
/// Fixed byte length of stable Zippy IDs.
pub const STABLE_ID_BYTES: usize = 16;
/// Random opaque server and ticket identity length.
pub const OPAQUE_TICKET_ID_BYTES: usize = 16;

/// One SQL-visible table binding decoded from a Flight command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightTableBinding {
    /// SQL relation name referenced by the submitted statement.
    pub sql_name: String,
    /// Catalog table resolved by name or immutable ID.
    pub table: TableRef,
}

/// One validated Flight query request ready for a server query frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightQueryRequest {
    /// Protocol-neutral query request.
    pub request: QueryRequest,
    /// Explicit relation-to-catalog bindings.
    pub bindings: Vec<FlightTableBinding>,
}

/// Encodes a protobuf query command as an Arrow Flight command descriptor.
#[must_use]
pub fn encode_query_descriptor(command: &QueryCommand) -> FlightDescriptor {
    FlightDescriptor::new_cmd(command.encode_to_vec())
}

/// Builds a canonical protocol command from one validated SQL query request.
pub fn encode_query_request(request: &FlightQueryRequest) -> Result<QueryCommand> {
    let query_id = request.request.query_id;
    let sql = match &request.request.input {
        QueryInput::Sql(sql) => sql.clone(),
        QueryInput::LogicalPlan(_) => {
            return Err(ServerError::new(
                ErrorCode::UnsupportedPlan,
                "Flight protocol version 1 only encodes SQL query input",
            )
            .with_query_id(request.request.query_id)
            .with_retryable(false));
        }
    };
    if sql.trim().is_empty() || sql.len() > MAX_QUERY_SQL_BYTES {
        return Err(
            invalid("query SQL is empty or exceeds the protocol limit").with_query_id(query_id)
        );
    }
    if request.bindings.is_empty() || request.bindings.len() > MAX_QUERY_BINDINGS {
        return Err(
            invalid("query requires a bounded non-empty binding set").with_query_id(query_id)
        );
    }
    if request.request.tables.len() != request.bindings.len()
        || request
            .request
            .tables
            .iter()
            .zip(&request.bindings)
            .any(|(table, binding)| table != &binding.table)
    {
        return Err(
            invalid("query tables must exactly match ordered Flight bindings")
                .with_query_id(query_id),
        );
    }
    let mut binding_names = HashSet::with_capacity(request.bindings.len());
    for binding in &request.bindings {
        require_name(&binding.sql_name, "binding sql_name")
            .map_err(|error| error.with_query_id(query_id))?;
        if !binding_names.insert(binding.sql_name.as_str()) {
            return Err(invalid(format!(
                "query binding name is duplicated name=[{}]",
                binding.sql_name
            ))
            .with_query_id(query_id));
        }
    }
    Ok(QueryCommand {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        query_id: Bytes::copy_from_slice(&request.request.query_id.into_bytes()),
        session_id: Bytes::copy_from_slice(&request.request.session_id.into_bytes()),
        sql,
        bindings: request.bindings.iter().map(binding_to_wire).collect(),
        priority: priority_to_wire(request.request.priority) as i32,
        deadline_ns: request.request.deadline_ns,
        memory_limit: Some(resource_limit_to_wire(request.request.memory_limit_bytes)),
        spill_limit: Some(resource_limit_to_wire(request.request.spill_limit_bytes)),
        result_batch_rows: request.request.result_batch_rows.map(NonZeroU32::get),
    })
}

/// Decodes and validates an Arrow Flight command descriptor.
pub fn decode_query_descriptor(descriptor: &FlightDescriptor) -> Result<FlightQueryRequest> {
    if DescriptorType::try_from(descriptor.r#type).ok() != Some(DescriptorType::Cmd)
        || !descriptor.path.is_empty()
    {
        return Err(invalid(
            "query descriptor must be a command with an empty path",
        ));
    }
    if descriptor.cmd.is_empty() || descriptor.cmd.len() > MAX_QUERY_COMMAND_BYTES {
        return Err(invalid("query command size is outside the protocol limit"));
    }
    let command = QueryCommand::decode(descriptor.cmd.clone())
        .map_err(|_| invalid("query command protobuf is malformed"))?;
    decode_query_command(command)
}

/// Decodes and validates an already parsed protocol query command.
pub fn decode_query_command(command: QueryCommand) -> Result<FlightQueryRequest> {
    require_version(command.protocol_version, "query command")?;
    let query_id = decode_stable_id(&command.query_id, QueryId::from_bytes, "query_id")?;
    let session_id = decode_stable_id(&command.session_id, SessionId::from_bytes, "session_id")?;
    if command.sql.trim().is_empty() || command.sql.len() > MAX_QUERY_SQL_BYTES {
        return Err(
            invalid("query SQL is empty or exceeds the protocol limit").with_query_id(query_id)
        );
    }
    if command.bindings.is_empty() || command.bindings.len() > MAX_QUERY_BINDINGS {
        return Err(
            invalid("query requires a bounded non-empty binding set").with_query_id(query_id)
        );
    }
    let mut binding_names = HashSet::with_capacity(command.bindings.len());
    let mut bindings = Vec::with_capacity(command.bindings.len());
    let mut tables = Vec::with_capacity(command.bindings.len());
    for binding in command.bindings {
        let binding = binding_from_wire(binding).map_err(|error| error.with_query_id(query_id))?;
        if !binding_names.insert(binding.sql_name.clone()) {
            return Err(invalid(format!(
                "query binding name is duplicated name=[{}]",
                binding.sql_name
            ))
            .with_query_id(query_id));
        }
        tables.push(binding.table.clone());
        bindings.push(binding);
    }
    let priority = match wire::QueryPriority::try_from(command.priority).ok() {
        Some(wire::QueryPriority::Interactive) => QueryPriority::Interactive,
        Some(wire::QueryPriority::Normal) => QueryPriority::Normal,
        Some(wire::QueryPriority::Maintenance) => QueryPriority::Maintenance,
        Some(wire::QueryPriority::Unspecified) | None => {
            return Err(invalid("query priority is missing or unknown").with_query_id(query_id));
        }
    };
    let memory_limit_bytes = command
        .memory_limit
        .and_then(resource_limit_from_wire)
        .ok_or_else(|| {
            invalid("query memory limit is missing or invalid").with_query_id(query_id)
        })?;
    let spill_limit_bytes = command
        .spill_limit
        .and_then(resource_limit_from_wire)
        .ok_or_else(|| {
            invalid("query spill limit is missing or invalid").with_query_id(query_id)
        })?;
    let result_batch_rows = command
        .result_batch_rows
        .map(|rows| {
            NonZeroU32::new(rows).ok_or_else(|| {
                invalid("query result batch rows must be positive").with_query_id(query_id)
            })
        })
        .transpose()?;
    Ok(FlightQueryRequest {
        request: QueryRequest {
            query_id,
            session_id,
            input: QueryInput::Sql(command.sql),
            tables,
            priority,
            deadline_ns: command.deadline_ns,
            memory_limit_bytes,
            spill_limit_bytes,
            result_batch_rows,
        },
        bindings,
    })
}

/// Encodes a query ticket for Arrow Flight `DoGet`.
#[must_use]
pub fn encode_query_ticket(ticket: &QueryTicket) -> Ticket {
    Ticket::new(ticket.encode_to_vec())
}

/// Decodes the fixed-width, versioned ticket envelope.
pub fn decode_query_ticket(ticket: &Ticket) -> Result<QueryTicket> {
    if ticket.ticket.is_empty() || ticket.ticket.len() > 256 {
        return Err(invalid("query ticket size is outside the protocol limit"));
    }
    let ticket = QueryTicket::decode(ticket.ticket.clone())
        .map_err(|_| invalid("query ticket protobuf is malformed"))?;
    validate_query_ticket(&ticket)?;
    Ok(ticket)
}

/// Validates a decoded ticket without consulting the server-side registry.
pub fn validate_query_ticket(ticket: &QueryTicket) -> Result<()> {
    require_version(ticket.protocol_version, "query ticket")?;
    require_bytes(
        &ticket.server_instance_id,
        OPAQUE_TICKET_ID_BYTES,
        "server_instance_id",
    )?;
    require_bytes(&ticket.ticket_id, OPAQUE_TICKET_ID_BYTES, "ticket_id")?;
    decode_stable_id(&ticket.query_id, QueryId::from_bytes, "query_id")?;
    NonZeroU64::new(ticket.generation)
        .ok_or_else(|| invalid("query ticket generation must be positive"))?;
    if ticket.expires_at_ns == 0 {
        return Err(invalid("query ticket expiry must be positive"));
    }
    Ok(())
}

/// Builds stable Flight metadata from a prepared query plan.
#[must_use]
pub fn query_info(plan: &QueryPlanInfo, generation: NonZeroU64) -> QueryInfo {
    QueryInfo {
        protocol_version: CURRENT_PROTOCOL_VERSION,
        query_id: Bytes::copy_from_slice(&plan.query_id.into_bytes()),
        generation: generation.get(),
        catalog_revision: plan.catalog_revision,
        planned_at_ns: plan.planned_at_ns,
        expires_at_ns: plan.expires_at_ns,
        snapshots: plan
            .snapshots
            .iter()
            .map(|snapshot| QueryTableVersion {
                table_id: Bytes::copy_from_slice(&snapshot.table_id.into_bytes()),
                snapshot_version: snapshot.snapshot_version,
                schema_version: snapshot.schema_version,
            })
            .collect(),
    }
}

fn binding_to_wire(binding: &FlightTableBinding) -> TableBinding {
    let table = match &binding.table {
        TableRef::Id { table_id } => Table::TableId(table_id.into_bytes().to_vec()),
        TableRef::Name { namespace, table } => Table::Name(wire::TableName {
            namespace: namespace.as_str().to_owned(),
            table: table.as_str().to_owned(),
        }),
    };
    TableBinding {
        sql_name: binding.sql_name.clone(),
        table: Some(table),
    }
}

fn binding_from_wire(binding: TableBinding) -> Result<FlightTableBinding> {
    require_name(&binding.sql_name, "binding sql_name")?;
    let table = match binding.table {
        Some(Table::Name(name)) => {
            require_name(&name.namespace, "table namespace")?;
            require_name(&name.table, "table name")?;
            TableRef::Name {
                namespace: CatalogName::new(name.namespace)
                    .map_err(|_| invalid("table namespace is invalid"))?,
                table: CatalogName::new(name.table)
                    .map_err(|_| invalid("table name is invalid"))?,
            }
        }
        Some(Table::TableId(value)) => TableRef::Id {
            table_id: decode_stable_id(&value, TableId::from_bytes, "table_id")?,
        },
        None => return Err(invalid("query binding table is missing")),
    };
    Ok(FlightTableBinding {
        sql_name: binding.sql_name,
        table,
    })
}

fn priority_to_wire(priority: QueryPriority) -> wire::QueryPriority {
    match priority {
        QueryPriority::Interactive => wire::QueryPriority::Interactive,
        QueryPriority::Normal => wire::QueryPriority::Normal,
        QueryPriority::Maintenance => wire::QueryPriority::Maintenance,
    }
}

fn decode_stable_id<T>(
    bytes: &[u8],
    constructor: impl FnOnce(
        [u8; STABLE_ID_BYTES],
    ) -> std::result::Result<T, zippy_types::id::NilStableIdError>,
    field: &str,
) -> Result<T> {
    let bytes: [u8; STABLE_ID_BYTES] = bytes
        .try_into()
        .map_err(|_| invalid(format!("{field} must contain exactly 16 bytes")))?;
    constructor(bytes).map_err(|_| invalid(format!("{field} must not be nil")))
}

fn require_name(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_QUERY_NAME_BYTES {
        return Err(invalid(format!(
            "{field} is empty or exceeds the protocol limit"
        )));
    }
    Ok(())
}

fn require_bytes(value: &[u8], expected: usize, field: &str) -> Result<()> {
    if value.len() != expected || value.iter().all(|byte| *byte == 0) {
        return Err(invalid(format!(
            "{field} must contain {expected} nonzero bytes"
        )));
    }
    Ok(())
}

fn require_version(version: u32, subject: &str) -> Result<()> {
    if version != CURRENT_PROTOCOL_VERSION {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "{subject} protocol version is unsupported version=[{version}] supported=[{CURRENT_PROTOCOL_VERSION}]"
            ),
        )
        .with_retryable(false));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zippy_types::table::CatalogName;
    use zippy_types::ResourceLimit;

    fn request() -> FlightQueryRequest {
        let table = TableRef::Name {
            namespace: CatalogName::new("market").unwrap(),
            table: CatalogName::new("ticks").unwrap(),
        };
        FlightQueryRequest {
            request: QueryRequest {
                query_id: QueryId::from_u128(1).unwrap(),
                session_id: SessionId::from_u128(2).unwrap(),
                input: QueryInput::Sql("select * from ticks".to_owned()),
                tables: vec![table.clone()],
                priority: QueryPriority::Interactive,
                deadline_ns: None,
                memory_limit_bytes: ResourceLimit::ServerDefault,
                spill_limit_bytes: ResourceLimit::Disabled,
                result_batch_rows: NonZeroU32::new(1024),
            },
            bindings: vec![FlightTableBinding {
                sql_name: "ticks".to_owned(),
                table,
            }],
        }
    }

    #[test]
    fn descriptor_round_trips_validated_query() {
        let request = request();
        let descriptor = encode_query_descriptor(&encode_query_request(&request).unwrap());

        assert_eq!(decode_query_descriptor(&descriptor).unwrap(), request);
    }

    #[test]
    fn descriptor_rejects_unknown_version_before_planning() {
        let request = request();
        let mut command = encode_query_request(&request).unwrap();
        command.protocol_version += 1;

        let error = decode_query_descriptor(&encode_query_descriptor(&command)).unwrap_err();
        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("protocol version"));
    }

    #[test]
    fn encoder_rejects_logical_plan_in_sql_only_protocol_version() {
        let mut request = request();
        request.request.input = QueryInput::LogicalPlan(
            zippy_types::query::LogicalPlanEnvelope::current(vec![1]).unwrap(),
        );

        let error = encode_query_request(&request).unwrap_err();

        assert_eq!(error.code(), ErrorCode::UnsupportedPlan);
        assert_eq!(error.query_id(), Some(request.request.query_id));
    }

    #[test]
    fn encoder_rejects_inconsistent_table_and_binding_contracts() {
        let mut request = request();
        request.request.tables.clear();

        let error = encode_query_request(&request).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("exactly match"));
    }

    #[test]
    fn decoder_rejects_duplicate_binding_names() {
        let request = request();
        let mut command = encode_query_request(&request).unwrap();
        command.bindings.push(command.bindings[0].clone());

        let error = decode_query_command(command).unwrap_err();

        assert_eq!(error.code(), ErrorCode::InvalidArgument);
        assert!(error.message().contains("duplicated"));
    }

    #[test]
    fn ticket_rejects_nil_or_wrong_width_ids() {
        let ticket = QueryTicket {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            server_instance_id: Bytes::from_static(&[1; OPAQUE_TICKET_ID_BYTES]),
            ticket_id: Bytes::from_static(&[0; OPAQUE_TICKET_ID_BYTES]),
            query_id: Bytes::copy_from_slice(&QueryId::from_u128(3).unwrap().into_bytes()),
            generation: 1,
            expires_at_ns: 10,
        };

        assert_eq!(
            decode_query_ticket(&encode_query_ticket(&ticket))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
    }
}
