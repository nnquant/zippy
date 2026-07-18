//! Bounded Arrow Flight SQL metadata requests and opaque single-use tickets.

use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::sql::{
    Any, Command, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
};
use arrow_flight::{FlightDescriptor, Ticket};
use bytes::Bytes;
use prost::Message;
use zippy_types::{ErrorCode, Result, ServerError};

use crate::query::{MAX_QUERY_COMMAND_BYTES, OPAQUE_TICKET_ID_BYTES};
use crate::wire::MetadataTicket;
use crate::CURRENT_PROTOCOL_VERSION;

const FLIGHT_SQL_TYPE_URL_PREFIX: &str = "type.googleapis.com/arrow.flight.protocol.sql.";
const METADATA_TICKET_TYPE_URL: &str = "type.googleapis.com/zippy.flight.v1.MetadataTicket";
const MAX_FILTER_BYTES: usize = 1_024;
const MAX_TABLE_TYPES: usize = 32;
const MAX_TABLE_TYPE_BYTES: usize = 64;
const MAX_METADATA_ROWS: usize = 4_096;
const MIN_METADATA_BYTES: usize = 64 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024 * 1024;
const MAX_METADATA_TICKET_TTL: Duration = Duration::from_secs(5 * 60);

/// Supported Flight SQL discovery command.
#[derive(Debug, Clone, PartialEq)]
pub enum FlightSqlMetadataRequest {
    /// Lists the single Zippy database catalog.
    Catalogs(CommandGetCatalogs),
    /// Lists Zippy namespaces as Flight SQL database schemas.
    DbSchemas(CommandGetDbSchemas),
    /// Lists active Zippy tables, optionally with Arrow schemas.
    Tables(CommandGetTables),
    /// Lists the supported table type names.
    TableTypes(CommandGetTableTypes),
}

/// Bounded materialization policy supplied to the metadata frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlightSqlMetadataLimits {
    max_rows: usize,
    max_bytes: usize,
    ticket_ttl: Duration,
}

impl FlightSqlMetadataLimits {
    /// Creates validated metadata row, byte, and ticket-lifetime limits.
    pub fn try_new(max_rows: usize, max_bytes: usize, ticket_ttl: Duration) -> Result<Self> {
        if !(1..=MAX_METADATA_ROWS).contains(&max_rows) {
            return Err(invalid(format!(
                "Flight SQL metadata rows must be between 1 and {MAX_METADATA_ROWS}"
            )));
        }
        if !(MIN_METADATA_BYTES..=MAX_METADATA_BYTES).contains(&max_bytes) {
            return Err(invalid(format!(
                "Flight SQL metadata bytes must be between {MIN_METADATA_BYTES} and {MAX_METADATA_BYTES}"
            )));
        }
        if ticket_ttl.is_zero() || ticket_ttl > MAX_METADATA_TICKET_TTL {
            return Err(invalid(
                "Flight SQL metadata ticket TTL must be positive and no longer than five minutes",
            ));
        }
        Ok(Self {
            max_rows,
            max_bytes,
            ticket_ttl,
        })
    }

    /// Returns the maximum rows examined and returned by one request.
    #[must_use]
    pub const fn max_rows(self) -> usize {
        self.max_rows
    }

    /// Returns the maximum retained Arrow bytes for one result.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the lifetime of one prepared single-use metadata ticket.
    #[must_use]
    pub const fn ticket_ttl(self) -> Duration {
        self.ticket_ttl
    }
}

/// One revision-fixed Arrow batch prepared for Flight SQL delivery.
#[derive(Debug)]
pub struct PreparedFlightSqlMetadata {
    catalog_revision: u64,
    batch: RecordBatch,
}

impl PreparedFlightSqlMetadata {
    /// Validates frontend output against the protocol-owned row and byte limits.
    pub fn try_new(
        catalog_revision: u64,
        batch: RecordBatch,
        limits: FlightSqlMetadataLimits,
    ) -> Result<Self> {
        if batch.num_rows() > limits.max_rows {
            return Err(resource_exhausted(format!(
                "Flight SQL metadata row budget exceeded actual=[{}] maximum=[{}]",
                batch.num_rows(),
                limits.max_rows
            )));
        }
        let retained_bytes = batch.get_array_memory_size();
        if retained_bytes > limits.max_bytes {
            return Err(resource_exhausted(format!(
                "Flight SQL metadata byte budget exceeded actual=[{retained_bytes}] maximum=[{}]",
                limits.max_bytes
            )));
        }
        Ok(Self {
            catalog_revision,
            batch,
        })
    }

    /// Returns the Catalog revision observed while materializing the batch.
    #[must_use]
    pub const fn catalog_revision(&self) -> u64 {
        self.catalog_revision
    }

    /// Returns the prepared Arrow batch.
    #[must_use]
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Returns the Arrow-owned bytes retained until ticket consumption or expiry.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.batch.get_array_memory_size()
    }

    /// Consumes the prepared result and returns its Arrow batch.
    #[must_use]
    pub fn into_batch(self) -> RecordBatch {
        self.batch
    }
}

/// Decodes a supported Flight SQL metadata descriptor, or returns `None` for native Zippy SQL.
pub fn decode_flight_sql_metadata_request(
    descriptor: &FlightDescriptor,
) -> Result<Option<FlightSqlMetadataRequest>> {
    if descriptor.cmd.len() > MAX_QUERY_COMMAND_BYTES {
        return Err(invalid(format!(
            "Flight SQL metadata command exceeds byte bound maximum=[{MAX_QUERY_COMMAND_BYTES}]"
        )));
    }
    let any = match Any::decode(descriptor.cmd.as_ref()) {
        Ok(any) if !any.type_url.is_empty() => any,
        Ok(_) | Err(_) => return Ok(None),
    };
    let type_url = any.type_url.clone();
    let command = Command::try_from(any).map_err(|error| {
        invalid(format!(
            "Flight SQL command envelope is malformed error=[{error}]"
        ))
    })?;
    let request = match command {
        Command::CommandGetCatalogs(command) => FlightSqlMetadataRequest::Catalogs(command),
        Command::CommandGetDbSchemas(command) => {
            validate_optional_filter(
                "db_schema_filter_pattern",
                &command.db_schema_filter_pattern,
            )?;
            validate_optional_filter("catalog", &command.catalog)?;
            FlightSqlMetadataRequest::DbSchemas(command)
        }
        Command::CommandGetTables(command) => {
            validate_optional_filter("catalog", &command.catalog)?;
            validate_optional_filter(
                "db_schema_filter_pattern",
                &command.db_schema_filter_pattern,
            )?;
            validate_optional_filter(
                "table_name_filter_pattern",
                &command.table_name_filter_pattern,
            )?;
            if command.table_types.len() > MAX_TABLE_TYPES
                || command
                    .table_types
                    .iter()
                    .any(|value| value.len() > MAX_TABLE_TYPE_BYTES)
            {
                return Err(invalid("Flight SQL table type filter exceeds its bound"));
            }
            FlightSqlMetadataRequest::Tables(command)
        }
        Command::CommandGetTableTypes(command) => FlightSqlMetadataRequest::TableTypes(command),
        Command::Unknown(_) if !type_url.starts_with(FLIGHT_SQL_TYPE_URL_PREFIX) => {
            return Err(unsupported(format!(
                "Flight descriptor type is unsupported type_url=[{type_url}]"
            )));
        }
        other => {
            return Err(unsupported(format!(
                "Flight SQL command is not enabled in metadata stage type_url=[{}]",
                other.type_url()
            )));
        }
    };
    Ok(Some(request))
}

/// Encodes one opaque Zippy metadata ticket inside a protobuf `Any` envelope.
#[must_use]
pub fn encode_metadata_ticket(ticket: &MetadataTicket) -> Ticket {
    Ticket::new(
        Any {
            type_url: METADATA_TICKET_TYPE_URL.to_owned(),
            value: Bytes::from(ticket.encode_to_vec()),
        }
        .encode_to_vec(),
    )
}

/// Decodes a Zippy metadata ticket, or returns `None` for a native query ticket.
pub fn decode_metadata_ticket(ticket: &Ticket) -> Result<Option<MetadataTicket>> {
    let any = match Any::decode(ticket.ticket.as_ref()) {
        Ok(any) if !any.type_url.is_empty() => any,
        Ok(_) | Err(_) => return Ok(None),
    };
    if any.type_url != METADATA_TICKET_TYPE_URL {
        return Err(invalid(format!(
            "Flight ticket type is unsupported type_url=[{}]",
            any.type_url
        )));
    }
    let ticket = MetadataTicket::decode(any.value)
        .map_err(|_| invalid("Flight SQL metadata ticket is malformed"))?;
    if ticket.protocol_version != CURRENT_PROTOCOL_VERSION
        || ticket.server_instance_id.len() != OPAQUE_TICKET_ID_BYTES
        || ticket.ticket_id.len() != OPAQUE_TICKET_ID_BYTES
        || ticket.server_instance_id.iter().all(|byte| *byte == 0)
        || ticket.ticket_id.iter().all(|byte| *byte == 0)
        || ticket.expires_at_ns == 0
    {
        return Err(invalid("Flight SQL metadata ticket fields are invalid"));
    }
    Ok(Some(ticket))
}

fn validate_optional_filter(name: &str, value: &Option<String>) -> Result<()> {
    if value
        .as_ref()
        .is_some_and(|value| value.len() > MAX_FILTER_BYTES)
    {
        return Err(invalid(format!(
            "Flight SQL metadata filter exceeds byte bound name=[{name}] maximum=[{MAX_FILTER_BYTES}]"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn unsupported(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::UnsupportedPlan, message).with_retryable(false)
}

fn resource_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}
