//! Checked-in prost definitions generated from the canonical protocol schema.

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionOpenRequest {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SessionOpenResponse {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub session_id: ::prost::bytes::Bytes,
    #[prost(string, tag = "3")]
    pub bearer_token: ::prost::alloc::string::String,
    #[prost(uint64, tag = "4")]
    pub expires_at_ns: u64,
    #[prost(enumeration = "SessionRole", tag = "5")]
    pub role: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SessionRole {
    Unspecified = 0,
    Read = 1,
    Administrator = 2,
    Writer = 3,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutCommand {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub session_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub put_id: ::prost::bytes::Bytes,
    #[prost(string, tag = "4")]
    pub stream_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "5")]
    pub first_batch_sequence: u64,
    #[prost(enumeration = "PutMutation", tag = "6")]
    pub mutation: i32,
    #[prost(enumeration = "PutDurability", tag = "7")]
    pub durability: i32,
    #[prost(bytes = "bytes", tag = "8")]
    pub producer_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "9")]
    pub connection_id: ::prost::bytes::Bytes,
    #[prost(uint64, optional, tag = "10")]
    pub expected_writer_epoch: ::core::option::Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PutMutation {
    Unspecified = 0,
    Append = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PutDurability {
    Unspecified = 0,
    Memory = 1,
    Wal = 2,
    Persisted = 3,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutBatchResult {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub put_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "3")]
    pub batch_sequence: u64,
    #[prost(uint64, tag = "4")]
    pub row_count: u64,
    #[prost(enumeration = "PutDurability", tag = "5")]
    pub durability_reached: i32,
    #[prost(bytes = "bytes", tag = "6")]
    pub request_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "7")]
    pub producer_epoch: u64,
    #[prost(uint64, optional, tag = "8")]
    pub first_log_offset: ::core::option::Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    pub last_log_offset: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "10")]
    pub previous_log_offset: ::core::option::Option<OffsetWatermark>,
    #[prost(message, optional, tag = "11")]
    pub committed_log_offset: ::core::option::Option<OffsetWatermark>,
    #[prost(uint64, optional, tag = "12")]
    pub persisted_snapshot_version: ::core::option::Option<u64>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryCommand {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub query_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub session_id: ::prost::bytes::Bytes,
    #[prost(string, tag = "4")]
    pub sql: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "5")]
    pub bindings: ::prost::alloc::vec::Vec<TableBinding>,
    #[prost(enumeration = "QueryPriority", tag = "6")]
    pub priority: i32,
    #[prost(uint64, optional, tag = "7")]
    pub deadline_ns: ::core::option::Option<u64>,
    #[prost(message, optional, tag = "8")]
    pub memory_limit: ::core::option::Option<ResourceLimit>,
    #[prost(message, optional, tag = "9")]
    pub spill_limit: ::core::option::Option<ResourceLimit>,
    #[prost(uint32, optional, tag = "10")]
    pub result_batch_rows: ::core::option::Option<u32>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableBinding {
    #[prost(string, tag = "1")]
    pub sql_name: ::prost::alloc::string::String,
    #[prost(oneof = "table_binding::Table", tags = "2, 3")]
    pub table: ::core::option::Option<table_binding::Table>,
}

pub mod table_binding {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Table {
        #[prost(message, tag = "2")]
        Name(super::TableName),
        #[prost(bytes, tag = "3")]
        TableId(::prost::alloc::vec::Vec<u8>),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableName {
    #[prost(string, tag = "1")]
    pub namespace: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QueryPriority {
    Unspecified = 0,
    Interactive = 1,
    Normal = 2,
    Maintenance = 3,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceLimit {
    #[prost(enumeration = "resource_limit::Kind", tag = "1")]
    pub kind: i32,
    #[prost(uint64, tag = "2")]
    pub bytes: u64,
}

pub mod resource_limit {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        Unspecified = 0,
        ServerDefault = 1,
        Disabled = 2,
        Bytes = 3,
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryTicket {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub server_instance_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub ticket_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "4")]
    pub query_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "5")]
    pub generation: u64,
    #[prost(uint64, tag = "6")]
    pub expires_at_ns: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetadataTicket {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub server_instance_id: ::prost::bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    pub ticket_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "4")]
    pub catalog_revision: u64,
    #[prost(uint64, tag = "5")]
    pub expires_at_ns: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetadataInfo {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(uint64, tag = "2")]
    pub catalog_revision: u64,
    #[prost(uint64, tag = "3")]
    pub expires_at_ns: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryInfo {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    pub query_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "3")]
    pub generation: u64,
    #[prost(uint64, tag = "4")]
    pub catalog_revision: u64,
    #[prost(uint64, tag = "5")]
    pub planned_at_ns: u64,
    #[prost(uint64, tag = "6")]
    pub expires_at_ns: u64,
    #[prost(message, repeated, tag = "7")]
    pub snapshots: ::prost::alloc::vec::Vec<QueryTableVersion>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryTableVersion {
    #[prost(bytes = "bytes", tag = "1")]
    pub table_id: ::prost::bytes::Bytes,
    #[prost(uint64, tag = "2")]
    pub snapshot_version: u64,
    #[prost(uint64, tag = "3")]
    pub schema_version: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErrorEnvelope {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(string, tag = "2")]
    pub code: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub message: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub retryable: bool,
    #[prost(bytes = "vec", optional, tag = "5")]
    pub request_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub query_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "7")]
    pub table_id: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(
        oneof = "error_envelope::Details",
        tags = "10, 11, 12, 13, 14, 15, 16, 17"
    )]
    pub details: ::core::option::Option<error_envelope::Details>,
}

pub mod error_envelope {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Details {
        #[prost(message, tag = "10")]
        RevisionConflict(super::RevisionConflict),
        #[prost(message, tag = "11")]
        ProducerEpoch(super::ExpectedActual),
        #[prost(message, tag = "12")]
        SequenceGap(super::ExpectedActual),
        #[prost(message, tag = "13")]
        SchemaVersion(super::ExpectedActual),
        #[prost(message, tag = "14")]
        OffsetExpired(super::OffsetExpired),
        #[prost(message, tag = "15")]
        ResourceLimit(super::ResourceLimitExceeded),
        #[prost(message, tag = "16")]
        DataCorruption(super::DataCorruption),
        #[prost(message, tag = "17")]
        UnsupportedPlan(super::UnsupportedPlan),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RevisionConflict {
    #[prost(uint64, tag = "1")]
    pub expected: u64,
    #[prost(uint64, tag = "2")]
    pub actual: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExpectedActual {
    #[prost(uint64, tag = "1")]
    pub expected: u64,
    #[prost(uint64, tag = "2")]
    pub actual: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OffsetExpired {
    #[prost(message, optional, tag = "1")]
    pub requested: ::core::option::Option<OffsetWatermark>,
    #[prost(message, optional, tag = "2")]
    pub earliest: ::core::option::Option<OffsetWatermark>,
    #[prost(message, optional, tag = "3")]
    pub latest: ::core::option::Option<OffsetWatermark>,
    #[prost(bool, tag = "4")]
    pub snapshot_recovery_available: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OffsetWatermark {
    #[prost(enumeration = "offset_watermark::State", tag = "1")]
    pub state: i32,
    #[prost(uint64, tag = "2")]
    pub offset: u64,
}

pub mod offset_watermark {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum State {
        Unspecified = 0,
        Empty = 1,
        Known = 2,
        LegacyUnknown = 3,
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceLimitExceeded {
    #[prost(string, tag = "1")]
    pub resource: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub limit: ::core::option::Option<ResourceLimit>,
    #[prost(uint64, tag = "3")]
    pub requested: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataCorruption {
    #[prost(string, tag = "1")]
    pub object: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub expected_checksum: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "3")]
    pub actual_checksum: ::core::option::Option<::prost::alloc::string::String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnsupportedPlan {
    #[prost(string, tag = "1")]
    pub operator: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CapabilitiesRequest {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CapabilitiesResponse {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(string, tag = "2")]
    pub server_version: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub capabilities: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(uint64, tag = "4")]
    pub max_message_bytes: u64,
    #[prost(uint64, tag = "5")]
    pub max_pending_tickets: u64,
    #[prost(bool, tag = "6")]
    pub authentication_required: bool,
    #[prost(uint64, tag = "7")]
    pub session_ttl_ms: u64,
    #[prost(bool, tag = "8")]
    pub administrator_authentication_enabled: bool,
    #[prost(uint64, tag = "9")]
    pub metadata_max_rows: u64,
    #[prost(uint64, tag = "10")]
    pub metadata_max_bytes: u64,
    #[prost(bool, tag = "11")]
    pub writer_authentication_enabled: bool,
    #[prost(bool, tag = "12")]
    pub ingest_enabled: bool,
    #[prost(uint64, tag = "13")]
    pub ingest_max_concurrent_puts: u64,
    #[prost(uint64, tag = "14")]
    pub ingest_max_batches_per_put: u64,
    #[prost(uint64, tag = "15")]
    pub ingest_max_rows_per_batch: u64,
    #[prost(uint64, tag = "16")]
    pub ingest_max_rows_per_put: u64,
    #[prost(uint64, tag = "17")]
    pub ingest_max_batch_bytes: u64,
    #[prost(uint64, tag = "18")]
    pub ingest_max_bytes_per_put: u64,
}
