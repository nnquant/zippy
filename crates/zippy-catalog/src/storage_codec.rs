//! Versioned durable BLOB codecs and canonical Arrow schema validation.

use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use std::mem::{size_of, size_of_val};
use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use serde::de::DeserializeOwned;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zippy_types::schema_contract::{
    MAX_CATALOG_SCHEMA_FIELDS as MAX_SCHEMA_TOTAL_FIELDS,
    MAX_CATALOG_SCHEMA_IPC_BYTES as MAX_SCHEMA_IPC_BYTES,
    MAX_CATALOG_SCHEMA_METADATA_BYTES as MAX_SCHEMA_METADATA_BYTES,
    MAX_CATALOG_SCHEMA_METADATA_ENTRIES as MAX_SCHEMA_METADATA_ENTRIES,
    MAX_CATALOG_SCHEMA_NESTING_DEPTH as MAX_SCHEMA_NESTING_DEPTH,
};
use zippy_types::schema_hash::canonical_catalog_schema_hash_v1;
use zippy_types::table::{
    AppendTableDefinition, ColumnName, ColumnSet, FiniteMetadataFloat, FrameTableDefinition,
    KeyValueTableDefinition, MetadataMap, MetadataScalar, SchemaDescriptor, SchemaIpcPayload,
    TableDefinition, TableDescriptor,
};
use zippy_types::{ErrorCode, Result, ServerError};

const MAX_SCHEMA_FLATBUFFER_DEPTH: usize = 64;
const MAX_SCHEMA_FLATBUFFER_TABLES: usize = 32_768;
const MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES: usize = 16 * 1024 * 1024;
const DECODED_SCHEMA_CHARGE_MULTIPLIER: u64 = 2;
const DECODED_SCHEMA_FIXED_OVERHEAD_BYTES: u64 = 1024;
const MAX_TYPED_JSON_BYTES: usize = 1024 * 1024;
const TYPED_JSON_CHUNK_BYTES: usize = 64 * 1024;
const TYPED_JSON_MAGIC: &[u8; 3] = b"ZCJ";
const TYPED_JSON_V1: u8 = 1;
const TYPED_JSON_PREFIX: &[u8; 4] = b"ZCJ\x01";
const SHA256_BYTES: usize = 32;
const TYPED_JSON_HEADER_BYTES: usize = TYPED_JSON_PREFIX.len() + SHA256_BYTES;
const TYPED_JSON_HASH_DOMAIN: &[u8] = b"zippy.catalog.typed-json.v1\0";

const TABLE_DEFINITION_BLOB_KIND: &str = "table_definition";
const TABLE_CONFIG_BLOB_KIND: &str = "table_config";
const SNAPSHOT_SUMMARY_BLOB_KIND: &str = "snapshot_summary";
const DATA_FILE_PARTITION_BLOB_KIND: &str = "data_file_partition";
const DATA_FILE_STATISTICS_BLOB_KIND: &str = "data_file_statistics";
const DATA_FILE_METADATA_BLOB_KIND: &str = "data_file_metadata";

/// One verified table schema plus its conservative retained-memory charge.
///
/// The charge is a stable logical startup budget, not an exact allocator or RSS
/// measurement. It includes Arrow's recursively reported field size, root
/// metadata allocations, reference-counted allocation overhead, and a
/// conservative multiplier and fixed allocator margin.
#[derive(Debug)]
pub struct DecodedPersistedTableSchema {
    schema: SchemaRef,
    retained_charge_bytes: u64,
}

impl DecodedPersistedTableSchema {
    /// Borrows the decoded Arrow schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the conservative bytes to charge while retaining this schema.
    #[must_use]
    pub const fn retained_charge_bytes(&self) -> u64 {
        self.retained_charge_bytes
    }

    /// Consumes the charged result and returns the decoded Arrow schema.
    #[must_use]
    pub fn into_schema(self) -> SchemaRef {
        self.schema
    }
}

/// Decodes and verifies the Arrow schema stored in a persistent Catalog descriptor.
///
/// This is the only public persisted-schema decoder. It accepts exactly one
/// bounded schema-only Arrow IPC stream and verifies that its canonical logical
/// hash matches the descriptor. It does not validate the descriptor's placement
/// in a table's schema history.
///
/// # Errors
///
/// Returns [`ErrorCode::DataCorruption`] when the persisted IPC payload is
/// malformed, exceeds its decoding budgets, contains anything other than one
/// schema, cannot be canonically hashed, or does not match `schema_hash`.
pub fn decode_persisted_schema(descriptor: &SchemaDescriptor) -> Result<SchemaRef> {
    let schema = decode_schema_ipc_stream(descriptor.schema_ipc.as_bytes()).map_err(|error| {
        persisted_schema_corruption(
            descriptor,
            format!("persisted Catalog schema IPC is invalid error=[{error}]"),
        )
    })?;
    let actual_hash = canonical_schema_hash(&schema).map_err(|error| {
        persisted_schema_corruption(
            descriptor,
            format!("persisted Catalog schema cannot be hashed error=[{error}]"),
        )
    })?;
    if actual_hash != descriptor.schema_hash {
        return Err(persisted_schema_corruption(
            descriptor,
            format!(
                "catalog schema hash mismatch expected=[{}] actual=[{}]",
                descriptor.schema_hash, actual_hash
            ),
        ));
    }
    Ok(schema)
}

/// Decodes a persisted schema and validates it against its owning table definition.
///
/// This table-aware entry point is the startup/runtime boundary. In addition to
/// [`decode_persisted_schema`], it verifies descriptor ownership and every
/// definition column reference so a valid schema row cannot be paired with an
/// incompatible table row.
///
/// # Errors
///
/// Returns [`ErrorCode::DataCorruption`] when the descriptors identify different
/// tables or when the decoded schema violates the persisted table definition.
pub fn decode_persisted_table_schema(
    table: &TableDescriptor,
    descriptor: &SchemaDescriptor,
) -> Result<DecodedPersistedTableSchema> {
    if table.table_id != descriptor.table_id {
        return Err(persisted_schema_corruption(
            descriptor,
            format!(
                "persisted catalog table and schema identities differ table_id=[{}]",
                table.table_id
            ),
        ));
    }
    let schema = decode_persisted_schema(descriptor)?;
    crate::table_domain::validate_table_schema(&schema, &table.definition).map_err(|error| {
        persisted_schema_corruption(
            descriptor,
            format!("persisted catalog table schema is incompatible error=[{error}]"),
        )
    })?;
    let retained_charge_bytes = decoded_schema_retained_charge(schema.as_ref()).map_err(|error| {
        ServerError::new(
            error.code(),
            format!(
                "persisted catalog schema retained-memory charge failed schema_version=[{}] error=[{error}]",
                descriptor.schema_version
            ),
        )
        .with_table_id(descriptor.table_id)
        .with_retryable(false)
    })?;
    Ok(DecodedPersistedTableSchema {
        schema,
        retained_charge_bytes,
    })
}

fn decoded_schema_retained_charge(schema: &Schema) -> Result<u64> {
    let root_bytes = size_of_val(schema)
        .checked_add(size_of::<SchemaRef>())
        .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
        .ok_or_else(|| schema_budget_exhausted("decoded schema root byte charge overflow"))?;
    let metadata_slots = schema
        .metadata
        .capacity()
        .checked_mul(size_of::<(String, String)>())
        .ok_or_else(|| schema_budget_exhausted("decoded schema metadata slot charge overflow"))?;
    let metadata_strings = schema
        .metadata
        .iter()
        .try_fold(0_usize, |bytes, (key, value)| {
            bytes
                .checked_add(key.capacity())
                .and_then(|bytes| bytes.checked_add(value.capacity()))
                .ok_or_else(|| {
                    schema_budget_exhausted("decoded schema metadata string charge overflow")
                })
        })?;
    let reported_bytes = root_bytes
        .checked_add(schema.fields().size())
        .and_then(|bytes| bytes.checked_add(metadata_slots))
        .and_then(|bytes| bytes.checked_add(metadata_strings))
        .ok_or_else(|| schema_budget_exhausted("decoded schema retained byte charge overflow"))?;
    u64::try_from(reported_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(DECODED_SCHEMA_CHARGE_MULTIPLIER))
        .and_then(|bytes| bytes.checked_add(DECODED_SCHEMA_FIXED_OVERHEAD_BYTES))
        .ok_or_else(|| schema_budget_exhausted("decoded schema retained byte charge overflow"))
}

/// Decodes exactly one schema-only Arrow IPC stream supplied by a command.
pub(crate) fn decode_schema_ipc_stream(payload: &[u8]) -> Result<SchemaRef> {
    validate_schema_only_ipc_framing(payload)?;

    // Framing validation bounds every length before Arrow allocates from it.
    let reader = catch_unwind(AssertUnwindSafe(|| {
        StreamReader::try_new(Cursor::new(payload), None)
    }))
    .map_err(|_| {
        invalid_schema_ipc("catalog schema IPC triggered an invalid Arrow schema conversion")
    })?;
    let mut reader = reader.map_err(|error| {
        invalid_schema_ipc(format!("invalid catalog schema IPC stream error=[{error}]"))
    })?;
    let schema = reader.schema();
    match reader.next() {
        None => {}
        Some(Ok(_)) => {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "catalog schema IPC stream must not contain record batches",
            ));
        }
        Some(Err(error)) => {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!("invalid catalog schema IPC stream body error=[{error}]"),
            ));
        }
    }
    let consumed = usize::try_from(reader.get_ref().position()).map_err(|_| {
        ServerError::new(
            ErrorCode::InvalidArgument,
            "catalog schema IPC stream position exceeds the supported domain",
        )
    })?;
    if consumed != payload.len() {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            format!(
                "catalog schema IPC stream has trailing bytes consumed=[{}] actual=[{}]",
                consumed,
                payload.len()
            ),
        ));
    }
    Ok(schema)
}

fn validate_schema_only_ipc_framing(payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_SCHEMA_IPC_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog schema IPC exceeds limit actual_bytes=[{}] maximum_bytes=[{}]",
                payload.len(),
                MAX_SCHEMA_IPC_BYTES
            ),
        ));
    }

    let mut offset = 0;
    let metadata_len = read_ipc_message_length(payload, &mut offset, "schema")?;
    if metadata_len == 0 {
        return Err(invalid_schema_ipc(
            "catalog schema IPC stream starts with an end marker",
        ));
    }
    let metadata_end = offset.checked_add(metadata_len).ok_or_else(|| {
        invalid_schema_ipc("catalog schema IPC metadata length overflows the supported domain")
    })?;
    if metadata_end > payload.len() {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC metadata exceeds remaining bytes declared=[{}] remaining=[{}]",
            metadata_len,
            payload.len().saturating_sub(offset)
        )));
    }
    let verifier_options = flatbuffers::VerifierOptions {
        max_depth: MAX_SCHEMA_FLATBUFFER_DEPTH,
        max_tables: MAX_SCHEMA_FLATBUFFER_TABLES,
        max_apparent_size: MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES,
        ..flatbuffers::VerifierOptions::default()
    };
    let message =
        arrow::ipc::root_as_message_with_opts(&verifier_options, &payload[offset..metadata_end])
            .map_err(map_schema_flatbuffer_error)?;
    if message.header_type() != arrow::ipc::MessageHeader::Schema {
        return Err(invalid_schema_ipc(
            "catalog schema IPC first message is not a schema",
        ));
    }
    let ipc_schema = message.header_as_schema().ok_or_else(|| {
        invalid_schema_ipc("catalog schema IPC message is missing its schema header")
    })?;
    if message.bodyLength() != 0 {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC schema message has a nonzero body length actual=[{}]",
            message.bodyLength()
        )));
    }
    validate_schema_flatbuffer(ipc_schema)?;
    offset = metadata_end;

    let next_len = read_ipc_message_length(payload, &mut offset, "end marker")?;
    if next_len != 0 {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC stream contains an extra message metadata_bytes=[{next_len}]"
        )));
    }
    if offset != payload.len() {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC stream has trailing bytes after end marker trailing=[{}]",
            payload.len() - offset
        )));
    }
    Ok(())
}

fn map_schema_flatbuffer_error(error: flatbuffers::InvalidFlatbuffer) -> ServerError {
    let code = if matches!(
        &error,
        flatbuffers::InvalidFlatbuffer::TooManyTables
            | flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge
            | flatbuffers::InvalidFlatbuffer::DepthLimitReached
    ) {
        ErrorCode::ResourceExhausted
    } else {
        ErrorCode::InvalidArgument
    };
    ServerError::new(
        code,
        format!("invalid or over-budget catalog schema IPC message metadata error=[{error}]"),
    )
}

fn validate_schema_flatbuffer(schema: arrow::ipc::Schema<'_>) -> Result<()> {
    if schema.endianness() != arrow::ipc::Endianness::Little {
        return Err(invalid_schema_ipc(
            "catalog schema IPC must use little-endian Arrow metadata",
        ));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| invalid_schema_ipc("catalog schema IPC is missing its field vector"))?;
    let mut budget = SchemaFlatbufferBudget::default();
    if let Some(metadata) = schema.custom_metadata() {
        budget.metadata(metadata.iter(), "schema")?;
    }
    for field in fields.iter() {
        budget.field(field, 1)?;
    }
    Ok(())
}

#[derive(Default)]
struct SchemaFlatbufferBudget {
    fields: usize,
    metadata_entries: usize,
    metadata_bytes: usize,
}

impl SchemaFlatbufferBudget {
    fn field(&mut self, field: arrow::ipc::Field<'_>, depth: usize) -> Result<()> {
        if depth > MAX_SCHEMA_NESTING_DEPTH {
            return Err(schema_budget_exhausted(format!(
                "catalog schema nesting exceeds maximum actual=[{depth}] maximum=[{MAX_SCHEMA_NESTING_DEPTH}]"
            )));
        }
        self.fields = self.fields.checked_add(1).ok_or_else(|| {
            schema_budget_exhausted("catalog schema recursive field count overflow")
        })?;
        if self.fields > MAX_SCHEMA_TOTAL_FIELDS {
            return Err(schema_budget_exhausted(format!(
                "catalog schema recursive field count exceeds maximum actual=[{}] maximum=[{}]",
                self.fields, MAX_SCHEMA_TOTAL_FIELDS
            )));
        }

        let name = field
            .name()
            .ok_or_else(|| invalid_schema_ipc("catalog schema IPC field is missing its name"))?;
        if field.type_type() == arrow::ipc::Type::NONE || field.type_().is_none() {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC field is missing its type field=[{name}]"
            )));
        }
        if let Some(metadata) = field.custom_metadata() {
            self.metadata(metadata.iter(), "field")?;
        }
        validate_dictionary_encoding(field, name)?;

        let children = field.children();
        let child_count = children.map_or(0, |children| children.len());
        validate_field_shape(field, child_count, name)?;
        if let Some(children) = children {
            for child in children.iter() {
                self.field(child, depth + 1)?;
            }
        }
        Ok(())
    }

    fn metadata<'a>(
        &mut self,
        entries: impl IntoIterator<Item = arrow::ipc::KeyValue<'a>>,
        context: &str,
    ) -> Result<()> {
        for entry in entries {
            let key = entry.key().ok_or_else(|| {
                invalid_schema_ipc(format!(
                    "catalog schema IPC {context} metadata is missing its key"
                ))
            })?;
            let value = entry.value().ok_or_else(|| {
                invalid_schema_ipc(format!(
                    "catalog schema IPC {context} metadata is missing its value key=[{key}]"
                ))
            })?;
            self.metadata_entries = self.metadata_entries.checked_add(1).ok_or_else(|| {
                schema_budget_exhausted("catalog schema metadata entry count overflow")
            })?;
            if self.metadata_entries > MAX_SCHEMA_METADATA_ENTRIES {
                return Err(schema_budget_exhausted(format!(
                    "catalog schema metadata entry count exceeds maximum actual=[{}] maximum=[{}]",
                    self.metadata_entries, MAX_SCHEMA_METADATA_ENTRIES
                )));
            }
            let entry_bytes = key.len().checked_add(value.len()).ok_or_else(|| {
                schema_budget_exhausted("catalog schema metadata byte count overflow")
            })?;
            self.metadata_bytes =
                self.metadata_bytes
                    .checked_add(entry_bytes)
                    .ok_or_else(|| {
                        schema_budget_exhausted("catalog schema metadata byte count overflow")
                    })?;
            if self.metadata_bytes > MAX_SCHEMA_METADATA_BYTES {
                return Err(schema_budget_exhausted(format!(
                    "catalog schema metadata bytes exceed maximum actual=[{}] maximum=[{}]",
                    self.metadata_bytes, MAX_SCHEMA_METADATA_BYTES
                )));
            }
        }
        Ok(())
    }
}

fn validate_dictionary_encoding(field: arrow::ipc::Field<'_>, name: &str) -> Result<()> {
    let Some(dictionary) = field.dictionary() else {
        return Ok(());
    };
    let index = dictionary.indexType().ok_or_else(|| {
        invalid_schema_ipc(format!(
            "catalog schema IPC dictionary is missing its index type field=[{name}]"
        ))
    })?;
    if !matches!(index.bitWidth(), 8 | 16 | 32 | 64) {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC dictionary has an unsupported index width field=[{name}] width=[{}]",
            index.bitWidth()
        )));
    }
    Ok(())
}

fn validate_field_shape(field: arrow::ipc::Field<'_>, children: usize, name: &str) -> Result<()> {
    use arrow::ipc::Type;

    let field_type = field.type_type();
    let expected = match field_type {
        Type::List | Type::LargeList | Type::FixedSizeList | Type::Map => Some(1),
        Type::RunEndEncoded => Some(2),
        Type::Struct_ | Type::Union => None,
        Type::ListView | Type::LargeListView => {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC uses an Arrow type unsupported by the current reader field=[{name}] type=[{field_type:?}]"
            )));
        }
        Type::Null
        | Type::Int
        | Type::FloatingPoint
        | Type::Binary
        | Type::Utf8
        | Type::Bool
        | Type::Decimal
        | Type::Date
        | Type::Time
        | Type::Timestamp
        | Type::Interval
        | Type::FixedSizeBinary
        | Type::Duration
        | Type::LargeBinary
        | Type::LargeUtf8
        | Type::BinaryView
        | Type::Utf8View => Some(0),
        _ => {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC field has an unknown type field=[{name}] type=[{field_type:?}]"
            )));
        }
    };
    if let Some(expected) = expected {
        if children != expected {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC field has an invalid child count field=[{name}] type=[{field_type:?}] actual=[{children}] expected=[{expected}]"
            )));
        }
    }
    if field_type == Type::Union {
        validate_union_type_ids(field, name, children)?;
    }
    Ok(())
}

fn validate_union_type_ids(
    field: arrow::ipc::Field<'_>,
    name: &str,
    children: usize,
) -> Result<()> {
    let union = field.type_as_union().ok_or_else(|| {
        invalid_schema_ipc(format!(
            "catalog schema IPC union is missing its type metadata field=[{name}]"
        ))
    })?;
    if !matches!(
        union.mode(),
        arrow::ipc::UnionMode::Sparse | arrow::ipc::UnionMode::Dense
    ) {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC union has an unknown mode field=[{name}] mode=[{:?}]",
            union.mode()
        )));
    }

    let Some(type_ids) = union.typeIds() else {
        if children > i8::MAX as usize {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC union has too many implicit type ids field=[{name}] children=[{children}] maximum=[{}]",
                i8::MAX
            )));
        }
        return Ok(());
    };
    if type_ids.len() != children {
        return Err(invalid_schema_ipc(format!(
            "catalog schema IPC union type id count does not match its children field=[{name}] ids=[{}] children=[{children}]",
            type_ids.len()
        )));
    }

    let mut seen = [false; i8::MAX as usize + 1];
    for type_id in type_ids.iter() {
        let index = usize::try_from(type_id).map_err(|_| {
            invalid_schema_ipc(format!(
                "catalog schema IPC union type id is outside the Arrow domain field=[{name}] type_id=[{type_id}]"
            ))
        })?;
        let Some(already_seen) = seen.get_mut(index) else {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC union type id is outside the Arrow domain field=[{name}] type_id=[{type_id}]"
            )));
        };
        if std::mem::replace(already_seen, true) {
            return Err(invalid_schema_ipc(format!(
                "catalog schema IPC union type ids must be unique field=[{name}] type_id=[{type_id}]"
            )));
        }
    }
    Ok(())
}

fn schema_budget_exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
}

fn read_ipc_message_length(payload: &[u8], offset: &mut usize, context: &str) -> Result<usize> {
    let first = read_ipc_i32(payload, offset, context)?;
    let length = if first == -1 {
        read_ipc_i32(payload, offset, context)?
    } else {
        first
    };
    usize::try_from(length).map_err(|_| {
        invalid_schema_ipc(format!(
            "catalog schema IPC {context} has a negative metadata length actual=[{length}]"
        ))
    })
}

fn read_ipc_i32(payload: &[u8], offset: &mut usize, context: &str) -> Result<i32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| invalid_schema_ipc("catalog schema IPC framing offset overflow"))?;
    let bytes: [u8; 4] = payload
        .get(*offset..end)
        .ok_or_else(|| {
            invalid_schema_ipc(format!(
                "catalog schema IPC is missing the explicit {context} framing"
            ))
        })?
        .try_into()
        .expect("four-byte IPC frame header");
    *offset = end;
    Ok(i32::from_le_bytes(bytes))
}

fn invalid_schema_ipc(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message)
}

fn persisted_schema_corruption(
    descriptor: &SchemaDescriptor,
    message: impl Into<String>,
) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!(
            "{} table_id=[{}] schema_version=[{}]",
            message.into(),
            descriptor.table_id,
            descriptor.schema_version
        ),
    )
    .with_table_id(descriptor.table_id)
    .with_retryable(false)
}

/// Computes the durable logical schema hash from the frozen V1 encoding.
///
/// Physical verifiers use this same function to bind the Arrow Schema observed in a data-file
/// footer to the Schema later admitted by the Catalog.
pub fn canonical_schema_hash(schema: &SchemaRef) -> Result<String> {
    Ok(canonical_catalog_schema_hash_v1(schema.as_ref()))
}

/// Decodes a schema-only Arrow IPC stream and returns its canonical logical hash.
pub fn canonical_schema_hash_from_ipc(schema_ipc: &SchemaIpcPayload) -> Result<String> {
    let schema = decode_schema_ipc_stream(schema_ipc.as_bytes())?;
    canonical_schema_hash(&schema)
}

/// Encodes a table definition through the frozen V1 storage DTO.
pub(crate) fn encode_table_definition_blob(value: &TableDefinition) -> Result<Vec<u8>> {
    encode_versioned_json(
        TABLE_DEFINITION_BLOB_KIND,
        &StoredTableDefinitionV1::from(value),
    )
}

/// Decodes a table definition through the historical version dispatcher.
pub(crate) fn decode_table_definition_blob(encoded: &[u8]) -> Result<TableDefinition> {
    decode_versioned_json::<StoredTableDefinitionV1>(TABLE_DEFINITION_BLOB_KIND, encoded)?
        .try_into()
}

/// Encodes table configuration through the frozen V1 storage DTO.
pub(crate) fn encode_table_config_blob(value: &BTreeMap<String, String>) -> Result<Vec<u8>> {
    encode_versioned_json(TABLE_CONFIG_BLOB_KIND, &StoredTableConfigV1(value.clone()))
}

/// Decodes table configuration through the historical version dispatcher.
pub(crate) fn decode_table_config_blob(encoded: &[u8]) -> Result<BTreeMap<String, String>> {
    Ok(decode_versioned_json::<StoredTableConfigV1>(TABLE_CONFIG_BLOB_KIND, encoded)?.0)
}

/// Encodes snapshot summary metadata through the frozen V1 storage DTO.
pub(crate) fn encode_snapshot_summary_blob(value: &MetadataMap) -> Result<Vec<u8>> {
    encode_metadata_map_blob(SNAPSHOT_SUMMARY_BLOB_KIND, value)
}

/// Decodes snapshot summary metadata through the historical version dispatcher.
pub(crate) fn decode_snapshot_summary_blob(encoded: &[u8]) -> Result<MetadataMap> {
    decode_metadata_map_blob(SNAPSHOT_SUMMARY_BLOB_KIND, encoded)
}

/// Encodes data-file partition metadata through the frozen V1 storage DTO.
pub(crate) fn encode_data_file_partition_blob(value: &MetadataMap) -> Result<Vec<u8>> {
    encode_metadata_map_blob(DATA_FILE_PARTITION_BLOB_KIND, value)
}

/// Decodes data-file partition metadata through the historical version dispatcher.
pub(crate) fn decode_data_file_partition_blob(encoded: &[u8]) -> Result<MetadataMap> {
    decode_metadata_map_blob(DATA_FILE_PARTITION_BLOB_KIND, encoded)
}

/// Encodes data-file statistics through the frozen V1 storage DTO.
pub(crate) fn encode_data_file_statistics_blob(value: &MetadataMap) -> Result<Vec<u8>> {
    encode_metadata_map_blob(DATA_FILE_STATISTICS_BLOB_KIND, value)
}

/// Decodes data-file statistics through the historical version dispatcher.
pub(crate) fn decode_data_file_statistics_blob(encoded: &[u8]) -> Result<MetadataMap> {
    decode_metadata_map_blob(DATA_FILE_STATISTICS_BLOB_KIND, encoded)
}

/// Encodes data-file metadata through the frozen V1 storage DTO.
pub(crate) fn encode_data_file_metadata_blob(value: &MetadataMap) -> Result<Vec<u8>> {
    encode_metadata_map_blob(DATA_FILE_METADATA_BLOB_KIND, value)
}

/// Decodes data-file metadata through the historical version dispatcher.
pub(crate) fn decode_data_file_metadata_blob(encoded: &[u8]) -> Result<MetadataMap> {
    decode_metadata_map_blob(DATA_FILE_METADATA_BLOB_KIND, encoded)
}

fn encode_metadata_map_blob(kind: &str, value: &MetadataMap) -> Result<Vec<u8>> {
    encode_bounded_metadata_json(kind, &StoredSnapshotSummaryRefV1(value))
}

fn decode_metadata_map_blob(kind: &str, encoded: &[u8]) -> Result<MetadataMap> {
    Ok(MetadataMap::from(decode_versioned_json::<
        StoredSnapshotSummaryV1,
    >(kind, encoded)?))
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "definition")]
enum StoredTableDefinitionV1 {
    #[serde(rename = "append_table")]
    Append(StoredAppendDefinitionV1),
    #[serde(rename = "frame_table")]
    Frame(StoredFrameDefinitionV1),
    #[serde(rename = "key_value_table")]
    KeyValue(StoredKeyValueDefinitionV1),
}

#[derive(Serialize, Deserialize)]
struct StoredAppendDefinitionV1 {
    event_time_column: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredFrameDefinitionV1 {
    by: String,
}

#[derive(Serialize, Deserialize)]
struct StoredKeyValueDefinitionV1 {
    keys: Vec<String>,
}

impl From<&TableDefinition> for StoredTableDefinitionV1 {
    fn from(value: &TableDefinition) -> Self {
        match value {
            TableDefinition::Append(definition) => Self::Append(StoredAppendDefinitionV1 {
                event_time_column: definition
                    .event_time_column
                    .as_ref()
                    .map(|column| column.as_str().to_owned()),
            }),
            TableDefinition::Frame(definition) => Self::Frame(StoredFrameDefinitionV1 {
                by: definition.by.as_str().to_owned(),
            }),
            TableDefinition::KeyValue(definition) => Self::KeyValue(StoredKeyValueDefinitionV1 {
                keys: definition
                    .keys()
                    .as_slice()
                    .iter()
                    .map(|column| column.as_str().to_owned())
                    .collect(),
            }),
        }
    }
}

impl TryFrom<StoredTableDefinitionV1> for TableDefinition {
    type Error = ServerError;

    fn try_from(value: StoredTableDefinitionV1) -> Result<Self> {
        match value {
            StoredTableDefinitionV1::Append(definition) => {
                let event_time_column = definition
                    .event_time_column
                    .map(ColumnName::new)
                    .transpose()
                    .map_err(|error| {
                        corruption(format!(
                            "catalog stored append definition is invalid error=[{error}]"
                        ))
                    })?;
                Ok(Self::Append(AppendTableDefinition { event_time_column }))
            }
            StoredTableDefinitionV1::Frame(definition) => {
                let by = ColumnName::new(definition.by).map_err(|error| {
                    corruption(format!(
                        "catalog stored frame definition is invalid error=[{error}]"
                    ))
                })?;
                Ok(Self::Frame(FrameTableDefinition { by }))
            }
            StoredTableDefinitionV1::KeyValue(definition) => {
                let keys = definition
                    .keys
                    .into_iter()
                    .map(ColumnName::new)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|error| {
                        corruption(format!(
                            "catalog stored key-value definition is invalid error=[{error}]"
                        ))
                    })?;
                let keys = ColumnSet::new(keys).map_err(|error| {
                    corruption(format!(
                        "catalog stored key-value columns are invalid error=[{error}]"
                    ))
                })?;
                let definition = KeyValueTableDefinition::new(keys).map_err(|error| {
                    corruption(format!(
                        "catalog stored key-value definition is invalid error=[{error}]"
                    ))
                })?;
                Ok(Self::KeyValue(definition))
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct StoredTableConfigV1(BTreeMap<String, String>);

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct StoredSnapshotSummaryV1(BTreeMap<String, StoredMetadataScalarV1>);

struct StoredSnapshotSummaryRefV1<'a>(&'a MetadataMap);

impl Serialize for StoredSnapshotSummaryRefV1<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            map.serialize_entry(key, &StoredMetadataScalarRefV1::from(value))?;
        }
        map.end()
    }
}

impl From<StoredSnapshotSummaryV1> for MetadataMap {
    fn from(value: StoredSnapshotSummaryV1) -> Self {
        value
            .0
            .into_iter()
            .map(|(key, scalar)| (key, MetadataScalar::from(scalar)))
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum StoredMetadataScalarV1 {
    Null,
    Boolean(bool),
    Int64(i64),
    #[serde(rename = "uint64")]
    UInt64(u64),
    Float64(StoredFiniteMetadataFloatV1),
    String(String),
    Bytes(Vec<u8>),
}

#[derive(Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum StoredMetadataScalarRefV1<'a> {
    Null,
    Boolean(&'a bool),
    Int64(&'a i64),
    #[serde(rename = "uint64")]
    UInt64(&'a u64),
    Float64(StoredFiniteMetadataFloatV1),
    String(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> From<&'a MetadataScalar> for StoredMetadataScalarRefV1<'a> {
    fn from(value: &'a MetadataScalar) -> Self {
        match value {
            MetadataScalar::Null => Self::Null,
            MetadataScalar::Boolean(value) => Self::Boolean(value),
            MetadataScalar::Int64(value) => Self::Int64(value),
            MetadataScalar::UInt64(value) => Self::UInt64(value),
            MetadataScalar::Float64(value) => {
                Self::Float64(StoredFiniteMetadataFloatV1::new(value.get()))
            }
            MetadataScalar::String(value) => Self::String(value),
            MetadataScalar::Bytes(value) => Self::Bytes(value),
        }
    }
}

impl From<StoredMetadataScalarV1> for MetadataScalar {
    fn from(value: StoredMetadataScalarV1) -> Self {
        match value {
            StoredMetadataScalarV1::Null => Self::Null,
            StoredMetadataScalarV1::Boolean(value) => Self::Boolean(value),
            StoredMetadataScalarV1::Int64(value) => Self::Int64(value),
            StoredMetadataScalarV1::UInt64(value) => Self::UInt64(value),
            StoredMetadataScalarV1::Float64(value) => Self::Float64(
                FiniteMetadataFloat::new(value.get()).expect("stored float is finite"),
            ),
            StoredMetadataScalarV1::String(value) => Self::String(value),
            StoredMetadataScalarV1::Bytes(value) => Self::Bytes(value),
        }
    }
}

#[derive(Clone, Copy)]
struct StoredFiniteMetadataFloatV1(u64);

impl StoredFiniteMetadataFloatV1 {
    fn new(value: f64) -> Self {
        let canonical = if value == 0.0 { 0.0 } else { value };
        Self(canonical.to_bits())
    }

    fn get(&self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl Serialize for StoredFiniteMetadataFloatV1 {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.get())
    }
}

impl<'de> Deserialize<'de> for StoredFiniteMetadataFloatV1 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        if !value.is_finite() {
            return Err(serde::de::Error::custom(
                "stored metadata float must be finite",
            ));
        }
        Ok(Self::new(value))
    }
}

fn encode_versioned_json<T>(kind: &str, value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let payload = canonical_json(value).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("encode catalog typed BLOB kind=[{kind}] error=[{error}]"),
        )
    })?;
    if payload.len() > MAX_TYPED_JSON_BYTES {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "catalog typed BLOB exceeds limit kind=[{}] actual_bytes=[{}] maximum_bytes=[{}]",
                kind,
                payload.len(),
                MAX_TYPED_JSON_BYTES
            ),
        ));
    }
    let checksum = typed_json_checksum(kind, &payload);
    let mut encoded = Vec::with_capacity(TYPED_JSON_HEADER_BYTES + payload.len());
    encoded.extend_from_slice(TYPED_JSON_PREFIX);
    encoded.extend_from_slice(&checksum);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn encode_bounded_metadata_json<T>(kind: &str, value: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    let mut writer = BoundedJsonWriter::new(MAX_TYPED_JSON_BYTES);
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if let Some(actual) = writer.limit_exceeded_at {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "catalog typed BLOB exceeds limit kind=[{kind}] actual_bytes_at_least=[{actual}] maximum_bytes=[{MAX_TYPED_JSON_BYTES}]"
                ),
            ));
        }
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!("encode catalog typed BLOB kind=[{kind}] error=[{error}]"),
        ));
    }
    Ok(writer.into_typed_blob(kind))
}

struct BoundedJsonWriter {
    chunks: Vec<Vec<u8>>,
    length: usize,
    maximum: usize,
    limit_exceeded_at: Option<usize>,
}

impl BoundedJsonWriter {
    fn new(maximum: usize) -> Self {
        Self {
            chunks: Vec::new(),
            length: 0,
            maximum,
            limit_exceeded_at: None,
        }
    }

    fn into_typed_blob(self, kind: &str) -> Vec<u8> {
        let checksum =
            typed_json_checksum_parts(kind, self.chunks.iter().map(|chunk| chunk.as_slice()));
        let mut encoded = Vec::with_capacity(TYPED_JSON_HEADER_BYTES + self.length);
        encoded.extend_from_slice(TYPED_JSON_PREFIX);
        encoded.extend_from_slice(&checksum);
        for chunk in self.chunks {
            encoded.extend_from_slice(&chunk);
        }
        encoded
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, mut input: &[u8]) -> std::io::Result<usize> {
        let input_len = input.len();
        let next = self.length.checked_add(input_len).ok_or_else(|| {
            self.limit_exceeded_at = Some(usize::MAX);
            std::io::Error::other("catalog typed JSON byte count overflow")
        })?;
        if next > self.maximum {
            self.limit_exceeded_at = Some(next);
            return Err(std::io::Error::other(
                "catalog typed JSON byte limit exceeded",
            ));
        }
        while !input.is_empty() {
            let needs_chunk = self
                .chunks
                .last()
                .is_none_or(|chunk| chunk.len() == TYPED_JSON_CHUNK_BYTES);
            if needs_chunk {
                let remaining = self.maximum - self.length;
                self.chunks
                    .push(Vec::with_capacity(TYPED_JSON_CHUNK_BYTES.min(remaining)));
            }
            let chunk = self
                .chunks
                .last_mut()
                .expect("nonempty input creates a JSON chunk");
            let available = TYPED_JSON_CHUNK_BYTES - chunk.len();
            let copied = available.min(input.len());
            chunk.extend_from_slice(&input[..copied]);
            input = &input[copied..];
            self.length += copied;
        }
        Ok(input_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn decode_versioned_json<T>(kind: &str, encoded: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    if encoded.len() < TYPED_JSON_PREFIX.len() || &encoded[..3] != TYPED_JSON_MAGIC {
        return Err(corruption(format!(
            "catalog typed BLOB header is invalid kind=[{kind}]"
        )));
    }
    match encoded[3] {
        TYPED_JSON_V1 => decode_typed_json_v1(kind, encoded),
        version => Err(corruption(format!(
            "catalog typed BLOB version is unsupported kind=[{kind}] version=[{version}]"
        ))),
    }
}

fn decode_typed_json_v1<T>(kind: &str, encoded: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    if encoded.len() < TYPED_JSON_HEADER_BYTES || &encoded[..4] != TYPED_JSON_PREFIX {
        return Err(corruption(format!(
            "catalog typed BLOB V1 header is invalid kind=[{kind}]"
        )));
    }
    let checksum = &encoded[TYPED_JSON_PREFIX.len()..TYPED_JSON_HEADER_BYTES];
    let payload = &encoded[TYPED_JSON_HEADER_BYTES..];
    if payload.len() > MAX_TYPED_JSON_BYTES {
        return Err(corruption(format!(
            "catalog typed BLOB exceeds limit kind=[{}] actual_bytes=[{}] maximum_bytes=[{}]",
            kind,
            payload.len(),
            MAX_TYPED_JSON_BYTES
        )));
    }
    let expected = typed_json_checksum(kind, payload);
    if checksum != expected {
        return Err(corruption(format!(
            "catalog typed BLOB checksum mismatch kind=[{kind}]"
        )));
    }
    let value: T = serde_json::from_slice(payload).map_err(|error| {
        corruption(format!(
            "decode catalog typed BLOB kind=[{kind}] error=[{error}]"
        ))
    })?;
    let canonical = canonical_json(&value).map_err(|error| {
        corruption(format!(
            "re-encode catalog typed BLOB kind=[{kind}] error=[{error}]"
        ))
    })?;
    if canonical != payload {
        return Err(corruption(format!(
            "catalog typed BLOB is not canonical kind=[{kind}]"
        )));
    }
    Ok(value)
}

fn canonical_json<T>(value: &T) -> serde_json::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(&sort_json_value(serde_json::to_value(value)?))
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(values) => {
            let mut entries = values.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_json_value(value)))
                    .collect(),
            )
        }
        scalar => scalar,
    }
}

fn typed_json_checksum(kind: &str, payload: &[u8]) -> [u8; SHA256_BYTES] {
    typed_json_checksum_parts(kind, std::iter::once(payload))
}

fn typed_json_checksum_parts<'a>(
    kind: &str,
    payload: impl IntoIterator<Item = &'a [u8]>,
) -> [u8; SHA256_BYTES] {
    let mut digest = Sha256::new();
    digest.update(TYPED_JSON_HASH_DOMAIN);
    digest.update(kind.as_bytes());
    digest.update([0]);
    for part in payload {
        digest.update(part);
    }
    digest.finalize().into()
}

fn corruption(message: String) -> ServerError {
    ServerError::new(ErrorCode::DataCorruption, message)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
    use arrow::ipc::convert::IpcSchemaEncoder;
    use arrow::ipc::writer::StreamWriter;
    use zippy_types::table::{
        AppendTableDefinition, CatalogName, ColumnName, ColumnSet, FiniteMetadataFloat,
        KeyValueTableDefinition, MetadataMap, MetadataScalar, SchemaCompatibility,
        SchemaDescriptor, SchemaIpcPayload, TableDefinition, TableDescriptor, TableState,
    };
    use zippy_types::{ErrorCode, NamespaceId, TableId};

    use crate::{decode_persisted_schema, decode_persisted_table_schema};

    use super::{
        canonical_schema_hash, decode_data_file_metadata_blob, decode_data_file_partition_blob,
        decode_data_file_statistics_blob, decode_schema_ipc_stream, decode_snapshot_summary_blob,
        decode_table_config_blob, decode_table_definition_blob, decode_versioned_json,
        encode_data_file_metadata_blob, encode_data_file_partition_blob,
        encode_data_file_statistics_blob, encode_snapshot_summary_blob, encode_table_config_blob,
        encode_table_definition_blob, typed_json_checksum, StoredTableConfigV1,
        StoredTableDefinitionV1, DATA_FILE_METADATA_BLOB_KIND,
        MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES, MAX_SCHEMA_METADATA_BYTES,
        MAX_SCHEMA_METADATA_ENTRIES, MAX_SCHEMA_NESTING_DEPTH, MAX_SCHEMA_TOTAL_FIELDS,
        MAX_TYPED_JSON_BYTES, TABLE_CONFIG_BLOB_KIND, TYPED_JSON_HEADER_BYTES, TYPED_JSON_PREFIX,
    };

    fn schema_payload(schema: &Schema, include_batch: bool) -> Vec<u8> {
        let schema = Arc::new(schema.clone());
        let mut payload = Vec::new();
        let mut writer = StreamWriter::try_new(&mut payload, &schema).unwrap();
        if include_batch {
            writer.write(&RecordBatch::new_empty(schema)).unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        payload
    }

    fn persisted_schema_descriptor(schema: &Schema) -> SchemaDescriptor {
        let schema = Arc::new(schema.clone());
        SchemaDescriptor {
            table_id: TableId::from_u128(1).unwrap(),
            schema_version: 1,
            schema_ipc: SchemaIpcPayload::new(schema_payload(schema.as_ref(), false)).unwrap(),
            schema_hash: canonical_schema_hash(&schema).unwrap(),
            compatibility: SchemaCompatibility::Initial,
            created_at_ns: 1,
        }
    }

    fn first_message_end(payload: &[u8]) -> usize {
        let first = i32::from_le_bytes(payload[..4].try_into().unwrap());
        let (header, metadata_len) = if first == -1 {
            (8, i32::from_le_bytes(payload[4..8].try_into().unwrap()))
        } else {
            (4, first)
        };
        header + usize::try_from(metadata_len).unwrap()
    }

    fn schema_payload_with_body_length(schema: &Schema, body_length: i64) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let schema = IpcSchemaEncoder::new()
            .schema_to_fb_offset(&mut builder, schema)
            .as_union_value();
        let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
        message.add_version(arrow::ipc::MetadataVersion::V5);
        message.add_header_type(arrow::ipc::MessageHeader::Schema);
        message.add_header(schema);
        message.add_bodyLength(body_length);
        let message = message.finish();
        builder.finish(message, None);

        let metadata = builder.finished_data();
        let metadata_len = i32::try_from(metadata.len()).unwrap();
        let mut payload = Vec::new();
        payload.extend_from_slice(&(-1_i32).to_le_bytes());
        payload.extend_from_slice(&metadata_len.to_le_bytes());
        payload.extend_from_slice(metadata);
        payload.extend_from_slice(&(-1_i32).to_le_bytes());
        payload.extend_from_slice(&0_i32.to_le_bytes());
        payload
    }

    fn raw_null_field_schema_payload(
        field_count: usize,
        field_name: Option<&str>,
        include_type: bool,
        include_fields: bool,
    ) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let name = field_name.map(|name| builder.create_string(name));
        let null_type = include_type.then(|| {
            arrow::ipc::NullBuilder::new(&mut builder)
                .finish()
                .as_union_value()
        });
        let field = arrow::ipc::Field::create(
            &mut builder,
            &arrow::ipc::FieldArgs {
                name,
                type_type: arrow::ipc::Type::Null,
                type_: null_type,
                ..Default::default()
            },
        );
        let fields = include_fields.then(|| builder.create_vector(&vec![field; field_count]));
        let schema = arrow::ipc::Schema::create(
            &mut builder,
            &arrow::ipc::SchemaArgs {
                fields,
                ..Default::default()
            },
        )
        .as_union_value();
        let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
        message.add_version(arrow::ipc::MetadataVersion::V5);
        message.add_header_type(arrow::ipc::MessageHeader::Schema);
        message.add_header(schema);
        let message = message.finish();
        builder.finish(message, None);

        let metadata = builder.finished_data();
        let metadata_len = i32::try_from(metadata.len()).unwrap();
        let mut payload = Vec::with_capacity(metadata.len() + 16);
        payload.extend_from_slice(&(-1_i32).to_le_bytes());
        payload.extend_from_slice(&metadata_len.to_le_bytes());
        payload.extend_from_slice(metadata);
        payload.extend_from_slice(&(-1_i32).to_le_bytes());
        payload.extend_from_slice(&0_i32.to_le_bytes());
        payload
    }

    #[test]
    fn typed_json_v1_golden_round_trips_and_rejects_tampering_or_wrong_kind() {
        const GOLDEN_CHECKSUM: [u8; 32] = [
            0xa4, 0x19, 0x8c, 0x4c, 0x22, 0xa1, 0x5f, 0x14, 0xf8, 0x18, 0x2f, 0xdc, 0x65, 0xe0,
            0x86, 0xf0, 0x86, 0x29, 0xa8, 0xba, 0x19, 0x31, 0x90, 0x34, 0x6a, 0x51, 0xdb, 0xc3,
            0x39, 0xf8, 0xe8, 0x7d,
        ];
        const GOLDEN_PAYLOAD: &[u8] =
            br#"{"definition":{"event_time_column":null},"kind":"append_table"}"#;

        let definition = TableDefinition::Append(AppendTableDefinition::default());
        let encoded = encode_table_definition_blob(&definition).unwrap();
        assert_eq!(&encoded[..TYPED_JSON_PREFIX.len()], TYPED_JSON_PREFIX);
        assert_eq!(
            &encoded[TYPED_JSON_PREFIX.len()..TYPED_JSON_HEADER_BYTES],
            GOLDEN_CHECKSUM
        );
        assert_eq!(&encoded[TYPED_JSON_HEADER_BYTES..], GOLDEN_PAYLOAD);
        assert_eq!(decode_table_definition_blob(&encoded).unwrap(), definition);
        let mut tampered = encoded.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_table_definition_blob(&tampered).unwrap_err().code(),
            ErrorCode::DataCorruption
        );
        let wrong_kind =
            decode_versioned_json::<StoredTableDefinitionV1>(TABLE_CONFIG_BLOB_KIND, &encoded)
                .err()
                .expect("kind-bound checksum must reject a table definition as config");
        assert_eq!(wrong_kind.code(), ErrorCode::DataCorruption);

        let mut future = encoded;
        future[3] = 2;
        assert_eq!(
            decode_table_definition_blob(&future).unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn frozen_config_definition_and_summary_wrappers_round_trip() {
        let config = BTreeMap::from([
            ("durability".to_owned(), "fsync".to_owned()),
            ("retention".to_owned(), "none".to_owned()),
        ]);
        let encoded_config = encode_table_config_blob(&config).unwrap();
        assert_eq!(decode_table_config_blob(&encoded_config).unwrap(), config);

        let keys = ColumnSet::new(vec![ColumnName::new("symbol").unwrap()]).unwrap();
        let definition = TableDefinition::KeyValue(KeyValueTableDefinition::new(keys).unwrap());
        let encoded_definition = encode_table_definition_blob(&definition).unwrap();
        assert_eq!(
            decode_table_definition_blob(&encoded_definition).unwrap(),
            definition
        );

        let summary = MetadataMap::from([
            ("bytes".to_owned(), MetadataScalar::Bytes(vec![1, 2, 3])),
            (
                "float".to_owned(),
                MetadataScalar::Float64(FiniteMetadataFloat::new(1.5).unwrap()),
            ),
            ("int".to_owned(), MetadataScalar::Int64(-7)),
            ("null".to_owned(), MetadataScalar::Null),
            ("uint".to_owned(), MetadataScalar::UInt64(u64::MAX)),
        ]);
        let encoded_summary = encode_snapshot_summary_blob(&summary).unwrap();
        assert_eq!(
            decode_snapshot_summary_blob(&encoded_summary).unwrap(),
            summary
        );
    }

    #[test]
    fn metadata_blob_v1_golden_round_trips_and_binds_checksum_domains() {
        const GOLDEN_PAYLOAD: &[u8] = br#"{"rows":{"type":"uint64","value":7}}"#;
        const GOLDEN_CHECKSUMS: [[u8; 32]; 4] = [
            [
                0x3c, 0xfc, 0x16, 0x4e, 0x86, 0xf5, 0x64, 0xcf, 0xab, 0xc6, 0x3e, 0xfa, 0xb6, 0xa5,
                0x90, 0xb0, 0x5a, 0x8b, 0x7a, 0xa8, 0xa4, 0x30, 0xc1, 0x39, 0x81, 0x99, 0xe6, 0x85,
                0x2b, 0xcd, 0xda, 0x6a,
            ],
            [
                0x0f, 0x63, 0x31, 0xcc, 0x79, 0x70, 0xff, 0x2a, 0x2b, 0x21, 0x11, 0x3a, 0x7d, 0x76,
                0x3b, 0x28, 0xfb, 0x31, 0x2a, 0x83, 0x5a, 0x78, 0x91, 0xd0, 0xf7, 0xbb, 0x29, 0x10,
                0x75, 0x24, 0x5e, 0xd6,
            ],
            [
                0x20, 0x5b, 0xca, 0xd3, 0x39, 0x62, 0x51, 0xb5, 0x0f, 0xed, 0xa2, 0x98, 0xfb, 0x36,
                0x84, 0x5f, 0xa7, 0x4e, 0x6a, 0x62, 0x2b, 0x80, 0x83, 0x0e, 0xf8, 0xc3, 0x33, 0xfc,
                0x0d, 0x28, 0x66, 0xc0,
            ],
            [
                0x93, 0x66, 0x90, 0xde, 0x4e, 0x59, 0xed, 0x3c, 0x1b, 0x31, 0x1e, 0x9d, 0xf9, 0xcb,
                0x40, 0x45, 0xe2, 0xa8, 0x3a, 0x44, 0x6d, 0xdf, 0x1f, 0xc7, 0xaf, 0x23, 0x8d, 0x5f,
                0x78, 0x44, 0xe3, 0xfd,
            ],
        ];

        let metadata = MetadataMap::from([("rows".to_owned(), MetadataScalar::UInt64(7))]);
        let encoded = [
            encode_snapshot_summary_blob(&metadata).unwrap(),
            encode_data_file_partition_blob(&metadata).unwrap(),
            encode_data_file_statistics_blob(&metadata).unwrap(),
            encode_data_file_metadata_blob(&metadata).unwrap(),
        ];
        type MetadataBlobDecoder = fn(&[u8]) -> zippy_types::Result<MetadataMap>;
        let decoders: [MetadataBlobDecoder; 4] = [
            decode_snapshot_summary_blob,
            decode_data_file_partition_blob,
            decode_data_file_statistics_blob,
            decode_data_file_metadata_blob,
        ];

        for (index, blob) in encoded.iter().enumerate() {
            assert_eq!(&blob[..TYPED_JSON_PREFIX.len()], TYPED_JSON_PREFIX);
            assert_eq!(
                &blob[TYPED_JSON_PREFIX.len()..TYPED_JSON_HEADER_BYTES],
                GOLDEN_CHECKSUMS[index]
            );
            assert_eq!(&blob[TYPED_JSON_HEADER_BYTES..], GOLDEN_PAYLOAD);

            for (decoder_index, decoder) in decoders.iter().enumerate() {
                if decoder_index == index {
                    assert_eq!(decoder(blob).unwrap(), metadata);
                } else {
                    assert_eq!(
                        decoder(blob).unwrap_err().code(),
                        ErrorCode::DataCorruption,
                        "metadata BLOB must reject a checksum from a different kind"
                    );
                }
            }

            let mut tampered = blob.clone();
            *tampered.last_mut().unwrap() ^= 1;
            assert_eq!(
                decoders[index](&tampered).unwrap_err().code(),
                ErrorCode::DataCorruption
            );
        }
    }

    #[test]
    fn metadata_blob_v1_rejects_oversized_encode_and_decode_payloads() {
        let oversized = MetadataMap::from([(
            "oversized".to_owned(),
            MetadataScalar::String("x".repeat(MAX_TYPED_JSON_BYTES)),
        )]);
        assert_eq!(
            encode_data_file_metadata_blob(&oversized)
                .unwrap_err()
                .code(),
            ErrorCode::ResourceExhausted
        );

        let escaped = MetadataMap::from([(
            "escaped".to_owned(),
            MetadataScalar::String("\0".repeat(MAX_TYPED_JSON_BYTES / 2)),
        )]);
        assert_eq!(
            encode_data_file_metadata_blob(&escaped).unwrap_err().code(),
            ErrorCode::ResourceExhausted
        );

        let payload = vec![b'0'; MAX_TYPED_JSON_BYTES + 1];
        let checksum = typed_json_checksum(DATA_FILE_METADATA_BLOB_KIND, &payload);
        let mut encoded = Vec::with_capacity(TYPED_JSON_HEADER_BYTES + payload.len());
        encoded.extend_from_slice(TYPED_JSON_PREFIX);
        encoded.extend_from_slice(&checksum);
        encoded.extend_from_slice(&payload);
        assert_eq!(
            decode_data_file_metadata_blob(&encoded).unwrap_err().code(),
            ErrorCode::DataCorruption
        );
    }

    #[test]
    fn typed_json_decoder_rejects_noncanonical_json_with_a_valid_checksum() {
        let payload = br#"{ "b": "2", "a": "1" }"#;
        let checksum = typed_json_checksum("table_config", payload);
        let mut encoded = Vec::new();
        encoded.extend_from_slice(TYPED_JSON_PREFIX);
        encoded.extend_from_slice(&checksum);
        encoded.extend_from_slice(payload);
        assert!(encoded.len() >= TYPED_JSON_HEADER_BYTES);
        let noncanonical =
            decode_versioned_json::<StoredTableConfigV1>(TABLE_CONFIG_BLOB_KIND, &encoded)
                .err()
                .expect("noncanonical JSON must be rejected");
        assert_eq!(noncanonical.code(), ErrorCode::DataCorruption);
    }

    #[test]
    fn canonical_schema_hash_has_simple_nested_and_metadata_golden_values() {
        let simple = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        assert_eq!(
            canonical_schema_hash(&simple).unwrap(),
            "sha256-v1:589bb89a896147ac3c2cde1cfa6765e796a39a69ab6dffb64beee35be8443811"
        );

        let level = Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("price", DataType::Decimal128(18, 4), false),
                    Field::new("quantity", DataType::UInt64, false),
                ]
                .into(),
            ),
            true,
        );
        let nested = Arc::new(Schema::new(vec![
            Field::new("levels", DataType::List(Arc::new(level)), true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        assert_eq!(
            canonical_schema_hash(&nested).unwrap(),
            "sha256-v1:aef67dd500dca797bd73d275506435ccd714d2271b0c6d8801f897faae2e0037"
        );

        let metadata_field =
            Field::new("symbol", DataType::Utf8, false).with_metadata(HashMap::from([
                ("encoding".to_owned(), "utf8".to_owned()),
                ("source".to_owned(), "feed".to_owned()),
            ]));
        let metadata = Arc::new(Schema::new_with_metadata(
            vec![metadata_field],
            HashMap::from([
                ("version".to_owned(), "1".to_owned()),
                ("owner".to_owned(), "market-data".to_owned()),
            ]),
        ));
        assert_eq!(
            canonical_schema_hash(&metadata).unwrap(),
            "sha256-v1:791d33da62dd39a6e234bfde483bb8925182973bdb2231219c403e398adcdfb5"
        );
    }

    #[test]
    fn canonical_schema_hash_sorts_schema_and_field_metadata() {
        let first_field =
            Field::new("price", DataType::Float64, false).with_metadata(HashMap::from([
                ("unit".to_owned(), "cny".to_owned()),
                ("source".to_owned(), "feed".to_owned()),
            ]));
        let second_field =
            Field::new("price", DataType::Float64, false).with_metadata(HashMap::from([
                ("source".to_owned(), "feed".to_owned()),
                ("unit".to_owned(), "cny".to_owned()),
            ]));
        let first = Arc::new(Schema::new_with_metadata(
            vec![first_field],
            HashMap::from([
                ("source".to_owned(), "feed".to_owned()),
                ("version".to_owned(), "1".to_owned()),
            ]),
        ));
        let second = Arc::new(Schema::new_with_metadata(
            vec![second_field],
            HashMap::from([
                ("version".to_owned(), "1".to_owned()),
                ("source".to_owned(), "feed".to_owned()),
            ]),
        ));
        assert_eq!(
            canonical_schema_hash(&first).unwrap(),
            canonical_schema_hash(&second).unwrap()
        );
    }

    #[test]
    fn schema_decoder_rejects_batches_trailing_bytes_and_malformed_streams() {
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        assert_eq!(
            decode_schema_ipc_stream(&schema_payload(&schema, true))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        let mut trailing = schema_payload(&schema, false);
        trailing.push(0);
        assert_eq!(
            decode_schema_ipc_stream(&trailing).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            decode_schema_ipc_stream(&[1]).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );

        let mut missing_eos = schema_payload(&schema, false);
        missing_eos.truncate(missing_eos.len() - 8);
        assert_eq!(
            decode_schema_ipc_stream(&missing_eos).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );

        let batch_stream = schema_payload(&schema, true);
        let batch_first = first_message_end(&batch_stream);
        assert_eq!(
            decode_schema_ipc_stream(&batch_stream[batch_first..])
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );

        let nonzero_body = schema_payload_with_body_length(&schema, 1);
        assert_eq!(
            decode_schema_ipc_stream(&nonzero_body).unwrap_err().code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn public_persisted_schema_decoder_verifies_payload_and_hash() {
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        let descriptor = persisted_schema_descriptor(&schema);
        assert_eq!(
            decode_persisted_schema(&descriptor).unwrap().as_ref(),
            &schema
        );

        let mut wrong_hash = descriptor.clone();
        wrong_hash.schema_hash = "sha256-v1:invalid".to_owned();
        let error = decode_persisted_schema(&wrong_hash).unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.retryable());
        assert_eq!(error.table_id(), Some(descriptor.table_id));
    }

    #[test]
    fn public_persisted_schema_decoder_maps_untrusted_ipc_to_corruption() {
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        let mut descriptor = persisted_schema_descriptor(&schema);
        descriptor.schema_ipc = SchemaIpcPayload::new(schema_payload(&schema, true)).unwrap();

        let error = decode_persisted_schema(&descriptor).unwrap_err();
        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.retryable());
        assert_eq!(error.table_id(), Some(descriptor.table_id));
    }

    #[test]
    fn decoded_schema_charge_accounts_for_shared_offset_expansion() {
        let payload =
            raw_null_field_schema_payload(MAX_SCHEMA_TOTAL_FIELDS, Some("reused"), true, true);
        let schema = decode_schema_ipc_stream(&payload).unwrap();
        let retained_charge = super::decoded_schema_retained_charge(schema.as_ref()).unwrap();

        assert_eq!(schema.fields().len(), MAX_SCHEMA_TOTAL_FIELDS);
        assert!(retained_charge > payload.len() as u64);
    }

    #[test]
    fn table_aware_persisted_schema_decoder_rejects_definition_mismatch() {
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        let descriptor = persisted_schema_descriptor(&schema);
        let table = TableDescriptor {
            table_id: descriptor.table_id,
            namespace_id: NamespaceId::from_u128(1).unwrap(),
            name: CatalogName::new("ticks").unwrap(),
            definition: TableDefinition::Append(AppendTableDefinition {
                event_time_column: Some(ColumnName::new("event_time").unwrap()),
            }),
            current_schema_version: 1,
            current_snapshot_version: 1,
            writer_epoch: 0,
            state: TableState::Active,
            config: BTreeMap::new(),
            created_at_ns: 1,
            updated_at_ns: 1,
        };

        let error = decode_persisted_table_schema(&table, &descriptor).unwrap_err();

        assert_eq!(error.code(), ErrorCode::DataCorruption);
        assert!(!error.retryable());
        assert_eq!(error.table_id(), Some(table.table_id));
    }

    #[test]
    fn schema_decoder_rejects_untrusted_lengths_before_arrow_allocates() {
        let declared_two_gib = [
            (-1_i32).to_le_bytes().as_slice(),
            i32::MAX.to_le_bytes().as_slice(),
        ]
        .concat();
        assert_eq!(
            decode_schema_ipc_stream(&declared_two_gib)
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );

        for negative in [-2_i32, i32::MIN] {
            assert_eq!(
                decode_schema_ipc_stream(&negative.to_le_bytes())
                    .unwrap_err()
                    .code(),
                ErrorCode::InvalidArgument
            );
        }

        let exceeds_remaining = [100_i32.to_le_bytes().as_slice(), [0_u8; 8].as_slice()].concat();
        assert_eq!(
            decode_schema_ipc_stream(&exceeds_remaining)
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn schema_decoder_rejects_repeated_offsets_before_arrow_expansion() {
        let repeated_name = "x".repeat(MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES / 16);
        let payload = raw_null_field_schema_payload(32, Some(&repeated_name), true, true);
        assert!(payload.len() < MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES / 8);
        assert_eq!(
            decode_schema_ipc_stream(&payload).unwrap_err().code(),
            ErrorCode::ResourceExhausted
        );
    }

    #[test]
    fn schema_decoder_rejects_recursive_field_and_metadata_budgets() {
        let fields =
            raw_null_field_schema_payload(MAX_SCHEMA_TOTAL_FIELDS + 1, Some("reused"), true, true);
        assert_eq!(
            decode_schema_ipc_stream(&fields).unwrap_err().code(),
            ErrorCode::ResourceExhausted
        );

        let schema_metadata = (0..MAX_SCHEMA_METADATA_ENTRIES / 2)
            .map(|index| (format!("schema_{index}"), String::new()))
            .collect();
        let field_metadata = (0..MAX_SCHEMA_METADATA_ENTRIES / 2 + 1)
            .map(|index| (format!("field_{index}"), String::new()))
            .collect();
        let metadata_entries = Schema::new_with_metadata(
            vec![Field::new("price", DataType::Float64, false).with_metadata(field_metadata)],
            schema_metadata,
        );
        assert_eq!(
            decode_schema_ipc_stream(&schema_payload(&metadata_entries, false))
                .unwrap_err()
                .code(),
            ErrorCode::ResourceExhausted
        );

        let metadata_bytes = Schema::new_with_metadata(
            vec![Field::new("price", DataType::Float64, false)],
            HashMap::from([("source".to_owned(), "x".repeat(MAX_SCHEMA_METADATA_BYTES))]),
        );
        assert_eq!(
            decode_schema_ipc_stream(&schema_payload(&metadata_bytes, false))
                .unwrap_err()
                .code(),
            ErrorCode::ResourceExhausted
        );

        let mut nested = Field::new("leaf", DataType::Int64, false);
        for depth in 0..MAX_SCHEMA_NESTING_DEPTH {
            nested = Field::new(
                format!("level_{depth}"),
                DataType::List(Arc::new(nested)),
                true,
            );
        }
        let excessive_depth = Schema::new(vec![nested]);
        assert_eq!(
            decode_schema_ipc_stream(&schema_payload(&excessive_depth, false))
                .unwrap_err()
                .code(),
            ErrorCode::ResourceExhausted
        );
    }

    #[test]
    fn schema_decoder_rejects_missing_flatbuffer_fields() {
        for payload in [
            raw_null_field_schema_payload(0, Some("unused"), true, false),
            raw_null_field_schema_payload(1, None, true, true),
            raw_null_field_schema_payload(1, Some("price"), false, true),
        ] {
            assert_eq!(
                decode_schema_ipc_stream(&payload).unwrap_err().code(),
                ErrorCode::InvalidArgument
            );
        }
    }

    #[test]
    fn schema_decoder_accepts_a_normal_complex_schema() {
        let book_level = Field::new(
            "level",
            DataType::Struct(
                vec![
                    Field::new("price", DataType::Decimal128(20, 6), false),
                    Field::new("quantity", DataType::UInt64, false),
                ]
                .into(),
            ),
            false,
        );
        let attributes = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        let event = DataType::Union(
            UnionFields::new(
                [1, 7],
                vec![
                    Field::new(
                        "trade",
                        DataType::Struct(vec![book_level.clone()].into()),
                        false,
                    ),
                    Field::new("status", DataType::Utf8, false),
                ],
            ),
            UnionMode::Dense,
        );
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new(
                    "symbol",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                ),
                Field::new("book", DataType::List(Arc::new(book_level)), true),
                Field::new(
                    "attributes",
                    DataType::Map(Arc::new(attributes), false),
                    true,
                ),
                Field::new("event", event, true),
            ],
            HashMap::from([
                ("source".to_owned(), "market-data".to_owned()),
                ("timezone".to_owned(), "UTC".to_owned()),
            ]),
        );

        let decoded = decode_schema_ipc_stream(&schema_payload(&schema, false)).unwrap();
        assert_eq!(decoded.as_ref(), &schema);
    }
}
