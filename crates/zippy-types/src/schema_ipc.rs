//! Bounded schema-only Arrow IPC decoding shared by command fingerprints and Catalog storage.

use std::io::Cursor;
use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;

use crate::schema_contract::{
    MAX_CATALOG_SCHEMA_FIELDS as MAX_SCHEMA_TOTAL_FIELDS,
    MAX_CATALOG_SCHEMA_IPC_BYTES as MAX_SCHEMA_IPC_BYTES,
    MAX_CATALOG_SCHEMA_METADATA_BYTES as MAX_SCHEMA_METADATA_BYTES,
    MAX_CATALOG_SCHEMA_METADATA_ENTRIES as MAX_SCHEMA_METADATA_ENTRIES,
    MAX_CATALOG_SCHEMA_NESTING_DEPTH as MAX_SCHEMA_NESTING_DEPTH,
};
use crate::{ErrorCode, Result, ServerError};

const MAX_SCHEMA_FLATBUFFER_DEPTH: usize = 64;
const MAX_SCHEMA_FLATBUFFER_TABLES: usize = 32_768;
const MAX_SCHEMA_FLATBUFFER_APPARENT_BYTES: usize = 16 * 1024 * 1024;

/// Decodes exactly one bounded schema-only stream, rejecting trailing data and malformed metadata.
///
/// Framing, recursive field budgets and FlatBuffer shape are checked before Arrow allocation.
pub fn decode_schema_ipc_stream(payload: &[u8]) -> Result<SchemaRef> {
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
