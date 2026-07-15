//! Conservative retained-memory accounting for shared Arrow schemas.

use std::mem::{size_of, size_of_val};
use std::num::NonZeroU64;

use arrow::datatypes::{Schema, SchemaRef};

use crate::{ErrorCode, Result, ServerError};

const SCHEMA_CHARGE_MULTIPLIER: u64 = 2;
const SCHEMA_FIXED_OVERHEAD_BYTES: u64 = 1024;

/// Returns the conservative retained-memory charge for one Arrow schema.
///
/// The charge includes Arrow's recursive field-size report, root metadata
/// allocations, reference-counted ownership overhead, and a fixed allocator
/// margin. Catalog decoding and query planning must use this same function so
/// a caller cannot understate the cost of a wide or deeply nested schema.
///
/// # Errors
///
/// Returns [`ErrorCode::ResourceExhausted`] when any intermediate byte count
/// exceeds the current platform or `u64` accounting domain.
pub fn conservative_arrow_schema_retained_bytes(schema: &Schema) -> Result<NonZeroU64> {
    let root_bytes = size_of_val(schema)
        .checked_add(size_of::<SchemaRef>())
        .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
        .ok_or_else(|| exhausted("arrow schema root byte charge overflow"))?;
    let metadata_slots = schema
        .metadata
        .capacity()
        .checked_mul(size_of::<(String, String)>())
        .ok_or_else(|| exhausted("arrow schema metadata slot charge overflow"))?;
    let metadata_strings = schema
        .metadata
        .iter()
        .try_fold(0_usize, |bytes, (key, value)| {
            bytes
                .checked_add(key.capacity())
                .and_then(|bytes| bytes.checked_add(value.capacity()))
                .ok_or_else(|| exhausted("arrow schema metadata string charge overflow"))
        })?;
    let reported_bytes = root_bytes
        .checked_add(schema.fields().size())
        .and_then(|bytes| bytes.checked_add(metadata_slots))
        .and_then(|bytes| bytes.checked_add(metadata_strings))
        .ok_or_else(|| exhausted("arrow schema retained byte charge overflow"))?;
    let charged_bytes = u64::try_from(reported_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(SCHEMA_CHARGE_MULTIPLIER))
        .and_then(|bytes| bytes.checked_add(SCHEMA_FIXED_OVERHEAD_BYTES))
        .ok_or_else(|| exhausted("arrow schema retained byte charge overflow"))?;
    NonZeroU64::new(charged_bytes)
        .ok_or_else(|| exhausted("arrow schema retained byte charge is zero"))
}

fn exhausted(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::conservative_arrow_schema_retained_bytes;

    #[test]
    fn retained_charge_grows_with_fields_and_metadata() {
        let narrow = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        let wide = Schema::new_with_metadata(
            vec![
                Field::new("price", DataType::Float64, false),
                Field::new("symbol", DataType::Utf8, false),
            ],
            HashMap::from([("source".to_owned(), "market-feed".to_owned())]),
        );

        let narrow_bytes = conservative_arrow_schema_retained_bytes(&narrow).unwrap();
        let wide_bytes = conservative_arrow_schema_retained_bytes(&wide).unwrap();

        assert!(wide_bytes > narrow_bytes);
    }
}
