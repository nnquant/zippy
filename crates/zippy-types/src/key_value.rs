//! Canonical native key-value event schemas and protocol-v2 change batches.
//!
//! Native storage retains every ordered upsert/delete event. Logical queries hide the control
//! columns and resolve the latest event for each key; event offsets never count snapshot rows.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{make_array, new_null_array, Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::ingest::KeyValueMutation;
use crate::{ErrorCode, Result, ServerError, TableId};

/// Monotonic one-based event sequence; the corresponding native log offset is sequence minus one.
pub const SEQUENCE: &str = "_zippy_kv_seq";
/// Explicit `upsert` or `delete` operation; delete non-key values have no business meaning.
pub const OPERATION: &str = "_zippy_kv_op";
/// One-based atomic mutation version, incremented once for each nonempty batch.
pub const VERSION: &str = "_zippy_kv_snapshot_version";
/// Last sequence in this complete atomic mutation.
pub const BATCH_END: &str = "_zippy_kv_batch_last_seq";
/// Stable logical table identity, unchanged by writer handoff or process restart.
pub const STREAM_ID: &str = "_zippy_kv_stream_id";
/// Number of protocol-v2 fields prepended to user columns.
pub const CONTROL_COLUMNS: usize = 5;
/// Catalog schema metadata identifying the ordered primary keys of a native event table.
pub const LOG_KEYS_METADATA: &str = "zippy.native.key_value.keys";
/// Manifest summary field retaining the last fully checkpointed key-value mutation version.
pub const STATE_VERSION_METADATA: &str = "zippy.native.key_value.version";

/// Decoded logical schema and ordered primary keys of a native key-value log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueLogSchema {
    /// User-visible fields and metadata, without the event envelope.
    pub logical: SchemaRef,
    /// Ordered UTF-8 composite key columns, matching the state-consumer contract.
    pub keys: Vec<String>,
}

/// Builds a physical event schema from a logical table definition.
///
/// Rejects reserved names, duplicate or missing keys, and non-UTF-8 keys before Catalog creation.
pub fn log_schema(logical: &Schema, keys: &[String]) -> Result<SchemaRef> {
    if logical.metadata().contains_key(LOG_KEYS_METADATA)
        || logical
            .metadata()
            .keys()
            .any(|name| name.starts_with("zippy_kv_"))
        || logical
            .fields()
            .iter()
            .any(|field| field.name().starts_with("_zippy_kv_"))
    {
        return Err(invalid(
            "logical key-value schema uses reserved event fields or metadata",
        ));
    }
    if keys.is_empty() {
        return Err(invalid("key-value table requires primary keys"));
    }
    let mut unique = BTreeSet::new();
    for key in keys {
        if !unique.insert(key) {
            return Err(invalid("duplicate key-value primary key"));
        }
        let field = logical.field_with_name(key).map_err(invalid)?;
        if field.data_type() != &DataType::Utf8 {
            return Err(invalid("key-value primary keys must be UTF-8"));
        }
    }
    let mut fields = control_fields();
    fields.extend(logical.fields().iter().cloned());
    let mut metadata = logical.metadata().clone();
    metadata.insert(
        LOG_KEYS_METADATA.to_owned(),
        serde_json::to_string(keys).map_err(invalid)?,
    );
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

/// Decodes and strictly validates a native event schema; ordinary table schemas return `None`.
pub fn decode_log_schema(physical: &Schema) -> Result<Option<KeyValueLogSchema>> {
    let Some(encoded) = physical.metadata().get(LOG_KEYS_METADATA) else {
        return Ok(None);
    };
    let keys: Vec<String> = serde_json::from_str(encoded).map_err(invalid)?;
    if physical.fields().len() <= CONTROL_COLUMNS
        || physical.fields()[..CONTROL_COLUMNS] != control_fields()[..]
    {
        return Err(invalid("native key-value control schema is malformed"));
    }
    let mut metadata = physical.metadata().clone();
    metadata.remove(LOG_KEYS_METADATA);
    let logical = Arc::new(Schema::new_with_metadata(
        physical.fields()[CONTROL_COLUMNS..].to_vec(),
        metadata,
    ));
    let canonical = log_schema(logical.as_ref(), &keys)?;
    if canonical.as_ref() != physical {
        return Err(invalid("native key-value event schema is not canonical"));
    }
    Ok(Some(KeyValueLogSchema { logical, keys }))
}

/// Encodes one complete nonempty mutation without deduplicating or reordering its events.
///
/// Delete accepts exact key columns or full logical rows. Missing non-key columns are filled with
/// deterministic type-valid placeholders and must be ignored by every consumer of a delete event.
pub fn change_batch(
    physical: SchemaRef,
    mutation: &KeyValueMutation,
    table_id: TableId,
    version: u64,
    first_sequence: u64,
) -> Result<RecordBatch> {
    let decoded = decode_log_schema(physical.as_ref())?
        .ok_or_else(|| invalid("key-value mutation requires an event schema"))?;
    let (operation, batch) = match mutation {
        KeyValueMutation::Upsert(batch) => ("upsert", batch),
        KeyValueMutation::Delete(batch) => ("delete", batch),
    };
    let key_fields = decoded
        .keys
        .iter()
        .map(|key| {
            decoded
                .logical
                .field_with_name(key)
                .cloned()
                .map_err(invalid)
        })
        .collect::<Result<Vec<_>>>()?;
    let key_schema = Schema::new_with_metadata(key_fields, decoded.logical.metadata().clone());
    let full = batch.schema().as_ref() == decoded.logical.as_ref();
    if !full && (operation != "delete" || batch.schema().as_ref() != &key_schema) {
        return Err(invalid(
            "key-value mutation schema does not match its logical table",
        ));
    }
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(physical));
    }
    let end = first_sequence
        .checked_add(batch.num_rows() as u64 - 1)
        .filter(|end| *end <= i64::MAX as u64)
        .ok_or_else(|| invalid("key-value sequence domain exhausted"))?;
    if first_sequence == 0 || version == 0 || version > i64::MAX as u64 {
        return Err(invalid(
            "key-value sequence and version must be positive signed 64-bit values",
        ));
    }
    for key in &decoded.keys {
        if batch
            .column_by_name(key)
            .ok_or_else(|| invalid("missing key-value key"))?
            .null_count()
            != 0
        {
            return Err(invalid("key-value primary key contains null"));
        }
    }
    let rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            (first_sequence..=end).map(|value| value as i64),
        )),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            operation, rows,
        ))),
        Arc::new(Int64Array::from(vec![version as i64; rows])),
        Arc::new(Int64Array::from(vec![end as i64; rows])),
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            format!("{:032x}", table_id.as_u128()),
            rows,
        ))),
    ];
    for field in decoded.logical.fields() {
        let column = match batch.column_by_name(field.name()) {
            Some(column) => Arc::clone(column),
            None => {
                let nulls = new_null_array(field.data_type(), rows);
                if field.is_nullable() {
                    nulls
                } else {
                    make_array(
                        nulls
                            .to_data()
                            .into_builder()
                            .nulls(None)
                            .build()
                            .map_err(invalid)?,
                    )
                }
            }
        };
        columns.push(column);
    }
    RecordBatch::try_new(physical, columns).map_err(invalid)
}

fn control_fields() -> Vec<Arc<Field>> {
    [
        (SEQUENCE, DataType::Int64),
        (OPERATION, DataType::Utf8),
        (VERSION, DataType::Int64),
        (BATCH_END, DataType::Int64),
        (STREAM_ID, DataType::Utf8),
    ]
    .into_iter()
    .map(|(name, data_type)| Arc::new(Field::new(name, data_type, false)))
    .collect()
}

fn invalid(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::SchemaMismatch,
        format!("invalid key-value log contract error=[{error}]"),
    )
    .with_retryable(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logical() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new("optional", DataType::Int64, true),
        ]))
    }

    #[test]
    fn event_schema_roundtrips_metadata_and_rejects_ambiguous_keys() {
        let logical = logical()
            .as_ref()
            .clone()
            .with_metadata([("source".to_owned(), "fixture".to_owned())].into());
        let keys = vec!["symbol".to_owned()];
        let physical = log_schema(&logical, &keys).unwrap();
        let decoded = decode_log_schema(&physical).unwrap().unwrap();
        assert_eq!(decoded.logical.as_ref(), &logical);
        assert_eq!(decoded.keys, keys);
        assert!(decode_log_schema(&logical).unwrap().is_none());
        for keys in [
            vec![],
            vec!["missing".to_owned()],
            vec!["price".to_owned()],
            vec!["symbol".to_owned(); 2],
        ] {
            assert!(log_schema(&logical, &keys).is_err());
        }
        assert!(log_schema(&physical, &["symbol".to_owned()]).is_err());
        let malformed =
            Schema::new_with_metadata(logical.fields().clone(), physical.metadata().clone());
        assert!(decode_log_schema(&malformed).is_err());
    }

    #[test]
    fn delete_key_tombstones_fill_valid_values_and_keep_input_order() {
        let physical = log_schema(&logical(), &["symbol".to_owned()]).unwrap();
        let tombstones = RecordBatch::try_new(
            Arc::new(Schema::new(vec![logical().field(0).clone()])),
            vec![Arc::new(StringArray::from(vec!["b", "a", "b"]))],
        )
        .unwrap();
        let table = TableId::from_u128(42).unwrap();
        let events = change_batch(
            physical.clone(),
            &KeyValueMutation::Delete(tombstones.clone()),
            table,
            3,
            7,
        )
        .unwrap();
        assert_eq!(events.num_rows(), 3);
        assert_eq!(
            events
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 8, 9]
        );
        assert_eq!(
            events
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[9; 3]
        );
        assert_eq!(events.column(5), tombstones.column(0));
        assert_eq!(events.column(6).null_count(), 0);
        assert_eq!(
            events
                .column(7)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            ""
        );
        assert_eq!(events.column(8).null_count(), 3);
        assert!(change_batch(
            physical.clone(),
            &KeyValueMutation::Upsert(tombstones.clone()),
            table,
            3,
            7
        )
        .is_err());
        for (version, sequence) in [(0, 1), (1, 0), (1, i64::MAX as u64), (u64::MAX, 1)] {
            assert!(change_batch(
                physical.clone(),
                &KeyValueMutation::Delete(tombstones.clone()),
                table,
                version,
                sequence
            )
            .is_err());
        }
    }

    #[test]
    fn null_primary_keys_are_rejected_before_materialization() {
        let logical = Schema::new(vec![Field::new("key", DataType::Utf8, true)]);
        let physical = log_schema(&logical, &["key".to_owned()]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(logical),
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();
        assert!(change_batch(
            physical,
            &KeyValueMutation::Upsert(batch),
            TableId::from_u128(1).unwrap(),
            1,
            1
        )
        .is_err());
    }
}
