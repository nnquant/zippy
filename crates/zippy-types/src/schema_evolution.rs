//! Native schema evolution that preserves stored values and immutable table identity columns.

use std::collections::BTreeSet;

use arrow::array::new_null_array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::schema_contract::validate_catalog_table_schema;
use crate::table::{SchemaCompatibility, TableDefinition};
use crate::{ErrorCode, Result, ServerError};

/// Classifies an explicit native schema change without rewriting historical data.
///
/// Existing columns retain their names, order and data types. New columns must be appended and
/// nullable; ordinary columns may become nullable. Primary keys, frame grouping and event-time
/// fields remain exact. Metadata changes on ordinary fields or the schema do not change values.
/// Arrow extension identities also remain exact. Nested field/type changes, removals, renames
/// and stricter nullability are rejected.
pub fn classify_schema_change(
    definition: &TableDefinition,
    current: &Schema,
    proposed: &Schema,
) -> Result<SchemaCompatibility> {
    validate_catalog_table_schema(proposed, definition).map_err(|error| {
        ServerError::new(
            if error.is_resource_exhausted() {
                ErrorCode::ResourceExhausted
            } else {
                ErrorCode::SchemaMismatch
            },
            error.to_string(),
        )
        .with_retryable(false)
    })?;
    if current.metadata().get(crate::key_value::LOG_KEYS_METADATA)
        != proposed.metadata().get(crate::key_value::LOG_KEYS_METADATA)
    {
        return Err(mismatch(
            "schema change cannot alter the key-value event contract",
        ));
    }
    let protected: BTreeSet<&str> = match definition {
        TableDefinition::Append(options) => options
            .event_time_column
            .iter()
            .map(|column| column.as_str())
            .collect(),
        TableDefinition::Frame(options) => [options.by.as_str()].into_iter().collect(),
        TableDefinition::KeyValue(options) => {
            // `validate_catalog_table_schema` already matched any proposed event keys to the
            // definition, and the metadata check above pins `current` to the same encoded keys.
            crate::key_value::decode_log_schema(current)?.ok_or_else(|| {
                mismatch("native schema change requires a key-value event schema")
            })?;
            options
                .keys()
                .as_slice()
                .iter()
                .map(|column| column.as_str())
                .collect()
        }
    };
    let encoding_changes = validate_historical_layout(current, proposed)?;
    for (old, new) in current.fields().iter().zip(proposed.fields()) {
        for key in ["ARROW:extension:name", "ARROW:extension:metadata"] {
            if old.metadata().get(key) != new.metadata().get(key) {
                return Err(mismatch(format!(
                    "schema change cannot reinterpret an Arrow extension column=[{}]",
                    old.name()
                )));
            }
        }
        if protected.contains(old.name().as_str()) && old != new {
            return Err(mismatch(format!(
                "schema change cannot alter table identity field column=[{}]",
                old.name()
            )));
        }
    }
    if current == proposed {
        return Err(ServerError::new(
            ErrorCode::InvalidArgument,
            "proposed schema is identical to the current schema",
        )
        .with_retryable(false));
    }
    Ok(if encoding_changes {
        SchemaCompatibility::Backward
    } else {
        SchemaCompatibility::Full
    })
}

/// Aligns one historical batch to a newer native schema while retaining existing column buffers.
///
/// Only appended nullable columns receive new all-null arrays. Names, positions and data types of
/// existing columns must match, and nullability cannot become stricter. The returned batch carries
/// the target metadata and original row count, including empty batches and zero-column batches.
/// Table identity and key metadata are validated against Catalog schemas at evolution time;
/// physical segment readers need not retain schema-level metadata in every decoded batch.
pub fn align_historical_batch(batch: &RecordBatch, target: SchemaRef) -> Result<RecordBatch> {
    validate_historical_layout(batch.schema().as_ref(), target.as_ref())?;
    let mut columns = batch.columns().to_vec();
    for field in target.fields().iter().skip(columns.len()) {
        columns.push(new_null_array(field.data_type(), batch.num_rows()));
    }
    RecordBatch::try_new_with_options(
        target,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|error| {
        mismatch(format!(
            "historical batch cannot match target schema error=[{error}]"
        ))
    })
}

fn validate_historical_layout(current: &Schema, proposed: &Schema) -> Result<bool> {
    if proposed.fields().len() < current.fields().len() {
        return Err(mismatch("schema change cannot remove existing columns"));
    }
    let mut encoding_changes = current.fields().len() != proposed.fields().len();
    for (old, new) in current.fields().iter().zip(proposed.fields()) {
        if old.name() != new.name() || old.data_type() != new.data_type() {
            return Err(mismatch(format!(
                "schema change must preserve existing column names order and types column=[{}]",
                old.name()
            )));
        }
        if old.is_nullable() && !new.is_nullable() {
            return Err(mismatch(format!(
                "schema change cannot tighten nullability column=[{}]",
                old.name()
            )));
        }
        encoding_changes |= old.is_nullable() != new.is_nullable();
    }
    if let Some(field) = proposed.fields()[current.fields().len()..]
        .iter()
        .find(|field| !field.is_nullable())
    {
        return Err(mismatch(format!(
            "new historical columns must be nullable column=[{}]",
            field.name()
        )));
    }
    Ok(encoding_changes)
}

fn mismatch(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::SchemaMismatch, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::table::{
        AppendTableDefinition, ColumnName, ColumnSet, FrameTableDefinition, KeyValueTableDefinition,
    };

    fn append() -> TableDefinition {
        TableDefinition::Append(AppendTableDefinition::default())
    }

    #[test]
    fn additive_alignment_preserves_values_buffers_and_row_count() {
        let old = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let new = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("comment", DataType::Utf8, true),
        ]));
        assert_eq!(
            classify_schema_change(&append(), &old, &new).unwrap(),
            SchemaCompatibility::Backward
        );
        let batch =
            RecordBatch::try_new(Arc::new(old), vec![Arc::new(Int64Array::from(vec![7, 9]))])
                .unwrap();
        let aligned = align_historical_batch(&batch, new.clone()).unwrap();
        assert!(Arc::ptr_eq(batch.column(0), aligned.column(0)));
        assert_eq!(aligned.num_rows(), 2);
        assert_eq!(aligned.column(1).null_count(), 2);
        assert_eq!(aligned.schema(), new);
        let empty = align_historical_batch(&batch.slice(0, 0), new).unwrap();
        assert_eq!(empty.num_rows(), 0);
    }

    #[test]
    fn incompatible_changes_are_explicit_errors() {
        let old = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
        for fields in [
            vec![],
            vec![Field::new("renamed", DataType::Int64, true)],
            vec![Field::new("value", DataType::Float64, true)],
            vec![Field::new("value", DataType::Int64, false)],
            vec![
                Field::new("value", DataType::Int64, true),
                Field::new("required", DataType::Utf8, false),
            ],
        ] {
            assert_eq!(
                classify_schema_change(&append(), &old, &Schema::new(fields))
                    .unwrap_err()
                    .code(),
                ErrorCode::SchemaMismatch
            );
        }
        assert_eq!(
            classify_schema_change(&append(), &old, &old)
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn metadata_changes_are_full_while_identity_fields_remain_fixed() {
        let old = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let new = old
            .clone()
            .with_metadata([("description".to_owned(), "new comment".to_owned())].into());
        assert_eq!(
            classify_schema_change(&append(), &old, &new).unwrap(),
            SchemaCompatibility::Full
        );
        let extension = Schema::new(vec![old.field(0).clone().with_metadata(
            [(
                "ARROW:extension:name".to_owned(),
                "custom.quantity".to_owned(),
            )]
            .into(),
        )]);
        assert_eq!(
            classify_schema_change(&append(), &old, &extension)
                .unwrap_err()
                .code(),
            ErrorCode::SchemaMismatch
        );
        let nullable = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
        for definition in [
            TableDefinition::Frame(FrameTableDefinition {
                by: ColumnName::new("value").unwrap(),
            }),
            TableDefinition::Append(AppendTableDefinition {
                event_time_column: Some(ColumnName::new("value").unwrap()),
            }),
        ] {
            assert!(classify_schema_change(&definition, &old, &nullable).is_err());
        }
    }

    #[test]
    fn native_key_value_changes_preserve_control_and_primary_key_fields() {
        let keys = vec!["key".to_owned()];
        let old = crate::key_value::log_schema(
            &Schema::new(vec![Field::new("key", DataType::Utf8, false)]),
            &keys,
        )
        .unwrap();
        let new = crate::key_value::log_schema(
            &Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("price", DataType::Int64, true),
            ]),
            &keys,
        )
        .unwrap();
        let definition = TableDefinition::KeyValue(
            KeyValueTableDefinition::new(
                ColumnSet::new(vec![ColumnName::new("key").unwrap()]).unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(
            classify_schema_change(&definition, &old, &new).unwrap(),
            SchemaCompatibility::Backward
        );
        let changed = crate::key_value::log_schema(
            &Schema::new(vec![Field::new("key", DataType::Utf8, true)]),
            &keys,
        )
        .unwrap();
        assert!(classify_schema_change(&definition, &old, &changed).is_err());
        let changed_keys = crate::key_value::log_schema(
            &Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("other", DataType::Utf8, true),
            ]),
            &["other".to_owned()],
        )
        .unwrap();
        assert!(classify_schema_change(&definition, &old, &changed_keys).is_err());
    }
}
