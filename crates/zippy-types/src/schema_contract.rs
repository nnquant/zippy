//! Shared admission rules for Catalog table definitions and Arrow schemas.

use std::collections::BTreeSet;

use arrow::datatypes::{DataType, Field, Schema};
use thiserror::Error;

use crate::table::{ColumnName, InvalidTableDefinition, TableDefinition};

/// The maximum number of fields admitted by one Catalog table schema.
pub const MAX_CATALOG_SCHEMA_FIELDS: usize = 4_096;
/// The maximum nesting depth admitted by one Catalog Arrow schema.
pub const MAX_CATALOG_SCHEMA_NESTING_DEPTH: usize = 32;
/// The maximum combined schema and field metadata entry count.
pub const MAX_CATALOG_SCHEMA_METADATA_ENTRIES: usize = 4_096;
/// The maximum combined UTF-8 bytes in schema and field metadata.
pub const MAX_CATALOG_SCHEMA_METADATA_BYTES: usize = 1024 * 1024;
/// The maximum encoded schema-only Arrow IPC payload admitted by the Catalog.
pub const MAX_CATALOG_SCHEMA_IPC_BYTES: usize = 16 * 1024 * 1024;

/// A structured failure returned by the shared Catalog table-schema contract.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum CatalogTableSchemaError {
    /// The schema contains more fields than the Catalog resource budget permits.
    #[error("catalog schema field count exceeds maximum actual=[{actual}] maximum=[{maximum}]")]
    FieldCountExceeded {
        /// The number of fields in the submitted schema.
        actual: usize,
        /// The maximum admitted field count.
        maximum: usize,
    },
    /// Recursive child fields make the schema exceed the Catalog field budget.
    #[error(
        "catalog schema recursive field count exceeds maximum actual=[{actual}] maximum=[{maximum}]"
    )]
    RecursiveFieldCountExceeded {
        /// The number of recursive fields observed before rejection.
        actual: usize,
        /// The maximum admitted recursive field count.
        maximum: usize,
    },
    /// The schema exceeds the Catalog nesting-depth budget.
    #[error("catalog schema nesting exceeds maximum actual=[{actual}] maximum=[{maximum}]")]
    NestingDepthExceeded {
        /// The field nesting depth observed before rejection.
        actual: usize,
        /// The maximum admitted field nesting depth.
        maximum: usize,
    },
    /// Schema and field metadata contain too many entries.
    #[error(
        "catalog schema metadata entry count exceeds maximum actual=[{actual}] maximum=[{maximum}]"
    )]
    MetadataEntryCountExceeded {
        /// The metadata entry count observed before rejection.
        actual: usize,
        /// The maximum admitted metadata entry count.
        maximum: usize,
    },
    /// Schema and field metadata contain too many UTF-8 bytes.
    #[error("catalog schema metadata bytes exceed maximum actual=[{actual}] maximum=[{maximum}]")]
    MetadataBytesExceeded {
        /// The metadata byte count observed before rejection.
        actual: usize,
        /// The maximum admitted metadata byte count.
        maximum: usize,
    },
    /// An Arrow field name does not satisfy [`ColumnName`] invariants.
    #[error("invalid catalog schema field name error=[{reason}]")]
    InvalidFieldName {
        /// The invalid Arrow field name.
        name: String,
        /// The violated column-name invariant.
        reason: InvalidTableDefinition,
    },
    /// More than one field has the same exact name.
    #[error("duplicate catalog schema field name=[{name}]")]
    DuplicateFieldName {
        /// The duplicated Arrow field name.
        name: String,
    },
    /// The table definition references a field absent from the schema.
    #[error("table definition references missing schema field=[{name}]")]
    MissingDefinitionField {
        /// The missing definition field name.
        name: String,
    },
    /// A definition field exists but uses a data type unsupported by its role.
    #[error(
        "table definition field has unsupported type field=[{name}] expected=[{expected:?}] actual=[{actual:?}]"
    )]
    UnsupportedDefinitionFieldType {
        /// The definition field name.
        name: String,
        /// The required Arrow data type.
        expected: DataType,
        /// The submitted Arrow data type.
        actual: DataType,
    },
}

impl CatalogTableSchemaError {
    /// Returns whether this failure represents a resource-budget violation.
    #[must_use]
    pub const fn is_resource_exhausted(&self) -> bool {
        matches!(
            self,
            Self::FieldCountExceeded { .. }
                | Self::RecursiveFieldCountExceeded { .. }
                | Self::NestingDepthExceeded { .. }
                | Self::MetadataEntryCountExceeded { .. }
                | Self::MetadataBytesExceeded { .. }
        )
    }
}

/// Validates one Arrow schema against the shared Catalog admission contract.
///
/// This function is protocol-neutral and does not choose a server error code.
/// Catalog implementations should map every error for which
/// [`CatalogTableSchemaError::is_resource_exhausted`] returns `true` to resource exhaustion, and
/// map all semantic failures to a schema mismatch.
///
/// # Errors
///
/// Returns [`CatalogTableSchemaError`] when the schema exceeds any field, nesting, or metadata
/// budget; has invalid or duplicate field names; omits a definition field; or assigns a non-UTF8
/// type to a key-value key.
pub fn validate_catalog_table_schema(
    schema: &Schema,
    definition: &TableDefinition,
) -> Result<(), CatalogTableSchemaError> {
    if schema.fields().len() > MAX_CATALOG_SCHEMA_FIELDS {
        return Err(CatalogTableSchemaError::FieldCountExceeded {
            actual: schema.fields().len(),
            maximum: MAX_CATALOG_SCHEMA_FIELDS,
        });
    }

    let mut budget = CatalogSchemaBudget::default();
    budget.add_metadata(schema.metadata())?;
    for field in schema.fields() {
        budget.visit_field(field, 1)?;
    }

    let mut names = BTreeSet::new();
    for field in schema.fields() {
        ColumnName::new(field.name().to_owned()).map_err(|reason| {
            CatalogTableSchemaError::InvalidFieldName {
                name: field.name().to_owned(),
                reason,
            }
        })?;
        if !names.insert(field.name().as_str()) {
            return Err(CatalogTableSchemaError::DuplicateFieldName {
                name: field.name().to_owned(),
            });
        }
    }

    match definition {
        TableDefinition::Append(definition) => {
            if let Some(column) = &definition.event_time_column {
                require_field(schema, column.as_str(), None)?;
            }
        }
        TableDefinition::Frame(definition) => {
            require_field(schema, definition.by.as_str(), None)?;
        }
        TableDefinition::KeyValue(definition) => {
            for column in definition.keys().as_slice() {
                require_field(schema, column.as_str(), Some(&DataType::Utf8))?;
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct CatalogSchemaBudget {
    fields: usize,
    metadata_entries: usize,
    metadata_bytes: usize,
}

impl CatalogSchemaBudget {
    fn visit_field(&mut self, field: &Field, depth: usize) -> Result<(), CatalogTableSchemaError> {
        Self::validate_depth(depth)?;
        self.fields = self.fields.checked_add(1).unwrap_or(usize::MAX);
        if self.fields > MAX_CATALOG_SCHEMA_FIELDS {
            return Err(CatalogTableSchemaError::RecursiveFieldCountExceeded {
                actual: self.fields,
                maximum: MAX_CATALOG_SCHEMA_FIELDS,
            });
        }
        self.add_metadata(field.metadata())?;
        self.visit_data_type(field.data_type(), depth)
    }

    fn visit_data_type(
        &mut self,
        data_type: &DataType,
        depth: usize,
    ) -> Result<(), CatalogTableSchemaError> {
        Self::validate_depth(depth)?;
        let child_depth = depth.saturating_add(1);
        match data_type {
            DataType::List(child)
            | DataType::ListView(child)
            | DataType::FixedSizeList(child, _)
            | DataType::LargeList(child)
            | DataType::LargeListView(child)
            | DataType::Map(child, _) => self.visit_field(child, child_depth)?,
            DataType::Struct(children) => {
                for child in children {
                    self.visit_field(child, child_depth)?;
                }
            }
            DataType::Union(children, _) => {
                for (_, child) in children.iter() {
                    self.visit_field(child, child_depth)?;
                }
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.visit_field(run_ends, child_depth)?;
                self.visit_field(values, child_depth)?;
            }
            DataType::Dictionary(key, value) => {
                self.visit_data_type(key, child_depth)?;
                self.visit_data_type(value, child_depth)?;
            }
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => {}
        }
        Ok(())
    }

    fn validate_depth(depth: usize) -> Result<(), CatalogTableSchemaError> {
        if depth > MAX_CATALOG_SCHEMA_NESTING_DEPTH {
            return Err(CatalogTableSchemaError::NestingDepthExceeded {
                actual: depth,
                maximum: MAX_CATALOG_SCHEMA_NESTING_DEPTH,
            });
        }
        Ok(())
    }

    fn add_metadata(
        &mut self,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<(), CatalogTableSchemaError> {
        self.metadata_entries = self
            .metadata_entries
            .checked_add(metadata.len())
            .unwrap_or(usize::MAX);
        if self.metadata_entries > MAX_CATALOG_SCHEMA_METADATA_ENTRIES {
            return Err(CatalogTableSchemaError::MetadataEntryCountExceeded {
                actual: self.metadata_entries,
                maximum: MAX_CATALOG_SCHEMA_METADATA_ENTRIES,
            });
        }
        for (key, value) in metadata {
            self.metadata_bytes = self
                .metadata_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .unwrap_or(usize::MAX);
            if self.metadata_bytes > MAX_CATALOG_SCHEMA_METADATA_BYTES {
                return Err(CatalogTableSchemaError::MetadataBytesExceeded {
                    actual: self.metadata_bytes,
                    maximum: MAX_CATALOG_SCHEMA_METADATA_BYTES,
                });
            }
        }
        Ok(())
    }
}

fn require_field(
    schema: &Schema,
    name: &str,
    required_type: Option<&DataType>,
) -> Result<(), CatalogTableSchemaError> {
    let field = schema.field_with_name(name).map_err(|_| {
        CatalogTableSchemaError::MissingDefinitionField {
            name: name.to_owned(),
        }
    })?;
    if let Some(required) = required_type.filter(|required| field.data_type() != *required) {
        return Err(CatalogTableSchemaError::UnsupportedDefinitionFieldType {
            name: name.to_owned(),
            expected: required.clone(),
            actual: field.data_type().clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{
        validate_catalog_table_schema, CatalogTableSchemaError, MAX_CATALOG_SCHEMA_FIELDS,
        MAX_CATALOG_SCHEMA_METADATA_BYTES, MAX_CATALOG_SCHEMA_METADATA_ENTRIES,
        MAX_CATALOG_SCHEMA_NESTING_DEPTH,
    };
    use crate::table::{
        AppendTableDefinition, ColumnName, ColumnSet, FrameTableDefinition,
        KeyValueTableDefinition, TableDefinition,
    };

    fn append(event_time_column: Option<&str>) -> TableDefinition {
        TableDefinition::Append(AppendTableDefinition {
            event_time_column: event_time_column.map(|name| ColumnName::new(name).unwrap()),
        })
    }

    fn frame(by: &str) -> TableDefinition {
        TableDefinition::Frame(FrameTableDefinition {
            by: ColumnName::new(by).unwrap(),
        })
    }

    fn key_value(keys: &[&str]) -> TableDefinition {
        let keys = keys
            .iter()
            .map(|name| ColumnName::new(*name).unwrap())
            .collect();
        TableDefinition::KeyValue(
            KeyValueTableDefinition::new(ColumnSet::new(keys).unwrap()).unwrap(),
        )
    }

    #[test]
    fn rejects_invalid_and_duplicate_field_names() {
        let invalid = Schema::new(vec![Field::new(" ", DataType::Utf8, false)]);
        assert!(matches!(
            validate_catalog_table_schema(&invalid, &append(None)),
            Err(CatalogTableSchemaError::InvalidFieldName { name, .. }) if name == " "
        ));

        let duplicate = Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("symbol", DataType::Int64, false),
        ]);
        assert_eq!(
            validate_catalog_table_schema(&duplicate, &append(None)),
            Err(CatalogTableSchemaError::DuplicateFieldName {
                name: "symbol".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_append_frame_and_key_value_fields() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, true)]);
        for definition in [
            append(Some("event_time")),
            frame("symbol"),
            key_value(&["key"]),
        ] {
            assert!(matches!(
                validate_catalog_table_schema(&schema, &definition),
                Err(CatalogTableSchemaError::MissingDefinitionField { .. })
            ));
        }
    }

    #[test]
    fn requires_utf8_key_value_fields() {
        let schema = Schema::new(vec![Field::new("key", DataType::Int64, false)]);
        assert_eq!(
            validate_catalog_table_schema(&schema, &key_value(&["key"])),
            Err(CatalogTableSchemaError::UnsupportedDefinitionFieldType {
                name: "key".to_owned(),
                expected: DataType::Utf8,
                actual: DataType::Int64,
            })
        );
    }

    #[test]
    fn admits_4096_fields_and_rejects_4097_fields() {
        let fields = (0..=MAX_CATALOG_SCHEMA_FIELDS)
            .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let at_limit = Schema::new(fields[..MAX_CATALOG_SCHEMA_FIELDS].to_vec());
        assert_eq!(
            validate_catalog_table_schema(&at_limit, &append(None)),
            Ok(())
        );

        let over_limit = Schema::new(fields);
        assert_eq!(
            validate_catalog_table_schema(&over_limit, &append(None)),
            Err(CatalogTableSchemaError::FieldCountExceeded {
                actual: 4_097,
                maximum: 4_096,
            })
        );
    }

    #[test]
    fn rejects_recursive_field_and_nesting_overflow() {
        let nested_children = (0..MAX_CATALOG_SCHEMA_FIELDS)
            .map(|index| Field::new(format!("child_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let recursive_overflow = Schema::new(vec![Field::new(
            "root",
            DataType::Struct(nested_children.into()),
            true,
        )]);
        assert_eq!(
            validate_catalog_table_schema(&recursive_overflow, &append(None)),
            Err(CatalogTableSchemaError::RecursiveFieldCountExceeded {
                actual: 4_097,
                maximum: 4_096,
            })
        );

        let mut nested = Field::new("leaf", DataType::Int64, true);
        for depth in 0..MAX_CATALOG_SCHEMA_NESTING_DEPTH {
            nested = Field::new(
                format!("level_{depth}"),
                DataType::List(Arc::new(nested)),
                true,
            );
        }
        assert_eq!(
            validate_catalog_table_schema(&Schema::new(vec![nested]), &append(None)),
            Err(CatalogTableSchemaError::NestingDepthExceeded {
                actual: 33,
                maximum: 32,
            })
        );
    }

    #[test]
    fn rejects_dictionary_wrapped_nesting_overflow() {
        let mut at_limit = DataType::Int64;
        for _ in 1..MAX_CATALOG_SCHEMA_NESTING_DEPTH {
            at_limit = DataType::Dictionary(Box::new(DataType::Int32), Box::new(at_limit));
        }
        assert_eq!(
            validate_catalog_table_schema(
                &Schema::new(vec![Field::new("root", at_limit.clone(), true)]),
                &append(None),
            ),
            Ok(())
        );

        let over_limit = DataType::Dictionary(Box::new(DataType::Int32), Box::new(at_limit));
        assert_eq!(
            validate_catalog_table_schema(
                &Schema::new(vec![Field::new("root", over_limit, true)]),
                &append(None),
            ),
            Err(CatalogTableSchemaError::NestingDepthExceeded {
                actual: 33,
                maximum: 32,
            })
        );
    }

    #[test]
    fn rejects_dictionary_wrapped_recursive_field_overflow() {
        let children = (0..MAX_CATALOG_SCHEMA_FIELDS)
            .map(|index| Field::new(format!("child_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>();
        let at_limit = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(
                children[..MAX_CATALOG_SCHEMA_FIELDS - 1].to_vec().into(),
            )),
        );
        assert_eq!(
            validate_catalog_table_schema(
                &Schema::new(vec![Field::new("root", at_limit, true)]),
                &append(None),
            ),
            Ok(())
        );

        let over_limit = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Struct(children.into())),
        );
        assert_eq!(
            validate_catalog_table_schema(
                &Schema::new(vec![Field::new("root", over_limit, true)]),
                &append(None),
            ),
            Err(CatalogTableSchemaError::RecursiveFieldCountExceeded {
                actual: 4_097,
                maximum: 4_096,
            })
        );
    }

    #[test]
    fn rejects_metadata_entry_and_byte_overflow() {
        let too_many_entries = (0..=MAX_CATALOG_SCHEMA_METADATA_ENTRIES)
            .map(|index| (format!("key_{index}"), String::new()))
            .collect::<HashMap<_, _>>();
        let entry_overflow = Schema::new_with_metadata(Vec::<Field>::new(), too_many_entries);
        assert_eq!(
            validate_catalog_table_schema(&entry_overflow, &append(None)),
            Err(CatalogTableSchemaError::MetadataEntryCountExceeded {
                actual: 4_097,
                maximum: 4_096,
            })
        );

        let byte_overflow = Schema::new_with_metadata(
            Vec::<Field>::new(),
            HashMap::from([(
                "source".to_owned(),
                "x".repeat(MAX_CATALOG_SCHEMA_METADATA_BYTES),
            )]),
        );
        assert_eq!(
            validate_catalog_table_schema(&byte_overflow, &append(None)),
            Err(CatalogTableSchemaError::MetadataBytesExceeded {
                actual: MAX_CATALOG_SCHEMA_METADATA_BYTES + "source".len(),
                maximum: MAX_CATALOG_SCHEMA_METADATA_BYTES,
            })
        );
    }

    #[test]
    fn admits_valid_definition_fields() {
        let schema = Schema::new(vec![
            Field::new("event_time", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
        ]);
        for definition in [
            append(Some("event_time")),
            frame("symbol"),
            key_value(&["symbol"]),
        ] {
            assert_eq!(validate_catalog_table_schema(&schema, &definition), Ok(()));
        }
    }
}
