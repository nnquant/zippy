//! Canonical hashes shared by Catalog producers and consumers.

use std::collections::HashMap;

use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use sha2::digest::Output;
use sha2::{Digest, Sha256};

const CATALOG_SCHEMA_HASH_DOMAIN_V1: &[u8] = b"zippy.catalog.canonical-arrow-schema.v1\0";
const SHA256_PREFIX: &str = "sha256-v1:";

/// Computes the durable Catalog V1 logical hash of an Arrow schema.
///
/// Catalog command producers and the storage verifier share this hash domain
/// so a schema cannot be accepted under a different logical encoding.
#[must_use]
pub fn canonical_catalog_schema_hash_v1(schema: &Schema) -> String {
    let digest = canonical_catalog_schema_digest_output_v1(schema);
    format!("{SHA256_PREFIX}{digest:x}")
}

/// Computes the durable Catalog V1 logical digest bytes of an Arrow schema.
///
/// This output form avoids hexadecimal formatting when another authenticated
/// envelope, such as a query admission fingerprint, needs to incorporate the
/// exact canonical schema content. Metadata key ordering may still require a
/// bounded temporary allocation.
#[must_use]
pub fn canonical_catalog_schema_digest_v1(schema: &Schema) -> [u8; 32] {
    canonical_catalog_schema_digest_output_v1(schema).into()
}

fn canonical_catalog_schema_digest_output_v1(schema: &Schema) -> Output<Sha256> {
    let mut encoder = CanonicalArrowSchemaEncoderV1::new();
    encoder.schema(schema);
    encoder.digest.finalize()
}

struct CanonicalArrowSchemaEncoderV1 {
    digest: Sha256,
}

impl CanonicalArrowSchemaEncoderV1 {
    fn new() -> Self {
        let mut digest = Sha256::new();
        digest.update(CATALOG_SCHEMA_HASH_DOMAIN_V1);
        Self { digest }
    }

    fn schema(&mut self, schema: &Schema) {
        self.tag(1);
        self.fields(schema.fields());
        self.metadata(schema.metadata());
    }

    fn fields(&mut self, fields: &Fields) {
        self.length(fields.len());
        for field in fields {
            self.field(field);
        }
    }

    fn field(&mut self, field: &Field) {
        self.string(field.name());
        self.boolean(field.is_nullable());
        self.data_type(field.data_type());
        self.optional_i64(field.dict_id());
        self.optional_boolean(field.dict_is_ordered());
        self.metadata(field.metadata());
    }

    fn union_fields(&mut self, fields: &UnionFields) {
        self.length(fields.len());
        for (type_id, field) in fields.iter() {
            self.signed_byte(type_id);
            self.field(field);
        }
    }

    fn data_type(&mut self, data_type: &DataType) {
        match data_type {
            DataType::Null => self.tag(0),
            DataType::Boolean => self.tag(1),
            DataType::Int8 => self.tag(2),
            DataType::Int16 => self.tag(3),
            DataType::Int32 => self.tag(4),
            DataType::Int64 => self.tag(5),
            DataType::UInt8 => self.tag(6),
            DataType::UInt16 => self.tag(7),
            DataType::UInt32 => self.tag(8),
            DataType::UInt64 => self.tag(9),
            DataType::Float16 => self.tag(10),
            DataType::Float32 => self.tag(11),
            DataType::Float64 => self.tag(12),
            DataType::Timestamp(unit, timezone) => {
                self.tag(13);
                self.time_unit(*unit);
                self.optional_string(timezone.as_deref());
            }
            DataType::Date32 => self.tag(14),
            DataType::Date64 => self.tag(15),
            DataType::Time32(unit) => {
                self.tag(16);
                self.time_unit(*unit);
            }
            DataType::Time64(unit) => {
                self.tag(17);
                self.time_unit(*unit);
            }
            DataType::Duration(unit) => {
                self.tag(18);
                self.time_unit(*unit);
            }
            DataType::Interval(unit) => {
                self.tag(19);
                self.interval_unit(*unit);
            }
            DataType::Binary => self.tag(20),
            DataType::FixedSizeBinary(width) => {
                self.tag(21);
                self.signed_u32(*width);
            }
            DataType::LargeBinary => self.tag(22),
            DataType::BinaryView => self.tag(23),
            DataType::Utf8 => self.tag(24),
            DataType::LargeUtf8 => self.tag(25),
            DataType::Utf8View => self.tag(26),
            DataType::List(field) => {
                self.tag(27);
                self.field(field);
            }
            DataType::ListView(field) => {
                self.tag(28);
                self.field(field);
            }
            DataType::FixedSizeList(field, width) => {
                self.tag(29);
                self.field(field);
                self.signed_u32(*width);
            }
            DataType::LargeList(field) => {
                self.tag(30);
                self.field(field);
            }
            DataType::LargeListView(field) => {
                self.tag(31);
                self.field(field);
            }
            DataType::Struct(fields) => {
                self.tag(32);
                self.fields(fields);
            }
            DataType::Union(fields, mode) => {
                self.tag(33);
                self.union_fields(fields);
                self.union_mode(*mode);
            }
            DataType::Dictionary(key, value) => {
                self.tag(34);
                self.data_type(key);
                self.data_type(value);
            }
            DataType::Decimal128(precision, scale) => {
                self.tag(35);
                self.tag(*precision);
                self.signed_byte(*scale);
            }
            DataType::Decimal256(precision, scale) => {
                self.tag(36);
                self.tag(*precision);
                self.signed_byte(*scale);
            }
            DataType::Map(field, sorted) => {
                self.tag(37);
                self.field(field);
                self.boolean(*sorted);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.tag(38);
                self.field(run_ends);
                self.field(values);
            }
        }
    }

    fn time_unit(&mut self, unit: TimeUnit) {
        self.tag(match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        });
    }

    fn interval_unit(&mut self, unit: IntervalUnit) {
        self.tag(match unit {
            IntervalUnit::YearMonth => 0,
            IntervalUnit::DayTime => 1,
            IntervalUnit::MonthDayNano => 2,
        });
    }

    fn union_mode(&mut self, mode: UnionMode) {
        self.tag(match mode {
            UnionMode::Sparse => 0,
            UnionMode::Dense => 1,
        });
    }

    fn metadata(&mut self, metadata: &HashMap<String, String>) {
        let mut entries = metadata.iter().collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
        self.length(entries.len());
        for (key, value) in entries {
            self.string(key);
            self.string(value);
        }
    }

    fn optional_string(&mut self, value: Option<&str>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.string(value);
            }
        }
    }

    fn optional_i64(&mut self, value: Option<i64>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.digest.update(value.to_be_bytes());
            }
        }
    }

    fn optional_boolean(&mut self, value: Option<bool>) {
        match value {
            None => self.tag(0),
            Some(value) => {
                self.tag(1);
                self.boolean(value);
            }
        }
    }

    fn string(&mut self, value: &str) {
        self.length(value.len());
        self.digest.update(value.as_bytes());
    }

    fn length(&mut self, value: usize) {
        self.digest.update((value as u64).to_be_bytes());
    }

    fn signed_u32(&mut self, value: i32) {
        self.digest.update(value.to_be_bytes());
    }

    fn signed_byte(&mut self, value: i8) {
        self.digest.update(value.to_be_bytes());
    }

    fn boolean(&mut self, value: bool) {
        self.tag(u8::from(value));
    }

    fn tag(&mut self, value: u8) {
        self.digest.update([value]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{canonical_catalog_schema_digest_v1, canonical_catalog_schema_hash_v1};

    #[test]
    fn canonical_catalog_schema_hash_v1_has_a_stable_golden_value() {
        let schema = Schema::new_with_metadata(
            vec![Field::new("price", DataType::Float64, false)
                .with_metadata(HashMap::from([("unit".to_owned(), "cny".to_owned())]))],
            HashMap::from([("source".to_owned(), "feed".to_owned())]),
        );

        let expected = "sha256-v1:ef1c08e2fa3e43f5a32554934a53dec5e9a5e8c639b2af165b348f3fd8b951ab";
        assert_eq!(canonical_catalog_schema_hash_v1(&schema), expected);
        assert_eq!(
            canonical_catalog_schema_digest_v1(&schema),
            hex_digest(expected.strip_prefix("sha256-v1:").unwrap())
        );
    }

    #[test]
    fn canonical_digest_sorts_metadata_and_captures_nested_content() {
        let mut left_metadata = HashMap::new();
        left_metadata.insert("currency".to_owned(), "cny".to_owned());
        left_metadata.insert("source".to_owned(), "feed".to_owned());
        let mut right_metadata = HashMap::new();
        right_metadata.insert("source".to_owned(), "feed".to_owned());
        right_metadata.insert("currency".to_owned(), "cny".to_owned());

        let left = Schema::new_with_metadata(
            vec![Field::new(
                "levels",
                DataType::List(Arc::new(Field::new("price", DataType::Float64, false))),
                false,
            )],
            left_metadata,
        );
        let reordered = Schema::new_with_metadata(left.fields().to_vec(), right_metadata);
        let changed_nested = Schema::new_with_metadata(
            vec![Field::new(
                "levels",
                DataType::List(Arc::new(Field::new("costs", DataType::Float64, false))),
                false,
            )],
            left.metadata().clone(),
        );

        assert_eq!(
            canonical_catalog_schema_digest_v1(&left),
            canonical_catalog_schema_digest_v1(&reordered)
        );
        assert_ne!(
            canonical_catalog_schema_digest_v1(&left),
            canonical_catalog_schema_digest_v1(&changed_nested)
        );
    }

    fn hex_digest(value: &str) -> [u8; 32] {
        let mut output = [0_u8; 32];
        for (index, byte) in output.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
        }
        output
    }
}
