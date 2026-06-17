use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SchemaRef, ZippyError};

const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x00000100000001b3;

/// Configuration for deterministic stream table shard routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardConfig {
    shard_key: Vec<String>,
    shard_nums: usize,
    shard_version: u32,
}

impl ShardConfig {
    /// Creates a validated shard routing configuration.
    pub fn new(shard_key: Vec<String>, shard_nums: usize, shard_version: u32) -> Result<Self> {
        if shard_key.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "shard key must not be empty".to_string(),
            });
        }
        if shard_nums == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "shard nums must be greater than zero".to_string(),
            });
        }
        if shard_version == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "shard version must be greater than zero".to_string(),
            });
        }

        let mut names = BTreeSet::new();
        for column in &shard_key {
            if column.is_empty() {
                return Err(ZippyError::InvalidConfig {
                    reason: "shard key column must not be empty".to_string(),
                });
            }
            if !names.insert(column) {
                return Err(ZippyError::InvalidConfig {
                    reason: format!("duplicate shard key column=[{}]", column),
                });
            }
        }

        Ok(Self {
            shard_key,
            shard_nums,
            shard_version,
        })
    }

    /// Returns the validated shard-key column names.
    pub fn shard_key(&self) -> &[String] {
        &self.shard_key
    }

    /// Returns the configured shard count.
    pub fn shard_nums(&self) -> usize {
        self.shard_nums
    }

    /// Returns the stable hash version.
    pub fn shard_version(&self) -> u32 {
        self.shard_version
    }
}

/// Scalar shard-key value used by tests and future FFI bindings.
#[derive(Debug, Clone, PartialEq)]
pub enum ShardValue {
    Utf8(String),
    LargeUtf8(String),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Date32(i32),
    Timestamp { value: i64, unit: TimeUnit },
}

/// Routes stream table rows to deterministic shard indexes.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    schema: SchemaRef,
    config: ShardConfig,
    columns: Vec<ShardColumn>,
}

#[derive(Debug, Clone)]
struct ShardColumn {
    name: String,
    index: usize,
    dtype: ShardDataType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShardDataType {
    Utf8,
    LargeUtf8,
    Int64,
    UInt64,
    Float64,
    Date32,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
}

impl ShardDataType {
    fn try_from_data_type(column: &str, data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Utf8 => Ok(Self::Utf8),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8),
            DataType::Int64 => Ok(Self::Int64),
            DataType::UInt64 => Ok(Self::UInt64),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Date32 => Ok(Self::Date32),
            DataType::Timestamp(TimeUnit::Second, _) => Ok(Self::TimestampSecond),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Self::TimestampMillisecond),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Self::TimestampMicrosecond),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Self::TimestampNanosecond),
            other => Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "unsupported shard key type column=[{}] type=[{}]",
                    column, other
                ),
            }),
        }
    }

    fn type_tag(self) -> &'static [u8] {
        match self {
            Self::Utf8 => b"utf8",
            Self::LargeUtf8 => b"large_utf8",
            Self::Int64 => b"int64",
            Self::UInt64 => b"uint64",
            Self::Float64 => b"float64",
            Self::Date32 => b"date32",
            Self::TimestampSecond => b"timestamp_second",
            Self::TimestampMillisecond => b"timestamp_millisecond",
            Self::TimestampMicrosecond => b"timestamp_microsecond",
            Self::TimestampNanosecond => b"timestamp_nanosecond",
        }
    }
}

impl ShardRouter {
    /// Creates a shard router and validates all shard key columns.
    pub fn try_new(schema: SchemaRef, config: ShardConfig) -> Result<Self> {
        let columns = config
            .shard_key
            .iter()
            .map(|column| {
                let (index, field) =
                    schema
                        .column_with_name(column)
                        .ok_or_else(|| ZippyError::SchemaMismatch {
                            reason: format!("missing shard key column=[{}]", column),
                        })?;
                let dtype = ShardDataType::try_from_data_type(column, field.data_type())?;
                Ok(ShardColumn {
                    name: column.clone(),
                    index,
                    dtype,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema,
            config,
            columns,
        })
    }

    /// Routes every row in a record batch to a shard index.
    pub fn route_batch(&self, batch: &RecordBatch) -> Result<Vec<usize>> {
        self.validate_batch_schema(batch)?;
        (0..batch.num_rows())
            .map(|row_index| self.route_batch_row(batch, row_index))
            .collect()
    }

    /// Splits a batch by shard while preserving original row order within each shard.
    pub fn split_batch(&self, batch: &RecordBatch) -> Result<Vec<Option<RecordBatch>>> {
        self.validate_batch_schema(batch)?;
        let mut row_groups = vec![Vec::<u32>::new(); self.config.shard_nums];
        for row_index in 0..batch.num_rows() {
            let shard_index = self.route_batch_row(batch, row_index)?;
            row_groups[shard_index].push(checked_row_index(row_index)?);
        }

        row_groups
            .into_iter()
            .map(|row_indices| {
                if row_indices.is_empty() {
                    return Ok(None);
                }
                let indices = UInt32Array::from(row_indices);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        take(column.as_ref(), &indices, None).map_err(|error| ZippyError::Io {
                            reason: format!(
                                "failed to split stream table shard batch error=[{}]",
                                error
                            ),
                        })
                    })
                    .collect::<Result<Vec<ArrayRef>>>()?;
                let split =
                    RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|error| {
                        ZippyError::Io {
                            reason: format!(
                                "failed to build stream table shard batch error=[{}]",
                                error
                            ),
                        }
                    })?;
                Ok(Some(split))
            })
            .collect()
    }

    /// Routes explicit shard-key values to a shard index.
    pub fn route_record_values(&self, values: &[ShardValue]) -> Result<usize> {
        if values.len() != self.columns.len() {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "shard value count mismatch expected=[{}] actual=[{}]",
                    self.columns.len(),
                    values.len()
                ),
            });
        }

        let mut hasher = StableFnvHasher::new(self.config.shard_version);
        for (column, value) in self.columns.iter().zip(values.iter()) {
            hash_shard_value(&mut hasher, column, value)?;
        }
        Ok((hasher.finish() % self.config.shard_nums as u64) as usize)
    }

    fn validate_batch_schema(&self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: "stream table shard batch schema does not match router schema".to_string(),
            });
        }
        Ok(())
    }

    fn route_batch_row(&self, batch: &RecordBatch, row_index: usize) -> Result<usize> {
        let mut hasher = StableFnvHasher::new(self.config.shard_version);
        for column in &self.columns {
            hash_batch_value(&mut hasher, batch, column, row_index)?;
        }
        Ok((hasher.finish() % self.config.shard_nums as u64) as usize)
    }
}

struct StableFnvHasher {
    state: u64,
}

impl StableFnvHasher {
    fn new(shard_version: u32) -> Self {
        let mut hasher = Self {
            state: FNV_OFFSET_BASIS,
        };
        hasher.write_bytes(b"zippy_stream_table_shard");
        hasher.write_bytes(&shard_version.to_le_bytes());
        hasher
    }

    fn write_tagged_value(&mut self, tag: &[u8], value: &[u8]) {
        self.write_len(tag.len());
        self.write_bytes(tag);
        self.write_len(value.len());
        self.write_bytes(value);
    }

    fn write_len(&mut self, len: usize) {
        self.write_bytes(&(len as u64).to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    fn finish(self) -> u64 {
        self.state
    }
}

fn hash_shard_value(
    hasher: &mut StableFnvHasher,
    column: &ShardColumn,
    value: &ShardValue,
) -> Result<()> {
    match (column.dtype, value) {
        (ShardDataType::Utf8, ShardValue::Utf8(value))
        | (ShardDataType::LargeUtf8, ShardValue::LargeUtf8(value)) => {
            hasher.write_tagged_value(column.dtype.type_tag(), value.as_bytes());
            Ok(())
        }
        (ShardDataType::Int64, ShardValue::Int64(value)) => {
            hasher.write_tagged_value(column.dtype.type_tag(), &value.to_le_bytes());
            Ok(())
        }
        (ShardDataType::UInt64, ShardValue::UInt64(value)) => {
            hasher.write_tagged_value(column.dtype.type_tag(), &value.to_le_bytes());
            Ok(())
        }
        (ShardDataType::Float64, ShardValue::Float64(value)) => {
            let bits = normalized_float64_bits_for_value(*value, &column.name)?;
            hasher.write_tagged_value(column.dtype.type_tag(), &bits.to_le_bytes());
            Ok(())
        }
        (ShardDataType::Date32, ShardValue::Date32(value)) => {
            hasher.write_tagged_value(column.dtype.type_tag(), &value.to_le_bytes());
            Ok(())
        }
        (
            ShardDataType::TimestampSecond,
            ShardValue::Timestamp {
                value,
                unit: TimeUnit::Second,
            },
        )
        | (
            ShardDataType::TimestampMillisecond,
            ShardValue::Timestamp {
                value,
                unit: TimeUnit::Millisecond,
            },
        )
        | (
            ShardDataType::TimestampMicrosecond,
            ShardValue::Timestamp {
                value,
                unit: TimeUnit::Microsecond,
            },
        )
        | (
            ShardDataType::TimestampNanosecond,
            ShardValue::Timestamp {
                value,
                unit: TimeUnit::Nanosecond,
            },
        ) => {
            hasher.write_tagged_value(column.dtype.type_tag(), &value.to_le_bytes());
            Ok(())
        }
        _ => Err(ZippyError::SchemaMismatch {
            reason: format!(
                "shard value type mismatch column=[{}] expected=[{}] actual=[{}]",
                column.name,
                column.dtype.name(),
                value.type_name()
            ),
        }),
    }
}

fn hash_batch_value(
    hasher: &mut StableFnvHasher,
    batch: &RecordBatch,
    column: &ShardColumn,
    row_index: usize,
) -> Result<()> {
    let array = batch.column(column.index);
    match column.dtype {
        ShardDataType::Utf8 => {
            let values = string_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(column.dtype.type_tag(), values.value(row_index).as_bytes());
        }
        ShardDataType::LargeUtf8 => {
            let values = large_string_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(column.dtype.type_tag(), values.value(row_index).as_bytes());
        }
        ShardDataType::Int64 => {
            let values = int64_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::UInt64 => {
            let values = uint64_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::Float64 => {
            let values = float64_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            let bits =
                normalized_float64_bits_for_row(values.value(row_index), &column.name, row_index)?;
            hasher.write_tagged_value(column.dtype.type_tag(), &bits.to_le_bytes());
        }
        ShardDataType::Date32 => {
            let values = date32_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::TimestampSecond => {
            let values = timestamp_second_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::TimestampMillisecond => {
            let values = timestamp_millisecond_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::TimestampMicrosecond => {
            let values = timestamp_microsecond_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
        ShardDataType::TimestampNanosecond => {
            let values = timestamp_nanosecond_array(array, &column.name)?;
            ensure_not_null(values, &column.name, row_index)?;
            hasher.write_tagged_value(
                column.dtype.type_tag(),
                &values.value(row_index).to_le_bytes(),
            );
        }
    }
    Ok(())
}

impl ShardDataType {
    fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "utf8",
            Self::LargeUtf8 => "large_utf8",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Float64 => "float64",
            Self::Date32 => "date32",
            Self::TimestampSecond => "timestamp_second",
            Self::TimestampMillisecond => "timestamp_millisecond",
            Self::TimestampMicrosecond => "timestamp_microsecond",
            Self::TimestampNanosecond => "timestamp_nanosecond",
        }
    }
}

impl ShardValue {
    fn type_name(&self) -> &'static str {
        match self {
            Self::Utf8(_) => "utf8",
            Self::LargeUtf8(_) => "large_utf8",
            Self::Int64(_) => "int64",
            Self::UInt64(_) => "uint64",
            Self::Float64(_) => "float64",
            Self::Date32(_) => "date32",
            Self::Timestamp {
                unit: TimeUnit::Second,
                ..
            } => "timestamp_second",
            Self::Timestamp {
                unit: TimeUnit::Millisecond,
                ..
            } => "timestamp_millisecond",
            Self::Timestamp {
                unit: TimeUnit::Microsecond,
                ..
            } => "timestamp_microsecond",
            Self::Timestamp {
                unit: TimeUnit::Nanosecond,
                ..
            } => "timestamp_nanosecond",
        }
    }
}

fn ensure_not_null(array: &dyn Array, column: &str, row_index: usize) -> Result<()> {
    if array.is_null(row_index) {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "shard key column [{}] is null at row [{}]",
                column, row_index
            ),
        });
    }
    Ok(())
}

fn normalized_float64_bits_for_row(value: f64, column: &str, row_index: usize) -> Result<u64> {
    if value.is_nan() {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "shard key column [{}] is nan at row [{}]",
                column, row_index
            ),
        });
    }
    Ok(normalized_float64_bits(value))
}

fn normalized_float64_bits_for_value(value: f64, column: &str) -> Result<u64> {
    if value.is_nan() {
        return Err(ZippyError::InvalidConfig {
            reason: format!("shard key value column [{}] is nan", column),
        });
    }
    Ok(normalized_float64_bits(value))
}

fn normalized_float64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn checked_row_index(row_index: usize) -> Result<u32> {
    if row_index > u32::MAX as usize {
        return Err(ZippyError::InvalidConfig {
            reason: format!(
                "stream table shard row index exceeds u32 max row=[{}]",
                row_index
            ),
        });
    }
    Ok(row_index as u32)
}

fn string_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| schema_type_mismatch(column, "utf8"))
}

fn large_string_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a LargeStringArray> {
    array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .ok_or_else(|| schema_type_mismatch(column, "large_utf8"))
}

fn int64_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Int64Array> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| schema_type_mismatch(column, "int64"))
}

fn uint64_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a UInt64Array> {
    array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| schema_type_mismatch(column, "uint64"))
}

fn float64_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Float64Array> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| schema_type_mismatch(column, "float64"))
}

fn date32_array<'a>(array: &'a ArrayRef, column: &str) -> Result<&'a Date32Array> {
    array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| schema_type_mismatch(column, "date32"))
}

fn timestamp_second_array<'a>(
    array: &'a ArrayRef,
    column: &str,
) -> Result<&'a TimestampSecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .ok_or_else(|| schema_type_mismatch(column, "timestamp_second"))
}

fn timestamp_millisecond_array<'a>(
    array: &'a ArrayRef,
    column: &str,
) -> Result<&'a TimestampMillisecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| schema_type_mismatch(column, "timestamp_millisecond"))
}

fn timestamp_microsecond_array<'a>(
    array: &'a ArrayRef,
    column: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| schema_type_mismatch(column, "timestamp_microsecond"))
}

fn timestamp_nanosecond_array<'a>(
    array: &'a ArrayRef,
    column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| schema_type_mismatch(column, "timestamp_nanosecond"))
}

fn schema_type_mismatch(column: &str, expected: &str) -> ZippyError {
    ZippyError::SchemaMismatch {
        reason: format!(
            "shard key array type mismatch column=[{}] expected=[{}]",
            column, expected
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Date32Array, Float64Array, Int64Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use zippy_core::ZippyError;

    use super::{normalized_float64_bits, ShardConfig, ShardRouter, ShardValue};

    fn batch_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, true),
            Field::new("venue", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]))
    }

    fn routing_batch() -> RecordBatch {
        RecordBatch::try_new(
            batch_schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("IF2606"),
                    Some("IH2606"),
                    Some("IF2606"),
                    Some("IC2606"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec!["CFFEX", "CFFEX", "CFFEX", "CFFEX"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3912.4, 2740.8, 3913.0, 5123.2])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn router_for(shard_key: Vec<&str>, shard_nums: usize) -> ShardRouter {
        let config = ShardConfig::new(
            shard_key
                .into_iter()
                .map(std::string::ToString::to_string)
                .collect(),
            shard_nums,
            1,
        )
        .unwrap();
        ShardRouter::try_new(batch_schema(), config).unwrap()
    }

    #[test]
    fn shard_router_config_exposes_validated_read_only_accessors() {
        let config = ShardConfig::new(vec!["instrument_id".to_string()], 4, 2).unwrap();

        assert_eq!(config.shard_key(), &["instrument_id".to_string()]);
        assert_eq!(config.shard_nums(), 4);
        assert_eq!(config.shard_version(), 2);
    }

    #[test]
    fn shard_router_config_rejects_invalid_values() {
        let cases = [
            (Vec::<String>::new(), 4, 1, "shard key must not be empty"),
            (
                vec!["".to_string()],
                4,
                1,
                "shard key column must not be empty",
            ),
            (
                vec!["instrument_id".to_string(), "instrument_id".to_string()],
                4,
                1,
                "duplicate shard key column=[instrument_id]",
            ),
            (
                vec!["instrument_id".to_string()],
                0,
                1,
                "shard nums must be greater than zero",
            ),
            (
                vec!["instrument_id".to_string()],
                4,
                0,
                "shard version must be greater than zero",
            ),
        ];

        for (shard_key, shard_nums, shard_version, expected) in cases {
            let error = ShardConfig::new(shard_key, shard_nums, shard_version).unwrap_err();
            assert!(matches!(error, ZippyError::InvalidConfig { .. }));
            assert!(
                error.to_string().contains(expected),
                "expected error to contain [{expected}], got [{error}]"
            );
        }
    }

    #[test]
    fn shard_router_same_utf8_key_maps_to_same_shard() {
        let router = router_for(vec!["instrument_id"], 8);
        let routes = router.route_batch(&routing_batch()).unwrap();

        assert_eq!(routes[0], routes[2]);
        assert!(routes.iter().all(|route| *route < 8));
    }

    #[test]
    fn shard_router_golden_single_utf8_key_uses_stable_hash_contract() {
        let router = router_for(vec!["instrument_id"], 8);
        let routes = router.route_batch(&routing_batch()).unwrap();
        let direct = router
            .route_record_values(&[ShardValue::Utf8("IF2606".to_string())])
            .unwrap();

        assert_eq!(routes[0], 1);
        assert_eq!(direct, 1);
    }

    #[test]
    fn shard_router_multi_key_routing_is_deterministic() {
        let router = router_for(vec!["instrument_id", "venue"], 16);
        let first = router.route_batch(&routing_batch()).unwrap();
        let second = router.route_batch(&routing_batch()).unwrap();
        let direct = router
            .route_record_values(&[
                ShardValue::Utf8("IF2606".to_string()),
                ShardValue::Utf8("CFFEX".to_string()),
            ])
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(first[0], direct);
    }

    #[test]
    fn shard_router_golden_multi_utf8_key_uses_stable_hash_contract() {
        let router = router_for(vec!["instrument_id", "venue"], 16);
        let routes = router.route_batch(&routing_batch()).unwrap();
        let direct = router
            .route_record_values(&[
                ShardValue::Utf8("IF2606".to_string()),
                ShardValue::Utf8("CFFEX".to_string()),
            ])
            .unwrap();

        assert_eq!(routes[0], 13);
        assert_eq!(direct, 13);
    }

    #[test]
    fn shard_router_golden_shard_version_changes_stable_hash_contract() {
        let config_v1 = ShardConfig::new(vec!["instrument_id".to_string()], 16, 1).unwrap();
        let config_v2 = ShardConfig::new(vec!["instrument_id".to_string()], 16, 2).unwrap();
        let router_v1 = ShardRouter::try_new(batch_schema(), config_v1).unwrap();
        let router_v2 = ShardRouter::try_new(batch_schema(), config_v2).unwrap();

        let route_v1 = router_v1
            .route_record_values(&[ShardValue::Utf8("IF2606".to_string())])
            .unwrap();
        let route_v2 = router_v2
            .route_record_values(&[ShardValue::Utf8("IF2606".to_string())])
            .unwrap();

        assert_eq!(route_v1, 1);
        assert_eq!(route_v2, 6);
    }

    #[test]
    fn shard_router_missing_key_column_is_rejected() {
        let config = ShardConfig::new(vec!["missing".to_string()], 4, 1).unwrap();
        let error = ShardRouter::try_new(batch_schema(), config).unwrap_err();

        assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
        assert!(error.to_string().contains("missing shard key column"));
    }

    #[test]
    fn shard_router_unsupported_key_type_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let config = ShardConfig::new(vec!["flag".to_string()], 4, 1).unwrap();
        let error = ShardRouter::try_new(schema, config).unwrap_err();

        assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
        assert!(error.to_string().contains("unsupported shard key type"));
    }

    #[test]
    fn shard_router_null_key_value_is_rejected_before_split_succeeds() {
        let batch = RecordBatch::try_new(
            batch_schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("IF2606"),
                    None,
                    Some("IH2606"),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec!["CFFEX", "CFFEX", "CFFEX"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            ],
        )
        .unwrap();
        let router = router_for(vec!["instrument_id"], 4);
        let error = router.split_batch(&batch).unwrap_err();

        assert!(error
            .to_string()
            .contains("shard key column [instrument_id] is null at row [1]"));
    }

    #[test]
    fn shard_router_split_preserves_per_shard_row_order_and_total_row_count() {
        let router = router_for(vec!["instrument_id"], 4);
        let batch = routing_batch();
        let routes = router.route_batch(&batch).unwrap();
        let splits = router.split_batch(&batch).unwrap();

        assert_eq!(splits.len(), 4);
        assert_eq!(
            splits
                .iter()
                .map(|split| split.as_ref().map_or(0, RecordBatch::num_rows))
                .sum::<usize>(),
            batch.num_rows()
        );

        for (shard_index, split) in splits.iter().enumerate() {
            let expected = routes
                .iter()
                .enumerate()
                .filter_map(|(row, route)| (*route == shard_index).then_some(row as i64))
                .collect::<Vec<_>>();
            let Some(split) = split else {
                assert!(expected.is_empty());
                continue;
            };
            let seq = split
                .column_by_name("seq")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let actual = (0..seq.len())
                .map(|row| seq.value(row) - 1)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn shard_router_date32_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trade_date",
            DataType::Date32,
            false,
        )]));
        let config = ShardConfig::new(vec!["trade_date".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Date32Array::from(vec![20_000, 20_001])) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_large_utf8_batch_and_direct_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "instrument_id",
            DataType::LargeUtf8,
            false,
        )]));
        let config = ShardConfig::new(vec!["instrument_id".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(LargeStringArray::from(vec!["IF2606", "IH2606"])) as ArrayRef],
        )
        .unwrap();
        let routes = router.route_batch(&batch).unwrap();
        let direct = router
            .route_record_values(&[ShardValue::LargeUtf8("IF2606".to_string())])
            .unwrap();

        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0], direct);
    }

    #[test]
    fn shard_router_int64_batch_and_direct_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "strategy_id",
            DataType::Int64,
            false,
        )]));
        let config = ShardConfig::new(vec!["strategy_id".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![-1_i64, 42_i64])) as ArrayRef],
        )
        .unwrap();
        let routes = router.route_batch(&batch).unwrap();
        let direct = router
            .route_record_values(&[ShardValue::Int64(-1)])
            .unwrap();

        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0], direct);
    }

    #[test]
    fn shard_router_golden_int64_key_uses_stable_hash_contract() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "strategy_id",
            DataType::Int64,
            false,
        )]));
        let config = ShardConfig::new(vec!["strategy_id".to_string()], 8, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![-1_i64])) as ArrayRef],
        )
        .unwrap();
        let routes = router.route_batch(&batch).unwrap();
        let direct = router
            .route_record_values(&[ShardValue::Int64(-1)])
            .unwrap();

        assert_eq!(routes[0], 7);
        assert_eq!(direct, 7);
    }

    #[test]
    fn shard_router_uint64_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "account_id",
            DataType::UInt64,
            false,
        )]));
        let config = ShardConfig::new(vec!["account_id".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![1_u64, u64::MAX])) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_float64_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let config = ShardConfig::new(vec!["price".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![1.25_f64, -0.0_f64])) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_direct_float64_positive_and_negative_zero_route_together() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let config = ShardConfig::new(vec!["price".to_string()], 16, 1).unwrap();
        let router = ShardRouter::try_new(schema, config).unwrap();

        let positive_zero = router
            .route_record_values(&[ShardValue::Float64(0.0)])
            .unwrap();
        let negative_zero = router
            .route_record_values(&[ShardValue::Float64(-0.0)])
            .unwrap();

        assert_eq!(positive_zero, negative_zero);
        assert_eq!(normalized_float64_bits(0.0), normalized_float64_bits(-0.0));
    }

    #[test]
    fn shard_router_batch_float64_nan_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let config = ShardConfig::new(vec!["price".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![1.0, 2.0, f64::NAN])) as ArrayRef],
        )
        .unwrap();
        let error = router.route_batch(&batch).unwrap_err();

        assert!(error
            .to_string()
            .contains("shard key column [price] is nan at row [2]"));
    }

    #[test]
    fn shard_router_direct_float64_nan_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Float64,
            false,
        )]));
        let config = ShardConfig::new(vec!["price".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(schema, config).unwrap();
        let error = router
            .route_record_values(&[ShardValue::Float64(f64::NAN)])
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("shard key value column [price] is nan"));
    }

    #[test]
    fn shard_router_timestamp_ns_tz_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        )]));
        let config = ShardConfig::new(vec!["dt".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![
                    1_710_000_000_000_000_000_i64,
                    1_710_000_000_100_000_000_i64,
                ])
                .with_timezone("UTC"),
            ) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_timestamp_second_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        )]));
        let config = ShardConfig::new(vec!["dt".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampSecondArray::from(vec![1_710_000_000_i64, 1_710_000_001_i64])
                    .with_timezone("UTC"),
            ) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_timestamp_millisecond_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )]));
        let config = ShardConfig::new(vec!["dt".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampMillisecondArray::from(vec![1_710_000_000_000_i64, 1_710_000_001_000_i64])
                    .with_timezone("UTC"),
            ) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }

    #[test]
    fn shard_router_timestamp_microsecond_routing_smoke_test() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        let config = ShardConfig::new(vec!["dt".to_string()], 4, 1).unwrap();
        let router = ShardRouter::try_new(Arc::clone(&schema), config).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![
                    1_710_000_000_000_000_i64,
                    1_710_000_001_000_000_i64,
                ])
                .with_timezone("UTC"),
            ) as ArrayRef],
        )
        .unwrap();

        assert_eq!(router.route_batch(&batch).unwrap().len(), 2);
    }
}
