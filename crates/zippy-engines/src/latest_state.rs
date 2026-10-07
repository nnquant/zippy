use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SchemaRef, SegmentTableView, ZippyError};

pub(crate) struct LatestColumnarState {
    schema: SchemaRef,
    key_fields: Vec<String>,
    key_indices: Vec<usize>,
    key_to_slot: BTreeMap<Vec<String>, usize>,
    allocated_slots: usize,
    columns: Vec<LatestColumnStore>,
    free_slots: Vec<usize>,
}

#[derive(Debug)]
pub(crate) struct LatestUpdateSet {
    slots: Vec<usize>,
}

impl LatestUpdateSet {
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

enum LatestColumnStore {
    Int64 {
        field_name: String,
        nullable: bool,
        values: Vec<i64>,
        valid: Vec<bool>,
    },
    Float64 {
        field_name: String,
        nullable: bool,
        values: Vec<f64>,
        valid: Vec<bool>,
    },
    Utf8 {
        field_name: String,
        nullable: bool,
        values: Vec<String>,
        valid: Vec<bool>,
    },
    TimestampNanosecond {
        field_name: String,
        nullable: bool,
        timezone: Option<Arc<str>>,
        values: Vec<i64>,
        valid: Vec<bool>,
    },
}

#[derive(Clone)]
enum LatestCellValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampNanosecond(i64),
}

struct PreparedLatestUpdate {
    rows: BTreeMap<Vec<String>, Vec<LatestCellValue>>,
}

impl LatestColumnarState {
    pub(crate) fn new(schema: SchemaRef, key_fields: Vec<String>) -> Result<Self> {
        if key_fields.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "latest state requires at least one key field".to_string(),
            });
        }

        let mut seen = BTreeSet::new();
        let key_indices = key_fields
            .iter()
            .map(|field_name| {
                if !seen.insert(field_name.clone()) {
                    return Err(ZippyError::InvalidConfig {
                        reason: format!("duplicate latest key field field=[{}]", field_name),
                    });
                }
                let (index, field) = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name() == field_name)
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!("missing latest key field field=[{}]", field_name),
                    })?;
                if field.data_type() != &DataType::Utf8 {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!("latest key field must be utf8 field=[{}]", field_name),
                    });
                }
                Ok(index)
            })
            .collect::<Result<Vec<_>>>()?;

        let columns = schema
            .fields()
            .iter()
            .map(|field| LatestColumnStore::new(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema,
            key_fields,
            key_indices,
            key_to_slot: BTreeMap::new(),
            allocated_slots: 0,
            columns,
            free_slots: Vec::new(),
        })
    }

    pub(crate) fn key_fields(&self) -> &[String] {
        &self.key_fields
    }

    pub(crate) fn len(&self) -> usize {
        self.key_to_slot.len()
    }

    pub(crate) fn apply_batch(&mut self, table: &SegmentTableView) -> Result<LatestUpdateSet> {
        if table.schema().as_ref() != self.schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: "latest state input schema mismatch".to_string(),
            });
        }

        let update = self.prepare_batch_update(table)?;
        self.commit_batch_update(update)
    }

    pub(crate) fn materialize_update(&self, update: &LatestUpdateSet) -> Result<RecordBatch> {
        self.materialize_slots(&update.slots)
    }

    /// Validates every key before removing any rows; missing or repeated keys are harmless.
    pub(crate) fn delete_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let arrays = self
            .key_fields
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!("missing utf8 delete key field=[{name}]"),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut keys = BTreeSet::new();
        for row in 0..batch.num_rows() {
            if arrays.iter().any(|array| array.is_null(row)) {
                return Err(ZippyError::SchemaMismatch {
                    reason: "delete key contains null".to_owned(),
                });
            }
            keys.insert(
                arrays
                    .iter()
                    .map(|array| array.value(row).to_owned())
                    .collect::<Vec<_>>(),
            );
        }
        for key in keys {
            if let Some(slot) = self.key_to_slot.remove(&key) {
                for column in &mut self.columns {
                    column.clear_slot(slot);
                }
                self.free_slots.push(slot);
            }
        }
        Ok(())
    }

    pub(crate) fn materialize_snapshot(&self) -> Result<RecordBatch> {
        let slots = self.key_to_slot.values().copied().collect::<Vec<_>>();
        self.materialize_slots(&slots)
    }

    pub(crate) fn select_keys(&self, keys: &[Vec<String>]) -> Result<RecordBatch> {
        let slots = keys
            .iter()
            .map(|key| {
                self.key_to_slot
                    .get(key)
                    .copied()
                    .ok_or(ZippyError::InvalidState {
                        status: "requested key is absent from the latest state",
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        self.materialize_slots(&slots)
    }

    fn prepare_batch_update(&self, table: &SegmentTableView) -> Result<PreparedLatestUpdate> {
        let arrays = self
            .schema
            .fields()
            .iter()
            .map(|field| table.column(field.name()))
            .collect::<Result<Vec<_>>>()?;
        let key_arrays = self
            .key_indices
            .iter()
            .zip(&self.key_fields)
            .map(|(index, field_name)| {
                arrays[*index]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!("latest key field must be utf8 field=[{}]", field_name),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut rows = BTreeMap::new();

        for row_index in 0..table.num_rows() {
            let mut key = Vec::with_capacity(key_arrays.len());
            for (key_array, field_name) in key_arrays.iter().zip(&self.key_fields) {
                if key_array.is_null(row_index) {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!("latest key field contains null field=[{}]", field_name),
                    });
                }
                key.push(key_array.value(row_index).to_string());
            }

            let cells = self
                .columns
                .iter()
                .zip(&arrays)
                .map(|(column, array)| column.cell_value(array, row_index))
                .collect::<Result<Vec<_>>>()?;
            rows.insert(key, cells);
        }

        Ok(PreparedLatestUpdate { rows })
    }

    fn commit_batch_update(&mut self, update: PreparedLatestUpdate) -> Result<LatestUpdateSet> {
        let mut updated_slots = Vec::with_capacity(update.rows.len());

        for (key, cells) in update.rows {
            let slot = match self.key_to_slot.get(&key).copied() {
                Some(slot) => slot,
                None => {
                    let slot = self.allocate_slot();
                    self.key_to_slot.insert(key, slot);
                    slot
                }
            };
            for (column, cell) in self.columns.iter_mut().zip(&cells) {
                column.write_cell(slot, cell)?;
            }
            updated_slots.push(slot);
        }

        Ok(LatestUpdateSet {
            slots: updated_slots,
        })
    }

    fn allocate_slot(&mut self) -> usize {
        if let Some(slot) = self.free_slots.pop() {
            return slot;
        }
        let slot = self.allocated_slots;
        self.allocated_slots += 1;
        for column in &mut self.columns {
            column.push_default();
        }
        slot
    }

    fn materialize_slots(&self, slots: &[usize]) -> Result<RecordBatch> {
        if slots.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        // Slots are append-only and every column grows with their allocation.
        // Validate once for the whole projection, not once per cell.
        if slots.iter().any(|slot| *slot >= self.allocated_slots) {
            return Err(ZippyError::InvalidState {
                status: "latest state slot is out of bounds",
            });
        }
        let columns = self
            .columns
            .iter()
            .map(|column| column.materialize_slots(slots))
            .collect();
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|error| ZippyError::Io {
            reason: format!("failed to build latest state batch error=[{}]", error),
        })
    }
}

impl LatestColumnStore {
    fn clear_slot(&mut self, slot: usize) {
        match self {
            Self::Int64 { values, valid, .. } | Self::TimestampNanosecond { values, valid, .. } => {
                values[slot] = 0;
                valid[slot] = false;
            }
            Self::Float64 { values, valid, .. } => {
                values[slot] = 0.0;
                valid[slot] = false;
            }
            Self::Utf8 { values, valid, .. } => {
                values[slot] = String::new();
                valid[slot] = false;
            }
        }
    }

    fn new(field: &Field) -> Result<Self> {
        let field_name = field.name().clone();
        let nullable = field.is_nullable();
        match field.data_type() {
            DataType::Int64 => Ok(Self::Int64 {
                field_name,
                nullable,
                values: Vec::new(),
                valid: Vec::new(),
            }),
            DataType::Float64 => Ok(Self::Float64 {
                field_name,
                nullable,
                values: Vec::new(),
                valid: Vec::new(),
            }),
            DataType::Utf8 => Ok(Self::Utf8 {
                field_name,
                nullable,
                values: Vec::new(),
                valid: Vec::new(),
            }),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Ok(Self::TimestampNanosecond {
                field_name,
                nullable,
                timezone: timezone.clone(),
                values: Vec::new(),
                valid: Vec::new(),
            }),
            other => Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "unsupported latest state field type field=[{}] type=[{:?}]",
                    field.name(),
                    other
                ),
            }),
        }
    }

    fn push_default(&mut self) {
        match self {
            Self::Int64 { values, valid, .. } => {
                values.push(0);
                valid.push(false);
            }
            Self::Float64 { values, valid, .. } => {
                values.push(0.0);
                valid.push(false);
            }
            Self::Utf8 { values, valid, .. } => {
                values.push(String::new());
                valid.push(false);
            }
            Self::TimestampNanosecond { values, valid, .. } => {
                values.push(0);
                valid.push(false);
            }
        }
    }

    fn cell_value(&self, array: &ArrayRef, row_index: usize) -> Result<LatestCellValue> {
        if array.is_null(row_index) {
            self.ensure_nullable()?;
            return Ok(LatestCellValue::Null);
        }

        match self {
            Self::Int64 { field_name, .. } => {
                let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ZippyError::SchemaMismatch {
                        reason: format!(
                            "int64 latest field downcast failed field=[{}]",
                            field_name
                        ),
                    }
                })?;
                Ok(LatestCellValue::Int64(values.value(row_index)))
            }
            Self::Float64 { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!(
                            "float64 latest field downcast failed field=[{}]",
                            field_name
                        ),
                    })?;
                Ok(LatestCellValue::Float64(values.value(row_index)))
            }
            Self::Utf8 { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!("utf8 latest field downcast failed field=[{}]", field_name),
                    })?;
                Ok(LatestCellValue::Utf8(values.value(row_index).to_string()))
            }
            Self::TimestampNanosecond { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!(
                            "timestamp latest field downcast failed field=[{}]",
                            field_name
                        ),
                    })?;
                Ok(LatestCellValue::TimestampNanosecond(
                    values.value(row_index),
                ))
            }
        }
    }

    fn write_cell(&mut self, slot: usize, cell: &LatestCellValue) -> Result<()> {
        match (self, cell) {
            (
                Self::Int64 {
                    values,
                    valid,
                    field_name: _,
                    ..
                },
                LatestCellValue::Int64(value),
            ) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (
                Self::Float64 {
                    values,
                    valid,
                    field_name: _,
                    ..
                },
                LatestCellValue::Float64(value),
            ) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (
                Self::Utf8 {
                    values,
                    valid,
                    field_name: _,
                    ..
                },
                LatestCellValue::Utf8(value),
            ) => {
                values[slot] = value.clone();
                valid[slot] = true;
            }
            (
                Self::TimestampNanosecond {
                    values,
                    valid,
                    field_name: _,
                    ..
                },
                LatestCellValue::TimestampNanosecond(value),
            ) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (column, LatestCellValue::Null) => {
                column.ensure_nullable()?;
                column.set_null(slot);
            }
            (column, _) => {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "latest cell type does not match column field=[{}]",
                        column.field_name()
                    ),
                });
            }
        }
        Ok(())
    }

    fn materialize_slots(&self, slots: &[usize]) -> ArrayRef {
        match self {
            Self::Int64 { values, valid, .. } => {
                let mut builder = Int64Builder::with_capacity(slots.len());
                for slot in slots {
                    if valid[*slot] {
                        builder.append_value(values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            Self::Float64 { values, valid, .. } => {
                let mut builder = Float64Builder::with_capacity(slots.len());
                for slot in slots {
                    if valid[*slot] {
                        builder.append_value(values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            Self::Utf8 { values, valid, .. } => {
                let value_capacity = slots
                    .iter()
                    .filter(|slot| valid[**slot])
                    .map(|slot| values[*slot].len())
                    .sum();
                let mut builder = StringBuilder::with_capacity(slots.len(), value_capacity);
                for slot in slots {
                    if valid[*slot] {
                        builder.append_value(&values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            Self::TimestampNanosecond {
                values,
                valid,
                timezone,
                ..
            } => {
                let materialized = slots
                    .iter()
                    .map(|slot| valid[*slot].then_some(values[*slot]))
                    .collect::<Vec<_>>();
                let array = TimestampNanosecondArray::from(materialized);
                let array = match timezone {
                    Some(timezone) => array.with_timezone(timezone.clone()),
                    None => array,
                };
                Arc::new(array) as ArrayRef
            }
        }
    }

    fn ensure_nullable(&self) -> Result<()> {
        if self.nullable() {
            return Ok(());
        }
        Err(ZippyError::SchemaMismatch {
            reason: format!(
                "non-nullable latest field received null field=[{}]",
                self.field_name()
            ),
        })
    }

    fn nullable(&self) -> bool {
        match self {
            Self::Int64 { nullable, .. }
            | Self::Float64 { nullable, .. }
            | Self::Utf8 { nullable, .. }
            | Self::TimestampNanosecond { nullable, .. } => *nullable,
        }
    }

    fn field_name(&self) -> &str {
        match self {
            Self::Int64 { field_name, .. }
            | Self::Float64 { field_name, .. }
            | Self::Utf8 { field_name, .. }
            | Self::TimestampNanosecond { field_name, .. } => field_name,
        }
    }

    fn set_null(&mut self, slot: usize) {
        match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Utf8 { valid, .. }
            | Self::TimestampNanosecond { valid, .. } => {
                valid[slot] = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use zippy_core::SegmentTableView;

    use super::*;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, true),
            Field::new("exchange_id", DataType::Utf8, false),
            Field::new("last_price", DataType::Float64, true),
            Field::new("volume", DataType::Int64, false),
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    fn batch(
        instrument_ids: Vec<Option<&str>>,
        exchange_ids: Vec<&str>,
        prices: Vec<Option<f64>>,
        volumes: Vec<i64>,
        dts: Vec<i64>,
    ) -> SegmentTableView {
        SegmentTableView::from_record_batch(
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
                    Arc::new(StringArray::from(exchange_ids)) as ArrayRef,
                    Arc::new(Float64Array::from(prices)) as ArrayRef,
                    Arc::new(Int64Array::from(volumes)) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(dts).with_timezone("UTC")) as ArrayRef,
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn latest_state_outputs_update_and_snapshot_in_key_order() {
        let mut state =
            LatestColumnarState::new(schema(), vec!["instrument_id".to_string()]).unwrap();

        let update = state
            .apply_batch(&batch(
                vec![Some("IH2606"), Some("IF2606"), Some("IF2606")],
                vec!["CFFEX", "CFFEX", "CFFEX"],
                vec![Some(2711.0), Some(4102.5), Some(4103.5)],
                vec![2, 1, 3],
                vec![102, 101, 103],
            ))
            .unwrap();

        let delta = state.materialize_update(&update).unwrap();
        let snapshot = state.materialize_snapshot().unwrap();
        let delta_ids = delta
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let delta_prices = delta
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let snapshot_ids = snapshot
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(delta.num_rows(), 2);
        assert_eq!(delta_ids.value(0), "IF2606");
        assert_eq!(delta_prices.value(0), 4103.5);
        assert_eq!(delta_ids.value(1), "IH2606");
        assert_eq!(snapshot_ids.value(0), "IF2606");
        assert_eq!(snapshot_ids.value(1), "IH2606");

        // Inserts on both sides retain deterministic key order in full snapshots and deltas.
        let inserted = state
            .apply_batch(&batch(
                vec![Some("ZZ"), Some("AA")],
                vec!["CFFEX", "CFFEX"],
                vec![Some(1.0), Some(2.0)],
                vec![4, 5],
                vec![104, 105],
            ))
            .unwrap();
        let inserted_delta = state.materialize_update(&inserted).unwrap();
        assert_eq!(
            inserted_delta
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["AA", "ZZ"])
        );
        let full = state.materialize_snapshot().unwrap();
        assert_eq!(
            full.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["AA", "IF2606", "IH2606", "ZZ"])
        );

        let existing = state
            .apply_batch(&batch(
                vec![Some("IH2606"), Some("AA"), Some("IH2606")],
                vec!["CFFEX"; 3],
                vec![Some(3.0), Some(4.0), Some(5.0)],
                vec![6, 7, 8],
                vec![106, 107, 108],
            ))
            .unwrap();
        let updated_delta = state.materialize_update(&existing).unwrap();
        assert_eq!(
            updated_delta
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec!["AA", "IH2606"])
        );
        assert_eq!(
            updated_delta
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap(),
            &Float64Array::from(vec![4.0, 5.0])
        );
        assert_eq!(state.len(), 4);
        assert!(state.materialize_slots(&[usize::MAX]).is_err());
    }

    #[test]
    fn latest_state_preserves_nullable_value_and_rejects_null_key_without_mutation() {
        let mut state =
            LatestColumnarState::new(schema(), vec!["instrument_id".to_string()]).unwrap();
        state
            .apply_batch(&batch(
                vec![Some("IF2606")],
                vec!["CFFEX"],
                vec![Some(4102.5)],
                vec![1],
                vec![101],
            ))
            .unwrap();

        let error = state
            .apply_batch(&batch(
                vec![None, Some("IH2606")],
                vec!["CFFEX", "CFFEX"],
                vec![None, Some(2711.0)],
                vec![2, 3],
                vec![102, 103],
            ))
            .unwrap_err();
        assert!(error.to_string().contains("contains null"));

        let snapshot = state.materialize_snapshot().unwrap();
        let ids = snapshot
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prices = snapshot
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(snapshot.num_rows(), 1);
        assert_eq!(ids.value(0), "IF2606");
        assert_eq!(prices.value(0), 4102.5);
    }
}
