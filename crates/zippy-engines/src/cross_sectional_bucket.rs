use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SchemaRef, SegmentTableView, ZippyError};

#[derive(Clone)]
pub(crate) struct CrossSectionalBucketState {
    schema: SchemaRef,
    id_field: String,
    id_index: usize,
    key_to_slot: BTreeMap<String, usize>,
    slots: Vec<BucketSlot>,
    columns: Vec<BucketColumnStore>,
}

#[derive(Clone)]
struct BucketSlot {
    occupied: bool,
}

#[derive(Clone)]
enum BucketColumnStore {
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
enum BucketCellValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampNanosecond(i64),
}

struct BucketSlotSnapshot {
    slot: BucketSlot,
    cells: Vec<BucketCellValue>,
}

pub(crate) struct BucketUndoLog {
    original_slots_len: usize,
    key_entries: BTreeMap<String, Option<usize>>,
    slot_entries: BTreeMap<usize, BucketSlotSnapshot>,
}

impl CrossSectionalBucketState {
    pub(crate) fn new(schema: SchemaRef, id_field: &str) -> Result<Self> {
        let (id_index, field) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == id_field)
            .ok_or_else(|| ZippyError::SchemaMismatch {
                reason: format!(
                    "missing cross-sectional bucket id field field=[{}]",
                    id_field
                ),
            })?;
        if field.data_type() != &DataType::Utf8 {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "cross-sectional bucket id field must be utf8 field=[{}]",
                    id_field
                ),
            });
        }

        let columns = schema
            .fields()
            .iter()
            .map(|field| BucketColumnStore::new(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema,
            id_field: id_field.to_string(),
            id_index,
            key_to_slot: BTreeMap::new(),
            slots: Vec::new(),
            columns,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.key_to_slot.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.key_to_slot.clear();
        self.slots.clear();
        for column in &mut self.columns {
            column.clear();
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_row(&mut self, table: &SegmentTableView, row_index: usize) -> Result<()> {
        self.apply_row_inner(table, row_index, None)
    }

    pub(crate) fn begin_undo_log(&self) -> BucketUndoLog {
        BucketUndoLog {
            original_slots_len: self.slots.len(),
            key_entries: BTreeMap::new(),
            slot_entries: BTreeMap::new(),
        }
    }

    pub(crate) fn apply_row_with_undo(
        &mut self,
        table: &SegmentTableView,
        row_index: usize,
        undo: &mut BucketUndoLog,
    ) -> Result<()> {
        self.apply_row_inner(table, row_index, Some(undo))
    }

    pub(crate) fn clear_with_undo(&mut self, undo: &mut BucketUndoLog) {
        let keys = self.key_to_slot.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.record_key_for_rollback(&key, undo);
        }
        for slot in 0..self.slots.len() {
            self.record_slot_for_rollback(slot, undo);
        }
        self.clear();
    }

    pub(crate) fn rollback_undo(&mut self, undo: BucketUndoLog) {
        self.rollback(undo);
    }

    fn apply_row_inner(
        &mut self,
        table: &SegmentTableView,
        row_index: usize,
        mut undo: Option<&mut BucketUndoLog>,
    ) -> Result<()> {
        if table.schema().as_ref() != self.schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: "cross-sectional bucket input schema mismatch".to_string(),
            });
        }

        let arrays = self
            .schema
            .fields()
            .iter()
            .map(|field| table.column(field.name()))
            .collect::<Result<Vec<_>>>()?;
        let id_array = arrays[self.id_index]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ZippyError::SchemaMismatch {
                reason: format!(
                    "cross-sectional bucket id field must be utf8 field=[{}]",
                    self.id_field
                ),
            })?;
        if id_array.is_null(row_index) {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "cross-sectional bucket id field contains null field=[{}]",
                    self.id_field
                ),
            });
        }

        let key = id_array.value(row_index).to_string();
        let cells = self
            .columns
            .iter()
            .zip(&arrays)
            .map(|(column, array)| column.cell_value(array, row_index))
            .collect::<Result<Vec<_>>>()?;

        if let Some(undo) = undo.as_mut() {
            self.record_key_for_rollback(&key, undo);
        }
        let slot = match self.key_to_slot.get(&key).copied() {
            Some(slot) => slot,
            None => self.allocate_slot(),
        };
        if let Some(undo) = undo.as_mut() {
            self.record_slot_for_rollback(slot, undo);
        }
        for (column, cell) in self.columns.iter_mut().zip(&cells) {
            column.write_cell(slot, cell)?;
        }
        self.key_to_slot.insert(key, slot);
        Ok(())
    }

    pub(crate) fn materialize(&self) -> Result<RecordBatch> {
        if self.key_to_slot.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let slots = self.key_to_slot.values().copied().collect::<Vec<_>>();
        let columns = self
            .columns
            .iter()
            .map(|column| column.materialize_slots(&slots, &self.slots))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|error| ZippyError::Io {
            reason: format!(
                "failed to build cross-sectional bucket batch error=[{}]",
                error
            ),
        })
    }

    fn allocate_slot(&mut self) -> usize {
        let slot = self.slots.len();
        self.slots.push(BucketSlot { occupied: true });
        for column in &mut self.columns {
            column.push_default();
        }
        slot
    }

    fn record_key_for_rollback(&self, key: &str, undo: &mut BucketUndoLog) {
        if undo.key_entries.contains_key(key) {
            return;
        }
        undo.key_entries
            .insert(key.to_string(), self.key_to_slot.get(key).copied());
    }

    fn record_slot_for_rollback(&self, slot: usize, undo: &mut BucketUndoLog) {
        if slot >= undo.original_slots_len || undo.slot_entries.contains_key(&slot) {
            return;
        }
        let snapshot = BucketSlotSnapshot {
            slot: self.slots[slot].clone(),
            cells: self
                .columns
                .iter()
                .map(|column| column.cell_at(slot))
                .collect(),
        };
        undo.slot_entries.insert(slot, snapshot);
    }

    fn rollback(&mut self, undo: BucketUndoLog) {
        for (slot, snapshot) in undo.slot_entries {
            if slot < self.slots.len() {
                self.slots[slot] = snapshot.slot;
                for (column, cell) in self.columns.iter_mut().zip(&snapshot.cells) {
                    column.restore_cell(slot, cell);
                }
            }
        }
        self.slots.truncate(undo.original_slots_len);
        for column in &mut self.columns {
            column.truncate(undo.original_slots_len);
        }
        for (key, previous_slot) in undo.key_entries {
            match previous_slot {
                Some(slot) => {
                    self.key_to_slot.insert(key, slot);
                }
                None => {
                    self.key_to_slot.remove(&key);
                }
            }
        }
    }
}

impl BucketColumnStore {
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
                    "unsupported cross-sectional bucket field type field=[{}] type=[{:?}]",
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

    fn clear(&mut self) {
        match self {
            Self::Int64 { values, valid, .. } => {
                values.clear();
                valid.clear();
            }
            Self::Float64 { values, valid, .. } => {
                values.clear();
                valid.clear();
            }
            Self::Utf8 { values, valid, .. } => {
                values.clear();
                valid.clear();
            }
            Self::TimestampNanosecond { values, valid, .. } => {
                values.clear();
                valid.clear();
            }
        }
    }

    fn truncate(&mut self, len: usize) {
        match self {
            Self::Int64 { values, valid, .. } => {
                values.truncate(len);
                valid.truncate(len);
            }
            Self::Float64 { values, valid, .. } => {
                values.truncate(len);
                valid.truncate(len);
            }
            Self::Utf8 { values, valid, .. } => {
                values.truncate(len);
                valid.truncate(len);
            }
            Self::TimestampNanosecond { values, valid, .. } => {
                values.truncate(len);
                valid.truncate(len);
            }
        }
    }

    fn cell_value(&self, array: &ArrayRef, row_index: usize) -> Result<BucketCellValue> {
        if array.is_null(row_index) {
            self.ensure_nullable()?;
            return Ok(BucketCellValue::Null);
        }

        match self {
            Self::Int64 { field_name, .. } => {
                let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ZippyError::SchemaMismatch {
                        reason: format!(
                            "int64 cross-sectional bucket field downcast failed field=[{}]",
                            field_name
                        ),
                    }
                })?;
                Ok(BucketCellValue::Int64(values.value(row_index)))
            }
            Self::Float64 { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!(
                            "float64 cross-sectional bucket field downcast failed field=[{}]",
                            field_name
                        ),
                    })?;
                Ok(BucketCellValue::Float64(values.value(row_index)))
            }
            Self::Utf8 { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!(
                            "utf8 cross-sectional bucket field downcast failed field=[{}]",
                            field_name
                        ),
                    })?;
                Ok(BucketCellValue::Utf8(values.value(row_index).to_string()))
            }
            Self::TimestampNanosecond { field_name, .. } => {
                let values = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!(
                            "timestamp cross-sectional bucket field downcast failed field=[{}]",
                            field_name
                        ),
                    })?;
                Ok(BucketCellValue::TimestampNanosecond(
                    values.value(row_index),
                ))
            }
        }
    }

    fn write_cell(&mut self, slot: usize, cell: &BucketCellValue) -> Result<()> {
        match (self, cell) {
            (Self::Int64 { values, valid, .. }, BucketCellValue::Int64(value)) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (Self::Float64 { values, valid, .. }, BucketCellValue::Float64(value)) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (Self::Utf8 { values, valid, .. }, BucketCellValue::Utf8(value)) => {
                values[slot] = value.clone();
                valid[slot] = true;
            }
            (
                Self::TimestampNanosecond { values, valid, .. },
                BucketCellValue::TimestampNanosecond(value),
            ) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (column, BucketCellValue::Null) => {
                column.ensure_nullable()?;
                column.set_null(slot);
            }
            (column, _) => {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "cross-sectional bucket cell type does not match column field=[{}]",
                        column.field_name()
                    ),
                });
            }
        }
        Ok(())
    }

    fn cell_at(&self, slot: usize) -> BucketCellValue {
        match self {
            Self::Int64 { values, valid, .. } => {
                if valid[slot] {
                    BucketCellValue::Int64(values[slot])
                } else {
                    BucketCellValue::Null
                }
            }
            Self::Float64 { values, valid, .. } => {
                if valid[slot] {
                    BucketCellValue::Float64(values[slot])
                } else {
                    BucketCellValue::Null
                }
            }
            Self::Utf8 { values, valid, .. } => {
                if valid[slot] {
                    BucketCellValue::Utf8(values[slot].clone())
                } else {
                    BucketCellValue::Null
                }
            }
            Self::TimestampNanosecond { values, valid, .. } => {
                if valid[slot] {
                    BucketCellValue::TimestampNanosecond(values[slot])
                } else {
                    BucketCellValue::Null
                }
            }
        }
    }

    fn restore_cell(&mut self, slot: usize, cell: &BucketCellValue) {
        match (self, cell) {
            (Self::Int64 { values, valid, .. }, BucketCellValue::Int64(value)) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (Self::Float64 { values, valid, .. }, BucketCellValue::Float64(value)) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (Self::Utf8 { values, valid, .. }, BucketCellValue::Utf8(value)) => {
                values[slot] = value.clone();
                valid[slot] = true;
            }
            (
                Self::TimestampNanosecond { values, valid, .. },
                BucketCellValue::TimestampNanosecond(value),
            ) => {
                values[slot] = *value;
                valid[slot] = true;
            }
            (column, BucketCellValue::Null) => column.set_null(slot),
            _ => {}
        }
    }

    fn materialize_slots(&self, slots: &[usize], slot_meta: &[BucketSlot]) -> Result<ArrayRef> {
        match self {
            Self::Int64 { values, valid, .. } => {
                let mut builder = Int64Builder::with_capacity(slots.len());
                for slot in slots {
                    ensure_occupied(slot_meta, *slot)?;
                    if valid[*slot] {
                        builder.append_value(values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            Self::Float64 { values, valid, .. } => {
                let mut builder = Float64Builder::with_capacity(slots.len());
                for slot in slots {
                    ensure_occupied(slot_meta, *slot)?;
                    if valid[*slot] {
                        builder.append_value(values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            Self::Utf8 { values, valid, .. } => {
                let value_capacity = slots
                    .iter()
                    .filter(|slot| valid[**slot])
                    .map(|slot| values[*slot].len())
                    .sum();
                let mut builder = StringBuilder::with_capacity(slots.len(), value_capacity);
                for slot in slots {
                    ensure_occupied(slot_meta, *slot)?;
                    if valid[*slot] {
                        builder.append_value(&values[*slot]);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            Self::TimestampNanosecond {
                values,
                valid,
                timezone,
                ..
            } => {
                let materialized = slots
                    .iter()
                    .map(|slot| {
                        ensure_occupied(slot_meta, *slot)?;
                        Ok(valid[*slot].then_some(values[*slot]))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let array = TimestampNanosecondArray::from(materialized);
                let array = match timezone {
                    Some(timezone) => array.with_timezone(timezone.clone()),
                    None => array,
                };
                Ok(Arc::new(array) as ArrayRef)
            }
        }
    }

    fn ensure_nullable(&self) -> Result<()> {
        if self.nullable() {
            return Ok(());
        }
        Err(ZippyError::SchemaMismatch {
            reason: format!(
                "non-nullable cross-sectional bucket field received null field=[{}]",
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

fn ensure_occupied(slots: &[BucketSlot], slot: usize) -> Result<()> {
    if slots.get(slot).is_some_and(|slot| slot.occupied) {
        return Ok(());
    }
    Err(ZippyError::InvalidState {
        status: "cross-sectional bucket slot is not occupied",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, false),
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("last_price", DataType::Float64, false),
        ]))
    }

    fn batch(instrument_ids: Vec<&str>, prices: Vec<f64>) -> SegmentTableView {
        let dts = (0..instrument_ids.len())
            .map(|index| 1_710_000_000_000_000_000_i64 + index as i64)
            .collect::<Vec<_>>();
        SegmentTableView::from_record_batch(
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(dts).with_timezone("UTC")) as ArrayRef,
                    Arc::new(Float64Array::from(prices)) as ArrayRef,
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn bucket_transaction_rollback_restores_touched_slots_and_new_keys() {
        let mut state = CrossSectionalBucketState::new(schema(), "instrument_id").unwrap();
        let initial = batch(vec!["A", "B"], vec![10.0, 20.0]);
        let mut initial_undo = state.begin_undo_log();
        state
            .apply_row_with_undo(&initial, 0, &mut initial_undo)
            .unwrap();
        state
            .apply_row_with_undo(&initial, 1, &mut initial_undo)
            .unwrap();

        let updates = batch(vec!["A", "C"], vec![11.0, 30.0]);
        let mut undo = state.begin_undo_log();
        state.apply_row_with_undo(&updates, 0, &mut undo).unwrap();
        state.apply_row_with_undo(&updates, 1, &mut undo).unwrap();
        state.rollback_undo(undo);

        let output = state.materialize().unwrap();
        let ids = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let prices = output
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(ids.value(0), "A");
        assert_eq!(prices.value(0), 10.0);
        assert_eq!(ids.value(1), "B");
        assert_eq!(prices.value(1), 20.0);
    }
}
