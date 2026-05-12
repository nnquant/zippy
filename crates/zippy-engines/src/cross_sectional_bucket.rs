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

enum BucketCellValue {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampNanosecond(i64),
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

    pub(crate) fn len(&self) -> usize {
        self.key_to_slot.len()
    }

    pub(crate) fn clear(&mut self) {
        self.key_to_slot.clear();
        self.slots.clear();
        for column in &mut self.columns {
            column.clear();
        }
    }

    pub(crate) fn apply_row(&mut self, table: &SegmentTableView, row_index: usize) -> Result<()> {
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

        let slot = match self.key_to_slot.get(&key).copied() {
            Some(slot) => slot,
            None => self.allocate_slot(),
        };
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
