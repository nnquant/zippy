use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{
    Engine, EngineMetricsDelta, LateDataPolicy, Result, SchemaRef, SegmentTableView, ZippyError,
};
use zippy_operators::{CrossSectionalFactor, CrossSectionalFactorContext};

use crate::cross_sectional_bucket::CrossSectionalBucketState;
use crate::table_view::{string_array, timestamp_ns_array};

/// Evaluate cross-sectional factors on event-time aligned buckets.
pub struct CrossSectionalEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    trigger_interval: i64,
    late_data_policy: LateDataPolicy,
    factors: Vec<Box<dyn CrossSectionalFactor>>,
    current_bucket_start: Option<i64>,
    current_rows: CrossSectionalBucketState,
    last_closed_bucket_start: Option<i64>,
    pending_late_rows: u64,
}

impl CrossSectionalEngine {
    /// Create a new cross-sectional engine.
    pub fn new(
        name: impl Into<String>,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        trigger_interval: i64,
        late_data_policy: LateDataPolicy,
        mut factors: Vec<Box<dyn CrossSectionalFactor>>,
    ) -> Result<Self> {
        if trigger_interval <= 0 {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "trigger interval must be positive trigger_interval=[{}]",
                    trigger_interval
                ),
            });
        }

        if factors.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "cross-sectional engine requires at least one factor".to_string(),
            });
        }

        validate_id_column(input_schema.as_ref(), id_column)?;
        validate_dt_column(input_schema.as_ref(), dt_column)?;
        if late_data_policy != LateDataPolicy::Reject {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "cross-sectional engine only supports late_data_policy=[reject] actual=[{:?}]",
                    late_data_policy
                ),
            });
        }

        let output_schema = build_output_schema(&input_schema, id_column, dt_column, &factors)?;
        let current_rows = CrossSectionalBucketState::new(Arc::clone(&input_schema), id_column)?;
        let empty_batch = RecordBatch::new_empty(Arc::clone(&input_schema));
        for factor in &mut factors {
            factor.evaluate(&empty_batch)?;
        }

        Ok(Self {
            name: name.into(),
            input_schema,
            output_schema,
            id_column: id_column.to_string(),
            dt_column: dt_column.to_string(),
            trigger_interval,
            late_data_policy,
            factors,
            current_bucket_start: None,
            current_rows,
            last_closed_bucket_start: None,
            pending_late_rows: 0,
        })
    }

    fn finalize_bucket(
        &mut self,
        bucket_start: i64,
        rows: &CrossSectionalBucketState,
    ) -> Result<RecordBatch> {
        let bucket_batch = rows.materialize()?;
        let id_index = self.input_schema.index_of(&self.id_column).map_err(|_| {
            ZippyError::SchemaMismatch {
                reason: format!(
                    "missing cross-sectional id field field=[{}]",
                    self.id_column
                ),
            }
        })?;
        let dt_field = self
            .output_schema
            .field_with_name(&self.dt_column)
            .map_err(|_| ZippyError::SchemaMismatch {
                reason: format!(
                    "missing cross-sectional output dt field field=[{}]",
                    self.dt_column
                ),
            })?;
        let mut columns = vec![
            Arc::clone(bucket_batch.column(id_index)),
            bucket_start_array(bucket_start, rows.len(), dt_field.data_type())?,
        ];

        let mut context = CrossSectionalFactorContext::new(&bucket_batch);
        for factor in &mut self.factors {
            columns.push(factor.evaluate_with_context(&mut context)?);
        }

        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!(
                    "failed to build cross-sectional output batch error=[{}]",
                    error
                ),
            }
        })
    }

    fn handle_late_row(
        &self,
        dt: i64,
        current_bucket_start: Option<i64>,
        last_closed_bucket_start: Option<i64>,
        pending_late_rows: &mut u64,
    ) -> Result<()> {
        match self.late_data_policy {
            LateDataPolicy::Reject => Err(ZippyError::LateData {
                dt,
                last_dt: current_bucket_start
                    .or(last_closed_bucket_start)
                    .unwrap_or(dt),
            }),
            LateDataPolicy::DropWithMetric => {
                *pending_late_rows += 1;
                Ok(())
            }
        }
    }
}

impl Engine for CrossSectionalEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match engine input schema engine=[{}]",
                    self.name
                ),
            });
        }
        if table.num_rows() == 0 {
            return Ok(vec![]);
        }

        let id_array = table.column(&self.id_column)?;
        let _ids = checked_id_array(&id_array, &self.id_column)?;
        let dt_array = table.column(&self.dt_column)?;
        let dts = checked_dt_array(&dt_array, &self.dt_column)?;
        let mut outputs = Vec::new();
        let mut next_current_bucket_start = self.current_bucket_start;
        let mut next_current_rows = self.current_rows.clone();
        let mut next_last_closed_bucket_start = self.last_closed_bucket_start;
        let mut next_pending_late_rows = self.pending_late_rows;
        let row_order = build_row_order(dts, table.num_rows(), self.trigger_interval);

        for row_index in row_order.iter() {
            let dt = dts.value(row_index);
            let bucket_start = align_bucket_start(dt, self.trigger_interval);

            match next_current_bucket_start {
                Some(current_bucket_start) if bucket_start < current_bucket_start => {
                    self.handle_late_row(
                        dt,
                        next_current_bucket_start,
                        next_last_closed_bucket_start,
                        &mut next_pending_late_rows,
                    )?;
                }
                Some(current_bucket_start) if bucket_start == current_bucket_start => {
                    next_current_rows.apply_row(&table, row_index)?;
                }
                Some(current_bucket_start) => {
                    outputs.push(self.finalize_bucket(current_bucket_start, &next_current_rows)?);
                    next_last_closed_bucket_start = Some(current_bucket_start);
                    next_current_bucket_start = Some(bucket_start);
                    next_current_rows.clear();
                    next_current_rows.apply_row(&table, row_index)?;
                }
                None => {
                    if matches!(
                        next_last_closed_bucket_start,
                        Some(last_closed_bucket_start) if bucket_start <= last_closed_bucket_start
                    ) {
                        self.handle_late_row(
                            dt,
                            next_current_bucket_start,
                            next_last_closed_bucket_start,
                            &mut next_pending_late_rows,
                        )?;
                    } else {
                        next_current_bucket_start = Some(bucket_start);
                        next_current_rows.apply_row(&table, row_index)?;
                    }
                }
            }
        }

        self.current_bucket_start = next_current_bucket_start;
        self.current_rows = next_current_rows;
        self.last_closed_bucket_start = next_last_closed_bucket_start;
        self.pending_late_rows = next_pending_late_rows;

        Ok(outputs
            .into_iter()
            .map(SegmentTableView::from_record_batch)
            .collect())
    }

    fn on_flush(&mut self) -> Result<Vec<SegmentTableView>> {
        let Some(bucket_start) = self.current_bucket_start else {
            return Ok(vec![]);
        };

        if self.current_rows.is_empty() {
            self.current_bucket_start = None;
            return Ok(vec![]);
        }

        let current_rows = self.current_rows.clone();
        let output = self.finalize_bucket(bucket_start, &current_rows)?;
        self.last_closed_bucket_start = Some(bucket_start);
        self.current_bucket_start = None;
        self.current_rows.clear();

        Ok(vec![SegmentTableView::from_record_batch(output)])
    }

    fn drain_metrics(&mut self) -> EngineMetricsDelta {
        let delta = EngineMetricsDelta {
            late_rows_total: self.pending_late_rows,
            filtered_rows_total: 0,
        };
        self.pending_late_rows = 0;
        delta
    }
}

fn build_output_schema(
    input_schema: &SchemaRef,
    id_column: &str,
    dt_column: &str,
    factors: &[Box<dyn CrossSectionalFactor>],
) -> Result<SchemaRef> {
    let mut output_names = HashSet::from([id_column.to_string(), dt_column.to_string()]);
    let mut output_fields = vec![
        Arc::new(
            input_schema
                .field_with_name(id_column)
                .map_err(|_| ZippyError::SchemaMismatch {
                    reason: format!("missing utf8 id field field=[{}]", id_column),
                })?
                .clone(),
        ),
        Arc::new(
            input_schema
                .field_with_name(dt_column)
                .map_err(|_| ZippyError::SchemaMismatch {
                    reason: format!("missing utc nanosecond dt field field=[{}]", dt_column),
                })?
                .clone(),
        ),
    ];

    for factor in factors {
        let output_field = factor.output_field();
        if !output_names.insert(output_field.name().clone()) {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "duplicate cross-sectional output field field=[{}]",
                    output_field.name()
                ),
            });
        }
        output_fields.push(Arc::new(output_field));
    }

    Ok(Arc::new(Schema::new(output_fields)))
}

enum RowOrder {
    Natural { row_count: usize },
    Sorted(Vec<usize>),
}

impl RowOrder {
    fn iter(&self) -> RowOrderIter<'_> {
        match self {
            Self::Natural { row_count } => RowOrderIter::Natural(0..*row_count),
            Self::Sorted(row_indices) => RowOrderIter::Sorted(row_indices.iter()),
        }
    }
}

enum RowOrderIter<'a> {
    Natural(std::ops::Range<usize>),
    Sorted(std::slice::Iter<'a, usize>),
}

impl Iterator for RowOrderIter<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Natural(row_indices) => row_indices.next(),
            Self::Sorted(row_indices) => row_indices.next().copied(),
        }
    }
}

fn build_row_order(
    dts: &TimestampNanosecondArray,
    row_count: usize,
    trigger_interval: i64,
) -> RowOrder {
    let mut previous_bucket_start = None;

    for row_index in 0..row_count {
        let bucket_start = align_bucket_start(dts.value(row_index), trigger_interval);

        if matches!(previous_bucket_start, Some(previous) if bucket_start < previous) {
            let mut row_indices = (0..row_count).collect::<Vec<_>>();
            row_indices.sort_by_key(|row_index| {
                (
                    align_bucket_start(dts.value(*row_index), trigger_interval),
                    *row_index,
                )
            });
            return RowOrder::Sorted(row_indices);
        }

        previous_bucket_start = Some(bucket_start);
    }

    RowOrder::Natural { row_count }
}

fn validate_id_column(schema: &Schema, id_column: &str) -> Result<()> {
    let field = schema
        .field_with_name(id_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing utf8 id field field=[{}]", id_column),
        })?;

    if field.data_type() != &DataType::Utf8 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_column),
        });
    }

    Ok(())
}

fn validate_dt_column(schema: &Schema, dt_column: &str) -> Result<()> {
    let field = schema
        .field_with_name(dt_column)
        .map_err(|_| ZippyError::SchemaMismatch {
            reason: format!(
                "missing timezone-aware nanosecond dt field field=[{}]",
                dt_column
            ),
        })?;

    if !matches!(
        field.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
    ) {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "dt field must be timezone-aware nanosecond timestamp field=[{}]",
                dt_column
            ),
        });
    }

    Ok(())
}

fn checked_id_array<'a>(array: &'a ArrayRef, id_column: &str) -> Result<&'a StringArray> {
    let ids = string_array(array, id_column)?;

    if ids.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("id field contains nulls field=[{}]", id_column),
        });
    }

    Ok(ids)
}

fn checked_dt_array<'a>(
    array: &'a ArrayRef,
    dt_column: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let dts = timestamp_ns_array(array, dt_column)?;

    if dts.null_count() > 0 {
        return Err(ZippyError::SchemaMismatch {
            reason: format!("dt field contains nulls field=[{}]", dt_column),
        });
    }

    Ok(dts)
}

fn align_bucket_start(dt: i64, trigger_interval: i64) -> i64 {
    dt.div_euclid(trigger_interval) * trigger_interval
}

fn bucket_start_array(bucket_start: i64, len: usize, data_type: &DataType) -> Result<ArrayRef> {
    let DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) = data_type else {
        return Err(ZippyError::SchemaMismatch {
            reason: format!(
                "cross-sectional output dt field must be timezone-aware nanosecond timestamp type=[{:?}]",
                data_type
            ),
        });
    };

    Ok(Arc::new(
        TimestampNanosecondArray::from(vec![bucket_start; len]).with_timezone(timezone.clone()),
    ) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use zippy_core::{SegmentTableView, ZippyError};

    use super::{build_row_order, RowOrder};
    use crate::cross_sectional_bucket::CrossSectionalBucketState;

    fn bucket_schema(id_nullable: bool) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, id_nullable),
            Field::new(
                "dt",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
                false,
            ),
            Field::new("ret_1m", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
        ]))
    }

    fn bucket_table(
        schema: Arc<Schema>,
        symbols: Vec<Option<&str>>,
        dts: Vec<i64>,
        values: Vec<f64>,
        volumes: Vec<i64>,
    ) -> SegmentTableView {
        SegmentTableView::from_record_batch(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(symbols)) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(dts).with_timezone("Asia/Shanghai"))
                        as ArrayRef,
                    Arc::new(Float64Array::from(values)) as ArrayRef,
                    Arc::new(Int64Array::from(volumes)) as ArrayRef,
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn bucket_state_keeps_last_row_per_id_in_sorted_id_order() {
        let schema = bucket_schema(false);
        let table = bucket_table(
            Arc::clone(&schema),
            vec![Some("B"), Some("A"), Some("A")],
            vec![101, 102, 103],
            vec![2.0, 1.0, 3.0],
            vec![20, 10, 30],
        );
        let mut state = CrossSectionalBucketState::new(schema, "symbol").unwrap();

        state.apply_row(&table, 0).unwrap();
        state.apply_row(&table, 1).unwrap();
        state.apply_row(&table, 2).unwrap();

        let materialized = state.materialize().unwrap();
        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dts = materialized
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let values = materialized
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let volumes = materialized
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids.iter().collect::<Vec<_>>(), vec![Some("A"), Some("B")]);
        assert!(matches!(
            dts.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
                if timezone.as_ref() == "Asia/Shanghai"
        ));
        assert_eq!(
            (0..dts.len())
                .map(|index| dts.value(index))
                .collect::<Vec<_>>(),
            vec![103, 101]
        );
        assert_eq!(
            (0..values.len())
                .map(|index| values.value(index))
                .collect::<Vec<_>>(),
            vec![3.0, 2.0]
        );
        assert_eq!(
            (0..volumes.len())
                .map(|index| volumes.value(index))
                .collect::<Vec<_>>(),
            vec![30, 20]
        );
    }

    #[test]
    fn bucket_state_rejects_null_id_without_polluting_existing_rows() {
        let schema = bucket_schema(true);
        let valid_table = bucket_table(
            Arc::clone(&schema),
            vec![Some("A")],
            vec![101],
            vec![1.0],
            vec![10],
        );
        let invalid_table = bucket_table(
            Arc::clone(&schema),
            vec![None],
            vec![102],
            vec![2.0],
            vec![20],
        );
        let mut state = CrossSectionalBucketState::new(schema, "symbol").unwrap();

        state.apply_row(&valid_table, 0).unwrap();
        let error = state.apply_row(&invalid_table, 0).unwrap_err();

        assert!(matches!(error, ZippyError::SchemaMismatch { .. }));
        let materialized = state.materialize().unwrap();
        let ids = materialized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.iter().collect::<Vec<_>>(), vec![Some("A")]);
    }

    #[test]
    fn row_order_natural_iterates_without_materialized_indices() {
        let rows = RowOrder::Natural { row_count: 3 };

        assert!(matches!(rows, RowOrder::Natural { row_count: 3 }));
        assert_eq!(rows.iter().collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[test]
    fn row_order_sorted_iterates_materialized_indices() {
        let rows = RowOrder::Sorted(vec![2, 0, 1]);

        assert!(matches!(rows, RowOrder::Sorted(_)));
        assert_eq!(rows.iter().collect::<Vec<_>>(), vec![2, 0, 1]);
    }

    #[test]
    fn build_row_order_keeps_natural_order_for_non_decreasing_buckets() {
        let dts = TimestampNanosecondArray::from(vec![0, 5, 10, 15]).with_timezone("UTC");
        let rows = build_row_order(&dts, 4, 10);

        assert!(matches!(rows, RowOrder::Natural { row_count: 4 }));
        assert_eq!(rows.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    }

    #[test]
    fn build_row_order_sorts_only_when_bucket_order_regresses() {
        let dts = TimestampNanosecondArray::from(vec![10, 0, 20]).with_timezone("UTC");
        let rows = build_row_order(&dts, 3, 10);

        assert!(matches!(rows, RowOrder::Sorted(_)));
        assert_eq!(rows.iter().collect::<Vec<_>>(), vec![1, 0, 2]);
    }
}
