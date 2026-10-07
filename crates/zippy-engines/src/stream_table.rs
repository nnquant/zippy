use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};

use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Fields, TimeUnit},
    record_batch::RecordBatch,
};
use zippy_core::{Engine, Result, SchemaRef, SegmentRowView, SegmentTableView, ZippyError};
use zippy_segment_store::{
    compile_schema, ActiveSegmentDescriptor, ColumnSpec, ColumnType, PartitionColumnWriter,
    PartitionHandle, PartitionRowWriter, PartitionWriterHandle, ReaderSession, RowSpanView,
    SegmentLease, SegmentStore, SegmentStoreConfig, ZippySegmentStoreError,
};

const STREAM_TABLE_PARTITION: &str = "all";
pub const DEFAULT_STREAM_TABLE_ROW_CAPACITY: usize = 65_536;
type SegmentIdentity = (u64, u64);

/// Materializes an input stream into an active segment-backed stream table.
pub struct StreamTableMaterializer {
    name: String,
    input_schema: SchemaRef,
    row_capacity: usize,
    frame_per_batch: bool,
    store: SegmentStore,
    partition: PartitionHandle,
    sealed_segment_descriptors: VecDeque<RetainedSegmentDescriptor>,
}

/// One server-local shared-memory segment retained by an append materializer.
#[derive(Debug, Clone)]
pub struct StreamTableLiveSegment {
    descriptor: ActiveSegmentDescriptor,
    row_count: usize,
    sealed: bool,
}

struct RetainedSegmentDescriptor {
    envelope: serde_json::Value,
    // Sealed prefixes are immutable; the cache is dropped with the retained descriptor.
    append_segment: OnceLock<StreamTableLiveSegment>,
}

impl From<serde_json::Value> for RetainedSegmentDescriptor {
    fn from(envelope: serde_json::Value) -> Self {
        Self {
            envelope,
            append_segment: OnceLock::new(),
        }
    }
}

impl StreamTableLiveSegment {
    /// Returns the attachable shared-memory descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ActiveSegmentDescriptor {
        &self.descriptor
    }

    /// Returns the committed prefix represented by this segment.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns whether rollover sealed the segment against further appends.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.sealed
    }
}

/// Keeps a stream table segment pinned for reader-safe retention tests.
pub struct StreamTableSegmentLease {
    _session: ReaderSession,
    _lease: SegmentLease,
}

impl StreamTableMaterializer {
    /// Create a new stream table materializer.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :returns: Initialized materializer.
    /// :rtype: Result<StreamTableMaterializer>
    pub fn new(name: impl Into<String>, input_schema: SchemaRef) -> Result<Self> {
        Self::new_with_row_capacity(name, input_schema, DEFAULT_STREAM_TABLE_ROW_CAPACITY)
    }

    /// Create a new stream table materializer with an explicit segment row capacity.
    ///
    /// :param name: Stream table name.
    /// :type name: impl Into<String>
    /// :param input_schema: Input schema consumed and materialized by the table.
    /// :type input_schema: SchemaRef
    /// :param row_capacity: Active segment row capacity before rollover.
    /// :type row_capacity: usize
    /// :returns: Initialized materializer.
    /// :rtype: Result<StreamTableMaterializer>
    pub fn new_with_row_capacity(
        name: impl Into<String>,
        input_schema: SchemaRef,
        row_capacity: usize,
    ) -> Result<Self> {
        Self::new_with_row_capacity_and_writer_epoch(name, input_schema, row_capacity, None)
    }

    /// Validates the exact storage layout without creating shared memory or a partition.
    ///
    /// :param input_schema: Arrow schema proposed for materialization.
    /// :returns: Success only when the normal materializer schema compiler accepts it.
    /// :raises ZippyError: If a type, column layout, or metadata contract is unsupported.
    pub fn validate_schema(input_schema: &SchemaRef) -> Result<()> {
        compile_arrow_schema(input_schema).map(|_| ())
    }

    pub fn new_with_row_capacity_and_writer_epoch(
        name: impl Into<String>,
        input_schema: SchemaRef,
        row_capacity: usize,
        writer_epoch: Option<u64>,
    ) -> Result<Self> {
        Self::new_with_row_capacity_and_origin(
            name,
            input_schema,
            row_capacity,
            writer_epoch.map(|epoch| (epoch, 0)),
        )
    }

    /// Create storage with an explicit writer epoch and initial segment generation.
    /// Native recovery seeds generations from its logical next offset to avoid lineage reuse.
    pub fn new_with_row_capacity_and_origin(
        name: impl Into<String>,
        input_schema: SchemaRef,
        row_capacity: usize,
        origin: Option<(u64, u64)>,
    ) -> Result<Self> {
        if row_capacity == 0 {
            return Err(ZippyError::InvalidConfig {
                reason: "stream table row_capacity must be greater than zero".to_string(),
            });
        }
        let name = name.into();
        let compiled_schema = compile_arrow_schema(&input_schema)?;
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: row_capacity,
        })
        .map_err(segment_error)?;
        let partition = match origin {
            Some((writer_epoch, initial_generation)) => store
                .open_partition_with_schema_and_origin(
                    &name,
                    STREAM_TABLE_PARTITION,
                    compiled_schema,
                    writer_epoch,
                    initial_generation,
                ),
            None => {
                store.open_partition_with_schema(&name, STREAM_TABLE_PARTITION, compiled_schema)
            }
        }
        .map_err(segment_error)?;

        Ok(Self {
            name,
            input_schema,
            row_capacity,
            frame_per_batch: false,
            store,
            partition,
            sealed_segment_descriptors: VecDeque::new(),
        })
    }

    /// Commit each input batch atomically into its own sealed segment.
    ///
    /// This mode preserves upstream batch boundaries for complete-frame consumers. A frame that
    /// does not fit into one segment is rejected instead of being split across segments.
    pub fn with_frame_per_batch(mut self, enabled: bool) -> Self {
        self.frame_per_batch = enabled;
        self
    }

    /// Return the active segment committed row count.
    pub fn active_committed_row_count(&self) -> usize {
        self.partition.active_committed_row_count()
    }

    /// Return a row span covering the currently committed active segment rows.
    pub fn active_row_span(&self) -> Result<RowSpanView> {
        let committed = self.partition.active_committed_row_count();
        self.partition
            .active_row_span(0, committed)
            .map_err(segment_error)
    }

    /// Export the active segment descriptor envelope for cross-process readers.
    pub fn active_descriptor_envelope_bytes(&self) -> Result<Vec<u8>> {
        let mut descriptor = self.active_descriptor_value()?;
        descriptor["sealed_segments"] = serde_json::Value::Array(
            self.sealed_segment_descriptors
                .iter()
                .map(|retained| retained.envelope.clone())
                .collect(),
        );
        serde_json::to_vec(&descriptor).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })
    }

    /// Return a debug Arrow snapshot of the active segment.
    pub fn active_record_batch(&self) -> Result<RecordBatch> {
        self.partition
            .debug_snapshot_record_batch()
            .map_err(segment_error)
    }

    /// Returns retained append or frame segments in logical row order.
    ///
    /// Each immutable sealed descriptor caches its actual committed row count, so partially
    /// filled frame segments retain their complete business-batch boundaries.
    pub fn live_segments(&self) -> Result<Vec<StreamTableLiveSegment>> {
        let active = self.partition.active_descriptor();
        let schema = active.schema().clone();
        let layout = active.layout().clone();
        let active_rows = self.active_committed_row_count();
        let mut segments = Vec::with_capacity(
            self.sealed_segment_descriptors.len() + usize::from(active_rows != 0),
        );
        for retained in &self.sealed_segment_descriptors {
            if let Some(segment) = retained.append_segment.get() {
                segments.push(segment.clone());
                continue;
            }
            let bytes = serde_json::to_vec(&retained.envelope).map_err(|error| ZippyError::Io {
                reason: format!("failed to encode retained append descriptor error=[{error}]"),
            })?;
            let descriptor = ActiveSegmentDescriptor::from_envelope_bytes(
                &bytes,
                schema.clone(),
                layout.clone(),
            )
            .map_err(|error| ZippyError::Io {
                reason: format!("failed to decode retained append descriptor error=[{error}]"),
            })?;
            let row_count = RowSpanView::from_active_descriptor(descriptor.clone(), 0, 0)
                .map_err(|error| ZippyError::Io {
                    reason: format!("failed to attach retained append descriptor error=[{error}]"),
                })?
                .active_segment_committed_row_count()
                .map_err(segment_error)?
                .ok_or(ZippyError::InvalidState {
                    status: "retained append descriptor is not shared-memory backed",
                })?;
            let segment = StreamTableLiveSegment {
                row_count,
                descriptor,
                sealed: true,
            };
            let _ = retained.append_segment.set(segment.clone());
            segments.push(segment);
        }
        if active_rows != 0 {
            segments.push(StreamTableLiveSegment {
                descriptor: active,
                row_count: active_rows,
                sealed: false,
            });
        }
        Ok(segments)
    }

    /// Counts retained segments without decoding or attaching their descriptors.
    ///
    /// :returns: Nonempty active segment plus retained sealed segments.
    pub fn live_segment_count(&self) -> Result<usize> {
        Ok(self.sealed_segment_descriptors.len()
            + usize::from(self.active_committed_row_count() != 0))
    }

    /// Retires a durable prefix without splitting segments or invalidating pinned readers.
    ///
    /// :param max_rows: Prefix rows already covered by the authoritative manifest.
    /// :returns: Rows actually retired; pinned segments can stop reclamation early.
    /// :raises ZippyError: Descriptor validation failure.
    pub fn retire_persisted_prefix(&mut self, max_rows: usize) -> Result<usize> {
        let segments = self.live_segments()?;
        let mut rows = 0_usize;
        let mut count = 0;
        for segment in &segments {
            if !segment.sealed || segment.row_count > max_rows - rows {
                break;
            }
            rows += segment.row_count;
            count += 1;
        }
        let before = self.sealed_segment_descriptors.len();
        self.trim_durable_retained_segments(before - count);
        let removed = before - self.sealed_segment_descriptors.len();
        Ok(segments[..removed]
            .iter()
            .map(|segment| segment.row_count)
            .sum())
    }

    /// Seals a non-empty prefix before ownership moves to another writer epoch.
    ///
    /// The newly allocated empty active segment is excluded from live segments. A later writer
    /// epoch owns its own materializer, while this instance retains only the sealed history.
    pub fn seal_active_for_writer_handoff(&mut self) -> Result<()> {
        if self.active_committed_row_count() == 0 {
            return Ok(());
        }
        self.rollover_active_segment()
    }

    /// Keep the owned store observable for future lifecycle integration.
    pub fn segment_store(&self) -> &SegmentStore {
        &self.store
    }

    /// Pin the current active segment until the returned lease is dropped.
    pub fn pin_active_segment_for_test(&self) -> Result<StreamTableSegmentLease> {
        let session = self
            .store
            .open_session("stream-table-test-reader")
            .map_err(segment_error)?;
        let lease = session
            .attach_active(&self.partition)
            .map_err(segment_error)?;
        Ok(StreamTableSegmentLease {
            _session: session,
            _lease: lease,
        })
    }

    fn materialize_table(&mut self, table: &SegmentTableView) -> Result<()> {
        if table.num_rows() == 0 {
            return Ok(());
        }
        if self.frame_per_batch {
            return self.materialize_frame(table);
        }

        self.materialize_table_rows(table)
    }

    fn materialize_frame(&mut self, table: &SegmentTableView) -> Result<()> {
        if self.active_committed_row_count() != 0 {
            return Err(ZippyError::InvalidState {
                status: "frame-per-batch active segment must be empty",
            });
        }
        if table.num_rows() > self.row_capacity {
            return Err(ZippyError::InvalidConfig {
                reason: format!(
                    "stream table frame rows exceed segment capacity rows=[{}] row_capacity=[{}]",
                    table.num_rows(),
                    self.row_capacity,
                ),
            });
        }

        let fields = self.input_schema.fields().clone();
        let columns = (0..fields.len())
            .map(|index| table.column_at(index))
            .collect::<Result<Vec<_>>>()?;
        let writer = self.partition.writer();
        let write_result = if columns.iter().all(|column| column.null_count() == 0) {
            writer.write_atomic_columnar_rows(table.num_rows(), |column_writer, rows_to_write| {
                Self::write_materialized_columnar_values(
                    column_writer,
                    &fields,
                    &columns,
                    0,
                    rows_to_write,
                )
            })
        } else {
            writer.rewrite_rows(table.num_rows(), |row, row_index| {
                Self::write_materialized_row_fields(row, &fields, &columns, row_index)
            })
        };
        match write_result {
            Ok(written) if written == table.num_rows() => {}
            Ok(written) => {
                return Err(ZippyError::Io {
                    reason: format!(
                        "stream table frame rewrite was incomplete rows=[{}] written=[{}]",
                        table.num_rows(),
                        written,
                    ),
                });
            }
            Err(ZippySegmentStoreError::Writer("segment is full")) => {
                return Err(ZippyError::InvalidConfig {
                    reason: format!(
                        "stream table frame payload exceeds segment capacity rows=[{}] row_capacity=[{}]",
                        table.num_rows(),
                        self.row_capacity,
                    ),
                });
            }
            Err(error) => return Err(segment_error(error)),
        }

        self.rollover_active_segment()
    }

    fn materialize_table_rows(&mut self, table: &SegmentTableView) -> Result<()> {
        if let Some(row_view) = table.as_segment_row_view() {
            return self.materialize_segment_rows(row_view);
        }

        let fields = self.input_schema.fields().clone();
        let columns = (0..fields.len())
            .map(|index| table.column_at(index))
            .collect::<Result<Vec<_>>>()?;
        let writer = self.partition.writer();
        if columns.iter().all(|column| column.null_count() == 0) {
            return self.materialize_columnar_table_rows(
                &writer,
                &fields,
                &columns,
                table.num_rows(),
            );
        }

        for row_index in 0..table.num_rows() {
            match self.write_materialized_row(&writer, &fields, &columns, row_index) {
                Ok(()) => {}
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    self.rollover_active_segment()?;

                    self.write_materialized_row(&writer, &fields, &columns, row_index)
                        .map_err(segment_error)?;
                }
                Err(error) => return Err(segment_error(error)),
            }
        }

        Ok(())
    }

    fn materialize_columnar_table_rows(
        &mut self,
        writer: &PartitionWriterHandle,
        fields: &Fields,
        columns: &[ArrayRef],
        row_count: usize,
    ) -> Result<()> {
        let mut copied = 0;
        while copied < row_count {
            match self.write_materialized_columnar_rows(
                writer,
                fields,
                columns,
                copied,
                row_count - copied,
            ) {
                Ok(rows) if rows > 0 => {
                    copied += rows;
                }
                Ok(_) => {
                    return Err(ZippyError::Io {
                        reason: "stream table columnar append made no progress".to_string(),
                    });
                }
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    self.rollover_active_segment()?;
                }
                Err(error) => return Err(segment_error(error)),
            }
        }

        Ok(())
    }

    fn materialize_segment_rows(&mut self, row_view: &SegmentRowView) -> Result<()> {
        let writer = self.partition.writer();
        let total_rows = row_view.num_rows();
        let mut copied = 0;
        while copied < total_rows {
            match writer.append_row_span(row_view.row_span(), copied, total_rows - copied) {
                Ok(rows) if rows > 0 => copied += rows,
                Ok(_) => {
                    return Err(ZippyError::Io {
                        reason: "stream table segment append made no progress".to_string(),
                    })
                }
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    self.rollover_active_segment()?;
                }
                Err(error) => return Err(segment_error(error)),
            }
        }
        Ok(())
    }

    fn write_materialized_row(
        &self,
        writer: &PartitionWriterHandle,
        fields: &Fields,
        columns: &[ArrayRef],
        row_index: usize,
    ) -> std::result::Result<(), ZippySegmentStoreError> {
        writer.write_row(|row| Self::write_materialized_row_fields(row, fields, columns, row_index))
    }

    fn write_materialized_row_fields(
        row: &mut PartitionRowWriter<'_>,
        fields: &Fields,
        columns: &[ArrayRef],
        row_index: usize,
    ) -> std::result::Result<(), ZippySegmentStoreError> {
        for (field_index, field) in fields.iter().enumerate() {
            let array = columns[field_index].as_ref();
            if array.is_null(row_index) {
                if field.is_nullable() {
                    continue;
                }
                return Err(ZippySegmentStoreError::Schema(
                    "non-nullable stream table column received null",
                ));
            }

            match field.data_type() {
                DataType::Int64 => {
                    let values = array.as_any().downcast_ref::<Int64Array>().ok_or(
                        ZippySegmentStoreError::Schema("int64 column downcast failed"),
                    )?;
                    row.write_i64(field.name(), values.value(row_index))?;
                }
                DataType::Float64 => {
                    let values = array.as_any().downcast_ref::<Float64Array>().ok_or(
                        ZippySegmentStoreError::Schema("float64 column downcast failed"),
                    )?;
                    row.write_f64(field.name(), values.value(row_index))?;
                }
                DataType::Utf8 => {
                    let values = array.as_any().downcast_ref::<StringArray>().ok_or(
                        ZippySegmentStoreError::Schema("utf8 column downcast failed"),
                    )?;
                    row.write_utf8(field.name(), values.value(row_index))?;
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let values = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or(ZippySegmentStoreError::Schema(
                            "timestamp[ns] column downcast failed",
                        ))?;
                    row.write_i64(field.name(), values.value(row_index))?;
                }
                _ => {
                    return Err(ZippySegmentStoreError::Schema(
                        "unsupported stream table column type",
                    ));
                }
            }
        }
        Ok(())
    }

    fn write_materialized_columnar_rows(
        &self,
        writer: &PartitionWriterHandle,
        fields: &Fields,
        columns: &[ArrayRef],
        start_row: usize,
        row_count: usize,
    ) -> std::result::Result<usize, ZippySegmentStoreError> {
        writer.write_columnar_rows(row_count, |column_writer, rows_to_write| {
            Self::write_materialized_columnar_values(
                column_writer,
                fields,
                columns,
                start_row,
                rows_to_write,
            )
        })
    }

    fn write_materialized_columnar_values(
        column_writer: &mut PartitionColumnWriter<'_>,
        fields: &Fields,
        columns: &[ArrayRef],
        start_row: usize,
        rows_to_write: usize,
    ) -> std::result::Result<(), ZippySegmentStoreError> {
        for (field_index, field) in fields.iter().enumerate() {
            let array = columns[field_index].as_ref();
            match field.data_type() {
                DataType::Int64 => {
                    let values = array.as_any().downcast_ref::<Int64Array>().ok_or(
                        ZippySegmentStoreError::Schema("int64 column downcast failed"),
                    )?;
                    let batch_values = (0..rows_to_write)
                        .map(|offset| values.value(start_row + offset))
                        .collect::<Vec<_>>();
                    column_writer.write_i64_values(field.name(), &batch_values)?;
                }
                DataType::Float64 => {
                    let values = array.as_any().downcast_ref::<Float64Array>().ok_or(
                        ZippySegmentStoreError::Schema("float64 column downcast failed"),
                    )?;
                    let batch_values = (0..rows_to_write)
                        .map(|offset| values.value(start_row + offset))
                        .collect::<Vec<_>>();
                    column_writer.write_f64_values(field.name(), &batch_values)?;
                }
                DataType::Utf8 => {
                    let values = array.as_any().downcast_ref::<StringArray>().ok_or(
                        ZippySegmentStoreError::Schema("utf8 column downcast failed"),
                    )?;
                    let batch_values = (0..rows_to_write)
                        .map(|offset| values.value(start_row + offset))
                        .collect::<Vec<_>>();
                    column_writer.write_utf8_values(field.name(), &batch_values)?;
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let values = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or(ZippySegmentStoreError::Schema(
                            "timestamp[ns] column downcast failed",
                        ))?;
                    let batch_values = (0..rows_to_write)
                        .map(|offset| values.value(start_row + offset))
                        .collect::<Vec<_>>();
                    column_writer.write_i64_values(field.name(), &batch_values)?;
                }
                _ => {
                    return Err(ZippySegmentStoreError::Schema(
                        "unsupported stream table column type",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Seals the active segment into retained history and opens a fresh active segment.
    fn rollover_active_segment(&mut self) -> Result<()> {
        let sealed_descriptor = self.active_descriptor_value()?;
        self.partition.writer().rollover().map_err(segment_error)?;
        self.sealed_segment_descriptors
            .push_back(sealed_descriptor.into());
        Ok(())
    }

    fn active_descriptor_value(&self) -> Result<serde_json::Value> {
        let envelope = self
            .partition
            .active_descriptor_envelope_bytes()
            .map_err(segment_error)?;
        serde_json::from_slice(&envelope).map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })
    }

    fn trim_durable_retained_segments(&mut self, retention_segments: usize) {
        while self.sealed_segment_descriptors.len() > retention_segments {
            let Some(identity) = descriptor_segment_identity(
                &self.sealed_segment_descriptors.front().unwrap().envelope,
            ) else {
                break;
            };
            if self
                .store
                .pin_count_for_segment(&self.partition, identity.0, identity.1)
                > 0
            {
                break;
            }
            self.sealed_segment_descriptors.pop_front();
            self.partition
                .release_retired_segment(identity.0, identity.1);
        }
    }
}

fn descriptor_segment_identity(descriptor: &serde_json::Value) -> Option<SegmentIdentity> {
    let segment_id = descriptor.get("segment_id")?.as_u64()?;
    let generation = descriptor.get("generation")?.as_u64()?;
    Some((segment_id, generation))
}

impl Engine for StreamTableMaterializer {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.input_schema)
    }

    fn on_data(&mut self, table: SegmentTableView) -> Result<Vec<SegmentTableView>> {
        if table.schema().as_ref() != self.input_schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "input batch schema does not match stream table input schema engine=[{}]",
                    self.name
                ),
            });
        }
        self.materialize_table(&table)?;
        Ok(vec![table])
    }
}

fn compile_arrow_schema(schema: &SchemaRef) -> Result<zippy_segment_store::CompiledSchema> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = match field.data_type() {
                DataType::Int64 => ColumnType::Int64,
                DataType::Float64 => ColumnType::Float64,
                DataType::Utf8 => ColumnType::Utf8,
                DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
                    let timezone = timezone
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "UTC".to_string());
                    ColumnType::timestamp_ns_tz(timezone)
                }
                _ => {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!(
                            "unsupported stream table column type column=[{}] data_type=[{:?}]",
                            field.name(),
                            field.data_type()
                        ),
                    });
                }
            };

            Ok(if field.is_nullable() {
                ColumnSpec::nullable(field.name().as_str(), data_type)
            } else {
                ColumnSpec::new(field.name().as_str(), data_type)
            })
        })
        .collect::<Result<Vec<_>>>()?;

    compile_schema(&columns)
        .and_then(|compiled| compiled.with_arrow_metadata(schema))
        .map_err(|reason| ZippyError::SchemaMismatch {
            reason: format!("failed to compile stream table schema reason=[{}]", reason),
        })
}

fn segment_error(error: ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod native_materializer_tests {
    use super::{descriptor_segment_identity, StreamTableLiveSegment, StreamTableMaterializer};
    use super::{Arc, ArrayRef, DataType, Engine, Int64Array, RecordBatch, SegmentTableView};
    use arrow::datatypes::{Field, Schema};
    use std::collections::BTreeSet;
    use std::time::Instant;

    #[test]
    #[ignore = "manual release benchmark for retained append descriptor publication"]
    fn append_descriptor_performance() {
        for sealed in [64, 512] {
            let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from_iter_values(0..sealed * 2 + 1)) as ArrayRef],
            )
            .unwrap();
            let mut materializer =
                StreamTableMaterializer::new_with_row_capacity("bench_append", schema, 2).unwrap();
            materializer
                .on_data(SegmentTableView::from_record_batch(batch))
                .unwrap();
            materializer.live_segments().unwrap();
            let iterations = 500;
            let start = Instant::now();
            for _ in 0..iterations {
                std::hint::black_box(materializer.live_segments().unwrap());
            }
            eprintln!(
                "sealed=[{sealed}] publication_us=[{:.2}]",
                start.elapsed().as_micros() as f64 / iterations as f64
            );
        }
    }

    #[test]
    fn native_manifest_prefix_retires_only_complete_durable_segments() {
        let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..17)) as ArrayRef],
        )
        .unwrap();
        let mut materializer =
            StreamTableMaterializer::new_with_row_capacity("retention", schema, 2).unwrap();
        materializer
            .on_data(SegmentTableView::from_record_batch(batch))
            .unwrap();
        let identities = materializer
            .sealed_segment_descriptors
            .iter()
            .map(|descriptor| descriptor_segment_identity(&descriptor.envelope).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), 8);
        assert_eq!(materializer.live_segment_count().unwrap(), 9);
        for _ in 0..2 {
            let segments = materializer.live_segments().unwrap();
            assert_eq!(
                segments
                    .iter()
                    .map(StreamTableLiveSegment::row_count)
                    .sum::<usize>(),
                17
            );
            assert!(segments[..8].iter().all(StreamTableLiveSegment::is_sealed));
        }

        // Only the native manifest authority supplies the durable prefix. A partial
        // next segment cannot be reclaimed, even if its rows are already visible.
        assert_eq!(materializer.retire_persisted_prefix(15).unwrap(), 14);
        assert_eq!(materializer.retire_persisted_prefix(1).unwrap(), 0);
        assert_eq!(materializer.live_segment_count().unwrap(), 2);
        assert_eq!(materializer.sealed_segment_descriptors.len(), 1);
        let segments = materializer.live_segments().unwrap();
        assert_eq!(
            segments
                .iter()
                .map(StreamTableLiveSegment::row_count)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        materializer.seal_active_for_writer_handoff().unwrap();
        assert_eq!(materializer.live_segment_count().unwrap(), 2);
        assert!(materializer
            .live_segments()
            .unwrap()
            .iter()
            .all(StreamTableLiveSegment::is_sealed));
    }
}
