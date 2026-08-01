//! Native append materialization and serialized TableActor publication.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::{StreamTableLiveSegment, StreamTableMaterializer};
use zippy_types::table::{SourceSegmentIdentity, TableDefinition};
use zippy_types::{
    BoxFuture, ErrorCode, LogOffset, OffsetRange, OffsetWatermark, Result, ServerError, TableId,
};

use crate::{
    NativeIngestApplyContext, NativeIngestSink, NativeIngestSinkError, NativeIngestTableBootstrap,
    TableActorHandle, TableActorLiveSegmentPublication, TableActorLiveSegmentRole,
    TableActorLiveSegmentsPublication, TableActorRegistryHandle,
};

/// Default row capacity of one native append shared-memory segment.
pub const DEFAULT_NATIVE_APPEND_ROW_CAPACITY: usize =
    zippy_engines::DEFAULT_STREAM_TABLE_ROW_CAPACITY;
/// Default maximum live shared-memory segments published for one append table.
pub const DEFAULT_NATIVE_APPEND_MAX_LIVE_SEGMENTS: usize = 1_024;
/// Hard ceiling for native append live-segment publication.
pub const MAX_NATIVE_APPEND_LIVE_SEGMENTS: usize = 1_024;

/// Bounded shared-memory policy for the native append sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAppendMaterializerConfig {
    row_capacity: usize,
    max_live_segments: usize,
}

impl Default for NativeAppendMaterializerConfig {
    fn default() -> Self {
        Self {
            row_capacity: DEFAULT_NATIVE_APPEND_ROW_CAPACITY,
            max_live_segments: DEFAULT_NATIVE_APPEND_MAX_LIVE_SEGMENTS,
        }
    }
}

impl NativeAppendMaterializerConfig {
    /// Creates the default bounded append materializer policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum rows allocated in one shared-memory segment.
    #[must_use]
    pub const fn with_row_capacity(mut self, row_capacity: usize) -> Self {
        self.row_capacity = row_capacity;
        self
    }

    /// Sets the maximum retained live segments published to one TableActor.
    #[must_use]
    pub const fn with_max_live_segments(mut self, maximum: usize) -> Self {
        self.max_live_segments = maximum;
        self
    }

    /// Validates static allocation bounds.
    pub fn validate(self) -> Result<()> {
        if self.row_capacity == 0 {
            return Err(invalid("native append row capacity must be positive"));
        }
        if self.max_live_segments == 0 || self.max_live_segments > MAX_NATIVE_APPEND_LIVE_SEGMENTS {
            return Err(invalid(format!(
                concat!("native append live segment limit must be between 1 and {} actual=[{}]"),
                MAX_NATIVE_APPEND_LIVE_SEGMENTS, self.max_live_segments
            )));
        }
        Ok(())
    }
}

/// Server-owned append materializer that publishes fixed logical prefixes through TableActor.
pub struct NativeAppendMaterializerSink {
    config: NativeAppendMaterializerConfig,
    tables: BTreeMap<TableId, NativeAppendTable>,
}

impl NativeAppendMaterializerSink {
    /// Builds one append sink from the same revision-consistent bootstraps as native ingest.
    pub fn start(
        config: NativeAppendMaterializerConfig,
        bootstraps: &[NativeIngestTableBootstrap],
        actors: &TableActorRegistryHandle,
    ) -> Result<Self> {
        config.validate()?;
        let mut tables = BTreeMap::new();
        for bootstrap in bootstraps {
            if !matches!(bootstrap.table_definition(), TableDefinition::Append(_)) {
                continue;
            }
            let table_id = bootstrap.table_id();
            let table = NativeAppendTable {
                table_id,
                schema_version: bootstrap.schema_version(),
                schema: Arc::clone(bootstrap.schema()),
                actor: actors.table(table_id)?,
                state: Mutex::new(NativeAppendTableState {
                    actor_writer_epoch: bootstrap.writer_epoch(),
                    persisted_log_offset: bootstrap.persisted_log_offset(),
                    last_applied_log_offset: bootstrap.persisted_log_offset(),
                    live_rows: 0,
                    epochs: Vec::new(),
                }),
            };
            if tables.insert(table_id, table).is_some() {
                return Err(ServerError::new(
                    ErrorCode::DataCorruption,
                    "native append materializer contains a duplicate table",
                )
                .with_table_id(table_id)
                .with_retryable(false));
            }
        }
        Ok(Self { config, tables })
    }

    /// Returns the number of append tables owned by this sink.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    fn table(&self, table_id: TableId) -> Result<&NativeAppendTable> {
        self.tables.get(&table_id).ok_or_else(|| {
            ServerError::new(
                ErrorCode::UnsupportedPlan,
                "native materializer currently supports append tables only",
            )
            .with_table_id(table_id)
            .with_retryable(false)
        })
    }
}

struct NativeAppendTable {
    table_id: TableId,
    schema_version: u64,
    schema: arrow::datatypes::SchemaRef,
    actor: TableActorHandle,
    state: Mutex<NativeAppendTableState>,
}

struct NativeAppendTableState {
    actor_writer_epoch: u64,
    persisted_log_offset: OffsetWatermark,
    last_applied_log_offset: OffsetWatermark,
    live_rows: u64,
    epochs: Vec<EpochMaterializer>,
}

struct EpochMaterializer {
    writer_epoch: u64,
    materializer: StreamTableMaterializer,
}

impl NativeIngestSink for NativeAppendMaterializerSink {
    fn fence_writer(&self, table_id: TableId, writer_epoch: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let table = self.table(table_id)?;
            let publication = {
                let mut state = table.state.lock().unwrap();
                if writer_epoch < state.actor_writer_epoch {
                    return Err(ServerError::new(
                        ErrorCode::StaleProducerEpoch,
                        format!(
                            concat!(
                                "native append writer epoch regressed expected=[{}] actual=[{}]"
                            ),
                            state.actor_writer_epoch, writer_epoch
                        ),
                    )
                    .with_table_id(table_id)
                    .with_retryable(false));
                }
                if writer_epoch == state.actor_writer_epoch {
                    None
                } else if let Some(current) = state.epochs.last_mut() {
                    current
                        .materializer
                        .seal_active_for_writer_handoff()
                        .map_err(|error| materializer_error("seal append writer handoff", error))?;
                    Some(build_publication(table, &state, state.live_rows)?)
                } else {
                    None
                }
            };

            if let Some(publication) = publication {
                table.actor.publish_live_segments(publication).await?;
            }
            table.actor.update_writer_epoch(writer_epoch).await?;
            table.state.lock().unwrap().actor_writer_epoch = writer_epoch;
            Ok(())
        })
    }

    fn apply(
        &self,
        context: NativeIngestApplyContext,
        mutation: &zippy_types::ingest::TableMutation,
    ) -> std::result::Result<(), NativeIngestSinkError> {
        let table = self
            .table(context.identity.table_id)
            .map_err(NativeIngestSinkError::Rejected)?;
        let zippy_types::ingest::TableMutation::Append(batch) = mutation else {
            return Err(NativeIngestSinkError::Rejected(
                ServerError::new(
                    ErrorCode::UnsupportedPlan,
                    "native append materializer received a non-append mutation",
                )
                .with_table_id(table.table_id)
                .with_retryable(false),
            ));
        };
        if context.schema_version != table.schema_version
            || batch.schema().as_ref() != table.schema.as_ref()
        {
            return Err(NativeIngestSinkError::Rejected(
                ServerError::new(
                    ErrorCode::SchemaMismatch,
                    "native append materializer schema differs from its startup schema",
                )
                .with_table_id(table.table_id)
                .with_retryable(false),
            ));
        }

        let mut state = table.state.lock().unwrap();
        validate_receipt_prefix(table.table_id, &state, &context)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if context.identity.producer_epoch > state.actor_writer_epoch {
            return Err(NativeIngestSinkError::Rejected(
                ServerError::new(
                    ErrorCode::StaleProducerEpoch,
                    "native append mutation epoch is ahead of the TableActor fencing epoch",
                )
                .with_table_id(table.table_id)
                .with_retryable(false),
            ));
        }
        preflight_live_segment_budget(self.config, &state, context.identity.producer_epoch, batch)
            .map_err(NativeIngestSinkError::Rejected)?;
        ensure_epoch_materializer(
            self.config,
            table,
            &mut state,
            context.identity.producer_epoch,
        )?;

        let current = state
            .epochs
            .last_mut()
            .expect("ensuring an epoch materializer creates one");
        current
            .materializer
            .on_data(SegmentTableView::from_record_batch(batch.clone()))
            .map_err(|error| {
                NativeIngestSinkError::Indeterminate(materializer_error(
                    "append native mutation",
                    error,
                ))
            })?;
        current.materializer.on_flush().map_err(|error| {
            NativeIngestSinkError::Indeterminate(materializer_error(
                "flush native append mutation",
                error,
            ))
        })?;

        let new_live_rows = state
            .live_rows
            .checked_add(context.receipt.row_count())
            .ok_or_else(|| {
                NativeIngestSinkError::Indeterminate(resource_exhausted(
                    table.table_id,
                    "native append live row counter overflow",
                ))
            })?;
        let publication = build_publication(table, &state, new_live_rows)
            .map_err(NativeIngestSinkError::Indeterminate)?;
        table
            .actor
            .try_publish_live_segments(publication)
            .map_err(NativeIngestSinkError::Indeterminate)?
            .wait_blocking()
            .map_err(NativeIngestSinkError::Indeterminate)?;
        state.live_rows = new_live_rows;
        state.last_applied_log_offset = context.receipt.committed_log_offset();
        Ok(())
    }
}

fn validate_receipt_prefix(
    table_id: TableId,
    state: &NativeAppendTableState,
    context: &NativeIngestApplyContext,
) -> std::result::Result<(), NativeIngestSinkError> {
    if context.receipt.previous_log_offset() != state.last_applied_log_offset {
        return Err(NativeIngestSinkError::Rejected(
            ServerError::new(
                ErrorCode::SequenceGap,
                "native append receipt does not continue the materialized logical prefix",
            )
            .with_table_id(table_id)
            .with_retryable(false),
        ));
    }
    Ok(())
}

fn ensure_epoch_materializer(
    config: NativeAppendMaterializerConfig,
    table: &NativeAppendTable,
    state: &mut NativeAppendTableState,
    writer_epoch: u64,
) -> std::result::Result<(), NativeIngestSinkError> {
    if let Some(current) = state.epochs.last_mut() {
        if current.writer_epoch > writer_epoch {
            return Err(NativeIngestSinkError::Rejected(
                ServerError::new(
                    ErrorCode::DataCorruption,
                    "native append mutation writer epochs regressed",
                )
                .with_table_id(table.table_id)
                .with_retryable(false),
            ));
        }
        if current.writer_epoch == writer_epoch {
            return Ok(());
        }
    }

    let materializer = StreamTableMaterializer::new_with_row_capacity_and_writer_epoch(
        format!("native-{}-{writer_epoch}", table.table_id),
        Arc::clone(&table.schema),
        config.row_capacity,
        Some(writer_epoch),
    )
    .map_err(|error| {
        NativeIngestSinkError::Rejected(materializer_error(
            "create native append materializer",
            error,
        ))
    })?;
    if let Some(current) = state.epochs.last_mut() {
        current
            .materializer
            .seal_active_for_writer_handoff()
            .map_err(|error| {
                NativeIngestSinkError::Indeterminate(materializer_error(
                    "seal recovered append writer handoff",
                    error,
                ))
            })?;
    }
    state.epochs.push(EpochMaterializer {
        writer_epoch,
        materializer,
    });
    Ok(())
}

fn preflight_live_segment_budget(
    config: NativeAppendMaterializerConfig,
    state: &NativeAppendTableState,
    writer_epoch: u64,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<()> {
    let rows = batch.num_rows();
    let mut projected = 0_usize;
    for epoch in &state.epochs {
        let segments = epoch
            .materializer
            .append_live_segments()
            .map_err(|error| materializer_error("inspect native append segments", error))?;
        if epoch.writer_epoch == writer_epoch {
            let active_rows = epoch.materializer.active_committed_row_count();
            let sealed = segments
                .iter()
                .filter(|segment| segment.is_sealed())
                .count();
            projected = projected
                .checked_add(sealed)
                .and_then(|value| {
                    value.checked_add(ceil_segments(
                        active_rows.checked_add(rows)?,
                        config.row_capacity,
                    ))
                })
                .ok_or_else(|| resource_exhausted_no_table("live segment count overflow"))?;
        } else {
            projected = projected
                .checked_add(segments.len())
                .ok_or_else(|| resource_exhausted_no_table("live segment count overflow"))?;
        }
    }
    if state
        .epochs
        .last()
        .is_none_or(|epoch| epoch.writer_epoch != writer_epoch)
    {
        projected = projected
            .checked_add(ceil_segments(rows, config.row_capacity))
            .ok_or_else(|| resource_exhausted_no_table("live segment count overflow"))?;
    }
    if projected > config.max_live_segments {
        return Err(ServerError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "native append live segment limit exhausted projected=[{projected}] maximum=[{}]",
                config.max_live_segments
            ),
        )
        .with_retryable(true));
    }
    Ok(())
}

fn ceil_segments(rows: usize, capacity: usize) -> usize {
    rows / capacity + usize::from(rows % capacity != 0)
}

fn build_publication(
    table: &NativeAppendTable,
    state: &NativeAppendTableState,
    expected_live_rows: u64,
) -> Result<TableActorLiveSegmentsPublication> {
    let mut next = next_offset(table.table_id, state.persisted_log_offset)?;
    let mut publications = Vec::new();
    let mut row_count = 0_u64;
    for epoch in &state.epochs {
        for segment in epoch
            .materializer
            .append_live_segments()
            .map_err(|error| materializer_error("snapshot native append segments", error))?
        {
            let rows = u64::try_from(segment.row_count()).map_err(|_| {
                resource_exhausted(table.table_id, "native append segment rows exceed u64")
            })?;
            let last = next.checked_add(rows.saturating_sub(1)).ok_or_else(|| {
                resource_exhausted(table.table_id, "native append logical offsets exhausted")
            })?;
            let offsets =
                OffsetRange::new(LogOffset::new(next), LogOffset::new(last)).map_err(|error| {
                    ServerError::new(
                        ErrorCode::Internal,
                        format!("construct native append segment offsets error=[{error}]"),
                    )
                    .with_table_id(table.table_id)
                    .with_retryable(false)
                })?;
            publications.push(segment_publication(
                epoch.writer_epoch,
                table.schema_version,
                offsets,
                segment,
            ));
            row_count = row_count.checked_add(rows).ok_or_else(|| {
                resource_exhausted(table.table_id, "native append live row count exhausted")
            })?;
            next = last.checked_add(1).ok_or_else(|| {
                resource_exhausted(table.table_id, "native append next offset exhausted")
            })?;
        }
    }
    if row_count != expected_live_rows {
        return Err(ServerError::new(
            ErrorCode::Internal,
            format!(
                "native append materialized row count mismatch expected=[{}] actual=[{row_count}]",
                expected_live_rows
            ),
        )
        .with_table_id(table.table_id)
        .with_retryable(false));
    }
    Ok(TableActorLiveSegmentsPublication::new(
        table.table_id,
        publications,
    ))
}

fn segment_publication(
    writer_epoch: u64,
    schema_version: u64,
    offsets: OffsetRange,
    segment: StreamTableLiveSegment,
) -> TableActorLiveSegmentPublication {
    let descriptor = segment.descriptor().clone();
    let identity = SourceSegmentIdentity {
        segment_id: descriptor.segment_id(),
        generation: descriptor.generation(),
        writer_epoch,
    };
    let role = if segment.is_sealed() {
        TableActorLiveSegmentRole::RetainedSealed
    } else {
        TableActorLiveSegmentRole::Active
    };
    TableActorLiveSegmentPublication::new(
        role,
        identity,
        schema_version,
        offsets,
        descriptor,
        0,
        segment.row_count(),
    )
}

fn next_offset(table_id: TableId, watermark: OffsetWatermark) -> Result<u64> {
    match watermark {
        OffsetWatermark::Empty => Ok(0),
        OffsetWatermark::Known(offset) => offset.get().checked_add(1).ok_or_else(|| {
            resource_exhausted(table_id, "native append persisted offset exhausted")
        }),
        OffsetWatermark::LegacyUnknown => Err(ServerError::new(
            ErrorCode::DataCorruption,
            "native append materializer cannot follow a legacy-unknown offset",
        )
        .with_table_id(table_id)
        .with_retryable(false)),
    }
}

fn materializer_error(stage: &str, error: ZippyError) -> ServerError {
    let code = match &error {
        ZippyError::InvalidConfig { .. } => ErrorCode::InvalidArgument,
        ZippyError::SchemaMismatch { .. } => ErrorCode::SchemaMismatch,
        ZippyError::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
        _ => ErrorCode::Internal,
    };
    ServerError::new(code, format!("{stage} error=[{error}]")).with_retryable(false)
}

fn invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn resource_exhausted(table_id: TableId, message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message)
        .with_table_id(table_id)
        .with_retryable(false)
}

fn resource_exhausted_no_table(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}
