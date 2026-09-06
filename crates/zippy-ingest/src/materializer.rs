//! Native append materialization and serialized TableActor publication.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::{StreamTableLiveSegment, StreamTableMaterializer};
use zippy_storage::{ParquetCandidateSpec, PersistCandidateInput, PersistRequest};
use zippy_types::table::{
    MetadataMap, NativeDataFileProvenance, SourceSegmentIdentity, SourceSegmentLineage,
    SourceSegmentLineages, TableDefinition,
};
use zippy_types::{
    BoxFuture, ErrorCode, FileId, LogOffset, OffsetRange, OffsetWatermark, RequestId, Result,
    ServerError, TableId,
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
    max_buffered_bytes: usize,
}

impl Default for NativeAppendMaterializerConfig {
    fn default() -> Self {
        Self {
            row_capacity: DEFAULT_NATIVE_APPEND_ROW_CAPACITY,
            max_live_segments: DEFAULT_NATIVE_APPEND_MAX_LIVE_SEGMENTS,
            max_buffered_bytes: 64 * 1024 * 1024,
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

    /// Sets the maximum accepted Arrow input bytes not yet reclaimed by a checkpoint.
    pub const fn with_max_buffered_bytes(mut self, maximum: usize) -> Self {
        self.max_buffered_bytes = maximum;
        self
    }

    /// Validates static allocation bounds.
    pub fn validate(self) -> Result<()> {
        if self.max_buffered_bytes == 0 || self.max_buffered_bytes > 256 * 1024 * 1024 {
            return Err(invalid(
                "native append buffered byte limit must be between 1 and 256 MiB",
            ));
        }
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
    tables: BTreeMap<TableId, Arc<NativeAppendTable>>,
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
                    retained_start: next_offset(table_id, bootstrap.persisted_log_offset())?,
                    buffered_bytes: 0,
                    batch_bytes: VecDeque::new(),
                    epochs: Vec::new(),
                }),
            };
            if tables.insert(table_id, Arc::new(table)).is_some() {
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
        self.tables.get(&table_id).map(Arc::as_ref).ok_or_else(|| {
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
    retained_start: u64,
    buffered_bytes: usize,
    batch_bytes: VecDeque<(u64, usize)>,
    epochs: Vec<EpochMaterializer>,
}

struct EpochMaterializer {
    writer_epoch: u64,
    materializer: StreamTableMaterializer,
}

impl NativeIngestSink for NativeAppendMaterializerSink {
    fn supports_checkpoint(&self) -> bool {
        self.tables
            .values()
            .any(|table| table.actor.persistence_enabled())
    }

    fn prepare_checkpoint(
        &self,
        table_id: TableId,
        watermark: OffsetWatermark,
    ) -> Result<Option<BoxFuture<'static, Result<OffsetWatermark>>>> {
        let table = Arc::clone(
            self.tables
                .get(&table_id)
                .ok_or_else(|| invalid("checkpoint table missing"))?,
        );
        if !table.actor.persistence_enabled() {
            return Ok(None);
        }
        {
            let mut state = table.state.lock().unwrap();
            if state.last_applied_log_offset != watermark {
                return Err(invalid(
                    "checkpoint does not match the accepted mutation boundary",
                ));
            }
            if let Some(current) = state.epochs.last_mut() {
                current
                    .materializer
                    .seal_active_for_writer_handoff()
                    .map_err(|error| materializer_error("seal checkpoint prefix", error))?;
            }
            table
                .actor
                .try_publish_live_segments(build_publication(&table, &state, state.live_rows)?)?
                .wait_blocking()?;
        }
        Ok(Some(Box::pin(async move {
            persist_checkpoint_prefix(table, watermark).await
        })))
    }

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
        let batch_bytes = batch.get_array_memory_size();
        if batch_bytes
            > self
                .config
                .max_buffered_bytes
                .saturating_sub(state.buffered_bytes)
        {
            return Err(NativeIngestSinkError::Rejected(
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "native append persistence backlog exhausted its byte budget",
                )
                .with_table_id(table.table_id)
                .with_retryable(true),
            ));
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
        state.buffered_bytes += batch_bytes;
        if let OffsetWatermark::Known(last) = context.receipt.committed_log_offset() {
            state.batch_bytes.push_back((last.get(), batch_bytes));
        }
        Ok(())
    }
}

async fn persist_checkpoint_prefix(
    table: Arc<NativeAppendTable>,
    watermark: OffsetWatermark,
) -> Result<OffsetWatermark> {
    let snapshot = table.actor.snapshot().await?;
    if snapshot.committed_log_offset != watermark {
        let first = next_offset(table.table_id, snapshot.committed_log_offset)?;
        let OffsetWatermark::Known(last) = watermark else {
            return Err(invalid("empty checkpoint cannot follow persisted rows"));
        };
        if first > last.get() {
            return Err(invalid("checkpoint watermark regressed"));
        }
        let mut batches = Vec::new();
        let mut lineage = Vec::new();
        for segment in snapshot.live_segments.iter() {
            if segment.offsets().first().get() > last.get() {
                break;
            }
            let end = segment.offsets().last().get().min(last.get());
            let count = usize::try_from(end - segment.offsets().first().get() + 1)
                .map_err(|_| invalid("checkpoint row count exceeds platform usize"))?;
            let span = segment
                .span()
                .slice_rows(0, count)
                .map_err(|error| invalid(format!("checkpoint row span failed error=[{error}]")))?;
            batches.push(
                SegmentTableView::from_row_span(span)
                    .to_record_batch()
                    .map_err(|error| materializer_error("read checkpoint prefix", error))?,
            );
            lineage.push(SourceSegmentLineage {
                source: segment.identity(),
                offsets: OffsetRange::new(segment.offsets().first(), LogOffset::new(end))
                    .map_err(|error| invalid(error.to_string()))?,
            });
        }
        let offsets = OffsetRange::new(LogOffset::new(first), last)
            .map_err(|error| invalid(error.to_string()))?;
        let provenance = NativeDataFileProvenance::new(
            offsets,
            SourceSegmentLineages::new(lineage).map_err(|error| invalid(error.to_string()))?,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let mut digest = Sha256::new();
        digest.update(b"zippy.native.checkpoint.v1");
        digest.update(table.table_id.to_string().as_bytes());
        digest.update(table.schema_version.to_le_bytes());
        digest.update(first.to_le_bytes());
        digest.update(last.get().to_le_bytes());
        let hash = digest.finalize();
        let mut id_bytes: [u8; 16] = hash[..16].try_into().expect("sha256 has at least 16 bytes");
        id_bytes[0] |= 1;
        let file_id = FileId::from_bytes(id_bytes).map_err(|error| invalid(error.to_string()))?;
        let request_id =
            RequestId::from_bytes(id_bytes).map_err(|error| invalid(error.to_string()))?;
        let created_at_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| invalid(error.to_string()))?
            .as_nanos();
        let candidate = PersistCandidateInput::new(
            ParquetCandidateSpec {
                file_id,
                table_id: table.table_id,
                schema_version: table.schema_version,
                schema: Arc::clone(&table.schema),
                provenance,
                partition: MetadataMap::new(),
                metadata: MetadataMap::new(),
                created_at_ns: u64::try_from(created_at_ns)
                    .map_err(|_| invalid("checkpoint timestamp exceeds u64 nanoseconds"))?,
            },
            batches,
        )?;
        let request = PersistRequest::new(
            request_id,
            table.table_id,
            snapshot.persisted_snapshot_version,
            vec![candidate],
            watermark,
        )?;
        table.actor.persist(request).await?;
    }
    drop(snapshot);
    // Keep publication ordered with synchronous admissions without blocking an async executor.
    tokio::task::spawn_blocking(move || {
        let mut state = table.state.lock().unwrap();
        state.persisted_log_offset = watermark;
        reclaim_materialized_prefix(&table, &mut state)
    })
    .await
    .map_err(|error| {
        invalid(format!(
            "checkpoint reclamation task failed error=[{error}]"
        ))
    })??;
    Ok(watermark)
}

fn reclaim_materialized_prefix(
    table: &NativeAppendTable,
    state: &mut NativeAppendTableState,
) -> Result<()> {
    let end = next_offset(table.table_id, state.persisted_log_offset)?;
    let mut budget = end.saturating_sub(state.retained_start);
    let mut reclaimed = 0_u64;
    for epoch in &mut state.epochs {
        let rows = epoch
            .materializer
            .retire_persisted_append_prefix(usize::try_from(budget).unwrap_or(usize::MAX))
            .map_err(|error| materializer_error("retire persisted append prefix", error))?
            as u64;
        budget -= rows;
        reclaimed += rows;
        if epoch
            .materializer
            .append_live_segment_count()
            .map_err(|error| materializer_error("count retained checkpoint segments", error))?
            != 0
        {
            break;
        }
    }
    state.retained_start += reclaimed;
    state.live_rows -= reclaimed;
    while state
        .batch_bytes
        .front()
        .is_some_and(|(end, _)| *end < state.retained_start)
    {
        state.buffered_bytes -= state.batch_bytes.pop_front().expect("front exists").1;
    }
    while state.epochs.len() > 1 {
        if state.epochs[0]
            .materializer
            .append_live_segment_count()
            .map_err(|error| materializer_error("count retired epoch segments", error))?
            != 0
        {
            break;
        }
        state.epochs.remove(0);
    }
    table
        .actor
        .try_publish_live_segments(build_publication(table, state, state.live_rows)?)?
        .wait_blocking()?;
    Ok(())
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
        let segment_count = epoch
            .materializer
            .append_live_segment_count()
            .map_err(|error| materializer_error("inspect native append segments", error))?;
        if epoch.writer_epoch == writer_epoch {
            let active_rows = epoch.materializer.active_committed_row_count();
            let sealed = segment_count - usize::from(active_rows != 0);
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
                .checked_add(segment_count)
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
    let mut next = state.retained_start;
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
