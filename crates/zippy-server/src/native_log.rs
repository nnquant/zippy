//! Ordered, bounded event windows over a single pinned native table snapshot.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use zippy_query::{PersistedDecodedOutputAdmission, PersistedFileDescription};
use zippy_segment_store::RowSpanBatchReader;
use zippy_types::query::{QueryInstanceId, QueryPlanInfo, QueryResultBatch};
use zippy_types::storage::CatalogSnapshotMemoryLease;
use zippy_types::table::{SourceSegmentIdentity, TableKind, TableRef};
use zippy_types::{
    ErrorCode, LogOffset, OffsetRange, OffsetWatermark, QueryId, ResourceLimit, Result, ServerError,
};

use crate::native_query::fail_execution;
use crate::query_execution::result_batch_memory_bytes;
use crate::query_service::{
    QueryExecutionScope, QueryResultCharge, QueryTaskCharge, QueryTaskPermit,
};
use crate::query_snapshot::{NativeQueryReadSnapshot, NativeQuerySnapshotCoordinator};

/// A finite event-log window, using the query service's ownership, deadline and memory ledger.
#[derive(Debug, Clone)]
pub struct NativeLogRequest {
    /// Caller identity for status and cancellation of this window.
    pub query_id: QueryId,
    /// Exact table reference resolved through the native Catalog.
    pub table: TableRef,
    /// Inclusive next event offset. `None` captures the latest boundary without replaying rows.
    pub start: Option<LogOffset>,
    /// Absolute UTC Unix-nanosecond deadline.
    pub deadline_ns: Option<u64>,
    /// Explicit or server-default memory budget; disabled memory is rejected.
    pub memory_limit_bytes: ResourceLimit,
    /// Maximum rows per physical output fragment; frames retain their complete boundary.
    pub batch_rows: Option<NonZeroU32>,
}

/// One original-order event fragment. Keep this owner alive while using the Arrow data.
pub struct NativeLogBatch {
    /// Result ownership retains its memory reservation even after window EOF.
    pub batch: QueryResultBatch,
    /// Inclusive offsets corresponding one-to-one to the delivered rows.
    pub offsets: OffsetRange,
    /// Full original frame boundary, when reading a frame table.
    pub frame: Option<OffsetRange>,
    /// Highest event in the fixed window containing this fragment.
    pub watermark: OffsetWatermark,
}

/// Sequential event fragments; dropping the stream cancels its query window.
pub type NativeLogStream = Pin<Box<dyn Stream<Item = Result<NativeLogBatch>> + Send>>;

/// One continuously followed native event log with a stable cancellation identity.
///
/// Frame and key-value mutation fragments retain their original atomic boundaries. Consumers
/// must assemble a complete frame or mutation before exposing it as an atomic state change.
pub struct NativeLogSubscription {
    window: NativeLogWindow,
    changes: zippy_ingest::TableActorChanges,
}

impl NativeLogSubscription {
    pub(crate) fn changes(&self) -> zippy_ingest::TableActorChanges {
        self.changes.clone()
    }

    pub(crate) fn cancellation(&self) -> crate::query_service::QueryCancellation {
        self.window.execution.cancellation()
    }

    /// Returns the immutable schema version bound by this connection.
    pub fn schema_version(&self) -> u64 {
        self.window.info.snapshots[0].schema_version
    }

    /// Returns the resolved immutable table identity.
    pub fn table_id(&self) -> zippy_types::TableId {
        self.window.info.snapshots[0].table_id
    }

    /// Returns the identity shared by every refreshed window of this subscription.
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.window.instance_id()
    }
    /// Returns the stable event schema. A schema change terminates the subscription.
    pub fn schema(&self) -> SchemaRef {
        self.window.schema()
    }
    /// Returns the logical table semantics of the event stream.
    pub fn kind(&self) -> TableKind {
        self.window.kind()
    }
    /// Returns the initial fixed boundary, including for a latest-only start.
    pub fn watermark(&self) -> OffsetWatermark {
        self.window.watermark()
    }
    /// Starts a reader-driven stream with no background producer or result queue.
    pub fn into_stream(self) -> NativeLogStream {
        self.window.into_reader(Some(self.changes))
    }
}

/// A pinned event window that neither follows future writes nor silently skips gaps.
pub struct NativeLogWindow {
    pub(crate) instance_id: QueryInstanceId,
    pub(crate) info: QueryPlanInfo,
    pub(crate) plan: Arc<NativeLogPlan>,
    pub(crate) execution: QueryExecutionScope,
    pub(crate) coordinator: NativeQuerySnapshotCoordinator,
    pub(crate) batch_rows: NonZeroUsize,
}

impl NativeLogWindow {
    pub(crate) async fn follow(
        mut self,
        start: Option<LogOffset>,
    ) -> Result<NativeLogSubscription> {
        let opening: Result<_> = async {
            let table_id = self.info.snapshots[0].table_id;
            let changes = self.coordinator.table_changes(table_id)?;
            // Register before the authoritative opening boundary to avoid a lost wakeup between
            // the initial name resolution and attachment to this immutable table identity.
            self.refresh(start).await?;
            if self.plan.sources.is_empty() {
                drop(self.execution.snapshot_refresh_token()?);
            }
            Ok(changes)
        }
        .await;
        match opening {
            Ok(changes) => Ok(NativeLogSubscription {
                window: self,
                changes,
            }),
            Err(error) => Err(fail_execution(self.execution, error)),
        }
    }

    async fn refresh(&mut self, start: Option<LogOffset>) -> Result<()> {
        let planning = self.execution.snapshot_refresh_token()?;
        let _admission = self.coordinator.admit_event_window(&planning).await?;
        let table = TableRef::Id {
            table_id: self.info.snapshots[0].table_id,
        };
        let mut snapshot = self.coordinator.pin_persisted(&planning, &[table]).await?;
        let plan = snapshot.log_plan(&planning, start)?;
        if plan.kind != self.plan.kind || plan.schema != self.plan.schema {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "event subscription schema changed",
            ));
        }
        if self.plan.watermark.known() > plan.watermark.known() {
            return Err(log_error("event subscription watermark regressed"));
        }
        self.info = self.execution.replace_native_snapshot(snapshot)?;
        self.plan = plan;
        Ok(())
    }

    /// Exact window incarnation accepted by query status and cancellation.
    pub const fn instance_id(&self) -> QueryInstanceId {
        self.instance_id
    }
    /// Catalog revision and versions pinned for this window.
    pub const fn info(&self) -> &QueryPlanInfo {
        &self.info
    }
    /// Raw event schema; key-value windows include all v2 mutation control columns.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.plan.schema)
    }
    /// Table semantics needed to assemble frame fragments or apply key-value mutations.
    pub fn kind(&self) -> TableKind {
        self.plan.kind
    }
    /// Highest event visible at window creation, independent of subsequent persistence.
    pub fn watermark(&self) -> OffsetWatermark {
        self.plan.watermark
    }
    /// First offset after the complete window, or `None` when the offset domain is exhausted.
    pub fn next_offset(&self) -> Option<LogOffset> {
        match self.plan.watermark {
            OffsetWatermark::Empty => Some(LogOffset::new(0)),
            OffsetWatermark::Known(last) => last.checked_next(),
        }
    }
    /// Begins sequential I/O only when polled. No producer task or unbounded queue is created.
    pub fn into_stream(self) -> NativeLogStream {
        self.into_reader(None)
    }

    fn into_reader(self, changes: Option<zippy_ingest::TableActorChanges>) -> NativeLogStream {
        let state = WindowReader {
            window: self,
            source_index: 0,
            reader: None,
            consumed: 0,
            changes,
        };
        Box::pin(futures::stream::try_unfold(state, |mut state| async move {
            match state.next().await {
                Ok(Some(batch)) => Ok(Some((batch, state))),
                Ok(None) => {
                    state.window.execution.finish()?;
                    Ok(None)
                }
                Err(error) => {
                    drop(state.reader.take());
                    Err(fail_execution(state.window.execution, error))
                }
            }
        }))
    }
}

pub(crate) struct NativeLogPlan {
    pub(crate) schema: SchemaRef,
    pub(crate) kind: TableKind,
    pub(crate) watermark: OffsetWatermark,
    pub(crate) sources: Vec<NativeLogSource>,
    // Last: release the schema and source allocations before their accounting lease.
    pub(crate) _memory: CatalogSnapshotMemoryLease,
}

#[derive(Clone, Copy)]
pub(crate) enum NativeLogLocation {
    File(PersistedFileDescription),
    Live {
        index: usize,
        max_utf8_value_bytes: u64,
    },
}

#[derive(Clone, Copy)]
pub(crate) struct NativeLogSource {
    pub(crate) location: NativeLogLocation,
    /// Original physical source range, before cursor clipping or frame selection.
    pub(crate) physical: OffsetRange,
    pub(crate) selected: OffsetRange,
    pub(crate) frame: Option<OffsetRange>,
    pub(crate) frame_source: Option<SourceSegmentIdentity>,
}

enum SourceReader {
    File(Box<ParquetRecordBatchStream<Box<dyn AsyncFileReader>>>),
    Live(QueryTaskPermit<RowSpanBatchReader>),
}

impl SourceReader {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            Self::File(reader) => reader.next().await.transpose().map_err(parquet_error),
            Self::Live(reader) => reader.resource_mut().next_batch().map_err(log_error),
        }
    }
}

struct WindowReader {
    window: NativeLogWindow,
    source_index: usize,
    reader: Option<SourceReader>,
    consumed: u64,
    changes: Option<zippy_ingest::TableActorChanges>,
}

impl WindowReader {
    async fn next(&mut self) -> Result<Option<NativeLogBatch>> {
        let mut cancellation = self.window.execution.cancellation();
        let mut changes = self.changes.clone();
        tokio::select! {
            biased;
            terminal = cancellation.wait_terminal() => {
                Err(terminal?.error.unwrap_or_else(|| ServerError::new(ErrorCode::Cancelled, "native log window was cancelled")))
            }
            terminal = async {
                match changes.as_mut() {
                    Some(changes) => changes.wait_for_stop().await,
                    None => std::future::pending().await,
                }
            } => Err(terminal),
            result = self.next_inner() => result,
        }
    }

    async fn next_inner(&mut self) -> Result<Option<NativeLogBatch>> {
        loop {
            if let Some(changes) = &self.changes {
                changes.check_active()?;
            }
            let Some(source) = self.window.plan.sources.get(self.source_index).copied() else {
                let Some(changes) = self.changes.as_mut() else {
                    return Ok(None);
                };
                let next = self.window.next_offset().ok_or_else(|| {
                    ServerError::new(
                        ErrorCode::ResourceExhausted,
                        "event subscription offset domain is exhausted",
                    )
                })?;
                // An idle subscriber needs its cursor and schema, not the old physical pins.
                drop(self.window.execution.snapshot_refresh_token()?);
                changes.changed().await?;
                loop {
                    match self.window.refresh(Some(next)).await {
                        Ok(()) => break,
                        Err(error) if error.code() == ErrorCode::ResourceExhausted => {
                            if !changes.wait_for_transition().await? {
                                return Err(error);
                            }
                        }
                        Err(error) => return Err(error),
                    }
                }
                self.source_index = 0;
                self.consumed = 0;
                continue;
            };
            if self.reader.is_none() {
                self.reader = Some(self.open(source).await?);
                self.consumed = 0;
            }
            match self.reader.as_mut().expect("opened source").next().await? {
                Some(batch) => {
                    if let Some(changes) = &self.changes {
                        changes.check_active()?;
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let first = source
                        .selected
                        .first()
                        .get()
                        .checked_add(self.consumed)
                        .ok_or_else(|| log_error("event offset overflowed"))?;
                    let last = first
                        .checked_add(batch.num_rows() as u64 - 1)
                        .ok_or_else(|| log_error("event batch offset overflowed"))?;
                    if last > source.selected.last().get() {
                        return Err(log_error("event reader exceeded the pinned range"));
                    }
                    let batch = zippy_types::schema_evolution::align_historical_batch(
                        &batch,
                        Arc::clone(&self.window.plan.schema),
                    )
                    .map_err(log_error)?;
                    let memory =
                        result_batch_memory_bytes(&batch, self.window.execution.query_id())?;
                    let permit = self.window.execution.admit_result(
                        QueryResultCharge {
                            memory_bytes: memory,
                        },
                        batch,
                    )?;
                    let batch = QueryResultBatch::with_owner(
                        permit.as_ref().clone(),
                        (permit, Arc::clone(&self.window.plan)),
                    );
                    self.consumed += batch.record_batch().num_rows() as u64;
                    return Ok(Some(NativeLogBatch {
                        batch,
                        offsets: OffsetRange::new(LogOffset::new(first), LogOffset::new(last))
                            .expect("nonempty batch"),
                        frame: source.frame,
                        watermark: self.window.watermark(),
                    }));
                }
                None => {
                    if u128::from(self.consumed) != source.selected.len() {
                        return Err(log_error("event reader ended before the pinned range"));
                    }
                    self.reader = None;
                    self.source_index += 1;
                }
            }
        }
    }

    async fn open(&mut self, source: NativeLogSource) -> Result<SourceReader> {
        let start = usize::try_from(source.selected.first().get() - source.physical.first().get())
            .map_err(log_error)?;
        let rows = usize::try_from(source.selected.len()).map_err(log_error)?;
        let bound = match source.location {
            NativeLogLocation::File(_) => None,
            NativeLogLocation::Live {
                max_utf8_value_bytes,
                ..
            } => Some(max_utf8_value_bytes),
        };
        let admission = PersistedDecodedOutputAdmission::try_for_log_scan(
            self.window.batch_rows,
            &self.window.plan.schema,
            bound,
            rows as u64,
        )
        .map_err(plan_error)?;
        match source.location {
            NativeLogLocation::File(file) => {
                let reader = self.window.coordinator.deferred_described_file_reader(
                    &self.window.execution,
                    QueryTaskCharge {
                        cpu_slots: NonZeroU32::new(1).unwrap(),
                        memory_bytes: NonZeroU64::new(64 * 1024).unwrap(),
                    },
                    file,
                    admission,
                )?;
                let reader: Box<dyn AsyncFileReader> = reader;
                let stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(parquet_error)?
                    .with_batch_size(self.window.batch_rows.get())
                    .with_offset(start)
                    .with_limit(rows)
                    .build()
                    .map_err(parquet_error)?;
                Ok(SourceReader::File(Box::new(stream)))
            }
            NativeLogLocation::Live { index, .. } => {
                let charge = QueryTaskCharge {
                    cpu_slots: NonZeroU32::new(1).unwrap(),
                    memory_bytes: admission.active_decode_memory_bytes().map_err(plan_error)?,
                };
                let reader = self
                    .window
                    .execution
                    .admit_snapshot_task::<NativeQueryReadSnapshot, _>(charge, |snapshot| {
                        snapshot
                            .log_span(index, source.physical)?
                            .slice_rows(start, start + rows)
                            .map_err(log_error)?
                            .batch_reader(self.window.batch_rows.get(), None)
                            .map_err(log_error)
                    })?;
                Ok(SourceReader::Live(reader))
            }
        }
    }
}

pub(crate) fn log_error(error: impl std::fmt::Display) -> ServerError {
    ServerError::new(
        ErrorCode::DataCorruption,
        format!("native event log is invalid error=[{error}]"),
    )
    .with_retryable(false)
}

fn parquet_error(error: parquet::errors::ParquetError) -> ServerError {
    plan_error(datafusion::error::DataFusionError::ParquetError(error))
}

fn plan_error(error: datafusion::error::DataFusionError) -> ServerError {
    use zippy_query::{
        classify_persisted_execution_error, structured_persisted_execution_error,
        PersistedExecutionErrorClass,
    };
    if let Some(error) = structured_persisted_execution_error(&error) {
        return error;
    }
    let code = match classify_persisted_execution_error(&error) {
        PersistedExecutionErrorClass::ResourceExhausted => ErrorCode::ResourceExhausted,
        PersistedExecutionErrorClass::Unsupported => ErrorCode::UnsupportedPlan,
        PersistedExecutionErrorClass::DataCorruption => ErrorCode::DataCorruption,
        PersistedExecutionErrorClass::Internal => ErrorCode::Internal,
    };
    ServerError::new(code, format!("native log reader failed error=[{error}]"))
}

/// Validates complete native history before clipping the requested replay window.
pub(crate) fn select_sources(
    sources: &mut Vec<NativeLogSource>,
    watermark: OffsetWatermark,
    start: Option<LogOffset>,
) -> Result<()> {
    sources.sort_unstable_by_key(|source| source.selected.first());
    let mut expected = Some(LogOffset::new(0));
    for source in sources.iter() {
        if Some(source.selected.first()) != expected {
            return Err(log_error(
                "native event history has a gap or overlapping sources",
            ));
        }
        expected = source.selected.last().checked_next();
    }
    let observed = sources.last().map_or(OffsetWatermark::Empty, |source| {
        OffsetWatermark::Known(source.selected.last())
    });
    if observed != watermark {
        return Err(log_error(
            "native event sources differ from the pinned watermark",
        ));
    }
    // Compaction can split a frame's lineage between output files. Recover the complete frame
    // by stable source identity, never by physical file or row-group boundaries.
    let mut seen = std::collections::BTreeSet::new();
    let mut index = 0;
    while index < sources.len() {
        let Some(identity) = sources[index].frame_source else {
            index += 1;
            continue;
        };
        if !seen.insert(identity) {
            return Err(log_error(
                "frame source identity reappears after another frame",
            ));
        }
        let mut end = index + 1;
        while end < sources.len() && sources[end].frame_source == Some(identity) {
            end += 1;
        }
        let frame = OffsetRange::new(
            sources[index].selected.first(),
            sources[end - 1].selected.last(),
        )
        .map_err(log_error)?;
        for source in &mut sources[index..end] {
            source.frame = Some(frame);
        }
        index = end;
    }
    if let Some(start) = start {
        if expected.is_some_and(|next| start > next) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "event cursor is beyond the pinned watermark",
            ));
        }
        if sources.iter().any(|source| {
            source
                .frame
                .is_some_and(|frame| frame.contains(start) && frame.first() != start)
        }) {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "frame cursor must identify a complete frame boundary",
            ));
        }
        sources.retain(|source| source.selected.last() >= start);
        if let Some(source) = sources.first_mut() {
            source.selected =
                OffsetRange::new(start.max(source.selected.first()), source.selected.last())
                    .map_err(log_error)?;
        }
    } else {
        sources.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn range(first: u64, last: u64) -> OffsetRange {
        OffsetRange::new(LogOffset::new(first), LogOffset::new(last)).unwrap()
    }
    fn source(first: u64, last: u64, frame: Option<u64>) -> NativeLogSource {
        NativeLogSource {
            location: NativeLogLocation::Live {
                index: 0,
                max_utf8_value_bytes: 0,
            },
            physical: range(first, last),
            selected: range(first, last),
            frame: frame.map(|_| range(first, last)),
            frame_source: frame.map(|id| SourceSegmentIdentity {
                segment_id: id,
                generation: 1,
                writer_epoch: 1,
            }),
        }
    }
    #[test]
    fn replay_orders_and_clips_without_skipping_gaps_or_accepting_future_cursors() {
        let mut sources = vec![source(3, 5, None), source(0, 2, None)];
        let watermark = OffsetWatermark::Known(LogOffset::new(5));
        select_sources(&mut sources, watermark, Some(LogOffset::new(1))).unwrap();
        assert_eq!(sources[0].selected, range(1, 2));
        assert_eq!(sources[0].physical, range(0, 2));
        assert_eq!(sources[1].selected, range(3, 5));
        for mut invalid in [
            vec![source(1, 5, None)],
            vec![source(0, 1, None), source(3, 5, None)],
            vec![source(0, 3, None), source(3, 5, None)],
            vec![source(0, 4, None)],
        ] {
            assert_eq!(
                select_sources(&mut invalid, watermark, Some(LogOffset::new(0)))
                    .unwrap_err()
                    .code(),
                ErrorCode::DataCorruption
            );
        }
        let mut sources = vec![source(0, 5, None)];
        assert_eq!(
            select_sources(&mut sources, watermark, Some(LogOffset::new(7)))
                .unwrap_err()
                .code(),
            ErrorCode::InvalidArgument
        );
        select_sources(&mut sources, watermark, Some(LogOffset::new(6))).unwrap();
        assert!(sources.is_empty());
        select_sources(
            &mut Vec::new(),
            OffsetWatermark::Empty,
            Some(LogOffset::new(0)),
        )
        .unwrap();
    }
    #[test]
    fn frame_lineage_rejoins_files_and_rejects_partial_resume_or_identity_reuse() {
        let original = vec![
            source(2, 4, Some(1)),
            source(5, 5, Some(2)),
            source(0, 1, Some(1)),
        ];
        let watermark = OffsetWatermark::Known(LogOffset::new(5));
        let mut sources = original.clone();
        select_sources(&mut sources, watermark, Some(LogOffset::new(0))).unwrap();
        assert_eq!(sources[0].frame, Some(range(0, 4)));
        assert_eq!(sources[1].frame, Some(range(0, 4)));
        assert_eq!(sources[2].frame, Some(range(5, 5)));
        for cursor in [1, 2, 4] {
            assert_eq!(
                select_sources(
                    &mut original.clone(),
                    watermark,
                    Some(LogOffset::new(cursor))
                )
                .unwrap_err()
                .code(),
                ErrorCode::InvalidArgument
            );
        }
        let mut invalid = vec![
            source(0, 0, Some(1)),
            source(1, 1, Some(2)),
            source(2, 2, Some(1)),
        ];
        assert_eq!(
            select_sources(
                &mut invalid,
                OffsetWatermark::Known(LogOffset::new(2)),
                None
            )
            .unwrap_err()
            .code(),
            ErrorCode::DataCorruption
        );
    }
}
