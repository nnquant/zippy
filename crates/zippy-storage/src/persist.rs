use std::collections::HashSet;
use std::mem::{size_of, size_of_val};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use zippy_types::table::{
    MetadataMap, MetadataScalar, NativeDataFileProvenance, SourceSegmentLineage,
    SourceSegmentLineages,
};
use zippy_types::{ErrorCode, LogOffset, OffsetWatermark, RequestId, Result, ServerError, TableId};

use crate::candidate::PreparedParquetCandidateSpec;
use crate::commit::CatalogCommitFailure;
use crate::commit_intent::MAX_FILES_PER_COMMIT_INTENT;
use crate::{
    CatalogCommitCoordinator, CommitDataFilesIntent, CommitDataFilesReceipt,
    DurableParquetCandidate, ParquetCandidateSpec, ParquetCandidateWriter,
};

const DEFAULT_MAX_OUTSTANDING_JOBS: usize = 64;
const DEFAULT_MAX_INFLIGHT_INPUT_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_MAX_PARALLEL_WRITERS: usize = 4;
const MAX_MAX_OUTSTANDING_JOBS: usize = 4_096;
const MAX_MAX_INFLIGHT_INPUT_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Hard safety ceiling for concurrent blocking native Parquet writers.
pub const MAX_PERSIST_PARALLEL_WRITERS: usize = 64;
const INPUT_BUDGET_UNIT_BYTES: u64 = 64 * 1024;
const CONSERVATIVE_METADATA_ENTRY_BYTES: usize = 1024;

/// Bounded admission and Parquet execution policy for native persist jobs.
#[derive(Debug, Clone)]
pub struct PersistControllerConfig {
    max_outstanding_jobs: usize,
    max_inflight_input_bytes: u64,
    max_parallel_writers: usize,
}

impl Default for PersistControllerConfig {
    fn default() -> Self {
        Self {
            max_outstanding_jobs: DEFAULT_MAX_OUTSTANDING_JOBS,
            max_inflight_input_bytes: DEFAULT_MAX_INFLIGHT_INPUT_BYTES,
            max_parallel_writers: DEFAULT_MAX_PARALLEL_WRITERS,
        }
    }
}

impl PersistControllerConfig {
    /// Creates the default 64-job, 512 MiB, four-writer policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum accepted jobs, including active and queued work.
    #[must_use]
    pub const fn with_max_outstanding_jobs(mut self, maximum: usize) -> Self {
        self.max_outstanding_jobs = maximum;
        self
    }

    /// Sets the aggregate retained persist-input memory budget in bytes.
    ///
    /// Accounting rounds each request up to 64 KiB and conservatively charges
    /// Arrow buffers, schemas, candidate descriptors, metadata, and prepared
    /// footer bytes once per candidate reference.
    #[must_use]
    pub const fn with_max_inflight_input_bytes(mut self, maximum: u64) -> Self {
        self.max_inflight_input_bytes = maximum;
        self
    }

    /// Sets the maximum number of concurrent blocking Parquet writers.
    #[must_use]
    pub const fn with_max_parallel_writers(mut self, maximum: usize) -> Self {
        self.max_parallel_writers = maximum;
        self
    }

    /// Validates hard operational limits before storage startup acquires resources.
    pub fn validate(&self) -> Result<()> {
        if self.max_outstanding_jobs == 0 || self.max_outstanding_jobs > MAX_MAX_OUTSTANDING_JOBS {
            return Err(terminal_invalid(format!(
                concat!(
                    "persist outstanding job limit is outside supported range ",
                    "actual=[{}] maximum=[{}]"
                ),
                self.max_outstanding_jobs, MAX_MAX_OUTSTANDING_JOBS
            )));
        }
        if self.max_inflight_input_bytes < INPUT_BUDGET_UNIT_BYTES
            || self.max_inflight_input_bytes > MAX_MAX_INFLIGHT_INPUT_BYTES
            || self.max_inflight_input_bytes % INPUT_BUDGET_UNIT_BYTES != 0
        {
            return Err(terminal_invalid(format!(
                concat!(
                    "persist input byte budget is outside supported aligned range ",
                    "actual=[{}] minimum=[{}] maximum=[{}] alignment=[{}]"
                ),
                self.max_inflight_input_bytes,
                INPUT_BUDGET_UNIT_BYTES,
                MAX_MAX_INFLIGHT_INPUT_BYTES,
                INPUT_BUDGET_UNIT_BYTES
            )));
        }
        if self.max_parallel_writers == 0
            || self.max_parallel_writers > MAX_PERSIST_PARALLEL_WRITERS
            || self.max_parallel_writers > self.max_outstanding_jobs
        {
            return Err(terminal_invalid(format!(
                concat!(
                    "persist parallel writer limit is outside supported range ",
                    "actual=[{}] maximum=[{}] outstanding_jobs=[{}]"
                ),
                self.max_parallel_writers, MAX_PERSIST_PARALLEL_WRITERS, self.max_outstanding_jobs
            )));
        }
        Ok(())
    }
}

/// One Parquet candidate specification and its retained Arrow input batches.
#[derive(Debug)]
pub struct PersistCandidateInput {
    prepared: PreparedParquetCandidateSpec,
    batches: Vec<RecordBatch>,
    retained_bytes: usize,
}

impl PersistCandidateInput {
    /// Creates and validates one candidate before any file transaction begins.
    pub fn new(mut spec: ParquetCandidateSpec, batches: Vec<RecordBatch>) -> Result<Self> {
        if spec.schema_version == 0 {
            return Err(
                terminal_invalid("persist candidate schema version must be nonzero")
                    .with_table_id(spec.table_id),
            );
        }
        if batches.is_empty() {
            return Err(
                terminal_invalid("persist candidate batches must not be empty")
                    .with_table_id(spec.table_id),
            );
        }
        normalize_provenance_allocation(&mut spec)?;
        let prepared = PreparedParquetCandidateSpec::new(spec)?;
        let spec = prepared.spec();
        let mut row_count = 0_u128;
        let mut normalized_batches = Vec::with_capacity(batches.len());
        for batch in batches {
            if batch.num_rows() == 0 {
                return Err(terminal_invalid(
                    "persist candidate batches must each contain at least one row",
                )
                .with_table_id(spec.table_id));
            }
            if batch.schema().as_ref() != spec.schema.as_ref() {
                return Err(ServerError::new(
                    ErrorCode::SchemaMismatch,
                    "persist candidate batch schema does not match its specification",
                )
                .with_table_id(spec.table_id)
                .with_retryable(false));
            }
            row_count = row_count
                .checked_add(batch.num_rows() as u128)
                .ok_or_else(|| {
                    terminal_resource("persist candidate row count overflow")
                        .with_table_id(spec.table_id)
                })?;
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            let columns = batch.columns().to_vec().into_boxed_slice().into_vec();
            let normalized =
                RecordBatch::try_new_with_options(Arc::clone(&spec.schema), columns, &options)
                    .map_err(|error| {
                        ServerError::new(
                            ErrorCode::SchemaMismatch,
                            format!("normalize persist RecordBatch error=[{error}]"),
                        )
                        .with_table_id(spec.table_id)
                        .with_retryable(false)
                    })?;
            normalized_batches.push(normalized);
        }
        if spec.provenance.offsets().len() != row_count {
            return Err(terminal_invalid(format!(
                concat!(
                    "persist candidate row count does not equal offset coverage ",
                    "rows=[{}] offsets=[{}]"
                ),
                row_count,
                spec.provenance.offsets().len()
            ))
            .with_table_id(spec.table_id));
        }
        let batches = normalized_batches.into_boxed_slice().into_vec();
        let retained_bytes = candidate_retained_bytes(&prepared, &batches)?;
        Ok(Self {
            prepared,
            batches,
            retained_bytes,
        })
    }

    fn spec(&self) -> &ParquetCandidateSpec {
        self.prepared.spec()
    }

    const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn normalize_provenance_allocation(spec: &mut ParquetCandidateSpec) -> Result<()> {
    let offsets = spec.provenance.offsets();
    let lineage = spec
        .provenance
        .source_lineage()
        .as_slice()
        .to_vec()
        .into_boxed_slice()
        .into_vec();
    let lineage = SourceSegmentLineages::new(lineage).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("normalize persist source lineage error=[{error}]"),
        )
        .with_table_id(spec.table_id)
        .with_retryable(false)
    })?;
    spec.provenance = NativeDataFileProvenance::new(offsets, lineage).map_err(|error| {
        ServerError::new(
            ErrorCode::Internal,
            format!("normalize persist provenance error=[{error}]"),
        )
        .with_table_id(spec.table_id)
        .with_retryable(false)
    })?;
    Ok(())
}

fn candidate_retained_bytes(
    prepared: &PreparedParquetCandidateSpec,
    batches: &[RecordBatch],
) -> Result<usize> {
    let spec = prepared.spec();
    let table_id = spec.table_id;
    let batch_slots = batches
        .len()
        .checked_mul(size_of::<RecordBatch>())
        .ok_or_else(|| retained_overflow(table_id, "persist RecordBatch vector"))?;
    let mut retained = prepared
        .footer_capacity()
        .checked_add(batch_slots)
        .ok_or_else(|| retained_overflow(table_id, "persist candidate footer and batches"))?;
    retained = checked_retained_add(
        retained,
        schema_retained_bytes(spec)?,
        table_id,
        "persist candidate schema",
    )?;
    let lineage_bytes = spec
        .provenance
        .source_lineage()
        .as_slice()
        .len()
        .checked_mul(size_of::<SourceSegmentLineage>())
        .ok_or_else(|| retained_overflow(table_id, "persist candidate provenance"))?;
    retained = checked_retained_add(
        retained,
        lineage_bytes,
        table_id,
        "persist candidate provenance",
    )?;
    retained = checked_retained_add(
        retained,
        metadata_map_retained_bytes(&spec.partition, table_id)?,
        table_id,
        "persist candidate partition metadata",
    )?;
    retained = checked_retained_add(
        retained,
        metadata_map_retained_bytes(&spec.metadata, table_id)?,
        table_id,
        "persist candidate file metadata",
    )?;
    batches.iter().try_fold(retained, |total, batch| {
        let column_slots = batch
            .num_columns()
            .checked_mul(size_of::<ArrayRef>())
            .ok_or_else(|| retained_overflow(table_id, "persist RecordBatch columns"))?;
        let total =
            checked_retained_add(total, column_slots, table_id, "persist RecordBatch columns")?;
        checked_retained_add(
            total,
            batch.get_array_memory_size(),
            table_id,
            "persist RecordBatch arrays",
        )
    })
}

fn schema_retained_bytes(spec: &ParquetCandidateSpec) -> Result<usize> {
    let schema = spec.schema.as_ref();
    let table_id = spec.table_id;
    let arc_header = 2_usize
        .checked_mul(size_of::<usize>())
        .ok_or_else(|| retained_overflow(table_id, "persist schema Arc header"))?;
    let metadata_slots = schema
        .metadata()
        .capacity()
        .checked_mul(size_of::<(String, String)>())
        .ok_or_else(|| retained_overflow(table_id, "persist schema metadata slots"))?;
    let metadata_strings = schema
        .metadata()
        .iter()
        .try_fold(0_usize, |total, (key, value)| {
            total
                .checked_add(key.capacity())
                .and_then(|bytes| bytes.checked_add(value.capacity()))
                .ok_or_else(|| retained_overflow(table_id, "persist schema metadata strings"))
        })?;
    [
        size_of_val(schema),
        arc_header,
        schema.fields().size(),
        metadata_slots,
        metadata_strings,
    ]
    .into_iter()
    .try_fold(0_usize, |total, bytes| {
        checked_retained_add(total, bytes, table_id, "persist candidate schema")
    })
}

fn metadata_map_retained_bytes(map: &MetadataMap, table_id: TableId) -> Result<usize> {
    let tree_nodes = map
        .len()
        .checked_mul(CONSERVATIVE_METADATA_ENTRY_BYTES)
        .ok_or_else(|| retained_overflow(table_id, "persist metadata tree nodes"))?;
    map.iter().try_fold(tree_nodes, |total, (key, value)| {
        let value_bytes = match value {
            MetadataScalar::String(value) => value.capacity(),
            MetadataScalar::Bytes(value) => value.capacity(),
            MetadataScalar::Null
            | MetadataScalar::Boolean(_)
            | MetadataScalar::Int64(_)
            | MetadataScalar::UInt64(_)
            | MetadataScalar::Float64(_) => 0,
        };
        total
            .checked_add(key.capacity())
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or_else(|| retained_overflow(table_id, "persist metadata payload"))
    })
}

fn checked_retained_add(
    total: usize,
    bytes: usize,
    table_id: TableId,
    resource: &str,
) -> Result<usize> {
    total
        .checked_add(bytes)
        .ok_or_else(|| retained_overflow(table_id, resource))
}

fn retained_overflow(table_id: TableId, resource: &str) -> ServerError {
    terminal_resource(format!("{resource} retained byte count overflow")).with_table_id(table_id)
}

/// One atomic table snapshot persist request admitted by the controller.
#[derive(Debug)]
pub struct PersistRequest {
    request_id: RequestId,
    table_id: TableId,
    expected_snapshot_version: u64,
    schema_version: u64,
    schema: SchemaRef,
    writer_epoch: u64,
    first_log_offset: LogOffset,
    candidates: Vec<PersistCandidateInput>,
    committed_log_offset: OffsetWatermark,
    retained_input_bytes: usize,
}

impl PersistRequest {
    /// Creates a fully prevalidated persist request and freezes candidate order.
    pub fn new(
        request_id: RequestId,
        table_id: TableId,
        expected_snapshot_version: u64,
        mut candidates: Vec<PersistCandidateInput>,
        committed_log_offset: OffsetWatermark,
    ) -> Result<Self> {
        if expected_snapshot_version == 0 {
            return Err(
                terminal_invalid("persist expected snapshot version must be nonzero")
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            );
        }
        if candidates.is_empty() {
            return Err(terminal_invalid("persist candidates must not be empty")
                .with_request_id(request_id)
                .with_table_id(table_id));
        }
        if candidates.len() > MAX_FILES_PER_COMMIT_INTENT {
            return Err(terminal_resource(format!(
                "persist candidate count exceeds maximum actual=[{}] maximum=[{}]",
                candidates.len(),
                MAX_FILES_PER_COMMIT_INTENT
            ))
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        let OffsetWatermark::Known(committed_log_offset_value) = committed_log_offset else {
            return Err(
                terminal_invalid("persist request requires a known committed log offset")
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            );
        };
        for candidate in &candidates {
            if candidate.spec().table_id != table_id {
                return Err(terminal_invalid(format!(
                    "persist candidate belongs to another table file_id=[{}]",
                    candidate.spec().file_id
                ))
                .with_request_id(request_id)
                .with_table_id(table_id));
            }
        }
        let schema_version = candidates[0].spec().schema_version;
        let schema = Arc::clone(&candidates[0].spec().schema);
        let writer_epoch = candidates[0].spec().provenance.source_lineage().as_slice()[0]
            .source
            .writer_epoch;
        for candidate in &candidates {
            if candidate.spec().schema_version != schema_version {
                return Err(ServerError::new(
                    ErrorCode::SchemaMismatch,
                    "persist request candidates must use one schema version",
                )
                .with_request_id(request_id)
                .with_table_id(table_id)
                .with_retryable(false));
            }
            if candidate.spec().schema.as_ref() != schema.as_ref() {
                return Err(ServerError::new(
                    ErrorCode::SchemaMismatch,
                    "persist request candidates must use one Arrow schema",
                )
                .with_request_id(request_id)
                .with_table_id(table_id)
                .with_retryable(false));
            }
            if candidate
                .spec()
                .provenance
                .source_lineage()
                .as_slice()
                .iter()
                .any(|lineage| lineage.source.writer_epoch != writer_epoch)
            {
                return Err(terminal_invalid(
                    "persist request candidates must use one writer epoch",
                )
                .with_request_id(request_id)
                .with_table_id(table_id));
            }
        }
        candidates.sort_unstable_by_key(|candidate| candidate.spec().file_id);
        if candidates
            .windows(2)
            .any(|pair| pair[0].spec().file_id == pair[1].spec().file_id)
        {
            return Err(terminal_invalid(
                "persist request contains duplicate candidate file identities",
            )
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        let mut coverage: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.spec().provenance.offsets())
            .collect();
        coverage.sort_unstable_by_key(|range| (range.first(), range.last()));
        for pair in coverage.windows(2) {
            let Some(expected) = pair[0].last().checked_next() else {
                return Err(terminal_invalid(
                    "persist candidate coverage continues after maximum log offset",
                )
                .with_request_id(request_id)
                .with_table_id(table_id));
            };
            if pair[1].first() != expected {
                return Err(terminal_invalid(format!(
                    concat!(
                        "persist candidate coverage is not contiguous ",
                        "previous_end=[{}] next_start=[{}]"
                    ),
                    pair[0].last(),
                    pair[1].first()
                ))
                .with_request_id(request_id)
                .with_table_id(table_id));
            }
        }
        let coverage_end = coverage
            .last()
            .expect("nonempty persist candidates have offset coverage")
            .last();
        let first_log_offset = coverage
            .first()
            .expect("nonempty persist candidates have offset coverage")
            .first();
        if committed_log_offset_value != coverage_end {
            return Err(terminal_invalid(format!(
                concat!(
                    "persist watermark does not equal candidate coverage end ",
                    "watermark=[{}] coverage_end=[{}]"
                ),
                committed_log_offset_value, coverage_end
            ))
            .with_request_id(request_id)
            .with_table_id(table_id));
        }
        let candidate_slots = candidates
            .capacity()
            .checked_mul(size_of::<PersistCandidateInput>())
            .ok_or_else(|| {
                terminal_resource("persist candidate vector byte count overflow")
                    .with_request_id(request_id)
                    .with_table_id(table_id)
            })?;
        let retained_input_bytes = candidates.iter().try_fold(
            size_of::<PersistRequest>()
                .checked_add(candidate_slots)
                .ok_or_else(|| {
                    terminal_resource("persist request retained byte count overflow")
                        .with_request_id(request_id)
                        .with_table_id(table_id)
                })?,
            |total, candidate| {
                total
                    .checked_add(candidate.retained_bytes())
                    .ok_or_else(|| {
                        terminal_resource("persist request retained byte count overflow")
                            .with_request_id(request_id)
                            .with_table_id(table_id)
                    })
            },
        )?;
        Ok(Self {
            request_id,
            table_id,
            expected_snapshot_version,
            schema_version,
            schema,
            writer_epoch,
            first_log_offset,
            candidates,
            committed_log_offset,
            retained_input_bytes,
        })
    }

    /// Returns the idempotency identity assigned to this persist operation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the table that owns every candidate in this operation.
    #[must_use]
    pub const fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the current snapshot version this request must extend.
    #[must_use]
    pub const fn expected_snapshot_version(&self) -> u64 {
        self.expected_snapshot_version
    }

    /// Returns the one schema version shared by every candidate.
    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// Borrows the exact Arrow schema shared by every candidate batch.
    #[must_use]
    pub const fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the one producer fencing epoch shared by every source lineage.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }

    /// Returns the first logical offset covered by this atomic request.
    #[must_use]
    pub const fn first_log_offset(&self) -> LogOffset {
        self.first_log_offset
    }

    /// Returns the durable logical boundary requested for the new snapshot.
    #[must_use]
    pub const fn committed_log_offset(&self) -> OffsetWatermark {
        self.committed_log_offset
    }

    /// Returns the conservative retained persist-input charge in bytes.
    #[must_use]
    pub const fn retained_input_bytes(&self) -> usize {
        self.retained_input_bytes
    }
}

/// Cloneable bounded persist admission endpoint.
///
/// The handle cannot close or drain the controller and never exposes the
/// underlying Catalog commit coordinator.
#[derive(Clone)]
pub struct PersistControllerHandle {
    sender: mpsc::Sender<PersistJob>,
    accepting: Arc<AtomicBool>,
    active_tables: Arc<Mutex<HashSet<TableId>>>,
    job_budget: Arc<Semaphore>,
    input_budget: Arc<Semaphore>,
    max_input_bytes: u64,
}

impl PersistControllerHandle {
    /// Immediately admits one request or returns a retry policy without waiting.
    ///
    /// A successful call has reserved this table's single-flight slot, the
    /// outstanding-job budget, retained-input budget, and one bounded queue
    /// slot before enqueueing.
    /// Dropping the returned ticket never cancels the accepted operation.
    pub fn submit(&self, request: PersistRequest) -> Result<PersistTicket> {
        self.submit_classified(request)
            .map_err(PersistCompletionFailure::into_error)
    }

    /// Immediately admits one request while preserving failure disposition.
    ///
    /// Backpressure and invalid request budgets are request-local rejections.
    /// A closed controller, closed internal budget, or poisoned admission state
    /// is fatal because the owning runtime can no longer accept native writes.
    pub fn submit_classified(
        &self,
        request: PersistRequest,
    ) -> std::result::Result<PersistTicket, PersistCompletionFailure> {
        let request_id = request.request_id;
        let table_id = request.table_id;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(PersistCompletionFailure::Fatal(
                controller_closed()
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            ));
        }
        let retained_input_bytes = u64::try_from(request.retained_input_bytes).map_err(|_| {
            PersistCompletionFailure::Rejected(
                terminal_resource("persist request retained bytes exceed u64 accounting domain")
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            )
        })?;
        if retained_input_bytes > self.max_input_bytes {
            return Err(PersistCompletionFailure::Rejected(
                terminal_resource(format!(
                    concat!(
                        "persist request exceeds retained input byte budget ",
                        "requested=[{}] maximum=[{}]"
                    ),
                    retained_input_bytes, self.max_input_bytes
                ))
                .with_request_id(request_id)
                .with_table_id(table_id),
            ));
        }
        let table_permit = TablePersistPermit::try_acquire(
            table_id,
            Arc::clone(&self.active_tables),
        )
        .map_err(|failure| {
            failure.map_error(|error| error.with_request_id(request_id).with_table_id(table_id))
        })?;
        let job_permit = Arc::clone(&self.job_budget)
            .try_acquire_owned()
            .map_err(|error| classify_admission_budget_error("outstanding_jobs", error))
            .map_err(|failure| {
                failure.map_error(|error| error.with_request_id(request_id).with_table_id(table_id))
            })?;
        let input_permits = input_request_permits(retained_input_bytes).map_err(|error| {
            PersistCompletionFailure::Rejected(
                error.with_request_id(request_id).with_table_id(table_id),
            )
        })?;
        let input_permit = Arc::clone(&self.input_budget)
            .try_acquire_many_owned(input_permits)
            .map_err(|error| classify_admission_budget_error("inflight_input_bytes", error))
            .map_err(|failure| {
                failure.map_error(|error| error.with_request_id(request_id).with_table_id(table_id))
            })?;
        let queue_permit = self
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => PersistCompletionFailure::Rejected(
                    temporary_resource("persist bounded queue has no immediately available slot"),
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    PersistCompletionFailure::Fatal(controller_closed())
                }
            })
            .map_err(|failure| {
                failure.map_error(|error| error.with_request_id(request_id).with_table_id(table_id))
            })?;
        if !self.accepting.load(Ordering::Acquire) {
            return Err(PersistCompletionFailure::Fatal(
                controller_closed()
                    .with_request_id(request_id)
                    .with_table_id(table_id),
            ));
        }
        let (response, receiver) = oneshot::channel();
        queue_permit.send(PersistJob {
            request,
            response,
            _permits: AdmissionPermits {
                _table: table_permit,
                _job: job_permit,
                _input: input_permit,
            },
        });
        Ok(PersistTicket {
            request_id,
            receiver,
        })
    }
}

/// Cancellation-safe waiter for one already accepted persist operation.
pub struct PersistTicket {
    request_id: RequestId,
    receiver: oneshot::Receiver<PersistCompletionResult>,
}

/// Classified failure returned after a persist request has been accepted.
///
/// Request rejection proves that Catalog did not commit the operation and the
/// durable intent was safely removed. Fatal failure means completion or
/// recovery safety is no longer known, so the owning runtime must stop native
/// write admission.
#[derive(Clone, Debug)]
pub enum PersistCompletionFailure {
    /// One request was definitively rejected without degrading the controller.
    Rejected(ServerError),
    /// The controller encountered a terminal execution or durability failure.
    Fatal(ServerError),
}

impl PersistCompletionFailure {
    /// Returns the structured server error carried by this classification.
    #[must_use]
    pub const fn error(&self) -> &ServerError {
        match self {
            Self::Rejected(error) | Self::Fatal(error) => error,
        }
    }

    /// Consumes the classification and returns its structured server error.
    #[must_use]
    pub fn into_error(self) -> ServerError {
        match self {
            Self::Rejected(error) | Self::Fatal(error) => error,
        }
    }

    /// Returns whether the owning persistence runtime must stop admission.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }

    fn map_error(self, map: impl FnOnce(ServerError) -> ServerError) -> Self {
        match self {
            Self::Rejected(error) => Self::Rejected(map(error)),
            Self::Fatal(error) => Self::Fatal(map(error)),
        }
    }
}

/// Completion result that preserves request-local versus terminal failure.
pub type PersistCompletionResult =
    std::result::Result<CommitDataFilesReceipt, PersistCompletionFailure>;

/// One observer for the first terminal execution failure of a persist controller.
///
/// Normal backpressure never reaches this monitor. A returned error means the
/// controller has permanently closed global admission and the owning server
/// must fail or degrade native-storage readiness before accepting more work.
pub struct PersistControllerFailureMonitor {
    receiver: watch::Receiver<Option<ServerError>>,
}

impl PersistControllerFailureMonitor {
    /// Waits until the controller reports a terminal failure or stops unexpectedly.
    pub async fn wait_for_failure(&mut self) -> ServerError {
        loop {
            if let Some(error) = self.receiver.borrow().clone() {
                return error;
            }
            if self.receiver.changed().await.is_err() {
                return ServerError::new(
                    ErrorCode::Internal,
                    "persist controller stopped without publishing a terminal outcome",
                )
                .with_retryable(false);
            }
        }
    }
}

impl PersistTicket {
    /// Returns the request identity associated with this waiter.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Waits for the durable Catalog commit receipt.
    ///
    /// Dropping this future or the ticket abandons only the response channel;
    /// writing, commit replay, and cleanup continue under controller ownership.
    pub async fn wait(self) -> Result<CommitDataFilesReceipt> {
        self.wait_classified()
            .await
            .map_err(PersistCompletionFailure::into_error)
    }

    /// Waits while preserving whether a failure is request-local or terminal.
    ///
    /// Runtime owners should keep serving after [`PersistCompletionFailure::Rejected`]
    /// and fail closed after [`PersistCompletionFailure::Fatal`].
    pub async fn wait_classified(self) -> PersistCompletionResult {
        self.receiver.await.unwrap_or_else(|_| {
            Err(PersistCompletionFailure::Fatal(
                ServerError::new(
                    ErrorCode::Unavailable,
                    "persist controller stopped after accepting the request",
                )
                .with_request_id(self.request_id),
            ))
        })
    }
}

/// Exclusive owner of native Parquet writer and Catalog commit lifecycles.
///
/// This type is intentionally not `Clone`. Only consuming [`Self::drain`]
/// joins all accepted blocking writes and drains the owned commit coordinator.
pub struct PersistController {
    sender: Option<mpsc::Sender<PersistJob>>,
    accepting: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    worker: Option<JoinHandle<Result<()>>>,
    handle: PersistControllerHandle,
    failure: watch::Receiver<Option<ServerError>>,
}

impl PersistController {
    /// Starts one bounded controller around an exclusively owned coordinator.
    ///
    /// Invalid configuration still drains the transferred coordinator before
    /// returning, so startup failure cannot skip durable-intent cleanup.
    pub async fn start(
        config: PersistControllerConfig,
        writer: ParquetCandidateWriter,
        coordinator: CatalogCommitCoordinator,
    ) -> Result<Self> {
        if let Err(error) = config.validate() {
            return Err(drain_after_startup_failure(coordinator, error).await);
        }
        if let Err(error) = Handle::try_current() {
            let error = ServerError::new(
                ErrorCode::Unavailable,
                format!("persist controller requires an active Tokio runtime error=[{error}]"),
            )
            .with_retryable(false);
            return Err(drain_after_startup_failure(coordinator, error).await);
        }
        let input_permits = match input_budget_permits(config.max_inflight_input_bytes) {
            Ok(permits) => permits,
            Err(error) => return Err(drain_after_startup_failure(coordinator, error).await),
        };
        let (sender, receiver) = mpsc::channel(config.max_outstanding_jobs);
        let accepting = Arc::new(AtomicBool::new(true));
        let job_budget = Arc::new(Semaphore::new(config.max_outstanding_jobs));
        let input_budget = Arc::new(Semaphore::new(input_permits as usize));
        let active_tables = Arc::new(Mutex::new(HashSet::new()));
        let handle = PersistControllerHandle {
            sender: sender.clone(),
            accepting: Arc::clone(&accepting),
            active_tables,
            job_budget,
            input_budget,
            max_input_bytes: config.max_inflight_input_bytes,
        };
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let (failure, failure_receiver) = watch::channel(None);
        let worker_accepting = Arc::clone(&accepting);
        let worker = tokio::spawn(run_controller(
            receiver,
            shutdown_receiver,
            Arc::new(writer),
            coordinator,
            config.max_parallel_writers,
            worker_accepting,
            failure,
        ));
        Ok(Self {
            sender: Some(sender),
            accepting,
            shutdown,
            worker: Some(worker),
            handle,
            failure: failure_receiver,
        })
    }

    /// Returns a cloneable endpoint that can only perform bounded admission.
    #[must_use]
    pub fn handle(&self) -> PersistControllerHandle {
        self.handle.clone()
    }

    /// Returns an independent observer for runtime-terminal controller failure.
    #[must_use]
    pub fn failure_monitor(&self) -> PersistControllerFailureMonitor {
        PersistControllerFailureMonitor {
            receiver: self.failure.clone(),
        }
    }

    /// Closes admission and drains every accepted job without a deadline.
    ///
    /// The controller closes the queue, joins every blocking Parquet writer,
    /// then drains the Catalog coordinator. The first execution or cleanup
    /// error is returned only after all later cleanup stages have run.
    pub async fn drain(mut self) -> Result<()> {
        self.close_admission();
        self.sender.take();
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .await
            .map_err(|error| join_error("drain persist controller", error))?
    }

    fn close_admission(&self) {
        self.accepting.store(false, Ordering::Release);
        let _ = self.shutdown.send(true);
    }
}

impl Drop for PersistController {
    fn drop(&mut self) {
        self.close_admission();
        self.sender.take();
    }
}

struct AdmissionPermits {
    _table: TablePersistPermit,
    _job: OwnedSemaphorePermit,
    _input: OwnedSemaphorePermit,
}

struct TablePersistPermit {
    table_id: TableId,
    active_tables: Arc<Mutex<HashSet<TableId>>>,
}

impl TablePersistPermit {
    fn try_acquire(
        table_id: TableId,
        active_tables: Arc<Mutex<HashSet<TableId>>>,
    ) -> std::result::Result<Self, PersistCompletionFailure> {
        let mut active = active_tables.lock().map_err(|_| {
            PersistCompletionFailure::Fatal(
                ServerError::new(
                    ErrorCode::Internal,
                    "persist table admission state is poisoned",
                )
                .with_table_id(table_id)
                .with_retryable(false),
            )
        })?;
        if !active.insert(table_id) {
            return Err(PersistCompletionFailure::Rejected(
                temporary_resource("persist table already has an accepted operation")
                    .with_table_id(table_id),
            ));
        }
        drop(active);
        Ok(Self {
            table_id,
            active_tables,
        })
    }
}

impl Drop for TablePersistPermit {
    fn drop(&mut self) {
        let mut active = self
            .active_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active.remove(&self.table_id);
    }
}

struct PersistJob {
    request: PersistRequest,
    response: oneshot::Sender<PersistCompletionResult>,
    _permits: AdmissionPermits,
}

struct WrittenPersistJob {
    request_id: RequestId,
    table_id: TableId,
    expected_snapshot_version: u64,
    candidates: Result<Vec<DurableParquetCandidate>>,
    committed_log_offset: OffsetWatermark,
    response: oneshot::Sender<PersistCompletionResult>,
    _permits: AdmissionPermits,
}

async fn run_controller(
    mut receiver: mpsc::Receiver<PersistJob>,
    mut shutdown: watch::Receiver<bool>,
    writer: Arc<ParquetCandidateWriter>,
    coordinator: CatalogCommitCoordinator,
    max_parallel_writers: usize,
    accepting: Arc<AtomicBool>,
    failure: watch::Sender<Option<ServerError>>,
) -> Result<()> {
    let mut writers = JoinSet::new();
    let mut closing = false;
    let mut channel_ended = false;
    let mut first_error = None;

    while !channel_ended || !writers.is_empty() {
        tokio::select! {
            biased;
            changed = shutdown.changed(), if !closing => {
                let _ = changed;
                closing = true;
                accepting.store(false, Ordering::Release);
                receiver.close();
            }
            job = receiver.recv(), if !channel_ended && writers.len() < max_parallel_writers => {
                match job {
                    Some(job) => {
                        let writer = Arc::clone(&writer);
                        writers.spawn_blocking(move || write_job(&writer, job));
                    }
                    None => channel_ended = true,
                }
            }
            completed = writers.join_next(), if !writers.is_empty() => {
                match completed {
                    Some(Ok(job)) => {
                        complete_written_job(
                            job,
                            &coordinator,
                            &mut first_error,
                            &accepting,
                            &mut receiver,
                            &failure,
                        ).await;
                    }
                    Some(Err(error)) => {
                        record_controller_error(
                            join_error("join Parquet candidate writer", error),
                            &mut first_error,
                            &accepting,
                            &mut receiver,
                            &failure,
                        );
                    }
                    None => {}
                }
            }
        }
    }

    if let Err(error) = coordinator.drain().await {
        publish_first_error(error, &mut first_error, &failure);
    }
    first_error.map_or(Ok(()), Err)
}

fn write_job(writer: &ParquetCandidateWriter, job: PersistJob) -> WrittenPersistJob {
    let PersistJob {
        request,
        response,
        _permits,
    } = job;
    let PersistRequest {
        request_id,
        table_id,
        expected_snapshot_version,
        schema_version: _,
        schema: _,
        writer_epoch: _,
        first_log_offset: _,
        candidates,
        committed_log_offset,
        retained_input_bytes: _,
    } = request;
    let mut durable = Vec::with_capacity(candidates.len());
    let candidates = candidates.into_iter().try_for_each(|candidate| {
        durable.push(writer.write_prepared_candidate(candidate.prepared, candidate.batches)?);
        Ok::<(), ServerError>(())
    });
    WrittenPersistJob {
        request_id,
        table_id,
        expected_snapshot_version,
        candidates: candidates.map(|()| durable),
        committed_log_offset,
        response,
        _permits,
    }
}

async fn complete_written_job(
    job: WrittenPersistJob,
    coordinator: &CatalogCommitCoordinator,
    first_error: &mut Option<ServerError>,
    accepting: &Arc<AtomicBool>,
    receiver: &mut mpsc::Receiver<PersistJob>,
    failure: &watch::Sender<Option<ServerError>>,
) {
    let WrittenPersistJob {
        request_id,
        table_id,
        expected_snapshot_version,
        candidates,
        committed_log_offset,
        response,
        _permits,
    } = job;
    let (result, fatal_error): (PersistCompletionResult, Option<ServerError>) = match candidates {
        Ok(candidates) => match CommitDataFilesIntent::new(
            request_id,
            table_id,
            expected_snapshot_version,
            candidates,
            committed_log_offset,
        ) {
            Ok(intent) => match coordinator.commit_classified(intent).await {
                Ok(receipt) => (Ok(receipt), None),
                Err(CatalogCommitFailure::DefinitiveRejection(error)) => {
                    (Err(PersistCompletionFailure::Rejected(error)), None)
                }
                Err(CatalogCommitFailure::Fatal(error)) => (
                    Err(PersistCompletionFailure::Fatal(error.clone())),
                    Some(error),
                ),
            },
            Err(error) => (
                Err(PersistCompletionFailure::Fatal(error.clone())),
                Some(error),
            ),
        },
        Err(error) => (
            Err(PersistCompletionFailure::Fatal(error.clone())),
            Some(error),
        ),
    };
    if let Some(error) = fatal_error {
        record_controller_error(error, first_error, accepting, receiver, failure);
    }
    drop(_permits);
    let _ = response.send(result);
}

fn record_controller_error(
    error: ServerError,
    first_error: &mut Option<ServerError>,
    accepting: &Arc<AtomicBool>,
    receiver: &mut mpsc::Receiver<PersistJob>,
    failure: &watch::Sender<Option<ServerError>>,
) {
    accepting.store(false, Ordering::Release);
    receiver.close();
    publish_first_error(error, first_error, failure);
}

fn publish_first_error(
    error: ServerError,
    first_error: &mut Option<ServerError>,
    failure: &watch::Sender<Option<ServerError>>,
) {
    if first_error.is_none() {
        failure.send_replace(Some(error.clone()));
    }
    record_first_error(error, first_error);
}

fn record_first_error(error: ServerError, first_error: &mut Option<ServerError>) {
    if first_error.is_none() {
        *first_error = Some(error);
    } else {
        tracing::error!(error = %error, "persist controller cleanup reported a later error");
    }
}

fn input_budget_permits(bytes: u64) -> Result<u32> {
    u32::try_from(bytes / INPUT_BUDGET_UNIT_BYTES).map_err(|_| {
        terminal_invalid(format!(
            "persist input byte budget exceeds semaphore domain actual=[{bytes}]"
        ))
    })
}

fn input_request_permits(bytes: u64) -> Result<u32> {
    let rounded = bytes
        .checked_add(INPUT_BUDGET_UNIT_BYTES - 1)
        .ok_or_else(|| terminal_resource("persist input byte accounting overflow"))?;
    u32::try_from(rounded / INPUT_BUDGET_UNIT_BYTES)
        .map_err(|_| terminal_resource("persist input bytes exceed semaphore domain"))
}

fn classify_admission_budget_error(
    resource: &str,
    error: TryAcquireError,
) -> PersistCompletionFailure {
    match error {
        TryAcquireError::NoPermits => PersistCompletionFailure::Rejected(temporary_resource(
            format!("persist {resource} budget is temporarily exhausted"),
        )),
        TryAcquireError::Closed => PersistCompletionFailure::Fatal(controller_closed()),
    }
}

fn temporary_resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(true)
}

fn controller_closed() -> ServerError {
    ServerError::new(
        ErrorCode::Unavailable,
        "persist controller is not accepting requests",
    )
    .with_retryable(true)
}

fn terminal_invalid(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn terminal_resource(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::ResourceExhausted, message).with_retryable(false)
}

fn join_error(operation: &str, error: JoinError) -> ServerError {
    ServerError::new(
        ErrorCode::Internal,
        format!("{operation} task failed error=[{error}]"),
    )
    .with_retryable(false)
}

async fn drain_after_startup_failure(
    coordinator: CatalogCommitCoordinator,
    primary: ServerError,
) -> ServerError {
    match coordinator.drain().await {
        Ok(()) => primary,
        Err(cleanup) => combine_errors(primary, cleanup),
    }
}

fn combine_errors(primary: ServerError, cleanup: ServerError) -> ServerError {
    let request_id = primary.request_id();
    let table_id = primary.table_id();
    let retryable = primary.retryable();
    let mut combined = ServerError::new(
        primary.code(),
        format!("{primary}; persist controller cleanup failed error=[{cleanup}]"),
    )
    .with_retryable(retryable);
    if let Some(request_id) = request_id {
        combined = combined.with_request_id(request_id);
    }
    if let Some(table_id) = table_id {
        combined = combined.with_table_id(table_id);
    }
    combined
}
