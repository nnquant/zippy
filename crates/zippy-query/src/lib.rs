//! DataFusion integration for server-owned analytical queries.
//!
//! This crate is deliberately separate from the ingest and callback runtimes. It owns
//! per-session DataFusion policy, capability-free persisted providers, and physical plan binding.
//! Keeping this boundary explicit prevents analytical planning or execution from entering the
//! real-time data hot path.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::future::Future;
use std::io::{self, Read};
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
pub use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::FOOTER_SIZE;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use zippy_types::{ErrorCode, ServerError};

mod active_plan;
mod output_admission;
mod persisted_plan;
mod prepared_plan;

pub use active_plan::{
    ActiveSegmentDescription, ActiveSegmentKey, ActiveSegmentOpenRequest, ActiveSegmentOpener,
    ActiveSegmentRole,
};
pub use output_admission::PersistedDecodedOutputAdmission;
pub use persisted_plan::{
    BoundPersistedPlan, PersistedFileDescription, PersistedFileKey, PersistedFileOpenRequest,
    PersistedFileOpener, PersistedSnapshotDescription, PersistedTableDescription,
    ZippyTableProvider,
};
pub use prepared_plan::{
    classify_persisted_plan_error, prepare_persisted_plan, profile_persisted_plan,
    PersistedPlanErrorClass, PersistedPlanLimits, PersistedPlanMetrics, PersistedPlanProfile,
    PersistedTableBinding, PreparedPersistedPlan,
};

/// The DataFusion error type returned while executing a bound persisted plan.
pub type PersistedExecutionError = DataFusionError;

/// One DataFusion record-batch stream bound to a server-owned persisted snapshot.
pub type PersistedRecordBatchStream = SendableRecordBatchStream;

/// Point-in-time process-wide DataFusion operator-memory counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryProcessMemoryStatus {
    /// Hard process-wide operator-memory capacity.
    pub capacity_bytes: usize,
    /// Bytes currently retained by all DataFusion operators.
    pub reserved_bytes: usize,
    /// Largest process-wide reservation observed since startup.
    pub peak_reserved_bytes: usize,
    /// Number of allocations rejected by either a query or process limit.
    pub rejected_allocations: usize,
}

/// Point-in-time counters for one query-scoped DataFusion operator-memory pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryOperatorMemoryStatus {
    /// Hard operator-memory limit for this query.
    pub limit_bytes: usize,
    /// Bytes currently retained by this query's DataFusion operators.
    pub reserved_bytes: usize,
    /// Largest operator-memory reservation observed during this query.
    pub peak_reserved_bytes: usize,
    /// Number of this query's allocations rejected by query or process limits.
    pub rejected_allocations: usize,
}

/// Process-owned DataFusion operator-memory authority shared by every native query.
///
/// Each execution receives a query-scoped child pool. A reservation must fit both the child
/// limit and this process limit before DataFusion may allocate it. Spilling is intentionally not
/// implied by this type; the current execution profile disables DataFusion's disk manager and
/// returns `RESOURCE_EXHAUSTED` when either limit is reached.
#[derive(Debug, Clone)]
pub struct QueryProcessMemoryPool {
    state: Arc<QueryProcessMemoryState>,
}

#[derive(Debug)]
struct QueryProcessMemoryState {
    capacity_bytes: usize,
    pool: GreedyMemoryPool,
    peak_reserved_bytes: AtomicUsize,
    rejected_allocations: AtomicUsize,
}

impl QueryProcessMemoryPool {
    /// Creates one process-wide operator-memory authority.
    pub fn try_new(capacity_bytes: NonZeroU64) -> zippy_types::Result<Self> {
        let capacity_bytes = usize::try_from(capacity_bytes.get()).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "native query process memory limit does not fit this platform",
            )
            .with_retryable(false)
        })?;
        Ok(Self {
            state: Arc::new(QueryProcessMemoryState {
                capacity_bytes,
                pool: GreedyMemoryPool::new(capacity_bytes),
                peak_reserved_bytes: AtomicUsize::new(0),
                rejected_allocations: AtomicUsize::new(0),
            }),
        })
    }

    /// Creates a query-local pool that also consumes this process authority.
    pub fn query_pool(
        &self,
        query_limit_bytes: NonZeroU64,
    ) -> zippy_types::Result<Arc<dyn MemoryPool>> {
        self.query_scoped_pool(query_limit_bytes)
            .map(|pool| pool as Arc<dyn MemoryPool>)
    }

    fn query_scoped_pool(
        &self,
        query_limit_bytes: NonZeroU64,
    ) -> zippy_types::Result<Arc<QueryScopedMemoryPool>> {
        let query_limit_bytes = usize::try_from(query_limit_bytes.get()).map_err(|_| {
            ServerError::new(
                ErrorCode::ResourceExhausted,
                "native query operator memory limit does not fit this platform",
            )
            .with_retryable(false)
        })?;
        if query_limit_bytes > self.state.capacity_bytes {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                "native query operator memory limit exceeds the process memory pool",
            )
            .with_retryable(false));
        }
        Ok(Arc::new(QueryScopedMemoryPool {
            process: Arc::clone(&self.state),
            limit_bytes: query_limit_bytes,
            reserved_bytes: AtomicUsize::new(0),
            peak_reserved_bytes: AtomicUsize::new(0),
            rejected_allocations: AtomicUsize::new(0),
        }))
    }

    /// Returns current process-wide allocation counters.
    #[must_use]
    pub fn status(&self) -> QueryProcessMemoryStatus {
        QueryProcessMemoryStatus {
            capacity_bytes: self.state.capacity_bytes,
            reserved_bytes: self.state.pool.reserved(),
            peak_reserved_bytes: self.state.peak_reserved_bytes.load(Ordering::Relaxed),
            rejected_allocations: self.state.rejected_allocations.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct QueryScopedMemoryPool {
    process: Arc<QueryProcessMemoryState>,
    limit_bytes: usize,
    reserved_bytes: AtomicUsize,
    peak_reserved_bytes: AtomicUsize,
    rejected_allocations: AtomicUsize,
}

impl QueryScopedMemoryPool {
    fn status(&self) -> QueryOperatorMemoryStatus {
        QueryOperatorMemoryStatus {
            limit_bytes: self.limit_bytes,
            reserved_bytes: self.reserved_bytes.load(Ordering::Relaxed),
            peak_reserved_bytes: self.peak_reserved_bytes.load(Ordering::Relaxed),
            rejected_allocations: self.rejected_allocations.load(Ordering::Relaxed),
        }
    }

    fn record_growth(&self, query_reserved: usize) {
        self.peak_reserved_bytes
            .fetch_max(query_reserved, Ordering::Relaxed);
        self.process
            .peak_reserved_bytes
            .fetch_max(self.process.pool.reserved(), Ordering::Relaxed);
    }

    fn reserve_query_bytes(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<usize> {
        self.reserved_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                reserved
                    .checked_add(additional)
                    .filter(|next| *next <= self.limit_bytes)
            })
            .map(|previous| previous + additional)
            .map_err(|reserved| {
                DataFusionError::ResourcesExhausted(format!(
                    "query operator memory exhausted consumer=[{}] requested=[{}] reserved=[{}] limit=[{}]",
                    reservation.consumer().name(),
                    additional,
                    reserved,
                    self.limit_bytes
                ))
            })
    }
}

impl MemoryPool for QueryScopedMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.process.pool.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.process.pool.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let query_reserved = self
            .reserved_bytes
            .fetch_add(additional, Ordering::Relaxed)
            .saturating_add(additional);
        self.process.pool.grow(reservation, additional);
        self.record_growth(query_reserved);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.process.pool.shrink(reservation, shrink);
        let previous = self.reserved_bytes.fetch_sub(shrink, Ordering::Relaxed);
        debug_assert!(
            previous >= shrink,
            "query memory pool reservation underflow"
        );
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::common::Result<()> {
        let query_reserved = match self.reserve_query_bytes(reservation, additional) {
            Ok(reserved) => reserved,
            Err(error) => {
                self.rejected_allocations.fetch_add(1, Ordering::Relaxed);
                self.process
                    .rejected_allocations
                    .fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if let Err(error) = self.process.pool.try_grow(reservation, additional) {
            self.reserved_bytes.fetch_sub(additional, Ordering::Relaxed);
            self.rejected_allocations.fetch_add(1, Ordering::Relaxed);
            self.process
                .rejected_allocations
                .fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.record_growth(query_reserved);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.reserved_bytes.load(Ordering::Relaxed)
    }
}

/// Stable execution-error classes used by the server protocol boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistedExecutionErrorClass {
    /// The physical operator exceeded its admitted memory or other resource limit.
    ResourceExhausted,
    /// A persisted Arrow, Parquet, object-store, or I/O input was corrupt or unreadable.
    DataCorruption,
    /// The requested operator is not implemented by the admitted native engine.
    Unsupported,
    /// The execution failed for an internal reason after planning and binding succeeded.
    Internal,
}

/// Classifies an error raised after a persisted physical plan has started executing.
#[must_use]
pub fn classify_persisted_execution_error(
    error: &PersistedExecutionError,
) -> PersistedExecutionErrorClass {
    match error {
        DataFusionError::Context(_, source) => classify_persisted_execution_error(source),
        DataFusionError::ResourcesExhausted(_) => PersistedExecutionErrorClass::ResourceExhausted,
        DataFusionError::NotImplemented(_) => PersistedExecutionErrorClass::Unsupported,
        DataFusionError::ArrowError(_, _)
        | DataFusionError::ParquetError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::IoError(_) => PersistedExecutionErrorClass::DataCorruption,
        DataFusionError::SQL(_, _)
        | DataFusionError::Plan(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Execution(_)
        | DataFusionError::Internal(_)
        | DataFusionError::ExecutionJoin(_)
        | DataFusionError::External(_)
        | DataFusionError::Substrait(_) => PersistedExecutionErrorClass::Internal,
    }
}

/// Finds a structured Zippy error preserved inside a DataFusion execution error chain.
#[must_use]
pub fn structured_persisted_execution_error(
    error: &PersistedExecutionError,
) -> Option<ServerError> {
    let mut current: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(error) = current.downcast_ref::<ServerError>() {
            return Some(error.clone());
        }
        current = current.source()?;
    }
}

/// Polls the next batch from a bound persisted query stream.
pub async fn next_persisted_batch(
    stream: &mut PersistedRecordBatchStream,
) -> Result<Option<RecordBatch>, PersistedExecutionError> {
    stream.next().await.transpose()
}

const MAX_POSITIONED_READ_BYTES: usize = 1024 * 1024;
const PARQUET_METADATA_FIXED_MEMORY_BYTES: u64 = 64 * 1024;
const PARQUET_METADATA_DECODE_EXPANSION_FACTOR: u64 = 64;

/// Exact byte length of the standard Parquet footer trailer.
pub const PARQUET_TRAILER_SIZE_BYTES: usize = FOOTER_SIZE;

/// Computes a conservative task-memory charge for decoded Parquet file metadata.
///
/// The trailer is validated before its serialized footer length is used. The returned charge is
/// a fixed decoder allowance plus a checked expansion of the serialized footer. Callers must
/// retain this charge with the [`AsyncFileReader`] for the complete lifetime of any decoded
/// [`ParquetMetaData`] derived from it.
///
/// # Errors
///
/// Returns a Parquet error when the trailer magic is invalid, the serialized footer does not fit
/// inside the verified file length, or the conservative charge calculation overflows.
pub fn parquet_metadata_memory_charge(
    file_size_bytes: u64,
    trailer: &[u8; PARQUET_TRAILER_SIZE_BYTES],
) -> ParquetResult<NonZeroU64> {
    let serialized_footer_bytes = u64::try_from(ParquetMetaDataReader::decode_footer(trailer)?)
        .map_err(|_| ParquetError::General("Parquet footer length does not fit u64".to_owned()))?;
    let trailer_bytes =
        u64::try_from(PARQUET_TRAILER_SIZE_BYTES).expect("Parquet trailer length always fits u64");
    let encoded_metadata_bytes = serialized_footer_bytes
        .checked_add(trailer_bytes)
        .ok_or_else(|| ParquetError::General("Parquet footer length overflowed".to_owned()))?;
    if encoded_metadata_bytes > file_size_bytes {
        return Err(ParquetError::EOF(format!(
            "Parquet footer exceeds verified file length footer_bytes=[{serialized_footer_bytes}] file_size_bytes=[{file_size_bytes}]"
        )));
    }

    let memory_bytes = serialized_footer_bytes
        .checked_mul(PARQUET_METADATA_DECODE_EXPANSION_FACTOR)
        .and_then(|expanded| expanded.checked_add(PARQUET_METADATA_FIXED_MEMORY_BYTES))
        .ok_or_else(|| {
            ParquetError::General("Parquet metadata memory charge overflowed".to_owned())
        })?;
    Ok(NonZeroU64::new(memory_bytes).expect("fixed Parquet metadata memory allowance is nonzero"))
}

/// A server-supplied Parquet reader whose concrete storage capability remains opaque here.
pub type BoxedAsyncFileReader = Box<dyn AsyncFileReader + Send>;

type DeferredOpen = Box<dyn FnOnce() -> zippy_types::Result<BoxedAsyncFileReader> + Send>;

enum DeferredParquetReaderState {
    Unopened(Option<DeferredOpen>),
    Opening(JoinHandle<zippy_types::Result<BoxedAsyncFileReader>>),
    Open(BoxedAsyncFileReader),
    Failed(ServerError),
}

struct DeferredParquetReader {
    state: DeferredParquetReaderState,
}

impl DeferredParquetReader {
    async fn ensure_open(&mut self) -> ParquetResult<()> {
        loop {
            match &mut self.state {
                DeferredParquetReaderState::Unopened(open) => {
                    let open = open
                        .take()
                        .expect("unopened deferred Parquet reader retains its opener");
                    self.state =
                        DeferredParquetReaderState::Opening(tokio::task::spawn_blocking(open));
                }
                DeferredParquetReaderState::Opening(opening) => {
                    let completed = (&mut *opening).await;
                    self.state = match completed {
                        Ok(Ok(reader)) => DeferredParquetReaderState::Open(reader),
                        Ok(Err(error)) => DeferredParquetReaderState::Failed(error),
                        Err(error) => DeferredParquetReaderState::Failed(
                            ServerError::new(
                                ErrorCode::Internal,
                                format!("deferred Parquet open worker failed error=[{error}]"),
                            )
                            .with_retryable(false),
                        ),
                    };
                }
                DeferredParquetReaderState::Open(_) => return Ok(()),
                DeferredParquetReaderState::Failed(error) => {
                    return Err(ParquetError::External(Box::new(error.clone())));
                }
            }
        }
    }
}

impl AsyncFileReader for DeferredParquetReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> Pin<Box<dyn Future<Output = ParquetResult<Bytes>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_open().await?;
            match &mut self.state {
                DeferredParquetReaderState::Open(reader) => reader.get_bytes(range).await,
                _ => unreachable!("successful deferred open installs an async Parquet reader"),
            }
        })
    }

    fn get_metadata(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ParquetResult<Arc<ParquetMetaData>>> + Send + '_>> {
        Box::pin(async move {
            self.ensure_open().await?;
            match &mut self.state {
                DeferredParquetReaderState::Open(reader) => reader.get_metadata().await,
                _ => unreachable!("successful deferred open installs an async Parquet reader"),
            }
        })
    }
}

/// Creates a Parquet reader whose blocking open work begins only on its first read operation.
///
/// The opener runs once on Tokio's blocking pool. If the first read future is dropped while that
/// work is active, the join handle remains inside the reader and a later call waits for the same
/// job instead of enqueueing another open. Dropping the reader detaches the job, whose captured
/// admission owners remain alive until the physical work actually exits.
pub fn deferred_parquet_reader(
    open: impl FnOnce() -> zippy_types::Result<BoxedAsyncFileReader> + Send + 'static,
) -> BoxedAsyncFileReader {
    Box::new(DeferredParquetReader {
        state: DeferredParquetReaderState::Unopened(Some(Box::new(open))),
    })
}

/// A path-free random-access file whose lifetime owns query admission and snapshot protection.
///
/// Implementations must retain the complete task reservation and storage lease for as long as
/// the value exists. Returning a detached file handle or reader violates this contract because a
/// Parquet cursor can outlive the source object that created it.
pub trait AdmittedRandomAccessFile: Send + Sync + 'static {
    /// A mutable range buffer that retains its query memory reservation until final drop.
    type RangeBuffer: AsRef<[u8]> + AsMut<[u8]> + Send + 'static;

    /// Returns the immutable, storage-verified file length.
    fn file_size_bytes(&self) -> u64;

    /// Reserves and allocates one query-accounted buffer of the exact requested length.
    ///
    /// The returned owner must keep its memory reservation through clones of any zero-copy view
    /// created from it. Allocation must happen only after admission succeeds.
    fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer>;

    /// Reads from an absolute byte offset without changing another cursor's position.
    ///
    /// Implementations should check query cancellation before each physical read and return a
    /// permanent I/O error for cancellation rather than [`io::ErrorKind::Interrupted`].
    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize>;
}

/// A Parquet [`ChunkReader`] backed by one admitted random-access query file.
///
/// Every cursor holds the same reference-counted admitted file, so dropping this outer source
/// cannot release its query task reservation or snapshot lease while physical reads remain.
pub struct AdmittedParquetSource<R: AdmittedRandomAccessFile> {
    inner: Arc<R>,
    blocking_operation_gate: Arc<Semaphore>,
}

impl<R: AdmittedRandomAccessFile> AdmittedParquetSource<R> {
    /// Wraps a random-access file that already owns query admission and snapshot protection.
    #[must_use]
    pub fn new(reader: R) -> Self {
        Self {
            inner: Arc::new(reader),
            blocking_operation_gate: Arc::new(Semaphore::new(1)),
        }
    }

    /// Returns mutable access to the admitted owner while no cursor clone is alive.
    ///
    /// This is intended for the one-time blocking open path: parse metadata under its initial
    /// reservation, compute a decoded-output envelope, then grow the same owner before publishing
    /// the source. Once any cursor or async reader clones the source, this returns `None`.
    pub fn admitted_file_mut(&mut self) -> Option<&mut R> {
        Arc::get_mut(&mut self.inner)
    }

    /// Borrows the admitted owner without exposing or cloning its storage capability.
    #[must_use]
    pub fn admitted_file(&self) -> &R {
        self.inner.as_ref()
    }

    /// Parses and retains this source's Parquet metadata on the current blocking worker.
    ///
    /// The admitted owner must already cover the metadata decoder charge. All temporary footer
    /// ranges continue to use [`AdmittedRandomAccessFile::reserve_range_buffer`].
    pub fn parse_metadata(&self) -> ParquetResult<Arc<ParquetMetaData>> {
        ParquetMetaDataReader::new()
            .parse_and_finish(self)
            .map(Arc::new)
    }
}

impl<R: AdmittedRandomAccessFile> Clone for AdmittedParquetSource<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            blocking_operation_gate: Arc::clone(&self.blocking_operation_gate),
        }
    }
}

impl<R: AdmittedRandomAccessFile> Length for AdmittedParquetSource<R> {
    fn len(&self) -> u64 {
        self.inner.file_size_bytes()
    }
}

impl<R: AdmittedRandomAccessFile> ChunkReader for AdmittedParquetSource<R> {
    type T = AdmittedParquetCursor<R>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len() {
            return Err(ParquetError::EOF(
                "Parquet cursor starts beyond admitted file length".to_owned(),
            ));
        }
        Ok(AdmittedParquetCursor {
            inner: Arc::clone(&self.inner),
            position: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let length_u64 = u64::try_from(length)
            .map_err(|_| ParquetError::EOF("Parquet range length does not fit u64".to_owned()))?;
        if start
            .checked_add(length_u64)
            .is_none_or(|end| end > self.len())
        {
            return Err(ParquetError::EOF(
                "Parquet range exceeds admitted file length".to_owned(),
            ));
        }
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut buffer = self.inner.reserve_range_buffer(length)?;
        if buffer.as_ref().len() != length || buffer.as_mut().len() != length {
            return Err(ParquetError::General(
                "admitted Parquet range reservation returned the wrong length".to_owned(),
            ));
        }
        let mut cursor = AdmittedParquetCursor {
            inner: Arc::clone(&self.inner),
            position: start,
        };
        cursor.read_exact(buffer.as_mut())?;
        Ok(Bytes::from_owner(buffer))
    }
}

/// A sequential Parquet cursor that keeps its admitted file and task lease alive.
pub struct AdmittedParquetCursor<R: AdmittedRandomAccessFile> {
    inner: Arc<R>,
    position: u64,
}

impl<R: AdmittedRandomAccessFile> Read for AdmittedParquetCursor<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.position == self.inner.file_size_bytes() {
            return Ok(0);
        }
        if self.position > self.inner.file_size_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Parquet cursor moved beyond admitted file length",
            ));
        }
        let remaining = self.inner.file_size_bytes() - self.position;
        let read_length = buffer
            .len()
            .min(MAX_POSITIONED_READ_BYTES)
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = self
            .inner
            .read_at(self.position, &mut buffer[..read_length])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "admitted Parquet file ended before its verified length",
            ));
        }
        if read > read_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "admitted Parquet reader returned more bytes than requested",
            ));
        }
        self.position = self.position.checked_add(read as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Parquet cursor offset overflowed",
            )
        })?;
        Ok(read)
    }
}

/// An asynchronous Parquet reader that delegates positioned I/O to blocking scan workers.
///
/// The wrapped source and every blocking job retain the same admitted file owner. Dropping an
/// in-flight future therefore cannot release task or snapshot ownership before its blocking I/O
/// exits. All readers cloned from one source also share a capacity-one operation gate, so detached
/// blocking work cannot exceed the file task's admitted CPU slot. Each returned [`Bytes`] retains
/// its own exact query-memory reservation. A source used by this adapter must not be scanned
/// concurrently through its synchronous [`ChunkReader`] API, which is intentionally outside the
/// async operation gate.
pub struct AdmittedAsyncParquetReader<R: AdmittedRandomAccessFile> {
    source: AdmittedParquetSource<R>,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl<R: AdmittedRandomAccessFile> AdmittedAsyncParquetReader<R> {
    /// Creates an async reader over an already admitted Parquet source.
    #[must_use]
    pub fn new(source: AdmittedParquetSource<R>) -> Self {
        Self {
            source,
            metadata: None,
        }
    }

    /// Creates an async reader with metadata parsed under the same admitted source owner.
    #[must_use]
    pub fn new_with_metadata(
        source: AdmittedParquetSource<R>,
        metadata: Arc<ParquetMetaData>,
    ) -> Self {
        Self {
            source,
            metadata: Some(metadata),
        }
    }
}

impl<R: AdmittedRandomAccessFile> AsyncFileReader for AdmittedAsyncParquetReader<R> {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> Pin<Box<dyn Future<Output = ParquetResult<Bytes>> + Send + '_>> {
        let source = self.source.clone();
        let operation_gate = Arc::clone(&source.blocking_operation_gate);
        Box::pin(async move {
            let start = u64::try_from(range.start).map_err(|_| {
                ParquetError::EOF("async Parquet range start does not fit u64".to_owned())
            })?;
            let length = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| ParquetError::EOF("async Parquet range is reversed".to_owned()))?;
            let operation_permit = operation_gate.acquire_owned().await.map_err(|_| {
                ParquetError::General("admitted Parquet operation gate is closed".to_owned())
            })?;
            tokio::task::spawn_blocking(move || {
                let _operation_permit = operation_permit;
                source.get_bytes(start, length)
            })
            .await
            .map_err(|error| ParquetError::External(Box::new(error)))?
        })
    }

    fn get_metadata(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ParquetResult<Arc<ParquetMetaData>>> + Send + '_>> {
        if let Some(metadata) = self.metadata.as_ref().map(Arc::clone) {
            return Box::pin(async move { Ok(metadata) });
        }
        let source = self.source.clone();
        let operation_gate = Arc::clone(&source.blocking_operation_gate);
        Box::pin(async move {
            let operation_permit = operation_gate.acquire_owned().await.map_err(|_| {
                ParquetError::General("admitted Parquet operation gate is closed".to_owned())
            })?;
            tokio::task::spawn_blocking(move || {
                let _operation_permit = operation_permit;
                ParquetMetaDataReader::new()
                    .parse_and_finish(&source)
                    .map(Arc::new)
            })
            .await
            .map_err(|error| ParquetError::External(Box::new(error)))?
        })
    }
}

/// Immutable policy used to create one query session context.
///
/// A context is intentionally not shared between client sessions. Catalog registration,
/// variables, and future session-level limits therefore cannot leak across owners.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuerySessionPolicy {
    target_partitions: NonZeroUsize,
    batch_size: NonZeroUsize,
}

const DEFAULT_PERSISTED_QUERY_BATCH_SIZE: usize = 8192;

impl QuerySessionPolicy {
    /// Creates a policy with an explicit optimizer target partition count.
    #[must_use]
    pub const fn new(target_partitions: NonZeroUsize) -> Self {
        Self {
            target_partitions,
            batch_size: NonZeroUsize::new(DEFAULT_PERSISTED_QUERY_BATCH_SIZE)
                .expect("default persisted query batch size is nonzero"),
        }
    }

    /// Overrides the maximum rows decoded into one Arrow batch.
    #[must_use]
    pub const fn with_batch_size(mut self, batch_size: NonZeroUsize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Returns the optimizer target partition count for this session.
    #[must_use]
    pub const fn target_partitions(self) -> NonZeroUsize {
        self.target_partitions
    }

    /// Returns the explicit maximum rows decoded into one Arrow batch.
    #[must_use]
    pub const fn batch_size(self) -> NonZeroUsize {
        self.batch_size
    }

    /// Creates an isolated DataFusion session context using this policy.
    #[must_use]
    pub fn create_context(self) -> SessionContext {
        SessionContext::new_with_config(self.session_config())
    }

    /// Creates an execution context with a hard operator-memory pool and spilling disabled.
    ///
    /// The caller must reserve the same byte count in its external resource ledger for the
    /// complete lifetime of this context. Disabling the disk manager keeps this first native
    /// execution gate fail-closed until the server owns a quota-enforced spill directory.
    ///
    /// # Errors
    ///
    /// Returns a resource error when the byte limit does not fit the current platform, or an
    /// internal error when DataFusion cannot construct the bounded runtime environment.
    pub fn create_execution_context(
        self,
        operator_memory_bytes: NonZeroU64,
    ) -> zippy_types::Result<PersistedExecutionContext> {
        let process_memory = QueryProcessMemoryPool::try_new(operator_memory_bytes)?;
        self.create_execution_context_in(operator_memory_bytes, &process_memory)
    }

    /// Creates an execution context inside a process-owned operator-memory authority.
    ///
    /// The query-scoped pool enforces `operator_memory_bytes`, while every allocation also
    /// consumes `process_memory`. The disk manager remains disabled, so an exhausted complex
    /// operator returns a resource error without creating an untracked temporary file.
    pub fn create_execution_context_in(
        self,
        operator_memory_bytes: NonZeroU64,
        process_memory: &QueryProcessMemoryPool,
    ) -> zippy_types::Result<PersistedExecutionContext> {
        let memory_pool = process_memory.query_scoped_pool(operator_memory_bytes)?;
        let runtime_memory_pool: Arc<dyn MemoryPool> = memory_pool.clone();
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(runtime_memory_pool)
            .with_disk_manager(DiskManagerConfig::Disabled)
            .build()
            .map_err(|error| {
                ServerError::new(
                    ErrorCode::Internal,
                    format!("failed to build native query runtime error=[{error}]"),
                )
                .with_retryable(false)
            })?;
        let context = SessionContext::new_with_config_rt(self.session_config(), Arc::new(runtime));
        Ok(PersistedExecutionContext {
            task_context: context.task_ctx(),
            memory_pool,
        })
    }

    fn session_config(self) -> SessionConfig {
        SessionConfig::new()
            .with_batch_size(self.batch_size.get())
            .with_coalesce_batches(false)
            .with_target_partitions(self.target_partitions.get())
            .with_repartition_file_scans(false)
            .with_parquet_page_index_pruning(false)
            .with_parquet_bloom_filter_pruning(false)
    }
}

/// Bounded DataFusion runtime context retained by one native persisted execution.
pub struct PersistedExecutionContext {
    task_context: Arc<TaskContext>,
    memory_pool: Arc<QueryScopedMemoryPool>,
}

impl PersistedExecutionContext {
    pub(crate) fn task_context(&self) -> Arc<TaskContext> {
        Arc::clone(&self.task_context)
    }

    /// Returns current and peak query-scoped operator-memory counters.
    #[must_use]
    pub fn operator_memory_status(&self) -> QueryOperatorMemoryStatus {
        self.memory_pool.status()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::DataFusionError;
    use datafusion::datasource::MemTable;
    use datafusion::execution::memory_pool::MemoryConsumer;
    use parquet::arrow::async_reader::AsyncFileReader;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::ChunkReader;

    use super::{
        classify_persisted_execution_error, deferred_parquet_reader,
        parquet_metadata_memory_charge, structured_persisted_execution_error,
        AdmittedAsyncParquetReader, AdmittedParquetSource, AdmittedRandomAccessFile,
        BoxedAsyncFileReader, PersistedDecodedOutputAdmission, PersistedExecutionErrorClass,
        QueryProcessMemoryPool, QuerySessionPolicy, MAX_POSITIONED_READ_BYTES,
        PARQUET_METADATA_DECODE_EXPANSION_FACTOR, PARQUET_METADATA_FIXED_MEMORY_BYTES,
        PARQUET_TRAILER_SIZE_BYTES,
    };
    use zippy_types::{ErrorCode, ServerError};

    struct TestAdmittedFile {
        contents: Arc<[u8]>,
        logical_length: u64,
        drops: Arc<AtomicUsize>,
    }

    impl TestAdmittedFile {
        fn new(contents: &[u8], logical_length: u64, drops: Arc<AtomicUsize>) -> Self {
            Self {
                contents: Arc::from(contents),
                logical_length,
                drops,
            }
        }
    }

    impl AdmittedRandomAccessFile for TestAdmittedFile {
        type RangeBuffer = Vec<u8>;

        fn file_size_bytes(&self) -> u64 {
            self.logical_length
        }

        fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer> {
            Ok(vec![0_u8; length])
        }

        fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
            let Ok(offset) = usize::try_from(offset) else {
                return Ok(0);
            };
            if offset >= self.contents.len() || buffer.is_empty() {
                return Ok(0);
            }
            let read = buffer.len().min(self.contents.len() - offset);
            buffer[..read].copy_from_slice(&self.contents[offset..offset + read]);
            Ok(read)
        }
    }

    impl Drop for TestAdmittedFile {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ObservedRangeFile {
        length: u64,
        reads: Arc<AtomicUsize>,
        largest_read: Arc<AtomicUsize>,
    }

    impl AdmittedRandomAccessFile for ObservedRangeFile {
        type RangeBuffer = Vec<u8>;

        fn file_size_bytes(&self) -> u64 {
            self.length
        }

        fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer> {
            Ok(vec![0_u8; length])
        }

        fn read_at(&self, _offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.largest_read.fetch_max(buffer.len(), Ordering::Relaxed);
            buffer.fill(0x5a);
            Ok(buffer.len())
        }
    }

    struct DropTrackedRangeBuffer {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for DropTrackedRangeBuffer {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl AsMut<[u8]> for DropTrackedRangeBuffer {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }

    impl Drop for DropTrackedRangeBuffer {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct FailingRangeFile {
        buffer_drops: Arc<AtomicUsize>,
    }

    impl AdmittedRandomAccessFile for FailingRangeFile {
        type RangeBuffer = DropTrackedRangeBuffer;

        fn file_size_bytes(&self) -> u64 {
            10
        }

        fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer> {
            Ok(DropTrackedRangeBuffer {
                bytes: vec![0_u8; length],
                drops: Arc::clone(&self.buffer_drops),
            })
        }

        fn read_at(&self, _offset: u64, _buffer: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }
    }

    struct BlockingRangeFile {
        contents: Arc<[u8]>,
        read_entries: tokio::sync::mpsc::UnboundedSender<usize>,
        read_count: Arc<AtomicUsize>,
        first_read_release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl AdmittedRandomAccessFile for BlockingRangeFile {
        type RangeBuffer = Vec<u8>;

        fn file_size_bytes(&self) -> u64 {
            self.contents.len() as u64
        }

        fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer> {
            Ok(vec![0_u8; length])
        }

        fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
            let read_number = self.read_count.fetch_add(1, Ordering::SeqCst);
            self.read_entries.send(read_number).map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "read observer was dropped")
            })?;
            if read_number == 0 {
                let (released, release_signal) = &*self.first_read_release;
                let mut released = released.lock().expect("lock first read release");
                while !*released {
                    released = release_signal
                        .wait(released)
                        .expect("wait for first read release");
                }
            }

            let offset = usize::try_from(offset).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read offset does not fit usize",
                )
            })?;
            if offset >= self.contents.len() || buffer.is_empty() {
                return Ok(0);
            }
            let read = buffer.len().min(self.contents.len() - offset);
            buffer[..read].copy_from_slice(&self.contents[offset..offset + read]);
            Ok(read)
        }
    }

    fn parquet_fixture() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .expect("build async Parquet fixture batch");
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema, None).expect("create Parquet fixture writer");
        writer.write(&batch).expect("write Parquet fixture batch");
        writer.close().expect("close Parquet fixture writer");
        bytes
    }

    fn parquet_trailer(serialized_footer_bytes: u32) -> [u8; PARQUET_TRAILER_SIZE_BYTES] {
        let mut trailer = [0_u8; PARQUET_TRAILER_SIZE_BYTES];
        trailer[..4].copy_from_slice(&serialized_footer_bytes.to_le_bytes());
        trailer[4..].copy_from_slice(b"PAR1");
        trailer
    }

    #[test]
    fn parquet_metadata_charge_validates_trailer_and_verified_file_bounds() {
        let serialized_footer_bytes = 1_024_u32;
        let trailer = parquet_trailer(serialized_footer_bytes);
        let charge = parquet_metadata_memory_charge(
            u64::from(serialized_footer_bytes) + PARQUET_TRAILER_SIZE_BYTES as u64,
            &trailer,
        )
        .expect("charge valid Parquet metadata");
        assert_eq!(
            charge.get(),
            PARQUET_METADATA_FIXED_MEMORY_BYTES
                + u64::from(serialized_footer_bytes) * PARQUET_METADATA_DECODE_EXPANSION_FACTOR
        );

        let mut invalid_magic = trailer;
        invalid_magic[4..].copy_from_slice(b"FAIL");
        assert!(parquet_metadata_memory_charge(u64::MAX, &invalid_magic).is_err());

        let truncated = parquet_metadata_memory_charge(
            u64::from(serialized_footer_bytes) + PARQUET_TRAILER_SIZE_BYTES as u64 - 1,
            &trailer,
        )
        .expect_err("serialized footer must fit inside the verified file");
        assert!(truncated.to_string().contains("verified file length"));
    }

    #[test]
    fn decoded_output_admission_is_fixed_width_and_row_group_bounded() {
        let bytes = parquet_fixture();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = AdmittedParquetSource::new(TestAdmittedFile::new(
            &bytes,
            bytes.len() as u64,
            Arc::clone(&drops),
        ));
        let metadata = source.parse_metadata().expect("parse admitted metadata");
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let admission = PersistedDecodedOutputAdmission::try_for_fixed_width_schema(
            NonZeroUsize::new(1024).expect("nonzero batch size"),
            &schema,
            NonZeroUsize::new(3).expect("nonzero batch copies"),
        )
        .expect("profile fixed-width decoded output");
        let charge = admission
            .file_decode_memory_bytes(&metadata)
            .expect("charge decoded row group");
        assert_eq!(admission.format_version(), 1);
        assert_eq!(admission.batch_size().get(), 1024);
        assert_eq!(admission.scan_column_count().get(), 1);
        assert!(charge > admission.batch_peak_memory_bytes());

        let variable = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
        let error = PersistedDecodedOutputAdmission::try_for_fixed_width_schema(
            NonZeroUsize::new(1024).expect("nonzero batch size"),
            &variable,
            NonZeroUsize::new(1).expect("nonzero batch copies"),
        )
        .expect_err("variable-width scan must not claim bounded output admission");
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
        drop(source);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn workspace_and_datafusion_record_batches_share_one_arrow_abi() {
        let values = Int64Array::from(vec![1_i64, 2, 3]);
        let values_ptr = values.values().as_ptr();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let workspace_batch =
            RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("build workspace batch");

        let datafusion_batch: datafusion::arrow::record_batch::RecordBatch =
            workspace_batch.clone();
        let round_trip: RecordBatch = datafusion_batch;
        let round_trip_values = round_trip
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("retain workspace Arrow array type");

        assert_eq!(round_trip_values.values().as_ptr(), values_ptr);
    }

    #[test]
    fn bounded_execution_context_enforces_memory_and_disables_temporary_files() {
        let policy = QuerySessionPolicy::new(NonZeroUsize::new(1).unwrap());
        let context = policy
            .create_execution_context(NonZeroU64::new(64).unwrap())
            .expect("create bounded execution context");
        let task = context.task_context();
        assert!(!task.runtime_env().disk_manager.tmp_files_enabled());

        let mut reservation = MemoryConsumer::new("bounded-test").register(task.memory_pool());
        reservation.try_grow(64).expect("fill bounded memory pool");
        let error = reservation
            .try_grow(1)
            .expect_err("memory pool must reject one byte beyond its limit");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        let status = context.operator_memory_status();
        assert_eq!(status.limit_bytes, 64);
        assert_eq!(status.reserved_bytes, 64);
        assert_eq!(status.peak_reserved_bytes, 64);
        assert_eq!(status.rejected_allocations, 1);
        drop(reservation);
        assert_eq!(task.memory_pool().reserved(), 0);
        assert_eq!(context.operator_memory_status().reserved_bytes, 0);
    }

    #[test]
    fn query_memory_children_share_one_process_limit_and_release_exactly() {
        let process = QueryProcessMemoryPool::try_new(NonZeroU64::new(96).unwrap())
            .expect("create process memory pool");
        let first = process
            .query_pool(NonZeroU64::new(64).unwrap())
            .expect("create first query pool");
        let second = process
            .query_pool(NonZeroU64::new(64).unwrap())
            .expect("create second query pool");

        let mut first_reservation = MemoryConsumer::new("first").register(&first);
        let mut second_reservation = MemoryConsumer::new("second").register(&second);
        first_reservation
            .try_grow(64)
            .expect("fill first query limit");
        let query_error = first_reservation
            .try_grow(1)
            .expect_err("query limit must reject independent of process capacity");
        assert!(matches!(
            query_error,
            DataFusionError::ResourcesExhausted(_)
        ));
        second_reservation
            .try_grow(32)
            .expect("fill remaining process capacity");
        let process_error = second_reservation
            .try_grow(1)
            .expect_err("shared process limit must reject another query");
        assert!(matches!(
            process_error,
            DataFusionError::ResourcesExhausted(_)
        ));

        let saturated = process.status();
        assert_eq!(saturated.capacity_bytes, 96);
        assert_eq!(saturated.reserved_bytes, 96);
        assert_eq!(saturated.peak_reserved_bytes, 96);
        assert_eq!(saturated.rejected_allocations, 2);
        drop(first_reservation);
        drop(second_reservation);
        assert_eq!(process.status().reserved_bytes, 0);
    }

    #[test]
    fn execution_errors_keep_stable_classes_and_structured_server_errors() {
        assert_eq!(
            classify_persisted_execution_error(&DataFusionError::ResourcesExhausted(
                "operator limit".to_owned()
            )),
            PersistedExecutionErrorClass::ResourceExhausted
        );
        assert_eq!(
            classify_persisted_execution_error(&DataFusionError::NotImplemented(
                "operator".to_owned()
            )),
            PersistedExecutionErrorClass::Unsupported
        );
        assert_eq!(
            classify_persisted_execution_error(&DataFusionError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupt input",
            ))),
            PersistedExecutionErrorClass::DataCorruption
        );

        let structured = ServerError::new(ErrorCode::ResourceExhausted, "reader budget");
        let wrapped = DataFusionError::Context(
            "persisted scan".to_owned(),
            Box::new(DataFusionError::External(Box::new(structured.clone()))),
        );
        assert_eq!(
            structured_persisted_execution_error(&wrapped),
            Some(structured)
        );
    }

    #[tokio::test]
    async fn isolated_session_executes_sql_over_workspace_record_batches() {
        let policy = QuerySessionPolicy::new(NonZeroUsize::new(2).unwrap());
        let context = policy.create_context();
        assert_eq!(context.state().config().target_partitions(), 2);
        assert!(!context.state().config().coalesce_batches());
        assert!(
            !context
                .state()
                .config()
                .options()
                .optimizer
                .repartition_file_scans
        );
        assert!(context.state().config().parquet_pruning());
        assert!(!context.state().config().parquet_page_index_pruning());
        assert!(!context.state().config().parquet_bloom_filter_pruning());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        context.register_table("numbers", Arc::new(table)).unwrap();

        let batches = context
            .sql("select value from numbers where value >= 2 order by value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(policy.target_partitions().get(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2, 3]
        );
    }

    #[test]
    fn parquet_cursors_retain_admission_and_have_independent_positions() {
        let drops = Arc::new(AtomicUsize::new(0));
        let source = AdmittedParquetSource::new(TestAdmittedFile::new(
            b"0123456789",
            10,
            Arc::clone(&drops),
        ));
        let mut first = source.get_read(1).expect("open first cursor");
        let mut second = source.get_read(6).expect("open second cursor");
        drop(source);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        let mut first_bytes = [0_u8; 3];
        let mut second_bytes = [0_u8; 2];
        first
            .read_exact(&mut first_bytes)
            .expect("read first range");
        second
            .read_exact(&mut second_bytes)
            .expect("read second range");
        assert_eq!(&first_bytes, b"123");
        assert_eq!(&second_bytes, b"67");

        drop(first);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(second);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parquet_ranges_fail_closed_on_bounds_overflow_and_premature_eof() {
        let source = AdmittedParquetSource::new(TestAdmittedFile::new(
            b"0123456789",
            10,
            Arc::new(AtomicUsize::new(0)),
        ));
        assert_eq!(
            source.get_bytes(2, 4).expect("read valid range").as_ref(),
            b"2345"
        );
        let mut eof = source.get_read(10).expect("cursor at logical EOF");
        assert_eq!(eof.read(&mut [0_u8; 1]).expect("read logical EOF"), 0);
        assert!(source.get_read(11).is_err());
        assert!(source.get_bytes(9, 2).is_err());
        assert!(source.get_bytes(u64::MAX, 1).is_err());

        let truncated = AdmittedParquetSource::new(TestAdmittedFile::new(
            b"short",
            10,
            Arc::new(AtomicUsize::new(0)),
        ));
        assert!(truncated.get_bytes(0, 10).is_err());

        let buffer_drops = Arc::new(AtomicUsize::new(0));
        let failed_read = AdmittedParquetSource::new(FailingRangeFile {
            buffer_drops: Arc::clone(&buffer_drops),
        });
        assert!(failed_read.get_bytes(0, 10).is_err());
        assert_eq!(buffer_drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn large_parquet_ranges_are_split_into_cancellable_positioned_reads() {
        let length = MAX_POSITIONED_READ_BYTES * 2 + 1;
        let reads = Arc::new(AtomicUsize::new(0));
        let largest_read = Arc::new(AtomicUsize::new(0));
        let source = AdmittedParquetSource::new(ObservedRangeFile {
            length: length as u64,
            reads: Arc::clone(&reads),
            largest_read: Arc::clone(&largest_read),
        });

        let bytes = source
            .get_bytes(0, length)
            .expect("read a chunked admitted Parquet range");
        assert_eq!(bytes.len(), length);
        assert_eq!(reads.load(Ordering::Relaxed), 3);
        assert_eq!(
            largest_read.load(Ordering::Relaxed),
            MAX_POSITIONED_READ_BYTES
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_parquet_reader_uses_the_admitted_source_for_ranges_and_metadata() {
        let contents = parquet_fixture();
        let drops = Arc::new(AtomicUsize::new(0));
        let source = AdmittedParquetSource::new(TestAdmittedFile::new(
            &contents,
            contents.len() as u64,
            Arc::clone(&drops),
        ));
        let mut reader = AdmittedAsyncParquetReader::new(source);

        assert_eq!(
            reader
                .get_bytes(0..4)
                .await
                .expect("read async Parquet magic")
                .as_ref(),
            b"PAR1"
        );
        let metadata = reader
            .get_metadata()
            .await
            .expect("read async Parquet metadata");
        assert_eq!(metadata.num_row_groups(), 1);
        assert_eq!(metadata.file_metadata().num_rows(), 3);
        assert_eq!(drops.load(Ordering::Relaxed), 0);

        drop(reader);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_async_future_keeps_the_shared_blocking_gate_until_io_exits() {
        let (read_entries, mut observed_reads) = tokio::sync::mpsc::unbounded_channel();
        let read_count = Arc::new(AtomicUsize::new(0));
        let first_read_release = Arc::new((Mutex::new(false), Condvar::new()));
        let source = AdmittedParquetSource::new(BlockingRangeFile {
            contents: Arc::from(&b"0123456789"[..]),
            read_entries,
            read_count: Arc::clone(&read_count),
            first_read_release: Arc::clone(&first_read_release),
        });
        let mut reader = AdmittedAsyncParquetReader::new(source);

        {
            let first_read = reader.get_bytes(0..1);
            tokio::pin!(first_read);
            tokio::select! {
                result = &mut first_read => panic!("first read unexpectedly completed: {result:?}"),
                observed = observed_reads.recv() => assert_eq!(observed, Some(0)),
            }
        }

        let second_read = reader.get_bytes(1..2);
        tokio::pin!(second_read);
        tokio::select! {
            result = &mut second_read => panic!("second read bypassed the blocking gate: {result:?}"),
            observed = observed_reads.recv() => panic!("second blocking read entered early: {observed:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert_eq!(read_count.load(Ordering::SeqCst), 1);

        {
            let (released, release_signal) = &*first_read_release;
            *released.lock().expect("lock first read release") = true;
            release_signal.notify_all();
        }

        assert_eq!(
            second_read
                .await
                .expect("second read completes after first I/O exits")
                .as_ref(),
            b"1"
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), observed_reads.recv())
                .await
                .expect("observe second blocking read"),
            Some(1)
        );
        assert_eq!(read_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_reader_opens_once_on_first_operation() {
        let contents = parquet_fixture();
        let opens = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let open_count = Arc::clone(&opens);
        let file_drops = Arc::clone(&drops);
        let mut reader = deferred_parquet_reader(move || {
            open_count.fetch_add(1, Ordering::SeqCst);
            let source = AdmittedParquetSource::new(TestAdmittedFile::new(
                &contents,
                contents.len() as u64,
                file_drops,
            ));
            Ok(Box::new(AdmittedAsyncParquetReader::new(source)) as BoxedAsyncFileReader)
        });

        assert_eq!(opens.load(Ordering::SeqCst), 0);
        assert_eq!(
            reader
                .get_bytes(0..4)
                .await
                .expect("read through deferred opener")
                .as_ref(),
            b"PAR1"
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        assert_eq!(
            reader
                .get_metadata()
                .await
                .expect("read deferred Parquet metadata")
                .file_metadata()
                .num_rows(),
            3
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        drop(reader);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropped_deferred_open_future_resumes_the_same_blocking_job() {
        let (open_entries, mut observed_opens) = tokio::sync::mpsc::unbounded_channel();
        let opens = Arc::new(AtomicUsize::new(0));
        let open_release = Arc::new((Mutex::new(false), Condvar::new()));
        let open_count = Arc::clone(&opens);
        let release = Arc::clone(&open_release);
        let mut reader = deferred_parquet_reader(move || {
            let open_number = open_count.fetch_add(1, Ordering::SeqCst);
            open_entries
                .send(open_number)
                .expect("observe deferred open entry");
            let (released, release_signal) = &*release;
            let mut released = released.lock().expect("lock deferred open release");
            while !*released {
                released = release_signal
                    .wait(released)
                    .expect("wait for deferred open release");
            }
            let source = AdmittedParquetSource::new(TestAdmittedFile::new(
                b"0123456789",
                10,
                Arc::new(AtomicUsize::new(0)),
            ));
            Ok(Box::new(AdmittedAsyncParquetReader::new(source)) as BoxedAsyncFileReader)
        });

        {
            let first = reader.get_bytes(0..1);
            tokio::pin!(first);
            tokio::select! {
                result = &mut first => panic!("deferred open unexpectedly completed: {result:?}"),
                observed = observed_opens.recv() => assert_eq!(observed, Some(0)),
            }
        }

        let second = reader.get_bytes(1..2);
        tokio::pin!(second);
        tokio::select! {
            result = &mut second => panic!("second read bypassed deferred open: {result:?}"),
            observed = observed_opens.recv() => panic!("second deferred open entered: {observed:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        {
            let (released, release_signal) = &*open_release;
            *released.lock().expect("lock deferred open release") = true;
            release_signal.notify_all();
        }
        assert_eq!(
            second
                .await
                .expect("second read resumes completed deferred open")
                .as_ref(),
            b"1"
        );
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_open_failure_is_sticky() {
        let opens = Arc::new(AtomicUsize::new(0));
        let open_count = Arc::clone(&opens);
        let mut reader = deferred_parquet_reader(move || {
            open_count.fetch_add(1, Ordering::SeqCst);
            Err(ServerError::new(
                ErrorCode::DataCorruption,
                "test deferred open failure",
            ))
        });

        for _ in 0..2 {
            let error = reader
                .get_bytes(0..1)
                .await
                .expect_err("deferred open remains failed");
            assert!(error.to_string().contains("test deferred open failure"));
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_opening_reader_retains_captured_owner_until_job_exits() {
        let (open_entries, mut observed_opens) = tokio::sync::mpsc::unbounded_channel();
        let owner_drops = Arc::new(AtomicUsize::new(0));
        let open_release = Arc::new((Mutex::new(false), Condvar::new()));
        let probe = DropProbe(Arc::clone(&owner_drops));
        let release = Arc::clone(&open_release);
        let mut reader = deferred_parquet_reader(move || {
            let _probe = probe;
            open_entries.send(()).expect("observe deferred open entry");
            let (released, release_signal) = &*release;
            let mut released = released.lock().expect("lock deferred open release");
            while !*released {
                released = release_signal
                    .wait(released)
                    .expect("wait for deferred open release");
            }
            Err(ServerError::new(
                ErrorCode::Cancelled,
                "test opening reader was dropped",
            ))
        });

        {
            let first = reader.get_bytes(0..1);
            tokio::pin!(first);
            tokio::select! {
                result = &mut first => panic!("deferred open unexpectedly completed: {result:?}"),
                observed = observed_opens.recv() => assert_eq!(observed, Some(())),
            }
        }
        drop(reader);
        assert_eq!(owner_drops.load(Ordering::SeqCst), 0);

        {
            let (released, release_signal) = &*open_release;
            *released.lock().expect("lock deferred open release") = true;
            release_signal.notify_all();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while owner_drops.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached deferred open job exits");
        assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deferred_open_worker_panic_is_sticky() {
        let opens = Arc::new(AtomicUsize::new(0));
        let open_count = Arc::clone(&opens);
        let mut reader = deferred_parquet_reader(move || {
            open_count.fetch_add(1, Ordering::SeqCst);
            panic!("test deferred open panic");
        });

        for _ in 0..2 {
            let error = reader
                .get_bytes(0..1)
                .await
                .expect_err("deferred open panic remains failed");
            assert!(error.to_string().contains("open worker failed"));
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);
    }
}
