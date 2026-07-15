//! DataFusion integration for server-owned analytical queries.
//!
//! This crate is deliberately separate from the ingest and callback runtimes. It owns
//! per-session DataFusion policy and, in later slices, Zippy table providers and physical
//! operators. Keeping this boundary explicit prevents analytical planning or execution from
//! entering the real-time data hot path.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::future::Future;
use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::context::{SessionConfig, SessionContext};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};
use tokio::sync::Semaphore;

mod persisted_plan;

pub use persisted_plan::{
    PersistedFileDescription, PersistedFileKey, PersistedTableDescription, ZippyTableProvider,
};

const MAX_POSITIONED_READ_BYTES: usize = 1024 * 1024;

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
}

impl<R: AdmittedRandomAccessFile> AdmittedAsyncParquetReader<R> {
    /// Creates an async reader over an already admitted Parquet source.
    #[must_use]
    pub fn new(source: AdmittedParquetSource<R>) -> Self {
        Self { source }
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
}

impl QuerySessionPolicy {
    /// Creates a policy with an explicit optimizer target partition count.
    #[must_use]
    pub const fn new(target_partitions: NonZeroUsize) -> Self {
        Self { target_partitions }
    }

    /// Returns the optimizer target partition count for this session.
    #[must_use]
    pub const fn target_partitions(self) -> NonZeroUsize {
        self.target_partitions
    }

    /// Creates an isolated DataFusion session context using this policy.
    #[must_use]
    pub fn create_context(self) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(self.target_partitions.get())
            .with_repartition_file_scans(false);
        SessionContext::new_with_config(config)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use parquet::arrow::async_reader::AsyncFileReader;
    use parquet::arrow::ArrowWriter;
    use parquet::file::reader::ChunkReader;

    use super::{
        AdmittedAsyncParquetReader, AdmittedParquetSource, AdmittedRandomAccessFile,
        QuerySessionPolicy, MAX_POSITIONED_READ_BYTES,
    };

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

    #[tokio::test]
    async fn isolated_session_executes_sql_over_workspace_record_batches() {
        let policy = QuerySessionPolicy::new(NonZeroUsize::new(2).unwrap());
        let context = policy.create_context();
        assert_eq!(context.state().config().target_partitions(), 2);
        assert!(
            !context
                .state()
                .config()
                .options()
                .optimizer
                .repartition_file_scans
        );
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
}
