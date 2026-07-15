//! Permit-owned physical readers for the staged native query executor.

use std::io;
use std::num::NonZeroU64;

use zippy_catalog::{PinnedDataFile, PinnedDataFileReader};
use zippy_query::{
    deferred_parquet_reader, parquet_metadata_memory_charge, AdmittedAsyncParquetReader,
    AdmittedParquetSource, AdmittedRandomAccessFile, BoxedAsyncFileReader,
    PARQUET_TRAILER_SIZE_BYTES,
};
use zippy_types::{ErrorCode, Result, ServerError};

use crate::query_service::{QueryResultCharge, QueryResultPermit, QueryTaskPermit};

/// One opened Parquet file whose complete query task reservation remains attached.
pub(crate) struct AdmittedPinnedParquetFile {
    task: QueryTaskPermit<PinnedDataFileReader>,
}

impl AdmittedPinnedParquetFile {
    /// Opens an already-admitted pinned file and preserves the original task reservation.
    ///
    /// This performs synchronous descriptor and optional legacy checksum I/O. Callers must invoke
    /// it on a bounded blocking scan worker, not a Tokio runtime worker.
    pub(crate) fn open(
        task: QueryTaskPermit<PinnedDataFile>,
    ) -> Result<AdmittedParquetSource<Self>> {
        let mut task = task.open_pinned_file()?;
        let metadata_memory_bytes = inspect_parquet_metadata_memory(&task)?;
        task.grow_memory_to(metadata_memory_bytes)?;
        Ok(AdmittedParquetSource::new(Self { task }))
    }

    /// Returns a lazy async reader that opens this admitted file on first physical read.
    ///
    /// The task permit moves into the blocking opener closure. Dropping an in-flight open future
    /// or the outer reader therefore cannot release task, memory, or snapshot ownership before
    /// synchronous descriptor and checksum work actually exits.
    pub(crate) fn deferred(task: QueryTaskPermit<PinnedDataFile>) -> BoxedAsyncFileReader {
        deferred_parquet_reader(move || {
            Self::open(task).map(|source| {
                Box::new(AdmittedAsyncParquetReader::new(source)) as BoxedAsyncFileReader
            })
        })
    }
}

fn inspect_parquet_metadata_memory(
    task: &QueryTaskPermit<PinnedDataFileReader>,
) -> Result<NonZeroU64> {
    let query_id = task.query_id();
    let reader = task.pinned_file_reader();
    let table_id = reader.table_id();
    let file_id = reader.file_id();
    let file_size_bytes = reader.file_size_bytes();
    let trailer_size =
        u64::try_from(PARQUET_TRAILER_SIZE_BYTES).expect("Parquet trailer length always fits u64");
    if file_size_bytes < trailer_size {
        return Err(ServerError::new(
            ErrorCode::DataCorruption,
            format!(
                "Parquet file is shorter than its trailer file_id=[{file_id}] file_size_bytes=[{file_size_bytes}]"
            ),
        )
        .with_query_id(query_id)
        .with_table_id(table_id)
        .with_retryable(false));
    }

    let mut trailer = [0_u8; PARQUET_TRAILER_SIZE_BYTES];
    reader
        .read_exact_at(file_size_bytes - trailer_size, &mut trailer)
        .map_err(|error| {
            let code = if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
            ) {
                ErrorCode::DataCorruption
            } else {
                ErrorCode::Unavailable
            };
            ServerError::new(
                code,
                format!("read Parquet trailer failed file_id=[{file_id}] error=[{error}]"),
            )
            .with_query_id(query_id)
            .with_table_id(table_id)
        })?;

    parquet_metadata_memory_charge(file_size_bytes, &trailer).map_err(|error| {
        ServerError::new(
            ErrorCode::DataCorruption,
            format!("invalid Parquet trailer file_id=[{file_id}] error=[{error}]"),
        )
        .with_query_id(query_id)
        .with_table_id(table_id)
        .with_retryable(false)
    })
}

impl AdmittedRandomAccessFile for AdmittedPinnedParquetFile {
    type RangeBuffer = QueryResultPermit<Vec<u8>>;

    fn file_size_bytes(&self) -> u64 {
        self.task.pinned_file_reader().file_size_bytes()
    }

    fn reserve_range_buffer(&self, length: usize) -> io::Result<Self::RangeBuffer> {
        let memory_bytes = u64::try_from(length)
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native query range buffer length is invalid",
                )
            })?;
        let mut buffer = self
            .task
            .admit_result(QueryResultCharge { memory_bytes }, Vec::new())
            .map_err(io::Error::other)?;
        buffer
            .resource_mut()
            .try_reserve_exact(length)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "unable to allocate admitted native query range buffer",
                )
            })?;
        buffer.resource_mut().resize(length, 0);
        Ok(buffer)
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        if self.task.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "native query file task was cancelled",
            ));
        }
        self.task.pinned_file_reader().read_at(offset, buffer)
    }
}
