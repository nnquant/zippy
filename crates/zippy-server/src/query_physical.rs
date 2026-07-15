//! Permit-owned physical readers for the staged native query executor.

use std::io;
use std::num::NonZeroU64;

use zippy_catalog::{PinnedDataFile, PinnedDataFileReader};
use zippy_query::{AdmittedParquetSource, AdmittedRandomAccessFile};
use zippy_types::Result;

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
        let task = task.try_map_resource(|file| file.open_reader())?;
        Ok(AdmittedParquetSource::new(Self { task }))
    }
}

impl AdmittedRandomAccessFile for AdmittedPinnedParquetFile {
    type RangeBuffer = QueryResultPermit<Vec<u8>>;

    fn file_size_bytes(&self) -> u64 {
        self.task.resource().file_size_bytes()
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
        self.task.resource().read_at(offset, buffer)
    }
}
