//! Server-owned compatibility memory ingest authority.
//!
//! This is the authority-cutover layer between the Stage 6 stream-name wire contract and the
//! producer-fenced native table actors. It owns each process-local materializer exactly once and
//! can be shared by Gateway and Flight adapters. It intentionally does not claim WAL durability,
//! producer fencing, stable logical offsets, or restart recovery.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use zippy_core::{Engine, SegmentTableView, ZippyError};
use zippy_engines::{
    StreamTableDescriptorPublisher, StreamTableMaterializer, DEFAULT_STREAM_TABLE_ROW_CAPACITY,
};
use zippy_segment_store::RowSpanView;
use zippy_types::{ErrorCode, Result, ServerError};

const MAX_STREAM_NAME_BYTES: usize = 512;

/// Default maximum number of materialized stream tables retained by one runtime.
pub const DEFAULT_MEMORY_INGEST_MAX_ACTIVE_TABLES: usize = 4_096;
/// Hard ceiling for configured active compatibility stream tables.
pub const MAX_MEMORY_INGEST_ACTIVE_TABLES: usize = 1_048_576;
/// Default deadline budget for one control-plane registration or close operation.
pub const DEFAULT_MEMORY_INGEST_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Registration returned by the compatibility Catalog publisher for one materialized table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIngestRegistration {
    source_name: String,
    writer_epoch: u64,
}

impl MemoryIngestRegistration {
    /// Creates a registration after a stream and its process-owned source are visible.
    #[must_use]
    pub fn new(source_name: impl Into<String>, writer_epoch: u64) -> Self {
        Self {
            source_name: source_name.into(),
            writer_epoch,
        }
    }

    /// Returns the process-owned source name released when the table writer closes.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Returns the fencing epoch assigned by the compatibility registry.
    #[must_use]
    pub const fn writer_epoch(&self) -> u64 {
        self.writer_epoch
    }
}

/// Compatibility metadata and descriptor publication required by a memory materializer.
///
/// Implementations are composition-root adapters. Native table mutation state remains owned by
/// this crate; a legacy Master adapter may temporarily publish descriptors for old readers.
pub trait MemoryIngestCatalog: Send + Sync {
    /// Registers one stream and process-owned source, returning its writer epoch.
    fn open_table(
        &self,
        stream_name: &str,
        schema: SchemaRef,
    ) -> zippy_core::Result<MemoryIngestRegistration>;

    /// Publishes the complete active-segment descriptor envelope.
    fn publish_active_descriptor(
        &self,
        stream_name: &str,
        descriptor_envelope: &[u8],
    ) -> zippy_core::Result<()>;

    /// Releases one process-owned source before the supplied absolute deadline.
    fn close_table(
        &self,
        registration: &MemoryIngestRegistration,
        deadline: Instant,
    ) -> zippy_core::Result<()>;

    /// Releases the publisher process lease before the supplied absolute deadline.
    fn close(&self, deadline: Instant) -> zippy_core::Result<()>;
}

/// Bounded configuration for one process-local memory ingest authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryIngestConfig {
    max_active_tables: usize,
    max_rows_per_batch: Option<usize>,
    row_capacity: usize,
    operation_timeout: Duration,
}

impl Default for MemoryIngestConfig {
    fn default() -> Self {
        Self {
            max_active_tables: DEFAULT_MEMORY_INGEST_MAX_ACTIVE_TABLES,
            max_rows_per_batch: None,
            row_capacity: DEFAULT_STREAM_TABLE_ROW_CAPACITY,
            operation_timeout: DEFAULT_MEMORY_INGEST_OPERATION_TIMEOUT,
        }
    }
}

impl MemoryIngestConfig {
    /// Creates the default bounded compatibility authority configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of simultaneously retained table materializers.
    #[must_use]
    pub const fn with_max_active_tables(mut self, maximum: usize) -> Self {
        self.max_active_tables = maximum;
        self
    }

    /// Sets an optional maximum number of rows accepted in one Arrow batch.
    #[must_use]
    pub const fn with_max_rows_per_batch(mut self, maximum: Option<usize>) -> Self {
        self.max_rows_per_batch = maximum;
        self
    }

    /// Sets the active shared-memory segment row capacity.
    #[must_use]
    pub const fn with_row_capacity(mut self, row_capacity: usize) -> Self {
        self.row_capacity = row_capacity;
        self
    }

    /// Sets the bounded control-plane operation timeout.
    #[must_use]
    pub const fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Validates allocation and deadline bounds without creating a materializer.
    pub fn validate(&self) -> Result<()> {
        if self.max_active_tables == 0 || self.max_active_tables > MAX_MEMORY_INGEST_ACTIVE_TABLES {
            return Err(invalid_argument(format!(
                "memory ingest max_active_tables is outside bounds value=[{}] maximum=[{}]",
                self.max_active_tables, MAX_MEMORY_INGEST_ACTIVE_TABLES
            )));
        }
        if self.max_rows_per_batch == Some(0) {
            return Err(invalid_argument(
                "memory ingest max_rows_per_batch must be positive when configured",
            ));
        }
        if self.row_capacity == 0 {
            return Err(invalid_argument(
                "memory ingest row_capacity must be positive",
            ));
        }
        if self.operation_timeout.is_zero() {
            return Err(invalid_argument(
                "memory ingest operation timeout must be positive",
            ));
        }
        Ok(())
    }
}

/// Point-in-time bounded memory ingest counters and lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryIngestStatus {
    /// Whether new write and close operations are admitted.
    pub accepting: bool,
    /// Number of admitted operations that have not yet returned.
    pub active_operations: usize,
    /// Number of table materializers currently owned by the runtime.
    pub active_tables: usize,
    /// Configured hard limit for retained table materializers.
    pub max_active_tables: usize,
    /// Successfully acknowledged Arrow batches.
    pub write_batches_total: u64,
    /// Successfully acknowledged logical rows.
    pub written_rows_total: u64,
    /// Rejected write or close operations.
    pub rejected_operations_total: u64,
    /// Successfully created table materializers.
    pub table_creations_total: u64,
    /// Active descriptor publications, including the initial descriptor.
    pub descriptor_publications_total: u64,
    /// Total time spent waiting for a table writer lock, in microseconds.
    pub writer_lock_wait_micros_total: u64,
    /// Total materializer append time, in microseconds.
    pub append_micros_total: u64,
    /// Total materializer flush time, in microseconds.
    pub flush_micros_total: u64,
}

/// Structured result of one bounded memory ingest shutdown attempt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryIngestShutdownOutcome {
    /// Whether every operation admitted before close completed.
    pub active_operations_drained: bool,
    /// Number of writers observed when shutdown first closed admission.
    pub writers_total: usize,
    /// Number of writers fully stopped and unregistered so far.
    pub writers_closed: usize,
    /// Failures from writers that remain owned and may be retried.
    pub writer_failures: Vec<String>,
    /// Whether the compatibility publisher process lease was released.
    pub catalog_closed: bool,
    /// Whether any stage exhausted the supplied absolute deadline.
    pub deadline_exceeded: bool,
}

impl MemoryIngestShutdownOutcome {
    /// Returns whether all admitted work, writers, and publisher leases closed cleanly.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.active_operations_drained
            && self.writers_closed == self.writers_total
            && self.writer_failures.is_empty()
            && self.catalog_closed
            && !self.deadline_exceeded
    }
}

#[derive(Default)]
struct MemoryIngestMetrics {
    write_batches_total: u64,
    written_rows_total: u64,
    rejected_operations_total: u64,
    table_creations_total: u64,
    descriptor_publications_total: u64,
    writer_lock_wait_micros_total: u64,
    append_micros_total: u64,
    flush_micros_total: u64,
}

struct MemoryIngestAdmission {
    accepting: bool,
    active_operations: usize,
}

struct MemoryIngestState {
    config: MemoryIngestConfig,
    catalog: Arc<dyn MemoryIngestCatalog>,
    writers: Mutex<BTreeMap<String, MemoryTableWriterHandle>>,
    admission: Mutex<MemoryIngestAdmission>,
    drained: Condvar,
    metrics: Arc<Mutex<MemoryIngestMetrics>>,
}

struct MemoryTableWriter {
    schema: SchemaRef,
    registration: MemoryIngestRegistration,
    materializer: StreamTableMaterializer,
    materializer_stopped: bool,
    source_closed: bool,
}

impl MemoryTableWriter {
    fn close(&mut self, catalog: &dyn MemoryIngestCatalog, deadline: Instant) -> Result<()> {
        if !self.materializer_stopped {
            self.materializer
                .on_stop()
                .map_err(|error| map_core_error("stop memory ingest materializer", error))?;
            self.materializer_stopped = true;
        }
        if !self.source_closed {
            if Instant::now() >= deadline {
                return Err(deadline_exceeded(
                    "memory ingest table cleanup deadline expired before source close",
                ));
            }
            catalog
                .close_table(&self.registration, deadline)
                .map_err(|error| map_core_error("close memory ingest table source", error))?;
            self.source_closed = true;
        }
        Ok(())
    }
}

type MemoryTableWriterHandle = Arc<Mutex<MemoryTableWriter>>;

struct MemoryDescriptorPublisher {
    catalog: Arc<dyn MemoryIngestCatalog>,
    metrics: Arc<Mutex<MemoryIngestMetrics>>,
    stream_name: String,
}

impl StreamTableDescriptorPublisher for MemoryDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> zippy_core::Result<()> {
        self.catalog
            .publish_active_descriptor(&self.stream_name, &descriptor_envelope)?;
        let mut metrics = self.metrics.lock().unwrap();
        metrics.descriptor_publications_total =
            metrics.descriptor_publications_total.saturating_add(1);
        Ok(())
    }
}

struct MemoryIngestOperationPermit {
    state: Arc<MemoryIngestState>,
}

impl Drop for MemoryIngestOperationPermit {
    fn drop(&mut self) {
        let mut admission = self.state.admission.lock().unwrap();
        admission.active_operations = admission.active_operations.saturating_sub(1);
        if admission.active_operations == 0 {
            self.state.drained.notify_all();
        }
    }
}

/// Cloneable process-local append and snapshot surface shared by protocol adapters.
#[derive(Clone)]
pub struct MemoryIngestHandle {
    state: Arc<MemoryIngestState>,
}

impl MemoryIngestHandle {
    /// Applies and flushes one Arrow batch through the unique table materializer.
    ///
    /// The acknowledgement is process-local memory visibility only.
    pub fn write_batch(&self, stream_name: &str, batch: RecordBatch) -> Result<()> {
        let result = self.write_batch_inner(stream_name, batch);
        if result.is_err() {
            let mut metrics = self.state.metrics.lock().unwrap();
            metrics.rejected_operations_total = metrics.rejected_operations_total.saturating_add(1);
        }
        result
    }

    fn write_batch_inner(&self, stream_name: &str, batch: RecordBatch) -> Result<()> {
        let _permit = self.admit()?;
        validate_stream_name(stream_name)?;
        let row_count = batch.num_rows();
        if self
            .state
            .config
            .max_rows_per_batch
            .is_some_and(|maximum| row_count > maximum)
        {
            return Err(ServerError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "memory ingest batch row count exceeds limit rows=[{row_count}] maximum=[{}]",
                    self.state.config.max_rows_per_batch.unwrap_or_default()
                ),
            )
            .with_retryable(false));
        }

        let writer = self.writer_handle(stream_name, batch.schema())?;
        let lock_started = Instant::now();
        let mut writer = writer.lock().unwrap();
        let lock_wait = lock_started.elapsed();
        if writer.schema.as_ref() != batch.schema().as_ref() {
            return Err(ServerError::new(
                ErrorCode::SchemaMismatch,
                "memory ingest batch schema differs from the existing table writer",
            )
            .with_retryable(false));
        }
        let append_started = Instant::now();
        writer
            .materializer
            .on_data(SegmentTableView::from_record_batch(batch))
            .map_err(|error| map_core_error("append memory ingest batch", error))?;
        let append_elapsed = append_started.elapsed();
        let flush_started = Instant::now();
        writer
            .materializer
            .on_flush()
            .map_err(|error| map_core_error("flush memory ingest batch", error))?;
        let flush_elapsed = flush_started.elapsed();
        drop(writer);

        let mut metrics = self.state.metrics.lock().unwrap();
        metrics.write_batches_total = metrics.write_batches_total.saturating_add(1);
        metrics.written_rows_total = metrics
            .written_rows_total
            .saturating_add(u64::try_from(row_count).unwrap_or(u64::MAX));
        metrics.writer_lock_wait_micros_total = metrics
            .writer_lock_wait_micros_total
            .saturating_add(duration_micros(lock_wait));
        metrics.append_micros_total = metrics
            .append_micros_total
            .saturating_add(duration_micros(append_elapsed));
        metrics.flush_micros_total = metrics
            .flush_micros_total
            .saturating_add(duration_micros(flush_elapsed));
        Ok(())
    }

    /// Closes and unregisters one table writer while retaining the runtime authority.
    pub fn close_writer(&self, stream_name: &str) -> Result<()> {
        let result = self.close_writer_inner(stream_name);
        if result.is_err() {
            let mut metrics = self.state.metrics.lock().unwrap();
            metrics.rejected_operations_total = metrics.rejected_operations_total.saturating_add(1);
        }
        result
    }

    fn close_writer_inner(&self, stream_name: &str) -> Result<()> {
        let _permit = self.admit()?;
        validate_stream_name(stream_name)?;
        let deadline = Instant::now()
            .checked_add(self.state.config.operation_timeout)
            .ok_or_else(|| invalid_argument("memory ingest close deadline overflow"))?;
        let mut writers = self.state.writers.lock().unwrap();
        let Some(writer) = writers.get(stream_name).cloned() else {
            return Ok(());
        };
        let close_result = writer
            .lock()
            .unwrap()
            .close(self.state.catalog.as_ref(), deadline);
        if close_result.is_ok() {
            writers.remove(stream_name);
        }
        close_result
    }

    /// Returns a pinned active row span for compatibility readers when the writer exists.
    pub fn active_row_span(&self, stream_name: &str) -> Result<Option<RowSpanView>> {
        let _permit = self.admit()?;
        validate_stream_name(stream_name)?;
        let writer = self.state.writers.lock().unwrap().get(stream_name).cloned();
        writer
            .map(|writer| {
                writer
                    .lock()
                    .unwrap()
                    .materializer
                    .active_row_span()
                    .map_err(|error| map_core_error("snapshot memory ingest active rows", error))
            })
            .transpose()
    }

    /// Returns a debug Arrow snapshot for compatibility collect paths when the writer exists.
    pub fn active_record_batch(&self, stream_name: &str) -> Result<Option<RecordBatch>> {
        let _permit = self.admit()?;
        validate_stream_name(stream_name)?;
        let writer = self.state.writers.lock().unwrap().get(stream_name).cloned();
        writer
            .map(|writer| {
                writer
                    .lock()
                    .unwrap()
                    .materializer
                    .active_record_batch()
                    .map_err(|error| map_core_error("snapshot memory ingest record batch", error))
            })
            .transpose()
    }

    /// Returns current lifecycle and bounded performance counters.
    #[must_use]
    pub fn status(&self) -> MemoryIngestStatus {
        self.state.status()
    }

    fn admit(&self) -> Result<MemoryIngestOperationPermit> {
        let mut admission = self.state.admission.lock().unwrap();
        if !admission.accepting {
            return Err(ServerError::new(
                ErrorCode::Unavailable,
                "memory ingest admission is closed",
            )
            .with_retryable(true));
        }
        admission.active_operations =
            admission.active_operations.checked_add(1).ok_or_else(|| {
                ServerError::new(
                    ErrorCode::ResourceExhausted,
                    "memory ingest active operation count exhausted usize",
                )
                .with_retryable(true)
            })?;
        drop(admission);
        Ok(MemoryIngestOperationPermit {
            state: Arc::clone(&self.state),
        })
    }

    fn writer_handle(
        &self,
        stream_name: &str,
        schema: SchemaRef,
    ) -> Result<MemoryTableWriterHandle> {
        let mut writers = self.state.writers.lock().unwrap();
        if let Some(writer) = writers.get(stream_name) {
            return Ok(Arc::clone(writer));
        }
        if writers.len() >= self.state.config.max_active_tables {
            return Err(ServerError::new(
                ErrorCode::ResourceExhausted,
                format!(
                    "memory ingest active table limit exhausted maximum=[{}]",
                    self.state.config.max_active_tables
                ),
            )
            .with_retryable(true));
        }

        let registration = self
            .state
            .catalog
            .open_table(stream_name, Arc::clone(&schema))
            .map_err(|error| map_core_error("register memory ingest table", error))?;
        let publisher = Arc::new(MemoryDescriptorPublisher {
            catalog: Arc::clone(&self.state.catalog),
            metrics: Arc::clone(&self.state.metrics),
            stream_name: stream_name.to_owned(),
        });
        let materializer_result = StreamTableMaterializer::new_with_row_capacity_and_writer_epoch(
            stream_name,
            Arc::clone(&schema),
            self.state.config.row_capacity,
            Some(registration.writer_epoch()),
        )
        .map(|materializer| materializer.with_descriptor_publisher(publisher));
        let mut materializer = match materializer_result {
            Ok(materializer) => materializer,
            Err(error) => {
                let _ = self.close_failed_registration(&registration);
                return Err(map_core_error(
                    "create memory ingest table materializer",
                    error,
                ));
            }
        };
        let descriptor = match materializer.active_descriptor_envelope_bytes() {
            Ok(descriptor) => descriptor,
            Err(error) => {
                let _ = materializer.on_stop();
                let _ = self.close_failed_registration(&registration);
                return Err(map_core_error(
                    "encode memory ingest initial descriptor",
                    error,
                ));
            }
        };
        if let Err(error) = self
            .state
            .catalog
            .publish_active_descriptor(stream_name, &descriptor)
        {
            let _ = materializer.on_stop();
            let _ = self.close_failed_registration(&registration);
            return Err(map_core_error(
                "publish memory ingest initial descriptor",
                error,
            ));
        }
        {
            let mut metrics = self.state.metrics.lock().unwrap();
            metrics.table_creations_total = metrics.table_creations_total.saturating_add(1);
            metrics.descriptor_publications_total =
                metrics.descriptor_publications_total.saturating_add(1);
        }
        let writer = Arc::new(Mutex::new(MemoryTableWriter {
            schema,
            registration,
            materializer,
            materializer_stopped: false,
            source_closed: false,
        }));
        writers.insert(stream_name.to_owned(), Arc::clone(&writer));
        Ok(writer)
    }

    fn close_failed_registration(
        &self,
        registration: &MemoryIngestRegistration,
    ) -> zippy_core::Result<()> {
        let deadline = Instant::now()
            .checked_add(self.state.config.operation_timeout)
            .unwrap_or_else(Instant::now);
        self.state.catalog.close_table(registration, deadline)
    }
}

impl MemoryIngestState {
    fn close_admission(&self) {
        self.admission.lock().unwrap().accepting = false;
    }

    fn status(&self) -> MemoryIngestStatus {
        let admission = self.admission.lock().unwrap();
        let accepting = admission.accepting;
        let active_operations = admission.active_operations;
        drop(admission);
        let active_tables = self.writers.lock().unwrap().len();
        let metrics = self.metrics.lock().unwrap();
        MemoryIngestStatus {
            accepting,
            active_operations,
            active_tables,
            max_active_tables: self.config.max_active_tables,
            write_batches_total: metrics.write_batches_total,
            written_rows_total: metrics.written_rows_total,
            rejected_operations_total: metrics.rejected_operations_total,
            table_creations_total: metrics.table_creations_total,
            descriptor_publications_total: metrics.descriptor_publications_total,
            writer_lock_wait_micros_total: metrics.writer_lock_wait_micros_total,
            append_micros_total: metrics.append_micros_total,
            flush_micros_total: metrics.flush_micros_total,
        }
    }

    fn wait_for_operations(&self, deadline: Instant) -> bool {
        let mut admission = self.admission.lock().unwrap();
        while admission.active_operations != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timeout) = self.drained.wait_timeout(admission, remaining).unwrap();
            admission = next;
            if timeout.timed_out() && admission.active_operations != 0 {
                return false;
            }
        }
        true
    }
}

/// Exclusive lifecycle owner for the shared compatibility memory ingest authority.
pub struct MemoryIngestRuntime {
    state: Arc<MemoryIngestState>,
    shutdown_writers_total: Option<usize>,
    writers_closed: usize,
    catalog_closed: bool,
}

impl MemoryIngestRuntime {
    /// Creates an authority over one already-open compatibility Catalog publisher.
    pub fn start(
        config: MemoryIngestConfig,
        catalog: Arc<dyn MemoryIngestCatalog>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            state: Arc::new(MemoryIngestState {
                config,
                catalog,
                writers: Mutex::new(BTreeMap::new()),
                admission: Mutex::new(MemoryIngestAdmission {
                    accepting: true,
                    active_operations: 0,
                }),
                drained: Condvar::new(),
                metrics: Arc::new(Mutex::new(MemoryIngestMetrics::default())),
            }),
            shutdown_writers_total: None,
            writers_closed: 0,
            catalog_closed: false,
        })
    }

    /// Returns the cloneable data-plane surface used by Gateway and Flight adapters.
    #[must_use]
    pub fn handle(&self) -> MemoryIngestHandle {
        MemoryIngestHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// Returns current lifecycle and bounded performance counters.
    #[must_use]
    pub fn status(&self) -> MemoryIngestStatus {
        self.state.status()
    }

    /// Irreversibly closes new operation admission without blocking.
    pub fn close_admission(&self) {
        self.state.close_admission();
    }

    /// Drains admitted operations, table writers, and the publisher lease by one deadline.
    pub fn shutdown_until(&mut self, deadline: Instant) -> MemoryIngestShutdownOutcome {
        self.close_admission();
        let writers_total = *self
            .shutdown_writers_total
            .get_or_insert_with(|| self.state.writers.lock().unwrap().len());
        if !self.state.wait_for_operations(deadline) {
            return MemoryIngestShutdownOutcome {
                active_operations_drained: false,
                writers_total,
                writers_closed: self.writers_closed,
                deadline_exceeded: true,
                ..MemoryIngestShutdownOutcome::default()
            };
        }

        let mut failures = Vec::new();
        loop {
            if Instant::now() >= deadline {
                return MemoryIngestShutdownOutcome {
                    active_operations_drained: true,
                    writers_total,
                    writers_closed: self.writers_closed,
                    writer_failures: failures,
                    catalog_closed: self.catalog_closed,
                    deadline_exceeded: true,
                };
            }
            let next = self
                .state
                .writers
                .lock()
                .unwrap()
                .iter()
                .next()
                .map(|(name, writer)| (name.clone(), Arc::clone(writer)));
            let Some((stream_name, writer)) = next else {
                break;
            };
            let close_result = writer
                .lock()
                .unwrap()
                .close(self.state.catalog.as_ref(), deadline);
            match close_result {
                Ok(()) => {
                    self.state.writers.lock().unwrap().remove(&stream_name);
                    self.writers_closed = self.writers_closed.saturating_add(1);
                }
                Err(error) => {
                    failures.push(format!(
                        "memory ingest writer close failed stream_name=[{stream_name}] error=[{error}]"
                    ));
                    break;
                }
            }
        }

        if failures.is_empty() && !self.catalog_closed {
            if Instant::now() >= deadline {
                return MemoryIngestShutdownOutcome {
                    active_operations_drained: true,
                    writers_total,
                    writers_closed: self.writers_closed,
                    catalog_closed: false,
                    deadline_exceeded: true,
                    ..MemoryIngestShutdownOutcome::default()
                };
            }
            match self.state.catalog.close(deadline) {
                Ok(()) => self.catalog_closed = true,
                Err(error) => failures.push(format!(
                    "memory ingest catalog close failed error=[{error}]"
                )),
            }
        }

        MemoryIngestShutdownOutcome {
            active_operations_drained: true,
            writers_total,
            writers_closed: self.writers_closed,
            writer_failures: failures,
            catalog_closed: self.catalog_closed,
            deadline_exceeded: false,
        }
    }
}

impl Drop for MemoryIngestRuntime {
    fn drop(&mut self) {
        self.close_admission();
    }
}

fn validate_stream_name(stream_name: &str) -> Result<()> {
    if stream_name.trim().is_empty() || stream_name.len() > MAX_STREAM_NAME_BYTES {
        return Err(invalid_argument(
            "memory ingest stream name is empty or too large",
        ));
    }
    Ok(())
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn map_core_error(stage: &str, error: ZippyError) -> ServerError {
    let code = match &error {
        ZippyError::InvalidConfig { .. } => ErrorCode::InvalidArgument,
        ZippyError::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
        ZippyError::NotFound { .. } => ErrorCode::NotFound,
        ZippyError::Conflict { .. } => ErrorCode::Conflict,
        ZippyError::DataCorruption { .. } => ErrorCode::DataCorruption,
        ZippyError::SchemaMismatch { .. } => ErrorCode::SchemaMismatch,
        ZippyError::Cancelled { .. } => ErrorCode::Cancelled,
        ZippyError::DeadlineExceeded { .. } => ErrorCode::DeadlineExceeded,
        ZippyError::MasterShutdownRequested { .. }
        | ZippyError::InvalidState { .. }
        | ZippyError::Io { .. }
        | ZippyError::ChannelSend
        | ZippyError::ChannelReceive
        | ZippyError::QueueClosed => ErrorCode::Unavailable,
        ZippyError::LateData { .. } => ErrorCode::Internal,
    };
    ServerError::new(code, format!("{stage}: {error}")).with_retryable(matches!(
        code,
        ErrorCode::ResourceExhausted | ErrorCode::Unavailable | ErrorCode::DeadlineExceeded
    ))
}

fn invalid_argument(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::InvalidArgument, message).with_retryable(false)
}

fn deadline_exceeded(message: impl Into<String>) -> ServerError {
    ServerError::new(ErrorCode::DeadlineExceeded, message).with_retryable(true)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use arrow::array::{ArrayRef, Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[derive(Default)]
    struct TestCatalog {
        opens: AtomicU64,
        publications: AtomicU64,
        closes: AtomicU64,
        closed: AtomicBool,
    }

    impl MemoryIngestCatalog for TestCatalog {
        fn open_table(
            &self,
            stream_name: &str,
            _schema: SchemaRef,
        ) -> zippy_core::Result<MemoryIngestRegistration> {
            self.opens.fetch_add(1, Ordering::Relaxed);
            Ok(MemoryIngestRegistration::new(
                format!("test.{stream_name}"),
                7,
            ))
        }

        fn publish_active_descriptor(
            &self,
            _stream_name: &str,
            _descriptor_envelope: &[u8],
        ) -> zippy_core::Result<()> {
            self.publications.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn close_table(
            &self,
            _registration: &MemoryIngestRegistration,
            _deadline: Instant,
        ) -> zippy_core::Result<()> {
            self.closes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn close(&self, _deadline: Instant) -> zippy_core::Result<()> {
            self.closed.store(true, Ordering::Release);
            Ok(())
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        let values: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema(), vec![values]).unwrap()
    }

    #[test]
    fn shared_handle_owns_one_writer_and_returns_active_rows() {
        let catalog = Arc::new(TestCatalog::default());
        let mut runtime = MemoryIngestRuntime::start(
            MemoryIngestConfig::new().with_row_capacity(8),
            catalog.clone(),
        )
        .unwrap();
        let first = runtime.handle();
        let second = runtime.handle();

        first.write_batch("ticks", batch(vec![1, 2])).unwrap();
        second.write_batch("ticks", batch(vec![3])).unwrap();

        let active = first.active_record_batch("ticks").unwrap().unwrap();
        assert_eq!(active.num_rows(), 3);
        assert_eq!(catalog.opens.load(Ordering::Relaxed), 1);
        let status = runtime.status();
        assert_eq!(status.active_tables, 1);
        assert_eq!(status.write_batches_total, 2);
        assert_eq!(status.written_rows_total, 3);
        assert_eq!(status.table_creations_total, 1);
        assert!(status.descriptor_publications_total >= 1);

        let outcome = runtime.shutdown_until(Instant::now() + Duration::from_secs(1));
        assert!(outcome.is_clean(), "outcome: {outcome:?}");
        assert_eq!(catalog.closes.load(Ordering::Relaxed), 1);
        assert!(catalog.closed.load(Ordering::Acquire));
    }

    #[test]
    fn table_limit_schema_mismatch_and_closed_admission_fail_explicitly() {
        let catalog = Arc::new(TestCatalog::default());
        let mut runtime = MemoryIngestRuntime::start(
            MemoryIngestConfig::new()
                .with_max_active_tables(1)
                .with_row_capacity(8),
            catalog,
        )
        .unwrap();
        let handle = runtime.handle();
        handle.write_batch("ticks", batch(vec![1])).unwrap();

        let limit = handle.write_batch("quotes", batch(vec![2])).unwrap_err();
        assert_eq!(limit.code(), ErrorCode::ResourceExhausted);

        let other_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )]));
        let values: ArrayRef = Arc::new(UInt64Array::from(vec![3_u64]));
        let other = RecordBatch::try_new(other_schema, vec![values]).unwrap();
        let mismatch = handle.write_batch("ticks", other).unwrap_err();
        assert_eq!(mismatch.code(), ErrorCode::SchemaMismatch);

        runtime.close_admission();
        let closed = handle.write_batch("ticks", batch(vec![4])).unwrap_err();
        assert_eq!(closed.code(), ErrorCode::Unavailable);
        let outcome = runtime.shutdown_until(Instant::now() + Duration::from_secs(1));
        assert!(outcome.is_clean(), "outcome: {outcome:?}");
    }

    #[test]
    fn close_writer_releases_registration_and_allows_reopen() {
        let catalog = Arc::new(TestCatalog::default());
        let mut runtime = MemoryIngestRuntime::start(
            MemoryIngestConfig::new().with_row_capacity(8),
            catalog.clone(),
        )
        .unwrap();
        let handle = runtime.handle();
        handle.write_batch("ticks", batch(vec![1])).unwrap();
        handle.close_writer("ticks").unwrap();
        assert_eq!(catalog.closes.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.status().active_tables, 0);

        handle.write_batch("ticks", batch(vec![2])).unwrap();
        assert_eq!(catalog.opens.load(Ordering::Relaxed), 2);
        assert!(runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .is_clean());
    }
}
