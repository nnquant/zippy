use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use arrow::record_batch::RecordBatch;

use crate::{
    compile_schema, debug::active_segment_record_batch, notify::SegmentBroadcaster,
    ActiveSegmentWriter, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan, PersistenceQueue,
    PersistenceRetryPolicy, PersistenceWorker, ReaderSession, RowSpanView, SealedSegmentHandle,
    SegmentNotifier, ShmRegion, ZippySegmentStoreError,
};

/// Segment store 的最小配置。
#[derive(Debug, Clone)]
pub struct SegmentStoreConfig {
    /// 默认 row 容量。
    pub default_row_capacity: usize,
}

impl SegmentStoreConfig {
    /// 返回测试配置。
    pub fn for_test() -> Self {
        Self {
            default_row_capacity: 32,
        }
    }
}

/// Segment store owner。
#[derive(Debug, Clone)]
pub struct SegmentStore {
    pub(crate) store_id: u64,
    pub(crate) config: SegmentStoreConfig,
    pub(crate) partitions: Arc<Mutex<HashMap<(String, String), PartitionHandle>>>,
    pub(crate) persistence: PersistenceQueue,
    pub(crate) sessions: Arc<Mutex<HashMap<u64, ReaderSessionState>>>,
    pub(crate) pins: Arc<Mutex<HashMap<LeaseKey, usize>>>,
    pub(crate) next_session_id: Arc<AtomicU64>,
}

impl SegmentStore {
    /// 创建 store。
    pub fn new(config: SegmentStoreConfig) -> Result<Self, ZippySegmentStoreError> {
        Ok(Self {
            store_id: next_store_id(),
            config,
            partitions: Arc::new(Mutex::new(HashMap::new())),
            persistence: PersistenceQueue::new(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            pins: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// 打开或创建分区。
    pub fn open_partition(
        &self,
        stream: &str,
        partition: &str,
    ) -> Result<PartitionHandle, ZippySegmentStoreError> {
        self.open_partition_with_schema(stream, partition, test_schema()?)
    }

    /// 按指定 schema 打开或创建分区。
    pub fn open_partition_with_schema(
        &self,
        stream: &str,
        partition: &str,
        schema: CompiledSchema,
    ) -> Result<PartitionHandle, ZippySegmentStoreError> {
        let mut partitions = self.partitions.lock().unwrap();
        let key = (stream.to_string(), partition.to_string());
        if let Some(handle) = partitions.get(&key) {
            if handle.inner.schema.schema_id() != schema.schema_id() {
                return Err(ZippySegmentStoreError::Schema("partition schema mismatch"));
            }
            return Ok(handle.clone());
        }

        let handle = PartitionHandle::new(
            self.store_id,
            stream,
            partition,
            schema,
            self.config.default_row_capacity,
            self.persistence.clone(),
        )?;
        partitions.insert(key, handle.clone());
        Ok(handle)
    }

    /// 打开 reader session。
    pub fn open_session(&self, name: &str) -> Result<ReaderSession, ZippySegmentStoreError> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        self.sessions
            .lock()
            .unwrap()
            .insert(session_id, ReaderSessionState::new(name.to_string()));
        Ok(ReaderSession {
            session_id,
            store: self.clone(),
        })
    }

    /// 返回指定分区当前 active segment 的 pin 数。
    pub fn pin_count(&self, handle: &PartitionHandle) -> usize {
        let Ok(lease_key) = handle.active_lease_key() else {
            return 0;
        };
        self.pin_count_for_key(lease_key)
    }

    /// 返回指定分区和 segment identity 的 pin 数。
    pub fn pin_count_for_segment(
        &self,
        handle: &PartitionHandle,
        segment_id: u64,
        generation: u64,
    ) -> usize {
        self.pin_count_for_key(LeaseKey {
            partition_id: handle.partition_id(),
            segment_id,
            generation,
        })
    }

    /// 回收死亡 session。
    pub fn collect_garbage(&self) -> Result<(), ZippySegmentStoreError> {
        let dead_sessions = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(session_id, state)| (!state.is_alive()).then_some(*session_id))
            .collect::<Vec<_>>();

        for session_id in dead_sessions {
            let leases = self
                .sessions
                .lock()
                .unwrap()
                .remove(&session_id)
                .map(|state| state.leases.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            for lease in leases {
                self.decrement_pin(lease);
            }
        }

        let handles = self
            .partitions
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for handle in handles {
            handle.collect_garbage(self);
        }
        Ok(())
    }

    /// 将 sealed segment 放入最小持久化队列。
    pub fn enqueue_persistence(
        &self,
        _segment: SealedSegmentHandle,
    ) -> Result<(), ZippySegmentStoreError> {
        Err(ZippySegmentStoreError::Writer(
            "manual persistence enqueue is disabled; rollover enqueues automatically",
        ))
    }

    /// 启动后台 parquet 持久化 worker。
    pub fn start_persistence_worker(
        &self,
        output_dir: impl Into<std::path::PathBuf>,
    ) -> Result<PersistenceWorker, ZippySegmentStoreError> {
        self.persistence
            .start_worker(output_dir.into())
            .map_err(ZippySegmentStoreError::Io)
    }

    /// 按指定策略启动后台 parquet 持久化 worker。
    pub fn start_persistence_worker_with_policy(
        &self,
        output_dir: impl Into<std::path::PathBuf>,
        policy: PersistenceRetryPolicy,
    ) -> Result<PersistenceWorker, ZippySegmentStoreError> {
        self.persistence
            .start_worker_with_policy(output_dir.into(), policy)
            .map_err(ZippySegmentStoreError::Io)
    }

    /// 仅用于测试：同步 flush 一条 parquet。
    pub fn flush_one_for_test(&self) -> Result<std::path::PathBuf, ZippySegmentStoreError> {
        self.persistence
            .flush_one_for_test()
            .map_err(ZippySegmentStoreError::Writer)
    }

    /// 仅用于测试：在给定超时内同步 flush 一条 parquet。
    pub fn flush_one_timeout_for_test(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<std::path::PathBuf>, ZippySegmentStoreError> {
        self.persistence
            .flush_one_timeout_for_test(timeout)
            .map_err(ZippySegmentStoreError::Writer)
    }

    pub(crate) fn attach_lease(
        &self,
        session_id: u64,
        lease_key: LeaseKey,
    ) -> Result<(), ZippySegmentStoreError> {
        let mut sessions = self.sessions.lock().unwrap();
        let state = sessions
            .get_mut(&session_id)
            .ok_or(ZippySegmentStoreError::Lifecycle("reader session missing"))?;
        if state.leases.insert(lease_key) {
            self.increment_pin(lease_key);
        }
        Ok(())
    }

    pub(crate) fn release_lease(
        &self,
        session_id: u64,
        lease_key: LeaseKey,
    ) -> Result<(), ZippySegmentStoreError> {
        let mut sessions = self.sessions.lock().unwrap();
        let Some(state) = sessions.get_mut(&session_id) else {
            return Ok(());
        };
        if state.leases.remove(&lease_key) {
            self.decrement_pin(lease_key);
        }
        Ok(())
    }

    pub(crate) fn mark_session_dead(&self, session_id: u64) {
        if let Some(state) = self.sessions.lock().unwrap().get_mut(&session_id) {
            state.alive = false;
        }
    }

    fn pin_count_for_key(&self, lease_key: LeaseKey) -> usize {
        *self.pins.lock().unwrap().get(&lease_key).unwrap_or(&0)
    }

    fn increment_pin(&self, lease_key: LeaseKey) {
        let mut pins = self.pins.lock().unwrap();
        *pins.entry(lease_key).or_insert(0) += 1;
    }

    fn decrement_pin(&self, lease_key: LeaseKey) {
        let mut pins = self.pins.lock().unwrap();
        let Some(count) = pins.get_mut(&lease_key) else {
            return;
        };
        if *count <= 1 {
            pins.remove(&lease_key);
        } else {
            *count -= 1;
        }
    }
}

/// 分区句柄。
#[derive(Debug, Clone)]
pub struct PartitionHandle {
    inner: Arc<PartitionInner>,
}

impl PartitionHandle {
    /// 创建一个最小分区。
    pub fn new(
        store_id: u64,
        stream: &str,
        partition: &str,
        schema: CompiledSchema,
        row_capacity: usize,
        persistence: PersistenceQueue,
    ) -> Result<Self, ZippySegmentStoreError> {
        let layout = LayoutPlan::for_schema(&schema, row_capacity)
            .map_err(ZippySegmentStoreError::Layout)?;
        let persistence_key = format!("store-{store_id}-{stream}-{partition}");
        let writer = ActiveSegmentWriter::new_with_origin_for_test(
            schema.clone(),
            layout,
            1,
            0,
            persistence_key,
        )
        .map_err(ZippySegmentStoreError::Writer)?;

        Ok(Self {
            inner: Arc::new(PartitionInner {
                store_id,
                partition_id: next_partition_id(),
                schema,
                _stream: stream.to_string(),
                _partition: partition.to_string(),
                persistence,
                state: Mutex::new(PartitionState {
                    row_capacity,
                    active_segment_id: 1,
                    generation: 0,
                    writer,
                    retired_segments: HashMap::new(),
                }),
                broadcaster: SegmentBroadcaster::default(),
            }),
        })
    }

    /// 返回当前 active segment id。
    pub fn segment_id(&self) -> u64 {
        self.inner.state.lock().unwrap().active_segment_id
    }

    /// 返回当前 active segment 的 `(segment_id, generation)`。
    pub fn active_segment_identity(&self) -> (u64, u64) {
        let state = self.inner.state.lock().unwrap();
        (state.active_segment_id, state.generation)
    }

    /// 返回写句柄。
    pub fn writer(&self) -> PartitionWriterHandle {
        PartitionWriterHandle {
            handle: self.clone(),
        }
    }

    /// 返回当前 active segment 的调试快照。
    pub fn debug_snapshot_record_batch(&self) -> Result<RecordBatch, ZippySegmentStoreError> {
        let state = self.inner.state.lock().unwrap();
        active_segment_record_batch(&state.writer)
    }

    /// 返回当前 active segment 的已提交行数。
    pub fn active_committed_row_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap()
            .writer
            .committed_row_count()
    }

    /// 为当前 active segment 构造一段调试/读取用的行视图。
    pub fn active_row_span(
        &self,
        start_row: usize,
        end_row: usize,
    ) -> Result<RowSpanView, ZippySegmentStoreError> {
        let state = self.inner.state.lock().unwrap();
        let descriptor = state.writer.active_descriptor();
        RowSpanView::from_active_descriptor(descriptor, start_row, end_row)
            .map_err(ZippySegmentStoreError::Layout)
    }

    /// 返回当前 active segment 描述符的跨进程传输 envelope。
    pub fn active_descriptor_envelope_bytes(&self) -> Result<Vec<u8>, ZippySegmentStoreError> {
        let state = self.inner.state.lock().unwrap();
        state
            .writer
            .active_descriptor()
            .to_envelope_bytes()
            .map_err(ZippySegmentStoreError::Writer)
    }

    /// 返回当前 active segment 描述符，仅用于测试。
    pub fn active_descriptor_for_test(&self) -> crate::ActiveSegmentDescriptor {
        let state = self.inner.state.lock().unwrap();
        state.writer.active_descriptor()
    }

    pub(crate) fn subscribe(&self) -> Result<SegmentNotifier, ZippySegmentStoreError> {
        self.inner
            .broadcaster
            .subscribe()
            .map_err(ZippySegmentStoreError::Io)
    }

    pub(crate) fn active_lease_key(&self) -> Result<LeaseKey, ZippySegmentStoreError> {
        let state = self.inner.state.lock().unwrap();
        Ok(LeaseKey {
            partition_id: self.inner.partition_id,
            segment_id: state.active_segment_id,
            generation: state.generation,
        })
    }

    pub(crate) fn partition_id(&self) -> u64 {
        self.inner.partition_id
    }

    /// Release one retired segment after the owner has decided it is no longer readable.
    pub fn release_retired_segment(&self, segment_id: u64, generation: u64) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        let Some(retained) = state.retired_segments.get(&segment_id) else {
            return false;
        };
        if retained.generation != generation {
            return false;
        }
        state.retired_segments.remove(&segment_id).is_some()
    }

    fn collect_garbage(&self, store: &SegmentStore) {
        let mut state = self.inner.state.lock().unwrap();
        state.retired_segments.retain(|segment_id, retained| {
            store.pin_count_for_key(LeaseKey {
                partition_id: self.inner.partition_id,
                segment_id: *segment_id,
                generation: retained.generation,
            }) > 0
        });
    }
}

#[derive(Debug)]
struct PartitionInner {
    store_id: u64,
    partition_id: u64,
    schema: CompiledSchema,
    _stream: String,
    _partition: String,
    persistence: PersistenceQueue,
    state: Mutex<PartitionState>,
    broadcaster: SegmentBroadcaster,
}

#[derive(Debug)]
struct PartitionState {
    row_capacity: usize,
    active_segment_id: u64,
    generation: u64,
    writer: ActiveSegmentWriter,
    retired_segments: HashMap<u64, RetainedSegment>,
}

#[derive(Debug)]
struct RetainedSegment {
    generation: u64,
    _sealed: SealedSegmentHandle,
    _shm_region: ShmRegion,
}

/// 分区写入句柄。
///
/// 对外写入必须通过 `write_row` 完成一整行事务，避免多个 writer handle
/// 交错打开/提交同一个 active row。
#[derive(Debug, Clone)]
pub struct PartitionWriterHandle {
    handle: PartitionHandle,
}

/// 单行事务写入器。
///
/// 该类型只在 `PartitionWriterHandle::write_row` 的闭包内有效，确保 begin/write/commit
/// 使用同一个分区锁，避免多个 writer handle 交错写入同一个 active row。
pub struct PartitionRowWriter<'a> {
    writer: &'a mut ActiveSegmentWriter,
}

impl PartitionRowWriter<'_> {
    /// 写入 i64 列值。
    pub fn write_i64(&mut self, column: &str, value: i64) -> Result<(), ZippySegmentStoreError> {
        self.writer
            .write_i64(column, value)
            .map_err(ZippySegmentStoreError::Writer)
    }

    /// 写入 f64 列值。
    pub fn write_f64(&mut self, column: &str, value: f64) -> Result<(), ZippySegmentStoreError> {
        self.writer
            .write_f64(column, value)
            .map_err(ZippySegmentStoreError::Writer)
    }

    /// 写入 utf8 列值。
    pub fn write_utf8(&mut self, column: &str, value: &str) -> Result<(), ZippySegmentStoreError> {
        self.writer
            .write_utf8(column, value)
            .map_err(ZippySegmentStoreError::Writer)
    }
}

impl PartitionWriterHandle {
    /// 在同一个分区锁内写入并提交一整行。
    pub fn write_row<F>(&self, write: F) -> Result<(), ZippySegmentStoreError>
    where
        F: FnOnce(&mut PartitionRowWriter<'_>) -> Result<(), ZippySegmentStoreError>,
    {
        let result = {
            let mut state = self.handle.inner.state.lock().unwrap();
            state
                .writer
                .begin_row()
                .map_err(ZippySegmentStoreError::Writer)?;

            let write_result = {
                let mut row_writer = PartitionRowWriter {
                    writer: &mut state.writer,
                };
                write(&mut row_writer)
            };
            let result = match write_result {
                Ok(()) => state
                    .writer
                    .commit_row()
                    .map_err(ZippySegmentStoreError::Writer),
                Err(error) => Err(error),
            };
            if result.is_err() && state.writer.has_open_row() {
                let _ = state.writer.abort_row();
            }
            result
        };

        result?;
        self.handle
            .inner
            .broadcaster
            .notify_all()
            .map_err(ZippySegmentStoreError::Io)?;
        Ok(())
    }

    /// 返回当前 active segment 的调试快照。
    pub fn debug_snapshot_record_batch(&self) -> Result<RecordBatch, ZippySegmentStoreError> {
        self.handle.debug_snapshot_record_batch()
    }

    /// 返回当前 active segment 的调试快照。
    pub fn active_record_batch_for_test(&self) -> Result<RecordBatch, ZippySegmentStoreError> {
        self.debug_snapshot_record_batch()
    }

    /// 返回当前 active segment 的已提交行数。
    pub fn committed_row_count(&self) -> usize {
        self.handle
            .inner
            .state
            .lock()
            .unwrap()
            .writer
            .committed_row_count()
    }

    /// 清空当前 active segment 中的已提交行，用于原地重写快照。
    pub fn clear_rows(&self) -> Result<(), ZippySegmentStoreError> {
        {
            let mut state = self.handle.inner.state.lock().unwrap();
            state
                .writer
                .clear_rows()
                .map_err(ZippySegmentStoreError::Writer)?;
        }

        self.handle
            .inner
            .broadcaster
            .notify_all()
            .map_err(ZippySegmentStoreError::Io)?;
        Ok(())
    }

    /// 追加一条测试 tick。
    pub fn append_tick_for_test(
        &self,
        dt: i64,
        instrument_id: &str,
        last_price: f64,
    ) -> Result<(), ZippySegmentStoreError> {
        self.write_row(|row| {
            row.write_i64("dt", dt)?;
            row.write_utf8("instrument_id", instrument_id)?;
            row.write_f64("last_price", last_price)?;
            Ok(())
        })
    }

    /// 创建新的 active segment，按需入队底层持久化队列，并通知 reader。
    fn rollover_inner(
        &self,
        enqueue_persistence: bool,
    ) -> Result<SealedSegmentHandle, ZippySegmentStoreError> {
        let sealed = {
            let mut state = self.handle.inner.state.lock().unwrap();
            if state.writer.has_open_row() {
                return Err(ZippySegmentStoreError::Writer(
                    "open row exists during rollover",
                ));
            }
            let old_segment_id = state.active_segment_id;
            let old_generation = state.generation;
            let next_segment_id = old_segment_id + 1;
            let next_generation = old_generation + 1;
            let schema = self.handle.inner.schema.clone();
            let layout = LayoutPlan::for_schema(&schema, state.row_capacity)
                .map_err(ZippySegmentStoreError::Layout)?;
            let persistence_key = format!(
                "store-{}-{}-{}",
                self.handle.inner.store_id, self.handle.inner._stream, self.handle.inner._partition
            );
            let new_writer = ActiveSegmentWriter::new_with_origin_for_test(
                schema,
                layout,
                next_segment_id,
                next_generation,
                persistence_key,
            )
            .map_err(ZippySegmentStoreError::Writer)?;
            let sealed = state
                .writer
                .seal_for_rollover()
                .map_err(ZippySegmentStoreError::Writer)?;
            let old_writer = std::mem::replace(&mut state.writer, new_writer);
            let shm_region = old_writer.into_shm_region();
            state.active_segment_id = next_segment_id;
            state.generation = next_generation;
            state.retired_segments.insert(
                old_segment_id,
                RetainedSegment {
                    generation: old_generation,
                    _sealed: sealed.clone(),
                    _shm_region: shm_region,
                },
            );
            sealed
        };

        if enqueue_persistence {
            self.handle
                .inner
                .persistence
                .enqueue(sealed.clone())
                .map_err(ZippySegmentStoreError::Writer)?;
        }
        self.handle
            .inner
            .broadcaster
            .notify_all()
            .map_err(ZippySegmentStoreError::Io)?;
        Ok(sealed)
    }

    /// 创建新的 active segment 并通知 reader。
    pub fn rollover(&self) -> Result<SealedSegmentHandle, ZippySegmentStoreError> {
        self.rollover_inner(true)
    }

    /// 创建新的 active segment 并通知 reader，但不进入底层通用持久化队列。
    pub fn rollover_without_persistence(
        &self,
    ) -> Result<SealedSegmentHandle, ZippySegmentStoreError> {
        self.rollover_inner(false)
    }
}

#[derive(Debug)]
pub(crate) struct ReaderSessionState {
    _name: String,
    alive: bool,
    leases: HashSet<LeaseKey>,
}

impl ReaderSessionState {
    pub(crate) fn new(name: String) -> Self {
        Self {
            _name: name,
            alive: true,
            leases: HashSet::new(),
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LeaseKey {
    pub(crate) partition_id: u64,
    pub(crate) segment_id: u64,
    pub(crate) generation: u64,
}

fn test_schema() -> Result<crate::CompiledSchema, ZippySegmentStoreError> {
    compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .map_err(ZippySegmentStoreError::Schema)
}

fn next_store_id() -> u64 {
    static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed)
}

fn next_partition_id() -> u64 {
    static NEXT_PARTITION_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_PARTITION_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollover_keeps_pinned_old_segment_until_collect_garbage() {
        let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
        let handle = store.open_partition("ticks", "rb2501").unwrap();
        let session = store.open_session("reader-a").unwrap();
        let lease = session.attach_active(&handle).unwrap();
        let old_segment_id = lease.segment_id();

        handle.writer().rollover().unwrap();

        {
            let state = handle.inner.state.lock().unwrap();
            assert_ne!(state.active_segment_id, old_segment_id);
            assert!(state.retired_segments.contains_key(&old_segment_id));
        }

        drop(lease);
        store.collect_garbage().unwrap();

        let state = handle.inner.state.lock().unwrap();
        assert!(!state.retired_segments.contains_key(&old_segment_id));
    }

    #[test]
    fn rollover_rejects_open_row_instead_of_dropping_it() {
        let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
        let handle = store.open_partition("ticks", "rb2501").unwrap();

        {
            let mut state = handle.inner.state.lock().unwrap();
            state.writer.begin_row().unwrap();
            state.writer.write_i64("dt", 1).unwrap();
        }

        let err = handle.writer().rollover().unwrap_err();
        assert!(matches!(err, ZippySegmentStoreError::Writer(_)));

        let state = handle.inner.state.lock().unwrap();
        assert_eq!(state.active_segment_id, 1);
        assert!(state.retired_segments.is_empty());
    }

    #[test]
    fn rollover_layout_failure_keeps_active_identity_unchanged() {
        let store = SegmentStore::new(SegmentStoreConfig::for_test()).unwrap();
        let handle = store.open_partition("ticks", "rb2501").unwrap();
        handle
            .writer()
            .append_tick_for_test(1, "rb2501", 4123.5)
            .unwrap();

        {
            let mut state = handle.inner.state.lock().unwrap();
            state.row_capacity = usize::MAX;
        }

        let err = handle.writer().rollover().unwrap_err();
        assert!(matches!(err, ZippySegmentStoreError::Layout(_)));

        let state = handle.inner.state.lock().unwrap();
        assert_eq!(state.active_segment_id, 1);
        assert_eq!(state.generation, 0);
        assert_eq!(state.writer.committed_row_count(), 1);
        assert!(state.retired_segments.is_empty());
    }
}
