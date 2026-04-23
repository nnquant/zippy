use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    compile_schema, notify::SegmentBroadcaster, ActiveSegmentWriter, ColumnSpec, ColumnType,
    LayoutPlan, ReaderSession, SegmentNotifier, ZippySegmentStoreError,
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
    pub(crate) config: SegmentStoreConfig,
    pub(crate) partitions: Arc<Mutex<HashMap<(String, String), PartitionHandle>>>,
    pub(crate) sessions: Arc<Mutex<HashMap<u64, ReaderSessionState>>>,
    pub(crate) pins: Arc<Mutex<HashMap<u64, usize>>>,
    pub(crate) next_session_id: Arc<AtomicU64>,
}

impl SegmentStore {
    /// 创建 store。
    pub fn new(config: SegmentStoreConfig) -> Result<Self, ZippySegmentStoreError> {
        Ok(Self {
            config,
            partitions: Arc::new(Mutex::new(HashMap::new())),
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
        let mut partitions = self.partitions.lock().unwrap();
        let key = (stream.to_string(), partition.to_string());
        if let Some(handle) = partitions.get(&key) {
            return Ok(handle.clone());
        }

        let handle = PartitionHandle::new(stream, partition, self.config.default_row_capacity)?;
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

    /// 返回当前 pin 数。
    pub fn pin_count(&self, segment_id: u64) -> usize {
        *self.pins.lock().unwrap().get(&segment_id).unwrap_or(&0)
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
                self.decrement_pin(lease.segment_id);
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
            self.increment_pin(lease_key.segment_id);
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
            self.decrement_pin(lease_key.segment_id);
        }
        Ok(())
    }

    pub(crate) fn mark_session_dead(&self, session_id: u64) {
        if let Some(state) = self.sessions.lock().unwrap().get_mut(&session_id) {
            state.alive = false;
        }
    }

    fn increment_pin(&self, segment_id: u64) {
        let mut pins = self.pins.lock().unwrap();
        *pins.entry(segment_id).or_insert(0) += 1;
    }

    fn decrement_pin(&self, segment_id: u64) {
        let mut pins = self.pins.lock().unwrap();
        let Some(count) = pins.get_mut(&segment_id) else {
            return;
        };
        if *count <= 1 {
            pins.remove(&segment_id);
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
        stream: &str,
        partition: &str,
        row_capacity: usize,
    ) -> Result<Self, ZippySegmentStoreError> {
        let schema = test_schema()?;
        let layout = LayoutPlan::for_schema(&schema, row_capacity)
            .map_err(ZippySegmentStoreError::Layout)?;
        let writer = ActiveSegmentWriter::new_with_ids_for_test(schema, layout, 1, 0)
            .map_err(ZippySegmentStoreError::Writer)?;

        Ok(Self {
            inner: Arc::new(PartitionInner {
                _stream: stream.to_string(),
                _partition: partition.to_string(),
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

    /// 返回写句柄。
    pub fn writer(&self) -> PartitionWriterHandle {
        PartitionWriterHandle {
            handle: self.clone(),
        }
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
            segment_id: state.active_segment_id,
            generation: state.generation,
        })
    }

    fn collect_garbage(&self, store: &SegmentStore) {
        let mut state = self.inner.state.lock().unwrap();
        state
            .retired_segments
            .retain(|segment_id, _| store.pin_count(*segment_id) > 0);
    }
}

#[derive(Debug)]
struct PartitionInner {
    _stream: String,
    _partition: String,
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
    _generation: u64,
    _writer: ActiveSegmentWriter,
}

/// 分区写入句柄。
#[derive(Debug, Clone)]
pub struct PartitionWriterHandle {
    handle: PartitionHandle,
}

impl PartitionWriterHandle {
    /// 追加一条测试 tick。
    pub fn append_tick_for_test(
        &self,
        dt: i64,
        instrument_id: &str,
        last_price: f64,
    ) -> Result<(), ZippySegmentStoreError> {
        {
            let mut state = self.handle.inner.state.lock().unwrap();
            state
                .writer
                .begin_row()
                .map_err(ZippySegmentStoreError::Writer)?;
            state
                .writer
                .write_i64("dt", dt)
                .map_err(ZippySegmentStoreError::Writer)?;
            state
                .writer
                .write_utf8("instrument_id", instrument_id)
                .map_err(ZippySegmentStoreError::Writer)?;
            state
                .writer
                .write_f64("last_price", last_price)
                .map_err(ZippySegmentStoreError::Writer)?;
            state
                .writer
                .commit_row()
                .map_err(ZippySegmentStoreError::Writer)?;
        }

        self.handle
            .inner
            .broadcaster
            .notify_all()
            .map_err(ZippySegmentStoreError::Io)?;
        Ok(())
    }

    /// 创建新的 active segment 并通知 reader。
    pub fn rollover(&self) -> Result<(), ZippySegmentStoreError> {
        {
            let mut state = self.handle.inner.state.lock().unwrap();
            let old_segment_id = state.active_segment_id;
            let old_generation = state.generation;
            state.active_segment_id += 1;
            state.generation += 1;
            let schema = test_schema()?;
            let layout = LayoutPlan::for_schema(&schema, state.row_capacity)
                .map_err(ZippySegmentStoreError::Layout)?;
            let new_writer = ActiveSegmentWriter::new_with_ids_for_test(
                schema,
                layout,
                state.active_segment_id,
                state.generation,
            )
            .map_err(ZippySegmentStoreError::Writer)?;
            let old_writer = std::mem::replace(&mut state.writer, new_writer);
            state.retired_segments.insert(
                old_segment_id,
                RetainedSegment {
                    _generation: old_generation,
                    _writer: old_writer,
                },
            );
        }

        self.handle
            .inner
            .broadcaster
            .notify_all()
            .map_err(ZippySegmentStoreError::Io)?;
        Ok(())
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
}
