//! Atomic, explicitly requested keyed snapshots and incremental consumer state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SegmentTableView, ZippyError};

use crate::latest_state::LatestColumnarState;

pub(crate) const SEQ: &str = "_zippy_kv_seq";
pub(crate) const OP: &str = "_zippy_kv_op";
pub(crate) const VERSION: &str = "_zippy_kv_snapshot_version";
pub(crate) const BATCH_END: &str = "_zippy_kv_batch_last_seq";
pub(crate) const STREAM_ID: &str = "_zippy_kv_stream_id";
const PROTOCOL: &str = "2";

pub(crate) struct SnapshotState {
    pub latest: LatestColumnarState,
    pub sequence: u64,
    pub version: u64,
    pub stream_id: String,
    pub healthy: bool,
}

impl SnapshotState {
    fn snapshot(&self) -> Result<RecordBatch> {
        if !self.healthy {
            return Err(invalid("key-value state requires a fresh snapshot"));
        }
        let batch = self.latest.materialize_snapshot()?;
        let mut metadata = HashMap::new();
        metadata.insert("zippy_kv_protocol".into(), PROTOCOL.into());
        metadata.insert("zippy_kv_seq".into(), self.sequence.to_string());
        metadata.insert("zippy_kv_version".into(), self.version.to_string());
        metadata.insert("zippy_kv_stream_id".into(), self.stream_id.clone());
        metadata.insert(
            "zippy_kv_keys".into(),
            serde_json::to_string(self.latest.key_fields())
                .map_err(|_| invalid("cannot encode key-value keys"))?,
        );
        let schema = Arc::new(Schema::new_with_metadata(
            batch.schema().fields().clone(),
            metadata,
        ));
        RecordBatch::try_new(schema, batch.columns().to_vec())
            .map_err(|_| invalid("cannot build key-value snapshot"))
    }
}

/// Shared snapshot capability; data and cursor are captured under the same producer lock.
#[derive(Clone)]
pub struct KeyValueSnapshotHandle(pub(crate) Arc<Mutex<SnapshotState>>);

impl KeyValueSnapshotHandle {
    /// Materializes a full snapshot only on explicit request. Metadata carries the delta cursor.
    pub fn snapshot(&self) -> Result<RecordBatch> {
        self.0
            .lock()
            .map_err(|_| invalid("key-value producer lock poisoned"))?
            .snapshot()
    }
}

/// Batch-oriented consumer with bounded fragment assembly and fail-closed continuity checks.
///
/// Bootstrap using an atomic producer snapshot, then apply unfiltered deltas. A failed apply
/// invalidates the consumer; construct a new instance from a fresh snapshot before continuing.
pub struct KeyValueState {
    state: SnapshotState,
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    pending_end: u64,
    received: u64,
    pending_bytes: usize,
    max_pending_bytes: usize,
}

impl KeyValueState {
    /// Loads a protocol-v2 snapshot, including its stream identity and exact committed cursor.
    pub fn from_snapshot(snapshot: RecordBatch, max_pending_bytes: usize) -> Result<Self> {
        if max_pending_bytes == 0 {
            return Err(invalid("pending delta byte budget must be positive"));
        }
        let schema = snapshot.schema();
        let metadata = schema.metadata();
        let get = |name: &str| {
            metadata
                .get(name)
                .ok_or_else(|| invalid("snapshot cursor metadata missing"))
        };
        if get("zippy_kv_protocol")? != PROTOCOL {
            return Err(invalid("unsupported key-value protocol"));
        }
        let sequence = get("zippy_kv_seq")?
            .parse::<u64>()
            .map_err(|_| invalid("invalid snapshot sequence"))?;
        let version = get("zippy_kv_version")?
            .parse::<u64>()
            .map_err(|_| invalid("invalid snapshot version"))?;
        if sequence > i64::MAX as u64
            || version > i64::MAX as u64
            || (sequence == 0) != (version == 0)
        {
            return Err(invalid("snapshot cursor is outside protocol bounds"));
        }
        let stream_id = get("zippy_kv_stream_id")?.clone();
        if stream_id.is_empty() {
            return Err(invalid("empty snapshot stream identity"));
        }
        let keys: Vec<String> = serde_json::from_str(get("zippy_kv_keys")?)
            .map_err(|_| invalid("invalid snapshot key metadata"))?;
        let schema = Arc::new(Schema::new(schema.fields().clone()));
        let mut latest = LatestColumnarState::new(Arc::clone(&schema), keys)?;
        latest.apply_batch(&SegmentTableView::from_record_batch(
            RecordBatch::try_new(Arc::clone(&schema), snapshot.columns().to_vec())
                .map_err(|_| invalid("invalid snapshot columns"))?,
        ))?;
        if latest.len() != snapshot.num_rows() {
            return Err(invalid("snapshot contains duplicate keys"));
        }
        Ok(Self {
            state: SnapshotState {
                latest,
                sequence,
                version,
                stream_id,
                healthy: true,
            },
            schema,
            pending: Vec::new(),
            pending_end: 0,
            received: sequence,
            pending_bytes: 0,
            max_pending_bytes,
        })
    }

    /// Applies complete versions atomically. Partial versions remain invisible until their end.
    /// Returns the number of complete versions applied; rows covered by the snapshot are skipped.
    pub fn apply(&mut self, batch: RecordBatch) -> Result<usize> {
        if !self.state.healthy {
            return Err(invalid("key-value state requires a fresh snapshot"));
        }
        let result = self.apply_inner(batch);
        if result.is_err() {
            self.state.healthy = false;
            self.pending.clear();
        }
        result
    }

    fn apply_inner(&mut self, batch: RecordBatch) -> Result<usize> {
        let schema = batch.schema();
        if schema.fields().len() != self.schema.fields().len() + 5
            || schema.fields()[5..] != self.schema.fields()[..]
            || schema.fields()[..5]
                .iter()
                .map(|field| field.name().as_str())
                .ne([SEQ, OP, VERSION, BATCH_END, STREAM_ID])
        {
            return Err(invalid("key-value delta schema mismatch"));
        }
        let seq = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid delta sequence type"))?;
        let op = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("invalid delta operation type"))?;
        let version = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid delta version type"))?;
        let end = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("invalid delta batch boundary type"))?;
        let stream = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("invalid delta identity type"))?;
        if batch.columns()[..5]
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("null delta control field"));
        }
        let mut applied = 0;
        let mut first = None;
        for row in 0..batch.num_rows() {
            if stream.value(row) != self.state.stream_id
                || seq.value(row) <= 0
                || version.value(row) <= 0
                || end.value(row) < seq.value(row)
                || op.value(row) != "upsert"
            {
                return Err(invalid("invalid delta identity, operation, or boundary"));
            }
            let sequence = seq.value(row) as u64;
            if sequence <= self.state.sequence {
                if version.value(row) as u64 > self.state.version
                    || end.value(row) as u64 > self.state.sequence
                {
                    return Err(invalid("delta overlaps an inconsistent snapshot boundary"));
                }
                continue;
            }
            if sequence != self.received + 1 || version.value(row) as u64 != self.state.version + 1
            {
                return Err(invalid(
                    "key-value delta gap or out-of-order version; resnapshot required",
                ));
            }
            if self.pending_end == 0 {
                self.pending_end = end.value(row) as u64;
            }
            if end.value(row) as u64 != self.pending_end {
                return Err(invalid("delta batch boundary changed"));
            }
            first.get_or_insert(row);
            self.received = sequence;
            if sequence == self.pending_end {
                self.push_fragment(&batch, first.take().expect("fragment started"), row + 1)?;
                let update = concat_batches(&self.schema, &self.pending)
                    .map_err(|_| invalid("cannot assemble delta version"))?;
                self.state
                    .latest
                    .apply_batch(&SegmentTableView::from_record_batch(update))?;
                self.state.sequence = sequence;
                self.state.version += 1;
                self.pending.clear();
                self.pending_bytes = 0;
                self.pending_end = 0;
                applied += 1;
            }
        }
        if let Some(first) = first {
            self.push_fragment(&batch, first, batch.num_rows())?;
        }
        Ok(applied)
    }

    fn push_fragment(&mut self, batch: &RecordBatch, first: usize, end: usize) -> Result<()> {
        let fragment = RecordBatch::try_new(
            Arc::clone(&self.schema),
            batch.columns()[5..]
                .iter()
                .map(|column| column.slice(first, end - first))
                .collect(),
        )
        .map_err(|_| invalid("cannot slice delta fragment"))?;
        let bytes = fragment.get_array_memory_size();
        if bytes > self.max_pending_bytes.saturating_sub(self.pending_bytes) {
            return Err(invalid(
                "incomplete delta version exceeds byte budget; resnapshot required",
            ));
        }
        self.pending_bytes += bytes;
        self.pending.push(fragment);
        Ok(())
    }

    /// Returns the latest fully applied state, never exposing an incomplete version.
    pub fn snapshot(&self) -> Result<RecordBatch> {
        self.state.snapshot()
    }

    /// Returns the last atomically applied delta sequence.
    pub fn sequence(&self) -> u64 {
        self.state.sequence
    }

    /// Reads only requested composite keys in caller order, without building a full snapshot.
    /// A missing key or invalidated state is an error; partial results are never bootstrap snapshots.
    pub fn select(&self, keys: &[Vec<String>]) -> Result<RecordBatch> {
        if !self.state.healthy {
            return Err(invalid("key-value state requires a fresh snapshot"));
        }
        self.state.latest.select_keys(keys)
    }
}

fn invalid(status: &'static str) -> ZippyError {
    ZippyError::InvalidState { status }
}
