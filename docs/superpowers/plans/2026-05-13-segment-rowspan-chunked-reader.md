# Segment RowSpan Chunked Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a chunked `RowSpanView` batch reader and wire it into Gateway `collect(stream=True)` for live segment streams so live chunks are produced from segment row spans instead of slicing one fully materialized batch.

**Architecture:** Add a small `RowSpanBatchReader` in `zippy-segment-store` that owns a `RowSpanView`, projection columns, `chunk_rows`, and offset. Gateway gets a new `Segment` producer variant that consumes row-span readers, applies pushed filters per chunk, and reports segment streaming metrics while preserving default `collect()` behavior.

**Tech Stack:** Rust, Arrow `RecordBatch`, zippy segment-store `RowSpanView`, zippy gateway native streaming collect tests.

---

## File Map

- Modify: `crates/zippy-segment-store/src/arrow_bridge.rs`
  - Add `RowSpanBatchReader`.
  - Add `RowSpanView::batch_reader()`.
  - Add private `RowSpanView::subspan()` helper used by the reader.
- Modify: `crates/zippy-segment-store/src/lib.rs`
  - Re-export `RowSpanBatchReader`.
- Modify: `crates/zippy-segment-store/tests/arrow_bridge.rs`
  - Add sealed chunk reader tests.
  - Add active chunk reader parity test.
- Modify: `crates/zippy-gateway/src/lib.rs`
  - Add segment streaming metrics fields.
  - Add `GatewayCollectStreamProducer::Segment`.
  - Add segment producer constructor and next-batch logic.
  - Route live segment `collect(stream=True)` through segment producer when there are no persisted files.
- Modify: `crates/zippy-gateway/tests/native_gateway.rs`
  - Update live stream metrics expectations.
  - Add projection/filter parity test for chunked live streaming.
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`
  - Update P008 status and verification list after tests pass.

---

### Task 1: Segment Store Sealed RowSpan Chunk Reader

**Files:**
- Modify: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Modify: `crates/zippy-segment-store/src/arrow_bridge.rs`
- Modify: `crates/zippy-segment-store/src/lib.rs`

- [ ] **Step 1: Write failing sealed chunk tests**

Add these tests to `crates/zippy-segment-store/tests/arrow_bridge.rs` near the existing projection tests:

```rust
#[test]
fn row_span_batch_reader_splits_sealed_span_by_chunk_rows() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();
    writer.append_tick_for_test(3, "rb2510", 4128.5).unwrap();

    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 0, 3).unwrap();
    let mut reader = span.batch_reader(2, None).unwrap();

    let first = reader.next_batch().unwrap().unwrap();
    let second = reader.next_batch().unwrap().unwrap();
    let end = reader.next_batch().unwrap();

    assert_eq!(first.num_rows(), 2);
    assert_eq!(second.num_rows(), 1);
    assert!(end.is_none());

    let first_ids = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second_ids = second
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first_ids.value(0), "rb2501");
    assert_eq!(first_ids.value(1), "rb2505");
    assert_eq!(second_ids.value(0), "rb2510");
}

#[test]
fn row_span_batch_reader_projects_sealed_columns_in_requested_order() {
    let schema = compile_schema(&[
        ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();

    let span = RowSpanView::new(writer.sealed_handle_for_test().unwrap(), 0, 2).unwrap();
    let projection = vec!["last_price".to_string(), "instrument_id".to_string()];
    let mut reader = span.batch_reader(1, Some(projection)).unwrap();

    let first = reader.next_batch().unwrap().unwrap();
    let second = reader.next_batch().unwrap().unwrap();

    assert_eq!(first.num_columns(), 2);
    assert_eq!(first.schema().field(0).name(), "last_price");
    assert_eq!(first.schema().field(1).name(), "instrument_id");

    let first_prices = first
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let second_ids = second
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(first_prices.value(0), 4123.5);
    assert_eq!(second_ids.value(0), "rb2505");
    assert!(reader.next_batch().unwrap().is_none());
}
```

- [ ] **Step 2: Run RED test**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge row_span_batch_reader -- --nocapture
```

Expected: compile failure because `RowSpanView::batch_reader` does not exist.

- [ ] **Step 3: Implement sealed-capable reader**

In `crates/zippy-segment-store/src/arrow_bridge.rs`, add `RowSpanBatchReader` above `impl RowSpanView`:

```rust
/// Incrementally exports a `RowSpanView` as Arrow `RecordBatch` chunks.
#[derive(Debug, Clone)]
pub struct RowSpanBatchReader {
    span: RowSpanView,
    chunk_rows: usize,
    projection_columns: Option<Vec<String>>,
    offset: usize,
}

impl RowSpanBatchReader {
    /// Returns the next chunk, or `None` once the span is exhausted.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        if self.offset >= self.span.row_count() {
            return Ok(None);
        }
        let rows = self.chunk_rows.min(self.span.row_count() - self.offset);
        let start = self.span.start_row + self.offset;
        let end = start + rows;
        let chunk = self.span.subspan(start, end)?;
        self.offset += rows;

        match self.projection_columns.as_ref() {
            Some(columns) => {
                let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
                chunk.as_record_batch_with_projection(&names)
            }
            None => chunk.as_record_batch(),
        }
        .map(Some)
    }
}
```

In the same `impl RowSpanView`, add:

```rust
    /// Creates a reader that exports this span as Arrow batch chunks.
    pub fn batch_reader(
        &self,
        chunk_rows: usize,
        projection_columns: Option<Vec<String>>,
    ) -> Result<RowSpanBatchReader, ArrowError> {
        if let Some(columns) = projection_columns.as_ref() {
            let names = columns.iter().map(String::as_str).collect::<Vec<_>>();
            self.projected_arrow_schema(&names)?;
        }
        Ok(RowSpanBatchReader {
            span: self.clone(),
            chunk_rows: chunk_rows.max(1),
            projection_columns,
            offset: 0,
        })
    }

    fn subspan(&self, start_row: usize, end_row: usize) -> Result<Self, ArrowError> {
        if start_row < self.start_row || end_row > self.end_row || start_row > end_row {
            return Err(ArrowError::SchemaError(
                "row span chunk is out of bounds".to_string(),
            ));
        }
        Ok(Self {
            backing: self.backing.clone(),
            start_row,
            end_row,
        })
    }
```

In `crates/zippy-segment-store/src/lib.rs`, change the existing `view` re-export to include the reader:

```rust
pub use view::{RowSpanView, SegmentCellValue};
pub use arrow_bridge::RowSpanBatchReader;
```

If `arrow_bridge` is not declared before this export in `lib.rs`, move only the export line to a location after `mod arrow_bridge;` and do not reorder unrelated modules.

- [ ] **Step 4: Run GREEN test**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge row_span_batch_reader -- --nocapture
```

Expected: the two `row_span_batch_reader_*sealed*` tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-segment-store/src/arrow_bridge.rs crates/zippy-segment-store/src/lib.rs crates/zippy-segment-store/tests/arrow_bridge.rs
git commit -m "feat: add rowspan batch reader"
```

---

### Task 2: Active RowSpan Chunk Reader Parity

**Files:**
- Modify: `crates/zippy-segment-store/tests/arrow_bridge.rs`
- Modify if needed: `crates/zippy-segment-store/src/arrow_bridge.rs`

- [ ] **Step 1: Write active parity test**

Add this test to `crates/zippy-segment-store/tests/arrow_bridge.rs` after `active_descriptor_reopens_shared_segment_for_record_batch_export`:

```rust
#[test]
fn row_span_batch_reader_reads_active_span_chunks() {
    let (schema, layout) = tick_schema_and_layout();
    let mut writer = ActiveSegmentWriter::new_for_test(schema, layout).unwrap();

    writer.append_tick_for_test(1, "rb2501", 4123.5).unwrap();
    writer.append_tick_for_test(2, "rb2505", 4125.0).unwrap();
    writer.append_tick_for_test(3, "rb2510", 4128.5).unwrap();

    let span = RowSpanView::from_active_descriptor(writer.active_descriptor(), 0, 3).unwrap();
    let full = span.as_record_batch().unwrap();
    let mut reader = span
        .batch_reader(
            2,
            Some(vec![
                "instrument_id".to_string(),
                "last_price".to_string(),
            ]),
        )
        .unwrap();

    let first = reader.next_batch().unwrap().unwrap();
    let second = reader.next_batch().unwrap().unwrap();
    assert!(reader.next_batch().unwrap().is_none());

    assert_eq!(first.num_rows(), 2);
    assert_eq!(second.num_rows(), 1);

    let full_ids = full
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let first_ids = first
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let second_prices = second
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(first_ids.value(0), full_ids.value(0));
    assert_eq!(first_ids.value(1), full_ids.value(1));
    assert_eq!(second_prices.value(0), 4128.5);
}
```

- [ ] **Step 2: Run active parity test**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge row_span_batch_reader_reads_active_span_chunks -- --nocapture
```

Expected: pass if Task 1 implementation correctly clones `RowSpanBacking::Active`; otherwise fail with the concrete active reader error.

- [ ] **Step 3: Fix active path only if the test fails**

If active attach or payload consistency fails, inspect `RowSpanView::subspan()` in `crates/zippy-segment-store/src/arrow_bridge.rs` and keep the clone-based implementation:

```rust
        Ok(Self {
            backing: self.backing.clone(),
            start_row,
            end_row,
        })
```

Do not reopen shared memory per chunk; cloned `ActiveSegmentAttachment` should reuse the same `Arc<ShmRegion>`.

- [ ] **Step 4: Run full segment-store arrow bridge test**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge -- --nocapture
```

Expected: all arrow bridge tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-segment-store/src/arrow_bridge.rs crates/zippy-segment-store/tests/arrow_bridge.rs
git commit -m "test: cover active rowspan batch reader"
```

If Step 3 required no production code change, commit only the test file.

---

### Task 3: Gateway Segment Streaming Producer

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Modify: `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Update live metrics test as RED**

In `native_gateway_collect_stream_reports_streaming_metrics`, replace the final materialized assertion:

```rust
    assert!(metrics["materialized_live_batches"].as_u64().unwrap() >= 1);
```

with:

```rust
    assert_eq!(metrics["materialized_live_batches"], json!(0));
    assert_eq!(metrics["segment_streamed_batches"], json!(2));
    assert_eq!(metrics["segment_streamed_rows"], json!(2));
```

- [ ] **Step 2: Run RED gateway metrics test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_reports_streaming_metrics -- --nocapture --test-threads=1
```

Expected: fail because the existing path reports `materialized_live_batches >= 1` and does not populate `segment_streamed_batches`.

- [ ] **Step 3: Add metrics fields**

In `crates/zippy-gateway/src/lib.rs`, extend `GatewayCollectStreamMetrics`:

```rust
    segment_streamed_batches: usize,
    segment_streamed_rows: usize,
```

Update `from_collect_metrics()`:

```rust
            metrics.segment_streamed_batches = object
                .get("segment_streamed_batches")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
            metrics.segment_streamed_rows = object
                .get("segment_streamed_rows")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or_default();
```

Update `value()`:

```rust
            "segment_streamed_batches": self.segment_streamed_batches,
            "segment_streamed_rows": self.segment_streamed_rows,
```

Update `normalize_collect_stream_metrics()` so missing fields default to zero:

```rust
    object.entry("segment_streamed_batches").or_insert(json!(0));
    object.entry("segment_streamed_rows").or_insert(json!(0));
```

- [ ] **Step 4: Add producer variant**

Import the reader near existing segment-store imports:

```rust
    RowSpanBatchReader,
```

Add this variant to `GatewayCollectStreamProducer`:

```rust
    Segment {
        schema: SchemaRef,
        readers: VecDeque<RowSpanBatchReader>,
        scan_pushdown: GatewayScanPushdown,
        metrics: GatewayCollectStreamMetrics,
    },
```

Update `schema()`, `metrics()`, and `metrics_mut()` match arms:

```rust
            Self::Segment { schema, .. } => Arc::clone(schema),
```

```rust
            Self::Segment { metrics, .. } => metrics.value(),
```

```rust
            Self::Segment { metrics, .. } => metrics,
```

- [ ] **Step 5: Add segment producer constructor**

Add this method to `impl GatewayCollectStreamProducer`:

```rust
    fn segment(
        schema: SchemaRef,
        spans: Vec<RowSpanView>,
        scan_pushdown: GatewayScanPushdown,
        chunk_rows: usize,
    ) -> Result<Self> {
        let mut readers = VecDeque::with_capacity(spans.len());
        for span in spans {
            let reader = span
                .batch_reader(chunk_rows, scan_pushdown.projection_columns.clone())
                .map_err(|error| ZippyError::Io {
                    reason: error.to_string(),
                })?;
            readers.push_back(reader);
        }
        Ok(Self::Segment {
            schema: schema_for_scan_pushdown(&schema, &scan_pushdown)?,
            readers,
            scan_pushdown,
            metrics: GatewayCollectStreamMetrics {
                streaming: true,
                ..GatewayCollectStreamMetrics::default()
            },
        })
    }
```

- [ ] **Step 6: Add segment next-batch branch**

In `GatewayCollectStreamProducer::next_batch()`, add:

```rust
            Self::Segment {
                readers,
                scan_pushdown,
                metrics,
                ..
            } => loop {
                let Some(reader) = readers.front_mut() else {
                    return Ok(None);
                };
                let Some(mut batch) = reader.next_batch().map_err(|error| ZippyError::Io {
                    reason: error.to_string(),
                })? else {
                    readers.pop_front();
                    continue;
                };
                metrics.segment_streamed_batches =
                    metrics.segment_streamed_batches.saturating_add(1);
                metrics.segment_streamed_rows =
                    metrics.segment_streamed_rows.saturating_add(batch.num_rows());
                metrics.scanned_rows = metrics.scanned_rows.saturating_add(batch.num_rows());
                for filter in &scan_pushdown.filters {
                    batch = apply_filter(batch, filter)?;
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                metrics.returned_rows = metrics.returned_rows.saturating_add(batch.num_rows());
                return Ok(Some(batch));
            },
```

- [ ] **Step 7: Route active writer live stream to segment producer**

In `GatewayState::collect_stream_producer()`, before falling back to `handle_collect()`, add an active writer branch for streams without `snapshot_id`:

```rust
        if stream_plan.snapshot_id.is_none() {
            if let Some(writer) = self
                .writers
                .lock()
                .unwrap()
                .get(&stream_plan.source)
                .cloned()
            {
                let writer = writer.lock().unwrap();
                let materializer = &writer.materializer;
                let span = materializer.active_row_span()?;
                return GatewayCollectStreamProducer::segment(
                    span.schema_ref(),
                    vec![span],
                    stream_plan.scan_pushdown,
                    stream_plan.chunk_rows,
                );
            }
        }
```

If `StreamTableMaterializer` does not yet expose `active_row_span()`, add this method in `crates/zippy-engines/src/stream_table.rs`:

```rust
    pub fn active_row_span(&self) -> Result<RowSpanView> {
        let committed = self.writer.committed_row_count()?;
        RowSpanView::from_active_descriptor(self.writer.active_descriptor(), 0, committed)
            .map_err(|status| ZippyError::InvalidState { status })
    }
```

Use the actual writer field name from `StreamTableMaterializer`; do not add a second writer handle.

- [ ] **Step 8: Run GREEN gateway metrics test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_reports_streaming_metrics -- --nocapture --test-threads=1
```

Expected: test passes and metrics report `materialized_live_batches=0`, `segment_streamed_batches=2`, `segment_streamed_rows=2`.

- [ ] **Step 9: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs crates/zippy-engines/src/stream_table.rs
git commit -m "feat: stream live segment chunks from rowspan readers"
```

If `stream_table.rs` was not changed, omit it from `git add`.

---

### Task 4: Gateway Projection, Filter, And Status Documentation

**Files:**
- Modify: `crates/zippy-gateway/tests/native_gateway.rs`
- Modify if needed: `crates/zippy-gateway/src/lib.rs`
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Add projection + filter parity test**

Add this test near the other `collect_stream` native gateway tests:

```rust
#[test]
fn native_gateway_collect_stream_live_segment_applies_projection_and_filter_by_chunk() {
    let master_endpoint = loopback_control_endpoint();
    let (master, master_thread) = spawn_master(master_endpoint.clone());
    let gateway_endpoint = format!("127.0.0.1:{}", reserve_tcp_port());
    let gateway = GatewayServer::new(GatewayServerConfig {
        endpoint: gateway_endpoint.clone(),
        master_endpoint: master_endpoint.clone(),
        token: Some("dev-token".to_string()),
        max_write_rows: Some(1024),
    })
    .unwrap()
    .start()
    .unwrap();

    let batch = RecordBatch::try_new(
        std::sync::Arc::new(Schema::new(vec![
            Field::new("instrument_id", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
            Field::new("last_price", DataType::Float64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606", "IF2607", "IF2608"])),
            std::sync::Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5, 4103.5, 4104.5])),
        ],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_live_chunk_filter_ticks",
            "token": "dev-token",
            "rows": 3
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_live_chunk_filter_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [
                {
                    "op": "filter",
                    "expr": {
                        "kind": "binary",
                        "op": ">",
                        "left": {"kind": "col", "value": "seq"},
                        "right": {"kind": "literal", "value": 1}
                    }
                },
                {
                    "op": "select",
                    "exprs": [
                        {"kind": "col", "value": "instrument_id"},
                        {"kind": "col", "value": "last_price"}
                    ]
                }
            ]
        }),
        vec![],
    );

    let chunks = frames
        .iter()
        .filter(|(header, _)| header["kind"] == json!("collect_chunk"))
        .map(|(_, payload)| decode_ipc_batch(payload))
        .collect::<Vec<_>>();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].schema().field(0).name(), "instrument_id");
    assert_eq!(chunks[0].schema().field(1).name(), "last_price");

    let ids = chunks
        .iter()
        .flat_map(|chunk| {
            let array = chunk
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len()).map(|row| array.value(row).to_string()).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["IF2607".to_string(), "IF2608".to_string()]);

    let metrics = &frames.last().unwrap().0["metrics"];
    assert_eq!(metrics["segment_streamed_batches"], json!(3));
    assert_eq!(metrics["segment_streamed_rows"], json!(3));
    assert_eq!(metrics["returned_rows"], json!(2));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run projection/filter test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_live_segment_applies_projection_and_filter_by_chunk -- --nocapture --test-threads=1
```

Expected: pass after Task 3. If it fails, fix only projection/filter ordering in the segment producer: projection must happen in `RowSpanBatchReader`, filters must happen after chunk materialization.

- [ ] **Step 3: Run focused regression suite**

Run:

```bash
cargo test -p zippy-segment-store --test arrow_bridge -- --nocapture
cargo test -p zippy-gateway --test native_gateway collect_stream -- --nocapture --test-threads=1
cargo test -p zippy-gateway --lib -- --nocapture
```

Expected:

- all `arrow_bridge` tests pass.
- native gateway `collect_stream` tests pass.
- gateway lib tests pass.

- [ ] **Step 4: Run clippy and formatting**

Run:

```bash
cargo clippy -p zippy-segment-store -p zippy-engines -p zippy-gateway --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 5: Update P008 status document**

In `docs/performance_audit_fix_status_2026-05-12.md`, update:

- status table P008 row to mention `RowSpan` projection and chunked reader.
- P008 section to add:

```markdown
- `RowSpanView` 增加 chunked batch reader，按 `chunk_rows` 逐块导出 segment 行范围。
- Gateway `collect(stream=True)` 的 live segment 路径接入 segment chunked producer，不再先把 active batch 完整 materialize 后切片。
- segment streaming metrics 增加 `segment_streamed_batches` 和 `segment_streamed_rows`，用于区分真实 chunked segment streaming 与 materialized fallback。
```

Keep the boundary text:

```markdown
- 本轮没有实现 Arrow buffer zero-copy。
- Utf8 和 nullable 列仍会在每个 chunk 投影时分配新 Arrow array。
- mixed persisted + live 的统一 streaming planner 仍需后续专项。
```

- [ ] **Step 6: Commit docs and final changes**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs docs/performance_audit_fix_status_2026-05-12.md
git commit -m "docs: update segment chunked reader status"
```

If Task 4 included code fixes, keep them in this commit only if they are directly required by the projection/filter test.

---

## Final Verification

Run the final verification set before reporting completion:

```bash
cargo test -p zippy-segment-store --test arrow_bridge -- --nocapture
cargo test -p zippy-gateway --test native_gateway collect_stream -- --nocapture --test-threads=1
cargo test -p zippy-gateway --lib -- --nocapture
cargo clippy -p zippy-segment-store -p zippy-engines -p zippy-gateway --all-targets -- -D warnings
cargo fmt --check
git diff --check
git status --short
```

Expected final state:

- All commands exit 0.
- `git status --short` is empty after the final commit.
- Latest commits include:
  - `feat: add rowspan batch reader`
  - `test: cover active rowspan batch reader`
  - `feat: stream live segment chunks from rowspan readers`
  - `docs: update segment chunked reader status`

---

## Self-Review

Spec coverage:

- `RowSpanView` chunked batch reader is covered by Tasks 1 and 2.
- Gateway live segment chunked streaming is covered by Task 3.
- Projection and filter semantics are covered by Task 4.
- Metrics distinction between materialized fallback and segment streaming is covered by Tasks 3 and 4.
- Default `collect()` unchanged is protected by only routing `collect_stream_producer()` and keeping existing `handle_collect()` intact.
- Zero-copy is explicitly excluded and remains a later专项.

Placeholder scan:

- This plan intentionally contains no placeholder markers.

Type consistency:

- `RowSpanBatchReader`, `RowSpanView::batch_reader`, `GatewayCollectStreamProducer::Segment`,
  `segment_streamed_batches`, and `segment_streamed_rows` are defined before they are used.
