# Gateway Streaming Collect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `collect(stream=True)` a real streaming Gateway path for persisted parquet, with residual-plan rejection, deterministic ordered output, bounded scan parallelism, and streaming metrics while leaving default `collect()` unchanged.

**Architecture:** Keep a separate streaming collect path in `crates/zippy-gateway/src/lib.rs`. Build and validate a streamable collect plan before sending `collect_start`; then use a blocking-side batch producer and async socket writer so slow clients do not hold Gateway blocking permits. Persisted parquet is the true streaming path; live/snapshot paths remain materialized and chunked for this专项.

**Tech Stack:** Rust Gateway (`tokio`, Arrow, Parquet, `serde_json`), native Gateway integration tests, Python API tests via `uv run pytest`.

---

## File Map

- Modify `crates/zippy-gateway/src/lib.rs`
  - Add stream plan parsing and residual rejection.
  - Add streaming producer data structures.
  - Add persisted parquet ordered streaming producer.
  - Add streaming metrics and resource limits.
  - Update `handle_collect_stream_async()` to consume producer batches instead of full collect payload.
- Modify `crates/zippy-gateway/tests/native_gateway.rs`
  - Add integration tests for residual rejection, persisted streaming order, head/tail, chunking, and metrics.
- Modify `python/zippy/__init__.py`
  - Ensure `_remote_collect_stream_request()` raises clear errors on pre-start error frames and mid-stream `collect_error`.
- Modify `pytests/test_python_api.py`
  - Add Python-level tests for residual rejection and default collect compatibility.
- Modify `docs/performance_audit_fix_status_2026-05-12.md`
  - Update P002/P009 status after implementation and verification.

## Task 1: Stream Plan Validation And Residual Rejection

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Write failing native test for residual rejection before collect_start**

Add this test near `native_gateway_collect_stream_returns_start_chunks_and_end_frames()`:

```rust
#[test]
fn native_gateway_collect_stream_rejects_residual_plan_before_start() {
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
            Field::new("last_price", DataType::Float64, false),
        ])),
        vec![
            std::sync::Arc::new(StringArray::from(vec!["IF2606"])),
            std::sync::Arc::new(Float64Array::from(vec![4102.5])),
        ],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({
            "kind": "write_batch",
            "stream_name": "stream_residual_ticks",
            "token": "dev-token",
            "rows": 1
        }),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let response = send_gateway_header_without_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_residual_ticks",
            "token": "dev-token",
            "plan": [
                {"op": "with_columns", "exprs": [
                    {"kind": "literal", "value": 1, "alias": "one"}
                ]}
            ]
        }),
        0,
    )
    .unwrap();

    assert_eq!(response["status"], "error");
    assert!(response["reason"]
        .as_str()
        .unwrap()
        .contains("collect(stream=True) requires a fully streamable plan"));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_rejects_residual_plan_before_start -- --nocapture
```

Expected: FAIL because current `collect_stream` reuses full collect and returns `collect_start` / chunks instead of a pre-start error.

- [ ] **Step 3: Add stream plan struct and builder**

In `crates/zippy-gateway/src/lib.rs`, add near `GatewayScanPushdown`:

```rust
struct GatewayCollectStreamPlan {
    source: String,
    snapshot_id: Option<String>,
    plan: Vec<Value>,
    row_range_pushdown: Option<(GatewayRowRangePushdown, usize)>,
    scan_residual_start: usize,
    scan_pushdown: GatewayScanPushdown,
    chunk_rows: usize,
}
```

Add a helper near `handle_collect` helpers:

```rust
fn collect_stream_plan_from_header(header: Value) -> Result<GatewayCollectStreamPlan> {
    let source = header
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| ZippyError::InvalidConfig {
            reason: "collect_stream requires source".to_string(),
        })?
        .to_string();
    let plan = header
        .get("plan")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let row_range_pushdown = collect_plan_row_range_prefix(&plan)?;
    let row_range_residual_start = row_range_pushdown.map_or(0, |(_, start)| start);
    let pushed_filter_count =
        collect_plan_leading_filter_count(&plan[row_range_residual_start..]);
    let scan_residual_start = row_range_residual_start + pushed_filter_count;
    let scan_pushdown = GatewayScanPushdown {
        filters: plan[row_range_residual_start..scan_residual_start].to_vec(),
        projection_columns: collect_plan_scan_projection_columns(
            &plan[row_range_residual_start..],
        )?,
    };
    let residual_start = if row_range_pushdown.is_some() {
        scan_residual_start
    } else {
        scan_residual_start
    };
    if residual_start < plan.len() {
        return Err(ZippyError::InvalidConfig {
            reason: "collect(stream=True) requires a fully streamable plan; use collect() for residual operations".to_string(),
        });
    }
    let chunk_rows = header
        .get("chunk_rows")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(65_536);
    Ok(GatewayCollectStreamPlan {
        source,
        snapshot_id: header
            .get("snapshot_id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        plan,
        row_range_pushdown,
        scan_residual_start,
        scan_pushdown,
        chunk_rows,
    })
}
```

Use this helper at the start of `handle_collect_stream_async()` before writing any frames:

```rust
let stream_plan = collect_stream_plan_from_header(header)?;
let chunk_rows = stream_plan.chunk_rows;
```

Temporarily keep the existing full collect fallback by rebuilding a collect header from `stream_plan` after validation.

- [ ] **Step 4: Run residual rejection test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_rejects_residual_plan_before_start -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run existing collect_stream test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_returns_start_chunks_and_end_frames -- --nocapture
```

Expected: PASS, proving `chunk_rows` and existing protocol still work for streamable plan.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "feat: reject residual plans for streaming collect"
```

## Task 2: Producer Interface And Materialized Fallback Paths

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Write failing unit test for chunked producer from materialized batch**

Add under `#[cfg(test)] mod tests` in `crates/zippy-gateway/src/lib.rs`:

```rust
#[test]
fn materialized_stream_producer_splits_batches_by_chunk_rows() {
    let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut producer = GatewayCollectStreamProducer::materialized(batch, 2);

    let first = producer.next_batch().unwrap().unwrap();
    let second = producer.next_batch().unwrap().unwrap();
    let end = producer.next_batch().unwrap();

    assert_eq!(first.num_rows(), 2);
    assert_eq!(second.num_rows(), 1);
    assert!(end.is_none());
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p zippy-gateway materialized_stream_producer_splits_batches_by_chunk_rows -- --nocapture
```

Expected: FAIL because `GatewayCollectStreamProducer` does not exist.

- [ ] **Step 3: Add producer enum**

Add in `crates/zippy-gateway/src/lib.rs` near scan structs:

```rust
enum GatewayCollectStreamProducer {
    Materialized {
        batch: RecordBatch,
        chunk_rows: usize,
        offset: usize,
    },
}

impl GatewayCollectStreamProducer {
    fn materialized(batch: RecordBatch, chunk_rows: usize) -> Self {
        Self::Materialized {
            batch,
            chunk_rows: chunk_rows.max(1),
            offset: 0,
        }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            Self::Materialized {
                batch,
                chunk_rows,
                offset,
            } => {
                if *offset >= batch.num_rows() {
                    return Ok(None);
                }
                let rows = (*chunk_rows).min(batch.num_rows() - *offset);
                let chunk = batch.slice(*offset, rows);
                *offset += rows;
                Ok(Some(chunk))
            }
        }
    }
}
```

- [ ] **Step 4: Change `handle_collect_stream_async()` to use producer**

Replace full collect payload decoding with:

```rust
let state_for_request = Arc::clone(&state);
let mut producer = run_blocking_request(Arc::clone(&state), move || {
    state_for_request.collect_stream_producer(stream_plan)
})
.await?;
```

Add `GatewayState::collect_stream_producer()` as a temporary wrapper:

```rust
fn collect_stream_producer(
    &self,
    stream_plan: GatewayCollectStreamPlan,
) -> Result<GatewayCollectStreamProducer> {
    let collect_header = json!({
        "kind": "collect",
        "source": stream_plan.source,
        "snapshot_id": stream_plan.snapshot_id,
        "plan": stream_plan.plan,
    });
    let (_response, payload) = self.handle_collect(collect_header)?;
    let batches = decode_ipc_batches(&payload)?;
    let batch = concat_record_batches(
        batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| Arc::new(Schema::empty())),
        batches,
    )?;
    Ok(GatewayCollectStreamProducer::materialized(
        batch,
        stream_plan.chunk_rows,
    ))
}
```

Then in async loop:

```rust
write_frame_async(stream, &json!({"status": "ok", "kind": "collect_start", "chunk_rows": chunk_rows}), &schema_payload).await?;
let mut chunk_index = 0usize;
loop {
    let next = producer.next_batch()?;
    let Some(next_batch) = next else { break; };
    let encode_started = Instant::now();
    let payload = run_blocking_request(Arc::clone(&state), move || encode_ipc_table(&next_batch)).await?;
    let rows = next_batch.num_rows();
    write_frame_async(stream, &json!({"status": "ok", "kind": "collect_chunk", "chunk_index": chunk_index, "rows": rows}), &payload).await?;
    chunk_index += 1;
    let _ = encode_started;
}
write_frame_async(stream, &json!({"status": "ok", "kind": "collect_end", "chunks": chunk_index, "metrics": {"streaming": true}}), &[]).await
```

For this task, call `producer.next_batch()` directly in the async task. Task 3 moves persisted parquet work into producer construction, so `next_batch()` remains cheap for materialized chunks.

- [ ] **Step 5: Run producer and native stream tests**

Run:

```bash
cargo test -p zippy-gateway materialized_stream_producer_splits_batches_by_chunk_rows -- --nocapture
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_returns_start_chunks_and_end_frames -- --nocapture
```

Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "refactor: route streaming collect through batch producer"
```

## Task 3: Persisted Parquet Streaming Producer

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/src/lib.rs`, `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Write failing native test for persisted streaming matching default collect order**

Add this test near existing persisted tests:

```rust
#[test]
fn native_gateway_collect_streams_persisted_files_in_default_order() {
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

    let temp = tempfile::tempdir().unwrap();
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ]));
    let first_path = temp.path().join("first.parquet");
    let second_path = temp.path().join("second.parquet");
    write_parquet_batch(
        &first_path,
        &RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["IF2606"])),
                std::sync::Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap(),
    );
    write_parquet_batch(
        &second_path,
        &RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(StringArray::from(vec!["IF2607"])),
                std::sync::Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap(),
    );

    let mut client = MasterClient::connect_endpoint(master_endpoint.clone()).unwrap();
    client.register_process("stream_persisted_writer").unwrap();
    client
        .register_stream("stream_persisted_ticks", schema, 64, 4096)
        .unwrap();
    client
        .register_source("stream_persisted_source", "test", "stream_persisted_ticks", json!({}))
        .unwrap();
    client
        .publish_persisted_file("stream_persisted_ticks", first_path.to_str().unwrap(), json!({}))
        .unwrap();
    client
        .publish_persisted_file("stream_persisted_ticks", second_path.to_str().unwrap(), json!({}))
        .unwrap();

    let (default_response, default_payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({
            "kind": "collect",
            "source": "stream_persisted_ticks",
            "token": "dev-token",
            "plan": [{"op": "select", "exprs": [{"kind": "col", "value": "seq"}]}]
        }),
        vec![],
    );
    assert_eq!(default_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({
            "kind": "collect_stream",
            "source": "stream_persisted_ticks",
            "token": "dev-token",
            "chunk_rows": 1,
            "plan": [{"op": "select", "exprs": [{"kind": "col", "value": "seq"}]}]
        }),
        vec![],
    );

    assert_eq!(frames.first().unwrap().0["kind"], "collect_start");
    assert_eq!(frames.last().unwrap().0["kind"], "collect_end");
    let streamed_batches = frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
        .map(|(_, payload)| decode_ipc_batch(payload))
        .collect::<Vec<_>>();
    let default_batch = decode_ipc_batch(&default_payload);
    let streamed_values = streamed_batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    let default_values = default_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(streamed_values, default_values);
    assert_eq!(frames.last().unwrap().0["metrics"]["streaming"], json!(true));
    assert_eq!(frames.last().unwrap().0["metrics"]["scanned_files"], json!(2));

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run test and verify it fails for the right reason**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_streams_persisted_files_in_default_order -- --nocapture
```

Expected: FAIL because `collect_stream` does not yet report persisted streaming `scanned_files` metrics.

- [ ] **Step 3: Add persisted producer variant**

In `GatewayCollectStreamProducer`, add:

```rust
Persisted {
    batches: std::collections::VecDeque<RecordBatch>,
    chunk_rows: usize,
    current_batch: Option<RecordBatch>,
    current_offset: usize,
    metrics: GatewayCollectStreamMetrics,
}
```

Add metrics struct:

```rust
#[derive(Default)]
struct GatewayCollectStreamMetrics {
    streaming: bool,
    scanned_files: usize,
    scanned_rows: usize,
    returned_rows: usize,
    scan_elapsed_ms: f64,
    filter_elapsed_ms: f64,
    encode_elapsed_ms: f64,
    write_elapsed_ms: f64,
    max_pending_file_results: usize,
    materialized_live_batches: usize,
}
```

Add `GatewayCollectStreamProducer::persisted_serial()`:

```rust
fn persisted_serial(
    stream: &StreamInfo,
    scan_pushdown: &GatewayScanPushdown,
    chunk_rows: usize,
) -> Result<Self> {
    let scan_started = Instant::now();
    let mut scanned_rows = 0usize;
    let mut scanned_files = Vec::new();
    let batches = persisted_file_record_batches(
        stream,
        scan_pushdown,
        &mut scanned_rows,
        &mut scanned_files,
    )?;
    let mut metrics = GatewayCollectStreamMetrics::default();
    metrics.streaming = true;
    metrics.scanned_files = scanned_files.len();
    metrics.scanned_rows = scanned_rows;
    metrics.scan_elapsed_ms = scan_started.elapsed().as_secs_f64() * 1000.0;
    Ok(Self::Persisted {
        batches: batches.into(),
        chunk_rows: chunk_rows.max(1),
        current_batch: None,
        current_offset: 0,
        metrics,
    })
}
```

Update `next_batch()` so `Persisted` pops batches and slices by `chunk_rows`, incrementing `metrics.returned_rows`.

- [ ] **Step 4: Route persisted streams to persisted producer**

In `GatewayState::collect_stream_producer()`, use `self.master.get_stream_blocking(&stream_plan.source)?`. For sources without an in-process writer active batch and with non-empty `stream.persisted_files`, return:

```rust
GatewayCollectStreamProducer::persisted_serial(
    &stream,
    &stream_plan.scan_pushdown,
    stream_plan.chunk_rows,
)
```

Keep live/snapshot on materialized fallback in this task.

- [ ] **Step 5: Run persisted streaming order test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_streams_persisted_files_in_default_order -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "feat: stream persisted parquet collect batches"
```

## Task 4: Head/Tail Streaming Semantics

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Add native tests for head and tail**

Add two tests:

```rust
#[test]
fn native_gateway_collect_stream_head_matches_default_collect() {
    assert_streaming_row_range_matches_default("head", json!({"op": "head", "n": 1}));
}

#[test]
fn native_gateway_collect_stream_tail_matches_default_collect() {
    assert_streaming_row_range_matches_default("tail", json!({"op": "tail", "n": 1}));
}
```

Add helper:

```rust
fn assert_streaming_row_range_matches_default(label: &str, row_range_op: serde_json::Value) {
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

    let temp = tempfile::tempdir().unwrap();
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let first_path = temp.path().join(format!("{}_first.parquet", label));
    let second_path = temp.path().join(format!("{}_second.parquet", label));
    write_parquet_batch(
        &first_path,
        &RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap(),
    );
    write_parquet_batch(
        &second_path,
        &RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(Int64Array::from(vec![2_i64]))],
        )
        .unwrap(),
    );

    let stream_name = format!("stream_range_{}_ticks", label);
    let mut client = MasterClient::connect_endpoint(master_endpoint.clone()).unwrap();
    client.register_process(&format!("stream_range_{}_writer", label)).unwrap();
    client.register_stream(&stream_name, schema, 64, 4096).unwrap();
    client
        .register_source(&format!("stream_range_{}_source", label), "test", &stream_name, json!({}))
        .unwrap();
    client
        .publish_persisted_file(&stream_name, first_path.to_str().unwrap(), json!({}))
        .unwrap();
    client
        .publish_persisted_file(&stream_name, second_path.to_str().unwrap(), json!({}))
        .unwrap();

    let plan = vec![row_range_op, json!({"op": "select", "exprs": [{"kind": "col", "value": "seq"}]})];
    let (default_response, default_payload) = send_gateway_frame_with_payload(
        gateway.endpoint(),
        json!({"kind": "collect", "source": stream_name, "token": "dev-token", "plan": plan}),
        vec![],
    );
    assert_eq!(default_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({"kind": "collect_stream", "source": stream_name, "token": "dev-token", "chunk_rows": 1, "plan": plan}),
        vec![],
    );
    let default_batch = decode_ipc_batch(&default_payload);
    let streamed_values = streamed_seq_values(&frames);
    let default_values = default_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(streamed_values, default_values);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}

fn streamed_seq_values(frames: &[(serde_json::Value, Vec<u8>)]) -> Vec<i64> {
    frames
        .iter()
        .filter(|(header, _)| header["kind"] == "collect_chunk")
        .flat_map(|(_, payload)| {
            let batch = decode_ipc_batch(payload);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
```

- [ ] **Step 2: Run tests and verify current failure**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_head_matches_default_collect -- --nocapture
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_tail_matches_default_collect -- --nocapture
```

Expected: at least one command fails because producer does not yet apply row range before streaming.

- [ ] **Step 3: Apply row range in producer construction**

When building persisted producer:

- For `GatewayRowRangePushdown::Head(n)`, stop after emitting `n` rows.
- For `GatewayRowRangePushdown::Tail(n)`, call `tail_persisted_file_record_batches()` instead of `persisted_file_record_batches()`.
- For `GatewayRowRangePushdown::Slice { offset, length }`, support existing row-range pushdown by skipping `offset` rows and taking at most `length` rows.

Add fields to persisted producer:

```rust
remaining_rows: Option<usize>,
skip_rows: usize,
```

In `next_batch()`, before returning a chunk:

```rust
if self.skip_rows >= available_rows {
    self.skip_rows -= available_rows;
    continue;
}
let start = self.skip_rows;
self.skip_rows = 0;
let mut rows = chunk_rows.min(available_rows - start);
if let Some(remaining) = self.remaining_rows.as_mut() {
    rows = rows.min(*remaining);
    *remaining -= rows;
}
let chunk = batch.slice(batch_offset + start, rows);
```

- [ ] **Step 4: Run head/tail tests**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_head_matches_default_collect -- --nocapture
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_tail_matches_default_collect -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "feat: preserve row range semantics in streaming collect"
```

## Task 5: Bounded Parallel Persisted Scan

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/src/lib.rs`

- [ ] **Step 1: Add unit test for ordered result buffer**

Add under `#[cfg(test)] mod tests`:

```rust
#[test]
fn ordered_file_results_emit_in_file_index_order() {
    let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let batch_one = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
    )
    .unwrap();
    let batch_two = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![2_i64])) as ArrayRef],
    )
    .unwrap();
    let mut results = OrderedGatewayFileResults::new(2);

    results.insert(1, vec![batch_two]).unwrap();
    assert!(results.pop_ready().is_none());
    results.insert(0, vec![batch_one]).unwrap();

    let first = results.pop_ready().unwrap();
    let second = results.pop_ready().unwrap();

    assert_eq!(
        first[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        second[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p zippy-gateway ordered_file_results_emit_in_file_index_order -- --nocapture
```

Expected: FAIL because `OrderedGatewayFileResults` does not exist.

- [ ] **Step 3: Add ordered result buffer**

Add near producer structs:

```rust
struct OrderedGatewayFileResults {
    next_file_index: usize,
    max_pending: usize,
    pending: BTreeMap<usize, Vec<RecordBatch>>,
    max_observed_pending: usize,
}

impl OrderedGatewayFileResults {
    fn new(max_pending: usize) -> Self {
        Self {
            next_file_index: 0,
            max_pending: max_pending.max(1),
            pending: BTreeMap::new(),
            max_observed_pending: 0,
        }
    }

    fn insert(&mut self, file_index: usize, batches: Vec<RecordBatch>) -> Result<()> {
        if self.pending.len() >= self.max_pending {
            return Err(ZippyError::Io {
                reason: "streaming collect pending file result limit exceeded".to_string(),
            });
        }
        self.pending.insert(file_index, batches);
        self.max_observed_pending = self.max_observed_pending.max(self.pending.len());
        Ok(())
    }

    fn pop_ready(&mut self) -> Option<Vec<RecordBatch>> {
        let batches = self.pending.remove(&self.next_file_index)?;
        self.next_file_index += 1;
        Some(batches)
    }
}
```

- [ ] **Step 4: Replace serial scan with bounded parallel scan**

Add constants:

```rust
const DEFAULT_GATEWAY_PERSISTED_SCAN_PARALLELISM: usize = 4;
const DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS: usize = 8;
```

Add scan task/result structs:

```rust
struct GatewayPersistedScanTask {
    file_index: usize,
    file_path: String,
    projection_columns: Option<Vec<String>>,
    scan_pushdown: GatewayScanPushdown,
}

struct GatewayPersistedScanResult {
    file_index: usize,
    batches: Vec<RecordBatch>,
    scanned_rows: usize,
}
```

Implement worker helper:

```rust
fn scan_persisted_file_task(task: GatewayPersistedScanTask) -> Result<GatewayPersistedScanResult> {
    let mut scanned_rows = 0usize;
    let file_batches = read_parquet_record_batches(
        Path::new(&task.file_path),
        task.projection_columns.as_deref(),
    )?;
    let mut batches = Vec::new();
    for batch in file_batches {
        let batch = apply_scan_pushdown_to_record_batch(
            batch,
            &task.scan_pushdown,
            &mut scanned_rows,
        )?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(GatewayPersistedScanResult {
        file_index: task.file_index,
        batches,
        scanned_rows,
    })
}
```

For initial bounded parallelism, use scoped threads from std:

```rust
let mut next_task_index = 0usize;
let mut ordered = OrderedGatewayFileResults::new(DEFAULT_GATEWAY_STREAMING_PENDING_FILE_RESULTS);
while next_task_index < tasks.len() {
    let chunk = &tasks[next_task_index..(next_task_index + DEFAULT_GATEWAY_PERSISTED_SCAN_PARALLELISM).min(tasks.len())];
    let results = std::thread::scope(|scope| {
        let handles = chunk
            .iter()
            .cloned()
            .map(|task| scope.spawn(move || scan_persisted_file_task(task)))
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Result<Vec<_>>>()
    })?;
    for result in results {
        scanned_rows += result.scanned_rows;
        ordered.insert(result.file_index, result.batches)?;
        while let Some(ready) = ordered.pop_ready() {
            batches.extend(ready);
        }
    }
    next_task_index += chunk.len();
}
```

Derive `Clone` for `GatewayScanPushdown`:

```rust
#[derive(Clone, Default)]
struct GatewayScanPushdown {
    filters: Vec<Value>,
    projection_columns: Option<Vec<String>>,
}
```

- [ ] **Step 5: Run ordered buffer and native persisted streaming tests**

Run:

```bash
cargo test -p zippy-gateway ordered_file_results_emit_in_file_index_order -- --nocapture
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_streams_persisted_files_in_default_order -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "feat: scan persisted collect files with bounded parallelism"
```

## Task 6: Streaming Metrics And Error Frames

**Files:**
- Modify: `crates/zippy-gateway/src/lib.rs`
- Test: `crates/zippy-gateway/tests/native_gateway.rs`

- [ ] **Step 1: Add native test for streaming metrics**

Add:

```rust
#[test]
fn native_gateway_collect_stream_reports_streaming_metrics() {
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
        std::sync::Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
        vec![std::sync::Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
    .unwrap();
    let write_response = send_gateway_frame(
        gateway.endpoint(),
        json!({"kind": "write_batch", "stream_name": "stream_metrics_ticks", "token": "dev-token", "rows": 2}),
        encode_ipc_batch(&batch),
    );
    assert_eq!(write_response["status"], "ok");

    let frames = send_gateway_stream_frames(
        gateway.endpoint(),
        json!({"kind": "collect_stream", "source": "stream_metrics_ticks", "token": "dev-token", "chunk_rows": 1, "plan": []}),
        vec![],
    );
    let metrics = &frames.last().unwrap().0["metrics"];

    assert_eq!(metrics["streaming"], json!(true));
    assert_eq!(metrics["returned_rows"], json!(2));
    assert!(metrics["encode_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert!(metrics["write_elapsed_ms"].as_f64().unwrap() >= 0.0);
    assert!(metrics["materialized_live_batches"].as_u64().unwrap() >= 1);

    gateway.stop();
    master.shutdown();
    master_thread.join().unwrap().unwrap();
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_reports_streaming_metrics -- --nocapture
```

Expected: FAIL because metrics are incomplete.

- [ ] **Step 3: Add metrics serialization**

Add method:

```rust
impl GatewayCollectStreamMetrics {
    fn to_json(&self) -> Value {
        json!({
            "streaming": self.streaming,
            "scanned_files": self.scanned_files,
            "scanned_rows": self.scanned_rows,
            "returned_rows": self.returned_rows,
            "scan_elapsed_ms": self.scan_elapsed_ms,
            "filter_elapsed_ms": self.filter_elapsed_ms,
            "encode_elapsed_ms": self.encode_elapsed_ms,
            "write_elapsed_ms": self.write_elapsed_ms,
            "max_pending_file_results": self.max_pending_file_results,
            "materialized_live_batches": self.materialized_live_batches,
        })
    }
}
```

Track encode/write durations in `handle_collect_stream_async()`:

```rust
let encode_started = Instant::now();
let payload = run_blocking_request(state_for_encode, move || encode_ipc_table(&next_batch)).await?;
producer.metrics_mut().encode_elapsed_ms += encode_started.elapsed().as_secs_f64() * 1000.0;

let write_started = Instant::now();
write_frame_async(stream, &chunk_header, &payload).await?;
producer.metrics_mut().write_elapsed_ms += write_started.elapsed().as_secs_f64() * 1000.0;
```

Add `metrics()` / `metrics_mut()` methods on producer.

- [ ] **Step 4: Send `collect_error` after start on producer failure**

In streaming loop, if `producer.next_batch()` returns `Err(error)` after `collect_start`, send:

```rust
write_frame_async(
    stream,
    &json!({"status": "error", "kind": "collect_error", "reason": error.to_string()}),
    &[],
)
.await?;
return Ok(());
```

Do not use this path for pre-start plan validation; those errors must remain ordinary error frames from `handle_client_async()`.

- [ ] **Step 5: Run metrics test**

Run:

```bash
cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_reports_streaming_metrics -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-gateway/src/lib.rs crates/zippy-gateway/tests/native_gateway.rs
git commit -m "feat: report streaming collect metrics"
```

## Task 7: Python Error Handling

**Files:**
- Modify: `python/zippy/__init__.py`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Add Python test for mid-stream collect_error**

Add near existing remote collect stream tests:

```python
def test_remote_collect_stream_raises_on_collect_error(monkeypatch) -> None:
    class FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    frames = iter(
        [
            ({"status": "ok", "kind": "collect_start"}, b""),
            ({"status": "error", "kind": "collect_error", "reason": "scan failed"}, b""),
        ]
    )

    monkeypatch.setattr(zippy.socket, "create_connection", lambda *args, **kwargs: FakeSocket())
    monkeypatch.setattr(zippy, "_send_remote_frame", lambda *args, **kwargs: None)
    monkeypatch.setattr(zippy, "_recv_remote_frame", lambda *args, **kwargs: next(frames))

    with pytest.raises(RuntimeError, match="scan failed"):
        zippy._remote_collect_stream_request(
            "tcp://127.0.0.1:17691",
            {"kind": "collect", "source": "ticks", "plan": []},
        )
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream_raises_on_collect_error"
```

Expected: FAIL until `_remote_collect_stream_request()` handles `collect_error`.

- [ ] **Step 3: Update `_remote_collect_stream_request()`**

In `python/zippy/__init__.py`, ensure the frame loop handles:

```python
if status == "error":
    reason = str(header.get("reason", "remote collect stream failed"))
    raise RuntimeError(reason)
if kind == "collect_error":
    reason = str(header.get("reason", "remote collect stream failed"))
    raise RuntimeError(reason)
```

Also ensure a pre-start ordinary error frame raises the same clear `RuntimeError`.

- [ ] **Step 4: Run Python collect stream tests**

Run:

```bash
uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/zippy/__init__.py pytests/test_python_api.py
git commit -m "fix: surface streaming collect errors in python"
```

## Task 8: Final Verification And Status Update

**Files:**
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Update performance audit fix status**

In `docs/performance_audit_fix_status_2026-05-12.md`:

- Change P002 from “部分修复，剩余继续处理” to “已修复 streaming 路径，live zero-copy 留待 P008”。
- Change P009 from “部分修复，剩余继续处理” to “已修复 streaming persisted scan 基础路径，row-group pruning 留待后续”。
- Add verification commands from this task.
- Keep row-group statistics pruning and live segment zero-copy listed as future work.

- [ ] **Step 2: Run Rust verification**

Run:

```bash
cargo test -p zippy-gateway --lib
cargo test -p zippy-gateway --test native_gateway -- --test-threads=1
cargo clippy -p zippy-gateway --all-targets -- -D warnings
cargo fmt --check
```

Expected: all pass.

- [ ] **Step 3: Run Python verification**

Run:

```bash
uv run black --check python/zippy/__init__.py pytests/test_python_api.py
uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream"
```

Expected: all pass.

- [ ] **Step 4: Commit final status update**

```bash
git add docs/performance_audit_fix_status_2026-05-12.md
git commit -m "docs: update streaming collect audit status"
```

- [ ] **Step 5: Report completion**

Final response must include:

- Summary of behavior changes.
- Explicit statement that default `collect()` is unchanged.
- Verification commands and pass/fail status.
- Remaining deferred work: live segment zero-copy/chunked reader, parquet row-group pruning, config exposure for limits.

## Plan Self-Review

- Spec coverage: Tasks 1-2 cover streamable plan contract and independent streaming path; Tasks 3-5 cover persisted parquet streaming, deterministic order, head/tail, and bounded parallel scan; Task 6 covers metrics and error frames; Task 7 covers Python client errors; Task 8 covers verification and audit status.
- Placeholder scan: no TBD/TODO placeholders are used as implementation instructions.
- Type consistency: plan uses `GatewayCollectStreamPlan`, `GatewayCollectStreamProducer`, `GatewayCollectStreamMetrics`, and `OrderedGatewayFileResults` consistently across tasks.
- Scope check: default `collect()` remains unchanged; live segment zero-copy and row-group pruning are explicitly deferred.
