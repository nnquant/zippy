# Low-Latency Performance Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the performance harness measure consumer-observed stream latency, drain-inclusive processing throughput, queue backlog, and row completeness, then enforce those semantics in the scheduled performance workflow.

**Architecture:** `write-to-subscribe-e2e` becomes a paced producer plus a blocking `ActiveSegmentReader` consumer sharing one monotonic origin; the report is built only after produced, consumed, measured, and sampled counts agree. `inproc-timeseries` keeps queue-admission latency but reports throughput through final drain and samples the maximum observed queue depth. Existing JSON fields remain unchanged and new fields are additive with serde defaults.

**Tech Stack:** Rust, `zippy-segment-store`, `zippy-core` runtime metrics, Serde JSON, Cargo tests, GitHub Actions.

---

## File Map

- Modify: `crates/zippy-perf/src/lib.rs`
  - Own report schema, latency aggregation, producer/consumer benchmark logic, in-process drive statistics, pass/fail evaluation, and unit/integration tests.
- Modify: `crates/zippy-perf/README.md`
  - Define the corrected measurement boundaries and explain shared-runner versus production thresholds.
- Modify: `.github/workflows/performance.yml`
  - Run the corrected write-to-subscribe profile on scheduled/manual Linux jobs and retain reports on failure.
- Create: `docs/perf_reports/2026-07-11-low-latency-performance-gates.md`
  - Preserve the pre-change synchronous report and the corrected consumer-boundary release report with an explicit warning that the two latency series use different semantics.

No public CLI option changes are required. `crates/zippy-perf/src/main.rs` already exposes the profile, thresholds, and JSON output path.

The new JSON fields are backward-compatible because they use serde defaults. They do change Rust source compatibility for external struct literals; this is accepted for the repository-internal `0.5.0` performance harness and must be called out in the release report.

### Task 0: Capture The Pre-Change Semantic Baseline

**Files:**
- Read: `crates/zippy-perf/src/lib.rs`
- Generate: `target/zippy-perf-write-to-subscribe-before.json`
- Generate: `/tmp/zippy-perf-write-to-subscribe-before.txt`

- [ ] **Step 1: Run the existing synchronous profile before editing code**

Run before any Phase 2 production edit, using `3ff850ade14dcbfbe9b636db94a9832aded20a87` as the code baseline:

```bash
set -euo pipefail
git diff --quiet 3ff850ade14dcbfbe9b636db94a9832aded20a87..HEAD -- \
  crates/zippy-perf/src/lib.rs crates/zippy-perf/src/main.rs
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- \
  write-to-subscribe-e2e \
  --rows-per-batch 1 \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 2 \
  --symbols 32 \
  --report-json target/zippy-perf-write-to-subscribe-before.json \
  | tee /tmp/zippy-perf-write-to-subscribe-before.txt
jq -e '.pass == true' target/zippy-perf-write-to-subscribe-before.json
```

Expected: exit zero and `pass=true`.

- [ ] **Step 2: Preserve the exact baseline evidence**

Do not interpret the old p50/p95/p99 as blocking-consumer latency: this implementation performs synchronous `commit -> read_available()` on the producer thread. Keep both generated files until Task 5, where their exact values and the base commit are copied into the committed performance report.

### Task 1: Add Truthful Latency Summary Fields

**Files:**
- Modify: `crates/zippy-perf/src/lib.rs:84-90`
- Modify: `crates/zippy-perf/src/lib.rs:189-220`
- Modify: `crates/zippy-perf/src/lib.rs:1724-1735`
- Test: `crates/zippy-perf/src/lib.rs` test module

- [ ] **Step 1: Write failing aggregation and compatibility tests**

Add tests that require count/max output and legacy JSON deserialization:

```rust
#[test]
fn latency_summary_reports_sample_count_and_maximum() {
    let summary = super::build_latency_summary(&[9, 1, 4]);

    assert_eq!(summary.sample_count, 3);
    assert_eq!(summary.max_micros, 9.0);
    assert_eq!(summary.p50_micros, 4.0);
    assert_eq!(summary.p95_micros, 9.0);
    assert_eq!(summary.p99_micros, 9.0);
}

#[test]
fn latency_summary_deserializes_legacy_json_without_additive_fields() {
    let summary: LatencySummary = serde_json::from_str(
        r#"{"p50_micros":1.0,"p95_micros":2.0,"p99_micros":3.0}"#,
    )
    .unwrap();

    assert_eq!(summary.sample_count, 0);
    assert_eq!(summary.max_micros, 0.0);
    assert_eq!(summary.p99_micros, 3.0);
}
```

Extend `writes_report_json_with_profile_and_pass_fields` with exact additive-field checks:

```rust
let formatted = super::format_report(&report);
assert!(formatted.contains("latency_micros[count="));
assert!(formatted.contains("max="));
assert!(content.contains("sample_count"));
assert!(content.contains("max_micros"));
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test -p zippy-perf tests::latency_summary_reports_sample_count_and_maximum -- --exact
cargo test -p zippy-perf tests::latency_summary_deserializes_legacy_json_without_additive_fields -- --exact
```

Expected: compilation fails because `LatencySummary` has no `sample_count` or `max_micros` fields.

- [ ] **Step 3: Add additive fields and populate them**

Use serde defaults so new code can read old artifacts:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencySummary {
    #[serde(default)]
    pub sample_count: u64,
    pub p50_micros: f64,
    pub p95_micros: f64,
    pub p99_micros: f64,
    #[serde(default)]
    pub max_micros: f64,
}
```

Build the populated summary from the sorted samples:

```rust
LatencySummary {
    sample_count: u64::try_from(sorted.len()).unwrap_or(u64::MAX),
    p50_micros: percentile(&sorted, 0.50),
    p95_micros: percentile(&sorted, 0.95),
    p99_micros: percentile(&sorted, 0.99),
    max_micros: sorted.last().copied().unwrap_or_default() as f64,
}
```

Render primary and secondary summaries as:

```text
latency_micros[count=3, p50=4.00, p95=9.00, p99=9.00, max=9.00]
```

Update the two `LatencySummary` literals in
`evaluate_pass_rejects_latency_above_configured_p95_threshold` and
`evaluate_pass_rejects_latency_above_configured_p99_threshold` by adding:

```rust
sample_count: 1,
max_micros: p99_micros,
```

Use each literal's existing p99 value (`101.0` and `80.0`) for `max_micros`.

- [ ] **Step 4: Run Task 1 tests and the library suite**

Run:

```bash
cargo test -p zippy-perf latency_summary -- --nocapture
cargo test -p zippy-perf --lib
```

Expected: focused tests pass and the existing 14-test library baseline remains green with the new tests added.

- [ ] **Step 5: Commit Task 1**

```bash
git add crates/zippy-perf/src/lib.rs
git commit -m "feat: report latency sample count and maximum"
```

### Task 2: Measure Write-To-Subscribe At The Consumer Boundary

**Files:**
- Modify: `crates/zippy-perf/src/lib.rs:505-564`
- Replace: `crates/zippy-perf/src/lib.rs:1393-1468`
- Test: `crates/zippy-perf/src/lib.rs` test module

- [ ] **Step 1: Write failing completion-validation tests**

Introduce tests for each invalid result independently:

```rust
#[test]
fn write_to_subscribe_rejects_produced_consumed_mismatch() {
    let error = super::validate_write_to_subscribe_completion(10, 9, 8, 8, 8).unwrap_err();
    assert!(error.contains("produced_rows=[10]"));
    assert!(error.contains("consumed_rows=[9]"));
}

#[test]
fn write_to_subscribe_rejects_measured_sample_mismatch() {
    let error = super::validate_write_to_subscribe_completion(10, 10, 8, 8, 7).unwrap_err();
    assert!(error.contains("measured_consumed_rows=[8]"));
    assert!(error.contains("latency_samples=[7]"));
}

#[test]
fn write_to_subscribe_rejects_measured_row_mismatch() {
    let error = super::validate_write_to_subscribe_completion(10, 10, 8, 7, 7).unwrap_err();
    assert!(error.contains("measured_produced_rows=[8]"));
    assert!(error.contains("measured_consumed_rows=[7]"));
}
```

Add a timeout test using an empty active segment and a short explicit deadline:

```rust
#[test]
fn write_to_subscribe_consumer_reports_timeout_without_rows() {
    let (_writer, reader, state) = write_to_subscribe_fixture();
    let error = super::consume_write_to_subscribe(
        reader,
        Instant::now(),
        0,
        Instant::now() + Duration::from_millis(20),
        state,
    )
    .unwrap_err();

    assert!(error.contains("consumer timed out"));
}
```

The test helper must create a real `ActiveSegmentWriter`/`ActiveSegmentReader` pair and keep the writer alive for the whole timeout test:

```rust
fn write_to_subscribe_fixture(
) -> (
    ActiveSegmentWriter,
    ActiveSegmentReader,
    Arc<WriteToSubscribeProducerState>,
) {
    let schema = compile_schema(&[
        ColumnSpec::new("symbol", ColumnType::Utf8),
        ColumnSpec::new("sent_elapsed_ns", ColumnType::Int64),
        ColumnSpec::new("price", ColumnType::Float64),
        ColumnSpec::new("volume", ColumnType::Float64),
    ])
    .unwrap();
    let layout = LayoutPlan::for_schema(&schema, 4).unwrap();
    let writer = ActiveSegmentWriter::new_for_runtime(schema.clone(), layout.clone(), 10_101, 0)
        .unwrap();
    let envelope = writer.active_descriptor().to_envelope_bytes().unwrap();
    let reader = ActiveSegmentReader::from_descriptor_envelope(&envelope, schema, layout).unwrap();
    (
        writer,
        reader,
        Arc::new(WriteToSubscribeProducerState::default()),
    )
}
```

It must not spawn a thread.

- [ ] **Step 2: Run completion tests and verify RED**

Run:

```bash
cargo test -p zippy-perf write_to_subscribe_rejects -- --nocapture
cargo test -p zippy-perf tests::write_to_subscribe_consumer_reports_timeout_without_rows -- --exact
```

Expected: compilation fails because the validation, state, and consumer helpers do not exist.

- [ ] **Step 3: Add producer state and explicit count validation**

Add imports for `AtomicBool` and define private state/result types:

```rust
#[derive(Default)]
struct WriteToSubscribeProducerState {
    produced_rows: AtomicU64,
    measured_produced_rows: AtomicU64,
    done: AtomicBool,
    cancelled: AtomicBool,
}

struct WriteToSubscribeConsumerResult {
    consumed_rows: u64,
    measured_consumed_rows: u64,
    latency_samples_micros: Vec<u64>,
}

enum WriteToSubscribeProducerOutcome {
    Completed {
        produced_rows: u64,
        measured_produced_rows: u64,
    },
    CancelledByConsumer,
}
```

The validation function must reject, in this order:

```rust
fn validate_write_to_subscribe_completion(
    produced_rows: u64,
    consumed_rows: u64,
    measured_produced_rows: u64,
    measured_consumed_rows: u64,
    latency_samples: usize,
) -> PerfResult<()> {
    if produced_rows != consumed_rows {
        return Err(format!(
            "write-to-subscribe row mismatch produced_rows=[{produced_rows}] consumed_rows=[{consumed_rows}]"
        ));
    }
    if measured_produced_rows != measured_consumed_rows {
        return Err(format!(
            "write-to-subscribe measured row mismatch measured_produced_rows=[{measured_produced_rows}] measured_consumed_rows=[{measured_consumed_rows}]"
        ));
    }
    let latency_samples = u64::try_from(latency_samples).unwrap_or(u64::MAX);
    if measured_consumed_rows != latency_samples {
        return Err(format!(
            "write-to-subscribe sample mismatch measured_consumed_rows=[{measured_consumed_rows}] latency_samples=[{latency_samples}]"
        ));
    }
    Ok(())
}
```

- [ ] **Step 4: Implement the blocking consumer**

Change the benchmark schema to use an explicit monotonic field:

```rust
ColumnSpec::new("sent_elapsed_ns", ColumnType::Int64)
```

Do not reuse the UTC `dt` field for a monotonic duration.

The consumer loop must use the lost-wakeup-safe order:

```rust
if state.cancelled.load(Ordering::Acquire) {
    return Err("write-to-subscribe consumer cancelled".to_string());
}
let observed = reader.notification_sequence().map_err(|error| error.to_string())?;
if let Some(span) = reader.read_available().map_err(|error| error.to_string())? {
    // Read every row's sent_elapsed_ns and record consumer-observed latency.
    continue;
}
if state.done.load(Ordering::Acquire)
    && consumed_rows >= state.produced_rows.load(Ordering::Acquire)
{
    break;
}
let remaining = deadline.saturating_duration_since(Instant::now());
if remaining.is_zero() {
    state.cancelled.store(true, Ordering::Release);
    return Err("write-to-subscribe consumer timed out".to_string());
}
reader
    .wait_for_notification_after(observed, remaining.min(Duration::from_millis(10)))
    .map_err(|error| error.to_string())?;
```

Immediately after `read_available()` returns `Some(span)`, capture one boundary timestamp for the entire span:

```rust
let span_observed_elapsed_ns = u64::try_from(origin.elapsed().as_nanos())
    .map_err(|_| "write-to-subscribe observed elapsed nanoseconds overflowed u64".to_string())?;
```

Then use that same value for every row in the returned span. This prevents later rows from inheriting the parsing cost of earlier rows:

```rust
let sent_elapsed_ns = match span
    .cell_value(row_offset, "sent_elapsed_ns")
    .map_err(|error| error.to_string())?
{
    SegmentCellValue::Int64(value) if value >= 0 => u64::try_from(value)
        .map_err(|_| "sent_elapsed_ns overflowed u64".to_string())?,
    value => return Err(format!("invalid sent_elapsed_ns value=[{value:?}]")),
};
if sent_elapsed_ns >= warmup_end_ns {
    measured_consumed_rows += 1;
    latency_samples_micros.push(
        span_observed_elapsed_ns.saturating_sub(sent_elapsed_ns) / 1_000,
    );
}
consumed_rows += 1;
```

The 10 ms bounded wait exists only so producer completion is observed without a synthetic sentinel row. Row latency still uses the mmap notification wake. Wrap the whole consumer body in a closure; if any read, cell, wait, cancellation, or timeout error escapes, set `cancelled=true` before returning that original error:

```rust
let result = consume_write_to_subscribe_loop(
    &mut reader,
    origin,
    warmup_end_ns,
    deadline,
    Arc::clone(&state),
);
if result.is_err() {
    state.cancelled.store(true, Ordering::Release);
}
result
```

Keep the loop itself in `consume_write_to_subscribe_loop`; its signature is:

```rust
fn consume_write_to_subscribe_loop(
    reader: &mut ActiveSegmentReader,
    origin: Instant,
    warmup_end_ns: u64,
    deadline: Instant,
    state: Arc<WriteToSubscribeProducerState>,
) -> PerfResult<WriteToSubscribeConsumerResult>
```

- [ ] **Step 5: Replace the synchronous writer-reader driver**

Construct the reader from the writer descriptor, clone one reader as a control observer, and move the consuming reader into a dedicated thread. Before producing rows, wait until `observer.control_snapshot()?.waiter_count > 0` with a five-second startup deadline; this proves the profile exercises a blocking notification path without a narrow shared-runner timing assertion. Use this helper:

```rust
fn wait_for_write_to_subscribe_consumer(
    observer: &ActiveSegmentReader,
    timeout: Duration,
) -> PerfResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if observer
            .control_snapshot()
            .map_err(|error| error.to_string())?
            .waiter_count
            > 0
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("write-to-subscribe consumer did not enter mmap wait".to_string());
        }
        thread::yield_now();
    }
}
```

Call the helper with `Duration::from_secs(5)`.

Use one shared `Instant` origin. Each producer row records `origin.elapsed()` immediately before `begin_row()` and performs a checked conversion before writing:

```rust
let sent_elapsed_ns = i64::try_from(origin.elapsed().as_nanos()).map_err(|_| {
    "write-to-subscribe sent elapsed nanoseconds overflowed i64".to_string()
})?;
writer
    .write_i64("sent_elapsed_ns", sent_elapsed_ns)
    .map_err(str::to_string)?;
```

After commit, increment `produced_rows` with `Release` ordering. Rows whose non-negative `sent_elapsed_ns` is at or beyond the warmup boundary also increment `measured_produced_rows`. Check `cancelled` before sleeping, after sleeping, and before beginning each row so an early consumer failure stops production promptly. A shared-flag cancellation returns `Ok(WriteToSubscribeProducerOutcome::CancelledByConsumer)`; writer, schema, or timing failures retain their original `Err`.

Use this producer signature:

```rust
fn run_write_to_subscribe_producer(
    config: &PerfConfig,
    writer: &mut ActiveSegmentWriter,
    origin: Instant,
    warmup_end_ns: u64,
    state: Arc<WriteToSubscribeProducerState>,
) -> PerfResult<WriteToSubscribeProducerOutcome>
```

Set the consumer deadline to `origin + warmup_duration + steady_duration + Duration::from_secs(5)`. Orchestrate every exit through this order:

```rust
let producer_result = run_write_to_subscribe_producer(
    &profile_config,
    &mut writer,
    origin,
    warmup_end_ns,
    Arc::clone(&state),
);
if producer_result.is_err() {
    state.cancelled.store(true, Ordering::Release);
}
state.done.store(true, Ordering::Release);
let consumer_result = consumer_handle
    .join()
    .map_err(|_| "write-to-subscribe consumer panicked".to_string())?;
match (producer_result, consumer_result) {
    (Err(producer_error), _) => return Err(producer_error),
    (Ok(WriteToSubscribeProducerOutcome::CancelledByConsumer), Err(consumer_error)) => {
        return Err(consumer_error);
    }
    (Ok(WriteToSubscribeProducerOutcome::CancelledByConsumer), Ok(_)) => {
        return Err(
            "write-to-subscribe producer was cancelled without a consumer error".to_string(),
        );
    }
    (Ok(WriteToSubscribeProducerOutcome::Completed { .. }), Err(consumer_error)) => {
        return Err(consumer_error);
    }
    (
        Ok(WriteToSubscribeProducerOutcome::Completed {
            produced_rows,
            measured_produced_rows,
        }),
        Ok(consumer),
    ) => {
        validate_write_to_subscribe_completion(
            produced_rows,
            consumer.consumed_rows,
            measured_produced_rows,
            consumer.measured_consumed_rows,
            consumer.latency_samples_micros.len(),
        )?;
    }
}
```

If waiter startup fails, use the same cleanup sequence: set `cancelled=true`, set `done=true`, join the consumer, then return the startup error. Never return while the consumer thread is alive.

Build the report from independent facts:

```rust
input_rows_total: produced_rows,
output_rows_total: consumer.consumed_rows,
processed_rows_total: consumer.consumed_rows,
latency: build_latency_summary(&consumer.latency_samples_micros),
```

- [ ] **Step 6: Strengthen the live profile test**

Extend `low_latency_profiles_produce_nonzero_rows_and_pass` or split out a dedicated test:

```rust
assert_eq!(report.input_rows_total, report.output_rows_total);
assert_eq!(report.latency.sample_count, report.output_rows_total);
assert!(report.latency.max_micros >= report.latency.p99_micros);
assert!(report.pass);
```

Use `warmup_sec=0` in this test so every consumed row is a measured sample.

- [ ] **Step 7: Run Task 2 tests and full zippy-perf tests**

Run:

```bash
cargo test -p zippy-perf write_to_subscribe -- --nocapture
cargo test -p zippy-perf --lib
```

Expected: timeout and mismatch tests pass; the live profile reports equal produced/consumed/sample counts; no test thread survives completion.

- [ ] **Step 8: Commit Task 2**

```bash
git add crates/zippy-perf/src/lib.rs
git commit -m "fix: measure write-to-subscribe at consumer boundary"
```

### Task 3: Make In-Process Throughput And Backlog Honest

**Files:**
- Modify: `crates/zippy-perf/src/lib.rs:91-101`
- Modify: `crates/zippy-perf/src/lib.rs:237-289`
- Modify: `crates/zippy-perf/src/lib.rs:1255-1327`
- Modify: `crates/zippy-perf/src/lib.rs:1736-1752`
- Modify: `crates/zippy-perf/src/lib.rs:1793-1828`
- Test: `crates/zippy-perf/src/lib.rs` test module

- [ ] **Step 1: Write failing throughput, completeness, and queue-depth tests**

Add pure contract tests:

```rust
#[test]
fn drain_inclusive_throughput_uses_total_processing_elapsed_time() {
    assert_eq!(super::count_per_sec_over_elapsed(100, Duration::from_secs(2)), 50.0);
}

#[test]
fn inproc_completeness_rejects_unprocessed_accepted_rows() {
    let error = super::validate_inproc_completion(100, 99).unwrap_err();
    assert!(error.contains("accepted_rows=[100]"));
    assert!(error.contains("processed_rows=[99]"));
}
```

Change `evaluate_pass_rejects_queue_depth_above_configured_threshold` to set `max_observed_queue_depth=3` while final `queue_depth=0`. It must still fail.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test -p zippy-perf tests::drain_inclusive_throughput_uses_total_processing_elapsed_time -- --exact
cargo test -p zippy-perf tests::inproc_completeness_rejects_unprocessed_accepted_rows -- --exact
cargo test -p zippy-perf tests::evaluate_pass_rejects_queue_depth_above_configured_threshold -- --exact
```

Expected: all three tests fail to compile because the helpers and additive maximum-depth field do not exist yet. After Step 3 adds the field but before changing `evaluate_pass`, the queue-depth assertion remains RED because the gate still reads final `queue_depth=0`.

- [ ] **Step 3: Add additive maximum observed queue depth**

Preserve final queue semantics and add a new field:

```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineMetricsReport {
    pub processed_batches_total: u64,
    pub processed_rows_total: u64,
    pub output_batches_total: u64,
    pub dropped_batches_total: u64,
    pub late_rows_total: u64,
    pub publish_errors_total: u64,
    pub queue_depth: usize,
    #[serde(default)]
    pub max_observed_queue_depth: usize,
}
```

Populate zero/default values for synchronous profiles. Change `evaluate_pass` to compare `config.max_queue_depth` against `engine_metrics.max_observed_queue_depth`, not final `queue_depth`.

- [ ] **Step 4: Capture drive statistics needed after drain**

Extend `DriveStats` with:

```rust
steady_rows_total: u64,
steady_batches_total: u64,
steady_started_at: Instant,
max_observed_queue_depth: usize,
```

Change `drive_generator` so its callback returns the queue depth observed immediately after a successful admission:

```rust
F: FnMut(RecordBatch) -> PerfResult<usize>
```

Both current call sites must write the batch and then return `handle.metrics().queue_depth`. Track the maximum across warmup and steady drive so a warmup backlog is not hidden at the measurement boundary. Every other `DriveStats` constructor must initialize `steady_rows_total` and `steady_batches_total` from its existing steady counters, `steady_started_at` from its existing `warmup_end`, and `max_observed_queue_depth` to zero.

- [ ] **Step 5: Calculate drain-inclusive rate and enforce completeness**

Add helpers:

```rust
fn count_per_sec_over_elapsed(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(1e-9)
}

fn validate_inproc_completion(accepted_rows: u64, processed_rows: u64) -> PerfResult<()> {
    if accepted_rows != processed_rows {
        return Err(format!(
            "inproc row mismatch accepted_rows=[{accepted_rows}] processed_rows=[{processed_rows}]"
        ));
    }
    Ok(())
}
```

In `run_inproc_timeseries`, call `handle.flush()`, capture `drain_completed_at=Instant::now()`, then stop. Validate total accepted rows against `processed_rows_total`. Compute:

```rust
let observed_processing_elapsed = drain_completed_at
    .saturating_duration_since(drive_stats.steady_started_at);
let actual_average_rows_per_sec = count_per_sec_over_elapsed(
    drive_stats.steady_rows_total,
    observed_processing_elapsed,
);
let batches_per_sec = count_per_sec_over_elapsed(
    drive_stats.steady_batches_total,
    observed_processing_elapsed,
);
```

Use `batches_per_sec` in the report. Its numerator and denominator are both steady-only, so warmup output cannot inflate the rate. Set `engine_metrics.max_observed_queue_depth` from `DriveStats` before evaluating the pass gate.

- [ ] **Step 6: Strengthen the in-process profile test**

Add assertions:

```rust
assert_eq!(
    report.input_rows_total,
    report.engine_metrics.processed_rows_total
);
assert!(
    report.engine_metrics.max_observed_queue_depth
        >= report.engine_metrics.queue_depth
);
assert!(report.actual_average_rows_per_sec > 0.0);
```

Also add a legacy metrics JSON deserialization test showing the missing field becomes zero:

```rust
#[test]
fn engine_metrics_deserializes_legacy_json_without_observed_maximum() {
    let metrics: EngineMetricsReport = serde_json::from_str(
        r#"{
            "processed_batches_total":1,
            "processed_rows_total":2,
            "output_batches_total":1,
            "dropped_batches_total":0,
            "late_rows_total":0,
            "publish_errors_total":0,
            "queue_depth":0
        }"#,
    )
    .unwrap();

    assert_eq!(metrics.max_observed_queue_depth, 0);
}
```

- [ ] **Step 7: Run Task 3 tests and the full library suite**

Run:

```bash
cargo test -p zippy-perf drain_inclusive -- --nocapture
cargo test -p zippy-perf inproc -- --nocapture
cargo test -p zippy-perf tests::evaluate_pass_rejects_queue_depth_above_configured_threshold -- --exact
cargo test -p zippy-perf --lib
```

Expected: all tests pass; the in-process report proves accepted/processed equality and uses sampled backlog for the configured gate.

- [ ] **Step 8: Commit Task 3**

```bash
git add crates/zippy-perf/src/lib.rs
git commit -m "fix: report drain-inclusive in-process throughput"
```

### Task 4: Add The Scheduled Gate And Correct Documentation

**Files:**
- Modify: `.github/workflows/performance.yml:9-60`
- Modify: `crates/zippy-perf/README.md:20-105`

- [ ] **Step 1: Add the scheduled/manual write-to-subscribe command**

Give the job a bounded runtime:

```yaml
jobs:
  zippy-perf-smoke:
    runs-on: ubuntu-latest
    timeout-minutes: 30
```

Add this step after the existing stream-table profiles:

```yaml
- name: run write-to-subscribe profile
  env:
    TMPDIR: /dev/shm
  run: |
    cargo run -p zippy-perf --release -- write-to-subscribe-e2e \
      --rows-per-batch 1 \
      --target-rows-per-sec 200 \
      --duration-sec 10 \
      --warmup-sec 2 \
      --symbols 32 \
      --max-p95-micros 1000 \
      --max-p99-micros 5000 \
      --report-json target/zippy-perf-write-to-subscribe-e2e.json
```

These are shared-runner regression thresholds, not a production SLA. Count equality and timeout remain deterministic hard gates.

- [ ] **Step 2: Retain artifacts when a gate fails**

Change the existing upload step to:

```yaml
- name: upload performance reports
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: zippy-perf-reports
    path: target/zippy-perf-*.json
```

This preserves JSON generated before exit code 2 so a tail-latency threshold failure is diagnosable. Execution errors such as timeout/count mismatch currently exit before report construction; those remain visible in the workflow log and are not falsely described as uploaded JSON.

- [ ] **Step 3: Correct README measurement claims**

Document all of the following explicitly:

- `inproc-timeseries` primary latency is queue-admission latency, not completion latency.
- Its average throughput includes the final drain interval.
- `max_observed_queue_depth` is sampled immediately after successful admissions; it is not an atomic runtime high-water mark.
- `write-to-subscribe-e2e` uses separate producer/consumer threads and records latency only after `read_available()` exposes each committed row.
- The profile includes commit, mmap notification, blocking wake, scheduling, visibility, and read availability; it excludes Python conversion and callback execution.
- Shared-runner `1000/5000 us` gates only catch coarse regressions. The documented native Linux target remains `p99 <= 300 us` and must be calibrated/enforced on a fixed host.
- Old synchronous `write-to-subscribe-e2e` reports are not directly comparable to the corrected consumer-boundary series.

- [ ] **Step 4: Validate YAML text, docs, and tests**

Run:

```bash
rg -n "write-to-subscribe-e2e|max-p95-micros|max-p99-micros|if: always" \
  .github/workflows/performance.yml crates/zippy-perf/README.md
cargo test -p zippy-perf --lib
git diff --check
```

Expected: the workflow contains one corrected profile command with consistent YAML indentation; README names the true boundaries; tests and whitespace checks pass. This repository has no local `actionlint` or YAML parser dependency, so structural validation is completed when GitHub parses the workflow after push.

- [ ] **Step 5: Commit Task 4**

```bash
git add .github/workflows/performance.yml crates/zippy-perf/README.md
git commit -m "ci: gate consumer-observed stream latency"
```

### Task 5: Final Verification And Release Baseline

**Files:**
- Verify: `crates/zippy-perf/src/lib.rs`
- Verify: `crates/zippy-perf/README.md`
- Verify: `.github/workflows/performance.yml`
- Create: `docs/perf_reports/2026-07-11-low-latency-performance-gates.md`

- [ ] **Step 1: Run correctness, formatting, and lint gates**

Run:

```bash
cargo test -p zippy-perf --lib
cargo fmt --check
cargo clippy -p zippy-perf --all-targets -- -D warnings
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Run the corrected release profile**

Run:

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- \
  write-to-subscribe-e2e \
  --rows-per-batch 1 \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 2 \
  --symbols 32 \
  --max-p95-micros 1000 \
  --max-p99-micros 5000 \
  --report-json target/zippy-perf-write-to-subscribe-e2e.json
```

Expected: exit zero and `pass=true`.

- [ ] **Step 3: Validate report invariants mechanically**

Run:

```bash
jq -e '
  .pass == true
  and .input_rows_total == .output_rows_total
  and .latency.sample_count > 0
  and .latency.max_micros >= .latency.p99_micros
  and .engine_metrics.processed_rows_total == .output_rows_total
' target/zippy-perf-write-to-subscribe-e2e.json
```

Expected: `true` and exit zero.

- [ ] **Step 4: Run a short in-process semantic check**

Run:

```bash
cargo run -p zippy-perf --release -- inproc-timeseries \
  --rows-per-batch 4096 \
  --target-rows-per-sec 100000 \
  --duration-sec 3 \
  --warmup-sec 1 \
  --symbols 128 \
  --report-json target/zippy-perf-inproc-timeseries-drain.json
```

Then run:

```bash
jq -e '
  .pass == true
  and .input_rows_total == .engine_metrics.processed_rows_total
  and .engine_metrics.max_observed_queue_depth >= .engine_metrics.queue_depth
' target/zippy-perf-inproc-timeseries-drain.json
```

Expected: both commands exit zero.

- [ ] **Step 5: Commit the exact before/after evidence**

Use `apply_patch` to create `docs/perf_reports/2026-07-11-low-latency-performance-gates.md` with these sections:

1. Base commit `3ff850ade14dcbfbe9b636db94a9832aded20a87` and the exact command from Task 0.
2. The complete contents of `/tmp/zippy-perf-write-to-subscribe-before.txt` and the old JSON's input/output/p50/p95/p99 values.
3. The reviewed Phase 2 HEAD and the exact command from Step 2.
4. The complete corrected stdout plus input/output/sample_count/p50/p95/p99/max values from the new JSON.
5. The in-process command plus accepted/processed/final queue/max observed queue/average throughput values.
6. An explicit statement that the old synchronous and new blocking-consumer latency distributions are not numerically comparable; the comparison proves the measurement boundary changed and the corrected series becomes the new baseline.

Then commit only the report:

```bash
git add docs/perf_reports/2026-07-11-low-latency-performance-gates.md
git commit -m "docs: record consumer-boundary latency baseline"
```

- [ ] **Step 6: Request independent code review**

Review the complete range from the Phase 2 base commit through HEAD. The reviewer must prioritize:

- lost-wakeup safety and consumer timeout cleanup;
- produced/consumed/measured/sample count truthfulness;
- monotonic timestamp units;
- drain-inclusive throughput denominator;
- sampled queue-depth semantics;
- legacy JSON deserialization;
- workflow threshold scope and artifact retention.

No Phase 2 completion claim is allowed until Critical and Important findings are resolved and the final verification commands are rerun on the reviewed HEAD.
