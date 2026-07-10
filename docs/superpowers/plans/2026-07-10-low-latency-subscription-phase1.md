# Low-Latency Subscription Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make non-`xfast` row commits wake subscribers immediately and make a throttled partial table flush after silence without changing callback thread ownership.

**Architecture:** `SegmentReaderDriver` waits on the active mmap notification before any blocking descriptor wait and accepts a per-poll timeout. `_TableSubscriptionCallback` exposes a private relative deadline contract; the native subscriber caches that deadline, shortens its existing wait, and invokes due flushes on the same subscriber thread.

**Tech Stack:** Rust, PyO3 0.22, Python 3.11, PyArrow, pytest, Cargo, maturin, uv

---

## File Structure

- Modify `crates/zippy-python/src/lib.rs`: fix mmap/descriptor wait ordering, add dynamic idle timeout, and bridge the private Python flush deadline into the native subscriber loop.
- Modify `python/zippy/__init__.py`: add the private deadline calculation and due-flush operations to `_TableSubscriptionCallback`.
- Modify `pytests/test_python_api.py`: cover the adapter clock contract, silent single-batch delivery, exactly-once shutdown, and deadline callback failure propagation.

No public signature or stub file changes are required. The callback object itself carries the private deadline capability, so `_NativeStreamSubscriber` constructor compatibility is preserved.

### Task 1: Wake an idle reader from the mmap notification

**Files:**
- Modify: `crates/zippy-python/src/lib.rs:3603-3740`
- Test: `crates/zippy-python/src/lib.rs:9354`

- [ ] **Step 1: Write the failing real-shared-memory regression test**

Add this test after `segment_reader_driver_switches_descriptor_only_after_draining_current_rows`:

```rust
#[test]
fn segment_reader_driver_row_commit_wakes_before_idle_timeout() {
    let schema = compile_segment_schema(&[
        ColumnSpec::new("instrument_id", ColumnType::Utf8),
        ColumnSpec::new("last_price", ColumnType::Float64),
    ])
    .unwrap();
    let store = SegmentStore::new(SegmentStoreConfig {
        default_row_capacity: 4,
    })
    .unwrap();
    let partition = store
        .open_partition_with_schema("ticks", "mmap-wake", schema.clone())
        .unwrap();
    let descriptor: serde_json::Value = serde_json::from_slice(
        &partition.active_descriptor_envelope_bytes().unwrap(),
    )
    .unwrap();
    let (reader, _) = build_active_segment_reader(&descriptor, &schema).unwrap();
    let descriptor_updates = Arc::new(DescriptorUpdateSlot::default());
    let descriptor_refresh_error = Arc::new(Mutex::new(None));
    let mut driver = SegmentReaderDriver::new(
        ActiveSegmentReaderHandle {
            reader,
            reader_lease: None,
        },
        serde_json::to_string(&descriptor).unwrap(),
        schema,
        Arc::clone(&descriptor_updates),
        Arc::clone(&descriptor_refresh_error),
        false,
        Duration::from_millis(500),
        0,
        None,
    );
    let writer_partition = partition.clone();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        writer_partition
            .writer()
            .write_row(|row| {
                row.write_utf8("instrument_id", "IF2606")?;
                row.write_f64("last_price", 4102.5)?;
                Ok(())
            })
            .unwrap();
    });

    let wait_started_at = Instant::now();
    assert!(matches!(
        driver.poll_next().unwrap(),
        SegmentReaderDriverEvent::Idle
    ));
    let wait_elapsed = wait_started_at.elapsed();
    writer.join().unwrap();

    assert!(
        wait_elapsed < Duration::from_millis(300),
        "row notification did not interrupt idle wait elapsed=[{wait_elapsed:?}]"
    );
    match driver.poll_next().unwrap() {
        SegmentReaderDriverEvent::Rows(span) => assert_eq!(span.row_count(), 1),
        other => panic!("expected notified row after idle wake got [{other:?}]"),
    }
}
```

- [ ] **Step 2: Run the regression test and verify RED**

Run:

```bash
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python \
  cargo test -p zippy-python --lib \
  tests::segment_reader_driver_row_commit_wakes_before_idle_timeout -- --exact --nocapture
```

Expected: FAIL after roughly 500 ms with `row notification did not interrupt idle wait`.

- [ ] **Step 3: Add a per-poll timeout without changing the default caller**

Rename the existing `poll_next` implementation to `poll_next_with_idle_wait`, add the
`requested_idle_wait` parameter and `effective_idle_wait` first statement, then insert
this compatibility wrapper immediately above it:

```rust
fn poll_next(&mut self) -> std::result::Result<SegmentReaderDriverEvent, String> {
    self.poll_next_with_idle_wait(self.idle_wait)
}

fn poll_next_with_idle_wait(
    &mut self,
    requested_idle_wait: Duration,
) -> std::result::Result<SegmentReaderDriverEvent, String> {
    let effective_idle_wait = requested_idle_wait.min(self.idle_wait);
    let poll_started_at = Instant::now();
```

The remainder of the renamed function starts with its existing
`take_descriptor_refresh_error` check and remains in the same order. Close
`poll_next_with_idle_wait()` where the old `poll_next()` ended. Replace the two blocking
wait arguments inside that poll with `effective_idle_wait`. Do not change
`descriptor_refresh_interval()`, which continues to use the configured upper bound.

- [ ] **Step 4: Replace the unsealed double wait with the mmap-first protocol**

Replace the current unsealed
`wait_for_update_after(&self.descriptor_text, self.idle_wait)` followed by
`wait_for_mmap_notification_after(observed_notification_seq, Duration::ZERO)` block with:

```rust
let notified = self.wait_for_mmap_notification_after(
    observed_notification_seq,
    effective_idle_wait,
)?;
if notified {
    return Ok(SegmentReaderDriverEvent::Idle);
}
if let Some(error) = take_descriptor_refresh_error(self.descriptor_refresh_error.as_ref()) {
    if let Some(metrics) = self.metrics.as_ref() {
        metrics.record_descriptor_refresh_error();
    }
    return Err(error);
}
if let Some(descriptor_generation) = self.apply_ready_descriptor_updates()? {
    return Ok(SegmentReaderDriverEvent::DescriptorUpdated(
        descriptor_generation,
    ));
}
```

Use `effective_idle_wait` in the sealed-segment descriptor wait as well. Returning immediately when `notified` is true is required: the next outer poll must read and drain the old segment before applying a concurrently queued descriptor update.

- [ ] **Step 5: Run the focused and descriptor-order tests and verify GREEN**

Run:

```bash
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python \
  cargo test -p zippy-python --lib segment_reader_driver -- --nocapture
```

Expected: all `segment_reader_driver` tests PASS, including the new wake test and `segment_reader_driver_switches_descriptor_only_after_draining_current_rows`.

- [ ] **Step 6: Commit the mmap wake fix**

```bash
git add crates/zippy-python/src/lib.rs
git commit -m "fix: wake subscribers from segment row notifications"
```

### Task 2: Define the Python throttle deadline contract

**Files:**
- Modify: `python/zippy/__init__.py:6038-6107`
- Test: `pytests/test_python_api.py:9500`

- [ ] **Step 1: Write the failing deterministic adapter-clock test**

Add after `test_subscribe_table_throttle_ms_flushes_pending_rows`:

```python
def test_table_subscription_callback_flushes_only_when_deadline_is_due(monkeypatch) -> None:
    times = iter([0, 1_000_000, 4_000_000, 5_000_000, 11_000_000, 11_000_000])
    received: list[pa.Table] = []
    monkeypatch.setattr(zippy.time, "perf_counter_ns", lambda: next(times))
    adapter = zippy._TableSubscriptionCallback(
        received.append,
        batch_size=2,
        throttle_ms=10,
    )

    adapter(pa.table({"seq": [1]}))

    assert adapter._zippy_flush_deadline_enabled is True
    assert adapter._zippy_next_flush_delay_ns() == 6_000_000
    assert adapter._zippy_flush_due() == 5_000_000
    assert received == []
    assert adapter._zippy_flush_due() is None
    assert [table.column("seq").to_pylist() for table in received] == [[1]]
    assert adapter._zippy_flush_due() is None
```

- [ ] **Step 2: Run the adapter test and verify RED**

Run:

```bash
VIRTUAL_ENV=$PWD/.venv PYO3_PYTHON=$PWD/.venv/bin/python \
  /home/jiangda/develop/zippy/.venv/bin/maturin develop --uv
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest \
  pytests/test_python_api.py::test_table_subscription_callback_flushes_only_when_deadline_is_due -q
```

Expected: FAIL with missing `_zippy_flush_deadline_enabled` or `_zippy_next_flush_delay_ns`.

- [ ] **Step 3: Implement the private deadline operations**

In `_TableSubscriptionCallback.__init__`, add:

```python
self._zippy_flush_deadline_enabled = self._throttle_ms is not None
```

Add these methods after `_should_emit()`:

```python
def _zippy_next_flush_delay_ns(self) -> int | None:
    if self._throttle_ms is None or not self._pending:
        return None
    deadline_ns = self._last_emit_ns + self._throttle_ms * 1_000_000
    return max(0, deadline_ns - time.perf_counter_ns())

def _zippy_flush_due(self) -> int | None:
    delay_ns = self._zippy_next_flush_delay_ns()
    if delay_ns is None or delay_ns > 0:
        return delay_ns
    self.flush()
    return None
```

These methods must remain private. `flush()` remains the only operation that clears pending tables and invokes the user callback.

- [ ] **Step 4: Run the adapter tests and verify GREEN**

Run:

```bash
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest pytests/test_python_api.py \
  -k "subscribe_table_throttle_ms or table_subscription_callback_flushes_only" -q
```

Expected: both throttle adapter tests PASS.

- [ ] **Step 5: Commit the adapter contract**

```bash
git add python/zippy/__init__.py pytests/test_python_api.py
git commit -m "fix: expose table subscription flush deadline"
```

### Task 3: Schedule due flushes on the native subscriber thread

**Files:**
- Modify: `crates/zippy-python/src/lib.rs:4140-4295`
- Modify: `crates/zippy-python/src/lib.rs:4625-4636`
- Test: `pytests/test_python_api.py:14840`

- [ ] **Step 1: Build the current worktree extension into its isolated venv**

Run:

```bash
VIRTUAL_ENV=$PWD/.venv \
PYO3_PYTHON=$PWD/.venv/bin/python \
  /home/jiangda/develop/zippy/.venv/bin/maturin develop --uv
```

Expected: maturin installs `zippy` from this worktree into `.venv` without modifying the root project environment.

- [ ] **Step 2: Write the failing live silent-batch integration test**

Add after `test_subscribe_table_uses_default_master_and_invokes_callback_with_live_table`:

```python
def test_subscribe_table_flushes_silent_partial_batch_at_throttle_deadline(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    client = zippy.connect(uri=control_endpoint)
    client.register_process("subscriber_throttle_test")
    client.register_stream("throttled_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "throttled_ticks", {})
    writer = _segment_test_writer(client, "throttled_ticks", tick_schema, row_capacity=16)
    client.publish_segment_descriptor("throttled_ticks", writer.descriptor())
    received: list[pa.Table] = []
    delivered = threading.Event()

    def on_table(table: pa.Table) -> None:
        received.append(table)
        delivered.set()

    subscriber = zippy.subscribe_table(
        "throttled_ticks",
        callback=on_table,
        poll_interval_ms=2_000,
        batch_size=2,
        throttle_ms=250,
    )
    try:
        writer.append_tick(1777017600000000000, "IF2606", 4102.5)
        assert delivered.wait(timeout=1.25)
        time.sleep(0.1)
        assert len(received) == 1
        assert received[0].column("instrument_id").to_pylist() == ["IF2606"]
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()
```

- [ ] **Step 3: Run the live test and verify RED after Task 1**

Rebuild the extension, then run:

```bash
VIRTUAL_ENV=$PWD/.venv PYO3_PYTHON=$PWD/.venv/bin/python \
  /home/jiangda/develop/zippy/.venv/bin/maturin develop --uv
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest \
  pytests/test_python_api.py::test_subscribe_table_flushes_silent_partial_batch_at_throttle_deadline -q
```

Expected: FAIL because the partial batch remains pending and the native loop waits close to 2 seconds.

- [ ] **Step 4: Detect deadline-aware callbacks once at subscriber start**

Import `PyAttributeError` and add these private names near the subscriber helpers:

```rust
const CALLBACK_FLUSH_ENABLED_ATTR: &str = "_zippy_flush_deadline_enabled";
const CALLBACK_NEXT_FLUSH_DELAY_METHOD: &str = "_zippy_next_flush_delay_ns";
const CALLBACK_FLUSH_DUE_METHOD: &str = "_zippy_flush_due";
```

Add:

```rust
fn callback_flush_deadline_enabled(py: Python<'_>, callback: &Py<PyAny>) -> PyResult<bool> {
    match callback.bind(py).getattr(CALLBACK_FLUSH_ENABLED_ATTR) {
        Ok(value) => value.extract(),
        Err(error) if error.is_instance_of::<PyAttributeError>(py) => Ok(false),
        Err(error) => Err(error),
    }
}

fn callback_flush_delay(
    py: Python<'_>,
    callback: &Py<PyAny>,
    method: &str,
) -> PyResult<Option<Duration>> {
    callback
        .bind(py)
        .call_method0(method)?
        .extract::<Option<u64>>()
        .map(|delay| delay.map(Duration::from_nanos))
}

fn deadline_from_delay(delay: Option<Duration>) -> Option<Instant> {
    delay.map(|duration| Instant::now() + duration)
}
```

In `StreamSubscriber::start`, compute `flush_deadline_enabled` before spawning the thread. It is true only for table mode and when the private adapter attribute is true:

```rust
let flush_deadline_enabled = row_factory.is_none()
    && callback_flush_deadline_enabled(py, &callback)?;
```

- [ ] **Step 5: Return the next deadline from normal table delivery**

Change `invoke_stream_callback` to return `Option<Duration>`:

```rust
fn invoke_stream_callback(
    callback: &Py<PyAny>,
    batch: RecordBatch,
    flush_deadline_enabled: bool,
) -> std::result::Result<Option<Duration>, String> {
    Python::with_gil(|py| -> PyResult<Option<Duration>> {
        let table = record_batch_to_pyarrow_table(py, batch)?;
        callback.call1(py, (table,))?;
        if !flush_deadline_enabled {
            return Ok(None);
        }
        callback_flush_delay(py, callback, CALLBACK_NEXT_FLUSH_DELAY_METHOD)
    })
    .map_err(|error| error.to_string())
}

fn invoke_stream_callback_flush_due(
    callback: &Py<PyAny>,
) -> std::result::Result<Option<Duration>, String> {
    Python::with_gil(|py| callback_flush_delay(py, callback, CALLBACK_FLUSH_DUE_METHOD))
        .map_err(|error| error.to_string())
}
```

Update the sole `invoke_stream_callback` caller to pass `flush_deadline_enabled`, bind its
return value to `next_flush_delay`, and assign
`callback_flush_deadline = deadline_from_delay(next_flush_delay);`.

- [ ] **Step 6: Shorten the native wait and flush before blocking when due**

Add this state immediately before the subscriber `while` loop:

```rust
let mut callback_flush_deadline: Option<Instant> = None;
```

At the top of the loop, before calling the driver:

```rust
if callback_flush_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
    callback_flush_deadline = deadline_from_delay(invoke_stream_callback_flush_due(
        &callback,
    )?);
    continue;
}
let effective_idle_wait = callback_flush_deadline
    .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    .unwrap_or(poll_interval)
    .min(poll_interval);
```

Call `driver.poll_next_with_idle_wait(effective_idle_wait)`. In the table-row branch, replace the old callback call with:

```rust
let next_flush_delay = invoke_stream_callback(
    &callback,
    batch,
    flush_deadline_enabled,
)?;
callback_flush_deadline = deadline_from_delay(next_flush_delay);
```

Do not add deadline calls to row mode. Do not create a timer thread.

- [ ] **Step 7: Rebuild and verify the live test is GREEN**

Run:

```bash
VIRTUAL_ENV=$PWD/.venv PYO3_PYTHON=$PWD/.venv/bin/python \
  /home/jiangda/develop/zippy/.venv/bin/maturin develop --uv
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest \
  pytests/test_python_api.py::test_subscribe_table_flushes_silent_partial_batch_at_throttle_deadline -q
```

Expected: PASS in substantially less than the 2-second polling interval and exactly one table is delivered.

- [ ] **Step 8: Commit the native deadline scheduler**

```bash
git add crates/zippy-python/src/lib.rs pytests/test_python_api.py
git commit -m "fix: flush throttled table subscriptions on deadline"
```

### Task 4: Preserve failure and shutdown semantics

**Files:**
- Test: `pytests/test_python_api.py:14900`

- [ ] **Step 1: Add a deadline-callback failure integration test**

Add this complete test after the silent-partial-batch integration test:

```python
def test_subscribe_table_deadline_callback_error_stops_subscriber(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    client = zippy.connect(uri=control_endpoint)
    client.register_process("subscriber_throttle_error_test")
    client.register_stream("throttle_error_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "throttle_error_ticks", {})
    writer = _segment_test_writer(
        client,
        "throttle_error_ticks",
        tick_schema,
        row_capacity=16,
    )
    client.publish_segment_descriptor("throttle_error_ticks", writer.descriptor())

    def fail_on_table(_table: pa.Table) -> None:
        raise ValueError("deadline callback failed")

    subscriber = zippy.subscribe_table(
        "throttle_error_ticks",
        callback=fail_on_table,
        poll_interval_ms=500,
        batch_size=2,
        throttle_ms=50,
    )
    try:
        writer.append_tick(1777017600000000000, "IF2606", 4102.5)
        deadline = time.time() + 2.0
        metrics = subscriber.metrics()
        while metrics["running"] is True and time.time() < deadline:
            time.sleep(0.01)
            metrics = subscriber.metrics()

        assert metrics["running"] is False
        assert "deadline callback failed" in str(metrics["last_error"])
        with pytest.raises(RuntimeError, match="deadline callback failed"):
            subscriber.stop()
        subscriber = None
    finally:
        if subscriber is not None:
            try:
                subscriber.stop()
            except RuntimeError:
                pass
        if reset_default_master is not None:
            reset_default_master()
        server.stop()
```

- [ ] **Step 2: Run the failure test**

Run:

```bash
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest \
  pytests/test_python_api.py::test_subscribe_table_deadline_callback_error_stops_subscriber -q
```

Expected: PASS, proving that a due-flush exception follows the normal native subscriber failure path.

- [ ] **Step 3: Run the full targeted subscription regression set**

Run:

```bash
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python \
  cargo test -p zippy-python --lib
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest pytests/test_python_api.py \
  -k "subscribe_table or segment_subscribe" -q
```

Expected: all selected Rust and Python tests PASS. Run the Rust command outside the sandbox when loopback master tests return `Operation not permitted`.

- [ ] **Step 4: Commit the failure-semantic coverage**

```bash
git add pytests/test_python_api.py
git commit -m "test: cover throttled subscriber callback failure"
```

### Task 5: Verify Phase 1 subscriber correctness

**Files:**
- Verify: `crates/zippy-python/src/lib.rs`
- Verify: `python/zippy/__init__.py`
- Verify: `pytests/test_python_api.py`

- [ ] **Step 1: Format touched code**

```bash
cargo fmt --all
uv run --project /home/jiangda/develop/zippy --no-sync \
  black --line-length 100 python/zippy/__init__.py pytests/test_python_api.py
```

Expected: commands exit 0 and only touched formatting changes appear.

- [ ] **Step 2: Run lint and type checks**

```bash
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python \
  cargo clippy -p zippy-python --all-targets -- -D warnings
uv run --project /home/jiangda/develop/zippy --no-sync \
  ruff check python/zippy/__init__.py pytests/test_python_api.py
uv run --project /home/jiangda/develop/zippy --no-sync mypy .
```

Expected: all commands exit 0. If project-wide mypy exposes a pre-existing failure, record the exact failure separately and still require touched-file checks to pass.

- [ ] **Step 3: Run final Rust and Python verification from fresh builds**

```bash
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python \
  cargo test -p zippy-python --lib
VIRTUAL_ENV=$PWD/.venv PYO3_PYTHON=$PWD/.venv/bin/python \
  /home/jiangda/develop/zippy/.venv/bin/maturin develop --uv
PYTHONPATH=/home/jiangda/develop/zippy/.venv/lib/python3.11/site-packages \
  .venv/bin/python -m pytest pytests/test_python_api.py \
  -k "subscribe_table or subscribe_filters_instrument_ids" -q
```

Expected: all commands exit 0 with no failed tests.

- [ ] **Step 4: Inspect the final diff and worktree state**

```bash
git diff --check
git status --short --branch
git log --oneline --decorate -6
```

Expected: no whitespace errors, no untracked source files, and only intentional Phase 1 commits after the two design commits.

- [ ] **Step 5: Record the handoff to the performance-gate plan**

Capture the focused wake-test elapsed time and the live throttle test elapsed time in the execution notes. The next plan will use those results to define and implement the two-thread `write-to-subscribe-e2e` profile and truthful in-process backlog metrics.
