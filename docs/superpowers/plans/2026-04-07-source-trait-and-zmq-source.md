# Source Trait and ZmqSource Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an event-driven `Source` abstraction plus `ZmqSource`, so remote engine outputs can drive local engines in `pipeline` or `consumer` mode without changing existing `Engine` semantics.

**Architecture:** Add a new source layer in `zippy-core` with `Source`, `SourceEvent`, and a source-aware runtime entrypoint. Then implement a single-stream, single-channel multipart envelope protocol in `zippy-io`, expose `ZmqSource` and `SourceMode` through PyO3, add a standalone `ZmqStreamPublisher` helper for remote publishing, and wire `TimeSeriesEngine` / `CrossSectionalEngine` to accept remote sources while preserving current in-process chaining.

**Tech Stack:** Rust, Arrow `RecordBatch`, ZeroMQ, PyO3, pytest, cargo test, cargo clippy

**Implementation note:** The current worktree already contains unrelated modified files. Every commit in this plan must stage explicit paths only; do not use `git add -A`.

---

## File Structure

**Core runtime and source abstractions**
- Create: `crates/zippy-core/src/source.rs`
  Purpose: Define `Source`, `SourceEvent`, `SourceMode`, `StreamHello`, `SourceHandle`, and `SourceSink`.
- Modify: `crates/zippy-core/src/lib.rs`
  Purpose: Export the new source module types.
- Modify: `crates/zippy-core/src/runtime.rs`
  Purpose: Add `spawn_source_engine_with_publisher(...)` and source-driven event mapping without breaking existing engine runtime.
- Create: `crates/zippy-core/tests/source_runtime.rs`
  Purpose: Lock event-driven source runtime semantics, including `pipeline` and `consumer` mode behavior.

**I/O protocol and transport**
- Modify: `crates/zippy-io/src/zmq.rs`
  Purpose: Add multipart envelope encoding/decoding, `ZmqSource`, and stream protocol support while preserving existing batch-only publisher/subscriber helpers.
- Modify: `crates/zippy-io/src/lib.rs`
  Purpose: Export `ZmqSource` and protocol-side types if needed.
- Create: `crates/zippy-io/tests/zmq_source.rs`
  Purpose: Lock protocol behavior: `HELLO`, `DATA`, `FLUSH`, `STOP`, schema mismatch, and mode-aware event forwarding.

**Python binding layer**
- Modify: `crates/zippy-python/src/lib.rs`
  Purpose: Expose `ZmqSource`, `SourceMode`, source protocol wiring, and extend engine constructors to accept remote sources.
- Modify: `python/zippy/__init__.py`
  Purpose: Export `ZmqSource` and `SourceMode`.
- Modify: `python/zippy/_internal.pyi`
  Purpose: Add type declarations for `ZmqSource`, `SourceMode`, and expanded `source=` signatures.
- Modify: `pytests/test_python_api.py`
  Purpose: Add end-to-end tests for remote source pipeline and consumer semantics.

**Examples and docs**
- Create: `examples/python/remote_pipeline.py`
  Purpose: Show one process publishing bars and another consuming them through `ZmqSource`.
- Modify: `examples/python/README.md`
  Purpose: Document the remote-source example and the distinction between `pipeline` and `consumer`.

---

### Task 1: Add Source Core Types and Lock Runtime Semantics

**Files:**
- Create: `crates/zippy-core/src/source.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Modify: `crates/zippy-core/src/runtime.rs`
- Create: `crates/zippy-core/tests/source_runtime.rs`

- [ ] **Step 1: Write the failing source runtime tests**

Add tests that lock:

- `pipeline` mode maps `Flush` to `engine.on_flush()`
- `consumer` mode ignores `Flush`
- `pipeline` mode maps `Stop` to `engine.on_stop()`
- `Data` events still drive `engine.on_data()`
- `Hello` must be accepted before `Data`

Use this test skeleton:

```rust
#[test]
fn pipeline_source_runtime_forwards_flush_into_engine() {
    let source = StaticSource::new(
        SourceMode::Pipeline,
        vec![
            SourceEvent::Hello(StreamHello::new("bars", TEST_SCHEMA.clone(), 1).unwrap()),
            SourceEvent::Data(test_batch()),
            SourceEvent::Flush,
            SourceEvent::Stop,
        ],
    );
    let engine = RecordingEngine::new(TEST_SCHEMA.clone(), TEST_SCHEMA.clone());

    let mut handle = spawn_source_engine_with_publisher(
        Box::new(source),
        engine,
        test_engine_config("pipeline"),
        NoopPublisher::default(),
    )
    .unwrap();

    handle.stop().unwrap();

    let events = handle.metrics();
    assert_eq!(events.output_batches_total, 2);
}
```

- [ ] **Step 2: Run the targeted test to confirm red**

Run:

```bash
cargo test -p zippy-core pipeline_source_runtime_forwards_flush_into_engine -- --exact
```

Expected: FAIL because `source.rs` and `spawn_source_engine_with_publisher(...)` do not exist yet.

- [ ] **Step 3: Add the source core types**

Create `crates/zippy-core/src/source.rs` with this initial shape:

```rust
use std::sync::Arc;
use std::thread::JoinHandle;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::{Result, ZippyError};

pub type SchemaRef = Arc<Schema>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceMode {
    Pipeline,
    Consumer,
}

#[derive(Clone, Debug)]
pub struct StreamHello {
    pub protocol_version: u16,
    pub stream_name: String,
    pub schema: SchemaRef,
    pub schema_hash: String,
}

impl StreamHello {
    pub fn new(stream_name: &str, schema: SchemaRef, protocol_version: u16) -> Result<Self> {
        let schema_hash = format!("{:x}", md5::compute(schema.to_string().as_bytes()));
        Ok(Self {
            protocol_version,
            stream_name: stream_name.to_string(),
            schema,
            schema_hash,
        })
    }
}

pub enum SourceEvent {
    Hello(StreamHello),
    Data(RecordBatch),
    Flush,
    Stop,
    Error(String),
}

pub trait SourceSink: Send + Sync + 'static {
    fn emit(&self, event: SourceEvent) -> Result<()>;
}

pub struct SourceHandle {
    join_handle: Option<JoinHandle<Result<()>>>,
}

impl SourceHandle {
    pub fn new(join_handle: JoinHandle<Result<()>>) -> Self {
        Self {
            join_handle: Some(join_handle),
        }
    }

    pub fn join(&mut self) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => join_handle.join().map_err(|_| ZippyError::Io {
                reason: "source thread panicked".to_string(),
            })?,
            None => Ok(()),
        }
    }
}

pub trait Source: Send + 'static {
    fn name(&self) -> &str;
    fn output_schema(&self) -> SchemaRef;
    fn mode(&self) -> SourceMode;
    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle>;
}
```

- [ ] **Step 4: Export the source module**

Update `crates/zippy-core/src/lib.rs` to export the new module:

```rust
pub mod source;

pub use source::{
    Source, SourceEvent, SourceHandle, SourceMode, SourceSink, StreamHello,
};
```

- [ ] **Step 5: Add the source-aware runtime entrypoint**

Extend `crates/zippy-core/src/runtime.rs` with:

- a source event queue
- a `SourceRuntimeSink` implementation
- `spawn_source_engine_with_publisher(...)`

Use this shape:

```rust
pub fn spawn_source_engine_with_publisher<S, E, P>(
    source: Box<S>,
    mut engine: E,
    config: EngineConfig,
    mut publisher: P,
) -> Result<EngineHandle>
where
    S: Source + ?Sized,
    E: Engine,
    P: Publisher,
{
    config.validate()?;
    // build a bounded queue of source events
    // launch source.start(...)
    // launch engine worker that consumes SourceEvent and maps it into engine lifecycle
}
```

Keep the existing `spawn_engine_with_publisher(...)` path intact.

- [ ] **Step 6: Re-run targeted source runtime tests**

Run:

```bash
cargo test -p zippy-core source_runtime -- --nocapture
```

Expected: PASS with tests covering `pipeline` and `consumer`.

- [ ] **Step 7: Commit the source runtime core**

Run:

```bash
git add \
  crates/zippy-core/src/source.rs \
  crates/zippy-core/src/lib.rs \
  crates/zippy-core/src/runtime.rs \
  crates/zippy-core/tests/source_runtime.rs
git commit -m "feat: add source runtime core"
```

---

### Task 2: Add Multipart Envelope Protocol and ZmqSource

**Files:**
- Modify: `crates/zippy-io/src/zmq.rs`
- Modify: `crates/zippy-io/src/lib.rs`
- Create: `crates/zippy-io/tests/zmq_source.rs`

- [ ] **Step 1: Write the failing protocol tests**

Add tests that lock:

- `HELLO` encodes and decodes schema correctly
- `DATA` carries Arrow IPC payload
- `FLUSH` and `STOP` decode without payload
- `ZmqSource` rejects `DATA` before `HELLO`
- schema mismatch fails fast

Use this test skeleton:

```rust
#[test]
fn zmq_source_rejects_data_before_hello() {
    let endpoint = test_endpoint();
    let mut publisher = ZmqStreamPublisher::bind(&endpoint, "bars", TEST_SCHEMA.clone()).unwrap();
    publisher.publish_data(&test_batch()).unwrap();

    let source = ZmqSource::connect(
        "bars_source",
        &endpoint,
        TEST_SCHEMA.clone(),
        SourceMode::Pipeline,
    )
    .unwrap();

    let error = run_source_until_failure(source).unwrap_err();
    assert!(error.to_string().contains("hello"));
}
```

- [ ] **Step 2: Run the targeted protocol test to confirm red**

Run:

```bash
cargo test -p zippy-io zmq_source_rejects_data_before_hello -- --exact
```

Expected: FAIL because `ZmqSource` and stream envelope helpers do not exist yet.

- [ ] **Step 3: Add the multipart envelope protocol**

Extend `crates/zippy-io/src/zmq.rs` with:

- protocol constants for `HELLO`, `DATA`, `FLUSH`, `STOP`, `ERROR`
- helpers for `meta` serialization
- schema IPC encode/decode helpers
- `ZmqStreamPublisher`
- `ZmqSource`

Use this envelope shape:

```rust
enum MessageKind {
    Hello,
    Data,
    Flush,
    Stop,
    Error,
}

struct EnvelopeMeta {
    protocol_version: u16,
    stream_name: String,
    seq_no: Option<u64>,
    schema_hash: Option<String>,
    message: Option<String>,
}
```

`ZmqStreamPublisher` should:

- send `HELLO` on startup
- send `DATA` batches
- repeat `FLUSH` and `STOP` three times

`ZmqSource` should:

- block `DATA` until a valid `HELLO` is accepted
- compare incoming schema to `expected_schema`
- emit `SourceEvent`

- [ ] **Step 4: Export the new source transport types**

Update `crates/zippy-io/src/lib.rs` to export:

```rust
pub use zmq::{ZmqSource, ZmqStreamPublisher};
```

- [ ] **Step 5: Re-run the ZMQ source tests**

Run:

```bash
cargo test -p zippy-io zmq_source -- --nocapture
```

Expected: PASS with protocol and source behavior locked.

- [ ] **Step 6: Commit the ZMQ source protocol layer**

Run:

```bash
git add \
  crates/zippy-io/src/zmq.rs \
  crates/zippy-io/src/lib.rs \
  crates/zippy-io/tests/zmq_source.rs
git commit -m "feat: add zmq source protocol"
```

---

### Task 3: Expose ZmqSource and SourceMode in Python

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing Python API tests**

Add tests that lock:

- `zippy.ZmqSource(...)` exists
- `zippy.SourceMode.PIPELINE` / `CONSUMER` exist
- `TimeSeriesEngine(source=zippy.ZmqSource(...))` accepts remote source
- `CrossSectionalEngine(source=zippy.ZmqSource(...))` accepts remote source

Use this test skeleton:

```python
def test_zmq_source_pipeline_can_drive_cross_sectional_engine() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:7101",
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    engine = zippy.CrossSectionalEngine(
        name="cs_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.NullPublisher(),
    )
    assert engine.output_schema().names == ["symbol", "window_start", "ret_rank"]
```

- [ ] **Step 2: Run targeted pytest to confirm red**

Run:

```bash
uv run --extra dev pytest pytests/test_python_api.py -k 'zmq_source or source_mode' -v
```

Expected: FAIL because `ZmqSource` and `SourceMode` are not exposed yet.

- [ ] **Step 3: Add Python constants and class exports**

In `crates/zippy-python/src/lib.rs`, add:

- `SourceMode` constants mirroring the existing policy-constant pattern
- `ZmqSource` PyO3 class
- source parsing helpers that accept `ZmqSource`

Use this shape:

```rust
#[pyclass]
struct ZmqSource {
    endpoint: String,
    expected_schema: Arc<Schema>,
    mode: SourceMode,
}
```

Extend `python/zippy/__init__.py` with:

```python
class SourceMode:
    PIPELINE = _PolicyConstant(
        kind="source_mode",
        value="pipeline",
        name="SourceMode.PIPELINE",
    )
    CONSUMER = _PolicyConstant(
        kind="source_mode",
        value="consumer",
        name="SourceMode.CONSUMER",
    )
```

Add matching declarations to `python/zippy/_internal.pyi`.

- [ ] **Step 4: Extend engine constructors to accept remote sources**

Update Python binding logic so:

- `TimeSeriesEngine` accepts local engine sources or `ZmqSource`
- `CrossSectionalEngine` accepts local `TimeSeriesEngine` or `ZmqSource`
- source/output schema checks remain strict

Do not expand `ReactiveStateEngine` to accept `ZmqSource` in this first pass.
Expose `ZmqStreamPublisher` in Python as a standalone publisher helper so end-to-end remote source tests and examples can drive the protocol without waiting for engine target integration.

- [ ] **Step 5: Add end-to-end Python tests for pipeline and consumer mode**

In `pytests/test_python_api.py`, add tests that cover:

- remote `pipeline` source driving a local `CrossSectionalEngine`
- remote `consumer` source ignoring `Flush`
- schema mismatch raising a clear error

- [ ] **Step 6: Re-run targeted Python tests**

Run:

```bash
uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml
uv run --extra dev pytest pytests/test_python_api.py -k 'zmq_source or source_mode' -v
```

Expected: PASS with remote source API coverage.

- [ ] **Step 7: Commit the Python remote source layer**

Run:

```bash
git add \
  crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py \
  python/zippy/_internal.pyi \
  pytests/test_python_api.py
git commit -m "feat: expose zmq source in python"
```

---

### Task 4: Add End-to-End Remote Pipeline Example

**Files:**
- Create: `examples/python/remote_pipeline.py`
- Modify: `examples/python/README.md`

- [ ] **Step 1: Add the failing example check**

Run:

```bash
uv run python -m py_compile examples/python/remote_pipeline.py
```

Expected: FAIL because the example file does not exist yet.

- [ ] **Step 2: Add the remote pipeline example**

Create `examples/python/remote_pipeline.py` that demonstrates:

- upstream process behavior:
  - `TimeSeriesEngine -> ZmqStreamPublisher`
- downstream process behavior:
  - `ZmqSource(mode=PIPELINE) -> CrossSectionalEngine`

Use this script structure:

```python
def run_upstream() -> None:
    ...

def run_downstream() -> None:
    ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["upstream", "downstream"])
    ...
```

This keeps the example single-file and avoids having to coordinate two separate example files.

- [ ] **Step 3: Document pipeline vs consumer mode in README**

Extend `examples/python/README.md` with:

- how to launch upstream and downstream
- when to choose `pipeline`
- when to choose `consumer`
- the fact that `consumer` ignores upstream `Flush`

- [ ] **Step 4: Re-run example syntax check**

Run:

```bash
uv run python -m py_compile examples/python/remote_pipeline.py
uv run python -m py_compile examples/python/*.py
```

Expected: PASS

- [ ] **Step 5: Commit the example and docs**

Run:

```bash
git add \
  examples/python/remote_pipeline.py \
  examples/python/README.md
git commit -m "docs: add remote source pipeline example"
```

---

### Task 5: Full Verification and Final Scope Check

**Files:**
- Modify: `docs/superpowers/specs/2026-04-07-source-trait-and-zmq-source-design.md`
- Modify: `docs/superpowers/plans/2026-04-07-source-trait-and-zmq-source.md`

- [ ] **Step 1: Re-read the spec and verify plan coverage**

Check each spec requirement against code/tests:

- `Source` trait exists
- runtime is event-driven
- `SourceMode.PIPELINE` / `CONSUMER`
- single endpoint / single stream / fixed schema
- multipart envelope protocol
- `Hello / Data / Flush / Stop / Error`
- `ZmqSource`
- strict `expected_schema`
- `pipeline` forwards `Flush/Stop`
- `consumer` ignores `Flush`
- Python binding and constants

If any item is missing, update the implementation before final verification.

- [ ] **Step 2: Run full project verification**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo bench -p zippy-engines --no-run
uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml
uv run --extra dev pytest pytests -v
uv run python -m py_compile examples/python/*.py
```

Expected: PASS

- [ ] **Step 3: Commit any final spec/plan adjustments**

Run:

```bash
git add \
  docs/superpowers/specs/2026-04-07-source-trait-and-zmq-source-design.md \
  docs/superpowers/plans/2026-04-07-source-trait-and-zmq-source.md
git commit -m "docs: finalize source trait plan"
```
