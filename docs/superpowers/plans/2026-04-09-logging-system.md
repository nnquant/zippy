# Unified Logging System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a unified structured logging system for `zippy` with explicit `zippy.setup_log(...)`, console text output, per-run JSONL file output, and first-class integration across runtime, Python bridge, perf, and OpenCTP live source paths.

**Architecture:** Logging is initialized once in `zippy-core` using `tracing` and `tracing-subscriber`. The system emits readable console logs and managed JSONL files under `logs/<app>/<date>_<run_id>.jsonl`, with a required English `message` field plus structured metadata. `zippy-python` exposes the logging setup API, while `zippy-core`, `zippy-perf`, and `zippy-openctp` emit events through the shared Rust stack.

**Tech Stack:** Rust `tracing`, `tracing-subscriber`, `serde_json`, PyO3, Python editable install via `maturin`, pytest, cargo test/check.

---

## File Map

- Create: `crates/zippy-core/src/logging.rs`
  - owns log config, run id generation, subscriber initialization, file path creation, and logging snapshot
- Modify: `crates/zippy-core/src/lib.rs`
  - exports the logging module API
- Modify: `Cargo.toml`
  - adds workspace dependencies for `tracing`, `tracing-subscriber`, and `serde_json`
- Modify: `crates/zippy-core/Cargo.toml`
  - consumes logging dependencies
- Modify: `crates/zippy-python/src/lib.rs`
  - exposes `setup_log()` to Python and logs Python bridge/runtime events
- Modify: `python/zippy/_internal.pyi`
  - types `setup_log()`
- Modify: `python/zippy/__init__.py`
  - re-exports `setup_log`
- Modify: `crates/zippy-core/src/runtime.rs`
  - replaces `eprintln!` stop diagnostics and adds runtime/source lifecycle logs
- Modify: `crates/zippy-perf/src/main.rs`
  - replaces plain output error reporting with tracing-backed logs while preserving report printing
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs`
  - replaces `openctp_debug` raw prints with tracing events and structured fields
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/driver_ctp.rs`
  - adds live driver lifecycle logs
- Test: `crates/zippy-core/tests/logging.rs`
  - Rust coverage for initialization, file creation, JSONL fields, and duplicate initialization behavior
- Test: `pytests/test_python_api.py`
  - Python coverage for `setup_log()` return value and file output

## Task 1: Add core logging module and dependencies

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/zippy-core/Cargo.toml`
- Create: `crates/zippy-core/src/logging.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Test: `crates/zippy-core/tests/logging.rs`

- [ ] **Step 1: Add the failing Rust test scaffold for logging initialization**

```rust
#[test]
fn setup_log_creates_jsonl_file_and_returns_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = zippy_core::setup_log(zippy_core::LogConfig::new(
        "runtime_test",
        "info",
        temp.path(),
        true,
        true,
    ))
    .unwrap();

    assert_eq!(snapshot.app, "runtime_test");
    assert!(snapshot.file_path.is_some());
}
```

- [ ] **Step 2: Run the test to verify the module does not exist yet**

Run: `cargo test -p zippy-core --test logging -v`
Expected: FAIL with missing `logging.rs`, missing `setup_log`, or missing `LogConfig`

- [ ] **Step 3: Add workspace dependencies**

```toml
[workspace.dependencies]
arrow = "53.3.0"
crossbeam-channel = "0.5.15"
parquet = "53.3.0"
thiserror = "2.0.12"
tracing = "0.1.41"
tracing-subscriber = { version = "0.3.19", features = ["fmt", "json", "env-filter"] }
serde_json = "1.0.140"
```
```

- [ ] **Step 4: Wire `zippy-core` to consume the new dependencies**

```toml
[dependencies]
arrow.workspace = true
crossbeam-channel.workspace = true
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
serde_json.workspace = true
```

- [ ] **Step 5: Add the logging module with configuration and snapshot types**

```rust
pub struct LogConfig {
    pub app: String,
    pub level: String,
    pub log_dir: PathBuf,
    pub to_console: bool,
    pub to_file: bool,
}

pub struct LogSnapshot {
    pub app: String,
    pub level: String,
    pub run_id: String,
    pub file_path: Option<PathBuf>,
}

pub fn setup_log(config: LogConfig) -> Result<LogSnapshot> {
    // initialize tracing exactly once
}
```

- [ ] **Step 6: Export the logging API from `zippy-core`**

```rust
pub mod logging;

pub use logging::{setup_log, LogConfig, LogSnapshot};
```

- [ ] **Step 7: Run the Rust logging test and make it pass**

Run: `cargo test -p zippy-core --test logging -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml crates/zippy-core/Cargo.toml crates/zippy-core/src/logging.rs crates/zippy-core/src/lib.rs crates/zippy-core/tests/logging.rs
git commit -m "feat: add core logging module"
```

## Task 2: Implement JSONL file output and duplicate initialization rules

**Files:**
- Modify: `crates/zippy-core/src/logging.rs`
- Test: `crates/zippy-core/tests/logging.rs`

- [ ] **Step 1: Add failing tests for file naming and repeat setup behavior**

```rust
#[test]
fn setup_log_uses_date_and_run_id_in_file_name() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot = zippy_core::setup_log(LogConfig::new("gateway", "info", temp.path(), false, true)).unwrap();
    let file_path = snapshot.file_path.unwrap();
    let file_name = file_path.file_name().unwrap().to_string_lossy();
    assert!(file_name.ends_with(".jsonl"));
    assert!(file_name.contains('_'));
}

#[test]
fn setup_log_reuses_equivalent_config_and_rejects_different_config() {
    let temp = tempfile::tempdir().unwrap();
    let first = zippy_core::setup_log(LogConfig::new("gateway", "info", temp.path(), true, true)).unwrap();
    let second = zippy_core::setup_log(LogConfig::new("gateway", "info", temp.path(), true, true)).unwrap();
    assert_eq!(first.run_id, second.run_id);
}
```

- [ ] **Step 2: Run the logging tests to verify the new expectations fail**

Run: `cargo test -p zippy-core --test logging -v`
Expected: FAIL on file naming and duplicate initialization behavior

- [ ] **Step 3: Implement per-run file creation and run id generation**

```rust
let date = chrono_like_utc_date_string();
let run_id = short_random_run_id();
let file_path = log_dir.join(app).join(format!("{date}_{run_id}.jsonl"));
```

- [ ] **Step 4: Implement one-time initialization semantics**

```rust
static LOG_STATE: OnceLock<InitializedLogState> = OnceLock::new();

match LOG_STATE.get() {
    Some(existing) if existing.config_equivalent(&config) => Ok(existing.snapshot()),
    Some(_) => Err(ZippyError::InvalidState { status: "logging already initialized with different settings" }),
    None => initialize_and_store(config),
}
```

- [ ] **Step 5: Verify JSONL records include required fields**

```rust
tracing::info!(
    component = "logging_test",
    event = "emit",
    message = "test record",
    "test record"
);
```

- [ ] **Step 6: Re-run the logging test suite**

Run: `cargo test -p zippy-core --test logging -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/zippy-core/src/logging.rs crates/zippy-core/tests/logging.rs
git commit -m "feat: finalize jsonl logging initialization"
```

## Task 3: Expose `zippy.setup_log(...)` in Python

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `python/zippy/__init__.py`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Add the failing Python test for `setup_log()`**

```python
def test_setup_log_returns_snapshot_and_creates_file(tmp_path: Path) -> None:
    result = zippy.setup_log(
        app="py_test",
        level="info",
        log_dir=str(tmp_path),
        to_console=False,
        to_file=True,
    )
    assert result["app"] == "py_test"
    assert result["file_path"].endswith(".jsonl")
```

- [ ] **Step 2: Run the Python test to verify the function is missing**

Run: `uv run python -m pytest pytests/test_python_api.py::test_setup_log_returns_snapshot_and_creates_file -v`
Expected: FAIL with missing `setup_log`

- [ ] **Step 3: Add the PyO3 wrapper**

```rust
#[pyfunction]
#[pyo3(signature = (app, level="info", log_dir="logs", to_console=true, to_file=true))]
fn setup_log(
    app: String,
    level: &str,
    log_dir: &str,
    to_console: bool,
    to_file: bool,
) -> PyResult<PyObject> {
    let snapshot = zippy_core::setup_log(LogConfig::new(...))?;
    // convert to Python dict
}
```

- [ ] **Step 4: Export the Python API**

```python
from ._internal import setup_log
```

- [ ] **Step 5: Add the type stub**

```python
def setup_log(
    app: str,
    level: str = "info",
    log_dir: str = "logs",
    to_console: bool = True,
    to_file: bool = True,
) -> dict[str, object]: ...
```

- [ ] **Step 6: Build and run the Python test**

Run: `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
Expected: build succeeds

Run: `uv run python -m pytest pytests/test_python_api.py::test_setup_log_returns_snapshot_and_creates_file -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi python/zippy/__init__.py pytests/test_python_api.py
git commit -m "feat: expose setup_log to python"
```

## Task 4: Replace runtime debug prints with structured tracing events

**Files:**
- Modify: `crates/zippy-core/src/runtime.rs`
- Test: `crates/zippy-core/tests/runtime_lifecycle.rs`

- [ ] **Step 1: Add a failing runtime log capture test**

```rust
#[test]
fn runtime_emits_start_and_stop_events() {
    // initialize test logger
    // start and stop a simple engine
    // assert captured logs include event=start and event=stop
}
```

- [ ] **Step 2: Run the runtime test and confirm it fails**

Run: `cargo test -p zippy-core --test runtime_lifecycle -v`
Expected: FAIL because no runtime log capture exists yet

- [ ] **Step 3: Replace `zippy_debug_stop_log` internals with tracing**

```rust
tracing::debug!(
    component = "runtime",
    engine = engine_name,
    event = "stop",
    status = self.status().as_str(),
    message = "engine stop requested"
);
```

- [ ] **Step 4: Add structured logs at runtime lifecycle points**

```rust
tracing::info!(component = "runtime", event = "start", engine = name, message = "engine started");
tracing::info!(component = "runtime", event = "flush", engine = name, message = "engine flushed");
tracing::info!(component = "runtime", event = "stop", engine = name, message = "engine stopped");
tracing::error!(component = "runtime", event = "worker_failure", engine = name, error = %err, message = "worker failed");
```

- [ ] **Step 5: Re-run the runtime lifecycle test**

Run: `cargo test -p zippy-core --test runtime_lifecycle -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-core/tests/runtime_lifecycle.rs
git commit -m "feat: add runtime lifecycle logs"
```

## Task 5: Instrument Python bridge and perf runner

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `crates/zippy-perf/src/main.rs`
- Modify: `crates/zippy-perf/src/lib.rs` if needed for structured summary emission
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Add a failing Python-side log test**

```python
def test_setup_log_file_contains_runtime_message(tmp_path: Path) -> None:
    snapshot = zippy.setup_log("bridge_test", log_dir=str(tmp_path), to_console=False, to_file=True)
    engine = zippy.StreamTableEngine(name="ticks", input_schema=..., target=zippy.NullPublisher())
    engine.start()
    engine.stop()
    content = Path(snapshot["file_path"]).read_text(encoding="utf-8")
    assert '"message"' in content
```

- [ ] **Step 2: Run the Python log test to verify missing bridge/runtime records**

Run: `uv run python -m pytest pytests/test_python_api.py::test_setup_log_file_contains_runtime_message -v`
Expected: FAIL because the file does not yet capture the expected lifecycle event

- [ ] **Step 3: Add tracing to Python bridge start/stop paths**

```rust
tracing::info!(component = "python_bridge", event = "start", engine = name, message = "python runtime bridge started");
tracing::info!(component = "python_bridge", event = "stop", engine = name, message = "python runtime bridge stopped");
tracing::error!(component = "python_bridge", event = "stop_failure", engine = name, error = %error, message = "python runtime bridge stop failed");
```

- [ ] **Step 4: Replace `zippy-perf` plain stderr errors with tracing**

```rust
tracing::info!(component = "perf", event = "profile_start", profile = ?profile, message = "performance profile started");
tracing::info!(component = "perf", event = "profile_finish", pass = report.pass, message = "performance profile finished");
tracing::error!(component = "perf", event = "profile_error", error = %error, message = "performance profile failed");
```

- [ ] **Step 5: Rebuild and rerun the Python log test**

Run: `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
Expected: build succeeds

Run: `uv run python -m pytest pytests/test_python_api.py::test_setup_log_file_contains_runtime_message -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-python/src/lib.rs crates/zippy-perf/src/main.rs crates/zippy-perf/src/lib.rs pytests/test_python_api.py
git commit -m "feat: add bridge and perf logging"
```

## Task 6: Instrument OpenCTP live source and preserve debug toggles

**Files:**
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/source.rs`
- Modify: `plugins/zippy-openctp/crates/zippy-openctp-core/src/driver_ctp.rs`
- Modify: `plugins/zippy-openctp/tests/test_python_api.py` if plugin tests cover setup assumptions

- [ ] **Step 1: Add a focused fake-driver logging test in the plugin**

```rust
#[test]
fn fake_source_emits_login_and_subscribe_events() {
    // drive fake source lifecycle and assert logs include connect/login/subscribe events
}
```

- [ ] **Step 2: Run the plugin test to verify the logging gap**

Run: `cargo test -p zippy-openctp-core -v`
Expected: FAIL on missing source log assertions

- [ ] **Step 3: Replace `openctp_debug_log` internals with tracing-backed events**

```rust
tracing::info!(component = "openctp_source", event = "connect_start", front = %config.front, message = "starting market data connection");
tracing::info!(component = "openctp_source", event = "login_success", broker_id = %config.broker_id, message = "market data login succeeded");
tracing::warn!(component = "openctp_source", event = "subscribe_failure", instrument = %instrument, message = "subscribe request failed");
tracing::warn!(component = "openctp_source", event = "reconnect", message = "reconnecting market data source");
```

- [ ] **Step 4: Keep the environment toggles as log gates**

```rust
if openctp_debug_enabled() {
    tracing::debug!(component = "openctp_source", event = "tick", instrument = %instrument, message = "received depth market data");
}
```

- [ ] **Step 5: Re-run plugin tests**

Run: `cargo test -p zippy-openctp-core -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git -C plugins/zippy-openctp add crates/zippy-openctp-core/src/source.rs crates/zippy-openctp-core/src/driver_ctp.rs tests
git -C plugins/zippy-openctp commit -m "feat: add live openctp source logs"
```

## Task 7: Final verification and docs touch-up

**Files:**
- Modify: `docs/superpowers/specs/2026-04-09-logging-system-design.md` only if implementation changed behavior
- Modify: relevant README snippets if `setup_log()` examples are added

- [ ] **Step 1: Run the core Rust verification set**

Run: `cargo test -p zippy-core --test logging -p zippy-core --test runtime_lifecycle -v`
Expected: PASS

- [ ] **Step 2: Run the Python verification set**

Run: `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
Expected: build succeeds

Run: `uv run python -m pytest pytests/test_python_api.py -k "setup_log or parquet or stream_table" -v`
Expected: PASS

- [ ] **Step 3: Run the perf compile/test verification**

Run: `cargo test -p zippy-perf -v`
Expected: PASS

- [ ] **Step 4: Run the plugin verification**

Run: `cargo test -p zippy-openctp-core -v`
Expected: PASS

Run: `uv run --project plugins/zippy-openctp python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py`
Expected: PASS

- [ ] **Step 5: Commit any final doc adjustments**

```bash
git add docs README.md python/zippy/__init__.py python/zippy/_internal.pyi
git commit -m "docs: document unified logging setup"
```

## Self-Review

### Spec coverage

- explicit `zippy.setup_log(...)`: covered by Task 3
- console text + file JSONL: covered by Task 1 and Task 2
- per-run file layout with `date_runid`: covered by Task 2
- required `message` field: covered by Task 2 and Task 5
- runtime / python / perf / openctp integration: covered by Tasks 4, 5, 6
- duplicate initialization behavior: covered by Task 2

No spec gaps remain.

### Placeholder scan

- no `TODO` / `TBD`
- all tasks include concrete files, commands, and expected outcomes
- commit steps are explicit

### Type consistency

- API names are consistently `setup_log`, `LogConfig`, `LogSnapshot`
- file field names consistently use `app`, `run_id`, `file_path`, `message`
- plugin logging is constrained to the same `component/event/message` vocabulary
