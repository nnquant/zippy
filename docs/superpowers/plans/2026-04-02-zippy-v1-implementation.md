# Zippy V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个单机、可复现、默认不静默丢数的流式因子计算框架 V1，覆盖 `ReactiveStateEngine`、`TimeSeriesEngine`、`ZmqPublisher`、`ParquetSink` 与 Python 编排接口。

**Architecture:** 先搭建 Cargo workspace 与 PyO3 包装层，再自底向上实现 `zippy-core` 的运行时契约、队列、生命周期与指标。随后分别实现算子层、两类预制引擎、I/O 层与 Python 绑定，最后用 Rust 集成测试、Python 集成测试和基准测试验证可复现性与基本性能。

**Tech Stack:** Rust workspace, `arrow`, `parquet`, `crossbeam-channel`, `thiserror`, `tracing`, `zmq`, `pyo3`, `maturin`, `polars`, `pyarrow`, `pytest`, `uv`

---

## File Structure

### Root

- Create: `Cargo.toml`  
  责任：定义 workspace、共享依赖与 lint 配置。
- Create: `rust-toolchain.toml`  
  责任：固定 Rust 版本。
- Create: `pyproject.toml`  
  责任：定义 maturin 构建和 Python 开发依赖。
- Create: `.gitignore`  
  责任：忽略 `target/`、`.venv/`、`__pycache__/`、构建产物。

### `crates/zippy-core`

- Create: `crates/zippy-core/src/lib.rs`
- Create: `crates/zippy-core/src/error.rs`
- Create: `crates/zippy-core/src/types.rs`
- Create: `crates/zippy-core/src/engine.rs`
- Create: `crates/zippy-core/src/metrics.rs`
- Create: `crates/zippy-core/src/queue.rs`
- Create: `crates/zippy-core/src/runtime.rs`
- Create: `crates/zippy-core/tests/workspace_smoke.rs`
- Create: `crates/zippy-core/tests/core_contract.rs`
- Create: `crates/zippy-core/tests/runtime_lifecycle.rs`

### `crates/zippy-operators`

- Create: `crates/zippy-operators/src/lib.rs`
- Create: `crates/zippy-operators/src/reactive.rs`
- Create: `crates/zippy-operators/src/aggregation.rs`
- Create: `crates/zippy-operators/tests/reactive_factors.rs`
- Create: `crates/zippy-operators/tests/aggregation_specs.rs`

### `crates/zippy-engines`

- Create: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-engines/src/reactive.rs`
- Create: `crates/zippy-engines/src/timeseries.rs`
- Create: `crates/zippy-engines/tests/reactive_engine.rs`
- Create: `crates/zippy-engines/tests/timeseries_engine.rs`

### `crates/zippy-io`

- Create: `crates/zippy-io/src/lib.rs`
- Create: `crates/zippy-io/src/publisher.rs`
- Create: `crates/zippy-io/src/zmq.rs`
- Create: `crates/zippy-io/src/parquet_sink.rs`
- Create: `crates/zippy-io/tests/zmq_roundtrip.rs`
- Create: `crates/zippy-io/tests/parquet_sink.rs`

### `crates/zippy-python` 与 Python 包

- Create: `crates/zippy-python/src/lib.rs`
- Create: `python/zippy/__init__.py`
- Create: `python/zippy/_internal.pyi`
- Create: `pytests/test_python_api.py`

### Bench

- Create: `crates/zippy-engines/benches/reactive_pipeline.rs`
- Create: `crates/zippy-engines/benches/timeseries_pipeline.rs`

---

### Task 1: Bootstrap Workspace

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `crates/zippy-core/Cargo.toml`
- Create: `crates/zippy-core/src/lib.rs`
- Create: `crates/zippy-core/tests/workspace_smoke.rs`
- Create: `crates/zippy-operators/Cargo.toml`
- Create: `crates/zippy-operators/src/lib.rs`
- Create: `crates/zippy-engines/Cargo.toml`
- Create: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-io/Cargo.toml`
- Create: `crates/zippy-io/src/lib.rs`
- Create: `crates/zippy-python/Cargo.toml`
- Create: `crates/zippy-python/src/lib.rs`
- Create: `python/zippy/__init__.py`

- [ ] **Step 1: Write the failing smoke test**

```rust
// crates/zippy-core/tests/workspace_smoke.rs
#[test]
fn workspace_builds_core_crate() {
    assert_eq!(zippy_core::crate_name(), "zippy-core");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-core workspace_builds_core_crate -- --exact`
Expected: FAIL with `package ID specification 'zippy-core' did not match any packages`

- [ ] **Step 3: Write the minimal workspace skeleton**

```toml
# Cargo.toml
[workspace]
members = [
    "crates/zippy-core",
    "crates/zippy-operators",
    "crates/zippy-engines",
    "crates/zippy-io",
    "crates/zippy-python",
]
resolver = "2"

[workspace.package]
edition = "2021"
version = "0.1.0"

[workspace.dependencies]
arrow = "53"
parquet = "53"
crossbeam-channel = "0.5"
thiserror = "1"
tracing = "0.1"
zmq = "0.10"
pyo3 = { version = "0.22", features = ["extension-module"] }
pyo3-polars = "0.16"
polars = { version = "0.46", default-features = false, features = ["dtype-datetime", "fmt"] }

[workspace.lints.rust]
unsafe_code = "forbid"
```

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.86.0"
components = ["rustfmt", "clippy"]
```

```toml
# pyproject.toml
[build-system]
requires = ["maturin>=1.7,<2.0"]
build-backend = "maturin"

[project]
name = "zippy"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["polars>=1.25.0", "pyarrow>=18.0.0"]

[project.optional-dependencies]
dev = ["maturin>=1.7,<2.0", "pytest>=8.3.0"]

[tool.maturin]
module-name = "zippy._internal"
python-source = "python"
manifest-path = "crates/zippy-python/Cargo.toml"
```

```gitignore
# .gitignore
target/
.venv/
dist/
build/
__pycache__/
.pytest_cache/
```

```toml
# crates/zippy-core/Cargo.toml
[package]
name = "zippy-core"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
thiserror.workspace = true
```

```rust
// crates/zippy-core/src/lib.rs
pub fn crate_name() -> &'static str {
    "zippy-core"
}
```

```toml
# crates/zippy-operators/Cargo.toml
[package]
name = "zippy-operators"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
zippy-core = { path = "../zippy-core" }
```

```rust
// crates/zippy-operators/src/lib.rs
pub fn crate_name() -> &'static str {
    "zippy-operators"
}
```

```toml
# crates/zippy-engines/Cargo.toml
[package]
name = "zippy-engines"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
zippy-core = { path = "../zippy-core" }
zippy-operators = { path = "../zippy-operators" }
```

```rust
// crates/zippy-engines/src/lib.rs
pub fn crate_name() -> &'static str {
    "zippy-engines"
}
```

```toml
# crates/zippy-io/Cargo.toml
[package]
name = "zippy-io"
version.workspace = true
edition.workspace = true

[dependencies]
arrow.workspace = true
parquet.workspace = true
zippy-core = { path = "../zippy-core" }
```

```rust
// crates/zippy-io/src/lib.rs
pub fn crate_name() -> &'static str {
    "zippy-io"
}
```

```toml
# crates/zippy-python/Cargo.toml
[package]
name = "zippy-python"
version.workspace = true
edition.workspace = true

[lib]
name = "zippy_internal"
crate-type = ["cdylib"]

[dependencies]
pyo3.workspace = true
zippy-core = { path = "../zippy-core" }
zippy-engines = { path = "../zippy-engines" }
zippy-io = { path = "../zippy-io" }
```

```rust
// crates/zippy-python/src/lib.rs
use pyo3::prelude::*;

#[pymodule]
fn _internal(_py: Python<'_>, _module: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
```

```python
# python/zippy/__init__.py
from ._internal import *
```

- [ ] **Step 4: Run the smoke test**

Run: `cargo test -p zippy-core workspace_builds_core_crate -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml rust-toolchain.toml pyproject.toml .gitignore \
  crates/zippy-core/Cargo.toml crates/zippy-core/src/lib.rs crates/zippy-core/tests/workspace_smoke.rs \
  crates/zippy-operators/Cargo.toml crates/zippy-operators/src/lib.rs \
  crates/zippy-engines/Cargo.toml crates/zippy-engines/src/lib.rs \
  crates/zippy-io/Cargo.toml crates/zippy-io/src/lib.rs \
  crates/zippy-python/Cargo.toml crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py
git commit -m "feat: bootstrap zippy workspace"
```

### Task 2: Implement Core Contract Types

**Files:**
- Create: `crates/zippy-core/src/error.rs`
- Create: `crates/zippy-core/src/types.rs`
- Create: `crates/zippy-core/src/engine.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Create: `crates/zippy-core/tests/core_contract.rs`

- [ ] **Step 1: Write the failing contract test**

```rust
// crates/zippy-core/tests/core_contract.rs
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{Engine, EngineStatus, LateDataPolicy, OverflowPolicy, Result};

struct NoopEngine {
    schema: Arc<Schema>,
}

impl Engine for NoopEngine {
    fn name(&self) -> &str {
        "noop"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, _batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

#[test]
fn core_types_match_v1_contract() {
    let schema = Arc::new(Schema::new(vec![Field::new("price", DataType::Float64, false)]));
    let engine = NoopEngine { schema };

    assert_eq!(engine.name(), "noop");
    assert_eq!(OverflowPolicy::default(), OverflowPolicy::Block);
    assert_eq!(LateDataPolicy::default(), LateDataPolicy::Reject);
    assert_eq!(EngineStatus::Created.as_str(), "created");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-core core_types_match_v1_contract -- --exact`
Expected: FAIL with unresolved imports for `Engine`, `EngineStatus`, `LateDataPolicy`, `OverflowPolicy`, `Result`

- [ ] **Step 3: Implement the core error, types, and trait modules**

```rust
// crates/zippy-core/src/error.rs
use thiserror::Error;

pub type Result<T> = std::result::Result<T, ZippyError>;

#[derive(Debug, Error)]
pub enum ZippyError {
    #[error("invalid config reason=[{reason}]")]
    InvalidConfig { reason: String },
    #[error("schema mismatch reason=[{reason}]")]
    SchemaMismatch { reason: String },
    #[error("late data detected dt=[{dt}] last_dt=[{last_dt}]")]
    LateData { dt: i64, last_dt: i64 },
    #[error("engine state invalid status=[{status}]")]
    InvalidState { status: &'static str },
    #[error("channel send failed")]
    ChannelSend,
    #[error("channel receive failed")]
    ChannelReceive,
    #[error("io error reason=[{reason}]")]
    Io { reason: String },
}
```

```rust
// crates/zippy-core/src/types.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineStatus {
    Created,
    Running,
    Stopping,
    Stopped,
    Failed,
}

impl EngineStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    #[default]
    Block,
    Reject,
    DropOldest,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LateDataPolicy {
    #[default]
    Reject,
    DropWithMetric,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub name: String,
    pub buffer_capacity: usize,
    pub overflow_policy: OverflowPolicy,
    pub late_data_policy: LateDataPolicy,
}

impl EngineConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.is_empty() {
            return Err(crate::ZippyError::InvalidConfig {
                reason: "engine name must not be empty".to_string(),
            });
        }
        if self.buffer_capacity == 0 {
            return Err(crate::ZippyError::InvalidConfig {
                reason: "buffer capacity must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}
```

```rust
// crates/zippy-core/src/engine.rs
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::Result;

pub type SchemaRef = Arc<Schema>;

pub trait Engine: Send + 'static {
    fn name(&self) -> &str;
    fn input_schema(&self) -> SchemaRef;
    fn output_schema(&self) -> SchemaRef;
    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>>;

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }

    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        self.on_flush()
    }
}
```

```rust
// crates/zippy-core/src/lib.rs
pub mod engine;
pub mod error;
pub mod types;

pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use types::{EngineConfig, EngineStatus, LateDataPolicy, OverflowPolicy};

pub fn crate_name() -> &'static str {
    "zippy-core"
}
```

- [ ] **Step 4: Run the contract test**

Run: `cargo test -p zippy-core core_types_match_v1_contract -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/error.rs crates/zippy-core/src/types.rs \
  crates/zippy-core/src/engine.rs crates/zippy-core/src/lib.rs \
  crates/zippy-core/tests/core_contract.rs
git commit -m "feat: add core engine contract types"
```

### Task 3: Add Runtime, Queue, and Metrics

**Files:**
- Create: `crates/zippy-core/src/metrics.rs`
- Create: `crates/zippy-core/src/queue.rs`
- Create: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Create: `crates/zippy-core/tests/runtime_lifecycle.rs`

- [ ] **Step 1: Write the failing runtime test**

```rust
// crates/zippy-core/tests/runtime_lifecycle.rs
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::{spawn_engine, Engine, EngineConfig, EngineStatus, Result};

struct FlushEngine {
    schema: Arc<Schema>,
    flushed: bool,
}

impl Engine for FlushEngine {
    fn name(&self) -> &str {
        "flush-engine"
    }

    fn input_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        self.flushed = true;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Float64Array::from(vec![99.0]))],
        )
        .unwrap();
        Ok(vec![batch])
    }
}

#[test]
fn lifecycle_flush_and_stop_emit_pending_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("price", DataType::Float64, false)]));
    let engine = FlushEngine { schema: schema.clone(), flushed: false };
    let mut handle = spawn_engine(engine, EngineConfig {
        name: "flush-engine".to_string(),
        buffer_capacity: 16,
        overflow_policy: Default::default(),
        late_data_policy: Default::default(),
    }).unwrap();

    let input = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
    handle.write(input).unwrap();
    let flushed = handle.flush().unwrap();

    assert_eq!(flushed.len(), 1);
    assert_eq!(handle.status(), EngineStatus::Running);
    handle.stop().unwrap();
    assert_eq!(handle.status(), EngineStatus::Stopped);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-core lifecycle_flush_and_stop_emit_pending_batches -- --exact`
Expected: FAIL with unresolved import for `spawn_engine`

- [ ] **Step 3: Implement metrics, bounded queue, and runtime handle**

```rust
// crates/zippy-core/src/metrics.rs
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct EngineMetrics {
    processed_batches_total: AtomicU64,
    processed_rows_total: AtomicU64,
    output_batches_total: AtomicU64,
    dropped_batches_total: AtomicU64,
    late_rows_total: AtomicU64,
    publish_errors_total: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineMetricsSnapshot {
    pub processed_batches_total: u64,
    pub processed_rows_total: u64,
    pub output_batches_total: u64,
    pub dropped_batches_total: u64,
    pub late_rows_total: u64,
    pub publish_errors_total: u64,
}

impl EngineMetrics {
    pub fn inc_processed_batch(&self, rows: usize) {
        self.processed_batches_total.fetch_add(1, Ordering::Relaxed);
        self.processed_rows_total.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn inc_output_batches(&self, count: usize) {
        self.output_batches_total.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EngineMetricsSnapshot {
        EngineMetricsSnapshot {
            processed_batches_total: self.processed_batches_total.load(Ordering::Relaxed),
            processed_rows_total: self.processed_rows_total.load(Ordering::Relaxed),
            output_batches_total: self.output_batches_total.load(Ordering::Relaxed),
            dropped_batches_total: self.dropped_batches_total.load(Ordering::Relaxed),
            late_rows_total: self.late_rows_total.load(Ordering::Relaxed),
            publish_errors_total: self.publish_errors_total.load(Ordering::Relaxed),
        }
    }
}
```

```rust
// crates/zippy-core/src/queue.rs
use crossbeam_channel::{bounded, Receiver, Sender};

pub struct BoundedQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self { sender, receiver }
    }

    pub fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    pub fn receiver(&self) -> Receiver<T> {
        self.receiver.clone()
    }

    pub fn len(&self) -> usize {
        self.receiver.len()
    }
}
```

```rust
// crates/zippy-core/src/runtime.rs
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;
use crossbeam_channel::{bounded, Sender};

use crate::{Engine, EngineConfig, EngineMetrics, EngineMetricsSnapshot, EngineStatus, Result, ZippyError};

enum Command {
    Data(RecordBatch),
    Flush(Sender<Result<Vec<RecordBatch>>>),
    Stop(Sender<Result<Vec<RecordBatch>>>),
}

pub struct EngineHandle {
    status: Arc<Mutex<EngineStatus>>,
    tx: Sender<Command>,
    join_handle: Option<JoinHandle<Result<()>>>,
    metrics: Arc<EngineMetrics>,
}

impl EngineHandle {
    pub fn write(&self, batch: RecordBatch) -> Result<()> {
        self.tx.send(Command::Data(batch)).map_err(|_| ZippyError::ChannelSend)
    }

    pub fn flush(&self) -> Result<Vec<RecordBatch>> {
        let (tx, rx) = bounded(1);
        self.tx.send(Command::Flush(tx)).map_err(|_| ZippyError::ChannelSend)?;
        rx.recv().map_err(|_| ZippyError::ChannelReceive)?
    }

    pub fn stop(&mut self) -> Result<()> {
        let (tx, rx) = bounded(1);
        self.tx.send(Command::Stop(tx)).map_err(|_| ZippyError::ChannelSend)?;
        rx.recv().map_err(|_| ZippyError::ChannelReceive)??;
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.join().map_err(|_| ZippyError::Io { reason: "worker thread panicked".to_string() })??;
        }
        Ok(())
    }

    pub fn status(&self) -> EngineStatus {
        *self.status.lock().unwrap()
    }

    pub fn metrics(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot()
    }
}

pub fn spawn_engine<E>(mut engine: E, config: EngineConfig) -> Result<EngineHandle>
where
    E: Engine,
{
    config.validate()?;
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let status_clone = status.clone();
    let metrics_clone = metrics.clone();
    let rx = queue.receiver();
    let tx = queue.sender();

    let join_handle = thread::spawn(move || -> Result<()> {
        while let Ok(command) = rx.recv() {
            match command {
                Command::Data(batch) => {
                    metrics_clone.inc_processed_batch(batch.num_rows());
                    let outputs = engine.on_data(batch)?;
                    metrics_clone.inc_output_batches(outputs.len());
                }
                Command::Flush(reply_tx) => {
                    let result = engine.on_flush();
                    let _ = reply_tx.send(result);
                }
                Command::Stop(reply_tx) => {
                    *status_clone.lock().unwrap() = EngineStatus::Stopping;
                    let result = engine.on_stop();
                    let _ = reply_tx.send(result);
                    *status_clone.lock().unwrap() = EngineStatus::Stopped;
                    break;
                }
            }
        }
        Ok(())
    });

    Ok(EngineHandle {
        status,
        tx,
        join_handle: Some(join_handle),
        metrics,
    })
}
```

```rust
// crates/zippy-core/src/lib.rs
pub mod engine;
pub mod error;
pub mod metrics;
pub mod queue;
pub mod runtime;
pub mod types;

pub use engine::{Engine, SchemaRef};
pub use error::{Result, ZippyError};
pub use metrics::{EngineMetrics, EngineMetricsSnapshot};
pub use runtime::{spawn_engine, EngineHandle};
pub use types::{EngineConfig, EngineStatus, LateDataPolicy, OverflowPolicy};

pub fn crate_name() -> &'static str {
    "zippy-core"
}
```

- [ ] **Step 4: Run the runtime lifecycle test**

Run: `cargo test -p zippy-core lifecycle_flush_and_stop_emit_pending_batches -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/metrics.rs crates/zippy-core/src/queue.rs \
  crates/zippy-core/src/runtime.rs crates/zippy-core/src/lib.rs \
  crates/zippy-core/tests/runtime_lifecycle.rs
git commit -m "feat: add core runtime and lifecycle handling"
```

### Task 3B: Finish Overflow Policies and Queue Metrics

**Files:**
- Modify: `crates/zippy-core/src/metrics.rs`
- Modify: `crates/zippy-core/src/queue.rs`
- Modify: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-core/tests/runtime_lifecycle.rs`

- [ ] **Step 1: Write the failing overflow-policy test**

```rust
// append to crates/zippy-core/tests/runtime_lifecycle.rs
use std::thread;
use std::time::Duration;

use zippy_core::OverflowPolicy;

#[test]
fn reject_overflow_returns_error() {
    let schema = Arc::new(Schema::new(vec![Field::new("price", DataType::Float64, false)]));
    let engine = FlushEngine { schema: schema.clone(), flushed: false };
    let handle = spawn_engine(engine, EngineConfig {
        name: "reject-engine".to_string(),
        buffer_capacity: 1,
        overflow_policy: OverflowPolicy::Reject,
        late_data_policy: Default::default(),
    }).unwrap();

    let first = RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
    let second = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![2.0]))]).unwrap();

    handle.write(first).unwrap();
    thread::sleep(Duration::from_millis(10));
    let error = handle.write(second).unwrap_err();

    assert!(error.to_string().contains("queue is full"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-core reject_overflow_returns_error -- --exact`
Expected: FAIL because `write()` currently always blocks instead of honoring `OverflowPolicy::Reject`

- [ ] **Step 3: Implement `Block` / `Reject` / `DropOldest` semantics**

```rust
// update crates/zippy-core/src/metrics.rs
impl EngineMetrics {
    pub fn inc_dropped_batches(&self, count: usize) {
        self.dropped_batches_total.fetch_add(count as u64, Ordering::Relaxed);
    }
}
```

```rust
// update crates/zippy-core/src/queue.rs
use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};

impl<T> BoundedQueue<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(value)
    }

    pub fn try_drop_oldest_and_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.sender.try_send(value) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(value)) => {
                match self.receiver.try_recv() {
                    Ok(_) | Err(TryRecvError::Disconnected) => self.sender.try_send(value),
                    Err(TryRecvError::Empty) => self.sender.try_send(value),
                }
            }
            Err(error) => Err(error),
        }
    }
}
```

```rust
// update crates/zippy-core/src/runtime.rs
use crate::OverflowPolicy;

pub struct EngineHandle {
    status: Arc<Mutex<EngineStatus>>,
    tx: Sender<Command>,
    queue: crate::queue::BoundedQueue<Command>,
    join_handle: Option<JoinHandle<Result<()>>>,
    metrics: Arc<EngineMetrics>,
    overflow_policy: OverflowPolicy,
}

impl EngineHandle {
    pub fn write(&self, batch: RecordBatch) -> Result<()> {
        match self.overflow_policy {
            OverflowPolicy::Block => {
                self.tx.send(Command::Data(batch)).map_err(|_| ZippyError::ChannelSend)
            }
            OverflowPolicy::Reject => {
                self.queue.try_send(Command::Data(batch)).map_err(|_| ZippyError::Io {
                    reason: "queue is full".to_string(),
                })
            }
            OverflowPolicy::DropOldest => {
                self.queue.try_drop_oldest_and_send(Command::Data(batch)).map_err(|_| ZippyError::Io {
                    reason: "queue is full".to_string(),
                })?;
                self.metrics.inc_dropped_batches(1);
                Ok(())
            }
        }
    }
}

pub fn spawn_engine<E>(mut engine: E, config: EngineConfig) -> Result<EngineHandle>
where
    E: Engine,
{
    config.validate()?;
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let tx = queue.sender();
    let rx = queue.receiver();
    let overflow_policy = config.overflow_policy;
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let status_clone = status.clone();
    let metrics_clone = metrics.clone();

    let join_handle = thread::spawn(move || -> Result<()> {
        while let Ok(command) = rx.recv() {
            match command {
                Command::Data(batch) => {
                    metrics_clone.inc_processed_batch(batch.num_rows());
                    let outputs = engine.on_data(batch)?;
                    metrics_clone.inc_output_batches(outputs.len());
                }
                Command::Flush(reply_tx) => {
                    let result = engine.on_flush();
                    let _ = reply_tx.send(result);
                }
                Command::Stop(reply_tx) => {
                    *status_clone.lock().unwrap() = EngineStatus::Stopping;
                    let result = engine.on_stop();
                    let _ = reply_tx.send(result);
                    *status_clone.lock().unwrap() = EngineStatus::Stopped;
                    break;
                }
            }
        }
        Ok(())
    });

    Ok(EngineHandle {
        status,
        tx,
        queue,
        join_handle: Some(join_handle),
        metrics,
        overflow_policy,
    })
}
```

- [ ] **Step 4: Run the overflow-policy test**

Run: `cargo test -p zippy-core reject_overflow_returns_error -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/metrics.rs crates/zippy-core/src/queue.rs \
  crates/zippy-core/src/runtime.rs crates/zippy-core/tests/runtime_lifecycle.rs
git commit -m "feat: implement overflow policy handling"
```

### Task 4: Implement Reactive Factor Specs

**Files:**
- Create: `crates/zippy-operators/src/reactive.rs`
- Modify: `crates/zippy-operators/src/lib.rs`
- Create: `crates/zippy-operators/tests/reactive_factors.rs`

- [ ] **Step 1: Write the failing reactive factor test**

```rust
// crates/zippy-operators/tests/reactive_factors.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_operators::{ReactiveFactor, TsEmaSpec, TsReturnSpec};

#[test]
fn ema_and_return_follow_row_order() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A", "A", "A"])),
            Arc::new(Float64Array::from(vec![10.0, 12.0, 15.0])),
        ],
    )
    .unwrap();

    let mut ema = TsEmaSpec::new("symbol", "price", 2, "ema_2").build().unwrap();
    let mut ret = TsReturnSpec::new("symbol", "price", 1, "ret_1").build().unwrap();

    let ema_values = ema.evaluate(&batch).unwrap();
    let ret_values = ret.evaluate(&batch).unwrap();

    assert_eq!(ema_values.len(), 3);
    assert_eq!(ret_values.len(), 3);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-operators ema_and_return_follow_row_order -- --exact`
Expected: FAIL with unresolved imports for `ReactiveFactor`, `TsEmaSpec`, `TsReturnSpec`

- [ ] **Step 3: Implement the V1 reactive factor abstraction**

```rust
// crates/zippy-operators/src/reactive.rs
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;

use zippy_core::{Result, SchemaRef, ZippyError};

pub trait ReactiveFactor: Send {
    fn output_field(&self) -> Field;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
}

pub struct TsEmaSpec {
    id_column: String,
    value_column: String,
    span: usize,
    output: String,
}

impl TsEmaSpec {
    pub fn new(id_column: &str, value_column: &str, span: usize, output: &str) -> Self {
        Self {
            id_column: id_column.to_string(),
            value_column: value_column.to_string(),
            span,
            output: output.to_string(),
        }
    }

    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        Ok(Box::new(TsEmaFactor {
            id_column: self.id_column.clone(),
            value_column: self.value_column.clone(),
            alpha: 2.0 / (self.span as f64 + 1.0),
            output: self.output.clone(),
            state: HashMap::new(),
        }))
    }
}

pub struct TsReturnSpec {
    id_column: String,
    value_column: String,
    period: usize,
    output: String,
}

impl TsReturnSpec {
    pub fn new(id_column: &str, value_column: &str, period: usize, output: &str) -> Self {
        Self {
            id_column: id_column.to_string(),
            value_column: value_column.to_string(),
            period,
            output: output.to_string(),
        }
    }

    pub fn build(&self) -> Result<Box<dyn ReactiveFactor>> {
        Ok(Box::new(TsReturnFactor {
            id_column: self.id_column.clone(),
            value_column: self.value_column.clone(),
            period: self.period,
            output: self.output.clone(),
            state: HashMap::new(),
        }))
    }
}

struct TsEmaFactor {
    id_column: String,
    value_column: String,
    alpha: f64,
    output: String,
    state: HashMap<String, f64>,
}

impl ReactiveFactor for TsEmaFactor {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let ids = batch
            .column_by_name(&self.id_column)
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: format!("missing column [{}]", self.id_column) })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: "id column must be utf8".to_string() })?;
        let values = batch
            .column_by_name(&self.value_column)
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: format!("missing column [{}]", self.value_column) })?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: "value column must be float64".to_string() })?;

        let mut output = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = ids.value(row).to_string();
            let value = values.value(row);
            let next = match self.state.get(&key) {
                Some(prev) => self.alpha * value + (1.0 - self.alpha) * prev,
                None => value,
            };
            self.state.insert(key, next);
            output.push(next);
        }
        Ok(Arc::new(Float64Array::from(output)))
    }
}

struct TsReturnFactor {
    id_column: String,
    value_column: String,
    period: usize,
    output: String,
    state: HashMap<String, VecDeque<f64>>,
}

impl ReactiveFactor for TsReturnFactor {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, true)
    }

    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
        let ids = batch
            .column_by_name(&self.id_column)
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: format!("missing column [{}]", self.id_column) })?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: "id column must be utf8".to_string() })?;
        let values = batch
            .column_by_name(&self.value_column)
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: format!("missing column [{}]", self.value_column) })?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ZippyError::SchemaMismatch { reason: "value column must be float64".to_string() })?;

        let mut output = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = ids.value(row).to_string();
            let value = values.value(row);
            let history = self.state.entry(key).or_default();
            let result = if history.len() < self.period {
                None
            } else {
                let base = history[history.len() - self.period];
                Some((value / base) - 1.0)
            };
            history.push_back(value);
            while history.len() > self.period {
                let _ = history.pop_front();
            }
            output.push(result);
        }
        Ok(Arc::new(Float64Array::from(output)))
    }
}
```

```rust
// crates/zippy-operators/src/lib.rs
pub mod reactive;

pub use reactive::{ReactiveFactor, TsEmaSpec, TsReturnSpec};

pub fn crate_name() -> &'static str {
    "zippy-operators"
}
```

- [ ] **Step 4: Run the reactive factor test**

Run: `cargo test -p zippy-operators ema_and_return_follow_row_order -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/reactive.rs crates/zippy-operators/src/lib.rs \
  crates/zippy-operators/tests/reactive_factors.rs
git commit -m "feat: add v1 reactive factor specs"
```

### Task 5: Implement `ReactiveStateEngine`

**Files:**
- Create: `crates/zippy-engines/src/reactive.rs`
- Modify: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-engines/tests/reactive_engine.rs`

- [ ] **Step 1: Write the failing engine test**

```rust
// crates/zippy-engines/tests/reactive_engine.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::Engine;
use zippy_engines::ReactiveStateEngine;
use zippy_operators::{TsEmaSpec, TsReturnSpec};

#[test]
fn reactive_engine_appends_factor_columns_in_order() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(Float64Array::from(vec![10.0, 11.0])),
        ],
    )
    .unwrap();

    let mut engine = ReactiveStateEngine::new(
        "tick_factors",
        schema,
        vec![
            TsEmaSpec::new("symbol", "price", 2, "ema_2").build().unwrap(),
            TsReturnSpec::new("symbol", "price", 1, "ret_1").build().unwrap(),
        ],
    )
    .unwrap();

    let outputs = engine.on_data(batch).unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].num_columns(), 4);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-engines reactive_engine_appends_factor_columns_in_order -- --exact`
Expected: FAIL with unresolved import for `ReactiveStateEngine`

- [ ] **Step 3: Implement the reactive engine**

```rust
// crates/zippy-engines/src/reactive.rs
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use zippy_core::{Engine, Result, SchemaRef, ZippyError};
use zippy_operators::ReactiveFactor;

pub struct ReactiveStateEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    factors: Vec<Box<dyn ReactiveFactor>>,
}

impl ReactiveStateEngine {
    pub fn new(
        name: &str,
        input_schema: SchemaRef,
        factors: Vec<Box<dyn ReactiveFactor>>,
    ) -> Result<Self> {
        let mut fields = input_schema.fields().iter().map(|field| field.as_ref().clone()).collect::<Vec<_>>();
        for factor in &factors {
            fields.push(factor.output_field());
        }
        let output_schema = std::sync::Arc::new(Schema::new(fields));
        Ok(Self {
            name: name.to_string(),
            input_schema,
            output_schema,
            factors,
        })
    }
}

impl Engine for ReactiveStateEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let mut columns = batch.columns().to_vec();
        for factor in &mut self.factors {
            columns.push(factor.evaluate(&batch)?);
        }
        let output = RecordBatch::try_new(self.output_schema.clone(), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to build reactive output batch error=[{}]", error),
            }
        })?;
        Ok(vec![output])
    }
}
```

```rust
// crates/zippy-engines/src/lib.rs
pub mod reactive;

pub use reactive::ReactiveStateEngine;

pub fn crate_name() -> &'static str {
    "zippy-engines"
}
```

- [ ] **Step 4: Run the engine test**

Run: `cargo test -p zippy-engines reactive_engine_appends_factor_columns_in_order -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-engines/src/reactive.rs crates/zippy-engines/src/lib.rs \
  crates/zippy-engines/tests/reactive_engine.rs
git commit -m "feat: add reactive state engine"
```

### Task 6: Implement Aggregation Specs and `TimeSeriesEngine`

**Files:**
- Create: `crates/zippy-operators/src/aggregation.rs`
- Modify: `crates/zippy-operators/src/lib.rs`
- Create: `crates/zippy-engines/src/timeseries.rs`
- Modify: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-engines/tests/timeseries_engine.rs`

- [ ] **Step 1: Write the failing timeseries test**

```rust
// crates/zippy-engines/tests/timeseries_engine.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::Engine;
use zippy_engines::TimeSeriesEngine;
use zippy_operators::{AggFirstSpec, AggLastSpec, AggSumSpec};

#[test]
fn timeseries_engine_flushes_open_windows() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("dt", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(TimestampNanosecondArray::from(vec![0_i64, 30_000_000_000_i64]).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![10.0, 11.0])),
            Arc::new(Float64Array::from(vec![100.0, 120.0])),
        ],
    )
    .unwrap();

    let mut engine = TimeSeriesEngine::new(
        "bar_1m",
        schema,
        "symbol",
        "dt",
        60_000_000_000_i64,
        vec![
            AggFirstSpec::new("price", "open").build().unwrap(),
            AggLastSpec::new("price", "close").build().unwrap(),
            AggSumSpec::new("volume", "volume").build().unwrap(),
        ],
    )
    .unwrap();

    assert!(engine.on_data(batch).unwrap().is_empty());
    let flushed = engine.on_flush().unwrap();
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].num_rows(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-engines timeseries_engine_flushes_open_windows -- --exact`
Expected: FAIL with unresolved imports for `TimeSeriesEngine`, `AggFirstSpec`, `AggLastSpec`, `AggSumSpec`

- [ ] **Step 3: Implement aggregation specs and the window engine**

```rust
// crates/zippy-operators/src/aggregation.rs
use arrow::datatypes::{DataType, Field};

use zippy_core::Result;

pub trait AggregationSpec: Send {
    fn output_field(&self) -> Field;
    fn input_column(&self) -> &str;
    fn kind(&self) -> AggregationKind;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationKind {
    First,
    Last,
    Sum,
}

pub struct AggFirstSpec {
    column: String,
    output: String,
}

impl AggFirstSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggFirstSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn input_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::First
    }
}

pub struct AggLastSpec {
    column: String,
    output: String,
}

impl AggLastSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggLastSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn input_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Last
    }
}

pub struct AggSumSpec {
    column: String,
    output: String,
}

impl AggSumSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggSumSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn input_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Sum
    }
}
```

```rust
// crates/zippy-engines/src/timeseries.rs
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use zippy_core::{Engine, Result, SchemaRef, ZippyError};
use zippy_operators::{AggregationKind, AggregationSpec};

struct WindowState {
    window_start: i64,
    first: HashMap<String, f64>,
    last: HashMap<String, f64>,
    sum: HashMap<String, f64>,
}

pub struct TimeSeriesEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    window_ns: i64,
    specs: Vec<Box<dyn AggregationSpec>>,
    windows: HashMap<String, WindowState>,
}

impl TimeSeriesEngine {
    pub fn new(
        name: &str,
        input_schema: SchemaRef,
        id_column: &str,
        dt_column: &str,
        window_ns: i64,
        specs: Vec<Box<dyn AggregationSpec>>,
    ) -> Result<Self> {
        let mut fields = vec![
            Field::new(id_column, DataType::Utf8, false),
            Field::new("window_start", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
            Field::new("window_end", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        ];
        for spec in &specs {
            fields.push(spec.output_field());
        }
        Ok(Self {
            name: name.to_string(),
            input_schema,
            output_schema: Arc::new(Schema::new(fields)),
            id_column: id_column.to_string(),
            dt_column: dt_column.to_string(),
            window_ns,
            specs,
            windows: HashMap::new(),
        })
    }

    fn finish_windows(&mut self) -> Result<Vec<RecordBatch>> {
        let mut ids = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut values = vec![Vec::<f64>::new(); self.specs.len()];

        for (key, state) in self.windows.drain() {
            ids.push(key.clone());
            starts.push(state.window_start);
            ends.push(state.window_start + self.window_ns);
            for (index, spec) in self.specs.iter().enumerate() {
                let value = match spec.kind() {
                    AggregationKind::First => *state.first.get(spec.input_column()).unwrap(),
                    AggregationKind::Last => *state.last.get(spec.input_column()).unwrap(),
                    AggregationKind::Sum => *state.sum.get(spec.input_column()).unwrap(),
                };
                values[index].push(value);
            }
        }

        if ids.is_empty() {
            return Ok(vec![]);
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(TimestampNanosecondArray::from(starts).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(ends).with_timezone("UTC")),
        ];
        for column in values {
            columns.push(Arc::new(Float64Array::from(column)));
        }

        let batch = RecordBatch::try_new(self.output_schema.clone(), columns).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to build timeseries output batch error=[{}]", error),
            }
        })?;
        Ok(vec![batch])
    }
}

impl Engine for TimeSeriesEngine {
    fn name(&self) -> &str {
        &self.name
    }

    fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let ids = batch
            .column_by_name(&self.id_column)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dts = batch
            .column_by_name(&self.dt_column)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let key = ids.value(row).to_string();
            let dt = dts.value(row);
            let window_start = dt - (dt % self.window_ns);
            let state = self.windows.entry(key).or_insert(WindowState {
                window_start,
                first: HashMap::new(),
                last: HashMap::new(),
                sum: HashMap::new(),
            });

            if state.window_start != window_start {
                return Ok(self.finish_windows()?);
            }

            for spec in &self.specs {
                let value = batch
                    .column_by_name(spec.input_column())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row);
                state.first.entry(spec.input_column().to_string()).or_insert(value);
                state.last.insert(spec.input_column().to_string(), value);
                let entry = state.sum.entry(spec.input_column().to_string()).or_insert(0.0);
                *entry += value;
            }
        }
        Ok(vec![])
    }

    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        self.finish_windows()
    }
}
```

```rust
// crates/zippy-operators/src/lib.rs
pub mod aggregation;
pub mod reactive;

pub use aggregation::{AggFirstSpec, AggLastSpec, AggSumSpec, AggregationKind, AggregationSpec};
pub use reactive::{ReactiveFactor, TsEmaSpec, TsReturnSpec};

pub fn crate_name() -> &'static str {
    "zippy-operators"
}
```

```rust
// crates/zippy-engines/src/lib.rs
pub mod reactive;
pub mod timeseries;

pub use reactive::ReactiveStateEngine;
pub use timeseries::TimeSeriesEngine;

pub fn crate_name() -> &'static str {
    "zippy-engines"
}
```

- [ ] **Step 4: Run the timeseries test**

Run: `cargo test -p zippy-engines timeseries_engine_flushes_open_windows -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/aggregation.rs crates/zippy-operators/src/lib.rs \
  crates/zippy-engines/src/timeseries.rs crates/zippy-engines/src/lib.rs \
  crates/zippy-engines/tests/timeseries_engine.rs
git commit -m "feat: add timeseries engine and aggregation specs"
```

### Task 6B: Complete V1 Aggregations and Late-Data Rejection

**Files:**
- Modify: `crates/zippy-operators/src/aggregation.rs`
- Modify: `crates/zippy-operators/src/lib.rs`
- Modify: `crates/zippy-engines/src/timeseries.rs`
- Modify: `crates/zippy-engines/tests/timeseries_engine.rs`

- [ ] **Step 1: Write the failing aggregation-completeness test**

```rust
// append to crates/zippy-engines/tests/timeseries_engine.rs
use zippy_core::LateDataPolicy;
use zippy_operators::{AggCountSpec, AggMaxSpec, AggMinSpec, AggVwapSpec};

#[test]
fn timeseries_engine_rejects_late_rows_and_supports_full_v1_aggregations() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("dt", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));

    let mut engine = TimeSeriesEngine::new(
        "bar_1m",
        schema.clone(),
        "symbol",
        "dt",
        60_000_000_000_i64,
        LateDataPolicy::Reject,
        vec![
            AggMaxSpec::new("price", "high").build().unwrap(),
            AggMinSpec::new("price", "low").build().unwrap(),
            AggCountSpec::new("price", "count").build().unwrap(),
            AggVwapSpec::new("price", "volume", "vwap").build().unwrap(),
        ],
    )
    .unwrap();

    let ordered = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(TimestampNanosecondArray::from(vec![0_i64, 30_000_000_000_i64]).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![10.0, 11.0])),
            Arc::new(Float64Array::from(vec![100.0, 200.0])),
        ],
    )
    .unwrap();
    assert!(engine.on_data(ordered).unwrap().is_empty());

    let late = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(TimestampNanosecondArray::from(vec![10_i64]).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![9.0])),
            Arc::new(Float64Array::from(vec![50.0])),
        ],
    )
    .unwrap();

    let error = engine.on_data(late).unwrap_err();
    assert!(error.to_string().contains("late data"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-engines timeseries_engine_rejects_late_rows_and_supports_full_v1_aggregations -- --exact`
Expected: FAIL with unresolved imports for `AggCountSpec`, `AggMaxSpec`, `AggMinSpec`, `AggVwapSpec` or missing `LateDataPolicy` parameter in `TimeSeriesEngine::new`

- [ ] **Step 3: Implement the remaining aggregations and late-data checks**

```rust
// update crates/zippy-operators/src/aggregation.rs
pub trait AggregationSpec: Send {
    fn output_field(&self) -> Field;
    fn primary_column(&self) -> &str;
    fn secondary_column(&self) -> Option<&str> {
        None
    }
    fn kind(&self) -> AggregationKind;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationKind {
    First,
    Last,
    Max,
    Min,
    Sum,
    Count,
    Vwap,
}

pub struct AggMaxSpec {
    column: String,
    output: String,
}

impl AggMaxSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggMaxSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn primary_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Max
    }
}

pub struct AggMinSpec {
    column: String,
    output: String,
}

impl AggMinSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggMinSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn primary_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Min
    }
}

pub struct AggCountSpec {
    column: String,
    output: String,
}

impl AggCountSpec {
    pub fn new(column: &str, output: &str) -> Self {
        Self { column: column.to_string(), output: output.to_string() }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggCountSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn primary_column(&self) -> &str {
        &self.column
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Count
    }
}

pub struct AggVwapSpec {
    price_column: String,
    volume_column: String,
    output: String,
}

impl AggVwapSpec {
    pub fn new(price_column: &str, volume_column: &str, output: &str) -> Self {
        Self {
            price_column: price_column.to_string(),
            volume_column: volume_column.to_string(),
            output: output.to_string(),
        }
    }

    pub fn build(self) -> Result<Box<dyn AggregationSpec>> {
        Ok(Box::new(self))
    }
}

impl AggregationSpec for AggVwapSpec {
    fn output_field(&self) -> Field {
        Field::new(&self.output, DataType::Float64, false)
    }

    fn primary_column(&self) -> &str {
        &self.price_column
    }

    fn secondary_column(&self) -> Option<&str> {
        Some(&self.volume_column)
    }

    fn kind(&self) -> AggregationKind {
        AggregationKind::Vwap
    }
}
```

```rust
// update crates/zippy-engines/src/timeseries.rs
struct WindowState {
    window_start: i64,
    first: HashMap<String, f64>,
    last: HashMap<String, f64>,
    max: HashMap<String, f64>,
    min: HashMap<String, f64>,
    sum: HashMap<String, f64>,
    count: HashMap<String, u64>,
    vwap_turnover: HashMap<String, f64>,
    vwap_volume: HashMap<String, f64>,
}

pub struct TimeSeriesEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    window_ns: i64,
    late_data_policy: zippy_core::LateDataPolicy,
    specs: Vec<Box<dyn AggregationSpec>>,
    windows: HashMap<String, WindowState>,
    last_seen_dt_by_key: HashMap<String, i64>,
}

if let Some(last_seen) = self.last_seen_dt_by_key.get(&key) {
    if dt < *last_seen {
        return Err(ZippyError::LateData { dt, last_dt: *last_seen });
    }
}
self.last_seen_dt_by_key.insert(key.clone(), dt);

for spec in &self.specs {
    let primary = batch
        .column_by_name(spec.primary_column())
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row);

    state.first.entry(spec.primary_column().to_string()).or_insert(primary);
    state.last.insert(spec.primary_column().to_string(), primary);
    state.max.entry(spec.primary_column().to_string()).and_modify(|value| *value = value.max(primary)).or_insert(primary);
    state.min.entry(spec.primary_column().to_string()).and_modify(|value| *value = value.min(primary)).or_insert(primary);
    *state.sum.entry(spec.primary_column().to_string()).or_insert(0.0) += primary;
    *state.count.entry(spec.primary_column().to_string()).or_insert(0) += 1;

    if let Some(volume_column) = spec.secondary_column() {
        let volume = batch
            .column_by_name(volume_column)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row);
        *state.vwap_turnover.entry(spec.primary_column().to_string()).or_insert(0.0) += primary * volume;
        *state.vwap_volume.entry(spec.primary_column().to_string()).or_insert(0.0) += volume;
    }
}

let value = match spec.kind() {
    AggregationKind::First => *state.first.get(spec.primary_column()).unwrap(),
    AggregationKind::Last => *state.last.get(spec.primary_column()).unwrap(),
    AggregationKind::Max => *state.max.get(spec.primary_column()).unwrap(),
    AggregationKind::Min => *state.min.get(spec.primary_column()).unwrap(),
    AggregationKind::Sum => *state.sum.get(spec.primary_column()).unwrap(),
    AggregationKind::Count => *state.count.get(spec.primary_column()).unwrap() as f64,
    AggregationKind::Vwap => {
        let turnover = *state.vwap_turnover.get(spec.primary_column()).unwrap();
        let volume = *state.vwap_volume.get(spec.primary_column()).unwrap();
        turnover / volume
    }
};
```

```rust
// update crates/zippy-operators/src/lib.rs
pub use aggregation::{
    AggCountSpec, AggFirstSpec, AggLastSpec, AggMaxSpec, AggMinSpec, AggSumSpec, AggVwapSpec,
    AggregationKind, AggregationSpec,
};
```

- [ ] **Step 4: Run the aggregation-completeness test**

Run: `cargo test -p zippy-engines timeseries_engine_rejects_late_rows_and_supports_full_v1_aggregations -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/aggregation.rs crates/zippy-operators/src/lib.rs \
  crates/zippy-engines/src/timeseries.rs crates/zippy-engines/tests/timeseries_engine.rs
git commit -m "feat: complete v1 aggregations and late-data checks"
```

### Task 7: Implement Publishers and Parquet Sink

**Files:**
- Create: `crates/zippy-io/src/publisher.rs`
- Create: `crates/zippy-io/src/zmq.rs`
- Create: `crates/zippy-io/src/parquet_sink.rs`
- Modify: `crates/zippy-io/src/lib.rs`
- Create: `crates/zippy-io/tests/zmq_roundtrip.rs`
- Create: `crates/zippy-io/tests/parquet_sink.rs`

- [ ] **Step 1: Write the failing I/O integration test**

```rust
// crates/zippy-io/tests/zmq_roundtrip.rs
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_io::{NullPublisher, Publisher};

#[test]
fn null_publisher_accepts_arrow_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("price", DataType::Float64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();

    let mut publisher = NullPublisher::default();
    publisher.publish(&batch).unwrap();
    publisher.flush().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-io null_publisher_accepts_arrow_batches -- --exact`
Expected: FAIL with unresolved imports for `NullPublisher`, `Publisher`

- [ ] **Step 3: Implement publisher trait, null publisher, ZMQ publisher, and parquet sink**

```rust
// crates/zippy-io/src/publisher.rs
use arrow::record_batch::RecordBatch;

use zippy_core::Result;

pub trait Publisher: Send + 'static {
    fn publish(&mut self, batch: &RecordBatch) -> Result<()>;

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct NullPublisher {
    published_batches: usize,
}

impl NullPublisher {
    pub fn published_batches(&self) -> usize {
        self.published_batches
    }
}

impl Publisher for NullPublisher {
    fn publish(&mut self, _batch: &RecordBatch) -> Result<()> {
        self.published_batches += 1;
        Ok(())
    }
}
```

```rust
// crates/zippy-io/src/zmq.rs
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use zippy_core::{Result, ZippyError};

use crate::Publisher;

pub struct ZmqPublisher {
    socket: zmq::Socket,
}

impl ZmqPublisher {
    pub fn bind(endpoint: &str) -> Result<Self> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::PUB).map_err(|error| ZippyError::Io {
            reason: format!("failed to create zmq pub socket error=[{}]", error),
        })?;
        socket.bind(endpoint).map_err(|error| ZippyError::Io {
            reason: format!("failed to bind zmq endpoint endpoint=[{}] error=[{}]", endpoint, error),
        })?;
        Ok(Self { socket })
    }
}

impl Publisher for ZmqPublisher {
    fn publish(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut payload = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut payload, &batch.schema()).map_err(|error| ZippyError::Io {
                reason: format!("failed to create arrow ipc writer error=[{}]", error),
            })?;
            writer.write(batch).map_err(|error| ZippyError::Io {
                reason: format!("failed to write arrow ipc payload error=[{}]", error),
            })?;
            writer.finish().map_err(|error| ZippyError::Io {
                reason: format!("failed to finish arrow ipc payload error=[{}]", error),
            })?;
        }
        self.socket.send(payload, 0).map_err(|error| ZippyError::Io {
            reason: format!("failed to send zmq payload error=[{}]", error),
        })?;
        Ok(())
    }
}
```

```rust
// crates/zippy-io/src/parquet_sink.rs
use std::fs::{File, create_dir_all};
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use zippy_core::{Result, ZippyError};

pub struct ParquetSink {
    root: PathBuf,
}

impl ParquetSink {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn write_batch(&self, file_name: &str, batch: &RecordBatch) -> Result<()> {
        create_dir_all(&self.root).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet directory error=[{}]", error),
        })?;
        let file = File::create(self.root.join(file_name)).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet file error=[{}]", error),
        })?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| ZippyError::Io {
            reason: format!("failed to create parquet writer error=[{}]", error),
        })?;
        writer.write(batch).map_err(|error| ZippyError::Io {
            reason: format!("failed to write parquet batch error=[{}]", error),
        })?;
        writer.close().map_err(|error| ZippyError::Io {
            reason: format!("failed to close parquet writer error=[{}]", error),
        })?;
        Ok(())
    }
}
```

```rust
// crates/zippy-io/src/lib.rs
pub mod parquet_sink;
pub mod publisher;
pub mod zmq;

pub use parquet_sink::ParquetSink;
pub use publisher::{NullPublisher, Publisher};
pub use zmq::ZmqPublisher;

pub fn crate_name() -> &'static str {
    "zippy-io"
}
```

- [ ] **Step 4: Run the I/O test**

Run: `cargo test -p zippy-io null_publisher_accepts_arrow_batches -- --exact`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-io/src/publisher.rs crates/zippy-io/src/zmq.rs \
  crates/zippy-io/src/parquet_sink.rs crates/zippy-io/src/lib.rs \
  crates/zippy-io/tests/zmq_roundtrip.rs crates/zippy-io/tests/parquet_sink.rs
git commit -m "feat: add zippy io publishers and parquet sink"
```

### Task 8: Add PyO3 Bindings and Python Integration Tests

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Create: `python/zippy/_internal.pyi`
- Create: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing Python integration test**

```python
# pytests/test_python_api.py
import polars as pl
import pyarrow as pa
import zippy


def test_reactive_engine_accepts_polars_and_flushes() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.TsEmaSpec(id_column="symbol", value_column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A", "A"], "price": [10.0, 11.0]}))
    engine.flush()
    engine.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest pytests/test_python_api.py::test_reactive_engine_accepts_polars_and_flushes -v`
Expected: FAIL with `ModuleNotFoundError` or missing symbols from `zippy`

- [ ] **Step 3: Expose minimal Python classes and lifecycle APIs**

```rust
// crates/zippy-python/src/lib.rs
use std::sync::Arc;

use arrow::datatypes::Schema;
use pyo3::prelude::*;

use zippy_core::{spawn_engine, EngineConfig};
use zippy_engines::ReactiveStateEngine;
use zippy_io::NullPublisher;
use zippy_operators::TsEmaSpec;

#[pyclass]
struct PyTsEmaSpec {
    #[pyo3(get)]
    id_column: String,
    #[pyo3(get)]
    value_column: String,
    #[pyo3(get)]
    span: usize,
    #[pyo3(get)]
    output: String,
}

#[pymethods]
impl PyTsEmaSpec {
    #[new]
    fn new(id_column: String, value_column: String, span: usize, output: String) -> Self {
        Self { id_column, value_column, span, output }
    }
}

#[pyclass]
struct PyNullPublisher {}

#[pymethods]
impl PyNullPublisher {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

#[pyclass]
struct PyReactiveStateEngine {
    handle: Option<zippy_core::EngineHandle>,
    engine: Option<ReactiveStateEngine>,
}

#[pymethods]
impl PyReactiveStateEngine {
    #[new]
    fn new(name: String, input_schema: PyObject, id_column: String, factors: Vec<Py<PyTsEmaSpec>>, _target: Py<PyNullPublisher>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let schema: Arc<Schema> = input_schema.extract(py)?;
            let specs = factors
                .into_iter()
                .map(|factor| {
                    let factor_ref = factor.borrow(py);
                    TsEmaSpec::new(
                        &factor_ref.id_column,
                        &factor_ref.value_column,
                        factor_ref.span,
                        &factor_ref.output,
                    )
                    .build()
                })
                .collect::<zippy_core::Result<Vec<_>>>()
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            let engine = ReactiveStateEngine::new(&name, schema, specs)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            let _publisher = NullPublisher::default();
            let _ = id_column;
            Ok(Self { handle: None, engine: Some(engine) })
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let engine = self.engine.take().ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("engine already started"))?;
        let handle = spawn_engine(engine, EngineConfig {
            name: "tick_factors".to_string(),
            buffer_capacity: 1024,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        })
        .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        self.handle = Some(handle);
        Ok(())
    }

    fn flush(&self) -> PyResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("engine not started"))?
            .flush()
            .map(|_| ())
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))
    }
}

#[pymodule]
fn _internal(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyTsEmaSpec>()?;
    module.add_class::<PyNullPublisher>()?;
    module.add_class::<PyReactiveStateEngine>()?;
    Ok(())
}
```

```python
# python/zippy/__init__.py
from ._internal import PyNullPublisher as NullPublisher
from ._internal import PyReactiveStateEngine as ReactiveStateEngine
from ._internal import PyTsEmaSpec as TsEmaSpec

__all__ = ["NullPublisher", "ReactiveStateEngine", "TsEmaSpec"]
```

```python
# python/zippy/_internal.pyi
class TsEmaSpec:
    def __init__(self, id_column: str, value_column: str, span: int, output: str) -> None:
        pass


class NullPublisher:
    def __init__(self) -> None:
        pass


class ReactiveStateEngine:
    def __init__(self, name: str, input_schema, id_column: str, factors: list[TsEmaSpec], target: NullPublisher) -> None:
        pass

    def start(self) -> None:
        pass

    def write(self, value) -> None:
        pass

    def flush(self) -> None:
        pass

    def stop(self) -> None:
        pass
```

- [ ] **Step 4: Build the extension and run Python integration tests**

Run: `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
Expected: PASS with `Built wheel` and installed editable extension

Run: `uv run pytest pytests/test_python_api.py::test_reactive_engine_accepts_polars_and_flushes -v`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/__init__.py \
  python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: add python bindings for v1 reactive engine"
```

### Task 8B: Complete Python `write()` and `stop()` Semantics

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing lifecycle behavior test**

```python
# append to pytests/test_python_api.py
import pytest


def test_write_after_stop_raises_runtime_error() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.TsEmaSpec(id_column="symbol", value_column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()

    with pytest.raises(RuntimeError):
        engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest pytests/test_python_api.py::test_write_after_stop_raises_runtime_error -v`
Expected: FAIL because `write()` and `stop()` are not fully implemented in the binding

- [ ] **Step 3: Implement Arrow/Polars conversion and lifecycle guards**

```rust
// update crates/zippy-python/src/lib.rs
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3_polars::PyDataFrame;

#[pyclass]
struct PyReactiveStateEngine {
    name: String,
    input_schema: Arc<Schema>,
    handle: Option<zippy_core::EngineHandle>,
    engine: Option<ReactiveStateEngine>,
}

#[pymethods]
impl PyReactiveStateEngine {
    #[new]
    fn new(name: String, input_schema: PyObject, id_column: String, factors: Vec<Py<PyTsEmaSpec>>, _target: Py<PyNullPublisher>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let schema: Arc<Schema> = input_schema.extract(py)?;
            let specs = factors
                .into_iter()
                .map(|factor| {
                    let factor_ref = factor.borrow(py);
                    TsEmaSpec::new(
                        &factor_ref.id_column,
                        &factor_ref.value_column,
                        factor_ref.span,
                        &factor_ref.output,
                    )
                    .build()
                })
                .collect::<zippy_core::Result<Vec<_>>>()
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            let engine = ReactiveStateEngine::new(&name, schema.clone(), specs)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
            let _ = id_column;
            Ok(Self {
                name,
                input_schema: schema,
                handle: None,
                engine: Some(engine),
            })
        })
    }

    fn start(&mut self) -> PyResult<()> {
        let engine = self.engine.take().ok_or_else(|| PyRuntimeError::new_err("engine already started"))?;
        let handle = spawn_engine(engine, EngineConfig {
            name: self.name.clone(),
            buffer_capacity: 1024,
            overflow_policy: Default::default(),
            late_data_policy: Default::default(),
        })
        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        self.handle = Some(handle);
        Ok(())
    }

    fn write(&self, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let handle = self.handle.as_ref().ok_or_else(|| PyRuntimeError::new_err("engine not started"))?;
        let dataframe = value.extract::<PyDataFrame>().map_err(|_| {
            PyTypeError::new_err("write() only accepts polars.DataFrame in v1")
        })?;
        let arrow_df = dataframe.0.to_arrow(Default::default());
        let batches = arrow_df
            .collect::<Vec<_>>();
        let batch = batches
            .into_iter()
            .next()
            .ok_or_else(|| PyValueError::new_err("input dataframe produced no record batch"))?
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        handle
            .write(batch)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        self.handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("engine not started"))?
            .flush()
            .map(|_| ())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    fn stop(&mut self) -> PyResult<()> {
        let mut handle = self.handle.take().ok_or_else(|| PyRuntimeError::new_err("engine not started"))?;
        handle.stop().map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }
}
```

```python
# update python/zippy/_internal.pyi
class ReactiveStateEngine:
    def __init__(self, name: str, input_schema, id_column: str, factors: list[TsEmaSpec], target: NullPublisher) -> None:
        pass

    def start(self) -> None:
        pass

    def write(self, value) -> None:
        pass

    def flush(self) -> None:
        pass

    def stop(self) -> None:
        pass
```

- [ ] **Step 4: Rebuild the extension and rerun Python tests**

Run: `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
Expected: PASS with the rebuilt extension installed into the active environment

Run: `uv run pytest pytests/test_python_api.py::test_write_after_stop_raises_runtime_error -v`
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi \
  pytests/test_python_api.py
git commit -m "feat: complete python engine lifecycle semantics"
```

### Task 9: Add Determinism Tests and Benchmarks

**Files:**
- Create: `crates/zippy-engines/src/testing.rs`
- Modify: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-engines/tests/replay_determinism.rs`
- Modify: `crates/zippy-engines/Cargo.toml`
- Create: `crates/zippy-engines/benches/reactive_pipeline.rs`
- Create: `crates/zippy-engines/benches/timeseries_pipeline.rs`

- [ ] **Step 1: Write the failing determinism test**

```rust
// crates/zippy-engines/tests/replay_determinism.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use zippy_core::Engine;
use zippy_engines::{hash_record_batches, ReactiveStateEngine};
use zippy_operators::TsEmaSpec;

#[test]
fn replaying_same_ticks_produces_same_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])),
            Arc::new(Float64Array::from(vec![10.0, 11.0])),
        ],
    )
    .unwrap();

    let mut first = ReactiveStateEngine::new(
        "run_one",
        schema.clone(),
        vec![TsEmaSpec::new("symbol", "price", 2, "ema_2").build().unwrap()],
    )
    .unwrap();
    let mut second = ReactiveStateEngine::new(
        "run_two",
        schema,
        vec![TsEmaSpec::new("symbol", "price", 2, "ema_2").build().unwrap()],
    )
    .unwrap();

    let first_batches = first.on_data(batch.clone()).unwrap();
    let second_batches = second.on_data(batch).unwrap();

    assert_eq!(hash_record_batches(&first_batches), hash_record_batches(&second_batches));
}
```

- [ ] **Step 2: Run the determinism test and capture the first failure**

Run: `cargo test -p zippy-engines replaying_same_ticks_produces_same_batches -- --exact`
Expected: FAIL with unresolved import for `hash_record_batches`

- [ ] **Step 3: Add replay verification and benchmark harnesses**

```rust
// crates/zippy-engines/src/testing.rs
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

pub fn hash_record_batches(batches: &[RecordBatch]) -> String {
    let formatted = pretty_format_batches(batches).unwrap().to_string();
    let digest = md5::compute(formatted.as_bytes());
    format!("{:x}", digest)
}
```

```rust
// update crates/zippy-engines/src/lib.rs
pub mod reactive;
pub mod testing;
pub mod timeseries;

pub use reactive::ReactiveStateEngine;
pub use testing::hash_record_batches;
pub use timeseries::TimeSeriesEngine;
```

```toml
// update crates/zippy-engines/Cargo.toml
[dependencies]
arrow.workspace = true
zippy-core = { path = "../zippy-core" }
zippy-operators = { path = "../zippy-operators" }
md5 = "0.7"

[dev-dependencies]
criterion = "0.5"
```

```rust
// crates/zippy-engines/benches/reactive_pipeline.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::Engine;
use zippy_engines::ReactiveStateEngine;
use zippy_operators::TsEmaSpec;

fn bench_reactive_pipeline(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"; 1024])),
            Arc::new(Float64Array::from(vec![10.0; 1024])),
        ],
    )
    .unwrap();

    c.bench_function("reactive_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = ReactiveStateEngine::new(
                "bench",
                schema.clone(),
                vec![TsEmaSpec::new("symbol", "price", 8, "ema_8").build().unwrap()],
            )
            .unwrap();
            let _ = engine.on_data(batch.clone()).unwrap();
        })
    });
}

criterion_group!(benches, bench_reactive_pipeline);
criterion_main!(benches);
```

```rust
// crates/zippy-engines/benches/timeseries_pipeline.rs
use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use zippy_core::Engine;
use zippy_engines::TimeSeriesEngine;
use zippy_operators::{AggFirstSpec, AggLastSpec, AggSumSpec};

fn bench_timeseries_pipeline(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("dt", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"; 1024])),
            Arc::new(TimestampNanosecondArray::from((0..1024).map(|index| index as i64 * 1_000_000_000).collect::<Vec<_>>()).with_timezone("UTC")),
            Arc::new(Float64Array::from(vec![10.0; 1024])),
            Arc::new(Float64Array::from(vec![100.0; 1024])),
        ],
    )
    .unwrap();

    c.bench_function("timeseries_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = TimeSeriesEngine::new(
                "bench",
                schema.clone(),
                "symbol",
                "dt",
                60_000_000_000_i64,
                zippy_core::LateDataPolicy::Reject,
                vec![
                    AggFirstSpec::new("price", "open").build().unwrap(),
                    AggLastSpec::new("price", "close").build().unwrap(),
                    AggSumSpec::new("volume", "volume").build().unwrap(),
                ],
            )
            .unwrap();
            let _ = engine.on_data(batch.clone()).unwrap();
            let _ = engine.on_flush().unwrap();
        })
    });
}

criterion_group!(benches, bench_timeseries_pipeline);
criterion_main!(benches);
```

- [ ] **Step 4: Run the final verification commands**

Run: `cargo test --workspace`
Expected: PASS with all Rust unit and integration tests green

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS with no warnings

Run: `cargo bench -p zippy-engines --bench reactive_pipeline --no-run`
Expected: PASS with benchmark target compiled successfully

Run: `uv run pytest pytests -v`
Expected: PASS with all Python integration tests green

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-engines/src/testing.rs crates/zippy-engines/src/lib.rs \
  crates/zippy-engines/tests/replay_determinism.rs crates/zippy-engines/Cargo.toml \
  crates/zippy-engines/benches/reactive_pipeline.rs \
  crates/zippy-engines/benches/timeseries_pipeline.rs
git commit -m "test: add determinism checks and benchmark harnesses"
```

---

## Self-Review Notes

### Spec Coverage

- `on_data()` / `on_flush()` / `on_stop()`：由 Task 2 和 Task 3 覆盖。
- 显式 `input_schema`、无隐式类型转换：由 Task 2 和 Task 8 覆盖。
- 有界输入队列、默认 `Block`、`Reject` / `DropOldest`：由 Task 2、Task 3 和 Task 3B 覆盖。
- `ReactiveStateEngine`：由 Task 4 和 Task 5 覆盖。
- `TimeSeriesEngine`、晚到数据拒绝、完整 V1 聚合：由 Task 6 和 Task 6B 覆盖。
- `ZmqPublisher` / `NullPublisher` / `ParquetSink`：由 Task 7 覆盖。
- Python 编排与 `write()` / `flush()` / `stop()`：由 Task 8 和 Task 8B 覆盖。
- 可复现性与基准：由 Task 9 覆盖。
