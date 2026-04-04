# CrossSectionalEngine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first `CrossSectionalEngine` with event-time `ClockTrigger`, `latest_timestamp_only` bucket semantics, and three minimal `CS_*` operators (`CS_RANK`, `CS_ZSCORE`, `CS_DEMEAN`) across Rust and Python APIs.

**Architecture:** Add a new operator family in `zippy-operators` for bucket-local cross-sectional evaluation, then layer a dedicated `CrossSectionalEngine` in `zippy-engines` that owns current-bucket state and closes buckets when a larger event-time bucket arrives. Expose the engine and operator specs through PyO3, wire them into the existing runtime/source/target/parquet stack, and validate the design with Rust unit tests plus Python end-to-end tests.

**Tech Stack:** Rust, Arrow `RecordBatch`, PyO3, pytest, cargo test, cargo clippy, Criterion bench harness

**Implementation note:** The current worktree already contains unrelated modified files. Every commit in this plan must stage explicit paths only; do not use `git add -A`.

---

## File Structure

**Operator layer**
- Create: `crates/zippy-operators/src/cross_sectional.rs`
  Purpose: Define `CrossSectionalFactor` plus `CS_RANK` / `CS_ZSCORE` / `CS_DEMEAN` specs and evaluation logic.
- Modify: `crates/zippy-operators/src/lib.rs`
  Purpose: Export the new operator trait and specs.
- Create: `crates/zippy-operators/tests/cross_sectional_factors.rs`
  Purpose: Lock operator semantics independently of engine state.

**Engine layer**
- Create: `crates/zippy-engines/src/cross_sectional.rs`
  Purpose: Implement event-time bucket state, late-data handling, bucket close/flush, and output construction.
- Modify: `crates/zippy-engines/src/lib.rs`
  Purpose: Export `CrossSectionalEngine`.
- Create: `crates/zippy-engines/tests/cross_sectional_engine.rs`
  Purpose: Lock engine state-machine semantics, late-data behavior, output ordering, and source-friendly output schema.
- Modify: `crates/zippy-engines/Cargo.toml`
  Purpose: Register a new bench target if the bench is added in Task 4.
- Create: `crates/zippy-engines/benches/crosssectional_pipeline.rs`
  Purpose: Add a compile-checked benchmark harness for the new engine.

**Python layer**
- Modify: `crates/zippy-python/src/lib.rs`
  Purpose: Add PyO3 classes for `CrossSectionalEngine`, `CSRankSpec`, `CSZscoreSpec`, `CSDemeanSpec`, plus runtime wiring.
- Modify: `python/zippy/__init__.py`
  Purpose: Export Python helpers `CS_RANK`, `CS_ZSCORE`, `CS_DEMEAN`, and the new engine class.
- Modify: `python/zippy/_internal.pyi`
  Purpose: Add public type declarations for the new engine and factor specs.
- Modify: `pytests/test_python_api.py`
  Purpose: Add end-to-end tests for API shape, source chaining, ZMQ, and parquet output.

**Examples and docs**
- Create: `examples/python/cross_sectional_pipeline.py`
  Purpose: Show `TimeSeriesEngine -> CrossSectionalEngine -> ZmqPublisher`.
- Modify: `examples/python/README.md`
  Purpose: Document the new example and the engine’s event-time semantics.

---

### Task 1: Lock Cross-Sectional Operator Semantics in Rust Tests

**Files:**
- Create: `crates/zippy-operators/tests/cross_sectional_factors.rs`
- Modify: `crates/zippy-operators/src/lib.rs`

- [ ] **Step 1: Write the failing operator tests**

Add tests that lock:

- average rank for ties
- zero-variance z-score returning `0.0`
- null rows excluded from sample but returning null output
- output dtype stability (`float64`)

Use this test skeleton:

```rust
#[test]
fn cs_rank_uses_average_rank_for_ties() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "dt",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("ret_1m", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C"])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![0, 0, 0]).with_timezone("UTC"))
                as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 2.0])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut factor = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();
    let output = factor.evaluate(&batch).unwrap();
    let values = output.as_any().downcast_ref::<Float64Array>().unwrap();

    assert_eq!(values.value(0), 1.0);
    assert_eq!(values.value(1), 2.5);
    assert_eq!(values.value(2), 2.5);
}
```

- [ ] **Step 2: Run the targeted test to confirm red**

Run:

```bash
cargo test -p zippy-operators cs_rank_uses_average_rank_for_ties -- --exact
```

Expected: FAIL because `cross_sectional.rs` and `CSRankSpec` do not exist yet.

- [ ] **Step 3: Implement the minimal operator layer**

Create `crates/zippy-operators/src/cross_sectional.rs` with this shape:

```rust
pub trait CrossSectionalFactor: Send {
    fn output_field(&self) -> Field;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
}

pub struct CSRankSpec {
    value_field: String,
    output_field: String,
}

impl CSRankSpec {
    pub fn new(value_field: &str, output_field: &str) -> Self {
        Self {
            value_field: value_field.to_string(),
            output_field: output_field.to_string(),
        }
    }

    pub fn build(&self) -> Result<Box<dyn CrossSectionalFactor>> {
        Ok(Box::new(CSRankFactor {
            value_field: self.value_field.clone(),
            output_field: Field::new(&self.output_field, DataType::Float64, true),
        }))
    }
}
```

Add the same pattern for `CSZscoreSpec` and `CSDemeanSpec`, and export them from `crates/zippy-operators/src/lib.rs`:

```rust
pub mod cross_sectional;

pub use cross_sectional::{
    CSDemeanSpec, CSRankSpec, CSZscoreSpec, CrossSectionalFactor,
};
```

- [ ] **Step 4: Re-run operator tests until green**

Run:

```bash
cargo test -p zippy-operators cross_sectional_factors -- --nocapture
```

Expected: PASS with the new operator tests and no regression in existing `reactive_factors.rs`.

- [ ] **Step 5: Commit the operator layer**

Run:

```bash
git add \
  crates/zippy-operators/src/cross_sectional.rs \
  crates/zippy-operators/src/lib.rs \
  crates/zippy-operators/tests/cross_sectional_factors.rs
git commit -m "feat: add cross-sectional operator specs"
```

---

### Task 2: Lock and Implement CrossSectionalEngine State-Machine Semantics

**Files:**
- Create: `crates/zippy-engines/src/cross_sectional.rs`
- Modify: `crates/zippy-engines/src/lib.rs`
- Create: `crates/zippy-engines/tests/cross_sectional_engine.rs`

- [ ] **Step 1: Write the failing engine tests**

Add tests that lock:

- same-bucket duplicate `id` keeps the last row
- seeing a larger bucket emits the previous bucket
- `flush()` emits the current bucket
- late data on closed buckets returns `ZippyError::LateData`
- output rows are sorted by `id`

Use this test skeleton:

```rust
#[test]
fn cross_sectional_engine_keeps_last_row_per_id_within_bucket() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(
            vec!["A", "A", "B"],
            vec![1_000_000_000, 2_000_000_000, 3_000_000_000],
            vec![1.0, 3.0, 2.0],
        ))
        .unwrap();

    assert!(outputs.is_empty());
    let flushed = engine.on_flush().unwrap();
    assert_eq!(string_values(flushed[0].column(0)), vec!["A", "B"]);
    assert_eq!(float_values(flushed[0].column(2)), vec![2.0, 1.0]);
}
```

- [ ] **Step 2: Run targeted engine tests to confirm red**

Run:

```bash
cargo test -p zippy-engines cross_sectional_engine_keeps_last_row_per_id_within_bucket -- --exact
```

Expected: FAIL because `CrossSectionalEngine` is not implemented or exported yet.

- [ ] **Step 3: Implement the engine core with minimal owned-row state**

Create `crates/zippy-engines/src/cross_sectional.rs` with this shape:

```rust
pub struct CrossSectionalEngine {
    name: String,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    id_column: String,
    dt_column: String,
    trigger_interval_ns: i64,
    late_data_policy: LateDataPolicy,
    factors: Vec<Box<dyn CrossSectionalFactor>>,
    current_bucket_start: Option<i64>,
    current_rows: BTreeMap<String, OwnedRow>,
    last_closed_bucket_start: Option<i64>,
}

#[derive(Clone)]
struct OwnedRow {
    id: String,
    dt: i64,
    value_columns: Vec<OwnedScalar>,
}
```

Implement `on_data()` so it:

- aligns each row into `bucket_start`
- closes the current bucket before opening a larger one
- replaces `current_rows[id]` on same-bucket duplicates
- rejects rows for already closed buckets

Implement `on_flush()` by materializing the current `BTreeMap` into a `RecordBatch`, then evaluating all `CrossSectionalFactor`s and appending their outputs in declaration order.

Export the engine from `crates/zippy-engines/src/lib.rs`:

```rust
pub mod cross_sectional;
pub use cross_sectional::CrossSectionalEngine;
```

- [ ] **Step 4: Run the new engine tests and fix ordering/late-data issues**

Run:

```bash
cargo test -p zippy-engines cross_sectional_engine_ -- --nocapture
```

Expected: PASS for the new cross-sectional engine tests and no regression in `timeseries_engine.rs`.

- [ ] **Step 5: Commit the engine core**

Run:

```bash
git add \
  crates/zippy-engines/src/cross_sectional.rs \
  crates/zippy-engines/src/lib.rs \
  crates/zippy-engines/tests/cross_sectional_engine.rs
git commit -m "feat: add cross-sectional engine core"
```

---

### Task 3: Add Python Bindings and End-to-End API Tests

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing Python tests**

Add tests for:

- `CrossSectionalEngine(..., trigger_interval=Duration.minutes(1))`
- `CS_RANK` / `CS_ZSCORE` / `CS_DEMEAN` helpers
- `source=TimeSeriesEngine`
- `late_data_policy` rejecting unsupported constants

Use this test skeleton:

```python
def test_cross_sectional_engine_accepts_timeseries_source_pipeline() -> None:
    bars = zippy.TimeSeriesEngine(...)
    cs = zippy.CrossSectionalEngine(
        name="cs_1m",
        source=bars,
        input_schema=bars.output_schema(),
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.NullPublisher(),
    )
    assert cs.output_schema().names == ["symbol", "dt", "ret_rank"]
```

- [ ] **Step 2: Run targeted pytest to confirm red**

Run:

```bash
uv run --extra dev pytest pytests/test_python_api.py -k 'cross_sectional_engine' -v
```

Expected: FAIL because the Python binding does not expose `CrossSectionalEngine` or `CS_*`.

- [ ] **Step 3: Implement PyO3 classes and helper exports**

In `crates/zippy-python/src/lib.rs`, add:

```rust
#[pyclass]
struct CSRankSpec {
    column: String,
    output: String,
}

#[pyclass]
struct CrossSectionalEngine {
    name: String,
    id_column: String,
    dt_column: String,
    trigger_interval_ns: i64,
    input_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    target: Vec<TargetConfig>,
    parquet_sink: Option<ParquetSinkConfig>,
    runtime_options: RuntimeOptions,
    status: SharedStatus,
    metrics: SharedMetrics,
    archive: SharedArchive,
    handle: SharedHandle,
    engine: Option<RustCrossSectionalEngine>,
    downstreams: Vec<DownstreamLink>,
    _source_owner: Option<Py<PyAny>>,
}
```

Extend `python/zippy/__init__.py` with:

```python
def CS_RANK(*, column: str, output: str) -> CSRankSpec:
    return CSRankSpec(column=column, output=output)
```

Add matching declarations to `python/zippy/_internal.pyi`.

- [ ] **Step 4: Re-run targeted Python tests**

Run:

```bash
uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml
uv run --extra dev pytest pytests/test_python_api.py -k 'cross_sectional_engine' -v
```

Expected: PASS with new engine/operator bindings and source pipeline tests.

- [ ] **Step 5: Commit the Python layer**

Run:

```bash
git add \
  crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py \
  python/zippy/_internal.pyi \
  pytests/test_python_api.py
git commit -m "feat: expose cross-sectional engine in python"
```

---

### Task 4: Add Output Publishing, Example, and Benchmark Harness

**Files:**
- Create: `examples/python/cross_sectional_pipeline.py`
- Modify: `examples/python/README.md`
- Modify: `crates/zippy-engines/Cargo.toml`
- Create: `crates/zippy-engines/benches/crosssectional_pipeline.rs`

- [ ] **Step 1: Write the failing integration checks**

Add a Python example that demonstrates:

- `ReactiveStateEngine -> TimeSeriesEngine -> CrossSectionalEngine`
- `ZmqPublisher` output from the cross-sectional stage

Use this example skeleton:

```python
cs = zippy.CrossSectionalEngine(
    name="cs_1m",
    source=bars,
    input_schema=bars.output_schema(),
    id_column="symbol",
    dt_column="dt",
    trigger_interval=zippy.Duration.minutes(1),
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[
        zippy.CS_RANK(column="ret_1m", output="ret_rank"),
        zippy.CS_ZSCORE(column="ret_1m", output="ret_z"),
    ],
    target=zippy.ZmqPublisher(endpoint=CS_ENDPOINT),
)
```

- [ ] **Step 2: Add the bench harness**

Create `crates/zippy-engines/benches/crosssectional_pipeline.rs`:

```rust
fn bench_crosssectional_pipeline(c: &mut Criterion) {
    c.bench_function("crosssectional_pipeline_1024_rows", |b| {
        b.iter(|| {
            let mut engine = CrossSectionalEngine::new(
                "bench",
                schema.clone(),
                "symbol",
                "dt",
                60_000_000_000,
                LateDataPolicy::Reject,
                vec![CSRankSpec::new("ret_1m", "ret_rank").build().unwrap()],
            )
            .unwrap();
            let _ = engine.on_data(batch.clone()).unwrap();
            let _ = engine.on_flush().unwrap();
        })
    });
}
```

Register it in `crates/zippy-engines/Cargo.toml`:

```toml
[[bench]]
name = "crosssectional_pipeline"
harness = false
```

- [ ] **Step 3: Run syntax and bench compile checks**

Run:

```bash
uv run python -m py_compile examples/python/*.py
cargo bench -p zippy-engines --no-run
```

Expected: PASS

- [ ] **Step 4: Commit the example and bench harness**

Run:

```bash
git add \
  examples/python/cross_sectional_pipeline.py \
  examples/python/README.md \
  crates/zippy-engines/Cargo.toml \
  crates/zippy-engines/benches/crosssectional_pipeline.rs
git commit -m "docs: add cross-sectional pipeline example"
```

---

### Task 5: Full Verification and Final Scope Check

**Files:**
- Modify: `docs/superpowers/specs/2026-04-04-crosssectional-engine-design.md`
- Modify: `docs/superpowers/plans/2026-04-04-crosssectional-engine.md`

- [ ] **Step 1: Re-read the spec and verify plan coverage**

Check each spec section against code/tests:

- `ClockTrigger`
- event-time alignment
- `latest_timestamp_only`
- no fill-forward
- close old bucket on larger event time
- `CS_RANK` / `CS_ZSCORE` / `CS_DEMEAN`
- Python API shape
- source/target/parquet boundaries

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

- [ ] **Step 3: Commit the finished feature**

Run:

```bash
git add \
  docs/superpowers/specs/2026-04-04-crosssectional-engine-design.md \
  docs/superpowers/plans/2026-04-04-crosssectional-engine.md \
  crates/zippy-operators/src/cross_sectional.rs \
  crates/zippy-operators/src/lib.rs \
  crates/zippy-operators/tests/cross_sectional_factors.rs \
  crates/zippy-engines/src/cross_sectional.rs \
  crates/zippy-engines/src/lib.rs \
  crates/zippy-engines/tests/cross_sectional_engine.rs \
  crates/zippy-engines/Cargo.toml \
  crates/zippy-engines/benches/crosssectional_pipeline.rs \
  crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py \
  python/zippy/_internal.pyi \
  pytests/test_python_api.py \
  examples/python/cross_sectional_pipeline.py \
  examples/python/README.md
git commit -m "feat: add cross-sectional engine"
```
