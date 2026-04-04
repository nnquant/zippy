# TimeSeries Expression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `TimeSeriesEngine` with explicit `pre_factors` and `post_factors` expression stages while preserving deterministic window semantics, strict late-data handling, and the existing publisher/archive boundaries.

**Architecture:** Keep `AGG_*` as the only window-state operators. Add two Rust-side expression phases around aggregation: `pre_factors` run on accepted input rows before window state updates, and `post_factors` run on aggregated output rows before publish/archive. Python only exposes explicit `EXPR(...)` lists for these phases and performs construction-time validation through Rust schema compilation.

**Tech Stack:** Rust, PyO3, Arrow RecordBatch, pytest, cargo test, cargo clippy

---

### Task 1: Lock the New TimeSeries Semantics in Tests

**Files:**
- Modify: `crates/zippy-engines/tests/timeseries_engine.rs`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Add failing Rust tests for the new execution phases**

Add targeted tests that prove:

- `pre_factors` can create derived columns consumed by `AGG_*`
- `post_factors` can extend aggregate outputs
- `post_factors` cannot reference raw input-only columns
- `flush()` executes `post_factors`

- [ ] **Step 2: Add failing Python integration tests for the public API**

Cover:

- `TimeSeriesEngine(..., pre_factors=[zippy.EXPR(...)], post_factors=[zippy.EXPR(...)])`
- rejection of illegal `post_factors` references
- `flush()` returning output rows with `post_factors`

- [ ] **Step 3: Run focused tests and verify the red state**

Run:

- `cargo test -p zippy-engines timeseries_engine_pre_ -- --exact`
- `cargo test -p zippy-engines timeseries_engine_post_ -- --exact`
- `uv run --extra dev pytest pytests/test_python_api.py -k 'timeseries_engine and (pre_factors or post_factors or flush_runs_post_factors)' -v`

Expected: FAIL because `TimeSeriesEngine` does not support these phases yet.

### Task 2: Implement Rust TimeSeries Pre/Post Expression Phases

**Files:**
- Modify: `crates/zippy-engines/src/timeseries.rs`

- [ ] **Step 1: Extend the engine state and constructor**

Add:

- `pre_factors`
- `post_factors`
- `pre_schema`
- `agg_schema`

Update constructor validation so:

- `pre_factors` compile and extend `pre_schema` in declaration order
- `AGG_*` validate against `pre_schema`
- `post_factors` compile and extend `output_schema` in declaration order
- duplicate field names are rejected across all stages

- [ ] **Step 2: Apply late-data filtering before `pre_factors`**

Ensure:

- rejected late rows never enter `pre_factors`
- `late_rows_total` still counts original rejected rows only
- accepted rows keep stable ordering

- [ ] **Step 3: Evaluate `pre_factors` before window updates**

Build an intermediate batch with `pre_schema` and use it as the source for aggregation input arrays.

- [ ] **Step 4: Evaluate `post_factors` after aggregation and on `flush()`**

Make both `on_data()` and `on_flush()` run the same post-processing path over `agg_schema` batches before returning outputs.

- [ ] **Step 5: Re-run focused Rust tests**

Run:

- `cargo test -p zippy-engines timeseries_engine_ -- --nocapture`

Expected: PASS for all new TimeSeries expression tests.

### Task 3: Expose TimeSeries Expression Phases in Python

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`

- [ ] **Step 1: Add `pre_factors` and `post_factors` to the Python constructor**

Requirements:

- both parameters default to empty lists
- each list only accepts `zippy.EXPR(...)`
- parsing order matches Rust schema expansion

- [ ] **Step 2: Keep schema introspection aligned**

Ensure:

- `output_schema()` includes `post_factors`
- construction-time errors surface as `ValueError`
- `config()` remains stable and serializable

- [ ] **Step 3: Re-run focused Python tests**

Run:

- `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
- `uv run --extra dev pytest pytests/test_python_api.py -k 'timeseries_engine and (pre_factors or post_factors or flush_runs_post_factors)' -v`

Expected: PASS

### Task 4: Align Examples and User-Facing Docs

**Files:**
- Modify: `examples/python/README.md`
- Modify: `examples/python/publish_pipeline.py`

- [ ] **Step 1: Update the published example to show `pre_factors` and `post_factors`**

Use the constant-based policy API:

- `window_type=zippy.WindowType.TUMBLING`
- `late_data_policy=zippy.LateDataPolicy.REJECT`

- [ ] **Step 2: Document the execution order clearly**

Explain:

- late-data filtering happens before `pre_factors`
- `post_factors` operate on aggregate output rows
- `flush()` triggers `post_factors`

- [ ] **Step 3: Verify example syntax**

Run:

- `uv run python -m py_compile examples/python/*.py`

Expected: PASS

### Task 5: Run Full Verification and Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-04-04-timeseries-expression-design.md`
- Modify: `docs/superpowers/plans/2026-04-04-timeseries-expression.md`

- [ ] **Step 1: Re-read the spec and confirm implementation matches**

Check:

- `pre_factors` run after late-data filtering
- `post_factors` only see `agg_schema`
- raw input columns are unavailable to `post_factors`
- `flush()` follows the same post-processing path as normal window completion

- [ ] **Step 2: Run full project verification**

Run:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo bench -p zippy-engines --no-run`
- `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
- `uv run --extra dev pytest pytests -v`
- `uv run python -m py_compile examples/python/*.py`

Expected: PASS

- [ ] **Step 3: Commit the change**

Run:

```bash
git add docs/superpowers/specs/2026-04-04-timeseries-expression-design.md \
  docs/superpowers/plans/2026-04-04-timeseries-expression.md \
  crates/zippy-engines/src/timeseries.rs \
  crates/zippy-engines/tests/timeseries_engine.rs \
  crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py \
  python/zippy/_internal.pyi \
  pytests/test_python_api.py \
  examples/python/README.md \
  examples/python/publish_pipeline.py
git commit -m "feat: add timeseries expression phases"
```
