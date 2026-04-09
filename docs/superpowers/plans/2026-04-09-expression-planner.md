# Expression Planner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `zippy.EXPR(...)` with planner-backed `zippy.Expr(...)`, enforce uppercase function names, and add first-phase DAG-planned `TS_*` expression support for `ReactiveStateEngine`.

**Architecture:** Keep parsing, planning, and execution separate. Extend the existing expression parser into a typed planner that lowers to reusable node kinds, then execute `ColumnOp` nodes through column evaluation and `TsOp` nodes through the existing reactive operator machinery. `ReactiveStateEngine` gets full `ColumnOp + TsOp` support, while `TimeSeriesEngine` explicitly rejects `TsOp` in `pre_factors` and `post_factors`.

**Tech Stack:** Rust, PyO3, Arrow, existing `ReactiveFactor` operator stack, Python API wrappers, pytest, cargo test.

---

### Task 1: Rename Public Python API to `Expr`

**Files:**
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`
- Modify: `examples/python/README.md`
- Modify: `examples/python/publish_pipeline.py`
- Modify: `examples/python/cross_sectional_pipeline.py`

- [ ] **Step 1: Write the failing Python API tests**

Add tests in `pytests/test_python_api.py` that assert:

```python
def test_expr_factory_replaces_expr():
    import zippy

    factor = zippy.Expr(expression="ABS(price)", output="score")
    assert factor.expression == "ABS(price)"
    assert factor.output == "score"
    assert not hasattr(zippy, "EXPR")
```

- [ ] **Step 2: Run the focused pytest to verify it fails**

Run:

```bash
uv run python -m pytest pytests/test_python_api.py::test_expr_factory_replaces_expr -v
```

Expected: FAIL because `zippy.Expr` does not exist or `zippy.EXPR` still exists.

- [ ] **Step 3: Replace the Python helper**

Update `python/zippy/__init__.py` to expose:

```python
def Expr(*, expression: str, output: str) -> ExpressionFactor:
    """Create a planner-backed expression factor spec."""
    return ExpressionFactor(expression=expression, output=output)
```

Delete the `EXPR(...)` helper entirely and update `__all__` to export `Expr` instead.

Update `python/zippy/_internal.pyi` so public typing and examples use `Expr`.

- [ ] **Step 4: Update examples and docs**

Replace every `zippy.EXPR(...)` occurrence in:

- `examples/python/README.md`
- `examples/python/publish_pipeline.py`
- `examples/python/cross_sectional_pipeline.py`

with `zippy.Expr(...)`.

- [ ] **Step 5: Re-run focused pytest**

Run:

```bash
uv run python -m pytest pytests/test_python_api.py::test_expr_factory_replaces_expr -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add python/zippy/__init__.py python/zippy/_internal.pyi pytests/test_python_api.py examples/python/README.md examples/python/publish_pipeline.py examples/python/cross_sectional_pipeline.py
git commit -m "feat: rename expression helper to Expr"
```

### Task 2: Enforce Uppercase Function Names in the Expression Parser

**Files:**
- Modify: `crates/zippy-operators/src/expression.rs`
- Modify: `crates/zippy-operators/tests/reactive_factors.rs`

- [ ] **Step 1: Write failing parser tests**

Add tests in `crates/zippy-operators/tests/reactive_factors.rs` that assert:

```rust
#[test]
fn expression_rejects_lowercase_function_names() {
    let error = ExpressionSpec::new("abs(value)", "score")
        .build(input_schema().as_ref())
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("function names must be uppercase"));
}

#[test]
fn expression_accepts_uppercase_functions() {
    let mut factor = ExpressionSpec::new("ABS(value)", "score")
        .build(input_schema().as_ref())
        .unwrap();

    let output = factor.evaluate(&input_batch()).unwrap();
    assert_eq!(output.len(), input_batch().num_rows());
}
```

- [ ] **Step 2: Run the focused Rust test and verify failure**

Run:

```bash
cargo test -p zippy-operators expression_rejects_lowercase_function_names -- --exact
```

Expected: FAIL because lowercase handling is still accepted or reports the wrong error.

- [ ] **Step 3: Tighten parser function resolution**

In `crates/zippy-operators/src/expression.rs`:

- detect function identifiers before normalization
- reject any recognized lowercase form with:

```rust
return Err(ZippyError::InvalidConfig {
    reason: format!(
        "function names must be uppercase function=[{}] expected=[{}]",
        actual,
        expected
    ),
});
```

- keep accepted built-ins in uppercase only:
  - `ABS`
  - `LOG`
  - `CLIP`
  - `CAST`

- [ ] **Step 4: Re-run focused Rust tests**

Run:

```bash
cargo test -p zippy-operators expression_rejects_lowercase_function_names -- --exact
cargo test -p zippy-operators expression_accepts_uppercase_functions -- --exact
```

Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/expression.rs crates/zippy-operators/tests/reactive_factors.rs
git commit -m "feat: enforce uppercase expression functions"
```

### Task 3: Introduce Typed Planner Nodes and Common Subexpression Elimination

**Files:**
- Modify: `crates/zippy-operators/src/expression.rs`
- Test: `crates/zippy-operators/tests/reactive_factors.rs`

- [ ] **Step 1: Write failing planner tests**

Add focused tests that assert repeated `TS_DIFF(...)` is planned once:

```rust
#[test]
fn expression_planner_deduplicates_repeated_ts_diff_subexpressions() {
    let plan = ExpressionSpec::new(
        "TS_DIFF(value, 2) / TS_STD(TS_DIFF(value, 2), 3)",
        "score",
    )
    .build_reactive_plan(input_schema().as_ref(), "id")
    .unwrap();

    assert_eq!(plan.stateful_node_count("TS_DIFF"), 1);
}
```

- [ ] **Step 2: Run the focused Rust test and verify failure**

Run:

```bash
cargo test -p zippy-operators expression_planner_deduplicates_repeated_ts_diff_subexpressions -- --exact
```

Expected: FAIL because planner APIs or node counting do not exist yet.

- [ ] **Step 3: Add planner data structures**

Extend `crates/zippy-operators/src/expression.rs` with:

- typed AST-to-plan lowering
- node kinds:
  - `Input`
  - `Literal`
  - `ColumnOp`
  - `TsOp`
- stable node IDs
- memoization for repeated subexpressions

Use a dedicated internal plan type so later executors can consume it without
re-parsing.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
cargo test -p zippy-operators expression_planner_deduplicates_repeated_ts_diff_subexpressions -- --exact
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/expression.rs crates/zippy-operators/tests/reactive_factors.rs
git commit -m "feat: add typed expression planner dag"
```

### Task 4: Support `TS_*` Expressions in `ReactiveStateEngine`

**Files:**
- Modify: `crates/zippy-operators/src/expression.rs`
- Modify: `crates/zippy-engines/src/reactive.rs`
- Modify: `crates/zippy-engines/tests/reactive_engine.rs`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write failing reactive engine tests**

Add Rust and Python tests for:

```python
def test_reactive_expr_supports_ts_nodes():
    engine = zippy.ReactiveStateEngine(
        name="expr_reactive",
        input_schema=schema,
        id_column="id",
        factors=[
            zippy.Expr(
                expression="TS_DIFF(price, 2) / TS_STD(TS_DIFF(price, 2), 3)",
                output="score",
            ),
        ],
        target=zippy.NullPublisher(),
    )
```

Expected assertions:
- construction succeeds
- output schema includes `score`
- write path returns aligned rows

- [ ] **Step 2: Run focused tests to verify failure**

Run:

```bash
cargo test -p zippy-engines reactive -- --nocapture
uv run python -m pytest pytests/test_python_api.py -k "reactive_expr_supports_ts_nodes" -v
```

Expected: FAIL because `Expr` cannot yet plan or execute `TS_*`.

- [ ] **Step 3: Connect planner output to reactive execution**

Implement execution by:

- mapping `ColumnOp` nodes to column-expression evaluation
- mapping `TsOp` nodes onto existing reactive stateful operator machinery
- reusing computed intermediate nodes inside one expression

Do not introduce a second independent stateful execution path.

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
cargo test -p zippy-engines reactive -- --nocapture
uv run python -m pytest pytests/test_python_api.py -k "reactive_expr_supports_ts_nodes" -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-operators/src/expression.rs crates/zippy-engines/src/reactive.rs crates/zippy-engines/tests/reactive_engine.rs pytests/test_python_api.py
git commit -m "feat: support ts expression planning in reactive engine"
```

### Task 5: Reject `TS_*` Expression Nodes in `TimeSeriesEngine`

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `crates/zippy-engines/tests/timeseries_engine.rs`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write failing validation tests**

Add tests for:

```python
def test_timeseries_pre_factors_reject_ts_expr_nodes():
    with pytest.raises(ValueError, match="stateful TS_\\* functions are only supported inside ReactiveStateEngine"):
        zippy.TimeSeriesEngine(
            ...,
            pre_factors=[
                zippy.Expr(expression="TS_DIFF(price, 2)", output="bad"),
            ],
        )
```

Mirror the same for `post_factors`.

- [ ] **Step 2: Run focused tests to verify failure**

Run:

```bash
cargo test -p zippy-engines timeseries -- --nocapture
uv run python -m pytest pytests/test_python_api.py -k "timeseries_pre_factors_reject_ts_expr_nodes or timeseries_post_factors_reject_ts_expr_nodes" -v
```

Expected: FAIL because validation does not exist yet.

- [ ] **Step 3: Add planner capability checks**

When building `TimeSeriesEngine` expression specs:

- allow `ColumnOp` only
- reject any planned `TsOp`
- produce an explicit construction-time error mentioning `ReactiveStateEngine`

- [ ] **Step 4: Re-run focused tests**

Run:

```bash
cargo test -p zippy-engines timeseries -- --nocapture
uv run python -m pytest pytests/test_python_api.py -k "timeseries_pre_factors_reject_ts_expr_nodes or timeseries_post_factors_reject_ts_expr_nodes" -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-python/src/lib.rs crates/zippy-engines/tests/timeseries_engine.rs pytests/test_python_api.py
git commit -m "feat: reject ts expression nodes in timeseries engine"
```

### Task 6: Full Regression for API, Docs, and Planner Semantics

**Files:**
- Modify: `examples/python/README.md`
- Modify: `examples/python/publish_pipeline.py`
- Modify: `examples/python/cross_sectional_pipeline.py`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Add final regression assertions**

Ensure tests cover:

- `zippy.Expr("ABS(price)", ...)`
- lowercase function rejection
- `ReactiveStateEngine` `TS_*` expression success
- `TimeSeriesEngine` `TS_*` expression rejection
- no remaining `zippy.EXPR(...)` references

- [ ] **Step 2: Verify there are no stale `EXPR` references**

Run:

```bash
rg -n "EXPR\\(" python crates pytests examples plugins
```

Expected: no results.

- [ ] **Step 3: Run the full verification set**

Run:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml
uv run python -m pytest pytests -v
uv run python -m py_compile examples/python/*.py
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add examples/python/README.md examples/python/publish_pipeline.py examples/python/cross_sectional_pipeline.py pytests/test_python_api.py
git commit -m "test: cover expression planner api and docs"
```

