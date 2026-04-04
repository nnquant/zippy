# Python Policy Constants Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Python string policy parameters with predefined constants for `window_type`, `late_data_policy`, and `overflow_policy`.

**Architecture:** Keep Rust runtime enums unchanged and implement strict constant parsing at the Python binding boundary. Export lightweight constant objects from `python/zippy/__init__.py`, update type stubs and examples, and enforce the new API with test-first changes.

**Tech Stack:** PyO3, Python module helpers, pytest, cargo test, cargo clippy

---

### Task 1: Lock the New Python API in Tests

**Files:**
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing tests**

Add tests that:

- use `zippy.WindowType.TUMBLING`
- use `zippy.LateDataPolicy.REJECT`
- use `zippy.OverflowPolicy.BLOCK`
- reject old strings like `"tumbling"` and `"reject"`
- reject wrong-category constants

- [ ] **Step 2: Run the targeted tests to verify they fail**

Run: `uv run --extra dev pytest pytests/test_python_api.py -k 'window_type or late_data_policy or overflow_policy' -v`

Expected: FAIL because the constants are not implemented yet and/or strings are still accepted.

- [ ] **Step 3: Update existing tests to new constant-based calls**

Replace old string arguments across the Python API tests with the new constants so the suite reflects the new contract.

- [ ] **Step 4: Re-run the targeted tests and confirm the red state is still correct**

Run: `uv run --extra dev pytest pytests/test_python_api.py -k 'window_type or late_data_policy or overflow_policy' -v`

Expected: FAIL only because parsing/export logic is missing.

### Task 2: Implement Python Constant Objects and Binding Parsers

**Files:**
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Modify: `crates/zippy-python/src/lib.rs`

- [ ] **Step 1: Add lightweight constant objects in Python**

Create constant namespaces for:

- `WindowType`
- `LateDataPolicy`
- `OverflowPolicy`

Each constant should carry stable hidden attributes for kind and value, and a readable repr.

- [ ] **Step 2: Update type stubs to use constant object types**

Change constructor signatures so:

- `TimeSeriesEngine.window_type` expects a window-type constant
- `TimeSeriesEngine.late_data_policy` expects a late-data-policy constant
- `ReactiveStateEngine.overflow_policy` and `TimeSeriesEngine.overflow_policy` expect an overflow-policy constant

- [ ] **Step 3: Change PyO3 constructors to accept Python objects instead of strings**

Update parsing helpers in `crates/zippy-python/src/lib.rs` to:

- read constant kind/value attributes
- reject old strings
- reject wrong-category constants
- keep omitted defaults for `window_type` and `overflow_policy`

- [ ] **Step 4: Run focused Rust and Python checks**

Run:

- `cargo test -p zippy-python`
- `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
- `uv run --extra dev pytest pytests/test_python_api.py -k 'window_type or late_data_policy or overflow_policy' -v`

Expected: PASS

### Task 3: Align Examples, Docs, and Config Expectations

**Files:**
- Modify: `examples/python/README.md`
- Modify: `examples/python/publish_pipeline.py`
- Modify: `examples/python/subscribe_bars.py`
- Modify: `examples/python/archive_reactive.py`

- [ ] **Step 1: Replace string policy arguments in examples**

Switch examples to:

- `zippy.WindowType.TUMBLING`
- `zippy.LateDataPolicy.REJECT`
- `zippy.OverflowPolicy.BLOCK`

- [ ] **Step 2: Document the migration in the examples README**

Explain that policy-like parameters now use predefined constants rather than strings.

- [ ] **Step 3: Verify config output stays string-based**

Ensure tests still assert values like `"reject"` and `"block"` inside `engine.config()`.

- [ ] **Step 4: Run example syntax checks**

Run: `uv run python -m py_compile examples/python/*.py`

Expected: PASS

### Task 4: Run Full Verification and Commit

**Files:**
- Modify: `docs/superpowers/specs/2026-04-04-python-policy-constants-design.md`
- Modify: `docs/superpowers/plans/2026-04-04-python-policy-constants.md`

- [ ] **Step 1: Re-read the spec and ensure implementation matches**

Check:

- no string compatibility remains
- wrong-category constants are rejected
- config output still uses stable strings

- [ ] **Step 2: Run full project verification**

Run:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `uv run maturin develop --manifest-path crates/zippy-python/Cargo.toml`
- `uv run --extra dev pytest pytests -v`
- `uv run python -m py_compile examples/python/*.py`

Expected: PASS

- [ ] **Step 3: Commit the change**

Run:

```bash
git add docs/superpowers/specs/2026-04-04-python-policy-constants-design.md \
  docs/superpowers/plans/2026-04-04-python-policy-constants.md \
  crates/zippy-python/src/lib.rs \
  python/zippy/__init__.py \
  python/zippy/_internal.pyi \
  pytests/test_python_api.py \
  examples/python/README.md \
  examples/python/publish_pipeline.py \
  examples/python/subscribe_bars.py \
  examples/python/archive_reactive.py
git commit -m "feat: replace python policy strings with constants"
```
