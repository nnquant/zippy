# TimeSeries State Transaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `TimeSeriesEngine::on_data()` batch-level state clone with touched-id rollback so successful batches mutate state in place and failed batches restore only affected ids.

**Architecture:** Add internal `TimeSeriesStateTxn`, `TimeSeriesRollback`, and undo records inside `crates/zippy-engines/src/timeseries.rs`. Row processing mutates `open_windows` and `last_dt_by_id` through the transaction, returns completed windows plus a rollback handle, then commits only after any required finalize succeeds. Existing aggregation, late-data, output schema, and public APIs remain unchanged.

**Tech Stack:** Rust, `BTreeMap`, Arrow `RecordBatch`, existing `zippy_core::Result` / `ZippyError`, Cargo tests and clippy.

---

## File Structure

- Modify `crates/zippy-engines/src/timeseries.rs`
  - Add `TimeSeriesStateTxn`, `TimeSeriesRollback`, `TimeSeriesStateUndo`, and `TimeSeriesTxnResult`.
  - Add an internal `apply_timeseries_rows()` helper.
  - Update `TimeSeriesEngine::on_data()` to use transaction state mutation instead of cloning full state maps.
  - Add unit tests for rollback behavior.
- Modify `crates/zippy-engines/tests/timeseries_engine.rs`
  - Add integration test that a finalize failure rolls back state.
- Modify `docs/performance_audit_fix_status_2026-05-12.md`
  - Update P004 status to note touched-id transaction rollback is implemented.

---

### Task 1: Add Failing Rollback Tests

**Files:**
- Modify: `crates/zippy-engines/src/timeseries.rs`
- Modify: `crates/zippy-engines/tests/timeseries_engine.rs`

- [ ] **Step 1: Add unit tests for transaction rollback**

In the existing `#[cfg(test)] mod tests` in `crates/zippy-engines/src/timeseries.rs`, extend the import:

```rust
use std::collections::BTreeMap;

use super::{OpenWindow, RowSelection, TimeSeriesRollback, TimeSeriesStateTxn};
```

Append these tests:

```rust
#[test]
fn time_series_state_txn_rollback_removes_new_id() {
    let mut open_windows = BTreeMap::new();
    let mut last_dt_by_id = BTreeMap::new();
    let mut txn = TimeSeriesStateTxn::new(&mut open_windows, &mut last_dt_by_id);

    txn.insert_open_window("A", OpenWindow::new("A".to_string(), 0, 10, 1));
    txn.set_last_dt("A", 5);
    let rollback = txn.into_rollback();

    rollback.restore(&mut open_windows, &mut last_dt_by_id);

    assert!(open_windows.is_empty());
    assert!(last_dt_by_id.is_empty());
}

#[test]
fn time_series_state_txn_rollback_restores_existing_id_once() {
    let original = OpenWindow::new("A".to_string(), 0, 10, 1);
    let mut open_windows = BTreeMap::from([("A".to_string(), original.clone())]);
    let mut last_dt_by_id = BTreeMap::from([("A".to_string(), 5)]);
    let mut txn = TimeSeriesStateTxn::new(&mut open_windows, &mut last_dt_by_id);

    txn.take_open_window("A");
    txn.insert_open_window("A", OpenWindow::new("A".to_string(), 10, 20, 1));
    txn.set_last_dt("A", 15);
    let rollback = txn.into_rollback();

    rollback.restore(&mut open_windows, &mut last_dt_by_id);

    assert_eq!(open_windows.get("A").unwrap().window_start, original.window_start);
    assert_eq!(open_windows.get("A").unwrap().window_end, original.window_end);
    assert_eq!(last_dt_by_id.get("A"), Some(&5));
}
```

- [ ] **Step 2: Add integration test for finalize failure rollback**

Append to `crates/zippy-engines/tests/timeseries_engine.rs`:

```rust
#[test]
fn timeseries_engine_rolls_back_state_when_finalize_fails() {
    let mut engine = TimeSeriesEngine::new(
        "bars",
        input_schema(),
        "id",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![AggVwapSpec::new("value", "weight", "vwap_value")
            .build()
            .unwrap()],
        vec![],
        vec![],
    )
    .unwrap();

    engine
        .on_data(batch(vec!["a"], vec![1_000_000_000], vec![10.0], vec![0.0]))
        .unwrap();

    let transition_error = engine
        .on_data(batch(
            vec!["a"],
            vec![MINUTE_NS + 1_000_000_000],
            vec![12.0],
            vec![1.0],
        ))
        .unwrap_err();

    assert!(matches!(
        transition_error,
        ZippyError::InvalidState {
            status: "vwap denominator is zero",
        }
    ));

    let flush_error = engine.on_flush().unwrap_err();

    assert!(matches!(
        flush_error,
        ZippyError::InvalidState {
            status: "vwap denominator is zero",
        }
    ));
}
```

- [ ] **Step 3: Run failing tests**

Run:

```bash
cargo test -p zippy-engines --lib timeseries::tests::time_series_state_txn -- --nocapture
cargo test -p zippy-engines --test timeseries_engine timeseries_engine_rolls_back_state_when_finalize_fails -- --nocapture
```

Expected: lib test fails to compile because transaction types do not exist. Integration test may pass under current full-clone behavior; it locks the required behavior before refactor.

---

### Task 2: Add Transaction Types

**Files:**
- Modify: `crates/zippy-engines/src/timeseries.rs`

- [ ] **Step 1: Add undo and rollback structs**

Add below `OpenWindow`:

```rust
struct TimeSeriesStateUndo {
    open_window: Option<OpenWindow>,
    last_dt: Option<i64>,
}

struct TimeSeriesRollback {
    undo: BTreeMap<String, TimeSeriesStateUndo>,
}

impl TimeSeriesRollback {
    fn restore(
        self,
        open_windows: &mut BTreeMap<String, OpenWindow>,
        last_dt_by_id: &mut BTreeMap<String, i64>,
    ) {
        for (id, undo) in self.undo {
            match undo.open_window {
                Some(open_window) => {
                    open_windows.insert(id.clone(), open_window);
                }
                None => {
                    open_windows.remove(&id);
                }
            }

            match undo.last_dt {
                Some(last_dt) => {
                    last_dt_by_id.insert(id, last_dt);
                }
                None => {
                    last_dt_by_id.remove(&id);
                }
            }
        }
    }
}
```

- [ ] **Step 2: Add transaction struct and methods**

Add:

```rust
struct TimeSeriesStateTxn<'a> {
    open_windows: &'a mut BTreeMap<String, OpenWindow>,
    last_dt_by_id: &'a mut BTreeMap<String, i64>,
    undo: BTreeMap<String, TimeSeriesStateUndo>,
}

impl<'a> TimeSeriesStateTxn<'a> {
    fn new(
        open_windows: &'a mut BTreeMap<String, OpenWindow>,
        last_dt_by_id: &'a mut BTreeMap<String, i64>,
    ) -> Self {
        Self {
            open_windows,
            last_dt_by_id,
            undo: BTreeMap::new(),
        }
    }

    fn take_open_window(&mut self, id: &str) -> Option<OpenWindow> {
        self.touch(id);
        self.open_windows.remove(id)
    }

    fn insert_open_window(&mut self, id: &str, open_window: OpenWindow) {
        self.touch(id);
        self.open_windows.insert(id.to_string(), open_window);
    }

    fn set_last_dt(&mut self, id: &str, dt: i64) {
        self.touch(id);
        self.last_dt_by_id.insert(id.to_string(), dt);
    }

    fn into_rollback(self) -> TimeSeriesRollback {
        TimeSeriesRollback { undo: self.undo }
    }

    fn touch(&mut self, id: &str) {
        if self.undo.contains_key(id) {
            return;
        }

        self.undo.insert(
            id.to_string(),
            TimeSeriesStateUndo {
                open_window: self.open_windows.get(id).cloned(),
                last_dt: self.last_dt_by_id.get(id).copied(),
            },
        );
    }
}
```

- [ ] **Step 3: Run unit tests**

Run:

```bash
cargo test -p zippy-engines --lib timeseries::tests::time_series_state_txn -- --nocapture
```

Expected: PASS for transaction rollback unit tests.

---

### Task 3: Refactor `on_data()` to Use Transaction

**Files:**
- Modify: `crates/zippy-engines/src/timeseries.rs`

- [ ] **Step 1: Add transaction result type**

Add:

```rust
struct TimeSeriesTxnResult {
    completed: Vec<OpenWindow>,
    rollback: TimeSeriesRollback,
}
```

- [ ] **Step 2: Add row application helper**

Add this helper near `collect_accepted_rows_for_rows()`:

```rust
fn apply_timeseries_rows(
    open_windows: &mut BTreeMap<String, OpenWindow>,
    last_dt_by_id: &mut BTreeMap<String, i64>,
    specs: &[Box<dyn AggregationSpec>],
    window_ns: i64,
    processed_input: &ProcessedInput<'_>,
) -> Result<TimeSeriesTxnResult> {
    let mut completed = Vec::new();
    let mut txn = TimeSeriesStateTxn::new(open_windows, last_dt_by_id);

    for row_index in processed_input.row_selection() {
        let id = processed_input.id_value(row_index)?.to_string();
        let dt = processed_input.dt_value(row_index)?;
        let window_start = align_window_start(dt, window_ns);
        let window_end = window_start.checked_add(window_ns).ok_or(ZippyError::InvalidState {
            status: "window end overflow",
        })?;

        match txn.take_open_window(&id) {
            Some(mut open_window) if open_window.window_start == window_start => {
                open_window.update(specs, processed_input.spec_inputs(), row_index)?;
                txn.insert_open_window(&id, open_window);
            }
            Some(open_window) => {
                completed.push(open_window);
                let mut next_window =
                    OpenWindow::new(id.clone(), window_start, window_end, specs.len());
                next_window.update(specs, processed_input.spec_inputs(), row_index)?;
                txn.insert_open_window(&id, next_window);
            }
            None => {
                let mut open_window =
                    OpenWindow::new(id.clone(), window_start, window_end, specs.len());
                open_window.update(specs, processed_input.spec_inputs(), row_index)?;
                txn.insert_open_window(&id, open_window);
            }
        }

        txn.set_last_dt(&id, dt);
    }

    Ok(TimeSeriesTxnResult {
        completed,
        rollback: txn.into_rollback(),
    })
}
```

- [ ] **Step 3: Replace clone-on-write block in `on_data()`**

Replace the block beginning with:

```rust
let mut completed = Vec::new();
let mut next_open_windows = self.open_windows.clone();
let mut next_last_dt_by_id = self.last_dt_by_id.clone();
```

through the final state assignment with:

```rust
let txn_result = match apply_timeseries_rows(
    &mut self.open_windows,
    &mut self.last_dt_by_id,
    &self.specs,
    self.window_ns,
    &processed_input,
) {
    Ok(result) => result,
    Err(error) => {
        return Err(error);
    }
};

if txn_result.completed.is_empty() {
    return Ok(vec![]);
}

match self.finalize_windows(txn_result.completed) {
    Ok(output) => Ok(vec![SegmentTableView::from_record_batch(output)]),
    Err(error) => {
        txn_result
            .rollback
            .restore(&mut self.open_windows, &mut self.last_dt_by_id);
        Err(error)
    }
}
```

If `apply_timeseries_rows()` returns an error after partially mutating state, update the helper to restore before returning:

```rust
let rollback = txn.into_rollback();
rollback.restore(open_windows, last_dt_by_id);
return Err(error);
```

- [ ] **Step 4: Run timeseries tests**

Run:

```bash
cargo test -p zippy-engines --test timeseries_engine -- --nocapture
```

Expected: PASS.

---

### Task 4: Update Docs and Verify

**Files:**
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Update P004 status**

Change P004 table text to mention transaction rollback. In the P004 section, add:

```markdown
- `on_data()` 改为 touched-id transaction rollback，不再每批 clone 全量 `open_windows` 和 `last_dt_by_id`。
- finalize 或 row update 失败时只恢复本批 touched ids，保持错误不污染状态语义。
```

Keep remaining boundary:

```markdown
- 每行 `id.to_string()` 和 `BTreeMap<String, ...>` 状态容器仍是剩余热点；id interning / slot arena 留待后续专项。
```

- [ ] **Step 2: Run final verification**

Run:

```bash
cargo test -p zippy-engines --lib timeseries::tests::time_series_state_txn -- --nocapture
cargo test -p zippy-engines --test timeseries_engine -- --nocapture
cargo test -p zippy-engines --lib -- --nocapture
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

Expected: all commands exit with code 0.

- [ ] **Step 3: Commit**

Run:

```bash
git add crates/zippy-engines/src/timeseries.rs crates/zippy-engines/tests/timeseries_engine.rs docs/performance_audit_fix_status_2026-05-12.md
git commit -m "refactor: use touched id transaction for timeseries state"
```
