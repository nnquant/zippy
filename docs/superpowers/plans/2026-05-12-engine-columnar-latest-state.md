# Engine Columnar Latest State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the latest/key-value `OwnedRow` state paths with a shared slot-indexed columnar state while preserving public behavior.

**Architecture:** Add an internal `LatestColumnarState` module in `zippy-engines`. `ReactiveLatestEngine` uses it to emit per-batch delta rows, and `KeyValueTableMaterializer` uses it to build full snapshots before the existing `replace_with_table()` publish path. The first implementation keeps deterministic key ordering and does not change stream-table descriptor or active segment update semantics.

**Tech Stack:** Rust 2021, Arrow arrays/builders, existing `zippy_core::Engine`, `SegmentTableView`, `cargo test`, `cargo clippy`.

---

## File Map

- Create `crates/zippy-engines/src/latest_state.rs`
  - Owns `LatestColumnarState`, `LatestUpdateSet`, typed column stores, validation, batch apply, delta materialization, and snapshot materialization.
- Modify `crates/zippy-engines/src/lib.rs`
  - Add private `mod latest_state;`.
- Modify `crates/zippy-engines/src/reactive_latest.rs`
  - Replace local `BTreeMap<Vec<String>, OwnedRow>` with `LatestColumnarState`.
  - Remove local `OwnedRow`, `LatestBatchUpdates`, `extract_owned_row`, and duplicate key helpers.
- Modify `crates/zippy-engines/src/stream_table.rs`
  - Replace `KeyValueTableMaterializer.latest_rows` with `LatestColumnarState`.
  - Remove `KeyValueOwnedRow` and duplicate key-value latest helpers if they become unused.
  - Keep `StreamTableMaterializer.replace_with_table()` unchanged.
- Modify `crates/zippy-engines/tests/reactive_latest_engine.rs`
  - Add nullable value and rollback-on-invalid-input behavior tests.
- Modify `crates/zippy-engines/tests/stream_table_materializer.rs`
  - Add same-batch duplicate key and nullable snapshot tests.
- Modify `docs/performance_audit_fix_status_2026-05-12.md`
  - Update P006 remaining status after verification.

## Task 1: Lock Latest Behavior With Failing Tests

**Files:**
- Modify: `crates/zippy-engines/tests/reactive_latest_engine.rs`
- Modify: `crates/zippy-engines/tests/stream_table_materializer.rs`

- [ ] **Step 1: Add ReactiveLatest nullable and invalid-input tests**

Append to `crates/zippy-engines/tests/reactive_latest_engine.rs`:

```rust
fn nullable_input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Utf8, false),
        Field::new("last_price", DataType::Float64, true),
    ]))
}

fn nullable_batch(
    instrument_ids: Vec<&str>,
    exchange_ids: Vec<&str>,
    last_prices: Vec<Option<f64>>,
) -> RecordBatch {
    RecordBatch::try_new(
        nullable_input_schema(),
        vec![
            Arc::new(StringArray::from(instrument_ids)) as ArrayRef,
            Arc::new(StringArray::from(exchange_ids)) as ArrayRef,
            Arc::new(Float64Array::from(last_prices)) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn reactive_latest_engine_preserves_nullable_values_in_snapshot() {
    let mut engine = ReactiveLatestEngine::new(
        "latest_ticks",
        nullable_input_schema(),
        vec!["instrument_id"],
    )
    .unwrap();

    engine
        .on_data(SegmentTableView::from_record_batch(nullable_batch(
            vec!["IF2606", "IH2606"],
            vec!["CFFEX", "CFFEX"],
            vec![Some(3912.4), None],
        )))
        .unwrap();

    let output = engine.on_flush().unwrap()[0].to_record_batch().unwrap();
    let prices = output
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(output.num_rows(), 2);
    assert_eq!(prices.value(0), 3912.4);
    assert!(prices.is_null(1));
}

#[test]
fn reactive_latest_engine_invalid_key_batch_leaves_existing_state_unchanged() {
    let mut engine =
        ReactiveLatestEngine::new("latest_ticks", input_schema(), vec!["instrument_id"]).unwrap();
    engine
        .on_data(SegmentTableView::from_record_batch(batch(
            vec!["IF2606"],
            vec!["CFFEX"],
            vec![3912.4],
        )))
        .unwrap();

    let invalid = RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(vec![None, Some("IH2606")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("CFFEX"), Some("CFFEX")])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let error = engine
        .on_data(SegmentTableView::from_record_batch(invalid))
        .unwrap_err();
    assert!(error.to_string().contains("contains null"));

    let output = engine.on_flush().unwrap()[0].to_record_batch().unwrap();
    assert_eq!(string_values(&output, 0), vec!["IF2606"]);
    assert_eq!(float_values(&output, 2), vec![3912.4]);
}
```

- [ ] **Step 2: Add KeyValue same-batch duplicate-key test**

Append to `crates/zippy-engines/tests/stream_table_materializer.rs`:

```rust
#[test]
fn key_value_table_materializer_keeps_last_row_for_duplicate_key_in_same_batch() {
    let mut materializer =
        KeyValueTableMaterializer::new("ticks_latest", input_schema(), vec!["instrument_id"])
            .unwrap();

    materializer
        .on_data(SegmentTableView::from_record_batch(latest_update_batch(
            vec!["IF2606", "IH2606", "IF2606"],
            vec![4102.5, 2711.0, 4103.5],
        )))
        .unwrap();

    let active = materializer.active_record_batch().unwrap();
    let instrument_ids = active
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let last_prices = active
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(active.num_rows(), 2);
    assert_eq!(instrument_ids.value(0), "IF2606");
    assert_eq!(last_prices.value(0), 4103.5);
    assert_eq!(instrument_ids.value(1), "IH2606");
    assert_eq!(last_prices.value(1), 2711.0);
}
```

- [ ] **Step 3: Run tests and verify at least the nullable/invalid tests fail before implementation**

Run:

```bash
cargo test -p zippy-engines --test reactive_latest_engine reactive_latest_engine_preserves_nullable_values_in_snapshot -- --nocapture
cargo test -p zippy-engines --test reactive_latest_engine reactive_latest_engine_invalid_key_batch_leaves_existing_state_unchanged -- --nocapture
cargo test -p zippy-engines --test stream_table_materializer key_value_table_materializer_keeps_last_row_for_duplicate_key_in_same_batch -- --nocapture
```

Expected:

- nullable value test currently fails because old row extraction path rejects null in `record_batch_from_table_rows()` / stream-table row writing boundaries or does not preserve nullable state consistently.
- invalid key rollback test fails if state is partially updated before the null key error.
- duplicate-key test may already pass; if it passes, keep it as regression coverage.

- [ ] **Step 4: Commit red tests if they fail for the intended reason**

```bash
git add crates/zippy-engines/tests/reactive_latest_engine.rs crates/zippy-engines/tests/stream_table_materializer.rs
git commit -m "test: cover latest columnar state edge cases"
```

## Task 2: Add Internal LatestColumnarState

**Files:**
- Create: `crates/zippy-engines/src/latest_state.rs`
- Modify: `crates/zippy-engines/src/lib.rs`

- [ ] **Step 1: Create state module skeleton**

Create `crates/zippy-engines/src/latest_state.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result, SchemaRef, SegmentTableView, ZippyError};

pub(crate) struct LatestColumnarState {
    schema: SchemaRef,
    key_fields: Vec<String>,
    key_indices: Vec<usize>,
    key_to_slot: BTreeMap<Vec<String>, usize>,
    slots: Vec<LatestSlot>,
    columns: Vec<LatestColumnStore>,
    sequence: u64,
}

pub(crate) struct LatestUpdateSet {
    slots: Vec<usize>,
}

#[derive(Clone)]
struct LatestSlot {
    key: Vec<String>,
    occupied: bool,
    sequence: u64,
}

enum LatestColumnStore {
    Int64 { values: Vec<i64>, valid: Vec<bool> },
    Float64 { values: Vec<f64>, valid: Vec<bool> },
    Utf8 { values: Vec<String>, valid: Vec<bool> },
    TimestampNanosecond { values: Vec<i64>, valid: Vec<bool> },
}
```

Add to `crates/zippy-engines/src/lib.rs`:

```rust
mod latest_state;
```

- [ ] **Step 2: Implement constructor validation**

Implement:

```rust
impl LatestColumnarState {
    pub(crate) fn new(schema: SchemaRef, key_fields: Vec<String>) -> Result<Self> {
        if key_fields.is_empty() {
            return Err(ZippyError::InvalidConfig {
                reason: "latest state requires at least one key field".to_string(),
            });
        }
        let mut seen = BTreeSet::new();
        let key_indices = key_fields
            .iter()
            .map(|field_name| {
                if !seen.insert(field_name.clone()) {
                    return Err(ZippyError::InvalidConfig {
                        reason: format!("duplicate latest key field field=[{}]", field_name),
                    });
                }
                let (index, field) = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, field)| field.name() == field_name)
                    .ok_or_else(|| ZippyError::SchemaMismatch {
                        reason: format!("missing latest key field field=[{}]", field_name),
                    })?;
                if field.data_type() != &DataType::Utf8 {
                    return Err(ZippyError::SchemaMismatch {
                        reason: format!("latest key field must be utf8 field=[{}]", field_name),
                    });
                }
                Ok(index)
            })
            .collect::<Result<Vec<_>>>()?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| LatestColumnStore::new(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            key_fields,
            key_indices,
            key_to_slot: BTreeMap::new(),
            slots: Vec::new(),
            columns,
            sequence: 0,
        })
    }
}
```

- [ ] **Step 3: Implement typed column append/update/materialize**

Implement `LatestColumnStore::new`, `append_default`, `write_from_array`, and `materialize` for
`Int64`, `Float64`, `Utf8`, and `Timestamp(Nanosecond, _)`. Non-nullable fields must reject null
input with a message containing `non-nullable latest field received null`.

- [ ] **Step 4: Implement two-phase apply**

Add:

```rust
impl LatestColumnarState {
    pub(crate) fn apply_batch(&mut self, table: &SegmentTableView) -> Result<LatestUpdateSet> {
        if table.schema().as_ref() != self.schema.as_ref() {
            return Err(ZippyError::SchemaMismatch {
                reason: "latest state input schema mismatch".to_string(),
            });
        }
        let update = self.prepare_batch_update(table)?;
        self.commit_batch_update(update)
    }
}
```

`prepare_batch_update()` must read all key/value cells into an owned temporary structure before
mutating state. `commit_batch_update()` assigns slots and writes final values. If prepare fails,
existing state must remain unchanged.

- [ ] **Step 5: Implement materialization**

Add:

```rust
impl LatestColumnarState {
    pub(crate) fn is_empty(&self) -> bool {
        self.key_to_slot.is_empty()
    }

    pub(crate) fn materialize_update(&self, update: &LatestUpdateSet) -> Result<RecordBatch> {
        self.materialize_slots(&update.slots)
    }

    pub(crate) fn materialize_snapshot(&self) -> Result<RecordBatch> {
        let slots = self.key_to_slot.values().copied().collect::<Vec<_>>();
        self.materialize_slots(&slots)
    }
}
```

Materialization order must follow sorted key order. Empty materialization returns
`RecordBatch::new_empty(schema)`.

- [ ] **Step 6: Run module-level compilation**

Run:

```bash
cargo test -p zippy-engines --lib
```

Expected: compile succeeds; if behavior is not yet wired into engines, existing tests still pass.

- [ ] **Step 7: Commit**

```bash
git add crates/zippy-engines/src/latest_state.rs crates/zippy-engines/src/lib.rs
git commit -m "feat: add latest columnar state"
```

## Task 3: Wire ReactiveLatestEngine To Shared State

**Files:**
- Modify: `crates/zippy-engines/src/reactive_latest.rs`
- Test: `crates/zippy-engines/tests/reactive_latest_engine.rs`

- [ ] **Step 1: Replace fields and constructor**

Change `ReactiveLatestEngine` to:

```rust
pub struct ReactiveLatestEngine {
    name: String,
    input_schema: SchemaRef,
    state: LatestColumnarState,
}
```

In `new()`, keep `validate_by_columns(...)`, then build:

```rust
let state = LatestColumnarState::new(Arc::clone(&input_schema), by)?;
```

Keep `by()` by returning `self.state.key_fields()` or remove only if no tests/API use it. Prefer adding
`LatestColumnarState::key_fields(&self) -> &[String]`.

- [ ] **Step 2: Replace on_data/on_flush**

Use:

```rust
let update = self.state.apply_batch(&table)?;
if update.is_empty() {
    return Ok(vec![]);
}
let output = self.state.materialize_update(&update)?;
Ok(vec![SegmentTableView::from_record_batch(output)])
```

Flush:

```rust
if self.state.is_empty() {
    return Ok(vec![]);
}
let output = self.state.materialize_snapshot()?;
Ok(vec![SegmentTableView::from_record_batch(output)])
```

- [ ] **Step 3: Remove duplicate local row/key helpers**

Delete local `OwnedRow`, `LatestBatchUpdates`, `build_key`, `collect_latest_updates`,
`collect_latest_updates_with_columns`, and `extract_owned_row` if unused.

- [ ] **Step 4: Run ReactiveLatest tests**

Run:

```bash
cargo test -p zippy-engines --test reactive_latest_engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-engines/src/reactive_latest.rs crates/zippy-engines/tests/reactive_latest_engine.rs
git commit -m "refactor: use columnar state for reactive latest"
```

## Task 4: Wire KeyValueTableMaterializer To Shared State

**Files:**
- Modify: `crates/zippy-engines/src/stream_table.rs`
- Test: `crates/zippy-engines/tests/stream_table_materializer.rs`

- [ ] **Step 1: Replace materializer field**

Change `KeyValueTableMaterializer`:

```rust
pub struct KeyValueTableMaterializer {
    name: String,
    input_schema: SchemaRef,
    by: Vec<String>,
    latest_state: LatestColumnarState,
    table: StreamTableMaterializer,
}
```

Constructor:

```rust
let latest_state = LatestColumnarState::new(Arc::clone(&input_schema), by.clone())?;
```

- [ ] **Step 2: Replace update and snapshot helpers**

Remove `update_latest_rows()` and `build_snapshot_batch()` implementations based on
`latest_rows`.

In `on_data()`:

```rust
let update = self.latest_state.apply_batch(&table)?;
if update.is_empty() {
    return Ok(vec![]);
}
let snapshot = self.latest_state.materialize_snapshot()?;
let view = SegmentTableView::from_record_batch(snapshot);
self.table.replace_with_table(&view)?;
Ok(vec![view])
```

In `on_flush()` and `on_stop()`, use `latest_state.materialize_snapshot()`.

- [ ] **Step 3: Remove duplicate key-value latest helpers**

Delete `KeyValueOwnedRow`, `latest_extract_owned_row`, `latest_build_key`, and
`latest_string_array` if no longer used.

- [ ] **Step 4: Run stream table materializer tests**

Run:

```bash
cargo test -p zippy-engines --test stream_table_materializer -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-engines/src/stream_table.rs crates/zippy-engines/tests/stream_table_materializer.rs
git commit -m "refactor: use columnar state for key value materializer"
```

## Task 5: Verification And Audit Status

**Files:**
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Run full zippy-engines verification**

Run:

```bash
cargo test -p zippy-engines --lib
cargo test -p zippy-engines --test reactive_latest_engine
cargo test -p zippy-engines --test stream_table_materializer
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

Expected: all PASS.

- [ ] **Step 2: Update audit status**

In `docs/performance_audit_fix_status_2026-05-12.md`, update P006 to say:

```text
`ReactiveLatestEngine` and `KeyValueTableMaterializer` now share a slot-indexed
columnar latest state. The remaining P006 work is active-segment slot-level update
or incremental snapshot publishing.
```

- [ ] **Step 3: Final status check**

Run:

```bash
git status --short
git diff --check
```

Expected: only the audit status doc is modified; diff check is clean.

- [ ] **Step 4: Commit**

```bash
git add docs/performance_audit_fix_status_2026-05-12.md
git commit -m "docs: update latest columnar state status"
```

## Self-Review

- Spec coverage: P006 pilot is covered by Tasks 1-4. P004/P005/P007 remain explicitly out of scope.
- Public API stability: no public Rust/Python constructors change.
- Failure boundary: tests cover invalid key batch leaving existing state unchanged.
- Deterministic order: existing latest/key-value tests plus duplicate-key regression preserve key-sorted output.
- Placeholder scan: no task relies on undefined future work; active segment slot update remains a documented follow-up.
