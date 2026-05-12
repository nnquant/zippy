# Reactive Factor Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a compatible `ReactiveFactorContext` path so `ReactiveStateEngine` no longer rebuilds an intermediate `RecordBatch` for every built-in factor.

**Architecture:** Extend `zippy_operators::ReactiveFactor` with a default `evaluate_with_context()` fallback. Update `ReactiveStateEngine` to pass a lightweight context over the current schema and column vector. Migrate built-in reactive and expression factors to context-native evaluation while keeping the old `evaluate(&RecordBatch)` API working.

**Tech Stack:** Rust 2021, Arrow `RecordBatch` / `ArrayRef`, existing `zippy_core::Engine`, `cargo test`, `cargo clippy`.

---

## File Map

- Modify `crates/zippy-operators/src/reactive.rs`
  - Add `ReactiveFactorContext`.
  - Extend `ReactiveFactor` with `evaluate_with_context()`.
  - Add context-native helpers for `TsEmaFactor`, `WindowHistoryFactor`, `UnaryFloatFactor`, and `CastFactor`.
  - Add unit tests for context validation and lookup.
- Modify `crates/zippy-operators/src/expression.rs`
  - Add context-native evaluation for `ExpressionFactor` and `PlannedExpressionFactor`.
  - Add `extract_context_value()` and keep batch helpers compatible.
- Modify `crates/zippy-engines/src/reactive.rs`
  - Replace per-factor intermediate `RecordBatch::try_new(... columns.clone())` with `ReactiveFactorContext`.
- Modify `crates/zippy-engines/tests/reactive_engine.rs`
  - Add a test-only factor that fails if the engine calls the old `evaluate(&RecordBatch)` path.
- Modify `crates/zippy-engines/benches/reactive_pipeline.rs`
  - Add a multi-factor reactive benchmark profile.
- Modify `docs/performance_audit_fix_status_2026-05-12.md`
  - Update P007 status after verification.

## Task 1: Add A Failing Engine Test For Context Dispatch

**Files:**
- Modify: `crates/zippy-engines/tests/reactive_engine.rs`

- [ ] **Step 1: Add imports for test factor**

At the top of `crates/zippy-engines/tests/reactive_engine.rs`, change the `zippy_operators` import to include the new context type after Task 2. For the red test, first add the test with the expected future names:

```rust
use zippy_operators::{
    CastSpec, ExpressionSpec, ReactiveFactor, ReactiveFactorContext, TsDiffSpec, TsEmaSpec,
    TsReturnSpec,
};
```

- [ ] **Step 2: Add context-only factor test**

Append this test helper and test:

```rust
struct ContextOnlyFactor {
    output: Field,
}

impl ReactiveFactor for ContextOnlyFactor {
    fn output_field(&self) -> Field {
        self.output.clone()
    }

    fn evaluate(&mut self, _batch: &RecordBatch) -> zippy_core::Result<ArrayRef> {
        panic!("engine should call evaluate_with_context for reactive factors");
    }

    fn evaluate_with_context(
        &mut self,
        ctx: &ReactiveFactorContext<'_>,
    ) -> zippy_core::Result<ArrayRef> {
        let values = ctx
            .column_by_name("value")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        Ok(Arc::new(Float64Array::from(
            (0..ctx.num_rows())
                .map(|row| values.value(row) + 1.0)
                .collect::<Vec<_>>(),
        )) as ArrayRef)
    }
}

#[test]
fn reactive_engine_uses_context_evaluation_path() {
    let mut engine = ReactiveStateEngine::new(
        "reactive",
        input_schema(),
        vec![Box::new(ContextOnlyFactor {
            output: Field::new("value_plus_one", DataType::Float64, false),
        })],
    )
    .unwrap();

    let outputs = engine
        .on_data(batch(vec!["a", "b"], vec![10.0, 20.0]))
        .unwrap();
    let output = &outputs[0];

    assert_eq!(
        column_names(output),
        vec!["id", "value", "value_plus_one"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(float64_values(output.column_at(2)), vec![Some(11.0), Some(21.0)]);
}
```

- [ ] **Step 3: Run the red test**

Run:

```bash
cargo test -p zippy-engines --test reactive_engine reactive_engine_uses_context_evaluation_path -- --nocapture
```

Expected: FAIL to compile because `ReactiveFactorContext` does not exist yet, or fail at runtime because engine calls `evaluate()`.

- [ ] **Step 4: Commit the red test only if it fails for the intended reason**

```bash
git add crates/zippy-engines/tests/reactive_engine.rs
git commit -m "test: require reactive context evaluation path"
```

## Task 2: Add ReactiveFactorContext And Trait Fallback

**Files:**
- Modify: `crates/zippy-operators/src/reactive.rs`

- [ ] **Step 1: Add context type**

Add near the `ReactiveFactor` trait:

```rust
pub struct ReactiveFactorContext<'a> {
    schema: &'a Arc<Schema>,
    columns: &'a [ArrayRef],
    row_count: usize,
}
```

Add imports if needed:

```rust
use arrow::datatypes::{DataType, Field, Schema};
```

- [ ] **Step 2: Implement constructors and accessors**

Implement:

```rust
impl<'a> ReactiveFactorContext<'a> {
    pub fn new(schema: &'a Arc<Schema>, columns: &'a [ArrayRef]) -> Result<Self> {
        if schema.fields().len() != columns.len() {
            return Err(ZippyError::SchemaMismatch {
                reason: format!(
                    "reactive factor context schema column count mismatch schema_columns=[{}] columns=[{}]",
                    schema.fields().len(),
                    columns.len()
                ),
            });
        }
        let row_count = columns.first().map_or(0, |column| column.len());
        for (index, column) in columns.iter().enumerate() {
            if column.len() != row_count {
                return Err(ZippyError::SchemaMismatch {
                    reason: format!(
                        "reactive factor context column length mismatch column_index=[{}] expected=[{}] actual=[{}]",
                        index,
                        row_count,
                        column.len()
                    ),
                });
            }
        }
        Ok(Self {
            schema,
            columns,
            row_count,
        })
    }

    pub fn from_batch(batch: &'a RecordBatch) -> Self {
        Self {
            schema: &batch.schema(),
            columns: batch.columns(),
            row_count: batch.num_rows(),
        }
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    pub fn index_of(&self, field: &str) -> Result<usize> {
        self.schema.index_of(field).map_err(|_| ZippyError::SchemaMismatch {
            reason: format!("missing reactive factor field field=[{}]", field),
        })
    }

    pub fn column(&self, index: usize) -> Result<&'a ArrayRef> {
        self.columns.get(index).ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("reactive factor column index out of bounds index=[{}]", index),
        })
    }

    pub fn column_by_name(&self, field: &str) -> Result<&'a ArrayRef> {
        let index = self.index_of(field)?;
        self.column(index)
    }

    pub fn record_batch(&self) -> Result<RecordBatch> {
        RecordBatch::try_new(Arc::clone(self.schema), self.columns.to_vec()).map_err(|error| {
            ZippyError::Io {
                reason: format!("failed to build reactive context fallback batch error=[{}]", error),
            }
        })
    }
}
```

If `from_batch()` cannot return `Result` because no validation is needed, keep it infallible. If the borrow checker rejects `&batch.schema()` because it is a temporary, adjust by using `ReactiveFactorContext::new(&batch.schema(), batch.columns())?` and make `from_batch()` return `Result<Self>`.

- [ ] **Step 3: Extend trait with fallback**

Change `ReactiveFactor`:

```rust
fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
    let batch = ctx.record_batch()?;
    self.evaluate(&batch)
}
```

- [ ] **Step 4: Add context unit tests**

Add module tests in `crates/zippy-operators/src/reactive.rs`:

```rust
#[test]
fn reactive_factor_context_resolves_columns_by_name() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let columns = vec![
        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
    ];
    let ctx = ReactiveFactorContext::new(&schema, &columns).unwrap();

    assert_eq!(ctx.num_rows(), 2);
    assert_eq!(ctx.index_of("value").unwrap(), 1);
    assert_eq!(ctx.column_by_name("id").unwrap().len(), 2);
}

#[test]
fn reactive_factor_context_rejects_mismatched_column_lengths() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let columns = vec![
        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
    ];

    let error = ReactiveFactorContext::new(&schema, &columns).unwrap_err();
    assert!(error.to_string().contains("column length mismatch"));
}
```

- [ ] **Step 5: Run context tests**

Run:

```bash
cargo test -p zippy-operators reactive_factor_context -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-operators/src/reactive.rs
git commit -m "feat: add reactive factor context"
```

## Task 3: Route ReactiveStateEngine Through Context

**Files:**
- Modify: `crates/zippy-engines/src/reactive.rs`
- Test: `crates/zippy-engines/tests/reactive_engine.rs`

- [ ] **Step 1: Import context**

Change:

```rust
use zippy_operators::ReactiveFactor;
```

to:

```rust
use zippy_operators::{ReactiveFactor, ReactiveFactorContext};
```

- [ ] **Step 2: Replace intermediate batch in factor loop**

In `ReactiveStateEngine::evaluate_table()`, replace the per-factor `RecordBatch::try_new(...)` block with:

```rust
let context = ReactiveFactorContext::new(&current_schema, &columns)?;
let output_field = factor.output_field();
let output_column: ArrayRef = factor.evaluate_with_context(&context)?;
columns.push(output_column);
```

Keep schema extension and final output `RecordBatch::try_new(...)` unchanged.

- [ ] **Step 3: Run context dispatch test**

Run:

```bash
cargo test -p zippy-engines --test reactive_engine reactive_engine_uses_context_evaluation_path -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Run reactive engine tests**

Run:

```bash
cargo test -p zippy-engines --test reactive_engine
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-engines/src/reactive.rs crates/zippy-engines/tests/reactive_engine.rs
git commit -m "refactor: route reactive engine through factor context"
```

## Task 4: Make Built-In Reactive Factors Context-Native

**Files:**
- Modify: `crates/zippy-operators/src/reactive.rs`
- Test: `crates/zippy-operators/tests/reactive_factors.rs`

- [ ] **Step 1: Add context column helper**

Add:

```rust
fn extract_context_columns<'a>(
    ctx: &'a ReactiveFactorContext<'a>,
    id_field: &str,
    value_field: &str,
) -> Result<(&'a StringArray, &'a Float64Array)> {
    let id_array = ctx
        .column_by_name(id_field)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("id field must be utf8 field=[{}]", id_field),
        })?;
    let value_array = ctx
        .column_by_name(value_field)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| ZippyError::SchemaMismatch {
            reason: format!("value field must be float64 field=[{}]", value_field),
        })?;
    validate_id_value_arrays(id_array, value_array, id_field, value_field)?;
    Ok((id_array, value_array))
}
```

Extract the existing validation from `extract_columns()` into:

```rust
fn validate_id_value_arrays(
    id_array: &StringArray,
    value_array: &Float64Array,
    id_field: &str,
    value_field: &str,
) -> Result<()> { ... }
```

- [ ] **Step 2: Add context-native evaluate methods**

For `TsEmaFactor`, `WindowHistoryFactor`, `UnaryFloatFactor`, and `CastFactor`:

- Keep existing `evaluate(&RecordBatch)` as compatibility path.
- Add `evaluate_with_context(&ReactiveFactorContext<'_>)`.
- Move core logic into private helper methods that accept `&StringArray` / `&Float64Array` when possible.

Example for `TsEmaFactor`:

```rust
fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
    let (ids, values) = extract_context_columns(ctx, &self.id_field, &self.value_field)?;
    self.evaluate_arrays(ids, values)
}
```

- [ ] **Step 3: Run reactive factor tests**

Run:

```bash
cargo test -p zippy-operators --test reactive_factors
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/zippy-operators/src/reactive.rs
git commit -m "refactor: evaluate reactive factors from context"
```

## Task 5: Make Expression Factors Context-Native

**Files:**
- Modify: `crates/zippy-operators/src/expression.rs`
- Test: `crates/zippy-operators/tests/reactive_factors.rs`
- Test: `crates/zippy-engines/tests/reactive_engine.rs`

- [ ] **Step 1: Import context**

Change:

```rust
use crate::reactive::{ReactiveFactor, StatefulFloatById, StatefulFloatKind};
```

to:

```rust
use crate::reactive::{
    ReactiveFactor, ReactiveFactorContext, StatefulFloatById, StatefulFloatKind,
};
```

- [ ] **Step 2: Add context value extractor**

Add:

```rust
fn extract_context_value(
    ctx: &ReactiveFactorContext<'_>,
    name: &str,
    row: usize,
) -> Result<EvalValue> {
    let array = ctx.column_by_name(name)?;
    extract_array_value(array, name, row)
}
```

Refactor `extract_batch_value()` to call a shared:

```rust
fn extract_array_value(array: &ArrayRef, name: &str, row: usize) -> Result<EvalValue> { ... }
```

- [ ] **Step 3: Add context-native ExpressionFactor**

Add:

```rust
fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
    let mut values = Vec::with_capacity(ctx.num_rows());
    for row in 0..ctx.num_rows() {
        values.push(evaluate_expr_context(&self.ast, ctx, row)?);
    }
    build_output_array(self.output_field.data_type(), values)
}
```

Add `evaluate_expr_context()` mirroring `evaluate_expr()` but using `extract_context_value()`.

- [ ] **Step 4: Add context-native PlannedExpressionFactor**

Add `evaluate_with_context()` mirroring the current planned evaluation, replacing:

- `batch.schema().index_of(...)` with `ctx.index_of(...)`
- `batch.column(...)` with `ctx.column(...)`
- `extract_batch_value(...)` with `extract_context_value(...)`
- `batch.num_rows()` with `ctx.num_rows()`

- [ ] **Step 5: Run expression/reactive tests**

Run:

```bash
cargo test -p zippy-operators --test reactive_factors
cargo test -p zippy-engines --test reactive_engine
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/zippy-operators/src/expression.rs
git commit -m "refactor: evaluate expression factors from context"
```

## Task 6: Add Multi-Factor Reactive Benchmark

**Files:**
- Modify: `crates/zippy-engines/benches/reactive_pipeline.rs`

- [ ] **Step 1: Expand benchmark imports**

Add specs:

```rust
use zippy_operators::{CastSpec, ExpressionSpec, TsEmaSpec, TsMeanSpec, TsStdSpec};
```

- [ ] **Step 2: Add multi-factor benchmark**

Add a second bench function:

```rust
fn bench_reactive_pipeline_multi_factor(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                (0..1024)
                    .map(|index| format!("S{:03}", index % 32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..1024)
                    .map(|index| 10.0 + (index % 17) as f64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    c.bench_function("reactive_pipeline_1024_rows_8_factors", |b| {
        b.iter(|| {
            let mut engine = ReactiveStateEngine::new(
                "bench",
                schema.clone(),
                vec![
                    TsEmaSpec::new("symbol", "price", 8, "ema_8").build().unwrap(),
                    TsEmaSpec::new("symbol", "price", 16, "ema_16").build().unwrap(),
                    TsMeanSpec::new("symbol", "price", 8, "mean_8").build().unwrap(),
                    TsStdSpec::new("symbol", "price", 8, "std_8").build().unwrap(),
                    CastSpec::new("symbol", "price", "int64", "price_i64").build().unwrap(),
                    ExpressionSpec::new("price + 1", "price_plus_one")
                        .build(schema.as_ref())
                        .unwrap(),
                    ExpressionSpec::new("abs(price)", "price_abs")
                        .build(schema.as_ref())
                        .unwrap(),
                    ExpressionSpec::new("clip(price, 10, 20)", "price_clip")
                        .build(schema.as_ref())
                        .unwrap(),
                ],
            )
            .unwrap();
            let _ = engine
                .on_data(SegmentTableView::from_record_batch(batch.clone()))
                .unwrap();
        })
    });
}
```

Update `criterion_group!` to include both bench functions.

- [ ] **Step 3: Run compile-only benchmark check**

Run:

```bash
cargo bench -p zippy-engines --bench reactive_pipeline --no-run
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/zippy-engines/benches/reactive_pipeline.rs
git commit -m "bench: add reactive multi factor profile"
```

## Task 7: Verification And Audit Status

**Files:**
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Run final verification**

Run:

```bash
cargo test -p zippy-operators --test reactive_factors
cargo test -p zippy-engines --test reactive_engine
cargo test -p zippy-engines --lib
cargo clippy -p zippy-operators --all-targets -- -D warnings
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

Expected: all PASS.

- [ ] **Step 2: Update P007 status**

In `docs/performance_audit_fix_status_2026-05-12.md`, update P007:

```text
`ReactiveStateEngine` now evaluates factors through `ReactiveFactorContext`, so built-in
context-native factors no longer require per-factor intermediate `RecordBatch` construction.
The remaining P007 work is id interning / borrowed key state for per-id HashMap updates.
```

- [ ] **Step 3: Final diff check**

Run:

```bash
git status --short
git diff --check
```

Expected: only audit doc is modified and diff check is clean.

- [ ] **Step 4: Commit**

```bash
git add docs/performance_audit_fix_status_2026-05-12.md
git commit -m "docs: update reactive factor context status"
```

## Self-Review

- Spec coverage: context type, trait fallback, engine path, built-in factors, expression factors, benchmark, and audit status are covered.
- Public API: existing constructors stay unchanged; old `evaluate(&RecordBatch)` remains.
- Rollback: transaction methods remain on `ReactiveFactor` and are not changed.
- Red-path proof: `ContextOnlyFactor` fails if engine uses old `evaluate()` path.
- Remaining scope: id interning and state key allocation are explicitly deferred.
