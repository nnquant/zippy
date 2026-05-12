# CrossSectional Factor Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `CrossSectionalFactorContext` so built-in cross-sectional factors share value extraction, stats, and rank caches within one finalized bucket.

**Architecture:** Keep `CrossSectionalFactor::evaluate(batch)` as the compatibility path and add a default `evaluate_with_context(ctx)` method. `CrossSectionalEngine` creates one context per finalized bucket and invokes factors through the context path. Built-in `CS_RANK`, `CS_ZSCORE`, and `CS_DEMEAN` override the context path and reuse cached values, stats, and ranks.

**Tech Stack:** Rust, Arrow `RecordBatch` / `ArrayRef`, `BTreeMap` deterministic caches, existing `zippy_core::Result` / `ZippyError`, Cargo tests and clippy.

---

## File Structure

- Modify `crates/zippy-operators/src/cross_sectional.rs`
  - Add `CrossSectionalFactorContext`.
  - Add `Float64Stats`.
  - Add `evaluate_with_context()` default method to `CrossSectionalFactor`.
  - Move existing value extraction, stats, and rank logic behind context cache methods.
  - Override `evaluate_with_context()` in built-in cross-sectional factors.
- Modify `crates/zippy-operators/src/lib.rs`
  - Re-export `CrossSectionalFactorContext` and `Float64Stats`.
- Modify `crates/zippy-operators/tests/cross_sectional_factors.rs`
  - Add tests for context value/stat/rank semantics.
  - Add tests that context evaluation matches old `evaluate()`.
- Modify `crates/zippy-engines/src/cross_sectional.rs`
  - In `finalize_bucket()`, create one context and call `factor.evaluate_with_context(&mut context)`.
- Modify `crates/zippy-engines/tests/cross_sectional_engine.rs`
  - Add multi-factor same-value-field integration coverage.
- Modify `docs/performance_audit_fix_status_2026-05-12.md`
  - Mark P005 shared factor context as implemented, leaving only future broader extensions.

---

### Task 1: Add Failing Operator Context Tests

**Files:**
- Modify: `crates/zippy-operators/tests/cross_sectional_factors.rs`

- [ ] **Step 1: Import context types**

Add `CrossSectionalFactorContext` to the existing `use zippy_operators::{...};` list:

```rust
use zippy_operators::{
    CSDemeanSpec, CSRankSpec, CSZscoreSpec, CrossSectionalFactorContext,
};
```

- [ ] **Step 2: Add context value/stat/rank tests**

Append these tests to `crates/zippy-operators/tests/cross_sectional_factors.rs`:

```rust
#[test]
fn cross_sectional_context_caches_float_values_and_stats() {
    let input = nullable_batch(vec![Some(1.0), None, Some(3.0)]);
    let mut context = CrossSectionalFactorContext::new(&input);

    assert_float_options_eq(
        context.float64_values("ret_1m").unwrap(),
        &[Some(1.0), None, Some(3.0)],
    );
    assert_float_options_eq(
        context.float64_values("ret_1m").unwrap(),
        &[Some(1.0), None, Some(3.0)],
    );

    let stats = context.float64_stats("ret_1m").unwrap();

    assert_eq!(stats.sample_count, 2);
    assert!((stats.mean - 2.0).abs() < 1e-12);
    assert!((stats.variance - 1.0).abs() < 1e-12);
    assert!((stats.std - 1.0).abs() < 1e-12);
}

#[test]
fn cross_sectional_context_ranks_match_existing_tie_policy() {
    let input = batch(vec![1.0, 2.0, 2.0]);
    let mut context = CrossSectionalFactorContext::new(&input);

    assert_float_options_eq(
        context.float64_ranks("ret_1m").unwrap(),
        &[Some(1.0), Some(2.5), Some(2.5)],
    );
    assert_float_options_eq(
        context.float64_ranks("ret_1m").unwrap(),
        &[Some(1.0), Some(2.5), Some(2.5)],
    );
}

#[test]
fn cross_sectional_context_evaluation_matches_legacy_evaluate() {
    let input = nullable_batch(vec![Some(1.0), None, Some(3.0)]);
    let mut context = CrossSectionalFactorContext::new(&input);
    let mut rank = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();
    let mut zscore = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();
    let mut demean = CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap();

    let rank_context = rank.evaluate_with_context(&mut context).unwrap();
    let zscore_context = zscore.evaluate_with_context(&mut context).unwrap();
    let demean_context = demean.evaluate_with_context(&mut context).unwrap();

    let mut legacy_rank = CSRankSpec::new("ret_1m", "ret_rank").build().unwrap();
    let mut legacy_zscore = CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap();
    let mut legacy_demean = CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap();

    assert_float_options_eq(
        &float_values(&rank_context),
        &float_values(&legacy_rank.evaluate(&input).unwrap()),
    );
    assert_float_options_eq(
        &float_values(&zscore_context),
        &float_values(&legacy_zscore.evaluate(&input).unwrap()),
    );
    assert_float_options_eq(
        &float_values(&demean_context),
        &float_values(&legacy_demean.evaluate(&input).unwrap()),
    );
}
```

- [ ] **Step 3: Run the failing test command**

Run:

```bash
cargo test -p zippy-operators --test cross_sectional_factors -- --nocapture
```

Expected: FAIL because `CrossSectionalFactorContext` and `evaluate_with_context()` are not defined or not exported yet.

---

### Task 2: Implement `CrossSectionalFactorContext`

**Files:**
- Modify: `crates/zippy-operators/src/cross_sectional.rs`
- Modify: `crates/zippy-operators/src/lib.rs`

- [ ] **Step 1: Add imports and public context structs**

In `crates/zippy-operators/src/cross_sectional.rs`, update imports and add these types near the top:

```rust
use std::collections::BTreeMap;
```

```rust
/// Cached cross-sectional factor inputs for one materialized bucket.
pub struct CrossSectionalFactorContext<'a> {
    batch: &'a RecordBatch,
    float64_values_by_field: BTreeMap<String, Vec<Option<f64>>>,
    float64_stats_by_field: BTreeMap<String, Float64Stats>,
    rank_by_field: BTreeMap<String, Vec<Option<f64>>>,
}

/// Summary statistics for a finite float64 cross-section.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Float64Stats {
    pub sample_count: usize,
    pub mean: f64,
    pub variance: f64,
    pub std: f64,
}
```

- [ ] **Step 2: Add context methods**

Add this impl block below the struct definitions:

```rust
impl<'a> CrossSectionalFactorContext<'a> {
    /// Create a factor context for a single materialized bucket.
    pub fn new(batch: &'a RecordBatch) -> Self {
        Self {
            batch,
            float64_values_by_field: BTreeMap::new(),
            float64_stats_by_field: BTreeMap::new(),
            rank_by_field: BTreeMap::new(),
        }
    }

    /// Return the underlying bucket batch for compatibility fallback paths.
    pub fn batch(&self) -> &RecordBatch {
        self.batch
    }

    /// Return cached finite float64 values for a field.
    pub fn float64_values(&mut self, field: &str) -> Result<&[Option<f64>]> {
        if !self.float64_values_by_field.contains_key(field) {
            let values = float64_values(self.batch, field)?;
            self.float64_values_by_field.insert(field.to_string(), values);
        }

        Ok(self
            .float64_values_by_field
            .get(field)
            .expect("cross-sectional values cache must contain requested field"))
    }

    /// Return cached population statistics for a finite float64 field.
    pub fn float64_stats(&mut self, field: &str) -> Result<Float64Stats> {
        if !self.float64_stats_by_field.contains_key(field) {
            let values = self.float64_values(field)?;
            let samples = values.iter().flatten().copied().collect::<Vec<_>>();
            let stats = compute_float64_stats(&samples);
            self.float64_stats_by_field
                .insert(field.to_string(), stats);
        }

        Ok(*self
            .float64_stats_by_field
            .get(field)
            .expect("cross-sectional stats cache must contain requested field"))
    }

    /// Return cached average-rank values for a finite float64 field.
    pub fn float64_ranks(&mut self, field: &str) -> Result<&[Option<f64>]> {
        if !self.rank_by_field.contains_key(field) {
            let values = self.float64_values(field)?;
            let ranks = compute_float64_ranks(values);
            self.rank_by_field.insert(field.to_string(), ranks);
        }

        Ok(self
            .rank_by_field
            .get(field)
            .expect("cross-sectional rank cache must contain requested field"))
    }
}
```

- [ ] **Step 3: Add helper functions**

Add these helpers near existing `float64_values()`:

```rust
fn compute_float64_stats(samples: &[f64]) -> Float64Stats {
    if samples.is_empty() {
        return Float64Stats {
            sample_count: 0,
            mean: 0.0,
            variance: 0.0,
            std: 0.0,
        };
    }

    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;

    Float64Stats {
        sample_count: samples.len(),
        mean,
        variance,
        std: variance.sqrt(),
    }
}

fn compute_float64_ranks(values: &[Option<f64>]) -> Vec<Option<f64>> {
    let mut samples = values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.map(|sample| (index, sample)))
        .collect::<Vec<_>>();

    samples.sort_by(|left, right| total_cmp(left.1, right.1));

    let mut ranked = vec![None; values.len()];
    let mut start = 0;
    while start < samples.len() {
        let mut end = start + 1;
        while end < samples.len() && total_cmp(samples[start].1, samples[end].1) == Ordering::Equal {
            end += 1;
        }

        let average_rank = (start + 1 + end) as f64 / 2.0;
        for (row_index, _) in &samples[start..end] {
            ranked[*row_index] = Some(average_rank);
        }
        start = end;
    }

    ranked
}
```

- [ ] **Step 4: Export context types**

In `crates/zippy-operators/src/lib.rs`, change the cross-sectional export to:

```rust
pub use cross_sectional::{
    CSDemeanSpec, CSRankSpec, CSZscoreSpec, CrossSectionalFactor, CrossSectionalFactorContext,
    Float64Stats,
};
```

- [ ] **Step 5: Run operator tests**

Run:

```bash
cargo test -p zippy-operators --test cross_sectional_factors -- --nocapture
```

Expected: compile gets further, but built-in factors still need `evaluate_with_context()` trait support.

---

### Task 3: Add Trait Default and Context-Native Built-In Factors

**Files:**
- Modify: `crates/zippy-operators/src/cross_sectional.rs`

- [ ] **Step 1: Add trait default method**

Change `CrossSectionalFactor` to:

```rust
pub trait CrossSectionalFactor: Send {
    /// Return the output field definition for this factor.
    fn output_field(&self) -> Field;

    /// Evaluate the factor for each row in the batch.
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the factor with per-bucket shared context.
    fn evaluate_with_context(
        &mut self,
        context: &mut CrossSectionalFactorContext<'_>,
    ) -> Result<ArrayRef> {
        self.evaluate(context.batch())
    }
}
```

- [ ] **Step 2: Update `CSRankFactor`**

Keep `evaluate()` as the old compatibility implementation, but change it to use the helper:

```rust
fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
    let values = float64_values(batch, &self.value_field)?;
    Ok(float64_array_from_options(&compute_float64_ranks(&values)))
}

fn evaluate_with_context(
    &mut self,
    context: &mut CrossSectionalFactorContext<'_>,
) -> Result<ArrayRef> {
    let ranks = context.float64_ranks(&self.value_field)?;
    Ok(float64_array_from_options(ranks))
}
```

- [ ] **Step 3: Update `CSZscoreFactor`**

Use cached values and stats in the context path:

```rust
fn evaluate_with_context(
    &mut self,
    context: &mut CrossSectionalFactorContext<'_>,
) -> Result<ArrayRef> {
    let values = context.float64_values(&self.value_field)?.to_vec();
    let stats = context.float64_stats(&self.value_field)?;
    Ok(zscore_array(&values, stats))
}
```

Refactor existing zscore logic into:

```rust
fn zscore_array(values: &[Option<f64>], stats: Float64Stats) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());

    if stats.sample_count == 0 {
        append_optional_values(&mut builder, &vec![None; values.len()]);
        return Arc::new(builder.finish());
    }

    for value in values {
        match value {
            Some(_) if stats.std == 0.0 => builder.append_value(0.0),
            Some(value) => builder.append_value((value - stats.mean) / stats.std),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}
```

- [ ] **Step 4: Update `CSDemeanFactor`**

Use cached values and stats in the context path:

```rust
fn evaluate_with_context(
    &mut self,
    context: &mut CrossSectionalFactorContext<'_>,
) -> Result<ArrayRef> {
    let values = context.float64_values(&self.value_field)?.to_vec();
    let stats = context.float64_stats(&self.value_field)?;
    Ok(demean_array(&values, stats))
}
```

Refactor existing demean logic into:

```rust
fn demean_array(values: &[Option<f64>], stats: Float64Stats) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());

    if stats.sample_count == 0 {
        append_optional_values(&mut builder, &vec![None; values.len()]);
        return Arc::new(builder.finish());
    }

    for value in values {
        match value {
            Some(value) => builder.append_value(value - stats.mean),
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}
```

- [ ] **Step 5: Add shared array helper**

Add:

```rust
fn float64_array_from_options(values: &[Option<f64>]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    append_optional_values(&mut builder, values);
    Arc::new(builder.finish())
}
```

- [ ] **Step 6: Run operator tests**

Run:

```bash
cargo test -p zippy-operators --test cross_sectional_factors -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit operator changes**

Run:

```bash
git add crates/zippy-operators/src/cross_sectional.rs crates/zippy-operators/src/lib.rs crates/zippy-operators/tests/cross_sectional_factors.rs
git commit -m "feat: add cross sectional factor context"
```

---

### Task 4: Route `CrossSectionalEngine` Through Context

**Files:**
- Modify: `crates/zippy-engines/src/cross_sectional.rs`
- Modify: `crates/zippy-engines/tests/cross_sectional_engine.rs`

- [ ] **Step 1: Add engine test for multiple factors on same field**

Append a test to `crates/zippy-engines/tests/cross_sectional_engine.rs`:

```rust
#[test]
fn cross_sectional_engine_evaluates_multiple_same_field_factors_through_context() {
    let mut engine = CrossSectionalEngine::new(
        "cs",
        input_schema(),
        "symbol",
        "dt",
        MINUTE_NS,
        LateDataPolicy::Reject,
        vec![
            CSRankSpec::new("ret_1m", "rank_a").build().unwrap(),
            CSRankSpec::new("ret_1m", "rank_b").build().unwrap(),
            CSZscoreSpec::new("ret_1m", "ret_z").build().unwrap(),
            CSDemeanSpec::new("ret_1m", "ret_dm").build().unwrap(),
        ],
    )
    .unwrap();

    engine
        .on_data(batch(
            vec!["C", "A", "B"],
            vec![3_000_000_000, 1_000_000_000, 2_000_000_000],
            vec![30.0, 10.0, 20.0],
        ))
        .unwrap();

    let flushed = engine.on_flush().unwrap();

    assert_eq!(
        column_names(&flushed[0]),
        vec!["symbol", "dt", "rank_a", "rank_b", "ret_z", "ret_dm"]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    );
    assert_float_slices_eq(&float_values(flushed[0].column_at(2)), &[1.0, 2.0, 3.0]);
    assert_float_slices_eq(&float_values(flushed[0].column_at(3)), &[1.0, 2.0, 3.0]);
    assert_float_slices_eq(
        &float_values(flushed[0].column_at(4)),
        &[-1.224744871391589, 0.0, 1.224744871391589],
    );
    assert_float_slices_eq(&float_values(flushed[0].column_at(5)), &[-10.0, 0.0, 10.0]);
}
```

- [ ] **Step 2: Run engine test before implementation**

Run:

```bash
cargo test -p zippy-engines --test cross_sectional_engine cross_sectional_engine_evaluates_multiple_same_field_factors_through_context -- --nocapture
```

Expected: PASS may already occur through fallback behavior. This test locks the public behavior before changing engine dispatch.

- [ ] **Step 3: Update engine imports**

In `crates/zippy-engines/src/cross_sectional.rs`, change:

```rust
use zippy_operators::CrossSectionalFactor;
```

to:

```rust
use zippy_operators::{CrossSectionalFactor, CrossSectionalFactorContext};
```

- [ ] **Step 4: Update `finalize_bucket()` dispatch**

Change the factor loop from:

```rust
for factor in &mut self.factors {
    columns.push(factor.evaluate(&bucket_batch)?);
}
```

to:

```rust
let mut context = CrossSectionalFactorContext::new(&bucket_batch);
for factor in &mut self.factors {
    columns.push(factor.evaluate_with_context(&mut context)?);
}
```

- [ ] **Step 5: Run engine tests**

Run:

```bash
cargo test -p zippy-engines --test cross_sectional_engine -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit engine routing**

Run:

```bash
git add crates/zippy-engines/src/cross_sectional.rs crates/zippy-engines/tests/cross_sectional_engine.rs
git commit -m "refactor: route cross sectional engine through factor context"
```

---

### Task 5: Update Status Docs and Run Final Verification

**Files:**
- Modify: `docs/performance_audit_fix_status_2026-05-12.md`

- [ ] **Step 1: Update P005 status**

In the status table, change the P005 row to state that row order fast path, columnar bucket state, and factor context are implemented. In the P005 section, add bullets:

```markdown
- 新增 `CrossSectionalFactorContext`，同一个 finalized bucket 内共享 float64 value extraction、stats 和 rank cache。
- `CrossSectionalEngine` finalize 阶段为每个 bucket 创建一个 context，并通过 `evaluate_with_context()` 调用 factor。
- 内置 `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN` 已迁移到 context-native path；旧 `evaluate(batch)` 仍保留兼容。
```

Keep this remaining boundary:

```markdown
- context cache 只在单 bucket 内有效；跨 bucket cache、group_by/industry neutralization 和 id interning 仍属后续专项。
```

- [ ] **Step 2: Run final verification**

Run:

```bash
cargo test -p zippy-operators --test cross_sectional_factors -- --nocapture
cargo test -p zippy-engines --test cross_sectional_engine -- --nocapture
cargo test -p zippy-engines --lib -- --nocapture
cargo clippy -p zippy-operators --all-targets -- -D warnings
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

Expected: every command exits with code 0.

- [ ] **Step 3: Commit docs and any final formatting**

Run:

```bash
git add docs/performance_audit_fix_status_2026-05-12.md
git commit -m "docs: update cross sectional factor context status"
```

- [ ] **Step 4: Confirm clean worktree**

Run:

```bash
git status --short
```

Expected: no output.
