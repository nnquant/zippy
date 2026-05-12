# Reactive Factor Context Design

日期：2026-05-12

## 背景

`docs/performance_audit_findings_2026-05-12.md` 的 P007 指出：

- `ReactiveStateEngine` 每个 factor 都会用 `columns.clone()` 构造一次中间
  `RecordBatch`，并重建 schema。
- stateful factor 每行仍会把 id 转成 `String` 存入 per-factor `HashMap`。
- rolling mean/std 的 O(window) 问题已经在上一轮通过 `WindowHistory` running
  sum/sumsq 修复。

本设计只处理 P007 的 engine/factor evaluation context 部分：减少多 factor 链路中重复构造
中间 `RecordBatch` 和 schema 的开销。id interning / borrowed key state 属于更大的 shared id
registry 设计，留给后续 P004/P005/P007 联合处理。

## 目标

- 新增轻量只读 `ReactiveFactorContext`，表示当前已可见的 schema、columns 和 row count。
- `ReactiveStateEngine` 在 factor 链路中维护 `Vec<ArrayRef>` 和当前 schema，不再为每个
  context-native factor 构造中间 `RecordBatch`。
- `ReactiveFactor` trait 增加默认 `evaluate_with_context(ctx)`，保留旧
  `evaluate(&RecordBatch)` 兼容路径。
- 内置 reactive factors 改为 context-native：
  - `TsEmaFactor`
  - `WindowHistoryFactor`
  - `UnaryFloatFactor`
  - `CastFactor`
  - `ExpressionFactor`
  - `PlannedExpressionFactor`
- 不改变 public Rust/Python engine 构造器和用户可见输出语义。
- 保持 rollback transaction 语义不变。

## 非目标

- 不在本专项中引入 id interning 或 shared `IdRegistry`。
- 不改变 `StatefulFloatById` 的 `HashMap<String, ...>` 存储方式。
- 不重写 expression planner 的 DAG 结构。
- 不改变 `CrossSectionalFactor` trait。
- 不改变 `TimeSeriesEngine` 的 pre/post factor 语义。
- 不要求第三方 factor 立即实现新方法；默认 fallback 必须继续工作。

## 设计选项

### 方案 1：兼容式 `ReactiveFactorContext`（采用）

做法：

- 在 `zippy-operators::reactive` 中新增：

```text
ReactiveFactorContext<'a> {
    schema: &'a SchemaRef,
    columns: &'a [ArrayRef],
    row_count: usize,
}
```

- 提供方法：

```text
schema() -> &Schema
num_rows() -> usize
column(index) -> &ArrayRef
column_by_name(name) -> Result<&ArrayRef>
index_of(name) -> Result<usize>
record_batch() -> Result<RecordBatch>
```

- `ReactiveFactor` 增加默认方法：

```text
fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
    let batch = ctx.record_batch()?;
    self.evaluate(&batch)
}
```

- 现有 `evaluate(&RecordBatch)` 保留，避免破坏未迁移 factor。
- engine 先调用 `evaluate_with_context()`，内置 factor 覆写该方法后不再触发 fallback batch 构造。

优点：

- 兼容性最好，第三方 factor 不需要同轮迁移。
- 可以先让核心内置 factor 获益。
- 失败时仍能快速回退到旧 `RecordBatch` 语义。

缺点：

- trait 同时保留两个 evaluate 方法，会有一段过渡期。
- 没有覆写 context 方法的 factor 仍会构造中间 `RecordBatch`。

### 方案 2：直接替换 trait 签名

做法：

- 删除或弱化 `evaluate(&RecordBatch)`。
- 所有 factor 只接受 `ReactiveFactorContext`。

优点：

- 接口最清爽，没有 fallback。

缺点：

- 破坏性更强。
- 所有 factor 和测试必须同轮改完。
- 不利于外部自定义 factor 兼容。

### 方案 3：仅缓存 schema，继续构造中间 batch

做法：

- engine 缓存逐步扩展的 schema/fields。
- 每个 factor 仍构造 `RecordBatch`。

优点：

- 改动最小。

缺点：

- 只能减少一部分 schema rebuild，不能解决 P007 的核心 `RecordBatch` 构造开销。

## 采用方案

采用方案 1。

第一版以兼容为优先：引入 `ReactiveFactorContext`，让内置 factor 走 context-native 读取列；
旧 `evaluate(&RecordBatch)` 保留为 fallback。这样可以在不改变用户 API 的前提下消除核心
factor 链路的大量中间 `RecordBatch` 构造。

## 核心设计

### 1. ReactiveFactorContext

`ReactiveFactorContext` 放在 `crates/zippy-operators/src/reactive.rs`，因为它是
`ReactiveFactor` trait 的输入类型，属于 operator API。

结构：

```text
pub struct ReactiveFactorContext<'a> {
    schema: &'a SchemaRef,
    columns: &'a [ArrayRef],
    row_count: usize,
}
```

构造时校验：

- `columns.len() == schema.fields().len()`
- 所有 column 的 `len()` 必须等于 `row_count`
- 若 `row_count` 无法从 columns 推导，空 columns 时使用 0

主要方法：

- `num_rows()`
- `schema()`
- `column(index)`
- `column_by_name(field)`
- `index_of(field)`
- `record_batch()`

`record_batch()` 只用于 fallback，不是内置 factor 的热路径。

### 2. Trait 兼容策略

`ReactiveFactor` 变为：

```text
pub trait ReactiveFactor: Send {
    fn output_field(&self) -> Field;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
    fn evaluate_with_context(&mut self, ctx: &ReactiveFactorContext<'_>) -> Result<ArrayRef> {
        let batch = ctx.record_batch()?;
        self.evaluate(&batch)
    }
    ...
}
```

这保持当前所有实现可编译。后续可逐步把内置 factor 的 `evaluate()` 改成简单 fallback：

```text
fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef> {
    let ctx = ReactiveFactorContext::from_batch(batch)?;
    self.evaluate_with_context(&ctx)
}
```

第一版可以直接在每个内置 factor 中保留旧 `evaluate()`，同时新增
`evaluate_with_context()`，两者复用私有 helper，避免重复逻辑。

### 3. Engine 执行流

`ReactiveStateEngine::evaluate_table()` 当前流程：

1. project/filter input columns。
2. 对每个 factor 构造 `RecordBatch(current_schema, columns.clone())`。
3. factor evaluate。
4. push output column。
5. 重建 schema。

新流程：

1. project/filter input columns。
2. 维护 `current_schema` 和 `columns`。
3. 对每个 factor 构造轻量 `ReactiveFactorContext { &current_schema, &columns, row_count }`。
4. 调 `factor.evaluate_with_context(&ctx)`。
5. push output column。
6. 只在追加字段时更新 schema。
7. 最后构造一次最终 output `RecordBatch`。

这意味着 context-native factor 链路中，只有最终输出 batch 一次构造。

### 4. 内置 Factor 迁移

`crates/zippy-operators/src/reactive.rs`：

- `extract_columns()` 改为接受 `ReactiveFactorContext` 或新增
  `extract_context_columns()`。
- `TsEmaFactor`、`WindowHistoryFactor`、`UnaryFloatFactor`、`CastFactor` 覆写
  `evaluate_with_context()`。
- 旧 `evaluate(&RecordBatch)` 通过 `ReactiveFactorContext::from_batch()` 复用 context 实现。

`crates/zippy-operators/src/expression.rs`：

- `ExpressionFactor` 覆写 `evaluate_with_context()`。
- `PlannedExpressionFactor` 覆写 `evaluate_with_context()`。
- `extract_batch_value()` 新增 context 版本，例如 `extract_context_value(ctx, name, row)`。
- 旧 batch helper 保留或改成从 batch 构造 context 后复用。

### 5. 错误处理

错误消息保持现有风格：

- 缺字段：`missing ... field field=[...]`
- 类型不匹配：`... field must be ... field=[...]`
- null id/value：保持现有 `contains nulls` 文案。
- context 构造失败：返回 `SchemaMismatch`，说明 columns/schema/row_count 不一致。

`ReactiveStateEngine` 对 factor error 的 transaction 处理不变：

- `Rollback` 模式下，任意 factor 返回 error 后调用 `rollback_transaction()`。
- `FailFast` 模式保持原行为。

### 6. 测试策略

新增/扩展测试重点：

- `ReactiveFactorContext::from_batch()` 可正确按字段名取列。
- context 构造拒绝 schema/columns 数量不一致。
- context 构造拒绝不同长度 columns。
- `ReactiveStateEngine` 多 factor 输出列顺序和值不变。
- `ReactiveStateEngine` 在多 factor 链路中不会为 context-native factor 构造中间
  `RecordBatch`。

最后一条需要一个测试专用 factor：

- `ContextOnlyFactor` 覆写 `evaluate_with_context()` 并在 `evaluate()` 中 panic 或返回错误。
- 把它放入 `ReactiveStateEngine`，如果 engine 仍走旧 `evaluate()`，测试失败。

这比用 benchmark 证明更直接，可以锁住执行路径。

### 7. 性能验证

现有 `reactive_pipeline_1024_rows` 只有 1 个 factor，不能代表 P007 的多 factor 链路。
本专项应扩展 bench：

- 1024 rows
- 32 symbols
- 8-16 个 reactive factors
- 至少包含 `TsEma`、`TsMean`、`TsStd`、`Cast`、`ExpressionSpec`

benchmark 只作为观察项，不作为 CI pass/fail 阈值。

## 文件边界

预期改动：

- `crates/zippy-operators/src/reactive.rs`
  - 新增 `ReactiveFactorContext`。
  - 扩展 `ReactiveFactor` trait。
  - 迁移内置 reactive factor 到 context-native。
- `crates/zippy-operators/src/expression.rs`
  - 迁移 expression factors 到 context-native。
- `crates/zippy-engines/src/reactive.rs`
  - engine factor loop 改为创建 context 并调用 `evaluate_with_context()`。
- `crates/zippy-engines/tests/reactive_engine.rs`
  - 增加 context-only factor 测试。
- `crates/zippy-operators/tests/reactive_factors.rs`
  - 增加 context 行为单元测试，或补在 module tests 中。
- `crates/zippy-engines/benches/reactive_pipeline.rs`
  - 增加多 factor profile。
- `docs/performance_audit_fix_status_2026-05-12.md`
  - 更新 P007 状态。

## 后续扩展

- shared id registry / id interning，减少 stateful factor 每行 `id.to_string()`。
- 将 context 扩展为 typed column cache，避免同一 factor 内重复 downcast。
- 将 TimeSeriesEngine pre/post factor 路径也切到 context。
- 如果外部 factor 生态稳定后，下一阶段可以考虑废弃 `evaluate(&RecordBatch)` fallback。
