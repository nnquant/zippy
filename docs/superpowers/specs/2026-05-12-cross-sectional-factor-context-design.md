# CrossSectional Factor Context Design

日期：2026-05-12

## 背景

`CrossSectionalEngine` 已经通过 `CrossSectionalBucketState` 消除了 current bucket 的
per-row `OwnedRow` 和 finalize 阶段的 `concat_batches()`。当前剩余的 P005 热点在 factor
evaluation：

- `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN` 都各自调用 `float64_values(batch, value_field)`。
- 多个 factor 指向同一个 value column 时，会重复 downcast、遍历、过滤 null / non-finite。
- `CS_ZSCORE` 和 `CS_DEMEAN` 会分别统计 mean；`CS_ZSCORE` 还会额外统计 variance。
- rank 结果如果多个 factor 共用同一 value column，也会重复排序。

本设计只优化 CrossSectional factor evaluation 的共享输入与统计缓存，不改变
`CrossSectionalEngine` 的 bucket 状态机、输出 schema、排序语义或 public spec API。

## 目标

- 新增 `CrossSectionalFactorContext`，为一个 materialized bucket 提供共享列访问和统计缓存。
- `CrossSectionalFactor` 增加默认 context evaluation 方法，保留现有 `evaluate(batch)` 兼容路径。
- 内置 `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN` 走 context-native evaluation。
- 同一个 value column 在同一个 bucket 内只提取一次 `Vec<Option<f64>>`。
- `CS_ZSCORE` 和 `CS_DEMEAN` 共享同一套 mean / variance / std 统计。
- 同一个 value column 的 rank 结果只排序一次并可被多个 rank factor 复用。
- 保持现有 null、non-finite、tie rank、zero std、empty sample 行为完全不变。

## 非目标

- 不改变 `CrossSectionalEngine` 的 bucket lifecycle。
- 不改变 `CrossSectionalBucketState`。
- 不引入 universe 补全、forward fill、group_by、行业中性化。
- 不改变 output field 命名、顺序或 dtype。
- 不改变 Python/Rust factor spec 构造 API。
- 不要求第三方自定义 factor 立即迁移到 context 路径。
- 不做跨 bucket cache；context 只在单个 finalized bucket 内有效。
- 不做 id interning 或 undo log。

## 设计选项

### 方案 A：trait 增加默认 context 路径（采用）

做法：

- 新增 `CrossSectionalFactorContext`。
- `CrossSectionalFactor` 增加默认方法：

```text
fn evaluate_with_context(&mut self, ctx: &mut CrossSectionalFactorContext) -> Result<ArrayRef> {
    self.evaluate(ctx.batch())
}
```

- `CrossSectionalEngine` 在 finalize 时创建一个 context，然后对每个 factor 调用
  `evaluate_with_context(&mut context)`。
- 内置 factor override `evaluate_with_context()`，共享 context 中的 values / stats / ranks。

优点：

- 与 P007 `ReactiveFactorContext` 方向一致。
- 不破坏已有自定义 `CrossSectionalFactor`；未迁移 factor 自动 fallback 到旧路径。
- engine 不需要知道具体 factor 类型。
- 后续可以逐步扩展更多 context-native factor。

缺点：

- trait surface 增加一个默认方法。
- 旧 `evaluate(batch)` 暂时仍保留，内置 factor 里会有一段 fallback 兼容代码。

### 方案 B：engine 特判内置 factor

做法：

- engine downcast 或枚举内置 factor 类型。
- 对内置 `rank/zscore/demean` 直接做批量优化。

优点：

- 可以针对当前三个 factor 写最短路径。

缺点：

- engine 需要知道 operator 内部类型，模块边界变差。
- 自定义 factor 没有迁移路径。
- 后续新增 factor 会继续把特殊逻辑堆到 engine 里。

### 方案 C：直接把 trait 改成只接受 context

做法：

- 删除或弱化 `evaluate(batch)`，所有 factor 必须实现 context evaluation。

优点：

- 长期接口最统一。

缺点：

- 破坏现有自定义 factor 实现。
- 一次性迁移范围偏大，和本轮“低风险消除重复扫描”的目标不匹配。

## 采用方案

采用方案 A。

第一版保持 `evaluate(batch)` 为兼容主路径，同时引入默认 `evaluate_with_context()`。engine 只依赖
trait，不 downcast 内置 factor。内置 factor 使用 context-native 实现以消除重复提取和重复统计。

## 核心设计

### 1. `CrossSectionalFactorContext`

模块位置：

```text
crates/zippy-operators/src/cross_sectional.rs
```

核心字段：

```text
CrossSectionalFactorContext<'a> {
    batch: &'a RecordBatch,
    float64_values_by_field: BTreeMap<String, Vec<Option<f64>>>,
    float64_stats_by_field: BTreeMap<String, Float64Stats>,
    rank_by_field: BTreeMap<String, Vec<Option<f64>>>,
}
```

`BTreeMap` 用于 deterministic cache behavior；字段名数量通常很少，查找成本不是主要瓶颈。

公开给 factor 的方法：

```text
batch(&self) -> &RecordBatch
float64_values(&mut self, field: &str) -> Result<&[Option<f64>]>
float64_stats(&mut self, field: &str) -> Result<Float64Stats>
float64_ranks(&mut self, field: &str) -> Result<&[Option<f64>]>
```

`Float64Stats`：

```text
Float64Stats {
    sample_count: usize,
    mean: f64,
    variance: f64,
    std: f64,
}
```

如果 sample 为空，`sample_count = 0`，`mean/variance/std` 使用 `0.0`。调用方仍按旧语义决定输出全 null。

### 2. Value Extraction 语义

`float64_values(field)` 复用当前 `float64_values(batch, field)` 的语义：

- field 必须存在。
- field 必须是 `Float64`。
- null 输入返回 `None`。
- non-finite 输入返回 `None`。
- finite 输入返回 `Some(value)`。

这样 rank/zscore/demean 的数值行为不变。

### 3. Shared Stats

`float64_stats(field)` 基于 cached values 统计：

- 忽略 `None`。
- `mean = sum / sample_count`。
- `variance = sum((value - mean)^2) / sample_count`，保持当前 population variance 语义。
- `std = sqrt(variance)`。

`CS_ZSCORE`：

- sample 为空：全 null。
- `std == 0.0`：非 null value 输出 `0.0`。
- 否则输出 `(value - mean) / std`。

`CS_DEMEAN`：

- sample 为空：全 null。
- 否则输出 `value - mean`。

### 4. Shared Rank

`float64_ranks(field)` 基于 cached values 排序：

- 只对 `Some(value)` 排序。
- 排序继续使用 `f64::total_cmp`。
- tie rank 使用当前 average rank 语义：同值组 rank 为 `(start + 1 + end) / 2.0`。
- null / non-finite 行保持 `None`。

多个 `CSRankSpec::new(field, ...)` 指向同一 field 时，共用 rank vector。

### 5. Engine 接入

`CrossSectionalEngine::finalize_bucket()`：

```text
let bucket_batch = rows.materialize()?;
let mut context = CrossSectionalFactorContext::new(&bucket_batch);
for factor in &mut self.factors {
    columns.push(factor.evaluate_with_context(&mut context)?);
}
```

输出 id 和 bucket_start 构造保持不变。context 生命周期只覆盖本次 finalize，不跨 bucket 复用。

### 6. Backward Compatibility

`CrossSectionalFactor` trait：

```text
pub trait CrossSectionalFactor: Send {
    fn output_field(&self) -> Field;
    fn evaluate(&mut self, batch: &RecordBatch) -> Result<ArrayRef>;
    fn evaluate_with_context(&mut self, ctx: &mut CrossSectionalFactorContext<'_>) -> Result<ArrayRef> {
        self.evaluate(ctx.batch())
    }
}
```

已有自定义 factor 只实现 `evaluate()` 仍可编译并运行。内置 factor 同时保留 `evaluate()`，测试和旧调用路径仍可覆盖。

## 测试计划

新增/扩展测试：

- `CrossSectionalFactorContext`：
  - 同一 field 多次 `float64_values()` 返回相同值且不改变结果。
  - null 和 non-finite 都按 `None` 处理。
  - stats 与现有 zscore/demean 统计一致。
  - ranks 保持 tie average rank 语义。
- 内置 factor：
  - `evaluate_with_context()` 与旧 `evaluate()` 输出一致。
  - 同一 value field 的两个 rank factor 输出一致且共享 context cache。
- `CrossSectionalEngine`：
  - 现有 `cross_sectional_engine` 集成测试不变。
  - 增加多 factor 同 value field 的输出稳定性测试，如两个 rank + zscore + demean。

验证命令：

```text
cargo test -p zippy-operators --test cross_sectional_factors -- --nocapture
cargo test -p zippy-engines --test cross_sectional_engine -- --nocapture
cargo test -p zippy-engines --lib -- --nocapture
cargo clippy -p zippy-operators --all-targets -- -D warnings
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

## 后续扩展

- 将更多 CrossSectional factor 迁移到 context-native evaluation。
- 对 context cache 增加 lightweight counters，用 benchmark 验证提取/排序次数下降。
- 将 context-native shared stats 与未来行业中性化 / group_by 设计衔接。
- 设计 `CrossSectionalBucketState` touched-slot undo log，移除 engine 批级 clone。
