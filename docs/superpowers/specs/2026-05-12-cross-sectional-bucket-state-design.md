# CrossSectional Bucket State Design

日期：2026-05-12

## 背景

`CrossSectionalEngine` 的首版设计目标是一个事件时间驱动的、可复现的最小截面因子引擎：

- 输入按 `dt` 对齐到固定 `bucket_start`。
- 当前 bucket 内每个 `id` 只保留最后一条输入记录。
- 看到更大 bucket 时关闭旧 bucket 并产出结果。
- `flush()` 强制关闭当前 open bucket。
- 输出只覆盖该 bucket 实际到达的 `id` 集合，不补 universe、不做前值填充。
- 输出按 `id` 稳定排序，保证 replay 可复现。

当前实现用 `BTreeMap<String, OwnedRow>` 保存当前 bucket。每行输入都会通过
`record_batch_from_table_rows(..., &[row_index])` 抽成单行 `RecordBatch`；finalize 时再把所有
单行 batch `concat_batches()` 成 bucket batch，然后交给 `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN`
计算。

P005 性能审计指出，这条路径会在大截面场景中产生大量小 batch、Arrow 元数据和 concat 开销。
本设计只替换 bucket 内部状态模型，不改变 `CrossSectionalEngine` 的事件时间语义和
`CrossSectionalFactor` trait。

## 目标

- 引入内部 `CrossSectionalBucketState`，替代 `BTreeMap<String, OwnedRow>`。
- 用 key -> slot 索引和 typed column stores 保存当前 bucket 每个 id 的最后一行。
- finalize 时从 columnar slots 一次 materialize bucket `RecordBatch`。
- 保持输出按 id 稳定排序。
- 保持同 bucket 同 id 最后一行覆盖语义。
- 保持 late data、flush、空 bucket 和 skipped bucket 语义不变。
- 不改变 public Rust/Python API。

## 非目标

- 不改变 `CrossSectionalFactor` trait。
- 不实现 `CrossSectionalFactorContext`。
- 不做 rank/zscore/demean 的共享统计 pass。
- 不引入 shared id interning。
- 不支持 universe 补全、forward fill、group_by 或行业中性化。
- 不改变 late-data policy；当前仍只支持 `Reject`。
- 不改变输出 schema。

## 设计选项

### 方案 1：专用 `CrossSectionalBucketState`（采用）

做法：

- 新增 `crates/zippy-engines/src/cross_sectional_bucket.rs`。
- 状态结构专门服务 current bucket：

```text
CrossSectionalBucketState
  schema: SchemaRef
  id_field: String
  id_index: usize
  key_to_slot: BTreeMap<String, usize>
  slots: Vec<BucketSlot>
  columns: Vec<BucketColumnStore>
```

- `apply_row(table, row_index)` 更新或插入该 id 对应 slot。
- `materialize()` 按 `key_to_slot` 的 key 顺序构造 bucket `RecordBatch`。
- `clear()` 在 bucket 被关闭后释放当前 rows，但保留可复用容量。

优点：

- 不改变 engine 外部语义。
- 直接消除 per-row `OwnedRow` 和 finalize `concat_batches()`。
- 生命周期与 CrossSectional bucket 完全贴合，比复用 latest state 更清晰。

缺点：

- 和 `LatestColumnarState` 有部分 typed column store 相似代码。
- 多因子共享统计 pass 仍未解决。

### 方案 2：复用或泛化 `LatestColumnarState`

做法：

- 把 latest state 抽成更通用的 keyed latest row state。
- `ReactiveLatest`、`KeyValue`、`CrossSectional` 都复用同一个组件。

优点：

- 可减少一些重复代码。

缺点：

- CrossSectional 有 bucket 生命周期、flush/late-data 状态机和当前 bucket reset 语义。
- Latest/KeyValue 是长期状态，CrossSectional 是短生命周期 bucket 状态。
- 为了强行复用会让通用组件携带过多不同生命周期选项。

### 方案 3：先只优化 `build_bucket_batch()`

做法：

- 继续保存 `OwnedRow`。
- finalize 时尝试减少 concat 或减少 schema 克隆。

优点：

- 改动最小。

缺点：

- 不能消除每行单行 `RecordBatch` 抽取。
- P005 的主要分配热点仍然存在。

## 采用方案

采用方案 1。

第一版只替换 current bucket 的内部状态，不改 factor trait 和统计计算方式。这样改动集中在
`CrossSectionalEngine` 状态模型，行为风险可控，也为后续 `CrossSectionalFactorContext` 或共享
统计 pass 留出空间。

## 核心设计

### 1. Bucket State 结构

内部模块：

```text
crates/zippy-engines/src/cross_sectional_bucket.rs
```

核心类型：

```text
CrossSectionalBucketState {
    schema: SchemaRef,
    id_field: String,
    id_index: usize,
    key_to_slot: BTreeMap<String, usize>,
    slots: Vec<BucketSlot>,
    columns: Vec<BucketColumnStore>,
}
```

`BucketSlot`：

```text
BucketSlot {
    occupied: bool,
}
```

第一版不保存 sequence。输出顺序由 `BTreeMap<String, usize>` 决定；同 id 最后一行覆盖通过
slot update 完成。

### 2. 支持的列类型

第一版支持当前 engine 和 tests 已覆盖的常用 Arrow 类型：

- `Utf8`
- `Float64`
- `Int64`
- `Timestamp(Nanosecond, _)`

如果输入 schema 有其他类型，`CrossSectionalBucketState::new()` 返回 `SchemaMismatch`，避免
热路径半支持。

后续如果 CrossSectionalEngine 需要支持更多基础列，可以在 `BucketColumnStore` 中显式增加。

### 3. Row Apply

`apply_row(table, row_index)` 语义：

- 校验 table schema 与 state schema 相同。
- 读取 id column，id 不能为 null。
- id 不存在时分配新 slot，并为每列扩容一格。
- id 已存在时覆盖原 slot。
- 所有字段从输入 row 拷贝到 typed column store。
- 非 nullable 字段输入 null 时返回 `SchemaMismatch`。

这保持当前 “同 bucket 同 id 最后一行覆盖” 的语义。

### 4. Materialize

`materialize()`：

- 如果当前 bucket 为空，返回空 batch。
- 按 `key_to_slot` 的 key 排序遍历 slots。
- 对每个 column store 构造 Arrow array。
- 返回 schema 与原 input schema 相同的 bucket `RecordBatch`。

`CrossSectionalEngine::finalize_bucket()` 继续接收 bucket `RecordBatch` 并执行 factors：

```text
bucket_batch = current_rows.materialize()
output columns = [id, bucket_start, factor outputs...]
```

输出中的 `dt_column` 仍是 `bucket_start`，不是 bucket 中每个 row 的原始 `dt`。

### 5. Engine 状态机接入

`CrossSectionalEngine` 字段从：

```text
current_rows: BTreeMap<String, OwnedRow>
```

改为：

```text
current_rows: CrossSectionalBucketState
```

`on_data()` 状态机保持不变：

- 当前 bucket 为空：设置 `current_bucket_start`，`current_rows.apply_row(...)`。
- 同 bucket：`current_rows.apply_row(...)`。
- 更大 bucket：先 finalize 当前 bucket，再 `current_rows.clear()` 并 apply 新 row。
- 已关闭 bucket：按 late data 处理。

`on_flush()`：

- 若没有 current bucket 或 bucket state 为空，返回空。
- 否则 materialize/finalize，然后清空 state。

### 6. 错误与状态边界

当前 CrossSectionalEngine 在 `on_data()` 中会先 clone `current_rows`，等整批处理成功后再提交，
这给非法输入/late data error 提供了“不污染当前状态”的边界。

第一版保留这个边界：

- `CrossSectionalBucketState` 实现 `Clone`。
- `on_data()` 继续使用 `next_current_rows = self.current_rows.clone()`。
- 行处理失败时，错误直接返回，`self.current_rows` 不变。

这不是最终最高性能形态，但比直接原地更新安全。后续若要去掉 clone，应单独设计 touched-slot undo log。

### 7. 输出与 factor 语义

本专项不改变 `CrossSectionalFactor`：

- factors 仍接收 materialized bucket `RecordBatch`。
- `CS_RANK`、`CS_ZSCORE`、`CS_DEMEAN` 仍各自扫描输入列。
- 多因子共享统计 pass 留给后续专项。

这样 P005 第一阶段专注消除 per-row batch 和 concat，不把 trait 迁移和统计重写混进同一个风险面。

## 测试计划

新增/扩展 Rust tests：

- `CrossSectionalBucketState`：
  - 同 id 重复更新只保留最后一行。
  - materialize 按 id 稳定排序。
  - null id 报错。
  - 非 nullable 字段 null 报错。
  - timestamp timezone 保留。
- `CrossSectionalEngine`：
  - 现有 11 个 engine tests 不变。
  - 增加一个非法 row 不污染已打开 bucket 的测试。

验证命令：

```text
cargo test -p zippy-engines --lib cross_sectional_bucket -- --nocapture
cargo test -p zippy-engines --test cross_sectional_engine -- --nocapture
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

## 后续扩展

- 为 `CrossSectionalFactor` 增加 context-native evaluation，避免 factor 侧只接受
  `RecordBatch`。
- 对 `CS_ZSCORE` 和 `CS_DEMEAN` 共享一次 mean/std 统计。
- 对 rank/zscore/demean 的同一 value column 共享 `Vec<Option<f64>>` 提取结果。
- 与 P004/P007 的 shared id registry 合并考虑 id interning。
- 设计 touched-slot undo log，移除 `on_data()` 批级 clone。
