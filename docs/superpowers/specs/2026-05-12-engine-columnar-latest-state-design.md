# Engine Columnar Latest State Design

日期：2026-05-12

## 背景

`docs/performance_audit_fix_status_2026-05-12.md` 把 P004/P005/P006/P007 的剩余项归为
`Engine columnar state model` 专项。这一组问题共同指向当前 engine 热路径的状态存储方式：

- `TimeSeriesEngine` 仍在每批克隆 `open_windows` / `last_dt_by_id`，并按行分配 `String`
  key。
- `CrossSectionalEngine` 仍用 `BTreeMap<String, OwnedRow>` 保存当前 bucket，并在 finalize 时
  concat 单行 batch。
- `ReactiveLatestEngine` 仍用 `BTreeMap<Vec<String>, OwnedRow>` 保存最新行。
- `KeyValueTableMaterializer` 更新少量 key 后仍会构造全量 snapshot，并调用
  `replace_with_table()`。
- `ReactiveStateEngine` 多因子链路仍会为每个 factor 构造中间 `RecordBatch`。

本设计选择先处理 P006：`ReactiveLatestEngine` 与 `KeyValueTableMaterializer` 的 latest/key-value
状态。原因是这两条路径共享同一类语义：按 UTF8 key 保留最新一行，输入 batch 中同 key
多次更新时只保留最后一次。它们比 TimeSeries/CrossSectional 少了时间桶、迟到数据和窗口
rollback 复杂度，适合作为 columnar state model 的第一版 pilot。

## 目标

- 引入一个 `crates/zippy-engines` 内部共享的 latest state 组件。
- 用 slot-indexed state 替代 per-key `OwnedRow` 单行 `RecordBatch`。
- 让 `ReactiveLatestEngine` 与 `KeyValueTableMaterializer` 复用同一套 key 解析、slot 更新、
  delta 输出和 snapshot materialization 逻辑。
- `ReactiveLatestEngine.on_data()` 只输出本批被更新 key 的最终行，保持现有语义。
- `KeyValueTableMaterializer.on_data()` 第一版仍发布完整 snapshot，但 snapshot 从共享 state
  一次性 materialize，去掉每行单独抽取 `OwnedRow` 和重复 concat。
- 为后续 slot-level segment update / incremental snapshot 留出接口边界。

## 非目标

- 不在本专项第一版重写 `TimeSeriesEngine` 的 `open_windows` / `last_dt_by_id`。
- 不在本专项第一版重写 `CrossSectionalEngine` 的 bucket buffer。
- 不改变 `ReactiveStateEngine` 的 factor evaluation context。
- 不改变 public Python/Rust API。
- 不改变 latest/key-value 输出行排序：继续按 key 的稳定顺序输出。
- 不在第一版实现 active segment 原地 update 或 delete/retract。
- 不在第一版改变 `StreamTableMaterializer.replace_with_table()` 的 descriptor 语义。

## 现状问题

### ReactiveLatestEngine

当前状态：

- `latest_rows: BTreeMap<Vec<String>, OwnedRow>`。
- 每行通过 `record_batch_from_table_rows(..., &[row_index])` 抽成单行 `RecordBatch`。
- 输出本批 updates 时再对单行 batch 调 `concat_batches()`。
- flush snapshot 时对全量 `latest_rows` 再次 `concat_batches()`。

问题：

- 每行 update 都会构造 Arrow batch 元数据和 array slice/take。
- 同一 key 更新多次时，前面的单行 batch 已经被构造但最终丢弃。
- `ReactiveLatestEngine` 和 `KeyValueTableMaterializer` 维护重复逻辑。

### KeyValueTableMaterializer

当前状态：

- 每行同样抽取 `KeyValueOwnedRow`。
- 每批更新后通过 `concat_batches()` 构造全量 snapshot。
- 再调用 `replace_with_table()` 重新 rollover active segment 并发布 descriptor。

问题：

- 更新 1 个 key 时仍会重建 `total_keys` 行 snapshot。
- per-key `OwnedRow` 和 snapshot concat 带来大量小对象和 Arrow 元数据开销。
- 第一版可以先优化 state 到 snapshot 的构造方式；segment 原地 update 需要更大设计。

## 设计选项

### 方案 1：分别优化两个 engine 的 `OwnedRow` 路径

做法：

- 在 `ReactiveLatestEngine` 内单独改成 columnar state。
- 在 `KeyValueTableMaterializer` 内单独改成 columnar state。

优点：

- 单文件改动较直观。

缺点：

- 两条路径会继续重复 key 解析、slot 更新、snapshot 输出逻辑。
- 后续再做 id interning、delta snapshot 或 metrics 时需要改两遍。
- 不利于把 pilot 经验迁移到 P004/P005。

### 方案 2：抽内部 `LatestColumnarState`，两个 engine 共享（推荐）

做法：

- 新增内部模块，例如 `crates/zippy-engines/src/latest_state.rs`。
- `LatestColumnarState` 持有 schema、key column 配置、key 到 slot 的索引、slot 有效位和列式
  row storage。
- `ReactiveLatestEngine` 负责 engine 生命周期和输出策略。
- `KeyValueTableMaterializer` 负责 stream-table descriptor/persistence 生命周期。
- 两者共享同一个 state 组件。

优点：

- 直接消除两条 latest 路径的重复实现。
- 可以在一个地方维护 key 顺序、slot reuse、snapshot materialization 和测试。
- 后续支持 slot-level update 时，只需要在 materializer 层替换 snapshot 发布策略。

缺点：

- 需要设计一个清晰的内部状态 API。
- 第一版如果过度追求泛化，容易把 P004/P005/P007 的复杂度提前带进来。

### 方案 3：直接实现 KeyValue active segment 原地 update

做法：

- 跳过内部 state pilot，直接让 stream table 支持按 key slot 原地覆盖。
- active descriptor 指向一份可更新 segment，reader 读取 slot 最新值。

优点：

- 理论上最接近最终性能目标。

缺点：

- 会改变 segment writer、reader 可见性、descriptor、retention 和 stale reader 语义。
- 需要定义 update 的原子性、null validity、slot reuse、reader snapshot consistency。
- 风险明显高于本专项第一版。

## 采用方案

采用方案 2。

第一版只引入进程内 `LatestColumnarState`，优化 engine 内部状态模型和 snapshot 构造；不改变
stream-table segment 的 publish/replace 语义。这样可以先把 `OwnedRow`、重复 concat 和重复 key
逻辑拿掉，同时保持外部行为稳定。

## 核心设计

### 1. State 结构

内部状态概念如下：

```text
LatestColumnarState
  schema: SchemaRef
  key_fields: Vec<String>
  key_columns: Vec<usize>
  key_to_slot: BTreeMap<Vec<String>, usize>
  slots: Vec<LatestSlot>
  columns: Vec<LatestColumnStore>
```

`LatestSlot`：

```text
LatestSlot {
    key: Vec<String>,
    occupied: bool,
    sequence: u64,
}
```

`LatestColumnStore` 第一版只覆盖当前 stream table 已支持的类型：

- `Int64`
- `Float64`
- `Utf8`
- `Timestamp(Nanosecond, _)`

这些类型也与 `StreamTableMaterializer.write_materialized_columnar_rows()` 当前支持范围一致。若遇到
其他类型，构造 state 时直接返回 `SchemaMismatch` 或 `InvalidConfig`，不在热路径中半支持。

### 2. Key 和排序

第一版继续使用 `BTreeMap<Vec<String>, usize>`，保持输出按 key 稳定排序。

为什么不第一版就做 interning：

- P006 的直接热点是 per-row `OwnedRow` 和 full snapshot concat。
- `BTreeMap<Vec<String>, usize>` 可保持现有 deterministic order。
- id interning 更适合后续和 P004/P005 一起做 shared `IdRegistry`，否则容易在 latest state 中做出
  只能局部复用的实现。

### 3. Batch Update

state 暴露：

```text
apply_batch(table) -> LatestUpdateSet
```

语义：

- 先解析 key columns，并校验 key 非 null。
- 对输入 batch 顺序逐行处理。
- 若 key 不存在，分配新 slot。
- 若 key 已存在，覆盖对应 slot 的列值。
- 同一 batch 内同 key 多次出现，slot 最终保留最后一行。
- `LatestUpdateSet` 只记录本批最终被更新的 key/slot，且按 key 稳定输出。

第一版不做 rollback log。原因是当前 `ReactiveLatestEngine` 的输出构造在 state 更新前完成，
而新设计可以先在临时 `PendingLatestUpdate` 中解析和校验，再一次性 apply 到 state；如果解析失败，
state 不变。如果 apply 后后续 publish 失败，语义与当前 latest/key-value engine 一样，不引入新的
跨 publisher rollback 承诺。

### 4. Delta Materialization

state 暴露：

```text
materialize_slots(slots) -> RecordBatch
```

`ReactiveLatestEngine.on_data()`：

1. 调 `apply_batch(table)`。
2. 如果 update set 为空，返回空输出。
3. 调 `materialize_slots(update_set.slots)`。
4. 返回一个 delta `SegmentTableView`。

这保持现有语义：latest engine 输出本批更新 key 的最终行，而不是全量 snapshot。

### 5. Snapshot Materialization

state 暴露：

```text
materialize_snapshot() -> RecordBatch
```

`KeyValueTableMaterializer.on_data()`：

1. 调 `apply_batch(table)`。
2. 若 update set 为空，返回空输出。
3. 调 `materialize_snapshot()` 生成完整 key-value 表。
4. 继续调用现有 `StreamTableMaterializer.replace_with_table()`。
5. 返回 snapshot view。

`ReactiveLatestEngine.on_flush()` 同样使用 `materialize_snapshot()`。

第一版仍是全量 snapshot 发布，但它不再从 `BTreeMap<Vec<String>, OwnedRow>` concat
`total_keys` 个单行 batch，而是从 columnar slots 一次构造 Arrow arrays。

### 6. Null Handling

key columns：

- null key 仍报 `SchemaMismatch`，保持现有行为。

value columns：

- 第一版需要支持 nullable columns，因为 `RecordBatch` schema 可能包含 nullable fields。
- `LatestColumnStore` 对每列维护 validity。
- materialize 时按 schema nullable 构造 Arrow array。
- 若非 nullable 字段输入 null，报错，避免把非法状态写入 state。

如果 nullable column 的 columnar writer 能力不足，只影响后续 stream-table segment 写入；state
本身必须能正确 materialize nullable Arrow arrays。

### 7. 错误边界

`apply_batch()` 分两阶段：

- `prepare_batch_update(table)`：解析 schema、key、类型和值，构造临时 update。
- `commit_batch_update(update)`：修改 state。

这样保证输入数据非法时 state 不变。

对于 commit 后的下游失败：

- `ReactiveLatestEngine` 若 materialize delta 失败，返回错误；state 已更新。
- `KeyValueTableMaterializer` 若 `replace_with_table()` 失败，state 已更新。

这与当前非事务 engine 行为一致。本专项不扩大语义承诺；如果未来需要 publisher failure rollback，
应作为统一 engine transaction 设计处理。

### 8. Metrics

第一版可先加内部测试，不强制扩展 public metrics。

如果后续要扩展 `EngineMetricsDelta`，建议字段包括：

- `latest_keys_total`
- `latest_updated_keys_total`
- `latest_snapshot_rows_total`
- `latest_state_slots_total`

但这会触及核心 metrics 结构，第一版不做。

## 文件边界

预期改动：

- `crates/zippy-engines/src/latest_state.rs`
  - 新增内部 `LatestColumnarState`。
  - 新增 state 单元测试。
- `crates/zippy-engines/src/lib.rs`
  - 声明内部模块，不公开导出。
- `crates/zippy-engines/src/reactive_latest.rs`
  - 移除本地 `OwnedRow` / `LatestBatchUpdates`。
  - 接入 `LatestColumnarState`。
- `crates/zippy-engines/src/stream_table.rs`
  - `KeyValueTableMaterializer` 接入 `LatestColumnarState`。
  - 保留 `StreamTableMaterializer.replace_with_table()`。
- `crates/zippy-engines/tests/reactive_latest_engine.rs`
  - 增加同 batch 重复 key、flush snapshot、nullable value 行为测试。
- `crates/zippy-engines/tests/stream_table_materializer.rs`
  - 增加 key-value 更新少量 key 后 snapshot 正确性测试。
- `crates/zippy-engines/benches`
  - 增加或扩展 latest/key-value profile，用于比较大量 key 少量更新场景。

## 测试计划

Rust unit tests:

- state 构造拒绝空 key columns。
- state 构造拒绝缺失 key field。
- state 构造拒绝 unsupported data type。
- key column 为 null 时报错且 state 不变。
- 非 nullable value 输入 null 时报错且 state 不变。
- 同一 batch 内同 key 多次更新，只保留最后一行。
- delta materialization 按 key 稳定顺序输出。
- snapshot materialization 按 key 稳定顺序输出全部有效 slot。
- `ReactiveLatestEngine.on_data()` 只输出本批更新 key。
- `ReactiveLatestEngine.on_flush()` 输出全量 snapshot。
- `KeyValueTableMaterializer.on_data()` 输出和 active snapshot 一致。

性能/benchmark:

- 新增 latest state profile：
  - 100k keys 初始装载。
  - 每批更新 1k keys。
  - 对比旧路径或至少记录新路径 wall time。
- 新增 key-value materializer profile：
  - 100k keys + 小批量 update。
  - 记录 `on_data()` 时间与 active snapshot 行数。

验证命令：

```text
cargo test -p zippy-engines --test reactive_latest_engine
cargo test -p zippy-engines --test stream_table_materializer
cargo test -p zippy-engines --lib
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

## 后续扩展

本 pilot 完成后，再进入更大范围的 engine state model：

1. 为 P004/P005 设计 shared `IdRegistry` / interned id。
2. 为 `CrossSectionalEngine` 引入 columnar bucket buffer。
3. 为 `TimeSeriesEngine` 引入 touched-key undo log，去掉每批全量 clone。
4. 为 `ReactiveStateEngine` 引入 factor evaluation context，减少中间 `RecordBatch`。
5. 为 `KeyValueTableMaterializer` 设计 slot-level segment update 或增量 snapshot descriptor。

这个顺序的关键是先把最简单、语义最稳定的 latest/key-value state 做成可验证 pilot，再把抽象推广到
带时间、窗口和 rollback 的 engine。
