# KeyValue Atomic Snapshot Update Design

日期：2026-05-13

## 背景

P006 已经把 `ReactiveLatestEngine` 和 `KeyValueTableMaterializer` 的最新行状态迁到
`LatestColumnarState`：

- latest rows 存在 key -> slot + typed column stores 中。
- 同批同 key 只保留最后一行。
- `ReactiveLatestEngine` 只输出本批更新 key 的 delta rows。
- `KeyValueTableMaterializer` 仍然在每批更新后 materialize 全量 snapshot，并调用
  `StreamTableMaterializer::replace_with_table()`。

当前 `replace_with_table()` 会 rollover 当前 active segment，把旧 snapshot 放进
`replacement_retained_snapshots`，再把新的完整 snapshot 写入新 active segment 并发布 descriptor。
这个方案读侧语义安全：旧读者继续看旧 segment，新读者看到新 descriptor；但对高频 latest/key-value
表来说，每批小更新都会制造新 active segment、旧 snapshot retention 和全量 snapshot 重写。

需要注意：`PartitionWriterHandle::clear_rows()` 已存在，但不能直接用于 KeyValue snapshot 更新。
它会发布 committed row count = 0 并通知读者，然后再写入新 rows。如果直接用它替代
`replace_with_table()`，读者可能短暂看到空 snapshot 或部分 snapshot，破坏 key-value 表的原子可见性。

## 目标

- 降低 `KeyValueTableMaterializer` 每批小更新造成的 active segment rollover 和旧 snapshot retention churn。
- 保持读侧 snapshot 原子可见：读者只能看到旧完整 snapshot 或新完整 snapshot，不能看到空/半写状态。
- 保持现有 key-value 语义：
  - key 由 `by` 字段定义。
  - 同 key 保留最新行。
  - active snapshot 按 key 稳定排序。
  - output schema 不变。
- 保留 descriptor publisher 和 stale reader retention 的安全边界。
- 不改变 `LatestColumnarState` 的 public 行为。

## 非目标

- 不在本阶段改变查询 API 或 Python API。
- 不要求普通 append-only `StreamTableMaterializer` 使用新路径。
- 不做 reader-side latest-by-key collapse。
- 不引入跨 engine shared id registry。
- 不在第一阶段做 row-slot overwrite。
- 不放弃旧 descriptor / retention 兼容性。

## 设计选项

### 方案 A：原子同 active segment snapshot rewrite（推荐）

做法：

- 在 segment-store writer 增加一个“未发布 rewrite”能力：
  - 在 writer lock 内把 committed row count 暂时保持为旧值。
  - 清理/重写内部 row buffers 到新 snapshot。
  - 所有列写完后一次性发布新的 committed row count，并通知读者。
- `StreamTableMaterializer` 增加内部方法：

```text
rewrite_active_snapshot_atomically(table)
```

- `KeyValueTableMaterializer` 在 snapshot row count 不超过 active segment capacity 且没有 persist /
  replacement-retention 特殊约束时，优先走 atomic rewrite。
- 如果 atomic rewrite 不适用，继续 fallback 到现有 `replace_with_table()`。

优点：

- 保持读者只看到完整旧 snapshot 或完整新 snapshot。
- 避免每批小更新都 rollover 新 segment。
- 不需要改变 descriptor 格式或读侧 latest 语义。
- 改动集中在 writer 原子提交边界和 KeyValue materializer 路由。

缺点：

- 仍然会重写全量 snapshot；只是消除 segment churn 和 descriptor churn。
- 需要小心处理 UTF8 buffer / validity buffer 的内部清理与回滚。
- 如果新 snapshot 行数超过 active capacity，仍要 fallback replace。

### 方案 B：row-slot overwrite

做法：

- `LatestColumnarState` 暴露 key 对应 slot。
- `KeyValueTableMaterializer` 建立 key -> active segment row index。
- 已存在 key 的更新直接 overwrite 对应 row；新 key append 到 active segment。

优点：

- 真正把小更新成本降到 `O(updated_keys)`。
- 不需要每批 materialize 全量 snapshot。

缺点：

- 当前 segment store 主要是 append/committed-prefix 模型，没有公开安全 overwrite API。
- UTF8 变长列 overwrite 需要重新管理 offsets/value buffer，容易破坏读者一致性。
- 多列 row overwrite 需要版本/事务边界，否则读者可能看到半行新半行旧。
- 这是更大的 segment memory model 变更，适合后续独立专项。

### 方案 C：append-only delta + reader latest collapse

做法：

- KeyValue active stream 不再保存完整 snapshot，而是追加 delta rows。
- descriptor 标记 `latest_by=[...]`，读侧按 key collapse latest。

优点：

- 写入非常便宜。
- 保留 append-only segment 模型。

缺点：

- 改变读侧语义和 descriptor contract。
- 每次 snapshot 查询都需要 collapse，读成本变高。
- stale reader、persisted scan、Gateway collect 都要理解 latest-by descriptor。
- 范围超过 P006 小专项。

## 采用方案

采用方案 A。

第一阶段目标是消除 KeyValue 高频小更新下的 segment replacement churn，并保持现有 snapshot 读语义。
它不试图一次性解决全量 snapshot materialization。真正的 `O(updated_keys)` row-slot overwrite 留给
后续 segment memory model 专项。

## 核心设计

### 1. Segment Store 原子 rewrite API

在 `PartitionWriterHandle` 增加：

```text
rewrite_rows_atomically(row_count, write_columns) -> Result<usize>
```

语义：

- 必须持有 partition writer lock。
- active segment 不能 sealed。
- 不允许当前存在 open row。
- `row_count <= capacity`，否则返回 `segment is full`。
- rewrite 期间对外 committed row count 仍保持旧值。
- 先清理内部 row buffers、validity、utf8 offsets/value buffer，但不发布 committed row count。
- 按 columnar writer 写入完整新 snapshot。
- 全部成功后一次性发布 committed row count = `row_count` 并通知读者。
- 任一步失败时，第一版允许返回错误并要求调用方 fallback `replace_with_table()`；但只有在未发布新
  committed row count 前失败，读者仍看到旧 committed prefix。

实现上不复用现有 `clear_rows()`，因为它会立即发布 committed=0。应新增内部 builder 方法，例如：

```text
prepare_rewrite_rows(row_count)
finish_rewrite_rows(row_count)
abort_rewrite_rows()
```

其中 `finish_rewrite_rows()` 是唯一发布 committed row count 和通知读者的步骤。

### 2. `StreamTableMaterializer` 接入

新增内部方法：

```text
fn rewrite_active_with_table(&mut self, table: &SegmentTableView) -> Result<bool>
```

返回：

- `Ok(true)`：已原子重写 active snapshot 并发布 descriptor。
- `Ok(false)`：不适用，调用方应 fallback `replace_with_table()`。
- `Err(error)`：rewrite 前置校验或写入失败；如果 writer 保证旧 committed prefix 未被破坏，可
  fallback；如果进入不可恢复状态，则返回错误。

适用条件：

- schema 与 input schema 完全一致。
- `table.num_rows() <= active_capacity`。
- 没有 active sealed 状态。
- 第一版可以先禁用 persist_config 场景，避免 rewrite active 与 parquet sealed-segment 任务语义交叉。

rewrite 成功后：

- active descriptor 的 `segment_id/generation/shm_os_id` 可以保持不变。
- committed row count 和 descriptor envelope 中 row count 更新。
- 仍调用 descriptor publisher，让外部控制面知道 active snapshot 内容已更新。

### 3. `KeyValueTableMaterializer` 路由

当前：

```text
snapshot = latest_state.materialize_snapshot()
table.replace_with_table(snapshot)
```

改为：

```text
snapshot = latest_state.materialize_snapshot()
if !table.rewrite_active_with_table(snapshot)? {
    table.replace_with_table(snapshot)?
}
```

行为：

- 空 input batch 不变。
- invalid update 不污染 latest state 的现有 prepare/commit 边界保持不变。
- snapshot 行数超过 active capacity 时 fallback replace。
- persist / retention 不适用时 fallback replace。

### 4. Reader Safety

必须保持：

- 旧 reader 在 rewrite 过程中最多读取旧 committed row count 范围。
- 新 committed row count 发布后，读者看到完整新 snapshot。
- 不出现 committed=0 的空窗口。
- 不出现 committed row count 指向尚未完整写入的 rows。

第一版不承诺旧 reader 反复读取同一 active segment 时能稳定看到同一内容版本；当前 active segment
本来就是 live 视图。强约束是每次读取 committed prefix 时不能看到半写 row 或半写 column。

### 5. Fallback 与错误处理

如果 rewrite 不适用：

- 保持现有 `replace_with_table()` 路径。

如果 rewrite 写入失败：

- 如果失败发生在 committed row count 发布前且 writer 可以恢复旧 buffers，则返回错误或 fallback。
- 如果 writer 无法安全恢复旧 buffers，必须返回错误，不允许发布 descriptor。

因此实现计划里应先补最小 segment-store 单测，覆盖：

- rewrite 期间 committed row count 不提前变化。
- rewrite 成功后 active batch 是完整新 snapshot。
- rewrite 失败不发布 partial committed prefix。

## 测试计划

新增/扩展测试：

- `zippy-segment-store`：
  - atomic rewrite 成功后 committed row count 一次性变为新 row count。
  - rewrite 可以把 2 行改成 1 行或 3 行。
  - 写入失败时 committed row count 不变。
- `StreamTableMaterializer`：
  - `rewrite_active_with_table()` 不 rollover segment。
  - descriptor publisher 会收到更新。
  - schema mismatch 返回 `SchemaMismatch`。
- `KeyValueTableMaterializer`：
  - 小更新后 active descriptor `segment_id/shm_os_id` 不变。
  - active snapshot 内容更新。
  - snapshot 超过 row capacity 时 fallback 到 replace，segment id 变化。
  - persist_config 场景 fallback 到 replace。

验证命令：

```text
cargo test -p zippy-segment-store --lib rewrite -- --nocapture
cargo test -p zippy-engines --test stream_table_materializer -- --nocapture
cargo clippy -p zippy-segment-store --all-targets -- -D warnings
cargo clippy -p zippy-engines --all-targets -- -D warnings
cargo fmt --check
```

## 后续扩展

- row-slot overwrite，真正把 existing key update 降到 `O(updated_keys)`。
- latest state 暴露 stable slot / key ordering metadata。
- descriptor 增加 optional snapshot generation，帮助读者识别同一 active segment 的内容版本。
- persisted key-value snapshot compact / checkpoint 策略。
