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

需要注意：`PartitionWriterHandle::clear_rows()` 不能直接用于 KeyValue snapshot 更新。它会发布
`committed_row_count = 0` 并通知读者，然后再清理/写入新 rows。如果直接用它替代
`replace_with_table()`，读者可能短暂看到空 snapshot 或部分 snapshot，破坏 key-value 表的原子可见性。

## 实现校验结论

原先考虑过“同一个 active segment 内重写整张 snapshot，最后一次性发布 committed row count”。
对当前 segment-store 内存模型做实现校验后，这个方案不能成立。

原因是当前读写协议只有 committed-prefix 边界，没有 payload 版本边界：

- `RowSpanView::from_active_descriptor()` / `from_active_attachment()` 只读取
  `committed_row_count`，确认 `end_row <= committed` 后就构造 active shm view。
- `ActiveSegmentReader` 的 control snapshot 也只是用 acquire 读取 `committed_row_count`。
- `SegmentBatchBuilder::write_i64()`、`write_f64()`、`write_utf8()` 会把列 payload、validity、
  utf8 offsets/value bytes 直接 mirror 到共享内存。
- 共享内存 header 没有 snapshot generation、seqlock、row version 或 double-buffer 指针。

因此即使 writer 在 rewrite 期间保持旧 `committed_row_count`，旧读者仍可能先读到旧 committed
row count，再在读取 payload 时撞上 writer 正在改写列数据，看到旧/新 snapshot 混合、半写 utf8
offsets 或半写 validity。这个问题比 `clear_rows()` 的空窗口更隐蔽，因为它不会表现为 out-of-bounds，
而是表现为内容级不一致。

结论：在当前内存模型下，同 active segment snapshot rewrite 不是安全的小改动。本专项不能直接实现
这个方案。

## 目标

- 保持读侧 snapshot 原子可见：读者只能看到旧完整 snapshot 或新完整 snapshot，不能看到空/半写状态。
- 明确 `KeyValueTableMaterializer` 当前唯一安全的 snapshot 替换边界。
- 把后续优化拆成独立 segment memory model 专项，避免把 churn 优化混入一致性风险。
- 保持现有 key-value 语义：
  - key 由 `by` 字段定义。
  - 同 key 保留最新行。
  - active snapshot 按 key 稳定排序。
  - output schema 不变。
- 不改变 `LatestColumnarState` 的 public 行为。

## 非目标

- 不在本阶段改变查询 API 或 Python API。
- 不要求普通 append-only `StreamTableMaterializer` 使用新路径。
- 不做 reader-side latest-by-key collapse。
- 不引入跨 engine shared id registry。
- 不在当前 segment memory model 下做 row-slot overwrite。
- 不实现同 active segment 原地重写。

## 设计选项

### 方案 A：同 active segment snapshot rewrite（当前不采用）

做法：

- 在 writer lock 内保持旧 `committed_row_count`。
- 清理/重写 active segment payload 到新 snapshot。
- 所有列写完后一次性发布新的 `committed_row_count` 并通知读者。

优点：

- 可以避免每批小更新都 rollover 新 segment。
- 不需要改变 descriptor identity。

缺点：

- 当前读端没有 payload 版本校验，读者可能在旧 committed prefix 下读到正在重写的 payload。
- UTF8 offsets/value buffer、validity bitmap 和 fixed-width values 都可能出现跨版本混合。
- 要让这个方案成立，必须先扩展 segment memory model，而不是只新增 writer API。

### 方案 B：row-slot overwrite（当前不采用）

做法：

- `LatestColumnarState` 暴露 key 对应 slot。
- `KeyValueTableMaterializer` 建立 key -> active segment row index。
- 已存在 key 的更新直接 overwrite 对应 row；新 key append 到 active segment。

优点：

- 真正把小更新成本降到 `O(updated_keys)`。
- 不需要每批 materialize 全量 snapshot。

缺点：

- 当前 segment store 是 append/committed-prefix 模型，没有安全 overwrite API。
- UTF8 变长列 overwrite 需要重新管理 offsets/value buffer。
- 多列 row overwrite 同样需要版本/事务边界，否则读者可能看到半行新半行旧。

### 方案 C：append-only delta + reader latest collapse（当前不采用）

做法：

- KeyValue active stream 不再保存完整 snapshot，而是追加 delta rows。
- descriptor 标记 `latest_by=[...]`，读侧按 key collapse latest。

优点：

- 写入成本低。
- 保留 append-only segment 模型。

缺点：

- 改变读侧语义和 descriptor contract。
- 每次 snapshot 查询都需要 collapse，读成本变高。
- persisted scan、Gateway collect 和 stale reader 都要理解 latest-by descriptor。

### 方案 D：descriptor-level shadow snapshot replace（当前保留）

做法：

- 继续用 `replace_with_table()` 构建新的 active segment。
- 完整写入新 snapshot 后发布新 descriptor。
- 旧 descriptor / 旧 segment 通过 retention 保持给已 attach 的旧读者。

优点：

- 这是当前唯一已经具备原子 snapshot 可见性的路径。
- 不需要读端 payload retry、generation 或 seqlock。
- 和 stale reader retention、descriptor publisher、persist 健康检查保持一致。

缺点：

- 不能消除 segment churn。
- 小批更新仍会 materialize 全量 snapshot。

## 采用方案

当前采用方案 D，并明确终止方案 A 的直接实现。

这不是性能上的最终形态，而是把安全边界划清：只要 active shm payload 没有版本化读取协议，
KeyValue snapshot 的原子更新就只能通过“写新 segment，然后发布新 descriptor”来实现。

后续如果要继续降低 churn，应先进入独立的 segment memory model 专项。该专项要先提供一个读写双方都遵守的
payload 版本边界，再回到 KeyValue snapshot 更新。

## 后续专项设计：Active Payload Version Boundary

### 推荐方向

在 active segment header 中加入 snapshot generation / seqlock：

- writer 开始重写前把 generation 发布为 odd，表示 payload 正在更新。
- writer 写完所有 payload、row count、validity 和 utf8 offsets 后，把 generation 发布为新的 even。
- reader 构造 active view 或读取 batch 时：
  - 先读取 even generation 和 committed row count。
  - 读取 payload。
  - 再读取 generation。
  - 如果 generation 变化或为 odd，则 retry。

这个协议能把“是否读到同一个 payload 版本”变成显式 contract。只有在这个 contract 落地后，
方案 A 或 row-slot overwrite 才有安全基础。

### 设计约束

- header layout 需要版本迁移，旧 descriptor 要么拒绝新能力，要么走兼容路径。
- reader retry 必须有明确上限或 backoff，避免 writer 高频更新时 spin。
- UTF8 offsets/value bytes 的读取必须被 generation 覆盖，不能只保护 fixed-width values。
- writer 失败时必须能从 odd generation 恢复到旧 even generation，或把 segment 标记为不可用并发布新 segment。
- 普通 append-only 写入也要定义是否 bump generation，避免同一套 reader 语义出现分叉。

### 计划入口

新专项不应从 `KeyValueTableMaterializer` 开始，而应从 `zippy-segment-store` 开始：

1. 先设计 active segment header 版本字段和兼容策略。
2. 再设计 `RowSpanView` / active reader 的 retry 语义。
3. 最后才把 KeyValue snapshot rewrite 接到新的安全原语上。

## 当前代码边界

`KeyValueTableMaterializer` 保持当前路由：

```text
snapshot = latest_state.materialize_snapshot()
table.replace_with_table(snapshot)
```

这条路径虽然有 churn，但提供了正确的 snapshot 原子可见性。当前不新增
`rewrite_active_with_table()`，也不新增 `rewrite_rows_atomically()`，避免引入无法被现有读端验证的一致性风险。

## 测试策略

本专项当前不落代码，因此不新增测试。

如果后续进入 Active Payload Version Boundary 专项，测试应先覆盖 segment-store：

- reader 在 writer odd generation 期间不会返回半写 batch。
- writer 成功后 reader 只能读到完整新 generation。
- writer 失败后 reader 仍能读到旧完整 generation，或明确收到 segment invalid 状态。
- UTF8 offsets/value、validity bitmap、fixed-width values 都包含在同一 generation 校验里。

然后再扩展 engines：

- `KeyValueTableMaterializer` 小更新不 rollover segment。
- active descriptor identity 保持不变但 generation 变化。
- snapshot 超过 active capacity 时 fallback 到 descriptor-level replace。

## 决策记录

- 2026-05-13：实现校验发现 same-segment snapshot rewrite 在当前 payload 无版本模型下不安全。
- 2026-05-13：保留 `replace_with_table()` 作为 KeyValue snapshot 原子更新的唯一实现路径。
- 2026-05-13：把 churn 优化后移到 Active Payload Version Boundary 专项。
