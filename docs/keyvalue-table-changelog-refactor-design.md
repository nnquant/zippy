# KeyValue Table Changelog Refactor Design

日期：2026-05-18

## 背景

当前 `ReactiveLatestEngine.stream_table()` 的产物更接近 key-value snapshot 表，而不是普通
append stream。它的逻辑语义是：

```text
latest_by key -> latest row
```

现有实现为了让这张表可查询，会把最新状态 materialize 成一张完整 snapshot，然后通过
`KeyValueTableMaterializer` 更新 active segment。这个路径能服务 `read_table()` / `collect()`，
但和 `subscribe()` 的 row offset 语义冲突：

- 普通 `subscribe()` 追的是 active/sealed segment 中新增的 row offset。
- key-value 更新是同一个 key 的当前值变化，不一定增加 snapshot 行数。
- snapshot active payload rewrite 或 replace 是表版本变化，不是 append row 变化。
- 下游按 row offset 订阅时，读完初始 snapshot 后可能再也看不到后续同 key 更新。

已有设计已经明确：当前 active segment 只有 committed-prefix 边界，没有 payload version 边界。
因此在同一个 active segment 内原地改写 committed payload 不安全，读者可能看到跨版本混合数据。
`replace_with_table()` 通过写新 segment 再发布 descriptor 保持了 snapshot 原子可见性，但高频
KeyValue 小更新会产生全量 snapshot 重写和 segment churn。

本文目标不是继续把 KeyValue 表伪装成 append 表，而是把底层语义拆清楚：

```text
写入和订阅：append-only changelog
查询当前状态：materialized snapshot
一致性衔接：snapshot watermark / changelog sequence
```

## 问题定义

KeyValue 表需要同时满足三类访问模式：

1. 订阅变化：策略或 watcher 想知道哪些 key 刚更新。
2. 查询当前状态：UI、策略初始化、健康检查想拿完整最新表。
3. 从任意时刻恢复：进程重启后能从一个确定状态恢复到当前状态。

如果只使用 snapshot：

- `subscribe()` 无法靠 row offset 观察同 key 覆盖更新。
- 每次小更新都可能触发全量 snapshot replace。
- 高频行情和因子最新值场景会放大 CPU、内存分配和 descriptor churn。

如果只使用 changelog：

- 从 tail 开始消费的下游只能看到之后的变化，无法得到完整当前表。
- 从不同 offset 开始 materialize，最终本地表状态可能不同。
- 删除、重复、断线重连和 compact 后恢复都需要额外水位语义。

因此 KeyValue 表必须是：

```text
snapshot(version=N, last_applied_changelog_seq=S)
        +
changelog(seq > S)
        =
current key-value state
```

## 目标

- 让 `subscribe()` 对 KeyValue 表具备可靠变化回调：每次 upsert/delete 都对应 changelog row。
- 让 `subscribe_table()` 继续提供完整 snapshot watch 语义，下游每次收到的都是完整当前表。
- 让 `read_table()` 读取一个带水位的一致性 snapshot，避免读到半写状态。
- 保持普通 append stream 的现有语义和性能路径。
- 避免要求下游理解 internal segment、descriptor、reader lease 或 materializer 细节。
- 为后续高频场景优化提供正确边界：先保证语义，再优化 snapshot materialization 成本。

## 非目标

- 不把所有普通 stream 改成 changelog。
- 不要求第一阶段支持远端历史 seek 或任意 offset replay。
- 不在第一阶段实现 row-slot overwrite。
- 不在第一阶段依赖 active payload seqlock。
- 不要求 `subscribe()` 对 KeyValue 表直接返回完整表；完整表仍由 `subscribe_table()` 负责。
- 不把 delete 语义下放给用户猜测；delete 必须是显式 changelog op。

## 推荐架构

KeyValue 表底层拆成三个对象：

```text
KeyValueTable
  |
  |-- changelog stream      append-only，承载 upsert/delete
  |-- snapshot table        当前完整 key -> row 状态
  |-- version index         记录 snapshot_version、last_changelog_seq、schema、latest_by
```

数据流：

```text
upstream delta/update
        |
        v
append changelog segment
        |
        v
update in-memory key index
        |
        v
publish snapshot version / optional snapshot materialization
```

读写边界：

| 路径 | KeyValue 表语义 | 底层依据 |
| --- | --- | --- |
| `subscribe()` | 变化事件流 | changelog row offset / changelog seq |
| `subscribe_table()` | 完整 snapshot watch | snapshot version / descriptor generation |
| `read_table()` | 当前一致 snapshot | snapshot descriptor + last changelog seq |
| recovery | snapshot 后 replay changelog | snapshot watermark |

这个拆分保留 segment 的优势：segment 继续做 append-only changelog，这是它最擅长的模型。
snapshot 不再承担“被 row subscriber 追增量”的职责。

## Changelog 数据模型

每个 KeyValue 表有一个内部 changelog stream。第一版可以用内部命名，不作为主 API 暴露：

```text
<table_name>.__kv_changelog
```

changelog row 至少包含：

| 字段 | 含义 |
| --- | --- |
| `_zippy_kv_seq` | 单调递增 changelog sequence |
| `_zippy_kv_op` | `upsert` 或 `delete` |
| `_zippy_kv_snapshot_version` | 写入后目标 snapshot version，可选但推荐保留 |
| latest-by columns | key 字段，沿用用户 schema 中的 key 列 |
| payload columns | upsert 时的完整 row 值 |

约束：

- `upsert` 必须携带完整 row，不发送字段 diff。
- `delete` 必须携带完整 key；非 key payload 可以为 null 或省略到 tombstone schema。
- `_zippy_kv_seq` 在单 writer 内严格递增。
- 同一 key 在同一个 batch 中只保留最后一个 op，避免下游重复处理无意义中间状态。
- changelog schema 需要记录原始 table schema 和 latest_by metadata。

使用完整 upsert 而不是字段 diff 的原因是幂等性和恢复简单。下游重复消费某个 seq 时，只需要
按 seq 去重；重放 upsert 不依赖旧字段值。

## Snapshot 数据模型

snapshot table 表示完整当前状态：

```text
key -> latest row
```

snapshot descriptor metadata 必须包含：

| metadata | 含义 |
| --- | --- |
| `zippy_table_semantics=key_value_snapshot` | 表语义 |
| `zippy_latest_by=[...]` | key 字段 |
| `zippy_kv_snapshot_version` | 当前 snapshot 版本 |
| `zippy_kv_last_changelog_seq` | 该 snapshot 已应用的最大 changelog seq |
| `zippy_kv_changelog_stream` | 对应 changelog stream 名称 |
| `zippy_kv_schema_version` | schema 兼容性版本 |

snapshot 发布必须满足原子切换：

```text
write complete snapshot payload
publish descriptor with snapshot_version and last_changelog_seq
notify watchers
```

在 active payload version boundary 落地前，snapshot 仍应使用 descriptor-level replace，而不是
同 active segment 原地 rewrite。这样读者只能看到旧完整 snapshot 或新完整 snapshot。

## 一致性协议

### 写入顺序

单 writer KeyValue 写路径：

```text
1. 归并本批同 key 更新，得到最终 op。
2. 为每个 op 分配连续 _zippy_kv_seq。
3. append changelog rows 并 publish committed prefix。
4. 更新 writer 内存 key index。
5. bump snapshot_version。
6. 按策略 materialize snapshot，并发布 snapshot descriptor。
```

第一阶段可以采用同步策略：每批 changelog 成功提交后，立即发布对应 snapshot descriptor。
后续可引入节流：

```text
changelog 每批提交；
snapshot 每 N ms、N rows 或显式 flush 时 materialize；
descriptor 标明 last_changelog_seq。
```

节流后 `subscribe()` 仍能实时看到变化，`subscribe_table()` 的完整表回调频率由 snapshot 策略决定。

### 读表语义

`read_table()` 返回 snapshot 表，同时内部知道该表的 watermark：

```text
snapshot_version = V
last_changelog_seq = S
```

普通用户不需要直接看到这两个字段；但高级恢复或调试 API 可以暴露。

### 本地物化语义

如果未来提供“下游自己维护 KeyValue 状态”的高级 API，流程必须封装为：

```text
1. read snapshot，得到 table 和 last_changelog_seq=S。
2. subscribe changelog from S+1。
3. 按 seq 顺序 apply upsert/delete。
4. 对重复 seq 做幂等去重。
```

不能让用户直接从 tail changelog 推导完整表，否则启动状态会影响最终表。

## Python API 语义

对用户保持主 API 不变：

```python
zippy.subscribe("factors", callback=on_change)
zippy.subscribe_table("factors", callback=on_snapshot)
zippy.read_table("factors")
```

内部自动分派：

| API | 普通 append stream | KeyValue table |
| --- | --- | --- |
| `subscribe()` | 读原 stream rows | 读内部 changelog rows |
| `subscribe_table()` | 聚合新增 batch 回调 | watch snapshot version，回调完整表 |
| `read_table()` | 读 append table snapshot | 读 KeyValue snapshot |

`subscribe()` 的 callback 对 KeyValue 表收到的是变化 row，而不是完整表 row dump。为了兼容旧代码，
第一阶段可以把 changelog 的 internal columns 默认隐藏，只把 upsert payload 映射成原 row；
但 delete 需要明确处理。推荐做法：

- 默认 row callback 只回调 `upsert`，保持历史行为接近。
- 如果表支持 delete，用户需要显式选择包含 delete 事件的模式，或者 `Row` 增加 internal op 字段。
- `subscribe_table()` 始终是完整状态，适合不想处理 delete/changelog 的下游。

这个边界要在文档中写清楚：`subscribe()` 是变化事件，不是完整状态承诺。

## Rust 组件边界

建议新增或收敛到以下内部组件：

```rust
struct KeyValueTableWriter {
    latest_by: Vec<String>,
    schema: SchemaRef,
    current_version: u64,
    next_seq: u64,
    key_index: HashMap<KeyBytes, RowSlot>,
    changelog_writer: StreamTableMaterializer,
    snapshot_writer: StreamTableMaterializer,
}
```

核心职责：

- 计算 key bytes，保证 key 编码稳定。
- 对 batch 内同 key 归并最后 op。
- append changelog。
- 维护内存 key index / columnar state。
- materialize snapshot。
- 发布 snapshot descriptor metadata。

需要避免的边界：

- 不让普通 `StreamTableMaterializer` 同时承担 KeyValue 语义。
- 不让 row subscriber 自己识别 snapshot rewrite。
- 不让 reader 侧临时按 latest_by collapse 普通 append table，除非这是显式高级查询能力。

## 持久化与恢复

KeyValue 表恢复需要 checkpoint 思路：

```text
load latest persisted snapshot
        |
        v
read snapshot last_changelog_seq=S
        |
        v
replay changelog seq>S
        |
        v
rebuild key index and current snapshot
```

保留策略：

- snapshot 可以按版本保留最近若干个，用于读者 lease 和快速恢复。
- changelog 至少保留到最老可恢复 snapshot 的 `last_changelog_seq` 之后。
- compact 时必须先确认存在 snapshot 覆盖被删除 changelog 的状态。

不允许删除一段 changelog 后，只留下无法证明水位的 snapshot。否则恢复链路无法判断是否漏数据。

## 迁移方案

### 阶段 1：语义标注与内部路由

- 保留当前 snapshot replace 路径。
- 为 KeyValue descriptor 增加 changelog stream metadata。
- `subscribe()` 自动识别 KeyValue 表，并路由到 changelog。
- `subscribe_table()` 继续 watch snapshot descriptor generation。

验收标准：

- 同 key 多次更新时，`subscribe()` 每次都能收到变化事件。
- `subscribe_table()` 每次回调都是完整最新 snapshot。
- 普通 append stream 行为不变。

### 阶段 2：KeyValue writer 内聚

- 引入专用 `KeyValueTableWriter`。
- 把 changelog append、key index、snapshot publish 放到同一语义边界。
- snapshot metadata 必须包含 `snapshot_version` 和 `last_changelog_seq`。

验收标准：

- snapshot watermark 与 changelog seq 单调一致。
- 进程重启后能通过 snapshot + changelog 恢复当前状态。

### 阶段 3：性能优化

- snapshot materialization 支持节流。
- 内存 state 使用 columnar arena + key -> slot。
- active payload seqlock 落地后，再评估同 segment snapshot rewrite 或 row-slot overwrite。

验收标准：

- 高频小更新不再强制每批全量 snapshot replace。
- `subscribe()` 延迟由 changelog append 决定，不被 snapshot materialization 频率拖慢。

## 测试策略

Python API：

- KeyValue 表同 key 连续更新，`subscribe()` 收到每次 upsert。
- `subscribe_table()` 收到完整 snapshot，且同 key 只保留最新值。
- `start_from=-1` 对 KeyValue `subscribe()` 表示从 changelog tail 开始。
- 普通 append stream 的 `subscribe()` / `subscribe_table()` 测试保持不变。

Rust engines：

- batch 内同 key 多次更新只产生最终 changelog op。
- snapshot metadata 的 `last_changelog_seq` 等于已应用最大 seq。
- delete 后 snapshot 不再包含该 key。

恢复：

- 从 snapshot seq=S replay seq>S changelog 后得到当前状态。
- 删除旧 changelog 前必须存在覆盖水位的 snapshot。
- 重复 replay 同一 seq 不改变最终状态。

一致性：

- snapshot descriptor 发布前，读者仍看到旧完整 snapshot。
- snapshot descriptor 发布后，新读者看到新完整 snapshot。
- changelog 已提交但 snapshot 尚未 materialize 时，`subscribe()` 可见变化，`subscribe_table()` 仍只承诺已发布 snapshot。

## 风险与约束

- `subscribe()` 对 KeyValue 表的 delete 事件需要明确 API 表达，否则旧 row callback 无法自然表示删除。
- changelog stream 的命名和可见性要谨慎；内部 stream 不应污染普通 `list_streams()` 的用户视图，除非开启 debug。
- snapshot 节流会让 `subscribe()` 和 `subscribe_table()` 的实时性不同，需要在文档中明确。
- 多 writer KeyValue 表需要全局 seq 分配或 writer_epoch 合并规则；第一阶段应继续限定单 writer。
- schema evolution 必须同时约束 snapshot schema 和 changelog schema，不能只改其中之一。

## 决策建议

采用 `append changelog + materialized snapshot + watermark` 作为 KeyValue 表底层方向。

这条路线把三种语义分开：

- segment row offset 继续服务 append changelog。
- descriptor generation / snapshot version 服务完整表 watch。
- watermark 把 snapshot 和 changelog 串成可恢复的一致状态。

相比继续优化 active snapshot rewrite，这个方案更符合现有 segment 的 append-first 模型，也能直接解决
`subscribe()` 对 KeyValue 表无后续回调的问题。active payload seqlock 和 row-slot overwrite 可以作为
后续性能专项，而不是当前正确性修复的前置条件。
