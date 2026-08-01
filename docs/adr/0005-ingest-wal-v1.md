# ADR-0005：Producer fencing 与 ingest WAL v1

- 日期：2026-07-19
- 状态：已接受，Stage 7.3 Flight/native materializer 接线已实施
- 决策所有者：Zippy core maintainers

## 背景

Stage 7.1 统一了 Gateway 和 Flight 的进程内写入所有权，但成功回执只表示当前进程可见。连接在
响应返回前后断开时，客户端无法判断最后一批是否生效；旧连接也缺少可持久验证的 producer epoch，
重试可能重复写入或越过缺口。

持久写入需要同时解决四个问题：Catalog 颁发的 fencing、producer 内严格 sequence、跨重启稳定
log offset，以及能区分 memory/WAL/manifest 边界的回执。逐 batch 写 SQLite 会把 metadata actor
放入数据热路径，因此高频幂等状态必须由 per-table authority 和 ingest WAL 维护。

## 决策

每张 native table 第一版只允许一个 active producer connection。`AcquireProducer` 是类型化 Catalog
command：事务内把 table-wide `writer_epoch` 加一并重置该 producer 的 sequence watermark；相同
request ID 和相同 command 精确重放原响应，不允许历史重放回滚当前 epoch。producer ID 不得跨表
复用。连接重放同一次 acquire 时保留 epoch，但获得新的 boot-scoped connection lease 和 WAL segment。
acquire 状态机由 server-owned task 持有；调用方取消只丢弃响应，不取消已开始的 Catalog fencing 和
本地 epoch 安装。

一个 epoch 内的 `batch_sequence` 必须从 0 严格连续。已接受 sequence 只有 request ID 与 canonical
mutation fingerprint 同时一致时才返回原 receipt；同 sequence 不同内容、同 request ID 不同 sequence、
缺口和 stale epoch 均明确失败。幂等 receipt 只保留当前 epoch，数量有静态上限。

每张表的 WAL 位于调用方提供的 WAL root 下：

```text
<wal-root>/<table-id>/<producer-epoch:020>-<segment:020>.wal
```

WAL v1 使用 208-byte 固定 header，magic 为 `ZPWAL001`，包含 table/producer/epoch、schema version、
request/sequence、mutation kind、row count、前后 log offset、payload length 和 versioned SHA-256
mutation fingerprint。fingerprint 绑定排序后的 canonical schema digest，以及移除 schema message 后的
dictionary/record IPC payload，因此不受 Arrow metadata `HashMap` 遍历顺序影响。Arrow RecordBatch
payload 使用 Arrow IPC stream 编码；header 和 payload 分别由 CRC32C 校验，EOS 后 trailing bytes
明确拒绝。data record 后的 group commit marker 描述本次完整 prefix，marker 写入并完成 `sync_data`
后该 prefix 才达到 `Wal` durability。

回执边界固定如下：

| 请求目标 | 返回条件 | 崩溃语义 |
| --- | --- | --- |
| `Memory` | WAL data record 已追加且 mutation 已原子应用到进程内 sink | 未被 commit marker 覆盖的尾部可丢失 |
| `Wal` | 覆盖该 sequence 的 commit marker 已写入并 fsync | 可由 WAL 确定恢复 |
| `Persisted` | manifest snapshot 覆盖该 offset range | Stage 7.3 尚未接入，明确返回 `UnsupportedPlan` |

非空 mutation 按行分配从 0 开始的连续 log offset；offset 在 producer epoch 和 server restart 之间不
复用。零行 mutation 不分配 range，也不得改变 watermark。frame batch 作为一个原子 mutation，
key-value upsert/delete 作为 changelog event 分配 offset。key-value upsert 必须使用完整 table schema；
delete 只接受完整 table row 或按 table definition 顺序排列的 key-only tombstone schema。

资源预算固定为两层：默认全局最多 4,096 个 foreground/background active operations，每张表默认最多
1,024 个 pending durable ACK。任一预算在 mutation 应用前耗尽时返回可重试 `ResourceExhausted`，
不得出现“调用失败但 mutation 已生效”。

启动先有界扫描所有 table WAL，再开放 admission。只有 checksum-valid commit marker 覆盖的 record
可以恢复；Catalog persisted watermark 已覆盖的 prefix 不重复应用，后续 record 必须与它连续。
不完整或未提交的最终尾部被截断并 `sync_data`；checksum、fingerprint、schema、producer identity、
sequence 或 offset 不一致均 fail closed。恢复的 record/file/byte 数都有配置上限，禁止无界启动扫描。
正常 sink error 必须保证 mutation 未部分可见，authority 随即回滚最后一个 uncommitted WAL append；
sink panic、WAL rollback 失败或 commit-unknown 会把表置为 terminal，graceful shutdown 不提交其不确定
尾部。

## 结果

Stage 7.3 已将该 authority 接入 SQLite authoritative Catalog、native append materializer、TableActor
live snapshot、Flight v1 command 和 Python connector。`PutCommand` 现在携带 producer/connection
identity、期望 epoch 和 durability；`PutBatchResult` 返回确定性 request ID、producer epoch、offset
range、前后水位和实际 durability。Gateway 不再位于 durable write path，`sqlite_shadow` 也不能接受
native Flight ingest。真实 Server E2E 已验证 WAL ACK 后立即查询，以及 shutdown/restart/replay 后再次
查询相同数据。

正向结果：

- 响应丢失后可以按相同 epoch、sequence、request 和 payload 安全重试。
- 旧 producer connection 无法在新 epoch 生效后继续写入。
- `Wal` ACK 与进程内可见性分离，重启只重放确定提交的 prefix。
- log offset 不依赖 segment 地址，可作为后续 durable subscription 的稳定坐标。

代价与约束：

- `Memory` 路径仍执行 canonical fingerprint 和 WAL payload 两次 IPC 编码，并追加 WAL data record，
  因此比 Stage 7.1 的纯 materializer 路径增加 CPU 分配和 page-cache 写入；它不执行 fsync。
- 当前 commit 由 durable ACK、producer transition 或 shutdown 触发；更大的定时/批量 group commit
  coordinator 必须通过性能基准决定默认窗口，不能隐式扩大 ACK 延迟。
- 已被 manifest 覆盖的 WAL checkpoint 与文件回收尚未接入，运维不能自行删除 WAL。
- 当前恢复要求未 checkpoint 的 WAL 使用当前 table schema；schema evolution 必须先处理旧 WAL。
- Flight v1 已携带 producer ID、connection ID、期望 epoch 和 durable target；stream name 仍由服务端
  authoritative Catalog 解析为 table ID，客户端不能自报 table authority。
- 当前 append materializer 最多保留 1,024 个未持久化 live segment；达到上限会在 mutation 生效前返回
  可重试 `ResourceExhausted`。Stage 7.4 必须用 manifest checkpoint 推进 persisted prefix 并回收 WAL/
  live segment，运维仍不能自行删除文件。
- Unix file-backed SHM 在 `mmap` 后立即关闭原始 FD；mapping、路径和 owner 删除语义保持不变，避免
  多 epoch live snapshot 在线性增长时撞上常见的 1,024-FD 进程限制。

## 替代方案

逐 batch 更新 SQLite producer row：实现直接，但把 Catalog actor、SQLite transaction 和 fsync 放入
写入热路径，吞吐和尾延迟不可接受。

只使用客户端 request ID：无法阻止旧连接继续写，也不能检测 sequence 缺口。

只依赖 Parquet/manifest：每个小 batch 都持久化文件会造成严重写放大；批量持久化又无法提供低延迟
durable ACK。

## 重新评估条件

若真实 durable workload 表明 Arrow IPC 双重编码、每 ACK fsync 或 WAL 恢复上限成为主要瓶颈，应基于
p50/p99/p999、group size 和恢复时间证据调整编码复用、group commit 窗口和 checkpoint 策略。不得
降低 producer fencing、sequence 连续性、稳定 offset 或 ACK 边界来换取表面性能。
