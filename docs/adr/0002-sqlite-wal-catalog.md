# ADR-0002：SQLite WAL 作为 Catalog

- 日期：2026-07-13
- 状态：已接受
- 决策所有者：Zippy core maintainers

## 背景

当前 JSON registry 依赖全量快照和进程内聚合状态。随着表、schema、snapshot 和 data file 数量
增长，全量克隆、崩溃窗口、增量查询和 migration 都会成为系统性风险。

Catalog mutation 不在逐行热路径。它主要发生在建表、schema 变更、文件提交、持久 consumer
offset 和 maintenance job 状态变化时，适合使用成熟的单机事务数据库。

## 决策

Catalog authority 使用 SQLite，启用 WAL、`synchronous=FULL`、foreign key、busy timeout 和版本化
migration。启动只执行有界 `quick_check`；完整 `integrity_check` 由 maintenance job 执行，避免大型
Catalog 的恢复时间不可控。Rust driver 使用 `rusqlite`。

Catalog 只支持 SQLite 官方保证的本机文件系统语义，不允许把 `catalog.db` 放在 NFS 或其他可能
破坏文件锁、共享内存或 fsync 语义的网络文件系统上。

所有写入通过一个专用 OS actor thread 串行执行：

```text
bounded command channel
    -> typed CatalogCommand
        -> SQLite transaction
            -> typed response
```

async runtime 线程不得直接执行 SQLite IO。普通查询的短生命周期 snapshot lease 保存在 server
内存中，不逐查询写 Catalog。只有需要跨进程或跨重启保留的 lease 才持久化。

Catalog 只保存持久事实：table、schema、snapshot、data file、producer checkpoint、durable consumer
offset、maintenance job 和受控幂等结果。heartbeat、active query、连接、实时 descriptor 和 metrics
属于 `RuntimeRegistry`，server 重启后全部失效。

SQLite 成为 authority 前使用同一 `CatalogCommand` 驱动 legacy primary 和 SQLite shadow，并持续
比较结果。禁止业务代码分别拼装两套写操作。

Shadow 阶段的提交和失败规则固定如下：

1. Legacy primary 先执行，并且它的响应仍是客户端唯一权威响应。
2. Primary 失败时不执行 shadow；primary 成功后才提交相同 request ID 和 payload hash 到 SQLite。
3. Shadow 失败不回滚 primary，也不能让客户端误以为业务操作失败后重试；服务进入 degraded，记录
   待重放命令和 mismatch。
4. SQLite 对相同 request ID 和 payload hash 幂等返回原结果；相同 ID 配不同 payload 明确失败。
5. 进程在 primary 成功、shadow 提交前崩溃时，由全量 reconciliation 和可重放命令补齐，不把
   shadow 当作恢复 authority。
6. 切换 authority 前必须完成一次全量 table/schema/file 对账，并在规定观察窗口内保持增量
   mismatch 为 0。
7. 切换后 SQLite 成为唯一写 authority；legacy JSON 只通过受控 exporter 生成，不继续反向双写。

## 结果

正向结果：

- metadata mutation 获得事务、WAL、索引和 schema migration。
- status 查询不再反序列化或克隆完整 JSON registry。
- snapshot/file 可见性可在单个事务中原子提交。

代价与约束：

- Catalog actor 的 channel 必须有界并暴露排队时间和拒绝指标。
- 不得把逐 batch、逐行或普通 query lease 写入 SQLite 热路径。
- backup 必须使用 SQLite backup API 或一致 checkpoint，不能复制正在变化的三个数据库文件。
- maintenance worker 只能通过本地 RPC 使用 CatalogService，不能自行打开 `catalog.db`。

## 替代方案

继续扩展 JSON registry：实现简单，但无法可靠解决增量写、索引、migration 和原子多对象 mutation。

自研 metadata WAL：能够定制格式，但会重复实现事务恢复、checkpoint、校验和 migration，风险高于
收益。

嵌入式 LSM 数据库：适合大规模 key-value，但 Catalog 需要关系约束、多表事务和运维可检查性，
SQLite 更匹配当前访问模式。

## 重新评估条件

只有性能证据表明正确分层后的 Catalog actor 无法满足 metadata SLA，且瓶颈不能通过批处理、索引
或 checkpoint 解决时，才评估替代存储。重新评估不得以逐行写入误入 Catalog 为前提。
