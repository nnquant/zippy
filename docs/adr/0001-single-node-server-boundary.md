# ADR-0001：单机数据库服务器边界

- 日期：2026-07-13
- 状态：已接受
- 决策所有者：Zippy core maintainers

## 背景

现有 `master + gateway` 由实时数据总线逐步演进而来。它已经具备共享内存热路径、持久文件、
远端订阅和查询，但 metadata、查询生命周期、写入恢复与 durable subscription 缺少统一的
数据库边界。

Zippy 的产品定位是单机实时数据库服务器。用户需要统一入口、统一数据目录和可验证的恢复
协议，而不是由部署者拼装多个数据库角色。

## 决策

Zippy 重构为一个逻辑上的 `zippy-server`，内部按以下服务边界组合：

```text
FrontendService
CatalogService
StorageEngine
IngestService
SubscriptionService
QueryService
MaintenanceService
```

默认部署只要求 `zippy server run`。第一阶段除受监督的 maintenance worker 外，不要求拆分
常驻进程。各服务必须使用明确接口、独立资源预算和可观察的生命周期。

以下能力明确不在本轮范围内：

- Raft、选主和多副本。
- 跨节点查询、shuffle 和分布式事务。
- 自动故障转移和跨机器数据修复。
- 第一阶段的多表 ACID 写事务。

本地写入、逐因子回调及订阅继续使用 mmap/segment 热路径。Catalog、通用查询和维护任务不得
进入逐行或逐 tick 热路径。

## 结果

正向结果：

- 对外只有一个产品入口和一套恢复语义。
- 内部可以分别演进 Catalog、查询、写入和订阅，而不复制进程级状态。
- 单机资源治理可以统一处理 CPU、内存、IO 和临时磁盘。

代价与约束：

- 单进程核心服务仍共享故障域，必须使用资源预算和取消机制控制故障扩散。
- maintenance worker 与 server 之间需要稳定的本地管理协议。
- 旧 `master`、`gateway` 名称在迁移期只能作为适配层，不能继续承载新数据库职责。
- Phase 0 的 managed data root 仅在 Unix 上建立 owner-only 权限契约；Windows server 在完成等价
  ACL 实现前 fail closed。该限制不影响 Windows Flight 客户端。

## 替代方案

维持独立 master 和 gateway：部署简单但职责会继续漂移，无法形成单一 Catalog、查询和恢复边界。

直接设计分布式架构：会在单机正确性尚未闭合时引入共识、分片和跨节点恢复，成本与当前目标
不匹配。

## 重新评估条件

只有在单机格式、Catalog、查询、durable ingest/subscription 和恢复门槛全部完成，并出现明确的
容量或可用性需求后，才单独设计集群架构。集群设计不得隐式扩展本 ADR。
