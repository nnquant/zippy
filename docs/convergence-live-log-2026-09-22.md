# 单主干收敛第九批：持续原生日志与固定资源身份

日期：2026-09-22。本批继续推进 F11/F16。原 34 项累计仍为 **21/34**，剩余 **13 项**；
Flight DoExchange、durable consumer ACK 和客户端恢复链路仍未完成。
前一批见[有序事件窗口](convergence-event-log-2026-09-22.md)。

## 持续读取

Rust 服务端新增 `NativeQueryEngine::subscribe_log`，沿用 `NativeLogRequest` 的起始 offset、
表引用、内存预算、batch 行数和 deadline。它返回 `NativeLogSubscription`，不是新的业务状态缓存。

- 先把表名解析为唯一 table ID，再注册 actor 通知并重新固定真正的起始窗口，消除解析与订阅
  之间的漏唤醒。后续始终按该 table ID 读，避免同名新表混入旧订阅。
- 从指定 offset 回放，读完后按前一窗口最高 offset + 1 继续。latest 模式固定开流时水位，
  只交付之后的数据。每个片段携带其固定窗口的 watermark。
- 整个订阅保持同一个 query ID 和 generation。更新窗口不会不断增加查询注册记录，
  owner/admin 取消、deadline 和停服都可定位同一订阅，包括空闲等待时。
- 无数据时等待合并的 actor 通知，不重复扫描忙轮询、不创建后台结果队列。checkpoint、
  writer 交接的通知可以触发一次空窗口，不会重复交付已消费事件。
- 更新窗口前必须排空物理 reader 和 retained plan；新的快照必须属于同一查询的真实内存租约，
  表身份及 schema 不变，Catalog revision、snapshot version 和日志水位不得回退。
- schema 改变、游标无效或读取失败都明确终止，未实现隐式重置状态、跳过缺口或协议降级。

输出仍是有序事件片段：frame 带完整原帧范围，key-value 带 v2 mutation 边界。
原子帧组装、持久消费 ACK、group membership 及 SnapshotThenTail 产品流程尚未接入；
不能把底层片段流直接当成已完成的 `SubscriptionService` 产品接口。

## 资源与快照租约

持续查询只保留一个物理读取窗口。旧窗口读完、或 latest 的起始窗口为空时，释放旧 Catalog pin；
空闲订阅保留查询身份、游标和 schema，不长期保留不再读取的物理文件。
下游持有的旧结果拥有独立内存计费，刷新和取消不会提前释放其预留。

原快照 lease 覆盖 SQLite 读取、metadata 构造与 schema 解码峰值。若一直把这个峰值当作窗口
驻留量，多订阅会在默认 512 MiB 下提前耗尽预算。本批对事件窗口改为：

1. 读取期间仍预留完整峰值。
2. Catalog 在最终 owned snapshot 上，用不分配副本的序列化计数、保守展开系数，以及显式
   table/file/schema sidecar 预留计算驻留上界；这不是 RSS 或精确堆内存测量。
3. 服务端先取得新的已计费 lease，再替换旧 lease。只有 issuer、query incarnation 匹配、
   容量充足且快照尚未共享时允许替换；已经派生 table/file capability 的快照不能更换证明。
4. 丢弃旧 lease 和旧 snapshot 的回调在账本锁之外执行，避免释放回调死锁。

没有提高默认内存上限。普通分析查询的快照契约保持固定，刷新能力只由持续日志调用。

## 验证与待办

最终命令结果及源码哈希记录在
[结构化证据](perf_reports/evidence/2026-09-22-live-log/summary.json)。

新增验收覆盖三类表的历史与 latest 双订阅、多次 checkpoint 后的精确 offset、重复键事件、
frame 范围、空闲 deadline/取消、停服、旧结果持有期间计费、释放后归零。
另有连续 16 次窗口更新仍只有一个查询注册、active reader 拒绝刷新、外来租约和取消后的
安装拒绝，以及 Catalog lease 的 issuer/query/容量/共享约束测试。

完整回归还复现了旧 ZMQ 测试的无界等待：固定睡眠不能证明 SUB 已连通，早期 PUB 消息丢失后
`join()` 不会返回。测试夹具改用 XPUB 的真实订阅通知，并为 join 加入超时、stop 清理和失败断言。
该修复没有删断言或跳过用例，也未改变产品 ZMQ 协议。首次完整回归中已确认的卡住测试被终止，
最终完整回归重新运行并通过；单独的 ZMQ 13 项回归已通过。
最终开流错误归因修补后，三类持续流和快照刷新专项再次通过，workspace 全目标全特性 Clippy
（禁止 warning）、rustfmt 和 diff 检查通过。开流失败保留 Failed 原因，不误报为取消。

仍待 Flight wire、原子交付层、durable ACK、客户端订阅与断线恢复验收。这些未完成前不删除
Gateway。表管理、本地计算、唯一目录权威、Python 合并与类型债务、平台矩阵继续按原清单推进。
