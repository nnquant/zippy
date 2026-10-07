# 单主干收敛第二十批：原生持久消费服务与原子确认

日期：2026-09-22。推进 F11，原 34 项仍为 **25/34 已关闭、9 项开放**。
本批把上一批的 Catalog 位点接入运行时订阅服务；公开 Flight ACK、旧 callback 和本地计算迁移仍未完成。

## 交付与确认共用原生日志

`NativeStorageRuntime.consumer_logs()` 返回共享的原生消费服务。连接由已认证会话的查询 capability
持有，使用现有 NativeLogStream、查询期限、取消信号、内存账本与快照所有权。消费位置仅来自
Catalog；请求必须绑定不可变 table ID，不能额外指定另一条起始位点覆盖已确认进度。

同一连接 ID、同一表的个人/组 authority 不能同时打开两个连接；克隆服务不会另建组注册表。
打开中的预留也有 1,024 个连接上限，之后仍需通过已有查询资源限制。这是单活动成员模式，
不是分区负载均衡或动态再平衡。

每次只允许一个未确认交付。append 按结果片段交付，frame 与 KV 必须收齐一个完整原子单元。
交付持有非 Clone 的 QueryResultBatch，保留每个片段的额度与快照所有权，不拼接复制 Arrow 数组，
也不把裸 RecordBatch 克隆当成已计费结果。调用方持有交付时，即便关闭连接，额度仍保留到释放数据。

KV 校验事件序号、mutation version、批次末尾、操作、表身份及完整水位；恢复时读取前一条事件，
验证 Catalog 位点正好位于 mutation 末尾，不能从半批继续。当前 KV 采用每行一个计费片段来保持
所有权和边界简单明确；这有额外开销，尚未完成吞吐验收，不能作为高吞吐订阅迁移的完成证据。
完整单元超过查询内存预算时失败，不能绕过预算聚合。

## ACK 先持久化，再推进位点

确认必须精确匹配本连接刚交付的序号和结束位点。未交付位置、半帧、半个 KV mutation、空位点
及超前位点均被拒绝。重复最后一个已成功确认返回成功；旧确认不会把下一次交付的位点推进。

新增逐表 checkpoint guard：仅刷对应表，持有 schema 切换 gate 和 ingest 生命周期 admission，
直到 Catalog 的 schema/CAS/水位校验及位点事务完成。普通写入仍可继续，schema/drop 不能插入
持久化与消费位点提交之间，服务关闭必须等待该操作释放。

已接纳 ACK 在独立任务中完成。取消等待保留原 request ID 和任务；关闭连接也不会提前释放该
ACK 所持的组所有权。已取消查询不能发起新 ACK，已接纳的提交则继续收尾。读交付的等待被取消时，
已组装的片段留在连接内；关闭连接后仍以 Catalog 为准重放。

## 验证与剩余边界

新增运行时回归对 append/frame/KV 逐一验证：Memory 写入、完整交付、错序/空/部分/超前 ACK 拒绝、
单成员组互斥、无 ACK 断连重放、ACK 触发真实持久化、服务重启后跳过已确认前沿、查询取消后拒绝
新 ACK、损坏 KV 半批位点拒绝、取消 ACK 等待并关闭连接后继续提交、在途组占有、结果额度保留与
释放、schema gate 排斥表切换、停服等待 checkpoint guard。测试使用真实 Catalog、WAL、TableActor
和原生日志，不用仅更新水位的 mock 替代数据恢复。

完整门禁结果和文件指纹见[结构化证据](perf_reports/evidence/2026-09-22-consumer-service/summary.json)。
本批 Rust workspace 为 1,760 passed、10 ignored；重建扩展后的 Python 为 730 passed、
1 skipped。Clippy、Black、Ruff、Rustfmt、协议再生成和配置范围 mypy 均通过。全仓 `mypy .`
仍为 991 errors in 12 files（85 files checked），F29 保持开放，没有新增忽略或缩小全仓检查范围。
没有新增 Windows/macOS、Arrow 版本矩阵或干净 sdist/wheel 验收。

本批服务仅接入内部 runtime 能力，没有发布 Flight durable ACK 或 Python 客户端确认协议；
也没有声称已实现 SubscriptionService 的全部视图、过滤、节流和 snapshot-then-tail 组合。
现有 Client.open_log 的显式游标接口继续有效，旧 Gateway/SHM callback 在完成迁移前保留。
下一步把此服务接入同一 Flight 会话和实时订阅协议，完成网络 ACK 丢失与重连验收，再迁移回调和计算。
