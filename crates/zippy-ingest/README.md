# zippy-ingest

本 crate 负责唯一原生写入路径：表运行时身份、producer fencing、串行 mutation admission、
WAL、回执、checkpoint 和恢复。Server 从 SQLite 的同一 Catalog revision 构建
TableRuntimeRegistry、TableActorRegistry、NativeTableMaterializer 与 NativeIngestRuntime。
旧 memory authority、Master 数据适配器与 Gateway 写入路径已经删除。

## 表 actor 与持久提交

`TableActorRegistry::start` 必须接收 `PersistControllerHandle`。每张表只有一个串行 actor，
默认邮箱容量 64，共享 outstanding command 预算 4096。每表仅允许一个进行中的持久请求。
运行时 owner 独占关闭 admission 和 drain 的权限；普通句柄只提交工作，不接管生命周期。
调用者丢弃等待句柄不会撤销已接收命令。

actor 在存储提交前检查表 ID、schema、writer epoch、当前快照与连续逻辑 offset。它在自己的
执行顺序内验证回执并原子推进快照。文件写入和 Catalog 提交期间仍可读取 actor 状态。
请求级拒绝、可重试背压与致命错误明确区分：确定的请求拒绝不破坏后续写入；写入失败、
提交结果不明、无效回执或控制器生命周期故障关闭全局 admission，并在有界停服时返回。

Catalog 中持久幂等记录是跨重启请求身份的权威。精确历史请求返回原回执，不回退较新的快照；
同 ID、不同指纹返回 Conflict。Advance 与 HistoricalReplay 显式传给 storage，历史重放不能
再次发布文件。相关实现见 `zippy-storage` 的 Catalog 提交协调器与 PersistController。

## 写入与恢复

NativeIngestRuntime 为每张表分配 producer epoch，将 writer 绑定到当前 boot 的连接租约，
并跨 epoch 分配连续的零基逻辑 offset。精确 epoch/sequence/request/fingerprint 重试不重复
应用 mutation；缺口、旧 epoch 与冲突重试返回结构化错误。producer 获取属于服务端拥有的
工作，调用者取消不能造成 Catalog fencing 与本地权限失配。

新 mutation 在可见前进入版本化 WAL。Memory 不承诺 fsync；Wal 等待带 checksum 的 commit
marker 和 sync_data。恢复只重放 Catalog persisted watermark 之后的完整已提交记录，截断
不完整的未提交尾部，并对 checksum、指纹、身份、sequence 或 offset 损坏关闭写入。
普通原子 sink 拒绝回滚当前未提交 append；panic 或回滚失败使表终止，不提交不确定的尾部。

Persisted 等待 manifest 覆盖目标 watermark，且精确回执已持久归档。已接收的 ACK 工作不因
调用者取消而丢失，也参与统一 shutdown drain。前台操作和后台 ACK 默认共享 4096 个预算，
每表另有默认 1024 个待持久确认的上限。

checkpoint 封存已接收前缀、提交 WAL、归档回执并发布 Parquet manifest，再保存恢复位点，
回收被覆盖的 WAL/SHM。持久权威使用当前 writer epoch，source lineage 保留历史段的 epoch。
读者 pin 可推迟旧段回收；不能凭内存可见性提前宣称持久完成。

## 三类表

- append：按逻辑 offset 追加，原生物化器保留当前未持久前缀。
- frame：非空业务批次对应完整 sealed segment，超限在 mutation 前拒绝；checkpoint 不拆帧，
  WAL 重放恢复原始批次边界。
- key-value：日志保留每条 upsert/delete 事件，原子快照按键合并状态并携带精确游标。
  delete 接受完整逻辑行或按原顺序排列的精确主键 tombstone schema。

三类表均已接入 Flight 写入、固定快照、持续日志、订阅 ACK、schema/drop 和重启恢复。
公开能力与最新验收见[当前能力表](../../docs/current-capabilities-2026-09-21.md)。
