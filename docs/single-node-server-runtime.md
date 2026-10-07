# 单节点服务运行时

2026-09-22 更新。当前原生使用方式见[公开示例指南](../examples/examples_guide.md)，
能力和未完成项见[当前能力](current-capabilities-2026-09-21.md)。本页描述当前运行顺序，
不再以历史迁移阶段作为操作指南。

## 启动与数据权威

统一入口为 `zippy server run --config /path/to/zippy.toml`。Linux 默认启动受管理的后台进程；
systemd、RSPM、容器等外部进程管理器应添加 `--foreground`，直接持有服务进程。
使用 `zippy server status` 和 `zippy server stop` 查询、停止同一配置的实例。

默认启动即使用 SQLite、native storage 和 native query，Flight 默认监听 `127.0.0.1:17667`。
`catalog.backend`、`server.native_storage_enabled`、`query.native_enabled` 及其环境变量已删除，
旧输入明确拒绝。Flight 地址独立于 Master 地址；配置 `flight.enabled=false` 只关闭监听器，
不跳过原生恢复。写入仍需启用 `flight.ingest` 并配置独立的 read/writer/admin 凭据。
完整配置见[Flight 服务](flight-query-service.md)。Gateway 服务及配置已删除，
`[gateway]` 和 `ZIPPY_GATEWAY*` 输入明确拒绝。

`sqlite_shadow`、`catalog.shadow` 和 `ZIPPY_CATALOG_SHADOW_*` 已删除，使用时明确报错。
Server 不再启动旧目录到 SQLite 的导入、命令影子同步或 Gateway 持久文件对照线程，
也不再提供 Gateway 到原生查询引擎的迁移适配器。Catalog 当前布局为 v11，
旧 v1–v10 拒绝且不改写；只接受原生 v1 请求账本，不保留迁移表或导入提交通道。

## 原生就绪边界

服务先取得数据目录所有权，再启动 SQLite 与原生存储。原生存储必须完成以下步骤：

1. 校验当前目录和 Catalog 布局，打开 SQLite，并进行恢复前文件核对。
2. 重放已持久化的 Catalog commit intent，再核对物理文件和 Catalog。
3. 在稳定目录 revision 下校验当前快照的路径、大小、checksum、footer 和行数。
4. 为每张有效表创建 TableActor，恢复对应的 schema、writer epoch 和已提交 offset。
5. 启动原生 ingest 并恢复 WAL，随后绑定控制和 Flight 监听器，最后开放请求。

三类表 append/frame/key-value 都走这条恢复路径。查询从 TableActor 捕获边界，再固定对应的
Catalog manifest；不再存在“影子表仅读持久文件、跳过 actor”的分支。多表边界按表 ID 排序，
writer epoch、schema、快照版本发生并发变化时重试或明确失败；不能混合两代状态。

内存可见、WAL 已同步、Parquet/manifest 已发布是三个不同承诺。默认 `wal` 确认恢复能力；
`persisted` 还等待 manifest 覆盖与回执归档。后台 checkpoint 不替代请求的持久性确认。

固定快照持有 Catalog pin 和 live 边界，查询执行由 DataFusion 完成。查询 CPU、内存、并发、
结果缓冲与租约资源保持有界，客户端取消、超时和会话关闭都会释放对应所有权。
Flight 原生日志负责三类表订阅、游标及持久 ACK；本地计算通过公开 Client 写回原生表。

## 生命周期与失败

后台启动器只在原生服务发布带 nonce 的就绪记录并完成协议探测后报告成功。
Server QueryEngineAdapter、Gateway collect facade 和旧组合层已删除，查询运行句柄只保留原生引擎。
LegacyCatalogAdapter 也已删除，Catalog 读取句柄只来自原生 SQLite；原生目录健康为 ready，
关闭 Flight 的 control_only 模式仍持有原生目录与查询引擎，但不宣称远端查询监听器就绪。
Gateway listener、旧 memory ingest 及其健康字段已删除。
当前 ready schema 为 v11；starting/stopping/failed 与失败结果记录为 v1。
不接受旧 ready 记录，不用默认值补齐缺失的就绪事实。四个原生恢复条件无条件参与就绪判断，
不再提供 native_storage_configured 旁路。

`status` 校验所有权锁、启动 nonce 和 Linux 进程启动身份，再执行有界探测。
`stop` 使用 pidfd 固定目标进程代际后发送 SIGTERM。只有显式 `--force` 才会在超时后发送
SIGKILL；清理状态文件前重新检查锁和 nonce，防止删除替代进程的状态。

服务持续检查动态健康状态。原生存储的第一个终止错误先关闭请求接入，再把状态原子更新为
failed 并开始清理；清理失败附加到原始错误上，不覆盖原始 ErrorCode。

停服共用一个单调时钟截止时间：关闭新请求，停止 Flight，排空进程租约并停止控制监听器，最后停止原生存储与 SQLite。
各阶段只使用剩余预算。控制监听线程的完成消息与 join 都可在超时后重试，未完成时不缓存最终停服结果。尚未退出的 worker 保留数据目录所有权，防止另一进程抢先打开。
删除影子任务后，停服不再等待影子发布者封口、对照队列或最终旧目录同步。

## 控制面与剩余收敛

ServerRuntime 直接持有原生服务和 Master 控制监听器。ProcessRuntimeRegistry 与进程租约共享锁及
启动身份；控制协议只保留配置发现、进程注册/心跳/注销，以及 Process/Shutdown Watch。
旧 stream/source/engine 表目录、文件发布、段描述符、读者租约和数据 watch 已删除。
控制面无法再建立第二份表目录或改写持久文件，表管理统一走原生 Flight。
控制 listener 不接受未认证的测试命令；请求按 typed 协议解析。进程注册的 `app` 最多
256 字节，boot-scoped 租约记录最多 4096 条。满额时只回收已过期记录；若全为活跃租约，
注册返回结构化 `RESOURCE_EXHAUSTED`，不会逐出活跃进程。

Master 不读写持久 registry，也不运行旧 Parquet compaction worker。
含 catalog/legacy-master-registry.json 的目录明确拒绝且保留原字节，不自动迁移。
旧 [table]、master.token、ZIPPY_TABLE* 和 ZIPPY_MASTER_TOKEN 明确拒绝；Flight 角色凭据保持独立。
materializer 旧 Parquet worker、独立分区文件与 sidecar 已删除，原生 ingest 复用的共享内存和原子
frame 写入保留，持久性统一由 WAL/TableActor/PersistController/SQLite 提供。

Rust 内部旧 source/publisher、KV/shard materializer 与专属基准已清理；
原生产品能力和恢复测试已补齐。
跨平台验证状态见当前能力页；Linux 验收不能替代 Windows/macOS 实机结果。
