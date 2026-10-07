# 单主干收敛第十五批：原生在线删除与运行时退出

日期：2026-09-22。原 34 项累计关闭 **24/34**，剩余 **10 项**。本批关闭 F10，推进 F09；
尚未完成整个项目的单主干切换。前置目录事务见[第十四批记录](convergence-drop-foundation-2026-09-22.md)。

## F10 关闭依据

真实 Rust Server 与 Python Client 从空目录开始，通过公开 Flight API 创建 append、frame、key-value，
写入并取得 Persisted 回执；同目录重启后查询原数据，再经公开 API 删除。删除返回前完成运行时退出，
后续同进程和重启后的相同请求均返回原回执，schema lookup 返回 NOT_FOUND。没有测试 seed、离线 SQL
或私有 Catalog 命令参与这条验收路径。

`scripts/check_native_client.py` 同时验收只读会话不能绕过客户端权限检查执行 drop、快照 CAS 冲突
不删除表、持续日志以 NOT_FOUND 结束；原先的三类数据恢复、状态订阅重建和本地只读会话验收仍保留。
Arrow 23.0.1 与 25.0.1 分别完成六次真实服务启动。

## 删除只保留一条原生执行路径

新增 `zippy.drop_table.v1` action、`catalog.drop.v1` 能力声明与 `Client.drop_table()`。
调用方明确提供 table ID、观察到的 snapshot version、稳定 request ID；Flight 仅校验协议和管理员身份，
随后调用 Server 管理服务。结果包含删除对象身份、Catalog revision；schema_version=0 表示删除结果。

Server 管理任务先取得停服排空许可和管理预算锁，在独立任务中依次执行：

1. 隔离该表的 ingest admission；若持久回执或 checkpoint 尚未结束，返回可重试错误。
2. 暂停 actor 新命令，以队列 barrier 等待此前命令处理；在途 persistence 不允许进入删除。
3. 提交 Catalog 的带 CAS 幂等逻辑删除；失败则两个 guard 自动恢复原 admission。
4. 移除 materializer、退休旧 writer 与 actor，等待 actor 退出；持续日志不再继续交付其历史 window。
5. 回收运行时表数量及逐表元数据额度。RPC 调用方取消等待不会取消已接纳的管理任务。

原生内部测试对每种表执行 64 次不同名称的创建/删除，在 max_tables=1 和有界 metadata 预算下验证额度
回收。旧 actor/writer 返回 NOT_FOUND，其他表和全局运行时仍健康。已经固定的 live span 保有自己的
mapping，删除后仍可读取；固定历史窗口不等于持续订阅，其已有快照语义继续保留。

历史 create 请求的回执可以重放，但不会重新安装后来已删除的表。当前 create preflight 仍会先检查
可用预算，因此不宣称所有历史 create 重试在任意容量状态下均无条件成功。

## F45：PyArrow 丢失结构化错误

新增真实冲突用例发现，服务端已有 grpc-status-details-bin 与 Zippy envelope，但缺少 Arrow 的状态头。
PyArrow 因而将 ABORTED 变成无 extra_info 的 FlightInternalError，客户端误报连接错误；部分其他状态
还可能绕过 FlightError 变成 ArrowInvalid。此前手工注入 extra_info 的单元测试无法发现这一跨语言问题。

根据 [Arrow 23.0.1 官方状态解码实现](https://github.com/apache/arrow/blob/apache-arrow-23.0.1/cpp/src/arrow/flight/transport/grpc/util_internal.cc)，
FromGrpcContext 只有看到 x-arrow-status 才重建二进制细节。服务端统一补充 x-arrow-status=5
（Arrow IOError）和原始消息头，保留原 gRPC code 与同一二进制 envelope。客户端继续按 envelope
解码 code/retryable/身份，不解析错误文本，也不新增按版本分叉。

真实 Arrow 23/25 验收现能准确获得 CONFLICT、NOT_FOUND；Rust 单测验证新增状态头与原 envelope 的
往返一致性。F45 是本轮新发现并修复的问题，不改变原 34 项分母。

## 验证与剩余边界

Rust workspace 1,751 passed、10 ignored；最终错误头修正后 protocol 44 passed。Clippy、Rustfmt、
Black、Ruff 和协议再生检查通过。重建扩展后根 pytest 共 726 passed、1 skipped（包含客户端）；
客户端独立运行 116 passed，覆盖率 81.54%。

完整结果、日志摘要和源码指纹见
[结构化证据](perf_reports/evidence/2026-09-22-runtime-drop/summary.json)。
全仓 `mypy .` 实际仍有 **991 errors in 12 files，checked 85 source files**；当前根配置的 14 个文件
及客户端 26 个文件通过。F29 保持开放，配置门禁通过不等于全仓类型问题已清零。

- F09 仍开放：alter/schema、同名新 ID 重建尚未完成。现有 SQLite 唯一约束仍占用已删名称；
  不通过篡改 tombstone 名称或删除历史身份规避该问题。
- 删除是逻辑删除；历史文件继续受 pin/lease 保护，现有 GC 负责文件回收。
  本轮没有实现 WAL/receipt 目录物理回收，也未验证每一个 RPC 取消时点。
- 旧 callback、统一订阅服务和 durable ACK、本地计算的原生注册/输出、Gateway/旧目录退出仍待完成。
- 仅有 Linux x86_64、CPython 3.11 与上述两版 Arrow 的真实结果；不新增跨平台兼容性承诺。

剩余原审计项：F07、F08、F09、F11、F13、F14、F15、F22、F28、F29。
