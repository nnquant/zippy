# 单主干收敛第五批：原生 frame 与管理协议统一

日期：2026-09-22。本批实质推进 F09/F10/F12，原 34 项仍为 **20 项关闭、14 项未完成**。
frame 的原生写入与恢复已经补齐，但 key-value、统一订阅和完整管理闭环仍未完成，不能关闭
覆盖这些能力的整体审计项。前一批见[Persisted 记录](convergence-persisted-2026-09-22.md)。

## 原生 frame

- 将 append materializer 扩展为唯一的 `NativeTableMaterializerSink`，frame 共用 ingest/WAL、
  producer fencing、TableActor、checkpoint 和回执归档；旧名称直接替换，没有保留 alias。
- frame 每个非空输入批次完整写入一个 segment，完成后立即封段。读取实际 committed row count，
  保留部分填充段的边界；段预算、回收、writer 交接均支持 frame。
- 超过单段行数在应用前拒绝；段预算不足可重试。TableActor 只接受 sealed frame，阻止持久化
  水位落在帧中间；Catalog manifest 接纳 frame 的追加历史与 compaction 语义。
- 支持 Memory/WAL/Persisted。WAL 恢复重建原批次边界，checkpoint 后恢复 manifest，原请求
  重试返回已确认回执；空批次不分配 offset，持有快照的读取者保留完整旧帧。
- Flight `PutMutation::Frame` 进入同一个 backend，表类型不匹配在 acquire producer 之前拒绝。
  capability 公布 `ingest.frame.v1` 与整帧行数上限。
- Python `Client.create_frame_table`、`Client.write_frame` 完成真实链路。多 chunk Table 合并成
  一个 Arrow batch；超行数/字节限制时发送前失败，不使用 append 的分批策略。

## 管理入口收敛

Flight 使用唯一 `zippy.create_table.v1` action 和 `CreateTableRequest.definition` oneof，明确
选择 append/frame。删除之前的 append 专用 wire/action/capability；Python 的
`create_append_table/create_frame_table` 是构造不同表定义的公开方法，共用同一个管理请求实现。
namespace 创建保留独立语义。客户端和服务端须一起升级，不提供旧协议自动接收分支。

## 新发现 F38：Catalog 并发打开时的 WAL 模式锁冲突

首次全仓回归中，8 个 Catalog 并发打开的测试在 `PRAGMA journal_mode = WAL` 偶发立即返回
`database is locked`。现在仅对 BUSY/LOCKED 重试该完整语句，使用配置的 busy timeout 作为总等待
预算；每次 SQLite 等待缩减到剩余时间，成功后恢复原 timeout，其他错误立即返回。

锁定数据库的确定性测试验证超时有界、返回可重试错误、释放锁后能够设置 WAL，并保持配置的
busy timeout。原并发打开/迁移测试和完整工作区复验通过。F38 是原审计之外的新修复，不计入 34 项。

## 验证

- Rust workspace：1714 passed、10 ignored；包括新增 frame 恢复与 Catalog 锁测试。
- workspace/all-targets/all-features Clippy、Rust fmt、diff whitespace 检查通过。
- 客户端完整套件：93 passed，覆盖率 81.22%；mypy 源码、测试与验收脚本共 25 文件通过；
  本轮 Python 文件 Black/Ruff 通过。
- proto 单源再生检查通过；旧 append 专用管理 request/action 和旧 materializer 名称无源码引用。
- 原生 frame 回归覆盖：空目录在线创建、含 null 的完整帧、超大帧拒绝、段预算拒绝、WAL 重放
  帧边界、半帧持久化拒绝、读取者保护、空批次、manifest 恢复和精确 Persisted 回执重试。
- 真实 Python ↔ Rust：同一空目录创建 append/frame，两次完整 frame 写入后查询 [10, null, 12]，
  正常停服并重启恢复；同时复验 append 的跨 writer WAL/Persisted 场景，Arrow 23.0.1。
- 日志与哈希见[结构化证据](perf_reports/evidence/2026-09-22-frame/summary.json)。首次全仓失败
  保留在 `/tmp/zippy-frame-workspace.log`；修复后的完整结果在 `/tmp/zippy-frame-workspace-final.log`。

本批未重跑主 Python 包全套与全仓 mypy；上一批全仓类型错误仍是开放债务。没有提交、推送、部署
或修改生产数据目录。

## 仍需完成

原生 key-value 的增量状态、delete、持久化与恢复、快照游标尚未接通；Flight 实时订阅、本地计算
输出接入、alter/drop、唯一 Catalog/数据格式切换及旧路径删除仍开放。frame 原生写入不代表
远端 frame 订阅已交付。剩余编号：F07–F16、F22、F26、F28、F29。
