# 单主干收敛第三批实施记录（2026-09-22）

后续进展见[第四批记录](convergence-persisted-2026-09-22.md)；以下计数和测量保留第三批时点。

状态：已补齐原生 append 在线建表与运行时注册，统一控制错误契约；**项目尚未全部收敛**。
frame、key-value、远端实时订阅和本地计算仍保留，接替实现未齐全前不删除其有效旧路径。

## 本批实现

1. `NativeTableAdmin` 接收明确的建 namespace / append table 命令，提交 SQLite Catalog 后，
   安装 TableActor、materializer、NativeIngestAuthority，再返回成功。三处表集合均支持在线注册。
   管理员 Flight DoAction 与 `zippy_client.Client.create_namespace/create_append_table` 接通同一服务。
2. 管理操作在 Catalog 提交前取得 ingest 生命周期许可。被接受的安装在客户端取消等待后继续；
   停服等待这些操作排空，新 actor 由原 supervisor 启动、监控并加入 drain。请求幂等依赖明确的
   request ID 和不可变对象 ID，同请求内容变化返回冲突，重启后可重放创建请求。
3. 在线创建复用启动时的最大表数和元数据预算；超过限额在 Catalog 提交前拒绝。没有表的启动
   状态也启用 checkpoint maintenance，避免动态创建的第一张表永远不触发后台持久化。
4. Flight DoPut 增加显式 namespace，贯穿 Client、FlightWriter 和服务端 Catalog lookup。
   删除从 put ID 推导缺失 producer/connection ID 的兼容分支。非默认 namespace 的写入、查询、
   重启恢复已通过真实 RPC 验证。
5. F25 的控制面补齐：删除可选错误码和 uncoded-error IO 回退；所有控制错误必须携带 `code`
   与 `retryable`。RegistryError 映射统一 ErrorCode，未专门建模的错误保留原码与重试策略。
   Python control 和 Gateway 都按结构化字段处理错误，不从 reason 文本猜测类别。
6. 跨语言验收新增 F36：PyArrow 自定义认证发送的 `auth-token-bin` 不满足 Rust 服务的 Bearer
   契约；之前 mock 未检查后续 RPC 的 Authorization。现在每次 RPC 显式发送已签发的会话 token，
   管理员、writer、read 仍使用独立会话。CI 增加真实跨语言恢复脚本。

F25 可关闭；F09/F10 获得实质推进但未关闭，因为 alter/drop 与完整删除恢复仍未交付。
原 34 项审计现关闭 18 项，仍有 16 项：F07–F17、F22、F26、F28–F29、F33。
F35（停服排空顺序）和本批 F36 均为审计后发现并修复的问题，不混入原 34 项计数。

## 验证

- Rust workspace：1706 passed、10 ignored；Clippy 全 workspace/all-targets/all-features 严格检查通过。
- Python 主包和客户端合跑：692 passed、1 skipped；客户端单跑：88 passed，覆盖率 81.28%。
- Black：83 文件通过；Ruff、Rust fmt、协议单源再生通过。
- 配置范围 mypy：主包 14 文件、客户端连测试 24 文件通过；新增跨语言脚本单独类型检查通过。
  全仓 mypy 仍有 982 errors / 11 files / 82 checked，不能据此宣布合并门禁完成。
- 真实 Python ↔ Rust：空目录启动 → 管理员建 namespace / append table → `market` namespace WAL
  写入 → 查询 → 正常停服 → 重启 → 幂等建表重试 → 恢复相同数据；Arrow 23.0.1。
- 确定性回归覆盖：动态 actor 的重复注册和停服拒绝；已接受注册阻止停服完成并可在 admission
  关闭后安装；表数/元数据预算拒绝不留 Catalog 表；重复建表、冲突、停止后拒绝及 checkpoint 恢复。
- 日志位于 `/tmp/zippy-convergence-rust-final.log`、`/tmp/zippy-convergence-python.log`、
  `/tmp/zippy-convergence-client-final.log`、`/tmp/zippy-convergence-clippy-final.log`、
  `/tmp/zippy-native-client-verified.log`。首次跨语言失败保留于 `/tmp/zippy-native-admin-smoke.log`。
  `scripts/check_native_client.py` 是可复现验收入口，CI 在两个扩展安装后执行。

## 未完成边界

尚未交付原生 frame/key-value materializer、delete、快照游标和恢复；尚未交付 Flight DoExchange
订阅、本地计算输出接入新权威、schema alter/drop、`Persisted` 请求确认和跨查询固定快照。
因此 legacy registry/shadow/Gateway、双套客户端解释器及数据目录开关仍不能删除。
支持矩阵、全仓 mypy 与最终文档归档也未完成。本批没有提交、推送、部署或操作生产数据。
