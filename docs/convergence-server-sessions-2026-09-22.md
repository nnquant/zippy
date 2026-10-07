# 单主干收敛第十三批：服务端签发的唯一 Flight 会话

日期：2026-09-22。本批关闭 F26，累计 **23/34**，剩余 **11 项**。
[第十二批](convergence-native-state-2026-09-22.md)已关闭 F16；本批统一其底层会话契约。

## 删除双套会话实现

删除 `legacy_request_session`、`SESSION_METADATA_KEY` 和客户端生成随机 session ID 的路径。
所有查询、状态快照、日志及写入均通过 Handshake 获得服务端签发的 session ID 与短期 bearer。
服务端只按 bearer 校验身份，command 的 session ID 必须匹配该身份。

SessionAuthority 现在是服务必有组件，共用容量、TTL、角色、过期、撤销、ticket 归属及关闭逻辑。
区别只在首次握手的部署策略：

- 生产认证模式验证 read/writer/admin 独立 bootstrap 凭据，然后签发相应角色会话。
- loopback 只读开发模式显式声明 `auth.local_read_handshake.v1`，握手不携带 bootstrap 凭据，
  只签发 read 会话；不允许写入和管理。它不证明外部用户身份，不能部署到公网。
- 协议服务构造必须显式选择策略；Server 依据 `[flight.auth]` 选择。无论是否 bootstrap 认证，
  session TTL 都必须覆盖 query plan/metadata ticket 的生命周期，容量限制同样生效。
- 非 loopback listener 在 bind 前要求 TLS 和 bootstrap 认证；底层 Flight listener 也执行检查，
  不能绕过产品配置的限制。

旧 `x-zippy-session-id` 不再有服务端解析或客户端发送分支，只保留为拒绝测试样本。
客户端要求 `session.server_authority.v1` 和有界 TTL，拒绝旧服务能力；如果配置了 bootstrap 凭据
却连到只读开发服务，明确报错，不忽略凭据并静默降级。

## 验证

- Rust workspace 全套通过；最后补充的 listener/TTL 契约由 protocol/core 全套再次验证。
  Clippy 全目标全特性、禁止 warning，通过。
- 客户端 115 项通过，覆盖率 81.57%；真实 PyArrow loopback RPC 测试已迁移为握手签发会话。
- 已重新构建主包原生扩展，主包 Python 全套 610 passed、1 skipped（既有跳过项）。
- Black/Ruff、配置范围内的根和客户端 mypy、rustfmt、diff 检查通过；F29 仍开放。
- `check_native_client.py` 分别在 Arrow 23.0.1 和 25.0.1 完成六次真实服务启动：认证模式的三类表
  写入/恢复/日志与状态重建，加上同目录的 loopback 只读查询、状态快照和原子帧日志。
  新通道只携带旧 session header 被 UNAUTHENTICATED 拒绝。
- 测试覆盖旧头拒绝、伪造 command 身份拒绝、外来 ticket 不被消耗、本地只读角色、session
  容量/过期/关闭，以及既有 writer/admin、TLS、取消与恢复行为。

日志和源码哈希见[结构化证据](perf_reports/evidence/2026-09-22-server-sessions/summary.json)。
本机证据仍为 Linux x86_64 / Python 3.11；不把两个 Arrow 版本的结果扩展成完整平台矩阵。

剩余 F07/F08/F09/F10/F11/F13/F14/F15/F22/F28/F29：唯一目录和写入权威、alter/drop、durable ACK
及旧回调、计算输出迁移、重复查询和客户端删除、类型债务与支持矩阵。Gateway 和旧数据目录
分支尚未删除，整体任务仍在进行。
