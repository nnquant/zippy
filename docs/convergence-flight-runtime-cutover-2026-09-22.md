# 第四十五批：Flight 成为唯一数据服务入口

日期：2026-09-22。承接[第44批](convergence-server-query-adapter-2026-09-22.md)。
关闭 **F11/F13/F14**，累计 **31/34**；F07/F08/F28 继续开放。

## 实现与契约

删除整个 zippy-gateway crate，以及 Server memory_ingest、Ingest memory 实现。
Gateway 的查询解释器、写入 worker、订阅 listener、固定快照、协议帧、鉴权和测试夹具一起退出。
工作区与 Server 依赖不再包含 Gateway；Master 的 GatewayConfig watch 事件也删除。
查询、写入和远端订阅统一使用已有原生 Flight 实现，本地计算继续通过 NativeProcessor 和 Client。
原生 Memory/WAL/Persisted 确认语义保留；删除的是另一套旧写入权威。

删除 gateway 配置字段、派生端口、环境覆盖和启动/停服分支。TOML/JSON 顶层拒绝未知字段，
`[gateway]` 即使 enabled=false 也报错；ZIPPY_GATEWAY 和 ZIPPY_GATEWAY_* 明确拒绝且不回显值。
Python CLI 删除 Gateway 帧协议探测、状态和展示字段。ready 记录升级为 **v9**；旧版本拒绝，
starting/stopping/failed 仍使用各自 v1 生命周期契约。

停服依次关闭入口、停止 Flight、排空进程并退出控制监听，最后关闭原生存储。
修复控制监听清理在超时后丢弃线程句柄的边界：保留已收到的完成结果与 join 责任，允许重试，
尚未退出时不缓存最终停服结果。新增确定性测试分别覆盖等待完成消息和等待线程退出两种超时。

## 验证与测试退役

- Rust 全 workspace：1,442 次通过记录、8 个原有忽略项；随后增加监听清理回归并复验
  Server：180 次通过、3 个原有忽略项。两个计数包含重复执行，不能相加作为测试总数。
- Clippy workspace/all-targets/all-features、`-D warnings` 通过；Rustfmt、Ruff、Black 92 文件通过。
- 重建服务端和扩展后 Python 全量 543 项通过；mypy 91 文件、客户端严格检查 27 文件通过。
- 当前二进制的三类表、schema/drop、固定快照、订阅与 ACK、计算/插件及重启矩阵全部通过。
- 原 Gateway 端口占用回滚测试迁到真实 Flight 端口冲突，并验证启动日志确实到达 bind 失败阶段；
  token 使用有效长度，避免提前在鉴权配置检查处失败而产生虚假通过。

仅退役删除实现专属的测试；原生恢复与停服断言保留。详细退役/改名、新增用例和保留边界见
[测试清单](perf_reports/evidence/2026-09-22-flight-runtime-cutover/test-retirement.json)，
源文件与日志摘要见[结构化证据](perf_reports/evidence/2026-09-22-flight-runtime-cutover/summary.json)。

默认配置仍可进入 Legacy 控制模式，Master 持久 registry 与 Rust 旧 source/materializer 仍在，
因此 F07/F08 未关闭。此批 Linux/CPython 3.11/Arrow 23.0.1 验收不替代新发行包或跨平台验收，
F28 保持开放。没有提交、推送、部署或迁移用户数据。
