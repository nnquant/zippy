# 控制入口与进程租约容量修复

日期：2026-09-23。针对原审计 F01/F02/F24，在当前源码做真实 listener 复核时发现：
debug 构建会先于 `ControlRequest` 解码接受无凭据的 `ExpireProcessForTest` JSON，
可直接把任意已知进程 ID 的租约标为过期。新增 Unix listener 回归测试在修复前按预期失败；
现在已删除该网络入口，`force_expire_process` 仅在 Rust 单元测试编译中存在。
正常 heartbeat 在拒绝未知请求后仍可使用。

另一资源边界是连续匿名注册：虽然连接数和单请求字节数已有上限，进程 registry 会永久
保留过期记录，连续注册仍可累积占用。现在 `app` 限制为 256 个 UTF-8 字节，
boot-scoped 进程记录上限为 4096 条。达到上限时仅逐出最早的过期记录及其运行时
投影；若所有租约仍活跃，返回结构化 `RESOURCE_EXHAUSTED`，不改变控制版本或进程 ID。
无效标签先于随机数请求被拒绝；随机数失败不会逐出待回收记录。单元测试覆盖容量边界、
活跃租约保护、过期回收和失败原子性；真实 listener 测试覆盖错误码和拒绝后的可用性。

验证使用仓库 Python 3.11：Rust 全工作区 `cargo test --workspace --offline` 通过；
`cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`、
`cargo fmt --all --check`、`git diff --check` 通过。服务端与 Python 扩展从当前源码重建后，
Python 全量 543 项通过；[原生矩阵](perf_reports/evidence/2026-09-23-control-registry-bounds/native_matrix.log)
58 个字段中 49 个布尔验收项全部为真，覆盖三类表、远端订阅、本地计算与重启恢复。
当前源码、服务端与扩展 SHA-256 及矩阵结果见[结构化证据](perf_reports/evidence/2026-09-23-control-registry-bounds/summary.json)。

本机沙箱禁止部分回环 TCP/Unix socket 操作，受限执行曾以 EPERM 停在 bind 或读超时设置；
上述完整运行使用非沙箱的本机回环测试，未访问外部服务。Windows/macOS 实机验收 F28
按用户要求延期，未计为通过。没有提交、推送、部署或迁移旧数据。
