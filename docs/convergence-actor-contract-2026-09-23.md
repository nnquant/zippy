# 第五十三批：actor 持久化契约收敛

日期：2026-09-23。承接[第52批](convergence-descriptor-contract-2026-09-22.md)。
累计关闭 32/34 项：F08 仍在本地收敛，F28 跨平台实机验收按用户要求延期。

原来的 `TableActorRegistry` 同时提供只读启动与带持久控制器的启动入口，actor 内又把
持久控制器表示成可选值，因而生产可走到“持久化关闭”的状态。现在唯一公开启动入口必须
接收 `PersistControllerHandle`；actor 只保留 Idle、InFlight、Failed 三种实际持久状态。
测试使用可控的模拟提交器，但沿同一个构造契约，覆盖控制器拒绝后 actor 仍可读取。

`StreamTableMaterializer` 同步删除可关闭 checkpoint 的旁路；启动 Catalog 校验仅保留
同时返回验证结果和 bootstrap 描述符的 `verify_with_bootstrap`，避免调用者丢失后者。
原生 append/frame/key-value、远端订阅和计算路径没有改写。

受影响的 ingest/storage/server 测试合计 304 条通过记录、5 个原有忽略项；全工作区
all-targets/all-features Clippy `-D warnings`、Rustfmt 和 diff 检查通过。重建服务端与 Python
扩展后，Python 543 项通过，原生三类表、订阅、计算及重启恢复矩阵全部通过。mypy 91
个源码文件、客户端配置 27 个文件、Ruff、Black 92 个文件均通过。全 Rust 工作区最近
一次完整测试见第51批；本批为受影响 crate 与全目标编译验收。

[结构化证据](perf_reports/evidence/2026-09-23-actor-contract/summary.json)包含测试日志及
当前二进制摘要。F08 仍需退出底层 segment-store 旧持久 worker、Catalog 提交器双入口，
并修复 Master 在 OS 随机失败时生成认证 token 的不安全回退。F28 不在本次本机验收范围。
未提交、推送、部署或迁移旧数据。
