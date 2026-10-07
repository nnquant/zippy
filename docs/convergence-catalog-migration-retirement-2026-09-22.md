# 第四十批：删除 Catalog 与 Storage 迁移通道

日期：2026-09-22。承接[第39批](convergence-online-shadow-retirement-2026-09-22.md)。
累计仍为 **27/34**，剩余 F07/F08/F11/F13/F14/F15/F28；本批不将部分删除计为关闭。

## 实现变化

- 删除 Catalog 的旧导入、reconcile、projection 三个模块和 Storage 的旧文件导入模块，
  四个模块共 7,659 行（包含模块内测试）。Catalog service 删除对应公共句柄、队列任务、
  导入窗口、请求版本与投影降级 hook；保留原生 manifest 发布、回执重放和快照 pin。
- 当前 Catalog 物理布局升级为 v10，删除七张迁移表，持久请求账本仅接受原生 v1。
  v1–v9 目录明确拒绝，不改写原文件、不切换 WAL、不创建 sidecar。
- Storage 不再读取或核对外部 legacy 文件；此类位置在物理读取前返回不可重试的
  DataCorruption。保留原生文件的路径、checksum、schema、行数和资源预算核对。
- Master 不再读取或复制 endpoint snapshot。移除对应配置字段，旧 TOML 字段和
  `ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT` 环境变量明确失败。

## 测试如何接替

删除只服务于已移除接口的迁移测试，逐项登记于
[退役清单](perf_reports/evidence/2026-09-22-catalog-migration-retirement/test-retirement.json)。
两个跨表预算测试改为两张原生表，继续断言总量超限先于物理文件读取失败。
新增旧 endpoint 文件不读取不复制、旧配置拒绝、布局无迁移表及旧请求版本拒绝、
外部文件在读取前拒绝等检查。没有用删除断言的方式掩盖原生行为退化。

## 验证

- 完整 Rust 工作区：1,690 通过，9 个原有忽略项。
  随后新增外部旧文件拒绝测试，Storage 文件核对专项 17 项通过；
  删除测试 fixture 的旧表后，Catalog domain 专项通过。
- 全工作区全 target Clippy 零警告；Rustfmt、diff 检查通过。
- 从当前源码重建服务端及 Python 扩展，Arrow 23.0.1 原生 API 重启矩阵通过。
  覆盖三类表、订阅/ACK、固定快照、本地计算及公开示例；额外只读检查真实运行目录
  `user_version=10`、无 `legacy_*` 表、请求账本只有 v1。
- Python 全量 595 项通过；默认 mypy 91 文件、客户端严格 mypy 27 文件通过；
  Ruff、Black 通过。

结果与源码、二进制、日志摘要见
[结构化证据](perf_reports/evidence/2026-09-22-catalog-migration-retirement/summary.json)。
本批没有重新验收 Arrow 25 发行包或 Windows/macOS。

## 剩余边界

Master 管理目录的持久 registry、投影事件发布者、共享 V2/V3/V4 类型、旧外部文件
元数据与编解码仍在；F15 继续开放。默认 Legacy、Gateway 读写/订阅、memory ingest
及内部 source/materializer 仍待删除，平台矩阵也尚未完成。
本批没有提交、推送、部署或迁移用户数据目录。
