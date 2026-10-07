# 第四十二批：删除旧文件与未知位点契约

日期：2026-09-22。承接[第41批](convergence-master-projection-retirement-2026-09-22.md)。
F15 关闭，累计 **28/34**；F07/F08/F11/F13/F14/F28 保持开放。

## 单一原生文件路径

删除 `ExternalDataPath`、旧文件 provenance、discovery order、Import 快照、迁移 actor
与 import maintenance job。Catalog 查询、固定快照 reader 和启动 verifier 只读取 managed/native
文件，统一按 file_id 排序；不再从系统根目录打开外部旧文件，也不保留旧文件专用摘要检查。
每个文件仍须具有精确 offset 覆盖、非空 lineage 和有效 checksum。

Catalog 物理布局推进到 **v11**：移除两列旧排序信息，以 SQL 约束拒绝外部位置、旧来源、
import 快照和 unknown 位点。v1–v10 与未来版本在启用 WAL 前拒绝，旧目录不被改写。
当前请求与回执继续使用原生 v1，版本号与物理布局版本独立。

共享位点只保留 Empty 和 Known，删除 LegacyUnknown 及贯穿 Catalog、WAL、actor、
materializer、查询和 Flight 的分支。合法位点 0 与空状态仍严格区分；u64 上限往返仍受测试保护。
Flight 的旧枚举编号 3 标为 reserved，Rust/Python 两端从同一 proto 再生并通过无差异检查。

本批相对批前备份的 34 个 Rust 文件净减 1,282 行。连同前三批删除的在线 shadow、
Catalog 导入/reconcile/projection、Storage 导入、endpoint snapshot、Master 投影事件和
V2/V3/V4 编解码，F15 所指的多代迁移桥接已从生产路径退出。

## 测试迁移与回归

退役 8 个旧测试名称：其中一个改为四个原生文件的统一排序测试，其余验证的是已删除的旧
排序、导入摘要或可构造的 LegacyUnknown。新增两个 WAL/Flight 旧标签拒绝测试。
完整逐项映射见[测试退役清单](perf_reports/evidence/2026-09-22-native-file-contract/test-retirement.json)。

旧标签同时在 SQL 约束和损坏后的读取边界验证，确保读取外部路径之前就失败。
已有 schema、文件身份、checksum、lineage gap/overlap、元数据预算、pin 生命周期和恢复断言保留。
两个 golden fixture 因旧文件改为原生文件、旧 producer 位点改为 Known(4) 而更新；
其余 3 个命令及 9 个回执 golden 保持原值，未把 fixture 内容变更描述为指纹算法变化。

- 完整 Rust workspace：日志记录 1,643 次通过、9 个原有忽略项，包含子进程复验记录。
- Clippy：全 workspace/all-targets、`-D warnings` 通过；Rustfmt、diff 检查通过。
- 重建 Python 扩展和服务端后，原生矩阵验证三类表、schema/drop、固定快照、订阅/ACK、
  本地计算、公开示例与重启恢复全部通过；直接检查 SQLite v11 与仅原生文件/位点/账本。
- Python 全量 595 项通过；mypy 91 文件、客户端严格 mypy 27 文件、Black 92 文件及 Ruff 通过。
- 当前扩展动态库依赖可解析；协议绑定再生无差异。

命令日志、源码及二进制摘要见
[结构化证据](perf_reports/evidence/2026-09-22-native-file-contract/summary.json)。

## 剩余边界

F15 关闭不表示旧服务端权威已经删除。默认 Legacy、Master 持久 registry、Gateway、
旧 memory ingest、查询/订阅和 Rust/Python 内部旧 source/materializer 仍属于剩余收敛工作。
本批 Linux/CPython 3.11/Arrow 23.0.1 证据不替代新的 Arrow 25 发行包或 Windows/macOS 实机验收。
没有提交、推送、部署或迁移用户数据目录，整体目标仍未完成。
