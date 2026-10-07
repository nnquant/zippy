# 显式 Table 快照绑定语义收敛

日期：2026-09-23。复核原审计 F05 时发现，默认 `Client.read_table()` 创建的普通表调用
`Table.snapshot()` 会向服务端申请固定快照，却只把它放进“手动快照”列表：返回了快照 ID，
后续 `collect()` 仍使用逐查询边界，重复调用还会申请另一个快照。这个 API 的返回值与实际
查询保证相矛盾，也会占用直到表关闭才释放的服务端租约。

现在首次调用 `Table.snapshot()` 就把固定快照绑定到该表；重复调用返回同一元数据，
`collect()` 与派生查询都使用该快照。最后一个持有者关闭时释放租约。
构造时指定 `snapshot=True`、手动后捕获、以及多表 `Client.open_snapshot()` 共用同一
`_SnapshotLease` 所有权逻辑。没有用途的 `_manual_snapshots` 列表和 `_snapshot_enabled`
状态位已删除；不改变默认不调用 `snapshot()` 时的逐查询一致快照语义。

针对性回归测试先在旧实现上失败：普通表重复调用返回不同 ID；修复后两个捕获入口都通过
同一克隆、会话换新及最终释放测试。[真实原生矩阵](perf_reports/evidence/2026-09-23-table-snapshot-binding/native_matrix.log)
又在 `fixed_frame` 表上验证了手动捕获、后续持久写入、旧数据读取、克隆引用和租约释放。
最终 Python 全量 **544 项通过**，原生矩阵 58 个字段中 49 个布尔验收项全部为真；
客户端 sdist → wheel → 仓库外隔离安装后的 **220 项**通过，覆盖率 **85.21%**，
超过 80% 门禁；
Ruff、根 mypy 91 源码、客户端 mypy 27 源码、改动文件 Black 和差异检查通过。
Rust 生产代码未在本批变更；Rust 全工作区及 Clippy 的最近一次通过记录见
[控制入口修复](convergence-control-registry-bounds-2026-09-23.md)。
源码与矩阵哈希见[结构化证据](perf_reports/evidence/2026-09-23-table-snapshot-binding/summary.json)。

F28 Windows/macOS 实机验收按用户要求延期，未记为通过。没有提交、推送、部署或迁移旧数据。
