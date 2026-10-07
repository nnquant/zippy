# 第四十六批：删除 Master 的持久目录

日期：2026-09-22。承接[第45批](convergence-flight-runtime-cutover-2026-09-22.md)。
累计 **31/34**；F07/F08/F28 保持开放。

## 删除范围

删除 Master snapshot.rs、LegacyCatalogAdapter、旧 registry JSON 的加载/恢复/写入、写入失败回滚，
以及旧 Parquet compaction worker、配置、专属测试和三份旧 golden fixture。
Master 构造函数不再接收快照路径；停服和注册请求不再刷新另一份持久目录。
Master 的直接 Parquet 依赖也已删除。运行时状态变更仍受 registry 锁保护，保留必要的内存事务回滚。

含 `catalog/legacy-master-registry.json` 的目录明确拒绝，不读取其内容或自动迁移。
新增测试使用不可解析字节，验证文件原样保留、未创建 SQLite、失败后目录所有权可释放。
旧 compaction TOML/JSON 字段和环境变量明确拒绝，避免静默接受失去执行者的配置。
原生 Catalog 的 compaction 命令域和原生存储生命周期保留。

## 原生目录与健康契约

Server.catalog() 只返回原生 SQLite read handle；原生存储未启用时为 None。
未启用 Flight 的模式改称 control_only，不再声称提供旧 Catalog 服务。
原生 Catalog 的当前发现、事务写入和历史快照能力按实际存储健康发布；删除 degraded 状态。
CLI 要求原生目录和启用的 Flight 查询明确报告 ready，旧目录健康不能冒充原生就绪。
ready schema 为 **v10**；旧版本和未来版本均拒绝，starting/stopping/failed 仍是 v1。

停服复验修正一个旧测试假设：重复调用可以继续完成线程清理，已经解决的错误无需保留。
测试仍验证超时进程不可遗忘、未完成结果不得被判为 clean、数据目录所有权不可提前释放。
运行时状态连续更新、读者租约和原生恢复断言均保留。

## 验证

- Core/Master/Server 回归：476 次通过记录、4 个原有忽略项，包含子进程复验记录。
- Clippy 全 workspace/all-targets/all-features、`-D warnings` 通过；Rustfmt 与 diff 检查通过。
- 重建服务端与扩展后，Python 全量 546 项通过；mypy 91 文件、客户端严格检查 27 文件通过；
  Ruff 与 Black 92 文件通过，扩展动态库依赖可解析。
- 当前二进制完成三类表、schema/drop、固定快照、订阅/ACK、计算/插件和重启恢复矩阵。
  新增最终断言证明经过真实多次重启后，不再生成旧 Master registry 文件。

[测试退役与迁移清单](perf_reports/evidence/2026-09-22-master-persistence-retirement/test-retirement.json)
列明旧快照专属用例与保留的运行时契约；
[结构化证据](perf_reports/evidence/2026-09-22-master-persistence-retirement/summary.json)
记录本批源文件、日志与二进制摘要。

F08 仍开放：临时 registry 的旧表/生产者控制请求、内部 source/materializer 尚未删除。
F07 的 Legacy backend 和原生功能开关尚待收敛；F28 仍缺当前版本的完整平台实测。
此批未重新运行整个 Rust workspace 的全部测试，只运行受影响的三个 crate 并检查所有 target/feature；
全 workspace 运行证据来自第45批。未新增发行包和 Windows/macOS 验收。
没有提交、推送、部署或改写用户数据文件。
