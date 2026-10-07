# 当前指南与历史设计的边界复核

日期：2026-09-23。承接[第五十六批实施记录](convergence-query-alias-control-deadline-2026-09-23.md)。

以当前 `ZippyConfig`、`ServerRuntime`、查询服务和原生计算源码为准，修正了仍作为现行指南的
[原生查询服务](native-query-service.md)、[原生查询校验器](native-query-canary.md)、
[原生计算](native-compute.md)、[单节点运行时](single-node-server-runtime.md)、
[当前能力](current-capabilities-2026-09-21.md)与 [ADR-0007](adr/0007-single-trunk-new-contract.md)。
这些文档现在统一表述：SQLite/原生查询是唯一主干，Flight 承载公开数据 API；旧开关、
Gateway 查询与旧 source/materializer 已退出。可选 canary 复用原生 QueryService，
只提供内部诊断，不形成第二套数据权威。

[辅助服务设计草案](server-owned-auxiliary-services.md)继续保留原阶段设想，但已增加醒目的
历史状态说明；其旧 Gateway/LegacyServerRuntime/compaction 部署步骤不是当前操作指南。
按日期保留的阶段实施记录和历史设计不整体重写，以免篡改当时的验收事实。

复核了上述七份文档的相对链接和 canary TOML 示例；链接目标存在，TOML 可解析，
且 canary operator 内存不超过示例中的全局预算。`cargo fmt --all --check` 与
`git diff --check` 通过。本批仅修正文档和两处 canary 类型注释，未改变运行逻辑；
运行逻辑验收仍以[第五十六批证据](perf_reports/evidence/2026-09-23-query-alias-control-deadline/summary.json)
为准。F28 Windows/macOS 实机验收按用户要求延期，不记为通过。
