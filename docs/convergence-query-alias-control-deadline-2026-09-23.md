# 第五十六批：查询、控制与本地因子单主干收敛

日期：2026-09-23。承接[第55批](convergence-native-commit-retirement-2026-09-23.md)。
再次按源码检查旧 API 和部署状态时，发现独立客户端 `Table` 仍暴露三个明确标注为兼容的
方法：`where()`、`limit()`、`stream_info()`。这些入口已删除，分别使用 `filter()`、
`head()`、`info()`；当前客户端 README 的示例已迁移。历史路线图仍保留当时的调用示例，
其页首已标明不是当前 API 说明。

`zippy-core` 还导出一套无调用者的旧 stream schema 元数据与 FNV hash 工具；该模块及
公开重导出已删除。持久表 schema 仍由 Catalog 的标准 SHA-256 域计算与校验，
表格式、schema 变更和重启恢复路径没有改动。

本地计算还留有批量 `evaluate` 与上下文 `evaluate_with_context` 两套因子算法：
时间序列、截面及表达式因子分别维护重复实现，旧接口的默认 fallback 甚至会把
轻量上下文重新物化为完整 RecordBatch。现在每个因子只实现上下文算法；批量调用
只是将已有 RecordBatch 包成上下文后调用同一内核。重复的批量提取器、递归表达式
求值器和物化 fallback 已删除，保留原生引擎的数值、状态回滚与错误验收。
截面测试改为核对含空值的实际排名与统计值，不再比较两条现已合并的执行路径。

控制客户端原先默认使用无期限请求，并允许 `set_request_timeout(None)` 恢复该路径。
现在普通请求默认使用 30 秒单调期限，配置只能给出正时长；无期限发送函数已从公开
控制 API 退出。Watch 长轮询有独立的请求时长加 5 秒响应余量，客户端和 Master
共同限制请求中的长轮询时长不超过 30 秒。这样仍允许正常长轮询，又不能用极大
`timeout_ms` 长期占满 256 个连接槽。真实 listener 的越界拒绝和客户端短普通期限下的
长轮询均有回归测试。同步 Unix socket 的 `connect` 系统调用本身不能被该用户态期限中断，
其余连接前后检查、请求读写共用同一期限。

同步修正了收敛方案的当前状态，以及健康字段、物理文件读取和有限回放中已经过时的
“迁移/兼容”注释；部署用的 `ControlOnly` 模式仍是当前模式，不是旧数据主干。
最后审查还发现现行查询与 canary 指南继续指向已删除的配置开关、
`LegacyServerRuntime` 和 Gateway 查询切换步骤；现已按原生查询唯一主干修正。
可选 canary 只做同一原生 QueryService 上的诊断与摘要比较，不承担第二套查询权威。

原审计维持 **33/34 项本地关闭**。F28 Windows/macOS 实机验收按用户要求延期，未计为
通过；本批只验证本机 Linux。全 Rust workspace **1190 条通过记录、7 个忽略项**，
全目标全 feature Clippy `-D warnings` 通过；最终构建的 Python 全量 **543 项**通过。
原生矩阵 58 个结果字段中 49 个布尔验收项全部为真，没有失败字段。
Ruff、两套 mypy 和当前源码清单 90 个 Python 文件的逐文件 Black 检查通过。
本机 `black .` 的多文件调用超时，逐文件检查覆盖 `rg --files -g '*.py'` 返回的全部
当前源码；被 Git 忽略的历史 worktree 不在门禁范围。全量测试与构建证据见
[结构化证据](perf_reports/evidence/2026-09-23-query-alias-control-deadline/summary.json)。
没有提交、推送、部署或迁移旧数据。
