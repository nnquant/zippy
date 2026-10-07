# 单主干收敛第十七批：原生 schema 演进与历史读取

日期：2026-09-22。原 34 项仍为 **24/34 已关闭、10 项开放**。本批推进 F09，
新增修复 F46、F47；公开 Flight/Python alter 入口尚未接入，不把内部管理服务完成当作 F09 关闭。

## 一套明确的演进规则

`zippy-types::schema_evolution` 统一校验 Catalog 变更与历史批次对齐。允许尾部追加可空列、
普通列由不可空放宽为可空，以及描述元数据更新。已有列的名称、顺序和数据类型保持稳定；
不隐式改名、删列、重排或转换类型。frame 分组字段、append 事件时间字段和 KV 主键字段
完整保持不变，Arrow extension 的语义元数据也不能替换。KV 始终使用当前原生事件 schema。
完全相同的 schema 不是新版本；丢失响应应使用原 request ID 重试。

新增列或可空性放宽记为 Backward，纯元数据变更记为 Full。这里的历史读取是新主干内
不可变文件的正常版本语义，不是旧 API、旧配置或旧物理目录的兼容分支。SQLite 仍只有布局 v8，
不重新引入在线物理迁移。

## Catalog 与运行时一起变更

Catalog `AlterTableSchema` 采用 expected_schema_version CAS，在同一事务内插入不可变 schema、
推进当前 schema version 和 writer epoch、写入 revision 与幂等回执。历史 schema、文件和快照
不改写；schema version 与 snapshot version 仍是两个独立选择器，不伪造新的数据快照。
读取旧 schema 复用既有有界读取、IPC 校验、canonical hash 校验及表定义一致性校验。
如果旧 schema 缺少定义引用的字段，不能通过新增该字段的提案静默修复损坏的 Catalog 配对。

Server 内部 `NativeTableAdmin::alter_table_schema` 的执行顺序为：

1. 取得管理预算锁与停服排空许可，检查幂等回执、预期版本、演进规则及新 metadata 额度。
2. 使用与删除共用的 `NativeTableTransitionGuard` 隔离新写入，等待已接纳 durable ACK 完成后才允许进入。
3. 在持有该 guard 时 checkpoint 全部已接收数据；确认 Catalog 持久水位覆盖旧 schema 的接收水位。
4. 暂停旧 actor 后提交 Catalog 事务。提交前失败由 guard 恢复旧 admission。
5. 提交后退休旧 ingest、materializer 和 actor，再按最新 Catalog 安装同一 table ID 的新实例。

操作由独立管理任务持有，调用方取消等待不会中断已接纳任务。已提交回执的重试不会重做变更，
也不会把较新实例退回旧 schema；若表已删除，不重新安装。旧固定 snapshot/span 仍持有原始数据。
本批没有穷举每一个取消或进程崩溃指令点，不能据此宣称任意故障注入矩阵已全部验收。

在线注册和启动恢复共用 checkpoint 校验：旧 schema checkpoint 只有在持久水位被覆盖、
writer epoch 已严格推进时才可使用。未来 schema、零版本、水位超前及未推进 epoch 明确拒绝。
checkpoint 之后需要重放的 WAL 仍要求与当前 schema 完全一致，没有按旧格式重新解释 WAL。

## 历史文件与实时读取

事件日志计划允许文件 schema 不高于当前表 schema；实际读取检查已有列的顺序、名称、类型和可空性，
只为追加列分配全空数组，原列继续共享原缓冲区。输出采用已验证的 Catalog 元数据，避免依赖
物理 segment reader 在每个批次保留 schema 级 KV 元数据。普通 SQL 的 Parquet schema 适配通过真实
历史文件与实时批次的混合查询验证，不只依赖执行计划声明。

三类表的运行时回归覆盖：旧 schema 内存写入、错误 CAS 保持可用、schema 变更前自动持久化、
旧 writer/actor 拒绝继续写入、旧固定窗口可读、新 schema WAL 写入、同目录两次重启、checkpoint、
完整事件 offset 和新增列空值、SQL 查询及 exact replay。KV 额外更新一个已持久化主键，验证
最新实时值覆盖旧文件中的值，重启及再次 checkpoint 后结果一致。

## F46：混合扫描错误地声明保持排序

KV 查询同时读取历史 Parquet 和实时 segment 时，DataFusion 的 SanityCheckPlan 拒绝窗口计划。
`PersistedAndActiveExec` 按先历史后实时拼接，却声明两个输入都保持排序；优化器因此将唯一排序
下推到两个子输入。两个分别有序的流直接拼接，不能满足整个结果的排序要求。

修复为不声明保持输入排序，保留全局排序节点。新增端到端回归验证跨历史/实时边界的相同主键
最新值以及查询结果排序。F46 不增加原 34 项分母，也不等同于 F14 的查询主干已经全面收敛。

## F47：目录允许的类型与实际写入能力不一致

Catalog 的 Arrow 类型空间宽于原生 materializer。原先在线建表只校验 Catalog schema，
例如 Boolean 字段可以提交并返回成功，直到首笔写入建立 segment 时才报不支持；同样的问题
也会影响新增列。Server 现在在建表和 schema 变更提交前调用 materializer 实际使用的
`compile_arrow_schema`，只编译布局，不创建 SHM。运行时注册/启动也复用同一校验。
没有另外维护一份可能漂移的类型白名单。

三类表回归都验证 Boolean 提案在 Catalog 提交前返回 SCHEMA_MISMATCH：失败建表没有目录行，
同 request ID 可以提交更正后的 schema；失败变更保持原 schema version。随后正常写入、演进、
历史查询和重启恢复全部继续验收。Catalog 通用类型能力与可写原生表能力因此明确分层；不会
因为通用 Arrow IPC 可解码就宣称原生写入器支持该类型。F47 不改变原 34 项分母。

## 验证与剩余边界

最终完整 Rust workspace：1,754 passed、10 ignored；
重建最终扩展后 Python：726 passed、1 skipped。Clippy、Rustfmt、Black、Ruff 和配置范围
类型检查通过；全仓 mypy 仍为 991 errors in 12 files（85 files checked）。

验证结果、日志与指纹见[结构化证据](perf_reports/evidence/2026-09-22-schema-runtime/summary.json)。
Python 生产代码和 wire schema 本批未变更。完整类型门禁仍存在历史错误；配置范围通过不代表
F29 已关闭。Windows/macOS 未新增实机结果。

下一步首先接入 schema 变更的公开 Flight/Python 管理入口，验证三类表真实远端调用、权限、
错误码、稳定重试与 schema 变化后的订阅行为。F07/F08 默认配置与权威切换、F11 统一订阅与
持久消费确认、F13/F14/F22 主包写入查询及本地计算统一、F15 旧 adapter 退出、F28 平台矩阵、
F29 全量类型检查继续保留为未完成项。
