# 单主干收敛第六批：原生 key-value 事件与查询

日期：2026-09-22。本批补齐 key-value 原生创建、upsert/delete、WAL/Persisted 恢复和逻辑查询，
推进 F09/F10/F12/F16。原 34 项仍为 **20 项关闭、14 项未完成**；一致状态快照与游标、订阅、
计算和权威切换尚未完成。前一批见 [frame 记录](convergence-frame-2026-09-22.md)。

## 同一写入与恢复路径

- `CreateTableRequest.definition` 增加 key-value 分支，共用现有管理 action、Catalog、动态安装
  及停服排空；不新增独立管理通道。Flight capability 明确公布 `ingest.key_value.v1`。
- 唯一 `NativeTableMaterializerSink` 接收三类表。key-value 的每次非空 mutation 生成一批
  protocol-v2 事件；控制列由服务端生成，用户 schema 中保留名称被拒绝。
- 事件序号等于原生 offset 加一；mutation version 每个非空批次递增一次；stream identity 为
  表 UUID 的 32 位十六进制串，跨 writer 和服务重启不变。重复键保留原始顺序和所有事件。
- upsert 要求完整逻辑 schema；delete 接受完整 schema 或精确的有序键 schema，并校验元数据。
  键必须是非 null UTF-8 值；非键 delete 字段只是确定性占位，消费者不得解释为业务更新。
- Python `create_key_value_table/upsert_key_value/delete_key_value` 共用 Flight 实现。每次 mutation
  保持单批原子边界，多 chunk Table 合并后再校验大小，不能沿用 append 的自动分批行为。
- checkpoint 保存完整 mutation version。持久化水位不能切开 mutation；WAL 重放、manifest 恢复、
  producer fencing 和 Persisted 精确回执复用现有原生契约。空批不推进事件或状态版本。

## 查询和状态消费

物理存储保存事件日志；公开 schema 隐藏 v2 控制列。DataFusion 在固定查询快照内，按复合键和
事件序号选出最新事件，排除 delete，随后执行用户过滤、投影和 limit。禁止将用户过滤或 limit
提前下推到事件日志，否则旧值可能复活。schema 的根和字段元数据保留。

KeyValue v2 本地消费者在整次 mutation 收齐后原子应用 delete；重复删除、缺失键、删除后重插和
分片批次都有回归。槽位复用，删除释放字符串值。此能力不等于已交付远端一致 snapshot+cursor。

## UTF-8 查询预算

原先原生查询只接纳定宽输出。现在在值解码前建立 UTF-8 上界：SHM 扫描固定快照前缀的 offsets，
以固定大小缓冲计算最大单元字节数；Parquet 使用已验证 footer 的 row-group 非压缩字节数作为
单元上界，再乘输出行数、UTF-8 列数和同时存活批次份数。字典编码重复引用不能按压缩大小计费。

窗口、聚合、排序、过滤、投影、重分区和 join 的表达式均参与检查。没有扩张证明的字符串表达式、
字符串聚合/窗口输出仍明确拒绝；普通列复制、键排序和原生 row_number 可用。溢出或缺失 SHM
快照证明在解码前失败。最终输出分区合并在物理计划验收前生成并计费，不在执行时隐式新增。

这是保守资源上界，可能拒绝预算不足的宽字符串查询；没有提高服务预算或关闭 admission 来通过验收。
它不宣称所有 Arrow 变长类型或任意 DataFusion UDF 都已支持。

## 新发现与修复

- **F39：SHM schema 元数据丢失。** 编译 schema、段 Arrow 导出，以及主包/独立客户端的读取端
  都需要保留根/字段元数据并纳入确定性 schema hash。现在使用相同规则；元数据字段排序后计算，
  重复应用不改变身份。旧 metadata 忽略分支未保留；两端须一起升级。客户端跨段读取回归增加
  source 与列单位元数据，验证生产与消费规则一致。
- **F40：通配符规划预算误按最大允许关系数展开。** 单表 `SELECT *` 被按 64 张表估算，实际只有
  7 个物理列却计为 449 个表达式，导致小表查询超出 1 GiB 预算。现在按 SQL 实际关系出现次数计算，
  保留 join/union 的保守上界、SQL/节点/表达式限制，并升级 profile 版本。没有简单降低安全系数。

F39/F40 是原审计之外的新增修复，不计入原 34 项完成数。

## 验证与边界

- Rust 完整 workspace：1727 passed、10 ignored；之后新增的查询 admission 集成测试独立复验，5 passed。
- 主 Python 包完整套件：605 passed、1 skipped；客户端完整套件：94 passed，覆盖率 81.10%。
- workspace/all-targets/all-features Clippy、Rust fmt、diff whitespace、proto 再生检查通过。
- 客户端源码、测试与验收脚本共 25 文件 mypy 通过；本轮 Python 范围 Black/Ruff 通过。

最终结果与日志哈希见 [结构化证据](perf_reports/evidence/2026-09-22-key-value/summary.json)。
真实 Python ↔ Rust 验收使用空目录创建三类表，连续三次启动；key-value 依次经历重复键覆盖、
删除、WAL 恢复、Persisted 恢复及后续写入，最终只保留 `symbol=c, price=6`。同时复验 append 和
frame。此次真实验收使用 Arrow 23.0.1，没有扩大平台支持声明。

主 Python 包重建后复验 v2 状态测试；独立客户端重新构建后跑完整套件和覆盖率门禁。没有把旧扩展
的测试结果当作当前源码证据。完整 Rust workspace、Clippy、fmt、proto 和 Python 局部静态检查
结果以证据文件为准；全仓 Python 类型债务与平台验证仍开放。

仍需完成：key-value 一致状态快照与游标、原生 compaction、Flight DoExchange、订阅缺口与恢复、
本地计算输出接入、alter/drop、固定跨查询租约、唯一 Catalog/新数据目录以及旧路径删除。
本批没有提交、推送、部署或改写生产目录。
