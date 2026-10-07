# 单主干收敛第七批：一致状态快照与游标

日期：2026-09-22。结合前两批三类表写入与恢复验收，本批关闭原审计 **F12**；累计 **21/34**，
剩余 **13 项**。F16 继续开放：快照 RPC 已交付，但原生持续订阅及断线后的完整恢复流程尚未接通。
前一批见 [key-value 实施记录](convergence-key-value-2026-09-22.md)。

## 一次固定快照返回状态和游标

新增 `Client.snapshot_state(source, namespace=...)`，通过 Flight 的既有 GetFlightInfo / DoGet
票据链路返回完整状态。capability 为 `state.key_value_snapshot.v2`。该入口也可供只读客户端使用，
不依赖 writer 凭据。服务端仅接受固定的单表完整状态查询；不能加过滤、投影、limit 或第二个表。
传输 batch 行数可以调整，不改变状态的完整性。

状态数据和游标来自同一 `NativeQueryReadSnapshot`：

- 已持久化部分使用固定 manifest 的 mutation version。
- 若存在未持久化事件，读取已经固定的 SHM 前缀尾行；要求事件序号、批末序号与快照水位相等，
  保证游标不落在原子 mutation 中间。
- schema metadata 返回 v2 的 protocol、sequence、version、stream identity 和有序键列表。
  stream identity 为表 UUID；客户端同时校验查询票据中的表身份，防止状态与游标串表。
- 初始空表返回 `(sequence=0, version=0)`；全部键删除后的空状态保留非零游标，重启后不归零。
- 用户根/字段元数据保留，KeyValueState 的再物化也保留用户根元数据。新建原生表拒绝占用
  `zippy_kv_` 元数据前缀，避免业务标签与状态协议发生歧义。

所有结果仍走同一 DataFusion 逻辑表视图。没有第二份 Python 状态查询解释器，也没有查询完成后
再取一次“当前游标”。票据只允许消费一次，鉴权、超时、取消、结果流及 schema 检查共用原有机制。

## 资源所有权

状态 schema 的解码临时空间在解码前预留、解码后释放；输出 schema 与游标有单独的保守预留，
由 prepared query、结果流和已经交给下游的结果批次持有。结果不复制列值。结果流读完不代表所有
下游批次都已释放；最后一个结果释放后资源账本才归零。没有提高默认查询预算以绕过验收。

## F12 关闭依据

F12 原问题为原生 materializer 和 Flight 仅承接 append，其他表只有 typed mutation 声明。
当前三类表均通过唯一 materializer/ingest/WAL/TableActor/manifest 路径完成在线创建、写入、
固定单次查询快照、持久化和重启恢复；key-value 又具备完整状态与精确游标 RPC。对应证据包括：

- 第五批：完整 frame 边界、写前超限拒绝、半帧持久化拒绝、WAL/manifest 恢复与精确回执。
- 第六批：key-value 原始事件顺序、重复键、delete、半 mutation 持久化拒绝及版本/回执恢复。
- 本批：先准备快照、再提交删除，旧票据仍返回删除前的数据和旧游标；下游持有结果时内存预留
  继续有效，释放后归零；空目录真实四次启动验证 append/frame/key-value，并验证全部删除后的
  空状态恢复为 `sequence=10, version=6`。

F12 的关闭只说明三类表的原生能力已经落地。固定跨查询租约、表级 alter/drop、原生 compaction、
持续订阅和本地计算接入仍各有未完成项，不能据此删除仍在承接这些功能的旧路径。

## 验证

- Rust 完整 workspace 回归通过；之后补充的结果所有权测试单独通过。
- 客户端完整套件：95 passed，覆盖率 81.11%；25 个文件的 mypy、相关 Python Black/Ruff 通过。
- 四次服务启动的真实 Python ↔ Rust 验收通过；包括初始空状态、跨批传输、持久化、全部删除后
  空状态恢复以及既有 append/frame 链路。
- 主 Python 包从当前源码重建后完整回归：605 passed、1 skipped。
- Clippy、Rust fmt、proto 再生检查通过；详细日志及哈希见[结构化证据](perf_reports/evidence/2026-09-22-state-snapshot/summary.json)为准。
  Rust 日志中的 logging 子测试输出有交错，证据记录完整命令退出状态，不将不完整的行解析数当作总测试数。

剩余编号：F07、F08、F09、F10、F11、F13、F14、F15、F16、F22、F26、F28、F29。
下一依赖项是同一原生事件日志上的 Flight 实时订阅，随后才能将 StateSubscription 的初始化、缺口检测
和重连恢复连为完整产品链路。没有提交、推送、部署或改写生产数据目录。
