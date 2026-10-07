# 单主干收敛第八批：有序原始事件窗口与变更通知

日期：2026-09-22。本批推进 F11/F16，不新增原 34 项关闭项：累计仍为 **21/34**，剩余 **13 项**。
前一批见[一致状态快照](convergence-state-snapshot-2026-09-22.md)。

## 已落地的读取基础

`NativeQueryEngine::prepare_log_window` 返回固定单表原生事件窗口，使用同一 Catalog、TableActor、
查询所有者、快照租约、取消、deadline 和内存账本。没有第二套业务状态权威，也不通过普通 SQL
最新状态查询模拟增量日志。窗口不是持续订阅；目前是 Rust 服务端 API，尚未接入 Flight wire。

- `start=Some(offset)` 从该 offset 包含式读取；`None` 固定当前水位但不回放。
- 完整历史按原生 provenance 的 offset 排序；重叠、缺口、未知水位、行数或 schema 不一致明确失败。
  超过当前尾部下一位置的游标拒绝，不悄悄跳到当前水位。
- 顺序打开 Parquet 或固定 SHM 前缀；不依赖文件名、SQL 扫描顺序、Parquet row group 或进程内队列。
  持久文件的第 i 行必须对应原生范围首 offset + i；所有后续 compactor 也必须遵守该写入契约。
- append 保留原始行序；key-value 保留全部 v2 控制列、重复键、upsert/delete 和 mutation 边界。
- frame 按 source identity 与 lineage 还原完整原帧。单个文件包含多个帧时分别标记；同一帧的
  lineage 被分到不同文件时重新合并边界。传输片段携带完整帧范围；从帧中间恢复明确拒绝。
  后续订阅层仍需将片段组装为产品要求的原子帧交付，不能把片段当成完整帧。
- 读到 EOF 后，调用方仍持有的结果继续保留内存计费；最后一个结果释放后账本归零。
  UTF-8 解码先根据固定 span 或 Parquet footer 预留，包括字典重复展开；无证明或溢出拒绝。
- 读者驱动 I/O，不建立后台结果队列；取消和丢弃窗口释放读取任务及快照，已交付结果独立保留。

## 变更唤醒

`TableActorHandle::changes()` 订阅成功 publication、持久化版本推进和 writer epoch 更新。
慢读者的通知合并为单个待处理状态，不缓存数据批次。快照读取和拒绝写入不会触发通知。
actor 正常退出或 panic 都会关闭通知，即使调用方仍持有 actor handle。

持续订阅应先注册通知，再固定窗口；读完窗口后等待通知并重新固定窗口。通知只用于唤醒，
真实数据和游标仍以固定窗口为准，因此并发写入不会因“先查后订阅”而漏唤醒。
取消等待不会吞掉尚未消费的通知。

## 新发现并修复：F41 历史 segment 与当前 writer epoch 混淆

TableActor 允许保留之前 writer 产生的已提交 segment，但查询快照的两处投影却要求 source epoch
等于当前 writer epoch。发生 writer 交接后，仍有效的历史 SHM 行会被误报为损坏。
现在区分当前写入 fencing epoch 与历史 source epoch：历史可以较早，来自未来的 source 仍拒绝。
真实双 producer 交接测试同时覆盖事件窗口和普通 DataFusion 查询，不仅测试 actor 内部快照。
F41 是原 34 项之外的新修复，不加入原审计关闭数。

## 验证与边界

本批验证结果和源码哈希记录在
[结构化证据](perf_reports/evidence/2026-09-22-event-log/summary.json)。
Rust 完整 workspace 测试、all-targets/all-features Clippy（警告视为错误）、rustfmt 和 diff 检查
全部通过。本批未改 Python，不重建扩展或重复 Python 套件；全仓 mypy 和平台矩阵债务仍开放。

验收覆盖真实 WAL/manifest 重启恢复、混合持久/SHM 窗口在 checkpoint 期间继续读取、offset 续读、
空窗口、帧边界、重复键/删除事件、双 writer 交接、取消及结果计费。

仍需完成 Flight DoExchange、连续窗口调度、durable consumer ACK、断线游标恢复、原子帧交付和
客户端 StateSubscription 接入，之后才能删除 Gateway 订阅。表管理、本地计算及其他原审计项
继续开放；本批不改默认存储权威，也不声称原生持续订阅已经可用。
