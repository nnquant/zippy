# 第二十三批：原生计算内核、结果写入与旧客户端订阅删除

日期：2026-09-22。推进 F11/F13/F22；原 34 项仍为 **25/34 已关闭、9 项开放**。
本批交付原生计算基础链路和三类结果表验收，不等同于旧 Session/Pipeline、插件 source、
服务权威和主包 API 已全部切换。使用示例见[原生本地计算](native-compute.md)。

## 原生计算

`NativeProcessor` 直接拥有现有 Rust ReactiveState、ReactiveLatest、TimeSeries、
CrossSectional、BarGenerator 内核，复用同一表达式/因子编译器。它不创建旧运行时线程、
Master source 或 SHM publisher。输入使用严格 Arrow schema；跨批状态在同一个内核中延续。
`transfer()` 转移当前状态并关闭原句柄，避免调用者与流水线交错修改状态。
内核执行或输出转换失败后不可继续复用；窗口通过 flush/finish 显式收尾。

`ComputePipeline` 使用独立 Client 的 Flight callback 消费，结果经原生 ingest 写入 append、
frame 或 KV upsert。完整输入先重组传输分片，再交给内核；输出保留引擎给出的批次边界。
只有结果写入获得回执后才完成输入 callback、推进原始输入游标。默认 WAL，允许显式
Memory/Persisted；后者等待 Parquet/Catalog 发布。停止顺序为停止并等待输入、收尾内核、
关闭输出 writer。异常退出可丢弃尚未生成结果的窗口；任何模糊写入都终止任务，不盲目重放。

输入失败和外部取消会反映到状态/指标，join/stop 传播原始失败。失败写入已收到的回执单独
保留，不能把未确认的批次算作完整成功。发现并修复了现有 Flight 写入异常链丢失关闭阶段
服务端终止原因的问题；此修复保留模糊写入分类，不把一般 CONFLICT 误判成绝对未提交。

## 不变表身份

DoPut 增加可选 `expected_table_id`，原生服务广告 `ingest.table_identity.v1`。指定身份时，
服务端在获取 producer 之前检查 Catalog 解析结果；不匹配明确拒绝。客户端在不支持该能力的
服务端前直接拒绝，不忽略此字段；持久 DoPut 复用与流轮转都保留身份约束。

计算流水线必须指定输出身份。真实验收覆盖三类输出表删除后同名重建，旧任务使用旧身份
写入失败，替代表保持为空。普通 SDK 写入仍可不指定该可选 guard，不将计算专用要求外推
成所有 writer 已自动固定身份。

## 删除独立客户端旧订阅

删除 `subscriber.py` 内 GatewaySubscriber/ShmSubscriber、旧 descriptor/segment 续读、
SHM 可选加载与可用性探测、旧订阅参数校验；删除 GatewayTransport.open_subscription 和
`shm` transport 枚举。公开 Client callback 已有经过验收的原生 Flight 实现，本批移除此前
仍可单独导入调用的旧类，不保留兼容别名。Row 和精确时间戳转换迁入 `row.py`。

旧组件专属的晚启动 Gateway writer、Gateway idle、两种 SHM rollover、descriptor helper
测试随对应实现退出。等价能力由真实原生空表先订阅后写入、固定身份续读、断线/服务重启、
完整帧和 KV 边界验收覆盖；额外补原生空闲等待和纳秒/空值/metadata/Row 映射回归。
不能将删除旧实现造成的覆盖率变化解释为新增功能测试数量。旧 SHM 扩展构建产物仍待从
发行配置清除，但客户端已不导入或使用它；主包自身旧订阅实现尚未退出。

## 验证与保留边界

完整日志摘要与源文件 SHA-256 见
[验收证据](perf_reports/evidence/2026-09-22-native-compute/summary.json)。

- 五类真实 Rust 内核的数值、跨批状态、窗口关闭、所有权转移和失败终止单测通过。
- 计算模块单测 14 项，覆盖率 83.43%。
- 真实服务在 Arrow 23.0.1 / 25.0.1 下验证 append 输入、三类计算结果、四代服务启动读取，
  以及旧表身份拒写。frame 普通查询读取历史，另外按日志验证每帧完整边界与最后一帧。
- Rust workspace、Clippy all-targets/all-features、协议再生、格式与配置范围类型门禁验收。
  全仓 mypy 的既有 991 项错误仍归 F29，不加 ignore、不缩小检查范围；compute 模块已加入
  配置门禁。最终测试计数以证据文件为准。

计算进程内状态尚无持久化 checkpoint。输入重连依赖同一进程仍持有正确状态，不能把输出
持久恢复等同于算子状态恢复；durable consumer 配置因此明确拒绝。服务重启后的输出会话
自动恢复、KV 状态输入适配和旧插件 source/Session/Pipeline 的完整迁移仍需继续完成。

F07/F08 目录与运行权威、F11 主包订阅与统一契约、F13/F14/F22 旧主包写入/查询/计算迁移、
F15 在线桥接退出、F28 支持平台/发行矩阵、F29 全仓类型检查保持开放。
