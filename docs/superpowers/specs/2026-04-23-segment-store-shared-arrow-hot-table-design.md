# Segment Store Shared Arrow Hot Table Design

## 背景

当前 `zippy` 的实时链路以 `RecordBatch + Arrow IPC + bus` 为主数据面：

- source 将原始行情规范化为 `RecordBatch`
- runtime / publisher 将 batch 编码后写入 bus
- reader 再解码为 batch
- 下游算子继续围绕 batch 流执行

这条路径的优点是通用，但对高频逐 tick 计算存在两个结构性问题：

1. 原始 tick 在 source 进入系统后会经历多次中间物化、编码、解码和批流分发。
2. 多个进程、多个算子虽然都在消费同一份基础数据，但共享的是消息流，不是共享的列式热表。

本设计的目标不是“继续优化 batch/bus 路径”，而是为 `OpenCTP ticks` 新增一条并行的数据主线：

- source 进入系统时只做一次物化
- 物化结果直接写入共享 Arrow-compatible 热表
- 同机多进程 reader 在这份基础热表上零拷贝读取
- 逐 tick 算子按增量方式执行
- sealed segment 异步刷 Parquet

这是一条新的 `segment runtime` 栈，不替换现有 `RecordBatch` 路径，但要为后续是否迁移提供真实性能和架构证据。

## 目标

- 为 `OpenCTP ticks` 建立一条 `Shared Arrow Hot Table` 垂直链路。
- source 将 tick 直接写入共享内存中的 Arrow-compatible 列缓冲，避免热路径上的 Arrow IPC encode/decode。
- 支持单机多进程 reader 对同一份基础热表做零拷贝读取。
- 支持逐 tick 增量因子计算，而不是每 tick 重扫一遍表。
- 通过 `active segment + committed_row_count + eventfd` 保证实时性与一致性。
- sealed segment 异步落盘为 Parquet，且不阻塞热路径。
- 保留 `Arrow schema / arrays / RecordBatch / IPC / Parquet` 的边界兼容能力。

## 非目标

- 不在第一版中支持任意动态 schema。
- 不支持 update / delete / retract / correction。
- 不支持多 writer 并发写入同一个 active segment。
- 不实现 SQL、查询优化器、统一历史查询引擎。
- 不实现复杂 compaction、冷热分层或多副本。
- 不让 Python 成为逐 tick 热路径上的主执行环境。
- 不在第一版中替换现有 `zippy-core` batch/runtime/bus 全部路径。

## 第一版范围

第一版范围严格限定为：

- 数据域：`OpenCTP ticks`
- 写入模型：append-only
- 运行形态：单机多进程
- 数据面：共享内存 Arrow-compatible 热表
- 计算形态：逐 tick 增量算子
- 持久化：sealed segment 异步写 Parquet

不处理：

- 通用数据库查询能力
- 通用多租户 schema 平台
- 所有 source/engine 全量迁移

## 设计选项

### 方案 1：继续以 `RecordBatch` 为中心优化现有路径

做法：

- 保留 `source -> RecordBatch -> IPC -> bus -> RecordBatch`
- 只优化 writer encode、reader decode、dispatch 等热点

优点：

- 改动小，兼容现有系统最好。

缺点：

- 不能实现“source 只物化一次，后续共享同一份基础热表”。
- 多算子、多进程依然共享的是消息流，不是共享热表。
- 后续扩展到共享基础表、零拷贝窗口因子会越来越别扭。

### 方案 2：新增 `segment runtime`，以共享 Arrow-compatible 热表作为主数据面（推荐）

做法：

- source 直接写入共享内存列缓冲
- active segment 通过 `committed_row_count` 向 reader 暴露已提交前缀
- reader 使用 `eventfd` 获知新增数据
- 算子消费 `RowSpanView`，而不是 batch 消息
- sealed segment 异步写 Parquet

优点：

- 最符合“单次物化、零拷贝基础热表、逐 tick 增量计算”的目标。
- 保留 Arrow 物理模型和边界兼容能力。
- 可与现有 batch 路径并行存在，迁移风险可控。

缺点：

- 需要新增一套 runtime 抽象：segment、view、lifecycle、notify、operator runtime。
- 需要自己设计共享内存生命周期和进程内调度，而不是复用现有 batch/bus 心智。

### 方案 3：仍以 Arrow IPC blob 作为共享内存中的主格式

做法：

- 每个共享对象直接存 Arrow IPC payload
- reader attach 后按 IPC 解析

优点：

- 更贴近标准 Arrow 交换格式。
- 复用现有 IPC reader/writer 逻辑更容易。

缺点：

- 热路径上仍会保留较重的 message/container 心智。
- 生命周期、引用计数和回收问题并不会因为 IPC blob 而自动消失。
- 不如 raw Arrow-compatible buffers 适合逐 tick 热层。

## 采用方案

采用方案 2。

系统内部以 `Shared Arrow Hot Table` 为主数据面，Arrow IPC / `RecordBatch` 降为边界兼容层，不再作为热路径主载体。

## 核心设计

### 1. Arrow 在系统中的位置

本设计不放弃 Arrow，而是改变 Arrow 在系统中的位置：

- 保留：
  - Arrow 列式物理布局
  - schema 语义
  - `ArrayData` / `RecordBatch` 视图能力
  - Parquet 导出能力
- 弱化：
  - Arrow IPC 作为热路径内部主格式
  - `RecordBatch` 作为热路径主消息对象

换句话说：

- 内部 runtime 以 `segment/view` 为中心
- 外部兼容层继续提供 `RecordBatch / IPC / Parquet`

### 2. 数据面主线

第一版主线固定为：

`OpenCTP source -> segment ingress writer -> active segment -> committed_row_count -> eventfd -> reader task -> RowSpanView -> operators -> sealed segment -> parquet`

其中：

- 共享的跨进程数据面只有 `active/sealed segment`
- 进程内 fanout 的对象是 `RowSpanView`
- `RecordBatch` 不再作为这条主线的主对象

### 3. Active Segment 与 Sealed Segment

每个 `stream/partition` 在任一时刻有：

- 一个 `active segment`
- 零到多个 `sealed segment`

语义如下：

- `active segment`
  - 只有一个 writer
  - 允许 reader 零拷贝读取已提交前缀
  - 通过 `committed_row_count` 暴露可见范围
- `sealed segment`
  - 不可变
  - 供复杂窗口、回放、持久化使用
  - 可在无引用后回收

这意味着第一版不是“只公开 sealed segment”，而是：

- 实时性依赖 `active segment`
- 历史和持久化依赖 `sealed segment`

### 4. Segment 的物理布局

每个 `SharedArrowSegment` 使用：

- `SegmentHeader`
- `ColumnDescriptor[column_count]`
- `PayloadBuffers`

`PayloadBuffers` 中的列缓冲遵循 Arrow-compatible 物理布局：

- fixed-width 列：`values`
- nullable 列：`validity bitmap`
- utf8 列：`offsets + values`

不采用 Arrow IPC blob 作为 segment 本体。

`SegmentHeader` 至少包含：

- `magic`
- `format_version`
- `schema_id`
- `segment_id`
- `generation`
- `sealed`
- `row_count`
- `column_count`
- `capacity_rows`
- `committed_row_count`
- `payload_bytes`
- `min_dt_ns`
- `max_dt_ns`
- `min_localtime_ns`
- `max_localtime_ns`

`ColumnDescriptor` 至少包含：

- `column_id`
- `type_tag`
- `null_count`
- `validity_offset`
- `validity_len`
- `offsets_offset`
- `offsets_len`
- `values_offset`
- `values_len`

### 5. Schema 模型

第一版不支持任意动态 schema，但也不要求永久手写硬编码。

采用：

- `columns=[...]` 作为 schema 输入接口
- `compile_schema(columns)` 作为一次性编译步骤

`compile_schema(columns)` 产出：

- `schema_id`
- 物理布局 plan
- typed builder factory
- view factory
- Arrow bridge metadata

关键边界：

- schema 列表可以自动构建
- 但热路径不能每次 append 时按 schema 列表逐列动态解释

因此：

- 控制面保留 schema 列表式接口
- 热路径只运行“编译后的专用路径”

### 6. Builder 模型

第一版 builder 采用：

- 专用物理路径
- 可生成的样板

含义是：

- source 写入时直接写共享内存中的最终列缓冲
- seal 时不再二次拷贝
- 对同一 schema 家族的样板代码可生成，而不是每次从零手写

第一版以 `OpenCTPTickV1` 为中心，但不把未来 schema 演进锁死。

builder 内至少维护：

- `row_cursor`
- 每列写指针或已用字节数
- 每列 `null_count`
- `min/max dt`
- `min/max localtime`
- segment 打开时间

seal 只做：

- 回填 header
- 固定统计值
- 标记 `sealed=true`
- 生成 descriptor

seal 不再复制 payload。

### 7. 提交协议

active segment 采用：

- 单 writer
- append-only
- `committed_row_count` 前缀提交协议

writer 顺序：

1. 选择尾行 `row`
2. 把该行所有列写入对应 buffer
3. 写完 utf8 offsets / validity / values
4. 更新局部统计
5. 最后以 `store-release` 推进 `committed_row_count = row + 1`

reader 顺序：

1. 用 `load-acquire` 读取 `committed_row_count`
2. 只访问 `row < committed_row_count` 的前缀
3. 处理 `[cursor, committed_row_count)` 区间

这样可以保证：

- reader 零拷贝读取
- reader 不会看到半行

### 8. 通知机制

第一版通知机制固定为 `eventfd`。

设计原则：

- `committed_row_count` 负责“哪些行可见”
- `eventfd` 负责“何时唤醒 reader”
- 可见性和通知严格分离

推荐模型：

- 每个 `stream/partition` 一个数据通知源
- 每个进程的 reader task 用 `epoll` 或等价机制统一等待多个 eventfd
- active segment rollover 也通过 eventfd 通知

第一版不引入新的跨进程消息总线；eventfd 仅为通知通道，不承载数据本体。

### 9. Reader 与 Operator Runtime

进程内不让每个算子各自直接读 `SegmentStore`。

采用分层：

- `ReaderTask`
  - 监听某个 partition 的 eventfd
  - attach active segment
  - 维护 cursor
  - 构造新增区间的 `RowSpanView`
- `LocalDispatcher`
  - 将同一个 `RowSpanView` 分发给该 partition 的多个算子
- `PartitionWorker`
  - partition 内串行执行算子
  - 保证顺序与 cache locality
- `Operator`
  - 声明 `required_columns`
  - 实现 `on_rows(span)`

不允许：

- 每个算子自己监听 eventfd
- 每个算子自己维护独立 cursor
- 同一进程内重复 attach / 重复读取同一个 partition

### 10. 算子模型

第一版算子主接口采用：

- `on_rows(span: RowSpanView)`

而不是：

- 查询式“每 tick 重扫表”
- `RecordBatch` 消息驱动

推荐模型是：

- reader 读取 `[cursor, committed)` 这段新增前缀
- 构造成零拷贝 `RowSpanView`
- 算子按新增行增量更新本地状态

边界如下：

- 基础热表读是零拷贝
- 算子内部状态不是零拷贝对象，可以维护本地 ring/deque/accumulator
- 简单窗口因子优先维护本地标量状态
- 复杂窗口因子可保存 `segment_handle + row_range`

### 11. 生命周期与回收

生命周期管理不采用“纯共享内存原子 refcount”。

第一版采用混合模型：

- 一个中心 `SegmentStore owner`
- 跨进程 `session + lease`
- 进程内本地 `Arc`

owner 负责：

- 分配共享内存 region/slab
- 维护 `segment_id + generation`
- 管理 reader session / heartbeat
- 跟踪 segment lease
- 判定何时 reclaim

推荐状态机：

- `building`
- `active`
- `sealed`
- `retired`
- `reclaimable`
- `freed`

回收条件至少包括：

- segment 已 `retired`
- `pin_count == 0`
- 如启用持久化则已持久化确认
- 满足一个短的 grace period

`generation` 必须参与 descriptor 和 lease，避免旧 descriptor attach 到已复用槽位。

### 12. 与 Arrow / RecordBatch 的兼容边界

`RecordBatch` 在第一版中保留，但只作为兼容层对象，不是热路径主对象。

应保留 `RecordBatch` 的地方：

- 兼容旧 engine / 旧 API
- 调试与诊断
- IPC 导出
- Parquet 写出
- 少量复用 Arrow compute 的桥接场景

不应保留 `RecordBatch` 作为主对象的地方：

- source 到热表写入
- 热路径跨进程分发
- eventfd 唤醒后的逐 tick 增量读取
- 多算子共享基础数据

## 模块拆分

### `crates/zippy-segment-store`

负责共享热表数据面，建议至少包含：

- `schema`
- `layout`
- `shm`
- `builder`
- `segment`
- `catalog`
- `notify`
- `lifecycle`
- `view`
- `arrow_bridge`
- `persistence`

职责：

- 管理共享内存 segment
- 管理提交可见性和通知
- 提供零拷贝 view
- 提供 Arrow / Parquet 边界桥接

### `crates/zippy-operator-runtime`

负责进程内算子执行层，建议至少包含：

- `reader`
- `dispatcher`
- `partition_runtime`
- `operator`
- `state`
- `result`

职责：

- 监听 eventfd
- 维护 partition cursor
- 分发 `RowSpanView`
- 执行增量算子

### `plugins/zippy-openctp`

第一版职责：

- 将 OpenCTP tick 规范化为热表行
- 选择 partition
- 写入 `MutableSegmentBuilder`

不再把 `RecordBatch` 作为这条主路径上的主输出对象。

### `crates/zippy-python`

第一版只提供：

- 控制面与配置桥
- 调试与只读桥
- 可选的 `RecordBatch` 导出桥

不把 Python 放入逐 tick 热路径核心。

## 与现有系统的关系

本设计是并行新增一条 `segment runtime` 栈，而不是重写整个 `zippy`。

现有：

- `zippy-core` batch/runtime/bus
- `RecordBatch` 主路径
- 相关 Python API

全部继续保留。

第一版只验证：

- `OpenCTP ticks` 是否适合改走共享热表
- 多进程零拷贝读是否成立
- 逐 tick 增量因子是否能显著优于 batch/bus 路径

## 第一版明确拒绝的能力

第一版明确不做：

- 任意动态 schema
- 多 writer 同写一个 active segment
- update / delete / retract
- Python 逐 tick 主执行
- 自由查询引擎
- 复杂 compaction / tiering
- 全系统替换

## 测试与验证策略

第一版应至少覆盖以下验证层次。

### 单元测试

- schema 编译与布局 plan 正确
- builder 对 fixed-width / utf8 / nullable 列写入正确
- `committed_row_count` 协议下 reader 看不到半行
- eventfd 通知与 rollover 语义正确
- `segment_id + generation` 生命周期边界正确

### 进程内集成测试

- 单进程 source 写入、reader 增量读取
- 多算子共享同一 `RowSpanView`
- 简单 rolling/EMA 因子增量更新正确

### 多进程集成测试

- 一个 writer，多个 reader 零拷贝 attach 同一 active/sealed segment
- 一个 reader 进程异常退出后，owner 能清理 session/lease
- sealed segment 在所有 lease 释放后可回收

### 性能验证

至少比较以下指标：

- source ingress 到 reader 看到新行的延迟
- 单进程多算子共享读取的 CPU 开销
- 当前 batch/bus 路径与新 segment 路径的逐 tick 因子延迟对比
- Parquet flush 对热路径的干扰

## 成功标准

第一版成功标准定义为：

- `OpenCTP source` 在 ingress 时只对原始 tick 做一次基础物化
- 至少一个外部 reader 进程可零拷贝 attach 并逐 tick 增量读取
- 同一进程多个算子共享一次读取结果
- `active segment + committed_row_count + eventfd` 语义稳定可验证
- sealed segment 可异步写 Parquet，且不阻塞热路径
- 与当前 batch/bus 路径相比，核心链路延迟更低且更稳定

## 范围判断

这份 spec 不是“通用实时数据库设计”，而是一个明确收缩后的第一版垂直切片：

- `OpenCTP ticks`
- 单机多进程
- 单 writer
- append-only
- 共享 Arrow-compatible 热表
- 增量算子
- 异步 Parquet

基于此，后续实现计划应继续保持垂直切片，不应在第一版中引入查询层、复杂 compaction 或跨所有 source 的统一迁移。
