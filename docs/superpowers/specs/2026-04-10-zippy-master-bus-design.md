# Zippy Master and Bus Design

## Goal

为 Zippy 增加一个面向单机多进程数据中心场景的控制平面与本机总线：

- `zippy-master`：独立常驻 daemon，负责控制面与元数据管理
- `zippy-bus`：`master` 内部持有的数据面组件，负责本机共享内存流传输

目标场景如下：

- 一个上游 source/engine 进程写入原始流，如 `openctp_ticks`
- 多个下游 engine 进程从同一个逻辑 stream 读取并分别计算
- 系统统一知道：
  - 有哪些 source / engine / sink / stream
  - 每个 engine 依赖哪里数据
  - 发布到哪里
  - 保存到哪里

本设计优先解决：

- 单机多进程
- 单写者、多读者
- 单逻辑 stream 统一命名
- 不再“一 stream 一个端口”

## Scope

### 包含

- 新增独立 `zippy-master` daemon
- `zippy-master` 内嵌 `zippy-bus` 组件
- 控制面使用 Unix domain socket
- 每个逻辑 `stream/table` 对应一个共享内存 ring buffer
- 单写者、多读者、每个 reader 独立游标
- worker 进程内单个 `MasterClient`
- `MasterClient.write_to(stream_name) -> Writer`
- `MasterClient.read_from(stream_name) -> Reader`
- source / engine / sink / stream 元数据注册表
- 基础控制接口：
  - 注册
  - 查询
  - 状态更新
  - 基础 start / stop
- V1 优先接通：
  - `StreamTableEngine -> bus stream`
  - `bus stream -> ReactiveStateEngine`

### 不包含

- 多机部署
- 高可用 / 选主
- 历史回放与恢复
- 自动系统级进程调度
- 配置热更新
- 鉴权 / 权限系统
- UI
- 所有 engine 一次性全部迁移到 bus

## Motivation

当前远端链路建立在 `ZmqStreamPublisher` / `ZmqSource` 之上，存在两个核心问题：

1. 数据面复用差  
每个发布者基本都要单独持有一个 publisher 与 endpoint，不利于单机多流数据中心场景。

2. 控制面缺失  
系统没有统一的位置回答这些问题：

- 现在有多少 source / engine / sink
- 每个 engine 依赖哪个上游 stream
- 每个 stream 由谁写入
- 数据保存到哪里
- 当前状态和 heartbeat 如何

这和 DolphinDB 的流表体验有明显差距。  
本设计通过 `master + bus` 分层解决：

- `bus` 只负责数据传输
- `master` 负责控制、注册、查询、状态和基础编排

## Core Principles

- `zippy-master` 是独立常驻进程
- `zippy-bus` 不单独做 daemon，只作为 `master` 内部组件
- 控制面和数据面职责严格分离
- 数据面优先为单机多进程优化
- 逻辑对象是 `stream/table name`，不是 engine 实例名
- 一个 stream 同时只能有一个活跃 writer
- 一个 stream 可以有多个 reader
- 每个 reader 都有独立读游标
- 慢 reader 不得拖慢 writer
- reader 落后导致覆盖时必须显式失败，不能静默跳过

## Architecture

总体结构如下：

```text
zippy-master (single daemon)
  ├─ control plane
  │   ├─ unix domain socket server
  │   ├─ registry
  │   ├─ process/source/engine/sink/stream metadata
  │   └─ start/stop/status API
  └─ bus component
      ├─ shm allocator
      ├─ per-stream ring buffer manager
      ├─ writer/reader attachment
      └─ stream metadata mirror

worker process
  ├─ runtime supervisor
  │   └─ MasterClient (one per process)
  ├─ sources
  ├─ engines
  └─ sinks
```

### zippy-master

`zippy-master` 负责：

- 启动并持有 `bus`
- 维护 registry
- 处理控制面请求
- 给 worker 分配 `Writer/Reader` 所需 descriptor
- 跟踪 process / source / engine / sink / stream 状态

### zippy-bus

`bus` 只负责数据面：

- 每个 stream 一段共享内存
- 每个 stream 一个 ring buffer
- writer 顺序写入
- reader 按各自游标读取

`bus` 不负责：

- 启动 engine
- 管理 DAG 配置
- 做跨机路由
- 做策略编排

### MasterClient

每个 worker 进程只持有一个 `MasterClient`，由 runtime supervisor 统一持有并复用。

`MasterClient` 负责：

- 进程注册
- source / engine / sink / stream 注册
- heartbeat
- 状态更新
- `write_to(stream_name)`
- `read_from(stream_name)`

业务组件本身不直接管理控制面 socket 连接。

## Control Plane Object Model

V1 注册表包含以下对象。

### ProcessRecord

一个 worker 进程一条：

- `process_id`
- `pid`
- `app`
- `started_at`
- `last_heartbeat_at`
- `status`

### SourceRecord

一个 source 一条：

- `source_name`
- `source_type`
- `process_id`
- `output_stream`
- `config`
- `status`
- `metrics`

### EngineRecord

一个 engine 一条：

- `engine_name`
- `engine_type`
- `process_id`
- `input_stream`
- `output_stream`
- `sink_names`
- `config`
- `status`
- `metrics`

### SinkRecord

一个 sink 一条：

- `sink_name`
- `sink_type`
- `process_id`
- `input_stream`
- `config`
- `status`

### StreamRecord

一个逻辑 stream 一条：

- `stream_name`
- `schema`
- `ring_capacity`
- `writer_process_id`
- `writer_owner`
- `reader_count`
- `write_seq`
- `status`

## Control Plane API

V1 至少提供以下控制接口：

- `register_process(app) -> process_id`
- `heartbeat(process_id)`
- `register_stream(stream_name, schema, ring_capacity)`
- `register_source(...)`
- `register_engine(...)`
- `register_sink(...)`
- `update_status(kind, name, status, metrics?)`
- `write_to(stream_name) -> WriterDescriptor`
- `read_from(stream_name) -> ReaderDescriptor`
- `list_streams()`
- `get_stream(stream_name)`
- `list_engines()`
- `get_engine(engine_name)`
- `list_sources()`
- `get_source(source_name)`
- `stop_process(process_id)` 或等价的细粒度停止接口

其中：

- `write_to/read_from` 是控制面准入动作，不是热路径数据操作
- 真正的持续 `write/read` 发生在 `Writer/Reader` handle 上

## Data Plane Semantics

### Stream Model

每个逻辑 `stream/table`：

- 一个共享内存段
- 一个 ring buffer
- 一个活跃 writer
- 多个 reader

### Writer Semantics

`write_to(stream_name)` 通过后返回 `Writer`。

`Writer` 暴露：

- `write(batch)`
- `flush()`
- `close()`

规则：

- 单个 stream 只允许一个活跃 writer
- 第二个 writer 请求同一 stream 时直接失败
- 不做多写者合并

### Reader Semantics

`read_from(stream_name)` 返回 `Reader`。

`Reader` 暴露：

- `read(timeout_ms=None)`
- `seek_latest()`
- `close()`
- 可选：
  - `lag()`
  - `stats()`

每个 reader 都有独立 `read_seq`，互不影响。

### Slow Reader / Overwrite Policy

writer 推进全局单调 `write_seq`。

如果某个 reader 落后太多，导致其尚未读取的数据已被 ring 覆盖：

- 下一次 `read()` 必须显式失败
- 失败类型应能区分为“reader lagged”而不是普通 EOF
- reader 不允许静默跳过
- 调用方必须：
  - `seek_latest()`
  - 或重新 `read_from(stream_name)`

这条规则是 V1 的关键语义：

- writer 不能被慢 reader 拖慢
- 但 reader 丢数也不能被隐藏

## API Shape

### Master daemon

`zippy-master` 是独立进程，不是嵌入式对象。

### Worker-side client

worker 进程内部使用 `MasterClient`，但默认由 runtime supervisor 持有。

对外能力分两层：

#### 控制面

- 注册 source / engine / sink / stream
- heartbeat
- status update
- 查询 registry

#### 数据面 handle 获取

```python
writer = master_client.write_to("openctp_ticks")
reader = master_client.read_from("openctp_mid_price_factors")
```

随后通过 handle 执行热路径：

```python
writer.write(batch)
writer.flush()
writer.close()

batch = reader.read(timeout_ms=1000)
reader.seek_latest()
reader.close()
```

## Ownership Model

### Who owns MasterClient

- 每个 worker 进程一个 `MasterClient`
- 由 runtime supervisor 统一持有
- 进程内所有 source / engine / sink 共用

### Who owns Writer / Reader

- `Writer` 由输出侧 runtime / publisher 层持有
- `Reader` 由输入侧 runtime / source 层持有
- 业务组件不直接管理控制面连接

## V1 Integration Targets

V1 先只接两条关键链路：

1. `StreamTableEngine -> bus stream`
2. `bus stream -> ReactiveStateEngine`

这样可以先覆盖最贴近生产的数据中心主链：

- `OpenCtpMarketDataSource -> StreamTableEngine -> openctp_ticks`
- `ReactiveStateEngine <- openctp_ticks`

`TimeSeriesEngine` / `CrossSectionalEngine` 后续再逐步迁移，不要求首版一步到位。

## Error Handling

### Control Plane Errors

- 重复注册 stream
- stream schema 不匹配
- 第二个 writer 竞争同一 stream
- 不存在的 stream

这些错误必须在控制面直接返回，不允许进入数据面后再失败。

### Data Plane Errors

- reader lagged
- shm 映射失败
- ring descriptor 无效
- writer/reader 已关闭

这些错误必须精确区分，不能统一成模糊 IO 错误。

## Testing Strategy

V1 需要至少覆盖：

1. `master` daemon 能启动并监听 Unix socket
2. `register_process/register_stream` 正常
3. `write_to/read_from` 能建立 writer/reader
4. 单写者约束生效
5. 多 reader 独立游标生效
6. 慢 reader 覆盖后显式失败
7. `StreamTableEngine -> bus stream`
8. `bus stream -> ReactiveStateEngine`
9. registry 能回答：
   - 有哪些 stream
   - 哪个 engine 依赖哪个 stream
   - 哪个 sink 挂在哪个 engine 上

## Out of Scope for V1

以下能力明确不在本次实现范围内：

- 多机 master
- HA / failover
- 自动恢复历史数据
- replay / snapshot
- 全部 engine 一次性切到 bus
- UI 管控台
- 鉴权系统
- 热更新配置
- 系统级 supervisor 替代品

## Recommended Delivery Order

建议实现顺序如下：

1. `zippy-master` daemon + Unix socket 控制面
2. 内嵌 `bus` 组件 + per-stream shm ring
3. `MasterClient`
4. `Writer/Reader` handle
5. `StreamTableEngine -> bus`
6. `bus -> ReactiveStateEngine`
7. `zippy-openctp` 示例改成走 master/bus

## Summary

V1 的核心不是“做一个很重的 master”，而是：

- 用 `zippy-master` 统一承载控制面
- 在内部持有只负责数据传输的 `zippy-bus`
- 用共享内存 per-stream ring 取代“一 stream 一个端口”
- 用 `MasterClient + Writer/Reader` 建立单机多进程的数据中心总线模型

这能先把当前最痛的问题解决掉，同时为以后扩展完整控制平面留下清晰边界。
