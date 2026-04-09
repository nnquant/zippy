# StreamTableEngine Design

## Goal

新增一个一等的 `StreamTableEngine`，用于承载“原始流数据中心”场景：

- 从任意 `Source` 接收显式 schema 的原始流
- 原样透传输入 `RecordBatch`
- 同时把原始流发布给下游 `target`
- 同时把原始流写入 `sink`

该引擎面向如下生产拓扑：

- `OpenCtpMarketDataSource -> StreamTableEngine -> ZmqStreamPublisher`
- `OpenCtpMarketDataSource -> StreamTableEngine -> ParquetSink`
- `OpenCtpMarketDataSource -> StreamTableEngine -> ZmqStreamPublisher + ParquetSink`

它的角色更接近 DolphinDB 的流数据表入口，而不是一个不做计算的 `ReactiveStateEngine`。

## Scope

### 包含

- 新增 `StreamTableEngine`
- 输入 schema 原样透传
- 支持 `source=...`
- 支持 `target=...`
- 支持 `sink=...`
- `flush/stop` 标准传播
- Python 绑定
- 最小 example 与测试

### 不包含

- 系统列自动追加
- latest view / keyed table
- replay
- snapshot / recovery
- mmap
- 自定义 log segment
- 查询接口
- 动态 schema

## Motivation

当前仓库中最接近“原始流表”的对象是 `ReactiveStateEngine(factors=[])`。  
它在行为上接近 pass-through，但职责不对：

- 名字表达的是 reactive factor 计算
- 仍带着 `id_column` 等 reactive 语义
- 后续若加 durable log、流表指标、原始流 example，会越来越别扭

`StreamTableEngine` 需要成为一个独立概念：

- 它不做列计算
- 它不改变 schema
- 它只表达“原始流接入、发布、落地”

## Core Principles

- `output_schema() == input_schema()`
- `on_data(batch)` 原样返回 `batch`
- 不追加系统列
- 不改变 batch 边界
- `target` 负责流式发布
- `sink` 负责落地/归档
- `StreamTableEngine` 自身不维护 keyed state / window state

## API

Python API 首版形状如下：

```python
table = zippy.StreamTableEngine(
    name="ctp_ticks",
    input_schema=zippy_openctp.schemas.TickDataSchema(),
    source=zippy_openctp.OpenCtpMarketDataSource(...),
    target=zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:7001",
        stream_name="ctp_ticks",
        schema=zippy_openctp.schemas.TickDataSchema(),
    ),
    sink=zippy.ParquetSink(
        path="data/ctp_ticks",
        write_input=True,
    ),
    buffer_capacity=8192,
    overflow_policy=zippy.OverflowPolicy.BLOCK,
)
```

### Parameter Semantics

- `name`
  - engine 实例名
- `input_schema`
  - 显式输入 schema
- `source`
  - 可选 source，支持现有 `source=` 语义
- `target`
  - 可选发布侧 publisher
- `sink`
  - 可选落地侧 sink
- `buffer_capacity`
  - runtime 队列容量
- `overflow_policy`
  - runtime 溢出策略

## Behavior

### on_data

`StreamTableEngine.on_data(batch)` 的行为固定为：

1. 校验输入 schema 与 engine `input_schema` 一致
2. 不做任何列变换或状态更新
3. 原样返回 `vec![batch]`

也就是说，它不产生衍生列，也不改变行数和列顺序。

### on_flush

`on_flush()` 默认返回空输出：

- 不凭空生成 batch
- 但 runtime 仍需要对 `target` 和 `sink` 执行 flush

### on_stop

`on_stop()` 默认返回空输出：

- 不额外生成停机输出
- 但 runtime 仍需要完成 source stop、publisher close、sink close

## Data Flow

`StreamTableEngine` 的数据流固定如下：

```text
Source -> StreamTableEngine -> target
                         └-> sink
```

首版不改变现有 runtime 分工：

- `StreamTableEngine` 只负责返回原始 batch
- `target` 继续由 publisher 链路处理
- `sink` 继续由 Python 层 archive/sink 链路处理

## Source / Target / Sink Semantics

### source

- 可选
- 首版必须同时支持：
  - live source，例如 `OpenCtpMarketDataSource`
  - 手工 `write(...)`

### target

- 可选
- 用于原始流继续发布
- 典型对象：
  - `NullPublisher`
  - `ZmqStreamPublisher`

### sink

- 可选
- 用于原始流归档或落地
- 首版只先对接现有 `ParquetSink`
- 但接口命名固定为通用的 `sink`，不给未来扩展设限

## Flush / Stop Semantics

首版 `StreamTableEngine` 遵守现有 source-engine-publisher 生命周期。

### flush

- `StreamTableEngine` 本身不生成新数据
- 但必须允许 runtime 把 flush 传播给：
  - `target`
  - `sink`
  - 下游 pipeline

### stop

停机顺序保持当前 runtime 约束：

1. source 先停
2. engine worker 收口
3. `target.close()`
4. `sink.close()`

这保证原始流在停机时不会被静默丢失尾部边界。

## Output Schema

首版输出 schema 规则非常简单：

- `output_schema() == input_schema()`

不追加：

- `ingest_time`
- `sequence_id`
- `source_name`

这些若未来需要，另做可选增强，不进入首版。

## Implementation Plan

实现按三层拆：

### zippy-engines

新增：

- `crates/zippy-engines/src/stream_table.rs`

内容：

- `StreamTableEngine`
- `Engine` trait 实现

### zippy-python

新增 Python 绑定：

- `StreamTableEngine`

职责：

- 复用现有 runtime / source / target / sink 绑定模式
- 参数形状与其他 engine 保持一致

### examples / tests

新增最小 example：

- `OpenCtpMarketDataSource -> StreamTableEngine -> ZmqStreamPublisher`
- `OpenCtpMarketDataSource -> StreamTableEngine -> ParquetSink`

## Testing

首版至少覆盖以下测试。

### Rust

1. `on_data()` 原样透传输入 batch
2. `output_schema() == input_schema()`
3. `flush()` 不生成 batch
4. `stop()` 不生成 batch
5. schema mismatch 仍被拒绝

### Python

1. `StreamTableEngine` 可构造
2. `output_schema()` 与 `input_schema()` 一致
3. `source + target` 可接通
4. `source + sink` 可接通
5. `source + target + sink` 可接通

## Example

最小生产化示例：

```python
table = zippy.StreamTableEngine(
    name="ctp_ticks",
    source=zippy_openctp.OpenCtpMarketDataSource(...),
    input_schema=zippy_openctp.schemas.TickDataSchema(),
    target=zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:7001",
        stream_name="ctp_ticks",
        schema=zippy_openctp.schemas.TickDataSchema(),
    ),
    sink=zippy.ParquetSink(
        path="data/ctp_ticks",
        write_input=True,
    ),
)

table.start()
```

这个 example 表达的就是：

- 上游 tick 进入流表
- 原始 tick 被保存
- 同一份原始 tick 被继续分发

## Non-Goals

本设计明确不把 `StreamTableEngine` 做成：

- DolphinDB keyed stream table 的完整复刻
- 查询引擎
- durable log 系统
- mmap 高速共享层

这些能力如果后续要做，应在 `StreamTableEngine` 之上继续扩展，而不是首版一次塞入。
