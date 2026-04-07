# Source Trait and ZmqSource Design

## Goal

为 Zippy 增加一套与 `Engine` / `Publisher` 对称的 `Source` 抽象，使一个进程里的
engine 输出可以作为另一个进程里的 engine 输入。

本设计面向如下拓扑：

- 进程 A：`CTPSource -> TimeSeriesEngine -> ParquetSink + ZmqPublisher`
- 进程 B：`ZmqSource -> CrossSectionalEngine -> ZmqPublisher`
- 进程 C：`ZmqSource -> ReactiveStateEngine -> ParquetSink`

目标不是实现“远程调用另一个 engine”，而是把跨进程链路建模为：

- `Source -> Engine -> Publisher`

其中：

- `Source` 负责接入外部事件流
- `Engine` 继续负责批处理计算
- `Publisher` 继续负责输出

## Scope

本次首版范围如下。

### 包含

- 新增 `Source` 抽象
- 新增 `SourceEvent`
- 新增事件驱动的 source runtime
- 新增 `ZmqSource`
- 新增 `SourceMode.PIPELINE`
- 新增 `SourceMode.CONSUMER`
- 单 endpoint、单 stream、固定 schema
- 单通道 ZMQ multipart envelope 协议
- `Hello / Data / Flush / Stop / Error` 控制事件
- Python 绑定与常量导出
- 让 `TimeSeriesEngine` / `CrossSectionalEngine` 可接受 `source=ZmqSource`

### 不包含

- 多 stream / topic 复用
- ACK / 重传 / exactly-once
- 动态 schema 更新
- Kafka / SHM / gRPC 等第二种远程 source
- 可靠控制通道拆分
- 分布式拓扑管理
- source 级状态恢复

## Motivation

当前代码里已经有两类边界：

- `Engine`
- `Publisher`

但是“数据从哪里进入 runtime”仍然只有两种方式：

- 用户手工 `write()`
- 进程内 `source=` 级联

这不足以表达“一个 engine 在一个进程里生产流，多个其他进程订阅它的输出继续处理”
这一类真实量化拓扑。

如果直接做“远程 engine 调用”，会把 transport、生命周期和计算耦合在一起。
更稳的建模是：

- 入口是 `Source`
- 中间是 `Engine`
- 出口是 `Publisher`

这样进程边界只落在 source / publisher 两侧，计算引擎本身不需要知道数据来自本地
还是远端。

## Core Principles

首版遵守以下原则：

- runtime 必须是事件驱动，而不是主动轮询 `recv()`
- 一条 endpoint 只承载一条 stream
- 一条 stream 只承载一份固定 schema
- 默认模式是 `pipeline`
- 同时支持 `consumer`
- 数据面允许丢包
- 控制事件必须进入协议，但首版不保证可靠必达
- `Source` 只负责接入与校验，不负责计算

## Architecture

新增后的总体结构如下：

```text
Source -> SourceRuntime -> Engine -> Publisher
```

其中：

- `Source` 把外部世界的事件推给 runtime
- `SourceRuntime` 把事件映射成本地 engine 生命周期
- `Engine` 继续使用现有 `on_data / on_flush / on_stop`
- `Publisher` 继续负责本地输出

### SourceRuntime

新增一条 runtime 启动路径：

```rust
spawn_source_engine_with_publisher(source, engine, config, publisher)
```

其职责是：

1. 启动 `Source`
2. 为 `Source` 提供一个线程安全的 `SourceSink`
3. 接收 `SourceEvent`
4. 将事件驱动到本地 `Engine`
5. 将 engine 输出交给本地 `Publisher`

这条路径与当前的 `spawn_engine_with_publisher(...)` 并存。

## Source Trait

首版 `Source` 相关抽象定义如下：

```rust
pub enum SourceMode {
    Pipeline,
    Consumer,
}

pub enum SourceEvent {
    Hello(StreamHello),
    Data(RecordBatch),
    Flush,
    Stop,
    Error(String),
}

pub trait SourceSink: Send + Sync + 'static {
    fn emit(&self, event: SourceEvent) -> Result<()>;
}

pub trait Source: Send + 'static {
    fn name(&self) -> &str;
    fn output_schema(&self) -> SchemaRef;
    fn mode(&self) -> SourceMode;
    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle>;
}
```

### Design Notes

- `Source` 不实现当前 `Engine` trait
- `Source` 是主动型入口节点
- `Engine` 保持被动处理模型
- `SourceHandle` 负责 source 生命周期收口

## SourceEvent Semantics

首版事件集合固定如下：

- `Hello(StreamHello)`
- `Data(RecordBatch)`
- `Flush`
- `Stop`
- `Error(String)`

### Hello

用于锁定：

- 协议版本
- stream 名称
- schema hash
- schema 本身

`Hello` 必须先于有效 `Data`。

### Data

承载 Arrow `RecordBatch`。

这是唯一允许 best-effort 丢包的数据事件。

### Flush

表示上游处理链显式发出了边界信号。

在 `pipeline` 模式下，它需要传播为本地 `engine.on_flush()`；
在 `consumer` 模式下，它默认不驱动本地 engine，只记观察性指标。

### Stop

表示上游处理链准备停止。

在 `pipeline` 模式下，它传播为本地 `engine.on_stop()`；
在 `consumer` 模式下，它默认只停止 source 接收，不自动关闭本地 engine。

### Error

表示上游 source / transport / protocol 已进入错误状态。

首版默认将本地 source runtime 置为 `failed`。

## Two Operating Modes

`ZmqSource` 支持两种模式。

### pipeline

默认模式。

语义为：

- “我是上游处理链的延续”

行为如下：

- `Hello`：必须先通过
- `Data`：驱动本地 `engine.on_data()`
- `Flush`：驱动本地 `engine.on_flush()`
- `Stop`：驱动本地 `engine.on_stop()` 并关闭 source
- `Error`：本地 runtime 进入 `failed`

适用场景：

- `TimeSeriesEngine -> CrossSectionalEngine`
- 希望上下游边界大体一致
- 希望在线和重放的边界语义尽量一致

### consumer

非默认模式。

语义为：

- “我是这个流的独立消费者”

行为如下：

- `Hello`：必须先通过
- `Data`：驱动本地 `engine.on_data()`
- `Flush`：默认忽略，仅记指标
- `Stop`：默认不自动关闭本地 engine，只停止 source
- `Error`：首版默认仍将本地 runtime 置为 `failed`

适用场景：

- 实时监控
- 研究脚本
- 松耦合多消费者

## ZmqSource

首版远程 source 具体实现为 `ZmqSource`。

### Scope

首版 `ZmqSource` 固定为：

- 单 endpoint
- 单 stream
- 固定 schema
- 单通道 envelope 协议

### Responsibilities

`ZmqSource` 负责：

- 建立订阅连接
- 解码 envelope
- 校验 `Hello`
- 校验 `RecordBatch` schema
- 将事件推给 `SourceSink`

`ZmqSource` 不负责：

- 执行 factor / aggregation / cross-sectional 计算
- 本地归档
- ACK / 重传
- stream 路由

## Single-Channel Envelope Protocol

首版采用：

- 一个 ZMQ socket
- 一条 stream
- multipart envelope

不拆成双通道。

### Multipart Layout

每条消息固定为：

- frame 0：`kind`
- frame 1：`meta`
- frame 2：`payload` 可选

### Kinds

固定支持：

- `HELLO`
- `DATA`
- `FLUSH`
- `STOP`
- `ERROR`

### HELLO

`meta` 至少包含：

- `protocol_version`
- `stream_name`
- `schema_hash`

`payload` 为 schema 的 Arrow IPC 表示。

### DATA

`meta` 至少包含：

- `seq_no`

`payload` 为 `RecordBatch` 的 Arrow IPC bytes。

### FLUSH

`meta` 至少包含：

- `seq_no`

无 `payload`。

### STOP

`meta` 至少包含：

- `seq_no`

无 `payload`。

### ERROR

`meta` 至少包含：

- `message`

无 `payload`。

## Hello and Schema Locking

`ZmqSource` 启动后，在收到第一条合法 `HELLO` 前，不接受有效 `DATA`。

`HELLO` 处理步骤如下：

1. 校验 `protocol_version`
2. 解析 `schema_ipc`
3. 对比 `schema_hash`
4. 与本地 `expected_schema` 严格比较
5. 通过后，source 状态切到 `ready`

如果不匹配：

- 直接进入 `failed`
- 后续 `Data` 不再交给本地 engine

首版不支持自动接受远端 schema，也不支持运行时 schema 漂移。

## Data Plane and Control Plane Semantics

你已确认的核心约束是：

- 行情数据允许丢包

因此首版定义为：

- `Data`：best-effort
- `Hello / Flush / Stop / Error`：也进入协议，但不承诺可靠必达

这意味着：

- `pipeline` 模式在 V1 是“边界传播尽力而为”
- 不是强一致屏障传播

为了让控制事件在 best-effort 单通道上尽量可用，首版补充两个约束：

- `HELLO` 在启动后周期性重发
- `FLUSH` 和 `STOP` 默认重复发送 3 次

这只是降低控制帧丢失概率，不等于可靠传输。

## Runtime Event Mapping

`SourceRuntime` 对事件的映射规则如下。

### pipeline mode

- `Hello`：锁定 schema 与协议版本
- `Data`：`engine.on_data(batch)`
- `Flush`：`engine.on_flush()`
- `Stop`：`engine.on_stop()`，然后关闭 source runtime
- `Error`：状态切到 `failed`

### consumer mode

- `Hello`：锁定 schema 与协议版本
- `Data`：`engine.on_data(batch)`
- `Flush`：忽略，仅记日志和指标
- `Stop`：停止 source，但不自动 stop engine
- `Error`：首版默认状态切到 `failed`

## Failure Semantics

首版失败分三类。

### Protocol Errors

包括：

- 缺失 `HELLO`
- 非法 `kind`
- `meta` 解码失败
- payload 结构错误
- schema 不匹配

处理：

- 直接 `failed`

### Transport Errors

包括：

- socket 断开
- 连接失败
- 解码底层 I/O 错误

处理：

- 首版可在 source 层做有限重连
- 若长时间未恢复，runtime 切到 `failed`

### Downstream Processing Errors

包括：

- 本地 `engine.on_data/on_flush/on_stop` 报错

处理：

- 与当前 runtime 一致，直接 `failed`

## Backpressure

首版 `ZmqSource` 不单独维护大 backlog。

原则是：

- source 收到事件后尽快推给 runtime
- 背压仍由本地 runtime 的队列负责

这样可以避免 source 再实现一套第二缓冲系统，从而保持行为一致：

- `buffer_capacity`
- `overflow_policy`

仍然是整条本地处理链的主要容量控制手段。

## Python API Shape

首版 Python API 如下：

```python
source = zippy.ZmqSource(
    endpoint="tcp://127.0.0.1:7001",
    expected_schema=bar_schema,
    mode=zippy.SourceMode.PIPELINE,
)

cs = zippy.CrossSectionalEngine(
    name="cs_1m",
    source=source,
    input_schema=bar_schema,
    id_column="symbol",
    dt_column="window_start",
    trigger_interval=zippy.Duration.minutes(1),
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[
        zippy.CS_RANK(column="ret_1m", output="ret_rank"),
    ],
    target=zippy.ZmqPublisher(endpoint="tcp://127.0.0.1:7002"),
)
```

新增常量：

```python
zippy.SourceMode.PIPELINE
zippy.SourceMode.CONSUMER
```

### API Notes

- `expected_schema` 必须显式提供
- `source=` 将从“只接受本地 engine”扩展为“接受本地 source 或远端 source”
- 首版重点支持 `TimeSeriesEngine` / `CrossSectionalEngine` 作为 `ZmqSource` 的下游
- `ReactiveStateEngine` 在这一轮仍只接受本地 source，不接受 `ZmqSource`
- Python 额外暴露一个 standalone `ZmqStreamPublisher`，用于测试、示例和远端发布入口

## Example Topologies

### Example 1: Upstream Stream Publisher with Multiple Subscribers

```text
进程 A:
  local producer / adapter -> ZmqStreamPublisher

进程 B:
  ZmqSource(pipeline) -> CrossSectionalEngine -> ZmqPublisher

进程 C:
  ZmqSource(consumer) -> TimeSeriesEngine -> ParquetSink
```

### Example 2: Cross-Process Fan-Out

```text
一个上游 engine 产出流
多个下游进程各自订阅
各自处理
各自发布或归档
```

这是首版必须支持的核心能力。

## Testing Strategy

首版测试至少包含：

### Rust Protocol Tests

- envelope 编解码
- `HELLO` 锁定逻辑
- schema mismatch fail-fast

### Rust Runtime Tests

- `pipeline` 模式下 `Flush` 驱动本地 `engine.on_flush()`
- `consumer` 模式下 `Flush` 不驱动本地 engine
- `Stop` 传播语义
- 控制帧重复发送下的幂等性

### Python End-to-End Tests

- `TimeSeriesEngine -> ZmqPublisher`
- 另一进程 / runtime 中 `ZmqSource -> CrossSectionalEngine`
- `pipeline` 模式下边界传播
- `consumer` 模式下忽略 `Flush`

## Out of Scope Follow-Ups

本设计之后最自然的后续扩展是：

- ACK / 重传
- 可靠控制平面
- 多 stream / topic
- `KafkaSource`
- `ShmSource`
- source 级快照与恢复

但这些都不进入当前首版。

## Summary

本设计的核心结论是：

- 不做“远程 engine 调用”
- 做 `Source -> Engine -> Publisher`
- `ZmqSource` 是首个远程 `Source`
- runtime 必须事件驱动
- 默认 `pipeline`，同时支持 `consumer`
- 单 endpoint、单 stream、固定 schema
- 单通道 envelope，数据 best-effort，控制事件进入协议但不承诺可靠必达

这足以支持：

- 一个上游 engine 在一个进程里生产流
- 多个下游进程直接订阅它的输出
- 各自继续处理并再次发布
