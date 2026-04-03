# Zippy V1 设计文档：单机流式因子计算框架

> 面向量化研究与交易的单机流式因子计算框架。  
> Rust 负责运行时、算子与 I/O，Python 负责定义、启动、写入与订阅。  
> 本文是可直接开工的 V1 设计，优先保证语义闭合、结果可复现、范围可交付。

---

## 1. 设计目标与边界

### 1.1 定位

Zippy 是一个嵌入式流式计算框架，运行在用户进程内，不依赖常驻控制面。
它面向量化场景中最常见的两类任务：

- 逐条更新的有状态因子计算
- 按时间窗口聚合的实时 bar 合成

V1 不追求替代 DolphinDB 的全部能力，而是先交付一条可靠的单机快速路径：

```
写入 Arrow/Polars 数据 -> Rust 引擎计算 -> 发布到下游 / 落盘
```

### 1.2 V1 目标

V1 只承诺以下能力：

- 单机运行，线程模型简单可预测
- 支持 `ReactiveStateEngine` 与 `TimeSeriesEngine`
- Rust 快速路径全程无 GIL
- Python 仅负责编排、数据写入与结果订阅
- 默认配置下不静默丢数
- 相同输入重放得到相同输出

### 1.3 V1 非目标

以下内容明确不属于 V1：

- SQL 语法
- 字符串表达式 DSL
- `CrossSectionalEngine`
- Python UDF 参与计算路径
- 分布式 / 多机部署
- 状态快照与故障恢复
- Web 管理界面
- 共享内存发布
- 自动 schema 推断
- 隐式类型转换

### 1.4 V1 成功标准

V1 交付完成时，至少满足以下验收标准：

1. 对同一份历史输入重放两次，输出内容一致，顺序一致。
2. 默认配置下，输入队列满时不会静默覆盖旧数据。
3. `ReactiveStateEngine` 能稳定输出常用逐条因子。
4. `TimeSeriesEngine` 能稳定输出按 key 的 tumbling bar。
5. Python 侧可以用 `Polars DataFrame` 或 `Arrow RecordBatch` 写入。
6. 结果可以通过 `ZmqPublisher` 发布，也可以可选写入 Parquet。

---

## 2. 核心原则

### 2.1 正确性优先于便利性

默认行为优先保证结果正确，不优先追求“尽量不断流”。
如果后续版本允许接受丢数或跳过晚到数据，这些都必须显式配置。
V1 不提供“归档失败后继续运行”的默认模式。

### 2.2 时间语义必须显式

- 所有时间戳均使用 UTC
- 窗口计算只基于事件时间，不基于处理时间
- 不做隐式重排
- 晚到数据策略必须显式配置

### 2.3 结果必须可复现

同一输入序列、同一参数、同一版本，必须得到同一输出。
因此 V1 不引入需要额外时钟协调的复杂触发器，也不引入隐式补数逻辑。

### 2.4 默认不静默丢数

输入队列溢出默认阻塞写入端，而不是覆盖旧数据。
只有用户明确选择 lossy 策略时，系统才允许丢弃数据，并且必须记录指标与日志。

### 2.5 快速路径只放 Rust

V1 的计算路径只包含 Rust 内置算子。
Python 在 V1 中只负责：

- 构造引擎
- 调用 `start()` / `write()` / `flush()` / `stop()`
- 订阅结果

这可以显著降低 GIL、回调异常、类型推断等复杂度。

---

## 3. 总体架构

### 3.1 分层

```
┌──────────────────────────────────────────────────────────┐
│                    Python 编排层 (PyO3)                  │
│  Engine 定义 / 生命周期管理 / write() 入口 / 订阅器       │
├──────────────────────────────────────────────────────────┤
│                    引擎层 (Rust)                         │
│  ReactiveStateEngine / TimeSeriesEngine                 │
├──────────────────────────────────────────────────────────┤
│                    运行时层 (Rust)                       │
│  Engine trait / 有界输入队列 / 线程管理 / 路由            │
├──────────────────────────────────────────────────────────┤
│                    I/O 层 (Rust)                         │
│  Arrow / ZMQ Publisher / NullPublisher / ParquetSink    │
└──────────────────────────────────────────────────────────┘
```

### 3.2 线程模型

V1 采用 `thread-per-engine`：

- 每个 Engine 实例运行在独立 OS 线程
- 每个 Engine 拥有独立有界输入队列
- Engine 之间通过进程内 channel 传递 `RecordBatch`
- 不使用 async runtime

选择理由：

- 调度简单，延迟更可预测
- Engine 数量在量化单机场景通常有限
- 更容易隔离计算、发布与归档错误

### 3.3 数据流

```
Python write()
    │
    ▼
schema 校验 + 格式转换
    │
    ▼
Engine 输入队列
    │
    ▼
Engine 线程执行
    │
    ├──▶ 下游 source 订阅
    ├──▶ Publisher 对外发布
    └──▶ ParquetSink 异步归档
```

注意：V1 不提供事务性 fan-out。
也就是说，某个 batch 已经成功送达下游 Engine，不代表外部 Publisher 一定也成功。
这类失败会被显式上报，并触发 engine 失败策略，但不会回滚已经送达的内部结果。

---

## 4. 运行时契约

这一节是 V1 最重要的部分。目标是先把“什么时候算、什么时候产出、什么时候失败”写死。

### 4.1 Engine 生命周期

每个 Engine 都有以下状态：

- `Created`：已构造，未启动
- `Running`：线程已启动，接受写入
- `Stopping`：收到停止信号，正在 flush
- `Stopped`：已完全停止，不再接受写入
- `Failed`：发生不可恢复错误，拒绝继续处理

状态迁移规则：

1. `start()`：`Created -> Running`
2. `flush()`：在 `Running` 内执行，不改变状态
3. `stop()`：`Running -> Stopping -> Stopped`
4. 任意不可恢复错误：`Running -> Failed`

### 4.2 核心 Trait

V1 的核心抽象不再强行把所有引擎压成“输入一批，立刻返回一批”。
改为显式区分数据处理与流结束处理：

```rust
trait Engine: Send + 'static {
    /// 引擎名称
    fn name(&self) -> &str;

    /// 输入 schema
    fn input_schema(&self) -> SchemaRef;

    /// 输出 schema
    fn output_schema(&self) -> SchemaRef;

    /// 处理一批输入。可返回 0 个、1 个或多个输出 batch。
    fn on_data(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>>;

    /// flush 时触发。用于关闭窗口、输出尾部结果。
    fn on_flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }

    /// stop 时触发。默认等价于 flush。
    fn on_stop(&mut self) -> Result<Vec<RecordBatch>> {
        self.on_flush()
    }
}
```

这个设计解决了前一版的三个问题：

- `ReactiveStateEngine` 可以按批处理并返回等长结果
- `TimeSeriesEngine` 可以在单次 `on_data()` 中返回 0 到多个窗口结果
- 流结束时可以通过 `flush()` 明确输出尾部窗口

### 4.3 写入顺序与处理顺序

运行时遵守以下顺序保证：

- 同一个 Engine 的输入按照入队顺序处理
- 同一个 batch 内的行顺序保持不变
- `ReactiveStateEngine` 不会按时间戳重排
- `TimeSeriesEngine` 仅要求同一 key 的事件时间非递减

### 4.4 时间语义

V1 统一使用事件时间。

约束如下：

- 时间列必须是 UTC 时间戳
- 推荐 Arrow 类型：`timestamp(ns, "UTC")`
- `ReactiveStateEngine` 默认按输入顺序更新状态，不依赖时间列排序
- `TimeSeriesEngine` 必须显式指定 `dt_column`

V1 不包含：

- watermark
- processing-time 触发
- 空窗口自动补齐
- idle timer 自动关窗

因此 `TimeSeriesEngine` 的窗口关闭规则为：

1. 收到新事件时，如果该事件使某个已打开窗口结束，则输出该窗口
2. 调用 `flush()` 时，强制输出当前所有未关闭窗口

这套语义非常保守，但实现简单、可重放、可测试。

### 4.5 晚到数据

`TimeSeriesEngine` 对晚到数据提供两种策略：

- `Reject`：默认。发现时间戳回退时立即报错，Engine 进入 `Failed`
- `DropWithMetric`：显式开启后允许丢弃晚到行，并记录 `late_rows_total`

V1 默认使用 `Reject`，原因很简单：大多数量化聚合一旦混入乱序补写，结果会变得不可解释。

### 4.6 错误处理

V1 区分三类错误：

| 类型 | 例子 | 默认行为 |
|------|------|----------|
| 配置错误 | schema 不合法、列不存在 | 构造阶段失败 |
| 数据错误 | 类型不匹配、晚到数据 | 当前 write 失败，Engine 进入 `Failed` |
| 输出错误 | ZMQ 发布失败、Parquet 写入失败 | Engine 进入 `Failed` |

V1 不做“吞掉错误继续跑”的默认设计。
如果未来需要更细的失败策略，再单独补设计。

---

## 5. 数据模型与写入 API

### 5.1 输入 schema

V1 要求 `input_schema` 显式声明。
这样做的原因不是保守，而是为了避免三类隐患：

- notebook 首批数据恰好不完整，推断错 schema
- 空列或全空列导致类型漂移
- Python 与 Arrow 的隐式类型升级行为不一致

因此：

- 所有 Engine 都必须显式提供 `input_schema`
- V1 不支持“第一次 write 自动推断并锁定”
- V1 不支持隐式列补齐或隐式类型转换

### 5.2 支持的写入格式

`engine.write()` 在 Python 侧接受以下格式：

| 格式 | 说明 | 是否拷贝 |
|------|------|----------|
| `pyarrow.RecordBatch` | 直接校验后入队 | 最少拷贝 |
| `polars.DataFrame` | 转为 Arrow 后入队 | 通常较少拷贝 |
| `dict[str, list]` | 列式输入 | 需要构造 Arrow |
| `dict[str, scalar]` | 单行输入 | 需要构造 Arrow |
| `list[dict]` | 行式输入 | 需要构造 Arrow |

V1 不把 “零拷贝” 作为对外承诺，只承诺：

- Engine 内部计算统一基于 Arrow
- 进程内 Engine 级联尽量复用 `RecordBatch`
- 跨 ZMQ 发布一定涉及序列化

### 5.3 输出 schema

输出 schema 由以下两部分组成：

1. 保留的输入列
2. 算子新增列

V1 规则如下：

- 每个算子必须显式提供 `output`
- 内置算子的输出类型由算子定义决定
- 不允许两个算子写入同名 `output`
- `TimeSeriesEngine` 额外输出 `window_start` 与 `window_end`

### 5.4 输入队列

每个 Engine 拥有独立有界输入队列，初始实现可以是固定容量 ring buffer 或等价的有界 channel。

关键参数：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `buffer_capacity` | 队列最大 batch 数 | `1024` |
| `overflow_policy` | 队列满时策略 | `Block` |

支持的溢出策略：

- `Block`：默认。阻塞写入端直到有空间
- `Reject`：当前写入失败并返回错误
- `DropOldest`：显式开启后，丢弃最旧 batch，并增加 `dropped_batches_total`

V1 默认不提供“打个日志继续覆盖旧数据”的行为。
对状态型计算来说，这种默认值太危险。

### 5.5 运行时指标

每个 Engine 至少暴露以下指标：

- `processed_batches_total`
- `processed_rows_total`
- `output_batches_total`
- `queue_depth`
- `dropped_batches_total`
- `late_rows_total`
- `publish_errors_total`

这些指标既用于调试，也用于证明系统没有在无声地偏离预期。

---

## 6. V1 预制引擎

### 6.1 ReactiveStateEngine

#### 6.1.1 语义

`ReactiveStateEngine` 面向逐条更新的状态型因子。

处理语义：

- 输入一批，输出一批
- 输出行数与输入行数相同
- 输出顺序与输入顺序完全一致
- 状态按 `id_column` 隔离维护

它适合以下场景：

- EMA
- 滚动均值与波动率
- N 期收益率
- 实时逐笔特征衍生

#### 6.1.2 V1 内置算子

V1 先只交付一组最常用、最容易验证的逐条算子：

| 算子 | 说明 | 参数 |
|------|------|------|
| `TS_EMA` | 指数移动均值 | `span` |
| `TS_MEAN` | 固定窗口均值 | `window` |
| `TS_STD` | 固定窗口标准差 | `window` |
| `TS_DELAY` | N 期滞后值 | `period` |
| `TS_DIFF` | 与 N 期前的差值 | `period` |
| `TS_RETURN` | 与 N 期前的收益率 | `period` |
| `ABS` | 绝对值 | - |
| `LOG` | 对数变换 | - |
| `CLIP` | 区间裁剪 | `min`, `max` |
| `CAST` | 显式类型转换 | `dtype` |

以下内容延后：

- Python 自定义 UDF
- 滚动相关、协方差、偏度、峰度
- 表达式字符串编译

#### 6.1.3 示例

```python
import pyarrow as pa
import zippy

tick_schema = pa.schema([
    ("symbol", pa.string()),
    ("dt", pa.timestamp("ns", tz="UTC")),
    ("price", pa.float64()),
    ("volume", pa.float64()),
])

engine = zippy.ReactiveStateEngine(
    name="tick_factors",
    input_schema=tick_schema,
    id_column="symbol",
    factors=[
        zippy.TS_EMA(column="price", span=20, output="ema_20"),
        zippy.TS_RETURN(column="price", period=1, output="ret_1"),
    ],
    target=[zippy.ZmqPublisher(endpoint="tcp://*:5555")],
)
```

### 6.2 TimeSeriesEngine

#### 6.2.1 语义

`TimeSeriesEngine` 面向按时间窗口聚合的任务。

V1 只支持：

- `tumbling` 窗口
- 事件时间驱动
- 每个 key 独立开窗

V1 不支持：

- `sliding` 窗口
- session window
- processing-time 触发
- 自动补空 bar

#### 6.2.2 关窗规则

对每个 `id_column`，运行时维护当前打开窗口。
当某条新数据的时间戳落入更晚窗口时：

1. 先关闭之前已结束的窗口
2. 发出窗口结果
3. 再把该条数据写入新窗口

流结束时，用户必须显式调用 `flush()`，这样最后一个未关闭窗口也能落出。

#### 6.2.3 V1 聚合算子

| 算子 | 说明 |
|------|------|
| `AGG_FIRST` | 窗口首值 |
| `AGG_LAST` | 窗口末值 |
| `AGG_MAX` | 窗口最大值 |
| `AGG_MIN` | 窗口最小值 |
| `AGG_SUM` | 窗口求和 |
| `AGG_COUNT` | 窗口计数 |
| `AGG_VWAP` | 成交量加权均价 |

#### 6.2.4 输出列

`TimeSeriesEngine` 的输出至少包含：

- `id_column`
- `window_start`
- `window_end`
- 每个聚合算子的 `output`

#### 6.2.5 示例

```python
import pyarrow as pa
import zippy

tick_schema = pa.schema([
    ("symbol", pa.string()),
    ("dt", pa.timestamp("ns", tz="UTC")),
    ("price", pa.float64()),
    ("volume", pa.float64()),
])

bars = zippy.TimeSeriesEngine(
    name="bar_1m",
    input_schema=tick_schema,
    id_column="symbol",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type="tumbling",
    late_data_policy="reject",
    factors=[
        zippy.AGG_FIRST(column="price", output="open"),
        zippy.AGG_MAX(column="price", output="high"),
        zippy.AGG_MIN(column="price", output="low"),
        zippy.AGG_LAST(column="price", output="close"),
        zippy.AGG_SUM(column="volume", output="volume"),
        zippy.AGG_VWAP(price_column="price", volume_column="volume", output="vwap"),
    ],
    target=[zippy.ZmqPublisher(endpoint="tcp://*:5556")],
)
```

### 6.3 不进入 V1 的引擎

#### 6.3.1 CrossSectionalEngine

`CrossSectionalEngine` 暂不进入 V1，原因有三点：

1. 需要额外定义 universe 生命周期
2. 需要明确截面对齐、超时、缺失补值与 warmup 规则
3. 需要处理更复杂的可复现性问题

这部分值得单独一份设计文档，不应和 V1 运行时一起首发。

#### 6.3.2 ExpressionFactor / 字符串表达式

表达式系统暂不进入 V1，原因同样明确：

1. 需要 parser、AST、类型推断、公共子表达式消除
2. 与自定义算子注册机制强耦合
3. 很容易放大 schema、空值、类型提升问题

V1 先以显式对象 API 为准。

---

## 7. source、target 与持久化

### 7.1 source：Engine 级联

V1 允许一个 Engine 订阅另一个 Engine 的输出。

```python
bars = zippy.TimeSeriesEngine(
    ...,
    source=engine,
)
```

运行时语义：

- 级联发生在 Rust 进程内
- 级联传输对象为 `RecordBatch`
- 不经过 Python 层
- 不触发 GIL

### 7.2 target：结果发布

V1 只交付两类 target：

| Target | 版本 | 用途 |
|--------|------|------|
| `ZmqPublisher` | V1 | 默认对外发布方式 |
| `NullPublisher` | V1 | 基准测试与压测 |

统一接口如下：

```rust
trait Publisher: Send + 'static {
    fn publish(&mut self, batch: &RecordBatch) -> Result<()>;

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
```

`ZmqPublisher` 使用 Arrow IPC 序列化。
这里不对“零拷贝网络传输”做承诺，只保证协议统一、实现简单、易于调试。

### 7.3 ParquetSink：可选异步归档

V1 保留可选归档能力，但不把它并入低延迟发布抽象。

```python
engine = zippy.ReactiveStateEngine(
    ...,
    parquet_sink=zippy.ParquetSink(
        path="/data/factors/{date}/",
        rotation="1h",
        write_input=True,
        write_output=True,
    ),
)
```

设计约束：

- `ParquetSink` 运行在独立写线程
- 写线程拥有独立有界队列
- 默认失败策略为 `StopEngine`

原因：归档不是可有可无的小功能，它直接关系到可审计性。
如果用户想把归档降级为 best-effort，后续版本再显式开放该策略。

---

## 8. Python 绑定与 API

### 8.1 GIL 策略

V1 的原则很简单：计算路径不引入 Python 回调。

| 操作 | GIL 状态 |
|------|----------|
| `engine.write(df)` 参数解析 | 短暂持有 |
| Arrow 转换完成后入队 | 释放 |
| Rust 引擎计算 | 无 GIL |
| ZMQ 发布 | 无 GIL |
| Parquet 写入 | 无 GIL |

因此 V1 不存在“多个含 Python UDF 的引擎互相争用 GIL”这一类问题。

### 8.2 生命周期 API

每个 Engine 提供一致的 Python 生命周期接口：

```python
engine.start()
engine.write(batch_or_dataframe)
engine.flush()
engine.stop()
```

语义约束：

- `start()` 之后才能 `write()`
- `flush()` 可重复调用
- `stop()` 会隐式执行一次 `flush()`
- `stop()` 后再次 `write()` 必须报错

### 8.3 端到端示例

```python
import polars as pl
import pyarrow as pa
import zippy

tick_schema = pa.schema([
    ("symbol", pa.string()),
    ("dt", pa.timestamp("ns", tz="UTC")),
    ("price", pa.float64()),
    ("volume", pa.float64()),
])

reactive = zippy.ReactiveStateEngine(
    name="tick_factors",
    input_schema=tick_schema,
    id_column="symbol",
    factors=[
        zippy.TS_EMA(column="price", span=20, output="ema_20"),
        zippy.TS_RETURN(column="price", period=1, output="ret_1"),
    ],
    target=[zippy.ZmqPublisher(endpoint="tcp://*:5555")],
)

bars = zippy.TimeSeriesEngine(
    name="bar_1m",
    source=reactive,
    input_schema=reactive.output_schema(),
    id_column="symbol",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type="tumbling",
    late_data_policy="reject",
    factors=[
        zippy.AGG_FIRST(column="price", output="open"),
        zippy.AGG_MAX(column="price", output="high"),
        zippy.AGG_MIN(column="price", output="low"),
        zippy.AGG_LAST(column="price", output="close"),
    ],
    target=[zippy.ZmqPublisher(endpoint="tcp://*:5556")],
)

reactive.start()
bars.start()

df = pl.DataFrame(
    {
        "symbol": ["000001", "000001"],
        "dt": [
            "2026-04-02T09:30:00.000000000Z",
            "2026-04-02T09:30:01.000000000Z",
        ],
        "price": [10.0, 10.1],
        "volume": [100.0, 120.0],
    }
).with_columns(pl.col("dt").str.to_datetime(time_unit="ns", time_zone="UTC"))

reactive.write(df)

reactive.flush()
bars.flush()

bars.stop()
reactive.stop()
```

---

## 9. 项目结构

### 9.1 Workspace 布局

```
zippy/
├── Cargo.toml
├── crates/
│   ├── zippy-core/       # Engine trait、公共类型、错误、指标
│   ├── zippy-operators/  # Reactive 与 window 聚合算子
│   ├── zippy-engines/    # ReactiveStateEngine、TimeSeriesEngine
│   ├── zippy-io/         # Publisher、ZMQ、ParquetSink
│   └── zippy-python/     # PyO3 绑定
├── python/
│   └── zippy/
│       ├── __init__.py
│       └── _internal.pyi
├── tests/                # Rust 集成测试
├── pytests/              # Python 端测试
├── benches/              # 性能与回放基准
└── docs/
```

### 9.2 依赖关系

```
zippy-core       <- arrow-rs
    ▲
zippy-operators  <- zippy-core
    ▲
zippy-engines    <- zippy-core + zippy-operators
    ▲
zippy-io         <- zippy-core
    ▲
zippy-python     <- zippy-engines + zippy-io
```

### 9.3 关键依赖

| 用途 | 依赖 |
|------|------|
| Arrow 内存格式 | `arrow-rs` |
| Parquet 读写 | `parquet` |
| 进程内队列 | `crossbeam-channel` 或等价有界结构 |
| ZMQ | `zmq` |
| Python 绑定 | `pyo3` + `maturin` |
| 日志与指标 | `tracing` |

V1 不引入表达式解析器与共享内存依赖。

---

## 10. 测试与验证

### 10.1 必做测试

V1 至少需要以下测试：

- `ReactiveStateEngine` golden replay 测试
- `TimeSeriesEngine` 关窗与 flush 测试
- 晚到数据拒绝测试
- 输入队列溢出策略测试
- ZMQ 发布集成测试
- ParquetSink 轮转与恢复测试
- Python `write(Polars DataFrame)` 集成测试

### 10.2 可复现性验证

可复现性不是一句口号，必须通过测试体现：

1. 用同一份 fixture 数据连续回放两次
2. 比较输出 batch 序列与行序
3. 对 stateful 算子校验数值结果
4. 对 bar 聚合校验窗口边界

### 10.3 基准测试

V1 基准测试只回答三个问题：

1. 单引擎纯 Rust 路径吞吐是多少
2. 加上 ZMQ 发布后开销是多少
3. 加上 ParquetSink 后开销是多少

不在 V1 里承诺绝对性能数字，先把测量框架搭起来。

---

## 11. V1 实施范围

### 11.1 包含

- `Engine` 运行时契约
- 有界输入队列与线程管理
- `ReactiveStateEngine`
- `TimeSeriesEngine`
- 一组最小可用的 TS 与 AGG 算子
- `ZmqPublisher`
- `NullPublisher`
- `ParquetSink`
- Python 编排与 `write()` 接口
- 测试与基准框架

### 11.2 不包含

- `CrossSectionalEngine`
- 字符串表达式系统
- Python UDF
- `CallbackPublisher`
- `ShmPublisher`
- `sliding` / session window
- 状态快照与故障恢复
- 分布式能力

### 11.3 推荐里程碑

建议按以下顺序落地：

1. `zippy-core`：错误、schema、trait、指标、生命周期
2. `zippy-engines`：先做 `ReactiveStateEngine`
3. `zippy-operators`：补齐 V1 TS 算子
4. `zippy-engines`：实现 `TimeSeriesEngine`
5. `zippy-io`：接入 `NullPublisher` 与 `ZmqPublisher`
6. `zippy-python`：完成 Python 编排与写入
7. `ParquetSink`：补异步归档
8. `tests` + `benches`：补端到端验证

---

## 12. 后续扩展方向

V1 稳定后，再分别补独立设计文档：

### 12.1 CrossSectionalEngine 设计

需要单独定义：

- universe 注册与变更
- 截面对齐策略
- 缺失值与 warmup 规则
- 定时触发与可复现性边界

### 12.2 ExpressionFactor 设计

需要单独定义：

- 语法
- AST
- 类型推断
- 公共子表达式消除
- 与内置算子注册机制的关系

### 12.3 研究舒适路径

如果未来确实需要 Python 舒适路径，应该把它设计成独立扩展层，而不是混入 V1 核心运行时：

- Python UDF
- `CallbackPublisher`
- notebook 友好的 schema helper

---

## 13. 结论

这份 V1 设计的核心取舍只有一句话：

先交付一个单机、保守、可复现、可验证的流式计算内核，再在其上扩展表达式、截面和研究舒适路径。

与前一版相比，新的设计有三个明确收口：

- 用 `on_data()` / `on_flush()` / `on_stop()` 取代过于理想化的单一 `process()`
- 默认策略从“宁可丢旧数据”改为“默认保正确性”
- 把 `CrossSectionalEngine`、表达式系统、Python UDF 从 V1 中拆出

这样做会让首版看起来更窄，但实现路径更短，语义更清晰，真正更有机会按质交付。
