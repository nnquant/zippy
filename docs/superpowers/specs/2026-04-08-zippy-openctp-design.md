# zippy-openctp 设计说明

## 1. 背景与目标

`zippy` 主仓库已经具备稳定的 `Source -> Engine -> Publisher` 运行时边界，也已经支持跨进程 `ZmqSource` / `ZmqStreamPublisher` 链路。下一步需要一种更贴近生产环境的独立插件包，让 Python 用户可以直接接入真实行情源，而不是自己处理底层柜台 API、连接、订阅、重连和 schema。

本设计定义一个独立仓库与独立发布的生态插件包：`zippy-openctp`。

`zippy-openctp` 的首版目标是：

- 面向 Python 用户提供开箱即用的 `OpenCtpMarketDataSource`
- 直接依赖 [`pseudocodes/ctp2rs`](https://github.com/pseudocodes/ctp2rs) 作为上游行情接口
- 预置 `TickDataSchema()`，不要求用户手写 CTP tick schema
- 自动管理连接、登录、静态订阅、断线重连和重订阅
- 将 OpenCTP 行情标准化为 `zippy` 可消费的 `Source`
- 支持低延迟默认语义，同时保留批处理优化能力

本设计**不**包含交易接口，不实现动态订阅，不实现插件扫描器，不将插件仓库并入当前 `zippy` 主仓库。

## 2. 总体方案

### 2.1 仓库形态

`zippy-openctp` 是一个独立仓库，不进入当前 `zippy` 主仓库的 workspace，也不通过主仓库扫描发现。

使用方式是显式依赖：

- Rust 用户通过 `Cargo.toml` 引入
- Python 用户通过 `pip` / `uv` 安装后显式 `import zippy_openctp`

这意味着 `zippy` 主仓库真正需要保证的是稳定的插件契约，而不是运行时动态插件系统。

### 2.2 用户体验目标

Python 用户的目标用法如下：

```python
import zippy
import zippy_openctp

source = zippy_openctp.OpenCtpMarketDataSource(
    front="tcp://127.0.0.1:12345",
    broker_id="9999",
    user_id="000001",
    password="******",
    instruments=["IF2506", "IH2506"],
    flow_path=".cache/openctp/md",
    schema=zippy_openctp.schemas.TickDataSchema(),
    reconnect=True,
    login_timeout_sec=10,
    rows_per_batch=1,
    flush_interval_ms=0,
)

engine = zippy.TimeSeriesEngine(
    name="bars",
    source=source,
    input_schema=zippy_openctp.schemas.TickDataSchema(),
    id_column="instrument_id",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type=zippy.WindowType.TUMBLING,
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[
        zippy.AGG_FIRST(column="last_price", output="open"),
        zippy.AGG_LAST(column="last_price", output="close"),
        zippy.AGG_SUM(column="volume", output="volume"),
    ],
)
```

`zippy` 核心仓库不感知 CTP/OpenCTP 细节，只把它当成一个 `Source`。

## 3. 插件契约

`zippy-openctp` 与 `zippy` 主仓库之间通过四层稳定契约集成。

### 3.1 数据契约

插件必须暴露预置 schema：

```python
zippy_openctp.schemas.TickDataSchema()
```

这个 schema 是 Python 用户的唯一推荐入口。首版不要求用户自己定义 tick schema，也不鼓励用户手工拼字段。

### 3.2 Source 契约

插件必须暴露一个能直接传给 `zippy` engine `source=` 参数的对象：

```python
zippy_openctp.OpenCtpMarketDataSource(...)
```

对 `zippy` 主仓库来说，它只是某个实现了 `zippy_core::Source` 的 source。

### 3.3 配置契约

OpenCTP 的连接地址、登录信息、订阅列表、重连策略、flow path 等都由插件自身管理。`zippy` 核心仓库不承载任何 OpenCTP 领域配置。

### 3.4 Python 契约

插件优先服务 Python 用户，因此必须提供：

- Python 包入口
- Python 类型桩
- Python 级最小示例
- Python 风格稳定 API

## 4. 上游依赖：ctp2rs

首版 `zippy-openctp` **直接依赖** `ctp2rs`，不额外包一层独立 adapter trait。

选择原因：

- 当前优先目标是尽快交付一个可用、贴近生产的独立插件
- `ctp2rs` 已经直接支持 `OpenCTP`
- 直接依赖可以减少一层“为了抽象而抽象”的间接封装
- 首版复杂度应集中在 `Source`、标准化和 Python 体验上

该设计要求 `zippy-openctp-core` 直接对接 `ctp2rs` 的行情 API 与回调模型。

## 5. 仓库结构

建议的独立仓库结构如下：

```text
zippy-openctp/
├── Cargo.toml
├── pyproject.toml
├── crates/
│   ├── zippy-openctp-core/
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── schema.rs
│   │       ├── source.rs
│   │       ├── normalize.rs
│   │       └── metrics.rs
│   └── zippy-openctp-python/
│       ├── Cargo.toml
│       └── src/lib.rs
├── python/
│   └── zippy_openctp/
│       ├── __init__.py
│       ├── _internal.pyi
│       └── schemas.py
├── examples/
│   ├── md_to_parquet.py
│   └── md_to_remote_pipeline.py
└── tests/
```

职责边界：

- `schema.rs`
  - 定义 tick schema 的 Rust 构造入口
- `normalize.rs`
  - 将 `ctp2rs` 行情回调结构转换为标准化后的 row
- `source.rs`
  - 核心 `Source` 实现
  - 负责连接、登录、订阅、重连、批量 flush
- `metrics.rs`
  - source 状态与指标快照
- `zippy-openctp-python/src/lib.rs`
  - PyO3 绑定层
- `python/zippy_openctp/schemas.py`
  - Python 级 schema 入口

## 6. Tick schema 设计

### 6.1 Python 入口

首版固定暴露：

```python
zippy_openctp.schemas.TickDataSchema()
```

它返回的 schema 必须和 `OpenCtpMarketDataSource` 实际输出 batch 严格一致。

### 6.2 设计原则

- 列名统一 `snake_case`
- 事件时间列统一命名为 `dt`
- 合约列统一命名为 `instrument_id`
- 不直接暴露柜台风格的原始字段命名
- 字段类型稳定，不允许运行时漂移

### 6.3 建议字段

首版建议包含但不限于这些字段：

- `instrument_id`
- `exchange_id`
- `trading_day`
- `action_day`
- `dt`
- `last_price`
- `pre_settlement_price`
- `pre_close_price`
- `open_price`
- `highest_price`
- `lowest_price`
- `volume`
- `turnover`
- `open_interest`
- `bid_price_1`
- `bid_volume_1`
- `ask_price_1`
- `ask_volume_1`
- `average_price`
- `upper_limit_price`
- `lower_limit_price`

具体首版字段数可以略小于完整 CTP depth market data 结构，但必须覆盖常用期货 tick 计算场景，并在文档中固定。

### 6.4 类型建议

- `instrument_id`: utf8
- `exchange_id`: utf8
- `trading_day`: utf8
- `action_day`: utf8
- `dt`: timestamp(ns, UTC) 或等价稳定纳秒时间表示
- 价格类字段：float64
- 数量类字段：int64

首版一旦发布，不允许在同一个包版本中随运行时环境漂移字段类型。

## 7. 薄标准化语义

`zippy-openctp` 首版输出前会做一层**薄标准化**，但不会做重度业务清洗。

### 7.1 标准化范围

1. 时间标准化
- 将 OpenCTP 原始日期/时间/毫秒字段统一整理为 `dt`
- 必须在文档中明确时间生成规则、时区和交易日口径

2. 命名标准化
- 使用稳定、可读的 `snake_case` 输出列名

3. 类型标准化
- 明确每个列的 Arrow 类型

4. 最小清洗
- 对明显无效的空字符串、未初始化字段做统一处理
- 允许映射为 null

### 7.2 不做的事

首版明确不做：

- 自动聚合 bar
- 自动修正异常行情价
- 自动去重
- 自动推导缺失字段
- 任何会改变行情原始语义的“智能修复”

也就是说，`zippy-openctp` 是“生产化 tick Source”，不是“行情清洗引擎”。

## 8. OpenCtpMarketDataSource 设计

### 8.1 Python API

建议首版 API 如下：

```python
source = zippy_openctp.OpenCtpMarketDataSource(
    front="tcp://127.0.0.1:12345",
    broker_id="9999",
    user_id="000001",
    password="******",
    instruments=["IF2506", "IH2506"],
    flow_path=".cache/openctp/md",
    schema=zippy_openctp.schemas.TickDataSchema(),
    reconnect=True,
    login_timeout_sec=10,
    rows_per_batch=1,
    flush_interval_ms=0,
)
```

### 8.2 生命周期

该对象对 Python 用户的语义是“一个可直接交给 `source=` 的生产 Source”，不是一个需要手动执行 `connect/login/subscribe` 的裸客户端。

当它被 `zippy` runtime 启动时，应自动完成：

1. 创建 OpenCTP 行情 API
2. 注册行情 SPI/回调
3. 建立连接
4. 登录
5. 按静态 `instruments` 列表订阅

### 8.3 连接与重连

首版必须支持：

- 断线自动重连
- 重连成功后自动重新订阅原 instruments

首版不支持运行中动态增删订阅。

### 8.4 输出语义

每收到一条 tick 回调：

1. 先进行薄标准化
2. 写入内部批缓冲
3. 根据 batching 规则决定是否生成 `RecordBatch`
4. `emit(SourceEvent::Data(...))`

### 8.5 停止语义

当 runtime 停止该 source 时，必须：

- 安全关闭行情连接
- 释放底层 API 资源
- 停止后台线程
- 清理批缓冲

不允许把资源释放交给 Python GC 的不确定时机。

## 9. Batching 语义

### 9.1 配置项

首版同时支持：

- `rows_per_batch`
- `flush_interval_ms`

建议 Python API：

```python
OpenCtpMarketDataSource(
    ...,
    rows_per_batch=1,
    flush_interval_ms=0,
)
```

### 9.2 默认语义

虽然内部保留 batching 能力，但默认要求满足下游低延迟需求，因此：

- 默认按单条 tick 立即可发布
- 即默认等价于 `rows_per_batch=1`
- `flush_interval_ms=0` 表示不依赖定时器攒批

### 9.3 触发规则

满足任一条件即发 batch：

- 缓冲行数达到 `rows_per_batch`
- 距离上次发送时间达到 `flush_interval_ms`

### 9.4 设计原则

- batching 是吞吐优化手段，不改变行情正确性语义
- 调大 batch 参数只改变吞吐/延迟权衡
- 不允许因为批处理而改写 tick 顺序或事件时间

## 10. 状态与指标

Python 侧首版至少提供：

- `status()`
- `metrics()`
- `config()`

建议最小指标集：

- `ticks_received_total`
- `ticks_emitted_total`
- `batches_emitted_total`
- `reconnects_total`
- `login_failures_total`
- `subscribe_failures_total`

这些指标用于帮助 Python 用户判断插件是否稳定工作，而不是只依赖日志。

## 11. 与 zippy 主仓库的集成方式

主仓库无需实现插件扫描器或运行时发现机制。

集成方式是：

- Python 用户显式安装 `zippy-openctp`
- 显式 `import zippy_openctp`
- 显式构造 `OpenCtpMarketDataSource`
- 再将其作为 `source=` 传给 `zippy` 的 engine

这保证：

- `zippy` 核心保持领域中立
- OpenCTP 领域逻辑留在插件包内部
- 未来可以自然扩展为更多独立生态包，例如 `zippy-binance`、`zippy-kafka`

## 12. 首版最小可交付范围

### 12.1 首版做

- 独立 repo：`zippy-openctp`
- 独立 Python 包发布
- `schemas.TickDataSchema()`
- `OpenCtpMarketDataSource`
- 静态 `instruments`
- 自动连接、登录、订阅
- 自动重连、重订阅
- tick 薄标准化
- `rows_per_batch` + `flush_interval_ms`
- 默认单条即时发布
- `status()` / `metrics()` / `config()`
- 最小 Python 示例：
  - `OpenCtpMarketDataSource -> TimeSeriesEngine -> ParquetSink`
  - `OpenCtpMarketDataSource -> TimeSeriesEngine -> ZmqStreamPublisher`

### 12.2 首版不做

- 交易接口
- 动态订阅
- 多账户/多前置统一编排
- 回放
- schema 多版本协商
- 主仓库插件扫描器

## 13. 测试策略

首版测试应分三层：

### 13.1 Rust 单测

- schema 构造
- 标准化逻辑
- batching 规则
- 状态与指标统计

### 13.2 Rust 集成测试

- 使用 fake/md callback 驱动 source
- 验证登录后订阅、断线重连、重订阅和 batch 输出
- 自动测试中不依赖真实 OpenCTP 柜台环境

### 13.3 Python 集成测试

- `OpenCtpMarketDataSource` 能被 `zippy` engine 作为 `source=` 使用
- 最小 pipeline 能启动、接收、处理、停止

设计原则是：

- 自动测试不依赖真实柜台环境
- 真实连柜台应只作为手动验收或 example 验证

## 14. 后续演进方向

首版稳定后，后续可考虑：

- 动态订阅接口
- 更多 schema 版本或扩展字段集
- 更多 source 指标
- 更细的重连策略配置
- 交易接口插件（但作为独立后续项目，不进入本设计范围）

