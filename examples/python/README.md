# Python Examples

这些示例展示了 Zippy V1 当前已经打通的最小用户路径：

- `publish_pipeline.py`：`ReactiveStateEngine -> TimeSeriesEngine` 进程内级联，并通过 `ZmqPublisher` 对外发布
- `cross_sectional_pipeline.py`：`ReactiveStateEngine -> TimeSeriesEngine -> CrossSectionalEngine` 进程内级联，并通过 `ZmqPublisher` 对外发布截面因子
- `remote_pipeline.py`：`TimeSeriesEngine -> ZmqStreamPublisher -> ZmqSource -> CrossSectionalEngine` 的跨进程处理链
- `stream_table_pipeline.py`：`StreamTableEngine` 作为原始流数据中心入口，原样透传输入 batch，同时写入 `sink` 并发布到 `target`
- `subscribe_bars.py`：使用 `ZmqSubscriber` 订阅并读取一个 `RecordBatch`
- `archive_reactive.py`：使用 `ParquetSink` 归档 `ReactiveStateEngine` 的输入和输出

`StreamTableEngine` 用于原始流数据中心场景：

- 原样透传原始输入 batch，不做聚合或状态计算
- 同时写入 `sink`，用于本地归档或后续离线分析
- 同时发布到 `target`，用于下游实时消费
- 最小示例只归档 `write_output=True`，不同时开启 `write_input=True`，因为 `StreamTableEngine` 的输出就是原样透传输入，双写会把同一批数据归档两次
- 示例每次运行都会写到 `/tmp/zippy-stream-table-demo/run-<timestamp>/`，用于隔离历史 parquet 文件

表达式因子：

- 当前 Python API 额外支持 `zippy.Expr(expression="price + ema_2", output="price_plus_ema")`
- 表达式运行在 `ReactiveStateEngine` 内，支持引用输入列和前序 reactive factor 输出
- `TimeSeriesEngine` 额外支持 `pre_factors=[...]` 和 `post_factors=[...]`
- `pre_factors` 在晚到数据过滤之后、窗口聚合之前执行
- `post_factors` 在 `AGG_*` 输出之后执行，`flush()` 也会触发这一阶段
- 当前支持的语法边界是：`+` / `-` / `*` / `/`、括号、数值字面量，以及 `abs(...)`、`log(...)`、`clip(...)`、`cast(...)`
- 未知标识符和不支持函数会在 `ReactiveStateEngine(...)` 构造阶段直接报错，而不是等到 `start()` 或 `write()` 才失败

最小示例：

```python
engine = zippy.ReactiveStateEngine(
    name="tick_expr",
    input_schema=schema,
    id_column="symbol",
    factors=[
        zippy.TS_EMA(column="price", span=2, output="ema_2"),
        zippy.Expr(expression="price + ema_2", output="price_plus_ema"),
        zippy.Expr(expression="clip(price_plus_ema, 20.0, 30.0)", output="clipped_total"),
    ],
    target=zippy.NullPublisher(),
)
```

时序表达式示例：

```python
bars = zippy.TimeSeriesEngine(
    name="bar_1m",
    input_schema=tick_schema,
    id_column="symbol",
    dt_column="dt",
    window=zippy.Duration.minutes(1),
    window_type=zippy.WindowType.TUMBLING,
    late_data_policy=zippy.LateDataPolicy.REJECT,
    pre_factors=[
        zippy.Expr(expression="price * volume", output="turnover_input"),
    ],
    factors=[
        zippy.AGG_FIRST(column="price", output="open"),
        zippy.AGG_LAST(column="price", output="close"),
        zippy.AGG_SUM(column="volume", output="volume"),
        zippy.AGG_SUM(column="turnover_input", output="turnover"),
    ],
    post_factors=[
        zippy.Expr(expression="close / open - 1.0", output="ret_1m"),
        zippy.Expr(expression="turnover / volume", output="vwap_1m"),
    ],
    target=zippy.NullPublisher(),
)
```

截面级联示例：

```python
cross_sectional = zippy.CrossSectionalEngine(
    name="cs_1m",
    source=bars,
    input_schema=bars.output_schema(),
    id_column="symbol",
    dt_column="window_start",
    trigger_interval=zippy.Duration.minutes(1),
    late_data_policy=zippy.LateDataPolicy.REJECT,
    factors=[
        zippy.CS_RANK(column="ret_1m", output="ret_rank"),
        zippy.CS_ZSCORE(column="ret_1m", output="ret_zscore"),
        zippy.CS_DEMEAN(column="ret_1m", output="ret_demean"),
    ],
    target=zippy.ZmqPublisher(endpoint="tcp://127.0.0.1:5557"),
)
```

运行顺序：

1. 在一个终端启动订阅侧：

```bash
uv run python examples/python/subscribe_bars.py
```

2. 在另一个终端启动发布侧：

```bash
uv run python examples/python/publish_pipeline.py
```

3. 如果要看本地 Parquet 归档示例，单独运行：

```bash
uv run python examples/python/archive_reactive.py
```

4. 如果要看三级级联到截面引擎的示例，单独运行：

```bash
uv run python examples/python/cross_sectional_pipeline.py
```

5. 如果要看跨进程 remote source 示例，先启动 downstream，再启动 upstream：

```bash
uv run python examples/python/remote_pipeline.py downstream
uv run python examples/python/remote_pipeline.py upstream
```
`remote_pipeline.py` 的 downstream 进程会直接打印最终状态和 metrics。

6. 如果要看 `StreamTableEngine` 的原始流透传示例，单独运行：

```bash
uv run python examples/python/stream_table_pipeline.py
```

如果要订阅 `cross_sectional_pipeline.py` 的输出，请使用 `tcp://127.0.0.1:5557`。
仓库里现成的 `subscribe_bars.py` 默认监听 `tcp://127.0.0.1:5556`，适用于
`publish_pipeline.py`，不直接复用到截面示例。
这个脚本会连续发布两批截面结果，并在启动后先等待订阅端完成 ZeroMQ 握手，
目的是降低 PUB/SUB 示例里“首包被握手窗口吃掉”的概率。
因为它直接消费 `TimeSeriesEngine` 的输出，所以 `CrossSectionalEngine` 的
`dt_column` 需要指向 bars 输出中的 `window_start`，而不是原始 tick 的 `dt`。

注意事项：

- 示例默认使用 `tcp://127.0.0.1:5555` 发布 reactive 输出，`tcp://127.0.0.1:5556` 发布 bars 输出
- `cross_sectional_pipeline.py` 默认使用 `tcp://127.0.0.1:5557` 发布 `CrossSectionalEngine` 输出
- `remote_pipeline.py` 默认使用 `tcp://127.0.0.1:5560` 作为远程 source 输入流，`tcp://127.0.0.1:5561` 发布 downstream 输出
- `stream_table_pipeline.py` 默认使用 `tcp://127.0.0.1:5562` 发布原始流，并写入 `/tmp/zippy-stream-table-demo/run-<timestamp>/`
- 发布侧遵守当前 source 生命周期约束：先启动 downstream，再写 source；停止时先停 source，再停 downstream
- `archive_reactive.py` 默认把文件写到 `/tmp/zippy-parquet-demo/input/` 和 `/tmp/zippy-parquet-demo/output/`
- 当前 Python API 使用预定义常量传递策略参数，例如 `zippy.WindowType.TUMBLING`、`zippy.LateDataPolicy.REJECT`、`zippy.OverflowPolicy.BLOCK`
- 远程 source 相关常量同样使用预定义值：`zippy.SourceMode.PIPELINE`、`zippy.SourceMode.CONSUMER`
- 当前 Python API 还支持 `engine.status()`、`engine.metrics()`、`engine.config()`，以及 `buffer_capacity` / `overflow_policy` / `archive_buffer_capacity` 这组运行时配置参数，`archive_reactive.py` 里有最小示例

远程 source 选择建议：

- `pipeline`：下游跟随上游 `Flush/Stop` 边界，适合正式处理链
- `consumer`：下游只消费 `Data`，忽略上游 `Flush`；如果需要结算，要由本地下游显式 `flush()`
- `remote_pipeline.py --mode consumer` 会在本地等待一段时间后手动 `flush()` 和 `stop()`，目的是把这种差异跑出来
