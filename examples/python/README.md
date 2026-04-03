# Python Examples

这些示例展示了 Zippy V1 当前已经打通的最小用户路径：

- `publish_pipeline.py`：`ReactiveStateEngine -> TimeSeriesEngine` 进程内级联，并通过 `ZmqPublisher` 对外发布
- `subscribe_bars.py`：使用 `ZmqSubscriber` 订阅并读取一个 `RecordBatch`
- `archive_reactive.py`：使用 `ParquetSink` 归档 `ReactiveStateEngine` 的输入和输出

表达式因子：

- 当前 Python API 额外支持 `zippy.EXPR(expression="price + ema_2", output="price_plus_ema")`
- 表达式运行在 `ReactiveStateEngine` 内，支持引用输入列和前序 reactive factor 输出
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
        zippy.EXPR(expression="price + ema_2", output="price_plus_ema"),
        zippy.EXPR(expression="clip(price_plus_ema, 20.0, 30.0)", output="clipped_total"),
    ],
    target=zippy.NullPublisher(),
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

注意事项：

- 示例默认使用 `tcp://127.0.0.1:5555` 发布 reactive 输出，`tcp://127.0.0.1:5556` 发布 bars 输出
- 发布侧遵守当前 source 生命周期约束：先启动 downstream，再写 source；停止时先停 source，再停 downstream
- `archive_reactive.py` 默认把文件写到 `/tmp/zippy-parquet-demo/input/` 和 `/tmp/zippy-parquet-demo/output/`
- 当前 Python API 还支持 `engine.status()`、`engine.metrics()`、`engine.config()`，以及 `buffer_capacity` / `overflow_policy` / `archive_buffer_capacity` 这组运行时配置参数，`archive_reactive.py` 里有最小示例
