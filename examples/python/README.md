# Python Examples

这些示例展示了 Zippy V1 当前已经打通的最小用户路径：

- `publish_pipeline.py`：`ReactiveStateEngine -> TimeSeriesEngine` 进程内级联，并通过 `ZmqPublisher` 对外发布
- `subscribe_bars.py`：使用 `ZmqSubscriber` 订阅并读取一个 `RecordBatch`

运行顺序：

1. 在一个终端启动订阅侧：

```bash
uv run python examples/python/subscribe_bars.py
```

2. 在另一个终端启动发布侧：

```bash
uv run python examples/python/publish_pipeline.py
```

注意事项：

- 示例默认使用 `tcp://127.0.0.1:5555` 发布 reactive 输出，`tcp://127.0.0.1:5556` 发布 bars 输出
- 发布侧遵守当前 source 生命周期约束：先启动 downstream，再写 source；停止时先停 source，再停 downstream
