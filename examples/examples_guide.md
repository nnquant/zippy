# Zippy 示例指南

这组示例按使用场景组织，目标是让 Python 用户先掌握高层 API，再按需理解底层组件。

## 环境准备

大多数跨进程示例都需要先启动本地 `zippy-master`：

```bash
uv run zippy master run default
```

`default` 会解析到 `~/.zippy/control_endpoints/default/master.sock`。如果你使用其他 master，
运行示例时传入 `--uri <name-or-path>`，例如：

```bash
uv run python examples/01_quickstart/01_connect_and_read_table.py --uri default
```

查看 master 当前注册的表：

```bash
uv run zippy stream ls --uri default
```

## 目录结构

- `01_quickstart/`：连接 master、创建最小 StreamTable、读取最新数据。
- `02_stream_table/`：StreamTable 的持久化、分区和删除。
- `03_query/`：`read_table()`、`tail()`、`collect()`、表达式查询和 DataFrame 转换。
- `04_subscribe/`：行级订阅和表级批量订阅。
- `05_engines/`：`Session` 编排、`ReactiveLatestEngine`、时序和截面引擎。
- `06_replay/`：从 Parquet 回放到 StreamTable，用于不开盘时做系统测试。

## 推荐学习顺序

1. 运行 `01_quickstart/02_create_stream_table_with_pipeline.py` 创建一张示例表。
2. 运行 `01_quickstart/01_connect_and_read_table.py` 读取这张表的最新数据。
3. 运行 `03_query/02_query_expressions_and_dataframe.py` 熟悉表达式过滤和格式转换。
4. 用 `04_subscribe/` 里的脚本观察实时增量回调。
5. 用 `05_engines/01_reactive_latest_session.py` 把上游表聚合成最新快照表。
6. 用 `06_replay/01_parquet_replay_to_stream_table.py` 做不开盘环境下的回放测试。

## API 分层

日常 Python 用户优先使用这几类接口：

- `zp.connect(uri="default", app="...")`：建立默认 master 连接并维护进程租约。
- `zp.Pipeline(...).stream_table(...)`：把一个 source 或手动写入的数据物化成 StreamTable。
- `zp.Session(...).engine(...).stream_table(...).run()`：编排下游 Engine，并把输出注册成可查询表。
- `zp.read_table("table_name")`：读取表，底层会拼接 persisted、sealed 和 active segment。
- `zp.subscribe(...)`：按行接收 `zp.Row`。
- `zp.subscribe_table(...)`：按批接收 `pyarrow.Table`。
- `zp.drop_table(...)`：删除表元数据，并可同步删除持久化数据。

`StreamTableEngine`、`SegmentStreamSource` 等底层对象仍然可以使用，但示例默认不直接暴露这些细节。

## 运行约定

- 示例中的时间列使用 UTC 时间戳，字段名使用小写 snake_case。
- 会写数据的示例默认使用 `/tmp/zippy-examples`，避免污染工作目录。
- 持久化示例使用 `persist="parquet"`；不需要落盘时传 `persist=None` 或使用高层
  `Session.stream_table(..., persist=False)`。
- 多数脚本提供 `--drop-existing`，便于重复运行。生产环境不要随意使用这个参数。

