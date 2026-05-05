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
- `06_replay/`：从 persisted table / Parquet 回放到 StreamTable，并驱动下游 Engine。
- `07_ops/`：运行时表状态、master 元数据和运维观察入口。
- `08_remote_gateway/`：Windows/远端客户端通过 GatewayServer 写入、订阅和查询 WSL/Linux
  中的 named stream。

## 推荐学习顺序

1. 运行 `01_quickstart/02_create_stream_table_with_pipeline.py` 创建一张示例表。
2. 运行 `01_quickstart/01_connect_and_read_table.py` 读取这张表的最新数据。
3. 运行 `01_quickstart/03_run_pipeline_forever.py` 观察持续运行的 source 如何交给
   `Pipeline.run_forever()` 管理生命周期。
4. 运行 `03_query/02_query_expressions_and_dataframe.py` 熟悉表达式过滤和格式转换。
5. 用 `04_subscribe/` 里的脚本观察实时增量回调。
6. 用 `05_engines/01_reactive_latest_session.py` 把上游表聚合成最新快照表。
7. 用 `05_engines/03_named_stream_timeseries_session.py` 从 named StreamTable 直接生成 1m bar。
8. 用 `05_engines/04_named_stream_cross_sectional_session.py` 从 named StreamTable 直接计算截面排名。
9. 用 `06_replay/01_parquet_replay_to_stream_table.py` 做不开盘环境下的回放测试。
10. 用 `06_replay/02_replay_parity_check.py` 比较 live persisted 数据和 replay 输出。
11. 用 `06_replay/03_replay_to_reactive_latest_engine.py` 验证 replay stream 驱动下游 Engine。
12. 用 `07_ops/01_table_observability.py` 查看 master 中的表状态。
13. 用 `07_ops/02_table_perf_probe.py` 记录 `tail(n)` 延迟、底层表读取吞吐和可选 replay 吞吐。
14. 用 `07_ops/03_subscribe_latency_probe.py` 记录 append、subscriber 行级和 rollover 延迟。
15. 用 `07_ops/04_consumer_wait_for_table.py` 验证 consumer 先启动、producer 后注册表的场景。
16. 用 `07_ops/05_table_health_check.py` 检查 stale stream、persist 失败等健康告警。
17. 用 `zippy gateway run --uri default --endpoint 127.0.0.1:17666` 或
    `08_remote_gateway/01_start_gateway_server.py` 在 WSL/Linux 侧启动 GatewayServer。
18. 用 `08_remote_gateway/02_remote_writer.py` 模拟 Windows 侧行情源逐行写入。
19. 用 `08_remote_gateway/03_remote_subscribe_and_query.py` 模拟 Windows 侧策略订阅和主动查询。
20. 用 `zippy gateway smoke --master-uri tcp://127.0.0.1:28690 --gateway-endpoint 127.0.0.1:28666`
    跑跨进程远端写入和查询 smoke。
21. 在 Windows 侧用
    `zippy gateway smoke-client --uri zippy://<wsl-host>:17690/default --stream windows_smoke_ticks`
    验证只作为远端客户端访问 WSL/Linux Gateway 的路径。完整联调步骤见
    `docs/remote_gateway_windows_smoke_runbook.md`。
22. 如果 Windows 侧还没有 zippy wheel，可先用
    `08_remote_gateway/04_standalone_windows_smoke_client.py` 验证 Windows 到 WSL/Linux
    master/Gateway 的纯 Python wire protocol。

## API 分层

日常 Python 用户优先使用这几类接口：

- `zp.connect(uri="default", app="...")`：建立默认 master 连接并维护进程租约。
- `zp.Pipeline(...).source(...).stream_table(...).run_forever()`：长期运行 source 到
  StreamTable 的推荐入口，Ctrl-C 后统一停止 source 和 engine。
- `zp.Pipeline(...).stream_table(...)`：一次性写入或测试时，可以手动
  `start()` / `write()` / `flush()` / `stop()`。
- `zp.Session(...).engine(...).stream_table(...).run()`：编排下游 Engine，并把输出注册成可查询表。
- `zp.read_table("table_name")`：读取表，底层会拼接 persisted、sealed 和 active segment。
  若监控或策略进程需要先于 producer 启动，可用
  `zp.read_table("table_name", wait=True, timeout="30s")` 等待表注册。
- `zp.read_table("table_name").info()` / `.health()` / `.alerts()`：查看单张表的低频诊断信息。
- `zp.ops.list_tables()` / `zp.ops.table_info("table_name")`：查看 master 中的表状态和元数据。
- `zp.ops.table_health("table_name")` / `zp.ops.table_alerts("table_name")`：基于 master
  元数据汇总表健康状态和告警。
- `zp.subscribe(...)`：按行接收 `zp.Row`。
- `zp.subscribe_table(...)`：按批接收 `pyarrow.Table`。
  这两个订阅接口也支持 `wait=True`，用于 consumer 先启动、producer 后注册 stream 的场景。
- `zp.replay(...)`：把已持久化的 Zippy 表回放到 callback 或 named StreamTable。
  可用 `start` / `end` / `time_column` 做闭区间回放，用 `replay_rate` 指定固定
  rows/sec；下游需要先订阅时，使用 `zp.TableReplayEngine(...).init()` 建立输出流后再
  `run()`。
- `zp.compare_replay(...)`：把 live persisted 数据和 replay 输出按 key 对齐比较。
- `zp.ops.drop_table(...)`：删除表元数据，并可同步删除持久化数据。
- `zp.ops.compact_table(...)`：低频合并 persisted parquet 小文件，按 partition 分组
  生成 compacted parquet，并替换 master 中的 persisted metadata。
- `zp.ops.compact_tables(...)` / `zp.ops.start_compaction_worker(...)`：批量或后台执行
  低频 compaction；该类运维任务不属于写入热路径。
- `zp.GatewayServer(...)`：跨平台远端数据面服务，通常运行在 WSL/Linux 侧，由 master
  config 暴露 endpoint/token；Windows 侧继续使用 `connect()`、`get_writer()`、
  `subscribe_table()` 和 `read_table()`，不需要直接接触 mmap/segment。

`ParquetReplayEngine` 用于显式 parquet 路径回放。`ParquetReplaySource`、
`SegmentStreamSource` 等底层对象仍然可以使用；StreamTable 物化器保留在内部层，
示例默认不直接暴露这些细节。

## 运行约定

- 示例中的时间列使用 UTC 时间戳，字段名使用小写 snake_case。
- 会写数据的示例默认使用 `/tmp/zippy-examples`，避免污染工作目录。
- 持久化示例使用 `persist="parquet"`；不需要落盘时传 `persist=None` 或使用高层
  `Session.stream_table(..., persist=False)`。
- 多数脚本提供 `--drop-existing`，便于重复运行。生产环境不要随意使用这个参数。
