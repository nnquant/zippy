# Zippy Roadmap

更新时间：2026-04-26

本文记录 Zippy 从低层 segment 数据通道走向跨进程、可编排、可查询实时数据系统的路线。
重点目标是：保持低延迟数据面，同时让 Python 用户用少量概念完成实盘行情接入、因子计算和查询分析。

## 总体方向

Zippy 的长期形态不是 DolphinDB 风格的专用脚本语言，也不是让用户手动管理大量底层对象。
目标是用 Python 编排实时数据流，用 segment 做跨进程数据交换，用 Arrow 生态承接查询和分析。

核心原则：

1. 跨进程数据交换一律走 segment，不再保留旧 bus 或 POSIX shm fallback 作为主路径。
2. 用户以 named stream 为边界组织系统，例如 `source="ctp_ticks"`。
3. master 是 named stream catalog，负责发现、schema、descriptor、状态和生命周期元数据。
4. 内部 engine 消费 `SegmentRowView` / `SegmentTableView`，不再兼容旧 `RecordBatch on_data`。
5. 对外查询对象应尽量表现为 `zippy.Table`、`zippy.DataFrame` 或 Arrow 对象。
6. Zippy 不自研复杂查询语言。复杂查询交给 DuckDB、Polars、PyArrow Dataset。
7. 实时查询优先读 active/sealed segment，历史查询优先读 archived parquet。

## 当前基线

已经具备或正在迁移中的能力：

1. segment 数据路径已经成为主要方向。
2. OpenCTP source 可以通过 segment ingress 写入 active segment。
3. `SegmentStreamSource` 可以基于 master 上的 segment descriptor 跨进程 attach 数据。
4. 下游 engine 已开始迁移到 `SegmentTableView` 输入。
5. `zippy-segment-store` 已有 active segment reader、sealed segment、parquet persistence 的基础实现。
6. Python 侧已有 `ParquetSink` 归档能力，但它不是完整查询服务。

当前缺口：

1. master registry 还不是完整 named stream catalog，schema 和 stream metadata 不够 first-class。
2. `StreamTableEngine` 当前主要是 pass-through，不是可查询的 materialized live table。
3. 尚未提供用户级 `QueryEngine(source="ctp_ticks")`。
4. active、sealed、archived parquet 尚未统一成一个 Arrow-compatible query snapshot。
5. Pipeline API 还没有把 source、stream table、engine、sink 的生命周期统一托管。

## Milestone 0: 稳定 Segment-Only 数据路径

目标：把数据路径语义收敛，避免兼容路径导致的理解和维护成本。

工作项：

1. 移除 `data_path_mode` 这类兼容旧路径的用户参数。
2. 明确 `.mmap` 命名，避免 `.flink` 与 Apache Flink 产生歧义。
3. 不再保留旧 POSIX shm `os_id` fallback 作为默认或隐式路径。
4. 确认 master control plane 调用释放 GIL，避免 Python 控制面阻塞。
5. 将 `/dev/shm` SIGBUS 问题归因到 shm 生命周期和运行时清理，并补充清理与保护机制。

验收标准：

1. OpenCTP segment live e2e 能稳定跑通。
2. 多进程读写只依赖 segment descriptor。
3. `/dev/shm` 被占满时能给出明确错误，异常退出后有可追踪的清理路径。
4. 性能基准记录 queue、process、total latency，并保留回归阈值。

## Milestone 1: Named Stream Catalog

目标：让跨进程下游可以通过 `source="ctp_ticks"` 向 master 查询并绑定上游流。

工作项：

1. 扩展 master `StreamRecord`：
   - `stream_name`
   - `schema`
   - `schema_hash`
   - `data_path = "segment"`
   - `active_segment_descriptor`
   - `descriptor_generation`
   - `writer_process_id`
   - `status`
   - `created_at` / `updated_at`
2. 让 `register_stream` 真正注册 schema，而不是忽略 schema。
3. 增加 `resolve_stream(name)` 语义，返回完整 `StreamInfo`。
4. 支持 consumer 先启动：
   - 默认等待 named stream ready。
   - 支持 timeout。
   - 超时错误必须包含 stream name 和 master endpoint。
5. 检查 writer heartbeat，标记 stale stream。

目标 API：

```python
q = zippy.QueryEngine(source="ctp_ticks")
engine = zippy.ReactiveEngine(source="ctp_ticks", factors=[...])
```

验收标准：

1. 下游进程不需要手动传 schema。
2. 下游进程不需要手动创建 `SegmentStreamSource`。
3. schema mismatch 能在启动阶段报清楚。
4. writer 退出后，reader 能观察到 stale 状态。

## Milestone 2: Python Pipeline 用户 API

目标：减少用户手动创建 source、engine、publisher、handle、flush、stop 的心智负担。

推荐 API：

```python
(
    zippy.Pipeline("ctp_ingest", master="unix:///tmp/zippy-master.sock")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table("ctp_ticks")
    .run_forever()
)
```

```python
(
    zippy.Pipeline("tick_factors", master="unix:///tmp/zippy-master.sock")
    .reactive(source="ctp_ticks", factors=[...])
    .run_forever()
)
```

工作项：

1. 引入 `Pipeline` 作为生命周期 owner。
2. `Pipeline` 负责 start、flush、stop、异常传播和资源关闭。
3. 组件仍然保留明确名称：`OpenCtpMarketDataSource`、`StreamTableEngine`、`TimeSeriesEngine`。
4. 用户可以显式写组件，但推荐 Pipeline 托管。
5. Pipeline 内部统一处理 named stream resolution。

验收标准：

1. OpenCTP -> StreamTable 的最小代码不需要手动管理 publisher。
2. ReactiveEngine 跨进程订阅 named stream 不需要手动创建 source。
3. Ctrl-C 或异常退出时，source、engine、archive、segment 资源能正确关闭。

## Milestone 3: StreamTable Materialization

目标：`stream_table("ctp_ticks")` 不再只是 pass-through，而是一个可查询的 named live table。

工作项：

1. StreamTable 维护 active segment。
2. rollover 后生成 sealed segment。
3. 保留最近若干 sealed segments，用于 live retention 查询。
4. 支持 retention 策略：
   - `retention_rows`
   - `retention_seconds`
   - `retention_segments`
5. 可选归档到 parquet。
6. 向 master 发布 live table metadata。

推荐 API：

```python
(
    zippy.Pipeline("ctp_ingest")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table(
        "ctp_ticks",
        retention_rows=1_000_000,
        archive=zippy.ParquetArchive("data/ctp_ticks"),
    )
    .run_forever()
)
```

验收标准：

1. `ctp_ticks` 能被跨进程发现。
2. active segment 中最新数据可查询。
3. sealed segment 中最近数据可查询。
4. retention 超限时给出明确错误或 fallback 到 archive。
5. parquet archive 与 live retention 的边界可追踪。

## Milestone 4: QueryEngine 与 Arrow 查询层

目标：让用户通过 `QueryEngine(source="ctp_ticks")` 查询实时和历史数据，并把复杂查询交给 Arrow 生态。

推荐 API：

```python
q = zippy.QueryEngine(source="ctp_ticks")

schema = q.schema()
latest = q.tail(1000)
reader = q.scan_live()
snapshot = q.snapshot()
```

小结果返回 `pyarrow.Table`：

```python
tbl = q.tail(1000)
```

大结果返回批流或 dataset：

```python
reader = q.scan_live()
archive = q.scan_archive()
```

工作项：

1. `QueryEngine` 通过 master resolve named stream。
2. 建立一致性 `QuerySnapshot`：
   - active descriptor
   - active committed row high watermark
   - retained sealed segment list
   - archived parquet file list
   - schema hash
3. `tail(n)` 支持从 active + sealed 拼接最新 n 行。
4. `scan_live()` 返回 Arrow `RecordBatchReader` 或等价对象。
5. `scan_archive()` 返回 `pyarrow.dataset.Dataset` 或 parquet file list。
6. `query(columns=[...], n=...)` 只做轻量 projection，不做 SQL。

验收标准：

1. `q.tail(1000)` 能跨进程返回最新 1000 行。
2. active + sealed 数据可以物化为 `pyarrow.RecordBatch` / `pyarrow.Table`。
3. archived parquet 可以通过 PyArrow Dataset、DuckDB、Polars 查询。
4. 大数据查询不强制物化成单个巨大 `pyarrow.Table`。
5. snapshot 创建后，上游继续写入不影响本次查询边界。

## Milestone 5: Arrow Ecosystem Integration

目标：让 Zippy 查询结果自然进入 DuckDB、Polars、PyArrow。

工作项：

1. 暴露 `to_arrow()`、`to_arrow_reader()`、`to_pyarrow_dataset()`。
2. 小数据支持：

```python
tbl = q.tail(1000)
df = pl.from_arrow(tbl)
```

3. 大数据支持：

```python
reader = q.scan_live()
files = q.archive_files()
```

4. 推荐 DuckDB / Polars 处理复杂查询：

```python
duckdb.from_arrow(tbl).query("t", "select instrument_id, avg(last_price) from t group by 1")
```

验收标准：

1. 不引入自研 SQL parser。
2. DuckDB 可以消费 live snapshot 的 Arrow 数据。
3. Polars 可以消费 live snapshot 的 Arrow 数据。
4. parquet archive 可以被 DuckDB / Polars lazy scan。

## Milestone 6: Downstream Engine Segment-Native Composition

目标：让 `TimeSeriesEngine`、`CrossSectionalEngine`、`ReactiveEngine` 都能直接接收 segment-native 输入。

工作项：

1. engine 内部以 `SegmentTableView` 为主要输入。
2. 避免在热路径把数据强制转成 `RecordBatch`。
3. 只在 operator 或外部查询边界物化 Arrow。
4. 支持 `source="ctp_ticks"` 作为跨进程 named stream 输入。
5. 支持 Pipeline 中进程内连接和跨进程连接使用一致 API。

验收标准：

1. ReactiveEngine 可跨进程读取 `ctp_ticks`。
2. TimeSeriesEngine 可跨进程读取 `ctp_ticks` 并生成 bar 或窗口因子。
3. CrossSectionalEngine 可读取上游 engine 输出。
4. e2e 延迟和吞吐有固定基准。

## Milestone 7: 存储与 Retention 策略

目标：明确 active、sealed、archive 三层存储的职责。

职责划分：

1. active segment：最新写入，低延迟读取。
2. sealed segment：近期不可变数据，支持 live tail 和 replay。
3. archived parquet：历史数据，支持大数据查询和离线分析。

工作项：

1. 定义 segment rollover 策略。
2. 定义 sealed segment retention 策略。
3. 定义 parquet archive 文件命名和分区策略。
4. master 记录 archive metadata 或可发现路径。
5. 支持 archive flush 指标和失败事件。

验收标准：

1. live retention 内查询不依赖 parquet。
2. 超过 live retention 的历史查询可定位到 parquet。
3. archive 写入失败不会静默丢失。
4. 数据源、schema、segment id、archive file 之间可追踪。

## Milestone 8: 测试、性能与可运维性

目标：把实盘链路变成可重复验证的工程资产。

必须覆盖的测试链路：

1. Python MasterServer -> segment descriptor -> SegmentStreamSource -> StreamTableEngine。
2. OpenCTP source -> stream_table("ctp_ticks") -> reactive(source="ctp_ticks")。
3. OpenCTP source -> stream_table("ctp_ticks") -> QueryEngine.tail(1000)。
4. active + sealed + archived parquet 的 snapshot 查询。
5. writer crash、reader restart、master stale 状态。
6. `/dev/shm` 容量不足和异常退出后的资源清理。

性能基准：

1. segment write latency。
2. segment read latency。
3. OpenCTP live queue/process/total latency。
4. `tail(1000)`、`tail(100000)` 查询耗时。
5. Arrow materialization 吞吐。
6. parquet archive flush 吞吐。

可运维指标：

1. active segment committed rows。
2. segment generation。
3. sealed segment count。
4. archive flush success/failure。
5. reader count。
6. stale stream count。
7. shm bytes allocated。

## 暂不做事项

1. 不自研 SQL 查询语言。
2. 不做 DolphinDB DOS 风格编排语言。
3. 不保留旧 bus/POSIX shm fallback 作为用户可见主路径。
4. 不在第一版追求所有 Arrow 导出零拷贝。
5. 不把大数据查询强制物化成单个 `pyarrow.Table`。
6. 不在第一版实现分布式 query planner。

## 推荐实现顺序

1. 先完成 master named stream catalog。
2. 再做 `source="ctp_ticks"` resolution。
3. 接着做 Pipeline 生命周期托管。
4. 然后把 StreamTable 做成 materialized live table。
5. 再实现 `QueryEngine.tail()` 和 `QuerySnapshot`。
6. 最后补齐 Arrow reader、archive dataset 和大数据查询路径。

这个顺序的原因是：跨进程命名流是所有下游能力的根。如果 master 不能稳定描述
`ctp_ticks`，Pipeline、ReactiveEngine、QueryEngine 都会被迫重新暴露底层对象。

## 第一阶段建议范围

第一阶段应保持足够小，目标是打穿用户最关心的实盘路径：

```python
(
    zippy.Pipeline("ctp_ingest")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table("ctp_ticks", retention_rows=1_000_000)
    .run_forever()
)
```

```python
q = zippy.QueryEngine(source="ctp_ticks")
latest = q.tail(1000)
```

```python
(
    zippy.Pipeline("tick_factors")
    .reactive(source="ctp_ticks", factors=[...])
    .run_forever()
)
```

第一阶段完成后，Zippy 应该已经具备一个清晰的用户心智模型：

```text
OpenCTP source 写入 named stream。
ReactiveEngine 通过 named stream 做实时计算。
QueryEngine 通过 named stream 查询 Arrow 数据。
Pipeline 托管生命周期。
master 负责跨进程发现和元数据。
```
