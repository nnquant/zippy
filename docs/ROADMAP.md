# Zippy Roadmap

> 更新时间：2026-04-28
> 目标：把 Zippy 从底层实时数据通道演进为「量化实时行情与因子计算基础设施」。
> 本文重点覆盖 **named stream、StreamTable、Table、persist/replay、Pipeline 生命周期、可靠性与性能验收**。
> 因子系统的具体建模方式暂不在本文中定稿，仅保留必要的接入点和演进边界。

---

## 1. 项目定位

Zippy 的长期目标不是复刻 DolphinDB，也不是做一门专用脚本语言，而是成为一个更轻量、更透明、更容易嵌入策略系统的实时数据与因子基础设施。

目标形态：

```text
行情事件流
  -> named stream
  -> materialized StreamTable
  -> realtime query snapshot
  -> downstream engines / factor engines
  -> factor/result named stream
  -> strategy subscribe + query history
  -> persist + replay
```

核心定位：

```text
Rust 负责低延迟数据面；
Python 负责编排、接入和策略侧使用；
segment 负责跨进程实时交换；
Arrow/Polars/DuckDB 负责查询分析；
Parquet 负责历史持久化；
master 负责 named stream catalog、schema、descriptor、状态和生命周期。
```

---

## 2. 设计原则

### 2.1 用户只应该理解少量概念

用户侧核心概念应尽量收敛到：

```text
Pipeline
Named Stream
Table
Table
Persist
```

内部实现和架构讨论可以继续使用：

```text
StreamTable
StreamTableMaterializer
SegmentTableView
TableRuntime / TableExecutor
TableSnapshot
```

不要让用户手动管理：

```text
shared memory object
segment descriptor
publisher handle
reader handle
low-level bus frame
manual flush/stop lifecycle
```

### 2.2 对外命名要避免实现泄漏

对外 API 中：

```text
Table      表示用户可查询、可导出到 Arrow/Polars/DuckDB 的数据对象；
read_table 表示打开 named table 的用户入口；
Engine     表示持续运行的计算组件，例如 TimeSeriesEngine；
Pipeline   表示 source、materializer、engine、persist 的生命周期 owner。
```

内部实现中：

```text
StreamTable 表示由实时流物化出来的 named live table；
StreamTableMaterializer 表示负责写 active segment、维护 watermark、更新 master metadata 的组件；
TableRuntime / TableExecutor 表示执行查询的内部实现；
TableSnapshot 表示一次查询的固定边界。
```

`StreamTableEngine` 和 `QueryEngine` 容易让用户误以为它们和计算 engine 同类。后续不应作为
公开主 API；若保留，只应作为 internal/deprecated 过渡命名。

### 2.3 named stream 是系统边界

所有跨进程、跨组件、跨引擎的连接都应该通过 named stream 表达：

```python
source="ctp_ticks"
source="factor.tick.microstructure.v1"
```

而不是传递底层 descriptor、schema、路径或内部句柄。

### 2.4 同一份数据必须同时支持 push 和 pull

实盘系统需要两种访问模式：

```text
push：逐 tick 订阅最新事件或因子；
pull：策略随时查询最近 N 条、某个窗口、某个快照。
```

因此 Zippy 的中心不是单纯的 message bus，也不是单纯的 database，而是：

```text
可订阅、可查询、可归档、可回放的 materialized stream table。
```

### 2.5 查询必须有一致边界

实时查询不能直接读取“正在变化的无限流”而不定义边界。所有查询都应基于 `TableSnapshot`：

```text
snapshot 创建时的 active segment descriptor
snapshot 创建时的 committed row high watermark
retained sealed segments
persisted file list
schema hash
```

`snapshot` 创建后，上游继续写入不影响本次查询的边界。

### 2.6 不自研复杂查询语言

Zippy 不做 SQL parser，不做通用 OLAP 引擎。

推荐做法：

```text
实时小窗口：Table.tail/window/current
复杂分析：DuckDB / Polars / PyArrow Dataset
历史数据：Parquet persist + Arrow ecosystem
```

### 2.7 因子系统暂不提前定死

本文暂不定义完整 factor DSL、FactorSpec、factor graph、factor versioning 细节。

但基础设施必须预留：

```text
downstream engine 能订阅 named stream；
downstream engine 能输出 named stream；
输出结果能被 StreamTable materialize；
输出结果能被 Table 实时查询；
输出结果能 persist 和 replay。
```

也就是说，本文先把“因子系统需要依赖的地基”做好。

---

## 3. 术语

### Named Stream

一个由 master 管理的命名数据流，例如：

```text
ctp_ticks
bar.1m
factor.tick.microstructure.v1
```

Named Stream 至少包含：

```text
stream_name
schema
schema_hash
data_path
active_segment_descriptor
sealed_segment metadata
persist metadata
status
writer identity
created_at / updated_at
```

### Table

用户侧的数据对象。它表示一个 named stream 在某个查询边界下可读取的数据视图，可以导出为
Arrow、Polars 或 DuckDB 可消费的对象。

第一版 `Table` 可以先由 `Table.tail()` 返回 `pyarrow.Table` 承接；后续再引入
`zippy.Table` 包装 Arrow reader、active snapshot、persisted dataset 等不同后端。

### StreamTable

内部物化层概念，表示把一个实时事件流物化成可查询实时表的组件。

它不是普通 sink，也不是 pass-through engine，而是 live table owner：

```text
维护 active segment；
维护 committed row high watermark；
维护 sealed segment；
执行 retention；
可选归档到 parquet；
向 master 发布 metadata。
```

### Active Segment

当前正在写入的 segment。用于最低延迟的最新数据读取。

### Sealed Segment

已经 rollover、不再写入的 segment。用于近期 live retention 查询和 replay。

### Persist

历史数据层，优先使用 Parquet，面向 PyArrow Dataset、DuckDB、Polars 等生态。

### TableSnapshot

一次查询的边界描述。用于保证 active + sealed + persisted parquet 拼接时不重、不漏、边界清晰。

### Pipeline

用户侧生命周期 owner，负责 source、engine、StreamTable、persist、resource cleanup、error propagation。

---

## 4. 当前主要问题

### 4.1 Master 还不是完整的 stream catalog

当前 master 更像 control-plane registry，还没有把 schema、schema_hash、descriptor generation、persist metadata、created_at/updated_at 等信息完整变成一等公民。

目标是让下游只写：

```python
q = zippy.read_table("ctp_ticks")
```

而不是手动传递 schema、descriptor、底层 source 对象。

### 4.2 StreamTable 还不是真正的 materialized live table

`StreamTableEngine` 不应只是 pass-through。它应该负责 active segment、sealed segment、retention、persist 和 master metadata 更新。

### 4.3 Table 尚未形成用户级闭环

必须让用户能够直接：

```python
q = zippy.read_table("ctp_ticks")
q.tail(1000)
q.snapshot()
q.scan_live()
q.scan_persisted()
```

否则 Zippy 只能推送数据，不能满足策略实时查询历史窗口的核心需求。

### 4.4 数据路径需要继续收敛

热路径应尽量收敛到 segment-native，不要长期并存多套数据路径。

原则：

```text
跨进程实时交换：segment
外部查询边界：Arrow
历史归档：Parquet
复杂查询：Polars/DuckDB/PyArrow
```

### 4.5 Pipeline 生命周期需要统一

用户不应该手动管理每个组件的 start、flush、stop、publisher、reader、persist close。

Pipeline 应该成为推荐入口。

### 4.6 历史查询语义需要先定义

尤其要明确：

```text
tail(1000) 是全 stream 最近 1000 行，还是 key 内最近 1000 行？
时间窗口用 event_ts、recv_ts 还是 calc_ts？
active + sealed + persisted parquet 如何拼接？
上游正在写时查询边界如何固定？
retention 超限后如何 fallback 到 persisted parquet？
```

---

## 5. 目标架构

```text
┌─────────────────────────────────────────────────────────────────┐
│                           Python API                             │
│ Pipeline / Table / Persist / downstream engines             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Master Catalog                           │
│ stream_name -> schema / descriptor / status / retention / persist │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                           Data Plane                             │
│ active segment -> sealed segments -> parquet persist              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Table Read Plane                              │
│ TableSnapshot -> tail / scan_live / scan_persisted -> Arrow ecosystem│
└─────────────────────────────────────────────────────────────────┘
```

推荐实盘链路：

```python
(
    zippy.Pipeline("ctp_ingest", master="unix:///tmp/zippy-master.sock")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table(
        "ctp_ticks",
        schema=TickSchema,
        retention_rows=1_000_000,
        persist="parquet",  # 可省略，由 master 全局配置决定
        data_dir="data",
    )
    .run_forever()
)
```

策略查询：

```python
q = zippy.read_table("ctp_ticks")
latest = q.tail(1000)
snapshot = q.snapshot()
reader = q.scan_live()
persisted = q.scan_persisted()
```

下游计算链路，因子系统细节暂不定稿：

```python
(
    zippy.Pipeline("downstream_compute", master="unix:///tmp/zippy-master.sock")
    .engine(source="ctp_ticks", engine=some_engine)
    .stream_table(
        "some_output_stream.v1",
        schema=SomeOutputSchema,
        retention_rows=2_000_000,
        persist="parquet",
        data_dir="data",
    )
    .run_forever()
)
```

---

## 6. Roadmap 总览

```text
M0  数据契约与命名规范
M1  Segment-only 数据路径收敛
M2  Named Stream Catalog
M3  StreamTable Materialization
M4  TableSnapshot 与 Table
M5  Persist 与 Retention
M6  Pipeline 生命周期 API
M6.5 Low-latency IPC v2
M7  Segment-native downstream engine composition
M8  Replay / Backfill / Live-Replay parity
M9  测试、性能、可运维性
M10 后续专题：factor system、索引、多节点、权限、完整查询优化
```

建议优先级：

```text
第一阶段：M0 + M1 + M2 + M3 + M4.tail(active-only)
第二阶段：M4 + M5
第三阶段：M6 + M6.5 + M7 + M8
第四阶段：M9 持续完善
```

M6.5 是 M7 的前置基础。当前 master long-poll 已经能消除固定 50ms descriptor
轮询长尾，适合作为 correctness baseline；长期低延迟 IPC v2 应在 Pipeline/Persist
语义稳定后、下游 engine 大规模 segment-native 化之前推进，避免 M7 完成后再反复改
reader attach、rollover、wakeup 和资源清理协议。

---

## 7. Milestone 0：数据契约与命名规范

### 目标

先明确系统语义，再写更多 engine。避免后续所有组件对 timestamp、sequence、schema、stream name 的解释不一致。

### 工作项

#### 7.1 定义基础字段语义

建议 tick / event 类 stream 至少支持以下语义字段：

```text
seq             系统内单调递增序列
trading_day     交易日
event_ts        事件发生时间，优先交易所时间
recv_ts         本机接收时间
source_id       数据源标识
instrument_id   合约/股票/资产标识
```

计算结果类 stream 可额外包含：

```text
upstream_seq    触发本条结果的上游 seq
calc_ts         计算完成时间
engine_name     产生该结果的 engine
engine_version  产生该结果的 engine 版本，可选
```

#### 7.2 定义 stream naming convention

推荐：

```text
ctp_ticks
market.tick.ctp
bar.1m
bar.5m
factor.tick.microstructure.v1     # 暂作为示例，不定稿 factor 体系
engine.output_name.v1
```

命名规则：

```text
使用小写；
用 dot 表达层级；
用 v1/v2 表达 schema 或语义版本；
不要把运行环境写进 stream name，例如 prod/dev；环境应由 master endpoint 或 namespace 区分。
```

#### 7.3 定义 schema version / schema hash

每个 named stream 必须有：

```text
schema
schema_hash
schema_version 可选
```

schema mismatch 必须在启动阶段报错，而不是运行中隐式失败。

#### 7.4 定义查询时间语义

第一版建议：

```text
tail(n) 默认按 append order / seq；
window(last="5m") 默认按 event_ts，但必须显式记录；
persist partition 默认按 trading_day；
current(key=...) 属于后续扩展，不作为 Table v0 必须项。
```

### 验收标准

- 文档中固定字段语义和命名规则。
- 测试 schema mismatch 时能稳定报错。
- stream name、schema hash、timestamp 字段进入 master metadata。
- 所有 e2e demo 使用统一命名。

---

## 8. Milestone 1：Segment-only 数据路径收敛

### 目标

把实时数据路径收敛到 segment-native，减少长期维护多套热路径的复杂度。

### 工作项

#### 8.1 明确主路径

用户可见主路径：

```text
source -> StreamTable -> active segment -> sealed segment -> persist
```

source 负责产生 rows/events，StreamTable 负责拥有 named stream 的 active segment 和
committed row high watermark。跨进程消费者通过 master 获取 descriptor 后 attach segment。

插件或数据源内部可以使用 segment ingress 优化采集链路，但它不应替代用户可见 named
stream 的 StreamTable owner 语义。

#### 8.2 降低旧路径存在感

- 不再把旧 bus / POSIX shm fallback 作为用户可见主路径。
- 旧 bus / POSIX shm fallback 不再作为公开 API 或 examples 路径。
- 遗留代码只允许作为 internal test/legacy 过渡存在，后续可直接删除。
- 文档和 examples 不再推荐旧路径。

#### 8.3 engine 输入向 segment-native 迁移

热路径中 engine 尽量消费：

```text
SegmentRowView
SegmentTableView
```

不要在每个环节强制转换成 Arrow `RecordBatch`。

Arrow 应该出现在：

```text
查询边界；
归档边界；
外部 Python 分析边界；
必要的 operator 边界。
```

#### 8.4 shm / mmap 生命周期治理

需要处理：

```text
/dev/shm 空间不足；
writer 异常退出；
reader attach stale descriptor；
segment 文件或 shm 对象清理；
SIGBUS 风险；
```

阶段边界：

```text
短期：master descriptor long-poll 作为切段发现机制，去掉固定 50ms 轮询长尾；
长期：M6.5 Low-latency IPC v2 将 descriptor generation、committed watermark、
      rollover signal 等热路径状态下沉到 segment-native control block。
```

### 验收标准

- OpenCTP 或 mock source 可以写入 active segment。
- 另一个进程可以通过 master descriptor attach 读取。
- examples 中不需要用户手动选择旧数据路径。
- `/dev/shm` 空间不足时给出明确错误。
- writer 异常退出后不会导致 reader 静默读取脏数据。

---

## 9. Milestone 2：Named Stream Catalog

### 目标

让 master 从简单 registry 演进为完整的 stream catalog。下游只依赖 stream name 即可发现、校验、订阅、查询数据。

### 推荐数据结构

```rust
struct StreamInfo {
    stream_name: String,
    schema: Schema,
    schema_hash: String,
    data_path: DataPathKind, // segment
    active_segment_descriptor: Option<SegmentDescriptor>,
    descriptor_generation: u64,
    sealed_segments: Vec<SealedSegmentInfo>,
    persist: Option<PersistInfo>,
    writer: Option<WriterInfo>,
    status: StreamStatus,
    created_at: Timestamp,
    updated_at: Timestamp,
}
```

状态建议：

```text
CREATING
READY
STALE
CLOSED
ERROR
```

### 工作项

#### 9.1 `register_stream` 必须注册 schema

注册 named stream 时，schema 不能被忽略。

需要持久或半持久记录：

```text
schema
schema_hash
stream_name
data_path
writer identity
created_at
```

#### 9.2 增加 `resolve_stream(name)`

返回完整 `StreamInfo`。

语义：

```python
info = master.resolve_stream("ctp_ticks")
```

应支持：

```text
wait=True/False
timeout
expected_schema_hash
expected_status
```

#### 9.3 支持 consumer 先启动

策略、下游 engine、Table 可以先启动并等待上游 stream ready。

错误信息必须包含：

```text
stream name
master endpoint
timeout
当前已知状态
```

#### 9.4 writer heartbeat / stale detection

master 需要能判断 writer 是否仍然存活。

最小实现可以是：

```text
writer 注册 pid/process id；
writer 定期 heartbeat；
master 根据 heartbeat 超时标记 STALE；
reader resolve 时看到 STALE 必须明确提示。
```

#### 9.5 descriptor generation

每次 active segment rollover 或 descriptor 更新，都递增 generation。

消费者可用 generation 判断是否需要重新 attach。

### 目标 API

```python
q = zippy.read_table("ctp_ticks")
info = q.stream_info()
```

```python
engine = zippy.SomeEngine(source="ctp_ticks")
```

### 验收标准

- 下游进程不需要手动传 schema。
- 下游进程不需要手动创建 `SegmentStreamSource`。
- schema mismatch 能在启动阶段报清楚。
- consumer 先启动后，上游 stream ready 时能自动继续。
- writer 退出后，reader 能看到 STALE 状态。
- descriptor rollover 后，下游可感知 generation 变化。

---

## 10. Milestone 3：StreamTable Materialization

### 目标

让 `stream_table("ctp_ticks")` 成为真正的可查询 live table，而不是 pass-through engine。

### StreamTable 职责

```text
创建/维护 active segment；
append incoming rows；
维护 committed row high watermark；
rollover active segment；
生成 sealed segment metadata；
执行 retention；
可选写入 parquet persist；
向 master 更新 StreamInfo；
暴露 live query metadata。
```

### 推荐 API

```python
(
    zippy.Pipeline("ctp_ingest")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table(
        "ctp_ticks",
        schema=TickSchema,
        retention_rows=1_000_000,
        retention_segments=16,
        persist="parquet",
        data_dir="data",
    )
    .run_forever()
)
```

### 工作项

#### 10.1 active segment ownership

StreamTable 必须明确拥有 active segment 的写入权限。

同一个 stream 同一时间只允许一个 writer owner，除非未来明确支持 multi-writer。

#### 10.2 committed row high watermark

写入时需要区分：

```text
allocated rows
written rows
committed rows
```

TableSnapshot 只能读取 committed row high watermark 以内的数据。

#### 10.3 rollover

支持按以下策略 rollover：

```text
max_rows_per_segment
max_bytes_per_segment
max_duration_per_segment
manual flush/rollover
```

#### 10.4 sealed segment metadata

rollover 后 master 记录：

```text
segment_id
schema_hash
row_count
min_seq / max_seq
min_event_ts / max_event_ts 可选
path / shm id / mmap path
created_at / sealed_at
```

#### 10.5 retention

支持：

```text
retention_rows
retention_seconds
retention_segments
```

retention 删除或释放 sealed segment 前，必须保证 persist 状态明确。

当前已落地：

```text
retention_segments
```

`retention_segments` 已接入 StreamTable descriptor retention；未启用 persist 时按最近 N 个
sealed segment 裁剪 live descriptor，启用 persist 时必须等对应 sealed segment 的 parquet
metadata 发布成功后才允许裁剪，避免查询层在 live/persisted 之间出现空洞。

#### 10.6 persist hook

StreamTable 可选接入持久化；默认行为由 master 全局配置决定：

```python
persist="parquet"  # 或 None
```

persist 失败不能静默丢失。至少要进入 ERROR 状态或产生 error event。

### 验收标准

- `stream_table("ctp_ticks")` 能被 master 发现。
- active segment 中最新数据可查询。
- sealed segment 中近期数据可查询。
- active + sealed 能支持 `tail(n)`。
- rollover 不导致查询不重不漏。
- retention 超限时有明确错误或 persisted fallback。
- persist 写入失败不会静默丢失。

---

## 11. Milestone 4：TableSnapshot 与 Table

### 目标

让用户可以通过 `read_table("...")` 查询实时和历史数据。

第一版只做轻量查询，不做完整 SQL。

### 推荐 API

```python
q = zippy.read_table("ctp_ticks")

schema = q.schema()
info = q.stream_info()
latest = q.tail(1000)
table = q.collect()
reader = q.reader()
arrow_table = q.to_pyarrow()
pandas_df = q.to_pandas()
polars_df = q.to_polars()

filtered = (
    q.select([
        zippy.col("dt"),
        zippy.col("instrument_id"),
        zippy.col("last_price"),
        (zippy.col("ask_price_1") - zippy.col("bid_price_1")).alias("spread"),
    ])
    .where(
        (zippy.col("instrument_id") == "IF2606")
        & (zippy.col("last_price") > 4000)
    )
    .between(zippy.col("dt"), start_ns, end_ns)
    .tail(1000)
)

# lower-level storage/debug APIs
snapshot = q.snapshot()
live_reader = q.scan_live()
persisted = q.scan_persisted()
```

可选轻量 projection：

```python
tbl = q.tail(
    1000,
    columns=["event_ts", "instrument_id", "last_price", "volume"],
)
```

### TableSnapshot 结构

```python
snapshot = {
    "stream_name": "ctp_ticks",
    "schema_hash": "...",
    "active_segment_descriptor": "...",
    "active_committed_row_high_watermark": 123456,
    "sealed_segments": [...],
    "persisted_files": [...],
    "descriptor_generation": 7,
    "created_at": "...",
}
```

### 查询语义

#### 11.1 `tail(n)`

第一版语义：

```text
在 snapshot 创建时刻，从 persisted parquet + retained sealed segments + active 中按 append order 取最新 n 行。
```

如果 retained live 数据不足：

```text
默认从 persisted parquet 补齐；
如果 persisted + live 仍不足，则返回可用数据，返回行数可以小于 `n`；
用户不需要关心数据来自 active、sealed 还是 persisted；
底层通过 source segment identity 排除 retained sealed 与 persisted parquet 的重复数据。
```

#### 11.2 `scan_live()`

底层存储接口，返回 active + retained sealed 的 Arrow reader 或等价批流。

不强制物化为单个巨大 Table。

#### 11.3 `scan_persisted()`

底层存储接口，返回：

```text
pyarrow.dataset.Dataset
或 parquet file list
或 lazy scan handle
```

复杂历史分析交给 DuckDB / Polars / PyArrow。

#### 11.4 `snapshot()`

必须固定查询边界。

`snapshot` 创建后：

```text
上游继续写入不影响本次 snapshot；
active high watermark 固定；
sealed list 固定；
persisted file list 固定；
```

#### 11.5 `zippy.col()` 与 TablePlan

`zippy.col()` 是 Zippy 的表达式门面，不直接等同于 `polars.col()`：

```text
zippy.Expr AST -> Python Polars Expr -> LazyFrame 执行
```

第一版表达式范围：

```text
col(name)
literal(value)
比较：== != > >= < <=
布尔组合：& |
算术：+ - * /
alias(name)
is_in(values)
between(column, start, end)
```

用户层 API：

```python
q.select([...])
q.where(expr)
q.between(zippy.col("dt"), start_ns, end_ns)
q.tail(n)
q.collect()
q.to_pyarrow()
q.to_pandas()
q.to_polars()
```

v0 执行策略：

```text
Table 负责 snapshot、persisted/live 拼接、去重；
Python Polars 负责 projection/filter/expression/tail 执行；
Zippy 保留 Expr AST，后续可编译到 Rust/DataFusion/DuckDB/segment pushdown。
```

### 工作项

- `Table` 通过 master resolve named stream。
- 实现 `schema()`、`stream_info()`、`snapshot()`。
- 实现 `tail(n)`。
- 实现 `collect()`。
- 实现 `reader()`。
- 实现 `to_pyarrow()`、`to_pandas()`、`to_polars()`。
- 实现 `zippy.col()` 和最小 `Expr` AST。
- 实现 `Table.select()`、`Table.where()`、`Table.between()`。
- 第一版 TablePlan 在 Python 层编译到 Polars LazyFrame 执行。
- 实现 active + sealed 拼接。
- 实现 `scan_live()`。
- 实现 `scan_persisted()` 或 persisted file list。
- 小结果返回 `pyarrow.Table`。
- 大结果返回 reader/dataset，不强制一次性加载。

### 验收标准

- `q.tail(1000)` 能跨进程返回最新 1000 行。
- snapshot 创建后，上游继续写入不影响本次查询结果。
- active + sealed 拼接不重不漏。
- live retention 内查询不依赖 parquet。
- persisted parquet 可通过 PyArrow Dataset / DuckDB / Polars 查询。
- 大数据查询不会强制物化为单个巨大 `pyarrow.Table`。

---

## 12. Milestone 5：Persist 与 Retention

### 目标

明确 active、sealed、persisted parquet 三层存储职责，让实时查询和历史查询自然衔接。

### 存储分层

```text
active segment
  最新写入，最低延迟读取。

sealed segment
  近期不可变数据，支持 live tail、scan_live、replay。

persisted parquet
  历史数据，支持批量查询、研究、回放。
```

### 推荐 persist layout

```text
data/
  ctp_ticks/
    dt_part=202604/
      instrument_id=AU2606/
        ticks-segment-00000000000000000001-generation-00000000000000000000-part-00000.parquet
      instrument_id=IF2606/
        ticks-segment-00000000000000000001-generation-00000000000000000000-part-00001.parquet
```

第一版 partition spec：

```text
dt_column: datetime/timestamp 列名，可为 None
id_column: instrument/security identifier 列名，可为 None
dt_part: %Y | %Y%m | %Y%m%d | %Y%m%d%H
```

`dt_part` 不是数据列，而是从 `dt_column` 动态推导出的 strftime 子集格式。`%m`
表示月份，`%M` 表示分钟；第一版不开放分钟/秒级分区，避免小文件过多。`id_column`
分区值写入路径前必须做 percent-encoding，例如 `A/B` 写成 `A%2FB`，metadata
同时记录 raw partition value 和 encoded path value。

### Persist metadata

master 或 metadata 文件至少能追踪：

```text
stream_name
schema_hash
file_path
row_count
min_seq / max_seq
min_event_ts / max_event_ts
source segment id
created_at
```

### 工作项

- 定义 parquet 文件命名规则。
- 定义 partition strategy。
- 定义 persist commit 协议。
- persist 成功后再允许释放对应 sealed segment。
- persist 失败时进入 ERROR 或产生 error event。
- Table 能发现 persisted file list。

当前已落地：

- master 从 `~/.zippy/config.toml` 或 `-c/--config` 读取全局配置，并支持
  `ZIPPY_TABLE_ROW_CAPACITY`、`ZIPPY_TABLE_RETENTION_SEGMENTS`、
  `ZIPPY_TABLE_PERSIST`、`ZIPPY_TABLE_PERSIST_DATA_DIR` 等环境变量覆盖。
- persist partition 支持 `dt_column`、`id_column`、`dt_part`；master config 支持
  `[table.persist.partition]`，Python `Pipeline.stream_table()` 也支持同名参数。
- `Pipeline.stream_table()` 默认读取 master config；用户不传 `row_capacity`、
  `retention_segments`、`persist`、`data_dir` 时使用 master 下发的默认值。
- StreamTable rollover 后后台写 parquet；persist publisher 成功发布 metadata 后，
  retention 才会裁剪对应 sealed segment。
- persist worker 会在后台按 `dt_part + id_column` 拆分 sealed RecordBatch，每个
  partition 写一个 parquet 文件，并在 metadata 中记录 `partition`、`partition_path`
  和 `partition_spec`。
- StreamTable 已维护 sealed segment 级 persist commit 状态机：
  `pending -> writing -> committed | failed`；retention 只裁剪 `committed` 的
  segment，失败状态会保留错误原因并继续阻止裁剪。
- StreamTable persist worker 已支持有限次数自动重试；只有最终失败才进入 failure
  状态。persisted parquet metadata 已带稳定 `persist_file_id`，master 按该 id
  upsert，避免 retry 后重复登记同一个 parquet 文件。
- StreamTable 最终 persist 失败会发布 `persist_failed` 事件；master catalog 暴露
  `persist_events`，Python `Table.persist_events()` 可查询这些生命周期事件。
- 后台 persist 失败会被记录，并在后续 `flush`、`stop` 或新的 `on_data` 中返回错误，
  不再只停留在测试辅助状态里。
- `drop_table(table_name, drop_persisted=True)` 已能清理 master catalog、segment descriptor
  和已登记 persisted parquet 文件。
- StreamTable retention 已接入 reader lease 防护：本进程内仍被 pin 的 sealed mmap
  不会被裁剪；裁剪后会触发 segment store GC，释放已经退休的 mmap 文件。
- master catalog 已支持跨进程 `segment_reader_leases`；Pipeline 创建 StreamTable 时会注入
  retention guard，裁剪 sealed segment 前先查询 master lease，避免外部 reader 仍在 attach
  时删除底层 mmap 文件。
- `Table.tail()`、`Table.snapshot()`、`Table.scan_live()`、`zippy.subscribe()` 和
  segment source 已在 attach/read active segment 时自动 acquire/release reader lease；
  rollover 切换时先 acquire 新 active lease，再释放旧 active lease，用户不需要手动管理
  底层 lease。
- master lease reaper 已清理过期 process 持有的 `segment_reader_leases`，并同步写入
  snapshot，避免 master 重启后 stale lease 复活并永久阻止 retention。

剩余重点：

- persist 失败后的告警还需要补齐。
- partitioned parquet 小文件数量需要后续 compaction 策略。

### 验收标准

- sealed segment 可以 flush 到 parquet。
- persisted parquet 文件可以通过 PyArrow Dataset 读取。
- Table 可以返回 persisted files 或 dataset。
- persist 写入失败不会静默丢数据。
- retention 清理前能确认 persist 状态。
- stream、segment、persisted file 之间可追踪。

---

## 13. Milestone 6：Pipeline 生命周期 API

### 目标

把 source、StreamTable、engine、persist 的生命周期统一托管，降低 Python 用户心智负担。

### 推荐 API

#### 行情接入

```python
(
    zippy.Pipeline("ctp_ingest", master="unix:///tmp/zippy-master.sock")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table(
        "ctp_ticks",
        schema=TickSchema,
        retention_rows=1_000_000,
        persist="parquet",
        data_dir="data",
    )
    .run_forever()
)
```

#### 下游计算

```python
(
    zippy.Pipeline("downstream_compute", master="unix:///tmp/zippy-master.sock")
    .engine(source="ctp_ticks", engine=some_engine)
    .stream_table(
        "some_output_stream.v1",
        schema=SomeOutputSchema,
        retention_rows=2_000_000,
    )
    .run_forever()
)
```

#### 查询

```python
q = zippy.read_table("ctp_ticks", master="unix:///tmp/zippy-master.sock")
latest = q.tail(1000)
```

### Pipeline 职责

```text
启动 source；
启动 engine；
注册 stream；
管理 StreamTable；
管理 persist；
异常传播；
Ctrl-C graceful shutdown；
flush；
stop；
资源清理。
```

### 工作项

- 引入 `Pipeline` 作为推荐入口。
- 支持 `.source(...).stream_table(...).run_forever()`。
- 支持 `.engine(source="...").stream_table(...)`。
- Pipeline 内部处理 named stream resolution。
- Pipeline 内部处理 resource cleanup。
- 保留底层组件 API，但 examples 推荐 Pipeline。

### 验收标准

- OpenCTP/mock source -> StreamTable 最小代码不需要手动 publisher。
- downstream engine 跨进程订阅 named stream 不需要手动 source descriptor。
- Ctrl-C 后 source、segment、persist 资源能关闭。
- 异常退出时 master 状态可观察。
- Pipeline 错误信息包含 stream name、component name、master endpoint。

---

## 14. Milestone 6.5：Low-latency IPC v2

### 目标

把当前已经稳定的 master long-poll 切段发现机制，升级为面向长期实盘低延迟的
segment-native IPC 协议。

master 继续负责 named stream catalog、schema、lease、snapshot 和 coarse-grained
metadata；数据热路径和切段热路径尽量不依赖 Unix socket 控制面请求。

### 设计原则

```text
不重新引入旧 bus / POSIX shm fallback 作为主路径；
不暴露底层 IPC 细节给 Python 用户；
master 是 catalog，不是每条热路径事件的调度器；
active segment 自带可验证的 control block；
reader 能通过 generation / watermark / signal 感知新数据和 rollover；
所有资源清理必须和 lease、retention、persist 状态绑定。
```

### 推荐协议形态

```text
StreamTable writer
  -> mmap active segment data pages
  -> mmap segment control block
  -> committed watermark / descriptor_generation / rollover state
  -> wake readers

Subscriber / downstream engine
  -> master resolve stream once
  -> attach active segment + control block
  -> wait generation / committed watermark change
  -> read SegmentRowView / SegmentTableView
  -> rollover 时 attach 新 active segment
```

### 工作项

#### 14.1 correctness baseline

当前 master long-poll 保留为 correctness baseline 和对照测试对象。它不作为最终热路径目标，
但用于证明 IPC v2 行为没有破坏切段顺序、descriptor generation 和 stale descriptor 检测。

#### 14.2 segment control block

定义固定布局的 mmap control block，至少包含：

```text
magic / version
stream_id 或 schema_id
segment_id
writer_epoch
descriptor_generation
committed_rows
row_capacity
sealed flag
error / closed state
last_update_ns
checksum 或 layout guard
```

读端 attach 时必须校验 magic、version、schema/layout、segment_id 和 writer_epoch。

#### 14.3 wakeup primitive

定义 Linux 优先的低延迟唤醒机制，例如 futex 或 eventfd。目标不是永久 spin，而是让
reader 在无数据时可阻塞，在写入和 rollover 时被快速唤醒。

需要保留明确的 timeout / health check 路径，用于检测 writer crash、lease expired、
control block 损坏和 resource cleanup。

#### 14.4 rollover protocol

明确 active -> sealed -> new active 的顺序：

```text
writer seal old active；
writer 创建 new active + new control block；
writer 更新 descriptor_generation；
writer 发布新 descriptor metadata；
writer wake readers；
reader 验证 generation 后切换 active segment。
```

协议必须保证 reader 不会静默读到已释放 segment，也不会在 rollover 边界丢第一批数据。

#### 14.5 subscriber / engine 接入

`zippy.subscribe()`、`zippy.subscribe_table()` 和后续 downstream engine 应共享同一套
segment-native reader。行回调、表回调、engine 输入都只是消费视图方式不同，不应维护
三套切段发现逻辑。

#### 14.6 benchmark 与回归

建立固定 benchmark：

```text
rollover first-row latency p50/p95/p99/max
subscribe row callback latency p50/p95/p99/max
subscribe_table batch callback latency p50/p95/p99/max
downstream engine source attach latency
multi-reader fanout latency
writer crash / stale descriptor detection latency
CPU usage under idle / low-rate / high-rate ingest
```

### 验收标准

- rollover 后第一批数据不再出现固定 50ms 量化长尾。
- subscriber 稳态读取不需要周期性控制面 descriptor polling。
- reader attach stale descriptor 能得到明确错误或自动切换。
- writer crash 后 reader 不会静默读取脏数据。
- mmap control block 资源能随 stream retention / persist / lease 明确清理。
- M7 下游 engine 可以复用同一套 segment-native reader。

---

## 15. Milestone 7：Segment-native Downstream Engine Composition

### 目标

让后续 time-series engine、cross-sectional engine、reactive engine、factor engine 都能自然基于 named stream 组合。

本文不定稿 factor 系统，只定义基础设施接入点。

### 推荐组合方式

```text
input named stream
  -> downstream engine
  -> output rows
  -> StreamTable materialization
  -> output named stream
  -> Table / strategy / persist
```

### 工作项

#### 15.1 engine 输入

engine 应能直接接收：

```text
source="ctp_ticks"
```

内部通过 master resolve stream 并 attach segment。

#### 15.2 engine 输出

engine 输出不应只是 callback，也应该能进入 StreamTable：

```python
.engine(source="ctp_ticks", engine=some_engine)
.stream_table("engine.output.v1", schema=OutputSchema)
```

#### 15.3 segment-native processing

engine 内部尽量处理 `SegmentTableView`，只在必要时物化 Arrow。

当前已落地：

- `ReactiveLatestEngine` 已支持按 `by` 的一个或多个 UTF8 维度维护 latest row state；
  输入 schema 与输出 schema 一致，用于 `instrument_id -> latest tick` 等展示型视图。
- `ReactiveLatestEngine` 支持 Python API、source linkage 和普通 engine 生命周期
  `start/write/flush/stop`；`flush` 会输出当前全量 latest snapshot。
- `ReactiveLatestEngine` 支持 `source="ctp_ticks"` 形式从 master 查询 stream metadata，
  自动推导 `input_schema` 并 attach segment source；显式传 `input_schema` 时会校验 master
  中的 schema 一致。
- `Session.engine(...).run()` 已作为轻量 Python 编排层起步：Session 注入共享 master、
  默认 `NullPublisher()`，并统一管理多个 engine 的 `start/stop` 生命周期。后续需要在
  Session 上继续补 `to_table`、callback 输出和更完整的命名 output stream 管理。
- 当前默认语义调整为：当 `.engine(..., source="named_stream")` 且用户没有显式传
  `target` 时，Session 会把 engine 输出自动物化为同名 StreamTable，也就是
  `name="ctp_ticks_latest"` 会注册并发布 `ctp_ticks_latest`，使
  `zippy.read_table("ctp_ticks_latest")` 可以直接查询。
- 推荐显式写法已经推进为 `.engine(...).stream_table("ctp_ticks_latest")`：
  `name` 表示 engine 实例名，`stream_table(...)` 表示将最近一个 engine 输出物化为
  输出 StreamTable。未传 `stream_table(...)` 或 `output` 时，仍在 `run()` 前按
  `name` 同名表默认物化，以兼容当前 REPL 用法。
- `output_stream="ctp_ticks_latest"` 保留为 `.engine(...)` 的快捷兼容参数，但文档主推
  `.stream_table(...)`；旧的 `output=` 不再支持，避免与 factor 输出列命名混淆。
  顶层表创建接口后续命名为 `zippy.create_stream_table(...)`，
  避免与 Session 链式 `.stream_table(...)` 混淆。
- Session 自动物化同名表时会为当前进程注册一个 `session_engine_output` source，
  让当前进程拥有该表 segment descriptor 发布权限；source 名包含 process_id，
  避免 REPL/重启后与旧 source 记录冲突。
- 自动物化表默认 `persist=False`，只保留 live table；用户显式传
  `.engine(..., persist=True)` 时才落盘。当前唯一持久化协议是 parquet，因此
  `persist=True` 等价于为同名表启用 parquet persist。

#### 15.4 预留 factor system 接口

后续 factor 系统至少需要接入：

```text
input named stream;
output named stream;
state lifecycle;
persist/replay;
Table 查询输出结果；
strategy subscribe 输出结果。
```

但以下内容暂不在本文定稿：

```text
FactorSpec 格式；
factor versioning 规则；
factor graph；
factor dependency；
late data policy；
state snapshot policy；
wide table vs narrow table；
per-factor stream vs factor pack stream。
```

### 验收标准

- downstream engine 可以跨进程读取 `ctp_ticks`。
- downstream engine 输出可以 materialize 为新的 named stream。
- 新输出 stream 可以被 `Table` 查询。
- 新输出 stream 可以 persist。
- engine 进程重启后能重新 resolve upstream stream。

---

## 16. Milestone 8：Replay / Backfill / Live-Replay Parity

### 目标

让历史归档数据能够重新驱动下游计算，形成实盘与回放一致性的基础。

这一步是实时因子基础设施和普通行情转发器的分水岭。

### 推荐 API 草案

```python
zippy.replay(
    source_persisted="data/ctp_ticks",
    output_stream="replay.ctp_ticks.20260426",
    start="2026-04-26 09:00:00",
    end="2026-04-26 15:00:00",
)
```

或：

```python
(
    zippy.Pipeline("replay_ctp_ticks")
    .source(zippy.ParquetReplaySource("data/ctp_ticks", start=..., end=...))
    .stream_table("replay.ctp_ticks", schema=TickSchema)
    .run()
)
```

### 工作项

- 实现 ParquetReplaySource。
  - 已完成最小 Python source plugin：读取 parquet file/directory，按 batch
    as-fast-as-possible 发给 `Pipeline.source(...).stream_table(...)`。
  - 已完成 replay source 生命周期修正：自然回放结束后只 flush 并保持 source idle，
    输出 StreamTable 生命周期由 `Pipeline.stop()` 控制，避免 replay 表刚生成即释放
    active mmap。
- 支持按 event_ts / seq 回放。
- 支持 controlled speed：
  - as-fast-as-possible
  - original timing
  - fixed rate
- replay 输出仍然是 named stream。
- replay 输出可以接 downstream engine。
- 定义 live vs replay 比对工具。
  - 已完成 `zippy.compare_replay(left, right, by=[...])` 基础版，支持 table name、
    `zippy.Table`、`pyarrow.Table`、RecordBatchReader 和 PyArrow Dataset 输入。
  - 已完成 replay e2e 测试：persisted parquet -> ParquetReplaySource ->
    replay named stream -> Table.collect -> compare_replay。

### 验收标准

- persisted 数据能重新变成 named stream。
- replay stream 能被 Table 查询。
- replay stream 能驱动 downstream engine。
- 对同一段数据，live persisted 与 replay 结果可以按 key/seq/time 对齐比较。
- replay 不依赖实盘 source。

---

## 17. Milestone 9：测试、性能与可运维性

### 目标

把实盘链路变成可重复验证的工程资产，而不是一次性 demo。

### 必须覆盖的 e2e 测试

#### 17.1 Ingest e2e

```text
MasterServer
  -> mock/OpenCTP source
  -> StreamTable("ctp_ticks")
  -> read_table("ctp_ticks").tail(1000)
```

验收：跨进程可读，数据不重不漏。

#### 17.2 Consumer starts before producer

```text
read_table("ctp_ticks") 先启动；
producer 后启动并注册 ctp_ticks；
Table 自动继续。
```

#### 17.3 Rollover e2e

```text
active segment 写满；
rollover sealed segment；
新 active segment 继续写；
tail(n) 跨 active + sealed 返回正确数据。
```

#### 17.4 Persist e2e

```text
sealed segment flush parquet；
retention 清理 sealed segment；
Table.scan_persisted() 可查询历史数据。
```

#### 17.5 Writer crash

```text
writer 异常退出；
master 标记 stream STALE；
reader 查询或订阅时得到明确状态。
```

#### 17.6 Persist failure

```text
parquet 写入失败；
系统不得静默丢失 sealed segment；
master 或 error event 可观察。
```

#### 17.7 Replay e2e

```text
persisted parquet
  -> ParquetReplaySource
  -> replay named stream
  -> Table tail
```

### 性能指标

至少记录：

```text
ingest throughput rows/sec
append latency p50/p95/p99
query tail(1000) latency p50/p95/p99
active scan throughput
persist flush throughput
replay throughput
memory usage
/dev/shm usage
rollover latency
```

### 可观测指标

建议每个 stream 暴露：

```text
stream status
writer heartbeat age
active segment id
active committed rows
sealed segment count
persist pending segments
persist failed segments
reader count
last append timestamp
last persist timestamp
```

### 错误信息原则

所有错误必须包含足够定位信息：

```text
stream_name
component_name
master_endpoint
schema_hash expected/actual
segment_id
descriptor_generation
persist_path
```

---

## 18. 版本目标建议

### v0.1：实时表闭环

目标：打通 ingest -> StreamTable -> Table.tail。

当前落地状态：

```text
已完成 M0/M2 基础：master catalog 暴露 schema、schema_hash、data_path、descriptor_generation；
已完成 M3 基础：StreamTableMaterializer 写入 active segment，并保留 StreamTableEngine 兼容别名；
已完成 M4 基础：read_table(source, master).tail(n) 可读取 active descriptor 并返回 pyarrow.Table；
已完成 M4 增量：StreamTable rollover descriptor 附带 retained sealed segment metadata，Table.tail(n) 可拼接 retained sealed + active；
已完成 M4 增量：Table.snapshot() 暴露 stream/schema、active descriptor、active high watermark、retained sealed segments、persisted_files 占位；
已完成 UX 基础：zippy.connect(uri=...) 管理默认 master 连接和已注册进程 heartbeat，Table/subscribe 可省略 master 参数；
已完成订阅基础：zippy.subscribe(source, callback) 默认回调 zippy.Row；
已完成订阅基础：zippy.subscribe_table(source, callback) 回调增量 pyarrow.Table；
已完成订阅稳定性基础：subscriber descriptor watcher 使用 master long-poll，
  已消除固定 50ms descriptor polling 长尾；
已完成 Pipeline 基础：Pipeline.stream_table(...) 自动注册 stream/source、发布 active descriptor、托管 start/write/stop；
已完成 Pipeline 基础：Pipeline.source(...) 支持 Python source schema 推断、source name/type 元数据、stop 托管；
已完成 TableSnapshot 增量：master StreamInfo 暴露 active_segment_descriptor、
  sealed_segments、persisted_files，Table.snapshot()/tail() 基于该边界读取；
已完成 Table live scan 基础：Table.scan_live() 返回 pyarrow.RecordBatchReader，
  按 retained sealed segments -> active high watermark 顺序读取 live batches，
  不把结果强制 concat 成单个 pyarrow.Table；
已完成 Table tail 统一查询基础：Table.tail(n) 作为用户层入口，
  live retention 不足时自动从 persisted parquet 补齐，并按
  source_segment_id/source_generation 排除 retained sealed 与 persisted 的重叠段；
已完成 Table collect/reader 基础：Table.collect() 和 Table.reader() 作为用户层入口，
  自动拼接去重后的 persisted parquet 与 live segment 视图；
已完成 Table 转换基础：Table.to_pyarrow() 作为 collect() 别名，
  Table.to_pandas()/to_polars() 基于 collect() 结果转换；
已完成 Table 表达式层 v0：zippy.col() 返回 Zippy Expr AST，
  Table.select()/where()/between() 记录 TablePlan，用户 API 不直接绑定 Polars；
  v0 在 Python 层把 TablePlan 编译到 Polars LazyFrame 执行，后续可下推到
  Rust/DataFusion/DuckDB/segment reader；
已完成 Persist metadata 基础：master 可登记 persisted file metadata，
  Table.persisted_files()/scan_persisted() 可把 parquet 文件交给 PyArrow Dataset；
已完成 StreamTable persist worker 基础：StreamTable rollover 后只把 sealed segment
  persist task 入队，后台线程写 parquet 并通过 Pipeline 的 persist/data_dir 配置把
  persisted metadata 发布到 master，写入热路径不等待 parquet flush 或 metadata publish；
已完成全局配置基础：master 默认读取 ~/.zippy/config.toml，也支持 -c/--config；
  ZIPPY_TABLE_* 环境变量可覆盖配置，MasterClient.get_config()/zippy.config() 可读取
  master 下发配置，Pipeline.stream_table() 默认使用 table row_capacity/persist 配置；
待完成长期低延迟 IPC：M6.5 将切段发现、watermark 和 reader wakeup 下沉到
  segment-native control block；
待完成 Persist/Retention：persist 告警、partition compaction；
已完成 Persist/Retention 安全闭环：persist commit gating、reader lease、mmap GC、stale lease cleanup；
待完成用户闭环：OpenCTP/native source 的真实长跑生命周期和错误传播验收。
```

包含：

```text
M0 数据契约与命名规范
M1 Segment-only 主路径
M2 Named Stream Catalog 基础版
M3 StreamTable active segment materialization
M4 Table.snapshot + tail retained sealed + active 基础版
```

最小 demo：

```python
(
    zippy.Pipeline("mock_ingest")
    .source(zippy.MockTickSource(...))
    .stream_table("ctp_ticks", schema=TickSchema, retention_rows=1_000_000)
    .run_forever()
)
```

```python
q = zippy.read_table("ctp_ticks")
print(q.tail(1000))
```

### v0.2：active + sealed + persist

目标：实时查询和历史查询打通。

包含：

```text
rollover
sealed segment metadata
retention
parquet persist
scan_live 基础版
scan_persisted
TableSnapshot 完整边界
```

最小 demo：

```python
q = zippy.read_table("ctp_ticks")
live = q.scan_live()
persisted = q.scan_persisted()
```

### v0.2.5：Low-latency IPC v2

目标：在下游 engine 大规模 segment-native 化之前，定稿长期低延迟 IPC 协议。

包含：

```text
mmap segment control block
committed watermark
descriptor_generation / writer_epoch
rollover signal
reader wakeup
stale descriptor detection
resource cleanup
latency benchmark baseline
```

最小 demo：

```python
subscriber = zippy.subscribe("ctp_ticks", callback=on_tick, xfast=True)
```

该 demo 的用户 API 不应变化；变化应发生在内部 reader attach、wakeup 和 rollover 协议。

### v0.3：Pipeline 与下游计算

目标：下游 engine 可基于 named stream 组合，并输出新的 named stream。

包含：

```text
Pipeline lifecycle
engine(source="...")
engine output -> StreamTable
output Table
output persist
```

最小 demo：

```python
(
    zippy.Pipeline("compute_demo")
    .engine(source="ctp_ticks", engine=some_engine)
    .stream_table("engine.output.v1", schema=OutputSchema)
    .run_forever()
)
```

### v0.4：Replay 与一致性验证

目标：persisted 数据可以 replay，并验证 live/replay parity。

包含：

```text
ParquetReplaySource
replay named stream
replay -> downstream engine
live/replay comparison helper
```

---

## 19. 暂缓事项

以下内容重要，但不建议在 v0.1/v0.2 之前展开：

```text
完整 factor DSL / FactorSpec；
复杂 factor graph；
跨 stream 一致性 join；
完整 SQL 查询语言；
分布式多节点；
复杂权限系统；
自动 query optimizer；
复杂二级索引；
多 writer 同时写同一个 stream；
```

这些事项可以作为后续专题设计，不应阻塞 named stream、StreamTable、Table 的闭环。

---

## 20. Open Questions

这些问题需要在实现过程中逐步定稿：

1. `tail(n)` 是否只支持全 stream append order，还是 v0 就支持 key 内 tail？
2. `window(last="5m")` 是否进入 Table v0，还是先交给 Polars/DuckDB？
3. M6.5 的 mmap control block 是否使用 futex、eventfd，还是二者组合？
4. persist metadata 存在 master 内存、sidecar metadata 文件，还是轻量 catalog 文件？
5. retention 清理策略如何和 persist commit 协议绑定？
6. query snapshot 是否需要支持多个 stream 的一致边界？
7. Python API 返回 `pyarrow.Table`、`RecordBatchReader`、还是自定义 `zippy.Table`？
8. 是否需要为 instrument_id 建立轻量索引？如果需要，在哪个版本做？
9. factor 输出到底是宽表、窄表、按 domain 分组，还是每个 factor 独立 stream？此问题暂缓到 factor system 专题。

---

## 21. 最终验收场景

当以下场景稳定跑通时，Zippy 才算完成完整第一阶段目标。v0.1 可以先收敛到
`source -> StreamTable active segment -> Table.tail(1000)` 的 active-only 闭环。

### 场景一：实时行情写入与查询

```python
(
    zippy.Pipeline("ctp_ingest")
    .source(openctp.OpenCtpMarketDataSource(...))
    .stream_table("ctp_ticks", schema=TickSchema, retention_rows=1_000_000)
    .run_forever()
)
```

另一个进程：

```python
q = zippy.read_table("ctp_ticks")
latest = q.tail(1000)
```

要求：跨进程查询稳定，延迟可度量，schema 自动发现。

### 场景二：consumer 先启动

```python
q = zippy.read_table("ctp_ticks", wait=True, timeout="30s")
```

然后启动 producer。

要求：consumer 不需要用户手动重启。

### 场景三：active + sealed 查询

写入超过一个 segment 后：

```python
latest = q.tail(100_000)
```

要求：能跨 active + sealed 返回正确结果。

### 场景四：persisted 查询

当 live retention 不足时：

```python
persisted = q.scan_persisted()
```

要求：历史数据能被 PyArrow / Polars / DuckDB 消费。

### 场景五：下游输出成为新的 named stream

```python
(
    zippy.Pipeline("downstream_compute")
    .engine(source="ctp_ticks", engine=some_engine)
    .stream_table("engine.output.v1", schema=OutputSchema)
    .run_forever()
)
```

然后：

```python
q = zippy.read_table("engine.output.v1")
q.tail(1000)
```

要求：下游结果与原始行情一样可订阅、可查询、可归档。

### 场景六：replay

```python
(
    zippy.Pipeline("replay")
    .source(zippy.ParquetReplaySource("data/ctp_ticks"))
    .stream_table("replay.ctp_ticks", schema=TickSchema)
    .run()
)
```

要求：persisted 数据可以重新驱动系统。

---

## 22. 实施建议

推荐实现顺序：

```text
1. 先修 master catalog，让 schema 和 StreamInfo first-class；
2. 再让 StreamTable 真正拥有 active segment；
3. 再实现 TableSnapshot；
4. 再做 Table.tail；
5. 再做 sealed rollover；
6. 再做 persist；
7. 再做 Pipeline API；
8. 最后接 downstream engine 和 replay。
```

不要优先做：

```text
更多复杂因子算子；
完整 SQL；
多节点集群；
复杂 DSL；
过早极限零拷贝优化；
```

优先完成一个小但闭环的系统：

```text
source -> stream_table("ctp_ticks") -> read_table("ctp_ticks").tail(1000)
```

这个闭环完成后，再逐步把 downstream engine、persist、replay、factor system 接进来。
