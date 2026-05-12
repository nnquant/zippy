# Zippy 性能审计问题记录

日期：2026-05-12

范围：只读审计当前 `zippy` 工作区的性能风险。目标场景是量化交易系统中大量标的、
大量因子的批量与流式计算，同时关注延迟和吞吐。本轮不改实现代码。

结论：我不能对当前项目性能达到 100% 信心。当前已经有一些好的性能基础，例如
segment descriptor forwarding、`zippy-perf` 持续压测工具、Gateway bounded blocking
执行和若干 pushdown；但性能信心仍被三类问题限制：

- 热路径仍有多处按行拷贝、整表重建、整批 materialize 和串行扫描。
- CI 只编译 bench，不运行持续压测或延迟阈值门禁。
- 现有 benchmark 场景太窄，无法代表“很多标的、很多因子、远端 Gateway、多文件 parquet、
  长时间运行、尾延迟和内存峰值”的生产压力。

## 本轮验证

- `cargo test -p zippy-engines -p zippy-segment-store -p zippy-gateway`：通过。
- `cargo bench -p zippy-engines --no-run`：通过，只证明 bench 可编译。
- `cargo bench -p zippy-engines`：
  - `crosssectional_pipeline_1024_rows`：约 `1.0513 ms..1.0908 ms`。
  - `reactive_pipeline_1024_rows`：约 `55.184 us..56.670 us`。
  - `timeseries_pipeline_1024_rows`：约 `158.28 us..170.69 us`。
- `cargo test -p zippy-segment-store --test openctp_segment_perf_smoke -- --nocapture`：
  - segment smoke `p50=9.519us p95=42.773us p99=151.306us`。
  - active reader smoke `p50=8.436us p95=12.294us p99=40.288us`。
  - single-row batch smoke `p50=2.194us p95=3.687us p99=12.584us`。
- `cargo run -p zippy-perf --release -- inproc-timeseries --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024`：
  - `pass=true`，平均约 `1,000,789 rows/s`，`p50=24us p95=36us p99=44us`。
- `cargo run -p zippy-perf --release -- stream-table-segment-copy --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024`：
  - `pass=true`，平均约 `1,000,789 rows/s`，`p50=235us p95=1181us p99=10729us`。
- `cargo run -p zippy-perf --release -- stream-table-segment-forward --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024`：
  - `pass=true`，平均约 `1,000,789 rows/s`，`p50=36us p95=58us p99=72us`。

说明：这些验证能证明当前局部路径可运行，并不能证明生产性能达标。尤其是
`stream-table-segment-copy` 的 p99 已到 `10.729 ms`，但仍然 `pass=true`，说明当前 pass
判定没有覆盖尾延迟。

## 优先级说明

- P0：会直接阻止建立生产性能信心，或可能在目标场景下造成无法解释的尾延迟/吞吐塌陷。
- P1：重要热路径复杂度或资源风险，应在性能验收前修复或量化。
- P2：性能工程体系缺口，会让回归长期隐藏。

## P0 问题

### P001 性能门禁没有运行真实压测和尾延迟阈值

证据：

- CI 只执行 `cargo bench -p zippy-engines --no-run`，没有运行 Criterion benchmark 或
  `zippy-perf` profile：`.github/workflows/ci.yml:51`。
- `zippy-perf` 的 pass 判定只检查平均吞吐达到目标 90%、无 drop/error 等条件，
  没有 p95/p99 延迟、最大延迟、内存峰值、队列深度峰值门禁：
  `crates/zippy-perf/src/lib.rs:1062`。
- 本轮短测中 `stream-table-segment-copy` p99 为 `10729us` 仍然 `pass=true`。

影响：

当前无法阻止“平均吞吐达标但尾延迟不可接受”的回归。量化交易场景里，p99/p999、短时队列
堆积、GC/分配尖峰、rollover/persist 尖峰通常比均值更关键。

修复建议：

- 在 CI 或 nightly 中运行 `zippy-perf` 的最小矩阵：inproc timeseries、stream table copy、
  stream table forward、remote upstream/downstream、Gateway collect/subscribe。
- pass 判定增加 p95/p99/p999、最大队列深度、drop、publish error、RSS 峰值、持续运行时长。
- 将“快速 PR 门禁”和“长跑 nightly”拆开，PR 保护结构性退化，nightly 保护容量退化。

### P002 Gateway collect 仍是一次性整批执行，不适合大查询和慢客户端

证据：

- `handle_collect()` 最终把扫描结果应用 residual plan 后一次性 `encode_ipc_table()` 返回：
  `crates/zippy-gateway/src/lib.rs:815`。
- persisted parquet 路径逐文件读成 `RecordBatch`，再收集为 `Vec<RecordBatch>` 并 concat：
  `crates/zippy-gateway/src/lib.rs:2010`、`crates/zippy-gateway/src/lib.rs:2150`。
- ROADMAP 已记录 streaming collect 是后续项，说明当前仍是已知缺口：`ROADMAP.md:8`。

影响：

大结果集会造成首包延迟高、峰值内存高、慢客户端占用 blocking worker。多个大查询并发时，
Gateway 的 bounded blocking pool 会保护系统不崩，但会以拒绝或排队表现出来，吞吐稳定性不可
确认。

修复建议：

- 将 collect 改为 streaming Arrow IPC：扫描、filter、project、encode、write 分块流水化。
- 客户端断开时取消后续扫描和编码。
- 给 collect metrics 拆分 scan/decode/filter/residual/encode/write 耗时和峰值 batch 大小。

### P003 StreamTable Arrow batch 写入路径仍按行写入，segment copy 尾延迟已暴露风险

证据：

- 非 segment input 路径在 `materialize_table_rows()` 中对每行调用 `write_materialized_row()`：
  `crates/zippy-engines/src/stream_table.rs:783`。
- `write_materialized_row()` 内部走 `PartitionWriterHandle::write_row()`，意味着每行单独事务、
  锁和通知：`crates/zippy-engines/src/stream_table.rs:802`。
- segment row view 路径有批量 append/forwarding 优化：`crates/zippy-engines/src/stream_table.rs:829`。
- 本轮 `stream-table-segment-copy` 在 1M rows/s 短测中 p99 为 `10729us`，而
  `stream-table-segment-forward` p99 为 `72us`，两条路径差异很大。

影响：

只要上游以普通 Arrow batch 进入 stream table，而不是 segment row view/descriptor forwarding，
就会落入按行写入路径。大量标的、大 batch、rollover 和持久化同时出现时，尾延迟会显著高于
forwarding 路径。

修复建议：

- 在 Arrow batch materialization 中使用 `PartitionWriterHandle::write_columnar_rows()` 或等价
  columnar append，一次锁内写入多行并只发布一次 committed prefix。
- 对 Utf8 列使用批量 offsets/values 写入，避免每行写字符串。
- 把 `stream-table-segment-copy` p99 纳入压测阈值；copy 路径不能只靠平均吞吐 pass。

## P1 问题

### P004 TimeSeriesEngine 每批克隆全量状态并按行分配 key

证据：

- 每个 `on_data()` 都克隆 `open_windows` 和 `last_dt_by_id`：
  `crates/zippy-engines/src/timeseries.rs:342`。
- 每行 `id_value(...).to_string()`，再对 `BTreeMap` remove/insert：
  `crates/zippy-engines/src/timeseries.rs:346`、`crates/zippy-engines/src/timeseries.rs:357`。
- 无 id filter 时也构造完整 `row_indices` 向量：
  `crates/zippy-engines/src/timeseries.rs:416`。

影响：

复杂度接近 `O(batch_rows * log(active_ids) + active_ids clone)`，并伴随大量字符串和 Vec 分配。
当标的数很大、窗口很多、因子很多时，这会拉高 CPU 和 allocator 压力。

修复建议：

- 默认原地更新状态；只在需要 rollback 语义时记录 touched-key undo log。
- 用 `HashMap`/`IndexMap` 或 interned id 替代 `BTreeMap<String, ...>` 的热路径查找。
- 对输入按 id/window 已排序的常见行情流增加 fast path，避免每行 remove/insert。
- 预解析 spec 输入数组，避免每个 factor 每行重复 downcast。

### P005 CrossSectionalEngine 按行抽取 OwnedRow，且每批排序全量 row indices

证据：

- 每批克隆 `current_rows`，构造 `row_indices` 并按 bucket 排序：
  `crates/zippy-engines/src/cross_sectional.rs:175`。
- 每行通过 `extract_owned_row()` 构造单行 `RecordBatch`：
  `crates/zippy-engines/src/cross_sectional.rs:188`。
- finalize 时再 concat 所有单行 batch，因子再各自扫描 batch：
  `crates/zippy-engines/src/cross_sectional.rs:92`。
- Criterion 仅 1024 行、32 个 symbol、3 个因子就约 `1.07ms`。

影响：

截面计算本来就是大量标的同一时刻一起算因子的核心路径。按行 batch 化和 concat 会让 CPU 与内存
分配随标的数放大；多个截面因子叠加时，排序、拷贝和多次扫描会继续叠加。

修复建议：

- 用 columnar bucket buffer 保存当前截面，而不是 `BTreeMap<String, OwnedRow>`。
- 对同 bucket 已排序输入增加 append/update fast path。
- 多个截面因子共享一次 `float64_values`/有效值索引，避免每个因子重复提取。
- 对 rank 之外的 zscore/demean 使用单次统计 pass。

### P006 ReactiveLatestEngine 和 KeyValueTableMaterializer 更新一行也可能重建大状态

证据：

- `ReactiveLatestEngine` 每批克隆 `latest_rows`，每行抽取单行 batch，最后 concat 更新行：
  `crates/zippy-engines/src/reactive_latest.rs:95`。
- `KeyValueTableMaterializer` 每行抽取单行 batch，`on_data()` 后构造全量 snapshot 并 replace：
  `crates/zippy-engines/src/stream_table.rs:1307`、`crates/zippy-engines/src/stream_table.rs:1372`。

影响：

latest/key-value 表在行情系统中通常是高频更新面板。当前实现对“少量 key 更新、大量 key 已存在”
的场景不友好，容易变成 `O(total_keys)` 重写和大批量 concat。

修复建议：

- 把 latest state 改成 columnar arena + key 到 row slot 的索引。
- 输出只包含更新 key 的 delta；需要 snapshot 时再显式 materialize。
- KeyValueTable replace 应支持 slot-level update 或分段增量 snapshot，而不是每个 batch 全量重写。

### P007 ReactiveStateEngine 多因子链路重复构造中间 RecordBatch，滚动窗口因子是 O(window)

证据：

- 每个 factor 都用 `columns.clone()` 构造一次中间 `RecordBatch`，并重建 schema：
  `crates/zippy-engines/src/reactive.rs:282`。
- EMA/rolling state 每行都把 id 转成 `String` 存入 `HashMap`：
  `crates/zippy-operators/src/reactive.rs:190`。
- rolling mean/std 每行对整个窗口求和或方差：
  `crates/zippy-operators/src/reactive.rs:205`。

影响：

大量因子链路会产生 `factor_count` 倍的 batch/schema 构造和 column Vec 克隆。rolling window 越大，
mean/std 的每行成本越高，无法线性支撑大量标的和大量因子。

修复建议：

- 引入 factor evaluation context，按列追加结果，避免每个 factor 重建完整 `RecordBatch`。
- 对 rolling mean/std 维护 running sum/sumsq 和 ring buffer，实现 O(1) 更新。
- 对 id 使用 dictionary/interning 或 row-level borrowed key，减少字符串分配。

### P008 Arrow bridge 和 live query 会把 segment 行范围物化成新数组

证据：

- `RowSpanView::as_record_batch()` 对 schema 字段逐列 project 并创建新 Arrow array：
  `crates/zippy-segment-store/src/arrow_bridge.rs:15`。
- sealed/active 投影中大量 `to_vec()`、`collect::<Vec<_>>()`、Utf8 `to_owned()`：
  `crates/zippy-segment-store/src/arrow_bridge.rs:60`、`crates/zippy-segment-store/src/arrow_bridge.rs:159`。
- segment perf smoke 自身也是单行 append 后立刻 `as_record_batch()`，只能覆盖最小场景：
  `crates/zippy-segment-store/src/perf.rs:24`。

影响：

segment store 的写入/共享内存方向已经较快，但一旦查询或跨进程读转成 Arrow batch，仍会发生整列拷贝。
对于大 snapshot、大 tail、大订阅 batch，这会成为读侧 CPU 和内存瓶颈。

修复建议：

- 为 active/sealed segment 增加 Arrow buffer zero-copy 或 chunked borrowed array 路径。
- 查询侧支持 iterator/reader 逐块产出，而不是先 `as_record_batch()` 再 concat。
- 对 `RowSpanView::column()` 添加基于 scan filter 的最小列读取，避免先读无关列。

### P009 persisted parquet scan 串行，filter pushdown 主要发生在读后

证据：

- Gateway 对 persisted 文件逐个循环读取：`crates/zippy-gateway/src/lib.rs:2016`。
- `read_parquet_record_batch_with_tail()` 读取 reader 后收集所有 batch 再 concat：
  `crates/zippy-gateway/src/lib.rs:2150`。
- filters 在 `apply_scan_pushdown_to_record_batch()` 中对已读出的 batch 再执行：
  `crates/zippy-gateway/src/lib.rs:2164`。
- ROADMAP 已列出 parallel persisted scan 是后续项：`ROADMAP.md:19`。

影响：

多文件、多分区、多日历史数据的查询会受单线程文件扫描和内存拼接限制。即使有 projection 和 tail
row selection，普通 filter 仍可能先读大量不匹配数据。

修复建议：

- 用 bounded scan pool 并行读取多个 parquet 文件，同时保持 deterministic output order。
- 将可表达 filter 下推到 Parquet/Arrow reader，使用 row group statistics 做跳过。
- 对 scan、decode、filter、concat 分别计时，避免 `elapsed_ms` 过粗。

### P010 Gateway write path 用全局 writers mutex 包住物化和 flush

证据：

- `write_batch()` 获取 `self.writers.lock()` 后，在锁内创建 writer、`on_data()` 和 `on_flush()`：
  `crates/zippy-gateway/src/lib.rs:1023`。

影响：

多个 stream 的远端写入会争用同一把 writers map 锁。某个 stream 触发 rollover、descriptor publish
或 persist health 检查时，可能阻塞其他 stream 的写入，形成跨标的/跨策略尾延迟耦合。

修复建议：

- 全局 map 只保护查找/插入；每个 writer 自己持有独立锁。
- 创建 writer 的慢路径与写入热路径拆开。
- metrics 增加 writer lock wait、materialize、flush、descriptor publish 分段耗时。

### P011 subscribe 仍有轮询路径，空闲订阅会引入延迟下限和无效扫描

证据：

- Gateway subscribe 没有新 batch 时固定 `sleep(5ms)`：
  `crates/zippy-gateway/src/lib.rs:1726`。
- ROADMAP 已记录需要接入 segment committed-row notification：`ROADMAP.md:54`。

影响：

实时订阅的空闲 CPU 与最低延迟都会受轮询间隔影响。对于高频行情，5ms 级等待可能已经超过很多
策略链路预算。

修复建议：

- Gateway subscribe 使用 `ActiveSegmentReader::wait_for_notification_after()` 或等价通知机制。
- rollover、shutdown、descriptor update 也作为唤醒源。
- 保留 best-effort live 语义，但不要用固定 sleep 当主要调度机制。

## P2 问题

### P012 当前 benchmark 场景不能代表目标容量

证据：

- `crosssectional_pipeline` bench 只有 1024 行、32 个 symbol、3 个因子，并且每次迭代重新创建 engine：
  `crates/zippy-engines/benches/crosssectional_pipeline.rs:21`。
- segment perf smoke 只有 2000 行、2 个 instrument，并且断言只是分位数大于 0：
  `crates/zippy-segment-store/src/perf.rs:147`、`crates/zippy-segment-store/tests/openctp_segment_perf_smoke.rs:28`。
- ROADMAP 仍把 Gateway performance profiles 列为后续项：`ROADMAP.md:84`。

影响：

现有 benchmark 可以做微小回归提示，但不能回答“1024/5000/10000 标的、100/500 因子、长时间运行、
多文件查询、远端订阅并发”是否达标。

修复建议：

- 增加容量矩阵：symbol 数、factor 数、window 大小、batch size、persisted file 数、subscriber 数。
- benchmark 不应默认重建 engine；应区分冷启动、稳态、rollover、flush、stop。
- 每个 profile 输出机器信息、参数、git sha、RSS 峰值和 p50/p95/p99/p999。

### P013 Python remote writer 逐行 dict 缓冲再转换 Arrow，不适合高频远端写入默认路径

证据：

- `RemoteGatewayWriter` 用 `_rows: list[dict]` 缓冲，`write()` 对 scalar dict 做
  `dict(value)`，flush 时再 `_value_to_pyarrow_table()`：
  `python/zippy/__init__.py:3009`、`python/zippy/__init__.py:3017`、
  `python/zippy/__init__.py:3027`。
- 每次 `_send_table()` 都是一帧 `write_batch` 请求，没有长连接批量流水：
  `python/zippy/__init__.py:3056`。

影响：

如果用户从 Python 逐 tick 远端写入，开销会集中在 Python dict、PyArrow 转换、socket request/response。
这条路径更像便利 API，不应被当作高频写入默认方案。

修复建议：

- 文档中明确高频远端写入应批量传 Arrow table/RecordBatch，避免逐行 dict。
- 增加长连接 writer 或 async pipeline writer，支持背压和批量流水。
- 在 `zippy-perf` 中加入 Python remote writer profile，避免该路径性能不可见。

## 建议的修复顺序

1. 先补性能门禁：运行 `zippy-perf`，加 p99/p999 和内存/队列阈值。没有门禁就无法知道后续改动是否真的提升。
2. 修 StreamTable Arrow batch columnar 写入，因为本轮已有 copy vs forward 的巨大 p99 差距。
3. 修 Gateway streaming collect 和 persisted parallel scan，避免查询侧峰值内存和慢客户端占用。
4. 修 TimeSeries/CrossSectional/ReactiveLatest/KeyValue 的按行 batch、全量 clone 和全量 snapshot。
5. 修 ReactiveState rolling factor O(window) 和多因子中间 batch 构造。
6. 最后扩展 Python/remote 高吞吐路径和订阅 notification-driven 模式。

## 100% 信心条件

只有在以下条件满足后，才应重新声称“接近 100% 性能信心”：

- 所有 P0/P1 热路径问题有修复或有量化证明不影响目标场景。
- CI/nightly 有真实性能矩阵和明确阈值，不只是 bench 编译。
- 至少覆盖：
  - 1M、5M rows/s 进程内时序聚合；
  - 1000、5000、10000 symbols 截面计算；
  - 50、200、500 factors reactive chain；
  - stream table copy/forward/persist rollover；
  - Gateway write/collect/subscribe 并发；
  - persisted parquet 100、1000 文件扫描；
  - 30 分钟以上 soak，记录 p99/p999、RSS、队列深度、drop/error。
- 性能报告随 commit 存档，包含参数、机器、git sha 和结果 JSON。
