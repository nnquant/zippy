# Zippy 性能审计修复状态

日期：2026-05-12

依据：`docs/performance_audit_findings_2026-05-12.md`

范围：本轮按审计文档修复低风险、边界清楚的问题。涉及协议、存储状态模型、查询执行器、
跨进程通知或大规模 engine 状态重构的问题先标记跳过，留待后续专项设计。

## 状态总览

| 问题 | 状态 | 本轮处理 |
| --- | --- | --- |
| P001 性能门禁没有运行真实压测和尾延迟阈值 | 已修复基础门禁 | `zippy-perf` 增加 p95/p99/queue depth 阈值，新增 nightly/manual performance workflow。 |
| P002 Gateway collect 一次性整批执行 | 部分修复，剩余继续处理 | `collect(stream=True)` 显式启用 Gateway `collect_stream` 多帧协议；默认 `collect()` 保持旧路径。 |
| P003 StreamTable Arrow batch 按行写入 | 已修复典型路径 | 非空 Arrow batch 走 columnar append；含 null batch 保留原逐行语义。 |
| P004 TimeSeriesEngine 全量 clone 和按行 key 分配 | 部分修复，剩余继续处理 | 无 id filter 的常见路径不再物化全量 row index Vec；状态 clone、String key 分配和 id interning 留待专项。 |
| P005 CrossSectionalEngine 按行 OwnedRow 和全量排序 | 部分修复，剩余继续处理 | bucket 非递减输入不再分配并排序全量 row index Vec；OwnedRow/concat 和多因子重复扫描留待专项。 |
| P006 Latest/KeyValue 更新触发大状态重建 | 部分修复，剩余继续处理 | `ReactiveLatestEngine` 不再每批 clone 全量 latest map；KeyValue 全量 snapshot replace 留待专项。 |
| P007 ReactiveState 多因子中间 batch 与 O(window) rolling | 部分修复，剩余继续处理 | rolling mean/std 改为 per-id running sum/sumsq；factor 中间 batch 重建留待专项。 |
| P008 Arrow bridge 物化 segment 行范围 | 部分修复，剩余继续处理 | `RowSpanView` 支持 projection batch，只物化请求列；zero-copy/chunked reader 留待专项。 |
| P009 persisted parquet scan 串行与 filter 读后执行 | 部分修复，剩余继续处理 | persisted parquet 读取改为按 reader batch 逐块过滤，避免每文件过滤前先 concat；并行 scan/row-group pruning 留待专项。 |
| P010 Gateway write 全局 writers mutex 包住 flush | 部分修复，剩余继续处理 | writer map 改为 per-stream writer handle；已有 writer 的 `on_data/on_flush` 不再持有全局 map 锁。 |
| P011 subscribe 固定 sleep 轮询 | 部分修复，剩余继续处理 | 本 gateway 写入成功后通知 subscribe idle wait；跨进程 active-segment notification 留待专项。 |
| P012 benchmark 场景不能代表目标容量 | 部分修复，剩余跳过 | performance workflow 覆盖 3 个核心 profile；JSON 报告补充 git sha、target 和启动时间；完整容量矩阵仍需专项。 |
| P013 Python remote writer 逐行 dict 路径不可见 | 部分修复，剩余跳过 | Python docstring 明确高频路径应传 Arrow table/RecordBatch；新增 `RemoteGatewayWriter.write_batch()` 显式批量入口；profile 仍需专项。 |

## 已落地修复

### P001 `zippy-perf` 阈值门禁

变更：

- `PerfConfig` 增加 `max_p95_micros`、`max_p99_micros`、`max_queue_depth`。
- `evaluate_pass()` 在吞吐、drop、publish error 之外检查尾延迟和队列深度。
- CLI 增加 `--max-p95-micros`、`--max-p99-micros`、`--max-queue-depth`。
- 新增 `.github/workflows/performance.yml`，支持 nightly 和手动运行：
  - `inproc-timeseries`
  - `stream-table-segment-copy`
  - `stream-table-segment-forward`

边界：

- 远端 upstream/downstream 需要双进程编排，本轮没有放入 workflow，避免把 CI 变成脆弱的
  orchestration 测试。
- RSS 峰值和 p999 尚未采集；需要后续扩展 `zippy-perf` report schema。

### P002 Gateway streaming collect opt-in

变更：

- Python `Table.collect(stream=True)` 显式启用远端 streaming collect；默认 `collect()` 不传 streaming
  标志，保持现有单响应协议。
- `_RemoteQuery.collect_plan(..., stream=True)` 走新的 `_remote_collect_stream_request()`。
- Gateway 新增 `collect_stream` 请求种类，响应顺序为：
  - `collect_start`：返回 schema payload；
  - 多个 `collect_chunk`：每个 payload 是一个 Arrow IPC table chunk；
  - `collect_end`：返回 metrics。
- `chunk_rows` 可由请求指定；本轮测试用 `chunk_rows=1` 验证多 chunk 行为。

边界：

- 本轮修复的是协议和客户端显式 opt-in。Gateway 内部当前仍复用现有 collect 计算路径生成结果后再切
  chunk，下一步还要把 scan/filter/project/encode 改成真正分块流水线。
- 默认 collect 没变，避免破坏已有客户端和测试。

### P003 StreamTable columnar materialization

变更：

- `StreamTableMaterializer` 对普通 Arrow batch 输入增加非空 batch fast path。
- 非空 batch 使用 `PartitionWriterHandle::write_columnar_rows()`，一次锁内写入多行，并只发布一次
  committed prefix。
- 如果任意列包含 null，仍回退到原逐行写入路径，保持 nullable 行为不变。
- rollover、descriptor publish、retention、persist enqueue 继续沿用原语义。

边界：

- 当前 fast path 仍会为 Arrow arrays 生成临时 typed vectors，主要先消除逐行锁/通知/事务成本。
- 含 null batch 还没有 columnar validity 写入能力，所以本轮保守跳过。

### P004 TimeSeries row selection fast path

变更：

- `TimeSeriesEngine` 新增内部 `RowSelection`，用 `All { row_count }` 表达未过滤批次。
- 无 `id_filter` 时不再构造 `(0..row_count)` 的全量 `Vec<u32>`。
- `ProcessedInput`、迟到数据校验和 drop-with-metric 过滤统一消费 `RowSelection` iterator。
- 有 pre factor 时仍在构造 accepted batch 前显式转换为 row index `Vec<u32>`，保持现有
  `record_batch_from_table_rows()` 接口不变。

边界：

- 本轮没有改动 `open_windows` / `last_dt_by_id` 的 clone-on-write rollback 语义。
- 每行 `id.to_string()` 和 `BTreeMap<String, ...>` 状态模型仍是剩余热点，需要和 id interning、
  slot state 设计一起处理。
- `DropWithMetric` 路径仍会构造 accepted row vector，因为它本身需要剔除迟到行。

### P005 CrossSectional row order fast path

变更：

- `CrossSectionalEngine` 新增内部 `RowOrder`，用自然行序表示 bucket 非递减输入批次。
- `on_data()` 先扫描 bucket 顺序；只有发现 bucket 回退时才分配 `Vec<usize>` 并按
  `(bucket_start, row_index)` 排序。
- 乱序批次保留旧排序语义，同一 bucket 内仍按原始 row index 稳定处理。

边界：

- 本轮没有改动 `BTreeMap<String, OwnedRow>` 状态模型。
- 每行 `extract_owned_row()`、finalize 时 `concat_batches()`、以及多个截面因子重复扫描
  `bucket_batch` 仍是剩余热点。
- 真正的 columnar bucket buffer、append/update fast path 和多因子共享统计 pass 需要后续专项处理。

### P006 ReactiveLatest local update collection

变更：

- `ReactiveLatestEngine` 新增本批 `LatestBatchUpdates` 收集路径。
- `on_data()` 不再 clone 全量 `latest_rows`；先把本批 key -> row 更新收集到局部 map。
- 输出 batch 直接从本批局部更新构造，成功后再合入 `self.latest_rows`，保留失败时不污染状态的
  rollback 语义。
- 同一批内同 key 多次更新仍只输出最终最新行，输出 key 顺序继续由 `BTreeSet` 保持稳定。

边界：

- 本轮没有改动每行 `OwnedRow` / 单行 `RecordBatch` 抽取。
- `KeyValueTableMaterializer` 仍会在每批后构造全量 snapshot 并 `replace_with_table()`，这是 P006
  剩余的主要热点。
- slot-level latest state、增量 snapshot 和 active segment 原地更新仍需后续专项设计。

### P007 Reactive rolling window state

变更：

- `StatefulFloatById` 的窗口状态从裸 `VecDeque<f64>` 改为内部 `WindowHistory`。
- `WindowHistory` 在 push/trim 时维护 running `sum` 和 `sum_squares`。
- rolling mean 从每行扫描窗口求和改为 O(1) 读取 running sum。
- rolling std 从每行两次扫描窗口改为 O(1) 使用 `sum_squares / n - mean^2`，并对浮点误差导致的
  微小负 variance 做 0 下限保护。
- delay/diff/return 继续复用窗口队列，输出语义不变。
- rollback 仍记录每个 dirty id 的完整窗口快照，保持现有事务恢复边界。

边界：

- 本轮没有改 `ReactiveStateEngine` 每个 factor 构造中间 `RecordBatch` 的方式。
- 每行 id 仍会转成 `String` 存入 per-factor `HashMap`。
- factor evaluation context、列追加式 batch builder 和 id interning 仍需后续专项。

### P008 RowSpan projection batch export

变更：

- `RowSpanView` 新增 `as_record_batch_with_projection(&[&str])`。
- projection batch 只构造请求列的 Arrow schema，并只调用对应列的 `project_array()`。
- 默认 `as_record_batch()` 保持全列导出行为不变。

边界：

- 本轮没有实现 active/sealed segment 的 Arrow buffer zero-copy。
- Utf8 和 nullable 列仍会在列投影时分配新 Arrow array。
- query/subscribe 侧还需要逐块 reader，而不是先把大 span 一次性导出成单个 batch。

### P009 Persisted parquet chunked filtering

变更：

- persisted parquet 读取新增 chunked batch helper，reader batch size 固定为 1024 行。
- 普通 persisted scan 不再把每个 parquet 文件先 concat 成单个 batch 后过滤，而是逐 reader batch
  执行 projection/filter，再把非空结果交给上层统一 concat。
- tail persisted scan 保留从新到旧读取文件和最终正序输出语义，同时按 chunk 过滤。
- 旧单 batch parquet helper 已移除，避免热路径回到“先 concat 再过滤”。

边界：

- 本轮没有引入 bounded scan pool；多个 persisted 文件仍按确定性顺序串行读取。
- filter 仍是 Arrow batch 读后执行，还没有使用 parquet row-group statistics pruning。
- `scanned_rows` 仍表示 projection 后、filter 前的实际扫描行数，指标语义保持不变。

### P010 Gateway per-stream writer lock

变更：

- `GatewayState.writers` 从直接保存 `GatewayTableWriter` 改为保存可克隆的 per-stream writer handle。
- 写入已有 stream 时，全局 `writers` map 锁只用于取得 handle；`on_data()` 和 `on_flush()` 在
  单个 stream 的 writer lock 下执行。
- `collect()`、`get_stream()` 和 subscribe 获取 active batch/schema 时也先克隆 writer handle，再释放
  全局 map 锁后访问 materializer。
- `close_writer()` / `close_all_writers()` 先从全局 map 移除 handle，再锁具体 writer 执行 stop/unregister。

边界：

- 首次创建 writer 仍在全局 map 锁内完成，避免并发重复 register stream/source 的竞态。
- 本轮没有实现 per-stream writer actor，也没有增加 lock wait / materialize / flush 分段耗时指标。
- 同一 stream 内写入仍串行，这是必要的单 writer 顺序语义。

### P011 Gateway subscribe idle notification

变更：

- `GatewayState` 增加 `GatewaySubscribeNotifier`，用单调递增 activity sequence 区分真实写入活动。
- `write_batch()` 在 `on_data()` 和 `on_flush()` 成功后发布 subscribe activity notification。
- subscribe 空闲时不再固定 `sleep(5ms)`；现在先记录 fetch 前的 sequence，若 fetch 后没有新 batch，
  则等待 activity notification，最多 1 秒兜底。
- sequence 检查覆盖“写入发生在 fetch 和 wait 之间”的竞态，避免订阅方错过通知后无意义等待。

边界：

- 本轮通知只覆盖通过当前 gateway 写入的 stream。
- 其他进程写入、segment rollover、descriptor 更新和 shutdown 还没有接入
  `ActiveSegmentReader::wait_for_notification_after()`。
- subscribe 仍是 best-effort 语义；本轮没有引入 per-stream subscriber registry 或 backpressure policy。

### P012 性能覆盖

变更：

- nightly/manual workflow 产出 JSON report artifact。
- README 补齐 stream table profiles 和门禁阈值用法。
- `PerfReport` 增加 metadata，记录 `git_sha`、`target`、`started_at_unix_ms`，让报告可追溯到
  commit、运行目标和启动时间。

边界：

- 还没有覆盖 5000/10000 symbols、200/500 factors、多文件 parquet scan、Gateway 并发订阅、
  30 分钟 soak。

### P013 Python remote writer

变更：

- `RemoteGatewayWriter` 和 `write_stream()` docstring 明确 scalar dict 写入是便利路径。
- 高频远端写入应传 Arrow table/RecordBatch，避免 Python dict 和 PyArrow row conversion 成为热路径。
- `RemoteGatewayWriter.write_batch()` 提供显式批量入口，直接发送 PyArrow table、record batch、
  list[dict] 或 DataFrame-like 对象，并拒绝 scalar row dict，避免热路径误用逐行便利 API。

边界：

- 尚未实现长连接 async writer 或 Python remote writer `zippy-perf` profile。

## 跳过项的后续入口

这些项目不适合在本轮直接改代码，因为它们会改变架构边界或核心状态模型：

- Gateway 查询协议：P002、P009 剩余项。
- engine 状态存储模型：P004/P005/P006/P007 剩余项。
- segment/Arrow 读侧内存模型：P008 剩余项。
- Gateway 写入调度和订阅调度：P010、P011 剩余项。
- 性能容量矩阵、长跑和报告扩展：P012 剩余项。
- Python remote writer pipeline/profile：P013 剩余项。

建议后续拆成六个专项：

1. Gateway streaming collect + persisted parallel scan。
2. Engine columnar state model。
3. Segment Arrow zero-copy/chunked reader。
4. Gateway per-stream writer actor + notification-driven subscribe。
5. Capacity matrix + soak + RSS/p999 performance reporting。
6. Python remote writer async pipeline + zippy-perf profile。

## 本轮验证

- `cargo test -p zippy-engines -p zippy-operators -p zippy-segment-store -p zippy-gateway -p zippy-perf`：
  全部通过。
- `cargo clippy -p zippy-engines -p zippy-operators -p zippy-segment-store -p zippy-gateway -p zippy-perf --all-targets -- -D warnings`：
  通过。
- `cargo fmt --check`：通过。
- `uv run black --check python/zippy/__init__.py pytests/test_python_api.py`：通过。
- `uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream_true or remote_query_collect_plan_stream_true or remote_query_sends_snapshot_id or remote_gateway_writer_write_batch or remote_gateway_writer"`：
  7 个测试通过。
- `cargo test -p zippy-perf`：10 个测试通过。
- `cargo test -p zippy-engines --test stream_table_materializer`：30 个测试通过。
- `cargo test -p zippy-engines -p zippy-perf`：通过。
- `cargo test -p zippy-gateway --test native_gateway native_gateway_collect_stream_returns_start_chunks_and_end_frames -- --nocapture`：通过。
- `cargo test -p zippy-gateway --test native_gateway -- --test-threads=1`：17 个测试通过。
- `cargo clippy -p zippy-gateway --all-targets -- -D warnings`：通过。
- `cargo test -p zippy-engines timeseries::tests::row_selection -- --nocapture`：2 个测试通过。
- `cargo test -p zippy-engines --test timeseries_engine -- --nocapture`：22 个测试通过。
- `cargo test -p zippy-engines`：126 个测试通过。
- `cargo test -p zippy-engines cross_sectional::tests::row_order -- --nocapture`：2 个测试通过。
- `cargo test -p zippy-engines cross_sectional::tests::build_row_order -- --nocapture`：2 个测试通过。
- `cargo test -p zippy-engines --test cross_sectional_engine -- --nocapture`：11 个测试通过。
- `cargo test -p zippy-engines reactive_latest::tests::collect_latest_updates -- --nocapture`：1 个测试通过。
- `cargo test -p zippy-engines --test reactive_latest_engine -- --nocapture`：3 个测试通过。
- `cargo test -p zippy-operators reactive::tests::window_history -- --nocapture`：1 个测试通过。
- `cargo test -p zippy-operators --test reactive_factors -- --nocapture`：20 个测试通过。
- `cargo test -p zippy-engines --test reactive_engine -- --nocapture`：15 个测试通过。
- `cargo test -p zippy-operators -p zippy-engines`：154 个测试通过。
- `cargo test -p zippy-segment-store --test arrow_bridge row_span_record_batch_projection_materializes_only_requested_columns -- --nocapture`：
  1 个测试通过。
- `cargo test -p zippy-segment-store --test arrow_bridge -- --nocapture`：18 个测试通过。
- `cargo test -p zippy-segment-store`：78 个测试通过。
- `cargo clippy -p zippy-segment-store --all-targets -- -D warnings`：通过。
- `cargo test -p zippy-gateway persisted_parquet_reader_exposes_batches_before_concat -- --nocapture`：
  1 个测试通过。
- `cargo test -p zippy-gateway --test native_gateway persisted -- --nocapture --test-threads=1`：
  2 个测试通过。
- `cargo test -p zippy-gateway --test native_gateway -- --test-threads=1`：17 个测试通过。
- `cargo test -p zippy-gateway gateway_writer_handle_is_cloneable_for_per_stream_locking -- --nocapture`：
  1 个测试通过。
- `cargo test -p zippy-gateway subscribe_notifier -- --nocapture`：2 个测试通过。
- `cargo test -p zippy-gateway --lib`：5 个测试通过。
- `cargo test -p zippy-gateway --test native_gateway -- --test-threads=1`：17 个测试通过。
- `cargo clippy -p zippy-gateway --all-targets -- -D warnings`：通过。
- `cargo test -p zippy-perf writes_report_json_with_profile_and_pass_fields -- --nocapture`：
  1 个测试通过。
- `cargo test -p zippy-perf`：10 个测试通过。
- `uv run pytest pytests/test_python_api.py -q -k "remote_gateway_writer_write_batch"`：
  2 个测试通过。
- `uv run pytest pytests/test_python_api.py -q -k "remote_gateway_writer"`：
  4 个测试通过。
- `cargo clippy -p zippy-operators --all-targets -- -D warnings`：通过。
- `cargo clippy -p zippy-engines --all-targets -- -D warnings`：通过。
- `cargo clippy -p zippy-engines -p zippy-perf --all-targets -- -D warnings`：通过。
- `cargo fmt --check`：通过。
- `uv run black --check python/zippy/__init__.py`：通过。
- `uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream_true or remote_query_collect_plan_stream_true or remote_query_sends_snapshot_id"`：
  3 个测试通过。
- `uv run black --check python/zippy/__init__.py pytests/test_python_api.py`：通过。
- `uv run pytest pytests/test_python_api.py -q -k "remote_collect_stream_true or remote_query_collect_plan_stream_true or remote_query_sends_snapshot_id or remote_gateway_writer_write_batch or remote_gateway_writer"`：
  7 个测试通过。
- `cargo run -p zippy-perf --release -- inproc-timeseries --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024 --max-p95-micros 1000 --max-p99-micros 5000 --max-queue-depth 0`：
  `pass=true`，`p50=24us p95=36us p99=48us`。
- `cargo run -p zippy-perf --release -- stream-table-segment-copy --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024 --max-p95-micros 5000 --max-p99-micros 20000`：
  `pass=true`，`p50=236us p95=488us p99=4034us`。
- `cargo run -p zippy-perf --release -- stream-table-segment-forward --rows-per-batch 4096 --target-rows-per-sec 1000000 --duration-sec 3 --warmup-sec 1 --symbols 1024 --max-p95-micros 1000 --max-p99-micros 5000`：
  `pass=true`，`p50=38us p95=67us p99=81us`。
