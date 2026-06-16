# zippy-perf

`zippy-perf` 是 Zippy 的持续压测工具，用于验证进程内和跨进程 pipeline 在稳态流量下的吞吐、延迟和运行状态。

当前已实现的 profile：

- `inproc-timeseries`
- `remote-pipeline-upstream`
- `remote-pipeline-downstream`
- `stream-table-segment-copy`
- `stream-table-segment-forward`
- `single-row-low-rate`
- `write-to-subscribe-e2e`
- `runtime-stream-table-e2e`
- `stream-table-subscriber-e2e`

## 最小用法

查看帮助：

```bash
cargo run -p zippy-perf -- --help
```

进程内时序压测：

```bash
cargo run -p zippy-perf --release -- inproc-timeseries \
  --rows-per-batch 4096 \
  --target-rows-per-sec 1000000 \
  --duration-sec 60 \
  --warmup-sec 10 \
  --symbols 1024 \
  --max-p95-micros 1000 \
  --max-p99-micros 5000 \
  --max-queue-depth 0
```

跨进程压测需要先启动 downstream，再启动 upstream：

```bash
cargo run -p zippy-perf --release -- remote-pipeline-downstream \
  --endpoint tcp://127.0.0.1:5560 \
  --rows-per-batch 4096 \
  --target-rows-per-sec 1000000 \
  --duration-sec 60 \
  --warmup-sec 10 \
  --symbols 1024
```

```bash
cargo run -p zippy-perf --release -- remote-pipeline-upstream \
  --endpoint tcp://127.0.0.1:5560 \
  --rows-per-batch 4096 \
  --target-rows-per-sec 1000000 \
  --duration-sec 60 \
  --warmup-sec 10 \
  --symbols 1024
```

`remote-pipeline-downstream` 的运行时语义：

- 会先等待远端 stream 的首个 `HELLO/DATA` 事件，再进入正式观察窗口
- 在已经观测到远端 stream 后，如果长时间没有新的事件到达，会按 idle grace 主动收口
- 因为底层控制事件走单通道 `PUB/SUB`，`STOP` 仍是 best-effort；idle 收口是为了避免下游因为漏掉 `STOP` 一直挂到 deadline

输出 JSON 报告：

```bash
cargo run -p zippy-perf --release -- inproc-timeseries \
  --duration-sec 10 \
  --warmup-sec 0 \
  --report-json /tmp/zippy-perf-report.json
```

低频单行写入基准：

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- single-row-low-rate \
  --target-rows-per-sec 200 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --symbols 32 \
  --max-p99-micros 200
```

`single-row-low-rate` 会强制 `rows_per_batch=1`，测量单行 `StreamTableMaterializer::on_data()` 同步调用耗时。它会按目标行数预设 active segment 容量，避免 rollover 尖峰混入低频常态指标。

写入到同机 reader 可见性基准：

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- write-to-subscribe-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --symbols 32 \
  --max-p99-micros 300
```

`write-to-subscribe-e2e` 同样强制 `rows_per_batch=1`。它直接在 segment-store 内执行 `write row -> commit prefix -> ActiveSegmentReader::read_available()`，测量 committed prefix 对同机 reader 可见的延迟。这个 profile 不包含 runtime queue、worker 调度、Python callback、watcher 打印或真实 CTP 回调，因此适合和 LDC watcher 结果拆口径对比。

runtime fast data path 基准：

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- runtime-stream-table-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --symbols 32 \
  --max-p99-micros 500
```

`runtime-stream-table-e2e` 强制 `rows_per_batch=1`。它启动一个 synthetic native source，按低频单行发送 `SegmentTableView` 给 `StreamTableMaterializer`，主 `latency_micros` 表示行内时间戳到 publisher 收到输出的端到端耗时，覆盖 `SourceSink::emit -> fast data queue -> worker -> stream_table on_data -> publisher`。`secondary_latency_micros` 表示 source 线程里 `sink.emit(SourceEvent::Data)` 返回耗时，通常主要反映入队和唤醒通知成本。这个 profile 不包含 Python callback 或 watcher 打印。

stream_table 目标表 external reader 基准：

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- stream-table-subscriber-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --symbols 32 \
  --max-p99-micros 200
```

`stream-table-subscriber-e2e` 强制 `rows_per_batch=1`。它同步调用 `StreamTableMaterializer::on_data()` 写入目标 active segment，然后用外部 `ActiveSegmentReader::read_available()` 读取目标表新增 committed row。主 `latency_micros` 表示 `on_data -> external reader` 完整耗时，`secondary_latency_micros` 表示单独的 `on_data` 耗时。这个 profile 用来拆 `stream_table active segment -> subscriber read`，不包含 runtime queue 或 Python callback。

## 性能门禁

`zippy-perf` 的 `pass` 不是单纯的平均吞吐判断。默认会检查：

- engine 没有进入 failed 状态
- 平均吞吐不低于目标吞吐的 90%
- 没有 dropped batches
- 没有 publish errors
- remote source 没有 decode errors

可以用以下可选阈值把尾延迟和队列堆积纳入门禁：

- `--max-p95-micros`
- `--max-p99-micros`
- `--max-queue-depth`

示例：

```bash
cargo run -p zippy-perf --release -- stream-table-segment-copy \
  --rows-per-batch 4096 \
  --target-rows-per-sec 1000000 \
  --duration-sec 60 \
  --warmup-sec 10 \
  --symbols 1024 \
  --max-p95-micros 2000 \
  --max-p99-micros 15000
```

## 当前输出

首版报告包含：

- metadata：git sha、target、started_at_unix_ms
- input rows total
- output rows total
- actual average rows/s
- actual peak rows/s
- batches/s
- batch write latency p50/p95/p99
- final engine status
- engine metrics
- remote profile 的 source metrics
- pass/fail

## 当前边界

- 远程压测通过两个独立进程运行，不自动拉起对端
- `pass` 以目标吞吐的 `90%` 作为默认吞吐判定线，便于本地小规模 smoke 与高目标压测共用一套逻辑
- 延迟和队列阈值默认关闭；生产门禁应显式设置这些阈值
