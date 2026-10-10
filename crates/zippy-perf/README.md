# zippy-perf

性能工具只测量现行计算内核与共享内存基础设施。可用 profile：

- `timeseries-kernel`：同步 TimeSeriesEngine 计算与最终 flush。
- `stream-table-segment-copy`：物化写入 active segment。
- `single-row-low-rate`：单行物化写入。
- `segment-write-to-read`：同进程独立 producer/consumer 线程间的 segment 可见性。
- `stream-table-subscriber-e2e`：物化器写入至外部 reader 读取。

旧后台 Source/Publisher、ZeroMQ remote pipeline、runtime-stream-table 和 descriptor forwarding profile 已删除，
CLI 不接受旧 profile、endpoint、buffer-capacity、overflow-policy 或 max-queue-depth。
原生远端 Flight 的功能与恢复验收由 `scripts/check_native_client.py` 执行。
旧名 `write-to-subscribe-e2e` 已删除，CLI 和 JSON 报告配置均不接受该名称或旧枚举值。

## 同步计算基准

```bash
cargo run -p zippy-perf --release -- timeseries-kernel \
  --rows-per-batch 4096 \
  --target-rows-per-sec 1000000 \
  --duration-sec 60 \
  --warmup-sec 10 \
  --symbols 1024 \
  --max-p95-micros 5000 \
  --max-p99-micros 10000 \
  --report-json /tmp/zippy-perf-kernel.json
```

`latency_micros` 包含 Arrow 输入转为 SegmentTableView、同步内核计算与输出计数耗时。
旧 `inproc-timeseries` 仅测入队返回耗时，两者不能横向比较。新 CI 门禁为 p95 ≤ 5 ms、p99 ≤ 10 ms。目标速率下
每批间隔为 4.096 ms，持续吞吐仍必须达到目标的 90%；尾延迟另给共享 runner 调度余量。
目标主机应重新采集并校准同步内核基线。
吞吐以稳态窗口内完成处理的 rows/batches 为分子，分母从 steady start 到
`max(configured steady end, final flush completion)`；检查全部输入都完成处理。
生成器使用有理数 deadline 和半开窗口，避免在测量窗口结束后继续接收批次。

## 单机重构基线清单

`--baseline-manifest-json` 在运行 benchmark 前写入 `planned` 清单，运行成功后覆写为
`completed`，执行失败时覆写为 `failed`。每次状态更新都通过同目录临时文件和原子 rename
完成。清单包含完整 Git SHA、工作树 dirty 状态、实际可执行文件路径/大小/SHA-256、包版本、
debug/release 构建身份、完整 `PerfConfig`、数据集 ID、输入契约和数据生成参数。
完成后的清单还会内嵌原始 `PerfReport`，便于单文件归档和自动比较。

先用 dry-run 审计将要执行的基线，不会启动 benchmark：

```bash
cargo run -p zippy-perf --release -- timeseries-kernel \
  --duration-sec 60 \
  --warmup-sec 10 \
  --dataset-id zippy-perf.synthetic-ticks.v1 \
  --baseline-manifest-json /tmp/zippy-single-node-baseline.json \
  --dry-run
```

正式采集仍复用同一个 profile 驱动，只需去掉 `--dry-run`。如需保留独立原始报告，可以同时
传入 `--report-json`；manifest 不依赖该选项。本地 profile 未指定数据集 ID 时使用版本化 synthetic 默认值。
当前 manifest 格式为 v2；只支持当前配置和完整延迟字段，不为旧队列报告补默认值。

Git SHA 和 dirty 状态在构建 `zippy-perf` 时嵌入，因此复制二进制后不依赖原源码目录。无 `.git`
的可复现构建必须显式设置 `ZIPPY_BUILD_GIT_SHA` 和 `ZIPPY_BUILD_GIT_DIRTY=true|false`。

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
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- segment-write-to-read \
  --rows-per-batch 1 \
  --target-rows-per-sec 200 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --symbols 32 \
  --max-p99-micros 300
```

`segment-write-to-read` 强制 `rows_per_batch=1`，使用独立 producer/consumer 线程。consumer
先进入 waiter/futex notification wait；producer 按节奏写入行并 commit，consumer 被唤醒后调用
`ActiveSegmentReader::read_available()`，再逐行读取共享 monotonic origin 下的
`sent_elapsed_ns` 并记录延迟。这个边界覆盖 commit、notify、consumer 线程调度、mmap
可见性、阻塞唤醒和 read availability，不经过 Flight、ingest、WAL 或 TableActor，也不包含
Python 序列化、解码或公开客户端 callback 执行。真实公开写入到订阅路径由
`scripts/bench_public_e2e.py` 单独测量。

报告的 `latency.sample_count` 和 `latency.max_micros` 分别给出稳态样本数与最大值。只有
produced/consumed 总数相等，且稳态 produced/consumed/sample count 全部相等时才会构造报告，
因此该边界把零丢失作为硬门禁。旧同步实现的 `p50/p95/p99=11/17/24 us` 基线与新的独立
consumer 分布测量边界不同，不能横向比较。

定时/手动 workflow 在共享 `ubuntu-latest` runner 上使用 `p95 <= 1000 us`、
`p99 <= 5000 us`，只用于阻挡粗粒度回归。原生 Linux 固定主机的目标仍是
`p99 <= 300 us`；落门禁前应在目标主机重复运行 10-20 次完成基线校准。

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

## 报告与门禁

内核、读写或 flush 失败直接返回错误，不生成成功报告。正常完成后，`pass` 要求平均吞吐
至少达到目标的 90%，并满足显式设置的 `--max-p95-micros` / `--max-p99-micros`。
当前报告只记录实际测得的输入、输出、吞吐、峰值、延迟和内核指标，不再输出恒为零的旧队列、
发布错误或 Source 指标。延迟报告必须包含 sample_count 和 max_micros。

`actual_peak_rows_per_sec` 取以稳态起点为 origin 的绝对半开 1 秒桶的最大行数。
`metadata.started_at_unix_ms` 在配置验证后、耗时工作开始前捕获。

CLI 会在阈值不通过时先写报告再以 exit code 2 退出。执行错误以 exit code 1 退出，
baseline manifest 记录 failed 状态与错误。CI 使用 `if: always()` 上传已生成报告。
