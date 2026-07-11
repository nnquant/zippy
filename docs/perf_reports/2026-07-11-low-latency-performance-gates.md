# Low-Latency Performance Gates Release Baseline

日期：2026-07-11

目标：保存 Phase 2 性能门禁的前后口径证据，建立 consumer-observed latency 和
drain-inclusive throughput 的新发布基线。

## 证据身份

- Phase 2 基础代码 commit：`3ff850ade14dcbfbe9b636db94a9832aded20a87`
- 实施计划与 Task0 证据 commit：`ae25e1f4a6cdf14bc9f00f03be794dbbd2b7b4d3`
- 已审阅 Phase 2 HEAD：`9f31e7fd7d84de17234805afec096c9283f26703`
- target：`linux-x86_64`
- Task0 stdout：`/tmp/zippy-perf-write-to-subscribe-before.txt`
- Task0 JSON：`target/zippy-perf-write-to-subscribe-before.json`
- 新 consumer stdout：`/tmp/zippy-perf-write-to-subscribe-after.txt`
- 新 consumer JSON：`target/zippy-perf-write-to-subscribe-e2e.json`
- in-process stdout：`/tmp/zippy-perf-inproc-timeseries-drain.txt`
- in-process JSON：`target/zippy-perf-inproc-timeseries-drain.json`

## 旧同步基线

Task0 在基础代码 `3ff850ade14dcbfbe9b636db94a9832aded20a87` 未修改
`zippy-perf` 实现的前提下运行。计划证据 commit `ae25e1f4a6cd` 只增加实施计划；
profile stdout 中记录该运行时 HEAD。

命令：

```bash
set -euo pipefail
git diff --quiet 3ff850ade14dcbfbe9b636db94a9832aded20a87..HEAD -- \
  crates/zippy-perf/src/lib.rs crates/zippy-perf/src/main.rs
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- \
  write-to-subscribe-e2e \
  --rows-per-batch 1 \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 2 \
  --symbols 32 \
  --report-json target/zippy-perf-write-to-subscribe-before.json \
  | tee /tmp/zippy-perf-write-to-subscribe-before.txt
jq -e '.pass == true' target/zippy-perf-write-to-subscribe-before.json
```

保存的完整 stdout：

```text
2026-07-11T00:16:36.235396565Z  INFO performance profile started component=[perf] event=[profile_start] profile=[WriteToSubscribeE2e]
profile=WriteToSubscribeE2e
git_sha=ae25e1f4a6cd
target=linux-x86_64
pass=true
engine_status=stopped
input_rows_total=2401
output_rows_total=2401
actual_average_rows_per_sec=200.10
actual_peak_rows_per_sec=201.00
batches_per_sec=240.10
latency_micros[p50=11.00, p95=17.00, p99=24.00]
2026-07-11T00:16:48.238165497Z  INFO performance profile finished component=[perf] event=[profile_finish] profile=[WriteToSubscribeE2e] pass=[true]
```

Task0 JSON 精确值：

| 字段 | 值 |
| --- | ---: |
| `pass` | `true` |
| `input_rows_total` | `2401` |
| `output_rows_total` | `2401` |
| `actual_average_rows_per_sec` | `200.1` |
| `actual_peak_rows_per_sec` | `201.0` |
| `batches_per_sec` | `240.1` |
| `latency.p50_micros` | `11.0` |
| `latency.p95_micros` | `17.0` |
| `latency.p99_micros` | `24.0` |

## Consumer-Boundary 新基线

命令：

```bash
set -o pipefail
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- \
  write-to-subscribe-e2e \
  --rows-per-batch 1 \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 2 \
  --symbols 32 \
  --max-p95-micros 1000 \
  --max-p99-micros 5000 \
  --report-json target/zippy-perf-write-to-subscribe-e2e.json \
  | tee /tmp/zippy-perf-write-to-subscribe-after.txt
```

保存的完整 stdout：

```text
2026-07-11T02:38:56.758406953Z  INFO performance profile started component=[perf] event=[profile_start] profile=[WriteToSubscribeE2e]
profile=WriteToSubscribeE2e
git_sha=9f31e7fd7d84
target=linux-x86_64
pass=true
engine_status=stopped
input_rows_total=2400
output_rows_total=2400
actual_average_rows_per_sec=200.00
actual_peak_rows_per_sec=200.00
batches_per_sec=200.00
latency_micros[count=2000, p50=49.00, p95=77.00, p99=99.00, max=200.00]
2026-07-11T02:39:08.754350753Z  INFO performance profile finished component=[perf] event=[profile_finish] profile=[WriteToSubscribeE2e] pass=[true]
```

新 JSON 精确值：

| 字段 | 值 |
| --- | ---: |
| `pass` | `true` |
| `input_rows_total` | `2400` |
| `output_rows_total` | `2400` |
| `engine_metrics.processed_rows_total` | `2400` |
| `actual_average_rows_per_sec` | `200.0` |
| `actual_peak_rows_per_sec` | `200.0` |
| `batches_per_sec` | `200.0` |
| `latency.sample_count` | `2000` |
| `latency.p50_micros` | `49.0` |
| `latency.p95_micros` | `77.0` |
| `latency.p99_micros` | `99.0` |
| `latency.max_micros` | `200.0` |

由于系统没有 `jq`，本轮使用 Python 标准库对 JSON 做同等严格断言：

```bash
python3 -c "import json; d=json.load(open('target/zippy-perf-write-to-subscribe-e2e.json')); assert d['pass'] is True; assert d['input_rows_total'] == d['output_rows_total']; assert d['latency']['sample_count'] > 0; assert d['latency']['max_micros'] >= d['latency']['p99_micros']; assert d['engine_metrics']['processed_rows_total'] == d['output_rows_total']"
```

结果：退出码为 0，`input == output == processed == 2400`，样本数为 `2000`，
并且 `max 200.0us >= p99 99.0us`。

## In-Process Drain 检查

命令：

```bash
set -o pipefail
cargo run -p zippy-perf --release -- inproc-timeseries \
  --rows-per-batch 4096 \
  --target-rows-per-sec 100000 \
  --duration-sec 3 \
  --warmup-sec 1 \
  --symbols 128 \
  --report-json target/zippy-perf-inproc-timeseries-drain.json \
  | tee /tmp/zippy-perf-inproc-timeseries-drain.txt
```

保存的完整 stdout：

```text
2026-07-11T02:39:55.906324152Z  INFO performance profile started component=[perf] event=[profile_start] profile=[InprocTimeseries]
2026-07-11T02:39:55.906858398Z  INFO engine started component=[runtime] engine=[InprocTimeseries] event=[start] status=[running]
2026-07-11T02:39:59.922773824Z  INFO engine stopped component=[runtime] engine=[InprocTimeseries] event=[stop] batch_rows=[0]
profile=InprocTimeseries
git_sha=9f31e7fd7d84
target=linux-x86_64
pass=true
engine_status=stopped
input_rows_total=405504
output_rows_total=405504
actual_average_rows_per_sec=100504.54
actual_peak_rows_per_sec=102400.00
batches_per_sec=24.54
latency_micros[count=74, p50=26.00, p95=33.00, p99=44.00, max=152.00]
2026-07-11T02:39:59.924698521Z  INFO performance profile finished component=[perf] event=[profile_finish] profile=[InprocTimeseries] pass=[true]
```

in-process JSON 精确值：

| 字段 | 值 |
| --- | ---: |
| `pass` | `true` |
| accepted rows (`input_rows_total`) | `405504` |
| processed rows (`engine_metrics.processed_rows_total`) | `405504` |
| final queue (`engine_metrics.queue_depth`) | `0` |
| max observed queue (`engine_metrics.max_observed_queue_depth`) | `1` |
| `actual_average_rows_per_sec` | `100504.54332534889` |
| `actual_peak_rows_per_sec` | `102400.0` |
| `engine_metrics.processed_batches_total` | `99` |
| `engine_metrics.output_batches_total` | `100` |
| `batches_per_sec` | `24.537242022790256` |

机械断言结果：退出码为 0，`pass == true`，accepted 与 processed 都为 `405504`，
且最大观测队列 `1` 大于等于最终队列 `0`。

## 验证结果

在已审阅 HEAD `9f31e7fd7d84de17234805afec096c9283f26703` 上运行：

| 命令 | 结果 |
| --- | --- |
| `cargo test -p zippy-perf --lib` | 通过，37 passed，0 failed |
| `cargo fmt --check` | 通过，退出码 0 |
| `cargo clippy -p zippy-perf --all-targets -- -D warnings` | 通过，退出码 0 |
| `git diff --check` | 通过，退出码 0 |
| consumer release profile | 通过，`pass=true` |
| consumer JSON 严格断言 | 通过，退出码 0 |
| in-process release profile | 通过，`pass=true` |
| in-process JSON 严格断言 | 通过，退出码 0 |

## 口径与发布注意事项

旧 profile 在 producer 线程内同步执行 `commit -> read_available()`，不包含 futex
阻塞 consumer 的唤醒与调度路径。新 profile 使用独立阻塞 consumer，并在 consumer
真实观察到 row 后记录延迟。因此旧同步分布 `11/17/24us` 与新 consumer-boundary
分布 `49/77/99us` 不能做数值横向比较；它们共同证明测量边界已改变。新
consumer-boundary 序列从本报告开始成为后续回归的新 baseline。

workflow 使用的 `p95 <= 1000us`、`p99 <= 5000us` 是 shared-runner 回归阈值，
不是生产 SLA。行数一致性和 timeout 仍是确定性的硬门禁；生产阈值应基于固定硬件、
调度策略和真实负载单独校准。

JSON 新增的 `LatencySummary.sample_count`、`LatencySummary.max_micros` 和
`EngineMetricsReport.max_observed_queue_depth` 是带 serde default 的 additive 字段，
旧 JSON 仍可反序列化。它们会改变外部 Rust struct literal 的 source compatibility；
这是仓库内部 `0.5.0` 性能 harness 的已接受兼容性边界，不应被解读为外部 Rust API
完全 source-compatible。
