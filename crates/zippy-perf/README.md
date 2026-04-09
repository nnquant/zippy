# zippy-perf

`zippy-perf` 是 Zippy 的持续压测工具，用于验证进程内和跨进程 pipeline 在稳态流量下的吞吐、延迟和运行状态。

当前已实现的 profile：

- `inproc-timeseries`
- `remote-pipeline-upstream`
- `remote-pipeline-downstream`

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
  --symbols 1024
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

## 当前输出

首版报告包含：

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

- 首版只实现 `inproc-timeseries` 与 `remote-pipeline`
- 远程压测通过两个独立进程运行，不自动拉起对端
- `pass` 以目标吞吐的 `90%` 作为默认判定线，便于本地小规模 smoke 与高目标压测共用一套逻辑
