# Performance Harness Design

## Background

当前仓库已有的 benchmark 主要基于 `criterion`，目标是测局部算子或单个 engine 在小批次上的函数级吞吐。

现有 benchmark 不覆盖以下关键问题：

- engine/runtime 在长时间持续写入下的稳态吞吐
- 队列背压、overflow policy 与控制事件的真实行为
- `ZmqStreamPublisher -> ZmqSource` 跨进程链路的协议与传输开销
- `pipeline` / `consumer` 两种远程 source 模式在持续流量下的行为差异
- 写入延迟分位数、丢包、协议错误、源侧 sequence gap 等可观测指标

因此需要新增一个独立的性能测试程序，而不是继续扩展当前的 `criterion` bench。

## Goals

新增一个独立的 Rust 压测程序，用于回答下面两个问题：

1. 进程内 pipeline 在持续输入下，能否稳定承受 `1_000_000 rows/s`
2. 跨进程 pipeline 在持续输入下，能否稳定承受 `1_000_000 rows/s`

首版压测程序必须：

- 支持持续运行而非单次迭代
- 支持匀速发流而非一次性灌满
- 复用长期存活的 engine/runtime
- 输出吞吐、延迟和 runtime/source 指标
- 区分进程内与跨进程两种路径
- 使用可复现的 synthetic market data

## Non-Goals

首版不做：

- 真实 CTP 行情接入
- Kafka / SHM / 多 topic 压测
- 自动生成 flamegraph / perf report
- CI 中强制性能阈值门禁
- 图形化 dashboard
- Python 作为主压测驱动

## High-Level Approach

新增一个独立 crate：`crates/zippy-perf`。

它提供一个 Rust CLI，用于运行长时间压测。

首版支持四类 profile：

- `inproc-reactive`
- `inproc-timeseries`
- `remote-raw`
- `remote-pipeline`

这四类 profile 分别覆盖：

- 纯进程内 reactive 计算上限
- 纯进程内时序聚合上限
- 纯远程传输上限
- 远程 `TimeSeriesEngine -> ZmqStreamPublisher -> ZmqSource -> CrossSectionalEngine` 组合上限

## Why a Dedicated CLI

独立 CLI 比 `criterion` 更合适，原因如下：

- 需要持续运行几十秒到几分钟
- 需要打印稳态窗口指标，而不是一次函数迭代平均值
- 需要支持 upstream/downstream 多进程角色
- 需要支持外部参数配置和结果导出
- 需要验证 runtime queue、source event、flush/stop 等完整生命周期

## CLI Shape

首版 CLI 形状如下：

```bash
cargo run -p zippy-perf --release -- inproc-timeseries
cargo run -p zippy-perf --release -- remote-raw-upstream
cargo run -p zippy-perf --release -- remote-raw-downstream
cargo run -p zippy-perf --release -- remote-pipeline-upstream
cargo run -p zippy-perf --release -- remote-pipeline-downstream
```

公共参数：

- `--rows-per-batch`
- `--target-rows-per-sec`
- `--duration-sec`
- `--warmup-sec`
- `--symbols`
- `--buffer-capacity`
- `--overflow-policy`
- `--endpoint`
- `--report-json`

## Data Model

压测程序使用可复现 synthetic tick data。

基础字段：

- `symbol: Utf8`
- `dt: timestamp[ns, UTC]`
- `price: f64`
- `volume: f64`

生成规则：

- `symbol` 在固定 universe 中循环
- `dt` 单调递增，按目标速率推进
- `price` 按稳定公式变化，例如 `base + (row_index % 100) * step`
- `volume` 使用简单周期值，避免全部常数导致算子过于理想化

为了避免把 Arrow 构造成本混入 engine 成本，首版使用“预生成 batch 模板 + 改写时间列”的策略复用数据。

## Profiles

### inproc-reactive

链路：

`generator -> ReactiveStateEngine -> NullPublisher`

用途：

- 测 reactive factor 与本地 runtime 上限

### inproc-timeseries

链路：

`generator -> TimeSeriesEngine -> NullPublisher`

用途：

- 测本地窗口聚合和 flush 路径上限

### remote-raw

upstream：

`generator -> ZmqStreamPublisher`

downstream：

`ZmqSource -> null sink`

用途：

- 单独测 stream protocol 与 ZMQ 传输能力
- 把“纯传输瓶颈”和“计算瓶颈”拆开

### remote-pipeline

upstream：

`generator -> TimeSeriesEngine -> ZmqStreamPublisher`

downstream：

`ZmqSource -> CrossSectionalEngine -> NullPublisher`

用途：

- 测当前主线中最接近真实跨进程处理链的上限

## Rate Control

压测必须是匀速发流，而不是“尽快写入”。

发流器按下面逻辑工作：

- 根据 `target_rows_per_sec / rows_per_batch` 计算目标 batch 频率
- 每次发送前等待到目标时间点
- 若发送落后，则记录 lag 指标，但不通过休眠补偿历史积压

这样测得的是持续稳态吞吐，而不是瞬时打爆队列的峰值。

## Runtime Model

每次压测只创建一次 engine/runtime，并在整个压测周期内复用。

压测分三段：

1. `warmup`
2. `steady-state`
3. `cooldown`

语义：

- warmup 阶段不计最终指标
- steady-state 阶段统计吞吐、延迟与错误
- cooldown 阶段执行 `flush/stop` 并收集最终状态

## Metrics

首版必须输出以下指标：

- input rows total
- output rows total
- actual average rows/s
- actual peak rows/s
- batches/s
- write latency p50
- write latency p95
- write latency p99
- final engine status

若 profile 含 engine/runtime，还要输出：

- `dropped_batches_total`
- `late_rows_total`
- `publish_errors_total`

若 profile 含 `ZmqSource`，还要输出：

- `source_events_total`
- `source_data_batches_total`
- `source_control_events_total`
- `source_decode_errors_total`
- `source_schema_mismatches_total`
- `source_sequence_gaps_total`

## Pass Criteria

“扛住 100 万条/秒”在首版中定义为：

- steady-state 连续运行期间，`actual average rows/s >= 1_000_000`
- final status 不为 `failed`
- `dropped_batches_total == 0`
- `publish_errors_total == 0`
- 若有 source 指标，则 `source_decode_errors_total == 0`

延迟阈值首版不设 hard fail，但必须打印出来。

## Output Format

默认输出人类可读摘要。

如果传入 `--report-json <path>`，则额外写一份 JSON 结果，方便后续做趋势比较。

JSON 至少包含：

- profile
- config
- start/end timestamp
- throughput summary
- latency summary
- engine/source metrics
- final pass/fail

## Process Layout

### In-Process Profiles

单进程执行即可。

### Remote Profiles

使用两个独立进程：

- upstream role
- downstream role

二者通过单个 endpoint 的 stream protocol 通信。

首版不在压测工具内部自动拉起对端进程，先通过两个命令分别启动，避免引入进程编排复杂度。

后续如有需要，再补一个轻量 orchestration wrapper。

## Benchmarks vs. Perf Harness

现有 `criterion` bench 继续保留，用于：

- 算子/engine 微基准
- 回归比较
- `cargo bench --no-run` 编译门禁

新增 `zippy-perf` 用于：

- 稳态吞吐
- runtime/source/queue 行为
- 跨进程传输和生命周期验证

二者职责不同，不互相替代。

## Initial Scope

首版仅交付：

- `crates/zippy-perf`
- `inproc-timeseries`
- `remote-pipeline-upstream`
- `remote-pipeline-downstream`
- JSON 报告输出
- README 级使用说明

之所以不把所有 profile 一次做全，是因为 `inproc-timeseries + remote-pipeline` 已经足够覆盖：

- 本地计算上限
- 远程处理链上限
- 当前主线最重要的时序与截面处理路径

`inproc-reactive` 与 `remote-raw` 可以在后续迭代补上。

## Risks

### Synthetic Data Too Idealized

如果所有 symbol 和数值分布过于简单，压测可能高估真实性能。

缓解方式：

- 保持多 symbol
- 保持价格和成交量的轻度变化
- 后续可增加 profile 参数控制数据分布

### ZMQ Slow Joiner Effects

跨进程 profile 在启动阶段可能受 PUB/SUB 握手影响。

缓解方式：

- downstream 先启动
- upstream 在 warmup 前等待一个短暂握手窗口

### Arrow Construction Dominates

如果每次都全量新建 batch，可能测到的是数据构造而非 pipeline。

缓解方式：

- 复用 batch 模板
- 把时间列更新成本控制在可解释范围内

## Validation Plan

实现完成后至少验证：

- CLI 在 `--help` 下可用
- `inproc-timeseries` 能运行并输出汇总
- `remote-pipeline-upstream/downstream` 能完成一次端到端压测
- JSON 报告结构稳定
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`

## Open Follow-Ups

不阻塞首版，但建议后续追加：

- `inproc-reactive`
- `remote-raw`
- 自动拉起双进程脚本
- perf baseline 文档
- flamegraph 集成
