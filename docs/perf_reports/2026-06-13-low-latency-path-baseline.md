# Low Latency Path Baseline

日期：2026-06-13

目标：把 LDC/OpenCTP 低频 tick 延迟拆成可复现的本地基线，避免把
OpenCTP、segment-store、runtime、subscriber 和 Python watcher 的开销混在一起。

## 测量环境

- repo: `/home/jiangda/develop/zippy`
- git sha: `09e27328f908`
- target: `linux-x86_64`
- shm: `TMPDIR=/dev/shm`
- rate: `200 rows/s`
- duration: `10s`
- warmup: `1s`
- symbols: `4`

## 命令与结果

### 1. 单行 stream_table on_data

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- single-row-low-rate \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 1 \
  --symbols 4
```

结果：

```text
latency_micros[p50=26.00, p95=38.00, p99=51.00]
```

口径：单行 `StreamTableMaterializer::on_data()` 同步调用耗时。该 profile 会按
目标行数预设 row capacity，避免 rollover 尖峰混入低频常态指标。

### 2. segment writer 到同机 reader

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- write-to-subscribe-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 1 \
  --symbols 4
```

结果：

```text
latency_micros[p50=15.00, p95=23.00, p99=31.00]
```

口径：`write row -> commit prefix -> ActiveSegmentReader::read_available()`。不包含
runtime queue、worker 调度、Python callback、watcher 打印或真实 CTP 回调。

### 3. runtime source 到 stream_table publisher

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- runtime-stream-table-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 1 \
  --symbols 4
```

结果：

```text
latency_micros[p50=104.00, p95=146.00, p99=185.00]
secondary_latency_micros[p50=19.00, p95=25.00, p99=29.00]
```

口径：

- `latency_micros`: 行内 timestamp 到 publisher 收到输出，覆盖
  `SourceSink::emit -> fast data queue -> worker -> stream_table on_data -> publisher`。
- `secondary_latency_micros`: source 线程内 `sink.emit(SourceEvent::Data)` 返回耗时，
  主要反映入队和唤醒通知成本。

### 4. stream_table 到 external reader

```bash
TMPDIR=/dev/shm cargo run -p zippy-perf --release -- stream-table-subscriber-e2e \
  --target-rows-per-sec 200 \
  --duration-sec 10 \
  --warmup-sec 1 \
  --symbols 4
```

结果：

```text
latency_micros[p50=24.00, p95=35.00, p99=55.00]
secondary_latency_micros[p50=22.00, p95=32.00, p99=52.00]
```

口径：

- `latency_micros`: `StreamTableMaterializer::on_data()` 写目标 active segment 后，
  外部 `ActiveSegmentReader::read_available()` 读到新增 row 的完整耗时。
- `secondary_latency_micros`: 单独的 `StreamTableMaterializer::on_data()` 耗时。

### 5. Python subscriber callback probe

```bash
uv run python examples/07_ops/03_subscribe_latency_probe.py \
  --uri /tmp/zippy-probe-master.sock \
  --drop-existing \
  --rows 300 \
  --interval-ms 1 \
  --row-capacity 100000 \
  --warmup-ms 50 \
  --discard-first-rows 20 \
  --xfast \
  --callback-work minimal
```

结果：

```text
minimal: callback_enter_delay_ms[p50=0.958, p95=1.200, p99=1.363]
minimal: callback_enter_minus_append_ms[p50=0.244, p95=0.349, p99=0.460]
minimal: callback_work_ms[p50=0.001, p95=0.002, p99=0.003]

to_dict: callback_enter_delay_ms[p50=0.979, p95=1.209, p99=1.464]
to_dict: callback_enter_minus_append_ms[p50=0.247, p95=0.357, p99=0.453]
to_dict: callback_work_ms[p50=0.001, p95=0.003, p99=0.003]

format: callback_enter_delay_ms[p50=0.958, p95=1.195, p99=1.389]
format: callback_enter_minus_append_ms[p50=0.239, p95=0.372, p99=0.590]
format: callback_work_ms[p50=0.005, p95=0.009, p99=0.013]
```

额外小样本：

```text
print stderr, 80 rows: callback_work_ms[p50=0.028, p95=0.044, p99=0.172]
```

刷新 Python native extension 后，打开 subscriber trace：

```bash
ZIPPY_SUBSCRIBER_LATENCY_TRACE=1 .venv/bin/python \
  examples/07_ops/03_subscribe_latency_probe.py \
  --uri /tmp/zippy-probe-master.sock \
  --drop-existing \
  --table subscribe_latency_probe_trace2 \
  --rows 40 \
  --interval-ms 1 \
  --row-capacity 100000 \
  --warmup-ms 50 \
  --discard-first-rows 5 \
  --slowest-rows 2 \
  --xfast \
  --callback-work minimal
```

关键输出：

```text
zippy_subscriber_latency_trace event=[rows_read] ... localtime_to_read_min_us=[1529.649]
zippy_subscriber_latency_trace event=[delivery] mode=[row] ... row_delivery_total_us=[44.487]
...
zippy_subscriber_latency_trace event=[delivery] mode=[row] ... row_delivery_total_us=[31.091]
```

口径：`row_delivery_total_us` 覆盖 subscriber 线程从 `RowSpanView` 开始，逐行构造
Python `Row`、调用 Python callback，并等待 callback 返回。40 行小样本中，除首条
attach 噪声约 `124us` 外，常态大多在 `31-60us`。这说明 native subscriber delivery
加最小 Python callback 不是 `0.3-0.5ms` 的主体。

口径：

- `callback_enter_delay_ms`: 行内 `localtime_ns` 到 Python callback 进入。
- `callback_enter_minus_append_ms`: `callback_enter_delay_ms - append_latency_ms`。
  这是本地 Python writer probe 的近似相对指标，用来避免把 `pipeline.write()`、
  Polars/Arrow 写入和 segment append 成本误算成 subscriber 处理延迟。它不是严格
  的物理分段延迟；subscriber 可以在 `pipeline.write()` 返回前进入 callback，
  所以该值可能为负。
- `callback_work_ms`: Python callback 内部工作，按 `--callback-work` 分别覆盖
  最小字段访问、`Row.to_dict()` 浅拷贝、watcher 风格字符串格式化和打印。

结论：这个 probe 不能用 `localtime_ns -> callback_enter` 直接代表 subscriber
处理延迟，因为 synthetic writer 的 `append_latency_ms` p50 约 `0.7ms`。扣除 append
后，subscriber/callback 入口的 p50 约 `0.24-0.25ms`，与 runtime profile 的
`~0.2ms` 量级一致。`Row.to_dict()` 在当前 Python 层只是浅拷贝，p99 约几微秒；
watcher 风格格式化 p99 约十几微秒；真实打印会增加几十微秒常态和更明显长尾。
native `row_delivery_total_us` 进一步把 `RowSpanView -> Python callback done`
压到了几十微秒常态。

## 当前延迟预算

| 路径 | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| segment writer -> reader | 15us | 23us | 31us |
| stream_table on_data | 26us | 38us | 51us |
| stream_table on_data -> external reader | 24us | 35us | 55us |
| stream_table on_data only in subscriber profile | 22us | 32us | 52us |
| runtime source -> stream_table publisher | 104us | 146us | 185us |
| source sink.emit return | 19us | 25us | 29us |
| Python subscriber after append, minimal callback | 244us | 349us | 460us |
| Python subscriber after append, format callback | 239us | 372us | 590us |
| native row delivery, minimal Python callback | ~31-45us | ~55-60us | ~124us attach noise |

解释：

1. segment-store committed prefix 可见性本体是几十微秒级，不解释 watcher 里
   `0.4ms` 级延迟。
2. 单行 `stream_table on_data` 在低频常态下 p99 约 `51us`，仍不是 ms 级来源。
3. runtime fast data path 加上 worker/materializer/publisher 后 p99 约 `185us`，
   比 segment-store 本体多出约 `130-150us`。
4. source 线程 `sink.emit()` 返回 p99 约 `29us`，说明主要尾部不在入队返回，
   而在 worker 被唤醒、调度并完成 materializer/publisher 的路径。
5. stream_table 目标 active segment 到 external reader 的增量只有几微秒，
   不是 `0.4ms` watcher 延迟的主因。
6. Python probe 的 `localtime_ns -> callback` 总延迟会被本地 Python writer
   的 `append_latency_ms` 放大到约 `1ms`。用它判断 live subscriber 延迟时，
   必须先确认锚点是否在 native source emit 之后；否则会把写入侧成本也算进去。
7. watcher callback 的 `to_dict` 和格式化本体不是主要来源；实际打印 IO 会贡献
   几十微秒常态成本，并可能制造长尾。
8. native subscriber delivery trace 显示 `RowSpanView -> Python callback done`
   常态几十微秒，进一步排除了 row 构造和最小 callback 是 `0.4ms` 主因的假设。

## 与 LDC watcher 的差额

此前 LDC row-mode watcher 观察到 `total_delay_ms` 约 `0.45-0.50ms`，而当前
runtime profile p99 约 `0.185ms`。剩余差额优先继续拆：

1. live OpenCTP source 内部 `callback/source_emit` 到 zippy runtime source emit。
2. zippy Python subscriber 线程把 `RowSpanView` 转成 Python row 并调用 callback。
   已重装 extension 的 native delivery trace 显示该段常态几十微秒；如果 live
   watcher 仍高，需要检查 callback 之外的调度/输出或 live source 对齐。
3. watcher callback 里的过滤、格式化和 `print(flush=True)`；当前 synthetic 显示
   格式化本体较小，打印 IO 才更可能解释几十微秒以上的尾部。
4. WSL2/调度噪声、非隔离核、Python GIL。

下一步应在不改 tick schema 的前提下，继续沿用 `source_emit_ns` 和 native
subscriber trace，把 live LDC 的
`CTP callback -> source_emit -> subscriber_read -> Python callback done`
与本报告的 runtime baseline 对齐。

当前四个切点的日志字段为：

| 切点/分段 | 字段 | 来源 |
| --- | --- | --- |
| `CTP callback -> source_emit` | `openctp.callback_to_source_emit_us_avg` | `OPENCTP_LATENCY_TRACE=1` |
| `source_emit -> subscriber_read` | `subscriber.source_emit_to_read.avg` | `ZIPPY_SUBSCRIBER_LATENCY_TRACE=1` |
| `subscriber_read -> Python callback done` | `subscriber.row_delivery_total` | `ZIPPY_SUBSCRIBER_LATENCY_TRACE=1` row mode |
| `Python watcher callback work/print` | `watcher.source_emit_ns.process_delay` | `scripts/watch_ticks.py` 输出 |

`openctp.callback_to_source_recv_us_avg` 仍保留为诊断字段，但它只到 source event
loop 收到 driver event，不等价于 `source_emit_ns` 写入 row 的时间点。live-chain
完整性检查应要求 `callback_to_source_emit_us_avg`，否则无法直接回答
`callback -> source_emit` 这一段。

注意 watcher 自身输出的 `process_delay_ms` 是 callback 内部的局部取样：

- row-mode：从 Python callback 入口到取 `observed_ns`，包含 row 提取、过滤和格式化前
  的准备，不包含后续 `print(..., flush=True)` 返回时间。
- table-mode：从 table callback 入口到匹配行取样，包含 Arrow filter、`to_pylist` 和
  行提取。
- 真正的 `subscriber_read -> Python callback done` 应以 native
  `subscriber.row_delivery_total` 为准；它等待 Python callback 返回，因此 row-mode 下会
  覆盖 watcher callback 内的打印和 summary 输出。

`scripts/analyze_latency_trace.py` 会额外输出 `derived.*` 估算项，例如
`derived.callback_tail_after_watcher_observed`，用于近似观察
`row_delivery_total - watcher.process_delay` 这段 callback 尾部/打印成本。它们是跨日志
聚合均值，不是逐 tick 配对样本。

## LDC watcher 口径修正

2026-06-13 检查 `local-market-data-center` 的 `tick_watch.py` 后确认，watcher
虽然已经读取 `source_emit_ns`，但 `_delay_anchor_ns()` 旧逻辑优先使用
`localtime_ns`，只有 `localtime_ns <= 0` 时才 fallback 到 `source_emit_ns`。这解释了
此前输出仍然是：

```text
delay_anchor=localtime_ns queue_delay_ms=0.389 process_delay_ms=0.067 total_delay_ms=0.457
```

该口径不是我们要的 subscriber 处理延迟，它包含了 `localtime_ns -> source_emit_ns`
之前的 openctp/source 内部时间。已在 LDC watcher 中改为：

1. 优先使用 `source_emit_ns`。
2. `source_emit_ns` 缺失或无效时 fallback 到 `localtime_ns`。
3. 两者都无效时再 fallback 到 `dt`。

LDC 侧已新增 `scripts/analyze_latency_trace.py --require-live-chain` 和
`--min-paired-samples`，它要求同时看到 OpenCTP、subscriber 和 watcher 三类日志字段，
并可强制要求足够数量的四段 full-chain 单 tick 配对样本。完整采集命令：

采集前先做只读 readiness 检查，确认当前 Python 环境加载到的 native extension
确实包含所需 trace 字段：

```bash
cd /home/jiangda/services/local-market-data-center
uv run python scripts/check_latency_trace_readiness.py
```

期望输出十项 `status=ok`：

```text
status=ok check=openctp_native_trace_fields ...
status=ok check=openctp_native_sink_trace_fields ...
status=ok check=zippy_subscriber_trace_fields ...
status=ok check=zippy_runtime_stream_trace_fields ...
status=ok check=zippy_native_source_trace_fields ...
status=ok check=analyzer_live_chain_gate ...
status=ok check=analyzer_full_chain_pair_gate ...
status=ok check=analyzer_full_chain_diagnostics_gate ...
status=ok check=analyzer_evidence_ledger_gate ...
status=ok check=analyzer_diagnosis_gate ...
```

然后在交易时段采集：

```bash
cd /home/jiangda/services/local-market-data-center
uv run python scripts/prepare_latency_trace_pm_config.py \
  --output /tmp/zippy.pm.latency-trace.toml

ZIPPY_PM_CONFIG=/tmp/zippy.pm.latency-trace.toml \
  scripts/cron/ldc-open.sh

uv run python scripts/capture_latency_trace.py \
  --instrument au2607 \
  --preflight-only \
  --output /tmp/ldc-au2607-latency-trace.log

uv run python scripts/capture_latency_trace.py \
  --instrument au2607 \
  --duration-sec 30 \
  --wait-ready-sec 300 \
  --min-paired-samples 20 \
  --output /tmp/ldc-au2607-latency-trace.log
```

这个 wrapper 会：

1. 先跑 readiness，确认当前 Python 环境加载到的 native extension 含 OpenCTP、
   native sink、subscriber、runtime/stream-table、native source bridge trace 字段，
   并确认 analyzer 能输出 full-chain、evidence ledger 和 diagnosis 行。
2. 只读打印 `ldc_ctp_ticks` 的 stream 状态，特别是 `writer_process_id`、
   `active_segment_id` 和 `descriptor_generation`。
3. 如果 `writer_process_id` 存在，只读检查 `/proc/<pid>/environ`，确认 writer
   进程启动时是否带有 `OPENCTP_LATENCY_TRACE=1`。
4. 如果设置 `--wait-ready-sec`，只读轮询 stream status 和 writer env，等待已有
   writer 进入 trace-ready；不会启动、停止或重启 writer。
   若等待到期仍未 ready，会输出 `wait_ready_status=timeout waited_sec=...`。
5. 以 `ZIPPY_SUBSCRIBER_LATENCY_TRACE=1` 启动 duration-limited watcher，并把
   watcher stdout/stderr 写入 `--output`。
6. 对输出日志运行 live-chain analyzer；缺任一关键切点时返回 2。

`--preflight-only` 只做前 3 步，不启动 watcher。期望看到：

```text
capture_readiness ready=[true] live_writer=[true] writer_env_trace=[enabled] writer_internal_trace=[enabled] pm_config_trace=[...] pm_config_internal_trace=[...] reason=[ready]
capture_next_action action=[run_capture] reason=[ready]
capture_next_command command=[uv run python scripts/capture_latency_trace.py ...] safe_to_run=[true]
preflight_status=ready
```

如果当前 `writer_process_id=[None]` 或 writer 不是用 `OPENCTP_LATENCY_TRACE=1`
启动，默认会在启动 watcher 前返回 2。最终 reason 会区分无 writer 和有 writer
但 trace 未启用：

```text
capture_readiness ready=[false] live_writer=[false|true] writer_env_trace=[unknown|disabled|enabled] writer_internal_trace=[unknown|disabled|enabled] pm_config_trace=[disabled|enabled|unknown] pm_config_internal_trace=[disabled|enabled|unknown] reason=[no_live_writer|writer_env_not_enabled]
capture_next_action action=[prepare_trace_pm_config|wait_for_writer|restart_writer_with_trace_config] reason=[...]
capture_next_command command=[...] safe_to_run=[true|false]
capture_aborted reason=[no_live_writer|writer_env_not_enabled]
preflight_status=not_ready reason=[no_live_writer|writer_env_not_enabled]
```

`capture_readiness` 是总判定行，避免再手工组合 `stream_status`、`writer_env`
和 `writer_pm_config_trace`；现在 ready 要求 writer 的 `OPENCTP_LATENCY_TRACE` 和
`ZIPPY_RUNTIME_LATENCY_TRACE/ZIPPY_STREAM_TABLE_LATENCY_TRACE` 都启用。
`capture_next_action` 是下一步动作行；
`capture_next_command` 是下一条安全命令。这样可以避免等待一个必然缺 OpenCTP 段的
采样窗口。若只是想 smoke 下游 watcher 和 analyzer，可以显式加
`--allow-incomplete`。

缺 OpenCTP trace 或 zippy 内部 trace 时不能把该次结果当成完整延迟路径证据。
期望看到：

```text
writer_env writer_process_id=[...] openctp_latency_trace=[enabled] zippy_runtime_latency_trace=[enabled] zippy_stream_table_latency_trace=[enabled] writer_internal_trace=[enabled] value=[1]
```

如果输出是 `openctp_latency_trace=[disabled|unknown]` 或
`writer_internal_trace=[disabled|unknown]`，只能用该次日志分析已出现的下游段，不能闭环
`CTP callback -> source_emit -> runtime/stream_table -> subscriber_read` 的完整内部路径。

默认 `configs/zippy.pm.toml` 不永久打开 OpenCTP trace；
`prepare_latency_trace_pm_config.py` 会生成一份临时诊断配置，只给 `ldc-ctp-md` 加：

```toml
[tasks.ldc-ctp-md.env]
OPENCTP_LATENCY_TRACE = "1"
```

`scripts/cron/ldc-open.sh` 和 `scripts/cron/ldc-close.sh` 支持通过 `ZIPPY_PM_CONFIG`
显式选择这份临时配置。这样诊断开关只在本次 trace window 生效。
这一点由 `tests/process/test_runtime_assets.py::test_cron_scripts_encode_expected_start_and_stop_order`
覆盖，脚本内会先 `validate -f "${ZIPPY_PM_CONFIG}"` 再 `apply -f "${ZIPPY_PM_CONFIG}"`。

仍可直接手工分析已有日志：

```bash
uv run python scripts/analyze_latency_trace.py \
  --require-live-chain \
  --min-paired-samples 20 \
  /tmp/ldc-au2607-latency-trace.log
```

验证：

```bash
cd /home/jiangda/services/local-market-data-center
uv run python scripts/check_latency_trace_readiness.py
uv run python scripts/prepare_latency_trace_pm_config.py --output /tmp/zippy.pm.latency-trace.toml
uv run zippy pm validate -f /tmp/zippy.pm.latency-trace.toml
uv run pytest tests/test_latency_trace_pm_config.py tests/test_latency_trace_capture.py tests/test_latency_trace_readiness.py tests/test_latency_trace.py

cd /home/jiangda/develop/zippy-openctp
cargo fmt --check
cargo test -p zippy-openctp-core
```

结果：

```text
LDC readiness: 10 ok
LDC diagnostic pm config: validate passed
LDC latency trace tests: 75 passed
zippy-openctp: fmt passed; core full test suite passed
```

覆盖范围包括 `extract_matching_rows()` 的 anchor 选择，以及 table-mode / row-mode
CLI 输出中实际出现 `delay_anchor=source_emit_ns`。

当前 LDC live 状态仍无法做 au2607 现场复测；只读 preflight 的关键输出为：

```text
status=ok check=openctp_native_trace_fields ...
status=ok check=openctp_native_sink_trace_fields ...
status=ok check=zippy_subscriber_trace_fields ...
status=ok check=zippy_runtime_stream_trace_fields ...
status=ok check=zippy_native_source_trace_fields ...
status=ok check=analyzer_live_chain_gate complete_trace_accepted
status=ok check=analyzer_full_chain_pair_gate full_chain_pair_accepted
status=ok check=analyzer_full_chain_diagnostics_gate full_chain_diagnostics_accepted
status=ok check=analyzer_evidence_ledger_gate evidence_ledger_accepted
status=ok check=analyzer_diagnosis_gate diagnosis_accepted
stream_status stream_name=[ldc_ctp_ticks] error=[RuntimeError: failed to connect to zippy master uri=[tcp://127.0.0.1:17690]; start zippy-master or call zippy.connect(uri=...) with the active master endpoint]
writer_env writer_process_id=[None] openctp_latency_trace=[unknown] zippy_runtime_latency_trace=[unknown] zippy_stream_table_latency_trace=[unknown] writer_internal_trace=[unknown] reason=[no_writer]
writer_schedule writer_task=[ldc-ctp-md] pm_config=[configs/zippy.pm.toml] next_start=[2026-06-15T08:30:00+08:00] reason=[cron_start]
writer_pm_config_trace writer_task=[ldc-ctp-md] pm_config=[configs/zippy.pm.toml] openctp_latency_trace=[disabled] pm_internal_trace=[disabled] zippy_runtime_latency_trace=[disabled] zippy_stream_table_latency_trace=[disabled] reason=[env_missing]
capture_hint reason=[pm_config_trace_not_enabled] prepare=[uv run python scripts/prepare_latency_trace_pm_config.py --output /tmp/zippy.pm.latency-trace.toml] start_with=[ZIPPY_PM_CONFIG=/tmp/zippy.pm.latency-trace.toml]
capture_readiness ready=[false] live_writer=[false] writer_env_trace=[unknown] writer_internal_trace=[unknown] pm_config_trace=[disabled] pm_config_internal_trace=[disabled] reason=[no_live_writer]
capture_next_action action=[prepare_trace_pm_config] reason=[pm_config_trace_not_enabled]
capture_next_command command=[uv run python scripts/prepare_latency_trace_pm_config.py --output /tmp/zippy.pm.latency-trace.toml] safe_to_run=[true]
preflight_status=not_ready reason=[no_live_writer]
```

诊断 pm config 已只读生成并验证：

```text
latency_trace_pm_config base=[configs/zippy.pm.toml] output=[/tmp/zippy.pm.latency-trace.toml] task=[ldc-ctp-md] OPENCTP_LATENCY_TRACE=[1]
valid [local-market-data-center] tasks=[6]
[tasks.ldc-ctp-md.env]
OPENCTP_LATENCY_TRACE = "1"
ZIPPY_RUNTIME_LATENCY_TRACE = "1"
ZIPPY_STREAM_TABLE_LATENCY_TRACE = "1"
[tasks.ldc-master.env]
ZIPPY_RUNTIME_LATENCY_TRACE = "1"
ZIPPY_STREAM_TABLE_LATENCY_TRACE = "1"
```

`scripts/cron/ldc-open.sh`/`ldc-close.sh` 已确认会读取 `ZIPPY_PM_CONFIG`，并且
`bash -n` 语法检查通过；`test_cron_scripts_encode_expected_start_and_stop_order`
也覆盖了这条路径。

使用诊断 pm config 再跑 `--preflight-only`，仍因当前无 live writer 返回 2，
但 future writer trace-env 已变为 enabled：

```text
writer_schedule writer_task=[ldc-ctp-md] pm_config=[/tmp/zippy.pm.latency-trace.toml] next_start=[2026-06-15T08:30:00+08:00] reason=[cron_start]
writer_env writer_process_id=[None] openctp_latency_trace=[unknown] zippy_runtime_latency_trace=[unknown] zippy_stream_table_latency_trace=[unknown] writer_internal_trace=[unknown] reason=[no_writer]
writer_pm_config_trace writer_task=[ldc-ctp-md] pm_config=[/tmp/zippy.pm.latency-trace.toml] openctp_latency_trace=[enabled] pm_internal_trace=[enabled] zippy_runtime_latency_trace=[enabled] zippy_stream_table_latency_trace=[enabled] value=[1]
capture_readiness ready=[false] live_writer=[false] writer_env_trace=[unknown] writer_internal_trace=[unknown] pm_config_trace=[enabled] pm_config_internal_trace=[enabled] reason=[no_live_writer]
capture_next_action action=[wait_for_writer] reason=[no_live_writer]
capture_next_command command=[uv run python scripts/capture_latency_trace.py --instrument au2607 --duration-sec 30 --wait-ready-sec 300 --min-paired-samples 20 --output /tmp/ldc-au2607-latency-trace.log --pm-config /tmp/zippy.pm.latency-trace.toml] safe_to_run=[true]
preflight_status=not_ready reason=[no_live_writer]
```

`zippy pm ls` 只读状态显示 `ldc-ctp-md` 当前 `stopped`，上次运行窗口为
`06-12 20:30:00` 到 `06-13 05:00:03`，下一次自动启动为
`2026-06-15 08:30:00 Asia/Shanghai`；`capture_latency_trace.py` 现在也会从
`configs/zippy.pm.toml` 只读推导同一个 `writer_schedule`。同一份 preflight
还显示默认配置的 `writer_pm_config_trace` 为 `disabled/env_missing`，所以
**不能只等默认 cron 启动**；最终采样窗口必须让 `ldc-ctp-md` 使用带
`OPENCTP_LATENCY_TRACE=1` 的诊断 pm config。当前缺 writer 是非交易窗口下的
正常关闭，不是本次 trace 工具造成的残留状态。

下次开盘后使用 `capture_latency_trace.py` 做最终 live 对齐，不再手工只给
`watch_ticks.py` 加环境变量。原因是 `OPENCTP_LATENCY_TRACE=1` 必须出现在
`ldc-ctp-md` writer 进程启动环境里；只给 watcher 子进程加这个变量，无法补出
OpenCTP native 侧的 `callback -> source_emit` 段。

非开盘时可用隔离 TTS trace 替代真实交易所 session 做完整链路验证。该路径不会写
生产 `ldc_ctp_ticks`，而是生成 `/tmp/ldc.tts-trace.toml`，将 OpenCTP writer
隔离到 `ldc_ctp_tts_trace_ticks`，并把 writer stdout/stderr 与 watcher 输出合并到
同一个日志后交给同一套 analyzer：

```bash
cd /home/jiangda/services/local-market-data-center
uv run python scripts/capture_tts_latency_trace.py \
  --instrument au2607 \
  --prepare-only

uv run python scripts/capture_tts_latency_trace.py \
  --instrument au2607 \
  --duration-sec 45 \
  --warmup-sec 5 \
  --min-paired-samples 20 \
  --output /tmp/ldc-tts-trace-latency-trace.log
```

`--prepare-only` 只生成配置并打印命令，不连接 TTS：

```text
tts_trace_config base=[configs/ldc.toml] output=[/tmp/ldc.tts-trace.toml] broker=[tts_7_24] stream_name=[ldc_ctp_tts_trace_ticks] instruments=[au2607]
tts_trace_writer_command python -u -m local_market_data_center.process.ctp_md --config /tmp/ldc.tts-trace.toml --broker tts_7_24
tts_trace_watcher_command python -u scripts/watch_ticks.py --master-uri tcp://127.0.0.1:17690 --stream-name ldc_ctp_tts_trace_ticks --instrument au2607 --xfast --row-mode
```

这条替代路径比 `ldc-ctp-sim` 更接近生产，因为它使用
`OpenCtpMarketDataSource`，可以覆盖 OpenCTP native callback/source emit 段；
`ldc-ctp-sim` 只适合验证 generator -> stream_table -> subscriber/watcher 下游段。

TTS 45 秒实采已经通过完整性 gate：

```bash
uv run python scripts/analyze_latency_trace.py \
  --require-live-chain \
  --min-paired-samples 20 \
  /tmp/ldc-tts-trace-latency-trace.log
```

关键结果：

```text
lines_seen=149 segments=33
live_chain.status=complete
live_chain.full_chain_paired_sample count=30 paired=[true]
live_chain.full_chain_summary paired=[true] count=30 total_avg_us=932.173 total_p99_us=1163.262 total_max_us=1163.262 avg_dominant_segment=[source_emit_to_subscriber_read] p99_dominant_segment=[source_emit_to_subscriber_read] max_dominant_segment=[source_emit_to_subscriber_read]
live_chain.full_chain_segment name=[openctp_callback_to_source_emit] paired=[true] count=30 avg_us=162.095 p95_us=254.076 p99_us=327.370 max_us=327.370
live_chain.full_chain_segment name=[source_emit_to_subscriber_read] paired=[true] count=30 avg_us=516.693 p95_us=619.494 p99_us=682.571 max_us=682.571
live_chain.full_chain_segment name=[subscriber_read_to_py_callback_done] paired=[true] count=30 avg_us=253.385 p95_us=441.055 p99_us=456.938 max_us=456.938
live_chain.full_chain_segment name=[openctp_callback_to_py_callback_done] paired=[true] count=30 avg_us=932.173 p95_us=1089.607 p99_us=1163.262 max_us=1163.262
live_chain.full_chain_dominant_segment metric=[avg] name=[source_emit_to_subscriber_read] value_us=516.693 paired=[true] share_pct=55.429
live_chain.full_chain_dominant_segment metric=[p99] name=[source_emit_to_subscriber_read] value_us=682.571 paired=[true] share_pct=46.532
live_chain.full_chain_dominant_segment metric=[max] name=[source_emit_to_subscriber_read] value_us=682.571 paired=[true] share_pct=46.532
```

这说明在 TTS 替代链路中，四段已闭环；均值、p99 和 max 的 dominant segment 都是
`source_emit_to_subscriber_read`。生产 `ldc_ctp_ticks` 仍需要开盘后复验，但当前
native OpenCTP -> zippy stream_table -> subscriber -> Python callback 的延迟路径已经
可以完整分段。

2026-06-13 22:33 继续拆 `source_emit_to_subscriber_read` 时，zippy subscriber
driver 增加了 `event=[driver_poll]` trace，用于把 subscriber 侧同步读拆成：

```text
notification_sequence_us  # 读取 mmap notification sequence
read_available_us         # ActiveSegmentReader.read_available()
poll_to_event_us          # poll_next() 到 Rows event 返回前
```

同一条隔离 TTS capture 重新采 45 秒，完整性 gate 通过：

```text
lines_seen=199 segments=36
live_chain.status=complete
live_chain.full_chain_paired_sample count=34 paired=[true]
subscriber.driver_poll.notification_sequence count=34 min_us=0.030 avg_us=0.084 p95_us=0.341 p99_us=0.371 max_us=0.371
subscriber.driver_poll.read_available count=34 min_us=12.003 avg_us=14.345 p95_us=17.574 p99_us=23.466 max_us=23.466
subscriber.driver_poll.poll_to_event count=34 min_us=12.415 avg_us=14.886 p95_us=19.488 p99_us=24.037 max_us=24.037
live_chain.full_chain_segment name=[openctp_callback_to_source_emit] paired=[true] count=34 avg_us=179.560 p95_us=361.288 p99_us=566.914 max_us=566.914
live_chain.full_chain_segment name=[source_emit_to_subscriber_read] paired=[true] count=34 avg_us=525.080 p95_us=639.060 p99_us=651.440 max_us=651.440
live_chain.full_chain_segment name=[subscriber_read_to_py_callback_done] paired=[true] count=34 avg_us=238.229 p95_us=351.434 p99_us=354.762 max_us=354.762
live_chain.full_chain_segment name=[openctp_callback_to_py_callback_done] paired=[true] count=34 avg_us=942.869 p95_us=1304.247 p99_us=1476.647 max_us=1476.647
```

这个结果把 `source_emit_to_subscriber_read` 里的 subscriber driver 自身开销排除了：
`poll_next()` 从进入到返回 Rows 平均只有约 15us，p99 约 24us；其中
`read_available()` 是主要部分。也就是说 525us 均值里的大头不在
`ActiveSegmentReader.read_available()`、`RowSpanView` 构造或 cursor advance，而更像在
writer commit/visibility、subscriber 被唤醒前的调度等待、以及 writer 侧
`write_segment_rows -> emit_segment_available` 之间。下一步如果继续拆，应在 writer
侧把 `source_emit_ns` 之后到 segment write 完成、emit 通知发出之间再切开。

2026-06-13 22:57 继续拆 writer/runtime。新增两类 trace：

```text
openctp.source_emit_to_write_done_us       # OpenCTP source_emit_ns -> source active segment write done
openctp.emit_sink_us                       # OpenCTP SourceSink::emit(Data) 同步耗时
openctp.source_emit_to_emit_done_us        # source_emit_ns -> SourceSink::emit(Data) 返回后
runtime.source_emit_to_runtime_start.avg   # source_emit_ns -> zippy runtime worker 开始处理
runtime.runtime_on_data_us                 # StreamTableMaterializer::on_data()
runtime.runtime_publish_us                 # publish_tables()，通常写目标 active segment
runtime.runtime_data_total_us              # runtime worker data event 总耗时
```

TTS 45 秒实采，writer/watcher 分文件采集后合并，避免两进程日志行交织：

```text
lines_seen=303 segments=53
openctp.source_emit_to_write_done_us count=43 avg_us=91.364 p95_us=141.675 p99_us=610.641
openctp.emit_sink_us count=43 avg_us=152.123 p95_us=174.929 p99_us=273.400
openctp.source_emit_to_emit_done_us count=43 avg_us=262.281 p95_us=307.808 p99_us=776.842
runtime.source_emit_to_runtime_start.avg count=43 avg_us=282.882 p95_us=375.560 p99_us=796.761
runtime.runtime_on_data_us count=43 avg_us=234.941 p95_us=364.507 p99_us=610.891
runtime.runtime_publish_us count=43 avg_us=43.431 p95_us=114.810 p99_us=128.158
runtime.runtime_data_total_us count=43 avg_us=377.635 p95_us=647.826 p99_us=752.696
subscriber.driver_poll.poll_to_event count=37 avg_us=15.155 p95_us=25.139 p99_us=27.243
subscriber.source_emit_to_read.avg count=37 avg_us=597.487 p95_us=985.590 p99_us=1032.299
live_chain.full_chain_summary paired=[true] count=37 total_avg_us=1001.229 total_p99_us=1612.668 total_max_us=1612.668 avg_dominant_segment=[source_emit_to_subscriber_read]
```

这轮拆分把主要耗时从 subscriber driver 往前推到了 zippy runtime：

1. OpenCTP source 自己写 source active segment 平均约 91us。
2. `SourceSink::emit(Data)` 平均约 152us，但 fast path 下它只是入队并唤醒 worker，
   不代表目标 stream table 已经 materialize。
3. runtime worker 从 `source_emit_ns` 到开始处理平均约 283us，说明入队/唤醒/调度
   已经是显著部分。
4. `StreamTableMaterializer::on_data()` 平均约 235us，是 runtime worker 内部最大的
   已拆子段。
5. subscriber driver `poll_next -> Rows` 仍只有约 15us，基本可以排除为主因。

注意：runtime 子段目前是 aggregate trace，还没有用单行 `source_emit_ns` 与
subscriber read 做逐 tick 配对，因此不要把各段均值直接相加当作严格单 tick 总延迟。
如果要继续精确闭环，下一步应让 `zippy_runtime_latency_trace` 在单行输入时输出
`source_emit_ns`，再在 analyzer 中增加
`source_emit -> runtime_start -> runtime_publish_done -> subscriber_read` 的 paired
样本。

2026-06-13 23:04 已补 runtime `source_emit_ns` 并完成 paired 验证。TTS 45 秒实采：

```text
runtime.source_emit_to_runtime_start.avg count=42 avg_us=285.149 p95_us=371.186 p99_us=865.581
runtime.runtime_on_data_us count=42 avg_us=220.052 p95_us=275.074 p99_us=800.816
runtime.runtime_publish_us count=42 avg_us=35.496 p95_us=42.051 p99_us=51.771
runtime.runtime_data_total_us count=42 avg_us=363.833 p95_us=444.824 p99_us=1021.334
subscriber.source_emit_to_read.avg count=38 avg_us=581.718 p95_us=758.902 p99_us=777.500
live_chain.full_chain_summary paired=[true] count=38 total_avg_us=984.119 total_p99_us=1178.366 total_max_us=1178.366 avg_dominant_segment=[source_emit_to_subscriber_read]
live_chain.runtime_paired_sample count=38 paired=[true] source_emit_to_runtime_start_avg_us=272.171 runtime_on_data_avg_us=207.524 runtime_publish_avg_us=35.314 runtime_start_to_publish_done_avg_us=311.361 source_emit_to_publish_done_avg_us=583.532 publish_done_to_subscriber_read_avg_us=-1.814 runtime_start_to_subscriber_read_avg_us=309.548
live_chain.runtime_paired_commit count=38 paired=[true] runtime_data_total_avg_us=350.171
```

这条 paired 证据把 `source_emit -> subscriber_read` 的 581.7us 均值基本闭合：

```text
source_emit -> runtime_start       272.2us
runtime_start -> publish_done      311.4us
publish_done -> subscriber_read     ~0us
```

`publish_done_to_subscriber_read_avg_us=-1.814` 是跨日志时间戳重建和微秒四舍五入带来的
微小误差，工程上可视为 0。也就是说 subscriber driver 和 publish 后可见性不是主因；
主因是：

1. runtime worker 被唤醒并开始处理之前的等待，约 270-280us。
2. `StreamTableMaterializer::on_data()`，均值约 200-235us，尾部可到 800us 量级。

下一步优化应优先看 fast data queue worker 的唤醒/调度策略，以及
`StreamTableMaterializer::on_data()` 单行写 active segment 的实现。单纯优化
subscriber `read_available()` 或 Python watcher 打印，对这段主延迟帮助有限。

2026-06-13 23:21 继续补 OpenCTP emit done 与 runtime start 的同源配对，并拆
`StreamTableMaterializer::on_data()` 内部。TTS 45 秒实采：

```text
openctp.source_emit_to_emit_done_us count=47 avg_us=256.225 p95_us=346.111 p99_us=526.761
runtime.source_emit_to_runtime_start.avg count=47 avg_us=271.303 p95_us=355.580 p99_us=547.993
stream_table.schema_check_us count=48 avg_us=41.304 p95_us=54.135 p99_us=62.200
stream_table.materialize_us count=48 avg_us=179.787 p95_us=235.667 p99_us=576.618
stream_table.on_data_total_us count=48 avg_us=224.816 p95_us=301.304 p99_us=621.675
stream_table.append_row_span_us count=47 avg_us=136.002 p95_us=182.043 p99_us=542.983
stream_table.segment_append_total_us count=47 avg_us=144.178 p95_us=190.339 p99_us=551.448
live_chain.openctp_runtime_paired_sample count=41 paired=[true] source_emit_to_emit_done_avg_us=249.144 source_emit_to_runtime_start_avg_us=265.072 emit_done_to_runtime_start_avg_us=15.927
live_chain.stream_table_paired_sample count=47 paired=[true] ensure_persist_avg_us=0.572 schema_check_avg_us=42.007 materialize_avg_us=174.905 on_data_total_avg_us=220.258 append_row_span_avg_us=136.002 segment_append_total_avg_us=144.178
```

这修正了上一段的解释：`source_emit -> runtime_start` 不能简单归因成
worker 唤醒等待。OpenCTP 的 `SourceSink::emit(Data)` 返回点与 runtime start
同源配对后，`emit_done -> runtime_start` 平均只有约 16us；大部分
`source_emit -> runtime_start` 时间与 OpenCTP/native source emit 调用本身重叠。
因此主路径应改写为：

```text
source_emit -> native/source sink emit done   ~249us
emit done -> runtime start                     ~16us
runtime on_data                               ~220us
  schema_check                                 ~42us
  materialize                                 ~175us
    append_row_span                           ~136us
runtime publish                                ~39us
publish_done -> subscriber_read                ~0us
```

2026-06-13 23:34 继续拆 `SourceSink::emit` 与 segment-store active append。新增
`runtime.source_sink.*` 和 `segment_store.*` trace 后，TTS 45 秒实采：

```text
runtime.source_sink.emit_lock_wait_us count=39 avg_us=1.577 p95_us=2.545 p99_us=2.555
runtime.source_sink.push_blocking_us count=39 avg_us=2.519 p95_us=4.599 p99_us=7.294
runtime.source_sink.notify_worker_us count=39 avg_us=18.424 p95_us=26.411 p99_us=48.764
runtime.source_sink.emit_total_us count=39 avg_us=67.837 p95_us=88.120 p99_us=116.254
segment_store.state_lock_wait_us count=39 avg_us=0.806 p95_us=1.303 p99_us=1.363
segment_store.inner_append_us count=39 avg_us=180.727 p95_us=247.508 p99_us=729.280
segment_store.broadcast_notify_us count=39 avg_us=2.796 p95_us=3.818 p99_us=4.068
segment_store.append_total_us count=39 avg_us=184.964 p95_us=252.429 p99_us=733.158
stream_table.append_row_span_us count=39 avg_us=213.691 p95_us=275.443 p99_us=758.096
```

结论：

1. runtime 最终 `SourceRuntimeSink::emit` 本身平均约 68us；锁等待和
   `push_blocking` 都是微秒级，`notify_worker` 平均约 18us。
2. `append_row_span` 的大头不是外层锁，也不是 broadcaster notify，而是
   `ActiveSegmentWriter::append_row_span()` 内部列复制与 committed prefix 更新，
   平均约 181us。
3. OpenCTP 侧 `emit_sink_us` 仍明显大于 `runtime.source_sink.emit_total_us`，
   说明它还包含 Python native capsule 边界和 descriptor attach/rebuild 成本。

2026-06-13 23:42 继续拆 native capsule 边界。新增两侧 trace：

```text
native_source.capsule_sink.*   # zippy-openctp-python NativeCapsuleSink.emit_data()
native_source.bridge.*         # zippy-python native_source_bridge.emit_data_segment_impl()
```

TTS 45 秒实采：

```text
native_source.capsule_sink.envelope_us count=45 avg_us=24.166 p95_us=31.021 p99_us=33.185
native_source.capsule_sink.abi_call_us count=45 avg_us=235.204 p95_us=322.493 p99_us=551.166
native_source.capsule_sink.total_us count=45 avg_us=260.070 p95_us=355.878 p99_us=574.791
native_source.bridge.layout_us count=45 avg_us=10.192 p95_us=23.375 p99_us=37.864
native_source.bridge.descriptor_decode_us count=45 avg_us=29.610 p95_us=42.672 p99_us=65.617
native_source.bridge.attach_us count=45 avg_us=56.390 p95_us=66.709 p99_us=208.774
native_source.bridge.sink_emit_us count=45 avg_us=113.461 p95_us=154.068 p99_us=278.489
native_source.bridge.total_us count=45 avg_us=211.287 p95_us=291.463 p99_us=522.290
runtime.source_sink.emit_total_us count=45 avg_us=66.720 p95_us=84.604 p99_us=95.404
segment_store.inner_append_us count=44 avg_us=184.178 p95_us=277.036 p99_us=420.254
stream_table.on_data_total_us count=46 avg_us=303.347 p95_us=501.878 p99_us=524.556
live_chain.runtime_paired_sample count=39 paired=[true] source_emit_to_runtime_start_avg_us=323.474 runtime_on_data_avg_us=318.670 runtime_publish_avg_us=36.693 runtime_start_to_publish_done_avg_us=387.659 source_emit_to_publish_done_avg_us=711.133 publish_done_to_subscriber_read_avg_us=-30.517
```

这轮把 OpenCTP `emit_sink_us` 与 zippy runtime sink 的差额闭合了。LDC 当前的
Python-native OpenCTP 接入路径是：

```text
OpenCTP core
  -> zippy-openctp-python NativeCapsuleSink.emit_data()
     envelope active descriptor             ~24us
     ABI call into zippy-python             ~235us
       LayoutPlan::for_schema               ~10us
       ActiveSegmentDescriptor decode       ~30us
       RowSpanView attach                   ~56us
       SourceRuntimeSink::emit              ~67-113us
```

因此，当前 TTS 替代链路里 `source_emit -> subscriber_read` 的主体已经可以归到两块：

1. native capsule/bridge 边界约 260us。这里有重复的 descriptor envelope 编码、
   layout 重建、descriptor decode 和 active segment attach。
2. stream table materialize/active append 约 300us，其中 segment-store inner append
   约 180us。

subscriber driver 读、Python watcher row 构造/最小 callback、runtime queue
`push_blocking` 都不是主因。后续优化的优先级应是：

1. P1：减少 native capsule 的 per-tick descriptor/layout/attach 重建。理想方向是
   在 capsule ABI 中传递可复用的 descriptor/layout 或更直接的 active span handle，
   目标把 capsule 边界从约 260us 压到 100us 以内。
2. P2：优化 stream table 单行 active append，减少每 tick 多列小块 copy、validity copy
   和 committed prefix 发布成本，目标把 `on_data_total` 从约 300us 压到
   120-180us。
3. P3：再看 `notify_worker` 和 runtime publish 的几十微秒级成本；这两段不是当前
   主要瓶颈。

2026-06-14 00:04 继续拆 `native_source.bridge.sink_emit_us` 和
`segment_store.inner_append_us`。新增：

```text
native_source.bridge.table_view_us
segment_store.active_append.{utf8_fit,layout_lookup,i64_copy,f64_copy,utf8_copy,validity_copy,publish_committed,total}_us
```

TTS 45 秒实采，完整性 gate 通过：

```text
native_source.bridge.table_view_us count=34 avg_us=0.485 p95_us=0.732 p99_us=0.851
native_source.bridge.sink_emit_us count=34 avg_us=116.523 p95_us=145.689 p99_us=147.908
native_source.bridge.total_us count=34 avg_us=226.606 p95_us=331.885 p99_us=388.874

segment_store.active_append.utf8_fit_us count=34 avg_us=36.127 p95_us=62.851 p99_us=70.650
segment_store.active_append.layout_lookup_us count=34 avg_us=5.492 p95_us=8.133 p99_us=8.727
segment_store.active_append.i64_copy_us count=34 avg_us=38.893 p95_us=60.018 p99_us=147.443
segment_store.active_append.f64_copy_us count=34 avg_us=35.593 p95_us=57.934 p99_us=127.164
segment_store.active_append.utf8_copy_us count=34 avg_us=43.987 p95_us=99.086 p99_us=186.798
segment_store.active_append.validity_copy_us count=34 avg_us=31.140 p95_us=59.987 p99_us=64.773
segment_store.active_append.publish_committed_us count=34 avg_us=3.616 p95_us=6.052 p99_us=6.142
segment_store.active_append.total_us count=34 avg_us=204.899 p95_us=347.359 p99_us=578.426

segment_store.state_lock_wait_us count=34 avg_us=1.062 p95_us=1.723 p99_us=2.334
segment_store.inner_append_us count=34 avg_us=252.469 p95_us=410.475 p99_us=621.779
segment_store.broadcast_notify_us count=34 avg_us=2.854 p95_us=4.238 p99_us=5.672
stream_table.on_data_total_us count=35 avg_us=355.694 p95_us=524.360 p99_us=723.174

live_chain.full_chain_paired_sample count=30 paired=[true]
live_chain.full_chain_summary paired=[true] count=30 total_avg_us=1123.665 total_p99_us=1756.038 total_max_us=1756.038 avg_dominant_segment=[source_emit_to_subscriber_read]
```

这轮结论：

1. `SegmentTableView::from_row_span(span)` 几乎不是成本来源，平均只有约 0.5us。
   `native_source.bridge.sink_emit_us` 基本就是最终 `SourceRuntimeSink::emit` 及其
   trace/调度噪声口径，不需要再把 table view 包装列为优化目标。
2. active append 的主体是每 tick 多列小块复制和 validity/utf8 处理：
   `utf8_fit + i64_copy + f64_copy + utf8_copy + validity_copy` 均值合计约 186us。
   其中 `utf8_copy`、`i64_copy`、`f64_copy` 的 p99 分别约 187us、147us、127us，
   是当前 active append 尾部最明显的来源。
3. committed prefix 发布平均约 3.6us，外层锁约 1us，broadcast notify 约 2.9us；
   这些都不是主因。

因此，stream table 侧如果要继续优化，优先方向不是减少 publish/notify，而是减少
单行 active-to-active copy 的固定开销：

1. 避免每列分配 `Vec<u8>` 后 read/write 小块；对单行数值列可走栈上 `[u8; 8]`
   或直接 typed read/write。
2. 对 nullable 单行批量 validity copy 减少每列函数调用和 bit read/write 循环。
3. 对 utf8 列减少重复 offset 读取；当前 `active_row_span_utf8_ranges_fit()` 已经读取
   utf8 起止 offset，随后 `copy_active_utf8_column()` 又重新读取。
4. 如果 schema 固定且同源 active segment，考虑缓存 source/target `ColumnLayout`
   配对，避免每 tick 遍历 schema 做布局查找和 clone。

2026-06-14 00:23 继续拆 OpenCTP source 写 source active segment，以及 subscriber
row delivery 内部。新增：

```text
segment_store.write_rows.{state_lock_wait,begin_row,row_write,finish_row,publish_committed,broadcast_notify,total}_us
subscriber.row_delivery_detail.{filter,row_dict,row_factory,py_callback,total}_us
```

注意：OpenCTP source writer 在 `zippy_openctp` 扩展内，因此必须 rebuild
`zippy-openctp` 后才能看到 `segment_store.write_rows.*`。最终 TTS 45 秒实采：

```text
openctp.write_segment_rows_us count=46 avg_us=142.856 p95_us=223.213 p99_us=786.076
segment_store.write_rows.state_lock_wait_us count=46 avg_us=0.258 p95_us=0.311 p99_us=0.711
segment_store.write_rows.begin_row_us count=46 avg_us=1.107 p95_us=1.613 p99_us=6.303
segment_store.write_rows.row_write_us count=46 avg_us=75.821 p95_us=146.434 p99_us=694.106
segment_store.write_rows.finish_row_us count=46 avg_us=3.162 p95_us=4.098 p99_us=4.719
segment_store.write_rows.publish_committed_us count=46 avg_us=4.075 p95_us=4.779 p99_us=5.060
segment_store.write_rows.broadcast_notify_us count=46 avg_us=2.216 p95_us=3.257 p99_us=3.637
segment_store.write_rows.total_us count=46 avg_us=88.995 p95_us=161.723 p99_us=711.501

subscriber.row_delivery_detail.filter_us count=41 avg_us=11.625 p95_us=14.688 p99_us=30.399
subscriber.row_delivery_detail.row_dict_us count=41 avg_us=44.039 p95_us=83.533 p99_us=104.653
subscriber.row_delivery_detail.row_factory_us count=41 avg_us=25.957 p95_us=41.591 p99_us=50.338
subscriber.row_delivery_detail.py_callback_us count=41 avg_us=110.676 p95_us=150.734 p99_us=165.493
subscriber.row_delivery_detail.total_us count=41 avg_us=205.479 p95_us=285.585 p99_us=326.567
subscriber.row_delivery_total count=41 avg_us=247.581 p95_us=325.953 p99_us=382.994

live_chain.full_chain_paired_sample count=41 paired=[true]
live_chain.full_chain_summary paired=[true] count=41 total_avg_us=1076.919 total_p99_us=2144.041 total_max_us=2144.041 avg_dominant_segment=[source_emit_to_subscriber_read]
```

这轮结论：

1. OpenCTP source 写 source active segment 的主因是 `write_normalized_tick_row()` 内逐列
   写入完整 tick 字段，`row_write_us` 平均约 76us，p99 约 694us。锁、begin/finish、
   committed prefix publish 和 broadcast 都是个位数微秒级。
2. `openctp.write_segment_rows_us` 平均约 143us，而 `segment_store.write_rows.total_us`
   平均约 89us；剩余约 50us 来自 `OpenCtpSegmentIngress::write_rows()` 外层循环、
   active snapshot 更新、descriptor identity 检查和 trace 口径差异。
3. subscriber row delivery 的 `subscriber_read -> Python callback done` 平均约 248us。
   其中 native row dict 构造约 44us，row factory 约 26us，instrument filter 约 12us；
   最大的一段是 Python watcher callback 本体，平均约 111us。该 callback 本体包含
   `_emit_matching_row()` 里的 `extract_matching_row()`、delay 计算、格式化、`print(...,
   flush=True)` 和 summary 更新。
4. watcher 自己输出的 `process_delay_ms` 平均约 33us，只到 callback 内 `observed_ns`
   取样点；`subscriber.row_delivery_detail.py_callback_us` 才是 Python callback 返回前
   的完整耗时，所以两者差值主要就是格式化、打印 flush 和 summary 记录尾部。

至此 TTS 替代链路可以按常态均值归为：

```text
OpenCTP callback -> source_emit             ~160us
source_emit -> subscriber_read              ~670us
  source write_rows                         ~143us
    row_write                               ~76us
  native capsule/bridge                     ~250us
  runtime/source sink                       ~70us
  stream_table on_data                      ~290us
    active append                           ~216us
subscriber_read -> Python callback done     ~248us
  row dict + row factory                    ~70us
  Python watcher callback/print             ~111us
```

这些子段来自不同 trace 事件，存在重叠和采样口径差异，不能简单逐项相加成单 tick
总延迟；判断端到端规模仍以 `live_chain.full_chain_* paired=[true]` 为准。

```bash
cd /home/jiangda/services/local-market-data-center
uv run python scripts/prepare_latency_trace_pm_config.py \
  --output /tmp/zippy.pm.latency-trace.toml

uv run zippy pm validate -f /tmp/zippy.pm.latency-trace.toml

ZIPPY_PM_CONFIG=/tmp/zippy.pm.latency-trace.toml \
  scripts/cron/ldc-open.sh

uv run python scripts/capture_latency_trace.py \
  --instrument au2607 \
  --preflight-only \
  --output /tmp/ldc-au2607-latency-trace.log

uv run python scripts/capture_latency_trace.py \
  --instrument au2607 \
  --duration-sec 30 \
  --wait-ready-sec 300 \
  --min-paired-samples 20 \
  --output /tmp/ldc-au2607-latency-trace.log
```

期望看到：

1. preflight 输出 `preflight_status=ready`。
2. analyzer 输出 `live_chain.evidence`，逐项列出
   `openctp_callback_to_source_emit`、`source_emit_to_subscriber_read`、
   `subscriber_read_to_py_callback_done`、`watcher_source_emit_to_observed` 和
   `openctp_callback_to_py_callback_done` 是否有 aggregate / pairable / full-chain
   证据。若这些行仍是 `present=[false]`，该日志不能作为“延迟路径已拆清楚”的证据。
3. analyzer 输出 `live_chain.status=complete`；这与 `--require-live-chain` 的完整性 gate
   一致，缺任一关键字段都会显示 `status=incomplete missing=[...]`。
4. analyzer 输出 `live_chain.openctp_callback_to_source_emit`、
   `live_chain.source_emit_to_subscriber_read`、
   `live_chain.subscriber_read_to_py_callback_done` 和
   `live_chain.openctp_callback_to_py_callback_done`。
5. analyzer 输出 `live_chain.dominant_segment metric=[avg|p95|p99|max]`，直接指出三段里
   均值、分位数和最坏样本各自最大的段和占比；定位尾部时优先看 `metric=[max]` 和
   `metric=[p99]`。
6. 如果 subscriber trace 的 `rows_read` 是单行，analyzer 输出
   `live_chain.paired_sample`，这是通过 `read_ready_ns` 和 `delay_anchor_ns` 做出的
   单行配对校验。
7. analyzer 输出 `live_chain.full_chain_paired_sample`；只有 OpenCTP 的
   `source_emit_wall_ns` 也能和同一个 source emit 锚点对齐时，这一项才会
   `paired=[true]`。
8. analyzer 输出 `live_chain.full_chain_summary`，这是同一批 full-chain paired
   样本的高层概览，包含样本数、端到端 avg/p99/max、主导段和 watcher/subscriber
   read 差值。
9. analyzer 输出 `live_chain.full_chain_segment`，这是同一批 full-chain paired
   样本的三段和总延迟分位数，定位真实尾部时优先看这里的 `p99_us` 和 `max_us`。
10. analyzer 输出 `live_chain.full_chain_dominant_segment metric=[avg|p95|p99|max]`，
   这是同一批 full-chain paired 样本里三段的自动归因行。
11. analyzer 输出 `live_chain.full_chain_worst_sample metric=[total]`，这是本次日志里
   full-chain 端到端最慢的那个 source emit 锚点、三段构成和 watcher 观测点。
12. analyzer 输出 `live_chain.full_chain_watcher_segment`，这是同一批 full-chain paired
   样本里 watcher observed、watcher process 以及 watcher/subscriber read 差值的聚合。
13. watcher 行上 `delay_anchor=source_emit_ns` 和 `delay_anchor_ns=...`，用于核对该行
   实际锚定的 source emit 时间点。
14. OpenCTP trace 行上 `source_emit_wall_ns` 和 `callback_to_source_emit_us_*`；
   前者应与 watcher 的 `delay_anchor_ns` 对齐，后者是
   `CTP callback -> source_emit` 的段。
14. subscriber trace 行上 `source_emit_to_read_*`，这是
   `source_emit -> subscriber_read` 的段。
15. subscriber trace 行上 `row_delivery_total_us`，这是
   `subscriber_read -> Python callback done` 的段。
16. watcher 行上 `process_delay_ms`，这是 callback 内提取、格式化、打印前的局部段。
17. `scripts/analyze_latency_trace.py` 汇总出 `openctp.*`、`subscriber.*`、
   `watcher.source_emit_ns.*` 三组分位数。
18. analyzer 输出 `diagnosis.source_emit_to_read_scope`、`diagnosis.source_write`、
   `diagnosis.native_bridge`、`diagnosis.runtime_stream_table` 和
   `diagnosis.subscriber_delivery`，把当前已拆开的热点子段按均值并列出来。注意这些
   字段来自不同 trace scope，只用于排序优化目标，不能简单相加成端到端耗时。

`live_chain` 的含义：

```text
live_chain.evidence                             # 每个切点是否有 aggregate/pairable/full-chain 证据
live_chain.openctp_callback_to_source_emit      # CTP callback -> source_emit
live_chain.source_emit_to_subscriber_read       # source_emit -> subscriber_read
live_chain.subscriber_read_to_py_callback_done  # subscriber_read -> Python callback done
live_chain.openctp_callback_to_py_callback_done # 以上三段求和
live_chain.dominant_segment metric=[avg]        # 三段均值里的最大来源和占比
live_chain.dominant_segment metric=[p95|p99]    # 三段尾部里的最大来源和占比
live_chain.dominant_segment metric=[max]        # native max / delivery max 里的最坏样本来源
live_chain.paired_sample                        # 单行 subscriber/watcher 配对校验
live_chain.full_chain_paired_sample             # OpenCTP + subscriber/watcher 四段配对校验
live_chain.full_chain_summary                   # full-chain paired 样本高层概览
live_chain.full_chain_segment                   # full-chain paired 样本的分段分位数
live_chain.full_chain_dominant_segment          # full-chain paired 样本的三段主导来源
live_chain.full_chain_worst_sample              # 端到端最慢 full-chain tick 的三段拆解
live_chain.full_chain_watcher_segment           # full-chain paired 样本的 watcher 口径聚合
```

`diagnosis` 的含义：

```text
diagnosis.source_emit_to_read_scope # source_emit -> subscriber_read 大段内的重叠子段视图
diagnosis.source_write          # OpenCTP source 写 source active segment 的主因排序
diagnosis.native_bridge         # zippy-openctp capsule -> zippy native source bridge 成本
diagnosis.runtime_stream_table  # runtime worker + stream_table + target active append 成本
diagnosis.subscriber_delivery   # subscriber row delivery + watcher callback/print 成本
```

最新 TTS trace 的自动诊断输出：

```text
diagnosis.source_emit_to_read_scope source_emit_to_subscriber_read_avg_us=669.330 source_write_avg_us=142.856 native_capsule_avg_us=250.398 runtime_on_data_avg_us=306.916 stream_table_on_data_avg_us=289.227 active_append_avg_us=150.648 subscriber_poll_to_event_avg_us=16.451 dominant_measured_child=[runtime_on_data] scope=[overlapping_subsegments]
diagnosis.source_write openctp_write_segment_rows_avg_us=142.856 segment_store_write_rows_total_avg_us=88.995 row_write_avg_us=75.821 outer_overhead_avg_us=53.861 dominant=[row_write]
diagnosis.native_bridge capsule_total_avg_us=250.398 bridge_total_avg_us=199.554 descriptor_decode_avg_us=33.196 attach_avg_us=52.972 table_view_avg_us=0.494 sink_emit_avg_us=101.255 dominant=[sink_emit]
diagnosis.runtime_stream_table source_sink_emit_avg_us=72.187 runtime_on_data_avg_us=306.916 stream_table_on_data_avg_us=289.227 active_append_avg_us=150.648 active_append_copy_sum_avg_us=133.355 dominant=[runtime_on_data]
diagnosis.subscriber_delivery row_delivery_total_avg_us=247.581 row_delivery_detail_total_avg_us=205.479 row_construct_avg_us=69.995 py_callback_avg_us=110.676 watcher_process_avg_us=33.244 watcher_tail_after_observed_avg_us=77.432 dominant=[py_callback]
```

`diagnosis.source_emit_to_read_scope` 是为了回答
`source_emit -> subscriber_read` 的 669us 均值里“看起来有哪些大块”。这一行标记
`scope=[overlapping_subsegments]`，表示 `runtime_on_data`、`stream_table_on_data` 和
`active_append` 是嵌套/重叠关系，不能加总；它只用于说明当前最大可观测子块是
`runtime_on_data`，其中又主要落在 `stream_table_on_data/active_append`。

注意：`metric=[p95|p99]` 是对 analyzer 收到的 trace 样本做分位数。OpenCTP 和
subscriber read 的原始日志包含 batch/span 级 `avg` 和 `max`，因此现场排最坏尖峰时，
`metric=[max]` 比 `metric=[p99]` 更接近“本次日志里看到的最坏段”。
这些 `dominant_segment` 行会标记 `paired=[false]`，含义是不同段的 p99/max 不保证来自
同一个 tick，只能用于定位“哪一段贡献最大”，不能当成逐 tick 配对总延迟。
`live_chain.full_chain_segment` 会标记 `paired=[true]`，只使用已经通过
`source_emit_wall_ns + read_ready_ns + delay_anchor_ns` 对齐的 full-chain 样本，因此更适合
判断 0.3-0.5ms 尾部到底来自 callback 前半段、subscriber read，还是 Python callback
尾部。
`live_chain.full_chain_summary` 是最先看的概览行：`total_avg_us/p99_us/max_us` 表示
full-chain 端到端规模，`avg/p99/max_dominant_segment` 指出三段主导来源，
`watcher_observed_minus_subscriber_read_avg_us` 解释 watcher observed 口径相对 native
subscriber read 平均多出来多少。
`live_chain.full_chain_dominant_segment` 同样标记 `paired=[true]`，只比较
`openctp_callback_to_source_emit`、`source_emit_to_subscriber_read` 和
`subscriber_read_to_py_callback_done` 三段，不把总延迟行纳入比较；现场排尾部时直接看
`metric=[p99]` 和 `metric=[max]`。
`live_chain.full_chain_worst_sample` 标记本次采集中
`openctp_callback_to_py_callback_done` 最大的 full-chain paired 样本，并输出
`source_emit_ns`、三段耗时、`watcher_source_emit_to_observed_us`、
`watcher_observed_minus_subscriber_read_us`、`watcher_process_us`、
`watcher_observed_to_py_callback_done_delta_us`、`total_us` 和该 tick 内的
`dominant_segment`。如果只想看“最差那一笔到底慢在哪里”，优先读这一行。
`live_chain.full_chain_watcher_segment` 则把这些 watcher 相关字段做成 paired=true
聚合分位数；其中 `watcher_observed_minus_subscriber_read` 用来解释 `watch_ticks`
观测口径比 native subscriber read 多出来多少。

`live_chain.watcher_source_emit_to_observed` 和
`live_chain.watcher_observed_to_callback_done_delta` 是 watcher 观测点的交叉校验；
它们用于解释 `watch_ticks.py` 的 row 提取/格式化/打印尾部，但不是逐 tick 配对样本。
`--require-live-chain` 会要求 watcher 行包含 `delay_anchor_ns`；缺该字段说明日志来自旧
watcher 或非完整采集，不能作为最终闭环证据。
`live_chain.paired_sample` 只在 `rows_read` 为单行时输出。它通过
`source_emit_ns = read_ready_ns - source_emit_to_read_avg_us` 反推 subscriber 读到的
行锚点，再与 watcher 的 `delay_anchor_ns` 匹配，覆盖
`source_emit -> subscriber_read -> Python callback done / watcher observed`。OpenCTP
native trace 同时输出 `source_emit_wall_ns`；当它与 watcher `delay_anchor_ns` 对齐时，
`paired_sample` 会额外输出 `openctp_callback_to_source_emit_avg_us` 和
`openctp_callback_to_py_callback_done_avg_us`，把前半段也归到同一个 source emit 锚点上。
`live_chain.full_chain_paired_sample` 是更严格的验收项：它只统计 OpenCTP
`source_emit_wall_ns`、subscriber `read_ready_ns` 反推锚点、watcher `delay_anchor_ns`
三者都对齐的样本，覆盖
`CTP callback -> source_emit -> subscriber_read -> Python callback done`。
如果不能配对，analyzer 会输出 `live_chain.paired_sample count=0 paired=[false]` 和原因，
常见原因是 `no_single_row_subscriber_read`（subscriber 一次读到多行）或
`no_matching_watcher_anchor`（watcher 的 `delay_anchor_ns` 与 subscriber 反推锚点未对齐）。
`capture_latency_trace.py` 默认会按 `full_chain_paired_sample` 验收；如果只有
subscriber/watcher 下半段配上、OpenCTP anchor 没配上，也会返回 2。
如果只想保留非配对分段统计，可显式加 `--allow-unpaired`。
默认要求至少 20 条 full-chain paired 样本，避免只凭一两条 tick 判断尾部来源；
可用 `--min-paired-samples N` 显式覆盖这个门槛。

当前 live preflight 状态：

```text
default_pm_config:
  capture_readiness ready=[false] live_writer=[false] writer_env_trace=[unknown] writer_internal_trace=[unknown] pm_config_trace=[disabled] pm_config_internal_trace=[disabled] reason=[no_live_writer]
  capture_next_action action=[prepare_trace_pm_config] reason=[pm_config_trace_not_enabled]

isolated_trace_pm_config:
  path=[/tmp/zippy.pm.latency-trace.toml]
  validate=[valid local-market-data-center tasks=6]
  writer_env_config=[OPENCTP_LATENCY_TRACE,ZIPPY_RUNTIME_LATENCY_TRACE,ZIPPY_STREAM_TABLE_LATENCY_TRACE]
  master_env_config=[ZIPPY_RUNTIME_LATENCY_TRACE,ZIPPY_STREAM_TABLE_LATENCY_TRACE]
  writer_pm_config_trace openctp_latency_trace=[enabled] pm_internal_trace=[enabled] zippy_runtime_latency_trace=[enabled] zippy_stream_table_latency_trace=[enabled] value=[1]
  capture_readiness ready=[false] live_writer=[false] writer_env_trace=[unknown] writer_internal_trace=[unknown] pm_config_trace=[enabled] pm_config_internal_trace=[enabled] reason=[no_live_writer]
  capture_next_action action=[wait_for_writer] reason=[no_live_writer]
  writer_schedule next_start=[2026-06-15T08:30:00+08:00]
```

这说明当前不是 analyzer 或 trace 字段缺失，而是生产 `ldc_ctp_ticks` writer/master
不在线；隔离 trace 配置已经可用，开盘后应直接用
`--pm-config /tmp/zippy.pm.latency-trace.toml` 的 capture 命令等待 writer 并采样。

注意：`source_emit_ns` 在 `zippy-openctp` 当前实现里是在
`handle_normalized_row_batch()` 中、写 active segment 前统一采样并写入 row。严格说，
live `source_emit_to_read` 覆盖的是：

```text
normalized row timestamp -> segment write -> source emit/wakeup -> subscriber read
```

而不是 CTP native callback 到 source 线程的整段。更早的部分需要同时看
`OPENCTP_LATENCY_TRACE=1` 输出里的 `callback_to_source_recv_*`、
`driver_queue_to_source_recv_*`、`normalize_write_us` 和 `emit_us`。

如果需要完全不碰 LDC master/table/subscriber，也可以用 `zippy-openctp` 的独立
probe 直连 CTP front 做前半段对照：

```bash
cd /home/jiangda/develop/zippy-openctp
cargo run -p zippy-openctp-core --example internal_latency_probe -- \
  --front '<front>' \
  --broker-id '<broker-id>' \
  --user-id '<user-id>' \
  --password '<password>' \
  --instruments 'au2607' \
  --duration-sec 30 \
  --flow-path /tmp/openctp-latency-probe-md
```

这个 probe 只验证 OpenCTP native callback、driver queue、source normalize/write
和 source emit，不经过 LDC 的 zippy master、active segment subscriber 或
`watch_ticks.py` 打印链路。它适合回答“前半段是否已经有长尾”，但不能替代完整
LDC watcher 端到端复测。

当前 `segment_ingress` 的旧类型断言已修正，`cargo test -p zippy-openctp-core`
全量通过；因此 OpenCTP core 的 trace 字段和 segment ingress 相关行为已经有
当前测试覆盖。live 前半段仍以 `internal_latency_probe` 或 LDC 交易时段 trace
的实测输出为准。

为了避免人工读日志出错，LDC 已新增纯文本解析工具：

```bash
uv run python scripts/analyze_latency_trace.py \
  --require-live-chain \
  --min-paired-samples 20 \
  <trace-log>
```

它会把 native OpenCTP trace、zippy subscriber trace、watcher 输出统一换算成微秒，
并输出 count/min/avg/p50/p95/p99/max。`--require-live-chain` 会检查
`openctp.*`、`subscriber.*`、`watcher.source_emit_ns.*` 三组关键段是否齐全；
缺任何一段时返回非零退出码并在 stderr 输出缺失 segment 名称。
`--min-paired-samples` 会检查 `live_chain.full_chain_paired_sample` 的四段单 tick
配对数量；不足时返回非零退出码并输出
`full_chain_paired_sample_count_below_min`。
