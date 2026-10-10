# 阶段 1.5：checkpoint 与 live publication 竞态修复及公开路径复测

实际运行与审查日期：2026-10-10。文件名保留任务规格中的 2026-10-08。

本阶段以确定性交错测试证实并修复了 checkpoint 与 live publication 的 `SEQUENCE_GAP`
竞态。append、frame、key-value 三种表在修复前均产生同一偏移错误，修复后全部通过。
生产修改集中在 `persist_checkpoint_prefix` 的同步范围；Actor 校验、公开 API、wire 协议、
持久化格式及错误码/错误消息保持原有契约。当前分支为 `feature/public-path-latency-bench`，
没有切换分支、commit 或 push。

本次四格各收齐 20,000 个计量样本，浸泡收齐 300,000 个，CI 短版收齐 3,000 个；全部
完整性通过、零 SEQUENCE_GAP、所有子进程和服务端正常退出。三条基准命令仍因原有延迟
门限退出 1：CI p99 达标但 p50 超限，故保留 continue-on-error。完成性、完整性与延迟
门限分别报告，不将性能门限失败解释为竞态复发。

## 测量口径与 A/B/C/D 目标

| 层级 | 路径与时间区间 | p50 | p99 | p99.9 | 本报告适用性 |
| --- | --- | ---: | ---: | ---: | --- |
| A | 本机 Rust SHM；Memory ack → 订阅可见 | ≤20µs | ≤100µs | ≤500µs | 公开路径尚未实现；不判定 |
| B | 本机 Python；写入发起 → 用户回调执行 | ≤50µs | ≤300µs | ≤1ms | open_log 交付仅作差距参考 |
| C | Flight loopback/远端；写入发起 → 订阅收到 | ≤1ms | ≤3ms | ≤10ms | 本次只判定 loopback visible |
| D | 持久确认；写入发起 → WAL 回执 | ≤2ms | ≤5ms | — | 仅判定 WAL 档 ack |

公开客户端使用 `Client.get_writer(batch_size=1, flush_interval_ms=None).write(row)`。
一次写调用发送一行 Arrow RecordBatch 并等待配置的 durability 回执；SDK 复用 DoPut stream，
不是每行新建一个 RPC，见 `clients/python/src/zippy_client/transports/flight.py:1444`。

ack 从调用公开 `write` 前的 `time.monotonic_ns()` 算起，包含 Python 行转换、Flight 编解码、
native ingest 和确认返回。Memory ack 表示内存发布完成；WAL ack 还要求 WAL `sync_data()`。
两者分开统计，Memory ack 不使用 D 层持久确认结论。

visible 的终点是独立订阅进程中 `Client.open_log(batch_rows=1)` 的 `next(reader)` 返回处；
采样在 `event.table.to_pylist()` 前进行。它不包含逐行业务字段转换及用户 callback 执行，见
`scripts/bench_public_e2e.py:345`。`Client.subscribe` 还经过 Row 包装与回调调用，见
`clients/python/src/zippy_client/flight_subscriber.py:359`，所以本报告不宣称完成严格 B 层测量。

写入、订阅、背景写入分别运行于独立 Python 子进程；服务端只监听 `127.0.0.1`。
每格独立临时数据目录，使用 Linux 系统范围可比较的单调时钟，不以 UTC 墙钟相减求延迟。
前景按绝对单调时钟截止时刻排程，目标 1,000 行/秒；背景向另一张 append 表批量写入，
目标为 0 或 100,000 行/秒，避免同表多个 producer 引入 epoch fencing。

预热在“至少运行 2 秒”且“至少剔除 1,000 个样本”同时满足后结束，取较晚边界。
四格每格至少 20,000 个计量样本；浸泡为 memory / 背景 100,000，至少 300,000 个。
CI 短版为 memory / 背景 0、3,000 个计量样本。
完整性检查覆盖预热与正式样本的全部序列，并要求最终 ordered end marker。
缺口、重复、乱序、意外序号或样本不足均不能算完成。

百分位使用 nearest-rank，排序后取一基下标 `ceil(p × N)`；样本少于 10,000 时 p99.9 为
`null`，不插值补算。实际前景/背景吞吐与目标速率分别记录。同步 ack 导致写入落后时，
visible 仍从实际发起时间起算，不含计划发起到实际发起之间的排队时间；该部分另存
`schedule_lag`。因此 WAL 未达到 1,000 行/秒时，不能把这格解释为实际 1,000 行/秒负载。

## 环境与构建身份

| 项目 | 本次实际值 |
| --- | --- |
| 分支 / git SHA / dirty | `feature/public-path-latency-bench` / `7c6b6a9784336827c7239a872e50bccfda15d501` / `true` |
| 四格开始 / 结束 UTC | `2026-10-10T10:06:29.032039+00:00` / `2026-10-10T10:08:47.385422+00:00` |
| 浸泡开始 / 结束 UTC | `2026-10-10T10:10:08.246199+00:00` / `2026-10-10T10:15:13.020318+00:00` |
| OS / CPU | `5.15.153.1-microsoft-standard-WSL2` / AMD Ryzen 9 9950X 16-Core Processor；32 逻辑核，affinity 0–31 |
| Python / PyArrow / zippy / zippy-client | 3.11.14 / 23.0.1 / 0.5.0 / 0.1.0 |
| 数据 / server | 每格 `/tmp/zippy-public-e2e-*`；`target/release/zippy-server` |
| 构建 | server release optimized：25.07s；maturin release optimized：23.82s，均退出 0 |

脚本 SHA-256：`bec0a8a5b9a8abf930f5a423eb0d48eb4b37d59edc77aa3dbe89f7b235d72015`。
服务端 SHA-256：`19c886769cb064aed6e848ebe38669e5ee4e3269e58cdd14dfc921d15f8fad56`。
Python 扩展 SHA-256：`e557c394bb4b2bd3750db959f71d916bc0dbc56da1c37cc319b7f3ff3a7442bb`。

本次正式性能数据在构建和验证任务结束后测量，没有主动并行编译、测试或其他基准负载。
JSON metadata 记录主机、版本、git SHA/dirty、脚本及构建产物 hash；release 身份由实际
构建命令证明。WSL2、宿主机调度和文件系统可能影响尾延迟，本机结果不构成裸机或远端 SLA。

## 执行命令与证据组织

工作目录 `/home/jiangda/develop/zippy`；本次实际执行：

```bash
PYO3_PYTHON=$PWD/.venv/bin/python cargo build --release -p zippy-server --bin zippy-server
UV_CACHE_DIR=/tmp/zippy-uv-cache PYO3_PYTHON=$PWD/.venv/bin/python \
  uv run --no-sync maturin develop --release
```

`baseline` 实际退出 1，完整性与完成性全部通过；退出状态由原有延迟门限决定。

```bash
UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/bench_public_e2e.py \
  --durability memory wal \
  --background-rate 0 100000 \
  --rate 1000 \
  --samples 20000 \
  --warmup-seconds 2 \
  --warmup-samples 1000 \
  --background-batch-rows 1000 \
  --timeout-seconds 600 \
  --report-json docs/perf_reports/evidence/2026-10-08-public-path-e2e/baseline.json \
  --raw-samples-json /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/post-fix-baseline.json
```

`soak` 实际退出 1，完整性与完成性全部通过；退出状态由原有延迟门限决定。

```bash
UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/bench_public_e2e.py \
  --durability memory \
  --background-rate 100000 \
  --rate 1000 \
  --samples 300000 \
  --warmup-seconds 2 \
  --warmup-samples 1000 \
  --background-batch-rows 1000 \
  --timeout-seconds 900 \
  --report-json docs/perf_reports/evidence/2026-10-08-public-path-e2e/soak.json \
  --raw-samples-json /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/post-fix-soak.json
```

`ci-smoke` 实际退出 1，完整性与完成性全部通过；退出状态由原有延迟门限决定。

```bash
UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/bench_public_e2e.py \
  --samples 3000 \
  --durability memory \
  --max-visible-p99-micros 3000 \
  --report-json docs/perf_reports/evidence/2026-10-08-public-path-e2e/ci-smoke.json \
  --raw-samples-json /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/post-fix-ci-smoke.json
```

所有 Python 命令使用 `uv run --no-sync`；Rust 构建与检查设置
`PYO3_PYTHON=$PWD/.venv/bin/python`。若设置 `UV_CACHE_DIR=/tmp/zippy-uv-cache`，
它只改变缓存位置，不同步或替换依赖。需要监听 loopback 的任务使用允许本机 socket 的环境。

仓库证据目录为 `docs/perf_reports/evidence/2026-10-08-public-path-e2e/`。
本次 `baseline.json`、`soak.json`、`ci-smoke.json` 分别保存四格、浸泡与 CI 短版汇总。
`deterministic-before.log`、`deterministic-after.log` 保存修复前后确定性回归输出。
构建和验收日志、`validation.json` 保存实际命令及结果，不用历史通过记录替代本次验证。

`--report-json` 默认只保存统计、完整性、配置、metadata、进程结果、错误摘要与 traceback，
不包含逐样本时间戳或完整服务端日志。`workers.recorded_samples` 保留每类原始数组长度，
`diagnostic_summary` 保留有界错误行与截断计数，见 `scripts/bench_public_e2e.py:193`。
新增 `--raw-samples-json PATH` 可额外保存完整原始证据，见
`scripts/bench_public_e2e.py:236`；两种输出不得使用同一路径。

修复前大文件原件已移至仓库外
`/tmp/zippy-perf-raw/2026-10-08-public-path-e2e/`：

- 原 `baseline.json` 对应仓库内 `pre-fix-baseline.json` 汇总。
- 原 `ci-smoke.json` 对应仓库内 `pre-fix-ci-smoke.json` 汇总。
- 原 `initial-failed-run.json` 对应仓库内同名汇总。

本次原始输出也位于该仓库外目录，使用本次命令中明确指定的文件名，避免覆盖历史原件。
仓库内每个证据文件均限制为 ≤300KB；`SHA256SUMS` 仅覆盖仓库内文件。
从仓库根目录运行以下命令核对，不把 `/tmp` 原始文件列入仓库校验清单：

```bash
sha256sum -c docs/perf_reports/evidence/2026-10-08-public-path-e2e/SHA256SUMS
```

## 根因与修复

### 确定性交错：从源码推断变为实证

测试使用真实 SQLite Catalog、Parquet persistence controller、TableActor 和 materializer。
先写 offset 0 并准备 checkpoint；测试钩子恰好在 Actor 确认持久水位后、物化前缀回收前
暂停 checkpoint，再发起 offset 1 的写入。通过 channel 明确推进各步骤，不依赖 sleep 或
反复运行碰触调度窗口，见 `crates/zippy-ingest/src/materializer/tests.rs:249`。

旧实现此时没有持有 state 锁，测试先完成竞争写入，再允许 checkpoint 回收。
修复实现持有 state 锁，竞争写入必须等回收完成后才能构造 publication；测试用 `try_lock`
检查这个同步事实并释放 channel 屏障，不用超时推测“写入大概被阻塞”。
三类测试共享同一交错过程，见 `crates/zippy-ingest/src/materializer/tests.rs:347`。

修复前实际输出摘录如下，完整日志为 `deterministic-before.log`：

```text
interleaved write must succeed: SEQUENCE_GAP: live segment offsets are not contiguous
expected=[1] actual=[0]; checkpoint result: Err(...
"native table materialized row count mismatch expected=[0] actual=[1]" ...)
test result: FAILED. 0 passed; 3 failed
```

append、frame、key-value 均复现 `expected=[1] actual=[0]`，同时产生物理/逻辑行数差异。
这直接证实“Actor 持久水位已推进、materializer 仍按旧前缀发布”这一交错是充分触发条件；
它不声称每个历史生产失败均记录了完整线程轨迹。

相同三个测试在修复后的输出见 `deterministic-after.log`：

```text
test materializer::tests::checkpoint_publication_interleaving_append ... ok
test materializer::tests::checkpoint_publication_interleaving_key_value ... ok
test materializer::tests::checkpoint_publication_interleaving_frame ... ok
test result: ok. 3 passed; 0 failed
```

最终断言还核对 Actor 水位为 0、唯一 live publication 为 `[1,1]`、本地 `retained_start=1`、
`live_rows=1`，以及物理行数与 publication 构造一致。全部测试钩子和额外字段均受
`#[cfg(test)]` 保护，release 构建没有该钩子的状态分配或执行成本。

### 原因、同步范围和所有正常交错的安全性

Actor 在持久化完成后推进 `current_snapshot`，再返回 persist 响应，见
`crates/zippy-ingest/src/actor.rs:2068`。旧实现收到响应后才另起 blocking task 获取
materializer state 锁并回收前缀，允许新 apply 在两者之间获得锁。
publication 从 `retained_start` 开始构造，见 `crates/zippy-ingest/src/materializer.rs:827`；
Actor 则要求从当前持久水位后一行开始，见 `crates/zippy-ingest/src/actor.rs:1292`。
包含旧前缀的 publication 因而必然违反连续性契约。

修复保持 candidate 构造在外侧；构造完 request 后释放 snapshot，在一个 `spawn_blocking`
闭包内先取得现有 state 锁，再等待 Actor persist，随后更新本地水位、回收前缀并发布，
直到这些步骤退出才释放锁，见 `crates/zippy-ingest/src/materializer.rs:612`。
Actor 的 `SEQUENCE_GAP` 和行数检查均未放宽，也没有引入接受重叠后静默裁剪的新行为。

所有正常交错分为三类：

1. apply 先取得锁：先完成写入、publication 和逻辑计数更新。checkpoint 在取得锁前不能
   发出 persist，因此此次 publication 使用旧前缀与旧 Actor 水位匹配。
2. checkpoint 先取得锁：Actor 水位推进后，新 apply 仍无法构造 publication；只有本地
   回收和发布完成后才释放锁。新 apply 观察到的起点已经对应新持久水位。
3. candidate 构造期间发生 apply：snapshot 固定已捕获的 checkpoint 前缀，新写入只扩展
   后缀；进入 persist 的实际状态转换仍须取得同一把锁，所以归入前两类。

同步锁与 persist 等待均在 blocking worker 上，async executor 可以继续驱动 Actor。
Actor 不获取 materializer state 锁，因此没有新增反向锁依赖。writer fencing 与 checkpoint
已经由 native `open_gate` 串行化，见 `crates/zippy-ingest/src/native.rs:1268`、
`crates/zippy-ingest/src/native.rs:2353`，无需扩大修改到 `fence_writer`。
代价是该表的 apply 可能等待 checkpoint 持久化完成；其延迟影响由本次公开路径数据体现。

### 错误与取消边界

candidate 构造失败或在提交 blocking task 前取消，尚未发出 persist；不会推进持久水位。
blocking 闭包开始后，取消外层等待不会中断闭包或提前释放 state 锁；它继续执行 persist、
回收和 publication。公开 checkpoint 的上层 task 还保持 operation permit 与 `open_gate`，
见 `crates/zippy-ingest/src/native.rs:699`，所以调用者取消也不会让 writer fencing 闯入。

| 情形 | 保证与限制 |
| --- | --- |
| persist 准入或明确拒绝 | 本地水位、物化前缀不更新；Actor rejection 也不推进 snapshot。 |
| 持久成功后回收自身报错 | 仍可能留下 Actor 新水位和未完成或部分完成的本地回收；沿用终止/恢复语义。 |
| 回收完成后 publication 背压 | 本地前缀已正确；Actor 旧 live 集合在 snapshot 中按新持久水位过滤，可再次发布。 |
| Actor 终止、响应丢失、panic | 结果仍可能不确定，不能据错误推断 durable commit 从未发生。 |

因此修复保证是“正常并发和调用者取消下，对 publication 构造原子可见”，不是跨 Actor、
Catalog、segment 的故障补偿事务。现有 checkpoint 错误处理将非重试错误置为表终止，见
`crates/zippy-ingest/src/native.rs:2360`；本次没有扩大为故障恢复协议重构。

另有尚未实测的活性边界：checkpoint blocking worker 等待 Actor persist，而 persistence
controller 也需要 `spawn_blocking`，见 `crates/zippy-storage/src/persist.rs:1113`。
后台 checkpoint 使用独立 runtime；显式 checkpoint 若与 persistence 共用容量为 1 或已
饱和的 blocking 池，静态上存在自等待可能。这不改变 offset 互斥证明，但意味着不能承诺
任意 executor 容量下都有进展。消除该限制需要改异步 admission 或隔离 executor，未随本次重构。

### 同类水位/回收模式检查

| 范围 | 检查结果 | 关键引用 |
| --- | --- | --- |
| KV producer version | publication 成功后同锁更新版本与逻辑行数；与 append/frame 共用本次修复。 | `crates/zippy-ingest/src/materializer.rs:493` |
| KV consumer state | `&mut self` 同步处理完整 mutation 后推进 sequence/version，无异步窗口；错误使 state 失效。 | `crates/zippy-engines/src/key_value_state.rs:230`、`crates/zippy-engines/src/key_value_state.rs:145` |
| WAL checkpoint/reclaim | 先完整持久化，再同一 blocking closure 内提交 checkpoint 并删冻结边界以内 WAL；旧文件暂留是保守方向。 | `crates/zippy-ingest/src/native.rs:2509`、`crates/zippy-ingest/src/wal.rs:1502` |
| WAL 恢复 | checkpoint 覆盖的残留文件跳过；已有 crash-before-unlink 和幂等回收测试。 | `crates/zippy-ingest/src/wal.rs:669`、`crates/zippy-ingest/src/wal.rs:1881` |
| Receipt archive/cache | 先持久 archive，再推进归档水位及清内存缓存；窗口只多保留缓存。 | `crates/zippy-ingest/src/native.rs:2465`、`crates/zippy-ingest/src/native.rs:2480` |
| Consumer offset | checkpoint → Catalog commit → 本地 committed；实例串行化，Catalog 事务 CAS 且不许越过持久水位。 | `crates/zippy-server/src/consumer_log.rs:416`、`crates/zippy-catalog/src/consumer_domain.rs:75` |
| Writer fencing | Actor epoch 与本地 epoch 分步更新，但已有 `open_gate`/`transitioning` 排除并发写入和 checkpoint。 | `crates/zippy-ingest/src/native.rs:1336`、`crates/zippy-ingest/src/native.rs:2353` |

未发现上述其他路径存在需要随本次一起修复的同根因窗口。
底层 SegmentStore lease 可以阻止物理前缀释放，但 native 查询 span 使用独立 attach 的固定
行范围，不使用该 lease；若未来改变读者 pin 机制，需要重新核对这个契约。

### 独立发现：publication 失败后的物理行污染

新增第 4 个测试在首行成功后关闭 Actor admission，再写入第 2 行，确定性验证公开发布被
拒绝后物理 segment 为 2 行、本地 `live_rows` 仍为 1，原 snapshot 仍只包含首行，见
`crates/zippy-ingest/src/materializer/tests.rs:362`。测试确认的错误为：

```text
native table materialized row count mismatch expected=[1] actual=[2]
```

这证明修复 checkpoint 竞态后，独立的 apply 失败原子性缺口仍存在：`on_data` 先完成物理
写入，publication 成功后才更新逻辑计数，见 `crates/zippy-ingest/src/materializer.rs:462`、
`crates/zippy-ingest/src/materializer.rs:493`。Actor admission 关闭、邮箱/全局预算耗尽、
descriptor 校验失败或响应丢失都可能触发；上层仍将其设为终止错误，禁止盲目重试原批次。

Actor 仅在全部校验成功后替换 live 集合，明确拒绝不会部分安装查询范围，见
`crates/zippy-ingest/src/actor.rs:1455`。但响应丢失不能证明未安装，不能统一截断物理后缀。
底层已有 active payload snapshot/restore，只服务于单次 rewrite，见
`crates/zippy-segment-store/src/builder.rs:395`；不覆盖成功写入后的 publication 失败，
也不覆盖跨多段 rollover。全面补偿需处理多段、UTF8 游标、既有读者和 WAL 不确定性，
超出本次局部修复范围，因此明确记录为未完成项，没有把它包装为已具备失败原子性。

## 修复后四格基线、浸泡与 CI 短版

四格各 20,000、浸泡 300,000、CI 3,000 个计量样本均收齐；包括预热的全部序列及 end marker 均验证通过。所有子进程/服务端退出 0；全部原始日志与错误字段未出现 SEQUENCE_GAP。

**ack 延迟（µs）**

| 运行 / durability / 背景目标行/秒 | 计量样本数 | p50 | p99 | p99.9 | max | 完整性 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 基线 / memory / 0 | 20,000 | 793.202 | 1,333.606 | 9,496.564 | 15,021.962 | 通过 |
| 基线 / memory / 100,000 | 20,000 | 764.595 | 1,096.021 | 9,520.726 | 17,327.426 | 通过 |
| 基线 / wal / 0 | 20,000 | 2,069.097 | 3,257.505 | 10,303.669 | 13,842.603 | 通过 |
| 基线 / wal / 100,000 | 20,000 | 2,068.292 | 3,529.735 | 10,695.380 | 14,637.698 | 通过 |
| 浸泡 / memory / 100,000 | 300,000 | 812.747 | 1,482.741 | 18,246.198 | 41,827.315 | 通过 |
| CI / memory / 0 | 3,000 | 735.200 | 1,061.586 | null | 9,782.816 | 通过 |

**visible 延迟（µs）**

| 运行 / durability / 背景目标行/秒 | 计量样本数 | p50 | p99 | p99.9 | max | 完整性 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 基线 / memory / 0 | 20,000 | 1,291.368 | 2,039.929 | 10,156.880 | 15,663.592 | 通过 |
| 基线 / memory / 100,000 | 20,000 | 1,266.270 | 1,792.898 | 10,379.701 | 18,161.960 | 通过 |
| 基线 / wal / 0 | 20,000 | 1,380.531 | 2,390.524 | 9,404.951 | 13,836.502 | 通过 |
| 基线 / wal / 100,000 | 20,000 | 1,366.840 | 2,428.947 | 10,070.166 | 14,048.511 | 通过 |
| 浸泡 / memory / 100,000 | 300,000 | 2,842.847 | 5,693.982 | 20,766.074 | 44,185.003 | 通过 |
| CI / memory / 0 | 3,000 | 1,142.435 | 1,576.935 | null | 10,363.279 | 通过 |

| 运行 | 已确认 / 已接收行（含预热） | 缺口 / 重复 / 乱序 | 实际前景行/秒 | 实际背景行/秒 |
| --- | ---: | --- | ---: | ---: |
| 基线 / memory / 0 | 22,000 / 22,000 | 0 / 0 / 0 | 1,000.01 | 0.00 |
| 基线 / memory / 100,000 | 22,000 / 22,000 | 0 / 0 / 0 | 1,000.01 | 99,999.29 |
| 基线 / wal / 0 | 21,000 / 21,000 | 0 / 0 / 0 | 464.85 | 0.00 |
| 基线 / wal / 100,000 | 21,000 / 21,000 | 0 / 0 / 0 | 458.64 | 99,999.65 |
| 浸泡 / memory / 100,000 | 302,000 / 302,000 | 0 / 0 / 0 | 999.87 | 99,999.96 |
| CI / memory / 0 | 5,000 / 5,000 | 0 / 0 / 0 | 1,000.09 | 0.00 |

**保持原门限的失败项：**

- 基线 / memory / 0：`visible_p50_micros=[1291.368] exceeds limit=[1000.000]`；`visible_p99_9_micros=[10156.880] exceeds limit=[10000.000]`。
- 基线 / memory / 100,000：`visible_p50_micros=[1266.270] exceeds limit=[1000.000]`；`visible_p99_9_micros=[10379.701] exceeds limit=[10000.000]`。
- 基线 / wal / 0：`visible_p50_micros=[1380.531] exceeds limit=[1000.000]`；`wal_ack_p50_micros=[2069.097] exceeds limit=[2000.000]`。
- 基线 / wal / 100,000：`visible_p50_micros=[1366.840] exceeds limit=[1000.000]`；`visible_p99_9_micros=[10070.166] exceeds limit=[10000.000]`；`wal_ack_p50_micros=[2068.292] exceeds limit=[2000.000]`。
- 浸泡 / memory / 100,000：`visible_p50_micros=[2842.847] exceeds limit=[1000.000]`；`visible_p99_micros=[5693.982] exceeds limit=[3000.000]`；`visible_p99_9_micros=[20766.074] exceeds limit=[10000.000]`。
- CI / memory / 0：`visible_p50_micros=[1142.435] exceeds limit=[1000.000]`。

浸泡前景进程连续写入 302.04 秒（含预热），超过 5 分钟；不通过重试失败批次拼接样本。

ack 和 visible 是不同确认分支，不能用各自独立百分位相减计算某一跳耗时。
缺口/重复/乱序为零还必须同时满足样本数与 end marker，才能证明本次运行完成。
门限失败、完整性失败及运行中断分别列出；表中所有延迟单位均为微秒。

### 与 A/B/C/D 的差距

| 运行 | C p50 / 1ms | C p99 / 3ms | C p99.9 / 10ms | B p50 / 50µs | B p99 / 300µs | B p99.9 / 1ms | D ack p50 / 2ms | D ack p99 / 5ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 基线 / memory / 0 | 1.291× | 0.680× | 1.016× | 25.827× | 6.800× | 10.157× | 不适用 | 不适用 |
| 基线 / memory / 100,000 | 1.266× | 0.598× | 1.038× | 25.325× | 5.976× | 10.380× | 不适用 | 不适用 |
| 基线 / wal / 0 | 1.381× | 0.797× | 0.940× | 27.611× | 7.968× | 9.405× | 1.035× | 0.652× |
| 基线 / wal / 100,000 | 1.367× | 0.810× | 1.007× | 27.337× | 8.096× | 10.070× | 1.034× | 0.706× |
| 浸泡 / memory / 100,000 | 2.843× | 1.898× | 2.077× | 56.857× | 18.980× | 20.766× | 不适用 | 不适用 |

A 层起点是 Memory ack，当前 visible 起点是写入发起，两者不能换算。
B 层仅列参考倍数，不将 open_log 交付替代 callback。C 只覆盖 loopback；D 的 WAL ack
不表示对应数据已经发布到 Parquet/Catalog。WAL 同步单行写入实际吞吐未达 1,000 行/秒的
单元，应按表中实际速率解释，不能以目标配置替代达到速率。

## 公开路径分析

以下是静态控制流审查，不是单跳计时或瓶颈占比结论。

| 阶段 | 实际行为 | 关键引用 |
| --- | --- | --- |
| Python 写入 | 单行转换为 Arrow，复用 DoPut，发送 batch 后同步读取 ack metadata。 | `clients/python/src/zippy_client/writer.py:88`、`clients/python/src/zippy_client/transports/flight.py:1483` |
| 服务端 ingest | 每批进入 blocking worker，计算 mutation fingerprint，分派 durability。 | `crates/zippy-server/src/flight_query.rs:689` |
| WAL 与内存发布 | 先 append WAL，再 sink.apply；物理写入后通过 Actor 安装 live segments。 | `crates/zippy-ingest/src/native.rs:1937`、`crates/zippy-ingest/src/native.rs:1955` |
| 通知与订阅刷新 | Actor publication 成功后 watch 通知，NativeLog 在窗口读尽后等 changed，再刷新 snapshot。 | `crates/zippy-ingest/src/actor.rs:1801`、`crates/zippy-server/src/native_log.rs:311` |
| 查询边界 | Catalog resolve → Actor snapshot → Catalog pin，再合并持久和 live 数据来源。 | `crates/zippy-server/src/query_snapshot.rs:897`、`crates/zippy-server/src/query_snapshot.rs:939` |
| Python 接收 | DoExchange/read_chunk 后校验与组装 LogEvent，交给 open_log 迭代器。 | `clients/python/src/zippy_client/log_reader.py:313`、`clients/python/src/zippy_client/log_reader.py:215` |

订阅等待是 watch 事件唤醒，没有固定 sleep/interval 取数轮询；watch 可以合并通知，连续
cursor/log offset 保证数据连续性，不要求一条通知对应一行。segment 的底层共享内存通知
不是此 NativeLog 路径直接等待的信号。

Memory 可在 WAL sync 完成前订阅可见：共同路径先 WAL append → apply → publication，
Memory 不等待 sync；Wal 则在共同 publication 后继续等待 commit。WAL 持久边界是
`crates/zippy-ingest/src/wal.rs:587` 的 `sync_data()`。这证明实现允许该顺序，不证明每一行
的实际 subscriber 接收必定先于 sync；本次没有单跳时间戳统计其比例。

待 profiling 验证的候选成本包括单行 Python/Arrow/Flight 固定成本、blocking worker 与
Actor 切换、多个 state/WAL 锁、订阅刷新 Catalog snapshot、WAL sync、后台 checkpoint
以及 WSL2 调度。本次持锁跨 checkpoint 的等待也可能进入尾延迟，尚未拆分其占比。
没有证据支持用总延迟分布直接给这些因素排序。

历史 SHM 组件测量为 p50=46µs、p99=96µs、max=202µs、2,000 个样本，见
`docs/perf_reports/2026-07-11-low-latency-performance-gates.md:135`。
它是同进程 segment producer/consumer 边界，目标 200 行/秒；本次包含完整服务端、
独立 Python 进程、Flight、ingest 与 Catalog，样本数、速率和负载也不同。
两者只可比较量级，不能视为相同测量口径下的退化倍数，历史数据也不代表 A 层公开路径。

## 修复前历史

`pre-fix-baseline.json` 保存上阶段第二次四格运行汇总，结果没有与修复后数据混拼。
该次三格因生产错误中断；WAL / 背景 100,000 收齐，但 C/D p50 超限。

| durability / 背景 | ack / visible 计量样本 | 已确认 / 已接收（含预热） | 前景实际行/秒 | 历史结果 |
| --- | ---: | ---: | ---: | --- |
| memory / 0 | 597 / 597 | 2,597 / 2,597 | 994.74 | SEQUENCE_GAP 中断 |
| memory / 100,000 | 5,870 / 5,870 | 7,870 / 7,870 | 996.59 | 前景与背景均出现 SEQUENCE_GAP |
| wal / 0 | 16,209 / 16,209 | 17,209 / 17,209 | 421.85 | SEQUENCE_GAP 中断 |
| wal / 100,000 | 20,000 / 20,000 | 21,000 / 21,000 | 448.34 | 收齐；C/D p50 超限 |

| 历史进程 | 保留的首个服务端错误 |
| --- | --- |
| memory / 0，前景 | `SEQUENCE_GAP: live segment offsets are not contiguous expected=[2577] actual=[1552]` |
| memory / 100,000，前景 | `SEQUENCE_GAP: live segment offsets are not contiguous expected=[7858] actual=[6813]` |
| memory / 100,000，背景 | `SEQUENCE_GAP: live segment offsets are not contiguous expected=[786000] actual=[682000]` |
| wal / 0，前景 | `SEQUENCE_GAP: live segment offsets are not contiguous expected=[17205] actual=[16911]` |

错误链外层是 `AmbiguousWriteError`；其后的 subscriber `UNAVAILABLE` 是停止服务清理的后果。
前三格的已确认前缀没有序列缺口，但缺少最终 end marker，最后失败写入的 outcome 不确定。
因此历史 `integrity.passed=true` 不能解释为整次完成。无订阅者的背景表也出错，说明订阅
不是触发条件；本次三类确定性测试进一步证实 checkpoint/publication 窗口这一根因。

`initial-failed-run.json` 是更早一次的汇总。各格 ack 计量样本依次为
4,708 / 0 / 18,017 / 0，visible 均未保存；已确认行数为 6,708 / 545 / 19,017 / 0。
那次清理直接终止子进程，subscriber 内存数据缺失，不能把 visible=0 解释为实际全部丢失。
其关停日志中的行数错误包括 `expected=[12] actual=[13]`、`expected=[21] actual=[22]`。
随后只改进基准的失败证据保存：先停止服务解除 reader 阻塞，再有界清理，保留完整异常链。

`pre-fix-ci-smoke.json` 收齐 3,000 个计量样本，含预热共 5,000 行，零缺口；visible
p50=1198.723µs、p99=1987.688µs。该次虽满足 p99≤3000µs，仍因 p50>1000µs 退出 1。
这也是不能只看单一 p99 决定 CI 门禁通过的历史例子。

## 验证、CI 决策与未完成项

| 本次命令 / 检查 | 实际结果 |
| --- | --- |
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 通过，5.26s |
| `cargo test --workspace` | 91 组结果，1206 passed、7 ignored、0 failed；无需 -j 4 回退 |
| 确定性交错测试修复前 / 后 | 前：0 passed、3 failed、退出 101；后：3 passed、退出 0 |
| 最终 `materializer::tests`（含失败原子性验证） | 4 passed，另包含于完整 workspace 测试 |
| `uv run --no-sync pytest pytests -q` | 重建后 396 passed，4.74s |
| `cd clients/python && uv run --no-sync python -m pytest -q` | 247 passed，3.13s；coverage 85.45% ≥ 80% |
| `uv run --no-sync pytest pytests/test_bench_public_e2e.py -q` | 72 passed |
| `uv run --no-sync ruff check scripts/bench_public_e2e.py pytests/test_bench_public_e2e.py` | 通过 |
| 两个改动 Python 文件分别 `uv run --no-sync black --check -l 100 FILE` | 均通过 |
| `uv run --no-sync mypy scripts/bench_public_e2e.py pytests/test_bench_public_e2e.py` | 通过 |
| 证据大小、`sha256sum -c`、`git diff --check` | 均通过，所有证据文件 ≤300KB |

重建前曾运行根目录 Python 测试，出现 1 failed、395 passed：旧扩展 build revision 为
`ff0f950...`，HEAD 已为 `7c6b6a9...`。原失败保留在 `pytests-before-rebuild.log`；
按要求重建 release 扩展后全量通过，没有跳过或放宽版本校验。


CI 短版 visible p99=1576.935µs ≤3000µs，但 p50=1142.435µs >1000µs，整体退出 1。因此保留 `.github/workflows/performance.yml` 中公开路径步骤的 `continue-on-error: true`，更新注释说明竞态已修复、剩余原因是原有延迟门限。没有放宽 C p50/p99/p99.9 或 D ack 门限，也没有在 GitHub 触发实际工作流。

未完成项与结论边界如下：

- 独立 publication 失败仍可能留下未计入逻辑状态的物理行；本次有确定性测试和报告，
  未实现跨 Actor/segment/WAL 的事务回滚。
- 未单独测量严格 B 层用户 callback；A 层本机 Rust SHM 公开路径、远端 Flight、裸机 SLA
  与 GitHub 实际 runner 表现不属于本机复测的证明范围。
- 未添加各跳生产计时或 profiling，不能给出瓶颈排名、锁等待占比或 checkpoint 尾延迟占比。
- 显式 checkpoint 与 persistence 共用极小或饱和 blocking 池时的自等待风险仅有静态推断，
  尚未做容量边界实测或执行器隔离改造。
- WAL 档同步单行写入未达到配置的 1,000 行/秒时，应继续按实际吞吐解释性能结果；
  本次不通过改批量/并发口径伪装目标吞吐已经达到。
- CI 是否移除 `continue-on-error` 依据本次完整门限结果，见上方决策；所有原有门限不放宽。
- 四格、浸泡、检查命令的实际失败或运行限制均以上表为准，不用历史结果替代本次验收。

## 临界区缩小（阶段 1.6，2026-10-10）

本节追加记录阶段 1.5 正确性修复之后的临界区缩小，不改写前文历史结果。阶段 1.5 用同一把 materializer state 锁覆盖 Actor 持久水位推进与本地前缀回收，消除了两套边界交错的问题，但该锁也覆盖了持久化等待。阶段 1.6 保留边界切换的互斥关系，把 Parquet/Catalog 持久提交放到锁外；Catalog 已提交而 Actor 尚未推进时，查询继续使用旧 Actor 指定的 Catalog 版本与旧 live 前缀。性能结果及运行命令由本节后续表格单独记录，以下论证不以延迟指标代替正确性证据。

### 两阶段实现与锁的准确范围

checkpoint 内部现在分成两个阶段：

1. **完成持久提交，暂不切换查询可见性。** `persist_checkpoint_prefix` 在独立异步 owner 中调用内部 `prepare_persist`；此时没有取得 materializer state 锁，也不占用一个 blocking worker 等待另一个 blocking worker 完成文件工作。Actor 仍调用原 receipt 校验，成功后把 receipt、Advance/Replay disposition、单表 permit 和全局 permit 留在新增的私有 `Prepared` 状态。外部既有 `TablePersistenceStatus` 仍映射为 `InFlight`，没有新增公开状态枚举项。证据：`crates/zippy-ingest/src/materializer.rs:616`、`crates/zippy-ingest/src/materializer.rs:618`、`crates/zippy-ingest/src/actor.rs:681`、`crates/zippy-ingest/src/actor.rs:2151`、`crates/zippy-ingest/src/actor.rs:2177`、`crates/zippy-ingest/src/actor.rs:1575`。

2. **在短临界区内切换可见边界并移交回收资源。** 持久准备完成后，blocking worker 取得 materializer state 锁，通过预留 oneshot 通道请求 Actor 推进 snapshot；随后更新 materializer 持久水位、移除完整持久前缀、更新行数与字节预算，再解锁。锁内同步等待的是一次内存 Actor 发布操作，不再等待 Parquet/Catalog 提交。证据：`crates/zippy-ingest/src/materializer.rs:628`、`crates/zippy-ingest/src/materializer.rs:632`、`crates/zippy-ingest/src/materializer.rs:636`、`crates/zippy-ingest/src/materializer.rs:643`、`crates/zippy-ingest/src/materializer.rs:651`、`crates/zippy-ingest/src/actor.rs:365`、`crates/zippy-ingest/src/actor.rs:2245`。

真正切换 Actor snapshot 时仍发出既有 changes 通知；仅持久准备完成不会提前通知 snapshot 已推进。公开 `try_persist`/`persist` 保留原完成语义，仅 materializer 使用 crate 内部两阶段入口。证据：`crates/zippy-ingest/src/actor.rs:1740`、`crates/zippy-ingest/src/actor.rs:623`、`crates/zippy-ingest/src/actor.rs:675`、`crates/zippy-ingest/src/actor.rs:2175`、`crates/zippy-ingest/src/actor.rs:2189`。

### Catalog 领先于 Actor 时，查询为什么不会重复读前缀

查询首先取得 Actor 边界，然后把 **Actor 的 persisted_snapshot_version** 放入 Catalog selector；它没有把 Catalog 当前最新文件集合直接拼到旧 live 集合上。Catalog selector 允许选择历史版本，文件 SQL 按 `added_snapshot_version <= selected` 与 `removed_snapshot_version > selected` 或尚未移除过滤。因此 Prepared 期间新增文件虽然已在 Catalog 最新版本中，旧 Actor 对应的 selector 不会把它加入持久文件集。证据：`crates/zippy-server/src/query_snapshot.rs:923`、`crates/zippy-server/src/query_snapshot.rs:929`、`crates/zippy-server/src/query_snapshot.rs:931`、`crates/zippy-server/src/query_snapshot.rs:942`、`crates/zippy-catalog/src/snapshot_domain.rs:705`、`crates/zippy-catalog/src/snapshot_domain.rs:69`。

查询校验允许 Catalog 当前 snapshot 领先于 Actor，但不允许 Catalog 落后、writer epoch 不一致；完成 pin 后还会再次核验 selector 等于 Actor 观察值，以及所选 snapshot 的 committed_log_offset 等于 Actor 持久水位。证据：`crates/zippy-server/src/query_snapshot.rs:1696`、`crates/zippy-server/src/query_snapshot.rs:1698`、`crates/zippy-server/src/query_snapshot.rs:1008`、`crates/zippy-server/src/query_snapshot.rs:1020`。

最终发布只对 Actor 已安装、已校验的 live spans 按新持久水位裁剪。`RowSpanView::slice_rows` 检查相对行界限并克隆既有 backing，底层共享内存是 `Arc<ShmRegion>`；这里不重新打开 descriptor 文件。旧 live Arc 被移入 publication guard，已取得的查询 span 也继续持有自己的映射。证据：`crates/zippy-ingest/src/actor.rs:1269`、`crates/zippy-ingest/src/actor.rs:1312`、`crates/zippy-ingest/src/actor.rs:2249`、`crates/zippy-ingest/src/actor.rs:2250`、`crates/zippy-segment-store/src/view.rs:31`、`crates/zippy-segment-store/src/view.rs:219`。

这里的历史快照安全有明确范围：Actor 存活本身不等于持有 Catalog GC pin。查询 pin 在 Catalog 查询接口中取得；普通 checkpoint 的 CommitDataFiles 只插入 snapshot/输出文件并推进表版本，不把旧文件标记为 removed，GC 只选择已标记移除的 managed 文件。因此本次普通 checkpoint 的 Prepared 窗口不会单独使旧文件成为 GC 候选；不能据此推广成“任意并发 Catalog compaction/drop 下，旧 Actor 自然保护所有历史文件”。证据：`crates/zippy-catalog/src/service.rs:764`、`crates/zippy-catalog/src/manifest_domain.rs:940`、`crates/zippy-catalog/src/manifest_domain.rs:951`、`crates/zippy-catalog/src/manifest_domain.rs:957`、`crates/zippy-catalog/src/gc_domain.rs:255`、`crates/zippy-catalog/src/gc_domain.rs:257`。

### 回收不能把文件析构藏在锁内

仅把显式持久化 await 移出锁还不够：旧 segment 最后一个 owner 的析构会解除映射、关闭文件，owner 还会删除 backing path。证据：`crates/zippy-segment-store/src/shm.rs:562`、`crates/zippy-segment-store/src/shm.rs:567`、`crates/zippy-segment-store/src/shm.rs:584`、`crates/zippy-segment-store/src/shm.rs:586`。

因此这次同时把三类资源移到锁外释放：

- **retired segment owner 与 descriptor。** `retire_persisted_prefix_deferred` 先检查所有 sealed descriptor 已有缓存，然后只按缓存行数、完整段边界与 pin 条件移除拥有者。底层 partition 只从 retired map 中 move 出资源，不在锁内销毁。缓存缺失会在任何移除之前返回错误，避免“前段已经移出、后段失败”导致局部 guard 在调用者锁内析构。证据：`crates/zippy-engines/src/stream_table.rs:321`、`crates/zippy-engines/src/stream_table.rs:325`、`crates/zippy-engines/src/stream_table.rs:344`、`crates/zippy-engines/src/stream_table.rs:351`、`crates/zippy-engines/src/stream_table.rs:358`、`crates/zippy-segment-store/src/catalog.rs:420`、`crates/zippy-segment-store/src/catalog.rs:430`。
- **已经没有 live 数据的旧 epoch materializer。** 从 epochs 中移出后先存入 retired_epochs，空 active segment 等仍由它持有的资源也到解锁后才析构。证据：`crates/zippy-ingest/src/materializer.rs:711`、`crates/zippy-ingest/src/materializer.rs:720`、`crates/zippy-ingest/src/materializer.rs:655`。
- **Actor 发布前的旧 live Arc。** 快发布把旧 Arc 和两个 permit 一起交回 materializer；permit 因而继续排除另一次 persist 与表退休，直到本地回收完成。证据：`crates/zippy-ingest/src/actor.rs:357`、`crates/zippy-ingest/src/actor.rs:2250`、`crates/zippy-ingest/src/actor.rs:2253`、`crates/zippy-ingest/src/materializer.rs:656`。

materializer 把可失败的收尾工作放在内部结果闭包中，随后显式先 `drop(state)`，再 drop 三类资源；跨 epoch 回收若后续返回普通错误，之前收集的资源也在解锁后释放。回收结尾不再重新 `build_publication → reopen descriptor → install`；Actor 已按水位裁剪 live 集合，下一次正常 apply 仍执行原完整 publication 校验。证据：`crates/zippy-ingest/src/materializer.rs:633`、`crates/zippy-ingest/src/materializer.rs:651`、`crates/zippy-ingest/src/materializer.rs:654`、`crates/zippy-ingest/src/materializer.rs:675`、`crates/zippy-ingest/src/materializer.rs:722`、`crates/zippy-ingest/src/materializer.rs:491`、`crates/zippy-ingest/src/actor.rs:1341`。

### 其他 persist、schema/drop、关闭与取消

| 边界 | 当前行为及证据 |
| --- | --- |
| 另一次 persist | admission 在进入 mailbox 前占单表 permit 和全局预算。Prepared 保留它们，快发布后交给 materializer guard；回收完成前第二个 persist 仍按既有 ResourceExhausted 拒绝。`crates/zippy-ingest/src/actor.rs:643`、`crates/zippy-ingest/src/actor.rs:646`、`crates/zippy-ingest/src/actor.rs:2181`、`crates/zippy-ingest/src/actor.rs:2254`、`crates/zippy-ingest/src/materializer.rs:656`。 |
| schema 变更 | Native transition 先取得 open_gate；schema 路径先完成 ingest checkpoint，再取得 Actor retirement guard，之后才提交 Catalog schema 命令。Actor retirement 也必须取得同一单表 permit。`crates/zippy-ingest/src/native.rs:753`、`crates/zippy-server/src/table_admin.rs:295`、`crates/zippy-server/src/table_admin.rs:296`、`crates/zippy-server/src/table_admin.rs:297`、`crates/zippy-server/src/table_admin.rs:298`、`crates/zippy-ingest/src/actor.rs:813`。 |
| drop | drop 同样先建立 Native transition，再准备 Actor retirement，然后才提交 Catalog 命令和移除运行时。已有 checkpoint 占 gate 或 permit 时不会穿过该窗口。`crates/zippy-server/src/table_admin.rs:143`、`crates/zippy-server/src/table_admin.rs:149`、`crates/zippy-server/src/table_admin.rs:153`、`crates/zippy-ingest/src/native.rs:753`。 |
| 正常 Native fencing | open writer 与 checkpoint 共用 open_gate；正常公开 fencing 不会在未结束的 checkpoint 中途切换 epoch。Actor 另对内部 staged InFlight/Prepared 拒绝 raw epoch 更新。`crates/zippy-ingest/src/native.rs:1268`、`crates/zippy-ingest/src/native.rs:2353`、`crates/zippy-ingest/src/actor.rs:1905`。 |
| Actor 关闭 | 普通 mailbox 关闭不会丢弃已接受操作的 continuation。Prepared 对应的 publication receiver 仍属于 completion；退出条件要求 mailbox_closed 且 completion 为空。快发布用预留 oneshot，不需要重新通过已关闭的 admission。`crates/zippy-ingest/src/actor.rs:685`、`crates/zippy-ingest/src/actor.rs:2185`、`crates/zippy-ingest/src/actor.rs:1716`、`crates/zippy-ingest/src/actor.rs:1749`、`crates/zippy-ingest/src/actor.rs:365`。 |
| 整体服务 shutdown | 先等 Native ingest/checkpoint 完整 drain，再关闭 Actor admission；后续依次 drain Actor、persistence，最后关闭 Catalog。不能只用“Actor 发布消息已完成”替代整个 checkpoint job 的结束。`crates/zippy-server/src/native_storage.rs:844`、`crates/zippy-server/src/native_storage.rs:849`、`crates/zippy-server/src/native_storage.rs:1495`、`crates/zippy-server/src/native_storage.rs:1499`、`crates/zippy-server/src/native_storage.rs:1503`。 |
| 调用者取消 | 公共 checkpoint 的独立任务持 operation permit 和 open_gate；materializer 内部另有独立 owner 持 prepared token，并把收尾交给 blocking worker。丢弃外部 waiter 不会取消已接纳的持久操作或遗漏本地回收。`crates/zippy-ingest/src/native.rs:810`、`crates/zippy-ingest/src/native.rs:813`、`crates/zippy-ingest/src/native.rs:816`、`crates/zippy-ingest/src/materializer.rs:616`、`crates/zippy-ingest/src/materializer.rs:628`。 |

需要精确区分 **Actor 已发布** 和 **materializer 已完成回收**：快发布会把 Actor 内部状态恢复为 Idle，permit 此时仍在 materializer guard 中。因此这段时间内另一次 persist、schema/drop 仍受 permit 阻止，但 raw `TableActorHandle::update_writer_epoch` 的状态检查已经不再命中 Prepared，可能成功。正常 Native fencing 仍由上述 open_gate 和 state mutex 排除；本次没有把 raw Actor epoch 接口改成“直到 guard 析构都拒绝”。不能把更强的 raw Actor 排他语义写成本次保证。证据：`crates/zippy-ingest/src/actor.rs:2227`、`crates/zippy-ingest/src/actor.rs:1905`、`crates/zippy-ingest/src/actor.rs:1915`、`crates/zippy-ingest/src/materializer.rs:651`、`crates/zippy-ingest/src/materializer.rs:656`。

### 失败与恢复

普通持久提交 rejection 保留旧 snapshot 并释放预算；fatal 或无效 receipt 继续走已有 terminal failure。request/table identity、parent、snapshot version、operation、commit LSN 和 watermark 校验仍在原校验函数中，未因拆阶段被删除。Prepared 私有 owner 异常丢失时不会自动在无 materializer 锁的情况下发布，而是关闭 admission 并报告失败。证据：`crates/zippy-ingest/src/actor.rs:2197`、`crates/zippy-ingest/src/actor.rs:2203`、`crates/zippy-ingest/src/actor.rs:2261`、`crates/zippy-ingest/src/actor.rs:2233`。

durable 已成功后若 finalization/reclaim 返回错误，Catalog 提交不能回滚；正常 Native checkpoint 路径把非可重试 ResourceExhausted 错误记录为表 terminal error。代码没有承诺跨 Catalog、Actor、segment 的事务回滚，也没有把独立 apply publication 失败后可能遗留的物理行问题扩展宣称为已修复。证据：`crates/zippy-ingest/src/native.rs:2360`、`crates/zippy-ingest/src/native.rs:2366`、`crates/zippy-ingest/src/materializer.rs:636`、`crates/zippy-ingest/src/materializer.rs:644`。

如果进程恰在 Catalog 已提交、Actor 尚未发布时退出，重启以 Catalog bootstrap 构建 runtime 与 Native bootstrap；恢复 visible_watermark 从 Catalog persisted_log_offset 开始。WAL 恢复跳过完全被该水位覆盖的原子 mutation，仅重放后缀；如果持久水位切开一个原子 mutation，仍按既有 DataCorruption 拒绝。证据：`crates/zippy-server/src/native_storage.rs:1130`、`crates/zippy-server/src/native_storage.rs:1148`、`crates/zippy-ingest/src/actor.rs:1239`、`crates/zippy-ingest/src/native.rs:2218`、`crates/zippy-ingest/src/native.rs:2242`、`crates/zippy-ingest/src/native.rs:2263`、`crates/zippy-ingest/src/native.rs:2597`、`crates/zippy-ingest/src/native.rs:2598`。

WAL archive checkpoint 提交和文件回收仍在完整 materializer job 返回之后，不能因 prepare receipt 已可用而提前执行。证据：`crates/zippy-ingest/src/native.rs:2492`、`crates/zippy-ingest/src/native.rs:2514`、`crates/zippy-ingest/src/native.rs:2515`。

### 保留的 I/O 边界与接口披露

“锁内不做文件 I/O”限定于本次改造的 **持久准备完成后的 finalization 临界区**。原 `prepare_checkpoint` 仍在 state 锁内 seal/rollover 活跃段并进行普通 publication；rollover 分配新映射文件，普通 publication 会 attach descriptor。普通 apply 的物化与发布也仍在其原临界区内。此阶段没有重构这些已有路径，不能写成“全部 materializer 临界区已无 I/O”。证据：`crates/zippy-ingest/src/materializer.rs:270`、`crates/zippy-ingest/src/materializer.rs:279`、`crates/zippy-ingest/src/materializer.rs:284`、`crates/zippy-segment-store/src/catalog.rs:1230`、`crates/zippy-segment-store/src/builder.rs:186`、`crates/zippy-segment-store/src/shm.rs:71`、`crates/zippy-ingest/src/materializer.rs:353`、`crates/zippy-ingest/src/materializer.rs:491`、`crates/zippy-ingest/src/actor.rs:1479`。

客户端 API、wire 协议、磁盘格式、原有 Rust 方法签名及公开状态枚举保持不变。Actor 两阶段入口与 token 使用 `pub(crate)`。为了在 engines、segment-store、ingest 三个 crate 之间移交资源所有权，新增了两个 `#[doc(hidden)]` helper 方法和两个 guard 类型；**它们技术上仍是 Rust `pub`，doc(hidden) 不等于私有**。这是隐藏实现接口的增量，不能表述为“完全没有新增 Rust 公开表面”。证据：`crates/zippy-ingest/src/actor.rs:351`、`crates/zippy-ingest/src/actor.rs:681`、`crates/zippy-engines/src/stream_table.rs:50`、`crates/zippy-engines/src/stream_table.rs:321`、`crates/zippy-segment-store/src/catalog.rs:419`、`crates/zippy-segment-store/src/catalog.rs:482`、`crates/zippy-engines/src/lib.rs:23`、`crates/zippy-segment-store/src/lib.rs:19`。

原 `retire_persisted_prefix` 先调用原 `live_segments()` 做校验并填缓存，再使用新 helper 并立即释放 guard；原 `release_retired_segment` 也仍在返回前释放资源，所以既有调用者不需要改变生命周期管理。新 helper 才要求 caller 已填好缓存并在解锁后 drop 返回资源。证据：`crates/zippy-engines/src/stream_table.rs:303`、`crates/zippy-engines/src/stream_table.rs:305`、`crates/zippy-engines/src/stream_table.rs:307`、`crates/zippy-segment-store/src/catalog.rs:409`。

### 当前验证证据及其限度

`docs/perf_reports/evidence/2026-10-10-critical-section/checkpoint-fixed.log:86` 记录 zippy-ingest 本次测试结果为 **73 passed、0 failed、2 ignored**。其中包含：append/frame/key-value 的既有 publication 交错回归；三个表类型在 Catalog 持久提交暂停时继续写入；单 blocking worker 不被嵌套持久化等待耗尽；Prepared 期间其他 persist、epoch 更新及 retirement 的拒绝；关闭 admission 后完成预留发布；私有 owner 丢失、rejection 与 fatal 分支。该数字是这份日志的测试范围，不是所有 crate 的累计测试总数。

新增真实 Catalog Prepared 测试覆盖三个表类型：提交新 snapshot 后暂停可见发布，验证旧 Actor selector 的持久文件与 live 行合并不缺不重；测试还故意把 Catalog 最新版本与旧 Actor live 混合，确认会产生重复，从反面验证 selector 约束的必要性。随后从该真实 Catalog 状态新建 runtime、Actor 和 materializer，验证恢复采用新水位，再显式应用未持久后缀。证据：`crates/zippy-ingest/src/materializer/prepared_tests.rs:85`、`crates/zippy-ingest/src/materializer/prepared_tests.rs:140`、`crates/zippy-ingest/src/materializer/prepared_tests.rs:178`、`crates/zippy-ingest/src/materializer/prepared_tests.rs:181`、`crates/zippy-ingest/src/materializer/prepared_tests.rs:195`、`crates/zippy-ingest/src/materializer/prepared_tests.rs:245`。

上述恢复测试是从真实 Catalog 中间状态重建运行时的确定性测试，**没有杀死独立 server 进程并执行完整 WAL 重放**；WAL 跳过持久前缀的结论由生产代码与已有 WAL recovery 回归共同支持。延迟析构单元测试另外验证了 reader pin、零/部分段预算、generation/id 不匹配、guard 存活时文件仍可 attach、guard 释放后 unlink，以及后段缓存缺失不会提前释放前段。证据：`crates/zippy-engines/src/stream_table.rs:834`、`crates/zippy-engines/src/stream_table.rs:897`、`docs/perf_reports/evidence/2026-10-10-critical-section/checkpoint-fixed.log:45`。

当前独立源码审查没有发现正常公开 checkpoint 路径新增的数据一致性阻塞问题。这个结论限定于上述实现和验证范围，不扩大为所有崩溃点、任意内部 API 交错、所有 apply 失败路径或任意并发 Catalog 管理操作均已被端到端验证。

### 四格基准对比

本次四格均完成 20,000 个计量样本，完整性无缺口、重复或乱序。相对 b5bf6d3 收录的历史基线，四格 visible p99.9 均下降；按本节采用的修复前 >10ms 频次目标，前三格未通过，只有 wal / 背景 100,000 通过。不能以改善幅度替代该门限，也不能把截断历史观测当作稳定 SLA。

所有时长单位均明确标注。延迟为 nearest-rank；单格样本少于 10,000 时 p99.9 记为 null。各列均直接来自指定原始 JSON 并与其对应摘要及重算统计核对一致，没有修改历史数据。

#### 身份与观测范围

“b5bf6d3”表示该提交收录的历史证据；这些数据实际 metadata 为 `7c6b6a9784336827c7239a872e50bccfda15d501` 且 `git_dirty=true`，不能称为 clean b5bf6d3 的实测。本次 metadata 为 `b5bf6d3e10bcd51eb7b34decb22db3091e4ab30e` 且 `git_dirty=true`。pre-fix 为 `ff0f95055c0e16df64b1b3c455f3b53755c8dbc6` 且 dirty。

三列均为 WSL2（5.15.153.1-microsoft-standard-WSL2），AMD Ryzen 9 9950X、32 逻辑核、affinity 0–31，Python 3.11.14 / PyArrow 23.0.1 / zippy 0.5.0 / zippy-client 0.1.0。前景目标 1,000 行/秒、一次一行且只有一个 outstanding write；背景目标 100,000 行/秒按 1,000 行一批写另一张表。预热至少 2 秒且至少剔除前 1,000 行。

| 路径 / 背景目标行每秒 | pre-fix：样本 / visible 暴露秒 / 完成性 | b5 历史：样本 / 暴露秒 | 本次：样本 / 暴露秒 |
| --- | --- | --- | --- |
| memory / 0 | 597 / 0.597607 / 失败截断 | 20,000 / 20.000234 | 20,000 / 20.000420 |
| memory / 100,000 | 5,870 / 5.887585 / 失败截断 | 20,000 / 20.000467 | 20,000 / 20.000141 |
| wal / 0 | 16,209 / 38.417625 / 失败截断 | 20,000 / 43.024357 | 20,000 / 41.918054 |
| wal / 100,000 | 20,000 / 44.608603 / 完整 | 20,000 / 43.606683 | 20,000 / 42.950467 |

暴露窗口严格取首个计量样本实际 sent_ns 至最后一个计量样本 received_ns；不使用“样本数÷目标速率”。WAL 同步确认使实际前景速率低于目标，因而两格的真实计量暴露约 42–45 秒。pre-fix 前三格失败截断；虽然已收到的序列没有缺口，其运行仍不完整。

#### visible 延迟（µs）

| 路径 / 背景目标行每秒 | pre-fix：p50 / p99 / p99.9 / max | b5 历史：p50 / p99 / p99.9 / max | 本次：p50 / p99 / p99.9 / max |
| --- | --- | --- | --- |
| memory / 0 | 1,198.608 / 1,623.041 / null / 5,354.688 | 1,291.368 / 2,039.929 / 10,156.880 / 15,663.592 | 1,235.225 / 1,748.249 / 6,157.364 / 14,533.116 |
| memory / 100,000 | 1,284.721 / 2,177.713 / null / 12,526.676 | 1,266.270 / 1,792.898 / 10,379.701 / 18,161.960 | 1,250.600 / 1,956.124 / 8,475.353 / 16,042.638 |
| wal / 0 | 1,506.708 / 2,943.924 / 5,174.738 / 17,777.358 | 1,380.531 / 2,390.524 / 9,404.951 / 13,836.502 | 1,332.277 / 2,321.517 / 5,178.351 / 14,788.946 |
| wal / 100,000 | 1,434.116 / 2,140.156 / 5,605.399 / 18,142.310 | 1,366.840 / 2,428.947 / 10,070.166 / 14,048.511 | 1,349.946 / 2,235.049 / 5,235.842 / 12,153.181 |

#### ack 延迟（µs）

| 路径 / 背景目标行每秒 | pre-fix：p50 / p99 / p99.9 / max | b5 历史：p50 / p99 / p99.9 / max | 本次：p50 / p99 / p99.9 / max |
| --- | --- | --- | --- |
| memory / 0 | 749.140 / 1,070.345 / null / 4,987.368 | 793.202 / 1,333.606 / 9,496.564 / 15,021.962 | 766.404 / 1,102.931 / 5,534.270 / 14,051.882 |
| memory / 100,000 | 805.133 / 1,397.089 / null / 11,779.930 | 764.595 / 1,096.021 / 9,520.726 / 17,327.426 | 765.001 / 1,220.421 / 7,745.233 / 15,416.948 |
| wal / 0 | 2,250.871 / 4,754.791 / 10,800.811 / 21,375.236 | 2,069.097 / 3,257.505 / 10,303.669 / 13,842.603 | 2,012.409 / 3,467.674 / 6,485.029 / 13,484.120 |
| wal / 100,000 | 2,142.489 / 3,343.207 / 7,226.061 / 19,171.089 | 2,068.292 / 3,529.735 / 10,695.380 / 14,637.698 | 2,052.186 / 3,425.499 / 6,894.838 / 12,486.548 |

四格 visible p99.9 相对 b5 历史的变化：memory / 0 下降 39.38%；memory / 100,000 下降 18.35%；wal / 0 下降 44.94%；wal / 100,000 下降 48.01%。这一改善结论的比较对象是 b5 历史；与 pre-fix 的 wal / 0 相比，本次 p99.9 5,178.351µs 略高于 5,174.738µs，不能泛化为对所有旧数据均改善。

#### visible 严格超限次数与每分钟频次

判定为 latency > 门限；等于门限不计入。每个单元为“次数 / 次每分钟”，频次使用该格真实暴露秒数归一化。

| 路径 / 背景目标行每秒 | 阈值 | pre-fix：次数 / 每分钟 | b5 历史：次数 / 每分钟 | 本次：次数 / 每分钟 |
| --- | --- | --- | --- | --- |
| memory / 0 | >2ms | 2 / 200.801 | 228 / 683.992 | 91 / 272.994 |
| memory / 0 | >5ms | 1 / 100.400 | 54 / 161.998 | 35 / 104.998 |
| memory / 0 | >10ms | 0 / 0.000 | 24 / 71.999 | 5 / 15.000 |
| memory / 100,000 | >2ms | 80 / 815.275 | 111 / 332.992 | 176 / 527.996 |
| memory / 100,000 | >5ms | 9 / 91.718 | 54 / 161.996 | 39 / 116.999 |
| memory / 100,000 | >10ms | 1 / 10.191 | 25 / 74.998 | 8 / 24.000 |
| wal / 0 | >2ms | 1,184 / 1849.151 | 442 / 616.395 | 449 / 642.683 |
| wal / 0 | >5ms | 19 / 29.674 | 58 / 80.884 | 23 / 32.921 |
| wal / 0 | >10ms | 7 / 10.932 | 9 / 12.551 | 8 / 11.451 |
| wal / 100,000 | >2ms | 370 / 497.662 | 521 / 716.863 | 391 / 546.211 |
| wal / 100,000 | >5ms | 35 / 47.076 | 67 / 92.188 | 22 / 30.733 |
| wal / 100,000 | >10ms | 6 / 8.070 | 23 / 31.647 | 5 / 6.985 |

#### >10ms 频次目标与诚实判定

pre-fix memory / 0 仅暴露 0.597607 秒且观察到 0 次 >10ms；该数据不足以估计稳定频次，不能把“0 次”解释为可靠的零频次目标。本次借用同为 memory 的背景 100,000 格：1 次 / 5.887585 秒 = 10.190936 次每分钟。这个借用跨了背景负载，且原格本身失败截断、仅 1 个事件，统计不确定性很大；它只是本阶段明确采用的临时比较目标，不是测得的等价无背景基线或置信上限。memory / 100,000 同样使用其自身截断观测。WAL 两格使用各自 pre-fix 原始观测，wal / 0 也须保留失败截断说明。

| 路径 / 背景目标行每秒 | 目标：本次 >10ms 频次不得高于（次/分钟） | 本次（次/分钟） | 判定 |
| --- | ---: | ---: | --- |
| memory / 0 | 10.190936 | 14.999685 | **未通过** |
| memory / 100,000 | 10.190936 | 23.999830 | **未通过** |
| wal / 0 | 10.932482 | 11.450913 | **未通过** |
| wal / 100,000 | 8.070192 | 6.984790 | 通过 |

不能改用较高的 b5 历史尖峰频次作为通过门限，不能扩大到任意同类格的最大值，也不能加入额外的误差容忍区间把前三格改判通过。以上是本阶段相对频次目标；原始 C 层 visible 1,000 / 3,000 / 10,000µs（p50 / p99 / p99.9）和 D 层 WAL ack 2,000 / 5,000µs（p50 / p99）保持不变。

#### 尖峰簇与簇起点间隔

对每个 >2/5/10ms 阈值分别聚类。按 received_ns 顺序，相邻超限样本间隔 ≤10ms 属于同簇，>10ms 开始新簇；不以 seq 连续性代替时间条件。间隔取相邻簇第一个超限样本的接收时刻之差，仅包含暴露窗口内观测到的完整间隔。至少 5 个间隔（即至少 6 簇）才报告间隔分布，否则为 null 并列明数量。该规则得到的是描述统计，不证明周期或因果。

下表单位：簇数；起点间隔 p50 / p95 / max 均为 ms。频次与暴露窗见上表；末尾未观测到下一簇的等待时间不作为一个间隔。

| 路径 / 背景目标行每秒 | 阈值 | pre-fix：簇数；间隔 p50 / p95 / max ms | b5 历史：簇数；间隔 p50 / p95 / max ms | 本次：簇数；间隔 p50 / p95 / max ms |
| --- | --- | --- | --- | --- |
| memory / 0 | >2ms | 2 簇；null（仅 1 个间隔） | 87 簇；111.705 / 760.248 / 1,024.519 | 69 簇；211.067 / 1,006.951 / 1,023.962 |
| memory / 0 | >5ms | 1 簇；null（仅 0 个间隔） | 37 簇；21.168 / 1,024.693 / 1,024.856 | 24 簇；1,023.962 / 1,025.659 / 1,027.662 |
| memory / 0 | >10ms | 0 簇；null（仅 0 个间隔） | 22 簇；1,024.463 / 1,035.296 / 1,035.721 | 5 簇；null（仅 4 个间隔） |
| memory / 100,000 | >2ms | 26 簇；179.333 / 537.416 / 1,015.701 | 68 簇；199.389 / 794.827 / 1,023.571 | 69 簇；113.973 / 1,022.716 / 1,046.662 |
| memory / 100,000 | >5ms | 6 簇；1,047.745 / 1,080.614 / 1,080.614 | 39 簇；18.594 / 1,045.279 / 1,049.649 | 27 簇；1,034.236 / 1,046.662 / 1,048.497 |
| memory / 100,000 | >10ms | 1 簇；null（仅 0 个间隔） | 24 簇；1,039.900 / 1,048.061 / 1,051.538 | 8 簇；2,095.876 / 3,135.890 / 3,135.890 |
| wal / 0 | >2ms | 286 簇；56.449 / 526.358 / 1,642.653 | 184 簇；89.968 / 983.224 / 1,020.455 | 170 簇；105.697 / 1,010.148 / 2,111.030 |
| wal / 0 | >5ms | 19 簇；1,025.257 / 4,100.496 / 4,100.496 | 47 簇；1,019.459 / 1,029.366 / 1,033.095 | 22 簇；1,023.579 / 3,075.375 / 4,101.818 |
| wal / 0 | >10ms | 7 簇；4,097.864 / 4,123.802 / 4,123.802 | 9 簇；3,064.055 / 9,195.564 / 9,195.564 | 8 簇；3,073.260 / 8,124.216 / 8,124.216 |
| wal / 100,000 | >2ms | 221 簇；71.052 / 838.445 / 1,793.483 | 195 簇；79.668 / 985.439 / 1,057.353 | 169 簇；100.542 / 940.644 / 2,807.999 |
| wal / 100,000 | >5ms | 34 簇；1,041.595 / 4,179.833 / 8,368.524 | 62 簇；1,033.290 / 1,048.069 / 1,057.353 | 22 簇；1,047.705 / 3,143.218 / 4,176.412 |
| wal / 100,000 | >10ms | 6 簇；3,117.234 / 6,269.037 / 6,269.037 | 23 簇；1,048.197 / 4,171.334 / 4,179.360 | 5 簇；null（仅 4 个间隔） |

本次 memory / 0 与 wal / 100,000 的 >10ms 都只有 5 簇、4 个间隔，依规则不估计间隔分布。memory / 100,000 为 8 簇、7 个间隔，中位数约 2.096 秒；wal / 0 为 8 簇、7 个间隔，中位数约 3.073 秒。样本稀少且区间宽，不应宣称残余尖峰存在稳定周期。

### 300,000 样本浸泡对比：memory / 背景 100,000 行每秒

本次浸泡完整完成，计量 ack 和 visible 各 300,000 个样本，含预热共收到 302,000 行，缺口、重复、乱序和意外序号均为 0；所有工作进程及服务端退出码为 0。基准主脚本退出 1，原因是原始 C 层 visible p50、p99、p99.9 全部超过门限。相对 b5 历史，p99.9 和 >10ms 频次明显改善，但 p50/p99 未改善，最大值变大；本阶段临时 >10ms 频次目标仍未通过。

pre-fix **没有同配置、同样本规模的浸泡数据**，因此下表该列均标记缺失。不能把 pre-fix 20,000 目标样本基线或其截断观测当成 pre-fix soak，也不能据此给出浸泡改进百分比。

#### 完成性、暴露时长与实际负载

| 指标 | pre-fix soak | b5 历史 soak | 本次 soak |
| --- | --- | ---: | ---: |
| 计量 ack / visible 样本 | 缺数据 | 300,000 / 300,000 | 300,000 / 300,000 |
| 含预热收到 / 应收行数 | 缺数据 | 302,000 / 302,000 | 302,000 / 302,000 |
| 预热剔除行数 | 缺数据 | 2000 | 2000 |
| visible 暴露秒 | 缺数据 | 300.042534710 | 300.033145408 |
| 实际前景行/秒 | 缺数据 | 999.867829 | 999.897605 |
| 实际背景行/秒 | 缺数据 | 99999.961692 | 99999.940953 |
| 缺口 / 重复 / 乱序 / 意外序号 | 缺数据 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| ordered end marker | 缺数据 | 完整 | 完整 |
| worker / server 错误 | 缺数据 | 0 / 0 | 0 / 0 |
| 基准主脚本退出码 | 缺数据 | 1（延迟门限） | 1（延迟门限） |

两次均为 Linux CLOCK_MONOTONIC、WSL2、独立 Python 写入/订阅/背景子进程；前景目标 1,000 行/秒，一次一行、同步等待 ack；背景向另一张表以 1,000 行一批写入，目标 100,000 行/秒。预热至少 2 秒且至少 1,000 行，两次实际剔除 2,000 行。visible 的真实暴露窗口为首个计量样本实际 sent_ns 至最后一个计量样本 received_ns。

“b5 历史”指 b5bf6d3 收录的证据，原 metadata 是 `7c6b6a9784336827c7239a872e50bccfda15d501`、dirty=true；本次是 `b5bf6d3e10bcd51eb7b34decb22db3091e4ab30e`、dirty=true。不能把前者称为 clean b5bf6d3 的现场测试。输入路径、实际元数据和 raw SHA-256 均保留在 `docs/perf_reports/evidence/2026-10-10-critical-section/comparison.json`，这里不重复抄写哈希。

#### 延迟（µs，nearest-rank）

| 指标 | pre-fix soak | b5 历史：p50 / p99 / p99.9 / max | 本次：p50 / p99 / p99.9 / max |
| --- | --- | --- | --- |
| visible | 缺数据 | 2,842.847 / 5,693.982 / 20,766.074 / 44,185.003 | 2,879.595 / 5,711.784 / 10,906.406 / 49,040.372 |
| ack | 缺数据 | 812.747 / 1,482.741 / 18,246.198 / 41,827.315 | 830.995 / 1,485.417 / 8,391.969 / 45,757.370 |

相对 b5 历史：visible p99.9 下降 47.48%；ack p99.9 下降 54.01%。visible p50 增加 1.29%、p99 增加 0.31%，visible max 从 44,185.003µs 增至 49,040.372µs，ack max 从 41,827.315µs 增至 45,757.370µs。没有重复运行形成置信区间，因此这些微小中位/尾部分位差异仅描述本次观测，不作统计显著性判断。

#### visible 严格超限次数与每分钟频次

仅 latency > 阈值计数；等于阈值不计入。频次 = 超限次数 / 实际暴露分钟数。

| 阈值 | pre-fix soak | b5 历史：次数 / 次每分钟 | 本次：次数 / 次每分钟 |
| --- | --- | ---: | ---: |
| >2ms | 缺数据 | 217,717 / 43537.227189 | 218,803 / 43755.765658 |
| >5ms | 缺数据 | 11,222 / 2244.081829 | 11,946 / 2388.936059 |
| >10ms | 缺数据 | 642 / 128.381798 | 373 / 74.591759 |

>10ms 频次从 128.381798 降至 74.591759 次每分钟，下降 41.90%。本阶段临时门限沿用 pre-fix memory / 背景 100,000 基线失败前的同类估计：1 次 / 5.887585059 秒 = 10.190936 次每分钟。该样本只有 1 个事件、5.888 秒且被失败截断，不是 pre-fix soak 测量，更不是可靠的稳定频率或置信上界。本次 74.591759 > 10.190936，约为临时目标 7.32 倍，**未通过**。不改用较宽松的 b5 soak 128.381798 次每分钟作为通过门限。

#### 尖峰簇间隔

每个阈值独立按 received_ns 聚类：相邻超限样本间隔 ≤10ms 归同簇，>10ms 新开一簇。间隔是相邻簇的第一个超限样本接收时刻之差；至少 5 个完整间隔才输出分布，否则 null。簇之前/之后的未完整观测等待不计入间隔。这些是描述统计，不证明固定周期或 checkpoint 的因果归属。

| 阈值 | pre-fix soak | b5 历史：簇数 / 间隔数；p50 / p95 / max（ms） | 本次：簇数 / 间隔数；p50 / p95 / max（ms） |
| --- | --- | --- | --- |
| >2ms | 缺数据 | 1,347 / 1,346；44.042 / 1,037.785 / 1,051.493 | 1,217 / 1,216；47.096 / 1,056.725 / 1,071.487 |
| >5ms | 缺数据 | 2,708 / 2,707；28.785 / 1,009.340 / 1,042.969 | 2,381 / 2,380；32.134 / 1,027.977 / 1,061.962 |
| >10ms | 缺数据 | 633 / 632；29.100 / 1,041.069 / 2,080.905 | 369 / 368；1,031.317 / 1,061.857 / 3,132.811 |

本次 >10ms 共 369 簇、368 个间隔，中位间隔 1,031.317ms，p95 1,061.857ms，最大 3,132.811ms；相比 b5 的中位 29.100ms 有明显变化。不过簇分组对 10ms 静默阈值敏感，不能仅据此宣称所有残余尖峰严格每秒发生一次。

#### 分钟桶对比

以下每个单元为“b5 历史次数 → 本次次数”。第 1–5 桶各真实暴露 60 秒，因此次数数值也等于该桶次/分钟；按各运行计量起点对齐，不按墙钟时间对齐。

| 计量时间窗 | >2ms：b5 → 本次 | >5ms：b5 → 本次 | >10ms：b5 → 本次 |
| --- | ---: | ---: | ---: |
| [0, 60) 秒 | 1,476 → 1,850 | 169 → 111 | 90 → 38 |
| [60, 120) 秒 | 36,545 → 37,218 | 176 → 130 | 119 → 67 |
| [120, 180) 秒 | 59,692 → 59,606 | 377 → 387 | 129 → 72 |
| [180, 240) 秒 | 60,001 → 60,090 | 1,295 → 1,119 | 140 → 87 |
| [240, 300) 秒 | 59,956 → 60,002 | 9,191 → 10,190 | 164 → 109 |

两次的 >2ms 都在第 2 分钟明显增加，第 3–5 分钟几乎覆盖全部样本；>5ms 都在最后一分钟大幅增加，本次从第 1 分钟 111 次升至第 5 分钟 10,190 次，历史从 169 次升至 9,191 次。>10ms 在每个完整分钟均低于历史，但本次仍从 38、67、72、87 上升至 109 次。由此可见，本次降低了较大的 >10ms 尖峰频次，长时间运行下更普遍的可见延迟抬升仍存在；这仅是时间分布观察，尚未定位该趋势的根因。

最后不足一分钟的尾桶必须单独看：b5 仅暴露 0.042535 秒，>2/5/10ms 为 47/14/0 次；本次仅暴露 0.033145 秒，为 37/9/0 次。派生 JSON 对尾桶仍按真实暴露计算频次，但该窗口极短，不用于趋势推断或外推 SLA。完整一分钟里个别 >2ms 计数超过 60,000，也不表示统计错误：目标速率是绝对时刻排程，调度落后后可能在局部时间窗追赶。

#### 原始基准退出原因

本次 summary 的整体 `exit_code=1`、`passed=false`，并非运行故障。具体失败项原文：

- `visible_p50_micros=[2879.595] exceeds limit=[1000.000]`
- `visible_p99_micros=[5711.784] exceeds limit=[3000.000]`
- `visible_p99_9_micros=[10906.406] exceeds limit=[10000.000]`

即 visible p50=2,879.595µs >1,000µs；p99=5,711.784µs >3,000µs；p99.9=10,906.406µs >10,000µs。memory ack 不适用 D 层持久确认门限，因此没有把 memory ack 改判为 WAL 合格或不合格。>10ms 频次临时目标是另行分析判定，不是上述原脚本退出原因。

### 确定性回归：先在 b5bf6d3 取证，再修复

新增慢持久化测试使用真实 Parquet writer、PersistController 和 SqliteCatalog，只在
`#[cfg(test)]` 模块中包装 CatalogPersistence：到达 Catalog 提交前通过 oneshot 通知主测试，
直到主测试放行才继续提交。收到通知后才启动同表写入；写入完成后验证 live offset 连续，
再释放持久化。2 秒 timeout 仅用于失败兜底，不用 sleep 制造交错。
见 `crates/zippy-ingest/src/materializer/tests.rs:143`、
`crates/zippy-ingest/src/materializer/tests.rs:485`。

修改生产代码前，HEAD 为 `b5bf6d3e10bcd51eb7b34decb22db3091e4ab30e`，
生产文件保持原样，仅加入上述三个测试。实际命令与输出：

```text
PYO3_PYTHON=$PWD/.venv/bin/python cargo test -p zippy-ingest checkpoint_slow_persistence -- --nocapture
same-table write must complete while checkpoint Catalog persistence is paused kind=[Frame]
same-table write must complete while checkpoint Catalog persistence is paused kind=[Append]
same-table write must complete while checkpoint Catalog persistence is paused kind=[KeyValue]
test result: FAILED. 0 passed; 3 failed; ... finished in 2.08s
exit_code=101
```

失败日志为 `evidence/2026-10-10-critical-section/slow-persistence-b5bf6d3-failure.log`；
对应 metadata 保存生产提交、当时测试源码、测试二进制和日志的 SHA-256。
同一旧实现上的原三个 publication 交错测试仍然全部通过，见
`publication-interleaving-b5bf6d3.log`。

修复后，`cargo test -p zippy-ingest --lib -- --nocapture` 为 73 passed、2 ignored、0 failed。
三类慢持久化测试、三个旧交错测试、三个真实 Prepared 查询/恢复测试、六个 Actor
生命周期测试及单 blocking worker 测试均通过，见 `checkpoint-fixed.log`。
新 Prepared 测试位于独立模块，只有新生产代码引用该模块；慢持久化三测试不依赖新的
production hook/API，恢复 b5bf6d3 的生产文件后仍可单独编译并复跑上述失败命令。

开发期间新加的单 blocking worker 测试曾因在 Tokio runtime 外构造 timeout 报
`there is no reactor running`，当次为 16 passed、1 failed。该测试构造错误已修复，
原输出保存在 `checkpoint-test-harness-initial-failure.log`，没有将其冒充旧实现回归证据。
Python 扩展重建前也曾出现 413 passed、1 failed：旧扩展 revision 为 `7c6b6a9...`，
HEAD 已是 `b5bf6d3...`；重建后 414 项全部通过，原失败单独保留。

### 构建、测量与验证命令

所有命令从仓库根目录执行；四格和浸泡顺序运行，其间没有 Cargo 构建或测试任务。
没有改变任何基准延迟门限、batch 大小、前景并发、checkpoint 周期或 wire 参数。

```bash
PYO3_PYTHON=$PWD/.venv/bin/python cargo build --release -p zippy-server --bin zippy-server
UV_CACHE_DIR=/tmp/zippy-uv-cache PYO3_PYTHON=$PWD/.venv/bin/python \
  uv run --no-sync maturin develop --release

UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/bench_public_e2e.py \
  --durability memory wal --background-rate 0 100000 --rate 1000 --samples 20000 \
  --warmup-seconds 2 --warmup-samples 1000 --background-batch-rows 1000 \
  --timeout-seconds 600 \
  --report-json docs/perf_reports/evidence/2026-10-10-critical-section/baseline.json \
  --raw-samples-json /tmp/zippy-perf-raw/2026-10-10-critical-section/baseline.json

UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/bench_public_e2e.py \
  --durability memory --background-rate 100000 --rate 1000 --samples 300000 \
  --warmup-seconds 2 --warmup-samples 1000 --background-batch-rows 1000 \
  --timeout-seconds 900 \
  --report-json docs/perf_reports/evidence/2026-10-10-critical-section/soak.json \
  --raw-samples-json /tmp/zippy-perf-raw/2026-10-10-critical-section/soak.json
```

| 验证命令 | 结果 |
| --- | --- |
| `PYO3_PYTHON=$PWD/.venv/bin/python cargo fmt --all -- --check` | 通过 |
| `PYO3_PYTHON=$PWD/.venv/bin/python cargo clippy --workspace --all-targets -- -D warnings` | 通过 |
| `PYO3_PYTHON=$PWD/.venv/bin/python cargo test --workspace -j 4` | 91 组，1221 passed、7 ignored、0 failed |
| `PYO3_PYTHON=$PWD/.venv/bin/python cargo test -p zippy-ingest --lib -- --nocapture` | 73 passed、2 ignored、0 failed |
| `UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync pytest pytests -q` | 414 passed |
| `UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync pytest clients/python/tests -q` | 247 passed，覆盖率 85.45% |
| 新分析脚本和测试的 ruff、逐文件 black、mypy | 全部通过；分析脚本测试 18 项已包含于 pytests |
| release server / extension | 均通过，23.07s / 24.68s |

基准脚本的实际退出码及原始门限失败原因保留在各自 JSON / log 中。
本节的相对频次验收独立于脚本原有延迟门限；不能把完整性通过解释为性能通过。

### 证据、复算与改动范围

证据目录为 `docs/perf_reports/evidence/2026-10-10-critical-section/`。
`comparison.json`（144,669 字节）同时保留三份历史/两份本次 raw 的身份、真实暴露窗口、
超限计数、每分钟频次、分钟桶与簇间隔；所有 raw 和原 summary 的 metadata/config/统计
均核对一致，并从 raw 重算分位数。pre-fix raw 的 SHA 另外与既有摘要中预先记录的
`raw_artifact.sha256` 相符；其余 SHA 是本次计算，不能冒称早已独立认证的历史哈希。

新增分析脚本拒绝不一致输入、覆盖输入文件以及超过 300,000 字节的输出。
复算命令如下：

```bash
UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync python scripts/analyze_public_e2e.py \
  --run pre-fix \
    /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/baseline.json \
    docs/perf_reports/evidence/2026-10-08-public-path-e2e/pre-fix-baseline.json \
  --run b5bf6d3-baseline \
    /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/post-fix-baseline.json \
    docs/perf_reports/evidence/2026-10-08-public-path-e2e/baseline.json \
  --run b5bf6d3-soak \
    /tmp/zippy-perf-raw/2026-10-08-public-path-e2e/post-fix-soak.json \
    docs/perf_reports/evidence/2026-10-08-public-path-e2e/soak.json \
  --run current-baseline \
    /tmp/zippy-perf-raw/2026-10-10-critical-section/baseline.json \
    docs/perf_reports/evidence/2026-10-10-critical-section/baseline.json \
  --run current-soak \
    /tmp/zippy-perf-raw/2026-10-10-critical-section/soak.json \
    docs/perf_reports/evidence/2026-10-10-critical-section/soak.json \
  --report-json docs/perf_reports/evidence/2026-10-10-critical-section/comparison.json
```

`acceptance.json` 给出各格逐项判定；`verification.json` 保存命令退出码、测试结果和当前
改动源码 SHA。`slow-persistence-tests.patch` 可应用到 b5bf6d3 后只运行
`checkpoint_slow_persistence`，不依赖两阶段生产代码。

本次 raw 文件为 baseline 15,060,015 字节、soak 53,034,031 字节，均留在指定 `/tmp` 目录。
两份 raw 扫描中 `SEQUENCE_GAP` 字符串出现次数均为 0，所有 worker/server 退出码均为 0，
端标记和计量样本数完整。完整性和原有延迟门限分别判定，两个基准主脚本实际都退出 1。

每个入库证据文件不超过 300,000 字节，`SHA256SUMS` 覆盖目录内其他所有证据文件；
`RAW-SHA256SUMS` 另覆盖两份本次原始样本。

```bash
cd docs/perf_reports/evidence/2026-10-10-critical-section
sha256sum -c SHA256SUMS
sha256sum -c RAW-SHA256SUMS
```

改动文件清单如下；历史 evidence 目录和本报告既有段落未改。

| 文件 | 用途 |
| --- | --- |
| `crates/zippy-ingest/src/actor.rs` | 私有两阶段 receipt 状态、预留发布通道、permit 生命周期与内存裁剪 |
| `crates/zippy-ingest/src/materializer.rs` | 持久等待移至锁外，短发布/前缀回收及锁外析构 |
| `crates/zippy-engines/src/stream_table.rs` | 缓存专用退役移交及资源生命周期测试 |
| `crates/zippy-segment-store/src/catalog.rs` | 退役 owner 移交，延迟文件资源析构 |
| `crates/zippy-engines/src/lib.rs`、`crates/zippy-segment-store/src/lib.rs` | 跨 crate 隐藏实现类型导出 |
| `crates/zippy-ingest/src/materializer/tests.rs` | b5 可复现失败的三个测试、单 blocking worker 回归 |
| `crates/zippy-ingest/src/materializer/prepared_tests.rs` | 真实 Catalog/Parquet 中间态查询、retirement 与恢复 bootstrap |
| `crates/zippy-ingest/src/actor/checkpoint_tests.rs` | 六项两阶段状态及生命周期测试 |
| `crates/zippy-ingest/Cargo.toml`、`Cargo.lock` | 仅增加测试读取 Parquet 的 workspace dev dependency |
| `scripts/analyze_public_e2e.py`、`pytests/test_analyze_public_e2e.py` | 可复算尖峰统计与 18 项边界/一致性测试 |
| `docs/perf_reports/2026-10-08-public-path-e2e-baseline.md` | 仅追加阶段 1.6 记录 |
| `docs/perf_reports/evidence/2026-10-10-critical-section/` | 本次摘要、检查日志、前后失败/通过证据和哈希 |


### 发现、未完成项与结论边界

本次完成了两阶段持久化及 finalization 临界区的缩小，保留 SEQUENCE_GAP、行数、receipt
校验；四格的 >10ms 频次要求仍有三格未通过，因此**不能宣布阶段 1.6 全部验收通过**。
没有重复运行后挑选更好的一轮替换这次结果，也没有修改历史数据或提高通过门限。

本次 >5ms 簇的间隔中位数仍约为 1.024 / 1.034 / 1.024 / 1.048 秒，
与 1 秒 checkpoint 周期相容；>10ms 样本已更稀疏，不能由其中少量间隔证明稳定周期。
没有加入生产逐阶段 tracing，也没有做因果干预，因此不能把残余尖峰全部归因于以下某一项。

- 初始 `prepare_checkpoint` 仍在 materializer state 锁内 seal/rollover 并做普通 publication，
  这是原有路径。见 `crates/zippy-ingest/src/materializer.rs:270`。
- Native checkpoint 在 authority state 锁内执行 WAL commit、准备 materializer checkpoint
  以及 WAL rotate，仍可能阻塞同表写入；见 `crates/zippy-ingest/src/native.rs:2378`、
  `crates/zippy-ingest/src/native.rs:2404`、`crates/zippy-ingest/src/native.rs:2426`。
- ReceiptArchive 使用 SQLite WAL + FULL，同一 archive mutex 还服务请求索引读取。
  checkpoint archive 与写入侧 archive 查询有共享锁；见
  `crates/zippy-ingest/src/checkpoint.rs:36`、`crates/zippy-ingest/src/checkpoint.rs:90`、
  `crates/zippy-ingest/src/native.rs:2467`、`crates/zippy-ingest/src/native.rs:2557`。
  这是静态识别的后续测量候选，尚未测得它占本轮尖峰的比例。
- 跨 crate 的两个 hidden helper 和 guard 在 Rust 层技术上是新增 pub 表面；已有 API
  签名、客户端协议及格式未变。如果“不得改变公开 API”包括禁止一切内部跨 crate pub
  增量，此项仍需调整接口边界，不能用 `doc(hidden)` 声称满足字面上的零增量。
- 恢复验证没有执行独立 server kill/restart；本次确定性测试覆盖真实 Catalog 状态重建，
  完整 WAL 恢复仍由既有测试与源码论证支持。
- 历史 pre-fix 三格被失败截断，尤其 memory 的频次参考只有 0 / 1 个事件，估计不稳定。
  WSL2 主机上的结果也不能充当裸机或本机 Rust SHM 100–300µs SLA 的证明。

原有 CI 延迟门限和配置未改；本次没有 commit、push 或切换分支。


## 可见延迟随时间增长（阶段 1.7，2026-10-10）

本轮结论：已去除已追上订阅每次刷新时对本表全历史清单的重复读取；确定性回归及全部指定功能检查通过。最终有背景十分钟趋势满足门限（2.024µs/min、末/首0.984），但无背景五分钟斜率22.576µs/min及四格尾部对比未全面达标，**整体验收未通过**；详见本节末的最终表和 `final-acceptance.json`。

本节追加修复前 control-a/control-b、历史趋势复算、最小实现及验证，前述阶段的历史数据不变。功能验证与性能验收分别记录，不以工作量回归通过代替长时延迟验收。

### 分钟统计口径与可复算身份

在原始双条件预热 mask 后，以第一条实际计量 sent_ns 为共同起点；按实际 sent_ns 划分左闭右开 60 秒桶，ack 与 visible 的同一行归同一个发送分钟。各延迟分位数沿用 nearest-rank，>10ms 是严格大于。所有分桶使用真实时间，不把“60,000 行”当作“一分钟”。

发送覆盖终点是最后一条计量 sent_ns；某桶必须完整覆盖 60 秒才进入趋势判定。尾桶仍展示 count/p50/p99/>10ms，但不参与 OLS 或首末比例：短窗口样本量和暴露时长与完整分钟不可比，慢响应也不能把发送覆盖延长。若最后一条恰好位于分钟边界，则属于一个零发送覆盖的尾桶，不据此制造额外完整分钟。

OLS 对各完整分钟 p50 按实际分钟中心作无权线性回归，单位 µs/min，保留有符号斜率；至少 2 个有样本的完整分钟才估计。门限同时要求 slope≤20µs/min、最后完整分钟 p50 / 第一完整分钟 p50≤1.15；源运行必须完整且没有空的完整分钟才能通过。首分钟 p50=0 时比例未定义，不判通过。趋势判定与原 C/D 延迟门限分别报告。新增分析输出位于每格 `minute_trends.buckets` 和 `minute_trends.p50_trends`；分析 CLI 的退出 0 仍仅表示报告成功生成，自动化验收应读取 `p50_trends.visible_latency.passed` 并额外检查所需完整分钟数（10 或 5）。新增 20 个单元测试覆盖真实发送分桶、预热、部分尾桶、边界样本、正负斜率、比例、空桶与不完整运行；该分析测试文件共 38 项通过。

既有分析中的超限“每分钟桶”按 received_ns 分桶，其暴露终点也是最后接收时刻；这部分保留兼容、不覆写历史结果。新 minute_trends 明确按 sent_ns，跨分钟才收到的慢样本可能与旧桶不同。用户监督数据若按 received 分钟聚合，应先对齐口径再比较，不能把边界小差异当作样本丢失。

固定 N/目标速率只能近似持续时长。例如 180,000 / 1,000 并不保证从首发到末发覆盖满 180 秒，运行可能只含 2 个完整分钟；本次 control-a 实际为 180.010450329 秒，得到 3 个完整分钟和 10.450329ms 尾桶。后续计划以 600,100 / 300,100 样本留约 0.1 秒余量，面向 10 / 5 个完整分钟；最终仍以真实 sent 覆盖和完整桶数为准，不能仅凭样本数填“10 分钟通过”。

### 旧趋势与 control-a/control-b 对照

| 数据集 | 配置 | 完整分钟 | visible p50 OLS（µs/min） | visible 末/首 | visible 趋势门限 | ack OLS（µs/min） | ack 末/首 | ack 趋势门限 |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | --- |
| b5bf6d3 收录的历史 soak | memory / 背景 100,000 | 5 | 693.9318 | 3.047523 | 未通过 | 14.7399 | 1.106208 | 通过 |
| e1e67b5 收录的 critical-section soak | memory / 背景 100,000 | 5 | 700.1403 | 3.052170 | 未通过 | 14.9046 | 1.110773 | 通过 |
| control-a-before（e1 生产，无背景） | memory / 背景 0 | 3 | 718.5290 | 2.040058 | 未通过 | 26.3970 | 1.066344 | 未通过 |
| control-b-before（e1 生产，有背景及诊断采样） | memory / 背景 100,000 | 3 | 754.6140 | 2.075679 | 未通过 | 33.2280 | 1.084720 | 未通过 |

归档标签不是测量时的 clean checkout 身份：b5bf6d3 收录的旧 soak metadata 是 `7c6b6a9… dirty=true`；e1e67b5 收录的 critical-section soak metadata 是 `b5bf6d3… dirty=true`；本次 control-a/control-b metadata 均是 `e1e67b5… dirty=true`。各原始 SHA、完整 metadata 和配置由对应 trends JSON 保留，不改写为发布提交上新测的数据。

两次历史 soak 的 ack 趋势较平，而 visible p50 每分钟增长约 0.7ms。control-a 无背景仍呈约 0.719ms/min 的 visible 增长，说明背景批量写入并非该趋势的必要条件。control-a ack 的3分钟OLS为26.397µs/min，超过20门限，不能称为所有ack场景均已达标；其末/首仍只有1.0663。上述是观测对照，不单独证明耗时具体落在哪一跳。

#### control-a 分钟明细

| 发送时间窗 | 样本数 | ack p50 / p99（µs） | ack >10ms | visible p50 / p99（µs） | visible >10ms | 参与趋势 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0, 60.000000000) 秒 | 60,001 | 795.766 / 1478.049 | 45 | 1381.709 / 2704.121 | 49 | 是 |
| [60, 120.000000000) 秒 | 60,000 | 854.607 / 1280.839 | 74 | 2093.864 / 3316.874 | 76 | 是 |
| [120, 180.000000000) 秒 | 59,987 | 848.560 / 1416.813 | 74 | 2818.767 / 4214.934 | 79 | 是 |
| [180, 240) 秒，观测至 180.010450329 秒 | 12 | 817.945 / 1110.857 | 0 | 2977.006 / 3904.362 | 0 | 否，尾桶 |

control-a 计量 ack/visible 各180,000；含预热182,000行全部收到，缺口/重复/乱序/意外序号均为0，ordered end marker完整；实际前景999.937749行/秒，背景0，所有进程正常退出。原基准整体exit=1，仅由以下原始C层延迟门限造成：

- `visible_p50_micros=[2113.357] exceeds limit=[1000.000]`
- `visible_p99_micros=[3923.731] exceeds limit=[3000.000]`
- `visible_p99_9_micros=[13970.027] exceeds limit=[10000.000]`

control-a 证据为 `docs/perf_reports/evidence/2026-10-10-latency-growth/control-a-before.json` 与 `control-a-before-trends.json`；trends文件20,181字节。历史趋势保存在同一 evidence 目录的 `historical-soak-trends.json`，其原字段已逐项验证与归档 comparison 一致，仅新增分钟趋势；`control-before-trends.json` 同时保存本次 A/B 对照、元信息差异和原始样本 SHA。新 evidence 本段中的简称均相对于 `docs/perf_reports/evidence/2026-10-10-latency-growth/`。

#### control-b 分钟明细与完整性

| 发送时间窗 | 样本数 | ack p50 / p99（µs） | ack >10ms | visible p50 / p99（µs） | visible >10ms | 参与趋势 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0, 60) 秒 | 60,001 | 784.418 / 1208.959 | 40 | 1403.047 / 2216.414 | 47 | 是 |
| [60, 120) 秒 | 59,999 | 862.721 / 1374.690 | 59 | 2212.046 / 3566.725 | 68 | 是 |
| [120, 180) 秒 | 60,000 | 850.874 / 1385.802 | 59 | 2912.275 / 4282.177 | 75 | 是 |
| [180, 240) 秒，观测至 180.098918507 秒 | 100 | 882.189 / 1442.167 | 0 | 3208.078 / 4544.651 | 0 | 否，尾桶 |

control-b 计量 ack/visible 各 180,100；含预热 182,100 行全部收到，缺口/重复/乱序/意外序号均为 0，ordered end marker 完整，所有进程退出 0。前景实际 999.999214 行/秒，背景 99,999.934861 行/秒。原基准整体 exit=1，仅由原 C 层 visible p50=2187.854µs>1000、p99=4045.516µs>3000、p99.9=10223.684µs>10000 造成。新趋势门限单独判定：visible 斜率和比例均失败；ack 斜率失败、比例通过，不能遗漏 ack 的失败项。

A/B 使用同一 release server 和扩展 SHA、同一原基准脚本 SHA，但 B 启用了边界和资源诊断且增加 100 个计量样本；因此二者不是只有背景速率单变量不同的严格开销实验。两者和旧两次 soak 均出现增长，诊断用于定位增长边界，不能据此量化背景写入的独立成本。

### control-b 边界与资源诊断

诊断证据为 `control-b-before-diagnostic-analysis.json`，原始边界/资源 JSONL 留在 `/tmp/zippy-perf-raw/2026-10-10-latency-growth/` 并由证据记录输入身份。每 1,000 行固定采样一次，约 1Hz，可能与 checkpoint 周期有相位关系；下述结论限定在采样点，不能覆盖全部客户端慢事件。总计 183 个边界样本，原预热 mask 后 181 个，三个完整分钟分别为 61 / 59 / 60 个，尾桶 1 个。下表是稀疏诊断样本，不替代上方完整 180,100 行统计。

| 边界 | 第 1 / 2 / 3 完整分钟 p50（µs） | p50 OLS（µs/min） | 末 / 首 |
| --- | --- | ---: | ---: |
| sent → read_chunk 返回 | 1402.111 / 2081.972 / 2995.439 | 796.6640 | 2.136378 |
| read_chunk → next 返回 | 36.280 / 33.003 / 32.041 | -2.1195 | 0.883159 |
| next 返回 → 基准 received | 5.220 / 4.779 / 4.789 | -0.2155 | 0.917433 |
| 采样样本总 visible | 1444.903 / 2114.013 / 3037.850 | 796.4735 | 2.102459 |

增长主要发生在 read_chunk 返回以前；被单独测量的 Python 日志元信息校验/event 构造后缀，以及 next 返回交接至 received 读钟的段，没有同向 p50 增长。received 之后的 to_pylist 和列表追加未被单独计时；它们造成的后续消费积压仍可能落入下一行的 sent→read_chunk，不能仅凭分界采样排除所有客户端累积。sent→read_chunk 仍包括服务端处理/等待、本地传输和客户端 C++ Arrow 解码，**不是已观测的 server-send 时间戳**，不能仅凭此表把上游所有增长归给服务端。每事件都经过包装和 modulo 判断，采样事件额外两次读钟及字典插入；结果消费完成后输出。稀疏分钟 p99 仅供描述，不能用于替代完整基准 p99 验收。

资源采样每约 60 秒一次，以下是监控经过时间：elapsed=0 的原点比首计量 sent 早 2.519452273 秒；首资源快照 elapsed=0.301095862，位于计量起点前 2.218356411 秒。实际采样时间保存在证据内。CPU 为累计 CPU 秒及相邻快照差，不是 CPU 百分比，也不是精确计量分钟桶。

| 监控经过秒 | probe 持久行 / files / lineage | 背景持久行 / files / lineage | server 累计 CPU 秒 | server 本间隔 CPU 秒 | subscriber 累计 CPU 秒 | subscriber 本间隔 CPU 秒 | server mapped segment 文件 |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 60.304 | 59,215 / 57 / 57 | 5,922,000 / 57 / 113 | 59.70 | 59.68 | 13.61 | 13.47 | 2 |
| 120.308 | 119,239 / 114 / 114 | 11,926,000 / 114 / 227 | 136.49 | 76.79 | 26.68 | 13.07 | 2 |
| 180.312 | 179,405 / 171 / 171 | 17,941,000 / 171 / 341 | 216.09 | 79.60 | 37.51 | 10.83 | 2 |

同三次快照中，采样保留的 server TID 22（comm=zippy-catalog-s）单线程累计 CPU 为 17.14 / 48.09 / 83.06 秒，相邻增量约 17.14 / 30.95 / 34.97 秒；TID 21 的 control 线程累计 CPU 另为 3.88 / 7.40 / 10.06 秒。线程采样只保留 CPU 最高的 12 个，不能把未列出的线程当作 0。server RSS 为 89,763,840 / 95,543,296 / 99,389,440 字节。持久 files/lineage 持续增加，server CPU（尤其 Catalog 线程）随时间增加，而 subscriber CPU 没有同向增长；这与重复历史快照工作的代码证据相符。mapped segment 文件在这三个快照均为 2，但结束快照为 3，不能泛化为全程恒定 2。

资源采样 `monitor_errors=[]`。独立 Catalog 只读查询有 50ms progress budget，非跨表原子快照；每次整个快照约 4.6–6.7ms。末次采样时部分 worker 已正常退出，保留 FileNotFound 而不当作运行失败或伪填 0 CPU/RSS。writer/subscriber 保留原始样本整数列表，RSS 增长不能直接证明泄漏；server RSS 增长也需要所有权/预算证据才能归因。

### 现有刷新路径与确定性工作量证据

修复前订阅窗口每次 refresh 都调用 `NativeQuerySnapshotCoordinator::pin_persisted` 再构造 log_plan。e1e67b5 的 `native_log.rs` 中 `NativeLogWindow::refresh` 先取得 snapshot_refresh_token、admit_event_window，随后无条件 pin_persisted；`query_snapshot.rs` 的 pin_persisted 沿既有Catalog快照路径读取、固定并核算持久文件和lineage等元信息。即使订阅游标已追到持久前缀之后，仍可能携带与本表历史文件数量相关的快照工作。这里描述的是修复前 e1e67b5 的代码路径，避免将正在实现的路径与旧生产混同。

在 production files 与 e1e67b5 校验一致时，确定性回归使用相同单行live刷新，改变本表或同namespace背景表历史文件数，得到：

| 本表历史文件 | 背景表历史文件 | active_snapshot_memory_bytes | 判定 |
| ---: | ---: | ---: | --- |
| 1 | 1 | 1,272,296 | 小历史对照 |
| 256 | 1 | 8,391,176 | 历史无关性断言失败 |
| 1 | 256 | 1,272,296 | 背景历史隔离断言通过 |

该证据证明单行live刷新保留的快照核算字节随**本表**持久历史增加；从1到256个文件增加7,118,880字节，总量约为原来的6.60倍，而同namespace背景表的256文件不增加此指标。它衡量快照核算工作/持有量，不是RSS，不是逐跳实测耗时，也不能独自归因全部0.7ms/min。结合无背景 control-a 仍增长、control-b 的增长位于 Python 后缀之前，以及 Catalog 线程 CPU 随历史增长，这确定了本次最小修复目标：每次 live 刷新重复读取和保留全历史快照。工作量依赖已经由确定性测试证明；该依赖具体解释多少延迟增长，仍须修复后的同工作量回归及长时干预对照验证，不能先行归因全部 0.7ms/min。

证据：`history-refresh-before.log`（1 passed、1 failed、预期exit101）、`history-refresh-before-metadata.json`（生产文件与e1的SHA一致及命令/日志身份）、`history-regression-test.patch`。此失败是新增不随历史增长约束在旧代码上的预期回归证据，不应改写为旧功能完整性失败。

### 最小方案及边界

服务端内部新增私有live日志快照。每次基于一个一致Actor观察判定：只有订阅的下一条inclusive cursor严格大于Actor committed watermark时，才能用Actor快照、固定Catalog表元信息/revision，以及cursor之后的live suffix构建窗口，不读取该表全部历史files/lineage。若cursor仍需要持久文件回放，则走既有完整快照路径。Empty committed、latest-only start和边界相等情况必须有明确分支；不能把catalog最新committed代替Actor当前观察。

该方案针对已追上的订阅刷新，不改变公开Client API、Flight wire、文件格式或持久化语义。全历史查询、落后消费者的Parquet回放保留既有能力。工作量目标是去除“随历史持久文件总数增加”的部分；仍允许与当前所需live suffix大小相关，不能承诺所有窗口永远恒定字节或零分配。

| 关注点 | 必须维持的约束 / 最终验收点 |
| --- | --- |
| schema | 订阅固定schema/kind；Catalog 的 schema/snapshot 版本可领先 Actor、不能落后，writer_epoch 要相等。演进后沿既有SchemaMismatch终止，不能用旧schema解码新live span。 |
| drop / 生命周期 | 按不可变table_id及Active状态验证，保留table actor生命周期、窗口租约和RowSpan所有权；drop/retire期间不输出悬空capability，也不重新绑定同名新表。 |
| cancel / shutdown | 快速路径仍由QueryService的查询实例、generation、cancellation/deadline与refresh token管理；取消或关闭后停止等待与交付，预算随实际请求/快照所有者释放；已入队或开始的后台请求仍可能完成并回收结果（`crates/zippy-ingest/src/actor.rs:1819`）。 |
| budget | 固定元信息、live描述/行视图、结果解码均纳入现有内存预算与所有权账本；不能跳过charge来“通过恒定工作量测试”。预算耗尽必须按现有错误契约失败。 |
| fencing / epoch | Catalog 与 Actor 的 writer_epoch 相等；segment identity/generation 固定，segment 的 writer_epoch≤Actor、schema_version 与 Actor 相等；fencing切换不能混用旧epoch描述或以无效cursor跳过行。 |
| watermark / cursor | 同一Actor观察固定committed/max/live范围；窗口watermark不得回退，cursor之前不重放、之后不漏发，空窗仍保留先登记watch后捕捉边界的防丢唤醒顺序。 |
| prepared checkpoint | Catalog已提交而Actor尚未publish时，查询仍以Actor的persisted边界与live范围为准；不能将Catalog最新文件和旧Actor live组合造成重复。 |
| lagging fallback | cursor≤Actor committed 时回到原持久快照/Parquet+live路径；start=None 也走原路径。live 缺口、重叠或错误 watermark 直接报 DataCorruption，未来 cursor 或 Frame 内部 cursor 报 InvalidArgument，不用回退掩盖损坏。 |
| frame / KV | 保留原 Frame/KV 原子边界标记、序号与切片规则；消费者仍须拼齐完整 frame/mutation 才能作为原子业务变更使用（`crates/zippy-server/src/native_log.rs:65`），不暗示每个推送 batch 都是完整 mutation。 |

### 实际实现、复杂度及安全边界

根因的具体历史读集位于 `crates/zippy-catalog/src/snapshot_domain.rs:580`、`crates/zippy-catalog/src/snapshot_domain.rs:603`：逐文件解码并读取 lineage；`crates/zippy-catalog/src/pinned_snapshot.rs:361` 对完整快照序列化计费。修复前 e1e67b5 的 `crates/zippy-server/src/native_log.rs:153` 每次 refresh 无条件进入该路径；当前该位置已改为优先选择 live 路径（`:150`）。旧完整历史路径仍保留，因此可从当前 `crates/zippy-server/src/query_snapshot.rs:1321` 的 log_plan 查看先构造全部文件 source、最后裁剪 cursor 的流程。

| 当前源码位置 | 改动与约束 |
| --- | --- |
| `crates/zippy-server/src/native_log.rs:150` | 有显式 cursor 时尝试 live 快照；仍需要持久前缀则回到旧 full 路径；两条路径都拒绝 watermark 回退。 |
| `crates/zippy-server/src/live_log_snapshot.rs:82` | 按固定 table_id 解析 Catalog descriptor、观察 Actor，再读取实际 Catalog revision。schema/epoch 使用原校验，Catalog 可领先，但不能落后于 Actor。 |
| `crates/zippy-server/src/live_log_snapshot.rs:140` | snapshot 与 plan 分别预留、持有与自身存活期对应的预算；保留原协调峰值，未靠取消内存计费通过回归。 |
| `crates/zippy-server/src/live_log_snapshot.rs:168`、`crates/zippy-server/src/live_log_snapshot.rs:255` | 先验证覆盖并按 cursor 裁剪，再只扫描本次选中 suffix 的 UTF-8 offsets；损坏使用原 `cannot bound live UTF-8 values` 错误映射。 |
| `crates/zippy-server/src/native_log.rs:527` | 共用完整性校验从指定持久边界的下一 offset 开始；完整历史入口仍从 0 校验。Frame 身份、边界、缺口和未来 cursor 校验保留。 |
| `crates/zippy-server/src/query_service.rs:3483` | 两种快照复用同一个安装函数，保留 running/drained、查询实例与 reservation 归属、schema/版本单调、服务状态和 owner 上限检查。 |

新快照只持有固定 RowSpan/Arc 和预算，无持久文件 capability，不需要 Catalog 文件 GC pin；checkpoint 随后回收 Actor 前缀不影响已经持有的 span。Prepared checkpoint 期间 Catalog 可领先，快路仍以同一次 Actor 观察的 committed/max/live 范围为准。schema/drop/shutdown 仍由原 changes/watch 与交付前检查终止；Frame checkpoint 必须落在完整 frame 末尾，KV 必须落在 BATCH_END（`crates/zippy-ingest/src/actor.rs:2079`、`crates/zippy-ingest/src/actor.rs:2123`），没有绕过这些写入侧约束。

令 H 为本表历史文件/lineage 数，S 为 Actor 当前持有并遍历的 live 段元数据数（包括观察时随后跳过或裁剪的持久前缀），U 为本次选中 suffix 的 UTF-8 offset 数。固定 schema 下，新 live 规划为 Append/KV O(S+U)，Frame 为 O(S log S+U)，不再读取或保留 O(H) 的历史清单。Actor 快照仍创建当前可见 span 数组，见 `crates/zippy-ingest/src/actor.rs:1269`；S 的既有硬上限为 1024（`crates/zippy-ingest/src/actor.rs:29`、`crates/zippy-ingest/src/actor.rs:1353`）。不能把该实现描述成所有场景严格 O(新增行数)：当前 live 元数据仍全部检查，Frame 使用 BTreeSet，schema 宽度及 Catalog 控制队列也有成本。

首次 `prepare_log_window` 仍完整绑定历史（`crates/zippy-server/src/native_query.rs:358`）；follow 注册 watch 后的权威刷新，若带显式且已追上的 cursor，可以选择 live。start=None 或 cursor≤Actor committed 的回放仍保留完整历史路径。本次没有修改 Actor、Catalog、wire、持久格式或公开 API；新增符号只在 Server 内部使用。

### 确定性回归与功能验证

主要回归只修改原 `native_query_canary.rs` 的 cfg(test) 模块，使用 e1 已有接口，移植该测试文件或证据中的 patch 即可在 e1 编译。1→256 个历史文件通过一次额外 persist 创建，没有随文件数制造 255 次 Catalog 版本提交；固定 Int64 schema、一个 live span、一个输出行，避免把 UTF-8 payload 扫描混入该历史清单计数。

旧版失败摘录：

```text
one-row live refresh retained history: small=[1272296] large=[8391176]
test result: FAILED. 1 passed; 1 failed
```

修复后 `live-refresh-after.log`：

```text
live refresh history_files=[1] other_files=[1] active_snapshot_memory_bytes=[136446]
live refresh history_files=[256] other_files=[1] active_snapshot_memory_bytes=[136446]
live refresh history_files=[1] other_files=[256] active_snapshot_memory_bytes=[136446]
test result: ok. 6 passed; 0 failed
```

这是保留快照/计划的账本字节，不是 RSS、刷新峰值或 wall-clock 时间。回归核对输出值/offset/行数，并在 drop stream 后检查账本归零，见 `crates/zippy-server/src/native_query_canary.rs:2194`、`crates/zippy-server/src/native_query_canary.rs:2312`。另有 UTF-8 已消费前缀 2048 字节而选中新值 2 字节的上界回归，以及 live 后缀连续性、Frame 与未来 cursor 测试。

集成测试补强三条现有用例：同一订阅跨 writer epoch；三类 schema 演进后以既有 SchemaMismatch 终止；三类慢消费者跨四次 checkpoint，先回放 persisted+live，再追上后续 live，并核验行值、offset、Frame/KV 边界及释放后的内存。位置为 `crates/zippy-server/tests/native_storage_runtime.rs:44`、`crates/zippy-server/tests/native_storage_runtime.rs:3183`、`crates/zippy-server/tests/native_storage_runtime.rs:3381`。三条均在最终全 workspace 运行中通过。

开发期间缺少 QueryReadSnapshotGuard::into_any 的编译失败保留在 `live-refresh-initial-compile-failure.log`；并行子代理在源码写入中遇到同一错误，记录于 `integration-build-in-progress.log`。子代理的三类集成运行还遇到原有 Flight 监听端口绑定 EPERM；该失败日志保留，主执行环境的全 workspace 测试随后通过同一用例。上述开发/环境失败没有冒充 e1 上的确定性回归失败。

Prepared 状态对新 live owner 的精确暂停交错没有另加专项测试，安全论证来自共用 Actor 边界和原两阶段 checkpoint 测试；本轮新增测试不声称覆盖全部并发分支。

### 第一轮修复后四格：保留失败结果

证据为 `baseline-first-comparison.json`（95,649 字节）。该文件由原分析工具同时校验并复算旧 critical-section 基线与本次 baseline-after；原 raw/summary 没有覆盖。以下每个单元格写“旧 → 首轮修复后”，延迟单位 µs。

| 配置 | 端点 | p50 | p99 | p99.9 | max |
| --- | --- | ---: | ---: | ---: | ---: |
| memory / 背景 0 | visible | 1235.225 → 1284.912 | 1748.249 → 9261.024 | 6157.364 → 24231.985 | 14533.116 → 52628.646 |
| memory / 背景 0 | ack | 766.404 → 934.285 | 1102.931 → 4231.288 | 5534.270 → 19809.060 | 14051.882 → 43976.578 |
| memory / 背景 100,000 | visible | 1250.600 → 1409.590 | 1956.124 → 6957.274 | 8475.353 → 18678.962 | 16042.638 → 37995.509 |
| memory / 背景 100,000 | ack | 765.001 → 1000.361 | 1220.421 → 4181.477 | 7745.233 → 14823.676 | 15416.948 → 32065.179 |
| wal / 背景 0 | visible | 1332.277 → 1344.823 | 2321.517 → 4607.874 | 5178.351 → 17045.909 | 14788.946 → 44092.962 |
| wal / 背景 0 | ack | 2012.409 → 2411.572 | 3467.674 → 10546.941 | 6485.029 → 37437.286 | 13484.120 → 240018.116 |
| wal / 背景 100,000 | visible | 1349.946 → 1319.142 | 2235.049 → 3362.977 | 5235.842 → 8633.245 | 12153.181 → 35160.880 |
| wal / 背景 100,000 | ack | 2052.186 → 2390.348 | 3425.499 → 6418.051 | 6894.838 → 24873.606 | 12486.548 → 139482.514 |

| 配置 | 前景实测速率（行/s） | 背景实测速率（行/s） | visible >10ms 次数 | visible >10ms /min | 实际暴露秒数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory / 背景 0 | 1000.011 → 818.385 | 0.000 → 0.000 | 5 → 181 | 14.999685 → 444.376645 | 20.000419832 → 24.438728092 |
| memory / 背景 100,000 | 1000.018 → 827.809 | 99999.284 → 99999.426 | 8 → 96 | 23.999830 → 238.405675 | 20.000141281 → 24.160498736 |
| wal / 背景 0 | 477.115 → 348.551 | 0.000 → 0.000 | 8 → 57 | 11.450913 → 59.604688 | 41.918053711 → 57.378036949 |
| wal / 背景 100,000 | 465.648 → 372.544 | 100014.130 → 99999.753 | 5 → 14 | 6.984790 → 15.647169 | 42.950466812 → 53.683831463 |

两次四格均完整采集每格 20,000 个计量 ack/visible 样本，均无缺口、重复、乱序或意外序号，ordered end marker 完整；所有 worker/server 退出码为 0。两次原基准主脚本均退出 1，结构完整不等于延迟验收通过。>10ms 频次以首计量 sent 至末计量 received 的实际暴露归一化，严格 >10ms，未以名义 20 秒替代实际时长。全部窗口不足 60 秒，本表不能估计分钟趋势斜率。

本次各格的原 C/D 门限失败项：

- memory / 背景 0：`visible_p50_micros=[1284.912] exceeds limit=[1000.000]`；`visible_p99_micros=[9261.024] exceeds limit=[3000.000]`；`visible_p99_9_micros=[24231.985] exceeds limit=[10000.000]`。
- memory / 背景 100,000：`visible_p50_micros=[1409.590] exceeds limit=[1000.000]`；`visible_p99_micros=[6957.274] exceeds limit=[3000.000]`；`visible_p99_9_micros=[18678.962] exceeds limit=[10000.000]`。
- wal / 背景 0：`visible_p50_micros=[1344.823] exceeds limit=[1000.000]`；`visible_p99_micros=[4607.874] exceeds limit=[3000.000]`；`visible_p99_9_micros=[17045.909] exceeds limit=[10000.000]`；`wal_ack_p50_micros=[2411.572] exceeds limit=[2000.000]`；`wal_ack_p99_micros=[10546.941] exceeds limit=[5000.000]`。
- wal / 背景 100,000：`visible_p50_micros=[1319.142] exceeds limit=[1000.000]`；`visible_p99_micros=[3362.977] exceeds limit=[3000.000]`；`wal_ack_p50_micros=[2390.348] exceeds limit=[2000.000]`；`wal_ack_p99_micros=[6418.051] exceeds limit=[5000.000]`。

首轮四格 visible p99/p99.9/max 均较旧 critical-section 恶化，ack 四个分位也全部恶化，前景吞吐四格均下降；仅 wal/背景100k 的 visible p50 有小幅下降（1349.946→1319.142µs），仍高于原 1000µs 门限，不能以此掩盖其余失败。原始配置、门限和样本数保持一致，没有放宽门限或丢弃不利格。

数据身份及比较限制：

- `e1-critical-section`：git=`b5bf6d3e10bcd51eb7b34decb22db3091e4ab30e`、dirty=true，开始 UTC `2026-10-10T10:38:34.904725+00:00`；server SHA256=`49f9e1c924200ee23d2c741da6cfca693c5a6149f08e6bc7dd857ef99dff2736`；extension SHA256=`8aa8431af472f5adadc7f03324b840fde66dc18ac0a3c62e13eaeb3dd606e7d5`。原 raw SHA256=`0f446725df388dca5e4e9a32fe6eb281041a3e540f79712fd3d0a95652f624f2`。
- `baseline-after-first`：git=`e1e67b569d1f5ffb48f6a093c242f271608a8265`、dirty=true，开始 UTC `2026-10-10T11:22:58.203960+00:00`；server SHA256=`16b94d80e4b3127b9b7b69ce69ad0d18d27870224b061b3f0fd80a885c82c27d`；extension SHA256=`74c32f8d66409865dc892fd76624d4df79c4f9f74b22121d4e14861a2469678a`。原 raw SHA256=`e8f77f7737124acd5d77f68e929fbc63cdaccd7874c41d6a690be38b2b875fa1`。

旧列为 e1 收录的 critical-section 数据，测量 metadata 实际是 b5bf6d3 dirty；不是当前时段重跑的 clean e1。两次开始相差约 44 分钟，server/extension 二进制不同。仅由这一前后对照无法区分代码效应与主机负载/调度/存储状态变化；环境漂移也不能在缺乏对照时用于豁免首轮失败。同期对照另列如下，不替换本轮失败证据。

### 同期旧版/新版短对照

为检查首轮退化是否稳定复现，在相同主机时段顺序运行 memory/0 各 20,000 样本；未修改基准门限或采样。仅将四个生产文件临时替换为 git show e1e67b5 的内容构建旧版对照二进制，finally 按保存的原始字节恢复当前修改，再重建新版。分支与 HEAD 未变；新二进制重建前后的 SHA-256 完全相同（16b94d80…），证明下面和首轮使用相同实现，见 paired-build-metadata.json。原测试文件的生产部分没有改动；旧版构建不会编译 cfg(test)。

| 端点 | 同期 e1 p50 / p99 / p99.9 / max（µs） | 同期新版（µs） |
| --- | --- | --- |
| visible | 1410.006 / 3120.534 / 7052.390 / 18096.914 | 1141.498 / 2024.712 / 7271.563 / 16798.914 |
| ack | 856.262 / 2474.306 / 6370.340 / 17102.335 | 830.445 / 1578.881 / 6872.111 / 16060.826 |

两次各 20,000 样本与完整性均通过；前景分别约 1009.559 / 999.874 行/秒。新版 p50/p99 改善，但 visible 和 ack 的 p99.9 略差，不能据此宣布尾延迟全部不劣化。二者原 C 门限均失败、主脚本退出 1，见 paired-exit-codes.json 与 paired-comparison.json。新版首轮 memory/0 的 p99=9261µs、p99.9=24232µs，在代码和二进制未变时下降到本表水平；这证明短运行存在明显变化，但不足以把首轮全部退化归咎于环境，更不能删除首轮结果。

静态固定开销复核：对于已追上、单次无 Conflict 重试的刷新尝试，新旧均为两次 Catalog 请求与一次 Actor 请求；opening、重试和落后回放不能套用这个固定计数。旧版第二次请求进入 snapshot worker，新版 revision 进入 control worker。revision 仅按 catalog_meta 主键读取 8 字节值（`crates/zippy-catalog/src/service.rs:913`、`crates/zippy-catalog/src/service.rs:1908` 是派发；实际查询为 `crates/zippy-catalog/src/layout.rs:379`、`crates/zippy-catalog/src/layout.rs:386`，主键见 `crates/zippy-catalog/src/layout.sql:3`，8 字节解码见 `crates/zippy-catalog/src/encoding.rs:27`），没有隐藏的全表扫描。落后消费者需先做候选快路检查再完整回放，仍可能增加固定开销。长时验收使用原基准，不启用诊断 monkeypatch；其后预先安排再跑一轮完整四格，以检验结果稳定性，所有轮次保留。


### 计费修正前候选 v1 的十分钟结果

首轮候选（server SHA 16b94d80…）完成 600,100 个计量样本，实际发送覆盖 609.676086756 秒，有 10 个完整分钟。visible p50 OLS 为 **2.539952µs/min**，末/首 **1.023047878**；ack OLS 为 1.006842µs/min，末/首 1.001167277。两项 p50 趋势门限通过，含预热 602,100 行全部收到、完整性零错误，但原 C 延迟门限全部失败、主脚本 exit=1。全程 visible p50/p99/p99.9/max 为 1164.755 / 11680.840 / 103240.193 / 375478.372µs；>10ms 共 6,594 次、648.933549 次/分钟。第 2/8 个完整分钟的 p99 分别达 62.202/73.734ms，不能以趋势通过掩盖尾延迟恶化。

前景实际 984.291900 行/秒、背景 99999.972283 行/秒；schedule_lag p50/max 为 5.226/19.067 秒。此滞后是实际 sent 相对预定发送 deadline 的落后，ack/visible 从实际 sent 起算，不含发送前排程积压。分钟样本数在 50,501–67,022 之间，未按固定行数重分桶或丢弃样本。

原名 `soak-after.json` 的 raw/summary 保留，详细分钟表、斜率、尾桶、配置和二进制身份见 `soak-candidate-v1-trends.json`；完整解释另存 `soak-candidate-v1.md`。随后审查发现每个 live span 深拷贝 LayoutPlan 的宽表计费遗漏，必须修正，故此处仅是 v1 的独立证据，最终版本另行构建测量。现有客户端消费与基准逻辑未改变，服务端去除历史清单工作后增长停止，支持服务端刷新路径是本次线性增长的主要来源；这不等于排除所有客户端开销或解释全部尾部尖峰。


### 最终候选补齐快照独立计费

最终只读审查发现 v1 的每段固定 512 字节没有覆盖 RowSpanView 内 ActiveSegmentDescriptor 深拷贝的 LayoutPlan 列数组和 SHM 定位字符串；CompiledSchema/列名/映射仍共享，但布局 Vec 不是共享所有权。这是新快照 retained 计费的实际遗漏，已在 `crates/zippy-server/src/live_log_snapshot.rs:224` 修复为：

`64KiB + Σ(512B + compiled_columns_i × sizeof(ColumnLayout) + shm_os_id_i.len())`。

每段只读列数与字符串长度，生产计算仍为 O(S)，不逐列扫描。plan 保持自己的 schema/source 租约，不能借 plan 的预算替寿命更长的 snapshot 付费。依据为 `crates/zippy-segment-store/src/view.rs:89`、`crates/zippy-segment-store/src/segment.rs:87`、`crates/zippy-segment-store/src/layout.rs:23` 的 Clone 与拥有关系。

宽表回归 `live_wide_snapshot_owns_layout_charge_after_plan_drop`（`crates/zippy-server/src/live_log_snapshot.rs:369`）通过真实 runtime 构造 128 列、32 段，逐个实际 layout entry 计数且确认 32 个布局指针互异：实际布局 262,144 字节、snapshot lease 346,208 字节、plan lease 188,432 字节。释放 plan 后账本精确保留 snapshot 的 346,208 字节，释放 snapshot 后归零。该回归连同已有聚焦测试共 7 passed；首次 fixture 导入错误记录于 `live-refresh-accounting-initial-compile-failure.log`，修正后输出为 `live-refresh-accounting-after.log`。

最终历史无关性回归的三种历史规模均为 **136,577 字节**，而 v1 是 136,446 字节；增加来自补齐本次单段的实际深拷贝费用。两轮日志均保留，不能将旧值当作最终版本输出。

前置 planning reservation 维持原完整快照路径的 346,658,536 字节，没有新增 256MiB 的最低准入要求。4096 列×1024 段的布局本身为 268,435,456 字节，小于该总预算，不能声称已证明总峰值不足；但固定 coordination 子项没有按 S×C 建模，本轮尚未完成所有合法输入组合及取消时序的全域瞬时峰值证明。这是继承的估算边界，不是本轮测出的内存超额，也不是遗漏 retained 计费的豁免理由。


### 最终功能验证与 release 构建

计费修正后的精确 argv、cwd、环境、退出码、时间和日志 SHA 见 `final-validation.json`，9 个源码及 release 二进制身份见 `final-implementation-metadata.json`。工作目录为 `/home/jiangda/develop/zippy`；所有命令设置 `PYO3_PYTHON=$PWD/.venv/bin/python`、`UV_CACHE_DIR=/tmp/zippy-uv-cache`。以下 11 条命令均退出 0；日志位于本节 evidence 目录。

| 命令 | exit | 外层耗时（s） | 结果 | 日志 |
| --- | ---: | ---: | --- | --- |
| `cargo fmt --all -- --check` | 0 | 1.105 | 格式检查通过；空日志以记录的退出 0 为准 | `final-fmt.log` |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 | 2.398 | workspace/all-targets、warnings 作为错误；通过 | `final-clippy.log` |
| `cargo test --workspace -j 4` | 0 | 81.384 | 93 组结果；1,228 passed、7 ignored、0 failed | `final-cargo-test-workspace.log` |
| `cargo build --release -p zippy-server --bin zippy-server` | 0 | 30.533 | optimized release server 构建成功 | `final-build-server.log` |
| `uv run --no-sync maturin develop --release` | 0 | 19.703 | optimized release 扩展构建并 editable 安装成功 | `final-build-extension.log` |
| `uv run --no-sync pytest pytests -q` | 0 | 5.134 | 434 passed；pytest 内部报告 4.72s | `final-pytest-root.log` |
| `uv run --no-sync pytest clients/python/tests -q` | 0 | 3.307 | 247 passed；覆盖率 85.45% ≥ 80%；pytest 内部报告 3.03s | `final-pytest-client.log` |
| `uv run --no-sync ruff check scripts/analyze_public_e2e.py pytests/test_analyze_public_e2e.py` | 0 | 0.082 | 指定 2 文件全部通过 | `final-ruff.log` |
| `uv run --no-sync black --check -l 100 scripts/analyze_public_e2e.py` | 0 | 0.174 | 分析脚本无需格式修改 | `final-black-script.log` |
| `uv run --no-sync black --check -l 100 pytests/test_analyze_public_e2e.py` | 0 | 0.148 | 分析测试文件无需格式修改 | `final-black-test.log` |
| `uv run --no-sync mypy scripts/analyze_public_e2e.py pytests/test_analyze_public_e2e.py` | 0 | 0.129 | 指定 2 个源文件无类型错误 | `final-mypy.log` |

Rust 结果包含单元、集成及 doc tests；7 个 ignored 是既有手工 release 性能基准，未计入通过数。主要历史无关性、宽表计费、UTF-8 suffix、连续性/未来 cursor、跨 checkpoint、schema 演进和 writer epoch 测试均在最终全 workspace 运行通过。两套 Python 测试在 release 扩展重新安装之后执行；Ruff/Black/mypy 的范围是本次改动的两份 Python 文件，没有冒称全仓 Python lint。

最终 server SHA-256 为 `4313a31a58eaf41edebfa115789f99567afd2aa852f26f7898b3dbe7cb064604`，extension 为 `9298888be7c8a5b003dcd568c009470f2aacf3b4c01e2c351d73ff293e27f730`。原 benchmark 脚本仍为 `bec0a8a5b9a8abf930f5a423eb0d48eb4b37d59edc77aa3dbe89f7b235d72015`。最终性能数据必须匹配这些身份，不能仅凭相同 dirty HEAD 把 v1 与最终候选混为一轮。


### 发现与未证明的边界

- 已追上窗口的第二次 Catalog 请求从 snapshot 读队列改为 control 读队列；原第一次 resolve 也走 control。两队列与 writer 分属独立线程/连接，不能称为 revision 在同一队列等待 background commit/fsync（`crates/zippy-catalog/src/service.rs:913`、`crates/zippy-catalog/src/service.rs:1724`、`crates/zippy-catalog/src/service.rs:1735`、`crates/zippy-catalog/src/service.rs:2262`）。control 内部同步处理单次 backend 调用，可能等前面的读操作；本轮没有测量各队列等待时长，未把尾部尖峰归因于它，也未据此改调度。细节见 `catalog-tail-risk-audit.md`。
- 首次打开和需要持久回放的窗口仍有原历史清单成本与限制：`crates/zippy-server/src/query_snapshot.rs:57` 的上限为 4096 个 visible files、16384 个 lineage rows、2MiB 原始响应。这里没有长期全天、新订阅跨该上限的验收数据；本次优化不能宣称已经解决该独立的历史查询规模边界。
- Actor 当前 live 元数据仍需遍历，Frame 仍使用 BTreeSet；prepared 状态对新 owner 的精确暂停交错没有另加专项测试。前置规划预算的全域瞬时峰值证明也未在本轮完成，具体范围见前文；未以全量测试通过代替这些证明。
- 固定每 1000 行的诊断采样可能与周期维护存在相位相关；read_chunk 前的边界包括网络及客户端 C++ 解码，而 received 之后的 to_pylist/列表追加未独立计时。服务端根因结论综合代码读集、确定性账本计数、Catalog 线程 CPU、无背景对照和服务端修改后的长时干预结果，不单凭稀疏采样排除所有客户端因素。


### 最终计费修正版：长时趋势与四格性能验收

最终版十分钟有背景 soak 的 ack/visible p50 趋势均通过；五分钟无背景 control 的 visible 斜率 **22.5756µs/min > 20µs/min，未通过**，其首末比例与 ack 趋势通过。四格相对 e1 收录的旧 critical-section 基线也未全部不劣化。三次原基准均因延迟门限退出 1，故本轮**不能宣布阶段 1.7 全部验收通过**；功能、样本完整性和构建身份通过不能替代这些失败项。

本节仅报告最后一次计费修正后的预定三组运行。此前 v1 首轮四格、短配对和 v1 soak 仍独立保留；不选择其中较好数字替换最终版本，也不以最终结果覆盖旧失败。

证据为 `soak-final-comparison.json`、`control-final-comparison.json`、`baseline-final-comparison.json` 与逐项 `final-acceptance.json`。三个 comparison 分别为 74,069 / 43,375 / 143,161 字节，验收 JSON 为 59,832 字节，均小于 300,000 字节。历史 raw 的 expected SHA 来自此前已保存的 trends/comparison，并在本次读取时验证；历史 summary 和 raw 均未修改。

#### 最终运行归属、时长与完整性

三次运行的 server SHA-256 均为 `4313a31a58eaf41edebfa115789f99567afd2aa852f26f7898b3dbe7cb064604`，extension 为 `9298888be7c8a5b003dcd568c009470f2aacf3b4c01e2c351d73ff293e27f730`，benchmark 脚本为 `bec0a8a5b9a8abf930f5a423eb0d48eb4b37d59edc77aa3dbe89f7b235d72015`。它们与 `final-implementation-metadata.json` 一致，metadata 的 HEAD 为 e1e67b5、dirty=true；不能只凭 HEAD 把这些测量称为 clean e1。

| 最终运行 | 计量样本 / 含预热收到 | 外层 wall time（s） | 实际计量 sent 覆盖（s） | 完整分钟 | 原脚本 exit |
| --- | --- | ---: | ---: | ---: | ---: |
| soak-final | 600,100 / 602,100 | 613.409 | 604.778297027 | 10 | 1 |
| control-a-final | 300,100 / 302,100 | 324.010 | 318.291605742 | 5 | 1 |
| baseline-final 四格 | 每格 20,000 / 分别 22,000、21,975、21,000、21,000 | 154.675 | 各格见下方真实暴露与吞吐 | 各格不足 1 分钟 | 1 |

外层 wall time 包括启动、预热和结束等阶段，不能代替计量暴露。预热沿原双条件 mask 排除：最终 memory/背景100k 短格为 1,975 行，WAL 两格各 1,000 行；不强行把每格预热改写成 2,000。六格所有 missing/duplicate/out_of_order/unexpected 均为 0，ack/visible 计量数相等且符合配置，ordered end marker 完整，server/worker 退出码全为 0。

这次从三份最终 raw 的 `diagnostic_logs` 重新检查了全部 21 份预期 server/worker 日志（soak 4、control 3、baseline 14），全部存在；不区分大小写扫描 SEQUENCE_GAP、SequenceGap 及空格/连字符变体，匹配数均为 0，整份 raw 同样为 0。未将上轮的零错误结论沿用。本次最终 raw/summary SHA 与 comparison 再次核对一致；9 份当前源码哈希和稳定运行 metadata 均与最终构建记录匹配，详细逐文件结果保存在 `final-acceptance.json`。

#### 长时完整分钟趋势判定

| 运行 / 端点 | 完整分钟数 | p50 OLS（µs/min） | 首分钟 p50（µs） | 末分钟 p50（µs） | 末 / 首 | slope≤20 | ratio≤1.15 |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| soak-final / visible | 10 | 2.023667 | 1156.196 | 1138.115 | 0.984361648 | 通过 | 通过 |
| soak-final / ack | 10 | 0.252176 | 835.463 | 813.154 | 0.973297441 | 通过 | 通过 |
| control-a-final / visible | 5 | 22.575600 | 1126.362 | 1268.704 | 1.126373226 | **未通过** | 通过 |
| control-a-final / ack | 5 | 13.584200 | 812.413 | 902.193 | 1.110510295 | 通过 | 通过 |

两次运行实际达到要求的 10 / 5 个完整分钟。OLS 使用所有完整分钟，首末比例仅用首末两个完整分钟；二者不互相替代，正斜率与末/首小于 1 可以同时出现。尾桶仅展示，未加入趋势，也未将 22.5756 四舍五入到门限内。

##### soak-final 逐分钟明细

| 实际 sent 分钟窗 | 数量 | ack p50 / p99（µs） | ack >10ms | visible p50 / p99（µs） | visible >10ms | 趋势 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0, 60) 秒 | 55,068 | 835.463 / 2013.815 | 128 | 1156.196 / 3670.163 | 270 | 是 |
| [60, 120) 秒 | 54,313 | 896.593 / 3546.020 | 154 | 1261.527 / 37986.582 | 1,449 | 是 |
| [120, 180) 秒 | 65,842 | 808.335 / 1579.338 | 67 | 1132.143 / 2206.671 | 95 | 是 |
| [180, 240) 秒 | 57,871 | 838.714 / 2634.696 | 163 | 1177.333 / 27324.893 | 824 | 是 |
| [240, 300) 秒 | 57,602 | 840.608 / 2587.017 | 165 | 1177.306 / 4160.196 | 239 | 是 |
| [300, 360) 秒 | 63,350 | 829.185 / 2380.504 | 83 | 1163.916 / 4139.062 | 106 | 是 |
| [360, 420) 秒 | 65,920 | 794.028 / 1356.294 | 62 | 1102.014 / 1779.451 | 67 | 是 |
| [420, 480) 秒 | 60,035 | 821.295 / 1449.290 | 64 | 1132.244 / 1946.755 | 70 | 是 |
| [480, 540) 秒 | 48,999 | 942.746 / 3715.701 | 149 | 1366.595 / 23350.139 | 829 | 是 |
| [540, 600) 秒 | 66,292 | 813.154 / 1626.242 | 70 | 1138.115 / 2126.052 | 72 | 是 |
| [600, 660) 秒，观测至 604.778297027 秒 | 4,808 | 894.111 / 2006.012 | 7 | 1255.203 / 2582.046 | 7 | 否，尾桶 |

##### control-a-final 逐分钟明细

| 实际 sent 分钟窗 | 数量 | ack p50 / p99（µs） | ack >10ms | visible p50 / p99（µs） | visible >10ms | 趋势 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [0, 60) 秒 | 55,648 | 812.413 / 2149.573 | 99 | 1126.362 / 11381.513 | 614 | 是 |
| [60, 120) 秒 | 59,452 | 860.683 / 2322.158 | 84 | 1198.057 / 41888.954 | 949 | 是 |
| [120, 180) 秒 | 54,121 | 868.549 / 4115.561 | 189 | 1206.758 / 8401.375 | 436 | 是 |
| [180, 240) 秒 | 62,505 | 816.965 / 2957.978 | 66 | 1139.129 / 4070.557 | 103 | 是 |
| [240, 300) 秒 | 52,609 | 902.193 / 3242.380 | 165 | 1268.704 / 54061.997 | 1,763 | 是 |
| [300, 360) 秒，观测至 318.291605742 秒 | 15,765 | 891.228 / 3722.289 | 19 | 1323.821 / 18317.589 | 203 | 否，尾桶 |

分钟表沿 sent 分桶、排除原预热且不按固定行数重排。soak 第 2/4/9 分钟、control 第 2/5 分钟仍有明显尾部升高；不能由 p50 斜率下降推导 p99/p99.9 已达标。

##### 历史趋势与最终结果的描述对照

| 数据集 | 背景行/s | 完整分钟 | visible p50 OLS（µs/min） | visible 末 / 首 | ack p50 OLS（µs/min） | ack 末 / 首 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| b5-soak | 100000 | 5 | 693.931800 | 3.047522657 | 14.739900 | 1.106207847 |
| critical-soak | 100000 | 5 | 700.140300 | 3.052169567 | 14.904600 | 1.110773344 |
| soak-final | 100000 | 10 | 2.023667 | 0.984361648 | 0.252176 | 0.973297441 |
| control-a-before | 0 | 3 | 718.529000 | 2.040058363 | 26.397000 | 1.066343624 |
| control-a-final | 0 | 5 | 22.575600 | 1.126373226 | 13.584200 | 1.110510295 |

旧 b5 收录的 soak 实际 metadata 为 7c6b6a9 dirty，旧 critical-section soak 为 b5bf6d3 dirty，control-a-before 为 e1e67b5 dirty；不是相同时间、相同样本时长的 clean checkout 实验。最终两组的 p50 增长斜率较修复前大幅下降，但无背景组仍超过 20µs/min，不能称增长验收完全解决。

#### 长时全程分位、吞吐与发起滞后

| 运行 / 端点 | p50（µs） | p99（µs） | p99.9（µs） | max（µs） |
| --- | ---: | ---: | ---: | ---: |
| soak-final / visible | 1160.945 | 5537.777 | 73774.958 | 349989.424 |
| soak-final / ack | 831.331 | 2481.782 | 21542.473 | 349681.961 |
| control-a-final / visible | 1183.565 | 19071.302 | 131010.665 | 229079.073 |
| control-a-final / ack | 847.828 | 3078.575 | 21148.140 | 196598.107 |

| 运行 | 实际前景 / 背景（行/s） | visible >10ms 次数 / 每分钟 | >10ms 暴露（s） | schedule_lag p50 / p99 / max（s） |
| --- | ---: | ---: | ---: | ---: |
| soak-final | 992.263183 / 99999.976986 | 4,028 / 399.616726 | 604.779490140 | 5.286970 / 12.242719 / 12.532741 |
| control-a-final | 942.843698 / 0.000000 | 4,068 / 766.840505 | 318.293045931 | 7.888227 / 17.630278 / 18.219322 |

schedule_lag 是实际 sent 相对 pacing deadline 的发起滞后；ack/visible 从实际 sent 起算，未包含发起前落后。全部样本最终收到，不表示前景稳定达到目标 1,000 行/s。>10ms 频次按首计量 sent 至末 received 的真实暴露计算，与仅到末 sent 的趋势覆盖明确分开；minute_trends 使用 sent，原 visible_spikes 的分钟桶使用 received，未混用两者。

最终 soak 及 control 的原 C 门限均失败：

- soak-final：`visible_p50_micros=[1160.945] exceeds limit=[1000.000]`；`visible_p99_micros=[5537.777] exceeds limit=[3000.000]`；`visible_p99_9_micros=[73774.958] exceeds limit=[10000.000]`。
- control-a-final：`visible_p50_micros=[1183.565] exceeds limit=[1000.000]`；`visible_p99_micros=[19071.302] exceeds limit=[3000.000]`；`visible_p99_9_micros=[131010.665] exceeds limit=[10000.000]`。

#### 最终四格：e1 归档基线与修正版比较

每格均为 20,000 计量样本。下面写“旧 critical-section → 最终版”，单位 µs；基线 metadata 是 b5bf6d3 dirty，不能把归档标签 e1 当成测量的 clean SHA。`baseline-final-comparison.json` 同时保留首轮 v1 四格，不以更差的 v1 作为“不退化”的宽松参考。

| 配置 | 端点 | p50 | p99 | p99.9 | max |
| --- | --- | ---: | ---: | ---: | ---: |
| memory / 背景 0 | visible | 1235.225 → 1173.032 | 1748.249 → 3997.895 | 6157.364 → 11302.025 | 14533.116 → 82013.172 |
| memory / 背景 0 | ack | 766.404 → 842.886 | 1102.931 → 2861.496 | 5534.270 → 8016.655 | 14051.882 → 81337.597 |
| memory / 背景 100,000 | visible | 1250.600 → 1114.327 | 1956.124 → 1703.472 | 8475.353 → 7979.449 | 16042.638 → 15788.483 |
| memory / 背景 100,000 | ack | 765.001 → 794.074 | 1220.421 → 1329.655 | 7745.233 → 7635.078 | 15416.948 → 15420.724 |
| wal / 背景 0 | visible | 1332.277 → 1151.847 | 2321.517 → 2336.516 | 5178.351 → 5785.015 | 14788.946 → 19676.950 |
| wal / 背景 0 | ack | 2012.409 → 2158.020 | 3467.674 → 3986.645 | 6485.029 → 10613.945 | 13484.120 → 34692.992 |
| wal / 背景 100,000 | visible | 1349.946 → 1248.405 | 2235.049 → 4586.896 | 5235.842 → 18915.198 | 12153.181 → 38466.093 |
| wal / 背景 100,000 | ack | 2052.186 → 2312.963 | 3425.499 → 9579.661 | 6894.838 → 27363.958 | 12486.548 → 82413.945 |

| 配置 | 前景行/s：旧 → 最终 | 最终背景行/s | visible >10ms 次数：旧 → 最终 | >10ms /min：旧 → 最终 | 最终暴露（s） |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory / 背景 0 | 1000.010677 → 977.295334 | 0.000000 | 5 → 34 | 14.999685 → 99.683050 | 20.464863292 |
| memory / 背景 100,000 | 1000.018086 → 1001.266364 | 100035.400804 | 8 → 6 | 23.999830 → 18.022586 | 19.974936419 |
| wal / 背景 0 | 477.115497 → 439.184004 | 0.000000 | 8 → 9 | 11.450913 → 11.858244 | 45.537941254 |
| wal / 背景 100,000 | 465.648269 → 357.660665 | 100009.238140 | 5 → 50 | 6.984790 → 53.649969 | 55.918019441 |

逐分位以不引入容差的“最终值≤旧值”描述比较：visible p50 四格都下降，但只有 memory/背景100k 的 visible p99/p99.9/max 也全部下降，其余三格这三项均升高。ack p50/p99 四格全部升高，ack p99.9 仅 memory/背景100k 下降，ack max 四格全部升高。因此 visible 四分位全不增仅 1/4 格，ack 为 0/4 格；不能宣布四格全部不劣化。吞吐和 >10ms 频次单列为描述对照，没有凭它们创造替代通过门限。

四格原 C/D 门限也全部失败，原始失败项为：

- memory / 背景 0：`visible_p50_micros=[1173.032] exceeds limit=[1000.000]`；`visible_p99_micros=[3997.895] exceeds limit=[3000.000]`；`visible_p99_9_micros=[11302.025] exceeds limit=[10000.000]`。
- memory / 背景 100,000：`visible_p50_micros=[1114.327] exceeds limit=[1000.000]`。
- wal / 背景 0：`visible_p50_micros=[1151.847] exceeds limit=[1000.000]`；`wal_ack_p50_micros=[2158.020] exceeds limit=[2000.000]`。
- wal / 背景 100,000：`visible_p50_micros=[1248.405] exceeds limit=[1000.000]`；`visible_p99_micros=[4586.896] exceeds limit=[3000.000]`；`visible_p99_9_micros=[18915.198] exceeds limit=[10000.000]`；`wal_ack_p50_micros=[2312.963] exceeds limit=[2000.000]`；`wal_ack_p99_micros=[9579.661] exceeds limit=[5000.000]`。

最终四格与此前历史发生在不同主机时段，这些是观察到的前后差异，不足以单独判断每项退化的因果来源。同期短配对和首轮失败已经单独保留；没有对明显不利的格做选择性删除，也没有提升 20µs/min 或原 C/D 门限。

#### 本轮验收状态及原始身份

| 检查 | 最终结果 |
| --- | --- |
| 6 格样本/序号/端标记/进程退出 | 通过 |
| 本轮捕获日志 SEQUENCE_GAP、构建身份、原始证据一致性 | 通过 |
| 有背景 10 完整分钟 ack/visible p50 趋势 | 通过 |
| 无背景 5 完整分钟 ack/visible p50 趋势 | **未通过：visible 22.5756µs/min** |
| 三次运行的原 C/D 延迟门限 | **未通过** |
| 四格各端点所报告分位数全部不升高 | **未通过** |
| 阶段 1.7 整体验收 | **未全部通过** |

原始样本留在 `/tmp/zippy-perf-raw/2026-10-10-latency-growth/`，各 SHA 已在分析后再读取核对：

| raw 文件 | 字节 | SHA-256 |
| --- | ---: | --- |
| soak-final.json | 107,667,873 | `c07b6740b82b5aa2e10943a93c37da101972c72c77eb8fe7ff12cf28f3b4aec0` |
| control-a-final.json | 54,040,665 | `49761b30500eabc07ef4771ee6be05d5a993a0d1cc249450667ef04e28cfc0e6` |
| baseline-final.json | 15,103,131 | `6c5645cd3ec5f0a3301e75b231544a694f2f524857afecc35dafdb986e7127ce` |

精确基准 argv、cwd、外层耗时及 exit 由 `final-benchmark-commands.json` 保存。完整分钟逐点、分位数、历史 SHA 校验、元信息差异、日志逐文件哈希和失败理由分别保存在上述 comparison 与 `final-acceptance.json`；本段未重新复述功能验证日志。


基准共用 `--rate 1000 --warmup-seconds 2 --warmup-samples 1000 --background-batch-rows 1000 --timeout-seconds 1200`；soak 使用 memory/背景100000、600100 样本，control 使用 memory/背景0、300100 样本，四格使用 memory/wal×背景0/100000、各20000样本。Python 均通过 `UV_CACHE_DIR=/tmp/zippy-uv-cache uv run --no-sync` 执行，精确参数与路径见命令 JSON。

OLS 是观测斜率，不能将无背景组的小幅超限直接认定为某个确定的新线性根因，也不能用波动或环境推测豁免失败。

### 最终证据交付与尚未完成项

证据索引为 `docs/perf_reports/evidence/2026-10-10-latency-growth/README.md`。`SHA256SUMS` 收录76份证据文件（不含清单自身），`sha256sum -c SHA256SUMS` 全部通过；最大单文件为 `baseline-final-comparison.json`，143161字节，所有证据均≤300000字节。逐行raw留在指定/tmp目录，comparison记录其路径、字节数和SHA。

`final-source-audit.json` 已复核9份源码与release二进制的SHA、原基准脚本与e1逐字节一致、报告e1历史前缀完整、旧版失败测试文件及patch一致；生产diff没有临时诊断插桩。分支仍为 `feature/public-path-latency-bench`，HEAD仍为e1e67b5；没有commit、push或切换分支。

未完成的验收项是：无背景5分钟的趋势门限、四格相对e1的全面不退化，以及残余尾部尖峰的具体归因。已完成服务端历史清单依赖的最小修复、可移植的确定性回归和要求的测量/功能验证，但**本轮整体性能验收未通过**。现有证据不足以把残余失败归因于环境或某条读队列；本轮没有进一步实施缺乏定位依据的调度改动。前置峰值估算证明、首次/落后历史回放的规模边界仍如“发现”所述，不冒称已经解决。

### 监督方独立复核（2026-10-10 20:29–20:52）

以上为实现方记录，保持原样。监督方在同一台机器上复核如下；机器在复核期间没有运行构建或测试。

**回归测试有效性**：在 `e1e67b5` 的 git worktree 中只放入 `native_query_canary.rs` 的测试改动，结果为
`live_log_refresh_memory_does_not_grow_with_persisted_history` 失败（`small=[1272296] large=[8391176]`），
另一个"背景表历史"对照测试通过，与实现方一致。

**ABBA 交替对照**：memory 档，背景 100,000，每轮 180,100 样本；`server-e1`（sha256 `ea5f6452…`）与
`server-final`（sha256 `4313a31a…`，即当前 release 构建）按 e1 → final → final → e1 的顺序运行。逐分钟数据见
`evidence/2026-10-10-latency-growth/supervisor-ab-summary.json`。

| 运行 | 各分钟 visible p50（ms） | p50 斜率 | 末/首 | 末分钟 p99（ms） | >100ms |
|---|---|---:|---:|---:|---:|
| run1 e1 | 1.533 / 2.269 / 3.003 | 735.3µs/min | 1.959 | 5.541 | 0 |
| run2 final | 1.127 / 1.101 / 1.125 | −0.9µs/min | 0.998 | 1.928 | 0 |
| run3 final | 1.078 / 1.121 / 1.087 | 4.8µs/min | 1.009 | 1.680 | 0 |
| run4 e1 | 1.415 / 2.313 / 3.104 | 844.1µs/min | 2.193 | 5.578 | 0 |

**10 分钟浸泡**（`server-final`，600,100 样本）：斜率 3.26µs/min，末/首 1.037，达到门限；各分钟 p50 在
1.076–1.133ms，p99 在 1.43–1.99ms；完整性零错误，日志无 SEQUENCE_GAP。第 0 分钟有 2 个样本超过 100ms
（最大 308ms），对应的 ack 同样约为 308ms，属于写入侧停顿，与订阅刷新路径无关。脚本退出码为 1，原因是第 C 档的绝对门限
（p50 ≤ 1ms、p99.9 ≤ 10ms），`e1e67b5` 同样达不到，不属于本阶段的通过标准。

**对实现方"未通过"项的判断**：实现方最终浸泡和 control-a 中 100–350ms 的连续停顿，每次都伴随同等幅度的 ack
停顿，写端发起滞后的 p50 达到 5.3s。也就是说写入路径整体被卡住，而本次改动只涉及订阅刷新的读路径。监督方的 ABBA 对照中，
两个版本都没有出现这类停顿，新版在 p50、p99、>10ms 次数和发起滞后上都优于 e1。据此判定这些停顿来自当时的运行环境，
不是本次改动造成的退化。无背景对照和 WAL 两格未由监督方重跑；确定性测试已经证明根因与背景表无关。

**其他发现**：`zippy-catalog` 的
`service::tests::cooperative_manifest_preflight_yields_and_releases_cancelled_command_budget` 在 5 次全量运行中失败 1 次
（`ResourceExhausted ... outstanding command budget exhausted used=[1] maximum=[1]`）。该 crate 本次没有改动，
属于早已存在的不稳定测试，需要另行修复。
