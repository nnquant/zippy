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
