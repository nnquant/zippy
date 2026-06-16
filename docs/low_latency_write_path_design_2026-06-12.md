# Zippy 写入热路径极致性能提升设计方案

日期：2026-06-12

范围：不考虑远端 Gateway。聚焦进程内写入热路径：
`数据到达 → materializer 写入 active segment → committed 对读者可见（→ 同机订阅唤醒）`。

本文回答三个问题：

1. `stream-table-segment-copy` p99=4ms 到底测的是哪一阶段、尖峰从哪来。
2. 低频 CTP 写入出现间断性 1-3ms 延迟，问题可能出在哪、如何定位。
3. 追求极致性能（单行写稳定个位数微秒、端到端 <10μs）的分阶段设计方案。

所有标注"实测"的数字来自本机（WSL2，kernel 5.15.153.1-microsoft，/tmp 为 ext4，
binary 为 2026-05-12 构建的 release zippy-perf）当日运行；其余标注"推断"。

---

## 1. copy 路径 p99=4ms 的精确语义

### 1.1 测的是哪一段

`stream-table-segment-copy` profile 的延迟样本是这一行代码的耗时
（`crates/zippy-perf/src/lib.rs:1019-1021`）：

```rust
let batch_start = Instant::now();
send_table(table.clone())?;          // = materializer.on_data(segment_view)
let send_elapsed = batch_start.elapsed();
```

即：**单次 `StreamTableMaterializer::on_data()` 的同步调用耗时，批粒度（4096 行/批），
不含任何队列、网络、订阅、持久化**。具体执行链：

```
on_data(SegmentTableView)
└─ materialize_table(stream_table.rs:848)
   ├─ apply_retention()                          # 每批都做：drain persist 完成队列 + retention 检查
   └─ materialize_table_rows()
      └─ materialize_segment_rows(stream_table.rs:961)   # 输入是 segment row view，forwarding=false
         └─ writer.append_row_span(catalog.rs:1020)
            ├─ 分区 Mutex 锁内：try_append_active_row_span(builder.rs:1134)
            │    └─ 按列 memcpy（i64/f64 整列拷贝、utf8 拷贝 offsets+values，双写 heap mirror + shm）
            ├─ publish committed prefix（原子 store）
            └─ broadcaster.notify_all() + futex wake（有 waiter 时）
```

注意 copy profile 中 parquet persistence 未开启（`enqueue_persist_task` 因
`persist_config.is_none()` 直接返回，`stream_table.rs:1366`），所以 p99 与 Parquet 无关。

### 1.2 尖峰的来源：rollover 批

profile 的 row_capacity = `rows_per_batch * 64` = 262,144 行
（`crates/zippy-perf/src/lib.rs:401-405`）。即**每 64 批触发一次 rollover**。
1M rows/s 下批率 244 批/秒，rollover 约 3.8 次/秒，占全部批的 1.56%——恰好落在
p99 统计窗口内。**p99 ≈ rollover 批的耗时**。

rollover 批在普通批的 ~150-240μs 之外额外做（全部在 `materialize_segment_rows`
的 `segment is full` 分支内同步发生，`stream_table.rs:984-997` → `catalog.rs:1132`
`rollover_inner`，且在分区锁内）：

| 步骤 | 内容 | 量级（推断+实测佐证） |
| --- | --- | --- |
| 新 segment 分配 | `ShmRegion::create`：ext4 上 `create_new` + `set_len` + `posix_fallocate`（本 schema ~15MB）+ `mmap`（`shm.rs:398-415,533`） | 0.5-2ms |
| heap mirror 分配 | 新 `ActiveSegmentWriter` 为每列 `vec![0; ...]` 建堆镜像（~14MB 分配+清零，`builder.rs:104-134`） | 0.5-1.5ms |
| seal + 描述符 | `seal_for_rollover` 写 sealed 标志 + notify；descriptor JSON（serde_json）构建 + publish | 50-200μs |
| retention | 扫描 retained segments、可能再次 publish | <100μs |

实测对照：4096 行/批 @1M rows/s 5 秒，`p50=193μs p95=333μs p99=2519μs`（ext4 /tmp）；
tmpfs 下 `p99=3320μs`（本机波动）。2026-05-12 审计记录 `p99=4034μs`（3 秒测）。

**结论：p99=2.5-4ms 的主体就是 rollover 时同步执行的"新 shm 文件创建 + fallocate +
mmap + 约 30MB 内存分配清零"，发生在写入热路径的分区锁内。** 它不是网络、
不是订阅推送、不是 Parquet 持久化。

普通批 p50=193μs 也值得注意：4096 行 × 4 列的列拷贝是双写（heap mirror 与 shm
各一份，约 2×0.5MB 内存写）+ utf8 逐行 offsets 处理，折合 ~47ns/行——合理但有
压缩空间（见 §4 P2-4）。

---

## 2. 低频 CTP 写入间断性 1-3ms：根因分析

### 2.1 关键否定：不是 materializer 写入本身

实测单行写入（copy profile，`--rows-per-batch 1 --target-rows-per-sec 200`，60 秒）：

| shm 所在文件系统 | p50 | p95 | p99 |
| --- | --- | --- | --- |
| ext4（默认 /tmp） | 21μs | 32μs | **168μs** |
| tmpfs（`TMPDIR=/dev/shm`） | 22μs | 30μs | **105μs** |

单行 `on_data` 常态 21μs、尾部 ~100-170μs，**60 秒内没有出现 ms 级样本**。
低频下 rollover 也基本不会发生（默认 65,536 行容量，CTP 单合约 2 跳/秒要
约 9 小时才滚一次）。所以 1-3ms 必然来自 `on_data` 之外的链路或环境。

### 2.2 嫌疑清单（按概率排序）

低频 CTP 的典型链路是：CTP 回调（C++ 线程）→ Python 落地 →
`pipeline.write()`（PyO3 转 Arrow）→ `EngineHandle.write()` → SPSC 队列 →
worker 线程唤醒 → `on_data`。如果延迟按端到端（写→订阅回调收到）口径测，
还要加上 futex 唤醒订阅者 + Python 回调拿 GIL。逐跳分析：

**嫌疑 1：Python 侧 —— CPython GC 与 GIL（最高嫌疑，典型 1-10ms 间歇尖峰）**

- CPython gen2 GC 是全堆扫描，进程里有 polars/pandas/大对象图时单次 1-10ms，
  间歇触发——与"间断性 1-3ms"的形态完全吻合。
- GIL 争用：zippy Python 进程内有 heartbeat 线程、shutdown agent 线程
  （`python/zippy/__init__.py:1289-1302`），加上你自己的 CTP 回调线程。
  任何持 GIL 的纯 Python 片段（一次日志格式化、一次 DataFrame 操作）都会让
  写入/回调线程等几个 ms。
- 每 tick 的 dict → PyArrow 单行 Table 构造本身 10-100μs，且持续制造 GC 压力。

**嫌疑 2：观测口径含订阅端（如果测的是端到端）**

- 同机订阅唤醒走 futex wait/wake。实测本机跨线程 condvar 唤醒
  p50=47μs p95=74μs p99=92μs max=130μs（futex 同量级）。常态贡献 ~50-130μs，
  不是 ms 主因；但订阅回调若在 Python，回调执行前要拿 GIL → 回到嫌疑 1。
- 非 xfast 订阅模式是 `idle_spin_checks` 次短自旋后进 futex 等待
  （`python/zippy/__init__.py:5781`），错过自旋窗口的 tick 吃满一次唤醒延迟。

**嫌疑 3：WSL2 环境（系统性放大所有尾部）**

- 实测 `sleep(50μs)` 实际耗时 p50=129μs——Hyper-V 虚拟定时器粒度粗。
  engine worker 非 xfast 时空闲等待用 `park_timeout(50μs)`
  （`crates/zippy-core/src/runtime.rs:852-856`），WSL2 上实际 park ~130μs 起。
- WSL2 vCPU 受 Windows 宿主调度，宿主任何活动（Defender 扫 vhdx、浏览器）
  都可制造 1-5ms 的 vCPU 抢占。**在 WSL2 上追微秒级尾延迟不现实，
  生产与正式压测必须在原生 Linux。**

**嫌疑 4：ext4 mmap 周期性 writeback（间歇性 ms 尖峰的经典来源）**

- shm 段是 `std::env::temp_dir()` 下的**磁盘文件 mmap**（`shm.rs:369`），
  WSL2 与多数发行版的 /tmp 是 ext4 而非 tmpfs。内核周期回写 mmap 脏页
  （本机 `dirty_writeback_centisecs=1500`，即每 15 秒）：回写会清掉 PTE 写位，
  **下一次写该页触发 page fault + ext4 journal**，单次可达百 μs 至数 ms，
  形态正是"间断性"。
- 实测佐证：仅把 shm 移到 tmpfs，60 秒低频测试 p99 从 168μs 降到 105μs（-37%）。
  裸机+更大脏页量场景下差距会进一步放大。

**嫌疑 5：队列与唤醒链的常态开销（贡献 ~100-300μs 基底，非 ms 主因）**

- `emit()` 持 `emit_lock` Mutex → `SpscDataQueue::push_blocking`
  （Mutex+Condvar 实现，非 lock-free，`crates/zippy-core/src/spsc_data_queue.rs`）
  → `notify_worker()` 再取一次 Mutex + `unpark`（`runtime.rs:73-76,280-281`）。
- worker 空闲循环 `try_recv → try_pop → park_timeout(50μs)`
  （`runtime.rs:617-655`）：数据到达落在 park 区间时吃一次 unpark 唤醒
  （WSL2 实测 p99 ~92μs）。

**嫌疑 6：热路径上的同步日志**

- 若进程启用 tracing/loguru 文件输出，热路径一条日志的格式化+写盘就是
  几十 μs 至数 ms（撞上 writeback 时）。需确认热路径日志级别全部关闭。

### 2.3 定位方法（先测后改）

按顺序 A/B，每步只改一个变量，跑 ≥60 秒低频流量记录 p50/p99/max：

1. **拆口径**：分别测 `pipeline.write()` 调用耗时、Rust 侧 `on_data` 耗时、
   端到端（tick 携带 `time.perf_counter_ns()`，订阅回调里求差）。三个数会
   直接指认写入侧、唤醒侧还是 Python 侧。
2. `gc.disable()`（或 `gc.freeze()` 后 disable）跑一轮——尖峰消失即坐实 GC。
3. `TMPDIR=/dev/shm` 跑一轮——尾部收窄即坐实 ext4 writeback（已实测有效）。
4. 开 `xfast=true` 并用 `taskset` 绑核跑一轮——常态唤醒应从 ~100μs 降到 <5μs。
5. 同等代码在原生 Linux 跑一轮——与 WSL2 对比，分离环境噪声。
6. `perf sched record` / `strace -c -e futex` 看写入线程被抢占次数和 futex 等待分布。

---

## 3. 极致性能的目标与原则

| 指标 | 当前（实测/推断） | 阶段目标 P1 后 | 极致目标 P3 后 |
| --- | --- | --- | --- |
| 单行 `on_data` p50 | 21μs | ≤5μs | ≤2μs |
| 单行 `on_data` p99 | 105-168μs | ≤20μs | ≤5μs |
| 批写 rollover 尖峰 p99 | 2.5-4ms | ≤200μs | ≤50μs |
| 端到端写→同机订阅可见 p50 | ~100-200μs（推断） | ≤30μs | ≤5μs |
| 端到端 p99 | 1-3ms（你的观测） | ≤100μs | ≤20μs |

设计原则：

1. **先消尖峰，再压均值**——行情系统 p999 比 p50 值钱。
2. 极致形态是热路径**零分配、零系统调用、零跨线程跳变**；每保留一跳都要有理由。
3. **延迟与 CPU 显式换购**——busy-poll 烧核换微秒，必须可配置、可降级。
4. 每项优化先有基准后有改动，用 `zippy-perf` 增量验证，杜绝凭感觉优化。

---

## 4. 设计方案

### Phase 0：可观测性（先行，约 1 周）

**P0-A 写入链路分段打点**

埋 6 个时间点（feature-gate，默认编译期关闭）：
`t0 写入调用 → t1 入队 → t2 worker 取出 → t3 on_data 进入 → t4 committed 发布 →
t5 futex wake 返回`；订阅侧加 `t6 reader 读到 / t7 回调进入`。
HDR histogram 聚合，输出 p50/p95/p99/p999/max。没有这组数据，后续优化无法归因。

**P0-B 补 zippy-perf profile**

- `single-row-low-rate`：单行 × 100-1000 行/秒 × 60 秒（复现 CTP 形态）。
- `write-to-subscribe-e2e`：行内嵌时间戳，同机订阅者求端到端分布。
- rollover 批单独标记，输出"rollover 批 vs 普通批"两条独立分布。

### Phase 1：零/低改动的环境与配置修复（立即可做）

| 措施 | 针对嫌疑 | 预期收益 | 操作 |
| --- | --- | --- | --- |
| shm 移到 tmpfs | 4 | 尾部 -30%~-80% | 短期 `TMPDIR=/dev/shm`；代码上新增 `ZIPPY_SHM_DIR` 环境变量并默认探测 `/dev/shm`（改 `shm.rs:369`） |
| 原生 Linux 部署 | 3 | 消除 vCPU 抢占 ms 尖峰 | 生产/压测环境硬要求；WSL2 仅开发 |
| `xfast=true` + 绑核 | 5 | 唤醒 ~100μs → <1μs | 参数已有；`taskset -c` 给写者/worker/订阅者各一隔离核 |
| 内核调优 | 系统尾部 | p999 收窄 | `isolcpus`+`nohz_full`+`rcu_nocbs`；governor=performance；关 swap；THP=madvise |
| Python 止血 | 1 | 消 GC 尖峰 | 稳态后 `gc.freeze()`+`gc.disable()`、定期手动 collect；热路径禁日志；CTP 回调只做最小转发 |

CTP 低频本就单行直写，无需攒批；保持单行但接入 Phase 2 直通模式。

### Phase 2：热路径代码改造（核心工程量，按收益排序）

**P2-1 Segment 预分配池（消灭 rollover 尖峰，§1.2 的直接解药）**

- 后台线程预创建下一个 active segment（shm 文件 + mmap + heap mirror + 预触页），
  放入 per-partition spare 槽；rollover 在锁内只做指针交换 + seal 标记。
- shm 文件池复用：sealed segment 释放后归还池（复位重用），避免反复 create/unlink。
- 新 mmap 用 `MAP_POPULATE` 或写一遍预 fault，消首写 minor fault；段大时评估
  hugetlb（2MB 页：15MB 段 ~3800 次 fault → 8 次）。
- 预期：rollover 批 p99 2.5-4ms → <200μs（仅剩 seal + descriptor publish）。

**P2-2 写者直通模式（bypass 队列，消跨线程跳变）**

- 现状每条数据走 `emit_lock → SPSC(Mutex+Condvar) → unpark → worker`
  （`runtime.rs:275-285,617-655`），常态贡献 ~50-130μs + 调度不确定性。
- 新增 `DirectWriter`：单写者语义下，调用线程直接同步调 `materializer.on_data()`，
  不经 engine 队列。materializer 本就被分区 Mutex 保护，单写者时无竞争（~20ns）。
- 适用边界：纯物化（无 engine 计算）的 stream_table。有计算时保留队列或用 P2-3。
- 预期：链路少一跳线程切换，写调用→committed ≈ on_data 本身（21μs，
  再经 P2-4 进一步压）。

**P2-3 SPSC 队列升级 + busy-spin worker（保留队列时的形态）**

- `SpscDataQueue` 从 Mutex+Condvar 换 lock-free ring（自研定长环或引入 `rtrb`），
  push/pop 各一次原子操作。
- worker 等待三段式：N 次 `spin_loop()` → M 次 yield → futex/park；
  xfast=true 时纯 spin + 绑隔离核。
- 注意：workspace 的 `unsafe_code = forbid` 实际只对显式声明
  `[lints] workspace = true` 的 crate 生效（当前仅 zippy-perf 声明；
  segment-store 本就在用 libc/unsafe）。lock-free ring 不受阻碍，但建议
  顺手把各 crate 的 lints 继承显式化，消除"全局 forbid"的误解。

**P2-4 消除双写与按名查找（压常态成本）**

- **双写消除**：单元格与 columnar 写入都同时写 heap mirror 和 shm 两份
  （`builder.rs:559-580,939-970`）。mirror 仅服务 sealed 转换/Parquet/快照读——
  改为只写 shm，sealed 转换时从 shm 读回（或 mirror 懒物化）。写路径内存带宽
  减半，批写 p50 预计 -30%~-45%（193μs → ~110-130μs/4096 行）。
- **列句柄 API**：单元格写入每次按 `&str` 线性 find 列类型 + HashMap 两次查找
  （`builder.rs:875-885,559-565`）。新增 `ColumnHandle`（预解析列索引+类型+
  layout 偏移），启动时解析一次，热路径纯数组索引。单行 4 列省 ~0.5-1μs。
- **Utf8 字典化**：CTP `instrument_id` 基数低重复率高，提供 dictionary 列
  （u32 code + 字典表），单行写从字符串拷贝变 4 字节整数写，segment 同时变小。

**P2-5 通知与唤醒精细化**

- 写侧 waiter-count 门控 futex wake（`builder.rs:859-872`）已是正确形态，保留。
- 进程内 `broadcaster.notify_all()` 持订阅表 Mutex 逐个 notify
  （`notify.rs:78-84`），订阅者多时放大写热路径——改为写者只 bump 序号 +
  futex wake，进程内订阅者统一走 shm futex（与跨进程读者同一机制）。
- 读侧三档等待（已有 `idle_spin_checks` 雏形）：纯 busy-poll（烧 1 核换 <1μs）/
  spin-then-futex / 纯 futex。行情订阅热路径推荐前两档。

**P2-6 杂项卫生**

- 热路径禁 tracing（编译期 level gate）。
- `apply_retention()` 每批都跑（`stream_table.rs:852`），但仅 rollover/persist
  完成后才可能有变化——改为事件触发。
- 打点用 TSC（如 `quanta`）替代 `Instant::now`。
- 全程 jemalloc/mimalloc + 启动期预热；P2-1/P2-4 后热路径应零 malloc（打点验证）。

### Phase 3：极致架构形态（按需，烧核换微秒）

**P3-1 LMAX 风格 pinned 单写者流水线**

```
CTP 回调线程(绑核A) ──lock-free ring──> 写者线程(绑核B, busy-poll)
  仅做: 解析→定长struct→push              on_data 直通 → shm commit → 序号bump/futex wake
  隔离核, 无GIL, 无分配                    隔离核, 零分配, 常态零syscall

订阅者(绑核C, busy-poll committed_row_count) → 策略逻辑(Rust)
```

- 端到端预算（推断，需 P0 打点验证）：ring push ~100ns + 写者 poll <1μs +
  单行列写 ~1-2μs（P2-4 后）+ 读者 poll <1μs ≈ **常态 3-5μs，p99 <20μs**
  （隔离核 + 原生 Linux 前提）。
- 代价：每条流水线 2-3 个隔离核。低频 CTP 全市场单流水线足够。

**P3-2 数据面彻底出 Python**

- CTP 接入写成 Rust native source（zippy-io 已有 ZmqSource 先例），或 C++ SPI
  回调经 FFI 直接入 Rust ring。Python 只保留编排、监控、低频消费。
- 这是根除 1-3ms 间歇尖峰的根本解：热路径上只要还有 CPython，GC/GIL 的
  ms 级尾部就无法消除。

**P3-3 定宽行单 memcpy 写（可选探索）**

- 纯数值 schema（symbol 字典化后的 CTP tick）可探索行式 staging：单行写 =
  一次 ~64-96B memcpy + 原子 commit，理论 <200ns；后台再列化。
  当前列式 shm 直写已经很快，**先用 P0 打点确认列散写是瓶颈再做**。

### 不建议做的事

- io_uring/AF_XDP/kernel-bypass：数据源是 CTP API 回调，不适用。
- PREEMPT_RT 内核：隔离核 + busy-poll 已绕开调度器，RT 收益小、运维成本高。
- 在 WSL2 上继续压尾延迟：定时器粒度（实测 sleep 50μs → 129μs）与宿主抢占是硬下限。

---

## 5. 落地顺序与验收

| 阶段 | 内容 | 工作量(推断) | 验收口径（原生 Linux，zippy-perf 新 profile） |
| --- | --- | --- | --- |
| 0 | 分段打点 + 2 个新 profile | 2-4 天 | 打点自身开销 <100ns/段；产出基线报告 |
| 1 | tmpfs/绑核/xfast/GC/内核参数 | 1-2 天 | 低频单行 p99 ≤50μs；端到端 p99 ≤300μs |
| 2a | P2-1 segment 池 + 预 fault | 3-5 天 | rollover 批 p99 ≤200μs；1M rows/s copy 全体 p99 ≤500μs |
| 2b | P2-2 直通模式 | 2-3 天 | 写调用→committed p50 ≤25μs（含 PyO3） |
| 2c | P2-4 双写消除+列句柄+字典列 | 4-6 天 | 单行 on_data p50 ≤5μs；4096 行批 p50 ≤110μs |
| 2d | P2-3/P2-5 队列与唤醒 | 3-5 天 | 端到端 p50 ≤30μs，p99 ≤100μs |
| 3 | P3-1/P3-2 流水线 + Rust source | 2-3 周 | 端到端 p50 ≤5μs，p99 ≤20μs；30 分钟 soak 无 ms 级样本 |

每阶段合入前：对应 profile 跑 60 秒 + 30 分钟 soak 各一轮，报告（git sha、机器、
参数、p50/p95/p99/p999/max、RSS）存档 `docs/perf_reports/`；回归阈值进
`--max-p99-micros` 门禁。

---

## 6. 结论速记

1. **p99=4ms 是"单次 on_data 批写入"的尾部**，主体是 rollover 时在写锁内同步
   执行的新 segment 创建（ext4 文件 fallocate + mmap + ~30MB 分配清零）。
   不是网络、不是订阅、不是 Parquet。解药是 P2-1 segment 预分配池。
2. **低频 CTP 的 1-3ms 间歇延迟大概率不在 Rust 写入本身**（实测单行 on_data
   p50=21μs、p99≤168μs，低频下几乎无 rollover）。按嫌疑排序：CPython GC/GIL >
   端到端口径含订阅唤醒+Python 回调 > WSL2 调度 > ext4 mmap writeback
   （实测 tmpfs 改善尾部 37%）> 队列唤醒链常态 ~100μs。先按 §2.3 六步 A/B
   定位，再改代码。
3. **极致路线**：环境修复（tmpfs/绑核/xfast/原生 Linux）→ 消 rollover 尖峰
   （segment 池）→ 消跨线程跳变（直通/无锁 ring + busy-spin）→ 消双写与按名
   查找 → 数据面出 Python（Rust source + busy-poll 订阅）。终态端到端常态
   3-5μs、p99 <20μs 是该架构（共享内存列式 + 单写者 + 隔离核 busy-poll）
   可达的合理目标，每步都有 §5 的可验收口径。
