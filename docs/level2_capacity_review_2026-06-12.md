# Zippy 热路径承载 A 股全市场 Level2 数据能力审查与改进文档

日期：2026-06-12

范围：基于当前工作区代码（commit `09e2732`）与已有审计文档
（`performance_audit_findings_2026-05-12.md`、`performance_audit_fix_status_2026-05-12.md`），
评估两个问题：

1. 当前实现能否承载 A 股全市场 Level2 级别的数据量？
2. 能否实现微秒级的行情处理速度？

本文是审查文档，不改实现代码。所有结论都附带代码位置或实测数据出处，
未实测的部分明确标注为推断。

---

## 1. 目标负载量化

先把"A 股全市场 Level2"翻译成工程指标，作为后续评估基准：

| 维度 | 量级 | 说明 |
| --- | --- | --- |
| 标的数 | 约 5,000+（沪深两市股票）+ 可转债/基金/期权约数千 | 全订阅约 8,000-10,000 个 instrument |
| 消息类型 | 快照（10 档行情）、逐笔委托、逐笔成交 | 逐笔通道是主要压力源 |
| 稳态消息率 | 10-20 万条/秒 | 盘中正常时段，逐笔+快照合计 |
| 峰值消息率 | 30-50 万条/秒 | 开盘集合竞价后 09:30、尾盘 14:57 附近 |
| 极端突发 | 可达 70-100 万条/秒（百毫秒级窗口） | 异动行情、千股涨跌停日 |
| 单日总量 | 1-3 亿条逐笔 + 数千万快照 | 持久化与日内回查压力 |
| 快照消息宽度 | 10 档买卖价量 ≈ 40+ 个字段 | int64/float64 为主，宽表 |
| 逐笔消息宽度 | 约 8-12 个字段 | 窄表，频率最高 |

延迟目标分层定义（"微秒级"必须明确指什么）：

- **写入热路径延迟**：数据进入进程到对读者可见（committed）—— 目标 < 100μs。
- **进程内端到端**：写入到同机订阅方收到 —— 目标 < 1ms，理想 < 100μs。
- **跨进程/远端端到端**：经 Gateway TCP —— 受网络与批攒影响，毫秒级是合理预期。

---

## 2. 当前实现的热路径盘点

### 2.1 写入路径（最关键）

数据进入有两条主要路径：

**路径 A：进程内 segment forwarding（最快路径）**

上游产出 `SegmentRowView` 时，`StreamTableMaterializer` 走
`append_row_span()` 直接批量转发（`crates/zippy-engines/src/stream_table.rs:856` 起的
`materialize_table_rows()` 分支）。实测（2026-05-12 审计，1M rows/s，4096 行/批）：

- `stream-table-segment-forward`：`p50=38μs p95=67μs p99=81μs`（批级延迟）。

**路径 B：普通 Arrow batch columnar 写入**

非 segment 输入走 `materialize_columnar_table_rows()`
（`crates/zippy-engines/src/stream_table.rs:913`），通过
`PartitionWriterHandle::write_columnar_rows()`
（`crates/zippy-segment-store/src/catalog.rs:932`）在单次分区锁内批量写多行、
只发布一次 committed prefix、只 notify 一次。实测：

- `stream-table-segment-copy`：`p50=236μs p95=488μs p99=4034μs`（修复后数据）。

**路径 B 的逐行 fallback（残留风险）**

含 null 的 batch 仍回退到逐行 `write_materialized_row()`
（`crates/zippy-engines/src/stream_table.rs:887-908`），每行单独锁、事务、通知。
这是 P003 修复明确保留的边界。

**远端写入（Gateway write_batch）**

TCP 帧（4B header_len + 8B payload_len + JSON header + Arrow IPC payload）→
`decode_ipc_batches()` → per-stream writer handle（P010 修复后全局 map 锁只用于
查找 handle）→ `on_data()/on_flush()`。每批开销约为 IPC 解码 15-105μs +
物化 10-50μs（来自代码路径推断，未压测远端 write_batch 延迟分布）。

### 2.2 存储与持久化

- Active segment 是共享内存（mmap）列式布局，固定 row capacity，默认 65,536 行
  （`crates/zippy-engines/src/stream_table.rs:28`）。
- committed_row_count 通过原子操作发布，读者用 futex
  （`crates/zippy-segment-store/src/shm.rs:215-290`，Linux `SYS_futex`）等待新行通知，
  非 Linux 平台退化为 1ms sleep 轮询（`shm.rs:267`）。
- 段满触发 rollover：seal 当前段 → publish descriptor → 新建 active 段 →
  persist 任务经 `std::sync::mpsc::channel`（无界）交给独立持久化线程写 Parquet
  （`stream_table.rs:355`、`stream_table.rs:1361`）。持久化完全异步，不阻塞写入热路径。

### 2.3 订阅/读取路径

- 同机订阅：`ActiveSegmentReader::read_available()` 直接读共享内存 committed 行区间，
  空闲时 `wait_for_notification_after()` futex 等待（`active_reader.rs:99`），
  100ms 超时兜底。P011 修复后无固定 sleep 轮询。
- 远端订阅（Gateway）：行模式逐行 JSON 序列化推送；表模式按 chunk 走 Arrow IPC，
  有 batch_size 阈值与节流。
- 查询：`collect(stream=True)` 已支持 streaming chunk、persisted parquet 有界并行扫描
  （4 文件并发）；默认 `collect()` 仍是一次性整批。

### 2.4 线程与并发模型

- Gateway 使用 tokio 多线程 runtime，阻塞操作隔离到 `spawn_blocking`（上限 64）。
- 进程内 engine 是 thread-per-engine + 有界 SPSC 队列；xfast 模式下空闲等待为
  `spin_loop()` busy-spin，非 xfast 为 `park_timeout(50μs)`
  （`crates/zippy-core/src/runtime.rs:852-856`）。
- 无 CPU 亲和性绑定、无 NUMA 感知。
- workspace 级 `unsafe_code = "forbid"`（`Cargo.toml:32`），shm/futex 通过
  crate 级豁免使用 `libc`。

---

## 3. 结论一：能否承载 A 股全市场 Level2 数据量？

### 3.1 吞吐量评估：稳态可承载，峰值有条件承载

**支持承载的证据：**

- 实测 `inproc-timeseries` 与两个 stream-table profile 在 1M rows/s 目标下
  `pass=true` 且平均吞吐约 100 万行/秒（单 stream、单机、3 秒短测）。
  这已高于全市场稳态 10-20 万条/秒，约等于峰值上限 50 万条/秒的 2 倍。
- 写入按批处理（columnar append、单锁单 notify），每批摊销后单行成本约
  0.05-0.5μs 量级，CPU 上有富余。
- 持久化异步解耦，写入热路径不受 Parquet 编码拖累。

**有条件/存疑之处（按风险排序）：**

1. **基准是单 stream 单 schema，目标负载是数千 instrument 多表多通道。**
   实际部署需决定分表策略：
   - 全市场一张大表（逐笔一张、快照一张）：单 writer 串行语义下单表 100 万行/秒
     够用，但 rollover 频率剧增 —— 65,536 行容量在 50 万条/秒峰值下
     **每 0.13 秒 rollover 一次**，每次 rollover 伴随 descriptor publish、
     retention 检查、persist 入队。`stream-table-segment-copy` 修复前
     p99=10.7ms 的尾延迟很可能与 rollover/persist 尖峰相关，修复后 p99=4ms
     仍明显高于 forward 路径的 81μs。
   - 按 instrument 或按板块分表：降低单表压力，但 Gateway writers map、
     master descriptor 管理、订阅 fan-out 的规模压力未经验证。
2. **现有压测全部是 3 秒短测 + 1024 symbols。** 没有 30 分钟以上 soak、
   没有 5000/10000 symbols 矩阵、没有 RSS/p999 采集（P012 明确遗留）。
   长跑下 allocator 碎片、retained sealed segments、persisted 文件目录膨胀
   都可能侵蚀容量。
3. **极端突发（70-100 万条/秒，百毫秒窗口）依赖上游攒批。** 当前每批 4096 行
   的压测形态对应每秒 244 批；若上游接收程序按 tick 小批写入（例如每批 64 行），
   批次率上升 64 倍，per-batch 固定开销（锁、notify、descriptor 检查）占比剧增，
   吞吐上限会显著低于 100 万行/秒。**攒批策略是容量成立的前提条件，
   但代码库目前没有内置的时间窗 + 行数双阈值攒批组件，需要上游自行实现。**
4. **远端 Gateway 写入路径未压测。** `remote write_batch` 的
   latency/throughput profile 在 ROADMAP 中仍是待办。如果行情接收进程与
   zippy 不同机/不同进程且走 TCP Gateway，IPC 编解码 + 帧协议 + tokio 调度
   的总开销没有实测数据背书。
5. **逐行 fallback 是隐藏地雷。** Level2 快照中部分字段（如停牌标的的档位）
   天然含 null。任何含 null 的 batch 都会跌入逐行写入路径
   （`stream_table.rs:887`），单批成本可能放大一到两个数量级。
   **全市场快照表几乎必然触发该路径**，这是当前实现对 Level2 负载最直接的不适配点。

**吞吐结论：**

> 单机稳态 10-20 万条/秒：**可承载**（有 5-10 倍实测余量，前提是批量写入且数据无 null）。
> 峰值 30-50 万条/秒：**有条件承载** —— 必须满足（a）上游攒批 ≥1024 行/批或等效、
> （b）规避 null fallback、（c）增大 segment row capacity 或接受高频 rollover 的
> p99 尾延迟、（d）分表策略经过验证。
> 当前**不能**在缺少上述条件下宣称承载全市场峰值，且没有任何 soak 数据支持全天运行结论。

### 3.2 存储量评估

单日 1-3 亿条逐笔，按 12 字段 ×8 字节 ≈ 100B/行，原始列数据约 10-30GB/日，
Parquet 压缩后约 3-10GB/日。持久化线程单线程写 Parquet，按审计估算单段
20-150ms，50 万行/秒 ÷ 65,536 行/段 ≈ 7.6 段/秒，**持久化线程在峰值时段
可能跟不上 seal 速度**（7.6 段/秒 × 20-150ms/段 = 15%-114% 占用率），
导致 sealed segments 在内存中堆积。mpsc channel 无界（`stream_table.rs:355`），
堆积表现为内存增长而非背压，长时间峰值下有 OOM 风险。**这是必须量化验证的点。**

---

## 4. 结论二：能否实现微秒级行情处理速度？

### 4.1 分层回答

| 层 | 当前能力 | 证据 | 结论 |
| --- | --- | --- | --- |
| 写入 committed（批级） | forward 路径 p50=38μs/批，copy 路径 p50=236μs/批 | 2026-05-12 实测 | **forward 路径已达微秒级**；copy 路径 p50 达标但 p99=4ms 不达标 |
| 单行摊销成本 | 0.05-0.5μs/行（columnar 批写） | 代码路径推断 + 吞吐反推 | 微秒级（摊销意义上） |
| 同机订阅唤醒 | futex 直接唤醒，无固定轮询 | `active_reader.rs:99`、P011 修复 | 唤醒延迟微秒级可达（Linux） |
| 进程内端到端（写入→订阅可见） | 无直接实测 | — | **推断可达 <100μs**，但缺专项基准 |
| 远端端到端（Gateway TCP） | 无实测；路径含 IPC 编解码 + TCP + tokio | — | **毫秒级**，不应宣称微秒级 |
| 默认 collect 查询 | 一次性整批，20-40ms 级 | P002 | 非热路径，毫秒-几十毫秒 |

### 4.2 "微秒级"成立的限定条件

当前架构在以下限定下可以说"微秒级处理"：

1. **同机、共享内存路径**（写者与订阅者都在本机，经 segment store 而非 Gateway）。
2. **批量写入**（单行摊销微秒级；如果定义是"每条消息独立到达后微秒内可见"，
   则取决于上游攒批窗口 —— 攒批 1ms 窗口意味着端到端至少 1ms，这是吞吐与
   延迟的根本权衡，需要产品层面明确取舍）。
3. **Linux 平台**（futex 通知；其他平台退化为 1ms sleep 轮询，`shm.rs:267`）。
4. **避开 rollover/persist 尖峰**（p99 尾部当前不是微秒级）。

以下场景**不是**微秒级，且短期内架构上达不到：

- 任何经过 Gateway TCP 的远端读写（帧协议 + JSON header + IPC 编解码 + tokio 调度）。
- 行模式订阅的逐行 JSON 序列化（每行 10-50μs 仅序列化，加网络后毫秒级）。
- 含 null batch 的逐行写入路径。
- engine 链路（TimeSeries/CrossSectional/Reactive）上的 P004-P007 遗留热点
  （per-row `String` key 分配、`BTreeMap` 查找、fallback batch 重建）在
  10,000 symbols 规模下未验证。

### 4.3 尾延迟是当前最大的短板

微秒级宣称的真正障碍不是中位数而是尾部：

- copy 路径 p99 = 4,034μs（修复后），与 p50 = 236μs 相差 17 倍。
- 旧数据 p99 曾达 10,729μs，说明 rollover/persist/allocator 尖峰真实存在。
- 没有 p999 采集，行情系统恰恰最关心 p999/p9999（每秒 50 万条时，
  p999 事件每秒发生 500 次）。

---

## 5. 改进清单

按"对 Level2 目标的阻塞程度"排序。P0 = 不做就无法宣称承载目标负载；
P1 = 容量/延迟显著受益；P2 = 体系化保障。

### P0-1 消除含 null batch 的逐行写入 fallback

- 位置：`crates/zippy-engines/src/stream_table.rs:887`（逐行路径）、
  P003 修复边界说明。
- 改进：为 columnar 写入路径增加 validity bitmap 批量写入能力
  （`write_columnar_rows()` 闭包内支持 nullable 列），使含 null batch
  也走单锁批写。
- 验收：构造 50% 列含 null 的 Level2 快照 schema，
  `stream-table-segment-copy` 变体压测 p99 与无 null 路径同量级。

### P0-2 验证并解除 rollover/persist 尖峰与堆积风险

- 位置：`stream_table.rs:891-906`（rollover）、`stream_table.rs:355`
  （无界 persist channel）、`stream_table.rs:1361`（enqueue）。
- 改进：
  1. 把 segment row capacity 配置化并给出 Level2 推荐值（例如 1M-4M 行，
     把 rollover 频率从亚秒级降到分钟级；需同时评估 shm 体积与
     descriptor/retention 行为）。
  2. persist channel 改为有界 + 显式溢出策略（背压或计数告警），
     杜绝静默内存堆积。
  3. 压测 profile 增加"持续 rollover"场景，单独采集 rollover 期间的
     写入延迟分布。
- 验收：50 万行/秒 × 30 分钟 soak，RSS 平稳、persist 队列深度有界、
  写入 p99 不随 rollover 发生数量级抖动。

### P0-3 建立 Level2 容量基准矩阵（把"能否承载"变成可证伪命题）

- 现状：压测只有 1024 symbols、3 秒、单 stream（P012 遗留）。
- 改进：`zippy-perf` 增加 Level2 profile：
  - schema：逐笔 12 字段窄表 + 快照 45 字段宽表（含 nullable 档位列）；
  - 负载：恒定 20 万行/秒 + 周期性 50 万行/秒突发（模拟开盘）；
  - 规模：8,000 symbols；分表策略两档（单大表 / 按板块 16 表）；
  - 时长：30 分钟 soak + 5 分钟峰值段；
  - 采集：p50/p95/p99/p999、RSS 峰值、persist 队列深度、rollover 次数、
    drop/error。
- 验收：报告随 commit 存档（沿用现有 JSON report + git sha 机制）。

### P0-4 上游攒批组件与小批写入性能基线

- 现状：4096 行/批是压测假设，真实行情到达是逐条的；代码库无内置攒批器。
- 改进：
  1. 提供"时间窗（如 500μs-2ms）+ 行数（如 1024）双阈值"攒批写入器
     （Rust 侧，置于行情接入与 materializer 之间），让延迟/吞吐权衡显式可配。
  2. 压测 64/256/1024 行小批下的吞吐曲线，量化 per-batch 固定开销，
     给出最小安全批量建议。
- 验收：64 行/批下仍能维持 ≥50 万行/秒，或文档明确标注最小批量要求。

### P1-1 进程内端到端延迟基准（写入→订阅可见）

- 现状：写入侧与读取侧分别有数据，无端到端打点。
- 改进：增加 profile —— writer 写入带时间戳的行，同机 subscriber 收到后计算
  `now - row_ts`，输出端到端分布。这是"微秒级"宣称的直接证据。
- 验收：同机共享内存路径端到端 p99 < 100μs（批内最后一行口径）。

### P1-2 远端 Gateway write_batch / subscribe 延迟画像

- 现状：ROADMAP 待办，零实测。
- 改进：补 remote write_batch 与 subscribe_table 的 latency profile；
  明确文档化"远端路径是毫秒级，微秒级仅限同机共享内存路径"。

### P1-3 Engine 层 P004-P007 遗留热点在万级 symbols 下的量化

- 现状：id `String` 分配、`BTreeMap` 热路径查找、第三方 factor fallback
  batch 重建均未在 8,000-10,000 symbols 下验证。
- 改进：先压测再决定是否做 id interning / slot arena 专项；
  避免在没有数据时盲目重构。

### P1-4 读侧 zero-copy（P008 遗留）

- 现状：`RowSpanView` 投影仍整列拷贝（`arrow_bridge.rs`），大 tail/大订阅
  batch 时读侧 CPU 放大。
- 改进：Arrow buffer 借用或 chunked borrowed array；对 Level2 日内全量回查
  （亿行级）尤其重要。

### P2-1 性能门禁补 p999 与 RSS

- `zippy-perf` 的 `evaluate_pass()` 已有 p95/p99/queue depth 门禁，
  补 p999 直方图与 RSS 峰值采集，nightly 跑 Level2 profile。

### P2-2 平台与部署约束文档化

- futex 通知仅 Linux 生效，其他平台 1ms 轮询兜底；生产 Level2 部署应
  明确要求 Linux + 建议 CPU 亲和性/隔离核（当前代码无亲和性支持，
  可先用 `taskset`/cgroup 外部约束，是否内置 core pinning 待压测数据决策）。

---

## 6. 总结

**问题一（A 股全市场 Level2 数据量）：有条件可承载，当前不能无条件宣称。**
单机单 stream 实测 100 万行/秒的能力对稳态 10-20 万条/秒有充分余量；
但峰值 30-50 万条/秒的承载依赖四个未闭环的前提 —— 批量写入、规避 null
逐行 fallback、rollover/persist 不堆积、分表策略验证。其中 null fallback
（快照表几乎必然命中）和 persist 单线程跟不上峰值 seal 速度（无界队列
静默堆积）是两个具体的、可能在生产首日就触发的缺陷。

**问题二（微秒级处理速度）：摊销意义上已达到，端到端有条件达到，尾部未达到。**
同机共享内存路径上，批量写入的单行摊销成本 0.05-0.5μs，forward 路径批级
p50=38μs，futex 唤醒订阅无固定轮询 —— 这些构成"微秒级"的坚实基础。但
（a）每条消息独立的端到端延迟受上游攒批窗口下限约束，（b）copy 路径
p99=4ms 距微秒级差两个数量级，（c）任何经 Gateway 的远端路径是毫秒级，
（d）没有端到端实测和 p999 数据。**准确的表述是：同机热路径具备微秒级
中位数处理能力，尾延迟和端到端口径尚未达标、尚未实证。**

最短闭环路径：先做 P0-3（Level2 基准矩阵）和 P1-1（端到端打点）把问题
变成可测量的，再按测量结果做 P0-1（null columnar 写入）、P0-2
（rollover/persist 加固）、P0-4（攒批组件）。没有这组数据之前，
任何"能/不能"的二元结论都缺乏工程依据。
