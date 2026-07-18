# Native QueryService

## 定位

Phase 5 的 QueryService 让单机 `zippy server` 拥有查询的完整计划、快照、执行和资源生命周期。
Python 客户端只发送 SQL 和明确的 Catalog table binding；projection、filter、slice、sort、aggregate、
join 和 window 都在 Server 的 DataFusion 执行器中完成。native 模式不允许“服务器拉全表、Python 再做
residual”这种隐式回退。

该能力默认关闭，适合先在测试环境或指定查询流量上灰度。关闭后，现有 Gateway legacy collect 路径和
本地共享内存订阅路径保持不变。native query frontend 至少需要 Gateway 或 Flight 之一；两者可同时启用，
也可仅启用 Flight。Flight Stage 6.2 的边界见 [`flight-query-service.md`](flight-query-service.md)。

## 启用

正式 native query frontend 依赖 native storage、SQLite shadow Catalog，以及 Gateway/Flight 中至少一个
frontend。现有 Python 客户端仍使用 Gateway 时按以下方式配置：

```toml
[server]
native_storage_enabled = true

[catalog]
backend = "sqlite_shadow"

[gateway]
enabled = true
endpoint = "0.0.0.0:17666"

[runtime]
max_cpu_cores = 4
max_query_buffer_bytes = 536870912

[query]
native_enabled = true
parallelism = 4
operator_memory_bytes = 67108864
max_buffered_bytes = 67108864
plan_ttl_ms = 30000

[query.resources]
max_concurrent_queries = 64
max_active_tasks = 16
max_memory_bytes = 536870912
```

对应的主要环境变量是：

```text
ZIPPY_QUERY_NATIVE_ENABLED
ZIPPY_QUERY_OPERATOR_MEMORY_BYTES
ZIPPY_QUERY_PARALLELISM
ZIPPY_QUERY_MAX_BUFFERED_BYTES
ZIPPY_QUERY_PLAN_TTL_MS
ZIPPY_QUERY_MAX_CONCURRENT_QUERIES
ZIPPY_QUERY_MAX_ACTIVE_TASKS
ZIPPY_QUERY_MAX_MEMORY_BYTES
ZIPPY_MAX_CPU_CORES
ZIPPY_MAX_QUERY_BUFFER_BYTES
```

若只验证标准 Arrow Flight 查询，可关闭 `[gateway]`，增加：

```toml
[flight]
enabled = true
endpoint = "127.0.0.1:17667"
protocol_version = 1
max_message_bytes = 2097152
max_pending_tickets = 64
```

Stage 6.2 允许在同时启用 `[flight.tls]` 和 `[flight.auth]` 时使用非 loopback endpoint；完整安全配置、
Handshake 和凭据文件约束见 [Arrow Flight 查询服务](flight-query-service.md)。现有 Python Table API
尚未切换到 Flight；这段 loopback 配置用于 Rust/integration 验收以及后续客户端迁移，不替代当前远程
Gateway 配置。

`operator_memory_bytes` 未配置时取 `min(64 MiB, query.resources.max_memory_bytes / 2)`，且至少为
1 byte。它是单查询 DataFusion buffering operator 上限；`query.resources.max_memory_bytes` 是全局
QueryService 账本上限；`max_buffered_bytes` 是单查询 Gateway transport 上限。三者用途不同，不应通过
扩大其中一个来规避另外两个。

## Python API

既有 Table API 保持不变。连接到开启 native capability 的远端 Server 后，完整计划自动在服务端执行：

```python
import zippy as zp

session = zp.connect("zippy://10.0.0.8:17690/default")
ticks = zp.read_table("ticks", master=session)
instruments = zp.read_table("instruments", master=session)

query = (
    ticks.filter(zp.col("price") > 100.0)
    .join(instruments, on="instrument_id")
    .with_columns(
        (zp.col("price") * zp.col("multiplier")).alias("notional")
    )
    .select("instrument_id", "ts", "notional")
    .sort("ts")
    .tail(100)
)

result = query.collect()
explanation = query.explain()
profile = query.profile()
```

`tail(n)` 必须有显式、确定的前置 `sort(...)`。没有排序的关系结果不存在稳定的“最后 n 行”语义，native
模式会直接报错，不会退回 Python 全表处理。

任意 SQL 可使用 `read_sql`。简单 `FROM`/`JOIN` 可自动推断 binding；复杂命名、子查询或需要固定
namespace 时应显式传入：

```python
result = zp.read_sql(
    """
    SELECT instrument_id, avg(price) AS avg_price
    FROM ticks
    GROUP BY instrument_id
    ORDER BY instrument_id
    """,
    master=session,
    tables={"ticks": "market_ticks"},
    memory_limit_bytes=128 * 1024 * 1024,
    result_batch_rows=8192,
)
```

`Table.explain()` 会请求 Server 固定一次真实 snapshot、构建物理计划、返回 schema/plan/operator 指标，
随后立即释放计划，不执行结果算子。`Table.profile()` 会执行查询，并在结果之外返回同一组计划指标、
operator memory 峰值、结果 batch/row/byte 和 Gateway transport 指标。

## 快照与实时数据

每次 native 查询都固定一个 Catalog revision、每张表的 persisted snapshot/schema version，以及能够证明
逻辑 offset 连续性的 active segment publication。查询开始后的 append 不可见；已固定 payload 被原地改写
时返回 `DATA_CORRUPTION`，不会返回混合版本。

真正由 native table actor 发布且携带精确 logical offset 的 active/sealed segment 可以和 Parquet 使用同一
DataFusion 计划。旧版生产 writer descriptor 没有这项 provenance 时，Server 必须继续走 legacy live 路径；
不得猜测 offset，也不得把 legacy descriptor 伪装成 native active snapshot。这个边界不影响 persisted native
查询，生产 writer publication 的完整迁移属于后续 authority cutover 工作。

## 背压、取消和资源边界

Gateway 每次只从 QueryService 拉取一个 permit-owned Arrow batch。该 batch 在编码和 socket write 完成前
持续持有 QueryService result-memory permit；写完并释放后才拉下一批。transport 还要取得全局 bounded byte
reservation，因此慢客户端不会让 Server 无界预取结果。

Gateway 在规划、transport admission 和每次等待下一批时同时监听连接断开。客户端断连会 drop result stream，
QueryService 发布取消并释放 DataFusion task、operator memory、Catalog snapshot pin 和 active segment lease。
deadline 使用请求携带的 UTC Unix nanoseconds，并覆盖 planning 和 running 生命周期。

DataFusion 44 的 disk manager 在本 profile 中显式关闭。当前没有可证明配额的 spill/temp directory，因此
sort/hash aggregate/join/window 超过查询或进程内存预算时返回稳定的 `RESOURCE_EXHAUSTED`；不会写入未受管
临时文件。请求非零 spill budget 会返回 `UNSUPPORTED_PLAN`。这是有意的 fail-closed 行为。

## Explain/Profile 指标

指标名称是有界的稳定集合，不包含 SQL、table 名或 operator 实例 ID 等高基数标签：

- `plan.*`：profile version、SQL bytes、binding/input 数、逻辑/表达式/物理节点数、scan 数、计划构建峰值和
  retained bytes；
- `operator.<kind>.count`：scan、filter、projection、sort、aggregate、join、window、limit、union、exchange
  和 other 的物理节点数；
- `operator.memory_*`：单查询 operator limit、当前值、峰值和拒绝次数；
- `result.*`：QueryService 交付的 batch、row 和 accounted bytes；
- `transport.*`：Gateway 实际写出的 batch 和 row。

`operator.other.count` 非零并不自动表示错误；DataFusion 可能使用 empty/placeholder 等基础节点。诊断复杂查询时
应结合 `plan.physical_nodes` 与各已知 category，而不是把单个节点数当作性能结论。

## 推荐灰度顺序

1. 保持 `native_enabled=false`，先用 canary 对 persisted golden query 做等价性验证。
2. 在测试 Server 开启 native frontend，验证 `explain()`、小结果 projection/filter 和预算拒绝。
3. 验证 sort/aggregate/join/window，并观察 `operator.memory_peak_bytes` 和首批延迟。
4. 注入客户端断连、deadline、低内存预算，确认 query/pin/operator/transport 资源归零。
5. 对比开启前后的本地 row/table subscription 延迟 gate；DataFusion 不应进入该热路径。
6. 只对具备 exact logical-offset provenance 的 writer 打开 native active snapshot publication。

回滚只需关闭 `query.native_enabled` 并重启 Server，查询会回到 legacy capability 路由。native 模式运行中
遇到不支持的计划会明确失败，不会自动拉全表执行 residual。
