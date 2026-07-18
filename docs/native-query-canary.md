# Native persisted query canary

## 定位

native query canary 是 `zippy-server` 内部的 persisted-only 查询验证入口。它走完整的
QueryService、固定 Catalog snapshot、DataFusion 物理计划、受限 operator pool、Parquet reader 和
result permit 链路，但不会把裸 `RecordBatch` 返回给调用方。Server 逐批消费结果，最后只返回行数、
资源统计、耗时和固定版本的 SHA-256 摘要。

它适合：

- 在 legacy 查询仍为主路径时，对同一份落盘数据做 native golden 对照；
- 在 LDC 或其他单机生产环境验证 snapshot、取消、deadline 和内存限额；
- 收集 native planning / execution 耗时，评估正式切换前的性能。

它不适合作为 Python/远端客户端查询入口。启用 canary 不会自动开启 Gateway；远端能力由独立的
`[query].native_enabled` gate 控制，见 [`native-query-service.md`](native-query-service.md)。canary 仍主要用于
persisted golden 对照，而正式 native query 可以读取具有 exact logical-offset provenance 的 active segment。
当前 persisted planner 要求每个 canonical manifest table 恰好一个 binding，因此同表 self-join / 多 alias
也不属于本阶段的灰度范围。

当前 decoded-output 准入要求完整物理计划的中间和输出 schema 都是固定宽度，并且 operator/expression
位于显式 allowlist。projection、基础定宽 filter、limit/slice、sort、aggregate、join、union 和 window 已接入
同一个 process-wide/operator query memory pool；`Utf8`/`Binary` 等变长中间结果、未知 UDF/expression 或未知
physical operator 会在 reader 创建前返回 `UNSUPPORTED_PLAN`。超出 sort/hash/window 内存上限会返回
`RESOURCE_EXHAUSTED`，不会 spill 到未受配额管理的目录。下文 golden 摘要仍可验证 flat string/binary legacy
结果，因此摘要支持范围可以大于 native 执行 allowlist。

## 配置

canary 默认关闭，并且只能在 native storage 已启用时开启：

```toml
[server]
native_storage_enabled = true

[runtime]
max_cpu_cores = 4

[query.resources]
max_concurrent_queries = 64
max_active_tasks = 16
max_memory_bytes = 536870912

[query.canary]
enabled = true
max_rows = 1000000
max_batches = 4096
max_output_bytes = 268435456
# 省略时取 min(64 MiB, query.resources.max_memory_bytes / 2)
operator_memory_bytes = 67108864
```

对应环境变量：

```text
ZIPPY_QUERY_CANARY_ENABLED
ZIPPY_QUERY_CANARY_MAX_ROWS
ZIPPY_QUERY_CANARY_MAX_BATCHES
ZIPPY_QUERY_CANARY_MAX_OUTPUT_BYTES
ZIPPY_QUERY_CANARY_OPERATOR_MEMORY_BYTES
```

CPU 并行度继续复用 `[query].parallelism`，未配置时继承 `ZIPPY_MAX_CPU_CORES`。canary 的单查询
task/CPU/memory 上限不能超过 `[query.resources]` 的全局 QueryService 上限。

native persisted session 当前显式使用 8192 行 batch，并关闭 DataFusion `CoalesceBatchesExec`。每个文件在
DataFusion 解码前先解析一次 footer、校验 Catalog row count，再为最大 row group 的解压空间和完整
fixed-width Arrow pipeline 申请 task memory。row group 过大时会在 reader 发布前返回
`RESOURCE_EXHAUSTED`，因此生产灰度应同时观察 Parquet row-group 尺寸和 QueryService memory budget。

`max_output_bytes` 约束累计 Arrow result charge 与 canonical scalar bytes 的和。每次只持有当前
`NativeQueryBatch` 的 result permit，完成摘要后先释放该批次，再拉取下一批；它不是全结果 collect。

## Server API

嵌入式 Server 可从 `LegacyServerRuntime` 获取 canary。未配置时返回 `None`：

```rust
let canary = runtime
    .native_query_canary()
    .expect("query canary is explicitly enabled");

let request = NativeQueryCanaryRequest::new(
    query_id,
    session_id,
    "SELECT price, volume FROM ticks",
    vec![NativeQueryCanaryBinding::try_new("ticks", table_ref)?],
)
.with_deadline_ns(deadline_ns)
.with_expected(expected);

let run = canary.submit(request)?;
let instance_id = run.instance_id();
let report = run.execute().await?;

assert_eq!(report.comparison(), Some(NativeQueryCanaryComparison::Match));
```

`submit` 先返回不可克隆的 `NativeQueryCanaryRun`，因此调用方能在执行前取得精确
`QueryInstanceId`。运行期间可用 `canary.query_status(instance_id)` 查询资源，用
`canary.cancel(instance_id)` 取消。canary 只接受自己签发并保留在有界 registry 中的 instance，不能
借用内部 admin handle 操作其他 QueryService 查询。

丢弃尚未执行或执行中的 `NativeQueryCanaryRun` 会发布取消。Server shutdown 会先关闭 canary admission，
再关闭 snapshot admission，最后等待 query、reader 和 runtime pin 排空。

## Legacy golden

legacy 主路径不需要收集完整结果。使用 `NativeQueryCanaryDigestBuilder` 逐批生成预期值：

```rust
let mut digest = NativeQueryCanaryDigestBuilder::try_new(
    primary_query_id,
    primary_schema,
    268435456,
)?;

while let Some(batch) = next_legacy_batch().await? {
    digest.update_batch(&batch)?;
}

let expected = digest.finish();
```

摘要格式为 `zippy-arrow-multiset-sha256-v1`，摘要字符串也携带同名格式前缀。它包含规范化的 Arrow
logical schema、按 key 排序的 schema/field metadata、结果行数和两个独立的 256-bit 行哈希累加器，
并保留重复行的出现次数。摘要忽略 batch 边界和行顺序，因为没有
`ORDER BY` 的 SQL 结果本来就没有顺序语义；因此 legacy 与 native 的合法并行调度差异不会制造假
mismatch。schema、null、primitive 位模式、字符串/二进制值或重复行数量变化都会改变摘要。

摘要构造器使用固定大小 accumulator，不保存行哈希集合。schema 摘要移除 IPC 中非确定顺序的
`HashMap` metadata，再把排序后的 metadata 单独编码；primitive scalar 直接哈希底层位模式，字符串和
二进制值流式哈希，因此 NaN payload 和任意长文本都不会经由有歧义的显示文本比较。schema 编码和
scalar 输入同样受 byte limit 约束，batch schema 不一致会返回 `SCHEMA_MISMATCH`。

当前 golden 摘要只接受 flat scalar 结果列。List/Struct/Map/Union、Dictionary 和 RunEndEncoded 等嵌套或
编码列会明确返回 `INVALID_ARGUMENT`，不会用可能产生同文异值碰撞的 pretty-display 字符串冒充确定性摘要。

## 报告和状态

`NativeQueryCanaryReport` 包含 query instance、Catalog revision、rows、batches、累计 Arrow charge、
canonical bytes、最大单批 charge，以及 snapshot/plan、execution、result wait、digest、总耗时。耗时单位为
微秒；其中 result wait 主要覆盖 DataFusion/Parquet 产出，digest 单独反映 golden 规范化成本。
报告还包含 digest format、digest 和可选 golden comparison。

`NativeQueryCanaryStatus` 提供 active runs、最近受控 instance 数和累计成功、失败、取消、放弃、
match/mismatch、rows、batches、bytes、分阶段及总耗时指标。单查询状态额外暴露 active memory、result permits、
execution-complete 与 cancellation 标志。

所有行数、批次数、累计输出或 operator/query 预算超限都会让 QueryService 进入 `Failed`，而不是只在
canary 外层返回错误。deadline、显式 cancel 和 run drop 保留 `TimedOut` / `Cancelled` 终态。资源拒绝
信息包含 task、CPU、memory 的实际值与上限，便于直接定位应调整的预算。

## 灰度顺序

1. 保持 `[query].native_enabled=false`，仅在测试进程启用 canary。
2. 从 flat scalar 单表 projection/filter 开始，用 legacy batch builder 生成 golden。
3. 检查 mismatch、失败分类、最大单批内存和总耗时，不只比较平均延迟。
4. 扩大到多文件、多表和并发请求，同时验证 cancel、deadline 与 shutdown drain。
5. 上述 golden 稳定后，再按 `native-query-service.md` 开启 bounded Gateway query frontend；标准 Flight、
   TLS/auth 和配额化 spill/temp 属于后续阶段。
