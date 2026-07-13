# ADR-0003：DataFusion 作为通用查询引擎

- 日期：2026-07-13
- 状态：已接受，版本待 spike 固定
- 决策所有者：Zippy core maintainers

## 背景

现有 Gateway 和 Python 客户端共同承担部分查询规划和执行。继续扩展手写 projection、filter、
aggregate、join、sort 和 window 会放大正确性、资源治理及客户端版本漂移问题。

通用查询与实时逐因子回调是不同工作负载。前者需要优化器、spill、取消和 operator metrics；
后者需要稳定的 mmap/segment 低延迟路径。

## 决策

服务器端通用查询基于 Apache DataFusion 扩展，不 fork DataFusion。允许的扩展面包括：

```text
TableProvider
ExecutionPlan
OptimizerRule
ScalarUDF / AggregateUDF / WindowUDF
ObjectStore
MemoryPool / DiskManager
```

DataFusion 负责 SQL 绑定、逻辑和物理优化、通用关系算子、Parquet scan、streaming execution、
memory pool、spill 与 explain metrics。

Zippy 负责 Catalog snapshot、table kind、active/sealed mmap scan、persisted/live overlap resolution、
log offset、frame/key-value 语义、admission 和 session policy。

客户端必须提交完整 SQL 或 typed logical plan，由 server 完成规划和执行。服务端不支持的计划在
执行前返回稳定错误，禁止自动拉取全表到 Python 执行 residual plan。

在引入正式依赖前必须完成版本 spike，使 DataFusion 和 workspace 使用同一个 Arrow 主版本。禁止
长期保留两套 Arrow 类型并在查询热路径转换。

## 结果

正向结果：

- 查询计划、snapshot、取消、deadline 和资源预算由 server 统一拥有。
- Zippy 可以集中实现 active segment 与 persisted file 的一致读取。
- 通用算子获得成熟优化器、spill 和执行指标。

代价与约束：

- DataFusion 编译体积和依赖规模增加。
- 自定义 active segment execution plan 需要持续跟随 DataFusion API。
- 逐 tick callback、实时 factor 和本地 subscription 不得改走 DataFusion。
- 引入前必须以已有 query golden fixtures 验证新旧结果一致。

## 替代方案

继续手写查询引擎：短期依赖少，但 aggregate/join/window、优化器、spill 和取消的长期成本不可控。

只在 Python 使用 Polars/DuckDB：无法让 server 拥有 snapshot 和资源生命周期，也会要求远端拉取
不必要数据。

嵌入其他完整数据库：会与 Zippy 的 active mmap segment、表语义和 ingest 生命周期形成双重
storage authority。

## 重新评估条件

若版本 spike 无法消除 Arrow 类型复制，或关键 golden query 无法通过公开扩展 API 正确实现，可在
Phase 5 开始前重新评估查询内核。不得以 fork DataFusion 作为默认补救方式。
