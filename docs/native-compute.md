# 原生本地计算

`zippy.NativeProcessor` 直接调用现有 Rust 因子内核，接收严格 schema 的 Arrow RecordBatch；
`zippy.ComputePipeline` 独占内核，使用 `zippy-client` 原生 Flight 订阅和写入。计算节点不注册
旧 Master source，不创建旧 SHM publisher，也不经过 Gateway。

支持的工厂为 `reactive`、`latest`、`timeseries`、`cross_sectional`、`bars`、`identity`、`chain`。因子规格和
交易时段 profile 与原内核共用；`latest` 输出本批键更新，截面引擎输出完整已关闭时间桶。
`flush()` 关闭当前待输出窗口，`finish()` 输出余下窗口并关闭内核，`close()` 直接丢弃待输出状态。
输入 schema（含 metadata）必须完全一致，不执行隐式类型转换。
reactive/timeseries 的 id_filter 必须非空且不含空白标识；匹配保留原字符串，不自动 trim。
流水线在打开源日志时就校验内核 schema，空表也会立即拒绝不匹配；不等第一条数据到达。
普通订阅也可用 `SubscriptionOptions(expected_schema=...)` 声明这一约束。

下面示例使用已有的独立 `Client` 和命名空间。建表身份由调用者生成并保留；表名不能代替身份。

```python
import uuid

import pyarrow as pa
from zippy import ComputePipeline, NativeProcessor, TS_MEAN

schema = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.float64()),
])
processor = NativeProcessor.reactive(
    "mean_price", schema, "symbol",
    [TS_MEAN(column="price", window=2, output="mean_price")],
)
output_id = uuid.uuid4()
client.create_append_table(
    "mean_price", processor.output_schema(), namespace_id=namespace_id,
    table_id=output_id, request_id=uuid.uuid4(),
)
with ComputePipeline(
    client, processor, output="mean_price", output_table_id=output_id,
    output_namespace="market", durability="wal",
) as job:
    job.write(pa.Table.from_pydict(
        {"symbol": ["A", "A"], "price": [10.0, 12.0]}, schema=schema,
    ))
```

流水线构造成功后，`processor` 原句柄关闭，现有状态转交流水线。独立使用处理器时，可调用
`process()` 获得输出 RecordBatch 列表，直接处理结果，不创建任何服务端表。

## 多级内核与无因子生产者

`NativeProcessor.chain(name, processors, max_intermediate_bytes=67_108_864)` 将多个内核
顺序连接，上一级完整输出直接进入下一级。它接管各级已有的进程内状态；相邻 schema（包括
metadata 和 nullability）、重复或已关闭句柄、非正字节上限均在转移前拒绝，构造失败不消费
有效内核。构造成功后原句柄关闭，链可以独立处理 Arrow 输入，也可以交给 ComputePipeline。

```python
from zippy import ExpressionFactor

derived = NativeProcessor.reactive(
    "derived", processor.output_schema(), "symbol",
    [ExpressionFactor(expression="price * 2.0", output="twice")],
)
processor = NativeProcessor.chain("mean_then_derived", [processor, derived])
```

上例接在首个示例的处理器构造之后、建输出表之前；已转交流水线的句柄不能再次串联。
flush/finish 按上游到下游顺序处理：上游尾批先送入下游，然后下游才收尾。批次顺序和边界
保留，各级输出按构造时冻结的 schema 校验。执行期间失败或超限后整链终止，不对部分已更新
状态进行回滚，不能重试相同输入。当前支持顺序连接，尚不提供分叉 DAG 或持久算子 checkpoint。

字节上限约束每级返回的输出集合，以及一次 flush/finish 合并后的最终输出；使用 Arrow buffer
大小加批次结构计费，共享 buffer 也重复计费。它不是进程 RSS 上限，不包含内核持久状态，也
不能阻止内核调用内部的瞬时分配。流水线本身的输入/输出上限继续生效。

无需计算的采集源可使用 `NativeProcessor.identity("raw", schema)`，通过相同流水线写入。
它保留原 Arrow buffers、顺序、重复值、空值和完整批次，不要求 symbol 或时间字段，也不
加入去重、键状态或虚构因子。输出表自身的 append/frame/KV 语义继续决定持久结果。

## Python source 输入

`source` 也可传入实现 `zippy.source_input.SourcePlugin` 协议的对象。它声明精确 Arrow schema、
源名称，并通过 `_zippy_start(sink)` 返回持有 `stop()`/`join()` 的采集句柄。已有
`ParquetReplaySource` 可直接接入：

```python
from zippy import ParquetReplaySource

source = ParquetReplaySource("ticks.parquet", batch_size=1024)
# processor 的输入 schema 必须与该 Parquet source 的声明完全相同。
with ComputePipeline(
    client, processor, source=source,
    output="factors", output_table_id=output_id,
    output_namespace="market", durability="persisted",
) as job:
    source.wait_replay(timeout=30)
```

源依次发 hello、完整 Arrow Table/RecordBatch、flush、stop；错误用 emit_error 报告。
hello 必须匹配声明名称和当前协议 1。数据交付同步执行内核并等待输出回执，没有内部预取队列；
多次 sink 调用串行执行，不接受隐式字典转 Arrow 或 schema 转换。源自身须保证事件的业务顺序，
控制它拥有的采集缓存，并在 stop() 中请求取消、join() 中等待工作线程退出。

插件源不使用 Master 注册、Gateway 或输入原生日志，因此不能同时传 `input_options`，也没有
原生日志 cursor/checkpoint。停止先取消源、等待工作线程，再收尾内核和输出 writer；默认最多
等待源 join 五秒，这个期限不包括插件自己的 stop() 调用。stop() 必须及时返回，生命周期由
外部调用方管理。取消后的迟到数据不会进入计算；真实采集或写入错误仍终止任务并保留首个异常。
Parquet source 是有限输入：读取完毕后依次 flush/stop 并释放线程，`wait_replay()` 返回完整回放
是否成功，取消返回 False；`job.join()` 也可等待源结束。此时任务状态为 source_stopped，退出
流水线上下文才完成内核收尾及 writer 关闭。输出表归服务所有，源和任务结束后仍可查询。

显式 schema 是严格约束，不是转换请求；所有 Parquet fragments 的字段、nullability 和 metadata
必须一致。`batch_size` 必须是正整数；`replay_rate` 可省略或指定有限正数，每秒行数是唯一速率
配置，旧 `mode` 已删除。速率等待可被取消，不会因低速回放阻塞停服数小时。

旧 `TableReplayEngine` 和全局 `replay()` 已删除，回放助手不再从旧 registry 解析 Parquet 路径或
注册 SHM 输出。文件写入使用上述 ParquetReplaySource + ComputePipeline；命名表历史输入使用
`source="ticks"` 与 `SubscriptionOptions(namespace="market", start="earliest")`，明确选择输入
游标和任务停止边界。后者是持续日志，不能把它误认为仅遍历旧 persisted_files 的有限文件回放。
需要固定历史结果时先用 Client 的显式快照查询取得所需范围，再按完整批次 `job.write()`。

仅需本地回调时使用 `ParquetReplayEngine(path, callback=..., table_callback=...)`。它复用同一个
有限源，默认按 Row 交付（时间戳为 schema 单位的整数）；table_callback=True 保留完整 Arrow
批次。已删除 output_stream/master/persist 等旧发布参数，构造回调引擎不会创建任何服务端表。

自定义插件的采集游标、重新连接与重放仍由插件明确管理。只有内核已生成的持久输出可以按上述
原生 writer 契约重试；源重新启动不自动恢复内核状态。三类结果的真实验收见
[第三十批实施记录](convergence-source-input-2026-09-22.md)。

订阅驱动时传 `source="ticks"` 和
`SubscriptionOptions(namespace="market", start="latest")`，然后 `start()` / `join()` / `stop()`。
不要混用订阅和 `write()`。调用者持有 Client 生命周期，应先停止流水线再关闭 Client。
`join()` 传播原始失败；`metrics()` 包含源健康、已完成输出、最后回执及失败写入的已确认回执。

输出可选 `append`、`frame`、`key_value`，对应追加、完整帧、键更新。frame 查询返回帧历史，
不等于最新帧；订阅保留每个完整帧的边界。KV 表必须预先声明非空键。每次打开输出流都校验
`output_table_id`；服务端未提供 `ingest.table_identity.v1` 时直接拒绝启动。同名重建表不能
承接旧任务的结果。输出默认 WAL；`persisted` 等待 Parquet/Catalog 发布后才推进输入处理游标。

## 恢复边界

- 算子状态位于计算进程内。原始输入订阅断开后可凭处理游标续读，但该游标不是算子 checkpoint。
- 计算进程重启时，需要显式重建状态或重新播放输入；尚未提供原子持久化算子状态与输出提交。
  旧计算内核也没有跨进程持久 checkpoint；这是后续新增能力，不是此次删除旧路径造成的能力回退。
  因此流水线拒绝 durable consumer 配置，不把输入 ACK 冒充计算状态保存。
- 已确认输出按所选持久性恢复。WAL/Persisted 默认允许每批 5 次输出重连，可用
  `output_reconnect_attempts=0..10` 调整；需要服务能力 `ingest.resume_after_restart.v1`。
  服务重启或丢回执时重放同一输出及请求身份，保留原 producer epoch/批次序号，不重算输入。
  Memory 不启用重试；重试耗尽、身份冲突或协议错误仍终止流水线。详见
  [计算输出恢复](convergence-compute-recovery-2026-09-22.md)。
- 内核失败、输出失败或超限后均终止当前流水线。模糊写入可能已提交部分输出，
  以异常回执和服务端记录确定边界，不能对有状态算子盲目重试同一输入。
- 原生 KV 变更日志可以作为显式事件输入，必须使用完整日志 schema，并由计算逻辑明确处理
  删除操作；它不等于普通行情行或当前状态快照。状态视图到因子输入的适配仍待补齐。

## KV 事件输入

旧 `KeyValueTableMaterializer.on_data()` 向下游输出协议 v2 变更日志；原生计算保留该契约。
先通过 `client.open_log(table_id=...).schema` 取得精确 schema，再据此创建 NativeProcessor。
控制列 `_zippy_kv_seq`、`_zippy_kv_op`、`_zippy_kv_snapshot_version`、
`_zippy_kv_batch_last_seq`、`_zippy_kv_stream_id` 都保留，Flight 分片不会拆开一次计算输入。

`start="earliest"` 显式回放保留的历史事件；`start="latest"` 只消费后续变更，不自动补一份
当前状态。断线后从已完成计算的原游标续读，不能用新快照跳过中间事件来冒充连续恢复。
删除行的非键业务值不具有数据意义；若计算价格均值等普通行情因子，不能直接把整份 KV 日志
交给它。也不能丢弃操作列后把删除输出写成 KV upsert。事件统计可以显式对事件序号等控制值
计算，同时保留原操作和边界；序号为 int64，浮点因子须使用表达式 `CAST(column, 'float64')`
显式转换。

真实验收在同一个有状态内核中对每键事件序号求两期均值，跨服务重启保留更新、删除、清空和
重插顺序；再将输出中原始协议列交给 `KeyValueState`，检查状态确实清空并可重建。
这是事件计算及状态重放验收，不是价格因子删除重置或算子持久 checkpoint 实现。
示例见 [KV 计算验收脚本](../scripts/check_compute_kv_input.py)。

Python 旧 Session/Pipeline、隐式表发布与主包订阅实现已经删除；表操作统一为显式 Client，
计算发布统一为 ComputePipeline。Python 扩展不再导出旧后台引擎、Master/Gateway、ZeroMQ、SHM reader/subscriber 或 materializer；
旧 native_source_sink.v2 capsule 及 WindowType/LateDataPolicy/OverflowPolicy/SourceMode 常量也已删除。
内核使用上述 NativeProcessor 工厂，迟到策略使用显式字符串，采集插件使用 SourcePlugin 协议。
Rust 服务端内部旧 source/materializer 与旧数据权威已退出；原生计算复用同一上下文因子内核。
当前入口与验收见[第四十三批记录](convergence-python-native-bindings-2026-09-22.md)及
[原生示例指南](../examples/examples_guide.md)。
