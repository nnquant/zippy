# 原生 QueryService

本页说明当前原生查询入口。主包和独立客户端通过同一个显式 `Client` 使用 Flight，
持久表目录来自 SQLite Catalog，通用查询由 Server 持有的 DataFusion 执行。
旧 Table/Gateway 查询实现已经退出；下面的 API 是当前唯一公开查询主干。
整体切换状态见[当前能力表](current-capabilities-2026-09-21.md)。

## 启用与连接

SQLite Catalog、原生存储和原生查询默认启用，旧开关
`server.native_storage_enabled`、`catalog.backend`、`query.native_enabled` 已删除，输入会被拒绝。
Flight 默认启用；写入另需启用 `flight.ingest`。通过 `zippy server run` 启动，
客户端 URI 指向用于发现 Flight endpoint 的服务控制地址。

Flight Handshake 签发短期会话。读取、写入、管理使用各自的 bootstrap 凭据；服务端 token
文件需权限 0600。客户端读取凭据可用 `flight_token` 或 `ZIPPY_FLIGHT_AUTH_TOKEN`；远端部署
需 TLS，相关配置见[Flight 查询服务](flight-query-service.md)。不要在示例或仓库中写真实凭据。

```python
from zippy import Client, col

# 凭据由进程环境提供；调用者显式管理连接生命周期。
with Client("zippy://127.0.0.1:17690/default") as client:
    ticks = client.read_table("ticks", namespace="market")
    instruments = client.read_table("instruments", namespace="market")
    query = (
        ticks.filter(col("price") > 100.0)
        .join(instruments, on="instrument_id")
        .with_columns((col("price") * col("multiplier")).alias("notional"))
        .select("instrument_id", "ts", "notional")
        .sort("ts")
        .tail(100)
    )
    result = query.collect()
```

`from zippy_client import Client` 提供同一个类，无主包全局默认连接。惰性 Table API 的 Flight
分支使用共享 SQL 编译器；tail 需要显式前置 sort。业务存在相同时间值时，应补充稳定的排序键。
复杂派生查询的规划预留量当前偏保守；真实 join 验收明确使用 4 GiB，并不代表默认 512 MiB
能够执行同样的计划。内存参数应根据具体计划及资源门禁确定。

## 任意 SQL 的唯一公开入口

使用 `Client.read_sql()`，显式将 SQL relation 绑定到 Catalog 表。主包 `zippy.read_sql()`
及其正则推断绑定已经删除；独立客户端不再发送或接收 Gateway SQL 协议。
Gateway 数据路径和服务端适配器已删除，SQL 请求统一由 Flight 交给原生查询引擎。

```python
with Client("zippy://127.0.0.1:17690/default") as client:
    result = client.read_sql(
        """
        SELECT instrument_id, avg(price) AS avg_price
        FROM ticks
        GROUP BY instrument_id
        ORDER BY instrument_id
        """,
        bindings={"ticks": {"namespace": "market", "table": "market_ticks"}},
        result_batch_rows=8192,
    )
```

`bindings` 必填；不猜测 FROM/JOIN，也不在客户端执行 SQL。已删除仅用于 Gateway 的
`parameters` 参数，需要值绑定时使用 Table 表达式 API。已删除 Flight 中被忽略的 SQL `stream`
参数：传输始终采用 Flight 批次流，返回值仍为合并后的 Arrow Table，不是有界结果迭代器。
请求支持 UTC 纳秒 deadline、优先级、查询内存预算和结果批次行数；错误、认证失败或不支持的
能力直接传播。此处是任意 SQL 的契约。

## 快照、资源和恢复

每次查询固定 Catalog revision、各表的 persisted snapshot/schema version，以及具有精确逻辑
offset 的 live publication。查询期间新写入不改变已固定边界。持久化发布与 WAL 确认不同：
WAL 可恢复不等于对应 Parquet 已进入 Catalog。三类表的完整行、帧与 KV 状态语义由原生表定义决定。

跨次查询需显式固定快照，见[固定快照实施记录](convergence-fixed-snapshot-2026-09-22.md)。
它绑定服务会话、有期限和数量上限；过期或重启后明确失败，不是持久算子 checkpoint。
普通表首次调用 `Table.snapshot()` 后，返回的固定快照会绑定该表后续查询及其派生查询；
多表查询使用同一 `Client.open_snapshot()` 作用域。重复调用同一表的 `snapshot()`
返回相同快照，不重新捕获当前数据。

QueryService 对规划、执行、结果以及快照资源分别记账，Flight 传输保持结果所有权，断连/取消
释放相应资源。扩大传输 batch 上限不能绕过查询内存预算。当前没有实现有配额保障的磁盘 spill，
非零 spill budget 返回 UNSUPPORTED_PLAN，超限返回 RESOURCE_EXHAUSTED。

`Table.explain()` 返回服务端计划；`profile()` 执行并返回计划/执行指标。关注 plan、operator、
result 各组指标及实际错误，不以某个 operator 名称或单次小查询成功推断所有负载可用。

空目录建表、三类结果、共享表达式、显式 SQL 聚合及重启验收使用
[真实原生客户端脚本](../scripts/check_native_client.py)。原生查询是唯一 SQL 主干；
旧权威、主包旧 Table/Session 与相关配置已退出。

独立客户端所有数据操作现已统一 Flight。表查询的 `collect/profile` 不再接收 `stream` 参数；
Gateway transport 与 token 参数均已删除。排序后的 head/slice/tail 在相同 SQL 查询层绑定
ORDER BY 与 LIMIT/OFFSET，保证截取的行符合明确排序。验收见
[第三十四批记录](convergence-flight-only-client-2026-09-22.md)。
