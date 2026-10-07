# zippy-client

`zippy-client` 是 Zippy 的轻量 Python 客户端发行包，提供连接、查询、订阅和写入，
不包含 Server、Master、Gateway daemon、WebUI、Compaction 或计算引擎。

KeyValue 订阅返回 v2 增量，保留序号、版本和批次边界；不能把单次回调当作完整市场状态。
完整初始化流程见仓库 `docs/key-value-v2-migration.md`，状态重建前不要过滤或截断增量。

## 原生日志与显式续读

`Client.open_log` 使用原生 Flight：append 返回有序批次，frame 返回完整帧，KV 返回完整 mutation。
`batch_rows` 只限制传输分片，`max_pending_bytes` 限制原子组装缓存。latest 起点不回放已有数据；
`start="earliest"` 从 offset 0 读取。它不依赖旧 callback 订阅，也不会自动重连或切换 Gateway。

```python
with client.open_log("frames", namespace="market", start="earliest", batch_rows=128) as events:
    event = next(events)
    process_frame(event.table)  # 完整帧；业务处理成功后保存游标
    saved_cursor = event.cursor

with client.open_log(cursor=saved_cursor) as events:
    next_event = next(events)
```

游标绑定 table ID 和 schema；缺口或身份变化明确报错。读取失败时 `events.cursor` 保留上一次
完整交付边界，未完成的帧/mutation 会丢弃。游标不等于服务端 durable ACK，也不代表调用者业务
处理已经成功。KV 完整 mutation 仍不是完整状态快照；主包 `zippy.StateSubscription` 已基于此接口
提供一致快照和自动状态恢复。独立客户端 callback 已使用同一原生日志，主包旧 callback 已删除，主包共享同一个 Client。
使用 context manager 或 close/stop 取消阻塞读取；Client.close 同样会停止其全部订阅。

需要服务端保存处理进度时，使用固定的非零 `consumer_id` 和 `start="committed"`，要求服务公布
`subscription.durable_log.v1`。例如：

```python
with client.open_log(
    "frames", namespace="market", start="committed", consumer_id=consumer_id,
) as events:
    for event in events:
        process_frame(event.table)
        events.acknowledge(event)
```

每次只能有一个未确认单元；处理成功后显式 ACK，收到持久回执后才能读取下一批。服务先确保数据
已进入 manifest，再提交消费位点。未确认数据在断连或重启后重放，业务处理应能承受重复执行。
`group_id` 可指定共享位点，同一表的组只允许一个活动成员；这不是分区分发或自动再平衡。
组成员使用各自固定的 `consumer_id`；同一 consumer ID 不能改绑另一张表。

如果抛出 `AmbiguousAcknowledgementError`，确认可能已提交，应使用相同 consumer/group 身份重新
打开，让 Catalog 决定重放位置；不能用本地交付游标假定 ACK 成功。`SCHEMA_MISMATCH` 则要求按
新 schema 重新打开连接。持久模式不接受 `cursor`、latest 或 earliest 起点，也不自动重连。
KV 当前采用每行一个计费片段组装完整 mutation，吞吐优化仍在推进，主包旧 callback 已删除。

两包要求 PyArrow >=23；Flight 读取按数据类型对齐缓冲区，保证原生状态引擎安全导入 Arrow 数据。
已验收 23.0.1 和 25.0.1，不代表全部后续版本均已验收。

## 安装

```bash
uv add zippy-client
```

## Flight 会话

所有 Flight 连接都先 Handshake，由服务器签发短期 session ID 和 bearer。远端部署必须启用 TLS
及 bootstrap 认证；loopback 可显式启用只读握手，两者共用会话生命周期。客户端不再自选 session ID，
不接受旧 `x-zippy-session-id` 协议；已配置凭据时不会静默改用无需 bootstrap 的部署。

`Client.close()` 先停止本客户端订阅与写入流，再通过 `zippy.close_session.v1` 撤销其持有的
各角色会话，包括刷新后仍被固定快照使用的旧会话。服务端立即回收对应的未消费查询 ticket、
固定快照租约和会话名额；不会撤销其他客户端的会话。已开始的服务端 RPC 仍遵循各自的取消和
deadline 契约。新客户端要求服务端支持该 action，没有旧服务端回退路径。
网络断开或会话已经失效时，本地仍关闭所有通道并记录警告；此时不能据此确认远端立即回收，
服务端 TTL 负责最终清理。应始终使用 context manager 或显式关闭客户端，不能依赖垃圾回收。

## 显式 Client API

连接由调用方持有，每个 Client 独立管理自己的订阅和进程租约：

```python
import zippy_client as zp

client = zp.Client("zippy://127.0.0.1:17690/default")

ticks = (
    client.read_table("ldc_tgw_ticks", snapshot=False)
    .filter(zp.col("instrument_id") == "110059.SH")
    .select("instrument_id", "dt", "last_price")
    .sort("dt")
    .tail(1000)
    .collect()
)
```

使用 `with zp.Client(uri) as client:` 或显式 `client.close()` 管理生命周期。
不再提供进程级默认连接、模块级读写/订阅函数和 `master=` / `local=` 兼容参数。
省略 URI 时只读取 `ZIPPY_CONTROL_ENDPOINT`；已删除的 `ZIPPY_MASTER_URI` 和
`ZIPPY_MASTER_ENDPOINT` 即使与显式 URI 同时存在也会报错，启动前应清除旧环境变量。
`tcp://` URI 只接受主机和端口；路径、用户信息、查询串或片段不会被忽略后继续连接。
远端 `zippy://host:port` 可省略路径；若带路径，只接受 `/default`，其他 profile 会报错。
Windows 客户端的默认逻辑 URI 使用 TCP；显式 Unix socket URI 或文件路径会报错。
在非 Unix 平台，`zippy://other` 等具名本地端点会报错，不会连接到默认服务；
远端使用显式 `tcp://host:port` 或 `zippy://host:port/default`。
创建第二个 Client 不会替换或关闭第一个；用 `transport=` 约束 Flight endpoint 的本地／远端范围。

查询默认每次 collect 获取独立的一致快照。Flight 服务公布 `query.fixed_snapshot.v1` 时，
`read_table(..., snapshot=True)` 在创建句柄时固定原生数据与 schema，后续 collect 和派生查询
复用同一边界。多表查询通过同一 `Client.open_snapshot()` 作用域捕获，再创建各表和 join：

```python
with client.open_snapshot([("market", "ticks"), ("market", "instruments")]) as snapshot:
    with snapshot.read_table("ticks", namespace="market") as ticks:
        with snapshot.read_table("instruments", namespace="market") as instruments:
            with ticks.join(instruments, on="symbol") as query:
                first = query.collect()
                second = query.collect()  # 相同数据边界，包含捕获时的 schema。
```

默认请求 60 秒租约，可用 `ttl_ms` 指定 1–300000 毫秒，实际到期时间不晚于认证会话到期时间。
每个服务最多保留 32 个固定租约；逐查询元数据继续独立计费，原 manifest/live 数据有 pin 保护。
作用域、表和派生查询各自持有引用，关闭原始句柄后派生查询仍可执行；最后一个引用关闭时释放。
客户端在会话提前换新后仍使用快照的原会话，直至其到期。过期、释放或服务重启后明确失败，
不会自动重新捕获最新数据。固定快照不提供持久检查点，也不与 KV 原子状态游标请求混用。
固定表的 `snapshot()` 返回已有快照元数据；普通表首次调用 `snapshot()` 会固定当前数据与
schema，之后的 collect 和派生查询使用同一边界，重复调用返回同一快照。
所有固定快照均使用原生 Flight 租约。
Flight 的 `list_tables()`、`read_table(wait=True)` 和表 metadata/schema 均读取原生 Catalog，
不再回退到旧 registry。查询结束后的快照版本可从 `profile()` 的 query metadata 查看。

`Client.read_sql()` 是主包与独立客户端共用的任意 SQL 入口，只使用 Flight 提交给服务端执行。
主包可通过 `from zippy import Client` 使用同一个类；旧 `zippy.read_sql()` 及独立客户端 Gateway SQL 路径已删除。
Flight version 1 要求显式声明 SQL relation
与 Catalog table 的绑定，服务端不会从 SQL 文本猜表：

```python
result = client.read_sql(
    "select instrument_id, max(dt) from ticks group by instrument_id",
    bindings={"ticks": "ldc_tgw_ticks"},
)
```

Flight SQL 必须显式提供 `bindings`。查询 bootstrap token 使用 `Client(flight_token=...)`
或 `ZIPPY_FLIGHT_AUTH_TOKEN`；writer/admin 使用对应的显式参数或环境变量。旧 `token` 参数
及 `ZIPPY_GATEWAY_TOKEN` 读取路径已删除。

任意 SQL 已删除旧 `parameters`；SQL、Table.collect/profile 均删除无效的 `stream` 选项。需要值表达式时使用 Table API；
SQL 传输始终是 Flight 批次流，返回值为合并后的 Arrow Table。

## Transport 路由

公开 transport 名称为：

- `auto`
- `flight_local`
- `flight_remote`

`auto` 根据已公布 endpoint 的位置选择 Flight；显式范围必须与 endpoint 一致：

| 操作 | 唯一路径 |
|---|---|
| 实时订阅 | 原生 Flight 日志（本地/远端使用同一处理与 ACK 规则） |
| 表查询 | 原生 Flight |
| SQL | 原生 Flight |
| 写入 | 支持 ingest 的原生 Flight |

`Client.subscribe()` / `subscribe_table()` 使用原生 Flight 日志。旧 `GatewaySubscriber`、
`ShmSubscriber`、`shm_available` 和 `shm` transport 已删除，不再选择旧 Gateway/SHM
或自动切换到最新位置。表必须已在 Catalog 中创建；`start="latest"` 在调用返回前固定起点，
空表可先订阅、后启动 writer。`start="earliest"` 读取完整保留日志。

```python
from zippy_client import SubscriptionOptions

subscriber = client.subscribe(
    "ticks", on_tick,
    options=SubscriptionOptions(namespace="market", instrument_ids=("110059.SH",)),
)
# Frame/KV 必须使用 subscribe_table：一次完整 frame/mutation 对应一次回调。
subscriber.stop()
```

`options.filter` 接受 `pyarrow.compute.Expression`（例如 `pc.field("price") > 0`），
只筛选 append 事件；`count` 显式截取每个过滤后 append 批次的尾部。Frame/KV 禁止过滤和截断。
`batch_rows` 是传输片段上限，不定义 frame/mutation 边界，也不保证 append 回调大小。
`throttle_ms` 在完整回调单位之间施加背压，不增加无界缓存或等待凑批。
旧 `transport`、`start_from`、`xfast`、`poll_interval_ms`、`allow_data_loss` 等参数已删除。

普通订阅仅在整批回调成功后保存 `subscriber.cursor`；重连使用固定表 ID 和该位置，
不会重新解析同名替代表。`stop()` 等待回调退出，回调应有界；可在回调内停止自身，此时
本批不推进位置。回调异常无论异常类型都直接终止，通过 `join()` / `stop()` 传播。

持久处理使用 `SubscriptionOptions(start="committed", consumer_id=uuid, group_id=group)`。
ACK 在回调成功后提交；服务重启后同一身份从 Catalog 位置继续。ACK 丢回执时自动重连，
服务器可能重发已处理批次，因此回调副作用必须幂等，这是 **at-least-once**，不是 exactly-once。
Schema 变化、表删除、连续性错误终止订阅，需要业务显式处理后重新打开。

## Flight TLS 和写入

私有 CA、mTLS 和 server name 使用以下参数或对应环境变量：

```python
client = zp.Client(
    "zippy://10.0.0.8:17690/default",
    flight_token=read_token,
    flight_writer_token=writer_token,
    flight_tls_root_cert_file="zippy-ca.pem",
    flight_tls_cert_chain_file="client.pem",  # 仅 mTLS 需要
    flight_tls_private_key_file="client.key", # 仅 mTLS 需要
    flight_tls_server_name="zippy.internal",
)
```

```text
ZIPPY_FLIGHT_AUTH_TOKEN
ZIPPY_FLIGHT_AUTH_WRITER_TOKEN
ZIPPY_FLIGHT_TLS_ROOT_CERT_FILE
ZIPPY_FLIGHT_TLS_CLIENT_CERT_FILE
ZIPPY_FLIGHT_TLS_CLIENT_KEY_FILE
ZIPPY_FLIGHT_TLS_SERVER_NAME
```

服务端已公布 Flight endpoint 时，Flight 连接、协议或 capability 错误直接返回，不降级到 Gateway。
缺少 Flight endpoint 时连接立即失败。Gateway 查询、写入、独立帧协议、能力广告和传输选项
均已删除。writer schema 查找失败直接传播原错误，不以未指定 schema 的写入掩盖权限或目录错误。

`get_writer()` 默认请求 `wal` durability，自动按服务端上限切分 Arrow batch。同一个 writer 会跨
`flush()` 复用 producer、connection、epoch 和开放的 DoPut stream，在 batch/row/byte 配额或 RPC
deadline 前有界轮换，并在每次 `flush()` 返回前逐批校验 request ID、连续 log offset、水位和 WAL
ACK。需要最低延迟且明确接受进程
崩溃丢失未提交尾部时，才显式选择 `memory`：

```python
with client.get_writer(
    "ldc_tgw_ticks",
    batch_size=4096,
    flush_interval_ms=10,
    durability="wal",
) as writer:
    writer.write({"instrument_id": "110059.SH", "dt": ts, "last_price": 121.35})
    writer.flush()
    print(writer.last_receipts[-1]["committed_log_offset"])
```

同一个 `FlightWriter` 保留稳定 producer/connection ID，并把前一次服务端 epoch 作为下一次 resume 的
期望值；其他连接抢先接管表时会明确失败。若网络在提交与回执之间中断，最后一批仍可能状态不确定，高层 writer 不会
用新身份盲目重试，而是永久 fail-closed。`AmbiguousWriteError.receipts` 只列出已明确收到的回执；调用方
按稳定 offset 或业务主键核对后创建新 writer。

需要在存储服务重启或丢回执后自动恢复时，可在 `client.flight.write_table()` 显式指定
`max_reconnect_attempts=1..10`，同时提供 `expected_table_id`，且使用 WAL/Persisted。
服务必须公布 `ingest.resume_after_restart.v1`。恢复模式将每个有界 Arrow batch 放进独立 DoPut，
同一重试保留完整输出、put ID、producer/connection ID、批次序号和原 epoch；服务端验证指纹并重放
原回执。这样会增加每批 RPC 开销。内存确认不提供这种恢复，schema/身份改变、writer 被接管或
协议损坏也不会盲目重试。耗尽重试后仍抛出 `AmbiguousWriteError`，已收到的回执保留在 `receipts`。

主包 `ComputePipeline` 对持久输出默认使用 5 次重连，可用 `output_reconnect_attempts` 调整或关闭。
存储服务重启时本地内核继续持有状态；网络重试只重新发送已经计算出的输出，不重新执行内核。
这不等于计算进程自身的持久 checkpoint；计算进程重启仍须明确重建状态。

原生 append 服务公布 `ingest.append.persisted.v1` 时，可指定 `durability="persisted"`。
每批会等待 Parquet/manifest 覆盖相应 offset，并将回执持久归档后才返回；回执的
`persisted_snapshot_version` 标识确认时的 manifest 版本。此方式增加每批持久化开销，
默认仍为 WAL。服务没有对应能力时明确报错，不降低确认等级。

Flight 默认每次 terminal query 都固定一个服务端 snapshot；同一次 `collect()` 内结果一致。
跨查询固定快照须使用上述租约 API，缺少相应服务端能力时明确拒绝。`tail()` 必须先显式
`sort()`，否则客户端会拒绝不确定的“最后 N 行”。

## 协议代码生成

修改 `crates/zippy-protocol/proto/zippy/flight/v1/protocol.proto` 后重新生成 Python protobuf：

```bash
uv run --package zippy-client --group dev python -m grpc_tools.protoc \
  -I crates/zippy-protocol/proto/zippy/flight/v1 \
  --python_out clients/python/src/zippy_client/_generated \
  --pyi_out clients/python/src/zippy_client/_generated \
  crates/zippy-protocol/proto/zippy/flight/v1/protocol.proto
```

## 构建发行 wheel

客户端使用固定版本的 setuptools 构建，不编译 Rust，也不需要 Cargo workspace：

```bash
uv build --package zippy-client --out-dir /tmp/zippy-client-dist
```

产物包括源码包和 `zippy_client-0.1.0-py3-none-any.whl`。客户端 wheel 本身不绑定 CPU、
操作系统或 CPython ABI；PyArrow 等依赖仍需匹配平台，不能据此宣称所有平台运行测试已通过。
CI 的发行验收会从源码包重建 wheel，再在仓库外新环境中安装，检查类型标记和生成协议文件。

## 依赖边界

`zippy-client` 默认依赖 PyArrow、protobuf 和 Loguru。旧 `_shm` 扩展及其 Rust crate、
类型桩、构建配置均已删除，发行包不包含本地二进制扩展。
它不依赖完整 `zippy` 包。完整 `zippy` 可以依赖 `zippy-client`，依赖方向不能反转。

## 原生在线建表

启用 SQLite Catalog、native storage/query、Flight ingest，并配置相互独立的 read、writer、admin
凭据后，可以从空数据目录在线建表。客户端管理员凭据使用 `flight_admin_token` 或环境变量
`ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN`；它不会替代查询和写入凭据。

SQLite Catalog 当前只接受物理布局 v11；新目录直接初始化，旧 v1–v10 目录明确拒绝，
不会自动升级或覆盖。开始使用该版本时需提供新数据目录。

```python
import uuid
import pyarrow as pa
from zippy_client import Client

# 保存这些 ID；请求超时或重启后重试时，复用相同 ID 和完整请求内容。
namespace_id, namespace_request, table_id, table_request = (uuid.uuid4() for _ in range(4))
with Client("zippy://default") as client:
    client.create_namespace("market", namespace_id=namespace_id, request_id=namespace_request)
    client.create_append_table(
        "ticks",
        pa.schema([("value", pa.int64())]),
        namespace_id=namespace_id,
        table_id=table_id,
        request_id=table_request,
    )
    with client.get_writer("ticks", namespace="market", batch_size=1) as writer:
        writer.write({"value": 42})  # 默认等待 WAL 确认
    print(client.read_table("ticks", namespace="market").collect())
```

创建成功表示 Catalog、TableActor、materializer 和 ingest 均已就绪。并发管理超额会明确返回
`RESOURCE_EXHAUSTED`；重复请求复用原结果，修改同一 request ID 的内容会返回冲突。
append/frame/key-value 在线建表和删除均已支持；原生日志使用 `Client.open_log()`。
同名重建必须使用新 table ID；schema 变更使用下文的原生管理接口，主包订阅统一使用显式 Client。
Flight 写入现在必须显式携带 namespace、producer ID 和 connection ID，客户端会生成并发送后两者，
服务端不再从 put ID 推导缺失身份。旧控制错误缺少 `code` / `retryable` 时同样明确拒绝。

运行 `uv run --no-sync scripts/check_native_client.py` 可验收真实 Rust server 与 Python Client
的三类表空目录建表、持久写入、重启读取、删除、订阅终止和删除后重启幂等重试；
先执行 `cargo build -p zippy-server`。
该验收目前在 Linux 上执行。


## 原生在线删除

服务端公布 `catalog.drop.v1` 后，管理员可按不可变 table ID 和已观察到的 snapshot version 删除：

```python
observed = client.read_table("ticks", namespace="market").head(1).profile()
snapshot = observed["metrics"]["query"]["snapshots"][0]
drop_request = uuid.uuid4()  # 保存此 ID 和下面完整参数，响应丢失后原样重试
result = client.drop_table(
    snapshot["table_id"],
    expected_snapshot_version=snapshot["snapshot_version"],
    request_id=drop_request,
)
```

快照已变化时返回 `CONFLICT`，调用方应重新检查目标；管理或持久任务忙碌返回可重试的
`RESOURCE_EXHAUSTED`。成功返回前，Catalog 已提交删除，旧写入者、actor 和 materializer 完成退出，
运行时表数量及元数据预算回收。持续日志随后以 `NOT_FOUND` 结束，已交付给客户端的批次不能撤回。
查询已固定的快照及其文件继续受 pin/lease 保护。删除是逻辑删除，物理文件由 GC 管理；
WAL/回执目录尚未自动回收。删除后的名称可用新 table ID 重建；
旧 table ID 永不复用，旧请求和旧游标仍绑定原表，不会跳转到同名新表。

## 原生 schema 变更

服务端公布 `catalog.schema.v1` 后，管理员使用逻辑用户 schema 和已观察到的 schema version：

```python
observed = client.read_table("ticks", namespace="market").head(1).profile()
snapshot = observed["metrics"]["query"]["snapshots"][0]
change_request = uuid.uuid4()  # 保存 ID 及完整参数；丢失响应后原样重试
result = client.alter_table_schema(
    snapshot["table_id"],
    pa.schema([("value", pa.int64()), ("note", pa.string())]),
    expected_schema_version=snapshot["schema_version"],
    request_id=change_request,
)
```

三类表共用此入口。KV 必须同时传 `keys=["原主键", ...]`，保持原顺序；传用户字段，
不要传 `open_log()` 返回的物理事件 envelope。显式 keys 让请求在表删除后仍可独立重放。
允许尾部追加可空列、普通字段放宽可空性及描述元数据变化；不删除、重排、重命名或转换已有字段。
KV 主键、frame 分组、append 事件时间和 Arrow extension 的语义保持不变。

变更先持久化旧 schema 的已接收写入，再原子推进 schema version 和 writer epoch。
历史文件的新列读作 null。旧日志/写入句柄以 `SCHEMA_MISMATCH` 终止，调用方重新获取 schema
并打开句柄；旧 schema 的游标不能静默续读新 schema。固定历史快照仍保留原始 schema。
若前几批已确认、后续批次被明确拒绝，`ServerError.receipts` 和 writer 的 `last_receipts`
保留本次调用已确认的回执；无法确认结果的失败仍为 `AmbiguousWriteError`，不能盲目重写整批。

建表/变更的幂等指纹使用逻辑 schema 内容；IPC 对齐和元数据键顺序不改变请求身份，字段、
元数据内容、目标或预期版本变化会产生冲突。当前 Catalog 目录为 v11，旧布局明确拒绝，
不执行在线目录迁移。真实验收覆盖新增字段、纯元数据变更、冷启动恢复及删除后的原请求重放。

## 原生 frame 写入

frame 保留一个完整业务批次，使用显式 `write_frame()`，不会按 writer 缓冲阈值拆分：

```python
client.create_frame_table(
    "factor_frames", schema, by="dt", namespace_id=namespace_id,
    table_id=frame_table_id, request_id=create_request_id,
)
receipt = client.write_frame(
    "factor_frames", complete_frame, namespace="market", durability="persisted",
)[0]
```

UUID 与 namespace 的创建方式同上节。frame 按追加历史语义查询；每批立即封段，WAL 恢复保留
帧边界。服务公布 `ingest.frame.v1` 和 `ingest_max_rows_per_frame`，超过整帧行数/字节限制会拒绝，
不拆分为多个批次。空帧不写入并返回空回执列表，因此上例要求非空输入。
`Client.open_log()` 支持远端完整 frame 交付及显式持久 ACK；`Client.subscribe_table()`
在同一链路上提供完整 frame callback 和处理后 ACK。主包旧 callback 接口已删除，使用同一个显式 Client。
### 时间过滤与行回调

两包共用 `zippy_client` 的表达式与时间范围解析。日期字符串支持一致的紧凑格式
`YYYYMMDD` 或连字符格式 `YYYY-MM-DD`，月、日、分钟、秒和小数精度分别表示对应的
左闭右开时间范围。时间部分必须带完整日期；时区偏移分钟为 00–59，不接受混合分隔符、
尾随换行或超过九位的小数。无时区的时间按列声明的时区解释，列也未声明时使用 UTC。

`zippy.Row` 和 `zippy_client.Row` 是同一个 Mapping 类型。Flight 行回调及主包 Parquet
行重放中的 timestamp 保留整数，单位由 Arrow schema 的 `s/ms/us/ns` 声明，空值为 None。
表回调继续交付带 schema 的 Arrow Table。需 datetime 时由调用方显式转换，不通过隐式
datetime 往返截断纳秒；Row 本身不附带 schema，消费者需保留源表的单位与时区信息。

原生建 namespace/table、schema 变更与 drop 遇到 `RESOURCE_EXHAUSTED` 且 `retryable=true`
时，SDK 最多发送三次完全相同的管理请求，退避为 50/100 ms，所有尝试共享原总超时预算。
版本冲突、权限等其它错误直接传播；次数或时间耗尽也保留原错误。请求 ID、对象身份、schema
及预期版本不会在自动重试中变化。传输响应不确定时仍应由调用者保存同一请求 ID 显式重试。
