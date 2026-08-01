# zippy-client

`zippy-client` 是 Zippy 的轻量 Python 客户端发行包。它只包含连接、查询、订阅、写入和
本地只读 SHM 能力，不包含 Server、Master、Gateway daemon、WebUI、Compaction 或计算引擎。

## 安装

```bash
uv add zippy-client
```

## 兼容 API

新包保留原有的模块级调用形态：

```python
import zippy_client as zp

zp.connect("zippy://127.0.0.1:17690/default")

ticks = (
    zp.read_table("ldc_tgw_ticks", snapshot=False)
    .filter(zp.col("instrument_id") == "110059.SH")
    .select("instrument_id", "dt", "last_price")
    .sort("dt")
    .tail(1000)
    .collect(stream=True)
)
```

`zp.connect()` 会注册进程内唯一的默认连接。之后所有模块级 API 自动共享该连接，不需要保存
返回值，也不需要传入 `master`。再次连接会关闭并替换旧连接；显式关闭或进程正常退出时，连接、
订阅和进程租约会被统一回收。

旧代码中的 `master=` 参数仍可迁移使用，但不是新客户端的推荐调用方式。

`read_sql()` 只提交给服务端执行，不在客户端解析 SQL。Flight version 1 要求显式声明 SQL relation
与 Catalog table 的绑定，服务端不会从 SQL 文本猜表：

```python
result = zp.read_sql(
    "select instrument_id, max(dt) from ticks group by instrument_id",
    bindings={"ticks": "ldc_tgw_ticks"},
)
```

连接旧 Gateway 时，原来的无 `bindings` 调用仍走 compatibility adapter。Flight 查询 bootstrap token
使用 `connect(flight_token=...)` 或 `ZIPPY_FLIGHT_AUTH_TOKEN`；Gateway token 仍使用
`connect(token=...)` 或 `ZIPPY_GATEWAY_TOKEN`。

## Transport 路由

公开 transport 名称为：

- `auto`
- `shm`
- `flight_local`
- `flight_remote`
- `gateway_local`
- `gateway_remote`

`auto` 按操作选择：

| 操作 | 选择顺序 |
|---|---|
| 实时订阅 | SHM -> Local Gateway -> Remote Gateway |
| 表查询 | Local Flight -> Remote Flight -> Gateway compatibility |
| SQL | Local Flight -> Remote Flight -> Gateway compatibility |
| 写入 | 支持 ingest 的 Local Flight -> Remote Flight -> Gateway compatibility |

SHM 仅用于同一 IPC namespace 内的实时只读订阅。Windows 客户端连接 WSL 服务端时，即使网络地址
是 localhost，也不能访问 WSL 的 POSIX SHM；查询和写入使用 Flight，实时订阅在
Phase 7 DoExchange 上线前继续使用 Gateway。

```python
subscriber = zp.subscribe(
    "ldc_tgw_ticks",
    on_tick,
    transport="auto",
    instrument_ids=["110059.SH"],
)
```

同一 IPC namespace 内可使用 `xfast=True, poll_interval_ms=0` 进入 Rust 短自旋路径；该模式
以持续占用一个 CPU core 换取最低唤醒延迟。普通模式先短自旋，再进入 mmap futex wait。

客户端不会在无法证明连续性的情况下静默切换实时 transport。Gateway 不支持 resume cursor
时，收到过数据后的断线默认抛出 `ContinuityError`。只有显式设置 `allow_data_loss=True` 才允许
从最新位置重新订阅。

## Flight TLS 和写入

私有 CA、mTLS 和 server name 使用以下参数或对应环境变量：

```python
zp.connect(
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

`get_writer()` 默认请求 `wal` durability，自动按服务端上限切分 Arrow batch。同一个 writer 会跨
`flush()` 复用 producer、connection、epoch 和开放的 DoPut stream，在 batch/row/byte 配额或 RPC
deadline 前有界轮换，并在每次 `flush()` 返回前逐批校验 request ID、连续 log offset、水位和 WAL
ACK。需要最低延迟且明确接受进程
崩溃丢失未提交尾部时，才显式选择 `memory`：

```python
with zp.get_writer(
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
按稳定 offset 或业务主键核对后创建新 writer。`Persisted` durability 尚未开放。

Flight 每次 terminal query 都固定一个服务端 snapshot；同一次 `collect()` 内结果一致。旧 Gateway
`snapshot=True` 的跨多次 terminal 调用 lease 复用不属于 Flight version 1。`tail()` 必须先显式
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

构建时必须把 Cargo 产物放到仓库外；客户端 SHM crate 会间接编译 Arrow/Parquet，复用根目录
`target/` 会快速占用数 GiB：

```bash
uv run --package zippy-client --extra dev maturin build --release \
  --target-dir /tmp/zippy-client-target \
  --out /tmp/zippy-client-dist
```

发布完成后删除两个 `/tmp/zippy-client-*` 目录。面向用户应发布各平台的预编译 wheel，不能要求
Windows 或远程客户端现场编译 Rust workspace。

## 依赖边界

`zippy-client` 默认依赖 PyArrow、protobuf 和 Loguru，并包含一个小型 `zippy_client._shm` 原生模块。
它不依赖完整 `zippy` 包。完整 `zippy` 可以依赖 `zippy-client`，依赖方向不能反转。
