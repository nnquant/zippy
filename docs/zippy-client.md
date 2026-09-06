# zippy-client Python 连接器

`zippy-client` 是独立发行的轻量 Python 包，源码位于 `clients/python`。它面向本地策略、
Windows 客户端和远程应用，只包含连接、查询、订阅、写入及本地只读 SHM reader，不携带
Master、Gateway、Server、WebUI、Compaction 或计算引擎。

KeyValue 订阅已切换为保留控制字段的 v2 增量，不再返回每批完整状态。生产者、消费者须
同步升级；初始化和下游修改见 [KeyValue v2 迁移指南](key-value-v2-migration.md)。

## 安装和导入

```bash
uv add zippy-client
```

```python
import zippy_client as zp
```

发行包名使用连字符 `zippy-client`，Python 模块名使用下划线 `zippy_client`。它不依赖完整
`zippy` 包。

## 旧 API 兼容

模块级入口继续保留：

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

`zp.connect()` 将连接注册为进程内唯一的默认 `Client`。后续 `read_table()`、`read_sql()`、
`get_writer()` 和 `subscribe*()` 自动共享它，用户不需要保存连接对象，也不需要传入 `master`。

再次调用 `zp.connect()` 会串行创建新连接并关闭旧连接。调用 `Client.close()` 会同步清空默认
连接；进程正常退出时，客户端还会通过退出钩子停止订阅、释放控制面进程租约并关闭连接。
`master=` 只为迁移旧代码保留，不是推荐用法。

以下模块级入口均可使用进程默认连接：

| API | 用途 | 执行位置 |
|---|---|---|
| `zp.connect()` | 建立并注册进程唯一默认连接 | Client + Master |
| `zp.read_table()` | 构建 lazy table plan | Flight；旧服务回退 Gateway |
| `zp.read_sql()` | 提交服务端 SQL | Flight；旧服务回退 Gateway |
| `zp.get_writer()` | 写入行或 Arrow batch | Flight；旧服务回退 Gateway |
| `zp.subscribe()` | 逐行实时回调 | SHM 或 Gateway |
| `zp.subscribe_table()` | Arrow table 实时回调 | SHM 或 Gateway |

Flight `read_sql()` 要求显式 Catalog binding，例如
`bindings={"ticks": "ldc_tgw_ticks"}`。这不是额外的客户端 SQL 解析负担，而是明确限定本次查询可见的
表；Server 不会从 SQL 文本猜测 Catalog authority。连接旧 Gateway 时，无 `bindings` 的原调用仍通过
compatibility adapter 执行。两条路径都不会把全表拉回客户端做 residual。

## Transport 路由

默认 `transport="auto"` 按操作选择：

| 操作 | 自动选择顺序 |
|---|---|
| `subscribe*` | SHM -> Local Gateway -> Remote Gateway |
| `read_table` / `read_sql` | Local Flight -> Remote Flight -> Gateway compatibility |
| `get_writer` | 支持 ingest 的 Local Flight -> Remote Flight -> Gateway compatibility |

SHM 只用于实时只读订阅。主动查询和写入优先由标准 Arrow Flight 执行；Gateway 保留为旧服务与
Phase 7 之前远端订阅的 compatibility adapter。客户端不承担全量扫描、SQL 解析或存储一致性逻辑。

可用的显式值为 `auto`、`shm`、`flight_local`、`flight_remote`、`gateway_local` 和
`gateway_remote`：

```python
zp.connect(
    "zippy://127.0.0.1:17690/default",
    transport="flight_local",
)
```

旧代码里的 `connect(local=False)` 仍可用；它只禁止后续订阅自动切换到 SHM，不会阻止查询和写入
选择 Flight。

## Windows 连接 WSL

Windows 进程和 WSL 进程不属于同一个 POSIX IPC namespace。即使地址都是 `localhost`，
客户端也不会尝试读取 WSL 内的 SHM。查询和写入通过 Flight，实时订阅在 DoExchange 上线前通过
Gateway：

```python
zp.connect("zippy://127.0.0.1:17690/default")
decision = zp.client().transport_for("subscribe")
print(decision.selected)
```

只有 Unix control socket 能证明客户端与服务端处于可访问同一 segment 的本地 namespace，
因此 SHM 自动选择不会依赖主机名猜测。

## 查询语义

`read_table()` 返回 lazy `Table`。`filter`、`select`、`with_columns`、`head`、`tail`、`slice`、
`sort`、`drop`、`rename` 和 `join` 会编译为带明确 binding 的完整 SQL，在 Server DataFusion
执行。当前服务端不能精确执行的操作会抛出 `CapabilityError`，不会静默退化为客户端全量计算。

```python
query = zp.read_table("ldc_tgw_ticks").sort("dt")
latest = query.tail(100).collect()
```

Flight 每次 terminal query 都固定一个 snapshot，同一次 `collect()` 内不会混读不同版本。Flight version 1
ticket 是单次消费，因此旧 Gateway `snapshot=True` 的跨多次 terminal 调用 lease 复用不适用于 Flight。
`tail()` 必须继承显式 `sort()`，客户端会拒绝没有确定顺序的“最后 N 行”。

## 实时订阅语义

CTA 等依赖逐事件处理的策略使用 `subscribe()`：

```python
subscriber = zp.subscribe(
    "ldc_tgw_ticks",
    on_tick,
    instrument_ids=["110059.SH"],
)
```

批量处理使用 `subscribe_table()`，避免每行创建 Python 对象：

```python
subscriber = zp.subscribe_table("ldc_tgw_ticks", on_table)
```

同机极低延迟场景可以显式启用 `xfast=True`。该模式在 Rust reader 中释放 GIL 后持续短自旋，
会占用一个 CPU core；普通模式先执行 `idle_spin_checks` 次短自旋，再进入 mmap futex wait：

```python
subscriber = zp.subscribe(
    "ldc_tgw_ticks",
    on_tick,
    xfast=True,
    poll_interval_ms=0,
    idle_spin_checks=64,
)
```

本地 SHM subscriber 读取 retained sealed descriptors 后再切换 active descriptor，连续 rollover
时不会直接跳到最新段。Gateway 尚未支持 resume cursor 时，收到过数据后的断线默认抛出
`ContinuityError`；只有显式设置 `allow_data_loss=True` 才允许从最新位置重连。

## 认证和协议边界

Flight read/writer 使用不同 bootstrap credential。显式参数优先于环境变量：

```python
import os

zp.connect(
    server_uri,
    flight_token=os.environ["ZIPPY_FLIGHT_AUTH_TOKEN"],
    flight_writer_token=os.environ.get("ZIPPY_FLIGHT_AUTH_WRITER_TOKEN"),
    flight_tls_root_cert_file="zippy-ca.pem",
)
```

TLS/mTLS 对应配置是：

```text
ZIPPY_FLIGHT_AUTH_TOKEN
ZIPPY_FLIGHT_AUTH_WRITER_TOKEN
ZIPPY_FLIGHT_TLS_ROOT_CERT_FILE
ZIPPY_FLIGHT_TLS_CLIENT_CERT_FILE
ZIPPY_FLIGHT_TLS_CLIENT_KEY_FILE
ZIPPY_FLIGHT_TLS_SERVER_NAME
```

客户端先通过 `DoAction("zippy.capabilities.v1")` 校验协议、结构化错误和服务端硬上限。认证模式下，
read/writer 分别 Handshake，后续 RPC 只使用服务端签发的短期 bearer；bootstrap token 不会进入查询
descriptor。业务错误解析 versioned `ErrorEnvelope`，公开为带 `code`、`retryable`、ID 和 details 的
`ServerError`，不会解析英文 message 决定重试。

`get_writer()` 的 Stage 6.5 durability 是 `Append + Memory`。逐 batch receipt 只能证明数据已进入当前
Server 内存 materializer，不代表 WAL/persisted durability，也不提供 producer fencing 或去重。断线后
不能盲目重试不确定批次；writer 会永久 fail-closed，并在 `AmbiguousWriteError.receipts` 中保留已明确
收到的回执。需要 exactly-once 或重启恢复时等待 Phase 7 durable ingest。

Gateway token 的优先级仍是显式 `token=`、`ZIPPY_GATEWAY_TOKEN`、服务端广告配置。compatibility
frame v1 的本地预算仍可通过以下变量收紧：

```text
ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES
ZIPPY_CLIENT_MAX_FRAME_PAYLOAD_BYTES
```

这些限制只适用于 Gateway 单 frame，不是 Flight 整次查询的总结果上限。

## 仓库边界

```text
clients/python/                 # 独立 Python 发行项目
  pyproject.toml
  src/zippy_client/             # 纯 Python connector 和 _shm 类型声明
  tests/                        # 协议、路由和真实服务集成测试
crates/zippy-client-py/         # 最小 PyO3 SHM reader
```

依赖方向必须保持为完整 `zippy` 可以复用 `zippy-client`，`zippy-client` 不反向依赖完整
`zippy`。这样 Windows 或远程用户不需要下载和构建 Server 端的全部 Rust/Python 组件。

维护者构建 wheel 时必须显式使用仓库外的 Cargo target，并向用户发布预编译产物：

```bash
uv run --package zippy-client --extra dev maturin build --release \
  --target-dir /tmp/zippy-client-target \
  --out /tmp/zippy-client-dist
```

不要让 `maturin` 复用仓库根目录 `target/`；最小 SHM 扩展仍会间接编译 Arrow/Parquet，临时产物可达
数 GiB，发布完成后应删除上述临时目录。
