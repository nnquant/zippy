# zippy-client Python 连接器

`zippy-client` 是独立发行的轻量 Python 包，源码位于 `clients/python`。它面向本地策略、
Windows 客户端和远程应用，只包含连接、查询、订阅、写入及本地只读 SHM reader，不携带
Master、Gateway、Server、WebUI、Compaction 或计算引擎。

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
| `zp.read_table()` | 构建 lazy table plan | Gateway 执行 |
| `zp.read_sql()` | 提交服务端 SQL | Gateway 查询服务执行 |
| `zp.get_writer()` | 写入行或 Arrow batch | Gateway 执行 |
| `zp.subscribe()` | 逐行实时回调 | SHM 或 Gateway |
| `zp.subscribe_table()` | Arrow table 实时回调 | SHM 或 Gateway |

`read_sql()` 的 API 已固定，但当前 0.5 服务端尚未发布 `query_sql` capability。连接旧服务端时
会立即抛出 `CapabilityError`，客户端不会自行解析 SQL，也不会把全表拉回本地执行。

## Transport 路由

默认 `transport="auto"` 按操作选择：

| 操作 | 自动选择顺序 |
|---|---|
| `subscribe*` | SHM -> Local Gateway -> Remote Gateway |
| `read_table` / `read_sql` | Local Gateway -> Remote Gateway |
| `get_writer` | Local Gateway -> Remote Gateway |

SHM 只用于实时只读订阅。主动查询和写入始终由 Gateway 执行，以避免客户端承担全量扫描、
SQL 解析或存储一致性逻辑。

可用的显式值为 `auto`、`shm`、`gateway_local` 和 `gateway_remote`：

```python
zp.connect(
    "zippy://127.0.0.1:17690/default",
    transport="gateway_local",
)
```

旧代码里的 `connect(local=False)` 仍可用，它会选择服务端实际发现的 Gateway scope，并禁止
后续订阅自动切换到 SHM。

## Windows 连接 WSL

Windows 进程和 WSL 进程不属于同一个 POSIX IPC namespace。即使地址都是 `localhost`，
客户端也不会尝试读取 WSL 内的 SHM，而会通过 Gateway：

```python
zp.connect("zippy://127.0.0.1:17690/default")
decision = zp.client().transport_for("subscribe")
print(decision.selected)
```

只有 Unix control socket 能证明客户端与服务端处于可访问同一 segment 的本地 namespace，
因此 SHM 自动选择不会依赖主机名猜测。

## 查询语义

`read_table()` 返回 lazy `Table`。`filter`、`select`、`head`、`tail`、`slice`、`sort`、
`drop` 和 `rename` 会序列化为 typed plan，在 Gateway 侧执行。当前服务端不能精确执行的操作
会抛出 `CapabilityError`，不会静默退化为客户端全量计算。

```python
query = zp.read_table("ldc_tgw_ticks", snapshot=True)
first = query.tail(100).collect()
second = query.tail(100).collect()
```

`snapshot=True` 会让同一查询链复用一个服务端 snapshot。查询 clone 共享引用计数 lease，父查询
关闭不会提前释放仍被子查询使用的 snapshot。`snapshot=False` 的每次终结操作读取最新边界。

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

Gateway token 的优先级为：显式 `token=`、`ZIPPY_GATEWAY_TOKEN`、服务端广告配置。

```python
import os

zp.connect(server_uri, token=os.environ["ZIPPY_GATEWAY_TOKEN"])
```

客户端只支持 Gateway frame protocol v1，并在连接阶段拒绝未知版本。JSON header 默认限制为
1 MiB，Arrow IPC payload 默认限制为 512 MiB；可通过以下环境变量收紧：

```text
ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES
ZIPPY_CLIENT_MAX_FRAME_PAYLOAD_BYTES
```

这些限制是单 frame budget，不是整次流式查询的总结果上限。

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
