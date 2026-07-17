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
    .tail(1000)
    .collect(stream=True)
)
```

`zp.connect()` 会注册进程内唯一的默认连接。之后所有模块级 API 自动共享该连接，不需要保存
返回值，也不需要传入 `master`。再次连接会关闭并替换旧连接；显式关闭或进程正常退出时，连接、
订阅和进程租约会被统一回收。

旧代码中的 `master=` 参数仍可迁移使用，但不是新客户端的推荐调用方式。

`read_sql()` 只提交给服务端执行，不在客户端解析 SQL。连接的服务端必须显式发布
`query_sql` capability，否则客户端会抛出 `CapabilityError`：

```python
result = zp.read_sql(
    "select instrument_id, max(dt) from ldc_tgw_ticks group by instrument_id"
)
```

Gateway token 可以通过 `connect(token=...)` 或 `ZIPPY_GATEWAY_TOKEN` 提供；显式参数优先。

## Transport 路由

公开 transport 名称为：

- `auto`
- `shm`
- `gateway_local`
- `gateway_remote`

`auto` 按操作选择：

| 操作 | 选择顺序 |
|---|---|
| 实时订阅 | SHM -> Local Gateway -> Remote Gateway |
| 表查询 | Local Gateway -> Remote Gateway |
| SQL | Local Gateway -> Remote Gateway |
| 写入 | Local Gateway -> Remote Gateway |

SHM 仅用于同一 IPC namespace 内的实时只读订阅。Windows 客户端连接 WSL 服务端时，
即使网络地址是 localhost，也不能访问 WSL 的 POSIX SHM，因此会使用 Gateway。

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

## 依赖边界

`zippy-client` 默认依赖 PyArrow 和 Loguru，并包含一个小型 `zippy_client._shm` 原生模块。
它不依赖完整 `zippy` 包。完整 `zippy` 可以依赖 `zippy-client`，依赖方向不能反转。
