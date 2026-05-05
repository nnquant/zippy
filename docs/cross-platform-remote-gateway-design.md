# Zippy Cross-platform Remote Gateway 设计

> 日期：2026-05-05
>
> 目标：让 Windows 侧行情源和策略/交易程序可以写入、订阅和查询 WSL/Linux 中的 Zippy
> named stream，同时保留 Linux 本地 segment/mmap 快路径。

## 1. 背景

当前 Zippy 在 Linux/WSL 上的高性能路径依赖：

```text
master unix socket 控制面
+ local mmap segment 数据面
+ 同一 OS / 同一命名空间内的 reader attach
```

这个模型不适合 Windows 原生进程直接访问 WSL 里的 segment。典型问题是：

- QMT 等行情源只能运行在 Windows 侧；
- 某些策略或交易程序只能运行在 Windows 侧，但只需要消费 WSL 中的行情数据；
- Windows 进程不能直接 attach WSL 的 Linux mmap segment；
- 不能要求每个 Windows writer 自己管理一个 gateway 或一个独立端口。

因此需要新增一层跨平台 remote data plane。它不替代 Linux 本地 segment path，而是在
Windows/WSL 边界上做协议转换和多路复用。

## 2. 目标与非目标

### 2.1 目标

1. Windows 侧可以持续逐行或批量写入 WSL 中的 named stream。
2. Windows 侧可以订阅 WSL 中的 named stream，接收 row callback 或 table callback。
3. Windows 侧可以 `read_table()` 构建 lazy plan，并在 `collect()` 时把 plan 发送到 WSL
   执行，返回 Arrow IPC 二进制。
4. Gateway 不是一写入器一实例，而是 master 管理的单实例或少量实例，多 writer/subscriber
   按 stream name 路由。
5. 用户侧 API 尽量统一：`connect()`、`get_writer()`、`subscribe()`、`read_table()` 自动选择
   local segment path 或 remote gateway path。

### 2.2 非目标

- 不让 Windows 直接 attach WSL mmap segment。
- 不把行情热路径塞进 master 进程的控制面 request loop。
- 不在第一版追求 remote gateway 延迟等同 Linux 本地 mmap。
- 不在本设计中解决交易指令、委托回报、成交回报等交易数据链路。
- 不在第一版支持多个 writer 同时无序写同一个 append-only stream，除非 stream 显式配置
  multi-writer 合并策略。

## 3. 总体架构

推荐架构：

```text
Windows QMT writer  ┐
Windows subscriber  ├── TCP + Arrow IPC ──> GatewayServer ──> local StreamTable / subscribe / query
Windows query       ┘                         ▲
                                              │
                                   master catalog / capability / lifecycle
```

职责边界：

| 组件 | 职责 |
| --- | --- |
| `Master` | 控制面、stream catalog、schema、capability、gateway endpoint 发布、gateway 生命周期管理 |
| `GatewayServer` | 跨平台数据面，处理 remote write、remote subscribe、remote collect |
| `StreamTable` | Linux/WSL 本地 named stream 物化层，继续拥有 active/sealed/persist |
| `Windows client` | 远端轻量客户端，负责 row/batch buffering、RPC framing、callback 分发 |

关键原则：

```text
master 管 Gateway；
Gateway 走数据热路径；
segment 继续作为 WSL/Linux 本地高速数据面；
Windows 侧不感知 mmap/descriptor。
```

## 4. Gateway 生命周期与部署模型

Gateway 不应与 writer 一一对应。默认模型是：

```text
一个 master profile -> 一个 remote gateway endpoint
多个 remote writer/subscriber/query client -> 同一个 gateway
```

实现上可以有两种物理形态：

1. master 内置线程；
2. master 托管的 sidecar 进程。

推荐第一版采用“master 托管 sidecar 进程”的语义：

- 用户只启动 master，或由 `zippy.connect()`/CLI 启动 master；
- master 负责启动、停止、健康检查和发布 gateway endpoint；
- gateway 数据面与 master 控制面隔离，避免大流量行情或大查询拖慢 heartbeat、descriptor
  和 metadata 请求；
- Python API 对用户表现为“master 自带 remote capability”。

master config/capability 中需要暴露：

```toml
[remote_gateway]
enabled = true
endpoint = "127.0.0.1:17666"
token = "..."
protocol_version = 1
```

## 5. URI 与连接判定

`connect(uri=...)` 负责表达连接位置，但最终数据路径由 master capability 决定。

建议解析规则：

```text
zp.connect()
zp.connect("default")
zp.connect("zippy://default")
  -> local profile，优先连接本机 master

zp.connect("/tmp/zippy/master.sock")
zp.connect("unix:///tmp/zippy/master.sock")
  -> 显式 local unix socket

zp.connect("zippy://wsl-host:17665/default")
  -> remote master control endpoint
```

核心规则：

```text
没有 host = local profile
有 host = remote profile
显式 unix path = local
显式 tcp/ws/http endpoint = remote
```

客户端连接 master 后，再读取 capability：

```text
can_attach_local_segment = true/false
remote_gateway.endpoint = ...
```

后续 `get_writer()`、`subscribe()`、`read_table().collect()` 根据 capability 自动选择：

```text
local attach 可用 -> local segment path
local attach 不可用且 gateway 可用 -> remote gateway path
否则 -> 明确错误
```

## 6. Remote Writer API

用户侧入口：

```python
writer = zp.get_writer("qmt_ticks")

writer.write({
    "instrument_id": "IF2606",
    "dt": dt,
    "last_price": 4102.5,
})
```

`get_writer()` 行为：

1. 向 master 查询 stream metadata 和 schema；
2. 判断当前进程是否可以 local attach segment；
3. local 可用时返回本地 writer；
4. remote 可用时返回 `RemoteGatewayWriter`；
5. stream 不存在时默认报错，只有显式 `create=True` 且提供 schema 时才创建。

建议接口：

```python
writer = zp.get_writer(
    "qmt_ticks",
    create=False,
    schema=None,
    batch_size=1024,
    flush_interval_ms=5,
)
```

逐行写入应支持，但底层必须批量化：

```text
write(row)
  -> client-side buffer
  -> batch_size 或 flush_interval_ms 触发
  -> Arrow IPC batch
  -> GatewayServer
  -> WSL local StreamTable writer
```

用户也可以直接写 batch：

```python
writer.write(pyarrow_table_or_record_batch)
writer.flush()
writer.close()
```

第一版写入语义：

- 默认 single writer per stream；
- 同一个 remote client 可以写多个 stream；
- 同一个 gateway 可以服务多个 stream；
- 如果需要多 writer 合并，必须后续显式设计 merge key、sequence 和冲突策略。

## 7. Remote Subscribe API

用户侧入口保持一致：

```python
zp.subscribe("ctp_ticks_latest", callback=on_tick)

zp.subscribe_table(
    "ctp_ticks",
    callback=on_table,
    filter=zp.col("instrument_id").is_in(["IF2606", "IH2606"]),
    batch_size=1000,
    throttle_ms=20,
    count=1000,
)
```

remote 模式下：

```text
Windows client subscribe request
  -> Gateway
  -> GatewayServer 在 WSL 本地 subscribe / subscribe_table
  -> Gateway 按 filter/batch/count 处理或下推
  -> Arrow IPC / row frames
  -> Windows callback
```

推荐默认：

- `subscribe()` 返回 `zippy.Row`；
- `subscribe_table()` 返回 `pyarrow.Table`；
- 高吞吐场景推荐 table callback；
- row callback 只适合低频或已经过滤后的行情。

## 8. Remote Query / read_table 语义

远程 `read_table()` 不应读取数据，只拿轻量 metadata：

```text
schema
stream info summary
capabilities
snapshot token 可选
```

用户构建的表达式计划留在客户端：

```python
df = (
    zp.read_table("ctp_ticks", snapshot=True)
    .filter(zp.col("instrument_id") == "IF2606")
    .select("instrument_id", "dt", "last_price")
)
```

只有 terminal operation 触发数据面：

```python
table = df.collect()
```

remote `collect()` 流程：

```text
Windows client
  -> serialize TablePlan
  -> send collect request to GatewayServer

Gateway
  -> resolve snapshot boundary
  -> execute plan against local Table / persisted parquet / live segment
  -> predicate/projection pushdown
  -> Arrow IPC stream bytes

Windows client
  -> deserialize Arrow IPC
  -> pyarrow.Table
```

snapshot 语义：

```text
snapshot=True
  read_table 或首次 collect 获取固定 snapshot token；
  后续 collect 使用同一边界。

snapshot=False
  每次 collect 都在 Gateway 侧解析最新 snapshot。
```

## 9. Remote Protocol

第一版协议建议基于 TCP framed messages，不引入复杂 broker。

控制消息可以是 length-prefixed JSON 或 MessagePack：

```text
hello
open_writer
write_batch
flush
subscribe
collect
close
error
```

数据 payload 优先使用 Arrow IPC stream：

```text
frame header:
  protocol_version
  request_id
  stream_name
  message_kind
  payload_encoding = arrow_ipc_stream
  payload_len

payload:
  Arrow IPC bytes
```

这样可以复用在：

- remote writer batch；
- remote subscribe_table batch；
- remote query collect result；
- replay 后续跨平台消费。

## 10. Backpressure 与错误处理

remote writer 必须支持 backpressure：

```text
client buffer full -> write(row) 阻塞或按策略丢弃
gateway ingest queue full -> 返回 retryable error 或背压 ack
schema mismatch -> 启动/首次写入时报错
stream stale/drop -> writer/subscriber 收到明确错误
```

第一版推荐：

- 默认阻塞式 backpressure；
- 可配置 `max_write_rows`，拒绝异常大的单次 `write_batch`；
- 行情源场景后续可增加 `drop_oldest` / `drop_newest` 策略，但不作为默认；
- 所有错误包含 `stream_name`、`endpoint`、`request_id`、`schema_hash`。

## 11. 安全边界

默认只面向内网或本机 Windows/WSL 场景。

第一版建议：

- gateway 默认绑定 loopback 或显式配置的 host；
- 远程访问必须配置 token；
- token 由 master 下发或配置文件提供；
- 日志中不要打印 token；
- 不把 gateway 默认暴露到公网地址。

## 12. 与 Windows 原生 Runtime 的关系

本文设计的是：

```text
Windows client -> WSL/Linux zippy core
```

`docs/windows-availability-design.md` 设计的是：

```text
Windows 原生 master + Windows 原生 segment backend
```

两者不是互斥关系：

- Remote Gateway 解决近期实际接入问题；
- Windows 原生 Runtime 解决长期跨平台本地运行能力；
- Python API 应保持一致，由 capability 自动选择 local 或 remote path。

## 13. 实施阶段

### M10.1 Gateway control capability

- master metadata 暴露 gateway capability；
- `connect()` 区分 local/remote profile；
- Windows client 可连接 remote master 并发现 gateway endpoint。

### M10.2 Remote writer

- 实现 `zp.get_writer()`；
- local 环境返回 local writer；
- remote 环境返回 `RemoteGatewayWriter`；
- 支持 row write、batch write、flush、close；
- 支持 client-side buffering。

### M10.3 Remote subscribe

- remote `subscribe()` 支持 row callback；
- remote `subscribe_table()` 支持 Arrow table callback；
- 支持 `filter`、`batch_size`、`throttle_ms`、`count`。

### M10.4 Remote lazy query

- remote `read_table()` 只拉 schema/metadata；
- `collect()` 发送 TablePlan 到 Gateway；
- GatewayServer 执行计划并返回 Arrow IPC；
- 支持 snapshot=True/False 语义。

### M10.5 Reliability and ops

- gateway metrics：已提供 `GatewayServer.metrics()` 和远端 `metrics` request；
- client reconnect：`RemoteStreamSubscriber` 已支持 Gateway 暂不可用时按间隔重连；
- backpressure：当前 request/response 同步阻塞，且 `max_write_rows` 可限制单批写入；
- token：当前 `remote_gateway.token` 可由 master config 下发，GatewayServer 校验请求 token；
- CLI：已提供 `zippy gateway run`，用于启动统一 GatewayServer 服务；
- Smoke：已提供 `zippy gateway smoke`，用独立子进程模拟远端客户端写入和查询；
- Windows client smoke：已提供 `zippy gateway smoke-client --uri zippy://host:port/default`，
  用于在 Windows 侧连接已有 WSL/Linux master/Gateway 验收；
- e2e examples：已补 `examples/08_remote_gateway/`；
- Windows QMT mock writer + WSL subscriber/query 验收：执行步骤见
  [remote_gateway_windows_smoke_runbook.md](remote_gateway_windows_smoke_runbook.md)。

当前 Linux/WSL 侧已经具备本地跨进程 smoke；真正完成 M10 前，还需要在 Windows
原生 Python 环境执行 `zippy gateway smoke-client --uri zippy://<wsl-host>:17690/default`
并记录结果。Linux 侧 smoke 不能替代这个验收，因为它不覆盖 Windows 网络栈、Windows
Python 包安装和 Windows client 到 WSL/Linux 的可达性。

## 14. 验收场景

### 场景一：Windows QMT 写入 WSL

```python
zp.connect("zippy://wsl-host:17690/default")
writer = zp.get_writer("qmt_ticks")

def on_qmt_tick(tick):
    writer.write(normalize_qmt_tick(tick))
```

WSL 侧：

```python
zp.read_table("qmt_ticks").tail(1000)
```

要求：Windows 逐行写入，WSL 可持续查询。

### 场景二：Windows 策略消费 WSL 行情

```python
zp.connect("zippy://wsl-host:17690/default")
zp.subscribe("ctp_ticks_latest", callback=on_tick)
```

要求：Windows callback 能持续收到 WSL 中的行情流。

### 场景三：Windows 远程查询

```python
table = (
    zp.read_table("ctp_ticks")
    .filter(zp.col("instrument_id") == "IF2606")
    .select("instrument_id", "dt", "last_price")
    .collect()
)
```

要求：`read_table()` 不拉全量数据；`collect()` 在 Gateway 侧执行计划并返回 Arrow。
