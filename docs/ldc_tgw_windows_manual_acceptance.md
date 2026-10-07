# LDC TGW Windows 客户端手动验收

> 2026-09-22：本文涉及的 Gateway CLI 与 `08_remote_gateway` 脚本已删除。当前操作见
> [原生远端客户端](native-remote-client.md)，以下仅保留历史验收记录。

> 历史验收记录：以下环境与命令描述 2026-07-16 的旧版本，不再作为当前安装指南。
> 2026-09-21 已删除主包纯 Python 降级；当前远端入口使用独立 `zippy-client`，参见
> [客户端指南](../clients/python/README.md) 和 [新契约](adr/0007-single-trunk-new-contract.md)。

> 验收日期：2026-07-16
>
> 服务端：WSL `local-market-data-center`
>
> 客户端：Windows 原生 Python
>
> 验收对象：`ldc_tgw_ticks` 只读查询与订阅

## 1. 目标与安全边界

本文用于从 Windows 原生进程验收 WSL 中已经运行的 Zippy/LDC，而不是在 Windows 中
启动另一套 Zippy。实际链路如下：

```text
Windows Python
  -> Master tcp://127.0.0.1:17690
  -> 自动发现 Gateway 127.0.0.1:17691
  -> WSL ldc_tgw_ticks
  -> persisted Parquet + sealed segment + active segment
```

验收脚本是只读的。它不会创建或删除 stream，不会写行情，不会修改 LDC 配置，也不会
启停生产进程。当前 WSL localhost forwarding 已验证可用，所以客户端 URI 固定为：

```text
zippy://127.0.0.1:17690/default
```

不要把 Windows 客户端改成读取 WSL 的 mmap/segment 文件。跨操作系统访问必须经过
Master/Gateway；脚本用 `local=False` 显式固定这条路径。

## 2. 本次覆盖的用户能力

| 能力 | 用户接口 | 通过条件 |
|---|---|---|
| 远端连接与服务发现 | `zp.connect(..., local=False)` | Master 可达并自动发现 Gateway |
| 落盘与实时统一查询 | `zp.read_table()` | 同一个查询返回非空结果 |
| 服务端查询下推 | `filter/select/tail/profile` | executor 为 `remote`，tail 和过滤下推生效 |
| 有界流式读取 | `collect(stream=True)` | 分批传输后得到非空 Arrow Table |
| 固定查询快照 | `snapshot=True` | 同一 Table 对象重复 collect 结果相同 |
| 最新查询快照 | `snapshot=False` | 行情持续时重复 collect 能看到推进 |
| 股票/批处理消费 | `subscribe_table()` | Windows 收到过滤后的 Arrow Table |
| CTA 逐事件消费 | `subscribe()` | Windows 收到指定证券的一行回调 |
| writer 重启连续性 | stream metadata | writer epoch 与活动 descriptor 一致 |
| 落盘文件身份唯一性 | `persist_file_id` | 所有已登记文件 ID 非空且不重复 |

这份验收不覆盖 Windows 行情写入。当前测试目标是读取实盘 TGW，可避免为了验收而污染
生产数据。

### 2.1 当前 LDC 的服务端运行形态

LDC 使用一个 `zippy-rspm` daemon 管理任务。RSPM 监督的核心任务只有一个
`zippy server run --foreground`；该 server 进程内部按顺序启动并检查 Master、Gateway、
Catalog 和持久化能力。Master 与 Gateway 不是两个需要分别维护的后台进程。

在 WSL 中查看运行状态：

```bash
cd /home/jiangda/services/local-market-data-center
.venv/bin/zippy pm status
```

当前通过条件：

- `zippy-server` 为 `healthy`，health 为 `ok`；
- 开盘验证时 `ldc-tgw-md` 为 `online`；
- `ldc-tgw-sync` 完成盘前同步后显示 `stopped` 是正常状态；
- 不应启动任何 CTP/OpenCTP 行情任务。

查看 server 启动事实：

```bash
.venv/bin/zippy pm logs zippy-server --lines 80
```

日志应包含加载的 Zippy 版本、Master endpoint、Gateway endpoint，以及
`event=[server_ready] status=[ready] mode=[catalog_and_query]`。当前部署由外部 RSPM 监督
foreground server，所以运维状态以 `zippy pm status` 为准；`zippy server status` 只用于
Zippy 自己后台化启动的 detached 模式，不能拿它判断这个 RSPM task 是否存活。

当前 `configs/zippy.toml` 未启用 WebUI，因此 WebUI 不属于本轮验收项。WebUI 未运行不会
影响 Master/Gateway 的查询和订阅健康。

## 3. Windows 环境准备

打开 **Windows PowerShell**，不要在 WSL shell 中运行本节命令。

当前机器已验证的环境是 Windows Python `D:\miniconda3\python.exe`、Python 3.12.7，
并且已经安装 `pyarrow`。Windows 暂未安装原生 Zippy wheel，因此使用仓库中的纯 Python
远端客户端：

```powershell
$ZippyRoot = "\\wsl.localhost\Ubuntu-20.04\home\jiangda\develop\zippy"
$env:PYTHONPATH = "$ZippyRoot\python"
$env:ZIPPY_FORCE_PURE_PYTHON = "1"

python -c "import sys, zippy; print(sys.executable); print(zippy.__file__)"
```

通过条件：

- 第一行是 Windows Python 路径，例如 `D:\miniconda3\python.exe`；
- 第二行位于 `\\wsl.localhost\Ubuntu-20.04\...\zippy\python\zippy`；
- 不能显示 `/usr/bin/python` 或 WSL `.venv/bin/python`。

以后安装 Windows 原生 Zippy wheel 后，应删除 `PYTHONPATH` 和
`ZIPPY_FORCE_PURE_PYTHON`，其余验收命令保持不变。

## 4. 第一步：检查网络边界

```powershell
Test-NetConnection 127.0.0.1 -Port 17690
Test-NetConnection 127.0.0.1 -Port 17691
```

两个命令的 `TcpTestSucceeded` 都必须是 `True`：

- `17690` 是 Master 控制面，用于 stream 发现、状态和 Gateway capability；
- `17691` 是 Gateway 数据面，用于查询结果和订阅数据传输。

如果任一端口失败，先看第 10 节，不要直接修改生产监听地址。

## 5. 第二步：运行一键只读验收

验收脚本：[`07_ldc_tgw_read_acceptance.py`](../examples/08_remote_gateway/07_ldc_tgw_read_acceptance.py)

保持第 3 节的 PowerShell 会话，执行：

```powershell
python "$ZippyRoot\examples\08_remote_gateway\07_ldc_tgw_read_acceptance.py" `
  --uri zippy://127.0.0.1:17690/default `
  --stream ldc_tgw_ticks
```

脚本成功时退出码为 `0`，并输出一份 JSON。核心判据如下，具体证券代码、时间、行数和
耗时会随行情变化：

```json
{
  "status": "ok",
  "stream": "ldc_tgw_ticks",
  "stream_status": "writer_attached",
  "writer_epoch_consistent": true,
  "persisted_file_ids_unique": true,
  "query": {
    "query_rows": 1000,
    "tail_pushdown": "tail",
    "filter_pushdown_count": 1,
    "stream_rows": 1000,
    "fixed_snapshot_equal": true,
    "fresh_snapshot_advanced": true
  },
  "subscriptions": {
    "table": {
      "status": "ok",
      "callback_rows": 20
    },
    "row": {
      "status": "ok"
    }
  }
}
```

逐项判断：

- 顶层 `status` 必须为 `ok`；
- 开盘且 TGW 在线时，`stream_status` 应为 `writer_attached`；
- `writer_epoch` 和 `descriptor_writer_epoch` 应相等；
- `persisted_files` 至少为 `1`，文件 ID 必须唯一；
- `query_rows` 和 `stream_rows` 必须大于 `0`；
- `tail_pushdown` 必须为 `tail`，`filter_pushdown_count` 至少为 `1`；
- `fixed_snapshot_equal` 必须为 `true`；
- 行情持续更新时，`fresh_snapshot_advanced` 通常为 `true`；短暂无行情时为 `false` 不代表故障；
- table/row 两个订阅状态都应为 `ok`。

收盘后或 TGW writer 未启动时，可以只验收查询与落盘数据：

```powershell
python "$ZippyRoot\examples\08_remote_gateway\07_ldc_tgw_read_acceptance.py" `
  --uri zippy://127.0.0.1:17690/default `
  --stream ldc_tgw_ticks `
  --skip-live
```

此时 `subscriptions.status` 为 `skipped` 是预期结果。

## 6. 如何理解每一项能力

### 6.1 统一查询与服务端下推

用户仍然使用普通的 lazy Table API：

```python
profile = (
    zp.read_table("ldc_tgw_ticks", snapshot=False)
    .filter(zp.col("instrument_id") == "118066.SH")
    .select("instrument_id", "dt", "last_price")
    .tail(20)
    .profile()
)
```

Windows 只发送查询计划，过滤、列裁剪和 `tail` 在 WSL Gateway 侧执行。客户端不会先把
全表拉回 Windows 再过滤。`profile()["metrics"]` 用来证明执行位置与下推结果，而不是靠
耗时猜测。

`read_table()` 会在一个逻辑视图中拼接已落盘文件和当前活动 segment。writer rollover
或重启后，用户不需要自己枚举 epoch 或 Parquet 文件。

### 6.2 有界流式 collect

```python
table = (
    zp.read_table("ldc_tgw_ticks", snapshot=False)
    .select("instrument_id", "dt", "last_price")
    .tail(1000)
    .collect(stream=True)
)
```

`stream=True` 表示 Gateway 分批读取并传输结果，服务端只保留有界 in-flight batch，适合
多文件历史查询。最终 API 仍返回完整的 `pyarrow.Table`；它控制服务端扫描过程中的峰值
内存，不代表客户端永远不物化最终结果。结果集本身很大时仍应增加时间、证券过滤或
`head/tail/slice` 限制。

### 6.3 固定快照与最新快照

- `snapshot=True`：Table 首次执行时固定一次视图，同一个 Table 对象重复终结操作看到
  相同数据，适合报表、回测和多步一致性分析；
- `snapshot=False`：每次终结操作重新解析最新视图，适合行情监控和轮询最新状态。

这里的 snapshot 是查询视图语义，不是复制一份完整数据，也不是交易所“截面”的同义词。

### 6.4 CTA 的逐行回调

```python
subscriber = zp.subscribe(
    "ldc_tgw_ticks",
    callback=on_tick,
    instrument_ids="118066.SH",
    wait=True,
    timeout=15,
)
```

`subscribe()` 面向逐事件处理，每个 callback 收到一行，适合依赖逐 tick/逐因子触发的
CTA 策略。

当前远端订阅是 **at-least-once 风格的实时消费**，不是 exactly-once 消息队列：首次连接
会补发当前活动 segment 中已经存在的匹配行，断线重连也可能重放活动段。生产策略应使用
来源序号或 `(instrument_id, dt, 业务序号)` 做幂等和去重，不能把“收到 callback”直接
等同于“全局只处理一次”。

### 6.5 Arrow 表批回调

```python
subscriber = zp.subscribe_table(
    "ldc_tgw_ticks",
    callback=on_table,
    filter=zp.col("instrument_id") == "118066.SH",
    batch_size=64,
    throttle_ms=100,
    count=1000,
    wait=True,
    timeout=15,
)
```

`subscribe_table()` 适合向量化因子、落盘桥接或批量 UI 更新。回调收到 `pyarrow.Table`，
减少逐行 Python 调用开销。

需要注意：`batch_size` 是触发批交付的最小积累阈值，不承诺每批最多只有这么多行；
活动段补发时首批可能更大。用 `filter` 缩小证券范围，用 `count` 限制本次订阅总行数。

Arrow batch 只是传输批次，不代表“某一时刻所有股票都齐全”的完整截面。股票截面策略
不能把一次 `subscribe_table()` callback 直接当作完整截面。

### 6.6 完整截面能力的边界

完整截面应使用 Frame Table / `subscribe_frames()` 的语义：按 `by` 列收齐一个 frame 后
再触发一次回调。目前 `subscribe_frames()` 仍是同机本地数据面的能力，Remote Gateway
尚未开放该协议；`ldc_tgw_ticks` 也只是 append stream，不是 Frame Table。

因此 Windows 上的股票截面策略当前有两种选择：

1. 在 WSL 内运行截面聚合/Frame Table，只把完成后的结果发布给 Windows；
2. Windows 订阅 Arrow batch 后自行按业务时间、水位线和 universe 完整性组帧。

不要用任意网络批次边界替代业务截面边界。

## 7. writer 重启与落盘连续性

这次实现重点解决了 writer 重启后 epoch 推进、旧 catalog checkpoint 复用和落盘文件 ID
冲突。日常只读验收已经检查：

- stream 的 `writer_epoch` 与活动 segment descriptor 一致；
- 每个 persisted 文件都有非空且唯一的 `persist_file_id`；
- 查询不需要调用方感知 epoch，仍能统一读取历史和活动数据。

若要专门验收“重启后仍可读”，必须在收盘后与 WSL 运维人员协调。该操作会短暂停止行情，
不要在交易时段执行。

WSL 侧操作：

```bash
cd /home/jiangda/services/local-market-data-center
.venv/bin/zippy pm stop ldc-tgw-md
.venv/bin/zippy pm restart zippy-server
.venv/bin/zippy pm status
```

确认 `zippy-server` healthy 后，在 Windows 重新运行第 5 节命令并加 `--skip-live`。通过条件：

- 旧 persisted 数据仍可查询；
- `persisted_file_ids_unique` 仍为 `true`；
- 查询不出现 schema、checkpoint 或重复文件 ID 错误。

需要继续采集时，再在 WSL 启动 TGW：

```bash
.venv/bin/zippy pm start ldc-tgw-md
```

## 8. 当前实测基线

2026-07-16 13:26 左右，使用 Windows 原生 Python 跨 WSL 实测：

| 指标 | 实测值 |
|---|---:|
| Master / Gateway | `127.0.0.1:17690` / `127.0.0.1:17691` 可达 |
| stream 状态 | `writer_attached` |
| writer epoch | `3` |
| persisted files | `4` |
| tail 查询 | `1000` 行，约 `33.9 ms` |
| Gateway 执行 | 约 `3.9 ms` |
| 查询覆盖证券 | `302` |
| Arrow 批回调 | 过滤后 `20` 行 |
| CTA 行回调 | 收到指定证券 `1` 行 |

这些数字只用于确认链路量级，不是 SLA。磁盘缓存、行情密度、rollover 和 Windows 调度都会
影响结果。正式性能回归应比较同一查询、同一数据规模的分位数，而不是比较一次 wall time。

## 9. 验收记录模板

```text
验收时间：
Windows Python：
Zippy commit/version：
Master URI：zippy://127.0.0.1:17690/default
Stream：ldc_tgw_ticks

[ ] 17690 TcpTestSucceeded=True
[ ] 17691 TcpTestSucceeded=True
[ ] WSL zippy-server health=ok
[ ] Windows Python 成功导入远端客户端
[ ] 顶层 status=ok
[ ] executor=remote（由脚本内部强制检查）
[ ] tail_pushdown=tail
[ ] filter_pushdown_count>=1
[ ] fixed_snapshot_equal=true
[ ] persisted_file_ids_unique=true
[ ] writer_epoch_consistent=true
[ ] subscribe_table status=ok，且 callback_rows<=20
[ ] subscribe row status=ok
[ ] 未创建/修改/删除任何生产 stream

结论：通过 / 有条件通过 / 不通过
异常与 JSON 附件：
```

## 10. 常见故障

### 10.1 端口不通

如果 `17690` 或 `17691` 为 `False`，依次确认：

1. WSL 发行版 `Ubuntu-20.04` 是否正在运行；
2. `zippy-server` 是否 healthy；
3. Windows 的 WSL localhost forwarding 是否被禁用；
4. Windows 防火墙或安全软件是否阻断本地端口。

当前生产配置监听 `127.0.0.1`，本机 Windows 已验证能通过 localhost forwarding 访问。
不要为了临时验收直接改成 `0.0.0.0`，这会扩大服务暴露面。

### 10.2 导入了错误的 Python/Zippy

重新执行：

```powershell
Get-Command python
python -c "import sys, zippy; print(sys.executable); print(zippy.__file__)"
```

确认解释器是 Windows 路径，Zippy 来自第 3 节设置的 UNC 路径。不要从 Windows 调用 WSL
`.venv/bin/python` 来冒充跨平台验收。

### 10.3 本地 segment 或 mmap 错误

必须使用 `zippy://127.0.0.1:17690/default`，并保持 `local=False`。纯 Python 客户端还需要：

```powershell
$env:ZIPPY_FORCE_PURE_PYTHON = "1"
```

这能防止客户端误选仅适用于 Linux 同机进程的本地 attach 路径。

### 10.4 实时订阅超时

先判断 TGW 是否在线或当前是否有行情。收盘后用 `--skip-live` 验收历史查询；不要为了让
callback 有数据而伪造生产 tick。

### 10.5 首批数据比 batch_size 大

这是活动 segment catch-up 与批交付语义，不一定是 bug。验收脚本使用证券过滤和
`count=20` 把载荷限制在可控范围。业务代码也应设置明确的 filter/count 或自己实施背压。

### 10.6 重连后看到重复行

当前远端订阅允许重放活动 segment。检查 subscriber metrics 中的 `reconnects_total`，并在
策略侧按稳定事件标识去重。不要用 callback 次数直接统计成交、风控或资金事件。

### 10.7 `python -m zippy.cli` 出现 FastAPI/Pydantic 冲突

本验收不依赖 WebUI/CLI 的可选依赖，直接运行第 5 节脚本即可。查询和订阅高层 API 能力
与 Windows 本地 FastAPI 版本无关。
