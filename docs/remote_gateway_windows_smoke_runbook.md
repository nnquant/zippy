# Zippy Remote Gateway Windows Smoke Runbook

> 日期：2026-05-05
>
> 目标：在 Windows 原生 Python 进程中验证 `zippy://host:port/default` 远程连接，
> 覆盖 Windows 侧写入、查询和 Gateway token 校验。Linux/WSL 侧仍然使用本地
> segment/mmap 快路径。

## 1. 验收边界

这个 runbook 验证的是 M10 Cross-platform Remote Gateway 的跨平台边界：

```text
Windows Python client
  -> zippy://wsl-host:17690/default
  -> WSL/Linux master tcp control endpoint
  -> master config 下发 GatewayServer endpoint/token
  -> master 管理的 native GatewayServer 写入/查询 WSL/Linux 本地 StreamTable
```

它不验证 Windows 直接 attach Linux mmap segment。Windows 侧只访问 master TCP
控制面和 GatewayServer TCP 数据面。

成功判据：

- Windows 侧 `zippy gateway smoke-client` 可以连接 WSL/Linux master；
- Windows client 不启动本地 master/Gateway；
- Windows client 通过 `zp.get_writer()` 写入 1 行 tick；
- Windows client 通过 `zp.read_table(...).collect()` 查询到刚写入的数据；
- 输出 JSON 中 `rows=1`，`instrument_id=IF2606`，`last_price=4102.5`。

## 2. WSL/Linux 侧配置

准备一个 config，例如 `/tmp/zippy-gateway-smoke.toml`：

```toml
[master]
host = "0.0.0.0"
port = 17690

[gateway]
enabled = true
token = "dev-token"
protocol_version = 1
```

配置含义：

- `enabled = true`：master 会启动 native GatewayServer，并向远端 client 发布 Gateway
  capability；
- gateway 默认监听地址从 master TCP 配置推导，端口为 `master.port + 1`；
- `token`：GatewayServer 的轻量访问令牌，Windows client 会从 master config 自动发现；
- `protocol_version`：当前协议版本，第一版固定为 `1`。

也可以直接使用 `endpoint = "host:port"`，它会优先于默认推导。也可以只覆写
`host` 或 `port` 中的一个，缺失项会从 master 配置推导。如果 Windows
不能直接访问 WSL 的 `0.0.0.0:17691`，需要把配置写成 Windows 可达的 WSL IP 或宿主
转发地址，例如：

```toml
[master]
host = "0.0.0.0"
port = 17690

[gateway]
enabled = true
host = "172.20.10.2"
token = "dev-token"
protocol_version = 1
```

master 自身也可以通过配置文件指定 TCP 监听地址：

```toml
[master]
host = "0.0.0.0"
port = 17690
```

## 3. WSL/Linux 侧启动

在 WSL/Linux 终端启动 TCP master。GatewayServer 会由 master 根据 `[gateway]`
配置自动启动，不需要再单独启动第二个程序：

```bash
uv run zippy master run tcp://0.0.0.0:17690 --config /tmp/zippy-gateway-smoke.toml
```

如果 master 或 Gateway 端口被占用，可以换端口，但要同步修改：

- master 启动 URI；
- Windows 侧 `zippy://<wsl-host>:<master-port>/default`；
- config 中的 `gateway.endpoint`，或从 `[master]` 推导出的 gateway endpoint。

## 4. Windows 侧准备

先确认 Windows 原生 Python 能导入 zippy：

```powershell
python -c "import sys; print(sys.executable); import zippy; print(zippy.__file__)"
```

如果没有安装 zippy，需要先安装 Windows 版 wheel，或在 Windows 环境中构建 editable
扩展。构建前置条件：

- Windows Python 版本与 wheel tag 匹配；
- Rust MSVC toolchain 可用，`rustc --version` 和 `cargo --version` 能正常返回；
- `maturin` 可用；
- rustup 不能卡在 channel 同步或下载超时。

如果 `zippy` console script 不在 `PATH` 中，也可以使用 Python module 入口：

```powershell
python -m zippy.cli --help
```

如果 Windows 侧暂时还没有 zippy wheel，可以先运行纯 Python standalone smoke。这个脚本
不导入 `zippy`，只依赖 `pyarrow`，用于先验证 Windows 到 WSL/Linux master/Gateway 的
网络和 wire protocol：

```powershell
python examples\08_remote_gateway\04_standalone_windows_smoke_client.py `
  --uri zippy://<wsl-host>:17690/default `
  --stream windows_standalone_smoke_ticks `
  --json
```

standalone smoke 通过后，只能说明跨平台协议和 Gateway 数据面可达；完整用户 API
验收仍然需要安装 Windows 版 zippy 后运行 `zippy gateway smoke-client`。

如果 Windows 侧暂时不能构建 native `_internal` 扩展，但可以拿到 `python/zippy`
源码目录，也可以用 pure Python remote fallback 验证高层 API。这个模式只支持
remote Gateway 路径，本地 segment、Engine、master server 等 native 能力会明确报错：

```powershell
$env:PYTHONPATH = "C:\path\to\zippy\python"
$env:ZIPPY_FORCE_PURE_PYTHON = "1"
python - <<'PY'
import json
import pyarrow as pa
import zippy as zp

schema = pa.schema([
    ("instrument_id", pa.string()),
    ("last_price", pa.float64()),
])
zp.connect("zippy://<wsl-host>:17690/default", app="windows_pure_remote")
writer = zp.get_writer("windows_pure_ticks", schema=schema, batch_size=1)
writer.write({"instrument_id": "IF2606", "last_price": 4102.5})
writer.close()
table = zp.read_table("windows_pure_ticks").collect()
print(json.dumps({"rows": table.num_rows, "data": table.to_pydict()}))
PY
```

## 5. Windows 侧 smoke

在 Windows 原生 Python 环境中安装或切换到当前 zippy 包后运行：

```powershell
zippy gateway smoke-client `
  --uri zippy://<wsl-host>:17690/default `
  --stream windows_smoke_ticks `
  --json
```

其中 `<wsl-host>` 必须是 Windows 能访问到的 WSL/Linux 地址。常见选择：

- WSL VM IP；
- Windows 到 WSL 的端口转发地址；
- 同网段 Linux 主机 IP。

期望输出类似：

```json
{
  "status": "ok",
  "uri": "zippy://<wsl-host>:17690/default",
  "stream": "windows_smoke_ticks",
  "rows": 1,
  "instrument_id": "IF2606",
  "last_price": 4102.5
}
```

## 6. 失败排查

### 6.1 master 连接失败

现象：

```text
io error reason=[connection refused]
```

排查：

- 确认 WSL/Linux master 用 `tcp://0.0.0.0:17690` 或可达 IP 启动；
- 确认 Windows 侧 `<wsl-host>:17690` 可以连通；
- 检查 Windows 防火墙、WSL 网络、端口转发配置。
- 查看 master 启动日志中打印的 master host/port 和 gateway host/port。

### 6.2 Gateway 连接失败

现象：

```text
gateway endpoint unavailable
```

排查：

- 确认 master config 中 `[gateway] enabled = true`，并查看 master 启动日志中
  GatewayServer 是否启动成功；
- 确认 master config 中 `[gateway].endpoint` 或推导出的 gateway endpoint 是 Windows 可达地址；
- 不要把 endpoint 写成 `127.0.0.1:17666`，除非 Windows client 与 GatewayServer
  在同一个 OS 网络命名空间内。

### 6.3 token 错误

现象：

```text
gateway request unauthorized
```

排查：

- 确认 master config 中的 `[gateway].token` 与 Windows client 自动发现的 token 一致；
- 如果不需要 token，两边都不要配置 token；
- 不要只改配置文件而忘记重启 master。

### 6.4 Windows Python 没有 zippy

现象：

```text
ModuleNotFoundError: No module named 'zippy'
```

排查：

- 确认当前 `python` 是预期的 Windows Python，不是 WSL Python；
- 优先安装 Windows 版 zippy wheel；
- 如果需要本地构建，先让 `rustc --version`、`cargo --version` 和 `python -m pip show maturin`
  都能正常返回。

### 6.5 查询不到刚写入的数据

排查：

- 确认 `--stream` 没有拼写错误；
- 确认 smoke-client 输出里没有 write error；
- 换一个新的 stream name 重试，避免旧测试数据或旧 master metadata 干扰。

## 7. Linux 侧替代 smoke

如果当前没有 Windows 环境，可以先在 Linux/WSL 内跑跨进程 smoke：

```bash
uv run zippy gateway smoke \
  --master-uri tcp://127.0.0.1:28790 \
  --gateway-endpoint 127.0.0.1:28766 \
  --token dev-token \
  --stream gateway_smoke_ticks \
  --timeout-sec 20 \
  --json
```

这只能证明 Gateway 协议、URI、master config 下发和跨进程 client 路径可用，不能替代
Windows 原生进程验收。
