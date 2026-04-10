# Zippy Python CLI Design

## Goal

为 `zippy` 增加一个正式的 Python CLI 入口，支持通过统一命令面管理本地 `zippy-master` 和
后续 `stream / engine / source / sink` 资源。

首版目标是让用户可以直接执行：

```bash
uv run zippy master run /tmp/zippy-master.sock
uv run zippy stream ls
uv run zippy stream show openctp_ticks
```

这个 CLI 不是临时脚本，而是 `zippy` Python 包的一等入口。后续所有管理型命令都挂在这棵命令树上。

## Scope

### 包含

- 新增 `zippy` Python CLI 根命令
- 采用 `click` 构建多级命令树
- 在 `pyproject.toml` 中注册 `project.scripts`
- 首版命令：
  - `zippy master run [CONTROL_ENDPOINT]`
  - `zippy stream ls`
  - `zippy stream show STREAM_NAME`
- `stream ls/show` 依赖 `zippy-master` 控制面查询接口
- CLI 层统一错误输出与 exit code
- CLI 层最小测试与 README 用法说明

### 不包含

- `engine ls/show`
- `source ls/show`
- `sink ls/show`
- 热更新配置
- 自动拉起整条 pipeline
- shell completion
- TUI / Web UI
- 单独的 `zippy-master` Python 包

## Motivation

当前系统已经具备：

- `zippy-master` daemon
- `MasterClient`
- `Writer/Reader`
- `BusStreamSource/BusStreamTarget`

但用户入口仍然割裂：

- 启动 master 依赖 `cargo run -p zippy-master`
- 查询 stream/engine 没有统一命令面
- examples 只能作为示例脚本，不是正式运维入口

随着 `master/bus` 成为单机数据中心的核心设施，需要一个统一管理壳层：

- 面向 Python 用户
- 能被 `uv run` 直接执行
- 与 `MasterClient` / `MasterServer` 共用同一套控制语义

## Command Model

CLI 根命令固定为：

```text
zippy <resource> <action> [arguments...]
```

首版资源分组：

- `master`
- `stream`

后续再扩：

- `engine`
- `source`
- `sink`

这种命令形状比单个扁平命令更稳，原因是：

- 资源边界清楚
- 后续扩展自然
- 与 `master` 注册表对象模型一致

## Packaging and Entry Point

CLI 不写进 `python/zippy/__init__.py`，而是作为单独模块存在。

建议结构：

- `python/zippy/cli.py`
- `python/zippy/cli_master.py`
- `python/zippy/cli_stream.py`
- `python/zippy/cli_common.py`

在 `pyproject.toml` 中新增：

```toml
[project.scripts]
zippy = "zippy.cli:main"
```

这样安装后的用户入口就是：

```bash
uv run zippy ...
```

而不是：

```bash
uv run python -m zippy.cli ...
```

## Implementation Strategy

### click

CLI 使用 `click` 实现，不使用 `argparse` 或 `typer`。

原因：

- 多级命令树更清楚
- 参数帮助和错误体验更成熟
- 后续扩展 `engine/source/sink` 命令时更稳定

### Rust Integration

CLI 层不重新实现 `master` 逻辑，也不 shell out 到 `cargo run`。

首版直接复用 Python 绑定里的 Rust 对象：

- `zippy.MasterServer`
- `zippy.MasterClient`

也就是说：

- `zippy master run` 直接构造 `MasterServer`
- `zippy stream ls/show` 直接构造 `MasterClient`

这保证：

- 开发态和安装态行为一致
- wheel 环境可直接运行
- CLI 只是控制面壳层，不复制核心逻辑

## Commands

### `zippy master run [CONTROL_ENDPOINT]`

作用：

- 前台启动本地 `zippy-master`
- 默认 endpoint 为 `/tmp/zippy-master.sock`
- 阻塞运行直到 `Ctrl-C`

行为：

- 构造 `zippy.MasterServer(control_endpoint=...)`
- 调用 `start()`
- 等待 `join()`
- 收到 `KeyboardInterrupt` 时执行 `stop()` 再 `join()`

输出：

- 启动成功时打印简短确认信息
- 非法 endpoint 或启动失败时返回非零退出码

### `zippy stream ls`

作用：

- 查询当前 master 上所有已注册 stream

行为：

- 通过 `MasterClient` 连接控制面
- 调用 `list_streams()`
- 默认输出人类可读文本表格

最少字段：

- `stream_name`
- `ring_capacity`
- `status`

支持：

- `--control-endpoint`
- `--json`

### `zippy stream show STREAM_NAME`

作用：

- 查询单个 stream 的详细信息

行为：

- 通过 `MasterClient` 连接控制面
- 调用 `get_stream(stream_name)`
- 默认输出 key/value 文本块

最少字段：

- `stream_name`
- `ring_capacity`
- `writer`
- `reader_count`
- `status`
- 如果控制面已暴露，再补 `write_seq`

支持：

- `--control-endpoint`
- `--json`

## Output and Error Style

### Text Output

默认输出优先面向人类阅读：

```text
$ uv run zippy stream ls
STREAM NAME                 RING CAPACITY  STATUS
openctp_ticks               131072         running
openctp_mid_price_factors   131072         running
```

```text
$ uv run zippy stream show openctp_ticks
stream_name: openctp_ticks
ring_capacity: 131072
writer: proc_1
reader_count: 2
status: running
```

### JSON Output

所有查询命令支持 `--json`，便于脚本化调用。

### Errors

CLI 层必须：

- 输出简短英文错误
- 返回非零 exit code
- 不把 Python traceback 直接暴露给普通用户

例如：

```text
error: failed to connect master control_endpoint=[/tmp/zippy-master.sock]
```

## Control Plane Dependencies

为了让 `stream ls/show` 成立，控制面需要补最小查询协议：

- `list_streams`
- `get_stream`

同时 Python 侧 `MasterClient` 也要补：

- `list_streams()`
- `get_stream(name)`

这部分属于本 CLI feature 的必要后端，不是额外范围。

## Testing

首版至少覆盖：

1. `master run`
- 能启动 `MasterServer`
- 能响应 `KeyboardInterrupt` 并优雅退出

2. `stream ls`
- 当 master 中有多个 stream 时，输出正确
- `--json` 输出可解析

3. `stream show`
- 已存在 stream 时输出详情
- 不存在 stream 时返回非零退出码

4. packaging
- `project.scripts` 注册后，`uv run zippy --help` 正常

## Rollout Order

建议实现顺序：

1. 控制面补查询协议
2. `MasterClient` 补查询方法
3. Python CLI 模块与 `project.scripts`
4. CLI 测试
5. README / examples 对齐

## Future Extensions

这个设计明确为后续命令留出空间：

- `zippy engine ls`
- `zippy engine show`
- `zippy source ls`
- `zippy sink ls`
- `zippy master status`

但这些不属于 V1。
