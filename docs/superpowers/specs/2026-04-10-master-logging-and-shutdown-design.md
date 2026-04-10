# Zippy Master Logging and Graceful Shutdown Design

## Goal

为 `zippy-master` 增加一套正式的统一日志能力，并确保以下两种运行方式都支持 `Ctrl+C` 优雅退出：

```bash
uv run zippy master run ~/.zippy/master.sock
cargo run -p zippy-master -- ~/.zippy/master.sock
```

本设计的目标不是扩展数据面能力，而是让 `zippy-master` 成为一个可观察、可控、可正常收口的基础设施进程。

## Scope

### 包含

- `zippy-master` 复用 `zippy-core::logging`
- `zippy-master` 独立进程自行初始化日志
- 控制台文本日志
- 文件 JSONL 日志
- `SIGINT` / `SIGTERM` 优雅退出
- `Ctrl+C` 后 socket 清理与正常收口
- 生命周期日志
- 控制面请求日志
- Rust 独立入口的最小日志参数：
  - `control_endpoint`
  - `--log-dir`
  - `--log-level`
  - `--no-console-log`

### 不包含

- 数据面高频读写日志
- 每个 batch 的细粒度性能日志
- 配置文件化日志系统
- 远程日志采集
- 复杂日志过滤规则
- `master` UI 或 TUI

## Motivation

当前 `zippy-master` 存在两个明显问题：

1. **缺少必要日志**
- 启动后几乎无输出
- 无法从日志判断是否真的监听成功
- 无法追踪 `register_process/register_stream/write_to/read_from` 这类关键控制面事件

2. **`Ctrl+C` 退出不稳**
- 作为独立进程运行时，不能稳定优雅退出
- 这会影响：
  - socket 文件清理
  - 本地守护进程体验
  - CLI 壳层与 Rust 入口的一致性

`zippy-master` 现在已经是单机数据中心的控制平面核心，不应该继续依赖“无日志 + 不确定退出”的开发态行为。

## Principles

- `zippy-master` 自己负责日志与信号处理
- Python CLI 不能替代 daemon 处理 OS signal
- 日志必须复用仓库现有统一日志系统
- 生命周期日志优先于高频细节日志
- 控制面日志优先于数据面日志
- 正常启动后必须有明确可见的 ready 信号
- 收到退出信号后必须优雅停止并清理 socket

## Logging Strategy

### Logging Backend

`zippy-master` 直接复用 `zippy-core::logging`：

- `setup_log(...)`
- 控制台文本输出
- 文件 JSONL 输出

不新增独立 logger，不使用 `println!/eprintln!` 作为正式日志输出。

### Default Logging Config

独立 `zippy-master` 进程默认使用：

- `app = "zippy-master"`
- `level = "info"`
- `log_dir = "logs"`
- `to_console = true`
- `to_file = true`

生成日志文件类似：

```text
logs/zippy-master/<date>_<run_id>.jsonl
```

### Required Log Events

首版必须覆盖两类日志。

#### 1. 生命周期日志

- `master_start`
- `master_listening`
- `master_shutdown_requested`
- `master_stopped`
- `master_fatal_error`

最少字段：

- `component=[master]`
- `event=[...]`
- `control_endpoint=[...]`
- `status=[...]`
- `message`

#### 2. 控制面请求日志

- `register_process`
- `register_stream`
- `write_to`
- `read_from`
- `list_streams`
- `get_stream`

最少字段：

- `component=[master_server]`
- `event=[...]`
- `process_id=[...]` 如果有
- `stream_name=[...]` 如果有
- `status=[success|error]`
- `message`

### Logging Exclusions

首版明确不记录：

- 每次 reader 实际读取一条 batch
- 每次 writer 实际写入一条 batch
- 高频队列深度变化
- 每个 batch 的 payload 详情

原因是这些信息量太大，会淹没控制面诊断价值。

## Graceful Shutdown

### Supported Entrypoints

以下两条运行路径都必须支持优雅退出：

```bash
uv run zippy master run ~/.zippy/master.sock
cargo run -p zippy-master -- ~/.zippy/master.sock
```

### Signal Handling

`zippy-master` Rust 入口必须安装：

- `SIGINT`
- `SIGTERM`

处理策略：

1. 记录 `master_shutdown_requested`
2. 调用 `server.shutdown()`
3. 让 `serve()` 主循环检测到 `running=false`
4. 退出监听循环
5. 清理 socket 文件
6. 记录 `master_stopped`

### Failure Semantics

如果发生以下情况：

- bind socket 失败
- 删除旧 socket 失败
- serve 循环返回错误
- 线程/内部运行异常

则必须：

- 记录 `master_fatal_error`
- 返回非零退出码

不能静默失败。

## Entrypoint Parameters

### Rust `zippy-master` Binary

首版参数建议：

```text
zippy-master [CONTROL_ENDPOINT] [--log-dir LOG_DIR] [--log-level LEVEL] [--no-console-log]
```

其中：

- `CONTROL_ENDPOINT`
  - 默认 `~/.zippy/master.sock`
- `--log-dir`
  - 默认 `logs`
- `--log-level`
  - 默认 `info`
- `--no-console-log`
  - 关闭控制台输出，仅保留文件日志

### Python CLI

Python CLI 的 `zippy master run` 不应该自己实现第二套日志系统。

它只作为壳层，最终也应走同一套 `zippy-master` 启动语义。

这意味着：

- Python CLI 启动 `master` 时，日志行为与 Rust 独立入口一致
- 不能出现“CLI 启动有日志，cargo 启动没日志”的分裂

## Operational Behavior

正常启动时，至少应能观察到：

1. `master_start`
2. `master_listening`

正常退出时，至少应能观察到：

1. `master_shutdown_requested`
2. `master_stopped`

控制面请求发生时，应在日志中看到：

- 谁注册了 process
- 谁注册了 stream
- 哪个 process 对哪个 stream 执行了 `write_to/read_from`
- `list_streams/get_stream` 是否成功

## Testing

首版至少需要覆盖：

1. `cargo run -p zippy-master -- ~/.zippy/master.sock`
- 能正常启动
- 能响应 `Ctrl+C`
- 退出后 socket 文件被清理

2. `uv run zippy master run ~/.zippy/master.sock`
- 能正常启动
- 能响应 `Ctrl+C`
- 退出后 socket 文件被清理

3. 生命周期日志
- 启动与退出事件实际写入日志

4. 控制面日志
- `register_process/register_stream/write_to/read_from/list_streams/get_stream`
  至少一条请求可在日志中看到对应事件

## Rollout Order

推荐实现顺序：

1. 给 `zippy-master` 独立入口补日志初始化
2. 给 `main.rs`/`server.rs` 接生命周期日志
3. 接控制面请求日志
4. 加信号处理与优雅退出
5. 验证 Rust 入口和 Python CLI 入口两条路径

## Future Extensions

这次设计为后续扩展留出空间：

- `--json-console`
- 更细粒度日志级别
- 更强的控制面 tracing span
- `master status`
- reader lag 诊断日志

但这些不属于 V1。
