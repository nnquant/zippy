# zippy pm rspm bridge design

## 背景

`rspm` 是一个基于 Rust 的任务进程管理器，已经具备 TOML 任务配置、DAG 编排、
daemon control plane、健康检查、日志、定时调度、Rust SDK 和 Python SDK 等能力。
`zippy` 需要把这套能力继承进自身，用 `zippy pm ...` 作为用户入口，为本机
`zippy-master`、gateway 和自定义任务提供统一生命周期管理。

第一版选择 bridge 方案：`zippy` 不直接复制 `rspm` crate，也不重写最小 runtime；
而是在 `zippy` 内建立明确的 adapter 层，复用 `rspm` SDK / runtime 能力，同时把所有
用户可见语义收敛到 `zippy` 命名空间。

## 目标

1. 提供 `zippy pm add/start/stop/restart/apply/validate/status/ls/logs/events/doctor`
   等本机进程管理入口。
2. 默认配置文件为 `zippy.pm.toml`，并通过 `--file/-f` 支持任意兼容 TOML。
3. 用户可见 daemon 名称为 `zippy-rspm`。
4. 默认 runtime 目录为 `.zippy/rspm/`，包含 `logs/`、`state/`、`run/`。
5. 提供 zippy-aware 任务模板：`master`、`gateway`、`custom`。
6. 保持 `rspm` 的 task-centric 边界，不在第一版引入中心控制多节点或 cluster 语义。

## 非目标

1. 不实现跨机器中心控制面，例如 `[nodes]`、SSH fanout、远程 agent 编排。
2. 不把 `rspm-core`、`rspm-daemon`、`rspm-sdk` 立即 vendor 进 `zippy` workspace。
3. 不把 `zippy.pm.toml` 合并进现有 `zippy.toml`。
4. 不把 `zippy pm` 做成对用户 PATH 中任意 `rspm` binary 的裸 shell wrapper。

## 用户接口

`zippy pm` 是唯一公开入口。第一版命令面：

```text
zippy pm validate [-f zippy.pm.toml]
zippy pm apply [-f zippy.pm.toml] [--dry-run]
zippy pm add master [options]
zippy pm add gateway [options]
zippy pm add custom <command> [options]
zippy pm start <task|all>
zippy pm stop <task|all>
zippy pm restart <task|all>
zippy pm status
zippy pm ls
zippy pm logs [task] [--follow] [--lines N]
zippy pm events
zippy pm doctor
```

全局默认值：

```text
config file: zippy.pm.toml
daemon name: zippy-rspm
log dir:     .zippy/rspm/logs
state dir:   .zippy/rspm/state
run dir:     .zippy/rspm/run
socket path: .zippy/rspm/run/zippy-rspm.sock
```

如果底层 `rspm` 当前只稳定支持 TCP transport，第一版 adapter 可以先使用 TCP 默认端口；
但目录、日志、pid、doctor 输出必须使用 `.zippy/rspm/` 语义。socket path 作为后续
Unix socket / Windows named-pipe 对齐的固定默认值保留。

## 配置模型

`zippy.pm.toml` 保持 `rspm` 兼容 task schema，避免第一版制造第二套 runtime schema。
zippy 只在模板生成阶段注入更高层语义。

示例：

```toml
[project]
name = "zippy-local"
timezone = "Asia/Shanghai"
display_timezone = "Asia/Shanghai"

[defaults]
restart = "on-failure"
restart_delay = "2s"
max_restarts = 5

[tasks.master]
cmd = "zippy"
args = ["master", "run", "tcp://127.0.0.1:17690"]
autostart = true

[tasks.master.health]
type = "tcp"
address = "127.0.0.1:17690"
interval = "1s"
timeout = "500ms"
success_after = 1
failure_after = 3

[tasks.gateway]
cmd = "zippy"
args = ["gateway", "run", "--master-uri", "tcp://127.0.0.1:17690"]
depends_on = ["master"]
start_when = "healthy"
autostart = true
```

## Adapter 边界

`zippy` 内新增 adapter 层，建议命名为 `zippy_pm` 或 `cli_pm.py`。职责：

1. 解析 `zippy pm` CLI 参数。
2. 生成和修改 `zippy.pm.toml`。
3. 注入 zippy 默认 runtime 目录、daemon 名称、配置路径。
4. 调用 `rspm` SDK 或明确配置的 runtime API。
5. 捕获底层错误，并把用户可见文案改写成 zippy 语义。

adapter 不直接散落在各个 CLI 文件中。这样后续如果从 bridge 迁移到 workspace 原生
crate，CLI 命令面和用户配置都可以保持稳定。

## Runtime 依赖

第一版优先级：

1. 优先使用 `rspm` SDK / library API 管理 task 和 daemon。
2. daemon 自启动需要 binary 时，使用显式 binary path 配置，默认寻找 `zippy-rspm`。
3. 不依赖用户 PATH 中的 `rspm` 作为产品入口。

如果当前 `rspm` SDK 不能覆盖某些 CLI 功能，允许短期通过一个内部 helper 调用
`zippy-rspm` binary，但必须满足：

1. helper 只存在于 adapter 层。
2. helper 传入完整 zippy 默认值。
3. 错误信息和输出保持 `zippy-rspm` / `.zippy/rspm/` / `zippy.pm.toml` 语义。
4. spec 中记录为 bridge 技术债，后续替换为 SDK API。

## 任务模板

### master

`zippy pm add master` 生成本机 master task。

默认行为：

1. 命令：`zippy master run tcp://127.0.0.1:17690`。
2. 健康检查：TCP `127.0.0.1:17690`。
3. `autostart = true`。
4. restart 策略继承 defaults，必要时允许命令行覆盖。

### gateway

`zippy pm add gateway` 生成本机 gateway task。

默认行为：

1. 命令：`zippy gateway run --master-uri tcp://127.0.0.1:17690`。
2. 默认依赖 `master`。
3. `start_when = "healthy"`。
4. 如提供 gateway endpoint/token，则写入 args。

### custom

`zippy pm add custom <command>` 保留通用 task 能力。它只负责把命令写入
`zippy.pm.toml`，不注入 zippy 服务语义。

## 错误处理

1. 配置不存在时，`validate/apply/start` 给出明确路径：`zippy.pm.toml`。
2. daemon 不可达时，输出 `zippy-rspm` endpoint、pid file、日志路径。
3. task 不存在时，列出当前已知 task 名称。
4. 底层 `rspm` 错误不原样泄漏产品名，除非错误来自兼容说明或 debug 输出。
5. 所有 Python CLI 异常通过现有 `cli_error()` 风格退出。

## 测试策略

1. Python CLI 单元测试：
   - `zippy pm --help` 包含 pm 命令。
   - `zippy pm validate -f <tmp>` 调用 adapter 并正确处理默认文件。
   - `zippy pm add master/gateway/custom` 生成确定性的 TOML。
2. Adapter 测试：
   - 默认路径为 `.zippy/rspm/...`。
   - 默认配置文件为 `zippy.pm.toml`。
   - daemon 名称为 `zippy-rspm`。
3. 集成烟测：
   - 使用临时目录生成 `zippy.pm.toml`。
   - `validate` 通过。
   - 在可用 runtime 下执行 `apply/start/stop/status`。
4. 不要求第一版多节点测试。

## 迁移路径

bridge 方案是过渡架构。未来若需要把 `rspm` 完整原生化进 `zippy`：

1. 将 `rspm-core`、`rspm-daemon`、`rspm-sdk` 迁移为 `zippy-rspm-core`、
   `zippy-rspm-daemon`、`zippy-rspm-sdk`。
2. 保持 `zippy pm` CLI 不变。
3. 保持 `zippy.pm.toml` schema 兼容。
4. 保持 adapter API 不变，只替换底层实现。
5. 再考虑中心控制多节点能力。

## 验收标准

1. `zippy pm validate` 默认读取 `zippy.pm.toml`。
2. `zippy pm add master/gateway/custom` 能生成或更新 `zippy.pm.toml`。
3. `zippy pm apply/start/stop/restart/status/ls/logs/events/doctor` 命令存在，并通过
   adapter 使用 zippy 默认 runtime 语义。
4. 用户可见输出不出现 `.rspm/` 默认路径。
5. 本轮实现不提交当前工作区已有的无关改动。
6. 相关 Python/Rust 测试通过，无法执行的外部 runtime 验证需要明确说明原因。
