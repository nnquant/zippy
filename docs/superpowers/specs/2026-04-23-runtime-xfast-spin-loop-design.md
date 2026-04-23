# Runtime Xfast Spin Loop Design

## 背景

当前 `zippy` 的低延迟链路已经分成两层等待策略：

- bus reader 侧已有 `xfast`，开启后采用纯 `spin_loop()`，以占满一个 CPU 核为代价换取更低的读延迟。
- runtime dispatch 侧在 `Pipeline + Block` fast path 下，当前仍使用固定的 `spin_loop() + park_timeout(50us)`。

实测表明，runtime fast path 已经显著降低了 `publisher_dispatch`，但 dispatch 路径仍保留了 `park_timeout(50us)` 这一固定让出 CPU 的行为。对于明确追求最低延迟、能够接受单核满载的场景，需要给 runtime 侧提供与 reader 侧对齐的 `xfast` 选项。

## 目标

- 为 runtime 新增 `xfast` 配置，并从 Python Engine 上层透传。
- 在 `xfast=true` 时，把 runtime fast path 的等待策略改成纯 `spin_loop()`，不再调用 `park_timeout(50us)`。
- 保持现有 fast path 的边界不变：
  - 仅 `spawn_source_engine_with_publisher() + SourceMode::Pipeline + OverflowPolicy::Block` 启用双通道 fast path。
  - `engine.on_data()` / `publisher.publish()` 仍只在 worker 线程执行。
  - `Hello/Flush/Stop/Error/SourceTerminated` 仍走 control queue。
- 保持默认行为不变：未显式开启 `xfast` 时，runtime 继续使用当前的 `spin_loop() + park_timeout(50us)`。

## 非目标

- 不把 bus reader 的 `xfast` 与 runtime 的 `xfast` 合并成同一个开关。
- 不修改现有 reader `xfast` 的行为。
- 不改变 fast path 的 gating 条件。
- 不在本次工作中继续优化 `arrow_encode`、bus write 或 reader decode。

## 设计选项

### 方案 1：Engine 级新增 `xfast`，runtime 在 `xfast=true` 时纯 `spin_loop()`（推荐）

做法：

- 在 `zippy-core::EngineConfig` 中新增 `xfast: bool`。
- 在 `zippy-python` 的 runtime options 和各类 Engine 构造器中新增 `xfast=False` 关键字参数，并透传到 `EngineConfig`。
- runtime worker 在 `Pipeline + Block + xfast=true` 时：
  - worker 空转等待采用纯 `std::hint::spin_loop()`
  - fast data drain / source-terminated 前置等待也采用纯 `std::hint::spin_loop()`
- `xfast=false` 时保持当前行为不变。

优点：

- 语义清晰，reader 与 runtime 的低延迟策略各自独立。
- 兼容现有 bus source、python source、remote source 的统一 runtime 配置模型。
- 不需要隐式复用 source 侧的 `xfast`，边界更稳定。

缺点：

- 需要在端到端低延迟场景中分别配置 reader `xfast` 与 runtime `xfast`。

### 方案 2：复用 `BusStreamSource.xfast` 控制 runtime 等待

做法：

- 仅在 bus source 场景下，把 `BusStreamSource.xfast` 顺便映射到 runtime dispatch。

优点：

- 改动最小。

缺点：

- 把 reader 等待策略与 runtime 等待策略耦合到一个 source 参数上。
- 只对 bus source 生效，无法覆盖 python/remote source，语义不统一。

### 方案 3：所有 `Pipeline + Block` 统一纯 `spin_loop()`

做法：

- 不新增配置，直接把 runtime fast path 默认改成纯 busy spin。

优点：

- 实现最简单。

缺点：

- 默认就吃满一个核，破坏现有默认资源占用特性。
- 不适合作为通用默认行为。

## 采用方案

采用方案 1。

## 架构设计

### 配置模型

在 `zippy-core` 中：

- `EngineConfig` 新增：
  - `xfast: bool`

在 `zippy-python` 中：

- `RuntimeOptions` 新增：
  - `xfast: bool`
- 所有会创建 runtime 的 Python Engine 构造器新增关键字参数：
  - `xfast=False`
- runtime config introspection 同步暴露该字段。

### runtime 等待策略

仅在满足以下条件时，runtime 采用双通道 fast path：

- `SourceMode::Pipeline`
- `OverflowPolicy::Block`

在此基础上：

- `xfast=false`
  - 保持现状：
    - 空轮询时 `spin_loop() + park_timeout(50us)`
    - fast data drain 等待时 `spin_loop() + park_timeout(50us)`
- `xfast=true`
  - 改为纯忙等：
    - 空轮询时仅 `spin_loop()`
    - fast data drain 等待时仅 `spin_loop()`

非 fast path 场景保持现状：

- `Consumer` 模式或非 `Block` overflow 仍沿用既有 control queue 路径。
- 旧路径不因 `xfast` 改变行为。

### 与现有 reader `xfast` 的关系

两者相互独立：

- `BusStreamSource(..., xfast=True)` 只控制 bus reader 的等待策略。
- `Engine(..., xfast=True)` 只控制 runtime worker 的等待策略。

因此，端到端最低延迟场景下需要显式同时开启：

- reader `xfast`
- runtime `xfast`

## 数据流与行为

### `xfast=false`

`source.emit(Data)` -> `SpscDataQueue` -> worker `try_recv/try_pop` -> 无数据时 `spin_loop + park_timeout(50us)` -> `engine.on_data()` -> `publisher.publish()`

### `xfast=true`

`source.emit(Data)` -> `SpscDataQueue` -> worker `try_recv/try_pop` -> 无数据时纯 `spin_loop()` -> `engine.on_data()` -> `publisher.publish()`

控制事件路径不变：

`Hello/Flush/Stop/Error/SourceTerminated` -> control queue -> worker

## 错误处理与并发语义

- 维持当前 fast path 的 `emit_lock` 串行化语义，不新增并发窗口。
- 维持当前 `worker_running` + queue drain 语义，避免 worker 失败后 control emit 永久等待。
- `SourceTerminated` 在 fast path 下仍需等待已发出的 fast data 排空，避免抢在 data 前面。
- `xfast=true` 只改变等待策略，不改变错误传播、队列顺序或 shutdown 语义。

## 测试策略

### Rust

- 更新 `runtime_fast_path`：
  - 覆盖 `xfast=true` 场景下 runtime fast path 仍通过现有顺序与 shutdown 测试。
  - 覆盖 `Consumer + Block` 等非 fast path 场景不受 `xfast` 影响。
- 更新 `source_runtime`：
  - 回归验证 `hello/data/flush/stop/source_terminated` 语义不变。

### Python

- 更新 `pytests/test_python_api.py`：
  - 验证 Python Engine 构造器接受 `xfast` 参数。
  - 验证 runtime config/introspection 暴露 `xfast`。

## 验证策略

实现完成后，至少执行：

- `cargo test -p zippy-core --test runtime_fast_path`
- `cargo test -p zippy-core --test source_runtime`
- `uv run pytest pytests/test_python_api.py -k xfast`

如需验证 live 效果，再重编 `zippy-python` 到 LDC venv，并对 `ldc_ctp_ticks` 运行 live probe，对比：

- `publisher_dispatch_avg_delay_ms`
- `queue_total_avg_delay_ms`

## 风险与权衡

- `xfast=true` 会让 runtime worker 在空闲时占满一个 CPU 核，这是预期行为，不应作为默认值。
- 对低吞吐或非超低延迟场景，默认仍应保持 `xfast=false`。
- runtime `xfast` 与 reader `xfast` 独立配置会增加一个参数，但换来了更清晰的边界与更可控的调优路径。

## 成功标准

- runtime 提供独立的 `xfast` 配置口，并可从 Python Engine 透传。
- `xfast=false` 时行为与当前版本保持一致。
- `xfast=true` 时，runtime fast path 的等待策略变为纯 `spin_loop()`。
- 现有 fast path 的顺序、错误传播、shutdown 语义不回归。
