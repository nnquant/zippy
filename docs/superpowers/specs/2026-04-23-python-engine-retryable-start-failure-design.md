# Python Engine Retryable Start Failure Design

## 背景

当前 `zippy-python` 的 `start_runtime_engine()` 在 external source 场景下会先执行：

- `engine.take()`
- 再进入 `spawn_source_engine_with_publisher(...)`

如果 `spawn_source_engine_with_publisher(...)` 因 `source.start()` 失败而返回错误，Python 包装层会直接把错误返回给用户，但不会把原本取出的 engine 放回对象状态中。结果是：

1. 第一次 `engine.start()` 返回真正的启动错误，例如 `stream not found ...`
2. 第二次对同一个 engine 对象再调用 `start()`，会变成 `engine already started`

这会破坏“外部条件修正后可重试启动”的基本语义。

## 目标

- external source 启动失败后，同一个 Python engine 对象可以再次调用 `start()` 重试。
- 修复应覆盖通用 external source 路径，而不只针对 `BusStreamSource`。
- 保持已有运行时语义不变：
  - source 启动成功后的 fast path / control path 语义不变
  - `engine.on_data()` / `publisher.publish()` 仍只在 worker 线程执行
  - 启动成功后的 metrics / status / source monitor 行为不变

## 非目标

- 不改变 `xfast` 语义。
- 不调整 bus reader 或 runtime dispatch 的等待策略。
- 不为单一 source 类型增加 Python 层特判。
- 不在本次工作中重构 Python engine 生命周期模型。

## 设计选项

### 方案 1：把 core 启动顺序改成“先 source.start() 成功，再消费 engine/publisher”（推荐）

做法：

- 调整 `spawn_source_engine_with_publisher()` 的启动顺序。
- 在 core 里先建立 runtime 队列和 `SourceRuntimeSink`。
- 先调用 `source.start(sink)`。
- 只有当 `source.start()` 成功后，才真正启动 worker 线程并把 `engine` / `publisher` move 进去。
- Python 包装层在 `spawn_source_engine_with_publisher(...)` 返回错误时，可以直接把 `engine` 放回 `self.engine`，因为此时 engine 还未被消耗。

优点：

- 修复覆盖所有 external source。
- 不需要修改 core API 返回类型。
- Python 层只需要做简单的“失败时把 engine 放回”处理。
- 从生命周期上更自然：先确认 source 能启动，再真正启动 runtime worker。

缺点：

- 需要小心处理 `source.start()` 期间同步发出的 `Hello/Data`。
- 必须保证在 worker 尚未启动前，`SourceRuntimeSink.emit()` 的行为仍然安全。

### 方案 2：让 core 在启动失败时返还 `engine`

做法：

- 修改 `spawn_source_engine_with_publisher(...)` 的返回类型，使其在 `source.start()` 失败时把 `engine` 一并返还。

优点：

- 失败恢复语义非常显式。

缺点：

- core API 和错误类型会变复杂。
- 泛型签名改动面更大。
- 与现有 runtime API 风格不一致。

### 方案 3：只在 Python 层为 `BusStreamSource` 做 preflight 检查

做法：

- 在 `start_runtime_engine()` 中，对于 `BusStreamSource` 先做 stream existence 检查，失败则不执行 `engine.take()`。

优点：

- 改动最小。

缺点：

- 只修 bus source。
- 对 `PythonSourceBridge`、未来的新 source 类型无效。
- 本质上是补丁，不是通用生命周期修复。

## 采用方案

采用方案 1。

## 架构设计

### core 启动顺序

当前顺序大致是：

1. 创建 queue / status / metrics
2. 创建 fast path 共享状态
3. 启动 worker 线程，并把 `engine` / `publisher` move 进线程
4. 调 `source.start(sink)`
5. 若 `source.start()` 失败，再尝试清理 worker

调整后的顺序是：

1. 创建 queue / status / metrics
2. 创建 fast path 共享状态
3. 创建 `SourceRuntimeSink`
4. 调 `source.start(sink)`
5. 若 `source.start()` 成功，再启动 worker 线程，并把 `engine` / `publisher` move 进线程
6. 再启动 source monitor 线程并返回 `EngineHandle`

这样，`source.start()` 失败时：

- worker 还没启动
- `engine` 仍在当前栈帧中
- `publisher` 仍在当前栈帧中
- Python 层可以安全恢复 engine 所有权

### `SourceRuntimeSink` 在 worker 启动前的语义

设计约束：

- `source.start()` 可能同步发出 `Hello` / `Data` / `Error`
- worker 尚未启动前，这些事件仍需安全进入队列/fast queue

因此：

- `SourceRuntimeSink` 仍然在 `source.start()` 前创建
- fast path 的 `data_queue` 可先接收数据
- control queue 也可先接收 `Hello/Flush/Stop/Error`
- worker 启动后按现有顺序继续消费这些已排队事件

这要求 `FastDataPath` 中与 worker 强绑定的部分可在 worker 启动后再补齐，例如：

- worker thread handle
- worker_running 标志

或者改成可延迟初始化的共享状态。

### Python 包装层恢复逻辑

在 `start_runtime_engine()` 中：

- 仍然需要 `engine.take()` 来获得所有权
- 但在新的 core 语义下，若 external source 启动失败，`spawn_source_engine_with_publisher(...)` 返回时 engine 还未被消耗
- 因此 Python 层要在错误分支中把 engine 放回原来的 `Option<E>`

目标语义：

- 第一次 `start()` 失败，返回真实错误
- 修正外部条件后，再次 `start()` 可以重新尝试

### `EngineHandle` 与 worker 共享状态

由于 worker 现在晚于 `source.start()` 启动，现有 `FastDataPath` 里直接保存：

- `worker_thread`
- `worker_running`

的结构需要调整为可在 source 成功启动后再就位的形式。

设计要求：

- 不改变已有 `flush/stop` barrier 语义
- 不破坏 `emit_lock`
- 不破坏 `SourceTerminated` monitor drain 语义

可接受的实现方向包括：

- 把 `FastDataPath` 中的 worker 相关字段做成延迟填充的共享槽位
- 或把 worker 相关状态放入单独共享结构，在 worker 创建后再写入

本 spec 不限定具体数据结构，但要求满足上述语义。

## 数据流

### source 启动成功

1. Python 调 `engine.start()`
2. Python 层取出 engine，并调用 core `spawn_source_engine_with_publisher(...)`
3. core 创建 sink
4. `source.start(sink)` 成功
5. core 启动 worker
6. 已由 source 同步 emit 的事件被 worker 正常消费
7. Python 层保存 `EngineHandle`

### source 启动失败

1. Python 调 `engine.start()`
2. Python 层取出 engine，并调用 core `spawn_source_engine_with_publisher(...)`
3. core 创建 sink
4. `source.start(sink)` 返回错误
5. core 直接返回错误，不启动 worker
6. Python 层把 engine 放回 `self.engine`
7. 用户修正外部条件后可再次调用 `start()`

## 错误处理

- 若 `source.start()` 失败：
  - 返回原始启动错误
  - 不应留下 worker 线程
  - 不应消耗 Python engine 对象中的 engine 所有权
- 若 source 启动成功后，后续运行失败：
  - 保持现有错误传播与 `EngineHandle` 语义
- 若 Python 层在失败恢复过程中再遇到异常：
  - 优先返回最原始、最具诊断价值的启动错误

## 测试策略

### Rust

更新 `source_runtime`：

- 增加一个 external source `start()` 失败但不会留下 worker 的回归用例
- 覆盖 fast path 下 `source.start()` 前已有 emit 数据、启动成功后仍能被正常消费的场景

### Python

更新 `pytests/test_python_api.py`：

- 增加 `BusStreamSource` 或其他 external source 的启动失败重试测试：
  - 第一次 `start()` 因外部条件失败
  - 修正条件后第二次 `start()` 不再报 `engine already started`
- 保持已有 `config()`、`xfast` 测试不回归

## 验证策略

至少执行：

- `cargo test -p zippy-core --test source_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`
- `uv run pytest pytests/test_python_api.py -k "start failure or engine already started" -v`

如需补充，可再跑：

- `cargo test -p zippy-core --test runtime_fast_path --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

## 风险与权衡

- 把 worker 启动推迟到 `source.start()` 之后，会让 `FastDataPath` 初始化次序更复杂。
- `source.start()` 期间同步 emit 的事件在 worker 尚未启动时进入队列，这是本设计最需要小心验证的点。
- 但与 Python 层做 source-type 特判相比，这种方式的生命周期语义更稳定，也更容易长期维护。

## 成功标准

- external source 启动失败后，同一个 Python engine 对象可再次 `start()`
- 失败后不会出现 `engine already started` 假阳性
- 不留下 worker 线程或忙等线程
- 启动成功后的现有 runtime 语义不回归
