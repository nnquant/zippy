# Runtime Xfast Spin Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 runtime 新增独立的 `xfast` 配置，并在 `Pipeline + Block` fast path 下于 `xfast=true` 时改成纯 `spin_loop()`。

**Architecture:** 在 `zippy-core::EngineConfig` 中引入 `xfast`，由 `zippy-python` 的 `RuntimeOptions` 与各类 Engine 构造器透传。runtime fast path 继续保留双通道架构，只把等待策略从“`spin_loop + park_timeout(50us)`”切成可配置模式：默认不变，`xfast=true` 时纯忙等。

**Tech Stack:** Rust, PyO3, `zippy-core` runtime, `zippy-python`, cargo tests, pytest, live bus probe

---

### Task 1: 扩展 `EngineConfig` 以承载 runtime `xfast`

**Files:**
- Modify: `crates/zippy-core/src/types.rs`
- Modify: `crates/zippy-core/tests/runtime_lifecycle.rs`
- Modify: `crates/zippy-core/tests/source_runtime.rs`
- Modify: `crates/zippy-core/tests/runtime_fast_path.rs`

- [ ] **Step 1: 先写失败测试，约束 `EngineConfig` 暴露 `xfast` 且默认可显式关闭**

```rust
#[test]
fn engine_config_accepts_explicit_xfast_flag() {
    let config = EngineConfig {
        name: "runtime-xfast".to_string(),
        buffer_capacity: 8,
        overflow_policy: OverflowPolicy::Block,
        late_data_policy: Default::default(),
        xfast: true,
    };

    config.validate().unwrap();
    assert!(config.xfast);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-core --test runtime_lifecycle engine_config_accepts_explicit_xfast_flag --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示 `EngineConfig` 缺少 `xfast` 字段。

- [ ] **Step 3: 写最小实现，给 `EngineConfig` 新增 `xfast`**

```rust
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub name: String,
    pub buffer_capacity: usize,
    pub overflow_policy: OverflowPolicy,
    pub late_data_policy: LateDataPolicy,
    pub xfast: bool,
}
```

并把当前所有直接构造 `EngineConfig` 的测试补成显式字段：

```rust
EngineConfig {
    name: "bars".to_string(),
    buffer_capacity: 1024,
    overflow_policy: Default::default(),
    late_data_policy: Default::default(),
    xfast: false,
}
```

- [ ] **Step 4: 运行相关测试确认通过**

Run: `cargo test -p zippy-core --test runtime_lifecycle --test source_runtime --test runtime_fast_path --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，`EngineConfig` 编译通过，现有 runtime/source 行为测试不回归。

- [ ] **Step 5: 提交 `EngineConfig` 扩展**

```bash
git add crates/zippy-core/src/types.rs crates/zippy-core/tests/runtime_lifecycle.rs crates/zippy-core/tests/source_runtime.rs crates/zippy-core/tests/runtime_fast_path.rs
git commit -m "feat: add runtime xfast engine config"
```

### Task 2: 在 runtime fast path 下按 `xfast` 切换为纯 `spin_loop()`

**Files:**
- Modify: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-core/tests/runtime_fast_path.rs`
- Modify: `crates/zippy-core/tests/source_runtime.rs`

- [ ] **Step 1: 写失败测试，约束 `xfast=true` 仍保持 fast path 顺序语义**

在 `crates/zippy-core/tests/runtime_fast_path.rs` 新增一个基于现有夹具的场景，显式传入 `xfast=true`：

```rust
#[test]
fn source_pipeline_block_mode_with_xfast_keeps_control_ordering() {
    let (runtime, first_data_release_tx) =
        spawn_test_runtime_with_behavior(SourceMode::Pipeline, OverflowPolicy::Block, true, false);
    runtime.source_emit_data(single_row_batch(1)).unwrap();
    runtime.wait_until_first_data_started();

    runtime.source_emit_flush().unwrap();
    let stop_result = runtime.source_emit_stop_async();
    assert!(stop_result.recv_timeout(Duration::from_millis(100)).is_err());

    first_data_release_tx.send(()).unwrap();
    assert!(matches!(
        stop_result.recv_timeout(Duration::from_secs(1)),
        Ok(Ok(()))
    ));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-core --test runtime_fast_path source_pipeline_block_mode_with_xfast_keeps_control_ordering --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: FAIL，提示测试夹具尚未支持 `xfast` 参数或 runtime 还未消费该配置。

- [ ] **Step 3: 写最小实现，把 runtime 等待策略抽成按 `xfast` 分支**

在 `crates/zippy-core/src/runtime.rs` 中引入等待辅助函数，替换当前硬编码的 `park_timeout(50us)`：

```rust
fn runtime_idle_wait(xfast: bool) {
    std::hint::spin_loop();
    if !xfast {
        thread::park_timeout(Duration::from_micros(50));
    }
}

fn wait_for_fast_data_queue_drain(
    data_queue: &SpscDataQueue<RecordBatch>,
    worker_running: &AtomicBool,
    xfast: bool,
) {
    while worker_running.load(Ordering::Acquire) && !data_queue.is_empty() {
        runtime_idle_wait(xfast);
    }
}
```

并在 worker fast path 循环里改成：

```rust
runtime_idle_wait(config.xfast);
```

以及把 `wait_for_fast_data_drain(...)` / monitor 转发 `SourceTerminated` 的 drain 等待都改为带 `xfast` 参数。

- [ ] **Step 4: 更新测试夹具，让 `runtime_fast_path` 能显式构造 `xfast=true/false`**

把测试配置函数改为：

```rust
fn test_engine_config(overflow_policy: OverflowPolicy, xfast: bool) -> EngineConfig {
    EngineConfig {
        name: "runtime-fast-path".to_string(),
        buffer_capacity: 1,
        overflow_policy,
        late_data_policy: Default::default(),
        xfast,
    }
}
```

并把 `spawn_test_runtime_with_behavior(...)` 扩成：

```rust
fn spawn_test_runtime_with_behavior(
    source_mode: SourceMode,
    overflow_policy: OverflowPolicy,
    xfast: bool,
    fail_after_first_release: bool,
) -> (TestPipelineRuntime, Sender<()>) { ... }
```

- [ ] **Step 5: 运行 Rust 回归测试确认通过**

Run: `cargo test -p zippy-core --test runtime_fast_path --test source_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，fast path 在 `xfast=true` 下仍保持当前顺序、错误传播与 shutdown 语义。

- [ ] **Step 6: 提交 runtime `xfast` 等待策略**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-core/tests/runtime_fast_path.rs crates/zippy-core/tests/source_runtime.rs
git commit -m "feat: add runtime xfast spin wait"
```

### Task 3: 从 Python Engine 上层透传 runtime `xfast`

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: 写失败测试，约束 Python Engine 暴露 `xfast` 并回显到 `config()`**

在 `pytests/test_python_api.py` 中给 `ReactiveStateEngine` 增加断言：

```python
def test_reactive_engine_config_exposes_runtime_xfast(tmp_path: Path) -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
        target=zippy.NullPublisher(),
        xfast=True,
    )

    assert engine.config()["xfast"] is True
```

再补一个默认值断言：

```python
def test_stream_table_engine_runtime_xfast_defaults_false() -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=schema,
        target=zippy.NullPublisher(),
    )
    assert engine.config()["xfast"] is False
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest pytests/test_python_api.py -k "runtime_xfast" -v`

Expected: FAIL，提示 Engine 构造器不接受 `xfast` 或 `config()` 中没有该字段。

- [ ] **Step 3: 写最小实现，扩展 `RuntimeOptions`、各类 Engine 构造器和 config dict**

在 `crates/zippy-python/src/lib.rs` 中：

```rust
#[derive(Clone)]
struct RuntimeOptions {
    buffer_capacity: usize,
    overflow_policy: OverflowPolicy,
    archive_buffer_capacity: usize,
    xfast: bool,
}

fn parse_runtime_options(
    buffer_capacity: usize,
    overflow_policy: Option<&Bound<'_, PyAny>>,
    archive_buffer_capacity: usize,
    xfast: bool,
) -> PyResult<RuntimeOptions> {
    // 现有校验保持不变
    Ok(RuntimeOptions {
        buffer_capacity,
        overflow_policy: parse_overflow_policy(overflow_policy)?,
        archive_buffer_capacity,
        xfast,
    })
}
```

把 4 个 Engine 构造器签名都加上 `xfast=false`：

```rust
#[pyo3(signature = (..., buffer_capacity=1024, overflow_policy=None, archive_buffer_capacity=1024, xfast=false))]
```

并在 `spawn_runtime_engine(...)` 构造 `EngineConfig` 时透传：

```rust
let config = EngineConfig {
    name: name.to_string(),
    buffer_capacity: runtime_options.buffer_capacity,
    overflow_policy: runtime_options.overflow_policy,
    late_data_policy: Default::default(),
    xfast: runtime_options.xfast,
};
```

最后在 `engine_base_config_dict(...)` 中回显：

```rust
dict.set_item("xfast", runtime_options.xfast)?;
```

同步更新 `python/zippy/_internal.pyi` 中 4 个 Engine 的 `__init__` 签名。

- [ ] **Step 4: 运行 Python 与 Rust 组合回归**

Run: `uv run pytest pytests/test_python_api.py -k "runtime_xfast or exposes_status_metrics_and_config_lifecycle or timeseries_engine_config_and_output_archive_roundtrip" -v`

Expected: PASS，Python 构造器接受 `xfast`，`config()` 回显正确，旧的 runtime config 断言不回归。

- [ ] **Step 5: 提交 Python 透传链路**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: expose runtime xfast to python engines"
```

### Task 4: 端到端验证 runtime `xfast` 对 live 延迟的影响

**Files:**
- Use: `crates/zippy-core/src/runtime.rs`
- Use: `/home/jiangda/develop/zippy/.worktrees/zippy-openctp-debug-openctp-latency/crates/zippy-openctp-core/examples/live_bus_probe.rs`

- [ ] **Step 1: 跑本地 Rust 回归，确认所有核心路径仍绿**

Run: `cargo test -p zippy-core --test runtime_fast_path --test source_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS。

- [ ] **Step 2: 把新的 `zippy-python` 重编进 LDC venv**

Run: `uv run maturin develop --manifest-path /home/jiangda/develop/zippy/crates/zippy-python/Cargo.toml`

Expected: `Installed zippy-0.1.0`

- [ ] **Step 3: 重启 `ldc-ctp-md`**

Run:

```bash
/bin/bash -lc "cd /home/jiangda/services/local-market-data-center && pkill -f 'uv run ldc-ctp-md' || true && nohup /home/jiangda/.local/bin/uv run ldc-ctp-md > /tmp/ldc-ctp-md.log 2>&1 & echo \$!"
```

Expected: 输出新的进程号。

- [ ] **Step 4: 用 live probe 对比开启 runtime `xfast` 后的稳定窗**

Run:

```bash
timeout 15s /home/jiangda/develop/zippy/.worktrees/zippy-openctp-debug-openctp-latency/target/debug/examples/live_bus_probe \
  --socket-path /home/jiangda/services/local-market-data-center/runtime/zippy-master.sock \
  --stream ldc_ctp_ticks \
  --window-sec 5 \
  --xfast
```

Expected: 输出 2 个以上窗口；忽略重启后的首个抖动窗，稳定窗中重点记录：

- `publisher_dispatch_avg_delay_ms`
- `queue_total_avg_delay_ms`
- `reader_wait_avg_delay_ms`

并与当前基线对比：

- `publisher_dispatch_avg_delay_ms ≈ 0.493 ~ 0.530`
- `queue_total_avg_delay_ms ≈ 0.830 ~ 0.891`

若稳定窗明显更低，则说明 runtime `xfast` 起效。

- [ ] **Step 5: 提交最终实现**

```bash
git add crates/zippy-core/src/types.rs crates/zippy-core/src/runtime.rs crates/zippy-core/tests/runtime_lifecycle.rs crates/zippy-core/tests/runtime_fast_path.rs crates/zippy-core/tests/source_runtime.rs crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "perf: add runtime xfast spin wait"
```
