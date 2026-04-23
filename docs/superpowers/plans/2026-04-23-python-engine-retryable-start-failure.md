# Python Engine Retryable Start Failure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 external source 第一次 `start()` 因 `source.start()` 失败时，不会丢失 Python engine 对象里的底层 engine 所有权，修正外部条件后可对同一个 engine 实例再次调用 `start()`。

**Architecture:** 在 `zippy-core` 里把 external source runtime 启动拆成两阶段：第一阶段只创建 queue/sink 并执行 `source.start()`，把 worker 启动所需上下文封装成 `PreparedSourceRuntime`；第二阶段在 source 启动成功后再把 `engine` 和 `publisher` move 进 worker。`zippy-python` 只在 prepare 成功后执行 `engine.take()`，因此当 prepare 失败时可以保持 Python 对象可重试。

**Tech Stack:** Rust, PyO3, crossbeam-channel, Arrow RecordBatch, pytest, cargo integration tests

---

## File Map

- `crates/zippy-core/src/runtime.rs`
  - 新增两阶段 external source 启动入口与 `PreparedSourceRuntime`
  - 把 `FastDataPath` 调整为支持 worker 延迟注册
  - 保持 `spawn_source_engine_with_publisher(...)` 现有对外语义不变
- `crates/zippy-core/tests/source_runtime.rs`
  - 新增“source.start 同步 emit 事件后再启动 worker”回归测试
  - 覆盖 prepare 阶段与后续 worker 启动后的消费顺序
- `crates/zippy-python/src/lib.rs`
  - `start_runtime_engine()` 改为先 prepare external source，再消费 `engine`
  - 把 external source 三个分支统一收口到一个 helper，避免分支间行为漂移
- `pytests/test_python_api.py`
  - 新增 missing stream 场景下的 retryable start 回归测试
  - 固定“第一次报真实启动错误，第二次不再报 engine already started”的语义

### Task 1: Introduce Prepared Source Runtime in Core

**Files:**
- Modify: `crates/zippy-core/src/runtime.rs`
- Test: `crates/zippy-core/tests/source_runtime.rs`

- [ ] **Step 1: Write the failing Rust regression for deferred worker start**

在 `crates/zippy-core/tests/source_runtime.rs` 里新增一个同步发事件的 source helper 和回归测试。这个测试先调用新的 prepare API，再调用 worker 启动 API；在当前代码里会因为 `prepare_source_runtime(...)` / `spawn_with_publisher(...)` 不存在而编译失败。

```rust
struct SyncStartSource {
    mode: SourceMode,
    schema: Arc<Schema>,
    events: Vec<SourceEvent>,
}

impl Source for SyncStartSource {
    fn name(&self) -> &str {
        "sync-start-source"
    }

    fn output_schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        self.mode
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> Result<SourceHandle> {
        for event in self.events {
            sink.emit(event)?;
        }

        Ok(SourceHandle::new(thread::spawn(|| Ok(()))))
    }
}

#[test]
fn prepared_source_runtime_replays_events_emitted_during_start() {
    let schema = test_schema();
    let calls = Arc::new(Mutex::new(RecordedCalls::default()));
    let prepared = zippy_core::runtime::prepare_source_runtime(
        Box::new(SyncStartSource {
            mode: SourceMode::Pipeline,
            schema: schema.clone(),
            events: vec![
                SourceEvent::Hello(StreamHello::new("bars", schema.clone(), 1).unwrap()),
                SourceEvent::Data(test_batch()),
                SourceEvent::Stop,
            ],
        }),
        test_engine_config("prepared-runtime"),
    )
    .unwrap();

    let engine = RecordingEngine::new(schema, calls.clone());
    let mut handle = prepared.spawn_with_publisher(engine, NoopPublisher).unwrap();

    wait_for_status(&handle, EngineStatus::Stopped);
    assert_eq!(calls.lock().unwrap().data_count, 1);
    assert_eq!(calls.lock().unwrap().stop_count, 1);
    handle.stop().unwrap();
}
```

- [ ] **Step 2: Run the Rust test to verify it fails**

Run: `cargo test -p zippy-core --test source_runtime prepared_source_runtime_replays_events_emitted_during_start -- --nocapture`

Expected: 编译失败，报 `prepare_source_runtime` 或 `spawn_with_publisher` 未定义。

- [ ] **Step 3: Implement two-phase source startup in `runtime.rs`**

先把 fast path 的 worker 句柄改成“可延迟安装”，再把 prepare 阶段从 `spawn_source_engine_with_publisher_inner(...)` 里拆出来。

先调整 `FastDataPath`，避免 `SourceRuntimeSink.emit()` 在 worker 尚未创建时访问裸 `Thread`：

```rust
struct FastDataPath {
    data_queue: Arc<SpscDataQueue<RecordBatch>>,
    worker_thread: Mutex<Option<std::thread::Thread>>,
    worker_running: Arc<AtomicBool>,
    xfast: bool,
    emit_lock: Mutex<()>,
}

impl FastDataPath {
    fn notify_worker(&self) {
        if let Some(worker_thread) = self.worker_thread.lock().unwrap().as_ref() {
            worker_thread.unpark();
        }
    }

    fn install_worker(&self, worker_thread: std::thread::Thread) {
        *self.worker_thread.lock().unwrap() = Some(worker_thread);
        self.worker_running.store(true, Ordering::Release);
    }
}
```

把 `SourceRuntimeSink.emit()` 的 data 分支改成在 push 完后调用 `notify_worker()`，而不是直接依赖 `path.worker_thread`：

```rust
impl SourceSink for SourceRuntimeSink {
    fn emit(&self, event: SourceEvent) -> Result<()> {
        if let Some(path) = &self.fast_data_path {
            let _emit_guard = path.emit_lock.lock().unwrap();
            return match event {
                SourceEvent::Data(batch) => {
                    path.data_queue.push_blocking(batch)?;
                    path.notify_worker();
                    Ok(())
                }
                other => {
                    match &other {
                        SourceEvent::Flush | SourceEvent::Stop | SourceEvent::Error(_) => {
                            wait_for_fast_data_drain(path.as_ref());
                        }
                        _ => {}
                    }
                    enqueue_runtime_command(
                        &self.status,
                        self.overflow_policy,
                        &self.tx,
                        &self.metrics,
                        Command::SourceEvent(other),
                    )
                }
            };
        }

        match event {
            SourceEvent::Data(batch) => enqueue_runtime_command(
                &self.status,
                self.overflow_policy,
                &self.tx,
                &self.metrics,
                Command::SourceEvent(SourceEvent::Data(batch)),
            ),
            other => enqueue_runtime_command(
                &self.status,
                self.overflow_policy,
                &self.tx,
                &self.metrics,
                Command::SourceEvent(other),
            ),
        }
    }
}
```

然后新增 prepare 结构，把现有 `spawn_source_engine_with_publisher_inner(...)` 的前半段拆进去：

```rust
pub struct PreparedSourceRuntime {
    config: EngineConfig,
    engine_name: String,
    source_name: String,
    source_mode: SourceMode,
    source_schema: Arc<Schema>,
    status: Arc<Mutex<EngineStatus>>,
    metrics: Arc<EngineMetrics>,
    tx: QueueSender<Command>,
    rx: crate::queue::QueueReceiver<Command>,
    source_handle: RuntimeSourceHandle,
    fast_data_path: Option<Arc<FastDataPath>>,
    fast_data_queue: Option<Arc<SpscDataQueue<RecordBatch>>>,
}

pub fn prepare_source_runtime<S>(
    source: Box<S>,
    config: EngineConfig,
) -> Result<PreparedSourceRuntime>
where
    S: Source + ?Sized,
{
    config.validate()?;
    let engine_name = config.name.clone();
    let queue = crate::queue::BoundedQueue::new(config.buffer_capacity);
    let status = Arc::new(Mutex::new(EngineStatus::Running));
    let metrics = Arc::new(EngineMetrics::default());
    let rx = queue.receiver();
    let tx = queue.sender();
    let source_mode = source.mode();
    let source_schema = source.output_schema();
    let source_name = source.name().to_string();
    let fast_data_queue =
        if source_mode == SourceMode::Pipeline && config.overflow_policy == OverflowPolicy::Block {
            Some(Arc::new(SpscDataQueue::new(config.buffer_capacity)?))
        } else {
            None
        };
    let fast_data_path = fast_data_queue.clone().map(|data_queue| {
        Arc::new(FastDataPath {
            data_queue,
            worker_thread: Mutex::new(None),
            worker_running: Arc::new(AtomicBool::new(false)),
            xfast: config.xfast,
            emit_lock: Mutex::new(()),
        })
    });

    let sink = Arc::new(SourceRuntimeSink {
        status: status.clone(),
        overflow_policy: config.overflow_policy,
        tx: tx.clone(),
        metrics: metrics.clone(),
        fast_data_path: fast_data_path.clone(),
    });
    let source_handle = source.start(sink)?;

    Ok(PreparedSourceRuntime {
        config,
        engine_name,
        source_name,
        source_mode,
        source_schema,
        status,
        metrics,
        tx,
        rx,
        source_handle,
        fast_data_path,
        fast_data_queue,
    })
}
```

最后把原来的 worker 启动逻辑搬到 `PreparedSourceRuntime::spawn_with_publisher(...)`，并让旧的 `spawn_source_engine_with_publisher(...)` 走同一路径：

```rust
impl PreparedSourceRuntime {
    pub fn spawn_with_publisher<E, P>(self, mut engine: E, mut publisher: P) -> Result<EngineHandle>
    where
        E: Engine,
        P: Publisher,
    {
        let status_clone = self.status.clone();
        let metrics_clone = self.metrics.clone();
        let source_mode = self.source_mode;
        let source_schema = self.source_schema.clone();
        let engine_schema = engine.input_schema();
        let worker_engine_name = self.engine_name.clone();
        let worker_source_name = self.source_name.clone();
        let rx = self.rx;
        let xfast = self.config.xfast;
        let worker_fast_data_queue = self.fast_data_queue.clone();
        let worker_fast_data_path = self.fast_data_path.clone();

        let join_handle = thread::spawn(move || -> Result<()> {
            let mut hello_seen = false;
            match &worker_fast_data_queue {
                Some(data_queue) => loop {
                    if let Some(command) = rx.try_recv() {
                        if handle_source_control_command(
                            &mut engine,
                            &mut publisher,
                            &metrics_clone,
                            &status_clone,
                            source_mode,
                            &source_schema,
                            &engine_schema,
                            worker_engine_name.as_str(),
                            worker_source_name.as_str(),
                            &mut hello_seen,
                            command,
                        )? {
                            return Ok(());
                        }
                        continue;
                    }

                    if let Some(batch) = data_queue.try_pop() {
                        if !hello_seen {
                            return fail_worker(
                                &mut engine,
                                &status_clone,
                                ZippyError::InvalidConfig {
                                    reason: "source hello must arrive before data".to_string(),
                                },
                            );
                        }
                        process_data_event(&mut engine, &mut publisher, &metrics_clone, batch)
                            .inspect_err(|_| {
                                *status_clone.lock().unwrap() = EngineStatus::Failed;
                            })?;
                        continue;
                    }

                    runtime_idle_wait(xfast);
                },
                None => {
                    while let Ok(command) = rx.recv() {
                        if handle_source_control_command(
                            &mut engine,
                            &mut publisher,
                            &metrics_clone,
                            &status_clone,
                            source_mode,
                            &source_schema,
                            &engine_schema,
                            worker_engine_name.as_str(),
                            worker_source_name.as_str(),
                            &mut hello_seen,
                            command,
                        )? {
                            return Ok(());
                        }
                    }
                    Ok(())
                }
            }
        });

        if let Some(path) = &worker_fast_data_path {
            path.install_worker(join_handle.thread().clone());
        }

        let source_handle = self.source_handle.clone();
        let monitor_fast_data_path = self.fast_data_path.clone();
        let monitor_status = self.status.clone();
        let monitor_tx = self.tx.clone();
        let monitor_metrics = self.metrics.clone();
        let source_monitor_handle = thread::spawn(move || {
            let source_result = source_handle.join();
            if source_handle.stop_requested() && source_result.is_ok() {
                return;
            }

            if let Some(path) = &monitor_fast_data_path {
                wait_for_fast_data_queue_drain(
                    path.data_queue.as_ref(),
                    path.worker_running.as_ref(),
                    path.xfast,
                );
            }

            let _ = enqueue_runtime_command(
                &monitor_status,
                OverflowPolicy::Block,
                &monitor_tx,
                &monitor_metrics,
                Command::SourceTerminated(source_result),
            );
        });

        Ok(EngineHandle {
            status: self.status,
            overflow_policy: self.config.overflow_policy,
            tx: self.tx,
            join_handle: Some(join_handle),
            source_handle: Some(self.source_handle),
            source_monitor_handle: Some(source_monitor_handle),
            metrics: self.metrics,
            fast_data_path: self.fast_data_path,
        })
    }
}

pub fn spawn_source_engine_with_publisher<S, E, P>(
    source: Box<S>,
    engine: E,
    config: EngineConfig,
    publisher: P,
) -> Result<EngineHandle>
where
    S: Source + ?Sized,
    E: Engine,
    P: Publisher,
{
    prepare_source_runtime(source, config)?.spawn_with_publisher(engine, publisher)
}
```

- [ ] **Step 4: Run the Rust tests to verify the new startup path works**

Run: `cargo test -p zippy-core --test source_runtime prepared_source_runtime_replays_events_emitted_during_start source_runtime_pipeline_xfast_start_failure_cleans_up_fast_path_worker -- --nocapture`

Expected: `2 passed`

- [ ] **Step 5: Commit the core runtime refactor**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-core/tests/source_runtime.rs
git commit -m "refactor: split source runtime startup into prepare and spawn phases"
```

### Task 2: Make Python Engine Start Retryable After Prepare Failures

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing Python regression**

在 `pytests/test_python_api.py` 里追加一个缺失 bus stream 的回归测试。第一次 `engine.start()` 应返回真实 attach 错误，修复外部条件后第二次 `start()` 必须成功；当前代码会在第二次报 `engine already started`。

```python
def test_bus_source_start_failure_keeps_engine_retryable(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reactive_reader")
    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")

    engine = zippy.ReactiveStateEngine(
        name="reactive_bus_retry",
        source=zippy.BusStreamSource(
            stream_name="ticks",
            expected_schema=tick_schema,
            master=reader_master,
            mode=zippy.SourceMode.PIPELINE,
            xfast=True,
        ),
        input_schema=tick_schema,
        id_column="instrument_id",
        factors=[zippy.Expr(expression="last_price * 2.0", output="price_x2")],
        target=zippy.NullPublisher(),
    )

    try:
        with pytest.raises(RuntimeError, match="stream .* not found"):
            engine.start()

        writer_master.register_stream("ticks", tick_schema, 64, 4096)

        engine.start()
        engine.stop()
    finally:
        if engine.status() == "running":
            engine.stop()
        server.stop()
```

- [ ] **Step 2: Run the Python regression to verify it fails**

Run: `uv run pytest pytests/test_python_api.py -k "bus_source_start_failure_keeps_engine_retryable" -v`

Expected: FAIL，第二次 `engine.start()` 抛 `RuntimeError: engine already started`。

- [ ] **Step 3: Update `start_runtime_engine()` to prepare external sources before `engine.take()`**

在 `crates/zippy-python/src/lib.rs` 里新增一个 Rust helper，把 external source 三个分支统一成“先 prepare、后消费 engine”的流程。关键点是：`prepare_source_runtime(...)` 失败时不能触碰 `engine: &mut Option<E>`。

先补 import：

```rust
use zippy_core::runtime::prepare_source_runtime;
```

再加一个本地 helper，统一 external source 的启动分支：

```rust
fn start_prepared_source_runtime<S, E, P>(
    source: Box<S>,
    config: EngineConfig,
    publisher: P,
    engine: &mut Option<E>,
) -> PyResult<EngineHandle>
where
    S: Source + ?Sized,
    E: Engine,
    P: CorePublisher,
{
    let prepared = prepare_source_runtime(source, config)
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let engine_instance = match engine.take() {
        Some(engine_instance) => engine_instance,
        None => return Err(py_runtime_error("engine already started")),
    };
    prepared
        .spawn_with_publisher(engine_instance, publisher)
        .map_err(|error| py_runtime_error(error.to_string()))
}
```

然后把 `start_runtime_engine()` 里 external source 的三个分支改成先构 source，再调用 `start_prepared_source_runtime(...)`；本地输入分支 `spawn_engine_with_publisher(...)` 保持现状：

```rust
let handle = match (remote_source, bus_source, python_source) {
    (Some(remote_source), None, None) => {
        let source = Box::new(
            RustZmqSource::connect(
                &format!("{name}_source"),
                &remote_source.endpoint,
                remote_source.expected_schema.clone(),
                remote_source.mode,
            )
            .map_err(|error| py_runtime_error(error.to_string()))?,
        );
        start_prepared_source_runtime(source, config.clone(), publisher, engine)
    }
    (None, Some(bus_source), None) => {
        let source = Box::new(BusSourceBridge {
            stream_name: bus_source.stream_name.clone(),
            expected_schema: Arc::clone(&bus_source.expected_schema),
            mode: bus_source.mode,
            master: Arc::clone(&bus_source.master),
            xfast: bus_source.xfast,
        });
        start_prepared_source_runtime(source, config.clone(), publisher, engine)
    }
    (None, None, Some(python_source)) => {
        let source = Box::new(PythonSourceBridge {
            owner: Python::with_gil(|py| python_source.owner.clone_ref(py)),
            name: python_source.name.clone(),
            output_schema: Arc::clone(&python_source.output_schema),
            mode: python_source.mode,
        });
        start_prepared_source_runtime(source, config.clone(), publisher, engine)
    }
    (None, None, None) => {
        let engine_instance = match engine.take() {
            Some(engine_instance) => engine_instance,
            None => return Err(py_runtime_error("engine already started")),
        };
        spawn_engine_with_publisher(engine_instance, config, publisher)
            .map_err(|error| py_runtime_error(error.to_string()))
    }
    _ => Err(py_runtime_error(
        "engine cannot use more than one external source at the same time",
    )),
}?; 
```

- [ ] **Step 4: Run the Python regression and the core source-runtime suite**

Run: `uv run pytest pytests/test_python_api.py -k "bus_source_start_failure_keeps_engine_retryable" -v`
Expected: `1 passed`

Run: `cargo test -p zippy-core --test source_runtime --manifest-path /home/jiangda/develop/zippy/Cargo.toml`
Expected: 全部通过，且不回归已有 `source_runtime_pipeline_xfast_start_failure_cleans_up_fast_path_worker`

- [ ] **Step 5: Commit the Python bridge change**

```bash
git add crates/zippy-python/src/lib.rs pytests/test_python_api.py
git commit -m "fix: allow retrying python engines after source start failure"
```

### Task 3: Full Verification Sweep

**Files:**
- Verify only: `crates/zippy-core/src/runtime.rs`
- Verify only: `crates/zippy-core/tests/source_runtime.rs`
- Verify only: `crates/zippy-python/src/lib.rs`
- Verify only: `pytests/test_python_api.py`

- [ ] **Step 1: Run targeted Rust regression suites**

Run: `cargo test -p zippy-core --test source_runtime --test runtime_fast_path --manifest-path /home/jiangda/develop/zippy/Cargo.toml`

Expected: PASS，确认两阶段启动没有破坏 fast path 与 stop/drain 语义。

- [ ] **Step 2: Run targeted Python regression suites**

Run: `uv run pytest pytests/test_python_api.py -k "bus_source_start_failure_keeps_engine_retryable or runtime_xfast" -v`

Expected: PASS，确认新的 retryable-start 修复不影响之前已落地的 runtime `xfast` 配置回归。

- [ ] **Step 3: Inspect the diff before handoff**

Run: `git diff -- crates/zippy-core/src/runtime.rs crates/zippy-core/tests/source_runtime.rs crates/zippy-python/src/lib.rs pytests/test_python_api.py`

Expected: 只包含两阶段启动、Python 重试语义、以及对应回归测试；不包含与 bus latency/xfast 之外的无关编辑。

- [ ] **Step 4: Create the final verification commit**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-core/tests/source_runtime.rs crates/zippy-python/src/lib.rs pytests/test_python_api.py
git commit -m "test: cover retryable python engine starts for external sources"
```
