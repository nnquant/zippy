# Master Bus SHM Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `zippy-master` 数据面从 `seq_*.ipc` 文件批次彻底迁移到真正的 shm lock-free frame ring，并保持 `write_to/read_from` 主用法不变。

**Architecture:** 保留 `zippy-master + MasterClient` 控制面，彻底替换 `Writer/Reader` 的文件热路径为单写者、多读者、每 reader 独立游标的共享内存 frame ring。控制面字段同步从 `ring_capacity` 迁移到 `buffer_size/frame_size`，并回归 `StreamTableEngine -> bus -> ReactiveStateEngine` 主链。

**Tech Stack:** Rust, atomics, Unix domain socket, PyO3, Arrow IPC, cargo test, cargo clippy, pytest

---

## File Map

- Modify: `crates/zippy-core/src/bus_protocol.rs`
  - 更新控制面协议与 descriptor，使用 `buffer_size/frame_size`。
- Modify: `crates/zippy-core/src/master_client.rs`
  - 用 shm frame ring 重写 `Writer/Reader` 热路径，移除 `seq_*.ipc` 文件逻辑。
- Modify: `crates/zippy-master/src/ring.rs`
  - 实现真正的 shm frame ring 内存布局、原子 frame header 和 lock-free 读写语义。
- Modify: `crates/zippy-master/src/bus.rs`
  - 以 shm ring 作为 stream 数据面，移除目录/文件批次逻辑。
- Modify: `crates/zippy-master/src/registry.rs`
  - 将 stream 配置字段从 `ring_capacity` 对齐到 `buffer_size/frame_size`。
- Modify: `crates/zippy-master/src/server.rs`
  - 控制面 `register_stream/write_to/read_from/list/get` 对齐新字段。
- Modify: `crates/zippy-master/src/snapshot.rs`
  - snapshot 中 stream 字段迁移到 `buffer_size/frame_size`。
- Modify: `crates/zippy-master/src/main.rs`
  - 如有需要，对默认 stream 展示字段做轻微对齐，不改行为边界。
- Modify: `crates/zippy-master/tests/master_server.rs`
  - 控制面 register/list/show/restore 迁移测试。
- Modify: `crates/zippy-master/tests/master_client_roundtrip.rs`
  - reader 从最新开始、lagged、writer 重连、frame 超限等回归。
- Modify: `crates/zippy-core/tests/master_client.rs`
  - 协议和客户端 roundtrip 对齐 `buffer_size/frame_size`。
- Modify: `crates/zippy-python/src/lib.rs`
  - Python `MasterClient` / stream info 绑定对齐新字段。
- Modify: `python/zippy/_internal.pyi`
  - 类型声明更新。
- Modify: `python/zippy/cli_stream.py`
  - `stream ls/show` 展示 `buffer_size/frame_size`。
- Modify: `pytests/test_python_api.py`
  - Python 侧 master client / bus 主链回归。
- Modify: `pytests/test_python_cli.py`
  - CLI 输出字段回归。
- Modify: `examples/python/README.md`
  - 文档字段与运行说明更新。
- Modify: `plugins/zippy-openctp/examples/md_to_remote_pipeline.py`
  - 如果显式创建 stream 配置，则改为 `buffer_size/frame_size`。
- Modify: `plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`
  - 同上。
- Modify: `plugins/zippy-openctp/examples/subscribe_mid_price_factors.py`
  - 如需要展示 stream info，对齐新字段。
- Modify: `plugins/zippy-openctp/README.md`
  - 文档字段同步。

### Task 1: 定义新的 stream 配置与协议字段

**Files:**
- Modify: `crates/zippy-core/src/bus_protocol.rs`
- Test: `crates/zippy-core/tests/master_client.rs`

- [ ] **Step 1: 写出协议字段迁移的失败测试**

```rust
#[test]
fn stream_info_uses_buffer_and_frame_sizes() {
    use zippy_core::bus_protocol::StreamInfo;

    let info = StreamInfo {
        stream_name: "ticks".to_string(),
        buffer_size: 1024,
        frame_size: 65536,
        writer_process_id: None,
        reader_count: 0,
        write_seq: 0,
        status: "running".to_string(),
    };

    let encoded = serde_json::to_string(&info).unwrap();
    assert!(encoded.contains("buffer_size"));
    assert!(encoded.contains("frame_size"));
    assert!(!encoded.contains("ring_capacity"));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-core --test master_client stream_info_uses_buffer_and_frame_sizes -v -- --exact`
Expected: FAIL，提示 `StreamInfo` 仍缺少 `buffer_size/frame_size` 或仍使用 `ring_capacity`。

- [ ] **Step 3: 最小修改 bus protocol**

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegisterStreamRequest {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub write_seq: u64,
    pub status: String,
}
```

- [ ] **Step 4: 运行协议测试确认通过**

Run: `cargo test -p zippy-core --test master_client stream_info_uses_buffer_and_frame_sizes -v -- --exact`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/zippy-core/src/bus_protocol.rs crates/zippy-core/tests/master_client.rs
git commit -m "refactor: rename master stream sizing fields"
```

### Task 2: 为 shm frame ring 写底层失败测试

**Files:**
- Modify: `crates/zippy-master/src/ring.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`

- [ ] **Step 1: 写 reader 从最新开始与 lagged 的失败测试**

```rust
#[test]
fn frame_ring_new_reader_starts_after_current_write_seq() {
    let mut ring = StreamRing::new(4, 1024).unwrap();
    ring.publish_for_test(1, b"a").unwrap();
    ring.publish_for_test(2, b"b").unwrap();

    let attachment = ring.attach_reader_at_latest();
    assert_eq!(attachment.next_read_seq, 3);
}

#[test]
fn frame_ring_reports_lagged_reader_after_overwrite() {
    let mut ring = StreamRing::new(2, 1024).unwrap();
    ring.publish_for_test(1, b"a").unwrap();
    ring.publish_for_test(2, b"b").unwrap();
    ring.publish_for_test(3, b"c").unwrap();

    let error = ring.read_for_test(1).unwrap_err();
    assert!(error.to_string().contains("lagged"));
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-master --test master_client_roundtrip frame_ring_new_reader_starts_after_current_write_seq -v -- --exact`
Expected: FAIL，说明 `StreamRing` 还不具备新 API 或语义不对。

- [ ] **Step 3: 在 `ring.rs` 定义新 frame ring 结构**

```rust
pub struct StreamRing {
    buffer_size: usize,
    frame_size: usize,
    write_seq: AtomicU64,
    // frame headers + payload mapping fields
}

pub struct ReaderAttachment {
    pub reader_id: String,
    pub next_read_seq: u64,
}
```

- [ ] **Step 4: 实现最小测试辅助能力**

```rust
impl StreamRing {
    pub fn new(buffer_size: usize, frame_size: usize) -> Result<Self, RingConfigError> {
        // validate and allocate shm-backed layout
        # unimplemented!()
    }

    pub fn attach_reader_at_latest(&mut self) -> ReaderAttachment {
        // next_read_seq = current_write_seq + 1
        # unimplemented!()
    }
}
```

- [ ] **Step 5: 运行底层测试确认通过**

Run: `cargo test -p zippy-master --test master_client_roundtrip frame_ring_new_reader_starts_after_current_write_seq -v -- --exact`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add crates/zippy-master/src/ring.rs crates/zippy-master/tests/master_client_roundtrip.rs
git commit -m "feat: add shm frame ring primitives"
```

### Task 3: 用 shm frame ring 替换 bus 数据面

**Files:**
- Modify: `crates/zippy-master/src/bus.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`

- [ ] **Step 1: 写 bus descriptor 的失败测试**

```rust
#[test]
fn bus_read_from_returns_latest_next_read_seq() {
    let mut bus = Bus::default();
    bus.ensure_stream("ticks", 4, 1024).unwrap();

    let writer = bus.write_to("ticks", "proc_1").unwrap();
    assert_eq!(writer.next_write_seq, 1);

    bus.publish_test_frame("ticks", 1, b"abc").unwrap();
    let reader = bus.read_from("ticks", "proc_2").unwrap();
    assert_eq!(reader.next_read_seq, 2);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-master --test master_client_roundtrip bus_read_from_returns_latest_next_read_seq -v -- --exact`
Expected: FAIL，说明 `Bus` 仍依赖旧文件序号或旧签名。

- [ ] **Step 3: 更新 bus 配置与 stream state**

```rust
struct StreamState {
    ring: StreamRing,
    writer_process_id: Option<String>,
    writer_id: Option<String>,
    shm_name: String,
    layout_version: u32,
    buffer_size: usize,
    frame_size: usize,
}
```

- [ ] **Step 4: 删除旧文件序号逻辑并接入 ring**

```rust
pub fn write_to(&mut self, stream_name: &str, process_id: &str) -> Result<WriterDescriptor, BusError> {
    let stream = self.streams.get_mut(stream_name).ok_or(...)?;
    let next_write_seq = stream.ring.next_write_seq();
    Ok(WriterDescriptor {
        stream_name: stream_name.to_string(),
        buffer_size: stream.buffer_size,
        frame_size: stream.frame_size,
        shm_name: stream.shm_name.clone(),
        next_write_seq,
        ..
    })
}
```

- [ ] **Step 5: 运行 bus roundtrip 测试确认通过**

Run: `cargo test -p zippy-master --test master_client_roundtrip -v`
Expected: PASS，至少覆盖新 reader 从最新开始、writer 重连、reader lagged。

- [ ] **Step 6: 提交**

```bash
git add crates/zippy-master/src/bus.rs crates/zippy-master/tests/master_client_roundtrip.rs
git commit -m "feat: replace master bus file storage with shm ring"
```

### Task 4: 用 shm ring 重写 MasterClient Writer/Reader 热路径

**Files:**
- Modify: `crates/zippy-core/src/master_client.rs`
- Test: `crates/zippy-core/tests/master_client.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`

- [ ] **Step 1: 写文件热路径移除的失败测试**

```rust
#[test]
fn writer_and_reader_use_shm_descriptors_without_seq_files() {
    let descriptor = WriterDescriptor {
        stream_name: "ticks".to_string(),
        buffer_size: 4,
        frame_size: 1024,
        layout_version: 1,
        shm_name: "/tmp/test-shm".to_string(),
        writer_id: "ticks_writer".to_string(),
        process_id: "proc_1".to_string(),
        next_write_seq: 1,
    };

    assert_eq!(descriptor.frame_size, 1024);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-core --test master_client writer_and_reader_use_shm_descriptors_without_seq_files -v -- --exact`
Expected: FAIL，descriptor 仍是旧字段或客户端仍引用文件路径语义。

- [ ] **Step 3: 删除 `fs::write/read_dir` 数据面路径**

```rust
impl Writer {
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let payload = encode_batch(&batch)?;
        self.ring_view.write_frame(self.next_write_seq, &payload)?;
        self.next_write_seq += 1;
        Ok(())
    }
}

impl Reader {
    pub fn read(&mut self, timeout_ms: Option<u64>) -> Result<RecordBatch> {
        let payload = self.ring_view.read_frame(self.next_read_seq, timeout_ms)?;
        let batch = decode_batch(&payload)?;
        self.next_read_seq += 1;
        Ok(batch)
    }
}
```

- [ ] **Step 4: 移除旧辅助函数**

```rust
// remove:
// seq_file_name
// list_available_sequences
// prune_old_batch_files
```

- [ ] **Step 5: 运行 roundtrip 测试确认通过**

Run: `cargo test -p zippy-core --test master_client -v && cargo test -p zippy-master --test master_client_roundtrip -v`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add crates/zippy-core/src/master_client.rs crates/zippy-core/tests/master_client.rs crates/zippy-master/tests/master_client_roundtrip.rs
git commit -m "feat: migrate master client bus io to shm frames"
```

### Task 5: 对齐 registry、snapshot 和 server 控制面字段

**Files:**
- Modify: `crates/zippy-master/src/registry.rs`
- Modify: `crates/zippy-master/src/snapshot.rs`
- Modify: `crates/zippy-master/src/server.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: 写控制面字段迁移失败测试**

```rust
#[test]
fn master_server_lists_streams_with_buffer_and_frame_sizes() {
    let stream = fetch_stream_info_from_server("ticks");
    assert_eq!(stream.buffer_size, 1024);
    assert_eq!(stream.frame_size, 65536);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `cargo test -p zippy-master --test master_server master_server_lists_streams_with_buffer_and_frame_sizes -v -- --exact`
Expected: FAIL，说明 server/registry/snapshot 仍使用 `ring_capacity`。

- [ ] **Step 3: 更新 registry 与 snapshot 数据结构**

```rust
pub struct StreamRecord {
    pub stream_name: String,
    pub buffer_size: usize,
    pub frame_size: usize,
    // ...
}
```

- [ ] **Step 4: 更新 server 请求处理与 stream info 输出**

```rust
ControlRequest::RegisterStream(request) => {
    bus.ensure_stream(&request.stream_name, request.buffer_size, request.frame_size)?;
    registry.ensure_stream(&request.stream_name, request.buffer_size, request.frame_size)?;
}
```

- [ ] **Step 5: 运行 master_server 测试确认通过**

Run: `cargo test -p zippy-master --test master_server -v`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add crates/zippy-master/src/registry.rs crates/zippy-master/src/snapshot.rs crates/zippy-master/src/server.rs crates/zippy-master/tests/master_server.rs
git commit -m "refactor: align master control plane with frame ring sizing"
```

### Task 6: 更新 Python 绑定和 CLI

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `python/zippy/cli_stream.py`
- Test: `pytests/test_python_api.py`
- Test: `pytests/test_python_cli.py`

- [ ] **Step 1: 写 Python stream info 字段失败测试**

```python
def test_master_client_stream_info_uses_buffer_and_frame_size(master_client):
    info = master_client.get_stream("ticks")
    assert info["buffer_size"] == 1024
    assert info["frame_size"] == 65536
    assert "ring_capacity" not in info
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest pytests/test_python_api.py -k stream_info_uses_buffer_and_frame_size -v`
Expected: FAIL

- [ ] **Step 3: 更新 PyO3 绑定和类型声明**

```rust
#[pyclass]
pub struct PyStreamInfo {
    #[pyo3(get)]
    pub stream_name: String,
    #[pyo3(get)]
    pub buffer_size: usize,
    #[pyo3(get)]
    pub frame_size: usize,
}
```

- [ ] **Step 4: 更新 CLI 输出**

```python
TABLE_COLUMNS = ["stream_name", "buffer_size", "frame_size", "reader_count", "status"]
```

- [ ] **Step 5: 运行 Python 测试确认通过**

Run: `uv run pytest pytests/test_python_api.py -k stream_info_uses_buffer_and_frame_size -v && uv run pytest pytests/test_python_cli.py -v`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi python/zippy/cli_stream.py pytests/test_python_api.py pytests/test_python_cli.py
git commit -m "feat: expose master frame ring stream info to python and cli"
```

### Task 7: 回归主链与文档对齐

**Files:**
- Modify: `examples/python/README.md`
- Modify: `plugins/zippy-openctp/examples/md_to_remote_pipeline.py`
- Modify: `plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`
- Modify: `plugins/zippy-openctp/examples/subscribe_mid_price_factors.py`
- Modify: `plugins/zippy-openctp/README.md`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: 写主链回归测试**

```python
def test_stream_table_and_reactive_chain_work_over_master_frame_ring(...):
    # register stream with buffer_size/frame_size
    # publish from stream table
    # consume with reactive engine
    # assert downstream batch received
    ...
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest pytests/test_python_api.py -k 'stream_table_engine_can_publish_to_master_bus_direct_reader or reactive_engine_can_consume_master_bus_stream_and_publish_to_bus' -v`
Expected: FAIL，至少有一处字段或数据面不兼容。

- [ ] **Step 3: 更新 examples 与 README**

```python
client.register_stream(
    stream_name="openctp_ticks",
    schema=schema,
    buffer_size=131072,
    frame_size=65536,
)
```

- [ ] **Step 4: 运行端到端与 smoke 验证**

Run: `uv run pytest pytests/test_python_api.py -k 'stream_table_engine_can_publish_to_master_bus_direct_reader or reactive_engine_can_consume_master_bus_stream_and_publish_to_bus' -v`
Expected: PASS

- [ ] **Step 5: 运行最终验证**

Run: `cargo test -p zippy-core --test master_client -v && cargo test -p zippy-master --test master_client_roundtrip -v && cargo test -p zippy-master --test master_server -v && cargo clippy -p zippy-core -p zippy-master -p zippy-python --all-targets -- -D warnings && uv run pytest pytests/test_python_api.py -v && uv run pytest pytests/test_python_cli.py -v`
Expected: 全部 PASS

- [ ] **Step 6: 提交**

```bash
git add examples/python/README.md plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py plugins/zippy-openctp/examples/subscribe_mid_price_factors.py plugins/zippy-openctp/README.md pytests/test_python_api.py
git commit -m "docs: align master bus examples with shm frame ring"
```
