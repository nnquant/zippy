# Zippy Master and Bus Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-daemon `zippy-master` with an embedded shared-memory `bus`, plus `MasterClient` / `Writer` / `Reader` APIs and the first end-to-end `StreamTableEngine -> bus -> ReactiveStateEngine` integration.

**Architecture:** `zippy-master` owns the control plane and embeds the bus component. Worker processes talk to `master` over a Unix domain socket through a single per-process `MasterClient`; data movement happens through per-stream shared-memory ring buffers with one writer and multiple independent readers.

**Tech Stack:** Rust, Unix domain socket IPC, shared memory ring buffers, Arrow `RecordBatch`, existing `zippy-core` / `zippy-engines` / `zippy-python` runtime, Python integration tests.

---

## File Map

### New files

- Create: `crates/zippy-master/Cargo.toml`
- Create: `crates/zippy-master/src/main.rs`
- Create: `crates/zippy-master/src/lib.rs`
- Create: `crates/zippy-master/src/server.rs`
- Create: `crates/zippy-master/src/registry.rs`
- Create: `crates/zippy-master/src/bus.rs`
- Create: `crates/zippy-master/src/ring.rs`
- Create: `crates/zippy-master/tests/master_server.rs`
- Create: `crates/zippy-master/tests/bus_ring.rs`
- Create: `crates/zippy-master/tests/master_client_roundtrip.rs`
- Create: `crates/zippy-core/src/master_client.rs`
- Create: `crates/zippy-core/src/bus_protocol.rs`
- Create: `crates/zippy-core/tests/master_client.rs`
- Create: `crates/zippy-python/tests/master_bus_smoke.py` (if repo prefers Python smoke tests colocated elsewhere, use `pytests/test_master_bus.py`)

### Modified files

- Modify: `Cargo.toml`
- Modify: `crates/zippy-core/src/lib.rs`
- Modify: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-engines/src/stream_table.rs`
- Modify: `crates/zippy-engines/tests/stream_table_engine.rs`
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`
- Modify: `plugins/zippy-openctp/examples/md_to_remote_pipeline.py`
- Modify: `plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`
- Modify: `plugins/zippy-openctp/README.md`

### Responsibility boundaries

- `crates/zippy-master/src/registry.rs`: in-memory metadata for process/source/engine/sink/stream records
- `crates/zippy-master/src/ring.rs`: per-stream ring buffer semantics only
- `crates/zippy-master/src/bus.rs`: stream lifecycle + writer/reader descriptor allocation
- `crates/zippy-master/src/server.rs`: Unix socket request/response handling
- `crates/zippy-core/src/bus_protocol.rs`: shared request/response/descriptor types
- `crates/zippy-core/src/master_client.rs`: worker-side control-plane client
- `crates/zippy-python/src/lib.rs`: Python `MasterClient`, `Writer`, `Reader`, plus engine integration hooks

---

### Task 1: Scaffold `zippy-master` crate and protocol types

**Files:**
- Create: `crates/zippy-master/Cargo.toml`
- Create: `crates/zippy-master/src/lib.rs`
- Create: `crates/zippy-master/src/main.rs`
- Create: `crates/zippy-core/src/bus_protocol.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Modify: `Cargo.toml`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing protocol smoke test**

```rust
use zippy_core::bus_protocol::{ControlRequest, ControlResponse, RegisterProcessRequest};

#[test]
fn protocol_types_roundtrip_debug_repr() {
    let request = ControlRequest::RegisterProcess(RegisterProcessRequest {
        app: "local_dc".to_string(),
    });

    let debug = format!("{request:?}");
    assert!(debug.contains("RegisterProcess"));

    let response = ControlResponse::ProcessRegistered {
        process_id: "proc_1".to_string(),
    };
    assert!(format!("{response:?}").contains("proc_1"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test master_server protocol_types_roundtrip_debug_repr -v`  
Expected: FAIL because `zippy-master` crate and protocol module do not exist yet.

- [ ] **Step 3: Add minimal crate and shared protocol definitions**

```rust
// crates/zippy-core/src/bus_protocol.rs
#[derive(Debug, Clone)]
pub struct RegisterProcessRequest {
    pub app: String,
}

#[derive(Debug, Clone)]
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
}

#[derive(Debug, Clone)]
pub enum ControlResponse {
    ProcessRegistered { process_id: String },
    Error { reason: String },
}
```

```rust
// crates/zippy-master/src/lib.rs
pub mod bus;
pub mod registry;
pub mod ring;
pub mod server;
```

```rust
// crates/zippy-master/src/main.rs
fn main() {
    eprintln!("zippy-master bootstrap");
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test master_server protocol_types_roundtrip_debug_repr -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/zippy-core/src/lib.rs crates/zippy-core/src/bus_protocol.rs crates/zippy-master
git commit -m "feat: scaffold zippy master crate and protocol types"
```

### Task 2: Implement registry records and in-memory registry

**Files:**
- Create: `crates/zippy-master/src/registry.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing registry test**

```rust
use zippy_master::registry::Registry;

#[test]
fn registry_stores_process_and_stream_records() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc");
    registry.register_stream("openctp_ticks", 1024);

    assert_eq!(registry.processes_len(), 1);
    assert_eq!(registry.streams_len(), 1);
    assert!(registry.get_process(&process_id).is_some());
    assert!(registry.get_stream("openctp_ticks").is_some());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test master_server registry_stores_process_and_stream_records -v`  
Expected: FAIL because `Registry` does not exist.

- [ ] **Step 3: Implement minimal registry**

```rust
#[derive(Default)]
pub struct Registry {
    processes: BTreeMap<String, ProcessRecord>,
    streams: BTreeMap<String, StreamRecord>,
    next_process_id: u64,
}

impl Registry {
    pub fn register_process(&mut self, app: &str) -> String { /* increment counter */ }
    pub fn register_stream(&mut self, stream_name: &str, ring_capacity: usize) { /* insert */ }
    pub fn processes_len(&self) -> usize { self.processes.len() }
    pub fn streams_len(&self) -> usize { self.streams.len() }
    pub fn get_process(&self, process_id: &str) -> Option<&ProcessRecord> { self.processes.get(process_id) }
    pub fn get_stream(&self, stream_name: &str) -> Option<&StreamRecord> { self.streams.get(stream_name) }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test master_server registry_stores_process_and_stream_records -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/registry.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add master registry records"
```

### Task 3: Implement per-stream ring buffer semantics

**Files:**
- Create: `crates/zippy-master/src/ring.rs`
- Test: `crates/zippy-master/tests/bus_ring.rs`

- [ ] **Step 1: Write the failing ring buffer tests**

```rust
use zippy_master::ring::{ReaderLagged, StreamRing};

#[test]
fn stream_ring_supports_independent_reader_offsets() {
    let mut ring = StreamRing::new(4);
    let reader_a = ring.attach_reader();
    let reader_b = ring.attach_reader();

    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();

    assert_eq!(ring.read(&reader_a).unwrap(), vec![vec![1], vec![2]]);
    assert_eq!(ring.read(&reader_b).unwrap(), vec![vec![1], vec![2]]);
}

#[test]
fn stream_ring_reports_lagged_reader_when_overwritten() {
    let mut ring = StreamRing::new(2);
    let reader = ring.attach_reader();
    ring.write(vec![1]).unwrap();
    ring.write(vec![2]).unwrap();
    ring.write(vec![3]).unwrap();

    let error = ring.read(&reader).unwrap_err();
    assert!(matches!(error, ReaderLagged { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test bus_ring -v`  
Expected: FAIL because `StreamRing` does not exist.

- [ ] **Step 3: Implement minimal in-memory ring semantics**

```rust
pub struct StreamRing {
    capacity: usize,
    write_seq: u64,
    slots: Vec<Option<RingItem>>,
    readers: BTreeMap<String, u64>,
    next_reader_id: u64,
}

impl StreamRing {
    pub fn new(capacity: usize) -> Self { /* ... */ }
    pub fn attach_reader(&mut self) -> String { /* ... */ }
    pub fn write(&mut self, payload: Vec<u8>) -> Result<(), RingWriteError> { /* ... */ }
    pub fn read(&mut self, reader_id: &str) -> Result<Vec<Vec<u8>>, ReaderLagged> { /* ... */ }
    pub fn seek_latest(&mut self, reader_id: &str) -> Result<(), ReaderNotFound> { /* ... */ }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test bus_ring -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/ring.rs crates/zippy-master/tests/bus_ring.rs
git commit -m "feat: add per-stream ring buffer semantics"
```

### Task 4: Implement bus component and `write_to/read_from` descriptors

**Files:**
- Create: `crates/zippy-master/src/bus.rs`
- Modify: `crates/zippy-core/src/bus_protocol.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing bus test**

```rust
use zippy_master::bus::Bus;

#[test]
fn bus_enforces_single_writer_and_multiple_readers() {
    let mut bus = Bus::default();
    bus.create_stream("openctp_ticks", 1024).unwrap();

    let writer = bus.write_to("openctp_ticks", "proc_1").unwrap();
    let _reader_a = bus.read_from("openctp_ticks", "proc_2").unwrap();
    let _reader_b = bus.read_from("openctp_ticks", "proc_3").unwrap();

    assert_eq!(writer.stream_name, "openctp_ticks");
    assert!(bus.write_to("openctp_ticks", "proc_9").is_err());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test master_server bus_enforces_single_writer_and_multiple_readers -v`  
Expected: FAIL because `Bus` and descriptors do not exist.

- [ ] **Step 3: Implement bus and descriptors**

```rust
pub struct WriterDescriptor {
    pub stream_name: String,
    pub writer_id: String,
}

pub struct ReaderDescriptor {
    pub stream_name: String,
    pub reader_id: String,
}

pub struct Bus {
    streams: BTreeMap<String, StreamState>,
}

impl Bus {
    pub fn create_stream(&mut self, stream_name: &str, ring_capacity: usize) -> Result<(), BusError> { /* ... */ }
    pub fn write_to(&mut self, stream_name: &str, process_id: &str) -> Result<WriterDescriptor, BusError> { /* ... */ }
    pub fn read_from(&mut self, stream_name: &str, process_id: &str) -> Result<ReaderDescriptor, BusError> { /* ... */ }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test master_server bus_enforces_single_writer_and_multiple_readers -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/bus.rs crates/zippy-master/src/registry.rs crates/zippy-core/src/bus_protocol.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add bus descriptors and stream attachment"
```

### Task 5: Implement Unix socket server in `zippy-master`

**Files:**
- Create: `crates/zippy-master/src/server.rs`
- Modify: `crates/zippy-master/src/main.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing server integration test**

```rust
#[test]
fn master_server_registers_process_over_unix_socket() {
    let socket_path = tempdir().unwrap().path().join("zippy-master.sock");
    let server = spawn_test_server(&socket_path);

    let response = send_register_process(&socket_path, "local_dc");
    assert_eq!(response.process_id, "proc_1");

    server.shutdown();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test master_server master_server_registers_process_over_unix_socket -v`  
Expected: FAIL because Unix socket server does not exist.

- [ ] **Step 3: Implement minimal line-delimited JSON control server**

```rust
pub fn serve(socket_path: &Path) -> Result<()> {
    // bind UnixListener
    // accept connections
    // decode ControlRequest JSON
    // route into registry/bus
    // encode ControlResponse JSON
}
```

```rust
fn main() -> Result<()> {
    let socket_path = std::env::args().nth(1).unwrap_or("/tmp/zippy-master.sock".to_string());
    zippy_master::server::serve(Path::new(&socket_path))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test master_server master_server_registers_process_over_unix_socket -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/server.rs crates/zippy-master/src/main.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add zippy master unix socket server"
```

### Task 6: Implement worker-side `MasterClient`

**Files:**
- Create: `crates/zippy-core/src/master_client.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Test: `crates/zippy-core/tests/master_client.rs`

- [ ] **Step 1: Write the failing client test**

```rust
use zippy_core::MasterClient;

#[test]
fn master_client_registers_process_and_fetches_writer_descriptor() {
    let mut client = MasterClient::connect("/tmp/test-zippy-master.sock").unwrap();
    let process_id = client.register_process("local_dc").unwrap();
    assert_eq!(process_id, "proc_1");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-core --test master_client -v`  
Expected: FAIL because `MasterClient` does not exist.

- [ ] **Step 3: Implement minimal synchronous client**

```rust
pub struct MasterClient {
    socket_path: PathBuf,
    process_id: Option<String>,
}

impl MasterClient {
    pub fn connect(socket_path: impl Into<PathBuf>) -> Result<Self> { /* ... */ }
    pub fn register_process(&mut self, app: &str) -> Result<String> { /* ... */ }
    pub fn register_stream(&mut self, stream_name: &str, schema: SchemaRef, ring_capacity: usize) -> Result<()> { /* ... */ }
    pub fn write_to(&mut self, stream_name: &str) -> Result<WriterDescriptor> { /* ... */ }
    pub fn read_from(&mut self, stream_name: &str) -> Result<ReaderDescriptor> { /* ... */ }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-core --test master_client -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/master_client.rs crates/zippy-core/src/lib.rs crates/zippy-core/tests/master_client.rs
git commit -m "feat: add master client"
```

### Task 7: Add `Writer` / `Reader` runtime handles

**Files:**
- Modify: `crates/zippy-core/src/master_client.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`

- [ ] **Step 1: Write the failing roundtrip test**

```rust
#[test]
fn writer_and_reader_roundtrip_batches_through_master_bus() {
    let (mut client_a, mut client_b) = test_clients();
    client_a.register_process("writer").unwrap();
    client_b.register_process("reader").unwrap();
    client_a.register_stream("ticks", test_schema(), 64).unwrap();

    let mut writer = client_a.write_to("ticks").unwrap();
    let mut reader = client_b.read_from("ticks").unwrap();

    writer.write(test_batch()).unwrap();
    let received = reader.read(Some(1000)).unwrap();
    assert_eq!(received.num_rows(), test_batch().num_rows());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p zippy-master --test master_client_roundtrip -v`  
Expected: FAIL because handle-backed write/read is not implemented.

- [ ] **Step 3: Implement `Writer` / `Reader`**

```rust
pub struct Writer { /* descriptor + shm mapping */ }
pub struct Reader { /* descriptor + shm mapping + read_seq */ }

impl Writer {
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> { /* encode batch into ring slot */ }
    pub fn flush(&mut self) -> Result<()> { /* metadata flush marker for V1 */ }
    pub fn close(&mut self) -> Result<()> { /* mark closed */ }
}

impl Reader {
    pub fn read(&mut self, timeout_ms: Option<u64>) -> Result<RecordBatch> { /* decode next batch */ }
    pub fn seek_latest(&mut self) -> Result<()> { /* set read_seq = write_seq */ }
    pub fn close(&mut self) -> Result<()> { /* mark detached */ }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p zippy-master --test master_client_roundtrip -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/master_client.rs crates/zippy-master/tests/master_client_roundtrip.rs
git commit -m "feat: add master bus writer and reader handles"
```

### Task 8: Integrate `StreamTableEngine` output with bus writer

**Files:**
- Modify: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-engines/src/stream_table.rs`
- Modify: `crates/zippy-engines/tests/stream_table_engine.rs`
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/__init__.py`
- Modify: `python/zippy/_internal.pyi`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing stream-table-to-bus test**

```python
def test_stream_table_engine_can_publish_to_master_bus(tmp_path: Path) -> None:
    master = zippy.MasterClient(control_endpoint=str(tmp_path / "zippy-master.sock"))
    master.register_process("test")
    master.create_stream("ticks", schema=tick_schema(), ring_capacity=1024)

    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=tick_schema(),
        target=zippy.BusStreamTarget(stream_name="ticks", master=master),
    )
    engine.start()
    engine.write({"instrument_id": ["IF2606"], "dt": [...], "last_price": [10.0]})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest pytests/test_python_api.py -k stream_table_engine_can_publish_to_master_bus -v`  
Expected: FAIL because bus target does not exist.

- [ ] **Step 3: Implement minimal bus target integration**

```rust
enum TargetConfig {
    // existing variants...
    BusStream {
        stream_name: String,
        writer: SharedBusWriter,
    },
}
```

```python
class BusStreamTarget:
    def __init__(self, stream_name: str, master: MasterClient): ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest pytests/test_python_api.py -k stream_table_engine_can_publish_to_master_bus -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-engines/src/stream_table.rs crates/zippy-engines/tests/stream_table_engine.rs crates/zippy-python/src/lib.rs python/zippy/__init__.py python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: publish stream table output to master bus"
```

### Task 9: Integrate bus reader as `ReactiveStateEngine` input source

**Files:**
- Modify: `crates/zippy-core/src/runtime.rs`
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Modify: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing bus-to-reactive test**

```python
def test_reactive_engine_can_consume_master_bus_stream(tmp_path: Path) -> None:
    master = boot_test_master(tmp_path)
    master.create_stream("ticks", schema=tick_schema(), ring_capacity=1024)
    write_tick_to_bus(master, "ticks")

    engine = zippy.ReactiveStateEngine(
        name="reactive_bus",
        input_schema=tick_schema(),
        id_column="instrument_id",
        factors=[zippy.Expr(expression="last_price * 2.0", output="price_x2")],
        source=zippy.BusStreamSource(stream_name="ticks", master=master),
        target=zippy.NullPublisher(),
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest pytests/test_python_api.py -k reactive_engine_can_consume_master_bus_stream -v`  
Expected: FAIL because bus source does not exist.

- [ ] **Step 3: Implement minimal bus source integration**

```rust
struct BusStreamSourceConfig {
    stream_name: String,
    reader: SharedBusReader,
}
```

```python
class BusStreamSource:
    def __init__(self, stream_name: str, master: MasterClient): ...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest pytests/test_python_api.py -k reactive_engine_can_consume_master_bus_stream -v`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/runtime.rs crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: add bus source for reactive engine"
```

### Task 10: Migrate `zippy-openctp` examples to `master/bus`

**Files:**
- Modify: `plugins/zippy-openctp/examples/md_to_remote_pipeline.py`
- Modify: `plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`
- Modify: `plugins/zippy-openctp/README.md`

- [ ] **Step 1: Write the failing example smoke expectation**

```python
def test_openctp_examples_compile_with_master_bus_paths() -> None:
    ...
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run python -m py_compile plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`  
Expected: FAIL because examples still reference remote ZMQ-only flow.

- [ ] **Step 3: Update examples to use master/bus**

```python
master = zippy.MasterClient(control_endpoint=args.control_endpoint)
writer_target = zippy.BusStreamTarget(stream_name="openctp_ticks", master=master)
reader_source = zippy.BusStreamSource(stream_name="openctp_ticks", master=master)
```

- [ ] **Step 4: Run verification**

Run: `uv run python -m py_compile plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py plugins/zippy-openctp/README.md
git commit -m "docs: align openctp examples with master bus"
```

### Task 11: Final verification and cleanup

**Files:**
- Modify only as needed from prior tasks

- [ ] **Step 1: Run Rust verification**

Run:

```bash
cargo test -p zippy-master --test master_server --test bus_ring --test master_client_roundtrip -v
cargo test -p zippy-core --test master_client -v
cargo test -p zippy-engines --test stream_table_engine -v
cargo clippy -p zippy-master -p zippy-core -p zippy-engines -p zippy-python --all-targets -- -D warnings
```

Expected: PASS

- [ ] **Step 2: Run Python verification**

Run:

```bash
uv run pytest pytests/test_python_api.py -k 'master_bus or stream_table or reactive_engine_can_consume_master_bus_stream' -v
uv run python -m py_compile plugins/zippy-openctp/examples/md_to_remote_pipeline.py plugins/zippy-openctp/examples/remote_mid_price_diff_200_std_200.py
```

Expected: PASS

- [ ] **Step 3: Commit the final verification fixes**

```bash
git add crates/zippy-master crates/zippy-core crates/zippy-engines crates/zippy-python python/zippy pytests plugins/zippy-openctp
git commit -m "feat: add zippy master and shared-memory bus v1"
```

---

## Self-Review

- **Spec coverage:** covered daemonized `zippy-master`, embedded `bus`, Unix socket control plane, per-stream ring, single writer / multiple readers, `MasterClient`, `Writer/Reader`, `StreamTableEngine -> bus`, and `bus -> ReactiveStateEngine`.
- **Placeholder scan:** no `TODO`/`TBD`; each task includes concrete files, code sketches, and commands.
- **Type consistency:** object names are kept stable across tasks: `MasterClient`, `Writer`, `Reader`, `Bus`, `StreamRing`, `BusStreamTarget`, `BusStreamSource`.

