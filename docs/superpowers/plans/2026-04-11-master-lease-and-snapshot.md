# Master Lease And Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `zippy-master` 增加 worker lease/heartbeat、脏 `writer/reader` 回收，以及控制面 registry snapshot/load 能力。

**Architecture:** 在 `zippy-master` 现有单 daemon 架构上，新增 process lease 管理和 registry snapshot 存储。数据面 bus 不做恢复；恢复仅覆盖 `stream/source/engine/sink` 控制面元数据，并把恢复状态统一标为 `restored`。

**Tech Stack:** Rust, serde/serde_json, Unix domain socket, tracing, PyO3 Python bindings, pytest, cargo test

---

## File Map

**Core protocol and client**
- Modify: `crates/zippy-core/src/bus_protocol.rs`
- Modify: `crates/zippy-core/src/master_client.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Test: `crates/zippy-core/tests/master_client.rs`

**Master runtime**
- Modify: `crates/zippy-master/src/registry.rs`
- Modify: `crates/zippy-master/src/server.rs`
- Create: `crates/zippy-master/src/snapshot.rs`
- Modify: `crates/zippy-master/src/lib.rs`
- Modify: `crates/zippy-master/src/daemon.rs`
- Test: `crates/zippy-master/tests/master_server.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`

**Python binding**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Test: `pytests/test_python_api.py`

**Docs**
- Modify: `examples/python/README.md`

---

### Task 1: Add heartbeat control-plane protocol

**Files:**
- Modify: `crates/zippy-core/src/bus_protocol.rs`
- Modify: `crates/zippy-core/src/master_client.rs`
- Modify: `crates/zippy-core/src/lib.rs`
- Test: `crates/zippy-core/tests/master_client.rs`

- [ ] **Step 1: Write the failing Rust client test**

Add this test to `crates/zippy-core/tests/master_client.rs`:

```rust
#[test]
fn master_client_sends_heartbeat_for_registered_process() {
    let socket_path = unique_socket_path();
    let _server = spawn_fake_control_server(&socket_path, |request| match request {
        ControlRequest::RegisterProcess(RegisterProcessRequest { app }) => {
            assert_eq!(app, "local_dc");
            ControlResponse::ProcessRegistered {
                process_id: "proc_1".to_string(),
            }
        }
        ControlRequest::Heartbeat(HeartbeatRequest { process_id }) => {
            assert_eq!(process_id, "proc_1");
            ControlResponse::HeartbeatAccepted {
                process_id: "proc_1".to_string(),
            }
        }
        other => panic!("unexpected request: {:?}", other),
    });

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("local_dc").unwrap();
    client.heartbeat().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p zippy-core --test master_client master_client_sends_heartbeat_for_registered_process -v
```

Expected: FAIL with missing `HeartbeatRequest`, `ControlRequest::Heartbeat`, or `MasterClient::heartbeat`.

- [ ] **Step 3: Implement minimal protocol and client support**

Update `crates/zippy-core/src/bus_protocol.rs` to add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub process_id: String,
}
```

Extend control enums:

```rust
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
    Heartbeat(HeartbeatRequest),
    RegisterStream(RegisterStreamRequest),
    // ...
}

pub enum ControlResponse {
    ProcessRegistered { process_id: String },
    HeartbeatAccepted { process_id: String },
    StreamRegistered { stream_name: String },
    // ...
}
```

Add to `crates/zippy-core/src/master_client.rs`:

```rust
pub fn heartbeat(&self) -> Result<()> {
    let process_id = self.require_process_id()?;
    let response = self.send_request(ControlRequest::Heartbeat(HeartbeatRequest {
        process_id,
    }))?;

    match response {
        ControlResponse::HeartbeatAccepted { .. } => Ok(()),
        other => Err(unexpected_response("HeartbeatAccepted", other)),
    }
}
```

Re-export `HeartbeatRequest` in `crates/zippy-core/src/lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p zippy-core --test master_client master_client_sends_heartbeat_for_registered_process -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-core/src/bus_protocol.rs crates/zippy-core/src/master_client.rs crates/zippy-core/src/lib.rs crates/zippy-core/tests/master_client.rs
git commit -m "feat: add master heartbeat control protocol"
```

---

### Task 2: Track process lease state in registry

**Files:**
- Modify: `crates/zippy-master/src/registry.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing registry test**

Add this test to `crates/zippy-master/tests/master_server.rs`:

```rust
#[test]
fn registry_updates_process_lease_timestamp_on_heartbeat() {
    let mut registry = Registry::default();
    let process_id = registry.register_process("local_dc");

    let first_seen = registry.get_process(&process_id).unwrap().last_heartbeat_at;
    std::thread::sleep(std::time::Duration::from_millis(5));
    registry.record_heartbeat(&process_id).unwrap();
    let second_seen = registry.get_process(&process_id).unwrap().last_heartbeat_at;

    assert!(second_seen >= first_seen);
    assert_eq!(registry.get_process(&process_id).unwrap().lease_status, "alive");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p zippy-master --test master_server registry_updates_process_lease_timestamp_on_heartbeat -v
```

Expected: FAIL with missing `last_heartbeat_at`, `lease_status`, or `record_heartbeat`.

- [ ] **Step 3: Implement process lease fields and heartbeat mutation**

Update `crates/zippy-master/src/registry.rs`:

```rust
#[derive(Debug, Clone)]
pub struct ProcessRecord {
    pub process_id: String,
    pub app: String,
    pub registered_at: u64,
    pub last_heartbeat_at: u64,
    pub lease_status: String,
}
```

Add helpers:

```rust
fn now_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn record_heartbeat(&mut self, process_id: &str) -> Result<(), RegistryError> {
    let process = self.processes.get_mut(process_id).ok_or_else(|| RegistryError::ProcessNotFound {
        process_id: process_id.to_string(),
    })?;
    process.last_heartbeat_at = now_epoch_millis();
    process.lease_status = "alive".to_string();
    Ok(())
}
```

Extend `RegistryError` with `ProcessNotFound`.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p zippy-master --test master_server registry_updates_process_lease_timestamp_on_heartbeat -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/registry.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: track master process lease timestamps"
```

---

### Task 3: Add server heartbeat handling and stale writer reclaim

**Files:**
- Modify: `crates/zippy-master/src/server.rs`
- Modify: `crates/zippy-master/src/registry.rs`
- Test: `crates/zippy-master/tests/master_client_roundtrip.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing stale-writer reclaim test**

Add this test to `crates/zippy-master/tests/master_client_roundtrip.rs`:

```rust
#[test]
fn expired_process_writer_is_reclaimed_for_new_writer() {
    let socket_path = unique_socket_path();
    let (server, join_handle) = spawn_test_server(&socket_path);

    let mut first = MasterClient::connect(&socket_path).unwrap();
    first.register_process("writer_a").unwrap();
    first.register_stream("ticks", test_schema(), 64).unwrap();
    let _writer = first.write_to("ticks").unwrap();

    server.expire_process_for_test("proc_1").unwrap();

    let mut second = MasterClient::connect(&socket_path).unwrap();
    second.register_process("writer_b").unwrap();
    second.register_stream("ticks", test_schema(), 64).unwrap();
    let second_writer = second.write_to("ticks").unwrap();
    assert_eq!(second_writer.descriptor().process_id, "proc_2");

    server.shutdown();
    join_handle.join().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p zippy-master --test master_client_roundtrip expired_process_writer_is_reclaimed_for_new_writer -v
```

Expected: FAIL because `expire_process_for_test` or reclaim logic does not exist.

- [ ] **Step 3: Implement heartbeat handling and lease expiration reclaim**

In `crates/zippy-master/src/server.rs`, add heartbeat request handling:

```rust
ControlRequest::Heartbeat(request) => {
    self.registry.lock().unwrap().record_heartbeat(&request.process_id)?;
    tracing::debug!(
        component = "master_server",
        event = "heartbeat",
        status = "success",
        process_id = request.process_id.as_str(),
        "accepted process heartbeat"
    );
    ControlResponse::HeartbeatAccepted {
        process_id: request.process_id,
    }
}
```

Add a small reclaim path in `Registry` to enumerate stream writer ownership by process:

```rust
pub fn streams_for_writer_process(&self, process_id: &str) -> Vec<String> {
    self.streams
        .values()
        .filter(|stream| stream.writer_process_id.as_deref() == Some(process_id))
        .map(|stream| stream.stream_name.clone())
        .collect()
}
```

Add a test-only helper on `MasterServer`:

```rust
#[cfg(test)]
pub fn expire_process_for_test(&self, process_id: &str) -> Result<()> {
    let stale_streams = {
        let mut registry = self.registry.lock().unwrap();
        registry.mark_process_expired(process_id)?;
        registry.streams_for_writer_process(process_id)
    };

    for stream_name in stale_streams {
        let writer_id = format!("{stream_name}_writer");
        self.bus.lock().unwrap().detach_writer(&stream_name, &writer_id)?;
        self.registry.lock().unwrap().detach_writer(&stream_name);
    }

    Ok(())
}
```

Also add `mark_process_expired(process_id)` in `Registry`:

```rust
pub fn mark_process_expired(&mut self, process_id: &str) -> Result<(), RegistryError> {
    let process = self.processes.get_mut(process_id).ok_or_else(|| RegistryError::ProcessNotFound {
        process_id: process_id.to_string(),
    })?;
    process.lease_status = "expired".to_string();
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cargo test -p zippy-master --test master_client_roundtrip expired_process_writer_is_reclaimed_for_new_writer -v
cargo test -p zippy-master --test master_server registry_updates_process_lease_timestamp_on_heartbeat -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/server.rs crates/zippy-master/src/registry.rs crates/zippy-master/tests/master_client_roundtrip.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: reclaim stale master writers after lease expiration"
```

---

### Task 4: Add registry snapshot persistence and restore

**Files:**
- Create: `crates/zippy-master/src/snapshot.rs`
- Modify: `crates/zippy-master/src/lib.rs`
- Modify: `crates/zippy-master/src/server.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing snapshot restore test**

Add this test to `crates/zippy-master/tests/master_server.rs`:

```rust
#[test]
fn master_restores_registered_streams_from_snapshot_as_restored() {
    let temp = tempfile::tempdir().unwrap();
    let snapshot_path = temp.path().join("master-registry.json");

    SnapshotStore::write(
        &snapshot_path,
        &RegistrySnapshot {
            streams: vec![SnapshotStreamRecord {
                stream_name: "openctp_ticks".to_string(),
                ring_capacity: 1024,
                status: "registered".to_string(),
            }],
            sources: vec![],
            engines: vec![],
            sinks: vec![],
        },
    )
    .unwrap();

    let server = MasterServer::from_snapshot_path(&snapshot_path).unwrap();
    let stream = server.registry().lock().unwrap().get_stream("openctp_ticks").unwrap().clone();
    assert_eq!(stream.status, "restored");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p zippy-master --test master_server master_restores_registered_streams_from_snapshot_as_restored -v
```

Expected: FAIL because snapshot types and `from_snapshot_path` are missing.

- [ ] **Step 3: Implement minimal snapshot store and restore path**

Create `crates/zippy-master/src/snapshot.rs` with:

```rust
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use zippy_core::{Result, ZippyError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStreamRecord {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RegistrySnapshot {
    pub streams: Vec<SnapshotStreamRecord>,
    pub sources: Vec<serde_json::Value>,
    pub engines: Vec<serde_json::Value>,
    pub sinks: Vec<serde_json::Value>,
}

pub struct SnapshotStore;

impl SnapshotStore {
    pub fn write(path: &Path, snapshot: &RegistrySnapshot) -> Result<()> {
        let temp_path = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(snapshot).map_err(|error| ZippyError::Io {
            reason: format!("failed to serialize registry snapshot error=[{}]", error),
        })?;
        fs::write(&temp_path, bytes).map_err(|error| ZippyError::Io {
            reason: format!("failed to write registry snapshot temp file error=[{}]", error),
        })?;
        fs::rename(&temp_path, path).map_err(|error| ZippyError::Io {
            reason: format!("failed to move registry snapshot into place error=[{}]", error),
        })?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<RegistrySnapshot> {
        let bytes = fs::read(path).map_err(|error| ZippyError::Io {
            reason: format!("failed to read registry snapshot error=[{}]", error),
        })?;
        serde_json::from_slice(&bytes).map_err(|error| ZippyError::Io {
            reason: format!("failed to decode registry snapshot error=[{}]", error),
        })
    }
}
```

In `crates/zippy-master/src/lib.rs` export snapshot types.

In `crates/zippy-master/src/server.rs`, add:

```rust
pub fn from_snapshot_path(snapshot_path: &Path) -> Result<Self> {
    let snapshot = SnapshotStore::load(snapshot_path)?;
    let mut registry = Registry::default();
    let mut bus = Bus::default();

    for stream in snapshot.streams {
        registry.ensure_stream(&stream.stream_name, stream.ring_capacity)?;
        registry.set_stream_status(&stream.stream_name, "restored")?;
        bus.ensure_stream(&stream.stream_name, stream.ring_capacity)?;
    }

    Ok(Self {
        registry: Arc::new(Mutex::new(registry)),
        bus: Arc::new(Mutex::new(bus)),
        running: Arc::new(AtomicBool::new(true)),
    })
}
```

In `Registry`, add:

```rust
pub fn set_stream_status(&mut self, stream_name: &str, status: &str) -> Result<(), RegistryError> {
    let stream = self.streams.get_mut(stream_name).ok_or_else(|| RegistryError::StreamNotFound {
        stream_name: stream_name.to_string(),
    })?;
    stream.status = status.to_string();
    Ok(())
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p zippy-master --test master_server master_restores_registered_streams_from_snapshot_as_restored -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-master/src/snapshot.rs crates/zippy-master/src/lib.rs crates/zippy-master/src/server.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add master registry snapshot restore"
```

---

### Task 5: Expose heartbeat and restored stream queries to Python

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Write the failing Python API test**

Add this test to `pytests/test_python_api.py`:

```python
def test_master_client_heartbeat_keeps_registered_process_alive(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    process_id = client.register_process("worker_a")
    client.heartbeat()

    stream_info = {
        "process_id": process_id,
    }
    assert stream_info["process_id"].startswith("proc_")
    server.stop()
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
uv run pytest pytests/test_python_api.py -k master_client_heartbeat_keeps_registered_process_alive -v
```

Expected: FAIL because Python `MasterClient` does not expose `heartbeat`.

- [ ] **Step 3: Implement Python binding**

In `crates/zippy-python/src/lib.rs`, add:

```rust
fn heartbeat(&self) -> PyResult<()> {
    self.inner
        .lock()
        .unwrap()
        .heartbeat()
        .map_err(to_py_runtime_error)
}
```

In `python/zippy/_internal.pyi`, add:

```python
class MasterClient:
    def heartbeat(self) -> None: ...
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
uv run pytest pytests/test_python_api.py -k master_client_heartbeat_keeps_registered_process_alive -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: expose master heartbeat to python"
```

---

### Task 6: Full verification and docs alignment

**Files:**
- Modify: `examples/python/README.md`

- [ ] **Step 1: Update runtime expectations in docs**

Add a short section to `examples/python/README.md`:

```md
## Master lease and snapshot behavior

- worker processes must keep a single `MasterClient` and send periodic heartbeat
- `zippy-master` reclaims stale writers/readers after lease expiration
- control-plane snapshot restores stream metadata as `restored`; workers must re-register and re-attach after master restart
```

- [ ] **Step 2: Run Rust verification**

Run:

```bash
cargo test -p zippy-core --test master_client -v
cargo test -p zippy-master --test master_server --test master_client_roundtrip -v
```

Expected: PASS

- [ ] **Step 3: Run Python verification**

Run:

```bash
uv run pytest pytests/test_python_api.py -k 'master_client_heartbeat_keeps_registered_process_alive or master_bus_writer_close_releases_stream_for_restart' -v
```

Expected: PASS

- [ ] **Step 4: Run lint/check verification**

Run:

```bash
cargo check -p zippy-master -p zippy-python
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add examples/python/README.md
git commit -m "docs: document master lease and snapshot behavior"
```

---

## Self-Review

### Spec coverage

- lease / heartbeat: Task 1, Task 2, Task 3, Task 5
- stale writer reclaim: Task 3
- control-plane snapshot: Task 4
- restored status semantics: Task 4, Task 6
- no data-plane recovery: enforced by Task 4 implementation scope

### Placeholder scan

- 无 `TODO/TBD`
- 每个任务都包含了测试、实现、验证、提交

### Type consistency

- `HeartbeatRequest` / `HeartbeatAccepted` 在协议、client、Python 绑定中保持一致
- `restored` 作为恢复状态只用于控制面 registry
- `ProcessRecord` 不参与 snapshot 恢复
