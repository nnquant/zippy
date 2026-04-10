# Python CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python-first `zippy` CLI so users can run `uv run zippy master run ...` and inspect master-managed streams with `zippy stream ls/show`.

**Architecture:** Add minimal stream query APIs to the master control plane, expose them through `MasterClient`, and wrap them in a `click`-based Python CLI entrypoint registered via `project.scripts`. Keep the CLI as a thin management shell over existing Rust-backed bindings rather than reimplementing control-plane behavior.

**Tech Stack:** Rust (`zippy-core`, `zippy-master`, `zippy-python`), PyO3, Python `click`, `pytest`, `uv`, `maturin`

---

## File Map

### Control plane protocol and server

- Modify: `crates/zippy-core/src/bus_protocol.rs`
  - add `list_streams` / `get_stream` request-response shapes and a serializable stream summary/detail record
- Modify: `crates/zippy-master/src/registry.rs`
  - expose `list_streams()` and richer `get_stream()` data for CLI queries
- Modify: `crates/zippy-master/src/server.rs`
  - serve new query requests over the existing Unix socket control API
- Test: `crates/zippy-master/tests/master_server.rs`
  - cover list/show control responses

### Rust master client + Python bindings

- Modify: `crates/zippy-core/src/master_client.rs`
  - add `list_streams()` / `get_stream()`
- Test: `crates/zippy-core/tests/master_client.rs`
  - cover the new query calls
- Modify: `crates/zippy-python/src/lib.rs`
  - expose query methods on `MasterClient`
- Modify: `python/zippy/_internal.pyi`
  - add signatures and typed return shapes
- Modify: `python/zippy/__init__.py`
  - ensure `MasterClient` remains top-level and CLI helpers can import cleanly
- Test: `pytests/test_python_api.py`
  - cover Python query calls against a live `MasterServer`

### Python CLI package

- Modify: `pyproject.toml`
  - add `click` dependency and `project.scripts`
- Create: `python/zippy/cli.py`
  - root `click` group and main entrypoint
- Create: `python/zippy/cli_common.py`
  - shared endpoint option, JSON/text rendering, error wrapper
- Create: `python/zippy/cli_master.py`
  - `master run`
- Create: `python/zippy/cli_stream.py`
  - `stream ls`, `stream show`
- Create: `pytests/test_python_cli.py`
  - CLI behavior tests with `click.testing.CliRunner`
- Modify: `python/zippy/__init__.py`
  - keep package imports clean; no CLI auto-import side effects

### Docs

- Modify: `examples/python/README.md`
  - document `uv run zippy master run`, `stream ls`, `stream show`

---

### Task 1: Add stream query protocol types

**Files:**
- Modify: `crates/zippy-core/src/bus_protocol.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Write the failing server-side test for list/show requests**

Add two tests to `crates/zippy-master/tests/master_server.rs` that send raw control requests:

```rust
#[test]
fn list_streams_returns_registered_streams() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("master.sock");
    let (_server, handle) = spawn_test_server(&socket_path);

    let mut client = zippy_core::MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client.register_stream("openctp_ticks", Arc::new(arrow::datatypes::Schema::empty()), 64).unwrap();

    let streams = client.list_streams().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "openctp_ticks");
    assert_eq!(streams[0].ring_capacity, 64);

    _server.shutdown();
    handle.join().unwrap();
}

#[test]
fn get_stream_returns_single_registered_stream() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("master.sock");
    let (_server, handle) = spawn_test_server(&socket_path);

    let mut client = zippy_core::MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client.register_stream("openctp_ticks", Arc::new(arrow::datatypes::Schema::empty()), 64).unwrap();

    let stream = client.get_stream("openctp_ticks").unwrap();
    assert_eq!(stream.stream_name, "openctp_ticks");
    assert_eq!(stream.ring_capacity, 64);

    _server.shutdown();
    handle.join().unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p zippy-master --test master_server list_streams_returns_registered_streams -- --exact
```

Expected: FAIL with missing `list_streams` / `get_stream` protocol or client methods.

- [ ] **Step 3: Add protocol request/response and stream info types**

Extend `crates/zippy-core/src/bus_protocol.rs` with concrete serializable records:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    RegisterProcess(RegisterProcessRequest),
    RegisterStream(RegisterStreamRequest),
    WriteTo(AttachStreamRequest),
    ReadFrom(AttachStreamRequest),
    ListStreams,
    GetStream { stream_name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    ProcessRegistered { process_id: String },
    StreamRegistered { stream_name: String },
    WriterAttached { descriptor: WriterDescriptor },
    ReaderAttached { descriptor: ReaderDescriptor },
    StreamsListed { streams: Vec<StreamInfo> },
    StreamFetched { stream: StreamInfo },
    Error { reason: String },
}
```

Also update `fmt::Display` to render the new response shapes.

- [ ] **Step 4: Run the master server test again**

Run:

```bash
cargo test -p zippy-master --test master_server list_streams_returns_registered_streams -- --exact
```

Expected: still FAIL, now at registry/server/client gaps rather than protocol type absence.

- [ ] **Step 5: Commit protocol scaffolding**

```bash
git add crates/zippy-core/src/bus_protocol.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add stream query protocol types"
```

### Task 2: Implement registry and server query handlers

**Files:**
- Modify: `crates/zippy-master/src/registry.rs`
- Modify: `crates/zippy-master/src/server.rs`
- Test: `crates/zippy-master/tests/master_server.rs`

- [ ] **Step 1: Extend the failing test to assert detail fields**

Add assertions in `crates/zippy-master/tests/master_server.rs`:

```rust
assert_eq!(streams[0].writer_process_id, None);
assert_eq!(streams[0].reader_count, 0);
assert_eq!(streams[0].status, "registered");
```

and:

```rust
assert_eq!(stream.writer_process_id, None);
assert_eq!(stream.reader_count, 0);
assert_eq!(stream.status, "registered");
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p zippy-master --test master_server get_stream_returns_single_registered_stream -- --exact
```

Expected: FAIL because registry/server do not implement these queries yet.

- [ ] **Step 3: Add registry query helpers and server request handling**

In `crates/zippy-master/src/registry.rs`, add helpers like:

```rust
pub fn list_streams(&self) -> Vec<StreamRecord> {
    self.streams.values().cloned().collect()
}

pub fn get_stream(&self, stream_name: &str) -> Option<&StreamRecord> {
    self.streams.get(stream_name)
}
```

and extend `StreamRecord` to hold:

```rust
pub struct StreamRecord {
    pub stream_name: String,
    pub ring_capacity: usize,
    pub writer_process_id: Option<String>,
    pub reader_count: usize,
    pub status: String,
}
```

In `crates/zippy-master/src/server.rs`, handle:

```rust
ControlRequest::ListStreams => {
    let streams = self
        .registry
        .lock()
        .unwrap()
        .list_streams()
        .into_iter()
        .map(StreamInfo::from)
        .collect();
    ControlResponse::StreamsListed { streams }
}
ControlRequest::GetStream { stream_name } => {
    match self.registry.lock().unwrap().get_stream(&stream_name).cloned() {
        Some(stream) => ControlResponse::StreamFetched {
            stream: StreamInfo::from(stream),
        },
        None => ControlResponse::Error {
            reason: format!("stream not found stream_name=[{}]", stream_name),
        },
    }
}
```

Also update stream registration to initialize:

```rust
writer_process_id: None,
reader_count: 0,
status: "registered".to_string(),
```

- [ ] **Step 4: Run server query tests**

Run:

```bash
cargo test -p zippy-master --test master_server list_streams_returns_registered_streams get_stream_returns_single_registered_stream -v
```

Expected: PASS

- [ ] **Step 5: Commit server query support**

```bash
git add crates/zippy-master/src/registry.rs crates/zippy-master/src/server.rs crates/zippy-master/tests/master_server.rs
git commit -m "feat: add master stream query handlers"
```

### Task 3: Add Rust master client query methods

**Files:**
- Modify: `crates/zippy-core/src/master_client.rs`
- Test: `crates/zippy-core/tests/master_client.rs`

- [ ] **Step 1: Write the failing master client tests**

Add to `crates/zippy-core/tests/master_client.rs`:

```rust
#[test]
fn master_client_lists_streams() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("master.sock");
    let (_server, handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client.register_stream("ticks", empty_schema(), 32).unwrap();

    let streams = client.list_streams().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].stream_name, "ticks");

    _server.shutdown();
    handle.join().unwrap();
}

#[test]
fn master_client_gets_stream() {
    let socket_dir = tempfile::tempdir().unwrap();
    let socket_path = socket_dir.path().join("master.sock");
    let (_server, handle) = spawn_test_server(&socket_path);

    let mut client = MasterClient::connect(&socket_path).unwrap();
    client.register_process("writer").unwrap();
    client.register_stream("ticks", empty_schema(), 32).unwrap();

    let stream = client.get_stream("ticks").unwrap();
    assert_eq!(stream.stream_name, "ticks");

    _server.shutdown();
    handle.join().unwrap();
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p zippy-core --test master_client master_client_lists_streams -- --exact
```

Expected: FAIL because `MasterClient` lacks query methods.

- [ ] **Step 3: Implement `list_streams()` and `get_stream()`**

In `crates/zippy-core/src/master_client.rs`, add:

```rust
pub fn list_streams(&self) -> Result<Vec<StreamInfo>> {
    match self.send_request(ControlRequest::ListStreams)? {
        ControlResponse::StreamsListed { streams } => Ok(streams),
        other => Err(unexpected_response("StreamsListed", other)),
    }
}

pub fn get_stream(&self, stream_name: &str) -> Result<StreamInfo> {
    match self.send_request(ControlRequest::GetStream {
        stream_name: stream_name.to_string(),
    })? {
        ControlResponse::StreamFetched { stream } => Ok(stream),
        other => Err(unexpected_response("StreamFetched", other)),
    }
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test -p zippy-core --test master_client -v
```

Expected: PASS

- [ ] **Step 5: Commit Rust client query methods**

```bash
git add crates/zippy-core/src/master_client.rs crates/zippy-core/tests/master_client.rs
git commit -m "feat: add master client stream queries"
```

### Task 4: Expose stream queries through PyO3 `MasterClient`

**Files:**
- Modify: `crates/zippy-python/src/lib.rs`
- Modify: `python/zippy/_internal.pyi`
- Test: `pytests/test_python_api.py`

- [ ] **Step 1: Write failing Python tests for `MasterClient.list_streams/get_stream`**

Add to `pytests/test_python_api.py`:

```python
def test_master_client_lists_streams(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", schema, 64)

    streams = client.list_streams()
    assert len(streams) == 1
    assert streams[0]["stream_name"] == "openctp_ticks"
    assert streams[0]["ring_capacity"] == 64

    server.stop()


def test_master_client_gets_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", schema, 64)

    stream = client.get_stream("openctp_ticks")
    assert stream["stream_name"] == "openctp_ticks"
    assert stream["status"] == "registered"

    server.stop()
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest pytests/test_python_api.py -k 'master_client_lists_streams or master_client_gets_stream' -v
```

Expected: FAIL with missing Python methods.

- [ ] **Step 3: Add PyO3 wrappers and stubs**

In `crates/zippy-python/src/lib.rs`, add methods on `#[pyclass] MasterClient`:

```rust
fn list_streams(&self, py: Python<'_>) -> PyResult<PyObject> {
    let streams = self
        .client
        .lock()
        .unwrap()
        .list_streams()
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let records = PyList::empty_bound(py);
    for stream in streams {
        let dict = PyDict::new_bound(py);
        dict.set_item("stream_name", stream.stream_name)?;
        dict.set_item("ring_capacity", stream.ring_capacity)?;
        dict.set_item("writer_process_id", stream.writer_process_id)?;
        dict.set_item("reader_count", stream.reader_count)?;
        dict.set_item("status", stream.status)?;
        records.append(dict)?;
    }
    Ok(records.into_py(py))
}

fn get_stream(&self, py: Python<'_>, stream_name: String) -> PyResult<PyObject> {
    let stream = self
        .client
        .lock()
        .unwrap()
        .get_stream(&stream_name)
        .map_err(|error| py_runtime_error(error.to_string()))?;
    let dict = PyDict::new_bound(py);
    dict.set_item("stream_name", stream.stream_name)?;
    dict.set_item("ring_capacity", stream.ring_capacity)?;
    dict.set_item("writer_process_id", stream.writer_process_id)?;
    dict.set_item("reader_count", stream.reader_count)?;
    dict.set_item("status", stream.status)?;
    Ok(dict.into_py(py))
}
```

In `python/zippy/_internal.pyi`, extend `MasterClient`:

```python
    def list_streams(self) -> list[dict[str, object]]: ...
    def get_stream(self, stream_name: str) -> dict[str, object]: ...
```

- [ ] **Step 4: Run Python tests**

Run:

```bash
cargo check -p zippy-python
uv run pytest pytests/test_python_api.py -k 'master_client_lists_streams or master_client_gets_stream' -v
```

Expected: PASS

- [ ] **Step 5: Commit Python query bindings**

```bash
git add crates/zippy-python/src/lib.rs python/zippy/_internal.pyi pytests/test_python_api.py
git commit -m "feat: expose master stream queries in python"
```

### Task 5: Add CLI package structure and `master run`

**Files:**
- Modify: `pyproject.toml`
- Create: `python/zippy/cli.py`
- Create: `python/zippy/cli_common.py`
- Create: `python/zippy/cli_master.py`
- Test: `pytests/test_python_cli.py`

- [ ] **Step 1: Write failing CLI tests for `master run`**

Create `pytests/test_python_cli.py`:

```python
from click.testing import CliRunner

from zippy.cli import main


def test_master_run_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "--help"])
    assert result.exit_code == 0
    assert "Run the local zippy-master daemon" in result.output
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
uv run pytest pytests/test_python_cli.py::test_master_run_help -v
```

Expected: FAIL because `zippy.cli` does not exist.

- [ ] **Step 3: Create CLI scaffolding and entrypoint**

Add `click` dependency and script entry in `pyproject.toml`:

```toml
[project]
dependencies = ["polars>=1.25.0", "pyarrow>=18.0.0", "click>=8.1.0"]

[project.scripts]
zippy = "zippy.cli:main"
```

Create `python/zippy/cli.py`:

```python
import click

from .cli_master import master_group
from .cli_stream import stream_group


@click.group()
def main() -> None:
    """zippy management CLI."""


main.add_command(master_group)
main.add_command(stream_group)
```

Create `python/zippy/cli_common.py`:

```python
import json
import click


def echo_json(payload: object) -> None:
    click.echo(json.dumps(payload, ensure_ascii=False, indent=2))


def cli_error(message: str) -> "NoReturn":
    raise click.ClickException(message)
```

Create `python/zippy/cli_master.py`:

```python
import click
import zippy


@click.group("master")
def master_group() -> None:
    """Manage the local zippy-master daemon."""


@master_group.command("run")
@click.argument("control_endpoint", required=False, default="/tmp/zippy-master.sock")
def run_master(control_endpoint: str) -> None:
    """Run the local zippy-master daemon."""
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    try:
        server.start()
        click.echo(f"master running control_endpoint=[{control_endpoint}]")
        server.join()
    except KeyboardInterrupt:
        server.stop()
        server.join()
```

- [ ] **Step 4: Run the CLI help test**

Run:

```bash
uv run pytest pytests/test_python_cli.py::test_master_run_help -v
uv run zippy master run --help
```

Expected: PASS and help text shows the `master run` command.

- [ ] **Step 5: Commit CLI scaffolding**

```bash
git add pyproject.toml python/zippy/cli.py python/zippy/cli_common.py python/zippy/cli_master.py pytests/test_python_cli.py
git commit -m "feat: add python cli master command"
```

### Task 6: Add `stream ls` and `stream show`

**Files:**
- Create: `python/zippy/cli_stream.py`
- Modify: `pytests/test_python_cli.py`

- [ ] **Step 1: Write failing CLI tests for stream commands**

Extend `pytests/test_python_cli.py`:

```python
def test_stream_ls_lists_registered_streams(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", pa.schema([("instrument_id", pa.string())]), 64)

    runner = CliRunner()
    result = runner.invoke(main, ["stream", "ls", "--control-endpoint", control_endpoint])

    assert result.exit_code == 0
    assert "openctp_ticks" in result.output
    server.stop()


def test_stream_show_returns_single_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", pa.schema([("instrument_id", pa.string())]), 64)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["stream", "show", "openctp_ticks", "--control-endpoint", control_endpoint],
    )

    assert result.exit_code == 0
    assert "stream_name: openctp_ticks" in result.output
    server.stop()
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest pytests/test_python_cli.py -k 'stream_ls_lists_registered_streams or stream_show_returns_single_stream' -v
```

Expected: FAIL because `stream` commands do not exist.

- [ ] **Step 3: Implement `stream` CLI**

Create `python/zippy/cli_stream.py`:

```python
import click
import zippy

from .cli_common import cli_error, echo_json


def _client(control_endpoint: str) -> zippy.MasterClient:
    return zippy.MasterClient(control_endpoint=control_endpoint)


@click.group("stream")
def stream_group() -> None:
    """Inspect master-managed streams."""


@stream_group.command("ls")
@click.option("--control-endpoint", default="/tmp/zippy-master.sock", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False)
def list_streams(control_endpoint: str, as_json: bool) -> None:
    try:
        streams = _client(control_endpoint).list_streams()
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(streams)
        return

    click.echo("STREAM NAME                 RING CAPACITY  STATUS")
    for stream in streams:
        click.echo(
            f\"{stream['stream_name']:<27} {stream['ring_capacity']:<14} {stream['status']}\"
        )


@stream_group.command("show")
@click.argument("stream_name")
@click.option("--control-endpoint", default="/tmp/zippy-master.sock", show_default=True)
@click.option("--json", "as_json", is_flag=True, default=False)
def show_stream(stream_name: str, control_endpoint: str, as_json: bool) -> None:
    try:
        stream = _client(control_endpoint).get_stream(stream_name)
    except RuntimeError as error:
        cli_error(str(error))

    if as_json:
        echo_json(stream)
        return

    for key in ["stream_name", "ring_capacity", "writer_process_id", "reader_count", "status"]:
        click.echo(f"{key}: {stream[key]}")
```

- [ ] **Step 4: Run CLI tests**

Run:

```bash
uv run pytest pytests/test_python_cli.py -v
uv run zippy stream ls --help
uv run zippy stream show --help
```

Expected: PASS

- [ ] **Step 5: Commit stream CLI**

```bash
git add python/zippy/cli_stream.py pytests/test_python_cli.py
git commit -m "feat: add python cli stream commands"
```

### Task 7: Document and verify the final CLI

**Files:**
- Modify: `examples/python/README.md`
- Test: `pytests/test_python_cli.py`

- [ ] **Step 1: Add CLI usage docs**

Update `examples/python/README.md` with a short section:

```md
## Python CLI

Run the local master:

```bash
uv run zippy master run /tmp/zippy-master.sock
```

List streams:

```bash
uv run zippy stream ls --control-endpoint /tmp/zippy-master.sock
```

Show one stream:

```bash
uv run zippy stream show openctp_ticks --control-endpoint /tmp/zippy-master.sock
```
```

- [ ] **Step 2: Run the full focused verification**

Run:

```bash
cargo test -p zippy-master --test master_server -v
cargo test -p zippy-core --test master_client -v
cargo check -p zippy-python
uv run pytest pytests/test_python_api.py -k 'master_client_lists_streams or master_client_gets_stream' -v
uv run pytest pytests/test_python_cli.py -v
uv run zippy --help
uv run zippy master run --help
uv run zippy stream ls --help
```

Expected:
- all Rust tests PASS
- all Python tests PASS
- CLI help commands exit 0

- [ ] **Step 3: Commit docs and final verification changes**

```bash
git add examples/python/README.md
git commit -m "docs: add python cli usage"
```

---

## Self-Review

- Spec coverage:
  - `master run`: Task 5
  - `stream ls/show`: Tasks 1-6
  - `click` + `project.scripts`: Task 5
  - control-plane query dependency: Tasks 1-4
- Placeholder scan:
  - no `TODO/TBD/similar to`
- Type consistency:
  - `MasterClient.list_streams() -> list[dict[str, object]]`
  - `MasterClient.get_stream(name) -> dict[str, object]`
  - CLI command names match the spec exactly
