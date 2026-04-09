# Unified Logging System Design

## Context

`zippy` currently lacks a real logging system. Runtime diagnostics, source lifecycle traces, and
performance output still rely on ad-hoc `println!` / `eprintln!` paths or environment-gated debug
probes. This is not enough for a production streaming system.

The missing capability is not "more print statements"; it is a unified, structured, configurable
logging system that works across:

- `zippy-core` runtime and source lifecycle
- `zippy-python` bridge and engine lifecycle
- `zippy-perf`
- `zippy-openctp`

## Goals

- provide a single logging stack for Rust and Python-facing workflows
- make logs readable by humans and machine-parsable at the same time
- support console output and managed file output together
- generate per-run log files automatically
- keep Python as a thin setup layer while the real logging system lives in Rust
- define a stable field vocabulary for the whole system

## Non-Goals

- distributed log shipping
- remote log ingestion or SaaS integrations
- log rotation by size
- arbitrary user-supplied formatter plugins
- replacing metrics with logs

## User Interface

Python entrypoint:

```python
zippy.setup_log(
    app="openctp_gateway",
    level="info",
    log_dir="logs",
    to_console=True,
    to_file=True,
)
```

Semantics:

- `app`: logical application name and directory segment
- `level`: default global log level
- `log_dir`: root directory for managed log files
- `to_console`: enable readable terminal logs
- `to_file`: enable JSONL file logs

`setup_log()` returns a small configuration snapshot:

```python
{
    "app": "openctp_gateway",
    "level": "info",
    "run_id": "ab12cd34",
    "file_path": "logs/openctp_gateway/2026-04-09_ab12cd34.jsonl",
}
```

The function is explicit. Logging is never auto-initialized by `engine.start()` or `source.start()`.

## File Layout

Managed log files are written to:

```text
logs/<app>/<date>_<run_id>.jsonl
```

Example:

```text
logs/openctp_gateway/2026-04-09_ab12cd34.jsonl
```

Rules:

- `run_id` is auto-generated
- each process run gets one file
- repeated runs on the same day do not overwrite previous files
- the file is newline-delimited JSON

## Output Formats

Two output layers are enabled from the same Rust subscriber stack.

### Console

- human-readable text
- concise event messages
- intended for local development, services, and terminal observation

### File

- JSON lines
- structured fields for downstream parsing
- each record must include a readable `message` field

The file format is not "fields only". Human readability is still required.

## Required JSON Fields

Every file log record must contain at least:

- `ts`
- `level`
- `app`
- `run_id`
- `component`
- `event`
- `message`

Additional fields may be added depending on the event.

Example:

```json
{
  "ts": "2026-04-09T10:15:32.123456Z",
  "level": "INFO",
  "app": "openctp_gateway",
  "run_id": "ab12cd34",
  "component": "openctp_source",
  "event": "subscribe_success",
  "instrument": "IF2606",
  "message": "subscribed market data successfully"
}
```

## Message Rules

- `message` is mandatory
- `message` is always English
- `message` should be short and readable
- contextual data belongs in structured fields, not only in the message text

Examples:

- good:
  - `message="engine started"`
  - `message="received source hello"`
  - `message="subscribe request failed"`
- bad:
  - `message="engine=foo queue=3 status=running"`
  - `message="发生错误"`

## Field Vocabulary

The first version standardizes the following field set.

### Core fields

- `app`
- `run_id`
- `component`
- `event`
- `message`
- `status`

### Runtime and pipeline fields

- `engine`
- `source`
- `stream`
- `sink`
- `rows`
- `batch_rows`
- `queue_depth`

### Market-data fields

- `instrument`
- `front`
- `broker_id`

### Error field

- `error`

Callers may attach more fields, but these names are the shared vocabulary expected across crates.

## Rust Architecture

Logging initialization lives in `zippy-core`.

New module:

- `crates/zippy-core/src/logging.rs`

Primary API:

- Rust: `zippy_core::setup_log(...)`
- Python: `zippy.setup_log(...)` is a thin wrapper over the Rust API

Implementation stack:

- `tracing`
- `tracing-subscriber`

Architecture:

- one subscriber registry
- one console layer
- one JSON file layer
- shared per-process context carrying `app` and `run_id`

No crate other than `zippy-core` initializes the subscriber.

## Initialization Rules

- calling `setup_log()` more than once in one process must not panic
- the first successful call establishes the active subscriber
- later calls may either:
  - return the existing configuration snapshot, or
  - return a clean runtime error indicating logging is already initialized

V1 chooses the simpler behavior:

- return the existing configuration snapshot if the requested config is equivalent
- otherwise return a runtime error saying logging is already initialized with different settings

## Integration Scope

The first version must wire logs into these areas.

### `zippy-core`

- engine start
- engine flush
- engine stop
- source start
- source stop
- worker failure
- publish failure
- source termination events

### `zippy-python`

- `setup_log()`
- runtime start / stop failures
- Python source bridge start / stop

### `zippy-perf`

- profile start
- profile finish
- pass / fail summary
- report path emission

### `zippy-openctp`

- connect start
- login success / failure
- subscribe success / failure
- reconnect start / success / failure
- source running / stopped

## Existing Debug Probes

Current debug toggles such as:

- `ZIPPY_DEBUG_STOP`
- `OPENCTP_DEBUG`

must remain available in V1, but their implementation should be redirected to the unified tracing
stack instead of raw `eprintln!`.

This preserves existing operational behavior while removing duplicated debug output paths.

## Error Handling

If file logging cannot be initialized:

- `setup_log()` fails
- no partial "console only" downgrade happens silently

Reason:

- silent degradation would hide missing production logs
- initialization errors should be explicit

If `to_console=False` and `to_file=False`:

- `setup_log()` returns an error

Reason:

- a logging system with zero outputs is a configuration bug

## Testing Strategy

### Rust

- logging setup creates the expected file path
- JSONL records contain required fields
- repeated setup calls follow the initialization rules

### Python

- `zippy.setup_log(...)` returns the expected snapshot
- file path exists after emitting at least one log
- JSONL file contains `message`, `level`, `component`, `event`, and `run_id`

### Manual verification

- call `zippy.setup_log(app="demo")`
- start a small engine flow
- verify:
  - terminal logs appear
  - file appears under `logs/demo/<date>_<run_id>.jsonl`
  - JSONL records are readable and structured

## Implementation Order

1. add `tracing` and `tracing-subscriber` to the workspace
2. implement `zippy-core` logging module and configuration snapshot
3. expose `zippy.setup_log(...)` in `zippy-python`
4. replace the current ad-hoc runtime debug prints
5. instrument `zippy-perf`
6. instrument `zippy-openctp`

## Open Questions Resolved

- console and file output are both supported in V1
- file logs are JSONL
- console logs are human-readable text
- `message` is mandatory and English
- logging is explicitly initialized
- file path is `logs/<app>/<date>_<run_id>.jsonl`
- `run_id` is auto-generated

## Acceptance Criteria

This design is complete when:

- `zippy.setup_log(...)` exists and is documented
- log files are created per run under the required path pattern
- JSONL records include `message`
- runtime and source lifecycle events are visible in logs
- `zippy-perf` and `zippy-openctp` emit structured logs through the same stack
