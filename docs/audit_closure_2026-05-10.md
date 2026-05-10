# Zippy Audit Closure 2026-05-10

## Summary

This document closes the `2026-05-08` audit findings by status. It separates
resolved correctness/security issues from deferred architecture and performance work.

Verification baseline:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `uv run --extra dev black --check python pytests`
- `uv run --extra dev ruff check python pytests`
- `uv run --extra dev mypy`
- `.venv/bin/python -m pytest pytests -q`

Python gateway tests require permission to open local TCP sockets in the sandbox.

## Capability Description

### Local Segment Runtime

Local stream-table reads use segment metadata and committed-row high watermarks. Snapshot reads
pin a fixed boundary and reader leases protect active/sealed segments while the snapshot is used.
Live subscribe remains best-effort and does not backfill rows missed before subscribe start or
while disconnected.

### Remote Gateway Data Plane

Gateway supports:

- `write_batch`
- `close_writer`
- `get_stream`
- `list_streams`
- `metrics`
- `collect`
- `create_snapshot`
- `release_snapshot`
- `subscribe_table`

Gateway guarantees:

- token is checked after header read and before payload read;
- header and payload sizes are bounded;
- TCP accept/read/write is Tokio based;
- blocking scan/encode/materializer work is behind bounded gates;
- master control requests use a Gateway-owned async control client instead of a global
  `MasterClient` mutex;
- remote snapshot uses pinned active high watermark and retained leases;
- remote `subscribe()` and `subscribe_table()` evaluate supported filters at the Gateway.

Gateway native query support is intentionally a subset. Unsupported or unsafe query plan nodes must
stay in residual/local evaluation instead of being blindly pushed into Gateway execution.

Native Gateway query support includes:

- `head`
- `tail`
- `slice`
- simple column projection
- `drop`
- `rename`
- `sort` by supported columns
- simple filter expressions including binary comparisons, boolean `and`/`or`, and supported
  `is_in` predicates

Deferred native Gateway query support:

- `with_columns`
- arbitrary computed `select` expressions
- native remote join
- streaming collect
- parallel persisted parquet scan

Deferred architecture and performance work is tracked in `ROADMAP.md`.

## Diagnostics

### Build And Test Diagnostics

Use the full verification baseline when closing audit work. For targeted gateway work, use:

```bash
cargo test -p zippy-gateway
cargo clippy -p zippy-gateway --all-targets -- -D warnings
cargo test -p zippy-master master_binary_starts_native_gateway_when_enabled -- --nocapture
uv run maturin develop
.venv/bin/python -m pytest pytests/test_python_api.py -q -k "remote_subscribe_table or gateway_token or RemoteGatewayWriter or remote_gateway"
```

### Gateway Runtime Diagnostics

Gateway metrics include:

- `requests_total`
- `auth_failures_total`
- `errors_total`
- `write_batches_total`
- `written_rows_total`
- `write_rejections_total`
- `collect_requests_total`
- `subscribe_clients_total`
- `connections_active`
- `connections_rejected_total`
- `blocking_requests_active`
- `blocking_requests_rejected_total`
- `subscribe_clients_active`
- `subscribe_clients_rejected_total`
- `request_timeouts_total`
- `payload_timeouts_total`
- `master_async_requests_total`
- `master_process_reregistrations_total`

Useful interpretations:

- rising `auth_failures_total` means clients are missing or using the wrong token;
- rising `connections_rejected_total` means the Gateway connection limit is too low or clients are
  leaking sockets;
- rising `blocking_requests_rejected_total` means scan/encode/write work is saturated;
- rising `subscribe_clients_rejected_total` means remote subscriber count hit the configured
  safety limit;
- rising `master_process_reregistrations_total` means Gateway lost its master process lease and
  had to re-register.

### State And Ownership Diagnostics

When stream state looks inconsistent:

- use `get_stream` to inspect `writer_epoch`, `write_seq`, `status`, active descriptor, sealed
  segments, persisted files and reader leases;
- verify writer/source ownership before destructive operations;
- verify persisted files live under the configured table persist root;
- verify snapshot reads use a `snapshot_id` and release it after use;
- distinguish empty result, stale stream, missing active descriptor and unreadable descriptor
  preflight failures.

## Closure Table

Status values:

- `fixed`: code and tests close the finding.
- `mitigated`: correctness or safety is protected, but native capability or diagnostics remain
  intentionally limited.
- `deferred`: known architecture/performance follow-up, tracked in `ROADMAP.md`.
- `not_applicable`: finding was removed by architecture deletion, usually bus ring removal.

| ID | Status | Closure note |
| --- | --- | --- |
| F001 | fixed | Workspace test/build gate passes with `zippy-perf` included. |
| F002 | fixed | `zippy-python` native tests compile in workspace verification. |
| F003 | fixed | Python suite passes: `309 passed`. |
| F004 | fixed | `python -m zippy` entrypoint and CLI tests are covered. |
| F005 | fixed | black, ruff, mypy, `py.typed`, and CI gates are present. |
| F006 | fixed | `replace_persisted_files` requires authorized process/admin capability and path validation. |
| F007 | fixed | `drop_table` is authorized and destructive deletion is guarded. |
| F008 | fixed | Persisted file paths are normalized and constrained to configured data roots. |
| F009 | fixed | `drop_table(drop_persisted=True)` prevalidates deletion before registry commit. |
| F010 | fixed | Persisted metadata stream/schema fields are owned by master normalization. |
| F011 | fixed | Status update requires owner/process capability. |
| F012 | fixed | Raw source/engine/sink registration validates process capability. |
| F057 | fixed | Raw unregister validates process capability. |
| F058 | fixed | Raw heartbeat validates process capability. |
| F059 | fixed | Stream registration requires capability/admin path and bus-root escape is removed. |
| F060 | not_applicable | Legacy bus writer/reader attach path was removed with bus ring shutdown. |
| F061 | not_applicable | Legacy bus close writer/reader path was removed with bus ring shutdown. |
| F062 | fixed | Publish descriptor/persist metadata validates owner and writer epoch. |
| F013 | fixed | Segment reader lease acquisition validates segment identity. |
| F014 | fixed | Lease acquire/release snapshot failure paths roll back. |
| F015 | fixed | Pipeline stop detaches writer and clears active descriptor. |
| F016 | fixed | Table health no longer treats ghost writer state as healthy. |
| F017 | fixed | Ready wait no longer accepts ghost table state. |
| F018 | fixed | Remote writer close and Gateway stop detach master writer/source. |
| F019 | fixed | Stream-table startup failure rolls back partial registration. |
| F020 | fixed | Removed dependency on same-client schema cache for write attachment. |
| F021 | fixed | Engine stop no longer holds runtime lock across external source stop. |
| F022 | fixed | Runtime startup failure cleans up archive/background worker state. |
| F023 | fixed | Native source sink capsule lifecycle is guarded. |
| F024 | not_applicable | Closed/stale bus mmap access was removed with bus ring shutdown. |
| F025 | not_applicable | Bus frame-level instrument filter was removed with bus ring shutdown. |
| F026 | fixed | Replay/Pipeline writer epoch is bound to master source epoch. |
| F027 | fixed | Persisted snapshot holds leases and fixed live boundary. |
| F028 | fixed | Remote `snapshot=True` is executed by Gateway via snapshot id. |
| F029 | fixed | Descriptor update handling preserves multiple generations in order. |
| F030 | fixed | TimeSeries flush duplicate-window behavior is covered and corrected. |
| F031 | fixed | Reactive state failure policy supports default fail-fast and optional rollback. |
| F032 | fixed | RecordBatch row reconstruction preserves non-identity order. |
| F033 | fixed | Compaction metadata deletion is constrained to owned metadata paths. |
| F034 | fixed | Python persisted scan path uses validated metadata boundaries. |
| F035 | fixed | Gateway collect executes remote snapshot semantics. |
| F036 | fixed | Gateway row-range/filter order is preserved with residual plan handling. |
| F037 | fixed | Unsupported Gateway filters stay residual or return explicit unsupported errors. |
| F038 | mitigated | Remote correctness is protected by residual/local fallback; arbitrary native select expressions are deferred. |
| F039 | fixed | Projection/filter ordering is guarded by scan projection column collection and residual execution. |
| F040 | mitigated | `with_columns` is not native Gateway execution; residual/local path protects correctness. |
| F041 | mitigated | Join is not native Gateway execution; capability/residual behavior protects correctness. |
| F042 | fixed | Remote `subscribe()` and `subscribe_table()` support Gateway-side filters. |
| F043 | fixed | Gateway authenticates before payload read and enforces header/payload limits. |
| F044 | fixed | Gateway TCP data plane uses Tokio and bounded resources instead of per-connection threads. |
| F045 | fixed | `TsReturn` division by zero does not produce `inf`. |
| F046 | fixed | Non-finite floats are rejected or normalized before polluting reactive/TS state. |
| F047 | fixed | `Log` rejects invalid non-positive/non-finite inputs. |
| F048 | fixed | Cast behavior handles NaN/inf deterministically. |
| F049 | fixed | Python factor spec `id_column` is honored. |
| F050 | fixed | Empty id filters are rejected. |
| F051 | fixed | Python stream info exposes `writer_epoch` and `write_seq`. |
| F052 | fixed | Low-level persisted scan no longer trusts arbitrary file paths. |
| F053 | mitigated | Capability behavior is documented; native Gateway capability expansion is deferred. |
| F054 | fixed | Stream metadata exposes segment row capacity separately from buffer size. |
| F055 | fixed | Active descriptor and shm identity are validated before attach/read. |
| F056 | not_applicable | Legacy bus generation issue was removed with bus ring shutdown; segment descriptors retain generation checks. |

## Remaining Non-Blocking Work

The following are explicitly not blocking this audit closure:

- streaming collect;
- parallel persisted parquet scan;
- native Gateway `with_columns` and join;
- configurable Gateway resource limits;
- async master server and long-lived control sessions;
- formal performance baseline report.

They are tracked in `ROADMAP.md`.
