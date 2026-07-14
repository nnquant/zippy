# Single-node server runtime

## Recommended entry point

New deployments should start the complete single-node product through one command:

```bash
zippy server run --config /path/to/zippy.toml
```

On Linux this command is a bounded launcher: it starts the native server in a new session, waits for
the complete runtime readiness boundary, performs real Master and Gateway protocol probes, prints a
startup summary, and returns to the shell. The summary includes the loaded native version, PID,
startup health, active mode, effective endpoints, WebUI reachability, bootstrap/native log paths,
and the owner-only state file. A successful example is:

```text
zippy server started
version: 0.5.0.devN+gabcdef0
pid: 12345
health: healthy
mode: catalog_only
startup check: passed elapsed_ms=[82.4]
master: healthy endpoint=[tcp://127.0.0.1:17690]
gateway: disabled
webui: not running managed=[no] url=[http://127.0.0.1:17688/]
bootstrap log: /home/user/.zippy/logs/server-....log
native log: /home/user/.zippy/logs/zippy-master/....jsonl
state: /home/user/.zippy/run/server-....json
stop: zippy server stop tcp://127.0.0.1:17690
```

The launcher does not report success when only the Master port is open. Rust publishes its
nonce-bound ready record only after the Master listener, optional Gateway bind, Gateway-to-Master
request, compatibility adapter activation, and admission opening have all completed. Failure or
timeout terminates the child and removes partial active state. The child keeps a separate
owner-only, code-only failure outcome for startup diagnostics. A minimal child bootstrap publishes
`starting` before importing the full zippy package, then Rust atomically promotes the same state
file to `ready`. The short-lived launcher never rewrites authoritative child state, so killing the
launcher does not orphan an unmanageable daemon.

The bootstrap `starting` and failure-outcome records remain schema v1. The promoted `ready` record
uses schema v2 and requires typed `gateway_configured` and `native_storage_configured` facts; when
either dependency is configured, all of its readiness gates must be explicitly true. The CLI
rejects older or partially populated ready records instead of interpreting missing facts as a
disabled dependency. `native_catalog_ready` covers Catalog migration/integrity/recovery;
`native_current_snapshot_files_ready` separately proves that every managed file in each active
table's current snapshot passed physical verification. It does not claim historical snapshot
availability, and native historical-query admission remains closed in this migration phase.
Fresh startup accepts only ready v2. During a rolling CLI upgrade, `status` and `stop` still accept
an already-running ready-v1 generation after validating its unchanged PID/boot/start-tick/nonce
identity; status uses a non-persisted compatibility copy that explicitly treats native storage as
disabled.

Use the following commands for the detached process:

```bash
zippy server status --config /path/to/zippy.toml
zippy server stop --config /path/to/zippy.toml
```

`status` verifies the ownership lock and Linux process start identity before issuing bounded
protocol probes. `stop` opens a Linux pidfd, checks the boot ID and process start ticks against that
pinned process generation, and then sends `SIGTERM` through the pidfd. It only escalates to
`SIGKILL` when `--force` is explicitly supplied after the graceful timeout. A non-forced stop is
reported as graceful only after the child writes its clean shutdown outcome. State and lock files
default to `~/.zippy/run`, bootstrap and native logs default to `~/.zippy/logs`. They can be changed
with `--run-dir`, `--log-dir`, `ZIPPY_SERVER_RUN_DIR`, and `ZIPPY_SERVER_LOG_DIR`.
After process exit, `stop` must reacquire the endpoint lock and recheck the launch nonce before
removing state. If another generation has already started, cleanup fails explicitly and preserves
the replacement server.
`--startup-timeout-sec` is one end-to-end admission budget: the detached child receives the
launcher's remaining monotonic-clock budget instead of starting a second fixed timeout.

External supervisors must keep ownership of the long-lived process instead of supervising the
short-lived launcher:

```bash
zippy server run --foreground --config /path/to/zippy.toml
```

Use `--foreground` for systemd, RSPM, containers, PM task definitions, and interactive debugging.
The foreground form retains native `SIGINT`/`SIGTERM` graceful shutdown behavior.

The server owns startup and shutdown order. When native storage is enabled, it first opens and
checks SQLite, runs pre-replay reconciliation, replays durable Catalog intents, and runs final
reconciliation. The storage supervisor then starts a bounded `PersistController`, which exclusively
owns native Parquet writer jobs and the durable Catalog commit coordinator. Writer concurrency uses
`runtime.max_parallelism` (or `ZIPPY_MAX_PARALLELISM`) with a storage safety ceiling of 64; the
controller defaults retain at most 64 accepted jobs and 512 MiB of complete request input. It then
reads the current Catalog snapshots at one stable revision and verifies every visible managed
Parquet path, size, checksum, footer, and row count before binding any listener. One serial
`TableActor` is then started for each active table. The actor revalidates the current snapshot,
exact Arrow schema, writer epoch, and contiguous logical offset before using the controller; it
advances local state only after validating the durable Catalog receipt. A terminal controller error
or abnormal actor exit closes native admission, revokes dynamic server readiness, and starts
ordered actor/controller/Catalog cleanup. Definitive Catalog rejections remain request-local, and
an exact retained request ID can replay its original receipt after restart without rolling a newer
actor snapshot backward. It then binds the master listener, waits for its
readiness barrier, starts Gateway when `[gateway].enabled = true`, performs a real Gateway-to-master
control probe, and only then opens service admission. With Gateway disabled it intentionally runs
in catalog-only mode; it does not publish a query service that cannot execute.

## Managed data root ownership

The native runtime acquires `<data_root>/server.lock` before logging, loading the legacy snapshot,
or binding a listener. Configure the root independently from the legacy Parquet directory:

```toml
[server]
data_dir = "data/zippy"
```

`ZIPPY_SERVER_DATA_DIR` overrides the file setting. A leading `~/` is expanded while configuration
is loaded. Relative paths in a TOML file resolve from that file's directory; the environment
override must be absolute. When the setting is absent, the runtime uses
`<control-endpoint-state-dir>/data`. Unix state directories include the complete socket file name,
and TCP state directories include the complete bind address, so sibling sockets or equal ports on
different addresses do not share a database root.

The data-root lock is separate from the detached launcher's endpoint lock under `~/.zippy/run`.
The launcher lock protects one CLI-managed endpoint generation. The data-root lock protects the
database itself, so two different endpoints configured with the same `[server].data_dir` still
conflict before either listener is bound. `server.lock` is owner-only, never unlinked during normal
cleanup, and contains diagnostic PID/version/acquisition metadata; the kernel advisory lock is the
authority and is released after process exit, including abrupt exit.

The legacy registry snapshot now follows database identity rather than listener identity. Its
managed path is `<data_root>/catalog/legacy-master-registry.json`. When no explicit data root is
configured and an old endpoint-scoped `master-registry.json` exists, startup fails closed until the
operator stops the old server and explicitly sets `ZIPPY_SERVER_IMPORT_LEGACY_SNAPSHOT=1` for the
one import. The import checks that the old endpoint is offline, publishes the managed snapshot with
file and directory durability, and retains the old file as rollback evidence. An explicitly
configured data root never imports endpoint state. Once the managed file exists it is always
authoritative, even if the server later uses a different control endpoint.

A clean native-storage shutdown first closes and drains all per-table actors, then drains every
accepted Parquet write and Catalog commit, and only then closes SQLite. The server releases
data-root ownership only after Gateway, native storage, and Master/listener cleanup have completed.
An incomplete shutdown retains the top-level capability,
and a listener or storage supervisor that has not joined retains its own capability clone. Ordinary
`DataDirectory::open` remains a read-only layout validation operation and does not create missing
directories. Writable SQLite Catalog
configurations and durable file publication require an `OwnedDataDirectory` capability.
Runtime inspection APIs expose only a borrowed capability; they do not expose the internal `Arc`
used to retain ownership across the top-level runtime and listener thread.

Native storage lifecycle is an explicit migration gate and is disabled by default:

```toml
[server]
native_storage_enabled = true
```

`ZIPPY_SERVER_NATIVE_STORAGE=true` is the strict boolean environment override. Once enabled, SQLite
Catalog recovery is a required dependency: migration, integrity, retained intent replay, or either
direction of storage verification failure prevents listener bind and readiness. It never silently
falls back to the legacy-only path. The supervisor owns a dedicated OS thread and Tokio runtime, so synchronous
SQLite open/close never nests a runtime on the server caller thread. The low-level startup timeout
returns an owning handle rather than detaching the supervisor. The server composition root cancels
that handle and transfers it to a named background reaper before returning the deadline error. The
reaper retains the supervisor join handle and data-root ownership until coordinator drain and
Catalog close complete, without turning cleanup latency into an unbounded startup wait. Shutdown
timeout remains observational: accepted work is not cancelled and ownership remains held until
cleanup actually completes. If the process cannot create the reaper because of OS resource
exhaustion, startup still returns on time and logs a detached-cleanup error; the original supervisor
continues to retain the Catalog and data-root capability until it exits.

This phase establishes process ownership, crash-safe publication, gated recovery, and the internal
native table persistence state machine. The production runtime still uses the legacy metadata and
Gateway authorities until the native path is wired into public ingest, conflicting replay cannot
leave unpublished candidates, daemon status follows runtime storage failures, query mirroring and
shadow comparison pass, and authority cutover is completed.

On shutdown the order is reversed: adapter and native admission close first, Gateway releases its
master lease and stops, the native coordinator drains and closes SQLite, then master performs its
graceful shutdown and persists the legacy catalog snapshot. Gateway, native storage, master process
draining, and listener join share one absolute monotonic deadline from `master.shutdown_timeout_ms`;
a later stage receives only the remaining budget. The structured shutdown result records native
cleanup, process leases, listener errors, and deadline exhaustion. Repeated top-level shutdown calls
return the first result without hiding an earlier failure. Any timeout is returned as
`DEADLINE_EXCEEDED`; other cleanup failures also make the daemon return an error.

## WebUI reporting boundary

WebUI is still an independent process in this phase. `server run` reports and probes its URL so the
operator sees one startup summary, but `managed=[no]` means the server neither started nor will stop
that process. `not_running` is informational and does not degrade core server health. Start it
separately when needed:

```bash
zippy webui --uri tcp://127.0.0.1:17690
```

The reported URL defaults to `http://127.0.0.1:17688/`. Override it with `--webui-url` or
`ZIPPY_WEBUI_URL`; `ZIPPY_WEBUI_HOST` and `ZIPPY_WEBUI_PORT` are used when no complete URL is set.
The URL must point at the origin root and cannot contain credentials, a path, query, or fragment, so
session material cannot be persisted in server state or printed in the startup summary.
An HTTP response is reported as `endpoint reachable unverified`: it proves only that the configured
URL answered, not that the responding process is the expected WebUI or that its dependencies are
healthy.
WebUI will become `managed=[yes]` only after its lifecycle, authentication, readiness, and shutdown
contracts move under server ownership.

## Compatibility entry points

The following legacy entry points remain available:

```bash
zippy master run --config /path/to/zippy.toml
```

```python
zippy.run_master_daemon(config="/path/to/zippy.toml")
```

They preserve the old command arguments and the `master starting ...` CLI line, but forward to the
same server-owned native runtime as `zippy server run` and `zippy.run_server_daemon()`. Consequently,
an enabled Gateway is now started after the master readiness barrier instead of before it. New
process supervision and observability should use the `server_*` lifecycle events; the legacy CLI
name does not create a second master-only lifecycle.

Unlike `zippy server run`, the compatibility `zippy master run` command remains foreground-only.

The standalone Rust `zippy-master` binary and
`zippy_master::daemon::run_master_daemon()` remain a lower-level rollback path in Phase 1 because
their crate cannot depend upward on `zippy-server` without a Cargo dependency cycle. They do not
yet share the owning runtime above and must not be used as the production full-server entry point.
Their signal handler now closes Gateway before Master under one deadline, but startup ordering,
adapter admission, readiness, and structured outcomes remain on the legacy path. Moving that binary
target to the `zippy-server` package is required before the legacy Rust daemon path can be removed.

## Signal boundary

On Unix, the server runtime installs its `SIGINT` and `SIGTERM` subscriptions before opening any
listener. The first signal triggers the bounded graceful shutdown described above.

The Phase 1 native daemon does not yet provide an equivalent in-process signal subscription on
non-Unix platforms. It remains resident until the external process supervisor terminates it, so a
non-Unix forced termination does not guarantee the graceful persistence and lease cleanup path.
Programmatic users that need deterministic cleanup should own `LegacyServerRuntime` and call
`begin_shutdown()` followed by `shutdown()` explicitly until a cross-platform signal owner is
implemented.

The CLI currently rejects detached mode and `server stop` on every platform other than Linux because
its process identity contract requires Linux boot ID, `/proc` process start ticks, and pidfd process
control. Use
`zippy server run --foreground` elsewhere. This is an explicit safety boundary rather than claiming
that a PID-only signal completed the server's persistence and lease cleanup contract.

The startup timeout is an admission deadline, not a mechanism for cancelling synchronous file I/O.
Configuration and snapshot loading, Gateway runtime construction, and rollback are checked against
the deadline, but an already-running filesystem call cannot be force-cancelled by Rust. Startup
never publishes readiness after the deadline. General rollback uses its own bounded shutdown budget
and appends any incomplete cleanup to the returned startup error; native-storage startup
cancellation hands the owning handle to a background reaper before returning, so a failed startup
neither waits indefinitely nor discards the only handle retaining the data-root lock.
