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

The server owns startup and shutdown order. It binds the master listener first, waits for its
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
is loaded, so later environment changes cannot redirect an already loaded configuration. When the
setting is absent, the runtime uses `<control-endpoint-state-dir>/data`; this keeps the default
stable for one endpoint while preventing unrelated endpoint profiles from sharing a root by
accident.

The data-root lock is separate from the detached launcher's endpoint lock under `~/.zippy/run`.
The launcher lock protects one CLI-managed endpoint generation. The data-root lock protects the
database itself, so two different endpoints configured with the same `[server].data_dir` still
conflict before either listener is bound. `server.lock` is owner-only, never unlinked during normal
cleanup, and contains diagnostic PID/version/acquisition metadata; the kernel advisory lock is the
authority and is released after process exit, including abrupt exit.

A clean server shutdown releases data-root ownership only after Gateway and Master/listener cleanup
has completed. An incomplete shutdown retains the top-level capability, and a listener that has not
joined retains its own capability clone. Ordinary `DataDirectory::open` remains a read-only layout
validation operation and does not create missing directories. Writable SQLite Catalog
configurations and durable file publication require an `OwnedDataDirectory` capability.

This phase establishes process ownership and crash-safe publication primitives. The production
runtime still uses the legacy metadata authority until Parquet candidate publication, Catalog
manifest visibility, reconciliation, and authority cutover are completed.

On shutdown the order is reversed: adapter admission closes first, Gateway releases its master
lease and stops, then master performs its graceful shutdown and persists the catalog snapshot.
Gateway, master process draining, and listener join share one absolute monotonic deadline from
`master.shutdown_timeout_ms`; a later stage receives only the remaining budget. The structured
shutdown result records incomplete writer cleanup, process leases, listener errors, and deadline
exhaustion. Repeated shutdown calls return the first result without retrying cleanup or hiding an
earlier failure. Any timeout is returned as `DEADLINE_EXCEEDED`; other cleanup failures also make
the daemon return an error.

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
never publishes readiness after the deadline; cleanup uses its own bounded shutdown budget and any
incomplete rollback is appended to the returned startup error.
