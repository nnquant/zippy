# Single-node server runtime

## Recommended entry point

New deployments should start the complete single-node product through one command:

```bash
zippy server run --config /path/to/zippy.toml
```

The server owns startup and shutdown order. It binds the master listener first, waits for its
readiness barrier, starts Gateway when `[gateway].enabled = true`, performs a real Gateway-to-master
control probe, and only then opens service admission. With Gateway disabled it intentionally runs
in catalog-only mode; it does not publish a query service that cannot execute.

On shutdown the order is reversed: adapter admission closes first, Gateway releases its master
lease and stops, then master performs its graceful shutdown and persists the catalog snapshot.
Gateway, master process draining, and listener join share one absolute monotonic deadline from
`master.shutdown_timeout_ms`; a later stage receives only the remaining budget. The structured
shutdown result records incomplete writer cleanup, process leases, listener errors, and deadline
exhaustion. Repeated shutdown calls return the first result without retrying cleanup or hiding an
earlier failure. Any timeout is returned as `DEADLINE_EXCEEDED`; other cleanup failures also make
the daemon return an error.

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

The startup timeout is an admission deadline, not a mechanism for cancelling synchronous file I/O.
Configuration and snapshot loading, Gateway runtime construction, and rollback are checked against
the deadline, but an already-running filesystem call cannot be force-cancelled by Rust. Startup
never publishes readiness after the deadline; cleanup uses its own bounded shutdown budget and any
incomplete rollback is appended to the returned startup error.
