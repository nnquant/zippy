# zippy-ingest

`zippy-ingest` owns native per-table runtime identity and serialized mutation admission. It has no
dependency on the legacy Master or Gateway writer map. The native actor path is independent from
`StreamTableMaterializer`; the temporary `memory` module deliberately wraps that materializer as
the Stage 7.1 compatibility authority shared by protocol adapters.

At startup, `TableRuntimeRegistry` atomically validates the Catalog-derived active table, schema,
snapshot, writer epoch, and committed logical watermark. `TableActorRegistry` then starts one
serial actor per table with two bounded admission layers:

- a 64-command mailbox per table by default;
- a shared 4,096-command budget across all tables by default.

The registry owner is not cloneable. Service adapters receive cloneable lookup handles, while the
owner alone closes admission and drains every accepted command during shutdown. A dropped response
waiter does not cancel an admitted command. Actor queue saturation returns retryable
`ResourceExhausted`; stopped actors return structured `Unavailable`; actor task failure is surfaced
immediately through `TableActorFailureMonitor`, closes global admission, and remains surfaced during
ordered drain. The monitor is driven by the owned actor tasks themselves, so supervision does not
create detached watcher tasks.

`TableActorRegistry::start` remains the read-only compatibility entrypoint.
`TableActorRegistry::start_with_persistence` is the production entrypoint: every table gets one
single-flight persist slot, and each accepted `PersistRequest` is revalidated against the
actor-owned current snapshot, schema version, writer epoch, and contiguous logical offset before
the shared `PersistController` sees it. Candidate writing and Catalog commit do not block snapshot
inspection. The actor validates the returned receipt in its own turn and atomically advances its
local snapshot. Persist admission and completion preserve an explicit `Rejected` versus `Fatal`
classification across the storage boundary. A definitive Catalog rejection fails only that request
and leaves the actor idle, regardless of whether it was an advance or historical replay. Writer,
commit-unknown, invalid-receipt, and controller-lifecycle failures close global actor admission and
are also reported during shutdown; retryable backpressure remains request-local.

Catalog-backed request identity is authoritative across restart while its durable idempotency row
is retained. An exact historical request returns the original receipt without rolling the actor's
newer snapshot backward; the same request ID with a different fingerprint returns `Conflict` and
does not poison later writes. The actor passes `Advance` or `HistoricalReplay` explicitly to
storage. Both modes may build Parquet bytes in parallel temporary files, but storage probes the
complete Catalog envelope first: exact, conflicting, and missing historical replays drop those
temporary files without publishing; only a missing advance may cross the final publication
boundary.

`MemoryIngestRuntime` now owns each process-local compatibility materializer exactly once. Gateway
and Flight receive cloneable handles, so either listener can be disabled without moving writer
ownership to the other protocol. The runtime bounds retained tables, rejects schema drift, exposes
lock/append/flush counters, closes admission before drain, and publishes active descriptors through
a composition-root Catalog adapter. The unified Server uses this authority; standalone Gateway
retains its internal writer only as a legacy rollback path.

The current legacy Master adapter registers one dedicated `zippy-server-ingest` process and owns a
one-second heartbeat worker. The worker shares the same process credentials as descriptor/source
operations, has a one-second control-request timeout, and is joined before process unregister.
Heartbeat and writer cleanup therefore remain inside the Server's absolute shutdown deadline.

This cutover still acknowledges process-local memory visibility only. RecordBatch append into the
native table actor, producer-session fencing, sequence idempotency, ingest WAL, stable logical
offsets, restart recovery, and durable subscription remain subsequent Phase 7 stages.
