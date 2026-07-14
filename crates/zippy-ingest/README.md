# zippy-ingest

`zippy-ingest` owns native per-table runtime identity and serialized mutation admission. It has no
dependency on the legacy master, Gateway writer map, or `StreamTableMaterializer`.

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
does not poison later writes. This path still writes a candidate before Catalog can reject a
different replay fingerprint, so replay-only publication cleanup is an authority-cutover gate.

RecordBatch append, segment rollover, query snapshot leases, producer-session fencing, and ingest
WAL remain subsequent implementation stages. Until the later authority cutover is complete, the
legacy Gateway remains write authority.
