# Legacy service adapters

`LegacyCatalogAdapter`, `LegacyRuntimeRegistryAdapter`, and `LegacyQueryEngineAdapter` are Phase 1 migration components. They let
the new single-node composition root depend on stable `zippy-types` service traits while the
runtime still uses the existing master registry and gateway collect implementation.

They are not new database engines and must not become long-term feature surfaces.

## Data paths

```text
zippy-server
  -> CatalogRead/CatalogWrite
  -> LegacyCatalogAdapter
  -> master Registry

zippy-server
  -> RuntimeRegistry
  -> LegacyRuntimeRegistryAdapter
  -> boot-scoped state embedded in master Registry

zippy-server
  -> QueryEngine
  -> LegacyQueryEngineAdapter
  -> LegacyCollectFacade
  -> bounded GatewayCollectStreamProducer
```

`LegacyCollectFacade` is the narrow gateway-internal bridge. It hides `GatewayState` and the
producer enum. `LegacyQueryEngineAdapter` owns the public query lifecycle: owner capabilities,
one-time execution tokens, status, cancellation, plan TTL, admission limits, and Arrow batch
delivery.

## Capability matrix

| Capability | Phase 1 behavior |
| --- | --- |
| Default namespace discovery | Supported through one synthetic `default` namespace |
| Append/frame/key-value discovery | Supported only for streams with explicit `zippy_table_kind` |
| Internal physical shard discovery | Hidden by `zippy_internal` and strict legacy shard-name fallback |
| Stable namespace/table identity | Deterministic SHA-256 domain-separated legacy IDs |
| Catalog revision | Dedicated monotonic revision persisted in the legacy snapshot |
| Current snapshot version | Advances only when the committed persisted-file view changes |
| Exact historical catalog snapshot | Rejected with `UNSUPPORTED_PLAN` |
| Typed catalog mutation | Rejected with `UNSUPPORTED_PLAN` |
| Process/source/engine runtime status | Supported from the same legacy Registry critical section |
| Subscription connection status | Supported for legacy reader attach/detach registrations |
| Segment reader lease status | Not projected; these query/storage leases remain on their legacy owner path |
| Query runtime status | Intentionally absent; `QueryEngine` is the sole authority |
| Runtime restart recovery | Never restored; every master boot receives a new boot ID and empty records |
| Runtime owner authorization | Not expressible by the current trait; trusted in-process callers only |
| Legacy filter/projection collect | Supported through the bounded producer path |
| SQL | Rejected with `UNSUPPORTED_PLAN` |
| Snapshot query pinning | Rejected; no materialized fallback is allowed |
| Head/tail/slice or residual operations | Rejected until a bounded implementation exists |
| Per-query memory override and spill | Rejected when the legacy runtime cannot enforce it |

The catalog adapter captures a lightweight owned view under one registry lock and performs table
mapping after releasing the lock. It does not clone schemas, file manifests, persisted events, or
runtime metrics into discovery responses. Runtime-only status changes do not advance the catalog
revision. Every catalog read also shares the master lifecycle flag and returns `UNAVAILABLE` after
the master stops, including reads issued through handles cloned before shutdown.

Source and engine status plus metrics belong to live runtime state. Status updates mutate the
in-memory registry and notify watchers without cloning or serializing the persistent catalog.
Durable catalog mutations still use full rollback on snapshot failure, but their snapshots encode
the restart-safe `registered` status and empty metrics instead of copying live values. Reloaded
streams, sources, and engines enter `restored` with empty metrics.

The runtime adapter does not maintain a best-effort mirror. Its boot-scoped records are embedded in
the legacy `Registry` and are mutated while holding the same registry mutex as legacy
process/source/engine/reader state. Existing full-registry rollback therefore restores both views
together when a durable snapshot write fails. Heartbeats and status updates mutate one indexed
record, advance only the runtime revision, and neither clone the registry nor write the legacy JSON
snapshot. Runtime records are omitted by the explicit `RegistrySnapshot` DTO, and snapshot restore
clears any temporary records created while reconstructing durable source and engine definitions.

Legacy typed IDs use a reserved, domain-separated SHA-256 namespace over the server boot ID,
legacy identity, and registration revision. Adapter clones see the same ID; re-registration gets a
new nonce and ID; a new server boot gets a new boot ID. A single monotonic runtime revision guards
compare-and-set, while removal guards both boot ID and revision, so stale work cannot operate on a
replacement instance. Direct
trait registration rejects the reserved legacy ID prefix, preventing caller IDs from colliding
with derived IDs.

Legacy status mapping is centralized. Known active, degraded, stopping, stopped, and failed values
map to legal `RuntimeState` transitions; unknown strings fail closed as `Failed`. Only top-level
finite JSON numbers become typed metrics. Non-numeric and nested values are ignored
deterministically. Records keep low-cardinality legacy name/type/process/input/output stream
attributes, but never expose the process token.

The `RuntimeRegistry` trait currently carries boot ID, revision, and expected state but no owner
capability or instance nonce argument. Those guards prevent cross-boot and stale-instance CAS, but
they do not authenticate which in-process caller owns a record. Phase 1 therefore exposes this
adapter only as a trusted local service. A future public control boundary must add an opaque owner
capability before it permits remote runtime mutation.

Legacy-derived records are read-only through the generic trait mutation methods. Their mutation
must pass through the existing control path, where the process token is checked before the same
Registry critical section updates legacy and typed runtime views. Generic trait registration and
CAS remain available for non-legacy IDs to support new trusted in-process runtime objects.

Query lifecycle is not copied into the runtime adapter. `QueryEngine` remains the sole authority
for plan, execution, status, cancellation, TTL, and retention. Legacy subscription coverage is
limited to reader attach/detach paths; the current legacy wire protocol has no general-purpose
subscription connection registration message.

The composition root creates this runtime adapter with closed admission. `activate()` opens it only
after legacy dependencies are ready, and `begin_shutdown()` closes it irreversibly under the same
Registry lock used by mutations. Master lifecycle shutdown also takes that lock before publishing
the stopped flag, giving runtime mutation a clear before-or-after shutdown ordering.

The query adapter accepts only `LEGACY_GATEWAY_PLAN_FORMAT_VERSION`. This JSON compatibility codec
has a distinct version and cannot be decoded as the native version 1 protobuf plan. The gateway
cursor preserves the configured per-query and process-wide byte budgets. The in-process result
bridge buffers at most one `RecordBatch`. Logical query admission and blocking opener/driver
workers use separate bounded permits: cancellation releases logical admission immediately, while a
worker blocked in legacy socket or file I/O retains its physical permit until that I/O returns.
The blocking-worker limit defaults to 72 for the default logical concurrency of 64, leaving eight
bounded replacement slots. Physical exhaustion fails fast with `RESOURCE_EXHAUSTED`. Persisted
Master lookups pass cancellation into the TCP control transport, which polls cancellation and one
absolute deadline in bounded intervals. Blocking file I/O can still retain its worker permit, and
synchronous Unix-socket `connect()` can only check cancellation immediately before and after the
connect call.

Retained lifecycle entries also have a fixed limit. The periodic sweeper normally removes terminal
entries past their retention deadline. Registration keeps its common path O(1): it checks duplicate
identity and available capacity without scanning or locking existing query statuses. Only when the
registry is full does registration use its terminal min-heap to remove expired entries and, if
necessary, evict the oldest remaining terminal entry. Terminal transitions add exactly one entry;
retention and capacity removal are amortized O(log n), with stale generations discarded lazily and
without scanning or locking every retained status. Ordering is deterministic by
`(finished_at_ns, query_id, generation)`; planned and running queries are never capacity-evicted. A
terminal entry removed before its normal retention deadline is no longer queryable and `status`
returns `NOT_FOUND`. Existing execution tokens remain protected by the engine nonce,
control-generation, and pointer identity checks, so reusing an evicted query ID cannot execute a
retired query instance. Gateway resource exhaustion, missing files, and persisted-data corruption
remain distinct structured errors when they cross the query boundary; callers do not need to parse
error messages to decide retry behavior. Structured gateway cancellation maps to `Cancelled`, while
a structured deadline returned while opening the producer is compared with the query's absolute
deadline. It maps to `TimedOut` with `DEADLINE_EXCEEDED` only after that query deadline is due; an
earlier gateway write/control timeout, including a query without a deadline, maps to retryable
`UNAVAILABLE` and `Failed`. The first terminal transition remains authoritative when cancellation,
operational timeout, and query timeout race.

Every owner capability also has an engine-local nonce in addition to its session identity. Status
and cancellation require both values, preventing an older capability for the same session from
operating a query created by a newer capability. Early capacity eviction retains a lightweight
QueryId tombstone until the evicted query's original terminal-retention deadline, so the ID cannot
be reused while delayed status or cancellation remains valid. Tombstones use a separate expiry
heap and have a hard configurable limit (`max_query_tombstones`, default 65,536). If that limit is
reached, further early eviction fails closed with `RESOURCE_EXHAUSTED` rather than dropping an
unexpired identity guard. Reuse after retention is outside the legacy status guarantee; a future
wire contract should carry an opaque query-instance key if it needs indefinite protection from
arbitrarily delayed requests.

## Readiness boundary

`LegacyServerServices::health()` reports in-process adapter readiness, not end-to-end process
liveness. Catalog and query health are evaluated independently:

- catalog readiness requires open adapter admission and a running local master lifecycle;
- query readiness additionally requires the gateway facade to remain outside its stopped state;
- a stopped gateway therefore makes query unavailable without hiding catalog availability;
- `begin_shutdown()` closes adapter admission irreversibly, so a later `activate()` is rejected.

The master lifecycle flag and gateway facade validation are local observations. Direct users of
`LegacyServerServices` therefore receive `end_to_end_liveness: null`; that value must not be used as
an external readiness probe. `LegacyServerRuntime` owns the stronger boundary: it waits for the
master listener bind barrier, starts the configured Gateway, performs a real Gateway-to-master
control request, and only then calls `activate()`. Its health view publishes
`end_to_end_liveness: true` after those checks and changes it to `false` before shutdown. The
recommended product entry point is `zippy server run`; `zippy master run` is a compatibility alias
to the same server-owned runtime.

Catalog-only mode uses the same closed-then-activated `RuntimeRegistry` admission gate as full
mode. Shutdown caches its first structured result and gives Gateway, master process draining, and
the master listener one absolute deadline; repeated calls neither retry blocking cleanup nor turn a
previous timeout into a clean result. The standalone Rust `zippy-master` binary remains a Phase 1
rollback path and is not this composition root.

The query adapter enforces the composition admission gate. Phase 1 still returns the raw catalog
adapter for compatibility, so a previously cloned catalog handle remains callable after
`begin_shutdown()` while the master itself is still running. Once the master lifecycle stops, the
catalog handle rejects reads and query planning fails at catalog revision binding. Replacing the
remaining composition-admission convention with a catalog admission wrapper belongs in the
server-owned runtime phase.

## Recommended use

Use these adapters for:

- moving business services to `CatalogRead` and `QueryEngine` before storage replacement;
- moving local status consumers to `RuntimeRegistry` without persisting heartbeat or metrics;
- running existing filter/projection queries without changing current wire behavior;
- comparing legacy results with future SQLite catalog or DataFusion shadow implementations;
- proving lifecycle, cancellation, readiness, and capability contracts independently of the old
  registry and gateway internals.

Do not use them to add new database features. In particular, do not add SQL parsing, joins,
historical manifests, transactional writes, or new persistence semantics to either adapter. Those
belong to the SQLite catalog, manifest storage, and DataFusion phases.

## Removal path

1. SQLite becomes the catalog read authority after importer and shadow-diff validation.
2. Manifest storage provides exact schema/file snapshots and leases.
3. DataFusion implements native plan version 1 and server-side SQL.
4. The composition root switches trait objects to the new implementations.
5. Runtime mutation receives an owner-capability contract before it becomes a remote API.
6. Legacy adapters and the compatibility plan version are removed after wire and result parity is
   proven.
