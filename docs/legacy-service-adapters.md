# Legacy service adapters

`LegacyCatalogAdapter` and `LegacyQueryEngineAdapter` are Phase 1 migration components. They let
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

The master lifecycle flag and gateway facade validation are local observations. They do not prove
that TCP or Unix listeners are reachable, or that the master endpoint configured in the gateway is
healthy. Phase 1 reports `end_to_end_liveness: null` rather than deriving it from those flags. The
legacy CLI/runtime orchestrator must complete its existing listener and dependency probes before it
calls `activate()`. A later server phase should own those listeners and publish a real end-to-end
probe before using this structure as an external load-balancer readiness response.

The query adapter enforces the composition admission gate. Phase 1 still returns the raw catalog
adapter for compatibility, so a previously cloned catalog handle remains callable after
`begin_shutdown()` while the master itself is still running. Once the master lifecycle stops, the
catalog handle rejects reads and query planning fails at catalog revision binding. Replacing the
remaining composition-admission convention with a catalog admission wrapper belongs in the
server-owned runtime phase.

## Recommended use

Use these adapters for:

- moving business services to `CatalogRead` and `QueryEngine` before storage replacement;
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
5. Legacy adapters and the compatibility plan version are removed after wire and result parity is
   proven.
