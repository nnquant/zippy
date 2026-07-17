# zippy-query

`zippy-query` owns the analytical query engine boundary for the single-node server. It is kept
separate from ingest, table actors, and callback execution so DataFusion work cannot enter the
real-time hot path.

The first integration gate pins DataFusion 44.0.0. That release declares Arrow/Parquet 53.3.0
compatible dependencies; the Zippy lock file resolves both DataFusion and the workspace to one
Arrow/Parquet 53.4.1 graph. Unit tests pass workspace `RecordBatch` values directly through
DataFusion types and SQL execution while asserting that cloned arrays retain the same value buffer.
Any future dependency update that creates two incompatible Arrow types therefore fails at compile
time instead of adding a serialization fallback.

Only Parquet support and recursive plan protection are enabled. Compression codecs, nested,
crypto, regular-expression, and other optional expression families remain off until a supported
query surface requires them.

The repository currently builds with Rust 1.86. Keep the transitive `comfy-table` dependency locked
to 7.1.4 until that compiler baseline is intentionally raised; 7.2.2 uses syntax not accepted by
the current toolchain. Recursive protection also requires `psm` 0.1.28: newer 0.1.31 resolves an
archive-writer build dependency that requires Rust 1.88. Dependency updates must run the locked
offline tests before changing either pin.

Current scope:

- isolated per-session `SessionContext` creation;
- explicit optimizer target-partition policy, which is not a hard resource limit;
- file-range repartitioning is disabled so DataFusion cannot split one admitted file task into
  unaccounted parallel scan ranges;
- Arrow ABI and SQL execution compatibility gates;
- a path-free `AdmittedRandomAccessFile` contract and Parquet `ChunkReader` adapter whose source
  and every derived cursor share one owner. The server implementation places the complete
  QueryService task permit behind that owner, so a cursor cannot continue physical I/O after CPU,
  concurrency, or snapshot-pin accounting has been released. Every materialized Parquet range
  first obtains a separate exact memory permit; `Bytes::from_owner` retains that permit through all
  zero-copy clones and releases it only with the final clone.
- an `AdmittedAsyncParquetReader` that moves range and metadata work onto Tokio blocking workers.
  Each in-flight worker retains the admitted source and a shared capacity-one operation permit even
  if its async future is dropped, preventing detached blocking jobs from bypassing the file task's
  CPU admission. All returned ranges keep the same exact result-memory permits as the synchronous
  adapter. One admitted source must not be scanned concurrently through both its synchronous and
  asynchronous adapters because the operation gate covers async blocking jobs only.
- capability-free `PersistedTableDescription`, `PersistedFileKey`, and
  `PersistedFileDescription` manifests. `PersistedFileKey` binds protocol-neutral
  `TableId`/`FileId` values to exact snapshot scan ordinals; the controlled description
  constructors add nonzero versions, sizes, row counts, and an Arrow schema, but no physical file
  location, reader, pinned object, callback, runtime, or arbitrary extension. Logical Arrow
  schema metadata is preserved and is not treated as a physical locator. Ordinals must be
  contiguous in Catalog scan order, and file sizes and aggregate statistics are checked against
  `usize` before DataFusion 44 can truncate them internally. Each table description retains the
  Catalog-computed decoded-schema memory charge for execution admission.
- a planning-only `ZippyTableProvider` that emits `ParquetExec` plans over synthetic opaque object
  keys. Each `FileMeta` carries its typed pure file description as an extension. Filters are
  declared `Inexact`: Parquet may use the predicate for pruning, while DataFusion retains an exact
  `FilterExec`. The sealed reader factory always fails closed, so planning performs no I/O and the
  resulting plan cannot perform persisted-file I/O without a server-owned snapshot binding stage.
  Empty or optimizer-proven no-I/O plans may still execute. DataFusion 44 exposes one
  `PartitionedFile.extensions` slot, so this typed identity currently occupies the same slot
  otherwise used by `ParquetAccessPlan`; row-group access plans must be composed into a future
  Zippy-owned extension instead of overwriting the identity.

The server projects these descriptions snapshot-wide from one exact pinned Catalog owner after
QueryService task admission. It verifies query-level table ordering and uniqueness, descriptor and
file identities, visibility, versions, and the decoder-produced schema charge before retaining a
capability-free manifest.

Persisted planning and binding are now admitted end to end. A conservative parse-independent
preflight task is reserved before SQL profiling; the resulting immutable profile then supplies the
exact build peak and retained-plan charge used by asynchronous preparation and whole-tree binding.
Binding replaces every planning marker exactly once and rejects missing, duplicate, ordinary, or
mismatched Parquet scans. The actual `ParquetExec`, not only the session defaults, has page-index and
bloom-filter reads disabled and retains row-group pruning. Bind-time validation rejects either
option being re-enabled.

The server's non-cloneable `NativeExecutionLease` consumes the original admitted snapshot manifest
and atomically rebuilds its retained reservation as a bound physical-plan reservation. Bound reader
factories hold only a weak execution authority, so retaining or cloning a DataFusion plan cannot
extend query or snapshot lifetime. Successful finish, explicit failure, and drop all close reader
admission before publishing or preserving the terminal state and releasing plan ownership.

Each persisted file starts with a 64 KiB task-memory reservation. Its deferred open reads and
validates the exact 8-byte Parquet trailer, computes the serialized footer size, and atomically grows
the same query/session/process task reservation to `64 KiB + footer_bytes * 64` before DataFusion
parses metadata. A rejected growth leaves every ledger unchanged. Page indexes and bloom metadata
remain disabled, which bounds this charge to the retained footer model while preserving row-group
statistics pruning.

The server now has an internal pull-based native persisted result stream. Each execution reserves a
QueryService `OperatorArena` permit covering the configured pool plus execution-context overhead,
and constructs it under an additional transient build allowance. DataFusion runs with a
`GreedyMemoryPool` exactly matching the configured operator pool; spill is disabled, so an operator
that exceeds the admitted pool fails instead of writing to an uncontrolled directory. The server
also retains an execution-driver task permit whose CPU slots equal the session's target partition
count. Every returned Arrow batch receives its own conservative array, schema,
and control-memory permit and keeps that permit until the downstream owner drops the batch.
Reaching EOF first drops
the DataFusion stream and operator arena, then marks execution complete; the query becomes
`Finished` only after already-delivered batches drain. Dropping the stream publishes cancellation
before releasing execution resources.

This result stream remains an internal canary and is not advertised by Gateway capabilities. An
Arrow batch is currently materialized by DataFusion immediately before its result permit is
admitted, so the operator arena bounds the production step but there is not yet a strict
batch-specific pre-admission guarantee. Active-segment execution, a process-wide DataFusion memory
pool, quota-enforced spill/temp storage, transport backpressure, and Gateway routing remain required
before native execution can replace the legacy query engine. The footer multiplier covers valid
Zippy-managed Parquet files; treating arbitrary hostile local files as input would additionally
require a bounded Thrift decoder or process/allocator isolation.
