# Stream Table Sharding Design

## Goal

Add first-class sharding for append stream tables created through
`Pipeline.stream_table(...)`, so high-throughput source ingestion can use multiple
`StreamTableMaterializer` instances while preserving the current single-writer safety
boundary inside each shard.

The first release covers logical append tables only:

```python
zp.Pipeline("ctp_ingest").source(source).stream_table(
    "ldc_ctp_ticks",
    schema=tick_schema,
    shard_key="instrument_id",
    shard_nums=8,
)
```

When `shard_key` and `shard_nums` are omitted, behavior is unchanged and the pipeline
uses one normal `StreamTableMaterializer`.

## Non-Goals

- Do not shard `Session.append_table(...).publish(...)` in the first release.
- Do not shard `key_value_table(...)` or `KeyValueTableMaterializer` in the first
  release.
- Do not implement logical sharded `subscribe(...)` fan-in in the first release.
- Do not implement dynamic resharding or shard rebalance.
- Do not change single-shard `StreamTableMaterializer` writer semantics.
- Do not make the system infer shard keys automatically.

## Public API

Add two keyword arguments to `Pipeline.stream_table(...)`:

```python
def stream_table(
    self,
    name: str,
    *,
    schema: object | None = None,
    shard_key: str | list[str] | tuple[str, ...] | None = None,
    shard_nums: int | Literal["auto"] | None = None,
    ...
) -> Pipeline:
    ...
```

Validation rules:

- `shard_key is None and shard_nums is None`: no sharding.
- `shard_nums == 1`: no sharding; `shard_key` must be omitted.
- `shard_nums` as `int` must be positive.
- `shard_nums > 1` requires `shard_key`.
- `shard_nums == "auto"` requires `shard_key`.
- `shard_key` requires `shard_nums`.
- `shard_key` columns must exist in the resolved input schema.
- `shard_key` values must be non-null when writing rows.

`auto` is resolved at pipeline start to a concrete number:

```text
resolved_shard_nums = min(available_parallelism, 16)
```

The resolved numeric value is persisted in metadata. The string `"auto"` is never
stored in master metadata or persisted descriptors.

## Table Model

The user-facing table name remains a logical table:

```text
ldc_ctp_ticks
```

The runtime creates internal physical shard streams:

```text
ldc_ctp_ticks.__shard_0000
ldc_ctp_ticks.__shard_0001
...
ldc_ctp_ticks.__shard_0007
```

The logical table is the read/query boundary. Users should not have to enumerate shard
streams for normal `read_table(...)` or `Table(...).collect()` use.

Internal shard streams remain visible only when callers explicitly ask for internal
streams or reference the internal stream name directly for diagnostics.

## Metadata

Logical table metadata:

```json
{
  "zippy_table_kind": "append_table",
  "zippy_sharded": true,
  "zippy_shard_key": ["instrument_id"],
  "zippy_shard_nums": 8,
  "zippy_shard_mode": "hash",
  "zippy_shard_version": 1,
  "zippy_shards": [
    "ldc_ctp_ticks.__shard_0000",
    "ldc_ctp_ticks.__shard_0001",
    "ldc_ctp_ticks.__shard_0002",
    "ldc_ctp_ticks.__shard_0003",
    "ldc_ctp_ticks.__shard_0004",
    "ldc_ctp_ticks.__shard_0005",
    "ldc_ctp_ticks.__shard_0006",
    "ldc_ctp_ticks.__shard_0007"
  ]
}
```

Physical shard metadata:

```json
{
  "zippy_table_kind": "append_table",
  "zippy_internal": true,
  "zippy_logical_parent": "ldc_ctp_ticks",
  "zippy_shard_index": 0,
  "zippy_shard_nums": 8,
  "zippy_shard_key": ["instrument_id"],
  "zippy_shard_mode": "hash",
  "zippy_shard_version": 1
}
```

Metadata must be written before data is accepted. If logical table registration or any
physical shard registration fails, the pipeline start fails before source startup.

## Hashing

Shard routing uses a deterministic hash over the normalized shard key tuple:

```text
shard_index = stable_hash(shard_key_values, shard_version=1) % shard_nums
```

Requirements:

- Hash output must be stable across processes, Python/Rust boundaries, and restarts.
- Hash input encoding must include type tags and value bytes to avoid collisions between
  distinct typed values with the same display string.
- `Utf8`, `Int64`, `UInt64`, `Float64`, `Date32`, and timestamp nanosecond values are
  supported in the first release.
- Null shard-key values fail the write before any shard accepts the affected batch.
- Unsupported shard-key types fail pipeline start.

## Write Path

Introduce a `ShardedStreamTableMaterializer` wrapper above normal
`StreamTableMaterializer`:

```text
ShardedStreamTableMaterializer
  -> ShardRouter
  -> shard 0 StreamTableMaterializer
  -> shard 1 StreamTableMaterializer
  -> ...
```

Write flow:

1. Resolve and validate shard configuration.
2. Register the logical table and internal shard streams.
3. Start one `StreamTableMaterializer` per shard.
4. For each input batch, compute shard indexes from `shard_key`.
5. Split the input batch into per-shard sub-batches while preserving row order within
   each shard.
6. Send each non-empty sub-batch to its shard materializer.
7. Flush all touched shards on pipeline flush.
8. Stop all shards on pipeline stop.

The first implementation may process shard sub-batches sequentially behind the wrapper
to reduce scope, but the type and metadata boundary must allow the shard writes to move
to parallel workers without changing the public API or table metadata.

## Failure Semantics

Batch routing is all-or-fail at the logical batch boundary:

- If shard-key extraction fails, no shard is written.
- If splitting succeeds but any shard write fails, the sharded materializer enters a
  failed state and reports the failed shard index and root error.
- The first release does not provide cross-shard atomic rollback after a shard write has
  committed. This must be documented in the error and metrics surface.
- Pipeline health must expose logical table failure and per-shard failure counts.

This matches the existing best-effort live table model while making partial failure
visible instead of silent.

## Read And Collect

`read_table("ldc_ctp_ticks")` and `Table("ldc_ctp_ticks").collect()` detect logical
sharded metadata and fan out to physical shards.

First-release behavior:

- Collect all shards.
- Concatenate shard results.
- Preserve deterministic shard order by shard index.
- Do not promise global time ordering.
- Users who require global order must sort explicitly in the query.

Shard pruning:

- If the query filter includes equality predicates for every shard key column with
  literal values, compute the shard index and query only that shard.
- If the filter cannot be statically mapped to one shard, query all shards.
- Pruning must preserve query correctness; uncertain filters fall back to all shards.

## List And Drop Semantics

Default list APIs hide internal shard streams and return the logical table once.

Drop behavior:

- Dropping a logical sharded table drops all physical shard streams and persisted files
  associated with those shards.
- Dropping an internal shard stream directly is rejected unless an explicit internal
  maintenance flag is supplied.
- Failed partial drop reports the logical table name and each shard outcome.

## Subscribe Boundary

Logical sharded subscribe is not implemented in the first release.

If a caller subscribes to the logical table, the API returns a clear error:

```text
logical sharded subscribe is not implemented; subscribe internal shard streams explicitly
or use collect/read_table
```

Subscribing to an internal shard stream remains possible for diagnostics and advanced
low-level users.

## Compatibility

Existing tables without sharding metadata keep their current behavior.

`shard_key=None, shard_nums=None` must produce the same registration, descriptors,
write path, query path, and persisted layout as current `Pipeline.stream_table(...)`.

`shard_nums=1` is treated as no sharding and must not create internal shard streams.

## Testing

Add focused tests for:

- `Pipeline.stream_table()` rejects incomplete shard configuration.
- `Pipeline.stream_table()` rejects missing or nullable shard key columns.
- `shard_nums="auto"` resolves to a concrete positive number in metadata.
- Same key always maps to the same physical shard.
- Multi-key sharding is deterministic and order-independent across process restarts.
- Logical table registration records all shard metadata.
- Internal shard streams are hidden from default list output.
- Writing a multi-key batch creates per-shard rows whose concatenation equals input rows
  after sorting by a test-only stable row id.
- `read_table(logical)` returns all rows from all shards.
- Equality filter on the shard key prunes to one shard.
- Unsupported logical sharded `subscribe()` fails with the documented error.
- Drop logical table removes all shard metadata and persisted shard files.
- Omitted shard parameters preserve existing non-sharded tests.

## Verification

Required command groups for the implementation phase:

```bash
cargo test -p zippy-core -p zippy-master -p zippy-engines -p zippy-python
PYO3_PYTHON=/home/jiangda/develop/zippy/.venv/bin/python cargo check -p zippy-python
uv run pytest pytests/test_python_api.py pytests/test_python_cli.py
```

Performance validation should include a synthetic multi-instrument ingest benchmark that
compares:

- current non-sharded `Pipeline.stream_table()`;
- sharded `Pipeline.stream_table(shard_key="instrument_id", shard_nums=4)`;
- sharded `Pipeline.stream_table(shard_key="instrument_id", shard_nums="auto")`.

The benchmark must report rows/sec, p95/p99 append latency, per-shard queue depth, and
failed shard writes.
