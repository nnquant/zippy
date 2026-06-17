# Stream Table Sharding

`Pipeline.stream_table()` supports first-version append table sharding with
`shard_key` and `shard_nums`. The feature is opt-in. Existing stream tables keep
the single-materializer path unless both sharding parameters are supplied.

## Public API

```python
import pyarrow as pa
import zippy as zp

tick_schema = pa.schema(
    [
        ("instrument_id", pa.string()),
        ("price", pa.float64()),
    ]
)

pipeline = (
    zp.Pipeline("ctp_ingest")
    .source(source)
    .stream_table(
        "ldc_ctp_ticks",
        schema=tick_schema,
        shard_key="instrument_id",
        shard_nums=8,
        persist=False,
    )
    .start()
)

ticks = zp.read_table("ldc_ctp_ticks").collect()
```

Use `shard_key=None, shard_nums=None` for the default non-sharded behavior. This
is also what happens when both arguments are omitted.

## Parameters

`shard_key` is a column name or an ordered list/tuple of column names. Every
column must exist in the table schema. Values are routed with a stable native
hash, not Python `hash()`.

`shard_nums` is either an integer greater than 1 or `"auto"`. `"auto"` resolves
before the pipeline starts to `min(os.cpu_count() or 1, 16)`, and metadata stores
the resolved integer rather than the string.

Invalid combinations fail before streams are registered:

- `shard_key` without `shard_nums`: rejected.
- `shard_nums` without `shard_key`: rejected.
- `shard_nums=1` with `shard_key`: rejected; omit both arguments instead.
- missing shard-key columns: rejected.

First-version sharded stream tables do not support parquet persistence or
retained live segments. Pass `persist=False` or `persist=None` when the master
default enables persistence.

## Logical And Physical Streams

The user-facing table name is logical. For `stream_table("ldc_ctp_ticks",
shard_nums=4)`, Zippy registers:

- `ldc_ctp_ticks`
- `ldc_ctp_ticks.__shard_0000`
- `ldc_ctp_ticks.__shard_0001`
- `ldc_ctp_ticks.__shard_0002`
- `ldc_ctp_ticks.__shard_0003`

Default `list_streams()` hides internal shard streams. Use
`list_streams(include_internal=True)` for diagnostics and maintenance.

Logical metadata includes:

- `zippy_sharded=true`
- `zippy_shard_key`
- `zippy_shard_nums`
- `zippy_shard_mode="hash"`
- `zippy_shard_version=1`
- `zippy_shards`

Physical shard metadata includes `zippy_internal=true`,
`zippy_logical_parent`, and `zippy_shard_index`.

## Reads

`read_table("logical").collect()` detects logical sharded metadata, reads every
physical shard in shard-name order, concatenates the tables, and then applies the
query plan globally.

This first version does not guarantee global time ordering. Use an explicit
`sort(...)` in the query when the result needs deterministic global order.

`Table.explain()` and `Table.profile()` mark this path as `sharded_fanout`.
Row-range and gateway pushdown are not reported for logical sharded reads,
because `head`, `tail`, `slice`, filters, projections, joins, and computed
columns run after concat.

## Subscribe Boundary

Logical sharded subscribe fan-in is not implemented in the first version.

These calls raise a clear runtime error:

```python
zp.subscribe("ldc_ctp_ticks", on_row)
zp.subscribe_table("ldc_ctp_ticks", on_batch)
```

Subscribe to internal shard streams only when the downstream explicitly owns
fan-in and ordering semantics:

```python
zp.subscribe_table("ldc_ctp_ticks.__shard_0000", on_batch)
```

For logical table snapshots, use `read_table(...).collect()`.

## Failure Semantics

Writes are routed and split before shard materialization. If routing fails, for
example because a shard-key value is null, no shard write is attempted.

The first version writes shards sequentially and does not provide cross-shard
rollback. If a later shard write fails after an earlier shard has accepted rows,
the failed shard is recorded in materializer failure state and the caller sees
the error. Treat sharded stream table writes as an append-oriented throughput
tool, not a transactional multi-shard commit.

`drop_table("logical")` drops physical shards first and then the logical table
while sharding metadata is still available. Directly dropping internal shard
streams is rejected by default; drop the logical table instead.
