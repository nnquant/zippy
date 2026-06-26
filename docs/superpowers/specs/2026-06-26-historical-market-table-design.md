# Historical Market Table Design

## Purpose

Zippy needs a more complete historical market data table capability without
turning the project into a general-purpose database. The target workload is
append-only tick, quote, bar, and factor data where reproducibility matters more
than row-level mutation.

The design upgrades persisted stream-table parquet files into a database-like
historical table layer with snapshots, manifests, append transactions,
compaction, and table-level drop.

## Scope

In scope:

- append-only historical tables backed by parquet files
- table metadata with schema, partition spec, sort keys, and table identity
- snapshot-based reads
- atomic append commits
- atomic compaction replacement
- table-level drop with persisted file cleanup
- compatibility with the existing `read_table()` user workflow
- migration path from current `persisted_files` metadata

Out of scope:

- row-level `update`
- predicate `delete`
- cross-table transactions
- long-running SQL transactions
- row-level MVCC
- distributed storage
- full DolphinDB compatibility

## Design Principles

Historical market data is immutable after ingestion. If source data must be
corrected, the system should rebuild a table or partition and switch the visible
table version intentionally. Silent in-place mutation is not allowed because it
breaks replay, audit, and factor reproducibility.

Queries must never depend on directory listing as the source of truth. They read
from a committed snapshot manifest. Files in staging directories are invisible.

Compaction is a physical maintenance operation. It may replace small files with
larger files, but it must not change logical rows.

## Storage Layout

Each historical table has one root directory:

```text
<table_root>/
  _zippy/
    table.json
    current
    snapshots/
      000000000000000001.json
      000000000000000002.json
    manifests/
      manifest-000000000000000001.json
    commits/
      commit-log.jsonl
  data/
    trade_date=20260626/
      instrument_id=IF2606/
        part-<uuid>.parquet
  staging/
    txn-<uuid>/
      part-<uuid>.parquet
```

`table.json` stores stable table metadata:

- `table_id`
- `table_name`
- Arrow schema and schema hash
- partition spec, such as `trade_date` and `instrument_id`
- sort keys, such as `dt`
- created timestamp in UTC
- format version

`current` stores the active snapshot id. It is updated atomically.

Snapshot files store:

- snapshot id
- parent snapshot id
- commit id
- operation type, such as `append`, `compact`, or `drop`
- manifest file references
- created timestamp in UTC
- summary metrics, including rows and file counts

Manifest files store data-file metadata:

- file path
- row count
- file size
- partition values
- schema hash
- source segment identity when available
- writer epoch when available
- min/max stats for pruning, especially `dt`
- optional checksum

## Transactions

The system supports lightweight snapshot transactions. This is not a general SQL
transaction model. It exists to prevent partial append or compaction visibility.

Append commit flow:

1. Create a transaction id.
2. Write parquet files under `staging/txn-<id>/`.
3. Build file metadata and stats.
4. Move completed data files into `data/`.
5. Write a new manifest.
6. Write a new snapshot that references the previous snapshot.
7. Atomically replace `current`.
8. Append a commit-log entry.
9. Remove the empty staging directory.

If the process crashes before `current` is replaced, the transaction is not
visible. Recovery can ignore or clean the staging directory. If the process
crashes after `current` is replaced, the new snapshot is visible and recoverable.

Compaction commit flow:

1. Select candidate files from one committed snapshot.
2. Write compacted files to staging.
3. Write a new manifest replacing only the compacted source files.
4. Commit a new snapshot atomically.
5. Mark source files as obsolete.
6. Delete obsolete files only after they are not required by retained snapshots.

Drop flow:

1. Validate table identity and authorization.
2. Mark the table dropped in catalog metadata or remove the table registration.
3. Persist the metadata change.
4. Delete persisted data files under the table root when `drop_persisted=True`.
5. Report any physical cleanup errors explicitly.

## API

The first public API should be small and explicit:

```python
zippy.db.create_table(
    "ctp_ticks",
    schema=tick_schema,
    partition_by=["trade_date", "instrument_id"],
    sort_by=["dt"],
)

zippy.db.append("ctp_ticks", batch)

zippy.read_table("ctp_ticks") \
    .between("dt", start, end) \
    .filter(zippy.col("instrument_id") == "IF2606") \
    .select("instrument_id", "dt", "last_price", "volume") \
    .collect()

zippy.ops.compact_table("ctp_ticks")
zippy.ops.drop_table("ctp_ticks", drop_persisted=True)
```

Existing `Pipeline.stream_table(..., persist="parquet")` should remain valid.
Internally, it should publish committed historical table snapshots instead of a
flat `persisted_files` list.

The following APIs should not exist in this design:

```python
zippy.db.update(...)
zippy.db.delete_where(...)
```

Historical correction should use rebuild and alias replacement instead:

```python
zippy.db.create_table("ctp_ticks_v2", schema=tick_schema, ...)
zippy.db.import_parquet("ctp_ticks_v2", corrected_files)
zippy.db.swap_alias("ctp_ticks", "ctp_ticks_v2")
zippy.ops.drop_table("ctp_ticks_v1", drop_persisted=True)
```

Alias replacement can be added after the base table format is stable.

## Query Semantics

`read_table()` keeps the existing user-facing contract. It opens a logical table
and returns a lazy table reader.

For historical market tables, query execution should:

1. Read the active snapshot id from `current`.
2. Load the snapshot and manifests.
3. Prune files using partition values and min/max stats.
4. Push column projection and simple filters into parquet scanning.
5. Scan matched parquet files.
6. Merge with live active and sealed segments when the table also has a live
   writer.
7. Apply remaining expressions through the existing query expression layer.

The first implementation can keep using PyArrow and Polars for local execution.
The table format should not depend on those engines so that DataFusion or DuckDB
can be introduced later.

## Catalog Integration

The master registry currently exposes stream metadata, active descriptors,
sealed segments, and `persisted_files`. The new historical layer should add a
historical table descriptor that points to a table root and current snapshot.

During migration, the system can expose both:

- legacy `persisted_files` for compatibility
- new historical table metadata for snapshot-aware readers

Once readers use snapshots, `persisted_files` becomes derived compatibility
metadata rather than the source of truth.

## Error Handling

All write operations must fail closed:

- incomplete staging files are not visible
- invalid manifests are not committed
- schema hash mismatch rejects the commit
- partition value mismatch rejects the commit
- failed snapshot write leaves `current` unchanged
- failed physical cleanup after drop or compaction is reported explicitly

Log messages should include table name, transaction id, snapshot id, and file
path where applicable. Sensitive environment details should not be logged.

## Recovery

Startup recovery should:

- read `current`
- verify the active snapshot exists and is parseable
- ignore staging transactions not referenced by a committed snapshot
- optionally remove old staging directories
- expose explicit health alerts for missing manifests, missing data files, or
  schema mismatches

Recovery should not rebuild committed state from directory listing unless an
operator explicitly runs a repair command.

## Testing

Unit tests:

- table metadata serialization and schema hash stability
- manifest validation
- snapshot parent linkage
- partition value extraction
- file stats generation
- invalid commit rejection

Integration tests:

- append writes are invisible before commit and visible after commit
- crash-before-current-switch leaves no visible partial rows
- compact replaces files without changing query results
- drop table removes catalog metadata and persisted files under the table root
- `read_table()` merges historical snapshot rows and live segment rows
- legacy `persisted_files` compatibility remains available during migration

Performance tests:

- time-range query prunes files by partition and min/max stats
- projection pushdown avoids reading unused columns
- compaction reduces file count without changing row count

## Migration Plan

Phase 1: Add table format types and local metadata read/write.

Phase 2: Teach append persistence to produce snapshots and manifests while still
publishing legacy `persisted_files`.

Phase 3: Teach `read_table()` to prefer historical snapshots when present.

Phase 4: Move compaction to snapshot replacement semantics.

Phase 5: Treat `persisted_files` as compatibility metadata and document the new
historical table API as the preferred interface.

## Success Criteria

- Historical append is atomic at snapshot level.
- Queries never read staging files.
- Compact does not duplicate or lose logical rows.
- Drop table remains the only supported delete operation.
- There is no row-level update API.
- Existing stream-table users can migrate without losing `read_table()` behavior.
- The design can later support SQL execution without changing the table format.
