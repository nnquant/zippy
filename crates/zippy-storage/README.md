# zippy-storage

`zippy-storage` owns the new server-side durable data-file boundary. It does not make legacy
`persisted_files` authoritative.

## Candidate protocol

`ParquetCandidateWriter` writes one immutable native file to:

```text
parquet/<table_id>/<file_id>.parquet
```

The writer freezes Parquet settings, embeds typed provenance in the footer, computes a
`sha256-v1` checksum, fsyncs the file, publishes without overwrite, and fsyncs the parent
directory. Its result is a `PendingDataFile`; only the Catalog may create committed visibility.

## Commit protocol

`CatalogCommitCoordinator` is a single FIFO worker because the SQLite Catalog serializes manifest
mutations. For each accepted request it:

1. builds one frozen `CommitDataFiles` envelope with `expected_revision=None` and a table-scoped
   `expected_snapshot_version` CAS;
2. writes the exact envelope to `wal/catalog-commit-intents/` and fsyncs it;
3. verifies candidate size, checksum, Parquet row count, and footer identity;
4. submits the Catalog command;
5. replays the unchanged envelope on commit-unknown errors;
6. validates the returned request, table, snapshot parent, operation, watermark, and commit LSN;
7. durably removes the intent record.

Cancellation after queue admission abandons only the caller response. A retained intent is replayed
before the coordinator accepts new work after restart.

Default outstanding budgets are 64 commits, 128 MiB of frozen command bytes, and 8 GiB of
uncommitted candidate bytes. Candidate-byte accounting rounds each request up to 64 KiB. These are
backlog budgets, not Parquet writer memory limits.

Orphan discovery, logical drop, snapshot leases, safe GC, compaction, and integration with the
authoritative server table actor remain later refactor steps.
