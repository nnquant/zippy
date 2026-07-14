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

## Startup reconciliation

`StartupOrphanReconciler` runs after durable intent replay and before table-writer admission. It
scans only the canonical `parquet/<table_id>/<file_id>.parquet` tree, probes both `file_id` and
managed path in one bounded Catalog transaction, and requires one stable Catalog revision across
the whole pass. It reports:

- current or historical Catalog references;
- retained durable intents;
- verified native orphan candidates;
- unpublished temporary files;
- unexpected or invalid unreferenced entries.

The default pass admits at most 65,536 filesystem and intent entries, probes 512 candidates per
Catalog read transaction, retains at most 8 MiB of relative paths, and verifies at most 8 GiB of
candidate content. It also caps serialized durable intents and retained recovery metadata at
128 MiB each, with at most 65,536 candidate descriptors decoded from intents. Directory enumeration
is streaming, so rejected excess entries are not first collected in memory. A retained multi-file
intent remains one atomic group: every file must be either an exact Catalog reference or
unreferenced; a partial Catalog representation is corruption.

The report is read-only. In particular, a Catalog `Unreferenced` result is not permission to unlink
a file because publication could begin after the read transaction. Online orphan removal requires a
publication gate; ordinary file removal additionally requires snapshot leases and the safe-GC
lifecycle. Read-only intent enumeration reports temporary WAL entries rather than cleaning them;
only coordinator recovery performs that cleanup under startup ownership. This pass scans physical
files toward the Catalog; the maintenance verifier remains responsible for the reverse check that
every Catalog-referenced managed path exists and matches its checksum.

Logical drop, snapshot leases, safe GC, compaction, and integration with the authoritative server
table actor remain later refactor steps.
