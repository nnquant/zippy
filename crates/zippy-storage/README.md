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

## Persist controller

`PersistController` is the non-cloneable owner of native Parquet execution and the
`CatalogCommitCoordinator`. Its cloneable handle can only perform immediate bounded admission:
every accepted request first reserves an outstanding-job permit, a conservative retained
request-memory charge, a per-table single-flight permit, and one queue slot. A request larger than
the total memory budget is a nonretryable `ResourceExhausted`; temporary table, job, memory, or queue
pressure is retryable. Shared
Arrow buffers referenced by multiple batches are deliberately charged once per batch, so admission
never relies on fragile buffer-identity deduplication.

The defaults are 64 outstanding jobs, 512 MiB of retained request input, and four concurrent
blocking Parquet writers. Hard limits are 4,096 jobs, 64 GiB, and 64 writers. `submit()` returns a
`PersistTicket` only after the request is in the bounded queue. Dropping that ticket abandons only
the response: candidate publication, Catalog commit replay, and cleanup continue. Any writer or
commit error closes global admission, but the controller still drains every accepted queue item,
joins all blocking writers, and finally drains its exclusively owned coordinator before returning
the first error.

## Startup reconciliation

`StartupOrphanReconciler` runs only while table-writer admission is closed. The server composition
root executes it once before durable intent replay, preserving atomic multi-file intent evidence,
and once after replay, producing the revision-stable report used for readiness. Each pass scans only
the canonical `parquet/<table_id>/<file_id>.parquet` tree, probes both `file_id` and managed path in
one bounded Catalog transaction, and requires one stable Catalog revision across the whole pass. It
reports:

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

The reconciliation report is read-only. In particular, a Catalog `Unreferenced` result is not
permission to unlink a file because publication could begin after the read transaction. Online orphan removal requires a
publication gate; ordinary file removal additionally requires snapshot leases and the safe-GC
lifecycle. Read-only intent enumeration reports temporary WAL entries rather than cleaning them;
only coordinator recovery performs that cleanup under startup ownership. This pass scans physical
files toward the Catalog.

After replay and final reconciliation, `StartupCatalogFileVerifier` performs the reverse startup
gate. It lists active tables in 512-table pages, reads current snapshots in batches of 512 at one
stable Catalog revision, and fully verifies every currently visible managed file. Verification
opens each path relative to the retained data-root and table directory descriptors with
`O_NOFOLLOW`, then reuses the candidate size, checksum, Parquet footer, and row-count invariants.
Missing files, symlink substitutions, and corrupted content fail startup with
`DataCorruption`. The default pass admits at most 65,536 active tables, 65,536 current managed
files, 8 GiB of checksummed content, and 128 MiB of retained runtime metadata plus schema data.
`verify_with_bootstrap()` returns one atomic result containing the verification report and every
active table's `TableDescriptor`, current `SchemaDescriptor`, and current `TableSnapshot`, ordered
by table ID. Its metadata budget charges JSON-encoded table/snapshot/schema fields plus raw schema
IPC bytes. During server bootstrap, every decoded Arrow schema adds a conservative retained-memory
charge against that same 128 MiB limit before its table runtime is published. Visible file
descriptors remain batch-local and are not retained in the bootstrap result. Pagination, snapshot
alignment, physical verification, decoded-schema charging, budget enforcement, or the final
Catalog revision guard failing discards the entire result. The existing `verify()` API remains a
compatibility wrapper that returns only the verification report. External legacy paths and
historical files removed from the current snapshot are outside this startup gate; maintenance
verification remains responsible for historical/deep audits.

Logical drop, snapshot leases, safe GC, compaction, TableActor persist-state integration, and public
native write admission remain later refactor steps.
