# zippy-storage

`zippy-storage` owns the new server-side durable data-file boundary. It does not make legacy
`persisted_files` authoritative.

## Legacy import verification

`verify_legacy_parquet_import()` is a trusted startup-only migration helper, not an RPC boundary.
It opens an absolute legacy file below its optional canonical root descriptor component by
component with `NOFOLLOW|NONBLOCK`, verifies regular-file identity, Parquet footer, logical Arrow
Schema, row count, and full `sha256-v1` content, then derives a stable file ID. The resulting DTO is
accepted only through the Catalog's non-cloneable, instance-bound startup import session.
The verifier computes the Catalog canonical hash from the same Arrow Schema compared with the
Parquet footer and writes a reserved proof field; the Catalog transaction requires that proof to
match the table command. `CatalogActor` remains audit metadata and is not authorization. Before
verification, Server planning must sort files by the legacy Gateway key
`(writer_epoch, source_segment_id, source_generation, created_at, file_path)` and assign the
resulting contiguous position to `discovery_order`; the original Registry index is retained only
for audit diagnostics.

The Server V2 planner/executor is wired to this verifier. It first performs the Catalog's bounded
request-ID replay probe. A committed probe result is only a hint: because it contains no physical
file fingerprint, the Server must compare every plan-derivable response identity, revision,
table/snapshot shape, file count, legacy revision, and known row count before classifying it as a
replay. Only `Missing` at the plan's expected Catalog revision is allowed to scan and hash the
physical files and submit the import transaction. Consequently the initial commit pays the full
legacy-file scan, while an exact replay does not repeat migration I/O.

The verifier cannot make an externally owned pathname immutable after it returns. Startup
orchestration must keep legacy roots quiescent/read-only until the Catalog transaction terminates,
and query execution reopens and rechecks the recorded checksum for every external reader. This is
still a mutable-file/TOCTOU boundary, not an immutable storage guarantee. Authority cutover must
remain blocked until external files are copied or reflinked into managed immutable storage and
opened through the managed capability. The current helper does not make SQLite or Gateway
authoritative by itself.

## Candidate protocol

`ParquetCandidateWriter` writes one immutable native file to:

```text
parquet/<table_id>/<file_id>.parquet
```

The writer freezes Parquet settings, embeds typed provenance in the footer, and computes a
`sha256-v1` checksum. The controller's parallel writer stage retains each file as an affine
temporary-file token. Final no-clobber publication and parent-directory fsync happen only after the
Catalog idempotency probe authorizes a new advance and the FIFO coordinator owns a shared Catalog
publication permit. A post-rename directory-sync failure is classified separately from an
unpublished failure, so the controller retains cleanup authority for the final directory entry.
The public `write_candidate()` compatibility API still publishes directly, but does not transfer
controller cleanup authority. Only the Catalog may create committed visibility.

## Commit protocol

`CatalogCommitCoordinator` is a single FIFO worker because the SQLite Catalog serializes manifest
mutations. For each accepted native persist request it:

1. builds one frozen `CommitDataFiles` envelope with `expected_revision=None` and a table-scoped
   `expected_snapshot_version` CAS;
2. probes the complete envelope through `CatalogIdempotencyRead` before final publication;
3. returns the original receipt for an exact replay, rejects a fingerprint conflict, and rejects a
   missing historical replay while dropping every staged temporary file;
4. only for a missing `Advance`, publishes candidates and writes the exact envelope to
   `wal/catalog-commit-intents/`;
5. verifies candidate size, checksum, Parquet row count, and footer identity;
6. submits the Catalog command and replays it unchanged on commit-unknown errors;
7. on a definitive Catalog rejection, durably removes the intent, then consumes non-cloneable
   cleanup tokens under an exclusive Catalog publication guard, only after revision-stable Catalog
   and complete physical identity rechecks authorize deletion;
8. validates a successful response's request, table, snapshot parent, operation, watermark, and
   commit LSN;
9. durably removes the committed intent record.

Cancellation after queue admission abandons only the caller response. A retained intent is replayed
before the coordinator accepts new work after restart. Resource pressure, unavailability,
cancel/deadline responses, internal failures, and data corruption are never inferred to be safe
business rejections. Failure to remove a definitively rejected intent remains fatal and retains the
recovery boundary.

`PersistController` accepts only a coordinator created with `start_with_persistence()`. Publication
permits are acquired by the single FIFO worker only after it dequeues the current staged candidate
set, immediately before physical publication. Later queued jobs hold only affine temporary-file
tokens, so they cannot deadlock an earlier rejected job's exclusive cleanup. The current permit then
moves into the Catalog actor, so cancelling a waiter cannot expose an unowned final file or release
cleanup exclusion while an admitted mutation is still running. Catalog cleanup takes every
publication permit before its first reference read and holds them through checksum/footer
revalidation and durable unlink.

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
`PersistTicket` only after the request is in the bounded queue. `submit_classified()` and
`PersistTicket::wait_classified()` preserve request-local `Rejected` versus runtime-terminal
`Fatal` outcomes for actor supervision; the original methods remain compatibility mappings to
`ServerError`. `submit_with_mode()` carries an explicit `Advance` or `HistoricalReplay`; the
compatibility `submit()` defaults to `Advance`. Dropping a ticket abandons only the response:
candidate publication, Catalog commit
replay, and cleanup continue. A definitive Catalog business rejection removes its durable intent
and leaves controller admission open. Writer, unresolved commit, cleanup, or internal durability
failure closes global admission, but the controller still drains every accepted queue item, joins
all blocking writers, and finally drains its exclusively owned coordinator before returning the
first error. `PersistControllerFailureMonitor` publishes that first terminal error before the
failed ticket wakes; the server supervisor consumes it to revoke native readiness and begin ordered
cleanup instead of continuing with a permanently closed controller.

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

The plain reconciliation report is read-only. Under the explicit pre-admission startup method, the
server may remove scanned candidate/intent `.tmp-*` files and verified orphans only when the
reconciler was constructed from one Catalog object that supplies both file-reference reads and
publication control. Cleanup acquires that Catalog's exclusive publication exclusion before the
first scan and holds it through revision checks, unlink, and the second full reconciliation pass. It
verifies each orphan's complete `PendingDataFile` again immediately before unlink and rejects every
invalid unreferenced candidate as startup corruption. A read-only reconciler cannot enter cleanup,
and a Catalog `Unreferenced` result alone is not deletion authority. The coordinator may remove only
its own rejected affine candidates while holding exclusive publication authority. Ordinary
historical-file removal additionally requires snapshot leases and the safe-GC lifecycle. This pass
scans physical files toward the Catalog.

After replay and final reconciliation, `StartupCatalogFileVerifier` performs the reverse startup
gate. It lists active tables in 512-table pages, reads current snapshots in batches of 512 at one
stable Catalog revision, and fully verifies every currently visible managed or external legacy
file. Managed paths remain descriptor-relative to the retained data-root. External absolute paths
are traversed one component at a time with `O_NOFOLLOW`; verification retains one file descriptor,
checks regular-file type and Catalog size, SHA-256, bounded Parquet footer, exact snapshot Schema,
row count, and the descriptor identity before and after content reads. External paths are never
included in startup errors. Missing files, symlink substitutions, identity changes, and corrupted
content fail the readiness gate. For an `Import` snapshot, the visible manifest's aggregate file,
row, and byte counts must exactly match the strictly typed `added_files`, `added_rows`, and
`added_bytes` summary entries; `legacy_files_revision` must be the only fourth entry and nonzero.
The Catalog reader separately requires a frozen V2 import to be snapshot 2 with parent 1 and a
complete `0..file_count` order of schema-v1 ExternalLegacy descriptors, so startup never verifies a
partially reconstructed or mixed import manifest.
This full check runs on every native startup, including a startup whose migration path accepted a
Catalog replay without rescanning files. The replay optimization therefore never turns readiness
into a metadata-only check.
The default pass admits at most 65,536 active tables, 65,536 current files across both location
classes, 8 GiB of checksummed content, and 128 MiB of retained runtime metadata plus schema data.
The verification report exposes managed, external legacy, and aggregate file counts separately.
`verify_with_bootstrap()` returns one atomic result containing the verification report and every
active table's `TableDescriptor`, current `SchemaDescriptor`, and current `TableSnapshot`, ordered
by table ID. Its metadata budget charges JSON-encoded table/snapshot/schema fields plus raw schema
IPC bytes. During server bootstrap, every decoded Arrow schema adds a conservative retained-memory
charge against that same 128 MiB limit before its table runtime is published. Visible file
descriptors remain batch-local and are not retained in the bootstrap result. Pagination, snapshot
alignment, import-summary validation, physical verification, decoded-schema charging, budget
enforcement, or the final Catalog revision guard failing discards the entire result. The existing
`verify()` API remains a compatibility wrapper that returns only the verification report.
Historical files removed from the current snapshot remain outside this startup gate; maintenance
verification remains responsible for historical/deep audits.

Logical drop, snapshot leases, safe GC, compaction, and public native write admission remain later
refactor steps. Native `TableActor`s now own the per-table persist
state machine and Catalog-backed receipt replay, but the legacy Gateway remains write authority
until the explicit cutover.
