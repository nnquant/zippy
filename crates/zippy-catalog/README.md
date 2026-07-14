# SQLite Catalog

Phase P2.1 established the non-authoritative migration foundation at
`<data_root>/catalog/catalog.db`. Phase P2.2 now adds the physical domain schema
and the first typed command slice, while legacy metadata remains authoritative.

Implemented:

- each Catalog handle confines its own SQLite connection to one dedicated OS
  actor thread;
- the actor revalidates managed directories and opens `catalog.db` with
  `SQLITE_OPEN_NOFOLLOW`;
- a bounded actor command queue rejects excess read or mutation work;
- WAL, `synchronous=FULL`, foreign keys, disabled auto-checkpoint, busy timeout;
- transactional physical migrations with `user_version`, checksum audit, and
  catalog metadata;
- per-version normalized `sqlite_schema` inventory fingerprints, including
  implicit auto-indexes, validated before each migration commits and on reopen;
- VM-step-interruptible startup and explicit `quick_check(1)` plus
  `foreign_key_check`; ordinary inspection returns the cached integrity result;
- future physical schema versions fail closed;
- physical v2 domain tables for namespaces, tables, schemas, snapshots, files,
  source lineage, producer checkpoints, durable consumer offsets, long-lived
  leases, maintenance jobs, and idempotent responses;
- physical v3 cursor indexes for global and namespace-scoped active-table
  discovery without table scans or temporary sorts;
- physical v4 table-first partial indexes for bounded safe-GC retry/claim
  scans and table-first snapshot-lease protection checks;
- lossless big-endian BLOB encodings for stable IDs, every `u64`, optional
  values, and explicit empty/known/legacy-unknown watermarks;
- a second single-connection actor implementing `CreateNamespace`,
  `CreateTable`, revision, namespace/table get/list/resolve, exact idempotent
  replay, and revision CAS through the stable `Catalog` traits;
- atomic initial table creation: one transaction writes the table descriptor,
  schema version 1, snapshot version 1, catalog revision, and replay response;
- allocation-bounded schema-only Arrow IPC validation before writer admission:
  payloads are capped at 16 MiB, recursive fields at 4,096, nesting at 32,
  metadata at 4,096 entries and 1 MiB, and FlatBuffer traversal at 32,768
  tables and 16 MiB apparent size;
- versioned canonical logical schema hashes (`sha256-v1:<64 hex>`) and
  checksummed canonical JSON BLOBs backed by frozen V1 storage DTOs;
- table discovery is bounded by both item count and bytes: list pages stop at a
  4 MiB raw-descriptor budget and expose a continuation cursor, while atomic
  multi-table resolution rejects responses above that budget; resolution input
  is rejected before queue cloning above 4,096 references or 1 MiB of names;
- table configuration is bounded to 4,096 entries and 1 MiB of key/value text;
- new SQLite Catalog namespace and table writes are bounded to 1 KiB of UTF-8
  bytes without changing frozen V1 decoding; a separate exact encoded-row
  admission check combines the name, definition, and configuration sizes so
  every newly created table descriptor fits the discovery byte budget;
- one `BEGIN IMMEDIATE` transaction serializes the locked version read,
  fingerprint validation, and all remaining migrations, making concurrent
  first-open and v1/v2 upgrade attempts deterministic;
- exact multi-table snapshots read table, schema, snapshot, visible files, and
  native lineage from one deferred transaction. Admission is capped at 4,096
  strictly ordered selectors; each all-or-nothing response is capped at 64 MiB,
  65,536 visible files, and 262,144 lineage rows before owned allocation;
- lifecycle-safe read handles expose `read_pinned_snapshot_at` as the Catalog half of physical
  query execution. It admits an in-process pin before reading exact metadata and returns an owner
  that retains every table pin until drop. The metadata view remains cloneable for planning, so a
  physical QueryService must retain the owner, or an opaque lease derived from it, through every
  scan task instead of treating a cloned view as a lifetime capability. Pin admission is split
  across 64 table-keyed shards. Its configurable defaults are 131,072 active pins and 16,384
  active tables, with hard platform ceilings of 1,048,576 and 131,072 respectively, so unrelated
  tables do not serialize on one mutex and abandoned queries cannot grow unbounded state.
  `SqliteCatalog::runtime_snapshot_pin_status()` and the lifecycle-safe read handle return
  lock-free active gauges, configured limits, and cumulative pin/table budget-exhaustion counters
  without entering a SQLite lane. A retained guard makes shutdown observable as closed admission;
  after the last guard drops, the read handle returns terminal unavailability;
- server-internal frozen V1 manifest commits for native append tables:
  `CommitDataFiles` advances one contiguous persisted watermark, while
  `CommitCompaction` atomically replaces an exact visible input set with
  row- and schema-preserving outputs. Both write the new snapshot before its
  files, preserve exact historical visibility, and commit catalog revision plus
  idempotent response in the same immediate transaction;
- manifest admission before writer-lock acquisition: at most 4,096 new/output
  files, 4,096 compaction inputs, 4,096 lineage rows per file, 65,536 lineage
  rows in total, 64 MiB of encoded metadata per command, and 1 MiB per typed
  partition/statistics/metadata BLOB;
- metadata typed JSON is serialized through a 64 KiB chunked writer with a hard
  1 MiB logical limit, so escaped strings cannot create an unbounded temporary
  JSON allocation. Command fingerprints stream canonical JSON into SHA-256
  rather than allocating the complete escaped command bytes;
- queued manifest commands reserve an estimated in-memory byte charge before
  enqueue. The configurable aggregate queue budget defaults to 128 MiB, so the
  64-entry command queue cannot retain gigabytes of maximum-sized manifests.
  Actor principals are capped at 4 KiB before every command enters the queue
  and are included in each manifest reservation;
- complete command replay probes use an independent bounded read lane and never
  execute mutation preparation;
- `CatalogPersistence` exposes shared publication permits and an exclusive
  cleanup exclusion. Each permit includes bounded command admission and moves
  into the writer actor, so cancellation cannot release it before an admitted
  mutation terminates. The public wrapper is accepted only when its opaque
  payload downcasts to the private capability type issued by that Catalog
  instance; application-created lookalikes are rejected. Cleanup acquires every
  permit before checking file references and retains the exclusion through
  physical unlink;
- `check_runtime_health()` locally verifies that the writer, control-read, and
  snapshot-read threads remain live. A confirmed stopped worker is terminal for
  that Catalog instance and is never classified as an indefinitely retryable
  commit-unknown response;
- `CatalogGarbageCollection` serializes safe-GC with every Catalog mutation.
  Claim atomically checks unexpired durable leases plus the Catalog-owned
  runtime minimum pin and transitions eligible managed historical files to
  `deleting`. Existing retries and new claims have independent nonzero bounds,
  so failed deletions cannot consume new-claim progress. Complete descriptors
  are decoded after the write transaction commits. Exact finalize moves only
  `deleting` files to `deleted`; interrupted deletion leaves durable `deleting`
  rows that are returned again after restart.

Physical SQLite schema versions and `CatalogCommand` wire versions are separate
version spaces. Physical v1 remains the immutable migration foundation.
Physical v2 is the complete first domain layout. Physical v3 adds table
discovery indexes and physical v4 adds safe-GC indexes; neither changes typed
domain encodings or implies that every V1 command is executable.

Historical-manifest reads seek `data_files_visible_by_table` by table, committed
state, and the half-open added/removed snapshot interval. SQLite returns bounded
candidates without `ORDER BY`; the Catalog sorts the bounded result by `file_id`
in Rust, avoiding a SQLite temporary B-tree. Native source lineage is fetched in
chunks of at most 512 file IDs rather than with one query per file.

Current typed capability matrix:

| Capability | Status |
| --- | --- |
| `CreateNamespace` and namespace discovery | implemented |
| `CreateTable` and active table get/list/resolve | implemented transactionally |
| exact modern multi-table query snapshots | implemented transactionally |
| server-internal native append `CommitDataFiles` | implemented transactionally with contiguous watermark and lineage validation |
| server-internal append row-preserving `CommitCompaction` | implemented transactionally with exact input/output coverage |
| durable `AcquireSnapshotLease` / `ReleaseSnapshotLease` | implemented transactionally; incomplete deleting/deleted historical snapshots reject new leases |
| managed historical-file safe-GC claim/finalize | implemented transactionally with durable-lease and runtime-min-pin protection |
| preserved legacy discovery-order snapshots | `UnsupportedPlan` pending V2 read DTO |
| producer-fenced public ingest manifest protocol | not provided by V1 manifest commands |
| maintenance lease-fenced compaction worker protocol | not provided by V1 manifest commands |
| legacy, frame-aware, or key-value materializing manifest mutations | `UnsupportedPlan` pending V2 commands |
| alter/drop/schema/producer/consumer/job commands | `UnsupportedPlan` |
| legacy primary + SQLite shadow fan-out | bounded Gateway comparison worker connected in `sqlite_shadow`; exact legacy manifest import/revision mapping remains pending for nonempty file sets |
| SQLite read or write authority | disabled |

The physical schema reserves `legacy_discovery_order`, but frozen V1 snapshot
DTOs cannot return that order without violating their ordering contract. Exact
reads support native files and legacy files that do not require preserved
discovery order. A request containing any visible preserve-order legacy file
fails atomically with `UnsupportedPlan`; V2 must carry explicit scan order.

The V1 manifest commands are deliberately narrow Catalog primitives, not public
ingest or maintenance-worker protocols. They carry no producer identity, epoch,
batch sequence, maintenance job, or lease-bound worker identity. The envelope
actor is audit metadata, not an RPC authentication or lease-fencing boundary.
The server must complete file and directory durability plus producer fencing
before `CommitDataFiles`; a maintenance coordinator must validate its job lease
before `CommitCompaction`. V1 accepts only native append data. Persisted file
ranges must form the exact contiguous interval after the parent watermark and
end at the requested watermark. Compaction cannot change that watermark, row
count, logical-offset coverage, or aggregate source-lineage coverage of its
exact visible input set. Schema-version-specific logical coverage must also be
identical; schema rewrites require a later command version.

Manifest byte preflight reserves a command slot first, scans exact resident
bytes cooperatively, and yields after every 256 file or metadata work items.
The exact result still controls the aggregate byte budget. Fingerprint
verification and semantic preparation then run on a dedicated bounded worker
before the SQLite writer transaction. Mutations retain actor admission order,
including while a manifest is preparing. Accepted mutations continue after
response cancellation, and synchronous close drains them before closing SQLite.

Control reads and exact snapshot reads use separate bounded queues, OS workers,
and read-only WAL connections. A large snapshot therefore does not block table
discovery, revision reads, or writer commits. Reads may overlap an accepted
mutation and complete first against the previous committed revision;
read-after-write callers must await the mutation response. Queue capacities
default to 64 control reads and 8 snapshot reads. CreateTable schema preparation,
manifest finalization, configurable snapshot parallelism, active-read
interruption, and absolute shutdown deadlines remain future isolation work.

Before server integration, the Catalog must still add remaining table commands,
public producer-fenced ingest and lease-fenced maintenance orchestration, V2
legacy/frame/key-value manifest contracts, a V2 legacy scan-order read contract,
process-level ownership, network filesystem rejection, directory-handle pinning
against parent replacement races, process-kill crash failpoints, and an absolute
shutdown deadline. The current reliable `close` may wait indefinitely for a
worker already executing work; dropping the handle invokes the same shutdown path.

The integrity timeout bounds SQLite VM progress-handler checks, not all wall
time spent opening files, waiting in filesystem I/O, or running migrations.
The current implementation therefore does not claim an absolute startup or
shutdown deadline and must not be selected by `zippy server run` yet.
