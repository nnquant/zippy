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
- VM-step-interruptible pre-migration and post-migration `quick_check(1)` plus
  `foreign_key_check`, so a corrupt old Catalog is not advanced before startup rejects it; ordinary
  inspection returns the cached integrity result;
- future physical schema versions fail closed;
- physical v2 domain tables for namespaces, tables, schemas, snapshots, files,
  source lineage, producer checkpoints, durable consumer offsets, long-lived
  leases, maintenance jobs, and idempotent responses;
- physical v3 cursor indexes for global and namespace-scoped active-table
  discovery without table scans or temporary sorts;
- physical v4 table-first partial indexes for bounded safe-GC retry/claim
  scans and table-first snapshot-lease protection checks;
- physical v5 adds the dedicated permanent `legacy_import_requests` replay table in O(1) schema
  work. It does not rebuild or widen `idempotent_requests`; its historical DDL, index roots, and row
  bytes remain untouched. Hard-coded migration-checksum and complete schema-inventory goldens freeze
  physical v1-v4 compatibility;
- physical v6 adds explicit per-table `legacy_files_revision` to native snapshot mappings and a
  complete per-snapshot legacy discovery-order table. Retained files may therefore move to a new
  scan position without mutating their immutable `data_files` row or changing historical reads;
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
  query execution. It admits every in-process table pin before reading exact metadata and returns
  an opaque `SqliteCatalogPinnedQuerySnapshot`. Raw `CatalogQuerySnapshot`,
  `CatalogTableSnapshot`, `DataFileDescriptor`, and physical locations are deliberately not
  exposed by this pinned API. Planning uses location-free metadata from `PinnedTableManifest`;
  parallel scans clone `PinnedDataFile` tasks and call synchronous `open_reader` on a bounded
  blocking scan worker once per physical file scan, producing independent seek cursors. Executors
  must not open the same external legacy file once per parallel worker because each open performs a
  full checksum pass. Every task and every `PinnedDataFileReader` shares one `Arc` owner that
  retains all pins and the owned data-root capability until the final task or reader drops. Managed
  readers expose bounded positioned `read_at`/`read_exact_at` operations implemented with Unix
  descriptor reads; they do not expose a path, raw `File`, or descriptor clone, and positioned
  reads do not mutate the reader's existing sequential cursor. Query-layer cursors can therefore
  read independent ranges while retaining the same pin.
  Managed files are opened component by component relative to the stable data-root descriptor with
  `NOFOLLOW`, then checked for regular-file type, exact Catalog size, owner, link count, and
  owner-only permissions. External legacy files are reopened with `NOFOLLOW|NONBLOCK`, hashed on
  the same descriptor, compared with the Catalog checksum, and rewound before scanning. This
  transitional correctness check is O(file size) per opened external reader; legacy roots must
  remain quiescent/read-only, and native managed files do not pay this extra full-file hash. Pin
  admission is split across
  64 table-keyed shards. Its configurable defaults are 131,072 active pins and 16,384 active
  tables, with hard platform ceilings of 1,048,576 and 131,072 respectively, so unrelated tables
  do not serialize on one mutex and abandoned queries cannot grow unbounded state.
  `SqliteCatalog::runtime_snapshot_pin_status()` and the lifecycle-safe read handle return lock-free
  active gauges, configured limits, and cumulative pin/table budget-exhaustion counters without
  entering a SQLite lane. A retained task or reader makes shutdown observable as closed admission;
  after the final owner drops, the read handle returns terminal unavailability;
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
- the trusted V2 legacy importer also exposes a fixed-cost request-ID replay
  probe. One read-only transaction returns the observed Catalog revision and
  either a bounded committed response or `Missing`; it does not read file
  descriptors or prove the current migration plan's physical fingerprint. The
  Server therefore treats the result only as a replay hint and must compare every
  plan-derivable response identity, revision, table/snapshot shape, and summary
  value before accepting a replay;
- a cloneable, lifecycle-bound `SqliteCatalogLegacyReconcileHandle` submits trusted V3 online
  full-view reconciles through the same bounded publication, command, byte, and preparation gates.
  The transaction performs per-table legacy-revision, schema, and writer-epoch CAS; classifies
  retained/new/removed files; rejects immutable identity drift and removed identity resurrection;
  creates one `Import` snapshot; writes its complete scan order and explicit revision mapping; and
  advances the table and Catalog revision atomically. Exact request replay does not advance either
  revision. Revision jumps are accepted but persisted as `gap` continuity. Retained-file metadata
  comparison ignores only the planner's snapshot-local `legacy_registry_index` and
  `legacy_effective_scan_order` audit fields; all other metadata remains immutable. Creating the
  handle seals startup V2 imports, and retained handles fail terminally after Catalog close;
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
discovery indexes, physical v4 adds safe-GC indexes, and physical v5 adds a
dedicated permanent replay table for the trusted V2 legacy importer. Physical
v6 adds explicit legacy revision mappings and snapshot-local file order. These
versions do not imply that every V1 command is executable.

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
| trusted startup-only V2 legacy table import | Catalog transaction and Server V2 planner/executor are wired behind a non-cloneable instance-bound startup session; only a missing replay at the expected revision performs the initial `zippy-storage` file scan |
| trusted online V3 legacy full-view reconcile for an existing migrated table | implemented transactionally behind a cloneable lifecycle-bound handle, with typed verified files, per-table revision/schema/writer-epoch CAS, exact replay, explicit revision mapping, logical removal, and gap continuity |
| exact legacy revision-to-native snapshot lookup | implemented on the Catalog and lifecycle-safe read handle; callers do not infer snapshots with base-plus-delta arithmetic |
| preserved legacy Gateway scan-order snapshots | implemented without changing raw snapshot order: raw files remain canonical by `file_id`, while snapshot-local ordinals preserve both historical and current pinned query order |
| producer-fenced public ingest manifest protocol | not provided by V1 manifest commands |
| maintenance lease-fenced compaction worker protocol | not provided by V1 manifest commands |
| legacy, frame-aware, or key-value materializing manifest mutations | `UnsupportedPlan` pending V2 commands |
| alter/drop/schema/producer/consumer/job commands | `UnsupportedPlan` |
| legacy primary + SQLite shadow fan-out | startup import, exact mapping, and the existing-table online reconcile primitive are implemented; runtime create-with-full-view for a newly registered nonempty table, complete authority continuity gates, and authority cutover remain pending |
| SQLite read or write authority | disabled |

Raw exact snapshots keep the frozen canonical `file_id` ordering contract. A
preserve-order legacy descriptor carries its explicit `legacy_discovery_order`,
but the raw file vector is not reordered. `read_pinned_snapshot_at` derives the
logical scan order for query planning: preserved legacy files are ordered by
their discovery ordinal, and native/non-preserved files retain canonical
`file_id` order. This separates deterministic Catalog serialization and shadow
comparison from legacy-compatible physical scan scheduling.
The frozen V2 `Import` snapshot is stricter: it must be snapshot 2 with parent 1,
contain only schema-v1 external legacy files with unknown offsets, and preserve a
complete unique discovery range `0..file_count`. Any partial, mixed, or drifted
manifest fails the entire snapshot read as Catalog corruption.
Later V3 reconcile snapshots also use `Import`, but require the immediately previous parent,
strict reconcile summary, a complete snapshot-local external-legacy order, and an exact explicit
revision mapping. An empty authoritative target is valid. They do not masquerade as native persist
or compaction commits.

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

Before authority integration, the Catalog must still add remaining table commands,
public producer-fenced ingest and lease-fenced maintenance orchestration, V2
frame/key-value manifest contracts, network filesystem rejection, directory-handle pinning against
parent replacement races, process-kill crash failpoints, and an absolute shutdown deadline. Step 3
must also replace authority-side access to raw location-bearing snapshots with a lifecycle-owned
capability API, so query planning and physical open cannot bypass the pin owner or retain a pathname
after its lease/capability is gone. The current reliable `close` may wait indefinitely for a worker
already executing work; dropping the handle invokes the same shutdown path.

The V3 reconcile primitive only updates a table that already exists with strict migration
provenance. It deliberately does not solve the runtime race where `RegisterStream` and
`RegisterSource` publish files before an empty V1 table can be mirrored. Complete shadow authority
still requires a trusted online create-with-full-view transaction, or a fail-closed unsupported
gate for that race.

The integrity timeout bounds SQLite VM progress-handler checks, not all wall
time spent opening files, waiting in filesystem I/O, or running migrations.
The current implementation therefore does not claim an absolute startup or
shutdown deadline and must not be selected by `zippy server run` yet.
