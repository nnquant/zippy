# SQLite Catalog

Phase P2.1 established the non-authoritative migration foundation at
`<data_root>/catalog/catalog.db`. Phase P2.2 now adds the physical domain schema
and the first typed command slice, while legacy metadata remains authoritative.

Implemented:

- each Catalog handle confines its own SQLite connection to one dedicated OS
  actor thread;
- the actor revalidates managed directories and opens `catalog.db` with
  `SQLITE_OPEN_NOFOLLOW`;
- a bounded actor command queue rejects excess inspection work;
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
  65,536 visible files, and 262,144 lineage rows before owned allocation.

Physical SQLite schema versions and `CatalogCommand` wire versions are separate
version spaces. Physical v1 remains the immutable migration foundation.
Physical v2 is the complete first domain layout. Physical v3 only adds table
discovery indexes; neither version implies that every V1 command is executable.

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
| preserved legacy discovery-order snapshots | `UnsupportedPlan` pending V2 read DTO |
| alter/drop/schema/manifest/producer/consumer/job commands | `UnsupportedPlan` |
| legacy primary + SQLite shadow fan-out | not connected |
| SQLite read or write authority | disabled |

The physical schema reserves `legacy_discovery_order`, but frozen V1 snapshot
DTOs cannot return that order without violating their ordering contract. Exact
reads support native files and legacy files that do not require preserved
discovery order. A request containing any visible preserve-order legacy file
fails atomically with `UnsupportedPlan`; V2 must carry explicit scan order.

Before server integration, the Catalog must still add remaining table and
manifest command execution, a V2 legacy scan-order read contract, process-level
ownership, network filesystem rejection, directory-handle pinning against
parent replacement races,
process-kill crash failpoints, and an absolute shutdown deadline. The current
reliable `close` may wait indefinitely for an actor already executing work;
dropping the handle invokes the same shutdown path.

The integrity timeout bounds SQLite VM progress-handler checks, not all wall
time spent opening files, waiting in filesystem I/O, or running migrations.
The current implementation therefore does not claim an absolute startup or
shutdown deadline and must not be selected by `zippy server run` yet.
