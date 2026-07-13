# SQLite Catalog Foundation

Phase P2.1 establishes a non-authoritative SQLite migration foundation at the
managed path `<data_root>/catalog/catalog.db`.

Implemented:

- one dedicated OS actor thread owns the only SQLite connection;
- the actor revalidates managed directories and opens `catalog.db` with
  `SQLITE_OPEN_NOFOLLOW`;
- a bounded actor command queue rejects excess inspection work;
- WAL, `synchronous=FULL`, foreign keys, disabled auto-checkpoint, busy timeout;
- transactional physical migrations with `user_version`, checksum audit, and
  catalog metadata;
- per-version normalized `sqlite_schema` inventory fingerprints, including
  implicit auto-indexes, validated before each migration commits and on reopen;
- VM-step-interruptible startup and explicit `quick_check(1)` through SQLite's
  progress handler; ordinary inspection returns the cached integrity result;
- future physical schema versions fail closed.

Physical SQLite schema versions and `CatalogCommand` wire versions are separate
version spaces. Physical v1 creates only `catalog_meta` and
`catalog_migrations`. It intentionally has no namespace, table, snapshot, file,
idempotency, or maintenance-job domain tables and does not implement the
`Catalog` trait.

Before server integration, P2.2 must freeze the complete typed physical schema,
including lossless unsigned encoding and all table, watermark, provenance,
lineage, and idempotency invariants. It must also add server-lock ownership,
network-filesystem rejection, directory-handle pinning against parent-path
replacement races, process-kill crash failpoints, and an absolute shutdown
deadline. The current reliable `close` may wait indefinitely for an actor
already executing work; dropping the handle invokes the same shutdown path.

The integrity timeout bounds SQLite VM progress-handler checks, not all wall
time spent opening files, waiting in filesystem I/O, or running migrations.
P2.1 therefore does not claim an absolute startup deadline.
