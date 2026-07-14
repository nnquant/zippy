# zippy-master

`zippy-master` is the legacy in-process control plane retained while the native Catalog and
runtime services become authoritative. New product code should depend on the typed adapters rather
than mutate `Registry` directly.

## Legacy Catalog commit events

`legacy_catalog_commit` provides bounded observation for migration shadow workers:

1. Create `LegacyCatalogCommitPublisher::bounded(nonzero_capacity)`.
2. Retain its metrics handle and receiver in the shadow worker.
3. Install the publisher with `MasterServer::install_legacy_catalog_commit_publisher` before the
   Unix or TCP listener starts.
4. Drain events asynchronously. Never perform SQLite, filesystem, logging, or schema conversion on
   the Master request thread.

Each event is a fixed-capacity `Copy` value containing only the previous/committed legacy Catalog
revision range, mutation kind, stable table ID, and a bounded diagnostic stream-name prefix. It
never clones a schema or persisted-file list.

Publication uses `SyncSender::try_send`. A full queue or disconnected receiver cannot delay or fail
the legacy primary response. Either condition permanently sets `continuity_lost`, increments
`continuity_loss_epoch`, and records `last_lost_revision`. The consumer must complete a full
revision-stable reconciliation before trusting incremental events again; reading or logging these
metrics belongs off the primary path.

Installation captures `baseline_revision` while holding the same snapshot and Registry locks used
by commits. Server-owned shutdown is deliberately split into stages:

1. `drain_registered_processes_for_shutdown` notifies producers and waits for their normal
   unregister/persist cleanup while the Master remains readable.
2. `seal_legacy_catalog_commit_events` atomically closes Catalog-mutation admission, waits for
   already admitted mutations, records `final_revision`, and drops the shared sender.
3. The shadow worker drains to receiver disconnection, captures the final stable migration view,
   and completes reconciliation before native Catalog shutdown.
4. `MasterServer::shutdown` stops the listener after shadow reconciliation no longer needs the
   legacy adapter.

Mutation admission uses an atomic in-flight counter. It does not add a mutex or channel wait to
the Master request hot path. Compaction acquires its permit before physical output is written, so
publisher sealing cannot leave a successfully generated but uncommitted compaction file merely
because admission closed between those phases. Standalone listener cleanup still seals the
publisher defensively.

Events are emitted after the legacy Registry mutation and successful JSON snapshot write, but
before releasing the existing `snapshot_lock` and Registry lock. This preserves commit order and
ensures snapshot-write rollback emits no event. An absent snapshot path is the legacy in-memory
durability mode and still counts as a successful primary commit.

Observed Catalog mutations are:

- new stream registration, including its immutable schema;
- source registration or re-registration only when the Catalog revision changes;
- source removal;
- persisted-file publication and full manifest replacement;
- persisted-event history as a representable watermark-only event;
- table drop;
- background compaction manifest replacement.

Process registration, heartbeat, process/source/engine status, reader leases, and engine
registration are runtime-only and do not emit Catalog commit events. The first
`PublishSegmentDescriptor` can initialize a zero writer epoch without a JSON snapshot. That legacy
edge does not emit a false commit event; it explicitly latches continuity loss so the shadow worker
must reconcile.

The event is a wake-up/ordering signal, not the shadow mutation payload. A consumer resolves the
stable table identity against a detached legacy snapshot and then builds one immutable typed
Catalog command outside Master locks. File-list loss or queue overflow always falls back to full
reconciliation.
