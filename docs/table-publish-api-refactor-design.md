# Table Publish API Refactor Design

## Background

Zippy currently uses `stream_table()` for more than one table semantic. For normal
streaming outputs, the materialized table is append-oriented. For
`ReactiveLatestEngine`, the materialized table represents the latest row per key and
is backed by a snapshot plus an internal changelog.

This overloading makes the public API harder to reason about:

- `stream_table()` can mean either append history or latest state materialization.
- `subscribe()` follows row offsets for append tables, but key-value tables need
  changelog events.
- `subscribe_table()` is used both for incremental table batches and full snapshot
  callbacks.
- `ReactiveStateEngine` contains stateful factor logic, but that should not be
  confused with a state-style table.

The refactor should make table semantics explicit while preserving compatibility for
existing users.

## Decision

Zippy exposes two table kinds:

```text
append_table
key_value_table
```

`append_table` is an append-only logical table. Existing committed rows are immutable
at the logical row level, row offsets are monotonic, and new data is represented by
appending rows. Storage-layer compaction, retention, parquet rewrite, or segment
cleanup does not violate this contract as long as the logical table remains append-only.

`key_value_table` is a key-value materialized table. It represents the latest value for
each configured key. Its storage model is:

```text
snapshot(version=N, last_applied_changelog_seq=S)
        +
changelog(seq > S)
        =
current key-value state
```

The public API should make this split visible:

```python
session.publish_append_table("factor_events", persist=True)

session.publish_key_value_table(
    "latest_factors",
    by=["instrument_id"],
)
```

Do not add `kv_table` or `keyvalue_table` aliases. The canonical public name is
`key_value_table`.

## Table Kind Semantics

| Table kind | Meaning | `subscribe()` | `subscribe_table()` | `read_table()` |
| --- | --- | --- | --- | --- |
| `append_table` | Append-only history/event table | Newly appended rows | Existing compatible table callback behavior | Point-in-time query over append history |
| `key_value_table` | `key -> latest value` current table | Key-value changelog events | Full latest snapshot callback | Current consistent snapshot |

All tables can be subscribed to. The callback semantic is selected by table kind, not
by a user remembering which engine produced the table.

## Publish API

`Session.publish_append_table(...)` materializes the latest engine output as an
append table:

```python
(
    session
    .engine(factor_engine)
    .publish_append_table("factor_events", persist=True)
)
```

`Session.publish_key_value_table(...)` materializes the latest engine output as a
key-value table:

```python
(
    session
    .engine(factor_engine)
    .publish_key_value_table(
        "latest_factors",
        by=["instrument_id"],
    )
)
```

The method can also derive a key-value table from an existing append table:

```python
(
    session
    .source("factor_events")
    .publish_key_value_table(
        "latest_factors",
        by=["instrument_id"],
        start_from="replay",
    )
)
```

`start_from="replay"` is the default for append-table-derived key-value views. It means
the materializer should rebuild state from the append table's readable history and then
follow the live tail. `start_from="tail"` is allowed only when the caller explicitly
wants a live-only view from the current point forward.

## Compatibility

Keep `Session.stream_table(...)` as a compatibility wrapper:

```text
normal engine + stream_table(name)
  -> append_table(name).publish(...)

ReactiveLatestEngine + stream_table(name)
  -> key_value_table(name, by=engine.by).publish(...)
```

New documentation should prefer `append_table(...).publish(...)` and
`key_value_table(...).publish(...)`. `publish_append_table(...)` and
`publish_key_value_table(...)` remain deprecated aliases for compatibility.
`output_stream=...` remains a legacy shortcut and should follow the same compatibility mapping.

## Engine Support Matrix

| Engine/source | `publish_append_table` | `publish_key_value_table` |
| --- | --- | --- |
| Normal event/factor engine | Supported | Supported when `by` is explicit |
| `ReactiveStateEngine` | Supported | Supported when `by` is explicit |
| `ReactiveLatestEngine` | Not supported | Supported and preferred |
| `TimeSeriesEngine` | Supported | Allowed only with explicit `by` and clear latest-view semantics |
| `BarGeneratorEngine` | Supported | Allowed only with explicit `by` and clear latest-view semantics |
| Existing append table source | Not applicable | Supported |

`ReactiveLatestEngine` must not publish as an append table. Its output does not
represent complete historical events; treating it as append history would create a
misleading half-history table.

`ReactiveStateEngine` should continue to support append tables. Its name refers to
stateful factor computation, not key-value table materialization.

## Metadata

Every registered table descriptor should include:

```text
zippy_table_kind = append_table | key_value_table
```

`key_value_table` descriptors should also include:

```text
zippy_key_value_keys
zippy_key_value_changelog_stream
zippy_key_value_snapshot_version
zippy_key_value_last_changelog_seq
zippy_key_value_schema_version
```

During migration, readers should accept existing key-value metadata fields. Writers for
new tables should emit `zippy_table_kind` and the new `zippy_key_value_*` fields. The
implementation may temporarily dual-write old internal `zippy_kv_*` fields until all
runtime paths have been migrated.

Internal changelog stream names can keep the existing suffix:

```text
<table_name>.__kv_changelog
```

The suffix is an internal storage detail. Public API and documentation should use
`key_value_table`.

## Subscription Rules

`subscribe(source, callback)` is event-oriented:

- For `append_table`, it follows append row offsets and emits newly appended rows.
- For `key_value_table`, it follows the internal changelog stream and emits key-value
  changes. Internal `_zippy_*` fields are hidden from normal callbacks.

`subscribe_table(source, callback)` is table-oriented:

- For `append_table`, it preserves existing compatible table callback behavior.
- For `key_value_table`, it emits full latest snapshots when the snapshot descriptor
  advances.

Do not add a separate `watch_table()` API in this refactor. `subscribe_table()` remains
the public snapshot-watch entry point.

## Replay And Recovery

A key-value table derived from an append table should be replay-backed by default:

```text
append table historical snapshot
        +
append table live tail
        =
key_value_table current state
```

This is the only default that matches user expectations for a current-state table. A
live-only view is valid but must be explicit:

```python
session.source("factor_events").publish_key_value_table(
    "latest_factors",
    by=["instrument_id"],
    start_from="tail",
)
```

The first implementation may stage replay support behind the API boundary, but it must
not silently present a live-only table as a complete replay-backed table.

## Implementation Phases

### Phase 1: API and metadata

- Add `Session.publish_append_table(...)`.
- Add `Session.publish_key_value_table(...)`.
- Add `Session.source(...)` for source-table-derived materialization.
- Write `zippy_table_kind` for newly materialized tables.
- Keep `stream_table()` and `output_stream` as compatibility wrappers.
- Update subscribe detection to prefer `zippy_table_kind`.

### Phase 2: Key-value table alignment

- Route `publish_key_value_table(...)` through `KeyValueTableMaterializer`.
- Keep snapshot, changelog, and watermark metadata in one semantic boundary.
- Hide internal changelog streams from default list APIs.
- Preserve `subscribe()` and `subscribe_table()` behavior for existing key-value users.

### Phase 3: Append-table-derived key-value views

- Support `source("append_table").publish_key_value_table(...)`.
- Implement `start_from="tail"` for explicit live-only views.
- Implement `start_from="replay"` by reading append table history, building initial
  key-value state, then following the live tail.

### Phase 4: Cleanup and documentation

- Move examples from `stream_table()` to the new publish methods.
- Document `append_table` and `key_value_table` as the only public table kinds.
- Deprecate ambiguous examples that imply `ReactiveLatestEngine` can produce append
  history.

## Test Matrix

- `publish_append_table(...)` creates `zippy_table_kind=append_table`.
- `publish_key_value_table(...)` creates `zippy_table_kind=key_value_table`.
- `stream_table()` remains compatible for normal engines.
- `ReactiveLatestEngine.stream_table()` maps to `key_value_table`.
- `ReactiveLatestEngine.publish_append_table(...)` raises a clear error.
- `ReactiveStateEngine.publish_append_table(...)` remains supported.
- `subscribe()` on append tables emits appended rows.
- `subscribe()` on key-value tables emits changelog events and hides internal fields.
- `subscribe_table()` on key-value tables emits full snapshots.
- `read_table()` on key-value tables returns a consistent snapshot.
- `list_streams()` hides internal changelog streams by default.
- `list_streams(include_internal=True)` shows internal changelog streams.

## Open Implementation Notes

- The exact Python signatures should stay small and explicit; avoid overloading by
  accepting too many source shapes in one method.
- `start_from="replay"` needs a clear failure mode if the append source has no readable
  history and no persisted snapshot.
- Metadata migration should be tolerant because older tables may only have the legacy
  key-value semantics marker.
- Table kind is a public contract. It should not be inferred from engine class once the
  descriptor exists.
