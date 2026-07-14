# zippy-ingest

`zippy-ingest` owns native per-table runtime identity and serialized mutation admission. It has no
dependency on the legacy master, Gateway writer map, or `StreamTableMaterializer`.

At startup, `TableRuntimeRegistry` atomically validates the Catalog-derived active table, schema,
snapshot, writer epoch, and committed logical watermark. `TableActorRegistry` then starts one
serial actor per table with two bounded admission layers:

- a 64-command mailbox per table by default;
- a shared 4,096-command budget across all tables by default.

The registry owner is not cloneable. Service adapters receive cloneable lookup handles, while the
owner alone closes admission and drains every accepted command during shutdown. A dropped response
waiter does not cancel an admitted command. Actor queue saturation returns retryable
`ResourceExhausted`; stopped actors return structured `Unavailable`; actor task failure is surfaced
during drain.

The current actor command set is intentionally read-only introspection. PersistController,
RecordBatch append, segment rollover, Catalog commit receipts, query snapshot leases, producer
fencing, and ingest WAL are subsequent implementation stages. Until those stages land, the legacy
Gateway remains write authority.
