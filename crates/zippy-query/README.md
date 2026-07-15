# zippy-query

`zippy-query` owns the analytical query engine boundary for the single-node server. It is kept
separate from ingest, table actors, and callback execution so DataFusion work cannot enter the
real-time hot path.

The first integration gate pins DataFusion 44.0.0. That release declares Arrow/Parquet 53.3.0
compatible dependencies; the Zippy lock file resolves both DataFusion and the workspace to one
Arrow/Parquet 53.4.1 graph. Unit tests pass workspace `RecordBatch` values directly through
DataFusion types and SQL execution while asserting that cloned arrays retain the same value buffer.
Any future dependency update that creates two incompatible Arrow types therefore fails at compile
time instead of adding a serialization fallback.

Only Parquet support and recursive plan protection are enabled. Compression codecs, nested,
crypto, regular-expression, and other optional expression families remain off until a supported
query surface requires them.

The repository currently builds with Rust 1.86. Keep the transitive `comfy-table` dependency locked
to 7.1.4 until that compiler baseline is intentionally raised; 7.2.2 uses syntax not accepted by
the current toolchain. Recursive protection also requires `psm` 0.1.28: newer 0.1.31 resolves an
archive-writer build dependency that requires Rust 1.88. Dependency updates must run the locked
offline tests before changing either pin.

Current scope:

- isolated per-session `SessionContext` creation;
- explicit optimizer target-partition policy, which is not a hard resource limit;
- Arrow ABI and SQL execution compatibility gates.

Next scope is a snapshot-bound `TableProvider` over Catalog-issued `PinnedDataFile` readers, followed
by QueryService cancellation, memory-pool, result-stream, and spill integration.
