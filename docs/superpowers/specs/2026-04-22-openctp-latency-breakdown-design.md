# OpenCTP Cross-Process Latency Breakdown Design

**Date:** 2026-04-22

## Goal

Measure where the current `1ms+` OpenCTP cross-process latency comes from, using a reproducible local benchmark that decomposes latency into source batching, bus transfer, and engine dispatch segments before making any optimization.

## Problem Statement

`zippy-openctp` already stamps each tick with `localtime_ns`, which is the local receive time sampled in the source process. The current end-to-end complaint is that OpenCTP data still shows `1ms+` cross-process latency, but the codebase does not expose enough segment-level timing to tell whether the delay is caused by:

- source-side batching or batch materialization
- master bus transfer and reader wake-up policy
- engine scheduling after the batch is read

Optimizing before splitting these segments would be guesswork.

## Non-Goals

- Changing the semantic meaning of `dt` or `localtime_ns`
- Replacing the bus transport with a notification-based design in this task
- Tuning live OpenCTP networking or CTP vendor callbacks
- Building a production telemetry pipeline or long-term metrics backend

## Current Constraints

- `zippy-openctp` source defaults to `rows_per_batch=1` and `flush_interval_ms=0`
- bus reader already supports `xfast=true` for busy-spin and `xfast=false` for `sleep(10us)`
- the relevant live path is `OpenCTP source -> StreamTableEngine -> master bus -> downstream reader/engine`
- the repository currently lacks a dedicated benchmark that reports per-segment latency

## Recommended Approach

Implement a local, reproducible latency harness with explicit timepoints at each handoff boundary, then run four controlled experiments to isolate each segment. Only after the measurements identify the dominant segment should we change the implementation.

This is the most reliable path because it turns a single opaque `1ms+` symptom into four directly comparable latency distributions.

## Considered Alternatives

### Option 1: Only measure existing end-to-end delay

Add one downstream timestamp and compare it with `localtime_ns`.

Pros:

- smallest code change
- closest to the user-visible symptom

Cons:

- cannot identify which segment is slow
- does not tell us whether `xfast` matters
- encourages blind optimization

### Option 2: Add segment-level timestamps and local benchmark harness

Add timestamps at source emit, bus read return, and engine entry, then measure in controlled local experiments.

Pros:

- directly identifies the dominant segment
- comparable across `xfast=false` and `xfast=true`
- reproducible without live market dependency

Cons:

- requires targeted instrumentation and benchmark support

### Option 3: Jump straight to system-level tracing

Use `perf` or eBPF before adding in-process timestamps.

Pros:

- can reveal scheduler and syscall behavior
- useful if the slow segment is already known

Cons:

- weak first step because it does not define the segment boundary
- higher setup cost and harder to automate

## Measurement Model

The benchmark will use these local timestamps:

- `t_source_recv_ns`
  Reuse existing `localtime_ns` from the source process. This is the local Unix epoch nanosecond timestamp sampled when the source accepts a tick for batching.
- `t_source_emit_ns`
  Sampled immediately before the source emits the `RecordBatch` to its sink.
- `t_bus_read_ns`
  Sampled immediately after downstream `reader.read()` returns a batch.
- `t_engine_on_data_ns`
  Sampled at downstream engine `on_data()` entry.

Derived latency segments:

- `source_batching_ns = t_source_emit_ns - t_source_recv_ns`
- `bus_transfer_ns = t_bus_read_ns - t_source_emit_ns`
- `engine_dispatch_ns = t_engine_on_data_ns - t_bus_read_ns`
- `end_to_end_ns = t_engine_on_data_ns - t_source_recv_ns`

## Experiment Matrix

All experiments should use:

- one instrument
- one stream
- `rows_per_batch=1`
- `flush_interval_ms=0`
- fixed warmup count, recommended `500`
- fixed sample count, recommended `5000`

### Experiment A: Source Only

Path:

`FakeOpenCtpSourceRuntime -> RecordBatch`

Purpose:

- measure `source_batching_ns`
- verify whether source-side batch construction already contributes a large share of the delay

### Experiment B: Source to Bus Reader, `xfast=false`

Path:

`source -> master bus -> reader.read()`

Purpose:

- measure `source_batching_ns + bus_transfer_ns`
- establish the default-reader baseline

### Experiment C: Source to Bus Reader, `xfast=true`

Path:

`source -> master bus -> reader.read()`

Purpose:

- compare bus transfer against Experiment B
- identify whether reader waiting policy is the dominant bus-side cost

### Experiment D: Source to BusStreamSource to Dummy Engine

Path:

`source -> master bus -> BusStreamSource -> engine.on_data()`

Purpose:

- add `engine_dispatch_ns`
- confirm whether post-read engine scheduling is materially contributing to the `1ms+` complaint

## Output Requirements

Each experiment must report:

- sample count
- `avg`, `p50`, `p95`, `p99`, and `max`
- per-segment latency summary
- the slowest 20 samples

If practical, the harness should also persist raw samples as CSV or Parquet for later analysis.

## Optimization Decision Rules

No implementation optimization should be attempted until the measurements are available.

After the first benchmark run:

- If `source_batching_ns` is large, optimize source-side batch materialization and flush path first.
- If `bus_transfer_ns` is large and `xfast=true` materially improves it, focus on reader waiting policy next.
- If `bus_transfer_ns` is large but `xfast=true` does not materially improve it, inspect shared-ring access, Arrow IPC overhead, and thread handoff before changing policy again.
- If `engine_dispatch_ns` is large, inspect downstream engine entry path and Python/Rust boundary overhead.

## File-Level Design

Expected code changes are split across the isolated worktrees already created for this investigation.

Main repository worktree:

- `crates/zippy-core/src/master_client.rs`
  Reader-side timing support and benchmark helpers if the benchmark lives near the bus client path.
- `crates/zippy-master/tests/` or a dedicated benchmark target
  Local bus latency experiment harness.
- `crates/zippy-python/src/lib.rs` and `pytests/`
  Only if Python-level experiment plumbing is needed.

`zippy-openctp` plugin worktree:

- `crates/zippy-openctp-core/src/source.rs`
  Source emit timestamp instrumentation.
- `crates/zippy-openctp-core/tests/`
  Source-side timing validation.
- `examples/` or a dedicated benchmark module
  Optional experiment driver for local runs.

## Risks

- The benchmark may perturb latency slightly because extra timestamp sampling is not free.
- Cross-process measurements using local clocks assume system time is reasonably synchronized inside the same host.
- Python-based experiment layers may hide Rust-side bottlenecks if used too early; the first pass should stay as close to Rust boundaries as possible.

## Success Criteria

This design is successful when:

- the benchmark can reproduce the reported latency locally without live market dependency
- the output cleanly separates source, bus, and engine segments
- the dominant segment is obvious enough to guide the first optimization without guesswork
