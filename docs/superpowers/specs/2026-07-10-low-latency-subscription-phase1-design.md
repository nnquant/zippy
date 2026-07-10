# Low-Latency Subscription Phase 1 Design

## Goal

Reduce avoidable tail latency in the local stream subscription path without changing
callback ordering, callback thread ownership, public APIs, or stream recovery semantics.

Phase 1 covers three related defects:

1. A non-`xfast` subscriber waits on the descriptor-update condition variable before it
   waits on the active segment mmap notification. Normal row commits only wake the mmap
   notification, so delivery can be delayed by the full polling interval.
2. A table subscription with `throttle_ms` only checks its flush deadline when another
   table arrives. A partial batch followed by silence therefore remains pending until a
   later batch or subscriber shutdown.
3. The current performance harness can report queue-enqueue latency and offered input
   rate as if they were processing latency and sustained throughput.

The implementation must preserve deterministic callback delivery and make the measured
latency correspond to work observed at the consumer boundary.

## Non-Goals

- Do not change `xfast` busy-spin behavior.
- Do not move table filtering, concatenation, or batching into Rust.
- Do not execute user callbacks from a Python timer or an additional dispatcher thread.
- Do not change public `subscribe()` or `subscribe_table()` signatures.
- Do not redesign segment rollover, retention, native source descriptor caching,
  runtime queue locking, sharding, or reactive-expression evaluation in this phase.
- Do not introduce hard microsecond assertions into ordinary unit-test CI.

## Current Failure Modes

### Row wake ordering

For an unsealed active segment with no currently readable rows, the driver performs the
following waits:

```text
descriptor update Condvar wait(poll_interval)
mmap notification wait(0)
```

A normal row commit increments the shared `notify_seq` and wakes futex waiters, but it
does not wake the descriptor-update condition variable. The subscriber therefore cannot
observe the row until the unrelated descriptor wait expires.

### Silent throttled batch

`_TableSubscriptionCallback` appends filtered input to `_pending` and evaluates
`_should_emit()` only from `__call__`. If `_pending_rows` stays below `batch_size` and no
new input arrives, the elapsed `throttle_ms` deadline is never evaluated.

### Misleading performance signals

`inproc-timeseries` measures the duration of `handle.write()`, which is primarily queue
admission time. It flushes later and reports the final queue depth, so a run can build a
backlog during the measured interval and still finish with depth zero. Offered input rows
per second can also hide a slow drain after the generator stops.

## Chosen Approach

Use the native subscriber loop as the only scheduler for pending table flushes. The
Python adapter exposes private deadline operations, while Rust caches the deadline and
shortens the next mmap wait. User callbacks continue to run serially on the native
subscriber thread.

Rejected alternatives:

- A `threading.Timer` is smaller but changes callback thread ownership and can execute a
  callback concurrently with the subscriber.
- A dedicated Python dispatcher serializes callbacks but changes exception propagation,
  stop behavior, and delivery latency.
- Full Rust batching removes Python scheduling overhead but duplicates current PyArrow
  filtering and concatenation behavior and has a substantially larger compatibility
  surface.

## Subscriber Wait Protocol

The driver keeps the existing check-before-wait pattern:

1. Read the active segment `notify_seq`.
2. Attempt `read_available()`.
3. If rows are available, return them immediately.
4. If no rows are available, apply ready descriptor updates without blocking.
5. Inspect the sealed flag.

For an unsealed segment, the driver waits directly on the mmap notification using the
effective idle timeout. It must not first block on the descriptor condition variable.
`wait_for_notification_after()` rechecks `notify_seq` before entering futex wait, so a
commit between steps 1 and 5 cannot be lost.

When the mmap wait observes a notification, the driver returns to the outer loop before
applying a descriptor update. The next poll reads the old active segment first, which
preserves the existing rule that committed rows are drained before rebinding. When the
mmap wait times out with no notification, the driver checks descriptor errors and ready
descriptor updates before returning idle. A writer restart that publishes a descriptor
without waking the old mmap is therefore observed no later than the effective idle
timeout.

For a sealed segment, the driver continues to wait for a descriptor update because the
old segment cannot receive additional rows. Segment sealing increments `notify_seq` and
wakes existing mmap waiters, so a subscriber already blocked on the old active segment
transitions to the sealed branch promptly.

## Deadline-Aware Table Flush

`_TableSubscriptionCallback` gains two private operations:

```text
next_flush_delay_ns() -> int | None
flush_due() -> int | None
```

`next_flush_delay_ns()` returns `None` when no throttled rows are pending. Otherwise it
returns the non-negative duration until the current throttle deadline.

`flush_due()` checks the deadline using the same monotonic Python clock as the adapter.
It flushes only when due and returns the next remaining delay, if any. It is safe for the
native loop to call this operation slightly early because clock conversion and scheduler
jitter cannot force an early user callback.

The native subscriber stores an optional Rust `Instant` deadline:

1. Invoke the table adapter with a newly read batch.
2. While the GIL is already held, query `next_flush_delay_ns()` and convert the returned
   relative delay to a Rust `Instant`.
3. Before an idle wait, use `min(poll_interval, deadline_remaining)` as the effective
   timeout.
4. When the deadline is reached, call `flush_due()` from the same native subscriber
   thread and refresh or clear the cached deadline.

No Python call is added to the idle hot path when no throttled data is pending. Row-mode
subscriptions and immediate table subscriptions do not use the deadline machinery.

`stop()` and `join()` retain their existing final flush after the native thread has
stopped. Because there is no timer thread, the final flush cannot race with a scheduled
flush. Callback exceptions raised by a deadline flush propagate through the native
subscriber result in the same way as normal table callback exceptions.

## Effective Timeout

The driver accepts a per-poll effective timeout while retaining its configured
`poll_interval` as the upper bound for descriptor recovery and idle polling:

```text
effective_timeout = min(poll_interval, pending_flush_deadline_remaining)
```

If the deadline is already due, the native loop flushes before entering a blocking wait.
For `xfast`, the driver continues spinning and the loop evaluates the cached deadline
between polls.

## Performance Measurement

### Write-to-subscribe profile

`write-to-subscribe-e2e` becomes a producer/consumer profile:

- The producer commits rows to an active shared-memory segment.
- The consumer runs on a separate thread and blocks on `notify_seq` changes.
- Both threads share one monotonic-clock origin.
- Each benchmark row carries its send time as nanoseconds elapsed from that origin.
- The consumer records latency only after `read_available()` exposes the committed row.

This measures commit, notification, wake, memory visibility, and read availability. It
does not claim to measure Python object conversion or user callback execution; those are
covered by the Python integration test and can receive a separate profile later.

The profile fails when any of the following is true:

- produced and consumed row counts differ;
- the consumer times out;
- the latency sample count differs from the measured consumed-row count;
- configured p95 or p99 thresholds are exceeded.

Latency reports add sample count and maximum latency while retaining p50, p95, and p99.

### In-process timeseries profile

The existing `handle.write()` duration remains useful as queue-admission latency but is
not described as processing completion latency. Sustained throughput includes the final
drain duration. The profile records maximum observed queue depth during the drive rather
than using only the final, post-flush depth, and it verifies that all accepted rows were
processed.

### Workflow boundary

Deterministic unit and integration tests run in normal correctness CI. The release-mode
performance workflow adds `write-to-subscribe-e2e` with zero-loss and p95/p99 gates.
Numeric tail-latency gates remain in the scheduled or explicitly dispatched performance
workflow so shared PR runners do not turn scheduler noise into flaky correctness tests.

## Error Handling

- Shared-memory read, notification, and descriptor errors retain their current error
  types and terminate the subscriber thread visibly.
- A Python deadline-method error is treated as a callback failure and is recorded in
  subscriber metrics.
- A user exception from `flush_due()` stops the subscriber exactly like an exception
  from a data-triggered callback.
- The performance consumer returns an explicit timeout or count-mismatch error rather
  than silently producing fewer latency samples.

## Testing Strategy

Implementation follows test-driven development.

1. Add a Rust regression test using a real active segment. Configure a long idle timeout,
   commit a row from another thread after the reader starts waiting, and demonstrate that
   the current driver waits for the old descriptor timeout.
2. Change the wait ordering and verify that the same test wakes through the mmap
   notification well before the configured idle timeout.
3. Add Python adapter tests for deadline calculation, due flush, early no-op, and final
   flush idempotence.
4. Add a Python integration test that writes one partial table batch followed by silence
   and verifies one callback after `throttle_ms` without a second write.
5. Add stop/join coverage proving the pending batch is delivered once and no background
   scheduler survives shutdown.
6. Add performance-harness unit tests for sample-count mismatch, consumer timeout,
   maximum queue-depth gating, and latency summary fields.
7. Run the same release-mode `write-to-subscribe-e2e` command before and after the code
   change and retain p50, p95, p99, maximum, sample count, and row counts.

Timing assertions in correctness tests use a wide separation between the delayed writer
and the configured idle timeout. They prove notification ordering, not a production
microsecond service-level objective.

## Compatibility

- Public Python signatures and return types remain unchanged.
- Callback order remains FIFO for each subscriber.
- Running callbacks remain serialized on the native subscriber thread.
- `poll_interval_ms` remains the maximum non-`xfast` idle wait.
- Existing JSON report fields remain present; new latency and maximum-depth fields are
  additive.
- Row subscriptions, snapshot subscriptions, and immediate table callbacks preserve
  their existing behavior.

## Rollout And Acceptance

Phase 1 is accepted when:

- the new regression tests fail on the pre-fix implementation and pass after the fix;
- the targeted Rust and Python suites pass;
- formatting, linting, and typing checks for touched code pass;
- the release performance profile consumes every produced row without timeout;
- before/after results show the non-`xfast` wake no longer tracks the configured polling
  interval;
- no existing callback-thread, descriptor-rebind, or shutdown tests regress.

Rollover/retention optimization begins only after these checks pass and Phase 1 results
are recorded.
