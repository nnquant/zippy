"""Protocol-v2 keyed-state consumption with native columnar delta application."""

from __future__ import annotations

import math
import threading
import time
from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from zippy_client import Client, FlightLogReader


class KeyValueState:
    """Maintain state from an atomic snapshot and unfiltered, ordered delta batches.

    :param snapshot: Atomic snapshot from ``Client.snapshot_state`` or local Session with metadata.
    :param max_pending_bytes: Maximum retained Arrow bytes for one incomplete version.
    :raises ValueError: Snapshot metadata or configured budget is invalid.
    :example:

        state = KeyValueState(producer.snapshot_state("quotes"))
        state.apply(delta_table)
        latest = state.snapshot()

    Start and buffer the delta subscription **before** requesting the snapshot, then apply
    buffered deltas. Snapshot-covered rows are ignored. On any continuity error, stop using
    the state and repeat that bootstrap sequence. This class does not silently reconnect.
    """

    def __init__(self, snapshot, *, max_pending_bytes: int = 64 * 1024 * 1024) -> None:
        from ._internal import KeyValueState as NativeState

        self._lock = threading.Lock()
        self._state = NativeState(self._snapshot_batch(snapshot), max_pending_bytes)

    @staticmethod
    def _snapshot_batch(snapshot):
        import pyarrow as pa

        if isinstance(snapshot, pa.RecordBatch):
            return snapshot
        if not isinstance(snapshot, pa.Table):
            raise TypeError("snapshot must be an Arrow Table or RecordBatch")
        batches = snapshot.combine_chunks().to_batches()
        if batches:
            return batches[0]
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in snapshot.schema], schema=snapshot.schema
        )

    def apply(self, delta) -> int:
        """Apply deltas without exposing partial versions.

        :param delta: Unfiltered Arrow Table or RecordBatch from a v2 subscription.
        :returns: Number of complete versions applied.
        :raises RuntimeError: A gap, epoch change, malformed batch, or budget overflow occurs.
        """
        import pyarrow as pa

        if isinstance(delta, pa.RecordBatch):
            batches = [delta]
        elif isinstance(delta, pa.Table):
            batches = delta.to_batches()
        else:
            raise TypeError("delta must be an Arrow Table or RecordBatch")
        with self._lock:
            return sum(self._state.apply(batch) for batch in batches)

    @property
    def sequence(self) -> int:
        """Return the last completely applied delta cursor.

        :returns: Committed sequence, excluding buffered partial versions.
        """
        with self._lock:
            return self._state.sequence

    def snapshot(self):
        """Explicitly materialize the complete state.

        :returns: Arrow Table with protocol-v2 cursor metadata.
        :raises RuntimeError: A previous continuity error invalidated this state.
        """
        import pyarrow as pa

        with self._lock:
            return pa.Table.from_batches([self._state.snapshot()])

    def select(self, keys: list[list[str]]):
        """Read selected keys without materializing the full universe.

        :param keys: Composite keys in snapshot-key-column order, e.g. ``[["000001.SZ"]]``.
        :returns: Arrow Table in requested order, without bootstrap metadata.
        :raises RuntimeError: A key is absent or state continuity was lost.
        """
        import pyarrow as pa

        with self._lock:
            return pa.Table.from_batches([self._state.select(keys)])


class StateSubscription:
    """Maintain native KV state from an identity-bound Flight snapshot and continuous log.

    :param source: Native key-value table name.
    :param client: Explicitly owned zippy-client connection; this handle does not close it.
    :param namespace: Catalog namespace.
    :param timeout: Per-bootstrap deadline and maximum stop join time, in seconds.
    :param max_pending_bytes: Native delta and Flight atomic-assembly byte limit.
    :param reconnect: Rebuild state after transport/continuity loss, keeping the same table ID/schema.
    :param max_reconnect_attempts: Maximum consecutive recovery failures before a terminal error.
    :param reconnect_interval_sec: Initial cancellable exponential-backoff delay, capped at five seconds.
    :raises RuntimeError: Bootstrap or state continuity cannot be proved.
    :example:

        with StateSubscription("quotes", client=client, namespace="market") as state:
            selected = state.select([["a"]])

    Recovery discards the old state and obtains a fresh snapshot; intermediate event delivery is
    not guaranteed. Reads fail while recovering instead of returning stale state as current.
    """

    def __init__(
        self,
        source: str,
        *,
        client: Client,
        namespace: str = "default",
        timeout: float = 5.0,
        max_pending_bytes: int = 64 * 1024 * 1024,
        reconnect: bool = True,
        max_reconnect_attempts: int = 5,
        reconnect_interval_sec: float = 0.1,
    ) -> None:
        if not source or not namespace:
            raise ValueError("state table and namespace must be nonempty")
        if not math.isfinite(timeout) or timeout <= 0 or max_pending_bytes <= 512:
            raise ValueError("bootstrap timeout and pending byte budget must be positive")
        if (
            type(max_reconnect_attempts) is not int
            or max_reconnect_attempts < 0
            or not math.isfinite(reconnect_interval_sec)
            or reconnect_interval_sec <= 0
        ):
            raise ValueError("reconnect attempts and interval are invalid")
        self._source, self._namespace, self._client = source, namespace, client
        self._timeout, self._maximum = timeout, max_pending_bytes
        self._reconnect, self._max_attempts = reconnect, max_reconnect_attempts
        self._interval = reconnect_interval_sec
        self._condition = threading.Condition(threading.RLock())
        self._stop_event = threading.Event()
        self._state: KeyValueState | None = None
        self._reader: FlightLogReader | None = None
        self._error: Exception | None = None
        self._phase = "created"
        self._thread: threading.Thread | None = None
        self._table_id: Any = None
        self._schema: Any = None
        self._generation = 0
        self._attempts = 0

    def _bootstrap(self) -> None:
        from zippy_client import ContinuityError

        deadline = time.time_ns() + int(self._timeout * 1_000_000_000)
        options: dict[str, Any] = {"max_pending_bytes": self._maximum}
        if self._table_id is None:
            options.update(source=self._source, namespace=self._namespace)
        else:
            self._client.flight.refresh_read_session(timeout_sec=self._timeout)
            options["table_id"] = self._table_id
        remaining = (deadline - time.time_ns()) / 1_000_000_000
        if remaining <= 0:
            raise TimeoutError("state bootstrap timed out during authentication")
        options["open_timeout_sec"] = remaining
        reader = self._client.open_log(**options)
        try:
            if reader.kind != "key_value":
                raise ValueError("state subscription requires a native key-value table")
            if self._table_id is not None and (
                reader.table_id != self._table_id
                or not reader.schema.equals(self._schema, check_metadata=True)
            ):
                raise ContinuityError("state recovery changed table identity or schema")
            with self._condition:
                if self._stop_event.is_set():
                    raise RuntimeError("state subscription stopped during bootstrap")
                self._reader = reader
            snapshot = self._client.snapshot_state(table_id=reader.table_id, deadline_ns=deadline)
            metadata = snapshot.schema.metadata or {}
            if metadata.get(b"zippy_kv_stream_id") != reader.table_id.hex.encode("ascii"):
                raise ContinuityError("state snapshot belongs to a different log identity")
            state = KeyValueState(snapshot, max_pending_bytes=self._maximum)
            if state.sequence < reader.cursor.next_offset:
                raise ContinuityError("state snapshot cursor precedes its attached log")
            with self._condition:
                if self._stop_event.is_set():
                    raise RuntimeError("state subscription stopped during bootstrap")
                self._table_id, self._schema = reader.table_id, reader.schema
                self._state = state
                self._generation += 1
                self._phase = "running"
                self._condition.notify_all()
        except BaseException:
            reader.close()
            with self._condition:
                if self._reader is reader:
                    self._reader = None
            raise

    def start(self) -> StateSubscription:
        """Attach latest first, load the same table's snapshot, then consume its ordered log.

        :returns: This handle, with a complete initial state already installed.
        :raises RuntimeError: Already started, stopped, or initial bootstrap failed.
        """
        with self._condition:
            if self._phase != "created":
                raise RuntimeError("state subscription cannot be restarted; create a new handle")
            self._phase = "starting"
        try:
            self._bootstrap()
            with self._condition:
                if self._stop_event.is_set():
                    raise RuntimeError("state subscription stopped during startup")
                self._thread = threading.Thread(
                    target=self._run, name=f"zippy-state-{self._table_id}", daemon=True
                )
                self._thread.start()
        except Exception as error:
            with self._condition:
                self._error = error
                self._state = None
                self._phase = "stopped" if self._stop_event.is_set() else "failed"
                self._condition.notify_all()
            reader = self._reader
            if reader is not None:
                reader.close()
            raise
        return self

    def _run(self) -> None:
        from zippy_client import ContinuityError, ServerError, ZippyConnectionError

        while not self._stop_event.is_set():
            try:
                if self._reader is None:
                    self._bootstrap()
                reader = self._reader
                if reader is None:
                    raise RuntimeError("state log reader is missing")
                event = next(reader)
                with self._condition:
                    if self._stop_event.is_set():
                        return
                    if self._state is None:
                        raise RuntimeError("state snapshot is missing")
                    try:
                        self._state.apply(event.table)
                    except RuntimeError as error:
                        raise ContinuityError("native state delta application failed") from error
                    self._attempts = 0
                    self._condition.notify_all()
            except Exception as error:
                reader = self._reader
                with self._condition:
                    self._reader = None
                    self._state = None
                    self._error = error
                    self._attempts += 1
                    retryable = isinstance(
                        error, (ContinuityError, ZippyConnectionError, TimeoutError)
                    )
                    if isinstance(error, ServerError):
                        retryable = error.retryable
                    recover = self._reconnect and retryable and self._attempts <= self._max_attempts
                    self._phase = "recovering" if recover else "failed"
                    if self._stop_event.is_set():
                        self._phase = "stopped"
                    self._condition.notify_all()
                if reader is not None:
                    try:
                        reader.close()
                    except Exception as close_error:
                        error.add_note(f"state reader close failed error=[{close_error}]")
                if self._stop_event.is_set():
                    return
                if not recover:
                    logger.error(
                        "state subscription failed table_id=[{}] error=[{}]", self._table_id, error
                    )
                    return
                logger.warning(
                    "state subscription recovering table_id=[{}] attempt=[{}] error=[{}]",
                    self._table_id,
                    self._attempts,
                    error,
                )
                delay = min(5.0, self._interval * 2 ** min(self._attempts - 1, 10))
                if self._stop_event.wait(delay):
                    return

    def _healthy_state(self) -> KeyValueState:
        if self._phase != "running" or self._state is None:
            raise RuntimeError(f"state subscription is [{self._phase}]") from self._error
        return self._state

    def snapshot(self):
        """Return complete current state; fail while recovering, stopped or invalid.

        :returns: Arrow Table with its exact committed cursor metadata.
        :raises RuntimeError: No healthy live state is available.
        """
        with self._condition:
            return self._healthy_state().snapshot()

    @property
    def sequence(self) -> int:
        """Return the healthy applied sequence without materializing a full snapshot."""
        with self._condition:
            return self._healthy_state().sequence

    def select(self, keys: list[list[str]]):
        """Read selected keys from healthy state without materializing the full table.

        :param keys: Composite keys in the table's declared key order.
        :returns: Arrow Table for the requested keys.
        :raises RuntimeError: State is unavailable or a key does not exist.
        """
        with self._condition:
            return self._healthy_state().select(keys)

    def wait_for_sequence(self, sequence: int, *, timeout: float = 5.0) -> bool:
        """Wait for a complete applied cursor, including any explicit state recovery.

        :param sequence: Minimum one-based applied sequence, or zero for the empty state.
        :param timeout: Maximum monotonic wait in seconds.
        :returns: True when ready at the requested sequence, False on timeout.
        :raises RuntimeError: The subscription has failed or stopped.
        """
        if sequence < 0 or not math.isfinite(timeout) or timeout < 0:
            raise ValueError("sequence and wait timeout must be nonnegative")
        deadline = time.monotonic() + timeout
        with self._condition:
            while True:
                if self._phase in {"failed", "stopped", "created"}:
                    self._healthy_state()
                if (
                    self._phase == "running"
                    and self._state is not None
                    and self._state.sequence >= sequence
                ):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)

    def metrics(self) -> dict[str, Any]:
        """Report state phase, bootstrap generation, retries and the last observed failure."""
        with self._condition:
            return {
                "state": self._phase,
                "ready": self._phase == "running",
                "bootstrap_generation": self._generation,
                "reconnects_total": max(0, self._generation - 1),
                "consecutive_failures": self._attempts,
                "table_id": str(self._table_id) if self._table_id is not None else None,
                "last_error": str(self._error) if self._error is not None else None,
            }

    def stop(self) -> None:
        """Cancel reading/backoff and wait for the worker, leaving the caller's Client open.

        :raises TimeoutError: An in-flight bootstrap RPC did not finish within the join deadline.
        """
        self._stop_event.set()
        with self._condition:
            reader, worker = self._reader, self._thread
            self._phase = "stopped"
            self._state = None
            self._condition.notify_all()
        if reader is not None:
            reader.close()
        if worker is not None and worker is not threading.current_thread():
            worker.join(self._timeout)
            if worker.is_alive():
                raise TimeoutError("state subscription worker did not stop before deadline")

    def __enter__(self) -> StateSubscription:
        """Start and return this explicitly owned state subscription."""
        return self.start()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Cancel the subscription when its context exits."""
        self.stop()
