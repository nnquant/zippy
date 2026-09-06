"""Protocol-v2 keyed-state consumption with native columnar delta application."""

from __future__ import annotations

import math
import threading
import time
from typing import Any, Callable


class KeyValueState:
    """Maintain state from an atomic snapshot and unfiltered, ordered delta batches.

    :param snapshot: Producer snapshot from ``Session.snapshot_state`` with cursor metadata.
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
    """Bootstrap a lossless delta subscription before loading the producer snapshot.

    :param source: Published KeyValue table name.
    :param snapshot_loader: Callable returning an atomic producer snapshot, locally or by RPC.
    :param master: Optional Zippy master client.
    :param timeout: Maximum subscription-readiness wait in seconds.
    :param max_pending_bytes: Bootstrap and incomplete-version Arrow memory budget.
    :raises RuntimeError: Continuity fails; discard this handle and bootstrap a new one.
    :example:

        subscription = StateSubscription("quotes", lambda: producer.snapshot_state("quotes"))
        subscription.start()
        latest = subscription.snapshot()
        subscription.stop()
    """

    def __init__(
        self,
        source: str,
        snapshot_loader: Callable[[], Any],
        *,
        master=None,
        timeout: float = 5.0,
        max_pending_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0 or max_pending_bytes <= 0:
            raise ValueError("readiness timeout and pending byte budget must be positive")
        self._source = source
        self._snapshot_loader = snapshot_loader
        self._master = master
        self._timeout = timeout
        self._maximum = max_pending_bytes
        self._lock = threading.Lock()
        self._pending: list[Any] = []
        self._pending_bytes = 0
        self._state: KeyValueState | None = None
        self._subscriber: Any = None
        self._error: Exception | None = None
        self._started = False

    def _apply(self, delta) -> None:
        with self._lock:
            try:
                if self._error is not None:
                    raise RuntimeError("state subscription invalidated") from self._error
                if delta.num_rows == 0:
                    return
                if self._state is None:
                    size = delta.get_total_buffer_size()
                    if size > self._maximum - self._pending_bytes:
                        raise RuntimeError("state bootstrap exceeded its byte budget")
                    self._pending.append(delta)
                    self._pending_bytes += size
                else:
                    self._state.apply(delta)
            except Exception as error:
                self._error = error
                self._pending.clear()
                raise

    def start(self) -> StateSubscription:
        """Start delivery, await attachment, then atomically install snapshot plus buffered deltas.

        :returns: This handle.
        :raises RuntimeError: The stream fails or does not implement the delta protocol.
        :raises TimeoutError: Reader attachment does not complete before the deadline.
        """
        from . import subscribe_table

        if self._started:
            raise RuntimeError("state subscription cannot be restarted; create a new handle")
        self._started = True
        self._subscriber = subscribe_table(
            self._source, self._apply, master=self._master, wait=True, timeout=self._timeout
        )
        try:
            deadline = time.monotonic() + self._timeout
            while not self._subscriber.metrics().get("ready"):
                self._require_healthy()
                if time.monotonic() >= deadline:
                    raise TimeoutError("state delta reader did not become ready")
                time.sleep(0.001)
            snapshot = self._snapshot_loader()
            with self._lock:
                self._require_healthy()
                state = KeyValueState(snapshot, max_pending_bytes=self._maximum)
                for delta in self._pending:
                    state.apply(delta)
                self._state = state
                self._pending.clear()
                self._pending_bytes = 0
        except Exception as error:
            self._error = error
            # Preserve the bootstrap failure if the transport also reports its callback error.
            try:
                self._subscriber.stop()
            except Exception as stop_error:
                error.add_note(f"subscriber stop failed error=[{stop_error}]")
            raise
        return self

    def _require_healthy(self) -> None:
        if self._error is not None:
            raise RuntimeError("state subscription requires a fresh bootstrap") from self._error
        if self._subscriber is None:
            raise RuntimeError("state subscription is not started")
        metrics = self._subscriber.metrics()
        if metrics.get("last_error") or metrics.get("reconnects_total", 0):
            raise RuntimeError("state subscription lost continuity; bootstrap again")
        if metrics.get("running") is False or metrics.get("state") in {"stopped", "failed"}:
            raise RuntimeError("state subscription is no longer live")

    def snapshot(self):
        """Request the latest complete state while verifying transport continuity.

        :returns: Arrow Table with its committed cursor.
        :raises RuntimeError: Bootstrap is incomplete or transport/state continuity was lost.
        """
        with self._lock:
            self._require_healthy()
            if self._state is None:
                raise RuntimeError("state bootstrap is not complete")
            return self._state.snapshot()

    @property
    def sequence(self) -> int:
        """Read the healthy committed cursor without allocating a full snapshot.

        :returns: Last completely applied delta sequence.
        :raises RuntimeError: Bootstrap is incomplete or continuity was lost.
        """
        with self._lock:
            self._require_healthy()
            if self._state is None:
                raise RuntimeError("state bootstrap is not complete")
            return self._state.sequence

    def select(self, keys: list[list[str]]):
        """Read only selected keys from a healthy live state.

        :param keys: Composite keys in configured key-column order.
        :returns: Arrow Table containing the requested keys.
        :raises RuntimeError: Bootstrap, key lookup, or continuity validation fails.
        """
        with self._lock:
            self._require_healthy()
            if self._state is None:
                raise RuntimeError("state bootstrap is not complete")
            return self._state.select(keys)

    def stop(self) -> None:
        """Stop the transport and release buffered bootstrap data.

        :returns: None.
        :raises RuntimeError: The transport reports an earlier terminal error.
        """
        if self._subscriber is not None:
            self._subscriber.stop()
        with self._lock:
            self._pending.clear()
            self._state = None
