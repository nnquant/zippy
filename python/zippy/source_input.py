"""Ordered Python source delivery into the native compute path.

A source owns its acquisition thread. Each sink call completes computation and output
confirmation synchronously, so source delivery cannot outrun the configured batch budget.
"""

from __future__ import annotations

from collections.abc import Callable
import math
import threading
from typing import Protocol, runtime_checkable

import pyarrow as pa


class _SourceCancelled(RuntimeError):
    """Stop an in-flight cooperative producer without recording a data failure."""


class SourceSink(Protocol):
    """Receive an ordered hello, Arrow data, flush and terminal source event."""

    def emit_hello(self, stream_name: str, protocol_version: int = 1) -> None:
        """Declare the source identity and current event protocol."""
        ...

    def emit_data(self, value: pa.Table | pa.RecordBatch) -> None:
        """Deliver one complete Arrow unit and wait for its output confirmation."""
        ...

    def emit_flush(self) -> None:
        """Flush buffered kernel windows without closing the source."""
        ...

    def emit_stop(self) -> None:
        """Declare successful end of input; no subsequent events are allowed."""
        ...

    def emit_error(self, reason: str) -> None:
        """Declare a terminal source error."""
        ...


@runtime_checkable
class SourceHandle(Protocol):
    """Own acquisition resources; stop requests cancellation and join waits for exit."""

    def stop(self) -> None:
        """Request cancellation without waiting on a sink callback."""
        ...

    def join(self) -> None:
        """Wait for all source workers and propagate acquisition failures."""
        ...


@runtime_checkable
class SourcePlugin(Protocol):
    """Provide an exact Arrow schema, identity and explicitly owned acquisition handle.

    :example: ComputePipeline(client, kernel, source=ParquetReplaySource(path), ...)
    """

    def _zippy_output_schema(self) -> pa.Schema: ...

    def _zippy_source_name(self) -> str: ...

    def _zippy_start(self, sink: SourceSink) -> SourceHandle: ...


class PluginSubscriber:
    """Adapt a source plugin to the common compute input lifecycle.

    :param source: Plugin owning acquisition; no Master or Gateway registration occurs.
    :param on_data: Synchronous complete-unit callback.
    :param on_flush: Explicit kernel flush callback.
    :param stop_timeout_sec: Maximum join wait after requesting source cancellation.
    :raises ValueError: Source identity or join deadline is invalid.
    """

    def __init__(
        self,
        source: SourcePlugin,
        on_data: Callable[[pa.Table], None],
        on_flush: Callable[[], None],
        *,
        stop_timeout_sec: float = 5.0,
    ) -> None:
        self.source = source._zippy_source_name()
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("source plugin name must be a nonempty string")
        if not math.isfinite(stop_timeout_sec) or stop_timeout_sec <= 0:
            raise ValueError("source stop timeout must be positive and finite")
        self._plugin, self._on_data, self._on_flush = source, on_data, on_flush
        self._timeout = stop_timeout_sec
        self._delivery = threading.Lock()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._cancel = threading.Event()
        self._done = threading.Event()
        self._phase = "created"
        self._error: BaseException | None = None
        self._hello = False
        self._ended = False
        self._handle: SourceHandle | None = None
        self._worker: threading.Thread | None = None
        self._rows = 0
        self._batches = 0

    def start(self) -> PluginSubscriber:
        """Start acquisition once, accepting synchronous startup emissions.

        :returns: This input handle.
        :raises BaseException: Startup or a synchronous sink callback failed.
        """
        with self._lock:
            if self._phase != "created":
                raise RuntimeError("source plugin can only be started once")
            self._phase = "running"
        try:
            handle = self._plugin._zippy_start(self)
            if not isinstance(handle, SourceHandle):
                raise TypeError("source plugin must return a stop/join handle")
            self._handle = handle
            self._worker = threading.Thread(
                target=self._watch,
                args=(handle,),
                name=f"zippy-source-owner-{self.source}",
                daemon=True,
            )
            self._worker.start()
            if self._error is not None or self._cancel.is_set():
                self.stop()
        except BaseException as error:
            self._fail(error)
            raise
        return self

    def _watch(self, handle: SourceHandle) -> None:
        try:
            handle.join()
            with self._lock:
                if not self._ended and not self._cancel.is_set() and self._error is None:
                    raise RuntimeError("source exited without a terminal event")
                if self._error is None:
                    self._phase = "stopped"
        except BaseException as error:
            self._fail(error)
        finally:
            with self._condition:
                self._done.set()
                self._condition.notify_all()

    def _fail(self, error: BaseException) -> None:
        if isinstance(error, _SourceCancelled) and self._cancel.is_set():
            return
        with self._condition:
            self._error = self._error or error
            self._phase = "failed"
            self._condition.notify_all()

    def _require_delivery(self, *, hello: bool = False) -> None:
        if self._cancel.is_set():
            raise _SourceCancelled("source input is closed status=[stopped]")
        with self._lock:
            if self._error is not None:
                raise RuntimeError("source input has failed") from self._error
            if self._ended or self._phase != "running":
                raise RuntimeError("source input is not running")
            if not hello and not self._hello:
                raise RuntimeError("source data requires hello before delivery")

    def emit_hello(self, stream_name: str, protocol_version: int = 1) -> None:
        """Validate identity and protocol before permitting any data."""
        with self._delivery:
            try:
                self._require_delivery(hello=True)
                if (
                    self._hello
                    or stream_name != self.source
                    or type(protocol_version) is not int
                    or protocol_version != 1
                ):
                    raise ValueError("source hello has duplicate or invalid identity/protocol")
                self._hello = True
            except BaseException as error:
                self._fail(error)
                raise

    def emit_data(self, value: pa.Table | pa.RecordBatch) -> None:
        """Process a complete Arrow unit without a prefetch queue or implicit casts."""
        with self._delivery:
            try:
                self._require_delivery()
                table = (
                    pa.Table.from_batches([value]) if isinstance(value, pa.RecordBatch) else value
                )
                if not isinstance(table, pa.Table):
                    raise TypeError("source data must be an Arrow Table or RecordBatch")
                self._on_data(table)
                self._rows += table.num_rows
                self._batches += 1
            except BaseException as error:
                self._fail(error)
                raise

    def emit_flush(self) -> None:
        """Confirm all current kernel window outputs before returning to acquisition."""
        with self._delivery:
            try:
                self._require_delivery()
                self._on_flush()
            except BaseException as error:
                self._fail(error)
                raise

    def emit_stop(self) -> None:
        """End input without closing the caller-owned Client or compute writer."""
        with self._delivery:
            if self._cancel.is_set():
                return
            try:
                self._require_delivery()
                with self._lock:
                    self._ended = True
                    self._phase = "stopped"
            except BaseException as error:
                self._fail(error)
                raise

    def emit_error(self, reason: str) -> None:
        """Record a terminal acquisition failure; join/stop propagate it."""
        if self._cancel.is_set() and reason == "source input is closed status=[stopped]":
            return
        self._fail(RuntimeError(f"source acquisition failed reason=[{reason}]"))

    def status(self) -> str:
        """Return created, running, stopped or failed."""
        with self._lock:
            return self._phase

    @property
    def cursor(self) -> None:
        """Plugins have no native log cursor or durable checkpoint."""
        return None

    def metrics(self) -> dict[str, object]:
        """Report completed delivery counts and the first terminal error."""
        with self._lock:
            return {
                "source": self.source,
                "transport": "python_source",
                "state": self._phase,
                "rows_delivered_total": self._rows,
                "batches_delivered_total": self._batches,
                "last_error": None if self._error is None else str(self._error),
            }

    def join(self, timeout: float | None = None) -> None:
        """Wait for acquisition exit and propagate its original terminal failure."""
        with self._condition:
            self._condition.wait_for(
                lambda: self._done.is_set() or self._error is not None, timeout
            )
            if self._error is not None:
                raise self._error

    def stop(self) -> None:
        """Cancel the producer and wait for its workers before releasing the input.

        :raises TimeoutError: Producer workers did not exit within the join deadline.
        :raises BaseException: The first source or callback failure.
        """
        with self._condition:
            if self._phase == "created":
                self._cancel.set()
                self._phase = "stopped"
                self._done.set()
                self._condition.notify_all()
                return
        self._cancel.set()
        if self._handle is not None:
            try:
                self._handle.stop()
            except BaseException as error:
                self._fail(error)
            if not self._done.wait(self._timeout):
                timeout_error = TimeoutError("source workers did not stop before the join deadline")
                self._fail(timeout_error)
                raise timeout_error
        with self._lock:
            if self._error is None:
                self._phase = "stopped"
        if self._error is not None:
            raise self._error
