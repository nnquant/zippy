"""Ordered callbacks over native logs with processing-aware recovery.

Callbacks run serially without a background prefetch queue. Durable consumers provide
at-least-once processing: callback side effects must be idempotent across ambiguous ACKs.

:example: subscriber = client.subscribe_table("ticks", print)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import inspect
import math
import threading
import time
from typing import Any
import uuid

from loguru import logger
import pyarrow as pa
import pyarrow.compute as pc

from .errors import ContinuityError, ServerError, ZippyConnectionError
from .log_reader import FlightLogReader, LogCursor, LogEvent
from .row import Row, _table_rows


@dataclass(frozen=True)
class SubscriptionOptions:
    """Configure native callback delivery and explicit recovery boundaries.

    :param namespace: Catalog namespace.
    :param table_id: Immutable table identity instead of a source name.
    :param start: ``latest`` or ``earliest``; ``committed`` requires consumer_id.
    :param cursor: Last successfully processed raw event boundary.
    :param consumer_id: Durable consumer identity; ACK follows successful callbacks.
    :param group_id: Optional shared durable offset authority.
    :param filter: Arrow predicate for append rows, applied before callback delivery.
    :param instrument_ids: Optional append instrument selection, exclusive with filter.
    :param batch_rows: Maximum wire fragment rows; never splits a frame/KV callback.
    :param throttle_ms: Minimum spacing between callback units; no prefetch queue.
    :param count: Keep the latest count rows of each filtered append delivery.
    :param reconnect: Retry transport failures using the processing boundary.
    :param reconnect_interval_ms: Interruptible delay between connection attempts.
    :param open_timeout_sec: Finite timeout for opening each transport connection.
    :param max_pending_bytes: Client atomic-unit assembly budget.
    :param memory_limit_bytes: Optional server query budget.
    :param deadline_ns: Optional absolute UTC deadline, unchanged on reconnect.
    :param expected_schema: Exact raw input schema, checked before starting the callback worker.
    :raises ValueError: Options are inconsistent or out of range.
    :example: options = SubscriptionOptions(start="earliest", batch_rows=1024)
    """

    namespace: str = "default"
    table_id: str | uuid.UUID | None = None
    start: str = "latest"
    cursor: LogCursor | None = None
    consumer_id: str | uuid.UUID | None = None
    group_id: str | uuid.UUID | None = None
    filter: pc.Expression | None = None
    instrument_ids: tuple[str, ...] | None = None
    batch_rows: int | None = None
    throttle_ms: int | None = None
    count: int | None = None
    reconnect: bool = True
    reconnect_interval_ms: int = 100
    open_timeout_sec: float = 5.0
    max_pending_bytes: int = 64 * 1024 * 1024
    memory_limit_bytes: int | None = None
    deadline_ns: int | None = None
    expected_schema: pa.Schema | None = None

    def __post_init__(self) -> None:
        if self.expected_schema is not None and not isinstance(self.expected_schema, pa.Schema):
            raise TypeError("expected_schema must be a pyarrow.Schema")
        if self.start not in {"latest", "earliest", "committed"}:
            raise ValueError("start must be latest, earliest or committed")
        if (self.consumer_id is not None) != (self.start == "committed"):
            raise ValueError("durable consumers require start=committed")
        if self.group_id is not None and self.consumer_id is None:
            raise ValueError("group_id requires consumer_id")
        if self.consumer_id is not None and self.cursor is not None:
            raise ValueError("durable consumers resume from Catalog, not an explicit cursor")
        if self.filter is not None and not isinstance(self.filter, pc.Expression):
            raise TypeError("filter must be a pyarrow.compute.Expression")
        if self.filter is not None and self.instrument_ids is not None:
            raise ValueError("filter and instrument_ids cannot be used together")
        if self.instrument_ids is not None and (
            not isinstance(self.instrument_ids, tuple)
            or any(not isinstance(value, str) for value in self.instrument_ids)
        ):
            raise TypeError("instrument_ids must be a tuple of strings")
        for name in (
            "batch_rows",
            "throttle_ms",
            "count",
            "reconnect_interval_ms",
            "max_pending_bytes",
            "memory_limit_bytes",
            "deadline_ns",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError(f"{name} must be a positive integer")
        if not math.isfinite(self.open_timeout_sec) or self.open_timeout_sec <= 0:
            raise ValueError("open_timeout_sec must be positive and finite")


class FlightSubscriber:
    """Own one ordered native callback worker and its active Flight reader.

    :param source: Source label for diagnostics.
    :param callback: Row or Arrow table callback; exceptions are terminal, never retried.
    :param open_reader: Open a reader from the last processed boundary.
    :param options: Validated native subscription options.
    :param table_callback: Deliver whole event tables; required for frame/KV sources.
    :raises ContinuityError: Reconnection changes the table, schema or kind.
    :example: subscriber = client.subscribe("ticks", print); subscriber.stop()
    """

    def __init__(
        self,
        source: str,
        callback: Callable[[Any], Any],
        open_reader: Callable[[LogCursor | None], FlightLogReader],
        *,
        options: SubscriptionOptions,
        table_callback: bool = False,
    ) -> None:
        if not callable(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError("callback must be a synchronous callable")
        self.source, self.callback = source, callback
        self._open_reader, self.options = open_reader, options
        self._table_callback = table_callback
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._reader: FlightLogReader | None = None
        self._cursor = options.cursor
        self._state = "created"
        self._error: BaseException | None = None
        self._reconnects = 0
        self._rows = 0
        self._batches = 0
        self._last_success_ns = 0
        self._last_delivery = 0.0

    @property
    def cursor(self) -> LogCursor | None:
        """Return the last processed boundary, never the reader's unprocessed delivery cursor."""
        return self._cursor

    def start(self) -> FlightSubscriber:
        """Open synchronously before returning so a latest subscription has a fixed boundary.

        :returns: This running subscriber.
        :raises Exception: Opening or option validation failed; no worker remains active.
        """
        with self._lock:
            if self._state != "created":
                raise RuntimeError("subscriber can only be started once")
            self._state = "starting"
        try:
            self._attach(self._open_reader(self._cursor))
            with self._lock:
                if self._stop.is_set():
                    self._state = "stopped"
                    return self
                self._thread = threading.Thread(
                    target=self._run, name=f"zippy-flight-{self.source}", daemon=True
                )
                self._thread.start()
        except BaseException as error:
            self._error, self._state = error, "failed"
            self._close_reader()
            raise
        return self

    def stop(self) -> None:
        """Cancel transport and join the worker; callbacks must return to finish stopping.

        Calling stop inside a callback is supported and leaves that unit unacknowledged.
        :raises BaseException: The worker's terminal callback or protocol failure.
        """
        self._stop.set()
        self._close_reader()
        with self._lock:
            thread = self._thread
            if thread is None:
                self._state = "stopped"
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        if self._error is not None:
            raise self._error

    def join(self, timeout: float | None = None) -> None:
        """Wait for completion and propagate terminal failures.

        :param timeout: Optional maximum waiting seconds.
        :raises BaseException: The worker's terminal failure.
        """
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        if self._error is not None:
            raise self._error

    def status(self) -> str:
        """Return created, starting, running, reconnecting, stopped or failed."""
        return "failed" if self._error is not None else self._state

    def metrics(self) -> dict[str, object]:
        """Return delivery counters; replayed callbacks are included in delivery totals."""
        state = self.status()
        return {
            "source": self.source,
            "transport": "flight",
            "state": state,
            "running": state in {"starting", "running", "reconnecting"},
            "last_error": None if self._error is None else str(self._error),
            "reconnects_total": self._reconnects,
            "rows_delivered_total": self._rows,
            "batches_delivered_total": self._batches,
            "last_success_ns": self._last_success_ns,
            "continuity_lost": False,
            "processed_next_offset": None if self._cursor is None else self._cursor.next_offset,
            "durable": self.options.consumer_id is not None,
        }

    def health(self) -> dict[str, object]:
        """Return normalized lifecycle and health fields."""
        return _health("flight_subscriber", self.metrics())

    def _attach(self, reader: FlightLogReader) -> None:
        try:
            if self.options.expected_schema is not None and not reader.schema.equals(
                self.options.expected_schema, check_metadata=True
            ):
                raise ContinuityError("subscription input schema differs from expected_schema")
            if self._cursor is not None and (
                reader.table_id != self._cursor.table_id
                or reader.kind != self._cursor.kind
                or not reader.schema.equals(self._cursor.schema, check_metadata=True)
                or reader.cursor.next_offset < self._cursor.next_offset
            ):
                raise ContinuityError(
                    "callback reconnect changed identity, schema or processing boundary"
                )
            if reader.kind != "append" and (
                not self._table_callback
                or self.options.filter is not None
                or self.options.instrument_ids is not None
                or self.options.count is not None
            ):
                raise ValueError("frame/KV require complete, unfiltered table callbacks")
            if reader.durable != (self.options.consumer_id is not None):
                raise ContinuityError("callback reader changed durable mode")
            with self._lock:
                if self._stop.is_set():
                    reader.close()
                    return
                self._reader = reader
                self._cursor = reader.cursor
                self._state = "running"
        except BaseException:
            reader.close()
            raise

    def _close_reader(self) -> None:
        with self._lock:
            reader, self._reader = self._reader, None
        if reader is not None:
            reader.close()

    def _recover(self, error: Exception, *, opening: bool = False) -> bool:
        retryable = isinstance(error, ZippyConnectionError) or (
            isinstance(error, ServerError)
            and (
                error.retryable
                or (opening and self.options.consumer_id is not None and error.code == "CONFLICT")
            )
        )
        if not retryable or not self.options.reconnect:
            raise error
        self._close_reader()
        self._state = "reconnecting"
        self._reconnects += 1
        logger.warning(
            "flight subscription reconnecting source=[{}] error=[{}]", self.source, error
        )
        return not self._stop.wait(self.options.reconnect_interval_ms / 1000)

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                reader = self._reader
                if reader is None:
                    try:
                        self._attach(self._open_reader(self._cursor))
                    except Exception as error:
                        if self._stop.is_set() or not self._recover(error, opening=True):
                            break
                    continue
                try:
                    event = next(reader)
                except Exception as error:
                    if self._stop.is_set() or not self._recover(error):
                        break
                    continue
                if self._stop.is_set():
                    break
                # Deliberately outside transport retry handling: user code must never be
                # retried merely because its exception resembles a connection failure.
                self._deliver(event)
                if self._stop.is_set():
                    break
                if reader.durable:
                    try:
                        reader.acknowledge(event)
                    except Exception as error:
                        if self._stop.is_set() or not self._recover(error):
                            break
                        del event
                        continue
                self._cursor = event.cursor
                del event
        except BaseException as error:
            self._error = error
            logger.exception("flight callback subscription failed source=[{}]", self.source)
        finally:
            self._close_reader()
            self._state = "failed" if self._error is not None else "stopped"

    def _deliver(self, event: LogEvent) -> None:
        table = event.table
        predicate = self.options.filter
        if self.options.instrument_ids is not None:
            predicate = (
                pc.field("instrument_id").isin(self.options.instrument_ids)
                if self.options.instrument_ids
                else pc.scalar(False)
            )
        if predicate is not None:
            table = table.filter(predicate)
        if self.options.count is not None and table.num_rows > self.options.count:
            table = table.slice(table.num_rows - self.options.count)
        if not table.num_rows:
            return
        if self.options.throttle_ms is not None:
            remaining = self._last_delivery + self.options.throttle_ms / 1000 - time.monotonic()
            if remaining > 0 and self._stop.wait(remaining):
                return
        if self._table_callback:
            self._invoke_callback(table)
            self._rows += table.num_rows
            self._batches += 1
        else:
            for values in _table_rows(table):
                if self._stop.is_set():
                    return
                self._invoke_callback(Row(values))
                self._rows += 1
        self._last_success_ns = time.time_ns()
        self._last_delivery = time.monotonic()

    def _invoke_callback(self, value: Any) -> None:
        result = self.callback(value)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("callback returned an awaitable; synchronous completion is required")


def _health(kind: str, metrics: dict[str, object]) -> dict[str, object]:
    state = str(metrics.get("state", "unknown"))
    last_error = metrics.get("last_error")
    running = bool(metrics.get("running", False))
    return {
        **metrics,
        "kind": kind,
        "status": state,
        "healthy": running and state == "running" and not last_error,
        "terminal": state in {"failed", "stopped"},
        "restart_required": state == "failed" or bool(last_error),
    }
