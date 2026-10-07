"""Buffered native Flight writers used by :class:`zippy_client.Client`."""

from __future__ import annotations

import time
import uuid
from typing import Any, Protocol, cast

from .errors import AmbiguousWriteError, ServerError
from .arrow_values import is_scalar_row, value_to_table


class _FlightWriteTransport(Protocol):
    def write_table(
        self,
        stream_name: str,
        table: Any,
        *,
        namespace: str = "default",
        first_batch_sequence: int = 0,
        durability: str = "wal",
        producer_id: bytes | str | uuid.UUID | None = None,
        connection_id: bytes | str | uuid.UUID | None = None,
        expected_writer_epoch: int | None = None,
    ) -> list[dict[str, object]]: ...

    def close_write(self, connection_id: bytes | str | uuid.UUID) -> None: ...


class FlightWriter:
    """
    Buffer scalar rows and append bounded Arrow batches through Flight DoPut.

    :param stream_name: Target stream name.
    :type stream_name: str
    :param flight: Connected Flight transport.
    :type flight: FlightTransport
    :param schema: Optional Arrow schema used for row conversion.
    :type schema: object | None
    :param batch_size: Maximum buffered scalar rows per flush.
    :type batch_size: int
    :param flush_interval_ms: Optional maximum scalar-row buffering time.
    :type flush_interval_ms: int | None
    :param durability: Required acknowledgement boundary, ``wal``, ``memory`` or ``persisted``.
    :type durability: str
    """

    def __init__(
        self,
        stream_name: str,
        flight: _FlightWriteTransport,
        *,
        namespace: str = "default",
        schema: object | None = None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
        durability: str = "wal",
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval_ms is not None and flush_interval_ms <= 0:
            raise ValueError("flush_interval_ms must be positive or None")
        durability = str(durability).strip().lower()
        if durability not in {"memory", "wal", "persisted"}:
            raise ValueError("durability must be 'memory', 'wal' or 'persisted'")
        self.namespace = str(namespace)
        self.stream_name = str(stream_name)
        self.flight = flight
        self.schema = schema
        self.batch_size = int(batch_size)
        self.flush_interval_ms = flush_interval_ms
        self.durability = durability
        self._producer_id = uuid.uuid4().bytes
        self._connection_id = uuid.uuid4().bytes
        self._writer_epoch: int | None = None
        self._rows: list[dict[str, object]] = []
        self._last_flush_ns = time.perf_counter_ns()
        self._next_batch_sequence = 0
        self._last_receipts: list[dict[str, object]] = []
        self._failed = False
        self._closed = False

    @property
    def last_receipts(self) -> list[dict[str, object]]:
        """Return a copy of the latest acknowledged write receipts."""
        return [dict(receipt) for receipt in self._last_receipts]

    def write(self, value: object) -> None:
        """Write one scalar row or one already batched value."""
        self._ensure_open()
        if is_scalar_row(value):
            self._rows.append(dict(value))
            if len(self._rows) >= self.batch_size or self._flush_interval_elapsed():
                self.flush()
            return
        self.write_batch(value)

    def write_batch(self, value: object) -> None:
        """Send an already batched value immediately."""
        self._ensure_open()
        if is_scalar_row(value):
            raise TypeError("write_batch requires a batched value; use write() for scalar rows")
        self.flush()
        self._send_table(value_to_table(value, self.schema))

    def flush(self) -> None:
        """Flush buffered scalar rows and wait for the configured durability."""
        self._ensure_open()
        if not self._rows:
            return
        rows, self._rows = self._rows, []
        self._send_table(value_to_table(rows, self.schema))
        self._last_flush_ns = time.perf_counter_ns()

    def close(self) -> None:
        """Flush pending rows and close this client-side writer."""
        if self._closed:
            return
        if self._failed:
            try:
                self.flight.close_write(self._connection_id)
            finally:
                self._closed = True
            return
        try:
            self.flush()
            self.flight.close_write(self._connection_id)
        except BaseException:
            self._failed = True
            self._closed = True
            raise
        self._closed = True

    def __enter__(self) -> FlightWriter:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def _send_table(self, table: Any) -> None:
        try:
            receipts = self.flight.write_table(
                self.stream_name,
                table,
                namespace=self.namespace,
                first_batch_sequence=self._next_batch_sequence,
                durability=self.durability,
                producer_id=self._producer_id,
                connection_id=self._connection_id,
                expected_writer_epoch=self._writer_epoch,
            )
        except BaseException as error:
            if isinstance(error, (AmbiguousWriteError, ServerError)):
                self._last_receipts = [dict(receipt) for receipt in error.receipts]
            self._failed = True
            raise
        if receipts:
            # The transport validates every receipt's positive producer epoch.
            self._writer_epoch = cast(int, receipts[-1]["producer_epoch"])
        self._last_receipts = [dict(receipt) for receipt in receipts]
        self._next_batch_sequence += len(receipts)

    def _flush_interval_elapsed(self) -> bool:
        if self.flush_interval_ms is None:
            return False
        elapsed_ns = time.perf_counter_ns() - self._last_flush_ns
        return elapsed_ns >= self.flush_interval_ms * 1_000_000

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Flight writer is closed")
        if self._failed:
            raise RuntimeError(
                "Flight writer is failed after a write rejection or ambiguous partial commit; "
                "inspect receipts and create a new writer"
            )
