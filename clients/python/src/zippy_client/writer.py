"""Buffered Flight and Gateway writers used by :class:`zippy_client.Client`."""

from __future__ import annotations

import time
from typing import Any, Protocol

from .errors import AmbiguousWriteError
from .protocol import is_scalar_row, table_to_ipc, value_to_table


class _GatewayRequestTransport(Protocol):
    def request(
        self,
        header: dict[str, object],
        payload: bytes = b"",
    ) -> tuple[dict[str, Any], bytes]: ...


class _FlightWriteTransport(Protocol):
    def write_table(
        self,
        stream_name: str,
        table: Any,
        *,
        first_batch_sequence: int = 0,
    ) -> list[dict[str, object]]: ...


class GatewayWriter:
    """
    Buffer scalar rows and send Arrow batches through Gateway.

    :param stream_name: Target stream name.
    :type stream_name: str
    :param gateway: Connected Gateway transport.
    :type gateway: GatewayTransport
    :param schema: Optional Arrow schema used for row conversion.
    :type schema: object | None
    :param batch_size: Maximum buffered scalar rows per request.
    :type batch_size: int
    :param flush_interval_ms: Optional maximum scalar-row buffering time.
    :type flush_interval_ms: int | None
    """

    def __init__(
        self,
        stream_name: str,
        gateway: _GatewayRequestTransport,
        *,
        schema: object | None = None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval_ms is not None and flush_interval_ms <= 0:
            raise ValueError("flush_interval_ms must be positive or None")
        self.stream_name = str(stream_name)
        self.gateway = gateway
        self.schema = schema
        self.batch_size = int(batch_size)
        self.flush_interval_ms = flush_interval_ms
        self._rows: list[dict[str, object]] = []
        self._last_flush_ns = time.perf_counter_ns()
        self._closed = False

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
        """Flush buffered scalar rows."""
        self._ensure_open()
        if not self._rows:
            return
        rows, self._rows = self._rows, []
        try:
            self._send_table(value_to_table(rows, self.schema))
        except BaseException:
            self._rows = [*rows, *self._rows]
            raise
        self._last_flush_ns = time.perf_counter_ns()

    def close(self) -> None:
        """Flush pending rows and close the server-side writer."""
        if self._closed:
            return
        self.flush()
        self.gateway.request({"kind": "close_writer", "stream_name": self.stream_name})
        self._closed = True

    def __enter__(self) -> GatewayWriter:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def _send_table(self, table: Any) -> None:
        self.gateway.request(
            {
                "kind": "write_batch",
                "stream_name": self.stream_name,
                "rows": int(table.num_rows),
            },
            table_to_ipc(table),
        )

    def _flush_interval_elapsed(self) -> bool:
        if self.flush_interval_ms is None:
            return False
        elapsed_ns = time.perf_counter_ns() - self._last_flush_ns
        return elapsed_ns >= self.flush_interval_ms * 1_000_000

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Gateway writer is closed")


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
    """

    def __init__(
        self,
        stream_name: str,
        flight: _FlightWriteTransport,
        *,
        schema: object | None = None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval_ms is not None and flush_interval_ms <= 0:
            raise ValueError("flush_interval_ms must be positive or None")
        self.stream_name = str(stream_name)
        self.flight = flight
        self.schema = schema
        self.batch_size = int(batch_size)
        self.flush_interval_ms = flush_interval_ms
        self._rows: list[dict[str, object]] = []
        self._last_flush_ns = time.perf_counter_ns()
        self._next_batch_sequence = 0
        self._last_receipts: list[dict[str, object]] = []
        self._failed = False
        self._closed = False

    @property
    def last_receipts(self) -> list[dict[str, object]]:
        """Return a copy of the latest memory-durability receipts."""
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
        """Flush buffered scalar rows and wait for memory receipts."""
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
            self._closed = True
            return
        self.flush()
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
                first_batch_sequence=self._next_batch_sequence,
            )
        except BaseException as error:
            if isinstance(error, AmbiguousWriteError):
                self._last_receipts = [dict(receipt) for receipt in error.receipts]
            self._failed = True
            raise
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
                "Flight writer is failed after an ambiguous partial commit; "
                "inspect receipts and create a new writer"
            )
