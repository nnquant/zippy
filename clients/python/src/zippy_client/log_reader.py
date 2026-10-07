"""Bounded native log delivery with atomic frames/mutations and explicit resume cursors.

:example: with client.open_log("ticks", start="earliest") as events: event = next(events)
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import threading
from typing import Any
import uuid

from google.protobuf.message import DecodeError
from loguru import logger
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight

from ._generated import protocol_pb2 as wire
from .errors import AmbiguousAcknowledgementError, ContinuityError, ProtocolError, ServerError


@dataclass(frozen=True)
class LogCursor:
    """Resume after a completely delivered event unit, independently of connection or table name.

    :param table_id: Immutable Catalog identity.
    :param next_offset: Inclusive next offset after complete delivery.
    :param kind: Native table kind.
    :param schema: Exact raw event schema required on reconnect.
    :param version: Last completely delivered KV version, if known.
    :note: This is a delivery cursor, not a durable processing acknowledgement.
    """

    table_id: uuid.UUID
    next_offset: int
    kind: str
    schema: pa.Schema
    version: int | None = None


@dataclass(frozen=True)
class LogEvent:
    """Deliver append rows, one complete frame, or one complete key-value mutation.

    :param table: Arrow table retaining fragment buffers without concatenating their contents.
    :param first_offset: Inclusive first event offset.
    :param last_offset: Inclusive last event offset.
    :param watermark: Highest native offset observed by the final fragment's fixed window.
    :param cursor: Position immediately after this complete delivery.
    :param delivery_sequence: Connection-local durable ACK identity, absent for raw streams.
    """

    table: pa.Table
    first_offset: int
    last_offset: int
    watermark: int
    cursor: LogCursor
    delivery_sequence: int | None = None


class FlightLogReader(Iterator[LogEvent]):
    """Read native logs synchronously; no hidden reconnect, queue, or partial atomic delivery.

    :param reader: Opened PyArrow Flight response reader.
    :param writer: Request writer, kept open when durable acknowledgements are enabled.
    :param query_id: Opening query identity required in the peer response.
    :param start: Initial latest or earliest boundary.
    :param cursor: Previously delivered cursor, if resuming.
    :param max_pending_bytes: Maximum retained Arrow buffer bytes plus fragment overhead.
    :param consumer: Expected durable identity, absent for an explicit-cursor raw stream.
    :raises ProtocolError: Wire metadata or mutation boundaries are malformed.
    :raises ContinuityError: Offsets, table identity, schema or mutation versions diverge.
    :example: event = next(client.open_log("frames", start="earliest"))
    """

    def __init__(
        self,
        reader: Any,
        writer: Any,
        *,
        query_id: bytes,
        start: str,
        cursor: LogCursor | None,
        max_pending_bytes: int,
        consumer: wire.LogConsumer | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._closed = threading.Event()
        self._reading = threading.Lock()
        self._maximum = max_pending_bytes
        self._pending: list[pa.RecordBatch] = []
        self._pending_bytes = 0
        self._unit_end: int | None = None
        self._unit_first = 0
        self._unit_version: int | None = None
        self._remainder: tuple[pa.RecordBatch, wire.LogProgress] | None = None
        self._cursor: LogCursor | None = None
        self.durable = consumer is not None
        self._unacknowledged: LogEvent | None = None
        self._confirmed_event: tuple[int, int | None, int] | None = None
        self._last_delivery_sequence = 0
        try:
            opening = self._read_chunk()
            metadata = _metadata(opening.app_metadata)
            if opening.data is not None or metadata.WhichOneof("payload") != "opened":
                raise ProtocolError("log must begin with one standalone opening message")
            opened = metadata.opened
            if (
                opened.protocol_version != 1
                or opened.query_id != query_id
                or opened.generation == 0
                or len(opened.table_id) != 16
                or opened.table_id == bytes(16)
                or opened.table_kind not in {"append", "frame", "key_value"}
            ):
                raise ProtocolError("log opening identity or kind is invalid")
            self.table_id = uuid.UUID(bytes=opened.table_id)
            self.kind = opened.table_kind
            self.query_id = uuid.UUID(bytes=query_id)
            self.generation = opened.generation
            self.schema: pa.Schema = reader.schema
            self._watermark = _watermark(opened)
            if (
                opened.HasField("consumer") != self.durable
                or opened.HasField("next_offset") != self.durable
            ):
                raise ProtocolError("log opening durable mode differs from the request")
            if self.durable:
                if opened.consumer != consumer or cursor is not None:
                    raise ProtocolError("log opening consumer identity differs from the request")
                self._next = opened.next_offset
                self._version = 0 if self.kind == "key_value" and self._next == 0 else None
            elif cursor is not None:
                if (
                    cursor.table_id != self.table_id
                    or cursor.kind != self.kind
                    or not cursor.schema.equals(self.schema, check_metadata=True)
                ):
                    raise ContinuityError("log resume changed table identity, kind or schema")
                self._next = cursor.next_offset
                self._version = cursor.version
            else:
                self._next = self._watermark + 1 if start == "latest" else 0
                self._version = 0 if self.kind == "key_value" and start == "earliest" else None
            if not 0 <= self._next <= self._watermark + 1:
                raise ContinuityError("log opening watermark precedes the requested cursor")
            self._cursor = self._make_cursor()
        except BaseException:
            self.close()
            raise

    @property
    def cursor(self) -> LogCursor:
        """Return the last complete delivery cursor; incomplete buffered rows never advance it."""
        if self._cursor is None:
            raise RuntimeError("log opening did not complete")
        return self._cursor

    def __iter__(self) -> FlightLogReader:
        """Return this single-consumer iterator."""
        return self

    def __next__(self) -> LogEvent:
        """Read one complete unit or raise; transport failure preserves the preceding cursor."""
        if not self._reading.acquire(blocking=False):
            raise RuntimeError("log reader supports only one concurrent consumer")
        if self._unacknowledged is not None and not self._closed.is_set():
            self._reading.release()
            raise RuntimeError("acknowledge the previous durable delivery before reading another")
        try:
            while not self._closed.is_set():
                batch, progress = self._take_fragment()
                first = progress.first_offset
                if self._unit_end is None:
                    self._unit_first = first
                    self._unit_end, self._unit_version = self._boundary(batch, progress)
                if self.durable and progress.delivery_last_offset != self._unit_end:
                    raise ContinuityError("durable delivery boundary changed within an atomic unit")
                end = self._unit_end
                if end < first or end > self._watermark:
                    raise ProtocolError("log atomic boundary is outside its committed window")
                rows = min(batch.num_rows, end - first + 1)
                part = batch.slice(0, rows)
                if self.kind == "key_value":
                    self._validate_mutation(part, first, end)
                if self.kind == "frame" and (
                    progress.frame_first_offset != self._unit_first
                    or progress.frame_last_offset != end
                ):
                    raise ContinuityError("frame boundary changed before complete delivery")
                if rows < batch.num_rows:
                    remaining = wire.LogProgress()
                    remaining.CopyFrom(progress)
                    remaining.first_offset += rows
                    self._remainder = (batch.slice(rows), remaining)
                self._pending_bytes += part.get_total_buffer_size() + 512
                if self._pending_bytes > self._maximum:
                    raise ContinuityError("log atomic unit exceeds the pending byte budget")
                self._pending.append(part)
                self._next += rows
                if self._next != end + 1:
                    continue
                table = pa.Table.from_batches(self._pending, schema=self.schema)
                self._pending = []
                self._pending_bytes = 0
                self._unit_end = None
                if self._closed.is_set():
                    raise StopIteration
                self._version = self._unit_version
                self._cursor = self._make_cursor()
                sequence = progress.delivery_sequence if self.durable else None
                event = LogEvent(
                    table, self._unit_first, end, self._watermark, self._cursor, sequence
                )
                if self.durable:
                    self._last_delivery_sequence = progress.delivery_sequence
                    self._unacknowledged = event
                return event
            raise StopIteration
        except BaseException:
            self.close()
            raise
        finally:
            if self._closed.is_set():
                self._pending.clear()
                self._remainder = None
                self._pending_bytes = 0
            self._reading.release()

    def acknowledge(self, event: LogEvent, *, request_id: str | uuid.UUID | None = None) -> None:
        """Confirm a completely processed durable event and wait for its persisted receipt.

        :param event: Exact event returned by this connection's last iterator read.
        :param request_id: Optional non-nil idempotency key; generated when omitted.
        :returns: None after Catalog has durably committed the processed position.
        :raises ValueError: The event is foreign, stale, or this stream is not durable.
        :raises AmbiguousAcknowledgementError: Confirmation may have committed; reconnect using
            the same consumer identity to recover its authoritative position.
        :raises ServerError: The service explicitly rejects the confirmation.
        :example: event = next(events); process(event.table); events.acknowledge(event)
        """
        if not self.durable:
            raise ValueError("raw logs do not support durable acknowledgements")
        identity = (id(event), event.delivery_sequence, event.last_offset)
        if identity == self._confirmed_event:
            return
        if event is not self._unacknowledged or event.delivery_sequence is None:
            raise ValueError("acknowledgement requires this connection's pending complete event")
        if self._closed.is_set():
            raise RuntimeError("log reader is closed; reconnect using the same consumer identity")
        if not self._reading.acquire(blocking=False):
            raise RuntimeError("log reader supports only one concurrent operation")
        try:
            identifier = uuid.uuid4() if request_id is None else uuid.UUID(str(request_id))
            if identifier.int == 0:
                raise ValueError("acknowledgement request_id must not be nil")
            expected = wire.LogAcknowledgement(
                request_id=identifier.bytes,
                delivery_sequence=event.delivery_sequence,
                processed_through=event.last_offset,
            )
            try:
                self._writer.write_metadata(
                    wire.LogControl(
                        protocol_version=1,
                        acknowledge=expected,
                    ).SerializeToString()
                )
                chunk = self._read_chunk()
                metadata = _metadata(chunk.app_metadata)
                if (
                    chunk.data is not None
                    or metadata.WhichOneof("payload") != "acknowledged"
                    or metadata.acknowledged != expected
                ):
                    raise ProtocolError("durable acknowledgement receipt differs from the request")
            except BaseException as error:
                self.close()
                if not isinstance(error, Exception):
                    raise
                if isinstance(error, flight.FlightError):
                    from .transports.flight import _translate_flight_error

                    error = _translate_flight_error(error, "DoExchange.acknowledge")
                if isinstance(error, ServerError) and error.code in {
                    "INVALID_ARGUMENT",
                    "SCHEMA_MISMATCH",
                    "CONFLICT",
                    "PERMISSION_DENIED",
                    "NOT_FOUND",
                    "UNSUPPORTED_PLAN",
                }:
                    raise error
                raise AmbiguousAcknowledgementError(
                    "durable acknowledgement outcome is unknown; reconnect with the same consumer",
                    request_id=str(identifier),
                ) from error
            self._confirmed_event = identity
            self._unacknowledged = None
        finally:
            self._reading.release()

    def _make_cursor(self) -> LogCursor:
        return LogCursor(self.table_id, self._next, self.kind, self.schema, self._version)

    def _read_chunk(self) -> Any:
        from .transports.flight import _translate_flight_error

        try:
            return self._reader.read_chunk()
        except flight.FlightError as error:
            raise _translate_flight_error(error, "DoExchange.log") from error
        except StopIteration as error:
            raise ContinuityError("log ended without explicit cancellation") from error

    def _take_fragment(self) -> tuple[pa.RecordBatch, wire.LogProgress]:
        if self._remainder is not None:
            batch, progress = self._remainder
            self._remainder = None
            return batch, progress
        chunk = self._read_chunk()
        metadata = _metadata(chunk.app_metadata)
        if chunk.data is None or metadata.WhichOneof("payload") != "progress":
            raise ProtocolError("log data must carry exactly one progress message")
        batch = chunk.data
        progress = metadata.progress
        delivery_fields = progress.HasField("delivery_sequence"), progress.HasField(
            "delivery_last_offset"
        )
        if delivery_fields != ((True, True) if self.durable else (False, False)):
            raise ProtocolError("log delivery metadata differs from its durable mode")
        if self.durable and (
            progress.delivery_sequence != self._last_delivery_sequence + 1
            or progress.delivery_last_offset < progress.last_offset
        ):
            raise ContinuityError("durable delivery sequence or boundary is discontinuous")
        watermark = _watermark(progress)
        if (
            progress.first_offset != self._next
            or progress.last_offset < progress.first_offset
            or batch.num_rows != progress.last_offset - progress.first_offset + 1
            or watermark < self._watermark
            or watermark < progress.last_offset
        ):
            raise ContinuityError("log offsets or watermark are discontinuous")
        if not batch.schema.equals(self.schema, check_metadata=True):
            raise ContinuityError("log schema changed during delivery")
        if batch.get_total_buffer_size() + 512 > self._maximum:
            raise ContinuityError("log fragment exceeds the pending byte budget")
        frame_fields = progress.HasField("frame_first_offset"), progress.HasField(
            "frame_last_offset"
        )
        if frame_fields != ((True, True) if self.kind == "frame" else (False, False)):
            raise ProtocolError("log frame boundary does not match table kind")
        if self.kind == "frame" and not (
            progress.frame_first_offset
            <= progress.first_offset
            <= progress.last_offset
            <= progress.frame_last_offset
            <= watermark
        ):
            raise ProtocolError("frame fragment exceeds its complete committed boundary")
        self._watermark = watermark
        return batch, progress

    def _boundary(
        self, batch: pa.RecordBatch, progress: wire.LogProgress
    ) -> tuple[int, int | None]:
        if self.kind == "append":
            return (progress.delivery_last_offset if self.durable else progress.last_offset), None
        if self.kind == "frame":
            if (
                progress.frame_first_offset != progress.first_offset
                or progress.frame_last_offset < progress.last_offset
            ):
                raise ContinuityError("log begins inside an incomplete frame")
            return progress.frame_last_offset, None
        for name in ("_zippy_kv_seq", "_zippy_kv_snapshot_version", "_zippy_kv_batch_last_seq"):
            column = (
                batch.column(batch.schema.get_field_index(name))
                if name in batch.schema.names
                else None
            )
            if column is None or column.type != pa.int64() or column.null_count:
                raise ProtocolError("key-value integer control column is missing or invalid")
        version = batch.column("_zippy_kv_snapshot_version")[0].as_py()
        end = batch.column("_zippy_kv_batch_last_seq")[0].as_py() - 1
        if version <= 0 or (self._version is not None and version != self._version + 1):
            raise ContinuityError("key-value mutation version is discontinuous")
        return end, version

    def _validate_mutation(self, batch: pa.RecordBatch, first: int, end: int) -> None:
        for name, expected in (
            ("_zippy_kv_snapshot_version", self._unit_version),
            ("_zippy_kv_batch_last_seq", end + 1),
            ("_zippy_kv_stream_id", self.table_id.hex),
        ):
            if name not in batch.schema.names:
                raise ProtocolError("key-value mutation control column is missing")
            column = batch.column(name)
            if column.null_count or not pc.all(pc.equal(column, expected)).as_py():
                raise ContinuityError("key-value mutation identity or boundary changed")
        operations = batch.column("_zippy_kv_op") if "_zippy_kv_op" in batch.schema.names else None
        if (
            operations is None
            or operations.null_count
            or not pc.all(pc.is_in(operations, value_set=pa.array(["upsert", "delete"]))).as_py()
        ):
            raise ProtocolError("key-value operation is invalid")
        sequence = batch.column("_zippy_kv_seq")
        expected = pa.array(range(first + 1, first + batch.num_rows + 1), type=pa.int64())
        if sequence.null_count or not pc.all(pc.equal(sequence, expected)).as_py():
            raise ContinuityError("key-value sequence differs from native log offsets")

    def close(self) -> None:
        """Cancel network reading and discard incomplete units, retaining the last complete cursor."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._unacknowledged = None
        self._reader.cancel()
        try:
            self._writer.close()
        except flight.FlightError as error:
            # Cancelled reads make writer Close report cancellation as well.
            if not isinstance(error, (flight.FlightCancelledError, flight.FlightTimedOutError)):
                logger.warning("log writer cleanup failed error=[{}]", error)
        if self._reading.acquire(blocking=False):
            try:
                self._pending.clear()
                self._remainder = None
                self._pending_bytes = 0
            finally:
                self._reading.release()

    def stop(self) -> None:
        """Cancel this reader; alias used by the owning client's lifecycle cleanup."""
        self.close()

    def __enter__(self) -> FlightLogReader:
        """Return this reader for deterministic context-managed cancellation."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Cancel and release the stream when its context exits."""
        self.close()


def _metadata(value: Any) -> wire.LogMetadata:
    if value is None or not 0 < len(value) <= 16384:
        raise ProtocolError("log metadata is missing or exceeds its size limit")
    message = wire.LogMetadata()
    try:
        message.ParseFromString(bytes(value))
    except DecodeError as error:
        raise ProtocolError("log metadata protobuf is malformed") from error
    return message


def _watermark(value: Any) -> int:
    if not value.HasField("watermark"):
        raise ProtocolError("log watermark is missing")
    watermark = value.watermark
    if watermark.state == wire.OffsetWatermark.STATE_EMPTY and watermark.offset == 0:
        return -1
    if watermark.state == wire.OffsetWatermark.STATE_KNOWN:
        return int(watermark.offset)
    raise ProtocolError("log watermark must be empty or known")
