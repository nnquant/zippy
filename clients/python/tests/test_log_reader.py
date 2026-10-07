"""Atomic native-log delivery, cursor continuity and client lifecycle tests."""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from types import SimpleNamespace
import threading
from typing import Any
import uuid
import weakref

import pyarrow as pa
import pytest

from zippy_client import Client, ContinuityError, FlightLogReader, ProtocolError
from zippy_client._generated import protocol_pb2 as wire
from zippy_client.errors import CapabilityError
from zippy_client.transports.flight import FlightTransport

TABLE = uuid.UUID(int=10)
QUERY = uuid.UUID(int=20)


def mark(offset: int) -> wire.OffsetWatermark:
    return wire.OffsetWatermark(
        state=wire.OffsetWatermark.STATE_EMPTY if offset < 0 else wire.OffsetWatermark.STATE_KNOWN,
        offset=max(0, offset),
    )


def opening(kind: str, watermark: int, query: bytes = QUERY.bytes) -> Any:
    return SimpleNamespace(
        data=None,
        app_metadata=wire.LogMetadata(
            opened=wire.LogOpened(
                protocol_version=1,
                query_id=query,
                generation=1,
                table_id=TABLE.bytes,
                table_kind=kind,
                watermark=mark(watermark),
            )
        ).SerializeToString(),
    )


def fragment(
    batch: pa.RecordBatch, first: int, watermark: int, frame: tuple[int, int] | None = None
) -> Any:
    progress = wire.LogProgress(
        first_offset=first, last_offset=first + batch.num_rows - 1, watermark=mark(watermark)
    )
    if frame is not None:
        progress.frame_first_offset, progress.frame_last_offset = frame
    return SimpleNamespace(
        data=batch, app_metadata=wire.LogMetadata(progress=progress).SerializeToString()
    )


class Reader:
    def __init__(self, schema: pa.Schema, chunks: list[Any]) -> None:
        self.schema, self.chunks, self.cancelled = schema, deque(chunks), False

    def read_chunk(self) -> Any:
        if not self.chunks:
            raise StopIteration
        chunk = self.chunks.popleft()
        if isinstance(chunk, Exception):
            raise chunk
        return chunk

    def cancel(self) -> None:
        self.cancelled = True


class Writer:
    def __init__(self) -> None:
        self.closed, self.half_closed = False, False

    def close(self) -> None:
        self.closed = True

    def done_writing(self) -> None:
        self.half_closed = True


def log(kind: str, batch: pa.RecordBatch, chunks: list[Any], **kwargs: Any) -> FlightLogReader:
    reader = Reader(batch.schema, chunks)
    return FlightLogReader(
        reader,
        Writer(),
        query_id=QUERY.bytes,
        start=kwargs.pop("start", "earliest"),
        cursor=kwargs.pop("cursor", None),
        max_pending_bytes=kwargs.pop("max_pending_bytes", 65536),
    )


def test_frames_reassemble_and_partial_disconnect_keeps_complete_cursor() -> None:
    batch = pa.record_batch({"value": [1, 2, 3]})
    partial = log("frame", batch, [opening("frame", 2), fragment(batch.slice(0, 1), 0, 2, (0, 2))])
    cursor = partial.cursor
    with pytest.raises(ContinuityError, match="ended"):
        next(partial)
    assert partial.cursor == cursor
    assert partial._reader.cancelled
    with log(
        "frame",
        batch,
        [
            opening("frame", 2),
            fragment(batch.slice(0, 1), 0, 2, (0, 2)),
            fragment(batch.slice(1, 2), 1, 2, (0, 2)),
        ],
        cursor=cursor,
    ) as resumed:
        event = next(resumed)
        assert event.table.to_pydict() == {"value": [1, 2, 3]}
        assert (event.first_offset, event.last_offset, event.cursor.next_offset) == (0, 2, 3)
        assert event.table.column("value").num_chunks == 2
    assert resumed._writer.closed
    assert list(resumed) == []


def kv_batch() -> pa.RecordBatch:
    return pa.record_batch(
        {
            "_zippy_kv_seq": [1, 2, 3, 4],
            "_zippy_kv_snapshot_version": [1, 1, 2, 2],
            "_zippy_kv_batch_last_seq": [2, 2, 4, 4],
            "_zippy_kv_stream_id": [TABLE.hex] * 4,
            "_zippy_kv_op": ["upsert", "upsert", "delete", "delete"],
            "symbol": ["a", "b", "a", "b"],
            "price": [10, 20, None, None],
        }
    )


def test_key_value_splits_combined_wire_batch_into_complete_mutations() -> None:
    batch = kv_batch()
    reader = log("key_value", batch, [opening("key_value", 3), fragment(batch, 0, 3)])
    first = next(reader)
    assert first.table.num_rows == 2
    assert first.cursor.version == 1
    assert first.cursor.next_offset == 2
    cursor = reader.cursor
    reader.close()
    with log(
        "key_value",
        batch,
        [
            opening("key_value", 3),
            fragment(batch.slice(2, 1), 2, 3),
            fragment(batch.slice(3, 1), 3, 3),
        ],
        cursor=cursor,
    ) as resumed:
        event = next(resumed)
        assert event.table.column("_zippy_kv_op").to_pylist() == ["delete", "delete"]
        assert (event.cursor.next_offset, event.cursor.version) == (4, 2)
    with log("key_value", batch, [opening("key_value", 3), fragment(batch, 0, 3)]) as all_events:
        assert [next(all_events).cursor.version for _ in range(2)] == [1, 2]


@pytest.mark.parametrize("failure", ["gap", "watermark", "schema", "frame", "missing", "budget"])
def test_bad_log_never_advances_delivery_cursor(failure: str) -> None:
    batch = pa.record_batch({"value": [1, 2]})
    data = fragment(batch, 0, 1)
    if failure == "gap":
        data = fragment(batch, 1, 2)
    elif failure == "watermark":
        data = fragment(batch, 0, 0)
    elif failure == "schema":
        data = fragment(pa.record_batch({"different": [1, 2]}), 0, 1)
    elif failure == "frame":
        data = fragment(batch, 0, 1, (0, 1))
    elif failure == "missing":
        data.app_metadata = b""
    reader = log(
        "append",
        batch,
        [opening("append", 1), data],
        max_pending_bytes=513 if failure == "budget" else 65536,
    )
    cursor = reader.cursor
    with pytest.raises((ContinuityError, ProtocolError)):
        next(reader)
    assert reader.cursor == cursor
    assert reader._pending == []
    assert reader._reader.cancelled


@pytest.mark.parametrize(
    "column,values",
    [
        ("_zippy_kv_seq", [1, 3, 3, 4]),
        ("_zippy_kv_snapshot_version", [2, 2, 3, 3]),
        ("_zippy_kv_batch_last_seq", [2, 3, 4, 4]),
        ("_zippy_kv_stream_id", [str(uuid.UUID(int=99))] * 4),
        ("_zippy_kv_op", ["replace"] * 4),
    ],
)
def test_malformed_mutation_is_rejected(column: str, values: list[Any]) -> None:
    batch = kv_batch()
    index = batch.schema.get_field_index(column)
    batch = batch.set_column(index, batch.schema.field(index), pa.array(values))
    reader = log("key_value", batch, [opening("key_value", 3), fragment(batch, 0, 3)])
    with pytest.raises((ContinuityError, ProtocolError)):
        next(reader)
    assert reader.cursor.next_offset == 0


def test_latest_cursor_and_identity_schema_validation() -> None:
    batch = pa.record_batch({"value": [1]})
    reader = log("append", batch, [opening("append", 40), fragment(batch, 41, 41)], start="latest")
    assert reader.cursor.next_offset == 41
    cursor = next(reader).cursor
    assert cursor.next_offset == 42
    reader.stop()
    for changed in [
        replace(cursor, table_id=uuid.UUID(int=11)),
        replace(cursor, kind="frame"),
        replace(cursor, schema=pa.schema([("other", pa.int64())])),
    ]:
        with pytest.raises(ContinuityError, match="identity"):
            log("append", batch, [opening("append", 41)], cursor=changed)
    empty = log("append", batch, [opening("append", -1)], start="latest")
    assert empty.cursor.next_offset == 0
    empty.close()


def test_transport_and_client_use_authenticated_flight_and_explicit_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[wire.LogCommand] = []
    writers: list[Writer] = []
    batch = pa.record_batch({"value": [42, 43]})

    def exchange(descriptor: Any, options: Any) -> tuple[Writer, Reader]:
        command = wire.LogCommand.FromString(descriptor.command)
        commands.append(command)
        assert dict(options.headers)[b"authorization"] == b"Bearer test-token"
        assert options.read_options.ensure_alignment == pa.ipc.Alignment.DataTypeSpecific
        writer = Writer()
        writers.append(writer)
        first = command.offset if command.WhichOneof("start") == "offset" else 2
        chunks = [opening("append", 1, command.query_id)]
        if first < 2:
            chunks.append(fragment(batch.slice(first), first, 1))
        return writer, Reader(batch.schema, chunks)

    transport = FlightTransport.__new__(FlightTransport)
    monkeypatch.setattr(
        transport,
        "capabilities",
        SimpleNamespace(names={"subscription.log_fragments.v1"}),
        raising=False,
    )
    transport.timeout_sec = 0.1
    transport._connection_lock = threading.RLock()
    transport._closed = False
    transport._log_readers = weakref.WeakSet()
    connection = SimpleNamespace(
        client=SimpleNamespace(do_exchange=exchange),
        session=SimpleNamespace(session_id=uuid.UUID(int=30).bytes, bearer_token="test-token"),
    )
    monkeypatch.setattr(transport, "_connection", lambda role, **kwargs: connection)
    client = Client.__new__(Client)
    client._flight = transport
    client._closed = False
    client._subscriptions = weakref.WeakSet()
    with client.open_log("ticks", namespace="market", start="earliest", batch_rows=1) as reader:
        assert next(reader).table.to_pydict() == {"value": [42, 43]}
        cursor = reader.cursor
    assert writers[0].half_closed and writers[0].closed
    assert commands[0].table.name.namespace == "market"
    with client.open_log(cursor=cursor) as resumed:
        assert resumed.cursor.next_offset == 2
    assert commands[1].table.table_id == TABLE.bytes
    assert commands[1].offset == 2
    with pytest.raises(ValueError):
        client.open_log("ticks", cursor=cursor)
    for options in (
        {"start": "other"},
        {"max_pending_bytes": 0},
        {"batch_rows": 0},
        {"memory_limit_bytes": 0},
        {"source": ""},
        {"namespace": ""},
    ):
        with pytest.raises(ValueError):
            transport.open_log(**({"source": "ticks"} | options))
    monkeypatch.setattr(transport, "capabilities", SimpleNamespace(names=set()))
    with pytest.raises(CapabilityError):
        client.open_log("ticks")
    monkeypatch.setattr(
        transport, "capabilities", SimpleNamespace(names={"subscription.log_fragments.v1"})
    )
    pending = client.open_log(cursor=cursor)
    direct = transport.open_log(cursor=cursor)
    transport._put_lock = threading.RLock()
    transport._active_puts = {}
    transport._fixed_connections = {}
    monkeypatch.setattr(transport, "_owned_connections", [connection], raising=False)
    connection.session.expires_at_ns = 0
    monkeypatch.setattr(transport, "_read_connection", connection, raising=False)
    monkeypatch.setattr(transport, "_writer_connection", None, raising=False)
    monkeypatch.setattr(transport, "_admin_connection", None, raising=False)
    connection.client.close = lambda: None
    monkeypatch.setattr(client, "control", SimpleNamespace(close=lambda: None), raising=False)
    client.close()
    assert pending._reader.cancelled and direct._reader.cancelled
    assert pending._writer.closed and direct._writer.closed


def test_stop_interrupts_blocked_read_without_advancing_partial_frame() -> None:
    import pyarrow.flight as flight

    batch = pa.record_batch({"value": [1, 2]})
    entered, cancelled = threading.Event(), threading.Event()

    class BlockingReader(Reader):
        def read_chunk(self) -> Any:
            if self.chunks:
                return super().read_chunk()
            entered.set()
            assert cancelled.wait(2)
            raise flight.FlightCancelledError("cancelled")

        def cancel(self) -> None:
            super().cancel()
            cancelled.set()

    raw = BlockingReader(
        batch.schema, [opening("frame", 1), fragment(batch.slice(0, 1), 0, 1, (0, 1))]
    )
    reader = FlightLogReader(
        raw, Writer(), query_id=QUERY.bytes, start="earliest", cursor=None, max_pending_bytes=65536
    )
    cursor = reader.cursor
    outcome: list[object] = []

    def consume() -> None:
        try:
            outcome.append(next(reader))
        except Exception as error:
            outcome.append(error)

    worker = threading.Thread(target=consume)
    worker.start()
    assert entered.wait(2)
    reader.stop()
    worker.join(2)
    assert not worker.is_alive()
    assert len(outcome) == 1 and isinstance(outcome[0], Exception)
    assert reader.cursor == cursor
    assert reader._pending == []


def test_frame_transport_fragment_cannot_cross_declared_frame() -> None:
    batch = pa.record_batch({"value": [1, 2]})
    reader = log("frame", batch, [opening("frame", 1), fragment(batch, 0, 1, (0, 0))])
    with pytest.raises(ProtocolError, match="boundary"):
        next(reader)
    assert reader.cursor.next_offset == 0


def test_opening_timeout_cancels_rpc_without_limiting_stream_lifetime() -> None:
    import pyarrow.flight as flight

    cancelled = threading.Event()

    class SilentReader:
        def read_chunk(self) -> Any:
            assert cancelled.wait(1)
            raise flight.FlightCancelledError("cancelled")

        def cancel(self) -> None:
            cancelled.set()

    transport = FlightTransport.__new__(FlightTransport)
    transport.timeout_sec = 1
    writer = Writer()
    with pytest.raises(TimeoutError, match="opening"):
        transport._open_log_reader(SilentReader(), writer, QUERY.bytes, "latest", None, 65536, 0.01)
    assert writer.closed and cancelled.is_set()


def durable_reader(*, receipt_error: Exception | None = None) -> tuple[FlightLogReader, list[Any]]:
    """Construct a durable split append delivery and inject its acknowledgement response."""
    batch = pa.record_batch({"value": [4, 5]})
    consumer = wire.LogConsumer(consumer_id=uuid.UUID(int=30).bytes, individual=True)
    opened = wire.LogMetadata.FromString(opening("append", 5).app_metadata)
    opened.opened.consumer.CopyFrom(consumer)
    opened.opened.next_offset = 4
    chunks = [SimpleNamespace(data=None, app_metadata=opened.SerializeToString())]
    for offset in (4, 5):
        part = fragment(batch.slice(offset - 4, 1), offset, 5)
        metadata = wire.LogMetadata.FromString(part.app_metadata)
        metadata.progress.delivery_sequence = 1
        metadata.progress.delivery_last_offset = 5
        part.app_metadata = metadata.SerializeToString()
        chunks.append(part)
    reader = Reader(batch.schema, chunks)
    controls: list[Any] = []

    class AcknowledgingWriter(Writer):
        def write_metadata(self, metadata: bytes) -> None:
            control = wire.LogControl.FromString(metadata)
            assert control.WhichOneof("control") == "acknowledge"
            controls.append(control)
            reader.chunks.appendleft(
                receipt_error
                if receipt_error is not None
                else SimpleNamespace(
                    data=None,
                    app_metadata=wire.LogMetadata(
                        acknowledged=control.acknowledge
                    ).SerializeToString(),
                )
            )

    return (
        FlightLogReader(
            reader,
            AcknowledgingWriter(),
            query_id=QUERY.bytes,
            start="committed",
            cursor=None,
            max_pending_bytes=65536,
            consumer=consumer,
        ),
        controls,
    )


def test_durable_ack_waits_for_complete_split_delivery_and_validates_identity() -> None:
    events, controls = durable_reader()
    event = next(events)
    assert event.delivery_sequence == 1
    assert event.table.to_pydict() == {"value": [4, 5]}
    assert event.table.column("value").num_chunks == 2
    with pytest.raises(RuntimeError, match="acknowledge"):
        next(events)
    with pytest.raises(ValueError, match="pending complete event"):
        events.acknowledge(replace(event))
    with pytest.raises(ValueError, match="nil"):
        events.acknowledge(event, request_id=uuid.UUID(int=0))
    assert not controls and not events._closed.is_set()
    events.acknowledge(event, request_id=uuid.UUID(int=99))
    events.acknowledge(event)
    assert len(controls) == 1
    assert controls[0].acknowledge.processed_through == 5
    assert controls[0].acknowledge.request_id == uuid.UUID(int=99).bytes
    events.close()


def test_durable_ack_lost_response_is_ambiguous_and_does_not_advance_known_confirmation() -> None:
    import pyarrow.flight as flight
    from zippy_client import AmbiguousAcknowledgementError

    events, _ = durable_reader(receipt_error=flight.FlightUnavailableError("lost response"))
    event = next(events)
    with pytest.raises(AmbiguousAcknowledgementError) as error:
        events.acknowledge(event, request_id=uuid.UUID(int=99))
    assert error.value.request_id == str(uuid.UUID(int=99))
    assert events._confirmed_event is None and events._closed.is_set()


def test_durable_delivery_rejects_changed_sequence_and_truncated_atomic_unit() -> None:
    for change in ("sequence", "boundary", "missing"):
        events, _ = durable_reader()
        second = events._reader.chunks[-1]
        metadata = wire.LogMetadata.FromString(second.app_metadata)
        if change == "sequence":
            metadata.progress.delivery_sequence = 2
        elif change == "boundary":
            metadata.progress.delivery_last_offset = 6
        else:
            metadata.progress.ClearField("delivery_sequence")
        second.app_metadata = metadata.SerializeToString()
        before = events.cursor
        with pytest.raises((ProtocolError, ContinuityError)):
            next(events)
        assert events.cursor == before
        assert events._closed.is_set()


def test_durable_ack_preserves_structured_rejection_raised_by_request_writer() -> None:
    import pyarrow.flight as flight
    from zippy_client import ServerError

    events, _ = durable_reader()
    event = next(events)
    envelope = wire.ErrorEnvelope(
        protocol_version=1,
        code="SCHEMA_MISMATCH",
        message="table schema changed",
        retryable=False,
    )

    def reject(_: bytes) -> None:
        raise flight.FlightServerError("opaque", extra_info=envelope.SerializeToString())

    events._writer.write_metadata = reject
    with pytest.raises(ServerError) as error:
        events.acknowledge(event)
    assert error.value.code == "SCHEMA_MISMATCH"
    assert events._confirmed_event is None and events._closed.is_set()
