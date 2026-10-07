"""Callback success, durable ACK, atomicity and cancellation on the native log path."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
import threading
import time
from typing import Any, cast
import uuid
import weakref

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from zippy_client import (
    AmbiguousAcknowledgementError,
    Client,
    ContinuityError,
    FlightLogReader,
    FlightSubscriber,
    LogCursor,
    LogEvent,
    ServerError,
    SubscriptionOptions,
    ZippyConnectionError,
)
from zippy_client.endpoints import ControlEndpoint
from zippy_client.router import TransportRouter

SCHEMA = pa.schema([("instrument_id", pa.string()), ("value", pa.int64())])
TABLE = uuid.UUID(int=900)


def cursor(offset: int = 0, kind: str = "append") -> LogCursor:
    return LogCursor(TABLE, offset, kind, SCHEMA)


def event(first: int, values: list[int], kind: str = "append") -> LogEvent:
    table = pa.table({"instrument_id": ["a", "b"][: len(values)], "value": values}, schema=SCHEMA)
    last = first + len(values) - 1
    return LogEvent(table, first, last, last, cursor(last + 1, kind), first + 1)


class Reader:
    def __init__(
        self,
        events: Sequence[LogEvent | Exception],
        *,
        offset: int = 0,
        kind: str = "append",
        durable: bool = False,
        ack_error: Exception | None = None,
    ) -> None:
        self.events = deque(events)
        self.cursor = cursor(offset, kind)
        self.kind, self.table_id, self.schema = kind, TABLE, SCHEMA
        self.durable, self.ack_error = durable, ack_error
        self.closed = threading.Event()
        self.acknowledged: list[LogEvent] = []

    def __next__(self) -> LogEvent:
        if not self.events:
            self.closed.wait(5)
            raise ZippyConnectionError("reader cancelled")
        value = self.events.popleft()
        if isinstance(value, Exception):
            raise value
        self.cursor = value.cursor
        return value

    def acknowledge(self, value: LogEvent) -> None:
        if self.ack_error is not None:
            raise self.ack_error
        self.acknowledged.append(value)

    def close(self) -> None:
        self.closed.set()


def wait_until(predicate: Any) -> None:
    deadline = time.monotonic() + 3
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("callback condition timed out")
        time.sleep(0.001)


def subscriber(
    readers: list[Reader], callback: Any, **kwargs: Any
) -> tuple[FlightSubscriber, list[Any]]:
    pending = deque(readers)
    openings: list[Any] = []

    def open_reader(previous: LogCursor | None) -> FlightLogReader:
        openings.append(previous)
        assert pending, "unexpected reconnect"
        return cast(FlightLogReader, pending.popleft())

    return (
        FlightSubscriber(
            "ticks",
            callback,
            open_reader,
            options=kwargs.pop("options", SubscriptionOptions(reconnect_interval_ms=1)),
            **kwargs,
        ),
        openings,
    )


def test_raw_reconnect_resumes_only_after_successful_callbacks() -> None:
    first = Reader([event(0, [1, 2]), ZippyConnectionError("network lost")])
    second = Reader([event(2, [3, 4])], offset=2)
    received: list[Any] = []
    sub, openings = subscriber([first, second], received.append)
    sub.start()
    try:
        wait_until(lambda: sub.cursor is not None and sub.cursor.next_offset == 4)
        assert [row["value"] for row in received] == [1, 2, 3, 4]
        assert openings == [None, cursor(2)]
        assert first.closed.is_set()
        assert sub.metrics()["reconnects_total"] == 1
        assert sub.health()["healthy"] is True
    finally:
        sub.stop()
    assert sub.status() == "stopped"
    assert second.closed.is_set()
    with pytest.raises(RuntimeError, match="once"):
        sub.start()


@pytest.mark.parametrize("failure", [ZippyConnectionError("user database"), OSError("user file")])
def test_callback_exceptions_never_reconnect_or_ack(failure: Exception) -> None:
    reader = Reader([event(0, [1, 2])], durable=True)
    calls = []

    def callback(row: Any) -> None:
        calls.append(row)
        raise failure

    sub, openings = subscriber(
        [reader],
        callback,
        options=SubscriptionOptions(
            start="committed",
            consumer_id=uuid.UUID(int=901),
            reconnect_interval_ms=1,
        ),
    )
    sub.start()
    with pytest.raises(type(failure)) as caught:
        sub.join(3)
    assert caught.value is failure
    assert len(calls) == 1 and openings == [None]
    assert sub.cursor == cursor(0) and not reader.acknowledged
    assert sub.health()["terminal"] is True
    assert reader.closed.is_set()
    with pytest.raises(type(failure)):
        sub.stop()


@pytest.mark.parametrize("committed", [False, True])
def test_ambiguous_ack_reconnect_uses_catalog_and_allows_at_least_once(committed: bool) -> None:
    unit = event(0, [1, 2], "frame")
    lost = Reader(
        [unit],
        kind="frame",
        durable=True,
        ack_error=AmbiguousAcknowledgementError(
            "receipt lost",
            request_id=str(uuid.UUID(int=903)),
        ),
    )
    resumed = Reader(
        [] if committed else [unit], offset=2 if committed else 0, kind="frame", durable=True
    )
    tables: list[pa.Table] = []
    sub, openings = subscriber(
        [lost, resumed],
        tables.append,
        table_callback=True,
        options=SubscriptionOptions(
            start="committed", consumer_id=uuid.UUID(int=902), reconnect_interval_ms=1
        ),
    )
    sub.start()
    try:
        wait_until(lambda: sub.cursor is not None and sub.cursor.next_offset == 2)
        assert len(tables) == (1 if committed else 2)
        assert all(table.num_rows == 2 for table in tables)
        assert openings == [None, cursor(0, "frame")]
        assert len(resumed.acknowledged) == (0 if committed else 1)
    finally:
        sub.stop()


@pytest.mark.parametrize("kind", ["frame", "key_value"])
@pytest.mark.parametrize("option", ["rows", "filter", "count", "instrument_ids"])
def test_atomic_sources_reject_partial_views_before_starting(kind: str, option: str) -> None:
    reader = Reader([], kind=kind)
    options = SubscriptionOptions(
        **{
            "filter": {"filter": pc.field("value") > 1},
            "count": {"count": 1},
            "instrument_ids": {"instrument_ids": ("a",)},
            "rows": {},
        }[option]
    )
    sub, _ = subscriber(
        [reader], lambda value: None, options=options, table_callback=option != "rows"
    )
    with pytest.raises(ValueError, match="complete"):
        sub.start()
    assert reader.closed.is_set()


def test_filter_count_and_empty_batches_acknowledge_the_original_unit() -> None:
    units = [event(0, [1, 2]), event(2, [3, 4])]
    reader = Reader(units, durable=True)
    tables: list[pa.Table] = []
    sub, _ = subscriber(
        [reader],
        tables.append,
        table_callback=True,
        options=SubscriptionOptions(
            start="committed",
            consumer_id=uuid.UUID(int=904),
            filter=pc.field("value") >= 3,
            count=1,
        ),
    )
    sub.start()
    try:
        wait_until(lambda: len(reader.acknowledged) == 2)
        assert [table.column("value").to_pylist() for table in tables] == [[4]]
        assert [ack.last_offset for ack in reader.acknowledged] == [1, 3]
    finally:
        sub.stop()


def test_instrument_filter_and_empty_selection() -> None:
    unit = event(0, [1, 2])
    rows: list[Any] = []
    reader = Reader([unit])
    sub, _ = subscriber([reader], rows.append, options=SubscriptionOptions(instrument_ids=("b",)))
    sub.start()
    try:
        wait_until(lambda: sub.cursor == cursor(2))
        assert [row.to_dict() for row in rows] == [{"instrument_id": "b", "value": 2}]
    finally:
        sub.stop()
    empty_reader = Reader([unit])
    sub, _ = subscriber([empty_reader], rows.append, options=SubscriptionOptions(instrument_ids=()))
    sub.start()
    try:
        wait_until(lambda: sub.cursor == cursor(2))
        assert len(rows) == 1
    finally:
        sub.stop()


def test_stop_inside_callback_leaves_partial_unit_unacknowledged() -> None:
    reader = Reader([event(0, [1, 2])], durable=True)
    rows: list[Any] = []

    def callback(row: Any) -> None:
        rows.append(row)
        sub.stop()

    sub, _ = subscriber(
        [reader],
        callback,
        options=SubscriptionOptions(start="committed", consumer_id=uuid.UUID(int=905)),
    )
    sub.start()
    sub.join(3)
    assert sub.status() == "stopped"
    assert len(rows) == 1
    assert sub.cursor == cursor(0) and not reader.acknowledged


def test_stop_interrupts_throttle_without_advancing_or_acknowledging() -> None:
    reader = Reader([event(0, [1]), event(1, [2])], durable=True)
    rows: list[Any] = []
    sub, _ = subscriber(
        [reader],
        rows.append,
        options=SubscriptionOptions(
            start="committed", consumer_id=uuid.UUID(int=906), throttle_ms=60000
        ),
    )
    sub.start()
    wait_until(lambda: reader.cursor.next_offset == 2)
    sub.stop()
    assert sub.cursor == cursor(1)
    assert len(rows) == 1 and len(reader.acknowledged) == 1


@pytest.mark.parametrize("reconnect", [True, False])
def test_structured_terminal_errors_do_not_retry(reconnect: bool) -> None:
    error = ServerError("SCHEMA_MISMATCH", "changed", retryable=False)
    reader = Reader([error])
    sub, openings = subscriber(
        [reader], lambda value: None, options=SubscriptionOptions(reconnect=reconnect)
    )
    sub.start()
    with pytest.raises(ServerError) as caught:
        sub.join(3)
    assert caught.value is error and openings == [None]


def test_raw_reconnect_rejects_schema_drift() -> None:
    first = Reader([ZippyConnectionError("offline")])
    changed = Reader([])
    changed.schema = SCHEMA.append(pa.field("other", pa.int64()))
    sub, _ = subscriber([first, changed], lambda value: None)
    sub.start()
    with pytest.raises(ContinuityError, match="schema"):
        sub.join(3)
    assert changed.closed.is_set()


@pytest.mark.parametrize(
    "expected",
    [
        SCHEMA.append(pa.field("missing", pa.int64())),
        SCHEMA.with_metadata({"source": "different"}),
        pa.schema([pa.field("instrument_id", pa.string(), nullable=False), SCHEMA.field(1)]),
    ],
)
def test_input_schema_is_checked_before_empty_source_starts(expected: pa.Schema) -> None:
    reader = Reader([])
    calls: list[Any] = []
    sub, _ = subscriber(
        [reader], calls.append, options=SubscriptionOptions(expected_schema=expected)
    )
    with pytest.raises(ContinuityError, match="expected_schema"):
        sub.start()
    assert reader.closed.is_set() and sub._thread is None and not calls
    assert sub.cursor is None and sub.status() == "failed"


def test_matching_expected_schema_preserves_delivery() -> None:
    calls: list[Any] = []
    sub, _ = subscriber(
        [Reader([event(0, [1])])],
        calls.append,
        options=SubscriptionOptions(expected_schema=SCHEMA),
    )
    sub.start()
    try:
        wait_until(lambda: sub.cursor == cursor(1))
        assert calls[0]["value"] == 1
    finally:
        sub.stop()


@pytest.mark.parametrize(
    "values",
    [
        {"start": "wrong"},
        {"start": "committed"},
        {"consumer_id": uuid.UUID(int=1)},
        {"group_id": uuid.UUID(int=1)},
        {"filter": "value > 1"},
        {"filter": pc.field("value") > 1, "instrument_ids": ("a",)},
        {"instrument_ids": ["a"]},
        {"throttle_ms": 0},
        {"batch_rows": True},
        {"open_timeout_sec": float("inf")},
        {"open_timeout_sec": 0},
        {"expected_schema": "schema"},
        {"start": "committed", "consumer_id": uuid.UUID(int=1), "cursor": cursor()},
    ],
)
def test_options_fail_explicitly(values: dict[str, Any]) -> None:
    with pytest.raises((ValueError, TypeError)):
        SubscriptionOptions(**values)


def test_client_callbacks_never_consult_gateway_or_legacy_registry() -> None:
    first = Reader([event(0, [1]), ZippyConnectionError("disconnect")])
    second = Reader([event(1, [2])], offset=1)
    readers = deque([first, second])
    calls: list[dict[str, Any]] = []

    class Transport:
        def refresh_read_session(self, *, timeout_sec: float) -> None:
            assert timeout_sec == 5.0

        def open_log(self, source: str | None, **kwargs: Any) -> FlightLogReader:
            calls.append({"source": source, **kwargs})
            if len(calls) == 2:
                assert source is None and kwargs["table_id"] is None
                assert kwargs["start"] == "latest" and kwargs["cursor"] == cursor(1)
            return cast(FlightLogReader, readers.popleft())

        def close(self) -> None:
            pass

    class Control:
        def close(self) -> None:
            pass

    client = object.__new__(Client)
    client._closed = False
    client._flight = cast(Any, Transport())
    client._subscriptions = weakref.WeakSet()
    client.control = cast(Any, Control())
    from zippy_client.models import Transport as Mode

    client.default_transport = Mode.AUTO
    client.router = TransportRouter(
        ControlEndpoint("tcp", "127.0.0.1:1", "test"),
        flight_endpoint="127.0.0.1:2",
    )
    rejected = Reader([])
    readers.appendleft(rejected)
    with pytest.raises(ContinuityError, match="expected_schema") as failed_start:
        client.subscribe_table(
            "ticks",
            lambda table: None,
            options=SubscriptionOptions(expected_schema=SCHEMA.with_metadata({"version": "other"})),
        )
    # The retained exception traceback keeps the failed subscriber alive. It must not
    # poison Client.close() after a caller has already handled the startup rejection.
    assert failed_start.value is not None and rejected.closed.is_set()
    assert not client._subscriptions
    calls.clear()
    tables: list[Any] = []
    sub = client.subscribe_table(
        "ticks",
        tables.append,
        options=SubscriptionOptions(start="earliest", reconnect_interval_ms=1),
    )
    wait_until(lambda: sub.cursor == cursor(2))
    client.close()
    assert sub.status() == "stopped" and len(tables) == 2
    assert calls[0]["source"] == "ticks" and calls[0]["start"] == "earliest"
    assert first.closed.is_set() and second.closed.is_set()


def test_stop_during_open_closes_late_reader_without_starting_worker() -> None:
    entered, release = threading.Event(), threading.Event()
    reader = Reader([])

    def open_reader(previous: LogCursor | None) -> FlightLogReader:
        entered.set()
        assert release.wait(3)
        return cast(FlightLogReader, reader)

    sub = FlightSubscriber("ticks", lambda row: None, open_reader, options=SubscriptionOptions())
    start = threading.Thread(target=sub.start)
    start.start()
    assert entered.wait(3)
    sub.stop()
    release.set()
    start.join(3)
    assert not start.is_alive() and reader.closed.is_set()
    assert sub.status() == "stopped"
    assert sub._thread is None


def test_durable_reopen_waits_for_previous_owner_but_initial_conflict_is_explicit() -> None:
    first = Reader([ZippyConnectionError("offline")], durable=True)
    second = Reader([event(0, [1])], durable=True)
    conflict = ServerError("CONFLICT", "consumer authority is active", retryable=False)
    attempts = deque([first, conflict, second])

    def open_reader(previous: LogCursor | None) -> FlightLogReader:
        attempt = attempts.popleft()
        if isinstance(attempt, Exception):
            raise attempt
        return cast(FlightLogReader, attempt)

    options = SubscriptionOptions(
        start="committed", consumer_id=uuid.UUID(int=920), reconnect_interval_ms=1
    )
    sub = FlightSubscriber("ticks", lambda row: None, open_reader, options=options)
    sub.start()
    try:
        wait_until(lambda: sub.cursor == cursor(1))
        assert sub.metrics()["reconnects_total"] == 2
    finally:
        sub.stop()
    attempts.append(conflict)
    sub = FlightSubscriber("ticks", lambda row: None, open_reader, options=options)
    with pytest.raises(ServerError) as caught:
        sub.start()
    assert caught.value is conflict and not attempts


def test_reconnect_disabled_preserves_failure_and_stop_interrupts_backoff() -> None:
    failure = ZippyConnectionError("offline")
    sub, _ = subscriber(
        [Reader([failure])], lambda row: None, options=SubscriptionOptions(reconnect=False)
    )
    sub.start()
    with pytest.raises(ZippyConnectionError) as caught:
        sub.join(3)
    assert caught.value is failure
    sub, calls = subscriber(
        [Reader([failure])],
        lambda row: None,
        options=SubscriptionOptions(reconnect_interval_ms=60000),
    )
    sub.start()
    wait_until(lambda: sub.status() == "reconnecting")
    sub.stop()
    assert sub.status() == "stopped" and calls == [None]


def test_async_callbacks_are_not_silently_acknowledged() -> None:
    async def callback(value: Any) -> None:
        raise AssertionError("coroutine must never execute on a synchronous worker")

    reader = Reader([event(0, [1])], durable=True)
    options = SubscriptionOptions(start="committed", consumer_id=uuid.UUID(int=921))
    with pytest.raises(TypeError, match="synchronous"):
        subscriber([reader], callback, options=options)
    sub, _ = subscriber([reader], lambda value: callback(value), options=options)
    sub.start()
    with pytest.raises(TypeError, match="awaitable"):
        sub.join(3)
    assert not reader.acknowledged and sub.cursor == cursor(0)


def test_native_row_callback_preserves_nanoseconds_nulls_and_mapping_values() -> None:
    from zippy_client import Row

    schema = pa.schema(
        [
            pa.field("dt", pa.timestamp("ns", tz="UTC"), metadata={"unit": "ns"}),
            pa.field("instrument_id", pa.string()),
            pa.field("value", pa.int64()),
        ],
        metadata={"source": "native-row-regression"},
    )
    table = pa.Table.from_pydict(
        {"dt": [1_789_992_123_000_000_001, None], "instrument_id": ["a", "b"], "value": [1, None]},
        schema=schema,
    )
    position = LogCursor(TABLE, 2, "append", schema)
    reader = Reader([LogEvent(table, 0, 1, 1, position)])
    reader.schema = schema
    reader.cursor = LogCursor(TABLE, 0, "append", schema)
    rows: list[Row] = []
    sub, _ = subscriber([reader], rows.append)
    try:
        sub.start()
        wait_until(lambda: sub.cursor is not None and sub.cursor.next_offset == 2)
        assert rows[0].to_dict() == {
            "dt": 1_789_992_123_000_000_001,
            "instrument_id": "a",
            "value": 1,
        }
        assert dict(rows[1]) == {"dt": None, "instrument_id": "b", "value": None}
        assert len(rows[0]) == 3 and "instrument_id" in rows[0]
        copy = rows[0].to_dict()
        copy["value"] = 9
        assert rows[0]["value"] == 1
        assert repr(rows[0]).startswith("zippy_client.Row(")
        assert sub.cursor is not None and sub.cursor.schema.equals(schema, check_metadata=True)
    finally:
        sub.stop()


def test_idle_native_subscription_stays_open_until_explicit_stop() -> None:
    reader = Reader([])
    sub, openings = subscriber([reader], lambda row: None)
    sub.start()
    wait_until(lambda: sub.status() == "running")
    assert sub.health()["healthy"] is True
    assert not reader.closed.wait(0.15)
    assert sub.status() == "running"
    assert len(openings) == 1
    sub.stop()
    assert reader.closed.is_set()
    assert sub.health()["terminal"] is True
