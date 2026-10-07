"""Native state bootstrap/recovery uses one table identity and never exposes stale state."""

from __future__ import annotations

import queue
import threading
from types import SimpleNamespace
from typing import Any, cast
import uuid

import pyarrow as pa
import pytest

import zippy

from state_fixtures import changelog_schema
from zippy_client import ContinuityError, LogCursor, ProtocolError, ServerError

TABLE = uuid.UUID(int=44)
SCHEMA = pa.schema([("symbol", pa.string()), ("price", pa.int64())])


def snapshot(sequence: int = 0, version: int = 0, price: int | None = None) -> pa.Table:
    metadata = {
        b"zippy_kv_protocol": b"2",
        b"zippy_kv_seq": str(sequence).encode(),
        b"zippy_kv_version": str(version).encode(),
        b"zippy_kv_stream_id": TABLE.hex.encode(),
        b"zippy_kv_keys": b'["symbol"]',
    }
    return pa.Table.from_arrays(
        [
            pa.array([] if price is None else ["a"], pa.string()),
            pa.array([] if price is None else [price], pa.int64()),
        ],
        schema=SCHEMA.with_metadata(metadata),
    )


def delta(sequence: int, version: int, price: int) -> pa.Table:
    schema = changelog_schema(SCHEMA)
    return pa.Table.from_arrays(
        [
            pa.array([sequence]),
            pa.array(["upsert"]),
            pa.array([version]),
            pa.array([sequence]),
            pa.array([TABLE.hex]),
            pa.array(["a"]),
            pa.array([price]),
        ],
        schema=schema,
    )


class Log:
    def __init__(self, sequence: int) -> None:
        self.kind, self.table_id, self.schema = "key_value", TABLE, delta(1, 1, 1).schema
        self.cursor = LogCursor(TABLE, sequence, self.kind, self.schema)
        self.events: queue.Queue[Any] = queue.Queue()
        self.closed = False

    def __next__(self) -> Any:
        event = self.events.get(timeout=3)
        if isinstance(event, Exception):
            raise event
        return SimpleNamespace(table=event)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.events.put(ContinuityError("test log closed"))


class NativeClient:
    def __init__(self) -> None:
        self.logs: list[Log] = []
        self.current = snapshot()
        self.on_snapshot: Any = None
        self.opened = threading.Event()
        self.recover_gate: threading.Event | None = None
        self.refreshes = 0
        self.open_options: list[dict[str, Any]] = []
        self.flight = SimpleNamespace(refresh_read_session=self.refresh)

    def refresh(self, **kwargs: Any) -> None:
        self.refreshes += 1

    def open_log(self, **options: Any) -> Log:
        self.open_options.append(options)
        if self.recover_gate is not None and self.logs:
            self.opened.set()
            assert self.recover_gate.wait(2)
        stream = Log(int(self.current.schema.metadata[b"zippy_kv_seq"]))
        self.logs.append(stream)
        return stream

    def snapshot_state(self, *, table_id: uuid.UUID, deadline_ns: int) -> pa.Table:
        assert table_id == TABLE and deadline_ns > 0
        if self.on_snapshot is not None:
            self.on_snapshot()
        return self.current


def test_bootstrap_orders_log_before_snapshot_and_applies_concurrent_updates() -> None:
    client = NativeClient()

    def during_snapshot() -> None:
        assert len(client.logs) == 1
        client.logs[-1].events.put(delta(1, 1, 10))
        client.current = snapshot(1, 1, 10)
        client.logs[-1].events.put(delta(2, 2, 20))

    client.on_snapshot = during_snapshot
    with zippy.StateSubscription(
        "quotes", client=cast(zippy.Client, client), namespace="market"
    ) as state:
        assert state.wait_for_sequence(2, timeout=1)
        assert state.select([["a"]]).column("price").to_pylist() == [20]
        assert state.metrics()["bootstrap_generation"] == 1
        assert state.metrics()["ready"]
        assert client.open_options[0]["source"] == "quotes"
        assert not state.wait_for_sequence(3, timeout=0)
    assert client.logs[0].closed
    with pytest.raises(RuntimeError, match="stopped"):
        state.snapshot()
    with pytest.raises(RuntimeError, match="restarted"):
        state.start()


def test_recovery_discards_old_state_reauthenticates_and_binds_table_id() -> None:
    client = NativeClient()
    client.current = snapshot(1, 1, 10)
    state = zippy.StateSubscription(
        "quotes", client=cast(zippy.Client, client), reconnect_interval_sec=0.001
    ).start()
    try:
        client.current = snapshot(2, 2, 20)
        client.recover_gate = threading.Event()
        client.logs[0].events.put(ServerError("UNAVAILABLE", "server stopped", retryable=True))
        assert client.opened.wait(1)
        with pytest.raises(RuntimeError, match="recovering"):
            state.snapshot()
        client.recover_gate.set()
        assert state.wait_for_sequence(2, timeout=1)
        assert state.snapshot().column("price").to_pylist() == [20]
        assert client.open_options[1]["table_id"] == TABLE
        assert "source" not in client.open_options[1]
        assert client.refreshes == 1
        assert state.metrics()["reconnects_total"] == 1
    finally:
        if client.recover_gate is not None:
            client.recover_gate.set()
        state.stop()


@pytest.mark.parametrize(
    "error",
    [
        ProtocolError("corrupt wire"),
        ServerError("CANCELLED", "administrator cancelled", retryable=False),
    ],
)
def test_nonretryable_failure_does_not_silently_restart(error: Exception) -> None:
    client = NativeClient()
    with zippy.StateSubscription("quotes", client=cast(zippy.Client, client)) as state:
        client.logs[0].events.put(error)
        with pytest.raises(RuntimeError, match="failed"):
            state.wait_for_sequence(1, timeout=1)
        assert len(client.logs) == 1
        assert not state.metrics()["ready"]


def test_identity_mismatch_and_rollback_fail_initial_bootstrap() -> None:
    client = NativeClient()
    client.current = snapshot(1, 1, 10)
    original = client.current

    def rollback() -> None:
        client.current = snapshot()

    client.on_snapshot = rollback
    with pytest.raises(ContinuityError, match="precedes"):
        zippy.StateSubscription("quotes", client=cast(zippy.Client, client)).start()
    assert client.logs[0].closed
    client.current = original.replace_schema_metadata(
        {**original.schema.metadata, b"zippy_kv_stream_id": uuid.UUID(int=55).hex.encode()}
    )
    client.on_snapshot = None
    with pytest.raises(ContinuityError, match="different"):
        zippy.StateSubscription("quotes", client=cast(zippy.Client, client)).start()


def test_state_no_longer_accepts_legacy_loader_contract() -> None:
    with pytest.raises(TypeError):
        cast(Any, zippy.StateSubscription)("quotes", lambda: snapshot())
    with pytest.raises(ValueError, match="budget"):
        zippy.StateSubscription(
            "quotes", client=cast(zippy.Client, NativeClient()), max_pending_bytes=1
        )


def test_recovery_attempts_are_bounded_and_keep_old_state_unavailable() -> None:
    from zippy_client import ZippyConnectionError

    class OfflineClient(NativeClient):
        def open_log(self, **options: Any) -> Log:
            if self.logs:
                raise ZippyConnectionError("still offline")
            return super().open_log(**options)

    client = OfflineClient()
    with zippy.StateSubscription(
        "quotes",
        client=cast(zippy.Client, client),
        max_reconnect_attempts=1,
        reconnect_interval_sec=0.001,
    ) as state:
        client.logs[0].events.put(ZippyConnectionError("connection lost"))
        with pytest.raises(RuntimeError, match="failed"):
            state.wait_for_sequence(1, timeout=1)
        assert state.metrics()["consecutive_failures"] == 2
        assert client.refreshes == 1
        assert client.logs[0].closed
        with pytest.raises(RuntimeError):
            state.snapshot()
