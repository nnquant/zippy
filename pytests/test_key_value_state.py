"""Protocol-v2 bootstrap and fail-closed consumer contracts."""

import pyarrow as pa
import pytest

import zippy


def snapshot():
    schema = pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("price", pa.int64(), nullable=False),
        ],
        metadata={
            "zippy_kv_protocol": "2",
            "zippy_kv_seq": "0",
            "zippy_kv_version": "0",
            "zippy_kv_stream_id": "test-stream",
            "zippy_kv_keys": '["symbol"]',
        },
    )
    return pa.Table.from_arrays(
        [pa.array([], pa.string()), pa.array([], pa.int64())], schema=schema
    )


def delta():
    schema = zippy._key_value_changelog_schema(snapshot().schema.remove_metadata())
    return pa.Table.from_arrays(
        [
            pa.array([1, 2]),
            pa.array(["upsert", "upsert"]),
            pa.array([1, 1]),
            pa.array([2, 2]),
            pa.array(["test-stream", "test-stream"]),
            pa.array(["A", "B"]),
            pa.array([10, 20]),
        ],
        schema=schema,
    )


def test_state_fragments_are_atomic_and_gap_requires_rebuild():
    state = zippy.KeyValueState(snapshot())
    assert state.apply(delta().slice(0, 1)) == 0
    assert state.snapshot().num_rows == 0
    assert state.apply(delta().slice(1, 1)) == 1
    assert state.sequence == 2
    assert state.apply(delta()) == 0
    assert state.snapshot().column("price").to_pylist() == [10, 20]
    assert state.select([["B"], ["A"]]).column("price").to_pylist() == [20, 10]
    with pytest.raises(RuntimeError, match="absent"):
        state.select([["missing"]])
    broken = zippy.KeyValueState(snapshot())
    with pytest.raises(RuntimeError, match="gap"):
        broken.apply(delta().slice(1, 1))
    with pytest.raises(RuntimeError, match="fresh snapshot"):
        broken.snapshot()


def test_bootstrap_buffers_deltas_during_snapshot_request_and_rejects_reconnect(monkeypatch):
    class FakeSubscriber:
        def __init__(self):
            self.callback = None
            self.reconnects = 0
            self.running = True

        def metrics(self):
            return {"ready": True, "reconnects_total": self.reconnects, "running": self.running}

        def stop(self):
            pass

    subscriber = FakeSubscriber()

    def subscribe(source, callback, **kwargs):
        subscriber.callback = callback
        return subscriber

    def load():
        subscriber.callback(delta())
        return snapshot()

    monkeypatch.setattr(zippy, "subscribe_table", subscribe)
    handle = zippy.StateSubscription("quotes", load).start()
    assert handle.sequence == 2
    assert handle.snapshot().num_rows == 2
    subscriber.reconnects = 1
    with pytest.raises(RuntimeError, match="lost continuity"):
        _ = handle.sequence
    with pytest.raises(RuntimeError, match="lost continuity"):
        handle.snapshot()
    subscriber.reconnects = 0
    subscriber.running = False
    with pytest.raises(RuntimeError, match="no longer live"):
        handle.snapshot()
    handle.stop()
    with pytest.raises(RuntimeError):
        _ = handle.sequence


def test_bootstrap_memory_budget_fails_closed(monkeypatch):
    class FakeSubscriber:
        def metrics(self):
            return {"ready": True}

        def stop(self):
            pass

    callback = None

    def subscribe(source, on_delta, **kwargs):
        nonlocal callback
        callback = on_delta
        return FakeSubscriber()

    def load():
        callback(delta())
        return snapshot()

    monkeypatch.setattr(zippy, "subscribe_table", subscribe)
    with pytest.raises(RuntimeError, match="byte budget"):
        zippy.StateSubscription("quotes", load, max_pending_bytes=1).start()
