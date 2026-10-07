"""Parquet source schema, finite completion, cancellation and failure contracts."""

from pathlib import Path
import threading
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from zippy import ParquetReplayEngine, ParquetReplaySource
import zippy.replay_source as replay_source


class Sink:
    def __init__(self):
        self.events = []
        self.tables = []
        self.first = threading.Event()

    def emit_hello(self, stream_name, protocol_version=1):
        self.events.append(("hello", stream_name, protocol_version))

    def emit_data(self, table):
        self.tables.append(table)
        self.first.set()

    def emit_flush(self):
        self.events.append("flush")

    def emit_stop(self):
        self.events.append("stop")

    def emit_error(self, reason):
        self.events.append(("error", reason))


@pytest.fixture
def parquet_input(tmp_path: Path):
    data = pa.table({"value": [1, None, 1], "dt": [0, 1, 2]})
    path = tmp_path / "input.parquet"
    pq.write_table(data, path)
    return path, data


def test_finite_source_finishes_without_external_stop_and_releases_owner(parquet_input):
    path, data = parquet_input
    source = ParquetReplaySource(path, batch_size=2)
    for _ in range(2):
        sink = Sink()
        handle = source._zippy_start(sink)
        assert source.wait_replay(2)
        assert handle._stopped_event.wait(2)
        handle.join()
        assert sink.events == [("hello", "parquet_replay", 1), "flush", "stop"]
        assert [table.num_rows for table in sink.tables] == [2, 1]
        assert pa.concat_tables(sink.tables).equals(data, check_metadata=True)


def test_low_rate_cancel_interrupts_wait_without_delivering_an_extra_row(parquet_input):
    path, _ = parquet_input
    source = ParquetReplaySource(path, replay_rate=1 / 3600)
    sink = Sink()
    handle = source._zippy_start(sink)
    assert sink.first.wait(2)
    handle.stop()
    assert handle._stopped_event.wait(2), "cancellation must interrupt an hour-long delay"
    handle.join()
    assert len(sink.tables) == 1
    assert source.wait_replay(0) is False
    assert sink.events[-1] == "stop" and "flush" not in sink.events


def test_source_rejects_overlapping_owners_before_delivery(parquet_input):
    path, _ = parquet_input
    source = ParquetReplaySource(path, replay_rate=1 / 3600)
    sink = Sink()
    handle = source._zippy_start(sink)
    try:
        assert sink.first.wait(2)
        second = Sink()
        with pytest.raises(RuntimeError, match="active owner"):
            source._zippy_start(second)
        assert not second.events
    finally:
        handle.stop()
        assert handle._stopped_event.wait(2)
        handle.join()


def test_original_failure_survives_error_callback_failure_and_stopped_text(parquet_input):
    path, _ = parquet_input
    primary = ValueError("actual callback failure status=[stopped]")

    class FailedSink(Sink):
        def emit_data(self, table):
            raise primary

        def emit_error(self, reason):
            raise RuntimeError("secondary reporting failure")

    source = ParquetReplaySource(path)
    handle = source._zippy_start(FailedSink())
    for wait in (lambda: source.wait_replay(2), handle.join):
        with pytest.raises(RuntimeError, match="actual callback failure") as raised:
            wait()
        assert raised.value.__cause__ is primary


@pytest.mark.parametrize("mismatch", ["type", "metadata", "nullability"])
def test_expected_schema_rejects_casts_and_metadata_before_start(parquet_input, mismatch):
    path, data = parquet_input
    if mismatch == "type":
        schema = pa.schema([("value", pa.float64()), ("dt", pa.int64())])
    elif mismatch == "metadata":
        schema = data.schema.with_metadata({"version": "different"})
    else:
        schema = pa.schema([pa.field("value", pa.int64(), nullable=False), ("dt", pa.int64())])
    source = ParquetReplaySource(path, schema=schema)
    sink = Sink()
    with pytest.raises(ValueError, match="schema differs"):
        source._zippy_start(sink)
    assert not sink.events


def test_fragment_mismatch_rejected_before_dataset_can_cast(tmp_path):
    first, second = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"value": pa.array([1], type=pa.int64())}), first)
    pq.write_table(pa.table({"value": pa.array([1.5], type=pa.float64())}), second)
    source = ParquetReplaySource([first, second])
    with pytest.raises(ValueError, match="inconsistent schemas"):
        source._zippy_output_schema()


@pytest.mark.parametrize("rate", [float("nan"), float("inf"), -1, 0, True, "10"])
def test_invalid_rates_do_not_start_workers(parquet_input, rate):
    with pytest.raises((TypeError, ValueError), match="replay_rate"):
        ParquetReplaySource(parquet_input[0], replay_rate=rate)


@pytest.mark.parametrize("size", [True, 1.5, 0, -1])
def test_batch_size_requires_positive_integer(parquet_input, size):
    with pytest.raises(ValueError, match="positive integer"):
        ParquetReplaySource(parquet_input[0], batch_size=size)


def test_rate_limiter_preserves_absolute_timing_and_cancelled_position(monkeypatch):
    now = [100.0]
    delays = []
    monkeypatch.setattr(replay_source, "time", SimpleNamespace(monotonic=lambda: now[0]))

    class ClockCancel(threading.Event):
        def wait(self, timeout=None):
            delays.append(timeout)
            now[0] += timeout
            return self.is_set()

    cancel = ClockCancel()
    limiter = replay_source._ReplayRateLimiter(10)
    emitted = []
    for _ in range(3):
        assert limiter.wait_before_emit(cancel)
        emitted.append(now[0])
    assert emitted == pytest.approx([100.0, 100.1, 100.2])
    assert delays == pytest.approx([0.1, 0.1])
    cancel.set()
    assert not limiter.wait_before_emit(cancel)
    assert limiter._emitted_rows == 3


def test_callback_filtering_and_cancellation_keep_exact_delivery_counts(parquet_input):
    path, _ = parquet_input
    rows = []
    engine = ParquetReplayEngine(path, callback=rows.append, time_column="dt", start=1, end=1)
    engine.init()
    assert engine.status() == "initialized" and not rows
    engine.run()
    assert [row.to_dict() for row in rows] == [{"value": None, "dt": 1}]
    assert (
        engine.metrics()["rows_delivered_total"] == engine.metrics()["batches_delivered_total"] == 1
    )
    assert engine.status() == "completed"
    with pytest.raises(RuntimeError, match="after termination"):
        engine.start()

    received = []

    def cancel_after_first(row):
        received.append(row)
        cancelled.stop()

    cancelled = ParquetReplayEngine(path, callback=cancel_after_first, replay_rate=1 / 3600)
    cancelled.run(timeout=2)
    assert len(received) == 1 and cancelled.status() == "stopped"
    assert cancelled._handle._stopped_event.wait(2)


def test_callback_timeout_cancels_worker_and_context_preserves_primary_error(parquet_input):
    path, _ = parquet_input
    engine = ParquetReplayEngine(path, callback=lambda _: None, replay_rate=1 / 3600)
    with pytest.raises(TimeoutError, match="did not finish"):
        engine.run(timeout=0.01)
    assert engine.status() == "stopped" and engine._handle._stopped_event.is_set()

    def fail(_):
        raise ValueError("original callback failure")

    with pytest.raises(RuntimeError, match="callback replay failed") as error:
        with ParquetReplayEngine(path, callback=fail) as failed:
            failed.run()
    assert failed.status() == "failed"
    assert any("cleanup also failed" in note for note in error.value.__notes__)
