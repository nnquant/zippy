"""Native compute consumes existing source protocols with explicit lifecycle ownership."""

from types import SimpleNamespace
from typing import cast
import threading
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from zippy_client import Client, SubscriptionOptions

import zippy
from zippy.source_input import PluginSubscriber
from test_native_compute import FakeFlight, SCHEMA, batch, reactive


class FinishedHandle:
    def stop(self):
        pass

    def join(self):
        pass


class Source:
    def __init__(self, emit):
        self.emit = emit

    def _zippy_output_schema(self):
        return SCHEMA

    def _zippy_source_name(self):
        return "input"

    def _zippy_start(self, sink):
        self.emit(sink)
        return FinishedHandle()


def test_stop_before_start_releases_join_and_never_acquires_source():
    source = Source(lambda _: pytest.fail("stopped input must not acquire a source"))
    sub = PluginSubscriber(source, lambda _: None, lambda: None)
    sub.stop()
    assert sub._done.is_set()
    sub.join(0)
    assert sub.status() == "stopped"
    with pytest.raises(RuntimeError, match="only be started once"):
        sub.start()


def test_cancel_during_acquisition_stops_handle_when_it_becomes_available():
    acquiring, release, cancelled = (threading.Event() for _ in range(3))

    class Handle:
        def stop(self):
            cancelled.set()

        def join(self):
            assert cancelled.wait(2)

    class DelayedSource(Source):
        def _zippy_start(self, sink):
            acquiring.set()
            assert release.wait(2)
            return Handle()

    sub = PluginSubscriber(DelayedSource(lambda _: None), lambda _: None, lambda: None)
    errors = []

    def acquire():
        try:
            sub.start()
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=acquire)
    worker.start()
    try:
        assert acquiring.wait(2)
        sub.stop()
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive() and not errors
    assert cancelled.is_set() and sub._done.wait(2)
    assert sub.status() == "stopped"


def pipeline(source, flight=None):
    return zippy.ComputePipeline(
        cast(Client, SimpleNamespace(flight=flight or FakeFlight())),
        reactive(),
        output="factors",
        output_table_id=uuid.UUID(int=42),
        source=source,
    )


def test_synchronous_source_preserves_kernel_order_and_output_confirmation():
    flight = FakeFlight()

    def emit(sink):
        sink.emit_hello("input")
        sink.emit_data(batch([1.0]))
        assert len(flight.writes) == 1
        sink.emit_data(batch([3.0]))
        sink.emit_flush()
        sink.emit_stop()

    with pipeline(Source(emit), flight) as job:
        job.join(1)
        assert job.status() == "source_stopped"
        assert job.cursor is None
        assert job.metrics()["input_rows"] == 2
        assert flight.writes[-1][0].column("mean").to_pylist() == [2.0]
        with pytest.raises(RuntimeError, match="manual input"):
            job.write(batch([5.0]))
    assert job.status() == "stopped"


@pytest.mark.parametrize("event", ["missing_hello", "wrong_hello", "bad_data", "missing_stop"])
def test_invalid_source_protocol_fails_without_silent_recovery(event):
    def emit(sink):
        if event == "missing_hello":
            sink.emit_data(batch([1.0]))
        else:
            sink.emit_hello("wrong" if event == "wrong_hello" else "input")
            if event == "bad_data":
                sink.emit_data({"price": 1.0})

    job = pipeline(Source(emit))
    with pytest.raises((RuntimeError, ValueError, TypeError)):
        job.start().join(1)
    assert job.status() == "failed"
    with pytest.raises((RuntimeError, ValueError, TypeError)):
        job.stop(flush=False)


def test_source_schema_rejection_keeps_kernel_ownership():
    source = Source(lambda sink: None)
    source._zippy_output_schema = lambda: SCHEMA.with_metadata({"wrong": "schema"})
    kernel = reactive()
    with pytest.raises(ValueError, match="plugin schema"):
        zippy.ComputePipeline(
            cast(Client, SimpleNamespace(flight=FakeFlight())),
            kernel,
            source=source,
            output="out",
            output_table_id=uuid.uuid4(),
        )
    assert kernel.process(batch([1.0]))
    kernel.close()
    with pytest.raises(ValueError, match="log options"):
        zippy.ComputePipeline(
            cast(Client, SimpleNamespace(flight=FakeFlight())),
            reactive(),
            source=Source(lambda sink: None),
            output="out",
            output_table_id=uuid.uuid4(),
            input_options=SubscriptionOptions(),
        )


def test_plugin_join_failure_is_terminal():
    failure = OSError("acquisition failed")

    class Handle(FinishedHandle):
        def join(self):
            raise failure

    source = Source(lambda sink: None)
    source._zippy_start = lambda sink: Handle()
    sub = PluginSubscriber(source, lambda table: None, lambda: None)
    with pytest.raises(OSError) as caught:
        sub.start().join(1)
    assert caught.value is failure


def test_stuck_worker_has_bounded_stop_wait():
    release = threading.Event()

    class Handle(FinishedHandle):
        def join(self):
            release.wait(3)

    source = Source(lambda sink: None)
    source._zippy_start = lambda sink: Handle()
    sub = PluginSubscriber(source, lambda table: None, lambda: None, stop_timeout_sec=0.01)
    sub.start()
    try:
        with pytest.raises(TimeoutError):
            sub.stop()
    finally:
        release.set()
        assert sub._done.wait(1)
    assert sub.status() == "failed"


def test_parquet_source_feeds_native_kernel_without_master(tmp_path):
    path = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_batches([batch([1.0, 3.0, 5.0])]), path)
    source = zippy.ParquetReplaySource(path, batch_size=1)
    flight = FakeFlight()
    with pipeline(source, flight) as job:
        assert source.wait_replay(3)
        assert job.metrics()["input_rows"] == 3
        assert [item[0].column("mean").to_pylist()[0] for item in flight.writes] == [None, 2.0, 4.0]
    assert job.status() == "stopped"


def test_output_failure_stops_plugin_without_reexecuting_input(tmp_path):
    path = tmp_path / "input.parquet"
    pq.write_table(pa.Table.from_batches([batch([1.0, 3.0])]), path)
    source = zippy.ParquetReplaySource(path, batch_size=1)
    flight = FakeFlight()
    failure = ValueError("output rejected")
    flight.failure = failure
    job = pipeline(source, flight)
    job.start()
    with pytest.raises(RuntimeError, match="output rejected"):
        source.wait_replay(3)
    with pytest.raises(ValueError) as caught:
        job.join(1)
    assert caught.value is failure and len(flight.writes) == 1
    with pytest.raises(ValueError) as caught:
        job.stop(flush=False)
    assert caught.value is failure


def test_cancellation_rejects_late_plugin_delivery_without_recording_failure():
    ready, cancelled = threading.Event(), threading.Event()

    class Handle:
        def __init__(self, sink):
            def run():
                sink.emit_hello("input")
                ready.set()
                assert cancelled.wait(3)
                try:
                    sink.emit_data(batch([99.0]))
                except RuntimeError as error:
                    sink.emit_error(str(error))

            self.worker = threading.Thread(target=run)
            self.worker.start()

        def stop(self):
            cancelled.set()

        def join(self):
            self.worker.join()

    source = Source(lambda sink: None)
    source._zippy_start = Handle
    flight = FakeFlight()
    job = pipeline(source, flight).start()
    assert ready.wait(1)
    job.stop()
    assert job.status() == "stopped" and not flight.writes
