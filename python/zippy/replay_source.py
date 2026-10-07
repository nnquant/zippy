"""Finite Parquet acquisition using the native source protocol.

Schema and replay timing have one implementation shared by direct native ingestion
and finite source replay. Both publish through the same native ingest path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
import math
import os
from pathlib import Path
import threading
import time

from loguru import logger
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from zippy_client import Row
from zippy_client.row import _table_rows

from .source_input import SourceSink, _SourceCancelled


class _ParquetReplayHandle:
    """Background runtime handle for ``ParquetReplaySource``."""

    def __init__(self, source: ParquetReplaySource, sink: SourceSink) -> None:
        self._source = source
        self._sink = sink
        self._stop_event = threading.Event()
        self._stopped_event = threading.Event()
        self._error: Exception | None = None
        self._completed = False
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-parquet-replay-{source.source_name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Request the replay thread to stop."""
        self._stop_event.set()

    def join(self) -> None:
        """Wait for the replay thread and propagate its original failure."""
        self._thread.join()
        if self._error is not None:
            raise RuntimeError(f"parquet replay failed reason=[{self._error}]") from self._error

    def wait_replay(self, timeout: float | None = None) -> bool:
        """Wait until all replay rows have been emitted and flushed."""
        completed = self._stopped_event.wait(timeout)
        if completed and self._error is not None:
            raise RuntimeError(f"parquet replay failed reason=[{self._error}]") from self._error
        return completed and self._completed

    def _run(self) -> None:
        try:
            self._sink.emit_hello(self._source.source_name)
            rate_limiter = _ReplayRateLimiter(self._source.replay_rate)
            for table in self._source._iter_tables():
                if self._stop_event.is_set():
                    break
                if rate_limiter.enabled:
                    for row_index in range(table.num_rows):
                        if not rate_limiter.wait_before_emit(self._stop_event):
                            break
                        self._sink.emit_data(table.slice(row_index, 1))
                else:
                    self._sink.emit_data(table)
            if not self._stop_event.is_set():
                self._sink.emit_flush()
            self._sink.emit_stop()
            self._completed = not self._stop_event.is_set()
        except _SourceCancelled:
            # The native sink has already accepted a cancellation request.
            pass
        except Exception as error:
            self._error = error
            try:
                self._sink.emit_error(str(error))
            except Exception:
                logger.exception(
                    "parquet source error callback failed source=[{}]", self._source.source_name
                )
        finally:
            self._stopped_event.set()


def _normalize_parquet_replay_paths(value: object) -> str | list[str]:
    if isinstance(value, (str, os.PathLike)):
        return str(Path(value).expanduser())

    if not isinstance(value, Iterable):
        raise TypeError("parquet replay source must be a path or a sequence of paths")
    paths = []
    for item in value:
        if not isinstance(item, (str, os.PathLike)):
            raise TypeError("parquet replay paths must contain only filesystem paths")
        paths.append(str(Path(item).expanduser()))
    if not paths:
        raise ValueError("parquet replay source path list must not be empty")
    return paths


def _normalize_replay_rate(replay_rate: object) -> float | None:
    if replay_rate is not None:
        if isinstance(replay_rate, bool) or not isinstance(replay_rate, (int, float)):
            raise TypeError("replay_rate must be numeric rows per second")
        rate = float(replay_rate)
        if not math.isfinite(rate) or rate <= 0.0:
            raise ValueError("replay_rate must be positive and finite")
        return rate
    return None


class _ReplayRateLimiter:
    def __init__(self, replay_rate: float | None) -> None:
        self.replay_rate = replay_rate
        self.enabled = replay_rate is not None
        self._start_time: float | None = None
        self._emitted_rows = 0

    def wait_before_emit(self, cancel: threading.Event) -> bool:
        if cancel.is_set():
            return False
        if self.replay_rate is None:
            return True
        if self._start_time is None:
            self._start_time = time.monotonic()
            self._emitted_rows = 1
            return True

        target_time = self._start_time + (self._emitted_rows / self.replay_rate)
        delay = target_time - time.monotonic()
        if delay > 0.0 and cancel.wait(delay):
            return False
        self._emitted_rows += 1
        return not cancel.is_set()


def _filter_replay_table(
    table: pa.Table, time_column: str | None, start: object, end: object
) -> pa.Table:
    if start is None and end is None:
        return table
    if time_column is None:
        raise ValueError("time_column is required when start or end is provided")
    if time_column not in table.column_names:
        raise ValueError(f"time_column not found column=[{time_column}]")

    values = table[time_column]
    mask = None
    if start is not None:
        start_scalar = pa.scalar(start, type=values.type)
        mask = pc.greater_equal(values, start_scalar)
    if end is not None:
        end_scalar = pa.scalar(end, type=values.type)
        end_mask = pc.less_equal(values, end_scalar)
        mask = end_mask if mask is None else pc.and_(mask, end_mask)
    return table.filter(mask)


class ParquetReplaySource:
    """
    Replay finite Parquet input through the native source protocol.

    Complete input emits flush/stop and releases its worker. Use with ComputePipeline;
    no Master registration, publisher or default connection is needed.

    :param path: Parquet file, directory, or explicit file list.
    :type path: str | os.PathLike[str] | list[str | os.PathLike[str]]
    :param schema: Optional exact expected Arrow schema; no implicit casts are performed.
    :type schema: pyarrow.Schema | None
    :param batch_size: Maximum rows emitted per batch.
    :type batch_size: int
    :param source_name: Diagnostic identity for the source protocol.
    :type source_name: str | None
    :param replay_rate: Fixed replay rate in rows per second. When provided, fixed-rate replay
        is enabled.
    :type replay_rate: float | None
    :param time_column: Column used for inclusive ``start`` / ``end`` filtering.
    :type time_column: str | None
    :param start: Optional inclusive lower bound for ``time_column``.
    :type start: object
    :param end: Optional inclusive upper bound for ``time_column``.
    :type end: object
    :raises ValueError: Batch size, rate, identity or input schemas differ from the contract.
    :raises TypeError: Paths, schema or numeric arguments have incompatible types.
    :example: ComputePipeline(client, kernel, source=ParquetReplaySource("ticks.parquet"), ...)
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | Iterable[str | os.PathLike[str]],
        schema: pa.Schema | None = None,
        *,
        batch_size: int = 65_536,
        source_name: str | None = None,
        replay_rate: float | None = None,
        time_column: str | None = "dt",
        start: object = None,
        end: object = None,
    ) -> None:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if schema is not None and not isinstance(schema, pa.Schema):
            raise TypeError("schema must be an Arrow schema")
        if source_name is not None and (not isinstance(source_name, str) or not source_name):
            raise ValueError("source_name must be a nonempty string")
        replay_rate = _normalize_replay_rate(replay_rate)
        self.path = _normalize_parquet_replay_paths(path)
        self.schema = schema
        self.batch_size = batch_size
        self.source_name = source_name or "parquet_replay"
        self.replay_rate = replay_rate
        self.time_column = time_column
        self.start_bound = start
        self.end_bound = end
        self._last_handle: _ParquetReplayHandle | None = None
        self._lifecycle = threading.Lock()

    def _zippy_output_schema(self) -> pa.Schema:
        if self.schema is None:
            self.schema = self._open_dataset().schema
        else:
            self._open_dataset()
        return self.schema

    def _zippy_source_name(self) -> str:
        return self.source_name

    def _zippy_source_type(self) -> str:
        return "parquet_replay"

    def _zippy_start(self, sink: SourceSink) -> _ParquetReplayHandle:
        with self._lifecycle:
            if self._last_handle is not None and not self._last_handle._stopped_event.is_set():
                raise RuntimeError("parquet replay source already has an active owner")
            self._zippy_output_schema()
            handle = _ParquetReplayHandle(self, sink)
            self._last_handle = handle
            return handle

    def wait_replay(self, timeout: float | None = None) -> bool:
        """
        Wait until the active replay handle has emitted and flushed all rows.

        :param timeout: Maximum seconds to wait. ``None`` waits indefinitely.
        :type timeout: float | None
        :returns: ``True`` after complete replay, ``False`` on timeout or cancellation.
        :rtype: bool
        :raises RuntimeError: If the replay thread failed.
        """
        if self._last_handle is None:
            raise RuntimeError("parquet replay source is not started")
        return self._last_handle.wait_replay(timeout)

    def _open_dataset(self) -> ds.Dataset:
        dataset = ds.dataset(self.path, format="parquet")
        expected = self.schema if self.schema is not None else dataset.schema
        if not dataset.schema.equals(expected, check_metadata=True):
            raise ValueError("parquet replay schema differs from the expected schema")
        for fragment in dataset.get_fragments():
            if not fragment.physical_schema.equals(expected, check_metadata=True):
                raise ValueError("parquet replay fragments have inconsistent schemas")
        return dataset

    def _iter_tables(self) -> Iterator[pa.Table]:
        dataset = self._open_dataset()
        scanner = dataset.scanner(batch_size=self.batch_size)
        expected_schema = self.schema if self.schema is not None else dataset.schema
        for batch in scanner.to_batches():
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            if not table.schema.equals(expected_schema, check_metadata=True):
                raise ValueError("parquet replay schema changed during acquisition")
            table = _filter_replay_table(
                table,
                self.time_column,
                self.start_bound,
                self.end_bound,
            )
            if table.num_rows == 0:
                continue
            yield table


class ParquetReplayEngine:
    """Deliver finite Parquet input directly to row or complete-table callbacks.

    :param source: Explicit filesystem path or ordered path collection.
    :param callback: Receives Row values or Arrow Tables when table_callback is true.
    :param table_callback: Keep complete Arrow batch boundaries instead of delivering rows.
    :param schema: Exact expected schema, including metadata and nullability.
    :param batch_size: Positive maximum source batch rows.
    :param source_name: Source diagnostic identity.
    :param replay_rate: Optional positive finite rows per second.
    :param time_column: Column for inclusive start/end filtering.
    :param start: Optional inclusive lower bound.
    :param end: Optional inclusive upper bound.
    :raises RuntimeError: Source acquisition or user callback fails.
    :example: ParquetReplayEngine("ticks.parquet", callback=print).run()
    """

    def __init__(
        self,
        source: str | os.PathLike[str] | Iterable[str | os.PathLike[str]],
        *,
        callback: Callable[..., None],
        table_callback: bool = False,
        schema: pa.Schema | None = None,
        batch_size: int = 65_536,
        source_name: str | None = None,
        replay_rate: float | None = None,
        time_column: str | None = "dt",
        start: object = None,
        end: object = None,
    ) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        self._source = ParquetReplaySource(
            source,
            schema=schema,
            batch_size=batch_size,
            source_name=source_name,
            replay_rate=replay_rate,
            time_column=time_column,
            start=start,
            end=end,
        )
        self._callback = callback
        self._table_callback = table_callback
        self._handle: _ParquetReplayHandle | None = None
        self._status = "created"
        self._cancelled = False
        self._rows = self._batches = self._started_ns = self._finished_ns = 0

    def init(self) -> ParquetReplayEngine:
        """Validate the finite source without acquiring a worker or emitting rows."""
        if self._status == "created":
            self._source._zippy_output_schema()
            self._status = "initialized"
        elif self._status not in {"initialized", "running"}:
            raise RuntimeError("replay engine cannot be initialized after termination")
        return self

    def start(self) -> ParquetReplayEngine:
        """Acquire the finite source once; completed or failed engines cannot restart."""
        if self._status == "running":
            return self
        self.init()
        self._status = "running"
        self._started_ns = time.perf_counter_ns()
        try:
            self._handle = self._source._zippy_start(self)
            if self._cancelled:
                self._handle.stop()
        except BaseException:
            self._status = "failed"
            raise
        return self

    def run(self, *, wait: bool = True, timeout: float | None = None) -> ParquetReplayEngine:
        """Start and optionally await completion; a timed out blocking run cancels its worker."""
        self.start()
        if wait and not self.wait(timeout):
            if self._cancelled:
                return self
            self.stop()
            raise TimeoutError("callback replay did not finish")
        return self

    def wait(self, timeout: float | None = None) -> bool:
        """Return completion status and propagate source/callback failures with their cause."""
        try:
            return self._source.wait_replay(timeout)
        except RuntimeError as error:
            self._status = "failed"
            raise RuntimeError(f"callback replay failed reason=[{error}]") from error

    def stop(self) -> None:
        """Cancel and join acquisition; callbacks may request cancellation without self-joining."""
        self._cancelled = True
        if self._handle is not None:
            self._handle.stop()
            if threading.current_thread() is not self._handle._thread:
                self._handle.join()
        if self._status not in {"completed", "failed"}:
            self._status = "stopped"

    def output_schema(self) -> pa.Schema:
        """Return the validated source schema."""
        return self._source._zippy_output_schema()

    def status(self) -> str:
        """Return created, initialized, running, completed, stopped or failed."""
        return self._status

    def metrics(self) -> dict[str, object]:
        """Return delivered rows/callbacks and elapsed monotonic nanoseconds."""
        elapsed = (
            (self._finished_ns or time.perf_counter_ns()) - self._started_ns
            if self._started_ns
            else 0
        )
        return {
            "callback_mode": "table" if self._table_callback else "row",
            "rows_delivered_total": self._rows,
            "batches_delivered_total": self._batches,
            "elapsed_ns": max(0, elapsed),
            "rows_per_callback": self._rows / self._batches if self._batches else 0.0,
        }

    def emit_hello(self, stream_name: str, protocol_version: int = 1) -> None:
        """Validate the owned source's protocol identity."""
        if stream_name != self._source.source_name or protocol_version != 1:
            raise ValueError("replay source identity or protocol differs")

    def emit_data(self, value: pa.Table | pa.RecordBatch) -> None:
        """Deliver a complete source batch through the shared row conversion contract."""
        table = pa.Table.from_batches([value]) if isinstance(value, pa.RecordBatch) else value
        if self._table_callback:
            if not self._cancelled:
                self._callback(table)
                self._rows += table.num_rows
                self._batches += 1
        else:
            for values in _table_rows(table):
                if self._cancelled:
                    break
                self._callback(Row(values))
                self._rows += 1
                self._batches += 1

    def emit_flush(self) -> None:
        """Callbacks have no additional buffered state to flush."""

    def emit_stop(self) -> None:
        """Mark finite input complete without owning or closing any server table."""
        self._status = "stopped" if self._cancelled else "completed"
        self._finished_ns = time.perf_counter_ns()

    def emit_error(self, reason: str) -> None:
        """Keep the engine failed; the source handle retains the original exception."""
        self._status = "failed"
        self._finished_ns = time.perf_counter_ns()

    def __enter__(self) -> ParquetReplayEngine:
        return self.init()

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if isinstance(exc_value, BaseException):
            try:
                self.stop()
            except Exception as cleanup_error:
                exc_value.add_note(f"replay cleanup also failed reason=[{cleanup_error}]")
        else:
            self.stop()
