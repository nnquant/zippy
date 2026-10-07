"""Local Rust factor execution with native Catalog tables and Flight I/O.

Kernel state belongs to this process. Input delivery resumes while that state survives;
a compute-process restart requires explicit state reconstruction/replay. Durable output recovery
replays exact output bytes and request identities; the kernel is never executed twice for a retry.
Unresolved or nonretryable writes remain terminal.

:example: with ComputePipeline(client, processor, output="factors", output_table_id=identity) as job:
              job.write(batch)
"""

from __future__ import annotations

from dataclasses import replace
import threading
from typing import Literal, cast
import uuid

from loguru import logger
import pyarrow as pa
from zippy_client import Client, FlightSubscriber, LogCursor, SubscriptionOptions
from zippy_client.errors import AmbiguousWriteError, CapabilityError, ProtocolError, ServerError

from ._internal import NativeProcessor
from .source_input import PluginSubscriber, SourcePlugin


class ComputePipeline:
    """Own a factor kernel and publish each output batch through native ingest.

    :param client: Explicit authenticated client; caller retains its lifecycle.
    :param processor: Native kernel whose current state is transferred into this pipeline.
    :param output: Existing Catalog output table name.
    :param output_table_id: Immutable identity required on every writer open.
    :param output_namespace: Output Catalog namespace.
    :param output_kind: append, frame or key_value (complete upsert updates).
    :param durability: Explicit output confirmation boundary; defaults to WAL.
    :param source: Native table name or explicitly owned Python source plugin; omit for write().
    :param input_options: Raw native-log configuration; durable ACK is not a kernel checkpoint.
    :param output_reconnect_attempts: Reconnects per durable output batch; memory never retries.
        Kernel execution is not repeated; the same output bytes and request identity are replayed.
    :param max_batch_bytes: Maximum bytes in a complete input/output unit.
    :raises ValueError: Options would imply unsupported durable kernel recovery.
    :raises CapabilityError: Native identity-fenced output is unavailable.
    :example: job = ComputePipeline(client, kernel, output="bars", output_table_id=table_id)
    """

    def __init__(
        self,
        client: Client,
        processor: NativeProcessor,
        *,
        output: str,
        output_table_id: str | uuid.UUID,
        output_namespace: str = "default",
        output_kind: Literal["append", "frame", "key_value"] = "append",
        durability: Literal["memory", "wal", "persisted"] = "wal",
        source: str | SourcePlugin | None = None,
        input_options: SubscriptionOptions | None = None,
        max_batch_bytes: int = 64 * 1024 * 1024,
        output_reconnect_attempts: int = 5,
    ) -> None:
        options = input_options or SubscriptionOptions()
        if options.consumer_id is not None:
            raise ValueError("durable input ACK cannot replace a stateful kernel checkpoint")
        if output_kind not in {"append", "frame", "key_value"}:
            raise ValueError("output_kind must be append, frame or key_value")
        if durability not in {"memory", "wal", "persisted"}:
            raise ValueError("durability must be memory, wal or persisted")
        if type(max_batch_bytes) is not int or max_batch_bytes <= 0:
            raise ValueError("max_batch_bytes must be a positive integer")
        if type(output_reconnect_attempts) is not int or not 0 <= output_reconnect_attempts <= 10:
            raise ValueError("output_reconnect_attempts must be between 0 and 10")
        recovery_attempts = output_reconnect_attempts if durability != "memory" else 0
        if (
            recovery_attempts
            and "ingest.resume_after_restart.v1" not in client.flight.capabilities.names
        ):
            raise CapabilityError("native compute requires durable producer recovery")
        identity = uuid.UUID(str(output_table_id))
        if identity.int == 0:
            raise ValueError("output_table_id must be non-nil")
        if not output or not output_namespace:
            raise ValueError("output name and namespace must be nonempty")
        if "ingest.table_identity.v1" not in client.flight.capabilities.names:
            raise CapabilityError("native compute requires immutable output table identity guards")
        input_schema = processor.input_schema()
        if source is not None and not isinstance(source, str):
            if not isinstance(source, SourcePlugin):
                raise TypeError("source must be a table name or SourcePlugin")
            if input_options is not None:
                raise ValueError("native log options cannot be applied to a Python source")
            plugin_schema = source._zippy_output_schema()
            if not isinstance(plugin_schema, pa.Schema) or not plugin_schema.equals(
                input_schema, check_metadata=True
            ):
                raise ValueError("source plugin schema differs from the compute kernel")
        if options.expected_schema is not None and not options.expected_schema.equals(
            input_schema, check_metadata=True
        ):
            raise ValueError("subscription expected_schema differs from the compute kernel")
        options = replace(options, expected_schema=input_schema)
        self._client, self._options = client, options
        self._source = source
        self._output, self._namespace = output, output_namespace
        self._output_id, self._kind, self._durability = identity, output_kind, durability
        self._maximum = max_batch_bytes
        self._output_reconnect_attempts = recovery_attempts
        self._lock = threading.Lock()
        self._lifecycle = threading.Lock()
        self._subscription: FlightSubscriber | PluginSubscriber | None = None
        self._state = "created"
        self._error: BaseException | None = None
        self._producer, self._connection = uuid.uuid4(), uuid.uuid4()
        self._sequence = 0
        self._epoch: int | None = None
        self._input_rows = 0
        self._output_rows = 0
        self._receipts: list[dict[str, object]] = []
        self._failed_write_receipts: list[dict[str, object]] = []
        self._processor = processor.transfer()
        self._input_schema = self._processor.input_schema()
        self._output_schema = self._processor.output_schema()

    def start(self) -> ComputePipeline:
        """Start the optional input reader, or enable direct Arrow input.

        :returns: This running pipeline.
        :raises RuntimeError: The pipeline has already stopped or failed.
        """
        with self._lifecycle:
            if self._state == "running":
                return self
            if self._state != "created":
                raise RuntimeError("compute pipeline cannot be restarted")
            self._state = "running"
            if self._source is not None and not isinstance(self._source, str):
                try:
                    self._subscription = PluginSubscriber(self._source, self._process, self.flush)
                    self._subscription.start()
                except BaseException as error:
                    self._fail(error)
                    raise
                return self
            if (
                self._source is not None
                or self._options.table_id is not None
                or self._options.cursor
            ):
                try:
                    self._subscription = self._client.subscribe_table(
                        self._source,
                        self._process,
                        options=self._options,
                    )
                except BaseException as error:
                    self._fail(error)
                    raise
        return self

    def write(self, value: pa.RecordBatch | pa.Table) -> None:
        """Process direct input; source-driven pipelines reject mixed manual input.

        :param value: One complete Arrow input unit with the exact kernel schema.
        :raises RuntimeError: A source already owns input ordering, or processing failed.
        """
        self.start()
        if self._subscription is not None:
            raise RuntimeError("source-driven compute cannot also accept manual input")
        table = pa.Table.from_batches([value]) if isinstance(value, pa.RecordBatch) else value
        self._process(table)

    def _process(self, table: pa.Table) -> None:
        with self._lock:
            self._require_running()
            try:
                if not isinstance(table, pa.Table) or not table.schema.equals(
                    self._input_schema,
                    check_metadata=True,
                ):
                    raise ValueError("compute input schema differs; implicit casts are forbidden")
                if table.get_total_buffer_size() > self._maximum:
                    raise ValueError("compute input exceeds its complete-unit byte limit")
                # A Flight frame can arrive in several fragments. Preserve the original
                # atomic unit before invoking kernels whose batch boundary has meaning.
                batches = table.combine_chunks().to_batches()
                for batch in batches:
                    self._publish(self._processor.process(batch))
                self._input_rows += table.num_rows
            except BaseException as error:
                self._fail(error)
                raise

    def _publish(self, batches: list[pa.RecordBatch]) -> None:
        if sum(batch.get_total_buffer_size() for batch in batches) > self._maximum:
            raise ValueError("compute output exceeds its complete-unit byte limit")
        for batch in batches:
            if not batch.schema.equals(self._output_schema, check_metadata=True):
                raise ProtocolError("compute kernel changed its declared output schema")
            if batch.num_rows == 0:
                continue
            receipts = self._client.flight.write_table(
                self._output,
                pa.Table.from_batches([batch]),
                namespace=self._namespace,
                mutation="upsert" if self._kind == "key_value" else self._kind,
                durability=self._durability,
                producer_id=self._producer,
                connection_id=self._connection,
                first_batch_sequence=self._sequence,
                expected_writer_epoch=self._epoch,
                expected_table_id=self._output_id,
                max_reconnect_attempts=self._output_reconnect_attempts,
            )
            # write_table returns validated receipts, at least one for a nonempty batch.
            self._epoch = cast(int, receipts[-1]["producer_epoch"])
            self._sequence += len(receipts)
            self._receipts = receipts
            self._output_rows += batch.num_rows

    def flush(self) -> None:
        """Explicitly close current aggregation windows and persist their outputs.

        :raises BaseException: A kernel or output write failed; the pipeline becomes terminal.
        """
        with self._lock:
            self._require_running()
            try:
                self._publish(self._processor.flush())
            except BaseException as error:
                self._fail(error)
                raise

    def stop(self, *, flush: bool = True) -> None:
        """Stop input first, then optionally finish buffered windows and release the writer.

        :param flush: False discards pending kernel state without producing further outputs.
        :raises BaseException: The first input, kernel, write or cleanup failure.
        """
        with self._lifecycle:
            if self._state == "stopped":
                return
            if self._subscription is not None:
                try:
                    self._subscription.stop()
                except BaseException as error:
                    self._fail(error)
            with self._lock:
                try:
                    if flush and self._state == "running":
                        self._publish(self._processor.finish())
                except BaseException as error:
                    self._fail(error)
                finally:
                    self._processor.close()
                    try:
                        self._client.flight.close_write(self._connection)
                    except BaseException as error:
                        self._fail(error)
                self._state = "failed" if self._error is not None else "stopped"
            if self._error is not None:
                raise self._error

    def _fail(self, error: BaseException) -> None:
        self._error = self._error or error
        if isinstance(error, (AmbiguousWriteError, ServerError)):
            self._failed_write_receipts = [dict(receipt) for receipt in error.receipts]
        self._state = "failed"
        self._processor.close()
        logger.error("native compute failed output_id=[{}] error=[{}]", self._output_id, error)

    def _require_running(self) -> None:
        if self.status() != "running":
            raise RuntimeError(f"compute pipeline is [{self.status()}]") from self._error

    def status(self) -> str:
        """Return lifecycle state, including terminal input-reader failure or cancellation.

        :returns: created, running, source_stopped, stopped or failed.
        """
        if self._state == "running" and self._subscription is not None:
            source_state = self._subscription.status()
            if source_state == "failed":
                return "failed"
            if source_state == "stopped":
                return "source_stopped"
        return self._state

    def join(self, timeout: float | None = None) -> None:
        """Wait for the source worker and propagate its original terminal failure.

        :param timeout: Maximum wait in seconds; None waits until the source stops.
        :raises BaseException: The first processing or input-reader failure.
        """
        if self._subscription is not None:
            try:
                self._subscription.join(timeout)
            except BaseException as error:
                with self._lock:
                    self._fail(error)
                raise
        if self._error is not None:
            raise self._error

    @property
    def cursor(self) -> LogCursor | None:
        """Return the processed input position; this is not a persisted kernel checkpoint."""
        return None if self._subscription is None else self._subscription.cursor

    def metrics(self) -> dict[str, object]:
        """Report immutable output identity, confirmed output counts and source health."""
        source = None if self._subscription is None else self._subscription.metrics()
        return {
            "state": self.status(),
            "output_table_id": str(self._output_id),
            "input_rows": self._input_rows,
            "output_rows": self._output_rows,
            "next_batch_sequence": self._sequence,
            "durability": self._durability,
            "last_receipts": list(self._receipts),
            "failed_write_receipts": [dict(receipt) for receipt in self._failed_write_receipts],
            "source": source,
            "error": (
                str(self._error)
                if self._error is not None
                else (None if source is None else source.get("last_error"))
            ),
        }

    def __enter__(self) -> ComputePipeline:
        """Start this explicitly owned pipeline."""
        return self.start()

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Finish on success; discard pending windows when the context body failed."""
        self.stop(flush=exc_type is None)
