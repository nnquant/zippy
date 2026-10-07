"""Verify plugin acquisition reaches the native three-kind output authority."""

from pathlib import Path
from contextlib import ExitStack
from typing import Literal
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from zippy import (
    ComputePipeline,
    ExpressionFactor,
    NativeProcessor,
    ParquetReplaySource,
    TsMeanSpec,
)
from zippy_client import Client, SubscriptionOptions


def verify_compute_plugin(
    client: Client, namespace_id: uuid.UUID, directory: Path, iteration: int
) -> None:
    """Read local Parquet through the source protocol and persist three native outputs.

    :param client: Explicit client of the isolated native server.
    :param namespace_id: Existing market namespace.
    :param directory: Temporary acceptance directory; no production source files are changed.
    :param iteration: Storage restart generation; only generation zero acquires plugin input.
    :raises AssertionError: Native computation, ordering or output persistence differs.
    """
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    source_path = directory / "compute-plugin-input.parquet"
    if iteration == 0:
        pq.write_table(
            pa.table({"symbol": ["A"] * 3, "price": [1.0, 3.0, 5.0]}, schema=schema), source_path
        )
    kinds: tuple[Literal["append", "frame", "key_value"], ...] = ("append", "frame", "key_value")

    def check(name: str, kind: str) -> None:
        rows = client.read_table(name, namespace="market").sort("price").collect().to_pylist()
        expected = [
            {"symbol": "A", "price": p, "mean": m, "twice": p * 2}
            for p, m in [(1.0, None), (3.0, 2.0), (5.0, 4.0)]
        ]
        assert rows == (expected[-1:] if kind == "key_value" else expected), rows
        if kind == "frame":
            with client.open_log(
                name,
                namespace="market",
                start="earliest",
                batch_rows=1,
                deadline_ns=time.time_ns() + 5_000_000_000,
            ) as reader:
                first, last = next(reader), next(reader)
                assert first.table.num_rows == 2 and last.table.num_rows == 1
                assert first.cursor.next_offset == 2 and last.cursor.next_offset == 3

    for index, kind in enumerate(kinds):
        name, table_id = f"compute_plugin_{kind}", uuid.UUID(int=4700 + index)
        if iteration:
            check(name, kind)
            continue
        mean = NativeProcessor.reactive(
            "plugin_mean",
            schema,
            "symbol",
            [TsMeanSpec(id_column="symbol", value_column="price", window=2, output="mean")],
        )
        derived = NativeProcessor.reactive(
            "derived",
            mean.output_schema(),
            "symbol",
            [ExpressionFactor(expression="price * 2.0", output="twice")],
        )
        processor = NativeProcessor.chain(
            "plugin_chain", [NativeProcessor.identity("raw", schema), mean, derived]
        )
        options = dict(
            namespace_id=namespace_id, table_id=table_id, request_id=uuid.UUID(int=4710 + index)
        )
        if kind == "append":
            client.create_append_table(
                name,
                processor.output_schema(),
                namespace_id=namespace_id,
                table_id=table_id,
                request_id=options["request_id"],
            )
        elif kind == "frame":
            client.create_frame_table(name, processor.output_schema(), by="symbol", **options)
        else:
            client.create_key_value_table(
                name, processor.output_schema(), keys=["symbol"], **options
            )
        source = ParquetReplaySource(source_path, batch_size=2)
        observed: list[pa.Table] = []
        with ExitStack() as stack:
            subscriber = None
            if kind == "frame":
                subscriber = client.subscribe_table(
                    name,
                    observed.append,
                    options=SubscriptionOptions(namespace="market", start="latest"),
                )
                stack.callback(subscriber.stop)
            job = stack.enter_context(
                ComputePipeline(
                    client,
                    processor,
                    source=source,
                    output=name,
                    output_table_id=table_id,
                    output_namespace="market",
                    output_kind=kind,
                    durability="persisted",
                )
            )
            assert source.wait_replay(10)
            job.join(2)
            assert job.status() == "source_stopped"
            assert job.metrics()["input_rows"] == job.metrics()["output_rows"] == 3
            assert job.cursor is None
            check(name, kind)
            if subscriber is not None:
                deadline = time.monotonic() + 5
                while subscriber.cursor is None or subscriber.cursor.next_offset < 3:
                    subscriber.join(0)
                    assert time.monotonic() < deadline, subscriber.metrics()
                    time.sleep(0.002)
                assert [part.num_rows for part in observed] == [2, 1]
        assert job.status() == "stopped"
        check(name, kind)

    raw_schema = pa.schema([("number", pa.int64()), ("text", pa.string())])
    raw = pa.table({"number": [7, None, 7], "text": ["x", "y", "x"]}, schema=raw_schema)
    if iteration == 0:
        raw_path = directory / "plain-producer.parquet"
        pq.write_table(raw, raw_path)
        raw_id = uuid.UUID(int=4740)
        client.create_append_table(
            "compute_plugin_identity",
            raw_schema,
            namespace_id=namespace_id,
            table_id=raw_id,
            request_id=uuid.UUID(int=4741),
        )
        raw_source = ParquetReplaySource(raw_path, batch_size=2)
        with ComputePipeline(
            client,
            NativeProcessor.identity("identity", raw_schema),
            source=raw_source,
            output="compute_plugin_identity",
            output_table_id=raw_id,
            output_namespace="market",
            durability="persisted",
        ):
            assert raw_source.wait_replay(10)
    actual = client.read_table("compute_plugin_identity", namespace="market").collect()
    assert actual.sort_by("text").equals(raw.sort_by("text")), actual
