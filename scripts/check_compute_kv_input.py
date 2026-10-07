"""Verify native KV event computation, tombstones and continuous restart replay.

:example: Called by scripts/check_native_client.py against its isolated real server.
"""

from collections.abc import Callable
import time
import uuid

import pyarrow as pa
from zippy import ComputePipeline, ExpressionFactor, KeyValueState, NativeProcessor, TsMeanSpec
from zippy_client import Client, ContinuityError, SubscriptionOptions


def verify_compute_kv_input(
    client: Client, namespace_id: uuid.UUID, restart: Callable[[], None]
) -> None:
    """Retain the old complete-changelog input contract over native Flight.

    :param client: Explicit client connected to the isolated server under test.
    :param namespace_id: Existing market namespace.
    :param restart: Restart storage while preserving this process and its kernels.
    :raises AssertionError: Mutation boundaries, tombstones or processing cursors change.
    """
    logical = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    source_id, output_id = uuid.UUID(int=4600), uuid.UUID(int=4601)
    client.create_key_value_table(
        "compute_kv_input",
        logical,
        keys=["symbol"],
        namespace_id=namespace_id,
        table_id=source_id,
        request_id=uuid.UUID(int=4602),
    )
    initial = client.snapshot_state(table_id=source_id)
    state = KeyValueState(initial)
    with client.open_log(table_id=source_id) as reader:
        event_schema = reader.schema

    # A logical-schema kernel must fail even while this source is completely empty.
    wrong = ComputePipeline(
        client,
        NativeProcessor.latest("wrong", logical, ["symbol"]),
        output="compute_kv_events",
        output_table_id=output_id,
        output_namespace="market",
        input_options=SubscriptionOptions(table_id=source_id),
    )
    try:
        wrong.start()
    except ContinuityError as error:
        assert "expected_schema" in str(error)
    else:
        raise AssertionError("logical schema accepted as a KV changelog schema")
    try:
        wrong.stop(flush=False)
    except ContinuityError:
        pass
    assert wrong.status() == "failed" and wrong.metrics()["output_rows"] == 0

    processor = NativeProcessor.reactive(
        "kv_events",
        event_schema,
        "symbol",
        [
            ExpressionFactor(expression="CAST(_zippy_kv_seq, 'float64')", output="event_sequence"),
            TsMeanSpec(
                id_column="symbol",
                value_column="event_sequence",
                window=2,
                output="mean_event_sequence",
            ),
        ],
    )
    client.create_append_table(
        "compute_kv_events",
        processor.output_schema(),
        namespace_id=namespace_id,
        table_id=output_id,
        request_id=uuid.UUID(int=4603),
    )

    def upsert(symbols: list[str], prices: list[float]) -> None:
        client.flight.write_table(
            "compute_kv_input",
            pa.Table.from_pydict({"symbol": symbols, "price": prices}, schema=logical),
            namespace="market",
            durability="persisted",
            mutation="upsert",
            expected_table_id=source_id,
            max_reconnect_attempts=5,
        )

    def delete(symbols: list[str]) -> None:
        client.flight.write_table(
            "compute_kv_input",
            pa.table({"symbol": symbols}),
            namespace="market",
            durability="persisted",
            mutation="delete",
            expected_table_id=source_id,
            max_reconnect_attempts=5,
        )

    # Earliest explicitly replays preexisting mutations; latest is not a bootstrap snapshot.
    upsert(["A", "B"], [1.0, 8.0])
    with ComputePipeline(
        client,
        processor,
        output="compute_kv_events",
        output_table_id=output_id,
        output_namespace="market",
        durability="persisted",
        input_options=SubscriptionOptions(table_id=source_id, start="earliest", batch_rows=1),
    ) as job:

        def wait(offset: int) -> None:
            deadline = time.monotonic() + 15
            while job.cursor is None or job.cursor.next_offset != offset:
                job.join(0)
                assert time.monotonic() < deadline, job.metrics()
                time.sleep(0.005)

        wait(2)
        assert job.metrics()["next_batch_sequence"] == 1  # Two fragments, one computation.
        upsert(["A"], [3.0])
        wait(3)
        restart()
        delete(["B"])
        wait(4)
        delete(["A"])
        wait(5)
        empty_events = (
            client.read_table("compute_kv_events", namespace="market")
            .sort("_zippy_kv_seq")
            .collect()
        )
        # Replay the computed event envelope through the existing native state machine.
        # Derived columns are not part of the original delta protocol.
        for start, length in [(0, 2), (2, 1), (3, 1), (4, 1)]:
            delta = empty_events.slice(start, length).select(event_schema.names)
            state.apply(delta.replace_schema_metadata(event_schema.metadata))
        assert state.sequence == 5 and state.snapshot().num_rows == 0
        upsert(["A"], [9.0])
        wait(6)
        result = (
            client.read_table("compute_kv_events", namespace="market")
            .sort("_zippy_kv_seq")
            .collect()
        )
        assert result.column("_zippy_kv_seq").to_pylist() == [1, 2, 3, 4, 5, 6]
        assert result.column("_zippy_kv_op").to_pylist() == [
            "upsert",
            "upsert",
            "upsert",
            "delete",
            "delete",
            "upsert",
        ]
        assert result.column("_zippy_kv_snapshot_version").to_pylist() == [1, 1, 2, 3, 4, 5]
        assert result.column("_zippy_kv_batch_last_seq").to_pylist() == [2, 2, 3, 4, 5, 6]
        assert result.column("_zippy_kv_stream_id").to_pylist() == [source_id.hex] * 6
        means = result.column("mean_event_sequence").to_pylist()
        assert means == [None, None, 2.0, 3.0, 4.0, 5.5], means
        last = result.slice(5).select(event_schema.names)
        state.apply(last.replace_schema_metadata(event_schema.metadata))
        assert state.snapshot().to_pydict() == {"symbol": ["A"], "price": [9.0]}
        assert job.metrics()["input_rows"] == job.metrics()["output_rows"] == 6
        source_metrics = job.metrics()["source"]
        assert isinstance(source_metrics, dict) and source_metrics["reconnects_total"] >= 1
