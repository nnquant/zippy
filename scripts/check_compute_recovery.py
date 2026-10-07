"""Real native compute recovery acceptance with an unchanged Python kernel owner.

:example: uv run --no-sync scripts/check_native_client.py
"""

from collections.abc import Callable
from contextlib import ExitStack
from typing import Any, Literal
import uuid
import time

import pyarrow as pa
from zippy_client import Client
from zippy_client.errors import ZippyConnectionError


def verify_compute_recovery(
    client: Client, namespace_id: uuid.UUID, restart: Callable[[], None]
) -> None:
    """Keep kernels alive across restart and lose a receipt after its real durable commit.

    :param client: Native service client with explicit read/writer/admin credentials.
    :param namespace_id: Existing namespace for isolated acceptance outputs.
    :param restart: Stop and restart the same service against the same durable directory.
    :raises AssertionError: Replay duplicates output, reruns a kernel or changes producer epoch.
    """
    from zippy import ComputePipeline, NativeProcessor, TsMeanSpec

    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with ExitStack() as stack:
        jobs = []
        kinds: tuple[Literal["append", "frame", "key_value"], ...] = (
            "append",
            "frame",
            "key_value",
        )
        for index, kind in enumerate(kinds):
            kernel = NativeProcessor.reactive(
                "resume_mean",
                schema,
                "symbol",
                [TsMeanSpec(id_column="symbol", value_column="price", window=2, output="mean")],
            )
            name, identity = f"resume_compute_{kind}", uuid.UUID(int=13000 + index)
            output_schema = kernel.output_schema()
            if kind == "append":
                client.create_append_table(
                    name,
                    output_schema,
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=uuid.uuid4(),
                )
            elif kind == "frame":
                client.create_frame_table(
                    name,
                    output_schema,
                    by="symbol",
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=uuid.uuid4(),
                )
            else:
                client.create_key_value_table(
                    name,
                    output_schema,
                    keys=["symbol"],
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=uuid.uuid4(),
                )
            job = stack.enter_context(
                ComputePipeline(
                    client,
                    kernel,
                    output=name,
                    output_namespace="market",
                    output_table_id=identity,
                    output_kind=kind,
                    durability="wal",
                )
            )
            job.write(pa.table({"symbol": ["A"], "price": [1.0]}, schema=schema))
            receipts = job.metrics()["last_receipts"]
            assert isinstance(receipts, list)
            jobs.append((kind, name, job, receipts[0]["producer_epoch"]))
        restart()
        for kind, name, job, epoch in jobs:
            original = client.flight._invoke
            lost = False

            def lose_receipt(operation: str, function: Callable[[], Any]) -> Any:
                nonlocal lost
                result = original(operation, function)
                if operation == "DoPut.read_receipt" and result is not None and not lost:
                    lost = True
                    if kind == "append":
                        # Lose the acknowledgement AND the server process after durable commit.
                        # The retry must now use recovered receipts, not an in-memory response.
                        restart()
                    raise ZippyConnectionError("injected disconnect after durable server commit")
                return result

            setattr(client.flight, "_invoke", lose_receipt)
            try:
                job.write(pa.table({"symbol": ["A"], "price": [3.0]}, schema=schema))
            finally:
                setattr(client.flight, "_invoke", original)
            assert lost
            receipts = job.metrics()["last_receipts"]
            assert isinstance(receipts, list)
            assert receipts[0]["producer_epoch"] == epoch
            job.write(pa.table({"symbol": ["A"], "price": [5.0]}, schema=schema))
            assert job.metrics()["next_batch_sequence"] == 3
            assert job.metrics()["input_rows"] == job.metrics()["output_rows"] == 3
            rows = client.read_table(name, namespace="market").sort("price").collect().to_pylist()
            expected = [
                {"symbol": "A", "price": 1.0, "mean": None},
                {"symbol": "A", "price": 3.0, "mean": 2.0},
                {"symbol": "A", "price": 5.0, "mean": 4.0},
            ]
            assert rows == (expected[-1:] if kind == "key_value" else expected), (kind, rows)
            with client.open_log(
                name,
                namespace="market",
                start="earliest",
                batch_rows=1,
                deadline_ns=time.time_ns() + 5_000_000_000,
            ) as log:
                seen: list[int] = []
                try:
                    for _ in range(3):
                        seen.append(next(log).cursor.next_offset)
                except Exception as error:
                    error.add_note(
                        f"compute recovery log incomplete table=[{name}] cursors=[{seen}]"
                    )
                    raise
                assert seen == [1, 2, 3]
