"""Compute from a native tick log into a new append, frame or key-value table.

Input follows the quickstart tick schema. Use a fixed through-offset for finite replay;
otherwise SIGINT/SIGTERM stops the process. Kernel state is local and is not checkpointed.

:example: python 02_native_table_compute.py --namespace-id <UUID> --through-offset 4
"""

import argparse
import json
import signal
import threading
import time
from typing import Literal
import uuid

from zippy_client import SubscriptionOptions

from zippy import AGG_LAST, AGG_SUM, CS_RANK, Client, ComputePipeline, Expr, NativeProcessor


def main() -> None:
    """Publish via identity-fenced native ingest, including buffered window tails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--namespace-id", type=uuid.UUID, required=True)
    parser.add_argument("--source", default="demo_ticks")
    parser.add_argument("--output", default="demo_computed")
    parser.add_argument(
        "--kind", choices=["latest", "timeseries", "cross_sectional"], default="latest"
    )
    parser.add_argument("--window-ns", type=int, default=60_000_000_000)
    parser.add_argument("--through-offset", type=int)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.window_ns <= 0 or not 0 < args.timeout <= 3600:
        parser.error("window-ns must be positive and timeout must be between 0 and 3600 seconds")
    if args.through_offset is not None and args.through_offset <= 0:
        parser.error("through-offset must be positive")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    output_id = uuid.uuid4()
    with Client(args.uri) as client:
        schema = client.flight.get_table_schema(args.source, namespace=args.namespace)
        output_kind: Literal["append", "frame", "key_value"]
        if args.kind == "latest":
            kernel = NativeProcessor.latest("latest", schema, ["instrument_id"])
            output_kind = "key_value"
        elif args.kind == "timeseries":
            kernel = NativeProcessor.timeseries(
                "bars",
                schema,
                "instrument_id",
                "dt",
                args.window_ns,
                [
                    AGG_LAST(column="last_price", output="close"),
                    AGG_SUM(column="volume_float", output="volume"),
                ],
                pre_factors=[Expr(expression="CAST(volume, 'float64')", output="volume_float")],
            )
            output_kind = "append"
        else:
            kernel = NativeProcessor.cross_sectional(
                "ranks",
                schema,
                "instrument_id",
                "dt",
                args.window_ns,
                [CS_RANK(column="last_price", output="rank")],
            )
            output_kind = "frame"
        try:
            if output_kind == "key_value":
                client.create_key_value_table(
                    args.output,
                    kernel.output_schema(),
                    keys=["instrument_id"],
                    namespace_id=args.namespace_id,
                    table_id=output_id,
                    request_id=uuid.uuid4(),
                )
            elif output_kind == "frame":
                client.create_frame_table(
                    args.output,
                    kernel.output_schema(),
                    by="dt",
                    namespace_id=args.namespace_id,
                    table_id=output_id,
                    request_id=uuid.uuid4(),
                )
            else:
                client.create_append_table(
                    args.output,
                    kernel.output_schema(),
                    namespace_id=args.namespace_id,
                    table_id=output_id,
                    request_id=uuid.uuid4(),
                )
            with ComputePipeline(
                client,
                kernel,
                source=args.source,
                input_options=SubscriptionOptions(namespace=args.namespace, start="earliest"),
                output=args.output,
                output_table_id=output_id,
                output_namespace=args.namespace,
                output_kind=output_kind,
                durability="persisted",
            ) as job:
                deadline = time.monotonic() + args.timeout
                while not stop.wait(0.01):
                    job.join(0)
                    if args.through_offset is not None:
                        if job.cursor is not None and job.cursor.next_offset >= args.through_offset:
                            break
                        if time.monotonic() >= deadline:
                            raise TimeoutError("source did not reach the requested offset")
            with client.read_table(args.output, namespace=args.namespace) as table:
                result = table.collect()
            print(
                json.dumps(
                    {
                        "output_id": str(output_id),
                        "rows": result.num_rows,
                        "kind": output_kind,
                        "metrics": job.metrics(),
                    }
                )
            )
        finally:
            kernel.close()


if __name__ == "__main__":
    main()
