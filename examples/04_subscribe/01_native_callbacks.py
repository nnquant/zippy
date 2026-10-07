"""Consume native row or complete-table callbacks from an explicit log boundary.

:example: python 01_native_callbacks.py --table demo_ticks --rows 4 --mode rows
"""

import argparse
import json
import threading
import time

import pyarrow as pa
from zippy_client import Client, Row, SubscriptionOptions


def main() -> None:
    """Read a minimum number of rows, propagate failures, and close on timeout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="zippy://default")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--table", default="demo_ticks")
    parser.add_argument("--mode", choices=["rows", "tables"], default="tables")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--rows", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if args.rows <= 0 or not 0 < args.timeout <= 3600:
        parser.error("rows must be positive and timeout must be between 0 and 3600 seconds")
    done = threading.Event()
    received = 0

    def receive(value: Row | pa.Table) -> None:
        nonlocal received
        received += value.num_rows if isinstance(value, pa.Table) else 1
        if received >= args.rows:
            done.set()

    with Client(args.uri) as client:
        subscribe = client.subscribe if args.mode == "rows" else client.subscribe_table
        subscriber = subscribe(
            args.table,
            receive,
            options=SubscriptionOptions(
                namespace=args.namespace,
                start=args.start,
                batch_rows=1,
                deadline_ns=time.time_ns() + int(args.timeout * 1e9),
            ),
        )
        try:
            deadline = time.monotonic() + args.timeout
            while not done.wait(0.02):
                subscriber.join(0)
                if time.monotonic() >= deadline:
                    raise TimeoutError("not enough callback rows arrived")
        finally:
            subscriber.stop()
        print(json.dumps({"rows": received, "metrics": subscriber.metrics()}))


if __name__ == "__main__":
    main()
