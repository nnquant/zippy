"""Inspect native fixed query results and measure this client's callback deliveries.

This read-only example requires only zippy-client, without the server Python package.
It reports subscription wait/throughput, not market event or end-to-end ingest latency.

:example: python 02_read_and_subscribe.py --uri tcp://localhost:17690 --table demo_ticks \
    --order-by dt --start earliest --rows 10
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
import math
import threading
import time
from typing import Literal

import pyarrow as pa
from zippy_client import Client, Row, SubscriptionOptions


def measure_delivery(
    client: Client,
    *,
    table: str,
    namespace: str,
    mode: Literal["rows", "tables"],
    rows: int,
    timeout_sec: float,
    start: str = "earliest",
    batch_rows: int = 1024,
) -> dict[str, object]:
    """Observe a minimum row count without splitting atomic table callbacks.

    :param client: Connected native client owned by the caller.
    :param table: Existing table name.
    :param namespace: Exact namespace.
    :param mode: Row callbacks for append, or atomic table callbacks for all three kinds.
    :param rows: Positive minimum delivered rows; the last atomic unit may exceed it.
    :param timeout_sec: Positive finite total wait, including subscription open.
    :param start: Explicit earliest or latest native log boundary.
    :param batch_rows: Wire fragment target; frame/KV callbacks remain atomic.
    :returns: Local observations and this subscriber's own final counters/cursor.
    :raises TimeoutError: Not enough rows arrived within the wait budget.
    :raises ValueError: Options are invalid.
    :raises RuntimeError: A callback fails or observations disagree with local metrics.
    """
    if type(rows) is not int or rows <= 0 or mode not in {"rows", "tables"}:
        raise ValueError("rows must be positive and mode must be rows or tables")
    if not math.isfinite(timeout_sec) or timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive and finite")
    if start not in {"earliest", "latest"}:
        raise ValueError("start must be earliest or latest")
    done = threading.Event()
    received_rows = 0
    callbacks = 0
    first_wait_ms: float | None = None
    sample_batch_rows: list[int] = []
    started = time.monotonic()

    def receive(value: Row | pa.Table) -> None:
        nonlocal received_rows, callbacks, first_wait_ms
        count = value.num_rows if isinstance(value, pa.Table) else 1
        if first_wait_ms is None:
            first_wait_ms = (time.monotonic() - started) * 1000
        received_rows += count
        callbacks += 1
        if len(sample_batch_rows) < 32:
            sample_batch_rows.append(count)
        if received_rows >= rows:
            done.set()

    options = SubscriptionOptions(
        namespace=namespace,
        start=start,
        batch_rows=batch_rows,
        open_timeout_sec=timeout_sec,
        deadline_ns=time.time_ns() + int(timeout_sec * 1e9),
    )
    subscribe = client.subscribe if mode == "rows" else client.subscribe_table
    subscriber = subscribe(table, receive, options=options)
    try:
        while not done.is_set():
            subscriber.join(0)
            remaining = timeout_sec - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError(
                    f"subscription observation timed out received_rows=[{received_rows}]"
                )
            done.wait(min(0.02, remaining))
    finally:
        subscriber.stop()
    elapsed_ms = (time.monotonic() - started) * 1000
    metrics = subscriber.metrics()
    if metrics["rows_delivered_total"] != received_rows:
        raise RuntimeError("local subscriber counters disagree with observed callbacks")
    if mode == "tables" and metrics["batches_delivered_total"] != callbacks:
        raise RuntimeError("local subscriber batch counter disagrees with callbacks")
    return {
        "namespace": namespace,
        "table": table,
        "mode": mode,
        "requested_minimum_rows": rows,
        "received_rows": received_rows,
        "callbacks": callbacks,
        "first_callback_wait_ms": first_wait_ms,
        "elapsed_ms": elapsed_ms,
        "observed_rows_per_second": received_rows * 1000 / elapsed_ms if elapsed_ms > 0 else None,
        "sample_batch_rows": sample_batch_rows,
        "subscriber": metrics,
    }


def main() -> None:
    """Print fixed query identity and callback observations without writing data.

    :raises RuntimeError: Query identity or repeated fixed results are inconsistent.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--table", required=True)
    parser.add_argument("--mode", choices=["rows", "tables"], default="tables")
    parser.add_argument("--start", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("--batch-rows", type=int, default=1024)
    parser.add_argument("--timeout-sec", type=float, default=10)
    parser.add_argument("--order-by", action="append", required=True)
    args = parser.parse_args()
    with ExitStack() as resources:
        client = resources.enter_context(Client(args.uri))
        source = resources.enter_context(
            client.read_table(args.table, namespace=args.namespace, snapshot=True)
        )
        ordered = resources.enter_context(source.sort(args.order_by))
        query = resources.enter_context(ordered.tail(5))
        profile = query.profile()
        result = profile["result"]
        if not isinstance(result, pa.Table) or not result.equals(
            query.collect(), check_metadata=True
        ):
            raise RuntimeError("fixed query snapshot changed")
        metrics = profile["metrics"]
        if not isinstance(metrics, dict) or not isinstance(metrics.get("query"), dict):
            raise RuntimeError("native query metadata is missing")
        snapshots = metrics["query"].get("snapshots")
        if not snapshots:
            raise RuntimeError("native query identity is missing")
        delivery = measure_delivery(
            client,
            table=args.table,
            namespace=args.namespace,
            mode=args.mode,
            rows=args.rows,
            timeout_sec=args.timeout_sec,
            start=args.start,
            batch_rows=args.batch_rows,
        )
    print(
        json.dumps(
            {
                "query_rows": result.num_rows,
                "columns": result.column_names,
                "snapshots": snapshots,
                "delivery": delivery,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
