"""
测量远端 GatewayServer ``subscribe_table`` 的交付情况。

这个脚本面向 Windows/WSL 现场验收：它连接已有 master，强制走 remote gateway
数据路径，订阅指定 named stream，并把 Python 侧收到的 table/row 数与 gateway
delivery metrics 对账。

:example:

    uv run python examples/08_remote_gateway/04_remote_subscribe_perf_probe.py \\
        --uri zippy://127.0.0.1:17690/default \\
        --stream qmt_ticks \\
        --rows 100 \\
        --instrument-id IF2606
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from typing import Any

import pyarrow as pa

import zippy as zp

DELIVERY_METRIC_KEYS = (
    "subscribe_rows_delivered_total",
    "subscribe_tables_delivered_total",
    "subscribe_table_rows_delivered_total",
)


def _metric_value(metrics: dict[str, object], key: str) -> int:
    """
    Read a non-negative integer metric value.

    :param metrics: Gateway metrics dictionary.
    :type metrics: dict[str, object]
    :param key: Metric key.
    :type key: str
    :returns: Integer metric value, defaulting missing values to zero.
    :rtype: int
    """
    value = metrics.get(key, 0)
    if value is None:
        return 0
    return int(value)


def gateway_delivery_delta(
    before: dict[str, object],
    after: dict[str, object],
) -> dict[str, int]:
    """
    Compute delivery metric deltas between two gateway metric snapshots.

    :param before: Metrics captured before subscribing.
    :type before: dict[str, object]
    :param after: Metrics captured after subscribing.
    :type after: dict[str, object]
    :returns: Delta for row/table delivery counters.
    :rtype: dict[str, int]
    """
    return {
        key: _metric_value(after, key) - _metric_value(before, key) for key in DELIVERY_METRIC_KEYS
    }


def table_row_counts(tables: list[pa.Table]) -> list[int]:
    """
    Return row counts for received Arrow tables.

    :param tables: Received callback tables.
    :type tables: list[pyarrow.Table]
    :returns: Row count per table.
    :rtype: list[int]
    """
    return [int(table.num_rows) for table in tables]


def build_probe_report(
    *,
    uri: str,
    stream: str,
    expected_rows: int,
    batch_size: int,
    timeout_sec: float,
    received_tables: list[pa.Table],
    first_batch_wait_ms: float | None,
    elapsed_ms: float,
    gateway_metrics_before: dict[str, object],
    gateway_metrics_after: dict[str, object],
) -> dict[str, object]:
    """
    Build the JSON-serializable remote subscribe performance report.

    :param uri: Master URI used by the probe.
    :type uri: str
    :param stream: Subscribed stream name.
    :type stream: str
    :param expected_rows: Rows requested before the probe stops.
    :type expected_rows: int
    :param batch_size: Requested remote ``subscribe_table`` batch size.
    :type batch_size: int
    :param timeout_sec: Probe timeout in seconds.
    :type timeout_sec: float
    :param received_tables: Tables received by the callback.
    :type received_tables: list[pyarrow.Table]
    :param first_batch_wait_ms: Milliseconds until the first callback, or ``None``.
    :type first_batch_wait_ms: float | None
    :param elapsed_ms: Total elapsed milliseconds.
    :type elapsed_ms: float
    :param gateway_metrics_before: Gateway metrics before subscribing.
    :type gateway_metrics_before: dict[str, object]
    :param gateway_metrics_after: Gateway metrics after subscribing.
    :type gateway_metrics_after: dict[str, object]
    :returns: Probe report.
    :rtype: dict[str, object]
    """
    batch_rows = table_row_counts(received_tables)
    received_rows = sum(batch_rows)
    delivery_delta = gateway_delivery_delta(gateway_metrics_before, gateway_metrics_after)
    return {
        "uri": uri,
        "stream": stream,
        "expected_rows": expected_rows,
        "batch_size": batch_size,
        "timeout_sec": timeout_sec,
        "received_tables": len(received_tables),
        "received_rows": received_rows,
        "batch_rows": batch_rows,
        "first_batch_wait_ms": first_batch_wait_ms,
        "elapsed_ms": elapsed_ms,
        "gateway_delivery_delta": delivery_delta,
        "gateway_delivery_matches_received": (
            delivery_delta["subscribe_tables_delivered_total"] == len(received_tables)
            and delivery_delta["subscribe_table_rows_delivered_total"] == received_rows
        ),
        "gateway_metrics_before": dict(gateway_metrics_before),
        "gateway_metrics_after": dict(gateway_metrics_after),
    }


def remote_gateway_client_from_master(master: object) -> zp.RemoteMasterClient:
    """
    Build a remote gateway facade from the current master connection.

    :param master: ``zp.connect(..., local=False)`` result.
    :type master: object
    :returns: Remote gateway client with ``gateway_metrics`` support.
    :rtype: zippy.RemoteMasterClient
    :raises RuntimeError: If the master does not advertise gateway endpoint.
    """
    if isinstance(master, zp.RemoteMasterClient):
        return master

    config = master.get_config()
    gateway = config.get("gateway", {})
    if not isinstance(gateway, dict) or not gateway.get("endpoint"):
        raise RuntimeError("master does not advertise gateway.endpoint")
    token = str(gateway["token"]) if gateway.get("token") else None
    return zp.RemoteMasterClient(str(gateway["endpoint"]), token=token)


def run_probe(args: argparse.Namespace) -> dict[str, object]:
    """
    Run the remote subscribe performance probe.

    :param args: Command-line arguments.
    :type args: argparse.Namespace
    :returns: Probe report.
    :rtype: dict[str, object]
    :raises ValueError: If numeric arguments are invalid.
    :raises TimeoutError: If expected rows are not received before timeout.
    """
    if args.rows <= 0:
        raise ValueError("rows must be greater than zero")
    if args.batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if args.timeout_sec <= 0.0:
        raise ValueError("timeout_sec must be greater than zero")

    master = zp.connect(uri=args.uri, app="remote_subscribe_perf_probe", local=False)
    gateway_client = remote_gateway_client_from_master(master)
    metrics_before = gateway_client.gateway_metrics()

    done = threading.Event()
    lock = threading.Lock()
    received_tables: list[pa.Table] = []
    first_batch_wait_ms: float | None = None
    started_ns = time.perf_counter_ns()

    def on_table(table: pa.Table) -> None:
        """
        Record one remote GatewayServer table callback.

        :param table: Incremental table callback payload.
        :type table: pyarrow.Table
        :returns: None
        :rtype: None
        """
        nonlocal first_batch_wait_ms
        now_ns = time.perf_counter_ns()
        with lock:
            if first_batch_wait_ms is None:
                first_batch_wait_ms = (now_ns - started_ns) / 1_000_000.0
            received_tables.append(table)
            if sum(table_row_counts(received_tables)) >= args.rows:
                done.set()

    filter_expr = None
    if args.instrument_id:
        filter_expr = zp.col("instrument_id") == args.instrument_id

    subscriber = zp.subscribe_table(
        args.stream,
        callback=on_table,
        filter=filter_expr,
        batch_size=args.batch_size,
        wait=True,
        timeout=args.timeout_sec,
    )
    try:
        if not done.wait(args.timeout_sec):
            with lock:
                received_rows = sum(table_row_counts(received_tables))
            raise TimeoutError(
                "remote subscribe perf probe timed out "
                f"stream=[{args.stream}] received_rows=[{received_rows}] "
                f"expected_rows=[{args.rows}] timeout_sec=[{args.timeout_sec}]"
            )

        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
        metrics_after = gateway_client.gateway_metrics()
        with lock:
            tables = list(received_tables)
            first_wait = first_batch_wait_ms
        return build_probe_report(
            uri=args.uri,
            stream=args.stream,
            expected_rows=args.rows,
            batch_size=args.batch_size,
            timeout_sec=args.timeout_sec,
            received_tables=tables,
            first_batch_wait_ms=first_wait,
            elapsed_ms=elapsed_ms,
            gateway_metrics_before=metrics_before,
            gateway_metrics_after=metrics_after,
        )
    finally:
        subscriber.stop()


def main() -> None:
    """
    Parse CLI arguments and print a JSON report.

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="测量远端 GatewayServer subscribe_table 交付")
    parser.add_argument("--uri", required=True, help="远端 master URI")
    parser.add_argument("--stream", default="qmt_ticks", help="订阅的 named stream")
    parser.add_argument("--rows", type=int, default=100, help="收到至少 N 行后结束")
    parser.add_argument("--batch-size", type=int, default=1, help="subscribe_table batch_size")
    parser.add_argument("--timeout-sec", type=float, default=10.0, help="等待接收完成的超时时间")
    parser.add_argument("--instrument-id", default=None, help="可选 instrument_id 过滤")
    args = parser.parse_args()

    print(json.dumps(run_probe(args), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
