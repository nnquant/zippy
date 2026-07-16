"""
Run read-only Windows-to-WSL acceptance checks against the LDC TGW stream.

The script exercises only remote Master/Gateway APIs. It never creates a stream or writes data.
It works with either a native Windows Zippy installation or the pure-Python remote fallback.

:example:

    python 07_ldc_tgw_read_acceptance.py \
        --uri zippy://127.0.0.1:17690/default \
        --stream ldc_tgw_ticks
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections.abc import Mapping
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

import zippy as zp

DEFAULT_URI = "zippy://127.0.0.1:17690/default"
DEFAULT_STREAM = "ldc_tgw_ticks"
QUERY_COLUMNS = (
    "instrument_id",
    "dt",
    "last_price",
    "bid_price_1",
    "ask_price_1",
)


def _as_python(value: object) -> object:
    as_py = getattr(value, "as_py", None)
    return as_py() if callable(as_py) else value


def _require_nonempty_table(value: object, name: str) -> pa.Table:
    if not isinstance(value, pa.Table):
        raise TypeError(f"{name} must return pyarrow.Table")
    if value.num_rows == 0:
        raise RuntimeError(f"{name} returned no rows")
    return value


def _query_checks(stream: str) -> tuple[dict[str, object], str]:
    query = zp.read_table(stream, snapshot=False).select(*QUERY_COLUMNS).tail(1000)
    profile = query.profile()
    table = _require_nonempty_table(profile["result"], "profile")
    metrics = dict(profile["metrics"])
    gateway_metrics = dict(metrics.get("gateway", {}))
    if metrics.get("executor") != "remote":
        raise RuntimeError("query did not use the remote Gateway executor")
    if gateway_metrics.get("row_range_pushdown") != "tail":
        raise RuntimeError("tail was not pushed down to Gateway")

    instrument_id = str(_as_python(table["instrument_id"][table.num_rows - 1]))
    filtered_profile = (
        zp.read_table(stream, snapshot=False)
        .filter(zp.col("instrument_id") == instrument_id)
        .select("instrument_id", "dt", "last_price")
        .tail(20)
        .profile()
    )
    filtered = _require_nonempty_table(filtered_profile["result"], "filtered profile")
    if set(filtered["instrument_id"].to_pylist()) != {instrument_id}:
        raise RuntimeError("Gateway filter returned another instrument")

    streamed = _require_nonempty_table(
        zp.read_table(stream, snapshot=False)
        .select(*QUERY_COLUMNS)
        .tail(1000)
        .collect(stream=True),
        "streaming collect",
    )

    fixed_query = (
        zp.read_table(stream, snapshot=True).select("instrument_id", "dt", "last_price").tail(1)
    )
    fixed_first = _require_nonempty_table(
        fixed_query.collect(stream=True),
        "first fixed snapshot",
    )
    time.sleep(1.2)
    fixed_second = _require_nonempty_table(
        fixed_query.collect(stream=True),
        "second fixed snapshot",
    )
    if not fixed_first.equals(fixed_second):
        raise RuntimeError("snapshot=True did not reuse a fixed query snapshot")

    fresh_query = (
        zp.read_table(stream, snapshot=False).select("instrument_id", "dt", "last_price").tail(1)
    )
    fresh_first = _require_nonempty_table(
        fresh_query.collect(stream=True),
        "first fresh snapshot",
    )
    time.sleep(1.2)
    fresh_second = _require_nonempty_table(
        fresh_query.collect(stream=True),
        "second fresh snapshot",
    )

    filtered_gateway_metrics = dict(filtered_profile["metrics"].get("gateway", {}))
    report = {
        "query_rows": table.num_rows,
        "query_instruments": len(pc.unique(table["instrument_id"])),
        "query_max_dt": _as_python(pc.max(table["dt"])),
        "query_elapsed_ms": metrics.get("elapsed_ms"),
        "gateway_elapsed_ms": metrics.get("gateway_elapsed_ms"),
        "gateway_scanned_live_rows": metrics.get("scanned_live_rows"),
        "tail_pushdown": gateway_metrics.get("row_range_pushdown"),
        "filter_pushdown_count": len(filtered_gateway_metrics.get("pushed_filters", [])),
        "filtered_instrument": instrument_id,
        "filtered_rows": filtered.num_rows,
        "stream_rows": streamed.num_rows,
        "fixed_snapshot_equal": fixed_first.equals(fixed_second),
        "fresh_snapshot_advanced": not fresh_first.equals(fresh_second),
    }
    return report, instrument_id


def _table_subscription_check(
    stream: str,
    instrument_id: str,
    timeout_sec: float,
) -> dict[str, object]:
    event = threading.Event()
    tables: list[pa.Table] = []

    def on_table(table: pa.Table) -> None:
        tables.append(table)
        event.set()

    subscriber = zp.subscribe_table(
        stream,
        callback=on_table,
        filter=zp.col("instrument_id") == instrument_id,
        batch_size=1,
        throttle_ms=500,
        count=20,
        wait=True,
        timeout=timeout_sec,
    )
    try:
        if not event.wait(timeout_sec):
            raise TimeoutError("subscribe_table did not receive a batch")
        metrics = subscriber.metrics()
    finally:
        subscriber.stop()

    table = _require_nonempty_table(tables[0], "subscribe_table callback")
    if table.num_rows > 20:
        raise RuntimeError("subscribe_table count=20 was not enforced")
    if set(table["instrument_id"].to_pylist()) != {instrument_id}:
        raise RuntimeError("subscribe_table filter returned another instrument")
    return {
        "status": "ok",
        "callback_rows": table.num_rows,
        "instrument_id": instrument_id,
        "subscriber_metrics": metrics,
    }


def _row_subscription_check(
    stream: str,
    instrument_id: str,
    timeout_sec: float,
) -> dict[str, object]:
    event = threading.Event()
    rows: list[dict[str, Any]] = []

    def on_row(row: object) -> None:
        to_dict = getattr(row, "to_dict", None)
        values = to_dict() if callable(to_dict) else row
        if not isinstance(values, Mapping):
            raise TypeError("subscribe callback row must be mapping-compatible")
        rows.append(dict(values))
        event.set()

    subscriber = zp.subscribe(
        stream,
        callback=on_row,
        instrument_ids=instrument_id,
        wait=True,
        timeout=timeout_sec,
    )
    try:
        if not event.wait(timeout_sec):
            raise TimeoutError("subscribe did not receive a row")
        metrics = subscriber.metrics()
    finally:
        subscriber.stop()

    first = rows[0]
    if first.get("instrument_id") != instrument_id:
        raise RuntimeError("subscribe instrument filter returned another instrument")
    return {
        "status": "ok",
        "instrument_id": instrument_id,
        "dt": first.get("dt"),
        "subscriber_metrics": metrics,
    }


def run_acceptance(
    *,
    uri: str,
    stream: str,
    timeout_sec: float,
    skip_live: bool,
) -> dict[str, object]:
    """
    Run read-only query and optional live subscription checks.

    :param uri: Remote Master URI.
    :type uri: str
    :param stream: LDC TGW stream name.
    :type stream: str
    :param timeout_sec: Maximum wait for each live callback.
    :type timeout_sec: float
    :param skip_live: Skip callback checks when the writer is offline.
    :type skip_live: bool
    :returns: Structured acceptance report.
    :rtype: dict[str, object]
    :raises RuntimeError: If a query, persistence, snapshot, or callback invariant fails.
    :raises ValueError: If ``timeout_sec`` is not positive.
    """
    if timeout_sec <= 0:
        raise ValueError("timeout_sec must be positive")
    master = zp.connect(uri, app="windows_ldc_acceptance", local=False)
    stream_info = master.get_stream(stream)
    query_report, instrument_id = _query_checks(stream)

    raw_persisted_files = stream_info.get("persisted_files", [])
    if not isinstance(raw_persisted_files, list):
        raise TypeError("persisted_files metadata must be a list")
    if not all(isinstance(value, Mapping) for value in raw_persisted_files):
        raise TypeError("persisted_files entries must be mappings")
    persisted_files = raw_persisted_files
    if not persisted_files:
        raise RuntimeError("stream has no persisted files")
    persisted_file_ids = [str(value.get("persist_file_id")) for value in persisted_files]
    if any(value in {"", "None"} for value in persisted_file_ids):
        raise RuntimeError("persisted file is missing persist_file_id")
    if len(persisted_file_ids) != len(set(persisted_file_ids)):
        raise RuntimeError("persist_file_id values are not unique")

    writer_epoch = stream_info.get("writer_epoch")
    active_descriptor = stream_info.get("active_segment_descriptor")
    if active_descriptor is not None and not isinstance(active_descriptor, Mapping):
        raise TypeError("active_segment_descriptor metadata must be a mapping")
    descriptor_writer_epoch = (
        active_descriptor.get("writer_epoch") if active_descriptor is not None else None
    )
    if (
        writer_epoch is not None
        and descriptor_writer_epoch is not None
        and writer_epoch != descriptor_writer_epoch
    ):
        raise RuntimeError("stream and active descriptor writer epochs differ")

    report: dict[str, object] = {
        "status": "ok",
        "uri": uri,
        "stream": stream,
        "stream_status": stream_info.get("status"),
        "writer_epoch": writer_epoch,
        "descriptor_writer_epoch": descriptor_writer_epoch,
        "writer_epoch_consistent": True,
        "persisted_files": len(persisted_files),
        "persisted_file_ids_unique": True,
        "query": query_report,
    }
    if skip_live:
        report["subscriptions"] = {"status": "skipped"}
    else:
        report["subscriptions"] = {
            "table": _table_subscription_check(stream, instrument_id, timeout_sec),
            "row": _row_subscription_check(stream, instrument_id, timeout_sec),
        }
    return report


def main() -> int:
    """
    Parse arguments, execute acceptance checks, and print JSON.

    :returns: Process exit code.
    :rtype: int
    """
    parser = argparse.ArgumentParser(description="read-only LDC TGW Windows acceptance")
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--stream", default=DEFAULT_STREAM)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="skip subscribe/subscribe_table checks when the TGW writer is offline",
    )
    args = parser.parse_args()
    report = run_acceptance(
        uri=args.uri,
        stream=args.stream,
        timeout_sec=args.timeout_sec,
        skip_live=args.skip_live,
    )
    print(json.dumps(report, default=str, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
