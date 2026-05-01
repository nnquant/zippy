"""
测量 Zippy subscriber 的行级端到端延迟。

这个脚本用于 M6.5 low-latency IPC 验收。它会创建一张临时 StreamTable，先启动
``zp.subscribe(...)``，再按固定间隔写入带 ``localtime_ns`` 的 tick，并统计 Python
回调进入时相对写入时间戳的延迟。

:example:

    uv run python examples/07_ops/03_subscribe_latency_probe.py \
        --drop-existing --rows 256 --interval-ms 1 --row-capacity 32
"""

from __future__ import annotations

import argparse
import json
import math
import threading
import time
from datetime import datetime, timezone
from typing import Any

import polars as pl
import pyarrow as pa

import zippy as zp


def tick_schema() -> pa.Schema:
    """
    返回探针使用的 tick schema。

    ``localtime_ns`` 是延迟锚点，必须尽量贴近写入前的本地时间。

    :returns: Arrow schema
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("seq", pa.int64()),
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("localtime_ns", pa.int64()),
            ("last_price", pa.float64()),
        ]
    )


def nearest_rank_percentile(sorted_samples: list[float], percentile: float) -> float | None:
    """
    Compute a nearest-rank percentile from sorted samples.

    :param sorted_samples: Samples sorted in ascending order.
    :type sorted_samples: list[float]
    :param percentile: Percentile in the inclusive ``[0, 100]`` range.
    :type percentile: float
    :returns: Percentile value, or ``None`` when samples are empty.
    :rtype: float | None
    :raises ValueError: If ``percentile`` is outside ``[0, 100]``.
    """
    if percentile < 0.0 or percentile > 100.0:
        raise ValueError("percentile must be between 0 and 100")
    if not sorted_samples:
        return None
    rank = math.ceil((percentile / 100.0) * len(sorted_samples))
    index = min(max(rank - 1, 0), len(sorted_samples) - 1)
    return sorted_samples[index]


def summarize_samples_ms(samples_ms: list[float]) -> dict[str, float | int | None]:
    """
    Summarize latency samples in milliseconds.

    :param samples_ms: Latency samples in milliseconds.
    :type samples_ms: list[float]
    :returns: Summary with count, min, avg, p50, p95, p99, and max.
    :rtype: dict[str, float | int | None]
    """
    if not samples_ms:
        return {
            "count": 0,
            "min": None,
            "avg": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        }

    samples = sorted(float(sample) for sample in samples_ms)
    return {
        "count": len(samples),
        "min": samples[0],
        "avg": sum(samples) / len(samples),
        "p50": nearest_rank_percentile(samples, 50.0),
        "p95": nearest_rank_percentile(samples, 95.0),
        "p99": nearest_rank_percentile(samples, 99.0),
        "max": samples[-1],
    }


def slowest_latency_rows(samples: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """
    返回延迟最高的若干行样本。

    :param samples: 行级延迟样本，每项至少包含 ``latency_ms``。
    :type samples: list[dict[str, typing.Any]]
    :param limit: 返回行数上限；小于等于 0 时返回空列表。
    :type limit: int
    :returns: 按 ``latency_ms`` 从高到低排序的样本副本。
    :rtype: list[dict[str, typing.Any]]
    """
    if limit <= 0:
        return []
    return [
        dict(sample)
        for sample in sorted(
            samples,
            key=lambda sample: float(sample["latency_ms"]),
            reverse=True,
        )[:limit]
    ]


def make_tick(seq: int, localtime_ns: int) -> pl.DataFrame:
    """
    构造单行 tick。

    :param seq: 行序号。
    :type seq: int
    :param localtime_ns: 写入前本地时间戳，单位纳秒。
    :type localtime_ns: int
    :returns: 单行 Polars DataFrame。
    :rtype: polars.DataFrame
    """
    return pl.DataFrame(
        {
            "seq": [seq],
            "instrument_id": ["IF2606"],
            "dt": [datetime.fromtimestamp(localtime_ns / 1_000_000_000, tz=timezone.utc)],
            "localtime_ns": [localtime_ns],
            "last_price": [4100.0 + (seq * 0.2)],
        }
    )


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    """
    运行 subscriber 延迟探针。

    :param args: 命令行参数。
    :type args: argparse.Namespace
    :returns: JSON 可序列化报告。
    :rtype: dict[str, typing.Any]
    :raises TimeoutError: 如果超时前没有收到所有写入行。
    """
    zp.connect(uri=args.uri, app="example_subscribe_latency_probe")

    if args.drop_existing:
        try:
            zp.ops.drop_table(args.table, drop_persisted=True)
        except RuntimeError:
            pass

    pipeline = zp.Pipeline("example_subscribe_latency_probe")
    pipeline.stream_table(
        args.table,
        schema=tick_schema(),
        dt_column="dt",
        id_column="instrument_id",
        dt_part="%Y%m",
        persist=None,
        row_capacity=args.row_capacity,
    )

    done = threading.Event()
    lock = threading.Lock()
    latencies_ms: list[float] = []
    rollover_first_latencies_ms: list[float] = []
    latency_samples: list[dict[str, Any]] = []
    append_latencies_ms: list[float] = []
    rollover_append_latencies_ms: list[float] = []
    received_rows = 0

    def on_row(row: zp.Row) -> None:
        """
        记录一条 subscriber 回调延迟。

        :param row: Zippy 行对象。
        :type row: zippy.Row
        :returns: None
        :rtype: None
        """
        nonlocal received_rows
        callback_started_ns = time.time_ns()
        values = row.to_dict()
        seq = int(values["seq"])
        localtime_ns = int(values["localtime_ns"])
        latency_ms = (callback_started_ns - localtime_ns) / 1_000_000.0
        rollover_first_row = seq > 0 and seq % args.row_capacity == 0
        measured = seq >= args.discard_first_rows

        with lock:
            if measured:
                latencies_ms.append(latency_ms)
                latency_samples.append(
                    {
                        "seq": seq,
                        "latency_ms": latency_ms,
                        "rollover_first_row": rollover_first_row,
                    }
                )
                if rollover_first_row:
                    rollover_first_latencies_ms.append(latency_ms)
            received_rows += 1
            if received_rows >= args.rows:
                done.set()

    pipeline.start()
    subscriber = zp.subscribe(
        args.table,
        callback=on_row,
        poll_interval_ms=args.poll_interval_ms,
        xfast=args.xfast,
        idle_spin_checks=args.idle_spin_checks,
    )
    try:
        if args.warmup_ms > 0.0:
            time.sleep(args.warmup_ms / 1000.0)

        for seq in range(args.rows):
            tick = make_tick(seq, time.time_ns())
            append_started_ns = time.perf_counter_ns()
            pipeline.write(tick)
            append_latency_ms = (time.perf_counter_ns() - append_started_ns) / 1_000_000.0
            append_latencies_ms.append(append_latency_ms)
            if seq > 0 and seq % args.row_capacity == 0:
                rollover_append_latencies_ms.append(append_latency_ms)
            if args.interval_ms > 0.0:
                time.sleep(args.interval_ms / 1000.0)

        if not done.wait(args.timeout_sec):
            raise TimeoutError(
                "subscriber latency probe timed out "
                f"received_rows=[{received_rows}] expected_rows=[{args.rows}]"
            )

        snapshot = zp.read_table(args.table).snapshot()
        with lock:
            all_latencies = list(latencies_ms)
            rollover_latencies = list(rollover_first_latencies_ms)
            row_samples = [dict(sample) for sample in latency_samples]

        return {
            "table": args.table,
            "rows": args.rows,
            "interval_ms": args.interval_ms,
            "row_capacity": args.row_capacity,
            "xfast": args.xfast,
            "poll_interval_ms": args.poll_interval_ms,
            "idle_spin_checks": args.idle_spin_checks,
            "warmup_ms": args.warmup_ms,
            "discard_first_rows": args.discard_first_rows,
            "measured_rows": len(all_latencies),
            "append_latency_ms": summarize_samples_ms(append_latencies_ms),
            "rollover_append_latency_ms": summarize_samples_ms(rollover_append_latencies_ms),
            "latency_ms": summarize_samples_ms(all_latencies),
            "slowest_rows": slowest_latency_rows(row_samples, args.slowest_rows),
            "rollover_first_row_latency_ms": summarize_samples_ms(rollover_latencies),
            "subscriber_metrics": subscriber.metrics(),
            "active_segment_control": snapshot.get("active_segment_control"),
        }
    finally:
        subscriber.stop()
        pipeline.stop()


def main() -> None:
    """
    解析命令行参数并输出 JSON 报告。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="测量 Zippy subscriber 行级延迟")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", default="subscribe_latency_probe_ticks", help="探针表名")
    parser.add_argument("--rows", type=int, default=256, help="写入和接收的行数")
    parser.add_argument("--interval-ms", type=float, default=1.0, help="每行写入间隔")
    parser.add_argument("--row-capacity", type=int, default=32, help="active segment 行容量")
    parser.add_argument(
        "--poll-interval-ms", type=int, default=1, help="subscriber health check 间隔"
    )
    parser.add_argument(
        "--idle-spin-checks",
        type=int,
        default=64,
        help="非 xfast 模式进入 futex wait 前的短自旋检查次数；0 表示纯 futex wait",
    )
    parser.add_argument(
        "--warmup-ms",
        type=float,
        default=20.0,
        help="subscriber 启动后、正式写入前的预热等待时间，用于隔离启动噪声",
    )
    parser.add_argument(
        "--discard-first-rows",
        type=int,
        default=0,
        help="从延迟统计中丢弃前 N 条行样本，用于排除首条写入/attach 噪声",
    )
    parser.add_argument(
        "--slowest-rows",
        type=int,
        default=5,
        help="报告中保留的最慢行样本数量，便于定位长尾 seq",
    )
    parser.add_argument("--timeout-sec", type=float, default=10.0, help="等待接收完成的超时时间")
    parser.add_argument("--xfast", action="store_true", help="使用 spin loop 模式")
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="运行前删除同名表，便于重复执行示例",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("rows must be greater than zero")
    if args.row_capacity <= 0:
        raise ValueError("row_capacity must be greater than zero")
    if args.idle_spin_checks < 0:
        raise ValueError("idle_spin_checks must be greater than or equal to zero")
    if args.warmup_ms < 0.0:
        raise ValueError("warmup_ms must be greater than or equal to zero")
    if args.discard_first_rows < 0:
        raise ValueError("discard_first_rows must be greater than or equal to zero")
    if args.discard_first_rows >= args.rows:
        raise ValueError("discard_first_rows must be less than rows")
    if args.slowest_rows < 0:
        raise ValueError("slowest_rows must be greater than or equal to zero")
    if args.poll_interval_ms <= 0 and not args.xfast:
        raise ValueError("poll_interval_ms must be greater than zero unless xfast is true")

    print(json.dumps(run_probe(args), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
