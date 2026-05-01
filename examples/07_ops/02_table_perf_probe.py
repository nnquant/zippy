"""
测量 Zippy 表的基础查询和回放性能。

这个脚本是运维和系统测试用的轻量探针，不设置硬阈值。它只记录当前环境下的观测值，
便于比较同一机器、同一数据规模、同一 master 配置下的变化。

:example:

    uv run python examples/07_ops/02_table_perf_probe.py --table ctp_ticks --tail-n 1000
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import time

import zippy as zp


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

    Percentiles use nearest-rank semantics. This keeps the probe deterministic and avoids
    pretending that a short smoke run has enough samples for interpolated statistics.

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


def summarize_numeric_samples(samples: list[float]) -> dict[str, float | int | None]:
    """
    Summarize generic numeric samples.

    :param samples: Numeric samples.
    :type samples: list[float]
    :returns: Summary with count, min, avg, p50, p95, p99, and max.
    :rtype: dict[str, float | int | None]
    """
    return summarize_samples_ms(samples)


def table_storage_summary(table_info: dict[str, object]) -> dict[str, object]:
    """
    Summarize storage-related metadata from ``zippy.ops.table_info``.

    :param table_info: Metadata returned by ``zippy.ops.table_info``.
    :type table_info: dict[str, object]
    :returns: Storage summary.
    :rtype: dict[str, object]
    """
    persisted_files = list(table_info.get("persisted_files") or [])
    sealed_segments = list(table_info.get("sealed_segments") or [])
    active_descriptor = table_info.get("active_segment_descriptor") or {}
    active_committed_rows = None
    if isinstance(active_descriptor, dict):
        active_committed_rows = active_descriptor.get("committed_rows")

    return {
        "status": table_info.get("status"),
        "descriptor_generation": table_info.get("descriptor_generation"),
        "active_committed_rows": active_committed_rows,
        "sealed_segment_count": len(sealed_segments),
        "persisted_file_count": len(persisted_files),
        "persisted_rows": sum(
            int(item.get("row_count", 0))
            for item in persisted_files
            if isinstance(item, dict)
        ),
        "reader_count": table_info.get("reader_count"),
    }


def dev_shm_usage() -> dict[str, int] | None:
    """
    Return ``/dev/shm`` disk usage when available.

    :returns: Usage in bytes, or ``None`` if ``/dev/shm`` is unavailable.
    :rtype: dict[str, int] | None
    """
    if not os.path.exists("/dev/shm"):
        return None
    usage = shutil.disk_usage("/dev/shm")
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }


def measure_tail_latency(table_name: str, tail_n: int, iterations: int) -> dict[str, object]:
    """
    Measure repeated ``read_table(...).tail(...)`` latency.

    :param table_name: Zippy table name.
    :type table_name: str
    :param tail_n: Number of rows requested from ``tail``.
    :type tail_n: int
    :param iterations: Number of repeated reads.
    :type iterations: int
    :returns: Tail latency report.
    :rtype: dict[str, object]
    :raises ValueError: If ``tail_n`` or ``iterations`` is invalid.
    """
    if tail_n < 0:
        raise ValueError("tail_n must be non-negative")
    if iterations <= 0:
        raise ValueError("iterations must be greater than zero")

    table = zp.read_table(table_name)
    samples_ms: list[float] = []
    last_rows = 0
    for _ in range(iterations):
        started_ns = time.perf_counter_ns()
        result = table.tail(tail_n)
        elapsed_ns = time.perf_counter_ns() - started_ns
        samples_ms.append(elapsed_ns / 1_000_000.0)
        last_rows = result.num_rows

    return {
        "tail_n": tail_n,
        "iterations": iterations,
        "last_rows": last_rows,
        "latency_ms": summarize_samples_ms(samples_ms),
    }


def measure_scan_live_throughput(table_name: str, iterations: int) -> dict[str, object]:
    """
    Measure ``read_table(...).scan_live()`` throughput.

    :param table_name: Zippy table name.
    :type table_name: str
    :param iterations: Number of repeated scans.
    :type iterations: int
    :returns: Scan throughput report.
    :rtype: dict[str, object]
    :raises ValueError: If ``iterations`` is invalid.
    """
    if iterations <= 0:
        raise ValueError("iterations must be greater than zero")

    table = zp.read_table(table_name)
    samples_ms: list[float] = []
    throughput_samples: list[float] = []
    last_batches = 0
    last_rows = 0

    for _ in range(iterations):
        started_ns = time.perf_counter_ns()
        rows = 0
        batches = 0
        for batch in table.scan_live():
            batches += 1
            rows += batch.num_rows
        elapsed_ns = time.perf_counter_ns() - started_ns
        elapsed_ms = elapsed_ns / 1_000_000.0
        elapsed_sec = elapsed_ns / 1_000_000_000.0
        samples_ms.append(elapsed_ms)
        throughput_samples.append(rows / elapsed_sec if elapsed_sec > 0.0 else 0.0)
        last_batches = batches
        last_rows = rows

    return {
        "iterations": iterations,
        "last_batches": last_batches,
        "last_rows": last_rows,
        "latency_ms": summarize_samples_ms(samples_ms),
        "throughput_rows_per_sec": summarize_numeric_samples(throughput_samples),
    }


def measure_replay_throughput(table_name: str) -> dict[str, object]:
    """
    Measure callback replay throughput over persisted rows.

    :param table_name: Persisted Zippy table name.
    :type table_name: str
    :returns: Replay throughput report.
    :rtype: dict[str, object]
    """
    rows = 0

    def on_row(row: zp.Row) -> None:
        nonlocal rows
        rows += 1

    started_ns = time.perf_counter_ns()
    zp.replay(table_name, callback=on_row)
    elapsed_sec = (time.perf_counter_ns() - started_ns) / 1_000_000_000.0
    throughput = rows / elapsed_sec if elapsed_sec > 0.0 else None
    return {
        "rows": rows,
        "elapsed_sec": elapsed_sec,
        "throughput_rows_per_sec": throughput,
    }


def main() -> None:
    """
    Run the table performance probe and print a JSON report.

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="测量 Zippy 表基础性能")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", required=True, help="要测量的表名")
    parser.add_argument("--tail-n", type=int, default=1000, help="tail 请求行数")
    parser.add_argument("--iterations", type=int, default=20, help="tail 重复测量次数")
    parser.add_argument("--scan-iterations", type=int, default=5, help="scan_live 重复测量次数")
    parser.add_argument("--include-replay", action="store_true", help="额外测量 persisted replay 吞吐")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_table_perf_probe")
    info = zp.ops.table_info(args.table)
    report: dict[str, object] = {
        "table": args.table,
        "observed_at_unix_ns": time.time_ns(),
        "storage": table_storage_summary(info),
        "tail": measure_tail_latency(args.table, args.tail_n, args.iterations),
        "scan_live": measure_scan_live_throughput(args.table, args.scan_iterations),
        "dev_shm": dev_shm_usage(),
    }

    if args.include_replay:
        try:
            report["replay"] = measure_replay_throughput(args.table)
        except RuntimeError as error:
            report["replay"] = {"error": str(error)}

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
