"""Derive bounded latency and spike evidence from immutable public-path benchmark raw files.

Every input has a raw report and its existing summary; mismatched metadata, configuration,
statistics or a recorded raw SHA-256 are rejected. No input is modified. Spike frequencies use
observed exposure, not requested sample count divided by the target rate. Cluster statistics
are descriptive and do not establish periodicity or causation.

:returns: A comparison JSON no larger than 300,000 bytes.
:raises ValueError: Inconsistent evidence, invalid timestamps, or oversized output.
:example: uv run --no-sync scripts/analyze_public_e2e.py --run old raw.json summary.json \
    --report-json /tmp/comparison.json
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from bench_public_e2e import check_integrity, measurement_mask, summarize_ns

NANOSECONDS_PER_SECOND = 1_000_000_000
MAX_REPORT_BYTES = 300_000
MIN_CLUSTER_INTERVALS = 5
MAX_P50_SLOPE_MICROS_PER_MINUTE = 20.0
MAX_LAST_TO_FIRST_P50_RATIO = 1.15


def _p50_trend(
    buckets: Sequence[dict[str, Any]], metric: str, measurement_complete: bool
) -> dict[str, Any]:
    full = [bucket for bucket in buckets if bucket["complete_minute"]]
    sampled = [bucket for bucket in full if bucket[metric]["count"] > 0]
    slope = ratio = first = last = None
    failures = []
    if len(sampled) >= 2:
        times = [bucket["minute_index"] + 0.5 for bucket in sampled]
        medians = [bucket[metric]["p50_micros"] for bucket in sampled]
        x_mean, y_mean = math.fsum(times) / len(times), math.fsum(medians) / len(medians)
        slope = math.fsum((x - x_mean) * (y - y_mean) for x, y in zip(times, medians)) / math.fsum(
            (x - x_mean) ** 2 for x in times
        )
        first, last = medians[0], medians[-1]
        if first > 0:
            ratio = last / first
        else:
            failures.append("first complete-minute p50 is zero; ratio is undefined")
    else:
        failures.append("fewer than 2 sampled complete minutes; trend is not estimated")
    if len(sampled) != len(full):
        failures.append("one or more complete minutes contain no samples")
    if not measurement_complete:
        failures.append("measurement is incomplete or censored; trend gate cannot pass")
    eligible = not failures
    if slope is not None and slope > MAX_P50_SLOPE_MICROS_PER_MINUTE:
        failures.append(f"p50 slope exceeds limit=[{MAX_P50_SLOPE_MICROS_PER_MINUTE}]")
    if ratio is not None and ratio > MAX_LAST_TO_FIRST_P50_RATIO:
        failures.append(f"last-to-first p50 ratio exceeds limit=[{MAX_LAST_TO_FIRST_P50_RATIO}]")
    return {
        "complete_minute_count": len(full),
        "sampled_complete_minute_count": len(sampled),
        "slope_micros_per_minute": slope,
        "first_complete_minute_p50_micros": first,
        "last_complete_minute_p50_micros": last,
        "last_to_first_p50_ratio": ratio,
        "max_slope_micros_per_minute": MAX_P50_SLOPE_MICROS_PER_MINUTE,
        "max_last_to_first_p50_ratio": MAX_LAST_TO_FIRST_P50_RATIO,
        "eligible": eligible,
        "passed": eligible and not failures,
        "failures": failures,
    }


def minute_latency_trends(
    writer_sent_ns: Sequence[int],
    ack_ns: Sequence[int],
    visible_sent_ns: Sequence[int],
    received_ns: Sequence[int],
    *,
    measurement_complete: bool,
) -> dict[str, Any]:
    """Bucket eligible latencies by actual send time and assess complete-minute p50 growth.

    :param writer_sent_ns: Actual writer send timestamps after the original warmup mask.
    :param ack_ns: Corresponding acknowledgement timestamps, including every measured write.
    :param visible_sent_ns: Writer-verified send timestamps of delivered measured samples.
    :param received_ns: Corresponding subscriber receive timestamps.
    :param measurement_complete: False for a failed, censored, or incomplete source run.
    :returns: Minute p50/p99, strict >10ms counts, and separate ack/visible trend gates.
    :raises ValueError: Array lengths, timestamp order, or measurement ranges are invalid.
    :example: minute_latency_trends([], [], [], [], measurement_complete=False)["buckets"] == []

    The common origin is the first eligible actual send. Buckets are half-open 60-second
    intervals; their coverage ends at the last eligible writer send, not at a delayed receive.
    An exact final boundary sample enters a zero-coverage tail bucket. Every partial tail is
    displayed but excluded from OLS and first/last ratios because its short exposure and fewer
    samples are not comparable to a full minute. OLS uses unweighted complete-minute p50s
    against actual minute centers and a signed slope. At least two sampled complete minutes,
    no empty complete minutes, and a complete source run are required for a passing gate.
    """
    for starts, ends in ((writer_sent_ns, ack_ns), (visible_sent_ns, received_ns)):
        if len(starts) != len(ends):
            raise ValueError("latency timestamp array lengths differ")
        if any(end < start for start, end in zip(starts, ends)):
            raise ValueError("latency endpoint precedes actual send time")
        if any(right < left for left, right in zip(starts, starts[1:])):
            raise ValueError("actual send timestamps must be nondecreasing")
    if visible_sent_ns and (
        not writer_sent_ns
        or visible_sent_ns[0] < writer_sent_ns[0]
        or visible_sent_ns[-1] > writer_sent_ns[-1]
    ):
        raise ValueError("visible sends fall outside the measured writer interval")
    origin = writer_sent_ns[0] if writer_sent_ns else None
    last_sent = writer_sent_ns[-1] if writer_sent_ns else None
    span_ns = last_sent - origin if origin is not None and last_sent is not None else 0
    minute_ns = 60 * NANOSECONDS_PER_SECOND
    bucket_count = span_ns // minute_ns + 1 if writer_sent_ns else 0
    ack_buckets: list[list[int]] = [[] for _ in range(bucket_count)]
    visible_buckets: list[list[int]] = [[] for _ in range(bucket_count)]
    if origin is not None:
        for starts, ends, grouped in (
            (writer_sent_ns, ack_ns, ack_buckets),
            (visible_sent_ns, received_ns, visible_buckets),
        ):
            for start, end in zip(starts, ends):
                grouped[(start - origin) // minute_ns].append(end - start)
    buckets = []
    for index, (ack_values, visible_values) in enumerate(zip(ack_buckets, visible_buckets)):
        coverage_ns = min(minute_ns, span_ns - index * minute_ns)
        bucket: dict[str, Any] = {
            "minute_index": index,
            "start_offset_seconds": index * 60,
            "send_coverage_seconds": coverage_ns / NANOSECONDS_PER_SECOND,
            "complete_minute": coverage_ns == minute_ns,
        }
        for name, values in (("ack_latency", ack_values), ("visible_latency", visible_values)):
            stats = summarize_ns(values)
            bucket[name] = {key: stats[key] for key in ("count", "p50_micros", "p99_micros")}
            bucket[name]["over_10ms_count"] = sum(value > 10_000_000 for value in values)
        buckets.append(bucket)
    return {
        "bucket_clock": "actual sent_ns; both latencies are assigned by the same send boundary",
        "origin_sent_ns": origin,
        "last_measured_sent_ns": last_sent,
        "send_coverage_seconds": span_ns / NANOSECONDS_PER_SECOND,
        "tail_policy": "partial/zero-coverage final minute shown but excluded from trend gates",
        "buckets": buckets,
        "p50_trends": {
            metric: _p50_trend(buckets, metric, measurement_complete)
            for metric in ("ack_latency", "visible_latency")
        },
    }


def spike_statistics(
    sent_ns: Sequence[int],
    received_ns: Sequence[int],
    threshold_ns: int,
    cluster_gap_ns: int,
) -> dict[str, Any]:
    """Count strict latency exceedances and clusters in the observed visibility window.

    :param sent_ns: Eligible actual send timestamps, in subscriber delivery order.
    :param received_ns: Corresponding monotonic receive timestamps, nondecreasing.
    :param threshold_ns: Strict latency threshold; equality is not an exceedance.
    :param cluster_gap_ns: Maximum adjacent exceedance receive gap within one cluster.
    :returns: Counts, normalized frequency, minute buckets and cluster interval statistics.
    :raises ValueError: Lengths, order, latencies, or threshold parameters are invalid.
    :example: spike_statistics([0], [3_000_000], 2_000_000, 10_000_000)["count"] == 1
    """
    if len(sent_ns) != len(received_ns):
        raise ValueError("send and receive timestamp lengths differ")
    if threshold_ns < 0 or cluster_gap_ns < 0:
        raise ValueError("spike threshold and cluster gap must be nonnegative")
    if any(end < start for start, end in zip(sent_ns, received_ns)):
        raise ValueError("received timestamp precedes its send timestamp")
    if any(right < left for left, right in zip(received_ns, received_ns[1:])):
        raise ValueError("receive timestamps must be nondecreasing")
    if any(right < left for left, right in zip(sent_ns, sent_ns[1:])):
        raise ValueError("send timestamps must be nondecreasing")
    start = sent_ns[0] if sent_ns else None
    end = received_ns[-1] if received_ns else None
    duration_ns = end - start if start is not None and end is not None else 0
    exceedances = [
        received for sent, received in zip(sent_ns, received_ns) if received - sent > threshold_ns
    ]
    clusters: list[tuple[int, int, int]] = []
    for received in exceedances:
        if not clusters or received - clusters[-1][1] > cluster_gap_ns:
            clusters.append((received, received, 1))
        else:
            first, _, count = clusters[-1]
            clusters[-1] = first, received, count + 1
    intervals = [right[0] - left[0] for left, right in zip(clusters, clusters[1:])]
    enough_intervals = len(intervals) >= MIN_CLUSTER_INTERVALS
    interval_summary = summarize_ns(intervals) if enough_intervals else None
    minute_ns = 60 * NANOSECONDS_PER_SECOND
    buckets: list[dict[str, Any]] = []
    if duration_ns > 0 and start is not None:
        counts = [0] * math.ceil(duration_ns / minute_ns)
        for received in exceedances:
            # The final observation belongs to the final exposure bucket even at an exact minute.
            index = min((received - start) // minute_ns, len(counts) - 1)
            counts[index] += 1
        for index, count in enumerate(counts):
            exposure_ns = min(minute_ns, duration_ns - index * minute_ns)
            buckets.append(
                {
                    "minute_index": index,
                    "exposure_seconds": exposure_ns / NANOSECONDS_PER_SECOND,
                    "count": count,
                    "frequency_per_minute": count * minute_ns / exposure_ns,
                }
            )
    return {
        "threshold_milliseconds": threshold_ns / 1_000_000,
        "count": len(exceedances),
        "frequency_per_minute": (
            len(exceedances) * minute_ns / duration_ns if duration_ns > 0 else None
        ),
        "exposure_start_sent_ns": start,
        "exposure_end_received_ns": end,
        "exposure_seconds": duration_ns / NANOSECONDS_PER_SECOND,
        "minute_buckets": buckets,
        "cluster_gap_milliseconds": cluster_gap_ns / 1_000_000,
        "cluster_count": len(clusters),
        "cluster_frequency_per_minute": (
            len(clusters) * minute_ns / duration_ns if duration_ns > 0 else None
        ),
        "cluster_sizes_rows": {
            "max": max((cluster[2] for cluster in clusters), default=None),
            "mean": len(exceedances) / len(clusters) if clusters else None,
        },
        "cluster_start_interval_count": len(intervals),
        "cluster_start_intervals_micros": interval_summary,
        "cluster_interval_unavailable_reason": (
            None
            if enough_intervals
            else f"fewer than {MIN_CLUSTER_INTERVALS} observed cluster-start intervals"
        ),
        "cluster_first_20_start_offsets_seconds": (
            [(first - start) / NANOSECONDS_PER_SECOND for first, _, _ in clusters[:20]]
            if start is not None
            else []
        ),
    }


def analyze_cell(
    config: dict[str, Any], cell: dict[str, Any], cluster_gap_ns: int
) -> dict[str, Any]:
    """Recompute eligible statistics and distinguish completion from latency gate failure.

    :param config: Original benchmark matrix configuration with warmup and sample requirements.
    :param cell: Original cell containing raw_samples and original summary statistics.
    :param cluster_gap_ns: Silence gap separating visible-latency spike clusters.
    :returns: Compact per-cell latency, integrity, exposure and spike analysis.
    :raises ValueError: Raw evidence disagrees with its original recorded statistics.
    :example: analyze_cell(config, raw_cell, 10_000_000)["sample_count_complete"]
    """
    raw = cell["raw_samples"]
    writer, subscriber = raw["writer"], raw["subscriber"]
    for role, fields in (
        (writer, ("seq", "sent_ns", "ack_ns")),
        (subscriber, ("seq", "sent_ns", "received_ns")),
    ):
        if len({len(role[field]) for field in fields}) != 1:
            raise ValueError("raw worker timestamp and sequence array lengths differ")
    if not check_integrity(writer["seq"], len(writer["seq"]))["passed"]:
        raise ValueError("writer sequence is incomplete or unordered")
    mask = measurement_mask(
        writer["seq"],
        writer["sent_ns"],
        writer["started_ns"],
        config["warmup_seconds"],
        config["warmup_samples"],
    )
    selected = {seq for seq, include in zip(writer["seq"], mask) if include}
    sent_by_seq = dict(zip(writer["seq"], writer["sent_ns"]))
    measured_sent: list[int] = []
    measured_received: list[int] = []
    for seq, sent, received in zip(
        subscriber["seq"], subscriber["sent_ns"], subscriber["received_ns"]
    ):
        if sent_by_seq.get(seq) != sent:
            raise ValueError("subscriber timestamp differs from writer evidence")
        if seq in selected:
            measured_sent.append(sent)
            measured_received.append(received)
    ack = summarize_ns(
        [
            end - start
            for start, end, include in zip(writer["sent_ns"], writer["ack_ns"], mask)
            if include
        ]
    )
    visible = summarize_ns([end - start for start, end in zip(measured_sent, measured_received)])
    integrity = check_integrity(subscriber["seq"], len(writer["seq"]))
    for field, recomputed in (
        ("ack_latency", ack),
        ("visible_latency", visible),
        ("integrity", integrity),
    ):
        if recomputed != cell[field]:
            raise ValueError(f"recomputed evidence differs field=[{field}]")
    sample_count_complete = ack["count"] == visible["count"] == config["samples"]
    ordered_end_marker = bool(writer.get("end_sent")) and subscriber.get("end_seq") == len(
        writer["seq"]
    )
    errors = {role: value["error"] for role, value in raw.items() if "error" in value}
    process_exit_codes = cell.get("process_exit_codes", {})
    complete = (
        sample_count_complete
        and ordered_end_marker
        and integrity["passed"]
        and not errors
        and not cell.get("interrupted", False)
        and all(code == 0 for code in process_exit_codes.values())
    )
    return {
        "durability": cell["durability"],
        "background_target_rows_per_second": cell["background_target_rows_per_second"],
        "expected_measured_samples": config["samples"],
        "warmup_rows": sum(not include for include in mask),
        "ack_latency": ack,
        "visible_latency": visible,
        "integrity": integrity,
        "sample_count_complete": sample_count_complete,
        "ordered_end_marker": ordered_end_marker,
        "measurement_complete": complete,
        "censored_or_incomplete": not complete,
        "comparison_caveat": (
            None
            if complete
            else "failed or truncated exposure; not a completed-run latency estimate"
        ),
        "original_gate_passed": cell["passed"],
        "original_failures": cell.get("failures", []),
        "process_exit_codes": process_exit_codes,
        "worker_errors": errors,
        "achieved_probe_rows_per_second": cell["achieved_probe_rows_per_second"],
        "background_achieved_rows_per_second": cell["background"].get(
            "achieved_rows_per_second", 0.0
        ),
        "minute_trends": minute_latency_trends(
            [sent for sent, include in zip(writer["sent_ns"], mask) if include],
            [ack for ack, include in zip(writer["ack_ns"], mask) if include],
            measured_sent,
            measured_received,
            measurement_complete=complete,
        ),
        "visible_spikes": {
            f"over_{milliseconds}ms": spike_statistics(
                measured_sent, measured_received, milliseconds * 1_000_000, cluster_gap_ns
            )
            for milliseconds in (2, 5, 10)
        },
    }


def analyze_reports(
    raw: dict[str, Any], summary: dict[str, Any], cluster_gap_ns: int = 10_000_000
) -> list[dict[str, Any]]:
    """Cross-check a raw/summary pair before deriving comparable cell evidence.

    :param raw: Original report with full per-worker timestamp arrays.
    :param summary: Existing compact report from the same measurement invocation.
    :param cluster_gap_ns: Maximum adjacent exceedance gap in one cluster.
    :returns: Analyzed cells retaining their original matrix ordering.
    :raises ValueError: Metadata, configuration, cells or original statistics disagree.
    :example: analyze_reports(raw, summary)[0]["visible_spikes"]["over_10ms"]["count"]
    """
    for key in ("metadata", "config", "passed", "exit_code", "finished_at_utc"):
        if raw[key] != summary[key]:
            raise ValueError(f"raw and summary differ field=[{key}]")
    if len(raw["cells"]) != len(summary["cells"]):
        raise ValueError("raw and summary cell counts differ")
    result = []
    for cell, compact in zip(raw["cells"], summary["cells"]):
        for key in (
            "durability",
            "background_target_rows_per_second",
            "ack_latency",
            "visible_latency",
            "integrity",
            "passed",
            "process_exit_codes",
        ):
            if cell[key] != compact[key]:
                raise ValueError(f"raw and summary cells differ field=[{key}]")
        result.append(analyze_cell(raw["config"], cell, cluster_gap_ns))
    return result


def _load(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    content = path.read_bytes()
    return json.loads(content), {
        "path": str(path.resolve()),
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Generate a reproducible bounded comparison without modifying source artifacts.

    :param argv: Optional CLI arguments; defaults to the process arguments.
    :returns: Zero after writing validated comparison evidence.
    :raises ValueError: Input identities, labels, cluster gap or output size are invalid.
    :example: main(["--run", "old", "raw.json", "summary.json", "--report-json", "out.json"])
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run", nargs=3, action="append", required=True, metavar=("LABEL", "RAW", "SUMMARY")
    )
    parser.add_argument(
        "--expected-sha256", nargs=2, action="append", default=[], metavar=("LABEL", "SHA256")
    )
    parser.add_argument("--cluster-gap-ms", type=float, default=10.0)
    parser.add_argument("--report-json", required=True, type=Path)
    args = parser.parse_args(argv)
    if not math.isfinite(args.cluster_gap_ms) or args.cluster_gap_ms < 0:
        raise ValueError("cluster gap must be finite and nonnegative")
    expected = dict(args.expected_sha256)
    labels = [item[0] for item in args.run]
    if len(set(labels)) != len(labels) or set(expected) - set(labels):
        raise ValueError("run labels must be unique and expected hashes must name an input")
    input_paths = {Path(path).resolve() for _, raw, summary in args.run for path in (raw, summary)}
    if args.report_json.resolve() in input_paths:
        raise ValueError("comparison output must not overwrite any input")
    runs = []
    for label, raw_path, summary_path in args.run:
        raw, identity = _load(Path(raw_path))
        summary, summary_identity = _load(Path(summary_path))
        recorded_hash = summary.get("raw_artifact", {}).get("sha256")
        for expected_hash in (expected.get(label), recorded_hash):
            if expected_hash is not None and expected_hash != identity["sha256"]:
                raise ValueError(f"raw artifact SHA-256 mismatch label=[{label}]")
        runs.append(
            {
                "label": label,
                "raw_artifact": identity,
                "summary_artifact": summary_identity,
                "raw_sha256_verified_against_prior_identity": (
                    bool(recorded_hash) or label in expected
                ),
                "metadata": raw["metadata"],
                "config": raw["config"],
                "cells": analyze_reports(raw, summary, round(args.cluster_gap_ms * 1_000_000)),
            }
        )
    result = {
        "schema_version": 1,
        "artifact_kind": "public_e2e_comparison",
        "analysis_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "rules": {
            "latency": "original dual-condition warmup; nearest-rank; p99.9 null below 10000 samples",
            "exceedances": "strict visible latency > threshold; equality excluded",
            "exposure": "first measured actual sent_ns through last measured received_ns",
            "frequency": "count / observed exposure minutes; final minute uses actual exposure",
            "minute_buckets": "left-closed/right-open, except final observed endpoint is included",
            "minute_trends": (
                "actual sent_ns buckets from first to last measured send; "
                "original received_ns spike buckets remain unchanged and can differ at boundaries"
            ),
            "trend_gate": (
                "complete-minute p50 unweighted OLS signed slope <=20us/min and last/first <=1.15; "
                "requires complete source run and at least 2 sampled complete minutes"
            ),
            "trend_tail": (
                "partial/zero-coverage minute displayed only; excluded from OLS and ratio gates"
            ),
            "cluster": "adjacent exceedance receive gaps <= configured gap form one cluster",
            "cluster_gap_milliseconds": args.cluster_gap_ms,
            "cluster_intervals": "successive cluster-start receive times; fewer than 5 yields null",
            "interpretation": "descriptive only; truncated runs are censored; no periodicity claim",
        },
        "runs": runs,
        "metadata_differences": {
            key: {run["label"]: run["metadata"].get(key) for run in runs}
            for key in sorted(set().union(*(run["metadata"].keys() for run in runs)))
            if len({json.dumps(run["metadata"].get(key), sort_keys=True) for run in runs}) > 1
        },
    }
    encoded = (json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    if len(encoded) > MAX_REPORT_BYTES:
        raise ValueError(f"comparison evidence exceeds byte limit=[{MAX_REPORT_BYTES}]")
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_bytes(encoded)
    print(f"comparison written path=[{args.report_json}] bytes=[{len(encoded)}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
