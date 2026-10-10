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
