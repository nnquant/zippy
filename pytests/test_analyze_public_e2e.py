"""Validate comparison evidence math with deterministic in-memory samples only."""

from __future__ import annotations

import copy
from pathlib import Path
import runpy
from typing import Any

import pytest


@pytest.fixture
def analysis(monkeypatch) -> dict[str, Any]:
    """Load the standalone analyzer without executing its CLI.

    :param monkeypatch: Pytest path isolation helper.
    :returns: Analyzer pure functions.
    """
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    return runpy.run_path(str(scripts / "analyze_public_e2e.py"))


def test_spike_threshold_is_strict_and_frequency_uses_observed_exposure(analysis):
    """Equality is excluded and real measurement duration determines frequency."""
    stats = analysis["spike_statistics"](
        [0, 10_000_000, 20_000_000], [2_000_000, 15_000_000, 31_000_000], 2_000_000, 10_000_000
    )
    assert stats["count"] == 2
    assert stats["exposure_seconds"] == 0.031
    assert stats["frequency_per_minute"] == pytest.approx(2 * 60 / 0.031)
    assert stats["cluster_count"] == 2
    assert stats["cluster_start_intervals_micros"] is None
    assert stats["cluster_start_interval_count"] == 1


def test_cluster_gap_boundary_is_inclusive(analysis):
    """An exact gap joins the cluster; a larger gap starts a new cluster."""
    stats = analysis["spike_statistics"]([0, 10, 21], [1, 11, 22], 0, 10)
    assert stats["cluster_count"] == 2
    assert stats["cluster_sizes_rows"] == {"max": 2, "mean": 1.5}


def test_cluster_intervals_need_five_observed_intervals(analysis):
    """Five intervals allow descriptive estimates without any periodicity inference."""
    sent = [index * 20_000_000 for index in range(6)]
    stats = analysis["spike_statistics"](sent, [stamp + 1_000_000 for stamp in sent], 0, 10_000_000)
    assert stats["cluster_start_interval_count"] == 5
    assert stats["cluster_start_intervals_micros"]["p50_micros"] == 20_000
    assert stats["cluster_interval_unavailable_reason"] is None


def test_minute_buckets_normalize_partial_final_exposure(analysis):
    """The final partial minute is not treated as sixty seconds of observation."""
    stats = analysis["spike_statistics"](
        [0, 61_000_000_000], [3_000_000, 61_003_000_000], 2_000_000, 10_000_000
    )
    assert stats["minute_buckets"][0] == {
        "minute_index": 0,
        "exposure_seconds": 60,
        "count": 1,
        "frequency_per_minute": 1,
    }
    assert stats["minute_buckets"][1]["exposure_seconds"] == 1.003
    assert stats["minute_buckets"][1]["frequency_per_minute"] == pytest.approx(60 / 1.003)


def test_final_endpoint_exactly_on_minute_is_included_once(analysis):
    """An exact final endpoint contributes to the last observed bucket."""
    stats = analysis["spike_statistics"]([0], [60_000_000_000], 0, 10_000_000)
    assert stats["count"] == 1
    assert len(stats["minute_buckets"]) == 1
    assert stats["minute_buckets"][0]["count"] == 1


def test_empty_exposure_has_unavailable_rate_and_intervals(analysis):
    """No samples imply neither a fabricated frequency nor a period estimate."""
    stats = analysis["spike_statistics"]([], [], 2_000_000, 10_000_000)
    assert stats["frequency_per_minute"] is None
    assert stats["cluster_start_intervals_micros"] is None
    assert stats["minute_buckets"] == []


@pytest.mark.parametrize(
    "sent,received,threshold,gap",
    [
        ([0], [], 0, 1),
        ([2], [1], 0, 1),
        ([0, 1], [3, 2], 0, 1),
        ([1, 0], [2, 3], 0, 1),
        ([0], [1], -1, 1),
        ([0], [1], 0, -1),
    ],
)
def test_invalid_evidence_is_rejected(analysis, sent, received, threshold, gap):
    """Malformed timestamps and invalid analysis parameters never produce success evidence."""
    with pytest.raises(ValueError):
        analysis["spike_statistics"](sent, received, threshold, gap)


def _report(analysis) -> dict[str, Any]:
    writer = {
        "seq": [0, 1, 2],
        "sent_ns": [0, 1_000_000_000, 2_000_000_000],
        "ack_ns": [50_000_000, 1_001_000_000, 2_001_000_000],
        "started_ns": 0,
        "end_sent": True,
    }
    subscriber = {
        "seq": [0, 1, 2],
        "sent_ns": writer["sent_ns"],
        "received_ns": [80_000_000, 1_003_000_000, 2_003_000_000],
        "end_seq": 3,
    }
    return {
        "metadata": {"git_sha": "synthetic", "git_dirty": False},
        "config": {"samples": 2, "warmup_seconds": 1, "warmup_samples": 1},
        "passed": False,
        "exit_code": 1,
        "finished_at_utc": "2000-01-01T00:00:00+00:00",
        "cells": [
            {
                "durability": "memory",
                "background_target_rows_per_second": 0,
                "ack_latency": analysis["summarize_ns"]([1_000_000, 1_000_000]),
                "visible_latency": analysis["summarize_ns"]([3_000_000, 3_000_000]),
                "integrity": analysis["check_integrity"]([0, 1, 2], 3),
                "passed": False,
                "failures": ["synthetic threshold failure"],
                "raw_samples": {"writer": writer, "subscriber": subscriber},
                "process_exit_codes": {"server": 0, "writer": 0, "subscriber": 0},
                "achieved_probe_rows_per_second": 1,
                "background": {"achieved_rows_per_second": 0},
            }
        ],
    }


def test_warmup_is_excluded_and_gate_failure_is_distinct_from_incompletion(analysis):
    """Exclude the large warmup sample and retain completion despite threshold failure."""
    report = _report(analysis)
    before = copy.deepcopy(report)
    result = analysis["analyze_reports"](report, copy.deepcopy(report))[0]
    assert result["ack_latency"]["p50_micros"] == 1000
    assert result["visible_latency"]["max_micros"] == 3000
    assert result["visible_latency"]["p99_9_micros"] is None
    assert result["measurement_complete"] is True
    assert result["original_gate_passed"] is False
    assert report == before


def test_truncated_run_remains_censored_even_when_received_sequence_is_complete(analysis):
    """Receipt equality alone cannot turn a short failed run into completed evidence."""
    report = _report(analysis)
    report["config"]["samples"] = 20000
    report["cells"][0]["raw_samples"]["writer"]["end_sent"] = False
    result = analysis["analyze_reports"](report, copy.deepcopy(report))[0]
    assert result["integrity"]["passed"] is True
    assert result["sample_count_complete"] is False
    assert result["censored_or_incomplete"] is True


@pytest.mark.parametrize("key", ["metadata", "config", "visible_latency"])
def test_raw_and_summary_disagreement_is_rejected(analysis, key):
    """Reject mismatched run identity, sampling configuration and summary statistics."""
    report = _report(analysis)
    summary = copy.deepcopy(report)
    if key == "visible_latency":
        summary["cells"][0][key]["count"] = 99
    else:
        summary[key]["mismatch"] = True
    with pytest.raises(ValueError, match="differ"):
        analysis["analyze_reports"](report, summary)


def test_recomputed_raw_statistics_must_match_original(analysis):
    """Even mutually matching summaries cannot hide altered raw timestamp evidence."""
    report = _report(analysis)
    report["cells"][0]["raw_samples"]["writer"]["ack_ns"][-1] += 1
    with pytest.raises(ValueError, match="recomputed evidence differs"):
        analysis["analyze_reports"](report, copy.deepcopy(report))
