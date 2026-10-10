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


def _trend_from_medians(analysis, medians_micros, tail_micros=999_999):
    minute_ns = 60_000_000_000
    sent = [index * minute_ns for index in range(len(medians_micros) + 1)]
    ends = [start + value * 1000 for start, value in zip(sent, [*medians_micros, tail_micros])]
    return analysis["minute_latency_trends"](sent, ends, sent, ends, measurement_complete=True)


@pytest.mark.parametrize("medians", [[1000, 1020, 1040], [100, 115]])
def test_trend_gates_include_exact_slope_and_ratio_boundaries(analysis, medians):
    """The exact <=20us/min and <=1.15 limits remain passing boundaries."""
    result = _trend_from_medians(analysis, medians)
    for stats in result["p50_trends"].values():
        assert stats["eligible"] is True
        assert stats["passed"] is True
        assert stats["slope_micros_per_minute"] <= 20
        assert stats["last_to_first_p50_ratio"] <= 1.15


@pytest.mark.parametrize(
    "medians,slope,ratio,failed_text",
    [([1000, 1021], 21, 1.021, "slope"), ([100, 120], 20, 1.2, "ratio")],
)
def test_slope_and_first_last_ratio_are_independent_gates(
    analysis, medians, slope, ratio, failed_text
):
    """One acceptable trend metric cannot conceal failure of the other metric."""
    stats = _trend_from_medians(analysis, medians)["p50_trends"]["visible_latency"]
    assert stats["eligible"] is True
    assert stats["passed"] is False
    assert stats["slope_micros_per_minute"] == slope
    assert stats["last_to_first_p50_ratio"] == ratio
    assert len(stats["failures"]) == 1
    assert failed_text in stats["failures"][0]


def test_ols_uses_all_complete_minutes_instead_of_endpoint_slope(analysis):
    """A non-linear sequence distinguishes OLS from simply comparing endpoints."""
    stats = _trend_from_medians(analysis, [1000, 1400, 1100, 1200])["p50_trends"]["visible_latency"]
    assert stats["slope_micros_per_minute"] == 30
    assert stats["last_to_first_p50_ratio"] == 1.2


def test_extreme_partial_tail_is_shown_but_does_not_change_gate(analysis):
    """An unstable short tail must not overwhelm the two comparable full minutes."""
    sent = [0, 60_000_000_000, 120_000_000_000, 120_020_000_000]
    ends = [sent[0] + 1_000_000, sent[1] + 1_000_000, sent[2] + 50_000_000, sent[3] + 50_000_000]
    result = analysis["minute_latency_trends"](sent, ends, sent, ends, measurement_complete=True)
    tail = result["buckets"][-1]
    assert tail["send_coverage_seconds"] == 0.02
    assert tail["complete_minute"] is False
    assert tail["visible_latency"]["over_10ms_count"] == 2
    assert tail["visible_latency"]["p50_micros"] == 50_000
    stats = result["p50_trends"]["visible_latency"]
    assert stats["complete_minute_count"] == 2
    assert stats["slope_micros_per_minute"] == 0
    assert stats["last_to_first_p50_ratio"] == 1
    assert stats["passed"] is True


def test_exact_final_send_boundary_has_zero_coverage_tail(analysis):
    """A sample at minute two starts a tail and cannot create a third full minute."""
    result = _trend_from_medians(analysis, [1000, 1000])
    assert len(result["buckets"]) == 3
    assert result["buckets"][-1]["send_coverage_seconds"] == 0
    assert result["buckets"][-1]["complete_minute"] is False
    assert result["p50_trends"]["visible_latency"]["complete_minute_count"] == 2


def test_send_buckets_do_not_move_slow_samples_to_receive_minute(analysis):
    """Cross-boundary responses retain their send cohort while old receipt buckets stay valid."""
    sent = [0, 59_999_000_000, 60_000_000_000, 120_000_000_000]
    received = [1000, 60_019_000_000, 60_030_000_000, 120_000_001_000]
    result = analysis["minute_latency_trends"](
        sent, [stamp + 1000 for stamp in sent], sent, received, measurement_complete=True
    )
    old = analysis["spike_statistics"](sent, received, 10_000_000, 10_000_000)
    assert result["buckets"][0]["visible_latency"]["over_10ms_count"] == 1
    assert result["buckets"][1]["visible_latency"]["over_10ms_count"] == 1
    assert old["minute_buckets"][0]["count"] == 0
    assert old["minute_buckets"][1]["count"] == 2
    assert sum(b["visible_latency"]["over_10ms_count"] for b in result["buckets"]) == old["count"]


def test_empty_full_minute_is_not_compressed_out_of_ols_time_axis(analysis):
    """A missing minute retains its elapsed time and prevents a passing assessment."""
    sent = [0, 120_000_000_000, 180_000_000_000]
    ends = [sent[0] + 1_000_000, sent[1] + 1_040_000, sent[2] + 1_000_000]
    stats = analysis["minute_latency_trends"](sent, ends, sent, ends, measurement_complete=True)[
        "p50_trends"
    ]["visible_latency"]
    assert stats["slope_micros_per_minute"] == 20
    assert stats["complete_minute_count"] == 3
    assert stats["sampled_complete_minute_count"] == 2
    assert stats["eligible"] is False
    assert stats["passed"] is False
    assert any("no samples" in failure for failure in stats["failures"])


@pytest.mark.parametrize("sent", [[], [0], [0, 60_000_000_000, 119_999_999_999]])
def test_fewer_than_two_complete_minutes_cannot_estimate_or_pass(analysis, sent):
    """No sample-count substitution or delayed receives can fabricate full-minute coverage."""
    ends = [stamp + 500_000_000_000 for stamp in sent]
    stats = analysis["minute_latency_trends"](sent, ends, sent, ends, measurement_complete=True)[
        "p50_trends"
    ]["visible_latency"]
    assert stats["slope_micros_per_minute"] is None
    assert stats["last_to_first_p50_ratio"] is None
    assert stats["eligible"] is False
    assert stats["passed"] is False


def test_incomplete_run_cannot_pass_even_with_flat_complete_minutes(analysis):
    """A failed run can show descriptive slopes without becoming successful evidence."""
    sent = [0, 60_000_000_000, 120_000_000_000]
    ends = [stamp + 1_000_000 for stamp in sent]
    stats = analysis["minute_latency_trends"](sent, ends, sent, ends, measurement_complete=False)[
        "p50_trends"
    ]["visible_latency"]
    assert stats["slope_micros_per_minute"] == 0
    assert stats["eligible"] is False
    assert stats["passed"] is False


def test_zero_first_p50_keeps_ratio_undefined(analysis):
    """A zero denominator must not generate infinity or an invented passing ratio."""
    stats = _trend_from_medians(analysis, [0, 0])["p50_trends"]["visible_latency"]
    assert stats["last_to_first_p50_ratio"] is None
    assert stats["passed"] is False


def test_minute_trend_integration_reuses_original_warmup_mask(analysis):
    """Warmup's slow sample does not enter either the trend origin or per-minute statistics."""
    report = _report(analysis)
    result = analysis["analyze_reports"](report, copy.deepcopy(report))[0]["minute_trends"]
    assert result["origin_sent_ns"] == 1_000_000_000
    assert result["buckets"][0]["ack_latency"] == {
        "count": 2,
        "p50_micros": 1000,
        "p99_micros": 1000,
        "over_10ms_count": 0,
    }
    assert result["buckets"][0]["visible_latency"]["p50_micros"] == 3000


@pytest.mark.parametrize(
    "writer_sent,ack,visible_sent,received",
    [
        ([0], [], [], []),
        ([1], [0], [], []),
        ([1, 0], [2, 2], [], []),
        ([1], [2], [0], [2]),
        ([], [], [0], [1]),
    ],
)
def test_minute_trends_reject_invalid_raw_timestamp_pairs(
    analysis, writer_sent, ack, visible_sent, received
):
    """Malformed array lengths or time domains cannot yield plausible trend gates."""
    with pytest.raises(ValueError):
        analysis["minute_latency_trends"](
            writer_sent, ack, visible_sent, received, measurement_complete=True
        )
