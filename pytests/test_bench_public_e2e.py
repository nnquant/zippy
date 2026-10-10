"""Validate public-path benchmark statistics and decisions without starting services."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import runpy
import sys
from typing import Any

import pytest


@pytest.fixture(scope="module")
def bench() -> dict[str, Any]:
    """Load benchmark helpers without executing the CLI.

    :returns: Module globals containing the pure benchmark helpers.
    """
    return runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/bench_public_e2e.py"))


@pytest.fixture
def thresholds() -> dict[str, int]:
    """Provide the unchanged Flight and WAL latency budgets.

    :returns: Latency thresholds expressed in microseconds.
    """
    return {
        "visible_p50_micros": 1_000,
        "visible_p99_micros": 3_000,
        "visible_p99_9_micros": 10_000,
        "wal_ack_p50_micros": 2_000,
        "wal_ack_p99_micros": 5_000,
    }


@pytest.mark.parametrize(
    "percentile,expected",
    [(0, 10), (1, 10), (25, 10), (50, 20), (75, 30), (95, 40), (99.9, 40), (100, 40)],
)
def test_nearest_rank_uses_observed_values_without_interpolation(bench, percentile, expected):
    """Nearest-rank percentiles select the ceiling rank from an unsorted sample."""
    values = [40, 10, 30, 20]
    assert bench["nearest_rank"](values, percentile) == expected
    assert values == [40, 10, 30, 20]


def test_nearest_rank_handles_empty_singleton_and_repeated_samples(bench):
    """Empty input is unavailable; ties and a single observation retain their values."""
    assert bench["nearest_rank"]([], 99) is None
    assert bench["nearest_rank"]([17], 99.9) == 17
    assert bench["nearest_rank"]([1, 1, 1, 2], 75) == 1


@pytest.mark.parametrize("percentile", [-0.1, 100.1, float("nan"), float("inf"), -float("inf")])
def test_nearest_rank_rejects_invalid_percentiles(bench, percentile):
    """Reject out-of-range and nonfinite percentile requests explicitly."""
    with pytest.raises(ValueError):
        bench["nearest_rank"]([1, 2], percentile)


def test_summary_converts_nanoseconds_to_microseconds(bench):
    """All reported durations use microseconds while ranks retain observed samples."""
    summary = bench["summarize_ns"]([4_000, 1_000, 3_000, 2_000])
    assert summary["count"] == 4
    assert summary["p50_micros"] == 2
    assert summary["p95_micros"] == 4
    assert summary["p99_micros"] == 4
    assert summary["max_micros"] == 4
    assert summary["mean_micros"] == 2.5
    assert summary["p99_9_micros"] is None
    assert summary["p99_9_unavailable_reason"]


def test_summary_preserves_submicrosecond_precision(bench):
    """Conversion must not truncate submicrosecond observations."""
    summary = bench["summarize_ns"]([125, 375])
    assert summary["p50_micros"] == 0.125
    assert summary["max_micros"] == 0.375
    assert summary["mean_micros"] == 0.25


@pytest.mark.parametrize("count", [0, 1, 9_999])
def test_summary_does_not_report_p99_9_for_insufficient_samples(bench, count):
    """Fewer than ten thousand samples cannot produce the advertised tail percentile."""
    summary = bench["summarize_ns"]([1_000] * count)
    assert summary["count"] == count
    assert summary["p99_9_micros"] is None
    assert summary["p99_9_unavailable_reason"]


def test_summary_reports_p99_9_at_exact_sample_minimum(bench):
    """At ten thousand samples the 99.9th percentile is the 9,990th ranked value."""
    summary = bench["summarize_ns"]([value * 1_000 for value in range(1, 10_001)])
    assert summary["count"] == 10_000
    assert summary["p99_9_micros"] == 9_990
    assert not summary["p99_9_unavailable_reason"]


def test_empty_summary_cannot_fabricate_latency_measurements(bench):
    """An empty measurement set has no numeric duration statistics."""
    summary = bench["summarize_ns"]([])
    for key in ("p50_micros", "p95_micros", "p99_micros", "max_micros", "mean_micros"):
        assert summary[key] is None


def test_warmup_requires_both_elapsed_time_and_sample_minimum(bench):
    """A row becomes measurable only after both independent warmup boundaries."""
    started_ns = 10_000_000_000
    mask = bench["measurement_mask"](
        [0, 999, 1_000, 1_001, 1_002],
        [
            started_ns,
            started_ns + 2_500_000_000,
            started_ns + 1_999_999_999,
            started_ns + 2_000_000_000,
            started_ns + 3_000_000_000,
        ],
        started_ns,
        2.0,
        1_000,
    )
    assert mask == [False, False, False, True, True]


def test_warmup_boundaries_are_inclusive_and_use_fractional_seconds(bench):
    """The first post-warmup sequence at the exact elapsed-time boundary is included."""
    assert bench["measurement_mask"]([5, 6], [124_999_999, 125_000_000], 0, 0.125, 5) == [
        False,
        True,
    ]
    assert bench["measurement_mask"]([5], [125_000_000], 0, 0.125, 5) == [True]


def test_zero_warmup_includes_the_first_row_and_empty_input_stays_empty(bench):
    """Disabling both warmup conditions counts every supplied sample."""
    assert bench["measurement_mask"]([0, 1], [100, 101], 100, 0, 0) == [True, True]
    assert bench["measurement_mask"]([], [], 100, 2, 1_000) == []


@pytest.mark.parametrize(
    "sequences,expected_count,missing,duplicates,out_of_order,unexpected,passed",
    [
        ([0, 1, 2, 3], 4, 0, 0, 0, 0, True),
        ([0, 2, 3], 4, 1, 0, 0, 0, False),
        ([0, 1, 2], 4, 1, 0, 0, 0, False),
        ([1, 2, 3], 4, 1, 0, 0, 0, False),
        ([], 4, 4, 0, 0, 0, False),
        ([0, 1, 1, 1, 2, 3], 4, 0, 2, 0, 0, False),
        ([0, 2, 1, 3], 4, 0, 0, 1, 0, False),
        ([3, 2, 1, 0], 4, 0, 0, 3, 0, False),
        ([-1, 0, 1, 2, 3, 4], 4, 0, 0, 0, 2, False),
        ([0, 2, 2, 1, 4], 4, 1, 1, 1, 1, False),
    ],
)
def test_integrity_rejects_gaps_duplicates_reordering_and_unexpected_sequences(
    bench, sequences, expected_count, missing, duplicates, out_of_order, unexpected, passed
):
    """Integrity failure identifies each independently observable delivery defect."""
    result = bench["check_integrity"](sequences, expected_count)
    assert result["passed"] is passed
    assert result["missing_count"] == missing
    assert result["duplicate_count"] == duplicates
    assert result["out_of_order_count"] == out_of_order
    assert result["unexpected_count"] == unexpected


@pytest.mark.parametrize("durability", ["memory", "wal"])
def test_thresholds_accept_exact_limits(bench, thresholds, durability):
    """Every published threshold is inclusive."""
    visible = {"count": 10_000, "p50_micros": 1_000, "p99_micros": 3_000, "p99_9_micros": 10_000}
    ack = {"count": 10_000, "p50_micros": 2_000, "p99_micros": 5_000}
    result = bench["evaluate_thresholds"](visible, ack, durability, thresholds)
    assert result["passed"] is True
    assert result["failures"] == []


@pytest.mark.parametrize(
    "statistic,value",
    [("p50_micros", 1_000.001), ("p99_micros", 3_000.001), ("p99_9_micros", 10_000.001)],
)
@pytest.mark.parametrize("durability", ["memory", "wal"])
def test_each_visible_threshold_can_fail_independently(
    bench, thresholds, statistic, value, durability
):
    """A single visible latency breach fails either durability mode."""
    visible = {
        "count": 10_000,
        "p50_micros": 1,
        "p99_micros": 2,
        "p99_9_micros": 3,
        statistic: value,
    }
    ack = {"count": 10_000, "p50_micros": 1, "p99_micros": 2}
    result = bench["evaluate_thresholds"](visible, ack, durability, thresholds)
    assert result["passed"] is False
    assert len(result["failures"]) == 1


@pytest.mark.parametrize("statistic,value", [("p50_micros", 2_000.001), ("p99_micros", 5_000.001)])
def test_wal_ack_thresholds_apply_only_to_wal(bench, thresholds, statistic, value):
    """WAL acknowledgement budgets do not classify a memory acknowledgement as durable."""
    visible = {"count": 10_000, "p50_micros": 1, "p99_micros": 2, "p99_9_micros": 3}
    ack = {"count": 10_000, "p50_micros": 1, "p99_micros": 2, statistic: value}
    wal = bench["evaluate_thresholds"](visible, ack, "wal", thresholds)
    memory = bench["evaluate_thresholds"](visible, ack, "memory", thresholds)
    assert wal["passed"] is False
    assert len(wal["failures"]) == 1
    assert memory["passed"] is True
    assert memory["failures"] == []


def test_unavailable_p99_9_skips_only_that_threshold(bench, thresholds):
    """Short smoke runs must still enforce p50 and p99 when p99.9 is unavailable."""
    visible = {"count": 3_000, "p50_micros": 1, "p99_micros": 2, "p99_9_micros": None}
    ack = {"count": 3_000, "p50_micros": 1, "p99_micros": 2}
    assert bench["evaluate_thresholds"](visible, ack, "wal", thresholds)["passed"] is True
    visible["p99_micros"] = 3_001
    result = bench["evaluate_thresholds"](visible, ack, "wal", thresholds)
    assert result["passed"] is False
    assert len(result["failures"]) == 1


def test_empty_samples_do_not_pass_thresholds(bench, thresholds):
    """An empty sample set cannot yield a successful latency verdict."""
    empty = bench["summarize_ns"]([])
    result = bench["evaluate_thresholds"](empty, empty, "wal", thresholds)
    assert result["passed"] is False
    assert result["failures"]


def test_custom_thresholds_are_used_without_silently_relaxing_them(bench, thresholds):
    """The supplied budget directly controls a cell's pass/fail result."""
    visible = {"count": 10_000, "p50_micros": 1, "p99_micros": 2, "p99_9_micros": 3}
    ack = {"count": 10_000, "p50_micros": 1, "p99_micros": 2}
    thresholds["visible_p99_micros"] = 1
    result = bench["evaluate_thresholds"](visible, ack, "memory", thresholds)
    assert result["passed"] is False
    assert len(result["failures"]) == 1


@pytest.mark.parametrize(
    "cells,expected",
    [
        ([], 1),
        ([{"passed": True}], 0),
        ([{"passed": True}, {"passed": True}], 0),
        ([{"passed": False}], 1),
        ([{"passed": True}, {"passed": False}], 1),
        ([{"passed": False}, {"passed": True}], 1),
    ],
)
def test_exit_code_requires_every_matrix_cell_to_pass(bench, cells, expected):
    """A failure in any cell or an unexecuted matrix produces a nonzero exit status."""
    assert bench["exit_code"](cells) == expected


@pytest.fixture
def complete_evidence(thresholds) -> tuple[dict[str, Any], dict[str, Any]]:
    """Provide a complete stream with one warmup row and two measured rows.

    :param thresholds: Flight and WAL latency budgets in microseconds.
    :returns: Cell configuration and independent writer/subscriber evidence.
    """
    started_ns = 1_000_000_000
    sent_ns = [started_ns + sequence * 1_000_000 for sequence in range(3)]
    config = {
        "samples": 2,
        "warmup_seconds": 0.001,
        "warmup_samples": 1,
        "durability": "memory",
        "background_rate": 0,
        "rate": 1_000,
        "thresholds": thresholds,
    }
    raw = {
        "writer": {
            "seq": [0, 1, 2],
            "sent_ns": sent_ns,
            "ack_ns": [stamp + 100 for stamp in sent_ns],
            "schedule_lag_ns": [0, 0, 0],
            "started_ns": started_ns,
            "finished_ns": sent_ns[-1] + 100,
            "end_sent": True,
        },
        "subscriber": {
            "seq": [0, 1, 2],
            "sent_ns": sent_ns.copy(),
            "received_ns": [stamp + 200 for stamp in sent_ns],
            "finished_ns": sent_ns[-1] + 300,
            "end_seq": 3,
        },
    }
    return config, raw


def test_analyze_accepts_complete_evidence_and_excludes_warmup(bench, complete_evidence):
    """A valid short run passes with two measured rows and an unavailable p99.9."""
    config, raw = complete_evidence
    cell = bench["_analyze"](config, raw)
    assert cell["passed"] is True
    assert cell["failures"] == []
    assert cell["integrity"]["passed"] is True
    assert cell["warmup_rows"] == 1
    assert cell["ack_latency"]["count"] == cell["visible_latency"]["count"] == 2
    assert cell["ack_latency"]["p50_micros"] == 0.1
    assert cell["visible_latency"]["p50_micros"] == 0.2
    assert cell["visible_latency"]["p99_9_micros"] is None
    assert bench["exit_code"]([cell]) == 0


@pytest.mark.parametrize("side", ["writer", "subscriber"])
def test_analyze_rejects_missing_or_inconsistent_end_marker(bench, complete_evidence, side):
    """Receiving every acknowledged row does not prove the stream ended successfully."""
    config, raw = complete_evidence
    if side == "writer":
        raw["writer"]["end_sent"] = False
    else:
        raw["subscriber"]["end_seq"] = 2
    cell = bench["_analyze"](config, raw)
    assert cell["integrity"]["passed"] is True
    assert cell["passed"] is False
    assert any("end marker" in failure for failure in cell["failures"])
    assert bench["exit_code"]([cell]) == 1


def test_analyze_rejects_complete_prefix_with_too_few_measurements(bench, complete_evidence):
    """A complete acknowledged prefix cannot satisfy a larger requested measurement count."""
    config, raw = complete_evidence
    config["samples"] = 3
    cell = bench["_analyze"](config, raw)
    assert cell["integrity"]["passed"] is True
    assert cell["passed"] is False
    assert any("ack sample count=[2] expected=[3]" in failure for failure in cell["failures"])
    assert any("visible sample count=[2] expected=[3]" in failure for failure in cell["failures"])
    assert bench["exit_code"]([cell]) == 1


@pytest.mark.parametrize("role", ["writer", "subscriber", "background"])
def test_analyze_propagates_any_worker_error_despite_complete_rows(bench, complete_evidence, role):
    """Successful latency samples cannot hide a terminal worker failure."""
    config, raw = complete_evidence
    raw.setdefault(role, {})["error"] = "deterministic terminal failure"
    cell = bench["_analyze"](config, raw)
    assert cell["integrity"]["passed"] is True
    assert cell["passed"] is False
    assert f"{role}: deterministic terminal failure" in cell["failures"]
    assert bench["exit_code"]([cell]) == 1


def test_analyze_keeps_partial_latency_evidence_without_declaring_success(bench, complete_evidence):
    """A failed run preserves valid prefix statistics while reporting incomplete execution."""
    config, raw = complete_evidence
    config["samples"] = 20_000
    raw["writer"].update(end_sent=False, error="ambiguous write outcome")
    raw["subscriber"]["end_seq"] = None
    cell = bench["_analyze"](config, raw)
    assert cell["integrity"]["missing_count"] == 0
    assert cell["ack_latency"]["count"] == cell["visible_latency"]["count"] == 2
    assert cell["passed"] is False
    assert "writer: ambiguous write outcome" in cell["failures"]
    assert any("end marker" in failure for failure in cell["failures"])
    assert any("sample count" in failure for failure in cell["failures"])
    assert bench["exit_code"]([cell]) == 1


def test_analyze_rejects_changed_send_timestamp_even_with_complete_sequences(
    bench, complete_evidence
):
    """A modified embedded timestamp invalidates otherwise complete, within-budget evidence."""
    config, raw = complete_evidence
    raw["subscriber"]["sent_ns"][1] += 1
    cell = bench["_analyze"](config, raw)
    assert cell["integrity"]["passed"] is True
    assert cell["visible_latency"]["count"] == 2
    assert cell["passed"] is False
    assert "received sent_ns differs from the writer evidence" in cell["failures"]
    assert bench["exit_code"]([cell]) == 1


@pytest.mark.parametrize(
    "observed,defect",
    [
        ([1, 2], "missing_count"),
        ([0, 0, 1, 2], "duplicate_count"),
        ([1, 0, 2], "out_of_order_count"),
    ],
)
def test_analyze_integrity_failure_overrides_complete_measurement_counts(
    bench, complete_evidence, observed, defect
):
    """Warmup delivery defects still fail the cell and the matrix when measured rows are intact."""
    config, raw = complete_evidence
    sent_ns = raw["writer"]["sent_ns"]
    raw["subscriber"].update(
        seq=observed,
        sent_ns=[sent_ns[sequence] for sequence in observed],
        received_ns=[sent_ns[-1] + 200 + arrival * 100 for arrival in range(len(observed))],
    )
    cell = bench["_analyze"](config, raw)
    assert cell["ack_latency"]["count"] == cell["visible_latency"]["count"] == 2
    assert cell["integrity"][defect] == 1
    assert cell["integrity"]["passed"] is False
    assert cell["passed"] is False
    assert "sequence integrity failed (including warmup)" in cell["failures"]
    assert len(cell["failures"]) == 1
    assert bench["exit_code"]([{"passed": True}, cell]) == 1


def test_summary_preserves_error_chains_without_raw_timestamps_or_full_logs(
    bench, complete_evidence
):
    """Retain failure diagnosis and exact sample counts in the default compact artifact.

    :returns: None.
    """
    config, raw = complete_evidence
    error = "SEQUENCE_GAP: live segment offsets are not contiguous expected=[4] actual=[1]"
    raw["writer"].update(error="AmbiguousWriteError", error_traceback=f"{error}\ncaused by write")
    cell = bench["_analyze"](config, raw)
    mismatch = "ERROR native table materialized row count mismatch expected=[3] actual=[4]"
    cell["diagnostic_logs"] = {"server.log": f"INFO routine server startup\n{mismatch}\n"}
    original = copy.deepcopy(cell)
    summary = bench["_cell_summary"](cell)
    assert "raw_samples" not in summary
    assert "diagnostic_logs" not in summary
    assert summary["ack_latency"] == cell["ack_latency"]
    assert summary["visible_latency"] == cell["visible_latency"]
    assert summary["integrity"] == cell["integrity"]
    assert summary["failures"] == cell["failures"]
    writer = summary["workers"]["writer"]
    assert writer["error_traceback"] == raw["writer"]["error_traceback"]
    assert writer["recorded_samples"] == {
        "seq": 3,
        "sent_ns": 3,
        "ack_ns": 3,
        "schedule_lag_ns": 3,
    }
    assert "sent_ns" not in writer
    assert "ack_ns" not in writer
    diagnostics = summary["diagnostic_summary"]["server.log"]
    assert diagnostics["error_lines"] == [mismatch]
    assert diagnostics["omitted_error_lines"] == 0
    assert "routine server startup" not in json.dumps(summary)
    assert cell == original


def test_summary_bounds_repeated_and_long_server_errors(bench):
    """Repeated server failures cannot grow the summary into a full diagnostic log.

    :returns: None.
    """
    long_error = "ERROR " + "x" * 5000
    lines = [long_error, long_error, *(f"ERROR failure [{index}]" for index in range(30))]
    summary = bench["_cell_summary"]({"diagnostic_logs": {"server.log": "\n".join(lines)}})
    diagnostics = summary["diagnostic_summary"]["server.log"]
    assert len(diagnostics["error_lines"]) == 20
    assert len(diagnostics["error_lines"][0]) == 2000
    assert diagnostics["omitted_error_lines"] == 11
    assert diagnostics["truncated_error_lines"] == 1


@pytest.mark.parametrize("include_raw", [False, True])
def test_report_outputs_separate_summary_and_optional_raw_evidence(
    bench, complete_evidence, tmp_path, include_raw
):
    """Raw output is opt-in and never replaces the default summary's error evidence.

    :returns: None.
    """
    config, raw = complete_evidence
    report = {
        "schema_version": 2,
        "config": config,
        "metadata": {"git_sha": "test-commit"},
        "cells": [bench["_analyze"](config, raw)],
        "error": "outer failure",
        "error_traceback": "complete outer traceback",
    }
    report["cells"][0]["diagnostic_logs"] = {"server.log": "INFO full server diagnostic log"}
    report_path, raw_path = tmp_path / "summary.json", tmp_path / "raw.json"
    bench["_write_reports"](report_path, raw_path if include_raw else None, report)
    summary = json.loads(report_path.read_text())
    assert summary["artifact_kind"] == "summary"
    for field in ("schema_version", "config", "metadata", "error", "error_traceback"):
        assert summary[field] == report[field]
    assert "raw_samples" not in summary["cells"][0]
    assert "diagnostic_logs" not in summary["cells"][0]
    assert raw_path.exists() is include_raw
    if include_raw:
        saved_raw = json.loads(raw_path.read_text())
        assert saved_raw["artifact_kind"] == "raw_samples"
        assert saved_raw["cells"][0]["raw_samples"] == raw
        assert saved_raw["cells"][0]["diagnostic_logs"] == report["cells"][0]["diagnostic_logs"]


@pytest.mark.parametrize("include_raw", [False, True])
def test_cli_raw_samples_output_is_optional(bench, tmp_path, monkeypatch, include_raw):
    """The CLI accepts an explicit raw artifact path and otherwise defaults to summaries.

    :returns: None.
    """
    server = tmp_path / "zippy-server"
    server.touch()
    arguments = ["benchmark", "--server-bin", str(server)]
    if include_raw:
        arguments.extend(["--raw-samples-json", str(tmp_path / "raw.json")])
    monkeypatch.setattr(sys, "argv", arguments)
    args = bench["_parse_args"]()
    assert args.raw_samples_json == (tmp_path / "raw.json" if include_raw else None)


def test_cli_rejects_shared_summary_and_raw_output_path(bench, tmp_path, monkeypatch):
    """An output collision must fail before raw timestamps can overwrite the summary.

    :returns: None.
    """
    path = str(tmp_path / "evidence.json")
    monkeypatch.setattr(
        sys, "argv", ["benchmark", "--report-json", path, "--raw-samples-json", path]
    )
    with pytest.raises(SystemExit) as error:
        bench["_parse_args"]()
    assert error.value.code == 2
