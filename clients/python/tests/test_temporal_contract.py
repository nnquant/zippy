"""One exact temporal literal contract for both Python packages."""

from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from zippy_client.expressions import col, expression_to_json, lit, normalize_temporal_filter


def bounds(value: object, zone: str | None = "UTC") -> tuple[int, int]:
    schema = pa.schema([("dt", pa.timestamp("ns", tz=zone))])
    expression = expression_to_json(normalize_temporal_filter(col("dt") == value, schema))
    lower, upper = expression["args"]
    return lower["args"][1]["value"], upper["args"][1]["value"]


@pytest.mark.parametrize(
    "value",
    [
        "2026-0912",
        "202609-12",
        "2026-09 12:00",
        "202609T12:00:00Z",
        "2026-09-12\n",
        " 2026-09-12",
        "2026-09-12T12:00:00+00:60",
        "2026-09-12T12:00:00-0199",
        "2026-09-12T12:00:00+24:00",
        "2026-09-12T12:00:00.1234567890Z",
        "2026-09-12T12:00:00junk",
    ],
)
def test_malformed_literals_never_silently_change_query_range(value: str) -> None:
    with pytest.raises(ValueError, match="invalid temporal literal"):
        bounds(value)


@pytest.mark.parametrize(
    "compact,dashed",
    [
        ("202609", "2026-09"),
        ("20260912", "2026-09-12"),
        ("20260912T12:34:56.123456789+0800", "2026-09-12T12:34:56.123456789+08:00"),
    ],
)
def test_compact_and_dashed_formats_have_identical_bounds(compact: str, dashed: str) -> None:
    assert bounds(compact) == bounds(dashed)


def test_fractional_boundaries_keep_nanoseconds_before_and_after_epoch() -> None:
    assert bounds("1970-01-01T00:00:00.000000001Z") == (1, 2)
    assert bounds("1969-12-31T23:59:59.999999999Z") == (-1, 0)
    assert bounds("1970-01-01T00:00:00.12Z") == (120_000_000, 130_000_000)
    assert bounds("1970-01-01T08:00:00.000000001+08:00") == (1, 2)


@pytest.mark.parametrize("value,hours", [("2026-03-08", 23), ("2026-11-01", 25)])
def test_calendar_day_ranges_follow_declared_zone(value: str, hours: int) -> None:
    first, last = bounds(value, "America/New_York")
    assert last - first == hours * 3_600_000_000_000
    assert bounds(date.fromisoformat(value), "America/New_York") == (first, last)


def test_datetime_subclass_nanosecond_remainder_uses_column_zone() -> None:
    class NanoDatetime(datetime):
        @property
        def nanosecond(self) -> int:
            return 7

        @property
        def value(self) -> Any:
            raise AssertionError("unit-dependent datetime .value must not be read")

    local = NanoDatetime(1970, 1, 1, 8)
    assert bounds(local, "Asia/Shanghai") == (7, 8)
    assert bounds(NanoDatetime(1970, 1, 1, tzinfo=timezone.utc)) == (7, 8)
    assert bounds(datetime(1970, 1, 1, 8), "Asia/Shanghai") == (0, 1_000)


def test_noncomparison_nodes_are_not_rewritten_as_time_ranges() -> None:
    expression = lit("1970-01-01") + col("dt")
    schema = pa.schema([("dt", pa.timestamp("ns", tz="UTC"))])
    assert normalize_temporal_filter(expression, schema) is expression
