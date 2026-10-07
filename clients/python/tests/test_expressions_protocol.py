"""Shared expression serialization tests."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from zippy_client.expressions import (
    col,
    expression_to_json,
    normalize_temporal_filter,
)


def test_temporal_day_equality_becomes_typed_half_open_range() -> None:
    schema = pa.schema([("dt", pa.timestamp("ns", tz="UTC"))])

    normalized = normalize_temporal_filter(col("dt") == "2026-07-17", schema)
    payload = expression_to_json(normalized)

    assert payload["kind"] == "binary"
    assert payload["op"] == "and"
    lower, upper = payload["args"]
    assert lower["op"] == "ge"
    assert upper["op"] == "lt"
    assert lower["args"][1]["literal_type"] == "timestamp_ns"
    assert upper["args"][1]["literal_type"] == "timestamp_ns"
    assert lower["args"][1]["timezone"] == "UTC"
    assert upper["args"][1]["value"] - lower["args"][1]["value"] == 86_400_000_000_000


def test_datetime_literal_preserves_explicit_utc() -> None:
    schema = pa.schema([("dt", pa.timestamp("ns", tz="UTC"))])
    value = datetime(2026, 7, 17, 9, 30, tzinfo=timezone.utc)
    assert value.tzinfo == timezone.utc

    normalized = normalize_temporal_filter(col("dt") >= value, schema)
    payload = expression_to_json(normalized)

    assert payload["args"][1]["literal_type"] == "timestamp_ns"
    assert payload["args"][1]["timezone"] == "UTC"


def test_is_in_serializes_values_as_one_literal_array() -> None:
    payload = expression_to_json(col("instrument_id").is_in(["A", "B"]))

    assert payload == {
        "kind": "is_in",
        "args": [
            {"kind": "col", "value": "instrument_id"},
            {"kind": "literal", "value": ["A", "B"]},
        ],
    }
