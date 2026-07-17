"""Expression serialization and bounded frame codec tests."""

from __future__ import annotations

from datetime import datetime, timezone
import socket
import struct

import pyarrow as pa
import pytest

from zippy_client.errors import ProtocolError
from zippy_client.expressions import (
    col,
    expression_to_json,
    normalize_temporal_filter,
)
from zippy_client.protocol import receive_frame, send_frame


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


def test_frame_round_trip_uses_bounded_header_and_payload() -> None:
    left, right = socket.socketpair()
    try:
        send_frame(left, {"status": "ok", "kind": "table"}, b"payload")
        header, payload = receive_frame(right)
    finally:
        left.close()
        right.close()

    assert header == {"status": "ok", "kind": "table"}
    assert payload == b"payload"


def test_receive_frame_rejects_payload_length_above_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ZIPPY_CLIENT_MAX_FRAME_PAYLOAD_BYTES", "8")
    left, right = socket.socketpair()
    try:
        left.sendall(struct.pack("!IQ", 2, 9) + b"{}")
        with pytest.raises(ProtocolError, match="payload exceeds client budget"):
            receive_frame(right)
    finally:
        left.close()
        right.close()
