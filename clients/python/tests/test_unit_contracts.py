"""Deterministic unit contracts for client-side parsing and buffering."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from zippy_client.client import (
    _capabilities,
    _int_config,
    _parse_timeout,
)
from zippy_client.endpoints import (
    ControlEndpoint,
    connectable_data_endpoint,
    data_endpoint_is_local,
    normalize_tcp_address,
    resolve_control_endpoint,
    split_host_port,
)
from zippy_client.errors import TransportUnavailableError
from zippy_client.expressions import (
    Expr,
    col,
    expression_to_json,
    lit,
    normalize_expressions,
    normalize_temporal_filter,
)
from zippy_client.models import Operation, Transport
from zippy_client.arrow_values import (
    is_scalar_row,
    value_to_table,
)
from zippy_client.router import TransportRouter


def test_expression_ast_serializes_supported_variants() -> None:
    expressions = [
        col("price").alias("last_price"),
        col("price").is_null(),
        col("price").is_not_null(),
        col("price").is_between(1, 2),
        ~col("enabled"),
        -col("price"),
        col("price") != 1,
        col("price") > 1,
        col("price") <= 1,
        (col("price") > 1) | (col("price") < 3),
        col("price") + 1,
        1 + col("price"),
        col("price") - 1,
        1 - col("price"),
        col("price") * 2,
        2 * col("price"),
        col("price") / 2,
        2 / col("price"),
    ]

    payloads = [expression_to_json(expression) for expression in expressions]

    assert payloads[0]["kind"] == "alias"
    assert payloads[1]["kind"] == "is_null"
    assert payloads[2]["kind"] == "is_not_null"
    assert payloads[3]["kind"] == "is_between"
    assert payloads[4]["op"] == "not"
    assert payloads[5]["op"] == "neg"
    assert expression_to_json(42) == {"kind": "literal", "value": 42}
    assert expression_to_json("price") == {"kind": "col", "value": "price"}
    with pytest.raises(TypeError, match="cannot be evaluated"):
        bool(col("price") == 1)
    with pytest.raises(ValueError, match="unsupported query expression"):
        expression_to_json(Expr("unknown"))


def test_expression_normalization_contract() -> None:
    normalized = normalize_expressions((["a", "b"],), {"renamed": col("c")})

    assert normalized[:2] == ["a", "b"]
    assert expression_to_json(normalized[2])["kind"] == "alias"
    with pytest.raises(TypeError, match="is_in values"):
        expression_to_json(col("a").is_in(1))


@pytest.mark.parametrize(
    "predicate",
    [
        col("dt") != "2026-07",
        col("dt") > "2026-07-17 09:30",
        col("dt") < "2026-07-17T09:30:00.123456789+08:00",
        col("dt") <= date(2026, 7, 17),
        lit("2026-07-17") < col("dt"),
        col("dt") >= datetime(2026, 7, 17, 9, 30),
        col("dt") >= datetime(2026, 7, 17, 9, 30, tzinfo=timezone.utc),
    ],
)
def test_temporal_normalization_supports_ranges_and_timezones(predicate: Expr) -> None:
    schema = pa.schema([("dt", pa.timestamp("ns", tz="Asia/Shanghai"))])

    payload = expression_to_json(normalize_temporal_filter(predicate, schema))

    assert payload["kind"] == "binary"


def test_temporal_normalization_handles_date_fields_and_invalid_values() -> None:
    date_schema = pa.schema([("trading_day", pa.date32()), ("value", pa.int64())])
    normalized = normalize_temporal_filter(
        col("trading_day") == date(2026, 7, 17),
        date_schema,
    )

    assert expression_to_json(normalized)["op"] == "and"
    untouched = col("value") == 1
    assert normalize_temporal_filter(untouched, date_schema) is untouched
    missing = col("missing") == "2026-07-17"
    assert normalize_temporal_filter(missing, date_schema) is missing
    unsupported_literal = col("trading_day") == 123
    assert normalize_temporal_filter(unsupported_literal, date_schema) is unsupported_literal
    with pytest.raises(ValueError, match="invalid temporal literal"):
        normalize_temporal_filter(col("trading_day") == "not-a-date", date_schema)


def test_writer_value_normalization() -> None:
    schema = pa.schema([("sequence", pa.int64())])
    table = pa.table({"sequence": [1, 2]})
    batch = table.to_batches()[0]

    assert value_to_table(table) is table
    assert value_to_table(batch).equals(table)
    assert value_to_table({"sequence": 1}, schema).to_pydict() == {"sequence": [1]}
    assert value_to_table({"sequence": [1, 2]}).equals(table)
    assert value_to_table([{"sequence": 1}], schema).num_rows == 1

    class ArrowLike:
        def to_arrow(self):
            return table

    assert value_to_table(ArrowLike()).equals(table)
    assert is_scalar_row({"sequence": 1})
    assert not is_scalar_row({"sequence": [1]})
    assert not is_scalar_row(1)
    with pytest.raises(TypeError, match="writer value"):
        value_to_table(object())


def test_endpoint_resolution_covers_uri_and_wildcard_forms(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    assert resolve_control_endpoint("tcp://localhost:17690").address == "localhost:17690"
    assert Path(resolve_control_endpoint("unix:///tmp/master.sock").address) == Path(
        "/tmp/master.sock"
    )
    assert Path(resolve_control_endpoint("file:///tmp/master.sock").address) == Path(
        "/tmp/master.sock"
    )
    assert resolve_control_endpoint("./master.sock").kind == "unix"
    assert split_host_port("[::1]:17690") == ("::1", 17690)
    assert normalize_tcp_address("[::1]:17690") == "[::1]:17690"
    with pytest.raises(ValueError, match="no longer supported"):
        resolve_control_endpoint("zippy+tcp://localhost:17690")
    with pytest.raises(ValueError, match="must include host and port"):
        normalize_tcp_address("localhost")

    unix_control = ControlEndpoint("unix", "/tmp/master.sock", "zippy://default")
    tcp_control = ControlEndpoint(
        "tcp",
        "192.0.2.10:17690",
        "zippy://192.0.2.10:17690/default",
    )
    assert connectable_data_endpoint("0.0.0.0:17666", unix_control) == "127.0.0.1:17666"
    assert connectable_data_endpoint("0.0.0.0:17666", tcp_control) == "192.0.2.10:17666"
    assert connectable_data_endpoint("localhost:17666", tcp_control) == "localhost:17666"
    assert data_endpoint_is_local("127.0.0.1:17666", tcp_control)
    assert data_endpoint_is_local("192.0.2.11:17666", unix_control)
    assert not data_endpoint_is_local("192.0.2.11:17666", tcp_control)


def test_transport_parsing_and_explicit_router_failures() -> None:
    assert Transport.parse(Transport.FLIGHT_LOCAL) is Transport.FLIGHT_LOCAL
    assert Transport.parse("flight_remote") is Transport.FLIGHT_REMOTE
    for invalid in ("invalid", "gateway_local", "gateway_remote", "shm"):
        with pytest.raises(ValueError, match="unsupported transport"):
            Transport.parse(invalid)
    local = ControlEndpoint("unix", "/tmp/master.sock", "zippy://default")
    with pytest.raises(TransportUnavailableError, match="does not advertise"):
        TransportRouter(local).select(Operation.QUERY, Transport.AUTO)
    with pytest.raises(TransportUnavailableError, match="scope does not match"):
        TransportRouter(local, "127.0.0.1:17667").select(Operation.QUERY, Transport.FLIGHT_REMOTE)


def test_client_configuration_helpers_validate_boundaries() -> None:
    assert _parse_timeout("500ms") == 0.5
    assert _parse_timeout("2m") == 120.0
    assert _parse_timeout("1h") == 3600.0
    assert _parse_timeout("2") == 2.0
    assert _parse_timeout(None) is None
    with pytest.raises(ValueError, match="non-negative"):
        _parse_timeout(-1)

    assert _int_config(None, 4) == 4
    assert _int_config("8", 4) == 8
    with pytest.raises(TypeError, match="must be an integer"):
        _int_config(True, 4)
    from zippy_client.transports.flight import FlightCapabilities

    native = FlightCapabilities(
        protocol_version=1,
        server_version="test",
        names=frozenset(
            {
                "subscription.log_fragments.v1",
                "ingest.append.wal.v1",
                "query.get_flight_info_do_get.v1",
            }
        ),
        max_message_bytes=1024,
        max_pending_tickets=2,
        authentication_required=True,
        session_ttl_ms=1000,
        writer_authentication_enabled=True,
        ingest_enabled=True,
        ingest_max_concurrent_puts=1,
        ingest_max_batches_per_put=2,
        ingest_max_rows_per_batch=10,
        ingest_max_rows_per_put=20,
        ingest_max_batch_bytes=1024,
        ingest_max_bytes_per_put=2048,
        ingest_max_rows_per_frame=10,
    )
    capabilities = _capabilities(native)
    assert capabilities.flight and capabilities.flight_ingest and capabilities.query_sql
    assert capabilities.resume_cursor
    assert capabilities.flight_protocol_version == 1
