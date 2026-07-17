"""Deterministic unit contracts for client-side parsing and buffering."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
import socket
import struct
from typing import Any

import pyarrow as pa
import pytest

from zippy_client.client import (
    _capabilities,
    _int_config,
    _parse_timeout,
    _validate_subscriber_wait,
)
from zippy_client.endpoints import (
    ControlEndpoint,
    connectable_gateway_endpoint,
    gateway_is_local,
    normalize_tcp_address,
    resolve_control_endpoint,
    split_host_port,
)
from zippy_client.errors import ContinuityError, ProtocolError, TransportUnavailableError
from zippy_client.expressions import (
    Expr,
    col,
    expression_supported_by_gateway,
    expression_to_json,
    lit,
    normalize_expressions,
    normalize_temporal_filter,
)
from zippy_client.models import Operation, Transport
from zippy_client.protocol import (
    is_scalar_row,
    receive_frame,
    schema_from_ipc,
    send_frame,
    table_from_ipc,
    table_to_ipc,
    value_to_table,
)
from zippy_client.router import TransportRouter
from zippy_client.subscriber import _next_retained_descriptor
from zippy_client.writer import GatewayWriter


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


def test_expression_normalization_and_gateway_support_contract() -> None:
    normalized = normalize_expressions((["a", "b"],), {"renamed": col("c")})

    assert normalized[:2] == ["a", "b"]
    assert expression_to_json(normalized[2])["kind"] == "alias"
    assert expression_supported_by_gateway(col("a").is_in({1, 2}))
    assert expression_supported_by_gateway((col("a") == 1) & (col("b") >= 2))
    assert not expression_supported_by_gateway("a")
    assert not expression_supported_by_gateway(col("a").is_null())
    assert not expression_supported_by_gateway(col("a") + 1)
    assert not expression_supported_by_gateway(Expr("is_in", None, (lit(1), [1])))
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


def test_arrow_ipc_and_writer_value_normalization_round_trip() -> None:
    schema = pa.schema([("sequence", pa.int64())])
    table = pa.table({"sequence": [1, 2]})
    batch = table.to_batches()[0]

    payload = table_to_ipc(table)

    assert table_from_ipc(payload).equals(table)
    assert schema_from_ipc(payload).equals(table.schema)
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


def test_frame_codec_rejects_bad_headers_and_client_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    left, right = socket.socketpair()
    try:
        monkeypatch.setenv("ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES", "1")
        with pytest.raises(ProtocolError, match="request header exceeds"):
            send_frame(left, {"kind": "ping"})
        monkeypatch.setenv("ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES", "1024")
        monkeypatch.setenv("ZIPPY_CLIENT_MAX_FRAME_PAYLOAD_BYTES", "1")
        with pytest.raises(ProtocolError, match="request payload exceeds"):
            send_frame(left, {}, b"ab")
    finally:
        left.close()
        right.close()

    for raw_header, match in ((b"{", "not valid"), (b"[]", "must be a JSON object")):
        left, right = socket.socketpair()
        try:
            left.sendall(struct.pack("!IQ", len(raw_header), 0) + raw_header)
            with pytest.raises(ProtocolError, match=match):
                receive_frame(right)
        finally:
            left.close()
            right.close()


@pytest.mark.parametrize("value", ["invalid", "0", "-1"])
def test_frame_budget_environment_must_be_positive_integer(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES", value)
    left, right = socket.socketpair()
    try:
        with pytest.raises(ValueError, match="environment value"):
            send_frame(left, {})
    finally:
        left.close()
        right.close()


def test_receive_frame_reports_early_peer_close() -> None:
    left, right = socket.socketpair()
    left.close()
    try:
        with pytest.raises(ProtocolError, match="connection closed"):
            receive_frame(right)
    finally:
        right.close()


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
    assert connectable_gateway_endpoint("0.0.0.0:17666", unix_control) == "127.0.0.1:17666"
    assert connectable_gateway_endpoint("0.0.0.0:17666", tcp_control) == "192.0.2.10:17666"
    assert connectable_gateway_endpoint("localhost:17666", tcp_control) == "localhost:17666"
    assert gateway_is_local("127.0.0.1:17666", tcp_control)
    assert gateway_is_local("192.0.2.11:17666", unix_control)
    assert not gateway_is_local("192.0.2.11:17666", tcp_control)


def test_transport_parsing_and_explicit_router_failures() -> None:
    assert Transport.parse(Transport.SHM) is Transport.SHM
    assert Transport.parse("gateway_remote") is Transport.GATEWAY_REMOTE
    with pytest.raises(ValueError, match="unsupported transport"):
        Transport.parse("invalid")

    local = ControlEndpoint("unix", "/tmp/master.sock", "zippy://default")
    remote = ControlEndpoint("tcp", "192.0.2.10:17690", "zippy://192.0.2.10:17690/default")
    no_gateway = TransportRouter(local, None, lambda: True)
    with pytest.raises(TransportUnavailableError, match="does not advertise"):
        no_gateway.select(Operation.QUERY, Transport.AUTO)
    with pytest.raises(TransportUnavailableError, match="same IPC namespace"):
        TransportRouter(remote, "192.0.2.10:17666", lambda: True).select(
            Operation.SUBSCRIBE,
            Transport.SHM,
        )
    with pytest.raises(TransportUnavailableError, match="unavailable"):
        TransportRouter(local, "127.0.0.1:17666", lambda: False).select(
            Operation.SUBSCRIBE,
            Transport.SHM,
        )
    with pytest.raises(TransportUnavailableError, match="does not expose"):
        TransportRouter(local, "127.0.0.1:17666", lambda: True).select(
            Operation.SUBSCRIBE,
            Transport.SHM,
            stream_info={"data_path": "historical"},
        )
    with pytest.raises(TransportUnavailableError, match="scope does not match"):
        TransportRouter(local, "127.0.0.1:17666", lambda: True).select(
            Operation.QUERY,
            Transport.GATEWAY_REMOTE,
        )


def test_retained_descriptor_selection_never_skips_a_sealed_successor() -> None:
    first: dict[str, object] = {"segment_id": 1, "generation": 0, "writer_epoch": 1}
    second: dict[str, object] = {"segment_id": 2, "generation": 0, "writer_epoch": 1}
    third: dict[str, object] = {"segment_id": 3, "generation": 0, "writer_epoch": 1}

    assert (
        _next_retained_descriptor(
            {"stream_name": "ticks", "sealed_segments": [first, second]},
            first,
        )
        == second
    )
    assert (
        _next_retained_descriptor(
            {
                "stream_name": "ticks",
                "sealed_segments": [first, second],
                "active_segment_descriptor": third,
            },
            second,
        )
        == third
    )
    with pytest.raises(ContinuityError, match="cannot be proven"):
        _next_retained_descriptor(
            {"stream_name": "ticks", "sealed_segments": [second]},
            first,
        )
    with pytest.raises(ProtocolError, match="must be a list"):
        _next_retained_descriptor(
            {"stream_name": "ticks", "sealed_segments": {}},
            first,
        )


class _RecordingGateway:
    def __init__(self) -> None:
        self.requests: list[tuple[dict[str, object], bytes]] = []
        self.fail_next = False

    def request(
        self,
        header: dict[str, object],
        payload: bytes = b"",
    ) -> tuple[dict[str, Any], bytes]:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("injected write failure")
        self.requests.append((header, payload))
        return {"status": "ok"}, b""


def test_gateway_writer_buffers_batches_retries_and_closes() -> None:
    gateway = _RecordingGateway()
    schema = pa.schema([("sequence", pa.int64())])
    writer = GatewayWriter("ticks", gateway, schema=schema, batch_size=2)

    writer.write({"sequence": 1})
    assert gateway.requests == []
    gateway.fail_next = True
    with pytest.raises(RuntimeError, match="injected write failure"):
        writer.write({"sequence": 2})
    assert writer._rows == [{"sequence": 1}, {"sequence": 2}]

    writer.flush()
    writer.write_batch(pa.table({"sequence": [3]}))
    writer.close()

    assert [request[0]["kind"] for request in gateway.requests] == [
        "write_batch",
        "write_batch",
        "close_writer",
    ]
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.write({"sequence": 4})
    with pytest.raises(TypeError, match="requires a batched value"):
        GatewayWriter("other", gateway, schema=schema).write_batch({"sequence": 1})


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
    _validate_subscriber_wait(0, True, 0)
    with pytest.raises(ValueError, match="unless xfast"):
        _validate_subscriber_wait(0, False, 0)
    with pytest.raises(ValueError, match="idle_spin_checks"):
        _validate_subscriber_wait(1, False, -1)

    capabilities = _capabilities(
        {"server": {"capabilities": {"query_sql": True}}},
        {"enabled": True, "endpoint": "127.0.0.1:17666", "protocol_version": "1"},
    )
    assert capabilities.gateway
    assert capabilities.query_sql
    assert capabilities.gateway_protocol_version == 1
