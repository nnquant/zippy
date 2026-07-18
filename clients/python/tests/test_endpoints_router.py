"""Endpoint resolution and operation-aware routing tests."""

from __future__ import annotations

import importlib
import os
from types import SimpleNamespace

import pytest

from zippy_client.client import Client, _source_ready
from zippy_client.control import ControlClient
from zippy_client.endpoints import ControlEndpoint, resolve_control_endpoint
from zippy_client.errors import CapabilityError, ProtocolError, TransportUnavailableError
from zippy_client.models import Operation, Transport
from zippy_client.router import TransportRouter
from zippy_client.transports.gateway import GatewayTransport


def test_client_explicit_token_overrides_environment_and_server_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ZIPPY_GATEWAY_TOKEN", "environment-token")
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {
                "enabled": True,
                "endpoint": "127.0.0.1:17666",
                "token": "server-token",
                "protocol_version": 1,
            }
        },
    )

    client = Client("tcp://127.0.0.1:17690", token="explicit-token")
    try:
        assert client.gateway.token == "explicit-token"
    finally:
        client.close()


def test_client_rejects_unknown_gateway_protocol_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {
                "enabled": True,
                "endpoint": "127.0.0.1:17666",
                "protocol_version": 2,
            }
        },
    )

    with pytest.raises(ProtocolError, match="unsupported Gateway protocol version"):
        Client("tcp://127.0.0.1:17690")


def test_read_sql_forwards_to_server_when_capability_is_advertised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {
                "enabled": True,
                "endpoint": "127.0.0.1:17666",
                "protocol_version": 1,
            },
            "capabilities": {"query_sql": True},
        },
    )

    def fake_read_sql(self, sql, *, parameters=None, stream=False):
        captured.update(sql=sql, parameters=parameters, stream=stream)
        return expected, {"executor": "test"}

    monkeypatch.setattr(GatewayTransport, "read_sql", fake_read_sql)
    client = Client("tcp://127.0.0.1:17690")
    try:
        result = client.read_sql(
            "select * from ticks where instrument_id = :instrument_id",
            parameters={"instrument_id": "110001.SH"},
            stream=True,
        )
    finally:
        client.close()

    assert result is expected
    assert captured == {
        "sql": "select * from ticks where instrument_id = :instrument_id",
        "parameters": {"instrument_id": "110001.SH"},
        "stream": True,
    }


def test_client_prefers_discovered_flight_and_preserves_read_sql_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {
                "enabled": True,
                "endpoint": "127.0.0.1:17666",
                "protocol_version": 1,
            },
            "flight": {
                "enabled": True,
                "endpoint": "127.0.0.1:17667",
                "protocol_version": 1,
                "max_message_bytes": 2 * 1024 * 1024,
                "tls": {"enabled": False},
            },
        },
    )

    class DummyFlight:
        def __init__(self, endpoint: str, **kwargs: object) -> None:
            self.endpoint = endpoint
            self.kwargs = kwargs
            self.capabilities = SimpleNamespace(protocol_version=1, memory_ingest=True)

        def read_sql(self, sql: str, bindings: object, **kwargs: object):
            captured.update(sql=sql, bindings=bindings, kwargs=kwargs)
            return expected, {"executor": "flight"}

        def close(self) -> None:
            captured["closed"] = True

    client_module = importlib.import_module("zippy_client.client")
    monkeypatch.setattr(client_module, "FlightTransport", DummyFlight)
    connected = Client("tcp://127.0.0.1:17690")
    try:
        assert connected.query_transport == Transport.FLIGHT_LOCAL
        result = connected.read_sql(
            "SELECT * FROM ticks",
            bindings={"ticks": "ticks"},
        )
    finally:
        connected.close()

    assert result is expected
    assert captured["sql"] == "SELECT * FROM ticks"
    assert captured["bindings"] == {"ticks": "ticks"}
    assert captured["closed"] is True


def test_explicit_gateway_sql_requires_gateway_query_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {
                "enabled": True,
                "endpoint": "127.0.0.1:17666",
                "protocol_version": 1,
            },
            "flight": {
                "enabled": True,
                "endpoint": "127.0.0.1:17667",
                "protocol_version": 1,
                "tls": {"enabled": False},
            },
        },
    )

    class QueryOnlyFlight:
        def __init__(self, endpoint: str, **kwargs: object) -> None:
            del endpoint, kwargs
            self.capabilities = SimpleNamespace(protocol_version=1, memory_ingest=False)

        def close(self) -> None:
            pass

    client_module = importlib.import_module("zippy_client.client")
    monkeypatch.setattr(client_module, "FlightTransport", QueryOnlyFlight)
    connected = Client(
        "tcp://127.0.0.1:17690",
        transport=Transport.GATEWAY_LOCAL,
    )
    try:
        with pytest.raises(CapabilityError, match="selected Gateway endpoint"):
            connected.read_sql("SELECT * FROM ticks", bindings={"ticks": "ticks"})
    finally:
        connected.close()


def test_remote_zippy_uri_resolves_tcp_control_endpoint() -> None:
    endpoint = resolve_control_endpoint("zippy://192.0.2.10:17690/default")

    assert endpoint.kind == "tcp"
    assert endpoint.address == "192.0.2.10:17690"
    assert not endpoint.is_namespace_local


@pytest.mark.parametrize(
    ("stream", "expected"),
    [
        ({"data_path": "historical", "status": "ready"}, True),
        ({"data_path": "segment", "status": "writer_attached"}, False),
        (
            {
                "data_path": "segment",
                "status": "writer_attached",
                "active_segment_descriptor": {},
                "active_segment_preflight": {"status": "ok", "readable": True},
            },
            True,
        ),
        (
            {
                "data_path": "segment",
                "status": "writer_attached",
                "active_segment_descriptor": {},
                "active_segment_preflight": {"status": "error", "readable": False},
            },
            False,
        ),
    ],
)
def test_source_readiness_requires_a_readable_segment_descriptor(
    stream: dict[str, object],
    expected: bool,
) -> None:
    assert _source_ready(stream) is expected


@pytest.mark.skipif(os.name == "nt", reason="Unix logical endpoints use TCP on Windows")
def test_default_logical_uri_resolves_namespace_local_socket(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    endpoint = resolve_control_endpoint("zippy://default")

    assert endpoint.kind == "unix"
    assert endpoint.address == str(
        tmp_path / ".zippy" / "control_endpoints" / "default" / "master.sock"
    )
    assert endpoint.is_namespace_local


def test_auto_subscription_prefers_shm_only_with_namespace_proof() -> None:
    unix_router = TransportRouter(
        ControlEndpoint("unix", "/tmp/zippy.sock", "zippy://default"),
        "127.0.0.1:17666",
        lambda: True,
    )
    tcp_router = TransportRouter(
        ControlEndpoint("tcp", "127.0.0.1:17690", "zippy://127.0.0.1:17690/default"),
        "127.0.0.1:17666",
        lambda: True,
    )
    stream: dict[str, object] = {
        "data_path": "segment",
        "active_segment_descriptor": {},
    }

    assert unix_router.select(Operation.SUBSCRIBE, Transport.AUTO, stream_info=stream).selected == (
        Transport.SHM
    )
    assert tcp_router.select(Operation.SUBSCRIBE, Transport.AUTO, stream_info=stream).selected == (
        Transport.GATEWAY_LOCAL
    )


def test_queries_and_writes_never_select_shm() -> None:
    router = TransportRouter(
        ControlEndpoint("unix", "/tmp/zippy.sock", "zippy://default"),
        "127.0.0.1:17666",
        lambda: True,
    )

    assert router.select(Operation.QUERY, Transport.AUTO).selected == Transport.GATEWAY_LOCAL
    assert router.select(Operation.WRITE, Transport.AUTO).selected == Transport.GATEWAY_LOCAL
    with pytest.raises(TransportUnavailableError, match="only supports live subscriptions"):
        router.select(Operation.QUERY, Transport.SHM)


def test_subscription_options_can_require_gateway() -> None:
    router = TransportRouter(
        ControlEndpoint("unix", "/tmp/zippy.sock", "zippy://default"),
        "127.0.0.1:17666",
        lambda: True,
    )
    stream: dict[str, object] = {
        "data_path": "segment",
        "active_segment_descriptor": {},
    }

    decision = router.select(
        Operation.SUBSCRIBE,
        Transport.AUTO,
        stream_info=stream,
        allow_shm=False,
    )

    assert decision.selected == Transport.GATEWAY_LOCAL
    with pytest.raises(TransportUnavailableError, match="requires Gateway"):
        router.select(
            Operation.SUBSCRIBE,
            Transport.SHM,
            stream_info=stream,
            allow_shm=False,
        )
