"""Endpoint resolution and operation-aware routing tests."""

from __future__ import annotations

import importlib
import os
from types import SimpleNamespace

import pytest

from zippy_client.client import Client
from zippy_client.control import ControlClient
from zippy_client.endpoints import ControlEndpoint, resolve_control_endpoint
from zippy_client.errors import (
    CapabilityError,
    ProtocolError,
    ServerError,
    TransportUnavailableError,
    ZippyConnectionError,
)
from zippy_client.models import Operation, Transport
from zippy_client.router import TransportRouter


@pytest.mark.parametrize("flight_config", [None, {}, {"enabled": False, "endpoint": "127.0.0.1:1"}])
def test_gateway_only_discovery_is_rejected(monkeypatch, flight_config):
    config = {
        "gateway": {"enabled": True, "endpoint": "127.0.0.1:17666", "protocol_version": 99},
        "capabilities": {"query_sql": True, "subscribe_resume_cursor": True},
        "flight": flight_config,
    }
    monkeypatch.setattr(ControlClient, "get_config", lambda self: config)
    with pytest.raises(TransportUnavailableError, match="Flight endpoint"):
        Client("tcp://127.0.0.1:17690")


@pytest.mark.parametrize("operation", list(Operation))
@pytest.mark.parametrize("local", [True, False])
def test_every_operation_uses_only_native_flight(operation, local):
    host = "127.0.0.1" if local else "192.0.2.10"
    router = TransportRouter(ControlEndpoint("tcp", f"{host}:17690", "test"), f"{host}:17667", True)
    selected = Transport.FLIGHT_LOCAL if local else Transport.FLIGHT_REMOTE
    assert router.select(operation, Transport.AUTO).selected == selected
    assert router.select(operation, selected).selected == selected
    router.flight_endpoint = None
    with pytest.raises(TransportUnavailableError, match="Flight endpoint"):
        router.select(operation, Transport.AUTO)


def test_native_writer_schema_failure_is_not_hidden(flight_catalog_client):
    from zippy_client.errors import ServerError

    client = flight_catalog_client
    failure = ServerError("PERMISSION_DENIED", "schema access denied", retryable=False)
    client.flight.failure = failure
    with pytest.raises(ServerError) as caught:
        client.get_writer("ticks", namespace="market")
    assert caught.value is failure
    assert client.flight.schema_requests == [("market", "ticks")]


def test_native_writer_requires_ingest_capability(flight_catalog_client):
    client = flight_catalog_client
    client.router.flight_ingest_available = False
    with pytest.raises(TransportUnavailableError, match="authenticated ingest"):
        client.get_writer("ticks", namespace="market")
    assert not client.flight.schema_requests


def test_flight_credentials_override_environment_without_gateway_settings(monkeypatch):
    client_module = importlib.import_module("zippy_client.client")
    monkeypatch.setenv("ZIPPY_GATEWAY_TOKEN", "removed-token")
    monkeypatch.setenv("ZIPPY_FLIGHT_AUTH_TOKEN", "environment-read")
    monkeypatch.setenv("ZIPPY_FLIGHT_AUTH_WRITER_TOKEN", "environment-writer")
    monkeypatch.setenv("ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN", "environment-admin")
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "flight": {"enabled": True, "endpoint": "127.0.0.1:17667"},
            "gateway": {"endpoint": "invalid address", "protocol_version": "removed"},
            "capabilities": {"subscribe_resume_cursor": False},
        },
    )
    captured = {}

    class Flight:
        capabilities = SimpleNamespace(
            protocol_version=1,
            memory_ingest=False,
            wal_ingest=True,
            persisted_ingest=False,
            query=True,
            names={"subscription.log_fragments.v1"},
        )

        def __init__(self, endpoint, **kwargs):
            captured.update(kwargs)

        def close(self):
            captured["closed"] = True

    monkeypatch.setattr(client_module, "FlightTransport", Flight)
    with Client(
        "tcp://127.0.0.1:17690",
        flight_token="explicit-read",
        flight_writer_token="explicit-writer",
        flight_admin_token="explicit-admin",
    ) as client:
        assert client.capabilities.flight_ingest
        assert client.capabilities.resume_cursor
        assert not hasattr(client, "gateway")
    assert captured["read_token"] == "explicit-read"
    assert captured["writer_token"] == "explicit-writer"
    assert captured["admin_token"] == "explicit-admin"
    assert captured["closed"] is True


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
            self.capabilities = SimpleNamespace(
                protocol_version=1,
                memory_ingest=True,
                wal_ingest=True,
                persisted_ingest=True,
                query=True,
                names=frozenset({"subscription.log_fragments.v1"}),
            )

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


def test_remote_zippy_uri_resolves_tcp_control_endpoint() -> None:
    endpoint = resolve_control_endpoint("zippy://192.0.2.10:17690/default")

    assert endpoint.kind == "tcp"
    assert endpoint.address == "192.0.2.10:17690"
    assert not endpoint.is_namespace_local


@pytest.mark.parametrize(
    "uri",
    [
        "zippy://192.0.2.10:17690/other",
        "zippy://192.0.2.10:17690/",
        "zippy://192.0.2.10:17690/default?mode=other",
    ],
)
def test_remote_zippy_uri_rejects_ignored_profile(uri: str) -> None:
    with pytest.raises(ValueError, match="only /default profile"):
        resolve_control_endpoint(uri)


@pytest.mark.parametrize("name", ["ZIPPY_MASTER_URI", "ZIPPY_MASTER_ENDPOINT"])
@pytest.mark.parametrize("uri", [None, "tcp://127.0.0.1:17690"])
def test_removed_control_endpoint_environment_aliases_fail(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    uri: str | None,
) -> None:
    monkeypatch.setenv(name, "tcp://192.0.2.10:17690")
    monkeypatch.setenv("ZIPPY_CONTROL_ENDPOINT", "tcp://127.0.0.1:17690")

    with pytest.raises(ValueError, match=f"{name}.*ZIPPY_CONTROL_ENDPOINT"):
        resolve_control_endpoint(uri)


def test_control_endpoint_environment_variable_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ZIPPY_MASTER_URI", raising=False)
    monkeypatch.delenv("ZIPPY_MASTER_ENDPOINT", raising=False)
    monkeypatch.setenv("ZIPPY_CONTROL_ENDPOINT", "tcp://127.0.0.1:17690")

    endpoint = resolve_control_endpoint()

    assert endpoint.kind == "tcp"
    assert endpoint.address == "127.0.0.1:17690"


@pytest.mark.parametrize(
    "uri",
    [
        "tcp://127.0.0.1:17690/other",
        "tcp://user@127.0.0.1:17690",
        "tcp://127.0.0.1:17690?profile=other",
        "tcp://127.0.0.1:17690#other",
    ],
)
def test_tcp_control_uri_rejects_discarded_components(uri: str) -> None:
    with pytest.raises(ValueError, match="TCP endpoint"):
        resolve_control_endpoint(uri)


def test_tcp_control_uri_does_not_echo_rejected_credentials() -> None:
    with pytest.raises(ValueError, match="TCP endpoint") as caught:
        resolve_control_endpoint("tcp://user:secret@127.0.0.1:17690")

    assert "secret" not in str(caught.value)


def test_tcp_control_uri_keeps_ipv6_address() -> None:
    endpoint = resolve_control_endpoint("tcp://[::1]:17690")

    assert endpoint.kind == "tcp"
    assert endpoint.address == "[::1]:17690"


@pytest.mark.parametrize(
    "uri",
    [
        "unix:///tmp/master.sock",
        "file:///tmp/master.sock",
        "/tmp/master.sock",
        r"C:\zippy\master.sock",
    ],
)
def test_non_unix_platform_rejects_unix_control_paths(
    monkeypatch: pytest.MonkeyPatch,
    uri: str,
) -> None:
    endpoint_module = importlib.import_module("zippy_client.endpoints")
    with monkeypatch.context() as patch:
        patch.setattr(endpoint_module.os, "name", "nt")
        with pytest.raises(ValueError, match="Unix control endpoint is not supported"):
            resolve_control_endpoint(uri)


def test_non_unix_platform_uses_tcp_for_default_logical_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint_module = importlib.import_module("zippy_client.endpoints")
    with monkeypatch.context() as patch:
        patch.setattr(endpoint_module.os, "name", "nt")
        endpoint = resolve_control_endpoint("zippy://default")

    assert endpoint.kind == "tcp"
    assert endpoint.address == "127.0.0.1:17690"


@pytest.mark.parametrize("uri", ["zippy://other", "zippy://default/other"])
def test_non_unix_platform_rejects_unknown_logical_name(
    monkeypatch: pytest.MonkeyPatch,
    uri: str,
) -> None:
    endpoint_module = importlib.import_module("zippy_client.endpoints")
    with monkeypatch.context() as patch:
        patch.setattr(endpoint_module.os, "name", "nt")
        with pytest.raises(ValueError, match="named control endpoint is not supported"):
            resolve_control_endpoint(uri)


def test_non_unix_platform_keeps_bare_tcp_address(monkeypatch: pytest.MonkeyPatch) -> None:
    endpoint_module = importlib.import_module("zippy_client.endpoints")
    with monkeypatch.context() as patch:
        patch.setattr(endpoint_module.os, "name", "nt")
        endpoint = resolve_control_endpoint("127.0.0.1:17777")

    assert endpoint.kind == "tcp"
    assert endpoint.address == "127.0.0.1:17777"


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


@pytest.mark.parametrize("control_kind", ["unix", "tcp"])
def test_auto_subscription_requires_native_flight_for_local_and_remote_control(
    control_kind: str,
) -> None:
    router = TransportRouter(
        ControlEndpoint(
            control_kind, "/tmp/zippy.sock" if control_kind == "unix" else "127.0.0.1:1", "test"
        ),
        flight_endpoint="127.0.0.1:17667",
    )
    assert router.select(Operation.SUBSCRIBE, Transport.AUTO).selected == Transport.FLIGHT_LOCAL
    router.flight_endpoint = None
    with pytest.raises(TransportUnavailableError, match="Flight endpoint"):
        router.select(Operation.SUBSCRIBE, Transport.AUTO)


@pytest.mark.parametrize("requested", ["gateway_local", "gateway_remote", "shm"])
def test_subscription_rejects_removed_transport_choices(requested: str) -> None:
    router = TransportRouter(
        ControlEndpoint("unix", "/tmp/zippy.sock", "zippy://default"),
        flight_endpoint="127.0.0.1:17667",
    )
    with pytest.raises(ValueError, match="unsupported transport"):
        router.select(Operation.SUBSCRIBE, requested)


@pytest.mark.parametrize("error_type", [ProtocolError, CapabilityError, ZippyConnectionError])
def test_configured_flight_failure_never_falls_back_to_gateway(
    monkeypatch: pytest.MonkeyPatch, error_type: type[Exception]
) -> None:
    client_module = importlib.import_module("zippy_client.client")
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {"enabled": True, "endpoint": "127.0.0.1:17666"},
            "flight": {"enabled": True, "endpoint": "127.0.0.1:17667"},
        },
    )
    failure = error_type("configured Flight is unavailable")

    def fail_flight(*args: object, **kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(client_module, "FlightTransport", fail_flight)
    with pytest.raises(error_type) as captured:
        Client("tcp://127.0.0.1:17690")
    assert captured.value is failure


@pytest.fixture
def flight_catalog_client(monkeypatch):
    import pyarrow as pa
    from zippy_client.errors import ServerError

    class CatalogFlight:
        capabilities = SimpleNamespace(
            protocol_version=1,
            memory_ingest=True,
            wal_ingest=True,
            persisted_ingest=True,
            query=True,
            names=frozenset({"subscription.log_fragments.v1"}),
        )
        endpoint = "127.0.0.1:17667"

        def __init__(self, *args, **kwargs):
            self.schema_requests = []
            self.schema_deadlines = []
            self.failure = None

        def create_snapshot(self, bindings, **kwargs):
            raise CapabilityError("Flight does not advertise native fixed cross-query snapshots")

        def list_tables(self, *, namespace=None):
            rows = [
                {"name": "ticks", "namespace": "market", "catalog": "zippy", "table_type": "TABLE"}
            ]
            return [row for row in rows if namespace is None or namespace == row["namespace"]]

        def get_table_schema(self, table, *, namespace="default", **kwargs):
            self.schema_requests.append((namespace, table))
            self.schema_deadlines.append(kwargs.get("deadline_ns"))
            if self.failure:
                raise self.failure
            if (namespace, table) != ("market", "ticks"):
                raise ServerError("NOT_FOUND", "table missing", retryable=False)
            return pa.schema([("value", pa.int64())])

        def close(self):
            pass

    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "gateway": {"enabled": True, "endpoint": "127.0.0.1:17666"},
            "flight": {"enabled": True, "endpoint": "127.0.0.1:17667"},
            "capabilities": {"query_sql": True},
        },
    )

    assert not hasattr(ControlClient, "get_stream")
    assert not hasattr(ControlClient, "list_streams")
    monkeypatch.setattr(
        importlib.import_module("zippy_client.client"), "FlightTransport", CatalogFlight
    )
    with Client("tcp://127.0.0.1:17690") as connected:
        yield connected


def test_flight_list_wait_and_table_info_share_catalog(flight_catalog_client):
    client = flight_catalog_client
    assert client.list_tables() == client.flight.list_tables()
    table = client.read_table("ticks", namespace="market", wait=True, timeout=0.1)
    assert table.info()["namespace"] == "market"
    assert table.schema().names == ["value"]
    assert client.flight.schema_requests == [("market", "ticks"), ("market", "ticks")]
    assert isinstance(client.flight.schema_deadlines[0], int)
    assert client.flight.schema_deadlines[0] > 0
    assert client.flight.schema_deadlines[1] is None
    with pytest.raises(TimeoutError, match="table did not become ready"):
        client.read_table("ticks", namespace="missing", wait=True, timeout=0)


def test_flight_wait_does_not_hide_authentication_or_capability_errors(flight_catalog_client):
    from zippy_client.errors import ServerError

    client = flight_catalog_client
    failure = ServerError("UNAUTHORIZED", "wrong role", retryable=False)
    client.flight.failure = failure
    with pytest.raises(ServerError) as captured:
        client.read_table("ticks", namespace="market", wait=True)
    assert captured.value is failure


@pytest.mark.parametrize("timeout", [None, 0.1])
def test_flight_wait_preserves_unbounded_deadline_error_and_bounds_requested_wait(
    flight_catalog_client, timeout
):
    failure = ServerError("DEADLINE_EXCEEDED", "query expired before submission", retryable=False)
    client = flight_catalog_client
    client.flight.failure = failure
    expected = ServerError if timeout is None else TimeoutError
    with pytest.raises(expected) as captured:
        client.read_table("ticks", namespace="market", wait=True, timeout=timeout)
    if timeout is None:
        assert captured.value is failure
    else:
        assert captured.value.__cause__ is failure
        assert "table did not become ready" in str(captured.value)


def test_flight_fixed_snapshot_and_implicit_sql_fallback_are_rejected(flight_catalog_client):
    client = flight_catalog_client
    with pytest.raises(CapabilityError, match="fixed cross-query snapshot"):
        client.read_table("ticks", snapshot=True)
    with client.read_table("ticks") as table:
        with pytest.raises(CapabilityError, match="fixed cross-query snapshot"):
            table.snapshot()
    with pytest.raises(CapabilityError, match="explicit bindings"):
        client.read_sql("SELECT * FROM ticks")


@pytest.mark.parametrize(
    ("payload", "expected_type", "expected_code"),
    [
        (
            {"code": "NOT_FOUND", "retryable": False, "reason": "authorization text is irrelevant"},
            "server",
            "NOT_FOUND",
        ),
        ({"code": "UNAVAILABLE", "retryable": True, "reason": "busy"}, "server", "UNAVAILABLE"),
        ({"code": "UNAUTHENTICATED", "retryable": False, "reason": "denied"}, "auth", None),
        ({"code": "PERMISSION_DENIED", "retryable": False, "reason": "denied"}, "auth", None),
        ({"reason": "old response"}, "protocol", None),
        ({"code": "NOT_FOUND", "retryable": "false", "reason": "bad flag"}, "protocol", None),
        ({"code": "NOT_FOUND", "retryable": False}, "protocol", None),
        ("old response", "protocol", None),
    ],
)
def test_control_errors_require_structured_code_and_retry_policy(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, object] | str,
    expected_type: str,
    expected_code: str | None,
) -> None:
    import json
    from zippy_client.errors import AuthenticationError, ServerError

    class ResponseSocket:
        def __init__(self) -> None:
            self.data = json.dumps({"Error": payload}).encode() + b"\n"

        def __enter__(self) -> ResponseSocket:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def sendall(self, data: bytes) -> None:
            assert b"GetConfig" in data

        def recv(self, maximum: int) -> bytes:
            data, self.data = self.data[:maximum], self.data[maximum:]
            return data

    monkeypatch.setattr(ControlClient, "_connect", lambda self: ResponseSocket())
    client = ControlClient(ControlEndpoint("tcp", "127.0.0.1:17690", "zippy://default"))
    error_type = {"server": ServerError, "auth": AuthenticationError, "protocol": ProtocolError}[
        expected_type
    ]
    with pytest.raises(error_type) as caught:
        client.get_config()
    if expected_code is not None:
        assert isinstance(caught.value, ServerError)
        assert isinstance(payload, dict)
        assert caught.value.code == expected_code
        assert caught.value.retryable == payload["retryable"]
