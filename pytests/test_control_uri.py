from __future__ import annotations

import os
from pathlib import Path
import socket
import tempfile

import zippy


def test_resolve_uri_passes_through_tcp_endpoint() -> None:
    assert zippy._resolve_uri("tcp://127.0.0.1:17690") == "tcp://127.0.0.1:17690"


def test_resolve_uri_maps_remote_zippy_uri_to_tcp_master_endpoint() -> None:
    assert (
        zippy._resolve_uri("zippy://127.0.0.1:17690/default")
        == "tcp://127.0.0.1:17690"
    )


def test_resolve_uri_maps_localhost_zippy_uri_to_tcp_master_endpoint() -> None:
    assert zippy._resolve_uri("zippy://localhost:17690") == "tcp://localhost:17690"


def test_resolve_uri_rejects_legacy_remote_gateway_uri() -> None:
    try:
        zippy._resolve_uri("zippy+tcp://127.0.0.1:17666/default")
    except ValueError as error:
        assert "zippy://host:port/profile" in str(error)
    else:
        raise AssertionError("legacy zippy+tcp uri should be rejected")


def test_home_dir_prefers_userprofile_when_home_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("USERPROFILE", r"C:\Users\quant")

    assert str(zippy._home_dir()) == r"C:\Users\quant"


def test_home_dir_falls_back_to_homedrive_homepath(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    monkeypatch.setenv("HOMEDRIVE", "C:")
    monkeypatch.setenv("HOMEPATH", r"\Users\quant")

    assert str(zippy._home_dir()) == r"C:\Users\quant"


def test_home_dir_falls_back_to_temp_dir(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    monkeypatch.delenv("HOMEDRIVE", raising=False)
    monkeypatch.delenv("HOMEPATH", raising=False)

    assert zippy._home_dir() == Path(tempfile.gettempdir())


def test_logical_uri_remains_platform_default() -> None:
    resolved = zippy._resolve_uri("default")
    explicit = zippy._resolve_uri("zippy://default")

    if os.name == "nt":
        assert resolved == "zippy://default"
        assert explicit == "zippy://default"
    else:
        assert resolved.endswith("/.zippy/control_endpoints/default/master.sock")
        assert explicit.endswith("/.zippy/control_endpoints/default/master.sock")


def test_windows_style_paths_are_not_treated_as_logical_names() -> None:
    assert zippy._resolve_uri(r"C:\zippy\master.sock") == r"C:\zippy\master.sock"
    assert zippy._resolve_uri(r"~\zippy\master.sock").endswith(r"zippy\master.sock")


def test_native_master_client_roundtrips_tcp_uri() -> None:
    uri = f"tcp://{_unused_loopback_addr()}"
    server = zippy.MasterServer(uri=uri)
    assert server.control_endpoint() == uri
    server.start()
    try:
        host, port = uri.removeprefix("tcp://").split(":")
        with socket.create_connection((host, int(port)), timeout=1.0):
            pass
        client = zippy.MasterClient(uri=uri)
        assert client.control_endpoint() == uri
        process_id = client.register_process("pytest_tcp")

        assert process_id.startswith("proc_")
    finally:
        server.stop()
        server.join()


def test_native_master_client_roundtrips_remote_zippy_uri() -> None:
    addr = _unused_loopback_addr()
    uri = f"zippy://{addr}/default"
    server = zippy.MasterServer(uri=uri)
    assert server.control_endpoint() == f"tcp://{addr}"
    server.start()
    try:
        client = zippy.MasterClient(uri=uri)
        assert client.control_endpoint() == f"tcp://{addr}"
        assert client.register_process("pytest_remote_zippy").startswith("proc_")
    finally:
        server.stop()
        server.join()


def test_native_master_client_roundtrips_localhost_remote_zippy_uri() -> None:
    port = _unused_loopback_port()
    uri = f"zippy://localhost:{port}"
    server = zippy.MasterServer(uri=uri)
    assert server.control_endpoint() == f"tcp://127.0.0.1:{port}"
    server.start()
    try:
        client = zippy.MasterClient(uri=uri)
        assert client.control_endpoint() == f"tcp://127.0.0.1:{port}"
        assert client.register_process("pytest_localhost_zippy").startswith("proc_")
    finally:
        server.stop()
        server.join()


def _unused_loopback_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


def _unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
