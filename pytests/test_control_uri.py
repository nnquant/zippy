from __future__ import annotations

import os
from pathlib import Path
import socket
import tempfile

import zippy


def test_resolve_uri_passes_through_tcp_endpoint() -> None:
    assert zippy._resolve_uri("tcp://127.0.0.1:17690") == "tcp://127.0.0.1:17690"


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


def _unused_loopback_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"
