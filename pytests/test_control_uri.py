"""CLI delegates every endpoint normalization rule to the independent client."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile

from zippy.cli_common import resolve_control_endpoint
from zippy_client.endpoints import _home_dir


def test_resolve_uri_passes_through_tcp_endpoint() -> None:
    assert resolve_control_endpoint("tcp://127.0.0.1:17690") == "tcp://127.0.0.1:17690"


def test_resolve_uri_maps_remote_zippy_uri_to_tcp_master_endpoint() -> None:
    assert resolve_control_endpoint("zippy://127.0.0.1:17690/default") == "tcp://127.0.0.1:17690"


def test_resolve_uri_maps_localhost_zippy_uri_to_tcp_master_endpoint() -> None:
    assert resolve_control_endpoint("zippy://localhost:17690") == "tcp://localhost:17690"


def test_resolve_uri_rejects_legacy_remote_gateway_uri() -> None:
    try:
        resolve_control_endpoint("zippy+tcp://127.0.0.1:17666/default")
    except ValueError as error:
        assert "zippy://host:port/default" in str(error)
    else:
        raise AssertionError("legacy zippy+tcp uri should be rejected")


def test_home_dir_prefers_userprofile_when_home_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("USERPROFILE", r"C:\Users\quant")

    assert str(_home_dir()) == r"C:\Users\quant"


def test_home_dir_falls_back_to_homedrive_homepath(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    monkeypatch.setenv("HOMEDRIVE", "C:")
    monkeypatch.setenv("HOMEPATH", r"\Users\quant")

    assert str(_home_dir()) == r"C:\Users\quant"


def test_home_dir_falls_back_to_temp_dir(monkeypatch) -> None:
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("USERPROFILE", raising=False)
    monkeypatch.delenv("HOMEDRIVE", raising=False)
    monkeypatch.delenv("HOMEPATH", raising=False)

    assert _home_dir() == Path(tempfile.gettempdir())


def test_logical_uri_remains_platform_default() -> None:
    resolved = resolve_control_endpoint("default")
    explicit = resolve_control_endpoint("zippy://default")

    if os.name == "nt":
        assert resolved == "tcp://127.0.0.1:17690"
        assert explicit == "tcp://127.0.0.1:17690"
    else:
        assert resolved.endswith("/.zippy/control_endpoints/default/master.sock")
        assert explicit.endswith("/.zippy/control_endpoints/default/master.sock")


def test_windows_style_paths_are_not_treated_as_logical_names() -> None:
    assert resolve_control_endpoint(r"C:\zippy\master.sock") == r"C:\zippy\master.sock"
    assert resolve_control_endpoint(r"~\zippy\master.sock").endswith(r"zippy\master.sock")
