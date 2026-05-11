from __future__ import annotations

import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import zippy


def test_resolve_uri_passes_through_tcp_endpoint() -> None:
    assert zippy._resolve_uri("tcp://127.0.0.1:17690") == "tcp://127.0.0.1:17690"


def test_resolve_uri_maps_remote_zippy_uri_to_tcp_master_endpoint() -> None:
    assert zippy._resolve_uri("zippy://127.0.0.1:17690/default") == "tcp://127.0.0.1:17690"


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


def test_connect_app_unhandled_exception_exits_without_native_fatal() -> None:
    uri = f"tcp://{_unused_loopback_addr()}"
    master_process = subprocess.Popen(
        [sys.executable, "-m", "zippy", "master", "run", uri],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_tcp_uri(uri, master_process)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import time\n"
                    "import zippy\n"
                    f"zippy.connect({uri!r}, app='pytest_unhandled_exit')\n"
                    "time.sleep(0.2)\n"
                    "raise RuntimeError('intentional subprocess failure')\n"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    finally:
        _stop_process(master_process)

    assert result.returncode != 0
    assert "intentional subprocess failure" in result.stderr
    assert "FATAL: exception not rethrown" not in result.stderr


def test_process_exit_cleanup_stops_default_control_agent(monkeypatch) -> None:
    events: list[str] = []

    class FakeControlAgent:
        def stop(self) -> None:
            events.append("stop")

    monkeypatch.setattr(zippy, "_DEFAULT_CONTROL_AGENT", FakeControlAgent())

    zippy._cleanup_default_control_agent_at_exit()

    assert events == ["stop"]
    assert zippy._DEFAULT_CONTROL_AGENT is None


def _unused_loopback_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


def _unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_tcp_uri(uri: str, process: subprocess.Popen[str]) -> None:
    host, port_text = uri.removeprefix("tcp://").split(":")
    deadline = time.monotonic() + 5.0
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1.0)
            raise RuntimeError(
                "master process exited before ready "
                f"returncode=[{process.returncode}] stdout=[{stdout}] stderr=[{stderr}]"
            )
        try:
            with socket.create_connection((host, int(port_text)), timeout=0.1):
                return
        except OSError as error:
            last_error = error
            time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for master uri=[{uri}] error=[{last_error}]")


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=5.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=5.0)
