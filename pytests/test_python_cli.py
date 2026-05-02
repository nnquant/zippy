from __future__ import annotations

from click.testing import CliRunner
import signal
import subprocess
import pytest
import pyarrow as pa
from pathlib import Path
import sys
import time

import zippy
from zippy.cli import main
from zippy.cli_common import DEFAULT_CONTROL_ENDPOINT, ensure_control_parent_dir


def start_master_server(tmp_path: Path) -> tuple[zippy.MasterServer, str]:
    control_endpoint = str(tmp_path / "zippy-master-cli.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()
    return server, control_endpoint


def test_master_run_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "--help"])
    assert result.exit_code == 0
    assert "Run the local zippy-master daemon." in result.output
    assert DEFAULT_CONTROL_ENDPOINT in result.output
    assert "URI" in result.output


def test_cli_root_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "master" in result.output
    assert "stream" in result.output


def test_master_run_returns_click_error_when_server_start_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_run_master_daemon(control_endpoint: str) -> None:
        assert control_endpoint
        raise RuntimeError("failed to bind control socket")

    class UnexpectedMasterServer:
        def __init__(self, control_endpoint: str) -> None:
            raise AssertionError(
                f"master run must not construct MasterServer control_endpoint=[{control_endpoint}]"
            )

    monkeypatch.setattr(zippy, "run_master_daemon", failing_run_master_daemon, raising=False)
    monkeypatch.setattr(zippy, "MasterServer", UnexpectedMasterServer)

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "/tmp/zippy-master-fail.sock"])

    assert result.exit_code != 0
    assert "failed to bind control socket" in result.output


def test_master_run_wraps_control_parent_creation_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_ensure_control_parent_dir(control_endpoint: str) -> None:
        raise PermissionError(f"cannot create control socket directory path=[{control_endpoint}]")

    monkeypatch.setattr(
        "zippy.cli_master.ensure_control_parent_dir",
        failing_ensure_control_parent_dir,
    )

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "/root/forbidden/master.sock"])

    assert result.exit_code != 0
    assert "cannot create control socket directory" in result.output
    assert "Traceback" not in result.output


def test_master_run_uses_expanded_default_control_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    def recording_run_master_daemon(control_endpoint: str) -> None:
        events.append(("run_master_daemon", control_endpoint))

    class UnexpectedMasterServer:
        def __init__(self, control_endpoint: str) -> None:
            raise AssertionError(
                f"master run must not construct MasterServer control_endpoint=[{control_endpoint}]"
            )

    fake_home = Path("/tmp/zippy-cli-home")
    monkeypatch.setattr(zippy, "run_master_daemon", recording_run_master_daemon, raising=False)
    monkeypatch.setattr(zippy, "MasterServer", UnexpectedMasterServer)
    monkeypatch.setenv("HOME", str(fake_home))

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run"])

    expected = str(fake_home / ".zippy" / "control_endpoints" / "default" / "master.sock")
    assert result.exit_code == 0
    assert events == [("run_master_daemon", expected)]
    assert result.output == ""
    assert "keyboard interrupt" not in result.output.lower()


def test_master_run_accepts_named_logical_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    def recording_run_master_daemon(control_endpoint: str) -> None:
        events.append(("run_master_daemon", control_endpoint))

    fake_home = Path("/tmp/zippy-cli-home")
    monkeypatch.setattr(zippy, "run_master_daemon", recording_run_master_daemon, raising=False)
    monkeypatch.setenv("HOME", str(fake_home))

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "sim"])

    expected = str(fake_home / ".zippy" / "control_endpoints" / "sim" / "master.sock")
    assert result.exit_code == 0
    assert events == [("run_master_daemon", expected)]
    assert result.output == ""


def test_master_run_accepts_explicit_control_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    def recording_run_master_daemon(control_endpoint: str) -> None:
        events.append(("run_master_daemon", control_endpoint))

    class UnexpectedMasterServer:
        def __init__(self, control_endpoint: str) -> None:
            raise AssertionError(
                f"master run must not construct MasterServer control_endpoint=[{control_endpoint}]"
            )

    monkeypatch.setattr(zippy, "run_master_daemon", recording_run_master_daemon, raising=False)
    monkeypatch.setattr(zippy, "MasterServer", UnexpectedMasterServer)

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "/tmp/custom-master.sock"])

    assert result.exit_code == 0
    assert events == [("run_master_daemon", "/tmp/custom-master.sock")]
    assert result.output == ""
    assert "keyboard interrupt" not in result.output.lower()


def test_control_parent_dir_skips_tcp_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    ensure_control_parent_dir("tcp://127.0.0.1:17690")

    assert not (tmp_path / "tcp:").exists()


def test_master_run_accepts_tcp_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    def recording_run_master_daemon(control_endpoint: str) -> None:
        events.append(control_endpoint)

    monkeypatch.setattr(zippy, "run_master_daemon", recording_run_master_daemon, raising=False)

    runner = CliRunner()
    result = runner.invoke(main, ["master", "run", "tcp://127.0.0.1:17690"])

    assert result.exit_code == 0
    assert events == ["tcp://127.0.0.1:17690"]


def test_master_run_forwards_config_path(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    def recording_run_master_daemon(control_endpoint: str, *, config: str | None = None) -> None:
        events.append((control_endpoint, config or ""))

    monkeypatch.setattr(zippy, "run_master_daemon", recording_run_master_daemon, raising=False)

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["master", "run", "/tmp/custom-master.sock", "--config", "/tmp/zippy-config.toml"],
    )

    assert result.exit_code == 0
    assert events == [("/tmp/custom-master.sock", "/tmp/zippy-config.toml")]


def test_master_run_exits_cleanly_on_sigint(tmp_path: Path) -> None:
    control_endpoint = tmp_path / "zippy-master-cli-signal.sock"
    owner_path = Path(f"{control_endpoint}.owner")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "zippy.cli",
            "master",
            "run",
            str(control_endpoint),
        ],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            if control_endpoint.exists():
                break
            if process.poll() is not None:
                break
            time.sleep(0.05)

        assert control_endpoint.exists(), "master socket was not created before SIGINT"

        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0
        assert not control_endpoint.exists()
        assert not owner_path.exists()
        assert "master_shutdown_requested" in stdout
        assert "master_stopped" in stdout
        assert "Aborted!" not in stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_stream_ls_lists_registered_streams(tmp_path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream(
        "openctp_ticks",
        pa.schema([("instrument_id", pa.string())]),
        64,
        4096,
    )

    runner = CliRunner()
    result = runner.invoke(main, ["stream", "ls", "--uri", control_endpoint])

    assert result.exit_code == 0
    assert "openctp_ticks" in result.output
    assert "BUFFER SIZE" in result.output
    assert "FRAME SIZE" in result.output
    assert "64" in result.output
    assert "4096" in result.output
    assert "ring_capacity" not in result.output

    server.stop()
    server.join()


def test_stream_show_returns_single_stream(tmp_path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream(
        "openctp_ticks",
        pa.schema([("instrument_id", pa.string())]),
        64,
        4096,
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["stream", "show", "openctp_ticks", "--uri", control_endpoint],
    )

    assert result.exit_code == 0
    assert "stream_name: openctp_ticks" in result.output
    assert "buffer_size: 64" in result.output
    assert "frame_size: 4096" in result.output
    assert "ring_capacity" not in result.output
    assert "status: registered" in result.output

    server.stop()
    server.join()
