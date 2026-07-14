from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time

from click.testing import CliRunner
import pytest

import zippy
from zippy.cli import main
import zippy.cli_server as cli_server
import zippy.server_process as server_process
from zippy.cli_server import _resolve_server_inputs
from zippy.server_process import (
    _ServerProcessError,
    _acquire_launch_lock,
    _format_server_summary,
    _instance_key,
    _master_get_config,
    _server_paths,
    _server_status,
    _stop_server,
    _validate_inherited_lock,
    _validate_ready_record,
)


def _unused_gateway_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        host, port = listener.getsockname()
    return f"{host}:{port}"


def _valid_ready_record() -> dict[str, object]:
    return {
        "schema_version": 2,
        "status": "ready",
        "health": {
            "readiness": {
                "admission_open": True,
                "master_listener_ready": True,
                "gateway_configured": True,
                "gateway_listener_ready": True,
                "gateway_master_reachable": True,
                "native_storage_configured": True,
                "native_catalog_ready": True,
                "native_current_snapshot_files_ready": True,
                "storage_reconciliation_complete": True,
                "commit_recovery_complete": True,
            },
            "services": {
                "accepting_work": True,
                "admission_open": True,
                "catalog": "degraded",
                "query": "degraded",
                "master_lifecycle_running": True,
                "gateway_facade_available": True,
                "end_to_end_liveness": True,
            },
        },
    }


def test_server_instance_paths_are_endpoint_scoped(tmp_path: Path) -> None:
    first = _server_paths(
        "tcp://127.0.0.1:17690",
        run_dir=tmp_path / "run",
        log_dir=tmp_path / "logs",
    )
    second = _server_paths(
        "tcp://127.0.0.1:17691",
        run_dir=tmp_path / "run",
        log_dir=tmp_path / "logs",
    )

    assert first.state_file != second.state_file
    assert first.lock_file != second.lock_file
    assert _instance_key("tcp://127.0.0.1:17690") in first.state_file.name
    assert first.state_file.parent == (tmp_path / "run").resolve()
    assert first.bootstrap_log.parent == (tmp_path / "logs").resolve()

    command = server_process._child_command(
        "tcp://127.0.0.1:17690",
        config_path=None,
        paths=first,
        launch_nonce="launch-1",
        startup_timeout_sec=37.5,
    )
    timeout_index = command.index("--startup-timeout-sec")
    assert command[timeout_index + 1] == "37.5"


def test_server_startup_summary_reports_health_logs_and_unmanaged_webui() -> None:
    payload: dict[str, object] = {
        "status": "healthy",
        "version": "0.5.0.dev1+g1234567",
        "pid": 4321,
        "mode": "catalog_only",
        "startup_elapsed_ms": 15.2,
        "requested_control_endpoint": "tcp://127.0.0.1:17690",
        "control_endpoint": "tcp://127.0.0.1:17690",
        "master": {
            "status": "healthy",
            "endpoint": "tcp://127.0.0.1:17690",
        },
        "gateway": {"enabled": False, "status": "disabled"},
        "webui": {
            "managed": False,
            "status": "not_running",
            "url": "http://127.0.0.1:17688/",
        },
        "bootstrap_log": "/tmp/server.log",
        "native_log": "/tmp/native.jsonl",
        "state_file": "/tmp/server.json",
        "run_dir": "/tmp/custom-run",
    }

    summary = _format_server_summary(payload, action="run")

    assert "zippy server started" in summary
    assert "version: 0.5.0.dev1+g1234567" in summary
    assert "health: healthy" in summary
    assert "master: healthy" in summary
    assert "gateway: disabled" in summary
    assert "webui: not running managed=[no] url=[http://127.0.0.1:17688/]" in summary
    assert "native log: /tmp/native.jsonl" in summary
    assert "--run-dir /tmp/custom-run" in summary


def test_stopped_server_summary_does_not_render_none_endpoint() -> None:
    summary = _format_server_summary(
        {
            "status": "stopped",
            "requested_control_endpoint": "unix:///tmp/zippy-master.sock",
            "state_file": "/tmp/server.json",
        },
        action="status",
    )

    assert "master: not probed" in summary
    assert "endpoint=[None]" not in summary


def test_server_run_defaults_to_detached_launcher(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: dict[str, object] = {}

    def fake_launch(control_endpoint: str, **kwargs: object) -> dict[str, object]:
        observed["control_endpoint"] = control_endpoint
        observed.update(kwargs)
        return {
            "status": "healthy",
            "version": "0.5.0",
            "pid": 4321,
            "mode": "catalog_only",
            "requested_control_endpoint": control_endpoint,
            "control_endpoint": control_endpoint,
            "master": {"status": "healthy", "endpoint": control_endpoint},
            "gateway": {"enabled": False, "status": "disabled"},
            "webui": {
                "managed": False,
                "status": "not_running",
                "url": "http://127.0.0.1:17688/",
            },
            "state_file": str(tmp_path / "server.json"),
        }

    monkeypatch.setattr(cli_server, "_launch_server", fake_launch)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "server",
            "run",
            "tcp://127.0.0.1:27690",
            "--run-dir",
            str(tmp_path / "run"),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
    )

    assert result.exit_code == 0
    assert observed["control_endpoint"] == "tcp://127.0.0.1:27690"
    assert observed["run_dir"] == tmp_path / "run"
    assert "zippy server started" in result.output
    assert "pid: 4321" in result.output


def test_detached_child_forwards_remaining_startup_budget_to_native(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: dict[str, object] = {}

    def recording_native_runner(control_endpoint: str, **kwargs: object) -> None:
        observed["control_endpoint"] = control_endpoint
        observed.update(kwargs)

    monkeypatch.setattr(zippy, "run_server_daemon", recording_native_runner)
    monkeypatch.setattr(cli_server, "_validate_inherited_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cli_server,
        "_validate_child_starting_state",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(cli_server, "_cleanup_child_state_from_environment", lambda: None)
    monkeypatch.setenv(
        "ZIPPY_SERVER_STARTUP_DEADLINE_MONOTONIC",
        str(time.monotonic() + 5.0),
    )

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "server",
            "run",
            str(tmp_path / "server.sock"),
            "--foreground",
            "--ready-file",
            str(tmp_path / "server.json"),
            "--launch-nonce",
            "launch-1",
            "--startup-timeout-sec",
            "30",
        ],
    )

    assert result.exit_code == 0, result.output
    assert observed["control_endpoint"] == str(tmp_path / "server.sock")
    remaining = observed["startup_timeout_sec"]
    assert isinstance(remaining, float)
    assert 0.0 < remaining <= 5.0


def test_detached_child_preserves_stable_native_error_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class NativeFailure(RuntimeError):
        pass

    def failing_native_runner(*_args: object, **_kwargs: object) -> None:
        error = NativeFailure("native storage failed")
        setattr(error, "zippy_error_code", "DATA_CORRUPTION")
        raise error

    outcome_file = tmp_path / "server.outcome.json"
    monkeypatch.setattr(zippy, "run_server_daemon", failing_native_runner)
    monkeypatch.setattr(cli_server, "_validate_inherited_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        cli_server,
        "_validate_child_starting_state",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(cli_server, "_cleanup_child_state_from_environment", lambda: None)
    monkeypatch.setenv("ZIPPY_SERVER_OUTCOME_FILE", str(outcome_file))
    monkeypatch.setenv("ZIPPY_SERVER_LAUNCH_NONCE", "native-failure-generation")

    result = CliRunner().invoke(
        main,
        [
            "server",
            "run",
            str(tmp_path / "server.sock"),
            "--foreground",
            "--ready-file",
            str(tmp_path / "server.json"),
            "--launch-nonce",
            "native-failure-generation",
        ],
    )

    assert result.exit_code != 0
    outcome = json.loads(outcome_file.read_text(encoding="utf-8"))
    assert outcome["clean"] is False
    assert outcome["error_code"] == "DATA_CORRUPTION"
    validated = server_process._read_shutdown_outcome(
        outcome_file,
        pid=os.getpid(),
        launch_nonce="native-failure-generation",
    )
    assert validated is not None
    assert validated["error_code"] == "DATA_CORRUPTION"


def test_server_run_rejects_missing_explicit_config_before_spawning(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["server", "run", "--config", str(tmp_path / "missing.toml")],
    )

    assert result.exit_code != 0
    assert "config file does not exist" in result.output


def test_server_explicit_default_endpoint_wins_over_config_and_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "zippy.toml"
    config_path.write_text(
        """
[master]
host = "127.0.0.1"
port = 28690
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("ZIPPY_MASTER_HOST", "127.0.0.1")
    monkeypatch.setenv("ZIPPY_MASTER_PORT", "29690")

    endpoint, _ = _resolve_server_inputs(
        "tcp://127.0.0.1:17690",
        str(config_path),
    )

    assert endpoint == "tcp://127.0.0.1:17690"


def test_server_omitted_endpoint_uses_native_env_over_config_merge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "zippy.toml"
    config_path.write_text(
        """
[master]
host = "127.0.0.1"
port = 28690
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("ZIPPY_MASTER_HOST", "0.0.0.0")
    monkeypatch.delenv("ZIPPY_MASTER_PORT", raising=False)

    endpoint, _ = _resolve_server_inputs(None, str(config_path))

    assert endpoint == "tcp://0.0.0.0:28690"


def test_server_omitted_endpoint_uses_config_without_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = tmp_path / "zippy.toml"
    config_path.write_text(
        """
[master]
host = "127.0.0.1"
port = 28690
""",
        encoding="utf-8",
    )
    monkeypatch.delenv("ZIPPY_MASTER_HOST", raising=False)
    monkeypatch.delenv("ZIPPY_MASTER_PORT", raising=False)

    endpoint, _ = _resolve_server_inputs(None, str(config_path))

    assert endpoint == "tcp://127.0.0.1:28690"


@pytest.mark.skipif(os.name != "posix", reason="file ownership locks require Unix")
def test_server_lock_without_state_is_starting_and_not_stoppable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_endpoint = str(tmp_path / "server.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    lock_fd = _acquire_launch_lock(paths.lock_file)
    try:
        os.set_inheritable(lock_fd, True)
        monkeypatch.setenv("ZIPPY_SERVER_LOCK_FD", str(lock_fd))

        _validate_inherited_lock()
        status = _server_status(control_endpoint, run_dir=run_dir, webui_url=None)

        assert not os.get_inheritable(lock_fd)
        assert status["status"] == "starting"
        with pytest.raises(_ServerProcessError, match="has not published process state"):
            _stop_server(
                control_endpoint,
                run_dir=run_dir,
                timeout_sec=1.0,
                force=False,
            )
    finally:
        os.close(lock_fd)


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("readiness", "master_listener_ready"),
        ("readiness", "gateway_listener_ready"),
        ("readiness", "gateway_master_reachable"),
        ("readiness", "native_catalog_ready"),
        ("readiness", "native_current_snapshot_files_ready"),
        ("readiness", "storage_reconciliation_complete"),
        ("readiness", "commit_recovery_complete"),
        ("services", "accepting_work"),
        ("services", "gateway_facade_available"),
        ("services", "end_to_end_liveness"),
    ],
)
def test_server_ready_record_requires_all_runtime_facts(section: str, field: str) -> None:
    record = _valid_ready_record()
    health = record["health"]
    assert isinstance(health, dict)
    target = health[section]
    assert isinstance(target, dict)
    target[field] = False

    with pytest.raises(_ServerProcessError, match="full readiness"):
        _validate_ready_record(record)


@pytest.mark.parametrize(
    ("mode", "value"),
    [
        ("missing", None),
        ("null", None),
        ("string", "true"),
    ],
)
def test_server_ready_record_requires_boolean_native_storage_configuration(
    mode: str,
    value: object,
) -> None:
    record = _valid_ready_record()
    health = record["health"]
    assert isinstance(health, dict)
    readiness = health["readiness"]
    assert isinstance(readiness, dict)
    if mode == "missing":
        readiness.pop("native_storage_configured")
    else:
        readiness["native_storage_configured"] = value

    with pytest.raises(_ServerProcessError, match="invalid native storage configuration"):
        _validate_ready_record(record)


@pytest.mark.parametrize("value", [None, "true"])
def test_server_ready_record_requires_boolean_gateway_configuration(value: object) -> None:
    record = _valid_ready_record()
    health = record["health"]
    assert isinstance(health, dict)
    readiness = health["readiness"]
    assert isinstance(readiness, dict)
    readiness["gateway_configured"] = value

    with pytest.raises(_ServerProcessError, match="invalid gateway configuration"):
        _validate_ready_record(record)


def test_server_ready_record_rejects_previous_schema_version() -> None:
    record = _valid_ready_record()
    record["schema_version"] = 1

    with pytest.raises(_ServerProcessError, match="unsupported ready schema"):
        _validate_ready_record(record)


def test_server_status_normalizes_ready_v1_without_mutating_authoritative_state() -> None:
    record = _valid_ready_record()
    record["schema_version"] = 1
    health = record["health"]
    assert isinstance(health, dict)
    readiness = health["readiness"]
    assert isinstance(readiness, dict)
    for field in (
        "native_storage_configured",
        "native_catalog_ready",
        "native_current_snapshot_files_ready",
        "storage_reconciliation_complete",
        "commit_recovery_complete",
    ):
        readiness.pop(field)

    compatibility = server_process._ready_record_for_status_validation(record)

    _validate_ready_record(compatibility)
    assert record["schema_version"] == 1
    assert "native_storage_configured" not in readiness
    compatibility_health = compatibility["health"]
    assert isinstance(compatibility_health, dict)
    compatibility_readiness = compatibility_health["readiness"]
    assert isinstance(compatibility_readiness, dict)
    assert compatibility_readiness["native_storage_configured"] is False


def test_master_health_probe_uses_one_absolute_deadline() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    host, port = listener.getsockname()

    def slow_response() -> None:
        try:
            connection, _ = listener.accept()
            with connection:
                connection.recv(4096)
                response = b'{"ConfigFetched":{"config":{}}}\n'
                for byte in response:
                    try:
                        connection.sendall(bytes([byte]))
                    except OSError:
                        break
                    time.sleep(0.02)
        finally:
            listener.close()

    server = threading.Thread(target=slow_response)
    server.start()
    started = time.monotonic()
    try:
        with pytest.raises(_ServerProcessError, match="health probe failed|deadline exceeded"):
            _master_get_config(f"tcp://{host}:{port}", 0.08)
    finally:
        server.join(timeout=1.0)

    assert time.monotonic() - started < 0.3


def test_server_status_rejects_generation_change_during_health_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_endpoint = str(tmp_path / "generation-status.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    state: dict[str, object] = {
        "schema_version": 2,
        "status": "ready",
        "launch_nonce": "old-generation",
        "pid": 43001,
        "version": "0.5.0",
        "mode": "catalog_only",
        "control_endpoint": control_endpoint,
        "gateway": {"enabled": False},
        "launcher": {
            "process_identity": {"boot_id": "boot-old", "start_ticks": "1"},
            "requested_control_endpoint": control_endpoint,
            "webui": {"url": "http://127.0.0.1:17688/"},
            "state_file": str(paths.state_file),
        },
    }
    paths.state_file.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(server_process, "_lock_is_held", lambda _path: True)
    monkeypatch.setattr(server_process, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(server_process, "_validate_ready_record", lambda _state: None)

    def replace_generation(*_args: object, **_kwargs: object):
        replacement = dict(state)
        replacement["pid"] = 43002
        replacement["launch_nonce"] = "new-generation"
        paths.state_file.write_text(json.dumps(replacement), encoding="utf-8")
        return (
            {"status": "healthy", "endpoint": control_endpoint},
            {"enabled": False, "status": "disabled"},
            {"managed": False, "status": "not_running", "url": "http://127.0.0.1:17688/"},
        )

    monkeypatch.setattr(server_process, "_live_health", replace_generation)

    status = _server_status(control_endpoint, run_dir=run_dir, webui_url=None)

    assert status["status"] == "unknown"
    assert status["health_error"] == "server generation changed during health probe"


def test_server_status_never_trusts_static_ready_record_after_daemon_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_endpoint = str(tmp_path / "failed-native-status.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    state = _valid_ready_record()
    state.update(
        {
            "launch_nonce": "failed-native-generation",
            "pid": 43003,
            "control_endpoint": control_endpoint,
            "launcher": {
                "process_identity": {"boot_id": "boot-failed", "start_ticks": "3"},
            },
        }
    )
    paths.state_file.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(server_process, "_lock_is_held", lambda _path: False)
    monkeypatch.setattr(server_process, "_process_matches", lambda *_args: False)

    status = _server_status(control_endpoint, run_dir=run_dir, webui_url=None)

    assert status["status"] == "stopped"
    assert status["stale_state"] is True


@pytest.mark.parametrize(
    ("state_name", "error_code"),
    [("stopping", None), ("failed", "DATA_CORRUPTION")],
)
def test_server_status_does_not_probe_transitioned_generation_as_healthy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state_name: str,
    error_code: str | None,
) -> None:
    control_endpoint = str(tmp_path / "transition-status.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    state: dict[str, object] = {
        "schema_version": 1,
        "status": state_name,
        "launch_nonce": "transition-generation",
        "pid": 43004,
        "launcher": {
            "process_identity": {"boot_id": "boot-transition", "start_ticks": "4"},
            "requested_control_endpoint": control_endpoint,
            "state_file": str(paths.state_file),
        },
    }
    if error_code is not None:
        state["error_code"] = error_code
    paths.state_file.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(server_process, "_lock_is_held", lambda _path: True)
    monkeypatch.setattr(server_process, "_process_matches", lambda *_args: True)

    def unexpected_live_health(*_args: object, **_kwargs: object):
        raise AssertionError("transitioned state must not run live readiness probes")

    monkeypatch.setattr(server_process, "_live_health", unexpected_live_health)

    status = _server_status(control_endpoint, run_dir=run_dir, webui_url=None)

    assert status["status"] == state_name
    if error_code is not None:
        assert status["error_code"] == error_code


def test_webui_url_rejects_sensitive_path() -> None:
    with pytest.raises(_ServerProcessError, match="must not contain a path"):
        server_process._normalize_webui_url("http://127.0.0.1:17688/session/top-secret")


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_server_detached_lifecycle_is_ready_probeable_and_stoppable(tmp_path: Path) -> None:
    if not getattr(zippy, "_NATIVE_AVAILABLE", False):
        pytest.skip("native extension is unavailable")

    control_endpoint = tmp_path / "server.sock"
    run_dir = tmp_path / "run"
    log_dir = tmp_path / "logs"
    data_dir = tmp_path / "database"
    config_path = tmp_path / "zippy.toml"
    config_path.write_text(
        "\n".join(
            [
                "[server]",
                f"data_dir = {json.dumps(str(data_dir))}",
                "native_storage_enabled = true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    pid: int | None = None
    try:
        started = runner.invoke(
            main,
            [
                "server",
                "run",
                str(control_endpoint),
                "--config",
                str(config_path),
                "--run-dir",
                str(run_dir),
                "--log-dir",
                str(log_dir),
                "--webui-url",
                "http://127.0.0.1:1/",
                "--json",
            ],
        )
        assert started.exit_code == 0, started.output
        startup = json.loads(started.output)
        pid = int(startup["pid"])
        state_file = Path(startup["state_file"])

        assert startup["status"] == "healthy"
        assert startup["version"] == zippy.version()
        assert startup["master"]["status"] == "healthy"
        assert startup["gateway"]["status"] == "disabled"
        assert startup["webui"]["status"] == "not_running"
        assert startup["webui"]["managed"] is False
        readiness = startup["runtime_health"]["readiness"]
        assert readiness["native_storage_configured"] is True
        assert readiness["native_catalog_ready"] is True
        assert readiness["native_current_snapshot_files_ready"] is True
        assert readiness["storage_reconciliation_complete"] is True
        assert readiness["commit_recovery_complete"] is True
        assert data_dir.joinpath("catalog", "catalog.db").is_file()
        assert "launch_nonce" not in startup
        assert "process_identity" not in startup
        assert "lock_file" not in startup
        assert control_endpoint.exists()
        assert state_file.exists()
        assert state_file.stat().st_mode & 0o077 == 0
        assert run_dir.stat().st_mode & 0o077 == 0
        assert log_dir.stat().st_mode & 0o077 == 0

        status = runner.invoke(
            main,
            [
                "server",
                "status",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert status.exit_code == 0, status.output
        status_payload = json.loads(status.output)
        assert status_payload["status"] == "healthy"
        assert status_payload["master"]["status"] == "healthy"

        duplicate = runner.invoke(
            main,
            [
                "server",
                "run",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--log-dir",
                str(log_dir),
            ],
        )
        assert duplicate.exit_code != 0
        assert "already running or starting" in duplicate.output

        stopped = runner.invoke(
            main,
            [
                "server",
                "stop",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert stopped.exit_code == 0, stopped.output
        stopped_payload = json.loads(stopped.output)
        assert stopped_payload["status"] == "stopped"
        assert stopped_payload["graceful"] is True
        assert stopped_payload["forced"] is False

        deadline = time.monotonic() + 5.0
        while control_endpoint.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not control_endpoint.exists()
        assert not state_file.exists()

        final_status = runner.invoke(
            main,
            [
                "server",
                "status",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert final_status.exit_code == 1
        assert json.loads(final_status.output)["status"] == "stopped"
        pid = None
    finally:
        if pid is not None:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_server_child_remains_manageable_when_launcher_is_killed(tmp_path: Path) -> None:
    if not getattr(zippy, "_NATIVE_AVAILABLE", False):
        pytest.skip("native extension is unavailable")

    control_endpoint = tmp_path / "orphan-safe-server.sock"
    run_dir = tmp_path / "run"
    log_dir = tmp_path / "logs"
    paths = _server_paths(
        str(control_endpoint),
        run_dir=run_dir,
        log_dir=log_dir,
    )
    environment = os.environ.copy()
    environment["ZIPPY_SERVER_TEST_PAUSE_AFTER_SPAWN_SEC"] = "30"
    launcher = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "zippy",
            "server",
            "run",
            str(control_endpoint),
            "--run-dir",
            str(run_dir),
            "--log-dir",
            str(log_dir),
            "--webui-url",
            "http://127.0.0.1:1/",
            "--startup-timeout-sec",
            "40",
            "--json",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )
    child_pid: int | None = None
    stopped = False
    try:
        deadline = time.monotonic() + 15.0
        state: dict[str, object] | None = None
        while time.monotonic() < deadline:
            try:
                candidate = json.loads(paths.state_file.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.05)
                continue
            if candidate.get("status") == "ready":
                state = candidate
                break
            time.sleep(0.05)
        assert state is not None
        child_pid = server_process._state_pid(state)

        launcher.kill()
        launcher.wait(timeout=5.0)

        runner = CliRunner()
        status = runner.invoke(
            main,
            [
                "server",
                "status",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--webui-url",
                "http://127.0.0.1:1/",
                "--json",
            ],
        )
        assert status.exit_code == 0, status.output
        status_payload = json.loads(status.output)
        assert status_payload["status"] == "healthy"
        assert status_payload["pid"] == child_pid

        stop = runner.invoke(
            main,
            [
                "server",
                "stop",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert stop.exit_code == 0, stop.output
        stop_payload = json.loads(stop.output)
        assert stop_payload["status"] == "stopped"
        assert stop_payload["graceful"] is True
        stopped = True
    finally:
        if launcher.poll() is None:
            launcher.kill()
            launcher.wait(timeout=5.0)
        if child_pid is not None and not stopped:
            try:
                os.kill(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_server_bootstrap_state_is_stoppable_before_zippy_import(tmp_path: Path) -> None:
    if not getattr(zippy, "_NATIVE_AVAILABLE", False):
        pytest.skip("native extension is unavailable")

    control_endpoint = tmp_path / "bootstrap-paused-server.sock"
    run_dir = tmp_path / "run"
    log_dir = tmp_path / "logs"
    paths = _server_paths(
        str(control_endpoint),
        run_dir=run_dir,
        log_dir=log_dir,
    )
    environment = os.environ.copy()
    environment["ZIPPY_SERVER_TEST_BOOTSTRAP_PAUSE_SEC"] = "30"
    launcher = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "zippy",
            "server",
            "run",
            str(control_endpoint),
            "--run-dir",
            str(run_dir),
            "--log-dir",
            str(log_dir),
            "--startup-timeout-sec",
            "40",
            "--json",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )
    child_pid: int | None = None
    stopped = False
    try:
        deadline = time.monotonic() + 10.0
        starting: dict[str, object] | None = None
        while time.monotonic() < deadline:
            try:
                candidate = json.loads(paths.state_file.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(0.05)
                continue
            if candidate.get("status") == "starting":
                starting = candidate
                break
            time.sleep(0.05)
        assert starting is not None
        child_pid = server_process._state_pid(starting)

        launcher.kill()
        launcher.wait(timeout=5.0)

        runner = CliRunner()
        status = runner.invoke(
            main,
            [
                "server",
                "status",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert status.exit_code == 1, status.output
        assert json.loads(status.output)["status"] == "starting"

        stop = runner.invoke(
            main,
            [
                "server",
                "stop",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert stop.exit_code == 0, stop.output
        stop_payload = json.loads(stop.output)
        assert stop_payload["status"] == "stopped"
        assert stop_payload["startup_aborted"] is True
        assert stop_payload["graceful"] is True
        stopped = True
    finally:
        if launcher.poll() is None:
            launcher.kill()
            launcher.wait(timeout=5.0)
        if child_pid is not None and not stopped:
            try:
                os.kill(child_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_old_stop_preserves_replacement_generation_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_endpoint = str(tmp_path / "generation-server.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    old_state: dict[str, object] = {
        "schema_version": 1,
        "status": "ready",
        "launch_nonce": "old-generation",
        "pid": 41001,
        "launcher": {
            "process_identity": {"boot_id": "boot-old", "start_ticks": "1"},
        },
    }
    paths.state_file.write_text(json.dumps(old_state), encoding="utf-8")
    monkeypatch.setattr(server_process, "_lock_is_held", lambda _path: True)
    monkeypatch.setattr(server_process, "_process_matches", lambda _pid, _identity: True)
    monkeypatch.setattr(server_process, "_signal_owned_process", lambda *_args: True)

    replacement = dict(old_state)
    replacement["launch_nonce"] = "new-generation"
    replacement["pid"] = 41002

    def replace_during_wait(_pid: int, _identity: object, _timeout: float) -> bool:
        paths.state_file.write_text(json.dumps(replacement), encoding="utf-8")
        return True

    monkeypatch.setattr(server_process, "_wait_for_process_exit", replace_during_wait)
    monkeypatch.setattr(server_process, "_try_acquire_launch_lock", lambda _path: None)

    with pytest.raises(_ServerProcessError, match="generation changed"):
        _stop_server(
            control_endpoint,
            run_dir=run_dir,
            timeout_sec=1.0,
            force=False,
        )

    assert json.loads(paths.state_file.read_text(encoding="utf-8")) == replacement


def test_failed_launcher_cleanup_preserves_replacement_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control_endpoint = str(tmp_path / "failed-launch-generation.sock")
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=tmp_path / "logs",
    )
    replacement = {
        "schema_version": 1,
        "status": "starting",
        "launch_nonce": "replacement-generation",
        "pid": 42002,
    }
    paths.state_file.write_text(json.dumps(replacement), encoding="utf-8")
    monkeypatch.setattr(server_process, "_try_acquire_launch_lock", lambda _path: None)

    server_process._cleanup_failed_launch(
        paths,
        pid=42001,
        launch_nonce="failed-generation",
        inherited_lock_fd=-1,
    )

    assert json.loads(paths.state_file.read_text(encoding="utf-8")) == replacement


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_server_detached_gateway_probe_does_not_persist_token(tmp_path: Path) -> None:
    if not getattr(zippy, "_NATIVE_AVAILABLE", False):
        pytest.skip("native extension is unavailable")

    control_endpoint = tmp_path / "gateway-server.sock"
    run_dir = tmp_path / "run"
    log_dir = tmp_path / "logs"
    config_path = tmp_path / "zippy.toml"
    gateway_endpoint = _unused_gateway_endpoint()
    token = "gateway-secret-must-not-be-persisted"
    config_path.write_text(
        f"""
[master]
shutdown_timeout_ms = 1000

[gateway]
enabled = true
endpoint = "{gateway_endpoint}"
token = "{token}"
""",
        encoding="utf-8",
    )
    runner = CliRunner()
    pid: int | None = None
    try:
        started = runner.invoke(
            main,
            [
                "server",
                "run",
                str(control_endpoint),
                "--config",
                str(config_path),
                "--run-dir",
                str(run_dir),
                "--log-dir",
                str(log_dir),
                "--webui-url",
                "http://127.0.0.1:1/",
                "--json",
            ],
        )
        assert started.exit_code == 0, started.output
        startup = json.loads(started.output)
        pid = int(startup["pid"])
        state_file = Path(startup["state_file"])

        assert startup["mode"] == "catalog_and_query"
        assert startup["gateway"]["status"] == "healthy"
        assert startup["gateway"]["endpoint"] == gateway_endpoint
        assert token not in started.output
        assert token not in state_file.read_text(encoding="utf-8")

        stopped = runner.invoke(
            main,
            [
                "server",
                "stop",
                str(control_endpoint),
                "--run-dir",
                str(run_dir),
                "--json",
            ],
        )
        assert stopped.exit_code == 0, stopped.output
        pid = None
    finally:
        if pid is not None:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="detached lifecycle requires Linux process identity",
)
def test_server_startup_failure_rolls_back_master_and_state(tmp_path: Path) -> None:
    if not getattr(zippy, "_NATIVE_AVAILABLE", False):
        pytest.skip("native extension is unavailable")

    control_endpoint = tmp_path / "failed-server.sock"
    run_dir = tmp_path / "run"
    log_dir = tmp_path / "logs"
    config_path = tmp_path / "zippy.toml"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        host, port = occupied.getsockname()
        config_path.write_text(
            f"""
[master]
shutdown_timeout_ms = 1000

[gateway]
enabled = true
endpoint = "{host}:{port}"
token = "startup-failure-secret"
""",
            encoding="utf-8",
        )
        result = CliRunner().invoke(
            main,
            [
                "server",
                "run",
                str(control_endpoint),
                "--config",
                str(config_path),
                "--run-dir",
                str(run_dir),
                "--log-dir",
                str(log_dir),
                "--startup-timeout-sec",
                "3",
            ],
        )

    assert result.exit_code != 0
    assert "server exited before readiness" in result.output
    assert "startup-failure-secret" not in result.output
    assert not control_endpoint.exists()
    paths = _server_paths(
        str(control_endpoint),
        run_dir=run_dir,
        log_dir=log_dir,
    )
    assert not paths.state_file.exists()
    outcome = json.loads(paths.outcome_file.read_text(encoding="utf-8"))
    assert outcome["clean"] is False
    assert "startup-failure-secret" not in json.dumps(outcome)
