from __future__ import annotations

from pathlib import Path
import tomllib

from click.testing import CliRunner
import pytest

from zippy.cli import main
from zippy.pm_bridge import (
    DEFAULT_PM_CONFIG_FILE,
    DEFAULT_PM_DAEMON_NAME,
    DEFAULT_PM_ROOT,
    PmCommandRunner,
    PmRpcClient,
    PmRuntimeConfig,
    PmSupervisor,
    custom_task,
    format_pm_toml,
    add_task,
    master_task,
)


def read_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def test_pm_help_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["pm", "--help"])

    assert result.exit_code == 0
    assert "Manage local zippy processes through zippy-rspm." in result.output
    assert "validate" in result.output
    assert "apply" in result.output
    assert "add" in result.output
    assert "start" in result.output
    assert "stop" in result.output
    assert "restart" in result.output
    assert "doctor" in result.output


def test_pm_runtime_defaults_are_zippy_scoped() -> None:
    config = PmRuntimeConfig.default()

    assert DEFAULT_PM_CONFIG_FILE == "zippy.pm.toml"
    assert DEFAULT_PM_DAEMON_NAME == "zippy-rspm"
    assert DEFAULT_PM_ROOT == Path(".zippy") / "rspm"
    assert config.log_dir == Path(".zippy") / "rspm" / "logs"
    assert config.state_dir == Path(".zippy") / "rspm" / "state"
    assert config.run_dir == Path(".zippy") / "rspm" / "run"
    assert config.socket_path == Path(".zippy") / "rspm" / "run" / "zippy-rspm.sock"


def test_pm_format_preserves_literal_task_names_with_dots() -> None:
    text = format_pm_toml(
        {
            "project": {"name": "zippy-local"},
            "runtime": {},
            "tasks": {"foo.bar": {"cmd": "zippy", "args": []}},
        }
    )

    parsed = tomllib.loads(text)

    assert parsed["tasks"]["foo.bar"]["cmd"] == "zippy"


def test_pm_add_preserves_defaults_table(tmp_path: Path) -> None:
    config_path = tmp_path / "zippy.pm.toml"
    config_path.write_text(
        """
[project]
name = "zippy-local"

[defaults]
restart = "on-failure"
restart_delay = "2s"
max_restarts = 7

[tasks.worker]
cmd = "python"
args = ["-m", "worker"]
""",
        encoding="utf-8",
    )

    add_task(config_path, "master", master_task())

    config = read_toml(config_path)
    assert config["defaults"] == {
        "restart": "on-failure",
        "restart_delay": "2s",
        "max_restarts": 7,
    }
    assert config["tasks"]["worker"]["cmd"] == "python"
    assert config["tasks"]["master"]["cmd"] == "zippy"


def test_pm_custom_task_rejects_invalid_env_key() -> None:
    with pytest.raises(ValueError):
        custom_task("python -m worker", env=["BAD.KEY=1"])


def test_pm_master_task_rejects_malformed_tcp_uri() -> None:
    with pytest.raises(ValueError, match="tcp://"):
        master_task("tcp://")


def test_pm_add_master_writes_zippy_pm_toml() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["pm", "add", "master"])

        assert result.exit_code == 0
        assert "added [master]" in result.output

        config = read_toml(Path(DEFAULT_PM_CONFIG_FILE))
        task = config["tasks"]["master"]
        health = task["health"]

        assert config["project"]["name"] == "zippy-local"
        assert task["cmd"] == "zippy"
        assert task["args"] == ["master", "run", "tcp://127.0.0.1:17690"]
        assert task["autostart"] is True
        assert health["type"] == "tcp"
        assert health["address"] == "127.0.0.1:17690"


def test_pm_add_gateway_writes_dependency_on_master() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main,
            [
                "pm",
                "add",
                "gateway",
                "--master-uri",
                "tcp://127.0.0.1:17690",
                "--endpoint",
                "127.0.0.1:17691",
                "--token",
                "dev-token",
            ],
        )

        assert result.exit_code == 0
        assert "added [gateway]" in result.output

        config = read_toml(Path(DEFAULT_PM_CONFIG_FILE))
        task = config["tasks"]["gateway"]

        assert task["cmd"] == "zippy"
        assert task["depends_on"] == ["master"]
        assert task["start_when"] == "healthy"
        assert task["args"] == [
            "gateway",
            "run",
            "--master-uri",
            "tcp://127.0.0.1:17690",
            "--endpoint",
            "127.0.0.1:17691",
            "--token",
            "dev-token",
        ]


def test_pm_add_custom_writes_explicit_command() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            main,
            [
                "pm",
                "add",
                "custom",
                "python -m worker --flag",
                "--name",
                "worker",
                "--cwd",
                "/srv/zippy",
                "--env",
                "MODE=dev",
                "--env",
                "EMPTY",
            ],
        )

        assert result.exit_code == 0
        assert "added [worker]" in result.output

        config = read_toml(Path(DEFAULT_PM_CONFIG_FILE))
        task = config["tasks"]["worker"]

        assert task["cmd"] == "python"
        assert task["args"] == ["-m", "worker", "--flag"]
        assert task["cwd"] == "/srv/zippy"
        assert task["env"] == {"MODE": "dev", "EMPTY": ""}


def test_pm_supervisor_builds_zippy_rspm_daemon_command() -> None:
    config = PmRuntimeConfig.default()
    supervisor = PmSupervisor(config)

    assert supervisor.daemon_command(Path("zippy.pm.toml")) == [
        "zippy-rspm",
        "daemon",
        "run",
        "zippy.pm.toml",
        "127.0.0.1:27691",
        str(config.log_dir),
        str(config.state_dir),
        str(config.socket_path),
    ]

    supervisor_with_token = PmSupervisor(config, token="dev-token")

    assert supervisor_with_token.daemon_command(Path("zippy.pm.toml"))[-2:] == [
        "--token",
        "dev-token",
    ]


def test_pm_validate_delegates_to_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Path]] = []

    class FakeRunner:
        def validate(self, path: str | Path) -> str:
            calls.append(("validate", Path(path)))
            return "valid [zippy-local] tasks=[1]"

    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "validate", "-f", "zippy.pm.toml"])

    assert result.exit_code == 0
    assert calls == [("validate", Path("zippy.pm.toml"))]
    assert result.output == "valid [zippy-local] tasks=[1]\n"


def test_pm_apply_delegates_to_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Path, bool]] = []

    class FakeRunner:
        def apply(self, path: str | Path, *, dry_run: bool = False) -> str:
            calls.append(("apply", Path(path), dry_run))
            return "apply dry-run [zippy-local] tasks=1"

    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "apply", "-f", "zippy.pm.toml", "--dry-run"])

    assert result.exit_code == 0
    assert calls == [("apply", Path("zippy.pm.toml"), True)]
    assert result.output == "apply dry-run [zippy-local] tasks=1\n"


def test_pm_validate_rejects_missing_config() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(main, ["pm", "validate"])

        assert result.exit_code != 0
        assert "missing config [zippy.pm.toml]" in result.output


def test_pm_validate_rejects_task_without_cmd() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path(DEFAULT_PM_CONFIG_FILE).write_text(
            """
[project]
name = "bad"

[tasks.worker]
args = ["-m", "worker"]
""",
            encoding="utf-8",
        )

        result = runner.invoke(main, ["pm", "validate"])

        assert result.exit_code != 0
        assert "task [worker] must define non-empty cmd" in result.output


def test_pm_start_all_delegates_to_all(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []

    class FakeRunner:
        def start(self, name: str | None = None) -> list[object]:
            calls.append(("start", name))
            return [type("Task", (), {"name": "master", "status": "online", "pid": 123})()]

    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "start", "all"])

    assert result.exit_code == 0
    assert calls == [("start", "all")]
    assert "start [master] status=[online] pid=[123]" in result.output


def test_pm_logs_accepts_optional_task(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None, int | None]] = []

    class FakeRunner:
        def logs(self, name: str | None = None, *, lines: int | None = None) -> str:
            calls.append(("logs", name, lines))
            return "combined logs"

    monkeypatch.setattr(PmCommandRunner, "default", classmethod(lambda cls: FakeRunner()))

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "logs", "--lines", "20"])

    assert result.exit_code == 0
    assert calls == [("logs", None, 20)]
    assert result.output == "combined logs\n"


def test_pm_supervisor_does_not_spawn_when_ready_probe_returns_rpc_error(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "zippy.pm.toml"
    config_path.write_text('[project]\nname = "zippy-local"\n', encoding="utf-8")
    spawned: list[Path] = []

    class FakeClient:
        def list_tasks(self) -> None:
            raise RuntimeError("auth failed")

    class FakeSupervisor(PmSupervisor):
        def client(self) -> FakeClient:
            return FakeClient()

        def _spawn_daemon(self, config_path: str | Path) -> None:
            spawned.append(Path(config_path))

    supervisor = FakeSupervisor(PmRuntimeConfig.default())

    with pytest.raises(RuntimeError, match="auth failed"):
        supervisor.ensure_daemon(config_path)

    assert spawned == []


def test_pm_rpc_client_rejects_success_response_without_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSocket:
        def __enter__(self) -> "FakeSocket":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def sendall(self, payload: bytes) -> None:
            return None

    monkeypatch.setattr("socket.create_connection", lambda *args, **kwargs: FakeSocket())
    monkeypatch.setattr(
        "zippy.pm_bridge._read_json_line",
        lambda sock: {"jsonrpc": "2.0", "id": 1},
    )

    with pytest.raises(RuntimeError, match="task.list"):
        PmRpcClient().request("task.list")


def test_pm_rpc_client_rejects_mismatched_response_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeSocket:
        def __enter__(self) -> "FakeSocket":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def sendall(self, payload: bytes) -> None:
            return None

    monkeypatch.setattr("socket.create_connection", lambda *args, **kwargs: FakeSocket())
    monkeypatch.setattr(
        "zippy.pm_bridge._read_json_line",
        lambda sock: {"jsonrpc": "2.0", "id": 99, "result": []},
    )

    with pytest.raises(RuntimeError, match="response id"):
        PmRpcClient().request("task.list")
