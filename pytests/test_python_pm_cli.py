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
    PmRuntimeConfig,
    custom_task,
    format_pm_toml,
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
