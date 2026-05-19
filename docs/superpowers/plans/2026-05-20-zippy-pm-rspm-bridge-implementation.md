# zippy pm rspm bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `zippy pm ...` as a local-first process manager bridge that controls a `zippy-rspm` daemon through the rspm JSON-RPC control plane.

**Architecture:** Keep all user-facing process-management semantics in Python CLI modules. Add a focused `python/zippy/pm_bridge.py` adapter with zippy defaults, deterministic TOML generation, a small JSON-RPC client, and a detached daemon supervisor for `zippy-rspm`. Add `python/zippy/cli_pm.py` as the Click command layer and register it from `python/zippy/cli.py`.

**Tech Stack:** Python 3.11, Click, `tomllib`, stdlib `socket` / `subprocess`, existing zippy CLI test style with `click.testing.CliRunner`, pytest.

---

## File Structure

- Create `python/zippy/pm_bridge.py`
  - Owns constants: `DEFAULT_PM_CONFIG_FILE`, `DEFAULT_PM_DAEMON_NAME`, `DEFAULT_PM_ROOT`, `DEFAULT_PM_HOST`, `DEFAULT_PM_PORT`.
  - Owns dataclasses: `PmRuntimeConfig`, `PmTaskInfo`, `PmRpcClient`, `PmSupervisor`, `PmCommandRunner`.
  - Owns deterministic `zippy.pm.toml` read/write helpers and task-template builders.
  - Contains no Click decorators.
- Create `python/zippy/cli_pm.py`
  - Owns the `@click.group("pm")` command group and all CLI commands.
  - Converts Click options into `pm_bridge` calls.
  - Uses existing `cli_error()` and compact table output style.
- Modify `python/zippy/cli.py`
  - Registers `pm_group`.
- Modify `pyproject.toml`
  - Adds `python/zippy/cli_pm.py` and `python/zippy/pm_bridge.py` to mypy file list.
- Create `pytests/test_python_pm_cli.py`
  - Covers help registration, defaults, template generation, command routing, and daemon supervisor command construction.
- Modify `pytests/test_python_cli.py`
  - Only update root-help assertions to include `pm`.

Do not stage unrelated current worktree changes. The implementation commits must include only files listed above unless a failing test proves another file is necessary.

---

### Task 1: Add Failing CLI Registration And Template Tests

**Files:**
- Create: `pytests/test_python_pm_cli.py`
- Modify: `pytests/test_python_cli.py`
- Later implementation target: `python/zippy/cli.py`, `python/zippy/cli_pm.py`, `python/zippy/pm_bridge.py`

- [ ] **Step 1: Create failing pm CLI tests**

Create `pytests/test_python_pm_cli.py` with these tests:

```python
from __future__ import annotations

from pathlib import Path
import tomllib

from click.testing import CliRunner

from zippy.cli import main
from zippy.pm_bridge import (
    DEFAULT_PM_CONFIG_FILE,
    DEFAULT_PM_DAEMON_NAME,
    DEFAULT_PM_ROOT,
    PmRuntimeConfig,
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
```

- [ ] **Step 2: Update existing root help test expectations**

In `pytests/test_python_cli.py`, update `test_cli_root_help()` and `test_python_module_entrypoint_shows_root_help()` so they assert `"pm" in result.output`.

- [ ] **Step 3: Run tests and verify they fail for missing module/command**

Run:

```bash
uv run pytest pytests/test_python_pm_cli.py pytests/test_python_cli.py::test_cli_root_help -q
```

Expected: fail with an import error for `zippy.pm_bridge` or a missing `pm` command. This proves the tests describe new behavior.

- [ ] **Step 4: Commit tests**

Do not commit yet if the repository policy requires green commits only. If committing red tests is acceptable for this branch, run:

```bash
git add pytests/test_python_pm_cli.py pytests/test_python_cli.py
git commit -m "test: cover zippy pm cli bridge"
```

If keeping commits green, skip this commit and include these tests in Task 2's commit.

---

### Task 2: Implement pm_bridge Defaults, TOML Writer, And Template Builders

**Files:**
- Create: `python/zippy/pm_bridge.py`
- Test: `pytests/test_python_pm_cli.py`

- [ ] **Step 1: Add bridge constants and dataclasses**

Create `python/zippy/pm_bridge.py` with this initial structure:

```python
"""
Adapter helpers for the local zippy process manager bridge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shlex
import socket
import subprocess
import time
import tomllib
from typing import Any


DEFAULT_PM_CONFIG_FILE = "zippy.pm.toml"
DEFAULT_PM_DAEMON_NAME = "zippy-rspm"
DEFAULT_PM_ROOT = Path(".zippy") / "rspm"
DEFAULT_PM_HOST = "127.0.0.1"
DEFAULT_PM_PORT = 27691
DEFAULT_MASTER_URI = "tcp://127.0.0.1:17690"


@dataclass(frozen=True)
class PmRuntimeConfig:
    """
    Runtime defaults for the local zippy-rspm sidecar.

    :param host: TCP host for the local JSON-RPC bridge.
    :type host: str
    :param port: TCP port for the local JSON-RPC bridge.
    :type port: int
    :param root_dir: Root directory for zippy-rspm runtime files.
    :type root_dir: pathlib.Path
    :param binary_path: Executable used to spawn the detached daemon.
    :type binary_path: str
    :param token: Optional local control token.
    :type token: str | None
    :param startup_timeout_sec: Seconds to wait for daemon readiness.
    :type startup_timeout_sec: float
    """

    host: str = DEFAULT_PM_HOST
    port: int = DEFAULT_PM_PORT
    root_dir: Path = DEFAULT_PM_ROOT
    binary_path: str = DEFAULT_PM_DAEMON_NAME
    token: str | None = None
    startup_timeout_sec: float = 10.0

    @classmethod
    def default(cls) -> "PmRuntimeConfig":
        """
        Build the default zippy-scoped runtime config.

        :returns: Runtime configuration.
        :rtype: PmRuntimeConfig
        """

        return cls()

    @property
    def log_dir(self) -> Path:
        """Return the daemon and task log directory."""

        return self.root_dir / "logs"

    @property
    def state_dir(self) -> Path:
        """Return the daemon state directory."""

        return self.root_dir / "state"

    @property
    def run_dir(self) -> Path:
        """Return the daemon runtime directory."""

        return self.root_dir / "run"

    @property
    def socket_path(self) -> Path:
        """Return the reserved local socket path."""

        return self.run_dir / "zippy-rspm.sock"

    @property
    def endpoint(self) -> str:
        """Return the TCP JSON-RPC endpoint."""

        return f"tcp://{self.host}:{self.port}"
```

- [ ] **Step 2: Add deterministic config builders**

Append this code to `python/zippy/pm_bridge.py`:

```python
def default_project_config() -> dict[str, Any]:
    """
    Build the default zippy pm TOML object.

    :returns: Mutable config dictionary.
    :rtype: dict[str, Any]
    """

    return {
        "project": {
            "name": "zippy-local",
            "timezone": "Asia/Shanghai",
            "display_timezone": "Asia/Shanghai",
        },
        "defaults": {
            "restart": "on-failure",
            "restart_delay": "2s",
            "max_restarts": 5,
        },
        "tasks": {},
    }


def load_pm_config(path: Path) -> dict[str, Any]:
    """
    Load a pm config file or return a default config if it does not exist.

    :param path: TOML config path.
    :type path: pathlib.Path
    :returns: Mutable config dictionary.
    :rtype: dict[str, Any]
    """

    if not path.exists():
        return default_project_config()
    with path.open("rb") as handle:
        config = tomllib.load(handle)
    config.setdefault("project", default_project_config()["project"])
    config.setdefault("defaults", default_project_config()["defaults"])
    config.setdefault("tasks", {})
    return config


def write_pm_config(path: Path, config: dict[str, Any]) -> None:
    """
    Write a deterministic rspm-compatible TOML config.

    :param path: Destination path.
    :type path: pathlib.Path
    :param config: Config object.
    :type config: dict[str, Any]
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_pm_toml(config), encoding="utf-8")


def format_pm_toml(config: dict[str, Any]) -> str:
    """
    Format a pm config as TOML.

    :param config: Config object.
    :type config: dict[str, Any]
    :returns: TOML text.
    :rtype: str
    """

    lines: list[str] = []
    _write_table(lines, "project", config.get("project", {}))
    _write_table(lines, "defaults", config.get("defaults", {}))
    tasks = config.get("tasks", {})
    if isinstance(tasks, dict):
        for task_name in sorted(tasks):
            task = tasks[task_name]
            if not isinstance(task, dict):
                continue
            lines.append("")
            lines.append(f"[tasks.{task_name}]")
            nested: dict[str, dict[str, Any]] = {}
            for key, value in task.items():
                if isinstance(value, dict):
                    nested[key] = value
                else:
                    lines.append(f"{key} = {_format_toml_value(value)}")
            for nested_name, nested_value in nested.items():
                lines.append("")
                lines.append(f"[tasks.{task_name}.{nested_name}]")
                for key, value in nested_value.items():
                    lines.append(f"{key} = {_format_toml_value(value)}")
    return "\n".join(lines).rstrip() + "\n"


def _write_table(lines: list[str], name: str, values: object) -> None:
    lines.append(f"[{name}]")
    if isinstance(values, dict):
        for key, value in values.items():
            lines.append(f"{key} = {_format_toml_value(value)}")
    lines.append("")


def _format_toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_format_toml_value(item) for item in value) + "]"
    if value is None:
        return '""'
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
```

- [ ] **Step 3: Add template functions and config mutation**

Append this code to `python/zippy/pm_bridge.py`:

```python
def master_task(master_uri: str = DEFAULT_MASTER_URI) -> dict[str, Any]:
    """
    Build the default zippy-master task.

    :param master_uri: zippy-master control URI.
    :type master_uri: str
    :returns: Task configuration.
    :rtype: dict[str, Any]
    """

    host, port = _tcp_host_port(master_uri)
    return {
        "cmd": "zippy",
        "args": ["master", "run", master_uri],
        "autostart": True,
        "health": {
            "type": "tcp",
            "address": f"{host}:{port}",
            "interval": "1s",
            "timeout": "500ms",
            "success_after": 1,
            "failure_after": 3,
        },
    }


def gateway_task(
    master_uri: str = DEFAULT_MASTER_URI,
    *,
    endpoint: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    """
    Build the default zippy gateway task.

    :param master_uri: zippy-master URI used by the gateway.
    :type master_uri: str
    :param endpoint: Optional gateway endpoint.
    :type endpoint: str | None
    :param token: Optional remote gateway token.
    :type token: str | None
    :returns: Task configuration.
    :rtype: dict[str, Any]
    """

    args = ["gateway", "run", "--master-uri", master_uri]
    if endpoint is not None:
        args.extend(["--endpoint", endpoint])
    if token is not None:
        args.extend(["--token", token])
    return {
        "cmd": "zippy",
        "args": args,
        "depends_on": ["master"],
        "start_when": "healthy",
        "autostart": True,
    }


def custom_task(command: str, *, cwd: str | None = None, env: list[str] | None = None) -> dict[str, Any]:
    """
    Build a custom task from a shell-like command string.

    :param command: Command text.
    :type command: str
    :param cwd: Optional working directory.
    :type cwd: str | None
    :param env: Environment entries in KEY or KEY=VALUE form.
    :type env: list[str] | None
    :returns: Task configuration.
    :rtype: dict[str, Any]
    """

    parts = shlex.split(command)
    if not parts:
        raise ValueError("custom command must not be empty")
    task: dict[str, Any] = {"cmd": parts[0], "args": parts[1:]}
    if cwd is not None:
        task["cwd"] = cwd
    env_map = parse_env_entries(env or [])
    if env_map:
        task["env"] = env_map
    return task


def parse_env_entries(entries: list[str]) -> dict[str, str]:
    """
    Parse CLI environment entries.

    :param entries: Entries in KEY or KEY=VALUE form.
    :type entries: list[str]
    :returns: Environment mapping.
    :rtype: dict[str, str]
    """

    env: dict[str, str] = {}
    for entry in entries:
        key, separator, value = entry.partition("=")
        if not key:
            raise ValueError("environment key must not be empty")
        env[key] = value if separator else ""
    return env


def add_task(path: Path, name: str, task: dict[str, Any]) -> dict[str, Any]:
    """
    Add or replace a task in a pm config file.

    :param path: TOML config path.
    :type path: pathlib.Path
    :param name: Task name.
    :type name: str
    :param task: Task configuration.
    :type task: dict[str, Any]
    :returns: Updated config.
    :rtype: dict[str, Any]
    """

    config = load_pm_config(path)
    tasks = config.setdefault("tasks", {})
    if not isinstance(tasks, dict):
        raise ValueError("[tasks] must be a TOML table")
    tasks[name] = task
    write_pm_config(path, config)
    return config


def _tcp_host_port(uri: str) -> tuple[str, int]:
    if not uri.startswith("tcp://"):
        raise ValueError(f"expected tcp URI for health check, got [{uri}]")
    authority = uri.removeprefix("tcp://").split("/", 1)[0]
    host, port_text = authority.rsplit(":", 1)
    return host, int(port_text)
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
uv run pytest pytests/test_python_pm_cli.py::test_pm_runtime_defaults_are_zippy_scoped pytests/test_python_pm_cli.py::test_pm_add_master_writes_zippy_pm_toml pytests/test_python_pm_cli.py::test_pm_add_gateway_writes_dependency_on_master pytests/test_python_pm_cli.py::test_pm_add_custom_writes_explicit_command -q
```

Expected: still fail on missing `pm` command until Task 3 registers CLI commands, but import errors for `zippy.pm_bridge` should be gone.

---

### Task 3: Add zippy pm Click Commands

**Files:**
- Create: `python/zippy/cli_pm.py`
- Modify: `python/zippy/cli.py`
- Test: `pytests/test_python_pm_cli.py`, `pytests/test_python_cli.py`

- [ ] **Step 1: Create `cli_pm.py` command group**

Create `python/zippy/cli_pm.py`:

```python
"""
Process management commands for the zippy CLI.
"""

from __future__ import annotations

from pathlib import Path

import click

from .cli_common import cli_error
from .pm_bridge import (
    DEFAULT_MASTER_URI,
    DEFAULT_PM_CONFIG_FILE,
    PmCommandRunner,
    add_task,
    custom_task,
    gateway_task,
    master_task,
)


@click.group("pm")
def pm_group() -> None:
    """
    Manage local zippy processes through zippy-rspm.
    """


@pm_group.command("validate")
@click.option("-f", "--file", "file_path", default=DEFAULT_PM_CONFIG_FILE, show_default=True)
def validate_pm(file_path: str) -> None:
    """
    Validate a zippy pm config file.
    """

    try:
        PmCommandRunner.default().validate(Path(file_path))
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


@pm_group.command("apply")
@click.option("-f", "--file", "file_path", default=DEFAULT_PM_CONFIG_FILE, show_default=True)
@click.option("--dry-run", is_flag=True, default=False)
def apply_pm(file_path: str, dry_run: bool) -> None:
    """
    Apply a zippy pm config file.
    """

    try:
        PmCommandRunner.default().apply(Path(file_path), dry_run=dry_run)
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


@pm_group.group("add")
def add_group() -> None:
    """
    Add a zippy pm task template.
    """


@add_group.command("master")
@click.option("-f", "--file", "file_path", default=DEFAULT_PM_CONFIG_FILE, show_default=True)
@click.option("--master-uri", default=DEFAULT_MASTER_URI, show_default=True)
def add_master(file_path: str, master_uri: str) -> None:
    """
    Add the local zippy-master task.
    """

    try:
        add_task(Path(file_path), "master", master_task(master_uri))
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    click.echo("added [master]")


@add_group.command("gateway")
@click.option("-f", "--file", "file_path", default=DEFAULT_PM_CONFIG_FILE, show_default=True)
@click.option("--master-uri", default=DEFAULT_MASTER_URI, show_default=True)
@click.option("--endpoint", default=None)
@click.option("--token", default=None)
def add_gateway(file_path: str, master_uri: str, endpoint: str | None, token: str | None) -> None:
    """
    Add the local zippy gateway task.
    """

    try:
        add_task(Path(file_path), "gateway", gateway_task(master_uri, endpoint=endpoint, token=token))
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    click.echo("added [gateway]")


@add_group.command("custom")
@click.argument("command")
@click.option("-f", "--file", "file_path", default=DEFAULT_PM_CONFIG_FILE, show_default=True)
@click.option("--name", default=None)
@click.option("--cwd", default=None)
@click.option("--env", "env_entries", multiple=True)
def add_custom(
    command: str,
    file_path: str,
    name: str | None,
    cwd: str | None,
    env_entries: tuple[str, ...],
) -> None:
    """
    Add a custom local task.
    """

    try:
        task = custom_task(command, cwd=cwd, env=list(env_entries))
        task_name = name or str(task["cmd"])
        add_task(Path(file_path), task_name, task)
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"added [{task_name}]")
```

- [ ] **Step 2: Register `pm_group` in root CLI**

Modify `python/zippy/cli.py`:

```python
from .cli_pm import pm_group
```

and add:

```python
main.add_command(pm_group)
```

- [ ] **Step 3: Run registration and template tests**

Run:

```bash
uv run pytest pytests/test_python_pm_cli.py::test_pm_help_is_registered pytests/test_python_pm_cli.py::test_pm_add_master_writes_zippy_pm_toml pytests/test_python_pm_cli.py::test_pm_add_gateway_writes_dependency_on_master pytests/test_python_pm_cli.py::test_pm_add_custom_writes_explicit_command pytests/test_python_cli.py::test_cli_root_help -q
```

Expected: `pm` help and add-template tests pass; command-runner methods may still fail if invoked directly because Task 4 has not implemented them.

- [ ] **Step 4: Commit CLI registration and templates**

Run:

```bash
git add python/zippy/pm_bridge.py python/zippy/cli_pm.py python/zippy/cli.py pytests/test_python_pm_cli.py pytests/test_python_cli.py
git commit -m "feat: add zippy pm task templates"
```

---

### Task 4: Implement JSON-RPC Client, Supervisor, And Command Runner

**Files:**
- Modify: `python/zippy/pm_bridge.py`
- Modify: `python/zippy/cli_pm.py`
- Test: `pytests/test_python_pm_cli.py`

- [ ] **Step 1: Add tests for supervisor command and command routing**

Append these tests to `pytests/test_python_pm_cli.py`:

```python
def test_pm_supervisor_builds_zippy_rspm_daemon_command(tmp_path: Path) -> None:
    from zippy.pm_bridge import PmSupervisor

    runtime = PmRuntimeConfig(root_dir=tmp_path / ".zippy" / "rspm")
    supervisor = PmSupervisor(runtime)

    command = supervisor.daemon_command(Path("zippy.pm.toml"))

    assert command == [
        "zippy-rspm",
        "daemon",
        "run",
        "zippy.pm.toml",
        "127.0.0.1:27691",
        str(tmp_path / ".zippy" / "rspm" / "logs"),
        str(tmp_path / ".zippy" / "rspm" / "state"),
        str(tmp_path / ".zippy" / "rspm" / "run" / "zippy-rspm.sock"),
    ]


def test_pm_validate_delegates_to_runner(monkeypatch) -> None:
    calls: list[tuple[str, str, bool | None]] = []

    class FakeRunner:
        @classmethod
        def default(cls):
            return cls()

        def validate(self, path: Path) -> None:
            calls.append(("validate", str(path), None))

    monkeypatch.setattr("zippy.cli_pm.PmCommandRunner", FakeRunner)

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "validate", "-f", "custom.pm.toml"])

    assert result.exit_code == 0
    assert calls == [("validate", "custom.pm.toml", None)]


def test_pm_apply_delegates_to_runner(monkeypatch) -> None:
    calls: list[tuple[str, str, bool]] = []

    class FakeRunner:
        @classmethod
        def default(cls):
            return cls()

        def apply(self, path: Path, *, dry_run: bool = False) -> None:
            calls.append(("apply", str(path), dry_run))

    monkeypatch.setattr("zippy.cli_pm.PmCommandRunner", FakeRunner)

    runner = CliRunner()
    result = runner.invoke(main, ["pm", "apply", "-f", "custom.pm.toml", "--dry-run"])

    assert result.exit_code == 0
    assert calls == [("apply", "custom.pm.toml", True)]
```

- [ ] **Step 2: Implement task info and JSON-RPC client**

Append this code to `python/zippy/pm_bridge.py`:

```python
@dataclass(frozen=True)
class PmTaskInfo:
    """
    Task state returned by zippy-rspm.

    :param name: Task name.
    :type name: str
    :param status: Runtime status.
    :type status: str
    :param pid: OS process id.
    :type pid: int | None
    :param health: Health-check state.
    :type health: str | None
    :param restart_count: Restart count.
    :type restart_count: int
    :param cmd: Command string.
    :type cmd: str
    """

    name: str
    status: str = "defined"
    pid: int | None = None
    health: str | None = None
    restart_count: int = 0
    cmd: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PmTaskInfo":
        """
        Convert a JSON-RPC task payload into a task info object.

        :param payload: Response item.
        :type payload: dict[str, Any]
        :returns: Task info.
        :rtype: PmTaskInfo
        """

        return cls(
            name=str(payload["name"]),
            status=str(payload.get("status", "defined")),
            pid=payload.get("pid"),
            health=payload.get("health"),
            restart_count=int(payload.get("restart_count", 0)),
            cmd=str(payload.get("cmd", "")),
        )
```

Append this code to `python/zippy/pm_bridge.py`:

```python
class PmRpcClient:
    """
    Minimal JSON-RPC client for the local zippy-rspm daemon.

    :param runtime: Runtime configuration.
    :type runtime: PmRuntimeConfig
    """

    def __init__(self, runtime: PmRuntimeConfig):
        self.runtime = runtime
        self._next_id = 1

    def build_request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Build one JSON-RPC request."""

        request_params = dict(params or {})
        if self.runtime.token is not None:
            request_params["token"] = self.runtime.token
        request = {
            "jsonrpc": "2.0",
            "id": self._next_id,
            "method": method,
            "params": request_params,
        }
        self._next_id += 1
        return request

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send one request and return its result."""

        import json

        request = self.build_request(method, params)
        with socket.create_connection((self.runtime.host, self.runtime.port), timeout=5) as sock:
            sock.sendall(json.dumps(request).encode("utf-8") + b"\n")
            response = _read_json_line(sock)
        if "error" in response:
            raise RuntimeError(_rewrite_rspm_text(str(response["error"])))
        return response.get("result")

    def list_tasks(self) -> list[PmTaskInfo]:
        """List configured daemon tasks."""

        result = self.request("task.list")
        if not isinstance(result, list):
            raise RuntimeError("zippy-rspm returned invalid task list")
        return [PmTaskInfo.from_dict(item) for item in result]

    def apply_file(self, path: Path) -> list[PmTaskInfo]:
        """Apply a TOML config file."""

        result = self.request("config.apply", {"toml": path.read_text(encoding="utf-8")})
        if not isinstance(result, list):
            raise RuntimeError("zippy-rspm returned invalid apply result")
        return [PmTaskInfo.from_dict(item) for item in result]

    def task_action(self, method: str, task: str) -> PmTaskInfo:
        """Run a task-scoped action."""

        result = self.request(method, {"task": task})
        if not isinstance(result, dict):
            raise RuntimeError("zippy-rspm returned invalid task result")
        return PmTaskInfo.from_dict(result)

    def logs(self, task: str) -> str:
        """Read task logs."""

        result = self.request("task.logs", {"task": task})
        return str(result)

    def events(self) -> list[dict[str, Any]]:
        """List task events."""

        result = self.request("event.list")
        if not isinstance(result, list):
            raise RuntimeError("zippy-rspm returned invalid event list")
        return result


def _read_json_line(sock: socket.socket) -> dict[str, Any]:
    import json

    chunks: list[bytes] = []
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        if chunk == b"\n":
            break
        chunks.append(chunk)
    if not chunks:
        raise RuntimeError("zippy-rspm returned an empty response")
    return json.loads(b"".join(chunks).decode("utf-8"))


def _rewrite_rspm_text(message: str) -> str:
    return (
        message.replace(".rspm/", ".zippy/rspm/")
        .replace(".rspm", ".zippy/rspm")
        .replace("rspmd", DEFAULT_PM_DAEMON_NAME)
        .replace("rspm", DEFAULT_PM_DAEMON_NAME)
    )
```

- [ ] **Step 3: Implement supervisor and command runner**

Append this code to `python/zippy/pm_bridge.py`:

```python
class PmSupervisor:
    """
    Detached zippy-rspm sidecar supervisor.

    :param runtime: Runtime configuration.
    :type runtime: PmRuntimeConfig
    """

    def __init__(self, runtime: PmRuntimeConfig):
        self.runtime = runtime

    def client(self) -> PmRpcClient:
        """Create a JSON-RPC client."""

        return PmRpcClient(self.runtime)

    def daemon_command(self, config_path: Path) -> list[str]:
        """Build the detached daemon command."""

        command = [
            self.runtime.binary_path,
            "daemon",
            "run",
            str(config_path),
            f"{self.runtime.host}:{self.runtime.port}",
            str(self.runtime.log_dir),
            str(self.runtime.state_dir),
            str(self.runtime.socket_path),
        ]
        if self.runtime.token is not None:
            command.extend(["--token", self.runtime.token])
        return command

    def ensure_daemon(self, config_path: Path) -> PmRpcClient:
        """Return a ready client, spawning zippy-rspm if needed."""

        if self._is_ready():
            return self.client()
        self._ensure_config_source(config_path)
        self._spawn_daemon(config_path)
        return self._wait_ready()

    def _is_ready(self) -> bool:
        try:
            self.client().list_tasks()
        except (OSError, RuntimeError):
            return False
        return True

    def _ensure_config_source(self, config_path: Path) -> None:
        applied_config = self.runtime.state_dir / "applied.toml"
        if config_path.exists() or applied_config.exists():
            return
        raise FileNotFoundError(
            f"missing config [{config_path}] and no applied config [{applied_config}]"
        )

    def _spawn_daemon(self, config_path: Path) -> None:
        self.runtime.log_dir.mkdir(parents=True, exist_ok=True)
        self.runtime.state_dir.mkdir(parents=True, exist_ok=True)
        self.runtime.run_dir.mkdir(parents=True, exist_ok=True)

        kwargs: dict[str, object] = {}
        if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP") and hasattr(subprocess, "DETACHED_PROCESS"):
            kwargs["creationflags"] = (
                subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            )
        else:
            kwargs["start_new_session"] = True

        stdout_path = self.runtime.log_dir / "zippy-rspm.stdout.log"
        stderr_path = self.runtime.log_dir / "zippy-rspm.stderr.log"
        with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
            process = subprocess.Popen(
                self.daemon_command(config_path),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                **kwargs,
            )
        (self.runtime.state_dir / "zippy-rspm.pid").write_text(str(process.pid), encoding="utf-8")

    def _wait_ready(self) -> PmRpcClient:
        deadline = time.monotonic() + self.runtime.startup_timeout_sec
        while True:
            if self._is_ready():
                return self.client()
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"{DEFAULT_PM_DAEMON_NAME} did not become ready at "
                    f"[{self.runtime.host}:{self.runtime.port}]"
                )
            time.sleep(0.1)


class PmCommandRunner:
    """
    High-level command runner used by the Click layer.

    :param supervisor: Sidecar supervisor.
    :type supervisor: PmSupervisor
    """

    def __init__(self, supervisor: PmSupervisor):
        self.supervisor = supervisor

    @classmethod
    def default(cls) -> "PmCommandRunner":
        """Build a command runner using default zippy runtime settings."""

        return cls(PmSupervisor(PmRuntimeConfig.default()))

    def validate(self, path: Path) -> None:
        """Validate a config by parsing it locally."""

        config = load_pm_config(path)
        tasks = config.get("tasks", {})
        if not isinstance(tasks, dict):
            raise ValueError("[tasks] must be a TOML table")
        print(f"valid [zippy-local] tasks=[{len(tasks)}]")

    def apply(self, path: Path, *, dry_run: bool = False) -> None:
        """Apply a config through zippy-rspm."""

        if dry_run:
            config = load_pm_config(path)
            tasks = config.get("tasks", {})
            task_count = len(tasks) if isinstance(tasks, dict) else 0
            print(f"apply dry-run [zippy-local] tasks={task_count}")
            return
        client = self.supervisor.ensure_daemon(path)
        tasks = client.apply_file(path)
        print(f"applied [zippy-local] tasks={len(tasks)}")

    def list_tasks(self) -> list[PmTaskInfo]:
        """List tasks through zippy-rspm."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).list_tasks()

    def start(self, task: str) -> PmTaskInfo:
        """Start a task."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).task_action(
            "task.start",
            task,
        )

    def stop(self, task: str) -> PmTaskInfo:
        """Stop a task."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).task_action(
            "task.stop",
            task,
        )

    def restart(self, task: str) -> PmTaskInfo:
        """Restart a task."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).task_action(
            "task.restart",
            task,
        )

    def logs(self, task: str) -> str:
        """Read task logs."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).logs(task)

    def events(self) -> list[dict[str, Any]]:
        """List task events."""

        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).events()
```

- [ ] **Step 4: Add lifecycle/status/log commands to `cli_pm.py`**

Append to `python/zippy/cli_pm.py`:

```python
@pm_group.command("status")
def status_pm() -> None:
    """Show zippy-rspm task status."""

    try:
        _print_tasks(PmCommandRunner.default().list_tasks())
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))


pm_group.add_command(status_pm, "ls")


@pm_group.command("start")
@click.argument("task")
def start_pm(task: str) -> None:
    """Start a local task."""

    _run_task_action("start", task)


@pm_group.command("stop")
@click.argument("task")
def stop_pm(task: str) -> None:
    """Stop a local task."""

    _run_task_action("stop", task)


@pm_group.command("restart")
@click.argument("task")
def restart_pm(task: str) -> None:
    """Restart a local task."""

    _run_task_action("restart", task)


@pm_group.command("logs")
@click.argument("task")
@click.option("--follow", is_flag=True, default=False)
@click.option("--lines", type=int, default=None)
def logs_pm(task: str, follow: bool, lines: int | None) -> None:
    """Show task logs."""

    if follow:
        cli_error("log follow is not implemented in the first bridge version")
    try:
        text = PmCommandRunner.default().logs(task)
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    if lines is not None:
        text = "\n".join(text.splitlines()[-lines:])
    click.echo(text)


@pm_group.command("events")
def events_pm() -> None:
    """Show zippy-rspm events."""

    try:
        events = PmCommandRunner.default().events()
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    for event in events:
        click.echo(event)


@pm_group.command("doctor")
def doctor_pm() -> None:
    """Show local zippy-rspm diagnostics."""

    from .pm_bridge import PmRuntimeConfig

    runtime = PmRuntimeConfig.default()
    click.echo(f"daemon: zippy-rspm")
    click.echo(f"endpoint: {runtime.endpoint}")
    click.echo(f"log_dir: {runtime.log_dir}")
    click.echo(f"state_dir: {runtime.state_dir}")
    click.echo(f"run_dir: {runtime.run_dir}")
    click.echo(f"socket_path: {runtime.socket_path}")


def _run_task_action(action: str, task: str) -> None:
    runner = PmCommandRunner.default()
    try:
        if action == "start":
            info = runner.start(task)
        elif action == "stop":
            info = runner.stop(task)
        elif action == "restart":
            info = runner.restart(task)
        else:
            raise ValueError(f"unsupported task action [{action}]")
    except (OSError, RuntimeError, ValueError) as error:
        cli_error(str(error))
    click.echo(f"{action} [{info.name}] status=[{info.status}] pid=[{info.pid}]")


def _print_tasks(tasks: list[object]) -> None:
    click.echo("TASK                      STATUS        PID      HEALTH     RESTARTS  COMMAND")
    for task in tasks:
        click.echo(
            f"{task.name:<25} "
            f"{task.status:<13} "
            f"{str(task.pid or ''):<8} "
            f"{str(task.health or ''):<10} "
            f"{task.restart_count:<9} "
            f"{task.cmd}"
        )
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
uv run pytest pytests/test_python_pm_cli.py -q
```

Expected: all pm bridge tests pass.

- [ ] **Step 6: Commit bridge runtime**

Run:

```bash
git add python/zippy/pm_bridge.py python/zippy/cli_pm.py pytests/test_python_pm_cli.py
git commit -m "feat: add zippy-rspm bridge runner"
```

---

### Task 5: Add Typing Config And Full CLI Verification

**Files:**
- Modify: `pyproject.toml`
- Test: Python CLI tests

- [ ] **Step 1: Add new CLI modules to mypy file list**

Modify `pyproject.toml` under `[tool.mypy] files` to include:

```toml
    "python/zippy/cli_pm.py",
    "python/zippy/pm_bridge.py",
```

- [ ] **Step 2: Run formatting**

Run:

```bash
uv run black python/zippy/cli.py python/zippy/cli_pm.py python/zippy/pm_bridge.py pytests/test_python_pm_cli.py pytests/test_python_cli.py
```

Expected: files are formatted without errors.

- [ ] **Step 3: Run lint**

Run:

```bash
uv run ruff check python/zippy/cli.py python/zippy/cli_pm.py python/zippy/pm_bridge.py pytests/test_python_pm_cli.py pytests/test_python_cli.py
```

Expected: no lint errors.

- [ ] **Step 4: Run focused pytest**

Run:

```bash
uv run pytest pytests/test_python_pm_cli.py pytests/test_python_cli.py -q
```

Expected: tests pass. If unrelated existing tests in `pytests/test_python_cli.py` fail because of the dirty worktree or local environment, rerun only the pm-related tests and record the exact failure.

- [ ] **Step 5: Run mypy if available**

Run:

```bash
uv run mypy
```

Expected: mypy passes for the configured Python files.

- [ ] **Step 6: Commit config and verification fixes**

Run:

```bash
git add pyproject.toml python/zippy/cli.py python/zippy/cli_pm.py python/zippy/pm_bridge.py pytests/test_python_pm_cli.py pytests/test_python_cli.py
git commit -m "test: verify zippy pm cli bridge"
```

---

### Task 6: Smoke Test With Optional zippy-rspm Binary

**Files:**
- No source changes expected.
- If source changes are required, create a small follow-up patch and explain why.

- [ ] **Step 1: Check whether `zippy-rspm` is available**

Run:

```bash
command -v zippy-rspm
```

Expected: prints a path. If it is missing, do not install anything silently; record that live runtime smoke is blocked by missing `zippy-rspm`.

- [ ] **Step 2: Validate generated config in a temporary directory**

Run:

```bash
tmpdir="$(mktemp -d)"
cd "$tmpdir"
python -m zippy pm add master
python -m zippy pm validate
python -m zippy pm apply --dry-run
```

Expected:

```text
added [master]
valid [zippy-local] tasks=[1]
apply dry-run [zippy-local] tasks=1
```

- [ ] **Step 3: Run live daemon smoke only if safe**

Run only if `zippy-rspm` is available and no existing process is using port `27691`:

```bash
tmpdir="$(mktemp -d)"
cd "$tmpdir"
python -m zippy pm add master
python -m zippy pm apply
python -m zippy pm status
python -m zippy pm stop master
```

Expected: `apply` starts or reaches `zippy-rspm`, `status` lists `master`, and `stop master` returns a task status. If this fails because `zippy-rspm` is not packaged yet, record the exact error and keep the bridge code tested through unit tests.

- [ ] **Step 4: Final status check**

Run:

```bash
git status --short
```

Expected: only files intentionally changed by this plan are modified or the worktree is clean except pre-existing unrelated changes.

---

## Self-Review

Spec coverage:

1. `zippy pm` commands: Tasks 3 and 4 register `validate/apply/add/start/stop/restart/status/ls/logs/events/doctor`.
2. Default `zippy.pm.toml`, `zippy-rspm`, `.zippy/rspm/`: Tasks 1 and 2 test and implement constants.
3. zippy task templates: Tasks 1 through 3 cover `master`, `gateway`, and `custom`.
4. Bridge rather than vendoring: Task 4 implements a protocol-level JSON-RPC adapter and detached `zippy-rspm` supervisor.
5. No first-version multi-node: No task introduces `[nodes]` or remote fanout.
6. Verification: Tasks 5 and 6 cover formatting, lint, pytest, mypy, and optional runtime smoke.

Placeholder scan: the plan contains no unresolved placeholder markers and no unspecified implementation steps.

Type consistency: `PmRuntimeConfig`, `PmTaskInfo`, `PmRpcClient`, `PmSupervisor`, and `PmCommandRunner` are introduced before later tasks use them. CLI imports match the module names in the file structure.
