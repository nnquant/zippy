"""
Helpers for describing local zippy process-manager tasks.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import shlex
import tomllib
from typing import Iterable, Mapping
from urllib.parse import urlparse

DEFAULT_PM_CONFIG_FILE = "zippy.pm.toml"
DEFAULT_PM_DAEMON_NAME = "zippy-rspm"
DEFAULT_PM_ROOT = Path(".zippy") / "rspm"
DEFAULT_PM_HOST = "127.0.0.1"
DEFAULT_PM_PORT = 27691
DEFAULT_MASTER_URI = "tcp://127.0.0.1:17690"
_BARE_TOML_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PmRuntimeConfig:
    """
    Runtime paths and endpoint used by the local zippy process-manager bridge.

    :param root: Root directory for process-manager runtime files.
    :type root: pathlib.Path
    :param daemon_name: Stable daemon name used by zippy-managed rspm.
    :type daemon_name: str
    :param host: Host for the process-manager control endpoint.
    :type host: str
    :param port: Port for the process-manager control endpoint.
    :type port: int
    :param log_dir: Directory for managed process logs.
    :type log_dir: pathlib.Path
    :param state_dir: Directory for managed process state.
    :type state_dir: pathlib.Path
    :param run_dir: Directory for runtime sockets and pid files.
    :type run_dir: pathlib.Path
    :param socket_path: Unix socket path for the local process manager.
    :type socket_path: pathlib.Path
    :param endpoint: TCP control endpoint for the local process manager.
    :type endpoint: str
    """

    root: Path
    daemon_name: str
    host: str
    port: int
    log_dir: Path
    state_dir: Path
    run_dir: Path
    socket_path: Path
    endpoint: str

    @classmethod
    def default(cls) -> "PmRuntimeConfig":
        """
        Build the default zippy-scoped process-manager runtime configuration.

        :returns: Runtime configuration rooted under ``.zippy/rspm``.
        :rtype: PmRuntimeConfig
        """
        run_dir = DEFAULT_PM_ROOT / "run"
        return cls(
            root=DEFAULT_PM_ROOT,
            daemon_name=DEFAULT_PM_DAEMON_NAME,
            host=DEFAULT_PM_HOST,
            port=DEFAULT_PM_PORT,
            log_dir=DEFAULT_PM_ROOT / "logs",
            state_dir=DEFAULT_PM_ROOT / "state",
            run_dir=run_dir,
            socket_path=run_dir / f"{DEFAULT_PM_DAEMON_NAME}.sock",
            endpoint=f"{DEFAULT_PM_HOST}:{DEFAULT_PM_PORT}",
        )


def default_project_config() -> dict[str, object]:
    """
    Build the default zippy local process-manager project configuration.

    :returns: A deterministic config dictionary suitable for TOML serialization.
    :rtype: dict[str, object]
    """
    runtime = PmRuntimeConfig.default()
    return {
        "project": {"name": "zippy-local"},
        "runtime": {
            "daemon_name": runtime.daemon_name,
            "root": str(runtime.root),
            "log_dir": str(runtime.log_dir),
            "state_dir": str(runtime.state_dir),
            "run_dir": str(runtime.run_dir),
            "socket_path": str(runtime.socket_path),
            "endpoint": runtime.endpoint,
        },
        "tasks": {},
    }


def load_pm_config(path: str | Path) -> dict[str, object]:
    """
    Load a zippy process-manager TOML config, or return defaults if absent.

    :param path: Config file path.
    :type path: str | pathlib.Path
    :returns: Parsed config dictionary.
    :rtype: dict[str, object]
    """
    config_path = Path(path)
    if not config_path.exists():
        return default_project_config()
    with config_path.open("rb") as handle:
        config = tomllib.load(handle)
    config.setdefault("project", {"name": "zippy-local"})
    config.setdefault("runtime", default_project_config()["runtime"])
    config.setdefault("tasks", {})
    return config


def write_pm_config(path: str | Path, config: dict[str, object]) -> None:
    """
    Write a zippy process-manager config as deterministic TOML.

    :param path: Destination TOML file path.
    :type path: str | pathlib.Path
    :param config: Process-manager config dictionary.
    :type config: dict[str, object]
    """
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(format_pm_toml(config), encoding="utf-8")


def format_pm_toml(config: dict[str, object]) -> str:
    """
    Format a process-manager config dictionary as deterministic TOML.

    :param config: Process-manager config dictionary.
    :type config: dict[str, object]
    :returns: TOML text with stable table and key ordering.
    :rtype: str
    :raises TypeError: If an unsupported TOML value is encountered.
    """
    lines: list[str] = []
    project = _table(config.get("project", {}), "project")
    runtime = _table(config.get("runtime", {}), "runtime")
    tasks = _table(config.get("tasks", {}), "tasks")

    _append_table(lines, ["project"], project)
    _append_table(lines, ["runtime"], runtime)

    for task_name in sorted(tasks):
        task = _table(tasks[task_name], f"tasks.{task_name}")
        scalar_values, nested_tables = _partition_table(task)
        _append_table(lines, ["tasks", task_name], scalar_values)
        for nested_name in sorted(nested_tables):
            nested_table = _table(
                nested_tables[nested_name],
                f"tasks.{task_name}.{nested_name}",
            )
            if nested_name == "env":
                nested_table = _validated_env(nested_table)
            _append_table(
                lines,
                ["tasks", task_name, nested_name],
                nested_table,
            )

    return "\n".join(lines).rstrip() + "\n"


def master_task(master_uri: str = DEFAULT_MASTER_URI) -> dict[str, object]:
    """
    Build the managed zippy master task definition.

    :param master_uri: TCP master URI passed to ``zippy master run``.
    :type master_uri: str
    :returns: Task dictionary containing command, arguments, and TCP health check.
    :rtype: dict[str, object]
    :raises ValueError: If ``master_uri`` is not a TCP URI.
    """
    return {
        "cmd": "zippy",
        "args": ["master", "run", master_uri],
        "autostart": True,
        "health": {
            "type": "tcp",
            "address": _tcp_health_address(master_uri),
        },
    }


def gateway_task(
    master_uri: str = DEFAULT_MASTER_URI,
    *,
    endpoint: str | None = None,
    token: str | None = None,
) -> dict[str, object]:
    """
    Build the managed zippy gateway task definition.

    :param master_uri: Master URI used by the gateway process.
    :type master_uri: str
    :param endpoint: Optional gateway endpoint.
    :type endpoint: str | None
    :param token: Optional gateway access token.
    :type token: str | None
    :returns: Task dictionary that starts after the master task is healthy.
    :rtype: dict[str, object]
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


def custom_task(
    command: str,
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | Iterable[str] | None = None,
) -> dict[str, object]:
    """
    Build a custom managed task from an explicit shell-like command string.

    :param command: Command line to split with :func:`shlex.split`.
    :type command: str
    :param cwd: Optional working directory for the task.
    :type cwd: str | pathlib.Path | None
    :param env: Optional environment variables or ``KEY=VALUE`` entries for the task.
    :type env: collections.abc.Mapping[str, str] | collections.abc.Iterable[str] | None
    :returns: Task dictionary containing command and arguments.
    :rtype: dict[str, object]
    :raises ValueError: If the command is empty.
    """
    parts = shlex.split(command)
    if not parts:
        raise ValueError("custom command must not be empty")

    task: dict[str, object] = {"cmd": parts[0], "args": parts[1:]}
    if cwd is not None:
        task["cwd"] = str(cwd)
    if env:
        if isinstance(env, Mapping):
            task["env"] = _validated_env(env)
        elif isinstance(env, str):
            task["env"] = parse_env_entries([env])
        else:
            task["env"] = parse_env_entries(env)
    return task


def parse_env_entries(entries: Iterable[str]) -> dict[str, str]:
    """
    Parse repeated ``KEY=VALUE`` environment entries.

    Entries without ``=`` are treated as keys with an empty string value.

    :param entries: Environment entries from CLI flags.
    :type entries: collections.abc.Iterable[str]
    :returns: Mapping from environment variable name to value.
    :rtype: dict[str, str]
    :raises ValueError: If an entry has an empty key.
    """
    env: dict[str, str] = {}
    for entry in entries:
        key, separator, value = entry.partition("=")
        _validate_env_key(key)
        env[key] = value if separator else ""
    return env


def add_task(path: str | Path, name: str, task: dict[str, object]) -> dict[str, object]:
    """
    Add or replace a task in a zippy process-manager config file.

    :param path: Config file path.
    :type path: str | pathlib.Path
    :param name: Task name.
    :type name: str
    :param task: Task definition.
    :type task: dict[str, object]
    :returns: Updated config dictionary.
    :rtype: dict[str, object]
    :raises ValueError: If ``name`` is empty.
    """
    if not name:
        raise ValueError("task name must not be empty")

    config = load_pm_config(path)
    tasks = config.setdefault("tasks", {})
    if not isinstance(tasks, dict):
        raise TypeError("config tasks must be a table")
    tasks[name] = dict(task)
    write_pm_config(path, config)
    return config


def _tcp_health_address(uri: str) -> str:
    parsed = urlparse(uri)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError(f"invalid TCP master URI [{uri}]") from error
    if parsed.scheme != "tcp" or not parsed.hostname or port is None:
        raise ValueError(f"invalid TCP master URI [{uri}]")
    return f"{parsed.hostname}:{port}"


def _append_table(lines: list[str], segments: list[str], values: dict[str, object]) -> None:
    if not values:
        return
    if lines:
        lines.append("")
    lines.append(_format_toml_header(segments))
    for key in sorted(values):
        lines.append(f"{_format_toml_key(key)} = {_format_toml_value(values[key])}")


def _partition_table(
    values: dict[str, object],
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    scalar_values: dict[str, object] = {}
    nested_tables: dict[str, dict[str, object]] = {}
    for key, value in values.items():
        if isinstance(value, dict):
            nested_tables[key] = value
        else:
            scalar_values[key] = value
    return scalar_values, nested_tables


def _table(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"config {name} must be a table")
    return value


def _format_toml_value(value: object) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, Path):
        return json.dumps(str(value))
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_format_toml_value(item) for item in value) + "]"
    raise TypeError(f"unsupported TOML value type: {type(value).__name__}")


def _format_toml_header(segments: Iterable[str]) -> str:
    return "[" + ".".join(_format_toml_key(segment) for segment in segments) + "]"


def _format_toml_key(key: object) -> str:
    if not isinstance(key, str):
        raise TypeError("TOML keys must be strings")
    if _BARE_TOML_KEY_RE.fullmatch(key):
        return key
    return json.dumps(key)


def _validated_env(env: Mapping[str, object]) -> dict[str, str]:
    validated: dict[str, str] = {}
    for key, value in env.items():
        _validate_env_key(key)
        validated[key] = str(value)
    return validated


def _validate_env_key(key: object) -> None:
    if not isinstance(key, str) or not _ENV_KEY_RE.fullmatch(key):
        raise ValueError(f"invalid environment key [{key}]")
