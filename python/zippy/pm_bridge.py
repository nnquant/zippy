"""
Helpers for describing local zippy process-manager tasks.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import time
import tomllib
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

DEFAULT_PM_CONFIG_FILE = "zippy.pm.toml"
DEFAULT_PM_DAEMON_NAME = "zippy-rspm"
DEFAULT_PM_ROOT = Path(".zippy") / "rspm"
DEFAULT_PM_HOST = "127.0.0.1"
DEFAULT_PM_PORT = 27691
DEFAULT_MASTER_URI = "tcp://127.0.0.1:17690"
DEFAULT_PM_CONNECT_TIMEOUT = 5.0
DEFAULT_PM_RPC_TIMEOUT = 120.0
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


@dataclass(frozen=True)
class PmTaskInfo:
    """
    Structured task state returned by the zippy process-manager daemon.

    :param name: Task name from the process-manager config.
    :type name: str
    :param status: Runtime lifecycle status.
    :type status: str
    :param pid: Current process id if the task is running.
    :type pid: int | None
    """

    name: str
    task_id: int = 0
    run_mode: str = ""
    pid: int | None = None
    status: str = "defined"
    health: str | None = None
    started_at: str | None = None
    stopped_at: str | None = None
    uptime_ms: int | None = None
    cpu_percent: float | None = None
    memory_bytes: int | None = None
    restart_count: int = 0
    last_exit_code: int | None = None
    cwd: str | None = None
    cmd: str = ""
    dependencies: list[str] | None = None
    dependents: list[str] | None = None
    schedule_state: str | None = None
    display_timezone: str | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "PmTaskInfo":
        """
        Build task info from a JSON-RPC result object.

        :param payload: JSON object returned by zippy-rspm.
        :type payload: collections.abc.Mapping[str, typing.Any]
        :returns: Structured task info.
        :rtype: PmTaskInfo
        """
        return cls(
            task_id=int(payload.get("task_id", 0)),
            name=str(payload["name"]),
            run_mode=str(payload.get("run_mode", "")),
            pid=payload.get("pid"),
            status=str(payload.get("status", "defined")),
            health=payload.get("health"),
            started_at=payload.get("started_at"),
            stopped_at=payload.get("stopped_at"),
            uptime_ms=payload.get("uptime_ms"),
            cpu_percent=payload.get("cpu_percent"),
            memory_bytes=payload.get("memory_bytes"),
            restart_count=int(payload.get("restart_count", 0)),
            last_exit_code=payload.get("last_exit_code"),
            cwd=payload.get("cwd"),
            cmd=str(payload.get("cmd", "")),
            dependencies=list(payload.get("dependencies", [])),
            dependents=list(payload.get("dependents", [])),
            schedule_state=payload.get("schedule_state"),
            display_timezone=payload.get("display_timezone"),
        )


@dataclass
class PmRpcClient:
    """
    Synchronous JSON-RPC client for the local zippy-rspm daemon.

    :param host: Daemon TCP host.
    :type host: str
    :param port: Daemon TCP port.
    :type port: int
    :param token: Optional daemon auth token.
    :type token: str | None
    """

    host: str = DEFAULT_PM_HOST
    port: int = DEFAULT_PM_PORT
    token: str | None = None
    timeout: float = DEFAULT_PM_RPC_TIMEOUT
    connect_timeout: float = DEFAULT_PM_CONNECT_TIMEOUT
    _next_id: int = 1

    def request(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        """
        Send one JSON-RPC request and return the decoded result.

        :param method: RPC method name.
        :type method: str
        :param params: RPC params object.
        :type params: collections.abc.Mapping[str, typing.Any] | None
        :returns: Decoded ``result`` field.
        :rtype: typing.Any
        :raises RuntimeError: If the daemon returns a JSON-RPC error.
        """
        request_params = dict(params or {})
        if self.token is not None:
            request_params["token"] = self.token
        request_id = self._next_id
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": request_params,
        }
        self._next_id += 1

        with socket.create_connection((self.host, self.port), timeout=self.connect_timeout) as sock:
            if hasattr(sock, "settimeout"):
                sock.settimeout(self.timeout)
            sock.sendall(json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n")
            response = _read_json_line(sock)

        error = response.get("error")
        if error is not None:
            if isinstance(error, Mapping):
                raise RuntimeError(
                    f"zippy-rspm RPC method=[{method}] endpoint=[{self.host}:{self.port}] "
                    f"error=[{error.get('code')}]: {error.get('message')}"
                )
            raise RuntimeError(
                f"zippy-rspm RPC method=[{method}] endpoint=[{self.host}:{self.port}] "
                f"error=[{error}]"
            )
        if response.get("id") != request_id:
            raise RuntimeError(
                f"zippy-rspm RPC response id mismatch method=[{method}] "
                f"endpoint=[{self.host}:{self.port}] expected=[{request_id}] "
                f"actual=[{response.get('id')}]"
            )
        if "result" not in response:
            raise RuntimeError(
                f"zippy-rspm RPC response missing result method=[{method}] "
                f"endpoint=[{self.host}:{self.port}]"
            )
        return response.get("result")

    def list_tasks(self) -> list[PmTaskInfo]:
        """
        List configured tasks.

        :returns: Task info list.
        :rtype: list[PmTaskInfo]
        """
        return _task_info_list(self.request("task.list"))

    def apply_file(self, path: str | Path) -> list[PmTaskInfo]:
        """
        Apply a process-manager TOML file through the daemon.

        :param path: Config file path.
        :type path: str | pathlib.Path
        :returns: Task info list after applying the config.
        :rtype: list[PmTaskInfo]
        """
        text = Path(path).read_text(encoding="utf-8")
        return _task_info_list(self.request("config.apply", {"toml": _rewrite_rspm_text(text)}))

    def start(self, name: str) -> PmTaskInfo:
        """Start one task."""
        return _task_info(self.request("task.start", {"task": name}))

    def stop(self, name: str) -> PmTaskInfo:
        """Stop one task."""
        return _task_info(self.request("task.stop", {"task": name}))

    def restart(self, name: str) -> PmTaskInfo:
        """Restart one task."""
        return _task_info(self.request("task.restart", {"task": name}))

    def start_all(self) -> list[PmTaskInfo]:
        """Start all tasks."""
        return _task_info_list(self.request("task.start_all"))

    def stop_all(self) -> list[PmTaskInfo]:
        """Stop all tasks."""
        return _task_info_list(self.request("task.stop_all"))

    def restart_all(self) -> list[PmTaskInfo]:
        """Restart all tasks."""
        self.stop_all()
        return self.start_all()

    def logs(self, name: str) -> str:
        """Read one task log."""
        result = self.request("task.logs", {"task": name})
        return str(result)

    def events(self) -> list[dict[str, Any]]:
        """List daemon events."""
        result = self.request("event.list")
        if not isinstance(result, list):
            raise TypeError("event.list result must be a list")
        return [dict(item) for item in result]


def _read_json_line(sock: socket.socket) -> dict[str, Any]:
    """
    Read a single line-delimited JSON response from a socket.

    :param sock: Connected socket.
    :type sock: socket.socket
    :returns: Decoded JSON object.
    :rtype: dict[str, typing.Any]
    :raises ConnectionError: If no response is returned.
    """
    chunks: list[bytes] = []
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        if chunk == b"\n":
            break
        chunks.append(chunk)
    if not chunks:
        raise ConnectionError("zippy-rspm returned empty response")
    response = json.loads(b"".join(chunks).decode("utf-8"))
    if not isinstance(response, dict):
        raise TypeError("zippy-rspm response must be a JSON object")
    return response


def _rewrite_rspm_text(text: str) -> str:
    """
    Rewrite zippy-local TOML into the subset consumed by rspm.

    The zippy bridge stores local runtime metadata under ``[runtime]``. rspm
    treats the task config as the source of truth, so that zippy-only table is
    stripped before ``config.apply`` while all task/default sections are kept.

    :param text: zippy process-manager TOML text.
    :type text: str
    :returns: TOML text suitable for rspm ``config.apply``.
    :rtype: str
    """
    tomllib.loads(text)
    lines = text.splitlines()
    rewritten: list[str] = []
    skipping_runtime = False
    for line in lines:
        stripped = line.strip()
        if stripped == "[runtime]":
            skipping_runtime = True
            continue
        if skipping_runtime and stripped.startswith("[") and stripped.endswith("]"):
            skipping_runtime = False
        if not skipping_runtime:
            rewritten.append(line)
    return "\n".join(rewritten).rstrip() + "\n"


@dataclass(frozen=True)
class PmSupervisor:
    """
    Ensure a detached zippy-rspm sidecar is ready for CLI commands.

    :param config: Runtime paths and endpoint.
    :type config: PmRuntimeConfig
    :param token: Optional local auth token.
    :type token: str | None
    :param startup_timeout: Seconds to wait for daemon readiness.
    :type startup_timeout: float
    """

    config: PmRuntimeConfig
    token: str | None = None
    startup_timeout: float = 10.0

    def client(self) -> PmRpcClient:
        """
        Build a JSON-RPC client for the configured daemon endpoint.

        :returns: Configured RPC client.
        :rtype: PmRpcClient
        """
        return PmRpcClient(self.config.host, self.config.port, self.token)

    def daemon_command(self, config_path: str | Path) -> list[str]:
        """
        Build the detached zippy-rspm daemon command.

        :param config_path: Process-manager config path.
        :type config_path: str | pathlib.Path
        :returns: Command argv.
        :rtype: list[str]
        """
        command = [
            self.config.daemon_name,
            "daemon",
            "run",
            str(config_path),
            self.config.endpoint,
            str(self.config.log_dir),
            str(self.config.state_dir),
            str(self.config.socket_path),
        ]
        if self.token is not None:
            command.extend(["--token", self.token])
        return command

    def ensure_daemon(self, config_path: str | Path) -> PmRpcClient:
        """
        Return a ready client, spawning the detached daemon if needed.

        :param config_path: Process-manager config path.
        :type config_path: str | pathlib.Path
        :returns: Ready RPC client.
        :rtype: PmRpcClient
        """
        if self._is_ready():
            return self.client()
        self._ensure_config_source(config_path)
        self._spawn_daemon(config_path)
        return self._wait_ready()

    def _is_ready(self) -> bool:
        try:
            self.client().list_tasks()
        except TimeoutError:
            raise
        except (ConnectionError, OSError):
            return False
        return True

    def _ensure_config_source(self, config_path: str | Path) -> None:
        config = Path(config_path)
        applied_config = self.config.state_dir / "applied.toml"
        if config.exists() or applied_config.exists():
            return
        raise FileNotFoundError(
            f"missing config [{config}] and no applied config [{applied_config}]"
        )

    def _spawn_daemon(self, config_path: str | Path) -> None:
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self.config.socket_path.parent.mkdir(parents=True, exist_ok=True)

        kwargs: dict[str, Any] = {}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(
                subprocess, "DETACHED_PROCESS", 0
            )
        else:
            kwargs["start_new_session"] = True

        with (
            (self.config.log_dir / "zippy-rspm.stdout.log").open("ab") as stdout,
            (self.config.log_dir / "zippy-rspm.stderr.log").open("ab") as stderr,
        ):
            process = subprocess.Popen(
                self.daemon_command(config_path),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                **kwargs,
            )
        (self.config.state_dir / "zippy-rspm.pid").write_text(
            str(process.pid),
            encoding="utf-8",
        )

    def _wait_ready(self) -> PmRpcClient:
        deadline = time.monotonic() + self.startup_timeout
        while True:
            if self._is_ready():
                return self.client()
            if time.monotonic() >= deadline:
                raise TimeoutError(f"zippy-rspm did not become ready at [{self.config.endpoint}]")
            time.sleep(0.1)


@dataclass(frozen=True)
class PmCommandRunner:
    """
    High-level command runner used by ``zippy pm`` CLI commands.

    :param supervisor: Daemon supervisor.
    :type supervisor: PmSupervisor
    """

    supervisor: PmSupervisor

    @classmethod
    def default(cls) -> "PmCommandRunner":
        """
        Build the default zippy-local command runner.

        :returns: Default runner.
        :rtype: PmCommandRunner
        """
        return cls(PmSupervisor(PmRuntimeConfig.default()))

    def validate(self, path: str | Path) -> str:
        """
        Validate local TOML without starting the daemon.

        :param path: Config file path.
        :type path: str | pathlib.Path
        :returns: Human-readable validation summary.
        :rtype: str
        """
        project_name, task_count = _config_summary(path)
        return f"valid [{project_name}] tasks=[{task_count}]"

    def apply(self, path: str | Path, *, dry_run: bool = False) -> str:
        """
        Apply a TOML config, or validate locally for dry-run.

        :param path: Config file path.
        :type path: str | pathlib.Path
        :param dry_run: Whether to skip daemon application.
        :type dry_run: bool
        :returns: Human-readable apply summary.
        :rtype: str
        """
        project_name, task_count = _config_summary(path)
        if dry_run:
            return f"apply dry-run [{project_name}] tasks={task_count}"
        tasks = self.supervisor.ensure_daemon(path).apply_file(path)
        return f"apply [{project_name}] tasks={len(tasks)}"

    def list_tasks(self) -> list[PmTaskInfo]:
        """List tasks through the default daemon."""
        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).list_tasks()

    def status(self) -> list[PmTaskInfo]:
        """Return task status through the default daemon."""
        return self.list_tasks()

    def start(self, name: str | None = None) -> list[PmTaskInfo]:
        """Start one task or all tasks."""
        client = self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE))
        if name is None or name == "all":
            return client.start_all()
        return [client.start(_resolve_task_target(client, name))]

    def stop(self, name: str | None = None) -> list[PmTaskInfo]:
        """Stop one task or all tasks."""
        client = self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE))
        if name is None or name == "all":
            return client.stop_all()
        return [client.stop(_resolve_task_target(client, name))]

    def restart(self, name: str | None = None) -> list[PmTaskInfo]:
        """Restart one task or all tasks."""
        client = self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE))
        if name is None or name == "all":
            return client.restart_all()
        return [client.restart(_resolve_task_target(client, name))]

    def logs(self, name: str | None = None, *, lines: int | None = None) -> str:
        """Read task logs through the default daemon."""
        client = self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE))
        if name is None:
            tasks = client.list_tasks()
            text = "\n".join(f"==> {task.name} <==\n{client.logs(task.name)}" for task in tasks)
        else:
            text = client.logs(_resolve_task_target(client, name))
        if lines is None:
            return text
        return "\n".join(text.splitlines()[-lines:])

    def events(self) -> list[dict[str, Any]]:
        """List daemon events through the default daemon."""
        return self.supervisor.ensure_daemon(Path(DEFAULT_PM_CONFIG_FILE)).events()

    def doctor(self) -> dict[str, str]:
        """Return local daemon diagnostic fields."""
        config = self.supervisor.config
        return {
            "daemon": config.daemon_name,
            "endpoint": config.endpoint,
            "log_dir": str(config.log_dir),
            "state_dir": str(config.state_dir),
            "run_dir": str(config.run_dir),
            "socket_path": str(config.socket_path),
        }


def _task_info(payload: Any) -> PmTaskInfo:
    if not isinstance(payload, Mapping):
        raise TypeError("task result must be a JSON object")
    return PmTaskInfo.from_dict(payload)


def _task_info_list(payload: Any) -> list[PmTaskInfo]:
    if not isinstance(payload, list):
        raise TypeError("task list result must be a list")
    return [_task_info(item) for item in payload]


def _resolve_task_target(client: PmRpcClient, target: str) -> str:
    if not target.isdecimal():
        return target
    task_id = int(target)
    for task in client.list_tasks():
        if task.task_id == task_id:
            return task.name
    raise ValueError(f"task_id [{task_id}] not found")


def _config_summary(path: str | Path) -> tuple[str, int]:
    config = read_pm_config(path)
    _validate_pm_config(config)
    project = _table(config.get("project", {}), "project")
    tasks = _table(config.get("tasks", {}), "tasks")
    return str(project.get("name", "zippy-local")), len(tasks)


def default_project_config() -> dict[str, object]:
    """
    Build the default zippy local process-manager project configuration.

    :returns: A deterministic config dictionary suitable for TOML serialization.
    :rtype: dict[str, object]
    """
    runtime = PmRuntimeConfig.default()
    return {
        "project": {"name": "zippy-local"},
        "defaults": {
            "restart": "on-failure",
            "restart_delay": "2s",
            "max_restarts": 5,
        },
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
    config = read_pm_config(config_path)
    config.setdefault("project", {"name": "zippy-local"})
    config.setdefault("defaults", default_project_config()["defaults"])
    config.setdefault("runtime", default_project_config()["runtime"])
    config.setdefault("tasks", {})
    return config


def read_pm_config(path: str | Path) -> dict[str, object]:
    """
    Read an existing process-manager TOML config.

    :param path: Config file path.
    :type path: str | pathlib.Path
    :returns: Parsed config dictionary.
    :rtype: dict[str, object]
    :raises FileNotFoundError: If the config file does not exist.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"missing config [{config_path}]")
    with config_path.open("rb") as handle:
        return tomllib.load(handle)


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
    tasks = _table(config.get("tasks", {}), "tasks")

    root_values, top_level_tables = _partition_table(config)
    if root_values:
        _append_root_values(lines, root_values)

    ordered_tables = ["project", "defaults", "runtime"]
    ordered_tables.extend(
        sorted(key for key in top_level_tables if key not in {*ordered_tables, "tasks"})
    )
    for table_name in ordered_tables:
        if table_name in top_level_tables:
            table = _table(top_level_tables[table_name], table_name)
            _append_table_recursive(lines, [table_name], table)

    for task_name in sorted(tasks):
        task = _table(tasks[task_name], f"tasks.{task_name}")
        _append_task_table_recursive(lines, ["tasks", task_name], task)

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


def _validate_pm_config(config: Mapping[str, object]) -> None:
    tasks = _table(config.get("tasks", {}), "tasks")
    if not tasks:
        raise ValueError("at least one [tasks.<name>] section is required")
    for task_name, task_value in tasks.items():
        task = _table(task_value, f"tasks.{task_name}")
        cmd = task.get("cmd")
        if not isinstance(cmd, str) or not cmd:
            raise ValueError(f"task [{task_name}] must define non-empty cmd")
        args = task.get("args", [])
        if not isinstance(args, list) or any(not isinstance(arg, str) for arg in args):
            raise ValueError(f"task [{task_name}] args must be a list of strings")
        depends_on = task.get("depends_on", [])
        if not isinstance(depends_on, list) or any(not isinstance(dep, str) for dep in depends_on):
            raise ValueError(f"task [{task_name}] depends_on must be a list of strings")
        env = task.get("env")
        if env is not None:
            _validated_env(_table(env, f"tasks.{task_name}.env"))


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


def _append_root_values(lines: list[str], values: dict[str, object]) -> None:
    for key in sorted(values):
        lines.append(f"{_format_toml_key(key)} = {_format_toml_value(values[key])}")


def _append_table_recursive(
    lines: list[str],
    segments: list[str],
    values: dict[str, object],
) -> None:
    scalar_values, nested_tables = _partition_table(values)
    _append_table(lines, segments, scalar_values)
    for nested_name in sorted(nested_tables):
        nested_table = _table(nested_tables[nested_name], ".".join([*segments, nested_name]))
        _append_table_recursive(lines, [*segments, nested_name], nested_table)


def _append_task_table_recursive(
    lines: list[str],
    segments: list[str],
    values: dict[str, object],
) -> None:
    scalar_values, nested_tables = _partition_table(values)
    _append_table(lines, segments, scalar_values)
    for nested_name in sorted(nested_tables):
        nested_table = _table(nested_tables[nested_name], ".".join([*segments, nested_name]))
        if nested_name == "env":
            nested_table = dict(_validated_env(nested_table))
        _append_task_table_recursive(lines, [*segments, nested_name], nested_table)


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
    if isinstance(value, float):
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
