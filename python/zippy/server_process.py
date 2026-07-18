"""
Detached process ownership and health probes for the single-node server CLI.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import shlex
import signal
import socket
import stat
import struct
import subprocess
import sys
import time
from typing import BinaryIO
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

if os.name == "posix":
    import fcntl


_STATE_SCHEMA_VERSION = 1
_READY_SCHEMA_VERSION = 5
_SUPPORTED_READY_SCHEMA_VERSIONS = frozenset({2, 3, 4, _READY_SCHEMA_VERSION})
_SERVER_ERROR_CODES = frozenset(
    {
        "INVALID_ARGUMENT",
        "UNAUTHENTICATED",
        "PERMISSION_DENIED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "CONFLICT",
        "STALE_PRODUCER_EPOCH",
        "SEQUENCE_GAP",
        "SCHEMA_MISMATCH",
        "OFFSET_EXPIRED",
        "RESOURCE_EXHAUSTED",
        "DEADLINE_EXCEEDED",
        "CANCELLED",
        "UNAVAILABLE",
        "DATA_CORRUPTION",
        "INTERNAL",
        "UNSUPPORTED_PLAN",
    }
)
_MAX_CONTROL_RESPONSE_BYTES = 4 * 1024 * 1024
_PROCESS_POLL_INTERVAL_SEC = 0.05


class _ServerProcessError(RuntimeError):
    pass


@dataclass(frozen=True)
class _ServerPaths:
    run_dir: Path
    log_dir: Path
    state_file: Path
    lock_file: Path
    ready_file: Path
    outcome_file: Path
    bootstrap_log: Path


def _default_run_dir() -> Path:
    configured = os.environ.get("ZIPPY_SERVER_RUN_DIR")
    return Path(configured).expanduser() if configured else Path.home() / ".zippy" / "run"


def _default_log_dir() -> Path:
    configured = os.environ.get("ZIPPY_SERVER_LOG_DIR")
    return Path(configured).expanduser() if configured else Path.home() / ".zippy" / "logs"


def _default_webui_url() -> str:
    configured = os.environ.get("ZIPPY_WEBUI_URL")
    if configured:
        return _normalize_webui_url(configured)
    host = os.environ.get("ZIPPY_WEBUI_HOST", "127.0.0.1")
    display_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    try:
        port = int(os.environ.get("ZIPPY_WEBUI_PORT", "17688"))
    except ValueError as error:
        raise _ServerProcessError("ZIPPY_WEBUI_PORT must be an integer") from error
    if not 1 <= port <= 65535:
        raise _ServerProcessError("ZIPPY_WEBUI_PORT must be between 1 and 65535")
    authority_host = f"[{display_host}]" if ":" in display_host else display_host
    return _normalize_webui_url(f"http://{authority_host}:{port}/")


def _normalize_webui_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise _ServerProcessError(f"invalid WebUI URL error=[{error}]") from error
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise _ServerProcessError("WebUI URL must use http or https with an explicit host")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise _ServerProcessError("WebUI URL must not contain credentials, query, or fragment")
    if parsed.path not in {"", "/"}:
        raise _ServerProcessError("WebUI URL must not contain a path")
    host = parsed.hostname
    authority_host = f"[{host}]" if ":" in host else host
    default_port = 443 if parsed.scheme == "https" else 80
    authority = authority_host if port in {None, default_port} else f"{authority_host}:{port}"
    path = (parsed.path or "/").rstrip("/") + "/"
    return urlunsplit((parsed.scheme, authority, path, "", ""))


def _instance_key(control_endpoint: str) -> str:
    return hashlib.sha256(control_endpoint.encode("utf-8")).hexdigest()[:16]


def _server_paths(
    control_endpoint: str,
    *,
    run_dir: Path | None,
    log_dir: Path | None,
) -> _ServerPaths:
    effective_run_dir = (run_dir or _default_run_dir()).expanduser().resolve()
    effective_log_dir = (log_dir or _default_log_dir()).expanduser().resolve()
    key = _instance_key(control_endpoint)
    state_file = effective_run_dir / f"server-{key}.json"
    return _ServerPaths(
        run_dir=effective_run_dir,
        log_dir=effective_log_dir,
        state_file=state_file,
        lock_file=effective_run_dir / f"server-{key}.lock",
        ready_file=state_file,
        outcome_file=effective_run_dir / f"server-{key}.outcome.json",
        bootstrap_log=effective_log_dir / f"server-{key}.log",
    )


def _prepare_owner_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise _ServerProcessError(f"server directory is not a real directory path=[{path}]")
        if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
            raise _ServerProcessError(
                f"server directory is not owned by current user path=[{path}]"
            )
        os.chmod(path, 0o700)
    except OSError as error:
        raise _ServerProcessError(
            f"prepare owner-only server directory path=[{path}] error=[{error}]"
        ) from error


def _open_regular_file(path: Path, flags: int, mode: int) -> int:
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags | no_follow, mode)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise _ServerProcessError(f"server file is not regular path=[{path}]")
        if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
            raise _ServerProcessError(f"server file is not owned by current user path=[{path}]")
        os.fchmod(fd, mode)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _acquire_launch_lock(path: Path) -> int:
    fd = _try_acquire_launch_lock(path)
    if fd is None:
        raise _ServerProcessError(f"server is already running or starting lock=[{path}]")
    return fd


def _try_acquire_launch_lock(path: Path) -> int | None:
    fd = _open_regular_file(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        os.close(fd)
        if error.errno in {errno.EACCES, errno.EAGAIN}:
            return None
        raise _ServerProcessError(f"acquire server lock path=[{path}] error=[{error}]") from error
    return fd


def _lock_is_held(path: Path) -> bool:
    if os.name != "posix" or not path.exists():
        return False
    try:
        fd = _open_regular_file(path, os.O_RDWR, 0o600)
    except (OSError, _ServerProcessError):
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                return True
            return False
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, payload: dict[str, object], nonce: str) -> None:
    temporary = path.with_name(f".{path.name}.{nonce}.tmp")
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd = _open_regular_file(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def _read_json_file(path: Path) -> dict[str, object]:
    try:
        fd = _open_regular_file(path, os.O_RDONLY, 0o600)
        with os.fdopen(fd, "r", encoding="utf-8", closefd=True) as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise _ServerProcessError(f"read server state path=[{path}] error=[{error}]") from error
    if not isinstance(payload, dict):
        raise _ServerProcessError(f"server state must be a JSON object path=[{path}]")
    return dict(payload)


def _process_identity(pid: int) -> dict[str, object]:
    identity: dict[str, object] = {}
    boot_id_path = Path("/proc/sys/kernel/random/boot_id")
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        identity["boot_id"] = boot_id_path.read_text(encoding="utf-8").strip()
        stat_line = stat_path.read_text(encoding="utf-8").strip()
        suffix = stat_line[stat_line.rfind(")") + 2 :].split()
        identity["start_ticks"] = suffix[19]
    except (OSError, IndexError):
        pass
    return identity


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").strip()
        process_state = stat_line[stat_line.rfind(")") + 2 :].split()[0]
        if process_state == "Z":
            return False
    except (OSError, IndexError):
        pass
    return True


def _process_matches(pid: int, expected: object) -> bool:
    if not _pid_exists(pid):
        return False
    if not isinstance(expected, dict):
        return False
    if not expected.get("boot_id") or not expected.get("start_ticks"):
        return False
    actual = _process_identity(pid)
    for name in ("boot_id", "start_ticks"):
        expected_value = expected.get(name)
        if expected_value is not None and actual.get(name) != expected_value:
            return False
    return True


def _cleanup_state_if_owned(state_file: Path, *, pid: int, launch_nonce: str) -> None:
    try:
        state = _read_json_file(state_file)
    except (OSError, _ServerProcessError):
        return
    if state.get("pid") == pid and state.get("launch_nonce") == launch_nonce:
        try:
            state_file.unlink()
        except OSError:
            pass


def _publish_child_shutdown_outcome(*, clean: bool, error_code: str | None) -> None:
    outcome_path = os.environ.get("ZIPPY_SERVER_OUTCOME_FILE")
    launch_nonce = os.environ.get("ZIPPY_SERVER_LAUNCH_NONCE")
    if not outcome_path or not launch_nonce:
        return
    payload: dict[str, object] = {
        "schema_version": _STATE_SCHEMA_VERSION,
        "launch_nonce": launch_nonce,
        "pid": os.getpid(),
        "clean": clean,
        "stopped_at": datetime.now(UTC).isoformat(),
    }
    if error_code:
        payload["error_code"] = error_code
    _atomic_write_json(Path(outcome_path), payload, launch_nonce)


def _exception_error_code(error: BaseException) -> str:
    code = getattr(error, "zippy_error_code", None)
    return code if isinstance(code, str) and code in _SERVER_ERROR_CODES else "runtime_error"


def _read_shutdown_outcome(
    outcome_file: Path,
    *,
    pid: int,
    launch_nonce: object,
) -> dict[str, object] | None:
    try:
        outcome = _read_json_file(outcome_file)
    except FileNotFoundError:
        return None
    if outcome.get("schema_version") != _STATE_SCHEMA_VERSION:
        raise _ServerProcessError("server shutdown outcome has unsupported schema")
    if outcome.get("pid") != pid or outcome.get("launch_nonce") != launch_nonce:
        raise _ServerProcessError("server shutdown outcome does not match process identity")
    if not isinstance(outcome.get("clean"), bool):
        raise _ServerProcessError("server shutdown outcome has an invalid clean flag")
    error_code = outcome.get("error_code")
    if error_code is not None and not isinstance(error_code, str):
        raise _ServerProcessError("server shutdown outcome has an invalid error code")
    clean = outcome["clean"]
    valid_error_codes = (
        {None, "startup_aborted"}
        if clean
        else {
            "runtime_error",
            "bootstrap_error",
        }
        | _SERVER_ERROR_CODES
    )
    if error_code not in valid_error_codes:
        raise _ServerProcessError("server shutdown outcome has an unsupported error code")
    return outcome


def _terminate_child(process: subprocess.Popen[bytes], timeout_sec: float = 5.0) -> None:
    if process.poll() is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_sec)


def _cleanup_failed_launch(
    paths: _ServerPaths,
    *,
    pid: int,
    launch_nonce: str,
    inherited_lock_fd: int,
) -> None:
    cleanup_lock_fd: int | None = None
    if inherited_lock_fd < 0:
        cleanup_lock_fd = _try_acquire_launch_lock(paths.lock_file)
        if cleanup_lock_fd is None:
            return
    try:
        _cleanup_state_if_owned(
            paths.state_file,
            pid=pid,
            launch_nonce=launch_nonce,
        )
    finally:
        if cleanup_lock_fd is not None:
            os.close(cleanup_lock_fd)


def _wait_for_ready(
    process: subprocess.Popen[bytes],
    paths: _ServerPaths,
    launch_nonce: str,
    timeout_sec: float,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if paths.ready_file.exists():
            try:
                record = _read_json_file(paths.ready_file)
            except FileNotFoundError:
                continue
            if record.get("launch_nonce") != launch_nonce or record.get("pid") != process.pid:
                raise _ServerProcessError("server state does not match this launch")
            if record.get("status") == "ready":
                _validate_ready_record(record)
                return record
            if record.get("status") not in {"starting", None}:
                raise _ServerProcessError(
                    f"server entered unexpected startup state status=[{record.get('status')}]"
                )
        return_code = process.poll()
        if return_code is not None:
            raise _ServerProcessError(
                f"server exited before readiness return_code=[{return_code}] log=[{paths.bootstrap_log}]"
            )
        time.sleep(_PROCESS_POLL_INTERVAL_SEC)
    raise _ServerProcessError(
        f"server startup timed out timeout_sec=[{timeout_sec}] log=[{paths.bootstrap_log}]"
    )


def _validate_ready_record(record: dict[str, object]) -> None:
    if record.get("schema_version") not in _SUPPORTED_READY_SCHEMA_VERSIONS:
        raise _ServerProcessError("native server returned an unsupported ready schema")
    health = record.get("health")
    if not isinstance(health, dict):
        raise _ServerProcessError("native server ready record has invalid health")
    facts = health.get("readiness")
    services = health.get("services")
    if not isinstance(facts, dict) or not isinstance(services, dict):
        raise _ServerProcessError("native server ready record is missing readiness facts")
    gateway_configured = facts.get("gateway_configured")
    if not isinstance(gateway_configured, bool):
        raise _ServerProcessError(
            "native server ready record has invalid gateway configuration fact"
        )
    schema_version = record.get("schema_version")
    flight_configured = facts.get("flight_configured")
    if schema_version == 2 and flight_configured is None:
        flight_configured = False
    if not isinstance(flight_configured, bool):
        raise _ServerProcessError(
            "native server ready record has invalid Flight configuration fact"
        )
    native_storage_configured = facts.get("native_storage_configured")
    if not isinstance(native_storage_configured, bool):
        raise _ServerProcessError(
            "native server ready record has invalid native storage configuration fact"
        )
    native_storage_ready = not native_storage_configured or (
        facts.get("native_catalog_ready") is True
        and facts.get("native_current_snapshot_files_ready") is True
        and facts.get("storage_reconciliation_complete") is True
        and facts.get("commit_recovery_complete") is True
    )
    runtime_ready = (
        facts.get("admission_open") is True
        and facts.get("master_listener_ready") is True
        and native_storage_ready
        and (
            not gateway_configured
            or (
                facts.get("gateway_listener_ready") is True
                and facts.get("gateway_master_reachable") is True
            )
        )
        and (
            not flight_configured
            or (
                facts.get("flight_listener_ready") is True
                and facts.get("flight_protocol_ready") is True
            )
        )
    )
    services_ready = (
        services.get("admission_open") is True
        and services.get("accepting_work") is True
        and services.get("master_lifecycle_running") is True
        and (not gateway_configured or services.get("gateway_facade_available") is True)
        and services.get("end_to_end_liveness") is True
        and services.get("catalog") != "unavailable"
        and (not gateway_configured or services.get("query") != "unavailable")
    )
    if not runtime_ready or not services_ready:
        raise _ServerProcessError("native server ready record did not satisfy full readiness")


def _ready_record_for_status_validation(record: dict[str, object]) -> dict[str, object]:
    if record.get("schema_version") != 1:
        return record
    health = record.get("health")
    if not isinstance(health, dict):
        return record
    facts = health.get("readiness")
    if not isinstance(facts, dict):
        return record
    compatibility_facts = dict(facts)
    compatibility_facts.update(
        {
            "native_storage_configured": False,
            "native_catalog_ready": False,
            "native_current_snapshot_files_ready": False,
            "storage_reconciliation_complete": False,
            "commit_recovery_complete": False,
            "flight_configured": False,
            "flight_listener_ready": False,
            "flight_protocol_ready": False,
        }
    )
    compatibility_health = dict(health)
    compatibility_health["readiness"] = compatibility_facts
    compatibility_record = dict(record)
    compatibility_record["schema_version"] = _READY_SCHEMA_VERSION
    compatibility_record["health"] = compatibility_health
    return compatibility_record


def _tcp_address(endpoint: str) -> tuple[str, int]:
    authority = endpoint.removeprefix("tcp://")
    if authority.startswith("["):
        host, separator, port_text = authority[1:].partition("]:")
        if not separator:
            raise _ServerProcessError(f"invalid TCP control endpoint endpoint=[{endpoint}]")
        connect_host = "::1" if host == "::" else host
        return connect_host, int(port_text)
    host, port_text = authority.rsplit(":", 1)
    connect_host = "127.0.0.1" if host == "0.0.0.0" else host
    return connect_host, int(port_text)


def _master_get_config(
    control_endpoint: str, timeout_sec: float
) -> tuple[dict[str, object], float]:
    started = time.monotonic()
    deadline = started + timeout_sec
    response = bytearray()
    try:
        if control_endpoint.startswith("tcp://"):
            stream = socket.create_connection(
                _tcp_address(control_endpoint),
                timeout=_remaining_io_time(deadline, "connect master"),
            )
        else:
            stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            stream.settimeout(_remaining_io_time(deadline, "connect master"))
            stream.connect(control_endpoint)
        with stream:
            stream.settimeout(_remaining_io_time(deadline, "write master request"))
            stream.sendall(b'{"GetConfig":{}}\n')
            while not response.endswith(b"\n"):
                stream.settimeout(_remaining_io_time(deadline, "read master response"))
                chunk = stream.recv(4096)
                if not chunk:
                    break
                response.extend(chunk)
                if len(response) > _MAX_CONTROL_RESPONSE_BYTES:
                    raise _ServerProcessError("master health response exceeded size limit")
    except (OSError, TimeoutError) as error:
        raise _ServerProcessError(
            f"master health probe failed endpoint=[{control_endpoint}] error=[{error}]"
        ) from error
    if not response:
        raise _ServerProcessError(
            f"master health probe returned no response endpoint=[{control_endpoint}]"
        )
    try:
        payload = json.loads(response.decode("utf-8"))
        if "Error" in payload:
            raise _ServerProcessError("master health probe returned a protocol error")
        config = payload["ConfigFetched"]["config"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise _ServerProcessError(
            f"master health probe returned invalid response endpoint=[{control_endpoint}]"
        ) from error
    if not isinstance(config, dict):
        raise _ServerProcessError("master health response config must be an object")
    return dict(config), (time.monotonic() - started) * 1000.0


def _remaining_io_time(deadline: float, stage: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _ServerProcessError(f"server health deadline exceeded stage=[{stage}]")
    return remaining


def _probe_gateway(
    gateway: dict[str, object],
    effective_config: dict[str, object],
    timeout_sec: float,
) -> dict[str, object]:
    if not gateway.get("enabled"):
        return {"enabled": False, "status": "disabled"}
    endpoint = str(gateway.get("endpoint") or "")
    probe_endpoint = _connectable_gateway_endpoint(endpoint)
    config_gateway = effective_config.get("gateway", {})
    token = config_gateway.get("token") if isinstance(config_gateway, dict) else None
    started = time.monotonic()
    try:
        response = _gateway_metrics_request(
            probe_endpoint,
            token=str(token) if token else None,
            timeout_sec=timeout_sec,
        )
        if response.get("status") != "ok":
            raise RuntimeError("gateway metrics response was not successful")
    except Exception as error:
        return {
            "enabled": True,
            "status": "unhealthy",
            "endpoint": endpoint,
            "error": str(error),
        }
    return {
        "enabled": True,
        "status": "healthy",
        "endpoint": endpoint,
        "protocol_version": gateway.get("protocol_version"),
        "probe_ms": round((time.monotonic() - started) * 1000.0, 3),
    }


def _connectable_gateway_endpoint(endpoint: str) -> str:
    value = endpoint.removeprefix("tcp://")
    try:
        host, port = value.rsplit(":", 1)
    except ValueError:
        return value
    if host == "0.0.0.0":
        return f"127.0.0.1:{port}"
    if host in {"::", "[::]"}:
        return f"[::1]:{port}"
    return value


def _gateway_metrics_request(
    endpoint: str,
    *,
    token: str | None,
    timeout_sec: float,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_sec
    host, port = _tcp_address(f"tcp://{endpoint}")
    header: dict[str, object] = {"kind": "metrics"}
    if token is not None:
        header["token"] = token
    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    prefix = struct.Struct("!IQ")
    with socket.create_connection(
        (host, port),
        timeout=_remaining_io_time(deadline, "connect gateway"),
    ) as stream:
        stream.settimeout(_remaining_io_time(deadline, "write gateway request"))
        stream.sendall(prefix.pack(len(header_bytes), 0))
        stream.sendall(header_bytes)
        response_prefix = _recv_exact_with_deadline(
            stream,
            prefix.size,
            deadline,
            "read gateway response prefix",
        )
        header_size, payload_size = prefix.unpack(response_prefix)
        if header_size > 1024 * 1024 or payload_size > _MAX_CONTROL_RESPONSE_BYTES:
            raise _ServerProcessError("gateway health response exceeded size limit")
        response_bytes = _recv_exact_with_deadline(
            stream,
            header_size,
            deadline,
            "read gateway response header",
        )
        if payload_size:
            _recv_exact_with_deadline(
                stream,
                payload_size,
                deadline,
                "read gateway response payload",
            )
    response = json.loads(response_bytes.decode("utf-8"))
    if not isinstance(response, dict):
        raise _ServerProcessError("gateway health response must be an object")
    return dict(response)


def _recv_exact_with_deadline(
    stream: socket.socket,
    size: int,
    deadline: float,
    stage: str,
) -> bytes:
    chunks: list[bytes] = []
    remaining_bytes = size
    while remaining_bytes:
        stream.settimeout(_remaining_io_time(deadline, stage))
        chunk = stream.recv(remaining_bytes)
        if not chunk:
            raise _ServerProcessError(f"gateway closed connection stage=[{stage}]")
        chunks.append(chunk)
        remaining_bytes -= len(chunk)
    return b"".join(chunks)


def _probe_webui(url: str, timeout_sec: float) -> dict[str, object]:
    request = Request(url, method="GET", headers={"User-Agent": "zippy-server-health/1"})
    started = time.monotonic()
    try:
        with urlopen(request, timeout=timeout_sec) as response:
            response.read(256)
            status_code = int(response.status)
    except HTTPError as error:
        status_code = int(error.code)
    except (OSError, TimeoutError, URLError, ValueError):
        return {"managed": False, "status": "not_running", "url": url}
    return {
        "managed": False,
        "status": "endpoint_reachable_unverified",
        "url": url,
        "http_status": status_code,
        "probe_ms": round((time.monotonic() - started) * 1000.0, 3),
    }


def _live_health(
    ready_record: dict[str, object],
    *,
    webui_url: str,
    timeout_sec: float,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    deadline = time.monotonic() + timeout_sec

    def remaining() -> float:
        value = deadline - time.monotonic()
        if value <= 0:
            raise _ServerProcessError("server health deadline exceeded")
        return value

    control_endpoint = str(ready_record["control_endpoint"])
    effective_config, master_probe_ms = _master_get_config(control_endpoint, remaining())
    master = {
        "status": "healthy",
        "endpoint": control_endpoint,
        "probe_ms": round(master_probe_ms, 3),
    }
    ready_gateway = ready_record.get("gateway", {})
    gateway = _probe_gateway(
        dict(ready_gateway) if isinstance(ready_gateway, dict) else {},
        effective_config,
        remaining(),
    )
    if time.monotonic() >= deadline:
        webui = {"managed": False, "status": "not_checked", "url": webui_url}
    else:
        webui = _probe_webui(webui_url, min(remaining(), 1.0))
    return master, gateway, webui


def _required_process_identity(pid: int) -> dict[str, object]:
    identity = _process_identity(pid)
    if not identity.get("boot_id") or not identity.get("start_ticks"):
        raise _ServerProcessError(f"reliable Linux process identity is unavailable pid=[{pid}]")
    return identity


def _signal_owned_process(pid: int, identity: object, signal_number: int) -> bool:
    if not isinstance(identity, dict):
        raise _ServerProcessError("server process identity is missing")
    boot_id = identity.get("boot_id")
    start_ticks = identity.get("start_ticks")
    if not isinstance(boot_id, str) or not isinstance(start_ticks, str):
        raise _ServerProcessError("server process identity is incomplete")
    from ._internal import _signal_server_process

    try:
        return _signal_server_process(pid, boot_id, start_ticks, signal_number)
    except (RuntimeError, ValueError) as error:
        raise _ServerProcessError(f"secure server process signal failed error=[{error}]") from error


def _launcher_from_state(state: dict[str, object]) -> dict[str, object]:
    launcher = state.get("launcher")
    return dict(launcher) if isinstance(launcher, dict) else {}


def _validate_child_starting_state(
    *,
    state_file: Path,
    launch_nonce: str,
) -> None:
    state = _read_json_file(state_file)
    if (
        state.get("schema_version") != _STATE_SCHEMA_VERSION
        or state.get("status") != "starting"
        or state.get("launch_nonce") != launch_nonce
        or state.get("pid") != os.getpid()
    ):
        raise _ServerProcessError("detached bootstrap state does not match child process")
    launcher = _launcher_from_state(state)
    if not launcher:
        raise _ServerProcessError("detached bootstrap state is missing launcher metadata")
    if launcher.get("process_identity") != _required_process_identity(os.getpid()):
        raise _ServerProcessError("detached bootstrap process identity does not match")


def _compose_live_payload(
    ready: dict[str, object],
    *,
    master: dict[str, object],
    gateway: dict[str, object],
    webui: dict[str, object],
    startup_elapsed_ms: float | None,
) -> dict[str, object]:
    launcher = _launcher_from_state(ready)
    gateway_healthy = gateway.get("status") in {"healthy", "disabled"}
    payload: dict[str, object] = {
        "status": "healthy" if gateway_healthy else "unhealthy",
        "version": ready.get("version"),
        "pid": ready.get("pid"),
        "ready_at_unix_ns": ready.get("ready_at_unix_ns"),
        "control_endpoint": ready.get("control_endpoint"),
        "requested_control_endpoint": launcher.get("requested_control_endpoint"),
        "config_path": launcher.get("config_path"),
        "mode": ready.get("mode"),
        "runtime_health": ready.get("health"),
        "master": master,
        "gateway": gateway,
        "webui": webui,
        "bootstrap_log": launcher.get("bootstrap_log"),
        "native_log": ready.get("log_file"),
        "state_file": launcher.get("state_file"),
        "run_dir": launcher.get("run_dir"),
        "started_at": launcher.get("started_at"),
    }
    if startup_elapsed_ms is not None:
        payload["startup_elapsed_ms"] = round(startup_elapsed_ms, 3)
    return payload


def _open_bootstrap_log(path: Path) -> BinaryIO:
    fd = _open_regular_file(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    return os.fdopen(fd, "ab", buffering=0)


def _child_command(
    control_endpoint: str,
    *,
    config_path: str | None,
    paths: _ServerPaths,
    launch_nonce: str,
    startup_timeout_sec: float,
) -> list[str]:
    bootstrap = Path(__file__).with_name("_server_bootstrap.py")
    command = [
        sys.executable,
        str(bootstrap),
        sys.executable,
        "-m",
        "zippy",
        "server",
        "run",
        control_endpoint,
        "--foreground",
        "--ready-file",
        str(paths.ready_file),
        "--launch-nonce",
        launch_nonce,
        "--startup-timeout-sec",
        str(startup_timeout_sec),
        "--log-dir",
        str(paths.log_dir),
    ]
    if config_path is not None:
        command.extend(["--config", config_path])
    return command


def _launch_server(
    control_endpoint: str,
    *,
    config_path: str | None,
    run_dir: Path | None,
    log_dir: Path | None,
    webui_url: str | None,
    startup_timeout_sec: float,
) -> dict[str, object]:
    if not sys.platform.startswith("linux"):
        raise _ServerProcessError(
            "detached server mode currently requires Linux process identity; use --foreground"
        )
    if not math.isfinite(startup_timeout_sec) or startup_timeout_sec <= 0:
        raise _ServerProcessError("server startup timeout must be a positive finite number")

    webui = _normalize_webui_url(webui_url) if webui_url else _default_webui_url()
    launch_nonce = secrets.token_hex(16)
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=log_dir,
    )
    _prepare_owner_directory(paths.run_dir)
    _prepare_owner_directory(paths.log_dir)
    lock_fd = _acquire_launch_lock(paths.lock_file)
    process: subprocess.Popen[bytes] | None = None
    started = time.monotonic()
    try:
        for stale in (paths.state_file, paths.outcome_file):
            try:
                stale.unlink()
            except FileNotFoundError:
                pass
        bootstrap = _open_bootstrap_log(paths.bootstrap_log)
        try:
            environment = os.environ.copy()
            environment["ZIPPY_SERVER_LOCK_FD"] = str(lock_fd)
            environment["ZIPPY_SERVER_LOCK_FILE"] = str(paths.lock_file)
            environment["ZIPPY_SERVER_STATE_FILE"] = str(paths.state_file)
            environment["ZIPPY_SERVER_OUTCOME_FILE"] = str(paths.outcome_file)
            environment["ZIPPY_SERVER_LAUNCH_NONCE"] = launch_nonce
            environment["ZIPPY_SERVER_BOOTSTRAP_LOG"] = str(paths.bootstrap_log)
            environment["ZIPPY_SERVER_WEBUI_URL"] = webui
            environment["ZIPPY_SERVER_RUN_DIR_EFFECTIVE"] = str(paths.run_dir)
            environment["ZIPPY_SERVER_LOG_DIR_EFFECTIVE"] = str(paths.log_dir)
            environment["ZIPPY_SERVER_REQUESTED_CONTROL_ENDPOINT"] = control_endpoint
            environment["ZIPPY_SERVER_STARTUP_DEADLINE_MONOTONIC"] = str(
                started + startup_timeout_sec
            )
            environment.pop("ZIPPY_SERVER_CONFIG_PATH", None)
            if config_path is not None:
                environment["ZIPPY_SERVER_CONFIG_PATH"] = config_path
            process = subprocess.Popen(
                _child_command(
                    control_endpoint,
                    config_path=config_path,
                    paths=paths,
                    launch_nonce=launch_nonce,
                    startup_timeout_sec=startup_timeout_sec,
                ),
                stdin=subprocess.DEVNULL,
                stdout=bootstrap,
                stderr=subprocess.STDOUT,
                close_fds=True,
                pass_fds=(lock_fd,),
                start_new_session=True,
                env=environment,
            )
            pause_after_spawn = None
            if os.environ.get("PYTEST_CURRENT_TEST"):
                pause_after_spawn = os.environ.get("ZIPPY_SERVER_TEST_PAUSE_AFTER_SPAWN_SEC")
            if pause_after_spawn:
                time.sleep(float(pause_after_spawn))
        finally:
            bootstrap.close()
        os.close(lock_fd)
        lock_fd = -1

        ready_timeout = startup_timeout_sec - (time.monotonic() - started)
        if ready_timeout <= 0:
            raise _ServerProcessError(
                f"server startup timed out timeout_sec=[{startup_timeout_sec}]"
            )
        ready = _wait_for_ready(process, paths, launch_nonce, ready_timeout)
        remaining = startup_timeout_sec - (time.monotonic() - started)
        if remaining <= 0:
            raise _ServerProcessError(
                f"server health checks exceeded startup timeout timeout_sec=[{startup_timeout_sec}]"
            )
        master, gateway, webui_health = _live_health(
            ready,
            webui_url=webui,
            timeout_sec=min(remaining, 2.0),
        )
        if gateway.get("status") not in {"healthy", "disabled"}:
            raise _ServerProcessError(
                f"gateway health probe failed endpoint=[{gateway.get('endpoint')}] "
                f"error=[{gateway.get('error', 'unknown error')}]"
            )
        if time.monotonic() - started > startup_timeout_sec:
            raise _ServerProcessError(
                f"server health checks exceeded startup timeout timeout_sec=[{startup_timeout_sec}]"
            )
        launcher = _launcher_from_state(ready)
        if process.poll() is not None or not _lock_is_held(paths.lock_file):
            raise _ServerProcessError("server exited immediately after readiness")
        if not _process_matches(process.pid, launcher.get("process_identity")):
            raise _ServerProcessError("server process identity changed after readiness")
        if not _signal_owned_process(process.pid, launcher.get("process_identity"), 0):
            raise _ServerProcessError("server process exited immediately after health verification")
        return _compose_live_payload(
            ready,
            master=master,
            gateway=gateway,
            webui=webui_health,
            startup_elapsed_ms=(time.monotonic() - started) * 1000.0,
        )
    except BaseException:
        if process is not None:
            _terminate_child(process)
            _cleanup_failed_launch(
                paths,
                pid=process.pid,
                launch_nonce=launch_nonce,
                inherited_lock_fd=lock_fd,
            )
        raise
    finally:
        if lock_fd >= 0:
            os.close(lock_fd)


def _state_for_endpoint(
    control_endpoint: str,
    *,
    run_dir: Path | None,
) -> tuple[_ServerPaths, dict[str, object] | None]:
    paths = _server_paths(
        control_endpoint,
        run_dir=run_dir,
        log_dir=None,
    )
    try:
        state = _read_json_file(paths.state_file)
    except FileNotFoundError:
        return paths, None
    schema_version = state.get("schema_version")
    schema_supported = (
        schema_version in ({1} | _SUPPORTED_READY_SCHEMA_VERSIONS)
        if state.get("status") == "ready"
        else schema_version == _STATE_SCHEMA_VERSION
    )
    if not schema_supported:
        raise _ServerProcessError(f"server state has unsupported schema path=[{paths.state_file}]")
    return paths, state


def _state_pid(state: dict[str, object]) -> int:
    value = state.get("pid")
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise _ServerProcessError("server state PID is invalid")
    try:
        pid = int(value)
    except ValueError as error:
        raise _ServerProcessError("server state PID is invalid") from error
    if pid <= 0:
        raise _ServerProcessError("server state PID must be positive")
    return pid


def _webui_url_from_state(state: dict[str, object], override: str | None) -> str:
    if override:
        return _normalize_webui_url(override)
    webui = _launcher_from_state(state).get("webui")
    if isinstance(webui, dict):
        value = webui.get("url")
        if isinstance(value, str) and value:
            return _normalize_webui_url(value)
    return _default_webui_url()


def _compose_starting_payload(state: dict[str, object]) -> dict[str, object]:
    launcher = _launcher_from_state(state)
    return {
        "status": "starting",
        "pid": state.get("pid"),
        "requested_control_endpoint": launcher.get("requested_control_endpoint"),
        "config_path": launcher.get("config_path"),
        "webui": launcher.get("webui"),
        "bootstrap_log": launcher.get("bootstrap_log"),
        "state_file": launcher.get("state_file"),
        "run_dir": launcher.get("run_dir"),
        "started_at": launcher.get("started_at"),
    }


def _generation_still_owned(
    paths: _ServerPaths,
    state: dict[str, object],
    *,
    pid: int,
) -> bool:
    if not _lock_is_held(paths.lock_file):
        return False
    try:
        latest = _read_json_file(paths.state_file)
    except (FileNotFoundError, _ServerProcessError):
        return False
    expected_identity = _launcher_from_state(state).get("process_identity")
    if (
        latest.get("schema_version") != state.get("schema_version")
        or latest.get("schema_version") not in ({1} | _SUPPORTED_READY_SCHEMA_VERSIONS)
        or latest.get("status") != "ready"
        or latest.get("pid") != pid
        or latest.get("launch_nonce") != state.get("launch_nonce")
        or _launcher_from_state(latest).get("process_identity") != expected_identity
    ):
        return False
    return _signal_owned_process(pid, expected_identity, 0)


def _server_status(
    control_endpoint: str,
    *,
    run_dir: Path | None,
    webui_url: str | None,
    probe_timeout_sec: float = 2.0,
) -> dict[str, object]:
    paths, state = _state_for_endpoint(control_endpoint, run_dir=run_dir)
    if state is None:
        if _lock_is_held(paths.lock_file):
            return {
                "status": "starting",
                "requested_control_endpoint": control_endpoint,
                "state_file": str(paths.state_file),
                "health_error": "server ownership lock is held before state publication",
            }
        return {
            "status": "stopped",
            "requested_control_endpoint": control_endpoint,
            "state_file": str(paths.state_file),
        }
    pid = _state_pid(state)
    launcher = _launcher_from_state(state)
    lock_held = _lock_is_held(paths.lock_file)
    process_matches = _process_matches(pid, launcher.get("process_identity"))
    if not lock_held and not process_matches:
        return {
            "status": "stopped",
            "requested_control_endpoint": control_endpoint,
            "stale_state": True,
            "state_file": str(paths.state_file),
        }
    if not lock_held or not process_matches:
        result = _compose_starting_payload(state)
        result["status"] = "unknown"
        result["health_error"] = "server process identity or ownership lock does not match"
        return result
    if state.get("status") == "starting":
        return _compose_starting_payload(state)
    if state.get("status") == "stopping":
        result = _compose_starting_payload(state)
        result["status"] = "stopping"
        result["health_error"] = "server shutdown is in progress"
        return result
    if state.get("status") == "failed":
        result = _compose_starting_payload(state)
        error_code = state.get("error_code")
        if not isinstance(error_code, str) or error_code not in _SERVER_ERROR_CODES:
            result["status"] = "unknown"
            result["health_error"] = "server failed state has an invalid error code"
            return result
        result["status"] = "failed"
        result["error_code"] = error_code
        result["health_error"] = f"server runtime failed error_code=[{error_code}]"
        return result
    if state.get("status") != "ready":
        result = _compose_starting_payload(state)
        result["status"] = "unknown"
        result["health_error"] = f"unsupported server state status=[{state.get('status')}]"
        return result

    try:
        _validate_ready_record(_ready_record_for_status_validation(state))
    except _ServerProcessError as error:
        result = _compose_starting_payload(state)
        result["status"] = "unhealthy"
        result["health_error"] = str(error)
        return result

    try:
        master, gateway, webui = _live_health(
            state,
            webui_url=_webui_url_from_state(state, webui_url),
            timeout_sec=probe_timeout_sec,
        )
    except _ServerProcessError as error:
        result = _compose_starting_payload(state)
        result["version"] = state.get("version")
        result["mode"] = state.get("mode")
        result["control_endpoint"] = state.get("control_endpoint")
        result["native_log"] = state.get("log_file")
        result["status"] = "unhealthy"
        result["health_error"] = str(error)
        return result
    result = _compose_live_payload(
        state,
        master=master,
        gateway=gateway,
        webui=webui,
        startup_elapsed_ms=None,
    )
    if not _generation_still_owned(paths, state, pid=pid):
        result["status"] = "unknown"
        result["health_error"] = "server generation changed during health probe"
        return result
    result["checked_at"] = datetime.now(UTC).isoformat()
    return result


def _wait_for_process_exit(pid: int, identity: object, timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not _process_matches(pid, identity):
            return True
        time.sleep(_PROCESS_POLL_INTERVAL_SEC)
    return not _process_matches(pid, identity)


def _stop_server(
    control_endpoint: str,
    *,
    run_dir: Path | None,
    timeout_sec: float,
    force: bool,
) -> dict[str, object]:
    if not sys.platform.startswith("linux"):
        raise _ServerProcessError("graceful detached server stop currently requires Linux")
    if not math.isfinite(timeout_sec) or timeout_sec <= 0:
        raise _ServerProcessError("server stop timeout must be a positive finite number")
    paths, state = _state_for_endpoint(control_endpoint, run_dir=run_dir)
    if state is None:
        if _lock_is_held(paths.lock_file):
            raise _ServerProcessError(
                "server is starting but has not published process state; retry stop"
            )
        return {"status": "stopped", "already_stopped": True, "state_file": str(paths.state_file)}
    pid = _state_pid(state)
    launcher = _launcher_from_state(state)
    identity = launcher.get("process_identity")
    if not isinstance(identity, dict) or not identity:
        raise _ServerProcessError("refusing to stop process without reliable process identity")
    launch_nonce = state.get("launch_nonce")
    if not isinstance(launch_nonce, str) or not launch_nonce:
        raise _ServerProcessError("refusing to stop process without a launch nonce")
    was_ready = state.get("status") in {"ready", "stopping", "failed"}
    outcome_file = paths.outcome_file
    if not _lock_is_held(paths.lock_file):
        if not _process_matches(pid, identity):
            return {
                "status": "stopped",
                "already_stopped": True,
                "stale_state": True,
                "state_file": str(paths.state_file),
            }
        raise _ServerProcessError("refusing to stop process without its server ownership lock")
    if not _process_matches(pid, identity):
        raise _ServerProcessError("refusing to stop process because its identity does not match")

    if not _lock_is_held(paths.lock_file) or not _process_matches(pid, identity):
        raise _ServerProcessError("server ownership changed before shutdown signal")
    try:
        latest_state = _read_json_file(paths.state_file)
    except FileNotFoundError as error:
        raise _ServerProcessError("server state changed before shutdown signal") from error
    if latest_state.get("pid") != pid or latest_state.get("launch_nonce") != launch_nonce:
        raise _ServerProcessError("server generation changed before shutdown signal")
    _signal_owned_process(pid, identity, signal.SIGTERM)
    forced = False
    if not _wait_for_process_exit(pid, identity, timeout_sec):
        if not force:
            raise _ServerProcessError(
                f"server did not stop within timeout pid=[{pid}] timeout_sec=[{timeout_sec}]"
            )
        _signal_owned_process(pid, identity, signal.SIGKILL)
        forced = True
        if not _wait_for_process_exit(pid, identity, min(timeout_sec, 5.0)):
            raise _ServerProcessError(f"server process remained after SIGKILL pid=[{pid}]")
    cleanup_lock_fd = _try_acquire_launch_lock(paths.lock_file)
    if cleanup_lock_fd is None:
        raise _ServerProcessError(
            "server generation changed during shutdown; active replacement was not stopped"
        )
    try:
        try:
            current_state = _read_json_file(paths.state_file)
        except FileNotFoundError:
            current_state = None
        if current_state is not None and (
            current_state.get("pid") != pid or current_state.get("launch_nonce") != launch_nonce
        ):
            raise _ServerProcessError(
                "server generation changed during shutdown; replacement state was preserved"
            )
        outcome = _read_shutdown_outcome(
            outcome_file,
            pid=pid,
            launch_nonce=launch_nonce,
        )
        if not forced:
            if outcome is None and was_ready:
                raise _ServerProcessError(
                    f"server stopped without a clean shutdown outcome pid=[{pid}]"
                )
            if outcome is not None and outcome.get("clean") is not True:
                raise _ServerProcessError(
                    f"server shutdown reported failure pid=[{pid}] "
                    f"error_code=[{outcome.get('error_code', 'unknown')}]"
                )
        _cleanup_state_if_owned(
            paths.state_file,
            pid=pid,
            launch_nonce=launch_nonce,
        )
        if outcome is not None:
            try:
                outcome_file.unlink()
            except FileNotFoundError:
                pass
    finally:
        os.close(cleanup_lock_fd)
    return {
        "status": "stopped",
        "pid": pid,
        "forced": forced,
        "graceful": not forced and outcome is not None and outcome.get("clean") is True,
        "startup_aborted": not was_ready,
        "state_file": str(paths.state_file),
    }


def _format_server_summary(payload: dict[str, object], *, action: str) -> str:
    if action == "stop":
        lines = ["zippy server stopped", f"pid: {payload.get('pid', 'not running')}"]
        lines.append(f"forced: {'yes' if payload.get('forced') else 'no'}")
        if payload.get("graceful") is not None:
            lines.append(f"graceful: {'yes' if payload.get('graceful') else 'no'}")
        lines.append(f"state: {payload.get('state_file')}")
        return "\n".join(lines)

    heading = "zippy server started" if action == "run" else "zippy server status"
    master = payload.get("master", {})
    gateway = payload.get("gateway", {})
    webui = payload.get("webui", {})
    master = master if isinstance(master, dict) else {}
    gateway = gateway if isinstance(gateway, dict) else {}
    webui = webui if isinstance(webui, dict) else {}
    lines = [
        heading,
        f"version: {payload.get('version', 'unknown')}",
        f"pid: {payload.get('pid', 'not running')}",
        f"health: {payload.get('status', 'unknown')}",
    ]
    if payload.get("mode"):
        lines.append(f"mode: {payload['mode']}")
    if payload.get("startup_elapsed_ms") is not None:
        lines.append(f"startup check: passed elapsed_ms=[{payload['startup_elapsed_ms']}]")
    control_endpoint = master.get("endpoint") or payload.get("control_endpoint")
    master_status = master.get("status", "not probed")
    master_line = f"master: {master_status}"
    if control_endpoint:
        master_line += f" endpoint=[{control_endpoint}]"
    lines.append(master_line)
    if gateway.get("enabled") is False or gateway.get("status") == "disabled":
        lines.append("gateway: disabled")
    elif gateway:
        lines.append(
            f"gateway: {gateway.get('status', 'unknown')} endpoint=[{gateway.get('endpoint')}]"
        )
    if webui:
        webui_status = str(webui.get("status", "unknown")).replace("_", " ")
        lines.append(f"webui: {webui_status} managed=[no] url=[{webui.get('url')}]")
    if payload.get("health_error"):
        lines.append(f"health error: {payload['health_error']}")
    if payload.get("bootstrap_log"):
        lines.append(f"bootstrap log: {payload['bootstrap_log']}")
    if payload.get("native_log"):
        lines.append(f"native log: {payload['native_log']}")
    lines.append(f"state: {payload.get('state_file')}")
    if action == "run":
        endpoint = shlex.quote(str(payload.get("requested_control_endpoint", "")))
        command = f"zippy server stop {endpoint}".rstrip()
        if payload.get("run_dir"):
            command += f" --run-dir {shlex.quote(str(payload['run_dir']))}"
        lines.append(f"stop: {command}")
    return "\n".join(lines)


def _cleanup_child_state_from_environment() -> None:
    state_path = os.environ.get("ZIPPY_SERVER_STATE_FILE")
    launch_nonce = os.environ.get("ZIPPY_SERVER_LAUNCH_NONCE")
    if not state_path or not launch_nonce:
        return
    _cleanup_state_if_owned(Path(state_path), pid=os.getpid(), launch_nonce=launch_nonce)


def _validate_inherited_lock(
    expected_path: Path | None = None,
    *,
    required: bool = False,
) -> None:
    raw_fd = os.environ.pop("ZIPPY_SERVER_LOCK_FD", None)
    if raw_fd is None:
        if required:
            raise _ServerProcessError("detached child is missing its inherited server lock")
        return
    try:
        lock_fd = int(raw_fd)
        info = os.fstat(lock_fd)
    except (OSError, ValueError) as error:
        raise _ServerProcessError(f"inherited server lock is invalid error=[{error}]") from error
    if not stat.S_ISREG(info.st_mode):
        raise _ServerProcessError("inherited server lock is not a regular file")
    if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
        raise _ServerProcessError("inherited server lock is not owned by current user")
    if info.st_mode & 0o077:
        raise _ServerProcessError("inherited server lock permissions are not owner-only")
    if expected_path is not None:
        expected = expected_path.lstat()
        if (expected.st_dev, expected.st_ino) != (info.st_dev, info.st_ino):
            raise _ServerProcessError("inherited server lock does not match expected lock path")
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(lock_fd, False)
