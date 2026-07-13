"""
Minimal detached-server bootstrap executed before importing the zippy package.
"""

from __future__ import annotations

from datetime import UTC, datetime
import errno
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time

import fcntl

_STATE_SCHEMA_VERSION = 1


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing detached bootstrap environment name=[{name}]")
    return value


def _process_identity(pid: int) -> dict[str, str]:
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").strip()
    fields = stat_line[stat_line.rfind(")") + 2 :].split()
    start_ticks = fields[19]
    if not boot_id or not start_ticks:
        raise RuntimeError("Linux process identity is unavailable")
    return {"boot_id": boot_id, "start_ticks": start_ticks}


def _validate_lock() -> int:
    lock_fd = int(_required_environment("ZIPPY_SERVER_LOCK_FD"))
    lock_path = Path(_required_environment("ZIPPY_SERVER_LOCK_FILE"))
    info = os.fstat(lock_fd)
    expected = lock_path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError("inherited server lock is not a regular file")
    if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
        raise RuntimeError("inherited server lock is not owned by current user")
    if info.st_mode & 0o077:
        raise RuntimeError("inherited server lock permissions are not owner-only")
    if (expected.st_dev, expected.st_ino) != (info.st_dev, info.st_ino):
        raise RuntimeError("inherited server lock does not match expected path")
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(lock_fd, True)
    return lock_fd


def _atomic_write_json(path: Path, payload: dict[str, object], nonce: str) -> None:
    temporary = path.with_name(f".{path.name}.{nonce}.bootstrap.tmp")
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("detached bootstrap state is not a regular file")
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except BaseException:
        try:
            os.close(fd)
        except OSError as error:
            if error.errno != errno.EBADF:
                raise
        try:
            temporary.unlink()
        except OSError:
            pass
        raise


def _launcher_state(state_file: Path) -> dict[str, object]:
    config_path = os.environ.get("ZIPPY_SERVER_CONFIG_PATH")
    return {
        "process_identity": _process_identity(os.getpid()),
        "started_at": datetime.now(UTC).isoformat(),
        "requested_control_endpoint": _required_environment(
            "ZIPPY_SERVER_REQUESTED_CONTROL_ENDPOINT"
        ),
        "config_path": config_path,
        "webui": {
            "managed": False,
            "status": "unknown",
            "url": _required_environment("ZIPPY_SERVER_WEBUI_URL"),
        },
        "bootstrap_log": _required_environment("ZIPPY_SERVER_BOOTSTRAP_LOG"),
        "state_file": str(state_file),
        "lock_file": _required_environment("ZIPPY_SERVER_LOCK_FILE"),
        "outcome_file": _required_environment("ZIPPY_SERVER_OUTCOME_FILE"),
        "run_dir": _required_environment("ZIPPY_SERVER_RUN_DIR_EFFECTIVE"),
        "log_dir": _required_environment("ZIPPY_SERVER_LOG_DIR_EFFECTIVE"),
    }


def _write_outcome(*, clean: bool, error_code: str | None) -> None:
    outcome_file = Path(_required_environment("ZIPPY_SERVER_OUTCOME_FILE"))
    nonce = _required_environment("ZIPPY_SERVER_LAUNCH_NONCE")
    payload: dict[str, object] = {
        "schema_version": _STATE_SCHEMA_VERSION,
        "launch_nonce": nonce,
        "pid": os.getpid(),
        "clean": clean,
        "stopped_at": datetime.now(UTC).isoformat(),
    }
    if error_code:
        payload["error_code"] = error_code
    _atomic_write_json(outcome_file, payload, nonce)


def _remove_starting_state(state_file: Path, nonce: str) -> None:
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return
    if payload.get("pid") == os.getpid() and payload.get("launch_nonce") == nonce:
        try:
            state_file.unlink()
        except FileNotFoundError:
            pass


def _main() -> int:
    if not sys.platform.startswith("linux"):
        raise RuntimeError("detached server bootstrap requires Linux")
    if len(sys.argv) < 2:
        raise RuntimeError("detached server bootstrap command is missing")
    _validate_lock()
    state_file = Path(_required_environment("ZIPPY_SERVER_STATE_FILE"))
    nonce = _required_environment("ZIPPY_SERVER_LAUNCH_NONCE")
    starting: dict[str, object] = {
        "schema_version": _STATE_SCHEMA_VERSION,
        "status": "starting",
        "launch_nonce": nonce,
        "pid": os.getpid(),
        "launcher": _launcher_state(state_file),
    }
    _atomic_write_json(state_file, starting, nonce)

    def handle_signal(signal_number: int, _frame: object) -> None:
        _write_outcome(clean=True, error_code="startup_aborted")
        _remove_starting_state(state_file, nonce)
        os._exit(128 + signal_number)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    pause = None
    if os.environ.get("PYTEST_CURRENT_TEST"):
        pause = os.environ.get("ZIPPY_SERVER_TEST_BOOTSTRAP_PAUSE_SEC")
    if pause:
        time.sleep(float(pause))
    command = sys.argv[1:]
    os.execve(command[0], command, os.environ.copy())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(_main())
    except Exception as error:
        try:
            _write_outcome(clean=False, error_code="bootstrap_error")
            _remove_starting_state(
                Path(_required_environment("ZIPPY_SERVER_STATE_FILE")),
                _required_environment("ZIPPY_SERVER_LAUNCH_NONCE"),
            )
        except BaseException:
            pass
        print(
            f"server bootstrap failed error_type=[{type(error).__name__}]",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
