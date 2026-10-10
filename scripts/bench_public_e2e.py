"""Measure isolated Python writers and log readers over the real loopback Flight path.

The public SDK calls a one-row put ``Client.get_writer(...).write(row)``. Both
latencies include scalar-to-Arrow conversion; visibility ends at the first Python
instruction after ``next(open_log(...))`` (one row per event), before value decoding.
This measures tier C, not a SHM path or the ``Client.subscribe`` callback wrapper.

:returns: CLI status 0 on complete, within-budget cells; 1 on failure; 130 on Ctrl-C.
:raises SystemExit: Invalid CLI arguments or a non-Linux monotonic clock domain.
:example: uv run --no-sync scripts/bench_public_e2e.py --report-json /tmp/e2e.json
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Sequence
from contextlib import ExitStack
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import subprocess
import sys
import sysconfig
import tempfile
import time
import traceback
from typing import Any
import uuid


def nearest_rank(values: Sequence[int], percentile: float) -> int | None:
    """Return the nearest-rank percentile without interpolation.

    :param values: Unordered integer samples in a common unit.
    :param percentile: Percentile in the inclusive interval [0, 100].
    :returns: Sorted sample at max(1, ceil(p * n / 100)), or None for no samples.
    :raises ValueError: The percentile is not finite or outside [0, 100].
    :example: nearest_rank([30, 10, 20], 50) == 20
    """
    if not math.isfinite(percentile) or not 0 <= percentile <= 100:
        raise ValueError("percentile must be finite and between [0, 100]")
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered) / 100) - 1)] if ordered else None


def summarize_ns(values: Sequence[int]) -> dict[str, Any]:
    """Summarize nanoseconds as microseconds, suppressing undersampled p99.9.

    :param values: Nonnegative monotonic duration samples in nanoseconds.
    :returns: Count, nearest-rank percentiles, maximum, mean and p99.9 availability reason.
    :raises ValueError: A negative duration indicates an invalid measurement.
    :example: summarize_ns([1000])["p50_micros"] == 1.0
    """
    if any(value < 0 for value in values):
        raise ValueError("negative latency invalidates the monotonic measurement")
    ordered = sorted(values)
    result: dict[str, Any] = {"count": len(ordered)}
    for name, percentile in (("p50", 50), ("p95", 95), ("p99", 99), ("p99_9", 99.9)):
        enough = bool(ordered) and (name != "p99_9" or len(ordered) >= 10_000)
        rank = max(0, math.ceil(percentile * len(ordered) / 100) - 1)
        result[f"{name}_micros"] = ordered[rank] / 1000 if enough else None
    result["max_micros"] = ordered[-1] / 1000 if ordered else None
    result["mean_micros"] = sum(ordered) / len(ordered) / 1000 if ordered else None
    result["p99_9_unavailable_reason"] = (
        "fewer than 10000 measured samples; p99.9 is not estimated"
        if len(ordered) < 10_000
        else None
    )
    return result


def measurement_mask(
    sequences: Sequence[int],
    sent_ns: Sequence[int],
    started_ns: int,
    warmup_seconds: float,
    warmup_samples: int,
) -> list[bool]:
    """Exclude warmup until both elapsed time and initial sample requirements hold.

    :param sequences: Zero-based sequence numbers, including warmup.
    :param sent_ns: Actual monotonic send times aligned with sequences.
    :param started_ns: Common absolute pacing origin in CLOCK_MONOTONIC nanoseconds.
    :param warmup_seconds: Minimum warmup duration in seconds.
    :param warmup_samples: Minimum number of initial rows to discard.
    :returns: One inclusion flag per input row.
    :raises ValueError: Inputs differ in length or warmup limits are invalid.
    :example: measurement_mask([0, 1], [0, 1000000000], 0, 1, 1) == [False, True]
    """
    if len(sequences) != len(sent_ns):
        raise ValueError("sequence and timestamp lengths differ")
    if not math.isfinite(warmup_seconds) or warmup_seconds < 0 or warmup_samples < 0:
        raise ValueError("warmup limits must be finite and nonnegative")
    boundary = started_ns + int(warmup_seconds * 1_000_000_000)
    return [seq >= warmup_samples and sent >= boundary for seq, sent in zip(sequences, sent_ns)]


def check_integrity(sequences: Sequence[int], expected_count: int) -> dict[str, Any]:
    """Check every delivered row, including warmup, against the exact writer sequence.

    :param sequences: Received sequence numbers in delivery order.
    :param expected_count: Expected contiguous rows numbered [0, expected_count).
    :returns: Pass flag and counts for missing, repeated, decreasing and unexpected rows.
    :raises ValueError: The expected count is negative.
    :example: check_integrity([0, 1, 2], 3)["passed"] is True
    """
    if expected_count < 0:
        raise ValueError("expected count must be nonnegative")
    counts = Counter(sequences)
    missing = [seq for seq in range(expected_count) if seq not in counts]
    duplicate_count = sum(count - 1 for count in counts.values())
    unexpected_count = sum(count for seq, count in counts.items() if not 0 <= seq < expected_count)
    out_of_order_count = sum(right < left for left, right in zip(sequences, sequences[1:]))
    return {
        "passed": not (missing or duplicate_count or unexpected_count or out_of_order_count),
        "expected_count": expected_count,
        "received_count": len(sequences),
        "missing_count": len(missing),
        "missing_seq_first_20": missing[:20],
        "duplicate_count": duplicate_count,
        "out_of_order_count": out_of_order_count,
        "unexpected_count": unexpected_count,
    }


def evaluate_thresholds(
    visible_stats: dict[str, Any],
    ack_stats: dict[str, Any],
    durability: str,
    thresholds: dict[str, float],
) -> dict[str, Any]:
    """Judge tier C visibility and, for WAL writes only, tier D acknowledgements.

    :param visible_stats: Output of summarize_ns for received rows.
    :param ack_stats: Output of summarize_ns for synchronous one-row writes.
    :param durability: Requested memory or wal receipt boundary.
    :param thresholds: Maximum latency in microseconds, keyed by measurement and percentile.
    :returns: Pass flag, explicit failure reasons and skipped undersampled p99.9 checks.
    :raises KeyError: A required threshold is missing.
    :example: evaluate_thresholds(v, a, "memory", limits)["passed"]
    """
    failures: list[str] = []
    skipped: list[str] = []
    checks: list[tuple[str, dict[str, Any], tuple[str, ...]]] = [
        ("visible", visible_stats, ("p50", "p99", "p99_9"))
    ]
    if durability == "wal":
        checks.append(("wal_ack", ack_stats, ("p50", "p99")))
    for prefix, stats, percentiles in checks:
        for percentile in percentiles:
            key = f"{percentile}_micros"
            limit = thresholds[f"{prefix}_{key}"]
            value = stats.get(key)
            if value is None and percentile == "p99_9" and stats.get("count", 0) > 0:
                skipped.append(f"{prefix}_{key}: {stats.get('p99_9_unavailable_reason')}")
            elif value is None:
                failures.append(f"{prefix}_{key}: no measured samples")
            elif value > limit:
                failures.append(f"{prefix}_{key}=[{value:.3f}] exceeds limit=[{limit:.3f}]")
    if ack_stats.get("count", 0) == 0:
        failures.append("ack_latency: no measured samples")
    return {"passed": not failures, "failures": failures, "skipped": skipped}


def exit_code(cells: Sequence[dict[str, Any]]) -> int:
    """Convert matrix results into a strict process exit status.

    :param cells: Completed matrix cells containing their combined passed flag.
    :returns: Zero only for a nonempty matrix in which every cell passes.
    :example: exit_code([{"passed": False}]) == 1
    """
    return 0 if cells and all(cell.get("passed", False) for cell in cells) else 1


def _write_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(".pending")
    temporary.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def _schema() -> Any:
    import pyarrow as pa

    return pa.schema(
        [
            ("symbol", pa.string()),
            ("seq", pa.int64()),
            ("sent_ns", pa.int64()),
            ("price", pa.float64()),
        ]
    )


def _client(config: dict[str, Any], *, admin: bool = False) -> Any:
    from zippy_client import Client

    return Client(
        config["uri"],
        transport="flight_local",
        flight_token=config["credentials"]["read"],
        flight_writer_token=config["credentials"]["writer"],
        flight_admin_token=config["credentials"]["admin"] if admin else None,
    )


def _sleep_until(target_ns: int) -> None:
    remaining = target_ns - time.monotonic_ns()
    while remaining > 0:
        time.sleep(remaining / 1_000_000_000)
        remaining = target_ns - time.monotonic_ns()


def _start_time(directory: Path, role: str) -> int:
    (directory / f"{role}.ready").touch()
    while not (directory / "start.json").exists():
        time.sleep(0.005)
    started_ns = int(json.loads((directory / "start.json").read_text()))
    _sleep_until(started_ns)
    return started_ns


def _writer(config: dict[str, Any], result: dict[str, Any]) -> None:
    directory = Path(config["directory"])
    result.update(seq=[], sent_ns=[], ack_ns=[], schedule_lag_ns=[])
    with (
        _client(config) as client,
        client.get_writer(
            "ticks",
            namespace="bench",
            schema=_schema(),
            batch_size=1,
            flush_interval_ms=None,
            durability=config["durability"],
        ) as writer,
    ):
        start = _start_time(directory, "writer")
        result["started_ns"] = start
        measured = 0
        seq = 0
        boundary = start + int(config["warmup_seconds"] * 1_000_000_000)
        while measured < config["samples"]:
            target = start + round(seq * 1_000_000_000 / config["rate"])
            _sleep_until(target)
            row = {"symbol": "probe", "seq": seq, "sent_ns": 0, "price": 100.0}
            sent = time.monotonic_ns()
            row["sent_ns"] = sent
            writer.write(row)
            ack = time.monotonic_ns()
            result["seq"].append(seq)
            result["sent_ns"].append(sent)
            result["ack_ns"].append(ack)
            result["schedule_lag_ns"].append(sent - target)
            if seq >= config["warmup_samples"] and sent >= boundary:
                measured += 1
            seq += 1
        result["finished_ns"] = time.monotonic_ns()
        # A final ordered row bounds the stream and exposes missing rows without waiting forever.
        writer.write({"symbol": "end", "seq": seq, "sent_ns": 0, "price": 0.0})
        result["end_sent"] = True


def _subscriber(config: dict[str, Any], result: dict[str, Any]) -> None:
    result.update(seq=[], sent_ns=[], received_ns=[], end_seq=None)
    with (
        _client(config) as client,
        client.open_log(
            "ticks",
            namespace="bench",
            start="earliest",
            batch_rows=1,
            open_timeout_sec=15,
            deadline_ns=time.time_ns() + int(config["timeout_seconds"] * 1_000_000_000),
        ) as reader,
    ):
        _start_time(Path(config["directory"]), "subscriber")
        for event in reader:
            received = time.monotonic_ns()
            if event.table.num_rows != 1:
                raise RuntimeError("one-row log delivery expected")
            row = event.table.to_pylist()[0]
            if row["symbol"] == "end":
                result["end_seq"] = row["seq"]
                break
            if row["symbol"] != "probe":
                raise RuntimeError("unexpected symbol on the probe table")
            result["seq"].append(row["seq"])
            result["sent_ns"].append(row["sent_ns"])
            result["received_ns"].append(received)


def _background(config: dict[str, Any], result: dict[str, Any]) -> None:
    import pyarrow as pa

    directory = Path(config["directory"])
    size = config["background_batch_rows"]
    result.update(rows=0, batches=0)
    with (
        _client(config) as client,
        client.get_writer(
            "background",
            namespace="bench",
            schema=_schema(),
            durability=config["durability"],
        ) as writer,
    ):
        start = _start_time(directory, "background")
        result["started_ns"] = start
        while not (directory / "stop").exists():
            target = start + round(result["rows"] * 1_000_000_000 / config["background_rate"])
            while time.monotonic_ns() < target and not (directory / "stop").exists():
                remaining = (target - time.monotonic_ns()) / 1_000_000_000
                time.sleep(max(0.0, min(0.05, remaining)))
            if (directory / "stop").exists():
                break
            base = result["rows"]
            batch = pa.table(
                {
                    "symbol": ["background"] * size,
                    "seq": range(base, base + size),
                    "sent_ns": [time.monotonic_ns()] * size,
                    "price": [100.0] * size,
                },
                schema=_schema(),
            )
            writer.write_batch(batch)
            result["rows"] += size
            result["batches"] += 1
        result["finished_ns"] = time.monotonic_ns()
        duration = (result["finished_ns"] - start) / 1_000_000_000
        result["achieved_rows_per_second"] = result["rows"] / duration if duration else 0.0


def _worker(role: str, path: Path) -> int:
    config = json.loads(path.read_text())
    result: dict[str, Any] = {"pid": os.getpid(), "role": role}
    try:
        {"writer": _writer, "subscriber": _subscriber, "background": _background}[role](
            config, result
        )
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
        result["error_traceback"] = "".join(traceback.format_exception(error))
    finally:
        result.setdefault("finished_ns", time.monotonic_ns())
        if role == "background" and "started_ns" in result:
            duration = (result["finished_ns"] - result["started_ns"]) / 1_000_000_000
            result["achieved_rows_per_second"] = result["rows"] / duration if duration > 0 else 0.0
        _write_json(path.parent / f"{role}.json", result)
    return int("error" in result)


def _terminate(process: subprocess.Popen[Any], *, server: bool = False) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGINT if server else signal.SIGTERM)
            process.wait(timeout=10 if server else 3)
        except ProcessLookupError:
            process.wait()
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The child exited between wait() and killpg(); still reap it below.
            process.wait()


def _launch(stack: ExitStack, command: list[str], logfile: Path, *, server: bool = False) -> Any:
    handle = stack.enter_context(logfile.open("w"))
    env = {**os.environ, "PYO3_PYTHON": sys.executable}
    libdir = sysconfig.get_config_var("LIBDIR")
    if libdir:
        env["LD_LIBRARY_PATH"] = str(libdir) + os.pathsep + env.get("LD_LIBRARY_PATH", "")
    process = subprocess.Popen(
        command, stdout=handle, stderr=subprocess.STDOUT, env=env, start_new_session=True
    )
    stack.callback(_terminate, process, server=server)
    return process


def _wait_ready(directory: Path, workers: dict[str, Any], server: Any, deadline: float) -> None:
    while True:
        for role, process in workers.items():
            if process.poll() is not None:
                raise RuntimeError(f"worker exited before ready role=[{role}]")
        if server.poll() is not None:
            raise RuntimeError("server exited before workers were ready")
        if all((directory / f"{role}.ready").exists() for role in workers):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("workers did not become ready")
        time.sleep(0.01)


def _drain_workers(directory: Path, processes: dict[str, Any]) -> None:
    # Closing the server releases blocking Flight reads so workers can save partial evidence.
    (directory / "stop").touch()
    _terminate(processes["server"], server=True)
    deadline = time.monotonic() + 3
    for role, process in processes.items():
        if role != "server" and process.poll() is None:
            try:
                process.wait(timeout=max(0.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                continue  # ExitStack's registered per-process terminator is the bounded fallback.


def _run_processes(
    config: dict[str, Any], directory: Path, stack: ExitStack, processes: dict[str, Any]
) -> None:
    credentials = {role: secrets.token_hex(24) for role in ("read", "writer", "admin")}
    for role, token in credentials.items():
        path = directory / f"{role}.token"
        path.write_text(token)
        path.chmod(0o600)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        endpoint = f"127.0.0.1:{probe.getsockname()[1]}"
    config.update(directory=str(directory), uri=str(directory / "control.sock"))
    worker_config = {**config, "credentials": credentials}
    config_path = directory / "workers.json"
    _write_json(config_path, worker_config)
    config_path.chmod(0o600)
    server_config = directory / "server.toml"
    server_config.write_text(
        f'[server]\ndata_dir = "{directory / "data"}"\n'
        "[master]\nshutdown_timeout_ms = 10000\n"
        f'[flight]\nendpoint = "{endpoint}"\n'
        "[flight.auth]\nenabled = true\nsession_ttl_ms = 3600000\nmax_sessions = 64\n"
        f'token_file = "{directory / "read.token"}"\n'
        f'writer_token_file = "{directory / "writer.token"}"\n'
        f'admin_token_file = "{directory / "admin.token"}"\n'
        "[flight.ingest]\nenabled = true\n"
    )
    server = _launch(
        stack,
        [
            config["server_bin"],
            config["uri"],
            "--config",
            str(server_config),
            "--log-dir",
            str(directory / "logs"),
        ],
        directory / "server.log",
        server=True,
    )
    processes["server"] = server
    deadline = time.monotonic() + 30
    while True:
        if server.poll() is not None:
            raise RuntimeError("server exited during startup")
        try:
            client = _client(worker_config, admin=True)
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)
    with client:
        namespace_id = uuid.uuid4()
        client.create_namespace("bench", namespace_id=namespace_id, request_id=uuid.uuid4())
        for name in ("ticks", "background"):
            client.create_append_table(
                name,
                _schema(),
                namespace_id=namespace_id,
                table_id=uuid.uuid4(),
                request_id=uuid.uuid4(),
            )
    roles = ["subscriber", "writer"]
    if config["background_rate"]:
        roles.append("background")
    workers = {
        role: _launch(
            stack,
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                role,
                "--worker-config",
                str(config_path),
            ],
            directory / f"{role}.log",
        )
        for role in roles
    }
    processes.update(workers)
    stack.callback(_drain_workers, directory, processes)
    _wait_ready(directory, workers, server, time.monotonic() + 30)
    _write_json(directory / "start.json", time.monotonic_ns() + 250_000_000)
    deadline = time.monotonic() + config["timeout_seconds"]
    while any(process.poll() is None for process in workers.values()):
        if workers["writer"].poll() == 0:
            (directory / "stop").touch()
        if server.poll() is not None:
            raise RuntimeError("server exited during measurement")
        for role, process in workers.items():
            if process.poll() not in (None, 0):
                raise RuntimeError(f"worker failed role=[{role}] status=[{process.returncode}]")
            if role == "background" and process.poll() == 0 and not (directory / "stop").exists():
                raise RuntimeError("background writer exited before the measurement ended")
        if time.monotonic() >= deadline:
            raise TimeoutError("measurement exceeded the cell timeout")
        time.sleep(0.01)


def _analyze(config: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    writer, subscriber = raw.get("writer", {}), raw.get("subscriber", {})
    sequences, sent = writer.get("seq", []), writer.get("sent_ns", [])
    observed = subscriber.get("seq", [])
    integrity = check_integrity(observed, len(sequences))
    failures = [f"{role}: {value['error']}" for role, value in raw.items() if "error" in value]
    if not integrity["passed"]:
        failures.append("sequence integrity failed (including warmup)")
    if not writer.get("end_sent") or subscriber.get("end_seq") != len(sequences):
        failures.append("ordered end marker missing or inconsistent")
    mask = measurement_mask(
        sequences,
        sent,
        writer.get("started_ns", 0),
        config["warmup_seconds"],
        config["warmup_samples"],
    )
    selected = {seq for seq, include in zip(sequences, mask) if include}
    stamps = dict(zip(sequences, sent))
    if any(stamps.get(seq) != stamp for seq, stamp in zip(observed, subscriber.get("sent_ns", []))):
        failures.append("received sent_ns differs from the writer evidence")
    ack = summarize_ns(
        [
            end - start
            for start, end, include in zip(sent, writer.get("ack_ns", []), mask)
            if include
        ]
    )
    visible = summarize_ns(
        [
            received - stamp
            for seq, stamp, received in zip(
                observed, subscriber.get("sent_ns", []), subscriber.get("received_ns", [])
            )
            if seq in selected
        ]
    )
    for name, stats in (("ack", ack), ("visible", visible)):
        if stats["count"] != config["samples"]:
            failures.append(
                f"{name} sample count=[{stats['count']}] expected=[{config['samples']}]"
            )
    judgement = evaluate_thresholds(visible, ack, config["durability"], config["thresholds"])
    failures.extend(judgement["failures"])
    measured_sent = [stamp for stamp, include in zip(sent, mask) if include]
    end = writer.get("finished_ns", 0)
    elapsed = (end - measured_sent[0]) / 1_000_000_000 if measured_sent else 0.0
    rate = len(measured_sent) / elapsed if elapsed > 0 else 0.0
    background = raw.get("background", {"rows": 0, "achieved_rows_per_second": 0.0})
    if config["background_rate"] and background.get("rows", 0) == 0:
        failures.append("requested background load produced no rows")
    return {
        "durability": config["durability"],
        "background_target_rows_per_second": config["background_rate"],
        "subscription_api": "Client.open_log(batch_rows=1)",
        "write_api": "Client.get_writer(batch_size=1, flush_interval_ms=None).write(row)",
        "ack_latency": ack,
        "visible_latency": visible,
        "integrity": integrity,
        "warmup_rows": len(sequences) - len(selected),
        "achieved_probe_rows_per_second": rate,
        "background": background,
        "schedule_lag": summarize_ns(
            [lag for lag, include in zip(writer.get("schedule_lag_ns", []), mask) if include]
        ),
        "tier_b_comparison": {
            "measurement": "open_log delivery proxy; subscribe callback wrapper is not measured",
            "p50_ratio": visible["p50_micros"] / 50 if visible["p50_micros"] is not None else None,
            "p99_ratio": visible["p99_micros"] / 300 if visible["p99_micros"] is not None else None,
            "p99_9_ratio": (
                visible["p99_9_micros"] / 1000 if visible["p99_9_micros"] is not None else None
            ),
        },
        "passed": not failures,
        "failures": failures,
        "skipped_thresholds": judgement["skipped"],
        "raw_samples": raw,
    }


def _run_cell(config: dict[str, Any]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="zippy-public-e2e-") as temporary:
        directory = Path(temporary)
        raw: dict[str, Any] = {}
        processes: dict[str, Any] = {}
        error_text = None
        interrupted = False
        with ExitStack() as stack:
            try:
                _run_processes(config, directory, stack, processes)
            except KeyboardInterrupt:
                error_text = "KeyboardInterrupt: measurement interrupted"
                interrupted = True
            except Exception as error:
                error_text = f"{type(error).__name__}: {error}"
            finally:
                # Stop every owned process before reading evidence or deleting the data directory.
                stack.close()
                for role in ("writer", "subscriber", "background"):
                    path = directory / f"{role}.json"
                    if path.exists():
                        raw[role] = json.loads(path.read_text())
        cell = _analyze(config, raw)
        cell["process_exit_codes"] = {
            role: process.returncode for role, process in processes.items()
        }
        cell["interrupted"] = interrupted
        if processes.get("server") is not None and processes["server"].returncode != 0:
            error_text = error_text or "server shutdown failed"
        if error_text:
            cell["passed"] = False
            cell["failures"].insert(0, error_text)
            cell["diagnostic_logs"] = {
                path.name: path.read_text(errors="replace")[-12000:]
                for path in directory.glob("*.log")
            }
        return cell


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _metadata(root: Path, server: Path) -> dict[str, Any]:
    def git(*args: str) -> str:
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

    cpuinfo = Path("/proc/cpuinfo").read_text()
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in cpuinfo.splitlines()
            if line.startswith("model name")
        ),
        platform.processor(),
    )
    spec = importlib.util.find_spec("zippy._internal")
    extension = Path(spec.origin) if spec and spec.origin else None
    return {
        "git_sha": git("rev-parse", "HEAD"),
        "benchmark_script_sha256": _sha256(Path(__file__).resolve()),
        "git_dirty": bool(git("status", "--porcelain")),
        "kernel": platform.release(),
        "is_wsl": "microsoft" in platform.release().lower(),
        "cpu_model": cpu,
        "logical_cpu_count": os.cpu_count(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "python_version": platform.python_version(),
        "versions": {
            name: importlib.metadata.version(name) for name in ("pyarrow", "zippy", "zippy-client")
        },
        "started_at_utc": datetime.now(UTC).isoformat(),
        "clock": "Linux CLOCK_MONOTONIC; nanoseconds; common clock domain across child processes",
        "server_binary": str(server),
        "server_sha256": _sha256(server),
        "extension_path": str(extension),
        "extension_sha256": _sha256(extension) if extension else None,
        "build_requirement": "release server/extension; commands in baseline report",
        "command_argv": [sys.executable, *sys.argv],
        "pacing": "absolute monotonic deadlines; one write outstanding; no dropped slots",
        "visibility_endpoint": "immediately after one-row open_log next(), before to_pylist()",
        "ack_endpoint": "after synchronous one-row FlightWriter.write() returns",
        "timestamp_boundary": "sent_ns immediately before write; includes Arrow conversion",
        "background_scope": "separate append table in the same namespace and server",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=20_000)
    parser.add_argument(
        "--durability", nargs="+", choices=("memory", "wal"), default=["memory", "wal"]
    )
    parser.add_argument("--rate", type=float, default=1000.0)
    parser.add_argument("--background-rate", nargs="+", type=float, default=[0.0])
    parser.add_argument("--background-batch-rows", type=int, default=1000)
    parser.add_argument("--warmup-seconds", type=float, default=2.0)
    parser.add_argument("--warmup-samples", type=int, default=1000)
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument(
        "--server-bin",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "target/release/zippy-server",
    )
    parser.add_argument("--report-json", type=Path, default=Path("/tmp/zippy-public-e2e.json"))
    for name, default in (
        ("visible-p50", 1000),
        ("visible-p99", 3000),
        ("visible-p99-9", 10000),
        ("wal-ack-p50", 2000),
        ("wal-ack-p99", 5000),
    ):
        parser.add_argument(f"--max-{name}-micros", type=float, default=float(default))
    parser.add_argument(
        "--worker", choices=("writer", "subscriber", "background"), help=argparse.SUPPRESS
    )
    parser.add_argument("--worker-config", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this benchmark requires Linux CLOCK_MONOTONIC shared across processes")
    if args.worker:
        if not args.worker_config:
            parser.error("worker-config is required for child workers")
        return args
    if args.samples <= 0 or args.background_batch_rows <= 0 or args.warmup_samples < 0:
        parser.error("sample/batch counts must be positive and warmup-samples nonnegative")
    for name in (
        "rate",
        "timeout_seconds",
        "warmup_seconds",
        "max_visible_p50_micros",
        "max_visible_p99_micros",
        "max_visible_p99_9_micros",
        "max_wal_ack_p50_micros",
        "max_wal_ack_p99_micros",
    ):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (value == 0 and name != "warmup_seconds"):
            parser.error(f"invalid finite positive limit [{name}]")
    if any(not math.isfinite(value) or value < 0 for value in args.background_rate):
        parser.error("background-rate must be finite and nonnegative")
    args.server_bin = args.server_bin.resolve()
    if not args.server_bin.is_file():
        parser.error(
            "build release server first: cargo build --release -p zippy-server --bin zippy-server"
        )
    return args


def main() -> int:
    """Run the configured matrix and always publish the completed evidence on interruption.

    :returns: 0 for success, 1 for a failed cell/runtime, or 130 for keyboard interruption.
    :raises SystemExit: Invalid arguments or unsupported platform.
    :example: uv run --no-sync scripts/bench_public_e2e.py --durability memory --samples 3000
    """
    args = _parse_args()
    if args.worker:
        return _worker(args.worker, args.worker_config)
    config = {
        key: value
        for key, value in vars(args).items()
        if key not in {"worker", "worker_config", "report_json"}
    }
    config["server_bin"] = str(args.server_bin)
    config["thresholds"] = {
        key.removeprefix("max_"): value for key, value in config.items() if key.startswith("max_")
    }
    report: dict[str, Any] = {
        "schema_version": 1,
        "config": config,
        "metadata": _metadata(Path(__file__).resolve().parents[1], args.server_bin),
        "cells": [],
    }
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    status = 1
    try:
        for durability in args.durability:
            for background_rate in args.background_rate:
                print(
                    f"running durability=[{durability}] background_rate=[{background_rate}]",
                    flush=True,
                )
                cell = _run_cell(
                    {**config, "durability": durability, "background_rate": background_rate}
                )
                report["cells"].append(cell)
                _write_json(args.report_json, report)
                if cell["interrupted"]:
                    raise KeyboardInterrupt
                print(
                    json.dumps(
                        {
                            key: value
                            for key, value in cell.items()
                            if key not in {"raw_samples", "diagnostic_logs"}
                        }
                    ),
                    flush=True,
                )
        status = exit_code(report["cells"])
    except KeyboardInterrupt:
        report["error"] = "interrupted by keyboard; owned children and server cleaned up"
        status = 130
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        report.update(
            passed=status == 0, exit_code=status, finished_at_utc=datetime.now(UTC).isoformat()
        )
        _write_json(args.report_json, report)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
