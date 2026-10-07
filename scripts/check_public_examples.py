"""Exercise current public examples and their persistent results across native restarts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pyarrow as pa
from zippy_client import Client


def verify_public_examples(
    client: Client,
    directory: Path,
    namespace_id: uuid.UUID,
    credentials: dict[str, str],
    iteration: int,
) -> None:
    """Run isolated scripts and compare three-kind data and compute after every restart.

    :param client: Administrator client for recovery assertions.
    :param directory: Current service directory containing the control socket.
    :param namespace_id: Existing market namespace UUID.
    :param credentials: Temporary read/writer/administrator bootstrap credentials.
    :param iteration: Zero creates fixtures; later generations check the same identities.
    :raises AssertionError: Any script, query, callback or recovery disagrees with expected data.
    """
    root = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "ZIPPY_FLIGHT_AUTH_TOKEN": credentials["read"],
        "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN": credentials["writer"],
        "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN": credentials["admin"],
    }
    common = ["--uri", str(directory / "control.sock"), "--namespace", "market"]

    def invoke(path: str, *args: str, local: bool = False, success: bool = True) -> object:
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                str(root / "examples" / path),
                *([] if local else common),
                *args,
            ],
            env=env,
            text=True,
            capture_output=True,
            timeout=35,
        )
        assert (result.returncode == 0) == success, (path, result.stdout, result.stderr)
        return json.loads(result.stdout) if success else result.stderr

    if iteration == 0:
        # The creation identity is random; data assertions below bind to its Catalog name.
        created = invoke(
            "01_quickstart/01_create_write_read.py",
            "--namespace-id",
            str(namespace_id),
            "--table",
            "public_ticks",
        )
        assert isinstance(created, dict) and created["prices"] == [10.0, 11.0, 12.0, 13.0]
        state = invoke(
            "02_stream_table/01_native_state_tables.py",
            "--namespace-id",
            str(namespace_id),
            "--prefix",
            "public_state",
        )
        assert isinstance(state, dict) and state["key_value"]["price"] == [11.0]
        chain = invoke("05_engines/01_native_local_chain.py", local=True)
        assert chain == {"rows": 4, "ranks": [1.0, 2.0, 1.0, 2.0]}
        for kind in ("latest", "timeseries", "cross_sectional"):
            report = invoke(
                "05_engines/02_native_table_compute.py",
                "--namespace-id",
                str(namespace_id),
                "--source",
                "public_ticks",
                "--output",
                f"public_{kind}",
                "--kind",
                kind,
                "--window-ns",
                "2",
                "--through-offset",
                "4",
            )
            assert isinstance(report, dict) and report["metrics"]["input_rows"] == 4
        # Explicit ownership closes process heartbeats even when user code raises.
        failed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                "from zippy import Client; import sys; "
                "exec(\"with Client(sys.argv[1], app='public_failure') as client:\\n"
                "    raise RuntimeError('intentional owned-client failure')\")",
                str(directory / "control.sock"),
            ],
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
        )
        assert failed.returncode != 0 and "intentional owned-client failure" in failed.stderr
        assert "FATAL: exception not rethrown" not in failed.stderr
        # Observe a real NOT_FOUND response before creating the table: this proves
        # waiting across late Catalog creation, rather than merely opening an existing table.
        wait_program = """
import sys
from zippy_client import Client, ServerError
with Client(sys.argv[1]) as client:
    original = client.flight.get_table_schema
    def observed(*args, **kwargs):
        try:
            return original(*args, **kwargs)
        except ServerError as error:
            if error.code == 'NOT_FOUND':
                print('waiting', flush=True)
            raise
    client.flight.get_table_schema = observed
    with client.read_table('public_late', namespace='market', wait=True, timeout=10) as table:
        assert table.collect().num_rows == 0
    print('ready', flush=True)
"""
        child = subprocess.Popen(
            [sys.executable, "-I", "-c", wait_program, str(directory / "control.sock")],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            assert child.stdout is not None
            assert child.stdout.readline().strip() == "waiting"
            client.create_append_table(
                "public_late",
                pa.schema([("value", pa.int64())]),
                namespace_id=namespace_id,
                table_id=uuid.uuid4(),
                request_id=uuid.uuid4(),
            )
            output, errors = child.communicate(timeout=12)
            assert child.returncode == 0 and output.strip().endswith("ready"), (output, errors)
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)

    queried = invoke("03_query/01_fixed_query.py", "--table", "public_ticks")
    assert queried == {
        "instrument_id": ["B", "A", "B"],
        "last_price": [11.0, 12.0, 13.0],
        "notional": [22.0, 36.0, 52.0],
    }
    for mode in ("rows", "tables"):
        callbacks = invoke(
            "04_subscribe/01_native_callbacks.py", "--table", "public_ticks", "--mode", mode
        )
        assert isinstance(callbacks, dict) and callbacks["rows"] == 4
    waited = invoke("07_ops/01_wait_for_table.py", "--table", "public_ticks")
    assert isinstance(waited, dict) and waited["rows"] == 4
    failure = invoke(
        "07_ops/01_wait_for_table.py",
        "--table",
        "public_absent",
        "--timeout",
        "0.05",
        success=False,
    )
    assert "TimeoutError" in str(failure), failure

    def read(name: str, order: list[str]) -> pa.Table:
        with client.read_table(name, namespace="market") as table:
            with table.sort(order) as ordered:
                return ordered.collect()

    assert read("public_state_frames", ["dt", "symbol"]).to_pydict() == {
        "symbol": ["A", "B", "A", "B"],
        "dt": [0, 0, 1, 1],
        "price": [10.0, 20.0, 11.0, 21.0],
    }
    assert read("public_state_latest", ["symbol"]).to_pydict() == {
        "symbol": ["A"],
        "dt": [1],
        "price": [11.0],
    }
    assert read("public_latest", ["instrument_id"]).column("last_price").to_pylist() == [12.0, 13.0]
    assert read("public_timeseries", ["window_start", "instrument_id"]).column(
        "volume"
    ).to_pylist() == [1, 2, 3, 4]
    assert read("public_cross_sectional", ["dt", "instrument_id"]).column("rank").to_pylist() == [
        1.0,
        2.0,
        1.0,
        2.0,
    ]
