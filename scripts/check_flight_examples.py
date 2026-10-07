"""Run SDK-only remote examples against the native server before and after restart."""

from __future__ import annotations

import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from typing import Any
import uuid

import pyarrow as pa
from zippy_client import Client


def verify_flight_examples(
    client: Client,
    directory: Path,
    namespace_id: uuid.UUID,
    credentials: dict[str, str],
    iteration: int,
) -> None:
    """Verify finite persisted writes, fixed reads, callbacks and atomic frame delivery.

    :param client: Native administrator used only to create isolated test fixtures.
    :param directory: Running test server directory.
    :param namespace_id: Existing market namespace.
    :param credentials: Test-only bootstrap credentials for child processes.
    :param iteration: Server generation, zero creates and writes fixtures.
    :raises AssertionError: The SDK-only child imports the server package or loses data.
    """
    root = Path(__file__).resolve().parents[1]
    examples = root / "examples/08_remote_flight"
    writer = examples / "01_write_ticks.py"
    probe = examples / "02_read_and_subscribe.py"

    def invoke(script: Path, *args: str, writable: bool = False, success: bool = True) -> Any:
        env = {**os.environ, "ZIPPY_FLIGHT_AUTH_TOKEN": credentials["read"]}
        env.pop("ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN", None)
        env.pop("ZIPPY_FLIGHT_AUTH_WRITER_TOKEN", None)
        if writable:
            env["ZIPPY_FLIGHT_AUTH_WRITER_TOKEN"] = credentials["writer"]
        command = [
            sys.executable,
            "-I",
            "-c",
            "import runpy,sys; script=sys.argv.pop(1); sys.argv[0]=script; "
            "runpy.run_path(script,run_name='__main__'); "
            "assert 'zippy' not in sys.modules, 'server package imported'",
            str(script),
            "--uri",
            str(directory / "control.sock"),
            "--namespace",
            "market",
            *args,
        ]
        result = subprocess.run(command, env=env, text=True, capture_output=True, timeout=20)
        assert (result.returncode == 0) == success, (
            script.name,
            args,
            result.stdout,
            result.stderr,
        )
        return json.loads(result.stdout) if success else result.stdout + result.stderr

    if iteration == 0:
        schema = runpy.run_path(str(writer))["build_schema"]()
        client.create_append_table(
            "flight_example_ticks",
            schema,
            namespace_id=namespace_id,
            table_id=uuid.UUID(int=9500),
            request_id=uuid.UUID(int=9501),
        )
        report = invoke(
            writer,
            "--table",
            "flight_example_ticks",
            "--rows",
            "3",
            "--batch-size",
            "2",
            "--start-ns",
            "1",
            writable=True,
        )
        assert report["rows"] == 3 and report["durability"] == "persisted"
        rejected = invoke(writer, "--table", "flight_example_ticks", "--rows", "1", success=False)
        assert "auth" in rejected.lower() or "permission" in rejected.lower(), rejected
        schema = pa.schema([("id", pa.string()), ("dt", pa.int64())])
        client.create_frame_table(
            "flight_probe_frames",
            schema,
            by="dt",
            namespace_id=namespace_id,
            table_id=uuid.UUID(int=9510),
            request_id=uuid.UUID(int=9511),
        )
        for frame in (1, 2):
            client.flight.write_table(
                "flight_probe_frames",
                pa.table({"id": ["A", "B"], "dt": [frame, frame]}, schema=schema),
                namespace="market",
                mutation="frame",
                durability="persisted",
            )
    with client.read_table("flight_example_ticks", namespace="market") as table:
        with table.sort("dt") as ordered:
            actual = ordered.collect()
            assert actual.column("dt").cast(pa.int64()).to_pylist() == [1, 2, 3]
            assert actual.column("volume").to_pylist() == [1, 2, 3]
    # More than the configured 32 sessions, without waiting for the 60-second TTL.
    # Deliberately leave each snapshot unreleased: Client.close owns its cleanup.
    for _ in range(40):
        with Client(str(directory / "control.sock"), flight_token=credentials["read"]) as short:
            snapshot = short.flight.create_snapshot(
                {"ticks": {"namespace": "market", "table": "flight_example_ticks"}}
            )
            assert snapshot["tables"]
    for mode in ("rows", "tables"):
        report = invoke(
            probe,
            "--table",
            "flight_example_ticks",
            "--rows",
            "3",
            "--batch-rows",
            "1",
            "--mode",
            mode,
            "--order-by",
            "dt",
        )
        assert report["query_rows"] == 3
        assert report["snapshots"][0]["table_id"] == str(uuid.UUID(int=9500))
        delivery = report["delivery"]
        assert delivery["received_rows"] == delivery["subscriber"]["rows_delivered_total"] == 3
        assert delivery["subscriber"]["processed_next_offset"] == 3
    frame = invoke(
        probe,
        "--table",
        "flight_probe_frames",
        "--rows",
        "3",
        "--batch-rows",
        "1",
        "--order-by",
        "dt",
        "--order-by",
        "id",
    )
    assert frame["delivery"]["received_rows"] == 4
    assert frame["delivery"]["sample_batch_rows"] == [2, 2]
    assert frame["delivery"]["callbacks"] == 2
    kv = invoke(probe, "--table", "cli_key_value", "--rows", "1", "--order-by", "dt")
    assert kv["delivery"]["received_rows"] == 1 and kv["query_rows"] == 1
