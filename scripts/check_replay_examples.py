"""Run current replay examples against the isolated native acceptance server."""

from pathlib import Path
import os
import subprocess
import sys
import uuid


def verify_replay_examples(
    root: Path, directory: Path, namespace_id: uuid.UUID, credentials: dict[str, str]
) -> None:
    """Exercise native file publication, fixed comparison and log-driven latest computation.

    :param root: Current repository root containing the executable examples.
    :param directory: Isolated acceptance directory and running control socket.
    :param namespace_id: Existing market namespace identity.
    :param credentials: Test-only read/writer/admin credentials created by the harness.
    :raises RuntimeError: An example fails; its captured diagnostic output is retained.
    """
    env = {
        **os.environ,
        "ZIPPY_FLIGHT_AUTH_TOKEN": credentials["read"],
        "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN": credentials["writer"],
        "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN": credentials["admin"],
    }
    common = ["--uri", str(directory / "control.sock"), "--namespace", "market"]
    examples = root / "examples/06_replay"
    commands = [
        [
            str(examples / "01_parquet_replay_to_stream_table.py"),
            *common,
            "--namespace-id",
            str(namespace_id),
            "--table",
            name,
            "--root",
            str(directory / "example-input"),
        ]
        for name in ("example_replay_left", "example_replay_right")
    ]
    commands += [
        [
            str(examples / "02_replay_parity_check.py"),
            *common,
            "--live-table",
            "example_replay_left",
            "--replay-table",
            "example_replay_right",
        ],
        [
            str(examples / "03_replay_to_reactive_latest_engine.py"),
            *common,
            "--namespace-id",
            str(namespace_id),
        ],
    ]
    # The old independent-client smoke used an in-process Master/Gateway pair.
    # Keep its package-isolation and real write/query assertions on the native service.
    smoke = """
import json
import sys
import uuid
import pyarrow as pa
from zippy_client import Client, col

assert "zippy" not in sys.modules
with Client(sys.argv[1]) as client:
    schema = pa.schema([("instrument_id", pa.string()), ("last_price", pa.float64())])
    client.create_append_table(
        "independent_client_smoke", schema, namespace_id=uuid.UUID(sys.argv[2]),
        table_id=uuid.UUID(int=9030), request_id=uuid.UUID(int=9031)
    )
    with client.get_writer("independent_client_smoke", namespace="market", batch_size=1) as writer:
        writer.write({"instrument_id": "IF2606", "last_price": 4102.5})
    with client.read_table("independent_client_smoke", namespace="market") as table:
        result = table.filter(col("instrument_id") == "IF2606").select(
            "instrument_id", "last_price"
        ).collect()
    assert result.to_pydict() == {"instrument_id": ["IF2606"], "last_price": [4102.5]}
assert "zippy" not in sys.modules
print(json.dumps({"independent_client_native_smoke": True}))
"""
    commands.append(["-I", "-c", smoke, str(directory / "control.sock"), str(namespace_id)])
    for index, command in enumerate(commands):
        log_path = directory / f"replay-example-{index}.log"
        try:
            with log_path.open("w") as output:
                subprocess.run(
                    [sys.executable, *command],
                    cwd=directory,
                    env=env,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=45,
                )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            raise RuntimeError(
                f"native example failed index=[{index}] output=[{log_path.read_text()}]"
            ) from error
