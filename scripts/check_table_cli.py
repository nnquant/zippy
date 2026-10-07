"""Verify native table commands against the same server before and after restart."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, TypedDict
import uuid

import pyarrow as pa
from zippy_client import Client


class _TableIdentity(TypedDict):
    namespace_id: uuid.UUID
    table_id: uuid.UUID
    request_id: uuid.UUID


def verify_table_cli(
    client: Client,
    directory: Path,
    namespace_id: uuid.UUID,
    credentials: dict[str, str],
    iteration: int,
) -> None:
    """Exercise namespace listing, exact schema, coherent metadata and guarded deletion.

    :param client: Native administration client.
    :param directory: Isolated running server directory.
    :param namespace_id: Existing market namespace identity.
    :param credentials: Test-only Flight bootstrap credentials.
    :param iteration: Server restart generation.
    :raises AssertionError: CLI output, authorization or mutation semantics differ.
    """
    env = {
        **os.environ,
        "ZIPPY_FLIGHT_AUTH_TOKEN": credentials["read"],
        "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN": credentials["writer"],
        "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN": credentials["admin"],
    }

    def invoke(*args: str, success: bool = True, input_text: str | None = None) -> Any:
        command = [
            sys.executable,
            "-I",
            "-m",
            "zippy",
            "table",
            *args,
            "--uri",
            str(directory / "control.sock"),
            "--json",
        ]
        result = subprocess.run(
            command, env=env, capture_output=True, text=True, input=input_text, timeout=20
        )
        assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
        return json.loads(result.stdout) if success else result.stdout + result.stderr

    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("dt", pa.timestamp("ns", tz="UTC"), nullable=False),
        ],
        metadata={b"source": b"native-cli", b"version": b"1"},
    )
    data = pa.table({"id": ["A"], "dt": [1]}, schema=schema)
    for index, kind in enumerate(("append", "frame", "key_value")):
        name = f"cli_{kind}"
        if iteration == 0:
            kwargs: _TableIdentity = {
                "namespace_id": namespace_id,
                "table_id": uuid.UUID(int=9300 + index),
                "request_id": uuid.UUID(int=9310 + index),
            }
            if kind == "append":
                client.create_append_table(name, schema, **kwargs)
            elif kind == "frame":
                client.create_frame_table(name, schema, by="dt", **kwargs)
            else:
                client.create_key_value_table(name, schema, keys=["id"], **kwargs)
            client.flight.write_table(
                name,
                data,
                namespace="market",
                durability="persisted",
                mutation={"append": "append", "frame": "frame", "key_value": "upsert"}[kind],
            )
        info = invoke("info", name, "--namespace", "market")
        assert info["namespace"] == "market" and info["name"] == name
        assert info["snapshots"][0]["snapshot_version"] >= 1
        fields = invoke("schema", name, "--namespace", "market")["fields"]
        assert [field["name"] for field in fields] == ["id", "dt"]
        assert fields[1]["data_type"] == "timestamp[ns, tz=UTC]"
        assert fields[1]["nullable"] is False
    names = invoke("ls", "--namespace", "market")
    assert {f"cli_{kind}" for kind in ("append", "frame", "key_value")} <= {
        entry["name"] for entry in names
    }
    assert all(entry["namespace"] == "market" for entry in names)
    assert invoke("ls", "--namespace", "not-present") == []
    if iteration != 0:
        return
    target_id = uuid.UUID(int=9350)
    client.create_append_table(
        "cli_drop",
        schema,
        namespace_id=namespace_id,
        table_id=target_id,
        request_id=uuid.UUID(int=9351),
    )
    receipts = client.flight.write_table(
        "cli_drop", data, namespace="market", durability="persisted"
    )
    version = int(str(receipts[-1]["persisted_snapshot_version"]))
    assert (
        "snapshot"
        in invoke(
            "drop",
            str(target_id),
            "--expected-snapshot-version",
            str(version + 1),
            "--request-id",
            str(uuid.UUID(int=9352)),
            "--yes",
            success=False,
        ).lower()
    )
    assert client.read_table("cli_drop", namespace="market").collect().num_rows == 1
    args = (
        "drop",
        str(target_id),
        "--expected-snapshot-version",
        str(version),
        "--request-id",
        str(uuid.UUID(int=9353)),
        "--yes",
    )
    first = invoke(*args)
    assert invoke(*args) == first
    assert "cli_drop" not in {row["name"] for row in client.list_tables(namespace="market")}
