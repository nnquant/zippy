"""Native table CLI identity, lifecycle, schema and error contracts."""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace
import uuid

from click.testing import CliRunner
import pyarrow as pa
import pytest
from zippy_client import AuthenticationError

from zippy.cli import main
from zippy import cli_table


@pytest.fixture
def native_cli(monkeypatch):
    events = []
    schema = pa.schema(
        [pa.field("dt", pa.timestamp("ns", tz="UTC"), nullable=False, metadata={b"unit": b"\xff"})],
        metadata={b"origin": b"native"},
    )
    info = {"name": "ticks", "namespace": "market", "catalog": "zippy", "table_type": "TABLE"}

    class Table:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            events.append("table_closed")

        def info(self):
            return dict(info)

        def schema(self):
            return schema

        def head(self, count):
            assert count == 0
            return self

        def profile(self):
            return {
                "metrics": {
                    "query": {
                        "snapshots": [{"table_id": str(uuid.UUID(int=1)), "snapshot_version": 2}]
                    }
                }
            }

    class Client:
        def __init__(self, uri):
            events.append(("connect", uri))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            events.append("client_closed")

        def list_tables(self, *, namespace):
            events.append(("list", namespace))
            return [info]

        def read_table(self, name, *, namespace, snapshot=False):
            events.append(("read", name, namespace, snapshot))
            return Table()

        def drop_table(self, table_id, *, expected_snapshot_version, request_id):
            events.append(("drop", table_id, expected_snapshot_version, request_id))
            return {"table_id": str(table_id), "request_id": str(request_id)}

    monkeypatch.setattr(cli_table, "Client", Client)
    return SimpleNamespace(events=events, schema=schema)


@pytest.mark.parametrize("as_json", [False, True])
def test_native_cli_lists_namespaces(native_cli, as_json):
    args = ["table", "ls", "--namespace", "market"] + (["--json"] if as_json else [])
    result = CliRunner().invoke(main, args)
    assert result.exit_code == 0, result.output
    assert "ticks" in result.output and "market" in result.output
    assert ("list", "market") in native_cli.events
    assert native_cli.events[-1] == "client_closed"


@pytest.mark.parametrize("command", ["info", "schema"])
@pytest.mark.parametrize("as_json", [False, True])
def test_native_cli_schema_preserves_arrow_contract(native_cli, command, as_json):
    result = CliRunner().invoke(
        main, ["table", command, "ticks", "--namespace", "market", *(["--json"] if as_json else [])]
    )
    assert result.exit_code == 0, result.output
    if as_json:
        data = json.loads(result.output)
        schema = data["schema"] if command == "info" else data
        assert schema["fields"][0]["data_type"] == "timestamp[ns, tz=UTC]"
        assert schema["fields"][0]["nullable"] is False
        assert base64.b64decode(schema["fields"][0]["metadata_base64"][0]["value"]) == b"\xff"
        if command == "info":
            assert data["snapshots"][0]["snapshot_version"] == 2
    else:
        assert "UTC" in result.output
    assert ("read", "ticks", "market", command == "info") in native_cli.events
    assert native_cli.events[-1] == "client_closed"


def test_native_drop_preserves_identity_and_idempotent_request(native_cli):
    args = [
        "table",
        "drop",
        str(uuid.UUID(int=1)),
        "--expected-snapshot-version",
        "2",
        "--request-id",
        str(uuid.UUID(int=2)),
        "--yes",
        "--json",
    ]
    for _ in range(2):
        result = CliRunner().invoke(main, args)
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["table_id"] == str(uuid.UUID(int=1))
    assert native_cli.events.count(("drop", uuid.UUID(int=1), 2, uuid.UUID(int=2))) == 2


def test_native_drop_cancel_does_not_connect(native_cli):
    result = CliRunner().invoke(
        main,
        [
            "table",
            "drop",
            str(uuid.UUID(int=1)),
            "--expected-snapshot-version",
            "2",
            "--request-id",
            str(uuid.UUID(int=2)),
        ],
        input="n\n",
    )
    assert result.exit_code == 0 and "drop aborted" in result.output
    assert native_cli.events == []


@pytest.mark.parametrize(
    "args",
    [
        ["stream", "ls"],
        ["table", "drop", "old-name"],
        ["table", "ls", "--control-endpoint", "old"],
        ["table", "drop", str(uuid.UUID(int=1)), "--keep-persisted"],
        ["table", "drop", str(uuid.UUID(int=1)), "--token", "old"],
    ],
)
def test_removed_cli_paths_are_rejected(native_cli, args):
    result = CliRunner().invoke(main, args)
    assert result.exit_code == 2
    assert native_cli.events == []


def test_native_cli_does_not_hide_authentication_failure(monkeypatch):
    def connect(uri):
        raise AuthenticationError("native read credential rejected")

    monkeypatch.setattr(cli_table, "Client", connect)
    result = CliRunner().invoke(main, ["table", "ls"])
    assert result.exit_code == 1
    assert "native read credential rejected" in result.output
