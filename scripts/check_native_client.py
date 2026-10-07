"""Verify Python-to-Rust native creation, WAL/Persisted writes and restart recovery.

:returns: Exit status zero after an empty-directory and restart acceptance pass.
:raises AssertionError: If management, receipts, recovery or shutdown violates the contract.
:example: uv run --no-sync scripts/check_native_client.py
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Literal

import pyarrow as pa
from zippy import Client
from zippy_client.log_reader import LogCursor
from zippy_client.writer import FlightWriter

# Isolated wheel verification uses Python -I, which excludes the script directory.
# Expose only acceptance helpers; production packages still resolve from the installed wheel.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_native_fixed_snapshot import verify_fixed_snapshots
from check_compute_recovery import verify_compute_recovery
from check_compute_kv_input import verify_compute_kv_input
from check_compute_plugin import verify_compute_plugin
from check_replay_examples import verify_replay_examples
from check_table_cli import verify_table_cli
from check_native_webui import verify_native_webui
from check_flight_examples import verify_flight_examples
from check_public_examples import verify_public_examples


def verify_shared_sql(client: Client, iteration: int, namespace_id: uuid.UUID) -> None:
    """Execute shared compiler regressions on DataFusion before and after restart.

    :param client: Authenticated native client for this server generation.
    :param iteration: Restart index, starting at zero.
    :param namespace_id: Existing market namespace identity.
    :raises AssertionError: If join keys, column order or shared expressions differ.
    """
    import zippy
    from datetime import datetime, timezone

    inputs = {
        "sql_left": pa.table({"id": ["A", "B"], "value": [10, 20], "last": [100, 200]}),
        "sql_right": pa.table({"id": ["B", "C"], "label": ["matched", "right_only"]}),
        "sql_time": pa.table({"ts": pa.array([0, 1, 2], type=pa.timestamp("ns", tz="UTC"))}),
    }
    if iteration == 0:
        for index, (name, data) in enumerate(inputs.items()):
            client.create_append_table(
                name,
                data.schema,
                namespace_id=namespace_id,
                table_id=uuid.UUID(int=9000 + index),
                request_id=uuid.UUID(int=9010 + index),
            )
            client.flight.write_table(name, data, namespace="market", durability="persisted")
    left = client.read_table("sql_left", namespace="market")
    right = client.read_table("sql_right", namespace="market")
    aggregate = client.read_sql(
        "SELECT count(*) AS rows, sum(value) AS total FROM ticks",
        bindings={"ticks": {"namespace": "market", "table": "sql_left"}},
        memory_limit_bytes=4 * 1024 * 1024 * 1024,
        result_batch_rows=1,
    )
    assert aggregate.to_pydict() == {"rows": [2], "total": [30]}, aggregate
    buffered_name = "flight_buffered_plan"
    if iteration == 0:
        client.create_append_table(
            buffered_name,
            pa.schema([("sequence", pa.int64()), ("instrument_id", pa.string())]),
            namespace_id=namespace_id,
            table_id=uuid.UUID(int=9020),
            request_id=uuid.UUID(int=9021),
        )
        with client.get_writer(
            buffered_name, namespace="market", batch_size=2, durability="persisted"
        ) as writer:
            for sequence in (3, 1, 2):
                writer.write({"sequence": sequence, "instrument_id": "A"})
    with client.read_table(buffered_name, namespace="market") as base:
        plan = base.sort("sequence").slice(1, 1).rename({"sequence": "seq"}).drop("instrument_id")
        with plan:
            result = plan.profile()["result"]
            assert isinstance(result, pa.Table)
            assert result.to_pydict() == {"seq": [2]}, (result.to_pydict(), plan.explain())
            assert "LIMIT 1 OFFSET 1" in str(plan.explain()["sql"])
        selected = base.filter(zippy.col("sequence") >= 2).select("sequence").sort("sequence")
        with selected:
            assert selected.collect().to_pydict() == {"sequence": [2, 3]}
        for descending in (False, True):
            ordered = base.sort("sequence", descending=descending)
            expected = [3, 2, 1] if descending else [1, 2, 3]
            for query, values in (
                (ordered.head(2), expected[:2]),
                (ordered.slice(1), expected[1:]),
                (ordered.tail(2), expected[-2:]),
                (ordered.rename(sequence="seq").slice(1, 1).select("seq"), expected[1:2]),
            ):
                with query:
                    actual = query.collect().column(0).to_pylist()
                    assert actual == values, (actual, values, query.explain())
    replaced = left.with_columns((zippy.col("value") + 1).alias("value")).sort("id").collect()
    assert replaced.to_pydict() == {"id": ["A", "B"], "value": [11, 21], "last": [100, 200]}
    assert replaced.column_names == ["id", "value", "last"]
    ranged = left.filter(zippy.col("value").is_between(10, 15)).collect()
    assert ranged.column("id").to_pylist() == ["A"]
    time_table = client.read_table("sql_time", namespace="market")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert time_table.filter(zippy.col("ts") >= epoch).collect().num_rows == 3
    exact = time_table.filter(zippy.col("ts") == "1970-01-01T00:00:00.000000001Z").collect()
    assert exact.column("ts").cast(pa.int64()).to_pylist() == [1]
    shifted = time_table.filter(zippy.col("ts") == "19700101T08:00:00.000000001+0800").collect()
    assert shifted.equals(exact, check_metadata=True)
    for invalid in ("1970-0101", "1970-01 00:00", "1970-01-01T00:00:00+00:60"):
        try:
            time_table.filter(zippy.col("ts") == invalid).collect()
        except ValueError as error:
            assert "invalid temporal literal" in str(error)
        else:
            raise AssertionError(f"invalid temporal literal was accepted [{invalid}]")
    from zippy_client import SubscriptionOptions

    rows: list[zippy.Row] = []
    subscriber = client.subscribe(
        "sql_time",
        rows.append,
        options=SubscriptionOptions(namespace="market", start="earliest"),
    )
    try:
        deadline = time.monotonic() + 5
        while subscriber.cursor is None or subscriber.cursor.next_offset < 3:
            subscriber.join(0)
            assert time.monotonic() < deadline, subscriber.metrics()
            time.sleep(0.002)
        assert all(isinstance(row, zippy.Row) for row in rows)
        assert [row["ts"] for row in rows] == [0, 1, 2]
    finally:
        subscriber.stop()
    from zippy_client.errors import ServerError
    from zippy_client.sql import compile_flight_table_sql

    for mode, ids in (("right", ["B", "C"]), ("full", ["A", "B", "C"])):
        query = left.join(right, on="id", how=mode).sort("id")
        compiled = compile_flight_table_sql(query)
        try:
            client.flight.read_sql(
                compiled.sql,
                compiled.bindings,
                memory_limit_bytes=1073741824,
            )
        except ServerError as error:
            assert error.code == "RESOURCE_EXHAUSTED", error
        else:
            raise AssertionError("derived join unexpectedly bypassed the 1 GiB memory bound")
        joined = query.collect()
        assert joined.column("id").to_pylist() == ids
        assert joined.column("label").to_pylist()[-1] == "right_only"
        assert joined.column("value").to_pylist()[-1] is None


def _server_command(root: Path, directory: Path, config: Path, installed: bool) -> list[str]:
    """Select the installed CLI or repository binary for the same recovery contract."""
    prefix = (
        [sys.executable, "-I", "-m", "zippy", "server", "run", "--foreground"]
        if installed
        else [
            str(root / "scripts/cargo-test-python-lib-runner.sh"),
            str(root / "target/debug/zippy-server"),
        ]
    )
    return [
        *prefix,
        str(directory / "control.sock"),
        "--config",
        str(config),
        "--log-dir",
        str(directory / "logs"),
    ]


def verify_local_read_mode(
    root: Path, directory: Path, config: Path, *, installed: bool = False
) -> None:
    """Verify server-issued local sessions against the same recovered native tables.

    :param root: Source root containing the built server and loader wrapper.
    :param directory: Isolated test directory holding the existing native database.
    :param config: Configuration to switch explicitly to loopback read-only bootstrap.
    :raises AssertionError: Recovery, identity, or legacy-header rejection fails.
    """
    import pyarrow.flight as flight
    from zippy_client._generated import protocol_pb2 as wire

    configured = config.read_text()
    configured = configured.replace(
        "[flight.auth]\n    enabled = true", "[flight.auth]\n    enabled = false"
    )
    configured = configured.replace(
        "[flight.ingest]\n    enabled = true", "[flight.ingest]\n    enabled = false"
    )
    configured = "\n".join(line for line in configured.splitlines() if "token_file =" not in line)
    config.write_text(configured)
    log_path = directory / "server-local-read.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            _server_command(root, directory, config, installed),
            env={**os.environ, "PYO3_PYTHON": sys.executable},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError(log_path.read_text())
                try:
                    client = Client(str(directory / "control.sock"))
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
            with client:
                assert not client.flight.capabilities.authentication_required
                assert "auth.local_read_handshake.v1" in client.flight.capabilities.names
                assert client.read_table("ticks", namespace="market").collect().to_pydict() == {
                    "value": [42, 43]
                }
                state = client.snapshot_state("quotes", namespace="market")
                assert state.num_rows == 0
                assert state.schema.metadata[b"zippy_kv_seq"] == b"10"
                with client.open_log(
                    "frames", namespace="market", start="earliest", batch_rows=1
                ) as reader:
                    assert next(reader).table.to_pydict() == {"value": [10, None]}
                    assert next(reader).table.to_pydict() == {"value": [12]}
                session = client.flight._connection("read").session
                assert session.bearer_token.startswith("zfs1_")
                assert session.expires_at_ns > time.time_ns()
                # A fresh channel holding only the old identity header cannot use the service.
                raw = flight.connect(f"grpc://{client.flight.endpoint}")
                try:
                    options = flight.FlightCallOptions(
                        timeout=2,
                        headers=[
                            (
                                b"x-zippy-session-id",
                                str(uuid.UUID(bytes=session.session_id)).encode(),
                            )
                        ],
                    )
                    descriptor = flight.FlightDescriptor.for_command(
                        wire.QueryCommand(protocol_version=1).SerializeToString()
                    )
                    try:
                        raw.get_flight_info(descriptor, options)
                    except flight.FlightUnauthenticatedError:
                        pass
                    else:
                        raise AssertionError("legacy session header unexpectedly authenticated")
                finally:
                    raw.close()
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise
            if process.returncode != 0:
                raise RuntimeError(log_path.read_text())


def verify_public_drop(
    client: Client,
    iteration: int,
    namespace_id: uuid.UUID,
    versions: dict[str, int],
    results: dict[str, dict[str, object]],
    cursors: dict[str, LogCursor],
) -> None:
    """Verify three table kinds across public creation, restart, deletion and replay.

    :param client: Authenticated native client for this server generation.
    :param iteration: Restart index, starting at zero.
    :param namespace_id: Existing test namespace identity.
    :param versions: Snapshot versions acknowledged before restart.
    :param results: Exact committed creation and deletion responses retained for replay.
    :param cursors: Delivered old-table cursors that must never attach to replacement identities.
    :raises AssertionError: If identity, authorization, recovery or deletion differs.
    """
    import pyarrow.flight as flight
    from zippy_client._generated import protocol_pb2 as wire
    from zippy_client.errors import ServerError

    schema = pa.schema([("value", pa.string())])
    for index, kind in enumerate(("append", "frame", "key_value")):
        name = f"drop_{kind}"
        table_id, create_id, drop_id = (uuid.UUID(int=800 + 10 * index + n) for n in (0, 1, 2))

        def create(identity: uuid.UUID, request: uuid.UUID) -> dict[str, object]:
            """Create the selected kind with an exact request identity."""
            if kind == "append":
                return client.create_append_table(
                    name, schema, namespace_id=namespace_id, table_id=identity, request_id=request
                )
            elif kind == "frame":
                return client.create_frame_table(
                    name,
                    schema,
                    by="value",
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request,
                )
            else:
                return client.create_key_value_table(
                    name,
                    schema,
                    keys=["value"],
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request,
                )

        def write(value: str) -> int:
            """Persist one distinguishing value and return its snapshot version."""
            batch = pa.table({"value": [value]})
            if kind == "key_value":
                receipts = client.upsert_key_value(
                    name, batch, namespace="market", durability="persisted"
                )
            elif kind == "frame":
                receipts = client.write_frame(
                    name, batch, namespace="market", durability="persisted"
                )
            else:
                receipts = client.flight.write_table(
                    name, batch, namespace="market", durability="persisted"
                )
            version = receipts[-1]["persisted_snapshot_version"]
            assert isinstance(version, int) and version > 0
            return version

        if iteration == 0:
            results[kind + "_create"] = create(table_id, create_id)
            versions[kind] = write("a")
            with client.open_log(name, namespace="market", start="earliest") as reader:
                cursors[kind] = next(reader).cursor
            continue
        if iteration == 1:
            assert client.read_table(name, namespace="market").collect().to_pydict() == {
                "value": ["a"]
            }
            request = wire.DropTableRequest(
                protocol_version=1,
                request_id=drop_id.bytes,
                table_id=table_id.bytes,
                expected_snapshot_version=versions[kind],
            )
            # Prove the server checks the authenticated role even if a client bypasses its API.
            connection = client.flight._connection("read")
            try:
                list(
                    connection.client.do_action(
                        flight.Action("zippy.drop_table.v1", request.SerializeToString()),
                        client.flight._call_options(connection),
                    )
                )
            except flight.FlightUnauthorizedError:
                pass
            else:
                raise AssertionError("read session deleted a table")
            try:
                client.drop_table(
                    table_id,
                    expected_snapshot_version=versions[kind] + 1,
                    request_id=uuid.UUID(int=900 + index),
                )
            except ServerError as error:
                assert error.code == "CONFLICT" and not error.retryable
            else:
                raise AssertionError("stale snapshot deleted a table")
            with client.open_log(name, namespace="market", start="latest") as reader:
                results[kind] = client.drop_table(
                    table_id, expected_snapshot_version=versions[kind], request_id=drop_id
                )
                try:
                    next(reader)
                except ServerError as error:
                    assert error.code == "NOT_FOUND"
                else:
                    raise AssertionError("deleted table log remained active")
        assert (
            client.drop_table(
                table_id, expected_snapshot_version=versions[kind], request_id=drop_id
            )
            == results[kind]
        )
        if iteration < 3:
            try:
                client.flight.get_table_schema(name, namespace="market")
            except ServerError as error:
                assert error.code == "NOT_FOUND"
            else:
                raise AssertionError("deleted table recovered as active")
        replacement_id = uuid.UUID(int=1000 + index)
        if iteration == 2:
            create(replacement_id, uuid.UUID(int=1100 + index))
            write("b")
        if iteration >= 2:
            assert create(table_id, create_id) == results[kind + "_create"]
            assert (
                client.drop_table(
                    table_id, expected_snapshot_version=versions[kind], request_id=drop_id
                )
                == results[kind]
            )
            profile = client.read_table(name, namespace="market").profile()
            result, metrics = profile["result"], profile["metrics"]
            assert isinstance(result, pa.Table) and isinstance(metrics, dict)
            assert result.to_pydict() == {"value": ["b"]}
            assert metrics["query"]["snapshots"][0]["table_id"] == str(replacement_id)
            try:
                with client.open_log(cursor=cursors[kind]):
                    raise AssertionError("old cursor attached to a same-name replacement")
            except ServerError as error:
                assert error.code == "NOT_FOUND"
            with client.open_log(name, namespace="market", start="earliest") as reader:
                event = next(reader)
                assert event.cursor.table_id == replacement_id
                if kind != "key_value":
                    assert event.table.to_pydict() == {"value": ["b"]}


def verify_public_schema(
    client: Client,
    iteration: int,
    namespace_id: uuid.UUID,
    results: dict[str, dict[str, object]],
    cursors: dict[str, LogCursor],
) -> None:
    """Exercise public schema changes, mixed history, two schema versions and old handles.

    :param client: Authenticated client for this server incarnation.
    :param iteration: Restart index from zero through three.
    :param namespace_id: Existing namespace UUID.
    :param results: Original committed management responses retained across restarts.
    :param cursors: Old-schema cursors that must not silently adopt a new schema.
    :raises AssertionError: If a public schema, permission, replay or recovery contract fails.
    """
    import pyarrow.flight as flight
    from zippy_client._generated import protocol_pb2 as wire
    from zippy_client.errors import ContinuityError, ServerError

    old = pa.schema([("key", pa.string()), ("value", pa.int64())])
    new = old.append(pa.field("note", pa.string()))
    described = new.with_metadata({"description": "native schema v3"})
    for index, kind in enumerate(("append", "frame", "key_value")):
        name = f"schema_{kind}"
        table_id, create_id, change_id, describe_id, drop_id = (
            uuid.UUID(int=10000 + index * 100 + n) for n in range(1, 6)
        )
        keys = ["key"] if kind == "key_value" else None

        def change(schema: pa.Schema, expected: int, request: uuid.UUID) -> dict[str, object]:
            """Submit the same explicit logical schema contract on every retry."""
            deadline = time.monotonic() + 5
            while True:
                try:
                    return client.alter_table_schema(
                        table_id,
                        schema,
                        expected_schema_version=expected,
                        request_id=request,
                        keys=keys,
                    )
                except ServerError as error:
                    # Background checkpoints can own the table transition gate. Retry only
                    # advertised admission backpressure, with the exact same request identity.
                    if (
                        error.code != "RESOURCE_EXHAUSTED"
                        or not error.retryable
                        or time.monotonic() >= deadline
                    ):
                        raise
                    time.sleep(0.01)

        def write(key: str, value: int, schema: pa.Schema, durability: str) -> None:
            """Write one version-bound batch using the selected native mutation."""
            row: dict[str, object] = {"key": key, "value": value}
            if "note" in schema.names:
                row["note"] = f"note-{key}"
            batch = pa.Table.from_pylist([row], schema=schema)
            if kind == "key_value":
                client.upsert_key_value(name, batch, namespace="market", durability=durability)
            elif kind == "frame":
                client.write_frame(name, batch, namespace="market", durability=durability)
            else:
                client.flight.write_table(name, batch, namespace="market", durability=durability)

        if iteration == 0:
            if kind == "key_value":
                client.create_key_value_table(
                    name,
                    old,
                    keys=["key"],
                    namespace_id=namespace_id,
                    table_id=table_id,
                    request_id=create_id,
                )
            elif kind == "frame":
                client.create_frame_table(
                    name,
                    old,
                    by="key",
                    namespace_id=namespace_id,
                    table_id=table_id,
                    request_id=create_id,
                )
            else:
                client.create_append_table(
                    name, old, namespace_id=namespace_id, table_id=table_id, request_id=create_id
                )
            for schema, expected_version, error_code in (
                (old, 1, "INVALID_ARGUMENT"),
                (new, 2, "CONFLICT"),
                (old.append(pa.field("flag", pa.bool_())), 1, "SCHEMA_MISMATCH"),
            ):
                try:
                    change(schema, expected_version, uuid.UUID(int=11000 + index))
                except ServerError as error:
                    assert error.code == error_code and not error.retryable
                else:
                    raise AssertionError("invalid schema change committed")
            output = pa.BufferOutputStream()
            with pa.ipc.new_stream(output, new):
                pass
            unauthorized = wire.AlterTableSchemaRequest(
                protocol_version=1,
                request_id=change_id.bytes,
                table_id=table_id.bytes,
                expected_schema_version=1,
                schema_ipc=output.getvalue().to_pybytes(),
            )
            if keys is not None:
                unauthorized.key_value.CopyFrom(wire.KeyValueTableOptions(keys=keys))
            connection = client.flight._connection("read")
            try:
                list(
                    connection.client.do_action(
                        flight.Action(
                            "zippy.alter_table_schema.v1", unauthorized.SerializeToString()
                        ),
                        client.flight._call_options(connection),
                    )
                )
            except flight.FlightUnauthorizedError:
                pass
            else:
                raise AssertionError("read session altered a table schema")
            if kind == "key_value":
                try:
                    client.alter_table_schema(
                        table_id,
                        new,
                        expected_schema_version=1,
                        request_id=change_id,
                        keys=["note"],
                    )
                except ServerError as error:
                    assert error.code == "SCHEMA_MISMATCH"
                else:
                    raise AssertionError("schema change replaced primary keys")
            writer = None
            if kind == "append":
                writer = client.get_writer(name, namespace="market", schema=old, durability="wal")
                assert isinstance(writer, FlightWriter)
                writer.write_batch(pa.Table.from_pylist([{"key": "a", "value": 10}], schema=old))
            else:
                write("a", 10, old, "wal")
            with client.open_log(name, namespace="market", start="earliest") as reader:
                cursors[kind] = next(reader).cursor
            with client.open_log(name, namespace="market", start="latest") as reader:
                results[kind + "_schema2"] = change(new, 1, change_id)
                try:
                    next(reader)
                except ServerError as error:
                    assert error.code == "SCHEMA_MISMATCH", error
                else:
                    raise AssertionError("old-schema subscription remained active")
            if writer is not None:
                try:
                    writer.write_batch(
                        pa.Table.from_pylist([{"key": "x", "value": 99}], schema=old)
                    )
                except ServerError as error:
                    assert error.code == "SCHEMA_MISMATCH", error
                else:
                    raise AssertionError("old writer accepted a schema-obsolete batch")
                finally:
                    writer.close()
            write("b", 20, new, "wal")

        assert change(new, 1, change_id) == results[kind + "_schema2"]
        if iteration == 1:
            with client.open_log(name, namespace="market", start="latest") as reader:
                results[kind + "_schema3"] = change(described, 2, describe_id)
                try:
                    next(reader)
                except ServerError as error:
                    assert error.code == "SCHEMA_MISMATCH", error
                else:
                    raise AssertionError("metadata-only schema change left old subscription active")
            write("c", 30, described, "wal")
        if iteration >= 1:
            assert change(described, 2, describe_id) == results[kind + "_schema3"]
        if iteration == 2:
            write("d", 40, described, "persisted")
        expected_keys = (
            ["a", "b"]
            if iteration == 0
            else (["a", "b", "c"] if iteration == 1 else ["a", "b", "c", "d"])
        )
        expected = [
            {
                "key": key,
                "value": 10 * (ord(key) - ord("a") + 1),
                "note": None if key == "a" else f"note-{key}",
            }
            for key in expected_keys
        ]
        profile = client.read_table(name, namespace="market").profile()
        result, metrics = profile["result"], profile["metrics"]
        assert isinstance(result, pa.Table) and isinstance(metrics, dict)
        assert sorted(result.to_pylist(), key=lambda row: row["key"]) == expected, (
            kind,
            iteration,
            result.to_pylist(),
        )
        assert metrics["query"]["snapshots"][0]["schema_version"] == (2 if iteration == 0 else 3)
        with client.open_log(name, namespace="market", start="earliest", batch_rows=1) as reader:
            for offset, row in enumerate(expected):
                event = next(reader)
                assert event.first_offset == event.last_offset == offset, (
                    kind,
                    iteration,
                    offset,
                    event.first_offset,
                    event.last_offset,
                )
                assert event.table.select(["key", "value", "note"]).to_pylist() == [row]
        try:
            with client.open_log(cursor=cursors[kind]):
                raise AssertionError("old cursor silently adopted a different schema")
        except ContinuityError:
            pass
        if iteration == 3:
            snapshot_version = metrics["query"]["snapshots"][0]["snapshot_version"]
            assert isinstance(snapshot_version, int)
            client.drop_table(
                table_id, expected_snapshot_version=snapshot_version, request_id=drop_id
            )
            assert change(new, 1, change_id) == results[kind + "_schema2"]
            assert change(described, 2, describe_id) == results[kind + "_schema3"]


def verify_durable_log(client: Client, iteration: int, namespace_id: uuid.UUID) -> None:
    """Verify real Flight ACK loss, atomic delivery and restart recovery for all table kinds.

    :param client: Authenticated native client for this server generation.
    :param iteration: Restart generation, zero through three.
    :param namespace_id: Existing Catalog namespace for isolated consumer tables.
    :raises AssertionError: Delivery, confirmation, group exclusion or recovery diverges.
    """
    from types import SimpleNamespace

    import pyarrow.flight as flight
    from zippy_client import AmbiguousAcknowledgementError, FlightLogReader, ServerError
    from zippy_client._generated import protocol_pb2 as wire

    assert "subscription.durable_log.v1" in client.flight.capabilities.names
    schema = pa.schema(
        [pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int64())]
    )
    for index, kind in enumerate(("append", "frame", "key_value")):
        name = f"consumer_{kind}"
        consumer_id, group_id = uuid.UUID(int=2000 + index), uuid.UUID(int=2100 + index)

        def write(values: list[int]) -> None:
            """Append one complete native mutation with explicit memory durability."""
            table = pa.Table.from_pydict({"key": ["a", "b"], "value": values}, schema=schema)
            if kind == "frame":
                client.write_frame(name, table, namespace="market", durability="memory")
            elif kind == "key_value":
                client.upsert_key_value(name, table, namespace="market", durability="memory")
            else:
                client.flight.write_table(name, table, namespace="market", durability="memory")

        def reopen() -> FlightLogReader:
            """Wait only for the preceding cancelled transport to release its group membership."""
            deadline = time.monotonic() + 5
            while True:
                try:
                    return client.open_log(
                        name,
                        namespace="market",
                        start="committed",
                        consumer_id=consumer_id,
                        group_id=group_id,
                        batch_rows=2 if kind == "append" else 1,
                        deadline_ns=time.time_ns() + 10_000_000_000,
                        open_timeout_sec=3,
                    )
                except ServerError as error:
                    if error.code != "CONFLICT" or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)

        if iteration == 0:
            identity = uuid.UUID(int=2200 + index)
            request_id = uuid.UUID(int=2400 + index)
            if kind == "frame":
                client.create_frame_table(
                    name,
                    schema,
                    by="key",
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request_id,
                )
            elif kind == "key_value":
                client.create_key_value_table(
                    name,
                    schema,
                    keys=["key"],
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request_id,
                )
            else:
                client.create_append_table(
                    name,
                    schema,
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request_id,
                )
            for values in ([0, 1], [2, 3], [4, 5]):
                write(values)
            with reopen() as unconfirmed:
                first = next(unconfirmed)
                assert first.table.column("value").to_pylist() == [0, 1]
                unconfirmed._writer.write_metadata(
                    wire.LogControl(
                        protocol_version=1,
                        acknowledge=wire.LogAcknowledgement(
                            request_id=uuid.uuid4().bytes,
                            delivery_sequence=first.delivery_sequence,
                            processed_through=first.last_offset + 1,
                        ),
                    ).SerializeToString()
                )
                try:
                    unconfirmed._reader.read_chunk()
                except flight.FlightError as error:
                    from zippy_client.transports.flight import _translate_flight_error

                    rejected = _translate_flight_error(error, "test.invalid_ack")
                    assert isinstance(rejected, ServerError) and rejected.code == "INVALID_ARGUMENT"
                else:
                    raise AssertionError("server accepted a forged delivery ACK")
        with reopen() as events:
            event = next(events)
            expected = [2 * iteration, 2 * iteration + 1]
            assert event.table.column("value").to_pylist() == expected
            assert (event.first_offset, event.last_offset) == (expected[0], expected[1])
            try:
                next(events)
            except RuntimeError as error:
                assert "acknowledge" in str(error)
            else:
                raise AssertionError("durable stream advanced without ACK")
            try:
                with client.open_log(
                    name,
                    namespace="market",
                    start="committed",
                    consumer_id=uuid.UUID(int=2300 + index),
                    group_id=group_id,
                    open_timeout_sec=3,
                ):
                    raise AssertionError("second group member opened an active authority")
            except ServerError as error:
                assert error.code == "CONFLICT"
            if iteration == 0:
                original = events._reader

                def lose_receipt() -> None:
                    """Discard an actual committed server receipt to simulate response loss."""
                    chunk = original.read_chunk()
                    metadata = wire.LogMetadata.FromString(bytes(chunk.app_metadata))
                    assert metadata.WhichOneof("payload") == "acknowledged"
                    raise flight.FlightUnavailableError("test-only lost committed ACK response")

                events._reader = SimpleNamespace(read_chunk=lose_receipt, cancel=original.cancel)
                try:
                    events.acknowledge(event)
                except AmbiguousAcknowledgementError:
                    pass
                else:
                    raise AssertionError("lost ACK response was treated as a known result")
            else:
                events.acknowledge(event)
                events.acknowledge(event)
                if iteration == 2:
                    write([6, 7])
                if iteration < 3:
                    following = next(events)
                    assert following.first_offset == expected[1] + 1
                else:
                    write([8, 9])
                    pending = next(events)
                    client.alter_table_schema(
                        events.table_id,
                        schema.append(pa.field("note", pa.string())),
                        expected_schema_version=1,
                        request_id=uuid.UUID(int=2500 + index),
                        keys=["key"] if kind == "key_value" else None,
                    )
                    try:
                        events.acknowledge(pending)
                    except ServerError as error:
                        assert error.code == "SCHEMA_MISMATCH", error
                    else:
                        raise AssertionError("old-schema durable subscription accepted ACK")
        if iteration == 0:
            with reopen() as recovered:
                assert next(recovered).first_offset == 2
        if iteration == 3:
            with reopen() as recovered:
                final = next(recovered)
                assert final.table.column("value").to_pylist() == [8, 9]
                assert final.table.column("note").to_pylist() == [None, None]
                recovered.acknowledge(final)
                duplicate = wire.LogAcknowledgement(
                    request_id=uuid.uuid4().bytes,
                    delivery_sequence=final.delivery_sequence,
                    processed_through=final.last_offset,
                )
                recovered._writer.write_metadata(
                    wire.LogControl(
                        protocol_version=1,
                        acknowledge=duplicate,
                    ).SerializeToString()
                )
                receipt = wire.LogMetadata.FromString(
                    bytes(recovered._reader.read_chunk().app_metadata)
                )
                assert receipt.acknowledged == duplicate


def verify_native_callbacks(client: Client, iteration: int, namespace_id: uuid.UUID) -> None:
    """Verify callbacks, ACK recovery and real transport cancellation for all native kinds.

    :param client: Authenticated native client.
    :param iteration: Server restart generation.
    :param namespace_id: Catalog namespace for isolated callback tables.
    :raises AssertionError: Callback boundaries, recovery or shutdown diverge.
    """
    from zippy_client import FlightSubscriber, Row, SubscriptionOptions

    schema = pa.schema(
        [pa.field("instrument_id", pa.string(), nullable=False), pa.field("value", pa.int64())]
    )

    def wait_processed(subscriber: FlightSubscriber, offset: int) -> None:
        deadline = time.monotonic() + 10
        while subscriber.cursor is None or subscriber.cursor.next_offset != offset:
            subscriber.join(0)
            assert time.monotonic() < deadline, subscriber.metrics()
            time.sleep(0.002)

    for index, kind in enumerate(("append", "frame", "key_value")):
        name = f"callback_{kind}"
        if iteration == 0:
            identity, request = uuid.UUID(int=3000 + index), uuid.UUID(int=3100 + index)
            if kind == "frame":
                client.create_frame_table(
                    name,
                    schema,
                    by="instrument_id",
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request,
                )
            elif kind == "key_value":
                client.create_key_value_table(
                    name,
                    schema,
                    keys=["instrument_id"],
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=request,
                )
            else:
                client.create_append_table(
                    name, schema, namespace_id=namespace_id, table_id=identity, request_id=request
                )

        def write(first: int) -> None:
            table = pa.Table.from_pydict(
                {"instrument_id": ["a", "b"], "value": [first, first + 1]}, schema=schema
            )
            if kind == "frame":
                client.write_frame(name, table, namespace="market", durability="memory")
            elif kind == "key_value":
                client.upsert_key_value(name, table, namespace="market", durability="memory")
            else:
                client.flight.write_table(name, table, namespace="market", durability="memory")

        first = iteration * 4
        write(first)
        tables: list[pa.Table] = []
        subscriber = client.subscribe_table(
            name,
            tables.append,
            options=SubscriptionOptions(
                namespace="market",
                start="committed",
                consumer_id=uuid.UUID(int=3200 + index),
                group_id=uuid.UUID(int=3300 + index),
                batch_rows=1,
                reconnect_interval_ms=10,
                deadline_ns=time.time_ns() + 30_000_000_000,
            ),
        )
        row_subscriber = None
        rows: list[Row] = []
        try:
            wait_processed(subscriber, first + 2)
            old_reader = subscriber._reader
            assert old_reader is not None
            # Cancel the actual gRPC receive stream, without marking the callback handle stopped.
            old_reader._reader.cancel()
            deadline = time.monotonic() + 10
            while subscriber._reader is None or subscriber._reader is old_reader:
                subscriber.join(0)
                assert time.monotonic() < deadline, subscriber.metrics()
                time.sleep(0.002)
            reconnects = subscriber.metrics()["reconnects_total"]
            assert isinstance(reconnects, int) and reconnects >= 1
            if kind == "append":
                row_subscriber = client.subscribe(
                    name,
                    rows.append,
                    options=SubscriptionOptions(
                        namespace="market",
                        instrument_ids=("b",),
                        batch_rows=1,
                        deadline_ns=time.time_ns() + 10_000_000_000,
                    ),
                )
            write(first + 2)
            wait_processed(subscriber, first + 4)
            assert [
                value for table in tables for value in table.column("value").to_pylist()
            ] == list(range(first, first + 4))
            if kind != "append":
                assert [table.num_rows for table in tables] == [2, 2]
            if row_subscriber is not None:
                wait_processed(row_subscriber, first + 4)
                assert len(rows) == 1 and dict(rows[0]) == {
                    "instrument_id": "b",
                    "value": first + 3,
                }
        finally:
            if row_subscriber is not None:
                row_subscriber.stop()
            subscriber.stop()
        assert subscriber.status() == "stopped"


def verify_native_compute(client: Client, iteration: int, namespace_id: uuid.UUID) -> None:
    """Verify Rust compute over native input, durable outputs and replacement fencing.

    :param client: Authenticated client of this real server generation.
    :param iteration: Restart generation; kernel state is explicitly reconstructed each run.
    :param namespace_id: Namespace containing input and three output table kinds.
    :raises AssertionError: Computation, output recovery or immutable identity fencing differs.
    """
    from contextlib import ExitStack

    from zippy import ComputePipeline, NativeProcessor, TsMeanSpec
    from zippy_client import SubscriptionOptions
    from zippy_client.errors import AmbiguousWriteError, ServerError

    schema = pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("price", pa.float64()),
        ]
    )
    source_id = uuid.UUID(int=4200)
    if iteration == 0:
        client.create_append_table(
            "compute_input",
            schema,
            namespace_id=namespace_id,
            table_id=source_id,
            request_id=uuid.UUID(int=4201),
        )

    def kernel() -> NativeProcessor:
        return NativeProcessor.reactive(
            "native_mean",
            schema,
            "symbol",
            [TsMeanSpec(id_column="symbol", value_column="price", window=2, output="mean")],
        )

    def expected(generation: int, kind: str) -> list[dict[str, object]]:
        a = float(generation * 10 + 1)
        b = float(generation * 10 + 8)
        rows: list[dict[str, object]] = [
            {"symbol": "A", "price": a, "mean": None},
            {"symbol": "B", "price": b, "mean": None},
            {"symbol": "A", "price": a + 2, "mean": a + 1},
        ]
        if kind == "key_value":
            return rows[1:]
        return rows

    def check(generation: int, kind: str) -> None:
        actual = client.read_table(f"compute_{kind}", namespace="market").collect().to_pylist()
        wanted = (
            sum((expected(i, kind) for i in range(generation + 1)), [])
            if kind != "key_value"
            else expected(generation, kind)
        )
        assert sorted(actual, key=lambda row: (row["symbol"], row["price"])) == sorted(
            wanted, key=lambda row: (str(row["symbol"]), float(str(row["price"])))
        ), (kind, generation, actual, wanted)
        if kind == "frame":
            with client.open_log(
                "compute_frame",
                namespace="market",
                start="earliest",
                batch_rows=1,
                deadline_ns=time.time_ns() + 10_000_000_000,
            ) as frames:
                for completed in range(generation + 1):
                    first, last = next(frames), next(frames)
                    assert first.table.num_rows == 2 and last.table.num_rows == 1
                    assert last.table.to_pylist() == expected(completed, kind)[-1:]
                assert last.cursor.next_offset == (generation + 1) * 3

    kinds: tuple[Literal["append", "frame", "key_value"], ...] = ("append", "frame", "key_value")
    jobs: list[ComputePipeline] = []
    with ExitStack() as stack:
        for index, kind in enumerate(kinds):
            name, identity = f"compute_{kind}", uuid.UUID(int=4210 + index)
            processor = kernel()
            if iteration == 0:
                kwargs = dict(
                    namespace_id=namespace_id,
                    table_id=identity,
                    request_id=uuid.UUID(int=4220 + index),
                )
                if kind == "frame":
                    client.create_frame_table(
                        name, processor.output_schema(), by="symbol", **kwargs
                    )
                elif kind == "key_value":
                    client.create_key_value_table(
                        name, processor.output_schema(), keys=["symbol"], **kwargs
                    )
                else:
                    client.create_append_table(
                        name,
                        processor.output_schema(),
                        namespace_id=kwargs["namespace_id"],
                        table_id=kwargs["table_id"],
                        request_id=kwargs["request_id"],
                    )
            else:
                check(iteration - 1, kind)
            jobs.append(
                stack.enter_context(
                    ComputePipeline(
                        client,
                        processor,
                        output=name,
                        output_table_id=identity,
                        output_namespace="market",
                        output_kind=kind,
                        durability="persisted",
                        source="compute_input",
                        input_options=SubscriptionOptions(namespace="market"),
                    )
                )
            )
        for symbols, values in [
            (["A", "B"], [iteration * 10 + 1.0, iteration * 10 + 8.0]),
            (["A"], [iteration * 10 + 3.0]),
        ]:
            client.flight.write_table(
                "compute_input",
                pa.Table.from_pydict({"symbol": symbols, "price": values}, schema=schema),
                namespace="market",
                durability="wal",
                expected_table_id=source_id,
            )
        for job in jobs:
            deadline = time.monotonic() + 10
            while job.cursor is None or job.cursor.next_offset != (iteration + 1) * 3:
                job.join(0)
                assert time.monotonic() < deadline, job.metrics()
                time.sleep(0.002)
            assert job.metrics()["input_rows"] == job.metrics()["output_rows"] == 3
        for kind in ("append", "frame", "key_value"):
            check(iteration, kind)

    if iteration != 3:
        return
    for index, (kind, job) in enumerate(zip(kinds, jobs)):
        name, identity = f"compute_{kind}", uuid.UUID(int=4210 + index)
        receipts = job.metrics()["last_receipts"]
        assert isinstance(receipts, list)
        version = receipts[-1]["persisted_snapshot_version"]
        client.drop_table(
            identity, expected_snapshot_version=version, request_id=uuid.UUID(int=4230 + index)
        )
        processor = kernel()
        kwargs = dict(
            namespace_id=namespace_id,
            table_id=uuid.UUID(int=4240 + index),
            request_id=uuid.UUID(int=4250 + index),
        )
        if kind == "frame":
            client.create_frame_table(name, processor.output_schema(), by="symbol", **kwargs)
        elif kind == "key_value":
            client.create_key_value_table(
                name, processor.output_schema(), keys=["symbol"], **kwargs
            )
        else:
            client.create_append_table(
                name,
                processor.output_schema(),
                namespace_id=kwargs["namespace_id"],
                table_id=kwargs["table_id"],
                request_id=kwargs["request_id"],
            )
        stale = ComputePipeline(
            client,
            processor,
            output=name,
            output_table_id=identity,
            output_namespace="market",
            output_kind=kind,
            durability="persisted",
        )
        try:
            stale.write(pa.Table.from_pydict({"symbol": ["A"], "price": [999.0]}, schema=schema))
        except (ServerError, AmbiguousWriteError) as error:
            if isinstance(error, ServerError):
                assert error.code == "CONFLICT" and not error.retryable
            else:
                cause: BaseException | None = error
                while cause is not None and not isinstance(cause, ServerError):
                    cause = cause.__cause__
                assert isinstance(cause, ServerError), error
                assert cause.code == "CONFLICT" and "identity" in str(cause), cause
        else:
            raise AssertionError("stale compute task wrote to a replacement table")
        finally:
            try:
                stale.stop()
            except (ServerError, AmbiguousWriteError):
                pass  # The same terminal failure is already asserted above.
        assert stale.status() == "failed"
        assert client.read_table(name, namespace="market").collect().num_rows == 0


def main() -> None:
    """Run recovery acceptance against the repository binary or installed server CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--installed-server", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not args.installed_server and not (root / "target/debug/zippy-server").is_file():
        raise SystemExit(
            "build the native server first: cargo build -p zippy-server --bin zippy-server"
        )
    with tempfile.TemporaryDirectory(prefix="zippy-native-admin-") as temporary:
        directory = Path(temporary)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            endpoint = f"127.0.0.1:{probe.getsockname()[1]}"
        credentials = {
            role: f"test-only-native-admin-{role}-000000001" for role in ("read", "writer", "admin")
        }
        for role, token in credentials.items():
            token_file = directory / f"{role}.token"
            token_file.write_text(token)
            token_file.chmod(0o600)
        config = directory / "server.toml"
        config.write_text(f"""
    [server]
    data_dir = "{directory / 'database'}"

    [master]
    shutdown_timeout_ms = 10000
    [query]
    operator_memory_bytes = 268435456
    [query.resources]
    max_memory_bytes = 4294967296
    [flight]
    endpoint = "{endpoint}"
    max_message_bytes = 2097152
    max_pending_tickets = 16
    [flight.auth]
    enabled = true
    token_file = "{directory / 'read.token'}"
    writer_token_file = "{directory / 'writer.token'}"
    admin_token_file = "{directory / 'admin.token'}"
    session_ttl_ms = 60000
    max_sessions = 32
    [flight.ingest]
    enabled = true
    """)
        namespace_id, namespace_request, table_id, table_request = (
            uuid.UUID(int=x) for x in range(1, 5)
        )
        fixed_snapshot_ids: list[str] = []
        drop_versions: dict[str, int] = {}
        drop_results: dict[str, dict[str, object]] = {}
        drop_cursors: dict[str, LogCursor] = {}
        schema_results: dict[str, dict[str, object]] = {}
        schema_cursors: dict[str, LogCursor] = {}
        for iteration in range(4):
            with (directory / f"server-{iteration}.log").open("w") as log:

                def launch() -> subprocess.Popen[bytes]:
                    """Start the exact built server against this temporary native directory."""
                    return subprocess.Popen(
                        _server_command(root, directory, config, args.installed_server),
                        env={**os.environ, "PYO3_PYTHON": sys.executable},
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )

                process = launch()
                try:
                    deadline = time.monotonic() + 15
                    while True:
                        if process.poll() is not None:
                            raise RuntimeError((directory / f"server-{iteration}.log").read_text())
                        try:
                            client = Client(
                                str(directory / "control.sock"),
                                flight_token=credentials["read"],
                                flight_writer_token=credentials["writer"],
                                flight_admin_token=credentials["admin"],
                            )
                            break
                        except Exception:
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.05)
                    with client:
                        client.create_namespace(
                            "market", namespace_id=namespace_id, request_id=namespace_request
                        )
                        if iteration == 0:
                            verify_replay_examples(root, directory, namespace_id, credentials)
                        verify_public_drop(
                            client,
                            iteration,
                            namespace_id,
                            drop_versions,
                            drop_results,
                            drop_cursors,
                        )
                        verify_durable_log(client, iteration, namespace_id)
                        verify_native_callbacks(client, iteration, namespace_id)
                        verify_native_compute(client, iteration, namespace_id)
                        verify_compute_plugin(client, namespace_id, directory, iteration)
                        if iteration == 0:

                            def restart_compute_service() -> None:
                                """Restart storage while keeping the exact local kernel owners alive."""
                                nonlocal process
                                process.send_signal(signal.SIGINT)
                                process.wait(timeout=20)
                                assert process.returncode == 0
                                process = launch()
                                restart_deadline = time.monotonic() + 15
                                while True:
                                    try:
                                        client.flight.refresh_read_session(timeout_sec=1)
                                        client.flight._admin_connection = None
                                        return
                                    except Exception:
                                        if (
                                            process.poll() is not None
                                            or time.monotonic() >= restart_deadline
                                        ):
                                            raise
                                        time.sleep(0.05)

                            verify_compute_recovery(client, namespace_id, restart_compute_service)
                            verify_compute_kv_input(client, namespace_id, restart_compute_service)

                        verify_table_cli(client, directory, namespace_id, credentials, iteration)
                        verify_native_webui(client, directory, namespace_id, credentials, iteration)
                        verify_flight_examples(
                            client, directory, namespace_id, credentials, iteration
                        )
                        verify_public_examples(
                            client, directory, namespace_id, credentials, iteration
                        )
                        verify_shared_sql(client, iteration, namespace_id)
                        verify_fixed_snapshots(client, namespace_id, iteration, fixed_snapshot_ids)
                        verify_public_schema(
                            client,
                            iteration,
                            namespace_id,
                            schema_results,
                            schema_cursors,
                        )
                        created = client.create_append_table(
                            "ticks",
                            pa.schema([("value", pa.int64())]),
                            namespace_id=namespace_id,
                            table_id=table_id,
                            request_id=table_request,
                        )
                        assert created["object_id"] == str(table_id)
                        client.create_frame_table(
                            "frames",
                            pa.schema([("value", pa.int64())]),
                            by="value",
                            namespace_id=namespace_id,
                            table_id=uuid.UUID(int=5),
                            request_id=uuid.UUID(int=6),
                        )
                        kv_schema = pa.schema([("symbol", pa.string()), ("price", pa.int64())])
                        client.create_key_value_table(
                            "quotes",
                            kv_schema,
                            keys=["symbol"],
                            namespace_id=namespace_id,
                            table_id=uuid.UUID(int=7),
                            request_id=uuid.UUID(int=8),
                        )
                        assert (
                            client.flight.get_table_schema("quotes", namespace="market")
                            == kv_schema
                        )
                        if iteration == 0:
                            empty_state = client.snapshot_state("quotes", namespace="market")
                            assert empty_state.num_rows == 0
                            assert empty_state.schema.metadata[b"zippy_kv_seq"] == b"0"
                            client.upsert_key_value(
                                "quotes",
                                pa.table({"symbol": ["a", "b", "a"], "price": [1, 2, 3]}),
                                namespace="market",
                                durability="wal",
                            )
                            current = client.snapshot_state(
                                "quotes", namespace="market", result_batch_rows=1
                            )
                            assert current.schema.metadata[b"zippy_kv_seq"] == b"3"
                            assert current.schema.metadata[b"zippy_kv_version"] == b"1"
                            assert current.sort_by("symbol").to_pydict() == {
                                "symbol": ["a", "b"],
                                "price": [3, 2],
                            }
                            client.delete_key_value(
                                "quotes",
                                pa.table({"symbol": ["b", "missing"]}),
                                namespace="market",
                                durability="persisted",
                            )
                            client.upsert_key_value(
                                "quotes",
                                pa.table({"symbol": ["c", "a"], "price": [4, 5]}),
                                namespace="market",
                                durability="persisted",
                            )
                            for durability, value in (("wal", 42), ("persisted", 43)):
                                with client.get_writer(
                                    "ticks",
                                    namespace="market",
                                    batch_size=1,
                                    durability=durability,
                                ) as writer:
                                    assert isinstance(writer, FlightWriter)
                                    writer.write({"value": value})
                                    receipt = writer.last_receipts[0]
                                    assert receipt["durability"] == durability
                                    if durability == "persisted":
                                        assert int(str(receipt["persisted_snapshot_version"])) > 0
                            for values in ([10, None], [12]):
                                receipts = client.write_frame(
                                    "frames",
                                    pa.table({"value": values}),
                                    namespace="market",
                                    durability="persisted",
                                )
                                assert len(receipts) == 1
                                assert receipts[0]["row_count"] == len(values)
                        if iteration == 1:
                            restored = client.read_table("quotes", namespace="market").collect()
                            assert restored.sort_by("symbol").to_pydict() == {
                                "symbol": ["a", "c"],
                                "price": [5, 4],
                            }
                            client.upsert_key_value(
                                "quotes",
                                pa.table({"symbol": ["c"], "price": [6]}),
                                namespace="market",
                                durability="wal",
                            )
                            client.delete_key_value(
                                "quotes",
                                pa.table({"symbol": ["a"]}),
                                namespace="market",
                                durability="persisted",
                            )
                        quotes = client.read_table("quotes", namespace="market").collect()
                        expected_quotes = (
                            {"symbol": ["a", "c"], "price": [5, 4]}
                            if iteration == 0
                            else (
                                {"symbol": ["c"], "price": [6]}
                                if iteration < 3
                                else {"symbol": [], "price": []}
                            )
                        )
                        assert quotes.sort_by("symbol").to_pydict() == expected_quotes, quotes
                        state = client.snapshot_state(
                            "quotes", namespace="market", result_batch_rows=1
                        )
                        assert state.sort_by("symbol").to_pydict() == expected_quotes
                        assert state.schema.metadata[b"zippy_kv_seq"] == (
                            b"7" if iteration == 0 else (b"9" if iteration < 3 else b"10")
                        )
                        assert state.schema.metadata[b"zippy_kv_version"] == (
                            b"3" if iteration == 0 else (b"5" if iteration < 3 else b"6")
                        )
                        assert (
                            state.schema.metadata[b"zippy_kv_stream_id"]
                            == uuid.UUID(int=7).hex.encode()
                        )
                        if iteration == 2:
                            client.delete_key_value(
                                "quotes",
                                pa.table({"symbol": ["c"]}),
                                namespace="market",
                                durability="persisted",
                            )
                            deleted = client.snapshot_state("quotes", namespace="market")
                            assert deleted.num_rows == 0
                            assert deleted.schema.metadata[b"zippy_kv_seq"] == b"10"
                            assert deleted.schema.metadata[b"zippy_kv_version"] == b"6"
                        frames = client.read_table("frames", namespace="market").collect()
                        assert frames.to_pydict() == {"value": [10, None, 12]}, frames
                        result = client.read_table("ticks", namespace="market").collect()
                        assert result.to_pydict() == {"value": [42, 43]}, result
                        stream_deadline = time.time_ns() + 10_000_000_000
                        with client.open_log(
                            "ticks",
                            namespace="market",
                            start="earliest",
                            batch_rows=1,
                            deadline_ns=stream_deadline,
                        ) as events:
                            first = next(events)
                            assert first.table.to_pydict() == {"value": [42]}
                            resume = first.cursor
                        with client.open_log(cursor=resume, deadline_ns=stream_deadline) as events:
                            assert next(events).table.to_pydict() == {"value": [43]}
                            assert events.cursor.next_offset == 2
                        with client.open_log(
                            "frames",
                            namespace="market",
                            start="earliest",
                            batch_rows=1,
                            deadline_ns=stream_deadline,
                        ) as events:
                            frame = next(events)
                            assert frame.table.to_pydict() == {"value": [10, None]}
                            assert frame.table.column("value").num_chunks == 2
                            assert frame.cursor.next_offset == 2
                            second = next(events)
                            assert second.table.to_pydict() == {"value": [12]}
                            assert second.cursor.next_offset == 3
                        state = client.snapshot_state("quotes", namespace="market")
                        sequence = int(state.schema.metadata[b"zippy_kv_seq"])
                        version = int(state.schema.metadata[b"zippy_kv_version"])
                        with client.open_log(
                            "quotes",
                            namespace="market",
                            start="earliest",
                            batch_rows=1,
                            deadline_ns=stream_deadline,
                        ) as events:
                            while events.cursor.next_offset < sequence:
                                mutation = next(events)
                                assert mutation.table.column(
                                    "_zippy_kv_snapshot_version"
                                ).unique().to_pylist() == [mutation.cursor.version]
                            assert events.cursor.next_offset == sequence
                            assert events.cursor.version == version
                        with client.open_log(
                            "ticks", namespace="market", deadline_ns=stream_deadline
                        ) as latest:
                            assert latest.cursor.next_offset == 2
                        if iteration == 0:
                            from zippy import StateSubscription

                            client.create_key_value_table(
                                "state_quotes",
                                kv_schema,
                                keys=["symbol"],
                                namespace_id=namespace_id,
                                table_id=uuid.UUID(int=90),
                                request_id=uuid.UUID(int=91),
                            )
                            client.upsert_key_value(
                                "state_quotes",
                                pa.table({"symbol": ["a", "b"], "price": [1, 2]}),
                                namespace="market",
                                durability="persisted",
                            )
                            with StateSubscription(
                                "state_quotes",
                                client=client,
                                namespace="market",
                                timeout=5,
                                reconnect_interval_sec=0.05,
                                max_reconnect_attempts=30,
                            ) as live:
                                assert live.sequence == 2
                                client.upsert_key_value(
                                    "state_quotes",
                                    pa.table({"symbol": ["a"], "price": [3]}),
                                    namespace="market",
                                    durability="wal",
                                )
                                assert live.wait_for_sequence(3)
                                client.delete_key_value(
                                    "state_quotes",
                                    pa.table({"symbol": ["b"]}),
                                    namespace="market",
                                    durability="persisted",
                                )
                                assert live.wait_for_sequence(4)
                                assert live.snapshot().to_pydict() == {
                                    "symbol": ["a"],
                                    "price": [3],
                                }
                                from zippy_client import SubscriptionOptions

                                callback_tables: list[pa.Table] = []
                                callback_live = client.subscribe_table(
                                    "state_quotes",
                                    callback_tables.append,
                                    options=SubscriptionOptions(
                                        namespace="market",
                                        start="committed",
                                        consumer_id=uuid.UUID(int=3500),
                                        batch_rows=1,
                                        reconnect_interval_ms=50,
                                    ),
                                )
                                try:
                                    deadline = time.monotonic() + 10
                                    while (
                                        callback_live.cursor is None
                                        or callback_live.cursor.next_offset != 4
                                    ):
                                        callback_live.join(0)
                                        assert time.monotonic() < deadline, callback_live.metrics()
                                        time.sleep(0.002)
                                    process.send_signal(signal.SIGINT)
                                    assert process.wait(timeout=20) == 0
                                    deadline = time.monotonic() + 10
                                    while live.metrics()["state"] != "recovering":
                                        assert live.metrics()["state"] != "failed", live.metrics()
                                        assert time.monotonic() < deadline, live.metrics()
                                        time.sleep(0.01)
                                    try:
                                        live.snapshot()
                                    except RuntimeError:
                                        pass
                                    else:
                                        raise AssertionError(
                                            "recovering state was exposed as current"
                                        )
                                    process = launch()
                                    deadline = time.monotonic() + 15
                                    while live.metrics()["bootstrap_generation"] < 2:
                                        assert process.poll() is None
                                        assert live.metrics()["state"] != "failed", live.metrics()
                                        assert time.monotonic() < deadline, live.metrics()
                                        time.sleep(0.01)
                                    assert live.snapshot().to_pydict() == {
                                        "symbol": ["a"],
                                        "price": [3],
                                    }
                                    with Client(
                                        str(directory / "control.sock"),
                                        flight_token=credentials["read"],
                                        flight_writer_token=credentials["writer"],
                                    ) as recovered_client:
                                        recovered_client.delete_key_value(
                                            "state_quotes",
                                            pa.table({"symbol": ["a"]}),
                                            namespace="market",
                                            durability="wal",
                                        )
                                        assert live.wait_for_sequence(5)
                                        assert live.snapshot().num_rows == 0
                                        recovered_client.upsert_key_value(
                                            "state_quotes",
                                            pa.table({"symbol": ["c"], "price": [6]}),
                                            namespace="market",
                                            durability="persisted",
                                        )
                                        assert live.wait_for_sequence(6)
                                        assert live.select([["c"]]).column("price").to_pylist() == [
                                            6
                                        ]
                                    assert live.metrics()["reconnects_total"] >= 1
                                    deadline = time.monotonic() + 10
                                    while (
                                        callback_live.cursor is None
                                        or callback_live.cursor.next_offset != 6
                                    ):
                                        callback_live.join(0)
                                        assert time.monotonic() < deadline, callback_live.metrics()
                                        time.sleep(0.002)
                                    assert [table.num_rows for table in callback_tables] == [
                                        2,
                                        1,
                                        1,
                                        1,
                                        1,
                                    ]
                                    reconnects = callback_live.metrics()["reconnects_total"]
                                    assert isinstance(reconnects, int) and reconnects >= 1
                                finally:
                                    callback_live.stop()

                finally:
                    if process.poll() is None:
                        process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                        raise
                    if process.returncode != 0:
                        raise RuntimeError((directory / f"server-{iteration}.log").read_text())
        verify_local_read_mode(root, directory, config, installed=args.installed_server)
        catalog_path = directory / "database" / "catalog" / "catalog.db"
        catalog = sqlite3.connect(f"{catalog_path.as_uri()}?mode=ro", uri=True)
        try:
            assert catalog.execute("PRAGMA user_version").fetchone() == (11,)
            assert catalog.execute(
                "SELECT count(*) FROM sqlite_schema WHERE name LIKE 'legacy_%'"
            ).fetchone() == (0,)
            assert catalog.execute(
                "SELECT DISTINCT location_kind, provenance_kind FROM data_files"
            ).fetchall() == [("managed", "native")]
            columns = {row[1] for row in catalog.execute("PRAGMA table_info(data_files)")}
            assert not any(name.startswith("legacy_") for name in columns)
            assert catalog.execute(
                "SELECT count(*) FROM table_snapshots WHERE operation = 'import' "
                "OR hex(substr(committed_log_offset, 1, 1)) = '02'"
            ).fetchone() == (0,)
            assert catalog.execute(
                "SELECT DISTINCT format_version FROM catalog_request_ids"
            ).fetchall() == [(1,)]
        finally:
            catalog.close()
        assert not (directory / "database/catalog/legacy-master-registry.json").exists()
        assert not (directory / "persist").exists()
        print(
            json.dumps(
                {
                    "catalog_v11_native_files_watermarks_and_request_versions": True,
                    "shared_sql_join_keys_column_order_range_and_time_after_restart": True,
                    "native_table_cli_three_kinds_metadata_drop_and_restart": True,
                    "native_webui_http_namespaces_three_kinds_preview_and_restart": True,
                    "sdk_only_flight_examples_write_query_callbacks_and_restart": True,
                    "flight_short_session_snapshot_cleanup_capacity_reuse": True,
                    "public_examples_three_kinds_compute_and_restart_without_legacy_api": True,
                    "main_package_explicit_client_sql_aggregate_after_restart": True,
                    "flight_only_buffered_writer_lazy_plan_and_restart": True,
                    "independent_client_native_child_without_main_import": True,
                    "server_launch": (
                        "installed_cli" if args.installed_server else "repository_binary"
                    ),
                    "shared_temporal_strict_literals_and_exact_native_row_timestamps": True,
                    "shared_sql_query_budget_bytes": 4294967296,
                    "shared_sql_one_gib_join_budget_rejected": True,
                    "native_fixed_snapshots_three_kinds_schema_drop_and_restart": True,
                    "native_fixed_snapshot_scope_clones_and_session_refresh": True,
                    "native_fixed_snapshot_expiry_roles_and_capacity": True,
                    "empty_directory_create": True,
                    "default_native_storage_query_and_flight_without_feature_switches": True,
                    "native_restart_never_creates_legacy_registry": True,
                    "native_recovery_without_old_table_configuration_or_external_persist_root": True,
                    "native_compute_three_kinds_and_output_restart_recovery": True,
                    "native_compute_same_kernel_writer_epoch_and_lost_receipt_recovery": True,
                    "native_compute_kv_changelog_delete_reinsert_and_restart": True,
                    "native_compute_empty_source_schema_guard": True,
                    "native_compute_parquet_plugin_three_native_output_kinds": True,
                    "native_compute_chain_and_plain_producer_after_restart": True,
                    "native_finite_replay_examples_presubscription_and_output_after_stop": True,
                    "native_compute_rejects_same_name_replacement": True,
                    "native_callbacks_ack_restart_all_three_kinds": True,
                    "native_callbacks_reconnect_real_cancelled_transport": True,
                    "native_callback_same_handle_survives_server_restart": True,
                    "native_callback_rows_filter_latest_without_gateway": True,
                    "durable_ack_atomic_replay_all_three_kinds": True,
                    "durable_ack_lost_response_recovers_from_catalog": True,
                    "durable_group_single_member_and_restart": True,
                    "public_three_kind_drop_after_restart": True,
                    "drop_replay_after_restart": True,
                    "same_name_new_identity_recovers_after_restart": True,
                    "old_requests_and_cursors_cannot_attach_to_replacement": True,
                    "drop_rejects_read_role_and_stale_snapshot": True,
                    "drop_terminates_continuous_logs": True,
                    "flight_atomic_frames_and_mutations": True,
                    "flight_identity_bound_resume_after_delivery": True,
                    "flight_logs_across_repeated_server_starts": True,
                    "server_issued_local_read_sessions": True,
                    "legacy_session_header_rejected": True,
                    "native_state_recovery_across_server_restart": True,
                    "public_schema_evolution_all_three_kinds": True,
                    "atomic_key_value_snapshot_cursor": True,
                    "key_value_recovered": {"symbol": [], "price": []},
                    "deleted_state_cursor_recovered": {"sequence": 10, "version": 6},
                    "frame_rows_recovered": [10, None, 12],
                    "namespace": "market",
                    "durabilities": ["wal", "persisted"],
                    "recovered_values": [42, 43],
                    "idempotent_after_restart": True,
                    "arrow_version": pa.__version__,
                }
            )
        )


if __name__ == "__main__":
    main()
