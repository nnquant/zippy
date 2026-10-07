"""Exercise native fixed snapshots through the public Flight SDK.

:param client: Used by the shared native acceptance harness.
:example: uv run --no-sync scripts/check_native_client.py
"""

from __future__ import annotations

import time
import uuid

import pyarrow as pa
import pyarrow.flight as flight

from zippy_client import Client
from zippy_client._generated import protocol_pb2 as wire
from zippy_client.errors import ServerError


def verify_fixed_snapshots(
    client: Client, namespace_id: uuid.UUID, iteration: int, previous: list[str]
) -> None:
    """Verify three table kinds, schema/drop isolation, ownership and lease cleanup.

    :param client: Current server generation's authenticated SDK client.
    :param namespace_id: Existing market namespace.
    :param iteration: Server restart generation.
    :param previous: Process-local snapshot IDs retained solely to assert restart invalidation.
    :raises AssertionError: A fixed boundary changes, is silently reopened or leaks ownership.
    """
    for snapshot_id in previous:
        try:
            client.flight.read_sql(
                "SELECT * FROM old",
                {"old": {"namespace": "market", "table": "fixed_append"}},
                fixed_snapshot_id=snapshot_id,
            )
        except ServerError as error:
            assert error.code == "NOT_FOUND", error
        else:
            raise AssertionError("a process-local snapshot survived server restart")
    previous.clear()
    if iteration:
        return
    assert "query.fixed_snapshot.v1" in client.flight.capabilities.names
    connection = client.flight._connection("read")
    actions = {
        item.type
        for item in connection.client.list_actions(client.flight._call_options(connection))
    }
    assert {"zippy.create_fixed_snapshot.v1", "zippy.release_fixed_snapshot.v1"} <= actions
    schema = pa.schema([("key", pa.string()), ("value", pa.int64())])
    kinds = ("append", "frame", "key_value")
    names = [("market", f"fixed_{kind}") for kind in kinds]

    def create(kind: str, identity: uuid.UUID) -> None:
        if kind == "append":
            client.create_append_table(
                f"fixed_{kind}",
                schema,
                namespace_id=namespace_id,
                table_id=identity,
                request_id=uuid.uuid4(),
            )
        elif kind == "frame":
            client.create_frame_table(
                f"fixed_{kind}",
                schema,
                by="key",
                namespace_id=namespace_id,
                table_id=identity,
                request_id=uuid.uuid4(),
            )
        else:
            client.create_key_value_table(
                f"fixed_{kind}",
                schema,
                keys=["key"],
                namespace_id=namespace_id,
                table_id=identity,
                request_id=uuid.uuid4(),
            )

    def write(kind: str, data: pa.Table, durability: str) -> None:
        name = f"fixed_{kind}"
        if kind == "append":
            client.flight.write_table(name, data, namespace="market", durability=durability)
        elif kind == "frame":
            client.write_frame(name, data, namespace="market", durability=durability)
        else:
            client.upsert_key_value(name, data, namespace="market", durability=durability)

    for index, kind in enumerate(kinds):
        create(kind, uuid.UUID(int=12000 + index))
        write(kind, pa.table({"key": ["a"], "value": [10]}), "wal")
    scope = client.open_snapshot(names)
    identity = str(scope.info()["snapshot_id"])
    tables = [scope.read_table(name, namespace=namespace) for namespace, name in names]
    joined = tables[0].join(tables[1], on="key").select("key", "value", "value_right")
    manual_join = client.read_table("fixed_append", namespace="market").join(
        client.read_table("fixed_frame", namespace="market"), on="key"
    )
    manual_join_id = str(manual_join.snapshot()["snapshot_id"])
    standalone = client.read_table("fixed_append", namespace="market", snapshot=True)
    manual = client.read_table("fixed_frame", namespace="market")
    manual_snapshot = manual.snapshot()
    manual_snapshot_id = manual_snapshot["snapshot_id"]
    assert isinstance(manual_snapshot_id, str)
    assert manual.snapshot()["snapshot_id"] == manual_snapshot_id
    manual_clone = manual.select("key", "value")
    scope.close()
    for kind in kinds:
        write(kind, pa.table({"key": ["b"], "value": [20]}), "persisted")
    assert standalone.collect().to_pydict() == {"key": ["a"], "value": [10]}
    standalone.close()
    assert manual.collect().to_pydict() == {"key": ["a"], "value": [10]}
    manual.close()
    assert manual_clone.collect().to_pydict() == {"key": ["a"], "value": [10]}
    manual_clone.close()
    assert not client.flight.release_snapshot(manual_snapshot_id)
    # A new authenticated read session must not invalidate existing unexpired snapshots.
    client.flight.refresh_read_session()
    assert joined.collect().to_pydict() == {"key": ["a"], "value": [10], "value_right": [10]}
    assert manual_join.collect().to_pydict() == {"key": ["a"], "value": [10], "value_right": [10]}
    for index, (kind, table) in enumerate(zip(kinds, tables, strict=True)):
        new_schema = schema.append(pa.field("note", pa.string()))
        table_id = uuid.UUID(int=12000 + index)
        client.alter_table_schema(
            table_id,
            new_schema,
            expected_schema_version=1,
            request_id=uuid.uuid4(),
            keys=["key"] if kind == "key_value" else None,
        )
        write(kind, pa.table({"key": ["c"], "value": [30], "note": ["new"]}), "persisted")
        assert table.schema().equals(schema)
        assert table.collect().to_pydict() == {"key": ["a"], "value": [10]}
        current = client.read_table(f"fixed_{kind}", namespace="market").profile()
        metrics = current["metrics"]
        assert isinstance(metrics, dict)
        version = metrics["query"]["snapshots"][0]["snapshot_version"]
        client.drop_table(table_id, expected_snapshot_version=version, request_id=uuid.uuid4())
        create(kind, uuid.UUID(int=12010 + index))
        write(kind, pa.table({"key": ["replacement"], "value": [99]}), "persisted")
        assert table.info()["snapshot_id"] == identity
        assert table.collect().to_pydict() == {"key": ["a"], "value": [10]}
        assert client.read_table(f"fixed_{kind}", namespace="market").collect().to_pydict() == {
            "key": ["replacement"],
            "value": [99],
        }
        table.close()
    # The join owns its right-side view independently from the original table handles.
    assert joined.collect().column("key").to_pylist() == ["a"]
    joined.close()
    assert manual_join.collect().column("key").to_pylist() == ["a"]
    manual_join.close()
    assert not client.flight.release_snapshot(manual_join_id)
    assert not client.flight.release_snapshot(identity)
    try:
        client.flight.read_sql(
            "SELECT * FROM t",
            {"t": {"namespace": "market", "table": "fixed_append"}},
            fixed_snapshot_id=identity,
        )
    except ServerError as error:
        assert error.code == "NOT_FOUND", error
    else:
        raise AssertionError("released snapshot silently read current data")

    binding = {"t": {"namespace": "market", "table": "fixed_append"}}
    expiring = client.flight.create_snapshot(binding, ttl_ms=100)
    time.sleep(0.15)
    try:
        client.flight.read_sql(
            "SELECT * FROM t", binding, fixed_snapshot_id=str(expiring["snapshot_id"])
        )
    except ServerError as error:
        assert error.code in {"NOT_FOUND", "DEADLINE_EXCEEDED"}, error
    else:
        raise AssertionError("expired snapshot silently read current data")
    client.flight.release_snapshot(str(expiring["snapshot_id"]))

    owned = client.flight.create_snapshot(binding)
    identity = str(owned["snapshot_id"])
    release = wire.ReleaseFixedSnapshotRequest(
        protocol_version=1, snapshot_id=uuid.UUID(identity).bytes
    )
    client.flight.refresh_read_session()
    foreign = client.flight._connection("read")
    try:
        list(
            foreign.client.do_action(
                flight.Action("zippy.release_fixed_snapshot.v1", release.SerializeToString()),
                client.flight._call_options(foreign),
            )
        )
    except flight.FlightUnauthorizedError:
        pass
    else:
        raise AssertionError("another authenticated session released a snapshot")
    query = wire.QueryCommand(
        protocol_version=1,
        query_id=uuid.uuid4().bytes,
        session_id=foreign.session.session_id,
        sql="SELECT * FROM t",
        priority=wire.QUERY_PRIORITY_INTERACTIVE,
        fixed_snapshot_id=uuid.UUID(identity).bytes,
        bindings=[
            wire.TableBinding(
                sql_name="t", name=wire.TableName(namespace="market", table="fixed_append")
            )
        ],
        memory_limit=wire.ResourceLimit(kind=wire.ResourceLimit.KIND_SERVER_DEFAULT),
        spill_limit=wire.ResourceLimit(kind=wire.ResourceLimit.KIND_DISABLED),
    )
    try:
        foreign.client.get_flight_info(
            flight.FlightDescriptor.for_command(query.SerializeToString()),
            client.flight._call_options(foreign),
        )
    except flight.FlightUnauthorizedError:
        pass
    else:
        raise AssertionError("another authenticated session queried a snapshot")
    owner = client.flight._snapshot_connection(uuid.UUID(identity).bytes)
    query.session_id = owner.session.session_id
    query.query_id = uuid.uuid4().bytes
    prepared = owner.client.get_flight_info(
        flight.FlightDescriptor.for_command(query.SerializeToString()),
        client.flight._call_options(owner),
    )
    writer = client.flight._connection("writer")
    try:
        list(
            writer.client.do_action(
                flight.Action("zippy.release_fixed_snapshot.v1", release.SerializeToString()),
                client.flight._call_options(writer),
            )
        )
    except flight.FlightUnauthorizedError:
        pass
    else:
        raise AssertionError("writer role managed a read snapshot")
    assert client.flight.release_snapshot(identity)
    write("append", pa.table({"key": ["later"], "value": [100]}), "persisted")
    result = owner.client.do_get(
        prepared.endpoints[0].ticket, client.flight._call_options(owner)
    ).read_all()
    assert result.to_pydict() == {"key": ["replacement"], "value": [99]}
    leases: list[str] = []
    try:
        for _ in range(32):
            leases.append(str(client.flight.create_snapshot(binding)["snapshot_id"]))
        try:
            client.flight.create_snapshot(binding)
        except ServerError as error:
            assert error.code == "RESOURCE_EXHAUSTED", error
        else:
            raise AssertionError("unbounded fixed snapshot lease admission")
    finally:
        for lease in leases:
            client.flight.release_snapshot(lease)
    # Intentionally leave one lease until shutdown to verify shutdown drains it and restart rejects it.
    previous.append(str(client.flight.create_snapshot(binding)["snapshot_id"]))
