"""Contract tests against the repository's real Master, Gateway, and SHM runtime."""

from __future__ import annotations

import os
from pathlib import Path
import socket
import time

import pyarrow as pa
import pytest

import zippy_client as zp

zippy = pytest.importorskip("zippy")


def _reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _wait_until(predicate, timeout_sec: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("condition did not become true before deadline")
        time.sleep(0.005)


@pytest.fixture
def gateway_stack():
    master_uri = f"tcp://127.0.0.1:{_reserve_tcp_port()}"
    gateway_endpoint = f"127.0.0.1:{_reserve_tcp_port()}"
    server = zippy.MasterServer(
        uri=master_uri,
        config={
            "gateway": {
                "enabled": True,
                "endpoint": gateway_endpoint,
                "protocol_version": 1,
                "timeout_sec": 0.05,
            }
        },
    )
    server.start()
    gateway = zippy.GatewayServer(
        endpoint=gateway_endpoint,
        master=zippy.MasterClient(uri=master_uri),
    )
    gateway.start()
    try:
        yield master_uri, gateway
    finally:
        gateway.stop()
        server.stop()
        server.join()


def test_module_level_connect_read_table_and_read_sql_capability(gateway_stack) -> None:
    master_uri, _ = gateway_stack
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )
    client = zp.connect(master_uri)
    writer = zp.get_writer("client_api_ticks", schema=schema, batch_size=2)
    try:
        assert zp.client() is client
        writer.write({"instrument_id": "110001.SH", "last_price": 101.5, "volume": 10})
        writer.write({"instrument_id": "110002.SH", "last_price": 102.5, "volume": 20})

        filtered = (
            zp.read_table("client_api_ticks")
            .filter(zp.col("volume") >= 10)
            .select("instrument_id", "last_price")
            .tail(2)
            .collect()
        )
        streamed = (
            zp.read_table("client_api_ticks", snapshot=False)
            .select("instrument_id", "last_price")
            .tail(2)
            .collect(stream=True)
        )

        assert (
            filtered.to_pydict()
            == streamed.to_pydict()
            == {
                "instrument_id": ["110001.SH", "110002.SH"],
                "last_price": [101.5, 102.5],
            }
        )
        assert client.transport_for("query").selected == zp.Transport.GATEWAY_LOCAL
        with pytest.raises(zp.CapabilityError, match="does not advertise query_sql"):
            zp.read_sql("select * from client_api_ticks")
    finally:
        writer.close()
        client.close()


def test_connect_replaces_and_closes_process_wide_client(gateway_stack) -> None:
    master_uri, _ = gateway_stack

    first = zp.connect(master_uri)
    second = zp.connect(master_uri)
    try:
        assert second is zp.client()
        with pytest.raises(RuntimeError, match="client is closed"):
            first.read_table("unused")
    finally:
        second.close()

    with pytest.raises(RuntimeError, match="call zp.connect"):
        zp.client()


def test_connect_local_false_forces_discovered_gateway_scope(gateway_stack) -> None:
    master_uri, _ = gateway_stack

    client = zp.connect(master_uri, local=False)
    try:
        assert client.default_transport == zp.Transport.GATEWAY_LOCAL
        assert client.transport_for("subscribe").selected == zp.Transport.GATEWAY_LOCAL
    finally:
        client.close()


def test_query_clone_keeps_fixed_snapshot_alive(gateway_stack) -> None:
    master_uri, _ = gateway_stack
    client = zp.Client(master_uri)
    schema = pa.schema([("sequence", pa.int64())])
    writer = client.get_writer("snapshot_clone_ticks", schema=schema, batch_size=1)
    base = client.read_table("snapshot_clone_ticks", snapshot=True)
    derived = None
    try:
        writer.write({"sequence": 1})
        assert base.collect().column("sequence").to_pylist() == [1]
        derived = base.tail(1)

        base.close()

        assert derived.collect().column("sequence").to_pylist() == [1]
    finally:
        if derived is not None:
            derived.close()
        base.close()
        writer.close()
        client.close()


def test_gateway_executes_supported_lazy_plan_operations(gateway_stack) -> None:
    master_uri, _ = gateway_stack
    client = zp.Client(master_uri)
    schema = pa.schema(
        [
            ("sequence", pa.int64()),
            ("instrument_id", pa.string()),
        ]
    )
    writer = client.get_writer("lazy_plan_ticks", schema=schema, batch_size=3)
    try:
        for sequence in (3, 1, 2):
            writer.write({"sequence": sequence, "instrument_id": "110001.SH"})

        query = (
            client.read_table("lazy_plan_ticks", snapshot=False)
            .sort("sequence")
            .slice(1, 1)
            .rename({"sequence": "seq"})
            .drop("instrument_id")
        )
        profile = query.profile()
        metrics = profile["metrics"]
        plan = query.explain()["plan"]

        assert getattr(profile["result"], "to_pydict")() == {"seq": [2]}
        assert isinstance(metrics, dict)
        assert metrics["executor"] == "gateway"
        assert isinstance(plan, list)
        assert [item["op"] for item in plan] == [
            "sort",
            "slice",
            "rename",
            "drop",
        ]
    finally:
        writer.close()
        client.close()


def test_gateway_subscription_waits_for_late_writer(gateway_stack) -> None:
    master_uri, _ = gateway_stack
    client = zp.Client(master_uri)
    schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    rows: list[zp.Row] = []
    subscriber = client.subscribe("late_gateway_ticks", rows.append)
    writer = client.get_writer("late_gateway_ticks", schema=schema, batch_size=1)
    try:
        writer.write(
            {
                "dt": time.time_ns(),
                "instrument_id": "110001.SH",
                "last_price": 101.5,
            }
        )
        _wait_until(lambda: len(rows) == 1)

        assert subscriber.metrics()["transport"] == "gateway"
        assert isinstance(rows[0]["dt"], int)
        assert rows[0]["instrument_id"] == "110001.SH"
    finally:
        subscriber.stop()
        writer.close()
        client.close()


def test_gateway_subscription_does_not_timeout_while_idle(gateway_stack) -> None:
    master_uri, _ = gateway_stack
    client = zp.Client(master_uri)
    schema = pa.schema([("sequence", pa.int64())])
    writer = client.get_writer("idle_gateway_ticks", schema=schema, batch_size=1)
    subscriber = None
    try:
        writer.write({"sequence": 1})
        subscriber = client.subscribe("idle_gateway_ticks", lambda row: None)
        _wait_until(lambda: subscriber.status() == "running")

        time.sleep(0.15)

        assert subscriber.status() == "running"
        assert subscriber.metrics()["reconnects_total"] == 0
    finally:
        if subscriber is not None:
            subscriber.stop()
        writer.close()
        client.close()


@pytest.mark.skipif(os.name == "nt", reason="Unix SHM namespace test")
@pytest.mark.parametrize(("xfast", "poll_interval_ms"), [(False, 2), (True, 0)])
def test_shm_subscription_drains_rows_before_descriptor_rebind(
    tmp_path: Path,
    xfast: bool,
    poll_interval_ms: int,
) -> None:
    endpoint = str(tmp_path / "master.sock")
    server = zippy.MasterServer(control_endpoint=endpoint)
    server.start()
    master = zippy.MasterClient(control_endpoint=endpoint)
    schema = pa.schema(
        [
            ("seq", pa.int64()),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    pipeline = (
        zippy.Pipeline("client_shm_rollover", master=master)
        .stream_table(
            "client_shm_rollover_ticks",
            schema=schema,
            row_capacity=2,
            retention_segments=4,
        )
        .start()
    )
    client = zp.Client(f"unix://{endpoint}")
    rows: list[zp.Row] = []
    subscriber = client.subscribe(
        "client_shm_rollover_ticks",
        rows.append,
        poll_interval_ms=poll_interval_ms,
        xfast=xfast,
        idle_spin_checks=32,
    )
    try:
        _wait_until(lambda: subscriber.status() == "running")
        for sequence in range(1, 7):
            pipeline.write(
                {
                    "seq": [sequence],
                    "instrument_id": ["110001.SH"],
                    "last_price": [100.0 + sequence],
                }
            )
        _wait_until(lambda: len(rows) == 6)

        assert [row["seq"] for row in rows] == [1, 2, 3, 4, 5, 6]
        assert subscriber.metrics()["transport"] == "shm"
        descriptor_rebinds = subscriber.metrics()["descriptor_rebinds_total"]
        assert isinstance(descriptor_rebinds, int)
        assert descriptor_rebinds >= 2
        assert subscriber.metrics()["xfast"] is xfast
        assert subscriber.metrics()["idle_spin_checks"] == 32
        assert subscriber.health()["healthy"] is True
    finally:
        subscriber.stop()
        client.close()
        pipeline.stop()
        server.stop()
