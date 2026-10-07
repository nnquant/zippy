"""Verify packaged WebUI HTTP reads against native tables across server restarts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import uuid

import pyarrow as pa
from zippy_client import Client


def verify_native_webui(
    client: Client,
    directory: Path,
    namespace_id: uuid.UUID,
    credentials: dict[str, str],
    iteration: int,
) -> None:
    """Exercise native HTTP identity, three table kinds and exact preview values.

    :param client: Administrator connected to the current native server generation.
    :param directory: Temporary server directory and HTTP log destination.
    :param namespace_id: Existing market namespace identity.
    :param credentials: Test-only bootstrap credentials; HTTP process receives read scope.
    :param iteration: Server generation; create persistent fixtures only at zero.
    :raises AssertionError: HTTP data or identity differs before or after restart.
    """
    other_namespace = uuid.UUID(int=9400)
    if iteration == 0:
        client.create_namespace(
            "webui_other", namespace_id=other_namespace, request_id=uuid.UUID(int=9401)
        )
        for index, (namespace, identity) in enumerate(
            (("market", namespace_id), ("webui_other", other_namespace))
        ):
            values = pa.table(
                {
                    "id": [3, 1, 2],
                    "ns": [namespace] * 3,
                    "large": [2**63 - 1] * 3,
                    "dt": pa.array([3, 1, 2], type=pa.timestamp("ns", tz="UTC")),
                }
            )
            client.create_append_table(
                "webui_same_name",
                values.schema,
                namespace_id=identity,
                table_id=uuid.UUID(int=9410 + index),
                request_id=uuid.UUID(int=9420 + index),
            )
            client.flight.write_table(
                "webui_same_name", values, namespace=namespace, durability="persisted"
            )
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"

    def get(path: str) -> Any:
        with urlopen(base_url + path, timeout=15) as response:
            return json.load(response)

    with (directory / f"webui-{iteration}.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-m",
                "zippy",
                "webui",
                "--uri",
                str(directory / "control.sock"),
                "--port",
                str(port),
                "--log-dir",
                str(directory / "logs"),
            ],
            env={**os.environ, "ZIPPY_FLIGHT_AUTH_TOKEN": credentials["read"]},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 20
            while True:
                try:
                    with urlopen(base_url + "/", timeout=1) as response:
                        assert '<div id="root"></div>' in response.read().decode()
                    break
                except (URLError, TimeoutError):
                    assert (
                        process.poll() is None and time.monotonic() < deadline
                    ), "webui startup failed"
                    time.sleep(0.03)
            dashboard = get("/api/dashboard")
            assert dashboard["catalog"] == {"status": "ok", "error": None}, dashboard
            listed = {(item["namespace"], item["name"]) for item in dashboard["tables"]}
            assert {("market", "webui_same_name"), ("webui_other", "webui_same_name")} <= listed
            assert not {"master", "gateway", "snapshot"} & dashboard.keys()
            for kind in ("append", "frame", "key_value"):
                detail = get(f"/api/table/cli_{kind}?namespace=market")
                assert detail["ok"], detail
                assert detail["table"]["snapshots"][0]["snapshot_version"] >= 1
                preview = get(f"/api/table/cli_{kind}/data?namespace=market&limit=1")
                assert preview["ok"] and preview["row_count"] == 1, preview
                assert preview["truncated"] is False
                assert preview["rows"] == [{"id": "A", "dt": "1970-01-01 00:00:00.000000001Z"}]
            for namespace, table_id in (("market", 9410), ("webui_other", 9411)):
                detail = get("/api/table/webui_same_name?" + urlencode({"namespace": namespace}))
                assert detail["table"]["namespace"] == namespace
                assert detail["table"]["snapshots"][0]["table_id"] == str(uuid.UUID(int=table_id))
                for descending, expected in ((False, [2, 3]), (True, [2, 1])):
                    query = urlencode(
                        {
                            "namespace": namespace,
                            "mode": "tail",
                            "limit": 2,
                            "order_by": "id",
                            "descending": str(descending).lower(),
                        }
                    )
                    data = get("/api/table/webui_same_name/data?" + query)
                    assert data["ok"] and data["truncated"], data
                    assert [row["id"] for row in data["rows"]] == expected, data
                    assert all(
                        row["ns"] == namespace and row["large"] == str(2**63 - 1)
                        for row in data["rows"]
                    )
                    assert data["snapshots"][0]["table_id"] == str(uuid.UUID(int=table_id))
            rejected = get("/api/table/webui_same_name/data?namespace=market&mode=tail")
            assert not rejected["ok"] and "order_by" in rejected["error"]
            missing = get("/api/table/webui_same_name?namespace=absent")
            assert not missing["ok"]
            for query in ("limit=1001", "limit=0", "mode=invalid"):
                try:
                    get("/api/table/webui_same_name/data?namespace=market&" + query)
                except HTTPError as error:
                    assert error.code == 422
                else:
                    raise AssertionError("invalid preview options were accepted")
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
