"""Native WebUI namespace, fixed snapshot, preview and lifecycle contracts."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pyarrow as pa
import pytest

from zippy import webui
from zippy.native_inspection import query_snapshots


@pytest.fixture
def native_ui(monkeypatch, tmp_path):
    events = []
    data = pa.table({"id": [3, 1, 2], "value": [30, 10, 20]})
    state = SimpleNamespace(data=data, events=events, error=None)

    class Table:
        def __init__(self, namespace, data=None):
            self.namespace = namespace
            self.data = state.data if data is None else data

        def __enter__(self):
            events.append("table_open")
            return self

        def __exit__(self, *args):
            events.append("table_close")

        def info(self):
            return {
                "namespace": self.namespace,
                "name": "ticks",
                "catalog": "zippy",
                "table_type": "TABLE",
            }

        def schema(self):
            return self.data.schema

        def sort(self, columns, *, descending):
            events.append(("sort", columns, descending))
            return Table(
                self.namespace,
                self.data.sort_by(
                    [(key, "descending" if descending else "ascending") for key in columns]
                ),
            )

        def head(self, count):
            events.append(("head", count))
            return Table(self.namespace, self.data.slice(0, count))

        def tail(self, count):
            events.append(("tail", count))
            return Table(self.namespace, self.data.slice(max(0, len(self.data) - count)))

        def profile(self):
            if state.error:
                raise state.error
            return {
                "result": self.data,
                "metrics": {
                    "query": {"snapshots": [{"table_id": "fixed-id", "snapshot_version": 7}]}
                },
            }

    class Client:
        def __init__(self, uri):
            events.append(("connect", uri))

        def __enter__(self):
            events.append("client_open")
            return self

        def __exit__(self, *args):
            events.append("client_close")

        def list_tables(self):
            if state.error:
                raise state.error
            return [Table(namespace).info() for namespace in ["z", "a"]]

        def read_table(self, name, *, namespace, snapshot):
            events.append(("read", name, namespace, snapshot))
            return Table(namespace)

    monkeypatch.setattr(webui, "Client", Client)
    monkeypatch.setattr(webui, "_pm_summary", lambda: {"status": "ok", "error": None, "tasks": []})
    state.config = webui.WebuiConfig(uri="test-uri", host="127.0.0.1", port=17688, log_dir=tmp_path)
    return state


def preview(**kwargs):
    return webui._table_data_preview(
        table_name="ticks",
        uri="test-uri",
        namespace="market",
        mode=kwargs.pop("mode", "head"),
        limit=kwargs.pop("limit", 2),
        order_by=kwargs.pop("order_by", None),
        descending=kwargs.pop("descending", False),
        **kwargs,
    )


def test_catalog_lists_namespaces_without_per_table_queries(native_ui):
    payload = webui.DashboardService(native_ui.config).dashboard()
    assert [table["namespace"] for table in payload["tables"]] == ["a", "z"]
    assert payload["totals"] == {"tables": 2, "namespaces": 2, "tasks": 0}
    assert payload["catalog"] == {"status": "ok", "error": None}
    assert not {"master", "gateway", "snapshot", "stale_error_tables"} & payload.keys()
    assert native_ui.events == [("connect", "test-uri"), "client_open", "client_close"]


def test_catalog_failure_does_not_hide_pm(native_ui):
    native_ui.error = RuntimeError("catalog unavailable")
    payload = webui.DashboardService(native_ui.config).dashboard()
    assert payload["catalog"] == {"status": "error", "error": "catalog unavailable"}
    assert payload["pm"]["status"] == "ok"
    assert native_ui.events[-1] == "client_close"


def test_detail_pins_metadata_and_closes_resources(native_ui):
    payload = webui._table_detail("ticks", "test-uri", "market")
    assert payload["ok"]
    assert payload["table"]["namespace"] == "market"
    assert payload["table"]["snapshots"][0]["snapshot_version"] == 7
    assert ("read", "ticks", "market", True) in native_ui.events
    assert ("head", 0) in native_ui.events
    assert native_ui.events.count("table_open") == native_ui.events.count("table_close") == 2
    assert native_ui.events[-1] == "client_close"


@pytest.mark.parametrize("limit,truncated", [(2, True), (3, False), (4, False)])
def test_preview_exact_truncation(native_ui, limit, truncated):
    payload = preview(limit=limit)
    assert payload["ok"] and payload["truncated"] is truncated
    assert payload["row_count"] == min(limit, 3)
    assert payload["schema"]["fields"][0]["data_type"] == "int64"
    assert payload["snapshots"][0]["table_id"] == "fixed-id"
    assert native_ui.events.count("table_open") == native_ui.events.count("table_close")


@pytest.mark.parametrize("descending,expected", [(False, [2, 3]), (True, [2, 1])])
def test_tail_uses_explicit_order_and_removes_lookahead_first(native_ui, descending, expected):
    payload = preview(mode="tail", order_by=["id"], descending=descending)
    assert payload["ok"], payload
    assert [row["id"] for row in payload["rows"]] == expected
    assert payload["truncated"]
    assert native_ui.events.count("table_open") == native_ui.events.count("table_close") == 3


@pytest.mark.parametrize(
    "kwargs", [{"mode": "tail"}, {"mode": "invalid"}, {"limit": 0}, {"limit": 1001}]
)
def test_invalid_preview_never_connects(native_ui, kwargs):
    assert not preview(**kwargs)["ok"]
    assert not native_ui.events


def test_unknown_sort_column_fails_and_releases_snapshot(native_ui):
    payload = preview(order_by=["missing"])
    assert payload["error"] == "order_by contains an unknown column"
    assert native_ui.events[-2:] == ["table_close", "client_close"]


@pytest.mark.parametrize(
    "operation", [lambda: preview(), lambda: webui._table_detail("ticks", "test-uri", "market")]
)
def test_query_failure_closes_all_resources(native_ui, operation):
    native_ui.error = RuntimeError("access denied")
    payload = operation()
    assert payload["error"] == "access denied"
    assert native_ui.events.count("table_open") == native_ui.events.count("table_close") == 2
    assert native_ui.events[-1] == "client_close"


def test_preview_json_preserves_nanoseconds_large_integers_and_nonfinite(native_ui):
    native_ui.data = pa.table(
        {
            "ts": pa.array([1, None, -1], type=pa.timestamp("ns", tz="UTC")),
            "large": [2**63 - 1, None, -(2**63)],
            "float": [float("inf"), float("nan"), float("-inf")],
            "bytes": [b"\xff", None, b"\x00"],
        }
    )
    payload = preview(limit=3)
    assert payload["ok"], payload
    encoded = json.dumps(payload, allow_nan=False)
    assert "9223372036854775807" in encoded
    assert payload["rows"][0]["large"] == str(2**63 - 1)
    assert payload["rows"][0]["ts"] == "1970-01-01 00:00:00.000000001Z"
    assert payload["rows"][1]["ts"] is None
    assert payload["rows"][2]["ts"] == "1969-12-31 23:59:59.999999999Z"
    assert payload["rows"][0]["bytes"] == "ff"


@pytest.mark.parametrize(
    "profile",
    [
        {},
        {"metrics": {}},
        {"metrics": {"query": {"snapshots": []}}},
        {"metrics": {"query": {"snapshots": [1]}}},
    ],
)
def test_missing_native_identity_is_rejected(profile):
    with pytest.raises(RuntimeError, match="snapshot metadata"):
        query_snapshots(profile)
