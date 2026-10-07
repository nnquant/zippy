"""Delivery probes must report only this handle's observations and propagate failures."""

from __future__ import annotations

from pathlib import Path
import runpy
from types import SimpleNamespace

import pyarrow as pa
import pytest
from zippy_client import Row


@pytest.fixture
def probe():
    return runpy.run_path(
        str(
            Path(__file__).resolve().parents[1]
            / "examples/08_remote_flight/02_read_and_subscribe.py"
        )
    )["measure_delivery"]


def fake_client(values, *, mismatched=False, error=None):
    events = []
    state = SimpleNamespace(events=events)

    class Subscriber:
        def stop(self):
            events.append("stop")
            if error:
                raise error

        def join(self, timeout):
            events.append(("join", timeout))
            if error:
                raise error

        def metrics(self):
            rows = sum(value.num_rows if isinstance(value, pa.Table) else 1 for value in values)
            return {
                "rows_delivered_total": rows + int(mismatched),
                "batches_delivered_total": len(values),
            }

    class Client:
        def subscribe(self, name, callback, *, options):
            events.append(("open", name, options))
            for value in values:
                callback(value)
            return Subscriber()

        subscribe_table = subscribe

    state.client = Client()
    return state


@pytest.mark.parametrize(
    "mode,values",
    [
        ("rows", [Row({"x": 1}), Row({"x": 2})]),
        ("tables", [pa.table({"x": [1, 2]})]),
    ],
)
def test_probe_counts_own_callbacks_and_closes(probe, mode, values):
    fake = fake_client(values)
    result = probe(fake.client, table="ticks", namespace="market", mode=mode, rows=2, timeout_sec=1)
    assert result["received_rows"] == 2
    assert result["callbacks"] == len(values)
    assert result["first_callback_wait_ms"] >= 0
    assert result["elapsed_ms"] >= result["first_callback_wait_ms"]
    assert fake.events[-1] == "stop"
    assert fake.events[0][2].namespace == "market"


def test_probe_retains_whole_frame_past_requested_minimum(probe):
    fake = fake_client([pa.table({"x": [1, 2, 3]})])
    result = probe(
        fake.client,
        table="frames",
        namespace="market",
        mode="tables",
        rows=2,
        timeout_sec=1,
        batch_rows=1,
    )
    assert result["requested_minimum_rows"] == 2
    assert result["received_rows"] == 3
    assert result["sample_batch_rows"] == [3]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rows": 0},
        {"rows": True},
        {"mode": "unknown"},
        {"start": "committed"},
        {"timeout_sec": 0},
        {"timeout_sec": float("nan")},
        {"timeout_sec": float("inf")},
    ],
)
def test_probe_rejects_invalid_options_before_subscribing(probe, kwargs):
    fake = fake_client([])
    options = {
        "table": "ticks",
        "namespace": "market",
        "mode": "tables",
        "rows": 1,
        "timeout_sec": 1,
        **kwargs,
    }
    with pytest.raises(ValueError):
        probe(fake.client, **options)
    assert not fake.events


def test_probe_propagates_terminal_failure_and_closes(probe):
    error = RuntimeError("subscription failed")
    fake = fake_client([], error=error)
    with pytest.raises(RuntimeError, match="subscription failed"):
        probe(fake.client, table="ticks", namespace="market", mode="tables", rows=1, timeout_sec=1)
    assert fake.events[-1] == "stop"


def test_probe_rejects_inconsistent_counters(probe):
    fake = fake_client([pa.table({"x": [1]})], mismatched=True)
    with pytest.raises(RuntimeError, match="counters disagree"):
        probe(fake.client, table="ticks", namespace="market", mode="tables", rows=1, timeout_sec=1)
    assert fake.events[-1] == "stop"


def test_probe_times_out_and_closes(probe):
    fake = fake_client([])
    with pytest.raises(TimeoutError, match="timed out"):
        probe(
            fake.client, table="ticks", namespace="market", mode="tables", rows=1, timeout_sec=0.001
        )
    assert fake.events[-1] == "stop"
