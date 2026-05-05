import argparse
import json
import importlib.util
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import zippy

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


def git_output(*args: str) -> str:
    if not (WORKSPACE_ROOT / ".git").exists():
        pytest.skip("git metadata is unavailable in this workspace")

    try:
        result = subprocess.run(
            ["git", "-C", str(WORKSPACE_ROOT), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        pytest.skip(f"git metadata is unavailable: {error}")

    return result.stdout.strip()


def reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_expr_factory_replaces_expr() -> None:
    factor = zippy.Expr(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"
    assert not hasattr(zippy, "EXPR")


def test_expression_factor_exposes_native_attributes() -> None:
    factor = zippy.ExpressionFactor(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"


def test_reactive_engine_accepts_polars_and_flushes() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A", "A"], "price": [10.0, 11.0]}))
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_record_batch() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["A", "A"]),
            pa.array([10.0, 11.0]),
        ],
        schema=schema,
    )

    engine.start()
    engine.write(batch)
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_table() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    table = pa.table(
        {
            "symbol": ["A", "A"],
            "price": [10.0, 11.0],
        },
        schema=schema,
    )

    engine.start()
    engine.write(table)
    engine.flush()
    engine.stop()


def test_reactive_latest_engine_accepts_single_and_multi_by() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("exchange_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    single_by = zippy.ReactiveLatestEngine(
        name="latest_by_instrument",
        input_schema=schema,
        by="instrument_id",
        target=zippy.NullPublisher(),
    )
    multi_by = zippy.ReactiveLatestEngine(
        name="latest_by_instrument_exchange",
        input_schema=schema,
        by=["instrument_id", "exchange_id"],
        target=zippy.NullPublisher(),
    )

    assert single_by.output_schema() == schema
    assert single_by.config()["by"] == ["instrument_id"]
    assert multi_by.config()["by"] == ["instrument_id", "exchange_id"]

    single_by.start()
    single_by.write(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606"],
            "exchange_id": ["CFFEX", "CFFEX", "CFFEX"],
            "last_price": [3912.4, 2740.8, 3913.2],
        }
    )
    single_by.flush()
    single_by.stop()


def test_reactive_latest_engine_infers_schema_from_named_segment_source(
    tmp_path: Path,
) -> None:
    try:
        server, control_endpoint = start_master_server(tmp_path)
    except RuntimeError as error:
        if "Operation not permitted" in str(error):
            pytest.skip("current sandbox cannot bind zippy master control socket")
        raise
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("exchange_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint=control_endpoint)
    master.register_process("reactive_latest_engine")
    master.register_stream("openctp_ticks", schema, 64, 4096)

    try:
        engine = zippy.ReactiveLatestEngine(
            name="latest_by_instrument",
            source="openctp_ticks",
            by="instrument_id",
            target=zippy.NullPublisher(),
            master=master,
        )

        assert engine.output_schema() == schema
        assert engine.config()["by"] == ["instrument_id"]
        assert engine.config()["source"] == "openctp_ticks"
        assert engine.config()["source_linked"] is True
    finally:
        server.stop()


def test_reactive_latest_engine_named_source_requires_master() -> None:
    with pytest.raises(ValueError, match="master is required when source is a stream name"):
        zippy.ReactiveLatestEngine(
            name="latest_by_instrument",
            source="openctp_ticks",
            by="instrument_id",
            target=zippy.NullPublisher(),
        )


def test_session_engine_factory_defaults_target_and_returns_session() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    session = zippy.Session(name="latest_session", master=master)

    returned = session.engine(
        zippy.ReactiveLatestEngine,
        name="latest_by_instrument",
        input_schema=schema,
        by="instrument_id",
    )

    assert returned is session
    engines = session.engines()
    assert len(engines) == 1
    assert engines[0].output_schema() == schema
    assert engines[0].config()["targets"] == [{"type": "null"}]


def test_session_engine_factory_injects_master_for_named_source() -> None:
    captured: dict[str, object] = {}

    class CapturingEngine:
        def __init__(self, *, source, master=None, target=None):
            captured["source"] = source
            captured["master"] = master
            captured["target"] = target
            self._status = "created"

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    session = zippy.Session(name="latest_session", master=master)

    session.engine(CapturingEngine, source="openctp_ticks")

    assert captured["source"] == "openctp_ticks"
    assert captured["master"] is master
    assert isinstance(captured["target"], zippy.NullPublisher)


def test_session_named_source_materializes_default_output_table() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def __init__(self):
            self.registered_streams: list[tuple[str, pa.Schema, int, int]] = []
            self.registered_sources: list[tuple[str, str, str, dict[str, object]]] = []
            self.published_descriptors: list[tuple[str, dict[str, object]]] = []

        def process_id(self) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "retention_segments": None,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            self.registered_streams.append((stream_name, stream_schema, buffer_size, frame_size))

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            self.registered_sources.append((source_name, source_type, output_stream, config))

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            self.published_descriptors.append((stream_name, descriptor))

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    class DummyRuntime:
        def join(self) -> None:
            return None

        def stop(self) -> None:
            return None

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self.name = name
            self.source = source
            self.master = master
            self.target = target
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

        def _zippy_start(self, sink):
            return DummyRuntime()

    master = FakeMaster()
    session = zippy.Session(name="latest_session", master=master)

    session.engine(
        CapturingPythonSourceEngine,
        name="ctp_ticks_latest",
        source="ldc_ctp_ticks",
    )

    assert master.registered_streams == []
    try:
        session.run()
    finally:
        session.stop()

    assert master.registered_streams == [("ctp_ticks_latest", schema, 64, 4096)]
    assert master.registered_sources == [
        (
            "latest_session.ctp_ticks_latest.materializer.proc_test",
            "session_engine_output",
            "ctp_ticks_latest",
            {"session": "latest_session"},
        )
    ]
    assert master.published_descriptors[0][0] == "ctp_ticks_latest"
    assert len(session.engines()) == 1


def test_session_stop_unregisters_materializer_source() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def __init__(self):
            self.registered_sources: list[str] = []
            self.unregistered_sources: list[str] = []

        def process_id(self) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "retention_segments": None,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            self.registered_sources.append(source_name)

        def unregister_source(self, source_name: str) -> None:
            self.unregistered_sources.append(source_name)

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            return None

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    class DummyRuntime:
        def join(self) -> None:
            return None

        def stop(self) -> None:
            return None

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self.name = name
            self.source = source
            self.master = master
            self.target = target
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

        def _zippy_start(self, sink):
            return DummyRuntime()

    master = FakeMaster()
    session = zippy.Session(name="latest_session", master=master)
    session.engine(
        CapturingPythonSourceEngine,
        name="ctp_ticks_latest",
        source="ldc_ctp_ticks",
    )

    session.run()
    session.stop()

    assert master.registered_sources == [
        "latest_session.ctp_ticks_latest.materializer.proc_test",
    ]
    assert master.unregistered_sources == [
        "latest_session.ctp_ticks_latest.materializer.proc_test",
    ]


def test_session_engine_output_overrides_default_output_table_name() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def __init__(self):
            self.registered_streams: list[tuple[str, pa.Schema, int, int]] = []
            self.registered_sources: list[tuple[str, str, str, dict[str, object]]] = []
            self.published_descriptors: list[tuple[str, dict[str, object]]] = []

        def process_id(self) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "retention_segments": None,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            self.registered_streams.append((stream_name, stream_schema, buffer_size, frame_size))

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            self.registered_sources.append((source_name, source_type, output_stream, config))

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            self.published_descriptors.append((stream_name, descriptor))

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    class DummyRuntime:
        def join(self) -> None:
            return None

        def stop(self) -> None:
            return None

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self.name = name
            self.source = source
            self.master = master
            self.target = target
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

        def _zippy_start(self, sink):
            return DummyRuntime()

    master = FakeMaster()
    session = zippy.Session(name="latest_session", master=master)

    session.engine(
        CapturingPythonSourceEngine,
        name="latest_engine",
        source="ldc_ctp_ticks",
        output_stream="ctp_ticks_latest",
    )

    engine = session.engines()[0]
    assert engine.name == "latest_engine"
    assert master.registered_streams == [("ctp_ticks_latest", schema, 64, 4096)]
    assert master.registered_sources == [
        (
            "latest_session.ctp_ticks_latest.materializer.proc_test",
            "session_engine_output",
            "ctp_ticks_latest",
            {"session": "latest_session"},
        )
    ]
    assert master.published_descriptors[0][0] == "ctp_ticks_latest"


def test_session_stream_table_materializes_last_engine_output() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def __init__(self):
            self.registered_streams: list[tuple[str, pa.Schema, int, int]] = []
            self.registered_sources: list[tuple[str, str, str, dict[str, object]]] = []
            self.published_descriptors: list[tuple[str, dict[str, object]]] = []

        def process_id(self) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "retention_segments": None,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            self.registered_streams.append((stream_name, stream_schema, buffer_size, frame_size))

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            self.registered_sources.append((source_name, source_type, output_stream, config))

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            self.published_descriptors.append((stream_name, descriptor))

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    class DummyRuntime:
        def join(self) -> None:
            return None

        def stop(self) -> None:
            return None

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self.name = name
            self.source = source
            self.master = master
            self.target = target
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

        def _zippy_start(self, sink):
            return DummyRuntime()

    master = FakeMaster()
    session = zippy.Session(name="latest_session", master=master)

    returned = session.engine(
        CapturingPythonSourceEngine,
        name="latest_engine",
        source="ldc_ctp_ticks",
    ).stream_table("ctp_ticks_latest", persist=False)

    assert returned is session
    assert session.engines()[0].name == "latest_engine"
    assert master.registered_streams == [("ctp_ticks_latest", schema, 64, 4096)]
    assert master.registered_sources == [
        (
            "latest_session.ctp_ticks_latest.materializer.proc_test",
            "session_engine_output",
            "ctp_ticks_latest",
            {"session": "latest_session"},
        )
    ]
    assert master.published_descriptors[0][0] == "ctp_ticks_latest"


def test_session_engine_rejects_output_stream_with_explicit_target() -> None:
    class CapturingEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self._status = "created"

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    session = zippy.Session(name="latest_session", master=master)

    with pytest.raises(ValueError, match="output_stream cannot be combined with explicit target"):
        session.engine(
            CapturingEngine,
            name="latest_engine",
            source="ldc_ctp_ticks",
            output_stream="ctp_ticks_latest",
            target=zippy.NullPublisher(),
        )


def test_session_engine_rejects_legacy_output_parameter() -> None:
    class CapturingEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self._status = "created"

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    session = zippy.Session(name="latest_session", master=master)

    with pytest.raises(TypeError, match="use output_stream or .stream_table"):
        session.engine(
            CapturingEngine,
            name="latest_engine",
            source="ldc_ctp_ticks",
            output="ctp_ticks_latest",
        )


def test_session_engine_persist_false_disables_default_output_persistence(monkeypatch) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def register_stream(self, *args) -> None:
            return None

        def publish_segment_descriptor(self, *args) -> None:
            return None

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "persist": {
                        "enabled": True,
                        "method": "parquet",
                        "data_dir": "/tmp/zippy-session-persist",
                        "partition": {},
                    },
                },
            }

    captured_materializer: dict[str, object] = {}

    class CapturingStreamTableMaterializer:
        def __init__(self, **kwargs):
            captured_materializer.update(kwargs)
            self._status = "created"

        def active_descriptor(self) -> dict[str, object]:
            return {}

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", CapturingStreamTableMaterializer)
    session = zippy.Session(name="latest_session", master=FakeMaster())
    session.engine(
        CapturingPythonSourceEngine,
        name="ctp_ticks_latest",
        source="ldc_ctp_ticks",
        persist=False,
    )

    try:
        session.run()
    finally:
        session.stop()

    assert captured_materializer["name"] == "ctp_ticks_latest"
    assert captured_materializer["persist_path"] is None


def test_session_engine_persist_true_materializes_parquet_output(monkeypatch) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def register_stream(self, *args) -> None:
            return None

        def publish_segment_descriptor(self, *args) -> None:
            return None

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "persist": {
                        "enabled": False,
                        "method": "parquet",
                        "data_dir": "/tmp/zippy-session-persist",
                        "partition": {},
                    },
                },
            }

    captured_materializer: dict[str, object] = {}

    class CapturingStreamTableMaterializer:
        def __init__(self, **kwargs):
            captured_materializer.update(kwargs)
            self._status = "created"

        def active_descriptor(self) -> dict[str, object]:
            return {}

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    class CapturingPythonSourceEngine:
        def __init__(self, *, name, source, master=None, target=None):
            self._status = "created"

        def output_schema(self):
            return schema

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

        def _zippy_output_schema(self):
            return schema

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", CapturingStreamTableMaterializer)
    session = zippy.Session(name="latest_session", master=FakeMaster())
    session.engine(
        CapturingPythonSourceEngine,
        name="ctp_ticks_latest",
        source="ldc_ctp_ticks",
        persist=True,
    )

    try:
        session.run()
    finally:
        session.stop()

    assert captured_materializer["name"] == "ctp_ticks_latest"
    assert captured_materializer["persist_path"] == "/tmp/zippy-session-persist/ctp_ticks_latest"
    assert captured_materializer["persist_publisher"] is not None


def test_session_run_starts_and_stop_stops_owned_engines() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    engine = zippy.ReactiveLatestEngine(
        name="latest_by_instrument",
        input_schema=schema,
        by="instrument_id",
        target=zippy.NullPublisher(),
    )
    session = zippy.Session(name="latest_session", master=master).engine(engine)

    session.run()
    assert engine.status() == "running"

    session.stop()
    assert engine.status() == "stopped"


def test_session_run_keeps_session_alive_until_stop() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint="/tmp/zippy-session-test.sock")
    engine = zippy.ReactiveLatestEngine(
        name="latest_by_instrument",
        input_schema=schema,
        by="instrument_id",
        target=zippy.NullPublisher(),
    )
    session = zippy.Session(name="latest_session_keepalive", master=master).engine(engine)

    session.run()
    assert zippy._ACTIVE_SESSIONS["latest_session_keepalive"] is session

    session.stop()
    assert "latest_session_keepalive" not in zippy._ACTIVE_SESSIONS


def test_parquet_replay_source_emits_parquet_batches(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IC2606"],
            "last_price": [4102.5, 2711.0, 6123.5],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(table, parquet_path)

    class CapturingSink:
        def __init__(self) -> None:
            self.hello: list[tuple[str, int]] = []
            self.tables: list[pa.Table] = []
            self.flushed = False
            self.stopped = False
            self.flushed_event = threading.Event()

        def emit_hello(self, stream_name: str, protocol_version: int = 1) -> None:
            self.hello.append((stream_name, protocol_version))

        def emit_data(self, value) -> None:
            self.tables.append(value)

        def emit_flush(self) -> None:
            self.flushed = True
            self.flushed_event.set()

        def emit_stop(self) -> None:
            self.stopped = True

        def emit_error(self, reason: str) -> None:
            raise AssertionError(reason)

    source = zippy.ParquetReplaySource(parquet_path, batch_size=2)
    sink = CapturingSink()

    handle = source._zippy_start(sink)
    assert sink.flushed_event.wait(timeout=2.0)
    handle.stop()
    handle.join()

    assert source._zippy_output_schema() == schema
    assert source._zippy_source_name() == "parquet_replay"
    assert source._zippy_source_type() == "parquet_replay"
    assert sink.hello == [("parquet_replay", 1)]
    assert [item.num_rows for item in sink.tables] == [2, 1]
    assert pa.concat_tables(sink.tables) == table
    assert sink.flushed is True
    assert sink.stopped is True


def test_replay_function_replays_persisted_table_to_named_stream(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    replay_engine = None
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    try:
        zippy.connect(uri=control_endpoint, app="table_replay_test")
        live_pipeline = (
            zippy.Pipeline("live_ingest_for_replay")
            .stream_table(
                "live_ticks",
                schema=schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606"],
                "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
                "last_price": [3898.0, 2675.0, 3898.5],
            }
        )

        live_query = zippy.read_table("live_ticks")
        for _ in range(50):
            persisted_files = live_query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 2:
                break
            time.sleep(0.02)
        expected = live_query._scan_persisted().to_table()
        assert expected.num_rows == 2

        replay_engine = zippy.replay(
            "live_ticks",
            output_stream="replay_ticks",
            batch_size=2,
            row_capacity=16,
            persist=None,
        )

        replayed = zippy.read_table("replay_ticks").collect()

        assert isinstance(replay_engine, zippy.TableReplayEngine)
        assert replay_engine.status() == "running"
        assert replay_engine.output_stream == "replay_ticks"
        assert replay_engine.output_schema() == schema
        assert replayed == expected
    finally:
        if replay_engine is not None:
            replay_engine.stop()
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_replay_function_replays_persisted_table_to_callback(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    received: list[dict[str, object]] = []

    def on_row(row: zippy.Row) -> None:
        received.append(row.to_dict())

    try:
        zippy.connect(uri=control_endpoint, app="table_replay_callback_test")
        live_pipeline = (
            zippy.Pipeline("live_ingest_for_callback_replay")
            .stream_table(
                "callback_live_ticks",
                schema=schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606"],
                "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
                "last_price": [3898.0, 2675.0, 3898.5],
            }
        )

        query = zippy.read_table("callback_live_ticks")
        for _ in range(50):
            persisted_files = query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 2:
                break
            time.sleep(0.02)

        expected = query._scan_persisted().to_table().to_pylist()
        replay_engine = zippy.replay("callback_live_ticks", callback=on_row)

        assert isinstance(replay_engine, zippy.TableReplayEngine)
        assert replay_engine.status() == "completed"
        assert received == expected
    finally:
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_replay_function_filters_persisted_table_by_time_window(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    received: list[dict[str, object]] = []

    def on_row(row: zippy.Row) -> None:
        received.append(row.to_dict())

    try:
        zippy.connect(uri=control_endpoint, app="table_replay_window_test")
        live_pipeline = (
            zippy.Pipeline("live_ingest_for_window_replay")
            .stream_table(
                "window_live_ticks",
                schema=schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606", "IC2606"],
                "dt": [
                    1777017600000000000,
                    1777017600000001000,
                    1777017600000002000,
                    1777017600000003000,
                    1777017600000004000,
                ],
                "last_price": [3898.0, 2675.0, 3898.5, 2675.5, 6123.0],
            }
        )

        query = zippy.read_table("window_live_ticks")
        for _ in range(50):
            persisted_files = query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 4:
                break
            time.sleep(0.02)
        expected = query._scan_persisted().to_table().slice(1, 2).to_pylist()

        replay_engine = zippy.replay(
            "window_live_ticks",
            callback=on_row,
            start=1777017600000001000,
            end=1777017600000002000,
        )

        assert isinstance(replay_engine, zippy.TableReplayEngine)
        assert replay_engine.status() == "completed"
        assert received == expected
    finally:
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_table_replay_init_allows_subscribe_before_run(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    replay_engine = None
    subscriber = None
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    received: list[dict[str, object]] = []
    replay_done = threading.Event()

    def on_row(row: zippy.Row) -> None:
        received.append(row.to_dict())
        if len(received) >= 2:
            replay_done.set()

    try:
        zippy.connect(uri=control_endpoint, app="table_replay_init_test")
        live_pipeline = (
            zippy.Pipeline("live_ingest_for_replay_init")
            .stream_table(
                "init_live_ticks",
                schema=schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606"],
                "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
                "last_price": [3898.0, 2675.0, 3898.5],
            }
        )

        query = zippy.read_table("init_live_ticks")
        for _ in range(50):
            persisted_files = query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 2:
                break
            time.sleep(0.02)
        expected = [
            {
                "instrument_id": "IF2606",
                "dt": 1777017600000000000,
                "last_price": 3898.0,
            },
            {
                "instrument_id": "IH2606",
                "dt": 1777017600000001000,
                "last_price": 2675.0,
            },
        ]

        replay_engine = zippy.TableReplayEngine(
            "init_live_ticks",
            output_stream="init_replay_ticks",
            batch_size=1,
            row_capacity=16,
        )
        replay_engine.init()
        assert replay_engine.status() == "initialized"

        subscriber = zippy.subscribe(
            "init_replay_ticks",
            callback=on_row,
            poll_interval_ms=1,
        )
        replay_engine.run()

        assert replay_done.wait(timeout=2.0)
        assert received == expected
    finally:
        if subscriber is not None:
            subscriber.stop()
        if replay_engine is not None:
            replay_engine.stop()
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_replay_output_stream_drives_reactive_latest_engine(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    replay_engine = None
    latest_session = None
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    try:
        zippy.connect(uri=control_endpoint, app="replay_downstream_engine_test")
        live_pipeline = (
            zippy.Pipeline("live_ingest_for_replay_downstream")
            .stream_table(
                "downstream_live_ticks",
                schema=tick_schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606", "IC2606"],
                "dt": [
                    1777017600000000000,
                    1777017600000001000,
                    1777017600000002000,
                    1777017600000003000,
                    1777017600000004000,
                ],
                "last_price": [3898.0, 2675.0, 3899.0, 2676.0, 6123.0],
            }
        )

        live_query = zippy.read_table("downstream_live_ticks")
        for _ in range(50):
            persisted_files = live_query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 4:
                break
            time.sleep(0.02)
        assert live_query._scan_persisted().to_table().num_rows == 4

        replay_engine = zippy.TableReplayEngine(
            "downstream_live_ticks",
            output_stream="replay_downstream_ticks",
            batch_size=1,
            row_capacity=16,
            persist=None,
        ).init()
        latest_session = (
            zippy.Session("replay_latest_session", master=zippy._DEFAULT_MASTER)
            .engine(
                zippy.ReactiveLatestEngine,
                name="replay_latest_by_instrument",
                source="replay_downstream_ticks",
                by="instrument_id",
            )
            .stream_table("replay_downstream_latest", persist=False)
            .run()
        )

        replay_engine.run()

        latest = zippy.read_table("replay_downstream_latest").tail(10)
        deadline = time.time() + 2.0
        while (
            latest.num_rows != 2 or latest.column("last_price").to_pylist() != [3899.0, 2676.0]
        ) and time.time() < deadline:
            time.sleep(0.02)
            latest = zippy.read_table("replay_downstream_latest").tail(10)

        assert latest.num_rows == 2
        assert latest.column("instrument_id").to_pylist() == ["IF2606", "IH2606"]
        assert latest.column("last_price").to_pylist() == [3899.0, 2676.0]
    finally:
        if latest_session is not None:
            latest_session.stop()
        if replay_engine is not None:
            replay_engine.stop()
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_parquet_replay_engine_replays_path_to_named_stream(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606"],
            "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
            "last_price": [3898.0, 2675.0, 3898.5],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(table, parquet_path)

    server, control_endpoint = start_master_server(tmp_path)
    replay_engine = None
    try:
        zippy.connect(uri=control_endpoint, app="parquet_replay_engine_test")
        replay_engine = zippy.ParquetReplayEngine(
            parquet_path,
            output_stream="parquet_replay_ticks",
            schema=schema,
            batch_size=2,
            row_capacity=16,
            persist=None,
        ).run()

        replayed = zippy.read_table("parquet_replay_ticks").collect()

        assert isinstance(replay_engine, zippy.ParquetReplayEngine)
        assert replay_engine.status() == "running"
        assert replay_engine.output_stream == "parquet_replay_ticks"
        assert replayed == table
    finally:
        if replay_engine is not None:
            replay_engine.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_parquet_replay_engine_callback_respects_replay_rate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeClock:
        def __init__(self) -> None:
            self.now = 100.0
            self.sleeps: list[float] = []

        def monotonic(self) -> float:
            return self.now

        def sleep(self, delay: float) -> None:
            self.sleeps.append(delay)
            self.now += delay

    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IC2606"],
            "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
            "last_price": [3898.0, 2675.0, 6123.0],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(table, parquet_path)
    fake_clock = FakeClock()
    callback_times: list[float] = []

    def on_row(row: zippy.Row) -> None:
        callback_times.append(fake_clock.monotonic())

    monkeypatch.setattr(zippy, "time", fake_clock, raising=False)

    replay_engine = zippy.ParquetReplayEngine(
        parquet_path,
        callback=on_row,
        schema=schema,
        batch_size=2,
        replay_rate=10,
    ).run()

    assert replay_engine.status() == "completed"
    assert callback_times == pytest.approx([100.0, 100.1, 100.2])
    assert fake_clock.sleeps == pytest.approx([0.1, 0.1])


def test_pipeline_stop_unregisters_registered_source(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class FakeMaster:
        def __init__(self) -> None:
            self.registered_sources: list[str] = []
            self.unregistered_sources: list[str] = []

        def process_id(self) -> str:
            return "proc_test"

        def register_process(self, app: str) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            self.registered_sources.append(source_name)

        def unregister_source(self, source_name: str) -> None:
            self.unregistered_sources.append(source_name)

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            return None

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    master = FakeMaster()
    source = zippy.ParquetReplaySource(
        tmp_path / "ticks.parquet",
        schema=schema,
        source_name="replay_source",
    )
    pipeline = (
        zippy.Pipeline("replay_pipeline", master=master)
        .source(source)
        .stream_table("replay_ticks", schema=schema, persist=None)
    )

    pipeline.stop()

    assert master.registered_sources == ["replay_source"]
    assert master.unregistered_sources == ["replay_source"]


def test_compare_replay_sorts_by_keys_and_reports_equal() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.int64()),
            ("last_price", pa.float64()),
        ]
    )
    left = pa.table(
        {
            "instrument_id": ["IH2606", "IF2606"],
            "dt": [2, 1],
            "last_price": [2675.0, 3898.0],
        },
        schema=schema,
    )
    right = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606"],
            "dt": [1, 2],
            "last_price": [3898.0, 2675.0],
        },
        schema=schema,
    )

    comparison = zippy.compare_replay(left, right, by=["instrument_id", "dt"])

    assert comparison == {
        "equal": True,
        "left_rows": 2,
        "right_rows": 2,
        "schema_equal": True,
        "missing_rows": 0,
        "extra_rows": 0,
        "mismatch_rows": 0,
    }


def test_parquet_replay_e2e_matches_persisted_table_snapshot(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    live_pipeline = None
    replay_engine = None
    try:
        zippy.connect(uri=control_endpoint, app="parquet_replay_e2e_test")
        tick_schema = pa.schema(
            [
                ("instrument_id", pa.string()),
                ("dt", pa.timestamp("ns", tz="UTC")),
                ("last_price", pa.float64()),
            ]
        )
        live_pipeline = (
            zippy.Pipeline("live_ingest")
            .stream_table(
                "live_ticks",
                schema=tick_schema,
                row_capacity=2,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606", "IC2606"],
                "dt": [
                    1777017600000000000,
                    1777017600000000001,
                    1777017600000000002,
                    1777017600000000003,
                    1777017600000000004,
                ],
                "last_price": [3898.0, 2675.0, 3898.5, 2675.5, 6123.0],
            }
        )

        live_query = zippy.read_table("live_ticks")
        for _ in range(50):
            persisted_files = live_query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 4:
                break
            time.sleep(0.02)

        persisted = live_query._scan_persisted().to_table()
        assert persisted.num_rows == 4

        replay_engine = zippy.replay(
            "live_ticks",
            output_stream="replay_ticks",
            batch_size=2,
            row_capacity=16,
        )

        replayed = zippy.read_table("replay_ticks").collect()
        for _ in range(50):
            if replayed.num_rows == persisted.num_rows:
                break
            time.sleep(0.02)
            replayed = zippy.read_table("replay_ticks").collect()

        comparison = zippy.compare_replay(
            persisted,
            replayed,
            by=["instrument_id", "dt"],
        )

        assert comparison["equal"] is True
        assert comparison["left_rows"] == 4
        assert comparison["right_rows"] == 4
    finally:
        if replay_engine is not None:
            replay_engine.stop()
        if live_pipeline is not None:
            live_pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_reactive_engine_accepts_columnar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": ["A", "A"], "price": [10.0, 11.0]})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_scalar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": "A", "price": 10.0})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_row_oriented_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        [
            {"symbol": "A", "price": 10.0},
            {"symbol": "A", "price": 11.0},
        ]
    )
    engine.flush()
    engine.stop()


def test_write_after_stop_raises_runtime_error() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()

    with pytest.raises(RuntimeError):
        engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))


def test_reactive_engine_exposes_output_schema_before_and_after_start() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_python_package_exposes_git_derived_version() -> None:
    commit_count = git_output("rev-list", "--count", "HEAD")
    short_sha = git_output("rev-parse", "--short", "HEAD")
    expected = f"0.1.0.dev{commit_count}+g{short_sha}"

    assert zippy.__version__ == expected
    assert zippy.version() == expected


def test_setup_log_returns_snapshot_and_log_path(tmp_path: Path) -> None:
    script = """
import json
import zippy

result = zippy.setup_log(
    app="py_test",
    level="info",
    log_dir=r\"\"\"%s\"\"\",
    to_console=False,
    to_file=True,
)
print(json.dumps(result))
""" % str(tmp_path)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    result = json.loads(completed.stdout.strip())

    assert result["app"] == "py_test"
    assert result["level"] == "info"
    assert result["run_id"]
    assert result["file_path"] is not None
    file_path = Path(result["file_path"])
    assert file_path.parent == tmp_path / "py_test"
    assert file_path.name.endswith(".jsonl")
    assert "_" in file_path.stem


def test_setup_log_file_contains_runtime_message(tmp_path: Path) -> None:
    script = """
import json
from pathlib import Path

import pyarrow as pa
import zippy

snapshot = zippy.setup_log(
    app="bridge_test",
    level="info",
    log_dir=r\"\"\"%s\"\"\",
    to_console=False,
    to_file=True,
)
engine = zippy._internal.StreamTableMaterializer(
    name="ticks",
    input_schema=pa.schema([("price", pa.float64())]),
    target=zippy.NullPublisher(),
)
engine.start()
engine.stop()
print(json.dumps(snapshot))
""" % str(tmp_path)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    snapshot = json.loads(completed.stdout.strip())
    file_path = Path(snapshot["file_path"])
    contents = file_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in contents if line.strip()]

    assert any(
        record.get("component") == "runtime"
        and record.get("event") == "start"
        and record.get("message")
        for record in records
    )
    assert any(
        record.get("component") == "python_bridge"
        and record.get("event") == "stop"
        and record.get("message") == "python runtime bridge stopped"
        for record in records
    )


def test_timeseries_engine_accepts_polars_and_exposes_output_schema() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            pa.field("window_start", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("window_end", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=60_000_000_000,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AggFirstSpec(column="price", output="open"),
            zippy.AggLastSpec(column="price", output="close"),
            zippy.AggSumSpec(column="volume", output="volume"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write(
        pl.DataFrame(
            {
                "symbol": ["A", "A"],
                "dt": [
                    "2026-04-02T09:30:00.000000000Z",
                    "2026-04-02T09:30:01.000000000Z",
                ],
                "price": [10.0, 11.0],
                "volume": [100.0, 120.0],
            }
        ).with_columns(pl.col("dt").str.to_datetime(time_unit="ns", time_zone="UTC"))
    )
    engine.flush()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_timeseries_engine_accepts_duration_window_api() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
            "volume": [100.0],
        }
    )
    engine.flush()
    engine.stop()


def test_timeseries_engine_rejects_string_policy_arguments() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="window_type"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type="sliding",
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy="reject",
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=pa.schema([("symbol", pa.string()), ("price", pa.float64())]),
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy="block",
        )


def test_engines_reject_wrong_policy_constant_categories() -> None:
    timeseries_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    reactive_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="window_type"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=timeseries_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.OverflowPolicy.BLOCK,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=timeseries_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.WindowType.TUMBLING,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=reactive_schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy=zippy.LateDataPolicy.REJECT,
        )


def test_reactive_engine_accepts_all_v1_operators_via_design_helpers() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
            pa.field("mean_3", pa.float64(), nullable=True),
            pa.field("std_3", pa.float64(), nullable=True),
            pa.field("delay_2", pa.float64(), nullable=True),
            pa.field("diff_2", pa.float64(), nullable=True),
            pa.field("ret_2", pa.float64(), nullable=True),
            pa.field("abs_price", pa.float64(), nullable=False),
            pa.field("log_price", pa.float64(), nullable=False),
            pa.field("clipped_price", pa.float64(), nullable=False),
            pa.field("price_i64", pa.int64(), nullable=False),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.TS_MEAN(column="price", window=3, output="mean_3"),
            zippy.TS_STD(column="price", window=3, output="std_3"),
            zippy.TS_DELAY(column="price", period=2, output="delay_2"),
            zippy.TS_DIFF(column="price", period=2, output="diff_2"),
            zippy.TS_RETURN(column="price", period=2, output="ret_2"),
            zippy.ABS(column="price", output="abs_price"),
            zippy.LOG(column="price", output="log_price"),
            zippy.CLIP(column="price", min=12.0, max=20.0, output="clipped_price"),
            zippy.CAST(column="price", dtype="int64", output="price_i64"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write({"symbol": ["A", "A", "A"], "price": [10.0, 16.0, 19.0]})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_expression_factor_helper() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
            pa.field("price_plus_ema", pa.float64(), nullable=True),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.Expr(expression="price + ema_2", output="price_plus_ema"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write({"symbol": ["A", "A"], "price": [10.0, 16.0]})
    engine.flush()

    received = subscriber.recv()

    assert received.column(2).to_pylist() == [10.0, 14.0]
    assert received.column(3).to_pylist() == [20.0, 30.0]

    engine.stop()
    subscriber.close()


def test_reactive_engine_expr_supports_ts_nodes() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("score", pa.float64(), nullable=True),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.Expr(
                expression="TS_DIFF(price, 2) / TS_STD(TS_DIFF(price, 2), 3)",
                output="score",
            ),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(
        {
            "symbol": ["A", "A", "A", "A", "A", "A"],
            "price": [10.0, 11.0, 13.0, 16.0, 20.0, 25.0],
        }
    )
    engine.flush()

    received = subscriber.recv()

    assert received.column(2).to_pylist() == [
        None,
        None,
        None,
        None,
        pytest.approx(4.286607049870562),
        pytest.approx(5.5113519212621505),
    ]

    engine.stop()
    subscriber.close()


def test_expression_factor_rejects_unknown_identifier() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unknown expression identifier"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="missing + 1.0", output="score")],
            target=zippy.NullPublisher(),
        )


def test_expression_factor_rejects_unsupported_function() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unsupported expression function"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="sqrt(price)", output="score")],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_accepts_all_v1_aggregation_operators_via_design_helpers() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            pa.field("window_start", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("window_end", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("count", pa.float64(), nullable=False),
            pa.field("vwap", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_MAX(column="price", output="high"),
            zippy.AGG_MIN(column="price", output="low"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_COUNT(column="price", output="count"),
            zippy.AGG_VWAP(price_column="price", volume_column="volume", output="vwap"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
            "volume": [100.0, 120.0],
        }
    )
    engine.flush()
    engine.stop()


def test_timeseries_engine_accepts_pre_and_post_expression_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            pa.field("window_start", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("window_end", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("turnover", pa.float64(), nullable=False),
            pa.field("ret_1m", pa.float64(), nullable=False),
            pa.field("vwap_1m", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        pre_factors=[
            zippy.Expr(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.Expr(expression="close / open - 1.0", output="ret_1m"),
            zippy.Expr(expression="turnover / volume", output="vwap_1m"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema


def test_timeseries_engine_rejects_non_expression_phase_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="pre_factors"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            pre_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(TypeError, match="post_factors"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            post_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_rejects_post_factors_referencing_raw_input_columns() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unknown expression identifier"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[
                zippy.AGG_FIRST(column="price", output="open"),
                zippy.AGG_LAST(column="price", output="close"),
            ],
            post_factors=[
                zippy.Expr(expression="price * 2.0", output="bad"),
            ],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_pre_factors_reject_ts_expr_nodes() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(
        ValueError,
        match="stateful TS_\\* functions are only supported inside ReactiveStateEngine",
    ):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            pre_factors=[
                zippy.Expr(expression="TS_DIFF(price, 2)", output="bad"),
            ],
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_post_factors_reject_ts_expr_nodes() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(
        ValueError,
        match="stateful TS_\\* functions are only supported inside ReactiveStateEngine",
    ):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            post_factors=[
                zippy.Expr(expression="TS_STD(sum_price, 2)", output="bad"),
            ],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_drop_with_metric_filters_late_rows_before_pre_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
        pre_factors=[
            zippy.Expr(expression="LOG(price)", output="log_price"),
        ],
        factors=[zippy.AGG_SUM(column="log_price", output="sum_log_price")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [0.0],
        }
    )
    engine.flush()

    assert engine.metrics()["late_rows_total"] == 1

    engine.stop()


def test_timeseries_engine_flush_runs_post_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        pre_factors=[
            zippy.Expr(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.Expr(expression="close / open - 1.0", output="ret_1m"),
            zippy.Expr(expression="turnover / volume", output="vwap_1m"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
            "volume": [100.0, 120.0],
        }
    )
    engine.flush()

    received = subscriber.recv()

    assert received.column_names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
        "close",
        "volume",
        "turnover",
        "ret_1m",
        "vwap_1m",
    ]
    assert received.column(6).to_pylist() == [2320.0]
    assert received.column(7).to_pylist() == pytest.approx([0.1])
    assert received.column(8).to_pylist() == pytest.approx([2320.0 / 220.0])

    engine.stop()
    subscriber.close()


def test_timeseries_engine_accepts_reactive_source_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()
    bars.start()
    reactive.write(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )
    reactive.flush()
    bars.flush()
    reactive.stop()
    bars.stop()


def test_timeseries_engine_rejects_source_schema_mismatch_at_construction() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    mismatched_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("extra", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    with pytest.raises(ValueError, match="source output schema"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            source=reactive,
            input_schema=mismatched_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )


def test_source_write_requires_downstream_started() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()

    with pytest.raises(RuntimeError, match="downstream engine must be started"):
        reactive.write(
            {
                "symbol": ["A"],
                "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
                "price": [10.0],
            }
        )

    reactive.stop()
    bars.start()
    bars.stop()


def test_downstream_stop_requires_source_stopped() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()
    bars.start()

    with pytest.raises(RuntimeError, match="source engine must be stopped"):
        bars.stop()

    reactive.stop()
    bars.stop()


def test_reactive_engine_accepts_target_list() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=[zippy.NullPublisher(), zippy.NullPublisher()],
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()


def test_reactive_engine_accepts_zmq_target() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.ZmqPublisher(endpoint="tcp://127.0.0.1:*"),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()


def test_zmq_subscriber_receives_record_batch_from_engine() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.flush()

    received = subscriber.recv()

    assert isinstance(received, pa.RecordBatch)
    assert received.schema == pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
        ]
    )
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [10.0]
    assert received.column(2).to_pylist() == [10.0]

    engine.stop()
    subscriber.close()


def test_reactive_engine_accepts_parquet_sink_and_archives_input_and_output(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="1h",
            write_input=True,
            write_output=True,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))
    engine.stop()

    parquet_files = list(tmp_path.rglob("*.parquet"))
    input_files = [path for path in parquet_files if "input" in path.parts]
    output_files = [path for path in parquet_files if "output" in path.parts]

    assert input_files
    assert output_files
    assert pq.read_schema(input_files[0]).names == ["symbol", "price"]
    assert pq.read_schema(output_files[0]).names == ["symbol", "price", "ema_2"]
    assert len(input_files) == 1
    assert len(output_files) == 1
    assert pq.read_table(input_files[0]).num_rows == 2
    assert pq.read_table(output_files[0]).num_rows == 2


def test_parquet_sink_flush_interval_flushes_small_batches(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=False,
            rows_per_batch=10_000,
            flush_interval_ms=50,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    time.sleep(0.12)
    engine.stop()

    input_files = list((tmp_path / "input").rglob("*.parquet"))

    assert len(input_files) == 1
    assert pq.read_table(input_files[0]).num_rows == 1


def test_source_pipeline_archives_downstream_input_via_parquet_sink(
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=False,
            rows_per_batch=4,
            flush_interval_ms=250,
        ),
    )

    reactive.start()
    bars.start()
    reactive.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    reactive.stop()
    bars.stop()

    input_files = list((tmp_path / "input").rglob("*.parquet"))

    assert input_files
    assert pq.read_schema(input_files[0]).names == ["symbol", "dt", "price", "ema_2"]


def test_engine_rejects_invalid_runtime_config_keywords(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="buffer_capacity"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            buffer_capacity=0,
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy="block",
        )

    with pytest.raises(ValueError, match="archive_buffer_capacity"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            parquet_sink=zippy.ParquetSink(
                path=str(tmp_path),
                rotation="none",
                write_input=True,
                write_output=False,
            ),
            archive_buffer_capacity=0,
        )


def test_reactive_engine_exposes_status_metrics_and_config_lifecycle(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=False,
            rows_per_batch=4,
            flush_interval_ms=250,
        ),
        buffer_capacity=32,
        overflow_policy=zippy.OverflowPolicy.REJECT,
        archive_buffer_capacity=8,
    )

    assert engine.status() == "created"
    assert engine.metrics() == {
        "processed_batches_total": 0,
        "processed_rows_total": 0,
        "output_batches_total": 0,
        "dropped_batches_total": 0,
        "late_rows_total": 0,
        "filtered_rows_total": 0,
        "publish_errors_total": 0,
        "queue_depth": 0,
    }

    config = engine.config()
    assert config["engine_type"] == "reactive"
    assert config["buffer_capacity"] == 32
    assert config["overflow_policy"] == "reject"
    assert config["archive_buffer_capacity"] == 8
    assert config["targets"] == [{"type": "null"}]
    assert config["source_linked"] is False
    assert config["parquet_sink"] == {
        "path": str(tmp_path),
        "rotation": "none",
        "write_input": True,
        "write_output": False,
        "rows_per_batch": 4,
        "flush_interval_ms": 250,
    }

    engine.start()
    assert engine.status() == "running"

    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.flush()

    metrics = engine.metrics()
    assert metrics["processed_batches_total"] == 1
    assert metrics["processed_rows_total"] == 1
    assert metrics["output_batches_total"] == 1
    assert metrics["filtered_rows_total"] == 0
    assert metrics["queue_depth"] == 0

    engine.stop()
    assert engine.status() == "stopped"
    assert engine.metrics()["processed_batches_total"] == 1


def test_reactive_engine_config_exposes_runtime_xfast(tmp_path: Path) -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
        target=zippy.NullPublisher(),
        xfast=True,
    )

    assert engine.config()["xfast"] is True


def test_reactive_engine_id_filter_filters_rows_and_updates_config_and_metrics() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_filter",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    config = engine.config()
    assert config["id_filter"] == ["A"]

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write({"symbol": ["A", "B"], "price": [10.0, 99.0]})
    received = subscriber.recv()

    assert received.column_names == ["symbol", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [10.0]
    assert received.column(2).to_pylist() == [20.0]

    engine.stop()
    metrics = engine.metrics()
    assert metrics["filtered_rows_total"] == 1
    assert metrics["late_rows_total"] == 0
    subscriber.close()


def test_reactive_engine_rejects_empty_id_filter() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.ReactiveStateEngine(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            target=zippy.NullPublisher(),
            id_filter=[],
        )


def test_reactive_engine_rejects_non_string_id_filter_elements() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must be a sequence of strings"):
        zippy.ReactiveStateEngine(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            target=zippy.NullPublisher(),
            id_filter=["A", 1],
        )


def test_timeseries_engine_config_and_output_archive_roundtrip(
    tmp_path: Path,
) -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
        buffer_capacity=17,
        overflow_policy=zippy.OverflowPolicy.DROP_OLDEST,
        archive_buffer_capacity=9,
    )

    config = engine.config()
    assert config["engine_type"] == "timeseries"
    assert config["window_ns"] == 60_000_000_000
    assert config["late_data_policy"] == "reject"
    assert config["buffer_capacity"] == 17
    assert config["overflow_policy"] == "drop_oldest"
    assert config["archive_buffer_capacity"] == 9
    assert config["parquet_sink"]["write_output"] is True
    assert config["parquet_sink"]["rows_per_batch"] == 8192
    assert config["parquet_sink"]["flush_interval_ms"] == 1000

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.stop()

    output_files = list((tmp_path / "output").rglob("*.parquet"))

    assert output_files
    assert pq.read_schema(output_files[0]).names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
    ]


def test_stream_table_engine_runtime_xfast_defaults_false() -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    engine = zippy._internal.StreamTableMaterializer(
        name="ticks",
        input_schema=schema,
        target=zippy.NullPublisher(),
    )
    assert engine.config()["xfast"] is False


def test_timeseries_engine_config_exposes_runtime_xfast() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
        xfast=True,
    )

    assert engine.config()["xfast"] is True


def test_timeseries_engine_id_filter_filters_rows_and_updates_config_and_metrics() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_filter",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    config = engine.config()
    assert config["id_filter"] == ["A"]

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )
    engine.flush()
    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [10.0]

    engine.stop()
    metrics = engine.metrics()
    assert metrics["filtered_rows_total"] == 1
    assert metrics["late_rows_total"] == 0
    subscriber.close()


def test_timeseries_engine_rejects_empty_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.TimeSeriesEngine(
            name="bar_filter",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="price", output="close")],
            target=zippy.NullPublisher(),
            id_filter=[],
        )


def test_timeseries_engine_rejects_non_string_id_filter_elements() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must be a sequence of strings"):
        zippy.TimeSeriesEngine(
            name="bar_filter",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="price", output="close")],
            target=zippy.NullPublisher(),
            id_filter=["A", 1],
        )


def test_timeseries_engine_metrics_report_late_rows() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)],
            "price": [11.0],
        }
    )
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.flush()

    assert engine.status() == "running"
    assert engine.metrics()["late_rows_total"] == 1

    engine.stop()
    assert engine.status() == "stopped"


def test_parquet_sink_failure_marks_engine_failed_status(tmp_path: Path) -> None:
    blocked_root = tmp_path / "blocked"
    blocked_root.write_text("not-a-directory", encoding="utf-8")
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(blocked_root),
            rotation="none",
            write_input=True,
            write_output=False,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))

    with pytest.raises(RuntimeError, match="failed to create parquet directory"):
        engine.stop()

    assert engine.status() == "failed"


def test_stream_table_engine_exposes_input_schema_as_output_schema() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy._internal.StreamTableMaterializer(
        name="ticks",
        input_schema=schema,
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == schema


def test_stream_table_engine_supports_source_target_and_sink(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:9999",
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    engine = zippy._internal.StreamTableMaterializer(
        name="ticks",
        input_schema=schema,
        source=source,
        target=zippy.NullPublisher(),
        sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=True,
            rows_per_batch=16,
            flush_interval_ms=500,
        ),
    )

    config = engine.config()

    assert config["engine_type"] == "stream_table"
    assert config["source_linked"] is True
    assert config["has_sink"] is True
    assert config["sink"] == {
        "path": str(tmp_path),
        "rotation": "none",
        "write_input": True,
        "write_output": True,
        "rows_per_batch": 16,
        "flush_interval_ms": 500,
    }
    assert "parquet_sink" not in config


def test_stream_table_engine_rejects_invalid_sink_type() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="sink must be zippy.ParquetSink"):
        zippy._internal.StreamTableMaterializer(
            name="ticks",
            input_schema=schema,
            target=zippy.NullPublisher(),
            sink=zippy.NullPublisher(),
        )


def test_stream_table_engine_passthrough_archives_output_and_publishes_remote_stream(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    first_dt = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    second_dt = datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc)
    relay_port = reserve_tcp_port()
    relay_endpoint = f"tcp://127.0.0.1:{relay_port}"

    stream_target = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="tick_table",
        schema=schema,
    )
    relay_source = zippy.ZmqSource(
        endpoint=stream_target.last_endpoint(),
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    relay = zippy._internal.StreamTableMaterializer(
        name="tick_table_relay",
        source=relay_source,
        input_schema=schema,
        target=zippy.ZmqPublisher(endpoint=relay_endpoint),
    )
    engine = zippy._internal.StreamTableMaterializer(
        name="tick_table",
        input_schema=schema,
        target=stream_target,
        sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
    )

    relay.start()
    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=relay_endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write(
        pl.DataFrame(
            {
                "symbol": ["IF2606", "IH2606"],
                "dt": [first_dt, second_dt],
                "price": [3898.2, 2675.4],
                "volume": [12.0, 8.0],
            }
        )
    )
    engine.flush()

    received = subscriber.recv()

    assert received.schema == schema
    assert received.column(0).to_pylist() == ["IF2606", "IH2606"]
    assert received.column(1).to_pylist() == [first_dt, second_dt]
    assert received.column(2).to_pylist() == [3898.2, 2675.4]
    assert received.column(3).to_pylist() == [12.0, 8.0]

    engine.stop()
    relay.stop()
    subscriber.close()

    parquet_files = list(tmp_path.rglob("*.parquet"))
    output_files = [path for path in parquet_files if "output" in path.parts]

    assert output_files

    archived = pq.read_table(output_files[0])
    assert archived.schema == schema
    assert archived.column("symbol").to_pylist() == ["IF2606", "IH2606"]
    assert archived.column("dt").to_pylist() == [first_dt, second_dt]
    assert archived.column("price").to_pylist() == [3898.2, 2675.4]
    assert archived.column("volume").to_pylist() == [12.0, 8.0]


def test_stream_table_engine_can_drive_timeseries_downstream_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    source = zippy._internal.StreamTableMaterializer(
        name="tick_table",
        input_schema=tick_schema,
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=source,
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    bucket_end = datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)

    bars.start()
    source.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    source.write(
        {
            "symbol": ["A", "A"],
            "dt": [bucket_start, datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc)],
            "price": [10.0, 11.0],
            "volume": [100.0, 120.0],
        }
    )
    source.flush()
    bars.flush()

    received = subscriber.recv()

    assert received.column_names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
        "close",
        "volume",
    ]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [bucket_start]
    assert received.column(2).to_pylist() == [bucket_end]
    assert received.column(3).to_pylist() == [10.0]
    assert received.column(4).to_pylist() == [11.0]
    assert received.column(5).to_pylist() == [220.0]

    source.stop()
    bars.stop()
    subscriber.close()


def test_stream_table_engine_can_publish_to_master_bus(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()

    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")
    writer_master.register_stream("ticks", schema, 64, 4096)

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reader")

    engine = zippy._internal.StreamTableMaterializer(
        name="ticks",
        input_schema=schema,
        target=zippy.BusStreamTarget(stream_name="ticks", master=writer_master),
    )
    reader = reader_master.read_from("ticks")

    try:
        engine.start()
        engine.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606", "IH2606"],
                    "last_price": [3898.2, 2675.4],
                }
            )
        )
        received = reader.read(1_000)

        assert received.schema == schema
        assert received.column("instrument_id").to_pylist() == ["IF2606", "IH2606"]
        assert received.column("last_price").to_pylist() == [3898.2, 2675.4]
    finally:
        reader.close()
        engine.stop()
        server.stop()
        server.join()


def test_master_server_start_raises_when_socket_is_already_active(tmp_path: Path) -> None:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    first_server = zippy.MasterServer(control_endpoint=control_endpoint)
    second_server = zippy.MasterServer(control_endpoint=control_endpoint)
    first_server.start()

    try:
        with pytest.raises(RuntimeError, match="control endpoint socket is already active"):
            second_server.start()
    finally:
        first_server.stop()
        first_server.join()


def test_master_server_start_waits_for_configured_ready_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    monkeypatch.setenv("ZIPPY_MASTER_TEST_PAUSE_BEFORE_READY_MS", "1200")

    try:
        server.start(3.0)
        assert Path(control_endpoint).exists()
    finally:
        server.stop()
        server.join()


def test_master_server_start_expands_user_path_and_creates_parent_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_endpoint = "~/.zippy/master.sock"
    expected_socket_path = tmp_path / ".zippy" / "master.sock"
    monkeypatch.setenv("HOME", str(tmp_path))
    server = zippy.MasterServer(control_endpoint=control_endpoint)

    try:
        server.start()
        assert expected_socket_path.exists()
    finally:
        server.stop()
        server.join()


def test_run_master_daemon_expands_user_path_and_creates_parent_dir(tmp_path: Path) -> None:
    socket_path = tmp_path / ".zippy" / "master.sock"
    script = "import zippy; zippy.run_master_daemon('~/.zippy/master.sock')"
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if socket_path.exists():
                break
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(
                    "run_master_daemon exited before creating expanded socket "
                    f"stdout=[{stdout}] stderr=[{stderr}]"
                )
            time.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=5)
            raise AssertionError(
                "run_master_daemon did not create expanded socket before timeout "
                f"stdout=[{stdout}] stderr=[{stderr}]"
            )

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stdout + stderr
        assert socket_path.parent.is_dir()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_reactive_engine_can_consume_master_bus_stream(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()

    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")
    writer_master.register_stream("ticks", schema, 64, 4096)
    writer = writer_master.write_to("ticks")

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reactive_reader")

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    engine = zippy.ReactiveStateEngine(
        name="reactive_bus",
        source=zippy.BusStreamSource(
            stream_name="ticks",
            expected_schema=schema,
            master=reader_master,
            mode=zippy.SourceMode.PIPELINE,
        ),
        input_schema=schema,
        id_column="instrument_id",
        factors=[
            zippy.Expr(expression="last_price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    try:
        engine.start()
        time.sleep(0.3)
        writer.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606"],
                    "last_price": [10.0],
                }
            )
        )

        deadline = time.time() + 2.0
        while True:
            try:
                received = subscriber.recv()
                break
            except RuntimeError as error:
                if "Resource temporarily unavailable" not in str(error) or time.time() >= deadline:
                    raise
                time.sleep(0.05)
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("last_price").to_pylist() == [10.0]
        assert received.column("price_x2").to_pylist() == [20.0]
    finally:
        writer.close()
        engine.stop()
        subscriber.close()
        server.stop()
        server.join()


def test_cross_sectional_engine_accepts_duration_trigger_interval_and_exposes_output_schema() -> (
    None
):
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            pa.field("ret_rank", pa.float64(), nullable=True),
            pa.field("ret_z", pa.float64(), nullable=True),
            pa.field("ret_dm", pa.float64(), nullable=True),
        ]
    )

    rank = zippy.CS_RANK(column="ret_1m", output="ret_rank")
    zscore = zippy.CS_ZSCORE(column="ret_1m", output="ret_z")
    demean = zippy.CS_DEMEAN(column="ret_1m", output="ret_dm")

    assert isinstance(rank, zippy.CSRankSpec)
    assert isinstance(zscore, zippy.CSZscoreSpec)
    assert isinstance(demean, zippy.CSDemeanSpec)

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[rank, zscore, demean],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_cross_sectional_engine_emits_bucketed_output_over_zmq() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.CS_RANK(column="ret_1m", output="ret_rank"),
            zippy.CS_ZSCORE(column="ret_1m", output="ret_z"),
            zippy.CS_DEMEAN(column="ret_1m", output="ret_dm"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(
        {
            "symbol": ["B", "A", "C"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 2, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 3, tzinfo=timezone.utc),
            ],
            "ret_1m": [2.0, 1.0, 3.0],
        }
    )
    engine.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "ret_rank", "ret_z", "ret_dm"]
    assert received.column(0).to_pylist() == ["A", "B", "C"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0, 3.0])
    assert received.column(3).to_pylist() == pytest.approx(
        [-1.224744871391589, 0.0, 1.224744871391589]
    )
    assert received.column(4).to_pylist() == pytest.approx([-1.0, 0.0, 1.0])

    engine.stop()
    subscriber.close()


def test_cross_sectional_engine_config_exposes_runtime_xfast() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.NullPublisher(),
        xfast=True,
    )

    assert engine.config()["xfast"] is True


def test_cross_sectional_engine_rejects_unsupported_late_data_policy() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
            factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_rejects_non_cross_sectional_factor_specs() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="CSRankSpec|CSZscoreSpec|CSDemeanSpec"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="ret_1m", output="bad")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_rejects_reactive_source() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    with pytest.raises(TypeError, match="source must be a named segment stream"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            source=reactive,
            input_schema=reactive.output_schema(),
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.CS_RANK(column="price", output="price_rank")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_accepts_timeseries_source_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    cs = zippy.CrossSectionalEngine(
        name="cs_1m",
        source=bars,
        input_schema=bars.output_schema(),
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="open", output="open_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    reactive.start()
    bars.start()
    cs.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    reactive.write(
        {
            "symbol": ["A", "A", "B", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0, 20.0, 21.0],
        }
    )
    reactive.flush()
    bars.flush()
    cs.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "open_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    reactive.stop()
    bars.stop()
    cs.stop()
    subscriber.close()


def test_source_mode_constants_and_zmq_source_are_exposed() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    assert repr(zippy.SourceMode.PIPELINE) == "SourceMode.PIPELINE"
    assert repr(zippy.SourceMode.CONSUMER) == "SourceMode.CONSUMER"

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:7101",
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )

    assert isinstance(source, zippy.ZmqSource)


def test_zmq_source_pipeline_can_drive_cross_sectional_engine() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="bars",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.CrossSectionalEngine(
        name="cs_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "window_start": [bucket_start, bucket_start],
            "ret_1m": [0.1, 0.2],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "ret_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_timeseries_engine_accepts_zmq_stream_target_for_remote_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    schema_probe = zippy.TimeSeriesEngine(
        name="bars_probe",
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.NullPublisher(),
    )
    bar_schema = schema_probe.output_schema()

    stream_target = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="bars",
        schema=bar_schema,
    )
    source = zippy.ZmqSource(
        endpoint=stream_target.last_endpoint(),
        expected_schema=bar_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    upstream = zippy.TimeSeriesEngine(
        name="bars_upstream",
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=stream_target,
    )
    downstream = zippy.CrossSectionalEngine(
        name="cs_remote",
        source=source,
        input_schema=bar_schema,
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="close", output="close_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    downstream.start()
    upstream.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    upstream.write(
        {
            "symbol": ["A", "B"],
            "dt": [bucket_start, bucket_start],
            "price": [10.0, 20.0],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "close_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    upstream.stop()
    downstream.stop()
    subscriber.close()


def test_timeseries_engine_accepts_named_segment_source(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint=control_endpoint)
    master.register_process("timeseries_named_source_test")
    master.register_stream("ticks", input_schema, 64, 4096)

    try:
        engine = zippy.TimeSeriesEngine(
            name="bars",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="price", output="close")],
            source="ticks",
            master=master,
            target=zippy.NullPublisher(),
        )

        assert engine.config()["source_linked"] is True
        assert engine.output_schema().names == ["symbol", "window_start", "window_end", "close"]
    finally:
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_timeseries_engine_filters_remote_zmq_source_rows_by_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_remote_filter",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [10.0]

    engine.stop()
    assert engine.metrics()["filtered_rows_total"] == 1
    upstream.stop()
    subscriber.close()


def test_zmq_source_consumer_mode_ignores_upstream_flush_until_local_flush() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.CONSUMER,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=200)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )
    upstream.flush()

    with pytest.raises(RuntimeError):
        subscriber.recv()

    engine.flush()
    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [11.0]

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_timeseries_engine_rejects_remote_source_schema_mismatch() -> None:
    source_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("close", pa.float64()),
        ]
    )

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:7102",
        expected_schema=source_schema,
        mode=zippy.SourceMode.PIPELINE,
    )

    with pytest.raises(ValueError, match="source output schema must match downstream input_schema"):
        zippy.TimeSeriesEngine(
            name="bar_remote",
            source=source,
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="close", output="last_close")],
            target=zippy.NullPublisher(),
        )


def test_reactive_engine_processes_remote_zmq_source() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.Expr(expression="price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A", "A"]
    assert received.column(2).to_pylist() == [10.0, 11.0]
    assert received.column(3).to_pylist() == [20.0, 22.0]

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_reactive_engine_filters_remote_zmq_source_rows_by_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_remote_filter",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        id_filter=["A"],
        factors=[
            zippy.Expr(expression="price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(2).to_pylist() == [10.0]
    assert received.column(3).to_pylist() == [20.0]

    engine.stop()
    assert engine.metrics()["filtered_rows_total"] == 1
    upstream.stop()
    subscriber.close()


def test_cross_sectional_engine_accepts_named_segment_source(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint=control_endpoint)
    master.register_process("cross_sectional_named_source_test")
    master.register_stream("minute_returns", input_schema, 64, 4096)

    try:
        engine = zippy.CrossSectionalEngine(
            name="cs_named",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="window_start",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
            source="minute_returns",
            master=master,
            target=zippy.NullPublisher(),
        )

        assert engine.config()["source_linked"] is True
        assert engine.output_schema().names == ["symbol", "window_start", "ret_rank"]
    finally:
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_cross_sectional_engine_archives_output_parquet(tmp_path: Path) -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["B", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 2, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "ret_1m": [2.0, 1.0],
        }
    )
    engine.stop()

    output_files = list((tmp_path / "output").rglob("*.parquet"))

    assert output_files
    output_table = pq.read_table(output_files[0])
    assert output_table.column_names == ["symbol", "dt", "ret_rank"]
    assert output_table.column("symbol").to_pylist() == ["A", "B"]
    assert output_table.column("dt").to_pylist() == [bucket_start, bucket_start]
    assert output_table.column("ret_rank").to_pylist() == pytest.approx([1.0, 2.0])


def test_cross_sectional_engine_start_can_retry_after_publisher_failure() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    blocker = zippy.ReactiveStateEngine(
        name="blocker",
        input_schema=pa.schema([("symbol", pa.string()), ("price", pa.float64())]),
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )
    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    blocker.start()

    with pytest.raises(RuntimeError):
        engine.start()

    blocker.stop()
    engine.start()
    engine.stop()


def start_master_server(tmp_path: Path) -> tuple[zippy.MasterServer, str]:
    control_endpoint = (
        unused_loopback_uri() if os.name == "nt" else str(tmp_path / "zippy-master.sock")
    )
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()
    return server, control_endpoint


def unused_loopback_uri() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        _, port = listener.getsockname()
    return f"tcp://127.0.0.1:{port}"


def expire_process_for_test(control_endpoint: str, process_id: str) -> dict[str, object]:
    request = json.dumps({"ExpireProcessForTest": {"process_id": process_id}}) + "\n"
    if control_endpoint.startswith("tcp://"):
        host, port_text = control_endpoint.removeprefix("tcp://").rsplit(":", 1)
        address = (host, int(port_text))
        family = socket.AF_INET
    else:
        address = control_endpoint
        family = socket.AF_UNIX
    with socket.socket(family, socket.SOCK_STREAM) as client:
        client.connect(address)
        client.sendall(request.encode("utf-8"))
        response = client.makefile("r", encoding="utf-8").readline()
    if not response:
        pytest.skip("ExpireProcessForTest is only available in debug master builds")
    return json.loads(response)


def test_master_client_roundtrips_batches_through_master_bus_direct_reader(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_client.register_process("writer")
    writer_client.register_stream("ticks", tick_schema, 64, 4096)
    writer = writer_client.write_to("ticks")

    reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_client.register_process("reader")
    reader = reader_client.read_from("ticks", xfast=True)

    writer.write({"instrument_id": ["IF2606"], "last_price": [4102.5]})
    batch = reader.read(timeout_ms=1000)

    assert batch.schema == tick_schema
    assert batch.column("instrument_id").to_pylist() == ["IF2606"]
    assert batch.column("last_price").to_pylist() == [4102.5]

    reader.close()
    writer.close()
    server.stop()


def test_master_bus_writer_close_releases_stream_for_restart(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    first_client = zippy.MasterClient(control_endpoint=control_endpoint)
    first_client.register_process("writer_1")
    first_client.register_stream("ticks", tick_schema, 64, 4096)
    first_writer = first_client.write_to("ticks")
    first_writer.close()

    second_client = zippy.MasterClient(control_endpoint=control_endpoint)
    second_client.register_process("writer_2")
    second_client.register_stream("ticks", tick_schema, 64, 4096)
    second_writer = second_client.write_to("ticks")

    second_writer.write({"instrument_id": ["IF2606"], "last_price": [4102.5]})
    second_writer.close()
    server.stop()


def test_stream_table_engine_can_publish_to_master_bus_direct_reader(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_client.register_process("stream_table_writer")
    writer_client.register_stream("openctp_ticks", tick_schema, 64, 4096)

    reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_client.register_process("stream_table_reader")
    reader = reader_client.read_from("openctp_ticks")

    engine = zippy._internal.StreamTableMaterializer(
        name="stream_table_writer",
        input_schema=tick_schema,
        target=zippy.BusStreamTarget(stream_name="openctp_ticks", master=writer_client),
    )

    try:
        engine.start()
        engine.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606"],
                    "dt": [datetime(2026, 4, 10, 9, 30, 0, tzinfo=timezone.utc)],
                    "last_price": [4102.5],
                }
            )
        )
        received = reader.read(timeout_ms=1000)

        assert received.schema == tick_schema
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("last_price").to_pylist() == [4102.5]
    finally:
        reader.close()
        engine.stop()
        server.stop()


def test_master_client_lists_streams(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)

    streams = client.list_streams()

    assert len(streams) == 1
    assert streams[0]["stream_name"] == "openctp_ticks"
    assert streams[0]["schema"]["fields"][0]["name"] == "instrument_id"
    assert streams[0]["schema"]["fields"][0]["segment_type"] == "utf8"
    assert streams[0]["schema"]["fields"][0]["nullable"] is True
    assert isinstance(streams[0]["schema_hash"], str)
    assert streams[0]["data_path"] == "segment"
    assert streams[0]["descriptor_generation"] == 0
    assert streams[0]["active_segment_descriptor"] is None
    assert streams[0]["sealed_segments"] == []
    assert streams[0]["persisted_files"] == []
    assert streams[0]["persist_events"] == []
    assert streams[0]["segment_reader_leases"] == []
    assert streams[0]["buffer_size"] == 64
    assert streams[0]["frame_size"] == 4096
    assert "ring_capacity" not in streams[0]
    assert streams[0]["writer_process_id"] is None
    assert streams[0]["reader_count"] == 0
    assert streams[0]["status"] == "registered"

    server.stop()


def test_master_client_gets_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)

    stream = client.get_stream("openctp_ticks")

    assert stream["stream_name"] == "openctp_ticks"
    assert stream["schema"]["fields"][0]["name"] == "instrument_id"
    assert stream["schema"]["fields"][0]["segment_type"] == "utf8"
    assert stream["schema"]["fields"][0]["nullable"] is True
    assert isinstance(stream["schema_hash"], str)
    assert stream["data_path"] == "segment"
    assert stream["descriptor_generation"] == 0
    assert stream["active_segment_descriptor"] is None
    assert stream["sealed_segments"] == []
    assert stream["persisted_files"] == []
    assert stream["persist_events"] == []
    assert stream["segment_reader_leases"] == []
    assert stream["buffer_size"] == 64
    assert stream["frame_size"] == 4096
    assert "ring_capacity" not in stream
    assert stream["writer_process_id"] is None
    assert stream["reader_count"] == 0
    assert stream["status"] == "registered"

    server.stop()


def test_top_level_table_observability_uses_default_master(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    try:
        client = zippy.connect(uri=control_endpoint, app="table_observability_test")
        client.register_stream("openctp_ticks", tick_schema, 64, 4096)

        tables = zippy.ops.list_tables()
        table = zippy.ops.table_info("openctp_ticks")

        assert [item["stream_name"] for item in tables] == ["openctp_ticks"]
        assert table["stream_name"] == "openctp_ticks"
        assert table["schema"]["fields"][0]["name"] == "instrument_id"
        assert table["status"] == "registered"
        assert table["descriptor_generation"] == 0
        assert table["persisted_files"] == []
    finally:
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_low_frequency_table_ops_are_not_top_level_exports() -> None:
    for name in (
        "list_tables",
        "table_info",
        "table_alerts",
        "table_health",
        "drop_table",
        "compact_table",
        "compact_tables",
        "start_compaction_worker",
    ):
        assert name not in zippy.__all__
        assert not hasattr(zippy, name)


def test_ops_namespace_exposes_table_observability_methods() -> None:
    class FakeMaster:
        def __init__(self) -> None:
            self.dropped: list[tuple[str, bool]] = []

        def list_streams(self) -> list[dict[str, object]]:
            return [
                {
                    "stream_name": "ctp_ticks",
                    "status": "writer_attached",
                    "descriptor_generation": 3,
                    "active_segment_descriptor": {"segment_id": 7, "generation": 3},
                    "persist_events": [],
                }
            ]

        def get_stream(self, table_name: str) -> dict[str, object]:
            assert table_name == "ctp_ticks"
            return self.list_streams()[0]

        def drop_table(
            self,
            table_name: str,
            drop_persisted: bool = True,
        ) -> dict[str, object]:
            self.dropped.append((table_name, drop_persisted))
            return {"table_name": table_name, "dropped": True}

    master = FakeMaster()

    assert zippy.ops.list_tables(master=master)[0]["stream_name"] == "ctp_ticks"
    assert zippy.ops.table_info("ctp_ticks", master=master)["stream_name"] == "ctp_ticks"
    assert zippy.ops.table_alerts("ctp_ticks", master=master) == []
    assert zippy.ops.table_health("ctp_ticks", master=master)["status"] == "ok"
    assert zippy.ops.drop_table("ctp_ticks", drop_persisted=False, master=master) == {
        "table_name": "ctp_ticks",
        "dropped": True,
    }
    assert master.dropped == [("ctp_ticks", False)]


def test_ops_compact_tables_discovers_persisted_tables(monkeypatch) -> None:
    class FakeMaster:
        def list_streams(self) -> list[dict[str, object]]:
            return [
                {"stream_name": "b_ticks", "persisted_files": [{"file_path": "b.parquet"}]},
                {"stream_name": "a_ticks", "persisted_files": [{"file_path": "a.parquet"}]},
                {"stream_name": "live_only_ticks", "persisted_files": []},
            ]

    calls: list[tuple[str, int, bool, object]] = []

    def fake_compact_table(
        table_name: str,
        *,
        min_files: int,
        delete_sources: bool,
        master: object,
    ) -> dict[str, object]:
        calls.append((table_name, min_files, delete_sources, master))
        return {
            "table_name": table_name,
            "groups_compacted": 1 if table_name == "a_ticks" else 0,
            "files_compacted": 2 if table_name == "a_ticks" else 0,
            "rows_compacted": 10 if table_name == "a_ticks" else 0,
        }

    master = FakeMaster()
    monkeypatch.setattr(zippy, "_compact_table", fake_compact_table)

    result = zippy.ops.compact_tables(
        master=master,
        min_files=3,
        delete_sources=False,
    )

    assert [call[0] for call in calls] == ["a_ticks", "b_ticks"]
    assert all(call[1] == 3 for call in calls)
    assert all(call[2] is False for call in calls)
    assert all(call[3] is master for call in calls)
    assert result["tables_scanned"] == 2
    assert result["tables_compacted"] == 1
    assert result["groups_compacted"] == 1
    assert result["files_compacted"] == 2
    assert result["rows_compacted"] == 10
    assert result["table_errors"] == []


def test_ops_start_compaction_worker_runs_background_pass(monkeypatch) -> None:
    class FakeMaster:
        pass

    calls: list[tuple[object, int, bool, bool, object]] = []

    def fake_compact_tables(
        table_names: object = None,
        *,
        min_files: int,
        delete_sources: bool,
        continue_on_error: bool,
        master: object,
    ) -> dict[str, object]:
        calls.append((table_names, min_files, delete_sources, continue_on_error, master))
        return {
            "tables_scanned": 1,
            "tables_compacted": 0,
            "groups_compacted": 0,
            "files_compacted": 0,
            "rows_compacted": 0,
            "table_results": [],
            "table_errors": [],
        }

    master = FakeMaster()
    monkeypatch.setattr(zippy, "_compact_tables", fake_compact_tables)

    worker = zippy.ops.start_compaction_worker(
        "ctp_ticks",
        master=master,
        interval_sec=0.01,
        min_files=3,
        delete_sources=False,
    )
    try:
        deadline = time.time() + 1.0
        while worker.latest_report() is None and time.time() < deadline:
            time.sleep(0.01)
    finally:
        worker.stop()
        worker.join(timeout=1.0)

    assert calls
    assert calls[0] == ("ctp_ticks", 3, False, True, master)
    assert worker.latest_report()["tables_scanned"] == 1
    assert worker.errors() == []
    assert not worker.is_alive()


def test_table_object_exposes_low_frequency_observability_methods() -> None:
    class FakeQuery:
        def stream_info(self) -> dict[str, object]:
            return {
                "stream_name": "ctp_ticks",
                "status": "writer_attached",
                "descriptor_generation": 3,
                "active_segment_descriptor": {"segment_id": 7, "generation": 3},
                "persist_events": [],
            }

    table = object.__new__(zippy.Table)
    table.source = "ctp_ticks"
    table._inner = FakeQuery()
    table._select_exprs = None
    table._where_expr = None

    assert table.info()["stream_name"] == "ctp_ticks"
    assert table.alerts() == []
    assert table.health()["status"] == "ok"


def test_table_health_reports_ok_for_active_stream() -> None:
    class FakeMaster:
        def get_stream(self, table_name: str) -> dict[str, object]:
            assert table_name == "ctp_ticks"
            return {
                "stream_name": table_name,
                "status": "writer_attached",
                "descriptor_generation": 3,
                "active_segment_descriptor": {"segment_id": 7, "generation": 3},
                "persist_events": [],
            }

    health = zippy.ops.table_health("ctp_ticks", master=FakeMaster())
    alerts = zippy.ops.table_alerts("ctp_ticks", master=FakeMaster())

    assert alerts == []
    assert health == {
        "table_name": "ctp_ticks",
        "status": "ok",
        "stream_status": "writer_attached",
        "descriptor_generation": 3,
        "alert_count": 0,
        "alerts": [],
    }


def test_table_health_reports_warning_for_stream_without_active_descriptor() -> None:
    class FakeMaster:
        def get_stream(self, table_name: str) -> dict[str, object]:
            return {
                "stream_name": table_name,
                "status": "registered",
                "descriptor_generation": 0,
                "active_segment_descriptor": None,
                "persist_events": [],
            }

    health = zippy.ops.table_health("empty_ticks", master=FakeMaster())

    assert health["status"] == "warning"
    assert health["alert_count"] == 1
    assert health["alerts"][0]["severity"] == "warning"
    assert health["alerts"][0]["kind"] == "active_descriptor_missing"
    assert health["alerts"][0]["table_name"] == "empty_ticks"
    assert "active segment descriptor is not published" in health["alerts"][0]["message"]


def test_table_health_reports_error_for_stale_stream_and_persist_failure() -> None:
    class FakeMaster:
        def get_stream(self, table_name: str) -> dict[str, object]:
            return {
                "stream_name": table_name,
                "status": "stale",
                "descriptor_generation": 4,
                "active_segment_descriptor": {"segment_id": 9, "generation": 4},
                "persist_events": [
                    {
                        "stream_name": table_name,
                        "persist_event_type": "persist_failed",
                        "source_segment_id": 8,
                        "source_generation": 0,
                        "attempts": 3,
                        "error": "Not a directory",
                        "created_at": 1777017600000000000,
                    }
                ],
            }

    alerts = zippy.ops.table_alerts("stale_ticks", master=FakeMaster())
    health = zippy.ops.table_health("stale_ticks", master=FakeMaster())

    assert health["status"] == "error"
    assert health["alert_count"] == 2
    assert [alert["kind"] for alert in alerts] == ["stream_stale", "persist_failed"]
    assert alerts[0]["severity"] == "error"
    assert alerts[0]["stream_status"] == "stale"
    assert alerts[1]["severity"] == "error"
    assert alerts[1]["source_segment_id"] == 8
    assert alerts[1]["source_generation"] == 0
    assert alerts[1]["attempts"] == 3
    assert alerts[1]["error"] == "Not a directory"


def test_read_table_waits_for_late_producer_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    ready = threading.Event()
    result: dict[str, object] = {}
    pipeline = None

    def open_table() -> None:
        ready.set()
        try:
            result["table"] = zippy.read_table(
                "late_ticks",
                master=reader_master,
                wait=True,
                timeout=2.0,
            )
        except BaseException as error:
            result["error"] = error

    reader_thread = threading.Thread(target=open_table, name="read-table-wait-test")
    try:
        reader_thread.start()
        assert ready.wait(timeout=1.0)
        time.sleep(0.05)

        pipeline = (
            zippy.Pipeline("late_ticks_ingest", master=writer_master)
            .stream_table("late_ticks", schema=schema, row_capacity=8)
            .start()
        )
        pipeline.write({"instrument_id": ["IF2606"], "last_price": [3912.5]})

        reader_thread.join(timeout=2.0)
        assert not reader_thread.is_alive()
        assert "error" not in result

        table = result["table"]
        assert isinstance(table, zippy.Table)
        rows = table.tail(1).to_pydict()

        assert rows == {
            "instrument_id": ["IF2606"],
            "last_price": [3912.5],
        }
    finally:
        if pipeline is not None:
            pipeline.stop()
        server.stop()


def test_read_table_wait_times_out_when_stream_never_appears(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)

    try:
        with pytest.raises(TimeoutError, match="timed out waiting for table source=\\[missing\\]"):
            zippy.read_table("missing", master=reader_master, wait=True, timeout="20ms")
    finally:
        server.stop()


def test_subscribe_waits_for_late_producer_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    ready = threading.Event()
    subscribed = threading.Event()
    received = threading.Event()
    rows: list[dict[str, object]] = []
    result: dict[str, object] = {}
    pipeline = None

    def on_row(row: zippy.Row) -> None:
        rows.append(row.to_dict())
        received.set()

    def start_subscriber() -> None:
        ready.set()
        try:
            result["subscriber"] = zippy.subscribe(
                "late_subscribe_ticks",
                callback=on_row,
                master=reader_master,
                wait=True,
                timeout=2.0,
                poll_interval_ms=1,
            )
            subscribed.set()
        except BaseException as error:
            result["error"] = error

    reader_thread = threading.Thread(target=start_subscriber, name="subscribe-wait-test")
    try:
        reader_thread.start()
        assert ready.wait(timeout=1.0)
        time.sleep(0.05)

        pipeline = (
            zippy.Pipeline("late_subscribe_ingest", master=writer_master)
            .stream_table("late_subscribe_ticks", schema=schema, row_capacity=8)
            .start()
        )
        assert subscribed.wait(timeout=2.0)
        pipeline.write({"instrument_id": ["IF2606"], "last_price": [3912.5]})

        reader_thread.join(timeout=2.0)
        assert not reader_thread.is_alive()
        assert "error" not in result
        assert received.wait(timeout=2.0)
        assert rows == [{"instrument_id": "IF2606", "last_price": 3912.5}]
    finally:
        subscriber = result.get("subscriber")
        if subscriber is not None:
            subscriber.stop()
        if pipeline is not None:
            pipeline.stop()
        server.stop()


def test_subscribe_table_waits_for_late_producer_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    ready = threading.Event()
    subscribed = threading.Event()
    received = threading.Event()
    batches: list[pa.Table] = []
    result: dict[str, object] = {}
    pipeline = None

    def on_table(table: pa.Table) -> None:
        batches.append(table)
        received.set()

    def start_subscriber() -> None:
        ready.set()
        try:
            result["subscriber"] = zippy.subscribe_table(
                "late_subscribe_table_ticks",
                callback=on_table,
                master=reader_master,
                wait=True,
                timeout=2.0,
                poll_interval_ms=1,
            )
            subscribed.set()
        except BaseException as error:
            result["error"] = error

    reader_thread = threading.Thread(target=start_subscriber, name="subscribe-table-wait-test")
    try:
        reader_thread.start()
        assert ready.wait(timeout=1.0)
        time.sleep(0.05)

        pipeline = (
            zippy.Pipeline("late_subscribe_table_ingest", master=writer_master)
            .stream_table("late_subscribe_table_ticks", schema=schema, row_capacity=8)
            .start()
        )
        assert subscribed.wait(timeout=2.0)
        pipeline.write({"instrument_id": ["IF2606"], "last_price": [3912.5]})

        reader_thread.join(timeout=2.0)
        assert not reader_thread.is_alive()
        assert "error" not in result
        assert received.wait(timeout=2.0)
        assert batches[-1].to_pydict() == {
            "instrument_id": ["IF2606"],
            "last_price": [3912.5],
        }
    finally:
        subscriber = result.get("subscriber")
        if subscriber is not None:
            subscriber.stop()
        if pipeline is not None:
            pipeline.stop()
        server.stop()


def test_subscribe_wait_times_out_when_stream_never_appears(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)

    try:
        with pytest.raises(TimeoutError, match="timed out waiting for table source=\\[missing\\]"):
            zippy.subscribe(
                "missing",
                callback=lambda row: None,
                master=reader_master,
                wait=True,
                timeout="20ms",
            )
    finally:
        server.stop()


def test_table_perf_probe_summarizes_latency_samples() -> None:
    module_path = WORKSPACE_ROOT / "examples" / "07_ops" / "02_table_perf_probe.py"
    spec = importlib.util.spec_from_file_location("table_perf_probe_example", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    summary = module.summarize_samples_ms([3.0, 1.0, 4.0, 2.0])

    assert summary == {
        "count": 4,
        "min": 1.0,
        "avg": 2.5,
        "p50": 2.0,
        "p95": 4.0,
        "p99": 4.0,
        "max": 4.0,
    }


def test_table_perf_probe_measures_private_scan_live_throughput(monkeypatch) -> None:
    module_path = WORKSPACE_ROOT / "examples" / "07_ops" / "02_table_perf_probe.py"
    spec = importlib.util.spec_from_file_location("table_perf_probe_example", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    schema = pa.schema([("seq", pa.int64())])
    batches = [
        pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema),
        pa.record_batch([pa.array([4, 5], type=pa.int64())], schema=schema),
    ]

    class FakeTable:
        def _scan_live(self):
            return pa.RecordBatchReader.from_batches(schema, batches)

    monkeypatch.setattr(module.zp, "read_table", lambda table_name: FakeTable())
    timestamps = iter([1_000_000_000, 1_005_000_000])
    monkeypatch.setattr(module.time, "perf_counter_ns", lambda: next(timestamps))

    report = module.measure_private_scan_live_throughput("ticks", iterations=1)

    assert report["iterations"] == 1
    assert report["last_batches"] == 2
    assert report["last_rows"] == 5
    assert report["latency_ms"]["avg"] == 5.0
    assert report["throughput_rows_per_sec"]["avg"] == 1000.0


def test_subscribe_latency_probe_summarizes_latency_samples() -> None:
    module_path = WORKSPACE_ROOT / "examples" / "07_ops" / "03_subscribe_latency_probe.py"
    spec = importlib.util.spec_from_file_location("subscribe_latency_probe_example", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    summary = module.summarize_samples_ms([0.9, 0.1, 0.4, 0.2])
    tick = module.make_tick(3, 1777017600000000000)
    slowest_rows = module.slowest_latency_rows(
        [
            {"seq": 1, "latency_ms": 0.2, "rollover_first_row": False},
            {"seq": 2, "latency_ms": 3.0, "rollover_first_row": False},
            {"seq": 3, "latency_ms": 1.4, "rollover_first_row": True},
        ],
        limit=2,
    )

    assert summary == {
        "count": 4,
        "min": 0.1,
        "avg": 0.4,
        "p50": 0.2,
        "p95": 0.9,
        "p99": 0.9,
        "max": 0.9,
    }
    assert tick["seq"].to_list() == [3]
    assert tick["instrument_id"].to_list() == ["IF2606"]
    assert tick["localtime_ns"].to_list() == [1777017600000000000]
    assert slowest_rows == [
        {"seq": 2, "latency_ms": 3.0, "rollover_first_row": False},
        {"seq": 3, "latency_ms": 1.4, "rollover_first_row": True},
    ]


def test_subscribe_latency_probe_runs_against_temp_master(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    module_path = WORKSPACE_ROOT / "examples" / "07_ops" / "03_subscribe_latency_probe.py"
    spec = importlib.util.spec_from_file_location("subscribe_latency_probe_example", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    server, control_endpoint = start_master_server(tmp_path)
    args = argparse.Namespace(
        uri=control_endpoint,
        table="subscribe_latency_probe_ticks",
        rows=20,
        interval_ms=0.0,
        row_capacity=8,
        poll_interval_ms=1,
        idle_spin_checks=7,
        warmup_ms=0.0,
        discard_first_rows=1,
        slowest_rows=3,
        timeout_sec=5.0,
        xfast=False,
        drop_existing=True,
    )

    try:
        report = module.run_probe(args)
    finally:
        if reset_default_master is not None:
            reset_default_master()
        server.stop()

    assert report["rows"] == 20
    assert report["idle_spin_checks"] == 7
    assert report["warmup_ms"] == 0.0
    assert report["discard_first_rows"] == 1
    assert report["measured_rows"] == 19
    assert report["append_latency_ms"]["count"] == 20
    assert report["rollover_append_latency_ms"]["count"] == 2
    assert report["latency_ms"]["count"] == 19
    assert len(report["slowest_rows"]) == 3
    assert min(row["seq"] for row in report["slowest_rows"]) >= 1
    assert report["slowest_rows"][0]["latency_ms"] >= report["slowest_rows"][1]["latency_ms"]
    assert report["rollover_first_row_latency_ms"]["count"] == 2
    assert report["subscriber_metrics"]["rows_delivered_total"] == 20
    assert report["subscriber_metrics"]["idle_spin_checks"] == 7
    assert report["active_segment_control"]["committed_row_count"] == 4


def test_master_client_drop_table_removes_stream_and_persisted_files(tmp_path: Path) -> None:
    try:
        server, control_endpoint = start_master_server(tmp_path)
    except RuntimeError as error:
        if "Operation not permitted" in str(error):
            pytest.skip("unix socket bind is not permitted in this test environment")
        raise
    tick_schema = pa.schema([("instrument_id", pa.string())])
    parquet_file = tmp_path / "tables" / "openctp_ticks" / "part-000001.parquet"
    parquet_file.parent.mkdir(parents=True)
    parquet_file.write_bytes(b"persisted rows")

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("drop_table_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "openctp", "openctp_ticks", {})
    client.publish_persisted_file(
        "openctp_ticks",
        {
            "file_path": str(parquet_file),
            "row_count": 1,
            "source_segment_id": 1,
        },
    )

    result = client.drop_table("openctp_ticks")

    assert result["table_name"] == "openctp_ticks"
    assert result["dropped"] is True
    assert result["sources_removed"] == 1
    assert result["persisted_files_deleted"] == 1
    assert not parquet_file.exists()
    assert client.list_streams() == []
    with pytest.raises(RuntimeError, match="stream not found"):
        client.get_stream("openctp_ticks")

    server.stop()


def test_master_client_publishes_and_gets_segment_descriptor(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("openctp_worker")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "openctp", "openctp_ticks", {})
    descriptor = {
        "magic": "zippy.segment.active",
        "version": 1,
        "schema_id": 7,
        "row_capacity": 64,
        "shm_os_id": "/tmp/zippy-segment",
        "payload_offset": 64,
        "committed_row_count_offset": 40,
        "segment_id": 1,
        "generation": 0,
    }

    client.publish_segment_descriptor("openctp_ticks", descriptor)
    fetched = client.get_segment_descriptor("openctp_ticks")

    assert fetched == descriptor

    server.stop()


def test_query_tail_reads_latest_available_active_segment_rows(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("query_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    writer.append_tick(1777017600000000000, "IF2606", 4102.5)
    writer.append_tick(1777017601000000000, "IF2606", 4103.0)
    writer.append_tick(1777017602000000000, "IF2607", 4104.5)
    client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    query = zippy.read_table(source="openctp_ticks", master=client)

    available = query.tail(10)
    assert isinstance(available, pa.Table)
    assert available.schema == tick_schema
    assert available.num_rows == 3
    assert available.column("instrument_id").to_pylist() == ["IF2606", "IF2606", "IF2607"]
    assert available.column("last_price").to_pylist() == [4102.5, 4103.0, 4104.5]

    latest = query.tail(2)
    assert latest.num_rows == 2
    assert latest.column("instrument_id").to_pylist() == ["IF2606", "IF2607"]
    assert latest.column("last_price").to_pylist() == [4103.0, 4104.5]

    server.stop()


def test_query_tail_reads_retained_sealed_segments_before_active_rows() -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        control_endpoint = str(Path(directory) / "zippy-master.sock")
        server = zippy.MasterServer(control_endpoint=control_endpoint)
        try:
            server.start()
        except RuntimeError as error:
            if "Operation not permitted" in str(error):
                pytest.skip("unix socket bind is not permitted in this test environment")
            raise
        pipeline = None
        try:
            client = zippy.connect(uri=control_endpoint, app="query_tail_sealed_test")
            tick_schema = pa.schema(
                [
                    ("instrument_id", pa.string()),
                    ("dt", pa.timestamp("ns", tz="UTC")),
                    ("last_price", pa.float64()),
                ]
            )
            pipeline = (
                zippy.Pipeline("query_tail_sealed_test")
                .stream_table("openctp_ticks", schema=tick_schema, row_capacity=32)
                .start()
            )
            row_count = 100
            pipeline.write(
                {
                    "instrument_id": [f"IF{index:04d}" for index in range(row_count)],
                    "dt": [1777017600000000000 + index for index in range(row_count)],
                    "last_price": [4100.0 + index for index in range(row_count)],
                }
            )

            query = zippy.read_table("openctp_ticks")
            latest = query.tail(row_count)
            for _ in range(50):
                if latest.num_rows == row_count:
                    break
                time.sleep(0.02)
                latest = query.tail(row_count)

            stream = client.get_stream("openctp_ticks")
            descriptor = client.get_segment_descriptor("openctp_ticks")
            assert len(stream["sealed_segments"]) >= 1
            assert "sealed_segments" not in descriptor
            assert latest.num_rows == row_count
            assert latest.column("last_price").to_pylist() == [
                4100.0 + index for index in range(row_count)
            ]
        finally:
            if pipeline is not None:
                pipeline.stop()
            if reset_default_master is not None:
                reset_default_master()
            server.stop()


def test_query_scan_live_returns_reader_over_retained_sealed_and_active_rows() -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        control_endpoint = str(Path(directory) / "zippy-master.sock")
        server = zippy.MasterServer(control_endpoint=control_endpoint)
        try:
            server.start()
        except RuntimeError as error:
            if "Operation not permitted" in str(error):
                pytest.skip("unix socket bind is not permitted in this test environment")
            raise
        pipeline = None
        try:
            zippy.connect(uri=control_endpoint, app="query_scan_live_test")
            tick_schema = pa.schema(
                [
                    ("instrument_id", pa.string()),
                    ("dt", pa.timestamp("ns", tz="UTC")),
                    ("last_price", pa.float64()),
                ]
            )
            pipeline = (
                zippy.Pipeline("query_scan_live_test")
                .stream_table("openctp_ticks", schema=tick_schema, row_capacity=32)
                .start()
            )
            row_count = 100
            pipeline.write(
                {
                    "instrument_id": [f"IF{index:04d}" for index in range(row_count)],
                    "dt": [1777017600000000000 + index for index in range(row_count)],
                    "last_price": [4100.0 + index for index in range(row_count)],
                }
            )

            query = zippy.read_table("openctp_ticks")
            reader = query._scan_live()
            assert isinstance(reader, pa.RecordBatchReader)

            table = reader.read_all()
            for _ in range(50):
                if table.num_rows == row_count:
                    break
                time.sleep(0.02)
                table = query._scan_live().read_all()

            assert table.schema == tick_schema
            assert table.num_rows == row_count
            assert table.column("last_price").to_pylist() == [
                4100.0 + index for index in range(row_count)
            ]
        finally:
            if pipeline is not None:
                pipeline.stop()
            if reset_default_master is not None:
                reset_default_master()
            server.stop()


def test_read_table_snapshot_collect_reuses_fixed_live_boundary() -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        control_endpoint = str(Path(directory) / "zippy-master.sock")
        server = zippy.MasterServer(control_endpoint=control_endpoint)
        try:
            server.start()
        except RuntimeError as error:
            if "Operation not permitted" in str(error):
                pytest.skip("unix socket bind is not permitted in this test environment")
            raise
        pipeline = None
        try:
            zippy.connect(uri=control_endpoint, app="query_snapshot_collect_test")
            tick_schema = pa.schema(
                [
                    ("instrument_id", pa.string()),
                    ("dt", pa.timestamp("ns", tz="UTC")),
                    ("last_price", pa.float64()),
                ]
            )
            pipeline = (
                zippy.Pipeline("query_snapshot_collect_test")
                .stream_table("snapshot_ticks", schema=tick_schema, row_capacity=32)
                .start()
            )
            pipeline.write(
                {
                    "instrument_id": ["IF2606", "IF2607"],
                    "dt": [1777017600000000000, 1777017600000000001],
                    "last_price": [4102.5, 4103.5],
                }
            )

            warmup = zippy.read_table("snapshot_ticks", snapshot=False).collect()
            for _ in range(50):
                if warmup.num_rows == 2:
                    break
                time.sleep(0.02)
                warmup = zippy.read_table("snapshot_ticks", snapshot=False).collect()
            assert warmup.num_rows == 2

            frozen = zippy.read_table("snapshot_ticks", snapshot=True)
            first = frozen.collect()
            assert first.num_rows == 2

            pipeline.write(
                {
                    "instrument_id": ["IF2608"],
                    "dt": [1777017600000000002],
                    "last_price": [4104.5],
                }
            )

            second = frozen.collect()
            assert second.num_rows == 2
            assert second.column("instrument_id").to_pylist() == ["IF2606", "IF2607"]

            latest = zippy.read_table("snapshot_ticks", snapshot=False).collect()
            for _ in range(50):
                if latest.num_rows == 3:
                    break
                time.sleep(0.02)
                latest = zippy.read_table("snapshot_ticks", snapshot=False).collect()
            assert latest.num_rows == 3
            assert latest.column("instrument_id").to_pylist() == ["IF2606", "IF2607", "IF2608"]
        finally:
            if pipeline is not None:
                pipeline.stop()
            if reset_default_master is not None:
                reset_default_master()
            server.stop()


def test_stream_table_e2e_ingest_rollover_persist_and_query(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    pipeline = None
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    row_count = 10
    expected_instruments = [f"IF{2600 + index}" for index in range(row_count)]
    expected_prices = [4100.0 + index for index in range(row_count)]

    try:
        zippy.connect(uri=control_endpoint, app="stream_table_e2e_test")
        pipeline = (
            zippy.Pipeline("stream_table_e2e_test")
            .stream_table(
                "e2e_ticks",
                schema=schema,
                row_capacity=4,
                retention_segments=1,
                persist="parquet",
                data_dir=tmp_path / "persisted",
            )
            .start()
        )
        pipeline.write(
            {
                "instrument_id": expected_instruments,
                "dt": [1777017600000000000 + index for index in range(row_count)],
                "last_price": expected_prices,
            }
        )

        query = zippy.read_table("e2e_ticks")
        persisted_files = []
        persisted_rows = 0
        for _ in range(50):
            persisted_files = query.persisted_files()
            persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)
            if persisted_rows == 8:
                break
            time.sleep(0.02)
        pipeline.flush()
        persisted_files = query.persisted_files()
        persisted_rows = sum(int(item.get("row_count", 0)) for item in persisted_files)

        latest = query.tail(row_count)
        for _ in range(50):
            if latest.num_rows == row_count:
                break
            time.sleep(0.02)
            latest = query.tail(row_count)

        stream = zippy.ops.table_info("e2e_ticks")
        persisted = query._scan_persisted().to_table()
        collected = query.collect()

        assert len(stream["sealed_segments"]) == 1
        assert persisted_rows == 8
        assert len(persisted_files) == 2
        assert all(Path(item["file_path"]).exists() for item in persisted_files)
        assert persisted.num_rows == 8
        assert persisted.column("last_price").to_pylist() == expected_prices[:8]
        assert latest.num_rows == row_count
        assert latest.column("instrument_id").to_pylist() == expected_instruments
        assert latest.column("last_price").to_pylist() == expected_prices
        assert collected.num_rows == row_count
        assert collected.column("last_price").to_pylist() == expected_prices
    finally:
        if pipeline is not None:
            pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_stream_table_e2e_persist_failure_is_queryable_and_blocks_retention(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    pipeline = None
    bad_data_dir = tmp_path / "not_a_directory"
    bad_data_dir.write_text("blocks persist directory creation", encoding="utf-8")
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )

    try:
        zippy.connect(uri=control_endpoint, app="stream_table_persist_failure_test")
        pipeline = (
            zippy.Pipeline("stream_table_persist_failure_test")
            .stream_table(
                "persist_failure_ticks",
                schema=schema,
                row_capacity=2,
                retention_segments=0,
                persist="parquet",
                data_dir=bad_data_dir,
            )
            .start()
        )
        pipeline.write(
            {
                "instrument_id": ["IF2606", "IF2607", "IF2608"],
                "dt": [
                    1777017600000000000,
                    1777017600000000001,
                    1777017600000000002,
                ],
                "last_price": [4100.0, 4101.0, 4102.0],
            }
        )

        query = zippy.read_table("persist_failure_ticks")
        events = []
        for _ in range(100):
            events = query.persist_events()
            if events:
                break
            time.sleep(0.02)

        with pytest.raises(RuntimeError, match="stream table persist failed"):
            pipeline.flush()

        stream = zippy.ops.table_info("persist_failure_ticks")
        health = zippy.ops.table_health("persist_failure_ticks")
        events = query.persist_events()
        assert query.persisted_files() == []
        assert len(events) == 1
        assert events[0]["stream_name"] == "persist_failure_ticks"
        assert events[0]["persist_event_type"] == "persist_failed"
        assert events[0]["source_segment_id"] == 1
        assert events[0]["source_generation"] == 0
        assert events[0]["attempts"] == 3
        assert "Not a directory" in events[0]["error"]
        assert len(stream["sealed_segments"]) == 1
        assert health["status"] == "error"
        assert [alert["kind"] for alert in health["alerts"]] == ["persist_failed"]
        assert health["alerts"][0]["source_segment_id"] == 1
    finally:
        if pipeline is not None:
            try:
                pipeline.stop()
            except RuntimeError as error:
                if "stream table persist failed" not in str(error):
                    raise
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_stream_table_e2e_writer_expiration_marks_stream_stale_and_blocks_live_reads(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    try:
        schema = pa.schema(
            [
                ("instrument_id", pa.string()),
                ("dt", pa.timestamp("ns", tz="UTC")),
                ("last_price", pa.float64()),
            ]
        )

        writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
        writer_process_id = writer_client.register_process("segment_writer")
        writer_client.register_stream("stale_ticks", schema, 64, 4096)
        writer_client.register_source("stale_source", "segment_test", "stale_ticks", {})
        writer = zippy._internal._SegmentTestWriter(
            "stale_ticks",
            schema,
            row_capacity=16,
        )
        writer.append_tick(1777017600000000000, "IF2606", 4102.5)
        writer_client.publish_segment_descriptor("stale_ticks", writer.descriptor())

        reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
        reader_client.register_process("query_reader")
        query = zippy.read_table("stale_ticks", master=reader_client)
        assert query.tail(1).column("instrument_id").to_pylist() == ["IF2606"]

        response = expire_process_for_test(control_endpoint, writer_process_id)
        assert response == {"HeartbeatAccepted": {"process_id": writer_process_id}}

        stream = writer_client.get_stream("stale_ticks")
        assert stream["status"] == "stale"
        assert stream["writer_process_id"] is None
        assert stream["active_segment_descriptor"] is not None

        with pytest.raises(RuntimeError, match="stream is stale"):
            query.tail(1)
    finally:
        server.stop()


def test_query_snapshot_includes_published_files() -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        root = Path(directory)
        control_endpoint = str(root / "zippy-master.sock")
        server = zippy.MasterServer(control_endpoint=control_endpoint)
        try:
            server.start()
        except RuntimeError as error:
            if "Operation not permitted" in str(error):
                pytest.skip("unix socket bind is not permitted in this test environment")
            raise

        try:
            tick_schema = pa.schema(
                [
                    ("instrument_id", pa.string()),
                    ("dt", pa.timestamp("ns", tz="UTC")),
                    ("last_price", pa.float64()),
                ]
            )
            persist_path = root / "data" / "ctp_ticks" / "trading_day=20260426"
            persist_path.mkdir(parents=True)
            parquet_file = persist_path / "part-000001.parquet"
            table = pa.table(
                {
                    "instrument_id": ["IF2606"],
                    "dt": [1777017600000000000],
                    "last_price": [4102.5],
                },
                schema=tick_schema,
            )
            pq.write_table(table, parquet_file)

            client = zippy.connect(uri=control_endpoint, app="persist_metadata_test")
            client.register_stream("openctp_ticks", tick_schema, 64, 4096)
            client.register_source("openctp_md", "persist_test", "openctp_ticks", {})
            writer = zippy._internal._SegmentTestWriter(
                "openctp_ticks",
                tick_schema,
                row_capacity=16,
            )
            writer.append_tick(1777017600000000000, "IF2606", 4102.5)
            client.publish_segment_descriptor("openctp_ticks", writer.descriptor())
            client.publish_persisted_file(
                "openctp_ticks",
                {
                    "file_path": str(parquet_file),
                    "row_count": 1,
                    "min_event_ts": 1777017600000000000,
                    "max_event_ts": 1777017600000000000,
                    "source_segment_id": 1,
                },
            )

            query = zippy.read_table("openctp_ticks")
            snapshot = query.snapshot()
            persisted_files = snapshot["persisted_files"]

            assert len(persisted_files) == 1
            assert persisted_files[0]["stream_name"] == "openctp_ticks"
            assert persisted_files[0]["file_path"] == str(parquet_file)
            assert persisted_files[0]["row_count"] == 1
            assert isinstance(persisted_files[0]["schema_hash"], str)
            assert pq.read_table(persisted_files[0]["file_path"]).num_rows == 1
        finally:
            if reset_default_master is not None:
                reset_default_master()
            server.stop()


def test_read_table_function_returns_table_object(monkeypatch) -> None:
    created: list[tuple[str, object, bool, object, bool]] = []

    class FakeTable:
        def __init__(
            self,
            source: str,
            master: object | None = None,
            *,
            wait: bool = False,
            timeout: float | str | None = None,
            snapshot: bool = True,
        ) -> None:
            self.source = source
            self.master = master
            self.wait = wait
            self.timeout = timeout
            self.snapshot = snapshot
            created.append((source, master, wait, timeout, snapshot))

    explicit_master = object()
    monkeypatch.setattr(zippy, "Table", FakeTable)

    table = zippy.read_table("ctp_ticks", master=explicit_master)

    assert isinstance(table, FakeTable)
    assert table.source == "ctp_ticks"
    assert table.master is explicit_master
    assert table.wait is False
    assert table.timeout is None
    assert table.snapshot is True
    assert created == [("ctp_ticks", explicit_master, False, None, True)]


def test_connect_sets_default_master_and_init_is_removed(monkeypatch) -> None:
    created: list[str] = []

    class FakeMasterClient:
        def __init__(self, control_endpoint: str) -> None:
            self.control_endpoint = control_endpoint
            self.registered_apps: list[str] = []
            created.append(control_endpoint)

        def register_process(self, app: str) -> None:
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            pass

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "MasterClient", FakeMasterClient)

    assert not hasattr(zippy, "init")

    try:
        client = zippy.connect(uri="~/zippy-master.sock", app="market_client")

        assert zippy.master() is client
        assert client.registered_apps == ["market_client"]
        assert created == [str(Path("~/zippy-master.sock").expanduser())]
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_default_heartbeat_interval_avoids_hot_control_plane_polling() -> None:
    assert zippy._DEFAULT_HEARTBEAT_INTERVAL_SEC == 3.0


def test_config_returns_master_runtime_config() -> None:
    class FakeMaster:
        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 2048,
                    "retention_segments": 4,
                    "persist": {
                        "enabled": True,
                        "method": "parquet",
                        "data_dir": "data",
                    },
                },
            }

    assert zippy.config(master=FakeMaster()) == {
        "table": {
            "row_capacity": 2048,
            "retention_segments": 4,
            "persist": {
                "enabled": True,
                "method": "parquet",
                "data_dir": "data",
            },
        },
    }


def test_connect_without_uri_uses_default_logical_endpoint(
    monkeypatch,
    tmp_path: Path,
) -> None:
    created: list[str] = []

    class FakeMasterClient:
        def __init__(self, control_endpoint: str) -> None:
            self.control_endpoint = control_endpoint
            created.append(control_endpoint)

        def list_streams(self) -> list[object]:
            return []

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "MasterClient", FakeMasterClient)
    monkeypatch.setenv("HOME", str(tmp_path))

    try:
        zippy.connect()

        assert created == [
            str(tmp_path / ".zippy" / "control_endpoints" / "default" / "master.sock")
        ]
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_resolve_uri_maps_logical_names_and_explicit_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    assert zippy._resolve_uri("default") == str(
        tmp_path / ".zippy" / "control_endpoints" / "default" / "master.sock"
    )
    assert zippy._resolve_uri("zippy://sim") == str(
        tmp_path / ".zippy" / "control_endpoints" / "sim" / "master.sock"
    )
    assert zippy._resolve_uri("~/custom/master.sock") == str(tmp_path / "custom" / "master.sock")
    assert zippy._resolve_uri("/tmp/custom-master.sock") == "/tmp/custom-master.sock"


def test_master_server_accepts_uri_keyword_for_logical_endpoint(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    server = zippy.MasterServer(uri="sim")

    assert server.control_endpoint() == str(
        tmp_path / ".zippy" / "control_endpoints" / "sim" / "master.sock"
    )


def test_connect_starts_background_heartbeat_for_registered_process(monkeypatch) -> None:
    heartbeat_seen = threading.Event()

    class FakeMasterClient:
        def __init__(self, control_endpoint: str) -> None:
            self.control_endpoint = control_endpoint
            self.registered_apps: list[str] = []
            self.heartbeat_count = 0

        def register_process(self, app: str) -> None:
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            self.heartbeat_count += 1
            heartbeat_seen.set()

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "MasterClient", FakeMasterClient)

    client = zippy.connect(
        uri="/tmp/zippy-master.sock",
        app="market_client",
        heartbeat_interval_sec=0.01,
    )

    try:
        assert heartbeat_seen.wait(timeout=1.0)
        assert client.registered_apps == ["market_client"]
        assert client.heartbeat_count >= 1
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_connect_without_app_validates_master_connection(monkeypatch) -> None:
    created: list[str] = []
    list_calls: list[str] = []

    class FakeMasterClient:
        def __init__(self, control_endpoint: str) -> None:
            self.control_endpoint = control_endpoint
            created.append(control_endpoint)

        def list_streams(self) -> list[object]:
            list_calls.append(self.control_endpoint)
            return []

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "MasterClient", FakeMasterClient)

    try:
        client = zippy.connect(uri="~/zippy-master.sock")

        assert zippy.master() is client
        assert created == [str(Path("~/zippy-master.sock").expanduser())]
        assert list_calls == created
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_drop_table_uses_default_master(monkeypatch) -> None:
    calls: list[tuple[str, bool]] = []

    class FakeMaster:
        def drop_table(self, table_name: str, drop_persisted: bool = True) -> dict[str, object]:
            calls.append((table_name, drop_persisted))
            return {
                "table_name": table_name,
                "dropped": True,
                "persisted_files_deleted": 2,
            }

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    master = FakeMaster()
    zippy._set_default_master(master, None, 10.0)

    try:
        result = zippy.ops.drop_table("ctp_ticks")

        assert result == {
            "table_name": "ctp_ticks",
            "dropped": True,
            "persisted_files_deleted": 2,
        }
        assert calls == [("ctp_ticks", True)]
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_drop_table_accepts_explicit_master_and_drop_persisted_flag() -> None:
    calls: list[tuple[str, bool]] = []

    class FakeMaster:
        def drop_table(self, table_name: str, drop_persisted: bool = True) -> dict[str, object]:
            calls.append((table_name, drop_persisted))
            return {
                "table_name": table_name,
                "dropped": True,
                "persisted_files_deleted": 0,
            }

    result = zippy.ops.drop_table("ctp_ticks", drop_persisted=False, master=FakeMaster())

    assert result == {
        "table_name": "ctp_ticks",
        "dropped": True,
        "persisted_files_deleted": 0,
    }
    assert calls == [("ctp_ticks", False)]


def test_ops_compact_table_replaces_small_partition_files(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    partition_dir = tmp_path / "persisted" / "dt_part=202604" / "instrument_id=IF2606"
    partition_dir.mkdir(parents=True)
    first_file = partition_dir / "part-000001.parquet"
    second_file = partition_dir / "part-000002.parquet"
    other_file = tmp_path / "persisted" / "dt_part=202604" / "instrument_id=IH2606" / "part.parquet"
    other_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2606"],
                "last_price": [4101.5],
            },
            schema=schema,
        ),
        first_file,
    )
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2606", "IF2606"],
                "last_price": [4102.5, 4103.5],
            },
            schema=schema,
        ),
        second_file,
    )
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IH2606"],
                "last_price": [2801.5],
            },
            schema=schema,
        ),
        other_file,
    )
    persisted_files = [
        {
            "persist_file_id": "first",
            "stream_name": "ctp_ticks",
            "schema_hash": "schema-a",
            "file_path": str(first_file),
            "row_count": 1,
            "source_segment_id": 1,
            "source_generation": 0,
            "partition_path": "dt_part=202604/instrument_id=IF2606",
            "partition": {"dt_part": "202604", "instrument_id": "IF2606"},
        },
        {
            "persist_file_id": "second",
            "stream_name": "ctp_ticks",
            "schema_hash": "schema-a",
            "file_path": str(second_file),
            "row_count": 2,
            "source_segment_id": 2,
            "source_generation": 0,
            "partition_path": "dt_part=202604/instrument_id=IF2606",
            "partition": {"dt_part": "202604", "instrument_id": "IF2606"},
        },
        {
            "persist_file_id": "other",
            "stream_name": "ctp_ticks",
            "schema_hash": "schema-a",
            "file_path": str(other_file),
            "row_count": 1,
            "source_segment_id": 3,
            "source_generation": 0,
            "partition_path": "dt_part=202604/instrument_id=IH2606",
            "partition": {"dt_part": "202604", "instrument_id": "IH2606"},
        },
    ]

    class FakeMaster:
        def __init__(self) -> None:
            self.replaced_files: list[dict[str, object]] | None = None

        def get_stream(self, table_name: str) -> dict[str, object]:
            assert table_name == "ctp_ticks"
            return {
                "stream_name": "ctp_ticks",
                "schema_hash": "schema-a",
                "active_segment_descriptor": {"segment_id": 99, "generation": 0},
                "sealed_segments": [],
                "persisted_files": persisted_files,
            }

        def replace_persisted_files(
            self,
            table_name: str,
            files: list[dict[str, object]],
        ) -> None:
            assert table_name == "ctp_ticks"
            self.replaced_files = files

    master = FakeMaster()

    result = zippy.ops.compact_table("ctp_ticks", master=master)

    assert result["groups_compacted"] == 1
    assert result["files_compacted"] == 2
    assert result["rows_compacted"] == 3
    assert result["source_files_deleted"] == 2
    assert not first_file.exists()
    assert not second_file.exists()
    assert other_file.exists()
    assert master.replaced_files is not None
    assert len(master.replaced_files) == 2
    compacted = [
        item for item in master.replaced_files if item.get("compacted_file_count") == 2
    ][0]
    assert compacted["row_count"] == 3
    assert compacted["partition_path"] == "dt_part=202604/instrument_id=IF2606"
    assert pq.read_table(compacted["file_path"], partitioning=None).num_rows == 3


def test_master_client_replace_persisted_files_updates_stream_metadata(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    try:
        client = zippy.MasterClient(control_endpoint=control_endpoint)
        schema = pa.schema([("instrument_id", pa.string())])
        client.register_stream("ctp_ticks", schema, buffer_size=8, frame_size=1024)
        parquet_file = tmp_path / "compact.parquet"
        pq.write_table(pa.table({"instrument_id": ["IF2606"]}, schema=schema), parquet_file)

        client.replace_persisted_files(
            "ctp_ticks",
            [
                {
                    "persist_file_id": "compact-1",
                    "file_path": str(parquet_file),
                    "row_count": 1,
                }
            ],
        )

        stream = client.get_stream("ctp_ticks")
        assert len(stream["persisted_files"]) == 1
        assert stream["persisted_files"][0]["persist_file_id"] == "compact-1"
        assert stream["persisted_files"][0]["stream_name"] == "ctp_ticks"
        assert isinstance(stream["persisted_files"][0]["schema_hash"], str)
    finally:
        server.stop()


def test_query_error_includes_source_and_master_uri(monkeypatch) -> None:
    class FakeMaster:
        def control_endpoint(self) -> str:
            return "/tmp/missing-zippy-master.sock"

    class BrokenNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            raise RuntimeError("io error reason=[No such file or directory]")

    monkeypatch.setattr(zippy, "_NativeQuery", BrokenNativeQuery)

    with pytest.raises(RuntimeError) as exc_info:
        zippy.read_table("ldc_md_ticks", master=FakeMaster())

    message = str(exc_info.value)
    assert "source=[ldc_md_ticks]" in message
    assert "master_uri=[/tmp/missing-zippy-master.sock]" in message


def test_query_registers_default_master_process_before_native_query(monkeypatch) -> None:
    created: list[tuple[str, object]] = []

    class FakeMaster:
        def __init__(self) -> None:
            self.process_id_value: str | None = None
            self.registered_apps: list[str] = []

        def process_id(self) -> str | None:
            return self.process_id_value

        def register_process(self, app: str) -> None:
            self.process_id_value = "proc_1"
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            return None

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            assert master.process_id() == "proc_1"
            created.append((source, master))

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    master = FakeMaster()
    zippy._set_default_master(master, None, 10.0)

    try:
        zippy.read_table("ldc_md_ticks")

        assert master.registered_apps == ["read_table.ldc_md_ticks"]
        assert created == [("ldc_md_ticks", master)]
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_pipeline_process_registration_starts_default_heartbeat(monkeypatch) -> None:
    heartbeat_seen = threading.Event()
    tick_schema = pa.schema([("instrument_id", pa.string())])

    class FakeMaster:
        def __init__(self) -> None:
            self.process_id_value: str | None = None
            self.registered_apps: list[str] = []
            self.heartbeat_count = 0

        def process_id(self) -> str | None:
            return self.process_id_value

        def register_process(self, app: str) -> None:
            self.process_id_value = "proc_1"
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            self.heartbeat_count += 1
            heartbeat_seen.set()

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def active_descriptor(self) -> dict[str, object]:
            return {}

    class SchemaSource:
        def _zippy_output_schema(self) -> pa.Schema:
            return tick_schema

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)

    master = FakeMaster()
    zippy._set_default_master(master, None, 0.01)

    try:
        zippy.Pipeline("test_ingest").source(SchemaSource()).stream_table("openctp_ticks")

        assert heartbeat_seen.wait(timeout=1.0)
        assert master.registered_apps == ["test_ingest"]
        assert master.heartbeat_count >= 1
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_subscribe_row_mode_delegates_row_creation_to_native(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 10,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    def on_tick(row: zippy.Row) -> None:
        return None

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber(
        source="ctp_ticks",
        callback=on_tick,
        master=explicit_master,
        poll_interval_ms=3,
        xfast=True,
    )

    assert created == {
        "source": "ctp_ticks",
        "master": explicit_master,
        "callback": on_tick,
        "poll_interval_ms": 3,
        "xfast": True,
        "idle_spin_checks": 64,
        "row_factory": zippy.Row,
        "instrument_ids": None,
    }


def test_row_reuses_exact_dict_values_without_copy() -> None:
    values: dict[str, object] = {
        "instrument_id": "IF2606",
        "last_price": 4102.5,
    }

    row = zippy.Row(values)

    assert row._values is values
    values["last_price"] = 4103.0
    assert row["last_price"] == 4103.0
    assert row.to_dict() == {
        "instrument_id": "IF2606",
        "last_price": 4103.0,
    }


def test_subscribe_row_mode_passes_instrument_filter_to_native(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    explicit_master = object()
    instrument_ids = ["IF2606", "IF2607"]
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber(
        "ctp_ticks",
        callback=lambda row: None,
        master=explicit_master,
        instrument_ids=instrument_ids,
    )

    assert created["instrument_ids"] is instrument_ids
    assert created["row_factory"] is zippy.Row


def test_subscribe_row_mode_accepts_col_instrument_filter(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber(
        "ctp_ticks",
        callback=lambda row: None,
        master=explicit_master,
        filter=zippy.col("instrument_id").is_in(["IF2606", "IF2607"]),
    )

    assert created["instrument_ids"] == ["IF2606", "IF2607"]
    assert created["row_factory"] is zippy.Row


def test_subscribe_passes_instrument_filter_to_stream_subscriber(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeStreamSubscriber:
        def __init__(
            self,
            source: str,
            callback: object,
            master: object | None = None,
            *,
            poll_interval_ms: int | None = None,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            instrument_ids: object | None = None,
            wait: bool = False,
            timeout: float | str | None = None,
        ) -> None:
            created["source"] = source
            created["callback"] = callback
            created["master"] = master
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["instrument_ids"] = instrument_ids
            created["wait"] = wait
            created["timeout"] = timeout

        def start(self) -> "FakeStreamSubscriber":
            created["started"] = True
            return self

    callback = lambda row: None
    explicit_master = object()
    monkeypatch.setattr(zippy, "StreamSubscriber", FakeStreamSubscriber)

    subscriber = zippy.subscribe(
        "ctp_ticks",
        callback=callback,
        master=explicit_master,
        instrument_ids=("IF2606", "IF2607"),
    )

    assert isinstance(subscriber, FakeStreamSubscriber)
    assert created == {
        "source": "ctp_ticks",
        "callback": callback,
        "master": explicit_master,
        "poll_interval_ms": 1,
        "xfast": False,
        "idle_spin_checks": 64,
        "instrument_ids": ("IF2606", "IF2607"),
        "wait": False,
        "timeout": None,
        "started": True,
    }


def test_subscribe_passes_idle_spin_checks_to_native(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber(
        "ctp_ticks",
        callback=lambda row: None,
        master=explicit_master,
        poll_interval_ms=5,
        idle_spin_checks=256,
    )

    assert created["poll_interval_ms"] == 5
    assert created["idle_spin_checks"] == 256
    assert created["row_factory"] is zippy.Row


def test_subscribe_row_mode_defaults_to_low_latency_poll_interval(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber("ctp_ticks", callback=lambda row: None, master=explicit_master)

    assert created["poll_interval_ms"] == 1
    assert created["idle_spin_checks"] == 64
    assert created["row_factory"] is zippy.Row
    assert created["instrument_ids"] is None


def test_subscribe_table_keeps_batch_friendly_default_poll_interval(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.StreamSubscriber(
        "ctp_ticks",
        callback=lambda table: None,
        master=explicit_master,
        _table_callback=True,
    )

    assert created["poll_interval_ms"] == 10
    assert created["idle_spin_checks"] == 64
    assert created["row_factory"] is None
    assert created["instrument_ids"] is None


def test_subscribe_table_filters_batches_and_limits_latest_count(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            captured["callback"] = callback
            captured["row_factory"] = row_factory
            captured["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    received: list[pa.Table] = []
    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    zippy.subscribe_table(
        "ctp_ticks",
        callback=received.append,
        master=explicit_master,
        filter=zippy.col("instrument_id") == "IF2606",
        batch_size=4,
        count=3,
    )

    native_callback = captured["callback"]
    assert callable(native_callback)
    assert captured["row_factory"] is None
    assert captured["instrument_ids"] is None

    native_callback(
        pa.table(
            {
                "instrument_id": ["IF2605", "IF2606"],
                "last_price": [100.0, 101.0],
            }
        )
    )
    assert received == []

    native_callback(
        pa.table(
            {
                "instrument_id": ["IF2606", "IF2605", "IF2606", "IF2606"],
                "last_price": [102.0, 103.0, 104.0, 105.0],
            }
        )
    )

    assert len(received) == 1
    assert received[0].to_pydict() == {
        "instrument_id": ["IF2606", "IF2606", "IF2606"],
        "last_price": [102.0, 104.0, 105.0],
    }


def test_subscribe_table_throttle_ms_flushes_pending_rows(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 1,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            captured["callback"] = callback

        def start(self) -> None:
            return None

    times = iter([0, 1_000_000, 12_000_000, 12_000_000])
    received: list[pa.Table] = []
    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)
    monkeypatch.setattr(zippy.time, "perf_counter_ns", lambda: next(times))

    zippy.subscribe_table(
        "ctp_ticks",
        callback=received.append,
        master=explicit_master,
        throttle_ms=10,
    )

    native_callback = captured["callback"]
    assert callable(native_callback)

    native_callback(pa.table({"instrument_id": ["IF2606"], "last_price": [101.0]}))
    assert received == []

    native_callback(pa.table({"instrument_id": ["IF2607"], "last_price": [102.0]}))
    assert len(received) == 1
    assert received[0].to_pydict() == {
        "instrument_id": ["IF2606", "IF2607"],
        "last_price": [101.0, 102.0],
    }


def test_subscribe_registers_default_master_process_before_native_subscriber(
    monkeypatch,
) -> None:
    created: dict[str, object] = {}

    class FakeMaster:
        def __init__(self) -> None:
            self.process_id_value: str | None = None
            self.registered_apps: list[str] = []

        def process_id(self) -> str | None:
            return self.process_id_value

        def register_process(self, app: str) -> None:
            self.process_id_value = "proc_1"
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            return None

    class FakeNativeStreamSubscriber:
        def __init__(
            self,
            source: str,
            master: object,
            callback: object,
            poll_interval_ms: int = 10,
            xfast: bool = False,
            idle_spin_checks: int = 64,
            row_factory: object | None = None,
            instrument_ids: object | None = None,
        ) -> None:
            assert master.process_id() == "proc_1"
            created["source"] = source
            created["master"] = master
            created["callback"] = callback
            created["poll_interval_ms"] = poll_interval_ms
            created["xfast"] = xfast
            created["idle_spin_checks"] = idle_spin_checks
            created["row_factory"] = row_factory
            created["instrument_ids"] = instrument_ids

        def start(self) -> None:
            return None

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()
    monkeypatch.setattr(zippy, "_NativeStreamSubscriber", FakeNativeStreamSubscriber)

    master = FakeMaster()
    zippy._set_default_master(master, None, 10.0)

    try:
        zippy.StreamSubscriber("ctp_ticks", callback=lambda row: None)

        assert master.registered_apps == ["subscribe.ctp_ticks"]
        assert created["source"] == "ctp_ticks"
        assert created["master"] is master
        assert created["row_factory"] is zippy.Row
        assert created["instrument_ids"] is None
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_read_from_uses_default_master(monkeypatch) -> None:
    class FakeMaster:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object, bool]] = []

        def read_from(
            self,
            stream_name: str,
            instrument_ids: object = None,
            xfast: bool = False,
        ) -> str:
            self.calls.append((stream_name, instrument_ids, xfast))
            return "reader"

    fake_master = FakeMaster()
    monkeypatch.setattr(zippy, "_DEFAULT_MASTER", fake_master)

    reader = zippy.read_from("ctp_ticks", instrument_ids=["IF2606"], xfast=True)

    assert reader == "reader"
    assert fake_master.calls == [("ctp_ticks", ["IF2606"], True)]


def test_read_from_registers_default_master_process_before_reading(monkeypatch) -> None:
    class FakeMaster:
        def __init__(self) -> None:
            self.process_id_value: str | None = None
            self.registered_apps: list[str] = []
            self.calls: list[tuple[str, object, bool]] = []

        def process_id(self) -> str | None:
            return self.process_id_value

        def register_process(self, app: str) -> None:
            self.process_id_value = "proc_1"
            self.registered_apps.append(app)

        def heartbeat(self) -> None:
            return None

        def read_from(
            self,
            stream_name: str,
            instrument_ids: object = None,
            xfast: bool = False,
        ) -> str:
            assert self.process_id() == "proc_1"
            self.calls.append((stream_name, instrument_ids, xfast))
            return "reader"

    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    master = FakeMaster()
    zippy._set_default_master(master, None, 10.0)

    try:
        reader = zippy.read_from("ctp_ticks", instrument_ids=["IF2606"], xfast=True)

        assert reader == "reader"
        assert master.registered_apps == ["read_from.ctp_ticks"]
        assert master.calls == [("ctp_ticks", ["IF2606"], True)]
    finally:
        if reset_default_master is not None:
            reset_default_master()


def test_query_object_exposes_schema_stream_info_and_snapshot(monkeypatch, tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    parquet_file = tmp_path / "persisted.parquet"
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2606"],
                "last_price": [4102.5],
            },
            schema=schema,
        ),
        parquet_file,
    )
    persisted_file = {
        "file_path": str(parquet_file),
        "row_count": 1,
    }
    stream_info = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "descriptor_generation": 2,
        "persisted_files": [persisted_file],
    }
    snapshot = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "active_committed_row_high_watermark": 12,
        "sealed_segments": [],
        "persisted_files": [persisted_file],
        "descriptor_generation": 2,
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return schema

        def stream_info(self) -> dict[str, object]:
            return stream_info

        def snapshot(self) -> dict[str, object]:
            return snapshot

        def scan_live(self) -> pa.RecordBatchReader:
            batch = pa.record_batch(
                {
                    "instrument_id": ["IF2606"],
                    "last_price": [4102.5],
                },
                schema=schema,
            )
            return pa.RecordBatchReader.from_batches(schema, [batch])

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    assert query.schema() == schema
    assert query.stream_info() == stream_info
    assert query.snapshot() == snapshot
    assert query.persisted_files() == [persisted_file]
    assert query._scan_live().read_all().num_rows == 1
    assert query._scan_persisted().count_rows() == 1


def test_query_tail_backfills_from_persisted_without_duplicate_live_segments(
    monkeypatch,
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    older_file = tmp_path / "segment-1.parquet"
    overlap_file = tmp_path / "segment-2.parquet"
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2604", "IF2605"],
                "last_price": [4099.5, 4100.5],
            },
            schema=schema,
        ),
        older_file,
    )
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2606"],
                "last_price": [4101.5],
            },
            schema=schema,
        ),
        overlap_file,
    )
    live_table = pa.table(
        {
            "instrument_id": ["IF2606", "IF2607"],
            "last_price": [4101.5, 4102.5],
        },
        schema=schema,
    )
    snapshot = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "active_segment_descriptor": {
            "segment_id": 3,
            "generation": 0,
        },
        "active_committed_row_high_watermark": 1,
        "sealed_segments": [
            {
                "segment_id": 2,
                "generation": 0,
            }
        ],
        "persisted_files": [
            {
                "file_path": str(older_file),
                "row_count": 2,
                "source_segment_id": 1,
                "source_generation": 0,
            },
            {
                "file_path": str(overlap_file),
                "row_count": 1,
                "source_segment_id": 2,
                "source_generation": 0,
            },
        ],
        "descriptor_generation": 3,
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def tail(self, n: int) -> pa.Table:
            assert n == 4
            return live_table

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            return snapshot

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    latest = query.tail(4)

    assert latest.column("instrument_id").to_pylist() == [
        "IF2604",
        "IF2605",
        "IF2606",
        "IF2607",
    ]
    assert latest.column("last_price").to_pylist() == [4099.5, 4100.5, 4101.5, 4102.5]


def test_query_tail_reads_partitioned_persisted_files_with_same_column_name(
    monkeypatch,
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    partitioned_file = tmp_path / "dt_part=202604" / "instrument_id=IF2604" / "segment-1.parquet"
    partitioned_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2604"],
                "last_price": [4099.5],
            },
            schema=schema,
        ),
        partitioned_file,
    )
    live_table = pa.table(
        {
            "instrument_id": ["IF2605"],
            "last_price": [4100.5],
        },
        schema=schema,
    )
    snapshot = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "active_segment_descriptor": {
            "segment_id": 2,
            "generation": 0,
        },
        "active_committed_row_high_watermark": 1,
        "sealed_segments": [],
        "persisted_files": [
            {
                "file_path": str(partitioned_file),
                "row_count": 1,
                "source_segment_id": 1,
                "source_generation": 0,
            },
        ],
        "descriptor_generation": 3,
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def tail(self, n: int) -> pa.Table:
            assert n == 2
            return live_table

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            return snapshot

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    latest = query.tail(2)

    assert latest.schema.field("instrument_id").type == pa.string()
    assert latest.column("instrument_id").to_pylist() == ["IF2604", "IF2605"]
    assert latest.column("last_price").to_pylist() == [4099.5, 4100.5]


def test_query_collect_and_reader_merge_persisted_and_live_without_duplicates(
    monkeypatch,
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    older_file = tmp_path / "segment-1.parquet"
    overlap_file = tmp_path / "segment-2.parquet"
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2604", "IF2605"],
                "last_price": [4099.5, 4100.5],
            },
            schema=schema,
        ),
        older_file,
    )
    pq.write_table(
        pa.table(
            {
                "instrument_id": ["IF2606"],
                "last_price": [4101.5],
            },
            schema=schema,
        ),
        overlap_file,
    )
    live_batch = pa.record_batch(
        {
            "instrument_id": ["IF2606", "IF2607"],
            "last_price": [4101.5, 4102.5],
        },
        schema=schema,
    )
    snapshot = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "active_segment_descriptor": {
            "segment_id": 3,
            "generation": 0,
        },
        "active_committed_row_high_watermark": 1,
        "sealed_segments": [
            {
                "segment_id": 2,
                "generation": 0,
            }
        ],
        "persisted_files": [
            {
                "file_path": str(older_file),
                "row_count": 2,
                "source_segment_id": 1,
                "source_generation": 0,
            },
            {
                "file_path": str(overlap_file),
                "row_count": 1,
                "source_segment_id": 2,
                "source_generation": 0,
            },
        ],
        "descriptor_generation": 3,
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            return snapshot

        def scan_live(self) -> pa.RecordBatchReader:
            return pa.RecordBatchReader.from_batches(schema, [live_batch])

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    collected = query.collect()
    from_reader = query.reader().read_all()

    assert collected.column("instrument_id").to_pylist() == [
        "IF2604",
        "IF2605",
        "IF2606",
        "IF2607",
    ]
    assert from_reader.column("instrument_id").to_pylist() == [
        "IF2604",
        "IF2605",
        "IF2606",
        "IF2607",
    ]


def test_query_conversion_helpers_use_collect(monkeypatch) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606"],
            "last_price": [4102.5],
        },
        schema=schema,
    )
    collect_calls = 0

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    def collect() -> pa.Table:
        nonlocal collect_calls
        collect_calls += 1
        return table

    query.collect = collect

    assert query.to_pyarrow() is table

    polars_frame = query.to_polars()
    assert isinstance(polars_frame, pl.DataFrame)
    assert polars_frame["instrument_id"].to_list() == ["IF2606"]

    class FakeTable:
        def to_pandas(self, **kwargs: object) -> tuple[str, dict[str, object]]:
            return ("pandas", kwargs)

    query.collect = lambda: FakeTable()

    assert query.to_pandas(split_blocks=True) == ("pandas", {"split_blocks": True})
    assert collect_calls == 2


def test_query_expr_plan_filters_projects_and_computes_with_polars_backend(
    monkeypatch,
) -> None:
    schema = pa.schema(
        [
            ("dt", pa.int64()),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
            ("bid_price_1", pa.float64()),
            ("ask_price_1", pa.float64()),
        ]
    )
    live_batch = pa.record_batch(
        {
            "dt": [1, 2, 3, 4],
            "instrument_id": ["IF2606", "IF2606", "IF2607", "IF2606"],
            "last_price": [3999.0, 4102.5, 4103.5, 4104.5],
            "bid_price_1": [3998.0, 4102.0, 4103.0, 4104.0],
            "ask_price_1": [3999.0, 4102.5, 4103.5, 4104.8],
        },
        schema=schema,
    )
    snapshot = {
        "stream_name": "ctp_ticks",
        "schema_hash": "abc",
        "active_segment_descriptor": {
            "segment_id": 1,
            "generation": 0,
        },
        "active_committed_row_high_watermark": 4,
        "sealed_segments": [],
        "persisted_files": [],
        "descriptor_generation": 1,
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            return snapshot

        def scan_live(self) -> pa.RecordBatchReader:
            return pa.RecordBatchReader.from_batches(schema, [live_batch])

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)
    filtered = (
        query.filter(
            (zippy.col("instrument_id") == "IF2606") & (zippy.col("last_price") > 4000.0)
        )
        .between(zippy.col("dt"), 2, 4)
        .select(
            [
                zippy.col("dt"),
                zippy.col("instrument_id"),
                (zippy.col("ask_price_1") - zippy.col("bid_price_1")).alias("spread"),
            ]
        )
    )

    table = filtered.tail(1)
    frame = filtered.to_polars()

    assert table.column_names == ["dt", "instrument_id", "spread"]
    assert table.column("dt").to_pylist() == [4]
    assert table.column("spread").to_pylist() == pytest.approx([0.8])
    assert frame["dt"].to_list() == [2, 4]
    assert frame["spread"].to_list() == pytest.approx([0.5, 0.8])
    assert query.collect().column_names == [
        "dt",
        "instrument_id",
        "last_price",
        "bid_price_1",
        "ask_price_1",
    ]


def test_read_table_snapshot_policy_controls_collect_boundary(monkeypatch) -> None:
    schema = pa.schema([("value", pa.int64())])

    class FakeNativeQuery:
        snapshot_calls = 0

        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            type(self).snapshot_calls += 1
            return {
                "stream_name": self.source,
                "schema_hash": "abc",
                "active_segment_descriptor": {"segment_id": 1, "generation": 0},
                "active_committed_row_high_watermark": type(self).snapshot_calls,
                "sealed_segments": [],
                "persisted_files": [],
                "descriptor_generation": type(self).snapshot_calls,
            }

        def scan_snapshot(self, snapshot: dict[str, object]) -> pa.RecordBatchReader:
            value = int(snapshot["active_committed_row_high_watermark"])
            batch = pa.record_batch({"value": [value]}, schema=schema)
            return pa.RecordBatchReader.from_batches(schema, [batch])

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    frozen = zippy.read_table("ticks", master=explicit_master, snapshot=True)
    first = frozen.collect()
    second = frozen.collect()

    assert first.column("value").to_pylist() == [1]
    assert second.column("value").to_pylist() == [1]

    live = zippy.read_table("ticks", master=explicit_master, snapshot=False)
    third = live.collect()
    fourth = live.collect()

    assert third.column("value").to_pylist() == [2]
    assert fourth.column("value").to_pylist() == [3]


def test_lazy_table_api_supports_filter_select_join_with_columns_and_lit(monkeypatch) -> None:
    spot_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    future_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("future_price", pa.float64()),
        ]
    )
    batches = {
        "spot_latest": pa.record_batch(
            {
                "instrument_id": ["a", "b"],
                "last_price": [102.0, 201.0],
            },
            schema=spot_schema,
        ),
        "future_latest": pa.record_batch(
            {
                "instrument_id": ["a", "b"],
                "future_price": [100.0, 200.0],
            },
            schema=future_schema,
        ),
    }

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return batches[self.source].schema

        def snapshot(self) -> dict[str, object]:
            return {
                "stream_name": self.source,
                "schema_hash": "abc",
                "active_segment_descriptor": {"segment_id": 1, "generation": 0},
                "active_committed_row_high_watermark": batches[self.source].num_rows,
                "sealed_segments": [],
                "persisted_files": [],
                "descriptor_generation": 1,
            }

        def scan_snapshot(self, snapshot: dict[str, object]) -> pa.RecordBatchReader:
            batch = batches[self.source]
            return pa.RecordBatchReader.from_batches(batch.schema, [batch])

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    spot = (
        zippy.read_table("spot_latest", master=explicit_master, snapshot=False)
        .filter(zippy.col("instrument_id") == "a")
        .select("instrument_id", "last_price")
    )
    future = (
        zippy.read_table("future_latest", master=explicit_master, snapshot=False)
        .filter(zippy.col("instrument_id") == "a")
        .select("instrument_id", "future_price")
        .join(spot, on="instrument_id")
        .with_columns(
            basis=zippy.col("last_price") / zippy.col("future_price") - zippy.lit(1.0),
        )
        .select("instrument_id", "basis")
    )

    result = future.collect()

    assert result.column_names == ["instrument_id", "basis"]
    assert result.column("instrument_id").to_pylist() == ["a"]
    assert result.column("basis").to_pylist() == pytest.approx([0.02])


def test_table_plan_pushes_projection_and_simple_predicate_to_persisted_reader(
    monkeypatch,
) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )
    persisted_table = pa.table(
        {
            "instrument_id": ["IF2606", "IF2607"],
            "last_price": [4102.5, 4103.5],
            "volume": [10, 20],
        },
        schema=schema,
    )
    read_calls: list[dict[str, object]] = []

    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

        def schema(self) -> pa.Schema:
            return schema

        def snapshot(self) -> dict[str, object]:
            return {
                "stream_name": self.source,
                "schema_hash": "abc",
                "active_segment_descriptor": {"segment_id": 1, "generation": 0},
                "active_committed_row_high_watermark": 0,
                "sealed_segments": [],
                "persisted_files": [
                    {
                        "file_path": "/tmp/ctp_ticks.parquet",
                        "source_segment_id": 2,
                        "source_generation": 0,
                    }
                ],
                "descriptor_generation": 1,
            }

        def scan_snapshot(self, snapshot: dict[str, object]) -> pa.RecordBatchReader:
            return pa.RecordBatchReader.from_batches(schema, [])

    def fake_read_persisted_parquet_file(
        file_path: object,
        *,
        columns: list[str] | None = None,
        filters: object | None = None,
    ) -> pa.Table:
        read_calls.append(
            {
                "file_path": file_path,
                "columns": columns,
                "filters": filters,
            }
        )
        table = persisted_table
        if filters == [("instrument_id", "==", "IF2606")]:
            table = table.filter(pc.equal(table["instrument_id"], "IF2606"))
        if columns is not None:
            table = table.select(columns)
        return table

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)
    monkeypatch.setattr(
        zippy,
        "_read_persisted_parquet_file",
        fake_read_persisted_parquet_file,
    )

    query = (
        zippy.read_table("ctp_ticks", master=explicit_master, snapshot=False)
        .filter(zippy.col("instrument_id") == "IF2606")
        .select("instrument_id", "last_price")
    )

    result = query.collect()

    assert result.column_names == ["instrument_id", "last_price"]
    assert result.column("instrument_id").to_pylist() == ["IF2606"]
    assert read_calls == [
        {
            "file_path": "/tmp/ctp_ticks.parquet",
            "columns": ["instrument_id", "last_price"],
            "filters": [("instrument_id", "==", "IF2606")],
        }
    ]


def test_table_scan_apis_are_private_to_avoid_query_semantics_confusion(monkeypatch) -> None:
    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    query = zippy.read_table("ctp_ticks", master=explicit_master)

    assert not hasattr(query, "scan_live")
    assert not hasattr(query, "scan_persisted")
    assert hasattr(query, "_scan_live")
    assert hasattr(query, "_scan_persisted")


def test_read_table_replaces_query_api(monkeypatch) -> None:
    class FakeNativeQuery:
        def __init__(self, source: str, master: object) -> None:
            self.source = source
            self.master = master

    explicit_master = object()
    monkeypatch.setattr(zippy, "_NativeQuery", FakeNativeQuery)

    table = zippy.read_table("ctp_ticks", master=explicit_master)

    assert isinstance(table, zippy.Table)
    assert table.source == "ctp_ticks"
    assert not hasattr(zippy, "Query")
    assert not hasattr(zippy, "query")
    assert "Query" not in zippy.__all__
    assert "query" not in zippy.__all__


def test_stream_table_engine_is_not_a_top_level_user_api() -> None:
    assert not hasattr(zippy, "StreamTableEngine")
    assert "StreamTableEngine" not in zippy.__all__
    assert hasattr(zippy._internal, "StreamTableMaterializer")
    assert not hasattr(zippy._internal, "StreamTableEngine")


def test_archive_compatibility_aliases_are_removed() -> None:
    assert not hasattr(zippy, "ParquetArchive")
    assert "ParquetArchive" not in zippy.__all__
    assert not hasattr(zippy.Table, "archive_files")
    assert not hasattr(zippy.Table, "scan_archive")
    assert not hasattr(zippy.MasterClient, "publish_archive_file")


def test_connect_sets_default_master_for_query_tail(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("query_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    writer.append_tick(1777017600000000000, "IF2606", 4102.5)
    writer.append_tick(1777017601000000000, "IF2607", 4104.5)
    client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    latest = zippy.read_table("openctp_ticks").tail(10)
    snapshot = zippy.read_table("openctp_ticks").snapshot()

    assert latest.num_rows == 2
    assert latest.column("instrument_id").to_pylist() == ["IF2606", "IF2607"]
    assert latest.column("last_price").to_pylist() == [4102.5, 4104.5]
    assert snapshot["active_segment_control"]["segment_id"] == 1
    assert snapshot["active_segment_control"]["generation"] == 0
    assert snapshot["active_segment_control"]["writer_epoch"] >= 1
    assert snapshot["active_segment_control"]["descriptor_generation"] == 1
    assert snapshot["active_segment_control"]["capacity_rows"] == 16
    assert snapshot["active_segment_control"]["row_count"] == 2
    assert snapshot["active_segment_control"]["committed_row_count"] == 2
    assert snapshot["active_segment_control"]["notify_seq"] == 2
    assert snapshot["active_segment_control"]["waiter_count"] == 0
    assert snapshot["active_segment_control"]["sealed"] is False

    if reset_default_master is not None:
        reset_default_master()
    server.stop()


def test_subscribe_uses_default_master_and_invokes_callback_with_live_row(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("subscriber_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    received: list[zippy.Row] = []
    ready = threading.Event()

    def on_tick(row: zippy.Row) -> None:
        received.append(row)
        ready.set()

    subscriber = zippy.subscribe(
        source="openctp_ticks",
        callback=on_tick,
        poll_interval_ms=1,
    )
    writer.append_tick(1777017600000000000, "IF2606", 4102.5)

    try:
        assert ready.wait(timeout=2.0)
        assert isinstance(received[-1], zippy.Row)
        assert received[-1]["instrument_id"] == "IF2606"
        assert received[-1]["last_price"] == 4102.5
        assert received[-1].to_dict()["last_price"] == 4102.5
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_subscribe_resumes_after_writer_process_restart(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    subscriber_client = zippy.connect(
        uri=control_endpoint,
        app="subscriber_restart_reader",
    )
    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_process_id = writer_client.register_process("segment_writer_1")
    writer_client.register_stream("restart_ticks", tick_schema, 64, 4096)
    writer_client.register_source("restart_source", "segment_test", "restart_ticks", {})

    writer = zippy._internal._SegmentTestWriter("restart_ticks", tick_schema, row_capacity=16)
    writer_client.publish_segment_descriptor("restart_ticks", writer.descriptor())

    received: list[dict[str, object]] = []
    first_ready = threading.Event()
    resumed_ready = threading.Event()

    def on_tick(row: zippy.Row) -> None:
        values = row.to_dict()
        received.append(values)
        if values["last_price"] == 4102.5:
            first_ready.set()
        if values["last_price"] == 4103.5:
            resumed_ready.set()

    subscriber = zippy.subscribe(
        source="restart_ticks",
        callback=on_tick,
        poll_interval_ms=1,
    )
    assert subscriber.metrics()["source"] == "restart_ticks"
    assert subscriber.metrics()["running"] is True

    try:
        writer.append_tick(1777017600000000000, "IF2606", 4102.5)
        assert first_ready.wait(timeout=2.0)
        assert subscriber.metrics()["rows_delivered_total"] == 1
        assert subscriber.metrics()["current_descriptor_generation"] == 1

        response = expire_process_for_test(control_endpoint, writer_process_id)
        assert response == {"HeartbeatAccepted": {"process_id": writer_process_id}}
        assert subscriber_client.get_stream("restart_ticks")["status"] == "stale"

        restarted_writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
        restarted_writer_client.register_process("segment_writer_2")
        restarted_writer_client.register_source(
            "restart_source",
            "segment_test",
            "restart_ticks",
            {},
        )
        restarted_writer = zippy._internal._SegmentTestWriter(
            "restart_ticks",
            tick_schema,
            row_capacity=16,
        )
        restarted_writer_client.publish_segment_descriptor(
            "restart_ticks",
            restarted_writer.descriptor(),
        )
        restarted_writer.append_tick(1777017601000000000, "IF2606", 4103.5)

        assert resumed_ready.wait(timeout=2.0)
        assert subscriber_client.get_stream("restart_ticks")["status"] == "registered"
        metrics = subscriber.metrics()
        assert metrics["rows_delivered_total"] == 2
        assert metrics["descriptor_updates_total"] >= 1
        assert metrics["current_descriptor_generation"] == 2
        assert metrics["last_descriptor_update_ns"] > 0
        latest = zippy.read_table("restart_ticks", master=subscriber_client).tail(1)
        assert latest.column("last_price").to_pylist() == [4103.5]
        assert [row["last_price"] for row in received] == [4102.5, 4103.5]
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_subscriber_metrics_expose_hybrid_mmap_wait_counters(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("hybrid_wait_writer")
    client.register_stream("hybrid_wait_ticks", tick_schema, 64, 4096)
    client.register_source("hybrid_wait_source", "segment_test", "hybrid_wait_ticks", {})
    writer = zippy._internal._SegmentTestWriter(
        "hybrid_wait_ticks",
        tick_schema,
        row_capacity=16,
    )
    client.publish_segment_descriptor("hybrid_wait_ticks", writer.descriptor())

    subscriber = zippy.subscribe(
        source="hybrid_wait_ticks",
        callback=lambda row: None,
        poll_interval_ms=20,
        idle_spin_checks=8,
    )

    try:
        deadline = time.time() + 1.0
        metrics = subscriber.metrics()
        while metrics.get("mmap_futex_waits_total", 0) == 0 and time.time() < deadline:
            time.sleep(0.01)
            metrics = subscriber.metrics()

        assert metrics["idle_spin_checks"] == 8
        assert metrics["mmap_spin_checks_total"] > 0
        assert metrics["mmap_futex_waits_total"] > 0
        assert metrics["mmap_futex_notifications_total"] >= 0
        assert metrics["mmap_futex_timeouts_total"] >= 0
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_subscribe_switches_from_sealed_segment_when_descriptor_arrives(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("rollover_writer")
    client.register_stream("rollover_ticks", tick_schema, 64, 4096)
    client.register_source("rollover_source", "segment_test", "rollover_ticks", {})
    writer = zippy._internal._SegmentTestWriter(
        "rollover_ticks",
        tick_schema,
        row_capacity=4,
    )
    client.publish_segment_descriptor("rollover_ticks", writer.descriptor())

    received: list[tuple[float, str]] = []
    first_ready = threading.Event()
    second_ready = threading.Event()

    def on_tick(row: zippy.Row) -> None:
        values = row.to_dict()
        instrument_id = str(values["instrument_id"])
        received.append((time.perf_counter(), instrument_id))
        if instrument_id == "IF2606":
            first_ready.set()
        if instrument_id == "IF2607":
            second_ready.set()

    subscriber = zippy.subscribe(
        source="rollover_ticks",
        callback=on_tick,
        poll_interval_ms=500,
    )

    try:
        writer.append_tick(1777017600000000000, "IF2606", 4102.5)
        assert first_ready.wait(timeout=2.0)

        writer.rollover()
        time.sleep(0.05)
        descriptor_published_at = time.perf_counter()
        client.publish_segment_descriptor("rollover_ticks", writer.descriptor())
        writer.append_tick(1777017601000000000, "IF2607", 4103.5)

        assert second_ready.wait(timeout=0.2)
        second_received_at = next(ts for ts, instrument in received if instrument == "IF2607")
        assert second_received_at - descriptor_published_at < 0.15
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_subscribe_filters_instrument_ids_before_row_callback(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("subscriber_filter_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    received: list[str] = []
    matched = threading.Event()
    unexpected = threading.Event()

    def on_tick(row: zippy.Row) -> None:
        instrument_id = str(row["instrument_id"])
        received.append(instrument_id)
        if instrument_id == "IF2606":
            matched.set()
        else:
            unexpected.set()

    subscriber = zippy.subscribe(
        source="openctp_ticks",
        callback=on_tick,
        poll_interval_ms=1,
        instrument_ids=["IF2606"],
    )

    try:
        writer.append_tick(1777017600000000000, "IF2607", 4104.5)
        assert not unexpected.wait(timeout=0.1)

        writer.append_tick(1777017601000000000, "IF2606", 4102.5)
        assert matched.wait(timeout=2.0)
        assert not unexpected.is_set()
        assert received == ["IF2606"]
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_subscribe_table_uses_default_master_and_invokes_callback_with_live_table(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    client = zippy.connect(uri=control_endpoint)
    client.register_process("subscriber_test")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    received: list[pa.Table] = []
    ready = threading.Event()

    def on_table(table: pa.Table) -> None:
        received.append(table)
        ready.set()

    subscriber = zippy.subscribe_table(
        source="openctp_ticks",
        callback=on_table,
        poll_interval_ms=1,
    )
    writer.append_tick(1777017600000000000, "IF2606", 4102.5)

    try:
        assert ready.wait(timeout=2.0)
        assert received[-1].schema == tick_schema
        assert received[-1].column("instrument_id").to_pylist() == ["IF2606"]
        assert received[-1].column("last_price").to_pylist() == [4102.5]
    finally:
        subscriber.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_pipeline_stream_table_publishes_descriptor_for_query_tail(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    zippy.connect(uri=control_endpoint)
    pipeline = (
        zippy.Pipeline("test_ingest").stream_table("openctp_ticks", schema=tick_schema).start()
    )
    pipeline.write(
        {
            "dt": [1777017600000000000, 1777017601000000000],
            "instrument_id": ["IF2606", "IF2607"],
            "last_price": [4102.5, 4104.5],
        }
    )

    try:
        latest = zippy.read_table("openctp_ticks").tail(10)
        assert latest.num_rows == 2
        assert latest.column("instrument_id").to_pylist() == ["IF2606", "IF2607"]
        assert latest.column("last_price").to_pylist() == [4102.5, 4104.5]
    finally:
        pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_reactive_latest_stream_table_tail_returns_key_value_snapshot(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    zippy.connect(uri=control_endpoint)
    engine = zippy.ReactiveLatestEngine(
        name="latest_by_instrument",
        input_schema=tick_schema,
        by="instrument_id",
        target=zippy.NullPublisher(),
    )
    session = (
        zippy.Session(name="latest_session", master=zippy._DEFAULT_MASTER)
        .engine(engine)
        .stream_table("ctp_ticks_latest", persist=False)
    )

    try:
        session.run()
        engine.write(
            {
                "instrument_id": ["IF2606", "IH2606"],
                "last_price": [4102.5, 2711.0],
            }
        )
        engine.write(
            {
                "instrument_id": ["IF2606"],
                "last_price": [4103.5],
            }
        )

        latest = zippy.read_table("ctp_ticks_latest").tail(10)
        deadline = time.time() + 2.0
        while (
            latest.num_rows != 2 or latest.column("last_price").to_pylist() != [4103.5, 2711.0]
        ) and time.time() < deadline:
            time.sleep(0.02)
            latest = zippy.read_table("ctp_ticks_latest").tail(10)

        assert latest.num_rows == 2
        assert latest.column("instrument_id").to_pylist() == ["IF2606", "IH2606"]
        assert latest.column("last_price").to_pylist() == [4103.5, 2711.0]
    finally:
        session.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_key_value_table_materializer_uses_materializer_name() -> None:
    assert hasattr(zippy._internal, "KeyValueTableMaterializer")
    assert not hasattr(zippy._internal, "KeyValueTableEngine")


def test_session_reactive_latest_passes_replacement_retention_snapshots(monkeypatch) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    captured_materializer: dict[str, object] = {}

    class FakeMaster:
        def process_id(self) -> str:
            return "proc_test"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 16,
                    "replacement_retention_snapshots": 3,
                    "persist": {
                        "enabled": False,
                        "partition": {},
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            stream_schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: dict[str, object],
        ) -> None:
            return None

        def publish_segment_descriptor(
            self,
            stream_name: str,
            descriptor: dict[str, object],
        ) -> None:
            return None

        def get_stream(self, stream_name: str) -> dict[str, object]:
            return {"segment_reader_leases": []}

    class CapturingKeyValueTableMaterializer:
        def __init__(self, **kwargs) -> None:
            captured_materializer.update(kwargs)

        def active_descriptor(self) -> dict[str, object]:
            return {}

    class FakeLatestEngine:
        def __init__(self) -> None:
            self._status = "created"

        def output_schema(self) -> pa.Schema:
            return schema

        def config(self) -> dict[str, object]:
            return {
                "engine_type": "reactive_latest",
                "by": ["instrument_id"],
            }

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    monkeypatch.setattr(
        zippy,
        "_KeyValueTableMaterializer",
        CapturingKeyValueTableMaterializer,
    )

    engine = FakeLatestEngine()
    zippy.Session(name="latest_session", master=FakeMaster()).engine(engine).stream_table(
        "ctp_ticks_latest",
        persist=False,
    )

    assert captured_materializer["replacement_retention_snapshots"] == 3
    assert callable(captured_materializer["retention_guard"])


def test_session_materializer_source_registration_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class CapturingKeyValueTableMaterializer:
        def __init__(self, **kwargs) -> None:
            self._status = "created"

        def active_descriptor(self) -> dict[str, object]:
            return {}

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    class FakeLatestEngine:
        def __init__(self) -> None:
            self._status = "created"

        def output_schema(self) -> pa.Schema:
            return schema

        def config(self) -> dict[str, object]:
            return {
                "engine_type": "reactive_latest",
                "by": ["instrument_id"],
            }

        def start(self) -> None:
            self._status = "running"

        def stop(self) -> None:
            self._status = "stopped"

        def status(self) -> str:
            return self._status

    monkeypatch.setattr(
        zippy,
        "_KeyValueTableMaterializer",
        CapturingKeyValueTableMaterializer,
    )

    zippy.connect(uri=control_endpoint)

    def build_session() -> zippy.Session:
        engine = FakeLatestEngine()
        return (
            zippy.Session(
                name="latest_session",
                master=zippy._DEFAULT_MASTER,
            )
            .engine(engine)
            .stream_table("ctp_ticks_latest", persist=False)
        )

    first_session = None
    second_session = None
    try:
        first_session = build_session()
        first_session.stop()
        second_session = build_session()
    finally:
        if second_session is not None:
            second_session.stop()
        if first_session is not None:
            first_session.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_reactive_latest_tail_stays_non_empty_during_snapshot_replace(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    zippy.connect(uri=control_endpoint)
    engine = zippy.ReactiveLatestEngine(
        name="latest_by_instrument",
        input_schema=tick_schema,
        by="instrument_id",
        target=zippy.NullPublisher(),
    )
    session = (
        zippy.Session(name="latest_session", master=zippy._DEFAULT_MASTER)
        .engine(engine)
        .stream_table("ctp_ticks_latest", persist=False)
    )

    try:
        session.run()
        engine.write(
            {
                "instrument_id": ["IF2606", "IH2606"],
                "last_price": [4102.5, 2711.0],
            }
        )
        latest = zippy.read_table("ctp_ticks_latest").tail(10)
        deadline = time.time() + 2.0
        while latest.num_rows == 0 and time.time() < deadline:
            time.sleep(0.01)
            latest = zippy.read_table("ctp_ticks_latest").tail(10)
        assert latest.num_rows == 2

        writer_done = threading.Event()
        writer_error: list[BaseException] = []
        empty_reads: list[pa.Table] = []

        def write_updates() -> None:
            try:
                for index in range(200):
                    engine.write(
                        {
                            "instrument_id": ["IF2606", "IH2606"],
                            "last_price": [4103.0 + index, 2712.0 + index],
                        }
                    )
                    time.sleep(0.001)
            except BaseException as error:
                writer_error.append(error)
            finally:
                writer_done.set()

        writer = threading.Thread(target=write_updates)
        writer.start()

        while not writer_done.wait(timeout=0.0):
            latest = zippy.read_table("ctp_ticks_latest").tail(10)
            if latest.num_rows == 0:
                empty_reads.append(latest)
                break
            time.sleep(0.001)

        writer.join(timeout=2.0)
        assert not writer.is_alive()
        assert writer_error == []
        assert empty_reads == []
        assert zippy.read_table("ctp_ticks_latest").tail(10).num_rows == 2
    finally:
        session.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_reactive_latest_named_segment_source_starts_from_live_tail(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_client.register_process("segment_writer")
    writer_client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    writer_client.register_source("openctp_md", "segment_test", "openctp_ticks", {})
    writer = zippy._internal._SegmentTestWriter("openctp_ticks", tick_schema, row_capacity=16)
    writer.append_tick(1777017600000000000, "IF2606", 4102.5)
    writer.append_tick(1777017601000000000, "IF2606", 4103.0)
    writer.append_tick(1777017602000000000, "IF2607", 4104.5)
    writer_client.publish_segment_descriptor("openctp_ticks", writer.descriptor())

    output_client = zippy.MasterClient(control_endpoint=control_endpoint)
    output_client.register_process("latest_reader")
    output_client.register_stream("latest_ticks", tick_schema, 64, 4096)
    output_reader = output_client.read_from("latest_ticks")

    engine_client = zippy.MasterClient(control_endpoint=control_endpoint)
    engine_client.register_process("latest_engine")
    engine = zippy.ReactiveLatestEngine(
        name="latest_named_segment",
        by="instrument_id",
        source="openctp_ticks",
        master=engine_client,
        target=zippy.BusStreamTarget(stream_name="latest_ticks", master=output_client),
    )

    engine_started = False
    try:
        engine.start()
        engine_started = True

        with pytest.raises(RuntimeError, match="reader timed out"):
            output_reader.read(timeout_ms=100)

        writer.append_tick(1777017603000000000, "IF2606", 4105.0)
        received = output_reader.read(timeout_ms=1000)

        assert received.schema == tick_schema
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("last_price").to_pylist() == [4105.0]
    finally:
        output_reader.close()
        if engine_started:
            engine.stop()
        server.stop()


def test_session_timeseries_named_segment_source_materializes_output_table(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    writer = None
    session = None

    try:
        zippy.connect(uri=control_endpoint)
        writer = (
            zippy.Pipeline("named_timeseries_ingest", master=zippy._DEFAULT_MASTER)
            .stream_table(
                "named_ticks",
                schema=tick_schema,
                dt_column="dt",
                id_column="symbol",
                dt_part="%Y%m",
                row_capacity=16,
                persist=None,
            )
            .start()
        )
        session = (
            zippy.Session("named_timeseries_session", master=zippy._DEFAULT_MASTER)
            .engine(
                zippy.TimeSeriesEngine,
                name="named_bars",
                input_schema=tick_schema,
                id_column="symbol",
                dt_column="dt",
                window=zippy.Duration.minutes(1),
                late_data_policy=zippy.LateDataPolicy.REJECT,
                factors=[zippy.AGG_LAST(column="price", output="close")],
                source="named_ticks",
            )
            .stream_table("named_bars", persist=False)
            .run()
        )

        bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
        writer.write(
            {
                "symbol": ["A", "A", "B", "A", "B"],
                "dt": [
                    bucket_start,
                    bucket_start.replace(second=10),
                    bucket_start.replace(second=20),
                    bucket_start.replace(minute=31),
                    bucket_start.replace(minute=31, second=1),
                ],
                "price": [10.0, 12.0, 20.0, 13.0, 21.0],
            }
        )
        writer.flush()

        bars = zippy.read_table("named_bars").tail(10)
        deadline = time.time() + 2.0
        while bars.num_rows != 2 and time.time() < deadline:
            time.sleep(0.02)
            bars = zippy.read_table("named_bars").tail(10)

        assert bars.column_names == ["symbol", "window_start", "window_end", "close"]
        assert bars.column("symbol").to_pylist() == ["A", "B"]
        assert bars.column("close").to_pylist() == [12.0, 20.0]
    finally:
        if session is not None:
            session.stop()
        if writer is not None:
            writer.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_session_cross_sectional_named_segment_source_materializes_output_table(
    tmp_path: Path,
) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    returns_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    writer = None
    session = None

    try:
        zippy.connect(uri=control_endpoint)
        writer = (
            zippy.Pipeline("named_cross_sectional_ingest", master=zippy._DEFAULT_MASTER)
            .stream_table(
                "named_returns",
                schema=returns_schema,
                dt_column="dt",
                id_column="symbol",
                dt_part="%Y%m",
                row_capacity=16,
                persist=None,
            )
            .start()
        )
        session = (
            zippy.Session("named_cross_sectional_session", master=zippy._DEFAULT_MASTER)
            .engine(
                zippy.CrossSectionalEngine,
                name="named_return_ranks",
                input_schema=returns_schema,
                id_column="symbol",
                dt_column="dt",
                trigger_interval=zippy.Duration.minutes(1),
                late_data_policy=zippy.LateDataPolicy.REJECT,
                factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
                source="named_returns",
            )
            .stream_table("named_return_ranks", persist=False)
            .run()
        )

        bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
        writer.write(
            {
                "symbol": ["B", "A", "C", "A"],
                "dt": [
                    bucket_start.replace(second=2),
                    bucket_start.replace(second=1),
                    bucket_start.replace(second=3),
                    bucket_start.replace(minute=31),
                ],
                "ret_1m": [0.02, 0.01, 0.03, 0.04],
            }
        )
        writer.flush()

        ranks = zippy.read_table("named_return_ranks").tail(10)
        deadline = time.time() + 2.0
        while ranks.num_rows != 3 and time.time() < deadline:
            time.sleep(0.02)
            ranks = zippy.read_table("named_return_ranks").tail(10)

        assert ranks.column_names == ["symbol", "dt", "ret_rank"]
        assert ranks.column("symbol").to_pylist() == ["A", "B", "C"]
        assert ranks.column("dt").to_pylist() == [bucket_start, bucket_start, bucket_start]
        assert ranks.column("ret_rank").to_pylist() == pytest.approx([1.0, 2.0, 3.0])
    finally:
        if session is not None:
            session.stop()
        if writer is not None:
            writer.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_pipeline_source_stream_table_lifecycle_feeds_query_tail(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class OneShotHandle:
        def __init__(self, sink) -> None:
            self.sink = sink
            self.stopped = threading.Event()
            self.thread = threading.Thread(target=self._run)
            self.thread.start()

        def _run(self) -> None:
            self.sink.emit_hello("openctp_ticks")
            self.sink.emit_data(
                {
                    "dt": [1777017600000000000],
                    "instrument_id": ["IF2606"],
                    "last_price": [4102.5],
                }
            )
            self.stopped.wait(timeout=5.0)

        def stop(self) -> None:
            self.stopped.set()

        def join(self) -> None:
            self.thread.join(timeout=5.0)

    class OneShotSource:
        def __init__(self) -> None:
            self.handle: OneShotHandle | None = None

        def _zippy_output_schema(self) -> pa.Schema:
            return tick_schema

        def _zippy_source_name(self) -> str:
            return "mock_openctp"

        def _zippy_start(self, sink) -> OneShotHandle:
            self.handle = OneShotHandle(sink)
            return self.handle

    zippy.connect(uri=control_endpoint)
    source = OneShotSource()
    pipeline = (
        zippy.Pipeline("test_ingest")
        .source(source)
        .stream_table("openctp_ticks", schema=tick_schema)
        .start()
    )

    try:
        deadline = time.time() + 2.0
        latest = zippy.read_table("openctp_ticks").tail(10)
        while latest.num_rows == 0 and time.time() < deadline:
            time.sleep(0.01)
            latest = zippy.read_table("openctp_ticks").tail(10)

        assert latest.num_rows == 1
        assert latest.column("instrument_id").to_pylist() == ["IF2606"]
        assert latest.column("last_price").to_pylist() == [4102.5]
    finally:
        pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()

    assert source.handle is not None
    assert source.handle.stopped.is_set()


def test_pipeline_stream_table_infers_schema_from_python_source(tmp_path: Path) -> None:
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    class IdleHandle:
        def __init__(self) -> None:
            self.stopped = threading.Event()

        def stop(self) -> None:
            self.stopped.set()

        def join(self) -> None:
            self.stopped.wait(timeout=5.0)

    class SchemaOnlySource:
        def _zippy_output_schema(self) -> pa.Schema:
            return tick_schema

        def _zippy_source_name(self) -> str:
            return "mock_openctp"

        def _zippy_start(self, sink) -> IdleHandle:
            sink.emit_hello("openctp_ticks")
            return IdleHandle()

    zippy.connect(uri=control_endpoint)
    pipeline = (
        zippy.Pipeline("test_ingest")
        .source(SchemaOnlySource())
        .stream_table("openctp_ticks")
        .start()
    )

    try:
        stream = zippy.master().get_stream("openctp_ticks")
        assert stream["schema"]["fields"][0]["name"] == "dt"
        assert stream["schema"]["fields"][1]["name"] == "instrument_id"
        assert stream["schema"]["fields"][2]["name"] == "last_price"
    finally:
        pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_pipeline_run_forever_stops_pipeline_on_keyboard_interrupt(monkeypatch) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def __init__(self) -> None:
            self.process = None

        def process_id(self) -> str | None:
            return self.process

        def register_process(self, app: str) -> None:
            self.process = app

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def unregister_source(self, source_name: str) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            self.started = False
            self.stopped = False
            created["engine"] = self

        def active_descriptor(self) -> dict[str, object]:
            return {}

        def start(self) -> None:
            self.started = True

        def stop(self) -> None:
            self.stopped = True

    sleep_intervals = []

    def interrupting_sleep(interval: float) -> None:
        sleep_intervals.append(interval)
        raise KeyboardInterrupt

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)
    monkeypatch.setattr(zippy.time, "sleep", interrupting_sleep)

    pipeline = zippy.Pipeline("test_ingest", master=FakeMaster()).stream_table(
        "openctp_ticks",
        schema=tick_schema,
    )

    assert pipeline.run_forever(poll_interval_sec=0.25) is pipeline
    engine = created["engine"]
    assert engine.started is True
    assert engine.stopped is True
    assert sleep_intervals == [0.25]


def test_pipeline_source_uses_python_source_control_plane_metadata() -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])

    class FakeMaster:
        def __init__(self) -> None:
            self.source_records: list[tuple[str, str, str, object]] = []

        def process_id(self) -> str | None:
            return "proc_1"

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            self.source_records.append((source_name, source_type, output_stream, config))

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class MetadataSource:
        def _zippy_output_schema(self) -> pa.Schema:
            return tick_schema

        def _zippy_source_name(self) -> str:
            return "openctp_md"

        def _zippy_source_type(self) -> str:
            return "openctp"

        def _zippy_start(self, sink) -> object:
            raise RuntimeError("metadata test does not start source")

    master = FakeMaster()
    zippy.Pipeline("test_ingest", master=master).source(MetadataSource()).stream_table(
        "openctp_ticks"
    )

    assert master.source_records == [
        ("openctp_md", "openctp", "openctp_ticks", {}),
    ]


def test_pipeline_stream_table_persist_publishes_master_metadata(
    monkeypatch,
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def __init__(self) -> None:
            self.persisted_files: list[tuple[str, dict[str, object]]] = []
            self.persist_events: list[tuple[str, dict[str, object]]] = []

        def process_id(self) -> str | None:
            return "proc_1"

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

        def publish_persisted_file(
            self,
            stream_name: str,
            persisted_file: dict[str, object],
        ) -> None:
            self.persisted_files.append((stream_name, persisted_file))

        def publish_persist_event(
            self,
            stream_name: str,
            persist_event: dict[str, object],
        ) -> None:
            self.persist_events.append((stream_name, persist_event))

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            created["args"] = args
            created["kwargs"] = kwargs

        def active_descriptor(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)
    master = FakeMaster()

    zippy.Pipeline("test_ingest", master=master).stream_table(
        "openctp_ticks",
        schema=tick_schema,
        persist_path=zippy.ParquetPersist(tmp_path / "ctp_ticks"),
    )

    kwargs = created["kwargs"]
    assert kwargs["persist_path"] == str(tmp_path / "ctp_ticks")
    assert callable(kwargs["persist_publisher"])
    assert callable(kwargs["retention_guard"])

    kwargs["persist_publisher"](
        json.dumps(
            {
                "file_path": str(tmp_path / "ctp_ticks" / "part.parquet"),
                "row_count": 1,
            }
        ).encode("utf-8")
    )

    assert master.persisted_files == [
        (
            "openctp_ticks",
            {
                "file_path": str(tmp_path / "ctp_ticks" / "part.parquet"),
                "row_count": 1,
            },
        )
    ]

    kwargs["persist_publisher"](
        json.dumps(
            {
                "persist_event_type": "persist_failed",
                "source_segment_id": 1,
                "error": "publisher failed",
            }
        ).encode("utf-8")
    )

    assert master.persist_events == [
        (
            "openctp_ticks",
            {
                "persist_event_type": "persist_failed",
                "source_segment_id": 1,
                "error": "publisher failed",
            },
        )
    ]


def test_pipeline_stream_table_uses_master_config_defaults(
    monkeypatch,
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def process_id(self) -> str | None:
            return "proc_1"

        def get_config(self) -> dict[str, object]:
            return {
                "log": {"level": "info"},
                "table": {
                    "row_capacity": 2048,
                    "retention_segments": 4,
                    "persist": {
                        "enabled": True,
                        "method": "parquet",
                        "data_dir": str(tmp_path),
                        "partition": {
                            "dt_column": "dt",
                            "id_column": "instrument_id",
                            "dt_part": "%Y%m",
                        },
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

        def publish_persisted_file(self, stream_name: str, persisted_file: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            created["kwargs"] = kwargs

        def active_descriptor(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)

    zippy.Pipeline("test_ingest", master=FakeMaster()).stream_table(
        "openctp_ticks",
        schema=tick_schema,
    )

    kwargs = created["kwargs"]
    assert kwargs["row_capacity"] == 2048
    assert kwargs["retention_segments"] == 4
    assert kwargs["persist_path"] == str(tmp_path / "openctp_ticks")
    assert kwargs["dt_column"] == "dt"
    assert kwargs["id_column"] == "instrument_id"
    assert kwargs["dt_part"] == "%Y%m"
    assert callable(kwargs["persist_publisher"])
    assert callable(kwargs["retention_guard"])


def test_pipeline_stream_table_explicit_persist_none_overrides_master_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def process_id(self) -> str | None:
            return "proc_1"

        def get_config(self) -> dict[str, object]:
            return {
                "table": {
                    "row_capacity": 2048,
                    "persist": {
                        "enabled": True,
                        "method": "parquet",
                        "data_dir": str(tmp_path),
                    },
                },
            }

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            created["kwargs"] = kwargs

        def active_descriptor(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)

    zippy.Pipeline("test_ingest", master=FakeMaster()).stream_table(
        "openctp_ticks",
        schema=tick_schema,
        persist=None,
    )

    kwargs = created["kwargs"]
    assert kwargs["row_capacity"] == 2048
    assert kwargs["persist_path"] is None
    assert kwargs["persist_publisher"] is None


def test_pipeline_stream_table_disables_default_descriptor_forwarding_on_windows(
    monkeypatch,
) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def process_id(self) -> str | None:
            return "proc_1"

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            created["kwargs"] = kwargs

        def active_descriptor(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)
    monkeypatch.setattr(zippy.os, "name", "nt")

    zippy.Pipeline("test_ingest", master=FakeMaster()).stream_table(
        "openctp_ticks",
        schema=tick_schema,
        persist=None,
    )

    assert created["kwargs"]["descriptor_forwarding"] is False


def test_pipeline_stream_table_rejects_legacy_partition_dt_part_format() -> None:
    with pytest.raises(ValueError, match="dt_part"):
        zippy.Pipeline("test_ingest", master=object()).stream_table(
            "openctp_ticks",
            schema=pa.schema([("dt", pa.timestamp("ns", tz="UTC"))]),
            dt_column="dt",
            dt_part="YYYYMM",
        )


def test_pipeline_stream_table_passes_retention_segments_to_engine(monkeypatch) -> None:
    tick_schema = pa.schema([("instrument_id", pa.string())])
    created: dict[str, object] = {}

    class FakeMaster:
        def process_id(self) -> str | None:
            return "proc_1"

        def register_stream(
            self,
            stream_name: str,
            schema: pa.Schema,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            return None

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            return None

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            return None

    class FakeStreamTableMaterializer:
        def __init__(self, *args, **kwargs) -> None:
            created["kwargs"] = kwargs

        def active_descriptor(self) -> dict[str, object]:
            return {}

    monkeypatch.setattr(zippy, "_StreamTableMaterializer", FakeStreamTableMaterializer)

    zippy.Pipeline("test_ingest", master=FakeMaster()).stream_table(
        "openctp_ticks",
        schema=tick_schema,
        retention_segments=2,
    )

    assert created["kwargs"]["retention_segments"] == 2


def test_stream_table_engine_persists_rollover_parquet_without_master(
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    engine = zippy._internal.StreamTableMaterializer(
        name="openctp_ticks",
        input_schema=tick_schema,
        target=zippy.NullPublisher(),
        row_capacity=2,
        persist_path=str(tmp_path),
    )

    engine.start()
    try:
        engine.write(
            {
                "instrument_id": ["IF2606", "IF2607", "IF2608"],
                "dt": [
                    1777017600000000000,
                    1777017601000000000,
                    1777017602000000000,
                ],
                "last_price": [4102.5, 4104.5, 4108.0],
            }
        )
    finally:
        engine.stop()

    parquet_files = list(tmp_path.glob("*.parquet"))
    assert len(parquet_files) == 1
    archived = pq.read_table(parquet_files[0])
    assert archived.num_rows == 2
    assert archived.column("instrument_id").to_pylist() == ["IF2606", "IF2607"]


def test_master_client_heartbeat_keeps_registered_process_alive(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    process_id = client.register_process("worker_a")
    client.heartbeat()

    assert process_id.startswith("proc_")

    server.stop()


def test_master_client_registers_control_plane_entities_and_updates_status(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("openctp_worker")
    client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    client.register_stream("openctp_mid_price_factors", tick_schema, 64, 4096)
    client.register_source(
        "openctp_md",
        "openctp",
        "openctp_ticks",
        {
            "front": "tcp://127.0.0.1:12345",
            "instruments": ["IF2606"],
        },
    )
    client.register_engine(
        "mid_price_factor",
        "reactive",
        "openctp_ticks",
        "openctp_mid_price_factors",
        ["factor_sink"],
        {
            "id_filter": ["IF2606"],
        },
    )
    client.register_sink(
        "factor_sink",
        "parquet",
        "openctp_mid_price_factors",
        {
            "path": "data/openctp_mid_price_factors",
        },
    )
    client.update_status(
        "engine",
        "mid_price_factor",
        "running",
        {
            "rows": 12,
        },
    )

    server.stop()


def test_reactive_engine_can_consume_master_bus_stream_and_publish_to_bus(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    factor_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
            ("price_x2", pa.float64()),
        ]
    )

    input_client = zippy.MasterClient(control_endpoint=control_endpoint)
    input_client.register_process("tick_writer")
    input_client.register_stream("openctp_ticks", tick_schema, 64, 4096)
    upstream = zippy._internal.StreamTableMaterializer(
        name="stream_table_writer",
        input_schema=tick_schema,
        target=zippy.BusStreamTarget(stream_name="openctp_ticks", master=input_client),
    )

    factor_client = zippy.MasterClient(control_endpoint=control_endpoint)
    factor_client.register_process("reactive_engine")
    factor_client.register_stream("openctp_factor_ticks", factor_schema, 64, 4096)

    output_client = zippy.MasterClient(control_endpoint=control_endpoint)
    output_client.register_process("factor_reader")
    output_reader = output_client.read_from("openctp_factor_ticks")

    engine = zippy.ReactiveStateEngine(
        name="reactive_bus",
        input_schema=tick_schema,
        id_column="instrument_id",
        factors=[zippy.Expr(expression="last_price * 2.0", output="price_x2")],
        source=zippy.BusStreamSource(
            stream_name="openctp_ticks",
            expected_schema=tick_schema,
            master=factor_client,
            mode=zippy.SourceMode.PIPELINE,
            xfast=True,
        ),
        target=zippy.BusStreamTarget(
            stream_name="openctp_factor_ticks",
            master=factor_client,
        ),
    )

    try:
        engine.start()
        upstream.start()
        upstream.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606"],
                    "last_price": [4102.5],
                }
            )
        )
        received = output_reader.read(timeout_ms=1000)

        assert received.schema == factor_schema
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("price_x2").to_pylist() == [8205.0]
    finally:
        output_reader.close()
        upstream.stop()
        engine.stop()
        server.stop()


def test_stream_table_engine_accepts_segment_stream_source() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint="/tmp/zippy-segment-source-test.sock")
    source = zippy.SegmentStreamSource(
        stream_name="openctp_ticks",
        expected_schema=schema,
        master=master,
        mode=zippy.SourceMode.PIPELINE,
        xfast=True,
    )

    engine = zippy._internal.StreamTableMaterializer(
        name="segment_table",
        input_schema=schema,
        source=source,
        target=zippy.NullPublisher(),
    )

    assert engine.config()["source_linked"] is True


def test_stream_table_engine_consumes_segment_stream_source_live_cross_process(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    output_client = zippy.MasterClient(control_endpoint=control_endpoint)
    output_client.register_process("segment_table_writer")
    output_client.register_stream("segment_table_ticks", tick_schema, 64, 4096)

    reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_client.register_process("segment_table_reader")
    output_reader = reader_client.read_from("segment_table_ticks")

    writer_code = """
import sys

import pyarrow as pa

import zippy

control_endpoint = sys.argv[1]
schema = pa.schema(
    [
        ("dt", pa.timestamp("ns", tz="UTC")),
        ("instrument_id", pa.string()),
        ("last_price", pa.float64()),
    ]
)

client = zippy.MasterClient(control_endpoint=control_endpoint)
client.register_process("segment_writer")
client.register_stream("openctp_ticks", schema, 64, 4096)
client.register_source("openctp_md", "segment_test", "openctp_ticks", {})

writer = zippy._internal._SegmentTestWriter("openctp_ticks", schema, row_capacity=16)
client.publish_segment_descriptor("openctp_ticks", writer.descriptor())
print("ready", flush=True)

if sys.stdin.readline().strip() != "go":
    raise RuntimeError("expected go command")

writer.append_tick(1777017600000000000, "IF2606", 4102.5)
print("written", flush=True)

sys.stdin.readline()
"""
    writer = subprocess.Popen(
        [sys.executable, "-c", writer_code, control_endpoint],
        cwd=WORKSPACE_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    engine_client = zippy.MasterClient(control_endpoint=control_endpoint)
    engine_client.register_process("segment_table_engine")
    source = zippy.SegmentStreamSource(
        stream_name="openctp_ticks",
        expected_schema=tick_schema,
        master=engine_client,
        mode=zippy.SourceMode.PIPELINE,
        xfast=True,
    )
    engine = zippy._internal.StreamTableMaterializer(
        name="segment_table_live",
        input_schema=tick_schema,
        source=source,
        target=zippy.BusStreamTarget(
            stream_name="segment_table_ticks",
            master=output_client,
        ),
    )

    engine_started = False
    try:
        assert writer.stdout is not None
        assert writer.stdin is not None
        ready = writer.stdout.readline().strip()
        if ready != "ready":
            stdout, stderr = writer.communicate(timeout=5)
            raise AssertionError(
                f"writer did not publish descriptor stdout={stdout} stderr={stderr}"
            )

        engine.start()
        engine_started = True
        writer.stdin.write("go\n")
        writer.stdin.flush()
        written = writer.stdout.readline().strip()
        if written != "written":
            stdout, stderr = writer.communicate(timeout=5)
            raise AssertionError(f"writer did not append row stdout={stdout} stderr={stderr}")

        received = output_reader.read(timeout_ms=2000)

        assert received.schema == tick_schema
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("last_price").to_pylist() == [4102.5]
    finally:
        if writer.stdin is not None and writer.poll() is None:
            writer.stdin.write("stop\n")
            writer.stdin.flush()
        try:
            writer.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            writer.kill()
            writer.communicate(timeout=5)
        output_reader.close()
        if engine_started:
            engine.stop()
        server.stop()


def test_stream_table_engine_rejects_segment_stream_source_schema_mismatch() -> None:
    source_schema = pa.schema([("instrument_id", pa.string())])
    input_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    master = zippy.MasterClient(control_endpoint="/tmp/zippy-segment-source-test.sock")
    source = zippy.SegmentStreamSource(
        stream_name="openctp_ticks",
        expected_schema=source_schema,
        master=master,
    )

    with pytest.raises(ValueError, match="source output schema must match downstream input_schema"):
        zippy._internal.StreamTableMaterializer(
            name="segment_table_mismatch",
            input_schema=input_schema,
            source=source,
            target=zippy.NullPublisher(),
        )


def test_bus_source_start_failure_keeps_engine_retryable(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reactive_reader")
    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")

    engine = zippy.ReactiveStateEngine(
        name="reactive_bus_retry",
        source=zippy.BusStreamSource(
            stream_name="ticks",
            expected_schema=tick_schema,
            master=reader_master,
            mode=zippy.SourceMode.PIPELINE,
            xfast=True,
        ),
        input_schema=tick_schema,
        id_column="instrument_id",
        factors=[zippy.Expr(expression="last_price * 2.0", output="price_x2")],
        target=zippy.NullPublisher(),
    )

    try:
        with pytest.raises(RuntimeError, match="stream not found"):
            engine.start()

        writer_master.register_stream("ticks", tick_schema, 64, 4096)

        engine.start()
        engine.stop()
    finally:
        if engine.status() == "running":
            engine.stop()
        server.stop()
