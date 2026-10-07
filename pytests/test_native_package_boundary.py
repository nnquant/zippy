"""The main package must never replace a missing native extension with a fallback."""

import subprocess
import sys

import pytest

import zippy


@pytest.mark.parametrize(
    "name",
    [
        "connect",
        "master",
        "config",
        "ops",
        "read_table",
        "get_writer",
        "Table",
        "Session",
        "SessionContext",
        "Pipeline",
        "Lifespan",
        "ParquetPersist",
        "subscribe",
        "subscribe_table",
        "subscribe_frames",
        "StreamSubscriber",
        "RemoteMasterClient",
        "RemoteGatewayWriter",
        "RemoteStreamSubscriber",
        "GatewayServer",
        "MasterClient",
        "MasterServer",
        "SegmentStreamSource",
        "_DEFAULT_MASTER",
        "_remote_gateway_plan_split",
        "_apply_query_plan_ops_to_arrow",
        "_resolve_uri",
    ],
)
def test_main_package_does_not_reintroduce_legacy_data_paths(name: str) -> None:
    assert not hasattr(zippy, name)
    assert name not in zippy.__all__


def test_main_package_does_not_hide_native_import_failure() -> None:
    child_code = """
import importlib.abc
import sys

class MissingNative(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "zippy._internal":
            raise ImportError("native load failed for contract test")

sys.meta_path.insert(0, MissingNative())
try:
    import zippy
except ImportError as error:
    assert str(error) == "native load failed for contract test"
else:
    raise AssertionError("main package silently replaced the native runtime")
"""
    result = subprocess.run(
        [sys.executable, "-c", child_code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "name",
    [
        "WindowType",
        "LateDataPolicy",
        "OverflowPolicy",
        "SourceMode",
        "NullPublisher",
        "ZmqPublisher",
        "ZmqStreamPublisher",
        "ZmqSubscriber",
        "ZmqSource",
        "MasterServer",
        "MasterClient",
        "GatewayServer",
        "Query",
        "StreamSubscriber",
        "SegmentStreamSource",
        "SegmentTestWriter",
        "ReactiveStateEngine",
        "ReactiveLatestEngine",
        "StreamTableMaterializer",
        "KeyValueTableMaterializer",
        "TimeSeriesEngine",
        "BarGeneratorEngine",
        "CrossSectionalEngine",
    ],
)
def test_native_extension_has_no_legacy_runtime_bindings(name: str) -> None:
    assert not hasattr(zippy._internal, name)
    assert not hasattr(zippy, name)
    assert name not in zippy.__all__
