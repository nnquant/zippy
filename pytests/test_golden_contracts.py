import inspect
import json
from pathlib import Path
from typing import Any, cast

import zippy

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "contracts"


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


class _FixtureMaster:
    def __init__(self, streams: dict[str, dict[str, object]]) -> None:
        self._streams = streams

    def get_stream(self, source: str) -> dict[str, object]:
        return self._streams[source]

    def get_stream_status(self, source: str) -> dict[str, object]:
        return self._streams[source]


def test_table_semantics_v2_matches_publish_query_and_subscribe_contracts() -> None:
    fixture = _load_fixture("table_semantics_v2.json")
    tables = fixture["tables"]
    assert fixture["format_version"] == 2
    assert isinstance(tables, list)

    builders = {
        "append_table": lambda table: zippy._append_table_descriptor_metadata(),
        "frame_table": lambda table: zippy._frame_table_descriptor_metadata(by=table["by"]),
        "key_value_table": lambda table: zippy._key_value_table_descriptor_metadata(
            by=table["by"],
            changelog_stream_name=table["subscribe"]["source"],
        ),
    }
    streams = {}
    for table in tables:
        kind = table["kind"]
        descriptor = table["descriptor"]
        assert builders[kind](table) == descriptor
        streams[table["name"]] = {
            "stream_name": table["name"],
            "active_segment_descriptor": descriptor,
        }

        assert zippy._descriptor_table_kind(streams[table["name"]]) == kind
        assert callable(getattr(zippy.Session, table["write"]["api"]))
        assert callable(getattr(zippy, table["query"]["api"]))
        assert callable(getattr(zippy, table["subscribe"]["api"]))

    master = cast(zippy.MasterClient, _FixtureMaster(streams))
    assert (
        zippy._key_value_changelog_subscription_source(
            "latest_factors", master, wait=False, timeout=None
        )
        == "latest_factors.__kv_changelog"
    )


def test_table_semantics_v2_freezes_public_parameter_names() -> None:
    frame_parameters = inspect.signature(zippy.Session.frame_table).parameters
    key_value_parameters = inspect.signature(zippy.Session.key_value_table).parameters

    assert "by" in frame_parameters
    assert "frame_by" not in frame_parameters
    assert "by" in key_value_parameters
    assert "latest_by" not in key_value_parameters
    assert "snapshot" in inspect.signature(zippy.read_table).parameters


def test_persisted_file_v1_preserves_query_overlap_and_ordering_contract() -> None:
    persisted_file = _load_fixture("persisted_file_v1.json")

    assert zippy._persisted_segment_identities(persisted_file) == {
        (41, 0, 7),
        (42, 0, 7),
    }
    assert zippy._persisted_file_order_key(persisted_file) == (
        7,
        41,
        0,
        1776038400123,
        "/var/lib/zippy/tables/factor_events/compact-000001.parquet",
    )

    overlapping_snapshot: dict[str, object] = {
        "active_segment_descriptor": {
            "segment_id": 42,
            "generation": 0,
            "writer_epoch": 7,
        },
        "sealed_segments": [],
        "persisted_files": [persisted_file],
    }
    assert zippy._non_overlapping_persisted_files(overlapping_snapshot) == []

    next_epoch_snapshot: dict[str, object] = {
        **overlapping_snapshot,
        "active_segment_descriptor": {
            "segment_id": 42,
            "generation": 0,
            "writer_epoch": 8,
        },
    }
    assert zippy._non_overlapping_persisted_files(next_epoch_snapshot) == [persisted_file]
