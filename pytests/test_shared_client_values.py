"""The main package consumes the client-owned row and temporal contracts."""

from collections.abc import Mapping

import pyarrow as pa
import pytest
import zippy
from zippy_client import Client, Row, col, lit
from zippy_client.row import _table_rows


def test_main_package_uses_single_row_and_temporal_implementation() -> None:
    assert zippy.Row is Row
    assert zippy.col is col and zippy.lit is lit
    assert zippy.Client is Client
    row = zippy.Row({"symbol": "A", "price": 1.0})
    assert isinstance(row, Mapping)
    assert row.get("missing") is None and dict(row) == row.to_dict()
    copied = row.to_dict()
    copied["price"] = 2.0
    assert row["price"] == 1.0


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_replay_and_flight_rows_preserve_exact_typed_timestamp_units(unit: str) -> None:
    table = pa.table(
        {
            "dt": pa.array([-1, 0, 1, None], type=pa.timestamp(unit, tz="UTC")),
            "symbol": ["A", "B", "C", "D"],
        }
    )
    replay = []
    engine = zippy.ParquetReplayEngine(
        "unused.parquet", callback=lambda row: replay.append(row.to_dict())
    )
    engine.emit_data(table)
    assert replay == list(_table_rows(table))
    assert [row["dt"] for row in replay] == [-1, 0, 1, None]
