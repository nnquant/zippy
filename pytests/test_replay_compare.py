"""Explicit Arrow replay comparison preserves typed values and duplicate multiplicity."""

from decimal import Decimal

import pyarrow as pa
import pytest

from zippy import compare_replay


def test_comparison_uses_exact_nanoseconds_nulls_and_duplicates_across_chunks():
    schema = pa.schema([("dt", pa.timestamp("ns", tz="UTC")), ("amount", pa.decimal128(8, 2))])
    table = pa.Table.from_pydict(
        {
            "dt": [1, 1, None, 2],
            "amount": [Decimal("1.01"), Decimal("1.01"), None, Decimal("2.02")],
        },
        schema=schema,
    )
    other = pa.concat_tables([table.slice(0, 2), table.slice(2)])
    assert compare_replay(table, other)["equal"] is True
    changed = pa.concat_tables([table.slice(0, 3), table.slice(2, 1)])
    result = compare_replay(table, changed)
    assert result["missing_rows"] == result["extra_rows"] == 1
    assert not result["equal"]


def test_comparison_keeps_order_significant_unless_sort_is_explicit():
    left, right = pa.table({"x": [1, 2]}), pa.table({"x": [2, 1]})
    result = compare_replay(left, right)
    assert result["equal"] is False and result["mismatch_rows"] == 0
    assert compare_replay(left, right, by="x")["equal"] is True


def test_comparison_ignores_descriptive_metadata_but_rejects_different_types():
    left = pa.table({"x": [1, 1]}, metadata={"source": "original"})
    right = pa.table({"x": [1, 2]}, metadata={"source": "replay"})
    right = right.cast(pa.schema([pa.field("x", pa.int64(), metadata={"source": "replay"})]))
    assert compare_replay(left, right)["missing_rows"] == 1
    other = pa.table({"x": ["1", "1"]})
    result = compare_replay(left, other)
    assert not result["schema_equal"] and result["missing_rows"] == 2


def test_comparison_requires_collected_tables_and_existing_sort_keys():
    table = pa.table({"x": [1]})
    with pytest.raises(TypeError, match="explicit Arrow"):
        compare_replay("implicit_table_name", table)
    for keys in ([], ["missing"]):
        with pytest.raises(ValueError, match="keys"):
            compare_replay(table, table, by=keys)
