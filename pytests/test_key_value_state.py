"""Protocol-v2 bootstrap and fail-closed consumer contracts."""

import pyarrow as pa
import pytest

import zippy

from state_fixtures import changelog_schema


def snapshot():
    schema = pa.schema(
        [
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("price", pa.int64(), nullable=False),
        ],
        metadata={
            "zippy_kv_protocol": "2",
            "zippy_kv_seq": "0",
            "zippy_kv_version": "0",
            "zippy_kv_stream_id": "test-stream",
            "zippy_kv_keys": '["symbol"]',
            "source": "state-fixture",
        },
    )
    return pa.Table.from_arrays(
        [pa.array([], pa.string()), pa.array([], pa.int64())], schema=schema
    )


def delta():
    schema = changelog_schema(snapshot().schema.remove_metadata())
    return pa.Table.from_arrays(
        [
            pa.array([1, 2]),
            pa.array(["upsert", "upsert"]),
            pa.array([1, 1]),
            pa.array([2, 2]),
            pa.array(["test-stream", "test-stream"]),
            pa.array(["A", "B"]),
            pa.array([10, 20]),
        ],
        schema=schema,
    )


def test_state_fragments_are_atomic_and_gap_requires_rebuild():
    state = zippy.KeyValueState(snapshot())
    assert state.apply(delta().slice(0, 1)) == 0
    assert state.snapshot().num_rows == 0
    assert state.apply(delta().slice(1, 1)) == 1
    assert state.sequence == 2
    assert state.snapshot().schema.metadata[b"source"] == b"state-fixture"
    assert state.apply(delta()) == 0
    assert state.snapshot().column("price").to_pylist() == [10, 20]
    assert state.select([["B"], ["A"]]).column("price").to_pylist() == [20, 10]
    with pytest.raises(RuntimeError, match="absent"):
        state.select([["missing"]])
    broken = zippy.KeyValueState(snapshot())
    with pytest.raises(RuntimeError, match="gap"):
        broken.apply(delta().slice(1, 1))
    with pytest.raises(RuntimeError, match="fresh snapshot"):
        broken.snapshot()


def test_state_deletes_commit_after_complete_version_and_reinsert():
    state = zippy.KeyValueState(snapshot())
    state.apply(delta())
    schema = delta().schema
    deleted = pa.Table.from_arrays(
        [
            pa.array([3, 4]),
            pa.array(["delete", "delete"]),
            pa.array([2, 2]),
            pa.array([4, 4]),
            pa.array(["test-stream", "test-stream"]),
            pa.array(["A", "absent"]),
            pa.array([0, 0]),
        ],
        schema=schema,
    )
    assert state.apply(deleted.slice(0, 1)) == 0
    assert state.snapshot().column("symbol").to_pylist() == ["A", "B"]
    assert state.apply(deleted.slice(1, 1)) == 1
    assert state.snapshot().column("symbol").to_pylist() == ["B"]
    assert state.apply(deleted) == 0
    inserted = pa.Table.from_arrays(
        [
            pa.array([5]),
            pa.array(["upsert"]),
            pa.array([3]),
            pa.array([5]),
            pa.array(["test-stream"]),
            pa.array(["A"]),
            pa.array([99]),
        ],
        schema=schema,
    )
    assert state.apply(inserted) == 1
    assert state.snapshot().column("price").to_pylist() == [99, 20]
