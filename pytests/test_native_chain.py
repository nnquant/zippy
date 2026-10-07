"""Exclusive native engine chains preserve state, batch order and window finalization."""

import pyarrow as pa
import pytest

from zippy import AggSumSpec, NativeProcessor
from test_native_compute import SCHEMA, batch, reactive, timeseries


def test_chain_transfers_existing_state_only_after_validating_every_link():
    first = reactive()
    assert first.process(batch([1.0]))[0].column("mean").to_pylist() == [None]
    wrong = NativeProcessor.identity("wrong", SCHEMA)
    with pytest.raises(ValueError, match="schema differs"):
        NativeProcessor.chain("bad", [first, wrong])
    assert first.process(batch([3.0]))[0].column("mean").to_pylist() == [2.0]
    assert wrong.process(batch([99.0]))
    last = NativeProcessor.latest("latest", first.output_schema(), ["symbol"])
    chain = NativeProcessor.chain("valid", [first, last])
    for transferred in (first, last):
        with pytest.raises(RuntimeError, match="closed"):
            transferred.flush()
    result = chain.process(batch([5.0]))[0]
    assert result.column("mean").to_pylist() == [4.0]
    chain.close()
    wrong.close()


def test_duplicate_closed_and_invalid_budget_owners_are_not_partially_consumed():
    first = reactive()
    with pytest.raises(RuntimeError, match="distinct"):
        NativeProcessor.chain("duplicate", [first, first])
    with pytest.raises(ValueError, match="positive"):
        NativeProcessor.chain("zero", [first], max_intermediate_bytes=0)
    with pytest.raises(ValueError, match="stages"):
        NativeProcessor.chain("empty", [])
    closed = NativeProcessor.identity("closed", first.output_schema())
    closed.close()
    with pytest.raises(RuntimeError, match="not active"):
        NativeProcessor.chain("closed", [first, closed])
    assert first.process(batch([1.0, 3.0]))[0].column("mean").to_pylist() == [None, 2.0]
    first.close()


def test_two_buffered_stages_receive_upstream_tail_before_finishing():
    first = timeseries()
    second = NativeProcessor.timeseries(
        "second",
        first.output_schema(),
        "symbol",
        "window_end",
        20_000,
        [AggSumSpec(column="total", output="sum_total")],
    )
    chain = NativeProcessor.chain("windows", [first, second])
    closed = chain.process(batch([1.0, 2.0, 3.0], dt=[0, 10_000, 20_000]))
    assert len(closed) == 1 and closed[0].column("sum_total").to_pylist() == [1.0]
    tail = chain.finish()
    assert len(tail) == 1 and tail[0].column("sum_total").to_pylist() == [2.0]
    assert chain.finish() == []
    with pytest.raises(RuntimeError, match="closed"):
        chain.flush()


def test_intermediate_budget_failure_is_terminal_for_the_complete_chain():
    first = NativeProcessor.identity("first", SCHEMA)
    last = NativeProcessor.latest("latest", SCHEMA, ["symbol"])
    chain = NativeProcessor.chain("small", [first, last], max_intermediate_bytes=1)
    with pytest.raises(RuntimeError, match="byte limit"):
        chain.process(batch([1.0]))
    for operation in (chain.flush, chain.finish, chain.transfer):
        with pytest.raises(RuntimeError, match="failed|not active"):
            operation()


def test_identity_keeps_arbitrary_schema_nulls_order_duplicates_and_buffers():
    schema = pa.schema(
        [("number", pa.int64()), ("text", pa.string())], metadata={"source": "fixture"}
    )
    data = pa.RecordBatch.from_pydict(
        {"number": [7, None, 7], "text": ["x", "y", "x"]}, schema=schema
    )
    identity = NativeProcessor.identity("raw", schema)
    result = identity.process(data)
    assert len(result) == 1 and result[0].equals(data, check_metadata=True)
    assert result[0].column(0).buffers()[1].address == data.column(0).buffers()[1].address
    assert identity.flush() == [] and identity.finish() == []
