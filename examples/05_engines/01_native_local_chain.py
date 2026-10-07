"""Run reactive -> timeseries -> cross-sectional kernels without a server.

:example: python 01_native_local_chain.py
"""

import json

import pyarrow as pa

from zippy import AGG_LAST, CS_RANK, TS_EMA, NativeProcessor


def main() -> None:
    """Process deterministic UTC nanoseconds and finalize every buffered stage once."""
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    reactive = NativeProcessor.reactive(
        "ema", schema, "symbol", [TS_EMA(column="price", span=2, output="ema")]
    )
    bars = NativeProcessor.timeseries(
        "bars",
        reactive.output_schema(),
        "symbol",
        "dt",
        2,
        [AGG_LAST(column="ema", output="close")],
    )
    ranks = NativeProcessor.cross_sectional(
        "ranks",
        bars.output_schema(),
        "symbol",
        "window_start",
        2,
        [CS_RANK(column="close", output="rank")],
    )
    chain = NativeProcessor.chain("three_stages", [reactive, bars, ranks])
    try:
        data = pa.RecordBatch.from_pydict(
            {"symbol": ["A", "B", "A", "B"], "dt": [0, 0, 2, 2], "price": [10.0, 20.0, 12.0, 22.0]},
            schema=schema,
        )
        batches = chain.process(data) + chain.finish()
        result = pa.Table.from_batches(batches)
        assert result.num_rows == 4 and result.column("rank").to_pylist() == [1.0, 2.0, 1.0, 2.0]
        print(json.dumps({"rows": result.num_rows, "ranks": result.column("rank").to_pylist()}))
    finally:
        chain.close()


if __name__ == "__main__":
    main()
