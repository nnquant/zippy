from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa

import zippy

ARCHIVE_ROOT = Path("/tmp/zippy-parquet-demo")


def main() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.TS_RETURN(column="price", period=1, output="ret_1"),
        ],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(ARCHIVE_ROOT),
            rotation="none",
            write_input=True,
            write_output=True,
        ),
    )

    engine.start()
    engine.write(
        pl.DataFrame(
            {
                "symbol": ["000001", "000001"],
                "dt": [
                    datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                    datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                ],
                "price": [10.0, 10.1],
            }
        )
    )
    engine.stop()

    print("archived input/output parquet files under", ARCHIVE_ROOT)
    for path in sorted(ARCHIVE_ROOT.rglob("*.parquet")):
        print(path)


if __name__ == "__main__":
    main()
