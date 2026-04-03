from datetime import datetime, timezone
import time

import polars as pl
import pyarrow as pa

import zippy

TICK_ENDPOINT = "tcp://127.0.0.1:5555"
BAR_ENDPOINT = "tcp://127.0.0.1:5556"
MINUTE_NS = 60_000_000_000


def main() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.TS_RETURN(column="price", period=1, output="ret_1"),
        ],
        target=zippy.ZmqPublisher(endpoint=TICK_ENDPOINT),
    )

    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window_ns=MINUTE_NS,
        late_data_policy="reject",
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_MAX(column="price", output="high"),
            zippy.AGG_MIN(column="price", output="low"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
        ],
        target=zippy.ZmqPublisher(endpoint=BAR_ENDPOINT),
    )

    bars.start()
    reactive.start()
    time.sleep(0.2)

    tick_frame = pl.DataFrame(
        {
            "symbol": ["000001", "000001", "000001"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 10.1, 10.3],
            "volume": [100.0, 120.0, 140.0],
        }
    )

    reactive.write(tick_frame)
    reactive.flush()
    bars.flush()

    reactive.stop()
    bars.stop()

    print("published one reactive batch to", TICK_ENDPOINT)
    print("published one completed bar batch to", BAR_ENDPOINT)


if __name__ == "__main__":
    main()
