from datetime import datetime, timezone
import time

import polars as pl
import pyarrow as pa

import zippy

CS_ENDPOINT = "tcp://127.0.0.1:5557"


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

    cross_sectional = zippy.CrossSectionalEngine(
        name="cs_1m",
        source=bars,
        input_schema=bars.output_schema(),
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.CS_RANK(column="ret_1m", output="ret_rank"),
            zippy.CS_ZSCORE(column="ret_1m", output="ret_zscore"),
            zippy.CS_DEMEAN(column="ret_1m", output="ret_demean"),
        ],
        target=zippy.ZmqPublisher(endpoint=CS_ENDPOINT),
    )

    cross_sectional.start()
    bars.start()
    reactive.start()
    time.sleep(1.0)

    first_tick_frame = pl.DataFrame(
        {
            "symbol": [
                "000001",
                "000002",
                "000001",
                "000002",
                "000001",
                "000002",
            ],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 20.0, 10.3, 19.8, 10.5, 20.4],
            "volume": [100.0, 80.0, 120.0, 90.0, 140.0, 95.0],
        }
    )

    second_tick_frame = pl.DataFrame(
        {
            "symbol": [
                "000001",
                "000002",
                "000001",
                "000002",
                "000001",
                "000002",
            ],
            "dt": [
                datetime(2026, 4, 2, 9, 32, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 32, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 32, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 32, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 33, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 33, 0, tzinfo=timezone.utc),
            ],
            "price": [10.7, 20.2, 10.6, 20.6, 10.9, 20.1],
            "volume": [110.0, 85.0, 125.0, 105.0, 135.0, 98.0],
        }
    )

    reactive.write(first_tick_frame)
    reactive.flush()
    bars.flush()
    cross_sectional.flush()
    time.sleep(0.5)

    reactive.write(second_tick_frame)
    reactive.flush()
    bars.flush()
    cross_sectional.flush()
    time.sleep(0.5)

    reactive.stop()
    bars.stop()
    cross_sectional.stop()

    print("published two cross-sectional batches to", CS_ENDPOINT)


if __name__ == "__main__":
    main()
