"""
进程内演示 ReactiveStateEngine -> TimeSeriesEngine -> CrossSectionalEngine。

这个示例不依赖 master，适合理解引擎之间的 source/target 级联关系。

:example:

    uv run python examples/05_engines/02_timeseries_and_cross_sectional.py
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pyarrow as pa

import zippy as zp


def tick_schema() -> pa.Schema:
    """
    返回输入 tick schema。

    :returns: Arrow schema
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )


def make_ticks() -> pl.DataFrame:
    """
    构造两个合约、两个分钟桶的确定性行情。

    :returns: 示例 tick
    :rtype: polars.DataFrame
    """
    return pl.DataFrame(
        {
            "instrument_id": [
                "IF2606",
                "IH2606",
                "IF2606",
                "IH2606",
                "IF2606",
                "IH2606",
            ],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 20, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "last_price": [3898.2, 2675.4, 3898.6, 2675.1, 3899.1, 2676.0],
            "volume": [12.0, 8.0, 18.0, 10.0, 20.0, 12.0],
        }
    )


def main() -> None:
    """
    启动三级引擎，写入一批 tick，然后 flush 触发窗口输出。

    :returns: None
    :rtype: None
    """
    schema = tick_schema()
    reactive = zp.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="instrument_id",
        factors=[
            zp.TS_EMA(column="last_price", span=2, output="ema_2"),
            zp.TS_RETURN(column="last_price", period=1, output="ret_1"),
        ],
        target=zp.NullPublisher(),
    )

    bars = zp.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="instrument_id",
        dt_column="dt",
        window=zp.Duration.minutes(1),
        window_type=zp.WindowType.TUMBLING,
        late_data_policy=zp.LateDataPolicy.REJECT,
        pre_factors=[zp.Expr(expression="last_price * volume", output="turnover_input")],
        factors=[
            zp.AGG_FIRST(column="last_price", output="open"),
            zp.AGG_MAX(column="last_price", output="high"),
            zp.AGG_MIN(column="last_price", output="low"),
            zp.AGG_LAST(column="last_price", output="close"),
            zp.AGG_SUM(column="volume", output="volume"),
            zp.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zp.Expr(expression="close / open - 1.0", output="ret_1m"),
            zp.Expr(expression="turnover / volume", output="vwap_1m"),
        ],
        target=zp.NullPublisher(),
    )

    cross_sectional = zp.CrossSectionalEngine(
        name="cs_1m",
        source=bars,
        input_schema=bars.output_schema(),
        id_column="instrument_id",
        dt_column="window_start",
        trigger_interval=zp.Duration.minutes(1),
        late_data_policy=zp.LateDataPolicy.REJECT,
        factors=[
            zp.CS_RANK(column="ret_1m", output="ret_rank"),
            zp.CS_ZSCORE(column="ret_1m", output="ret_zscore"),
        ],
        target=zp.NullPublisher(),
    )

    cross_sectional.start()
    bars.start()
    reactive.start()

    reactive.write(make_ticks())
    reactive.flush()
    bars.flush()
    cross_sectional.flush()

    reactive.stop()
    bars.stop()
    cross_sectional.stop()

    print("reactive metrics:", reactive.metrics())
    print("bars metrics:", bars.metrics())
    print("cross_sectional metrics:", cross_sectional.metrics())


if __name__ == "__main__":
    main()

