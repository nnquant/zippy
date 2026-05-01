"""
用 named StreamTable 直接驱动 TimeSeriesEngine，并把输出物化为 StreamTable。

这个示例展示 M7 推荐用法：下游引擎直接写 ``source="demo_ticks"``，由 Zippy
内部向 master 查询 schema 和 segment descriptor，用户不需要手动创建
``SegmentStreamSource``。

:example:

    uv run python examples/05_engines/03_named_stream_timeseries_session.py \
        --source demo_ticks --output demo_ticks_1m
"""

from __future__ import annotations

import argparse
import signal
import threading

import pyarrow as pa

import zippy as zp


def tick_schema() -> pa.Schema:
    """
    返回示例 tick schema。

    :returns: 输入 tick 的 Arrow schema。
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


def main() -> None:
    """
    启动 named stream -> TimeSeriesEngine -> output StreamTable 链路。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Named StreamTable 驱动 TimeSeriesEngine 示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--source", default="demo_ticks", help="上游 StreamTable")
    parser.add_argument("--output", default="demo_ticks_1m", help="输出 1m bar StreamTable")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_named_stream_timeseries")

    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    session = (
        zp.Session("example_named_stream_timeseries")
        .engine(
            zp.TimeSeriesEngine,
            name="bar_1m_engine",
            input_schema=tick_schema(),
            source=args.source,
            id_column="instrument_id",
            dt_column="dt",
            window=zp.Duration.minutes(1),
            window_type=zp.WindowType.TUMBLING,
            late_data_policy=zp.LateDataPolicy.REJECT,
            factors=[
                zp.AGG_FIRST(column="last_price", output="open"),
                zp.AGG_MAX(column="last_price", output="high"),
                zp.AGG_MIN(column="last_price", output="low"),
                zp.AGG_LAST(column="last_price", output="close"),
                zp.AGG_SUM(column="volume", output="volume"),
            ],
        )
        .stream_table(args.output, persist=False)
        .run()
    )

    print(f"timeseries engine is running source={args.source} output={args.output}")
    try:
        stop_event.wait()
    finally:
        session.stop()


if __name__ == "__main__":
    main()
