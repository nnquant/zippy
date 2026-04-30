"""
用 Pipeline 创建一张最小 StreamTable，并写入几行示例 tick。

:example:

    uv run python examples/01_quickstart/02_create_stream_table_with_pipeline.py \
        --drop-existing
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa

import zippy as zp


def tick_schema() -> pa.Schema:
    """
    返回示例 tick 的 Arrow schema。

    :returns: tick schema
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )


def make_ticks() -> pl.DataFrame:
    """
    构造一小批确定性的 tick 数据。

    :returns: 示例 tick
    :rtype: polars.DataFrame
    """
    return pl.DataFrame(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, 500000, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "last_price": [3898.2, 2675.4, 3898.6],
            "volume": [12, 8, 18],
        }
    )


def main() -> None:
    """
    创建表、写入数据、再通过 read_table 验证可读。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="创建最小 StreamTable")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", default="demo_ticks", help="示例表名")
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="运行前删除同名表，便于重复执行示例",
    )
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_create_stream_table")

    if args.drop_existing:
        try:
            zp.ops.drop_table(args.table, drop_persisted=True)
        except RuntimeError:
            # 表不存在时忽略，示例脚本保持可重复运行。
            pass

    pipeline = zp.Pipeline("example_create_stream_table")
    pipeline.stream_table(
        args.table,
        schema=tick_schema(),
        dt_column="dt",
        id_column="instrument_id",
        dt_part="%Y%m",
        persist=None,
    )

    try:
        pipeline.start()
        pipeline.write(make_ticks())
        pipeline.flush()
        print(zp.read_table(args.table).tail(10))
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
