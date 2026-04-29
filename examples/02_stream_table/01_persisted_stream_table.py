"""
创建带 Parquet 持久化和分区策略的 StreamTable。

:example:

    uv run python examples/02_stream_table/01_persisted_stream_table.py --drop-existing
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa

import zippy as zp

DEFAULT_DATA_DIR = Path("/tmp/zippy-examples/persisted")


def tick_schema() -> pa.Schema:
    """
    返回示例行情 schema。

    :returns: Arrow schema
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
    构造跨 instrument 的示例数据。

    :returns: 示例行情
    :rtype: polars.DataFrame
    """
    return pl.DataFrame(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "last_price": [3898.2, 2675.4, 3898.6, 2675.1],
            "volume": [12, 8, 18, 10],
        }
    )


def main() -> None:
    """
    写入带持久化的 StreamTable，并打印 master 记录的 parquet 文件。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="StreamTable Parquet 持久化示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--table", default="demo_ticks_persisted", help="表名")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="持久化根目录")
    parser.add_argument("--drop-existing", action="store_true", help="运行前删除同名表")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_persisted_stream_table")

    if args.drop_existing:
        try:
            zp.drop_table(args.table, drop_persisted=True)
        except RuntimeError:
            pass

    pipeline = zp.Pipeline("example_persisted_stream_table")
    pipeline.stream_table(
        args.table,
        schema=tick_schema(),
        dt_column="dt",
        id_column="instrument_id",
        dt_part="%Y%m",
        persist="parquet",
        data_dir=args.data_dir,
    )

    try:
        pipeline.start()
        pipeline.write(make_ticks())
        pipeline.flush()
    finally:
        pipeline.stop()

    query = zp.read_table(args.table)
    print(query.tail(10))
    print("persisted_files:")
    for item in query.persisted_files():
        print(item)


if __name__ == "__main__":
    main()

