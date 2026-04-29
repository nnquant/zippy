"""
从 Parquet 回放数据到 StreamTable。

这个示例适合在不开盘时构造可重复的系统测试输入。

:example:

    uv run python examples/06_replay/01_parquet_replay_to_stream_table.py --drop-existing
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq

import zippy as zp

DEFAULT_ROOT = Path("/tmp/zippy-examples/replay")


def tick_schema() -> pa.Schema:
    """
    返回回放数据 schema。

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


def write_demo_parquet(path: Path) -> None:
    """
    生成一份确定性的 Parquet 测试数据。

    :param path: parquet 文件路径
    :type path: pathlib.Path
    :returns: None
    :rtype: None
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
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
        },
        schema=tick_schema(),
    )
    pq.write_table(table, path)


def main() -> None:
    """
    生成 parquet，回放到 StreamTable，再读取结果。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="ParquetReplaySource 示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--table", default="replay_ticks", help="输出 StreamTable")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="示例数据目录")
    parser.add_argument("--drop-existing", action="store_true", help="运行前删除同名表")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_parquet_replay")
    root = Path(args.root)
    parquet_path = root / "input" / "ticks.parquet"
    write_demo_parquet(parquet_path)

    if args.drop_existing:
        try:
            zp.drop_table(args.table, drop_persisted=True)
        except RuntimeError:
            pass

    source = zp.ParquetReplaySource(
        parquet_path,
        schema=tick_schema(),
        batch_size=2,
        source_name="example_parquet_replay_source",
    )
    pipeline = (
        zp.Pipeline("example_parquet_replay")
        .source(source)
        .stream_table(
            args.table,
            dt_column="dt",
            id_column="instrument_id",
            dt_part="%Y%m",
            persist=None,
        )
    )

    try:
        pipeline.start()
        # 当前回放模式是 as_fast_as_possible，等待后台 source 线程发完小文件即可。
        time.sleep(0.5)
        print(zp.read_table(args.table).tail(10))
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()

