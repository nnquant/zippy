"""
按批订阅一个流，每次回调接收 pyarrow.Table。

:example:

    uv run python examples/04_subscribe/02_subscribe_table.py --stream demo_ticks
"""

from __future__ import annotations

import argparse
import threading

import pyarrow as pa

import zippy as zp


def main() -> None:
    """
    订阅表级批量回调，适合用 Arrow/Polars 做批处理过滤。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy 表级订阅示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--stream", default="demo_ticks", help="订阅的 stream/table 名")
    parser.add_argument("--max-rows", type=int, default=100, help="累计接收多少行后退出")
    parser.add_argument("--timeout-sec", type=float, default=30.0, help="最长等待时间")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_subscribe_table")
    done = threading.Event()
    received = 0

    def on_table(table: pa.Table) -> None:
        """
        处理一批增量数据。

        :param table: 增量 Arrow 表
        :type table: pyarrow.Table
        :returns: None
        :rtype: None
        """
        nonlocal received
        received += table.num_rows
        print(f"batch rows={table.num_rows} columns={table.column_names}")
        if received >= args.max_rows:
            done.set()

    subscriber = zp.subscribe_table(args.stream, callback=on_table)
    try:
        done.wait(args.timeout_sec)
    finally:
        subscriber.stop()


if __name__ == "__main__":
    main()

