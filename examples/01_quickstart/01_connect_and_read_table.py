"""
连接默认 master，并读取一张 Zippy 表的最新数据。

:example:

    uv run python examples/01_quickstart/01_connect_and_read_table.py \
        --table demo_ticks --n 10
"""

from __future__ import annotations

import argparse

import zippy as zp


def main() -> None:
    """
    读取指定表的 tail 数据。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="读取 Zippy 表的最新 N 行")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", default="demo_ticks", help="要读取的表名")
    parser.add_argument("--n", type=int, default=10, help="读取最近 N 行")
    args = parser.parse_args()

    # app 参数会注册当前 Python 进程，并启动后台 heartbeat，避免 lease 过期。
    zp.connect(uri=args.uri, app="example_read_table")

    table = zp.read_table(args.table).tail(args.n)
    print(table)


if __name__ == "__main__":
    main()

