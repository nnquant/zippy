"""
演示 read_table、tail 和 collect 的基本语义。

:example:

    uv run python examples/03_query/01_query_tail_collect.py --table demo_ticks
"""

from __future__ import annotations

import argparse

import zippy as zp


def main() -> None:
    """
    读取一张表的最新数据和完整可查询快照。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="查询 Zippy 表")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--table", default="demo_ticks", help="表名")
    parser.add_argument("--n", type=int, default=5, help="tail 行数")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_query_tail_collect")
    query = zp.read_table(args.table)

    # tail(n) 只关心最近 N 行，底层会自动补齐 persisted/live 数据。
    latest = query.tail(args.n)
    print("tail:")
    print(latest)

    # collect() 固定一个查询边界，读取当前所有可查询数据。
    collected = query.collect()
    print("collect row_count:", collected.num_rows)
    print("schema:", query.schema())


if __name__ == "__main__":
    main()

