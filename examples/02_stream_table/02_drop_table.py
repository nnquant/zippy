"""
删除已经创建的 Zippy 表。

:example:

    uv run python examples/02_stream_table/02_drop_table.py demo_ticks
"""

from __future__ import annotations

import argparse

import zippy as zp


def main() -> None:
    """
    删除表元数据，并默认删除已登记的持久化文件。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="删除 Zippy 表")
    parser.add_argument("table", help="要删除的表名")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument(
        "--keep-persisted",
        action="store_true",
        help="只删 master 元数据，不删除 parquet 文件",
    )
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_drop_table")
    result = zp.drop_table(args.table, drop_persisted=not args.keep_persisted)
    print(result)


if __name__ == "__main__":
    main()

