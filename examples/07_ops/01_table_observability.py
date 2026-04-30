"""
查看 master 中注册的 Zippy 表状态。

这个示例用于实盘或系统测试时快速确认：

1. master 当前知道哪些表。
2. 某张表的 schema、status、descriptor_generation 和 persist 状态。

:example:

    uv run python examples/07_ops/01_table_observability.py --table ctp_ticks
"""

from __future__ import annotations

import argparse
import json

import zippy as zp


def main() -> None:
    """
    输出表列表和指定表元数据。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="查看 Zippy 表可观测信息")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", default=None, help="可选：查看指定表详情")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_table_observability")

    tables = zp.ops.list_tables()
    print(json.dumps(tables, ensure_ascii=False, indent=2))

    if args.table is not None:
        table = zp.ops.table_info(args.table)
        print(json.dumps(table, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
