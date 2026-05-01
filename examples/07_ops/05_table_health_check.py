"""
检查 Zippy named table 的运行健康状态。

这个示例只读取 master catalog 元数据，不 attach live segment，也不扫描 parquet 文件。
它适合放在监控脚本、REPL 或运维巡检里，用来快速发现 stream stale、active descriptor
未发布、persist_failed 等问题。

:example:

    uv run python examples/07_ops/05_table_health_check.py --table ctp_ticks
"""

from __future__ import annotations

import argparse
import json

import zippy as zp


def main() -> None:
    """
    打印单张表的 health summary 和 alerts。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy table health check")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--table", required=True, help="要检查的 named table")
    args = parser.parse_args()

    master = zp.connect(uri=args.uri, app="example_table_health_check")
    health = zp.ops.table_health(args.table, master=master)

    print(json.dumps(health, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
