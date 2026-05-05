"""
从远端客户端逐行写入 WSL/Linux 中的 named stream。

典型用法是在 Windows 侧行情源进程中运行：

1. 先连接远端 master，例如 ``zippy://wsl-host:17690/default``；
2. master config 下发 GatewayServer endpoint/token；
3. ``zp.get_writer()`` 自动返回远端 writer；
4. 用户继续按行 ``write(dict)``，底层自动批量化成 Arrow IPC。

:example:

    uv run python examples/08_remote_gateway/02_remote_writer.py \
        --uri zippy://127.0.0.1:17690/default \
        --stream qmt_ticks
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import time

import pyarrow as pa

import zippy as zp


def build_schema() -> pa.Schema:
    """
    构造远端行情写入示例 schema。

    :returns: tick schema。
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


def main() -> None:
    """
    写入一组模拟 tick。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy 远端逐行写入示例")
    parser.add_argument("--uri", required=True, help="远端 master URI")
    parser.add_argument("--stream", default="qmt_ticks", help="目标 named stream")
    parser.add_argument("--rows", type=int, default=10, help="写入行数")
    parser.add_argument("--batch-size", type=int, default=4, help="客户端批量发送行数")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="remote_writer_example")
    writer = zp.get_writer(args.stream, schema=build_schema(), batch_size=args.batch_size)
    try:
        for index in range(args.rows):
            writer.write(
                {
                    "instrument_id": "IF2606",
                    "dt": datetime.now(timezone.utc),
                    "last_price": 4100.0 + index * 0.2,
                    "volume": index + 1,
                }
            )
            time.sleep(0.01)
        writer.flush()
    finally:
        writer.close()


if __name__ == "__main__":
    main()
