"""
从远端客户端订阅和查询 WSL/Linux 中的 named stream。

这个脚本适合 Windows 侧策略或监控程序：订阅走 GatewayServer table callback，
主动查询走 ``read_table().collect()``，查询计划会发送到 WSL/Linux 侧执行后返回
Arrow IPC，而不是把全量数据先拉到 Windows 再过滤。

:example:

    uv run python examples/08_remote_gateway/03_remote_subscribe_and_query.py \
        --uri zippy://127.0.0.1:17690/default \
        --stream qmt_ticks
"""

from __future__ import annotations

import argparse
import threading

import pyarrow as pa

import zippy as zp


def main() -> None:
    """
    先订阅增量 table callback，再执行一次主动查询。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy 远端订阅和查询示例")
    parser.add_argument("--uri", required=True, help="远端 master URI")
    parser.add_argument("--stream", default="qmt_ticks", help="读取的 named stream")
    parser.add_argument("--instrument-id", default="IF2606", help="过滤的合约代码")
    parser.add_argument("--timeout-sec", type=float, default=10.0, help="订阅等待时间")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="remote_subscribe_query_example")
    done = threading.Event()
    received: list[pa.Table] = []

    def on_table(table: pa.Table) -> None:
        """
        处理远端 GatewayServer 返回的增量 Arrow table。

        :param table: 增量数据。
        :type table: pyarrow.Table
        :returns: None
        :rtype: None
        """
        received.append(table)
        print("received remote batch rows=[{}]".format(table.num_rows))
        done.set()

    subscriber = zp.subscribe_table(
        args.stream,
        callback=on_table,
        filter=zp.col("instrument_id") == args.instrument_id,
        batch_size=1,
    )
    try:
        done.wait(args.timeout_sec)
    finally:
        subscriber.stop()

    latest = (
        zp.read_table(args.stream)
        .filter(zp.col("instrument_id") == args.instrument_id)
        .select("instrument_id", "dt", "last_price", "volume")
        .tail(5)
    )
    print(latest)


if __name__ == "__main__":
    main()
