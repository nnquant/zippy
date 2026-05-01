"""
按行订阅一个流，每条增量数据以 zp.Row 传入回调。

:example:

    uv run python examples/04_subscribe/01_subscribe_rows.py --stream demo_ticks
"""

from __future__ import annotations

import argparse
import threading

import zippy as zp


def main() -> None:
    """
    订阅单行回调，收到 max_rows 行后退出。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy 行级订阅示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--stream", default="demo_ticks", help="订阅的 stream/table 名")
    parser.add_argument("--instrument", action="append", default=[], help="可重复传入合约过滤")
    parser.add_argument("--max-rows", type=int, default=10, help="最多接收多少行")
    parser.add_argument("--timeout-sec", type=float, default=30.0, help="最长等待时间")
    parser.add_argument("--wait-stream", action="store_true", help="stream 尚未创建时等待 producer")
    parser.add_argument("--wait-timeout", default="30s", help="等待 stream 创建的超时时间")
    parser.add_argument("--xfast", action="store_true", help="低延迟 spin 读取")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_subscribe_rows")
    done = threading.Event()
    received = 0

    def on_row(row: zp.Row) -> None:
        """
        处理一条增量数据。

        :param row: Zippy 行对象，可用 to_dict() 转为普通 dict。
        :type row: zippy.Row
        :returns: None
        :rtype: None
        """
        nonlocal received
        values = row.to_dict()
        received += 1
        print(values)
        if received >= args.max_rows:
            done.set()

    subscriber = zp.subscribe(
        args.stream,
        callback=on_row,
        instrument_ids=args.instrument or None,
        xfast=args.xfast,
        wait=args.wait_stream,
        timeout=args.wait_timeout,
    )
    try:
        done.wait(args.timeout_sec)
    finally:
        subscriber.stop()


if __name__ == "__main__":
    main()
