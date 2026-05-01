"""
演示 consumer 先启动、producer 后注册 named table 的使用方式。

这个例子适合监控进程、策略 REPL 或研究脚本：它们可以先连接 master，然后用
``zippy.read_table(..., wait=True)`` 等待目标表出现。producer 稍后启动并发布
active segment descriptor 后，consumer 会得到普通的 ``zippy.Table`` 对象。
"""

from __future__ import annotations

import argparse
import threading
import time

import pyarrow as pa

import zippy as zp


def build_schema() -> pa.Schema:
    """
    构造示例 tick schema。

    :returns: 示例行情表 schema。
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )


def run_consumer(table_name: str, ready: threading.Event) -> None:
    """
    先于 producer 打开 table，并等待 producer 注册。

    :param table_name: 等待的 named table。
    :type table_name: str
    :param ready: 通知主线程 consumer 已开始等待的事件。
    :type ready: threading.Event
    """
    ready.set()
    table = zp.read_table(table_name, wait=True, timeout="5s")
    latest = table.tail(1)
    print("consumer received latest row:")
    print(latest)


def main() -> None:
    """
    启动一个本地演示：consumer 先等表，producer 延迟创建表并写入一条数据。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="default", help="zippy master uri")
    parser.add_argument("--table", default="ops.wait_ticks", help="named table")
    parser.add_argument("--delay-sec", type=float, default=0.2, help="producer 启动延迟")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="consumer_wait_for_table_example")

    ready = threading.Event()
    consumer = threading.Thread(
        target=run_consumer,
        args=(args.table, ready),
        name="zippy-consumer-wait-example",
    )
    consumer.start()
    ready.wait(timeout=1.0)

    # 这里模拟 producer 晚于 consumer 启动。真实环境中 producer 往往是另一个进程。
    time.sleep(args.delay_sec)
    pipeline = (
        zp.Pipeline("consumer_wait_demo_producer")
        .stream_table(args.table, schema=build_schema(), row_capacity=8)
        .start()
    )
    try:
        pipeline.write({"instrument_id": ["IF2606"], "last_price": [3912.5]})
        consumer.join(timeout=5.0)
        if consumer.is_alive():
            raise TimeoutError("consumer did not receive table before timeout")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
