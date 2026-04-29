"""
用 Session 编排 ReactiveLatestEngine，并把输出物化为 StreamTable。

:example:

    uv run python examples/05_engines/01_reactive_latest_session.py \
        --source demo_ticks --output demo_ticks_latest
"""

from __future__ import annotations

import argparse
import signal
import threading

import zippy as zp


def parse_by(value: str) -> str | list[str]:
    """
    解析按哪些维度保留最新值。

    :param value: 逗号分隔字段，例如 instrument_id 或 instrument_id,exchange_id
    :type value: str
    :returns: 单字段字符串或多字段列表
    :rtype: str | list[str]
    """
    parts = [item.strip() for item in value.split(",") if item.strip()]
    if len(parts) == 1:
        return parts[0]
    return parts


def main() -> None:
    """
    启动 latest 引擎服务，直到收到 Ctrl-C 或 SIGTERM。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="ReactiveLatestEngine Session 示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--source", default="demo_ticks", help="上游 StreamTable")
    parser.add_argument("--output", default="demo_ticks_latest", help="输出 StreamTable")
    parser.add_argument("--by", default="instrument_id", help="聚合维度，多个字段用逗号分隔")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_reactive_latest_session")

    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    session = (
        zp.Session("example_reactive_latest_session")
        .engine(
            zp.ReactiveLatestEngine,
            name="latest_engine",
            source=args.source,
            by=parse_by(args.by),
        )
        # latest 表是在线快照，默认只保留当前最新值。
        # 如果需要历史数据，请对上游 StreamTable 开启 persist="parquet"。
        .stream_table(args.output, persist=False)
        .run()
    )

    print(f"latest engine is running source={args.source} output={args.output}")
    try:
        stop_event.wait()
    finally:
        session.stop()


if __name__ == "__main__":
    main()
