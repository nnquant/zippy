"""
把 persisted 表回放为 named stream，并驱动 ReactiveLatestEngine。

这个示例对应不开盘系统测试的常见路径：

1. live 表开启 parquet persist，形成可回放历史数据。
2. replay 先初始化输出流，让下游 Engine 可以先 attach。
3. ReactiveLatestEngine 消费 replay stream，生成按 instrument_id 聚合的最新快照表。

:example:

    uv run python examples/06_replay/03_replay_to_reactive_latest_engine.py --drop-existing
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pyarrow as pa

import zippy as zp

DEFAULT_ROOT = Path("/tmp/zippy-examples/replay_downstream")


def tick_schema() -> pa.Schema:
    """
    返回示例 tick schema。

    :returns: Arrow schema
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )


def wait_persisted_rows(table_name: str, expected_rows: int, timeout_sec: float = 2.0) -> None:
    """
    等待 master 能看到指定数量的 persisted rows。

    :param table_name: live 表名
    :type table_name: str
    :param expected_rows: 期望已落盘行数
    :type expected_rows: int
    :param timeout_sec: 最长等待秒数
    :type timeout_sec: float
    :returns: None
    :rtype: None
    :raises TimeoutError: 如果等待超时
    """
    deadline = time.time() + timeout_sec
    query = zp.read_table(table_name)
    while time.time() < deadline:
        persisted_rows = sum(
            int(item.get("row_count", 0))
            for item in query.persisted_files()
        )
        if persisted_rows >= expected_rows:
            return
        time.sleep(0.02)
    raise TimeoutError(f"persisted rows not ready table=[{table_name}]")


def main() -> None:
    """
    运行 live persist -> replay stream -> latest engine 的完整链路。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="replay 驱动 ReactiveLatestEngine 示例")
    parser.add_argument("--uri", default="default", help="master URI")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="示例数据目录")
    parser.add_argument("--live-table", default="example_live_ticks", help="原始 live 表")
    parser.add_argument("--replay-stream", default="example_replay_ticks", help="回放输出流")
    parser.add_argument("--latest-table", default="example_replay_latest", help="latest 输出表")
    parser.add_argument("--replay-rate", type=float, default=None, help="固定回放速率，单位 rows/sec")
    parser.add_argument("--drop-existing", action="store_true", help="运行前删除同名表")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_replay_to_latest_engine")
    root = Path(args.root)

    if args.drop_existing:
        for table_name in (args.latest_table, args.replay_stream, args.live_table):
            try:
                zp.drop_table(table_name, drop_persisted=True)
            except RuntimeError:
                pass

    live_pipeline = None
    replay = None
    latest_session = None
    try:
        live_pipeline = (
            zp.Pipeline("example_live_persist_for_replay")
            .stream_table(
                args.live_table,
                schema=tick_schema(),
                row_capacity=2,
                persist="parquet",
                data_dir=root / "persisted",
            )
            .start()
        )
        live_pipeline.write(
            {
                "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606", "IC2606"],
                "dt": [
                    1777017600000000000,
                    1777017600000001000,
                    1777017600000002000,
                    1777017600000003000,
                    1777017600000004000,
                ],
                "last_price": [3898.0, 2675.0, 3899.0, 2676.0, 6123.0],
            }
        )
        wait_persisted_rows(args.live_table, expected_rows=4)

        # init() 只注册 replay 输出流，不发数据；这样下游 Engine 可以先启动。
        replay = zp.TableReplayEngine(
            args.live_table,
            output_stream=args.replay_stream,
            batch_size=1,
            row_capacity=16,
            replay_rate=args.replay_rate,
        ).init()
        latest_session = (
            zp.Session("example_replay_latest_session")
            .engine(
                zp.ReactiveLatestEngine,
                name="example_replay_latest_engine",
                source=args.replay_stream,
                by="instrument_id",
            )
            .stream_table(args.latest_table, persist=False)
            .run()
        )

        replay.run()

        latest = zp.read_table(args.latest_table).tail(10)
        print(latest)
    finally:
        if latest_session is not None:
            latest_session.stop()
        if replay is not None:
            replay.stop()
        if live_pipeline is not None:
            live_pipeline.stop()


if __name__ == "__main__":
    main()
