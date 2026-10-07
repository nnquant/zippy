"""
通过原生有限源把 Parquet 回放到持久 append 表。

这个示例适合在不开盘时构造可重复的系统测试输入。

:example:

    uv run python examples/06_replay/01_parquet_replay_to_stream_table.py --namespace-id <已有命名空间 UUID>
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

import zippy as zp

DEFAULT_ROOT = Path("/tmp/zippy-examples/replay")


def tick_schema() -> pa.Schema:
    """
    返回回放数据 schema。

    :returns: Arrow schema
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


def write_demo_parquet(path: Path) -> None:
    """
    生成一份确定性的 Parquet 测试数据。

    :param path: parquet 文件路径
    :type path: pathlib.Path
    :returns: None
    :rtype: None
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "instrument_id": ["IF2606", "IH2606", "IF2606", "IH2606"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "last_price": [3898.2, 2675.4, 3898.6, 2675.1],
            "volume": [12, 8, 18, 10],
        },
        schema=tick_schema(),
    )
    pq.write_table(table, path)


def main() -> None:
    """Create a native output, replay a finite source, then query after all workers stop."""
    parser = argparse.ArgumentParser(description="原生 Parquet 回放示例")
    parser.add_argument("--uri", default="zippy://default", help="服务控制地址")
    parser.add_argument("--namespace", default="market")
    parser.add_argument("--namespace-id", type=uuid.UUID, required=True)
    parser.add_argument("--table", default="replay_ticks", help="新输出表名，不覆盖已有表")
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--end-ns", type=int)
    parser.add_argument("--replay-rate", type=float)
    args = parser.parse_args()
    parquet_path = Path(args.root) / "input" / f"ticks-{uuid.uuid4().hex}.parquet"
    write_demo_parquet(parquet_path)
    source = zp.ParquetReplaySource(
        parquet_path,
        schema=tick_schema(),
        batch_size=2,
        replay_rate=args.replay_rate,
        time_column="dt",
        start=args.start_ns,
        end=args.end_ns,
    )
    table_id = uuid.uuid4()
    with zp.Client(args.uri) as client:
        client.create_append_table(
            args.table,
            source._zippy_output_schema(),
            namespace_id=args.namespace_id,
            table_id=table_id,
            request_id=uuid.uuid4(),
        )
        with zp.ComputePipeline(
            client,
            zp.NativeProcessor.identity("replay", source._zippy_output_schema()),
            source=source,
            output=args.table,
            output_table_id=table_id,
            output_namespace=args.namespace,
            durability="persisted",
        ) as job:
            if not source.wait_replay(30):
                raise TimeoutError("parquet replay did not finish")
            job.join(2)
        print(
            client.read_table(args.table, namespace=args.namespace)
            .sort(["dt", "instrument_id"])
            .collect()
        )


if __name__ == "__main__":
    main()
