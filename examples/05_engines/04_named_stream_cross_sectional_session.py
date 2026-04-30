"""
演示 named StreamTable 直接驱动 CrossSectionalEngine。

这个例子展示的链路是：

```
named_returns -> CrossSectionalEngine -> named_return_ranks -> read_table()
```

在实盘因子系统里，这类链路常用于把一批同一时间桶内的合约收益率、价差或指标转换成
截面排名、zscore 或 demean 结果，并把结果继续作为普通 named table 供策略查询。
"""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timezone
import time

import pyarrow as pa

import zippy as zp


def build_returns_schema() -> pa.Schema:
    """
    构造示例分钟收益率 schema。

    :returns: 输入 named table 的 Arrow schema。
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )


def main() -> None:
    """
    创建输入表、启动截面 engine、写入样例数据并查询输出表。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="default", help="zippy master uri")
    parser.add_argument("--input", default="example.named_returns", help="input table")
    parser.add_argument("--output", default="example.named_return_ranks", help="output table")
    parser.add_argument("--drop-existing", action="store_true", help="先删除同名示例表")
    args = parser.parse_args()

    master = zp.connect(uri=args.uri, app="named_cross_sectional_example")
    if args.drop_existing:
        for table_name in (args.output, args.input):
            try:
                zp.ops.drop_table(table_name, drop_persisted=True, master=master)
            except RuntimeError:
                pass

    schema = build_returns_schema()
    writer = (
        zp.Pipeline("named_cross_sectional_ingest", master=master)
        .stream_table(
            args.input,
            schema=schema,
            dt_column="dt",
            id_column="symbol",
            dt_part="%Y%m",
            row_capacity=16,
            persist=None,
        )
        .start()
    )
    session = (
        zp.Session("named_cross_sectional_session", master=master)
        .engine(
            zp.CrossSectionalEngine,
            name="return_rank_engine",
            input_schema=schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zp.Duration.minutes(1),
            late_data_policy=zp.LateDataPolicy.REJECT,
            factors=[zp.CS_RANK(column="ret_1m", output="ret_rank")],
            source=args.input,
        )
        .stream_table(args.output, persist=False)
        .run()
    )
    try:
        bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
        # CrossSectionalEngine 按时间桶输出。写入下一个桶的第一行用于触发前一个桶落地。
        writer.write(
            {
                "symbol": ["B", "A", "C", "A"],
                "dt": [
                    bucket_start.replace(second=2),
                    bucket_start.replace(second=1),
                    bucket_start.replace(second=3),
                    bucket_start.replace(minute=31),
                ],
                "ret_1m": [0.02, 0.01, 0.03, 0.04],
            }
        )
        writer.flush()

        table = zp.read_table(args.output, master=master)
        result = table.tail(10)
        deadline = time.monotonic() + 2.0
        while result.num_rows < 3 and time.monotonic() < deadline:
            time.sleep(0.02)
            result = table.tail(10)

        print(result)
    finally:
        session.stop()
        writer.stop()


if __name__ == "__main__":
    main()
