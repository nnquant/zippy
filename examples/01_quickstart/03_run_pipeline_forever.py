"""
用 ``Pipeline.run_forever()`` 运行一个持续写入的最小 StreamTable。

这个示例模拟实盘 source 的生命周期：source 在后台线程持续产生 tick，
``Pipeline`` 负责启动 source、物化 StreamTable，并在 Ctrl-C 后统一停止。

:example:

    uv run python examples/01_quickstart/03_run_pipeline_forever.py --drop-existing
"""

from __future__ import annotations

import argparse
import threading
import time
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa

import zippy as zp


def tick_schema() -> pa.Schema:
    """
    返回合成 tick 的 Arrow schema。

    :returns: tick schema。
    :rtype: pyarrow.Schema
    """
    return pa.schema(
        [
            ("seq", pa.int64()),
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("localtime_ns", pa.int64()),
            ("last_price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )


class SyntheticTickHandle:
    """
    管理合成 tick source 的后台线程。

    :param source: 合成 tick source。
    :type source: SyntheticTickSource
    :param sink: Zippy source sink。
    :type sink: object
    """

    def __init__(self, source: "SyntheticTickSource", sink) -> None:
        self._source = source
        self._sink = sink
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"synthetic-tick-source-{source.source_name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """
        请求后台线程停止。

        :returns: None
        :rtype: None
        """
        self._stop_event.set()

    def join(self) -> None:
        """
        等待后台线程退出。

        :returns: None
        :rtype: None
        """
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        try:
            self._sink.emit_hello(self._source.source_name)
            seq = 0
            while not self._stop_event.is_set():
                self._sink.emit_data(self._source.make_tick(seq, time.time_ns()))
                seq += 1
                time.sleep(self._source.interval_ms / 1000.0)
            self._sink.emit_flush()
            self._sink.emit_stop()
        except Exception as error:
            self._sink.emit_error(str(error))


class SyntheticTickSource:
    """
    一个用于演示 ``Pipeline.run_forever()`` 的 Python source。

    :param source_name: 注册到控制面的 source 名称。
    :type source_name: str
    :param instrument_id: 合成行情的 instrument_id。
    :type instrument_id: str
    :param interval_ms: 两条 tick 之间的生成间隔，单位毫秒。
    :type interval_ms: float
    """

    def __init__(self, source_name: str, instrument_id: str, interval_ms: float) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms must be positive")
        self.source_name = source_name
        self.instrument_id = instrument_id
        self.interval_ms = interval_ms

    def _zippy_output_schema(self) -> pa.Schema:
        """
        返回 source 输出 schema，供 ``Pipeline`` 自动推导 StreamTable schema。

        :returns: tick schema。
        :rtype: pyarrow.Schema
        """
        return tick_schema()

    def _zippy_source_name(self) -> str:
        """
        返回控制面 source 名称。

        :returns: source 名称。
        :rtype: str
        """
        return self.source_name

    def _zippy_start(self, sink) -> SyntheticTickHandle:
        """
        启动后台 source 线程。

        :param sink: Zippy source sink。
        :type sink: object
        :returns: source runtime handle。
        :rtype: SyntheticTickHandle
        """
        return SyntheticTickHandle(self, sink)

    def make_tick(self, seq: int, localtime_ns: int) -> pl.DataFrame:
        """
        构造单行 tick。

        :param seq: 自增序号。
        :type seq: int
        :param localtime_ns: 本地写入时间戳，单位纳秒。
        :type localtime_ns: int
        :returns: 单行 Polars DataFrame。
        :rtype: polars.DataFrame
        """
        return pl.DataFrame(
            {
                "seq": [seq],
                "instrument_id": [self.instrument_id],
                "dt": [datetime.fromtimestamp(localtime_ns / 1_000_000_000, tz=timezone.utc)],
                "localtime_ns": [localtime_ns],
                "last_price": [4100.0 + (seq % 100) * 0.2],
                "volume": [seq],
            }
        )


def main() -> None:
    """
    启动持续写入的 Pipeline。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="持续运行一个最小 Zippy Pipeline")
    parser.add_argument("--uri", default="default", help="master URI，默认 zippy://default")
    parser.add_argument("--table", default="demo_ticks_live", help="输出 StreamTable 名称")
    parser.add_argument("--instrument-id", default="IF2606", help="合成 tick 合约")
    parser.add_argument("--interval-ms", type=float, default=1000.0, help="tick 生成间隔")
    parser.add_argument("--row-capacity", type=int, default=1024, help="active segment 行容量")
    parser.add_argument("--drop-existing", action="store_true", help="运行前删除同名表")
    args = parser.parse_args()

    zp.connect(uri=args.uri, app="example_pipeline_run_forever")

    if args.drop_existing:
        try:
            zp.ops.drop_table(args.table, drop_persisted=True)
        except RuntimeError:
            pass

    source = SyntheticTickSource(
        source_name="example_synthetic_ticks",
        instrument_id=args.instrument_id,
        interval_ms=args.interval_ms,
    )

    print(f"running pipeline table=[{args.table}] instrument_id=[{args.instrument_id}]")
    print("press Ctrl-C to stop")
    (
        zp.Pipeline("example_pipeline_run_forever")
        .source(source)
        .stream_table(
            args.table,
            dt_column="dt",
            id_column="instrument_id",
            dt_part="%Y%m",
            persist=None,
            row_capacity=args.row_capacity,
        )
        .run_forever()
    )


if __name__ == "__main__":
    main()
