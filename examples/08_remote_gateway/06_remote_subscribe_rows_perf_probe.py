"""
测量远端 GatewayServer ``subscribe`` row callback 的交付情况。

这个脚本面向低频 row callback 场景。高吞吐场景仍建议优先使用
``05_remote_subscribe_perf_probe.py`` 或 ``zippy gateway subscribe-perf`` 的
``subscribe_table`` 路径。

:example:

    uv run python examples/08_remote_gateway/06_remote_subscribe_rows_perf_probe.py \\
        --uri zippy://127.0.0.1:17690/default \\
        --stream qmt_ticks \\
        --rows 20 \\
        --instrument-id IF2606
"""

from __future__ import annotations

import argparse
import json

from zippy.remote_gateway_perf import run_remote_subscribe_rows_perf_probe


def run_probe(args: argparse.Namespace) -> dict[str, object]:
    """
    Run the remote row subscribe performance probe.

    :param args: Command-line arguments.
    :type args: argparse.Namespace
    :returns: Probe report.
    :rtype: dict[str, object]
    """
    return run_remote_subscribe_rows_perf_probe(
        uri=args.uri,
        stream=args.stream,
        rows=args.rows,
        timeout_sec=args.timeout_sec,
        instrument_id=args.instrument_id,
        app="remote_subscribe_rows_perf_probe",
    )


def main() -> None:
    """
    Parse CLI arguments and print a JSON report.

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="测量远端 GatewayServer subscribe row 交付")
    parser.add_argument("--uri", required=True, help="远端 master URI")
    parser.add_argument("--stream", default="qmt_ticks", help="订阅的 named stream")
    parser.add_argument("--rows", type=int, default=20, help="收到至少 N 行后结束")
    parser.add_argument("--timeout-sec", type=float, default=10.0, help="等待接收完成的超时时间")
    parser.add_argument("--instrument-id", default=None, help="可选 instrument_id 过滤")
    args = parser.parse_args()

    print(json.dumps(run_probe(args), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
