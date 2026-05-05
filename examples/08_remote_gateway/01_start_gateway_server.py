"""
启动一个跨平台 GatewayServer。

这个脚本运行在 WSL/Linux 侧，负责把远端 TCP + Arrow IPC 请求转成本地 Zippy
StreamTable、subscribe_table 和 read_table 查询。Windows 侧 QMT/策略进程只需要连接
远端 master，再由 master config 发现这里启动的 GatewayServer。

:example:

    uv run python examples/08_remote_gateway/01_start_gateway_server.py \
        --uri default \
        --endpoint 0.0.0.0:17666 \
        --token dev-token
"""

from __future__ import annotations

import argparse
import signal
import threading

import zippy as zp


def main() -> None:
    """
    连接本地 master，并启动 GatewayServer 常驻服务。

    :returns: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Zippy GatewayServer 启动示例")
    parser.add_argument("--uri", default="default", help="本地 master URI")
    parser.add_argument("--endpoint", default="127.0.0.1:17666", help="Gateway 监听地址")
    parser.add_argument("--token", default=None, help="远端客户端访问 token")
    parser.add_argument(
        "--max-write-rows",
        type=int,
        default=None,
        help="单个远端 write_batch 允许的最大行数",
    )
    args = parser.parse_args()

    master = zp.connect(uri=args.uri, app="remote_gateway_server")
    gateway = zp.GatewayServer(
        endpoint=args.endpoint,
        master=master,
        token=args.token,
        max_write_rows=args.max_write_rows,
    ).start()

    stop_event = threading.Event()

    def request_stop(signum, frame) -> None:
        """
        接收 Ctrl-C 或进程停止信号后退出。

        :param signum: POSIX signal number。
        :type signum: int
        :param frame: 当前栈帧。
        :type frame: object
        """
        del signum, frame
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print("gateway started endpoint=[{}]".format(gateway.endpoint))
    print("master config should contain:")
    print("[remote_gateway]")
    print("enabled = true")
    print('endpoint = "{}"'.format(gateway.endpoint))
    print("protocol_version = 1")
    if args.token:
        print('token = "<redacted>"')

    try:
        stop_event.wait()
    finally:
        print("gateway metrics:", gateway.metrics())
        gateway.stop()


if __name__ == "__main__":
    main()
