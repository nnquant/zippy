"""
纯 Python Windows Gateway smoke client。

这个脚本用于验收 Windows 原生 Python 进程能否访问 WSL/Linux 中的 Zippy master
和 GatewayServer。它刻意不导入 ``zippy``，只依赖 ``pyarrow``，因此适合在 Windows
侧 zippy wheel 还没准备好时先验证网络、master config 下发和 Gateway 数据面协议。

示例：

    python examples/08_remote_gateway/04_standalone_windows_smoke_client.py \
        --uri zippy://127.0.0.1:17690/default \
        --stream windows_smoke_ticks \
        --json
"""

from __future__ import annotations

import argparse
import json
import socket
import struct
from typing import Any

import pyarrow as pa


REMOTE_FRAME_HEADER = struct.Struct("!IQ")


def _parse_remote_master_uri(uri: str) -> tuple[str, int]:
    """
    Parse a remote ``zippy://host:port/profile`` URI.

    :param uri: Remote zippy master URI.
    :type uri: str
    :returns: Host and port pair.
    :rtype: tuple[str, int]
    :raises ValueError: If the URI is not a remote zippy URI.
    """
    if not uri.startswith("zippy://"):
        raise ValueError("uri must use zippy://host:port/profile")
    authority = uri.removeprefix("zippy://").split("/", 1)[0]
    if ":" not in authority:
        raise ValueError("uri must include host and port")
    host, port_text = authority.rsplit(":", 1)
    return host, int(port_text)


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    value = endpoint.removeprefix("tcp://").split("/", 1)[0]
    if ":" not in value:
        raise ValueError("endpoint must include host and port")
    host, port_text = value.rsplit(":", 1)
    return host, int(port_text)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("connection closed while reading frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_gateway_frame(
    sock: socket.socket,
    header: dict[str, Any],
    payload: bytes = b"",
) -> None:
    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    sock.sendall(REMOTE_FRAME_HEADER.pack(len(header_bytes), len(payload)))
    sock.sendall(header_bytes)
    if payload:
        sock.sendall(payload)


def _recv_gateway_frame(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    prefix = _recv_exact(sock, REMOTE_FRAME_HEADER.size)
    header_len, payload_len = REMOTE_FRAME_HEADER.unpack(prefix)
    header = json.loads(_recv_exact(sock, header_len).decode("utf-8"))
    payload = _recv_exact(sock, payload_len) if payload_len else b""
    return header, payload


def _table_to_ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _table_from_ipc(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(pa.py_buffer(payload)).read_all()


def _master_get_config(uri: str, timeout_sec: float) -> dict[str, Any]:
    host, port = _parse_remote_master_uri(uri)
    with socket.create_connection((host, port), timeout=timeout_sec) as sock:
        sock.sendall(b'{"GetConfig":{}}\n')
        response_line = b""
        while not response_line.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_line += chunk
    if not response_line:
        raise RuntimeError("master returned empty response")
    response = json.loads(response_line.decode("utf-8"))
    if "Error" in response:
        raise RuntimeError(response["Error"].get("reason", "master get_config failed"))
    return dict(response["ConfigFetched"]["config"])


def _gateway_request(
    endpoint: str,
    header: dict[str, Any],
    payload: bytes = b"",
    *,
    token: str | None,
    timeout_sec: float,
) -> tuple[dict[str, Any], bytes]:
    host, port = _parse_endpoint(endpoint)
    request = dict(header)
    if token is not None:
        request["token"] = token
    with socket.create_connection((host, port), timeout=timeout_sec) as sock:
        _send_gateway_frame(sock, request, payload)
        response, response_payload = _recv_gateway_frame(sock)
    if response.get("status") != "ok":
        raise RuntimeError(response.get("reason", "gateway request failed"))
    return response, response_payload


def run_smoke(
    *,
    uri: str,
    stream_name: str,
    gateway_endpoint: str | None = None,
    token: str | None = None,
    timeout_sec: float = 5.0,
) -> dict[str, Any]:
    """
    Write one tick through GatewayServer and collect it back.

    :param uri: Remote master URI.
    :type uri: str
    :param stream_name: Stream used by the smoke probe.
    :type stream_name: str
    :param gateway_endpoint: Optional GatewayServer endpoint override.
    :type gateway_endpoint: str | None
    :param token: Optional GatewayServer token override.
    :type token: str | None
    :param timeout_sec: Socket timeout in seconds.
    :type timeout_sec: float
    :returns: Probe result.
    :rtype: dict[str, Any]
    """
    if gateway_endpoint is None:
        config = _master_get_config(uri, timeout_sec)
        remote_gateway = config.get("remote_gateway", {})
        if not isinstance(remote_gateway, dict) or not remote_gateway.get("endpoint"):
            raise RuntimeError("master config does not advertise remote_gateway.endpoint")
        gateway_endpoint = str(remote_gateway["endpoint"])
        if token is None and remote_gateway.get("token"):
            token = str(remote_gateway["token"])

    table = pa.Table.from_pylist(
        [{"instrument_id": "IF2606", "last_price": 4102.5}],
        schema=pa.schema(
            [
                ("instrument_id", pa.string()),
                ("last_price", pa.float64()),
            ]
        ),
    )
    _gateway_request(
        gateway_endpoint,
        {"kind": "write_batch", "stream_name": stream_name, "rows": table.num_rows},
        _table_to_ipc(table),
        token=token,
        timeout_sec=timeout_sec,
    )
    _, payload = _gateway_request(
        gateway_endpoint,
        {"kind": "collect", "source": stream_name, "plan": [], "snapshot": True},
        token=token,
        timeout_sec=timeout_sec,
    )
    collected = _table_from_ipc(payload)
    data = collected.to_pydict()
    return {
        "status": "ok",
        "uri": uri,
        "gateway_endpoint": gateway_endpoint,
        "stream": stream_name,
        "rows": collected.num_rows,
        "instrument_id": data.get("instrument_id", [None])[-1],
        "last_price": data.get("last_price", [None])[-1],
        "data": data,
    }


def main() -> None:
    """
    Run the standalone Gateway smoke from the command line.
    """
    parser = argparse.ArgumentParser(description="Zippy standalone Windows Gateway smoke")
    parser.add_argument("--uri", required=True, help="remote master URI, e.g. zippy://host:17690/default")
    parser.add_argument("--stream", default="windows_smoke_ticks", help="temporary stream name")
    parser.add_argument("--gateway-endpoint", default=None, help="override GatewayServer endpoint")
    parser.add_argument("--token", default=None, help="override GatewayServer token")
    parser.add_argument("--timeout-sec", type=float, default=5.0, help="socket timeout in seconds")
    parser.add_argument("--json", action="store_true", help="emit JSON output")
    args = parser.parse_args()

    result = run_smoke(
        uri=args.uri,
        stream_name=args.stream,
        gateway_endpoint=args.gateway_endpoint,
        token=args.token,
        timeout_sec=args.timeout_sec,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "standalone gateway smoke ok "
            f"stream=[{result['stream']}] rows=[{result['rows']}] "
            f"instrument_id=[{result['instrument_id']}] last_price=[{result['last_price']}]"
        )


if __name__ == "__main__":
    main()
