"""Bounded Gateway frame and Arrow IPC codec."""

from __future__ import annotations

import json
import os
import socket
import struct
from typing import Any, TypeGuard

from .errors import ProtocolError

_FRAME_HEADER = struct.Struct("!IQ")
_DEFAULT_MAX_HEADER_BYTES = 1024 * 1024
_DEFAULT_MAX_PAYLOAD_BYTES = 512 * 1024 * 1024
GATEWAY_PROTOCOL_VERSION = 1


def send_frame(
    sock: socket.socket,
    header: dict[str, object],
    payload: bytes = b"",
) -> None:
    """Send one Gateway protocol frame."""
    header_payload = json.dumps(header, separators=(",", ":")).encode("utf-8")
    if len(header_payload) > _max_header_bytes():
        raise ProtocolError(
            "gateway request header exceeds client budget "
            f"bytes=[{len(header_payload)}] max_bytes=[{_max_header_bytes()}]"
        )
    if len(payload) > _max_payload_bytes():
        raise ProtocolError(
            "gateway request payload exceeds client budget "
            f"bytes=[{len(payload)}] max_bytes=[{_max_payload_bytes()}]"
        )
    sock.sendall(_FRAME_HEADER.pack(len(header_payload), len(payload)))
    sock.sendall(header_payload)
    if payload:
        sock.sendall(payload)


def receive_frame(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    """Receive and validate one bounded Gateway protocol frame."""
    prefix = _receive_exact(sock, _FRAME_HEADER.size)
    header_len, payload_len = _FRAME_HEADER.unpack(prefix)
    if header_len > _max_header_bytes():
        raise ProtocolError(
            "gateway response header exceeds client budget "
            f"bytes=[{header_len}] max_bytes=[{_max_header_bytes()}]"
        )
    if payload_len > _max_payload_bytes():
        raise ProtocolError(
            "gateway response payload exceeds client budget "
            f"bytes=[{payload_len}] max_bytes=[{_max_payload_bytes()}]"
        )
    header_bytes = _receive_exact(sock, header_len)
    try:
        header = json.loads(header_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("gateway response header is not valid UTF-8 JSON") from error
    if not isinstance(header, dict):
        raise ProtocolError("gateway response header must be a JSON object")
    return header, _receive_exact(sock, payload_len)


def table_to_ipc(table: Any) -> bytes:
    """Encode one PyArrow table as an Arrow IPC stream."""
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def table_from_ipc(payload: bytes):
    """Decode an Arrow IPC stream into a PyArrow table."""
    import pyarrow as pa

    return pa.ipc.open_stream(pa.py_buffer(payload)).read_all()


def schema_from_ipc(payload: bytes):
    """Decode an Arrow schema from an IPC stream."""
    import pyarrow as pa

    return pa.ipc.open_stream(pa.py_buffer(payload)).schema


def value_to_table(value: object, schema: object | None = None) -> Any:
    """Normalize common batched Python values to a PyArrow table."""
    import pyarrow as pa

    if isinstance(value, pa.Table):
        return value.cast(schema) if schema is not None and value.schema != schema else value
    if isinstance(value, pa.RecordBatch):
        table = pa.Table.from_batches([value])
        return table.cast(schema) if schema is not None else table
    if isinstance(value, dict):
        if is_scalar_row(value):
            return pa.Table.from_pylist([value], schema=schema)
        return pa.table(value, schema=schema)
    if isinstance(value, list):
        return pa.Table.from_pylist(value, schema=schema)
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        return value_to_table(to_arrow(), schema)
    raise TypeError(
        "writer value must be a row dict, list[dict], pyarrow.Table, or pyarrow.RecordBatch"
    )


def is_scalar_row(value: object) -> TypeGuard[dict[str, object]]:
    """Return whether a dictionary represents one scalar row."""
    if not isinstance(value, dict):
        return False
    return not any(isinstance(item, (list, tuple)) for item in value.values())


def _receive_exact(sock: socket.socket, size: int) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        chunk = sock.recv(size - len(payload))
        if not chunk:
            raise ProtocolError("gateway connection closed while reading frame")
        payload.extend(chunk)
    return bytes(payload)


def _max_header_bytes() -> int:
    return _positive_env_int("ZIPPY_CLIENT_MAX_FRAME_HEADER_BYTES", _DEFAULT_MAX_HEADER_BYTES)


def _max_payload_bytes() -> int:
    return _positive_env_int("ZIPPY_CLIENT_MAX_FRAME_PAYLOAD_BYTES", _DEFAULT_MAX_PAYLOAD_BYTES)


def _positive_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(
            f"environment value must be an integer name=[{name}] value=[{raw}]"
        ) from error
    if value <= 0:
        raise ValueError(f"environment value must be positive name=[{name}] value=[{raw}]")
    return value
