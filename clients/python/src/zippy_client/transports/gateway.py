"""Gateway transport for remote queries, subscriptions, and writes."""

from __future__ import annotations

import socket
from typing import Any

from ..endpoints import normalize_tcp_address, split_host_port
from ..errors import AuthenticationError, ProtocolError, QueryError, ZippyConnectionError
from ..protocol import receive_frame, schema_from_ipc, send_frame, table_from_ipc


class GatewayTransport:
    """
    Execute framed requests against one Gateway endpoint.

    :param endpoint: Gateway TCP endpoint.
    :type endpoint: str
    :param token: Optional Gateway authentication token.
    :type token: str | None
    :param timeout_sec: Per-request socket timeout.
    :type timeout_sec: float
    """

    def __init__(
        self,
        endpoint: str,
        *,
        token: str | None = None,
        timeout_sec: float = 60.0,
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError("timeout_sec must be positive")
        self.endpoint = normalize_tcp_address(endpoint)
        self.token = token
        self.timeout_sec = float(timeout_sec)

    def request(
        self,
        header: dict[str, object],
        payload: bytes = b"",
    ) -> tuple[dict[str, Any], bytes]:
        """Execute one non-streaming Gateway request."""
        request_header = self._authorized_header(header)
        try:
            with self._connect() as sock:
                send_frame(sock, request_header, payload)
                response, response_payload = receive_frame(sock)
        except OSError as error:
            raise ZippyConnectionError(
                "gateway request failed "
                f"endpoint=[{self.endpoint}] kind=[{header.get('kind')}] error=[{error}]"
            ) from error
        self._raise_for_response(response)
        return response, response_payload

    def list_streams(self) -> list[dict[str, Any]]:
        """Return stream metadata visible through Gateway."""
        response, _ = self.request({"kind": "list_streams"})
        return [dict(item) for item in response.get("streams", [])]

    def get_stream(self, source: str) -> tuple[dict[str, Any], object]:
        """Return stream metadata and Arrow schema."""
        response, payload = self.request({"kind": "get_stream", "source": str(source)})
        return dict(response.get("stream", {})), schema_from_ipc(payload)

    def create_snapshot(self, source: str) -> dict[str, Any]:
        """Create a server-owned query snapshot."""
        response, _ = self.request({"kind": "create_snapshot", "source": str(source)})
        snapshot = response.get("snapshot")
        if isinstance(snapshot, dict):
            return dict(snapshot)
        snapshot_id = response.get("snapshot_id")
        if snapshot_id is None:
            raise ProtocolError(
                f"gateway snapshot response is missing snapshot_id source=[{source}]"
            )
        return {"source": str(source), "snapshot_id": str(snapshot_id)}

    def release_snapshot(self, source: str, snapshot_id: str) -> None:
        """Release a server-owned query snapshot."""
        self.request(
            {
                "kind": "release_snapshot",
                "source": str(source),
                "snapshot_id": str(snapshot_id),
            }
        )

    def collect(
        self,
        source: str,
        plan: list[dict[str, object]],
        *,
        snapshot: bool | dict[str, object],
        stream: bool = False,
    ) -> tuple[Any, dict[str, Any]]:
        """Execute a typed table plan and return its Arrow table plus metrics."""
        header: dict[str, object] = {
            "kind": "collect",
            "source": str(source),
            "plan": plan,
        }
        if isinstance(snapshot, dict):
            snapshot_id = snapshot.get("snapshot_id")
            if snapshot_id is None:
                raise ProtocolError(f"query snapshot is missing snapshot_id source=[{source}]")
            header["snapshot_id"] = str(snapshot_id)
        else:
            header["snapshot"] = bool(snapshot)
        if stream:
            return self._collect_stream(header)
        response, payload = self.request(header)
        metrics = response.get("metrics", {})
        return table_from_ipc(payload), dict(metrics) if isinstance(metrics, dict) else {}

    def read_sql(
        self,
        sql: str,
        *,
        parameters: dict[str, object] | None = None,
        stream: bool = False,
    ) -> tuple[Any, dict[str, Any]]:
        """Execute server-side SQL when the server advertises that capability."""
        header: dict[str, object] = {
            "kind": "query_sql",
            "sql": str(sql),
            "parameters": parameters or {},
        }
        if stream:
            return self._collect_stream(header)
        response, payload = self.request(header)
        metrics = response.get("metrics", {})
        return table_from_ipc(payload), dict(metrics) if isinstance(metrics, dict) else {}

    def open_subscription(self, header: dict[str, object]) -> socket.socket:
        """Open a persistent socket and send its initial subscription request."""
        sock = self._connect()
        try:
            send_frame(sock, self._authorized_header(header))
            # The connect/request timeout must not turn an idle live stream into a
            # false disconnect. ``GatewaySubscriber.stop()`` closes the socket to
            # interrupt the blocking read.
            sock.settimeout(None)
        except BaseException:
            sock.close()
            raise
        return sock

    def _collect_stream(
        self,
        header: dict[str, object],
    ) -> tuple[Any, dict[str, Any]]:
        import pyarrow as pa

        request_header = dict(header)
        request_header["kind"] = (
            "query_sql_stream" if header.get("kind") == "query_sql" else "collect_stream"
        )
        tables = []
        schema = None
        metrics: dict[str, Any] = {}
        try:
            with self._connect() as sock:
                send_frame(sock, self._authorized_header(request_header))
                while True:
                    response, payload = receive_frame(sock)
                    self._raise_for_response(response)
                    kind = response.get("kind")
                    if kind in {"collect_start", "query_sql_start"}:
                        if payload:
                            schema = schema_from_ipc(payload)
                        continue
                    if kind in {"collect_chunk", "query_sql_chunk"}:
                        table = table_from_ipc(payload)
                        schema = schema or table.schema
                        tables.append(table)
                        continue
                    if kind in {"collect_end", "query_sql_end"}:
                        value = response.get("metrics", {})
                        metrics = dict(value) if isinstance(value, dict) else {}
                        break
                    if kind in {"collect_error", "query_sql_error"}:
                        raise QueryError(
                            str(response.get("reason", "gateway streaming query failed"))
                        )
                    raise ProtocolError(f"unexpected gateway stream frame kind=[{kind}]")
        except OSError as error:
            raise ZippyConnectionError(
                "gateway streaming request failed "
                f"endpoint=[{self.endpoint}] kind=[{request_header.get('kind')}] error=[{error}]"
            ) from error
        if tables:
            return pa.concat_tables(tables), metrics
        if schema is not None:
            return pa.Table.from_batches([], schema=schema), metrics
        return pa.table({}), metrics

    def _connect(self) -> socket.socket:
        host, port = split_host_port(self.endpoint)
        sock = socket.create_connection((host, port), timeout=self.timeout_sec)
        sock.settimeout(self.timeout_sec)
        return sock

    def _authorized_header(self, header: dict[str, object]) -> dict[str, object]:
        value = dict(header)
        if self.token is not None:
            value["token"] = self.token
        return value

    @staticmethod
    def _raise_for_response(response: dict[str, Any]) -> None:
        if response.get("status") == "ok":
            return
        reason = str(response.get("reason", "gateway request failed"))
        if "unauthorized" in reason.lower() or "authentication" in reason.lower():
            raise AuthenticationError(reason)
        raise QueryError(reason)
