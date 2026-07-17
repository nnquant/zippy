"""Pure-Python Master control client used for discovery and SHM leases."""

from __future__ import annotations

import json
import socket
import threading
from typing import Any

from loguru import logger

from .endpoints import ControlEndpoint, split_host_port
from .errors import ProtocolError, ZippyConnectionError, ZippyClientError

_MAX_CONTROL_RESPONSE_BYTES = 64 * 1024 * 1024


class ControlClient:
    """
    Send bounded request/response messages to zippy-master.

    :param endpoint: Resolved Master endpoint.
    :type endpoint: ControlEndpoint
    :param timeout_sec: Per-request socket timeout.
    :type timeout_sec: float
    :param heartbeat_interval_sec: Process lease heartbeat interval.
    :type heartbeat_interval_sec: float
    """

    def __init__(
        self,
        endpoint: ControlEndpoint,
        *,
        timeout_sec: float = 5.0,
        heartbeat_interval_sec: float = 3.0,
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError("timeout_sec must be positive")
        if heartbeat_interval_sec <= 0:
            raise ValueError("heartbeat_interval_sec must be positive")
        self.endpoint = endpoint
        self.timeout_sec = float(timeout_sec)
        self.heartbeat_interval_sec = float(heartbeat_interval_sec)
        self._process_id: str | None = None
        self._process_token: str | None = None
        self._process_app: str | None = None
        self._registration_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_error: BaseException | None = None

    @property
    def process_id(self) -> str | None:
        """Return the active process lease identifier."""
        return self._process_id

    def ping(self) -> None:
        """Verify that the Master accepts control requests."""
        self.get_config()

    def get_config(self) -> dict[str, Any]:
        """Return the server configuration advertised by Master."""
        response = self._request("GetConfig", {})
        return dict(self._expect(response, "ConfigFetched").get("config", {}))

    def list_streams(self, *, status: bool = False) -> list[dict[str, Any]]:
        """Return stream metadata known by Master."""
        verb = "ListStreamStatuses" if status else "ListStreams"
        response = self._request(verb, {})
        payload = self._expect(response, "StreamsListed")
        return [dict(item) for item in payload.get("streams", [])]

    def get_stream(self, source: str, *, status: bool = True) -> dict[str, Any]:
        """Return metadata for one stream."""
        verb = "GetStreamStatus" if status else "GetStream"
        response = self._request(verb, {"stream_name": str(source)})
        return dict(self._expect(response, "StreamFetched").get("stream", {}))

    def ensure_registered(self, app: str = "zippy_client") -> str:
        """Create a process lease and start its heartbeat thread if needed."""
        with self._registration_lock:
            if self._process_id is not None:
                return self._process_id
            response = self._request("RegisterProcess", {"app": str(app)})
            payload = self._expect(response, "ProcessRegistered")
            process_id = str(payload.get("process_id", ""))
            process_token = str(payload.get("process_token", ""))
            if not process_id or not process_token:
                raise ProtocolError("master process registration response is incomplete")
            self._process_id = process_id
            self._process_token = process_token
            self._process_app = str(app)
            self._heartbeat_error = None
            self._start_heartbeat_locked()
            return process_id

    def heartbeat(self) -> None:
        """Renew the current process lease once."""
        process_id, process_token = self._require_process()
        response = self._request(
            "Heartbeat",
            {"process_id": process_id, "process_token": process_token},
        )
        self._expect(response, "HeartbeatAccepted")

    def acquire_segment_reader_lease(
        self,
        source: str,
        segment_id: int,
        generation: int,
    ) -> str:
        """Acquire a Master-owned lease protecting one SHM segment."""
        process_id, process_token = self._require_process()
        response = self._request(
            "AcquireSegmentReaderLease",
            {
                "stream_name": str(source),
                "process_id": process_id,
                "process_token": process_token,
                "source_segment_id": int(segment_id),
                "source_generation": int(generation),
            },
        )
        payload = self._expect(response, "SegmentReaderLeaseAcquired")
        lease_id = str(payload.get("lease_id", ""))
        if not lease_id:
            raise ProtocolError("master segment reader lease response is missing lease_id")
        return lease_id

    def release_segment_reader_lease(self, source: str, lease_id: str) -> None:
        """Release one previously acquired SHM reader lease."""
        process_id, process_token = self._require_process()
        response = self._request(
            "ReleaseSegmentReaderLease",
            {
                "stream_name": str(source),
                "process_id": process_id,
                "process_token": process_token,
                "lease_id": str(lease_id),
            },
        )
        self._expect(response, "SegmentReaderLeaseReleased")

    def close(self) -> None:
        """Stop heartbeats and release the active process lease."""
        self._stop_event.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.timeout_sec))
        with self._registration_lock:
            if self._process_id is None:
                return
            process_id = self._process_id
            process_token = self._process_token
            try:
                response = self._request(
                    "UnregisterProcess",
                    {"process_id": process_id, "process_token": process_token},
                )
                self._expect(response, "ProcessUnregistered")
            except ZippyClientError as error:
                logger.warning(
                    "failed to unregister client process process_id=[{}] error=[{}]",
                    process_id,
                    error,
                )
            finally:
                self._process_id = None
                self._process_token = None
                self._process_app = None

    def _request(self, verb: str, payload: dict[str, object]) -> dict[str, Any]:
        request = json.dumps({verb: payload}, separators=(",", ":")).encode("utf-8") + b"\n"
        try:
            with self._connect() as sock:
                sock.sendall(request)
                response_line = _receive_control_line(sock)
        except OSError as error:
            raise ZippyConnectionError(
                "master control request failed "
                f"endpoint=[{self.endpoint.address}] verb=[{verb}] error=[{error}]"
            ) from error
        try:
            response = json.loads(response_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProtocolError("master control response is not valid UTF-8 JSON") from error
        if not isinstance(response, dict):
            raise ProtocolError("master control response must be a JSON object")
        if "Error" in response:
            error_payload = response["Error"]
            reason = (
                error_payload.get("reason", "master control request failed")
                if isinstance(error_payload, dict)
                else str(error_payload)
            )
            raise ZippyClientError(f"master control request failed verb=[{verb}] reason=[{reason}]")
        return response

    def _connect(self) -> socket.socket:
        if self.endpoint.kind == "unix":
            if not hasattr(socket, "AF_UNIX"):
                raise ZippyConnectionError(
                    "Unix control endpoints are unavailable on this platform"
                )
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.timeout_sec)
            sock.connect(self.endpoint.address)
            return sock
        host, port = split_host_port(self.endpoint.address)
        return socket.create_connection((host, port), timeout=self.timeout_sec)

    def _require_process(self) -> tuple[str, str]:
        if self._heartbeat_error is not None:
            raise ZippyConnectionError(
                f"master process heartbeat failed error=[{self._heartbeat_error}]"
            ) from self._heartbeat_error
        if self._process_id is None or self._process_token is None:
            raise ZippyClientError("client process is not registered")
        return self._process_id, self._process_token

    def _start_heartbeat_locked(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="zippy-client-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.heartbeat_interval_sec):
            try:
                self.heartbeat()
                self._heartbeat_error = None
            except BaseException as error:
                self._heartbeat_error = error
                logger.error(
                    "client process heartbeat failed process_id=[{}] error=[{}]",
                    self._process_id,
                    error,
                )
                return

    @staticmethod
    def _expect(response: dict[str, Any], kind: str) -> dict[str, Any]:
        if kind not in response:
            actual = next(iter(response), "<empty>")
            raise ProtocolError(f"unexpected master response expected=[{kind}] actual=[{actual}]")
        payload = response[kind]
        if not isinstance(payload, dict):
            raise ProtocolError(f"master response payload must be an object kind=[{kind}]")
        return payload


def _receive_control_line(sock: socket.socket) -> bytes:
    response = bytearray()
    while not response.endswith(b"\n"):
        chunk = sock.recv(64 * 1024)
        if not chunk:
            raise ProtocolError("master control connection closed before response")
        response.extend(chunk)
        if len(response) > _MAX_CONTROL_RESPONSE_BYTES:
            raise ProtocolError(
                "master control response exceeds client budget "
                f"bytes=[{len(response)}] max_bytes=[{_MAX_CONTROL_RESPONSE_BYTES}]"
            )
    return bytes(response[:-1])
