from __future__ import annotations

import importlib.util
import json
import socket
import threading
from pathlib import Path

import pyarrow as pa


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "08_remote_gateway"
    / "04_standalone_windows_smoke_client.py"
)


def load_client_module():
    spec = importlib.util.spec_from_file_location("standalone_gateway_smoke_client", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def unused_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


class FakeMasterServer:
    def __init__(self, config: dict[str, object]) -> None:
        self.endpoint = unused_endpoint()
        self.config = config
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.requests: list[dict[str, object]] = []

    def start(self) -> "FakeMasterServer":
        host, port_text = self.endpoint.rsplit(":", 1)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, int(port_text)))
        listener.listen()
        listener.settimeout(0.1)

        def run() -> None:
            with listener:
                while not self._stop_event.is_set():
                    try:
                        client, _ = listener.accept()
                    except TimeoutError:
                        continue
                    with client:
                        request_line = b""
                        while not request_line.endswith(b"\n"):
                            chunk = client.recv(4096)
                            if not chunk:
                                break
                            request_line += chunk
                        if not request_line:
                            continue
                        self.requests.append(json.loads(request_line.decode("utf-8")))
                        response = {"ConfigFetched": {"config": self.config}}
                        client.sendall(json.dumps(response).encode("utf-8") + b"\n")

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        try:
            host, port_text = self.endpoint.rsplit(":", 1)
            with socket.create_connection((host, int(port_text)), timeout=0.1):
                pass
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)


class FakeGatewayServer:
    def __init__(self, client_module, token: str | None) -> None:
        self.client_module = client_module
        self.endpoint = unused_endpoint()
        self.token = token
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self.written_table: pa.Table | None = None
        self.requests: list[dict[str, object]] = []

    def start(self) -> "FakeGatewayServer":
        host, port_text = self.endpoint.rsplit(":", 1)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, int(port_text)))
        listener.listen()
        listener.settimeout(0.1)

        def run() -> None:
            with listener:
                while not self._stop_event.is_set():
                    try:
                        client, _ = listener.accept()
                    except TimeoutError:
                        continue
                    with client:
                        try:
                            header, payload = self.client_module._recv_gateway_frame(client)
                        except RuntimeError:
                            continue
                        self.requests.append(header)
                        if self.token is not None and header.get("token") != self.token:
                            self.client_module._send_gateway_frame(
                                client,
                                {"status": "error", "reason": "unauthorized"},
                            )
                            continue
                        if header["kind"] == "write_batch":
                            self.written_table = self.client_module._table_from_ipc(payload)
                            self.client_module._send_gateway_frame(client, {"status": "ok"})
                            continue
                        if header["kind"] == "collect":
                            assert self.written_table is not None
                            self.client_module._send_gateway_frame(
                                client,
                                {"status": "ok"},
                                self.client_module._table_to_ipc(self.written_table),
                            )
                            continue
                        self.client_module._send_gateway_frame(
                            client,
                            {"status": "error", "reason": "unsupported"},
                        )

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        try:
            host, port_text = self.endpoint.rsplit(":", 1)
            with socket.create_connection((host, int(port_text)), timeout=0.1):
                pass
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)


def test_standalone_gateway_smoke_discovers_gateway_from_master_config() -> None:
    client = load_client_module()
    gateway = FakeGatewayServer(client, token="dev-token").start()
    master = FakeMasterServer(
        {
            "remote_gateway": {
                "enabled": True,
                "endpoint": gateway.endpoint,
                "token": "dev-token",
                "protocol_version": 1,
            }
        }
    ).start()
    try:
        result = client.run_smoke(
            uri=f"zippy://{master.endpoint}/default",
            stream_name="standalone_windows_ticks",
            timeout_sec=2.0,
        )
    finally:
        master.stop()
        gateway.stop()

    assert result["status"] == "ok"
    assert result["rows"] == 1
    assert result["instrument_id"] == "IF2606"
    assert result["last_price"] == 4102.5
    assert master.requests[0] == {"GetConfig": {}}
    assert [request["kind"] for request in gateway.requests[:2]] == ["write_batch", "collect"]
    assert gateway.requests[0]["token"] == "dev-token"
