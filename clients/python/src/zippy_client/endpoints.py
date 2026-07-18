"""URI and endpoint normalization shared by control and data transports."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit

from .errors import ProtocolError

DEFAULT_MASTER_URI = "zippy://default"
DEFAULT_WINDOWS_CONTROL_ADDR = "127.0.0.1:17690"


@dataclass(frozen=True)
class ControlEndpoint:
    """Resolved Master control endpoint."""

    kind: str
    address: str
    original_uri: str

    @property
    def is_namespace_local(self) -> bool:
        """Return whether the endpoint proves the same local IPC namespace."""
        return self.kind == "unix"

    @property
    def host(self) -> str | None:
        """Return the TCP host, or ``None`` for Unix sockets."""
        if self.kind != "tcp":
            return None
        return split_host_port(self.address)[0]


def resolve_control_endpoint(uri: str | None = None) -> ControlEndpoint:
    """
    Resolve a Zippy logical URI to a Unix or TCP control endpoint.

    :param uri: Logical URI, raw socket path, or TCP endpoint.
    :type uri: str | None
    :returns: Resolved control endpoint.
    :rtype: ControlEndpoint
    """
    raw_uri = (
        uri
        or os.environ.get("ZIPPY_MASTER_URI")
        or os.environ.get("ZIPPY_MASTER_ENDPOINT")
        or os.environ.get("ZIPPY_CONTROL_ENDPOINT")
        or DEFAULT_MASTER_URI
    )
    if raw_uri.startswith("zippy+tcp://"):
        raise ValueError("zippy+tcp:// is no longer supported; use zippy://host:port/profile")
    if raw_uri.startswith("tcp://"):
        return ControlEndpoint("tcp", normalize_tcp_address(raw_uri), raw_uri)
    if raw_uri.startswith("zippy://"):
        body = raw_uri.removeprefix("zippy://")
        authority = body.split("/", 1)[0]
        if ":" in authority:
            return ControlEndpoint("tcp", normalize_tcp_address(authority), raw_uri)
        if os.name == "nt":
            address = DEFAULT_WINDOWS_CONTROL_ADDR if authority in {"", "default"} else authority
            return ControlEndpoint("tcp", normalize_tcp_address(address), raw_uri)
        return ControlEndpoint("unix", str(_logical_unix_path(body or "default")), raw_uri)
    if raw_uri.startswith("unix://"):
        return ControlEndpoint(
            "unix", str(Path(raw_uri.removeprefix("unix://")).expanduser()), raw_uri
        )
    if raw_uri.startswith("file://"):
        return ControlEndpoint(
            "unix", str(Path(raw_uri.removeprefix("file://")).expanduser()), raw_uri
        )
    if _looks_like_path(raw_uri):
        return ControlEndpoint("unix", str(Path(raw_uri).expanduser()), raw_uri)
    if os.name == "nt":
        address = DEFAULT_WINDOWS_CONTROL_ADDR if raw_uri in {"", "default"} else raw_uri
        return ControlEndpoint("tcp", normalize_tcp_address(address), raw_uri)
    return ControlEndpoint("unix", str(_logical_unix_path(raw_uri or "default")), raw_uri)


def normalize_tcp_address(value: str) -> str:
    """Normalize a TCP endpoint to ``host:port`` without a path."""
    text = str(value).removeprefix("tcp://")
    parsed = urlsplit(f"//{text}")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"TCP endpoint must include host and port endpoint=[{value}]")
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    return f"{host}:{parsed.port}"


def split_host_port(value: str) -> tuple[str, int]:
    """Parse a normalized TCP endpoint into host and port."""
    parsed = urlsplit(f"//{normalize_tcp_address(value)}")
    if parsed.hostname is None or parsed.port is None:
        raise ProtocolError(f"invalid TCP endpoint endpoint=[{value}]")
    return parsed.hostname, parsed.port


def connectable_gateway_endpoint(endpoint: str, control: ControlEndpoint) -> str:
    """Replace wildcard Gateway hosts with a host reachable by this client."""
    return connectable_data_endpoint(endpoint, control)


def connectable_data_endpoint(endpoint: str, control: ControlEndpoint) -> str:
    """Replace wildcard data-plane hosts with a host reachable by this client."""
    host, port = split_host_port(endpoint)
    if host not in {"0.0.0.0", "::"}:
        return normalize_tcp_address(endpoint)
    replacement = control.host or "127.0.0.1"
    rendered = f"[{replacement}]" if ":" in replacement else replacement
    return f"{rendered}:{port}"


def gateway_is_local(endpoint: str, control: ControlEndpoint) -> bool:
    """Return whether a Gateway endpoint is local to the client host."""
    return data_endpoint_is_local(endpoint, control)


def data_endpoint_is_local(endpoint: str, control: ControlEndpoint) -> bool:
    """Return whether a TCP data endpoint is local to the client host."""
    if control.kind == "unix":
        return True
    host, _ = split_host_port(endpoint)
    return host in {"127.0.0.1", "::1", "localhost"}


def _logical_unix_path(name: str) -> Path:
    return _home_dir() / ".zippy" / "control_endpoints" / name / "master.sock"


def _home_dir() -> Path:
    if home := os.environ.get("HOME"):
        return Path(home)
    if profile := os.environ.get("USERPROFILE"):
        return Path(profile)
    if (drive := os.environ.get("HOMEDRIVE")) and (path := os.environ.get("HOMEPATH")):
        return Path(drive + path)
    return Path(tempfile.gettempdir())


def _looks_like_path(value: str) -> bool:
    return value.startswith(("/", "~/", "~\\", "./", "../"))
