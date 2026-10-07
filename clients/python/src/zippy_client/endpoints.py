"""URI and endpoint normalization shared by control and data transports."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit

from .errors import ProtocolError

DEFAULT_CONTROL_URI = "zippy://default"
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
    :raises ValueError: If the URI is invalid or unsupported on this platform.
    """
    for removed_name in ("ZIPPY_MASTER_URI", "ZIPPY_MASTER_ENDPOINT"):
        if removed_name in os.environ:
            raise ValueError(
                f"removed control endpoint environment variable name=[{removed_name}]; "
                "use [ZIPPY_CONTROL_ENDPOINT]"
            )
    raw_uri = uri or os.environ.get("ZIPPY_CONTROL_ENDPOINT") or DEFAULT_CONTROL_URI
    if raw_uri.startswith("zippy+tcp://"):
        raise ValueError("zippy+tcp:// is no longer supported; use zippy://host:port/default")
    if raw_uri.startswith("tcp://"):
        return ControlEndpoint("tcp", normalize_tcp_address(raw_uri), raw_uri)
    if raw_uri.startswith("zippy://"):
        body = raw_uri.removeprefix("zippy://")
        authority, separator, profile = body.partition("/")
        if ":" in authority:
            if separator and profile != "default":
                raise ValueError("remote zippy control URI supports only /default profile")
            return ControlEndpoint("tcp", normalize_tcp_address(authority), raw_uri)
        if os.name == "nt":
            if body not in {"", "default"}:
                raise ValueError("named control endpoint is not supported on this platform")
            return ControlEndpoint("tcp", DEFAULT_WINDOWS_CONTROL_ADDR, raw_uri)
        return ControlEndpoint("unix", str(_logical_unix_path(body or "default")), raw_uri)
    if os.name == "nt" and (
        raw_uri.startswith(("unix://", "file://")) or _looks_like_path(raw_uri)
    ):
        raise ValueError("Unix control endpoint is not supported on this platform")
    if raw_uri.startswith("unix://"):
        return ControlEndpoint("unix", _expand_path(raw_uri.removeprefix("unix://")), raw_uri)
    if raw_uri.startswith("file://"):
        return ControlEndpoint("unix", _expand_path(raw_uri.removeprefix("file://")), raw_uri)
    if _looks_like_path(raw_uri):
        return ControlEndpoint("unix", _expand_path(raw_uri), raw_uri)
    if os.name == "nt":
        address = DEFAULT_WINDOWS_CONTROL_ADDR if raw_uri in {"", "default"} else raw_uri
        return ControlEndpoint("tcp", normalize_tcp_address(address), raw_uri)
    return ControlEndpoint("unix", str(_logical_unix_path(raw_uri or "default")), raw_uri)


def normalize_tcp_address(value: str) -> str:
    """Normalize a TCP endpoint to ``host:port`` without a path."""
    endpoint = str(value).removeprefix("tcp://")
    try:
        parsed = urlsplit(f"//{endpoint}")
        host = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError("TCP endpoint must include host and port only") from error
    if (
        host is None
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or "?" in endpoint
        or "#" in endpoint
    ):
        raise ValueError("TCP endpoint must include host and port only")
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{rendered_host}:{port}"


def split_host_port(value: str) -> tuple[str, int]:
    """Parse a normalized TCP endpoint into host and port."""
    parsed = urlsplit(f"//{normalize_tcp_address(value)}")
    if parsed.hostname is None or parsed.port is None:
        raise ProtocolError(f"invalid TCP endpoint endpoint=[{value}]")
    return parsed.hostname, parsed.port


def connectable_data_endpoint(endpoint: str, control: ControlEndpoint) -> str:
    """Replace wildcard data-plane hosts with a host reachable by this client."""
    host, port = split_host_port(endpoint)
    if host not in {"0.0.0.0", "::"}:
        return normalize_tcp_address(endpoint)
    replacement = control.host or "127.0.0.1"
    rendered = f"[{replacement}]" if ":" in replacement else replacement
    return f"{rendered}:{port}"


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
    return (
        value.startswith(("/", "~/", "~\\", "./", "../"))
        or "/" in value
        or "\\" in value
        or (len(value) > 1 and value[1] == ":")
        or value.endswith(".sock")
    )


def _expand_path(value: str) -> str:
    if value.startswith("~/"):
        return str(_home_dir() / value[2:])
    if value.startswith("~\\"):
        return str(_home_dir()) + "\\" + value[2:]
    return str(Path(value).expanduser())
