"""Typed exceptions exposed by :mod:`zippy_client`."""

from __future__ import annotations


class ZippyClientError(Exception):
    """Base exception for all client-side failures."""


class ZippyConnectionError(ZippyClientError):
    """Raised when a control or data endpoint cannot be reached."""


class ProtocolError(ZippyClientError):
    """Raised when a peer sends an invalid or incompatible protocol frame."""


class AuthenticationError(ZippyClientError):
    """Raised when the server rejects client authentication."""


class CapabilityError(ZippyClientError):
    """Raised when the connected server does not expose a requested capability."""


class TransportUnavailableError(ZippyClientError):
    """Raised when an explicitly selected transport cannot be used."""


class ContinuityError(ZippyClientError):
    """Raised when a live subscription cannot prove gap-free continuation."""


class QueryError(ZippyClientError):
    """Raised when the server rejects a table or SQL query."""
