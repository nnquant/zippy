"""Typed exceptions exposed by :mod:`zippy_client`."""

from __future__ import annotations

from typing import Any


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


class AmbiguousWriteError(QueryError):
    """
    Report a Flight write whose final committed prefix is not fully known.

    :param message: Diagnostic explaining where the write stopped.
    :type message: str
    :param receipts: Receipts definitely acknowledged before the failure.
    :type receipts: list[dict[str, object]]
    """

    def __init__(self, message: str, *, receipts: list[dict[str, object]]) -> None:
        super().__init__(message)
        self.receipts = [dict(receipt) for receipt in receipts]


class AmbiguousAcknowledgementError(ZippyConnectionError):
    """Report an ACK whose durable outcome was not received.

    Reconnect using the same consumer identity; Catalog decides whether to replay the unit.

    :param message: Description of the missing or invalid acknowledgement receipt.
    :param request_id: Stable request ID sent for this confirmation.
    """

    def __init__(self, message: str, *, request_id: str) -> None:
        super().__init__(message)
        self.request_id = request_id


class ServerError(QueryError):
    """
    Represent one structured error returned by a Zippy service.

    :param code: Stable server error code such as ``RESOURCE_EXHAUSTED``.
    :type code: str
    :param message: Human-readable diagnostic message.
    :type message: str
    :param retryable: Whether the server declares that retry may succeed.
    :type retryable: bool
    :param request_id: Optional request identifier encoded as a UUID string.
    :type request_id: str | None
    :param query_id: Optional query identifier encoded as a UUID string.
    :type query_id: str | None
    :param table_id: Optional table identifier encoded as a UUID string.
    :type table_id: str | None
    :param details: Optional code-specific structured details.
    :type details: dict[str, Any] | None
    :param receipts: Write receipts acknowledged before this explicit rejection, if any.
    :type receipts: list[dict[str, object]] | None
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        request_id: str | None = None,
        query_id: str | None = None,
        table_id: str | None = None,
        details: dict[str, Any] | None = None,
        receipts: list[dict[str, object]] | None = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        self.code = str(code)
        self.message = str(message)
        self.retryable = bool(retryable)
        self.request_id = request_id
        self.query_id = query_id
        self.table_id = table_id
        self.details = dict(details or {})
        self.receipts = [dict(receipt) for receipt in receipts or []]
