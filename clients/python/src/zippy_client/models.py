"""Public configuration and transport models for the Zippy client."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Transport(str, Enum):
    """Transport requested for one client operation."""

    AUTO = "auto"
    SHM = "shm"
    FLIGHT_LOCAL = "flight_local"
    FLIGHT_REMOTE = "flight_remote"
    GATEWAY_LOCAL = "gateway_local"
    GATEWAY_REMOTE = "gateway_remote"

    @classmethod
    def parse(cls, value: str | Transport) -> Transport:
        """
        Normalize a public transport value.

        :param value: Transport enum or its string representation.
        :type value: str | Transport
        :returns: Normalized transport enum.
        :rtype: Transport
        :raises ValueError: If the transport name is unsupported.
        """
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as error:
            choices = ", ".join(item.value for item in cls)
            raise ValueError(
                f"unsupported transport value=[{value}] choices=[{choices}]"
            ) from error


class Operation(str, Enum):
    """Operation category used by transport routing."""

    QUERY = "query"
    SQL = "sql"
    SUBSCRIBE = "subscribe"
    WRITE = "write"


@dataclass(frozen=True)
class ServerCapabilities:
    """
    Capabilities discovered from the connected Zippy server.

    :param flight: Whether an Arrow Flight data endpoint is available.
    :type flight: bool
    :param flight_protocol_version: Zippy Flight application protocol version.
    :type flight_protocol_version: int
    :param flight_ingest: Whether append + memory DoPut is available.
    :type flight_ingest: bool
    :param gateway: Whether a Gateway data endpoint is available.
    :type gateway: bool
    :param gateway_protocol_version: Gateway wire protocol version.
    :type gateway_protocol_version: int
    :param query_sql: Whether server-side SQL execution is available.
    :type query_sql: bool
    :param resume_cursor: Whether live subscriptions support resume cursors.
    :type resume_cursor: bool
    """

    flight: bool = False
    flight_protocol_version: int = 0
    flight_ingest: bool = False
    gateway: bool = False
    gateway_protocol_version: int = 0
    query_sql: bool = False
    resume_cursor: bool = False


@dataclass(frozen=True)
class TransportDecision:
    """
    Explain the transport selected for an operation.

    :param requested: Transport requested by the caller.
    :type requested: Transport
    :param selected: Transport selected by the router.
    :type selected: Transport
    :param reason: Stable human-readable selection reason.
    :type reason: str
    """

    requested: Transport
    selected: Transport
    reason: str
