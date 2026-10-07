"""Lightweight Python client for local and remote Zippy servers."""

from .client import Client
from .errors import (
    AmbiguousAcknowledgementError,
    AmbiguousWriteError,
    AuthenticationError,
    CapabilityError,
    ContinuityError,
    ProtocolError,
    QueryError,
    ServerError,
    TransportUnavailableError,
    ZippyClientError,
    ZippyConnectionError,
)
from .expressions import Expr, col, lit
from .flight_subscriber import FlightSubscriber, SubscriptionOptions
from .log_reader import FlightLogReader, LogCursor, LogEvent
from .models import Operation, ServerCapabilities, Transport, TransportDecision
from .query import Table
from .snapshot import FixedSnapshot
from .row import Row
from .writer import FlightWriter

__version__ = "0.1.0"

__all__ = [
    "AmbiguousAcknowledgementError",
    "AmbiguousWriteError",
    "AuthenticationError",
    "CapabilityError",
    "Client",
    "ContinuityError",
    "Expr",
    "FixedSnapshot",
    "FlightWriter",
    "FlightLogReader",
    "FlightSubscriber",
    "SubscriptionOptions",
    "LogCursor",
    "LogEvent",
    "Operation",
    "ProtocolError",
    "QueryError",
    "Row",
    "ServerCapabilities",
    "ServerError",
    "Table",
    "Transport",
    "TransportDecision",
    "TransportUnavailableError",
    "ZippyClientError",
    "ZippyConnectionError",
    "__version__",
    "col",
    "lit",
]
