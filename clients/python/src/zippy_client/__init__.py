"""Lightweight Python client for local and remote Zippy servers."""

from .client import (
    Client,
    client,
    connect,
    get_writer,
    master,
    read_sql,
    read_table,
    subscribe,
    subscribe_table,
)
from .errors import (
    AuthenticationError,
    CapabilityError,
    ContinuityError,
    ProtocolError,
    QueryError,
    TransportUnavailableError,
    ZippyClientError,
    ZippyConnectionError,
)
from .expressions import Expr, col, lit
from .models import Operation, ServerCapabilities, Transport, TransportDecision
from .query import Table
from .subscriber import GatewaySubscriber, Row, ShmSubscriber, shm_available
from .writer import GatewayWriter

__version__ = "0.1.0"

__all__ = [
    "AuthenticationError",
    "CapabilityError",
    "Client",
    "ContinuityError",
    "Expr",
    "GatewaySubscriber",
    "GatewayWriter",
    "Operation",
    "ProtocolError",
    "QueryError",
    "Row",
    "ServerCapabilities",
    "ShmSubscriber",
    "Table",
    "Transport",
    "TransportDecision",
    "TransportUnavailableError",
    "ZippyClientError",
    "ZippyConnectionError",
    "__version__",
    "client",
    "col",
    "connect",
    "get_writer",
    "lit",
    "master",
    "read_sql",
    "read_table",
    "shm_available",
    "subscribe",
    "subscribe_table",
]
