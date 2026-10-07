"""Arrow Flight query and bounded producer-fenced ingest transport."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
import threading
import time
from typing import Any, TypeVar, cast
import uuid
import weakref

from google.protobuf.any_pb2 import Any as ProtobufAny
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from loguru import logger
import pyarrow as pa
import pyarrow.flight as flight

from .._generated import protocol_pb2 as wire
from ..endpoints import normalize_tcp_address, split_host_port
from ..log_reader import FlightLogReader, LogCursor
from ..errors import (
    AmbiguousWriteError,
    AuthenticationError,
    CapabilityError,
    ProtocolError,
    QueryError,
    ServerError,
    ZippyConnectionError,
)

FLIGHT_PROTOCOL_VERSION = 1
CAPABILITIES_ACTION = "zippy.capabilities.v1"
QUERY_CAPABILITY = "query.get_flight_info_do_get.v1"
STRUCTURED_ERROR_CAPABILITY = "error.details.v1"
MEMORY_INGEST_CAPABILITY = "ingest.append.memory.v1"
WAL_INGEST_CAPABILITY = "ingest.append.wal.v1"
PERSISTED_INGEST_CAPABILITY = "ingest.append.persisted.v1"

_AUTHORIZATION_METADATA_KEY = b"authorization"
_PUT_DURABILITY = {
    "memory": wire.PUT_DURABILITY_MEMORY,
    "wal": wire.PUT_DURABILITY_WAL,
    "persisted": wire.PUT_DURABILITY_PERSISTED,
}
_PUT_DURABILITY_NAMES = {value: name for name, value in _PUT_DURABILITY.items()}
_SESSION_REFRESH_MARGIN_NS = 5_000_000_000

_T = TypeVar("_T")


@dataclass(frozen=True)
class FlightCapabilities:
    """Capabilities and hard limits advertised by one Flight listener."""

    protocol_version: int
    server_version: str
    names: frozenset[str]
    max_message_bytes: int
    max_pending_tickets: int
    authentication_required: bool
    session_ttl_ms: int
    writer_authentication_enabled: bool
    ingest_enabled: bool
    ingest_max_concurrent_puts: int
    ingest_max_batches_per_put: int
    ingest_max_rows_per_batch: int
    ingest_max_rows_per_put: int
    ingest_max_batch_bytes: int
    ingest_max_bytes_per_put: int
    ingest_max_rows_per_frame: int

    @property
    def query(self) -> bool:
        """Return whether version 1 query execution is advertised."""
        return QUERY_CAPABILITY in self.names

    @property
    def structured_errors(self) -> bool:
        """Return whether versioned structured errors are advertised."""
        return STRUCTURED_ERROR_CAPABILITY in self.names

    @property
    def memory_ingest(self) -> bool:
        """Return whether authenticated append + memory ingest is usable."""
        return (
            self.ingest_enabled
            and self.writer_authentication_enabled
            and MEMORY_INGEST_CAPABILITY in self.names
        )

    @property
    def wal_ingest(self) -> bool:
        """Return whether authenticated append + WAL ingest is usable."""
        return (
            self.ingest_enabled
            and self.writer_authentication_enabled
            and WAL_INGEST_CAPABILITY in self.names
        )

    @property
    def persisted_ingest(self) -> bool:
        """Return whether manifest-backed append acknowledgements are available."""
        return (
            self.ingest_enabled
            and self.writer_authentication_enabled
            and PERSISTED_INGEST_CAPABILITY in self.names
        )


@dataclass(frozen=True)
class _Session:
    session_id: bytes
    bearer_token: str
    expires_at_ns: int
    role: int


@dataclass(frozen=True)
class _RoleConnection:
    client: flight.FlightClient
    session: _Session
    auth_handler: _SessionAuthHandler


@dataclass
class _ActivePut:
    namespace: str
    stream_name: str
    schema: pa.Schema
    durability: str
    mutation: str
    producer_id: bytes
    connection_id: bytes
    put_id: bytes
    writer: Any
    metadata_reader: Any
    next_batch_sequence: int
    retire_at_ns: int
    deadline_at_ns: int
    batch_count: int = 0
    row_count: int = 0
    byte_count: int = 0
    producer_epoch: int | None = None
    expected_table_id: bytes | None = None


class _SessionAuthHandler(flight.ClientAuthHandler):
    """Exchange the versioned Zippy session payload over Flight Handshake."""

    def __init__(self) -> None:
        super().__init__()
        self.response: wire.SessionOpenResponse | None = None
        self._token = b""

    def authenticate(self, outgoing: Any, incoming: Any) -> None:
        request = wire.SessionOpenRequest(protocol_version=FLIGHT_PROTOCOL_VERSION)
        outgoing.write(request.SerializeToString())
        payload = incoming.read()
        if not payload:
            raise ProtocolError("Flight handshake returned an empty session response")
        response = wire.SessionOpenResponse()
        try:
            response.ParseFromString(bytes(payload))
        except DecodeError as error:
            raise ProtocolError("Flight handshake returned malformed protobuf") from error
        self.response = response
        try:
            self._token = response.bearer_token.encode("ascii")
        except UnicodeEncodeError as error:
            raise ProtocolError("Flight handshake returned a non-ASCII bearer token") from error

    def get_token(self) -> bytes:
        """Return the server-issued bearer used by subsequent Flight calls."""
        return self._token


class FlightTransport:
    """
    Execute server-owned queries and producer-fenced writes through Arrow Flight.

    :param endpoint: Connectable ``host:port`` Flight endpoint.
    :type endpoint: str
    :param tls_enabled: Whether to use ``grpc+tls``.
    :type tls_enabled: bool
    :param read_token: Optional read-session bootstrap token.
    :type read_token: str | None
    :param admin_token: Optional administrator-session bootstrap token.
    :type admin_token: str | None
    :param writer_token: Optional writer-session bootstrap token.
    :type writer_token: str | None
    :param timeout_sec: Default RPC timeout in seconds.
    :type timeout_sec: float
    :param configured_max_message_bytes: Master-advertised gRPC message bound.
    :type configured_max_message_bytes: int
    :param tls_root_certs: Optional trusted PEM roots.
    :type tls_root_certs: bytes | None
    :param cert_chain: Optional mTLS client certificate chain.
    :type cert_chain: bytes | None
    :param private_key: Optional mTLS client private key.
    :type private_key: bytes | None
    :param override_hostname: Optional TLS server-name override.
    :type override_hostname: str | None
    """

    def __init__(
        self,
        endpoint: str,
        *,
        tls_enabled: bool,
        read_token: str | None,
        writer_token: str | None,
        admin_token: str | None = None,
        timeout_sec: float = 60.0,
        configured_max_message_bytes: int = 2 * 1024 * 1024,
        tls_root_certs: bytes | None = None,
        cert_chain: bytes | None = None,
        private_key: bytes | None = None,
        override_hostname: str | None = None,
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError("Flight timeout_sec must be positive")
        if configured_max_message_bytes <= 0:
            raise ValueError("Flight configured_max_message_bytes must be positive")
        if (cert_chain is None) != (private_key is None):
            raise ValueError("Flight mTLS cert_chain and private_key must be provided together")
        self.endpoint = normalize_tcp_address(endpoint)
        self.tls_enabled = bool(tls_enabled)
        self.timeout_sec = float(timeout_sec)
        self._read_token = _normalize_token(read_token)
        self._writer_token = _normalize_token(writer_token)
        self._admin_token = _normalize_token(admin_token)
        self._configured_max_message_bytes = int(configured_max_message_bytes)
        self._tls_root_certs = tls_root_certs
        self._cert_chain = cert_chain
        self._private_key = private_key
        self._override_hostname = override_hostname
        self._connection_lock = threading.RLock()
        self._put_lock = threading.RLock()
        self._read_connection: _RoleConnection | None = None
        self._writer_connection: _RoleConnection | None = None
        self._admin_connection: _RoleConnection | None = None
        self._active_puts: dict[bytes, _ActivePut] = {}
        self._fixed_connections: dict[bytes, tuple[_RoleConnection, int]] = {}
        self._owned_connections: list[_RoleConnection] = []
        self._closed = False
        self._log_readers: weakref.WeakSet[FlightLogReader] = weakref.WeakSet()
        discovery = self._new_client()
        try:
            self.capabilities = self._discover_capabilities(discovery)
        finally:
            discovery.close()
        if "session.server_authority.v1" not in self.capabilities.names:
            raise ProtocolError("Flight listener must issue server-authoritative sessions")
        if self.capabilities.session_ttl_ms <= 0:
            raise ProtocolError("Flight listener must advertise a bounded session lifetime")
        if not self.capabilities.query:
            raise CapabilityError("Flight listener does not advertise version 1 query execution")
        if not self.capabilities.structured_errors:
            raise ProtocolError("Flight listener does not advertise structured error envelopes")

    def create_namespace(
        self,
        name: str,
        *,
        namespace_id: bytes | str | uuid.UUID,
        request_id: bytes | str | uuid.UUID,
    ) -> dict[str, object]:
        """Create a namespace through an authenticated administrator session.

        :param name: Unique Catalog namespace name.
        :param namespace_id: Stable non-nil UUID for this namespace.
        :param request_id: Stable UUID reused with the same payload after a failed response.
        :returns: Committed object identity and Catalog revision.
        :raises ServerError: If authorization or the Catalog command fails.
        :example: transport.create_namespace("default", namespace_id=ns, request_id=req)
        """
        object_id = _stable_id_bytes(namespace_id, "namespace_id")
        request = wire.CreateNamespaceRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            request_id=_stable_id_bytes(request_id, "request_id"),
            namespace_id=object_id,
            name=name,
        )
        return self._manage("zippy.create_namespace.v1", request, object_id, 0)

    def create_append_table(
        self,
        name: str,
        schema: pa.Schema,
        *,
        namespace_id: bytes | str | uuid.UUID,
        table_id: bytes | str | uuid.UUID,
        request_id: bytes | str | uuid.UUID,
        event_time_column: str | None = None,
    ) -> dict[str, object]:
        """Create and activate a native append table without restarting the server.

        :param name: Unique name within the namespace.
        :param schema: Initial Arrow schema.
        :param namespace_id: Existing namespace UUID.
        :param table_id: Stable non-nil UUID for this table.
        :param request_id: Stable UUID reused unchanged on retry.
        :param event_time_column: Optional explicit event-time field.
        :returns: Committed table identity, schema version and Catalog revision.
        :raises ServerError: If validation, authorization or installation fails.
        :example: transport.create_append_table("ticks", schema, namespace_id=ns,
                  table_id=table, request_id=req)
        """
        return self._create_table(
            name,
            schema,
            namespace_id=namespace_id,
            table_id=table_id,
            request_id=request_id,
            event_time_column=event_time_column,
        )

    def create_frame_table(
        self,
        name: str,
        schema: pa.Schema,
        *,
        namespace_id: bytes | str | uuid.UUID,
        table_id: bytes | str | uuid.UUID,
        request_id: bytes | str | uuid.UUID,
        by: str,
    ) -> dict[str, object]:
        """Create and activate a native frame table without restarting the server.

        :param name: Unique name within the namespace.
        :param schema: Initial Arrow schema.
        :param namespace_id: Existing namespace UUID.
        :param table_id: Stable non-nil UUID for this table.
        :param request_id: Stable UUID reused unchanged on retry.
        :param by: Explicit business frame grouping column.
        :returns: Committed table identity, schema version and Catalog revision.
        :raises ServerError: If validation, authorization or installation fails.
        :example: transport.create_frame_table("ticks", schema, by="dt", namespace_id=ns,
                  table_id=table, request_id=req)
        """
        if not isinstance(by, str) or not by:
            raise ValueError("frame grouping column must be a nonempty string")
        if "ingest.frame.v1" not in self.capabilities.names:
            raise CapabilityError("Flight listener does not advertise native frame tables")
        return self._create_table(
            name,
            schema,
            namespace_id=namespace_id,
            table_id=table_id,
            request_id=request_id,
            frame_by=by,
        )

    def create_key_value_table(
        self,
        name: str,
        schema: pa.Schema,
        *,
        keys: list[str],
        namespace_id: bytes | str | uuid.UUID,
        table_id: bytes | str | uuid.UUID,
        request_id: bytes | str | uuid.UUID,
    ) -> dict[str, object]:
        """Create a native key-value table with ordered UTF-8 primary keys.

        :param name: Unique table name within the namespace.
        :param schema: Logical Arrow schema without internal event fields.
        :param keys: Nonempty ordered list of unique UTF-8 column names.
        :param namespace_id: Existing namespace UUID.
        :param table_id: Stable non-nil table UUID.
        :param request_id: Stable UUID reused unchanged on retry.
        :returns: Committed table identity, schema version and Catalog revision.
        :raises ValueError: If primary key names are empty or duplicated.
        :raises ServerError: If validation, authorization or installation fails.
        :example: client.create_key_value_table("quotes", schema, keys=["symbol"],
                  namespace_id=ns, table_id=table, request_id=req)
        """
        if not keys or any(not isinstance(key, str) or not key for key in keys):
            raise ValueError("key-value keys must be nonempty column names")
        if len(set(keys)) != len(keys):
            raise ValueError("key-value keys must be unique")
        if "ingest.key_value.v1" not in self.capabilities.names:
            raise CapabilityError("Flight listener does not advertise native key-value tables")
        return self._create_table(
            name,
            schema,
            namespace_id=namespace_id,
            table_id=table_id,
            request_id=request_id,
            key_value_keys=keys,
        )

    def alter_table_schema(
        self,
        table_id: bytes | str | uuid.UUID,
        schema: pa.Schema,
        *,
        expected_schema_version: int,
        request_id: bytes | str | uuid.UUID,
        keys: list[str] | None = None,
    ) -> dict[str, object]:
        """Evolve a native table and acknowledge after its runtime uses the new schema.

        Append nullable columns, relax ordinary-column nullability, or update descriptive
        metadata. Existing names, types, order and identity fields remain unchanged. Reuse
        the same request ID, schema and keys after a lost response. Old continuous readers
        stop with SCHEMA_MISMATCH and must reopen against the new schema.

        :param table_id: Immutable non-nil table UUID.
        :param schema: Proposed logical Arrow schema without internal event columns.
        :param expected_schema_version: Positive current schema version observed by the caller.
        :param request_id: Stable non-nil UUID reused unchanged on retry.
        :param keys: Unchanged ordered primary keys for KV tables; omit for append/frame.
        :returns: Committed table identity, new schema version and Catalog revision.
        :raises ValueError: If identities, version or primary keys are invalid.
        :raises ServerError: If authorization, schema validation or runtime transition fails.
        :example: client.alter_table_schema(table_id, schema, expected_schema_version=1,
                  request_id=req, keys=["symbol"])
        """
        _require_positive_uint64(expected_schema_version, "expected_schema_version")
        if keys is not None:
            if (
                not isinstance(keys, list)
                or not keys
                or any(not isinstance(key, str) or not key for key in keys)
            ):
                raise ValueError("key-value keys must be a nonempty list of column names")
            if len(set(keys)) != len(keys):
                raise ValueError("key-value keys must be unique")
        object_id = _stable_id_bytes(table_id, "table_id")
        request = wire.AlterTableSchemaRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            request_id=_stable_id_bytes(request_id, "request_id"),
            table_id=object_id,
            expected_schema_version=expected_schema_version,
            schema_ipc=_schema_ipc(schema),
        )
        if keys is not None:
            request.key_value.CopyFrom(wire.KeyValueTableOptions(keys=keys))
        return self._manage(
            "zippy.alter_table_schema.v1", request, object_id, expected_schema_version + 1
        )

    def drop_table(
        self,
        table_id: bytes | str | uuid.UUID,
        *,
        expected_snapshot_version: int,
        request_id: bytes | str | uuid.UUID,
    ) -> dict[str, object]:
        """Logically delete a native table using an administrator session.

        Existing pinned snapshots retain their data until released. A changed snapshot
        rejects deletion; reuse the same request ID and payload after a lost response.

        :param table_id: Immutable non-nil table UUID.
        :param expected_snapshot_version: Positive version from query metadata or a receipt.
        :param request_id: Stable non-nil UUID reused unchanged on retry.
        :returns: Deleted table identity and committed Catalog revision.
        :raises ValueError: If identities or the expected version are invalid.
        :raises ServerError: If authorization, snapshot validation or retirement fails.
        :example: client.drop_table(table_id, expected_snapshot_version=2, request_id=req)
        """
        _require_positive_uint64(expected_snapshot_version, "expected_snapshot_version")
        object_id = _stable_id_bytes(table_id, "table_id")
        request = wire.DropTableRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            request_id=_stable_id_bytes(request_id, "request_id"),
            table_id=object_id,
            expected_snapshot_version=expected_snapshot_version,
        )
        return self._manage("zippy.drop_table.v1", request, object_id, 0)

    def _create_table(
        self,
        name: str,
        schema: pa.Schema,
        *,
        namespace_id: bytes | str | uuid.UUID,
        table_id: bytes | str | uuid.UUID,
        request_id: bytes | str | uuid.UUID,
        event_time_column: str | None = None,
        frame_by: str | None = None,
        key_value_keys: list[str] | None = None,
    ) -> dict[str, object]:
        object_id = _stable_id_bytes(table_id, "table_id")
        request = wire.CreateTableRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            request_id=_stable_id_bytes(request_id, "request_id"),
            table_id=object_id,
            namespace_id=_stable_id_bytes(namespace_id, "namespace_id"),
            name=name,
            schema_ipc=_schema_ipc(schema),
        )
        if key_value_keys is not None:
            request.key_value.CopyFrom(wire.KeyValueTableOptions(keys=key_value_keys))
        elif frame_by is not None:
            request.frame.CopyFrom(wire.FrameTableOptions(by=frame_by))
        else:
            options = wire.AppendTableOptions()
            if event_time_column is not None:
                options.event_time_column = event_time_column
            request.append.CopyFrom(options)
        return self._manage("zippy.create_table.v1", request, object_id, 1)

    def _manage(
        self,
        action: str,
        request: (
            wire.CreateNamespaceRequest
            | wire.CreateTableRequest
            | wire.DropTableRequest
            | wire.AlterTableSchemaRequest
        ),
        object_id: bytes,
        schema_version: int,
    ) -> dict[str, object]:
        capability = {
            "zippy.create_namespace.v1": "catalog.create.v1",
            "zippy.create_table.v1": "catalog.create.v1",
            "zippy.drop_table.v1": "catalog.drop.v1",
            "zippy.alter_table_schema.v1": "catalog.schema.v1",
        }[action]
        if capability not in self.capabilities.names:
            raise CapabilityError("Flight listener does not advertise native table management")
        payload = request.SerializeToString()
        if len(payload) > min(
            self.capabilities.max_message_bytes, self._configured_max_message_bytes
        ):
            raise ValueError("table management request exceeds Flight message limit")
        connection = self._connection("admin")
        deadline = time.monotonic() + self.timeout_sec
        for attempt in range(3):
            remaining = deadline - time.monotonic()
            try:
                results = self._invoke(
                    action,
                    lambda: list(
                        connection.client.do_action(
                            flight.Action(action, payload),
                            self._call_options(
                                connection, time.time_ns() + int(remaining * 1_000_000_000)
                            ),
                        )
                    ),
                )
                break
            except ServerError as error:
                delay = 0.05 * (2**attempt)
                if (
                    error.code != "RESOURCE_EXHAUSTED"
                    or not error.retryable
                    or attempt == 2
                    or deadline - time.monotonic() <= delay
                ):
                    raise
                logger.warning(
                    "native management retrying action=[{}] attempt=[{}] request_id=[{}]",
                    action,
                    attempt + 1,
                    uuid.UUID(bytes=request.request_id),
                )
                # The serialized request, identity and expected version stay unchanged.
                time.sleep(delay)
        if len(results) != 1:
            raise ProtocolError("table management must return exactly one result")
        result = wire.CatalogMutationResult()
        try:
            result.ParseFromString(_buffer_bytes(results[0].body))
        except DecodeError as error:
            raise ProtocolError("table management result is malformed") from error
        if (
            result.protocol_version != FLIGHT_PROTOCOL_VERSION
            or result.request_id != request.request_id
            or result.object_id != object_id
            or result.schema_version != schema_version
            or result.catalog_revision == 0
        ):
            raise ProtocolError("table management result does not match the submitted command")
        return {
            "request_id": str(uuid.UUID(bytes=bytes(result.request_id))),
            "object_id": str(uuid.UUID(bytes=bytes(result.object_id))),
            "catalog_revision": result.catalog_revision,
            "schema_version": result.schema_version,
        }

    def open_log(
        self,
        source: str | None = None,
        *,
        namespace: str = "default",
        table_id: str | uuid.UUID | None = None,
        start: str = "latest",
        cursor: LogCursor | None = None,
        consumer_id: str | uuid.UUID | None = None,
        group_id: str | uuid.UUID | None = None,
        deadline_ns: int | None = None,
        memory_limit_bytes: int | None = None,
        batch_rows: int | None = None,
        max_pending_bytes: int = 64 * 1024 * 1024,
        open_timeout_sec: float | None = None,
    ) -> FlightLogReader:
        """Open an ordered log with complete frame/KV delivery and explicit reconnection.

        :param source: Native table name, omitted when resuming a cursor.
        :param namespace: Catalog namespace for name resolution.
        :param table_id: Immutable table identity instead of a source name.
        :param start: ``latest`` / ``earliest``, or ``committed`` with a durable consumer.
        :param cursor: Last complete delivery cursor, binding table ID, schema and next offset.
        :param consumer_id: Durable consumer identity; requires ``start="committed"``.
        :param group_id: Shared group authority; omitted for an individual consumer.
        :param deadline_ns: Absolute UTC Unix-nanosecond stream deadline; absent means no deadline.
        :param memory_limit_bytes: Optional native query memory limit.
        :param batch_rows: Maximum rows requested per wire fragment.
        :param max_pending_bytes: Client atomic-assembly buffer budget.
        :param open_timeout_sec: Opening deadline, independent of the stream lifetime.
        :returns: Single-consumer context-managed iterator of complete log events.
        :raises CapabilityError: The native log protocol is unavailable.
        :raises ValueError: Options are invalid or a cursor is mixed with name/start selection.
        :example: with transport.open_log("frames", start="earliest") as events: next(events)
        """
        if "subscription.log_fragments.v1" not in self.capabilities.names:
            raise CapabilityError("server does not advertise native log subscriptions")
        if open_timeout_sec is not None and (
            not math.isfinite(open_timeout_sec) or open_timeout_sec <= 0
        ):
            raise ValueError("open_timeout_sec must be finite and positive")
        consumer = None
        if consumer_id is not None:
            if "subscription.durable_log.v1" not in self.capabilities.names:
                raise CapabilityError("server does not advertise durable log acknowledgements")
            if start != "committed" or cursor is not None:
                raise ValueError("durable consumers require committed start and no delivery cursor")
            consumer = wire.LogConsumer(consumer_id=_stable_id_bytes(consumer_id, "consumer_id"))
            if group_id is None:
                consumer.individual = True
            else:
                consumer.group_id = _stable_id_bytes(group_id, "group_id")
        elif group_id is not None or start == "committed":
            raise ValueError("committed start or group_id requires a durable consumer_id")
        elif start not in {"latest", "earliest"}:
            raise ValueError("raw log start must be latest or earliest")
        if type(max_pending_bytes) is not int or max_pending_bytes <= 512:
            raise ValueError("max_pending_bytes must be an integer greater than 512")
        if cursor is not None:
            if source is not None or table_id is not None or start != "latest":
                raise ValueError("resume cursor cannot be combined with source or start")
            if (
                type(cursor.next_offset) is not int
                or not 0 <= cursor.next_offset < 2**64
                or not isinstance(cursor.table_id, uuid.UUID)
                or cursor.table_id.int == 0
                or cursor.kind not in {"append", "frame", "key_value"}
            ):
                raise ValueError("log cursor identity, kind or offset is invalid")
            table = wire.TableBinding(table_id=cursor.table_id.bytes)
        elif table_id is not None:
            if source is not None:
                raise ValueError("log table_id cannot be combined with source")
            table = wire.TableBinding(table_id=_stable_id_bytes(table_id, "table_id"))
        else:
            if not isinstance(source, str) or not source or len(source.encode()) > 512:
                raise ValueError("log source must be a nonempty table name of at most 512 bytes")
            if not namespace or len(namespace.encode()) > 512:
                raise ValueError("log namespace must be nonempty and at most 512 bytes")
            table = wire.TableBinding(name=wire.TableName(namespace=namespace, table=source))
        if memory_limit_bytes is not None and memory_limit_bytes <= 0:
            raise ValueError("log memory limit must be positive")
        if batch_rows is not None and (type(batch_rows) is not int or not 0 < batch_rows < 2**32):
            raise ValueError("log batch rows must be a positive uint32")
        setup_timeout = self.timeout_sec if open_timeout_sec is None else open_timeout_sec
        setup_deadline = time.monotonic() + setup_timeout
        connection = self._connection("read", timeout_sec=setup_timeout)
        query_id = uuid.uuid4().bytes
        command = wire.LogCommand(
            protocol_version=1,
            query_id=query_id,
            session_id=connection.session.session_id,
            table=table,
            memory_limit=_resource_limit(memory_limit_bytes, default_kind="server"),
        )
        if consumer is not None:
            command.consumer.CopyFrom(consumer)
            command.committed = True
        elif cursor is not None or start == "earliest":
            command.offset = cursor.next_offset if cursor is not None else 0
        else:
            command.latest = True
        if deadline_ns is not None:
            command.deadline_ns = deadline_ns
        if batch_rows is not None:
            command.batch_rows = batch_rows
        writer, reader = self._invoke(
            "DoExchange.open",
            lambda: connection.client.do_exchange(
                flight.FlightDescriptor.for_command(command.SerializeToString()),
                self._call_options(connection, deadline_ns, streaming=True),
            ),
        )
        try:
            if consumer is None:
                writer.done_writing()
        except BaseException:
            reader.cancel()
            try:
                writer.close()
            except flight.FlightError:
                logger.debug("failed log opening writer cleanup after cancellation")
            raise
        subscription = self._open_log_reader(
            reader,
            writer,
            query_id,
            start,
            cursor,
            max_pending_bytes,
            max(0.0, setup_deadline - time.monotonic()),
            consumer,
        )
        with self._connection_lock:
            if self._closed:
                subscription.close()
                raise ZippyConnectionError("Flight transport closed during log opening")
            self._log_readers.add(subscription)
        return subscription

    def _open_log_reader(
        self,
        reader: Any,
        writer: Any,
        query_id: bytes,
        start: str,
        cursor: LogCursor | None,
        maximum: int,
        timeout: float,
        consumer: wire.LogConsumer | None = None,
    ) -> FlightLogReader:
        setup_lock = threading.Lock()
        completed, expired = False, False

        def expire() -> None:
            nonlocal expired
            with setup_lock:
                if not completed:
                    expired = True
                    reader.cancel()

        timer = threading.Timer(timeout, expire)
        timer.daemon = True
        timer.start()
        subscription: FlightLogReader | None = None
        try:
            subscription = FlightLogReader(
                reader,
                writer,
                query_id=query_id,
                start=start,
                cursor=cursor,
                max_pending_bytes=maximum,
                consumer=consumer,
            )
        finally:
            with setup_lock:
                completed = True
            timer.cancel()
            if expired:
                if subscription is not None:
                    subscription.close()
                raise TimeoutError("native log opening timed out")
        return subscription

    def snapshot_state(
        self,
        source: str | None = None,
        *,
        namespace: str = "default",
        table_id: str | uuid.UUID | None = None,
        deadline_ns: int | None = None,
        memory_limit_bytes: int | None = None,
        result_batch_rows: int | None = None,
    ) -> pa.Table:
        """Read a complete native key-value state and its atomic v2 cursor.

        :param source: Key-value table name.
        :param namespace: Catalog namespace.
        :param table_id: Immutable table identity instead of a source name.
        :param deadline_ns: Optional absolute deadline in UTC Unix nanoseconds.
        :param memory_limit_bytes: Optional server query memory budget.
        :param result_batch_rows: Optional transfer batch row limit.
        :returns: Arrow table with schema metadata containing keys, stream, version and sequence.
        :raises CapabilityError: The server cannot provide native state snapshots.
        :raises ProtocolError: Cursor metadata is malformed or disagrees with the query identity.
        :example: transport.snapshot_state("quotes", namespace="market")
        """
        if "state.key_value_snapshot.v2" not in self.capabilities.names:
            raise CapabilityError("server does not advertise native key-value state snapshots")
        if (source is None) == (table_id is None):
            raise ValueError("snapshot requires exactly one source name or table_id")
        binding: object = (
            (namespace, source)
            if table_id is None
            else {"table_id": _stable_id_bytes(table_id, "table_id")}
        )
        table, metrics = self.read_sql(
            'SELECT * FROM "zippy_key_value_snapshot"',
            {"zippy_key_value_snapshot": binding},
            deadline_ns=deadline_ns,
            memory_limit_bytes=memory_limit_bytes,
            result_batch_rows=result_batch_rows,
            _key_value_snapshot=True,
        )
        _validate_state_snapshot(table, metrics)
        return table

    def create_snapshot(
        self,
        bindings: Mapping[str, object] | Iterable[Mapping[str, object]],
        *,
        ttl_ms: int = 60_000,
    ) -> dict[str, object]:
        """Capture one immutable native boundary for repeated queries.

        :param bindings: Explicit Catalog table names or IDs to capture together.
        :param ttl_ms: Requested lifetime, at most five minutes and bounded by the session.
        :returns: Opaque snapshot identity, expiry and captured table versions.
        :raises CapabilityError: If the server lacks native fixed snapshots.
        :raises ProtocolError: If the response does not identify a bounded valid snapshot.
        """
        if type(ttl_ms) is not int or not 0 < ttl_ms <= 300_000:
            raise ValueError("snapshot ttl_ms must be between 1 and 300000")
        tables = _query_bindings(bindings)
        if not 0 < len(tables) <= 32:
            raise ValueError("fixed snapshot requires between 1 and 32 bindings")
        request = wire.CreateFixedSnapshotRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            bindings=tables,
            ttl_ms=ttl_ms,
        )
        result = wire.FixedSnapshotInfo()
        connection = self._connection("read")
        payload = self._snapshot_action(
            "zippy.create_fixed_snapshot.v1", request.SerializeToString(), connection=connection
        )
        try:
            result.ParseFromString(payload)
        except DecodeError as error:
            raise ProtocolError("fixed snapshot response is malformed") from error
        if (
            result.protocol_version != FLIGHT_PROTOCOL_VERSION
            or len(result.snapshot_id) != 16
            or not any(result.snapshot_id)
            or result.expires_at_ns == 0
            or result.expires_at_ns > connection.session.expires_at_ns
            or result.catalog_revision == 0
            or not 0 < len(result.tables) <= len(tables)
        ):
            raise ProtocolError(
                "fixed snapshot response has invalid identity, expiry or table count"
            )
        versions: list[dict[str, object]] = []
        seen: set[bytes] = set()
        for table in result.tables:
            if (
                len(table.table_id) != 16
                or not any(table.table_id)
                or table.table_id in seen
                or table.schema_version == 0
                or table.snapshot_version == 0
            ):
                raise ProtocolError("fixed snapshot has invalid or duplicate table versions")
            seen.add(table.table_id)
            versions.append(
                {
                    "table_id": str(uuid.UUID(bytes=table.table_id)),
                    "schema_version": table.schema_version,
                    "snapshot_version": table.snapshot_version,
                }
            )
        with self._connection_lock:
            now_ns = time.time_ns()
            self._fixed_connections = {
                key: value for key, value in self._fixed_connections.items() if value[1] > now_ns
            }
            self._fixed_connections[result.snapshot_id] = (connection, result.expires_at_ns)
        return {
            "snapshot_id": str(uuid.UUID(bytes=result.snapshot_id)),
            "expires_at_ns": result.expires_at_ns,
            "catalog_revision": result.catalog_revision,
            "tables": versions,
        }

    def release_snapshot(self, snapshot_id: bytes | str | uuid.UUID) -> bool:
        """Release an owned native snapshot; already-absent identities return false.

        :param snapshot_id: Exact server-issued snapshot identity.
        :returns: Whether the server removed a live lease. After transport close, the
            owning read session has already been revoked and this returns false.
        :raises ServerError: If the identity belongs to another session.
        """
        request = wire.ReleaseFixedSnapshotRequest(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            snapshot_id=_stable_id_bytes(snapshot_id, "snapshot_id"),
        )
        with self._connection_lock:
            if self._closed:
                return False
            connection = self._snapshot_connection(request.snapshot_id)
        payload = self._snapshot_action(
            "zippy.release_fixed_snapshot.v1", request.SerializeToString(), connection=connection
        )
        result = wire.ReleaseFixedSnapshotResult()
        try:
            result.ParseFromString(payload)
        except DecodeError as error:
            raise ProtocolError("fixed snapshot release response is malformed") from error
        if result.protocol_version != FLIGHT_PROTOCOL_VERSION:
            raise ProtocolError("fixed snapshot release version is unsupported")
        with self._connection_lock:
            self._fixed_connections.pop(request.snapshot_id, None)
        return result.released

    def _snapshot_connection(self, snapshot_id: bytes) -> _RoleConnection:
        with self._connection_lock:
            if self._closed:
                raise RuntimeError("Flight transport is closed")
            retained = self._fixed_connections.get(snapshot_id)
            if retained is not None:
                if retained[0].session.expires_at_ns > time.time_ns():
                    return retained[0]
                self._fixed_connections.pop(snapshot_id, None)
        return self._connection("read")

    def _snapshot_action(
        self, action: str, payload: bytes, *, connection: _RoleConnection
    ) -> bytes:
        if "query.fixed_snapshot.v1" not in self.capabilities.names:
            raise CapabilityError("Flight does not advertise native fixed cross-query snapshots")
        if len(payload) > min(
            self.capabilities.max_message_bytes, self._configured_max_message_bytes
        ):
            raise ValueError("fixed snapshot request exceeds Flight message limit")

        def invoke() -> list[flight.Result]:
            results: list[flight.Result] = []
            for result in connection.client.do_action(
                flight.Action(action, payload), self._call_options(connection)
            ):
                if results or len(result.body) > min(
                    self.capabilities.max_message_bytes, self._configured_max_message_bytes
                ):
                    raise ProtocolError("fixed snapshot action response exceeds protocol bounds")
                results.append(result)
            return results

        results = self._invoke(action, invoke)
        if len(results) != 1:
            raise ProtocolError("fixed snapshot action must return exactly one result")
        return _buffer_bytes(results[0].body)

    def read_sql(
        self,
        sql: str,
        bindings: Mapping[str, object] | Iterable[Mapping[str, object]],
        *,
        priority: str = "interactive",
        deadline_ns: int | None = None,
        memory_limit_bytes: int | None = None,
        spill_limit_bytes: int | None = 0,
        result_batch_rows: int | None = None,
        fixed_snapshot_id: bytes | str | uuid.UUID | None = None,
        _key_value_snapshot: bool = False,
    ) -> tuple[pa.Table, dict[str, object]]:
        """Execute one SQL statement with explicit Catalog bindings."""
        normalized_sql = str(sql)
        if not normalized_sql.strip():
            raise ValueError("Flight query SQL must not be empty")
        snapshot_bytes = (
            _stable_id_bytes(fixed_snapshot_id, "fixed_snapshot_id")
            if fixed_snapshot_id is not None
            else None
        )
        connection = (
            self._snapshot_connection(snapshot_bytes)
            if snapshot_bytes is not None
            else self._connection("read")
        )
        query_id = uuid.uuid4().bytes
        command = wire.QueryCommand(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            query_id=query_id,
            session_id=connection.session.session_id,
            sql=normalized_sql,
            key_value_snapshot=_key_value_snapshot,
            bindings=_query_bindings(bindings),
            priority=_query_priority(priority),
            memory_limit=_resource_limit(memory_limit_bytes, default_kind="server"),
            spill_limit=_resource_limit(spill_limit_bytes, default_kind="disabled"),
        )
        if snapshot_bytes is not None:
            if _key_value_snapshot:
                raise ValueError("atomic state cursor snapshots cannot use a fixed query snapshot")
            if "query.fixed_snapshot.v1" not in self.capabilities.names:
                raise CapabilityError(
                    "Flight does not advertise native fixed cross-query snapshots"
                )
            command.fixed_snapshot_id = snapshot_bytes
        if deadline_ns is not None:
            if int(deadline_ns) <= 0:
                raise ValueError("deadline_ns must be positive")
            command.deadline_ns = int(deadline_ns)
        if result_batch_rows is not None:
            if int(result_batch_rows) <= 0 or int(result_batch_rows) > 2**32 - 1:
                raise ValueError("result_batch_rows must be between 1 and 4294967295")
            command.result_batch_rows = int(result_batch_rows)
        descriptor = flight.FlightDescriptor.for_command(command.SerializeToString())
        options = self._call_options(connection, deadline_ns)
        started_ns = time.perf_counter_ns()
        info = self._invoke(
            "GetFlightInfo",
            lambda: connection.client.get_flight_info(descriptor, options),
        )
        if len(info.endpoints) != 1 or info.endpoints[0].ticket is None:
            raise ProtocolError("Flight query response must contain exactly one ticket")
        query_info = _decode_query_info(info.app_metadata, query_id)
        reader = self._invoke(
            "DoGet",
            lambda: connection.client.do_get(
                info.endpoints[0].ticket, self._call_options(connection, deadline_ns)
            ),
        )
        table = self._invoke("DoGet.read_all", reader.read_all)
        if _key_value_snapshot and not table.schema.equals(info.schema, check_metadata=True):
            raise ProtocolError("state snapshot result schema differs from its prepared ticket")
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000.0
        metrics: dict[str, object] = {
            "executor": "flight",
            "endpoint": self.endpoint,
            "elapsed_ms": elapsed_ms,
            "returned_rows": int(table.num_rows),
            "returned_batches": len(table.to_batches()),
            "query": query_info,
        }
        return table, metrics

    def list_tables(self, *, namespace: str | None = None) -> list[dict[str, object]]:
        """List Catalog tables via the standard Flight SQL metadata command.

        :param namespace: Optional exact namespace filter.
        :returns: Table name, namespace, catalog and table type dictionaries.
        :raises ProtocolError: If the server returns malformed discovery data.
        """
        connection = self._connection("read")
        # An empty protobuf body is CommandGetTables with all filters unset and
        # include_schema=False. Use the standard Any envelope; no private codec.
        command = ProtobufAny(
            type_url="type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables",
            value=b"",
        )
        descriptor = flight.FlightDescriptor.for_command(command.SerializeToString())
        options = self._call_options(connection, None)
        info = self._invoke(
            "GetFlightInfo.tables",
            lambda: connection.client.get_flight_info(descriptor, options),
        )
        if len(info.endpoints) != 1 or info.endpoints[0].ticket is None:
            raise ProtocolError("Flight table metadata must contain exactly one ticket")
        reader = self._invoke(
            "DoGet.tables",
            lambda: connection.client.do_get(info.endpoints[0].ticket, options),
        )
        result = self._invoke("DoGet.tables.read_all", reader.read_all)
        required = {"catalog_name", "db_schema_name", "table_name", "table_type"}
        if not required.issubset(result.column_names):
            raise ProtocolError("Flight table metadata has missing columns")
        tables: list[dict[str, object]] = []
        for row in result.to_pylist():
            if any(not isinstance(row[key], str) for key in required):
                raise ProtocolError("Flight table metadata has invalid field types")
            if namespace is None or row["db_schema_name"] == namespace:
                tables.append(
                    {
                        "name": row["table_name"],
                        "namespace": row["db_schema_name"],
                        "catalog": row["catalog_name"],
                        "table_type": row["table_type"],
                    }
                )
        return tables

    def get_table_schema(
        self,
        table: str,
        *,
        namespace: str = "default",
        deadline_ns: int | None = None,
        fixed_snapshot_id: bytes | str | uuid.UUID | None = None,
    ) -> pa.Schema:
        """Resolve one table schema without transferring table rows."""
        alias = "zippy_schema_source"
        result, _ = self.read_sql(
            f'SELECT * FROM "{alias}" LIMIT 0',
            {alias: {"namespace": namespace, "table": table}},
            result_batch_rows=1,
            deadline_ns=deadline_ns,
            fixed_snapshot_id=fixed_snapshot_id,
        )
        return result.schema

    def write_table(
        self,
        stream_name: str,
        table: pa.Table,
        *,
        namespace: str = "default",
        first_batch_sequence: int = 0,
        durability: str = "wal",
        mutation: str = "append",
        producer_id: bytes | str | uuid.UUID | None = None,
        connection_id: bytes | str | uuid.UUID | None = None,
        expected_writer_epoch: int | None = None,
        expected_table_id: bytes | str | uuid.UUID | None = None,
        max_reconnect_attempts: int = 0,
    ) -> list[dict[str, object]]:
        """Write one Arrow table using bounded producer-fenced Flight DoPut streams.

        :param expected_table_id: Optional immutable Catalog identity, checked before writer
            admission on every new stream. Prevents a stale task writing to a same-name
            replacement; requires the server's ingest.table_identity.v1 capability.
        :param max_reconnect_attempts: Bounded reconnects per durable batch, from 0 to 10.
            Recovery sends one batch per DoPut and reuses its exact put ID and sequence.
            Requires an immutable table identity and ingest.resume_after_restart.v1.
        :returns: Ordered receipts for confirmed batches.
        :raises CapabilityError: The server cannot enforce a requested identity guard.
        :raises AmbiguousWriteError: Some write results could not be determined.
        """
        durability = str(durability).strip().lower()
        if type(max_reconnect_attempts) is not int or not 0 <= max_reconnect_attempts <= 10:
            raise ValueError("max_reconnect_attempts must be between 0 and 10")
        if max_reconnect_attempts:
            if "ingest.resume_after_restart.v1" not in self.capabilities.names:
                raise CapabilityError(
                    "Flight listener does not advertise durable producer recovery"
                )
            if durability not in {"wal", "persisted"} or expected_table_id is None:
                raise ValueError(
                    "write recovery requires durable writes and an immutable table identity"
                )
        table_identity = None
        if expected_table_id is not None:
            if "ingest.table_identity.v1" not in self.capabilities.names:
                raise CapabilityError(
                    "Flight listener cannot guard immutable output table identity"
                )
            table_identity = _stable_id_bytes(expected_table_id, "expected_table_id")
        if mutation not in {"append", "frame", "upsert", "delete"}:
            raise ValueError("mutation must be 'append', 'frame', 'upsert' or 'delete'")
        if (
            mutation in {"upsert", "delete"}
            and "ingest.key_value.v1" not in self.capabilities.names
        ):
            raise CapabilityError("Flight listener does not advertise native key-value ingest")
        if mutation == "frame" and "ingest.frame.v1" not in self.capabilities.names:
            raise CapabilityError("Flight listener does not advertise native frame ingest")
        if durability not in {"memory", "wal", "persisted"}:
            raise ValueError("durability must be 'memory', 'wal' or 'persisted'")
        if durability == "persisted" and not self.capabilities.persisted_ingest:
            raise CapabilityError("Flight listener does not advertise append + persisted ingest")
        if durability == "wal" and not self.capabilities.wal_ingest:
            raise CapabilityError("Flight listener does not advertise append + WAL ingest")
        if durability == "memory" and not self.capabilities.memory_ingest:
            raise CapabilityError("Flight listener does not advertise append + memory ingest")
        if not isinstance(table, pa.Table):
            raise TypeError("Flight write_table requires a pyarrow.Table")
        if table.num_rows == 0:
            return []
        if first_batch_sequence < 0:
            raise ValueError("first_batch_sequence must be non-negative")
        if expected_writer_epoch is not None and expected_writer_epoch < 0:
            raise ValueError("expected_writer_epoch must be non-negative or None")
        producer_id_bytes = (
            uuid.uuid4().bytes
            if producer_id is None
            else _stable_id_bytes(producer_id, "producer_id")
        )
        if connection_id is None:
            keep_open = False
            connection_id_bytes = uuid.uuid4().bytes
        else:
            keep_open = True
            connection_id_bytes = _stable_id_bytes(connection_id, "connection_id")
        if mutation != "append":
            row_limit = min(
                self.capabilities.ingest_max_rows_per_batch,
                self.capabilities.ingest_max_rows_per_put,
            )
            if mutation == "frame":
                row_limit = min(row_limit, self.capabilities.ingest_max_rows_per_frame)
            byte_limit = min(
                self.capabilities.ingest_max_batch_bytes,
                self.capabilities.ingest_max_bytes_per_put,
                self.capabilities.max_message_bytes,
                self._configured_max_message_bytes,
            )
            if table.num_rows > row_limit or table.nbytes > byte_limit:
                unit = "frame" if mutation == "frame" else "key-value mutation"
                raise CapabilityError(f"one complete {unit} exceeds the server atomic batch limit")
            batches = table.combine_chunks().to_batches()
            if len(batches) != 1:
                raise ProtocolError("an atomic mutation must encode exactly one Arrow batch")
        else:
            batches = list(self._bounded_batches(table))
        receipts: list[dict[str, object]] = []
        sequence = int(first_batch_sequence)
        writer_epoch = expected_writer_epoch
        try:
            for batch in batches:
                receipt = self._write_batch_with_recovery(
                    str(stream_name),
                    table.schema,
                    batch,
                    namespace=namespace,
                    batch_sequence=sequence,
                    durability=durability,
                    mutation=mutation,
                    producer_id=producer_id_bytes,
                    connection_id=connection_id_bytes,
                    expected_writer_epoch=writer_epoch,
                    expected_table_id=table_identity,
                    max_reconnect_attempts=max_reconnect_attempts,
                )
                receipts.append(receipt)
                # _decode_put_receipt already proved a positive integer producer epoch.
                writer_epoch = cast(int, receipt["producer_epoch"])
                sequence += 1
            if not keep_open:
                with self._put_lock:
                    self._close_write_locked(connection_id_bytes)
        except ServerError as error:
            error.receipts = [*receipts, *error.receipts]
            raise
        except AmbiguousWriteError as error:
            raise AmbiguousWriteError(
                "Flight write stopped after an ambiguous partial commit; "
                "do not retry the original table blindly",
                receipts=[*receipts, *error.receipts],
            ) from error
        return receipts

    def close_write(self, connection_id: bytes | str | uuid.UUID) -> None:
        """Finish one reusable logical DoPut connection after all receipts were consumed."""
        connection_id_bytes = _stable_id_bytes(connection_id, "connection_id")
        with self._put_lock:
            self._retire_write_locked(connection_id_bytes)

    def close(self) -> None:
        """Close reusable puts and all role-specific Flight channels."""
        close_error: BaseException | None = None
        with self._put_lock:
            active_connections = list(self._active_puts)
            for connection_id in active_connections:
                try:
                    self._retire_write_locked(connection_id)
                except BaseException as error:
                    if close_error is None:
                        close_error = error
        with self._connection_lock:
            if self._closed:
                return
            self._closed = True
            log_readers = list(self._log_readers)
            self._log_readers.clear()
            connections = self._owned_connections
            self._owned_connections = []
            self._read_connection = None
            self._writer_connection = None
            self._admin_connection = None
            self._fixed_connections.clear()
        for reader in log_readers:
            try:
                reader.close()
            except BaseException as error:
                if close_error is None:
                    close_error = error
        for connection in connections:
            try:
                self._close_session(connection)
            except BaseException as error:
                if close_error is None:
                    close_error = error
            finally:
                try:
                    connection.client.close()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
        if close_error is not None:
            raise close_error

    def _close_session(self, connection: _RoleConnection) -> None:
        """Revoke an owned session without authenticating a replacement during cleanup."""
        if connection.session.expires_at_ns <= time.time_ns():
            return
        request = wire.SessionCloseRequest(protocol_version=FLIGHT_PROTOCOL_VERSION)
        try:
            results = self._invoke(
                "CloseSession",
                lambda: list(
                    connection.client.do_action(
                        flight.Action("zippy.close_session.v1", request.SerializeToString()),
                        options=self._call_options(connection),
                    )
                ),
            )
        except (AuthenticationError, ZippyConnectionError) as error:
            # Expiry/restart already invalidated this session; remote cleanup cannot succeed.
            logger.warning("flight session cleanup unavailable error=[{}]", error)
            return
        except ServerError as error:
            if error.code != "UNAUTHENTICATED":
                raise
            logger.warning(
                "flight session already invalidated during cleanup code=[{}]", error.code
            )
            return
        if len(results) != 1:
            raise ProtocolError("session close must return exactly one result")
        response = wire.SessionCloseResponse()
        try:
            response.ParseFromString(_buffer_bytes(results[0].body))
        except DecodeError as error:
            raise ProtocolError("session close response is malformed") from error
        if response.protocol_version != FLIGHT_PROTOCOL_VERSION or not response.closed:
            raise ProtocolError("session close response did not confirm revocation")

    def _write_batch_with_recovery(
        self,
        stream_name: str,
        schema: pa.Schema,
        batch: pa.RecordBatch,
        *,
        namespace: str,
        batch_sequence: int,
        durability: str,
        mutation: str,
        producer_id: bytes,
        connection_id: bytes,
        expected_writer_epoch: int | None,
        expected_table_id: bytes | None,
        max_reconnect_attempts: int,
    ) -> dict[str, object]:
        if not max_reconnect_attempts:
            return self._write_put_batch(
                stream_name,
                schema,
                batch,
                namespace=namespace,
                batch_sequence=batch_sequence,
                durability=durability,
                mutation=mutation,
                producer_id=producer_id,
                connection_id=connection_id,
                expected_writer_epoch=expected_writer_epoch,
                expected_table_id=expected_table_id,
            )
        put_id = uuid.uuid4().bytes
        receipt: dict[str, object] | None = None
        with self._put_lock:
            self._retire_write_locked(connection_id)
            for attempt in range(max_reconnect_attempts + 1):
                try:
                    if attempt:
                        with self._connection_lock:
                            self._writer_connection = self._open_connection("writer")
                    receipt = self._write_put_batch(
                        stream_name,
                        schema,
                        batch,
                        namespace=namespace,
                        batch_sequence=batch_sequence,
                        durability=durability,
                        mutation=mutation,
                        producer_id=producer_id,
                        connection_id=connection_id,
                        expected_writer_epoch=expected_writer_epoch,
                        expected_table_id=expected_table_id,
                        put_id=put_id,
                    )
                    self._close_write_locked(connection_id)
                    return receipt
                except Exception as error:
                    if attempt == max_reconnect_attempts or not _retryable_write_failure(error):
                        if attempt or isinstance(error, AmbiguousWriteError):
                            raise AmbiguousWriteError(
                                "durable write recovery could not prove the final batch outcome",
                                receipts=[] if receipt is None else [receipt],
                            ) from error
                        raise
                    logger.warning(
                        "durable write reconnecting source=[{}] sequence=[{}] attempt=[{}] error=[{}]",
                        stream_name,
                        batch_sequence,
                        attempt + 1,
                        error,
                    )
                    time.sleep(min(1.0, 0.05 * 2**attempt))
        raise AssertionError("bounded write recovery exhausted without a result")

    def _write_put_batch(
        self,
        stream_name: str,
        schema: pa.Schema,
        batch: pa.RecordBatch,
        *,
        namespace: str,
        batch_sequence: int,
        durability: str,
        mutation: str,
        producer_id: bytes,
        connection_id: bytes,
        expected_writer_epoch: int | None,
        expected_table_id: bytes | None = None,
        put_id: bytes | None = None,
    ) -> dict[str, object]:
        with self._put_lock:
            if (
                self.capabilities.ingest_max_batches_per_put <= 0
                or self.capabilities.ingest_max_rows_per_put <= 0
                or self.capabilities.ingest_max_bytes_per_put <= 0
            ):
                raise ProtocolError("Flight ingest capabilities contain invalid per-put limits")
            if (
                batch.num_rows > self.capabilities.ingest_max_rows_per_put
                or batch.nbytes > self.capabilities.ingest_max_bytes_per_put
            ):
                raise CapabilityError(
                    "one Flight batch exceeds the server per-put limit "
                    f"rows=[{batch.num_rows}] bytes=[{batch.nbytes}]"
                )
            active = self._active_puts.get(connection_id)
            if active is not None and not self._put_matches(
                active,
                stream_name=stream_name,
                namespace=namespace,
                schema=schema,
                batch_sequence=batch_sequence,
                durability=durability,
                mutation=mutation,
                producer_id=producer_id,
                expected_writer_epoch=expected_writer_epoch,
                expected_table_id=expected_table_id,
            ):
                self._retire_write_locked(connection_id)
                active = None
            if active is not None and not self._put_has_capacity(active, batch):
                self._retire_write_locked(connection_id)
                active = None
            if active is None:
                active = self._open_put(
                    stream_name,
                    schema,
                    namespace=namespace,
                    first_batch_sequence=batch_sequence,
                    durability=durability,
                    mutation=mutation,
                    producer_id=producer_id,
                    connection_id=connection_id,
                    expected_writer_epoch=expected_writer_epoch,
                    expected_table_id=expected_table_id,
                    put_id=put_id,
                )
                self._active_puts[connection_id] = active
            missing_receipt = False
            try:
                self._invoke(
                    "DoPut.write_batch",
                    lambda: active.writer.write_batch(batch),
                )
                metadata = self._invoke("DoPut.read_receipt", active.metadata_reader.read)
                if metadata is None:
                    missing_receipt = True
                    raise ZippyConnectionError(
                        "Flight DoPut ended before returning every batch receipt"
                    )
                receipt = _decode_put_receipt(
                    metadata,
                    put_id=active.put_id,
                    expected_sequence=batch_sequence,
                    expected_rows=batch.num_rows,
                    expected_batch_index=active.batch_count,
                    requested_durability=durability,
                )
            except BaseException as error:
                self._active_puts.pop(connection_id, None)
                terminal_error: BaseException | None = None
                try:
                    self._invoke("DoPut.close", active.writer.close)
                except BaseException as close_error:
                    terminal_error = close_error
                # Schema rejection happens before admission. Other terminal failures can
                # follow a commit and must retain the ambiguous-write contract.
                if isinstance(error, ServerError) and error.code == "SCHEMA_MISMATCH":
                    raise
                if (
                    missing_receipt
                    and isinstance(terminal_error, ServerError)
                    and terminal_error.code == "SCHEMA_MISMATCH"
                ):
                    raise terminal_error from error
                raise AmbiguousWriteError(
                    "Flight DoPut stopped before the current batch outcome was certain",
                    receipts=[],
                ) from (terminal_error if terminal_error is not None else error)
            active.batch_count += 1
            active.row_count += batch.num_rows
            active.byte_count += batch.nbytes
            active.next_batch_sequence += 1
            active.producer_epoch = cast(int, receipt["producer_epoch"])
            return receipt

    def _open_put(
        self,
        stream_name: str,
        schema: pa.Schema,
        *,
        namespace: str,
        first_batch_sequence: int,
        durability: str,
        mutation: str,
        producer_id: bytes,
        connection_id: bytes,
        expected_writer_epoch: int | None,
        expected_table_id: bytes | None = None,
        put_id: bytes | None = None,
    ) -> _ActivePut:
        connection = self._connection("writer")
        put_id = uuid.uuid4().bytes if put_id is None else put_id
        command = wire.PutCommand(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            session_id=connection.session.session_id,
            put_id=put_id,
            stream_name=stream_name,
            namespace=namespace,
            first_batch_sequence=first_batch_sequence,
            mutation={
                "append": wire.PUT_MUTATION_APPEND,
                "frame": wire.PUT_MUTATION_FRAME,
                "upsert": wire.PUT_MUTATION_KEY_VALUE_UPSERT,
                "delete": wire.PUT_MUTATION_KEY_VALUE_DELETE,
            }[mutation],
            durability=_PUT_DURABILITY[durability],
            producer_id=producer_id,
            connection_id=connection_id,
        )
        if expected_table_id is not None:
            command.expected_table_id = expected_table_id
        if expected_writer_epoch is not None:
            command.expected_writer_epoch = expected_writer_epoch
        descriptor = flight.FlightDescriptor.for_command(command.SerializeToString())
        options = self._call_options(connection)
        writer, metadata_reader = self._invoke(
            "DoPut",
            lambda: connection.client.do_put(descriptor, schema, options),
        )
        opened_ns = time.perf_counter_ns()
        timeout_ns = max(1, int(self.timeout_sec * 1_000_000_000))
        return _ActivePut(
            stream_name=stream_name,
            namespace=namespace,
            expected_table_id=expected_table_id,
            schema=schema,
            durability=durability,
            mutation=mutation,
            producer_id=producer_id,
            connection_id=connection_id,
            put_id=put_id,
            writer=writer,
            metadata_reader=metadata_reader,
            next_batch_sequence=first_batch_sequence,
            retire_at_ns=opened_ns + max(1, timeout_ns * 4 // 5),
            deadline_at_ns=opened_ns + timeout_ns,
        )

    def _put_matches(
        self,
        active: _ActivePut,
        *,
        stream_name: str,
        schema: pa.Schema,
        namespace: str,
        batch_sequence: int,
        durability: str,
        mutation: str,
        producer_id: bytes,
        expected_writer_epoch: int | None,
        expected_table_id: bytes | None = None,
    ) -> bool:
        return (
            active.expected_table_id == expected_table_id
            and active.namespace == namespace
            and active.stream_name == stream_name
            and active.schema.equals(schema, check_metadata=True)
            and active.next_batch_sequence == batch_sequence
            and active.durability == durability
            and active.mutation == mutation
            and active.producer_id == producer_id
            and (
                expected_writer_epoch is None
                or active.producer_epoch is None
                or active.producer_epoch == expected_writer_epoch
            )
        )

    def _put_has_capacity(self, active: _ActivePut, batch: pa.RecordBatch) -> bool:
        return (
            time.perf_counter_ns() < active.retire_at_ns
            and active.batch_count < self.capabilities.ingest_max_batches_per_put
            and active.row_count + batch.num_rows <= self.capabilities.ingest_max_rows_per_put
            and active.byte_count + batch.nbytes <= self.capabilities.ingest_max_bytes_per_put
        )

    def _retire_write_locked(self, connection_id: bytes) -> None:
        active = self._active_puts.get(connection_id)
        if active is None:
            return
        if time.perf_counter_ns() < active.deadline_at_ns:
            self._close_write_locked(connection_id)
            return
        self._active_puts.pop(connection_id, None)
        try:
            active.writer.close()
        except BaseException:
            pass

    def _close_write_locked(self, connection_id: bytes) -> None:
        active = self._active_puts.pop(connection_id, None)
        if active is None:
            return
        try:
            self._invoke("DoPut.done_writing", active.writer.done_writing)
            terminal = self._invoke("DoPut.read_terminal", active.metadata_reader.read)
            if terminal is not None:
                raise ProtocolError("Flight DoPut returned unexpected trailing metadata")
        except BaseException as error:
            try:
                active.writer.close()
            except BaseException:
                pass
            raise AmbiguousWriteError(
                "Flight DoPut could not confirm a clean terminal boundary",
                receipts=[],
            ) from error
        self._invoke("DoPut.close", active.writer.close)

    def _discover_capabilities(self, client: flight.FlightClient) -> FlightCapabilities:
        request = wire.CapabilitiesRequest(protocol_version=FLIGHT_PROTOCOL_VERSION)
        options = flight.FlightCallOptions(timeout=self.timeout_sec)
        results = self._invoke(
            "DoAction.capabilities",
            lambda: list(
                client.do_action(
                    flight.Action(CAPABILITIES_ACTION, request.SerializeToString()),
                    options,
                )
            ),
        )
        if len(results) != 1:
            raise ProtocolError("Flight capabilities action must return exactly one result")
        response = wire.CapabilitiesResponse()
        try:
            response.ParseFromString(bytes(results[0].body))
        except DecodeError as error:
            raise ProtocolError("Flight capabilities response is malformed") from error
        if response.protocol_version != FLIGHT_PROTOCOL_VERSION:
            raise ProtocolError(
                "unsupported Flight protocol version "
                f"server=[{response.protocol_version}] client=[{FLIGHT_PROTOCOL_VERSION}]"
            )
        if response.max_message_bytes <= 0:
            raise ProtocolError("Flight capabilities response has no message byte limit")
        return FlightCapabilities(
            protocol_version=int(response.protocol_version),
            server_version=str(response.server_version),
            names=frozenset(str(name) for name in response.capabilities),
            max_message_bytes=int(response.max_message_bytes),
            max_pending_tickets=int(response.max_pending_tickets),
            authentication_required=bool(response.authentication_required),
            session_ttl_ms=int(response.session_ttl_ms),
            writer_authentication_enabled=bool(response.writer_authentication_enabled),
            ingest_enabled=bool(response.ingest_enabled),
            ingest_max_concurrent_puts=int(response.ingest_max_concurrent_puts),
            ingest_max_batches_per_put=int(response.ingest_max_batches_per_put),
            ingest_max_rows_per_batch=int(response.ingest_max_rows_per_batch),
            ingest_max_rows_per_put=int(response.ingest_max_rows_per_put),
            ingest_max_batch_bytes=int(response.ingest_max_batch_bytes),
            ingest_max_bytes_per_put=int(response.ingest_max_bytes_per_put),
            ingest_max_rows_per_frame=int(response.ingest_max_rows_per_frame),
        )

    def _connection(self, role: str, *, timeout_sec: float | None = None) -> _RoleConnection:
        with self._connection_lock:
            if self._closed:
                raise RuntimeError("Flight transport is closed")
            current = getattr(self, f"_{role}_connection")
            now_ns = time.time_ns()
            if current is not None and current.session.expires_at_ns > (
                now_ns + _SESSION_REFRESH_MARGIN_NS
            ):
                return current
            connection = self._open_connection(role, timeout_sec=timeout_sec)
            setattr(self, f"_{role}_connection", connection)
            return connection

    def refresh_read_session(self, *, timeout_sec: float | None = None) -> None:
        """Authenticate a new read session after a server restart or explicit state recovery.

        :param timeout_sec: Optional authentication timeout for bounded recovery.
        :raises AuthenticationError: The configured bootstrap credential is no longer valid.
        :raises RuntimeError: This transport is closed.
        :note: Existing stream objects retain their own RPC ownership; they are not force-closed.
        """
        with self._connection_lock:
            if self._closed:
                raise RuntimeError("Flight transport is closed")
            self._read_connection = self._open_connection("read", timeout_sec=timeout_sec)

    def _open_connection(self, role: str, *, timeout_sec: float | None = None) -> _RoleConnection:
        # An expired session cannot own a usable fixed snapshot or continue a log RPC.
        now_ns = time.time_ns()
        retained = []
        for connection in self._owned_connections:
            if connection.session.expires_at_ns <= now_ns:
                connection.client.close()
            else:
                retained.append(connection)
        self._owned_connections = retained
        expected_role = {
            "read": wire.SESSION_ROLE_READ,
            "writer": wire.SESSION_ROLE_WRITER,
            "admin": wire.SESSION_ROLE_ADMINISTRATOR,
        }[role]
        client = self._new_client()
        token = getattr(self, f"_{role}_token")
        if self.capabilities.authentication_required:
            if "auth.bearer_handshake.v1" not in self.capabilities.names:
                client.close()
                raise ProtocolError("Flight listener does not advertise bearer handshake")
            if token is None:
                client.close()
                variable = {
                    "read": "ZIPPY_FLIGHT_AUTH_TOKEN",
                    "writer": "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN",
                    "admin": "ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN",
                }[role]
                raise AuthenticationError(
                    f"Flight {role} bootstrap token is required environment=[{variable}]"
                )
        else:
            if "auth.local_read_handshake.v1" not in self.capabilities.names:
                client.close()
                raise ProtocolError("Flight listener does not advertise local read handshake")
            if role != "read":
                client.close()
                raise CapabilityError("local Flight sessions are read-only")
            if token is not None:
                client.close()
                raise AuthenticationError(
                    "configured Flight credentials require authenticated service"
                )
        handler = _SessionAuthHandler()
        options = flight.FlightCallOptions(
            timeout=self.timeout_sec if timeout_sec is None else timeout_sec,
            headers=(
                [(_AUTHORIZATION_METADATA_KEY, f"Bearer {token}".encode("ascii"))]
                if token is not None
                else []
            ),
        )
        try:
            self._invoke("Handshake", lambda: client.authenticate(handler, options))
            response = handler.response
            if response is None:
                raise ProtocolError("Flight handshake completed without a session response")
            session = _validated_session(response, expected_role)
            connection = _RoleConnection(client=client, session=session, auth_handler=handler)
            self._owned_connections.append(connection)
            return connection
        except BaseException:
            client.close()
            raise

    def _new_client(self) -> flight.FlightClient:
        host, port = split_host_port(self.endpoint)
        rendered_host = f"[{host}]" if ":" in host else host
        scheme = "grpc+tls" if self.tls_enabled else "grpc"
        capabilities = getattr(self, "capabilities", None)
        message_limit = self._configured_max_message_bytes
        if capabilities is not None:
            message_limit = min(message_limit, capabilities.max_message_bytes)
        kwargs: dict[str, object] = {
            "write_size_limit_bytes": message_limit,
            "generic_options": [
                ("grpc.max_receive_message_length", message_limit),
                ("grpc.max_send_message_length", message_limit),
            ],
        }
        if self._tls_root_certs is not None:
            kwargs["tls_root_certs"] = self._tls_root_certs
        if self._cert_chain is not None:
            kwargs["cert_chain"] = self._cert_chain
            kwargs["private_key"] = self._private_key
        if self._override_hostname is not None:
            kwargs["override_hostname"] = self._override_hostname
        return flight.connect(f"{scheme}://{rendered_host}:{port}", **kwargs)

    def _call_options(
        self,
        connection: _RoleConnection,
        deadline_ns: int | None = None,
        *,
        streaming: bool = False,
    ) -> flight.FlightCallOptions:
        timeout = None if streaming else self.timeout_sec
        if deadline_ns is not None:
            remaining = (int(deadline_ns) - time.time_ns()) / 1_000_000_000.0
            if remaining <= 0:
                raise TimeoutError("Flight query deadline has already elapsed")
            timeout = remaining if timeout is None else min(timeout, remaining)
        # PyArrow's custom auth handler emits auth-token-bin. The Rust service
        # uses the explicit Bearer contract, so attach the issued session token.
        headers = [
            (
                _AUTHORIZATION_METADATA_KEY,
                f"Bearer {connection.session.bearer_token}".encode("ascii"),
            )
        ]
        return flight.FlightCallOptions(
            timeout=timeout,
            headers=headers,
            read_options=pa.ipc.IpcReadOptions(ensure_alignment=pa.ipc.Alignment.DataTypeSpecific),
        )

    def _bounded_batches(self, table: pa.Table) -> Iterable[pa.RecordBatch]:
        row_limit = self.capabilities.ingest_max_rows_per_batch
        byte_limit = self.capabilities.ingest_max_batch_bytes
        if row_limit <= 0 or byte_limit <= 0:
            raise ProtocolError("Flight ingest capabilities contain invalid batch limits")
        for batch in table.to_batches(max_chunksize=row_limit):
            yield from _split_batch_by_bytes(batch, byte_limit)

    @staticmethod
    def _invoke(operation: str, function: Callable[[], _T]) -> _T:
        try:
            return function()
        except (flight.FlightError, flight.FlightWriteSizeExceededError) as error:
            raise _translate_flight_error(error, operation) from error


def _require_positive_uint64(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**64:
        raise ValueError(f"{name} must be a positive uint64")


def _schema_ipc(schema: pa.Schema) -> bytes:
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, schema):
        pass
    return output.getvalue().to_pybytes()


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    if not token or not token.isascii():
        raise ValueError("Flight bootstrap tokens must be non-empty ASCII")
    return token


def _validate_state_snapshot(table: pa.Table, metrics: dict[str, object]) -> None:
    """Check snapshot identity and cursor without creating another copy of the state."""
    try:
        metadata = table.schema.metadata or {}
        if metadata[b"zippy_kv_protocol"] != b"2":
            raise ValueError("unsupported state protocol")
        sequence = int(metadata[b"zippy_kv_seq"])
        version = int(metadata[b"zippy_kv_version"])
        if not (0 <= version <= sequence <= 2**63 - 1) or (sequence == 0) != (version == 0):
            raise ValueError("invalid state cursor")
        stream = metadata[b"zippy_kv_stream_id"].decode("ascii")
        if uuid.UUID(hex=stream).hex != stream:
            raise ValueError("noncanonical state identity")
        keys = json.loads(metadata[b"zippy_kv_keys"])
        if (
            not isinstance(keys, list)
            or not keys
            or any(not isinstance(key, str) for key in keys)
            or len(set(keys)) != len(keys)
        ):
            raise ValueError("invalid state keys")
        for key in keys:
            if table.schema.field(key).type != pa.string() or table.column(key).null_count:
                raise ValueError("invalid state key column")
        query = metrics["query"]
        if not isinstance(query, dict):
            raise ValueError("missing snapshot query identity")
        snapshots = query["snapshots"]
        if (
            not isinstance(snapshots, list)
            or len(snapshots) != 1
            or uuid.UUID(snapshots[0]["table_id"]).hex != stream
        ):
            raise ValueError("state identity differs from the pinned query table")
    except (KeyError, ValueError, TypeError, UnicodeError, AttributeError) as error:
        raise ProtocolError("invalid native key-value snapshot metadata") from error


def _query_bindings(
    bindings: Mapping[str, object] | Iterable[Mapping[str, object]],
) -> list[wire.TableBinding]:
    if isinstance(bindings, Mapping):
        items = [_binding_from_value(str(sql_name), value) for sql_name, value in bindings.items()]
    else:
        items = []
        for value in bindings:
            if not isinstance(value, Mapping):
                raise TypeError("Flight binding entries must be mappings")
            sql_name = value.get("sql_name")
            if not isinstance(sql_name, str):
                raise ValueError("Flight binding objects require a string sql_name")
            if "table_id" in value:
                target: object = {"table_id": value["table_id"]}
            else:
                table = value.get("table")
                if table is None:
                    raise ValueError("Flight binding objects require table or table_id")
                if isinstance(table, Mapping):
                    target = table
                else:
                    target = {
                        "namespace": value.get("namespace", "default"),
                        "table": table,
                    }
            items.append(_binding_from_value(sql_name, target))
    if not items:
        raise ValueError("Flight query requires at least one explicit table binding")
    names = [item.sql_name for item in items]
    if len(set(names)) != len(names):
        raise ValueError("Flight query binding names must be unique")
    return items


def _binding_from_value(sql_name: str, value: object) -> wire.TableBinding:
    namespace = "default"
    table: str | None = None
    table_id: bytes | None = None
    if isinstance(value, str):
        table = value
    elif isinstance(value, (tuple, list)) and len(value) == 2:
        namespace, table = str(value[0]), str(value[1])
    elif isinstance(value, Mapping):
        if value.get("type") == "id" or "table_id" in value:
            raw_id = value.get("table_id") or value.get("id")
            table_id = _stable_id_bytes(raw_id, "table_id")
        else:
            namespace = str(value.get("namespace", "default"))
            table_value = value.get("table") or value.get("name")
            table = None if table_value is None else str(table_value)
    else:
        raise TypeError(
            "Flight binding values must be a table name, (namespace, table), or mapping"
        )
    if not sql_name or "\x00" in sql_name:
        raise ValueError("Flight binding sql_name must be non-empty and contain no NUL")
    if table_id is not None:
        return wire.TableBinding(sql_name=sql_name, table_id=table_id)
    if not namespace or not table or "\x00" in namespace or "\x00" in table:
        raise ValueError("Flight binding namespace and table must be non-empty")
    return wire.TableBinding(
        sql_name=sql_name,
        name=wire.TableName(namespace=namespace, table=table),
    )


def _query_priority(value: str) -> wire.QueryPriority:
    priorities = {
        "interactive": wire.QUERY_PRIORITY_INTERACTIVE,
        "normal": wire.QUERY_PRIORITY_NORMAL,
        "maintenance": wire.QUERY_PRIORITY_MAINTENANCE,
    }
    try:
        return priorities[str(value).lower()]
    except KeyError as error:
        raise ValueError(f"unsupported Flight query priority=[{value}]") from error


def _resource_limit(value: int | None, *, default_kind: str) -> wire.ResourceLimit:
    if value is None:
        kind = (
            wire.ResourceLimit.KIND_SERVER_DEFAULT
            if default_kind == "server"
            else wire.ResourceLimit.KIND_DISABLED
        )
        return wire.ResourceLimit(kind=kind, bytes=0)
    amount = int(value)
    if amount < 0:
        raise ValueError("Flight resource limits must be non-negative")
    if amount == 0:
        return wire.ResourceLimit(kind=wire.ResourceLimit.KIND_DISABLED, bytes=0)
    return wire.ResourceLimit(kind=wire.ResourceLimit.KIND_BYTES, bytes=amount)


def _validated_session(
    response: wire.SessionOpenResponse,
    expected_role: wire.SessionRole,
) -> _Session:
    if response.protocol_version != FLIGHT_PROTOCOL_VERSION:
        raise ProtocolError(
            "unsupported Flight session protocol version "
            f"server=[{response.protocol_version}] client=[{FLIGHT_PROTOCOL_VERSION}]"
        )
    session_id = bytes(response.session_id)
    if len(session_id) != 16 or session_id == bytes(16):
        raise ProtocolError("Flight session response contains an invalid session_id")
    if response.role != expected_role:
        raise AuthenticationError(
            "Flight bootstrap token opened the wrong session role "
            f"expected=[{expected_role}] actual=[{response.role}]"
        )
    if not response.bearer_token.startswith("zfs1_") or not response.bearer_token.isascii():
        raise ProtocolError("Flight session response contains an invalid bearer token")
    if response.expires_at_ns <= time.time_ns():
        raise ProtocolError("Flight session response is already expired")
    return _Session(
        session_id=session_id,
        bearer_token=response.bearer_token,
        expires_at_ns=int(response.expires_at_ns),
        role=int(response.role),
    )


def _decode_query_info(payload: object, expected_query_id: bytes) -> dict[str, object]:
    info = wire.QueryInfo()
    try:
        info.ParseFromString(_buffer_bytes(payload))
    except (DecodeError, TypeError) as error:
        raise ProtocolError("Flight QueryInfo metadata is malformed") from error
    if info.protocol_version != FLIGHT_PROTOCOL_VERSION:
        raise ProtocolError("Flight QueryInfo protocol version is unsupported")
    if bytes(info.query_id) != expected_query_id:
        raise ProtocolError("Flight QueryInfo query_id differs from the submitted query")
    return {
        "query_id": str(uuid.UUID(bytes=bytes(info.query_id))),
        "generation": int(info.generation),
        "catalog_revision": int(info.catalog_revision),
        "planned_at_ns": int(info.planned_at_ns),
        "expires_at_ns": int(info.expires_at_ns),
        "snapshots": [
            {
                "table_id": _stable_id_text(snapshot.table_id, "table_id"),
                "snapshot_version": int(snapshot.snapshot_version),
                "schema_version": int(snapshot.schema_version),
            }
            for snapshot in info.snapshots
        ],
    }


def _decode_put_receipt(
    payload: object,
    *,
    put_id: bytes,
    expected_sequence: int,
    expected_rows: int,
    expected_batch_index: int,
    requested_durability: str,
) -> dict[str, object]:
    receipt = wire.PutBatchResult()
    try:
        receipt.ParseFromString(_buffer_bytes(payload))
    except (DecodeError, TypeError) as error:
        raise ProtocolError("Flight DoPut receipt is malformed") from error
    if receipt.protocol_version != FLIGHT_PROTOCOL_VERSION:
        raise ProtocolError("Flight DoPut receipt protocol version is unsupported")
    if bytes(receipt.put_id) != put_id:
        raise ProtocolError("Flight DoPut receipt put_id differs from the request")
    if receipt.batch_sequence != expected_sequence:
        raise ProtocolError(
            "Flight DoPut receipt sequence differs from the request "
            f"expected=[{expected_sequence}] actual=[{receipt.batch_sequence}]"
        )
    if receipt.row_count != expected_rows:
        raise ProtocolError(
            "Flight DoPut receipt row count differs from the submitted batch "
            f"expected=[{expected_rows}] actual=[{receipt.row_count}]"
        )
    expected_request_id = _put_batch_request_id(put_id, expected_batch_index)
    if bytes(receipt.request_id) != expected_request_id:
        raise ProtocolError("Flight DoPut receipt request_id differs from the batch identity")
    if receipt.producer_epoch <= 0:
        raise ProtocolError("Flight DoPut receipt has no producer epoch")
    if receipt.durability_reached < _PUT_DURABILITY[requested_durability]:
        raise ProtocolError(
            "Flight DoPut did not reach requested durability "
            f"requested=[{requested_durability}] actual=[{receipt.durability_reached}]"
        )
    durability = _PUT_DURABILITY_NAMES.get(receipt.durability_reached)
    if durability is None:
        raise ProtocolError("Flight DoPut receipt contains an unknown durability")
    has_offsets = receipt.HasField("first_log_offset")
    if has_offsets != receipt.HasField("last_log_offset"):
        raise ProtocolError("Flight DoPut receipt contains a partial logical offset range")
    if expected_rows > 0 and not has_offsets:
        raise ProtocolError("Flight DoPut receipt omitted the logical offset range")
    if expected_rows == 0 and has_offsets:
        raise ProtocolError("Flight DoPut zero-row receipt unexpectedly assigned offsets")
    # From here on, offsets are present exactly when the batch has rows.
    first_offset: int | None = None
    last_offset: int | None = None
    if has_offsets:
        first_offset = int(receipt.first_log_offset)
        last_offset = int(receipt.last_log_offset)
        if last_offset - first_offset + 1 != expected_rows:
            raise ProtocolError("Flight DoPut receipt offset width differs from the row count")
    if not receipt.HasField("previous_log_offset") or not receipt.HasField("committed_log_offset"):
        raise ProtocolError("Flight DoPut receipt omitted its logical watermarks")
    previous_offset = _decode_put_watermark(receipt.previous_log_offset)
    committed_offset = _decode_put_watermark(receipt.committed_log_offset)
    if first_offset is not None:
        expected_first_offset = 0 if previous_offset is None else previous_offset + 1
        if first_offset != expected_first_offset:
            raise ProtocolError(
                "Flight DoPut offset range does not continue its previous watermark"
            )
        if committed_offset != last_offset:
            raise ProtocolError("Flight DoPut committed watermark differs from the offset range")
    elif previous_offset != committed_offset:
        raise ProtocolError("Flight DoPut zero-row receipt changed its logical watermark")
    has_snapshot = receipt.HasField("persisted_snapshot_version")
    if (durability == "persisted") != has_snapshot:
        raise ProtocolError("Flight DoPut receipt durability and persisted snapshot disagree")
    if has_snapshot and receipt.persisted_snapshot_version == 0:
        raise ProtocolError("Flight DoPut persisted snapshot version must be positive")
    return {
        "put_id": str(uuid.UUID(bytes=put_id)),
        "request_id": str(uuid.UUID(bytes=expected_request_id)),
        "batch_sequence": int(receipt.batch_sequence),
        "row_count": int(receipt.row_count),
        "durability": durability,
        "producer_epoch": int(receipt.producer_epoch),
        "first_log_offset": first_offset,
        "last_log_offset": last_offset,
        "previous_log_offset": previous_offset,
        "committed_log_offset": committed_offset,
        "persisted_snapshot_version": (
            int(receipt.persisted_snapshot_version) if has_snapshot else None
        ),
    }


def _put_batch_request_id(put_id: bytes, batch_index: int) -> bytes:
    hasher = hashlib.sha256()
    hasher.update(b"zippy-flight-put-batch-v1\0")
    hasher.update(put_id)
    hasher.update(int(batch_index).to_bytes(8, "little", signed=False))
    result = bytearray(hasher.digest()[:16])
    if result == bytearray(16):
        result[-1] = 1
    return bytes(result)


def _decode_put_watermark(value: wire.OffsetWatermark) -> int | None:
    if value.state == wire.OffsetWatermark.STATE_EMPTY and value.offset == 0:
        return None
    if value.state == wire.OffsetWatermark.STATE_KNOWN:
        return int(value.offset)
    raise ProtocolError("Flight DoPut receipt contains an unsupported logical watermark")


def _split_batch_by_bytes(batch: pa.RecordBatch, limit: int) -> Iterable[pa.RecordBatch]:
    if batch.nbytes <= limit:
        yield batch
        return
    if batch.num_rows <= 1:
        raise CapabilityError(
            "one Arrow row exceeds the Flight batch byte limit "
            f"bytes=[{batch.nbytes}] limit=[{limit}]"
        )
    midpoint = batch.num_rows // 2
    yield from _split_batch_by_bytes(batch.slice(0, midpoint), limit)
    yield from _split_batch_by_bytes(batch.slice(midpoint), limit)


def _translate_flight_error(error: BaseException, operation: str) -> BaseException:
    payload = bytes(getattr(error, "extra_info", b"") or b"")
    if payload:
        envelope = wire.ErrorEnvelope()
        try:
            envelope.ParseFromString(payload)
        except DecodeError:
            return ProtocolError(
                f"Flight returned malformed structured error operation=[{operation}]"
            )
        if envelope.protocol_version != FLIGHT_PROTOCOL_VERSION or not envelope.code:
            return ProtocolError(
                "Flight returned an incompatible structured error "
                f"operation=[{operation}] version=[{envelope.protocol_version}]"
            )
        detail_name = envelope.WhichOneof("details")
        details: dict[str, object] | None = None
        if detail_name is not None:
            detail = getattr(envelope, detail_name)
            details = {
                "kind": detail_name,
                "value": MessageToDict(detail, preserving_proto_field_name=True),
            }
        try:
            return ServerError(
                envelope.code,
                envelope.message,
                retryable=envelope.retryable,
                request_id=_optional_stable_id_text(envelope, "request_id"),
                query_id=_optional_stable_id_text(envelope, "query_id"),
                table_id=_optional_stable_id_text(envelope, "table_id"),
                details=details,
            )
        except ValueError:
            return ProtocolError(
                f"Flight returned malformed structured identifiers operation=[{operation}]"
            )
    if isinstance(error, (flight.FlightUnauthenticatedError, flight.FlightUnauthorizedError)):
        return AuthenticationError(f"Flight authentication failed operation=[{operation}]")
    if isinstance(error, flight.FlightUnavailableError):
        return ZippyConnectionError(f"Flight endpoint is unavailable operation=[{operation}]")
    if isinstance(error, flight.FlightTimedOutError):
        return QueryError(f"Flight request timed out operation=[{operation}]")
    if isinstance(error, flight.FlightWriteSizeExceededError):
        return CapabilityError(
            f"Flight request exceeds the message byte limit operation=[{operation}]"
        )
    if isinstance(error, flight.FlightServerError):
        return ProtocolError(
            f"Flight server error omitted the structured envelope operation=[{operation}]"
        )
    return ZippyConnectionError(f"Flight request failed operation=[{operation}] error=[{error}]")


def _optional_stable_id_text(message: Any, field: str) -> str | None:
    if not message.HasField(field):
        return None
    return _stable_id_text(getattr(message, field), field)


def _stable_id_text(value: object, field: str) -> str:
    raw = _buffer_bytes(value)
    if len(raw) != 16 or raw == bytes(16):
        raise ValueError(f"{field} must contain one non-nil 16-byte ID")
    return str(uuid.UUID(bytes=raw))


def _stable_id_bytes(value: object, field: str) -> bytes:
    if isinstance(value, uuid.UUID):
        raw = value.bytes
    elif isinstance(value, str):
        raw = uuid.UUID(value).bytes
    else:
        raw = _buffer_bytes(value)
    if len(raw) != 16 or raw == bytes(16):
        raise ValueError(f"{field} must contain one non-nil 16-byte ID")
    return raw


def _buffer_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, pa.Buffer):
        return value.to_pybytes()
    raise TypeError(f"value is not a bytes-like buffer type=[{type(value).__name__}]")


def _retryable_write_failure(error: BaseException) -> bool:
    """Retry exact durable bytes only after transport/authentication loss or server backpressure."""
    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, ServerError):
            return error.retryable or error.code == "UNAUTHENTICATED"
        if isinstance(error, (AuthenticationError, ZippyConnectionError)):
            return True
        if error.__cause__ is None:
            return False
        error = error.__cause__
    return False
