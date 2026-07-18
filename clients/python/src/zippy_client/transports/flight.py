"""Arrow Flight query and bounded in-memory ingest transport."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
import threading
import time
from typing import Any, TypeVar
import uuid

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
import pyarrow as pa
import pyarrow.flight as flight

from .._generated import protocol_pb2 as wire
from ..endpoints import normalize_tcp_address, split_host_port
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

_SESSION_METADATA_KEY = b"x-zippy-session-id"
_AUTHORIZATION_METADATA_KEY = b"authorization"
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


@dataclass(frozen=True)
class _Session:
    session_id: bytes
    bearer_token: str | None
    expires_at_ns: int
    role: int


@dataclass(frozen=True)
class _RoleConnection:
    client: flight.FlightClient
    session: _Session
    auth_handler: _SessionAuthHandler | None


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
    Execute server-owned queries and memory-durable writes through Arrow Flight.

    :param endpoint: Connectable ``host:port`` Flight endpoint.
    :type endpoint: str
    :param tls_enabled: Whether to use ``grpc+tls``.
    :type tls_enabled: bool
    :param read_token: Optional read-session bootstrap token.
    :type read_token: str | None
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
        self._configured_max_message_bytes = int(configured_max_message_bytes)
        self._tls_root_certs = tls_root_certs
        self._cert_chain = cert_chain
        self._private_key = private_key
        self._override_hostname = override_hostname
        self._connection_lock = threading.RLock()
        self._read_connection: _RoleConnection | None = None
        self._writer_connection: _RoleConnection | None = None
        self._closed = False
        discovery = self._new_client()
        try:
            self.capabilities = self._discover_capabilities(discovery)
        finally:
            discovery.close()
        if not self.capabilities.query:
            raise CapabilityError("Flight listener does not advertise version 1 query execution")
        if not self.capabilities.structured_errors:
            raise ProtocolError("Flight listener does not advertise structured error envelopes")

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
        stream: bool = False,
    ) -> tuple[pa.Table, dict[str, object]]:
        """Execute one SQL statement with explicit Catalog bindings."""
        del stream  # Flight always transfers a bounded stream of Arrow batches.
        normalized_sql = str(sql)
        if not normalized_sql.strip():
            raise ValueError("Flight query SQL must not be empty")
        connection = self._connection("read")
        query_id = uuid.uuid4().bytes
        command = wire.QueryCommand(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            query_id=query_id,
            session_id=connection.session.session_id,
            sql=normalized_sql,
            bindings=_query_bindings(bindings),
            priority=_query_priority(priority),
            memory_limit=_resource_limit(memory_limit_bytes, default_kind="server"),
            spill_limit=_resource_limit(spill_limit_bytes, default_kind="disabled"),
        )
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
            lambda: connection.client.do_get(info.endpoints[0].ticket, options),
        )
        table = self._invoke("DoGet.read_all", reader.read_all)
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

    def get_table_schema(self, table: str, *, namespace: str = "default") -> pa.Schema:
        """Resolve one table schema without transferring table rows."""
        alias = "zippy_schema_source"
        result, _ = self.read_sql(
            f'SELECT * FROM "{alias}" LIMIT 0',
            {alias: {"namespace": namespace, "table": table}},
            result_batch_rows=1,
        )
        return result.schema

    def write_table(
        self,
        stream_name: str,
        table: pa.Table,
        *,
        first_batch_sequence: int = 0,
    ) -> list[dict[str, object]]:
        """Append one Arrow table using one or more bounded Flight DoPut streams."""
        if not self.capabilities.memory_ingest:
            raise CapabilityError("Flight listener does not advertise append + memory ingest")
        if not isinstance(table, pa.Table):
            raise TypeError("Flight write_table requires a pyarrow.Table")
        if table.num_rows == 0:
            return []
        if first_batch_sequence < 0:
            raise ValueError("first_batch_sequence must be non-negative")
        batches = list(self._bounded_batches(table))
        receipts: list[dict[str, object]] = []
        sequence = int(first_batch_sequence)
        for group in self._put_groups(batches):
            try:
                group_receipts = self._write_put(
                    str(stream_name),
                    table.schema,
                    group,
                    first_batch_sequence=sequence,
                )
            except AmbiguousWriteError as error:
                raise AmbiguousWriteError(
                    "Flight write stopped after an ambiguous partial commit; "
                    "do not retry the original table blindly",
                    receipts=[*receipts, *error.receipts],
                ) from error
            receipts.extend(group_receipts)
            sequence += len(group)
        return receipts

    def close(self) -> None:
        """Close all role-specific Flight channels."""
        with self._connection_lock:
            if self._closed:
                return
            self._closed = True
            clients = [
                connection.client
                for connection in (self._read_connection, self._writer_connection)
                if connection is not None
            ]
            self._read_connection = None
            self._writer_connection = None
        seen: set[int] = set()
        for client in clients:
            if id(client) in seen:
                continue
            seen.add(id(client))
            client.close()

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
        )

    def _connection(self, role: str) -> _RoleConnection:
        with self._connection_lock:
            if self._closed:
                raise RuntimeError("Flight transport is closed")
            current = self._read_connection if role == "read" else self._writer_connection
            now_ns = time.time_ns()
            if current is not None and current.session.expires_at_ns > (
                now_ns + _SESSION_REFRESH_MARGIN_NS
            ):
                return current
            connection = self._open_connection(role)
            if role == "read":
                self._read_connection = connection
            else:
                self._writer_connection = connection
            return connection

    def _open_connection(self, role: str) -> _RoleConnection:
        expected_role = wire.SESSION_ROLE_READ if role == "read" else wire.SESSION_ROLE_WRITER
        client = self._new_client()
        if not self.capabilities.authentication_required:
            if role != "read":
                client.close()
                raise CapabilityError("unauthenticated Flight compatibility mode is read-only")
            session_id = uuid.uuid4().bytes
            return _RoleConnection(
                client=client,
                session=_Session(session_id, None, 2**63 - 1, expected_role),
                auth_handler=None,
            )
        token = self._read_token if role == "read" else self._writer_token
        if token is None:
            client.close()
            variable = (
                "ZIPPY_FLIGHT_AUTH_TOKEN" if role == "read" else "ZIPPY_FLIGHT_AUTH_WRITER_TOKEN"
            )
            raise AuthenticationError(
                f"Flight {role} bootstrap token is required environment=[{variable}]"
            )
        handler = _SessionAuthHandler()
        options = flight.FlightCallOptions(
            timeout=self.timeout_sec,
            headers=[(_AUTHORIZATION_METADATA_KEY, f"Bearer {token}".encode("ascii"))],
        )
        try:
            self._invoke("Handshake", lambda: client.authenticate(handler, options))
            response = handler.response
            if response is None:
                raise ProtocolError("Flight handshake completed without a session response")
            session = _validated_session(response, expected_role)
            return _RoleConnection(client=client, session=session, auth_handler=handler)
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
    ) -> flight.FlightCallOptions:
        timeout = self.timeout_sec
        if deadline_ns is not None:
            remaining = (int(deadline_ns) - time.time_ns()) / 1_000_000_000.0
            if remaining <= 0:
                raise TimeoutError("Flight query deadline has already elapsed")
            timeout = min(timeout, remaining)
        headers = None
        if connection.auth_handler is None:
            session_text = str(uuid.UUID(bytes=connection.session.session_id)).encode("ascii")
            headers = [(_SESSION_METADATA_KEY, session_text)]
        return flight.FlightCallOptions(timeout=timeout, headers=headers)

    def _bounded_batches(self, table: pa.Table) -> Iterable[pa.RecordBatch]:
        row_limit = self.capabilities.ingest_max_rows_per_batch
        byte_limit = self.capabilities.ingest_max_batch_bytes
        if row_limit <= 0 or byte_limit <= 0:
            raise ProtocolError("Flight ingest capabilities contain invalid batch limits")
        for batch in table.to_batches(max_chunksize=row_limit):
            yield from _split_batch_by_bytes(batch, byte_limit)

    def _put_groups(self, batches: list[pa.RecordBatch]) -> Iterable[list[pa.RecordBatch]]:
        max_batches = self.capabilities.ingest_max_batches_per_put
        max_rows = self.capabilities.ingest_max_rows_per_put
        max_bytes = self.capabilities.ingest_max_bytes_per_put
        if min(max_batches, max_rows, max_bytes) <= 0:
            raise ProtocolError("Flight ingest capabilities contain invalid per-put limits")
        group: list[pa.RecordBatch] = []
        rows = 0
        size = 0
        for batch in batches:
            would_overflow = group and (
                len(group) >= max_batches
                or rows + batch.num_rows > max_rows
                or size + batch.nbytes > max_bytes
            )
            if would_overflow:
                yield group
                group, rows, size = [], 0, 0
            if batch.num_rows > max_rows or batch.nbytes > max_bytes:
                raise CapabilityError(
                    "one Flight batch exceeds the server per-put limit "
                    f"rows=[{batch.num_rows}] bytes=[{batch.nbytes}]"
                )
            group.append(batch)
            rows += batch.num_rows
            size += batch.nbytes
        if group:
            yield group

    def _write_put(
        self,
        stream_name: str,
        schema: pa.Schema,
        batches: list[pa.RecordBatch],
        *,
        first_batch_sequence: int,
    ) -> list[dict[str, object]]:
        connection = self._connection("writer")
        put_id = uuid.uuid4().bytes
        command = wire.PutCommand(
            protocol_version=FLIGHT_PROTOCOL_VERSION,
            session_id=connection.session.session_id,
            put_id=put_id,
            stream_name=stream_name,
            first_batch_sequence=first_batch_sequence,
            mutation=wire.PUT_MUTATION_APPEND,
            durability=wire.PUT_DURABILITY_MEMORY,
        )
        descriptor = flight.FlightDescriptor.for_command(command.SerializeToString())
        options = self._call_options(connection)
        writer, metadata_reader = self._invoke(
            "DoPut",
            lambda: connection.client.do_put(descriptor, schema, options),
        )
        receipts: list[dict[str, object]] = []
        try:
            for index, batch in enumerate(batches):
                self._invoke(
                    "DoPut.write_batch",
                    lambda: _write_batch(writer, batch),
                )
                metadata = self._invoke("DoPut.read_receipt", metadata_reader.read)
                if metadata is None:
                    raise ProtocolError("Flight DoPut ended before returning every batch receipt")
                receipt = _decode_put_receipt(
                    metadata,
                    put_id=put_id,
                    expected_sequence=first_batch_sequence + index,
                    expected_rows=batch.num_rows,
                )
                receipts.append(receipt)
            self._invoke("DoPut.done_writing", writer.done_writing)
            terminal = self._invoke("DoPut.read_terminal", metadata_reader.read)
            if terminal is not None:
                raise ProtocolError("Flight DoPut returned unexpected trailing metadata")
        except BaseException as error:
            try:
                writer.close()
            except BaseException:
                pass
            raise AmbiguousWriteError(
                "Flight DoPut stopped after an ambiguous partial commit; "
                "only returned receipts are known to be acknowledged",
                receipts=receipts,
            ) from error
        else:
            self._invoke("DoPut.close", writer.close)
        return receipts

    @staticmethod
    def _invoke(operation: str, function: Callable[[], _T]) -> _T:
        try:
            return function()
        except (flight.FlightError, flight.FlightWriteSizeExceededError) as error:
            raise _translate_flight_error(error, operation) from error


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    if not token or not token.isascii():
        raise ValueError("Flight bootstrap tokens must be non-empty ASCII")
    return token


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
    if receipt.durability_reached != wire.PUT_DURABILITY_MEMORY:
        raise ProtocolError("Flight DoPut did not acknowledge memory durability")
    return {
        "put_id": str(uuid.UUID(bytes=put_id)),
        "batch_sequence": int(receipt.batch_sequence),
        "row_count": int(receipt.row_count),
        "durability": "memory",
    }


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


def _write_batch(writer: flight.FlightStreamWriter, batch: pa.RecordBatch) -> None:
    writer.write_batch(batch)
