"""Flight protocol, SQL compilation, and writer contract tests."""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from zippy_client import AmbiguousWriteError, ServerError, col, lit
from zippy_client._generated import protocol_pb2 as wire
from zippy_client.errors import (
    AuthenticationError,
    CapabilityError,
    ProtocolError,
    QueryError,
    TransportUnavailableError,
    ZippyConnectionError,
)
from zippy_client.models import Operation, Transport
from zippy_client.router import TransportRouter
from zippy_client.endpoints import ControlEndpoint
from zippy_client.sql import compile_flight_table_sql, expression_to_sql
from zippy_client.transports.flight import (
    CAPABILITIES_ACTION,
    FlightTransport,
    _decode_put_receipt,
    _put_batch_request_id,
    _translate_flight_error,
)
from zippy_client.writer import FlightWriter


class _Outgoing:
    def __init__(self) -> None:
        self.payload = b""

    def write(self, payload: bytes) -> None:
        self.payload = bytes(payload)


class _Incoming:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


class _FakeDoGetReader:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def read_all(self) -> pa.Table:
        return self.table


class _FakeMetadataReader:
    def __init__(self) -> None:
        self.pending: deque[pa.Buffer] = deque()
        self.done = False

    def read(self) -> pa.Buffer | None:
        if self.pending:
            return self.pending.popleft()
        if self.done:
            return None
        raise AssertionError("client tried to read a receipt before writing a batch")


class _FakePutWriter:
    def __init__(
        self,
        command: wire.PutCommand,
        metadata: _FakeMetadataReader,
        batches: list[pa.RecordBatch],
        state: _FakeFlightState,
        *,
        fail_before_first_batch: bool = False,
    ) -> None:
        self.command = command
        self.metadata = metadata
        self.batches = batches
        self.state = state
        self.fail_before_first_batch = fail_before_first_batch
        self.closed = False

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if self.fail_before_first_batch and not self.batches:
            raise flight.FlightUnavailableError("injected disconnect")
        index = len(self.batches)
        self.batches.append(batch)
        first_offset = self.state.next_log_offset
        last_offset = first_offset + batch.num_rows - 1
        previous = wire.OffsetWatermark(state=wire.OffsetWatermark.STATE_EMPTY)
        if first_offset > 0:
            previous = wire.OffsetWatermark(
                state=wire.OffsetWatermark.STATE_KNOWN,
                offset=first_offset - 1,
            )
        receipt = wire.PutBatchResult(
            protocol_version=1,
            put_id=self.command.put_id,
            batch_sequence=self.command.first_batch_sequence + index,
            row_count=batch.num_rows,
            durability_reached=self.command.durability,
            request_id=_put_batch_request_id(self.command.put_id, index),
            producer_epoch=len(self.state.put_batches),
            first_log_offset=first_offset,
            last_log_offset=last_offset,
            previous_log_offset=previous,
            committed_log_offset=wire.OffsetWatermark(
                state=wire.OffsetWatermark.STATE_KNOWN,
                offset=last_offset,
            ),
        )
        if self.command.durability == wire.PUT_DURABILITY_PERSISTED:
            receipt.persisted_snapshot_version = 2
        self.state.next_log_offset = last_offset + 1
        self.metadata.pending.append(pa.py_buffer(receipt.SerializeToString()))

    def done_writing(self) -> None:
        self.metadata.done = True

    def close(self) -> None:
        self.closed = True


class _FakeFlightState:
    def __init__(self, *, authenticated: bool) -> None:
        self.closed_sessions: list[_FakeFlightClient] = []
        capabilities = [
            "query.get_flight_info_do_get.v1",
            "query.single_use_ticket.v1",
            "error.details.v1",
            "session.server_authority.v1",
            "auth.session_roles.v1",
        ]
        if not authenticated:
            capabilities.append("auth.local_read_handshake.v1")
        if authenticated:
            capabilities.extend(
                [
                    "session.server_authority.v1",
                    "auth.bearer_handshake.v1",
                    "auth.session_roles.v1",
                    "ingest.append.memory.v1",
                    "ingest.append.wal.v1",
                    "ingest.append.persisted.v1",
                    "ingest.frame.v1",
                    "ingest.key_value.v1",
                    "catalog.create.v1",
                    "catalog.drop.v1",
                    "catalog.schema.v1",
                ]
            )
        self.capabilities = wire.CapabilitiesResponse(
            protocol_version=1,
            server_version="test-0.5.0",
            capabilities=capabilities,
            max_message_bytes=2 * 1024 * 1024,
            max_pending_tickets=8,
            authentication_required=authenticated,
            session_ttl_ms=60_000,
            writer_authentication_enabled=authenticated,
            ingest_enabled=authenticated,
            ingest_max_concurrent_puts=2 if authenticated else 0,
            ingest_max_batches_per_put=4 if authenticated else 0,
            ingest_max_rows_per_batch=2 if authenticated else 0,
            ingest_max_rows_per_put=3 if authenticated else 0,
            ingest_max_batch_bytes=1024 * 1024 if authenticated else 0,
            ingest_max_bytes_per_put=2 * 1024 * 1024 if authenticated else 0,
            ingest_max_rows_per_frame=2 if authenticated else 0,
        )
        self.query_commands: list[wire.QueryCommand] = []
        self.query_options: list[flight.FlightCallOptions] = []
        self.put_commands: list[wire.PutCommand] = []
        self.put_batches: list[list[pa.RecordBatch]] = []
        self.management_requests: list[object] = []
        self.management_response_error = False
        self.handshake_roles: list[int] = []
        self.clients: list[_FakeFlightClient] = []
        self.message_limits: list[int] = []
        self.fail_put_number: int | None = None
        self.next_log_offset = 0
        self.result = pa.table({"value": [10, 20]})

    def connect(self, location: str, **kwargs: object) -> _FakeFlightClient:
        assert location == "grpc://127.0.0.1:17667"
        message_limit = kwargs["write_size_limit_bytes"]
        assert isinstance(message_limit, int)
        self.message_limits.append(message_limit)
        client = _FakeFlightClient(self)
        self.clients.append(client)
        return client


class _FakeFlightClient:
    def __init__(self, state: _FakeFlightState) -> None:
        self.state = state
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def do_action(
        self,
        action: flight.Action,
        options: flight.FlightCallOptions,
    ) -> list[flight.Result]:
        if action.type == "zippy.close_session.v1":
            request = wire.SessionCloseRequest.FromString(bytes(action.body))
            assert request.protocol_version == 1
            assert dict(options.headers)[b"authorization"].startswith(b"Bearer zfs1_")
            self.state.closed_sessions.append(self)
            return [
                flight.Result(
                    wire.SessionCloseResponse(protocol_version=1, closed=True).SerializeToString()
                )
            ]
        if action.type in {
            "zippy.create_namespace.v1",
            "zippy.create_table.v1",
            "zippy.drop_table.v1",
            "zippy.alter_table_schema.v1",
        }:
            assert dict(options.headers)[b"authorization"].startswith(b"Bearer zfs1_")
            if action.type == "zippy.create_namespace.v1":
                namespace = wire.CreateNamespaceRequest.FromString(bytes(action.body))
                request_id, object_id, schema_version = (
                    namespace.request_id,
                    namespace.namespace_id,
                    0,
                )
                self.state.management_requests.append(namespace)
            elif action.type == "zippy.alter_table_schema.v1":
                alteration = wire.AlterTableSchemaRequest.FromString(bytes(action.body))
                request_id, object_id, schema_version = (
                    alteration.request_id,
                    alteration.table_id,
                    alteration.expected_schema_version + 1,
                )
                self.state.management_requests.append(alteration)
            elif action.type == "zippy.drop_table.v1":
                deletion = wire.DropTableRequest.FromString(bytes(action.body))
                request_id, object_id, schema_version = deletion.request_id, deletion.table_id, 0
                assert deletion.expected_snapshot_version > 0
                self.state.management_requests.append(deletion)
            else:
                table = wire.CreateTableRequest.FromString(bytes(action.body))
                assert len(pa.ipc.open_stream(table.schema_ipc).schema) > 0
                request_id, object_id, schema_version = table.request_id, table.table_id, 1
                self.state.management_requests.append(table)
            result = wire.CatalogMutationResult(
                protocol_version=1,
                request_id=request_id,
                object_id=object_id,
                catalog_revision=2,
                schema_version=schema_version,
            )
            if self.state.management_response_error:
                result.object_id = uuid.UUID(int=9999).bytes
            return [flight.Result(result.SerializeToString())]
        assert action.type == CAPABILITIES_ACTION
        assert options.timeout == pytest.approx(60.0)
        request = wire.CapabilitiesRequest.FromString(bytes(action.body))
        assert request.protocol_version == 1
        return [flight.Result(self.state.capabilities.SerializeToString())]

    def authenticate(self, handler: Any, options: flight.FlightCallOptions) -> None:
        header = dict(options.headers).get(b"authorization")
        if header is None and not self.state.capabilities.authentication_required:
            role = wire.SESSION_ROLE_READ
            session_id = uuid.UUID(int=404).bytes
        elif header == b"Bearer read-token":
            role = wire.SESSION_ROLE_READ
            session_id = uuid.UUID("00000000-0000-0000-0000-000000000101").bytes
        elif header == b"Bearer writer-token":
            role = wire.SESSION_ROLE_WRITER
            session_id = uuid.UUID("00000000-0000-0000-0000-000000000202").bytes
        elif header == b"Bearer admin-token":
            role = wire.SESSION_ROLE_ADMINISTRATOR
            session_id = uuid.UUID(int=303).bytes
        else:
            raise AssertionError(f"unexpected bootstrap header={header!r}")
        response = wire.SessionOpenResponse(
            protocol_version=1,
            session_id=session_id,
            bearer_token=f"zfs1_{'1' * 64}",
            expires_at_ns=2**63 - 1,
            role=role,
        )
        outgoing = _Outgoing()
        handler.authenticate(outgoing, _Incoming(response.SerializeToString()))
        request = wire.SessionOpenRequest.FromString(outgoing.payload)
        assert request.protocol_version == 1
        assert handler.get_token().startswith(b"zfs1_")
        self.state.handshake_roles.append(role)

    def get_flight_info(
        self,
        descriptor: flight.FlightDescriptor,
        options: flight.FlightCallOptions,
    ) -> object:
        command = wire.QueryCommand.FromString(descriptor.command)
        self.state.query_commands.append(command)
        self.state.query_options.append(options)
        if self.state.capabilities.authentication_required:
            assert dict(options.headers)[b"authorization"].startswith(b"Bearer zfs1_")
        info = wire.QueryInfo(
            protocol_version=1,
            query_id=command.query_id,
            generation=1,
            catalog_revision=7,
            planned_at_ns=10,
            expires_at_ns=2**63 - 1,
            snapshots=[
                wire.QueryTableVersion(
                    table_id=uuid.UUID("00000000-0000-0000-0000-000000000303").bytes,
                    snapshot_version=4,
                    schema_version=2,
                )
            ],
        )
        return SimpleNamespace(
            app_metadata=info.SerializeToString(),
            schema=self.state.result.schema,
            endpoints=[SimpleNamespace(ticket=flight.Ticket(b"query-ticket"))],
        )

    def do_get(
        self,
        ticket: flight.Ticket,
        options: flight.FlightCallOptions,
    ) -> _FakeDoGetReader:
        assert ticket.ticket == b"query-ticket"
        assert options.timeout > 0
        return _FakeDoGetReader(self.state.result)

    def do_put(
        self,
        descriptor: flight.FlightDescriptor,
        schema: pa.Schema,
        options: flight.FlightCallOptions,
    ) -> tuple[_FakePutWriter, _FakeMetadataReader]:
        del schema
        assert options.timeout > 0
        command = wire.PutCommand.FromString(descriptor.command)
        self.state.put_commands.append(command)
        batches: list[pa.RecordBatch] = []
        self.state.put_batches.append(batches)
        metadata = _FakeMetadataReader()
        return (
            _FakePutWriter(
                command,
                metadata,
                batches,
                self.state,
                fail_before_first_batch=(self.state.fail_put_number == len(self.state.put_batches)),
            ),
            metadata,
        )


class _LoopbackSessionAuth(flight.ServerAuthHandler):
    def authenticate(self, outgoing: Any, incoming: Any) -> None:
        request = wire.SessionOpenRequest.FromString(incoming.read())
        assert request.protocol_version == 1
        outgoing.write(
            wire.SessionOpenResponse(
                protocol_version=1,
                session_id=uuid.UUID(int=707).bytes,
                bearer_token=f"zfs1_{'7' * 64}",
                expires_at_ns=2**63 - 1,
                role=wire.SESSION_ROLE_READ,
            ).SerializeToString()
        )

    def is_valid(self, token: bytes) -> bytes:
        if token and token != f"zfs1_{'7' * 64}".encode():
            raise flight.FlightUnauthenticatedError("invalid test session")
        return token


class _LoopbackFlightServer(flight.FlightServerBase):
    def __init__(self) -> None:
        super().__init__(location=("127.0.0.1", 0), auth_handler=_LoopbackSessionAuth())
        self.table = pa.table({"value": [3, 5, 8]})
        self.commands: list[wire.QueryCommand] = []

    def do_action(self, context: object, action: flight.Action):
        del context
        if action.type == "zippy.close_session.v1":
            yield flight.Result(
                wire.SessionCloseResponse(protocol_version=1, closed=True).SerializeToString()
            )
            return
        assert action.type == CAPABILITIES_ACTION
        capabilities = wire.CapabilitiesResponse(
            protocol_version=1,
            server_version="loopback-test",
            capabilities=[
                "query.get_flight_info_do_get.v1",
                "query.single_use_ticket.v1",
                "error.details.v1",
                "session.server_authority.v1",
                "auth.local_read_handshake.v1",
                "auth.session_roles.v1",
            ],
            session_ttl_ms=60_000,
            max_message_bytes=2 * 1024 * 1024,
            max_pending_tickets=4,
        )
        yield flight.Result(capabilities.SerializeToString())

    def get_flight_info(
        self,
        context: object,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        del context
        command = wire.QueryCommand.FromString(descriptor.command)
        self.commands.append(command)
        query_info = wire.QueryInfo(
            protocol_version=1,
            query_id=command.query_id,
            generation=1,
            catalog_revision=9,
            planned_at_ns=10,
            expires_at_ns=2**63 - 1,
            snapshots=[
                wire.QueryTableVersion(
                    table_id=uuid.UUID("00000000-0000-0000-0000-000000000505").bytes,
                    snapshot_version=2,
                    schema_version=1,
                )
            ],
        )
        endpoint = flight.FlightEndpoint(flight.Ticket(b"loopback-ticket"), [])
        return flight.FlightInfo(
            self.table.schema,
            descriptor,
            [endpoint],
            self.table.num_rows,
            self.table.nbytes,
            ordered=True,
            app_metadata=query_info.SerializeToString(),
        )

    def do_get(self, context: object, ticket: flight.Ticket) -> flight.RecordBatchStream:
        del context
        assert ticket.ticket == b"loopback-ticket"
        return flight.RecordBatchStream(self.table)


def test_authenticated_flight_query_and_bounded_do_put(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )

    result, metrics = transport.read_sql(
        "SELECT value FROM ticks",
        {"ticks": {"namespace": "market", "table": "ticks"}},
        result_batch_rows=64,
    )
    receipts = transport.write_table(
        "ticks",
        pa.table({"value": [1, 2, 3, 4, 5]}),
        first_batch_sequence=7,
    )
    transport.close()

    assert result.equals(state.result)
    assert metrics["returned_rows"] == 2
    query_metrics = metrics["query"]
    assert isinstance(query_metrics, dict)
    assert query_metrics["catalog_revision"] == 7
    query = state.query_commands[0]
    assert query.session_id == uuid.UUID("00000000-0000-0000-0000-000000000101").bytes
    assert query.bindings[0].name.namespace == "market"
    assert query.bindings[0].name.table == "ticks"
    assert query.memory_limit.kind == wire.ResourceLimit.KIND_SERVER_DEFAULT
    assert query.spill_limit.kind == wire.ResourceLimit.KIND_DISABLED
    assert state.handshake_roles == [wire.SESSION_ROLE_READ, wire.SESSION_ROLE_WRITER]
    assert [command.first_batch_sequence for command in state.put_commands] == [7, 8]
    assert all(command.durability == wire.PUT_DURABILITY_WAL for command in state.put_commands)
    assert len({bytes(command.producer_id) for command in state.put_commands}) == 1
    assert len({bytes(command.connection_id) for command in state.put_commands}) == 1
    assert not state.put_commands[0].HasField("expected_writer_epoch")
    assert state.put_commands[1].expected_writer_epoch == 1
    assert [len(batches) for batches in state.put_batches] == [1, 2]
    assert [receipt["batch_sequence"] for receipt in receipts] == [7, 8, 9]
    assert [receipt["producer_epoch"] for receipt in receipts] == [1, 2, 2]
    assert [receipt["committed_log_offset"] for receipt in receipts] == [1, 3, 4]
    assert all(client.closed for client in state.clients)


def test_write_table_reuses_one_open_doput_across_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    producer_id = uuid.UUID("00000000-0000-0000-0000-000000000701")
    connection_id = uuid.UUID("00000000-0000-0000-0000-000000000702")

    first = transport.write_table(
        "ticks",
        pa.table({"value": [1]}),
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
    )
    second = transport.write_table(
        "ticks",
        pa.table({"value": [2]}),
        first_batch_sequence=1,
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
        expected_writer_epoch=1,
    )
    transport.close_write(connection_id)
    transport._read_connection = transport._writer_connection
    transport.close()

    assert len(state.put_commands) == 1
    assert [batch.num_rows for batch in state.put_batches[0]] == [1, 1]
    assert [first[0]["batch_sequence"], second[0]["batch_sequence"]] == [0, 1]
    assert [first[0]["producer_epoch"], second[0]["producer_epoch"]] == [1, 1]


def test_reusable_doput_rotates_when_durability_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    producer_id = uuid.UUID("00000000-0000-0000-0000-000000000711")
    connection_id = uuid.UUID("00000000-0000-0000-0000-000000000712")

    transport.write_table(
        "ticks",
        pa.table({"value": [1]}),
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
    )
    transport.write_table(
        "ticks",
        pa.table({"value": [2]}),
        first_batch_sequence=1,
        durability="wal",
        producer_id=producer_id,
        connection_id=connection_id,
        expected_writer_epoch=1,
    )
    transport.close_write(connection_id)
    transport.close()

    assert len(state.put_commands) == 2
    assert [command.durability for command in state.put_commands] == [
        wire.PUT_DURABILITY_MEMORY,
        wire.PUT_DURABILITY_WAL,
    ]


def test_expired_reusable_doput_is_aborted_before_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    producer_id = uuid.UUID("00000000-0000-0000-0000-000000000721")
    connection_id = uuid.UUID("00000000-0000-0000-0000-000000000722")
    transport.write_table(
        "ticks",
        pa.table({"value": [1]}),
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
    )
    expired = transport._active_puts[connection_id.bytes]
    expired.retire_at_ns = 0
    expired.deadline_at_ns = 0

    def fail_expired_close() -> None:
        raise RuntimeError("expired stream already closed by Flight")

    expired.writer.close = fail_expired_close

    transport.write_table(
        "ticks",
        pa.table({"value": [2]}),
        first_batch_sequence=1,
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
        expected_writer_epoch=1,
    )
    transport.close_write(connection_id)
    transport.close()

    assert not expired.metadata_reader.done
    assert len(state.put_commands) == 2


def test_transport_close_releases_clients_after_put_terminal_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    producer_id = uuid.UUID("00000000-0000-0000-0000-000000000731")
    connection_id = uuid.UUID("00000000-0000-0000-0000-000000000732")
    transport.write_table(
        "ticks",
        pa.table({"value": [1]}),
        durability="memory",
        producer_id=producer_id,
        connection_id=connection_id,
    )
    active = transport._active_puts[connection_id.bytes]
    active.metadata_reader.pending.append(pa.py_buffer(b"unexpected-terminal"))

    with pytest.raises(AmbiguousWriteError, match="clean terminal boundary"):
        transport.close()

    assert active.writer.closed
    assert all(client.closed for client in state.clients)


def test_write_table_rejects_invalid_and_oversized_per_put_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    original = transport.capabilities
    transport.capabilities = replace(original, ingest_max_batches_per_put=0)
    with pytest.raises(ProtocolError, match="invalid per-put limits"):
        transport.write_table("ticks", pa.table({"value": [1]}), durability="memory")

    transport.capabilities = replace(original, ingest_max_rows_per_put=1)
    with pytest.raises(CapabilityError, match="exceeds the server per-put limit"):
        transport.write_table("ticks", pa.table({"value": [1, 2]}), durability="memory")

    unknown_connection = uuid.UUID("00000000-0000-0000-0000-000000000799")
    transport.close_write(unknown_connection)
    with transport._put_lock:
        transport._close_write_locked(unknown_connection.bytes)
    transport.close()
    transport.close()


def test_pyarrow_loopback_executes_real_flight_rpc_stream() -> None:
    server = _LoopbackFlightServer()
    transport = FlightTransport(
        f"127.0.0.1:{server.port}",
        tls_enabled=False,
        read_token=None,
        writer_token=None,
    )
    try:
        result, metrics = transport.read_sql(
            "SELECT value FROM events",
            {"events": "events"},
        )
    finally:
        transport.close()
        server.shutdown()

    assert result.equals(server.table)
    assert metrics["returned_rows"] == 3
    assert server.commands[0].bindings[0].name.table == "events"


def test_local_read_flight_uses_server_issued_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=False)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token=None,
        writer_token=None,
    )

    result, _ = transport.read_sql("SELECT value FROM ticks", {"ticks": "ticks"})

    assert result.equals(state.result)
    command = state.query_commands[0]
    assert dict(state.query_options[0].headers)[b"authorization"].startswith(b"Bearer zfs1_")
    assert b"x-zippy-session-id" not in dict(state.query_options[0].headers)
    assert command.session_id == uuid.UUID(int=404).bytes
    assert state.handshake_roles == [wire.SESSION_ROLE_READ]
    with pytest.raises(CapabilityError, match="does not advertise append"):
        transport.write_table("ticks", pa.table({"value": [1]}))
    transport.close()


def test_flight_query_validates_resources_bindings_and_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token=None,
    )
    deadline_ns = 2**62

    transport.read_sql(
        "SELECT value FROM ticks",
        {"ticks": ("market", "ticks")},
        priority="normal",
        deadline_ns=deadline_ns,
        memory_limit_bytes=4096,
        spill_limit_bytes=2048,
    )

    command = state.query_commands[0]
    assert command.priority == wire.QUERY_PRIORITY_NORMAL
    assert command.deadline_ns == deadline_ns
    assert command.memory_limit.kind == wire.ResourceLimit.KIND_BYTES
    assert command.memory_limit.bytes == 4096
    assert command.spill_limit.bytes == 2048
    table_id = uuid.UUID("00000000-0000-0000-0000-000000000606")
    transport.read_sql(
        "SELECT * FROM namespaced JOIN by_id USING (value)",
        [
            {"sql_name": "namespaced", "namespace": "market", "table": "ticks"},
            {"sql_name": "by_id", "table_id": table_id},
        ],
    )
    iterable_command = state.query_commands[-1]
    assert iterable_command.bindings[0].name.namespace == "market"
    assert iterable_command.bindings[1].table_id == table_id.bytes
    with pytest.raises(ValueError, match="must not be empty"):
        transport.read_sql(" ", {"ticks": "ticks"})
    with pytest.raises(ValueError, match="at least one explicit"):
        transport.read_sql("SELECT 1", {})
    with pytest.raises(ValueError, match="result_batch_rows"):
        transport.read_sql("SELECT 1", {"ticks": "ticks"}, result_batch_rows=0)
    with pytest.raises(AuthenticationError, match="writer bootstrap token"):
        transport.write_table("ticks", pa.table({"value": [1]}))
    assert transport.write_table("ticks", pa.table({"value": pa.array([], pa.int64())})) == []
    transport.close()


def test_flight_channels_honor_the_stricter_discovered_message_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=False)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token=None,
        writer_token=None,
        configured_max_message_bytes=4 * 1024 * 1024,
    )

    transport.read_sql("SELECT value FROM ticks", {"ticks": "ticks"})
    transport.close()

    assert state.message_limits == [4 * 1024 * 1024, 2 * 1024 * 1024]


def test_flight_do_put_exposes_acknowledged_prefix_on_ambiguous_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    state.fail_put_number = 2
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )

    with pytest.raises(AmbiguousWriteError) as captured:
        transport.write_table("ticks", pa.table({"value": [1, 2, 3, 4]}))
    transport.close()

    assert [receipt["batch_sequence"] for receipt in captured.value.receipts] == [0]


@pytest.mark.parametrize("terminal_code", ["SCHEMA_MISMATCH", "CONFLICT", "INTERNAL", None])
def test_doput_eof_checks_terminal_status_and_preserves_acknowledged_prefix(
    monkeypatch: pytest.MonkeyPatch, terminal_code: str | None
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    original_write = _FakePutWriter.write_batch

    def write_batch(put: _FakePutWriter, batch: pa.RecordBatch) -> None:
        if put.command.first_batch_sequence == 0:
            original_write(put, batch)
        else:
            put.metadata.done = True

    def close(put: _FakePutWriter) -> None:
        put.closed = True
        if put.command.first_batch_sequence > 0 and terminal_code is not None:
            envelope = wire.ErrorEnvelope(
                protocol_version=1,
                code=terminal_code,
                message="opaque terminal failure",
                retryable=False,
            )
            raise flight.FlightServerError("opaque", extra_info=envelope.SerializeToString())

    monkeypatch.setattr(_FakePutWriter, "write_batch", write_batch)
    monkeypatch.setattr(_FakePutWriter, "close", close)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    writer = FlightWriter("ticks", transport)
    expected = ServerError if terminal_code == "SCHEMA_MISMATCH" else AmbiguousWriteError
    with pytest.raises(expected) as captured:
        writer.write_batch(pa.table({"value": [1, 2, 3, 4]}))
    assert isinstance(captured.value, (ServerError, AmbiguousWriteError))
    assert [receipt["batch_sequence"] for receipt in captured.value.receipts] == [0]
    assert writer.last_receipts == captured.value.receipts
    if isinstance(captured.value, ServerError):
        assert captured.value.code == "SCHEMA_MISMATCH"
    elif terminal_code is not None:
        cause: BaseException | None = captured.value
        while cause is not None and not isinstance(cause, ServerError):
            cause = cause.__cause__
        assert isinstance(cause, ServerError)
        assert cause.code == terminal_code
    assert transport._active_puts == {}
    writer.close()
    transport.close()


def test_flight_transport_maps_unstructured_transport_failures() -> None:
    assert isinstance(
        _translate_flight_error(flight.FlightUnavailableError("down"), "DoGet"),
        ZippyConnectionError,
    )
    assert isinstance(
        _translate_flight_error(flight.FlightTimedOutError("late"), "DoGet"),
        QueryError,
    )
    assert isinstance(
        _translate_flight_error(flight.FlightUnauthenticatedError("no"), "Handshake"),
        AuthenticationError,
    )
    assert isinstance(
        _translate_flight_error(flight.FlightServerError("opaque"), "DoGet"),
        ProtocolError,
    )


def test_structured_flight_error_is_not_parsed_from_message_text() -> None:
    request_id = uuid.UUID("00000000-0000-0000-0000-000000000404")
    envelope = wire.ErrorEnvelope(
        protocol_version=1,
        code="RESOURCE_EXHAUSTED",
        message="opaque diagnostic",
        retryable=True,
        request_id=request_id.bytes,
        resource_limit=wire.ResourceLimitExceeded(
            resource="query_memory",
            limit=wire.ResourceLimit(kind=wire.ResourceLimit.KIND_BYTES, bytes=1024),
            requested=2048,
        ),
    )
    error = RuntimeError("this text must not be parsed")
    error.extra_info = envelope.SerializeToString()  # type: ignore[attr-defined]

    translated = _translate_flight_error(error, "GetFlightInfo")

    assert isinstance(translated, ServerError)
    assert translated.code == "RESOURCE_EXHAUSTED"
    assert translated.retryable is True
    assert translated.request_id == str(request_id)
    assert translated.details["kind"] == "resource_limit"

    malformed = RuntimeError("bad")
    malformed.extra_info = b"not-protobuf"  # type: ignore[attr-defined]
    assert isinstance(_translate_flight_error(malformed, "DoGet"), ProtocolError)


class _CompilerTable:
    def __init__(
        self,
        source: str,
        schema: pa.Schema,
        plan: list[tuple[str, object]],
        *,
        namespace: str = "default",
    ) -> None:
        self.source = source
        self.namespace = namespace
        self._schema = schema
        self._plan = plan

    def schema(self) -> pa.Schema:
        return self._schema


def test_flight_sql_compiler_covers_exact_lazy_operations_and_join() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("ts", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("drop_me", pa.int64()),
        ]
    )
    left = _CompilerTable(
        "ticks",
        schema,
        [
            ("filter", (col("price") > 1.0) & col("instrument_id").is_not_null()),
            ("with_columns", [(col("price") * 2).alias("notional")]),
            ("drop", ["drop_me"]),
            ("rename", {"price": "last_price"}),
            ("sort", {"by": ["ts"], "descending": True}),
            ("tail", 10),
        ],
        namespace="market",
    )
    right = _CompilerTable(
        "instruments",
        pa.schema([("instrument_id", pa.string()), ("multiplier", pa.float64())]),
        [],
        namespace="reference",
    )
    joined = _CompilerTable(
        "ticks",
        schema,
        [
            ("join", {"other": right, "on": ["instrument_id"], "how": "left"}),
            ("select", ["instrument_id", "multiplier"]),
            ("head", 5),
        ],
        namespace="market",
    )

    left_plan = compile_flight_table_sql(left)
    join_plan = compile_flight_table_sql(joined)

    assert "WHERE" in left_plan.sql
    assert 'AS "notional"' in left_plan.sql
    assert "ORDER BY" in left_plan.sql
    assert "LIMIT 10" in left_plan.sql
    assert left_plan.bindings[0]["table"] == {"namespace": "market", "table": "ticks"}
    assert "LEFT JOIN" in join_plan.sql
    assert len(join_plan.bindings) == 2
    assert expression_to_sql(col("instrument_id").is_in(["A", "B"])) == (
        "(\"instrument_id\" IN ('A', 'B'))"
    )

    unsorted_tail = _CompilerTable("ticks", schema, [("tail", 1)])
    with pytest.raises(ValueError, match="requires an explicit preceding sort"):
        compile_flight_table_sql(unsorted_tail)


def test_flight_sql_compiler_renders_projection_slice_and_typed_literals() -> None:
    schema = pa.schema([("value", pa.int64()), ("name", pa.string())])
    table = _CompilerTable(
        "events",
        schema,
        [
            ("select", ["name", (col("value") + 1).alias("next_value")]),
            ("slice", {"offset": 2, "length": 3}),
        ],
    )
    open_ended = _CompilerTable(
        "events",
        schema,
        [("head", 8), ("slice", {"offset": 1, "length": None})],
    )

    plan = compile_flight_table_sql(table)
    open_plan = compile_flight_table_sql(open_ended)

    assert 'AS "next_value"' in plan.sql
    assert "LIMIT 3 OFFSET 2" in plan.sql
    assert "LIMIT 8" in open_plan.sql and "OFFSET 1" in open_plan.sql
    assert expression_to_sql(col("value") == None) == '("value" IS NULL)'  # noqa: E711
    assert expression_to_sql(~col("name").is_null()) == '(NOT ("name" IS NULL))'
    assert expression_to_sql(-col("value")) == '(-"value")'
    assert expression_to_sql(col("value").is_between(1, 3)) == (
        '(("value" >= 1) AND ("value" <= 3))'
    )
    assert expression_to_sql(date(2026, 7, 18)) == "DATE '2026-07-18'"
    assert expression_to_sql(Decimal("1.25")) == "1.25"
    assert expression_to_sql(lit("O'Reilly")) == "'O''Reilly'"
    timestamp = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    assert expression_to_sql(timestamp) == "to_timestamp_nanos(1000000000)"
    with pytest.raises(ValueError, match="must be finite"):
        expression_to_sql(float("inf"))


def test_flight_sql_compiler_preserves_replaced_columns_and_outer_join_keys() -> None:
    schema = pa.schema([("first", pa.int64()), ("value", pa.int64()), ("last", pa.int64())])
    replacement = _CompilerTable(
        "events",
        schema,
        [("with_columns", [(col("value") + 1).alias("value")])],
    )
    right = _CompilerTable(
        "reference",
        pa.schema([("value", pa.int64()), ("label", pa.string())]),
        [],
    )
    outer = _CompilerTable(
        "events",
        schema,
        [("join", {"other": right, "on": ["value"], "how": "full"})],
    )

    replacement_plan = compile_flight_table_sql(replacement)
    outer_plan = compile_flight_table_sql(outer)

    assert 'SELECT "first", ("value" + 1) AS "value", "last"' in replacement_plan.sql
    assert 'COALESCE(_zippy_left."value", _zippy_right."value") AS "value"' in outer_plan.sql

    duplicate_projection = _CompilerTable("events", schema, [("select", ["value", "value"])])
    with pytest.raises(ValueError, match="output names must be unique"):
        compile_flight_table_sql(duplicate_projection)


class _WriterTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, pa.Table, int]] = []
        self.connection_ids: list[bytes] = []
        self.closed_connections: list[bytes] = []

    def write_table(
        self,
        stream_name: str,
        table: pa.Table,
        *,
        namespace: str = "default",
        first_batch_sequence: int = 0,
        durability: str = "wal",
        producer_id: bytes | str | uuid.UUID | None = None,
        connection_id: bytes | str | uuid.UUID | None = None,
        expected_writer_epoch: int | None = None,
    ) -> list[dict[str, object]]:
        assert namespace == "default"
        del producer_id, expected_writer_epoch
        assert isinstance(connection_id, bytes)
        self.connection_ids.append(connection_id)
        self.calls.append((stream_name, table, first_batch_sequence))
        return [
            {
                "put_id": str(uuid.uuid4()),
                "batch_sequence": first_batch_sequence,
                "row_count": table.num_rows,
                "durability": durability,
                "producer_epoch": first_batch_sequence + 1,
            }
        ]

    def close_write(self, connection_id: bytes | str | uuid.UUID) -> None:
        assert isinstance(connection_id, bytes)
        self.closed_connections.append(connection_id)


def test_flight_writer_buffers_rows_and_router_prefers_flight() -> None:
    transport = _WriterTransport()
    writer = FlightWriter("ticks", transport, batch_size=2, flush_interval_ms=None)
    writer.write({"value": 1})
    writer.write({"value": 2})
    writer.write_batch(pa.table({"value": [3, 4]}))
    writer.close()

    assert [call[1].num_rows for call in transport.calls] == [2, 2]
    assert [call[2] for call in transport.calls] == [0, 1]
    assert len(set(transport.connection_ids)) == 1
    assert transport.closed_connections == [transport.connection_ids[0]]
    assert writer.last_receipts[0]["durability"] == "wal"

    router = TransportRouter(
        ControlEndpoint("tcp", "192.0.2.1:17690", "zippy://192.0.2.1:17690/default"),
        "192.0.2.1:17667",
        True,
    )
    assert router.select(Operation.QUERY, Transport.AUTO).selected == Transport.FLIGHT_REMOTE
    assert router.select(Operation.WRITE, Transport.AUTO).selected == Transport.FLIGHT_REMOTE
    assert router.select(Operation.SUBSCRIBE, Transport.AUTO).selected == Transport.FLIGHT_REMOTE

    query_only = TransportRouter(
        ControlEndpoint("tcp", "192.0.2.1:17690", "zippy://192.0.2.1:17690/default"),
        "192.0.2.1:17667",
        False,
    )
    with pytest.raises(TransportUnavailableError, match="authenticated ingest"):
        query_only.select(Operation.WRITE, Transport.AUTO)
    with pytest.raises(TransportUnavailableError, match="does not advertise"):
        query_only.select(Operation.WRITE, Transport.FLIGHT_REMOTE)


def test_flight_writer_fails_closed_after_ambiguous_flush() -> None:
    class FailingTransport:
        def __init__(self) -> None:
            self.calls = 0

        def write_table(
            self,
            stream_name: str,
            table: pa.Table,
            *,
            namespace: str = "default",
            first_batch_sequence: int = 0,
            durability: str = "wal",
            producer_id: bytes | str | uuid.UUID | None = None,
            connection_id: bytes | str | uuid.UUID | None = None,
            expected_writer_epoch: int | None = None,
        ) -> list[dict[str, object]]:
            del (
                stream_name,
                table,
                first_batch_sequence,
                durability,
                producer_id,
                connection_id,
                expected_writer_epoch,
            )
            self.calls += 1
            raise AmbiguousWriteError(
                "connection lost after an unknown number of receipts",
                receipts=[
                    {
                        "put_id": str(uuid.uuid4()),
                        "batch_sequence": 0,
                        "row_count": 1,
                        "durability": "memory",
                    }
                ],
            )

        def close_write(self, connection_id: bytes | str | uuid.UUID) -> None:
            del connection_id

    transport = FailingTransport()
    writer = FlightWriter("ticks", transport, batch_size=2, flush_interval_ms=None)
    writer.write({"value": 1})
    with pytest.raises(AmbiguousWriteError, match="unknown number"):
        writer.write({"value": 2})
    with pytest.raises(RuntimeError, match="ambiguous partial commit"):
        writer.write({"value": 3})
    writer.close()

    assert transport.calls == 1
    assert writer.last_receipts[0]["batch_sequence"] == 0


class _MetadataFlightServer(_LoopbackFlightServer):
    def get_flight_info(self, context, descriptor):
        from google.protobuf.any_pb2 import Any as ProtobufAny

        command = ProtobufAny.FromString(descriptor.command)
        assert command.type_url == "type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables"
        assert command.value == b""
        endpoint = flight.FlightEndpoint(flight.Ticket(b"loopback-ticket"), [])
        return flight.FlightInfo(self.table.schema, descriptor, [endpoint], self.table.num_rows, -1)


def test_flight_sql_discovery_uses_real_rpc_and_exact_namespace_filter() -> None:
    server = _MetadataFlightServer()
    server.table = pa.table(
        {
            "catalog_name": ["zippy", "zippy"],
            "db_schema_name": ["market", "market_other"],
            "table_name": ["ticks", "ticks"],
            "table_type": ["TABLE", "TABLE"],
        }
    )
    transport = FlightTransport(
        f"127.0.0.1:{server.port}", tls_enabled=False, read_token=None, writer_token=None
    )
    try:
        assert len(transport.list_tables()) == 2
        assert transport.list_tables(namespace="market") == [
            {
                "catalog": "zippy",
                "namespace": "market",
                "name": "ticks",
                "table_type": "TABLE",
            }
        ]
        server.table = pa.table({"table_name": ["ticks"]})
        with pytest.raises(ProtocolError, match="missing columns"):
            transport.list_tables()
        server.table = pa.table(
            {
                "catalog_name": ["zippy"],
                "db_schema_name": [None],
                "table_name": ["ticks"],
                "table_type": ["TABLE"],
            }
        )
        with pytest.raises(ProtocolError, match="invalid field types"):
            transport.list_tables()
    finally:
        transport.close()
        server.shutdown()


def test_native_management_uses_admin_session_and_validates_exact_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
        admin_token="admin-token",
    )
    namespace_id, table_id = uuid.UUID(int=11), uuid.UUID(int=12)
    request_id = uuid.UUID(int=13)
    result = transport.create_namespace("default", namespace_id=namespace_id, request_id=request_id)
    assert result["object_id"] == str(namespace_id)
    assert result["schema_version"] == 0
    repeated = transport.create_namespace(
        "default", namespace_id=namespace_id, request_id=request_id
    )
    assert repeated == result
    assert state.management_requests[0] == state.management_requests[1]
    result = transport.create_append_table(
        "ticks",
        pa.schema([("price", pa.int64())]),
        namespace_id=namespace_id,
        table_id=table_id,
        request_id=uuid.UUID(int=14),
    )
    assert result["object_id"] == str(table_id)
    assert result["schema_version"] == 1
    assert state.handshake_roles == [wire.SESSION_ROLE_ADMINISTRATOR]
    state.management_response_error = True
    with pytest.raises(ProtocolError, match="does not match"):
        transport.create_namespace("default", namespace_id=namespace_id, request_id=request_id)
    transport.close()
    assert all(client.closed for client in state.clients)


def test_native_management_requires_capability_and_explicit_admin_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    with pytest.raises(AuthenticationError, match="ADMIN_TOKEN"):
        transport.create_namespace(
            "default", namespace_id=uuid.UUID(int=1), request_id=uuid.UUID(int=2)
        )
    assert not state.management_requests
    transport.capabilities = replace(transport.capabilities, names=frozenset())
    with pytest.raises(CapabilityError, match="management"):
        transport.create_namespace(
            "default", namespace_id=uuid.UUID(int=1), request_id=uuid.UUID(int=2)
        )
    transport.close()


def test_put_namespace_is_explicit_and_never_reuses_another_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    connection_id = uuid.UUID(int=501)
    for namespace in ("first", "second"):
        transport.write_table(
            "ticks",
            pa.table({"value": [1]}),
            namespace=namespace,
            producer_id=uuid.UUID(int=502),
            connection_id=connection_id,
        )
    assert [command.namespace for command in state.put_commands] == ["first", "second"]
    transport.close()


def test_persisted_write_requires_capability_and_returns_manifest_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
    )
    receipts = transport.write_table("ticks", pa.table({"value": [1]}), durability="persisted")
    assert receipts[0]["durability"] == "persisted"
    assert receipts[0]["persisted_snapshot_version"] == 2
    transport.capabilities = replace(
        transport.capabilities,
        names=transport.capabilities.names - {"ingest.append.persisted.v1"},
    )
    with pytest.raises(CapabilityError):
        transport.write_table("ticks", pa.table({"value": [2]}), durability="persisted")
    assert len(state.put_commands) == 1
    assert len(state.put_batches[0]) == 1
    transport.close()


@pytest.mark.parametrize("invalid_proof", ["wal", "missing_snapshot", "zero_snapshot"])
def test_persisted_receipt_rejects_insufficient_durability_proof(invalid_proof: str) -> None:
    put_id = uuid.UUID(int=1).bytes
    receipt = wire.PutBatchResult(
        protocol_version=1,
        put_id=put_id,
        batch_sequence=0,
        row_count=1,
        durability_reached=wire.PUT_DURABILITY_PERSISTED,
        request_id=_put_batch_request_id(put_id, 0),
        producer_epoch=1,
        first_log_offset=0,
        last_log_offset=0,
        previous_log_offset=wire.OffsetWatermark(state=wire.OffsetWatermark.STATE_EMPTY),
        committed_log_offset=wire.OffsetWatermark(state=wire.OffsetWatermark.STATE_KNOWN, offset=0),
    )
    if invalid_proof == "wal":
        receipt.durability_reached = wire.PUT_DURABILITY_WAL
    elif invalid_proof == "zero_snapshot":
        receipt.persisted_snapshot_version = 0
    with pytest.raises(ProtocolError):
        _decode_put_receipt(
            receipt.SerializeToString(),
            put_id=put_id,
            expected_sequence=0,
            expected_rows=1,
            expected_batch_index=0,
            requested_durability="persisted",
        )


def test_native_frame_creation_and_chunked_input_preserve_one_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
        admin_token="admin-token",
    )
    transport.create_frame_table(
        "frames",
        pa.schema([("value", pa.int64())]),
        by="value",
        namespace_id=uuid.UUID(int=1),
        table_id=uuid.UUID(int=2),
        request_id=uuid.UUID(int=3),
    )
    request = state.management_requests[0]
    assert isinstance(request, wire.CreateTableRequest)
    assert request.WhichOneof("definition") == "frame"
    assert request.frame.by == "value"
    frame = pa.table({"value": pa.chunked_array([[1], [2]])})
    receipts = transport.write_table("frames", frame, mutation="frame", durability="persisted")
    assert len(receipts) == 1
    assert receipts[0]["row_count"] == 2
    assert state.put_commands[0].mutation == wire.PUT_MUTATION_FRAME
    assert len(state.put_batches[0]) == 1
    assert state.put_batches[0][0].num_rows == 2
    with pytest.raises(CapabilityError, match="complete frame"):
        transport.write_table("frames", pa.table({"value": [3, 4, 5]}), mutation="frame")
    assert len(state.put_commands) == 1
    transport.capabilities = replace(
        transport.capabilities, names=transport.capabilities.names - {"ingest.frame.v1"}
    )
    with pytest.raises(CapabilityError, match="native frame"):
        transport.write_table("frames", frame, mutation="frame")
    assert len(state.put_commands) == 1
    transport.close()


def test_native_key_value_creation_atomic_mutations_and_capability_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
        admin_token="admin-token",
    )
    schema = pa.schema([("symbol", pa.string()), ("price", pa.int64())])
    for keys in ([], ["symbol", "symbol"], [""]):
        with pytest.raises(ValueError, match="keys"):
            transport.create_key_value_table(
                "quotes",
                schema,
                keys=keys,
                namespace_id=uuid.UUID(int=1),
                table_id=uuid.UUID(int=2),
                request_id=uuid.UUID(int=3),
            )
    transport.create_key_value_table(
        "quotes",
        schema,
        keys=["symbol"],
        namespace_id=uuid.UUID(int=1),
        table_id=uuid.UUID(int=2),
        request_id=uuid.UUID(int=3),
    )
    request = state.management_requests[0]
    assert isinstance(request, wire.CreateTableRequest)
    assert request.WhichOneof("definition") == "key_value"
    assert list(request.key_value.keys) == ["symbol"]
    rows = pa.table({"symbol": pa.chunked_array([["a"], ["b"]]), "price": [1, 2]})
    receipts = transport.write_table("quotes", rows, mutation="upsert", durability="persisted")
    assert len(receipts) == 1
    assert state.put_commands[-1].mutation == wire.PUT_MUTATION_KEY_VALUE_UPSERT
    assert len(state.put_batches[-1]) == 1
    transport.write_table("quotes", pa.table({"symbol": ["a"]}), mutation="delete")
    assert state.put_commands[-1].mutation == wire.PUT_MUTATION_KEY_VALUE_DELETE
    with pytest.raises(CapabilityError, match="complete key-value mutation"):
        transport.write_table("quotes", pa.table({"symbol": ["a", "b", "c"]}), mutation="delete")
    assert len(state.put_commands) == 2
    transport.capabilities = replace(
        transport.capabilities, names=transport.capabilities.names - {"ingest.key_value.v1"}
    )
    with pytest.raises(CapabilityError, match="native key-value"):
        transport.write_table("quotes", rows, mutation="upsert")
    with pytest.raises(CapabilityError, match="native key-value"):
        transport.create_key_value_table(
            "quotes",
            schema,
            keys=["symbol"],
            namespace_id=uuid.UUID(int=1),
            table_id=uuid.UUID(int=2),
            request_id=uuid.UUID(int=3),
        )
    transport.close()


def test_atomic_key_value_snapshot_capability_cursor_and_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.append("state.key_value_snapshot.v2")
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token=None
    )
    metadata = {
        b"zippy_kv_protocol": b"2",
        b"zippy_kv_seq": b"3",
        b"zippy_kv_version": b"2",
        b"zippy_kv_stream_id": uuid.UUID("00000000-0000-0000-0000-000000000303").hex.encode(),
        b"zippy_kv_keys": b'["key"]',
        b"source": b"fixture",
    }
    state.result = pa.table({"key": ["a"], "price": [2]}).replace_schema_metadata(metadata)
    assert transport.snapshot_state("quotes", namespace="market").schema.metadata == metadata
    assert state.query_commands[-1].key_value_snapshot
    assert state.query_commands[-1].bindings[0].name.namespace == "market"
    snapshot_id = uuid.UUID(hex=metadata[b"zippy_kv_stream_id"].decode())
    assert transport.snapshot_state(table_id=snapshot_id).schema.metadata == metadata
    assert state.query_commands[-1].bindings[0].table_id == snapshot_id.bytes
    with pytest.raises(ValueError, match="exactly one"):
        transport.snapshot_state("quotes", table_id=snapshot_id)
    with pytest.raises(ValueError, match="exactly one"):
        transport.snapshot_state()
    old_session = transport._read_connection
    transport.refresh_read_session(timeout_sec=0.5)
    assert transport._read_connection is not old_session
    # Empty state still carries a nonempty cursor after all keys have been deleted.
    state.result = state.result.slice(0, 0)
    assert transport.snapshot_state("quotes").num_rows == 0
    for key, invalid in [
        (b"zippy_kv_seq", b"0"),
        (b"zippy_kv_version", b"4"),
        (b"zippy_kv_protocol", b"1"),
        (b"zippy_kv_stream_id", uuid.UUID(int=1).hex.encode()),
        (b"zippy_kv_keys", b'["missing"]'),
    ]:
        state.result = state.result.replace_schema_metadata({**metadata, key: invalid})
        with pytest.raises(ProtocolError, match="snapshot metadata"):
            transport.snapshot_state("quotes")
    transport.capabilities = replace(transport.capabilities, names=frozenset())
    with pytest.raises(CapabilityError, match="state snapshots"):
        transport.snapshot_state("quotes")


def test_old_session_capabilities_are_rejected_without_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=False)
    state.capabilities.capabilities.remove("session.server_authority.v1")
    monkeypatch.setattr(flight, "connect", state.connect)
    with pytest.raises(ProtocolError, match="server-authoritative"):
        FlightTransport("127.0.0.1:17667", tls_enabled=False, read_token=None, writer_token=None)
    assert not state.handshake_roles
    assert all(client.closed for client in state.clients)


def test_local_mode_cannot_silently_discard_configured_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=False)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token=None
    )
    try:
        with pytest.raises(AuthenticationError, match="require authenticated"):
            transport.read_sql("SELECT value FROM ticks", {"ticks": "ticks"})
    finally:
        transport.close()
    assert not state.handshake_roles
    assert all(client.closed for client in state.clients)


def test_native_drop_validates_version_capability_authority_and_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token=None,
        admin_token="admin-token",
    )
    table_id, request_id = uuid.UUID(int=901), uuid.UUID(int=902)
    for version in (0, -1, 2**64, True):
        with pytest.raises(ValueError, match="positive uint64"):
            transport.drop_table(table_id, expected_snapshot_version=version, request_id=request_id)
    assert not state.management_requests
    result = transport.drop_table(table_id, expected_snapshot_version=7, request_id=request_id)
    assert result["object_id"] == str(table_id)
    assert result["schema_version"] == 0
    assert (
        transport.drop_table(table_id, expected_snapshot_version=7, request_id=request_id) == result
    )
    assert state.management_requests[0] == state.management_requests[1]
    assert state.handshake_roles == [wire.SESSION_ROLE_ADMINISTRATOR]
    state.management_response_error = True
    with pytest.raises(ProtocolError, match="does not match"):
        transport.drop_table(table_id, expected_snapshot_version=7, request_id=request_id)
    transport.capabilities = replace(
        transport.capabilities, names=transport.capabilities.names - {"catalog.drop.v1"}
    )
    with pytest.raises(CapabilityError, match="management"):
        transport.drop_table(table_id, expected_snapshot_version=7, request_id=request_id)
    transport.close()
    unauthenticated = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token=None
    )
    with pytest.raises(AuthenticationError, match="ADMIN_TOKEN"):
        unauthenticated.drop_table(table_id, expected_snapshot_version=7, request_id=request_id)
    unauthenticated.close()


def test_native_schema_change_uses_logical_schema_keys_and_strict_result_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token=None,
        admin_token="admin-token",
    )
    table_id, request_id = uuid.UUID(int=910), uuid.UUID(int=911)
    schema = pa.schema([("key", pa.string()), ("value", pa.int64())])
    for version in (0, -1, 2**64, True):
        with pytest.raises(ValueError, match="positive uint64"):
            transport.alter_table_schema(
                table_id, schema, expected_schema_version=version, request_id=request_id
            )
    for keys in ([], [""], ["key", "key"]):
        with pytest.raises(ValueError, match="key-value keys"):
            transport.alter_table_schema(
                table_id, schema, expected_schema_version=2, request_id=request_id, keys=keys
            )
    assert not state.management_requests
    result = transport.alter_table_schema(
        table_id, schema, expected_schema_version=2, request_id=request_id, keys=["key"]
    )
    assert result["schema_version"] == 3
    assert result["object_id"] == str(table_id)
    assert (
        transport.alter_table_schema(
            table_id, schema, expected_schema_version=2, request_id=request_id, keys=["key"]
        )
        == result
    )
    assert state.management_requests[0] == state.management_requests[1]
    request = state.management_requests[0]
    assert isinstance(request, wire.AlterTableSchemaRequest)
    assert list(request.key_value.keys) == ["key"]
    assert pa.ipc.open_stream(request.schema_ipc).schema == schema
    state.management_response_error = True
    with pytest.raises(ProtocolError, match="does not match"):
        transport.alter_table_schema(
            table_id, schema, expected_schema_version=2, request_id=request_id
        )
    transport.capabilities = replace(
        transport.capabilities, names=transport.capabilities.names - {"catalog.schema.v1"}
    )
    with pytest.raises(CapabilityError):
        transport.alter_table_schema(
            table_id, schema, expected_schema_version=2, request_id=request_id
        )
    transport.close()


def test_output_table_identity_is_validated_and_preserved_across_put_rotation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.append("ingest.table_identity.v1")
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    identity = uuid.UUID(int=4401)
    connection, producer = uuid.UUID(int=4402), uuid.UUID(int=4403)
    receipts = transport.write_table(
        "ticks",
        pa.table({"value": [1, 2, 3, 4, 5]}),
        expected_table_id=identity,
        producer_id=producer,
        connection_id=connection,
    )
    assert len(receipts) == 3
    assert len(state.put_commands) == 2
    assert all(bytes(command.expected_table_id) == identity.bytes for command in state.put_commands)
    replacement = uuid.UUID(int=4404)
    transport.write_table(
        "ticks",
        pa.table({"value": [6]}),
        expected_table_id=replacement,
        producer_id=producer,
        connection_id=connection,
        first_batch_sequence=3,
        expected_writer_epoch=2,
    )
    assert len(state.put_commands) == 3
    assert bytes(state.put_commands[-1].expected_table_id) == replacement.bytes
    transport.close()


def test_output_identity_guard_cannot_silently_degrade_or_use_nil_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    with pytest.raises(CapabilityError, match="identity"):
        transport.write_table("ticks", pa.table({"value": [1]}), expected_table_id=uuid.UUID(int=1))
    assert state.put_commands == []
    transport.close()
    state.capabilities.capabilities.append("ingest.table_identity.v1")
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    invalid_identities: list[bytes | str] = [b"", bytes(16), bytes(15), "invalid-uuid"]
    for identity in invalid_identities:
        with pytest.raises(ValueError):
            transport.write_table("ticks", pa.table({"value": [1]}), expected_table_id=identity)
    assert state.put_commands == []
    transport.close()


class _ClientQueryFlightServer(_LoopbackFlightServer):
    """Protocol peer returning explicit fixtures, without interpreting client SQL."""

    def __init__(self) -> None:
        super().__init__()
        self.catalog = pa.table(
            {
                "catalog_name": ["zippy", "zippy"],
                "db_schema_name": ["market", "reference"],
                "table_name": ["ticks", "ticks"],
                "table_type": ["TABLE", "TABLE"],
            }
        )

    def get_flight_info(self, context, descriptor):
        from google.protobuf.any_pb2 import Any as ProtobufAny

        metadata = ProtobufAny.FromString(descriptor.command)
        if metadata.type_url:
            assert metadata.type_url.endswith("CommandGetTables")
            return flight.FlightInfo(
                self.catalog.schema,
                descriptor,
                [flight.FlightEndpoint(flight.Ticket(b"catalog"), [])],
                2,
                -1,
            )
        return super().get_flight_info(context, descriptor)

    def do_get(self, context, ticket):
        if ticket.ticket == b"catalog":
            return flight.RecordBatchStream(self.catalog)
        return super().do_get(context, ticket)


@pytest.fixture
def standalone_native_client():
    """Discover a real Flight peer using a bounded TCP control response, with no zippy import."""
    import json
    import socketserver
    import threading

    from zippy_client import Client

    peer = _ClientQueryFlightServer()
    requests: list[dict[str, object]] = []

    class Discovery(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            request = json.loads(self.rfile.readline())
            requests.append(request)
            if request != {"GetConfig": {}}:
                return
            response = {
                "ConfigFetched": {
                    "config": {
                        "flight": {"enabled": True, "endpoint": f"127.0.0.1:{peer.port}"},
                    }
                }
            }
            self.wfile.write(json.dumps(response).encode() + b"\n")

    control = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Discovery)
    worker = threading.Thread(target=control.serve_forever, daemon=True)
    worker.start()
    client = None
    try:
        client = Client(f"tcp://127.0.0.1:{control.server_address[1]}")
        yield client, peer, requests
    finally:
        if client is not None:
            client.close()
        control.shutdown()
        control.server_close()
        worker.join(3)
        peer.shutdown()


def test_installed_client_discovers_metadata_and_executes_flight_without_server_package(
    standalone_native_client,
) -> None:
    client, peer, requests = standalone_native_client
    client.control.ping()
    assert requests == [{"GetConfig": {}}, {"GetConfig": {}}]
    assert client.transport_for("query").selected == Transport.FLIGHT_LOCAL
    assert len(client.list_tables()) == 2
    with client.read_table("ticks", namespace="market") as table:
        assert table.schema().equals(peer.table.schema)
        assert table.info()["namespace"] == "market"
        assert table.filter() is table
        assert table.drop() is table
        assert table.rename() is table
        assert table.collect().equals(peer.table)
        plan = (
            table.filter(col("value") > 1, value=3)
            .with_columns(
                doubled=col("value") * 2,
            )
            .drop(["value"])
            .rename({"doubled": "price"})
            .sort("price")
            .tail(2)
            .select("price")
        )
        explanation = plan.explain()
        assert explanation["snapshot"] == "per_query"
        assert 'AS "doubled"' in explanation["sql"]
        assert 'AS "price"' in explanation["sql"]
        assert "LIMIT 2" in explanation["sql"]
        assert explanation["bindings"] == [
            {"sql_name": "zippy_source_0", "table": {"namespace": "market", "table": "ticks"}}
        ]
        # The peer supplies a fixture; DataFusion execution is covered by the real server matrix.
        profile = plan.profile()
        assert profile["result"].equals(peer.table)
        assert profile["metrics"]["transport"] == "flight_local"
        assert profile["metrics"]["returned_rows"] == 3
        assert peer.commands[-1].sql == explanation["sql"]
        assert peer.commands[-1].bindings[0].name.namespace == "market"
        assert "LIMIT 1 OFFSET 1" in table.head(3).slice(1, 1).explain()["sql"]
        assert "OFFSET 1" in table.slice(1).explain()["sql"]
        assert "WHERE" in table.between("value", 1, 5).explain()["sql"]
        other = client.read_table("ticks", namespace="reference")
        with other:
            joined = table.join(other, on="value", how="left").explain()
            assert "LEFT JOIN" in joined["sql"]
            assert {binding["table"]["namespace"] for binding in joined["bindings"]} == {
                "market",
                "reference",
            }
        # Clones must not mutate their source query.
        assert "WHERE" not in table.explain()["sql"]
    with pytest.raises(RuntimeError, match="closed"):
        table.collect()
    assert client.read_sql("SELECT value FROM ticks", bindings={"ticks": "ticks"}).equals(
        peer.table
    )
    unsupported_call = getattr(client, "read_sql")
    with pytest.raises(TypeError, match="parameters"):
        unsupported_call(
            "SELECT ? FROM ticks", parameters={"value": 3}, bindings={"ticks": "ticks"}
        )
    with pytest.raises(TypeError, match="stream"):
        unsupported_call("SELECT * FROM ticks", stream=True, bindings={"ticks": "ticks"})
    with pytest.raises(CapabilityError, match="explicit bindings"):
        client.read_sql("SELECT value FROM ticks")


def test_installed_native_query_rejects_unavailable_or_invalid_semantics(
    standalone_native_client,
) -> None:
    client, peer, _ = standalone_native_client
    with pytest.raises(CapabilityError, match="fixed cross-query"):
        client.read_table("ticks", snapshot=True)
    with client.read_table("ticks", namespace="market") as table:
        with pytest.raises(CapabilityError, match="fixed cross-query"):
            table.snapshot()
        for operation in (
            lambda: table.head(-1),
            lambda: table.tail(-1),
            lambda: table.slice(-1),
            lambda: table.slice(0, -1),
            lambda: table.sort([]),
        ):
            with pytest.raises(ValueError):
                operation()
        with pytest.raises(TypeError, match="between column"):
            table.between(3, 1, 2)
        with pytest.raises(TypeError, match="join other"):
            table.join("ticks", on="value")
        with pytest.raises(ValueError, match="explicit preceding sort"):
            table.tail(1).collect()
        # Successful metadata is never allowed to invent a missing Catalog table.
        peer.catalog = peer.catalog.slice(0, 0)
        with pytest.raises(QueryError, match="missing or ambiguous"):
            table.info()
    client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.read_table("ticks")


def test_shared_sql_resolver_preserves_shard_order_and_detects_cycles() -> None:
    from zippy_client.sql import FlightSqlCompiler, SqlSource

    table = _CompilerTable("logical", pa.schema([("id", pa.int64())]), [])
    operations: list[tuple[str, object]] = []

    def resolve(value: object) -> SqlSource:
        assert value is table
        return SqlSource([("market", "part_0"), ("market", "part_1")], ["id"], operations)

    plan = FlightSqlCompiler(resolve).compile(table)
    assert plan.sql == 'SELECT * FROM "zippy_source_0" UNION ALL SELECT * FROM "zippy_source_1"'
    assert [binding["table"] for binding in plan.bindings] == [
        {"namespace": "market", "table": "part_0"},
        {"namespace": "market", "table": "part_1"},
    ]
    operations.append(("join", {"other": table, "on": ["id"]}))
    with pytest.raises(ValueError, match="cyclic table join"):
        FlightSqlCompiler(resolve).compile(table)
    with pytest.raises(ValueError, match="requires columns and table bindings"):
        FlightSqlCompiler(lambda _: SqlSource([], ["id"], [])).compile(table)


@pytest.fixture
def native_snapshot_client(monkeypatch: pytest.MonkeyPatch):
    """Exercise real SDK lease ownership against a deterministic protocol peer."""
    from zippy_client import Client
    from zippy_client.control import ControlClient

    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.append("query.fixed_snapshot.v1")
    created: list[wire.CreateFixedSnapshotRequest] = []
    released: list[bytes] = []
    overrides: list[list[bytes]] = []
    original = _FakeFlightClient.do_action

    def action(self, request, options):
        if request.type == "zippy.create_fixed_snapshot.v1":
            command = wire.CreateFixedSnapshotRequest.FromString(bytes(request.body))
            created.append(command)
            result = wire.FixedSnapshotInfo(
                protocol_version=1,
                snapshot_id=uuid.UUID(int=1000 + len(created)).bytes,
                expires_at_ns=2**63 - 1,
                catalog_revision=7,
                tables=[
                    wire.QueryTableVersion(
                        table_id=uuid.UUID(int=500 + index).bytes,
                        snapshot_version=3,
                        schema_version=1,
                    )
                    for index in range(len(command.bindings))
                ],
            )
            payloads = overrides.pop(0) if overrides else [result.SerializeToString()]
            return [flight.Result(payload) for payload in payloads]
        if request.type == "zippy.release_fixed_snapshot.v1":
            release = wire.ReleaseFixedSnapshotRequest.FromString(bytes(request.body))
            released.append(release.snapshot_id)
            payloads = (
                overrides.pop(0)
                if overrides
                else [
                    wire.ReleaseFixedSnapshotResult(
                        protocol_version=1, released=True
                    ).SerializeToString()
                ]
            )
            return [flight.Result(payload) for payload in payloads]
        return original(self, request, options)

    monkeypatch.setattr(_FakeFlightClient, "do_action", action)
    monkeypatch.setattr(flight, "connect", state.connect)
    monkeypatch.setattr(
        ControlClient,
        "get_config",
        lambda self: {
            "flight": {"enabled": True, "endpoint": "127.0.0.1:17667"},
        },
    )
    with Client("tcp://127.0.0.1:17690", flight_token="read-token") as client:
        yield client, state, created, released, overrides


@pytest.mark.parametrize("manual_capture", [False, True])
def test_native_fixed_snapshot_clones_keep_capture_and_original_session(
    native_snapshot_client, manual_capture: bool
):
    client, state, created, released, _ = native_snapshot_client
    table = client.read_table("ticks", namespace="market", snapshot=not manual_capture)
    info = table.snapshot()
    assert table.snapshot()["snapshot_id"] == info["snapshot_id"]
    snapshot_id = uuid.UUID(info["snapshot_id"]).bytes
    connection = client.flight._read_connection
    clone = table.select("value")
    table.close()
    assert len(created) == 1 and not released
    client.flight.refresh_read_session()
    assert client.flight._read_connection is not connection
    assert client.flight._snapshot_connection(snapshot_id) is connection
    assert clone.collect().equals(state.result)
    assert state.query_commands[-1].fixed_snapshot_id == snapshot_id
    assert clone.info()["snapshot_id"] == info["snapshot_id"]
    assert clone.explain()["snapshot"] == info
    clone.close()
    assert released == [snapshot_id]
    with pytest.raises(RuntimeError, match="closed"):
        clone.select("value")
    assert snapshot_id not in client.flight._fixed_connections


def test_multi_table_snapshot_scope_and_join_have_independent_lifetimes(native_snapshot_client):
    client, state, created, released, _ = native_snapshot_client
    scope = client.open_snapshot([("market", "left"), ("market", "right")])
    snapshot_id = uuid.UUID(scope.info()["snapshot_id"]).bytes
    left = scope.read_table("left", namespace="market")
    right = scope.read_table("right", namespace="market")
    joined = left.join(right, on="value")
    clone = joined.head(1)
    with pytest.raises(ValueError, match="outside"):
        scope.read_table("missing")
    scope.close()
    left.close()
    right.close()
    joined.close()
    assert not released
    assert clone.collect().equals(state.result)
    assert len(state.query_commands[-1].bindings) == 2
    assert state.query_commands[-1].fixed_snapshot_id == snapshot_id
    assert [binding.name.table for binding in created[0].bindings] == ["left", "right"]
    clone.close()
    assert released == [snapshot_id]
    with pytest.raises(RuntimeError, match="closed"):
        scope.info()
    with pytest.raises(RuntimeError, match="closed"):
        scope.read_table("left", namespace="market")
    with pytest.raises(RuntimeError, match="closed"):
        scope.__enter__()


def test_join_snapshot_captures_every_query_source(native_snapshot_client):
    client, state, created, released, _ = native_snapshot_client
    left = client.read_table("left", namespace="market")
    right = client.read_table("right", namespace="market")
    third = client.read_table("third", namespace="other")
    joined = left.join(right, on="value").join(third, on="value")
    info = joined.snapshot()
    snapshot_id = uuid.UUID(info["snapshot_id"]).bytes
    assert [(binding.name.namespace, binding.name.table) for binding in created[0].bindings] == [
        ("market", "left"),
        ("market", "right"),
        ("other", "third"),
    ]
    assert joined.collect().equals(state.result)
    assert len(state.query_commands[-1].bindings) == 3
    assert state.query_commands[-1].fixed_snapshot_id == snapshot_id
    clone = joined.head(1)
    joined.close()
    assert not released
    clone.close()
    assert released == [snapshot_id]


def test_join_clones_captured_later_keep_independent_snapshot_schemas(
    native_snapshot_client, monkeypatch: pytest.MonkeyPatch
):
    client, state, created, released, _ = native_snapshot_client
    left = client.read_table("left", namespace="market")
    right = client.read_table("right", namespace="market")
    first = left.join(right, on="value")
    second = first.head(1)
    first_id = uuid.UUID(first.snapshot()["snapshot_id"]).bytes
    second_id = uuid.UUID(second.snapshot()["snapshot_id"]).bytes
    assert first_id != second_id
    assert len(created) == 2

    schema_reads: list[tuple[str, object]] = []
    original_schema = client.flight.get_table_schema

    def read_schema(name, **kwargs):
        schema_reads.append((name, kwargs.get("fixed_snapshot_id")))
        return original_schema(name, **kwargs)

    monkeypatch.setattr(client.flight, "get_table_schema", read_schema)
    assert first.collect().equals(state.result)
    assert second.collect().equals(state.result)
    assert ("right", str(uuid.UUID(bytes=first_id))) in schema_reads
    assert ("right", str(uuid.UUID(bytes=second_id))) in schema_reads
    first.close()
    second.close()
    assert released == [first_id, second_id]


def test_snapshot_refreshes_schema_cached_before_capture(
    native_snapshot_client, monkeypatch: pytest.MonkeyPatch
):
    client, state, _, _, _ = native_snapshot_client
    monkeypatch.setattr(
        client.flight,
        "list_tables",
        lambda *, namespace: [{"name": "ticks", "namespace": namespace}],
    )
    table = client.read_table("ticks", namespace="market")
    assert table.schema().names == ["value"]
    state.result = pa.table({"value": [10], "added": [1]})
    table.snapshot()
    assert table.schema().names == ["value", "added"]
    table.close()


def test_join_snapshot_deduplicates_sources_and_rejects_oversized_query(
    native_snapshot_client,
):
    client, _, created, released, _ = native_snapshot_client
    query = client.read_table("a", namespace="market").join(
        client.read_table("a", namespace="market"), on="value"
    )
    identity = uuid.UUID(query.snapshot()["snapshot_id"]).bytes
    assert len(created[0].bindings) == 1
    query.close()
    assert released == [identity]

    oversized = client.read_table("a", namespace="market")
    for _ in range(32):
        oversized = oversized.join(client.read_table("a", namespace="market"), on="value")
    with pytest.raises(ValueError, match="32 table bindings"):
        oversized.snapshot()
    oversized.close()
    assert len(created) == 1


def test_fixed_snapshot_scope_rejects_ambiguous_inputs_and_mixed_joins(native_snapshot_client):
    client, _, created, _, _ = native_snapshot_client
    for tables in [
        [],
        [("market", "a")] * 2,
        [("", "a")],
        ["ab"],
        [("market",)],
        [("market", str(index)) for index in range(33)],
    ]:
        with pytest.raises(ValueError, match="snapshot tables"):
            client.open_snapshot(tables)
    for ttl in [0, -1, 300001, True, 1.5]:
        with pytest.raises(ValueError, match="ttl_ms"):
            client.open_snapshot([("market", "a")], ttl_ms=ttl)
    assert not created
    with client.open_snapshot([("market", "a")]) as first:
        with client.open_snapshot([("market", "a")]) as second:
            with first.read_table("a", namespace="market") as left:
                with second.read_table("a", namespace="market") as right:
                    with pytest.raises(ValueError, match="same Client.open_snapshot"):
                        left.join(right, on="value")
                    with client.read_table("a", namespace="market") as current:
                        with pytest.raises(ValueError, match="same Client.open_snapshot"):
                            left.join(current, on="value")
    with client.read_table("a", namespace="market") as manual:
        assert manual.snapshot()["snapshot_id"]


@pytest.mark.parametrize(
    "mutation",
    [
        "version",
        "nil_id",
        "expiry",
        "revision",
        "empty_tables",
        "duplicate",
        "table_id",
        "schema",
        "snapshot",
        "malformed",
        "empty_response",
        "multiple_response",
        "oversized",
    ],
)
def test_fixed_snapshot_rejects_invalid_wire_proofs(native_snapshot_client, mutation):
    client, _, _, _, overrides = native_snapshot_client
    result = wire.FixedSnapshotInfo(
        protocol_version=1,
        snapshot_id=uuid.UUID(int=1).bytes,
        expires_at_ns=2**63 - 1,
        catalog_revision=1,
        tables=[
            wire.QueryTableVersion(
                table_id=uuid.UUID(int=2).bytes, snapshot_version=1, schema_version=1
            )
        ],
    )
    if mutation == "version":
        result.protocol_version = 2
    elif mutation == "nil_id":
        result.snapshot_id = bytes(16)
    elif mutation == "expiry":
        result.expires_at_ns = 0
    elif mutation == "revision":
        result.catalog_revision = 0
    elif mutation == "empty_tables":
        del result.tables[:]
    elif mutation == "duplicate":
        result.tables.append(result.tables[0])
    elif mutation == "table_id":
        result.tables[0].table_id = b"short"
    elif mutation == "schema":
        result.tables[0].schema_version = 0
    elif mutation == "snapshot":
        result.tables[0].snapshot_version = 0
    payloads = [result.SerializeToString()]
    if mutation == "malformed":
        payloads = [b"\xff"]
    elif mutation == "empty_response":
        payloads = []
    elif mutation == "multiple_response":
        payloads *= 2
    elif mutation == "oversized":
        payloads = [bytes(2 * 1024 * 1024 + 1)]
    overrides.append(payloads)
    with pytest.raises(ProtocolError, match="fixed snapshot"):
        client.flight.create_snapshot({"a": "a", "b": "b"})


def test_fixed_snapshot_release_validation_and_state_cursor_exclusion(native_snapshot_client):
    client, _, _, _, overrides = native_snapshot_client
    identity = uuid.UUID(int=42)
    for payload in [
        b"\xff",
        wire.ReleaseFixedSnapshotResult(protocol_version=2).SerializeToString(),
    ]:
        overrides.append([payload])
        with pytest.raises(ProtocolError, match="fixed snapshot release"):
            client.flight.release_snapshot(identity)
    with pytest.raises(ValueError, match="atomic state cursor"):
        client.flight.read_sql(
            "SELECT * FROM a", {"a": "a"}, fixed_snapshot_id=identity, _key_value_snapshot=True
        )
    client.flight.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.flight._snapshot_connection(identity.bytes)


def test_fixed_snapshot_expired_session_uses_new_auth_without_recapture(native_snapshot_client):
    client, _, created, _, overrides = native_snapshot_client
    info = client.flight.create_snapshot({"ticks": "ticks"})
    identity = uuid.UUID(info["snapshot_id"]).bytes
    connection, expiry = client.flight._fixed_connections[identity]
    expired = replace(connection, session=replace(connection.session, expires_at_ns=1))
    client.flight._fixed_connections[identity] = (expired, expiry)
    assert client.flight._snapshot_connection(identity) is not expired
    assert identity not in client.flight._fixed_connections
    assert len(created) == 1
    assert client.flight.release_snapshot(identity)
    invalid = wire.FixedSnapshotInfo(
        protocol_version=1,
        snapshot_id=uuid.UUID(int=1).bytes,
        expires_at_ns=2**63,
        catalog_revision=1,
        tables=[
            wire.QueryTableVersion(
                table_id=uuid.UUID(int=2).bytes, snapshot_version=1, schema_version=1
            )
        ],
    )
    overrides.append([invalid.SerializeToString()])
    with pytest.raises(ProtocolError, match="expiry"):
        client.flight.create_snapshot({"ticks": "ticks"})


def test_snapshot_owners_close_cleanly_after_client_revokes_session(native_snapshot_client):
    client, _, created, released, _ = native_snapshot_client
    scope = client.open_snapshot([("market", "ticks")])
    table = scope.read_table("ticks", namespace="market")
    direct = client.read_table("ticks", namespace="market", snapshot=True)
    assert len(created) == 2
    client.close()
    table.close()
    scope.close()
    direct.close()
    assert not released


def test_fixed_table_waits_before_capturing_native_boundary(native_snapshot_client, monkeypatch):
    client, _, created, _, _ = native_snapshot_client
    calls = []

    def wait(source, namespace, timeout):
        assert not created
        calls.append((source, namespace, timeout))

    monkeypatch.setattr(client, "_wait_for_table", wait)
    with client.read_table("ticks", namespace="market", snapshot=True, wait=True, timeout=2):
        assert calls == [("ticks", "market", 2)]
        assert len(created) == 1
    client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.open_snapshot([("market", "ticks")])


def test_durable_write_reconnect_reuses_batch_identity_without_recomputing(monkeypatch):
    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.extend(
        ["ingest.resume_after_restart.v1", "ingest.table_identity.v1"]
    )
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    original = _FakeMetadataReader.read
    lost = False

    def read(self):
        nonlocal lost
        result = original(self)
        if result is not None and not lost:
            lost = True
            raise flight.FlightUnavailableError("injected lost durable receipt")
        return result

    monkeypatch.setattr(_FakeMetadataReader, "read", read)
    data = pa.table({"value": [10, 20, 30]})
    receipts = transport.write_table(
        "ticks", data, expected_table_id=uuid.UUID(int=303), max_reconnect_attempts=2
    )
    transport.close()
    assert len(receipts) == 2
    assert len(state.put_commands) == 3
    first, retry, next_batch = state.put_commands
    assert first.put_id == retry.put_id != next_batch.put_id
    assert first.first_batch_sequence == retry.first_batch_sequence == 0
    assert next_batch.first_batch_sequence == 1
    assert first.producer_id == retry.producer_id == next_batch.producer_id
    assert first.connection_id == retry.connection_id == next_batch.connection_id
    assert state.put_batches[0][0].equals(state.put_batches[1][0])
    assert first.expected_table_id == retry.expected_table_id == uuid.UUID(int=303).bytes
    assert state.handshake_roles.count(wire.SESSION_ROLE_WRITER) == 2


def test_write_recovery_rejects_unsafe_settings_and_nonretryable_failures(monkeypatch):
    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.extend(
        ["ingest.resume_after_restart.v1", "ingest.table_identity.v1"]
    )
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    data = pa.table({"value": [10]})
    invalid_attempts: list[Any] = [-1, 11, True, 0.5]
    for attempts in invalid_attempts:
        with pytest.raises(ValueError, match="max_reconnect_attempts"):
            transport.write_table("ticks", data, max_reconnect_attempts=attempts)
    invalid_options: list[dict[str, Any]] = [
        {"durability": "memory", "expected_table_id": uuid.UUID(int=303)},
        {},
    ]
    for kwargs in invalid_options:
        with pytest.raises(ValueError, match="durable writes"):
            transport.write_table("ticks", data, max_reconnect_attempts=2, **kwargs)
    rejected = ServerError("STALE_PRODUCER_EPOCH", "another writer took over", retryable=False)

    def reject(self, batch):
        raise rejected

    monkeypatch.setattr(_FakePutWriter, "write_batch", reject)
    with pytest.raises(AmbiguousWriteError) as error:
        transport.write_table(
            "ticks", data, expected_table_id=uuid.UUID(int=303), max_reconnect_attempts=2
        )
    assert len(state.put_commands) == 1
    cause: BaseException | None = error.value
    while cause is not None and cause is not rejected:
        cause = cause.__cause__
    assert cause is rejected
    transport.close()


def test_write_recovery_budget_preserves_confirmed_prefix(monkeypatch):
    state = _FakeFlightState(authenticated=True)
    state.capabilities.capabilities.extend(
        ["ingest.resume_after_restart.v1", "ingest.table_identity.v1"]
    )
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    original = _FakePutWriter.write_batch

    def disconnect(self, batch):
        if batch.column("value")[0].as_py() == 30:
            raise flight.FlightUnavailableError("injected persistent disconnect")
        original(self, batch)

    monkeypatch.setattr(_FakePutWriter, "write_batch", disconnect)
    with pytest.raises(AmbiguousWriteError) as error:
        transport.write_table(
            "ticks",
            pa.table({"value": [10, 20, 30]}),
            expected_table_id=uuid.UUID(int=303),
            max_reconnect_attempts=2,
        )
    assert [receipt["batch_sequence"] for receipt in error.value.receipts] == [0]
    assert len(state.put_commands) == 4
    assert len({command.put_id for command in state.put_commands[1:]}) == 1
    assert all(command.first_batch_sequence == 1 for command in state.put_commands[1:])
    assert not transport._active_puts
    transport.close()


def test_native_clients_have_independent_lifetimes(standalone_native_client) -> None:
    import zippy_client as zp

    first, peer, _ = standalone_native_client
    with zp.Client(first.uri) as second:
        first.close()
        with pytest.raises(RuntimeError, match="client is closed"):
            first.read_table("unused")
        assert second.read_table("ticks", namespace="market").collect().equals(peer.table)
        for name in ("connect", "get_writer", "read_table", "read_sql", "master", "subscribe"):
            assert not callable(getattr(zp, name, None))


@pytest.mark.parametrize(
    ("code", "retryable", "failures", "attempts", "timeout"),
    [
        ("RESOURCE_EXHAUSTED", True, 2, 3, 60),
        ("RESOURCE_EXHAUSTED", True, 9, 3, 60),
        ("RESOURCE_EXHAUSTED", False, 9, 1, 60),
        ("CONFLICT", True, 9, 1, 60),
        ("RESOURCE_EXHAUSTED", True, 9, 1, 0.01),
    ],
)
def test_native_management_backpressure_reuses_exact_request(
    monkeypatch, code, retryable, failures, attempts, timeout
):
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    original = _FakeFlightClient.do_action
    payloads = []
    waits: list[float] = []
    failure = ServerError(code, "temporary table lock", retryable=retryable)

    def do_action(self, action, options):
        if action.type == "zippy.create_namespace.v1":
            payloads.append(bytes(action.body))
            assert 0 < options.timeout <= 60
            if len(payloads) <= failures:
                raise failure
        return original(self, action, options)

    monkeypatch.setattr(_FakeFlightClient, "do_action", do_action)
    # Avoid replacing the shared time module; only the transport's timer dependency is fake.
    import zippy_client.transports.flight as transport_module
    import time

    monkeypatch.setattr(
        transport_module,
        "time",
        SimpleNamespace(monotonic=time.monotonic, time_ns=time.time_ns, sleep=waits.append),
    )
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token=None,
        admin_token="admin-token",
    )
    transport._connection("admin")
    transport.timeout_sec = timeout
    try:
        if failures < attempts:
            result = transport.create_namespace(
                "market", namespace_id=uuid.UUID(int=1), request_id=uuid.UUID(int=2)
            )
            assert result["object_id"] == str(uuid.UUID(int=1))
        else:
            with pytest.raises(ServerError) as caught:
                transport.create_namespace(
                    "market", namespace_id=uuid.UUID(int=1), request_id=uuid.UUID(int=2)
                )
            assert caught.value is failure
        assert len(payloads) == attempts
        assert len(set(payloads)) == 1
        assert waits == [0.05, 0.1][: attempts - 1]
    finally:
        transport.close()


def test_close_revokes_all_roles_and_refreshed_sessions_once(monkeypatch):
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667",
        tls_enabled=False,
        read_token="read-token",
        writer_token="writer-token",
        admin_token="admin-token",
    )
    for role in ("read", "writer", "admin"):
        transport._connection(role)
    previous = transport._read_connection
    transport.refresh_read_session()
    assert previous in transport._owned_connections
    transport.close()
    transport.close()
    assert len(state.closed_sessions) == 4
    assert all(client.closed for client in state.clients)
    assert len(state.handshake_roles) == 4


@pytest.mark.parametrize("failure", ["malformed", "version", "empty", "denied"])
def test_close_protocol_failure_still_closes_every_channel(monkeypatch, failure):
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token="writer-token"
    )
    transport._connection("read")
    transport._connection("writer")
    calls = []

    def invalid(action, options):
        calls.append(action.type)
        if failure == "denied":
            raise ServerError("PERMISSION_DENIED", "denied", retryable=False)
        if failure == "empty":
            return []
        payload = (
            b"\xff"
            if failure == "malformed"
            else wire.SessionCloseResponse(protocol_version=2, closed=True).SerializeToString()
        )
        return [flight.Result(payload)]

    for connection in transport._owned_connections:
        monkeypatch.setattr(connection.client, "do_action", invalid)
    with pytest.raises((ProtocolError, ServerError)):
        transport.close()
    assert calls == ["zippy.close_session.v1"] * 2
    assert all(client.closed for client in state.clients)


@pytest.mark.parametrize(
    "failure",
    [
        AuthenticationError("expired"),
        ServerError("UNAUTHENTICATED", "expired", retryable=False),
        ZippyConnectionError("server stopped"),
    ],
)
def test_close_invalidated_session_does_not_open_replacement(monkeypatch, failure):
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token=None
    )
    connection = transport._connection("read")

    def invalid(action, options):
        raise failure

    monkeypatch.setattr(connection.client, "do_action", invalid)
    transport.close()
    assert len(state.handshake_roles) == 1
    assert connection.client.closed


def test_expired_role_channels_are_pruned_without_revocation(monkeypatch):
    state = _FakeFlightState(authenticated=True)
    monkeypatch.setattr(flight, "connect", state.connect)
    transport = FlightTransport(
        "127.0.0.1:17667", tls_enabled=False, read_token="read-token", writer_token=None
    )
    connection = transport._connection("read")
    expired = replace(connection, session=replace(connection.session, expires_at_ns=0))
    transport._read_connection = expired
    transport._owned_connections = [expired]
    transport.refresh_read_session()
    assert connection.client.closed
    assert len(transport._owned_connections) == 1
    transport.close()
    assert len(state.closed_sessions) == 1
