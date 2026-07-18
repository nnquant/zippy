"""Flight protocol, SQL compilation, and writer contract tests."""

from __future__ import annotations

from collections import deque
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
        *,
        fail_before_first_batch: bool = False,
    ) -> None:
        self.command = command
        self.metadata = metadata
        self.batches = batches
        self.fail_before_first_batch = fail_before_first_batch
        self.closed = False

    def write_batch(self, batch: pa.RecordBatch) -> None:
        if self.fail_before_first_batch and not self.batches:
            raise flight.FlightUnavailableError("injected disconnect")
        index = len(self.batches)
        self.batches.append(batch)
        receipt = wire.PutBatchResult(
            protocol_version=1,
            put_id=self.command.put_id,
            batch_sequence=self.command.first_batch_sequence + index,
            row_count=batch.num_rows,
            durability_reached=wire.PUT_DURABILITY_MEMORY,
        )
        self.metadata.pending.append(pa.py_buffer(receipt.SerializeToString()))

    def done_writing(self) -> None:
        self.metadata.done = True

    def close(self) -> None:
        self.closed = True


class _FakeFlightState:
    def __init__(self, *, authenticated: bool) -> None:
        capabilities = [
            "query.get_flight_info_do_get.v1",
            "query.single_use_ticket.v1",
            "error.details.v1",
        ]
        if authenticated:
            capabilities.extend(
                [
                    "session.server_authority.v1",
                    "auth.bearer_handshake.v1",
                    "auth.session_roles.v1",
                    "ingest.append.memory.v1",
                ]
            )
        self.capabilities = wire.CapabilitiesResponse(
            protocol_version=1,
            server_version="test-0.5.0",
            capabilities=capabilities,
            max_message_bytes=2 * 1024 * 1024,
            max_pending_tickets=8,
            authentication_required=authenticated,
            session_ttl_ms=60_000 if authenticated else 0,
            writer_authentication_enabled=authenticated,
            ingest_enabled=authenticated,
            ingest_max_concurrent_puts=2 if authenticated else 0,
            ingest_max_batches_per_put=4 if authenticated else 0,
            ingest_max_rows_per_batch=2 if authenticated else 0,
            ingest_max_rows_per_put=3 if authenticated else 0,
            ingest_max_batch_bytes=1024 * 1024 if authenticated else 0,
            ingest_max_bytes_per_put=2 * 1024 * 1024 if authenticated else 0,
        )
        self.query_commands: list[wire.QueryCommand] = []
        self.query_options: list[flight.FlightCallOptions] = []
        self.put_commands: list[wire.PutCommand] = []
        self.put_batches: list[list[pa.RecordBatch]] = []
        self.handshake_roles: list[int] = []
        self.clients: list[_FakeFlightClient] = []
        self.message_limits: list[int] = []
        self.fail_put_number: int | None = None
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
        assert action.type == CAPABILITIES_ACTION
        assert options.timeout == pytest.approx(60.0)
        request = wire.CapabilitiesRequest.FromString(bytes(action.body))
        assert request.protocol_version == 1
        return [flight.Result(self.state.capabilities.SerializeToString())]

    def authenticate(self, handler: Any, options: flight.FlightCallOptions) -> None:
        header = dict(options.headers)[b"authorization"]
        if header == b"Bearer read-token":
            role = wire.SESSION_ROLE_READ
            session_id = uuid.UUID("00000000-0000-0000-0000-000000000101").bytes
        elif header == b"Bearer writer-token":
            role = wire.SESSION_ROLE_WRITER
            session_id = uuid.UUID("00000000-0000-0000-0000-000000000202").bytes
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
                fail_before_first_batch=(self.state.fail_put_number == len(self.state.put_batches)),
            ),
            metadata,
        )


class _LoopbackFlightServer(flight.FlightServerBase):
    def __init__(self) -> None:
        super().__init__(location=("127.0.0.1", 0))
        self.table = pa.table({"value": [3, 5, 8]})
        self.commands: list[wire.QueryCommand] = []

    def do_action(self, context: object, action: flight.Action):
        del context
        assert action.type == CAPABILITIES_ACTION
        capabilities = wire.CapabilitiesResponse(
            protocol_version=1,
            server_version="loopback-test",
            capabilities=[
                "query.get_flight_info_do_get.v1",
                "query.single_use_ticket.v1",
                "error.details.v1",
            ],
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
    assert [len(batches) for batches in state.put_batches] == [1, 2]
    assert [receipt["batch_sequence"] for receipt in receipts] == [7, 8, 9]
    assert all(client.closed for client in state.clients)


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


def test_unauthenticated_flight_uses_explicit_session_header(
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
    session_header = dict(state.query_options[0].headers)[b"x-zippy-session-id"]
    assert uuid.UUID(session_header.decode("ascii")).bytes == command.session_id
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
    assert expression_to_sql(col("value").is_between(1, 3)) == ('("value" BETWEEN 1 AND 3)')
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

    def write_table(
        self,
        stream_name: str,
        table: pa.Table,
        *,
        first_batch_sequence: int = 0,
    ) -> list[dict[str, object]]:
        self.calls.append((stream_name, table, first_batch_sequence))
        return [
            {
                "put_id": str(uuid.uuid4()),
                "batch_sequence": first_batch_sequence,
                "row_count": table.num_rows,
                "durability": "memory",
            }
        ]


def test_flight_writer_buffers_rows_and_router_prefers_flight() -> None:
    transport = _WriterTransport()
    writer = FlightWriter("ticks", transport, batch_size=2, flush_interval_ms=None)
    writer.write({"value": 1})
    writer.write({"value": 2})
    writer.write_batch(pa.table({"value": [3, 4]}))
    writer.close()

    assert [call[1].num_rows for call in transport.calls] == [2, 2]
    assert [call[2] for call in transport.calls] == [0, 1]
    assert writer.last_receipts[0]["durability"] == "memory"

    router = TransportRouter(
        ControlEndpoint("tcp", "192.0.2.1:17690", "zippy://192.0.2.1:17690/default"),
        "192.0.2.1:17666",
        lambda: False,
        "192.0.2.1:17667",
        True,
    )
    assert router.select(Operation.QUERY, Transport.AUTO).selected == Transport.FLIGHT_REMOTE
    assert router.select(Operation.WRITE, Transport.AUTO).selected == Transport.FLIGHT_REMOTE
    assert router.select(Operation.SUBSCRIBE, Transport.AUTO).selected == Transport.GATEWAY_REMOTE

    query_only = TransportRouter(
        ControlEndpoint("tcp", "192.0.2.1:17690", "zippy://192.0.2.1:17690/default"),
        "192.0.2.1:17666",
        lambda: False,
        "192.0.2.1:17667",
        False,
    )
    assert query_only.select(Operation.WRITE, Transport.AUTO).selected == Transport.GATEWAY_REMOTE
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
            first_batch_sequence: int = 0,
        ) -> list[dict[str, object]]:
            del stream_name, table, first_batch_sequence
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
