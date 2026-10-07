"""Explicitly owned client connections and capability-based operation routing."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import os
from pathlib import Path
import time
from typing import Any
import uuid
import weakref

from loguru import logger
import pyarrow as pa

from .control import ControlClient
from .endpoints import (
    connectable_data_endpoint,
    resolve_control_endpoint,
)
from .errors import (
    CapabilityError,
    ServerError,
    TransportUnavailableError,
    ZippyConnectionError,
)
from .log_reader import FlightLogReader, LogCursor
from .models import Operation, ServerCapabilities, Transport, TransportDecision
from .query import Table
from .snapshot import FixedSnapshot
from .router import TransportRouter
from .flight_subscriber import FlightSubscriber, SubscriptionOptions
from .transports.flight import FlightCapabilities, FlightTransport
from .writer import FlightWriter


class Client:
    """
    Connect to one Zippy server and route operations by capability.

    :param uri: Master URI such as ``zippy://default`` or
        ``zippy://127.0.0.1:17690/default``.
    :type uri: str | None
    :param app: Optional process lease name registered immediately.
    :type app: str | None
    :param transport: Default operation transport.
    :type transport: str | Transport
    :param flight_token: Optional Flight read bootstrap token.
    :type flight_token: str | None
    :param flight_admin_token: Optional Flight administrator bootstrap token.
    :type flight_admin_token: str | None
    :param flight_writer_token: Optional Flight writer bootstrap token.
    :type flight_writer_token: str | None
    :param flight_tls_root_cert_file: Optional PEM trust roots for Flight TLS.
    :type flight_tls_root_cert_file: str | Path | None
    :param flight_tls_cert_chain_file: Optional mTLS client certificate chain.
    :type flight_tls_cert_chain_file: str | Path | None
    :param flight_tls_private_key_file: Optional mTLS client private key.
    :type flight_tls_private_key_file: str | Path | None
    :param flight_tls_server_name: Optional TLS server-name override.
    :type flight_tls_server_name: str | None
    :param heartbeat_interval_sec: Process lease heartbeat interval.
    :type heartbeat_interval_sec: float
    :param timeout_sec: Control request timeout.
    :type timeout_sec: float
    """

    def __init__(
        self,
        uri: str | None = None,
        *,
        app: str | None = None,
        transport: str | Transport = Transport.AUTO,
        flight_token: str | None = None,
        flight_writer_token: str | None = None,
        flight_admin_token: str | None = None,
        flight_tls_root_cert_file: str | Path | None = None,
        flight_tls_cert_chain_file: str | Path | None = None,
        flight_tls_private_key_file: str | Path | None = None,
        flight_tls_server_name: str | None = None,
        heartbeat_interval_sec: float = 3.0,
        timeout_sec: float = 5.0,
    ) -> None:
        self.uri = uri
        self.default_transport = Transport.parse(transport)
        self.control_endpoint = resolve_control_endpoint(uri)
        self.control = ControlClient(
            self.control_endpoint,
            timeout_sec=timeout_sec,
            heartbeat_interval_sec=heartbeat_interval_sec,
        )
        self.config = self.control.get_config()
        flight_config = self.config.get("flight", {})
        if not isinstance(flight_config, dict):
            flight_config = {}
        flight_endpoint = None
        if flight_config.get("enabled") is not False and flight_config.get("endpoint"):
            flight_endpoint = connectable_data_endpoint(
                str(flight_config["endpoint"]), self.control_endpoint
            )
        tls_config = flight_config.get("tls", {})
        if not isinstance(tls_config, dict):
            tls_config = {}
        read_token = flight_token or os.environ.get("ZIPPY_FLIGHT_AUTH_TOKEN")
        writer_token = flight_writer_token or os.environ.get("ZIPPY_FLIGHT_AUTH_WRITER_TOKEN")
        root_cert_file = flight_tls_root_cert_file or os.environ.get(
            "ZIPPY_FLIGHT_TLS_ROOT_CERT_FILE"
        )
        cert_chain_file = flight_tls_cert_chain_file or os.environ.get(
            "ZIPPY_FLIGHT_TLS_CLIENT_CERT_FILE"
        )
        private_key_file = flight_tls_private_key_file or os.environ.get(
            "ZIPPY_FLIGHT_TLS_CLIENT_KEY_FILE"
        )
        server_name = flight_tls_server_name or os.environ.get("ZIPPY_FLIGHT_TLS_SERVER_NAME")
        if flight_endpoint is None:
            raise TransportUnavailableError("server does not advertise an Arrow Flight endpoint")
        TransportRouter(self.control_endpoint, flight_endpoint).select(
            Operation.QUERY, self.default_transport
        )
        self._flight = FlightTransport(
            flight_endpoint,
            tls_enabled=bool(tls_config.get("enabled", False)),
            read_token=read_token,
            writer_token=writer_token,
            admin_token=flight_admin_token or os.environ.get("ZIPPY_FLIGHT_AUTH_ADMIN_TOKEN"),
            timeout_sec=float(flight_config.get("timeout_sec", 60.0) or 60.0),
            configured_max_message_bytes=_int_config(
                flight_config.get("max_message_bytes"), 2 * 1024 * 1024
            ),
            tls_root_certs=_read_optional_file(root_cert_file),
            cert_chain=_read_optional_file(cert_chain_file),
            private_key=_read_optional_file(private_key_file),
            override_hostname=server_name,
        )
        self.capabilities = _capabilities(self._flight.capabilities)
        self.router = TransportRouter(
            self.control_endpoint, flight_endpoint, self.capabilities.flight_ingest
        )
        self._subscriptions: weakref.WeakSet[FlightLogReader | FlightSubscriber] = weakref.WeakSet()
        self._closed = False
        if app is not None:
            self.control.ensure_registered(app)

    @property
    def flight(self) -> FlightTransport:
        """Return the discovered Arrow Flight transport."""
        return self._flight

    @property
    def query_transport(self) -> Transport:
        """Return the selected table-query transport."""
        return self.router.select(Operation.QUERY, self.default_transport).selected

    def transport_for(
        self,
        operation: str | Operation,
        *,
        transport: str | Transport | None = None,
    ) -> TransportDecision:
        """
        Explain transport selection without starting the operation.

        :param operation: Operation name.
        :type operation: str | Operation
        :param transport: Optional per-operation transport override.
        :type transport: str | Transport | None
        :returns: Transport selection decision.
        :rtype: TransportDecision
        """
        normalized_operation = (
            operation if isinstance(operation, Operation) else Operation(str(operation))
        )
        return self.router.select(
            normalized_operation,
            transport or self.default_transport,
        )

    def list_tables(self, *, namespace: str | None = None) -> list[dict[str, object]]:
        """Return native Catalog metadata, optionally restricted to one namespace.

        :param namespace: Exact namespace filter, or all visible tables when omitted.
        :returns: Native table name, namespace, catalog and type records.
        """
        self._ensure_open()
        return self.flight.list_tables(namespace=namespace)

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
        return self.flight.create_namespace(name, namespace_id=namespace_id, request_id=request_id)

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
        return self.flight.alter_table_schema(
            table_id,
            schema,
            expected_schema_version=expected_schema_version,
            request_id=request_id,
            keys=keys,
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
        return self.flight.drop_table(
            table_id,
            expected_snapshot_version=expected_snapshot_version,
            request_id=request_id,
        )

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
        return self.flight.create_append_table(
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
        return self.flight.create_frame_table(
            name,
            schema,
            namespace_id=namespace_id,
            table_id=table_id,
            request_id=request_id,
            by=by,
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
        return self.flight.create_key_value_table(
            name,
            schema,
            keys=keys,
            namespace_id=namespace_id,
            table_id=table_id,
            request_id=request_id,
        )

    def upsert_key_value(
        self,
        name: str,
        rows: pa.Table | pa.RecordBatch,
        *,
        namespace: str = "default",
        durability: str = "wal",
    ) -> list[dict[str, object]]:
        """Atomically insert or replace logical rows, taking the last row for duplicate keys.

        :param name: Existing native key-value table.
        :param rows: Complete logical Arrow rows, combined into one mutation batch.
        :param namespace: Explicit Catalog namespace.
        :param durability: Requested memory, wal or persisted acknowledgement.
        :returns: One receipt, or an empty list for empty input.
        :raises CapabilityError: If atomic batch limits or native support are insufficient.
        :raises ServerError: If the mutation is rejected or its outcome is uncertain.
        :example: client.upsert_key_value("quotes", rows, namespace="market")
        """
        if isinstance(rows, pa.RecordBatch):
            rows = pa.Table.from_batches([rows])
        return self.flight.write_table(
            name,
            rows,
            namespace=namespace,
            durability=durability,
            mutation="upsert",
        )

    def delete_key_value(
        self,
        name: str,
        keys: pa.Table | pa.RecordBatch,
        *,
        namespace: str = "default",
        durability: str = "wal",
    ) -> list[dict[str, object]]:
        """Atomically delete keys; missing keys have no effect.

        :param name: Existing native key-value table.
        :param keys: Ordered primary-key columns with their exact declared Arrow schema.
        :param namespace: Explicit Catalog namespace.
        :param durability: Requested memory, wal or persisted acknowledgement.
        :returns: One receipt, or an empty list for empty input.
        :raises CapabilityError: If atomic batch limits or native support are insufficient.
        :raises ServerError: If the mutation is rejected or its outcome is uncertain.
        :example: client.delete_key_value("quotes", keys, namespace="market")
        """
        if isinstance(keys, pa.RecordBatch):
            keys = pa.Table.from_batches([keys])
        return self.flight.write_table(
            name,
            keys,
            namespace=namespace,
            durability=durability,
            mutation="delete",
        )

    def write_frame(
        self,
        name: str,
        frame: pa.Table | pa.RecordBatch,
        *,
        namespace: str = "default",
        durability: str = "wal",
    ) -> list[dict[str, object]]:
        """Write one complete business frame without splitting its rows.

        :param name: Existing native frame table name.
        :param frame: Complete Arrow frame; an empty frame is a no-op.
        :param namespace: Explicit Catalog namespace.
        :param durability: Requested memory, wal or persisted acknowledgement.
        :returns: The single frame receipt, or an empty list for an empty frame.
        :raises CapabilityError: If complete-frame capacity or native support is insufficient.
        :raises ServerError: If the server rejects the frame or its outcome is uncertain.
        :example: client.write_frame("factors", frame, namespace="market")
        """
        if isinstance(frame, pa.RecordBatch):
            frame = pa.Table.from_batches([frame])
        return self.flight.write_table(
            name, frame, namespace=namespace, durability=durability, mutation="frame"
        )

    def open_snapshot(
        self, tables: Iterable[tuple[str, str]], *, ttl_ms: int = 60_000
    ) -> FixedSnapshot:
        """Capture one native multi-table boundary for repeated queries and joins.

        :param tables: Explicit namespace/table pairs captured together.
        :param ttl_ms: Lifetime capped by the authenticated session; no automatic renewal.
        :returns: Context-managed snapshot scope, with independently retained table views.
        :raises CapabilityError: If native fixed snapshots are unavailable.
        :example: with client.open_snapshot([("market", "ticks")]) as snapshot: ...
        """
        self._ensure_open()
        return FixedSnapshot(self, tables, ttl_ms=ttl_ms)

    def read_table(
        self,
        source: str,
        *,
        wait: bool = False,
        timeout: float | str | None = None,
        snapshot: bool = False,
        namespace: str = "default",
    ) -> Table:
        """Open a lazy server-owned table query."""
        self._ensure_open()
        if wait:
            self._wait_for_table(source, namespace, timeout)
        return Table(source, self, snapshot=snapshot, namespace=namespace)

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
        """Read native append events, complete frames or complete KV mutations over Flight.

        :param source: Table name, omitted when reconnecting with a cursor.
        :param namespace: Catalog namespace.
        :param table_id: Immutable table identity instead of a name.
        :param start: ``latest`` / ``earliest`` for raw logs, or ``committed`` with consumer_id.
        :param cursor: Last complete delivery cursor; reconnects use immutable table identity.
        :param consumer_id: Durable consumer identity; requires ``start="committed"``.
        :param group_id: Shared group authority; omitted for an individual consumer.
        :param deadline_ns: Optional absolute UTC Unix-nanosecond deadline.
        :param memory_limit_bytes: Optional native query memory budget.
        :param batch_rows: Wire fragment row limit, independent of atomic delivery boundaries.
        :param max_pending_bytes: Client atomic-assembly buffer limit.
        :param open_timeout_sec: Maximum opening time, independent of stream lifetime.
        :returns: Context-managed event iterator, also stopped by Client.close.
        :raises CapabilityError: The server does not provide native log subscriptions.
        :example: with client.open_log("quotes", start="earliest") as events: next(events)
        """
        self._ensure_open()
        reader = self.flight.open_log(
            source,
            namespace=namespace,
            table_id=table_id,
            start=start,
            cursor=cursor,
            consumer_id=consumer_id,
            group_id=group_id,
            deadline_ns=deadline_ns,
            memory_limit_bytes=memory_limit_bytes,
            batch_rows=batch_rows,
            max_pending_bytes=max_pending_bytes,
            open_timeout_sec=open_timeout_sec,
        )
        self._subscriptions.add(reader)
        return reader

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
        """Load a complete native key-value state with an exact protocol-v2 cursor.

        :param source: Table name.
        :param namespace: Catalog namespace.
        :param table_id: Immutable table identity instead of a name.
        :param deadline_ns: Optional absolute deadline in UTC Unix nanoseconds.
        :param memory_limit_bytes: Optional server query memory budget.
        :param result_batch_rows: Optional transfer batch row limit.
        :returns: Arrow table accepted by ``zippy.KeyValueState``.
        :raises CapabilityError: Native state snapshots are unavailable.
        :raises ProtocolError: The returned cursor or schema is invalid.
        :example: state = client.snapshot_state("quotes", namespace="market")
        """
        self._ensure_open()
        return self.flight.snapshot_state(
            source,
            namespace=namespace,
            table_id=table_id,
            deadline_ns=deadline_ns,
            memory_limit_bytes=memory_limit_bytes,
            result_batch_rows=result_batch_rows,
        )

    def read_sql(
        self,
        sql: str,
        *,
        bindings: dict[str, object] | None = None,
        priority: str = "interactive",
        deadline_ns: int | None = None,
        memory_limit_bytes: int | None = None,
        spill_limit_bytes: int | None = 0,
        result_batch_rows: int | None = None,
    ) -> pa.Table:
        """
        Execute server-side SQL and return a PyArrow table.

        Flight requires explicit relation-to-catalog ``bindings`` and executes
        the SQL in the server-owned DataFusion service. Gateway SQL and implicit
        binding inference are not supported.

        :param sql: SQL text executed by the native DataFusion service.
        :param bindings: Explicit relation aliases mapped to Catalog table identities or names.
        :param priority: Interactive, normal or maintenance admission class.
        :param deadline_ns: Optional absolute UTC Unix-nanosecond deadline.
        :param memory_limit_bytes: Optional query memory budget.
        :param spill_limit_bytes: Disk spill budget; only zero or None is currently supported.
        :param result_batch_rows: Preferred rows per Flight transfer batch.
        :returns: Complete Arrow result assembled from the Flight batch stream.
        :raises CapabilityError: Flight SQL or explicit bindings are unavailable.
        :raises TransportUnavailableError: The configured transport cannot execute native SQL.
        :example: client.read_sql("SELECT * FROM ticks", bindings={"ticks": "market_ticks"})
        """
        self._ensure_open()
        if not self.capabilities.query_sql:
            raise CapabilityError(
                "connected server does not advertise query_sql; "
                "upgrade or enable the server-owned DataFusion query service"
            )
        self.router.select(Operation.SQL, self.default_transport)
        if not bindings:
            raise CapabilityError(
                "Flight read_sql requires explicit bindings, for example "
                "bindings={'ticks': 'ticks'}"
            )
        table, _ = self.flight.read_sql(
            sql,
            bindings,
            priority=priority,
            deadline_ns=deadline_ns,
            memory_limit_bytes=memory_limit_bytes,
            spill_limit_bytes=spill_limit_bytes,
            result_batch_rows=result_batch_rows,
        )
        return table

    def get_writer(
        self,
        stream_name: str,
        *,
        namespace: str = "default",
        schema: object | None = None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
        durability: str | None = None,
        transport: str | Transport | None = None,
    ) -> FlightWriter:
        """Return a native Flight writer, requiring WAL unless another boundary is explicit.

        :param stream_name: Existing native table name.
        :param namespace: Catalog namespace containing the table.
        :param schema: Explicit row-conversion schema, otherwise fetched from Flight.
        :param batch_size: Maximum buffered scalar rows per flush.
        :param flush_interval_ms: Flush interval checked on subsequent writes.
        :param durability: Memory, WAL (default), or Persisted confirmation boundary.
        :param transport: Optional Flight locality constraint.
        :returns: Independently owned writer; close it before closing the client.
        :raises ServerError: Schema lookup or authorization fails.
        """
        self._ensure_open()
        self.router.select(Operation.WRITE, transport or self.default_transport)
        if schema is None:
            schema = self.flight.get_table_schema(stream_name, namespace=namespace)
        return FlightWriter(
            stream_name,
            self.flight,
            namespace=namespace,
            schema=schema,
            batch_size=batch_size,
            flush_interval_ms=flush_interval_ms,
            durability="wal" if durability is None else durability,
        )

    def subscribe(
        self,
        source: str | None,
        callback: Callable[[Any], Any],
        *,
        options: SubscriptionOptions | None = None,
    ) -> FlightSubscriber:
        """Start ordered append-row callbacks over the native Flight log.

        :param source: Catalog table name, or None with options.table_id/cursor.
        :param callback: Called once per selected row; exceptions stop delivery.
        :param options: Native start, recovery, filtering and resource options.
        :returns: Started subscriber owned by this client.
        :raises ValueError: The source is frame/KV; use subscribe_table for atomic units.
        :example: subscriber = client.subscribe("ticks", print)
        """
        return self._subscribe(source, callback, options or SubscriptionOptions(), False)

    def subscribe_table(
        self,
        source: str | None,
        callback: Callable[[Any], Any],
        *,
        options: SubscriptionOptions | None = None,
    ) -> FlightSubscriber:
        """Start Arrow callbacks for append batches, complete frames or complete KV mutations.

        :param source: Catalog table name, or None with options.table_id/cursor.
        :param callback: Called serially after a complete native event is assembled.
        :param options: Native start, recovery, filtering and resource options.
        :returns: Started subscriber; durable ACK follows callback success.
        :raises ValueError: Filtering/truncation is requested for an atomic frame/KV source.
        :example: subscriber = client.subscribe_table("frames", print)
        """
        return self._subscribe(source, callback, options or SubscriptionOptions(), True)

    def _subscribe(
        self,
        source: str | None,
        callback: Callable[[Any], Any],
        options: SubscriptionOptions,
        table_callback: bool,
    ) -> FlightSubscriber:
        self._ensure_open()
        # Subscription authority is always the native log. Never consult the old
        # registry for SHM descriptors or synthetic KV changelog stream names.
        self.router.select(Operation.SUBSCRIBE, self.default_transport)
        if options.cursor is not None and (source is not None or options.table_id is not None):
            raise ValueError("resume cursor cannot be combined with a source or table_id")

        def open_reader(cursor: LogCursor | None) -> FlightLogReader:
            self._ensure_open()
            if cursor is not None:
                self.flight.refresh_read_session(timeout_sec=options.open_timeout_sec)
            return self.flight.open_log(
                source if cursor is None else None,
                namespace=options.namespace,
                table_id=(
                    (cursor.table_id if options.consumer_id is not None else None)
                    if cursor is not None
                    else options.table_id
                ),
                start=(
                    "latest"
                    if cursor is not None and options.consumer_id is None
                    else options.start
                ),
                cursor=cursor if options.consumer_id is None else None,
                consumer_id=options.consumer_id,
                group_id=options.group_id,
                deadline_ns=options.deadline_ns,
                memory_limit_bytes=options.memory_limit_bytes,
                batch_rows=options.batch_rows,
                max_pending_bytes=options.max_pending_bytes,
                open_timeout_sec=options.open_timeout_sec,
            )

        subscriber = FlightSubscriber(
            source or str(options.table_id),
            callback,
            open_reader,
            options=options,
            table_callback=table_callback,
        )
        self._subscriptions.add(subscriber)
        try:
            return subscriber.start()
        except BaseException:
            self._subscriptions.discard(subscriber)
            raise

    def close(self) -> None:
        """Stop active subscriptions and release the control process lease."""
        if self._closed:
            return
        self._closed = True
        first_error: BaseException | None = None
        for subscriber in list(self._subscriptions):
            try:
                subscriber.stop()
            except BaseException as error:
                first_error = first_error or error
                logger.error("failed to stop client subscription error=[{}]", error)
        try:
            self._flight.close()
        except BaseException as error:
            first_error = first_error or error
            logger.error("failed to close Flight transport error=[{}]", error)
        try:
            self.control.close()
        except BaseException as error:
            first_error = first_error or error
        if first_error is not None:
            raise first_error

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def _wait_for_table(self, source: str, namespace: str, timeout: float | str | None) -> None:
        timeout_sec = _parse_timeout(timeout)
        deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(
                    f"table did not become ready namespace=[{namespace}] source=[{source}]"
                )
            deadline_ns = (
                None if remaining is None else time.time_ns() + int(remaining * 1_000_000_000)
            )
            try:
                self.flight.get_table_schema(source, namespace=namespace, deadline_ns=deadline_ns)
                return
            except ServerError as error:
                if error.code == "DEADLINE_EXCEEDED" and deadline is not None:
                    raise TimeoutError(
                        f"table did not become ready namespace=[{namespace}] source=[{source}]"
                    ) from error
                if error.code != "NOT_FOUND" and not error.retryable:
                    raise
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"table did not become ready namespace=[{namespace}] source=[{source}]"
                    ) from error
            time.sleep(0.01)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Zippy client is closed")


def _capabilities(capabilities: FlightCapabilities) -> ServerCapabilities:
    return ServerCapabilities(
        flight=True,
        flight_protocol_version=capabilities.protocol_version,
        flight_ingest=(
            capabilities.memory_ingest or capabilities.wal_ingest or capabilities.persisted_ingest
        ),
        query_sql=capabilities.query,
        resume_cursor="subscription.log_fragments.v1" in capabilities.names,
    )


def _read_optional_file(value: str | Path | None) -> bytes | None:
    if value is None:
        return None
    path = Path(value).expanduser()
    try:
        return path.read_bytes()
    except OSError as error:
        raise ZippyConnectionError(
            f"failed to read Flight TLS file path=[{path}] error=[{error}]"
        ) from error


def _parse_timeout(value: float | str | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        timeout = float(value)
    else:
        text = str(value).strip().lower()
        suffixes = (("ms", 0.001), ("s", 1.0), ("m", 60.0), ("h", 3600.0))
        for suffix, multiplier in suffixes:
            if text.endswith(suffix):
                timeout = float(text[: -len(suffix)]) * multiplier
                break
        else:
            timeout = float(text)
    if timeout < 0:
        raise ValueError("timeout must be non-negative")
    return timeout


def _int_config(value: object, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError(f"configuration value must be an integer value=[{value}]")
    return int(value)
