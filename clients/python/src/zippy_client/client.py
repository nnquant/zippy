"""Unified public Client facade and process-wide compatibility functions."""

from __future__ import annotations

import atexit
from collections.abc import Callable
import os
from pathlib import Path
import threading
import time
from typing import Any
import weakref

from loguru import logger

from .control import ControlClient
from .endpoints import (
    connectable_data_endpoint,
    connectable_gateway_endpoint,
    resolve_control_endpoint,
)
from .errors import (
    CapabilityError,
    ProtocolError,
    TransportUnavailableError,
    ZippyConnectionError,
)
from .expressions import normalize_temporal_filter
from .models import Operation, ServerCapabilities, Transport, TransportDecision
from .protocol import GATEWAY_PROTOCOL_VERSION
from .query import Table
from .router import TransportRouter
from .subscriber import GatewaySubscriber, ShmSubscriber, shm_available
from .transports.flight import FlightTransport
from .transports.gateway import GatewayTransport
from .writer import FlightWriter, GatewayWriter

_DEFAULT_CLIENT: Client | None = None
_DEFAULT_CLIENT_LOCK = threading.RLock()
_DEFAULT_CLIENT_CONNECT_LOCK = threading.Lock()


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
    :param token: Optional Gateway authentication token.
    :type token: str | None
    :param flight_token: Optional Flight read bootstrap token.
    :type flight_token: str | None
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
        token: str | None = None,
        flight_token: str | None = None,
        flight_writer_token: str | None = None,
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
        self._gateway_query_sql = _gateway_query_sql_enabled(self.config)
        self._allow_auto_shm = True
        gateway_config = self.config.get("gateway", {})
        if not isinstance(gateway_config, dict):
            gateway_config = {}
        gateway_endpoint = None
        if gateway_config.get("enabled") is not False and gateway_config.get("endpoint"):
            gateway_endpoint = connectable_gateway_endpoint(
                str(gateway_config["endpoint"]), self.control_endpoint
            )
        gateway_token = (
            token
            or os.environ.get("ZIPPY_GATEWAY_TOKEN")
            or (str(gateway_config["token"]) if gateway_config.get("token") else None)
        )
        gateway_timeout = float(gateway_config.get("timeout_sec", 60.0) or 60.0)
        self._gateway = (
            GatewayTransport(gateway_endpoint, token=gateway_token, timeout_sec=gateway_timeout)
            if gateway_endpoint is not None
            else None
        )
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
        self._flight: FlightTransport | None = None
        if flight_endpoint is not None:
            try:
                self._flight = FlightTransport(
                    flight_endpoint,
                    tls_enabled=bool(tls_config.get("enabled", False)),
                    read_token=read_token,
                    writer_token=writer_token,
                    timeout_sec=float(flight_config.get("timeout_sec", 60.0) or 60.0),
                    configured_max_message_bytes=_int_config(
                        flight_config.get("max_message_bytes"), 2 * 1024 * 1024
                    ),
                    tls_root_certs=_read_optional_file(root_cert_file),
                    cert_chain=_read_optional_file(cert_chain_file),
                    private_key=_read_optional_file(private_key_file),
                    override_hostname=server_name,
                )
            except (ZippyConnectionError, ProtocolError, CapabilityError) as error:
                if self._gateway is None:
                    raise
                logger.warning(
                    "Flight discovery failed; using Gateway compatibility endpoint=[{}] error=[{}]",
                    flight_endpoint,
                    error,
                )
                flight_endpoint = None
        self.router = TransportRouter(
            self.control_endpoint,
            gateway_endpoint,
            shm_available,
            flight_endpoint,
            bool(self._flight and self._flight.capabilities.memory_ingest),
        )
        self.capabilities = _capabilities(
            self.config,
            gateway_config,
            flight_config,
            self._flight,
        )
        if (
            self.capabilities.gateway
            and self.capabilities.gateway_protocol_version != GATEWAY_PROTOCOL_VERSION
        ):
            raise ProtocolError(
                "unsupported Gateway protocol version "
                f"server=[{self.capabilities.gateway_protocol_version}] "
                f"client=[{GATEWAY_PROTOCOL_VERSION}]"
            )
        self._subscriptions: weakref.WeakSet[object] = weakref.WeakSet()
        self._closed = False
        if app is not None:
            self.control.ensure_registered(app)

    @property
    def gateway(self) -> GatewayTransport:
        """Return the discovered Gateway transport."""
        if self._gateway is None:
            raise TransportUnavailableError("server does not advertise a Gateway endpoint")
        return self._gateway

    @property
    def flight(self) -> FlightTransport:
        """Return the discovered Arrow Flight transport."""
        if self._flight is None:
            raise TransportUnavailableError("server does not advertise a usable Flight endpoint")
        return self._flight

    @property
    def query_transport(self) -> Transport:
        """Return the selected table-query transport."""
        requested = (
            Transport.AUTO if self.default_transport == Transport.SHM else self.default_transport
        )
        return self.router.select(Operation.QUERY, requested).selected

    def transport_for(
        self,
        operation: str | Operation,
        *,
        transport: str | Transport | None = None,
        source: str | None = None,
    ) -> TransportDecision:
        """
        Explain transport selection without starting the operation.

        :param operation: Operation name.
        :type operation: str | Operation
        :param transport: Optional per-operation transport override.
        :type transport: str | Transport | None
        :param source: Optional source used for SHM eligibility checks.
        :type source: str | None
        :returns: Transport selection decision.
        :rtype: TransportDecision
        """
        normalized_operation = (
            operation if isinstance(operation, Operation) else Operation(str(operation))
        )
        stream_info = self.control.get_stream(source) if source is not None else None
        return self.router.select(
            normalized_operation,
            transport or self.default_transport,
            stream_info=stream_info,
        )

    def list_tables(self) -> list[dict[str, object]]:
        """Return table and stream metadata visible through the control plane."""
        if self._gateway is not None:
            return self._gateway.list_streams()
        return self.control.list_streams(status=True)

    def read_table(
        self,
        source: str,
        *,
        wait: bool = False,
        timeout: float | str | None = None,
        snapshot: bool = True,
        namespace: str = "default",
    ) -> Table:
        """Open a lazy server-owned table query."""
        self._ensure_open()
        if wait:
            self._wait_for_source(source, timeout)
        return Table(source, self, snapshot=snapshot, namespace=namespace)

    def read_sql(
        self,
        sql: str,
        *,
        parameters: dict[str, object] | None = None,
        bindings: dict[str, object] | None = None,
        stream: bool = False,
        priority: str = "interactive",
        deadline_ns: int | None = None,
        memory_limit_bytes: int | None = None,
        spill_limit_bytes: int | None = 0,
        result_batch_rows: int | None = None,
    ):
        """
        Execute server-side SQL and return a PyArrow table.

        Flight requires explicit relation-to-catalog ``bindings`` and executes
        the SQL in the server-owned DataFusion service. A legacy Gateway may
        accept the original call without bindings when it advertises
        ``query_sql``. SQL is never executed in the client process.
        """
        self._ensure_open()
        if not self.capabilities.query_sql:
            raise CapabilityError(
                "connected server does not advertise query_sql; "
                "upgrade or enable the server-owned DataFusion query service"
            )
        decision = self.router.select(Operation.SQL, self.default_transport)
        if decision.selected in {Transport.FLIGHT_LOCAL, Transport.FLIGHT_REMOTE}:
            if parameters:
                raise CapabilityError(
                    "Flight protocol version 1 does not encode SQL parameters; "
                    "bind typed values in the Table expression API"
                )
            if not bindings:
                if self._gateway is not None and self._gateway_query_sql:
                    table, _ = self.gateway.read_sql(
                        sql,
                        parameters=parameters,
                        stream=stream,
                    )
                    return table
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
                stream=stream,
            )
            return table
        if not self._gateway_query_sql:
            raise CapabilityError(
                "selected Gateway endpoint does not advertise query_sql; "
                "use Flight with explicit bindings or upgrade the Gateway"
            )
        table, _ = self.gateway.read_sql(sql, parameters=parameters, stream=stream)
        return table

    def get_writer(
        self,
        stream_name: str,
        *,
        schema: object | None = None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
        durability: str | None = None,
        transport: str | Transport | None = None,
    ) -> GatewayWriter | FlightWriter:
        """Return a buffered Flight writer or Gateway compatibility writer."""
        self._ensure_open()
        decision = self.router.select(Operation.WRITE, transport or self.default_transport)
        if decision.selected in {Transport.FLIGHT_LOCAL, Transport.FLIGHT_REMOTE}:
            if schema is None:
                try:
                    schema = self.flight.get_table_schema(stream_name)
                except Exception as error:
                    logger.debug(
                        "failed to resolve writer schema stream=[{}] transport=[flight] error=[{}]",
                        stream_name,
                        error,
                    )
                    schema = None
            return FlightWriter(
                stream_name,
                self.flight,
                schema=schema,
                batch_size=batch_size,
                flush_interval_ms=flush_interval_ms,
                durability="wal" if durability is None else durability,
            )
        if durability is not None and str(durability).strip().lower() != "memory":
            raise CapabilityError("Gateway writers only support memory durability")
        if schema is None:
            try:
                _, schema = self.gateway.get_stream(stream_name)
            except Exception as error:
                logger.debug(
                    "failed to resolve writer schema stream=[{}] transport=[gateway] error=[{}]",
                    stream_name,
                    error,
                )
                schema = None
        return GatewayWriter(
            stream_name,
            self.gateway,
            schema=schema,
            batch_size=batch_size,
            flush_interval_ms=flush_interval_ms,
        )

    def subscribe(
        self,
        source: str,
        callback: Callable[[Any], Any],
        *,
        instrument_ids: list[str] | tuple[str, ...] | str | None = None,
        filter: object | None = None,
        wait: bool = False,
        timeout: float | str | None = None,
        start_from: int = -1,
        transport: str | Transport | None = None,
        reconnect: bool = True,
        reconnect_interval_ms: int = 100,
        allow_data_loss: bool = False,
        poll_interval_ms: int = 1,
        xfast: bool = False,
        idle_spin_checks: int = 64,
    ) -> GatewaySubscriber | ShmSubscriber:
        """Start one ordered row callback per committed stream row."""
        if filter is not None and instrument_ids is not None:
            raise ValueError("filter and instrument_ids cannot be used together")
        if start_from < -1:
            raise ValueError("start_from must be -1 or a non-negative row offset")
        _validate_subscriber_wait(poll_interval_ms, xfast, idle_spin_checks)
        if wait:
            self._wait_for_source(source, timeout)
        requested = transport or self.default_transport
        stream_info = self._subscription_stream_info(source, requested)
        allow_shm = self._allow_auto_shm and filter is None
        decision = self.router.select(
            Operation.SUBSCRIBE,
            requested,
            stream_info=stream_info,
            allow_shm=allow_shm,
        )
        if decision.selected == Transport.SHM:
            descriptor_poll_ms = 1 if xfast else poll_interval_ms
            subscriber: GatewaySubscriber | ShmSubscriber = ShmSubscriber(
                source,
                self.control,
                callback,
                instrument_ids=instrument_ids,
                start_from=start_from,
                descriptor_poll_ms=descriptor_poll_ms,
                xfast=xfast,
                idle_spin_checks=idle_spin_checks,
            )
        else:
            if start_from != -1:
                raise ValueError("Gateway subscribe only supports start_from=-1")
            remote_filter = self._normalized_subscription_filter(source, filter)
            subscriber = GatewaySubscriber(
                source,
                self.gateway,
                callback,
                filter=remote_filter,
                instrument_ids=instrument_ids,
                reconnect=reconnect,
                reconnect_interval_ms=reconnect_interval_ms,
                allow_data_loss=allow_data_loss,
                resume_cursor=self.capabilities.resume_cursor,
            )
        subscriber.start()
        self._subscriptions.add(subscriber)
        return subscriber

    def subscribe_table(
        self,
        source: str,
        callback: Callable[[Any], Any],
        *,
        filter: object | None = None,
        batch_size: int | None = None,
        throttle_ms: int | None = None,
        count: int | None = None,
        wait: bool = False,
        timeout: float | str | None = None,
        start_from: int = -1,
        transport: str | Transport | None = None,
        reconnect: bool = True,
        reconnect_interval_ms: int = 100,
        allow_data_loss: bool = False,
        poll_interval_ms: int = 10,
        xfast: bool = False,
        idle_spin_checks: int = 64,
    ) -> GatewaySubscriber | ShmSubscriber:
        """Start incremental PyArrow table callbacks."""
        if start_from < -1:
            raise ValueError("start_from must be -1 or a non-negative row offset")
        _validate_subscriber_wait(poll_interval_ms, xfast, idle_spin_checks)
        if wait:
            self._wait_for_source(source, timeout)
        requested = transport or self.default_transport
        stream_info = self._subscription_stream_info(source, requested)
        allow_shm = (
            self._allow_auto_shm and filter is None and batch_size is None and throttle_ms is None
        )
        decision = self.router.select(
            Operation.SUBSCRIBE,
            requested,
            stream_info=stream_info,
            allow_shm=allow_shm,
        )
        if decision.selected == Transport.SHM:
            descriptor_poll_ms = 1 if xfast else poll_interval_ms
            subscriber: GatewaySubscriber | ShmSubscriber = ShmSubscriber(
                source,
                self.control,
                callback,
                table_callback=True,
                count=count,
                start_from=start_from,
                descriptor_poll_ms=descriptor_poll_ms,
                xfast=xfast,
                idle_spin_checks=idle_spin_checks,
            )
        else:
            if start_from != -1:
                raise ValueError("Gateway subscribe_table only supports start_from=-1")
            remote_filter = self._normalized_subscription_filter(source, filter)
            subscriber = GatewaySubscriber(
                source,
                self.gateway,
                callback,
                table_callback=True,
                filter=remote_filter,
                batch_size=batch_size,
                throttle_ms=throttle_ms,
                count=count,
                reconnect=reconnect,
                reconnect_interval_ms=reconnect_interval_ms,
                allow_data_loss=allow_data_loss,
                resume_cursor=self.capabilities.resume_cursor,
            )
        subscriber.start()
        self._subscriptions.add(subscriber)
        return subscriber

    def close(self) -> None:
        """Stop active subscriptions and release the control process lease."""
        if self._closed:
            return
        self._closed = True
        global _DEFAULT_CLIENT
        with _DEFAULT_CLIENT_LOCK:
            if _DEFAULT_CLIENT is self:
                _DEFAULT_CLIENT = None
        first_error: BaseException | None = None
        for subscriber in list(self._subscriptions):
            stop = getattr(subscriber, "stop", None)
            if not callable(stop):
                continue
            try:
                stop()
            except BaseException as error:
                first_error = first_error or error
                logger.error("failed to stop client subscription error=[{}]", error)
        if self._flight is not None:
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

    def _normalized_subscription_filter(self, source: str, filter: object | None) -> object | None:
        if filter is None:
            return None
        _, schema = self.gateway.get_stream(source)
        return normalize_temporal_filter(filter, schema)

    def _subscription_stream_info(
        self,
        source: str,
        requested: str | Transport,
    ) -> dict[str, object] | None:
        requested_transport = Transport.parse(requested)
        if not self._allow_auto_shm and requested_transport == Transport.AUTO:
            return None
        if not self.control_endpoint.is_namespace_local:
            return None
        if requested_transport not in {Transport.AUTO, Transport.SHM}:
            return None
        try:
            return self.control.get_stream(source)
        except BaseException:
            if requested_transport == Transport.SHM or self._gateway is None:
                return {"data_path": "segment", "active_segment_descriptor": None}
            return None

    def _wait_for_source(self, source: str, timeout: float | str | None) -> None:
        timeout_sec = _parse_timeout(timeout)
        deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
        last_error: BaseException | None = None
        while True:
            try:
                stream = self.control.get_stream(source)
                if _source_ready(stream):
                    return
            except BaseException as error:
                last_error = error
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"table did not become ready source=[{source}] error=[{last_error}]"
                ) from last_error
            time.sleep(0.01)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Zippy client is closed")


def connect(
    uri: str | None = None,
    *,
    app: str | None = None,
    heartbeat_interval_sec: float = 3.0,
    transport: str | Transport = Transport.AUTO,
    local: bool | None = None,
    token: str | None = None,
    flight_token: str | None = None,
    flight_writer_token: str | None = None,
    flight_tls_root_cert_file: str | Path | None = None,
    flight_tls_cert_chain_file: str | Path | None = None,
    flight_tls_private_key_file: str | Path | None = None,
    flight_tls_server_name: str | None = None,
) -> Client:
    """
    Connect and set the process-wide default client.

    :param uri: Master URI.
    :type uri: str | None
    :param app: Optional process lease name.
    :type app: str | None
    :param heartbeat_interval_sec: Process heartbeat interval.
    :type heartbeat_interval_sec: float
    :param transport: Default transport selection.
    :type transport: str | Transport
    :param local: Compatibility flag. ``False`` disables SHM preference.
    :type local: bool | None
    :param token: Optional Gateway authentication token. It overrides
        ``ZIPPY_GATEWAY_TOKEN`` and server-advertised configuration.
    :type token: str | None
    :param flight_token: Optional Flight read bootstrap token.
    :type flight_token: str | None
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
    :returns: Connected default client.
    :rtype: Client
    """
    with _DEFAULT_CLIENT_CONNECT_LOCK:
        disable_auto_shm = local is False
        if local is not None:
            if Transport.parse(transport) != Transport.AUTO:
                raise ValueError("local and transport cannot be specified together")
            transport = Transport.AUTO
        connected = Client(
            uri,
            app=app,
            transport=transport,
            token=token,
            flight_token=flight_token,
            flight_writer_token=flight_writer_token,
            flight_tls_root_cert_file=flight_tls_root_cert_file,
            flight_tls_cert_chain_file=flight_tls_cert_chain_file,
            flight_tls_private_key_file=flight_tls_private_key_file,
            flight_tls_server_name=flight_tls_server_name,
            heartbeat_interval_sec=heartbeat_interval_sec,
        )
        if disable_auto_shm:
            connected._allow_auto_shm = False
        global _DEFAULT_CLIENT
        with _DEFAULT_CLIENT_LOCK:
            previous, _DEFAULT_CLIENT = _DEFAULT_CLIENT, connected
        if previous is not None and previous is not connected:
            previous.close()
        return connected


def client() -> Client:
    """Return the process-wide default client."""
    with _DEFAULT_CLIENT_LOCK:
        if _DEFAULT_CLIENT is None:
            raise RuntimeError("zippy client is not connected; call zp.connect() first")
        return _DEFAULT_CLIENT


def master() -> Client:
    """Compatibility alias returning the process-wide default client."""
    return client()


def read_table(
    source: str,
    master: Client | None = None,
    *,
    wait: bool = False,
    timeout: float | str | None = None,
    snapshot: bool = True,
    namespace: str = "default",
) -> Table:
    """Open a lazy table query using the process-wide client.

    ``master`` is retained only for source compatibility with the full package.
    New code should call :func:`connect` once and omit it.
    """
    return (master or client()).read_table(
        source,
        wait=wait,
        timeout=timeout,
        snapshot=snapshot,
        namespace=namespace,
    )


def read_sql(
    sql: str,
    master: Client | None = None,
    *,
    parameters: dict[str, object] | None = None,
    bindings: dict[str, object] | None = None,
    stream: bool = False,
    priority: str = "interactive",
    deadline_ns: int | None = None,
    memory_limit_bytes: int | None = None,
    spill_limit_bytes: int | None = 0,
    result_batch_rows: int | None = None,
):
    """Execute server-owned SQL using the process-wide client.

    ``master`` is retained only for source compatibility with the full package.
    New code should call :func:`connect` once and omit it.
    """
    return (master or client()).read_sql(
        sql,
        parameters=parameters,
        bindings=bindings,
        stream=stream,
        priority=priority,
        deadline_ns=deadline_ns,
        memory_limit_bytes=memory_limit_bytes,
        spill_limit_bytes=spill_limit_bytes,
        result_batch_rows=result_batch_rows,
    )


def get_writer(
    stream_name: str,
    *,
    master: Client | None = None,
    schema: object | None = None,
    batch_size: int = 1024,
    flush_interval_ms: int | None = 5,
    durability: str | None = None,
    transport: str | Transport | None = None,
) -> GatewayWriter | FlightWriter:
    """Return a buffered writer using the process-wide client.

    ``master`` is retained only for source compatibility with the full package.
    New code should call :func:`connect` once and omit it.
    """
    return (master or client()).get_writer(
        stream_name,
        schema=schema,
        batch_size=batch_size,
        flush_interval_ms=flush_interval_ms,
        durability=durability,
        transport=transport,
    )


def subscribe(
    source: str,
    callback: Callable[[Any], Any],
    master: Client | None = None,
    **kwargs: Any,
) -> GatewaySubscriber | ShmSubscriber:
    """Start row callbacks using the process-wide client.

    ``master`` is retained only for source compatibility with the full package.
    New code should call :func:`connect` once and omit it.
    """
    return (master or client()).subscribe(source, callback, **kwargs)


def subscribe_table(
    source: str,
    callback: Callable[[Any], Any],
    master: Client | None = None,
    **kwargs: Any,
) -> GatewaySubscriber | ShmSubscriber:
    """Start Arrow table callbacks using the process-wide client.

    ``master`` is retained only for source compatibility with the full package.
    New code should call :func:`connect` once and omit it.
    """
    return (master or client()).subscribe_table(source, callback, **kwargs)


def _capabilities(
    config: dict[str, object],
    gateway_config: dict[str, object],
    flight_config: dict[str, object] | None = None,
    flight: FlightTransport | None = None,
) -> ServerCapabilities:
    flight_config = flight_config or {}
    return ServerCapabilities(
        flight=flight is not None,
        flight_protocol_version=(
            flight.capabilities.protocol_version
            if flight is not None
            else _int_config(flight_config.get("protocol_version"), 1)
        ),
        flight_ingest=bool(flight and flight.capabilities.memory_ingest),
        gateway=bool(gateway_config.get("endpoint")) and gateway_config.get("enabled") is not False,
        gateway_protocol_version=_int_config(gateway_config.get("protocol_version"), 1),
        query_sql=bool(flight) or _gateway_query_sql_enabled(config),
        resume_cursor=bool(_capability_config(config).get("subscribe_resume_cursor", False)),
    )


def _gateway_query_sql_enabled(config: dict[str, object]) -> bool:
    return bool(_capability_config(config).get("query_sql", False))


def _capability_config(config: dict[str, object]) -> dict[str, object]:
    capability_config = config.get("capabilities", {})
    if not isinstance(capability_config, dict):
        capability_config = {}
    server_config = config.get("server", {})
    if isinstance(server_config, dict) and isinstance(server_config.get("capabilities"), dict):
        capability_config = {**capability_config, **server_config["capabilities"]}
    return capability_config


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


def _validate_subscriber_wait(
    poll_interval_ms: int,
    xfast: bool,
    idle_spin_checks: int,
) -> None:
    if poll_interval_ms < 0 or (poll_interval_ms == 0 and not xfast):
        raise ValueError("poll_interval_ms must be positive unless xfast=True")
    if idle_spin_checks < 0 or idle_spin_checks > 2**32 - 1:
        raise ValueError("idle_spin_checks must be between 0 and 4294967295")


def _source_ready(stream: dict[str, object]) -> bool:
    if stream.get("data_path") != "segment":
        return True
    preflight = stream.get("active_segment_preflight")
    preflight_ready = not isinstance(preflight, dict) or (
        preflight.get("status") == "ok" and preflight.get("readable") is not False
    )
    return (
        stream.get("status") != "stale"
        and isinstance(stream.get("active_segment_descriptor"), dict)
        and preflight_ready
    )


def _close_default_client() -> None:
    global _DEFAULT_CLIENT
    with _DEFAULT_CLIENT_LOCK:
        active, _DEFAULT_CLIENT = _DEFAULT_CLIENT, None
    if active is None:
        return
    try:
        active.close()
    except BaseException as error:
        logger.error("failed to close default Zippy client error=[{}]", error)


atexit.register(_close_default_client)
