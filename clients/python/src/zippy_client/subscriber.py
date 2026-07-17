"""Gateway and SHM subscription handles with one health contract."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
import json
import socket
import threading
import time
from typing import Any

from loguru import logger

from .control import ControlClient
from .errors import ContinuityError, ProtocolError, TransportUnavailableError
from .expressions import expression_to_json
from .protocol import receive_frame, table_from_ipc
from .transports.gateway import GatewayTransport

try:
    from . import _shm as _shm_module

    _NativeShmReader: Any = _shm_module.ShmReader
    _SHM_IMPORT_ERROR: BaseException | None = None
except ImportError as error:
    _NativeShmReader = None
    _SHM_IMPORT_ERROR = error


class Row(Mapping[str, object]):
    """
    Represent one row delivered by a Zippy subscription.

    :param values: Row values keyed by column name.
    :type values: dict[str, object]
    """

    __slots__ = ("_values",)

    def __init__(self, values: dict[str, object]) -> None:
        self._values = values if type(values) is dict else dict(values)

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"zippy_client.Row({self._values!r})"

    def to_dict(self) -> dict[str, object]:
        """Return a shallow dictionary copy."""
        return dict(self._values)


def shm_available() -> bool:
    """Return whether the platform-specific SHM reader extension loaded."""
    return _NativeShmReader is not None


def shm_import_error() -> BaseException | None:
    """Return the native SHM import failure, if any."""
    return _SHM_IMPORT_ERROR


class GatewaySubscriber:
    """Persistent Gateway subscription handle."""

    def __init__(
        self,
        source: str,
        gateway: GatewayTransport,
        callback: Callable[[Any], Any],
        *,
        table_callback: bool = False,
        filter: object | None = None,
        instrument_ids: list[str] | tuple[str, ...] | str | None = None,
        batch_size: int | None = None,
        throttle_ms: int | None = None,
        count: int | None = None,
        reconnect: bool = True,
        reconnect_interval_ms: int = 100,
        allow_data_loss: bool = False,
        resume_cursor: bool = False,
    ) -> None:
        _validate_subscription_options(
            table_callback=table_callback,
            instrument_ids=instrument_ids,
            batch_size=batch_size,
            throttle_ms=throttle_ms,
            count=count,
        )
        if reconnect_interval_ms <= 0:
            raise ValueError("reconnect_interval_ms must be positive")
        self.source = str(source)
        self.gateway = gateway
        self.callback = callback
        self.table_callback = bool(table_callback)
        self.filter = filter
        self.instrument_ids = _normalize_instrument_ids(instrument_ids)
        self.batch_size = _positive_optional_int("batch_size", batch_size)
        self.throttle_ms = _positive_optional_int("throttle_ms", throttle_ms)
        self.count = _positive_optional_int("count", count)
        self.reconnect = bool(reconnect)
        self.reconnect_interval_ms = int(reconnect_interval_ms)
        self.allow_data_loss = bool(allow_data_loss)
        self.resume_cursor = bool(resume_cursor)
        self._stop_event = threading.Event()
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._state = "created"
        self._error: BaseException | None = None
        self._reconnects_total = 0
        self._rows_delivered_total = 0
        self._batches_delivered_total = 0
        self._last_event_ns = 0
        self._last_success_ns = 0
        self._continuity_lost = False

    def start(self) -> GatewaySubscriber:
        """Start subscription delivery on a daemon thread."""
        if self._started:
            raise RuntimeError("Gateway subscriber is already started")
        self._started = True
        self._state = "starting"
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-client-gateway-{self.source}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop delivery and raise any terminal subscriber error."""
        self._stop_event.set()
        self._close_socket()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        if self._error is not None:
            raise self._error

    def join(self, timeout: float | None = None) -> None:
        """Wait for delivery to stop and raise any terminal error."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._error is not None:
            raise self._error

    def status(self) -> str:
        """Return the current lifecycle state."""
        return "failed" if self._error is not None else self._state

    def metrics(self) -> dict[str, object]:
        """Return subscription delivery and continuity counters."""
        return {
            "source": self.source,
            "transport": "gateway",
            "gateway_endpoint": self.gateway.endpoint,
            "state": self.status(),
            "running": self.status() in {"starting", "running", "reconnecting"},
            "last_error": None if self._error is None else str(self._error),
            "reconnects_total": self._reconnects_total,
            "rows_delivered_total": self._rows_delivered_total,
            "batches_delivered_total": self._batches_delivered_total,
            "last_event_ns": self._last_event_ns,
            "last_success_ns": self._last_success_ns,
            "continuity_lost": self._continuity_lost,
        }

    def health(self) -> dict[str, object]:
        """Return normalized subscription health."""
        return _health("gateway_subscriber", self.metrics())

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
                if not self._stop_event.is_set():
                    self._state = "stopped"
                return
            except BaseException as error:
                if self._stop_event.is_set():
                    self._state = "stopped"
                    return
                if not self._retryable(error) or not self.reconnect:
                    self._error = error
                    self._state = "failed"
                    return
                if self._rows_delivered_total > 0 and not self.resume_cursor:
                    self._continuity_lost = True
                    if not self.allow_data_loss:
                        self._error = ContinuityError(
                            "Gateway reconnect cannot prove gap-free continuation because "
                            "the server does not advertise resume cursors"
                        )
                        self._state = "failed"
                        return
                self._reconnects_total += 1
                self._state = "reconnecting"
                logger.warning(
                    "gateway subscription reconnecting source=[{}] reconnects_total=[{}] "
                    "error=[{}]",
                    self.source,
                    self._reconnects_total,
                    error,
                )
                self._stop_event.wait(self.reconnect_interval_ms / 1000.0)

    def _run_once(self) -> None:
        request: dict[str, object] = {
            "kind": "subscribe_table" if self.table_callback else "subscribe_rows",
            "source": self.source,
            "filter": expression_to_json(self.filter) if self.filter is not None else None,
            "instrument_ids": self.instrument_ids,
            "batch_size": self.batch_size,
            "throttle_ms": self.throttle_ms,
            "count": self.count,
        }
        with self.gateway.open_subscription(request) as sock:
            self._socket = sock
            while not self._stop_event.is_set():
                header, payload = receive_frame(sock)
                if header.get("status") != "ok":
                    raise RuntimeError(str(header.get("reason", "Gateway subscribe failed")))
                kind = header.get("kind")
                if kind == "subscribed":
                    self._state = "running"
                    continue
                if kind == "row" and not self.table_callback:
                    row = header.get("row")
                    if not isinstance(row, dict):
                        raise ProtocolError("Gateway row frame is missing a row object")
                    self._deliver_row(row)
                    continue
                if kind != "table":
                    continue
                table = table_from_ipc(payload)
                if self.table_callback:
                    self.callback(table)
                    self._record_delivery(int(table.num_rows), batch=True)
                else:
                    for row in _table_rows(table):
                        self._deliver_row(row)
        self._socket = None

    def _deliver_row(self, values: dict[str, object]) -> None:
        self.callback(Row(values))
        self._record_delivery(1, batch=False)

    def _record_delivery(self, rows: int, *, batch: bool) -> None:
        self._rows_delivered_total += rows
        if batch:
            self._batches_delivered_total += 1
        now = time.time_ns()
        self._last_event_ns = now
        self._last_success_ns = now

    def _close_socket(self) -> None:
        sock, self._socket = self._socket, None
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass

    @staticmethod
    def _retryable(error: BaseException) -> bool:
        message = str(error).lower()
        return isinstance(error, (OSError, ProtocolError)) or any(
            token in message
            for token in (
                "connection closed",
                "stream is stale",
                "descriptor is not published",
                "stream not found",
                "stream not registered",
            )
        )


class ShmSubscriber:
    """Read-only local SHM subscription with Master-owned segment leases."""

    def __init__(
        self,
        source: str,
        control: ControlClient,
        callback: Callable[[Any], Any],
        *,
        table_callback: bool = False,
        instrument_ids: list[str] | tuple[str, ...] | str | None = None,
        count: int | None = None,
        start_from: int = -1,
        descriptor_poll_ms: int = 50,
        xfast: bool = False,
        idle_spin_checks: int = 64,
    ) -> None:
        if _NativeShmReader is None:
            raise TransportUnavailableError(
                f"zippy_client._shm is unavailable error=[{_SHM_IMPORT_ERROR}]"
            )
        if start_from < -1:
            raise ValueError("start_from must be -1 or a non-negative row offset")
        if descriptor_poll_ms <= 0:
            raise ValueError("descriptor_poll_ms must be positive")
        if idle_spin_checks < 0 or idle_spin_checks > 2**32 - 1:
            raise ValueError("idle_spin_checks must be between 0 and 4294967295")
        self.source = str(source)
        self.control = control
        self.callback = callback
        self.table_callback = bool(table_callback)
        self.instrument_ids = set(_normalize_instrument_ids(instrument_ids) or [])
        self.count = _positive_optional_int("count", count)
        self.start_from = int(start_from)
        self.descriptor_poll_ms = int(descriptor_poll_ms)
        self.xfast = bool(xfast)
        self.idle_spin_checks = int(idle_spin_checks)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._state = "created"
        self._error: BaseException | None = None
        self._reader: Any = None
        self._lease_id: str | None = None
        self._pending_lease_id: str | None = None
        self._descriptor: dict[str, object] | None = None
        self._schema: dict[str, object] | None = None
        self._descriptor_generation = 0
        self._rows_delivered_total = 0
        self._batches_delivered_total = 0
        self._descriptor_rebinds_total = 0
        self._last_event_ns = 0
        self._last_success_ns = 0
        self._last_descriptor_poll_ns = 0

    def start(self) -> ShmSubscriber:
        """Start local SHM delivery on a daemon thread."""
        if self._started:
            raise RuntimeError("SHM subscriber is already started")
        self.control.ensure_registered()
        self._started = True
        self._state = "starting"
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-client-shm-{self.source}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        """Stop delivery, release leases, and raise any terminal error."""
        self._stop_event.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2.0)
        self._release_all_leases()
        if self._error is not None:
            raise self._error

    def join(self, timeout: float | None = None) -> None:
        """Wait for delivery to stop and raise any terminal error."""
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        if self._error is not None:
            raise self._error

    def status(self) -> str:
        """Return the current lifecycle state."""
        return "failed" if self._error is not None else self._state

    def metrics(self) -> dict[str, object]:
        """Return SHM delivery and descriptor counters."""
        return {
            "source": self.source,
            "transport": "shm",
            "state": self.status(),
            "running": self.status() in {"starting", "running"},
            "last_error": None if self._error is None else str(self._error),
            "rows_delivered_total": self._rows_delivered_total,
            "batches_delivered_total": self._batches_delivered_total,
            "descriptor_rebinds_total": self._descriptor_rebinds_total,
            "current_descriptor_generation": self._descriptor_generation,
            "xfast": self.xfast,
            "idle_spin_checks": self.idle_spin_checks,
            "last_event_ns": self._last_event_ns,
            "last_success_ns": self._last_success_ns,
        }

    def health(self) -> dict[str, object]:
        """Return normalized SHM subscription health."""
        return _health("shm_subscriber", self.metrics())

    def _run(self) -> None:
        descriptor_missing_at_start = False
        try:
            while not self._stop_event.is_set() and self._reader is None:
                try:
                    stream = self.control.get_stream(self.source)
                except BaseException as error:
                    descriptor_missing_at_start = True
                    logger.debug(
                        "waiting for SHM stream source=[{}] error=[{}]",
                        self.source,
                        error,
                    )
                    self._stop_event.wait(self.descriptor_poll_ms / 1000.0)
                    continue
                if stream.get("data_path") != "segment":
                    raise TransportUnavailableError(
                        "stream does not expose the segment SHM data path "
                        f"source=[{self.source}] data_path=[{stream.get('data_path')}]"
                    )
                descriptor = stream.get("active_segment_descriptor")
                if not isinstance(descriptor, dict):
                    descriptor_missing_at_start = True
                    self._stop_event.wait(self.descriptor_poll_ms / 1000.0)
                    continue
                effective_start = 0 if descriptor_missing_at_start else self.start_from
                self._attach_initial(stream, descriptor, effective_start)
            if self._reader is None:
                self._state = "stopped"
                return
            self._state = "running"
            while not self._stop_event.is_set():
                batch = self._reader.read_available()
                if batch is not None:
                    self._deliver_batch(batch)
                    if self.count is not None and self._rows_delivered_total >= self.count:
                        self._state = "stopped"
                        return
                    continue
                observed = self._reader.notification_sequence()
                if self.xfast:
                    self._reader.spin_for_notification_after(
                        observed,
                        max(1, self.idle_spin_checks),
                    )
                else:
                    self._reader.wait_for_notification_after(
                        observed,
                        self.descriptor_poll_ms,
                        self.idle_spin_checks,
                    )
                now_ns = time.monotonic_ns()
                periodic_poll_due = now_ns - self._last_descriptor_poll_ns >= 50_000_000
                if self._reader.is_sealed() or periodic_poll_due:
                    self._refresh_descriptor()
                    self._last_descriptor_poll_ns = now_ns
            self._state = "stopped"
        except BaseException as error:
            if not self._stop_event.is_set():
                self._error = error
                self._state = "failed"
        finally:
            self._release_all_leases()

    def _attach_initial(
        self,
        stream: dict[str, object],
        descriptor: dict[str, object],
        start_from: int,
    ) -> None:
        schema = stream.get("schema")
        if not isinstance(schema, dict):
            raise ProtocolError(f"stream schema is missing source=[{self.source}]")
        segment_id, generation = _descriptor_identity(descriptor)
        lease_id = self.control.acquire_segment_reader_lease(self.source, segment_id, generation)
        try:
            reader = _NativeShmReader(
                json.dumps(descriptor, separators=(",", ":")),
                json.dumps(schema, separators=(",", ":")),
                start_from,
            )
        except BaseException:
            self.control.release_segment_reader_lease(self.source, lease_id)
            raise
        self._reader = reader
        self._lease_id = lease_id
        self._descriptor = descriptor
        self._schema = schema
        self._descriptor_generation = _int_value(stream.get("descriptor_generation"))

    def _refresh_descriptor(self) -> None:
        stream = self.control.get_stream(self.source, status=False)
        descriptor = _next_retained_descriptor(stream, self._descriptor)
        if descriptor is None:
            return
        schema = stream.get("schema")
        if schema != self._schema:
            raise ContinuityError(f"SHM schema changed during subscription source=[{self.source}]")
        if self._pending_lease_id is None:
            segment_id, generation = _descriptor_identity(descriptor)
            self._pending_lease_id = self.control.acquire_segment_reader_lease(
                self.source, segment_id, generation
            )
        rebound, pending_batch = self._reader.try_rebind(
            json.dumps(descriptor, separators=(",", ":"))
        )
        if pending_batch is not None:
            self._deliver_batch(pending_batch)
        if not rebound:
            return
        old_lease = self._lease_id
        self._lease_id = self._pending_lease_id
        self._pending_lease_id = None
        self._descriptor = descriptor
        self._descriptor_generation = _int_value(stream.get("descriptor_generation"))
        self._descriptor_rebinds_total += 1
        if old_lease is not None:
            self.control.release_segment_reader_lease(self.source, old_lease)

    def _deliver_batch(self, batch: object) -> None:
        import pyarrow as pa

        table = pa.Table.from_batches([batch])
        remaining = None
        if self.count is not None:
            remaining = self.count - self._rows_delivered_total
            if remaining <= 0:
                return
            if table.num_rows > remaining:
                table = table.slice(0, remaining)
        if self.table_callback:
            self.callback(table)
            delivered = int(table.num_rows)
            self._batches_delivered_total += 1
        else:
            delivered = 0
            for values in _table_rows(table):
                if (
                    self.instrument_ids
                    and str(values.get("instrument_id")) not in self.instrument_ids
                ):
                    continue
                self.callback(Row(values))
                delivered += 1
        self._rows_delivered_total += delivered
        if delivered:
            now = time.time_ns()
            self._last_event_ns = now
            self._last_success_ns = now

    def _release_all_leases(self) -> None:
        for attribute in ("_pending_lease_id", "_lease_id"):
            lease_id = getattr(self, attribute, None)
            setattr(self, attribute, None)
            if lease_id is None:
                continue
            try:
                self.control.release_segment_reader_lease(self.source, lease_id)
            except BaseException as error:
                logger.warning(
                    "failed to release SHM reader lease source=[{}] lease_id=[{}] error=[{}]",
                    self.source,
                    lease_id,
                    error,
                )


def _descriptor_identity(descriptor: dict[str, object]) -> tuple[int, int]:
    try:
        return _int_value(descriptor["segment_id"]), _int_value(descriptor["generation"])
    except (KeyError, TypeError, ValueError) as error:
        raise ProtocolError("SHM descriptor is missing segment identity") from error


def _next_retained_descriptor(
    stream: dict[str, object],
    current: dict[str, object] | None,
) -> dict[str, object] | None:
    """Return the immediate retained successor without skipping sealed segments."""
    if current is None:
        return None
    active = stream.get("active_segment_descriptor")
    sealed = stream.get("sealed_segments", [])
    if not isinstance(sealed, list):
        raise ProtocolError(
            f"stream sealed_segments must be a list source=[{stream.get('stream_name')}]"
        )
    candidates: list[dict[str, object]] = []
    raw_candidates = [*sealed, active] if isinstance(active, dict) else list(sealed)
    for descriptor in raw_candidates:
        if not isinstance(descriptor, dict):
            raise ProtocolError(
                f"stream contains an invalid retained descriptor source=[{stream.get('stream_name')}]"
            )
        if not candidates or _descriptor_continuity_identity(candidates[-1]) != (
            _descriptor_continuity_identity(descriptor)
        ):
            candidates.append(descriptor)
    current_identity = _descriptor_continuity_identity(current)
    for index, descriptor in enumerate(candidates):
        if _descriptor_continuity_identity(descriptor) != current_identity:
            continue
        return candidates[index + 1] if index + 1 < len(candidates) else None
    retained_identities = [_descriptor_continuity_identity(item) for item in candidates]
    raise ContinuityError(
        "SHM descriptor continuity cannot be proven because the current segment is no longer "
        f"retained current=[{current_identity}] retained=[{retained_identities}]"
    )


def _descriptor_continuity_identity(
    descriptor: dict[str, object],
) -> tuple[int, int, int | None]:
    segment_id, generation = _descriptor_identity(descriptor)
    writer_epoch_value = descriptor.get("writer_epoch")
    writer_epoch = None if writer_epoch_value is None else _int_value(writer_epoch_value)
    return segment_id, generation, writer_epoch


def _normalize_instrument_ids(
    value: list[str] | tuple[str, ...] | str | None,
) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def _positive_optional_int(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _int_value(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError(f"value must be an integer value=[{value}]")
    return int(value)


def _validate_subscription_options(
    *,
    table_callback: bool,
    instrument_ids: object,
    batch_size: int | None,
    throttle_ms: int | None,
    count: int | None,
) -> None:
    if table_callback and instrument_ids is not None:
        raise ValueError("instrument_ids is only supported by row callbacks")
    if not table_callback:
        for name, value in (
            ("batch_size", batch_size),
            ("throttle_ms", throttle_ms),
            ("count", count),
        ):
            if value is not None:
                raise ValueError(f"{name} is only supported by subscribe_table")


def _health(kind: str, metrics: dict[str, object]) -> dict[str, object]:
    state = str(metrics.get("state", "unknown"))
    last_error = metrics.get("last_error")
    running = bool(metrics.get("running", False))
    return {
        **metrics,
        "kind": kind,
        "status": state,
        "healthy": running and state == "running" and not last_error,
        "terminal": state in {"failed", "stopped"},
        "restart_required": state == "failed" or bool(last_error),
    }


def _table_rows(table: Any) -> Iterator[dict[str, object]]:
    import pyarrow as pa

    fields = list(table.schema)
    columns = [table.column(index) for index in range(table.num_columns)]
    for row_index in range(table.num_rows):
        values: dict[str, object] = {}
        for field, column in zip(fields, columns, strict=True):
            scalar = column[row_index]
            if not scalar.is_valid:
                values[field.name] = None
            elif pa.types.is_timestamp(field.type):
                values[field.name] = int(scalar.value)
            else:
                values[field.name] = scalar.as_py()
        yield values
