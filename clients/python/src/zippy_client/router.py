"""Operation-aware transport selection."""

from __future__ import annotations

from collections.abc import Callable

from .endpoints import ControlEndpoint, data_endpoint_is_local, gateway_is_local
from .errors import TransportUnavailableError
from .models import Operation, Transport, TransportDecision


class TransportRouter:
    """
    Select SHM, Flight, or Gateway without changing operation semantics.

    :param control_endpoint: Resolved Master endpoint.
    :type control_endpoint: ControlEndpoint
    :param gateway_endpoint: Advertised connectable Gateway endpoint.
    :type gateway_endpoint: str | None
    :param shm_available: Callback reporting native SHM module availability.
    :type shm_available: Callable[[], bool]
    :param flight_endpoint: Advertised connectable Flight endpoint.
    :type flight_endpoint: str | None
    :param flight_ingest_available: Whether Flight append + memory ingest is usable.
    :type flight_ingest_available: bool
    """

    def __init__(
        self,
        control_endpoint: ControlEndpoint,
        gateway_endpoint: str | None,
        shm_available: Callable[[], bool],
        flight_endpoint: str | None = None,
        flight_ingest_available: bool = False,
    ) -> None:
        self.control_endpoint = control_endpoint
        self.gateway_endpoint = gateway_endpoint
        self.flight_endpoint = flight_endpoint
        self.flight_ingest_available = bool(flight_ingest_available)
        self._shm_available = shm_available

    def select(
        self,
        operation: Operation,
        requested: str | Transport,
        *,
        stream_info: dict[str, object] | None = None,
        allow_shm: bool = True,
    ) -> TransportDecision:
        """Select one transport for an operation and explain the decision."""
        requested_transport = Transport.parse(requested)
        if requested_transport == Transport.SHM:
            if not allow_shm:
                raise TransportUnavailableError(
                    "this subscription option set requires Gateway execution"
                )
            self._require_shm(operation, stream_info)
            return TransportDecision(requested_transport, Transport.SHM, "explicit SHM transport")
        if requested_transport in {Transport.GATEWAY_LOCAL, Transport.GATEWAY_REMOTE}:
            selected = self._gateway_transport()
            if selected != requested_transport:
                raise TransportUnavailableError(
                    "requested Gateway scope does not match discovered endpoint "
                    f"requested=[{requested_transport.value}] discovered=[{selected.value}]"
                )
            return TransportDecision(requested_transport, selected, "explicit Gateway transport")
        if requested_transport in {Transport.FLIGHT_LOCAL, Transport.FLIGHT_REMOTE}:
            if operation == Operation.SUBSCRIBE:
                raise TransportUnavailableError(
                    "Flight subscription requires Phase 7 DoExchange and is not yet available"
                )
            if operation == Operation.WRITE and not self.flight_ingest_available:
                raise TransportUnavailableError(
                    "Flight endpoint does not advertise authenticated append + memory ingest"
                )
            selected = self._flight_transport()
            if selected != requested_transport:
                raise TransportUnavailableError(
                    "requested Flight scope does not match discovered endpoint "
                    f"requested=[{requested_transport.value}] discovered=[{selected.value}]"
                )
            return TransportDecision(requested_transport, selected, "explicit Flight transport")
        if operation == Operation.SUBSCRIBE and allow_shm and self._can_shm(stream_info):
            return TransportDecision(
                requested_transport,
                Transport.SHM,
                "same IPC namespace and compatible SHM reader are available",
            )
        flight_supports_operation = operation != Operation.WRITE or self.flight_ingest_available
        if (
            operation != Operation.SUBSCRIBE
            and self.flight_endpoint is not None
            and flight_supports_operation
        ):
            selected = self._flight_transport()
            reason = "query, SQL, and writes prefer the standard Flight endpoint"
            return TransportDecision(requested_transport, selected, reason)
        selected = self._gateway_transport()
        reason = (
            "Flight is unavailable for this operation, so it uses Gateway compatibility"
            if operation != Operation.SUBSCRIBE
            else "SHM is unavailable, so subscription uses Gateway compatibility"
        )
        return TransportDecision(requested_transport, selected, reason)

    def _can_shm(self, stream_info: dict[str, object] | None) -> bool:
        if not self.control_endpoint.is_namespace_local or not self._shm_available():
            return False
        if stream_info is None or stream_info.get("data_path") != "segment":
            return False
        descriptor = stream_info.get("active_segment_descriptor")
        return descriptor is None or isinstance(descriptor, dict)

    def _require_shm(
        self,
        operation: Operation,
        stream_info: dict[str, object] | None,
    ) -> None:
        if operation != Operation.SUBSCRIBE:
            raise TransportUnavailableError(
                f"SHM transport only supports live subscriptions operation=[{operation.value}]"
            )
        if not self.control_endpoint.is_namespace_local:
            raise TransportUnavailableError(
                "SHM requires a Unix control endpoint in the same IPC namespace"
            )
        if not self._shm_available():
            raise TransportUnavailableError("zippy_client._shm is unavailable on this platform")
        if stream_info is not None and stream_info.get("data_path") != "segment":
            raise TransportUnavailableError(
                "stream does not expose the segment SHM data path "
                f"data_path=[{stream_info.get('data_path')}]"
            )

    def _gateway_transport(self) -> Transport:
        if self.gateway_endpoint is None:
            raise TransportUnavailableError("server does not advertise a Gateway endpoint")
        if gateway_is_local(self.gateway_endpoint, self.control_endpoint):
            return Transport.GATEWAY_LOCAL
        return Transport.GATEWAY_REMOTE

    def _flight_transport(self) -> Transport:
        if self.flight_endpoint is None:
            raise TransportUnavailableError("server does not advertise an Arrow Flight endpoint")
        if data_endpoint_is_local(self.flight_endpoint, self.control_endpoint):
            return Transport.FLIGHT_LOCAL
        return Transport.FLIGHT_REMOTE
