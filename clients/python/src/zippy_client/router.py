"""Operation-aware transport selection."""

from __future__ import annotations

from collections.abc import Callable

from .endpoints import ControlEndpoint, gateway_is_local
from .errors import TransportUnavailableError
from .models import Operation, Transport, TransportDecision


class TransportRouter:
    """
    Select SHM or Gateway without changing operation semantics.

    :param control_endpoint: Resolved Master endpoint.
    :type control_endpoint: ControlEndpoint
    :param gateway_endpoint: Advertised connectable Gateway endpoint.
    :type gateway_endpoint: str | None
    :param shm_available: Callback reporting native SHM module availability.
    :type shm_available: Callable[[], bool]
    """

    def __init__(
        self,
        control_endpoint: ControlEndpoint,
        gateway_endpoint: str | None,
        shm_available: Callable[[], bool],
    ) -> None:
        self.control_endpoint = control_endpoint
        self.gateway_endpoint = gateway_endpoint
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
        if operation == Operation.SUBSCRIBE and allow_shm and self._can_shm(stream_info):
            return TransportDecision(
                requested_transport,
                Transport.SHM,
                "same IPC namespace and compatible SHM reader are available",
            )
        selected = self._gateway_transport()
        reason = (
            "query, SQL, and writes are server-owned Gateway operations"
            if operation != Operation.SUBSCRIBE
            else "SHM is unavailable, so subscription uses Gateway"
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
