"""Validate native Flight availability and requested endpoint locality."""

from __future__ import annotations

from .endpoints import ControlEndpoint, data_endpoint_is_local
from .errors import TransportUnavailableError
from .models import Operation, Transport, TransportDecision


class TransportRouter:
    """Select the single native data transport.

    :param control_endpoint: Server discovery endpoint.
    :param flight_endpoint: Advertised connectable Flight endpoint.
    :param flight_ingest_available: Whether authenticated native ingest is advertised.
    """

    def __init__(
        self,
        control_endpoint: ControlEndpoint,
        flight_endpoint: str | None = None,
        flight_ingest_available: bool = False,
    ) -> None:
        self.control_endpoint = control_endpoint
        self.flight_endpoint = flight_endpoint
        self.flight_ingest_available = flight_ingest_available

    def select(self, operation: Operation, requested: str | Transport) -> TransportDecision:
        """Validate one operation and return its Flight locality.

        :param operation: Query, SQL, subscription or write.
        :param requested: Automatic selection or explicit Flight locality.
        :returns: Selected endpoint scope and native authority reason.
        :raises TransportUnavailableError: Endpoint, ingest capability or locality is unavailable.
        :raises ValueError: The transport name has been removed or is invalid.
        """
        requested_transport = Transport.parse(requested)
        if self.flight_endpoint is None:
            raise TransportUnavailableError("server does not advertise an Arrow Flight endpoint")
        if operation == Operation.WRITE and not self.flight_ingest_available:
            raise TransportUnavailableError(
                "Flight endpoint does not advertise authenticated ingest"
            )
        selected = (
            Transport.FLIGHT_LOCAL
            if data_endpoint_is_local(self.flight_endpoint, self.control_endpoint)
            else Transport.FLIGHT_REMOTE
        )
        if requested_transport != Transport.AUTO and requested_transport != selected:
            raise TransportUnavailableError("requested Flight scope does not match endpoint")
        return TransportDecision(requested_transport, selected, "native Flight authority")
