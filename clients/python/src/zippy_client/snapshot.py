"""Session-owned native snapshot scopes for repeatable multi-table queries.

:example: with client.open_snapshot([("market", "ticks")]) as snapshot: ...
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from .query import Table, _SnapshotLease

if TYPE_CHECKING:
    from .client import Client


class FixedSnapshot:
    """Own one bounded native snapshot shared by all tables opened from this scope.

    :param client: Connected client owning the authenticated read session.
    :param tables: Explicit ``(namespace, table)`` pairs captured together.
    :param ttl_ms: Requested lifetime, bounded by the current session expiry.
    :raises ValueError: If the table list is empty, duplicated or malformed.
    :example:

        with client.open_snapshot([("market", "ticks"), ("market", "instruments")]) as snapshot:
            left = snapshot.read_table("ticks", namespace="market")
            right = snapshot.read_table("instruments", namespace="market")
            result = left.join(right, on="symbol").collect()
    """

    def __init__(
        self, client: Client, tables: Iterable[tuple[str, str]], *, ttl_ms: int = 60_000
    ) -> None:
        self._client = client
        self._closed = True
        self._lease: _SnapshotLease | None = None
        pairs = list(tables)
        if not 0 < len(pairs) <= 32 or any(
            not isinstance(pair, tuple)
            or len(pair) != 2
            or any(not isinstance(value, str) or not value for value in pair)
            for pair in pairs
        ):
            raise ValueError("snapshot tables must contain 1 to 32 (namespace, table) pairs")
        if len(set(pairs)) != len(pairs):
            raise ValueError("snapshot tables must be unique")
        self._tables = frozenset(pairs)
        info = client.flight.create_snapshot(
            {
                f"zippy_snapshot_{index}": {"namespace": namespace, "table": table}
                for index, (namespace, table) in enumerate(pairs)
            },
            ttl_ms=ttl_ms,
        )
        self._lease = _SnapshotLease("", client, info)
        self._closed = False

    def read_table(self, name: str, *, namespace: str = "default") -> Table:
        """Open a lazy view of one captured table, retaining this exact snapshot.

        :param name: Captured table name.
        :param namespace: Captured namespace name.
        :returns: Query retaining its own reference to the same server lease.
        :raises ValueError: If the requested table was not captured.
        :raises RuntimeError: If this scope is closed.
        """
        if self._closed or self._lease is None:
            raise RuntimeError("fixed snapshot is closed")
        if (namespace, name) not in self._tables:
            raise ValueError("table is outside the fixed snapshot")
        table = Table(name, self._client, namespace=namespace)
        table._fixed_snapshot = self._lease.acquire()
        return table

    def info(self) -> dict[str, object]:
        """Return the opaque lease identity, expiry and captured table versions.

        :returns: Snapshot metadata without refreshing the captured boundary.
        :raises RuntimeError: If this scope is closed.
        """
        if self._closed or self._lease is None:
            raise RuntimeError("fixed snapshot is closed")
        return dict(self._lease.snapshot)

    def close(self) -> None:
        """Release this owner; derived queries retain the lease until they close.

        :raises ServerError: If the server rejects release of the final reference.
        """
        if not self._closed:
            self._closed = True
            lease, self._lease = self._lease, None
            if lease is not None:
                lease.release()

    def __enter__(self) -> FixedSnapshot:
        if self._closed:
            raise RuntimeError("fixed snapshot is closed")
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def __del__(self) -> None:
        from loguru import logger

        try:
            self.close()
        except Exception as error:
            logger.warning("fixed snapshot cleanup failed error=[{}]", error)
