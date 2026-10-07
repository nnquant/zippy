"""Native Flight lazy table query exposed by :mod:`zippy_client`."""

from __future__ import annotations

import time
from typing import Any, TYPE_CHECKING

from loguru import logger

from .errors import QueryError
from .expressions import (
    Expr,
    col,
    normalize_expressions,
)
from .sql import compile_flight_table_sql

if TYPE_CHECKING:
    from .client import Client


class _SnapshotLease:
    """Reference-count one server-owned snapshot across lazy query clones."""

    def __init__(
        self,
        source: str,
        client: Client,
        snapshot: dict[str, object],
    ) -> None:
        self.source = source
        self.client = client
        self.snapshot = dict(snapshot)
        self._references = 1
        self._released = False

    def acquire(self) -> _SnapshotLease:
        """Add one owning table reference."""
        if self._released:
            raise RuntimeError("query snapshot lease is already released")
        self._references += 1
        return self

    def release(self) -> None:
        """Drop one owner and release the server snapshot after the last owner."""
        if self._released:
            return
        self._references -= 1
        if self._references > 0:
            return
        self._released = True
        snapshot_id = self.snapshot.get("snapshot_id")
        if snapshot_id is not None:
            self.client.flight.release_snapshot(str(snapshot_id))


class Table:
    """
    Build and execute a lazy query against one Zippy table.

    :param source: Table or stream name.
    :type source: str
    :param client: Explicit connected client.
    :type client: Client
    :param snapshot: Request a fixed snapshot across terminal operations.
        Unsupported transports reject this request; the default is per-query isolation.
    :type snapshot: bool
    :param namespace: Catalog namespace used by Flight bindings.
    :type namespace: str
    """

    def __init__(
        self,
        source: str,
        client: Client,
        *,
        snapshot: bool = False,
        namespace: str = "default",
    ) -> None:
        self.source = str(source)
        self.namespace = str(namespace)
        self._client = client
        self._fixed_snapshot: _SnapshotLease | None = None
        self._schema: Any | None = None
        self._info: dict[str, object] | None = None
        self._plan: list[tuple[str, object]] = []
        self._last_metrics: dict[str, object] = {}
        self._closed = False
        if snapshot:
            self.snapshot()

    def filter(self, predicate: object | None = None, **equals: object) -> Table:
        """Return a query with an additional filter predicate."""
        expression = predicate
        for name, value in equals.items():
            item = col(name) == value
            expression = item if expression is None else Expr("binary", "and", (expression, item))
        if expression is None:
            return self
        return self._clone(("filter", expression))

    def select(self, *expressions: object, **named_expressions: object) -> Table:
        """Return a query projecting columns or supported expressions."""
        normalized = normalize_expressions(expressions, named_expressions)
        return self._clone(("select", normalized))

    def with_columns(self, *expressions: object, **named_expressions: object) -> Table:
        """Return a query adding expressions, validated at collect time."""
        normalized = normalize_expressions(expressions, named_expressions)
        return self._clone(("with_columns", normalized))

    def tail(self, n: int) -> Table:
        """Return a query retaining the latest ``n`` rows."""
        if n < 0:
            raise ValueError("n must be non-negative")
        return self._clone(("tail", int(n)))

    def head(self, n: int) -> Table:
        """Return a query retaining the first ``n`` rows."""
        if n < 0:
            raise ValueError("n must be non-negative")
        return self._clone(("head", int(n)))

    def slice(self, offset: int, length: int | None = None) -> Table:
        """Return a query slicing rows by offset and optional length."""
        if offset < 0:
            raise ValueError("offset must be non-negative for queries")
        if length is not None and length < 0:
            raise ValueError("length must be non-negative")
        return self._clone(("slice", {"offset": int(offset), "length": length}))

    def sort(
        self,
        by: object | list[object] | tuple[object, ...],
        *,
        descending: bool | list[bool] | tuple[bool, ...] = False,
    ) -> Table:
        """Return a query sorted by one or more columns."""
        keys = list(by) if isinstance(by, (list, tuple)) else [by]
        if not keys:
            raise ValueError("sort requires at least one key")
        directions = list(descending) if isinstance(descending, (list, tuple)) else bool(descending)
        return self._clone(("sort", {"by": keys, "descending": directions}))

    def drop(self, *columns: object) -> Table:
        """Return a query dropping columns by name."""
        normalized = _column_names(columns)
        return self if not normalized else self._clone(("drop", normalized))

    def rename(self, mapping: dict[str, str] | None = None, **renames: str) -> Table:
        """Return a query renaming columns."""
        normalized = {str(key): str(value) for key, value in (mapping or {}).items()}
        normalized.update({str(key): str(value) for key, value in renames.items()})
        return self if not normalized else self._clone(("rename", normalized))

    def join(
        self,
        other: Table,
        *,
        on: str | list[str] | tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> Table:
        """Return a join query, validated against server capabilities at collect time."""
        if not isinstance(other, Table):
            raise TypeError("join other must be a zippy_client.Table")
        if self._client is not other._client:
            raise ValueError("joined tables must share one client")
        if self._fixed_snapshot is not other._fixed_snapshot:
            raise ValueError("fixed joins require tables from the same Client.open_snapshot scope")
        keys = [on] if isinstance(on, str) else [str(item) for item in on]
        return self._clone(
            (
                "join",
                {"other": other._clone(), "on": keys, "how": str(how), "suffix": str(suffix)},
            )
        )

    def between(self, column: object, start: object, end: object) -> Table:
        """Return a query filtered to inclusive ``[start, end]`` bounds."""
        expression = col(column) if isinstance(column, str) else column
        if not isinstance(expression, Expr):
            raise TypeError("between column must be a column name or Expr")
        return self.filter((expression >= start) & (expression <= end))

    def schema(self) -> Any:
        """Return the source Arrow schema."""
        self._load_metadata()
        return self._schema

    def info(self) -> dict[str, object]:
        """Return current stream metadata."""
        self._load_metadata(refresh=True)
        return dict(self._info or {})

    def snapshot(self) -> dict[str, object]:
        """Pin this query and its clones to one server-owned snapshot."""
        if self._closed:
            raise RuntimeError("table query is closed")
        if self._fixed_snapshot is not None:
            return dict(self._fixed_snapshot.snapshot)
        sources: dict[tuple[str, str], None] = {}
        visiting: set[int] = set()
        visited: set[int] = set()
        tables: list[Table] = []

        def collect_sources(table: Table) -> None:
            identity = id(table)
            if identity in visiting:
                raise ValueError("cyclic table join cannot be snapshotted")
            if identity in visited:
                return
            visiting.add(identity)
            tables.append(table)
            sources[(table.namespace, table.source)] = None
            for operation, arguments in table._plan:
                if operation == "join":
                    if not isinstance(arguments, dict) or not isinstance(
                        arguments.get("other"), Table
                    ):
                        raise ValueError("table join has no source query")
                    collect_sources(arguments["other"])
            visiting.remove(identity)
            visited.add(identity)

        collect_sources(self)
        if len(tables) > 32:
            raise ValueError("fixed snapshot query exceeds 32 table bindings")
        bindings = {
            f"zippy_snapshot_{index}": {"namespace": namespace, "table": name}
            for index, (namespace, name) in enumerate(sources)
        }
        snapshot = self._client.flight.create_snapshot(bindings)
        lease = _SnapshotLease(self.source, self._client, snapshot)
        for table in tables:
            table._fixed_snapshot = lease if table is self else lease.acquire()
            table._schema = None
            table._info = None
        return dict(snapshot)

    def collect(self):
        """Execute this query and return a PyArrow table."""
        table, _ = self._execute()
        return table

    def profile(self) -> dict[str, object]:
        """Execute this query and return its table plus client/server metrics."""
        started_ns = time.perf_counter_ns()
        table, metrics = self._execute()
        profile: dict[str, object] = {
            "executor": str(metrics.get("executor", "flight")),
            "transport": self._client.query_transport.value,
            "returned_rows": int(table.num_rows),
            "elapsed_ms": (time.perf_counter_ns() - started_ns) / 1_000_000.0,
        }
        profile["flight_endpoint"] = self._client.flight.endpoint
        profile["flight"] = dict(metrics)
        profile.update(metrics)
        return {"result": table, "metrics": profile}

    def explain(self) -> dict[str, object]:
        """Return the normalized server plan without executing it."""
        compiled = compile_flight_table_sql(self)
        return {
            "source": self.source,
            "namespace": self.namespace,
            "transport": self._client.query_transport.value,
            "flight_endpoint": self._client.flight.endpoint,
            "snapshot": (
                dict(self._fixed_snapshot.snapshot)
                if self._fixed_snapshot is not None
                else "per_query"
            ),
            "sql": compiled.sql,
            "bindings": compiled.bindings,
        }

    def close(self) -> None:
        """Release this query's server-owned snapshot lease."""
        if self._closed:
            return
        self._closed = True
        plan, self._plan = self._plan, []
        fixed_snapshot = self._fixed_snapshot
        self._fixed_snapshot = None
        first_error: BaseException | None = None
        for operation, arguments in plan:
            if operation != "join" or not isinstance(arguments, dict):
                continue
            other = arguments.get("other")
            if isinstance(other, Table):
                try:
                    other.close()
                except BaseException as error:
                    first_error = first_error or error
        if fixed_snapshot is not None:
            try:
                fixed_snapshot.release()
            except BaseException as error:
                first_error = first_error or error
        if first_error is not None:
            raise first_error

    def __enter__(self) -> Table:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type, exc_value, traceback
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except BaseException as error:
            logger.warning(
                "failed to release query snapshot source=[{}] error=[{}]",
                getattr(self, "source", "<unknown>"),
                error,
            )

    def _execute(self) -> tuple[Any, dict[str, object]]:
        if self._closed:
            raise RuntimeError("table query is closed")
        compiled = compile_flight_table_sql(self)
        table, metrics = self._client.flight.read_sql(
            compiled.sql, compiled.bindings, fixed_snapshot_id=self._native_snapshot_id()
        )
        self._last_metrics = dict(metrics)
        return table, dict(metrics)

    def _load_metadata(self, *, refresh: bool = False) -> None:
        if self._closed:
            raise RuntimeError("table query is closed")
        if not refresh and self._schema is not None and self._info is not None:
            return
        self._schema = self._client.flight.get_table_schema(
            self.source,
            namespace=self.namespace,
            fixed_snapshot_id=self._native_snapshot_id(),
        )
        if self._fixed_snapshot is not None:
            self._info = {
                "name": self.source,
                "namespace": self.namespace,
                "catalog": "zippy",
                "table_type": "TABLE",
                "snapshot_id": self._native_snapshot_id(),
            }
            return
        matches = [
            table
            for table in self._client.flight.list_tables(namespace=self.namespace)
            if table["name"] == self.source
        ]
        if len(matches) != 1:
            raise QueryError(f"table metadata is missing or ambiguous source=[{self.source}]")
        self._info = matches[0]
        return

    def _clone(self, operation: tuple[str, object] | None = None) -> Table:
        if self._closed:
            raise RuntimeError("table query is closed")
        clone = Table(
            self.source,
            self._client,
            snapshot=False,
            namespace=self.namespace,
        )
        if self._fixed_snapshot is not None:
            clone._fixed_snapshot = self._fixed_snapshot.acquire()
        clone._schema = self._schema
        clone._info = self._info
        clone._plan = []
        for kind, arguments in self._plan:
            if (
                kind == "join"
                and isinstance(arguments, dict)
                and isinstance(arguments.get("other"), Table)
            ):
                clone._plan.append((kind, {**arguments, "other": arguments["other"]._clone()}))
            else:
                clone._plan.append((kind, arguments))
        if operation is not None:
            clone._plan.append(operation)
        return clone

    def _native_snapshot_id(self) -> str | None:
        if self._fixed_snapshot is None:
            return None
        return str(self._fixed_snapshot.snapshot["snapshot_id"])


def _column_names(columns: tuple[object, ...]) -> list[str]:
    if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
        columns = tuple(columns[0])
    return [str(column) for column in columns]
