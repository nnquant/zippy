"""Transport-neutral lazy table query exposed by :mod:`zippy_client`."""

from __future__ import annotations

import time
from typing import Any, TYPE_CHECKING

from loguru import logger

from .errors import CapabilityError
from .expressions import (
    Expr,
    col,
    expression_supported_by_gateway,
    expression_to_json,
    normalize_expressions,
    normalize_temporal_filter,
)
from .models import Transport
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
            self.client.gateway.release_snapshot(self.source, str(snapshot_id))


class Table:
    """
    Build and execute a lazy query against one Zippy table.

    :param source: Table or stream name.
    :type source: str
    :param client: Explicit connected client.
    :type client: Client
    :param snapshot: Reuse a Gateway snapshot across terminal operations. Flight
        always creates one consistent snapshot per terminal query.
    :type snapshot: bool
    :param namespace: Catalog namespace used by Flight bindings.
    :type namespace: str
    """

    def __init__(
        self,
        source: str,
        client: Client,
        *,
        snapshot: bool = True,
        namespace: str = "default",
    ) -> None:
        self.source = str(source)
        self.namespace = str(namespace)
        self._client = client
        self._snapshot_enabled = bool(snapshot)
        self._fixed_snapshot: _SnapshotLease | None = None
        self._manual_snapshots: list[_SnapshotLease] = []
        self._schema: Any | None = None
        self._info: dict[str, object] | None = None
        self._plan: list[tuple[str, object]] = []
        self._last_metrics: dict[str, object] = {}
        self._closed = False

    def filter(self, predicate: object | None = None, **equals: object) -> Table:
        """Return a query with an additional filter predicate."""
        expression = predicate
        for name, value in equals.items():
            item = col(name) == value
            expression = item if expression is None else Expr("binary", "and", (expression, item))
        if expression is None:
            return self
        return self._clone(("filter", expression))

    def where(self, predicate: object | None = None, **equals: object) -> Table:
        """Compatibility alias for :meth:`filter`."""
        return self.filter(predicate, **equals)

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

    def limit(self, n: int) -> Table:
        """Compatibility alias for :meth:`head`."""
        return self.head(n)

    def slice(self, offset: int, length: int | None = None) -> Table:
        """Return a query slicing rows by offset and optional length."""
        if offset < 0:
            raise ValueError("offset must be non-negative for Gateway queries")
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
        keys = [on] if isinstance(on, str) else [str(item) for item in on]
        return self._clone(
            (
                "join",
                {"other": other, "on": keys, "how": str(how), "suffix": str(suffix)},
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

    def stream_info(self) -> dict[str, object]:
        """Compatibility alias for :meth:`info`."""
        return self.info()

    def snapshot(self) -> dict[str, object]:
        """Return the fixed or latest server-owned query snapshot."""
        if self._uses_flight():
            query = self._last_metrics.get("query")
            if isinstance(query, dict):
                return dict(query)
            return {
                "mode": "per_query",
                "source": self.source,
                "namespace": self.namespace,
            }
        if self._snapshot_enabled and self._fixed_snapshot is not None:
            return dict(self._fixed_snapshot.snapshot)
        snapshot = self._client.gateway.create_snapshot(self.source)
        if self._snapshot_enabled:
            self._fixed_snapshot = _SnapshotLease(self.source, self._client, snapshot)
        else:
            self._manual_snapshots.append(_SnapshotLease(self.source, self._client, snapshot))
        return dict(snapshot)

    def collect(self, *, stream: bool = False):
        """Execute this query and return a PyArrow table."""
        table, _ = self._execute(stream=stream)
        return table

    def profile(self, *, stream: bool = False) -> dict[str, object]:
        """Execute this query and return its table plus client/server metrics."""
        started_ns = time.perf_counter_ns()
        table, metrics = self._execute(stream=stream)
        profile: dict[str, object] = {
            "executor": str(metrics.get("executor", "gateway")),
            "transport": self._client.query_transport.value,
            "returned_rows": int(table.num_rows),
            "elapsed_ms": (time.perf_counter_ns() - started_ns) / 1_000_000.0,
        }
        if self._uses_flight():
            profile["flight_endpoint"] = self._client.flight.endpoint
            profile["flight"] = dict(metrics)
        else:
            profile["gateway_endpoint"] = self._client.gateway.endpoint
            profile["gateway"] = dict(metrics)
        profile.update(metrics)
        return {"result": table, "metrics": profile}

    def explain(self) -> dict[str, object]:
        """Return the normalized server plan without executing it."""
        if self._uses_flight():
            compiled = compile_flight_table_sql(self)
            return {
                "source": self.source,
                "namespace": self.namespace,
                "transport": self._client.query_transport.value,
                "flight_endpoint": self._client.flight.endpoint,
                "snapshot": "per_query",
                "sql": compiled.sql,
                "bindings": compiled.bindings,
            }
        return {
            "source": self.source,
            "transport": self._client.query_transport.value,
            "gateway_endpoint": self._client.gateway.endpoint,
            "snapshot": self._snapshot_enabled,
            "plan": self._serialized_plan(),
        }

    def close(self) -> None:
        """Release this query's server-owned snapshot lease."""
        if self._closed:
            return
        self._closed = True
        fixed_snapshot = self._fixed_snapshot
        self._fixed_snapshot = None
        manual_snapshots, self._manual_snapshots = self._manual_snapshots, []
        if fixed_snapshot is not None:
            fixed_snapshot.release()
        for snapshot in manual_snapshots:
            snapshot.release()

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

    def _execute(self, *, stream: bool) -> tuple[Any, dict[str, object]]:
        if self._closed:
            raise RuntimeError("table query is closed")
        if self._uses_flight():
            compiled = compile_flight_table_sql(self)
            table, metrics = self._client.flight.read_sql(
                compiled.sql,
                compiled.bindings,
                stream=stream,
            )
            self._last_metrics = dict(metrics)
            return table, dict(metrics)
        snapshot: bool | dict[str, object]
        snapshot = self.snapshot() if self._snapshot_enabled else False
        table, metrics = self._client.gateway.collect(
            self.source,
            self._serialized_plan(),
            snapshot=snapshot,
            stream=stream,
        )
        self._last_metrics = dict(metrics)
        return table, dict(metrics)

    def _serialized_plan(self) -> list[dict[str, object]]:
        schema = self.schema()
        plan: list[dict[str, object]] = []
        for kind, value in _move_row_range_to_front(self._plan):
            if kind == "filter":
                expression = normalize_temporal_filter(value, schema)
                if not expression_supported_by_gateway(expression):
                    raise CapabilityError(
                        "Gateway cannot execute this filter expression exactly; "
                        "client-side query fallback is intentionally disabled"
                    )
                plan.append({"op": "filter", "expr": expression_to_json(expression)})
            elif kind == "select":
                expressions = _require_list(value, "select")
                if not _column_expressions(expressions):
                    raise CapabilityError("Gateway select currently supports column names only")
                plan.append(
                    {
                        "op": "select",
                        "exprs": [expression_to_json(item) for item in expressions],
                    }
                )
            elif kind in {"tail", "head"}:
                plan.append({"op": kind, "n": _require_int(value, kind)})
            elif kind == "slice":
                spec = _require_dict(value, "slice")
                plan.append(
                    {
                        "op": "slice",
                        "offset": _require_int(spec.get("offset"), "slice.offset"),
                        "length": spec.get("length"),
                    }
                )
            elif kind == "sort":
                spec = _require_dict(value, "sort")
                keys = _require_list(spec.get("by"), "sort.by")
                if not _column_expressions(keys):
                    raise CapabilityError("Gateway sort currently supports column names only")
                plan.append(
                    {
                        "op": "sort",
                        "by": [expression_to_json(item) for item in keys],
                        "descending": spec.get("descending", False),
                    }
                )
            elif kind == "drop":
                plan.append({"op": "drop", "columns": _require_list(value, "drop")})
            elif kind == "rename":
                plan.append({"op": "rename", "mapping": _require_dict(value, "rename")})
            else:
                raise CapabilityError(f"Gateway does not advertise table plan operation=[{kind}]")
        return plan

    def _load_metadata(self, *, refresh: bool = False) -> None:
        if not refresh and self._schema is not None and self._info is not None:
            return
        if self._uses_flight():
            self._schema = self._client.flight.get_table_schema(
                self.source,
                namespace=self.namespace,
            )
            try:
                self._info = self._client.control.get_stream(self.source)
            except Exception as error:
                logger.debug(
                    "failed to resolve Flight table metadata source=[{}] namespace=[{}] error=[{}]",
                    self.source,
                    self.namespace,
                    error,
                )
                self._info = {
                    "name": self.source,
                    "namespace": self.namespace,
                    "transport": "flight",
                }
            return
        info, schema = self._client.gateway.get_stream(self.source)
        self._info = info
        self._schema = schema

    def _clone(self, operation: tuple[str, object]) -> Table:
        clone = Table(
            self.source,
            self._client,
            snapshot=self._snapshot_enabled,
            namespace=self.namespace,
        )
        if self._fixed_snapshot is not None:
            clone._fixed_snapshot = self._fixed_snapshot.acquire()
        clone._schema = self._schema
        clone._info = self._info
        clone._plan = [*self._plan, operation]
        return clone

    def _uses_flight(self) -> bool:
        return self._client.query_transport in {
            Transport.FLIGHT_LOCAL,
            Transport.FLIGHT_REMOTE,
        }


def _column_names(columns: tuple[object, ...]) -> list[str]:
    if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
        columns = tuple(columns[0])
    return [str(column) for column in columns]


def _column_expressions(expressions: object) -> bool:
    if not isinstance(expressions, list):
        return False
    return all(
        isinstance(item, str) or (isinstance(item, Expr) and item.kind == "col")
        for item in expressions
    )


def _move_row_range_to_front(
    plan: list[tuple[str, object]],
) -> list[tuple[str, object]]:
    commutable = {"select", "drop", "rename"}
    for index, (kind, value) in enumerate(plan):
        if kind in {"head", "tail", "slice"}:
            if all(prefix_kind in commutable for prefix_kind, _ in plan[:index]):
                return [(kind, value), *plan[:index], *plan[index + 1 :]]
            break
        if kind not in commutable:
            break
    return list(plan)


def _require_list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"query plan value must be a list operation=[{name}]")
    return value


def _require_dict(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"query plan value must be an object operation=[{name}]")
    return {str(key): item for key, item in value.items()}


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError(f"query plan value must be an integer operation=[{name}]")
    return int(value)
