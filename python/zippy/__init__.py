from __future__ import annotations

from collections import Counter
import hashlib
import json
import os
import socket
import struct
import tempfile
import threading
import time
from pathlib import Path

_NATIVE_IMPORT_ERROR: BaseException | None = None
_NATIVE_AVAILABLE = os.environ.get("ZIPPY_FORCE_PURE_PYTHON") != "1"


def _native_unavailable(name: str):
    class _NativeUnavailable:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs
            raise RuntimeError(
                "zippy native extension is unavailable; "
                f"{name} requires the platform-specific zippy wheel"
            ) from _NATIVE_IMPORT_ERROR

    _NativeUnavailable.__name__ = name
    return _NativeUnavailable


if _NATIVE_AVAILABLE:
    try:
        from ._internal import AbsSpec
        from ._internal import AggCountSpec
        from ._internal import AggFirstSpec
        from ._internal import AggLastSpec
        from ._internal import AggMaxSpec
        from ._internal import AggMinSpec
        from ._internal import AggSumSpec
        from ._internal import AggVwapSpec
        from ._internal import CastSpec
        from ._internal import ClipSpec
        from ._internal import CSDemeanSpec
        from ._internal import CSRankSpec
        from ._internal import CSZscoreSpec
        from ._internal import CrossSectionalEngine
        from ._internal import ExpressionFactor
        from ._internal import LogSpec
        from ._internal import MasterClient
        from ._internal import MasterServer
        from ._internal import run_master_daemon
        from ._internal import BusReader
        from ._internal import KeyValueTableMaterializer as _KeyValueTableMaterializer
        from ._internal import SegmentStreamSource
        from ._internal import BusStreamSource
        from ._internal import BusStreamTarget
        from ._internal import BusWriter
        from ._internal import NullPublisher
        from ._internal import ParquetSink
        from ._internal import Query as _NativeQuery
        from ._internal import ReactiveLatestEngine
        from ._internal import ReactiveStateEngine
        from ._internal import StreamSubscriber as _NativeStreamSubscriber
        from ._internal import StreamTableMaterializer as _StreamTableMaterializer
        from ._internal import TimeSeriesEngine
        from ._internal import TsDelaySpec
        from ._internal import TsDiffSpec
        from ._internal import TsEmaSpec
        from ._internal import TsMeanSpec
        from ._internal import TsReturnSpec
        from ._internal import TsStdSpec
        from ._internal import ZmqPublisher
        from ._internal import ZmqSource
        from ._internal import ZmqStreamPublisher
        from ._internal import ZmqSubscriber
        from ._internal import __version__
        from ._internal import log_info
        from ._internal import setup_log
        from ._internal import version
    except ImportError as error:
        _NATIVE_AVAILABLE = False
        _NATIVE_IMPORT_ERROR = error

if not _NATIVE_AVAILABLE:
    AbsSpec = _native_unavailable("AbsSpec")
    AggCountSpec = _native_unavailable("AggCountSpec")
    AggFirstSpec = _native_unavailable("AggFirstSpec")
    AggLastSpec = _native_unavailable("AggLastSpec")
    AggMaxSpec = _native_unavailable("AggMaxSpec")
    AggMinSpec = _native_unavailable("AggMinSpec")
    AggSumSpec = _native_unavailable("AggSumSpec")
    AggVwapSpec = _native_unavailable("AggVwapSpec")
    CastSpec = _native_unavailable("CastSpec")
    ClipSpec = _native_unavailable("ClipSpec")
    CSDemeanSpec = _native_unavailable("CSDemeanSpec")
    CSRankSpec = _native_unavailable("CSRankSpec")
    CSZscoreSpec = _native_unavailable("CSZscoreSpec")
    CrossSectionalEngine = _native_unavailable("CrossSectionalEngine")
    ExpressionFactor = _native_unavailable("ExpressionFactor")
    LogSpec = _native_unavailable("LogSpec")
    MasterClient = _native_unavailable("MasterClient")
    MasterServer = _native_unavailable("MasterServer")
    BusReader = _native_unavailable("BusReader")
    _KeyValueTableMaterializer = _native_unavailable("KeyValueTableMaterializer")
    SegmentStreamSource = _native_unavailable("SegmentStreamSource")
    BusStreamSource = _native_unavailable("BusStreamSource")
    BusStreamTarget = _native_unavailable("BusStreamTarget")
    BusWriter = _native_unavailable("BusWriter")
    NullPublisher = _native_unavailable("NullPublisher")
    ParquetSink = _native_unavailable("ParquetSink")
    _NativeQuery = _native_unavailable("Query")
    ReactiveLatestEngine = _native_unavailable("ReactiveLatestEngine")
    ReactiveStateEngine = _native_unavailable("ReactiveStateEngine")
    _NativeStreamSubscriber = _native_unavailable("StreamSubscriber")
    _StreamTableMaterializer = _native_unavailable("StreamTableMaterializer")
    TimeSeriesEngine = _native_unavailable("TimeSeriesEngine")
    TsDelaySpec = _native_unavailable("TsDelaySpec")
    TsDiffSpec = _native_unavailable("TsDiffSpec")
    TsEmaSpec = _native_unavailable("TsEmaSpec")
    TsMeanSpec = _native_unavailable("TsMeanSpec")
    TsReturnSpec = _native_unavailable("TsReturnSpec")
    TsStdSpec = _native_unavailable("TsStdSpec")
    ZmqPublisher = _native_unavailable("ZmqPublisher")
    ZmqSource = _native_unavailable("ZmqSource")
    ZmqStreamPublisher = _native_unavailable("ZmqStreamPublisher")
    ZmqSubscriber = _native_unavailable("ZmqSubscriber")
    __version__ = "0.1.0+pure"

    def run_master_daemon(*args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError(
            "zippy native extension is unavailable; run_master_daemon requires "
            "the platform-specific zippy wheel"
        ) from _NATIVE_IMPORT_ERROR

    def log_info(*args, **kwargs) -> None:
        del args, kwargs

    def setup_log(*args, **kwargs) -> None:
        del args, kwargs

    def version() -> str:
        return __version__

DEFAULT_MASTER_URI = "zippy://default"
DEFAULT_REMOTE_GATEWAY_PORT = 17666
_DEFAULT_HEARTBEAT_INTERVAL_DEFAULT_SEC = 3.0
_DEFAULT_HEARTBEAT_INTERVAL_SEC = _DEFAULT_HEARTBEAT_INTERVAL_DEFAULT_SEC
_DEFAULT_MASTER: MasterClient | None = None
_DEFAULT_HEARTBEAT: _HeartbeatHandle | None = None
_DEFAULT_HEARTBEAT_LOCK = threading.Lock()
_USE_MASTER_CONFIG = object()
_BUILTIN_CONFIG: dict[str, object] = {
    "log": {
        "level": "info",
    },
    "table": {
        "row_capacity": 65_536,
        "retention_segments": None,
        "replacement_retention_snapshots": 8,
        "persist": {
            "enabled": False,
            "method": "parquet",
            "data_dir": "data",
            "partition": {
                "dt_column": None,
                "id_column": None,
                "dt_part": None,
            },
        },
    },
    "remote_gateway": {
        "enabled": False,
        "endpoint": None,
        "token": None,
        "protocol_version": 1,
    },
}


class _QueryExpr:
    """
    Internal query expression AST node compiled to a Polars expression at execution time.
    """

    __slots__ = ("_kind", "_value", "_args")

    def __init__(
        self,
        kind: str,
        value: object = None,
        args: tuple[object, ...] = (),
    ) -> None:
        self._kind = kind
        self._value = value
        self._args = args

    def alias(self, name: str) -> _QueryExpr:
        """Return this expression with an output alias."""
        return _QueryExpr("alias", str(name), (self,))

    def is_in(self, values: object) -> _QueryExpr:
        """Return an expression testing membership in ``values``."""
        return _QueryExpr("is_in", None, (self, values))

    def is_null(self) -> _QueryExpr:
        """Return an expression testing whether this expression is null."""
        return _QueryExpr("is_null", None, (self,))

    def is_not_null(self) -> _QueryExpr:
        """Return an expression testing whether this expression is not null."""
        return _QueryExpr("is_not_null", None, (self,))

    def is_between(self, lower_bound: object, upper_bound: object) -> _QueryExpr:
        """Return an inclusive range predicate for this expression."""
        return _QueryExpr(
            "is_between",
            None,
            (self, _literal(lower_bound), _literal(upper_bound)),
        )

    def __eq__(self, other: object) -> _QueryExpr:  # type: ignore[override]
        return self._binary("eq", other)

    def __ne__(self, other: object) -> _QueryExpr:  # type: ignore[override]
        return self._binary("ne", other)

    def __gt__(self, other: object) -> _QueryExpr:
        return self._binary("gt", other)

    def __ge__(self, other: object) -> _QueryExpr:
        return self._binary("ge", other)

    def __lt__(self, other: object) -> _QueryExpr:
        return self._binary("lt", other)

    def __le__(self, other: object) -> _QueryExpr:
        return self._binary("le", other)

    def __and__(self, other: object) -> _QueryExpr:
        return self._binary("and", other)

    def __or__(self, other: object) -> _QueryExpr:
        return self._binary("or", other)

    def __add__(self, other: object) -> _QueryExpr:
        return self._binary("add", other)

    def __radd__(self, other: object) -> _QueryExpr:
        return _literal(other)._binary("add", self)

    def __sub__(self, other: object) -> _QueryExpr:
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> _QueryExpr:
        return _literal(other)._binary("sub", self)

    def __mul__(self, other: object) -> _QueryExpr:
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> _QueryExpr:
        return _literal(other)._binary("mul", self)

    def __truediv__(self, other: object) -> _QueryExpr:
        return self._binary("div", other)

    def __rtruediv__(self, other: object) -> _QueryExpr:
        return _literal(other)._binary("div", self)

    def __invert__(self) -> _QueryExpr:
        return _QueryExpr("unary", "not", (self,))

    def __neg__(self) -> _QueryExpr:
        return _QueryExpr("unary", "neg", (self,))

    def __bool__(self) -> bool:
        raise TypeError("query expressions cannot be evaluated as booleans; use '&' or '|'")

    def _binary(self, op: str, other: object) -> _QueryExpr:
        return _QueryExpr("binary", op, (self, _literal(other)))


def col(name: str) -> _QueryExpr:
    """
    Create a Zippy query column expression.

    :param name: Column name.
    :type name: str
    :returns: Table expression referencing the column.
    :rtype: zippy query expression
    """
    return _QueryExpr("col", str(name))


def lit(value: object) -> _QueryExpr:
    """
    Create a Zippy query literal expression.

    :param value: Literal value used in a query expression.
    :type value: object
    :returns: Table expression wrapping the literal value.
    :rtype: zippy query expression
    """
    return _literal(value)


def _literal(value: object) -> _QueryExpr:
    if isinstance(value, _QueryExpr):
        return value
    return _QueryExpr("literal", value)


def _compile_query_expr_to_polars(expr: object):
    import polars as pl

    if isinstance(expr, str):
        return pl.col(expr)
    if not isinstance(expr, _QueryExpr):
        return pl.lit(expr)

    if expr._kind == "col":
        return pl.col(str(expr._value))
    if expr._kind == "literal":
        return pl.lit(expr._value)
    if expr._kind == "alias":
        return _compile_query_expr_to_polars(expr._args[0]).alias(str(expr._value))
    if expr._kind == "is_in":
        return _compile_query_expr_to_polars(expr._args[0]).is_in(expr._args[1])
    if expr._kind == "is_null":
        return _compile_query_expr_to_polars(expr._args[0]).is_null()
    if expr._kind == "is_not_null":
        return _compile_query_expr_to_polars(expr._args[0]).is_not_null()
    if expr._kind == "is_between":
        return _compile_query_expr_to_polars(expr._args[0]).is_between(
            _compile_query_expr_to_polars(expr._args[1]),
            _compile_query_expr_to_polars(expr._args[2]),
        )
    if expr._kind == "unary":
        value = _compile_query_expr_to_polars(expr._args[0])
        op = expr._value
        if op == "not":
            return ~value
        if op == "neg":
            return -value
        raise ValueError(f"unsupported query expression operator=[{op}]")
    if expr._kind != "binary":
        raise ValueError(f"unsupported query expression kind=[{expr._kind}]")

    left = _compile_query_expr_to_polars(expr._args[0])
    right = _compile_query_expr_to_polars(expr._args[1])
    op = expr._value
    if op == "eq":
        return left == right
    if op == "ne":
        return left != right
    if op == "gt":
        return left > right
    if op == "ge":
        return left >= right
    if op == "lt":
        return left < right
    if op == "le":
        return left <= right
    if op == "and":
        return left & right
    if op == "or":
        return left | right
    if op == "add":
        return left + right
    if op == "sub":
        return left - right
    if op == "mul":
        return left * right
    if op == "div":
        return left / right
    raise ValueError(f"unsupported query expression operator=[{op}]")


def _expr_columns(expr: object) -> set[str]:
    if isinstance(expr, str):
        return {expr}
    if not isinstance(expr, _QueryExpr):
        return set()
    if expr._kind == "col":
        return {str(expr._value)}
    columns: set[str] = set()
    for arg in expr._args:
        columns.update(_expr_columns(arg))
    return columns


def _normalize_query_exprs(
    exprs: tuple[object, ...],
    named_exprs: dict[str, object],
) -> list[object]:
    normalized: list[object] = []
    if len(exprs) == 1 and isinstance(exprs[0], (list, tuple)):
        normalized.extend(exprs[0])
    else:
        normalized.extend(exprs)
    for name, expr in named_exprs.items():
        normalized.append(_literal(expr).alias(name))
    return normalized


def _combine_filter_ops(filters: list[object]) -> object | None:
    predicate: object | None = None
    for item in filters:
        predicate = _combine_query_predicates(predicate, item)
    return predicate


def _predicate_to_pyarrow_filters(
    predicate: object | None,
) -> list[tuple[str, str, object]] | None:
    if predicate is None:
        return None
    if isinstance(predicate, _QueryExpr):
        if predicate._kind == "binary":
            op = str(predicate._value)
            left, right = predicate._args
            if op == "and":
                left_filters = _predicate_to_pyarrow_filters(left)
                right_filters = _predicate_to_pyarrow_filters(right)
                if left_filters is None or right_filters is None:
                    return None
                return left_filters + right_filters
            pyarrow_op = {
                "eq": "==",
                "ne": "!=",
                "gt": ">",
                "ge": ">=",
                "lt": "<",
                "le": "<=",
            }.get(op)
            if pyarrow_op is None:
                return None
            simple = _simple_column_literal_pair(left, right)
            if simple is None:
                reverse = _simple_column_literal_pair(right, left)
                if reverse is None:
                    return None
                column, value = reverse
                reverse_op = {
                    "==": "==",
                    "!=": "!=",
                    ">": "<",
                    ">=": "<=",
                    "<": ">",
                    "<=": ">=",
                }[pyarrow_op]
                return [(column, reverse_op, value)]
            column, value = simple
            return [(column, pyarrow_op, value)]
        if predicate._kind == "is_in":
            target = predicate._args[0]
            values = predicate._args[1]
            if isinstance(target, _QueryExpr) and target._kind == "col":
                return [(str(target._value), "in", list(values))]
    return None


def _simple_column_literal_pair(left: object, right: object) -> tuple[str, object] | None:
    if not (isinstance(left, _QueryExpr) and left._kind == "col"):
        return None
    if isinstance(right, _QueryExpr):
        if right._kind != "literal":
            return None
        return (str(left._value), right._value)
    return (str(left._value), right)


def _ordered_subset(values: set[str], schema: object | None = None) -> list[str]:
    if schema is not None:
        names = list(getattr(schema, "names", []))
        ordered = [name for name in names if name in values]
        ordered.extend(sorted(values - set(ordered)))
        return ordered
    return sorted(values)


def _combine_query_predicates(left: object | None, right: object | None) -> object | None:
    if left is None:
        return right
    if right is None:
        return left
    return _literal(left) & _literal(right)


def _query_expr_to_json(expr: object) -> dict[str, object]:
    if isinstance(expr, str):
        return {"kind": "col", "value": expr}
    if not isinstance(expr, _QueryExpr):
        return {"kind": "literal", "value": expr}
    if expr._kind == "col":
        return {"kind": "col", "value": expr._value}
    if expr._kind == "literal":
        return {"kind": "literal", "value": expr._value}
    if expr._kind == "alias":
        return {
            "kind": "alias",
            "value": expr._value,
            "arg": _query_expr_to_json(expr._args[0]),
        }
    if expr._kind == "is_in":
        return {
            "kind": "is_in",
            "args": [
                _query_expr_to_json(expr._args[0]),
                _query_expr_to_json(_literal(list(expr._args[1]))),
            ],
        }
    if expr._kind in {"is_null", "is_not_null"}:
        return {
            "kind": expr._kind,
            "arg": _query_expr_to_json(expr._args[0]),
        }
    if expr._kind == "is_between":
        return {
            "kind": "is_between",
            "args": [_query_expr_to_json(arg) for arg in expr._args],
        }
    if expr._kind == "unary":
        return {
            "kind": "unary",
            "op": expr._value,
            "arg": _query_expr_to_json(expr._args[0]),
        }
    if expr._kind == "binary":
        return {
            "kind": "binary",
            "op": expr._value,
            "args": [_query_expr_to_json(arg) for arg in expr._args],
        }
    raise ValueError(f"unsupported query expression kind=[{expr._kind}]")


def _query_expr_from_json(payload: dict[str, object]) -> object:
    kind = str(payload["kind"])
    if kind == "col":
        return col(str(payload["value"]))
    if kind == "literal":
        return lit(payload.get("value"))
    if kind == "alias":
        return _literal(_query_expr_from_json(payload["arg"])).alias(str(payload["value"]))
    if kind == "is_in":
        args = payload["args"]
        return _literal(_query_expr_from_json(args[0])).is_in(
            _query_expr_from_json(args[1])._value
        )
    if kind == "is_null":
        return _literal(_query_expr_from_json(payload["arg"])).is_null()
    if kind == "is_not_null":
        return _literal(_query_expr_from_json(payload["arg"])).is_not_null()
    if kind == "is_between":
        args = payload["args"]
        return _literal(_query_expr_from_json(args[0])).is_between(
            _query_expr_from_json(args[1])._value,
            _query_expr_from_json(args[2])._value,
        )
    if kind == "unary":
        value = _literal(_query_expr_from_json(payload["arg"]))
        op = str(payload["op"])
        if op == "not":
            return ~value
        if op == "neg":
            return -value
        raise ValueError(f"unsupported remote query unary op=[{op}]")
    if kind == "binary":
        left, right = [_literal(_query_expr_from_json(arg)) for arg in payload["args"]]
        op = str(payload["op"])
        if op == "eq":
            return left == right
        if op == "ne":
            return left != right
        if op == "gt":
            return left > right
        if op == "ge":
            return left >= right
        if op == "lt":
            return left < right
        if op == "le":
            return left <= right
        if op == "and":
            return left & right
        if op == "or":
            return left | right
        if op == "add":
            return left + right
        if op == "sub":
            return left - right
        if op == "mul":
            return left * right
        if op == "div":
            return left / right
        raise ValueError(f"unsupported remote query binary op=[{op}]")
    raise ValueError(f"unsupported remote query expression kind=[{kind}]")


def _query_plan_to_json(plan_ops: list[tuple[str, object]]) -> list[dict[str, object]]:
    plan: list[dict[str, object]] = []
    for kind, value in plan_ops:
        if kind == "filter":
            plan.append({"op": "filter", "expr": _query_expr_to_json(value)})
        elif kind == "select":
            plan.append({"op": "select", "exprs": [_query_expr_to_json(expr) for expr in value]})
        elif kind == "with_columns":
            plan.append(
                {
                    "op": "with_columns",
                    "exprs": [_query_expr_to_json(expr) for expr in value],
                }
            )
        elif kind == "join":
            raise ValueError("remote query join is not supported in this gateway version")
        else:
            raise ValueError(f"unsupported table plan operation=[{kind}]")
    return plan


def _apply_query_plan_json(table: "Table", plan: list[dict[str, object]]) -> "Table":
    for item in plan:
        op = str(item["op"])
        if op == "filter":
            table = table.filter(_query_expr_from_json(item["expr"]))
        elif op == "select":
            table = table.select([_query_expr_from_json(expr) for expr in item["exprs"]])
        elif op == "with_columns":
            table = table.with_columns([_query_expr_from_json(expr) for expr in item["exprs"]])
        else:
            raise ValueError(f"unsupported remote query plan op=[{op}]")
    return table


class _HeartbeatHandle:
    def __init__(self, master: MasterClient, interval_sec: float) -> None:
        self.master = master
        self.interval_sec = interval_sec
        self.last_error: Exception | None = None
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="zippy-master-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=1.0)

    def _run(self) -> None:
        while not self._stop_event.wait(self.interval_sec):
            try:
                self.master.heartbeat()
                self.last_error = None
            except Exception as error:
                self.last_error = error


def connect(
    uri: str | None = None,
    *,
    app: str | None = None,
    heartbeat_interval_sec: float = _DEFAULT_HEARTBEAT_INTERVAL_SEC,
) -> MasterClient:
    """
    Connect to zippy-master and set the process-wide default master connection.

    :param uri: Master URI. When omitted,
        ``ZIPPY_MASTER_URI``, ``ZIPPY_MASTER_ENDPOINT``, ``ZIPPY_CONTROL_ENDPOINT``,
        or ``zippy://default`` is used. On Windows this resolves to the
        platform default TCP loopback endpoint; on Unix it resolves to
        ``~/.zippy/control_endpoints/default/master.sock``.
    :type uri: str | None
    :param app: Optional process name to register immediately after connecting. When provided,
        a daemon heartbeat thread is started for this process lease.
    :type app: str | None
    :param heartbeat_interval_sec: Process lease heartbeat interval in seconds.
    :type heartbeat_interval_sec: float
    :returns: The default master client.
    :rtype: MasterClient
    :raises RuntimeError: If the master connection or process registration fails.
    :raises ValueError: If ``heartbeat_interval_sec`` is not positive.
    :example:

        >>> client = zippy.connect(uri="default", app="research_session")
        >>> zippy.read_table("ctp_ticks").tail(1000)
    """
    interval_sec = _validate_heartbeat_interval(heartbeat_interval_sec)
    raw_uri = (
        uri
        or os.environ.get("ZIPPY_MASTER_URI")
        or os.environ.get("ZIPPY_MASTER_ENDPOINT")
        or os.environ.get("ZIPPY_CONTROL_ENDPOINT")
        or DEFAULT_MASTER_URI
    )
    endpoint = _resolve_uri(raw_uri)
    remote_master_endpoint = _remote_master_endpoint_from_zippy_uri(raw_uri)
    if remote_master_endpoint is not None and not _NATIVE_AVAILABLE:
        client = RemoteMasterClient.from_master_endpoint(remote_master_endpoint)
        if app is not None:
            client.register_process(app)
        _set_default_master(client, None, interval_sec)
        return client
    client = MasterClient(control_endpoint=endpoint)
    heartbeat = None
    try:
        if app is not None:
            client.register_process(app)
            heartbeat = _HeartbeatHandle(client, interval_sec)
        else:
            client.list_streams()
    except RuntimeError as error:
        raise RuntimeError(
            "failed to connect to zippy master "
            f"uri=[{endpoint}]; start zippy-master or call zippy.connect(uri=...) "
            "with the active master endpoint"
        ) from error

    _set_default_master(client, heartbeat, interval_sec)
    return client


def master() -> MasterClient:
    """
    Return the process-wide default master connection.

    :returns: The current default master client.
    :rtype: MasterClient
    :raises RuntimeError: If ``zippy.connect()`` has not been called.
    """
    if _DEFAULT_MASTER is None:
        raise RuntimeError("zippy master is not connected; call zippy.connect() first")
    _ensure_default_heartbeat()
    return _DEFAULT_MASTER


def config(master: MasterClient | None = None) -> dict[str, object]:
    """
    Return effective Zippy runtime config from master.

    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Effective runtime config.
    :rtype: dict[str, object]
    """
    return _master_config(master or _default_master())


def _list_tables(master: MasterClient | None = None) -> list[dict[str, object]]:
    """
    List tables registered in the connected Zippy master.

    This is the user-facing wrapper over the master stream registry. It returns the
    same metadata shape as :meth:`MasterClient.list_streams`, but avoids forcing Python
    users to manage a ``MasterClient`` in common monitoring code.

    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Registered table metadata entries.
    :rtype: list[dict[str, object]]
    :raises RuntimeError: If ``zippy.connect()`` has not been called or master rejects the request.
    :example:

        >>> zippy.connect()
        >>> zippy.ops.list_tables()
    """
    return list((master or _default_master()).list_streams())


def _table_info(
    table_name: str,
    *,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Return master metadata for one registered Zippy table.

    :param table_name: Named table to inspect.
    :type table_name: str
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Table metadata including schema, status, descriptors, and persist state.
    :rtype: dict[str, object]
    :raises ValueError: If ``table_name`` is empty.
    :raises RuntimeError: If the table does not exist or master rejects the request.
    :example:

        >>> zippy.connect()
        >>> zippy.ops.table_info("ctp_ticks")
    """
    if not isinstance(table_name, str):
        raise TypeError("table_name must be a string")
    if not table_name:
        raise ValueError("table_name must not be empty")
    return (master or _default_master()).get_stream(table_name)


def _table_alerts(
    table_name: str,
    *,
    master: MasterClient | None = None,
) -> list[dict[str, object]]:
    """
    Return metadata-derived health alerts for one Zippy table.

    This function does not scan data files or attach live segments. It only interprets
    master catalog metadata such as stream status, active descriptor state, and persist
    lifecycle events.

    :param table_name: Named table to inspect.
    :type table_name: str
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Health alerts ordered by control-plane importance.
    :rtype: list[dict[str, object]]
    :raises ValueError: If ``table_name`` is empty.
    :raises RuntimeError: If the table does not exist or master rejects the request.
    """
    return _build_table_alerts(_table_info(table_name, master=master))


def _table_health(
    table_name: str,
    *,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Return a compact health summary for one Zippy table.

    ``status`` is ``"error"`` when any error alert exists, ``"warning"`` when only
    warning alerts exist, and ``"ok"`` when no alerts are present.

    :param table_name: Named table to inspect.
    :type table_name: str
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Table health summary with alerts.
    :rtype: dict[str, object]
    :raises ValueError: If ``table_name`` is empty.
    :raises RuntimeError: If the table does not exist or master rejects the request.
    """
    return _table_health_from_info(_table_info(table_name, master=master), table_name)


def _table_health_from_info(
    info: dict[str, object],
    fallback_table_name: str | None = None,
) -> dict[str, object]:
    table_name = info.get("stream_name", fallback_table_name)
    alerts = _build_table_alerts(info)
    return {
        "table_name": table_name,
        "status": _table_health_status(alerts),
        "stream_status": info.get("status"),
        "descriptor_generation": info.get("descriptor_generation"),
        "alert_count": len(alerts),
        "alerts": alerts,
    }


def _build_table_alerts(info: dict[str, object]) -> list[dict[str, object]]:
    table_name = str(info.get("stream_name") or "")
    stream_status = str(info.get("status") or "unknown")
    alerts: list[dict[str, object]] = []

    if stream_status == "stale":
        alerts.append(
            {
                "severity": "error",
                "kind": "stream_stale",
                "table_name": table_name,
                "stream_status": stream_status,
                "message": f"stream is stale table_name=[{table_name}]",
            }
        )
    elif stream_status in {"error", "failed"}:
        alerts.append(
            {
                "severity": "error",
                "kind": "stream_error",
                "table_name": table_name,
                "stream_status": stream_status,
                "message": (
                    f"stream is in error status table_name=[{table_name}] "
                    f"status=[{stream_status}]"
                ),
            }
        )

    if info.get("active_segment_descriptor") is None:
        alerts.append(
            {
                "severity": "warning",
                "kind": "active_descriptor_missing",
                "table_name": table_name,
                "stream_status": stream_status,
                "message": (
                    "active segment descriptor is not published "
                    f"table_name=[{table_name}] status=[{stream_status}]"
                ),
            }
        )

    for event in info.get("persist_events", []) or []:
        if not isinstance(event, dict):
            continue
        if event.get("persist_event_type") != "persist_failed":
            continue
        alert = {
            "severity": "error",
            "kind": "persist_failed",
            "table_name": table_name,
            "message": _persist_failed_alert_message(table_name, event),
        }
        alert.update(event)
        alerts.append(alert)

    return alerts


def _persist_failed_alert_message(table_name: str, event: dict[str, object]) -> str:
    segment_id = event.get("source_segment_id")
    generation = event.get("source_generation")
    attempts = event.get("attempts")
    error = event.get("error")
    return (
        f"persist failed table_name=[{table_name}] "
        f"source_segment_id=[{segment_id}] source_generation=[{generation}] "
        f"attempts=[{attempts}] error=[{error}]"
    )


def _table_health_status(alerts: list[dict[str, object]]) -> str:
    severities = {str(alert.get("severity")) for alert in alerts}
    if "error" in severities:
        return "error"
    if "warning" in severities:
        return "warning"
    return "ok"


def _drop_table(
    table_name: str,
    *,
    drop_persisted: bool = True,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Drop a named Zippy table from master.

    :param table_name: Named table to remove.
    :type table_name: str
    :param drop_persisted: Whether to delete persisted parquet files registered for the table.
    :type drop_persisted: bool
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Drop summary returned by master.
    :rtype: dict[str, object]
    :raises RuntimeError: If master rejects the request.
    :example:

        >>> zippy.connect()
        >>> zippy.ops.drop_table("ctp_ticks")
    """
    if not table_name:
        raise ValueError("table_name must not be empty")
    return (master or _default_master()).drop_table(table_name, drop_persisted)


def _compact_table(
    table_name: str,
    *,
    min_files: int = 2,
    delete_sources: bool = True,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Compact small persisted parquet files for a named table.

    :param table_name: Named table to compact.
    :type table_name: str
    :param min_files: Minimum files in one partition group before compaction.
    :type min_files: int
    :param delete_sources: Whether to delete source parquet files after metadata replacement.
    :type delete_sources: bool
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Compaction summary.
    :rtype: dict[str, object]
    :raises ValueError: If ``table_name`` is empty or ``min_files`` is smaller than two.
    :raises RuntimeError: If master does not support persisted metadata replacement.
    """
    if not table_name:
        raise ValueError("table_name must not be empty")
    if min_files < 2:
        raise ValueError("min_files must be at least 2")

    master_client = master or _default_master()
    replace_persisted_files = getattr(master_client, "replace_persisted_files", None)
    if replace_persisted_files is None:
        raise RuntimeError("master does not support replace_persisted_files")

    stream = master_client.get_stream(table_name)
    persisted_files = [
        dict(item)
        for item in stream.get("persisted_files", []) or []
        if isinstance(item, dict) and item.get("file_path")
    ]
    groups = _persisted_compaction_groups(stream, persisted_files)
    compacted_files: list[dict[str, object]] = []
    compacted_source_keys: set[tuple[str, str]] = set()
    source_paths: list[Path] = []

    for group_files in groups.values():
        if len(group_files) < min_files:
            continue
        compacted_file = _compact_persisted_file_group(table_name, stream, group_files)
        compacted_files.append(compacted_file)
        for item in group_files:
            compacted_source_keys.add(_persisted_file_compaction_key(item))
            source_paths.append(Path(str(item["file_path"])))

    if not compacted_files:
        return {
            "table_name": table_name,
            "groups_compacted": 0,
            "files_compacted": 0,
            "rows_compacted": 0,
            "compacted_files": [],
            "source_files_deleted": 0,
            "source_file_delete_errors": [],
        }

    next_files = [
        item
        for item in persisted_files
        if _persisted_file_compaction_key(item) not in compacted_source_keys
    ]
    next_files.extend(compacted_files)
    next_files.sort(key=_persisted_file_order_key)
    replace_persisted_files(table_name, next_files)

    deleted = 0
    delete_errors: list[dict[str, str]] = []
    if delete_sources:
        for path in source_paths:
            try:
                path.unlink(missing_ok=True)
                deleted += 1
            except OSError as error:
                delete_errors.append({"file_path": str(path), "error": str(error)})

    return {
        "table_name": table_name,
        "groups_compacted": len(compacted_files),
        "files_compacted": len(compacted_source_keys),
        "rows_compacted": sum(int(item.get("row_count", 0)) for item in compacted_files),
        "compacted_files": compacted_files,
        "source_files_deleted": deleted,
        "source_file_delete_errors": delete_errors,
    }


def _compact_tables(
    table_names: object = None,
    *,
    min_files: int = 2,
    delete_sources: bool = True,
    continue_on_error: bool = False,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Compact persisted parquet files for multiple tables.

    :param table_names: Table name, iterable of table names, or ``None`` to discover
        persisted tables from master.
    :type table_names: object
    :param min_files: Minimum files in one partition group before compaction.
    :type min_files: int
    :param delete_sources: Whether to delete source parquet files after metadata replacement.
    :type delete_sources: bool
    :param continue_on_error: Whether to keep compacting other tables after one table fails.
    :type continue_on_error: bool
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :returns: Multi-table compaction summary.
    :rtype: dict[str, object]
    """
    if min_files < 2:
        raise ValueError("min_files must be at least 2")

    master_client = master or _default_master()
    resolved_table_names = _resolve_compaction_table_names(table_names, master_client)
    table_results: list[dict[str, object]] = []
    table_errors: list[dict[str, str]] = []

    for table_name in resolved_table_names:
        try:
            table_results.append(
                _compact_table(
                    table_name,
                    min_files=min_files,
                    delete_sources=delete_sources,
                    master=master_client,
                )
            )
        except Exception as error:
            if not continue_on_error:
                raise
            table_errors.append({"table_name": table_name, "error": str(error)})

    return {
        "tables_scanned": len(resolved_table_names),
        "tables_compacted": sum(
            1 for result in table_results if int(result.get("groups_compacted", 0)) > 0
        ),
        "groups_compacted": sum(
            int(result.get("groups_compacted", 0)) for result in table_results
        ),
        "files_compacted": sum(
            int(result.get("files_compacted", 0)) for result in table_results
        ),
        "rows_compacted": sum(
            int(result.get("rows_compacted", 0)) for result in table_results
        ),
        "table_results": table_results,
        "table_errors": table_errors,
    }


def _resolve_compaction_table_names(
    table_names: object,
    master: MasterClient,
) -> list[str]:
    if table_names is None:
        list_streams = getattr(master, "list_streams", None)
        if list_streams is None:
            raise RuntimeError("master does not support list_streams")
        names = [
            str(item["stream_name"])
            for item in list_streams()
            if item.get("stream_name") and item.get("persisted_files")
        ]
        return sorted(dict.fromkeys(names))

    if isinstance(table_names, str):
        names = [table_names]
    else:
        try:
            names = [str(item) for item in table_names]  # type: ignore[operator]
        except TypeError as error:
            raise TypeError("table_names must be a table name or iterable of table names") from error

    names = [name for name in names if name]
    if not names:
        raise ValueError("table_names must not be empty")
    return list(dict.fromkeys(names))


class _CompactionWorker:
    """
    Background worker for low-frequency persisted parquet compaction.

    The worker is intentionally separate from StreamTable writers. Writers publish
    persisted metadata; this worker periodically reads master catalog state and
    performs metadata-replacing compaction through ops APIs.

    :param table_names: Optional table name or iterable of table names. ``None`` means
        discover persisted tables from master on every iteration.
    :type table_names: object
    :param interval_sec: Seconds between compaction passes.
    :type interval_sec: float
    :param min_files: Minimum files in one partition group before compaction.
    :type min_files: int
    :param delete_sources: Whether to delete source parquet files after metadata replacement.
    :type delete_sources: bool
    :param master: Master client used for catalog operations.
    :type master: MasterClient
    """

    def __init__(
        self,
        table_names: object = None,
        *,
        interval_sec: float = 60.0,
        min_files: int = 4,
        delete_sources: bool = True,
        master: MasterClient,
    ) -> None:
        interval = float(interval_sec)
        if interval <= 0:
            raise ValueError("interval_sec must be positive")
        if min_files < 2:
            raise ValueError("min_files must be at least 2")

        self.table_names = _freeze_compaction_table_names(table_names)
        self.interval_sec = interval
        self.min_files = int(min_files)
        self.delete_sources = bool(delete_sources)
        self.master = master
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._reports: list[dict[str, object]] = []
        self._errors: list[dict[str, str]] = []
        self._thread = threading.Thread(
            target=self._run,
            name="zippy-compaction-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """
        Request the compaction worker to stop.

        :returns: None
        :rtype: None
        """
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        """
        Wait until the worker thread exits.

        :param timeout: Optional maximum seconds to wait.
        :type timeout: float | None
        :returns: None
        :rtype: None
        """
        self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        """
        Return whether the worker thread is alive.

        :returns: True if the thread is alive.
        :rtype: bool
        """
        return self._thread.is_alive()

    def latest_report(self) -> dict[str, object] | None:
        """
        Return the latest successful compaction pass report.

        :returns: Latest report, or ``None`` before the first pass completes.
        :rtype: dict[str, object] | None
        """
        with self._lock:
            if not self._reports:
                return None
            return dict(self._reports[-1])

    def reports(self) -> list[dict[str, object]]:
        """
        Return all successful compaction pass reports recorded by this worker.

        :returns: Report list snapshot.
        :rtype: list[dict[str, object]]
        """
        with self._lock:
            return [dict(report) for report in self._reports]

    def errors(self) -> list[dict[str, str]]:
        """
        Return unexpected worker-level errors.

        Per-table compaction errors are stored inside each report's ``table_errors``.

        :returns: Error list snapshot.
        :rtype: list[dict[str, str]]
        """
        with self._lock:
            return [dict(error) for error in self._errors]

    def run_once(self) -> dict[str, object]:
        """
        Run one compaction pass synchronously.

        :returns: Compaction pass report.
        :rtype: dict[str, object]
        """
        return _compact_tables(
            self.table_names,
            min_files=self.min_files,
            delete_sources=self.delete_sources,
            continue_on_error=True,
            master=self.master,
        )

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                report = self.run_once()
            except Exception as error:
                with self._lock:
                    self._errors.append({"error": str(error)})
            else:
                with self._lock:
                    self._reports.append(report)

            if self._stop_event.wait(self.interval_sec):
                break


def _freeze_compaction_table_names(table_names: object) -> object:
    if table_names is None or isinstance(table_names, str):
        return table_names
    try:
        names = tuple(str(item) for item in table_names)  # type: ignore[operator]
    except TypeError as error:
        raise TypeError("table_names must be a table name or iterable of table names") from error
    if not names:
        raise ValueError("table_names must not be empty")
    return names


def _persisted_compaction_groups(
    stream: dict[str, object],
    persisted_files: list[dict[str, object]],
) -> dict[str, list[dict[str, object]]]:
    groups: dict[str, list[dict[str, object]]] = {}
    live_identities = _live_segment_identities(stream)
    for item in persisted_files:
        identities = _persisted_segment_identities(item)
        if identities and identities & live_identities:
            continue
        path = Path(str(item["file_path"]))
        if not path.exists():
            continue
        key = _persisted_compaction_group_key(item, path)
        groups.setdefault(key, []).append(item)
    for group_files in groups.values():
        group_files.sort(key=_persisted_file_order_key)
    return groups


def _persisted_compaction_group_key(item: dict[str, object], path: Path) -> str:
    partition_path = item.get("partition_path")
    if partition_path:
        return f"partition_path:{partition_path}"
    partition = item.get("partition")
    if isinstance(partition, dict) and partition:
        return f"partition:{json.dumps(partition, sort_keys=True, separators=(',', ':'))}"
    return f"parent:{path.parent}"


def _compact_persisted_file_group(
    table_name: str,
    stream: dict[str, object],
    group_files: list[dict[str, object]],
) -> dict[str, object]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    tables = [_read_persisted_parquet_file(item["file_path"]) for item in group_files]
    table = pa.concat_tables([table for table in tables if table.num_rows > 0])
    first_file = group_files[0]
    target_dir = Path(str(first_file["file_path"])).parent
    target_path = target_dir / _compacted_parquet_file_name(table_name, group_files)
    temp_path = target_path.with_name(
        f"{target_path.name}.tmp-{os.getpid()}-{time.time_ns()}"
    )
    pq.write_table(table, temp_path)
    os.replace(temp_path, target_path)
    return _compacted_persisted_file_metadata(
        table_name,
        stream,
        group_files,
        target_path,
        table.num_rows,
    )


def _compacted_parquet_file_name(
    table_name: str,
    group_files: list[dict[str, object]],
) -> str:
    digest = hashlib.sha1(
        "|".join(str(item.get("persist_file_id") or item.get("file_path")) for item in group_files)
        .encode("utf-8")
    ).hexdigest()[:16]
    return f"compact-{_safe_file_token(table_name)}-{digest}.parquet"


def _compacted_persisted_file_metadata(
    table_name: str,
    stream: dict[str, object],
    group_files: list[dict[str, object]],
    target_path: Path,
    row_count: int,
) -> dict[str, object]:
    first_file = group_files[0]
    source_segments = [
        {
            "source_segment_id": identity[0],
            "source_generation": identity[1],
        }
        for item in group_files
        for identity in sorted(_persisted_segment_identities(item))
    ]
    digest = hashlib.sha1(
        "|".join(str(item.get("persist_file_id") or item.get("file_path")) for item in group_files)
        .encode("utf-8")
    ).hexdigest()[:16]
    metadata: dict[str, object] = {
        "persist_file_id": f"compact:{table_name}:{digest}",
        "stream_name": table_name,
        "schema_hash": stream.get("schema_hash"),
        "file_path": str(target_path),
        "row_count": row_count,
        "created_at": int(time.time() * 1000),
        "compacted": True,
        "compacted_file_count": len(group_files),
        "compacted_source_file_ids": [
            item.get("persist_file_id") or item.get("file_path") for item in group_files
        ],
    }
    if source_segments:
        metadata["source_segments"] = source_segments
        metadata["source_segment_id"] = source_segments[0]["source_segment_id"]
        metadata["source_generation"] = source_segments[0]["source_generation"]
    for key in ("partition", "partition_path", "partition_spec"):
        if key in first_file:
            metadata[key] = first_file[key]
    return metadata


def _persisted_file_compaction_key(item: dict[str, object]) -> tuple[str, str]:
    persist_file_id = item.get("persist_file_id")
    if persist_file_id:
        return ("id", str(persist_file_id))
    return ("path", str(item.get("file_path", "")))


def _safe_file_token(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


class Ops:
    """
    Namespace for low-frequency Zippy operations.

    These methods intentionally live under ``zippy.ops`` so the top-level API remains
    focused on high-frequency user workflows such as ``read_table`` and ``subscribe``.
    """

    def list_tables(self, master: MasterClient | None = None) -> list[dict[str, object]]:
        """
        List tables registered in the connected Zippy master.

        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Registered table metadata entries.
        :rtype: list[dict[str, object]]
        """
        return _list_tables(master=master)

    def table_info(
        self,
        table_name: str,
        *,
        master: MasterClient | None = None,
    ) -> dict[str, object]:
        """
        Return master metadata for one registered Zippy table.

        :param table_name: Named table to inspect.
        :type table_name: str
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Table metadata including schema, status, descriptors, and persist state.
        :rtype: dict[str, object]
        """
        return _table_info(table_name, master=master)

    def table_alerts(
        self,
        table_name: str,
        *,
        master: MasterClient | None = None,
    ) -> list[dict[str, object]]:
        """
        Return metadata-derived health alerts for one Zippy table.

        :param table_name: Named table to inspect.
        :type table_name: str
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Health alerts ordered by control-plane importance.
        :rtype: list[dict[str, object]]
        """
        return _table_alerts(table_name, master=master)

    def table_health(
        self,
        table_name: str,
        *,
        master: MasterClient | None = None,
    ) -> dict[str, object]:
        """
        Return a compact health summary for one Zippy table.

        :param table_name: Named table to inspect.
        :type table_name: str
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Table health summary with alerts.
        :rtype: dict[str, object]
        """
        return _table_health(table_name, master=master)

    def drop_table(
        self,
        table_name: str,
        *,
        drop_persisted: bool = True,
        master: MasterClient | None = None,
    ) -> dict[str, object]:
        """
        Drop a named Zippy table from master.

        :param table_name: Named table to remove.
        :type table_name: str
        :param drop_persisted: Whether to delete persisted parquet files registered for the table.
        :type drop_persisted: bool
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Drop summary returned by master.
        :rtype: dict[str, object]
        """
        return _drop_table(table_name, drop_persisted=drop_persisted, master=master)

    def compact_table(
        self,
        table_name: str,
        *,
        min_files: int = 2,
        delete_sources: bool = True,
        master: MasterClient | None = None,
    ) -> dict[str, object]:
        """
        Compact small persisted parquet files for one table.

        :param table_name: Named table to compact.
        :type table_name: str
        :param min_files: Minimum files in one partition group before compaction.
        :type min_files: int
        :param delete_sources: Whether to delete source parquet files after metadata replacement.
        :type delete_sources: bool
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Compaction summary.
        :rtype: dict[str, object]
        """
        return _compact_table(
            table_name,
            min_files=min_files,
            delete_sources=delete_sources,
            master=master,
        )

    def compact_tables(
        self,
        table_names: object = None,
        *,
        min_files: int = 2,
        delete_sources: bool = True,
        continue_on_error: bool = False,
        master: MasterClient | None = None,
    ) -> dict[str, object]:
        """
        Compact small persisted parquet files for multiple tables.

        :param table_names: Table name, iterable of table names, or ``None`` to discover
            persisted tables from master.
        :type table_names: object
        :param min_files: Minimum files in one partition group before compaction.
        :type min_files: int
        :param delete_sources: Whether to delete source parquet files after metadata replacement.
        :type delete_sources: bool
        :param continue_on_error: Whether to keep compacting other tables after one table fails.
        :type continue_on_error: bool
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Multi-table compaction summary.
        :rtype: dict[str, object]
        """
        return _compact_tables(
            table_names,
            min_files=min_files,
            delete_sources=delete_sources,
            continue_on_error=continue_on_error,
            master=master,
        )

    def start_compaction_worker(
        self,
        table_names: object = None,
        *,
        interval_sec: float = 60.0,
        min_files: int = 4,
        delete_sources: bool = True,
        master: MasterClient | None = None,
    ):
        """
        Start a background worker that periodically compacts persisted parquet files.

        This worker is an ops-side runtime helper. StreamTable writers remain responsible
        only for writing persisted files and publishing metadata; compaction reads master
        catalog state and replaces persisted metadata through the control plane.

        :param table_names: Table name, iterable of table names, or ``None`` to discover
            persisted tables from master on every pass.
        :type table_names: object
        :param interval_sec: Seconds between compaction passes.
        :type interval_sec: float
        :param min_files: Minimum files in one partition group before compaction.
        :type min_files: int
        :param delete_sources: Whether to delete source parquet files after metadata replacement.
        :type delete_sources: bool
        :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
        :type master: MasterClient | None
        :returns: Background compaction worker handle.
        :rtype: object
        """
        return _CompactionWorker(
            table_names,
            interval_sec=interval_sec,
            min_files=min_files,
            delete_sources=delete_sources,
            master=master or _default_master(),
        )


ops = Ops()


def _default_master() -> MasterClient:
    return master()


def _master_uri_for_error(master: MasterClient) -> str:
    try:
        return str(master.control_endpoint())
    except Exception:
        return "<unknown>"


def _resolve_uri(uri: str) -> str:
    if uri.startswith("zippy+tcp://"):
        raise ValueError(
            "zippy+tcp:// uri is no longer supported; use "
            "zippy://host:port/profile to connect to a remote master"
        )
    if uri.startswith("tcp://"):
        return uri
    remote_master_endpoint = _remote_master_endpoint_from_zippy_uri(uri)
    if remote_master_endpoint is not None:
        return f"tcp://{remote_master_endpoint}"
    if os.name == "nt" and not _looks_like_uri_path(uri):
        if uri.startswith("zippy://"):
            return uri
        return f"zippy://{uri or 'default'}"
    if uri.startswith("unix://"):
        return _resolve_uri_path(uri.removeprefix("unix://"))
    if uri.startswith("file://"):
        return _resolve_uri_path(uri.removeprefix("file://"))
    if uri.startswith("zippy://"):
        return _logical_control_endpoint_path(uri.removeprefix("zippy://"))
    if _looks_like_uri_path(uri):
        return _resolve_uri_path(uri)
    return _logical_control_endpoint_path(uri)


def _resolve_uri_path(uri: str) -> str:
    if uri.startswith("~/"):
        return str(_home_dir() / uri[2:])
    if uri.startswith("~\\"):
        return str(_home_dir()) + "\\" + uri[2:]
    return str(Path(uri).expanduser())


def _logical_control_endpoint_path(name: str) -> str:
    endpoint_name = name or "default"
    return str(
        _home_dir()
        / ".zippy"
        / "control_endpoints"
        / endpoint_name
        / "master.sock"
    )


def _home_dir() -> Path:
    if home := os.environ.get("HOME"):
        return Path(home)
    if user_profile := os.environ.get("USERPROFILE"):
        return Path(user_profile)
    if (drive := os.environ.get("HOMEDRIVE")) and (path := os.environ.get("HOMEPATH")):
        return Path(drive + path)
    return Path(tempfile.gettempdir())


def _looks_like_uri_path(uri: str) -> bool:
    if uri.startswith("zippy://"):
        return False
    return (
        uri.startswith("/")
        or uri.startswith("~/")
        or uri.startswith("~\\")
        or uri.startswith("./")
        or uri.startswith("../")
        or "\\" in uri
        or "/" in uri
        or (len(uri) > 1 and uri[1] == ":")
        or uri.endswith(".sock")
    )


def _validate_heartbeat_interval(interval_sec: float) -> float:
    interval = float(interval_sec)
    if not interval > 0:
        raise ValueError("heartbeat_interval_sec must be positive")
    return interval


def _parse_timeout_seconds(timeout: float | str | None) -> float | None:
    if timeout is None:
        return None
    if isinstance(timeout, str):
        text = timeout.strip().lower()
        if text.endswith("ms"):
            value = float(text[:-2])
            seconds = value / 1000.0
        elif text.endswith("s"):
            seconds = float(text[:-1])
        elif text.endswith("m"):
            seconds = float(text[:-1]) * 60.0
        else:
            seconds = float(text)
    else:
        seconds = float(timeout)
    if seconds < 0:
        raise ValueError("timeout must be non-negative")
    return seconds


def _wait_for_table_ready(
    source: str,
    master: MasterClient,
    timeout: float | str | None,
) -> None:
    timeout_sec = _parse_timeout_seconds(timeout)
    deadline = None if timeout_sec is None else time.monotonic() + timeout_sec
    last_error: BaseException | None = None

    while True:
        try:
            stream = master.get_stream(source)
        except RuntimeError as error:
            if "stream not found" not in str(error):
                raise
            last_error = error
        else:
            if stream.get("data_path") != "segment":
                return
            if stream.get("status") != "stale" and stream.get("active_segment_descriptor"):
                return
            last_error = RuntimeError(
                "table is not ready "
                f"source=[{source}] status=[{stream.get('status')}]"
            )

        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"timed out waiting for table source=[{source}] "
                    f"master_uri=[{_master_uri_for_error(master)}]"
                ) from last_error
            sleep_sec = min(0.01, remaining)
        else:
            sleep_sec = 0.01
        time.sleep(sleep_sec)


def _set_default_master(
    client: MasterClient,
    heartbeat: _HeartbeatHandle | None,
    heartbeat_interval_sec: float,
) -> None:
    global _DEFAULT_HEARTBEAT
    global _DEFAULT_HEARTBEAT_INTERVAL_SEC
    global _DEFAULT_MASTER

    with _DEFAULT_HEARTBEAT_LOCK:
        old_heartbeat = _DEFAULT_HEARTBEAT
        _DEFAULT_MASTER = client
        _DEFAULT_HEARTBEAT = heartbeat
        _DEFAULT_HEARTBEAT_INTERVAL_SEC = heartbeat_interval_sec

    if old_heartbeat is not None:
        old_heartbeat.stop()


def _ensure_default_heartbeat() -> None:
    global _DEFAULT_HEARTBEAT

    old_heartbeat = None
    with _DEFAULT_HEARTBEAT_LOCK:
        if _DEFAULT_MASTER is None:
            return
        if _DEFAULT_HEARTBEAT is not None and _DEFAULT_HEARTBEAT.master is _DEFAULT_MASTER:
            return

        process_id = getattr(_DEFAULT_MASTER, "process_id", lambda: None)()
        if process_id is None:
            return

        old_heartbeat = _DEFAULT_HEARTBEAT
        _DEFAULT_HEARTBEAT = _HeartbeatHandle(
            _DEFAULT_MASTER,
            _DEFAULT_HEARTBEAT_INTERVAL_SEC,
        )

    if old_heartbeat is not None:
        old_heartbeat.stop()


def _ensure_master_process(master: MasterClient, app: str) -> None:
    process_id = getattr(master, "process_id", None)
    register_process = getattr(master, "register_process", None)
    if process_id is None or register_process is None:
        return

    if process_id() is None:
        register_process(app)
    if master is _DEFAULT_MASTER:
        _ensure_default_heartbeat()


def _stop_default_heartbeat() -> None:
    global _DEFAULT_HEARTBEAT

    with _DEFAULT_HEARTBEAT_LOCK:
        heartbeat = _DEFAULT_HEARTBEAT
        _DEFAULT_HEARTBEAT = None

    if heartbeat is not None:
        heartbeat.stop()


def _reset_default_master_for_test() -> None:
    global _DEFAULT_HEARTBEAT_INTERVAL_SEC
    global _DEFAULT_MASTER

    _stop_default_heartbeat()
    _DEFAULT_HEARTBEAT_INTERVAL_SEC = _DEFAULT_HEARTBEAT_INTERVAL_DEFAULT_SEC
    _DEFAULT_MASTER = None


_REMOTE_FRAME_HEADER = struct.Struct("!IQ")


def _normalize_remote_endpoint(endpoint: str) -> str:
    value = str(endpoint)
    if value.startswith("tcp://"):
        value = value.removeprefix("tcp://")
    if "/" in value:
        value = value.split("/", 1)[0]
    return value


def _remote_master_endpoint_from_zippy_uri(uri: str) -> str | None:
    if not uri.startswith("zippy://"):
        return None
    body = uri.removeprefix("zippy://")
    authority = body.split("/", 1)[0]
    if not authority or ":" not in authority:
        return None
    return authority


def _remote_gateway_endpoint(master: object) -> str | None:
    endpoint = getattr(master, "remote_gateway_endpoint", None)
    if callable(endpoint):
        value = endpoint()
        return _normalize_remote_endpoint(value) if value else None
    value = getattr(master, "remote_gateway_endpoint", None)
    if isinstance(value, str):
        return _normalize_remote_endpoint(value)
    get_config = getattr(master, "get_config", None)
    if callable(get_config):
        remote = get_config().get("remote_gateway", {})
        if isinstance(remote, dict) and remote.get("enabled") is False:
            return None
        if isinstance(remote, dict) and remote.get("endpoint"):
            return _normalize_remote_endpoint(str(remote["endpoint"]))
    return None


def _remote_gateway_token(master: object) -> str | None:
    token = getattr(master, "remote_gateway_token", None)
    if callable(token):
        value = token()
        return str(value) if value else None
    value = getattr(master, "remote_gateway_token", None)
    if isinstance(value, str):
        return value
    get_config = getattr(master, "get_config", None)
    if callable(get_config):
        remote = get_config().get("remote_gateway", {})
        if isinstance(remote, dict) and remote.get("token"):
            return str(remote["token"])
    return None


def _is_remote_master(master: object) -> bool:
    return _remote_gateway_endpoint(master) is not None


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("remote gateway connection closed while reading frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_remote_frame(
    sock: socket.socket,
    header: dict[str, object],
    payload: bytes = b"",
) -> None:
    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    sock.sendall(_REMOTE_FRAME_HEADER.pack(len(header_bytes), len(payload)))
    sock.sendall(header_bytes)
    if payload:
        sock.sendall(payload)


def _recv_remote_frame(sock: socket.socket) -> tuple[dict[str, object], bytes]:
    prefix = _recv_exact(sock, _REMOTE_FRAME_HEADER.size)
    header_len, payload_len = _REMOTE_FRAME_HEADER.unpack(prefix)
    header = json.loads(_recv_exact(sock, header_len).decode("utf-8"))
    payload = _recv_exact(sock, payload_len) if payload_len else b""
    return header, payload


def _remote_request(
    endpoint: str,
    header: dict[str, object],
    payload: bytes = b"",
    *,
    token: str | None = None,
    timeout_sec: float = 5.0,
) -> tuple[dict[str, object], bytes]:
    host, port = _parse_remote_endpoint(endpoint)
    if token is not None:
        header = dict(header)
        header["token"] = token
    with socket.create_connection((host, port), timeout=timeout_sec) as sock:
        _send_remote_frame(sock, header, payload)
        response, response_payload = _recv_remote_frame(sock)
    if response.get("status") != "ok":
        reason = response.get("reason", "unknown remote gateway error")
        raise RuntimeError(f"remote gateway request failed reason=[{reason}]")
    return response, response_payload


def _remote_master_get_config(endpoint: str, timeout_sec: float = 5.0) -> dict[str, object]:
    host, port = _parse_remote_endpoint(endpoint)
    with socket.create_connection((host, port), timeout=timeout_sec) as sock:
        sock.sendall(b'{"GetConfig":{}}\n')
        response_line = b""
        while not response_line.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_line += chunk
    if not response_line:
        raise RuntimeError(f"remote master returned empty response endpoint=[{endpoint}]")
    response = json.loads(response_line.decode("utf-8"))
    if "Error" in response:
        reason = response["Error"].get("reason", "remote master get_config failed")
        raise RuntimeError(str(reason))
    return dict(response["ConfigFetched"]["config"])


def _parse_remote_endpoint(endpoint: str) -> tuple[str, int]:
    normalized = _normalize_remote_endpoint(endpoint)
    host, port_text = normalized.rsplit(":", 1)
    return host, int(port_text)


def _pyarrow_table_to_ipc(table: object) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _pyarrow_table_from_ipc(payload: bytes):
    import pyarrow as pa

    return pa.ipc.open_stream(pa.py_buffer(payload)).read_all()


def _pyarrow_schema_to_ipc(schema: object) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass
    return sink.getvalue().to_pybytes()


def _pyarrow_schema_from_ipc(payload: bytes):
    import pyarrow as pa

    return pa.ipc.open_stream(pa.py_buffer(payload)).schema


def _is_scalar_row_dict(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    return not any(isinstance(item, (list, tuple)) for item in value.values())


def _value_to_pyarrow_table(value: object, schema: object | None = None):
    import pyarrow as pa

    if isinstance(value, pa.Table):
        return value.cast(schema) if schema is not None and value.schema != schema else value
    if isinstance(value, pa.RecordBatch):
        table = pa.Table.from_batches([value])
        return table.cast(schema) if schema is not None else table
    if isinstance(value, dict):
        if _is_scalar_row_dict(value):
            return pa.Table.from_pylist([value], schema=schema)
        return pa.table(value, schema=schema)
    if isinstance(value, list):
        return pa.Table.from_pylist(value, schema=schema)
    to_arrow = getattr(value, "to_arrow", None)
    if callable(to_arrow):
        return _value_to_pyarrow_table(to_arrow(), schema)
    raise TypeError(
        "writer value must be a row dict, list[dict], pyarrow.Table, or pyarrow.RecordBatch"
    )


class RemoteMasterClient:
    """
    Lightweight remote master facade backed by a Zippy GatewayServer endpoint.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        token: str | None = None,
        master_endpoint: str | None = None,
    ) -> None:
        self._endpoint = _normalize_remote_endpoint(endpoint)
        self._token = token
        self._master_endpoint = (
            _normalize_remote_endpoint(master_endpoint)
            if master_endpoint is not None
            else None
        )
        self._process_id: str | None = None

    @classmethod
    def from_master_endpoint(cls, endpoint: str) -> "RemoteMasterClient":
        """
        Create a remote client by reading Gateway capability from a TCP master.

        :param endpoint: Remote master endpoint without the ``zippy://`` prefix.
        :type endpoint: str
        :returns: Remote master facade backed by the advertised GatewayServer.
        :rtype: RemoteMasterClient
        """
        config = _remote_master_get_config(endpoint)
        remote = config.get("remote_gateway", {})
        if not isinstance(remote, dict) or not remote.get("endpoint"):
            raise RuntimeError(
                "remote master does not advertise remote_gateway.endpoint "
                f"endpoint=[{endpoint}]"
            )
        token = str(remote["token"]) if remote.get("token") else None
        return cls(str(remote["endpoint"]), token=token, master_endpoint=endpoint)

    def remote_gateway_endpoint(self) -> str:
        return self._endpoint

    def remote_gateway_token(self) -> str | None:
        return self._token

    def control_endpoint(self) -> str:
        endpoint = self._master_endpoint or self._endpoint
        return f"zippy://{endpoint}/default"

    def process_id(self) -> str | None:
        return self._process_id

    def register_process(self, app: str) -> str:
        self._process_id = f"remote.{app}"
        return self._process_id

    def heartbeat(self) -> None:
        return None

    def list_streams(self) -> list[dict[str, object]]:
        response, _ = _remote_request(
            self._endpoint,
            {"kind": "list_streams"},
            token=self._token,
        )
        return list(response.get("streams", []))

    def get_stream(self, source: str) -> dict[str, object]:
        response, _ = _remote_request(
            self._endpoint,
            {"kind": "get_stream", "source": str(source)},
            token=self._token,
        )
        return dict(response.get("stream", {}))

    def get_stream_schema(self, source: str):
        _, payload = _remote_request(
            self._endpoint,
            {"kind": "get_stream", "source": str(source)},
            token=self._token,
        )
        return _pyarrow_schema_from_ipc(payload)

    def get_config(self) -> dict[str, object]:
        return {
            "remote_gateway": {
                "enabled": True,
                "endpoint": self._endpoint,
                "token": self._token,
                "protocol_version": 1,
            }
        }

    def gateway_metrics(self) -> dict[str, object]:
        response, _ = _remote_request(
            self._endpoint,
            {"kind": "metrics"},
            token=self._token,
        )
        return dict(response.get("metrics", {}))

    def collect(
        self,
        source: str,
        plan: list[dict[str, object]],
        *,
        snapshot: bool = True,
    ):
        _, payload = _remote_request(
            self._endpoint,
            {
                "kind": "collect",
                "source": str(source),
                "plan": plan,
                "snapshot": bool(snapshot),
            },
            token=self._token,
        )
        return _pyarrow_table_from_ipc(payload)


class _RemoteQuery:
    def __init__(
        self,
        source: str,
        master: object,
        *,
        endpoint: str | None = None,
        token: str | None = None,
    ) -> None:
        self.source = source
        self.master = master
        self.endpoint = endpoint or _remote_gateway_endpoint(master)
        self.token = token if token is not None else _remote_gateway_token(master)
        if self.endpoint is None:
            raise RuntimeError(f"remote gateway endpoint missing source=[{source}]")

    def schema(self):
        get_stream_schema = getattr(self.master, "get_stream_schema", None)
        if callable(get_stream_schema):
            return get_stream_schema(self.source)
        _, payload = _remote_request(
            self.endpoint,
            {"kind": "get_stream", "source": self.source},
            token=self.token,
        )
        return _pyarrow_schema_from_ipc(payload)

    def stream_info(self) -> dict[str, object]:
        get_stream = getattr(self.master, "get_stream", None)
        if callable(get_stream):
            return get_stream(self.source)
        response, _ = _remote_request(
            self.endpoint,
            {"kind": "get_stream", "source": self.source},
            token=self.token,
        )
        return dict(response.get("stream", {}))

    def snapshot(self) -> dict[str, object]:
        return {
            "stream_name": self.source,
            "data_path": "remote_gateway",
            "remote_gateway_endpoint": self.endpoint,
        }

    def tail(self, n: int):
        table = self.collect_plan([], snapshot=True)
        if n == 0:
            return table.slice(0, 0)
        return table.slice(max(0, table.num_rows - n), n)

    def collect_plan(self, plan: list[dict[str, object]], *, snapshot: bool):
        _, payload = _remote_request(
            self.endpoint,
            {
                "kind": "collect",
                "source": str(self.source),
                "plan": plan,
                "snapshot": bool(snapshot),
            },
            token=self.token,
        )
        return _pyarrow_table_from_ipc(payload)


class RemoteGatewayWriter:
    """
    Remote writer that accepts row writes and sends Arrow IPC batches to a gateway.
    """

    def __init__(
        self,
        stream_name: str,
        *,
        endpoint: str,
        schema=None,
        batch_size: int = 1024,
        flush_interval_ms: int | None = 5,
        token: str | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.stream_name = str(stream_name)
        self.endpoint = _normalize_remote_endpoint(endpoint)
        self.schema = schema
        self.batch_size = int(batch_size)
        self.flush_interval_ms = flush_interval_ms
        self.token = token
        self._rows: list[dict[str, object]] = []
        self._last_flush_ns = time.perf_counter_ns()
        self._closed = False

    def write(self, value: object) -> None:
        if self._closed:
            raise RuntimeError("remote writer is closed")
        if _is_scalar_row_dict(value):
            self._rows.append(dict(value))
            if len(self._rows) >= self.batch_size or self._flush_interval_elapsed():
                self.flush()
            return
        self.flush()
        self._send_table(_value_to_pyarrow_table(value, self.schema))

    def flush(self) -> None:
        if not self._rows:
            return
        table = _value_to_pyarrow_table(list(self._rows), self.schema)
        self._rows = []
        self._send_table(table)
        self._last_flush_ns = time.perf_counter_ns()

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        self._closed = True

    def _flush_interval_elapsed(self) -> bool:
        if self.flush_interval_ms is None:
            return False
        elapsed_ns = time.perf_counter_ns() - self._last_flush_ns
        return elapsed_ns >= int(self.flush_interval_ms) * 1_000_000

    def _send_table(self, table: object) -> None:
        _remote_request(
            self.endpoint,
            {
                "kind": "write_batch",
                "stream_name": self.stream_name,
                "rows": int(table.num_rows),
            },
            _pyarrow_table_to_ipc(table),
            token=self.token,
        )


class RemoteStreamSubscriber:
    """
    Remote subscriber backed by a persistent GatewayServer table stream.
    """

    def __init__(
        self,
        source: str,
        *,
        endpoint: str,
        callback,
        table_callback: bool = False,
        filter: object | None = None,
        batch_size: int | None = None,
        throttle_ms: int | None = None,
        count: int | None = None,
        token: str | None = None,
        reconnect: bool = True,
        reconnect_interval_ms: int = 100,
    ) -> None:
        self.source = str(source)
        self.endpoint = _normalize_remote_endpoint(endpoint)
        self.callback = callback
        self.table_callback = bool(table_callback)
        self.filter = filter
        self.batch_size = batch_size
        self.throttle_ms = throttle_ms
        self.count = count
        self.token = token
        self.reconnect = bool(reconnect)
        if reconnect_interval_ms <= 0:
            raise ValueError("reconnect_interval_ms must be positive")
        self.reconnect_interval_ms = int(reconnect_interval_ms)
        self._stop_event = threading.Event()
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._reconnects_total = 0

    def start(self) -> "RemoteStreamSubscriber":
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-remote-subscriber-{self.source}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        if self._socket is not None:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._socket.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        if self._error is not None:
            raise self._error

    def join(self) -> None:
        if self._thread is not None:
            self._thread.join()
        if self._error is not None:
            raise self._error

    def metrics(self) -> dict[str, object]:
        return {
            "source": self.source,
            "remote_gateway_endpoint": self.endpoint,
            "running": self._thread is not None and self._thread.is_alive(),
            "reconnects_total": self._reconnects_total,
        }

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
                return
            except OSError:
                if self._stop_event.is_set():
                    return
                if not self.reconnect:
                    self._error = RuntimeError("remote subscriber connection closed")
                    return
                self._reconnects_total += 1
                self._stop_event.wait(self.reconnect_interval_ms / 1000.0)
            except RuntimeError as error:
                if self._is_retryable_runtime_error(error):
                    if self._stop_event.is_set():
                        return
                    if not self.reconnect:
                        self._error = error
                        return
                    self._reconnects_total += 1
                    self._stop_event.wait(self.reconnect_interval_ms / 1000.0)
                    continue
                if not self._stop_event.is_set():
                    self._error = error
                return
            except BaseException as error:
                if not self._stop_event.is_set():
                    self._error = error
                return

    def _run_once(self) -> None:
        host, port = _parse_remote_endpoint(self.endpoint)
        with socket.create_connection((host, port), timeout=5.0) as sock:
            self._socket = sock
            request = {
                "kind": "subscribe_table",
                "source": self.source,
                "filter": (
                    _query_expr_to_json(self.filter) if self.filter is not None else None
                ),
                "batch_size": self.batch_size,
                "throttle_ms": self.throttle_ms,
                "count": self.count,
            }
            if self.token is not None:
                request["token"] = self.token
            _send_remote_frame(sock, request)
            while not self._stop_event.is_set():
                header, payload = _recv_remote_frame(sock)
                if header.get("status") != "ok":
                    raise RuntimeError(header.get("reason", "remote subscribe failed"))
                if header.get("kind") == "subscribed":
                    continue
                if header.get("kind") != "table":
                    continue
                table = _pyarrow_table_from_ipc(payload)
                if self.table_callback:
                    self.callback(table)
                else:
                    for row in table.to_pylist():
                        self.callback(Row(row))

    @staticmethod
    def _is_retryable_runtime_error(error: RuntimeError) -> bool:
        message = str(error)
        return "remote gateway connection closed" in message


class _LocalGatewayWriter:
    def __init__(self, stream_name: str, schema: object, master: MasterClient | None) -> None:
        self._pipeline = (
            Pipeline(f"remote_gateway.{stream_name}.{os.getpid()}", master=master)
            .stream_table(stream_name, schema=schema)
            .start()
        )

    def write(self, table: object) -> None:
        self._pipeline.write(table)

    def flush(self) -> None:
        self._pipeline.flush()

    def close(self) -> None:
        self._pipeline.stop()


class GatewayServer:
    """
    Minimal TCP gateway for remote writes and lazy collect requests.
    """

    def __init__(
        self,
        endpoint: str = "127.0.0.1:17666",
        *,
        master: MasterClient | None = None,
        writer_factory=None,
        stream_info_provider=None,
        query_executor=None,
        subscribe_table_provider=None,
        token: str | None = None,
        max_write_rows: int | None = None,
    ) -> None:
        self.endpoint = _normalize_remote_endpoint(endpoint)
        self.master = master
        self.token = token
        if max_write_rows is not None and int(max_write_rows) <= 0:
            raise ValueError("max_write_rows must be positive")
        self.max_write_rows = int(max_write_rows) if max_write_rows is not None else None
        self._writer_factory = writer_factory
        self._stream_info_provider = stream_info_provider
        self._query_executor = query_executor
        self._subscribe_table_provider = subscribe_table_provider
        self._writers: dict[str, object] = {}
        self._stop_event = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._metrics_lock = threading.Lock()
        self._metrics: dict[str, int] = {
            "requests_total": 0,
            "auth_failures_total": 0,
            "errors_total": 0,
            "write_batches_total": 0,
            "written_rows_total": 0,
            "write_rejections_total": 0,
            "collect_requests_total": 0,
            "subscribe_clients_total": 0,
        }

    def start(self) -> "GatewayServer":
        host, port = _parse_remote_endpoint(self.endpoint)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen()
        listener.settimeout(0.1)
        actual_host, actual_port = listener.getsockname()
        self.endpoint = f"{actual_host}:{actual_port}"
        self._listener = listener
        self._thread = threading.Thread(
            target=self._serve,
            name="zippy-remote-gateway",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        listener = self._listener
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        for writer in list(self._writers.values()):
            close = getattr(writer, "close", None)
            if callable(close):
                close()
        self._writers.clear()

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stop_event.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(
                target=self._handle_client,
                args=(client,),
                name="zippy-remote-gateway-client",
                daemon=True,
            ).start()

    def _handle_client(self, client: socket.socket) -> None:
        with client:
            try:
                request, payload = _recv_remote_frame(client)
                self._increment_metric("requests_total")
                self._authorize(request)
                if request.get("kind") == "subscribe_table":
                    self._serve_subscribe_table(client, request)
                    return
                response, response_payload = self._dispatch(request, payload)
            except Exception as error:
                self._increment_metric("errors_total")
                response = {"status": "error", "reason": str(error)}
                response_payload = b""
            try:
                _send_remote_frame(client, response, response_payload)
            except OSError:
                return

    def metrics(self) -> dict[str, int | str]:
        with self._metrics_lock:
            metrics = dict(self._metrics)
        metrics["endpoint"] = self.endpoint
        metrics["running"] = self._thread is not None and self._thread.is_alive()
        return metrics

    def _increment_metric(self, name: str, value: int = 1) -> None:
        with self._metrics_lock:
            self._metrics[name] = self._metrics.get(name, 0) + int(value)

    def _authorize(self, request: dict[str, object]) -> None:
        if self.token is None:
            return
        if request.get("token") == self.token:
            return
        self._increment_metric("auth_failures_total")
        raise PermissionError("unauthorized remote gateway request")

    def _serve_subscribe_table(
        self,
        client: socket.socket,
        request: dict[str, object],
    ) -> None:
        source = str(request["source"])
        self._increment_metric("subscribe_clients_total")
        filter_payload = request.get("filter")
        filter_expr = (
            _query_expr_from_json(filter_payload)
            if isinstance(filter_payload, dict)
            else None
        )
        options = {
            "filter": filter_expr,
            "batch_size": request.get("batch_size"),
            "throttle_ms": request.get("throttle_ms"),
            "count": request.get("count"),
        }
        _send_remote_frame(client, {"status": "ok", "kind": "subscribed"})

        send_lock = threading.Lock()

        def send_table(table: object) -> None:
            with send_lock:
                _send_remote_frame(
                    client,
                    {"status": "ok", "kind": "table"},
                    _pyarrow_table_to_ipc(table),
                )

        provider = self._subscribe_table_provider
        if provider is None:
            handle = subscribe_table(
                source,
                callback=send_table,
                master=self.master,
                filter=filter_expr,
                batch_size=(
                    int(options["batch_size"]) if options["batch_size"] is not None else None
                ),
                throttle_ms=(
                    int(options["throttle_ms"]) if options["throttle_ms"] is not None else None
                ),
                count=(int(options["count"]) if options["count"] is not None else None),
            )
        else:
            handle = provider(source, send_table, options)

        try:
            while not self._stop_event.wait(0.1):
                try:
                    client.send(b"")
                except OSError:
                    break
        finally:
            stop = getattr(handle, "stop", None)
            if callable(stop):
                stop()

    def _dispatch(
        self,
        request: dict[str, object],
        payload: bytes,
    ) -> tuple[dict[str, object], bytes]:
        kind = str(request.get("kind"))
        if kind == "write_batch":
            stream_name = str(request["stream_name"])
            table = _pyarrow_table_from_ipc(payload)
            if self.max_write_rows is not None and table.num_rows > self.max_write_rows:
                self._increment_metric("write_rejections_total")
                raise RuntimeError(
                    "write batch row count exceeds limit "
                    f"rows=[{table.num_rows}] max_write_rows=[{self.max_write_rows}]"
                )
            writer = self._writer_for(stream_name, table.schema)
            writer.write(table)
            flush = getattr(writer, "flush", None)
            if callable(flush):
                flush()
            self._increment_metric("write_batches_total")
            self._increment_metric("written_rows_total", int(table.num_rows))
            return {"status": "ok"}, b""
        if kind == "list_streams":
            streams = self.master.list_streams() if self.master is not None else []
            return {"status": "ok", "streams": streams}, b""
        if kind == "get_stream":
            info, schema = self._stream_info(str(request["source"]))
            return {"status": "ok", "stream": info}, _pyarrow_schema_to_ipc(schema)
        if kind == "collect":
            self._increment_metric("collect_requests_total")
            table = self._collect(
                str(request["source"]),
                list(request.get("plan", [])),
                bool(request.get("snapshot", True)),
            )
            return {"status": "ok"}, _pyarrow_table_to_ipc(table)
        if kind == "metrics":
            return {"status": "ok", "metrics": self.metrics()}, b""
        raise ValueError(f"unsupported remote gateway request kind=[{kind}]")

    def _writer_for(self, stream_name: str, schema: object):
        writer = self._writers.get(stream_name)
        if writer is None:
            factory = self._writer_factory
            if factory is None:
                writer = _LocalGatewayWriter(stream_name, schema, self.master)
            else:
                writer = factory(stream_name, schema)
            self._writers[stream_name] = writer
        return writer

    def _stream_info(self, source: str) -> tuple[dict[str, object], object]:
        if self._stream_info_provider is not None:
            return self._stream_info_provider(source)
        table = read_table(source, master=self.master, _force_local=True)
        return table.info(), table.schema()

    def _collect(
        self,
        source: str,
        plan: list[dict[str, object]],
        snapshot: bool,
    ):
        if self._query_executor is not None:
            return self._query_executor(source, plan, snapshot)
        table = read_table(source, master=self.master, snapshot=snapshot, _force_local=True)
        return _apply_query_plan_json(table, plan).collect()


def get_writer(
    stream_name: str,
    *,
    master: MasterClient | RemoteMasterClient | None = None,
    endpoint: str | None = None,
    schema=None,
    create: bool = False,
    batch_size: int = 1024,
    flush_interval_ms: int | None = 5,
):
    """
    Return a writer for a named stream, choosing local segment or remote gateway automatically.
    """
    selected_master = master or _default_master()
    remote_endpoint = endpoint or _remote_gateway_endpoint(selected_master)
    if remote_endpoint is not None:
        remote_token = _remote_gateway_token(selected_master)
        if schema is None and not create:
            try:
                schema = selected_master.get_stream_schema(stream_name)
            except Exception:
                schema = None
        return RemoteGatewayWriter(
            stream_name,
            endpoint=remote_endpoint,
            schema=schema,
            batch_size=batch_size,
            flush_interval_ms=flush_interval_ms,
            token=remote_token,
        )

    if schema is None:
        if not create:
            raise ValueError("local get_writer requires schema or create=True with schema")
        raise ValueError("schema is required when creating a local writer")
    return _LocalGatewayWriter(stream_name, schema, selected_master)


class Table:
    """
    Read a named Zippy table through the default master connection.

    :param source: Named table or stream to read.
    :type source: str
    :param master: Optional explicit master client. When omitted, the connection created by
        ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param wait: When true, wait until the table exists and has an active segment descriptor.
    :type wait: bool
    :param timeout: Optional maximum wait duration in seconds, or strings such as ``"30s"``.
    :type timeout: float | str | None
    :raises RuntimeError: If no explicit master is supplied and ``zippy.connect()`` was not called.
    """

    def __init__(
        self,
        source: str,
        master: MasterClient | None = None,
        *,
        wait: bool = False,
        timeout: float | str | None = None,
        snapshot: bool = True,
        _force_local: bool = False,
    ) -> None:
        selected_master = master or _default_master()
        self.source = source
        try:
            _ensure_master_process(selected_master, f"read_table.{source}")
            if wait:
                _wait_for_table_ready(source, selected_master, timeout)
            if _is_remote_master(selected_master) and not _force_local:
                self._inner = _RemoteQuery(
                    source=source,
                    master=selected_master,
                    endpoint=_remote_gateway_endpoint(selected_master),
                    token=_remote_gateway_token(selected_master),
                )
            else:
                self._inner = _NativeQuery(source=source, master=selected_master)
            self._snapshot_enabled = bool(snapshot)
            self._fixed_snapshot: dict[str, object] | None = None
            self._plan_ops: list[tuple[str, object]] = []
        except RuntimeError as error:
            raise RuntimeError(
                f"failed to read table source=[{source}] "
                f"master_uri=[{_master_uri_for_error(selected_master)}]"
            ) from error

    def tail(self, n: int):
        """
        Return the latest available rows as a ``pyarrow.Table``.

        This is the user-level query path. It reads from the current live segment
        view first, and transparently backfills from persisted parquet files when
        live retention contains fewer than ``n`` rows.

        :param n: Maximum number of rows to return.
        :type n: int
        :returns: Latest available rows.
        :rtype: pyarrow.Table
        """
        if n < 0:
            raise ValueError("n must be non-negative")
        if self._has_query_plan():
            table = self.collect()
            if n == 0:
                return table.slice(0, 0)
            return table.slice(table.num_rows - n)

        live = self._inner.tail(n)
        if live.num_rows >= n:
            return live

        snapshot = self.snapshot()
        needed = n - live.num_rows
        persisted = _tail_persisted_rows(snapshot, needed)
        if persisted is None or persisted.num_rows == 0:
            return live

        import pyarrow as pa

        combined = pa.concat_tables([persisted, live])
        if combined.num_rows > n:
            return combined.slice(combined.num_rows - n)
        return combined

    def schema(self):
        """
        Return the Arrow schema of this query source.

        :returns: Source stream schema.
        :rtype: pyarrow.Schema
        """
        return self._inner.schema()

    def stream_info(self) -> dict[str, object]:
        """
        Return current control-plane metadata for this query source.

        :returns: Stream metadata from master.
        :rtype: dict[str, object]
        """
        return self._inner.stream_info()

    def info(self) -> dict[str, object]:
        """
        Return current control-plane metadata for this table.

        :returns: Stream metadata from master.
        :rtype: dict[str, object]
        """
        return self.stream_info()

    def alerts(self) -> list[dict[str, object]]:
        """
        Return metadata-derived health alerts for this table.

        :returns: Health alerts ordered by control-plane importance.
        :rtype: list[dict[str, object]]
        """
        return _build_table_alerts(self.info())

    def health(self) -> dict[str, object]:
        """
        Return a compact health summary for this table.

        :returns: Table health summary with alerts.
        :rtype: dict[str, object]
        """
        return _table_health_from_info(self.info(), self.source)

    def snapshot(self) -> dict[str, object]:
        """
        Return a fixed query boundary for live table reads.

        :returns: Table snapshot metadata including active and retained sealed segments.
        :rtype: dict[str, object]
        """
        if self._snapshot_enabled:
            if self._fixed_snapshot is None:
                self._fixed_snapshot = self._inner.snapshot()
            return self._fixed_snapshot
        return self._inner.snapshot()

    def _scan_live(self):
        """
        Return a PyArrow RecordBatchReader over live segment data.

        The reader covers retained sealed segments followed by the current active
        segment at a fixed read boundary.

        :returns: Live segment batches.
        :rtype: pyarrow.RecordBatchReader
        """
        return self._inner.scan_live()

    def select(self, *columns: object, **named_exprs: object) -> Table:
        """
        Return a query selecting columns or expressions.

        :param columns: Column names or Zippy query expressions.
        :type columns: object
        :returns: New table reader with projection applied at execution time.
        :rtype: Table
        """
        select_exprs = _normalize_query_exprs(columns, named_exprs)
        return self._clone_with_plan(select_exprs=select_exprs)

    def filter(self, predicate: object | None = None, **equals: object) -> Table:
        """
        Return a query filtered by a predicate expression.

        :param predicate: Zippy query boolean expression.
        :type predicate: object | None
        :returns: New table reader with the filter applied at execution time.
        :rtype: Table
        """
        predicate_expr = predicate
        for name, value in equals.items():
            predicate_expr = _combine_query_predicates(
                predicate_expr,
                col(name) == value,
            )
        if predicate_expr is None:
            return self
        return self._clone_with_plan(filter_expr=predicate_expr)

    def where(self, predicate: object | None = None, **equals: object) -> Table:
        """
        Compatibility alias for :meth:`filter`.

        New user code should prefer ``filter`` to match Polars naming.
        """
        return self.filter(predicate, **equals)

    def with_columns(self, *exprs: object, **named_exprs: object) -> Table:
        """
        Return a query with additional or replaced columns.

        :param exprs: Zippy query expressions.
        :type exprs: object
        :returns: New table reader with expressions evaluated at execution time.
        :rtype: Table
        """
        column_exprs = _normalize_query_exprs(exprs, named_exprs)
        if not column_exprs:
            return self
        return self._clone_with_plan(with_columns_exprs=column_exprs)

    def join(
        self,
        other: Table,
        *,
        on: str | list[str] | tuple[str, ...],
        how: str = "inner",
        suffix: str = "_right",
    ) -> Table:
        """
        Return a query joining another Zippy table query.

        :param other: Right-hand table query.
        :type other: Table
        :param on: Join key column or columns.
        :type on: str | list[str] | tuple[str, ...]
        :param how: Join mode forwarded to Polars.
        :type how: str
        :param suffix: Suffix for duplicate right-hand column names.
        :type suffix: str
        :returns: New table reader with join applied at execution time.
        :rtype: Table
        """
        if not isinstance(other, Table):
            raise TypeError("join other must be a zippy.Table")
        keys = [on] if isinstance(on, str) else [str(item) for item in on]
        return self._clone_with_plan(
            join_spec={
                "other": other,
                "on": keys,
                "how": str(how),
                "suffix": str(suffix),
            }
        )

    def between(self, column: object, start: object, end: object) -> Table:
        """
        Return a query filtered to inclusive ``[start, end]`` bounds.

        :param column: Column name or Zippy query column expression.
        :type column: object
        :param start: Inclusive lower bound.
        :type start: object
        :param end: Inclusive upper bound.
        :type end: object
        :returns: New table reader with the range filter applied at execution time.
        :rtype: Table
        """
        column_expr = col(column) if isinstance(column, str) else _literal(column)
        return self.filter((column_expr >= start) & (column_expr <= end))

    def collect(self):
        """
        Collect all currently queryable rows as a ``pyarrow.Table``.

        This user-level query path merges persisted parquet rows with the live
        segment view and hides the active/sealed/persisted storage split.

        :returns: Current query result.
        :rtype: pyarrow.Table
        """
        remote_collect = getattr(self._inner, "collect_plan", None)
        if callable(remote_collect):
            return remote_collect(
                _query_plan_to_json(self._plan_ops),
                snapshot=self._snapshot_enabled,
            )

        snapshot = self.snapshot()
        schema = self.schema()
        read_columns = self._source_read_columns(schema)
        pushdown_filters = self._pushdown_filters(schema)
        persisted = _collect_persisted_rows(
            snapshot,
            columns=read_columns,
            filters=pushdown_filters,
        )
        live = self._scan_live_snapshot(snapshot).read_all()
        live = _filter_query_table(live, pushdown_filters)
        live = _select_query_columns(live, read_columns)
        table = _concat_query_tables(
            [persisted, live],
            _schema_for_query_columns(schema, read_columns),
        )
        return self._apply_query_plan(table)

    def to_pyarrow(self):
        """
        Return the query result as a ``pyarrow.Table``.

        This is an alias for :meth:`collect`.

        :returns: Current query result.
        :rtype: pyarrow.Table
        """
        return self.collect()

    def to_pandas(self, **kwargs):
        """
        Return the query result as a pandas DataFrame.

        Keyword arguments are forwarded to ``pyarrow.Table.to_pandas``.

        :returns: Current query result converted by PyArrow.
        :rtype: pandas.DataFrame
        """
        return self.collect().to_pandas(**kwargs)

    def to_polars(self, **kwargs):
        """
        Return the query result as a Polars DataFrame.

        Keyword arguments are forwarded to ``polars.from_arrow``.

        :returns: Current query result converted from Arrow.
        :rtype: polars.DataFrame
        """
        import polars as pl

        return pl.from_arrow(self.collect(), **kwargs)

    def reader(self):
        """
        Return all currently queryable rows as a ``pyarrow.RecordBatchReader``.

        :returns: Current query result reader.
        :rtype: pyarrow.RecordBatchReader
        """
        table = self.collect()
        import pyarrow as pa

        return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    def persisted_files(self) -> list[dict[str, object]]:
        """
        Return persisted file metadata from master stream metadata.

        :returns: Persisted file metadata entries.
        :rtype: list[dict[str, object]]
        """
        return list(self.stream_info().get("persisted_files", []))

    def persist_events(self) -> list[dict[str, object]]:
        """
        Return persist lifecycle events from master stream metadata.

        :returns: Persist lifecycle event metadata entries.
        :rtype: list[dict[str, object]]
        """
        return list(self.stream_info().get("persist_events", []))

    def segment_reader_leases(self) -> list[dict[str, object]]:
        """
        Return currently registered segment reader leases from master stream metadata.

        :returns: Segment reader lease metadata entries.
        :rtype: list[dict[str, object]]
        """
        return list(self.stream_info().get("segment_reader_leases", []))

    def _scan_persisted(self):
        """
        Return a PyArrow Dataset over persisted parquet files.

        :returns: PyArrow dataset backed by persisted parquet files.
        :rtype: pyarrow.dataset.Dataset
        :raises RuntimeError: If no persisted files are registered for this source.
        """
        files = self.persisted_files()
        paths = [str(item["file_path"]) for item in files if item.get("file_path")]
        if not paths:
            raise RuntimeError(f"persisted files are not registered source=[{self.source}]")
        import pyarrow.dataset as ds

        return ds.dataset(paths, format="parquet")

    def _clone_with_plan(
        self,
        *,
        select_exprs: list[object] | None = None,
        filter_expr: object | None = None,
        with_columns_exprs: list[object] | None = None,
        join_spec: dict[str, object] | None = None,
    ) -> Table:
        table = object.__new__(Table)
        table.source = self.source
        table._inner = self._inner
        table._snapshot_enabled = self._snapshot_enabled
        table._fixed_snapshot = self._fixed_snapshot
        table._plan_ops = list(self._plan_ops)
        if filter_expr is not None:
            table._plan_ops.append(("filter", filter_expr))
        if select_exprs is not None:
            table._plan_ops.append(("select", list(select_exprs)))
        if with_columns_exprs is not None:
            table._plan_ops.append(("with_columns", list(with_columns_exprs)))
        if join_spec is not None:
            table._plan_ops.append(("join", join_spec))
        return table

    def _has_query_plan(self) -> bool:
        return bool(self._plan_ops)

    def _apply_query_plan(self, table):
        if not self._has_query_plan():
            return table

        import polars as pl

        frame = pl.from_arrow(table).lazy()
        for kind, value in self._plan_ops:
            if kind == "filter":
                frame = frame.filter(_compile_query_expr_to_polars(value))
            elif kind == "select":
                frame = frame.select(
                    [_compile_query_expr_to_polars(expr) for expr in value]
                )
            elif kind == "with_columns":
                frame = frame.with_columns(
                    [_compile_query_expr_to_polars(expr) for expr in value]
                )
            elif kind == "join":
                other = value["other"]
                right_frame = pl.from_arrow(other.collect()).lazy()
                frame = frame.join(
                    right_frame,
                    on=value["on"],
                    how=value["how"],
                    suffix=value["suffix"],
                )
            else:
                raise ValueError(f"unsupported table plan operation=[{kind}]")
        return frame.collect().to_arrow()

    def _scan_live_snapshot(self, snapshot: dict[str, object]):
        scan_snapshot = getattr(self._inner, "scan_snapshot", None)
        if callable(scan_snapshot):
            return scan_snapshot(snapshot)
        return self._inner.scan_live()

    def _source_read_columns(self, schema: object | None) -> list[str] | None:
        if not self._plan_ops:
            return None
        if any(kind == "join" for kind, _ in self._plan_ops):
            return None
        if not any(kind == "select" for kind, _ in self._plan_ops):
            return None
        required: set[str] = set()
        for kind, value in self._plan_ops:
            if kind == "filter":
                required.update(_expr_columns(value))
            elif kind in {"select", "with_columns"}:
                for expr in value:
                    required.update(_expr_columns(expr))
        if not required:
            return None
        return _ordered_subset(required, schema)

    def _pushdown_filters(self, schema: object | None) -> list[tuple[str, str, object]] | None:
        filters = [value for kind, value in self._plan_ops if kind == "filter"]
        predicate = _combine_filter_ops(filters)
        pyarrow_filters = _predicate_to_pyarrow_filters(predicate)
        if pyarrow_filters is None:
            return None
        if schema is None:
            return pyarrow_filters
        schema_names = set(getattr(schema, "names", []))
        if any(column not in schema_names for column, _, _ in pyarrow_filters):
            return None
        return pyarrow_filters

def read_table(
    source: str,
    master: MasterClient | None = None,
    *,
    wait: bool = False,
    timeout: float | str | None = None,
    snapshot: bool = True,
    _force_local: bool = False,
) -> Table:
    """
    Open a named Zippy table.

    :param source: Named table or stream to read.
    :type source: str
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param wait: When true, wait until the table exists and has an active segment descriptor.
    :type wait: bool
    :param timeout: Optional maximum wait duration in seconds, or strings such as ``"500ms"``,
        ``"30s"`` or ``"2m"``. ``None`` waits indefinitely.
    :type timeout: float | str | None
    :param snapshot: When true, reuse one query snapshot for repeated terminal operations. When
        false, each terminal operation resolves the latest table snapshot.
    :type snapshot: bool
    :returns: Table object for further operations such as ``tail``.
    :rtype: Table
    """
    kwargs = {
        "source": source,
        "master": master,
        "wait": wait,
        "timeout": timeout,
        "snapshot": snapshot,
    }
    if _force_local:
        kwargs["_force_local"] = True
    return Table(**kwargs)


def compare_replay(
    left,
    right,
    *,
    by: str | list[str] | tuple[str, ...] | None = None,
    master: MasterClient | None = None,
) -> dict[str, object]:
    """
    Compare two live/replay query results after normalizing them to Arrow tables.

    ``left`` and ``right`` can be table names, :class:`Table` objects,
    ``pyarrow.Table`` instances, ``pyarrow.RecordBatchReader`` objects, or
    PyArrow datasets.

    :param left: Expected live or persisted data.
    :type left: object
    :param right: Replay output data.
    :type right: object
    :param by: Optional key columns used to sort both sides before comparison.
    :type by: str | list[str] | tuple[str, ...] | None
    :param master: Optional master used when either side is a table name.
    :type master: MasterClient | None
    :returns: Replay comparison summary.
    :rtype: dict[str, object]
    """
    left_table = _sort_replay_table(_coerce_replay_table(left, master), by)
    right_table = _sort_replay_table(_coerce_replay_table(right, master), by)

    schema_equal = _schemas_equal(left_table.schema, right_table.schema)
    data_equal = schema_equal and _tables_equal(left_table, right_table)
    if data_equal:
        missing_rows, extra_rows = 0, 0
    else:
        missing_rows, extra_rows = _replay_row_deltas(left_table, right_table)
    mismatch_rows = max(missing_rows, extra_rows)
    return {
        "equal": bool(schema_equal and data_equal and mismatch_rows == 0),
        "left_rows": left_table.num_rows,
        "right_rows": right_table.num_rows,
        "schema_equal": schema_equal,
        "missing_rows": missing_rows,
        "extra_rows": extra_rows,
        "mismatch_rows": mismatch_rows,
    }


def _coerce_replay_table(value, master: MasterClient | None):
    import pyarrow as pa

    if isinstance(value, str):
        return read_table(value, master=master).collect()
    if isinstance(value, Table):
        return value.collect()
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])

    read_all = getattr(value, "read_all", None)
    if callable(read_all):
        return read_all()

    to_table = getattr(value, "to_table", None)
    if callable(to_table):
        return to_table()

    raise TypeError("replay comparison input must be a table name or Arrow-compatible object")


def _sort_replay_table(table, by: str | list[str] | tuple[str, ...] | None):
    if by is None:
        return table
    keys = [by] if isinstance(by, str) else [str(item) for item in by]
    missing = [name for name in keys if name not in table.column_names]
    if missing:
        raise ValueError(f"replay comparison key columns are missing columns=[{missing}]")
    return table.sort_by([(name, "ascending") for name in keys])


def _schemas_equal(left, right) -> bool:
    try:
        return bool(left.equals(right, check_metadata=False))
    except TypeError:
        return bool(left == right)


def _tables_equal(left, right) -> bool:
    try:
        return bool(left.equals(right, check_metadata=False))
    except TypeError:
        return bool(left.equals(right))


def _replay_row_deltas(left, right) -> tuple[int, int]:
    if left.column_names != right.column_names:
        return left.num_rows, right.num_rows
    left_rows = _table_row_counter(left)
    right_rows = _table_row_counter(right)
    missing = left_rows - right_rows
    extra = right_rows - left_rows
    return sum(missing.values()), sum(extra.values())


def _table_row_counter(table) -> Counter:
    return Counter(_arrow_row_bytes(table, row_index) for row_index in range(table.num_rows))


def _arrow_row_bytes(table, row_index: int) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table.slice(row_index, 1))
    return sink.getvalue().to_pybytes()


def _tail_persisted_rows(snapshot: dict[str, object], n: int):
    if n <= 0:
        return None

    files = _non_overlapping_persisted_files(snapshot)
    if not files:
        return None

    tables = []
    row_count = 0
    for item in reversed(sorted(files, key=_persisted_file_order_key)):
        table = _read_persisted_parquet_file(item["file_path"])
        if table.num_rows == 0:
            continue
        tables.append(table)
        row_count += table.num_rows
        if row_count >= n:
            break

    if not tables:
        return None

    tables.reverse()
    combined = _concat_query_tables(tables, None)
    if combined.num_rows > n:
        return combined.slice(combined.num_rows - n)
    return combined


def _collect_persisted_rows(
    snapshot: dict[str, object],
    *,
    columns: list[str] | None = None,
    filters: object | None = None,
):
    files = _non_overlapping_persisted_files(snapshot)
    if not files:
        return None

    tables = [
        _read_persisted_parquet_file(
            item["file_path"],
            columns=columns,
            filters=filters,
        )
        for item in sorted(files, key=_persisted_file_order_key)
    ]
    return _concat_query_tables(tables, None)


def _read_persisted_parquet_file(
    file_path: object,
    *,
    columns: list[str] | None = None,
    filters: object | None = None,
):
    import pyarrow.parquet as pq

    return pq.read_table(str(file_path), columns=columns, filters=filters, partitioning=None)


def _non_overlapping_persisted_files(
    snapshot: dict[str, object],
) -> list[dict[str, object]]:
    live_identities = _live_segment_identities(snapshot)
    return [
        item
        for item in snapshot.get("persisted_files", [])
        if isinstance(item, dict)
        and item.get("file_path")
        and not (_persisted_segment_identities(item) & live_identities)
    ]


def _persisted_segment_identities(value: dict[str, object]) -> set[tuple[int, int]]:
    source_segments = value.get("source_segments")
    if isinstance(source_segments, list):
        identities = {
            identity
            for item in source_segments
            if isinstance(item, dict)
            for identity in [_persisted_segment_identity(item)]
            if identity is not None
        }
        if identities:
            return identities
    identity = _persisted_segment_identity(value)
    return {identity} if identity is not None else set()


def _concat_query_tables(tables: list[object | None], schema: object | None):
    import pyarrow as pa

    available = [table for table in tables if table is not None and table.num_rows > 0]
    if available:
        return pa.concat_tables(available)
    if schema is None:
        return None
    return pa.Table.from_batches([], schema=schema)


def _select_query_columns(table: object, columns: list[str] | None):
    if columns is None:
        return table
    available = [name for name in columns if name in table.column_names]
    return table.select(available)


def _filter_query_table(table: object, filters: object | None):
    if not filters:
        return table
    import pyarrow as pa
    import pyarrow.compute as pc

    mask = None
    for column, op, value in filters:
        if column not in table.column_names:
            return table
        column_values = table[column]
        if op == "==":
            current = pc.equal(column_values, value)
        elif op == "!=":
            current = pc.not_equal(column_values, value)
        elif op == ">":
            current = pc.greater(column_values, value)
        elif op == ">=":
            current = pc.greater_equal(column_values, value)
        elif op == "<":
            current = pc.less(column_values, value)
        elif op == "<=":
            current = pc.less_equal(column_values, value)
        elif op == "in":
            current = pc.is_in(column_values, value_set=pa.array(value))
        else:
            return table
        mask = current if mask is None else pc.and_(mask, current)
    if mask is None:
        return table
    return table.filter(mask)


def _schema_for_query_columns(schema: object | None, columns: list[str] | None):
    if schema is None or columns is None:
        return schema
    import pyarrow as pa

    fields = [schema.field(name) for name in columns if name in schema.names]
    return pa.schema(fields)


def _live_segment_identities(snapshot: dict[str, object]) -> set[tuple[int, int]]:
    identities: set[tuple[int, int]] = set()
    active_identity = _descriptor_segment_identity(
        snapshot.get("active_segment_descriptor")
    )
    if active_identity is not None:
        identities.add(active_identity)

    for descriptor in snapshot.get("sealed_segments", []):
        identity = _descriptor_segment_identity(descriptor)
        if identity is not None:
            identities.add(identity)
    return identities


def _descriptor_segment_identity(value: object) -> tuple[int, int] | None:
    if not isinstance(value, dict):
        return None
    return _segment_identity(value.get("segment_id"), value.get("generation"))


def _persisted_segment_identity(value: dict[str, object]) -> tuple[int, int] | None:
    return _segment_identity(
        value.get("source_segment_id"),
        value.get("source_generation"),
    )


def _segment_identity(segment_id: object, generation: object) -> tuple[int, int] | None:
    if segment_id is None or generation is None:
        return None
    try:
        return (int(segment_id), int(generation))
    except (TypeError, ValueError):
        return None


def _persisted_file_order_key(value: dict[str, object]) -> tuple[int, int, int, str]:
    return (
        _int_order_value(value.get("source_segment_id")),
        _int_order_value(value.get("source_generation")),
        _int_order_value(value.get("created_at")),
        str(value.get("file_path", "")),
    )


def _int_order_value(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def read_from(
    stream_name: str,
    instrument_ids: list[str] | tuple[str, ...] | str | None = None,
    master: MasterClient | None = None,
    *,
    xfast: bool = False,
) -> BusReader:
    """
    Attach a bus reader using the default master connection.

    :param stream_name: Named stream to read.
    :type stream_name: str
    :param instrument_ids: Optional instrument filter.
    :type instrument_ids: list[str] | tuple[str, ...] | str | None
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param xfast: Spin instead of sleeping in the low-level bus reader.
    :type xfast: bool
    :returns: Bus reader attached to the stream.
    :rtype: BusReader
    """
    selected_master = master or _default_master()
    _ensure_master_process(selected_master, f"read_from.{stream_name}")
    return selected_master.read_from(
        stream_name,
        instrument_ids=instrument_ids,
        xfast=xfast,
    )


class Row:
    """
    Represent one row from a Zippy stream callback.

    :param values: Row values keyed by column name.
    :type values: dict[str, object]
    """

    __slots__ = ("_values",)

    def __init__(self, values: dict[str, object]) -> None:
        if type(values) is dict:
            self._values = values
        else:
            self._values = dict(values)

    def __getitem__(self, key: str) -> object:
        return self._values[key]

    def __contains__(self, key: object) -> bool:
        return key in self._values

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"zippy.Row({self._values!r})"

    def get(self, key: str, default: object = None) -> object:
        return self._values.get(key, default)

    def keys(self):
        return self._values.keys()

    def values(self):
        return self._values.values()

    def items(self):
        return self._values.items()

    def to_dict(self) -> dict[str, object]:
        """
        Convert the row into a plain Python dictionary.

        :returns: A shallow copy of the row values.
        :rtype: dict[str, object]
        """
        return dict(self._values)


def _require_positive_optional_int(name: str, value: int | None) -> int | None:
    if value is None:
        return None
    normalized = int(value)
    if normalized <= 0:
        raise ValueError(f"{name} must be positive")
    return normalized


def _instrument_ids_from_query_filter(filter_expr: object | None) -> list[str] | None:
    if filter_expr is None:
        return None
    filters = _predicate_to_pyarrow_filters(filter_expr)
    if not filters:
        raise ValueError(
            "subscribe row filter only supports zp.col('instrument_id') equality/is_in"
        )

    selected: set[str] | None = None
    ordered: list[str] = []
    for column, op, value in filters:
        if column != "instrument_id":
            raise ValueError(
                "subscribe row filter only supports zp.col('instrument_id') equality/is_in"
            )
        if op == "==":
            values = [str(value)]
        elif op == "in":
            values = [str(item) for item in value]
        else:
            raise ValueError(
                "subscribe row filter only supports zp.col('instrument_id') equality/is_in"
            )

        value_set = set(values)
        if selected is None:
            selected = value_set
            ordered = values
        else:
            selected &= value_set
            ordered = [item for item in ordered if item in selected]

    return ordered if selected is not None else None


def _pyarrow_filters_from_query_filter(filter_expr: object | None):
    if filter_expr is None:
        return None
    filters = _predicate_to_pyarrow_filters(filter_expr)
    if filters is None:
        raise ValueError(
            "subscribe_table filter only supports simple zp.col predicates that can run on Arrow"
        )
    return filters


class _TableSubscriptionCallback:
    """
    Adapt native incremental Arrow tables into filtered, batched table callbacks.
    """

    def __init__(
        self,
        callback,
        *,
        filter_expr: object | None = None,
        batch_size: int | None = None,
        throttle_ms: int | None = None,
        count: int | None = None,
    ) -> None:
        self._callback = callback
        self._filters = _pyarrow_filters_from_query_filter(filter_expr)
        self._batch_size = _require_positive_optional_int("batch_size", batch_size)
        self._throttle_ms = _require_positive_optional_int("throttle_ms", throttle_ms)
        self._count = _require_positive_optional_int("count", count)
        self._pending: list[object] = []
        self._pending_rows = 0
        self._last_emit_ns = time.perf_counter_ns()
        self._immediate = (
            self._filters is None
            and self._batch_size is None
            and self._throttle_ms is None
            and self._count is None
        )

    def __call__(self, table: object) -> None:
        if self._immediate:
            self._callback(table)
            return

        table = _filter_query_table(table, self._filters)
        row_count = int(getattr(table, "num_rows", 0))
        if row_count <= 0:
            return

        if self._batch_size is None and self._throttle_ms is None:
            self._callback(self._tail(table))
            return

        self._pending.append(table)
        self._pending_rows += row_count
        if self._should_emit():
            self.flush()

    def flush(self) -> None:
        if not self._pending:
            return
        import pyarrow as pa

        if len(self._pending) == 1:
            table = self._pending[0]
        else:
            table = pa.concat_tables(self._pending)
        table = self._tail(table)
        self._pending = []
        self._pending_rows = 0
        self._last_emit_ns = time.perf_counter_ns()
        self._callback(table)

    def _should_emit(self) -> bool:
        if self._batch_size is not None and self._pending_rows >= self._batch_size:
            return True
        if self._throttle_ms is None:
            return False
        elapsed_ns = time.perf_counter_ns() - self._last_emit_ns
        return elapsed_ns >= self._throttle_ms * 1_000_000

    def _tail(self, table: object) -> object:
        if self._count is None:
            return table
        row_count = int(getattr(table, "num_rows", 0))
        if row_count <= self._count:
            return table
        return table.slice(row_count - self._count, self._count)


class StreamSubscriber:
    """
    Subscribe to a named stream and invoke a local Python callback with rows.

    Live subscriptions are best-effort: a running subscriber can attach to a
    restarted writer and continue receiving new rows, but rows missed while the
    writer or reader is unavailable are not automatically backfilled.

    :param source: Named stream to subscribe to.
    :type source: str
    :param callback: Function called once for each incremental ``zippy.Row``.
    :type callback: callable
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param poll_interval_ms: Sleep interval when no new rows are available.
    :type poll_interval_ms: int
    :param xfast: Spin instead of sleeping when no new rows are available.
    :type xfast: bool
    :param idle_spin_checks: Short spin checks before entering mmap futex wait in non-xfast mode.
    :type idle_spin_checks: int
    :param instrument_ids: Optional instrument filter evaluated before row callbacks.
    :type instrument_ids: list[str] | tuple[str, ...] | str | None
    :param filter: Optional ``zp.col`` predicate. Row mode supports ``instrument_id`` equality/is_in.
    :type filter: object | None
    :param wait: When true, wait until the stream exists and has an active segment descriptor.
    :type wait: bool
    :param timeout: Optional maximum wait duration in seconds, or strings such as ``"30s"``.
    :type timeout: float | str | None
    :raises RuntimeError: If no explicit master is supplied and ``zippy.connect()`` was not called.
    """

    def __init__(
        self,
        source: str,
        callback,
        master: MasterClient | None = None,
        *,
        poll_interval_ms: int | None = None,
        xfast: bool = False,
        idle_spin_checks: int = 64,
        instrument_ids: list[str] | tuple[str, ...] | str | None = None,
        filter: object | None = None,
        batch_size: int | None = None,
        throttle_ms: int | None = None,
        count: int | None = None,
        wait: bool = False,
        timeout: float | str | None = None,
        _table_callback: bool = False,
    ) -> None:
        selected_master = master or _default_master()
        _ensure_master_process(selected_master, f"subscribe.{source}")
        if wait:
            _wait_for_table_ready(source, selected_master, timeout)
        self._table_callback_adapter: _TableSubscriptionCallback | None = None
        if filter is not None and instrument_ids is not None:
            raise ValueError("filter and instrument_ids cannot be used together")
        if _table_callback and instrument_ids is not None:
            raise ValueError("instrument_ids is only supported by subscribe row callbacks")
        if _table_callback:
            callback = _TableSubscriptionCallback(
                callback,
                filter_expr=filter,
                batch_size=batch_size,
                throttle_ms=throttle_ms,
                count=count,
            )
            self._table_callback_adapter = callback
        else:
            if instrument_ids is None:
                instrument_ids = _instrument_ids_from_query_filter(filter)
            _require_positive_optional_int("batch_size", batch_size)
            _require_positive_optional_int("throttle_ms", throttle_ms)
            _require_positive_optional_int("count", count)
        if poll_interval_ms is None:
            effective_poll_interval_ms = 10 if _table_callback else 1
        else:
            effective_poll_interval_ms = poll_interval_ms
        self._inner = _NativeStreamSubscriber(
            source=source,
            master=selected_master,
            callback=callback,
            poll_interval_ms=effective_poll_interval_ms,
            xfast=xfast,
            idle_spin_checks=idle_spin_checks,
            row_factory=None if _table_callback else Row,
            instrument_ids=instrument_ids,
        )

    def start(self) -> "StreamSubscriber":
        """
        Start the background subscription thread.

        :returns: This subscriber handle.
        :rtype: StreamSubscriber
        """
        self._inner.start()
        return self

    def stop(self) -> None:
        """
        Stop the subscription thread and re-raise callback errors.

        :raises RuntimeError: If the background subscriber failed.
        """
        self._inner.stop()
        if self._table_callback_adapter is not None:
            self._table_callback_adapter.flush()

    def join(self) -> None:
        """
        Wait for the subscription thread to finish.

        :raises RuntimeError: If the background subscriber failed.
        """
        self._inner.join()
        if self._table_callback_adapter is not None:
            self._table_callback_adapter.flush()

    def metrics(self) -> dict[str, object]:
        """
        Return runtime counters for this subscriber.

        :returns: Subscriber counters such as delivered rows and descriptor updates.
        :rtype: dict[str, object]
        """
        return self._inner.metrics()


def subscribe(
    source: str,
    callback,
    master: MasterClient | None = None,
    *,
    poll_interval_ms: int = 1,
    xfast: bool = False,
    idle_spin_checks: int = 64,
    instrument_ids: list[str] | tuple[str, ...] | str | None = None,
    filter: object | None = None,
    wait: bool = False,
    timeout: float | str | None = None,
) -> StreamSubscriber:
    """
    Subscribe to a stream using the default master connection.

    This is a best-effort live subscription. It can resume on new writer
    descriptors, but does not automatically backfill rows missed during writer
    downtime.

    :param source: Named stream to subscribe to.
    :type source: str
    :param callback: Function called once for each incremental ``zippy.Row``.
    :type callback: callable
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param poll_interval_ms: Sleep interval when no new rows are available.
    :type poll_interval_ms: int
    :param xfast: Spin instead of sleeping when no new rows are available.
    :type xfast: bool
    :param idle_spin_checks: Short spin checks before entering mmap futex wait in non-xfast mode.
    :type idle_spin_checks: int
    :param instrument_ids: Optional instrument filter evaluated before row callbacks.
    :type instrument_ids: list[str] | tuple[str, ...] | str | None
    :param filter: Optional ``zp.col`` predicate. Row mode supports ``instrument_id`` equality/is_in.
    :type filter: object | None
    :param wait: When true, wait until the stream exists and has an active segment descriptor.
    :type wait: bool
    :param timeout: Optional maximum wait duration in seconds, or strings such as ``"30s"``.
    :type timeout: float | str | None
    :returns: Started subscriber handle.
    :rtype: StreamSubscriber
    """
    selected_master = master or _default_master()
    remote_endpoint = _remote_gateway_endpoint(selected_master)
    if remote_endpoint is not None:
        remote_token = _remote_gateway_token(selected_master)
        if filter is not None and instrument_ids is not None:
            raise ValueError("filter and instrument_ids cannot be used together")
        remote_filter = filter
        if remote_filter is None and instrument_ids is not None:
            values = [instrument_ids] if isinstance(instrument_ids, str) else list(instrument_ids)
            remote_filter = col("instrument_id").is_in(values)
        return RemoteStreamSubscriber(
            source,
            endpoint=remote_endpoint,
            callback=callback,
            table_callback=False,
            filter=remote_filter,
            token=remote_token,
        ).start()

    subscriber_kwargs = {
        "source": source,
        "callback": callback,
        "master": selected_master,
        "poll_interval_ms": poll_interval_ms,
        "xfast": xfast,
        "idle_spin_checks": idle_spin_checks,
        "instrument_ids": instrument_ids,
        "wait": wait,
        "timeout": timeout,
    }
    if filter is not None:
        subscriber_kwargs["filter"] = filter
    subscriber = StreamSubscriber(**subscriber_kwargs)
    return subscriber.start()


def subscribe_table(
    source: str,
    callback,
    master: MasterClient | None = None,
    *,
    poll_interval_ms: int = 10,
    xfast: bool = False,
    idle_spin_checks: int = 64,
    filter: object | None = None,
    batch_size: int | None = None,
    throttle_ms: int | None = None,
    count: int | None = None,
    wait: bool = False,
    timeout: float | str | None = None,
) -> StreamSubscriber:
    """
    Subscribe to a stream using incremental ``pyarrow.Table`` callbacks.

    This is a best-effort live subscription. It can resume on new writer
    descriptors, but does not automatically backfill rows missed during writer
    downtime.

    :param source: Named stream to subscribe to.
    :type source: str
    :param callback: Function called with each incremental ``pyarrow.Table``.
    :type callback: callable
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param poll_interval_ms: Sleep interval when no new rows are available.
    :type poll_interval_ms: int
    :param xfast: Spin instead of sleeping when no new rows are available.
    :type xfast: bool
    :param idle_spin_checks: Short spin checks before entering mmap futex wait in non-xfast mode.
    :type idle_spin_checks: int
    :param filter: Optional ``zp.col`` predicate applied before table callbacks.
    :type filter: object | None
    :param batch_size: Emit only after at least this many filtered pending rows are available.
    :type batch_size: int | None
    :param throttle_ms: Emit pending rows once this many milliseconds have elapsed.
    :type throttle_ms: int | None
    :param count: Limit each callback table to the latest ``count`` rows.
    :type count: int | None
    :param wait: When true, wait until the stream exists and has an active segment descriptor.
    :type wait: bool
    :param timeout: Optional maximum wait duration in seconds, or strings such as ``"30s"``.
    :type timeout: float | str | None
    :returns: Started subscriber handle.
    :rtype: StreamSubscriber
    """
    selected_master = master or _default_master()
    remote_endpoint = _remote_gateway_endpoint(selected_master)
    if remote_endpoint is not None:
        remote_token = _remote_gateway_token(selected_master)
        return RemoteStreamSubscriber(
            source,
            endpoint=remote_endpoint,
            callback=callback,
            table_callback=True,
            filter=filter,
            batch_size=batch_size,
            throttle_ms=throttle_ms,
            count=count,
            token=remote_token,
        ).start()

    subscriber_kwargs = {
        "source": source,
        "callback": callback,
        "master": selected_master,
        "poll_interval_ms": poll_interval_ms,
        "xfast": xfast,
        "idle_spin_checks": idle_spin_checks,
        "wait": wait,
        "timeout": timeout,
        "_table_callback": True,
    }
    if filter is not None:
        subscriber_kwargs["filter"] = filter
    if batch_size is not None:
        subscriber_kwargs["batch_size"] = batch_size
    if throttle_ms is not None:
        subscriber_kwargs["throttle_ms"] = throttle_ms
    if count is not None:
        subscriber_kwargs["count"] = count
    subscriber = StreamSubscriber(**subscriber_kwargs)
    return subscriber.start()


class ParquetPersist:
    """
    Configure parquet persistence output for a stream table.

    :param path: Directory used to store parquet persisted files.
    :type path: str | os.PathLike[str]
    """

    __slots__ = ("path",)

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = str(Path(path).expanduser())

    def _zippy_persist_path(self) -> str:
        return self.path


class _ParquetReplayHandle:
    """Background runtime handle for ``ParquetReplaySource``."""

    def __init__(self, source: "ParquetReplaySource", sink) -> None:
        self._source = source
        self._sink = sink
        self._stop_event = threading.Event()
        self._replay_done_event = threading.Event()
        self._stopped_event = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-parquet-replay-{source.source_name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Request the replay thread to stop."""
        self._stop_event.set()

    def join(self) -> None:
        """Wait for the replay thread to finish."""
        self._thread.join()

    def wait_replay(self, timeout: float | None = None) -> bool:
        """Wait until all replay rows have been emitted and flushed."""
        completed = self._replay_done_event.wait(timeout)
        if completed and self._error is not None:
            raise RuntimeError(f"parquet replay failed reason=[{self._error}]")
        return completed

    def _run(self) -> None:
        try:
            self._sink.emit_hello(self._source.source_name)
            rate_limiter = _ReplayRateLimiter(self._source.replay_rate)
            for table in self._source._iter_tables():
                if self._stop_event.is_set():
                    break
                if rate_limiter.enabled:
                    for row_index in range(table.num_rows):
                        if self._stop_event.is_set():
                            break
                        rate_limiter.wait_before_emit()
                        self._sink.emit_data(table.slice(row_index, 1))
                else:
                    self._sink.emit_data(table)
            self._sink.emit_flush()
            self._replay_done_event.set()
            self._stop_event.wait()
            try:
                self._sink.emit_stop()
            except RuntimeError as error:
                if "status=[stopped]" not in str(error):
                    raise
        except Exception as error:
            self._error = error
            self._replay_done_event.set()
            try:
                self._sink.emit_error(str(error))
            except RuntimeError as emit_error:
                if "status=[stopped]" not in str(emit_error):
                    raise
            finally:
                self._stopped_event.set()
        else:
            self._stopped_event.set()


def _normalize_parquet_replay_paths(value: object) -> str | list[str]:
    if isinstance(value, (str, os.PathLike)):
        return str(Path(value).expanduser())

    try:
        paths = [str(Path(item).expanduser()) for item in value]  # type: ignore[arg-type]
    except TypeError as error:
        raise TypeError("parquet replay source must be a path or a sequence of paths") from error
    if not paths:
        raise ValueError("parquet replay source path list must not be empty")
    return paths


def _iter_arrow_rows(table):
    for row_index in range(table.num_rows):
        values = {}
        for column_name in table.column_names:
            values[column_name] = _arrow_scalar_to_python(table[column_name][row_index])
        yield Row(values)


def _arrow_scalar_to_python(value):
    try:
        return value.as_py()
    except ValueError:
        scalar_value = getattr(value, "value", None)
        if scalar_value is not None:
            return scalar_value
        raise


def _normalize_replay_timing(mode: str, replay_rate: object) -> tuple[str, float | None]:
    if replay_rate is not None:
        try:
            rate = float(replay_rate)
        except (TypeError, ValueError) as error:
            raise TypeError("replay_rate must be numeric rows per second") from error
        if rate <= 0.0:
            raise ValueError("replay_rate must be greater than zero")
        if mode not in {"as_fast_as_possible", "fixed_rate"}:
            raise ValueError("mode must be 'as_fast_as_possible' or 'fixed_rate'")
        return "fixed_rate", rate

    if mode == "fixed_rate":
        raise ValueError("replay_rate is required when mode is 'fixed_rate'")
    if mode != "as_fast_as_possible":
        raise ValueError("mode must be 'as_fast_as_possible' or 'fixed_rate'")
    return mode, None


class _ReplayRateLimiter:
    def __init__(self, replay_rate: float | None) -> None:
        self.replay_rate = replay_rate
        self.enabled = replay_rate is not None
        self._start_time: float | None = None
        self._emitted_rows = 0

    def wait_before_emit(self) -> None:
        if self.replay_rate is None:
            return
        if self._start_time is None:
            self._start_time = time.monotonic()
            self._emitted_rows = 1
            return

        target_time = self._start_time + (self._emitted_rows / self.replay_rate)
        delay = target_time - time.monotonic()
        if delay > 0.0:
            time.sleep(delay)
        self._emitted_rows += 1


def _filter_replay_table(table, time_column: str | None, start: object, end: object):
    if start is None and end is None:
        return table
    if time_column is None:
        raise ValueError("time_column is required when start or end is provided")
    if time_column not in table.column_names:
        raise ValueError(f"time_column not found column=[{time_column}]")

    import pyarrow as pa
    import pyarrow.compute as pc

    values = table[time_column]
    mask = None
    if start is not None:
        start_scalar = pa.scalar(start, type=values.type)
        mask = pc.greater_equal(values, start_scalar)
    if end is not None:
        end_scalar = pa.scalar(end, type=values.type)
        end_mask = pc.less_equal(values, end_scalar)
        mask = end_mask if mask is None else pc.and_(mask, end_mask)
    return table.filter(mask)


class ParquetReplaySource:
    """
    Replay parquet data as a low-level Zippy Python source plugin.

    Most users should prefer :class:`ParquetReplayEngine` for explicit parquet paths, or
    :class:`TableReplayEngine` / :func:`replay` for persisted Zippy table names. Use this
    class directly only when custom pipeline wiring is required.

    :param path: Parquet file, directory, or explicit file list.
    :type path: str | os.PathLike[str] | list[str | os.PathLike[str]]
    :param schema: Optional output Arrow schema. When omitted, schema is read from parquet.
    :type schema: pyarrow.Schema | None
    :param batch_size: Maximum rows emitted per batch.
    :type batch_size: int
    :param source_name: Control-plane source name.
    :type source_name: str | None
    :param mode: Replay timing mode. Use ``"as_fast_as_possible"`` or ``"fixed_rate"``.
    :type mode: str
    :param replay_rate: Fixed replay rate in rows per second. When provided, fixed-rate replay
        is enabled.
    :type replay_rate: float | None
    :param time_column: Column used for inclusive ``start`` / ``end`` filtering.
    :type time_column: str | None
    :param start: Optional inclusive lower bound for ``time_column``.
    :type start: object
    :param end: Optional inclusive upper bound for ``time_column``.
    :type end: object
    """

    def __init__(
        self,
        path,
        schema=None,
        *,
        batch_size: int = 65_536,
        source_name: str | None = None,
        mode: str = "as_fast_as_possible",
        replay_rate: float | None = None,
        time_column: str | None = "dt",
        start: object = None,
        end: object = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        mode, replay_rate = _normalize_replay_timing(mode, replay_rate)
        self.path = _normalize_parquet_replay_paths(path)
        self.schema = schema
        self.batch_size = int(batch_size)
        self.source_name = source_name or "parquet_replay"
        self.mode = mode
        self.replay_rate = replay_rate
        self.time_column = time_column
        self.start_bound = start
        self.end_bound = end
        self._last_handle: _ParquetReplayHandle | None = None

    def _zippy_output_schema(self):
        if self.schema is None:
            import pyarrow.dataset as ds

            self.schema = ds.dataset(self.path, format="parquet").schema
        return self.schema

    def _zippy_source_name(self) -> str:
        return self.source_name

    def _zippy_source_type(self) -> str:
        return "parquet_replay"

    def _zippy_start(self, sink) -> _ParquetReplayHandle:
        handle = _ParquetReplayHandle(self, sink)
        self._last_handle = handle
        return handle

    def wait_replay(self, timeout: float | None = None) -> bool:
        """
        Wait until the active replay handle has emitted and flushed all rows.

        :param timeout: Maximum seconds to wait. ``None`` waits indefinitely.
        :type timeout: float | None
        :returns: ``True`` when replay completed, ``False`` on timeout.
        :rtype: bool
        :raises RuntimeError: If the replay thread failed.
        """
        if self._last_handle is None:
            raise RuntimeError("parquet replay source is not started")
        return self._last_handle.wait_replay(timeout)

    def _iter_tables(self):
        import pyarrow as pa
        import pyarrow.dataset as ds

        scanner = ds.dataset(self.path, format="parquet").scanner(batch_size=self.batch_size)
        expected_schema = self._zippy_output_schema()
        for batch in scanner.to_batches():
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            if table.schema != expected_schema:
                table = table.cast(expected_schema)
            table = _filter_replay_table(
                table,
                self.time_column,
                self.start_bound,
                self.end_bound,
            )
            if table.num_rows == 0:
                continue
            yield table


class _CallbackReplayHandle:
    """Background handle for direct row-callback replay."""

    def __init__(self, source: ParquetReplaySource, callback) -> None:
        self._source = source
        self._callback = callback
        self._stop_event = threading.Event()
        self._done_event = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._run,
            name=f"zippy-callback-replay-{source.source_name}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Request callback replay to stop."""
        self._stop_event.set()

    def join(self) -> None:
        """Wait for callback replay to stop."""
        self._thread.join()

    def wait_replay(self, timeout: float | None = None) -> bool:
        """Wait until callback replay completes."""
        completed = self._done_event.wait(timeout)
        if completed and self._error is not None:
            raise RuntimeError(f"callback replay failed reason=[{self._error}]")
        return completed

    def _run(self) -> None:
        try:
            rate_limiter = _ReplayRateLimiter(self._source.replay_rate)
            for table in self._source._iter_tables():
                if self._stop_event.is_set():
                    break
                for row in _iter_arrow_rows(table):
                    if self._stop_event.is_set():
                        break
                    rate_limiter.wait_before_emit()
                    self._callback(row)
        except Exception as error:
            self._error = error
        finally:
            self._done_event.set()


class ParquetReplayEngine:
    """
    Replay parquet data either into callbacks or into a named Zippy stream table.

    This class is the explicit path-based replay API. Most user-facing replay should
    use :class:`TableReplayEngine`, which starts from a persisted Zippy table name.

    ``start`` and ``end`` are inclusive bounds over ``time_column``. They are applied
    before rows are emitted, so both callback replay and stream replay see the same
    filtered row set. ``replay_rate`` enables fixed-rate replay in rows per second.
    """

    def __init__(
        self,
        source,
        *,
        output_stream: str | None = None,
        callback=None,
        schema=None,
        master: MasterClient | None = None,
        name: str | None = None,
        source_name: str | None = None,
        batch_size: int = 65_536,
        mode: str = "as_fast_as_possible",
        replay_rate: float | None = None,
        buffer_size: int = 64,
        frame_size: int = 4096,
        row_capacity: int | None = None,
        retention_segments: int | None = None,
        dt_column: str | None = None,
        id_column: str | None = None,
        dt_part: str | None = None,
        persist=None,
        data_dir: str | os.PathLike[str] | None = None,
        persist_path: ParquetPersist | str | os.PathLike[str] | None = None,
        time_column: str | None = "dt",
        start: object = None,
        end: object = None,
    ) -> None:
        if (output_stream is None) == (callback is None):
            raise ValueError("exactly one of output_stream or callback is required")
        if output_stream is not None and not isinstance(output_stream, str):
            raise TypeError("output_stream must be a string")
        if output_stream == "":
            raise ValueError("output_stream must not be empty")
        if callback is not None and not callable(callback):
            raise TypeError("callback must be callable")
        self.source = _normalize_parquet_replay_paths(source)
        self.output_stream = output_stream
        self.callback = callback
        self.schema = schema
        self.master = master
        self.name = name or f"replay_{output_stream or 'callback'}"
        self.source_name = source_name
        self.batch_size = batch_size
        self.mode, self.replay_rate = _normalize_replay_timing(mode, replay_rate)
        self.buffer_size = buffer_size
        self.frame_size = frame_size
        self.row_capacity = row_capacity
        self.retention_segments = retention_segments
        self.dt_column = dt_column
        self.id_column = id_column
        self.dt_part = dt_part
        self.persist = persist
        self.data_dir = data_dir
        self.persist_path = persist_path
        self.time_column = time_column
        self.start_bound = start
        self.end_bound = end
        self._source: ParquetReplaySource | None = None
        self._pipeline: Pipeline | None = None
        self._callback_handle: _CallbackReplayHandle | None = None
        self._status = "created"

    def start(self) -> "ParquetReplayEngine":
        """Start replay without waiting for completion."""
        if self._status == "running":
            return self
        if self._status in {"completed", "stopped"}:
            raise RuntimeError("replay engine cannot be restarted after completion")
        if self._status == "created":
            self.init()

        if self.callback is not None:
            if self._source is None:
                raise RuntimeError("replay source is not initialized")
            self._callback_handle = _CallbackReplayHandle(self._source, self.callback)
            self._status = "running"
            return self

        if self._pipeline is None:
            raise RuntimeError("replay pipeline is not initialized")
        self._pipeline.start()
        self._status = "running"
        return self

    def init(self) -> "ParquetReplayEngine":
        """
        Prepare replay resources without emitting data.

        For ``output_stream`` replay this registers the stream and publishes its initial
        descriptor so downstream subscribers can attach before :meth:`run` starts replay.
        """
        if self._status in {"initialized", "running"}:
            return self
        if self._status in {"completed", "stopped"}:
            raise RuntimeError("replay engine cannot be initialized after completion")

        source_name = self.source_name or self._default_source_name()
        source = ParquetReplaySource(
            self.source,
            schema=self.schema,
            batch_size=self.batch_size,
            source_name=source_name,
            mode=self.mode,
            replay_rate=self.replay_rate,
            time_column=self.time_column,
            start=self.start_bound,
            end=self.end_bound,
        )
        self._source = source
        if self.callback is not None:
            self._status = "initialized"
            return self

        master = self.master or _default_master()
        self.master = master
        _ensure_master_process(master, self.name)
        pipeline = (
            Pipeline(self.name, master=master)
            .source(source)
            .stream_table(
                self.output_stream,
                schema=self.schema,
                buffer_size=self.buffer_size,
                frame_size=self.frame_size,
                row_capacity=self.row_capacity,
                retention_segments=self.retention_segments,
                dt_column=self.dt_column,
                id_column=self.id_column,
                dt_part=self.dt_part,
                persist=self.persist,
                data_dir=self.data_dir,
                persist_path=self.persist_path,
            )
        )
        self._pipeline = pipeline
        self._status = "initialized"
        return self

    def run(
        self,
        *,
        wait: bool = True,
        timeout: float | None = None,
    ) -> "ParquetReplayEngine":
        """Start replay and optionally wait until all rows are emitted."""
        self.start()
        if wait:
            self.wait(timeout=timeout)
        return self

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until replay rows have been emitted."""
        if self._callback_handle is not None:
            completed = self._callback_handle.wait_replay(timeout)
            if completed:
                self._status = "completed"
            elif not completed:
                raise TimeoutError("callback replay did not finish")
            return True

        if self._source is None:
            raise RuntimeError("replay engine is not started")
        completed = self._source.wait_replay(timeout)
        if not completed:
            raise TimeoutError(
                f"table replay did not finish output_stream=[{self.output_stream}]"
            )
        return True

    def stop(self) -> None:
        """Stop replay and release runtime resources."""
        if self._callback_handle is not None:
            self._callback_handle.stop()
            self._callback_handle.join()
        if self._pipeline is not None:
            self._pipeline.stop()
        if self._status != "completed":
            self._status = "stopped"

    def status(self) -> str:
        """Return lifecycle status."""
        return self._status

    def output_schema(self):
        """Return the replay output Arrow schema."""
        if self._source is not None:
            return self._source._zippy_output_schema()
        return ParquetReplaySource(
            self.source,
            schema=self.schema,
            batch_size=self.batch_size,
            source_name=self.source_name,
            mode=self.mode,
            replay_rate=self.replay_rate,
            time_column=self.time_column,
            start=self.start_bound,
            end=self.end_bound,
        )._zippy_output_schema()

    def table(self) -> Table:
        """Open the replay output as a queryable Zippy table."""
        if self.output_stream is None:
            raise RuntimeError("callback replay does not create an output stream")
        if self.master is None:
            self.master = _default_master()
        return read_table(self.output_stream, master=self.master)

    def __enter__(self) -> "ParquetReplayEngine":
        return self.init()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def _default_source_name(self) -> str:
        if self.master is not None:
            get_process_id = getattr(self.master, "process_id", None)
            if callable(get_process_id):
                process_id = get_process_id()
                if process_id:
                    suffix = process_id
                    return f"{self.name}.{self.output_stream or 'callback'}.source.{suffix}"
        return f"{self.name}.{self.output_stream or 'callback'}.source.{id(self)}"


class TableReplayEngine:
    """
    Replay persisted data from a named Zippy table.

    ``TableReplayEngine`` accepts a table name, resolves its registered parquet
    ``persisted_files`` from master, then replays those files either to row callbacks
    or to another named stream table.

    ``start`` and ``end`` are inclusive bounds over ``time_column``. Use
    ``time_column="dt"`` for event-time windows, or a sequence column for deterministic
    sequence-range replay. ``replay_rate`` enables fixed-rate replay in rows per second.
    """

    def __init__(
        self,
        source: str,
        *,
        output_stream: str | None = None,
        callback=None,
        schema=None,
        master: MasterClient | None = None,
        name: str | None = None,
        source_name: str | None = None,
        batch_size: int = 65_536,
        mode: str = "as_fast_as_possible",
        replay_rate: float | None = None,
        buffer_size: int = 64,
        frame_size: int = 4096,
        row_capacity: int | None = None,
        retention_segments: int | None = None,
        dt_column: str | None = None,
        id_column: str | None = None,
        dt_part: str | None = None,
        persist=None,
        data_dir: str | os.PathLike[str] | None = None,
        persist_path: ParquetPersist | str | os.PathLike[str] | None = None,
        time_column: str | None = "dt",
        start: object = None,
        end: object = None,
    ) -> None:
        if not isinstance(source, str):
            raise TypeError("source must be a persisted Zippy table name")
        if not source:
            raise ValueError("source table name must not be empty")
        if (output_stream is None) == (callback is None):
            raise ValueError("exactly one of output_stream or callback is required")
        self.source = source
        self.output_stream = output_stream
        self.callback = callback
        self.schema = schema
        self.master = master or _default_master()
        self.name = name or f"replay_{output_stream or source}"
        self.source_name = source_name
        self.batch_size = batch_size
        self.mode, self.replay_rate = _normalize_replay_timing(mode, replay_rate)
        self.buffer_size = buffer_size
        self.frame_size = frame_size
        self.row_capacity = row_capacity
        self.retention_segments = retention_segments
        self.dt_column = dt_column
        self.id_column = id_column
        self.dt_part = dt_part
        self.persist = persist
        self.data_dir = data_dir
        self.persist_path = persist_path
        self.time_column = time_column
        self.start_bound = start
        self.end_bound = end
        self._delegate: ParquetReplayEngine | None = None

    def start(self) -> "TableReplayEngine":
        """Start replay without waiting for completion."""
        if self._delegate is not None and self._delegate.status() == "running":
            return self
        if self._delegate is None:
            self.init()
        elif self._delegate.status() != "initialized":
            raise RuntimeError("table replay engine cannot be restarted after completion")

        if self._delegate is None:
            raise RuntimeError("table replay engine is not initialized")
        self._delegate.start()
        return self

    def init(self) -> "TableReplayEngine":
        """
        Prepare replay resources without emitting data.

        For ``output_stream`` replay this makes the output stream visible to master so a
        downstream subscriber can attach before replay starts.
        """
        if self._delegate is not None:
            if self._delegate.status() in {"initialized", "running"}:
                return self
            raise RuntimeError("table replay engine cannot be initialized after completion")
        paths, schema = self._resolve_persisted_paths_and_schema()
        self._delegate = ParquetReplayEngine(
            paths,
            output_stream=self.output_stream,
            callback=self.callback,
            schema=schema,
            master=self.master,
            name=self.name,
            source_name=self.source_name,
            batch_size=self.batch_size,
            mode=self.mode,
            replay_rate=self.replay_rate,
            buffer_size=self.buffer_size,
            frame_size=self.frame_size,
            row_capacity=self.row_capacity,
            retention_segments=self.retention_segments,
            dt_column=self.dt_column,
            id_column=self.id_column,
            dt_part=self.dt_part,
            persist=self.persist,
            data_dir=self.data_dir,
            persist_path=self.persist_path,
            time_column=self.time_column,
            start=self.start_bound,
            end=self.end_bound,
        ).init()
        return self

    def run(
        self,
        *,
        wait: bool = True,
        timeout: float | None = None,
    ) -> "TableReplayEngine":
        """Start replay and optionally wait until all rows are emitted."""
        self.start()
        if wait:
            self.wait(timeout=timeout)
        return self

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until replay rows have been emitted."""
        if self._delegate is None:
            raise RuntimeError("table replay engine is not started")
        return self._delegate.wait(timeout=timeout)

    def stop(self) -> None:
        """Stop replay and release runtime resources."""
        if self._delegate is not None:
            self._delegate.stop()

    def status(self) -> str:
        """Return lifecycle status."""
        if self._delegate is None:
            return "created"
        return self._delegate.status()

    def output_schema(self):
        """Return replay output schema."""
        if self._delegate is not None:
            return self._delegate.output_schema()
        return self.schema or read_table(self.source, master=self.master).schema()

    def table(self) -> Table:
        """Open the replay output as a queryable Zippy table."""
        if self.output_stream is None:
            raise RuntimeError("callback replay does not create an output stream")
        return read_table(self.output_stream, master=self.master)

    def __enter__(self) -> "TableReplayEngine":
        return self.init()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def _resolve_persisted_paths_and_schema(self) -> tuple[list[str], object]:
        table = read_table(self.source, master=self.master)
        persisted_files = table.persisted_files()
        paths = [
            str(item["file_path"])
            for item in sorted(persisted_files, key=_persisted_file_order_key)
            if item.get("file_path")
        ]
        if not paths:
            raise RuntimeError(f"persisted files are not registered source=[{self.source}]")
        return paths, self.schema or table.schema()


def replay(
    source: str,
    *,
    output_stream: str | None = None,
    callback=None,
    schema=None,
    master: MasterClient | None = None,
    name: str | None = None,
    source_name: str | None = None,
    batch_size: int = 65_536,
    mode: str = "as_fast_as_possible",
    replay_rate: float | None = None,
    buffer_size: int = 64,
    frame_size: int = 4096,
    row_capacity: int | None = None,
    retention_segments: int | None = None,
    dt_column: str | None = None,
    id_column: str | None = None,
    dt_part: str | None = None,
    persist=None,
    data_dir: str | os.PathLike[str] | None = None,
    persist_path: ParquetPersist | str | os.PathLike[str] | None = None,
    time_column: str | None = "dt",
    start: object = None,
    end: object = None,
    wait: bool = True,
    timeout: float | None = None,
) -> TableReplayEngine:
    """
    Replay persisted data from a named Zippy table.

    :param source: Persisted Zippy table name.
    :type source: str
    :param output_stream: Optional replay output stream table.
    :type output_stream: str | None
    :param callback: Optional row callback receiving :class:`Row`.
    :type callback: callable | None
    :param replay_rate: Fixed replay rate in rows per second. When provided, fixed-rate replay
        is enabled.
    :type replay_rate: float | None
    :param time_column: Column used for inclusive ``start`` / ``end`` filtering.
    :type time_column: str | None
    :param start: Optional inclusive lower bound for ``time_column``.
    :type start: object
    :param end: Optional inclusive upper bound for ``time_column``.
    :type end: object
    :param wait: Whether to wait until replay rows are emitted before returning.
    :type wait: bool
    :returns: Table replay engine.
    :rtype: TableReplayEngine
    """
    engine = TableReplayEngine(
        source,
        output_stream=output_stream,
        callback=callback,
        schema=schema,
        master=master,
        name=name,
        source_name=source_name,
        batch_size=batch_size,
        mode=mode,
        replay_rate=replay_rate,
        buffer_size=buffer_size,
        frame_size=frame_size,
        row_capacity=row_capacity,
        retention_segments=retention_segments,
        dt_column=dt_column,
        id_column=id_column,
        dt_part=dt_part,
        persist=persist,
        data_dir=data_dir,
        persist_path=persist_path,
        time_column=time_column,
        start=start,
        end=end,
    )
    return engine.run(wait=wait, timeout=timeout)


_ACTIVE_SESSIONS: dict[str, object] = {}


def _engine_latest_by(engine: object) -> list[str] | None:
    config_fn = getattr(engine, "config", None)
    if not callable(config_fn):
        return None
    config = config_fn()
    if not isinstance(config, dict) or config.get("engine_type") != "reactive_latest":
        return None
    by = config.get("by")
    if isinstance(by, str):
        return [by]
    if by is None:
        return None
    return [str(item) for item in by]


class Session:
    """
    Own and run a small group of Python-configured Zippy engines.

    ``Session`` is intentionally a thin orchestration layer: it supplies the shared
    master connection, applies practical defaults, and manages engine lifecycle.

    :param name: Optional session/process name used when a process lease is needed.
    :type name: str | None
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    :param uri: Optional master URI used to create a default connection for this session.
    :type uri: str | None
    :param app: Optional process name. When omitted, ``name`` is used.
    :type app: str | None
    :param heartbeat_interval_sec: Process lease heartbeat interval when ``uri`` is used.
    :type heartbeat_interval_sec: float
    """

    def __init__(
        self,
        name: str | None = None,
        *,
        master: MasterClient | None = None,
        uri: str | None = None,
        app: str | None = None,
        heartbeat_interval_sec: float = _DEFAULT_HEARTBEAT_INTERVAL_SEC,
    ) -> None:
        self.name = name or app or "zippy_session"
        self.app = app or name
        if master is not None:
            self.master = master
        elif uri is not None:
            self.master = connect(
                uri=uri,
                app=self.app,
                heartbeat_interval_sec=heartbeat_interval_sec,
            )
        else:
            self.master = _default_master()
        self._engines: list[object] = []
        self._runtime_engines: list[object] = []
        self._pending_output_tables: dict[int, tuple[str, bool]] = {}
        self._materialized_engine_ids: set[int] = set()
        self._materializer_source_names: list[str] = []
        self._started = False
        self._needs_master_process = False

    def engine(self, engine=None, /, **kwargs) -> "Session":
        """
        Add an engine instance or build an engine from an engine class.

        :param engine: Engine instance or engine class, for example
            ``zippy.ReactiveLatestEngine``.
        :type engine: object
        :param kwargs: Constructor arguments when ``engine`` is a class.
            ``output_stream="name"`` remains a shortcut for ``.stream_table("name")``.
            Prefer ``.stream_table(..., persist=True|False)`` for new code.
        :type kwargs: object
        :returns: This session for fluent ``.engine(...).run()`` usage.
        :rtype: Session
        :raises TypeError: If no engine is supplied or the object is not lifecycle-compatible.
        """
        if engine is None and "engine" in kwargs:
            engine = kwargs.pop("engine")
        if engine is None:
            raise TypeError("engine() requires an engine instance or engine class")

        materializers: list[object] = []
        if isinstance(engine, type):
            engine_obj, materializers, pending_output = self._build_engine(engine, kwargs)
        else:
            if kwargs:
                raise TypeError("engine instance does not accept constructor keyword arguments")
            engine_obj = engine
            pending_output = None

        self._validate_engine(engine_obj)
        for materializer in materializers:
            self._validate_engine(materializer)
        self._engines.append(engine_obj)
        self._runtime_engines.extend(materializers)
        self._runtime_engines.append(engine_obj)
        if materializers:
            self._materialized_engine_ids.add(id(engine_obj))
        if pending_output is not None:
            self._pending_output_tables[id(engine_obj)] = pending_output
        return self

    def stream_table(self, name: str, *, persist: bool = False) -> "Session":
        """
        Materialize the latest engine output into a named stream table.

        :param name: Output stream table name.
        :type name: str
        :param persist: Whether to persist the stream table as parquet.
        :type persist: bool
        :returns: This session for fluent ``.stream_table(...).run()`` usage.
        :rtype: Session
        :raises RuntimeError: If no engine has been added.
        :raises ValueError: If the latest engine already has an output table.
        """
        if not isinstance(name, str):
            raise TypeError("stream table name must be a string")
        if not name:
            raise ValueError("stream table name must not be empty")
        if not isinstance(persist, bool):
            raise TypeError("persist must be True or False")
        if not self._engines:
            raise RuntimeError("stream_table() requires an engine() before it")

        engine = self._engines[-1]
        engine_id = id(engine)
        if engine_id in self._materialized_engine_ids:
            raise ValueError("latest engine output is already materialized")

        self._pending_output_tables.pop(engine_id, None)
        self._attach_engine_output_table(engine, name, persist=persist)
        return self

    def engines(self) -> tuple[object, ...]:
        """
        Return engines owned by this session.

        :returns: Engine objects in start order.
        :rtype: tuple[object, ...]
        """
        return tuple(self._engines)

    def start(self) -> "Session":
        """
        Start all owned engines.

        :returns: This session.
        :rtype: Session
        """
        self._materialize_pending_output_tables()
        if self._needs_master_process:
            _ensure_master_process(self.master, self.app or self.name)
        for engine in self._runtime_engines:
            if self._engine_status(engine) == "running":
                continue
            engine.start()
        self._started = True
        _ACTIVE_SESSIONS[self.name] = self
        return self

    def run(self) -> "Session":
        """
        Start all owned engines.

        :returns: This session.
        :rtype: Session
        """
        return self.start()

    def stop(self) -> None:
        """Stop all running engines in reverse order."""
        first_error: BaseException | None = None
        for engine in reversed(self._runtime_engines):
            if self._engine_status(engine) in {"created", "stopped"}:
                continue
            try:
                engine.stop()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        try:
            self._unregister_materializer_sources()
        except BaseException as error:
            if first_error is None:
                first_error = error
        self._started = False
        if _ACTIVE_SESSIONS.get(self.name) is self:
            _ACTIVE_SESSIONS.pop(self.name, None)
        if first_error is not None:
            raise first_error

    def __enter__(self) -> "Session":
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()

    def _build_engine(self, engine_cls: type, kwargs: dict[str, object]):
        explicit_target = "target" in kwargs
        if "output" in kwargs:
            raise TypeError("output is not supported; use output_stream or .stream_table(...)")
        output_stream = kwargs.pop("output_stream", None)
        if output_stream is not None and not isinstance(output_stream, str):
            raise TypeError("output_stream must be a stream table name")
        if output_stream == "":
            raise ValueError("output_stream must not be empty")
        if output_stream is not None and explicit_target:
            raise ValueError("output_stream cannot be combined with explicit target")
        persist = kwargs.pop("persist", False)
        if not isinstance(persist, bool):
            raise TypeError("persist must be True or False")
        source_is_named_stream = isinstance(kwargs.get("source"), str)
        output_table_name = (
            output_stream
            if output_stream is not None
            else (
                str(kwargs["name"])
                if source_is_named_stream and not explicit_target and kwargs.get("name")
                else None
            )
        )
        if persist and output_table_name is None:
            raise ValueError("persist=True requires automatic output table materialization")

        if not explicit_target:
            kwargs["target"] = NullPublisher()
        if source_is_named_stream:
            kwargs.setdefault("master", self.master)
            self._needs_master_process = True
        engine = engine_cls(**kwargs)

        materializers: list[object] = []
        pending_output = None
        if output_stream is not None and callable(getattr(engine, "output_schema", None)):
            materializers.append(
                self._materialize_engine_output(
                    engine,
                    output_stream,
                    persist=persist,
                )
            )
        elif output_table_name is not None:
            pending_output = (output_table_name, persist)
        return engine, materializers, pending_output

    def _materialize_pending_output_tables(self) -> None:
        for engine in list(self._engines):
            engine_id = id(engine)
            pending_output = self._pending_output_tables.pop(engine_id, None)
            if pending_output is None:
                continue
            table_name, persist = pending_output
            if engine_id in self._materialized_engine_ids:
                continue
            self._attach_engine_output_table(engine, table_name, persist=persist)

    def _attach_engine_output_table(
        self,
        engine: object,
        table_name: str,
        *,
        persist: bool,
    ) -> None:
        materializer = self._materialize_engine_output(engine, table_name, persist=persist)
        engine_index = self._runtime_engines.index(engine)
        self._runtime_engines.insert(engine_index, materializer)
        self._materialized_engine_ids.add(id(engine))

    def _materialize_engine_output(
        self,
        engine: object,
        table_name: str,
        *,
        persist: bool,
    ) -> object:
        output_schema = engine.output_schema()
        table_options = _resolve_stream_table_options(
            name=table_name,
            master=self.master,
            row_capacity=None,
            retention_segments=None,
            dt_column=None,
            id_column=None,
            dt_part=None,
            persist=persist,
            data_dir=None,
            persist_path=None,
        )
        latest_by = _engine_latest_by(engine)
        if latest_by is not None and table_options["persist_path"] is not None:
            raise ValueError("ReactiveLatestEngine stream_table does not support persist=True")
        _ensure_master_process(self.master, self.app or self.name)
        self.master.register_stream(table_name, output_schema, 64, 4096)
        self._register_materializer_source(table_name)
        if latest_by is not None:
            materializer = _KeyValueTableMaterializer(
                name=table_name,
                input_schema=output_schema,
                by=latest_by,
                source=engine,
                target=NullPublisher(),
                descriptor_publisher=self._descriptor_publisher(table_name),
                row_capacity=table_options["row_capacity"],
                retention_guard=self._retention_guard(table_name),
                replacement_retention_snapshots=table_options[
                    "replacement_retention_snapshots"
                ],
            )
        else:
            materializer = _StreamTableMaterializer(
                name=table_name,
                input_schema=output_schema,
                source=engine,
                target=NullPublisher(),
                descriptor_publisher=self._descriptor_publisher(table_name),
                row_capacity=table_options["row_capacity"],
                retention_segments=table_options["retention_segments"],
                retention_guard=self._retention_guard(table_name),
                dt_column=table_options["dt_column"],
                id_column=table_options["id_column"],
                dt_part=table_options["dt_part"],
                persist_path=table_options["persist_path"],
                persist_publisher=(
                    self._persist_publisher(table_name)
                    if table_options["persist_path"] is not None
                    else None
                ),
            )
        self.master.publish_segment_descriptor(table_name, materializer.active_descriptor())
        self._needs_master_process = True
        return materializer

    def _register_materializer_source(self, table_name: str) -> None:
        register_source = getattr(self.master, "register_source", None)
        if register_source is None:
            return

        process_id = None
        get_process_id = getattr(self.master, "process_id", None)
        if callable(get_process_id):
            process_id = get_process_id()
        suffix = process_id or str(id(self))
        source_name = f"{self.name}.{table_name}.materializer.{suffix}"
        register_source(
            source_name,
            "session_engine_output",
            table_name,
            {"session": self.name},
        )
        if source_name not in self._materializer_source_names:
            self._materializer_source_names.append(source_name)

    def _unregister_materializer_sources(self) -> None:
        unregister_source = getattr(self.master, "unregister_source", None)
        if unregister_source is None:
            self._materializer_source_names.clear()
            return

        first_error: BaseException | None = None
        for source_name in reversed(self._materializer_source_names):
            try:
                unregister_source(source_name)
            except RuntimeError as error:
                if "source not found" not in str(error):
                    first_error = first_error or error
            except BaseException as error:
                first_error = first_error or error
        if first_error is None:
            self._materializer_source_names.clear()
            return
        raise first_error

    def _descriptor_publisher(self, stream_name: str):
        def publish(payload) -> None:
            if isinstance(payload, (bytes, bytearray, memoryview)):
                descriptor = json.loads(bytes(payload).decode("utf-8"))
            elif isinstance(payload, str):
                descriptor = json.loads(payload)
            else:
                descriptor = payload
            self.master.publish_segment_descriptor(stream_name, descriptor)

        return publish

    def _persist_publisher(self, stream_name: str):
        def publish(payload) -> None:
            if isinstance(payload, (bytes, bytearray, memoryview)):
                persisted_file = json.loads(bytes(payload).decode("utf-8"))
            elif isinstance(payload, str):
                persisted_file = json.loads(payload)
            else:
                persisted_file = payload
            if (
                isinstance(persisted_file, dict)
                and persisted_file.get("persist_event_type") is not None
            ):
                publish_persist_event = getattr(self.master, "publish_persist_event", None)
                if publish_persist_event is None:
                    raise RuntimeError("master does not support publish_persist_event")
                publish_persist_event(stream_name, persisted_file)
                return
            publish_persisted_file = getattr(self.master, "publish_persisted_file", None)
            if publish_persisted_file is None:
                raise RuntimeError("master does not support publish_persisted_file")
            publish_persisted_file(stream_name, persisted_file)

        return publish

    def _retention_guard(self, stream_name: str):
        def can_release(segment_id: int, generation: int) -> bool:
            get_stream = getattr(self.master, "get_stream", None)
            if get_stream is None:
                return True
            stream = get_stream(stream_name)
            leases = stream.get("segment_reader_leases", [])
            for lease in leases:
                if not isinstance(lease, dict):
                    continue
                if _segment_identity(
                    lease.get("source_segment_id"),
                    lease.get("source_generation"),
                ) == (int(segment_id), int(generation)):
                    return False
            return True

        return can_release

    @staticmethod
    def _validate_engine(engine: object) -> None:
        if not callable(getattr(engine, "start", None)):
            raise TypeError("engine must provide start()")
        if not callable(getattr(engine, "stop", None)):
            raise TypeError("engine must provide stop()")

    @staticmethod
    def _engine_status(engine: object) -> str | None:
        status = getattr(engine, "status", None)
        if not callable(status):
            return None
        return status()


class Pipeline:
    """
    Own a simple Python-defined Zippy data pipeline.

    The first implementation focuses on one stream table materializer sink and hides
    stream/source registration plus active descriptor publication.

    :param name: Pipeline/process name.
    :type name: str
    :param master: Optional explicit master client. When omitted, ``zippy.connect()`` is used.
    :type master: MasterClient | None
    """

    def __init__(self, name: str, master: MasterClient | None = None) -> None:
        self.name = name
        self.master = master or _default_master()
        self._source = None
        self._source_name: str | None = None
        self._source_type = "pipeline"
        self._stream_name: str | None = None
        self._schema = None
        self._engine: _StreamTableMaterializer | None = None
        self._registered_source_name: str | None = None
        self._started = False

    def source(
        self,
        source,
        *,
        name: str | None = None,
        source_type: str | None = None,
    ) -> "Pipeline":
        """
        Attach a source object to the pipeline.

        :param source: Source object accepted by stream table materializer.
        :type source: object
        :param name: Optional control-plane source name.
        :type name: str | None
        :param source_type: Optional source type label.
        :type source_type: str | None
        :returns: This pipeline.
        :rtype: Pipeline
        """
        self._source = source
        self._source_name = name or self._call_optional_source_string("_zippy_source_name")
        self._source_type = (
            source_type
            or self._call_optional_source_string("_zippy_source_type")
            or source.__class__.__name__
        )
        return self

    def stream_table(
        self,
        name: str,
        *,
        schema=None,
        buffer_size: int = 64,
        frame_size: int = 4096,
        row_capacity: int | None = None,
        retention_segments: int | None = None,
        dt_column: str | None = None,
        id_column: str | None = None,
        dt_part: str | None = None,
        persist=_USE_MASTER_CONFIG,
        data_dir: str | os.PathLike[str] | None = None,
        persist_path: ParquetPersist | str | os.PathLike[str] | None = None,
    ) -> "Pipeline":
        """
        Materialize pipeline input into a named stream table.

        :param name: Named stream table.
        :type name: str
        :param schema: Arrow schema for the stream. When omitted, ``source._zippy_output_schema()``
            is used if available.
        :type schema: pyarrow.Schema | None
        :param buffer_size: Control-plane bus compatibility buffer size.
        :type buffer_size: int
        :param frame_size: Control-plane bus compatibility frame size.
        :type frame_size: int
        :param row_capacity: Optional active segment row capacity before rollover.
        :type row_capacity: int | None
        :param retention_segments: Optional sealed segment count retained in live descriptors.
        :type retention_segments: int | None
        :param dt_column: Optional timestamp column used to derive parquet date partitions.
        :type dt_column: str | None
        :param id_column: Optional identifier column used for parquet partitioning.
        :type id_column: str | None
        :param dt_part: Date partition format derived from ``dt_column``.
        :type dt_part: str | None
        :param persist: Optional persistence method. Omit to use master config,
            pass ``"parquet"`` to force parquet, or ``None`` to disable.
        :type persist: str | None
        :param data_dir: Optional persistence root directory. Each stream writes below this root.
        :type data_dir: str | os.PathLike[str] | None
        :param persist_path: Explicit directory used to store this stream table's parquet files.
        :type persist_path: ParquetPersist | str | os.PathLike[str] | None
        :returns: This pipeline.
        :rtype: Pipeline
        """
        self._ensure_process()
        schema = schema or self._infer_source_schema()
        table_options = _resolve_stream_table_options(
            name=name,
            master=self.master,
            row_capacity=row_capacity,
            retention_segments=retention_segments,
            dt_column=dt_column,
            id_column=id_column,
            dt_part=dt_part,
            persist=persist,
            data_dir=data_dir,
            persist_path=persist_path,
        )
        self._stream_name = name
        self._schema = schema
        self.master.register_stream(name, schema, buffer_size, frame_size)
        source_name = self._source_name or f"{self.name}.{name}"
        self.master.register_source(
            source_name,
            self._source_type,
            name,
            {},
        )
        self._registered_source_name = source_name
        self._engine = _StreamTableMaterializer(
            name=name,
            input_schema=schema,
            source=self._source,
            target=NullPublisher(),
            descriptor_publisher=self._descriptor_publisher(name),
            row_capacity=table_options["row_capacity"],
            retention_segments=table_options["retention_segments"],
            retention_guard=self._retention_guard(name),
            dt_column=table_options["dt_column"],
            id_column=table_options["id_column"],
            dt_part=table_options["dt_part"],
            persist_path=table_options["persist_path"],
            persist_publisher=(
                self._persist_publisher(name)
                if table_options["persist_path"] is not None
                else None
            ),
            # Windows file-backed mappings can be deleted as soon as the
            # upstream source retires a segment, so default to a table-owned
            # live segment there unless callers opt in at the native layer.
            descriptor_forwarding=(
                table_options["persist_path"] is None
                and table_options["retention_segments"] is None
                and os.name != "nt"
            ),
        )
        self.master.publish_segment_descriptor(name, self._engine.active_descriptor())
        return self

    def start(self) -> "Pipeline":
        """
        Start the owned stream table engine.

        :returns: This pipeline.
        :rtype: Pipeline
        """
        engine = self._require_engine()
        if not self._started:
            engine.start()
            self._started = True
        return self

    def run_forever(self, *, poll_interval_sec: float = 1.0) -> "Pipeline":
        """
        Start the pipeline and block until interrupted.

        :param poll_interval_sec: Sleep interval used by the foreground wait loop.
        :type poll_interval_sec: float
        :returns: This pipeline after graceful shutdown.
        :rtype: Pipeline
        :raises ValueError: If ``poll_interval_sec`` is not positive.
        """
        if poll_interval_sec <= 0:
            raise ValueError("poll_interval_sec must be positive")

        self.start()
        try:
            while True:
                time.sleep(poll_interval_sec)
        except KeyboardInterrupt:
            return self
        finally:
            self.stop()

    def write(self, value) -> None:
        """
        Write data into the owned stream table.

        :param value: Value accepted by the owned stream table materializer.
        :type value: object
        """
        if not self._started:
            self.start()
        self._require_engine().write(value)

    def flush(self) -> None:
        """Flush the owned stream table engine."""
        self._require_engine().flush()

    def stop(self) -> None:
        """Stop the owned stream table engine."""
        first_error: BaseException | None = None
        if self._started and self._engine is not None:
            try:
                self._engine.stop()
            except BaseException as error:
                first_error = error
        self._started = False
        try:
            self._unregister_registered_source()
        except BaseException as error:
            if first_error is None:
                first_error = error
        if first_error is not None:
            raise first_error

    def _unregister_registered_source(self) -> None:
        source_name = self._registered_source_name
        if source_name is None:
            return
        unregister_source = getattr(self.master, "unregister_source", None)
        if unregister_source is None:
            self._registered_source_name = None
            return
        try:
            unregister_source(source_name)
        except RuntimeError as error:
            if "source not found" not in str(error):
                raise
        self._registered_source_name = None

    def _ensure_process(self) -> None:
        _ensure_master_process(self.master, self.name)

    def _infer_source_schema(self):
        if self._source is None or not hasattr(self._source, "_zippy_output_schema"):
            raise ValueError("stream_table schema is required when source does not expose schema")
        return self._source._zippy_output_schema()

    def _call_optional_source_string(self, method_name: str) -> str | None:
        if self._source is None or not hasattr(self._source, method_name):
            return None
        value = getattr(self._source, method_name)()
        if value is None:
            return None
        return str(value)

    def _descriptor_publisher(self, stream_name: str):
        def publish(payload) -> None:
            if isinstance(payload, (bytes, bytearray, memoryview)):
                descriptor = json.loads(bytes(payload).decode("utf-8"))
            elif isinstance(payload, str):
                descriptor = json.loads(payload)
            else:
                descriptor = payload
            self.master.publish_segment_descriptor(stream_name, descriptor)

        return publish

    def _persist_publisher(self, stream_name: str):
        def publish(payload) -> None:
            if isinstance(payload, (bytes, bytearray, memoryview)):
                persisted_file = json.loads(bytes(payload).decode("utf-8"))
            elif isinstance(payload, str):
                persisted_file = json.loads(payload)
            else:
                persisted_file = payload
            if (
                isinstance(persisted_file, dict)
                and persisted_file.get("persist_event_type") is not None
            ):
                publish_persist_event = getattr(self.master, "publish_persist_event", None)
                if publish_persist_event is None:
                    raise RuntimeError("master does not support publish_persist_event")
                publish_persist_event(stream_name, persisted_file)
                return
            publish_persisted_file = getattr(self.master, "publish_persisted_file", None)
            if publish_persisted_file is None:
                raise RuntimeError("master does not support publish_persisted_file")
            publish_persisted_file(stream_name, persisted_file)

        return publish

    def _retention_guard(self, stream_name: str):
        def can_release(segment_id: int, generation: int) -> bool:
            get_stream = getattr(self.master, "get_stream", None)
            if get_stream is None:
                return True
            stream = get_stream(stream_name)
            leases = stream.get("segment_reader_leases", [])
            for lease in leases:
                if not isinstance(lease, dict):
                    continue
                if _segment_identity(
                    lease.get("source_segment_id"),
                    lease.get("source_generation"),
                ) == (int(segment_id), int(generation)):
                    return False
            return True

        return can_release

    def _require_engine(self) -> _StreamTableMaterializer:
        if self._engine is None:
            raise RuntimeError("pipeline stream_table() must be configured before start/write")
        return self._engine


def _persist_path(value: ParquetPersist | str | os.PathLike[str] | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, ParquetPersist):
        return value._zippy_persist_path()
    if isinstance(value, (str, os.PathLike)):
        return str(Path(value).expanduser())
    raise TypeError("persist_path must be zippy.ParquetPersist or a path-like value")


def _master_config(master: MasterClient) -> dict[str, object]:
    get_config = getattr(master, "get_config", None)
    if get_config is None:
        return dict(_BUILTIN_CONFIG)
    return get_config()


def _resolve_stream_table_options(
    *,
    name: str,
    master: MasterClient,
    row_capacity: int | None,
    retention_segments: int | None,
    dt_column: str | None,
    id_column: str | None,
    dt_part: str | None,
    persist,
    data_dir: str | os.PathLike[str] | None,
    persist_path: ParquetPersist | str | os.PathLike[str] | None,
) -> dict[str, object]:
    if persist_path is not None and (persist is not _USE_MASTER_CONFIG or data_dir is not None):
        raise ValueError("persist_path cannot be combined with persist or data_dir")

    master_config = _master_config(master)
    table_config = master_config.get("table", {})
    if not isinstance(table_config, dict):
        table_config = {}
    persist_config = table_config.get("persist", {})
    if not isinstance(persist_config, dict):
        persist_config = {}
    partition_config = persist_config.get("partition", {})
    if not isinstance(partition_config, dict):
        partition_config = {}

    if row_capacity is None:
        row_capacity = int(table_config.get("row_capacity", 65_536))
    if row_capacity <= 0:
        raise ValueError("row_capacity must be greater than zero")

    if retention_segments is None and "retention_segments" in table_config:
        configured_retention_segments = table_config.get("retention_segments")
        if configured_retention_segments is not None:
            retention_segments = int(configured_retention_segments)
    if retention_segments is not None and retention_segments < 0:
        raise ValueError("retention_segments must be non-negative")
    replacement_retention_snapshots = int(
        table_config.get("replacement_retention_snapshots", 8)
    )
    if replacement_retention_snapshots <= 0:
        raise ValueError("replacement_retention_snapshots must be greater than zero")

    if dt_column is None:
        dt_column = _optional_config_string(partition_config.get("dt_column"))
    if id_column is None:
        id_column = _optional_config_string(partition_config.get("id_column"))
    if dt_part is None:
        dt_part = _optional_config_string(partition_config.get("dt_part"))
    _validate_partition_options(dt_column=dt_column, id_column=id_column, dt_part=dt_part)

    if persist_path is not None:
        return {
            "row_capacity": row_capacity,
            "retention_segments": retention_segments,
            "replacement_retention_snapshots": replacement_retention_snapshots,
            "dt_column": dt_column,
            "id_column": id_column,
            "dt_part": dt_part,
            "persist_path": _persist_path(persist_path),
        }

    if persist is _USE_MASTER_CONFIG:
        persist = (
            str(persist_config.get("method", "parquet"))
            if bool(persist_config.get("enabled", False))
            else None
        )
    if isinstance(persist, bool):
        persist = "parquet" if persist else None

    if persist is None:
        return {
            "row_capacity": row_capacity,
            "retention_segments": retention_segments,
            "replacement_retention_snapshots": replacement_retention_snapshots,
            "dt_column": dt_column,
            "id_column": id_column,
            "dt_part": dt_part,
            "persist_path": None,
        }
    if persist != "parquet":
        raise ValueError("persist must be 'parquet' or None")

    root = Path(data_dir or str(persist_config.get("data_dir", "data"))).expanduser()
    return {
        "row_capacity": row_capacity,
        "retention_segments": retention_segments,
        "replacement_retention_snapshots": replacement_retention_snapshots,
        "dt_column": dt_column,
        "id_column": id_column,
        "dt_part": dt_part,
        "persist_path": str(root / name),
    }


def _optional_config_string(value: object) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _validate_partition_options(
    *,
    dt_column: str | None,
    id_column: str | None,
    dt_part: str | None,
) -> None:
    if dt_part is not None and dt_column is None:
        raise ValueError("dt_part requires dt_column")
    if dt_column is not None and dt_part is None:
        raise ValueError("dt_column requires dt_part")
    if dt_part is not None and dt_part not in {"%Y", "%Y%m", "%Y%m%d", "%Y%m%d%H"}:
        raise ValueError("dt_part must be one of: %Y, %Y%m, %Y%m%d, %Y%m%d%H")


class _PolicyConstant:
    """Represent a predefined policy constant understood by the Rust bindings."""

    __slots__ = ("_zippy_constant_kind", "_zippy_constant_value", "_zippy_constant_name")

    def __init__(self, *, kind: str, value: str, name: str) -> None:
        self._zippy_constant_kind = kind
        self._zippy_constant_value = value
        self._zippy_constant_name = name

    def __repr__(self) -> str:
        return self._zippy_constant_name


class WindowType:
    """Namespace for predefined window-type constants."""

    TUMBLING = _PolicyConstant(
        kind="window_type",
        value="tumbling",
        name="WindowType.TUMBLING",
    )

    def __new__(cls, *args: object, **kwargs: object) -> "WindowType":
        raise TypeError("WindowType cannot be instantiated")


class LateDataPolicy:
    """Namespace for predefined late-data-policy constants."""

    REJECT = _PolicyConstant(
        kind="late_data_policy",
        value="reject",
        name="LateDataPolicy.REJECT",
    )
    DROP_WITH_METRIC = _PolicyConstant(
        kind="late_data_policy",
        value="drop_with_metric",
        name="LateDataPolicy.DROP_WITH_METRIC",
    )

    def __new__(cls, *args: object, **kwargs: object) -> "LateDataPolicy":
        raise TypeError("LateDataPolicy cannot be instantiated")


class OverflowPolicy:
    """Namespace for predefined overflow-policy constants."""

    BLOCK = _PolicyConstant(
        kind="overflow_policy",
        value="block",
        name="OverflowPolicy.BLOCK",
    )
    REJECT = _PolicyConstant(
        kind="overflow_policy",
        value="reject",
        name="OverflowPolicy.REJECT",
    )
    DROP_OLDEST = _PolicyConstant(
        kind="overflow_policy",
        value="drop_oldest",
        name="OverflowPolicy.DROP_OLDEST",
    )

    def __new__(cls, *args: object, **kwargs: object) -> "OverflowPolicy":
        raise TypeError("OverflowPolicy cannot be instantiated")


class SourceMode:
    """Namespace for predefined source-mode constants."""

    PIPELINE = _PolicyConstant(
        kind="source_mode",
        value="pipeline",
        name="SourceMode.PIPELINE",
    )
    CONSUMER = _PolicyConstant(
        kind="source_mode",
        value="consumer",
        name="SourceMode.CONSUMER",
    )

    def __new__(cls, *args: object, **kwargs: object) -> "SourceMode":
        raise TypeError("SourceMode cannot be instantiated")


class Duration:
    """Represent a positive time duration in nanoseconds for Python APIs."""

    __slots__ = ("total_nanoseconds",)

    def __init__(self, total_nanoseconds: int) -> None:
        """
        Create a duration value.

        :param total_nanoseconds: Duration size in nanoseconds.
        :type total_nanoseconds: int
        :raises ValueError: If ``total_nanoseconds`` is not positive.
        """
        total_nanoseconds = int(total_nanoseconds)
        if total_nanoseconds <= 0:
            raise ValueError("duration must be positive")
        self.total_nanoseconds = total_nanoseconds

    @classmethod
    def nanoseconds(cls, value: int) -> "Duration":
        """Create a duration from nanoseconds."""
        return cls(value)

    @classmethod
    def seconds(cls, value: int) -> "Duration":
        """Create a duration from seconds."""
        return cls(value * 1_000_000_000)

    @classmethod
    def minutes(cls, value: int) -> "Duration":
        """Create a duration from minutes."""
        return cls.seconds(value * 60)

    @classmethod
    def hours(cls, value: int) -> "Duration":
        """Create a duration from hours."""
        return cls.minutes(value * 60)

    def __int__(self) -> int:
        """Return the duration in nanoseconds."""
        return self.total_nanoseconds

    def __repr__(self) -> str:
        return f"Duration(total_nanoseconds={self.total_nanoseconds})"


def TS_EMA(*, column: str, span: int, output: str) -> TsEmaSpec:
    """Create a reactive EMA factor spec."""
    return TsEmaSpec(id_column="", value_column=column, span=span, output=output)


def TS_MEAN(*, column: str, window: int, output: str) -> TsMeanSpec:
    """Create a reactive rolling mean factor spec."""
    return TsMeanSpec(id_column="", value_column=column, window=window, output=output)


def TS_STD(*, column: str, window: int, output: str) -> TsStdSpec:
    """Create a reactive rolling standard deviation factor spec."""
    return TsStdSpec(id_column="", value_column=column, window=window, output=output)


def TS_DELAY(*, column: str, period: int, output: str) -> TsDelaySpec:
    """Create a reactive delay factor spec."""
    return TsDelaySpec(id_column="", value_column=column, period=period, output=output)


def TS_DIFF(*, column: str, period: int, output: str) -> TsDiffSpec:
    """Create a reactive difference factor spec."""
    return TsDiffSpec(id_column="", value_column=column, period=period, output=output)


def TS_RETURN(*, column: str, period: int, output: str) -> TsReturnSpec:
    """Create a reactive return factor spec."""
    return TsReturnSpec(id_column="", value_column=column, period=period, output=output)


def ABS(*, column: str, output: str) -> AbsSpec:
    """Create a reactive absolute-value factor spec."""
    return AbsSpec(id_column="", value_column=column, output=output)


def LOG(*, column: str, output: str) -> LogSpec:
    """Create a reactive natural-log factor spec."""
    return LogSpec(id_column="", value_column=column, output=output)


def CLIP(*, column: str, min: float, max: float, output: str) -> ClipSpec:
    """Create a reactive clip factor spec."""
    return ClipSpec(id_column="", value_column=column, min=min, max=max, output=output)


def CAST(*, column: str, dtype: str, output: str) -> CastSpec:
    """Create a reactive cast factor spec."""
    return CastSpec(id_column="", value_column=column, dtype=dtype, output=output)


def Expr(*, expression: str, output: str) -> ExpressionFactor:
    """Create a planner-backed expression factor spec."""
    return ExpressionFactor(expression=expression, output=output)


def AGG_FIRST(*, column: str, output: str) -> AggFirstSpec:
    """Create a first-value aggregation spec."""
    return AggFirstSpec(column=column, output=output)


def AGG_LAST(*, column: str, output: str) -> AggLastSpec:
    """Create a last-value aggregation spec."""
    return AggLastSpec(column=column, output=output)


def AGG_MAX(*, column: str, output: str) -> AggMaxSpec:
    """Create a max-value aggregation spec."""
    return AggMaxSpec(column=column, output=output)


def AGG_MIN(*, column: str, output: str) -> AggMinSpec:
    """Create a min-value aggregation spec."""
    return AggMinSpec(column=column, output=output)


def AGG_SUM(*, column: str, output: str) -> AggSumSpec:
    """Create a sum aggregation spec."""
    return AggSumSpec(column=column, output=output)


def AGG_COUNT(*, column: str, output: str) -> AggCountSpec:
    """Create a count aggregation spec."""
    return AggCountSpec(column=column, output=output)


def AGG_VWAP(*, price_column: str, volume_column: str, output: str) -> AggVwapSpec:
    """Create a VWAP aggregation spec."""
    return AggVwapSpec(
        price_column=price_column,
        volume_column=volume_column,
        output=output,
    )


def CS_RANK(*, column: str, output: str) -> CSRankSpec:
    """Create a cross-sectional rank factor spec."""
    return CSRankSpec(column=column, output=output)


def CS_ZSCORE(*, column: str, output: str) -> CSZscoreSpec:
    """Create a cross-sectional z-score factor spec."""
    return CSZscoreSpec(column=column, output=output)


def CS_DEMEAN(*, column: str, output: str) -> CSDemeanSpec:
    """Create a cross-sectional demean factor spec."""
    return CSDemeanSpec(column=column, output=output)


__all__ = [
    "AbsSpec",
    "AggCountSpec",
    "AggFirstSpec",
    "AggLastSpec",
    "AggMaxSpec",
    "AggMinSpec",
    "AggSumSpec",
    "AggVwapSpec",
    "CastSpec",
    "ClipSpec",
    "CSDemeanSpec",
    "CSRankSpec",
    "CSZscoreSpec",
    "CrossSectionalEngine",
    "Duration",
    "ExpressionFactor",
    "GatewayServer",
    "LateDataPolicy",
    "LogSpec",
    "MasterClient",
    "MasterServer",
    "run_master_daemon",
    "BusReader",
    "BusStreamSource",
    "BusStreamTarget",
    "BusWriter",
    "NullPublisher",
    "OverflowPolicy",
    "ParquetPersist",
    "ParquetReplayEngine",
    "ParquetReplaySource",
    "ParquetSink",
    "Pipeline",
    "ReactiveLatestEngine",
    "ReactiveStateEngine",
    "RemoteGatewayWriter",
    "RemoteMasterClient",
    "RemoteStreamSubscriber",
    "Row",
    "Session",
    "SourceMode",
    "StreamSubscriber",
    "Table",
    "TableReplayEngine",
        "TimeSeriesEngine",
    "TsDelaySpec",
    "TsDiffSpec",
    "TsEmaSpec",
    "TsMeanSpec",
    "TsReturnSpec",
    "TsStdSpec",
    "WindowType",
    "ABS",
    "AGG_COUNT",
    "AGG_FIRST",
    "AGG_LAST",
    "AGG_MAX",
    "AGG_MIN",
    "AGG_SUM",
    "AGG_VWAP",
    "CAST",
    "CLIP",
    "CS_DEMEAN",
    "CS_RANK",
    "CS_ZSCORE",
    "Expr",
    "LOG",
    "TS_DELAY",
    "TS_DIFF",
    "TS_EMA",
    "TS_MEAN",
    "TS_RETURN",
    "TS_STD",
    "ZmqPublisher",
    "ZmqSource",
    "ZmqStreamPublisher",
    "ZmqSubscriber",
    "__version__",
    "col",
    "compare_replay",
    "config",
    "connect",
    "get_writer",
    "lit",
    "log_info",
    "master",
    "ops",
    "read_table",
    "read_from",
    "replay",
    "subscribe",
    "subscribe_table",
    "version",
]
