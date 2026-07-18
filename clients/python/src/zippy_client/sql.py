"""Compile the public lazy Table AST into server-owned DataFusion SQL."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import math

from .expressions import Expr, _TemporalInstant, normalize_temporal_filter


@dataclass(frozen=True)
class FlightSqlPlan:
    """One SQL statement and its explicit Catalog table bindings."""

    sql: str
    bindings: list[dict[str, object]]


@dataclass
class _Relation:
    sql: str
    columns: list[str]
    ordering: list[tuple[str, bool]]


class FlightSqlCompiler:
    """Compile one or more joined :class:`Table` objects into exact server SQL."""

    def __init__(self) -> None:
        self.bindings: list[dict[str, object]] = []
        self._source_aliases: dict[tuple[str, str], str] = {}
        self._active_tables: set[int] = set()

    def compile(self, table: object) -> FlightSqlPlan:
        """Compile a lazy table without leaving any client-side residual operators."""
        relation = self._compile_table(table)
        sql = relation.sql
        if relation.ordering:
            sql = self._ordered_sql(sql, relation.ordering)
        return FlightSqlPlan(sql=sql, bindings=list(self.bindings))

    def _compile_table(self, table: object) -> _Relation:
        identity = id(table)
        if identity in self._active_tables:
            raise ValueError("Flight query plan contains a cyclic table join")
        source = str(getattr(table, "source"))
        namespace = str(getattr(table, "namespace", "default"))
        schema = table.schema()  # type: ignore[attr-defined]
        columns = [str(name) for name in getattr(schema, "names", [])]
        if not columns:
            raise ValueError(f"Flight query source has no columns source=[{source}]")
        self._active_tables.add(identity)
        try:
            alias = self._bind_source(source, namespace)
            relation = _Relation(
                sql=f"SELECT * FROM {quote_identifier(alias)}",
                columns=columns,
                ordering=[],
            )
            for kind, value in list(getattr(table, "_plan", [])):
                if kind == "filter":
                    value = normalize_temporal_filter(value, schema)
                relation = self._apply(relation, str(kind), value)
            return relation
        finally:
            self._active_tables.remove(identity)

    def _bind_source(self, source: str, namespace: str) -> str:
        key = (namespace, source)
        alias = self._source_aliases.get(key)
        if alias is not None:
            return alias
        alias = f"zippy_source_{len(self.bindings)}"
        self._source_aliases[key] = alias
        self.bindings.append(
            {
                "sql_name": alias,
                "table": {"namespace": namespace, "table": source},
            }
        )
        return alias

    def _apply(self, relation: _Relation, kind: str, value: object) -> _Relation:
        if kind == "filter":
            predicate = expression_to_sql(value)
            relation.sql = f"SELECT * FROM ({relation.sql}) AS _zippy_q WHERE {predicate}"
            return relation
        if kind == "select":
            expressions = _as_list(value, "select")
            names = [expression_output_name(expression) for expression in expressions]
            if any(name is None for name in names):
                raise ValueError("Flight computed select expressions require explicit aliases")
            if len(set(names)) != len(names):
                raise ValueError("Flight select output names must be unique")
            relation.sql = (
                "SELECT "
                + ", ".join(
                    expression_to_sql(expression, include_alias=True) for expression in expressions
                )
                + f" FROM ({relation.sql}) AS _zippy_q"
            )
            relation.columns = [str(name) for name in names]
            relation.ordering = [item for item in relation.ordering if item[0] in relation.columns]
            return relation
        if kind == "with_columns":
            expressions = _as_list(value, "with_columns")
            names = [expression_output_name(expression) for expression in expressions]
            if any(name is None for name in names):
                raise ValueError("Flight with_columns expressions require explicit aliases")
            replacement_names = [str(name) for name in names]
            if len(set(replacement_names)) != len(replacement_names):
                raise ValueError("Flight with_columns output names must be unique")
            replacements = dict(zip(replacement_names, expressions))
            items = [
                (
                    expression_to_sql(replacements[name], include_alias=True)
                    if name in replacements
                    else quote_identifier(name)
                )
                for name in relation.columns
            ]
            appended = [name for name in replacement_names if name not in relation.columns]
            items.extend(
                expression_to_sql(replacements[name], include_alias=True) for name in appended
            )
            relation.sql = f"SELECT {', '.join(items)} FROM ({relation.sql}) AS _zippy_q"
            relation.columns = [*relation.columns, *appended]
            relation.ordering = [
                item for item in relation.ordering if item[0] not in replacement_names
            ]
            return relation
        if kind == "drop":
            dropped = {str(name) for name in _as_list(value, "drop")}
            missing = dropped - set(relation.columns)
            if missing:
                raise ValueError(f"Flight drop references missing columns={sorted(missing)}")
            relation.columns = [name for name in relation.columns if name not in dropped]
            if not relation.columns:
                raise ValueError("Flight drop cannot remove every result column")
            relation.sql = (
                "SELECT "
                + ", ".join(quote_identifier(name) for name in relation.columns)
                + f" FROM ({relation.sql}) AS _zippy_q"
            )
            relation.ordering = [item for item in relation.ordering if item[0] not in dropped]
            return relation
        if kind == "rename":
            if not isinstance(value, dict):
                raise TypeError("Flight rename plan must be a mapping")
            mapping = {str(source): str(target) for source, target in value.items()}
            missing = set(mapping) - set(relation.columns)
            if missing:
                raise ValueError(f"Flight rename references missing columns={sorted(missing)}")
            renamed = [mapping.get(name, name) for name in relation.columns]
            if len(set(renamed)) != len(renamed):
                raise ValueError("Flight rename produces duplicate columns")
            relation.sql = (
                "SELECT "
                + ", ".join(
                    (
                        quote_identifier(source)
                        if source == target
                        else f"{quote_identifier(source)} AS {quote_identifier(target)}"
                    )
                    for source, target in zip(relation.columns, renamed)
                )
                + f" FROM ({relation.sql}) AS _zippy_q"
            )
            relation.columns = renamed
            relation.ordering = [
                (mapping.get(name, name), descending) for name, descending in relation.ordering
            ]
            return relation
        if kind == "sort":
            if not isinstance(value, dict):
                raise TypeError("Flight sort plan must be a mapping")
            keys = [_simple_sort_column(item) for item in _as_list(value.get("by"), "sort.by")]
            if not keys:
                raise ValueError("Flight sort requires at least one simple column key")
            missing = set(keys) - set(relation.columns)
            if missing:
                raise ValueError(f"Flight sort references missing columns={sorted(missing)}")
            descending = value.get("descending", False)
            if isinstance(descending, (list, tuple)):
                directions = [bool(item) for item in descending]
                if len(directions) != len(keys):
                    raise ValueError("Flight sort directions must match sort keys")
            else:
                directions = [bool(descending)] * len(keys)
            relation.ordering = list(zip(keys, directions))
            relation.sql = self._ordered_sql(relation.sql, relation.ordering)
            return relation
        if kind == "head":
            count = _non_negative_count("head", value)
            relation.sql = f"SELECT * FROM ({relation.sql}) AS _zippy_q LIMIT {count}"
            return relation
        if kind == "slice":
            if not isinstance(value, dict):
                raise TypeError("Flight slice plan must be a mapping")
            offset = _non_negative_count("slice offset", value.get("offset", 0))
            length = value.get("length")
            if length is None:
                relation.sql = f"SELECT * FROM ({relation.sql}) AS _zippy_q OFFSET {offset}"
            else:
                count = _non_negative_count("slice length", length)
                relation.sql = (
                    f"SELECT * FROM ({relation.sql}) AS _zippy_q LIMIT {count} OFFSET {offset}"
                )
            return relation
        if kind == "tail":
            count = _non_negative_count("tail", value)
            if not relation.ordering:
                raise ValueError("Flight tail requires an explicit preceding sort")
            reversed_order = [(name, not descending) for name, descending in relation.ordering]
            limited = self._ordered_sql(relation.sql, reversed_order)
            relation.sql = f"SELECT * FROM ({limited}) AS _zippy_tail LIMIT {count}"
            relation.sql = self._ordered_sql(relation.sql, relation.ordering)
            return relation
        if kind == "join":
            if not isinstance(value, dict):
                raise TypeError("Flight join plan must be a mapping")
            return self._join(relation, value)
        raise ValueError(f"unsupported Flight table plan operation=[{kind}]")

    def _join(self, left: _Relation, value: dict[str, object]) -> _Relation:
        other = value.get("other")
        if other is None or not hasattr(other, "schema"):
            raise TypeError("Flight join requires another zippy_client.Table")
        right = self._compile_table(other)
        keys = [str(key) for key in _as_list(value.get("on"), "join.on")]
        if not keys:
            raise ValueError("Flight join requires at least one key")
        missing_left = set(keys) - set(left.columns)
        missing_right = set(keys) - set(right.columns)
        if missing_left or missing_right:
            raise ValueError(
                "Flight join key is missing "
                f"left={sorted(missing_left)} right={sorted(missing_right)}"
            )
        join_mode = {
            "inner": "INNER JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "full": "FULL OUTER JOIN",
            "outer": "FULL OUTER JOIN",
        }.get(str(value.get("how", "inner")).lower())
        if join_mode is None:
            raise ValueError(f"unsupported Flight join mode=[{value.get('how')}]")
        suffix = str(value.get("suffix", "_right"))
        output_columns = list(left.columns)
        preserve_right_keys = join_mode in {"RIGHT JOIN", "FULL OUTER JOIN"}
        items = []
        for name in left.columns:
            if preserve_right_keys and name in keys:
                expression = (
                    f"COALESCE(_zippy_left.{quote_identifier(name)}, "
                    f"_zippy_right.{quote_identifier(name)})"
                )
            else:
                expression = f"_zippy_left.{quote_identifier(name)}"
            items.append(f"{expression} AS {quote_identifier(name)}")
        for name in right.columns:
            if name in keys:
                continue
            output_name = f"{name}{suffix}" if name in output_columns else name
            if output_name in output_columns:
                raise ValueError(f"Flight join produces duplicate column=[{output_name}]")
            output_columns.append(output_name)
            items.append(
                f"_zippy_right.{quote_identifier(name)} AS {quote_identifier(output_name)}"
            )
        predicate = " AND ".join(
            f"_zippy_left.{quote_identifier(key)} = _zippy_right.{quote_identifier(key)}"
            for key in keys
        )
        sql = (
            f"SELECT {', '.join(items)} FROM ({left.sql}) AS _zippy_left "
            f"{join_mode} ({right.sql}) AS _zippy_right ON {predicate}"
        )
        return _Relation(sql=sql, columns=output_columns, ordering=[])

    @staticmethod
    def _ordered_sql(sql: str, ordering: list[tuple[str, bool]]) -> str:
        order = ", ".join(
            f"{quote_identifier(name)} {'DESC' if descending else 'ASC'}"
            for name, descending in ordering
        )
        return f"SELECT * FROM ({sql}) AS _zippy_ordered ORDER BY {order}"


def compile_flight_table_sql(table: object) -> FlightSqlPlan:
    """Compile one lazy table into a complete Flight SQL plan."""
    return FlightSqlCompiler().compile(table)


def quote_identifier(value: object) -> str:
    """Quote one DataFusion SQL identifier deterministically."""
    text = str(value)
    if not text or "\x00" in text:
        raise ValueError("SQL identifiers must be non-empty and contain no NUL")
    return f'"{text.replace(chr(34), chr(34) * 2)}"'


def expression_to_sql(expression: object, *, include_alias: bool = False) -> str:
    """Render one typed client expression as exact DataFusion SQL."""
    if isinstance(expression, str):
        return quote_identifier(expression)
    if not isinstance(expression, Expr):
        return _literal_to_sql(expression)
    if expression.kind == "col":
        return quote_identifier(expression.value)
    if expression.kind == "literal":
        return _literal_to_sql(expression.value)
    if expression.kind == "alias":
        value = expression_to_sql(expression.args[0])
        return f"{value} AS {quote_identifier(expression.value)}" if include_alias else value
    if expression.kind == "is_in":
        values = _membership_values(expression.args[1])
        if not values:
            return "FALSE"
        return (
            f"({expression_to_sql(expression.args[0])} IN "
            f"({', '.join(_literal_to_sql(value) for value in values)}))"
        )
    if expression.kind == "is_null":
        return f"({expression_to_sql(expression.args[0])} IS NULL)"
    if expression.kind == "is_not_null":
        return f"({expression_to_sql(expression.args[0])} IS NOT NULL)"
    if expression.kind == "is_between":
        return (
            f"({expression_to_sql(expression.args[0])} BETWEEN "
            f"{expression_to_sql(expression.args[1])} AND "
            f"{expression_to_sql(expression.args[2])})"
        )
    if expression.kind == "unary":
        value = expression_to_sql(expression.args[0])
        if expression.value == "not":
            return f"(NOT {value})"
        if expression.value == "neg":
            return f"(-{value})"
        raise ValueError(f"unsupported Flight unary operator=[{expression.value}]")
    if expression.kind != "binary":
        raise ValueError(f"unsupported Flight expression kind=[{expression.kind}]")
    left, right = expression.args
    if expression.value in {"eq", "ne"} and (_is_null_literal(left) or _is_null_literal(right)):
        target = right if _is_null_literal(left) else left
        operator = "IS NULL" if expression.value == "eq" else "IS NOT NULL"
        return f"({expression_to_sql(target)} {operator})"
    binary_operator = {
        "eq": "=",
        "ne": "<>",
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
        "and": "AND",
        "or": "OR",
        "add": "+",
        "sub": "-",
        "mul": "*",
        "div": "/",
    }.get(str(expression.value))
    if binary_operator is None:
        raise ValueError(f"unsupported Flight binary operator=[{expression.value}]")
    return f"({expression_to_sql(left)} {binary_operator} {expression_to_sql(right)})"


def expression_output_name(expression: object) -> str | None:
    """Return the output column name for one projection expression."""
    if isinstance(expression, str):
        return expression
    if not isinstance(expression, Expr):
        return None
    if expression.kind in {"col", "alias"}:
        return str(expression.value)
    return None


def _literal_to_sql(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, _TemporalInstant):
        return f"to_timestamp_nanos({value.epoch_ns})"
    if isinstance(value, datetime):
        return f"to_timestamp_nanos({_datetime_to_epoch_ns(value)})"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Flight SQL decimal literals must be finite")
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Flight SQL floating-point literals must be finite")
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"unsupported Flight SQL literal type=[{type(value).__name__}]")


def _datetime_to_epoch_ns(value: datetime) -> int:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    utc = normalized.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc - epoch
    return (
        delta.days * 86_400_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1_000
    )


def _membership_values(value: object) -> list[object]:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=lambda item: (type(item).__name__, repr(item)))
    raise TypeError("Flight is_in values must be a list, tuple, set, or frozenset")


def _is_null_literal(expression: object) -> bool:
    return expression is None or (
        isinstance(expression, Expr) and expression.kind == "literal" and expression.value is None
    )


def _simple_sort_column(expression: object) -> str:
    if isinstance(expression, str):
        return expression
    if isinstance(expression, Expr) and expression.kind == "col":
        return str(expression.value)
    raise ValueError("Flight sort requires simple column names")


def _as_list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"Flight query plan value must be a list operation=[{name}]")
    return value


def _non_negative_count(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise TypeError(f"Flight {name} must be an integer")
    count = int(value)
    if count < 0:
        raise ValueError(f"Flight {name} must be non-negative")
    return count
