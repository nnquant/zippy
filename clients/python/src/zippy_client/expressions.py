"""Shared query expressions, precise temporal ranges and transport serialization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, cast
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class _TemporalInstant:
    epoch_ns: int
    timezone_name: str | None
    unit: str


@dataclass(frozen=True)
class _TemporalRange:
    start_ns: int
    end_ns: int
    timezone_name: str | None
    unit: str


class Expr:
    """
    Immutable Zippy query expression.

    User code normally creates expressions with :func:`col` and :func:`lit`.
    """

    __slots__ = ("kind", "value", "args")

    def __init__(
        self,
        kind: str,
        value: object = None,
        args: tuple[object, ...] = (),
    ) -> None:
        self.kind = kind
        self.value = value
        self.args = args

    def alias(self, name: str) -> Expr:
        """Return this expression with an output alias."""
        return Expr("alias", str(name), (self,))

    def is_in(self, values: object) -> Expr:
        """Return a membership predicate."""
        return Expr("is_in", None, (self, values))

    def is_null(self) -> Expr:
        """Return a null predicate."""
        return Expr("is_null", None, (self,))

    def is_not_null(self) -> Expr:
        """Return a non-null predicate."""
        return Expr("is_not_null", None, (self,))

    def is_between(self, lower_bound: object, upper_bound: object) -> Expr:
        """Return an inclusive range predicate."""
        return Expr("is_between", None, (self, lit(lower_bound), lit(upper_bound)))

    def __eq__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("eq", other)

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("ne", other)

    def __gt__(self, other: object) -> Expr:
        return self._binary("gt", other)

    def __ge__(self, other: object) -> Expr:
        return self._binary("ge", other)

    def __lt__(self, other: object) -> Expr:
        return self._binary("lt", other)

    def __le__(self, other: object) -> Expr:
        return self._binary("le", other)

    def __and__(self, other: object) -> Expr:
        return self._binary("and", other)

    def __or__(self, other: object) -> Expr:
        return self._binary("or", other)

    def __add__(self, other: object) -> Expr:
        return self._binary("add", other)

    def __radd__(self, other: object) -> Expr:
        return lit(other)._binary("add", self)

    def __sub__(self, other: object) -> Expr:
        return self._binary("sub", other)

    def __rsub__(self, other: object) -> Expr:
        return lit(other)._binary("sub", self)

    def __mul__(self, other: object) -> Expr:
        return self._binary("mul", other)

    def __rmul__(self, other: object) -> Expr:
        return lit(other)._binary("mul", self)

    def __truediv__(self, other: object) -> Expr:
        return self._binary("div", other)

    def __rtruediv__(self, other: object) -> Expr:
        return lit(other)._binary("div", self)

    def __invert__(self) -> Expr:
        return Expr("unary", "not", (self,))

    def __neg__(self) -> Expr:
        return Expr("unary", "neg", (self,))

    def __bool__(self) -> bool:
        raise TypeError("query expressions cannot be evaluated as booleans; use '&' or '|'")

    def _binary(self, op: str, other: object) -> Expr:
        return Expr("binary", op, (self, lit(other)))


def col(name: str) -> Expr:
    """
    Create a column expression.

    :param name: Column name.
    :type name: str
    :returns: Column expression.
    :rtype: Expr
    """
    return Expr("col", str(name))


def lit(value: object) -> Expr:
    """
    Create a literal expression.

    :param value: Literal value.
    :type value: object
    :returns: Literal expression.
    :rtype: Expr
    """
    if isinstance(value, Expr):
        return value
    return Expr("literal", value)


def normalize_expressions(
    expressions: tuple[object, ...],
    named_expressions: dict[str, object],
) -> list[object]:
    """Normalize positional and named expression arguments."""
    normalized: list[object] = []
    if len(expressions) == 1 and isinstance(expressions[0], (list, tuple)):
        normalized.extend(expressions[0])
    else:
        normalized.extend(expressions)
    for name, expression in named_expressions.items():
        normalized.append(lit(expression).alias(name))
    return normalized


def expression_to_json(expression: object) -> dict[str, Any]:
    """Serialize one expression to the shared JSON expression representation."""
    if isinstance(expression, str):
        return {"kind": "col", "value": expression}
    if not isinstance(expression, Expr):
        return {"kind": "literal", "value": expression}
    if expression.kind == "col":
        return {"kind": "col", "value": expression.value}
    if expression.kind == "literal":
        if isinstance(expression.value, _TemporalInstant):
            return {
                "kind": "literal",
                "literal_type": "timestamp_ns",
                "value": expression.value.epoch_ns,
                "timezone": expression.value.timezone_name,
                "unit": expression.value.unit,
            }
        return {"kind": "literal", "value": expression.value}
    if expression.kind == "alias":
        return {
            "kind": "alias",
            "value": expression.value,
            "arg": expression_to_json(expression.args[0]),
        }
    if expression.kind == "is_in":
        values = _membership_values(expression.args[1])
        return {
            "kind": "is_in",
            "args": [
                expression_to_json(expression.args[0]),
                expression_to_json(lit(values)),
            ],
        }
    if expression.kind in {"is_null", "is_not_null"}:
        return {
            "kind": expression.kind,
            "arg": expression_to_json(expression.args[0]),
        }
    if expression.kind == "is_between":
        return {
            "kind": "is_between",
            "args": [expression_to_json(item) for item in expression.args],
        }
    if expression.kind == "unary":
        return {
            "kind": "unary",
            "op": expression.value,
            "arg": expression_to_json(expression.args[0]),
        }
    if expression.kind == "binary":
        return {
            "kind": "binary",
            "op": expression.value,
            "args": [expression_to_json(item) for item in expression.args],
        }
    raise ValueError(f"unsupported query expression kind=[{expression.kind}]")


def normalize_temporal_filter(expression: object, schema: object | None) -> object:
    """Normalize timestamp/date comparisons to inclusive-start, exclusive-end ranges.

    :param expression: Shared expression AST; non-temporal nodes are unchanged.
    :param schema: Source Arrow schema, supplying the timestamp unit and timezone.
    :returns: One normalized expression shared by both Python packages.
    :raises ValueError: A temporal literal has an invalid date, precision or offset.
    :example: normalize_temporal_filter(col("dt") == "2026-09", schema)
    """
    if schema is None or not isinstance(expression, Expr) or expression.kind != "binary":
        return expression
    op = str(expression.value)
    left, right = expression.args
    if op in {"and", "or"}:
        return Expr(
            "binary",
            op,
            (
                normalize_temporal_filter(left, schema),
                normalize_temporal_filter(right, schema),
            ),
        )
    if op not in {"eq", "ne", "gt", "ge", "lt", "le"}:
        return expression
    pair = _column_literal_pair(left, right)
    reverse = False
    if pair is None:
        pair = _column_literal_pair(right, left)
        reverse = pair is not None
    if pair is None:
        return expression
    column, value = pair
    field_info = _temporal_field_info(schema, column)
    if field_info is None:
        return expression
    temporal_range = _temporal_range(value, field_info)
    if temporal_range is None:
        return expression
    if reverse:
        op = {"eq": "eq", "ne": "ne", "gt": "lt", "ge": "le", "lt": "gt", "le": "ge"}[op]
    column_expression = col(column)
    start = _instant(temporal_range.start_ns, temporal_range)
    end = _instant(temporal_range.end_ns, temporal_range)
    if op == "eq":
        return (column_expression >= start) & (column_expression < end)
    if op == "ne":
        return (column_expression < start) | (column_expression >= end)
    if op == "ge":
        return column_expression >= start
    if op == "gt":
        return column_expression >= end
    if op == "lt":
        return column_expression < start
    return column_expression < end  # le


def _column_literal_pair(left: object, right: object) -> tuple[str, object] | None:
    if not isinstance(left, Expr) or left.kind != "col":
        return None
    if not isinstance(right, Expr) or right.kind != "literal":
        return None
    return str(left.value), right.value


def _temporal_field_info(schema: object, column: str) -> dict[str, object] | None:
    import pyarrow as pa

    if column not in getattr(schema, "names", []):
        return None
    field = cast(Any, schema).field(column)
    if pa.types.is_timestamp(field.type):
        return {
            "kind": "timestamp",
            "unit": str(field.type.unit),
            "timezone": field.type.tz,
        }
    if pa.types.is_date32(field.type) or pa.types.is_date64(field.type):
        return {"kind": "date", "unit": "ns", "timezone": None}
    return None


def _temporal_range(value: object, field_info: dict[str, object]) -> _TemporalRange | None:
    timezone_name = str(field_info.get("timezone") or "") or None
    unit = str(field_info.get("unit") or "ns")
    target_timezone = _timezone(timezone_name)
    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=target_timezone or timezone.utc)
        elif target_timezone is not None:
            normalized = normalized.astimezone(target_timezone)
        start_ns = _datetime_to_epoch_ns(normalized)
        # Timestamp datetime subclasses can carry a nanosecond remainder. Avoid
        # their unit-dependent integer .value and retain schema-local naive times.
        nanosecond = getattr(normalized, "nanosecond", None)
        if nanosecond is not None:
            if type(nanosecond) is not int or not 0 <= nanosecond < 1_000:
                raise ValueError("invalid datetime nanosecond remainder")
            start_ns += nanosecond
            return _TemporalRange(start_ns, start_ns + 1, timezone_name, unit)
        return _TemporalRange(start_ns, start_ns + 1_000, timezone_name, unit)
    if isinstance(value, date):
        start = datetime(
            value.year,
            value.month,
            value.day,
            tzinfo=target_timezone or timezone.utc,
        )
        return _TemporalRange(
            _datetime_to_epoch_ns(start),
            _datetime_to_epoch_ns(start + timedelta(days=1)),
            timezone_name,
            unit,
        )
    if isinstance(value, str):
        return _parse_temporal_string(value, timezone_name, unit)
    return None


_TEMPORAL_RE = re.compile(
    r"(?P<year>\d{4})(?P<separator>-?)(?P<month>\d{2})"
    r"(?:(?P=separator)(?P<day>\d{2})"
    r"(?:(?:[ T])(?P<hour>\d{2}):(?P<minute>\d{2})"
    r"(?::(?P<second>\d{2})(?:\.(?P<fraction>\d{1,9}))?)?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?)?)?"
)


def _parse_temporal_string(
    value: str,
    timezone_name: str | None,
    unit: str,
) -> _TemporalRange:
    match = _TEMPORAL_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"invalid temporal literal value=[{value}]")
    year = int(match.group("year"))
    month = int(match.group("month"))
    target_timezone = _timezone(timezone_name) or timezone.utc
    if match.group("day") is None:
        start = datetime(year, month, 1, tzinfo=target_timezone)
        end = (
            datetime(year + 1, 1, 1, tzinfo=target_timezone)
            if month == 12
            else datetime(year, month + 1, 1, tzinfo=target_timezone)
        )
        return _TemporalRange(
            _datetime_to_epoch_ns(start),
            _datetime_to_epoch_ns(end),
            timezone_name,
            unit,
        )
    day = int(match.group("day"))
    hour_text = match.group("hour")
    if hour_text is None:
        start = datetime(year, month, day, tzinfo=target_timezone)
        return _TemporalRange(
            _datetime_to_epoch_ns(start),
            _datetime_to_epoch_ns(start + timedelta(days=1)),
            timezone_name,
            unit,
        )
    tz_text = match.group("tz")
    literal_timezone = target_timezone
    if tz_text == "Z":
        literal_timezone = timezone.utc
    elif tz_text:
        compact = tz_text.replace(":", "")
        sign = 1 if compact[0] == "+" else -1
        offset_hour, offset_minute = int(compact[1:3]), int(compact[3:5])
        if offset_hour > 23 or offset_minute > 59:
            raise ValueError(f"invalid temporal literal offset value=[{value}]")
        literal_timezone = timezone(sign * timedelta(hours=offset_hour, minutes=offset_minute))
    minute = int(match.group("minute"))
    second_text = match.group("second")
    if second_text is None:
        start = datetime(year, month, day, int(hour_text), minute, tzinfo=literal_timezone)
        end = start + timedelta(minutes=1)
    else:
        second = int(second_text)
        fraction = match.group("fraction") or ""
        padded = (fraction + "0" * 9)[:9]
        start = datetime(
            year,
            month,
            day,
            int(hour_text),
            minute,
            second,
            int(padded[:6]),
            tzinfo=literal_timezone,
        )
        start_ns = _datetime_to_epoch_ns(start) + int(padded[6:] or "0")
        increment_ns = 10 ** (9 - len(fraction)) if fraction else 1_000_000_000
        return _TemporalRange(start_ns, start_ns + increment_ns, timezone_name, unit)
    return _TemporalRange(
        _datetime_to_epoch_ns(start),
        _datetime_to_epoch_ns(end),
        timezone_name,
        unit,
    )


def _timezone(name: str | None):
    if name is None:
        return None
    if name.upper() == "UTC":
        return timezone.utc
    return ZoneInfo(name)


def _datetime_to_epoch_ns(value: datetime) -> int:
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    utc_value = aware.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc_value - epoch
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000_000) + delta.microseconds * 1_000


def _instant(epoch_ns: int, temporal_range: _TemporalRange) -> Expr:
    return lit(
        _TemporalInstant(
            epoch_ns=epoch_ns,
            timezone_name=temporal_range.timezone_name,
            unit=temporal_range.unit,
        )
    )


def _membership_values(value: object) -> list[object]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        raise TypeError("is_in values must be a list, tuple, set, or frozenset")
    return list(value)
