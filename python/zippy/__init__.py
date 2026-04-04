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
from ._internal import NullPublisher
from ._internal import ParquetSink
from ._internal import ReactiveStateEngine
from ._internal import TimeSeriesEngine
from ._internal import TsDelaySpec
from ._internal import TsDiffSpec
from ._internal import TsEmaSpec
from ._internal import TsMeanSpec
from ._internal import TsReturnSpec
from ._internal import TsStdSpec
from ._internal import ZmqPublisher
from ._internal import ZmqSubscriber
from ._internal import __version__
from ._internal import version


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


def EXPR(*, expression: str, output: str) -> ExpressionFactor:
    """Create a reactive expression factor spec."""
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
    "LateDataPolicy",
    "LogSpec",
    "NullPublisher",
    "OverflowPolicy",
    "ParquetSink",
    "ReactiveStateEngine",
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
    "EXPR",
    "LOG",
    "TS_DELAY",
    "TS_DIFF",
    "TS_EMA",
    "TS_MEAN",
    "TS_RETURN",
    "TS_STD",
    "ZmqPublisher",
    "ZmqSubscriber",
    "__version__",
    "version",
]
