"""Typed local factor specifications and explicit nanosecond/window policies.

These values configure native kernels without owning connections or data directories.
"""

from __future__ import annotations

from dataclasses import dataclass

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
from ._internal import ExpressionFactor
from ._internal import LogSpec
from ._internal import TsDelaySpec
from ._internal import TsDiffSpec
from ._internal import TsEmaSpec
from ._internal import TsMeanSpec
from ._internal import TsReturnSpec
from ._internal import TsStdSpec


@dataclass(frozen=True)
class _BarInputColumns:
    instrument: str
    dt: str
    price: str
    volume: str
    total_turnover: str
    trading_day: str | None = None
    num_trades: str | None = None
    limit_up: str | None = None
    limit_down: str | None = None

    def to_bar_generator_spec(self) -> dict[str, str | None]:
        return {
            "instrument": self.instrument,
            "dt": self.dt,
            "price": self.price,
            "volume": self.volume,
            "total_turnover": self.total_turnover,
            "trading_day": self.trading_day,
            "num_trades": self.num_trades,
            "limit_up": self.limit_up,
            "limit_down": self.limit_down,
        }


@dataclass(frozen=True)
class _BarTradingSessions:
    timezone: str
    regular: list[tuple[str, str]]
    auction: list[tuple[str, str]]

    def to_bar_generator_spec(self) -> dict[str, object]:
        return {
            "timezone": self.timezone,
            "regular": list(self.regular),
            "auction": list(self.auction),
        }


@dataclass(frozen=True)
class _BarVolume:
    mode: str
    trading_day_column: str | None = None
    bootstrap: str | None = None
    output_dtype: str = "int64"

    @staticmethod
    def delta(output_dtype: str = "int64") -> "_BarVolume":
        return _BarVolume(mode="delta", output_dtype=output_dtype)

    @staticmethod
    def cumulative(
        trading_day_column: str,
        bootstrap: str = "skip_first_delta",
        output_dtype: str = "int64",
    ) -> "_BarVolume":
        return _BarVolume(
            mode="cumulative",
            trading_day_column=trading_day_column,
            bootstrap=bootstrap,
            output_dtype=output_dtype,
        )

    def to_bar_generator_spec(self) -> dict[str, object]:
        if self.mode == "delta":
            return {"mode": "delta", "output_dtype": self.output_dtype}
        return {
            "mode": self.mode,
            "trading_day_column": self.trading_day_column,
            "bootstrap": self.bootstrap,
            "output_dtype": self.output_dtype,
        }


@dataclass(frozen=True)
class _BarAuction:
    mode: str

    @staticmethod
    def drop() -> "_BarAuction":
        return _BarAuction("drop")

    @staticmethod
    def merge_to_first_regular_bar() -> "_BarAuction":
        return _BarAuction("merge_to_first_regular_bar")

    @staticmethod
    def emit_separate_bar() -> "_BarAuction":
        return _BarAuction("emit_separate_bar")

    def to_bar_generator_spec(self) -> str:
        return self.mode


@dataclass(frozen=True)
class _BarGeneratorProfile:
    frequency: str
    columns: _BarInputColumns
    sessions: _BarTradingSessions
    volume: _BarVolume
    auction: _BarAuction
    dt_label: str = "close_dt"

    def to_bar_generator_spec(self) -> dict[str, object]:
        return {
            "frequency": self.frequency,
            "columns": self.columns.to_bar_generator_spec(),
            "sessions": self.sessions.to_bar_generator_spec(),
            "volume": self.volume.to_bar_generator_spec(),
            "auction": self.auction.to_bar_generator_spec(),
            "dt_label": self.dt_label,
        }


class _BarNamespace:
    InputColumns = _BarInputColumns
    TradingSessions = _BarTradingSessions
    Volume = _BarVolume
    Auction = _BarAuction
    BarGeneratorProfile = _BarGeneratorProfile


bar = _BarNamespace()


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
