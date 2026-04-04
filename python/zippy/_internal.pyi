import polars as pl
import pyarrow as pa

__version__: str


class _WindowTypeValue: ...


class WindowType:
    TUMBLING: _WindowTypeValue


class _LateDataPolicyValue: ...


class LateDataPolicy:
    REJECT: _LateDataPolicyValue
    DROP_WITH_METRIC: _LateDataPolicyValue


class _OverflowPolicyValue: ...


class OverflowPolicy:
    BLOCK: _OverflowPolicyValue
    REJECT: _OverflowPolicyValue
    DROP_OLDEST: _OverflowPolicyValue


class Duration:
    total_nanoseconds: int

    def __init__(self, total_nanoseconds: int) -> None: ...

    @classmethod
    def nanoseconds(cls, value: int) -> Duration: ...

    @classmethod
    def seconds(cls, value: int) -> Duration: ...

    @classmethod
    def minutes(cls, value: int) -> Duration: ...

    @classmethod
    def hours(cls, value: int) -> Duration: ...

    def __int__(self) -> int: ...


class TsEmaSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        span: int,
        output: str,
    ) -> None: ...


class TsReturnSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        period: int,
        output: str,
    ) -> None: ...


class TsMeanSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        window: int,
        output: str,
    ) -> None: ...


class TsStdSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        window: int,
        output: str,
    ) -> None: ...


class TsDelaySpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        period: int,
        output: str,
    ) -> None: ...


class TsDiffSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        period: int,
        output: str,
    ) -> None: ...


class AbsSpec:
    def __init__(self, id_column: str, value_column: str, output: str) -> None: ...


class LogSpec:
    def __init__(self, id_column: str, value_column: str, output: str) -> None: ...


class ClipSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        min: float,
        max: float,
        output: str,
    ) -> None: ...


class CastSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        dtype: str,
        output: str,
    ) -> None: ...


class ExpressionFactor:
    def __init__(self, expression: str, output: str) -> None: ...


class AggFirstSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggLastSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggSumSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggMaxSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggMinSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggCountSpec:
    def __init__(self, column: str, output: str) -> None: ...


class AggVwapSpec:
    def __init__(self, price_column: str, volume_column: str, output: str) -> None: ...


class NullPublisher:
    def __init__(self) -> None: ...


class ParquetSink:
    def __init__(
        self,
        path: str,
        rotation: str = "none",
        write_input: bool = False,
        write_output: bool = True,
    ) -> None: ...


class ZmqPublisher:
    def __init__(self, endpoint: str) -> None: ...


class ZmqSubscriber:
    def __init__(self, endpoint: str, timeout_ms: int = 1000) -> None: ...

    def recv(self) -> pa.RecordBatch: ...

    def close(self) -> None: ...


WriteValue = (
    pl.DataFrame
    | pa.RecordBatch
    | pa.Table
    | dict[str, object]
    | list[dict[str, object]]
)
PublisherTarget = NullPublisher | ZmqPublisher | list[NullPublisher | ZmqPublisher]
ReactiveFactor = (
    TsEmaSpec
    | TsReturnSpec
    | TsMeanSpec
    | TsStdSpec
    | TsDelaySpec
    | TsDiffSpec
    | AbsSpec
    | LogSpec
    | ClipSpec
    | CastSpec
    | ExpressionFactor
)
AggregationFactor = (
    AggFirstSpec
    | AggLastSpec
    | AggSumSpec
    | AggMaxSpec
    | AggMinSpec
    | AggCountSpec
    | AggVwapSpec
)


class ReactiveStateEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        factors: list[ReactiveFactor],
        target: PublisherTarget,
        *,
        source: ReactiveStateEngine | TimeSeriesEngine | None = None,
        parquet_sink: ParquetSink | None = None,
        buffer_capacity: int = 1024,
        overflow_policy: _OverflowPolicyValue | None = None,
        archive_buffer_capacity: int = 1024,
    ) -> None: ...

    def start(self) -> None: ...

    def write(self, value: WriteValue) -> None: ...

    def output_schema(self) -> pa.Schema: ...

    def status(self) -> str: ...

    def metrics(self) -> dict[str, int]: ...

    def config(self) -> dict[str, object]: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...


class TimeSeriesEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        dt_column: str,
        late_data_policy: _LateDataPolicyValue,
        factors: list[AggregationFactor],
        target: PublisherTarget,
        *,
        window: Duration | int | None = None,
        window_type: _WindowTypeValue | None = None,
        window_ns: int | None = None,
        source: ReactiveStateEngine | TimeSeriesEngine | None = None,
        parquet_sink: ParquetSink | None = None,
        buffer_capacity: int = 1024,
        overflow_policy: _OverflowPolicyValue | None = None,
        archive_buffer_capacity: int = 1024,
    ) -> None: ...

    def start(self) -> None: ...

    def write(self, value: WriteValue) -> None: ...

    def output_schema(self) -> pa.Schema: ...

    def status(self) -> str: ...

    def metrics(self) -> dict[str, int]: ...

    def config(self) -> dict[str, object]: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...


def version() -> str: ...
