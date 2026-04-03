import polars as pl
import pyarrow as pa

__version__: str


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
        source: ReactiveStateEngine | TimeSeriesEngine | None = None,
    ) -> None: ...

    def start(self) -> None: ...

    def write(self, value: WriteValue) -> None: ...

    def output_schema(self) -> pa.Schema: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...


class TimeSeriesEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        dt_column: str,
        window_ns: int,
        late_data_policy: str,
        factors: list[AggregationFactor],
        target: PublisherTarget,
        source: ReactiveStateEngine | TimeSeriesEngine | None = None,
    ) -> None: ...

    def start(self) -> None: ...

    def write(self, value: WriteValue) -> None: ...

    def output_schema(self) -> pa.Schema: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...


def version() -> str: ...
