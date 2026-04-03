import polars as pl
import pyarrow as pa


class TsEmaSpec:
    def __init__(
        self,
        id_column: str,
        value_column: str,
        span: int,
        output: str,
    ) -> None: ...


class NullPublisher:
    def __init__(self) -> None: ...


class ZmqPublisher:
    def __init__(self, endpoint: str) -> None: ...


WriteValue = (
    pl.DataFrame
    | pa.RecordBatch
    | pa.Table
    | dict[str, object]
    | list[dict[str, object]]
)
PublisherTarget = NullPublisher | ZmqPublisher | list[NullPublisher | ZmqPublisher]


class ReactiveStateEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        factors: list[TsEmaSpec],
        target: PublisherTarget,
    ) -> None: ...

    def start(self) -> None: ...

    def write(self, value: WriteValue) -> None: ...

    def output_schema(self) -> pa.Schema: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...
