import polars as pl
import pyarrow as pa

__version__: str


def run_master_daemon(control_endpoint: str) -> None: ...


def setup_log(
    app: str,
    level: str = "info",
    log_dir: str = "logs",
    to_console: bool = True,
    to_file: bool = True,
) -> dict[str, str | None]: ...


def log_info(
    component: str,
    event: str,
    message: str,
    status: str | None = None,
) -> None: ...


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


class _SourceModeValue: ...


class SourceMode:
    PIPELINE: _SourceModeValue
    CONSUMER: _SourceModeValue


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
    expression: str
    output: str

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


class CSRankSpec:
    def __init__(self, column: str, output: str) -> None: ...


class CSZscoreSpec:
    def __init__(self, column: str, output: str) -> None: ...


class CSDemeanSpec:
    def __init__(self, column: str, output: str) -> None: ...


class NullPublisher:
    def __init__(self) -> None: ...


class ParquetSink:
    def __init__(
        self,
        path: str,
        rotation: str = "none",
        write_input: bool = False,
        write_output: bool = True,
        rows_per_batch: int = 8192,
        flush_interval_ms: int = 1000,
    ) -> None: ...


class ZmqPublisher:
    def __init__(self, endpoint: str) -> None: ...


class ZmqStreamPublisher:
    def __init__(self, endpoint: str, stream_name: str, schema: pa.Schema) -> None: ...

    def last_endpoint(self) -> str: ...

    def publish(
        self,
        value: pl.DataFrame | pa.RecordBatch | pa.Table | dict[str, object] | list[dict[str, object]],
    ) -> None: ...

    def publish_hello(self) -> None: ...

    def flush(self) -> None: ...

    def stop(self) -> None: ...


class ZmqSubscriber:
    def __init__(self, endpoint: str, timeout_ms: int = 1000) -> None: ...

    def recv(self) -> pa.RecordBatch: ...

    def close(self) -> None: ...


class ZmqSource:
    def __init__(
        self,
        endpoint: str,
        expected_schema: pa.Schema,
        mode: _SourceModeValue,
    ) -> None: ...


class MasterServer:
    def __init__(self, control_endpoint: str) -> None: ...

    def start(self, startup_timeout_sec: float = 10.0, /) -> None: ...

    def join(self) -> None: ...

    def stop(self) -> None: ...


class BusWriter:
    def write(self, value: WriteValue) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class BusReader:
    def read(self, timeout_ms: int = 1000) -> pa.RecordBatch: ...

    def seek_latest(self) -> None: ...

    def close(self) -> None: ...


class MasterClient:
    def __init__(self, control_endpoint: str) -> None: ...

    def register_process(self, app: str) -> str: ...

    def heartbeat(self) -> None: ...

    def process_id(self) -> str | None: ...

    def control_endpoint(self) -> str: ...

    def register_stream(
        self,
        stream_name: str,
        schema: pa.Schema,
        buffer_size: int,
        frame_size: int,
    ) -> None: ...

    def register_source(
        self,
        source_name: str,
        source_type: str,
        output_stream: str,
        config: object,
    ) -> None: ...

    def register_engine(
        self,
        engine_name: str,
        engine_type: str,
        input_stream: str,
        output_stream: str,
        sink_names: list[str],
        config: object,
    ) -> None: ...

    def register_sink(
        self,
        sink_name: str,
        sink_type: str,
        input_stream: str,
        config: object,
    ) -> None: ...

    def update_status(
        self,
        kind: str,
        name: str,
        status: str,
        metrics: object | None = None,
    ) -> None: ...

    def write_to(self, stream_name: str) -> BusWriter: ...

    def read_from(
        self,
        stream_name: str,
        instrument_ids: list[str] | tuple[str, ...] | str | None = None,
    ) -> BusReader: ...

    def list_streams(self) -> list[dict[str, object]]: ...

    def get_stream(self, stream_name: str) -> dict[str, object]: ...


class BusStreamTarget:
    def __init__(self, stream_name: str, master: MasterClient) -> None: ...


class BusStreamSource:
    def __init__(
        self,
        stream_name: str,
        expected_schema: pa.Schema,
        master: MasterClient,
        mode: _SourceModeValue | None = None,
    ) -> None: ...


WriteValue = (
    pl.DataFrame
    | pa.RecordBatch
    | pa.Table
    | dict[str, object]
    | list[dict[str, object]]
)
PublisherTarget = (
    NullPublisher
    | ZmqPublisher
    | ZmqStreamPublisher
    | BusStreamTarget
    | list[NullPublisher | ZmqPublisher | ZmqStreamPublisher | BusStreamTarget]
)
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
CrossSectionalFactor = CSRankSpec | CSZscoreSpec | CSDemeanSpec


class ReactiveStateEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        factors: list[ReactiveFactor],
        target: PublisherTarget,
        *,
        id_filter: list[str] | None = None,
        source: ReactiveStateEngine | StreamTableEngine | TimeSeriesEngine | ZmqSource | BusStreamSource | None = None,
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


class StreamTableEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        target: PublisherTarget,
        *,
        source: ReactiveStateEngine | StreamTableEngine | TimeSeriesEngine | ZmqSource | BusStreamSource | None = None,
        sink: ParquetSink | None = None,
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
        pre_factors: list[ExpressionFactor] | None = None,
        post_factors: list[ExpressionFactor] | None = None,
        id_filter: list[str] | None = None,
        source: ReactiveStateEngine | StreamTableEngine | TimeSeriesEngine | ZmqSource | BusStreamSource | None = None,
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


class CrossSectionalEngine:
    def __init__(
        self,
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        dt_column: str,
        trigger_interval: Duration | int,
        late_data_policy: _LateDataPolicyValue,
        factors: list[CrossSectionalFactor],
        target: PublisherTarget,
        *,
        source: TimeSeriesEngine | ZmqSource | None = None,
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
