import pyarrow as pa

__version__: str
__build_revision__: str | None

def _signal_server_process(
    pid: int,
    boot_id: str,
    start_ticks: str,
    signal_number: int,
) -> bool: ...
def resolve_server_control_endpoint(
    uri: str | None = None,
    *,
    config: str | None = None,
    endpoint_explicit: bool = False,
) -> str: ...
def run_server_daemon(
    uri: str | None = None,
    *,
    control_endpoint: str | None = None,
    config: str | None = None,
    ready_file: str | None = None,
    launch_nonce: str | None = None,
    log_dir: str | None = None,
    startup_timeout_sec: float | None = None,
    endpoint_explicit: bool = False,
) -> None: ...
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
    AggFirstSpec | AggLastSpec | AggSumSpec | AggMaxSpec | AggMinSpec | AggCountSpec | AggVwapSpec
)
CrossSectionalFactor = CSRankSpec | CSZscoreSpec | CSDemeanSpec

class KeyValueState:
    def select(self, keys: list[list[str]]) -> pa.RecordBatch: ...
    def __init__(self, snapshot: pa.RecordBatch, max_pending_bytes: int) -> None: ...
    def apply(self, batch: pa.RecordBatch) -> int: ...
    def snapshot(self) -> pa.RecordBatch: ...
    @property
    def sequence(self) -> int: ...

def version() -> str: ...

class NativeProcessor:
    @staticmethod
    def identity(name: str, input_schema: pa.Schema) -> NativeProcessor: ...
    @staticmethod
    def chain(
        name: str,
        processors: list[NativeProcessor],
        *,
        max_intermediate_bytes: int = 67_108_864,
    ) -> NativeProcessor: ...
    @staticmethod
    def reactive(
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        factors: list[ReactiveFactor],
        *,
        id_filter: list[str] | None = None,
        state_failure_policy: str = "fail_fast",
        invalid_value_policy: str = "reject",
    ) -> NativeProcessor: ...
    @staticmethod
    def latest(name: str, input_schema: pa.Schema, by: list[str]) -> NativeProcessor: ...
    @staticmethod
    def timeseries(
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        dt_column: str,
        window_ns: int,
        factors: list[AggregationFactor],
        *,
        late_data_policy: str = "reject",
        pre_factors: list[ExpressionFactor] | None = None,
        post_factors: list[ExpressionFactor] | None = None,
        id_filter: list[str] | None = None,
    ) -> NativeProcessor: ...
    @staticmethod
    def cross_sectional(
        name: str,
        input_schema: pa.Schema,
        id_column: str,
        dt_column: str,
        trigger_interval_ns: int,
        factors: list[CrossSectionalFactor],
    ) -> NativeProcessor: ...
    @staticmethod
    def bars(name: str, input_schema: pa.Schema, profile: object) -> NativeProcessor: ...
    def input_schema(self) -> pa.Schema: ...
    def output_schema(self) -> pa.Schema: ...
    def process(self, batch: pa.RecordBatch) -> list[pa.RecordBatch]: ...
    def flush(self) -> list[pa.RecordBatch]: ...
    def finish(self) -> list[pa.RecordBatch]: ...
    def transfer(self) -> NativeProcessor: ...
    def close(self) -> None: ...
