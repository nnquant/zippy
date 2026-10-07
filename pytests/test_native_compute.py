"""Native factor kernels and their single-owner Flight output lifecycle."""

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Literal, cast
import uuid

import pyarrow as pa
import pytest
from zippy_client import Client, SubscriptionOptions
from zippy_client.errors import AmbiguousWriteError, CapabilityError

import zippy
from zippy import ComputePipeline, NativeProcessor

SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("dt", pa.timestamp("ns", tz="UTC")),
        ("price", pa.float64()),
        ("volume", pa.float64()),
    ]
)


def batch(
    prices: list[float], *, dt: list[int] | None = None, symbols: list[str] | None = None
) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            "symbol": symbols or ["A"] * len(prices),
            "dt": dt or [0] * len(prices),
            "price": prices,
            "volume": [1.0] * len(prices),
        },
        schema=SCHEMA,
    )


def reactive() -> NativeProcessor:
    return NativeProcessor.reactive(
        "mean",
        SCHEMA,
        "symbol",
        [zippy.TsMeanSpec(id_column="symbol", value_column="price", window=2, output="mean")],
    )


def timeseries() -> NativeProcessor:
    return NativeProcessor.timeseries(
        "sum",
        SCHEMA,
        "symbol",
        "dt",
        10_000,
        [zippy.AggSumSpec(column="volume", output="total")],
    )


def test_reactive_state_transfers_and_rejects_schema_before_mutation() -> None:
    processor = reactive()
    assert processor.process(batch([1.0]))[0].column("mean").to_pylist() == [None]
    with pytest.raises(ValueError, match="schema differs"):
        processor.process(batch([100.0]).select(["symbol", "price"]))
    owned = processor.transfer()
    with pytest.raises(RuntimeError, match="closed"):
        processor.process(batch([3.0]))
    assert owned.process(batch([3.0, 5.0]))[0].column("mean").to_pylist() == [2.0, 4.0]
    assert owned.finish() == []
    assert owned.finish() == []
    with pytest.raises(RuntimeError, match="closed"):
        owned.flush()


def test_latest_outputs_key_updates_without_reemitting_absent_keys() -> None:
    processor = NativeProcessor.latest("latest", SCHEMA, ["symbol"])
    first = processor.process(batch([1.0, 8.0, 3.0], symbols=["A", "B", "A"]))[0]
    assert first.column("symbol").to_pylist() == ["A", "B"]
    assert first.column("price").to_pylist() == [3.0, 8.0]
    second = processor.process(batch([5.0]))[0]
    assert second.column("symbol").to_pylist() == ["A"]
    assert second.column("price").to_pylist() == [5.0]
    assert processor.finish() == []


def test_timeseries_closes_windows_once_and_finishes_remaining_state() -> None:
    processor = timeseries()
    assert processor.process(batch([1.0, 2.0], dt=[0, 1_000])) == []
    closed = processor.process(batch([3.0], dt=[10_000]))[0]
    assert closed.column("total").to_pylist() == [2.0]
    assert closed.column("window_start").cast(pa.int64()).to_pylist() == [0]
    assert closed.column("window_end").cast(pa.int64()).to_pylist() == [10_000]
    last = processor.finish()[0]
    assert last.column("total").to_pylist() == [1.0]
    assert last.column("window_start").cast(pa.int64()).to_pylist() == [10_000]
    assert processor.finish() == []


def test_kernel_error_is_terminal_after_state_may_have_changed() -> None:
    processor = timeseries()
    processor.process(batch([1.0], dt=[20_000]))
    with pytest.raises(RuntimeError, match="late"):
        processor.process(batch([2.0], dt=[0]))
    for action in (processor.flush, processor.finish, processor.transfer):
        with pytest.raises(RuntimeError, match="failed|not active"):
            action()


def test_cross_sectional_ranks_complete_bucket_using_latest_member_values() -> None:
    processor = NativeProcessor.cross_sectional(
        "ranks",
        SCHEMA,
        "symbol",
        "dt",
        10_000,
        [zippy.CSRankSpec(column="price", output="rank")],
    )
    assert processor.process(batch([9.0, 4.0], symbols=["A", "B"])) == []
    assert processor.process(batch([3.0], dt=[1_000])) == []
    result = processor.flush()[0]
    assert result.column("symbol").to_pylist() == ["A", "B"]
    assert result.column("rank").to_pylist() == [1.0, 2.0]
    assert processor.finish() == []


def test_bars_share_session_ohlcv_and_flush_semantics() -> None:
    schema = pa.schema(
        [
            pa.field(name, dtype, nullable=False)
            for name, dtype in [
                ("symbol", pa.string()),
                ("dt", pa.timestamp("ns", tz="UTC")),
                ("price", pa.float64()),
                ("volume", pa.float64()),
                ("turnover", pa.float64()),
                ("trading_day", pa.string()),
            ]
        ]
    )
    profile = zippy.bar.BarGeneratorProfile(
        frequency="1m",
        columns=zippy.bar.InputColumns(
            instrument="symbol",
            dt="dt",
            price="price",
            volume="volume",
            total_turnover="turnover",
            trading_day="trading_day",
            num_trades=None,
            limit_up=None,
            limit_down=None,
        ),
        sessions=zippy.bar.TradingSessions(
            timezone="UTC",
            regular=[("09:30:00", "15:00:00")],
            auction=[],
        ),
        volume=zippy.bar.Volume.delta(),
        auction=zippy.bar.Auction.drop(),
        dt_label="close_dt",
    )
    processor = NativeProcessor.bars("bars", schema, profile)
    ticks = pa.RecordBatch.from_pydict(
        {
            "symbol": ["A", "A", "A"],
            "dt": [datetime(2026, 9, 22, 9, 30, sec, tzinfo=timezone.utc) for sec in [1, 2, 3]],
            "price": [10.0, 12.0, 11.0],
            "volume": [1.0, 2.0, 3.0],
            "turnover": [10.0, 24.0, 33.0],
            "trading_day": ["2026-09-22"] * 3,
        },
        schema=schema,
    )
    assert processor.process(ticks.slice(0, 1)) == []
    assert processor.process(ticks.slice(1)) == []
    result = processor.finish()[0]
    assert result.num_rows == 1
    for column, expected in {
        "open": 10.0,
        "high": 12.0,
        "low": 10.0,
        "close": 11.0,
        "volume": 6,
    }.items():
        assert result.column(column).to_pylist() == [expected]
    assert processor.finish() == []


class FakeFlight:
    def __init__(self) -> None:
        self.capabilities = SimpleNamespace(
            names={"ingest.table_identity.v1", "ingest.resume_after_restart.v1"}
        )
        self.writes: list[tuple[pa.Table, dict[str, object]]] = []
        self.closed: list[uuid.UUID] = []
        self.failure: BaseException | None = None

    def write_table(self, name: str, table: pa.Table, **kwargs: object) -> list[dict[str, object]]:
        self.writes.append((table, kwargs))
        if self.failure is not None:
            raise self.failure
        return [{"producer_epoch": 7, "batch_sequence": kwargs["first_batch_sequence"]}]

    def close_write(self, connection: uuid.UUID) -> None:
        self.closed.append(connection)


class FakeSource:
    def __init__(self) -> None:
        self.state = "running"
        self.error: BaseException | None = None

    def status(self) -> str:
        return self.state

    def metrics(self) -> dict[str, object]:
        return {"state": self.state, "last_error": str(self.error) if self.error else None}

    def join(self, timeout: float | None = None) -> None:
        if self.error:
            raise self.error

    def stop(self) -> None:
        self.state = "stopped"
        self.join()


def pipeline(
    flight: FakeFlight,
    processor: NativeProcessor,
    *,
    output_kind: Literal["append", "frame", "key_value"] = "append",
    input_options: SubscriptionOptions | None = None,
) -> ComputePipeline:
    # Keep the test transport boundary explicit; real native processors still execute every row.
    return ComputePipeline(
        cast(Client, SimpleNamespace(flight=flight)),
        processor,
        output="factors",
        output_table_id=uuid.UUID(int=42),
        output_kind=output_kind,
        input_options=input_options,
    )


@pytest.mark.parametrize(
    "kind,mutation", [("append", "append"), ("frame", "frame"), ("key_value", "upsert")]
)
def test_pipeline_identity_receipts_and_exclusive_ownership(
    kind: Literal["append", "frame", "key_value"], mutation: str
) -> None:
    flight = FakeFlight()
    kernel = reactive()
    with pipeline(flight, kernel, output_kind=kind) as job:
        with pytest.raises(RuntimeError, match="closed"):
            kernel.process(batch([99.0]))
        job.write(batch([1.0]))
        job.write(batch([3.0]))
        assert flight.writes[-1][0].column("mean").to_pylist() == [2.0]
        assert job.metrics()["input_rows"] == 2
        assert job.metrics()["output_rows"] == 2
        assert job.metrics()["next_batch_sequence"] == 2
    first, second = (entry[1] for entry in flight.writes)
    assert first["mutation"] == second["mutation"] == mutation
    assert first["expected_table_id"] == second["expected_table_id"] == uuid.UUID(int=42)
    assert first["expected_writer_epoch"] is None
    assert second["expected_writer_epoch"] == 7
    assert first["producer_id"] == second["producer_id"]
    assert first["first_batch_sequence"] == 0
    assert second["first_batch_sequence"] == 1
    assert job.status() == "stopped"
    job.stop()
    assert len(flight.closed) == 1


def test_pipeline_finishes_windows_but_discards_on_context_failure() -> None:
    flight = FakeFlight()
    with pipeline(flight, timeseries()) as job:
        job.write(batch([1.0, 3.0]))
        assert flight.writes == []
    assert flight.writes[0][0].column("total").to_pylist() == [2.0]
    flight = FakeFlight()
    with pytest.raises(ValueError, match="upstream"):
        with pipeline(flight, timeseries()) as job:
            job.write(batch([1.0]))
            raise ValueError("upstream failed")
    assert flight.writes == []
    assert len(flight.closed) == 1


def test_pipeline_never_reexecutes_after_uncertain_output_and_keeps_known_receipts() -> None:
    flight = FakeFlight()
    job = pipeline(flight, timeseries()).start()
    job.write(batch([1.0]))
    error = AmbiguousWriteError("lost second receipt", receipts=[{"batch_sequence": 0}])
    flight.failure = error
    with pytest.raises(AmbiguousWriteError) as failed:
        job.write(batch([3.0], dt=[10_000]))
    assert failed.value is error
    assert job.status() == "failed"
    assert job.metrics()["failed_write_receipts"] == [{"batch_sequence": 0}]
    assert job.metrics()["next_batch_sequence"] == 0
    with pytest.raises(RuntimeError):
        job.write(batch([5.0]))
    with pytest.raises(AmbiguousWriteError):
        job.stop()
    assert len(flight.writes) == 1
    assert len(flight.closed) == 1


def test_pipeline_rejects_durable_ack_without_claiming_kernel_checkpoint() -> None:
    kernel = reactive()
    with pytest.raises(ValueError, match="kernel checkpoint"):
        pipeline(
            FakeFlight(),
            kernel,
            input_options=SubscriptionOptions(consumer_id=uuid.uuid4(), start="committed"),
        )
    # Invalid configuration must not consume caller ownership.
    assert kernel.process(batch([1.0]))
    flight = FakeFlight()
    flight.capabilities.names.clear()
    with pytest.raises(CapabilityError, match="recovery"):
        pipeline(flight, kernel)
    assert kernel.finish() == []


@pytest.mark.parametrize("failed", [False, True])
def test_pipeline_source_health_is_not_hidden(failed: bool) -> None:
    source = FakeSource()
    client = SimpleNamespace(flight=FakeFlight(), subscribe_table=lambda *a, **kw: source)
    job = ComputePipeline(
        cast(Client, client),
        timeseries(),
        source="ticks",
        output="bars",
        output_table_id=uuid.uuid4(),
    ).start()
    source.state = "failed" if failed else "stopped"
    source.error = ValueError("source protocol failed") if failed else None
    assert job.status() == ("failed" if failed else "source_stopped")
    assert job.metrics()["state"] == job.status()
    with pytest.raises(RuntimeError):
        job.flush()
    if failed:
        with pytest.raises(ValueError, match="source protocol failed"):
            job.join(0)
        with pytest.raises(ValueError, match="source protocol failed"):
            job.stop()
    else:
        job.stop()


def test_pipeline_output_recovery_is_explicit_and_memory_never_retries() -> None:
    flight = FakeFlight()
    with pipeline(flight, reactive()) as job:
        job.write(batch([1.0]))
    assert flight.writes[0][1]["max_reconnect_attempts"] == 5
    flight = FakeFlight()
    with ComputePipeline(
        cast(Client, SimpleNamespace(flight=flight)),
        reactive(),
        output="factors",
        output_table_id=uuid.UUID(int=42),
        durability="memory",
    ) as job:
        job.write(batch([1.0]))
    assert flight.writes[0][1]["max_reconnect_attempts"] == 0
    for attempts in [-1, 11, True]:
        with pytest.raises(ValueError, match="output_reconnect_attempts"):
            ComputePipeline(
                cast(Client, SimpleNamespace(flight=flight)),
                reactive(),
                output="factors",
                output_table_id=uuid.UUID(int=42),
                output_reconnect_attempts=attempts,
            )


def test_pipeline_schema_guard_preserves_ownership_on_conflicting_options() -> None:
    processor = reactive()
    with pytest.raises(ValueError, match="expected_schema"):
        pipeline(
            FakeFlight(),
            processor,
            input_options=SubscriptionOptions(expected_schema=SCHEMA.with_metadata({"x": "y"})),
        )
    assert processor.process(batch([1.0]))[0].column("mean").to_pylist() == [None]
    processor.close()


def test_pipeline_supplies_exact_kernel_schema_to_source() -> None:
    received: list[SubscriptionOptions] = []
    source = FakeSource()

    def subscribe_table(name, callback, *, options):
        received.append(options)
        return source

    options = SubscriptionOptions(namespace="market", start="earliest")
    with ComputePipeline(
        cast(Client, SimpleNamespace(flight=FakeFlight(), subscribe_table=subscribe_table)),
        reactive(),
        output="factors",
        output_table_id=uuid.uuid4(),
        source="ticks",
        input_options=options,
    ):
        assert len(received) == 1
        expected = received[0].expected_schema
        assert expected is not None and expected.equals(SCHEMA, check_metadata=True)
        assert received[0].namespace == "market" and received[0].start == "earliest"
        assert options.expected_schema is None


@pytest.mark.parametrize("kind", ["reactive", "timeseries"])
def test_native_filters_preserve_exact_nonblank_instrument_identity(kind: str) -> None:
    if kind == "reactive":
        processor = NativeProcessor.reactive(
            "filtered",
            SCHEMA,
            "symbol",
            [zippy.ABS(column="price", output="absolute")],
            id_filter=["A", " A "],
        )
    else:
        processor = NativeProcessor.timeseries(
            "filtered",
            SCHEMA,
            "symbol",
            "dt",
            10_000,
            [zippy.AGG_SUM(column="volume", output="total")],
            id_filter=["A", " A "],
        )
    results = processor.process(batch([1.0, 2.0, 3.0], symbols=["A", "B", " A "]))
    results.extend(processor.finish())
    assert sorted(pa.Table.from_batches(results).column("symbol").to_pylist()) == [" A ", "A"]
