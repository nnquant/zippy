from datetime import datetime, timezone
from pathlib import Path
import subprocess

import polars as pl
import pyarrow as pa
import pytest

import zippy

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(WORKSPACE_ROOT), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_reactive_engine_accepts_polars_and_flushes() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A", "A"], "price": [10.0, 11.0]}))
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_record_batch() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(["A", "A"]),
            pa.array([10.0, 11.0]),
        ],
        schema=schema,
    )

    engine.start()
    engine.write(batch)
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_pyarrow_table() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    table = pa.table(
        {
            "symbol": ["A", "A"],
            "price": [10.0, 11.0],
        },
        schema=schema,
    )

    engine.start()
    engine.write(table)
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_columnar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": ["A", "A"], "price": [10.0, 11.0]})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_scalar_dict_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write({"symbol": "A", "price": 10.0})
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_row_oriented_input() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        [
            {"symbol": "A", "price": 10.0},
            {"symbol": "A", "price": 11.0},
        ]
    )
    engine.flush()
    engine.stop()


def test_write_after_stop_raises_runtime_error() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()

    with pytest.raises(RuntimeError):
        engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))


def test_reactive_engine_exposes_output_schema_before_and_after_start() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_python_package_exposes_git_derived_version() -> None:
    commit_count = git_output("rev-list", "--count", "HEAD")
    short_sha = git_output("rev-parse", "--short", "HEAD")
    expected = f"0.1.0.dev{commit_count}+g{short_sha}"

    assert zippy.__version__ == expected
    assert zippy.version() == expected


def test_timeseries_engine_accepts_polars_and_exposes_output_schema() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            pa.field("window_start", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("window_end", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=60_000_000_000,
        late_data_policy="reject",
        factors=[
            zippy.AggFirstSpec(column="price", output="open"),
            zippy.AggLastSpec(column="price", output="close"),
            zippy.AggSumSpec(column="volume", output="volume"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write(
        pl.DataFrame(
            {
                "symbol": ["A", "A"],
                "dt": [
                    "2026-04-02T09:30:00.000000000Z",
                    "2026-04-02T09:30:01.000000000Z",
                ],
                "price": [10.0, 11.0],
                "volume": [100.0, 120.0],
            }
        ).with_columns(pl.col("dt").str.to_datetime(time_unit="ns", time_zone="UTC"))
    )
    engine.flush()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_reactive_engine_accepts_all_v1_operators_via_design_helpers() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
            pa.field("mean_3", pa.float64(), nullable=True),
            pa.field("std_3", pa.float64(), nullable=True),
            pa.field("delay_2", pa.float64(), nullable=True),
            pa.field("diff_2", pa.float64(), nullable=True),
            pa.field("ret_2", pa.float64(), nullable=True),
            pa.field("abs_price", pa.float64(), nullable=False),
            pa.field("log_price", pa.float64(), nullable=False),
            pa.field("clipped_price", pa.float64(), nullable=False),
            pa.field("price_i64", pa.int64(), nullable=False),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.TS_MEAN(column="price", window=3, output="mean_3"),
            zippy.TS_STD(column="price", window=3, output="std_3"),
            zippy.TS_DELAY(column="price", period=2, output="delay_2"),
            zippy.TS_DIFF(column="price", period=2, output="diff_2"),
            zippy.TS_RETURN(column="price", period=2, output="ret_2"),
            zippy.ABS(column="price", output="abs_price"),
            zippy.LOG(column="price", output="log_price"),
            zippy.CLIP(column="price", min=12.0, max=20.0, output="clipped_price"),
            zippy.CAST(column="price", dtype="int64", output="price_i64"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write({"symbol": ["A", "A", "A"], "price": [10.0, 16.0, 19.0]})
    engine.flush()
    engine.stop()


def test_timeseries_engine_accepts_all_v1_aggregation_operators_via_design_helpers() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            pa.field("window_start", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("window_end", pa.timestamp("ns", tz="UTC"), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("count", pa.float64(), nullable=False),
            pa.field("vwap", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=60_000_000_000,
        late_data_policy="reject",
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_MAX(column="price", output="high"),
            zippy.AGG_MIN(column="price", output="low"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_COUNT(column="price", output="count"),
            zippy.AGG_VWAP(price_column="price", volume_column="volume", output="vwap"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    engine.write(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
            "volume": [100.0, 120.0],
        }
    )
    engine.flush()
    engine.stop()


def test_reactive_engine_accepts_target_list() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=[zippy.NullPublisher(), zippy.NullPublisher()],
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()


def test_reactive_engine_accepts_zmq_target() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsEmaSpec(
                id_column="symbol",
                value_column="price",
                span=2,
                output="ema_2",
            )
        ],
        target=zippy.ZmqPublisher(endpoint="tcp://127.0.0.1:*"),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()
