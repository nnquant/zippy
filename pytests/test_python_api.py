from datetime import datetime, timezone
from pathlib import Path
import socket
import subprocess
import time

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
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


def reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


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
        late_data_policy=zippy.LateDataPolicy.REJECT,
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


def test_timeseries_engine_accepts_duration_window_api() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
            "volume": [100.0],
        }
    )
    engine.flush()
    engine.stop()


def test_timeseries_engine_rejects_string_policy_arguments() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="window_type"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type="sliding",
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy="reject",
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=pa.schema([("symbol", pa.string()), ("price", pa.float64())]),
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy="block",
        )


def test_engines_reject_wrong_policy_constant_categories() -> None:
    timeseries_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    reactive_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="window_type"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=timeseries_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.OverflowPolicy.BLOCK,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=timeseries_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.WindowType.TUMBLING,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=reactive_schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy=zippy.LateDataPolicy.REJECT,
        )


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


def test_reactive_engine_accepts_expression_factor_helper() -> None:
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
            pa.field("price_plus_ema", pa.float64(), nullable=True),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.EXPR(expression="price + ema_2", output="price_plus_ema"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write({"symbol": ["A", "A"], "price": [10.0, 16.0]})
    engine.flush()

    received = subscriber.recv()

    assert received.column(2).to_pylist() == [10.0, 14.0]
    assert received.column(3).to_pylist() == [20.0, 30.0]

    engine.stop()
    subscriber.close()


def test_expression_factor_rejects_unknown_identifier() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unknown expression identifier"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.EXPR(expression="missing + 1.0", output="score")],
            target=zippy.NullPublisher(),
        )


def test_expression_factor_rejects_unsupported_function() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unsupported expression function"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.EXPR(expression="sqrt(price)", output="score")],
            target=zippy.NullPublisher(),
        )


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
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
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


def test_timeseries_engine_accepts_pre_and_post_expression_factors() -> None:
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
            pa.field("turnover", pa.float64(), nullable=False),
            pa.field("ret_1m", pa.float64(), nullable=False),
            pa.field("vwap_1m", pa.float64(), nullable=False),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        pre_factors=[
            zippy.EXPR(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.EXPR(expression="close / open - 1.0", output="ret_1m"),
            zippy.EXPR(expression="turnover / volume", output="vwap_1m"),
        ],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema


def test_timeseries_engine_rejects_non_expression_phase_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="pre_factors"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            pre_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )

    with pytest.raises(TypeError, match="post_factors"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            post_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_rejects_post_factors_referencing_raw_input_columns() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="unknown expression identifier"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[
                zippy.AGG_FIRST(column="price", output="open"),
                zippy.AGG_LAST(column="price", output="close"),
            ],
            post_factors=[
                zippy.EXPR(expression="price * 2.0", output="bad"),
            ],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_drop_with_metric_filters_late_rows_before_pre_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
        pre_factors=[
            zippy.EXPR(expression="log(price)", output="log_price"),
        ],
        factors=[zippy.AGG_SUM(column="log_price", output="sum_log_price")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [0.0],
        }
    )
    engine.flush()

    assert engine.metrics()["late_rows_total"] == 1

    engine.stop()


def test_timeseries_engine_flush_runs_post_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        pre_factors=[
            zippy.EXPR(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.EXPR(expression="close / open - 1.0", output="ret_1m"),
            zippy.EXPR(expression="turnover / volume", output="vwap_1m"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
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

    received = subscriber.recv()

    assert received.column_names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
        "close",
        "volume",
        "turnover",
        "ret_1m",
        "vwap_1m",
    ]
    assert received.column(6).to_pylist() == [2320.0]
    assert received.column(7).to_pylist() == pytest.approx([0.1])
    assert received.column(8).to_pylist() == pytest.approx([2320.0 / 220.0])

    engine.stop()
    subscriber.close()


def test_timeseries_engine_accepts_reactive_source_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()
    bars.start()
    reactive.write(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )
    reactive.flush()
    bars.flush()
    reactive.stop()
    bars.stop()


def test_timeseries_engine_rejects_source_schema_mismatch_at_construction() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    mismatched_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("extra", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )

    with pytest.raises(ValueError, match="source output schema"):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            source=reactive,
            input_schema=mismatched_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            target=zippy.NullPublisher(),
        )


def test_source_write_requires_downstream_started() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()

    with pytest.raises(RuntimeError, match="downstream engine must be started"):
        reactive.write(
            {
                "symbol": ["A"],
                "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
                "price": [10.0],
            }
        )

    reactive.stop()
    bars.start()
    bars.stop()


def test_downstream_stop_requires_source_stopped() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    reactive.start()
    bars.start()

    with pytest.raises(RuntimeError, match="source engine must be stopped"):
        bars.stop()

    reactive.stop()
    bars.stop()


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


def test_zmq_subscriber_receives_record_batch_from_engine() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
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
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.flush()

    received = subscriber.recv()

    assert isinstance(received, pa.RecordBatch)
    assert received.schema == pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
        ]
    )
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [10.0]
    assert received.column(2).to_pylist() == [10.0]

    engine.stop()
    subscriber.close()


def test_reactive_engine_accepts_parquet_sink_and_archives_input_and_output(
    tmp_path: Path,
) -> None:
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
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="1h",
            write_input=True,
            write_output=True,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.stop()

    parquet_files = list(tmp_path.rglob("*.parquet"))
    input_files = [path for path in parquet_files if "input" in path.parts]
    output_files = [path for path in parquet_files if "output" in path.parts]

    assert input_files
    assert output_files
    assert pq.read_schema(input_files[0]).names == ["symbol", "price"]
    assert pq.read_schema(output_files[0]).names == ["symbol", "price", "ema_2"]


def test_source_pipeline_archives_downstream_input_via_parquet_sink(
    tmp_path: Path,
) -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    reactive = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=tick_schema,
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=reactive,
        input_schema=reactive.output_schema(),
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=False,
        ),
    )

    reactive.start()
    bars.start()
    reactive.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    reactive.stop()
    bars.stop()

    input_files = list((tmp_path / "input").rglob("*.parquet"))

    assert input_files
    assert pq.read_schema(input_files[0]).names == ["symbol", "dt", "price", "ema_2"]


def test_engine_rejects_invalid_runtime_config_keywords(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="buffer_capacity"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            buffer_capacity=0,
        )

    with pytest.raises(ValueError, match="overflow_policy"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            overflow_policy="block",
        )

    with pytest.raises(ValueError, match="archive_buffer_capacity"):
        zippy.ReactiveStateEngine(
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
            target=zippy.NullPublisher(),
            parquet_sink=zippy.ParquetSink(
                path=str(tmp_path),
                rotation="none",
                write_input=True,
                write_output=False,
            ),
            archive_buffer_capacity=0,
        )


def test_reactive_engine_exposes_status_metrics_and_config_lifecycle(
    tmp_path: Path,
) -> None:
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
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=False,
        ),
        buffer_capacity=32,
        overflow_policy=zippy.OverflowPolicy.REJECT,
        archive_buffer_capacity=8,
    )

    assert engine.status() == "created"
    assert engine.metrics() == {
        "processed_batches_total": 0,
        "processed_rows_total": 0,
        "output_batches_total": 0,
        "dropped_batches_total": 0,
        "late_rows_total": 0,
        "publish_errors_total": 0,
        "queue_depth": 0,
    }

    config = engine.config()
    assert config["engine_type"] == "reactive"
    assert config["buffer_capacity"] == 32
    assert config["overflow_policy"] == "reject"
    assert config["archive_buffer_capacity"] == 8
    assert config["targets"] == [{"type": "null"}]
    assert config["source_linked"] is False
    assert config["parquet_sink"] == {
        "path": str(tmp_path),
        "rotation": "none",
        "write_input": True,
        "write_output": False,
    }

    engine.start()
    assert engine.status() == "running"

    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.flush()

    metrics = engine.metrics()
    assert metrics["processed_batches_total"] == 1
    assert metrics["processed_rows_total"] == 1
    assert metrics["output_batches_total"] == 1
    assert metrics["queue_depth"] == 0

    engine.stop()
    assert engine.status() == "stopped"
    assert engine.metrics()["processed_batches_total"] == 1


def test_timeseries_engine_config_and_output_archive_roundtrip(
    tmp_path: Path,
) -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
        buffer_capacity=17,
        overflow_policy=zippy.OverflowPolicy.DROP_OLDEST,
        archive_buffer_capacity=9,
    )

    config = engine.config()
    assert config["engine_type"] == "timeseries"
    assert config["window_ns"] == 60_000_000_000
    assert config["late_data_policy"] == "reject"
    assert config["buffer_capacity"] == 17
    assert config["overflow_policy"] == "drop_oldest"
    assert config["archive_buffer_capacity"] == 9
    assert config["parquet_sink"]["write_output"] is True

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.stop()

    output_files = list((tmp_path / "output").rglob("*.parquet"))

    assert output_files
    assert pq.read_schema(output_files[0]).names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
    ]


def test_timeseries_engine_metrics_report_late_rows() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.TimeSeriesEngine(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
        factors=[zippy.AGG_FIRST(column="price", output="open")],
        target=zippy.NullPublisher(),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)],
            "price": [11.0],
        }
    )
    engine.write(
        {
            "symbol": ["A"],
            "dt": [datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)],
            "price": [10.0],
        }
    )
    engine.flush()

    assert engine.status() == "running"
    assert engine.metrics()["late_rows_total"] == 1

    engine.stop()
    assert engine.status() == "stopped"


def test_parquet_sink_failure_marks_engine_failed_status(tmp_path: Path) -> None:
    blocked_root = tmp_path / "blocked"
    blocked_root.write_text("not-a-directory", encoding="utf-8")
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
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(blocked_root),
            rotation="none",
            write_input=True,
            write_output=False,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))

    with pytest.raises(RuntimeError, match="failed to create parquet directory"):
        engine.stop()

    assert engine.status() == "failed"
